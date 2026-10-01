//! Preflight and channel A: the globs against a live catalog (config §11.2).

use data_avalanche_core::{ColumnValueType as V, HaltReason, LogicalType, SourcePosition};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, connector, fresh, pipeline, resolved};

#[tokio::test]
async fn preflight_accepts_the_test_server() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "preflight", "").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let p = c.preflight().await.expect("preflight");
    assert!(!p.server_version.is_empty());
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn preflight_refuses_a_user_who_cant_log_in() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "preflight_auth", "").await;
    let mut p = pipeline(&n, &["*"], "", "");
    p.source.credentials = data_avalanche_config::Credentials::Inline {
        user: "nobody_here".into(),
        password: data_avalanche_config::Secret::new(
            "x",
            &data_avalanche_config::FingerprintKey::from_bytes([1; 32]),
        ),
    };
    let e = connector(&p).preflight().await.unwrap_err();
    assert!(
        matches!(&e, data_avalanche_core::SourceError::Permanent(m) if m.contains("SQLSTATE 28")),
        "an authentication failure is permanent: {e:?}"
    );
    cleanup(&a, &n).await;
}

const DDL: &str = "
    CREATE TYPE mood AS ENUM ('sad', 'ok');
    CREATE DOMAIN price AS numeric(10,2);
    CREATE TABLE orders (id bigint PRIMARY KEY, gone int, note text, amount numeric(10,2),
                         at timestamptz);
    ALTER TABLE orders DROP COLUMN gone;
    CREATE TABLE typed (id int PRIMARY KEY, m mood, p price, tags text[], ms mood[], wide numeric,
                        doubled int GENERATED ALWAYS AS (id * 2) STORED);
    CREATE TABLE nopk (a int);
    CREATE TABLE nopk_named (a int);
    CREATE TABLE nothing (id int PRIMARY KEY);
    ALTER TABLE nothing REPLICA IDENTITY NOTHING;
    CREATE TABLE by_index (id int PRIMARY KEY, code text NOT NULL UNIQUE);
    ALTER TABLE by_index REPLICA IDENTITY USING INDEX by_index_code_key;
    CREATE TABLE reserved (id int PRIMARY KEY, \"_OP\" text);
    CREATE TABLE events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
    CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    CREATE TABLE tmp_x (id int PRIMARY KEY);
    CREATE UNLOGGED TABLE cache (id int PRIMARY KEY);
    CREATE UNLOGGED TABLE cache_named (id int PRIMARY KEY);
";

#[tokio::test]
async fn channel_a_resolves_checks_and_skips() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "resolve", DDL).await;
    let s = &n.schema;
    let tables = format!(
        "      exclude: [{s}.tmp_*]\n      primary_key: {{ {s}.by_index: [code], {s}.nothing: [id] }}"
    );
    let c = connector(&pipeline(
        &n,
        &["*", "nopk_named", "cache_named", "missing"],
        &tables,
        "",
    ));
    let r = resolved(&c).await;

    let names: Vec<&str> = r.tables.iter().map(|t| t.table().name.as_str()).collect();
    assert_eq!(names, ["by_index", "events_2024", "orders", "typed"]);
    let orders = &r.tables[2];
    let columns: Vec<(&str, Option<i32>)> = orders
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), c.source_ordinal))
        .collect();
    assert_eq!(
        columns,
        [
            ("id", Some(1)),
            ("note", Some(3)),
            ("amount", Some(4)),
            ("at", Some(5))
        ],
        "the dropped column is gone, its attnum isn't reused"
    );
    assert_eq!(
        orders.column("amount").unwrap().source_type,
        "numeric(10,2)"
    );
    assert_eq!(
        orders.column("at").unwrap().value_type,
        V::Timestamp { with_tz: true }
    );
    assert_eq!(
        r.tables[0].identifier(),
        ["code"],
        "REPLICA IDENTITY USING INDEX + override"
    );
    assert_eq!(
        r.tables[1].identifier(),
        ["id", "at"],
        "a leaf partition is a table"
    );

    let typed = &r.tables[3];
    let t: Vec<(&str, &V)> = typed
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), &c.value_type))
        .collect();
    assert_eq!(
        t,
        [
            ("id", &V::Int32),
            ("m", &V::Text),
            (
                "p",
                &V::Numeric {
                    precision: 10,
                    scale: 2
                }
            ),
            ("tags", &V::Array(Box::new(V::Text))),
            ("ms", &V::Array(Box::new(V::Text))),
            ("wide", &V::Text),
        ],
        "generated columns aren't in the stream, so they aren't in the schema"
    );
    assert_eq!(typed.column("wide").unwrap().logical, LogicalType::String);
    let wide = format!("{s}.typed.wide: numeric is read as text");
    assert!(r.warnings.contains(&wide), "{:?}", r.warnings);
    let missing = format!("tables.include {s}.missing matches no table");
    assert!(r.warnings.contains(&missing), "{:?}", r.warnings);
    assert!(
        !r.warnings.iter().any(|w| w.starts_with("V-106")),
        "a halted table's primary_key entry isn't stale: {:?}",
        r.warnings
    );

    let halted: Vec<(&str, &str)> = r
        .halted
        .iter()
        .map(|(t, h)| (t.name.as_str(), h.code()))
        .collect();
    assert_eq!(
        halted,
        [
            ("cache_named", "TABLE_NOT_REPLICABLE"),
            ("nopk_named", "NO_IDENTIFIER"),
            ("nothing", "IDENTIFIER_NOT_IN_REPLICA_IDENTITY"),
            ("reserved", "RESERVED_COLUMN_COLLISION"),
        ]
    );
    assert_eq!(
        r.halted[2].1,
        HaltReason::IdentifierNotInReplicaIdentity {
            missing: vec!["id".into()]
        }
    );
    let skipped: Vec<&str> = r.skipped.iter().map(|(t, _)| t.name.as_str()).collect();
    assert_eq!(
        skipped,
        ["cache", "events", "nopk"],
        "a glob-matched unlogged table, a partitioned parent and a glob-matched PK-less table"
    );
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn unconstrained_numerics_under_error_warn_v079() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "v079",
        "CREATE TABLE t (id int PRIMARY KEY, n numeric, w numeric(50,2));",
    )
    .await;
    let c = connector(&pipeline(
        &n,
        &["*"],
        "      numeric_overflow_action: error",
        "",
    ));
    let r = resolved(&c).await;
    assert!(r.tables.is_empty());
    assert_eq!(
        r.halted,
        [(
            n.table("t"),
            HaltReason::NumericOverflow { column: "w".into() }
        )],
        "numeric(50,2) can't be a decimal: the table halts"
    );
    let c = connector(&pipeline(
        &n,
        &["*"],
        &format!(
            "      numeric_overflow_action: error\n      column_type_overrides: {{ {}.t.w: \"decimal(38,2)\" }}",
            n.schema
        ),
        "",
    ));
    let r = resolved(&c).await;
    assert_eq!(
        r.tables[0].column("n").unwrap().value_type,
        V::Numeric {
            precision: 38,
            scale: 18
        }
    );
    assert!(
        r.warnings.iter().any(|w| w.starts_with("V-079")),
        "{:?}",
        r.warnings
    );
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn the_current_position_moves_with_writes() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "position", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let before = c.current_position().await.unwrap();
    a.execute(&format!("INSERT INTO {}.t VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    let after = c.current_position().await.unwrap();
    let (SourcePosition::Pg(b), SourcePosition::Pg(f)) = (before, after) else {
        panic!("PostgreSQL positions")
    };
    assert!(f > b, "{f} after {b}");
    cleanup(&a, &n).await;
}
