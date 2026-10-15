//! Channel A against the target server: the globs select the table set, identifiers decide what
//! halts and what is skipped, and each column plans per `Type_Mapping.md` (config §11.2).

use data_avalanche_core::{ColumnValueType as V, HaltReason, TableRef};
use data_avalanche_source_mysql::Flavour;
use data_avalanche_testkit::require_it;

use crate::common::{cleanup, connector, fresh, pipeline, target};

#[tokio::test]
async fn globs_and_identifiers_decide_the_table_set() {
    require_it!();
    let (n, _a) = fresh(
        "resolve_set",
        "CREATE TABLE orders (id INT PRIMARY KEY);
         CREATE TABLE order_items (id INT PRIMARY KEY);
         CREATE TABLE order_log (msg TEXT);
         CREATE TABLE audit (msg TEXT);
         CREATE TABLE keyed (k INT NOT NULL, v INT);
         CREATE VIEW order_view AS SELECT id FROM orders;",
    )
    .await;
    let p = pipeline(
        &n,
        &["order*", "audit", "keyed", "nothing"],
        "      exclude: [order_items]\n      primary_key: { keyed: [k], gone: [id] }",
        "",
    );
    let r = connector(&p).resolve_tables().await.expect("resolve");
    let t = |name: &str| TableRef::new(&n.db, name);
    let names: Vec<_> = r.tables.iter().map(|s| s.table().clone()).collect();
    assert_eq!(names, [t("keyed"), t("orders")], "a view isn't a table");
    assert_eq!(r.halted, [(t("audit"), HaltReason::NoIdentifier)]);
    assert_eq!(r.skipped.len(), 1);
    assert_eq!(r.skipped[0].0, t("order_log"));
    let keyed = r.tables.iter().find(|s| s.table() == &t("keyed")).unwrap();
    assert_eq!(keyed.identifier(), ["k"], "the tables.primary_key entry");
    assert!(
        r.warnings
            .iter()
            .any(|w| w.contains("nothing matches no table")),
        "{:?}",
        r.warnings
    );
    assert!(
        r.warnings
            .iter()
            .any(|w| w.starts_with("V-106") && w.contains("gone")),
        "{:?}",
        r.warnings
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn columns_plan_per_the_type_table() {
    require_it!();
    let (n, _a) = fresh(
        "resolve_types",
        "CREATE TABLE t (
            id BIGINT UNSIGNED NOT NULL PRIMARY KEY, b TINYINT(1), ti TINYINT, siu SMALLINT UNSIGNED,
            mi MEDIUMINT, iu INT UNSIGNED, f FLOAT, d DOUBLE, dec1 DECIMAL(10,2),
            c CHAR(3) CHARACTER SET latin1, vc VARCHAR(10), vb VARBINARY(4), bn BINARY(4),
            dt DATE, tm TIME(3), dtm DATETIME(6), ts TIMESTAMP NULL, yr YEAR,
            en ENUM('a','b'), st SET('x','y'), bt BIT(3), g POINT)",
    )
    .await;
    let r = connector(&pipeline(&n, &["t"], "", ""))
        .resolve_tables()
        .await
        .unwrap();
    let s = &r.tables[0];
    let types: Vec<(String, V, bool)> = s
        .columns()
        .iter()
        .map(|c| (c.name.clone(), c.value_type.clone(), c.nullable))
        .collect();
    let want = [
        ("id", V::UInt64, false),
        ("b", V::Bool, true),
        ("ti", V::Int16, true),
        ("siu", V::Int32, true),
        ("mi", V::Int32, true),
        ("iu", V::Int64, true),
        ("f", V::Float32, true),
        ("d", V::Float64, true),
        (
            "dec1",
            V::Numeric {
                precision: 10,
                scale: 2,
            },
            true,
        ),
        ("c", V::Text, true),
        ("vc", V::Text, true),
        ("vb", V::Bytes, true),
        ("bn", V::Bytes, true),
        ("dt", V::Date, true),
        ("tm", V::Time, true),
        ("dtm", V::Timestamp { with_tz: false }, true),
        ("ts", V::Timestamp { with_tz: true }, true),
        ("yr", V::Int32, true),
        ("en", V::Text, true),
        ("st", V::Array(Box::new(V::Text)), true),
        ("bt", V::Bytes, true),
        ("g", V::Bytes, true),
    ];
    let want: Vec<(String, V, bool)> = want.into_iter().map(|(a, b, c)| (a.into(), b, c)).collect();
    assert_eq!(types, want);
    // MySQL drops the display width, MariaDB keeps it: `smallint(5) unsigned`.
    let siu = &s.column("siu").unwrap().source_type;
    assert!(
        siu.starts_with("smallint") && siu.ends_with(" unsigned"),
        "{siu}"
    );
    assert_eq!(s.column("tm").unwrap().source_ordinal, Some(15));
    // tinyint1_as_bool off: TINYINT(1) is an integer.
    let r = connector(&pipeline(&n, &["t"], "", ", tinyint1_as_bool: false"))
        .resolve_tables()
        .await
        .unwrap();
    assert_eq!(r.tables[0].column("b").unwrap().value_type, V::Int16);
    cleanup(&n).await;
}

#[tokio::test]
async fn overrides_charsets_wide_decimals_and_reserved_names() {
    require_it!();
    let (n, _a) = fresh(
        "resolve_halts",
        "CREATE TABLE ok (id INT PRIMARY KEY, d DATE, i INT);
         CREATE TABLE sjis (id INT PRIMARY KEY, s VARCHAR(5) CHARACTER SET sjis);
         CREATE TABLE wide (id INT PRIMARY KEY, w DECIMAL(65,30));
         CREATE TABLE reserved (id INT PRIMARY KEY, _op INT);
         CREATE TABLE fkey (id INT PRIMARY KEY, f DOUBLE NOT NULL);",
    )
    .await;
    let tables = "      column_type_overrides: { ok.d: string, ok.i: int64 }\n      primary_key: { fkey: [f] }\n      numeric_overflow_action: error";
    let r = connector(&pipeline(
        &n,
        &["ok", "sjis", "wide", "reserved", "fkey"],
        tables,
        "",
    ))
    .resolve_tables()
    .await
    .unwrap();
    let ok = r.tables.iter().find(|s| s.table().name == "ok").unwrap();
    assert_eq!(ok.column("d").unwrap().value_type, V::Text);
    assert_eq!(ok.column("i").unwrap().value_type, V::Int64);
    let halted = |name: &str| {
        r.halted
            .iter()
            .find(|(t, _)| t.name == name)
            .map(|(_, h)| h.code())
    };
    assert_eq!(halted("sjis"), Some("SCHEMA_MISMATCH"));
    assert_eq!(
        halted("wide"),
        Some("NUMERIC_OVERFLOW"),
        "numeric_overflow_action: error"
    );
    assert_eq!(halted("reserved"), Some("RESERVED_COLUMN_COLLISION"));
    assert_eq!(
        halted("fkey"),
        Some("IDENTIFIER_NOT_USABLE"),
        "a floating key"
    );
    // The string action keeps the wide decimal as text, with a warning.
    let r = connector(&pipeline(
        &n,
        &["wide"],
        "      numeric_overflow_action: string",
        "",
    ))
    .resolve_tables()
    .await
    .unwrap();
    assert_eq!(r.tables[0].column("w").unwrap().value_type, V::Text);
    assert!(
        r.warnings.iter().any(|w| w.contains("decimal(65,30)")),
        "{:?}",
        r.warnings
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn mariadb_types_and_system_versioned_tables() {
    require_it!();
    if target().flavour != Flavour::Mariadb {
        eprintln!("skipped: MariaDB only");
        return;
    }
    let (n, _a) = fresh(
        "resolve_maria",
        "CREATE TABLE m (id INT PRIMARY KEY, u UUID, i4 INET4, i6 INET6, j JSON);
         CREATE TABLE hist (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING;",
    )
    .await;
    let r = connector(&pipeline(&n, &["m", "hist"], "", ""))
        .resolve_tables()
        .await
        .unwrap();
    let m = &r.tables[0];
    assert_eq!(m.column("u").unwrap().value_type, V::Uuid);
    assert_eq!(m.column("i4").unwrap().value_type, V::Text);
    assert_eq!(m.column("i6").unwrap().value_type, V::Text);
    assert_eq!(
        m.column("j").unwrap().value_type,
        V::Text,
        "MariaDB JSON is LONGTEXT"
    );
    assert_eq!(r.halted.len(), 1);
    assert_eq!(r.halted[0].1.code(), "TABLE_NOT_REPLICABLE");
    let r = connector(&pipeline(&n, &["'*'"], "", ""))
        .resolve_tables()
        .await
        .unwrap();
    assert_eq!(r.skipped.len(), 1, "a glob skips it");
    cleanup(&n).await;
}
