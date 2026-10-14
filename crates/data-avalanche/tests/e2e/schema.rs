//! Scenario 4, schema changes on Delta (Schema_Change §7): what is applied, what degrades, what
//! halts the table, a TRUNCATE, and a DDL replayed after a crash.

use std::time::Duration;

use data_avalanche_core::HaltReason;
use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};

fn row(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| (*v).to_owned()).collect()
}

/// ADD COLUMN is applied; DROP COLUMN soft-drops (the column stays, later rows NULL); RENAME
/// adds the new name beside the old, which keeps the history; TRUNCATE clears the table with a DELETE that rewrites no file.
#[tokio::test]
async fn columns_evolve_and_a_truncate_clears_the_table() {
    require_it!();
    let e = E2e::new(
        "schema_columns",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text, old text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a', 'o')").await;
    e.mirrored("items", &["id", "doc", "old"]).await;

    e.exec("ALTER TABLE items ADD COLUMN extra int; INSERT INTO items VALUES (2, 'b', 'p', 7)")
        .await;
    e.mirrored("items", &["id", "doc", "old", "extra"]).await;

    e.exec("ALTER TABLE items DROP COLUMN old; INSERT INTO items VALUES (3, 'c', 8)")
        .await;
    e.mirrored("items", &["id", "doc", "extra"]).await;
    assert_eq!(
        e.rows("items", &["id", "old"]).await,
        [row(&["1", "o"]), row(&["2", "p"]), row(&["3", "NULL"])],
        "the dropped column stays"
    );

    e.exec("ALTER TABLE items RENAME COLUMN doc TO body; INSERT INTO items VALUES (4, 'd', 9)")
        .await;
    e.converged("items").await;
    assert_eq!(
        e.rows("items", &["id", "doc", "body"]).await,
        [
            row(&["1", "a", "NULL"]),
            row(&["2", "b", "NULL"]),
            row(&["3", "c", "NULL"]),
            row(&["4", "NULL", "d"]),
        ],
        "the history stays under the old name"
    );

    e.exec("TRUNCATE items; INSERT INTO items VALUES (10, 'z', 1)")
        .await;
    e.mirrored("items", &["id", "body", "extra"]).await;
    let log = e.log("items").await;
    let delete = log
        .iter()
        .find(|c| {
            c.iter()
                .any(|a| a["commitInfo"]["operation"].as_str() == Some("DELETE"))
        })
        .expect("a DELETE commit");
    assert!(
        delete.iter().all(|a| a.get("add").is_none()),
        "metadata-only: {delete:?}"
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A type change, a reserved column name and a dropped table each halt their table, and the
/// halts are kept across a restart; the other tables stream on.
#[tokio::test]
async fn unsafe_changes_halt_their_table_only() {
    require_it!();
    let e = E2e::new(
        "schema_halts",
        "CREATE TABLE widen (id bigint PRIMARY KEY, n int);
         CREATE TABLE reserved (id bigint PRIMARY KEY, doc text);
         CREATE TABLE dropped (id bigint PRIMARY KEY);
         CREATE TABLE fine (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            pipeline: "schema_evolution: { rescan_interval: 2s }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec(
        "INSERT INTO widen VALUES (1, 1); INSERT INTO reserved VALUES (1, 'a');
         INSERT INTO dropped VALUES (1); INSERT INTO fine VALUES (1, 'a')",
    )
    .await;
    for t in ["widen", "reserved", "dropped", "fine"] {
        e.converged(t).await;
    }

    e.exec("ALTER TABLE widen ALTER COLUMN n TYPE bigint; INSERT INTO widen VALUES (2, 2)")
        .await;
    e.exec("ALTER TABLE reserved ADD COLUMN _op text; INSERT INTO reserved VALUES (2, 'b', 'x')")
        .await;
    e.exec("DROP TABLE dropped").await;
    e.exec("INSERT INTO fine VALUES (2, 'b')").await;
    e.converged("fine").await;

    eventually("three tables halt", Duration::from_secs(60), async || {
        e.halts().await.tables.len() == 3
    })
    .await;
    let mut halts: Vec<(String, &str)> = e
        .halts()
        .await
        .tables
        .iter()
        .map(|h| (h.table.name.clone(), reason(&h.reason)))
        .collect();
    halts.sort();
    assert_eq!(
        halts,
        [
            ("dropped".into(), "TABLE_DROPPED"),
            ("reserved".into(), "RESERVED_COLUMN_COLLISION"),
            ("widen".into(), "EVOLUTION_UNSUPPORTED"),
        ]
    );
    let labels = r#"pipeline="schema_halts",source_host="127.0.0.1",source_db="avalanche""#;
    p.metric(&format!("data_avalanche_halted_tables{{{labels}}} 3"))
        .await;
    assert_eq!(e.ids("widen").await, [1], "nothing after the halt");
    assert_eq!(e.ids("reserved").await, [1]);
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO widen VALUES (3, 3); INSERT INTO fine VALUES (3, 'c')")
        .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("fine").await;
    p.metric(&format!("data_avalanche_halted_tables{{{labels}}} 3"))
        .await;
    assert_eq!(e.ids("widen").await, [1], "still halted after a restart");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

fn reason(r: &HaltReason) -> &'static str {
    match r {
        HaltReason::ReservedColumnCollision { .. } => "RESERVED_COLUMN_COLLISION",
        HaltReason::TableDropped => "TABLE_DROPPED",
        HaltReason::UnsafeDdl { .. } => "UNSAFE_DDL",
        HaltReason::EvolutionUnsupported { .. } => "EVOLUTION_UNSUPPORTED",
        _ => "OTHER",
    }
}

/// A crash after an ADD COLUMN's rows were committed, before the checkpoint: the restart reads
/// the DDL again and finds the column there.
#[tokio::test]
async fn a_ddl_replayed_after_a_crash_is_applied_once() {
    require_it!();
    let e = E2e::new(
        "schema_replay",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[("DATA_AVALANCHE_E2E_CRASH", "sink_committed:1")]);
    p.ready().await;
    e.streaming().await;
    e.exec("ALTER TABLE items ADD COLUMN extra int; INSERT INTO items VALUES (1, 'a', 7)")
        .await;
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    e.mirrored("items", &["id", "doc", "extra"]).await;

    let mut p = e.start(&[]);
    p.ready().await;
    e.exec("INSERT INTO items VALUES (2, 'b', 8)").await;
    e.mirrored("items", &["id", "doc", "extra"]).await;
    let schema_commits = e
        .log("items")
        .await
        .iter()
        .filter(|c| c.iter().any(|a| a.get("metaData").is_some()))
        .count();
    assert_eq!(schema_commits, 2, "the create and one ADD COLUMN");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
