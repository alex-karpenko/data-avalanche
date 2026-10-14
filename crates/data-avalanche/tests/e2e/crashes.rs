//! Scenario 3, crash and replay: the process dies at each phase boundary (an abort at a crash
//! point, or `kill -9`), and the restart brings the lake to the source's rows, each key once.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, admin};

const COLS: &[&str] = &["id", "doc"];
const CRASH: &str = "DATA_AVALANCHE_E2E_CRASH";

/// A crash while a snapshot's chunks are being read: the restart resumes it and the rows changed
/// in between land too.
#[tokio::test]
async fn a_crash_mid_snapshot_resumes_it() {
    require_it!();
    let e = E2e::new(
        "crash_chunk",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, 5000) g;",
        Options {
            tables: ", snapshots: { mode: initial, chunk_size: 500 }",
            // One chunk ahead of the writer at most: the chunks before it are committed.
            sink: ", buffer_capacity_rows: 500",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[(CRASH, "chunk_emitted:6")]);
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    let cursor = e
        .snapshot("items")
        .await
        .and_then(|s| s.cursor)
        .expect("a persisted cursor");
    let part = e.ids("items").await.len();
    eprintln!("after the crash: {part} rows, {cursor:?}");
    assert!(
        cursor.rows_done >= 2000 && part as u64 >= cursor.rows_done && part < 5000,
        "{part} rows, {cursor:?}"
    );

    e.exec(
        "UPDATE items SET doc = 'changed' WHERE id % 7 = 0;
         DELETE FROM items WHERE id % 11 = 0;
         INSERT INTO items VALUES (6000, 'new')",
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", None).await;
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A crash after a commit, before its checkpoint: the restart reads the transaction again, and
/// the position guard skips it: the table's marker covers the transaction's commit, though the
/// replay's flush may end past the marker (a keep-alive).
#[tokio::test]
async fn a_crash_between_a_commit_and_its_checkpoint_skips_the_replay() {
    require_it!();
    let e = E2e::new(
        "crash_commit",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[(CRASH, "sink_committed:1")]);
    p.ready().await;
    e.streaming().await;
    e.exec(
        "INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, 100) g;
         UPDATE items SET doc = 'changed' WHERE id % 3 = 0",
    )
    .await;
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    e.mirrored("items", COLS).await;
    let marker = e.marker("items").await;
    assert!(
        !e.acknowledges(&marker).await,
        "the crash came before the ack"
    );
    let commits = e.commits("items").await;

    let mut p = e.start(&[]);
    p.ready().await;
    e.acknowledged(&marker).await;
    e.mirrored("items", COLS).await;
    assert_eq!(
        e.commits("items").await,
        commits,
        "the replay is skipped, not merged again"
    );
    assert_eq!(e.ids("items").await, (1..=100).collect::<Vec<i64>>());
    e.exec("DELETE FROM items WHERE id > 50").await;
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A crash between the tables of one flush: one table has the transaction, the other doesn't
/// until the restart replays it.
#[tokio::test]
async fn a_crash_mid_multi_table_flush_completes_it_on_restart() {
    require_it!();
    let e = E2e::new(
        "crash_tables",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         CREATE TABLE other (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[(CRASH, "sink_committed:1")]);
    p.ready().await;
    e.streaming().await;
    e.exec(
        "BEGIN;
         INSERT INTO items VALUES (1, 'a'), (2, 'b');
         INSERT INTO other VALUES (1, 'x'), (2, 'y');
         COMMIT",
    )
    .await;
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    let landed = [e.ids("items").await.len(), e.ids("other").await.len()];
    assert!(
        landed == [2, 0] || landed == [0, 2],
        "one table committed: {landed:?}"
    );

    let mut p = e.start(&[]);
    p.ready().await;
    e.mirrored("items", COLS).await;
    e.mirrored("other", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A crash after the checkpoint, before the source is told: the source sends the transaction
/// again and it is skipped.
#[tokio::test]
async fn a_crash_between_the_checkpoint_and_the_ack_skips_the_resent_transaction() {
    require_it!();
    let e = E2e::new(
        "crash_ack",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    // The slot is made first, and the transaction is in the WAL before the process with the
    // crash point starts: its first checkpoint is the transaction's. On an idle stream, a
    // keep-alive alone would move the frontier and persist a checkpoint first.
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    assert!(p.terminate().await.success());
    e.exec("INSERT INTO items VALUES (1, 'a'), (2, 'b')").await;
    let mut p = e.start(&[(CRASH, "checkpoint_persisted:1")]);
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    e.mirrored("items", COLS).await;
    let marker = e.marker("items").await;
    assert!(
        !e.acknowledges(&marker).await,
        "the crash came before the ack"
    );
    let commits = e.commits("items").await;

    let mut p = e.start(&[]);
    p.ready().await;
    e.exec("UPDATE items SET doc = 'B' WHERE id = 2").await;
    e.mirrored("items", COLS).await;
    assert_eq!(
        e.commits("items").await,
        commits + 1,
        "only the new transaction is committed"
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `kill -9` while a writer keeps changing rows: the restart catches up, each key once.
#[tokio::test]
async fn a_kill_under_load_loses_nothing() {
    require_it!();
    let e = E2e::new(
        "crash_kill",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, 200) g;",
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn(write_load(e.schema.clone(), Arc::clone(&stop)));
    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", None).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let status = p.kill().await;
    assert!(!status.success(), "{status}");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut p = e.start(&[]);
    p.ready().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::SeqCst);
    let ops = writer.await.expect("the writer");
    assert!(ops > 100, "the writer ran: {ops} statements");
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// Updates, deletes and inserts on keys 1–300 until `stop`, one statement per transaction;
/// returns how many ran.
async fn write_load(schema: String, stop: Arc<AtomicBool>) -> u64 {
    let c = admin().await;
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut ops = 0;
    while !stop.load(Ordering::SeqCst) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let id = (x % 300 + 1) as i64;
        let sql = match x % 3 {
            0 => format!("UPDATE {schema}.items SET doc = 'u{ops}' WHERE id = $1"),
            1 => format!("DELETE FROM {schema}.items WHERE id = $1"),
            _ => format!(
                "INSERT INTO {schema}.items VALUES ($1, 'i{ops}') ON CONFLICT (id) DO UPDATE SET doc = excluded.doc"
            ),
        };
        c.execute(&sql, &[&id]).await.expect("a load statement");
        ops += 1;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    ops
}
