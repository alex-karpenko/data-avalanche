//! Scenario 2, streaming: row changes, transaction shapes, a large transaction kept atomic, and a
//! split transaction replayed after a crash between its slices.

use std::time::{Duration, Instant};

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

const COLS: &[&str] = &["id", "doc"];

/// Inserts, updates, deletes, a key-changing UPDATE, a transaction over two tables and a
/// transaction that changes no captured table, each mirrored.
#[tokio::test]
async fn row_changes_and_transaction_shapes_are_mirrored() {
    require_it!();
    let e = E2e::new(
        "stream_rows",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         CREATE TABLE other (id bigint PRIMARY KEY, doc text);
         CREATE TABLE ignored (id bigint PRIMARY KEY);",
        Options {
            tables: ", exclude: [e2e_stream_rows.ignored], snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;

    e.exec("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, NULL)")
        .await;
    e.mirrored("items", COLS).await;
    e.exec("UPDATE items SET doc = 'B' WHERE id = 2; UPDATE items SET doc = 'c' WHERE id = 3; DELETE FROM items WHERE id = 1")
        .await;
    e.mirrored("items", COLS).await;

    e.exec("UPDATE items SET id = 20 WHERE id = 2").await;
    e.mirrored("items", COLS).await;
    assert_eq!(e.ids("items").await, [3, 20], "the old key is gone");

    e.exec(
        "BEGIN;
         INSERT INTO other VALUES (1, 'x');
         INSERT INTO items VALUES (4, 'd');
         DELETE FROM items WHERE id = 3;
         UPDATE other SET doc = 'X' WHERE id = 1;
         COMMIT",
    )
    .await;
    e.mirrored("items", COLS).await;
    e.mirrored("other", COLS).await;

    // Changes only an excluded table: an empty transaction to the pipeline.
    e.exec("INSERT INTO ignored VALUES (1)").await;
    e.exec("INSERT INTO items VALUES (5, 'e')").await;
    e.mirrored("items", COLS).await;
    assert_eq!(e.content("ignored").await, None);
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A 50,000-row transaction is never seen in part: it lands in one Delta commit.
#[tokio::test]
async fn a_large_transaction_lands_at_once() {
    require_it!();
    let e = E2e::new(
        "stream_large",
        "CREATE TABLE bulk (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    let before = e.commits("bulk").await;
    e.exec("INSERT INTO bulk SELECT g, 'row ' || g FROM generate_series(1, 50000) g")
        .await;
    let until = Instant::now() + Duration::from_secs(120);
    loop {
        let seen = e.content("bulk").await.map_or(0, |b| b.num_rows());
        assert!(seen == 0 || seen == 50_000, "the lake shows {seen} rows");
        if seen == 50_000 {
            break;
        }
        assert!(Instant::now() < until, "the transaction never landed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    e.mirrored("bulk", COLS).await;
    assert_eq!(
        e.commits("bulk").await,
        before + 1,
        "the transaction is one commit"
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// With `max_transaction_batch_rows`, a larger transaction is committed in slices; a crash after
/// the second slice leaves it in part, and the restart replays it to the whole, once.
#[tokio::test]
async fn a_split_transaction_is_replayed_after_a_crash_between_slices() {
    require_it!();
    let e = E2e::new(
        "stream_split",
        "CREATE TABLE bulk (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            sink: ", max_transaction_batch_rows: 1000",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[("DATA_AVALANCHE_E2E_CRASH", "sink_committed:2")]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO bulk SELECT g, 'row ' || g FROM generate_series(1, 5000) g")
        .await;
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    assert_eq!(
        e.ids("bulk").await,
        (1..=2000).collect::<Vec<i64>>(),
        "two slices of five landed"
    );

    let mut p = e.start(&[]);
    p.ready().await;
    e.mirrored("bulk", COLS).await;
    assert_eq!(e.ids("bulk").await, (1..=5000).collect::<Vec<i64>>());
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
