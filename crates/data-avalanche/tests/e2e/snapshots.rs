//! Scenario 1, the snapshot modes (config §8.2): what each start snapshots, against the state and
//! the lake it finds.

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
     INSERT INTO items VALUES (1, 'a'), (2, 'b');";

async fn world(test: &str, mode: &str) -> E2e {
    let tables = format!(", snapshots: {{ mode: {mode} }}");
    E2e::new(
        test,
        ITEMS,
        Options {
            tables: &tables,
            ..Options::default()
        },
    )
    .await
}

/// `initial` snapshots an empty sink once; a restart with its state streams on, the snapshot
/// document untouched.
#[tokio::test]
async fn initial_snapshots_once() {
    require_it!();
    let e = world("snap_initial", "initial").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    let first = e.completed("items", None).await;
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    p.metric(&e.status("running")).await;
    assert_eq!(e.snapshot("items").await, Some(first), "no second snapshot");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `always` snapshots at every start, also on an idle source whose stream frontier the sink's
/// commit marker covers already: the snapshot's rows are committed, not skipped as a replay.
#[tokio::test]
async fn always_snapshots_at_every_start() {
    require_it!();
    let e = world("snap_always", "always").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    let first = e.completed("items", None).await;
    assert!(p.terminate().await.success());
    let commits = e.commits("items").await;

    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", Some(&first)).await;
    assert!(
        e.commits("items").await > commits,
        "the idle source's snapshot chunk is committed"
    );
    assert_eq!(e.ids("items").await, [1, 2]);
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `never` streams only: the rows already there stay out of the lake.
#[tokio::test]
async fn never_streams_only() {
    require_it!();
    let e = world("snap_never", "never").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'A' WHERE id = 1")
        .await;
    crate::harness::eventually(
        "the streamed rows land",
        std::time::Duration::from_secs(60),
        async || e.ids("items").await == [1, 3],
    )
    .await;
    assert!(
        e.snapshot("items")
            .await
            .is_none_or(|s| s.completed_at.is_none())
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `dump_always` dumps and stops at every start.
#[tokio::test]
async fn dump_always_dumps_at_every_start() {
    require_it!();
    let e = world("snap_dump_always", "dump_always").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(e.ids("items").await, [1, 2]);
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(e.ids("items").await, [1, 2, 3], "dumped again");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `dump_initial` dumps once; with its state and the lake table it stops at once, and with the
/// table gone from the lake it dumps again.
#[tokio::test]
async fn dump_initial_dumps_until_the_lake_has_it() {
    require_it!();
    let e = world("snap_dump_initial", "dump_initial").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(e.ids("items").await, [1, 2]);
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    let commits = e.commits("items").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(e.ids("items").await, [1, 2], "not dumped again");
    assert_eq!(e.commits("items").await, commits);
    assert!(p.terminate().await.success());

    e.drop_lake_table("items").await;
    assert_eq!(e.ids("items").await, Vec::<i64>::new());
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(
        e.ids("items").await,
        [1, 2, 3],
        "the lost table is dumped again"
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
