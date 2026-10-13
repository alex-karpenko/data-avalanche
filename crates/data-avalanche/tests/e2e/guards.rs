//! Scenario 10, the guards: the WAL a slot retains, the open transaction's buffer, and a slot
//! whose WAL is gone.

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, admin};

const TABLES: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
     CREATE TABLE pad (id bigserial PRIMARY KEY, doc text);";

/// Writes WAL and switches to a new segment, `n` times: each moves the WAL end by up to 16 MiB.
async fn switch_wal(e: &E2e, n: usize) -> Result<(), tokio_postgres::Error> {
    for _ in 0..n {
        e.admin
            .batch_execute(&format!(
                "INSERT INTO {}.pad (doc) VALUES ('x'); SELECT pg_switch_wal()",
                e.schema
            ))
            .await?;
    }
    Ok(())
}

/// An open transaction pins the slot while the WAL grows past `max_wal_lag_bytes`: the pipeline
/// stops with an error, to protect the source's disk. The lag is read once per heartbeat, so the
/// heartbeat is short: at the default 60 s the first read could fall past the wait.
#[tokio::test]
async fn retained_wal_past_the_limit_stops_the_pipeline() {
    require_it!();
    let e = E2e::new(
        "guard_wal",
        TABLES,
        Options {
            tables: ", exclude: [e2e_guard_wal.pad], snapshots: { mode: never }",
            postgres: ", max_wal_lag_bytes: 64MiB, heartbeat: 5s",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    let held = admin().await;
    held.batch_execute(&format!(
        "BEGIN; INSERT INTO {}.items VALUES (1, 'held')",
        e.schema
    ))
    .await
    .expect("an open transaction");
    switch_wal(&e, 6).await.expect("WAL written");
    e.admin
        .batch_execute("CHECKPOINT")
        .await
        .expect("a checkpoint");
    p.metric(&e.status("error")).await;
    held.batch_execute("ROLLBACK").await.expect("rolled back");
    assert!(
        p.log().contains("over max_wal_lag_bytes 67108864"),
        "{}",
        p.log()
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A transaction larger than `max_transaction_buffer_bytes`, splitting off: the pipeline stops
/// with an error instead of growing past the ceiling, and nothing of it lands.
#[tokio::test]
async fn a_transaction_past_the_buffer_ceiling_stops_the_pipeline() {
    require_it!();
    let e = E2e::new(
        "guard_buffer",
        TABLES,
        Options {
            tables: ", snapshots: { mode: never }",
            sink: ", max_transaction_buffer_bytes: 16MiB",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items SELECT g, repeat('x', 300) FROM generate_series(1, 100000) g")
        .await;
    p.metric(&e.status("error")).await;
    assert!(
        p.log()
            .contains("over max_transaction_buffer_bytes 16777216"),
        "{}",
        p.log()
    );
    assert!(e.ids("items").await.is_empty());
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A slot PostgreSQL invalidated while the pipeline was down: the restart makes a new slot and
/// snapshots again, so the change whose WAL is gone lands too.
#[tokio::test]
async fn a_slot_whose_wal_is_gone_is_snapshotted_again() {
    require_it!();
    let e = E2e::new(
        "guard_lost",
        &format!("{TABLES} INSERT INTO items VALUES (1, 'a'), (2, 'b');"),
        Options {
            tables: ", exclude: [e2e_guard_lost.pad], snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    let first = e.completed("items", None).await;
    e.converged("items").await;
    assert!(p.terminate().await.success());
    e.exec("INSERT INTO items VALUES (3, 'c')").await;

    // A 1 MB allowance, WAL past it, a checkpoint: the slot is lost. Every step is fallible, so
    // the server-wide setting is restored whatever happens; the first reset undoes an earlier
    // run that died before its own. ALTER SYSTEM runs outside a transaction: one per call.
    let keep = async |size: Option<&str>| {
        let set = match size {
            None => "ALTER SYSTEM RESET max_slot_wal_keep_size".to_owned(),
            Some(s) => format!("ALTER SYSTEM SET max_slot_wal_keep_size = '{s}'"),
        };
        e.admin.batch_execute(&set).await?;
        e.admin.batch_execute("SELECT pg_reload_conf()").await
    };
    keep(None).await.expect("the default");
    let invalidate = async {
        keep(Some("1MB")).await?;
        switch_wal(&e, 3).await?;
        e.admin.batch_execute("CHECKPOINT").await?;
        e.admin
            .query_one(
                "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
                &[&e.schema],
            )
            .await
            .map(|r| r.get::<_, Option<String>>(0))
    }
    .await;
    keep(None).await.expect("the default restored");
    assert_eq!(
        invalidate.expect("the slot invalidated").as_deref(),
        Some("lost")
    );

    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", Some(&first)).await;
    e.converged("items").await;
    e.exec("INSERT INTO items VALUES (4, 'd')").await;
    e.converged("items").await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
