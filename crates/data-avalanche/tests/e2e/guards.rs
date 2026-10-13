//! Scenario 10, the guards: the WAL a slot retains, the open transaction's buffer, a slot whose
//! WAL is gone, and a source that stops answering.

use std::process::{Command, Stdio};
use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, admin, eventually, pg_port};

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

/// The server under test, frozen (`docker pause`) until dropped, on every path out of the test:
/// a server that keeps its connections open and never answers.
struct Frozen(String);

impl Frozen {
    fn new() -> Self {
        // test-infra/compose.yaml: project `data-avalanche-test`, `postgres-<major>` on 554<major>.
        let name = format!("data-avalanche-test-postgres-{}-1", pg_port() - 55400);
        docker("pause", &name);
        Self(name)
    }
}

impl Drop for Frozen {
    fn drop(&mut self) {
        docker("unpause", &self.0);
    }
}

fn docker(verb: &str, container: &str) {
    let status = Command::new("docker")
        .args([verb, container])
        .stdout(Stdio::null())
        .status()
        .expect("docker runs");
    assert!(status.success(), "docker {verb} {container}: {status}");
}

/// The reconnects counter's value.
fn reconnects(metrics: &str) -> u64 {
    metrics
        .lines()
        .find_map(|l| {
            l.strip_prefix("data_avalanche_source_reconnects_total{")
                .and_then(|rest| rest.rsplit_once(' '))
                .map(|(_, v)| v.parse().expect("a count"))
        })
        .expect("the reconnects series")
}

/// A source that stops answering (DA-SRC-16): the stream goes silent and the heartbeat's reads
/// time out, so the session ends and the pipeline reconnects, again and again while the server
/// is frozen; once it answers, the pipeline streams again.
#[tokio::test]
async fn a_source_that_stops_answering_is_reconnected_to() {
    require_it!();
    let e = E2e::new(
        "guard_frozen",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            source: "timeouts: { connect: 2s, command: 2s }",
            postgres: ", heartbeat: 1s",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'before')").await;
    e.mirrored("items", &["id", "doc"]).await;
    assert_eq!(reconnects(&p.metrics().await), 0);
    {
        let _frozen = Frozen::new();
        eventually("a reconnect", Duration::from_secs(60), async || {
            reconnects(&p.metrics().await) >= 1
        })
        .await;
        p.metric(&e.status("running")).await;
    }
    e.exec("INSERT INTO items VALUES (2, 'after')").await;
    e.mirrored("items", &["id", "doc"]).await;
    assert!(
        p.log()
            .contains("reconnecting to the source: the source isn't answering"),
        "{}",
        p.log()
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
