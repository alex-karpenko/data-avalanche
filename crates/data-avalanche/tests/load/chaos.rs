//! Chaos under load, at the product's defaults (roadmap §4 M12c): a storage outage shorter than
//! the lease and one longer, a restart of the source's server (Q-M12c-3: `docker compose
//! restart`, in this opt-in run only), and a partition of the holder from the other nodes. Each
//! ends with the lake holding the source's rows, each key once, its owner epochs never going
//! back.

use std::time::{Duration, Instant};

use data_avalanche_testkit::endpoints::RUSTFS_URL;

use crate::harness::{Db, E2e, Options, Store, eventually};
use crate::nodes::Cluster;
use crate::proxy::Proxy;
use crate::report::{knob, record};
use crate::workload::Workload;
use crate::{lands, require_load, snapshotted};

const RUN: &str = "chaos";
/// The product's `lease_duration`.
const LEASE: Duration = Duration::from_secs(15);
/// Under load before the fault, and after it before the writer stops.
const AROUND: Duration = Duration::from_secs(30);

/// A quarter of the bench's rate, over two tables of 20 000 keys.
fn workload() -> Workload {
    let rate = u32::try_from(knob("DATA_AVALANCHE_LOAD_RATE", 1000) / 4).expect("a rate");
    Workload::new(2, 20_000, rate)
}

fn options() -> Options<'static> {
    Options {
        tables: ", snapshots: { mode: initial }",
        defaults: true,
        ..Options::default()
    }
}

/// A port nothing listens on.
fn dead_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("a port")
        .local_addr()
        .expect("its address")
        .port()
}

/// The node reaches RustFS through a proxy, which refuses every connection for `outage` under
/// load; what the lease's epoch was before and after.
async fn storage_outage(test: &str, outage: Duration) -> Option<(u64, u64)> {
    let w = workload();
    let e = E2e::new(test, &w.ddl(), options()).await;
    if e.store != Store::RustFs {
        eprintln!(
            "skipped: the storage outage runs on RustFS, not {:?}",
            e.store
        );
        e.cleanup().await;
        return None;
    }
    let rustfs: u16 = RUSTFS_URL
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .expect("RustFS's port");
    let proxy = Proxy::tcp(rustfs).await;
    let endpoint = format!("http://127.0.0.1:{}", proxy.port());
    w.prefill(&e).await;
    let mut p = e.start(&[("AWS_ENDPOINT_URL", &endpoint)]);
    p.ready().await;
    snapshotted(&e, &w.tables, Duration::from_secs(600)).await;
    let writer = w.start(&e);
    tokio::time::sleep(AROUND).await;
    let before = e.lease().await.expect("a lease").doc.owner_epoch;
    proxy.switch(dead_port());
    tokio::time::sleep(outage).await;
    proxy.switch(rustfs);
    let back = Instant::now();
    p.metric(&e.status("running")).await;
    record(
        RUN,
        &format!("{test}_running_again"),
        back.elapsed().as_secs_f64(),
        "s",
        None,
    );
    tokio::time::sleep(AROUND).await;
    let counts = writer.stop().await;
    assert!(counts.changes > 0, "{test}: the writer wrote nothing");
    assert_eq!(
        counts.failed, 0,
        "{test}: {} statements failed",
        counts.failed
    );
    for t in &w.tables {
        lands(&e, "lake", t, Duration::from_secs(300)).await;
    }
    let after = e.lease().await.expect("a lease").doc.owner_epoch;
    assert!(p.terminate().await.success());
    e.cleanup().await;
    Some((before, after))
}

/// Shorter than the lease: the node keeps its lease (the same epoch) and catches up.
#[tokio::test]
async fn a_storage_outage_shorter_than_the_lease_loses_nothing() {
    require_load!();
    let outage = LEASE / 3;
    let Some((before, after)) = storage_outage("chaos_store_short", outage).await else {
        return;
    };
    record(
        RUN,
        "short_outage_epochs",
        (after - before) as f64,
        "",
        None,
    );
    assert_eq!(after, before, "an outage of {outage:?} keeps the lease");
}

/// Longer than the lease: the node stops writing when its lease runs out, and claims it again,
/// at the next epoch, once the store answers.
#[tokio::test]
async fn a_storage_outage_longer_than_the_lease_loses_nothing() {
    require_load!();
    let outage = LEASE * 3;
    let Some((before, after)) = storage_outage("chaos_store_long", outage).await else {
        return;
    };
    record(RUN, "long_outage_epochs", (after - before) as f64, "", None);
    assert!(after > before, "an outage of {outage:?} loses the lease");
}

/// The source's server restarts under load (`docker compose restart`): the stream reconnects and
/// the writer's committed changes all land.
#[tokio::test]
async fn a_source_restart_loses_nothing() {
    require_load!();
    let w = workload();
    let ddl = format!(
        "{} CREATE TABLE sentinel (id bigint PRIMARY KEY, doc text);",
        w.ddl()
    );
    let mut e = E2e::new("chaos_source", &ddl, options()).await;
    w.prefill(&e).await;
    let mut p = e.start(&[]);
    p.ready().await;
    snapshotted(&e, &w.tables, Duration::from_secs(600)).await;
    let writer = w.start(&e);
    tokio::time::sleep(AROUND).await;
    let compose = concat!(env!("CARGO_MANIFEST_DIR"), "/../../test-infra/compose.yaml");
    let t = Instant::now();
    let status = std::process::Command::new("docker")
        .args(["compose", "-f", compose, "restart", &e.source.service()])
        .status()
        .expect("docker runs");
    assert!(status.success(), "the restart: {status}");
    // The harness's own connection went with the server; the restart returns before the server
    // takes connections again.
    e.db = loop {
        match Db::try_connect(e.source).await {
            Ok(db) => break db,
            Err(why) => {
                assert!(
                    t.elapsed() < Duration::from_secs(120),
                    "the server is back: {why}"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    record(RUN, "source_restart", t.elapsed().as_secs_f64(), "s", None);
    p.metric(&e.status("running")).await;
    e.streaming().await;
    // A write through the reconnected connection: it must reach the lake.
    e.exec("INSERT INTO sentinel VALUES (1, 'after the restart')")
        .await;
    tokio::time::sleep(AROUND).await;
    let counts = writer.stop().await;
    assert!(
        counts.changes > 0,
        "source restart: the writer wrote nothing"
    );
    record(
        RUN,
        "source_restart_failed_statements",
        counts.failed as f64,
        "",
        None,
    );
    for t in &w.tables {
        lands(&e, "lake", t, Duration::from_secs(300)).await;
    }
    eventually("the sentinel lands", Duration::from_secs(300), async || {
        e.ids("sentinel").await == [1]
    })
    .await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// Three nodes under load; the holder is cut off from the other two for four leases: the store
/// still answers it, so it keeps the pipeline (the same holder and epoch), and nothing is lost.
#[tokio::test]
async fn a_partition_under_load_loses_nothing() {
    require_load!();
    let w = workload();
    let mut c = Cluster::with("chaos_split", &w.ddl(), "", options()).await;
    w.prefill(&c.e).await;
    c.start_all().await;
    snapshotted(&c.e, &w.tables, Duration::from_secs(600)).await;
    let writer = w.start(&c.e);
    tokio::time::sleep(AROUND).await;
    let test = c.e.test.clone();
    let before = c.holders().await[&test].clone();
    c.cut(&[before.0.as_str()]);
    tokio::time::sleep(LEASE * 4).await;
    c.heal();
    c.settled().await;
    tokio::time::sleep(AROUND).await;
    let counts = writer.stop().await;
    assert!(counts.changes > 0, "partition: the writer wrote nothing");
    assert_eq!(
        counts.failed, 0,
        "partition: {} statements failed",
        counts.failed
    );
    let after = c.holders().await[&test].clone();
    record(
        RUN,
        "partition_epochs",
        (after.1 - before.1) as f64,
        "",
        None,
    );
    for t in &w.tables {
        lands(&c.e, "lake", t, Duration::from_secs(300)).await;
    }
    assert_eq!(after, before, "the partition moved the pipeline");
    c.stop().await;
}
