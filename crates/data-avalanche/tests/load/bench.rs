//! D-V1.3's lag targets at the product's defaults (a 30 s flush, a 15 s lease renewed every 5 s):
//! p99 of `data_avalanche_replication_lag_seconds` at most 60 s in steady state, and the lag back
//! under 60 s within 2 minutes of the holder's loss. And MERGE's cost under random-key churn on a
//! large table (R-5, D-MS.15), measured, with no target: D-V1.3 excludes it.

use std::time::{Duration, Instant};

use crate::harness::{E2e, Options, eventually};
use crate::nodes::Cluster;
use crate::report::{Samples, histogram, knob, lag, record};
use crate::workload::Workload;
use crate::{lands, require_load, snapshotted};

const RUN: &str = "bench";
/// Before the lag is sampled: the writer's first flushes.
const WARM_UP: Duration = Duration::from_secs(60);
/// D-V1.3.
const STEADY_P99: f64 = 60.0;
const FAILOVER: Duration = Duration::from_secs(120);

fn rate() -> u32 {
    u32::try_from(knob("DATA_AVALANCHE_LOAD_RATE", 1000)).expect("a rate")
}

/// Four tables of `DATA_AVALANCHE_LOAD_KEYS` keys (100 000) each, snapshotted, then changed at
/// `DATA_AVALANCHE_LOAD_RATE` changes a second (1000) for `DATA_AVALANCHE_LOAD_MINUTES` (30)
/// after a minute's warm-up, the lag sampled every second.
#[tokio::test]
async fn steady_state_lag_at_the_defaults() {
    require_load!();
    let w = Workload::new(4, knob("DATA_AVALANCHE_LOAD_KEYS", 100_000), rate());
    let window = Duration::from_secs(60 * knob("DATA_AVALANCHE_LOAD_MINUTES", 30));
    let e = E2e::new(
        "load_steady",
        &w.ddl(),
        Options {
            tables: ", snapshots: { mode: initial }",
            defaults: true,
            ..Options::default()
        },
    )
    .await;
    w.prefill(&e).await;
    let mut p = e.start(&[]);
    p.ready().await;
    snapshotted(&e, &w.tables, Duration::from_secs(1800)).await;
    let writer = w.start(&e);
    tokio::time::sleep(WARM_UP).await;
    let lag = Samples::take(&p, &e.test, window).await;
    let counts = writer.stop().await;
    let p99 = lag.quantile(0.99);
    let secs = (WARM_UP + window).as_secs_f64();
    record(RUN, "steady_lag_p50", lag.quantile(0.5), "s", None);
    record(
        RUN,
        "steady_lag_p99",
        p99,
        "s",
        Some(("<= 60", p99 <= STEADY_P99)),
    );
    record(RUN, "steady_lag_max", lag.quantile(1.0), "s", None);
    record(RUN, "steady_lag_samples", lag.0.len() as f64, "", None);
    record(
        RUN,
        "steady_rate",
        counts.changes as f64 / secs,
        "changes/s",
        None,
    );
    record(RUN, "steady_failed", counts.failed as f64, "", None);
    assert!(counts.changes > 0, "steady: the writer wrote nothing");
    assert_eq!(
        counts.failed, 0,
        "steady: {} statements failed",
        counts.failed
    );
    let achieved = counts.changes as f64 / secs;
    assert!(
        achieved >= 0.8 * rate() as f64,
        "steady: {achieved} changes/s achieved of {} asked",
        rate()
    );
    let n = lag.0.len() as u64;
    assert!(
        n >= window.as_secs() * 9 / 10,
        "steady: {n} lag samples over {window:?}"
    );
    for t in &w.tables {
        lands(&e, "lake", t, Duration::from_secs(300)).await;
    }
    assert!(p99 <= STEADY_P99, "p99 lag {p99} s over {STEADY_P99} s");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// One table of `DATA_AVALANCHE_CHURN_KEYS` keys (1 000 000), its rows changed at random keys over
/// the whole key space for `DATA_AVALANCHE_CHURN_MINUTES` (10): each MERGE touches most of the
/// table's files. Its commit time and what each commit rewrites, measured.
#[tokio::test]
async fn merge_cost_under_random_key_churn() {
    require_load!();
    let w = Workload::new(1, knob("DATA_AVALANCHE_CHURN_KEYS", 1_000_000), rate());
    let window = Duration::from_secs(60 * knob("DATA_AVALANCHE_CHURN_MINUTES", 10));
    let e = E2e::new(
        "load_churn",
        &w.ddl(),
        Options {
            tables: ", snapshots: { mode: initial }",
            defaults: true,
            ..Options::default()
        },
    )
    .await;
    w.prefill(&e).await;
    let mut p = e.start(&[]);
    p.ready().await;
    snapshotted(&e, &w.tables, Duration::from_secs(3600)).await;
    let table = &w.tables[0];
    let copied = e.log(table).await.len();
    let commit = "data_avalanche_sink_commit_duration_seconds";
    let (s0, c0) = histogram(&p, commit, &e.test).await;
    let writer = w.start(&e);
    tokio::time::sleep(window).await;
    let counts = writer.stop().await;
    assert!(counts.changes > 0, "churn: the writer wrote nothing");
    assert_eq!(
        counts.failed, 0,
        "churn: {} statements failed",
        counts.failed
    );
    lands(&e, "lake", table, Duration::from_secs(600)).await;
    let (s1, c1) = histogram(&p, commit, &e.test).await;
    let log = e.log(table).await;
    let size = |actions: &[serde_json::Value], kind: &str| -> (f64, f64) {
        actions
            .iter()
            .filter_map(|a| a.get(kind))
            .fold((0.0, 0.0), |(n, b), f| {
                (n + 1.0, b + f["size"].as_f64().unwrap_or(0.0))
            })
    };
    let table_bytes: f64 = log[..copied].iter().map(|c| size(c, "add").1).sum();
    let merges: Vec<_> = log[copied..].iter().collect();
    let n = merges.len().max(1) as f64;
    let written: f64 = merges.iter().map(|c| size(c, "add").1).sum();
    let removed: f64 = merges.iter().map(|c| size(c, "remove").0).sum();
    record(RUN, "churn_table_bytes", table_bytes, "B", None);
    record(RUN, "churn_merges", merges.len() as f64, "", None);
    record(
        RUN,
        "churn_commit_seconds_mean",
        (s1 - s0) / (c1 - c0).max(1.0),
        "s",
        None,
    );
    record(RUN, "churn_bytes_written_per_merge", written / n, "B", None);
    record(RUN, "churn_files_removed_per_merge", removed / n, "", None);
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// Three nodes at the product's defaults, two tables under load: the holder is killed, and a row
/// written just after must land, and the new holder's lag be under 60 s, within 2 minutes.
#[tokio::test]
async fn a_failover_recovers_within_two_minutes() {
    require_load!();
    let w = Workload::new(2, knob("DATA_AVALANCHE_LOAD_KEYS", 100_000), rate());
    let ddl = format!(
        "{} CREATE TABLE sentinel (id bigint PRIMARY KEY, doc text);",
        w.ddl()
    );
    let mut c = Cluster::with(
        "load_failover",
        &ddl,
        "",
        Options {
            tables: ", snapshots: { mode: initial }",
            defaults: true,
            ..Options::default()
        },
    )
    .await;
    w.prefill(&c.e).await;
    c.start_all().await;
    snapshotted(&c.e, &w.tables, Duration::from_secs(1800)).await;
    let writer = w.start(&c.e);
    tokio::time::sleep(WARM_UP).await;
    let test = c.e.test.clone();
    let (victim, epoch) = c.holders().await[&test].clone();
    let i = c.index(&victim);
    let t0 = Instant::now();
    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
    c.e.exec("INSERT INTO sentinel VALUES (1, 'after the kill')")
        .await;
    eventually("the sentinel lands", Duration::from_secs(600), async || {
        c.e.ids("sentinel").await == [1]
    })
    .await;
    let landed = t0.elapsed();
    let recovered = loop {
        if let Some((holder, e)) = c.holders().await.get(&test).cloned()
            && holder != victim
            && e > epoch
            && lag(c.proc(&holder), &test)
                .await
                .is_some_and(|l| l <= STEADY_P99)
        {
            break t0.elapsed();
        }
        assert!(t0.elapsed() < Duration::from_secs(600), "no recovery");
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let counts = writer.stop().await;
    assert!(counts.changes > 0, "failover: the writer wrote nothing");
    assert_eq!(
        counts.failed, 0,
        "failover: {} statements failed",
        counts.failed
    );
    record(RUN, "failover_landed", landed.as_secs_f64(), "s", None);
    record(
        RUN,
        "failover_recovered",
        recovered.as_secs_f64(),
        "s",
        Some(("<= 120", recovered <= FAILOVER)),
    );
    c.settled().await;
    for t in &w.tables {
        lands(&c.e, "lake", t, Duration::from_secs(300)).await;
    }
    assert!(recovered <= FAILOVER, "recovered after {recovered:?}");
    c.stop().await;
}
