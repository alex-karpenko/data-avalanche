//! The soak (roadmap §4 M12c: 24 h with churn): three nodes, three pipelines over three tables,
//! the writer at a fifth of the bench's rate, and every `DATA_AVALANCHE_SOAK_CHURN_MINUTES` (10)
//! one change in turn: a column added, the test's pipeline paused and resumed, a node's leave and
//! restart, a snapshot job, a node's `kill -9` and restart. Every
//! `DATA_AVALANCHE_SOAK_CHECK_MINUTES` (60) the writer stops, every lake must hold the source's
//! rows, and `n1`'s resident memory is read: `n1` is never restarted, so its memory over the run is
//! the leak check (at most twice the first reading).

use std::time::{Duration, Instant};

use crate::harness::{Options, eventually};
use crate::nodes::{Cluster, lake};
use crate::report::{knob, record};
use crate::workload::Workload;
use crate::{lands, require_load, snapshotted};

const RUN: &str = "soak";

/// The test's pipeline's two peers over the same tables, at the product's defaults.
const PEERS: &str = r#"  b:
    id: b
@SOURCE(_b)@
    tables: { include: ['@INCLUDE@'], snapshots: { mode: initial } }
    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_b, delta: {} }
  c:
    id: c
@SOURCE(_c)@
    tables: { include: ['@INCLUDE@'], snapshots: { mode: initial } }
    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_c, delta: {} }
"#;

/// `pid`'s resident memory, in KiB (`ps`).
fn rss(pid: u32) -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .expect("ps runs");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("a resident size")
}

/// The table a request or a key names: the source's per-table config key.
fn key(c: &Cluster, table: &str) -> String {
    if c.e.source.is_postgres() {
        format!("{}.{table}", c.e.schema)
    } else {
        table.to_owned()
    }
}

/// Churn `step`, in turn of five.
async fn churn(c: &mut Cluster, w: &Workload, step: usize) {
    let test = c.e.test.clone();
    match step % 5 {
        0 => {
            c.e.exec(&format!(
                "ALTER TABLE {} ADD COLUMN extra_{step} text",
                w.tables[step % w.tables.len()]
            ))
            .await;
        }
        1 => {
            let n1 = c.proc("n1");
            assert_eq!(n1.ask(&test, "pause").await.0, 202);
            n1.shows(&test, "paused").await;
            assert_eq!(n1.ask(&test, "resume").await.0, 202);
            n1.shows(&test, "running").await;
        }
        2 | 4 => {
            // n2 and n3 in turn; n1 stays up throughout (the memory check's node).
            let i = 1 + (step / 5) % 2;
            let p = c.nodes[i].proc.take().expect("a live node");
            if step % 5 == 2 {
                assert!(p.terminate().await.success(), "a leave");
            } else {
                assert!(!p.kill().await.success(), "a kill");
            }
            c.start(i).await;
            c.settled().await;
        }
        _ => {
            let body = serde_json::json!({ "tables": [key(c, &w.tables[0])] }).to_string();
            let path = format!("/v1/pipelines/{test}/snapshots");
            let n1 = c.proc("n1");
            let (code, _, text) = n1
                .send(None, "POST", &path, &body)
                .await
                .expect("the API answers");
            assert_eq!(code, 202, "{text}");
            let job: serde_json::Value = serde_json::from_str(&text).expect("a job");
            let id = job["job_id"].as_str().expect("its id").to_owned();
            eventually("the job ends", Duration::from_secs(1800), async || {
                n1.call("GET", &format!("/v1/snapshots/{id}")).await.1["status"] == "completed"
            })
            .await;
        }
    }
}

/// The writer stopped: every pipeline's lake holds the source's rows.
async fn check(c: &Cluster, w: &Workload) {
    c.settled().await;
    for id in c.ids.clone() {
        for t in &w.tables {
            lands(&c.e, &lake(&c.e.test, &id), t, Duration::from_secs(600)).await;
        }
    }
}

#[tokio::test]
async fn a_day_of_churn_loses_nothing() {
    require_load!();
    let total = Duration::from_secs(60 * knob("DATA_AVALANCHE_SOAK_MINUTES", 24 * 60));
    let churn_every = Duration::from_secs(60 * knob("DATA_AVALANCHE_SOAK_CHURN_MINUTES", 10));
    let check_every = Duration::from_secs(60 * knob("DATA_AVALANCHE_SOAK_CHECK_MINUTES", 60));
    let rate = u32::try_from(knob("DATA_AVALANCHE_LOAD_RATE", 1000) / 5).expect("a rate");
    let w = Workload::new(3, 50_000, rate);
    let mut c = Cluster::with(
        "load_soak",
        &w.ddl(),
        PEERS,
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
    let n1 = c.proc("n1").pid();
    let mut memory = vec![];
    let mut writer = w.start(&c.e);
    let start = Instant::now();
    let (mut next_churn, mut next_check) = (start + churn_every, start + check_every);
    let mut step = 0;
    let mut changes = 0;
    loop {
        let end = start + total;
        let at = next_churn.min(next_check).min(end);
        tokio::time::sleep_until(at.into()).await;
        let now = Instant::now();
        if now >= next_churn && now < end {
            churn(&mut c, &w, step).await;
            step += 1;
            next_churn += churn_every;
        }
        if now >= next_check || now >= end {
            changes += writer.stop().await.changes;
            check(&c, &w).await;
            let kib = rss(n1);
            memory.push(kib);
            record(RUN, "n1_rss", kib as f64 * 1024.0, "B", None);
            if now >= end {
                break;
            }
            writer = w.start(&c.e);
            next_check += check_every;
        }
    }
    record(RUN, "churn_steps", step as f64, "", None);
    record(RUN, "changes", changes as f64, "", None);
    let (first, last) = (memory[0], *memory.last().expect("a reading"));
    let grew = last as f64 / first as f64;
    record(RUN, "n1_rss_growth", grew, "x", Some(("<= 2", grew <= 2.0)));
    assert!(grew <= 2.0, "n1's memory {first} KiB → {last} KiB");
    c.stop().await;
}
