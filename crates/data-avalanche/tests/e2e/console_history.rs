//! The chart history through any node (Admin_Console §7; M13e), on three nodes: a node that
//! doesn't run the test's pipeline reads its lag, its table's series and the holder's own series
//! from `globals.state`; the holder killed, the pipeline's history goes on under the next lease
//! term and keeps what the killed holder wrote.

use std::time::Duration;

use chrono::Utc;
use data_avalanche_testkit::require_it;
use serde_json::Value;

use crate::harness::{Doc, eventually};
use crate::nodes::Cluster;

/// The times of the points of series `name` in a history answer.
fn times(v: &Value, name: &str) -> Vec<i64> {
    v["series"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|s| s["name"] == name)
        .flat_map(|s| s["points"].as_array().cloned().unwrap_or_default())
        .filter_map(|p| p[0].as_i64())
        .collect()
}

#[tokio::test]
async fn a_pipeline_s_history_follows_it_across_its_nodes() {
    require_it!();
    let mut c = Cluster::new("console_history").await;
    c.start_all().await;
    let test = c.e.test.clone();
    c.e.insert_series("items", 5, Doc::Row).await;
    c.landed().await;
    let holders = c.holders().await;
    let holder = holders[&test].0.clone();
    let other = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != holder)
        .expect("another node")
        .to_owned();
    let items = if c.e.source.is_postgres() {
        format!("{}.items", c.e.schema)
    } else {
        "items".to_owned()
    };

    // The pipeline's lag and its table's series, written with their first samples, read through
    // a node that doesn't run it; the holder's own series too.
    let lag = format!("/v1/history?pipeline={test}&series=lag_seconds");
    let table = format!("/v1/history?pipeline={test}&table={items}");
    let node = format!("/v1/history?node={holder}&series=lease_renew_seconds");
    for (what, path, name) in [
        ("the lag", &lag, "lag_seconds"),
        ("the table's changes", &table, "changes_per_minute"),
        ("the holder's renewals", &node, "lease_renew_seconds"),
    ] {
        eventually(what, Duration::from_secs(60), async || {
            let (code, v) = c.proc(&other).call("GET", path).await;
            code == 200 && !times(&v, name).is_empty()
        })
        .await;
    }
    let (_, v) = c.proc(&other).call("GET", &table).await;
    let names: Vec<&str> = v["series"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["changes_per_minute", "files_rewritten", "commit_seconds"],
        "{v}"
    );
    assert_eq!(v["series"][0]["table"], items.as_str(), "{v}");

    // The holder killed: the next term's samples join what the killed holder wrote.
    let (_, before) = c.proc(&other).call("GET", &lag).await;
    let written = times(&before, "lag_seconds");
    assert!(
        !written.is_empty(),
        "the holder wrote lag samples before the kill: {before}"
    );
    let killed = Utc::now().timestamp();
    let i = c.index(&holder);
    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
    c.settled().await;
    let next = c.holders().await[&test].clone();
    assert_ne!(next.0, holder);
    // A point's time is its step's start: the next term's first sample may share the kill's step,
    // a point the killed holder hadn't written.
    eventually(
        "the next term's lag",
        Duration::from_secs(120),
        async || {
            let (code, v) = c.proc(&other).call("GET", &lag).await;
            let step = v["step"].as_i64().unwrap_or(15);
            code == 200
                && times(&v, "lag_seconds")
                    .iter()
                    .any(|t| *t > killed || (*t + step > killed && !written.contains(t)))
        },
    )
    .await;
    let (_, after) = c.proc(&other).call("GET", &lag).await;
    let all = times(&after, "lag_seconds");
    assert!(
        written.iter().all(|t| all.contains(t)),
        "the killed holder's samples are kept: {written:?} in {all:?}"
    );
    c.stop().await;
}
