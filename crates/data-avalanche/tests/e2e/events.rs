//! The cluster's timeline (Admin_Console §6) on three nodes: a pause asked of one node with a
//! note is recorded with who asked and why; the holder is killed, and the takeover, naming the
//! node it took over from, and the node's going down are recorded, the latter by one node; any
//! live node reads it all back through `GET /v1/events`.

use std::time::Duration;

use data_avalanche_testkit::require_it;
use serde_json::Value;

use crate::harness::{Proc, eventually};
use crate::nodes::Cluster;

/// The events `query` selects, as `node` lists them.
async fn listed(node: &Proc, query: &str) -> Vec<Value> {
    let (code, v) = node.call("GET", &format!("/v1/events?{query}")).await;
    assert_eq!(code, 200, "{v}");
    v["events"].as_array().expect("events").clone()
}

#[tokio::test]
async fn the_timeline_records_who_paused_and_a_failover() {
    require_it!();
    let mut c = Cluster::new("events_timeline").await;
    c.start_all().await;
    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
    c.landed().await;
    let id = c.e.test.clone();
    let path = format!("/v1/pipelines/{id}/pause");
    let first = c.nodes[0].proc.as_ref().expect("a live node");
    let (code, _, body) = first
        .send(None, "POST", &path, r#"{"note": "the e2e pause"}"#)
        .await
        .expect("an answer");
    assert_eq!(code, 202, "{body}");
    first.shows(&id, "paused").await;
    assert_eq!(first.ask(&id, "resume").await.0, 202);
    first.shows(&id, "running").await;

    let victim = c.holders().await[&id].0.clone();
    let i = c.index(&victim);
    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
    c.settled().await;
    let reader = c.live()[0].proc.as_ref().expect("a live node");

    let actions = listed(reader, &format!("kind=action&object={id}")).await;
    let kinds: Vec<&str> = actions
        .iter()
        .map(|e| e["kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        ["action.pipeline.resume", "action.pipeline.pause"],
        "newest first"
    );
    let pause = &actions[1];
    assert_eq!(pause["actor"]["name"], "anonymous");
    assert_eq!(pause["note"], "the e2e pause");
    assert_eq!(pause["outcome"], "accepted");
    assert_eq!(pause["node"], c.nodes[0].name.as_str());

    eventually("the takeover", Duration::from_secs(30), async || {
        listed(reader, &format!("kind=lease.takeover&object={id}"))
            .await
            .first()
            .is_some_and(|e| {
                e["text"]
                    .as_str()
                    .unwrap()
                    .contains(&format!("from {victim}"))
            })
    })
    .await;
    eventually("the node down", Duration::from_secs(60), async || {
        !listed(reader, &format!("kind=member.down&object={victim}"))
            .await
            .is_empty()
    })
    .await;
    // Both live nodes saw it go: one recorded it.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        listed(reader, &format!("kind=member.down&object={victim}"))
            .await
            .len(),
        1
    );
    c.stop().await;
}
