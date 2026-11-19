//! The live stream (Admin_Console §8.1; M13f) on three nodes: a stream opened on a node that
//! doesn't run the test's pipeline follows the two others, so it shows the holder's state and
//! figures as they change, and an action's event; the holder killed, it names it unreachable,
//! then shows the next holder's state and the members without it; an open stream doesn't hold
//! the nodes' stop.

use std::time::{Duration, Instant};

use data_avalanche_testkit::require_it;
use serde_json::json;

use crate::harness::Doc;
use crate::nodes::Cluster;

#[tokio::test]
async fn the_live_stream_merges_every_node_s_and_follows_a_failover() {
    require_it!();
    let mut c = Cluster::new("console_stream").await;
    c.start_all().await;
    let test = c.e.test.clone();
    c.e.insert_series("items", 5, Doc::Row).await;
    c.landed().await;
    let holder = c.holders().await[&test].0.clone();
    let other = ["n1", "n2", "n3"]
        .into_iter()
        .find(|n| *n != holder)
        .expect("another node")
        .to_owned();
    let mut s = c
        .proc(&other)
        .stream(&format!("/v1/stream?pipeline={test}"))
        .await
        .expect("the stream opens");
    let (kind, hello) = s.next().await.expect("hello");
    assert_eq!(kind, "hello");
    assert_eq!(hello["node"], other.as_str(), "{hello}");
    let peers: Vec<&str> = ["n1", "n2", "n3"]
        .into_iter()
        .filter(|n| *n != other)
        .collect();
    assert_eq!(hello["follows"], json!(peers), "{hello}");
    let minute = Duration::from_secs(60);

    // The holder's state and figures, through the node asked.
    let is_holder = |v: &serde_json::Value| v["node"] == holder.as_str();
    let state = s
        .until("the holder's state", "pipeline", minute, |v| is_holder(v))
        .await;
    assert_eq!(
        (&state["pipeline"], &state["actual"]),
        (&json!(test), &json!("running"))
    );
    let figures = s
        .until("the holder's figures", "figures", minute, |v| is_holder(v))
        .await;
    assert_eq!(
        (&figures["id"], &figures["actual"]),
        (&json!(test), &json!("running"))
    );

    // A pause: its event, then the holder's new state; and back.
    let o = c.proc(&other);
    assert_eq!(o.ask(&test, "pause").await.0, 202);
    s.until("the pause's event", "event", minute, |v| {
        v["kind"] == "action.pipeline.pause" && v["pipeline"] == test.as_str()
    })
    .await;
    s.until("the holder paused", "pipeline", minute, |v| {
        is_holder(v) && v["actual"] == "paused"
    })
    .await;
    assert_eq!(o.ask(&test, "resume").await.0, 202);
    s.until("the holder running again", "pipeline", minute, |v| {
        is_holder(v) && v["actual"] == "running"
    })
    .await;

    // The holder killed: named unreachable; the next holder's state; the members without it, in
    // whatever order they come.
    let i = c.index(&holder);
    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
    let (mut unreachable, mut next, mut members) = (false, None, false);
    let mut read = Vec::new();
    let until = Instant::now() + Duration::from_secs(120);
    while !(unreachable && next.is_some() && members) {
        let left = until.saturating_duration_since(Instant::now());
        let Ok(Some((kind, v))) = tokio::time::timeout(left, s.next()).await else {
            panic!(
                "after the kill: unreachable {unreachable}, next {next:?}, members {members}; read:\n{}",
                read.join("\n")
            );
        };
        match kind.as_str() {
            "peer" if v["peer"] == holder.as_str() && v["state"] == "unreachable" => {
                unreachable = true;
            }
            "pipeline" if !is_holder(&v) && v["actual"] == "running" => next = Some(v.clone()),
            "members"
                if v["members"]
                    .as_array()
                    .is_some_and(|m| m.iter().all(|m| m["node"] != holder.as_str())) =>
            {
                members = true;
            }
            _ => {}
        }
        read.push(format!("{kind} {v}"));
    }
    let next = next.unwrap();
    assert_eq!(next["pipeline"], test.as_str(), "{next}");

    // The nodes stop with the stream still open on one of them.
    c.stop().await;
    // What it sent before the stop read past, the stream ends.
    let ended = tokio::time::timeout(Duration::from_secs(10), async {
        while s.next().await.is_some() {}
    })
    .await;
    assert!(ended.is_ok(), "the stream ended");
}
