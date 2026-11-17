//! Nodes and config through any node (Admin_Console §4, §5; M13b), on three nodes: one node shows
//! another's detail; each node's file is seen to differ from the generation once the shared file
//! changes; a reload asked of n2 through n1 publishes it, recorded with its note, and the files
//! match again; any node lists the generations and the diff; a leave asked of n3 through n1 is
//! taken and recorded by n3.

use std::time::Duration;

use data_avalanche_testkit::require_it;
use serde_json::{Value, json};

use crate::harness::{Proc, eventually};
use crate::nodes::Cluster;

/// Each member's `file`, as `node` lists them.
async fn files(node: &Proc) -> Vec<(String, String)> {
    let (code, v) = node.call("GET", "/v1/config/nodes").await;
    assert_eq!(code, 200, "{v}");
    v["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .map(|n| {
            (
                n["node"].as_str().unwrap().to_owned(),
                n["file"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn all(file: &str) -> Vec<(String, String)> {
    ["n1", "n2", "n3"]
        .iter()
        .map(|n| ((*n).to_owned(), file.to_owned()))
        .collect()
}

#[tokio::test]
async fn a_node_is_seen_and_acted_on_through_another() {
    require_it!();
    let mut c = Cluster::new("console_nodes").await;
    c.start_all().await;
    let n1 = c.proc("n1");

    let (code, v) = n1.call("GET", "/v1/nodes/n2").await;
    assert_eq!(code, 200, "{v}");
    assert_eq!(
        (&v["node"], &v["config_generation"], &v["file"]["matches"]),
        (&json!("n2"), &json!(1), &json!(true)),
        "{v}"
    );
    assert!(
        v["process"].is_object() && v["gossip"]["peers"].is_array(),
        "{v}"
    );
    assert_eq!(n1.call("GET", "/v1/nodes/n9").await.0, 404);
    assert_eq!(files(n1).await, all("same"));

    // The shared file changes: every node sees its own differ, within its file check.
    c.e.write_config(
        &c.e.config()
            .replace("drain_timeout: 30s", "drain_timeout: 25s"),
    );
    eventually("the files differ", Duration::from_secs(45), async || {
        files(n1).await == all("differs")
    })
    .await;

    let (code, _, body) = n1
        .send(
            None,
            "POST",
            "/v1/nodes/n2/config/reload",
            r#"{"note": "shorter drain"}"#,
        )
        .await
        .expect("an answer");
    assert_eq!(code, 202, "{body}");
    eventually(
        "the files match again",
        Duration::from_secs(45),
        async || files(n1).await == all("same"),
    )
    .await;

    let n3 = c.proc("n3");
    let (code, v) = n3.call("GET", "/v1/config/generations").await;
    assert_eq!(code, 200, "{v}");
    let newest = &v["generations"][0];
    assert_eq!(
        (
            &newest["generation"],
            &newest["node"],
            &newest["note"],
            &newest["by"]["name"]
        ),
        (
            &json!(2),
            &json!("n2"),
            &json!("shorter drain"),
            &json!("anonymous")
        ),
        "{v}"
    );
    let (code, v) = n3.call("GET", "/v1/config/diff?from=1&to=2").await;
    assert_eq!(code, 200, "{v}");
    assert_eq!(
        v["changes"],
        json!([{
            "path": "globals.cluster.drain_timeout", "class": "in_place",
            "before": "30s", "after": "25s",
        }])
    );
    let (code, v) = n3.call("GET", "/v1/config/generations/2").await;
    assert_eq!(code, 200, "{v}");
    assert!(
        v["text"].as_str().unwrap().contains("drain_timeout: 25s"),
        "{v}"
    );

    let (code, _, body) = n1
        .send(
            None,
            "POST",
            "/v1/nodes/n3/leave",
            r#"{"note": "the e2e leave"}"#,
        )
        .await
        .expect("an answer");
    assert_eq!(code, 202, "{body}");
    eventually("n3 leaves", Duration::from_secs(60), async || {
        let (_, v) = n1.call("GET", "/v1/cluster/status").await;
        v["members"].as_array().is_some_and(|m| {
            !m.iter()
                .any(|m| m["node"] == "n3" && m["phase"] == "active")
        })
    })
    .await;
    eventually(
        "the leave is recorded by n3",
        Duration::from_secs(30),
        async || {
            let (_, v) = n1.call("GET", "/v1/events?kind=action.node.leave").await;
            v["events"].as_array().is_some_and(|e| {
                e.iter()
                    .any(|e: &Value| e["node"] == "n3" && e["note"] == "the e2e leave")
            })
        },
    )
    .await;
    c.stop().await;
}
