//! A reload on one node (config §12, Clustering §9): a restart-only change is refused and
//! nothing is published; a pipeline change restarts the pipeline under its lease; a restart runs
//! the cluster's generation, not the file (D-3), until a reload publishes the file.

use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

#[tokio::test]
async fn a_reload_publishes_the_file_and_a_restart_keeps_the_generation() {
    require_it!();
    let e = E2e::new(
        "reload_one",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.converged("items").await;
    let first = e.config();
    let epoch = e.lease().await.expect("a lease").doc.owner_epoch;

    // A field only a process restart applies (V-061): refused, nothing published.
    e.write_config(&first.replace("lease_duration: 6s", "tokens: 64\n    lease_duration: 6s"));
    let (code, v) = p.reload().await;
    assert_eq!(code, 422, "{v}");
    let errors = v["report"]["errors"].as_array().expect("the report");
    assert!(
        errors
            .iter()
            .any(|d| d["rule"] == "V-061" && d["path"] == "globals.cluster.tokens"),
        "{v}"
    );
    assert_eq!(p.config_status().await["generation"], 1);

    // A source field (PR): the pipeline restarts under the lease it holds.
    let timeouts = first.replacen(
        "      ssl_mode: disable\n",
        "      ssl_mode: disable\n      timeouts: { command: 45s }\n",
        1,
    );
    assert_ne!(timeouts, first);
    e.write_config(&timeouts);
    let (code, v) = p.reload().await;
    assert_eq!(
        (code, &v),
        (202, &serde_json::json!({ "generation": 2 })),
        "{v}"
    );
    let (code, v) = p.reload().await;
    assert_eq!(
        (code, &v),
        (
            200,
            &serde_json::json!({ "generation": 2, "unchanged": true })
        ),
        "the file is the generation already"
    );
    eventually(
        "the pipeline restarted",
        Duration::from_secs(30),
        async || p.log().contains("restarted for a reload"),
    )
    .await;
    p.metric("data_avalanche_config_generation 2").await;
    p.metric(&e.status("running")).await;
    e.exec("INSERT INTO items VALUES (2, 'b')").await;
    e.converged("items").await;
    assert_eq!(
        e.lease().await.expect("a lease").doc.owner_epoch,
        epoch,
        "restarted under the same lease"
    );

    // A restart doesn't apply the file: the node runs generation 2 until a reload.
    e.write_config(&first);
    assert!(p.terminate().await.success());
    let mut p = e.start(&[]);
    p.ready().await;
    let s = p.config_status().await;
    assert_eq!(
        (&s["generation"], &s["applied"]),
        (&2.into(), &2.into()),
        "{s}"
    );
    assert!(
        p.log().contains(
            "the cluster runs config generation 2 from globals.state, not this node's file"
        ),
        "{}",
        p.log()
    );
    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    e.converged("items").await;
    let (code, v) = p.reload().await;
    assert_eq!(
        (code, &v),
        (202, &serde_json::json!({ "generation": 3 })),
        "{v}"
    );
    p.metric("data_avalanche_config_generation 3").await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
