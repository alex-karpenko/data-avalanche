//! Three nodes as three processes on the host (Q-M8-4): each gossips on its own loopback port
//! behind a [`Forwarder`], seeded with every node's forwarder. Three pipelines over one table
//! spread over them. A crash (`kill -9`), a leave (SIGTERM), a partition (the forwarders drop a
//! side's packets) and a stop-the-world pause (SIGSTOP) each end with every pipeline held by one
//! live node, every row in the lake, and owner epochs that never go back in the Delta log. A
//! reload on one node reaches the three (Clustering §9.2). Any node's API drives a pipeline its
//! holder runs, and a leave through the API hands off and keeps the process up (M9a).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use data_avalanche_testkit::require_it;
use serde_json::json;

use crate::harness::{Proc, eventually};
use crate::nodes::{Cluster, lake};

/// The E2E config's `lease_duration` (harness.rs: `lease_duration: 6s`).
const LEASE: Duration = Duration::from_secs(6);

/// The holder of the test's own pipeline is killed: its pipelines go to the others once their
/// leases ran out (suspicion 4 s, lease 6 s), at the next epoch, and every row lands.
#[tokio::test]
async fn a_killed_nodes_pipelines_are_taken_over() {
    require_it!();
    let mut c = Cluster::new("cluster_kill").await;
    c.start_all().await;
    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
    c.landed().await;
    let before = c.holders().await;
    let (victim, epoch) = before[&c.e.test].clone();
    let i = c.index(&victim);
    let t = Instant::now();
    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
    // The victim is dead: each lease it held has its last renewal now.
    let mut last = BTreeMap::new();
    for (id, (holder, _)) in &before {
        if *holder == victim {
            last.insert(id.clone(), c.e.lease_of(id).await.expect("a lease").doc);
        }
    }
    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
    c.settled().await;
    let took = t.elapsed();
    let after = c.holders().await;
    for (id, (holder, e)) in &before {
        if *holder == victim {
            assert_ne!(after[id].0, victim);
            assert_eq!(after[id].1, e + 1, "{id}: the next epoch");
        } else {
            assert_eq!(after[id], before[id], "{id} stays");
        }
    }
    assert_eq!(after[&c.e.test].1, epoch + 1);
    for (id, was) in &last {
        let now = c.e.lease_of(id).await.expect("a lease").doc;
        let waited = (now.acquired_at - was.renewed_at).to_std().expect("after");
        assert!(
            waited >= LEASE,
            "{id}: taken over {waited:?} after the last renewal"
        );
    }
    assert!(
        took >= Duration::from_secs(6),
        "{took:?}: before the lease ran out"
    );
    c.landed().await;
    c.stop().await;
}

/// Each node in turn leaves (SIGTERM) and starts again: a leave hands its pipelines off
/// without a lease wait, and the rows inserted throughout all land.
#[tokio::test]
async fn a_rolling_restart_hands_off_and_loses_nothing() {
    require_it!();
    let mut c = Cluster::new("cluster_roll").await;
    c.start_all().await;
    for (i, row) in (0..3).zip(1..) {
        let name = c.nodes[i].name.clone();
        let held: Vec<(String, u64)> = c
            .holders()
            .await
            .into_iter()
            .filter(|(_, (h, _))| *h == name)
            .map(|(id, (_, e))| (id, e))
            .collect();
        let t = Instant::now();
        assert!(
            c.nodes[i].proc.take().unwrap().terminate().await.success(),
            "{name}"
        );
        // Right after the exit: each lease it held is released or taken at a higher epoch.
        for (id, epoch) in &held {
            let l = c.e.lease_of(id).await.expect("a lease").doc;
            assert!(
                l.released || (l.holder_node_id != name && l.owner_epoch > *epoch),
                "{name} left {id} neither released nor handed off: {l:?}"
            );
        }
        c.settled().await;
        let took = t.elapsed();
        assert!(
            took < Duration::from_secs(6),
            "{name}'s hand-off took {took:?}: a lease wait"
        );
        c.e.exec(&format!("INSERT INTO items VALUES ({row}, 'r')"))
            .await;
        c.start(i).await;
        c.settled().await;
    }
    c.landed().await;
    c.stop().await;
}

/// `{n1, n2} | {n3}`: each side sees the other gone, but every lease is still renewed, so no
/// pipeline moves; after the heal the members are whole and the leases where they were.
#[tokio::test]
async fn a_partition_moves_no_pipeline() {
    require_it!();
    let mut c = Cluster::new("cluster_split").await;
    c.start_all().await;
    let before = c.holders().await;
    c.cut(&["n3"]);
    let n3 = c.nodes[2].proc.as_ref().unwrap();
    n3.metric("data_avalanche_cluster_members 1").await;
    c.nodes[0]
        .proc
        .as_ref()
        .unwrap()
        .metric("data_avalanche_cluster_members 2")
        .await;
    c.nodes[1]
        .proc
        .as_ref()
        .unwrap()
        .metric("data_avalanche_cluster_members 2")
        .await;
    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(15) {
        assert_eq!(
            c.holders().await,
            before,
            "a lease moved during the partition"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    c.e.converged("items").await;
    c.heal();
    c.settled().await;
    assert_eq!(c.holders().await, before, "the heal moved a lease");
    c.landed().await;
    c.stop().await;
}

/// The holder of the test's pipeline stops (SIGSTOP) until its pipelines were taken over, past
/// their leases, and the rows converged. Back (SIGCONT), the paused node finds its lease lost
/// (its renewer ends as `lost`) and its commits fenced; nothing it wrote goes behind the new
/// owner's epoch, and it rejoins.
#[tokio::test]
async fn a_paused_holder_loses_its_pipelines_and_rejoins() {
    require_it!();
    let mut c = Cluster::new("cluster_pause").await;
    c.start_all().await;
    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
    c.landed().await;
    let held = c.holders().await;
    let (victim, _) = held[&c.e.test].clone();
    let held: Vec<(String, u64)> = held
        .into_iter()
        .filter(|(_, (h, _))| *h == victim)
        .map(|(id, (_, e))| (id, e))
        .collect();
    let i = c.index(&victim);
    c.nodes[i].proc.as_ref().unwrap().signal("STOP");
    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
    eventually(
        "every pipeline taken over while paused",
        Duration::from_secs(30),
        async || {
            let now = c.holders().await;
            held.iter()
                .all(|(id, e)| now.get(id).is_some_and(|(_, ne)| ne > e))
        },
    )
    .await;
    // The paused node's replication connection stays open: on PostgreSQL its walsender holds the
    // slot until the new holder ends it (DA-SRC-28), not until `wal_sender_timeout`.
    c.e.converged("items").await;
    c.nodes[i].proc.as_ref().unwrap().signal("CONT");
    let test = format!("pipeline=\"{}\"", c.e.test);
    let proc = c.nodes[i].proc.as_ref().unwrap();
    eventually(
        "the paused node's renewer ended as lost",
        Duration::from_secs(60),
        async || {
            proc.metrics().await.lines().any(|l| {
                l.starts_with("data_avalanche_lease_renew_failures_total{")
                    && l.contains(&test)
                    && l.contains("class=\"lost\"")
                    && l.rsplit(' ')
                        .next()
                        .and_then(|v| v.parse::<f64>().ok())
                        .is_some_and(|v| v >= 1.0)
            })
        },
    )
    .await;
    c.e.exec("INSERT INTO items VALUES (3, 'c')").await;
    c.settled().await;
    c.landed().await;
    c.stop().await;
}

/// A reload on `n2` (Clustering §9.2): its file removes `c`, adds `d` and renames the test's
/// pipeline (an R change, config §12.3). Every node applies generation 2 and reports it; `c`'s
/// owner releases its lease and drops its slot (`auto_cleanup`); `d` runs on the node the ring
/// places it on, its initial snapshot and its stream together holding every row; the renamed
/// pipeline keeps its lease and epoch, and is scraped by its new name.
#[tokio::test]
async fn a_reload_reaches_every_node() {
    require_it!();
    let mut c = Cluster::new("cluster_reload").await;
    c.start_all().await;
    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
    c.landed().await;
    let before = c.holders().await;
    let test = c.e.test.clone();

    let text = c.e.config();
    let at = text.find("  c:\n").expect("pipeline c");
    let d = text[at..]
        .replace("  c:\n    id: c\n", "  d:\n    id: d\n")
        .replace(&format!("{}_c", c.e.schema), &format!("{}_d", c.e.schema))
        .replace("/lake_c,", "/lake_d,")
        // A pipeline reports `running` from its start, before it reads (the status metric):
        // the snapshot takes what is in the table then, the stream what comes after.
        .replace("snapshots: { mode: never }", "snapshots: { mode: initial }");
    let renamed = format!("{test}_renamed");
    let next = text[..at].replace(
        &format!("  {test}:\n    id: {test}\n"),
        &format!("  {renamed}:\n    id: {test}\n"),
    ) + &d;
    assert_ne!(next, text);
    c.e.write_config(&next);
    let (code, v) = c.nodes[1].proc.as_ref().unwrap().reload().await;
    assert_eq!(
        (code, &v),
        (
            202,
            &serde_json::json!({ "generation": 2, "restart_pending": [] })
        ),
        "{v}"
    );

    for n in c.live() {
        let p = n.proc.as_ref().unwrap();
        p.metric("data_avalanche_config_generation 2").await;
    }
    eventually(
        "every node reports generation 2",
        Duration::from_secs(30),
        async || {
            let s = c.nodes[0].proc.as_ref().unwrap().config_status().await;
            s["converged"] == true
                && s["generation"] == 2
                && s["nodes"].as_object().map(|m| m.len()) == Some(3)
        },
    )
    .await;
    c.ids = vec![test.clone(), "b".into(), "d".into()];
    c.settled().await;
    let after = c.holders().await;
    assert_eq!(
        after[&test], before[&test],
        "renamed in place: the lease and epoch stay"
    );
    assert_eq!(after["b"], before["b"]);
    eventually(
        "c's lease is released",
        Duration::from_secs(30),
        async || c.e.lease_of("c").await.is_some_and(|l| l.doc.released),
    )
    .await;
    if let Some(exists) = c.e.slot_exists("_c").await {
        assert!(!exists, "c's slot is dropped (auto_cleanup)");
    }
    let holder = c.index(&after[&test].0);
    c.nodes[holder]
        .proc
        .as_ref()
        .unwrap()
        .metric(&c.e.status_of(&renamed, "running"))
        .await;

    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
    c.landed().await;
    assert_eq!(
        c.e.ids_in(&lake(&test, "c"), "items").await,
        [1],
        "c is gone"
    );
    c.stop().await;
}

/// The API of nodes that don't hold the test's pipeline pauses, resumes, restarts and flushes
/// it: the holder takes each at its next poll, a restart under the lease it holds, and any node
/// reads the pipeline's status from storage.
#[tokio::test]
async fn any_nodes_api_drives_the_holder() {
    require_it!();
    let mut c = Cluster::new("cluster_api").await;
    c.start_all().await;
    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
    c.landed().await;
    let id = c.e.test.clone();
    let (holder, epoch) = c.holders().await[&id].clone();
    let others: Vec<&Proc> = c
        .nodes
        .iter()
        .filter(|n| n.name != holder)
        .map(|n| n.proc.as_ref().unwrap())
        .collect();
    let (via, reader) = (others[0], others[1]);

    assert_eq!(via.ask(&id, "pause").await.0, 202);
    reader.shows(&id, "paused").await;
    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(c.e.ids("items").await, [1], "nothing is read while paused");
    assert_eq!(
        via.ask(&id, "flush").await,
        (200, json!({ "desired_state": "paused", "unchanged": true }))
    );
    assert_eq!(via.ask(&id, "resume").await.0, 202);
    reader.shows(&id, "running").await;
    c.e.converged("items").await;

    let (code, v) = via.ask(&id, "restart").await;
    assert_eq!(code, 202, "{v}");
    let held = c.proc(&holder);
    eventually(
        "the holder restarts it",
        Duration::from_secs(30),
        async || held.log().contains("restarted on request"),
    )
    .await;
    c.e.exec("INSERT INTO items VALUES (3, 'c')").await;
    assert_eq!(via.ask(&id, "flush").await.0, 202);
    eventually(
        "the holder flushes it",
        Duration::from_secs(30),
        async || held.log().contains("flushed on request"),
    )
    .await;
    c.e.converged("items").await;
    reader.shows(&id, "running").await;
    let v = reader.call("GET", &format!("/v1/pipelines/{id}")).await.1;
    assert_eq!(
        (&v["holder"]["node"], &v["holder"]["owner_epoch"]),
        (&json!(holder), &json!(epoch)),
        "the same lease: {v}"
    );
    assert_eq!(c.holders().await[&id], (holder.clone(), epoch));

    let (code, v) = reader.call("GET", "/v1/cluster/status").await;
    assert_eq!(code, 200, "{v}");
    assert_eq!(v["members"].as_array().map(Vec::len), Some(3), "{v}");
    let row = v["pipelines"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["id"] == json!(id))
        .cloned();
    // A live holder isn't stale (DA-API-13).
    assert_eq!(
        row,
        Some(json!({ "id": id, "holder": holder, "owner_epoch": epoch, "holder_stale": false }))
    );
    c.landed().await;
    c.stop().await;
}

/// `POST /v1/cluster/leave` hands the node's pipelines off without a lease wait and keeps the
/// process up: live, not ready, still answering; `POST /v1/admin/shutdown` then ends it with
/// the exit code 0.
#[tokio::test]
async fn a_leave_through_the_api_hands_off_and_stays_up() {
    require_it!();
    let mut c = Cluster::new("cluster_leave").await;
    c.start_all().await;
    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
    c.landed().await;
    let (leaver, _) = c.holders().await[&c.e.test].clone();
    let i = c.index(&leaver);
    let mut p = c.nodes[i].proc.take().unwrap();

    assert_eq!(
        p.call("POST", "/v1/cluster/leave").await,
        (202, json!({ "leaving": true, "node": leaver }))
    );
    c.settled().await;
    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
    c.landed().await;
    assert!(
        c.holders().await.values().all(|(h, _)| *h != leaver),
        "{leaver} holds nothing"
    );
    assert_eq!(p.answers("/health/live").await, Some(200));
    assert_eq!(p.answers("/health/ready").await, Some(503));
    p.metric("data_avalanche_owned_pipelines 0").await;
    assert_eq!(p.call("POST", "/v1/cluster/leave").await.0, 409);
    assert!(
        p.log()
            .contains("left the cluster; the process stays up until it is stopped"),
        "{}",
        p.log()
    );

    assert_eq!(
        p.call("POST", "/v1/admin/shutdown").await,
        (202, json!({ "shutting_down": true, "node": leaver }))
    );
    assert!(p.exit().await.success(), "{}", p.log());
    c.stop().await;
}
