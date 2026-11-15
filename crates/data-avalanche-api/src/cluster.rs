//! `/v1/cluster` and `/v1/admin`: the members this node sees and who holds each pipeline, and
//! the node's own leave and shutdown (D-M9a.8). A leave hands every pipeline off and leaves the
//! gossip, and the process stays up: not ready, owning nothing, still answering the API, until
//! it is stopped; a restart joins again. A shutdown is what SIGTERM does: the leave, then the
//! process ends.

use axum::Extension;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use data_avalanche_cluster::{Phase, Ring};
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::pipelines::{Liveness, liveness};
use crate::{Api, Principal, body, starting};

/// What the API asked of the node, which the binary carries out; each includes the ones before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stop {
    /// Nothing yet.
    Run,
    /// Leave the cluster, and stay up.
    Leave,
    /// Leave the cluster, and end.
    Shutdown,
}

/// The node's leave and shutdown, as the API asks for them.
#[derive(Debug, Clone)]
pub struct Control {
    asked: watch::Sender<Stop>,
}

impl Control {
    /// The control, and what the binary watches.
    pub fn new() -> (Self, watch::Receiver<Stop>) {
        let (asked, watched) = watch::channel(Stop::Run);
        (Self { asked }, watched)
    }

    /// Asks for `stop` unless it, or more, is asked already: false then.
    pub fn ask(&self, stop: Stop) -> bool {
        self.asked.send_if_modified(|now| {
            let more = stop > *now;
            if more {
                *now = stop;
            }
            more
        })
    }
}

fn phase(p: Phase) -> &'static str {
    match p {
        Phase::Joining => "joining",
        Phase::Active => "active",
        Phase::Draining => "draining",
        Phase::Leaving => "leaving",
    }
}

/// The members this node sees, with where each one's API is and its file's hash; each
/// pipeline's lease holder, epoch and expiry, and its owner on the ring of the active members
/// (where it goes once the ring has settled, D-M13b.13).
pub(crate) async fn status(State(api): State<Api>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let members = node.members.borrow().clone();
    let nodes: Vec<Value> = members
        .nodes
        .iter()
        .map(|(name, m)| {
            json!({
                "node": name,
                "addr": m.addr.to_string(),
                "phase": phase(m.meta.phase),
                "suspect": m.suspect,
                "config_generation": m.meta.config_generation,
                "build": m.meta.build,
                "api_url": (!m.meta.api_url.is_empty()).then_some(&m.meta.api_url),
                "file_hash": (!m.meta.file_hash.is_empty()).then_some(&m.meta.file_hash),
            })
        })
        .collect();
    let config = node.coordinator.config().await;
    let ring = Ring::new(
        members
            .nodes
            .iter()
            .filter(|(_, m)| m.meta.phase == Phase::Active)
            .map(|(name, _)| name.as_str()),
        config.globals.cluster.tokens,
    );
    let mut pipelines = vec![];
    let mut ids: Vec<&str> = config.pipelines.values().map(|p| p.id.as_str()).collect();
    ids.sort_unstable();
    for id in ids {
        let lease = match node.leases.observe(id).await {
            Ok(lease) => lease.map(|v| v.doc).filter(|l| !l.released),
            Err(e) => {
                return body(
                    StatusCode::SERVICE_UNAVAILABLE,
                    &json!({ "error": format!("the lease of {id}: {e}") }),
                );
            }
        };
        pipelines.push(json!({
            "id": id,
            "holder": lease.as_ref().map(|l| l.holder_node_id.clone()),
            "owner_epoch": lease.as_ref().map(|l| l.owner_epoch),
            "holder_stale": lease
                .as_ref()
                .map(|l| liveness(&members, l) == Liveness::Stale),
            "expires_at": lease.as_ref().map(|l| {
                l.renewed_at + chrono::Duration::milliseconds(
                    i64::try_from(l.lease_duration_ms).unwrap_or(i64::MAX),
                )
            }),
            "ring_owner": ring.owner(id),
        }));
    }
    body(
        StatusCode::OK,
        &json!({ "node": members.me, "members": nodes, "pipelines": pipelines }),
    )
}

/// 202 once the leave is asked, naming the node that leaves; 409 when this node leaves, or
/// shuts down, already.
pub(crate) async fn leave(State(api): State<Api>, Extension(by): Extension<Principal>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    if node.control.ask(Stop::Leave) {
        tracing::info!(by = %by.name, "leaving the cluster, asked through the API");
        let me = node.members.borrow().me.clone();
        body(
            StatusCode::ACCEPTED,
            &json!({ "leaving": true, "node": me }),
        )
    } else {
        body(
            StatusCode::CONFLICT,
            &json!({ "error": "this node leaves the cluster already" }),
        )
    }
}

/// 202 once the shutdown is asked, naming the node that shuts down; 409 when it is asked
/// already.
pub(crate) async fn shutdown(
    State(api): State<Api>,
    Extension(by): Extension<Principal>,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    if node.control.ask(Stop::Shutdown) {
        tracing::info!(by = %by.name, "shutting down, asked through the API");
        let me = node.members.borrow().me.clone();
        body(
            StatusCode::ACCEPTED,
            &json!({ "shutting_down": true, "node": me }),
        )
    } else {
        body(
            StatusCode::CONFLICT,
            &json!({ "error": "this node shuts down already" }),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use data_avalanche_state::Holder;

    use super::*;
    use crate::test_support::{CONFIG, api, json_of, lone, open, serve};

    #[test]
    fn a_leave_then_a_shutdown_is_asked_once_each() {
        let (control, watched) = Control::new();
        assert!(control.ask(Stop::Leave));
        assert!(!control.ask(Stop::Leave));
        assert_eq!(*watched.borrow(), Stop::Leave);
        assert!(control.ask(Stop::Shutdown));
        assert!(!control.ask(Stop::Leave), "a shutdown includes the leave");
        assert!(!control.ask(Stop::Shutdown));
        assert_eq!(*watched.borrow(), Stop::Shutdown);
    }

    #[tokio::test]
    async fn the_cluster_status_names_the_members_and_the_holders() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file).await;
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        let addr = serve(a).await;

        let (code, v) = json_of(addr, "GET", "/v1/cluster/status").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(v["node"], "node-a");
        assert_eq!(v["members"][0]["node"], "node-a");
        assert_eq!(v["members"][0]["phase"], "active");
        assert_eq!(v["members"][0]["config_generation"], 1);
        assert_eq!(v["members"][0]["api_url"], "http://127.0.0.1:8080");
        assert_eq!(
            v["members"][0]["file_hash"],
            data_avalanche_cluster::file_hash(CONFIG)
        );
        assert_eq!(
            v["pipelines"],
            json!([{
                "id": "orders", "holder": null, "owner_epoch": null, "holder_stale": null,
                "expires_at": null, "ring_owner": "node-a",
            }])
        );
        let holder = Holder {
            node_id: "node-b".into(),
            incarnation: 1,
        };
        lone.leases
            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
            .await
            .unwrap();
        let v = json_of(addr, "GET", "/v1/cluster/status").await.1;
        let p = &v["pipelines"][0];
        assert_eq!(
            (
                &p["holder"],
                &p["owner_epoch"],
                &p["holder_stale"],
                &p["ring_owner"]
            ),
            (&json!("node-b"), &json!(1), &json!(true), &json!("node-a")),
            "held away from its owner: {p}"
        );
        let expires: chrono::DateTime<Utc> = p["expires_at"].as_str().unwrap().parse().unwrap();
        let left = expires - Utc::now();
        assert!(
            left > chrono::Duration::seconds(25) && left <= chrono::Duration::seconds(30),
            "{left}"
        );
    }

    #[tokio::test]
    async fn a_leave_and_a_shutdown_are_asked_of_the_binary() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file).await;
        let watched = lone.stops.clone();
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        let addr = serve(a).await;

        assert_eq!(
            json_of(addr, "POST", "/v1/cluster/leave").await,
            (202, json!({ "leaving": true, "node": "node-a" }))
        );
        assert_eq!(*watched.borrow(), Stop::Leave);
        assert_eq!(json_of(addr, "POST", "/v1/cluster/leave").await.0, 409);
        assert_eq!(
            json_of(addr, "POST", "/v1/admin/shutdown").await,
            (202, json!({ "shutting_down": true, "node": "node-a" }))
        );
        assert_eq!(*watched.borrow(), Stop::Shutdown);
        assert_eq!(json_of(addr, "POST", "/v1/admin/shutdown").await.0, 409);
    }
}
