//! What a node sends its live streams of its own accord (M13f, D-M13f.3), from its memory only,
//! never from storage, and only while a stream is open on it:
//! - `pipeline`: a pipeline it runs, when its state, error or term changes (looked at every
//!   [`STATE_EVERY`]), and once more as `standby` when it stops running it;
//! - `figures`: each pipeline it runs, every [`FIGURES_EVERY`], as `GET /v1/pipelines/{id}/live`
//!   answers (the halts' times left out: they are in storage);
//! - `node`: its own process and gossip figures, every [`FIGURES_EVERY`].
//!
//! When a stream opens, every state is sent again at the next look: a stream starts without
//! replay, so a page learns each pipeline's state within a second of opening it.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use data_avalanche_cluster::{Actual, ControllerStatus, Live};
use serde_json::{Value, json};
use tokio::time::MissedTickBehavior;

use crate::Node;
use crate::live::{actual_of, answer};
use crate::nodes::{gossip_json, phase, process_json};

/// How often a pipeline's state is looked at.
pub const STATE_EVERY: Duration = Duration::from_secs(1);
/// How often a pipeline's and the node's figures are sent (Oleksii: 5 s).
pub const FIGURES_EVERY: Duration = Duration::from_secs(5);

/// What the node sent last of each pipeline it runs, and how many streams were ever opened on it
/// then ([`Feed::opened`](data_avalanche_cluster::Feed::opened)).
#[derive(Debug, Default)]
pub(crate) struct Publisher {
    sent: BTreeMap<String, Value>,
    opened: u64,
}

impl Publisher {
    /// The messages due at `now`: each pipeline whose state changed since the last look (every
    /// one when a stream opened on the node since), a `standby` for each that stopped running
    /// here, and with `figures` each pipeline's live figures and the node's.
    pub(crate) async fn due(
        &mut self,
        node: &Node,
        now: DateTime<Utc>,
        figures: bool,
    ) -> Vec<Live> {
        let opened = node.feed.opened();
        if opened != self.opened {
            self.sent.clear();
        }
        self.opened = opened;
        let me = node.members.borrow().me.clone();
        let running: BTreeMap<String, ControllerStatus> = node
            .statuses
            .borrow()
            .iter()
            .map(|(id, s)| (id.clone(), s.borrow().clone()))
            .filter(|(_, s)| {
                s.actual
                    .as_ref()
                    .is_some_and(|a| !matches!(a, Actual::Standby { .. }))
            })
            .collect();
        let mut out = Vec::new();
        for (id, s) in &running {
            let (actual, error) = s.actual.as_ref().map_or(("starting", None), actual_of);
            let state = json!({ "actual": actual, "error": error, "owner_epoch": s.owner_epoch });
            if self.sent.get(id) != Some(&state) {
                out.push(state_of(&me, id, now, &state));
                self.sent.insert(id.clone(), state);
            }
        }
        let gone: Vec<String> = self
            .sent
            .keys()
            .filter(|id| !running.contains_key(*id))
            .cloned()
            .collect();
        for id in gone {
            self.sent.remove(&id);
            let state = json!({ "actual": "standby", "error": null, "owner_epoch": null });
            out.push(state_of(&me, &id, now, &state));
        }
        if figures {
            let config = node.coordinator.config().await;
            for (id, s) in &running {
                if let Some(p) = config.pipelines.values().find(|p| p.id == *id) {
                    out.push(Live {
                        kind: "figures",
                        pipeline: Some(id.clone()),
                        data: answer(&me, p, s, None, now),
                    });
                }
            }
            out.push(node_of(node, &me, running.keys().cloned().collect(), now));
        }
        out
    }
}

/// A pipeline's state message.
fn state_of(me: &str, id: &str, now: DateTime<Utc>, state: &Value) -> Live {
    let mut data = json!({ "node": me, "pipeline": id, "at": now });
    for (k, v) in state.as_object().into_iter().flatten() {
        data[k] = v.clone();
    }
    Live {
        kind: "pipeline",
        pipeline: Some(id.to_owned()),
        data,
    }
}

/// The node's own figures: its phase, process and gossip, and the pipelines it runs.
fn node_of(node: &Node, me: &str, running: BTreeSet<String>, now: DateTime<Utc>) -> Live {
    let members = node.members.borrow().clone();
    let process = *node.process.borrow();
    let gossip = node.gossip.borrow().clone();
    Live {
        kind: "node",
        pipeline: None,
        data: json!({
            "node": me,
            "at": now,
            "phase": members.nodes.get(me).map(|m| phase(m.meta.phase)),
            "process": process_json(&process),
            "gossip": gossip_json(&members, gossip, now),
            "pipelines": running,
        }),
    }
}

/// Sends what is due every [`STATE_EVERY`] while a stream follows the node, until aborted.
pub async fn publish(node: Node) {
    let per_figures = (FIGURES_EVERY.as_secs() / STATE_EVERY.as_secs()).max(1);
    let mut every = tokio::time::interval(STATE_EVERY);
    every.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut publisher = Publisher::default();
    let mut ticks: u64 = 0;
    loop {
        every.tick().await;
        if node.feed.followers() == 0 {
            publisher = Publisher::default();
            continue;
        }
        let figures = ticks.is_multiple_of(per_figures);
        for live in publisher.due(&node, Utc::now(), figures).await {
            node.feed.publish(live);
        }
        ticks += 1;
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_cluster::Feed;
    use data_avalanche_pipeline::WorkerStatus;
    use tokio::sync::watch;

    use super::*;
    use crate::test_support::lone;

    type Statuses = watch::Sender<BTreeMap<String, watch::Receiver<ControllerStatus>>>;

    fn status(actual: Actual, epoch: u64) -> ControllerStatus {
        ControllerStatus {
            actual: Some(actual),
            owner_epoch: Some(epoch),
            worker: Some(WorkerStatus::default()),
            ..ControllerStatus::default()
        }
    }

    /// node-a running `orders` as `actual`, its statuses' sender kept to change them.
    async fn node(actual: Actual) -> (Node, Statuses, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut lone = lone(dir.path().join("config.yaml")).await;
        let (tx, rx) = watch::channel(BTreeMap::from([(
            "orders".to_owned(),
            watch::channel(status(actual, 4)).1,
        )]));
        lone.node.statuses = rx;
        (lone.node, tx, dir)
    }

    fn set(tx: &Statuses, s: ControllerStatus) {
        tx.send_replace(BTreeMap::from([("orders".to_owned(), watch::channel(s).1)]));
    }

    fn kinds(sent: &[Live]) -> Vec<&'static str> {
        sent.iter().map(|l| l.kind).collect()
    }

    #[tokio::test]
    async fn a_pipeline_s_state_is_sent_when_it_changes() {
        let (node, tx, _dir) = node(Actual::Running).await;
        let mut p = Publisher::default();
        let now = Utc::now();
        let sent = p.due(&node, now, false).await;
        assert_eq!(kinds(&sent), ["pipeline"]);
        let d = &sent[0].data;
        assert_eq!(sent[0].pipeline.as_deref(), Some("orders"));
        assert_eq!(
            (
                &d["node"],
                &d["pipeline"],
                &d["actual"],
                &d["error"],
                &d["owner_epoch"]
            ),
            (
                &json!("node-a"),
                &json!("orders"),
                &json!("running"),
                &Value::Null,
                &json!(4)
            )
        );
        assert!(p.due(&node, now, false).await.is_empty(), "unchanged");
        set(
            &tx,
            status(
                Actual::Retrying {
                    error: "the source is down".into(),
                },
                4,
            ),
        );
        let sent = p.due(&node, now, false).await;
        assert_eq!(sent[0].data["actual"], "retrying");
        assert_eq!(sent[0].data["error"], "the source is down");
        // The lease gone: once `standby`, then nothing.
        set(&tx, status(Actual::Standby { holder: None }, 4));
        let sent = p.due(&node, now, false).await;
        assert_eq!(kinds(&sent), ["pipeline"]);
        assert_eq!(sent[0].data["actual"], "standby");
        assert_eq!(sent[0].data["owner_epoch"], Value::Null);
        assert!(p.due(&node, now, false).await.is_empty());
    }

    #[tokio::test]
    async fn a_stream_that_opens_gets_every_state_again() {
        let (node, _tx, _dir) = node(Actual::Paused).await;
        let mut p = Publisher::default();
        let now = Utc::now();
        let first = node.feed.subscribe();
        assert_eq!(p.due(&node, now, false).await.len(), 1);
        assert!(p.due(&node, now, false).await.is_empty());
        let second = node.feed.subscribe();
        let again = p.due(&node, now, false).await;
        assert_eq!(again[0].data["actual"], "paused");
        drop(first);
        assert!(p.due(&node, now, false).await.is_empty(), "one closed");
        drop(second);
    }

    /// A stream that closes while another opens between two looks: the count is the same, the
    /// new stream still gets every state.
    #[tokio::test]
    async fn a_stream_that_opens_as_another_closes_gets_every_state_again() {
        let (node, _tx, _dir) = node(Actual::Paused).await;
        let mut p = Publisher::default();
        let now = Utc::now();
        let first = node.feed.subscribe();
        assert_eq!(p.due(&node, now, false).await.len(), 1);
        assert!(p.due(&node, now, false).await.is_empty());
        drop(first);
        let _second = node.feed.subscribe();
        let again = p.due(&node, now, false).await;
        assert_eq!(kinds(&again), ["pipeline"]);
        assert_eq!(again[0].data["actual"], "paused");
    }

    #[tokio::test]
    async fn figures_are_the_live_answer_and_the_node_s_own() {
        let (node, _tx, _dir) = node(Actual::Running).await;
        let mut p = Publisher::default();
        let now = Utc::now();
        let sent = p.due(&node, now, true).await;
        assert_eq!(kinds(&sent), ["pipeline", "figures", "node"]);
        let f = &sent[1].data;
        assert_eq!(
            (&f["id"], &f["node"], &f["actual"]),
            (&json!("orders"), &json!("node-a"), &json!("running"))
        );
        assert_eq!(f["worker"]["phase"], "starting");
        let n = &sent[2].data;
        assert_eq!(n["node"], "node-a");
        assert_eq!(n["pipelines"], json!(["orders"]));
        assert!(n["process"].get("memory_bytes").is_some(), "{n}");
        assert!(n["gossip"]["peers"].is_array(), "{n}");
        let again = p.due(&node, now, true).await;
        assert_eq!(
            kinds(&again),
            ["figures", "node"],
            "figures every time, the state once"
        );
    }

    /// The loop sends nothing while no stream follows, and the state once one does.
    #[tokio::test]
    async fn the_node_publishes_only_while_followed() {
        let (mut node, _tx, _dir) = node(Actual::Running).await;
        node.feed = Feed::new();
        let task = tokio::spawn(publish(node.clone()));
        tokio::time::sleep(STATE_EVERY * 2).await;
        let mut stream = node.feed.subscribe();
        let first = tokio::time::timeout(STATE_EVERY * 3, stream.recv())
            .await
            .expect("within a look")
            .unwrap();
        assert!(
            ["pipeline", "figures", "node"].contains(&first.kind),
            "{first:?}"
        );
        task.abort();
    }
}
