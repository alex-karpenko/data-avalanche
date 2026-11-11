//! Three nodes as three processes on the host (Q-M8-4): each gossips on its own loopback port
//! behind a [`Forwarder`], seeded with every node's forwarder, and runs one config's pipelines.
//! The cluster scenarios (`cluster.rs`) and the acceptance runs (`tests/load`) share it.

use std::collections::BTreeMap;
use std::time::Duration;

use data_avalanche_cluster::Ring;

use crate::forwarder::Forwarder;
use crate::harness::{E2e, Options, Proc, eventually, free_udp_port};

/// The `commitInfo.operation` of a table's creation commit.
pub const CREATE_OP: &str = "CREATE TABLE";

pub const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

/// The test's pipeline and two more over the same table, each into a lake of its own.
pub const MORE: &str = r#"  b:
    id: b
@SOURCE(_b)@
    tables: { include: ['@INCLUDE@'], snapshots: { mode: never } }
    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_b, delta: {}, flush: { seconds: 1 } }
  c:
    id: c
@SOURCE(_c)@
    tables: { include: ['@INCLUDE@'], snapshots: { mode: never } }
    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_c, delta: {}, flush: { seconds: 1 } }
"#;

pub struct Node {
    pub name: String,
    /// The port the node gossips from (its bind port).
    pub gossip: u16,
    pub fwd: Forwarder,
    pub proc: Option<Proc>,
}

/// The nodes are dropped (and print their logs on a failure) before the test's directory.
pub struct Cluster {
    pub nodes: Vec<Node>,
    /// The pipelines the config runs: the test's, and the [`SPARES`] its `more` names (`b`, `c`
    /// for [`Cluster::new`]); a reload changes them.
    pub ids: Vec<String>,
    pub e: E2e,
}

/// Each pipeline but the test's own has a source suffix and a lake of its name.
pub const SPARES: [&str; 3] = ["b", "c", "d"];

/// A pipeline's lake.
pub fn lake(test: &str, id: &str) -> String {
    if id == test {
        "lake".to_owned()
    } else {
        format!("lake_{id}")
    }
}

impl Cluster {
    /// `n1`, `n2` and `n3`, not started: the test's pipeline over [`ITEMS`] and [`MORE`]'s two,
    /// each flushing every second, none snapshotting.
    pub async fn new(test: &str) -> Self {
        Self::with(
            test,
            ITEMS,
            MORE,
            Options {
                tables: ", snapshots: { mode: never }",
                cluster: ", rebalance_stabilization: 3s",
                ..Options::default()
            },
        )
        .await
    }

    /// `n1`, `n2` and `n3`, not started, gossiping through their forwarders: `ddl` in the test's
    /// schema, the test's pipeline as `o` says, and `more` pipelines, each named for one of
    /// [`SPARES`] and reading through `@SOURCE(_<id>)@`.
    pub async fn with(test: &str, ddl: &str, more: &str, o: Options<'_>) -> Self {
        let mut nodes = Vec::new();
        for name in ["n1", "n2", "n3"] {
            let gossip = free_udp_port();
            let fwd = Forwarder::start(gossip).await;
            nodes.push(Node {
                name: name.to_owned(),
                gossip,
                fwd,
                proc: None,
            });
        }
        let seeds: Vec<String> = nodes
            .iter()
            .map(|n| format!("\"127.0.0.1:{}\"", n.fwd.port()))
            .collect();
        let memberlist = format!(
            ", push_pull_interval: 5s, join_members: [{}]",
            seeds.join(", ")
        );
        let e = E2e::new(
            test,
            ddl,
            Options {
                memberlist: &memberlist,
                pipelines: more,
                ..o
            },
        )
        .await;
        for id in SPARES {
            e.drop_source(&format!("_{id}")).await;
        }
        let ids = std::iter::once(e.test.clone())
            .chain(
                SPARES
                    .iter()
                    .filter(|id| more.contains(&format!("  {id}:\n")))
                    .map(|id| (*id).to_owned()),
            )
            .collect();
        Self { nodes, ids, e }
    }

    pub async fn start(&mut self, i: usize) {
        let n = &mut self.nodes[i];
        let (gossip, advertise) = (n.gossip.to_string(), n.fwd.port().to_string());
        let mut p = self.e.start_node_with(
            &n.name,
            &[
                ("E2E_GOSSIP_PORT", &gossip),
                ("E2E_ADVERTISE_PORT", &advertise),
            ],
        );
        p.ready().await;
        n.proc = Some(p);
    }

    pub async fn start_all(&mut self) {
        for i in 0..self.nodes.len() {
            self.start(i).await;
        }
        self.settled().await;
    }

    pub fn ids(&self) -> Vec<String> {
        self.ids.clone()
    }

    /// Each pipeline's lease: its holder and epoch, unless released.
    pub async fn holders(&self) -> BTreeMap<String, (String, u64)> {
        let mut by = BTreeMap::new();
        for id in self.ids() {
            if let Some(l) = self.e.lease_of(&id).await
                && !l.doc.released
            {
                by.insert(id, (l.doc.holder_node_id, l.doc.owner_epoch));
            }
        }
        by
    }

    pub fn live(&self) -> Vec<&Node> {
        self.nodes.iter().filter(|n| n.proc.is_some()).collect()
    }

    pub fn proc(&self, name: &str) -> &Proc {
        self.nodes[self.index(name)]
            .proc
            .as_ref()
            .expect("a live node")
    }

    pub fn index(&self, name: &str) -> usize {
        self.nodes
            .iter()
            .position(|n| n.name == name)
            .expect("a node")
    }

    /// Waits until every live node sees the others and holds what its metric says, and each
    /// pipeline is held by the node the ring over the live nodes places it on.
    pub async fn settled(&self) {
        let live = self.live();
        let ring = Ring::new(live.iter().map(|n| n.name.as_str()), 128);
        eventually("the cluster settles", Duration::from_secs(90), async || {
            let holders = self.holders().await;
            let placed = self
                .ids()
                .iter()
                .all(|id| holders.get(id).map(|(h, _)| h.as_str()) == ring.owner(id));
            if !placed {
                return false;
            }
            for n in &live {
                let text = n.proc.as_ref().unwrap().metrics().await;
                let held = holders.values().filter(|(h, _)| *h == n.name).count();
                let lines = [
                    format!("data_avalanche_cluster_members {}", live.len()),
                    format!("data_avalanche_owned_pipelines {held}"),
                ];
                if !lines.iter().all(|l| text.lines().any(|t| t == l)) {
                    return false;
                }
            }
            true
        })
        .await;
    }

    /// Cuts `side` off from every other node, both ways.
    pub fn cut(&self, side: &[&str]) {
        let (a, b): (Vec<&Node>, Vec<&Node>) = self
            .nodes
            .iter()
            .partition(|n| side.contains(&n.name.as_str()));
        for x in &a {
            x.fwd.block(&b.iter().map(|n| n.gossip).collect::<Vec<_>>());
        }
        for y in &b {
            y.fwd.block(&a.iter().map(|n| n.gossip).collect::<Vec<_>>());
        }
    }

    pub fn heal(&self) {
        for n in &self.nodes {
            n.fwd.heal();
        }
    }

    /// Every lake's `items` holds the source's rows, and its owner epochs never went back.
    pub async fn landed(&self) {
        for id in self.ids() {
            let lake = lake(&self.e.test, &id);
            let lake = lake.as_str();
            self.e
                .converged_in(lake, "items", Duration::from_secs(60))
                .await;
            let log = self.e.log_in(lake, "items").await;
            assert!(!log.is_empty(), "{lake}: no commits");
            let mut epochs = Vec::new();
            for actions in &log {
                let info = actions
                    .iter()
                    .find(|a| a.get("commitInfo").is_some())
                    .expect("a commit has its commitInfo");
                match info["commitInfo"]["data-avalanche.owner_epoch"].as_u64() {
                    Some(e) => epochs.push(e),
                    // CREATE_OP is the table's creation: it carries no epoch.
                    None => assert_eq!(
                        info["commitInfo"]["operation"], CREATE_OP,
                        "{lake}: a commit without an epoch: {info}"
                    ),
                }
            }
            assert!(!epochs.is_empty(), "{lake}: the commits carry their epoch");
            assert!(epochs.is_sorted(), "{lake}: an epoch went back: {epochs:?}");
        }
    }

    pub async fn stop(mut self) {
        for n in &mut self.nodes {
            if let Some(p) = n.proc.take() {
                assert!(p.terminate().await.success(), "{} stops", n.name);
            }
        }
        for id in SPARES {
            self.e.drop_source(&format!("_{id}")).await;
        }
        self.e.cleanup().await;
    }
}
