//! Three nodes as three processes on the host (Q-M8-4): each gossips on its own loopback port
//! behind a [`Forwarder`], seeded with every node's forwarder. Three pipelines over one table
//! spread over them. A crash (`kill -9`), a leave (SIGTERM), a partition (the forwarders drop a
//! side's packets) and a stop-the-world pause (SIGSTOP) each end with every pipeline held by one
//! live node, every row in the lake, and owner epochs that never go back in the Delta log.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::forwarder::Forwarder;
use data_avalanche_cluster::Ring;
use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, Proc, eventually, free_udp_port};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

/// The test's pipeline and two more over the same table, each into a lake of its own.
const MORE: &str = r#"  b:
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

struct Node {
    name: String,
    /// The port the node gossips from (its bind port).
    gossip: u16,
    fwd: Forwarder,
    proc: Option<Proc>,
}

/// The nodes are dropped (and print their logs on a failure) before the test's directory.
struct Cluster {
    nodes: Vec<Node>,
    e: E2e,
}

impl Cluster {
    /// `n1`, `n2` and `n3`, not started.
    async fn new(test: &str) -> Self {
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
            ITEMS,
            Options {
                tables: ", snapshots: { mode: never }",
                cluster: ", rebalance_stabilization: 3s",
                memberlist: &memberlist,
                pipelines: MORE,
                ..Options::default()
            },
        )
        .await;
        for suffix in ["_b", "_c"] {
            e.drop_source(suffix).await;
        }
        Self { nodes, e }
    }

    async fn start(&mut self, i: usize) {
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

    async fn start_all(&mut self) {
        for i in 0..self.nodes.len() {
            self.start(i).await;
        }
        self.settled().await;
    }

    fn ids(&self) -> [String; 3] {
        [self.e.test.clone(), "b".into(), "c".into()]
    }

    /// Each pipeline's lease: its holder and epoch, unless released.
    async fn holders(&self) -> BTreeMap<String, (String, u64)> {
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

    fn live(&self) -> Vec<&Node> {
        self.nodes.iter().filter(|n| n.proc.is_some()).collect()
    }

    fn index(&self, name: &str) -> usize {
        self.nodes
            .iter()
            .position(|n| n.name == name)
            .expect("a node")
    }

    /// Waits until every live node sees the others and holds what its metric says, and each
    /// pipeline is held by the node the ring over the live nodes places it on.
    async fn settled(&self) {
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
    fn cut(&self, side: &[&str]) {
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

    fn heal(&self) {
        for n in &self.nodes {
            n.fwd.heal();
        }
    }

    /// The lake's `items` holds the source's rows, and its owner epochs never went back.
    async fn landed(&self) {
        self.e.converged("items").await;
        let epochs: Vec<u64> = self
            .e
            .log("items")
            .await
            .iter()
            .flatten()
            .filter_map(|a| a["commitInfo"]["data-avalanche.owner_epoch"].as_u64())
            .collect();
        assert!(!epochs.is_empty(), "the commits carry their epoch");
        assert!(epochs.is_sorted(), "an epoch went back: {epochs:?}");
    }

    async fn stop(mut self) {
        for n in &mut self.nodes {
            if let Some(p) = n.proc.take() {
                assert!(p.terminate().await.success(), "{} stops", n.name);
            }
        }
        for suffix in ["_b", "_c"] {
            self.e.drop_source(suffix).await;
        }
        self.e.cleanup().await;
    }
}

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
        let t = Instant::now();
        assert!(
            c.nodes[i].proc.take().unwrap().terminate().await.success(),
            "{name}"
        );
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

/// The holder of the test's pipeline stops for 15 s (SIGSTOP), past its lease: another node
/// takes it at the next epoch while it is frozen. Back (SIGCONT), the paused node finds its
/// lease lost and its commits fenced; nothing it wrote goes behind the new owner's epoch, and
/// it rejoins.
#[tokio::test]
async fn a_paused_holder_loses_its_pipelines_and_rejoins() {
    require_it!();
    let mut c = Cluster::new("cluster_pause").await;
    c.start_all().await;
    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
    c.landed().await;
    let (victim, epoch) = c.holders().await[&c.e.test].clone();
    let i = c.index(&victim);
    c.nodes[i].proc.as_ref().unwrap().signal("STOP");
    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
    eventually(
        "taken over while paused",
        Duration::from_secs(30),
        async || {
            c.holders()
                .await
                .get(&c.e.test)
                .is_some_and(|(_, e)| *e > epoch)
        },
    )
    .await;
    // The paused node's replication connection stays open, so on PostgreSQL its walsender keeps
    // the slot until `wal_sender_timeout` (60 s) ends it; the new holder streams at its next
    // busy-slot retry after that, at most a backoff step (60 s) later (`Postgres_Slot…` D-15.5).
    c.e.converged_within("items", Duration::from_secs(150))
        .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    c.nodes[i].proc.as_ref().unwrap().signal("CONT");
    c.e.exec("INSERT INTO items VALUES (3, 'c')").await;
    c.settled().await;
    c.landed().await;
    c.stop().await;
}
