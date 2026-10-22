//! Three in-process nodes (their gossip on a hub, their leases in one store, paused time): the
//! cluster's scenarios, each checking at every step that no pipeline runs on two nodes, and
//! that the rows committed meanwhile all land.

use std::time::Duration;

use tokio::time::Instant;

use crate::test_support::{
    STABLE, TestCluster, TestNode, at, eventually, runners, settles, timing,
};
use crate::{Ring, leave};

const IDS: [&str; 6] = [
    "orders",
    "events",
    "customers",
    "users",
    "payments",
    "audit",
];

fn placed(nodes: &[&str]) -> impl Fn(&str) -> String {
    let ring = Ring::new(nodes.iter().copied(), 128);
    move |id| ring.owner(id).expect("a node").to_owned()
}

/// `a` first, then `b` and `c` through `a`; settled on the three-node ring.
async fn three(c: &TestCluster) -> (TestNode, TestNode, TestNode) {
    let a = c.start("a", 1, 1, None).await;
    let b = c.start("b", 2, 1, Some(at(1))).await;
    let n3 = c.start("c", 3, 1, Some(at(1))).await;
    tokio::time::sleep(STABLE).await;
    settles(
        "spread over a, b and c",
        &[&a, &b, &n3],
        placed(&["a", "b", "c"]),
    )
    .await;
    (a, b, n3)
}

/// For `d`, the pipelines stay where they run and none runs on two nodes.
async fn steady(what: &str, nodes: &[&TestNode], d: Duration) {
    let before = runners(nodes);
    let t = Instant::now();
    while t.elapsed() < d {
        assert_eq!(
            runners(nodes),
            before,
            "{what}: moved after {:?}",
            t.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn three_nodes_spread_the_pipelines_and_each_runs_once() {
    let c = TestCluster::new(&IDS);
    let (a, b, n3) = three(&c).await;
    let on = runners(&[&a, &b, &n3]);
    for name in ["a", "b", "c"] {
        assert!(
            on.values().any(|n| n == &[name]),
            "{name} runs nothing: {on:?}"
        );
    }
    c.insert(1);
    eventually("1 lands", || c.landed(&[1])).await;
    for n in [&a, &b, &n3] {
        let members = n.gossip.members().borrow().nodes.len();
        assert_eq!(members, 3, "{}", n.name);
    }
}

/// `c` crashes: the others see it gone after the suspicion, and take its pipelines once their
/// leases went a lease_duration unrenewed. The rows committed meanwhile land once.
#[tokio::test(start_paused = true)]
async fn a_crashed_nodes_pipelines_are_taken_over_after_its_lease() {
    let c = TestCluster::new(&IDS);
    let (a, b, n3) = three(&c).await;
    c.insert(1);
    eventually("1 lands", || c.landed(&[1])).await;
    let t = Instant::now();
    drop(n3);
    c.insert(2);
    settles("on a and b", &[&a, &b], placed(&["a", "b"])).await;
    let took = t.elapsed();
    assert!(
        took >= timing().duration,
        "{took:?}: before the lease expired"
    );
    assert!(
        took < timing().duration + Duration::from_secs(15),
        "{took:?}"
    );
    eventually("2 lands", || c.landed(&[1, 2])).await;
}

/// `{a, b} | {c}`: each side thinks the other died, but every lease is still renewed, so no
/// pipeline moves (the lease, not the ring, decides). After the heal and a stabilization the
/// ring is whole again and the pipelines are where it puts them.
#[tokio::test(start_paused = true)]
async fn a_partition_moves_no_pipeline_while_the_leases_hold() {
    let c = TestCluster::new(&IDS);
    let (a, b, n3) = three(&c).await;
    c.hub.cut(&[at(1), at(2)], &[at(3)]);
    eventually("split", || {
        a.gossip.members().borrow().peers() == 1 && n3.gossip.members().borrow().peers() == 0
    })
    .await;
    c.insert(1);
    steady("partitioned", &[&a, &b, &n3], Duration::from_secs(60)).await;
    eventually("1 lands", || c.landed(&[1])).await;
    c.hub.heal();
    eventually("healed", || {
        [&a, &b, &n3]
            .iter()
            .all(|n| n.gossip.members().borrow().peers() == 2)
    })
    .await;
    tokio::time::sleep(STABLE).await;
    settles("whole again", &[&a, &b, &n3], placed(&["a", "b", "c"])).await;
    c.insert(2);
    eventually("2 lands", || c.landed(&[1, 2])).await;
}

/// Every node leaves and comes back as its next incarnation, one at a time: each leave hands
/// off without a lease wait, and the rows committed throughout land.
#[tokio::test(start_paused = true)]
async fn a_rolling_restart_keeps_every_pipeline_running_once() {
    let c = TestCluster::new(&IDS);
    let (a, b, n3) = three(&c).await;
    let mut nodes = vec![a, b, n3];
    let mut row = 0;
    for i in 0..3 {
        let gone = nodes.remove(0);
        let (name, n) = (gone.name.clone(), u8::try_from(i + 1).unwrap());
        let t = Instant::now();
        let left = tokio::spawn(leave(gone.gossip, gone.reconciler));
        let rest: Vec<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
        let refs: Vec<&TestNode> = nodes.iter().collect();
        settles(&format!("{name} handed off"), &refs, placed(&rest)).await;
        assert!(t.elapsed() < timing().duration, "{name}: {:?}", t.elapsed());
        left.await.unwrap();
        row += 1;
        c.insert(row);
        // Back as its next incarnation, seeded by a node that stayed.
        let seed = if n == 1 { at(2) } else { at(1) };
        nodes.push(c.start(&name, n, 2, Some(seed)).await);
        tokio::time::sleep(STABLE).await;
        let refs: Vec<&TestNode> = nodes.iter().collect();
        settles(&format!("{name} back"), &refs, placed(&["a", "b", "c"])).await;
    }
    let all: Vec<i64> = (1..=row).collect();
    eventually("every row lands", || c.landed(&all)).await;
}
