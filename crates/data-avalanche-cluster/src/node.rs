//! A node's way out of the cluster (Clustering §3.5, §8.3).

use crate::{ClusterReconciler, Gossip, Phase};

/// Leaves the cluster on a shutdown. With peers, the node drains first (Clustering §8.3): its
/// phase becomes `Draining`, which takes it off every ring at once, so each pipeline it runs is
/// handed off (`max_concurrent_handoffs` at a time) and released, and its new owner acquires it
/// without waiting out the lease; then `Leaving`, and the gossip's leave, which the others see
/// at once rather than after a suspicion. A node without peers has no one to hand to: its
/// pipelines drain together and release their leases for the next process.
pub async fn leave(gossip: Gossip, reconciler: ClusterReconciler) {
    let peers = gossip.members().borrow().peers();
    if peers > 0 {
        tracing::info!(peers, "leaving the cluster: handing every pipeline off");
        gossip.set_phase(Phase::Draining);
        reconciler.released().await;
        gossip.set_phase(Phase::Leaving);
    }
    // What is still held (a claim that landed during the drain) drains here, uncapped.
    reconciler.shutdown().await;
    gossip.leave().await;
    tracing::info!("left the cluster");
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::*;
    use crate::Ring;
    use crate::test_support::{STABLE, TestCluster, at, eventually, leases, settles, timing};

    const IDS: [&str; 4] = ["orders", "events", "customers", "users"];

    /// `a` leaves a two-node cluster: `b` runs everything within a lease renewal or two, not
    /// after a lease_duration, and no pipeline ever runs on both.
    #[tokio::test(start_paused = true)]
    async fn a_leave_hands_every_pipeline_off_without_a_lease_wait() {
        let c = TestCluster::new(&IDS);
        let a = c.start("a", 1, 1, None);
        settles("all on a", &[&a], |_| "a".into()).await;
        let b = c.start("b", 2, 1, Some(at(1)));
        let two = Ring::new(["a", "b"], 128);
        tokio::time::sleep(STABLE).await;
        settles("spread over a and b", &[&a, &b], |id| {
            two.owner(id).unwrap().into()
        })
        .await;
        c.insert(1);
        eventually("1 lands", || c.landed(&[1])).await;
        let t = Instant::now();
        let left = tokio::spawn(leave(a.gossip, a.reconciler));
        settles("all on b", &[&b], |_| "b".into()).await;
        let took = t.elapsed();
        assert!(took < timing().duration, "{took:?}: a lease wait");
        left.await.unwrap();
        for id in IDS {
            let lease = leases(&c.store).observe(id).await.unwrap().unwrap();
            assert_eq!(lease.doc.holder_node_id, "b", "{id}");
        }
        c.insert(2);
        eventually("2 lands", || c.landed(&[1, 2])).await;
        assert_eq!(
            b.gossip.members().borrow().peers(),
            0,
            "a is gone from b's members"
        );
    }

    /// A lone node's leave releases every lease: the next process acquires them at once.
    #[tokio::test(start_paused = true)]
    async fn a_lone_node_leaves_by_releasing_its_leases() {
        let c = TestCluster::new(&IDS);
        let a = c.start("a", 1, 1, None);
        settles("all on a", &[&a], |_| "a".into()).await;
        leave(a.gossip, a.reconciler).await;
        for id in IDS {
            let lease = leases(&c.store).observe(id).await.unwrap().unwrap();
            assert!(lease.doc.released, "{id}");
        }
        let a2 = c.start("a", 1, 2, None);
        let t = Instant::now();
        settles("all on the next a", &[&a2], |_| "a".into()).await;
        assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    }
}
