//! A node's way into the cluster and out of it (Clustering §3.3, §3.5, §8.3).

use std::time::Duration;

use crate::{ClusterReconciler, Gossip, Phase};

/// How long a starting node with seeds waits for its first peer before it goes on alone.
pub const JOIN_WAIT: Duration = Duration::from_secs(5);

/// Takes a node that started its gossip as `Joining` to `Active`. With seeds (`seeded`), it
/// first waits, at most [`JOIN_WAIT`], to hear from a peer: a node that knows the cluster
/// before its first ring takes the others' rings as they are and waits for its own place
/// (`Placement`), where a node that knew no one would place everything on itself first.
pub async fn join(gossip: &Gossip, seeded: bool) {
    if seeded {
        let mut members = gossip.members();
        let peer = tokio::time::timeout(JOIN_WAIT, members.wait_for(|m| m.peers() > 0)).await;
        match peer {
            Ok(Ok(_)) => tracing::info!("joined the gossip"),
            Ok(Err(_)) => tracing::warn!("the gossip stopped before a seed answered"),
            Err(_) => tracing::warn!(
                "no gossip seed answered within {JOIN_WAIT:?}: going on alone, still asking (a seed \
                 must be a peer's advertised address, advertise_addr:advertise_port, not a Service \
                 or load-balancer address)"
            ),
        }
    }
    gossip.set_phase(Phase::Active);
}

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
    use crate::test_support::{
        Hub, STABLE, TestCluster, at, eventually, gossip_settings, leases, settles, timing,
    };

    fn joining(hub: &std::sync::Arc<Hub>, name: &str, n: u8) -> Gossip {
        let (port, inbox) = hub.port(at(n));
        let me = crate::NodeIdentity {
            node_id: name.into(),
            incarnation: 1,
            rev: 0,
            addr: at(n),
            meta: crate::NodeMeta {
                phase: Phase::Joining,
                config_generation: 1,
                build: "1.0.0".into(),
            },
        };
        let key = crate::GossipKey::derive(&[7; 32]);
        Gossip::start(me, key, gossip_settings(), std::sync::Arc::new(port), inbox)
    }

    fn phase(g: &Gossip, of: &str) -> Option<Phase> {
        g.members().borrow().nodes.get(of).map(|m| m.meta.phase)
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_without_seeds_is_active_at_once() {
        let hub = Hub::new();
        let a = joining(&hub, "a", 1);
        let t = Instant::now();
        join(&a, false).await;
        assert_eq!(t.elapsed(), Duration::ZERO);
        eventually("a active", || phase(&a, "a") == Some(Phase::Active)).await;
    }

    /// A seed that answers: active once a peer is known; one that doesn't: alone after the wait.
    #[tokio::test(start_paused = true)]
    async fn a_node_with_seeds_waits_for_a_peer_then_is_active() {
        let hub = Hub::new();
        let a = joining(&hub, "a", 1);
        join(&a, false).await;
        let b = joining(&hub, "b", 2);
        b.announce(vec![at(1)]);
        join(&b, true).await;
        assert_eq!(b.members().borrow().peers(), 1);
        eventually("b active on a", || phase(&a, "b") == Some(Phase::Active)).await;
        let c = joining(&hub, "c", 3);
        c.announce(vec![at(9)]);
        let t = Instant::now();
        join(&c, true).await;
        assert_eq!(t.elapsed(), JOIN_WAIT);
        assert_eq!(c.members().borrow().peers(), 0);
    }

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
