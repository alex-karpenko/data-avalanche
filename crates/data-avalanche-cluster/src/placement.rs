//! Which members are on the ring (Clustering §8.5, §20.3, D-11). A member is eligible while it
//! is alive or suspect and its phase is `Active`: a suspect stays, so a blip moves nothing. A
//! member that stops being eligible (down, left, or no longer `Active`) leaves the ring at once;
//! one that becomes eligible (a join, a rejoin) is added only once it stayed eligible for
//! `rebalance_stabilization`, so a flapping node never grabs and drops pipelines.
//!
//! A ring with no surviving node is filled at once, in the same update that emptied it: a node
//! that starts, having joined the gossip, takes the eligible peers it finds as they are (their
//! rings already hold them), and itself at once only when it has no eligible peer (a lone node,
//! or the first of a cold start).

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::Instant;

use crate::{Members, Phase, Ring};

/// The ring a node places pipelines on, as membership changes.
#[derive(Debug)]
pub struct Placement {
    tokens: u32,
    stabilization: Duration,
    /// The eligible members, each with the instant it became eligible.
    eligible: BTreeMap<String, Instant>,
    ring: Ring,
    /// Bumped on every change of the ring's nodes.
    generation: u64,
}

impl Placement {
    pub fn new(tokens: u32, stabilization: Duration) -> Self {
        Self {
            tokens,
            stabilization,
            eligible: BTreeMap::new(),
            ring: Ring::new([], tokens),
            generation: 0,
        }
    }

    /// A reloaded `rebalance_stabilization`: a joiner already waiting counts its wait against
    /// it from the next update.
    pub fn set_stabilization(&mut self, stabilization: Duration) {
        self.stabilization = stabilization;
    }

    /// Takes in `members` as of `now`. True when the ring's nodes changed.
    pub fn update(&mut self, members: &Members, now: Instant) -> bool {
        let eligible: Vec<&str> = members
            .nodes
            .iter()
            .filter(|(_, m)| m.meta.phase == Phase::Active)
            .map(|(name, _)| name.as_str())
            .collect();
        self.eligible
            .retain(|name, _| eligible.contains(&name.as_str()));
        for name in &eligible {
            self.eligible.entry((*name).to_owned()).or_insert(now);
        }
        let survivors: Vec<&str> = self
            .ring
            .nodes()
            .iter()
            .map(String::as_str)
            .filter(|n| self.eligible.contains_key(*n))
            .collect();
        let mut next: Vec<&str> = if survivors.is_empty() {
            let peers: Vec<&str> = eligible
                .iter()
                .copied()
                .filter(|n| *n != members.me)
                .collect();
            if peers.is_empty() { eligible } else { peers }
        } else {
            survivors
        };
        for (name, since) in &self.eligible {
            if now.duration_since(*since) >= self.stabilization && !next.contains(&name.as_str()) {
                next.push(name);
            }
        }
        next.sort_unstable();
        if next == self.ring.nodes() {
            return false;
        }
        self.ring = Ring::new(next, self.tokens);
        self.generation += 1;
        true
    }

    /// When the next waiting member is due on the ring: the next `update` worth making.
    pub fn next_due(&self) -> Option<Instant> {
        self.eligible
            .iter()
            .filter(|(name, _)| !self.ring.nodes().contains(name))
            .map(|(_, since)| *since + self.stabilization)
            .min()
    }

    pub fn ring(&self) -> &Ring {
        &self.ring
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::members;

    const STABLE: Duration = Duration::from_secs(15);

    use Phase::{Active, Draining, Joining};

    fn nodes(p: &Placement) -> Vec<&str> {
        p.ring().nodes().iter().map(String::as_str).collect()
    }

    #[test]
    fn a_lone_node_is_placed_at_once() {
        let mut p = Placement::new(128, STABLE);
        let t = Instant::now();
        assert!(
            !p.update(&members("a", &[("a", Joining, false)]), t),
            "a joining node"
        );
        assert!(p.ring().is_empty());
        assert!(p.update(&members("a", &[("a", Active, false)]), t));
        assert_eq!(nodes(&p), ["a"]);
        assert_eq!(p.generation(), 1);
        assert_eq!(p.next_due(), None);
    }

    /// A node that starts into a running cluster takes its peers as they are and waits for
    /// itself, as they wait for it.
    #[test]
    fn a_new_node_places_its_peers_at_once_and_itself_later() {
        let mut p = Placement::new(128, STABLE);
        let t = Instant::now();
        let view = members(
            "c",
            &[
                ("a", Active, false),
                ("b", Active, false),
                ("c", Active, false),
            ],
        );
        assert!(p.update(&view, t));
        assert_eq!(nodes(&p), ["a", "b"]);
        assert_eq!(p.next_due(), Some(t + STABLE));
        assert!(!p.update(&view, t + STABLE - Duration::from_millis(1)));
        assert!(p.update(&view, t + STABLE));
        assert_eq!(nodes(&p), ["a", "b", "c"]);
        assert_eq!(p.generation(), 2);
    }

    #[test]
    fn a_joiner_waits_out_the_stabilization_and_a_flap_restarts_its_wait() {
        let mut p = Placement::new(128, STABLE);
        let t = Instant::now();
        p.update(&members("a", &[("a", Active, false)]), t);
        let with_c = members("a", &[("a", Active, false), ("c", Active, false)]);
        assert!(!p.update(&with_c, t + Duration::from_secs(1)));
        // c goes away at 10 s and is back at 12 s: its wait starts again.
        p.update(
            &members("a", &[("a", Active, false)]),
            t + Duration::from_secs(10),
        );
        p.update(&with_c, t + Duration::from_secs(12));
        assert!(
            !p.update(&with_c, t + Duration::from_secs(16)),
            "not 15 s after its join"
        );
        assert_eq!(p.next_due(), Some(t + Duration::from_secs(27)));
        assert!(p.update(&with_c, t + Duration::from_secs(27)));
        assert_eq!(nodes(&p), ["a", "c"]);
    }

    #[test]
    fn a_suspect_stays_and_a_departure_or_a_drain_leaves_at_once() {
        let mut p = Placement::new(128, STABLE);
        let t = Instant::now();
        let three = [
            ("a", Active, false),
            ("b", Active, false),
            ("c", Active, false),
        ];
        p.update(&members("a", &three), t);
        assert_eq!(nodes(&p), ["b", "c"], "the peers first");
        p.update(&members("a", &three), t + STABLE);
        assert_eq!(nodes(&p), ["a", "b", "c"]);
        let later = t + STABLE + Duration::from_secs(1);
        let b_suspect = [
            ("a", Active, false),
            ("b", Active, true),
            ("c", Active, false),
        ];
        assert!(
            !p.update(&members("a", &b_suspect), later),
            "a suspect stays"
        );
        let b_gone = [("a", Active, false), ("c", Active, false)];
        assert!(p.update(&members("a", &b_gone), later));
        assert_eq!(nodes(&p), ["a", "c"]);
        let c_draining = [("a", Active, false), ("c", Draining, false)];
        assert!(p.update(&members("a", &c_draining), later));
        assert_eq!(nodes(&p), ["a"]);
        // A node that drains itself leaves its own ring: it owns nothing.
        assert!(p.update(&members("a", &[("a", Draining, false)]), later));
        assert!(p.ring().is_empty());
        assert_eq!(p.ring().owner("orders"), None);
    }

    #[test]
    fn a_ring_whose_nodes_all_go_is_filled_at_once() {
        let mut p = Placement::new(128, STABLE);
        let t = Instant::now();
        let three = [
            ("a", Active, false),
            ("b", Active, false),
            ("c", Active, false),
        ];
        p.update(&members("c", &three), t);
        assert_eq!(nodes(&p), ["a", "b"]);
        // c has waited 5 s of its 15 s, and a and b go down in the same update.
        assert!(p.update(
            &members("c", &[("c", Active, false)]),
            t + Duration::from_secs(5)
        ));
        assert_eq!(nodes(&p), ["c"]);
    }
}
