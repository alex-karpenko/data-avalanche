//! The placement ring (Clustering §3.1, §4): each node puts `tokens` points on a 64-bit circle,
//! and a pipeline belongs to the node of the first point at or after the hash of its `id`
//! (wrapping round). The ring is a pure function of the node names and `tokens`, so every node
//! that sees the same members computes the same owners; the hash is xxh3, the anchor's
//! `hash_algo`. A node's points don't depend on the other nodes, so a node that joins or leaves
//! moves only the pipelines it takes or gives up (§4.4).

use xxhash_rust::xxh3::xxh3_64;

/// The ring over a set of node names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ring {
    /// Sorted by point, then by node name.
    points: Vec<(u64, usize)>,
    /// Sorted, without duplicates.
    nodes: Vec<String>,
}

/// Node `node`'s `index`-th point: xxh3 of its name's bytes followed by the index, 4 bytes
/// big-endian. The index has a fixed width, so no two (name, index) pairs share their bytes.
fn point(node: &str, index: u32) -> u64 {
    let mut bytes = Vec::with_capacity(node.len() + 4);
    bytes.extend_from_slice(node.as_bytes());
    bytes.extend_from_slice(&index.to_be_bytes());
    xxh3_64(&bytes)
}

impl Ring {
    /// The ring of `nodes` (in any order, duplicates ignored), `tokens` points each.
    pub fn new<'a>(nodes: impl IntoIterator<Item = &'a str>, tokens: u32) -> Self {
        let mut nodes: Vec<String> = nodes.into_iter().map(str::to_owned).collect();
        nodes.sort();
        nodes.dedup();
        let mut points: Vec<(u64, usize)> = nodes
            .iter()
            .enumerate()
            .flat_map(|(n, name)| (0..tokens).map(move |i| (point(name, i), n)))
            .collect();
        points.sort_unstable();
        Self { points, nodes }
    }

    /// The node that owns `pipeline_id`; `None` on an empty ring.
    pub fn owner(&self, pipeline_id: &str) -> Option<&str> {
        let h = xxh3_64(pipeline_id.as_bytes());
        let at = self.points.partition_point(|&(p, _)| p < h);
        let (_, n) = self.points.get(at).or_else(|| self.points.first())?;
        Some(&self.nodes[*n])
    }

    /// The node names, sorted.
    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("pipeline-{i}")).collect()
    }

    fn owners(ring: &Ring, ids: &[String]) -> Vec<String> {
        ids.iter()
            .map(|id| ring.owner(id).expect("an owner").to_owned())
            .collect()
    }

    /// Every node of a cluster, and every version of this build, must place pipelines alike:
    /// these owners are pinned. A change to the hash input fails here, not in a split cluster.
    #[test]
    fn the_placement_is_pinned() {
        let ring = Ring::new(["worker-0", "worker-1", "worker-2"], 128);
        let got: Vec<(&str, &str)> = ["orders", "events", "customers", "payments", "audit"]
            .into_iter()
            .map(|id| (id, ring.owner(id).unwrap()))
            .collect();
        assert_eq!(
            got,
            [
                ("orders", "worker-1"),
                ("events", "worker-0"),
                ("customers", "worker-2"),
                ("payments", "worker-1"),
                ("audit", "worker-0"),
            ]
        );
        assert_eq!(point("worker-0", 0), 0xdb10_e65a_1e40_fe5f);
    }

    #[test]
    fn the_member_order_and_duplicates_dont_matter() {
        let a = Ring::new(["c", "a", "b"], 64);
        let b = Ring::new(["a", "b", "c", "a"], 64);
        assert_eq!(a, b);
        assert_eq!(a.nodes(), ["a", "b", "c"]);
    }

    #[test]
    fn an_empty_ring_has_no_owner_and_one_node_owns_everything() {
        assert_eq!(Ring::new([], 128).owner("orders"), None);
        assert!(Ring::new([], 128).is_empty());
        let one = Ring::new(["solo"], 128);
        assert!(owners(&one, &ids(200)).iter().all(|o| o == "solo"));
    }

    /// 128 tokens a node keep three nodes within a few percent of a third each.
    #[test]
    fn the_pipelines_spread_evenly() {
        let ring = Ring::new(["worker-0", "worker-1", "worker-2"], 128);
        let ids = ids(6000);
        let mut count: BTreeMap<String, usize> = BTreeMap::new();
        for o in owners(&ring, &ids) {
            *count.entry(o).or_default() += 1;
        }
        for (node, n) in &count {
            let share = *n as f64 / ids.len() as f64;
            assert!((0.25..0.42).contains(&share), "{node}: {share}");
        }
    }

    /// A joining node takes about a quarter of the pipelines and nothing moves between the
    /// others; a leaving node's pipelines are the only ones that move.
    #[test]
    fn a_join_or_a_leave_moves_only_what_it_must() {
        let ids = ids(4000);
        let three = owners(&Ring::new(["a", "b", "c"], 128), &ids);
        let four = owners(&Ring::new(["a", "b", "c", "d"], 128), &ids);
        let mut moved = 0;
        for (before, after) in three.iter().zip(&four) {
            if before != after {
                assert_eq!(after, "d", "a move between the old nodes");
                moved += 1;
            }
        }
        let share = moved as f64 / ids.len() as f64;
        assert!((0.15..0.35).contains(&share), "moved {share}");
        let two = owners(&Ring::new(["a", "c"], 128), &ids);
        for (before, after) in three.iter().zip(&two) {
            if before != "b" {
                assert_eq!(before, after, "a pipeline not on b moved");
            }
        }
    }
}
