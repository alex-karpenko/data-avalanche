use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// One MariaDB GTID: `domain-server_id-seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MariaGtid {
    pub domain: u32,
    pub server_id: u32,
    pub seq: u64,
}

/// A MariaDB replication position: the last GTID per replication domain (the shape of
/// `gtid_binlog_pos` and `@slave_connect_state`). Comparisons use `seq` within a domain, which is
/// monotonic under `gtid_strict_mode` (required by the source, D-V1.5).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MariaGtidState {
    by_domain: BTreeMap<u32, (u32, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid MariaDB GTID position {input:?}: {reason}")]
pub struct MariaGtidParseError {
    pub input: String,
    pub reason: String,
}

impl MariaGtidState {
    /// Records the GTID of the transaction just read; stream order is the truth.
    pub fn advance(&mut self, gtid: MariaGtid) {
        self.by_domain
            .insert(gtid.domain, (gtid.server_id, gtid.seq));
    }

    pub fn get(&self, domain: u32) -> Option<MariaGtid> {
        self.by_domain
            .get(&domain)
            .map(|&(server_id, seq)| MariaGtid {
                domain,
                server_id,
                seq,
            })
    }

    /// True when every domain of `other` is present here at an equal or later sequence.
    pub fn covers(&self, other: &MariaGtidState) -> bool {
        other.by_domain.iter().all(|(domain, &(_, seq))| {
            self.by_domain
                .get(domain)
                .is_some_and(|&(_, mine)| mine >= seq)
        })
    }

    /// Per domain, the later of the two GTIDs (by `seq`). When both sides hold the same `seq`
    /// for a domain, `self`'s GTID (and so its `server_id`) is kept — `union` is commutative only
    /// up to `server_id` on ties; the `seq` result is unaffected.
    pub fn union(&self, other: &MariaGtidState) -> MariaGtidState {
        let mut out = self.clone();
        for (&domain, &(server_id, seq)) in &other.by_domain {
            let later = out
                .by_domain
                .get(&domain)
                .is_none_or(|&(_, mine)| seq > mine);
            if later {
                out.by_domain.insert(domain, (server_id, seq));
            }
        }
        out
    }

    /// Per domain present in both, the earlier of the two GTIDs.
    pub fn intersect(&self, other: &MariaGtidState) -> MariaGtidState {
        let mut out = MariaGtidState::default();
        for (&domain, &mine) in &self.by_domain {
            if let Some(&theirs) = other.by_domain.get(&domain) {
                out.by_domain
                    .insert(domain, if theirs.1 < mine.1 { theirs } else { mine });
            }
        }
        out
    }
}

impl FromStr for MariaGtidState {
    type Err = MariaGtidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = |reason: String| MariaGtidParseError {
            input: s.to_string(),
            reason,
        };
        let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        let mut state = MariaGtidState::default();
        if compact.is_empty() {
            return Ok(state);
        }
        for item in compact.split(',') {
            let fields: Vec<&str> = item.split('-').collect();
            let [d, s_id, n] = fields.as_slice() else {
                return Err(err(format!("expected domain-server-seq, got {item:?}")));
            };
            let domain: u32 = d
                .parse()
                .map_err(|_| err(format!("bad domain in {item:?}")))?;
            let server_id: u32 = s_id
                .parse()
                .map_err(|_| err(format!("bad server id in {item:?}")))?;
            let seq: u64 = n
                .parse()
                .map_err(|_| err(format!("bad sequence in {item:?}")))?;
            if state.by_domain.insert(domain, (server_id, seq)).is_some() {
                return Err(err(format!("domain {domain} appears twice")));
            }
        }
        Ok(state)
    }
}

impl fmt::Display for MariaGtidState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, (domain, (server_id, seq))) in self.by_domain.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{domain}-{server_id}-{seq}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::position::MariaGtid;
    use crate::position::MariaGtidState;
    use proptest::prelude::*;

    fn st(s: &str) -> MariaGtidState {
        s.parse().unwrap()
    }

    #[test]
    fn parses_and_prints_sorted_by_domain() {
        assert_eq!(st("1-2-50, 0-1-100").to_string(), "0-1-100,1-2-50");
        assert_eq!(st("").to_string(), "");
    }

    #[test]
    fn rejects_malformed_and_duplicate_domains() {
        for s in ["0-1", "0-1-x", "a-1-2", "0-1-2-3", "0-1-2,0-3-4", "0-1-2,"] {
            assert!(s.parse::<MariaGtidState>().is_err(), "{s:?}");
        }
    }

    #[test]
    fn covers_compares_sequence_per_domain() {
        let pos = st("0-1-100,1-2-50");
        assert!(pos.covers(&st("0-1-99")));
        assert!(
            pos.covers(&st("0-9-100")),
            "server id does not matter for coverage"
        );
        assert!(!pos.covers(&st("0-1-101")));
        assert!(!pos.covers(&st("2-1-1")), "unknown domain is not covered");
        assert!(pos.covers(&st("")));
    }

    #[test]
    fn union_takes_max_and_intersect_takes_min_per_domain() {
        let a = st("0-1-100,1-2-50");
        let b = st("0-1-90,2-3-7");
        assert_eq!(a.union(&b).to_string(), "0-1-100,1-2-50,2-3-7");
        assert_eq!(a.intersect(&b).to_string(), "0-1-90");
    }

    #[test]
    fn advance_records_latest_gtid_of_its_domain() {
        let mut s = MariaGtidState::default();
        s.advance(MariaGtid {
            domain: 0,
            server_id: 1,
            seq: 5,
        });
        s.advance(MariaGtid {
            domain: 0,
            server_id: 2,
            seq: 6,
        });
        assert_eq!(s.to_string(), "0-2-6");
        assert_eq!(
            s.get(0),
            Some(MariaGtid {
                domain: 0,
                server_id: 2,
                seq: 6
            })
        );
        assert_eq!(s.get(1), None);
    }

    fn arb_state() -> impl Strategy<Value = MariaGtidState> {
        prop::collection::btree_map(0u32..4, (1u32..4, 1u64..1000), 0..4).prop_map(|m| {
            let mut s = MariaGtidState::default();
            for (domain, (server_id, seq)) in m {
                s.advance(MariaGtid {
                    domain,
                    server_id,
                    seq,
                });
            }
            s
        })
    }

    proptest! {
        #[test]
        fn round_trips(s in arb_state()) {
            prop_assert_eq!(s.to_string().parse::<MariaGtidState>().unwrap(), s);
        }

        #[test]
        fn union_covers_both(a in arb_state(), b in arb_state()) {
            let u = a.union(&b);
            prop_assert!(u.covers(&a) && u.covers(&b));
        }

        #[test]
        fn intersect_is_covered_by_both(a in arb_state(), b in arb_state()) {
            let i = a.intersect(&b);
            prop_assert!(a.covers(&i) && b.covers(&i));
        }

        #[test]
        fn covers_iff_union_is_identity(a in arb_state(), b in arb_state()) {
            prop_assert_eq!(a.covers(&b), a.union(&b) == a);
        }
    }
}
