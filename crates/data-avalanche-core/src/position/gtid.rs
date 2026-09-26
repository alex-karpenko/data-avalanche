use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;
use uuid::Uuid;

/// One GTID source: the server UUID plus an optional MySQL 8.4 GTID tag.
/// Ordering puts untagged intervals before tagged ones for the same UUID, as MySQL prints them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GtidSource {
    pub uuid: Uuid,
    pub tag: Option<String>,
}

/// A MySQL GTID set. Intervals are inclusive, sorted, and merged (no overlap, no adjacency), so the
/// structure is canonical and `==` is set equality.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GtidSet {
    intervals: BTreeMap<GtidSource, Vec<(u64, u64)>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid GTID set {input:?}: {reason}")]
pub struct GtidParseError {
    pub input: String,
    pub reason: String,
}

impl GtidSet {
    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty()
    }

    /// Adds one transaction number (streaming: record each transaction's GTID).
    pub fn insert(&mut self, source: &GtidSource, gno: u64) {
        self.insert_range(source.clone(), gno, gno);
    }

    /// Adds the inclusive range `start..=end`.
    ///
    /// # Panics
    /// Unless `1 <= start <= end` (a GTID transaction number starts at 1).
    pub fn insert_range(&mut self, source: GtidSource, start: u64, end: u64) {
        assert!(
            start >= 1 && start <= end,
            "invalid GTID interval {start}-{end}"
        );
        let ivs = self.intervals.entry(source).or_default();
        if let Some(last) = ivs.last_mut() {
            // Fast path for streaming inserts that extend the newest interval.
            if start >= last.0 && start <= last.1.saturating_add(1) {
                last.1 = last.1.max(end);
                return;
            }
        }
        ivs.push((start, end));
        normalize(ivs);
    }

    /// True when `other ⊆ self`.
    pub fn covers(&self, other: &GtidSet) -> bool {
        other
            .intervals
            .iter()
            .all(|(src, theirs)| match self.intervals.get(src) {
                None => theirs.is_empty(),
                Some(mine) => theirs
                    .iter()
                    .all(|&(a, b)| mine.iter().any(|&(x, y)| x <= a && b <= y)),
            })
    }

    pub fn union(&self, other: &GtidSet) -> GtidSet {
        let mut out = self.clone();
        for (src, ivs) in &other.intervals {
            let entry = out.intervals.entry(src.clone()).or_default();
            entry.extend_from_slice(ivs);
            normalize(entry);
        }
        out
    }

    pub fn intersect(&self, other: &GtidSet) -> GtidSet {
        let mut out = GtidSet::default();
        for (src, mine) in &self.intervals {
            let Some(theirs) = other.intervals.get(src) else {
                continue;
            };
            let (mut i, mut j) = (0, 0);
            let mut common = Vec::new();
            while i < mine.len() && j < theirs.len() {
                let (a1, b1) = mine[i];
                let (a2, b2) = theirs[j];
                let (lo, hi) = (a1.max(a2), b1.min(b2));
                if lo <= hi {
                    common.push((lo, hi));
                }
                if b1 < b2 {
                    i += 1;
                } else {
                    j += 1;
                }
            }
            if !common.is_empty() {
                normalize(&mut common);
                out.intervals.insert(src.clone(), common);
            }
        }
        out
    }
}

fn normalize(ivs: &mut Vec<(u64, u64)>) {
    ivs.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ivs.len());
    for &(a, b) in ivs.iter() {
        match merged.last_mut() {
            Some(last) if a <= last.1.saturating_add(1) => last.1 = last.1.max(b),
            _ => merged.push((a, b)),
        }
    }
    *ivs = merged;
}

fn parse_interval(part: &str) -> Result<(u64, u64), String> {
    let (a, b) = part.split_once('-').unwrap_or((part, part));
    let parse = |n: &str| {
        n.parse::<u64>()
            .map_err(|_| format!("bad interval {part:?}"))
    };
    let (a, b) = (parse(a)?, parse(b)?);
    if a == 0 || a > b {
        return Err(format!("bad interval {part:?}"));
    }
    Ok((a, b))
}

fn validate_tag(tag: &str) -> Result<(), String> {
    let mut chars = tag.chars();
    let first_ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_');
    let rest_ok = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if first_ok && rest_ok && tag.len() <= 32 {
        Ok(())
    } else {
        Err(format!("bad GTID tag {tag:?}"))
    }
}

impl FromStr for GtidSet {
    type Err = GtidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = |reason: String| GtidParseError {
            input: s.to_string(),
            reason,
        };
        let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        let mut set = GtidSet::default();
        if compact.is_empty() {
            return Ok(set);
        }
        for member in compact.split(',') {
            let mut parts = member.split(':');
            let uuid_text = parts.next().unwrap_or_default();
            let uuid = Uuid::parse_str(uuid_text)
                .map_err(|_| err(format!("bad source UUID {uuid_text:?}")))?;
            let mut current = GtidSource { uuid, tag: None };
            let mut current_has_interval = false;
            let mut any_interval = false;
            for part in parts {
                if part.starts_with(|c: char| c.is_ascii_digit()) {
                    let (a, b) = parse_interval(part).map_err(err)?;
                    set.insert_range(current.clone(), a, b);
                    current_has_interval = true;
                    any_interval = true;
                } else {
                    if current.tag.is_some() && !current_has_interval {
                        return Err(err(format!("tag {:?} has no intervals", current.tag)));
                    }
                    let tag = part.to_ascii_lowercase();
                    validate_tag(&tag).map_err(err)?;
                    current = GtidSource {
                        uuid,
                        tag: Some(tag),
                    };
                    current_has_interval = false;
                }
            }
            if !any_interval || !current_has_interval {
                return Err(err(format!("member {member:?} ends without intervals")));
            }
        }
        Ok(set)
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut current_uuid: Option<Uuid> = None;
        for (src, ivs) in &self.intervals {
            if current_uuid != Some(src.uuid) {
                if current_uuid.is_some() {
                    f.write_str(",")?;
                }
                write!(f, "{}", src.uuid.hyphenated())?;
                current_uuid = Some(src.uuid);
            }
            if let Some(tag) = &src.tag {
                write!(f, ":{tag}")?;
            }
            for &(a, b) in ivs {
                if a == b {
                    write!(f, ":{a}")?;
                } else {
                    write!(f, ":{a}-{b}")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const B: &str = "4e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn set(s: &str) -> GtidSet {
        s.parse().unwrap()
    }

    #[test]
    fn parses_and_prints_canonical_form() {
        assert_eq!(
            set(&format!("{B}:5,{A}:1-3:7")).to_string(),
            format!("{A}:1-3:7,{B}:5")
        );
        assert_eq!(set("").to_string(), "");
        assert!(set("").is_empty());
    }

    #[test]
    fn parses_multiline_uppercase_server_output() {
        let raw = format!("{}:1-5,\n  {}:1-3", A.to_uppercase(), B);
        assert_eq!(set(&raw).to_string(), format!("{A}:1-5,{B}:1-3"));
    }

    #[test]
    fn merges_overlapping_and_adjacent_intervals() {
        assert_eq!(
            set(&format!("{A}:1-3:4-6:5-9:11")).to_string(),
            format!("{A}:1-9:11")
        );
    }

    #[test]
    fn parses_and_prints_tagged_intervals() {
        let s = set(&format!("{A}:1-3:ETL:1-2:audit:5"));
        assert_eq!(s.to_string(), format!("{A}:1-3:audit:5:etl:1-2"));
    }

    #[test]
    fn rejects_malformed_members() {
        for s in [
            A.to_string(),
            format!("{A}:"),
            format!("{A}:0"),
            format!("{A}:5-3"),
            "not-a-uuid:1".to_string(),
            format!("{A}:tag"),
            format!("{A}:1-3:Bad-Tag:1"),
            format!("{A}:1-3,"),
        ] {
            assert!(s.parse::<GtidSet>().is_err(), "{s:?}");
        }
    }

    #[test]
    fn covers_is_subset() {
        let big = set(&format!("{A}:1-10,{B}:1-5"));
        assert!(big.covers(&set(&format!("{A}:2-4:9"))));
        assert!(big.covers(&set("")));
        assert!(!big.covers(&set(&format!("{A}:10-11"))));
        assert!(
            !big.covers(&set(&format!("{A}:1-10:etl:1"))),
            "different tag is a different source"
        );
    }

    #[test]
    fn union_and_intersect() {
        let x = set(&format!("{A}:1-5,{B}:1-2"));
        let y = set(&format!("{A}:4-8"));
        assert_eq!(x.union(&y).to_string(), format!("{A}:1-8,{B}:1-2"));
        assert_eq!(x.intersect(&y).to_string(), format!("{A}:4-5"));
    }

    #[test]
    fn insert_extends_intervals() {
        let src = GtidSource {
            uuid: A.parse().unwrap(),
            tag: None,
        };
        let mut s = GtidSet::default();
        for gno in [1, 2, 3, 5] {
            s.insert(&src, gno);
        }
        assert_eq!(s.to_string(), format!("{A}:1-3:5"));
    }

    fn arb_set() -> impl Strategy<Value = GtidSet> {
        let uuids = [A, B, "5e11fa47-71ca-11e1-9e33-c80aa9429562"];
        let tags = [None, Some("t1"), Some("t2")];
        prop::collection::vec((0usize..3, 0usize..3, 1u64..60, 0u64..6), 0..8).prop_map(
            move |items| {
                let mut s = GtidSet::default();
                for (u, t, start, len) in items {
                    let src = GtidSource {
                        uuid: uuids[u].parse().unwrap(),
                        tag: tags[t].map(str::to_string),
                    };
                    s.insert_range(src, start, start + len);
                }
                s
            },
        )
    }

    proptest! {
        #[test]
        fn display_parse_round_trip(s in arb_set()) {
            prop_assert_eq!(s.to_string().parse::<GtidSet>().unwrap(), s);
        }

        #[test]
        fn union_covers_both_and_commutes(a in arb_set(), b in arb_set()) {
            let u = a.union(&b);
            prop_assert!(u.covers(&a) && u.covers(&b));
            prop_assert_eq!(u, b.union(&a));
        }

        #[test]
        fn intersect_is_covered_by_both(a in arb_set(), b in arb_set()) {
            let i = a.intersect(&b);
            prop_assert!(a.covers(&i) && b.covers(&i));
        }

        #[test]
        fn covers_iff_union_is_identity(a in arb_set(), b in arb_set()) {
            prop_assert_eq!(a.covers(&b), a.union(&b) == a);
        }
    }
}
