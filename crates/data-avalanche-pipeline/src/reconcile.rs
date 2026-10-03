//! A snapshot chunk against the stream (DA-SNAP-9, Data-Flow Phase 1). A chunk row must not undo
//! a change the pipeline already took from the stream. That can happen when the chunk's read
//! didn't see the change's transaction: it committed after the read started, or it was committed
//! and decoded but not yet visible (a synchronous commit waiting for its standby, measured on
//! PostgreSQL 18).
//!
//! The reconciler logs the transactions applied from the stream while a snapshot runs, with the
//! keys they wrote in the tables being snapshotted. Before a chunk is emitted, its rows for keys
//! that a logged transaction wrote, and that the chunk's read didn't see, are dropped: the stream
//! already has those keys. A logged TRUNCATE the read didn't see drops the whole chunk.
//!
//! A logged transaction is kept until a chunk proves it visible, and until every chunk still
//! being read was issued after that proof: a read that starts later sees what an earlier read
//! saw. A source that can't say what a chunk's read saw ([`Chunk::visibility`] `None`) gets the
//! window rule: the transactions applied after the chunk was issued.
//!
//! [`Chunk::visibility`]: data_avalanche_core::Chunk::visibility

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use data_avalanche_core::{ChunkVisibility, Row, TableRef};

use crate::buffer::Key;

/// What one stream transaction wrote in the tables being snapshotted.
#[derive(Debug, Default)]
pub(crate) struct Touched {
    pub keys: HashMap<TableRef, HashSet<Key>>,
    pub truncated: BTreeSet<TableRef>,
}

impl Touched {
    pub fn key(&mut self, table: &TableRef, key: Key) {
        self.keys.entry(table.clone()).or_default().insert(key);
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.truncated.is_empty()
    }
}

/// One chunk read in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ticket {
    id: u64,
    issued: u64,
}

#[derive(Debug)]
struct Entry {
    /// When it was applied, on the reconciler's clock.
    at: u64,
    xid: u64,
    touched: Touched,
    /// When a chunk first proved it visible.
    seen: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct Reconciler {
    /// Orders applies, issues and reconciliations.
    clock: u64,
    next_id: u64,
    log: Vec<Entry>,
    /// The chunks being read: id → when issued.
    outstanding: BTreeMap<u64, u64>,
}

impl Reconciler {
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// A stream transaction the pipeline applied.
    pub fn applied(&mut self, xid: u64, touched: Touched) {
        let at = self.tick();
        if !touched.is_empty() {
            self.log.push(Entry {
                at,
                xid,
                touched,
                seen: None,
            });
        }
    }

    /// A chunk read starts.
    pub fn issue(&mut self) -> Ticket {
        let issued = self.tick();
        self.next_id += 1;
        self.outstanding.insert(self.next_id, issued);
        Ticket {
            id: self.next_id,
            issued,
        }
    }

    /// A chunk read failed: nothing of it is emitted.
    pub fn cancel(&mut self, ticket: Ticket) {
        self.outstanding.remove(&ticket.id);
    }

    /// The rows of a chunk of `table` the stream holds no newer change for. Called when the chunk
    /// is emitted, so it counts every transaction applied until then.
    pub fn reconcile(
        &mut self,
        ticket: Ticket,
        table: &TableRef,
        visibility: Option<&ChunkVisibility>,
        rows: Vec<(Key, Row)>,
    ) -> Vec<Row> {
        self.outstanding.remove(&ticket.id);
        let now = self.tick();
        let mut stale: HashSet<&Key> = HashSet::new();
        let mut truncated = false;
        for e in &mut self.log {
            let hidden = match visibility {
                Some(ChunkVisibility::Xids(s)) => {
                    let sees = s.sees(e.xid);
                    if sees {
                        e.seen.get_or_insert(now);
                    }
                    !sees
                }
                None => e.at > ticket.issued,
            };
            if hidden {
                truncated |= e.touched.truncated.contains(table);
                if let Some(keys) = e.touched.keys.get(table) {
                    stale.extend(keys);
                }
            }
        }
        let kept = if truncated {
            vec![]
        } else {
            rows.into_iter()
                .filter(|(k, _)| !stale.contains(k))
                .map(|(_, r)| r)
                .collect()
        };
        let horizon = self.outstanding.values().min().copied().unwrap_or(u64::MAX);
        self.log.retain(|e| match visibility {
            Some(_) => e.seen.is_none_or(|seen| seen >= horizon),
            None => e.at >= horizon,
        });
        kept
    }

    /// No snapshot runs any more: nothing needs the log.
    pub fn clear(&mut self) {
        self.log.clear();
        self.outstanding.clear();
    }

    #[cfg(test)]
    fn logged(&self) -> Vec<u64> {
        self.log.iter().map(|e| e.xid).collect()
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnValue, XidSnapshot};

    use super::*;
    use crate::buffer::identity;
    use crate::test_support::{row, schema, t};

    fn key(id: i64) -> Key {
        identity(&schema("a"), &row(id, "x")).unwrap()
    }

    fn chunk(ids: &[i64]) -> Vec<(Key, Row)> {
        ids.iter().map(|&id| (key(id), row(id, "snap"))).collect()
    }

    fn ids(rows: &[Row]) -> Vec<i64> {
        rows.iter()
            .map(|r| match r.get("id") {
                Some(ColumnValue::Int64(i)) => *i,
                other => panic!("{other:?}"),
            })
            .collect()
    }

    fn wrote(table: &str, ids: &[i64]) -> Touched {
        let mut t = Touched::default();
        for &id in ids {
            t.key(&TableRef::new("public", table), key(id));
        }
        t
    }

    /// A snapshot that sees every transaction below `xmax` except `xip`.
    fn snap(xmax: u64, xip: &[u64]) -> ChunkVisibility {
        ChunkVisibility::Xids(XidSnapshot {
            xmin: xip.iter().copied().min().unwrap_or(xmax),
            xmax,
            xip: xip.to_vec(),
        })
    }

    #[test]
    fn a_change_the_read_didnt_see_drops_its_rows() {
        let mut r = Reconciler::default();
        let c = r.issue();
        r.applied(700, wrote("a", &[2]));
        let kept = r.reconcile(c, &t("a"), Some(&snap(700, &[])), chunk(&[1, 2, 3]));
        assert_eq!(ids(&kept), [1, 3], "700 committed after the read began");
    }

    #[test]
    fn a_change_applied_before_the_issue_but_still_invisible_drops_its_rows() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[2]));
        let c = r.issue();
        let kept = r.reconcile(c, &t("a"), Some(&snap(701, &[700])), chunk(&[1, 2]));
        assert_eq!(
            ids(&kept),
            [1],
            "a synchronous commit decoded before it was visible"
        );
        assert_eq!(r.logged(), [700], "kept until a read sees it");
    }

    #[test]
    fn a_change_the_read_saw_keeps_the_rows_and_goes() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[2]));
        let c = r.issue();
        let kept = r.reconcile(c, &t("a"), Some(&snap(701, &[])), chunk(&[1, 2]));
        assert_eq!(ids(&kept), [1, 2]);
        assert!(r.logged().is_empty(), "seen, and no read is open");
    }

    #[test]
    fn an_unseen_truncate_drops_the_chunk() {
        let mut r = Reconciler::default();
        let c = r.issue();
        let mut tr = Touched::default();
        tr.truncated.insert(t("a"));
        r.applied(700, tr);
        let kept = r.reconcile(c, &t("a"), Some(&snap(700, &[])), chunk(&[1, 2]));
        assert!(kept.is_empty());
    }

    #[test]
    fn another_tables_keys_dont_drop_rows() {
        let mut r = Reconciler::default();
        let c = r.issue();
        r.applied(700, wrote("b", &[1]));
        let kept = r.reconcile(c, &t("a"), Some(&snap(700, &[])), chunk(&[1]));
        assert_eq!(ids(&kept), [1]);
    }

    /// D was issued before C proved 700 visible, so D's read may be older than C's: 700 stays
    /// until D is reconciled.
    #[test]
    fn a_proof_covers_only_the_reads_issued_after_it() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[2]));
        let d = r.issue();
        let c = r.issue();
        let kept = r.reconcile(c, &t("a"), Some(&snap(701, &[])), chunk(&[2]));
        assert_eq!(ids(&kept), [2]);
        assert_eq!(r.logged(), [700], "D is still being read");
        let kept = r.reconcile(d, &t("a"), Some(&snap(701, &[700])), chunk(&[2, 3]));
        assert_eq!(ids(&kept), [3], "D's read didn't see 700");
        assert!(r.logged().is_empty());
    }

    #[test]
    fn a_failed_read_doesnt_hold_the_log() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[2]));
        let d = r.issue();
        let c = r.issue();
        r.reconcile(c, &t("a"), Some(&snap(701, &[])), chunk(&[]));
        r.cancel(d);
        let e = r.issue();
        r.reconcile(e, &t("a"), Some(&snap(701, &[])), chunk(&[]));
        assert!(r.logged().is_empty());
    }

    #[test]
    fn without_visibility_the_changes_applied_after_the_issue_drop_their_rows() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[1]));
        let c = r.issue();
        r.applied(701, wrote("a", &[2]));
        let kept = r.reconcile(c, &t("a"), None, chunk(&[1, 2, 3]));
        assert_eq!(ids(&kept), [1, 3]);
        assert!(r.logged().is_empty(), "no read is open");
    }
}
