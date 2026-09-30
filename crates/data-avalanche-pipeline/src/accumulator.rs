//! The accumulator (Data-Flow Phase 3): routes a pipeline's change stream into per-table buffers,
//! tracks transaction boundaries and the source position they complete, and decides when to flush.
//!
//! Positions: the **frontier** is the commit position of the last complete transaction (or a
//! keep-alive past it); the **checkpoint** is the frontier of the last flush. A flush covers
//! `checkpoint..=frontier` and moves the checkpoint to the frontier, which is where the pipeline
//! resumes once every change set of the flush is committed.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use data_avalanche_core::{
    CdcEvent, PositionRange, SourcePosition, TableChangeSet, TableRef, TableSchema,
    TransactionBoundary,
};

use crate::{AccumulatorError, FlushPolicy, FlushReason, Lineage, TableBuffer};

#[derive(Debug)]
struct OpenTxn {
    xid: u64,
    /// When the transaction's first event arrived.
    first_event_at: Option<Instant>,
}

/// The change sets of one flush.
#[derive(Debug)]
pub struct Flush {
    /// One per table with buffered changes; tables without any are left out.
    pub change_sets: Vec<TableChangeSet>,
    /// Where the pipeline resumes once every change set is committed.
    pub checkpoint: SourcePosition,
}

#[derive(Debug)]
pub struct Accumulator {
    lineage: Lineage,
    policy: FlushPolicy,
    tables: BTreeMap<TableRef, TableBuffer>,
    /// Halted tables: their events are dropped.
    halted: BTreeSet<TableRef>,
    txn: Option<OpenTxn>,
    checkpoint: SourcePosition,
    frontier: SourcePosition,
    /// When the oldest flushable event (or frontier advance) arrived.
    flushable_since: Option<Instant>,
}

impl Accumulator {
    /// `resume_from` is the checkpoint the pipeline restarts at.
    pub fn new(lineage: Lineage, policy: FlushPolicy, resume_from: SourcePosition) -> Self {
        Self {
            lineage,
            policy,
            tables: BTreeMap::new(),
            halted: BTreeSet::new(),
            txn: None,
            checkpoint: resume_from.clone(),
            frontier: resume_from,
            flushable_since: None,
        }
    }

    /// `sink.flush` is reloadable (R): the next [`Self::due`] uses the new policy.
    pub fn set_policy(&mut self, policy: FlushPolicy) {
        self.policy = policy;
    }

    pub fn checkpoint(&self) -> &SourcePosition {
        &self.checkpoint
    }

    pub fn frontier(&self) -> &SourcePosition {
        &self.frontier
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }

    /// Starts tracking a table. `delete_extra` names the columns a delete carries besides the
    /// identifier.
    pub fn add_table(
        &mut self,
        schema: TableSchema,
        delete_extra: &[String],
    ) -> Result<(), AccumulatorError> {
        let table = schema.table().clone();
        if self.tables.contains_key(&table) {
            return Err(protocol(format!("table {table} is already tracked")));
        }
        let buffer =
            TableBuffer::new(schema, delete_extra).map_err(|reason| AccumulatorError::Halt {
                table: table.clone(),
                reason,
            })?;
        self.halted.remove(&table);
        self.tables.insert(table, buffer);
        Ok(())
    }

    /// Stops a table after a halt: its buffered changes are dropped and its later events ignored.
    pub fn halt_table(&mut self, table: &TableRef) {
        self.tables.remove(table);
        self.halted.insert(table.clone());
    }

    pub fn begin(&mut self, xid: u64) -> Result<(), AccumulatorError> {
        if let Some(open) = &self.txn {
            return Err(protocol(format!(
                "transaction {xid} begins while {} is open",
                open.xid
            )));
        }
        self.txn = Some(OpenTxn {
            xid,
            first_event_at: None,
        });
        Ok(())
    }

    pub fn apply(&mut self, event: &CdcEvent, now: Instant) -> Result<(), AccumulatorError> {
        let Some(txn) = &mut self.txn else {
            return Err(protocol(format!(
                "an event of {} outside a transaction",
                event.table
            )));
        };
        if event.xid != txn.xid {
            return Err(protocol(format!(
                "an event of transaction {} inside {}",
                event.xid, txn.xid
            )));
        }
        if self.halted.contains(&event.table) {
            return Ok(());
        }
        let buffer =
            self.tables
                .get_mut(&event.table)
                .ok_or_else(|| AccumulatorError::UnknownTable {
                    table: event.table.clone(),
                })?;
        buffer.apply(event)?;
        txn.first_event_at.get_or_insert(now);
        Ok(())
    }

    /// The source committed the open transaction: its changes become flushable and the frontier
    /// moves to its commit position.
    pub fn commit(
        &mut self,
        boundary: &TransactionBoundary,
        now: Instant,
    ) -> Result<(), AccumulatorError> {
        let open = self.txn.as_ref().map(|t| t.xid);
        if open != Some(boundary.xid) {
            return Err(protocol(format!(
                "commit of transaction {} while {open:?} is open",
                boundary.xid
            )));
        }
        self.advance(&boundary.commit_pos)?;
        let txn = self.txn.take().expect("checked above");
        for buffer in self.tables.values_mut() {
            buffer.commit_txn();
        }
        let since = txn.first_event_at.unwrap_or(now);
        self.flushable_since = Some(self.flushable_since.map_or(since, |s| s.min(since)));
        Ok(())
    }

    /// A position the source reports with no transaction (PostgreSQL keep-alive, an empty or
    /// filtered transaction): the frontier moves so the checkpoint can follow. Ignored inside a
    /// transaction or when it isn't past the frontier.
    pub fn keepalive(
        &mut self,
        position: &SourcePosition,
        now: Instant,
    ) -> Result<(), AccumulatorError> {
        if self.txn.is_some() || self.frontier.covers(position)? {
            return Ok(());
        }
        self.advance(position)?;
        self.flushable_since.get_or_insert(now);
        Ok(())
    }

    fn advance(&mut self, to: &SourcePosition) -> Result<(), AccumulatorError> {
        if !to.covers(&self.frontier)? {
            return Err(protocol(format!(
                "position {to} is behind the frontier {}",
                self.frontier
            )));
        }
        self.frontier = to.clone();
        Ok(())
    }

    /// Whether a flush is due at `now`. Called after each boundary and on a timer.
    pub fn due(&self, now: Instant) -> Option<FlushReason> {
        let since = self.flushable_since?;
        let p = &self.policy;
        if p.seconds == Some(0) {
            return Some(FlushReason::Boundary);
        }
        if p.seconds
            .is_some_and(|s| now.duration_since(since) >= Duration::from_secs(s))
        {
            return Some(FlushReason::Age);
        }
        let events: u64 = self
            .tables
            .values()
            .map(TableBuffer::committed_events)
            .sum();
        if p.rows.is_some_and(|r| events >= r) {
            return Some(FlushReason::Rows);
        }
        let bytes: usize = self.tables.values().map(TableBuffer::committed_bytes).sum();
        if p.bytes.is_some_and(|b| bytes as u64 >= b) {
            return Some(FlushReason::Bytes);
        }
        None
    }

    /// Takes every complete transaction as change sets covering `checkpoint..=frontier`; an open
    /// transaction stays buffered.
    pub fn flush(&mut self) -> Flush {
        let position = PositionRange {
            start: self.checkpoint.clone(),
            end: self.frontier.clone(),
        };
        let change_sets = self
            .tables
            .values_mut()
            .filter(|b| b.committed_events() > 0)
            .map(|b| b.flush(&self.lineage, position.clone(), None))
            .collect();
        self.checkpoint = self.frontier.clone();
        self.flushable_since = None;
        Flush {
            change_sets,
            checkpoint: self.checkpoint.clone(),
        }
    }
}

fn protocol(detail: String) -> AccumulatorError {
    AccumulatorError::Protocol { detail }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{
        ColumnSchema, ColumnValue, ColumnValueType as V, Op, Row, SourceSeq,
    };

    use super::*;

    fn table(name: &str) -> TableRef {
        TableRef::new("public", name)
    }

    fn schema(name: &str) -> TableSchema {
        TableSchema::new(
            table(name),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn lsn(n: u64) -> SourcePosition {
        SourcePosition::Pg(Lsn(n))
    }

    fn acc(policy: FlushPolicy) -> Accumulator {
        let lineage = Lineage {
            source_db: "db".into(),
            pipeline: "p".into(),
        };
        let mut a = Accumulator::new(lineage, policy, lsn(100));
        a.add_table(schema("a"), &[]).unwrap();
        a.add_table(schema("b"), &[]).unwrap();
        a
    }

    fn insert(t: &str, xid: u64, seq: u64, id: i64) -> CdcEvent {
        CdcEvent {
            op: Op::Insert,
            table: table(t),
            before: None,
            after: Some(Row(vec![
                ("id".into(), ColumnValue::Int64(id)),
                ("doc".into(), ColumnValue::Text("x".into())),
            ])),
            source_pos: lsn(seq + 1000),
            change_seq: SourceSeq::Pg(Lsn(seq)),
            xid,
            commit_ts_micros: 1,
        }
    }

    fn boundary(xid: u64, pos: u64) -> TransactionBoundary {
        TransactionBoundary {
            xid,
            commit_pos: lsn(pos),
            commit_ts_micros: 1,
        }
    }

    /// One transaction of `events`, each `(table, id)`, committed at `pos`.
    fn txn(a: &mut Accumulator, xid: u64, pos: u64, events: &[(&str, i64)], now: Instant) {
        a.begin(xid).unwrap();
        for (i, (t, id)) in events.iter().enumerate() {
            a.apply(&insert(t, xid, pos * 10 + i as u64, *id), now)
                .unwrap();
        }
        a.commit(&boundary(xid, pos), now).unwrap();
    }

    fn only_time(seconds: u64) -> FlushPolicy {
        FlushPolicy {
            seconds: Some(seconds),
            rows: None,
            bytes: None,
        }
    }

    #[test]
    fn a_flush_covers_the_checkpoint_to_the_last_commit_per_table() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        txn(&mut a, 1, 200, &[("a", 1), ("b", 1)], t0);
        txn(&mut a, 2, 300, &[("a", 2)], t0);
        let f = a.flush();
        assert_eq!(f.checkpoint, lsn(300));
        assert_eq!(f.change_sets.len(), 2);
        for cs in &f.change_sets {
            cs.validate().unwrap();
            assert_eq!(
                (&cs.position.start, &cs.position.end),
                (&lsn(100), &lsn(300))
            );
        }
        assert_eq!(f.change_sets[0].upsert_rows(), 2);
        txn(&mut a, 3, 400, &[("b", 5)], t0);
        let f = a.flush();
        assert_eq!(
            f.change_sets.len(),
            1,
            "a table without changes gets no change set"
        );
        assert_eq!(f.change_sets[0].position.start, lsn(300));
    }

    #[test]
    fn an_open_transaction_stays_out_of_a_flush() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        txn(&mut a, 1, 200, &[("a", 1)], t0);
        a.begin(2).unwrap();
        a.apply(&insert("a", 2, 1, 9), t0).unwrap();
        let f = a.flush();
        assert_eq!(f.checkpoint, lsn(200));
        assert_eq!(f.change_sets[0].upsert_rows(), 1);
        a.commit(&boundary(2, 300), t0).unwrap();
        let f = a.flush();
        assert_eq!(
            (f.checkpoint, f.change_sets[0].upsert_rows()),
            (lsn(300), 1)
        );
    }

    #[test]
    fn nothing_flushable_is_never_due() {
        let t0 = Instant::now();
        let mut a = acc(only_time(0));
        assert_eq!(a.due(t0), None);
        a.begin(1).unwrap();
        a.apply(&insert("a", 1, 1, 1), t0).unwrap();
        assert_eq!(
            a.due(t0 + Duration::from_secs(3600)),
            None,
            "only an open transaction"
        );
    }

    #[test]
    fn seconds_zero_flushes_at_every_boundary() {
        let t0 = Instant::now();
        let mut a = acc(only_time(0));
        txn(&mut a, 1, 200, &[("a", 1)], t0);
        assert_eq!(a.due(t0), Some(FlushReason::Boundary));
        a.flush();
        assert_eq!(a.due(t0), None);
    }

    #[test]
    fn age_counts_from_the_oldest_event_not_the_commit() {
        let t0 = Instant::now();
        let mut a = acc(only_time(30));
        a.begin(1).unwrap();
        a.apply(&insert("a", 1, 1, 1), t0).unwrap();
        a.commit(&boundary(1, 200), t0 + Duration::from_secs(20))
            .unwrap();
        assert_eq!(a.due(t0 + Duration::from_secs(29)), None);
        assert_eq!(a.due(t0 + Duration::from_secs(30)), Some(FlushReason::Age));
        a.flush();
        assert_eq!(
            a.due(t0 + Duration::from_secs(90)),
            None,
            "the clock restarts after a flush"
        );
    }

    #[test]
    fn rows_and_bytes_trigger_on_committed_changes() {
        let t0 = Instant::now();
        let rows = FlushPolicy {
            seconds: None,
            rows: Some(3),
            bytes: None,
        };
        let mut a = acc(rows);
        txn(&mut a, 1, 200, &[("a", 1), ("a", 1)], t0);
        assert_eq!(a.due(t0), None);
        txn(&mut a, 2, 300, &[("b", 1)], t0);
        assert_eq!(
            a.due(t0),
            Some(FlushReason::Rows),
            "events count, not collapsed rows"
        );

        let bytes = FlushPolicy {
            seconds: None,
            rows: None,
            bytes: Some(1),
        };
        let mut a = acc(bytes);
        txn(&mut a, 1, 200, &[("a", 1)], t0);
        assert_eq!(a.due(t0), Some(FlushReason::Bytes));
    }

    #[test]
    fn disabled_triggers_never_fire_and_the_policy_reloads() {
        let t0 = Instant::now();
        let never = FlushPolicy {
            seconds: None,
            rows: None,
            bytes: None,
        };
        let mut a = acc(never);
        txn(&mut a, 1, 200, &[("a", 1)], t0);
        assert_eq!(a.due(t0 + Duration::from_secs(86_400)), None);
        a.set_policy(only_time(0));
        assert_eq!(a.due(t0), Some(FlushReason::Boundary));
    }

    #[test]
    fn keepalives_advance_the_frontier_between_transactions() {
        let t0 = Instant::now();
        let mut a = acc(only_time(30));
        a.keepalive(&lsn(150), t0).unwrap();
        a.keepalive(&lsn(120), t0).unwrap();
        assert_eq!(a.frontier(), &lsn(150), "an older keep-alive is ignored");
        a.begin(1).unwrap();
        a.keepalive(&lsn(170), t0).unwrap();
        assert_eq!(a.frontier(), &lsn(150), "ignored inside a transaction");
        a.commit(&boundary(1, 200), t0).unwrap();
        assert_eq!(a.due(t0 + Duration::from_secs(30)), Some(FlushReason::Age));
        let f = a.flush();
        assert!(
            f.change_sets.is_empty(),
            "an empty transaction only moves the checkpoint"
        );
        assert_eq!(f.checkpoint, lsn(200));
    }

    #[test]
    fn stream_protocol_violations_are_errors() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        let is_protocol =
            |r: Result<(), AccumulatorError>| matches!(r, Err(AccumulatorError::Protocol { .. }));
        assert!(is_protocol(a.apply(&insert("a", 1, 1, 1), t0)));
        assert!(is_protocol(a.commit(&boundary(1, 200), t0)));
        a.begin(1).unwrap();
        assert!(is_protocol(a.begin(2)));
        assert!(is_protocol(a.apply(&insert("a", 2, 1, 1), t0)));
        assert!(is_protocol(a.commit(&boundary(2, 200), t0)));
        assert!(
            a.in_transaction(),
            "a rejected commit leaves the transaction open"
        );
        assert!(
            is_protocol(a.commit(&boundary(1, 50), t0)),
            "behind the frontier"
        );
        assert!(matches!(
            a.apply(&insert("nope", 1, 1, 1), t0),
            Err(AccumulatorError::UnknownTable { .. })
        ));
        assert!(is_protocol(a.add_table(schema("a"), &[])));
    }

    #[test]
    fn a_halted_table_drops_its_changes_and_the_rest_continue() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        a.begin(1).unwrap();
        let mut bad = insert("a", 1, 1, 1);
        bad.after.as_mut().unwrap().0[0].1 = ColumnValue::Null;
        let err = a.apply(&bad, t0).unwrap_err();
        assert!(matches!(err, AccumulatorError::Halt { .. }), "{err}");
        a.halt_table(&table("a"));
        a.apply(&insert("a", 1, 2, 2), t0).unwrap();
        a.apply(&insert("b", 1, 3, 3), t0).unwrap();
        a.commit(&boundary(1, 200), t0).unwrap();
        let f = a.flush();
        assert_eq!(f.change_sets.len(), 1);
        assert_eq!(f.change_sets[0].table, table("b"));
    }
}
