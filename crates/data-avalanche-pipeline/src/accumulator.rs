//! The accumulator (Data-Flow Phase 3): routes a pipeline's change stream into per-table buffers,
//! tracks transaction boundaries and the source position they complete, and decides when to flush.
//!
//! Positions: the **frontier** is the commit position of the last complete transaction (or a
//! keep-alive past it); the **checkpoint** is the frontier of the last flush. A flush covers
//! `checkpoint..=frontier` and moves the checkpoint to the frontier, which is where the pipeline
//! resumes once every change set of the flush is committed. Each table's change set also carries
//! its **last change**: the commit position of the last transaction that wrote the table, which
//! the sink's position guard compares (a replay's frontier may be past the table's marker while
//! its transactions aren't).

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use data_avalanche_core::{
    CdcEvent, PositionRange, SourcePosition, SplitInfo, TableChangeSet, TableRef, TableSchema,
    TransactionBoundary,
};

use crate::{AccumulatorError, FlushPolicy, FlushReason, Lineage, TableBuffer, TxnLimits};

#[derive(Debug)]
struct OpenTxn {
    xid: u64,
    /// When the transaction's first event (since its last slice) arrived.
    first_event_at: Option<Instant>,
    /// Slices flushed so far.
    slices: u32,
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
    limits: TxnLimits,
    tables: BTreeMap<TableRef, TableBuffer>,
    /// Halted tables: their events are dropped.
    halted: BTreeSet<TableRef>,
    /// Per table with changes no flush has taken: the commit position of the last transaction
    /// that wrote it (a slice's: the transaction's start).
    last_change: BTreeMap<TableRef, SourcePosition>,
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
            limits: TxnLimits::default(),
            tables: BTreeMap::new(),
            halted: BTreeSet::new(),
            last_change: BTreeMap::new(),
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

    /// The lineage the next flushes write (a renamed pipeline, config §12.3).
    pub fn set_lineage(&mut self, lineage: Lineage) {
        self.lineage = lineage;
    }

    /// The transaction limits (reloadable, like the flush policy). The default never splits.
    pub fn set_txn_limits(&mut self, limits: TxnLimits) {
        self.limits = limits;
    }

    fn open_events(&self) -> u64 {
        self.tables.values().map(TableBuffer::open_events).sum()
    }

    fn open_bytes(&self) -> usize {
        self.tables.values().map(TableBuffer::open_bytes).sum()
    }

    /// Bytes buffered and not flushed yet: the committed transactions' and the open one's.
    pub fn buffered_bytes(&self) -> usize {
        self.tables
            .values()
            .map(|b| b.committed_bytes() + b.open_bytes())
            .sum()
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
    /// Calling it again changes nothing.
    pub fn halt_table(&mut self, table: &TableRef) {
        self.tables.remove(table);
        self.last_change.remove(table);
        self.halted.insert(table.clone());
    }

    /// Whether the table has committed changes no flush has taken yet.
    pub fn has_committed(&self, table: &TableRef) -> bool {
        self.tables
            .get(table)
            .is_some_and(|b| b.committed_events() > 0)
    }

    /// Halts the table of a [`AccumulatorError::Halt`] before handing the error on: its open
    /// transaction's changes are dropped and its later events ignored. Its committed changes stay
    /// for a flush until [`Self::halt_table`], so the caller can hand them to the sink first.
    fn halting<T>(&mut self, r: Result<T, AccumulatorError>) -> Result<T, AccumulatorError> {
        if let Err(AccumulatorError::Halt { table, .. }) = &r {
            if let Some(b) = self.tables.get_mut(table) {
                b.discard_open();
            }
            self.halted.insert(table.clone());
        }
        r
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
            slices: 0,
        });
        Ok(())
    }

    /// Buffers one event of the open transaction. The transaction buffer ceiling
    /// (`TxnLimits::buffer_bytes`) is checked here: with splitting off, reaching it halts
    /// ([`AccumulatorError::TransactionTooLarge`]); with splitting on, an event that arrives once
    /// it is reached is refused with [`AccumulatorError::SplitRequired`] and nothing changes —
    /// split, commit the slice, then apply the event again. The split empties the open
    /// transaction, so the retry always makes progress. An [`AccumulatorError::Halt`] has already
    /// stopped the table, as it has from [`Self::truncate`] and [`Self::set_schema`]: its later
    /// events are ignored, and only its committed changes stay, until [`Self::halt_table`].
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
        if !self.tables.contains_key(&event.table) {
            return Err(AccumulatorError::UnknownTable {
                table: event.table.clone(),
            });
        }
        // Past the ceiling with something to split: an empty open transaction always takes the
        // event, so a retry after the split makes progress whatever the limit.
        let open_bytes: usize = self.tables.values().map(TableBuffer::open_bytes).sum();
        if self.limits.batch_rows.is_some()
            && open_bytes > 0
            && open_bytes as u64 >= self.limits.buffer_bytes
        {
            return Err(AccumulatorError::SplitRequired);
        }
        let buffer = self.tables.get_mut(&event.table).expect("checked above");
        if let Err(e) = buffer.apply(event) {
            return self.halting(Err(e));
        }
        txn.first_event_at.get_or_insert(now);
        if self.limits.batch_rows.is_none() {
            let bytes = self.open_bytes();
            if bytes as u64 >= self.limits.buffer_bytes {
                return Err(AccumulatorError::TransactionTooLarge {
                    bytes,
                    limit: self.limits.buffer_bytes,
                });
            }
        }
        Ok(())
    }

    /// A TRUNCATE of `tables` in the open transaction (Data-Flow §TRUNCATE barrier). When one of them
    /// has committed changes, nothing is applied and the result is
    /// [`AccumulatorError::FlushRequired`]: flush, commit the change sets, then truncate again.
    pub fn truncate(&mut self, tables: &[TableRef], now: Instant) -> Result<(), AccumulatorError> {
        if self.txn.is_none() {
            return Err(protocol("a TRUNCATE outside a transaction".into()));
        }
        let mut buffers = Vec::new();
        for t in tables.iter().filter(|t| !self.halted.contains(t)) {
            let b = self
                .tables
                .get(t)
                .ok_or_else(|| AccumulatorError::UnknownTable { table: t.clone() })?;
            if b.committed_events() > 0 {
                return Err(AccumulatorError::FlushRequired { table: t.clone() });
            }
            buffers.push(t.clone());
        }
        for t in &buffers {
            let r = self.tables.get_mut(t).expect("checked above").truncate();
            self.halting(r)?;
        }
        let txn = self.txn.as_mut().expect("checked above");
        txn.first_event_at.get_or_insert(now);
        Ok(())
    }

    /// A table's new schema (`Schema_Change…` §8), after the classifier found the change safe and
    /// the sink evolved the table, or found it ignorable. [`AccumulatorError::FlushRequired`] when
    /// the table has committed changes in a shape the new schema changes. A halted table is
    /// skipped.
    pub fn set_schema(
        &mut self,
        schema: TableSchema,
        delete_extra: &[String],
    ) -> Result<(), AccumulatorError> {
        let table = schema.table().clone();
        if self.halted.contains(&table) {
            return Ok(());
        }
        let r = self
            .tables
            .get_mut(&table)
            .ok_or(AccumulatorError::UnknownTable { table })?
            .set_schema(schema, delete_extra);
        self.halting(r)
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
        self.commit_open(&boundary.commit_pos);
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

    /// Whether a flush is due at `now`. Called after each boundary and on a timer. With splitting
    /// on, [`FlushReason::Split`] once the open transaction reaches `batch_rows` events or the
    /// byte ceiling; the ceiling itself doesn't depend on how often this is called, since
    /// [`Self::apply`] refuses an event past it ([`AccumulatorError::SplitRequired`]).
    pub fn due(&self, now: Instant) -> Option<FlushReason> {
        if let (Some(_), Some(rows)) = (&self.txn, self.limits.batch_rows)
            && (self.open_events() >= rows || self.open_bytes() as u64 >= self.limits.buffer_bytes)
        {
            return Some(FlushReason::Split);
        }
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

    /// Moves every buffer's open transaction to its committed layer; a table it wrote has its
    /// last change at `at`.
    fn commit_open(&mut self, at: &SourcePosition) {
        for (table, buffer) in &mut self.tables {
            if buffer.open_events() > 0 {
                self.last_change.insert(table.clone(), at.clone());
            }
            buffer.commit_txn();
        }
    }

    /// Takes the committed layers of the tables that have changes as change sets.
    fn take(&mut self, split: Option<SplitInfo>) -> Vec<TableChangeSet> {
        let (start, end) = (&self.checkpoint, &self.frontier);
        self.tables
            .iter_mut()
            .filter(|(_, b)| b.committed_events() > 0)
            .map(|(table, b)| {
                let position = PositionRange {
                    start: start.clone(),
                    end: end.clone(),
                    last_change: self
                        .last_change
                        .remove(table)
                        .unwrap_or_else(|| end.clone()),
                };
                b.flush(&self.lineage, position, split.clone())
            })
            .collect()
    }

    /// Takes every complete transaction as change sets covering `checkpoint..=frontier`; an open
    /// transaction stays buffered.
    pub fn flush(&mut self) -> Flush {
        let change_sets = self.take(None);
        self.checkpoint = self.frontier.clone();
        self.flushable_since = None;
        Flush {
            change_sets,
            checkpoint: self.checkpoint.clone(),
        }
    }

    /// Flushes a slice of the open transaction together with everything already complete
    /// (Large_Transaction §1). The change sets are marked [`SplitInfo`] and end at the frontier —
    /// the transaction's start — which is also the returned checkpoint: until the source commit,
    /// a restart replays the whole transaction. The transaction stays open; what it writes after
    /// its last slice goes out with the next regular flush.
    pub fn split(&mut self) -> Result<Flush, AccumulatorError> {
        let Some(txn) = &mut self.txn else {
            return Err(protocol("a split outside a transaction".into()));
        };
        txn.slices += 1;
        txn.first_event_at = None;
        let split = SplitInfo {
            txn_start: self.frontier.clone(),
            slice: txn.slices,
        };
        let start = split.txn_start.clone();
        self.commit_open(&start);
        let change_sets = self.take(Some(split));
        self.checkpoint = self.frontier.clone();
        self.flushable_since = None;
        Ok(Flush {
            change_sets,
            checkpoint: self.checkpoint.clone(),
        })
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
        let last: Vec<_> = f
            .change_sets
            .iter()
            .map(|cs| (cs.table.name.as_str(), &cs.position.last_change))
            .collect();
        assert_eq!(last, [("a", &lsn(300)), ("b", &lsn(200))]);
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
    fn a_position_of_another_dialect_is_a_position_error() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        let binlog = SourcePosition::parse_tagged("binlog:b.000001:9").unwrap();
        let is_position =
            |r: Result<(), AccumulatorError>| matches!(r, Err(AccumulatorError::Position(_)));
        assert!(is_position(a.keepalive(&binlog, t0)));
        a.begin(1).unwrap();
        let foreign = TransactionBoundary {
            xid: 1,
            commit_pos: binlog,
            commit_ts_micros: 1,
        };
        assert!(is_position(a.commit(&foreign, t0)));
        assert!(
            a.in_transaction(),
            "a rejected commit leaves the transaction open"
        );
        assert_eq!(a.frontier(), &lsn(100), "the frontier didn't move");
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
    fn a_keepalive_moves_the_end_but_not_the_last_change() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        txn(&mut a, 1, 200, &[("a", 1)], t0);
        a.keepalive(&lsn(250), t0).unwrap();
        let f = a.flush();
        let cs = &f.change_sets[0];
        cs.validate().unwrap();
        assert_eq!(
            (&cs.position.end, &cs.position.last_change),
            (&lsn(250), &lsn(200))
        );
        assert_eq!(f.checkpoint, lsn(250));
        txn(&mut a, 2, 300, &[("a", 2)], t0);
        assert_eq!(
            a.flush().change_sets[0].position.last_change,
            lsn(300),
            "the next flush starts afresh"
        );
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
    fn a_truncate_seals_flushes_then_applies() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        txn(&mut a, 1, 200, &[("a", 1), ("b", 1)], t0);
        a.begin(2).unwrap();
        a.apply(&insert("b", 2, 1, 2), t0).unwrap();
        let err = a.truncate(&[table("b"), table("a")], t0).unwrap_err();
        assert!(
            matches!(err, AccumulatorError::FlushRequired { .. }),
            "{err}"
        );

        let sealed = a.flush();
        assert_eq!(sealed.checkpoint, lsn(200));
        assert!(sealed.change_sets.iter().all(|cs| !cs.truncate));
        a.truncate(&[table("b"), table("a")], t0).unwrap();
        a.apply(&insert("a", 2, 2, 3), t0).unwrap();
        a.commit(&boundary(2, 300), t0).unwrap();
        let f = a.flush();
        assert_eq!(f.change_sets.len(), 2);
        let (ca, cb) = (&f.change_sets[0], &f.change_sets[1]);
        assert!(ca.truncate && cb.truncate);
        assert_eq!(
            (ca.upsert_rows(), cb.upsert_rows()),
            (1, 0),
            "b's pre-truncate row is gone"
        );
        assert_eq!(ca.position.start, lsn(200));
    }

    #[test]
    fn a_schema_change_seals_flushes_then_applies() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        txn(&mut a, 1, 200, &[("a", 1)], t0);
        let wider = TableSchema::new(
            table("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("n", V::Int32, "integer"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        let err = a.set_schema(wider.clone(), &[]).unwrap_err();
        assert!(
            matches!(err, AccumulatorError::FlushRequired { .. }),
            "{err}"
        );
        let old = a.flush();
        assert_eq!(old.change_sets[0].schema.fields().len(), 2 + 6);
        a.set_schema(wider, &[]).unwrap();
        a.begin(2).unwrap();
        let mut e = insert("a", 2, 1, 2);
        e.after
            .as_mut()
            .unwrap()
            .0
            .push(("n".into(), ColumnValue::Int32(4)));
        a.apply(&e, t0).unwrap();
        a.commit(&boundary(2, 300), t0).unwrap();
        let new = a.flush();
        new.change_sets[0].validate().unwrap();
        assert_eq!(new.change_sets[0].schema.fields().len(), 3 + 6);
        assert!(matches!(
            a.set_schema(schema("zzz"), &[]),
            Err(AccumulatorError::UnknownTable { .. })
        ));
    }

    #[test]
    fn a_truncate_needs_a_transaction_and_skips_halted_tables() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        assert!(matches!(
            a.truncate(&[table("a")], t0),
            Err(AccumulatorError::Protocol { .. })
        ));
        a.halt_table(&table("a"));
        a.begin(1).unwrap();
        a.truncate(&[table("a"), table("b")], t0).unwrap();
        a.commit(&boundary(1, 200), t0).unwrap();
        let f = a.flush();
        assert_eq!(f.change_sets.len(), 1);
        assert_eq!(f.change_sets[0].table, table("b"));
    }

    fn split_every(rows: u64) -> TxnLimits {
        TxnLimits {
            batch_rows: Some(rows),
            buffer_bytes: 1 << 30,
        }
    }

    #[test]
    fn a_large_transaction_halts_when_splitting_is_off() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        a.set_txn_limits(TxnLimits {
            batch_rows: None,
            buffer_bytes: 1,
        });
        a.begin(1).unwrap();
        let err = a.apply(&insert("a", 1, 1, 1), t0).unwrap_err();
        assert!(
            matches!(err, AccumulatorError::TransactionTooLarge { limit: 1, .. }),
            "{err}"
        );
    }

    #[test]
    fn slices_pin_the_checkpoint_at_the_transaction_start() {
        let t0 = Instant::now();
        let mut a = acc(only_time(0));
        a.set_txn_limits(split_every(2));
        txn(&mut a, 1, 200, &[("a", 1)], t0);
        a.begin(2).unwrap();
        a.apply(&insert("a", 2, 1, 2), t0).unwrap();
        assert_eq!(a.due(t0), Some(FlushReason::Boundary), "not yet a slice");
        a.apply(&insert("b", 2, 2, 2), t0).unwrap();
        assert_eq!(a.due(t0), Some(FlushReason::Split));

        let first = a.split().unwrap();
        assert_eq!(first.checkpoint, lsn(200));
        assert_eq!(first.change_sets.len(), 2);
        for cs in &first.change_sets {
            cs.validate().unwrap();
            assert_eq!(
                cs.split.as_ref().map(|s| (&s.txn_start, s.slice)),
                Some((&lsn(200), 1))
            );
            assert_eq!(
                (
                    &cs.position.start,
                    &cs.position.end,
                    &cs.position.last_change
                ),
                (&lsn(100), &lsn(200), &lsn(200))
            );
        }
        assert_eq!(
            first.change_sets[0].upsert_rows(),
            2,
            "the complete transaction rides along"
        );
        assert_eq!(a.due(t0), None, "the open transaction was emptied");

        a.apply(&insert("a", 2, 3, 3), t0).unwrap();
        a.apply(&insert("a", 2, 4, 4), t0).unwrap();
        let second = a.split().unwrap();
        assert_eq!(second.change_sets[0].split.as_ref().unwrap().slice, 2);
        assert_eq!(second.change_sets[0].position.start, lsn(200));

        a.apply(&insert("a", 2, 5, 5), t0).unwrap();
        a.commit(&boundary(2, 300), t0).unwrap();
        let rest = a.flush();
        assert_eq!(rest.checkpoint, lsn(300));
        assert_eq!(rest.change_sets[0].position.last_change, lsn(300));
        assert!(
            rest.change_sets[0].split.is_none(),
            "the rest is a regular flush"
        );
        assert_eq!(rest.change_sets[0].position.start, lsn(200));
    }

    #[test]
    fn with_splitting_on_the_byte_ceiling_splits_early() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        a.set_txn_limits(TxnLimits {
            batch_rows: Some(1_000_000),
            buffer_bytes: 1,
        });
        a.begin(1).unwrap();
        a.apply(&insert("a", 1, 1, 1), t0).unwrap();
        assert_eq!(a.due(t0), Some(FlushReason::Split));
        assert!(matches!(a.commit(&boundary(1, 200), t0), Ok(())));
        assert!(
            matches!(a.split(), Err(AccumulatorError::Protocol { .. })),
            "no open transaction"
        );
    }

    #[test]
    fn with_splitting_on_apply_asks_for_a_split_at_the_byte_ceiling() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        a.set_txn_limits(TxnLimits {
            batch_rows: Some(1_000_000),
            buffer_bytes: 1,
        });
        a.begin(1).unwrap();
        a.apply(&insert("a", 1, 1, 1), t0).unwrap();
        let before = (a.open_bytes(), a.open_events());
        let next = insert("b", 1, 2, 2);
        let err = a.apply(&next, t0).unwrap_err();
        assert!(matches!(err, AccumulatorError::SplitRequired), "{err}");
        assert_eq!((a.open_bytes(), a.open_events()), before, "nothing changed");

        let slice = a.split().unwrap();
        assert_eq!(slice.change_sets.len(), 1);
        a.apply(&next, t0).unwrap();
        assert_eq!(a.open_events(), 1, "the retry is buffered");
    }

    #[test]
    fn a_halt_stops_the_table_even_before_the_orchestrator_does() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        a.begin(1).unwrap();
        let mut bad = insert("a", 1, 1, 1);
        bad.after.as_mut().unwrap().0[0].1 = ColumnValue::Null;
        let err = a.apply(&bad, t0).unwrap_err();
        assert!(matches!(err, AccumulatorError::Halt { .. }), "{err}");
        a.apply(&insert("a", 1, 2, 2), t0).unwrap();
        assert_eq!(a.open_events(), 0, "the halted table buffers nothing");
        a.apply(&insert("b", 1, 3, 3), t0).unwrap();
        a.commit(&boundary(1, 200), t0).unwrap();
        let f = a.flush();
        assert_eq!(f.change_sets.len(), 1);
        assert_eq!(f.change_sets[0].table, table("b"));
        a.halt_table(&table("a"));
        a.halt_table(&table("a"));
    }

    /// A halt keeps the table's committed changes for a flush before `halt_table`; its open
    /// transaction's changes go.
    #[test]
    fn a_halt_keeps_the_tables_committed_changes_until_halt_table() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        a.begin(1).unwrap();
        a.apply(&insert("a", 1, 1, 1), t0).unwrap();
        a.commit(&boundary(1, 200), t0).unwrap();
        a.begin(2).unwrap();
        a.apply(&insert("a", 2, 2, 2), t0).unwrap();
        let mut bad = insert("a", 2, 3, 3);
        bad.after.as_mut().unwrap().0[0].1 = ColumnValue::Null;
        let err = a.apply(&bad, t0).unwrap_err();
        assert!(matches!(err, AccumulatorError::Halt { .. }), "{err}");
        assert!(a.has_committed(&table("a")));
        assert_eq!(a.open_events(), 0, "the open transaction's changes went");
        let f = a.flush();
        assert_eq!(f.change_sets.len(), 1);
        assert_eq!(f.change_sets[0].upsert_rows(), 1, "transaction 1's row");
        assert_eq!(f.checkpoint, boundary(1, 200).commit_pos);
        a.halt_table(&table("a"));
        assert!(!a.has_committed(&table("a")));
        a.apply(&insert("a", 2, 4, 4), t0).unwrap();
        a.commit(&boundary(2, 300), t0).unwrap();
        assert!(
            a.flush().change_sets.is_empty(),
            "its later changes are ignored"
        );
    }

    #[test]
    fn a_schema_change_of_a_halted_table_is_ignored() {
        let t0 = Instant::now();
        let mut a = acc(FlushPolicy::default());
        a.halt_table(&table("a"));
        a.set_schema(schema("a"), &[]).unwrap();

        let other_key = TableSchema::new(
            table("b"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text").not_null(),
            ],
            vec!["doc".into()],
        )
        .unwrap();
        let err = a.set_schema(other_key, &[]).unwrap_err();
        assert!(matches!(err, AccumulatorError::Halt { .. }), "{err}");
        a.begin(1).unwrap();
        a.apply(&insert("b", 1, 1, 1), t0).unwrap();
        assert_eq!(a.open_events(), 0, "a halt from set_schema halts the table");
        a.set_schema(schema("b"), &[]).unwrap();
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
