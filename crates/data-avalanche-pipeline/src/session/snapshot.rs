//! The snapshot runner inside a stream session (Data-Flow Phase 1). Snapshots run while the
//! stream is read: the tables whose snapshot document says `in_progress` are read chunk by chunk
//! in key order, up to `parallelism` tables at a time.
//!
//! A chunk is emitted between two stream transactions, as a transaction of its own at the
//! frontier: its rows are `Op::Snapshot` events with the lowest `_seq`, so a stream change of the
//! same key buffered with them wins. Before that, the reconciler drops the rows the stream holds
//! newer changes for (DA-SNAP-9). The cursor is persisted once the flush that carries the chunk is
//! durable. A table's completion is persisted the same way, and only then does a `dump_*` table
//! stop streaming.
//!
//! A transaction that was in flight when the session started may have committed before its
//! checkpoint and be in the sink already, while a chunk can't see it yet. A table's chunks wait
//! until no such transaction writes the table (Q-M6a-2).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, Chunk, HaltReason, KeysetCursor, Op, SourceConnector, SourceError, SourcePosition,
    SourceSeq, TableRef, TableSchema, TransactionBoundary,
};
use data_avalanche_state::{SnapshotStatus, TableSnapshotDoc};
use tokio::task::JoinSet;

use super::{End, Session, protocol};
use crate::buffer::identity;
use crate::cursor;
use crate::reconcile::{Reconciler, Ticket, Touched};
use crate::{HookAction, HookPoint, ReconcileLog, SnapshotProgress, TxnLimits, WorkerError};

/// One table's snapshot in this session.
#[derive(Debug)]
struct Snap {
    dump: bool,
    doc: TableSnapshotDoc,
    /// Where the next chunk starts; `None` at the table's start.
    cursor: Option<KeysetCursor>,
    reading: bool,
    /// The last chunk has been emitted.
    done: bool,
    /// After a failed read: not before this, and how many failed in a row.
    retry: Option<(Instant, u32)>,
}

/// A finished chunk read.
pub(super) struct Read {
    table: TableRef,
    ticket: Ticket,
    /// The schema the chunk was read under.
    schema: TableSchema,
    result: Result<Chunk, SourceError>,
    read_at_micros: i64,
}

#[derive(Default)]
pub(super) struct Snapshots {
    tables: BTreeMap<TableRef, Snap>,
    /// The chunk reads, each a task of its own: a read shares the source's connection with the
    /// calls the session awaits inline (a PostgreSQL catalog client answers one response at a
    /// time), so it must be polled whatever the session awaits (Task 13 review I3). Dropped with
    /// the session, they are aborted.
    pub(super) reads: JoinSet<Read>,
    ready: VecDeque<Read>,
    reconciler: Reconciler,
    /// What the open stream transaction wrote in the tables being snapshotted.
    touched: Touched,
    /// Transactions in flight when the session started: xid → the source ids of their tables.
    in_flight: BTreeMap<u64, BTreeSet<u64>>,
    /// When to read the in-flight transactions again, and how many reads found some.
    recheck: Option<(Instant, u32)>,
    /// Snapshot documents for the next flush, the latest per table.
    docs: BTreeMap<TableRef, TableSnapshotDoc>,
    /// Completions handed to the writer, with the number of the job that carries them.
    completing: Vec<(u64, TableRef)>,
    /// A read failed transiently: the table may have changed under the snapshot with nothing in
    /// the stream to say so (an idle table), so the session rescans (M5 Task 10 review).
    rescan: bool,
    /// Tables whose schema drifted while the pipeline was down: not read until a rescan has
    /// brought their schema up to date (D-M6a.11).
    held: BTreeSet<TableRef>,
    /// After a failed rescan round while tables are held: the next round not before this.
    pub(super) held_retry: Option<Instant>,
    /// The rounds each held table has waited through so far (I3's warning).
    held_rounds: BTreeMap<TableRef, u32>,
}

impl std::fmt::Debug for Snapshots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshots")
            .field("tables", &self.tables)
            .field("reads", &self.reads.len())
            .field("ready", &self.ready.len())
            .finish_non_exhaustive()
    }
}

impl Snapshots {
    /// The snapshot state of the streamed tables. Returns it with the `dump_*` tables whose
    /// snapshot is done (they aren't streamed) and warnings.
    pub(super) async fn load(
        core: &super::Core,
        schemas: &BTreeMap<TableRef, TableSchema>,
    ) -> Result<(Self, BTreeSet<TableRef>, Vec<String>), WorkerError> {
        let mut snaps = Self::default();
        let mut retired = BTreeSet::new();
        let mut warnings = vec![];
        for (table, schema) in schemas {
            let Some(v) = core.docs.table_snapshot(table).await? else {
                continue;
            };
            let dump = core.settings.is_dump(table);
            match v.doc.status {
                SnapshotStatus::Completed if dump => {
                    retired.insert(table.clone());
                }
                SnapshotStatus::InProgress => {
                    let mut doc = v.doc;
                    let cursor = match &doc.cursor {
                        None => None,
                        Some(c) => match cursor::from_doc(schema, c) {
                            Ok(c) => Some(c),
                            Err(why) => {
                                warnings.push(format!(
                                    "table {table}: the snapshot starts over, its cursor doesn't fit: {why}"
                                ));
                                // Its rows done go with it: the count starts over too.
                                doc.cursor = None;
                                None
                            }
                        },
                    };
                    snaps.tables.insert(
                        table.clone(),
                        Snap {
                            dump,
                            doc,
                            cursor,
                            reading: false,
                            done: false,
                            retry: None,
                        },
                    );
                }
                _ => {}
            }
        }
        Ok((snaps, retired, warnings))
    }

    /// Reads the transactions in flight now (session start).
    pub(super) async fn read_in_flight(
        &mut self,
        source: &dyn SourceConnector,
    ) -> Result<(), SourceError> {
        if self.tables.is_empty() {
            return Ok(());
        }
        if let Some(txns) = source.in_flight().await? {
            self.in_flight = txns
                .into_iter()
                .map(|t| (t.xid, t.tables.into_iter().collect()))
                .collect();
        }
        Ok(())
    }

    pub(super) fn tables(&self) -> impl Iterator<Item = &TableRef> {
        self.tables.keys()
    }

    /// Each running snapshot's progress so far.
    pub(super) fn progress(&self) -> impl Iterator<Item = (TableRef, SnapshotProgress)> + '_ {
        self.tables.iter().map(|(t, s)| {
            let rows_done = s.doc.cursor.as_ref().map_or(0, |c| c.rows_done);
            (t.clone(), SnapshotProgress { rows_done })
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// A stream transaction begins.
    pub(super) fn begin(&mut self) {
        self.touched = Touched::default();
    }

    /// A stream change of the open transaction.
    pub(super) fn touch(&mut self, schema: &TableSchema, e: &CdcEvent) {
        if !self.tables.get(&e.table).is_some_and(|s| !s.done) {
            return;
        }
        for row in e.before.iter().chain(&e.after) {
            if let Some(key) = identity(schema, row) {
                self.touched.key(&e.table, key);
            }
        }
    }

    pub(super) fn truncated(&mut self, tables: &[TableRef]) {
        for t in tables {
            if self.tables.contains_key(t) {
                self.touched.truncated.insert(t.clone());
            }
        }
    }

    /// The stream transaction `xid` was applied.
    pub(super) fn applied(&mut self, xid: u64) {
        self.in_flight.remove(&xid);
        if !self.tables.is_empty() {
            self.reconciler
                .applied(xid, std::mem::take(&mut self.touched));
        }
    }

    /// Holds the reads of those of `tables` being snapshotted.
    pub(super) fn hold(&mut self, tables: &BTreeSet<TableRef>) {
        self.held = tables
            .iter()
            .filter(|t| self.tables.contains_key(*t))
            .cloned()
            .collect();
    }

    /// The tables held for a rescan.
    pub(super) fn held(&self) -> &BTreeSet<TableRef> {
        &self.held
    }

    /// Releases every held table not in `keep`; returns them.
    pub(super) fn release_unless(&mut self, keep: &BTreeSet<&TableRef>) -> Vec<TableRef> {
        let (kept, released): (BTreeSet<TableRef>, BTreeSet<TableRef>) =
            std::mem::take(&mut self.held)
                .into_iter()
                .partition(|t| keep.contains(t));
        self.held = kept;
        for t in &released {
            self.held_rounds.remove(t);
        }
        released.into_iter().collect()
    }

    /// A rescan round ended without a failure: each held table waited through one more. Returns
    /// the tables that reach their second round now, with the count.
    pub(super) fn held_round(&mut self) -> Vec<(TableRef, u32)> {
        let mut second = vec![];
        for t in &self.held {
            let rounds = self.held_rounds.entry(t.clone()).or_default();
            *rounds += 1;
            if *rounds == 2 {
                second.push((t.clone(), *rounds));
            }
        }
        second
    }

    /// The reconciler log's size.
    pub(super) fn log_size(&self) -> ReconcileLog {
        self.reconciler.size()
    }

    /// The table halted: its snapshot stops.
    pub(super) fn stop(&mut self, table: &TableRef) {
        self.held_rounds.remove(table);
        self.held.remove(table);
        self.tables.remove(table);
        self.docs.remove(table);
        if self.tables.is_empty() {
            self.reconciler.clear();
        }
    }

    /// The documents for the next flush, carried by job `job`.
    pub(super) fn take_docs(&mut self, job: u64) -> Vec<TableSnapshotDoc> {
        let docs: Vec<TableSnapshotDoc> = std::mem::take(&mut self.docs).into_values().collect();
        for d in &docs {
            if d.status == SnapshotStatus::Completed {
                self.completing.push((job, d.table.clone()));
            }
        }
        docs
    }

    pub(super) fn has_docs(&self) -> bool {
        !self.docs.is_empty()
    }

    /// The tables whose completion is durable once jobs up to `job` are, each with whether it is
    /// a dump.
    pub(super) fn durable(&mut self, job: u64) -> Vec<(TableRef, bool)> {
        let mut done = vec![];
        self.completing.retain(|(n, table)| {
            if *n <= job {
                done.push(table.clone());
                false
            } else {
                true
            }
        });
        let out = done
            .into_iter()
            .filter_map(|t| self.tables.remove(&t).map(|s| (t, s.dump)))
            .collect();
        if self.tables.is_empty() {
            self.reconciler.clear();
        }
        out
    }
}

fn micros_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

/// The lowest change sequence of the source's dialect: any stream change of a key sorts after it.
fn first_seq(at: &SourcePosition) -> SourceSeq {
    match at {
        SourcePosition::Pg(_) => SourceSeq::Pg(Lsn(0)),
        _ => SourceSeq::Binlog {
            file_index: 0,
            end_log_pos: 0,
        },
    }
}

impl Session<'_> {
    /// Starts the chunk reads that may start: one per table, `parallelism` at a time, none while
    /// the buffer is full, none for a table an in-flight transaction writes. A table's wait for
    /// such writers warns once, naming them (final review I3); it ends once the table may be read.
    pub(super) fn issue_reads(&mut self) {
        if !self.queued.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut reading = self.snaps.tables.values().filter(|s| s.reading).count();
        let mut wait_for_writers = false;
        for (table, snap) in &mut self.snaps.tables {
            if reading >= self.core.settings.parallelism {
                break;
            }
            if snap.reading
                || snap.done
                || snap.retry.is_some_and(|(at, _)| at > now)
                || self.snaps.held.contains(table)
            {
                continue;
            }
            let Some(schema) = self.schemas.get(table).cloned() else {
                continue;
            };
            let writers: Vec<String> = schema.source_id().map_or_else(Vec::new, |id| {
                self.snaps
                    .in_flight
                    .iter()
                    .filter(|(_, tables)| tables.contains(&id))
                    .map(|(xid, _)| xid.to_string())
                    .collect()
            });
            if !writers.is_empty() {
                wait_for_writers = true;
                if self.core.writers_waited.insert(table.clone()) {
                    let warning = format!(
                        "table {table}: its snapshot waits for transactions that were in flight when the stream session started and write it (xids {}): it reads nothing until they end; a long-running or idle-in-transaction session holds one (pg_stat_activity, backend_xid), or a prepared transaction (pg_prepared_xacts)",
                        writers.join(", ")
                    );
                    self.core.update(|st| st.warnings.push(warning));
                }
                continue;
            }
            self.core.writers_waited.remove(table);
            let ticket = self.snaps.reconciler.issue();
            let source = Arc::clone(&self.core.source);
            let after = snap.cursor.clone();
            let limit = self.core.settings.chunk_size;
            let table = table.clone();
            self.snaps.reads.spawn(async move {
                let read_at_micros = micros_now();
                let result = source.read_chunk(&schema, after.as_ref(), limit).await;
                Read {
                    table,
                    ticket,
                    schema,
                    result,
                    read_at_micros,
                }
            });
            snap.reading = true;
            reading += 1;
        }
        if wait_for_writers && self.snaps.recheck.is_none() {
            let delay = self.core.tuning.in_flight_backoff.delay(0);
            self.snaps.recheck = Some((now + delay, 1));
        }
    }

    /// A chunk read finished: it is emitted at the next boundary.
    pub(super) async fn chunk_read(&mut self, read: Read) -> Result<Option<End>, WorkerError> {
        self.snaps.ready.push_back(read);
        if self.acc.in_transaction() {
            return Ok(None);
        }
        self.emit_ready().await
    }

    /// Emits the chunks read so far, then runs the rescan a failed read asked for; called between
    /// transactions. While a rescan's findings are pending, the request waits for them to apply.
    pub(super) async fn emit_ready(&mut self) -> Result<Option<End>, WorkerError> {
        while let Some(read) = self.snaps.ready.pop_front() {
            if let Some(end) = self.emit(read)? {
                return Ok(Some(end));
            }
        }
        if self.rescan.is_none()
            && std::mem::take(&mut self.snaps.rescan)
            && let Some(end) = self.rescan().await?
        {
            return Ok(Some(end));
        }
        self.issue_reads();
        Ok(None)
    }

    fn emit(&mut self, read: Read) -> Result<Option<End>, WorkerError> {
        let Read {
            table,
            ticket,
            schema,
            result,
            read_at_micros,
        } = read;
        let current = self.schemas.get(&table).cloned();
        let Some(snap) = self.snaps.tables.get_mut(&table) else {
            self.snaps.reconciler.cancel(ticket);
            return Ok(None);
        };
        snap.reading = false;
        let chunk = match result {
            Ok(chunk) if current.as_ref() == Some(&schema) => chunk,
            Ok(_) => {
                // The table's schema changed while the chunk was read: read it again.
                self.snaps.reconciler.cancel(ticket);
                return Ok(None);
            }
            Err(SourceError::TableHalt { reason, .. }) => {
                self.snaps.reconciler.cancel(ticket);
                self.halt(table, reason)?;
                return Ok(None);
            }
            Err(e) if e.is_transient() => {
                self.snaps.reconciler.cancel(ticket);
                let failed = snap.retry.map_or(0, |(_, n)| n);
                let delay = self.core.tuning.source_backoff.delay(failed);
                snap.retry = Some((Instant::now() + delay, failed + 1));
                self.snaps.rescan = true;
                // The first failure of the table's streak warns; a read that succeeds ends it.
                let streak = self.core.read_failing.entry(table.clone()).or_default();
                streak.failed(&self.core.status, || {
                    format!(
                        "table {table}: a snapshot chunk read failed ({e}): it is tried again with backoff, and the snapshot reads nothing more until a read succeeds"
                    )
                });
                return Ok(None);
            }
            Err(e) => {
                self.snaps.reconciler.cancel(ticket);
                return Ok(Some(End::Source(e)));
            }
        };
        snap.retry = None;
        self.core.read_failing.remove(&table);
        let mut keyed = Vec::with_capacity(chunk.rows.len());
        for row in chunk.rows.iter().cloned() {
            let Some(key) = identity(&schema, &row) else {
                return Err(protocol(format!("a snapshot row of {table} has no key")));
            };
            keyed.push((key, row));
        }
        let read = chunk.rows.len() as u64;
        let kept =
            self.snaps
                .reconciler
                .reconcile(ticket, &table, chunk.visibility.as_ref(), keyed);
        if !kept.is_empty() {
            // The stream's changes go first: the chunk is committed as a set of its own, which
            // the position guard doesn't skip (Q-M6a-6).
            self.flush()?;
        }
        // The cursor, or the completion, rides the flush that carries the chunk.
        let Some(snap) = self.snaps.tables.get_mut(&table) else {
            return Ok(None);
        };
        let rows_done = snap.doc.cursor.as_ref().map_or(0, |c| c.rows_done) + read;
        snap.cursor = chunk.next.clone();
        match &chunk.next {
            Some(next) => {
                let size = self.core.settings.chunk_size as u64;
                let doc = cursor::to_doc(&schema, next, size, rows_done)
                    .map_err(|why| protocol(format!("table {table}: {why}")))?;
                snap.doc.cursor = Some(doc);
            }
            None => {
                snap.done = true;
                snap.doc.status = SnapshotStatus::Completed;
                snap.doc.cursor = None;
                snap.doc.row_count = Some(rows_done);
                snap.doc.completed_at = Some(DateTime::<Utc>::from(SystemTime::now()));
            }
        }
        self.snaps.docs.insert(table.clone(), snap.doc.clone());
        self.core.update(|st| {
            st.snapshots
                .insert(table.clone(), SnapshotProgress { rows_done });
        });
        if kept.is_empty() {
            self.flush()?;
        } else {
            let at = self.acc.frontier().clone();
            // Chunk rows are bounded by `chunk_size`, not by the transaction limits: a slice
            // would be guarded, and the limit isn't meant for them.
            self.acc.set_txn_limits(TxnLimits {
                batch_rows: None,
                buffer_bytes: u64::MAX,
            });
            self.acc.begin(0)?;
            let mut applied = Ok(());
            for row in kept {
                let e = CdcEvent {
                    op: Op::Snapshot,
                    table: table.clone(),
                    before: None,
                    after: Some(row),
                    source_pos: at.clone(),
                    change_seq: first_seq(&at),
                    xid: 0,
                    commit_ts_micros: read_at_micros,
                };
                applied = self.apply(&e, Instant::now());
                if applied.is_err() {
                    break;
                }
            }
            self.acc.set_txn_limits(self.core.settings.limits);
            applied?;
            let b = TransactionBoundary {
                xid: 0,
                commit_pos: at,
                commit_ts_micros: read_at_micros,
            };
            self.acc.commit(&b, Instant::now())?;
            let f = self.acc.flush();
            let sets = f
                .change_sets
                .into_iter()
                .map(|mut cs| {
                    cs.snapshot = true;
                    cs
                })
                .collect();
            self.submit(sets, f.checkpoint)?;
        }
        if self.core.hooks.at(&HookPoint::ChunkEmitted {
            table: table.clone(),
        }) == HookAction::Crash
        {
            return Err(WorkerError::Crashed(HookPoint::ChunkEmitted { table }));
        }
        Ok(None)
    }

    /// On the tick: the snapshot timeout, and the in-flight transactions read again when due.
    pub(super) async fn snapshot_tick(&mut self) -> Result<Option<End>, WorkerError> {
        // A held table waits for a rescan; a failed round is tried again after a backoff.
        if !self.snaps.held.is_empty()
            && self.rescan.is_none()
            && self.snaps.held_retry.is_none_or(|at| at <= Instant::now())
            && let Some(end) = self.rescan().await?
        {
            return Ok(Some(end));
        }
        let now = Instant::now();
        let limit = self.core.settings.snapshot_timeout;
        // A halted table's snapshot stopped with the halt, and the halt's reason stays.
        let late: Vec<TableRef> = self
            .snaps
            .tables
            .iter()
            .filter(|(t, s)| {
                !s.done
                    && self.schemas.contains_key(*t)
                    && self
                        .core
                        .snapshot_started
                        .get(*t)
                        .is_some_and(|at| now.duration_since(*at) >= limit)
            })
            .map(|(t, _)| t.clone())
            .collect();
        for table in late {
            let mut doc = self.snaps.tables[&table].doc.clone();
            doc.status = SnapshotStatus::Broken;
            self.halt(
                table.clone(),
                HaltReason::SnapshotTimeout {
                    limit_secs: limit.as_secs(),
                },
            )?;
            self.snaps.docs.insert(table, doc);
        }
        self.report_log();
        if let Some((at, n)) = self.snaps.recheck
            && at <= now
        {
            match self.core.source.in_flight().await {
                Ok(Some(txns)) => {
                    let listed: BTreeSet<u64> = txns.iter().map(|t| t.xid).collect();
                    self.snaps.in_flight.retain(|xid, _| listed.contains(xid));
                }
                Ok(None) => self.snaps.in_flight.clear(),
                Err(e) if e.is_transient() => {}
                Err(e) => return Ok(Some(End::Source(e))),
            }
            self.snaps.recheck = None;
            self.issue_reads();
            if self.snaps.recheck.is_some() {
                let delay = self.core.tuning.in_flight_backoff.delay(n);
                self.snaps.recheck = Some((now + delay, n + 1));
            }
        }
        if !self.acc.in_transaction() {
            return self.emit_ready().await;
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use data_avalanche_config::Pipeline;
    use data_avalanche_core::{ColumnSchema, ColumnValue, ColumnValueType as V, Row, SinkError};
    use data_avalanche_state::{CursorKey, SnapshotCursor};
    use data_avalanche_testkit::{FakeSource, FakeTxn, MemorySink, Method, SinkOp};
    use tokio::sync::watch;

    use super::*;
    use crate::session::Core;
    use crate::test_support::{World, Yaml, row, t, wait_until};
    use crate::{Backoff, Command, Exit, Hooks, ReconcileLog, WorkerStatus};

    fn chunks_of(n: u64) -> Pipeline {
        let tables = format!("\n      snapshots: {{ chunk_size: {n} }}");
        Yaml {
            tables: &tables,
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build()
    }

    fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
        ids.map(|id| row(id, "x")).collect()
    }

    /// Runs a session over `core` until `drive` returns, then drains it.
    async fn run<F: Future<Output = ()>>(
        mut core: Core,
        control: tokio::sync::mpsc::Sender<Command>,
        status: watch::Receiver<WorkerStatus>,
        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
    ) -> (End, WorkerStatus) {
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let watching = status.clone();
        let driver = async move {
            drive(watching).await;
            let _ = control.send(Command::Drain).await;
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        let last = status.borrow().clone();
        (end.unwrap(), last)
    }

    async fn snapshot_done(w: &World, status: &watch::Receiver<WorkerStatus>, table: &str) {
        let _ = w;
        wait_until("the snapshot's completion is durable", || {
            let st = status.borrow();
            !st.snapshots.contains_key(&t(table)) && st.checkpoint.is_some()
        })
        .await;
    }

    /// The status warnings that a table's snapshot waits for in-flight writers.
    fn writer_waits(st: &WorkerStatus) -> Vec<String> {
        st.warnings
            .iter()
            .filter(|m| m.contains("in flight when the stream session started"))
            .cloned()
            .collect()
    }

    async fn doc(w: &World, table: &str) -> TableSnapshotDoc {
        w.docs.table_snapshot(&t(table)).await.unwrap().unwrap().doc
    }

    /// The stream inserts a key below the first chunk while it is being read: no chunk reads it.
    #[tokio::test]
    async fn a_snapshot_reads_the_table_in_chunks_while_streaming() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=5));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let (core, control, status) = w.core(&chunks_of(2), &["a"]);
        let (end, _) = run(core, control, status, async |status| {
            wait_until("the first chunk is being read", || {
                w.source.reads(&t("a")) == 1
            })
            .await;
            w.source.commit(FakeTxn::new().insert(&t("a"), row(0, "s")));
            w.source.release_reads();
            wait_until("every row is in", || w.ids("a") == [0, 1, 2, 3, 4, 5]).await;
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert!(matches!(end, End::Exit(Exit::Drained)));
        let d = doc(&w, "a").await;
        assert_eq!(
            (d.status, d.row_count, d.cursor),
            (SnapshotStatus::Completed, Some(5), None)
        );
        assert!(d.completed_at.is_some());
        assert_eq!(
            w.source.reads(&t("a")),
            4,
            "two, two, one, then the empty chunk"
        );
    }

    #[tokio::test]
    async fn a_stream_change_the_chunk_didnt_see_wins() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            wait_until("the chunk is being read", || w.source.reads(&t("a")) == 1).await;
            w.source
                .commit(FakeTxn::new().update(&t("a"), row(2, "new")));
            let end = w.source.wal_end();
            wait_until("the update is read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            w.source.release_reads();
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        let docs = w.text("a", "doc");
        assert_eq!(
            docs,
            [Some("x"), Some("new"), Some("x")].map(|d| d.map(String::from))
        );
    }

    /// DA-SNAP-9: a synchronous commit is streamed, and applied, before a chunk read issued after
    /// it can see it.
    #[tokio::test]
    async fn a_stalled_commit_applied_before_the_read_keeps_its_change() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let (core, control, status) = w.core(&chunks_of(1), &["a"]);
        run(core, control, status, async |status| {
            wait_until("the first chunk is being read", || {
                w.source.reads(&t("a")) == 1
            })
            .await;
            let xid = w
                .source
                .commit_stalled(FakeTxn::new().update(&t("a"), row(2, "new")));
            let end = w.source.wal_end();
            wait_until("the update is read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            w.source.release_reads();
            wait_until("the snapshot passed key 2", || w.source.reads(&t("a")) >= 3).await;
            w.source.release(xid);
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        let docs = w.text("a", "doc");
        assert_eq!(
            docs,
            [Some("x"), Some("new"), Some("x")].map(|d| d.map(String::from))
        );
    }

    /// Q-M6a-2: a commit the last session applied, still invisible when this one starts.
    #[tokio::test]
    async fn a_table_waits_for_the_writers_in_flight_at_the_start() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        let xid = w
            .source
            .commit_stalled(FakeTxn::new().update(&t("a"), row(1, "new")));
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            let end = w.source.wal_end();
            wait_until("the stalled commit is durable", || {
                status.borrow().checkpoint.as_ref() == Some(&end)
            })
            .await;
        })
        .await;
        assert_eq!(w.text("a", "doc"), [Some("new".to_owned())]);
        w.request(&["a"]).await;
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            wait_until("the table waits for the writer", || {
                !writer_waits(&status.borrow()).is_empty()
            })
            .await;
            assert_eq!(w.source.reads(&t("a")), 0, "the writer is still in flight");
            w.source.release(xid);
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(
            w.text("a", "doc"),
            [Some("new"), Some("x")].map(|d| d.map(String::from))
        );
    }

    #[tokio::test]
    async fn a_writer_the_session_applies_frees_the_table() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        let xid = w.source.open_writer(&[t("a")]);
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            wait_until("the table waits for the writer", || {
                !writer_waits(&status.borrow()).is_empty()
            })
            .await;
            assert_eq!(w.source.reads(&t("a")), 0);
            w.source
                .commit_writer(xid, FakeTxn::new().update(&t("a"), row(2, "new")));
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(
            w.text("a", "doc"),
            [Some("x"), Some("new")].map(|d| d.map(String::from))
        );
    }

    #[tokio::test]
    async fn a_dump_ends_the_worker_once_its_snapshot_is_durable() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.request(&["a"]).await;
        let p = Yaml {
            tables: "\n      snapshots: { mode: dump_always, chunk_size: 2 }",
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build();
        let (mut core, _control, _status) = w.core(&p, &["a"]);
        core.dump_only = true;
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let end = s.run().await.unwrap();
        assert!(matches!(end, End::Exit(Exit::Dumped)));
        assert_eq!(w.ids("a"), [1, 2, 3]);
        assert_eq!(doc(&w, "a").await.status, SnapshotStatus::Completed);
    }

    #[tokio::test]
    async fn a_snapshot_resumes_after_its_cursor() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=6));
        let mut d = TableSnapshotDoc {
            table: t("a"),
            status: SnapshotStatus::InProgress,
            start_position: None,
            cursor: Some(SnapshotCursor {
                key: vec![CursorKey {
                    col: "id".into(),
                    val: 2.into(),
                    ty: "bigint".into(),
                }],
                chunk_size: 2,
                rows_done: 2,
                collation: None,
            }),
            row_count: None,
            completed_at: None,
        };
        w.docs.save_table_snapshot(&d).await.unwrap();
        let (core, control, status) = w.core(&chunks_of(2), &["a"]);
        run(core, control, status, async |status| {
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(
            w.ids("a"),
            [3, 4, 5, 6],
            "rows 1 and 2 were read by an earlier run"
        );
        assert_eq!(w.source.reads(&t("a")), 3);
        d = doc(&w, "a").await;
        assert_eq!(d.row_count, Some(6));
    }

    #[tokio::test]
    async fn a_snapshot_past_its_timeout_halts_the_table() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let (mut core, control, status) = w.core(&chunks_of(10), &["a"]);
        core.settings.snapshot_timeout = Duration::from_millis(20);
        let (_, st) = run(core, control, status, async |status| {
            wait_until("the table halts", || !status.borrow().halted.is_empty()).await;
        })
        .await;
        w.source.release_reads();
        assert_eq!(
            st.halted.get(&t("a")),
            Some(&HaltReason::SnapshotTimeout { limit_secs: 0 })
        );
        assert!(st.snapshots.is_empty(), "{st:?}");
        assert_eq!(doc(&w, "a").await.status, SnapshotStatus::Broken);
        assert_eq!(w.docs.halts().await.unwrap().tables[0].table, t("a"));
        assert!(w.ids("a").is_empty());
    }

    #[tokio::test]
    async fn a_chunk_read_under_the_old_schema_is_read_again() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let noted = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        let reads = AtomicUsize::new(0);
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            wait_until("the chunk is being read", || w.source.reads(&t("a")) == 1).await;
            w.source.alter(&t("a"), noted);
            let mut r = row(9, "s");
            r.0.push(("note".into(), ColumnValue::Text("n".into())));
            w.source.commit(FakeTxn::new().insert(&t("a"), r));
            let end = w.source.wal_end();
            wait_until("the change is read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            w.source.release_reads();
            snapshot_done(&w, &status, "a").await;
            reads.store(w.source.reads(&t("a")), Ordering::SeqCst);
        })
        .await;
        assert_eq!(w.ids("a"), [1, 2, 9]);
        assert_eq!(
            w.text("a", "note"),
            [None, None, Some("n".to_owned())],
            "the rows read again under the new schema"
        );
        assert!(
            reads.load(Ordering::SeqCst) >= 3,
            "the stale chunk, again, then the empty one"
        );
    }

    /// The table changed before its snapshot read it, and no change of it comes through the
    /// stream: the failed read rescans, and the snapshot goes on under the new schema.
    #[tokio::test]
    async fn a_chunk_read_that_meets_a_changed_idle_table_rescans_it() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.alter(
            &t("a"),
            TableSchema::new(
                t("a"),
                vec![
                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("doc", V::Text, "text"),
                    ColumnSchema::new("note", V::Text, "text"),
                ],
                vec!["id".into()],
            )
            .unwrap(),
        );
        let (mut core, control, status) = w.core(&chunks_of(10), &["a"]);
        core.settings.rescan_interval = None;
        run(core, control, status, async |status| {
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(w.ids("a"), [1, 2]);
        assert_eq!(
            w.text("a", "note"),
            [None, None],
            "read under the new schema"
        );
    }

    /// Chunk reads share the source's connection with the heartbeat's `retained_bytes`, which the
    /// session awaits inline: a read hands back its rows only while that call waits, so the reads
    /// must be polled while the session awaits it (Task 13 review I3).
    #[tokio::test]
    async fn a_chunk_read_goes_on_while_the_session_awaits_the_source() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=5));
        w.request(&["a"]).await;
        w.source.share_connection();
        let (mut core, control, status) = w.core(&chunks_of(2), &["a"]);
        core.settings.heartbeat = Duration::from_millis(10);
        let (end, _) = run(core, control, status, async |status| {
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert!(matches!(end, End::Exit(Exit::Drained)));
        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5]);
        assert_eq!(w.source.reads(&t("a")), 4);
    }

    /// Holds the sink's commits again once one change set is committed.
    #[derive(Debug)]
    struct HoldAgain {
        sink: MemorySink,
        armed: AtomicBool,
    }

    impl Hooks for HoldAgain {
        fn at(&self, point: &HookPoint) -> HookAction {
            if matches!(point, HookPoint::SinkCommitted { .. })
                && self.armed.swap(false, Ordering::SeqCst)
            {
                self.sink.hold_commits();
            }
            HookAction::Continue
        }
    }

    /// Every chunk is emitted at one frontier while the sink holds the commits, so every job has
    /// the same checkpoint. Once the first is durable, the others still wait in the writer: the
    /// snapshot isn't done until the last one, which carries the completion, is durable.
    #[tokio::test]
    async fn a_snapshot_is_done_only_once_its_last_job_is_durable() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=6));
        w.request(&["a"]).await;
        w.sink.hold_commits();
        let (mut core, control, status) = w.core(&chunks_of(2), &["a"]);
        core.hooks = Arc::new(HoldAgain {
            sink: w.sink.clone(),
            armed: true.into(),
        });
        run(core, control, status, async |status| {
            wait_until("every chunk is emitted", || w.source.reads(&t("a")) == 4).await;
            w.sink.release_commits();
            wait_until("the first job is durable", || {
                !w.source.acknowledged().is_empty()
            })
            .await;
            assert_eq!(w.ids("a"), [1, 2], "the next jobs wait in the writer");
            assert!(
                status.borrow().snapshots.contains_key(&t("a")),
                "the completion isn't durable: {:?}",
                status.borrow()
            );
            w.sink.release_commits();
            snapshot_done(&w, &status, "a").await;
            assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6]);
        })
        .await;
        assert_eq!(doc(&w, "a").await.status, SnapshotStatus::Completed);
    }

    /// A Safe change arrives while chunk jobs wait in the writer, at the checkpoint the session
    /// started from: the sink evolves only once they are durable.
    #[tokio::test]
    async fn a_safe_change_evolves_only_once_the_chunk_jobs_are_durable() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=4));
        w.request(&["a"]).await;
        w.sink.hold_commits();
        let noted = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        let evolved = || {
            w.sink
                .content(&t("a"))
                .is_some_and(|c| c.column_by_name("note").is_some())
        };
        let (core, control, status) = w.core(&chunks_of(2), &["a"]);
        run(core, control, status, async |status| {
            wait_until("every chunk is emitted", || w.source.reads(&t("a")) == 3).await;
            w.source.alter(&t("a"), noted);
            let mut r = row(9, "s");
            r.0.push(("note".into(), ColumnValue::Text("n".into())));
            w.source.commit(FakeTxn::new().insert(&t("a"), r));
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!evolved(), "the chunk jobs aren't durable");
            w.sink.release_commits();
            wait_until("every row is in", || w.ids("a") == [1, 2, 3, 4, 9]).await;
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert!(evolved());
        assert_eq!(
            w.text("a", "note"),
            [None, None, None, None, Some("n".to_owned())]
        );
    }

    /// `public.a` with `note text`.
    fn with_note() -> TableSchema {
        TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    /// [`with_note`] and `extra text`.
    fn with_extra() -> TableSchema {
        let mut columns = with_note().columns().to_vec();
        columns.push(ColumnSchema::new("extra", V::Text, "text"));
        TableSchema::new(t("a"), columns, vec!["id".into()]).unwrap()
    }

    fn noted(id: i64) -> Row {
        let mut r = row(id, "s");
        r.0.push(("note".into(), ColumnValue::Text("n".into())));
        r
    }

    /// D-M6a.11, the unnamed release: `a` drifted, but its cache is the catalog's, so the rescan
    /// names nothing. The first round fails and is tried again; the second releases `a` at the
    /// rescan, and the snapshot reads it as it would any table.
    #[tokio::test]
    async fn a_held_table_the_rescan_doesnt_name_is_released_at_the_rescan() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.fail(
            Method::Rescan,
            SourceError::Transient("catalog busy".into()),
        );
        let (mut core, control, status) = w.core(&chunks_of(10), &["a"]);
        core.drifted.insert(t("a"));
        run(core, control, status, async |status| {
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(
            w.source.rescans(),
            2,
            "the failed round, then the one that released `a`"
        );
        assert_eq!(w.source.reads(&t("a")), 2, "the chunk, then the empty one");
        assert_eq!(w.ids("a"), [1, 2]);
    }

    /// D-M6a.11, the named release: `a` gained `note` while the pipeline was down. The source's
    /// position is ahead of the stream, so the rescan's finding waits for it, and `a` isn't read
    /// meanwhile: its cache still lacks `note`. Once the stream catches up the finding applies,
    /// and only then is `a` released.
    #[tokio::test]
    async fn a_held_table_is_released_only_once_its_finding_applied() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.alter(&t("a"), with_note());
        w.source.hold_stream();
        w.source.keepalive();
        let (mut core, control, status) = w.core(&chunks_of(10), &["a"]);
        core.drifted.insert(t("a"));
        run(core, control, status, async |status| {
            wait_until("the rescan found the change", || w.source.rescans() == 1).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(
                w.source.reads(&t("a")),
                0,
                "the finding waits for the stream"
            );
            w.source.release_stream();
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(
            w.source.reads(&t("a")),
            2,
            "under the new schema: the chunk, then the empty one"
        );
        assert_eq!(w.text("a", "note"), [None, None]);
    }

    /// Task 14's rescan waits for a pending finding (Task 14 review M1): `b`'s read fails while
    /// `a`'s finding waits for the stream. The rescan the failure asks for runs right after the
    /// finding applies, not after `b`'s next read fails (its backoff outlasts the test).
    #[tokio::test]
    async fn a_failed_read_while_a_finding_waits_rescans_once_it_applied() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.source.load(&t("b"), rows(1..=2));
        w.request(&["a", "b"]).await;
        w.source.alter(&t("a"), with_note());
        w.source.hold_stream();
        w.source.keepalive();
        w.source.hold_reads();
        let (mut core, control, status) = w.core(&chunks_of(10), &["a", "b"]);
        core.drifted.insert(t("a"));
        core.tuning.source_backoff = Backoff {
            first: Duration::from_secs(20),
            max: Duration::from_secs(20),
        };
        run(core, control, status, async |status| {
            wait_until("`b`'s first chunk is being read", || {
                w.source.reads(&t("b")) == 1
            })
            .await;
            wait_until("the rescan found `a`'s change", || w.source.rescans() == 1).await;
            w.source.fail(
                Method::ReadChunk,
                SourceError::Transient("connection reset".into()),
            );
            w.source.release_reads();
            wait_until("`b`'s first chunk is emitted", || {
                status
                    .borrow()
                    .snapshots
                    .get(&t("b"))
                    .is_some_and(|p| p.rows_done == 2)
            })
            .await;
            // `b`'s next read fails at once (a failed read isn't counted).
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(w.source.rescans(), 1, "the finding is pending");
            w.source.release_stream();
            // The round, the catch-up's re-read of `a`, then the rescan `b`'s failure asked for.
            wait_until("the failed read's rescan ran", || w.source.rescans() == 3).await;
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(w.source.reads(&t("b")), 1, "`b`'s retry is still to come");
    }

    /// A held table the catch-up skips because the stream moved its cache since the rescan stays
    /// held. While the pipeline was down, `a` gained `note`, a row was written with it, and `a`
    /// gained `extra`. The rescan finds `extra` and `note` against the old cache; the stream then
    /// applies `note` alone. The finding is older than the cache, and the cache is still behind
    /// the catalog: read now, the snapshot would miss `extra` (PostgreSQL's read checks only the
    /// cached columns). The next round applies `extra` before the snapshot reads.
    #[tokio::test]
    async fn a_held_table_whose_cache_moved_since_the_rescan_stays_held() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.alter(&t("a"), with_note());
        w.source.commit(FakeTxn::new().insert(&t("a"), noted(9)));
        w.source.alter(&t("a"), with_extra());
        w.source.hold_stream();
        let (mut core, control, status) = w.core(&chunks_of(10), &["a"]);
        core.drifted.insert(t("a"));
        run(core, control, status, async |status| {
            wait_until("the rescan found the change", || w.source.rescans() == 1).await;
            w.source.release_stream();
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(
            w.source.reads(&t("a")),
            2,
            "read under the catalog's schema only: the chunk, then the empty one"
        );
        assert_eq!(w.ids("a"), [1, 2, 9]);
        assert_eq!(w.text("a", "extra"), [None, None, None]);
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("a")));
    }

    /// A held table the rescan doesn't name because the stream already brought its cache up to
    /// date: the rescan hands that cache to the writer before it releases the table, whatever the
    /// flush policy (M-2). A session that ends right after, before any other flush, leaves the
    /// new schema cached, so the next session needn't hold the table.
    #[tokio::test]
    async fn a_held_tables_new_cache_is_handed_on_at_its_release() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.alter(&t("a"), with_note());
        w.source.commit(FakeTxn::new().insert(&t("a"), noted(9)));
        w.source.hold_reads();
        let p = Yaml {
            tables: "\n      snapshots: { chunk_size: 10 }",
            sink: "flush: { seconds: 3600 }",
            ..Yaml::default()
        }
        .build();
        let (mut core, _control, _status) = w.core(&p, &["a"]);
        core.drifted.insert(t("a"));
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let driver = async {
            wait_until("`a` is released and read", || w.source.reads(&t("a")) == 1).await;
            w.source.fail(
                Method::Next,
                SourceError::Transient("connection reset".into()),
            );
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        w.source.release_reads();
        assert!(matches!(end.unwrap(), End::Source(_)));
        assert_eq!(w.source.rescans(), 1, "the first round released `a`");
        assert!(core.drifted.is_empty());
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("a")), "the new schema is cached");
        assert_eq!(
            w.ids("a"),
            [9],
            "the stream's row, flushed with it; no chunk"
        );
    }

    /// The same with `flush.seconds: null` and a quiet table: no flush policy hands the stream's
    /// change on, and the table is still released at the first round and snapshotted.
    #[tokio::test]
    async fn a_quiet_held_table_is_snapshotted_with_no_flush_interval() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.alter(&t("a"), with_note());
        w.source.commit(FakeTxn::new().insert(&t("a"), noted(9)));
        let p = Yaml {
            tables: "\n      snapshots: { chunk_size: 10 }",
            sink: "flush: { seconds: null, rows: 100000 }",
            ..Yaml::default()
        }
        .build();
        let (mut core, control, status) = w.core(&p, &["a"]);
        core.drifted.insert(t("a"));
        core.settings.snapshot_timeout = Duration::from_secs(3);
        let (_, st) = run(core, control, status, async |status| {
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert!(st.halted.is_empty(), "{st:?}");
        assert_eq!(w.source.rescans(), 1, "the first round released `a`");
        assert_eq!(w.source.reads(&t("a")), 2);
        assert_eq!(w.ids("a"), [1, 2, 9]);
    }

    /// A held table's rescan keeps failing: a failed round is tried again after a backoff, not at
    /// every tick, and the streak warns once that the table's snapshot waits. The round that
    /// succeeds releases it. The session's ticks are driven by hand, and the backoff is checked
    /// at its instant, not by sleeping through part of it (final review M11).
    #[tokio::test]
    async fn a_held_tables_failing_rescan_backs_off_and_warns_once() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        for _ in 0..2 {
            w.source.fail(
                Method::Rescan,
                SourceError::Transient("catalog busy".into()),
            );
        }
        let (mut core, control, status) = w.core(&chunks_of(10), &["a"]);
        core.drifted.insert(t("a"));
        core.tuning.source_backoff = Backoff {
            first: Duration::from_millis(400),
            max: Duration::from_millis(400),
        };
        let waits = |st: &WorkerStatus| -> Vec<String> {
            st.warnings
                .iter()
                .filter(|m| m.contains("wait for a catalog rescan"))
                .cloned()
                .collect()
        };
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let failed = Instant::now();
        assert!(s.snapshot_tick().await.unwrap().is_none());
        assert_eq!(w.source.rescans(), 1, "the first round fails");
        let retry = s.snaps.held_retry.expect("the next round backs off");
        assert!(
            retry >= failed + Duration::from_millis(200),
            "the backoff's floor is 200 ms: {:?}",
            retry.duration_since(failed)
        );
        assert!(s.snapshot_tick().await.unwrap().is_none());
        assert_eq!(
            w.source.rescans(),
            1,
            "a tick before the backoff's instant doesn't rescan"
        );
        for rounds in [2, 3] {
            // The backoff's instant has come.
            s.snaps.held_retry = Some(Instant::now());
            assert!(s.snapshot_tick().await.unwrap().is_none());
            assert_eq!(w.source.rescans(), rounds);
        }
        assert!(s.snaps.held().is_empty(), "the third round released `a`");
        let driver = async {
            snapshot_done(&w, &status, "a").await;
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!(w.source.rescans(), 3, "nothing held, no round");
        let st = status.borrow().clone();
        let warned = waits(&st);
        assert!(
            matches!(&warned[..], [m] if m.contains("public.a") && m.contains("catalog busy")),
            "one warning for the streak: {:?}",
            st.warnings
        );
        assert_eq!(w.source.reads(&t("a")), 2);
    }

    /// A hold that ends with its rounds still failing ends their streak (Task 16 F review M-2):
    /// the held table halts on its snapshot timeout, and a later hold would start a streak of
    /// its own, with its own warning and backoff.
    #[tokio::test]
    async fn a_hold_that_ends_ends_its_failure_streak() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        for _ in 0..200 {
            w.source.fail(
                Method::Rescan,
                SourceError::Transient("catalog busy".into()),
            );
        }
        let (mut core, control, status) = w.core(&chunks_of(10), &["a"]);
        core.drifted.insert(t("a"));
        core.settings.snapshot_timeout = Duration::from_millis(100);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let driver = async {
            wait_until("the held table halts", || {
                !status.borrow().halted.is_empty()
            })
            .await;
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!(
            status.borrow().halted.get(&t("a")),
            Some(&HaltReason::SnapshotTimeout { limit_secs: 0 })
        );
        assert!(w.source.rescans() > 1, "the rounds failed");
        assert_eq!(
            core.held_failing.failures(),
            0,
            "the streak ended with the hold"
        );
    }

    /// Holds the source's chunk reads once a chunk is emitted.
    #[derive(Debug)]
    struct HoldReads(FakeSource);

    impl Hooks for HoldReads {
        fn at(&self, point: &HookPoint) -> HookAction {
            if matches!(point, HookPoint::ChunkEmitted { .. }) {
                self.0.hold_reads();
            }
            HookAction::Continue
        }
    }

    /// The sink refuses a dump table's first chunk while the next is being read: the writer halts
    /// the table, its snapshot stops, and the worker ends `Dumped` with the halt's reason kept.
    #[tokio::test]
    async fn a_writer_halt_stops_the_tables_snapshot() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.request(&["a"]).await;
        w.sink
            .fail(SinkOp::Commit, Some(&t("a")), 1, || SinkError::Permanent {
                reason: "refused".into(),
            });
        let p = Yaml {
            tables: "\n      snapshots: { mode: dump_always, chunk_size: 2 }",
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build();
        let (mut core, _control, status) = w.core(&p, &["a"]);
        core.dump_only = true;
        core.hooks = Arc::new(HoldReads(w.source.clone()));
        core.settings.snapshot_timeout = Duration::from_millis(200);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let end = tokio::time::timeout(Duration::from_secs(5), s.run())
            .await
            .expect("the dump ends")
            .unwrap();
        w.source.release_reads();
        assert!(matches!(end, End::Exit(Exit::Dumped)), "{end:?}");
        assert_eq!(
            w.source.reads(&t("a")),
            2,
            "the second chunk was being read"
        );
        let rejected = |r: &HaltReason| matches!(r, HaltReason::SinkRejected { .. });
        let st = status.borrow().clone();
        assert!(st.halted.get(&t("a")).is_some_and(rejected), "{st:?}");
        assert!(st.snapshots.is_empty(), "{st:?}");
        let halts = w.docs.halts().await.unwrap().tables;
        assert!(
            matches!(&halts[..], [h] if rejected(&h.reason)),
            "{halts:?}"
        );
        assert_eq!(doc(&w, "a").await.status, SnapshotStatus::InProgress);
        assert!(w.ids("a").is_empty());
    }

    /// A stream with a backlog is ready whenever the session polls it (final review I1): a chunk
    /// read that has finished is emitted all the same, and the tick runs: it reads the in-flight
    /// writers again, and `b`, freed by a rollback the stream never carries, is read.
    #[tokio::test]
    async fn a_stream_with_a_backlog_starves_neither_the_reads_nor_the_tick() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.source.load(&t("b"), rows(1..=2));
        w.request(&["a", "b"]).await;
        let xid = w.source.open_writer(&[t("b")]);
        w.source.flood_stream();
        let (core, control, status) = w.core(&chunks_of(10), &["a", "b"]);
        let (end, _) = run(core, control, status, async |status| {
            snapshot_done(&w, &status, "a").await;
            w.source.abort_writer(xid);
            snapshot_done(&w, &status, "b").await;
        })
        .await;
        assert!(matches!(end, End::Exit(Exit::Drained)));
        assert_eq!((w.ids("a"), w.ids("b")), (vec![1, 2], vec![1, 2]));
    }

    /// Arms a transient failure of the stream's next item when the `n`th chunk is emitted.
    #[derive(Debug)]
    struct EndStreamAt {
        source: FakeSource,
        n: usize,
        seen: AtomicUsize,
    }

    impl Hooks for EndStreamAt {
        fn at(&self, point: &HookPoint) -> HookAction {
            if matches!(point, HookPoint::ChunkEmitted { .. })
                && self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.n
            {
                self.source
                    .fail(Method::Next, SourceError::Transient("reset".into()));
            }
            HookAction::Continue
        }
    }

    /// The stream fails right after the completion is handed to the writer, so the completion is
    /// durable only while the session ends: `finish()` ends the snapshot in the status, as the
    /// loop does (final review I2). And a session's status shows only its own snapshots, whatever
    /// an earlier session left there.
    #[tokio::test]
    async fn a_completion_durable_as_the_session_ends_leaves_the_status() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.request(&["a"]).await;
        let (mut core, _control, status) = w.core(&chunks_of(10), &["a"]);
        core.hooks = Arc::new(EndStreamAt {
            source: w.source.clone(),
            n: 2,
            seen: AtomicUsize::new(0),
        });
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let end = tokio::time::timeout(Duration::from_secs(5), s.run())
            .await
            .expect("the stream fails")
            .unwrap();
        assert!(
            matches!(end, End::Source(SourceError::Transient(_))),
            "{end:?}"
        );
        assert_eq!(doc(&w, "a").await.status, SnapshotStatus::Completed);
        assert!(
            status.borrow().snapshots.is_empty(),
            "{:?}",
            status.borrow().snapshots
        );
        core.update(|st| {
            st.snapshots
                .insert(t("gone"), SnapshotProgress { rows_done: 7 });
        });
        let _next = Session::open(&mut core).await.unwrap().unwrap();
        assert!(
            status.borrow().snapshots.is_empty(),
            "{:?}",
            status.borrow().snapshots
        );
    }

    /// A table's wait for the writers in flight at the session start warns once, naming them and
    /// where to look (final review I3), however often its reads are checked meanwhile. The
    /// warning ends with the wait: a later wait warns again.
    #[tokio::test]
    async fn a_wait_for_in_flight_writers_warns_once_per_wait() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.source.load(&t("a"), rows(1..=2));
        let (mut core, control, status) = w.core(&chunks_of(10), &["a", "b"]);
        for wait in 1..=2 {
            w.request(&["a"]).await;
            let xid = w.source.open_writer(&[t("a")]);
            let reads = w.source.reads(&t("a"));
            let s = Session::open(&mut core).await.unwrap().unwrap();
            let driver = async {
                wait_until("the table waits", || {
                    writer_waits(&status.borrow()).len() == wait
                })
                .await;
                // Each boundary checks the reads again.
                for id in 1..=3 {
                    w.source
                        .commit(FakeTxn::new().insert(&t("b"), row(id, "b")));
                }
                let end = w.source.wal_end();
                wait_until("the commits are durable", || {
                    status.borrow().checkpoint.as_ref() == Some(&end)
                })
                .await;
                let waits = writer_waits(&status.borrow());
                assert_eq!(waits.len(), wait, "wait {wait}: {waits:?}");
                let m = &waits[wait - 1];
                assert!(
                    m.contains("public.a")
                        && m.contains(&format!("xids {xid})"))
                        && m.contains("pg_stat_activity")
                        && m.contains("pg_prepared_xacts"),
                    "{m}"
                );
                assert_eq!(w.source.reads(&t("a")), reads, "wait {wait}: not read");
                w.source.abort_writer(xid);
                snapshot_done(&w, &status, "a").await;
                control.send(Command::Drain).await.unwrap();
            };
            let (end, ()) = tokio::join!(s.run(), driver);
            assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        }
        assert_eq!(w.ids("a"), [1, 2]);
    }

    /// The reconciler's log size is reported (final review I3): it grows with the stream's
    /// changes of a table whose chunk is out, and empties once no snapshot runs.
    #[tokio::test]
    async fn the_reconciler_logs_size_is_reported() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            wait_until("the chunk is being read", || w.source.reads(&t("a")) == 1).await;
            w.source.commit(
                FakeTxn::new()
                    .update(&t("a"), row(1, "s"))
                    .update(&t("a"), row(2, "s")),
            );
            w.source.commit(FakeTxn::new().update(&t("a"), row(2, "t")));
            let logged = ReconcileLog {
                entries: 2,
                keys: 3,
            };
            wait_until("the log is reported", || {
                status.borrow().reconcile_log == logged
            })
            .await;
            w.source.release_reads();
            snapshot_done(&w, &status, "a").await;
            wait_until("the empty log is reported", || {
                status.borrow().reconcile_log == ReconcileLog::default()
            })
            .await;
        })
        .await;
        assert_eq!(
            w.text("a", "doc"),
            [Some("s"), Some("t"), Some("x")].map(|d| d.map(String::from))
        );
    }

    /// Hands the next chunk read a failure once a chunk is emitted.
    #[derive(Debug)]
    struct FailNextRead(FakeSource, AtomicBool);

    impl Hooks for FailNextRead {
        fn at(&self, point: &HookPoint) -> HookAction {
            if matches!(point, HookPoint::ChunkEmitted { .. })
                && self.1.swap(false, Ordering::SeqCst)
            {
                self.0.fail(
                    Method::ReadChunk,
                    SourceError::Transient("reset again".into()),
                );
            }
            HookAction::Continue
        }
    }

    /// Failing chunk reads warn once per streak, with the error (final review M2): two failures
    /// in a row warn once; a read that succeeds ends the streak, so the next failure warns again.
    #[tokio::test]
    async fn a_failing_chunk_read_warns_once_per_streak() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=4));
        w.request(&["a"]).await;
        for _ in 0..2 {
            w.source.fail(
                Method::ReadChunk,
                SourceError::Transient("connection reset".into()),
            );
        }
        let (mut core, control, status) = w.core(&chunks_of(2), &["a"]);
        core.hooks = Arc::new(FailNextRead(w.source.clone(), AtomicBool::new(true)));
        let (_, st) = run(core, control, status, async |status| {
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        let failed: Vec<&String> = st
            .warnings
            .iter()
            .filter(|m| m.contains("a snapshot chunk read failed"))
            .collect();
        assert!(
            matches!(&failed[..], [one, two] if one.contains("public.a")
                && one.contains("connection reset") && two.contains("reset again")),
            "{:?}",
            st.warnings
        );
        assert_eq!(w.ids("a"), [1, 2, 3, 4]);
        assert!(st.halted.is_empty(), "{st:?}");
    }

    /// A saved cursor that no longer fits the table's identifier: the snapshot starts over, and
    /// so does its row count (final review M4).
    #[tokio::test]
    async fn a_cursor_that_no_longer_fits_starts_the_count_over() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: t("a"),
                status: SnapshotStatus::InProgress,
                start_position: None,
                cursor: Some(SnapshotCursor {
                    key: vec![CursorKey {
                        col: "old_id".into(),
                        val: 2.into(),
                        ty: "bigint".into(),
                    }],
                    chunk_size: 2,
                    rows_done: 2,
                    collation: None,
                }),
                row_count: None,
                completed_at: None,
            })
            .await
            .unwrap();
        let (core, control, status) = w.core(&chunks_of(2), &["a"]);
        let (_, st) = run(core, control, status, async |status| {
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert!(
            st.warnings
                .iter()
                .any(|m| m.contains("its cursor doesn't fit")),
            "{:?}",
            st.warnings
        );
        let d = doc(&w, "a").await;
        assert_eq!(
            (d.status, d.row_count),
            (SnapshotStatus::Completed, Some(3))
        );
        assert_eq!(w.ids("a"), [1, 2, 3]);
    }
}
