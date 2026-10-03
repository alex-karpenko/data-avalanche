//! One stream session (Data-Flow Phases 2–5). It opens the stream at the durable checkpoint and
//! reads it into the accumulator. Each flush goes to the sink writer, and the stream is
//! acknowledged once the flush is durable. A session ends on a drain, on a source failure (the
//! worker decides what follows) or with an error.
//!
//! A session starts from durable state only: the checkpoint, the cached schemas and the recorded
//! halts. What it held in memory is lost when it ends, and the next session replays it from the
//! checkpoint.

mod snapshot;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use data_avalanche_core::{
    CdcEvent, ChangeStream, ColumnValue, HaltReason, Op, RescanReport, Row, SchemaHint, Sink,
    SourceConnector, SourceError, SourcePosition, SourceSeq, StreamItem, TableRef, TableSchema,
    TransactionBoundary, identifier_values,
};
use data_avalanche_state::{PipelineDocs, TableHalt};
use futures_util::StreamExt;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinHandle;

use self::snapshot::Snapshots;
use crate::schema_change::{Evolver, Planned};
use crate::settings::Settings;
use crate::writer::{self, Job, WriterCtx, WriterEvent};
use crate::{
    Accumulator, AccumulatorError, Command, CommitGuard, Exit, FlushReason, Hooks, Phase, Tuning,
    WorkerError, WorkerStatus,
};

/// What every session of one worker shares.
pub(crate) struct Core {
    pub settings: Settings,
    pub source: Arc<dyn SourceConnector>,
    pub sink: Arc<dyn Sink>,
    pub docs: PipelineDocs,
    pub owner_epoch: u64,
    pub guard: Arc<dyn CommitGuard>,
    pub hooks: Arc<dyn Hooks>,
    pub tuning: Tuning,
    pub control: mpsc::Receiver<Command>,
    pub status: watch::Sender<WorkerStatus>,
    /// The tables Phase 0 tracks. A session streams each one unless it is halted or a done dump.
    pub tracked: BTreeSet<TableRef>,
    /// Every table of the pipeline is a `dump_*` table: the worker ends once none is left.
    pub dump_only: bool,
    /// When each table's snapshot began in this run (`timeouts.snapshot`).
    pub snapshot_started: BTreeMap<TableRef, Instant>,
    /// Tables whose catalog schema moved past the cached one while the pipeline was down (Phase
    /// 0 on a resume). A snapshot of one reads only once a rescan has brought its schema up to
    /// date (D-M6a.11).
    pub drifted: BTreeSet<TableRef>,
}

impl Core {
    pub fn update(&self, f: impl FnOnce(&mut WorkerStatus)) {
        self.status.send_modify(f);
    }

    pub fn evolver(&self) -> Evolver<'_> {
        Evolver {
            source: &*self.source,
            sink: &*self.sink,
            guard: &*self.guard,
            tuning: &self.tuning,
            evolution: self.settings.evolution,
            status: &self.status,
        }
    }
}

/// How a session ended without an error.
#[derive(Debug)]
pub(crate) enum End {
    Exit(Exit),
    /// The source failed: a transient failure reopens the stream, a lost position recovers it.
    Source(SourceError),
}

fn protocol(detail: String) -> WorkerError {
    AccumulatorError::Protocol { detail }.into()
}

pub(crate) struct Session<'c> {
    core: &'c mut Core,
    acc: Accumulator,
    /// The streamed tables' schemas as of the frontier.
    schemas: BTreeMap<TableRef, TableSchema>,
    stream: Box<dyn ChangeStream>,
    jobs: UnboundedSender<Job>,
    events: UnboundedReceiver<WriterEvent>,
    writer: JoinHandle<()>,
    /// `buffer_capacity_rows`: rows handed to the writer and not yet durable.
    permits: Arc<Semaphore>,
    /// Flushes waiting for room in the buffer, in order. The stream isn't read meanwhile.
    queued: VecDeque<(Job, u32)>,
    /// The checkpoint of the last flush handed on.
    submitted: SourcePosition,
    durable: SourcePosition,
    /// Halts since the last flush, recorded with the next one.
    halts: Vec<TableHalt>,
    /// The open transaction's schema changes.
    txn_schemas: Vec<TableSchema>,
    /// Schema changes of committed transactions, each cached once a checkpoint covers it.
    pending_schemas: Vec<(SourcePosition, TableSchema)>,
    /// `toast_strategy: re_query`: the keys whose UPDATE in the open transaction carried
    /// unchanged values, and the transaction's last change.
    requery: BTreeMap<TableRef, Vec<Row>>,
    last_seq: Option<SourceSeq>,
    /// A rescan's findings, applied once the stream passes the position read after it.
    rescan: Option<(SourcePosition, RescanReport)>,
    snaps: Snapshots,
    draining: bool,
}

impl<'c> Session<'c> {
    /// Loads the durable state and opens the stream at the checkpoint; `Ok(Err)` when the source
    /// fails to open it.
    pub async fn open(core: &'c mut Core) -> Result<Result<Self, SourceError>, WorkerError> {
        let Some(slot) = core.settings.slot.clone() else {
            return Err(protocol("a stream session needs a PostgreSQL slot".into()));
        };
        let checkpoint = match core.docs.slot().await? {
            Some(v) if !v.doc.is_intent() => SourcePosition::Pg(v.doc.confirmed_flush_lsn),
            _ => return Err(protocol("a stream session needs a checkpoint".into())),
        };
        let cached: BTreeMap<TableRef, TableSchema> = core
            .docs
            .table_schemas()
            .await?
            .into_iter()
            .map(|s| (s.table().clone(), s))
            .collect();
        let recorded = core.docs.halts().await?.tables;
        let s = &core.settings;
        let mut acc = Accumulator::new(s.lineage.clone(), s.flush, checkpoint.clone());
        acc.set_txn_limits(s.limits);
        let mut halts = vec![];
        let mut schemas = BTreeMap::new();
        for table in &core.tracked {
            if recorded.iter().any(|h| &h.table == table) {
                acc.halt_table(table);
                continue;
            }
            let Some(schema) = cached.get(table) else {
                return Err(protocol(format!(
                    "tracked table {table} has no cached schema"
                )));
            };
            match acc.add_table(schema.clone(), &s.delete_extra(schema)) {
                Ok(()) => {
                    schemas.insert(table.clone(), schema.clone());
                }
                Err(AccumulatorError::Halt { table, reason }) => halts.push(TableHalt {
                    table,
                    reason,
                    at: checkpoint.clone(),
                }),
                Err(e) => return Err(e.into()),
            }
        }
        let (mut snaps, retired, warnings) = Snapshots::load(core, &schemas).await?;
        snaps.hold(&core.drifted);
        for table in &retired {
            acc.halt_table(table);
            schemas.remove(table);
        }
        if let Err(e) = snaps.read_in_flight(&*core.source).await {
            return Ok(Err(e));
        }
        let now = Instant::now();
        for table in snaps.tables() {
            core.snapshot_started.entry(table.clone()).or_insert(now);
        }
        let live: Vec<TableSchema> = schemas.values().cloned().collect();
        let stream = match core.source.open_stream(&checkpoint, &live).await {
            Ok(stream) => stream,
            Err(e) => return Ok(Err(e)),
        };
        let (jobs, jobs_rx) = mpsc::unbounded_channel();
        let (events_tx, events) = mpsc::unbounded_channel();
        let ctx = WriterCtx {
            sink: core.sink.clone(),
            docs: core.docs.clone(),
            slot,
            pipeline_id: core.settings.pipeline_id.clone(),
            owner_epoch: core.owner_epoch,
            guard: core.guard.clone(),
            hooks: core.hooks.clone(),
            backoff: core.tuning.sink_backoff,
        };
        let writer = tokio::spawn(writer::run(ctx, jobs_rx, events_tx));
        let rows = usize::try_from(core.settings.buffer_rows)
            .unwrap_or(usize::MAX)
            .min(Semaphore::MAX_PERMITS);
        core.update(|st| {
            st.phase = Phase::Streaming;
            st.checkpoint = Some(checkpoint.clone());
            for h in recorded.iter().chain(&halts) {
                st.halted.insert(h.table.clone(), h.reason.clone());
            }
            st.warnings.extend(warnings);
            st.snapshots.extend(snaps.progress());
        });
        Ok(Ok(Self {
            core,
            acc,
            schemas,
            stream,
            jobs,
            events,
            writer,
            permits: Arc::new(Semaphore::new(rows)),
            queued: VecDeque::new(),
            submitted: checkpoint.clone(),
            durable: checkpoint,
            halts,
            txn_schemas: vec![],
            pending_schemas: vec![],
            requery: BTreeMap::new(),
            last_seq: None,
            rescan: None,
            snaps,
            draining: false,
        }))
    }

    /// Runs the session to its end. Flushes already handed to the writer are made durable before
    /// it returns, unless it fails.
    pub async fn run(mut self) -> Result<End, WorkerError> {
        match self.event_loop().await {
            Ok(end) => {
                let end = self.finish(end).await?;
                Ok(end)
            }
            Err(e) => {
                self.writer.abort();
                Err(e)
            }
        }
    }

    async fn finish(mut self, end: End) -> Result<End, WorkerError> {
        while let Some((mut job, _)) = self.queued.pop_front() {
            job.permit = None;
            let _ = self.jobs.send(job);
        }
        drop(self.jobs);
        while let Some(ev) = self.events.recv().await {
            match ev {
                WriterEvent::Failed(e) => return Err(e),
                WriterEvent::Halted(h) => self.core.update(|st| {
                    st.halted.insert(h.table, h.reason);
                }),
                WriterEvent::Degraded(why) => self.core.update(|st| st.degraded = why),
                WriterEvent::Durable(cp) => self.core.update(|st| st.checkpoint = Some(cp)),
            }
        }
        let _ = self.stream.close().await;
        Ok(end)
    }

    async fn event_loop(&mut self) -> Result<End, WorkerError> {
        let mut tick = tokio::time::interval(self.core.tuning.tick);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let every = self.core.settings.heartbeat;
        let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut rescan = self
            .core
            .settings
            .rescan_interval
            .map(|every| tokio::time::interval_at(tokio::time::Instant::now() + every, every));
        self.issue_reads();
        loop {
            if self.dumped() {
                self.flush()?;
                if let Some(end) = self.until_durable().await? {
                    return Ok(end);
                }
                return Ok(End::Exit(Exit::Dumped));
            }
            if self.draining && !self.acc.in_transaction() {
                self.flush()?;
                if let Some(end) = self.until_durable().await? {
                    return Ok(end);
                }
                return Ok(End::Exit(Exit::Drained));
            }
            let reading = self.queued.is_empty();
            let need = self.queued.front().map_or(0, |(_, n)| *n);
            let permits = self.permits.clone();
            tokio::select! {
                biased;
                // Not once draining: a closed channel is ready at every poll, and would starve
                // the stream that must finish the open transaction.
                cmd = self.core.control.recv(), if !self.draining => self.command(cmd)?,
                ev = self.events.recv() => {
                    if let Some(end) = self.writer_event(ev).await? {
                        return Ok(end);
                    }
                }
                p = permits.acquire_many_owned(need), if !reading => {
                    self.send_front(p.expect("the semaphore isn't closed"));
                }
                item = self.stream.next(), if reading => match item {
                    Ok(item) => {
                        if let Some(end) = self.item(item).await? {
                            return Ok(end);
                        }
                    }
                    Err(e) => return Ok(End::Source(e)),
                },
                read = self.snaps.reads.next(), if !self.snaps.reads.is_empty() => {
                    let read = read.expect("not empty");
                    if let Some(end) = self.chunk_read(read).await? {
                        return Ok(end);
                    }
                }
                _ = tick.tick() => {
                    self.due(Instant::now())?;
                    if let Some(end) = self.snapshot_tick().await? {
                        return Ok(end);
                    }
                }
                _ = heartbeat.tick() => {
                    if let Some(end) = self.wal_lag().await? {
                        return Ok(end);
                    }
                }
                _ = async { rescan.as_mut().expect("guarded").tick().await },
                    if rescan.is_some() && self.rescan.is_none() =>
                {
                    if let Some(end) = self.rescan().await? {
                        return Ok(end);
                    }
                }
            }
        }
    }

    fn command(&mut self, cmd: Option<Command>) -> Result<(), WorkerError> {
        match cmd {
            Some(Command::Flush) => self.flush(),
            Some(Command::Drain) | None => {
                self.draining = true;
                self.core.update(|st| st.phase = Phase::Draining);
                Ok(())
            }
        }
    }

    async fn writer_event(&mut self, ev: Option<WriterEvent>) -> Result<Option<End>, WorkerError> {
        match ev {
            None => Err(protocol("the sink writer ended".into())),
            Some(WriterEvent::Failed(e)) => Err(e),
            Some(WriterEvent::Halted(h)) => {
                self.acc.halt_table(&h.table);
                self.schemas.remove(&h.table);
                self.core.update(|st| {
                    st.halted.insert(h.table, h.reason);
                });
                Ok(None)
            }
            Some(WriterEvent::Degraded(why)) => {
                self.core.update(|st| st.degraded = why);
                Ok(None)
            }
            Some(WriterEvent::Durable(cp)) => {
                self.durable = cp.clone();
                for (table, dump) in self.snaps.durable(&cp) {
                    self.core.update(|st| {
                        st.snapshots.remove(&table);
                    });
                    if dump {
                        // A dump stops once its snapshot is durable.
                        self.acc.halt_table(&table);
                        self.schemas.remove(&table);
                    }
                }
                if let Err(e) = self.stream.acknowledge(&cp).await {
                    return Ok(Some(End::Source(e)));
                }
                self.core.update(|st| st.checkpoint = Some(cp));
                Ok(None)
            }
        }
    }

    async fn item(&mut self, item: StreamItem) -> Result<Option<End>, WorkerError> {
        let now = Instant::now();
        match item {
            StreamItem::Begin { xid, .. } => {
                self.acc.begin(xid)?;
                self.snaps.begin();
                self.requery.clear();
                self.last_seq = None;
            }
            StreamItem::Change(e) => {
                if self.core.settings.re_query {
                    self.note_unchanged(&e);
                }
                self.last_seq = Some(e.change_seq);
                if let Some(schema) = self.schemas.get(&e.table) {
                    self.snaps.touch(schema, &e);
                }
                self.apply(&e, now)?;
            }
            StreamItem::Truncate { tables, .. } => loop {
                self.snaps.truncated(&tables);
                match self.acc.truncate(&tables, now) {
                    Err(AccumulatorError::FlushRequired { .. }) => self.flush()?,
                    Err(AccumulatorError::Halt { table, reason }) => self.halt(table, reason),
                    r => break self.absorb(r)?,
                }
            },
            StreamItem::SchemaSignal { table, hint, .. } => {
                return self.schema_signal(table, hint).await;
            }
            StreamItem::TableHalt { table, reason, .. } => self.halt(table, reason),
            StreamItem::Commit(b) => {
                if let Some(end) = self.re_query(&b).await? {
                    return Ok(Some(end));
                }
                self.acc.commit(&b, now)?;
                self.snaps.applied(b.xid);
                let at = b.commit_pos;
                self.pending_schemas
                    .extend(self.txn_schemas.drain(..).map(|s| (at.clone(), s)));
                return self.boundary(now).await;
            }
            StreamItem::KeepAlive(p) => {
                self.acc.keepalive(&p, now)?;
                return self.boundary(now).await;
            }
        }
        Ok(None)
    }

    /// Between transactions: what waited for the stream to get here, then the flush policy.
    async fn boundary(&mut self, now: Instant) -> Result<Option<End>, WorkerError> {
        self.read();
        if let Some(end) = self.caught_up().await? {
            return Ok(Some(end));
        }
        if let Some(end) = self.emit_ready().await? {
            return Ok(Some(end));
        }
        self.due(now)?;
        Ok(None)
    }

    fn apply(&mut self, e: &CdcEvent, now: Instant) -> Result<(), WorkerError> {
        loop {
            match self.acc.apply(e, now) {
                Err(AccumulatorError::SplitRequired) => self.split()?,
                r => break self.absorb(r)?,
            }
        }
        if self.acc.due(now) == Some(FlushReason::Split) {
            self.split()?;
        }
        Ok(())
    }

    /// Notes an UPDATE's key when the source sent some of its values as unchanged.
    fn note_unchanged(&mut self, e: &CdcEvent) {
        let Some(after) = &e.after else { return };
        if e.op != Op::Update || !after.0.iter().any(|(_, v)| *v == ColumnValue::Unchanged) {
            return;
        }
        let Some(key) = self
            .schemas
            .get(&e.table)
            .and_then(|s| identifier_values(s, after))
        else {
            return;
        };
        let keys = self.requery.entry(e.table.clone()).or_default();
        if !keys.contains(&key) {
            keys.push(key);
        }
    }

    /// `toast_strategy: re_query` at the commit: reads the current rows of the noted keys and
    /// applies them as the transaction's last change (Data-Flow §TOAST). A key whose row is gone
    /// is left to the delete that follows in the stream.
    async fn re_query(&mut self, b: &TransactionBoundary) -> Result<Option<End>, WorkerError> {
        let Some(seq) = self.last_seq else {
            return Ok(None);
        };
        for (table, keys) in std::mem::take(&mut self.requery) {
            let Some(schema) = self.schemas.get(&table).cloned() else {
                continue;
            };
            match self.core.source.fetch_rows(&schema, &keys).await {
                Ok(rows) => {
                    for row in rows {
                        let e = CdcEvent {
                            op: Op::Update,
                            table: table.clone(),
                            before: None,
                            after: Some(row),
                            source_pos: b.commit_pos.clone(),
                            change_seq: seq,
                            xid: b.xid,
                            commit_ts_micros: b.commit_ts_micros,
                        };
                        self.apply(&e, Instant::now())?;
                    }
                }
                Err(SourceError::TableHalt { table, reason }) => self.halt(table, reason),
                Err(e) => return Ok(Some(End::Source(e))),
            }
        }
        Ok(None)
    }

    /// Channel D: one catalog read of the streamed tables, and the source's position after it. A
    /// failed round waits for the next.
    async fn rescan(&mut self) -> Result<Option<End>, WorkerError> {
        let known: Vec<TableSchema> = self.schemas.values().cloned().collect();
        let Ok(report) = self.core.source.rescan(&known).await else {
            return Ok(None);
        };
        let named: BTreeSet<&TableRef> = report
            .changed
            .iter()
            .map(TableSchema::table)
            .chain(&report.dropped)
            .chain(report.halted.iter().map(|(t, _)| t))
            .collect();
        for table in self.snaps.release_unless(&named) {
            self.core.drifted.remove(&table);
        }
        if report == RescanReport::default() {
            return Ok(None);
        }
        let Ok(after) = self.core.source.current_position().await else {
            return Ok(None);
        };
        self.rescan = Some((after, report));
        if self.acc.in_transaction() {
            return Ok(None);
        }
        self.caught_up().await
    }

    /// Applies a rescan's findings once the stream has passed the position read after it (Q-M6a-4):
    /// every change the stream holds for those tables is in by then. A change the stream applied
    /// on its own meanwhile is skipped.
    async fn caught_up(&mut self) -> Result<Option<End>, WorkerError> {
        let Some((after, _)) = &self.rescan else {
            return Ok(None);
        };
        if !self
            .acc
            .frontier()
            .covers(after)
            .map_err(AccumulatorError::from)?
        {
            return Ok(None);
        }
        let (_, report) = self.rescan.take().expect("checked above");
        // The tables' changes up to here are good: flush them before the halts drop what is
        // buffered.
        self.flush()?;
        for table in report.dropped {
            if self.schemas.contains_key(&table) {
                self.halt(table, HaltReason::TableDropped);
            }
        }
        for (table, reason) in report.halted {
            if self.schemas.contains_key(&table) {
                self.halt(table, reason);
            }
        }
        for now in report.changed {
            let table = now.table().clone();
            let Some(cached) = self.schemas.get(&table) else {
                continue;
            };
            if cached.columns() == now.columns() && cached.identifier() == now.identifier() {
                continue;
            }
            let hint = SchemaHint::Columns(now.columns().to_vec());
            if let Some(end) = self.schema_signal(table, hint).await? {
                return Ok(Some(end));
            }
        }
        // Caches the applied schemas without waiting for the tables' next change.
        self.flush()?;
        for table in self.snaps.release_unless(&BTreeSet::new()) {
            self.core.drifted.remove(&table);
        }
        Ok(None)
    }

    /// The WAL-retention guard (G-22): the retained log is always reported; past
    /// `max_wal_lag_bytes` the pipeline stops to protect the source's disk. A failed read waits
    /// for the next heartbeat.
    async fn wal_lag(&mut self) -> Result<Option<End>, WorkerError> {
        let retained = match self.core.source.retained_bytes().await {
            Ok(retained) => retained,
            Err(e) if e.is_transient() => return Ok(None),
            Err(e) => return Ok(Some(End::Source(e))),
        };
        self.core.update(|st| st.retained_bytes = retained);
        match (retained, self.core.settings.max_wal_lag_bytes) {
            (Some(lag), Some(limit)) if lag > limit => Err(WorkerError::WalLag { lag, limit }),
            _ => Ok(None),
        }
    }

    /// Every table is a dump, and every one is done or halted.
    fn dumped(&self) -> bool {
        self.core.dump_only && self.schemas.is_empty() && self.snaps.is_empty()
    }

    fn read(&self) {
        let frontier = self.acc.frontier().clone();
        self.core.update(|st| st.read = Some(frontier));
    }

    /// A table halt is the table's alone; anything else ends the worker.
    fn absorb(&mut self, r: Result<(), AccumulatorError>) -> Result<(), WorkerError> {
        match r {
            Ok(()) => Ok(()),
            Err(AccumulatorError::Halt { table, reason }) => {
                self.halt(table, reason);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Stops the table: its buffered changes go, its later ones are dropped, and the halt is
    /// recorded before the next checkpoint. The sink holds its changes through the checkpoint.
    fn halt(&mut self, table: TableRef, reason: HaltReason) {
        self.acc.halt_table(&table);
        self.schemas.remove(&table);
        self.snaps.stop(&table);
        self.halts.push(TableHalt {
            table: table.clone(),
            reason: reason.clone(),
            at: self.acc.checkpoint().clone(),
        });
        self.core.update(|st| {
            st.halted.insert(table, reason);
        });
    }

    fn due(&mut self, now: Instant) -> Result<(), WorkerError> {
        match self.acc.due(now) {
            None => Ok(()),
            Some(FlushReason::Split) => self.split(),
            Some(_) => self.flush(),
        }
    }

    fn flush(&mut self) -> Result<(), WorkerError> {
        let f = self.acc.flush();
        self.submit(f.change_sets, f.checkpoint)
    }

    fn split(&mut self) -> Result<(), WorkerError> {
        let f = self.acc.split()?;
        self.submit(f.change_sets, f.checkpoint)
    }

    /// Hands a flush to the writer, with the halts since the last one and the schema changes its
    /// checkpoint covers; queued while the buffer is full.
    fn submit(
        &mut self,
        change_sets: Vec<data_avalanche_core::TableChangeSet>,
        checkpoint: SourcePosition,
    ) -> Result<(), WorkerError> {
        let mut schemas = vec![];
        let mut rest = vec![];
        for (at, schema) in self.pending_schemas.drain(..) {
            if checkpoint.covers(&at).map_err(AccumulatorError::from)? {
                schemas.push(schema);
            } else {
                rest.push((at, schema));
            }
        }
        self.pending_schemas = rest;
        if change_sets.is_empty()
            && checkpoint == self.submitted
            && self.halts.is_empty()
            && schemas.is_empty()
            && !self.snaps.has_docs()
        {
            return Ok(());
        }
        let rows: usize = change_sets
            .iter()
            .map(|cs| cs.upsert_rows() + cs.delete_rows())
            .sum();
        let cap = self
            .core
            .settings
            .buffer_rows
            .min(Semaphore::MAX_PERMITS as u64);
        let need = u32::try_from((rows as u64).min(cap)).unwrap_or(u32::MAX);
        let job = Job {
            change_sets,
            checkpoint: checkpoint.clone(),
            halts: std::mem::take(&mut self.halts),
            snapshots: self.snaps.take_docs(&checkpoint),
            schemas,
            permit: None,
        };
        self.submitted = checkpoint;
        self.queued.push_back((job, need));
        if self.queued.len() == 1
            && let Ok(p) = self.permits.clone().try_acquire_many_owned(need)
        {
            self.send_front(p);
        }
        Ok(())
    }

    fn send_front(&mut self, permit: OwnedSemaphorePermit) {
        if let Some((mut job, _)) = self.queued.pop_front() {
            job.permit = Some(permit);
            let _ = self.jobs.send(job);
        }
    }

    /// Waits until every flush handed on is durable.
    async fn until_durable(&mut self) -> Result<Option<End>, WorkerError> {
        while self.durable != self.submitted || !self.queued.is_empty() {
            let need = self.queued.front().map_or(0, |(_, n)| *n);
            let permits = self.permits.clone();
            tokio::select! {
                biased;
                ev = self.events.recv() => {
                    if let Some(end) = self.writer_event(ev).await? {
                        return Ok(Some(end));
                    }
                }
                p = permits.acquire_many_owned(need), if !self.queued.is_empty() => {
                    self.send_front(p.expect("the semaphore isn't closed"));
                }
            }
        }
        Ok(None)
    }

    /// `Schema_Change…` §8.1 at the signal's position: the catalog read (channel C) and the
    /// classification come first. An Ignorable change only rebuilds: no flush, no evolve. A Safe
    /// change flushes what is buffered and waits until it is durable, then evolves the sink and
    /// rebuilds. A change that halts the table (Unsafe, any with evolution off, a dropped table, a
    /// failed catalog read) flushes and waits the same way, so the table's committed rows reach
    /// the sink, then halts it at that flush's checkpoint. In an open transaction the change applies where the stream met it: the
    /// committed rows flush, and the transaction goes on under the new schema.
    async fn schema_signal(
        &mut self,
        table: TableRef,
        hint: SchemaHint,
    ) -> Result<Option<End>, WorkerError> {
        let Some(cached) = self.schemas.get(&table).cloned() else {
            return Ok(None);
        };
        let planned = self.core.evolver().plan(&cached, &hint).await;
        if planned.as_ref().is_ok_and(|p| p.now == cached) {
            // A signal with nothing behind it (REPLICA IDENTITY FULL signals at every Relation
            // message): no flush, no evolve, no cache write.
            return Ok(None);
        }
        if !matches!(&planned, Ok(p) if p.safe.is_empty()) {
            self.flush()?;
            if let Some(end) = self.until_durable().await? {
                return Ok(Some(end));
            }
            if !self.schemas.contains_key(&table) {
                // The sink refused the flush and the writer halted the table meanwhile.
                return Ok(None);
            }
        }
        let Planned { now, safe } = match planned {
            Ok(planned) => planned,
            Err(reason) => {
                self.halt(table, reason);
                return Ok(None);
            }
        };
        if !safe.is_empty() {
            match self.core.evolver().evolve(&table, safe).await? {
                Ok(None) => {}
                Ok(Some(warning)) => self.core.update(|st| st.warnings.push(warning)),
                Err(reason) => {
                    self.halt(table, reason);
                    return Ok(None);
                }
            }
        }
        let extra = self.core.settings.delete_extra(&now);
        loop {
            match self.acc.set_schema(now.clone(), &extra) {
                Err(AccumulatorError::FlushRequired { .. }) => self.flush()?,
                Err(AccumulatorError::Halt { table, reason }) => {
                    self.halt(table, reason);
                    return Ok(None);
                }
                r => break r?,
            }
        }
        self.schemas.insert(table, now.clone());
        if self.acc.in_transaction() {
            self.txn_schemas.push(now);
        } else {
            self.pending_schemas
                .push((self.acc.frontier().clone(), now));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{ColumnSchema, ColumnValue, ColumnValueType as V, Row, SinkError};
    use data_avalanche_testkit::{FakeTxn, Method, SinkOp};

    use super::*;
    use crate::test_support::{World, Yaml, pipeline, row, t, wait_until};

    /// Runs a session over `w` until `drive` returns, then drains it.
    async fn session<F: Future<Output = ()>>(
        w: &World,
        p: &data_avalanche_config::Pipeline,
        tables: &[&str],
        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
    ) -> (Result<End, WorkerError>, WorkerStatus) {
        let (mut core, control, status) = w.core(p, tables);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let watching = status.clone();
        let driver = async move {
            drive(watching).await;
            let _ = control.send(Command::Drain).await;
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        let last = status.borrow().clone();
        (end, last)
    }

    fn flush_each() -> data_avalanche_config::Pipeline {
        pipeline("", "flush: { seconds: 0 }", "")
    }

    async fn durable(status: &watch::Receiver<WorkerStatus>, at: SourcePosition) {
        wait_until("the log's end is durable", || {
            status.borrow().checkpoint.as_ref() == Some(&at)
        })
        .await;
    }

    async fn checkpoint(w: &World) -> Lsn {
        w.docs
            .slot()
            .await
            .unwrap()
            .unwrap()
            .doc
            .confirmed_flush_lsn
    }

    fn lsn_of(p: &SourcePosition) -> Lsn {
        match p {
            SourcePosition::Pg(l) => *l,
            other => panic!("{other}"),
        }
    }

    #[tokio::test]
    async fn a_session_streams_flushes_and_acknowledges_what_is_durable() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(1, "x"))
                    .insert(&t("b"), row(1, "y")),
            );
            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!((w.ids("a"), w.ids("b")), (vec![1, 2], vec![1]));
        let end = w.source.wal_end();
        assert_eq!(w.source.slot(), Some(end.clone()), "acknowledged");
        assert_eq!(checkpoint(&w).await, lsn_of(&end));
        assert_eq!((st.read, st.checkpoint), (Some(end.clone()), Some(end)));
    }

    #[tokio::test]
    async fn a_drain_flushes_what_the_policy_held_back() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (end, _) = session(&w, &p, &["a"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            let end = w.source.wal_end();
            wait_until("the commit is read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            assert!(w.ids("a").is_empty(), "the policy holds it");
        })
        .await;
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!(w.ids("a"), [1]);
        assert_eq!(checkpoint(&w).await, lsn_of(&w.source.wal_end()));
    }

    #[tokio::test]
    async fn a_drain_with_the_handle_gone_finishes_the_open_transaction() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline(
            "",
            "flush: { seconds: 0 }, max_transaction_batch_rows: 2, buffer_capacity_rows: 1",
            "",
        );
        let (mut core, control, _status) = w.core(&p, &["a"]);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        w.sink.hold_commits();
        let mut txn = FakeTxn::new();
        for id in 1..=6 {
            txn = txn.insert(&t("a"), row(id, "x"));
        }
        w.source.commit(txn);
        let driver = async {
            // A slice is in the writer and the next waits for room: the transaction is open.
            tokio::time::sleep(Duration::from_millis(50)).await;
            control.send(Command::Drain).await.unwrap();
            drop(control);
            w.sink.release_commits();
        };
        let (end, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(s.run(), driver)
        })
        .await
        .expect("the drain ends");
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn a_full_buffer_stops_the_reading() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 0 }, buffer_capacity_rows: 1", "");
        w.sink.hold_commits();
        let (end, _) = session(&w, &p, &["a"], async |status| {
            let mut ends = vec![];
            for id in 1..=3 {
                w.source
                    .commit(FakeTxn::new().insert(&t("a"), row(id, "x")));
                ends.push(w.source.wal_end());
            }
            wait_until("the second commit is read", || {
                status.borrow().read.as_ref() == Some(&ends[1])
            })
            .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(
                status.borrow().read.as_ref(),
                Some(&ends[1]),
                "one flush is in the writer and one waits: the third isn't read"
            );
            w.sink.release_commits();
            durable(&status, ends[2].clone()).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2, 3]);
    }

    #[tokio::test]
    async fn a_truncate_flushes_the_tables_earlier_changes_first() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (end, _) = session(&w, &p, &["a"], async |status| {
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(1, "x"))
                    .insert(&t("a"), row(2, "x")),
            );
            w.source.commit(
                FakeTxn::new()
                    .truncate(&[t("a")])
                    .insert(&t("a"), row(3, "x")),
            );
            let end = w.source.wal_end();
            wait_until("both are read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [3]);
        assert_eq!(
            w.sink.version(&t("a")),
            Some(2),
            "the barrier's flush, then the drain's"
        );
    }

    #[tokio::test]
    async fn a_large_transaction_commits_in_slices() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline(
            "",
            "flush: { seconds: 0 }, max_transaction_batch_rows: 2",
            "",
        );
        let (end, _) = session(&w, &p, &["a"], async |status| {
            let mut txn = FakeTxn::new();
            for id in 1..=5 {
                txn = txn.insert(&t("a"), row(id, "x"));
            }
            w.source.commit(txn);
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5]);
        assert!(
            w.sink.version(&t("a")).unwrap() >= 3,
            "two slices, then the commit"
        );
    }

    #[tokio::test]
    async fn a_halted_table_is_recorded_and_its_later_changes_dropped() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let start = w.source.wal_end();
        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
            let mut bad = row(1, "x");
            bad.0.push(("zzz".into(), ColumnValue::Int64(1)));
            w.source.commit(FakeTxn::new().insert(&t("a"), bad));
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(2, "x"))
                    .insert(&t("b"), row(1, "y")),
            );
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!((w.ids("a"), w.ids("b")), (vec![], vec![1]));
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1);
        assert_eq!((&halts[0].table, &halts[0].at), (&t("a"), &start));
        assert!(matches!(halts[0].reason, HaltReason::SchemaMismatch { .. }));
        assert!(st.halted.contains_key(&t("a")));
    }

    #[tokio::test]
    async fn a_sink_refusal_halts_the_table() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.sink
            .fail(SinkOp::Commit, Some(&t("a")), 1, || SinkError::Permanent {
                reason: "refused".into(),
            });
        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(2, "x"))
                    .insert(&t("b"), row(1, "y")),
            );
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!((w.ids("a"), w.ids("b")), (vec![], vec![1]));
        assert!(matches!(
            st.halted.get(&t("a")),
            Some(HaltReason::SinkRejected { .. })
        ));
    }

    #[tokio::test]
    async fn a_source_failure_ends_the_session_after_what_is_durable() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (mut core, _control, status) = w.core(&flush_each(), &["a"]);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let driver = async {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            durable(&status, w.source.wal_end()).await;
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(
            matches!(end.unwrap(), End::Source(SourceError::Transient(_))),
            "the worker reopens it"
        );
        assert_eq!(w.ids("a"), [1]);
    }

    #[tokio::test]
    async fn re_query_reads_the_values_the_stream_left_unchanged() {
        for (strategy, want) in [("re_query", Some("big")), ("carry_forward", None)] {
            let w = World::new(&["a"]);
            w.started(&["a"]).await;
            w.source.load(&t("a"), vec![row(1, "big")]);
            let source = format!("\n      toast_strategy: {strategy}");
            let p = Yaml {
                source: &source,
                sink: "flush: { seconds: 0 }",
                ..Yaml::default()
            }
            .build();
            let (end, _) = session(&w, &p, &["a"], async |status| {
                let unchanged = Row(vec![
                    ("id".into(), ColumnValue::Int64(1)),
                    ("doc".into(), ColumnValue::Unchanged),
                ]);
                w.source.commit(FakeTxn::new().update(&t("a"), unchanged));
                durable(&status, w.source.wal_end()).await;
            })
            .await;
            end.unwrap();
            assert_eq!(
                w.text("a", "doc"),
                [want.map(String::from)],
                "{strategy}: the sink has no row to keep the value from"
            );
        }
    }

    /// A session over `w` with channel D every few milliseconds.
    async fn rescanning<F: Future<Output = ()>>(
        w: &World,
        p: &data_avalanche_config::Pipeline,
        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
    ) -> WorkerStatus {
        let (mut core, control, status) = w.core(p, &["a"]);
        core.settings.rescan_interval = Some(Duration::from_millis(5));
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let watching = status.clone();
        let driver = async move {
            drive(watching).await;
            let _ = control.send(Command::Drain).await;
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        end.unwrap();
        status.borrow().clone()
    }

    #[tokio::test]
    async fn a_rescan_evolves_an_idle_tables_change() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        rescanning(&w, &flush_each(), async |_| {
            w.source.alter(&t("a"), with_note("a"));
            wait_until("the sink has the column", || {
                w.sink
                    .content(&t("a"))
                    .is_some_and(|c| c.column_by_name("note").is_some())
            })
            .await;
        })
        .await;
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert!(cached.column("note").is_some());
    }

    #[tokio::test]
    async fn a_dropped_table_halts_once_the_stream_has_its_last_changes() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 0 }, buffer_capacity_rows: 1", "");
        w.sink.hold_commits();
        let st = rescanning(&w, &p, async |status| {
            for id in 1..=3 {
                w.source
                    .commit(FakeTxn::new().insert(&t("a"), row(id, "x")));
            }
            w.source.drop_table(&t("a"));
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                status.borrow().halted.is_empty(),
                "the stream hasn't read the third insert yet"
            );
            w.sink.release_commits();
            wait_until("the table halts", || !status.borrow().halted.is_empty()).await;
        })
        .await;
        assert_eq!(st.halted.get(&t("a")), Some(&HaltReason::TableDropped));
        assert_eq!(w.ids("a"), [1, 2, 3], "nothing before the drop is lost");
    }

    #[tokio::test]
    async fn the_retained_wal_is_reported_and_bounded() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 0 }", "max_wal_lag_bytes: 64MiB");
        let (mut core, _control, status) = w.core(&p, &["a"]);
        core.settings.heartbeat = Duration::from_millis(5);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let driver = async {
            w.source.set_retained(Some(1024));
            wait_until("the lag is reported", || {
                status.borrow().retained_bytes == Some(1024)
            })
            .await;
            w.source.set_retained(Some(65 << 20));
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(matches!(
            end,
            Err(WorkerError::WalLag {
                lag: 68_157_440,
                limit: 67_108_864
            })
        ));
    }

    fn with_note(name: &str) -> TableSchema {
        TableSchema::new(
            t(name),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn noted(id: i64) -> Row {
        let mut r = row(id, "x");
        r.0.push(("note".into(), ColumnValue::Text("n".into())));
        r
    }

    #[tokio::test]
    async fn an_added_column_is_evolved_where_the_stream_meets_it() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (end, _) = session(&w, &flush_each(), &["a"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            w.source.alter(&t("a"), with_note("a"));
            w.source.commit(FakeTxn::new().insert(&t("a"), noted(2)));
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2]);
        let content = w.sink.content(&t("a")).unwrap();
        assert!(content.column_by_name("note").is_some());
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert!(cached.column("note").is_some(), "cached once durable");
        assert_eq!(cached.source_id(), w.source.schema(&t("a")).source_id());
    }

    #[tokio::test]
    async fn a_signal_that_changes_nothing_neither_flushes_nor_evolves() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (mut core, _control, _status) = w.core(&p, &["a"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let at = lsn_of(&w.source.wal_end()).0 + 0x100;
        crate::test_support::txn(&mut s.acc, 900, at, &[("a", 1, "x")]);
        let submitted = s.submitted.clone();
        let version = w.sink.version(&t("a"));
        let cached = s.schemas[&t("a")].clone();
        let hint = SchemaHint::Columns(cached.columns().to_vec());
        assert!(s.schema_signal(t("a"), hint).await.unwrap().is_none());
        assert_eq!(s.submitted, submitted, "no flush");
        assert!(s.queued.is_empty());
        assert_eq!(w.sink.version(&t("a")), version, "no evolve");
        assert!(s.pending_schemas.is_empty() && s.txn_schemas.is_empty());
    }

    #[tokio::test]
    async fn a_safe_change_halts_when_evolution_is_off() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = Yaml {
            pipeline: "\n    schema_evolution: { enabled: false }",
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build();
        let (end, st) = session(&w, &p, &["a"], async |status| {
            w.source.alter(&t("a"), with_note("a"));
            w.source.commit(FakeTxn::new().insert(&t("a"), noted(1)));
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        assert_eq!(st.halted.get(&t("a")), Some(&HaltReason::EvolutionDisabled));
        assert!(w.ids("a").is_empty());
    }

    #[tokio::test]
    async fn an_ignorable_change_neither_flushes_nor_evolves() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (mut core, _control, _status) = w.core(&p, &["a"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let at = lsn_of(&w.source.wal_end()).0 + 0x100;
        crate::test_support::txn(&mut s.acc, 900, at, &[("a", 1, "x")]);
        let submitted = s.submitted.clone();
        let version = w.sink.version(&t("a"));
        let not_null = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text").not_null(),
            ],
            vec!["id".into()],
        )
        .unwrap();
        w.source.alter(&t("a"), not_null);
        let hint = SchemaHint::Columns(w.source.schema(&t("a")).columns().to_vec());
        assert!(s.schema_signal(t("a"), hint).await.unwrap().is_none());
        assert_eq!(s.submitted, submitted, "no flush");
        assert!(s.queued.is_empty());
        assert_eq!(w.sink.version(&t("a")), version, "no evolve");
        assert!(!s.schemas[&t("a")].column("doc").unwrap().nullable);
        s.flush().unwrap();
        assert!(s.until_durable().await.unwrap().is_none());
        assert_eq!(w.ids("a"), [1], "the buffered row keeps its place");
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert!(!cached.column("doc").unwrap().nullable, "cached with it");
    }

    fn int_doc(name: &str) -> TableSchema {
        TableSchema::new(
            t(name),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Int64, "bigint"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn int_row(id: i64) -> Row {
        Row(vec![
            ("id".into(), ColumnValue::Int64(id)),
            ("doc".into(), ColumnValue::Int64(7)),
        ])
    }

    /// A change to `a` that halts it (`p`, the catalog becomes `altered`, the next row is
    /// `next(id)`) while `a` and `b` have committed rows buffered. `a`'s rows reach the sink
    /// before it halts, and the halt holds from that flush's checkpoint. A restart neither loses
    /// nor repeats a row.
    async fn halts_after_the_earlier_rows(
        p: &data_avalanche_config::Pipeline,
        altered: TableSchema,
        next: fn(i64) -> Row,
        want: fn(&HaltReason) -> bool,
    ) {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let mut before = None;
        let (end, st) = session(&w, p, &["a", "b"], async |status| {
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(1, "x"))
                    .insert(&t("a"), row(2, "x"))
                    .insert(&t("b"), row(1, "y")),
            );
            before = Some(w.source.wal_end());
            w.source.alter(&t("a"), altered);
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), next(3))
                    .insert(&t("b"), row(2, "y")),
            );
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        let before = before.expect("driven");
        assert!(want(&st.halted[&t("a")]), "{:?}", st.halted);
        assert_eq!(w.ids("a"), [1, 2], "the rows before the change are in");
        assert_eq!((w.ids("b"), w.sink.version(&t("a"))), (vec![1, 2], Some(1)));
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1);
        assert_eq!((&halts[0].table, &halts[0].at), (&t("a"), &before));
        assert!(want(&halts[0].reason));

        let (end, st) = session(&w, p, &["a", "b"], async |status| {
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), next(4))
                    .insert(&t("b"), row(3, "y")),
            );
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        assert!(st.halted.contains_key(&t("a")));
        assert_eq!(
            (w.ids("a"), w.sink.version(&t("a"))),
            (vec![1, 2], Some(1)),
            "the restart neither loses nor rewrites a row"
        );
        assert_eq!(w.ids("b"), [1, 2, 3]);
        assert_eq!(w.docs.halts().await.unwrap().tables, halts);
    }

    #[tokio::test]
    async fn an_unsafe_change_halts_after_the_tables_earlier_rows_are_durable() {
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        halts_after_the_earlier_rows(&p, int_doc("a"), int_row, |r| {
            matches!(r, HaltReason::UnsafeDdl { .. })
        })
        .await;
    }

    #[tokio::test]
    async fn a_change_with_evolution_off_halts_after_the_tables_earlier_rows_are_durable() {
        let p = Yaml {
            pipeline: "\n    schema_evolution: { enabled: false }",
            sink: "flush: { seconds: 3600 }",
            ..Yaml::default()
        }
        .build();
        halts_after_the_earlier_rows(&p, with_note("a"), noted, |r| {
            *r == HaltReason::EvolutionDisabled
        })
        .await;
    }

    #[tokio::test]
    async fn a_sink_refusal_of_the_flush_before_a_halt_keeps_its_reason() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.sink
            .fail(SinkOp::Commit, Some(&t("a")), 1, || SinkError::Permanent {
                reason: "refused".into(),
            });
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (end, st) = session(&w, &p, &["a"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            w.source.alter(&t("a"), int_doc("a"));
            w.source.commit(FakeTxn::new().insert(&t("a"), int_row(2)));
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        let halts = w.docs.halts().await.unwrap().tables;
        assert!(
            matches!(
                st.halted.get(&t("a")),
                Some(HaltReason::SinkRejected { .. })
            ) && matches!(&halts[..], [h] if matches!(h.reason, HaltReason::SinkRejected { .. })),
            "{halts:?}"
        );
    }

    #[tokio::test]
    async fn an_unsafe_change_halts_the_table() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (end, st) = session(&w, &flush_each(), &["a"], async |status| {
            w.source.alter(&t("a"), int_doc("a"));
            w.source.commit(FakeTxn::new().insert(&t("a"), int_row(1)));
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        assert!(matches!(
            st.halted.get(&t("a")),
            Some(HaltReason::UnsafeDdl { .. })
        ));
    }

    #[tokio::test]
    async fn a_widening_the_sink_cant_apply_halts_the_table() {
        let narrow = |v| {
            TableSchema::new(
                t("w"),
                vec![
                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("n", v, "int"),
                ],
                vec!["id".into()],
            )
            .unwrap()
        };
        let w = World::with(vec![narrow(V::Int32)]);
        w.started(&["w"]).await;
        let (end, st) = session(&w, &flush_each(), &["w"], async |status| {
            w.source.alter(&t("w"), narrow(V::Int64));
            w.source.commit(FakeTxn::new().insert(
                &t("w"),
                Row(vec![
                    ("id".into(), ColumnValue::Int64(1)),
                    ("n".into(), ColumnValue::Int64(1 << 40)),
                ]),
            ));
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        assert!(matches!(
            st.halted.get(&t("w")),
            Some(HaltReason::EvolutionUnsupported { .. })
        ));
    }

    #[tokio::test]
    async fn a_failed_catalog_read_halts_the_table() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
            w.source
                .fail(Method::Augment, SourceError::Permanent("denied".into()));
            w.source.alter(&t("a"), with_note("a"));
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), noted(1))
                    .insert(&t("b"), row(1, "y")),
            );
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        match st.halted.get(&t("a")) {
            Some(HaltReason::UnsafeDdl { detail }) => {
                assert!(detail.contains("denied"), "{detail}")
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(w.ids("b"), [1], "the other table goes on");
    }
}
