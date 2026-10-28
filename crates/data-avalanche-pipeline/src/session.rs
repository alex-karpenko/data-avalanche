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

use data_avalanche_config::Pipeline;
use data_avalanche_core::{
    CdcEvent, ChangeStream, ColumnValue, HaltReason, Op, RescanReport, RetainedLog, Row,
    SchemaHint, ServerIdentity, Sink, SinkTableState, SourceConnector, SourceError, SourcePosition,
    SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary, identifier_values,
};
use data_avalanche_state::{PipelineDocs, TableHalt};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinError, JoinHandle, JoinSet};

use self::snapshot::Snapshots;
use crate::schema_change::{Evolver, Planned, soft_dropped};
use crate::settings::Settings;
use crate::status::Streak;
use crate::watchdog::Watchdog;
use crate::writer::{self, Job, WriterCtx, WriterEvent};
use crate::{
    Accumulator, AccumulatorError, Command, CommitGuard, Exit, FlushReason, Hooks, Observer, Phase,
    RowsRead, Tuning, WorkerError, WorkerStatus,
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
    pub observer: Arc<dyn Observer>,
    pub tuning: Tuning,
    pub control: mpsc::Receiver<Command>,
    pub status: watch::Sender<WorkerStatus>,
    /// The tables Phase 0 tracks. A session streams each one unless it is halted or a done dump.
    pub tracked: BTreeSet<TableRef>,
    /// A job's tables outside the config whose copy runs (Q-M9b-3), with their schemas as Phase
    /// 0 described them: read like a dump, never streamed, their schemas cached nowhere.
    pub copies: BTreeMap<TableRef, TableSchema>,
    /// Every table of the pipeline is a `dump_*` table: the worker ends once none is left.
    pub dump_only: bool,
    /// When each table's snapshot began in this run (`timeouts.snapshot`): at the first session
    /// that runs it after Phase 0 requested it from scratch.
    pub snapshot_started: BTreeMap<TableRef, Instant>,
    /// Tables whose catalog schema moved past the cached one while the pipeline was down (Phase
    /// 0 on a resume). A snapshot of one reads only once a rescan has brought its schema up to
    /// date (D-M6a.11).
    pub drifted: BTreeSet<TableRef>,
    /// Channel D's failing rounds. It outlives a session: a reconnect doesn't start a streak.
    pub rescan_failing: Streak,
    /// The failing rounds while tables are held for a rescan (D-M6a.11). Like `rescan_failing`
    /// it outlives a session; it also sets the held tables' backoff. It ends with a round that
    /// succeeds, or when no table is held any more.
    pub held_failing: Streak,
    /// `toast_strategy: re_query`: the tables whose fetch is failing. Like `rescan_failing` it
    /// outlives a session, so the replay after a reconnect doesn't warn or back off again.
    pub requery_failing: BTreeMap<TableRef, Streak>,
    /// Phase 0's failing rescans of the tables gone from the source's list (a start after a lost
    /// position continues the streak).
    pub gone_failing: Streak,
    /// The WAL-lag guard's permanently failing reads. Like `rescan_failing` it outlives a session.
    pub wal_lag_failing: Streak,
    /// The snapshot tables whose reads wait for writers in flight at a session's start
    /// (Q-M6a-2), warned once per wait. A wait ends once the table may be read; like
    /// `rescan_failing` it outlives a session, so a reconnect that finds the table still waiting
    /// warns nothing more.
    pub writers_waited: BTreeSet<TableRef>,
    /// The tables whose chunk reads are failing, a streak each. Like `requery_failing` it outlives
    /// a session.
    pub read_failing: BTreeMap<TableRef, Streak>,
    /// The server the checkpoint is recorded with (MySQL; `None` for PostgreSQL): the one the
    /// last start read, from `prepare` or the reset position. A reconnect keeps it.
    pub server: Option<ServerIdentity>,
    /// [`Command::Pause`] is in force: a session finishes its open transaction, makes it durable
    /// and reads nothing more until [`Command::Resume`]. It outlives a session, so a reconnect
    /// while paused stays paused.
    pub pause: bool,
    /// [`WorkerStatus::rows_read`], counted by every session of the worker.
    pub rows_read: RowsRead,
}

impl Core {
    pub fn update(&self, f: impl FnOnce(&mut WorkerStatus)) {
        self.status.send_modify(f);
    }

    /// Asked before each write Phase 0 makes (state documents, the slot, a sink table), as the
    /// writer asks before each commit: a worker whose lease lapsed writes nothing more.
    pub fn may_write(&self) -> Result<(), WorkerError> {
        if self.guard.may_commit() {
            Ok(())
        } else {
            Err(WorkerError::LeaseLost)
        }
    }

    /// A reload's settings (config §1.2, class R): the worker's, and the source's.
    pub fn reconfigure(&mut self, p: &Pipeline) {
        self.settings.reconfigure(p);
        self.source.reconfigure(p);
    }

    /// The sink's soft-dropped columns of `now`'s table into the status, after an evolve; a
    /// failed read leaves the count as it was.
    pub async fn count_soft_dropped(&self, now: &TableSchema) {
        if let Ok(SinkTableState {
            schema: Some(sink), ..
        }) = self.sink.table_state(now.table()).await
        {
            let n = soft_dropped(&sink, now);
            self.update(|st| {
                st.soft_dropped.insert(now.table().clone(), n);
            });
        }
    }

    pub fn evolver(&self) -> Evolver<'_> {
        Evolver {
            source: &*self.source,
            sink: &*self.sink,
            guard: &*self.guard,
            owner_epoch: self.owner_epoch,
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

/// `buffer_capacity_rows` as semaphore permits.
fn buffer_permits(rows: u64) -> usize {
    usize::try_from(rows)
        .unwrap_or(usize::MAX)
        .min(Semaphore::MAX_PERMITS)
}

/// The heartbeat's and the rescans' timers (none with rescans off), each first due one interval
/// from now.
fn timers(s: &Settings) -> (tokio::time::Interval, Option<tokio::time::Interval>) {
    let every = |every| {
        let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        timer
    };
    (every(s.heartbeat), s.rescan_interval.map(every))
}

fn protocol(detail: String) -> WorkerError {
    AccumulatorError::Protocol { detail }.into()
}

/// The session's writer task, aborted when the session is dropped: a worker cut off mid-flush
/// (its task aborted, say past `drain_timeout`) leaves no writer committing on its own.
struct WriterTask(JoinHandle<()>);

impl Drop for WriterTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct Session<'c> {
    core: &'c mut Core,
    acc: Accumulator,
    /// The streamed tables' schemas as of the frontier.
    schemas: BTreeMap<TableRef, TableSchema>,
    /// [`Core::copies`] still being read.
    copies: BTreeMap<TableRef, TableSchema>,
    stream: Box<dyn ChangeStream>,
    jobs: UnboundedSender<Job>,
    events: UnboundedReceiver<WriterEvent>,
    writer: WriterTask,
    /// `buffer_capacity_rows`: rows handed to the writer and not yet durable.
    permits: Arc<Semaphore>,
    /// Flushes waiting for room in the buffer, in order. The stream isn't read meanwhile.
    queued: VecDeque<(Job, u32)>,
    /// The checkpoint of the last flush handed on.
    submitted: SourcePosition,
    /// Jobs handed on, queued ones included: each job's number, from 1, in order. Consecutive
    /// jobs may share a checkpoint (chunks are emitted at the frontier), so durability counts
    /// jobs, not positions.
    handed: u64,
    /// Jobs durable: the writer runs them in order and reports each once.
    durable: u64,
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
    /// A rescan's findings, applied once the stream passes the position read after it, with the
    /// changed tables' cached schemas as they were at the rescan.
    rescan: Option<(
        SourcePosition,
        RescanReport,
        BTreeMap<TableRef, TableSchema>,
    )>,
    snaps: Snapshots,
    draining: bool,
    /// When this session parked for [`Command::Pause`], with everything it read durable.
    paused: Option<Instant>,
    /// The heartbeat's read of the source's log, one at a time, run beside the loop: a source
    /// that doesn't answer holds up neither the stream nor a drain (DA-SRC-16).
    retained_read: JoinSet<Result<Option<RetainedLog>, SourceError>>,
    watchdog: Watchdog,
    /// A reload changed the heartbeat or the rescan interval: the loop re-arms its timers.
    retimed: bool,
    /// The open transaction's commit time (microseconds since the Unix epoch).
    open_since: Option<i64>,
    /// The commit time of the oldest transaction read since the last flush handed on.
    unflushed_since: Option<i64>,
    /// The jobs handed on and not durable yet, each with the commit time of its oldest
    /// transaction, in job order.
    unsynced: VecDeque<(u64, i64)>,
}

impl<'c> Session<'c> {
    /// Loads the durable state and opens the stream at the checkpoint; `Ok(Err)` when the source
    /// fails to open it.
    pub async fn open(core: &'c mut Core) -> Result<Result<Self, SourceError>, WorkerError> {
        let from = match core.settings.checkpoint.load(&core.docs).await? {
            Some(s) if !s.intent => s.checkpoint,
            _ => return Err(protocol("a stream session needs a checkpoint".into())),
        };
        let checkpoint = from.position.clone();
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
        let mut copies = BTreeMap::new();
        for (table, schema) in &core.copies {
            match acc.add_table(schema.clone(), &s.delete_extra(schema)) {
                Ok(()) => {
                    copies.insert(table.clone(), schema.clone());
                }
                Err(AccumulatorError::Halt { table, reason }) => halts.push(TableHalt {
                    table,
                    reason,
                    at: checkpoint.clone(),
                }),
                Err(e) => return Err(e.into()),
            }
        }
        let read: BTreeMap<TableRef, TableSchema> = schemas
            .iter()
            .chain(&copies)
            .map(|(t, s)| (t.clone(), s.clone()))
            .collect();
        let (mut snaps, retired, warnings) = Snapshots::load(core, &read).await?;
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
        let stream = match core.source.open_stream(&from, &live).await {
            Ok(stream) => stream,
            Err(e) => return Ok(Err(e)),
        };
        let (jobs, jobs_rx) = mpsc::unbounded_channel();
        let (events_tx, events) = mpsc::unbounded_channel();
        let ctx = WriterCtx {
            sink: core.sink.clone(),
            docs: core.docs.clone(),
            store: core.settings.checkpoint.clone(),
            server: core.server.clone(),
            pipeline_id: core.settings.pipeline_id.clone(),
            owner_epoch: core.owner_epoch,
            guard: core.guard.clone(),
            hooks: core.hooks.clone(),
            observer: core.observer.clone(),
            backoff: core.tuning.sink_backoff,
        };
        let writer = tokio::spawn(writer::run(ctx, jobs_rx, events_tx));
        let rows = buffer_permits(core.settings.buffer_rows);
        core.update(|st| {
            st.phase = Phase::Streaming;
            st.checkpoint = Some(checkpoint.clone());
            for h in recorded.iter().chain(&halts) {
                st.halted.insert(h.table.clone(), h.reason.clone());
            }
            warnings.into_iter().for_each(|w| st.warn(w));
            // Replaced, not extended: a snapshot an earlier session finished, or ended with a
            // completion this session's state shows durable, is over.
            st.snapshots = snaps.progress().collect();
        });
        Ok(Ok(Self {
            core,
            acc,
            schemas,
            copies,
            stream,
            jobs,
            events,
            writer: WriterTask(writer),
            permits: Arc::new(Semaphore::new(rows)),
            queued: VecDeque::new(),
            submitted: checkpoint,
            handed: 0,
            durable: 0,
            halts,
            txn_schemas: vec![],
            pending_schemas: vec![],
            requery: BTreeMap::new(),
            last_seq: None,
            rescan: None,
            snaps,
            draining: false,
            paused: None,
            retained_read: JoinSet::new(),
            watchdog: Watchdog::default(),
            retimed: false,
            open_since: None,
            unflushed_since: None,
            unsynced: VecDeque::new(),
        }))
    }

    /// Runs the session to its end. Flushes already handed to the writer are made durable before
    /// it returns, unless it fails.
    pub async fn run(mut self) -> Result<End, WorkerError> {
        let ended = self.event_loop().await;
        // The chunk reads still out are aborted: the next session reads again from the cursors.
        self.snaps.reads.abort_all();
        match ended {
            Ok(end) => {
                let end = self.finish(end).await?;
                Ok(end)
            }
            Err(e) => {
                self.writer.0.abort();
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
                    st.snapshots.remove(&h.table);
                    st.halted.insert(h.table, h.reason);
                }),
                WriterEvent::Degraded(why) => self.core.update(|st| st.degraded = why),
                WriterEvent::Durable(cp) => {
                    // A completion durable now ends its snapshot, as in the loop.
                    self.durable += 1;
                    let done = self.snaps.durable(self.durable);
                    self.core.update(|st| {
                        for (table, _) in done {
                            st.snapshots.remove(&table);
                        }
                        st.checkpoint = Some(cp);
                    });
                }
            }
        }
        let _ = self.stream.close().await;
        Ok(end)
    }

    async fn event_loop(&mut self) -> Result<End, WorkerError> {
        let mut tick = tokio::time::interval(self.core.tuning.tick);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let (mut heartbeat, mut rescan) = timers(&self.core.settings);
        self.issue_reads();
        loop {
            if std::mem::take(&mut self.retimed) {
                (heartbeat, rescan) = timers(&self.core.settings);
            }
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
            if self.core.pause && self.paused.is_none() && !self.acc.in_transaction() {
                self.flush()?;
                if let Some(end) = self.until_durable().await? {
                    return Ok(end);
                }
                self.paused = Some(Instant::now());
                self.core.update(|st| st.phase = Phase::Paused);
            }
            // Pausing (the open transaction still to finish) or paused: no chunk is read, no
            // rescan runs, the snapshot clock stands. Paused: the stream isn't read either.
            let parked = self.core.pause;
            let reading = self.reads_stream();
            let need = self.queued.front().map_or(0, |(_, n)| *n);
            let permits = self.permits.clone();
            // Biased: control, the writer's events and the buffer's room first; then the chunk
            // reads, the tick, the heartbeat and the rescans, each rarely ready; the stream last.
            // A stream with a backlog is ready at every poll, and above them it would starve
            // them: finished chunks unemitted, the snapshot timeout running on, no in-flight
            // recheck, the WAL-lag guard blind (final review I1).
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
                // Only for a queued job: an empty queue needs no permit, and would make this
                // arm ready at every poll while paused.
                p = permits.acquire_many_owned(need), if !self.queued.is_empty() => {
                    self.send_front(p.expect("the semaphore isn't closed"));
                }
                read = self.snaps.reads.join_next(), if !self.snaps.reads.is_empty() && !parked => {
                    // Nothing aborts a read while the loop runs: one that ended without its
                    // chunk panicked, a bug, which ends the worker like the writer's.
                    let read = read.expect("not empty").map_err(|e| {
                        protocol(format!("a chunk read ended without its chunk: {e}"))
                    })?;
                    if let Some(end) = self.chunk_read(read).await? {
                        return Ok(end);
                    }
                }
                _ = tick.tick() => {
                    self.due(Instant::now())?;
                    if !parked && let Some(end) = self.snapshot_tick().await? {
                        return Ok(end);
                    }
                }
                _ = heartbeat.tick() => {
                    if let Some(end) = self.heartbeat() {
                        return Ok(end);
                    }
                }
                read = self.retained_read.join_next(), if !self.retained_read.is_empty() => {
                    self.wal_lag(read.expect("not empty"))?;
                }
                _ = async { rescan.as_mut().expect("guarded").tick().await },
                    if rescan.is_some() && self.rescan.is_none() && !parked =>
                {
                    if let Some(end) = self.rescan().await? {
                        return Ok(end);
                    }
                }
                item = self.stream.next(), if reading => match item {
                    Ok(item) => {
                        self.watchdog.heard();
                        if let Some(end) = self.item(item).await? {
                            return Ok(end);
                        }
                    }
                    Err(e) => return Ok(End::Source(e)),
                },
            }
        }
    }

    /// Takes a reload's settings in place. The buffer gets a semaphore of the new size: what is
    /// handed on holds the old one's permits until it is durable, so for that while the buffer
    /// may hold up to the old size's more; a flush waiting for room waits for at most the new
    /// size, so it can't wait for ever.
    fn reconfigure(&mut self, p: &Pipeline) {
        self.core.reconfigure(p);
        let s = &self.core.settings;
        self.acc.set_policy(s.flush);
        self.acc.set_txn_limits(s.limits);
        self.acc.set_lineage(s.lineage.clone());
        let rows = buffer_permits(s.buffer_rows);
        self.permits = Arc::new(Semaphore::new(rows));
        let cap = u32::try_from(rows).unwrap_or(u32::MAX);
        for (_, need) in &mut self.queued {
            *need = (*need).min(cap);
        }
        self.retimed = true;
    }

    fn command(&mut self, cmd: Option<Command>) -> Result<(), WorkerError> {
        match cmd {
            Some(Command::Flush) => self.flush(),
            Some(Command::Drain) | None => {
                self.draining = true;
                self.core.update(|st| st.phase = Phase::Draining);
                Ok(())
            }
            Some(Command::Pause) => {
                self.core.pause = true;
                Ok(())
            }
            Some(Command::Reconfigure(p)) => {
                self.reconfigure(&p);
                Ok(())
            }
            Some(Command::Resume) => {
                self.core.pause = false;
                if let Some(at) = self.paused.take() {
                    // The snapshot timeout counts reading time: the pause is added back.
                    let held = at.elapsed();
                    for started in self.core.snapshot_started.values_mut() {
                        *started += held;
                    }
                    self.core.update(|st| st.phase = Phase::Streaming);
                    self.issue_reads();
                }
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
                self.copies.remove(&h.table);
                self.snaps.stop(&h.table);
                self.hold_ended();
                self.core.update(|st| {
                    st.snapshots.remove(&h.table);
                    st.halted.insert(h.table, h.reason);
                });
                Ok(None)
            }
            Some(WriterEvent::Degraded(why)) => {
                self.core.update(|st| st.degraded = why);
                Ok(None)
            }
            Some(WriterEvent::Durable(cp)) => {
                self.durable += 1;
                debug_assert!(self.durable <= self.handed, "one Durable per job");
                while self
                    .unsynced
                    .front()
                    .is_some_and(|(job, _)| *job <= self.durable)
                {
                    self.unsynced.pop_front();
                }
                for (table, dump) in self.snaps.durable(self.durable) {
                    self.core.update(|st| {
                        st.snapshots.remove(&table);
                    });
                    if dump {
                        // A dump, or a copy, stops once its snapshot is durable.
                        self.acc.halt_table(&table);
                        self.schemas.remove(&table);
                        self.copies.remove(&table);
                    }
                }
                if let Err(e) = self.stream.acknowledge(&cp).await {
                    return Ok(Some(End::Source(e)));
                }
                self.core.update(|st| st.checkpoint = Some(cp));
                self.report_progress();
                Ok(None)
            }
        }
    }

    async fn item(&mut self, item: StreamItem) -> Result<Option<End>, WorkerError> {
        let now = Instant::now();
        match item {
            StreamItem::Begin {
                xid,
                commit_ts_micros,
            } => {
                self.open_since = Some(commit_ts_micros);
                self.unflushed_since.get_or_insert(commit_ts_micros);
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
                self.core.rows_read.count(e.op, 1);
                if let Some(schema) = self.schemas.get(&e.table) {
                    self.snaps.touch(schema, &e);
                }
                self.apply(&e, now)?;
            }
            StreamItem::Truncate { tables, .. } => loop {
                self.snaps.truncated(&tables);
                match self.acc.truncate(&tables, now) {
                    Err(AccumulatorError::FlushRequired { .. }) => self.flush()?,
                    Err(AccumulatorError::Halt { table, reason }) => self.halt(table, reason)?,
                    r => break self.absorb(r)?,
                }
            },
            StreamItem::SchemaSignal { table, hint, .. } => {
                return self.schema_signal(table, hint).await;
            }
            // A table already halted (by the session, channel D or the writer) keeps its halt:
            // another would replace its reason, and its `at` with one past rows the sink
            // doesn't hold (re-review N1).
            StreamItem::TableHalt { table, reason, .. } => {
                if self.schemas.contains_key(&table) {
                    self.halt(table, reason)?;
                }
            }
            StreamItem::Commit(b) => {
                if let Some(end) = self.re_query(&b).await? {
                    return Ok(Some(end));
                }
                self.acc.commit(&b, now)?;
                self.open_since = None;
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
        self.report_log();
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
    /// is left to the delete that follows in the stream. A failed read is tried
    /// `augment_attempts` times (the cached schema may name a column dropped since); then the
    /// values stay masked, as with `carry_forward`, and the table's failure streak begins: while
    /// it lasts, a commit tries the read once, with no backoff, and warns nothing more.
    async fn re_query(&mut self, b: &TransactionBoundary) -> Result<Option<End>, WorkerError> {
        let Some(seq) = self.last_seq else {
            return Ok(None);
        };
        for (table, keys) in std::mem::take(&mut self.requery) {
            let Some(schema) = self.schemas.get(&table).cloned() else {
                continue;
            };
            let failing = self.core.requery_failing.get(&table);
            let attempts = if failing.is_some_and(|streak| streak.failures() > 0) {
                1
            } else {
                self.core.tuning.augment_attempts.max(1)
            };
            let mut attempt = 0;
            let fetched = loop {
                let r = self.core.source.fetch_rows(&schema, &keys).await;
                match &r {
                    Err(SourceError::TableHalt { .. } | SourceError::Permanent(_)) => break r,
                    Err(_) if attempt + 1 < attempts => {
                        tokio::time::sleep(self.core.tuning.source_backoff.delay(attempt)).await;
                        attempt += 1;
                    }
                    _ => break r,
                }
            };
            match fetched {
                Ok(rows) => {
                    self.core.requery_failing.remove(&table);
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
                Err(SourceError::TableHalt { table, reason }) => self.halt(table, reason)?,
                Err(e @ SourceError::Permanent(_)) => return Ok(Some(End::Source(e))),
                Err(e) => {
                    let streak = self.core.requery_failing.entry(table.clone()).or_default();
                    streak.failed(&self.core.status, || {
                        format!(
                            "table {table}: re_query couldn't read the current rows ({e}) after {attempts} attempts; the unchanged values stay masked: the sink keeps the values it holds (a row it doesn't hold, or a changed key, gets NULL)"
                        )
                    });
                }
            }
        }
        Ok(None)
    }

    /// Channel D: one catalog read of the streamed tables, and the source's position after it. A
    /// failed round waits for the next; the first of a streak warns that dropped-table detection
    /// is paused.
    ///
    /// A held table the rescan doesn't name has its cache up to date, and is released. When the
    /// stream brought it there with a change no flush has handed on yet, the rescan flushes it
    /// first, whatever the flush policy: a policy with no interval might never do it. A change
    /// in the open transaction keeps the table held until a round after its commit.
    async fn rescan(&mut self) -> Result<Option<End>, WorkerError> {
        let known: Vec<TableSchema> = self.schemas.values().cloned().collect();
        let report = match self.core.source.rescan(&known).await {
            Ok(report) => report,
            Err(e) => {
                self.rescan_failed(&e);
                return Ok(None);
            }
        };
        let held = self.snaps.held();
        if self
            .pending_schemas
            .iter()
            .any(|(_, s)| held.contains(s.table()))
        {
            // The flush's checkpoint is the frontier, which covers every pending change.
            self.flush()?;
        }
        let kept: BTreeSet<&TableRef> = report
            .changed
            .iter()
            .map(TableSchema::table)
            .chain(&report.dropped)
            .chain(report.halted.iter().map(|(t, _)| t))
            .chain(self.pending_schemas.iter().map(|(_, s)| s.table()))
            .chain(self.txn_schemas.iter().map(TableSchema::table))
            .collect();
        // The released tables' cached schemas are durable, or in jobs handed to the writer (the
        // flush above), which `finish()` drains before the session ends: the next session opens
        // with them cached and needn't hold the tables (M-2).
        for table in self.snaps.release_unless(&kept) {
            self.core.drifted.remove(&table);
        }
        self.hold_ended();
        if report == RescanReport::default() {
            self.rescan_succeeded();
            self.held_round_ended();
            return Ok(None);
        }
        let after = match self.core.source.current_position().await {
            Ok(after) => after,
            Err(e) => {
                self.rescan_failed(&e);
                return Ok(None);
            }
        };
        self.rescan_succeeded();
        let cached = known
            .into_iter()
            .filter(|k| report.changed.iter().any(|c| c.table() == k.table()))
            .map(|k| (k.table().clone(), k))
            .collect();
        self.rescan = Some((after, report, cached));
        if self.acc.in_transaction() {
            return Ok(None);
        }
        self.caught_up().await
    }

    /// A failed round. While tables are held, the next round waits for a backoff, and the first
    /// failure of a streak warns that their snapshots wait (Task 16 review M-1).
    fn rescan_failed(&mut self, e: &SourceError) {
        self.core.rescan_failing.failed(&self.core.status, || {
            format!(
                "a catalog rescan failed ({e}): dropped-table detection is paused until a rescan succeeds"
            )
        });
        let held = self.snaps.held();
        if held.is_empty() {
            return;
        }
        let names: Vec<String> = held.iter().map(ToString::to_string).collect();
        self.core.held_failing.failed(&self.core.status, || {
            format!(
                "the snapshots of {} wait for a catalog rescan, which failed ({e}): they read nothing until a rescan succeeds; it is tried again with backoff",
                names.join(", ")
            )
        });
        let failed = self.core.held_failing.failures();
        let delay = self.core.tuning.source_backoff.delay(failed - 1);
        self.snaps.held_retry = Some(Instant::now() + delay);
    }

    /// Once no table is held, the hold's failure streak is over: a later hold (a Phase 0 after a
    /// lost position) starts its own, with its own warning and backoff.
    fn hold_ended(&mut self) {
        if self.snaps.held().is_empty() {
            self.core.held_failing.succeeded();
        }
    }

    /// A round ended without a failure. A table it leaves held has waited through it; the second
    /// such round of one hold warns, once (final review I3): the table reads nothing, and only a
    /// failing round warned so far. The hold's count ends with its release.
    fn held_round_ended(&mut self) {
        for (table, rounds) in self.snaps.held_round() {
            self.core.update(|st| {
                st.warn(format!(
                    "table {table}: its snapshot is still held after {rounds} catalog rescans: its schema changed while the pipeline was down, and its cached schema hasn't caught up with the catalog (the catalog keeps changing, or the stream has yet to commit a change of it); it reads nothing until a rescan finds the cache up to date"
                ));
            });
        }
    }

    fn rescan_succeeded(&mut self) {
        self.core.rescan_failing.succeeded();
        self.core.held_failing.succeeded();
        self.snaps.held_retry = None;
    }

    /// Applies a rescan's findings once the stream has passed the position read after it (Q-M6a-4):
    /// every change the stream holds for those tables is in by then. A table whose cached schema
    /// moved since the rescan is skipped: the stream applied a change on its own meanwhile, and the
    /// finding may be older than it, while the cache may still be behind the catalog. Any other
    /// table is read again, and its finding applies only if it still holds: the catalog may have
    /// moved since, or the cache moved and came back. A failed read ends the reads: the tables
    /// left are skipped too. A skipped table is found again by the next round, and a held one
    /// stays held until then.
    async fn caught_up(&mut self) -> Result<Option<End>, WorkerError> {
        let Some((after, ..)) = &self.rescan else {
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
        let (_, report, at_rescan) = self.rescan.take().expect("checked above");
        // The tables' changes up to here are good: flush them before the halts drop what is
        // buffered.
        self.flush()?;
        for table in report.dropped {
            if self.schemas.contains_key(&table) {
                self.halt(table, HaltReason::TableDropped)?;
            }
        }
        for (table, reason) in report.halted {
            if self.schemas.contains_key(&table) {
                self.halt(table, reason)?;
            }
        }
        let mut unapplied = BTreeSet::new();
        let mut failed = None;
        for now in report.changed {
            let table = now.table().clone();
            let Some(cached) = self.schemas.get(&table).cloned() else {
                continue;
            };
            if cached.columns() == now.columns() && cached.identifier() == now.identifier() {
                continue;
            }
            if at_rescan.get(&table) != Some(&cached) || failed.is_some() {
                unapplied.insert(table);
                continue;
            }
            match self.core.source.rescan(std::slice::from_ref(&cached)).await {
                Ok(fresh)
                    if fresh.changed.as_slice() == std::slice::from_ref(&now)
                        && fresh.dropped.is_empty()
                        && fresh.halted.is_empty() => {}
                Ok(_) => {
                    unapplied.insert(table);
                    continue;
                }
                Err(e) => {
                    failed = Some(e);
                    unapplied.insert(table);
                    continue;
                }
            }
            let hint = SchemaHint::Columns(now.columns().to_vec());
            if let Some(end) = self.schema_signal(table, hint).await? {
                return Ok(Some(end));
            }
        }
        // Caches the applied schemas without waiting for the tables' next change.
        self.flush()?;
        // The released tables' cached schemas are in the jobs just handed to the writer, which
        // `finish()` drains before the session ends: the next session opens with them cached and
        // needn't hold the tables (M-2).
        for table in self.snaps.release_unless(&unapplied.iter().collect()) {
            self.core.drifted.remove(&table);
        }
        self.hold_ended();
        // Reported once the release is done, so it names only the tables still held.
        match failed {
            Some(e) => self.rescan_failed(&e),
            None => self.held_round_ended(),
        }
        Ok(None)
    }

    /// A heartbeat: the next read of the source's log goes out unless one still is, and the
    /// watchdog counts. `Some` ends the session: the source isn't answering, so the worker
    /// reconnects.
    fn heartbeat(&mut self) -> Option<End> {
        if self.retained_read.is_empty() {
            let source = self.core.source.clone();
            self.retained_read
                .spawn(async move { source.retained().await });
        }
        self.report_progress();
        let why = self.watchdog.beat(self.reads_stream())?;
        Some(End::Source(SourceError::Transient(why)))
    }

    /// Whether the session reads the stream now: not paused, and no flush waiting for room. The
    /// stream gate and the watchdog share it: a stream the session doesn't read is silent by its
    /// own doing, and proves nothing about the source.
    fn reads_stream(&self) -> bool {
        self.queued.is_empty() && self.paused.is_none()
    }

    /// A heartbeat's read of the source's log came back. The WAL-retention guard (G-22): the
    /// retained log is always reported; past `max_wal_lag_bytes` the pipeline stops to protect
    /// the source's disk. A failed read waits for the next heartbeat, and the pipeline keeps
    /// running. The first permanent failure of a streak warns that the guard is blind; a read
    /// that succeeds ends the streak. The watchdog hears whether the source answered: a
    /// transient failure, or a stream the source doesn't run (a slot no stream reads, no binlog
    /// dump), is no answer.
    fn wal_lag(
        &mut self,
        read: Result<Result<Option<RetainedLog>, SourceError>, JoinError>,
    ) -> Result<(), WorkerError> {
        let read = read.map_err(|e| {
            protocol(format!(
                "the heartbeat's read ended without its answer: {e}"
            ))
        })?;
        let retained = match read {
            Ok(retained) => retained,
            Err(e) if e.is_transient() => {
                self.watchdog.answered(Some(e.to_string()));
                return Ok(());
            }
            Err(e) => {
                // The source answered, with a refusal.
                self.watchdog.answered(None);
                let blind = if self.core.settings.max_wal_lag_bytes.is_some() {
                    "max_wal_lag_bytes isn't enforced"
                } else {
                    "the retained WAL isn't reported"
                };
                self.core.wal_lag_failing.failed(&self.core.status, || {
                    format!(
                        "the WAL-lag guard is blind: the source's retained log can't be read ({e}); {blind} until a read succeeds"
                    )
                });
                return Ok(());
            }
        };
        self.core.wal_lag_failing.succeeded();
        let unread = self.core.settings.checkpoint.unread();
        self.watchdog.answered(
            retained
                .is_some_and(|r| !r.streamed)
                .then(|| unread.to_owned()),
        );
        let retained = retained.map(|r| r.bytes);
        self.core.update(|st| st.retained_bytes = retained);
        match (retained, self.core.settings.max_wal_lag_bytes) {
            (Some(lag), Some(limit)) if lag > limit => Err(WorkerError::WalLag { lag, limit }),
            _ => Ok(()),
        }
    }

    /// The schema a snapshot reads `table` under: a streamed table's, or a copy's.
    pub(super) fn read_schema(&self, table: &TableRef) -> Option<&TableSchema> {
        self.schemas.get(table).or_else(|| self.copies.get(table))
    }

    /// Every table is a dump, and every one is done or halted.
    fn dumped(&self) -> bool {
        self.core.dump_only && self.schemas.is_empty() && self.snaps.is_empty()
    }

    fn read(&self) {
        let frontier = self.acc.frontier().clone();
        self.core.update(|st| st.read = Some(frontier));
        self.report_progress();
    }

    /// The lag's start, the rows read and the buffered bytes into the status, when they changed.
    fn report_progress(&self) {
        let since = self.unsynced.front().map(|(_, ts)| *ts);
        let since = match (since, self.unflushed_since) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let rows_read = self.core.rows_read;
        let bytes = self.acc.buffered_bytes() as u64;
        self.core.status.send_if_modified(|st| {
            let changed = (st.unsynced_since_micros, st.rows_read, st.buffer_bytes)
                != (since, rows_read, bytes);
            st.unsynced_since_micros = since;
            st.rows_read = rows_read;
            st.buffer_bytes = bytes;
            changed
        });
    }

    /// The reconciler log's size into the status, when it changed (I3).
    pub(super) fn report_log(&self) {
        let size = self.snaps.log_size();
        self.core.status.send_if_modified(|st| {
            let changed = st.reconcile_log != size;
            st.reconcile_log = size;
            changed
        });
    }

    /// A table halt is the table's alone; anything else ends the worker.
    fn absorb(&mut self, r: Result<(), AccumulatorError>) -> Result<(), WorkerError> {
        match r {
            Ok(()) => Ok(()),
            Err(AccumulatorError::Halt { table, reason }) => self.halt(table, reason),
            Err(e) => Err(e.into()),
        }
    }

    /// Stops the table. Its committed changes are flushed first (the owner's ruling on the final
    /// review's M1), so the halt holds from that flush's checkpoint and rides the next job: the
    /// writer runs jobs in order, so nothing waits. Then the open transaction's changes of it go,
    /// its later ones are dropped, its snapshot stops, and the halt is recorded before the next
    /// checkpoint. The sink holds its changes through the checkpoint.
    fn halt(&mut self, table: TableRef, reason: HaltReason) -> Result<(), WorkerError> {
        if self.acc.has_committed(&table) {
            self.flush()?;
        }
        self.acc.halt_table(&table);
        self.schemas.remove(&table);
        self.copies.remove(&table);
        self.snaps.stop(&table);
        self.core.writers_waited.remove(&table);
        self.hold_ended();
        self.halts.push(TableHalt {
            table: table.clone(),
            reason: reason.clone(),
            at: self.acc.checkpoint().clone(),
        });
        self.core.update(|st| {
            st.snapshots.remove(&table);
            st.halted.insert(table, reason);
        });
        Ok(())
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
        self.handed += 1;
        if let Some(since) = self.unflushed_since.take() {
            self.unsynced.push_back((self.handed, since));
        }
        // A slice of a large transaction leaves the rest of it to wait.
        self.unflushed_since = self.open_since;
        let job = Job {
            change_sets,
            checkpoint: checkpoint.clone(),
            halts: std::mem::take(&mut self.halts),
            snapshots: self.snaps.take_docs(self.handed),
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

    /// Waits until every job handed on is durable.
    async fn until_durable(&mut self) -> Result<Option<End>, WorkerError> {
        while self.durable < self.handed {
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
    /// the sink, then halts it at that flush's checkpoint. In an open transaction the change
    /// applies where the stream met it: the committed rows flush, and the transaction goes on
    /// under the new schema.
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
                self.halt(table, reason)?;
                return Ok(None);
            }
        };
        if !safe.is_empty() {
            match self.core.evolver().evolve(&table, safe).await? {
                Ok(None) => {}
                Ok(Some(warning)) => self.core.update(|st| st.warn(warning)),
                Err(reason) => {
                    self.halt(table, reason)?;
                    return Ok(None);
                }
            }
            self.core.count_soft_dropped(&now).await;
        }
        let extra = self.core.settings.delete_extra(&now);
        loop {
            match self.acc.set_schema(now.clone(), &extra) {
                Err(AccumulatorError::FlushRequired { .. }) => self.flush()?,
                Err(AccumulatorError::Halt { table, reason }) => {
                    self.halt(table, reason)?;
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
    use crate::test_support::{World, Yaml, pipeline, row, schema, t, wait_until};

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

    /// The fake source's first commit time.
    const FIRST_COMMIT: i64 = 1_700_000_000_000_000;

    /// The lag starts at the oldest transaction read and not yet durable: it stays at the first
    /// one while the policy holds the flush back, and clears once the flush is durable. The rows
    /// read count by operation, and the buffer holds the unflushed changes' bytes until the
    /// flush.
    #[tokio::test]
    async fn the_status_shows_the_oldest_unsynced_change_the_rows_read_and_the_buffer() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let held = pipeline("", "flush: { seconds: 600 }", "");
        let (end, _) = session(&w, &held, &["a"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            w.source.commit(
                FakeTxn::new()
                    .update(&t("a"), row(1, "y"))
                    .insert(&t("a"), row(2, "x")),
            );
            w.source.commit(
                FakeTxn::new().delete(&t("a"), Row(vec![("id".into(), ColumnValue::Int64(2))])),
            );
            let end = w.source.wal_end();
            wait_until("everything is read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            let st = status.borrow().clone();
            assert_eq!(st.unsynced_since_micros, Some(FIRST_COMMIT));
            assert_eq!(
                st.rows_read,
                RowsRead {
                    insert: 2,
                    update: 1,
                    delete: 1,
                    snapshot: 0
                }
            );
            assert!(st.buffer_bytes > 0, "{st:?}");
        })
        .await;
        end.unwrap();
    }

    /// Once the flush is durable, nothing waits: no lag, no buffer.
    #[tokio::test]
    async fn a_durable_flush_clears_the_lag_and_the_buffer() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (end, st) = session(&w, &flush_each(), &["a"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!((st.unsynced_since_micros, st.buffer_bytes), (None, 0));
        assert_eq!(st.rows_read.insert, 1);
    }

    /// A column the source drops stays in the sink: the evolve's soft-drop is counted.
    #[tokio::test]
    async fn a_dropped_column_is_counted_soft_dropped() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (end, st) = session(&w, &flush_each(), &["a"], async |status| {
            w.source.alter(&t("a"), with_note("a"));
            w.source.commit(FakeTxn::new().insert(&t("a"), noted(1)));
            durable(&status, w.source.wal_end()).await;
            assert_eq!(status.borrow().soft_dropped.get(&t("a")), Some(&0));
            w.source.alter(&t("a"), schema("a"));
            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!(st.soft_dropped.get(&t("a")), Some(&1), "{st:?}");
    }

    /// A slice of a large transaction leaves the rest of it pending (D-M10.3): the slice is
    /// durable, the transaction still open, and the lag still starts at its commit time.
    #[tokio::test]
    async fn a_slice_of_a_large_transaction_leaves_the_rest_pending() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline(
            "",
            "flush: { seconds: 0 }, max_transaction_batch_rows: 2",
            "",
        );
        let (mut core, _control, status) = w.core(&p, &["a"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let mut txn = FakeTxn::new();
        for id in 1..=4 {
            txn = txn.insert(&t("a"), row(id, "x"));
        }
        w.source.commit(txn);
        while s.handed == 0 {
            let item = s.stream.next().await.unwrap();
            assert!(s.item(item).await.unwrap().is_none());
            s.due(Instant::now()).unwrap();
        }
        assert!(
            s.acc.in_transaction(),
            "the slice leaves the transaction open"
        );
        assert!(s.until_durable().await.unwrap().is_none());
        assert_eq!((s.handed, s.durable), (1, 1));
        assert!(s.acc.in_transaction());
        assert_eq!(status.borrow().unsynced_since_micros, Some(FIRST_COMMIT));
    }

    /// With two jobs queued, the first one durable clears only its own lag: the lag starts at
    /// the second job's oldest transaction (D-M10.3).
    #[tokio::test]
    async fn a_durable_job_leaves_the_next_job_s_lag() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (mut core, _control, status) = w.core(&flush_each(), &["a"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
        let first = w.source.wal_end();
        w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
        let second = w.source.wal_end();
        pump(&mut s, &first).await;
        s.flush().unwrap();
        pump(&mut s, &second).await;
        s.flush().unwrap();
        assert_eq!((s.handed, s.durable), (2, 0));
        // The writer's events wait in the channel: take the first job's `Durable` only.
        let ev = s.events.recv().await;
        assert!(s.writer_event(ev).await.unwrap().is_none());
        assert_eq!(s.durable, 1);
        assert_eq!(
            status.borrow().unsynced_since_micros,
            Some(FIRST_COMMIT + 1_000),
            "the second transaction's commit time, a thousand microseconds on"
        );
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
        assert_eq!(
            w.source.acknowledged().last(),
            Some(&w.source.wal_end()),
            "the drain's last flush is acknowledged before the stream closes"
        );
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
            // A slice waits in the sink and the next for room in the buffer: the transaction is
            // open.
            wait_until("a slice waits in the sink", || {
                w.sink.waiting_commits() == 1
            })
            .await;
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

    /// A pause arrives with a transaction open (a slice in the sink, the next waiting for room):
    /// the session finishes the transaction and makes it durable, then reads nothing until it is
    /// resumed.
    #[tokio::test]
    async fn a_pause_finishes_the_open_transaction_then_reads_nothing_until_resumed() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline(
            "",
            "flush: { seconds: 0 }, max_transaction_batch_rows: 2, buffer_capacity_rows: 1",
            "",
        );
        let (mut core, control, status) = w.core(&p, &["a"]);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        w.sink.hold_commits();
        let mut txn = FakeTxn::new();
        for id in 1..=6 {
            txn = txn.insert(&t("a"), row(id, "x"));
        }
        w.source.commit(txn);
        let first = w.source.wal_end();
        let driver = async {
            wait_until("a slice waits in the sink", || {
                w.sink.waiting_commits() == 1
            })
            .await;
            control.send(Command::Pause).await.unwrap();
            w.sink.release_commits();
            wait_until("paused", || status.borrow().phase == Phase::Paused).await;
            assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6], "the transaction is in");
            assert_eq!(status.borrow().checkpoint.as_ref(), Some(&first));
            w.source.commit(FakeTxn::new().insert(&t("a"), row(7, "x")));
            let second = w.source.wal_end();
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(
                status.borrow().read.as_ref(),
                Some(&first),
                "nothing is read while paused"
            );
            control.send(Command::Resume).await.unwrap();
            durable(&status, second).await;
            assert_eq!(status.borrow().phase, Phase::Streaming);
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6, 7]);
    }

    /// A paused session waits on its timers and nothing else: ten seconds pass on the paused
    /// clock, which advances only while every task is idle. A session that spins keeps it still,
    /// and a real two-second watchdog fails the test.
    #[tokio::test(start_paused = true)]
    async fn a_paused_session_idles() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (mut core, control, status) = w.core(&flush_each(), &["a"]);
        core.pause = true;
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let driver = async {
            wait_until("parked", || status.borrow().phase == Phase::Paused).await;
            let (spun, spinning) = tokio::sync::oneshot::channel::<()>();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(2));
                let _ = spun.send(());
            });
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(10)) => {}
                _ = spinning => panic!("the paused session spins: the paused clock stood still"),
            }
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
    }

    #[tokio::test]
    async fn a_drain_while_paused_ends_at_once() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (end, last) = session(&w, &flush_each(), &["a"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            let first = w.source.wal_end();
            durable(&status, first).await;
        })
        .await;
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        let _ = last;
        let (mut core, control, status) = w.core(&flush_each(), &["a"]);
        core.pause = true;
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let driver = async {
            wait_until("a session that opens paused parks", || {
                status.borrow().phase == Phase::Paused
            })
            .await;
            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!(w.ids("a"), [1], "the paused session read nothing more");
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

    /// A halt mid-transaction (MySQL: a rows event whose later row can't be decoded) drops the
    /// table's rows of that transaction, before the halt and after it; its committed rows stay
    /// and the transaction's other tables land.
    #[tokio::test]
    async fn a_halt_mid_transaction_drops_the_tables_rows_of_it() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            let mut bad = row(3, "x");
            bad.0.push(("zzz".into(), ColumnValue::Int64(1)));
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(2, "before"))
                    .insert(&t("b"), row(1, "y"))
                    .insert(&t("a"), bad)
                    .insert(&t("a"), row(4, "after"))
                    .insert(&t("b"), row(2, "y")),
            );
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!((w.ids("a"), w.ids("b")), (vec![1], vec![1, 2]));
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1);
        assert_eq!(halts[0].table, t("a"));
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

    /// `public.<name>`: `id`, `doc` and `v text`, which an UPDATE changes while `doc` stays
    /// unchanged TOAST.
    fn with_v(name: &str) -> TableSchema {
        let mut columns = crate::test_support::schema(name).columns().to_vec();
        columns.push(ColumnSchema::new("v", V::Text, "text"));
        TableSchema::new(t(name), columns, vec!["id".into()]).unwrap()
    }

    fn v_row(id: i64, doc: &str, v: &str) -> Row {
        let mut r = row(id, doc);
        r.0.push(("v".into(), ColumnValue::Text(v.into())));
        r
    }

    /// An UPDATE of `v` that leaves `doc` unchanged.
    fn toasted(id: i64, v: &str) -> Row {
        Row(vec![
            ("id".into(), ColumnValue::Int64(id)),
            ("doc".into(), ColumnValue::Unchanged),
            ("v".into(), ColumnValue::Text(v.into())),
        ])
    }

    fn re_querying() -> data_avalanche_config::Pipeline {
        Yaml {
            source: "\n      toast_strategy: re_query",
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build()
    }

    /// `toast_strategy: re_query` with the fetch failing transiently `failures` times: the sink
    /// has `doc` `old`, the source `new`. Three attempts read it. After them the UPDATE commits
    /// masked: `v` lands, `doc` keeps `old`, a warning names the table, and nothing halts.
    #[tokio::test]
    async fn re_query_retries_a_failed_fetch_then_keeps_the_value_masked() {
        for (failures, want) in [(2, "new"), (3, "old")] {
            let w = World::with(vec![with_v("a")]);
            w.started(&["a"]).await;
            let (end, st) = session(&w, &re_querying(), &["a"], async |status| {
                w.source
                    .commit(FakeTxn::new().insert(&t("a"), v_row(1, "old", "0")));
                durable(&status, w.source.wal_end()).await;
                w.source.load(&t("a"), vec![v_row(1, "new", "0")]);
                for _ in 0..failures {
                    w.source.fail(
                        Method::FetchRows,
                        SourceError::Transient("column doc is gone".into()),
                    );
                }
                w.source
                    .commit(FakeTxn::new().update(&t("a"), toasted(1, "1")));
                durable(&status, w.source.wal_end()).await;
            })
            .await;
            assert!(
                matches!(end.unwrap(), End::Exit(Exit::Drained)),
                "{failures} failures: the session goes on"
            );
            assert!(st.halted.is_empty(), "{failures} failures: {:?}", st.halted);
            assert_eq!(
                (w.text("a", "doc"), w.text("a", "v")),
                (vec![Some(want.to_owned())], vec![Some("1".to_owned())]),
                "{failures} failures: the UPDATE commits"
            );
            let warned: Vec<&String> = st
                .warnings
                .iter()
                .filter(|w| w.contains("public.a") && w.contains("column doc is gone"))
                .collect();
            assert_eq!(
                warned.len(),
                usize::from(failures >= 3),
                "{failures} failures: {:?}",
                st.warnings
            );
        }
    }

    /// A table whose re_query fetch keeps failing: the streak's first commit tries three times
    /// and warns; the next tries once, with no backoff, and warns nothing more. A fetch that
    /// succeeds ends the streak: the next failure tries three times and warns again, a warning
    /// the status already shows, once.
    #[tokio::test]
    async fn a_failing_re_query_fetch_warns_once_per_streak_and_tries_once_while_failing() {
        let w = World::with(vec![with_v("a")]);
        w.started(&["a"]).await;
        let (end, st) = session(&w, &re_querying(), &["a"], async |status| {
            w.source
                .commit(FakeTxn::new().insert(&t("a"), v_row(1, "old", "0")));
            durable(&status, w.source.wal_end()).await;
            w.source.load(&t("a"), vec![v_row(1, "new", "0")]);
            // Commits an UPDATE of `v` while the next `failures` fetches fail; returns the
            // fetches it took.
            let update = async |v: &str, failures: usize| {
                let before = w.source.fetches(&t("a"));
                for _ in 0..failures {
                    w.source.fail(
                        Method::FetchRows,
                        SourceError::Transient("column doc is gone".into()),
                    );
                }
                w.source
                    .commit(FakeTxn::new().update(&t("a"), toasted(1, v)));
                durable(&status, w.source.wal_end()).await;
                w.source.fetches(&t("a")) - before
            };
            assert_eq!(update("1", 3).await, 3, "a streak begins: three attempts");
            assert_eq!(update("2", 1).await, 1, "while it fails: one attempt");
            assert_eq!(status.borrow().warnings.len(), 1, "one warning per streak");
            assert_eq!(w.text("a", "doc"), [Some("old".to_owned())], "masked");
            assert_eq!(update("3", 0).await, 1);
            assert_eq!(
                w.text("a", "doc"),
                [Some("new".to_owned())],
                "the read succeeds"
            );
            assert_eq!(
                update("4", 3).await,
                3,
                "a new streak: three attempts again"
            );
        })
        .await;
        end.unwrap();
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        assert_eq!(st.warnings.len(), 1, "{:?}", st.warnings);
        assert_eq!(
            (w.text("a", "doc"), w.text("a", "v")),
            (vec![Some("new".to_owned())], vec![Some("4".to_owned())])
        );
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

    /// A failed read of the retained log waits for the next heartbeat. A transient one warns
    /// nothing; permanent ones on consecutive heartbeats warn once that the WAL-lag guard is
    /// blind, and the session keeps streaming. A read that succeeds ends the streak.
    #[tokio::test]
    async fn a_failing_wal_lag_read_warns_once_per_streak_and_keeps_streaming() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 0 }", "max_wal_lag_bytes: 64MiB");
        let (mut core, control, status) = w.core(&p, &["a"]);
        core.settings.heartbeat = Duration::from_millis(5);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let blind = || -> Vec<String> {
            status
                .borrow()
                .warnings
                .iter()
                .filter(|m| m.contains("the WAL-lag guard is blind"))
                .cloned()
                .collect()
        };
        let driver = async {
            w.source
                .fail(Method::Retained, SourceError::Transient("reset".into()));
            for _ in 0..2 {
                w.source.fail(
                    Method::Retained,
                    SourceError::Permanent("permission denied".into()),
                );
            }
            w.source.set_retained(Some(1024));
            wait_until("a read succeeds", || {
                status.borrow().retained_bytes == Some(1024)
            })
            .await;
            let first = blind();
            assert_eq!(first.len(), 1, "{:?}", status.borrow().warnings);
            assert!(first[0].contains("permission denied"), "{first:?}");
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            durable(&status, w.source.wal_end()).await;
            w.source.fail(
                Method::Retained,
                SourceError::Permanent("permission denied again".into()),
            );
            wait_until("a new streak warns", || blind().len() == 2).await;
            assert!(blind()[1].contains("permission denied again"));
            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            durable(&status, w.source.wal_end()).await;
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!(w.ids("a"), [1, 2]);
    }

    /// A source that stops answering (a frozen server, DA-SRC-16): the stream is silent and the
    /// heartbeat's reads time out, so the session ends as a transient source error and the
    /// worker reconnects.
    #[tokio::test]
    async fn a_silent_stream_whose_source_doesn_t_answer_ends_the_session() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 0 }", "");
        let (mut core, _control, _status) = w.core(&p, &["a"]);
        core.settings.heartbeat = Duration::from_millis(5);
        for _ in 0..10 {
            w.source.fail(
                Method::Retained,
                SourceError::Transient("measure the slot's WAL timed out after 30s".into()),
            );
        }
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let end = s.run().await.unwrap();
        let End::Source(SourceError::Transient(why)) = end else {
            panic!("{end:?}")
        };
        assert!(why.starts_with("the source isn't answering"), "{why}");
        assert!(why.contains("timed out after 30s"), "{why}");
    }

    /// The server dropped the stream without the client hearing of it: the slot has no reader.
    #[tokio::test]
    async fn a_silent_stream_on_a_slot_no_stream_reads_ends_the_session() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 0 }", "");
        let (mut core, _control, _status) = w.core(&p, &["a"]);
        core.settings.heartbeat = Duration::from_millis(5);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        w.source.set_retained(Some(0));
        w.source.detach_stream();
        let end = s.run().await.unwrap();
        let End::Source(SourceError::Transient(why)) = end else {
            panic!("{end:?}")
        };
        assert!(why.contains("no stream reads the slot"), "{why}");
    }

    /// A paused session reads no stream, so a silent stream proves nothing about the source:
    /// the watchdog leaves it be, though the slot has no reader (Review Focus 3).
    #[tokio::test(start_paused = true)]
    async fn a_paused_session_outlives_a_source_that_doesn_t_answer() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (mut core, control, status) = w.core(&flush_each(), &["a"]);
        core.settings.heartbeat = Duration::from_millis(5);
        core.pause = true;
        let s = Session::open(&mut core).await.unwrap().unwrap();
        w.source.set_retained(Some(0));
        w.source.detach_stream();
        let driver = async {
            wait_until("parked", || status.borrow().phase == Phase::Paused).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        let end = end.unwrap();
        assert!(matches!(end, End::Exit(Exit::Drained)), "{end:?}");
    }

    /// A heartbeat read that never comes back holds up neither the stream nor a drain, and
    /// counts against nothing: it may wait behind a chunk read on a shared connection.
    #[tokio::test]
    async fn a_stalled_heartbeat_read_holds_up_nothing() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 0 }", "");
        let (mut core, control, status) = w.core(&p, &["a"]);
        core.settings.heartbeat = Duration::from_millis(5);
        w.source.stall_retained();
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let driver = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            durable(&status, w.source.wal_end()).await;
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!(w.ids("a"), [1]);
    }

    /// The blind warning names what is blind: the bound when `max_wal_lag_bytes` is set, else the
    /// reported lag.
    #[tokio::test]
    async fn the_blind_warning_names_what_is_blind() {
        for (postgres, tail) in [
            (
                "max_wal_lag_bytes: 64MiB",
                "max_wal_lag_bytes isn't enforced until a read succeeds",
            ),
            ("", "the retained WAL isn't reported until a read succeeds"),
        ] {
            let w = World::new(&["a"]);
            w.started(&["a"]).await;
            let p = pipeline("", "flush: { seconds: 0 }", postgres);
            let (mut core, _control, status) = w.core(&p, &["a"]);
            let mut s = Session::open(&mut core).await.unwrap().unwrap();
            s.wal_lag(Ok(Err(SourceError::Permanent("permission denied".into()))))
                .unwrap();
            let warnings = status.borrow().warnings.clone();
            assert!(
                matches!(&warnings[..], [m] if m.starts_with("the WAL-lag guard is blind") && m.ends_with(tail)),
                "{postgres:?}: {warnings:?}"
            );
        }
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

    /// [`with_note`] and `extra text`.
    fn with_extra(name: &str) -> TableSchema {
        let mut columns = with_note(name).columns().to_vec();
        columns.push(ColumnSchema::new("extra", V::Text, "text"));
        TableSchema::new(t(name), columns, vec!["id".into()]).unwrap()
    }

    /// Channel D (Q-M6a-4): a rescan finds `a` and the idle `b` with `note`. Before the stream
    /// passes the position read after it, the stream applies a newer shape of `a`, `note` and
    /// `extra`. The catch-up skips `a`: its finding is older than the cache, and would drop
    /// `extra`. It applies `b`'s.
    #[tokio::test]
    async fn a_catch_up_skips_a_table_whose_cache_moved_since_the_rescan() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let (mut core, control, status) = w.core(&flush_each(), &["a", "b"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        w.source.alter(&t("a"), with_note("a"));
        w.source.alter(&t("b"), with_note("b"));
        w.source.after_next_rescan(|src| {
            src.alter(&t("a"), with_extra("a"));
            let mut r = noted(1);
            r.0.push(("extra".into(), ColumnValue::Text("e".into())));
            src.commit(FakeTxn::new().insert(&t("a"), r));
        });
        assert!(s.rescan().await.unwrap().is_none());
        assert!(s.rescan.is_some(), "the finding waits for the stream");
        let driver = async {
            durable(&status, w.source.wal_end()).await;
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        let st = status.borrow().clone();
        assert!(st.halted.is_empty() && st.warnings.is_empty(), "{st:?}");
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("a")), "no column dropped");
        assert_eq!(w.ids("a"), [1]);
        let cached = w.docs.table_schema(&t("b")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("b")), "the catch-up ran");
    }

    /// `public.<name>`: `id`, `doc` and `extra text`.
    fn extra_only(name: &str) -> TableSchema {
        let mut columns = crate::test_support::schema(name).columns().to_vec();
        columns.push(ColumnSchema::new("extra", V::Text, "text"));
        TableSchema::new(t(name), columns, vec!["id".into()]).unwrap()
    }

    /// Reads the stream into `s` until its frontier is `to`.
    async fn pump(s: &mut Session<'_>, to: &SourcePosition) {
        while s.acc.frontier() != to {
            let item = s.stream.next().await.unwrap();
            assert!(s.item(item).await.unwrap().is_none());
        }
    }

    /// `name`'s cached schema document once every job handed on is durable.
    async fn persisted(s: &mut Session<'_>, w: &World, name: &str) -> TableSchema {
        assert!(s.until_durable().await.unwrap().is_none());
        w.docs.table_schema(&t(name)).await.unwrap().unwrap().doc
    }

    /// Asserts that nothing halted and `a`'s schema is the catalog's, in memory and cached.
    async fn current(s: &mut Session<'_>, w: &World) {
        let st = s.core.status.borrow().clone();
        assert!(st.halted.is_empty(), "{st:?}");
        assert_eq!(s.schemas[&t("a")], w.source.schema(&t("a")));
        assert_eq!(persisted(s, w, "a").await, w.source.schema(&t("a")));
    }

    /// Channel D, change and revert (ABA): a rescan finds `a` with `note`. Before the catch-up
    /// the stream moves the cache to `note` and back (a DROP COLUMN), and the catalog moves on to
    /// `note` and `extra`. The cache equals its copy from the rescan, but the finding is stale:
    /// the catch-up reads `a` again and skips it, and the next round applies the current shape.
    #[tokio::test]
    async fn a_catch_up_skips_a_finding_the_cache_moved_away_from_and_back() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (mut core, _control, _status) = w.core(&flush_each(), &["a"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let base = s.schemas[&t("a")].clone();
        w.source.alter(&t("a"), with_note("a"));
        w.source.after_next_rescan(|src| {
            src.commit(FakeTxn::new().insert(&t("a"), noted(1)));
            src.alter(&t("a"), crate::test_support::schema("a"));
            src.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            src.alter(&t("a"), with_extra("a"));
        });
        assert!(s.rescan().await.unwrap().is_none());
        pump(&mut s, &w.source.wal_end()).await;
        assert!(s.rescan.is_none(), "the catch-up ran");
        assert_eq!(
            s.schemas[&t("a")],
            base,
            "the cache came back, and nothing applied"
        );
        assert_eq!(persisted(&mut s, &w, "a").await, base);
        assert!(s.core.status.borrow().halted.is_empty());
        assert!(s.rescan().await.unwrap().is_none());
        current(&mut s, &w).await;
    }

    /// Channel D, the catalog moves after the rescan: a rescan finds `a` with `note` (an ADD
    /// COLUMN); before the catch-up `note` is dropped again and `extra` added, with nothing
    /// streamed. The finding is stale: the catch-up reads `a` again and skips it, and the next
    /// round applies the current shape.
    #[tokio::test]
    async fn a_catch_up_skips_a_finding_the_catalog_moved_on_from() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (mut core, _control, _status) = w.core(&flush_each(), &["a"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let base = s.schemas[&t("a")].clone();
        w.source.alter(&t("a"), with_note("a"));
        w.source
            .after_next_rescan(|src| src.alter(&t("a"), extra_only("a")));
        assert!(s.rescan().await.unwrap().is_none());
        assert!(
            s.rescan.is_none(),
            "the stream is at the position: the catch-up ran"
        );
        assert!(
            s.core.status.borrow().halted.is_empty(),
            "{:?}",
            s.core.status.borrow().halted
        );
        assert_eq!(s.schemas[&t("a")], base, "nothing applied");
        assert_eq!(persisted(&mut s, &w, "a").await, base, "nothing cached");
        assert!(s.rescan().await.unwrap().is_none());
        current(&mut s, &w).await;
        assert!(s.core.status.borrow().warnings.is_empty());
    }

    /// A catch-up's re-read fails: `a`, held for its snapshot, isn't applied and stays held; the
    /// failure warns once, and once that `a`'s snapshot waits; `b`, changed in the same round,
    /// isn't read after it. The next round applies both and releases `a`.
    #[tokio::test]
    async fn a_failed_re_read_ends_the_catch_up_and_the_next_round_applies_it() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.request(&["a"]).await;
        let (mut core, _control, _status) = w.core(&flush_each(), &["a", "b"]);
        core.drifted.insert(t("a"));
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let base = |name: &str| crate::test_support::schema(name).columns().to_vec();
        w.source.alter(&t("a"), with_note("a"));
        w.source.alter(&t("b"), with_note("b"));
        w.source.after_next_rescan(|src| {
            src.fail(
                Method::Rescan,
                SourceError::Transient("catalog down".into()),
            )
        });
        let before = w.source.rescans();
        assert!(s.rescan().await.unwrap().is_none());
        assert!(s.rescan.is_none(), "the catch-up ran");
        assert_eq!(
            w.source.rescans() - before,
            2,
            "the round and `a`'s re-read: `b` isn't read"
        );
        assert_eq!(s.schemas[&t("a")].columns(), base("a"), "not applied");
        assert_eq!(s.schemas[&t("b")].columns(), base("b"), "not applied");
        for name in ["a", "b"] {
            let cached = persisted(&mut s, &w, name).await;
            assert_eq!(cached.columns(), base(name), "{name}: not cached");
        }
        assert!(s.core.drifted.contains(&t("a")), "still held");
        let warnings = s.core.status.borrow().warnings.clone();
        assert!(
            matches!(&warnings[..], [d, h] if d.contains("catalog down")
                && h.contains("the snapshots of public.a wait") && h.contains("catalog down")),
            "channel D's warning and the held table's: {warnings:?}"
        );
        assert!(s.snaps.held_retry.is_some(), "the held table backs off");
        assert!(s.rescan().await.unwrap().is_none());
        assert!(s.snaps.held_retry.is_none(), "the streak ended");
        for name in ["a", "b"] {
            assert_eq!(s.schemas[&t(name)], w.source.schema(&t(name)));
            let cached = persisted(&mut s, &w, name).await;
            assert_eq!(cached, w.source.schema(&t(name)), "{name}: cached");
        }
        assert!(s.core.drifted.is_empty(), "released");
        assert!(s.core.status.borrow().halted.is_empty());
        assert_eq!(s.core.status.borrow().warnings, warnings);
    }

    /// A failed rescan round warns once per streak: the rescan failing twice warns once; a round
    /// that succeeds ends the streak, and the position read after a rescan failing warns again.
    /// A full round (a finding, its position, its catch-up) ends that streak too.
    #[tokio::test]
    async fn a_failing_rescan_warns_once_per_streak() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (mut core, _control, _status) = w.core(&flush_each(), &["a"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let warnings = |s: &Session<'_>| s.core.status.borrow().warnings.clone();
        for _ in 0..2 {
            w.source.fail(
                Method::Rescan,
                SourceError::Transient("catalog busy".into()),
            );
            assert!(s.rescan().await.unwrap().is_none());
        }
        let first = warnings(&s);
        assert_eq!(first.len(), 1, "{first:?}");
        assert!(
            first[0].contains("catalog busy")
                && first[0].contains("dropped-table detection is paused"),
            "{first:?}"
        );
        assert!(s.rescan().await.unwrap().is_none());
        assert_eq!(warnings(&s), first, "a round that succeeds warns nothing");
        w.source.alter(&t("a"), with_note("a"));
        w.source.fail(
            Method::CurrentPosition,
            SourceError::Transient("position unread".into()),
        );
        assert!(s.rescan().await.unwrap().is_none());
        let second = warnings(&s);
        assert_eq!(second.len(), 2, "a new streak: {second:?}");
        assert!(second[1].contains("position unread"), "{second:?}");
        assert!(s.rescan().await.unwrap().is_none());
        assert!(s.rescan.is_none() && s.schemas[&t("a")].column("note").is_some());
        assert_eq!(warnings(&s), second, "a full round succeeds");
        w.source.fail(
            Method::Rescan,
            SourceError::Transient("catalog busy again".into()),
        );
        assert!(s.rescan().await.unwrap().is_none());
        let third = warnings(&s);
        assert_eq!(third.len(), 3, "it ended the streak: {third:?}");
        assert!(third[2].contains("catalog busy again"), "{third:?}");
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

    /// The table's next evolve fails `Permanent`: an evolve would halt it `SINK_REJECTED`.
    fn evolve_fails(w: &World) {
        w.sink
            .fail(SinkOp::Evolve, Some(&t("a")), 1, || SinkError::Permanent {
                reason: "evolved".into(),
            });
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
        evolve_fails(&w);
        let cached = s.schemas[&t("a")].clone();
        let hint = SchemaHint::Columns(cached.columns().to_vec());
        assert!(s.schema_signal(t("a"), hint).await.unwrap().is_none());
        assert_eq!(s.submitted, submitted, "no flush");
        assert!(s.queued.is_empty());
        assert!(
            s.schemas.contains_key(&t("a")) && s.halts.is_empty(),
            "no evolve"
        );
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
        evolve_fails(&w);
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
        assert!(
            s.schemas.contains_key(&t("a")) && s.halts.is_empty(),
            "no evolve"
        );
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

    /// A crash once the rows before an unsafe change are durable, before the halt is recorded:
    /// the restart replays the change from the halt's `at` and halts the table there again.
    #[tokio::test]
    async fn a_crash_between_the_pre_halt_flush_and_the_halt_replays_from_at() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (mut core, _control, _status) = w.core(&p, &["a", "b"]);
        core.hooks = Arc::new(crate::test_support::CrashAt(|h| {
            matches!(h, crate::HookPoint::CheckpointPersisted { .. })
        }));
        let s = Session::open(&mut core).await.unwrap().unwrap();
        w.source.commit(
            FakeTxn::new()
                .insert(&t("a"), row(1, "x"))
                .insert(&t("a"), row(2, "x"))
                .insert(&t("b"), row(1, "y")),
        );
        let before = w.source.wal_end();
        w.source.alter(&t("a"), int_doc("a"));
        w.source.commit(
            FakeTxn::new()
                .insert(&t("a"), int_row(3))
                .insert(&t("b"), row(2, "y")),
        );
        let end = tokio::time::timeout(Duration::from_secs(5), s.run())
            .await
            .expect("the crash ends it");
        assert!(matches!(end, Err(WorkerError::Crashed(_))), "{end:?}");
        assert_eq!(checkpoint(&w).await, lsn_of(&before));
        assert!(w.docs.halts().await.unwrap().tables.is_empty());
        assert_eq!((w.ids("a"), w.sink.version(&t("a"))), (vec![1, 2], Some(1)));

        let (end, st) = session(&w, &p, &["a", "b"], async |status| {
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        assert!(matches!(
            st.halted.get(&t("a")),
            Some(HaltReason::UnsafeDdl { .. })
        ));
        let halts = w.docs.halts().await.unwrap().tables;
        assert!(
            matches!(&halts[..], [h] if h.table == t("a") && h.at == before),
            "{halts:?}"
        );
        assert_eq!(
            (w.ids("a"), w.sink.version(&t("a"))),
            (vec![1, 2], Some(1)),
            "the replay neither loses nor rewrites a row"
        );
        assert_eq!(w.ids("b"), [1, 2]);
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

    /// A row that halts its table (final review M1): the table's committed rows flush first, so
    /// the sink has them, and the halt holds from that flush's checkpoint.
    #[tokio::test]
    async fn a_data_halt_flushes_the_tables_committed_rows_first() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let mut before = None;
        let (end, st) = session(&w, &p, &["a", "b"], async |status| {
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(1, "x"))
                    .insert(&t("a"), row(2, "x"))
                    .insert(&t("b"), row(1, "y")),
            );
            before = Some(w.source.wal_end());
            let mut bad = row(3, "x");
            bad.0.push(("zzz".into(), ColumnValue::Int64(1)));
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("b"), row(2, "y"))
                    .insert(&t("a"), bad),
            );
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        let before = before.expect("driven");
        assert!(
            matches!(
                st.halted.get(&t("a")),
                Some(HaltReason::SchemaMismatch { .. })
            ),
            "{:?}",
            st.halted
        );
        assert_eq!(
            w.ids("a"),
            [1, 2],
            "the rows committed before the halt are in"
        );
        assert_eq!(w.ids("b"), [1, 2]);
        let halts = w.docs.halts().await.unwrap().tables;
        assert!(
            matches!(&halts[..], [h] if h.table == t("a") && h.at == before),
            "the halt holds from the flush's checkpoint: {halts:?}"
        );
    }

    /// The sink refuses the flush a data halt hands on first: the table keeps the sink's halt,
    /// with its reason and the `at` before the refused rows, not the data halt's.
    #[tokio::test]
    async fn a_sink_refusal_of_a_halts_flush_keeps_the_sinks_halt() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let start = w.source.wal_end();
        w.sink
            .fail(SinkOp::Commit, Some(&t("a")), 1, || SinkError::Permanent {
                reason: "refused".into(),
            });
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (end, st) = session(&w, &p, &["a", "b"], async |status| {
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(1, "x"))
                    .insert(&t("b"), row(1, "y")),
            );
            let mut bad = row(2, "x");
            bad.0.push(("zzz".into(), ColumnValue::Int64(1)));
            w.source.commit(FakeTxn::new().insert(&t("a"), bad));
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        let rejected = |r: &HaltReason| matches!(r, HaltReason::SinkRejected { .. });
        assert!(st.halted.get(&t("a")).is_some_and(rejected), "{st:?}");
        let halts = w.docs.halts().await.unwrap().tables;
        assert!(
            matches!(&halts[..], [h] if h.table == t("a") && rejected(&h.reason) && h.at == start),
            "{halts:?}"
        );
        assert_eq!((w.ids("a"), w.ids("b")), (vec![], vec![1]));
    }

    /// A table the stream halts (a value it can't decode; final review M1): its committed rows
    /// flush first, and the halt holds from that flush's checkpoint.
    #[tokio::test]
    async fn a_stream_halt_flushes_the_tables_committed_rows_first() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (mut core, _control, status) = w.core(&p, &["a", "b"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        w.source.commit(
            FakeTxn::new()
                .insert(&t("a"), row(1, "x"))
                .insert(&t("a"), row(2, "x"))
                .insert(&t("b"), row(1, "y")),
        );
        let before = w.source.wal_end();
        pump(&mut s, &before).await;
        let after = SourcePosition::Pg(Lsn(lsn_of(&before).0 + 0x10));
        let reason = HaltReason::SchemaMismatch {
            column: "doc".into(),
            detail: "a value the decoder can't read".into(),
        };
        let items = [
            StreamItem::Begin {
                xid: 900,
                commit_ts_micros: 1,
            },
            StreamItem::TableHalt {
                table: t("a"),
                seq: SourceSeq::Pg(lsn_of(&before)),
                reason: reason.clone(),
            },
            StreamItem::Commit(TransactionBoundary {
                xid: 900,
                commit_pos: after.clone(),
                commit_ts_micros: 1,
            }),
        ];
        for item in items {
            assert!(s.item(item).await.unwrap().is_none());
        }
        s.flush().unwrap();
        assert!(s.until_durable().await.unwrap().is_none());
        assert_eq!(
            w.ids("a"),
            [1, 2],
            "the rows committed before the halt are in"
        );
        assert_eq!(w.ids("b"), [1]);
        let halts = w.docs.halts().await.unwrap().tables;
        assert!(
            matches!(&halts[..], [h] if h.table == t("a") && h.at == before && h.reason == reason),
            "the halt holds from the flush's checkpoint: {halts:?}"
        );
        assert_eq!(status.borrow().halted.get(&t("a")), Some(&reason));
    }

    /// A stream `TableHalt` for a table already halted leaves its halt alone (re-review N1): the
    /// table halted on a row it couldn't take, the checkpoint moved on, then the stream halts it
    /// again (a table dropped and re-created). The reason and `at` stay the first halt's.
    #[tokio::test]
    async fn a_stream_halt_of_a_halted_table_keeps_the_first_halt() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let start = w.source.wal_end();
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (mut core, _control, status) = w.core(&p, &["a", "b"]);
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let mut bad = row(1, "x");
        bad.0.push(("zzz".into(), ColumnValue::Int64(1)));
        w.source.commit(FakeTxn::new().insert(&t("a"), bad));
        w.source.commit(FakeTxn::new().insert(&t("b"), row(1, "y")));
        let moved = w.source.wal_end();
        pump(&mut s, &moved).await;
        s.flush().unwrap();
        assert!(s.until_durable().await.unwrap().is_none());
        let first = w.docs.halts().await.unwrap().tables;
        assert!(
            matches!(&first[..], [h] if h.table == t("a") && h.at == start
                && matches!(h.reason, HaltReason::SchemaMismatch { .. })),
            "{first:?}"
        );
        let after = SourcePosition::Pg(Lsn(lsn_of(&moved).0 + 0x10));
        let items = [
            StreamItem::Begin {
                xid: 900,
                commit_ts_micros: 1,
            },
            StreamItem::TableHalt {
                table: t("a"),
                seq: SourceSeq::Pg(lsn_of(&moved)),
                reason: HaltReason::TableDropped,
            },
            StreamItem::Commit(TransactionBoundary {
                xid: 900,
                commit_pos: after,
                commit_ts_micros: 1,
            }),
        ];
        for item in items {
            assert!(s.item(item).await.unwrap().is_none());
        }
        s.flush().unwrap();
        assert!(s.until_durable().await.unwrap().is_none());
        assert_eq!(w.docs.halts().await.unwrap().tables, first);
        assert!(
            matches!(
                status.borrow().halted.get(&t("a")),
                Some(HaltReason::SchemaMismatch { .. })
            ),
            "{:?}",
            status.borrow().halted
        );
        assert_eq!(w.ids("b"), [1]);
    }

    /// A held table that stays held across rescan rounds warns once (final review I3), at its
    /// second round: each round's finding is stale by its catch-up, as the catalog keeps
    /// changing. A third round warns nothing more, and the round that applies the change
    /// releases it.
    #[tokio::test]
    async fn a_table_held_across_rounds_warns_once() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.request(&["a"]).await;
        let (mut core, _control, _status) = w.core(&flush_each(), &["a"]);
        core.drifted.insert(t("a"));
        let mut s = Session::open(&mut core).await.unwrap().unwrap();
        let held = |s: &Session<'_>| -> Vec<String> {
            s.core
                .status
                .borrow()
                .warnings
                .iter()
                .filter(|m| m.contains("is still held"))
                .cloned()
                .collect()
        };
        w.source.alter(&t("a"), with_note("a"));
        let rounds = [
            (1, extra_only("a")),
            (2, with_note("a")),
            (3, extra_only("a")),
        ];
        for (round, next) in rounds {
            w.source
                .after_next_rescan(move |src| src.alter(&t("a"), next));
            assert!(s.rescan().await.unwrap().is_none());
            assert!(s.rescan.is_none(), "round {round}: the catch-up ran");
            assert!(
                s.snaps.held().contains(&t("a")),
                "round {round}: the finding was stale"
            );
            assert_eq!(held(&s).len(), usize::from(round >= 2), "round {round}");
        }
        let warned = held(&s);
        assert!(
            matches!(&warned[..], [m] if m.contains("public.a") && m.contains("after 2 catalog rescans")),
            "{warned:?}"
        );
        assert!(s.rescan().await.unwrap().is_none());
        assert!(s.snaps.held().is_empty(), "released");
        current(&mut s, &w).await;
        assert_eq!(held(&s), warned, "one warning for the hold");
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
