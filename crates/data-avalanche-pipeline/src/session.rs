//! One stream session (Data-Flow Phases 2–5). It opens the stream at the durable checkpoint and
//! reads it into the accumulator. Each flush goes to the sink writer, and the stream is
//! acknowledged once the flush is durable. A session ends on a drain, on a source failure (the
//! worker decides what follows) or with an error.
//!
//! A session starts from durable state only: the checkpoint, the cached schemas and the recorded
//! halts. What it held in memory is lost when it ends, and the next session replays it from the
//! checkpoint.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use data_avalanche_core::{
    ChangeStream, DeltaClass, EvolveOutcome, HaltReason, SchemaDelta, SchemaHint, Sink, SinkError,
    SourceConnector, SourceError, SourcePosition, StreamItem, TableRef, TableSchema, classify,
};
use data_avalanche_state::{PipelineDocs, TableHalt};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinHandle;

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
    /// The tables Phase 0 tracks. A session streams each one unless it is halted.
    pub tracked: BTreeSet<TableRef>,
}

impl Core {
    pub fn update(&self, f: impl FnOnce(&mut WorkerStatus)) {
        self.status.send_modify(f);
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
        loop {
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
                cmd = self.core.control.recv() => self.command(cmd)?,
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
                _ = tick.tick() => self.due(Instant::now())?,
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
            StreamItem::Begin { xid, .. } => self.acc.begin(xid)?,
            StreamItem::Change(e) => {
                loop {
                    match self.acc.apply(&e, now) {
                        Err(AccumulatorError::SplitRequired) => self.split()?,
                        r => break self.absorb(r)?,
                    }
                }
                if self.acc.due(now) == Some(FlushReason::Split) {
                    self.split()?;
                }
            }
            StreamItem::Truncate { tables, .. } => loop {
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
                self.acc.commit(&b, now)?;
                let at = b.commit_pos;
                self.pending_schemas
                    .extend(self.txn_schemas.drain(..).map(|s| (at.clone(), s)));
                self.read();
                self.due(now)?;
            }
            StreamItem::KeepAlive(p) => {
                self.acc.keepalive(&p, now)?;
                self.read();
                self.due(now)?;
            }
        }
        Ok(None)
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
            snapshots: vec![],
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

    /// `Schema_Change…` §8.1 at the signal's position: the catalog read (channel C), the
    /// classification, then for a Safe change seal → flush → evolve, and the rebuild.
    async fn schema_signal(
        &mut self,
        table: TableRef,
        hint: SchemaHint,
    ) -> Result<Option<End>, WorkerError> {
        let Some(cached) = self.schemas.get(&table).cloned() else {
            return Ok(None);
        };
        let (delta, now) = match self.augment(&cached, &hint).await {
            Ok(found) => found,
            Err(reason) => {
                self.halt(table, reason);
                return Ok(None);
            }
        };
        let Some(now) = now else {
            self.halt(table, HaltReason::TableDropped);
            return Ok(None);
        };
        let mut safe = vec![];
        for (change, class) in classify(&cached, &delta) {
            match class {
                DeltaClass::Unsafe(reason) => {
                    self.halt(table, reason);
                    return Ok(None);
                }
                DeltaClass::Safe => safe.push(change),
                DeltaClass::Ignorable => {}
            }
        }
        if !safe.is_empty() {
            if !self.core.settings.evolution {
                self.halt(table, HaltReason::EvolutionDisabled);
                return Ok(None);
            }
            self.flush()?;
            if let Some(end) = self.until_durable().await? {
                return Ok(Some(end));
            }
            let delta = SchemaDelta {
                table: table.clone(),
                changes: safe,
            };
            match self.evolve(&delta).await? {
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

    /// Channel C, retried while it fails transiently; a failure halts the table (fail-safe).
    async fn augment(
        &mut self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), HaltReason> {
        let attempts = self.core.tuning.augment_attempts.max(1);
        let mut attempt = 0;
        loop {
            match self.core.source.augment(cached, hint).await {
                Ok(found) => return Ok(found),
                Err(SourceError::TableHalt { reason, .. }) => return Err(reason),
                Err(e) if e.is_transient() && attempt + 1 < attempts => {
                    tokio::time::sleep(self.core.tuning.source_backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(HaltReason::UnsafeDdl {
                        detail: format!("the catalog read for a schema change failed: {e}"),
                    });
                }
            }
        }
    }

    /// The sink's evolve, retried while it fails transiently. `Ok(Some(warning))` for a degraded
    /// apply; `Err` halts the table.
    async fn evolve(
        &mut self,
        delta: &SchemaDelta,
    ) -> Result<Result<Option<String>, HaltReason>, WorkerError> {
        let mut attempt = 0;
        loop {
            if !self.core.guard.may_commit() {
                return Err(WorkerError::LeaseLost);
            }
            let outcome = self.core.sink.evolve(&delta.table, delta).await;
            if attempt > 0 && outcome.is_ok() {
                self.core.update(|st| st.degraded = None);
            }
            match outcome {
                Ok(EvolveOutcome::Applied) => return Ok(Ok(None)),
                Ok(EvolveOutcome::Degraded { warning }) => {
                    return Ok(Ok(Some(format!("table {}: {warning}", delta.table))));
                }
                Ok(EvolveOutcome::Unsupported { reason }) => {
                    return Ok(Err(HaltReason::EvolutionUnsupported { detail: reason }));
                }
                Err(SinkError::Transient { reason }) => {
                    self.core.update(|st| {
                        st.degraded = Some(format!("table {}: {reason}", delta.table));
                    });
                    tokio::time::sleep(self.core.tuning.sink_backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(SinkError::Permanent { reason }) => {
                    return Ok(Err(HaltReason::SinkRejected { detail: reason }));
                }
                Err(e) => return Err(WorkerError::Sink(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{ColumnSchema, ColumnValue, ColumnValueType as V, Row};
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
    async fn an_unsafe_change_halts_the_table() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let int_doc = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Int64, "bigint"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        let (end, st) = session(&w, &flush_each(), &["a"], async |status| {
            w.source.alter(&t("a"), int_doc);
            w.source.commit(FakeTxn::new().insert(
                &t("a"),
                Row(vec![
                    ("id".into(), ColumnValue::Int64(1)),
                    ("doc".into(), ColumnValue::Int64(7)),
                ]),
            ));
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
