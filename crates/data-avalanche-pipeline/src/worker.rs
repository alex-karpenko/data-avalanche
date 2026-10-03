//! The pipeline worker (Data-Flow Phases 0–5): Phase 0 once, then stream sessions until a drain,
//! a finished dump or an error. Between sessions it decides what a source failure means: a
//! transient one reopens the stream after a backoff, a busy slot is retried for a bounded time,
//! and a lost position is replaced and every table snapshotted again.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use data_avalanche_config::{Pipeline, SnapshotMode};
use data_avalanche_core::{
    HaltReason, SchemaHint, Sink, SinkError, SinkKind, SnapshotReason, SourceConnector,
    SourceError, SourcePosition, StartPlan, TableRef, TableSchema, TableSpec,
};
use data_avalanche_state::{
    PipelineDocs, SlotDoc, SnapshotStatus, StateError, TableHalt, TableSnapshotDoc,
};
use data_avalanche_storage::StoreError;
use tokio::sync::{mpsc, watch};

use crate::decide::{Decision, Facts, Plan, decide};
use crate::schema_change::Planned;
use crate::session::{Core, End, Session};
use crate::settings::Settings;
use crate::{
    Command, CommitGuard, Exit, Hooks, NoHooks, Phase, Tuning, Unguarded, WorkerError, WorkerStatus,
};

/// What a worker runs against.
#[derive(Clone)]
pub struct WorkerDeps {
    pub source: Arc<dyn SourceConnector>,
    pub sink: Arc<dyn Sink>,
    /// The pipeline's state documents.
    pub docs: PipelineDocs,
    /// The lease epoch every commit carries (Clustering §19).
    pub owner_epoch: u64,
    pub guard: Arc<dyn CommitGuard>,
    pub hooks: Arc<dyn Hooks>,
}

impl std::fmt::Debug for WorkerDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerDeps")
            .field("owner_epoch", &self.owner_epoch)
            .finish_non_exhaustive()
    }
}

impl WorkerDeps {
    /// A worker no lease guards, with no test hooks.
    pub fn new(source: Arc<dyn SourceConnector>, sink: Arc<dyn Sink>, docs: PipelineDocs) -> Self {
        Self {
            source,
            sink,
            docs,
            owner_epoch: 1,
            guard: Arc::new(Unguarded),
            hooks: Arc::new(NoHooks),
        }
    }
}

/// The owner's side of a running worker.
#[derive(Debug, Clone)]
pub struct WorkerHandle {
    pub control: mpsc::Sender<Command>,
    pub status: watch::Receiver<WorkerStatus>,
}

/// One pipeline's worker.
pub struct PipelineWorker {
    core: Core,
}

impl std::fmt::Debug for PipelineWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineWorker")
            .field("pipeline", &self.core.settings.pipeline_id)
            .finish_non_exhaustive()
    }
}

impl PipelineWorker {
    pub fn new(pipeline: &Pipeline, deps: WorkerDeps, tuning: Tuning) -> (Self, WorkerHandle) {
        let (control, control_rx) = mpsc::channel(16);
        let (status_tx, status) = watch::channel(WorkerStatus::default());
        let core = Core {
            settings: Settings::new(pipeline),
            source: deps.source,
            sink: deps.sink,
            docs: deps.docs,
            owner_epoch: deps.owner_epoch,
            guard: deps.guard,
            hooks: deps.hooks,
            tuning,
            control: control_rx,
            status: status_tx,
            tracked: BTreeSet::new(),
            dump_only: false,
            snapshot_started: BTreeMap::new(),
        };
        (Self { core }, WorkerHandle { control, status })
    }

    /// Runs the pipeline until a [`Command::Drain`] (or the handle drops), until every `dump_*`
    /// table is done, or until an error stops it.
    ///
    /// # Errors
    /// [`WorkerError`]: the pipeline is refused, a permanent source or sink failure, a newer owner,
    /// a lost lease.
    pub async fn run(mut self) -> Result<Exit, WorkerError> {
        let core = &mut self.core;
        start(core, None).await?;
        let mut attempt = 0;
        let mut contended_since: Option<Instant> = None;
        loop {
            let opened = Instant::now();
            let failed = match Session::open(core).await {
                Ok(Ok(session)) => match session.run().await {
                    Ok(End::Exit(exit)) => return Ok(exit),
                    Ok(End::Source(e)) => e,
                    Err(e) if transient_state(&e) => SourceError::Transient(e.to_string()),
                    Err(e) => return Err(e),
                },
                Ok(Err(e)) => e,
                Err(e) if transient_state(&e) => SourceError::Transient(e.to_string()),
                Err(e) => return Err(e),
            };
            if opened.elapsed() >= core.tuning.source_backoff.max {
                attempt = 0;
            }
            match failed {
                SourceError::Transient(error) => {
                    contended_since = None;
                    core.update(|st| st.phase = Phase::Reconnecting { error });
                }
                SourceError::Contended(detail) => {
                    let since = *contended_since.get_or_insert_with(Instant::now);
                    let waited = since.elapsed();
                    if waited >= core.tuning.contended_window {
                        return Err(WorkerError::Contended { waited, detail });
                    }
                }
                SourceError::PositionUnreachable { position, detail } => {
                    contended_since = None;
                    let warning = format!(
                        "the source lost the stream position {position} ({detail}): the stream restarts at a new position and the tables are snapshotted again"
                    );
                    core.update(|st| st.warnings.push(warning));
                    let from = retry(&core.tuning, || core.source.reset_position()).await?;
                    start(core, Some(from)).await?;
                    attempt = 0;
                    continue;
                }
                e => return Err(WorkerError::Source(e)),
            }
            let delay = core.tuning.source_backoff.delay(attempt);
            attempt = attempt.saturating_add(1);
            if drained_while(core, delay).await {
                return Ok(Exit::Drained);
            }
        }
    }
}

/// A storage failure worth another session: the write or read may succeed next time.
fn transient_state(e: &WorkerError) -> bool {
    matches!(
        e,
        WorkerError::State(StateError::Store(
            StoreError::Transient { .. } | StoreError::Contention { .. }
        ))
    )
}

/// Waits `delay` between sessions; true when a drain arrives meanwhile (nothing is buffered).
async fn drained_while(core: &mut Core, delay: std::time::Duration) -> bool {
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            () = &mut sleep => return false,
            cmd = core.control.recv() => match cmd {
                Some(Command::Flush) => {}
                Some(Command::Drain) | None => return true,
            },
        }
    }
}

/// A source call, retried while it fails transiently.
async fn retry<T, F, Fut>(tuning: &Tuning, mut call: F) -> Result<T, WorkerError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SourceError>>,
{
    let mut attempt = 0;
    loop {
        match call().await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_transient() => {
                tokio::time::sleep(tuning.source_backoff.delay(attempt)).await;
                attempt += 1;
            }
            Err(e) => return Err(WorkerError::Source(e)),
        }
    }
}

/// A sink call, retried while it fails transiently.
async fn sink_retry<T, F, Fut>(tuning: &Tuning, mut call: F) -> Result<T, SinkError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SinkError>>,
{
    let mut attempt = 0;
    loop {
        match call().await {
            Err(e) if e.is_transient() => {
                tokio::time::sleep(tuning.sink_backoff.delay(attempt)).await;
                attempt += 1;
            }
            r => return r,
        }
    }
}

/// Data-Flow Phase 0, or its rerun after the stream position was lost mid-run (`lost` holds the
/// new position `reset_position` returned). Leaves the core tracking the tables to stream, with
/// every decision persisted: halts and snapshot requests first, then the checkpoint, then the
/// cached schemas.
pub(crate) async fn start(
    core: &mut Core,
    lost: Option<SourcePosition>,
) -> Result<(), WorkerError> {
    core.update(|st| st.phase = Phase::Starting);
    let s = &core.settings;
    if s.sink_kind == SinkKind::Iceberg {
        return Err(WorkerError::Refused(
            "the Iceberg sink ships in v2 (D-V1.4); other pipelines run".into(),
        ));
    }
    let Some(slot) = s.slot.clone() else {
        return Err(WorkerError::Refused(
            "this build streams from PostgreSQL only; the MySQL source arrives with M7".into(),
        ));
    };
    if core.sink.kind() != s.sink_kind {
        return Err(WorkerError::Refused(format!(
            "the sink given is {:?}, the config says {:?}",
            core.sink.kind(),
            s.sink_kind
        )));
    }
    let sink_type = match s.sink_kind {
        SinkKind::Delta => "delta",
        SinkKind::Iceberg => "iceberg",
    };
    match core.docs.open(&s.pipeline_id, sink_type).await {
        Ok(_) => {}
        Err(e @ (StateError::SinkTypeMismatch { .. } | StateError::PipelineIdMismatch { .. })) => {
            return Err(WorkerError::Refused(e.to_string()));
        }
        Err(e) => return Err(e.into()),
    }
    let source = Arc::clone(&core.source);
    let sink = Arc::clone(&core.sink);
    let tuning = core.tuning.clone();
    let preflight = retry(&tuning, || source.preflight()).await?;
    let resolved = retry(&tuning, || source.resolve_tables()).await?;
    let mut warnings = preflight.warnings;
    warnings.extend(resolved.warnings);
    warnings.extend(
        resolved
            .skipped
            .iter()
            .map(|(t, why)| format!("table {t} isn't replicated: {why}")),
    );
    for table in &resolved.tables {
        let partition_by = core.settings.partition_by(table.table());
        if let Some(why) = sink
            .capabilities()
            .refusal(core.settings.write_mode, &partition_by)
        {
            return Err(WorkerError::Refused(format!(
                "table {}: {why}",
                table.table()
            )));
        }
    }

    // The state as of the checkpoint.
    let cached: BTreeMap<TableRef, TableSchema> = core
        .docs
        .table_schemas()
        .await?
        .into_iter()
        .map(|t| (t.table().clone(), t))
        .collect();
    let recorded: BTreeSet<TableRef> = core
        .docs
        .halts()
        .await?
        .tables
        .into_iter()
        .map(|h| h.table)
        .collect();
    let slot_doc = core.docs.slot().await?.map(|v| v.doc);

    // New halts, recorded before the checkpoint below.
    let mut halts: Vec<(TableRef, HaltReason)> = resolved.halted.clone();
    let resolved_tables: BTreeMap<TableRef, TableSchema> = resolved
        .tables
        .into_iter()
        .map(|t| (t.table().clone(), t))
        .collect();
    let gone: Vec<TableSchema> = cached
        .values()
        .filter(|c| !resolved_tables.contains_key(c.table()) && !recorded.contains(c.table()))
        .cloned()
        .collect();
    if !gone.is_empty()
        && let Ok(report) = source.rescan(&gone).await
    {
        halts.extend(
            report
                .dropped
                .into_iter()
                .map(|t| (t, HaltReason::TableDropped)),
        );
    }
    for (table, now) in &resolved_tables {
        if let Some(c) = cached.get(table)
            && c.source_id().is_some()
            && c.source_id() != now.source_id()
        {
            halts.push((table.clone(), HaltReason::TableDropped));
        }
    }

    // The stream position: prepare, or the one a mid-run recovery got.
    let intent = slot_doc.as_ref().is_some_and(SlotDoc::is_intent);
    let checkpoint = slot_doc
        .as_ref()
        .map(|d| SourcePosition::Pg(d.confirmed_flush_lsn));
    let (plan, at, added) = match lost {
        Some(from) => (Plan::Fresh(SnapshotReason::PositionLost), from, vec![]),
        None => {
            if slot_doc.is_none()
                && let Some(name) = &core.settings.slot_intent
            {
                core.docs.save_slot(&SlotDoc::intent(name)).await?;
            }
            let refs: Vec<TableRef> = resolved_tables.keys().cloned().collect();
            let prepared = retry(&tuning, || source.prepare(checkpoint.as_ref(), &refs)).await?;
            warnings.extend(prepared.warnings);
            match prepared.plan {
                StartPlan::Resume => {
                    let Some(at) = checkpoint.clone() else {
                        return Err(WorkerError::Refused(
                            "the source resumes a stream the pipeline has no checkpoint for".into(),
                        ));
                    };
                    (Plan::Resume, at, prepared.added)
                }
                StartPlan::Snapshot { from, reason } => {
                    // A start cut short after the slot was made starts over as a first start.
                    let reason = if intent {
                        SnapshotReason::FirstStart
                    } else {
                        reason
                    };
                    (Plan::Fresh(reason), from, vec![])
                }
            }
        }
    };
    let gap = matches!(
        plan,
        Plan::Fresh(SnapshotReason::PositionLost | SnapshotReason::AdoptedOrphan)
    );

    // Each table: what this start does with it.
    let halted: BTreeSet<TableRef> = recorded
        .iter()
        .cloned()
        .chain(halts.iter().map(|(t, _)| t.clone()))
        .collect();
    let mut tracked = BTreeSet::new();
    let mut requests = vec![];
    let mut abandoned = vec![];
    let mut schemas = vec![];
    let mut dump_only = !resolved_tables.is_empty();
    for (table, now) in &resolved_tables {
        let mode = core.settings.mode(table);
        dump_only &= matches!(mode, SnapshotMode::DumpAlways | SnapshotMode::DumpInitial);
        if halted.contains(table) {
            tracked.insert(table.clone());
            continue;
        }
        let state = sink_retry(&tuning, || sink.table_state(table))
            .await
            .map_err(WorkerError::Sink)?;
        let doc = core.docs.table_snapshot(table).await?.map(|v| v.doc);
        let in_progress = doc
            .as_ref()
            .is_some_and(|d| d.status == SnapshotStatus::InProgress);
        let tracked_before = cached.contains_key(table);
        let facts = Facts {
            tracked_before,
            // After a gap, a snapshot starts over: rows before its cursor may have changed.
            in_progress: in_progress && !gap,
            sink_has_table: state.exists,
            added: added.contains(table),
        };
        let (decision, warning) = decide(mode, plan, facts);
        if let Some(w) = warning {
            warnings.push(format!("table {table}: {w}"));
        }
        match decision {
            Decision::Stop => continue,
            Decision::Stream if in_progress => abandoned.push(table.clone()),
            Decision::Snapshot { resume: false, .. } => requests.push(table.clone()),
            _ => {}
        }
        // The sink table: created when absent, baselined to the schema now after a gap in the
        // stream (`Schema_Change…` §8.2), else the cached schema stays until the stream replays.
        let partition_by = core.settings.partition_by(table);
        let spec = match TableSpec::new(now, partition_by) {
            Ok(spec) => spec,
            Err(reason) => {
                halts.push((table.clone(), reason));
                tracked.insert(table.clone());
                continue;
            }
        };
        let mut schema = now.clone();
        match cached.get(table) {
            Some(c)
                if state.exists
                    && matches!(plan, Plan::Fresh(_))
                    && c.columns() != now.columns() =>
            {
                let evolver = core.evolver();
                let hint = SchemaHint::Columns(now.columns().to_vec());
                match evolver.plan(c, &hint).await {
                    Err(reason) => {
                        halts.push((table.clone(), reason));
                        tracked.insert(table.clone());
                        continue;
                    }
                    Ok(Planned { now, safe }) => {
                        if !safe.is_empty() {
                            match evolver.evolve(table, safe).await? {
                                Ok(None) => {}
                                Ok(Some(w)) => warnings.push(w),
                                Err(reason) => {
                                    halts.push((table.clone(), reason));
                                    tracked.insert(table.clone());
                                    continue;
                                }
                            }
                        }
                        schema = now;
                    }
                }
            }
            Some(c) if matches!(plan, Plan::Resume) && c.columns() != now.columns() => {
                warnings.push(format!(
                    "table {table}: its schema changed while the pipeline was down; the stream replays the change in order"
                ));
                schema = c.clone();
            }
            Some(c) if matches!(plan, Plan::Resume) => schema = c.clone(),
            _ => {}
        }
        if !state.exists {
            match sink_retry(&tuning, || sink.ensure_table(&spec)).await {
                Ok(()) => {}
                Err(SinkError::Permanent { reason }) => {
                    halts.push((table.clone(), HaltReason::SinkRejected { detail: reason }));
                    tracked.insert(table.clone());
                    continue;
                }
                Err(e) => return Err(WorkerError::Sink(e)),
            }
        }
        if !tracked_before || matches!(plan, Plan::Fresh(_)) {
            schemas.push(schema);
        }
        tracked.insert(table.clone());
    }

    // Persist, in this order: a restart in between redoes what follows.
    for (table, reason) in &halts {
        core.docs
            .record_halt(&TableHalt {
                table: table.clone(),
                reason: reason.clone(),
                at: at.clone(),
            })
            .await?;
    }
    for table in &requests {
        core.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: table.clone(),
                status: SnapshotStatus::InProgress,
                start_position: Some(at.clone()),
                cursor: None,
                row_count: None,
                completed_at: None,
            })
            .await?;
    }
    for table in &abandoned {
        core.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: table.clone(),
                status: SnapshotStatus::None,
                start_position: None,
                cursor: None,
                row_count: None,
                completed_at: None,
            })
            .await?;
    }
    if !matches!(plan, Plan::Resume) {
        let SourcePosition::Pg(lsn) = at else {
            return Err(WorkerError::Refused(format!(
                "the source started at {at}, not a PostgreSQL position"
            )));
        };
        core.docs
            .save_slot(&SlotDoc {
                slot_name: slot,
                confirmed_flush_lsn: lsn,
            })
            .await?;
    }
    for schema in &schemas {
        core.docs.save_table_schema(schema).await?;
    }
    core.tracked = tracked;
    core.dump_only = dump_only;
    core.update(|st| st.warnings.extend(warnings));
    Ok(())
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, Row};
    use data_avalanche_testkit::{FakeTxn, Method};

    use super::*;
    use crate::HookPoint;
    use crate::test_support::{World, Yaml, fast, row, t, wait_until};

    fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
        ids.map(|id| row(id, "x")).collect()
    }

    fn mode(tables: &str) -> Pipeline {
        let tables = format!("\n      snapshots: {{ {tables} }}");
        Yaml {
            tables: &tables,
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build()
    }

    /// Runs a worker over `w` until `drive` returns, then drains it.
    async fn run<F: Future<Output = ()>>(
        w: &World,
        p: &Pipeline,
        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
    ) -> (Result<Exit, WorkerError>, WorkerStatus) {
        let deps = WorkerDeps::new(
            Arc::new(w.source.clone()),
            Arc::new(w.sink.clone()),
            w.docs.clone(),
        );
        let (worker, handle) = PipelineWorker::new(p, deps, fast());
        let status = handle.status.clone();
        let driver = async move {
            drive(handle.status.clone()).await;
            let _ = handle.control.send(Command::Drain).await;
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        let last = status.borrow().clone();
        (end, last)
    }

    async fn streamed(status: &watch::Receiver<WorkerStatus>, w: &World) {
        let end = w.source.wal_end();
        wait_until("the log's end is durable", || {
            let st = status.borrow();
            st.checkpoint.as_ref() == Some(&end) && st.snapshots.is_empty()
        })
        .await;
    }

    async fn slot_lsn(w: &World) -> Lsn {
        w.docs
            .slot()
            .await
            .unwrap()
            .unwrap()
            .doc
            .confirmed_flush_lsn
    }

    #[tokio::test]
    async fn a_first_start_snapshots_and_streams() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=3));
        let (end, st) = run(&w, &mode("mode: initial"), async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(9, "s")));
            streamed(&status, &w).await;
        })
        .await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert_eq!(w.ids("a"), [1, 2, 3, 9]);
        assert_eq!(w.source.prepares(), [None]);
        let SourcePosition::Pg(end) = w.source.wal_end() else {
            unreachable!()
        };
        assert_eq!(slot_lsn(&w).await, end);
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("a")));
        let doc = w.docs.table_snapshot(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(doc.status, SnapshotStatus::Completed);
        assert!(st.warnings.is_empty(), "{:?}", st.warnings);
    }

    #[tokio::test]
    async fn a_restart_resumes_from_its_checkpoint() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=2));
        let p = mode("mode: initial");
        run(&w, &p, async |status| streamed(&status, &w).await)
            .await
            .0
            .unwrap();
        let reads = w.source.reads(&t("a"));
        let checkpoint = w.source.wal_end();
        w.source
            .commit(FakeTxn::new().insert(&t("a"), row(5, "while down")));
        let (end, _) = run(&w, &p, async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.source.prepares().last(), Some(&Some(checkpoint)));
        assert_eq!(w.source.reads(&t("a")), reads, "no second snapshot");
        assert_eq!(w.ids("a"), [1, 2, 5]);
    }

    #[tokio::test]
    async fn a_first_start_cut_short_after_the_slot_was_made_starts_over() {
        let w = World::new(&["a"]);
        w.docs.save_slot(&SlotDoc::intent("p_slot")).await.unwrap();
        w.source.prepare(None, &[]).await.unwrap();
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(
            w.source.prepares().last(),
            Some(&Some(SourcePosition::Pg(Lsn(0)))),
            "the intent asks for a new slot"
        );
        assert!(
            st.warnings.is_empty(),
            "a first start, not a gap: {:?}",
            st.warnings
        );
        assert!(!w.docs.slot().await.unwrap().unwrap().doc.is_intent());
    }

    #[tokio::test]
    async fn a_lost_slot_snapshots_again_and_never_warns() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.source.load(&t("b"), rows(1..=2));
        w.source.lose_slot();
        let p = mode("mode: initial, overrides: { public.b: never }");
        let (end, st) = run(&w, &p, async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2]);
        assert_eq!(w.source.reads(&t("b")), 0);
        assert_eq!(st.warnings.len(), 1, "{:?}", st.warnings);
        assert!(st.warnings[0].contains("public.b") && st.warnings[0].contains("lost"));
    }

    #[tokio::test]
    async fn a_dump_initial_table_the_sink_has_stops_at_once() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        let deps = WorkerDeps::new(
            Arc::new(w.source.clone()),
            Arc::new(w.sink.clone()),
            w.docs.clone(),
        );
        let (worker, _handle) = PipelineWorker::new(&mode("mode: dump_initial"), deps, fast());
        assert_eq!(worker.run().await.unwrap(), Exit::Dumped);
        assert_eq!(w.source.reads(&t("a")), 0);
        assert!(w.source.opened().len() <= 1);
    }

    #[tokio::test]
    async fn a_dump_always_table_dumps_and_ends() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=3));
        let deps = WorkerDeps::new(
            Arc::new(w.source.clone()),
            Arc::new(w.sink.clone()),
            w.docs.clone(),
        );
        let (worker, _handle) = PipelineWorker::new(&mode("mode: dump_always"), deps, fast());
        assert_eq!(worker.run().await.unwrap(), Exit::Dumped);
        assert_eq!(w.ids("a"), [1, 2, 3]);
    }

    #[tokio::test]
    async fn a_state_of_another_sink_type_is_refused() {
        let w = World::new(&["a"]);
        w.docs.open("p", "iceberg").await.unwrap();
        let (end, _) = run(&w, &mode("mode: never"), async |_| {}).await;
        match end {
            Err(WorkerError::Refused(why)) => assert!(why.contains("sink type"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn an_iceberg_pipeline_is_refused_in_v1() {
        let w = World::new(&["a"]);
        let (mut core, _control, _status) = w.core(&mode("mode: never"), &[]);
        core.settings.sink_kind = SinkKind::Iceberg;
        match start(&mut core, None).await {
            Err(WorkerError::Refused(why)) => assert!(why.contains("v2"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_table_dropped_while_down_halts() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.source.drop_table(&t("b"));
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(st.halted.get(&t("b")), Some(&HaltReason::TableDropped));
        assert_eq!(w.docs.halts().await.unwrap().tables[0].table, t("b"));
    }

    #[tokio::test]
    async fn a_table_created_again_while_down_halts() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.drop_table(&t("a"));
        w.source.create_table(crate::test_support::schema("a"));
        let (end, st) = run(&w, &mode("mode: never"), async |_| {}).await;
        end.unwrap();
        assert_eq!(st.halted.get(&t("a")), Some(&HaltReason::TableDropped));
    }

    #[tokio::test]
    async fn a_transient_failure_reopens_the_stream() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (end, _) = run(&w, &mode("mode: never"), async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            streamed(&status, &w).await;
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2]);
        assert_eq!(w.source.opened().len(), 2);
    }

    #[tokio::test]
    async fn a_busy_slot_is_retried_for_a_while() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        for _ in 0..3 {
            w.source
                .fail(Method::OpenStream, SourceError::Contended("active".into()));
        }
        let (end, _) = run(&w, &mode("mode: never"), async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1]);

        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        for _ in 0..10_000 {
            w.source
                .fail(Method::OpenStream, SourceError::Contended("active".into()));
        }
        let (end, _) = run(&w, &mode("mode: never"), async |_| {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        })
        .await;
        assert!(matches!(end, Err(WorkerError::Contended { .. })), "{end:?}");
    }

    #[tokio::test]
    async fn a_position_lost_while_running_is_replaced_and_snapshotted() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=2));
        let (end, st) = run(&w, &mode("mode: initial"), async |status| {
            streamed(&status, &w).await;
            let reads = w.source.reads(&t("a"));
            w.source.lose_slot();
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(7, "lost")));
            wait_until("the table is snapshotted again", || {
                w.source.reads(&t("a")) > reads
            })
            .await;
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(
            w.ids("a"),
            [1, 2, 7],
            "the snapshot brings back what the stream lost"
        );
        assert!(
            st.warnings
                .iter()
                .any(|w| w.contains("lost the stream position"))
        );
    }

    #[tokio::test]
    async fn a_lost_position_baselines_a_drifted_table() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
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
        w.source.alter(&t("a"), noted);
        w.source.lose_slot();
        let (end, _) = run(&w, &mode("mode: initial"), async |status| {
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        let state = w.sink.table_state(&t("a")).await.unwrap();
        assert!(state.schema.unwrap().field_with_name("note").is_ok());
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert!(cached.column("note").is_some());
    }

    /// Runs a worker with `deps` until `drive` returns, then drains it.
    async fn run_with<F: Future<Output = ()>>(
        p: &Pipeline,
        deps: WorkerDeps,
        drive: impl FnOnce(WorkerHandle) -> F,
    ) -> Result<Exit, WorkerError> {
        let (worker, handle) = PipelineWorker::new(p, deps, fast());
        let control = handle.control.clone();
        let driver = async move {
            drive(handle).await;
            let _ = control.send(Command::Drain).await;
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        end
    }

    fn deps(w: &World) -> WorkerDeps {
        WorkerDeps::new(
            Arc::new(w.source.clone()),
            Arc::new(w.sink.clone()),
            w.docs.clone(),
        )
    }

    /// The source's rows of `table`, as the sink's `id`s and `doc`s.
    fn source_rows(w: &World, table: &str) -> (Vec<i64>, Vec<Option<String>>) {
        let rows = w.source.rows(&t(table));
        let ids = rows
            .iter()
            .map(|r| match r.get("id") {
                Some(data_avalanche_core::ColumnValue::Int64(i)) => *i,
                other => panic!("{other:?}"),
            })
            .collect();
        let docs = rows
            .iter()
            .map(|r| match r.get("doc") {
                Some(data_avalanche_core::ColumnValue::Text(d)) => Some(d.clone()),
                _ => None,
            })
            .collect();
        (ids, docs)
    }

    /// A crash at each point leaves state a restart finishes from: the sink ends equal to the
    /// source.
    #[tokio::test]
    async fn a_restart_after_a_crash_at_any_point_converges() {
        type At = fn(&HookPoint) -> bool;
        let points: [(&str, At, usize); 4] = [
            (
                "after a commit",
                |p| matches!(p, HookPoint::SinkCommitted { .. }),
                2,
            ),
            (
                "after a checkpoint",
                |p| matches!(p, HookPoint::CheckpointPersisted { .. }),
                2,
            ),
            (
                "after a chunk",
                |p| matches!(p, HookPoint::ChunkEmitted { .. }),
                2,
            ),
            (
                "after the last chunk",
                |p| matches!(p, HookPoint::ChunkEmitted { .. }),
                4,
            ),
        ];
        for (name, when, n) in points {
            let w = World::new(&["a"]);
            w.source.load(&t("a"), rows(1..=6));
            let p = mode("mode: initial, chunk_size: 2");
            let mut crashing = deps(&w);
            crashing.hooks = Arc::new(crate::test_support::CrashAfter::new(when, n));
            let end = run_with(&p, crashing, async |h| {
                w.source.commit(
                    FakeTxn::new()
                        .update(&t("a"), row(2, "streamed"))
                        .insert(&t("a"), row(8, "streamed")),
                );
                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    let mut st = h.status.clone();
                    let _ = st.wait_for(|_| false).await;
                })
                .await;
            })
            .await;
            assert!(
                matches!(end, Err(WorkerError::Crashed(_))),
                "{name}: {end:?}"
            );
            w.source.commit(FakeTxn::new().delete(
                &t("a"),
                Row(vec![(
                    "id".into(),
                    data_avalanche_core::ColumnValue::Int64(1),
                )]),
            ));
            let end = run_with(&p, deps(&w), async |h| streamed(&h.status, &w).await).await;
            assert_eq!(end.unwrap(), Exit::Drained, "{name}");
            let (ids, docs) = source_rows(&w, "a");
            assert_eq!(w.ids("a"), ids, "{name}");
            assert_eq!(w.text("a", "doc"), docs, "{name}");
        }
    }

    #[tokio::test]
    async fn a_newer_owner_stops_the_worker() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let end = run_with(&mode("mode: never"), deps(&w), async |h| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            streamed(&h.status, &w).await;
            w.sink.take_over(&t("a"), 9);
            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            let mut st = h.status.clone();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), st.wait_for(|_| false))
                .await;
        })
        .await;
        assert!(
            matches!(
                end,
                Err(WorkerError::Fenced {
                    current_epoch: 9,
                    ..
                })
            ),
            "{end:?}"
        );
        assert_eq!(w.ids("a"), [1]);
    }

    #[tokio::test]
    async fn a_lost_lease_stops_the_worker_before_its_next_commit() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let guard = Arc::new(crate::test_support::Switch::default());
        let mut d = deps(&w);
        d.guard = guard.clone();
        let end = run_with(&mode("mode: never"), d, async |h| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            streamed(&h.status, &w).await;
            guard.0.store(true, std::sync::atomic::Ordering::SeqCst);
            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            let mut st = h.status.clone();
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), st.wait_for(|_| false))
                .await;
        })
        .await;
        assert!(matches!(end, Err(WorkerError::LeaseLost)), "{end:?}");
        assert_eq!(w.ids("a"), [1]);
    }

    #[tokio::test]
    async fn a_flush_command_flushes_now() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = Yaml {
            sink: "flush: { seconds: 3600 }",
            ..Yaml::default()
        }
        .build();
        let end = run_with(&p, deps(&w), async |h| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            let end = w.source.wal_end();
            wait_until("it is read", || {
                h.status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            assert!(w.ids("a").is_empty());
            h.control.send(Command::Flush).await.unwrap();
            streamed(&h.status, &w).await;
            assert_eq!(w.ids("a"), [1], "flushed before the drain");
        })
        .await;
        end.unwrap();
    }

    #[tokio::test]
    async fn a_failing_sink_shows_degraded_until_it_recovers() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.sink
            .fail(data_avalanche_testkit::SinkOp::Commit, None, 3, || {
                SinkError::Transient {
                    reason: "throttled".into(),
                }
            });
        let seen = std::sync::Mutex::new(false);
        let end = run_with(&mode("mode: never"), deps(&w), async |h| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            let mut st = h.status.clone();
            let _ = st.wait_for(|s| s.degraded.is_some()).await;
            *seen.lock().unwrap() = true;
            streamed(&h.status, &w).await;
            assert_eq!(h.status.borrow().degraded, None);
        })
        .await;
        end.unwrap();
        assert!(*seen.lock().unwrap());
        assert_eq!(w.ids("a"), [1]);
    }
}
