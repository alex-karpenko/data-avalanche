//! The pipeline worker (Data-Flow Phases 0–5): Phase 0 once, then stream sessions until a drain,
//! a finished dump or an error. Between sessions it decides what a source failure means: a
//! transient one reopens the stream after a backoff, a busy slot's stale reader is ended and the
//! slot retried for a bounded time, and a lost position is replaced and every table snapshotted
//! again.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use data_avalanche_config::{Pipeline, SnapshotMode};
use data_avalanche_core::{
    Checkpoint, ColumnSchema, CommitMarker, CommitOutcome, HaltReason, PositionRange, SchemaHint,
    Sink, SinkError, SinkKind, SnapshotReason, SourceConnector, SourceError, SourcePosition,
    StartPlan, TableChangeSet, TableRef, TableSchema, TableSpec,
};
use data_avalanche_state::{PipelineDocs, SnapshotStatus, StateError, TableHalt, TableSnapshotDoc};
use data_avalanche_storage::StoreError;
use tokio::sync::{mpsc, watch};

use crate::decide::{Decision, Facts, Plan, decide};
use crate::jobs::Jobs;
use crate::schema_change::{Planned, missing_columns, soft_dropped};
use crate::session::{Core, End, Session};
use crate::settings::Settings;
use crate::status::Streak;
use crate::{
    Command, CommitGuard, Exit, HookAction, HookPoint, Hooks, NoHooks, NoObserver, Observer, Phase,
    RowsRead, Tuning, Unguarded, WorkerError, WorkerStatus,
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
    /// Hears the flushes, the commits and the chunk reads (the owner's metrics).
    pub observer: Arc<dyn Observer>,
    /// Whether this lease term already ended the stream position's other reader (DA-SRC-28):
    /// every worker run of the term shares it.
    pub reader_ended: Arc<AtomicBool>,
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
            observer: Arc::new(NoObserver),
            reader_ended: Arc::new(AtomicBool::new(false)),
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
    /// [`WorkerDeps::reader_ended`].
    reader_ended: Arc<AtomicBool>,
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
            observer: deps.observer,
            tuning,
            control: control_rx,
            status: status_tx,
            tracked: BTreeSet::new(),
            copies: BTreeMap::new(),
            dump_only: false,
            snapshot_started: BTreeMap::new(),
            drifted: BTreeSet::new(),
            rescan_failing: Streak::default(),
            held_failing: Streak::default(),
            requery_failing: BTreeMap::new(),
            gone_failing: Streak::default(),
            wal_lag_failing: Streak::default(),
            writers_waited: BTreeSet::new(),
            read_failing: BTreeMap::new(),
            server: None,
            pause: false,
            rows_read: RowsRead::default(),
        };
        let reader_ended = deps.reader_ended;
        (
            Self { core, reader_ended },
            WorkerHandle { control, status },
        )
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
            // A reconnect is counted once it is decided, in one update with its reason: an
            // observer that sees the count sees why. The new session has read nothing and holds
            // nothing pending: the frontier and the pending start are cleared in that update, so
            // the owner never reads the old session's frontier under the new count (DA-OBS-8).
            match failed {
                SourceError::Transient(error) => {
                    contended_since = None;
                    core.update(|st| {
                        st.reconnect();
                        st.phase = Phase::Reconnecting { error };
                    });
                }
                SourceError::Contended(detail) => {
                    let first = contended_since.is_none();
                    let since = *contended_since.get_or_insert_with(Instant::now);
                    let waited = since.elapsed();
                    if waited >= core.tuning.contended_window {
                        return Err(WorkerError::Contended { waited, detail });
                    }
                    // DA-SRC-28: under the lease, the other reader is a stale session. It is
                    // ended on the first busy attempt of a spell, and the stream retried from the
                    // first backoff step. At most once a lease term: two holders that each
                    // believe they hold the lease can't keep ending each other's stream; a later
                    // spell, in this run or a rerun under the same lease, waits and gives up with
                    // `Contended` after the window.
                    let ended = &self.reader_ended;
                    if first && !ended.load(Ordering::SeqCst) && core.may_write().is_ok() {
                        let warning = match core.source.end_other_reader().await {
                            Ok(true) => {
                                ended.store(true, Ordering::SeqCst);
                                attempt = 0;
                                Some(format!(
                                    "ended the stream position's other reader, a session the lease outlived ({detail})"
                                ))
                            }
                            Ok(false) => None,
                            Err(e) => Some(format!(
                                "couldn't end the stream position's other reader ({e}): retrying until it lets go"
                            )),
                        };
                        if let Some(warning) = warning {
                            core.update(|st| st.warn(warning));
                        }
                    }
                    let error = SourceError::Contended(detail).to_string();
                    core.update(|st| {
                        st.reconnect();
                        st.phase = Phase::Reconnecting { error };
                    });
                }
                SourceError::PositionUnreachable { position, detail } => {
                    contended_since = None;
                    let warning = format!(
                        "the source lost the stream position {position} ({detail}): the stream restarts at a new position and the tables are snapshotted again"
                    );
                    core.update(WorkerStatus::reconnect);
                    // The checkpoint whose position was lost, with the server the session read:
                    // on another server the source refuses to replace it (DA-SRC-20), and the
                    // halt or the rerun of Phase 0 its refusal asks ends the worker below.
                    let Some(stored) = core.settings.checkpoint.load(&core.docs).await? else {
                        return Err(crate::AccumulatorError::Protocol {
                            detail: "the stream lost a position the pipeline has no checkpoint for"
                                .into(),
                        }
                        .into());
                    };
                    let lost = Checkpoint {
                        server: core.server.clone().or(stored.checkpoint.server),
                        ..stored.checkpoint
                    };
                    let from = write_retry(
                        &core.tuning,
                        || core.may_write(),
                        || core.source.reset_position(&lost),
                    )
                    .await?;
                    // Warned only once the reset holds: a refusal on another server halts or
                    // reruns Phase 0 instead, and says so itself.
                    core.update(|st| st.warn(warning));
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
                // The next session parks (or reads) as it opens, with the reload's settings.
                Some(Command::Pause) => core.pause = true,
                Some(Command::Resume) => core.pause = false,
                Some(Command::Reconfigure(p)) => core.reconfigure(&p),
                Some(Command::Drain) | None => return true,
            },
        }
    }
}

/// A source call, retried while it fails transiently.
async fn retry<T, F, Fut>(tuning: &Tuning, call: F) -> Result<T, WorkerError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SourceError>>,
{
    write_retry(tuning, || Ok(()), call).await
}

/// A source write, retried while it fails transiently: `may_write` (`Core::may_write`) is asked
/// before every try, as the writer asks before each commit, so a lease that lapses during the
/// retries stops the write.
async fn write_retry<T, G, F, Fut>(
    tuning: &Tuning,
    may_write: G,
    mut call: F,
) -> Result<T, WorkerError>
where
    G: Fn() -> Result<(), WorkerError>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SourceError>>,
{
    let mut attempt = 0;
    loop {
        may_write()?;
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

/// A sink call, retried while it fails transiently. A write passes `Core::may_write` as
/// `may_write`, asked before every try (as in [`write_retry`]); a read passes `|| Ok(())`.
async fn sink_retry<T, G, F, Fut>(
    tuning: &Tuning,
    may_write: G,
    mut call: F,
) -> Result<T, WorkerError>
where
    G: Fn() -> Result<(), WorkerError>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SinkError>>,
{
    let mut attempt = 0;
    loop {
        may_write()?;
        match call().await {
            Err(e) if e.is_transient() => {
                tokio::time::sleep(tuning.sink_backoff.delay(attempt)).await;
                attempt += 1;
            }
            r => return r.map_err(WorkerError::Sink),
        }
    }
}

/// A change set that carries no rows, committed at Phase 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Empty {
    /// Moves the table's marker onto `at`, the new server's position (DA-SRC-20): the marker is
    /// replaced instead of joined. A table the lake holds no marker for has nothing to move.
    Reposition,
    /// Empties the sink table before a job's copy (DA-SNAP-15): a metadata-only delete, fenced,
    /// the marker joined with `at` as any commit's.
    Truncate,
}

/// Whether a cached schema's columns are the catalog's now: the same, in order, with the names
/// compared without case when the source compares them so (`fold`, MySQL): a change of case alone
/// is the same column, cached under its first spelling (DA-SRC-25).
fn same_columns(cached: &TableSchema, now: &TableSchema, fold: bool) -> bool {
    let (a, b) = (cached.columns(), now.columns());
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| match fold && x.name != y.name {
                true => {
                    x.name.to_lowercase() == y.name.to_lowercase()
                        && ColumnSchema {
                            name: y.name.clone(),
                            ..x.clone()
                        } == *y
                }
                false => x == y,
            })
}

/// Commits a change set with no row to `table`: a truncate under `identifier` (the source's
/// key), or a reposition under the key the table was made with (it carries no row, and the table
/// may be keyed otherwise than the source is now: a wiped state's, DA-SRC-32), else `identifier`.
/// A table the lake doesn't hold, or a reposition of one without a marker, has nothing to commit.
/// `Ok(false)` when there was no key to commit under: a reposition of a table that records none,
/// which the pipeline neither resolves nor caches.
async fn commit_empty(
    core: &Core,
    tuning: &Tuning,
    table: &TableRef,
    identifier: &[String],
    at: &SourcePosition,
    kind: Empty,
) -> Result<bool, WorkerError> {
    let state = sink_retry(tuning, || core.may_write(), || core.sink.table_state(table)).await?;
    let (true, Some(columns)) = (state.exists, state.schema) else {
        return Ok(true);
    };
    if kind == Empty::Reposition && state.marker.is_none() {
        return Ok(true);
    }
    let identifier = match kind {
        Empty::Reposition if !state.identifier.is_empty() => state.identifier,
        _ => identifier.to_vec(),
    };
    if identifier.is_empty() {
        return Ok(false);
    }
    let cs = TableChangeSet {
        table: table.clone(),
        schema: columns,
        identifier,
        truncate: kind == Empty::Truncate,
        upserts: vec![],
        deletes: vec![],
        position: PositionRange {
            start: at.clone(),
            end: at.clone(),
            last_change: at.clone(),
        },
        split: None,
        // A truncate is never skipped as covered: the job asked for it now.
        snapshot: kind == Empty::Truncate,
        reposition: kind == Empty::Reposition,
    };
    let marker = CommitMarker {
        owner_epoch: core.owner_epoch,
        committed_position: at.clone(),
        pipeline_id: core.settings.pipeline_id.clone(),
    };
    let committed = sink_retry(
        tuning,
        || core.may_write(),
        || core.sink.commit(cs.clone(), marker.clone()),
    )
    .await?;
    match committed {
        CommitOutcome::Fenced { current_epoch } => Err(WorkerError::Fenced {
            table: table.clone(),
            current_epoch,
        }),
        // Neither is ever skipped as covered (`covers` skips neither a reposition nor a
        // snapshot's set): the arm is unreachable, and harmless.
        CommitOutcome::Committed { .. } | CommitOutcome::SkippedCovered => Ok(true),
    }
}

/// Data-Flow Phase 0, or its rerun after the stream position was lost mid-run (`lost` holds the
/// new checkpoint `reset_position` returned). Leaves the core tracking the tables to stream, with
/// every decision persisted: halts and snapshot requests first, then the cached schemas, then a
/// fresh start's checkpoint, recorded with the server the start read.
pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(), WorkerError> {
    core.update(|st| st.phase = Phase::Starting);
    let s = &core.settings;
    if s.sink_kind == SinkKind::Iceberg {
        return Err(WorkerError::Refused(
            "the Iceberg sink ships in v2 (D-V1.4); other pipelines run".into(),
        ));
    }
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
    core.may_write()?;
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
    let recorded_halts: BTreeMap<TableRef, HaltReason> = core
        .docs
        .halts()
        .await?
        .tables
        .into_iter()
        .map(|h| (h.table, h.reason))
        .collect();
    let recorded: BTreeSet<TableRef> = recorded_halts.keys().cloned().collect();
    let stored = core.settings.checkpoint.load(&core.docs).await?;
    let jobs = Jobs::load(&core.docs).await?;
    let asked = jobs.asked();
    let running = jobs.running();

    // New halts, recorded before the checkpoint below.
    let mut halts: Vec<(TableRef, HaltReason)> = resolved.halted.clone();
    let resolved_tables: BTreeMap<TableRef, TableSchema> = resolved
        .tables
        .into_iter()
        .map(|t| (t.table().clone(), t))
        .collect();
    // A job asks for a halted table: its halt is cleared once its schema is baselined.
    let cleared: BTreeSet<TableRef> = recorded
        .iter()
        .filter(|t| asked.contains(*t) && resolved_tables.contains_key(*t))
        .cloned()
        .collect();
    let recorded: BTreeSet<TableRef> = recorded.difference(&cleared).cloned().collect();
    let gone: Vec<TableSchema> = cached
        .values()
        .filter(|c| !resolved_tables.contains_key(c.table()) && !recorded.contains(c.table()))
        .cloned()
        .collect();
    // A failed rescan halts nothing: the next start checks the tables again. The first failure of
    // a streak warns; a rescan that succeeds, or nothing gone to check, ends the streak.
    if gone.is_empty() {
        core.gone_failing.succeeded();
    } else {
        match source.rescan(&gone).await {
            Ok(report) => {
                core.gone_failing.succeeded();
                warnings.extend(report.warnings);
                halts.extend(
                    report
                        .dropped
                        .into_iter()
                        .map(|t| (t, HaltReason::TableDropped)),
                );
            }
            Err(e) => core.gone_failing.failed(&core.status, || {
                let tables: Vec<String> = gone.iter().map(|g| g.table().to_string()).collect();
                format!(
                    "the source no longer lists {}, and the rescan that checks whether they were dropped failed ({e}): none is halted; the next start checks again",
                    tables.join(", ")
                )
            }),
        }
    }
    for (table, now) in &resolved_tables {
        // A cleared table is baselined to the catalog's, a new source id included. One asked for
        // but not halted yet halts here: the stream's position may predate the drop.
        if !cleared.contains(table)
            && let Some(c) = cached.get(table)
            && c.source_id().is_some()
            && c.source_id() != now.source_id()
        {
            halts.push((table.clone(), HaltReason::TableDropped));
        }
    }

    // The stream position: prepare, or the one a mid-run recovery got.
    let intent = stored.as_ref().is_some_and(|s| s.intent);
    let checkpoint = stored.map(|s| s.checkpoint);
    let (plan, at, added, server) = match lost {
        Some(from) => (
            Plan::Fresh(SnapshotReason::PositionLost),
            from.position,
            vec![],
            from.server,
        ),
        None => {
            if checkpoint.is_none()
                && let Some(doc) = core.settings.checkpoint.intent()
            {
                core.may_write()?;
                core.docs.save_slot(&doc).await?;
            }
            let refs: Vec<TableRef> = resolved_tables.keys().cloned().collect();
            // `prepare` may create, recreate or advance the slot.
            let prepared = write_retry(
                &tuning,
                || core.may_write(),
                || source.prepare(checkpoint.as_ref(), &refs),
            )
            .await?;
            warnings.extend(prepared.warnings);
            match prepared.plan {
                StartPlan::Resume => {
                    let Some(at) = checkpoint.as_ref().map(|c| c.position.clone()) else {
                        return Err(WorkerError::Refused(
                            "the source resumes a stream the pipeline has no checkpoint for".into(),
                        ));
                    };
                    (Plan::Resume, at, prepared.added, prepared.server)
                }
                StartPlan::Snapshot { from, reason } => {
                    // A start cut short after the slot was made starts over as a first start.
                    let reason = if intent {
                        SnapshotReason::FirstStart
                    } else {
                        reason
                    };
                    (Plan::Fresh(reason), from, vec![], prepared.server)
                }
            }
        }
    };
    // The checkpoint document now: written at a fresh start, and a check at a resume that the
    // source gave what the store records (a MySQL source names the server).
    let doc = core.settings.checkpoint.doc(&Checkpoint {
        position: at.clone(),
        server: server.clone(),
    })?;
    let gap = matches!(
        plan,
        Plan::Fresh(
            SnapshotReason::PositionLost
                | SnapshotReason::AdoptedOrphan
                | SnapshotReason::SourceChanged
        )
    );

    // Each table: what this start does with it.
    let halted: BTreeSet<TableRef> = recorded
        .iter()
        .cloned()
        .chain(halts.iter().map(|(t, _)| t.clone()))
        .collect();
    let mut tracked = BTreeSet::new();
    let mut drifted = BTreeSet::new();
    let mut requests = vec![];
    let mut abandoned = vec![];
    let mut schemas = vec![];
    let mut dump_only = !resolved_tables.is_empty();
    let fold = source.column_names_fold();
    for (table, now) in &resolved_tables {
        let mode = core.settings.mode(table);
        dump_only &= matches!(mode, SnapshotMode::DumpAlways | SnapshotMode::DumpInitial);
        if halted.contains(table) {
            tracked.insert(table.clone());
            continue;
        }
        let state = sink_retry(&tuning, || Ok(()), || sink.table_state(table)).await?;
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
        let dump = matches!(mode, SnapshotMode::DumpAlways | SnapshotMode::DumpInitial);
        let (decision, warning) = if asked.contains(table) {
            (
                Decision::Snapshot {
                    resume: false,
                    dump,
                },
                None,
            )
        } else if running.contains(table) && in_progress {
            (Decision::Snapshot { resume: !gap, dump }, None)
        } else {
            decide(mode, plan, facts)
        };
        if let Some(w) = warning {
            warnings.push(format!("table {table}: {w}"));
        }
        // A request is pushed once the table passed the checks below that can halt it: a table
        // halted in this pass gets no `in_progress` document (final review M5).
        let request = matches!(decision, Decision::Snapshot { resume: false, .. });
        match decision {
            Decision::Stop => continue,
            Decision::Stream if in_progress => abandoned.push(table.clone()),
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
        let mut evolved = false;
        let baseline = matches!(plan, Plan::Fresh(_)) || cleared.contains(table);
        // A cleared table's change is planned under its source id now: one created again is
        // the table the catalog has.
        let rebased = cached.get(table).map(|c| match now.source_id() {
            Some(id) if cleared.contains(table) => c.clone().with_source_id(id),
            _ => c.clone(),
        });
        match rebased.as_ref() {
            Some(c) if state.exists && baseline && !same_columns(c, now, fold) => {
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
                            evolved = true;
                        }
                        schema = now;
                    }
                }
            }
            // No schema cached for a table the sink holds: a wiped state, or a table first
            // tracked at a resume whose sink table a job's copy made. Baselined against the
            // sink table's columns (DA-SNAP-13, DA-SNAP-22).
            None if state.exists => {
                let adds = match state.schema.as_deref().map(|s| missing_columns(s, now)) {
                    Some(Ok(adds)) => adds,
                    Some(Err(reason)) => {
                        halts.push((table.clone(), reason));
                        tracked.insert(table.clone());
                        continue;
                    }
                    None => vec![],
                };
                if !adds.is_empty() {
                    let evolver = core.evolver();
                    let applied = match evolver.evolution {
                        true => evolver.evolve(table, adds).await?,
                        false => Err(HaltReason::EvolutionDisabled),
                    };
                    match applied {
                        Ok(None) => {}
                        Ok(Some(w)) => warnings.push(w),
                        Err(reason) => {
                            halts.push((table.clone(), reason));
                            tracked.insert(table.clone());
                            continue;
                        }
                    }
                    evolved = true;
                }
            }
            // Only the case of a name differs (where names fold): the sink's column has the
            // cached spelling, which stays (DA-SRC-25).
            Some(c) if state.exists && baseline && c.columns() != now.columns() => {
                schema = match now.source_id() {
                    Some(id) => c.clone().with_source_id(id),
                    None => c.clone(),
                };
            }
            Some(_) if cleared.contains(table) => {}
            Some(c) if matches!(plan, Plan::Resume) && !same_columns(c, now, fold) => {
                drifted.insert(table.clone());
                let idle = match core.settings.rescan_interval {
                    Some(every) => format!(
                        "or, for an idle table, the next periodic rescan does (within {}s)",
                        every.as_secs()
                    ),
                    None => "and with periodic rescans off (rescan_interval 0) an idle table keeps its cached schema until a change of it streams".to_owned(),
                };
                warnings.push(format!(
                    "table {table}: its schema changed while the pipeline was down; the stream applies the change in order {idle}; if it is being snapshotted, it is held and rescanned before the snapshot reads it"
                ));
                schema = c.clone();
            }
            Some(c) if matches!(plan, Plan::Resume) => schema = c.clone(),
            _ => {}
        }
        if !state.exists {
            match sink_retry(&tuning, || core.may_write(), || sink.ensure_table(&spec)).await {
                Ok(()) => {}
                Err(WorkerError::Sink(SinkError::Permanent { reason })) => {
                    halts.push((table.clone(), HaltReason::SinkRejected { detail: reason }));
                    tracked.insert(table.clone());
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        // The sink's soft-dropped columns: the table as read, or as the baseline's evolve left it.
        match (&state.schema, evolved) {
            (_, true) => core.count_soft_dropped(&schema).await,
            (Some(sink_schema), false) => {
                let n = soft_dropped(sink_schema, &schema);
                core.update(|st| {
                    st.soft_dropped.insert(table.clone(), n);
                });
            }
            (None, false) => {}
        }
        if !tracked_before || baseline {
            schemas.push(schema);
        }
        if request {
            requests.push(table.clone());
        }
        tracked.insert(table.clone());
    }

    // A job's tables outside the config: copies, described by name, read without the stream
    // (Q-M9b-3). One running is described again at every start: its schema is cached nowhere.
    let configured: BTreeSet<&TableRef> = resolved_tables
        .keys()
        .chain(resolved.halted.iter().map(|(t, _)| t))
        .collect();
    let mut failed: BTreeMap<TableRef, String> = BTreeMap::new();
    let mut outside = BTreeSet::new();
    for table in asked
        .iter()
        .chain(&running)
        .filter(|t| !configured.contains(t))
    {
        match recorded_halts.get(table) {
            // A running copy that halted in an earlier run isn't read again: its job fails it,
            // with the halt.
            Some(reason) if !asked.contains(table) => {
                failed.insert(table.clone(), format!("{}: {reason}", reason.code()));
            }
            _ => {
                outside.insert(table.clone());
            }
        }
    }
    let outside: Vec<TableRef> = outside.into_iter().collect();
    let mut copies = BTreeMap::new();
    let mut cleared = cleared;
    if !outside.is_empty() {
        let described = retry(&tuning, || source.describe_tables(&outside)).await?;
        warnings.extend(described.warnings);
        for (table, reason) in described.halted {
            failed.insert(table, format!("{}: {reason}", reason.code()));
        }
        failed.extend(described.skipped);
        for schema in described.tables {
            let table = schema.table().clone();
            let is_asked = asked.contains(&table);
            if !is_asked {
                let doc = core.docs.table_snapshot(&table).await?.map(|v| v.doc);
                if !doc.is_some_and(|d| d.status == SnapshotStatus::InProgress) {
                    continue;
                }
            }
            let spec = match TableSpec::new(&schema, core.settings.partition_by(&table)) {
                Ok(spec) => spec,
                Err(reason) => {
                    failed.insert(table, format!("{}: {reason}", reason.code()));
                    continue;
                }
            };
            let state = sink_retry(&tuning, || Ok(()), || sink.table_state(&table)).await?;
            if !state.exists {
                match sink_retry(&tuning, || core.may_write(), || sink.ensure_table(&spec)).await {
                    Ok(()) => {}
                    Err(WorkerError::Sink(SinkError::Permanent { reason })) => {
                        let reason = HaltReason::SinkRejected { detail: reason };
                        failed.insert(table, format!("{}: {reason}", reason.code()));
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            if is_asked {
                // A copy halted in an earlier run: the job's start clears it.
                if recorded.contains(&table) {
                    cleared.insert(table.clone());
                }
                requests.push(table.clone());
            }
            copies.insert(table, schema);
        }
    }

    // Persist, in this order: a restart in between redoes what follows.
    for (table, reason) in &halts {
        core.may_write()?;
        core.docs
            .record_halt(&TableHalt {
                table: table.clone(),
                reason: reason.clone(),
                at: at.clone(),
                copy: false,
            })
            .await?;
    }
    for table in &requests {
        core.may_write()?;
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
        // A snapshot requested from scratch starts its `timeouts.snapshot` clock again at the
        // next session's open: a re-snapshot after a lost position isn't timed from the run's
        // first snapshot of the table (final review C1).
        core.snapshot_started.remove(table);
    }
    for table in &abandoned {
        core.may_write()?;
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
    let fresh = matches!(plan, Plan::Fresh(_)).then_some(doc);
    // The schemas before a fresh start's checkpoint: a restart in between still finds the
    // checkpoint before it and starts fresh again. The other way round, it would resume from the
    // new checkpoint under the cache of before the baseline, as if the stream still held the
    // change.
    for schema in &schemas {
        core.may_write()?;
        core.docs.save_table_schema(schema).await?;
    }
    // The cleared halts after the baselined schemas: a restart in between finds the table halted
    // and asked for still, and baselines it again.
    for table in &cleared {
        if !halts.iter().any(|(t, _)| t == table) {
            core.may_write()?;
            core.docs.clear_halt(table).await?;
        }
    }
    let started: BTreeSet<TableRef> = requests
        .iter()
        .filter(|t| asked.contains(*t))
        .cloned()
        .collect();
    failed.extend(
        halts
            .iter()
            .filter(|(t, _)| asked.contains(t))
            .map(|(t, reason)| (t.clone(), format!("{}: {reason}", reason.code()))),
    );
    // A truncating job empties each table it starts before the table's first chunk. Repeated
    // only while the job still has the table queued, before any chunk is read: once recorded
    // running below, a later start resumes the copy instead (DA-SNAP-15).
    let truncating = jobs.truncating();
    for table in started.iter().filter(|t| truncating.contains(*t)) {
        if let Some(schema) = resolved_tables.get(table).or_else(|| copies.get(table)) {
            // No key to commit under is as good as a committed truncate: nothing to empty.
            commit_empty(
                core,
                &tuning,
                schema.table(),
                schema.identifier(),
                &at,
                Empty::Truncate,
            )
            .await?;
        }
    }
    core.may_write()?;
    jobs.record(&core.docs, &started, &failed).await?;
    // A copy's halt goes once its job carries it: the pipeline doesn't replicate the table, and
    // no job can clear the halt of a table gone. It is the halt of a table neither configured
    // nor cached (a table taken out of the config keeps its schema) nor read now.
    for table in recorded_halts.keys() {
        if !configured.contains(table)
            && !cached.contains_key(table)
            && !requests.contains(table)
            && !copies.contains_key(table)
            && !cleared.contains(table)
        {
            core.may_write()?;
            core.docs.clear_halt(table).await?;
        }
    }
    // A source change moves every table the lake holds onto the new server's positions before
    // the checkpoint that names the server: a restart in between still finds the old one
    // recorded, and repositions them again (DA-SRC-20). Every table the sink holds, not only
    // those resolved or cached now: a job's copy, a table outside the config, would keep the old
    // server's marker and skip the new one's changes when tracked later (DA-SRC-32). A table the
    // sink refuses the reposition of halts (`SINK_REJECTED`) instead of ending every start.
    if matches!(plan, Plan::Fresh(SnapshotReason::SourceChanged)) {
        let held = sink_retry(&tuning, || Ok(()), || sink.held_tables()).await?;
        let tables: BTreeSet<&TableRef> = resolved_tables
            .keys()
            .chain(cached.keys())
            .chain(&held)
            .collect();
        for table in tables {
            let identifier = cached
                .get(table)
                .or_else(|| resolved_tables.get(table))
                .map_or(&[][..], |s| s.identifier());
            match commit_empty(core, &tuning, table, identifier, &at, Empty::Reposition).await {
                Ok(true) => {}
                Ok(false) => warnings.push(format!(
                    "table {table}: the lake holds it, but its table records no key and the \
                     pipeline doesn't track it, so its marker stays on the old server; track it \
                     and re-snapshot it before its changes stream"
                )),
                Err(WorkerError::Sink(SinkError::Permanent { reason })) => {
                    let reason = HaltReason::SinkRejected {
                        detail: format!("the source changed, and its reposition fails: {reason}"),
                    };
                    core.may_write()?;
                    core.docs
                        .record_halt(&TableHalt {
                            table: table.clone(),
                            reason: reason.clone(),
                            at: at.clone(),
                            copy: false,
                        })
                        .await?;
                    core.update(|st| {
                        st.halted.insert(table.clone(), reason);
                    });
                }
                Err(e) => return Err(e),
            }
        }
    }
    if let Some(doc) = fresh {
        if core.hooks.at(&HookPoint::SchemasCached) == HookAction::Crash {
            return Err(WorkerError::Crashed(HookPoint::SchemasCached));
        }
        core.may_write()?;
        doc.save(&core.docs).await?;
    }
    core.server = server;
    core.tracked = tracked;
    core.copies = copies;
    core.drifted = drifted;
    core.dump_only = dump_only;
    core.update(|st| warnings.into_iter().for_each(|w| st.warn(w)));
    Ok(())
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{
        ColumnSchema, ColumnValue, ColumnValueType as V, Row, SchemaChange, SchemaDelta,
        ServerIdentity, SourcePosition, upsert_schema,
    };
    use data_avalanche_state::{PositionDoc, SlotDoc};
    use data_avalanche_testkit::{FakeTxn, Method, SinkOp};

    use super::*;
    use crate::test_support::{
        Countdown, CrashAt, World, Yaml, docs, fast, row, schema, t, wait_until,
    };
    use crate::{Backoff, HookPoint};
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

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

    /// The start counts the sink's columns the source table doesn't have: soft-dropped, kept and
    /// written NULL.
    #[tokio::test]
    async fn the_start_counts_the_sink_columns_the_source_lacks() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let gone = SchemaDelta {
            table: t("a"),
            changes: vec![SchemaChange::AddColumn(ColumnSchema::new(
                "gone",
                V::Text,
                "text",
            ))],
        };
        w.sink.evolve(&t("a"), &gone, 1).await.unwrap();
        let (end, st) = run(&w, &mode("mode: initial"), async |status| {
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(st.soft_dropped, BTreeMap::from([(t("a"), 1)]));
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

    /// A failover to a replica behind the lake (DA-SRC-20): the new server's LSNs sit below the
    /// table's marker, and the guard would skip its changes. The source-change start repositions
    /// the table first, so a change on the new server lands even under `never` (nothing copied).
    #[tokio::test]
    async fn a_source_change_repositions_every_table_before_streaming() {
        let w = World::new(&["a"]);
        let pg = |timeline| ServerIdentity::Postgres {
            system_identifier: 7,
            timeline,
        };
        w.source.set_server(Some(pg(1)));
        w.source.load(&t("a"), rows(1..=2));
        run(&w, &mode("mode: initial"), async |status| {
            for id in 3..=6 {
                w.source
                    .commit(FakeTxn::new().insert(&t("a"), row(id, "before")));
            }
            streamed(&status, &w).await;
        })
        .await
        .0
        .unwrap();
        let marked = w.sink.table_state(&t("a")).await.unwrap().marker.unwrap();
        assert!(
            marked
                .committed_position
                .covers(&SourcePosition::Pg(Lsn(0x110)))
                .unwrap(),
            "{marked:?}"
        );
        w.source.fail_over(pg(2), Lsn(0x100));
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(9, "on the new server")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6, 9]);
        let moved = w.sink.table_state(&t("a")).await.unwrap().marker.unwrap();
        assert!(
            !moved
                .committed_position
                .covers(&marked.committed_position)
                .unwrap(),
            "{moved:?}"
        );
        assert_eq!(
            w.docs.slot().await.unwrap().unwrap().doc.server,
            Some(pg(2))
        );
        assert!(
            st.warnings
                .iter()
                .any(|w| w.contains("source server changed")),
            "{:?}",
            st.warnings
        );
    }

    /// A failover mid-run whose new server can't serve the slot's position (I-1): the stream ends
    /// with `PositionUnreachable` on another timeline. The reset refuses to make the slot there;
    /// the worker ends with `SourceChanged`, and the rerun (the controller's `Retry`) repositions
    /// every table, so a change on the new server below the old marker lands.
    #[tokio::test]
    async fn a_failover_mid_run_reruns_phase_0_instead_of_resetting_the_slot() {
        let w = World::new(&["a"]);
        let pg = |timeline| ServerIdentity::Postgres {
            system_identifier: 7,
            timeline,
        };
        w.source.set_server(Some(pg(1)));
        w.source.load(&t("a"), rows(1..=2));
        let (end, _) = run(&w, &mode("mode: initial"), async |status| {
            for id in 3..=6 {
                w.source
                    .commit(FakeTxn::new().insert(&t("a"), row(id, "before")));
            }
            streamed(&status, &w).await;
            w.source.fail_over(pg(2), Lsn(0x100));
            w.source.lose_slot();
            // Ends when the worker does, or once a reset position would be durable.
            let mut st = status.clone();
            let _ = st
                .wait_for(|s| {
                    s.reconnects >= 1
                        && s.snapshots.is_empty()
                        && s.checkpoint == Some(SourcePosition::Pg(Lsn(0x100)))
                })
                .await;
        })
        .await;
        let end = end.expect_err("the worker ends for a rerun");
        assert!(
            matches!(end, WorkerError::Source(SourceError::SourceChanged(_))),
            "{end:?}"
        );
        assert_eq!(end.recovery(), crate::Recovery::Retry);
        let marked = w.sink.table_state(&t("a")).await.unwrap().marker.unwrap();
        assert_eq!(
            w.docs.slot().await.unwrap().unwrap().doc.server,
            Some(pg(1)),
            "the checkpoint still names the old server"
        );
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(9, "on the new server")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6, 9], "no change skipped");
        let moved = w.sink.table_state(&t("a")).await.unwrap().marker.unwrap();
        assert!(
            !moved
                .committed_position
                .covers(&marked.committed_position)
                .unwrap(),
            "{moved:?}"
        );
        assert_eq!(
            w.docs.slot().await.unwrap().unwrap().doc.server,
            Some(pg(2))
        );
        assert!(
            st.warnings
                .iter()
                .any(|w| w.contains("source server changed")),
            "{:?}",
            st.warnings
        );
    }

    /// The same failover under `source_change_action: error`: the reset halts with
    /// `WAL_SOURCE_CHANGED` and replaces nothing; the checkpoint still names the old server, and
    /// a restart halts the same way.
    #[tokio::test]
    async fn a_failover_mid_run_halts_under_error() {
        let w = World::new(&["a"]);
        let pg = |timeline| ServerIdentity::Postgres {
            system_identifier: 7,
            timeline,
        };
        w.source.set_server(Some(pg(1)));
        w.source.halt_on_source_change();
        w.source.load(&t("a"), rows(1..=2));
        let (end, _) = run(&w, &mode("mode: initial"), async |status| {
            streamed(&status, &w).await;
            w.source.fail_over(pg(2), Lsn(0x100));
            w.source.lose_slot();
            // Until the worker ends (its status closes).
            let mut st = status.clone();
            let _ = tokio::time::timeout(Duration::from_secs(10), st.wait_for(|_| false)).await;
        })
        .await;
        let end = end.expect_err("the worker halts");
        assert_eq!(end.code(), Some("WAL_SOURCE_CHANGED"), "{end:?}");
        assert_eq!(end.recovery(), crate::Recovery::Error);
        assert_eq!(
            w.docs.slot().await.unwrap().unwrap().doc.server,
            Some(pg(1))
        );
        let (end, _) = run(&w, &mode("mode: never"), async |_| {}).await;
        assert_eq!(
            end.expect_err("a restart halts").code(),
            Some("WAL_SOURCE_CHANGED")
        );
    }

    /// A reconnect that reaches another server, with no position lost on the way (the stream's
    /// open reads the server): the worker ends with `SourceChanged`, and the rerun repositions.
    #[tokio::test]
    async fn a_reconnect_to_another_server_reruns_phase_0() {
        let w = World::new(&["a"]);
        let pg = |timeline| ServerIdentity::Postgres {
            system_identifier: 7,
            timeline,
        };
        w.source.set_server(Some(pg(1)));
        w.source.load(&t("a"), rows(1..=2));
        let (end, _) = run(&w, &mode("mode: initial"), async |status| {
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(3, "before")));
            streamed(&status, &w).await;
            w.source.fail_over(pg(2), Lsn(0x100));
            w.source.fail(
                Method::Next,
                SourceError::Transient("connection reset".into()),
            );
            // Until the worker ends (its status closes).
            let mut st = status.clone();
            let _ = tokio::time::timeout(Duration::from_secs(10), st.wait_for(|_| false)).await;
        })
        .await;
        assert!(
            matches!(end, Err(WorkerError::Source(SourceError::SourceChanged(_)))),
            "{end:?}"
        );
        let opened = w.source.opened_at();
        assert!(
            opened.iter().all(|c| c.server == Some(pg(1))),
            "no stream opened on the new server: {opened:?}"
        );
        let (end, _) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(9, "on the new server")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2, 3, 9]);
    }

    /// D-M12a.2's crash-repeat (M-10): a crash after the source-change start's resets and before
    /// its checkpoint is saved leaves the old server's checkpoint, so the rerun is a source change
    /// again and repositions every table once more, and the new server's changes land.
    #[tokio::test]
    async fn a_crash_between_the_resets_and_the_checkpoint_repositions_again() {
        let w = World::new(&["a"]);
        let pg = |timeline| ServerIdentity::Postgres {
            system_identifier: 7,
            timeline,
        };
        w.source.set_server(Some(pg(1)));
        w.source.load(&t("a"), rows(1..=2));
        run(&w, &mode("mode: initial"), async |status| {
            for id in 3..=6 {
                w.source
                    .commit(FakeTxn::new().insert(&t("a"), row(id, "before")));
            }
            streamed(&status, &w).await;
        })
        .await
        .0
        .unwrap();
        let recorded = w.docs.slot().await.unwrap().unwrap().doc;
        w.source.fail_over(pg(2), Lsn(0x100));
        let mut crashing = deps(&w);
        crashing.hooks = Arc::new(CrashAt(|p| matches!(p, HookPoint::SchemasCached)));
        let end = run_with(&mode("mode: never"), crashing, async |_| {}).await;
        assert!(
            matches!(end, Err(WorkerError::Crashed(HookPoint::SchemasCached))),
            "{end:?}"
        );
        let first = w.sink.table_state(&t("a")).await.unwrap().marker.unwrap();
        assert_eq!(
            first.committed_position,
            SourcePosition::Pg(Lsn(0x100)),
            "reset before the crash"
        );
        assert_eq!(
            w.docs.slot().await.unwrap().unwrap().doc,
            recorded,
            "the checkpoint naming the old server is kept"
        );
        // The new server writes on while the pipeline is down: the rerun starts past the first reset.
        w.source
            .commit(FakeTxn::new().insert(&t("a"), row(8, "while down")));
        let again = w.source.wal_end();
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
            let marker = w.sink.table_state(&t("a")).await.unwrap().marker.unwrap();
            assert_eq!(marker.committed_position, again, "repositioned again");
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(9, "on the new server")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(
            w.source
                .prepared_at()
                .last()
                .and_then(|c| c.clone()?.server),
            Some(pg(1)),
            "the rerun's prepare got the old server's checkpoint"
        );
        // Row 8 came before the rerun's position: under `never` nothing is copied.
        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6, 9]);
        assert_eq!(
            w.docs.slot().await.unwrap().unwrap().doc.server,
            Some(pg(2))
        );
        assert!(
            st.warnings
                .iter()
                .any(|w| w.contains("source server changed")),
            "{:?}",
            st.warnings
        );
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

    /// A rescan's warnings reach the status from Phase 0's rescan of the tables gone from the
    /// source's list (DA-SRC-36).
    #[tokio::test]
    async fn a_gone_tables_rescan_warnings_reach_the_status() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.source.drop_table(&t("b"));
        w.source
            .set_rescan_warnings(vec!["table public.b: maybe hidden".into()]);
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(st.halted.get(&t("b")), Some(&HaltReason::TableDropped));
        assert!(
            st.warnings
                .iter()
                .any(|m| m == "table public.b: maybe hidden"),
            "{:?}",
            st.warnings
        );
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
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
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
        assert_eq!(st.reconnects, 1);
    }

    /// A pause that arrives between sessions (the reconnect's backoff) holds the next session as
    /// it opens; a resume reads again.
    #[tokio::test]
    async fn a_pause_during_a_reconnect_parks_the_next_session() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let tuning = Tuning {
            source_backoff: Backoff {
                first: Duration::from_millis(300),
                max: Duration::from_millis(300),
            },
            ..fast()
        };
        let (worker, handle) = PipelineWorker::new(&mode("mode: never"), deps(&w), tuning);
        let driver = async {
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
            wait_until("reconnecting", || {
                matches!(handle.status.borrow().phase, Phase::Reconnecting { .. })
            })
            .await;
            handle.control.send(Command::Pause).await.unwrap();
            wait_until("the next session parks", || {
                handle.status.borrow().phase == Phase::Paused
            })
            .await;
            assert_eq!(w.source.opened().len(), 2, "a new session, parked");
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(w.ids("a").is_empty(), "nothing is read while paused");
            handle.control.send(Command::Resume).await.unwrap();
            wait_until("the row lands", || w.ids("a") == [1]).await;
            handle.control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
    }

    /// A busy slot whose other reader can't be ended (the end itself fails, here "permission
    /// denied") is retried for a while, then the worker gives up.
    #[tokio::test]
    async fn a_busy_slot_is_retried_for_a_while() {
        let denied = || SourceError::Permanent("permission denied to terminate process".into());
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.fail(Method::EndOtherReader, denied());
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
        w.source.fail(Method::EndOtherReader, denied());
        for _ in 0..10_000 {
            w.source
                .fail(Method::OpenStream, SourceError::Contended("active".into()));
        }
        let (end, st) = run(&w, &mode("mode: never"), async |_| {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        })
        .await;
        assert!(matches!(end, Err(WorkerError::Contended { .. })), "{end:?}");
        assert!(
            st.warnings.iter().any(|m| m.contains("couldn't end")
                && m.contains("permission denied to terminate process")),
            "{:?}",
            st.warnings
        );
        assert_eq!(w.source.readers_ended(), 0);
    }

    /// DA-SRC-28: under the lease, the slot's other reader is stale (a holder that stopped
    /// without closing its connection, or this node's own dropped session). The worker ends it
    /// instead of waiting out `wal_sender_timeout`, and warns.
    #[tokio::test]
    async fn a_busy_slot_s_stale_reader_is_ended() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        for _ in 0..10_000 {
            w.source.fail(
                Method::OpenStream,
                SourceError::Contended("replication slot \"s\" is active for PID 7".into()),
            );
        }
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1]);
        assert_eq!(w.source.readers_ended(), 1);
        assert!(
            st.warnings
                .iter()
                .any(|m| m.contains("ended the stream position's other reader")
                    && m.contains("active for PID 7")),
            "{:?}",
            st.warnings
        );
    }

    /// The stale reader is ended at most once a term: an ended reader that comes back (a second
    /// holder that believes it holds the lease) is waited out, not ended again.
    #[tokio::test]
    async fn an_ended_reader_that_comes_back_is_not_ended_again() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        for _ in 0..10_000 {
            w.source
                .fail(Method::OpenStream, SourceError::Contended("active".into()));
        }
        let end = run_with(&mode("mode: never"), deps(&w), async |h| {
            streamed(&h.status, &w).await;
            assert_eq!(w.source.readers_ended(), 1);
            for _ in 0..10_000 {
                w.source
                    .fail(Method::OpenStream, SourceError::Contended("active".into()));
            }
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
            let mut st = h.status.clone();
            let _ = tokio::time::timeout(Duration::from_secs(5), st.wait_for(|_| false)).await;
        })
        .await;
        assert!(matches!(end, Err(WorkerError::Contended { .. })), "{end:?}");
        assert_eq!(w.source.readers_ended(), 1);
    }

    /// A worker whose lease lapsed ends no reader: the other reader may be the new holder's.
    #[tokio::test]
    async fn a_worker_without_its_lease_ends_no_reader() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let guard = Arc::new(crate::test_support::Switch::default());
        let mut d = deps(&w);
        d.guard = guard.clone();
        let end = run_with(&mode("mode: never"), d, async |h| {
            streamed(&h.status, &w).await;
            guard.0.store(true, std::sync::atomic::Ordering::SeqCst);
            for _ in 0..10_000 {
                w.source
                    .fail(Method::OpenStream, SourceError::Contended("active".into()));
            }
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
            let mut st = h.status.clone();
            let _ = tokio::time::timeout(Duration::from_secs(5), st.wait_for(|_| false)).await;
        })
        .await;
        assert!(matches!(end, Err(WorkerError::Contended { .. })), "{end:?}");
        assert_eq!(w.source.readers_ended(), 0);
    }

    /// A source error that ends the worker is no reconnect: neither a permanent one nor a busy
    /// slot past its window counts toward `reconnects`.
    #[tokio::test]
    async fn a_fatal_source_error_counts_no_reconnect() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.fail(
            Method::OpenStream,
            SourceError::Permanent("permission denied".into()),
        );
        let (end, st) = run(&w, &mode("mode: never"), async |_| {}).await;
        assert!(
            matches!(end, Err(WorkerError::Source(SourceError::Permanent(_)))),
            "{end:?}"
        );
        assert_eq!(st.reconnects, 0);

        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source
            .fail(Method::OpenStream, SourceError::Contended("active".into()));
        let tuning = Tuning {
            contended_window: Duration::ZERO,
            ..fast()
        };
        let (worker, handle) = PipelineWorker::new(&mode("mode: never"), deps(&w), tuning);
        let end = worker.run().await;
        assert!(matches!(end, Err(WorkerError::Contended { .. })), "{end:?}");
        assert_eq!(handle.status.borrow().reconnects, 0);
    }

    /// DA-OBS-8: a reconnect starts a session that has read nothing and holds nothing pending.
    /// The count comes with a cleared frontier and pending start, so the owner's lag never takes
    /// the old session's frontier as the new one's.
    #[tokio::test]
    async fn a_reconnect_clears_the_frontier_and_the_pending_start() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = Yaml {
            sink: "flush: { seconds: 3600 }",
            ..Yaml::default()
        }
        .build();
        let end = run_with(&p, deps(&w), async |h| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            wait_until("a change read and pending", || {
                let st = h.status.borrow();
                st.read.is_some() && st.unsynced_since_micros.is_some()
            })
            .await;
            for _ in 0..10_000 {
                w.source
                    .fail(Method::OpenStream, SourceError::Transient("refused".into()));
            }
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
            wait_until("a reconnect", || h.status.borrow().reconnects >= 1).await;
            let st = h.status.borrow().clone();
            assert_eq!(st.read, None, "the new session has read nothing");
            assert_eq!(st.unsynced_since_micros, None, "nor holds anything pending");
        })
        .await;
        assert!(end.is_ok(), "{end:?}");
    }

    /// A busy slot's reconnect is counted with its reason, as a transient failure's is: the
    /// owner logs a reconnect by the `Reconnecting` phase beside the count.
    #[tokio::test(start_paused = true)]
    async fn a_busy_slot_reconnect_reports_its_reason() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.fail(
            Method::OpenStream,
            SourceError::Contended("replication slot \"s\" is active for PID 7".into()),
        );
        let tuning = Tuning {
            source_backoff: Backoff {
                first: Duration::from_secs(10),
                max: Duration::from_secs(10),
            },
            ..fast()
        };
        let (worker, handle) = PipelineWorker::new(&mode("mode: never"), deps(&w), tuning);
        let driver = async {
            wait_until("one reconnect", || handle.status.borrow().reconnects == 1).await;
            match &handle.status.borrow().phase {
                Phase::Reconnecting { error } => {
                    assert!(error.contains("held by another reader"), "{error}");
                    assert!(error.contains("active for PID 7"), "{error}");
                }
                other => panic!("{other:?}"),
            }
            handle.control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
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

    /// A MySQL → Delta pipeline `p` over the fake's `public.*`, its `snapshots` set to `tables`.
    fn mysql(tables: &str) -> Pipeline {
        let tables = format!("\n      snapshots: {{ {tables} }}");
        Yaml {
            mysql: Some(""),
            tables: &tables,
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build()
    }

    fn server(id: u32) -> ServerIdentity {
        ServerIdentity::Mariadb { server_id: id }
    }

    async fn position(w: &World) -> Option<PositionDoc> {
        w.docs.position().await.unwrap().map(|v| v.doc)
    }

    /// A MySQL pipeline's first start records its position in the position document, with the
    /// server `prepare` named, and every stream opens from it.
    #[tokio::test]
    async fn a_mysql_first_start_records_the_server_with_the_position() {
        let w = World::new(&["a"]);
        w.source.set_server(Some(server(1)));
        w.source.load(&t("a"), rows(1..=2));
        let (end, st) = run(&w, &mysql("mode: initial"), async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(9, "s")));
            streamed(&status, &w).await;
        })
        .await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert_eq!(w.ids("a"), [1, 2, 9]);
        assert_eq!(
            position(&w).await,
            Some(PositionDoc {
                position: w.source.wal_end(),
                server: server(1)
            })
        );
        assert!(w.docs.slot().await.unwrap().is_none(), "no slot document");
        let opened = w.source.opened_at();
        assert!(
            !opened.is_empty() && opened.iter().all(|c| c.server == Some(server(1))),
            "{opened:?}"
        );
        assert!(st.warnings.is_empty(), "{:?}", st.warnings);
    }

    /// A restart hands `prepare` the recorded server; the checkpoints after it record the server
    /// the start reads now (a GTID failover between two runs).
    #[tokio::test]
    async fn a_mysql_restart_hands_prepare_the_recorded_server() {
        let w = World::new(&["a"]);
        w.source.set_server(Some(server(1)));
        let p = mysql("mode: initial");
        run(&w, &p, async |status| streamed(&status, &w).await)
            .await
            .0
            .unwrap();
        let recorded = position(&w).await.unwrap();
        w.source.set_server(Some(server(2)));
        w.source
            .commit(FakeTxn::new().insert(&t("a"), row(5, "while down")));
        run(&w, &p, async |status| streamed(&status, &w).await)
            .await
            .0
            .unwrap();
        assert_eq!(
            w.source.prepared_at().last(),
            Some(&Some(Checkpoint {
                position: recorded.position,
                server: Some(server(1))
            }))
        );
        assert_eq!(w.ids("a"), [5]);
        assert_eq!(position(&w).await.unwrap().server, server(2));
    }

    /// A position lost mid-run restarts at the reset position, recorded with the server the reset
    /// read, and the stream reopens from it.
    #[tokio::test]
    async fn a_lost_mysql_position_records_the_resets_server() {
        let w = World::new(&["a"]);
        w.source.set_server(Some(server(1)));
        w.source.load(&t("a"), rows(1..=2));
        let (end, st) = run(&w, &mysql("mode: initial"), async |status| {
            streamed(&status, &w).await;
            let reads = w.source.reads(&t("a"));
            w.source.set_server(Some(server(2)));
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
        assert_eq!(w.ids("a"), [1, 2, 7]);
        assert_eq!(position(&w).await.unwrap().server, server(2));
        assert_eq!(
            w.source.opened_at().last().and_then(|c| c.server.clone()),
            Some(server(2))
        );
        assert!(
            st.warnings
                .iter()
                .any(|w| w.contains("lost the stream position"))
        );
    }

    /// The pipeline's state is gone, and the sink still holds its table: the start adds the
    /// columns the source gained, before the copy writes them (DA-SNAP-13).
    #[tokio::test]
    async fn a_wiped_state_baselines_against_the_sink_table() {
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
        let wiped = World {
            source: w.source.clone(),
            sink: w.sink.clone(),
            docs: docs().0,
        };
        let (end, st) = run(&wiped, &mode("mode: initial"), async |status| {
            streamed(&status, &wiped).await;
        })
        .await;
        end.unwrap();
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        let state = w.sink.table_state(&t("a")).await.unwrap();
        assert!(state.schema.unwrap().field_with_name("note").is_ok());
    }

    /// DA-SNAP-22: a table first tracked at a resume (here, one created since the last start)
    /// whose sink table exists already, as a job's copy leaves it, is baselined to the source's
    /// columns before its copy, like a wiped state's.
    #[tokio::test]
    async fn a_table_first_tracked_at_a_resume_is_baselined_against_its_sink_table() {
        let w = World::new(&["a"]);
        let p = mode("mode: initial");
        run(&w, &p, async |status| streamed(&status, &w).await)
            .await
            .0
            .unwrap();
        w.sink
            .ensure_table(&TableSpec::new(&schema("b"), vec![]).unwrap())
            .await
            .unwrap();
        let noted = TableSchema::new(
            t("b"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        w.source.create_table(noted);
        let mut r = row(1, "x");
        r.0.push(("note".into(), ColumnValue::Text("n".into())));
        w.source.load(&t("b"), vec![r]);
        let (end, st) = run(&w, &p, async |status| streamed(&status, &w).await).await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert_eq!(w.source.prepares().len(), 2, "a resume");
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        assert_eq!(w.text("b", "note"), [Some("n".to_owned())]);
    }

    /// DA-SNAP-22: a sink table holding a non-null column the source no longer has (a key it
    /// was made with) can't take the source's rows: it halts at the baseline, naming the column,
    /// rather than at the first MERGE.
    #[test]
    fn a_sink_table_with_a_non_null_column_the_source_lacks_halts() {
        let keyed = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("region", V::Text, "text").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
            ],
            vec!["id".into(), "region".into()],
        )
        .unwrap();
        let sink = upsert_schema(&keyed);
        let Err(HaltReason::SchemaMismatch { column, detail }) =
            missing_columns(&sink, &schema("a"))
        else {
            panic!("a non-null column the source lacks halts");
        };
        assert_eq!(column, "region");
        assert!(detail.contains("non-null"), "{detail}");
        // A nullable one is soft-dropped: kept, written NULL.
        let sink = upsert_schema(
            &TableSchema::new(
                t("a"),
                vec![
                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("doc", V::Text, "text"),
                    ColumnSchema::new("old", V::Text, "text"),
                ],
                vec!["id".into()],
            )
            .unwrap(),
        );
        assert!(missing_columns(&sink, &schema("a")).unwrap().is_empty());
    }

    /// A sink table without the source's key column is keyed otherwise: it halts.
    #[test]
    fn a_sink_table_without_the_key_column_halts() {
        let sink = upsert_schema(&schema("a"));
        let rekeyed = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("uid", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
            ],
            vec!["uid".into()],
        )
        .unwrap();
        assert!(matches!(
            missing_columns(&sink, &rekeyed),
            Err(HaltReason::SchemaMismatch { column, .. }) if column == "uid"
        ));
        assert!(missing_columns(&sink, &schema("a")).unwrap().is_empty());
    }

    /// Delta's names are case-insensitive: a sink column that matches by case alone halts, on a
    /// key or not, naming both spellings.
    #[test]
    fn a_sink_column_matching_by_case_alone_halts() {
        let sink = upsert_schema(&schema("a"));
        let cased = |id: &str, doc: &str| {
            TableSchema::new(
                t("a"),
                vec![
                    ColumnSchema::new(id, V::Int64, "bigint").not_null(),
                    ColumnSchema::new(doc, V::Text, "text"),
                ],
                vec![id.into()],
            )
            .unwrap()
        };
        let Err(HaltReason::SchemaMismatch { column, detail }) =
            missing_columns(&sink, &cased("id", "DOC"))
        else {
            panic!("a case-only match halts");
        };
        assert_eq!(column, "DOC");
        assert!(detail.contains("doc") && detail.contains("DOC"), "{detail}");
        assert!(matches!(
            missing_columns(&sink, &cased("ID", "doc")),
            Err(HaltReason::SchemaMismatch { column, .. }) if column == "ID"
        ));
        assert!(
            missing_columns(&sink, &cased("id", "doc"))
                .unwrap()
                .is_empty()
        );
        // Unicode case too, as delta-rs folds it (DA-SINK-12).
        let sink = upsert_schema(&cased("id", "ärger"));
        assert!(matches!(
            missing_columns(&sink, &cased("id", "ÄRGER")),
            Err(HaltReason::SchemaMismatch { column, .. }) if column == "ÄRGER"
        ));
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

    /// DA-SRC-25: a baseline (a lost position) of a table whose catalog spelling changed case
    /// alone, where names fold, caches the sink's spelling, not the catalog's.
    #[tokio::test]
    async fn a_baseline_of_a_case_only_change_caches_the_sink_spelling() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.fold_column_names();
        w.source.alter(
            &t("a"),
            TableSchema::new(
                t("a"),
                vec![
                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("DOC", V::Text, "text"),
                ],
                vec!["id".into()],
            )
            .unwrap(),
        );
        w.source.lose_slot();
        let (end, _) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached.columns()[1].name, "doc", "the sink's spelling");
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
                Some(ColumnValue::Int64(i)) => *i,
                other => panic!("{other:?}"),
            })
            .collect();
        let docs = rows
            .iter()
            .map(|r| match r.get("doc") {
                Some(ColumnValue::Text(d)) => Some(d.clone()),
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
            w.source.commit(
                FakeTxn::new().delete(&t("a"), Row(vec![("id".into(), ColumnValue::Int64(1))])),
            );
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

    /// A newer owner wrote the table: the evolve of a column the stream added is fenced, the
    /// worker stops, and nothing of the transaction lands.
    #[tokio::test]
    async fn a_newer_owner_stops_the_worker_at_an_evolve() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let end = run_with(&mode("mode: never"), deps(&w), async |h| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            streamed(&h.status, &w).await;
            w.sink.take_over(&t("a"), 9);
            w.source.alter(&t("a"), noted());
            let mut r = row(2, "x");
            r.0.push(("note".into(), ColumnValue::Text("n".into())));
            w.source.commit(FakeTxn::new().insert(&t("a"), r));
            // The status channel closes when the worker ends.
            let mut st = h.status.clone();
            let _ =
                tokio::time::timeout(std::time::Duration::from_secs(10), st.wait_for(|_| false))
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
        let content = w.sink.content(&t("a")).unwrap();
        assert!(content.column_by_name("note").is_none(), "nothing evolved");
    }

    /// The sink holds the table under another key (the source's key, or its override, changed
    /// since the table was created): its first commit is refused, the table halts
    /// `SINK_REJECTED` naming both keys, and the other tables go on.
    #[tokio::test]
    async fn a_sink_table_keyed_otherwise_halts_the_table() {
        let w = World::new(&["a", "b"]);
        let mut spec = TableSpec::new(&w.source.schema(&t("a")), vec![]).unwrap();
        spec.identifier = vec!["id".into(), "doc".into()];
        w.sink.ensure_table(&spec).await.unwrap();
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(1, "x"))
                    .insert(&t("b"), row(1, "x")),
            );
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        match st.halted.get(&t("a")) {
            Some(HaltReason::SinkRejected { detail }) => {
                assert!(detail.contains("keyed by"), "{detail}");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(w.ids("b"), [1]);
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

    /// Phase 0's writes so far on a first start of table `a`, in their order.
    async fn phase_0_writes(w: &World) -> usize {
        let slot = w.docs.slot().await.unwrap().map(|v| v.doc);
        [
            w.docs.state().await.unwrap().is_some(),
            slot.is_some(),
            !w.source.prepares().is_empty(),
            w.sink.table_state(&t("a")).await.unwrap().exists,
            w.docs.table_snapshot(&t("a")).await.unwrap().is_some(),
            w.docs.table_schema(&t("a")).await.unwrap().is_some(),
            slot.is_some_and(|d| !d.is_intent()),
        ]
        .into_iter()
        .filter(|&written| written)
        .count()
    }

    /// A first start writes the pipeline document, the slot intent, the slot (`prepare`), the
    /// sink table, the snapshot request, the schema and the checkpoint: seven writes, each asked
    /// of the guard. The lease lapsing before write `n + 1` leaves exactly `n`, and the next start
    /// completes the pipeline.
    #[tokio::test]
    async fn a_lease_lost_before_any_phase_0_write_stops_it_there() {
        for n in 0..7 {
            let w = World::new(&["a"]);
            w.source.load(&t("a"), rows(1..=3));
            let p = mode("mode: initial");
            let mut d = deps(&w);
            d.guard = Arc::new(Countdown(AtomicUsize::new(n)));
            let (worker, _handle) = PipelineWorker::new(&p, d, fast());
            let end = tokio::time::timeout(Duration::from_secs(5), worker.run())
                .await
                .expect("the lease lapses within Phase 0");
            assert!(matches!(end, Err(WorkerError::LeaseLost)), "{n}: {end:?}");
            assert_eq!(phase_0_writes(&w).await, n);
            let (end, _) = run(&w, &p, async |status| streamed(&status, &w).await).await;
            assert_eq!(end.unwrap(), Exit::Drained, "{n}");
            assert_eq!(w.ids("a"), [1, 2, 3], "{n}");
        }
    }

    /// A write retried while it fails transiently asks the guard before every try, as the writer
    /// does: the lease lapsing between two tries of `prepare` (the third write) or `ensure_table`
    /// (the fourth) stops it there, and the write never lands.
    #[tokio::test]
    async fn a_lease_lost_while_a_write_is_retried_stops_the_retry() {
        for n in [3, 4] {
            let w = World::new(&["a"]);
            w.source.load(&t("a"), rows(1..=3));
            if n == 3 {
                for _ in 0..3 {
                    w.source
                        .fail(Method::Prepare, SourceError::Transient("down".into()));
                }
            } else {
                w.sink
                    .fail(SinkOp::EnsureTable, None, 3, || SinkError::Transient {
                        reason: "down".into(),
                    });
            }
            let mut d = deps(&w);
            d.guard = Arc::new(Countdown(AtomicUsize::new(n)));
            let (worker, _handle) = PipelineWorker::new(&mode("mode: initial"), d, fast());
            let end = tokio::time::timeout(Duration::from_secs(5), worker.run())
                .await
                .expect("the lease lapses within Phase 0");
            assert!(matches!(end, Err(WorkerError::LeaseLost)), "{n}: {end:?}");
            assert_eq!(
                phase_0_writes(&w).await,
                n - 1,
                "{n}: the retried write never landed"
            );
        }
    }

    /// A lost stream position is replaced only under the lease: `reset_position` recreates the
    /// slot, a write like Phase 0's.
    #[tokio::test]
    async fn a_lost_position_is_not_replaced_once_the_lease_lapsed() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let guard = Arc::new(crate::test_support::Switch::default());
        let mut d = deps(&w);
        d.guard = guard.clone();
        let end = run_with(&mode("mode: never"), d, async |h| {
            streamed(&h.status, &w).await;
            guard.0.store(true, std::sync::atomic::Ordering::SeqCst);
            w.source.fail(
                Method::ResetPosition,
                SourceError::Permanent("reset_position was called".into()),
            );
            w.source.lose_slot();
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            let mut st = h.status.clone();
            let _ = tokio::time::timeout(Duration::from_secs(5), st.wait_for(|_| false)).await;
        })
        .await;
        assert!(matches!(end, Err(WorkerError::LeaseLost)), "{end:?}");
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

    fn noted() -> TableSchema {
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

    /// A fresh start caches the baselined schemas before it persists its new checkpoint: a crash
    /// in between leaves the checkpoint before it, so the next start is fresh again rather than a
    /// resume under a stale cache, and the sink converges.
    #[tokio::test]
    async fn a_crash_between_a_fresh_starts_schemas_and_checkpoint_starts_fresh_again() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=2));
        let p = mode("mode: initial");
        run(&w, &p, async |status| streamed(&status, &w).await)
            .await
            .0
            .unwrap();
        let before = w.source.wal_end();
        w.source.alter(&t("a"), noted());
        let mut r = row(3, "x");
        r.0.push(("note".into(), ColumnValue::Text("n".into())));
        w.source.commit(FakeTxn::new().insert(&t("a"), r));
        w.source.lose_slot();
        let mut crashing = deps(&w);
        crashing.hooks = Arc::new(CrashAt(|p| matches!(p, HookPoint::SchemasCached)));
        let end = run_with(&p, crashing, async |_| {}).await;
        assert!(
            matches!(end, Err(WorkerError::Crashed(HookPoint::SchemasCached))),
            "{end:?}"
        );
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("a")), "cached before the crash");
        assert_eq!(
            SourcePosition::Pg(slot_lsn(&w).await),
            before,
            "not yet the checkpoint"
        );
        let (end, st) = run(&w, &p, async |status| streamed(&status, &w).await).await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert_eq!(
            w.source.prepares().last(),
            Some(&Some(before)),
            "the checkpoint before the crash: the start is fresh again"
        );
        assert!(
            !st.warnings
                .iter()
                .any(|m| m.contains("changed while the pipeline was down")),
            "{:?}",
            st.warnings
        );
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("a")));
        assert_eq!(w.ids("a"), [1, 2, 3]);
        assert_eq!(w.text("a", "note"), [None, None, Some("n".to_owned())]);
    }

    /// DA-SRC-25: on a source that compares column names without case (MySQL), a column whose
    /// catalog spelling changed case alone while the pipeline was down is the same column: no
    /// drift, and the cache keeps the sink's spelling. On PostgreSQL it is a change.
    #[tokio::test]
    async fn a_change_of_case_alone_is_no_drift_where_names_fold() {
        let recased = || {
            TableSchema::new(
                t("a"),
                vec![
                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("DOC", V::Text, "text"),
                ],
                vec!["id".into()],
            )
            .unwrap()
        };
        let drift = |st: &WorkerStatus| {
            st.warnings
                .iter()
                .any(|m| m.contains("changed while the pipeline was down"))
        };
        for fold in [true, false] {
            let w = World::new(&["a"]);
            w.source.load(&t("a"), rows(1..=2));
            let p = mode("mode: initial");
            run(&w, &p, async |status| streamed(&status, &w).await)
                .await
                .0
                .unwrap();
            if fold {
                w.source.fold_column_names();
            }
            w.source.alter(&t("a"), recased());
            let (end, st) = run(&w, &p, async |status| streamed(&status, &w).await).await;
            assert_eq!(end.unwrap(), Exit::Drained);
            assert_eq!(drift(&st), !fold, "fold {fold}: {:?}", st.warnings);
            if fold {
                let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
                assert_eq!(cached.columns()[1].name, "doc", "the sink's spelling");
            }
        }
    }

    /// Phase 0's rescan of the tables gone from the source's list warns once per failure streak:
    /// two failing starts warn once and halt nothing; a start whose rescan succeeds halts the
    /// dropped table and ends the streak, so the next failure warns again. A start with nothing
    /// gone ends the streak too.
    #[tokio::test]
    async fn a_failing_rescan_of_gone_tables_warns_once_per_streak() {
        let w = World::new(&["a", "b", "c", "d"]);
        w.started(&["a", "b", "c", "d"]).await;
        w.source.drop_table(&t("b"));
        let (mut core, _control, status) = w.core(&mode("mode: never"), &[]);
        let gone = || -> Vec<String> {
            status
                .borrow()
                .warnings
                .iter()
                .filter(|m| m.contains("the source no longer lists"))
                .cloned()
                .collect()
        };
        for _ in 0..2 {
            w.source.fail(
                Method::Rescan,
                SourceError::Transient("catalog busy".into()),
            );
            start(&mut core, None).await.unwrap();
        }
        let first = gone();
        assert_eq!(first.len(), 1, "{first:?}");
        assert!(
            first[0].contains("public.b") && first[0].contains("catalog busy"),
            "{first:?}"
        );
        assert!(w.docs.halts().await.unwrap().tables.is_empty());
        start(&mut core, None).await.unwrap();
        assert_eq!(gone(), first, "a rescan that succeeds warns nothing");
        let halts = w.docs.halts().await.unwrap().tables;
        assert!(
            matches!(&halts[..], [h] if h.table == t("b") && h.reason == HaltReason::TableDropped),
            "{halts:?}"
        );
        w.source.drop_table(&t("c"));
        w.source.fail(
            Method::Rescan,
            SourceError::Transient("catalog busy again".into()),
        );
        start(&mut core, None).await.unwrap();
        let second = gone();
        assert_eq!(second.len(), 2, "a new streak: {second:?}");
        assert!(
            second[1].contains("public.c") && second[1].contains("catalog busy again"),
            "{second:?}"
        );
        w.source.create_table(crate::test_support::schema("c"));
        start(&mut core, None).await.unwrap();
        assert_eq!(gone(), second, "nothing gone: no rescan, no warning");
        w.source.drop_table(&t("d"));
        w.source.fail(
            Method::Rescan,
            SourceError::Transient("catalog busy once more".into()),
        );
        start(&mut core, None).await.unwrap();
        let third = gone();
        assert_eq!(third.len(), 3, "nothing gone ended the streak: {third:?}");
        assert!(
            third[2].contains("public.d") && third[2].contains("catalog busy once more"),
            "{third:?}"
        );
    }

    /// A table that gained a column while the worker was down, snapshotted again under
    /// `always`: its reads wait for a rescan to bring the schema up to date, so the snapshot
    /// reads the new column (D-M6a.11), and no read happens under the old schema.
    #[tokio::test]
    async fn a_table_changed_while_down_is_rescanned_before_its_snapshot() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=1));
        let p = mode("mode: always");
        let (end, _) = run(&w, &p, async |status| streamed(&status, &w).await).await;
        assert_eq!(end.unwrap(), Exit::Drained);
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
        let mut r = row(1, "x");
        r.0.push(("note".into(), ColumnValue::Text("n".into())));
        w.source.load(&t("a"), vec![r]);
        let before = w.source.reads(&t("a"));
        let (end, st) = run(&w, &p, async |status| {
            wait_until("the snapshot is durable", || {
                let st = status.borrow();
                st.phase == Phase::Streaming
                    && st.snapshots.is_empty()
                    && w.text("a", "note") == [Some("n".to_owned())]
            })
            .await;
        })
        .await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert!(
            st.warnings
                .iter()
                .any(|m| m.contains("changed while the pipeline was down")),
            "{:?}",
            st.warnings
        );
        assert_eq!(
            w.source.reads(&t("a")) - before,
            2,
            "the chunk, then the empty one: none under the old schema"
        );
    }

    /// A re-snapshot after a lost position is timed from its own start, not from the run's first
    /// snapshot of the table (final review C1). The run's first snapshot of `a` began longer ago
    /// than `timeouts.snapshot`; Phase 0 reruns after a lost position, and the snapshot it
    /// requests runs without a `SNAPSHOT_TIMEOUT` halt. The old start is set directly, so only
    /// the re-snapshot runs under the limit, with seconds to spare, and `a`'s read is held until
    /// a tick has checked the limit: `b`, which an open writer blocks, is rechecked at the tick,
    /// after the limit check (re-review N4).
    #[tokio::test]
    async fn a_snapshot_after_a_lost_position_is_timed_from_its_own_start() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.source.load(&t("b"), rows(1..=2));
        let (mut core, control, status) = w.core(&mode("mode: initial"), &[]);
        let limit = std::time::Duration::from_secs(5);
        core.settings.snapshot_timeout = limit;
        // `a` reads while `b` waits for its writer.
        core.settings.parallelism = 2;
        let long_ago = Instant::now()
            .checked_sub(limit * 2)
            .expect("the clock is past ten seconds");
        core.snapshot_started.insert(t("a"), long_ago);
        w.source.lose_slot();
        let lost = core.settings.checkpoint.load(&core.docs).await.unwrap();
        let from = core
            .source
            .reset_position(&lost.unwrap().checkpoint)
            .await
            .unwrap();
        start(&mut core, Some(from)).await.unwrap();
        let xid = w.source.open_writer(&[t("b")]);
        w.source.hold_reads();
        let before = w.source.in_flights();
        let session = Session::open(&mut core).await.unwrap().unwrap();
        let driver = async {
            wait_until("a tick has rechecked `b`'s writer", || {
                w.source.in_flights() >= before + 2
            })
            .await;
            w.source.release_reads();
            w.source.abort_writer(xid);
            streamed(&status, &w).await;
            control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(session.run(), driver);
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        let st = status.borrow().clone();
        assert!(st.halted.is_empty(), "halted: {:?}", st.halted);
        assert_eq!((w.ids("a"), w.ids("b")), (vec![1, 2], vec![1, 2]));
        let doc = w.docs.table_snapshot(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(doc.status, SnapshotStatus::Completed);
    }

    /// A table Phase 0 halts after deciding to snapshot it gets no `in_progress` document (final
    /// review M5): the sink refuses to create it.
    #[tokio::test]
    async fn a_table_halted_in_phase_0_gets_no_snapshot_request() {
        let w = World::new(&["a", "b"]);
        w.sink.fail(
            data_avalanche_testkit::SinkOp::EnsureTable,
            Some(&t("a")),
            1,
            || SinkError::Permanent {
                reason: "refused".into(),
            },
        );
        let (mut core, _control, _status) = w.core(&mode("mode: initial"), &[]);
        start(&mut core, None).await.unwrap();
        let halts = w.docs.halts().await.unwrap().tables;
        assert!(
            matches!(&halts[..], [h] if h.table == t("a")
                && matches!(h.reason, HaltReason::SinkRejected { .. })),
            "{halts:?}"
        );
        assert_eq!(w.docs.table_snapshot(&t("a")).await.unwrap(), None);
        let b = w.docs.table_snapshot(&t("b")).await.unwrap().unwrap().doc;
        assert_eq!(b.status, SnapshotStatus::InProgress);
    }

    /// A pipeline with `sink` entries and pipeline-level `top` YAML (a line of its own).
    fn reloadable(sink: &str, top: &str) -> Pipeline {
        Yaml {
            tables: "\n      snapshots: { mode: never }",
            sink,
            pipeline: top,
            ..Yaml::default()
        }
        .build()
    }

    /// A reload reaches the running worker (config §1.2, class R): the new flush policy flushes
    /// the row the old one held, the rows carry the new name, and the source takes its settings,
    /// all in the same session.
    #[tokio::test]
    async fn a_reload_reaches_the_running_worker() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let held = reloadable("flush: { seconds: 3600, rows: 1000 }", "");
        let (worker, handle) = PipelineWorker::new(&held, deps(&w), fast());
        let driver = async {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(w.ids("a").is_empty(), "held by the flush policy");
            let mut next = reloadable("flush: { seconds: 0 }", "");
            next.name = "renamed".into();
            handle
                .control
                .send(Command::Reconfigure(Box::new(next)))
                .await
                .unwrap();
            wait_until("the held row lands", || w.ids("a") == [1]).await;
            assert_eq!(w.text("a", "_pipeline"), [Some("renamed".to_owned())]);
            assert_eq!(w.source.reconfigures(), 1);
            assert_eq!(w.source.opened().len(), 1, "the same session");
            handle.control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
    }

    /// A reload re-arms the session's timers: periodic rescans, off when the session opened, run
    /// at the new interval.
    #[tokio::test]
    async fn a_reload_rearms_the_rescans() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let off = "\n    schema_evolution: { rescan_interval: 0s }";
        let p = reloadable("flush: { seconds: 0 }", off);
        let (worker, handle) = PipelineWorker::new(&p, deps(&w), fast());
        let driver = async {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            wait_until("streaming", || w.ids("a") == [1]).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(w.source.rescans(), 0, "rescans are off");
            let every = "\n    schema_evolution: { rescan_interval: 1s }";
            let next = reloadable("flush: { seconds: 0 }", every);
            handle
                .control
                .send(Command::Reconfigure(Box::new(next)))
                .await
                .unwrap();
            wait_until("a rescan runs", || w.source.rescans() > 0).await;
            handle.control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
    }

    /// A buffer made smaller than a flush that waits for room lets it through once there is room
    /// under the new size: the wait is capped at the new size, so it can't wait for ever.
    #[tokio::test]
    async fn a_smaller_buffer_lets_a_waiting_flush_through() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = reloadable("flush: { seconds: 0 }, buffer_capacity_rows: 4", "");
        let (worker, handle) = PipelineWorker::new(&p, deps(&w), fast());
        let driver = async {
            w.sink.hold_commits();
            let mut four = FakeTxn::new();
            for id in 1..=4 {
                four = four.insert(&t("a"), row(id, "x"));
            }
            w.source.commit(four);
            wait_until("the first flush waits", || w.sink.waiting_commits() == 1).await;
            let mut three = FakeTxn::new();
            for id in 5..=7 {
                three = three.insert(&t("a"), row(id, "x"));
            }
            w.source.commit(three);
            tokio::time::sleep(Duration::from_millis(100)).await;
            let next = reloadable("flush: { seconds: 0 }, buffer_capacity_rows: 2", "");
            handle
                .control
                .send(Command::Reconfigure(Box::new(next)))
                .await
                .unwrap();
            w.sink.release_commits();
            wait_until("every row lands", || w.ids("a") == [1, 2, 3, 4, 5, 6, 7]).await;
            handle.control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
    }

    /// A reload that arrives between sessions (the reconnect's backoff) is the next session's.
    #[tokio::test]
    async fn a_reload_during_a_reconnect_reaches_the_next_session() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let tuning = Tuning {
            source_backoff: Backoff {
                first: Duration::from_millis(300),
                max: Duration::from_millis(300),
            },
            ..fast()
        };
        let p = reloadable("flush: { seconds: 0 }", "");
        let (worker, handle) = PipelineWorker::new(&p, deps(&w), tuning);
        let driver = async {
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
            wait_until("reconnecting", || {
                matches!(handle.status.borrow().phase, Phase::Reconnecting { .. })
            })
            .await;
            let mut next = reloadable("flush: { seconds: 0 }", "");
            next.name = "renamed".into();
            handle
                .control
                .send(Command::Reconfigure(Box::new(next)))
                .await
                .unwrap();
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            wait_until("the row lands", || w.ids("a") == [1]).await;
            assert_eq!(w.text("a", "_pipeline"), [Some("renamed".to_owned())]);
            assert_eq!(w.source.reconfigures(), 1);
            handle.control.send(Command::Drain).await.unwrap();
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
    }
}
