//! The sink writer (Data-Flow Phase 4, and Phase 5 up to the acknowledgment): one task that
//! commits each flush's change sets in order, then makes the flush durable and reports it. The
//! event loop acknowledges the stream only on [`WriterEvent::Durable`] (D-IMPL-2).
//!
//! A job becomes durable in this order: every change set committed (or skipped as covered), the
//! halts recorded, the checkpoint, the snapshot progress, the schema cache. A halt is recorded
//! before the checkpoint passes it. Snapshot progress and cached schemas are written once the
//! checkpoint covers what they describe. A crash between two steps leaves a state that a restart
//! replays correctly.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use data_avalanche_core::{
    Checkpoint, CommitMarker, CommitOutcome, HaltReason, ServerIdentity, Sink, SinkError,
    SourcePosition, TableChangeSet, TableRef, TableSchema,
};
use data_avalanche_state::{PipelineDocs, StateError, TableHalt, TableSnapshotDoc};
use data_avalanche_storage::StoreError;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::backoff::Backoff;
use crate::checkpoint::{Doc, Store};
use crate::{CommitGuard, HookAction, HookPoint, Hooks, Observer, WorkerError};

/// One flush and what becomes durable with it.
#[derive(Debug)]
pub(crate) struct Job {
    pub change_sets: Vec<TableChangeSet>,
    pub checkpoint: SourcePosition,
    /// Halts the event loop found since the last job, recorded before the checkpoint.
    pub halts: Vec<TableHalt>,
    /// Snapshot documents whose chunks this flush carries.
    pub snapshots: Vec<TableSnapshotDoc>,
    /// Schemas whose change this flush's checkpoint covers.
    pub schemas: Vec<TableSchema>,
    /// The rows this job holds of `buffer_capacity_rows`; released once it is durable.
    pub permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug)]
pub(crate) enum WriterEvent {
    /// The sink refused the table's changes as permanent: the table is halted, and the halt is
    /// recorded before the checkpoint that passes it.
    Halted(TableHalt),
    /// A commit or a state write failed and is being retried (`Some`, the last error), or
    /// succeeded again (`None`).
    Degraded(Option<String>),
    /// Everything up to the checkpoint is durable: acknowledge it to the source. Exactly one per
    /// job, in job order, a job with refused change sets included: the session counts them to
    /// know which jobs are durable (consecutive jobs may share a checkpoint).
    Durable(SourcePosition),
    /// The writer stopped; no further event follows.
    Failed(WorkerError),
}

#[derive(Clone)]
pub(crate) struct WriterCtx {
    pub sink: Arc<dyn Sink>,
    pub docs: PipelineDocs,
    /// The document that holds the checkpoint.
    pub store: Store,
    /// The server each checkpoint is recorded with (MySQL; `None` for PostgreSQL): the one the
    /// last start read the stream from.
    pub server: Option<ServerIdentity>,
    pub pipeline_id: String,
    pub owner_epoch: u64,
    pub guard: Arc<dyn CommitGuard>,
    pub hooks: Arc<dyn Hooks>,
    pub observer: Arc<dyn Observer>,
    pub backoff: Backoff,
}

impl std::fmt::Debug for WriterCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterCtx")
            .field("store", &self.store)
            .field("pipeline_id", &self.pipeline_id)
            .field("owner_epoch", &self.owner_epoch)
            .finish_non_exhaustive()
    }
}

/// Runs jobs until the job channel closes or one fails.
pub(crate) async fn run(
    ctx: WriterCtx,
    mut jobs: UnboundedReceiver<Job>,
    events: UnboundedSender<WriterEvent>,
) {
    let mut w = Writer {
        ctx,
        rejected: BTreeSet::new(),
        events,
        degraded: false,
    };
    while let Some(job) = jobs.recv().await {
        if let Err(e) = w.job(job).await {
            let _ = w.events.send(WriterEvent::Failed(e));
            return;
        }
    }
}

struct Writer {
    ctx: WriterCtx,
    /// Tables the sink refused: their later change sets and snapshot documents are dropped.
    rejected: BTreeSet<TableRef>,
    events: UnboundedSender<WriterEvent>,
    degraded: bool,
}

/// A failure worth retrying: the sink's transient class, or a storage failure whose write may not
/// have landed.
fn transient_state(e: &StateError) -> bool {
    matches!(
        e,
        StateError::Store(StoreError::Transient { .. } | StoreError::Contention { .. })
    )
}

impl Writer {
    fn hook(&self, point: HookPoint) -> Result<(), WorkerError> {
        match self.ctx.hooks.at(&point) {
            HookAction::Continue => Ok(()),
            HookAction::Crash => Err(WorkerError::Crashed(point)),
        }
    }

    /// Reports every failed attempt, and the first success after them.
    fn degraded(&mut self, why: Option<String>) {
        if why.is_some() || self.degraded {
            self.degraded = why.is_some();
            let _ = self.events.send(WriterEvent::Degraded(why));
        }
    }

    async fn job(&mut self, job: Job) -> Result<(), WorkerError> {
        let started = Instant::now();
        let rows = job.change_sets.iter().map(rows_of).sum();
        // A table the sink refused keeps that halt, its reason and its `at`: a halt the session
        // sends after it (one whose flush the sink refused, final review M1) would claim the
        // sink holds the refused rows.
        let mut halts: Vec<TableHalt> = job
            .halts
            .into_iter()
            .filter(|h| !self.rejected.contains(&h.table))
            .collect();
        for cs in job.change_sets {
            if self.rejected.contains(&cs.table) {
                continue;
            }
            let table = cs.table.clone();
            let at = cs.position.start.clone();
            match self.commit(cs).await? {
                Some(detail) => {
                    let halt = TableHalt {
                        table: table.clone(),
                        reason: HaltReason::SinkRejected { detail },
                        at,
                    };
                    self.rejected.insert(table);
                    let _ = self.events.send(WriterEvent::Halted(halt.clone()));
                    halts.push(halt);
                }
                None => self.hook(HookPoint::SinkCommitted { table })?,
            }
        }
        for halt in &halts {
            self.state(Write::Halt(halt)).await?;
        }
        self.save_checkpoint(&job.checkpoint).await?;
        self.hook(HookPoint::CheckpointPersisted {
            position: job.checkpoint.clone(),
        })?;
        // A refused table's progress isn't in the sink: its document stays as it was. The table
        // is in `rejected` already when this job's change set was the one refused.
        for doc in &job.snapshots {
            if self.rejected.contains(&doc.table) {
                continue;
            }
            self.state(Write::Snapshot(doc)).await?;
        }
        for schema in &job.schemas {
            self.state(Write::Schema(schema)).await?;
        }
        drop(job.permit);
        self.ctx.observer.flushed(rows, started.elapsed());
        let _ = self.events.send(WriterEvent::Durable(job.checkpoint));
        Ok(())
    }

    /// Commits one change set, retrying a transient failure. `Some(detail)` when the sink refused
    /// it as permanent: the table halts.
    async fn commit(&mut self, cs: TableChangeSet) -> Result<Option<String>, WorkerError> {
        let marker = CommitMarker {
            owner_epoch: self.ctx.owner_epoch,
            committed_position: cs.position.end.clone(),
            pipeline_id: self.ctx.pipeline_id.clone(),
        };
        let rows = rows_of(&cs);
        let observer = Arc::clone(&self.ctx.observer);
        let at = Instant::now();
        let mut attempt = 0;
        loop {
            if !self.ctx.guard.may_commit() {
                return Err(WorkerError::LeaseLost);
            }
            let outcome = self.ctx.sink.commit(cs.clone(), marker.clone()).await;
            if outcome.is_err() {
                observer.commit_failed();
            }
            match outcome {
                Ok(CommitOutcome::Committed { .. }) => {
                    observer.committed(rows, at.elapsed());
                    self.degraded(None);
                    return Ok(None);
                }
                Ok(CommitOutcome::SkippedCovered) => {
                    self.degraded(None);
                    return Ok(None);
                }
                Ok(CommitOutcome::Fenced { current_epoch }) => {
                    return Err(WorkerError::Fenced {
                        table: cs.table,
                        current_epoch,
                    });
                }
                Err(SinkError::Transient { reason }) => {
                    self.degraded(Some(format!("table {}: {reason}", cs.table)));
                    tokio::time::sleep(self.ctx.backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(SinkError::Permanent { reason }) => {
                    self.degraded(None);
                    return Ok(Some(reason));
                }
                Err(e) => return Err(WorkerError::Sink(e)),
            }
        }
    }

    async fn save_checkpoint(&mut self, checkpoint: &SourcePosition) -> Result<(), WorkerError> {
        let doc = self.ctx.store.doc(&Checkpoint {
            position: checkpoint.clone(),
            server: self.ctx.server.clone(),
        })?;
        if !self.ctx.guard.may_commit() {
            return Err(WorkerError::LeaseLost);
        }
        self.state(Write::Checkpoint(&doc)).await
    }

    /// A state write, retried while it fails transiently.
    async fn state(&mut self, write: Write<'_>) -> Result<(), WorkerError> {
        let mut attempt = 0;
        loop {
            // Checked before every attempt, like a commit's: a writer that lost its lease while a
            // write kept failing must not land it later, behind the new owner's (a checkpoint
            // write overwrites).
            if !self.ctx.guard.may_commit() {
                return Err(WorkerError::LeaseLost);
            }
            let docs = &self.ctx.docs;
            let r = match write {
                Write::Halt(h) => docs.record_halt(h).await.map(drop),
                Write::Checkpoint(d) => d.save(docs).await,
                Write::Snapshot(d) => docs.save_table_snapshot(d).await.map(drop),
                Write::Schema(s) => docs.save_table_schema(s).await.map(drop),
            };
            match r {
                Ok(()) => {
                    self.degraded(None);
                    return Ok(());
                }
                Err(e) if transient_state(&e) => {
                    self.degraded(Some(e.to_string()));
                    tokio::time::sleep(self.ctx.backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

/// A change set's rows: its upserts and its deletes.
fn rows_of(cs: &TableChangeSet) -> u64 {
    (cs.upsert_rows() + cs.delete_rows()) as u64
}

/// One state document a job writes.
#[derive(Clone, Copy)]
enum Write<'a> {
    Halt(&'a TableHalt),
    Checkpoint(&'a Doc),
    Snapshot(&'a TableSnapshotDoc),
    Schema(&'a TableSchema),
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{SinkTableState, TableSpec};
    use data_avalanche_state::{PositionDoc, SLOT_KEY, SnapshotCursor, SnapshotStatus};
    use data_avalanche_testkit::{MemorySink, SinkOp};
    use tokio::sync::mpsc;

    use super::*;
    use crate::NoHooks;
    use crate::test_support::{
        CrashAt, Ears, FlakyStore, Heard, Switch, accumulator, docs, lsn, schema, txn,
    };

    const FAST: Backoff = Backoff {
        first: Duration::from_millis(1),
        max: Duration::from_millis(4),
    };

    struct Rig {
        sink: MemorySink,
        docs: PipelineDocs,
        store: Arc<FlakyStore>,
        guard: Arc<Switch>,
        /// The checkpoint's document, and the server it is recorded with.
        checkpoints: Store,
        server: Option<ServerIdentity>,
        ears: Arc<Ears>,
    }

    async fn rig(tables: &[&str]) -> Rig {
        let sink = MemorySink::new();
        for t in tables {
            sink.ensure_table(&TableSpec::new(&schema(t), vec![]).unwrap())
                .await
                .unwrap();
        }
        let (docs, store) = docs();
        Rig {
            sink,
            docs,
            store,
            guard: Arc::default(),
            checkpoints: Store::Slot {
                name: "p_slot".into(),
                intent: None,
            },
            server: None,
            ears: Arc::default(),
        }
    }

    /// A lease that lapses after `n` checks.
    #[derive(Debug)]
    struct Lapses(AtomicUsize);

    impl CommitGuard for Lapses {
        fn may_commit(&self) -> bool {
            self.0
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        }
    }

    impl Rig {
        /// Runs the writer over `jobs` to the end and returns its events.
        async fn run(&self, jobs: Vec<Job>, hooks: Arc<dyn Hooks>) -> Vec<WriterEvent> {
            self.run_guarded(jobs, hooks, self.guard.clone()).await
        }

        async fn run_guarded(
            &self,
            jobs: Vec<Job>,
            hooks: Arc<dyn Hooks>,
            guard: Arc<dyn CommitGuard>,
        ) -> Vec<WriterEvent> {
            let ctx = WriterCtx {
                sink: Arc::new(self.sink.clone()),
                docs: self.docs.clone(),
                store: self.checkpoints.clone(),
                server: self.server.clone(),
                pipeline_id: "p".into(),
                owner_epoch: 1,
                guard,
                hooks,
                observer: self.ears.clone(),
                backoff: FAST,
            };
            let (jobs_tx, jobs_rx) = mpsc::unbounded_channel();
            let (events_tx, mut events_rx) = mpsc::unbounded_channel();
            for j in jobs {
                jobs_tx.send(j).unwrap();
            }
            drop(jobs_tx);
            run(ctx, jobs_rx, events_tx).await;
            let mut out = vec![];
            while let Some(e) = events_rx.recv().await {
                out.push(e);
            }
            out
        }

        async fn checkpoint(&self) -> Option<Lsn> {
            self.docs
                .slot()
                .await
                .unwrap()
                .map(|v| v.doc.confirmed_flush_lsn)
        }

        async fn state(&self, t: &str) -> SinkTableState {
            self.sink
                .table_state(&TableRef::new("public", t))
                .await
                .unwrap()
        }
    }

    fn job(flush: crate::Flush) -> Job {
        Job {
            change_sets: flush.change_sets,
            checkpoint: flush.checkpoint,
            halts: vec![],
            snapshots: vec![],
            schemas: vec![],
            permit: None,
        }
    }

    fn snapshot_doc(t: &str) -> TableSnapshotDoc {
        TableSnapshotDoc {
            table: TableRef::new("public", t),
            status: SnapshotStatus::Completed,
            start_position: Some(lsn(100)),
            cursor: None,
            row_count: Some(1),
            completed_at: None,
        }
    }

    fn rows(r: &Rig, t: &str) -> usize {
        r.sink
            .content(&TableRef::new("public", t))
            .map_or(0, |b| b.num_rows())
    }

    #[tokio::test]
    async fn a_job_commits_then_records_halts_checkpoint_progress_and_schemas() {
        let r = rig(&["a", "b"]).await;
        let mut acc = accumulator(&["a", "b"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x"), ("b", 1, "y")]);
        let mut j = job(acc.flush());
        let overflow = TableHalt {
            table: TableRef::new("public", "c"),
            reason: HaltReason::NumericOverflow { column: "n".into() },
            at: lsn(100),
        };
        j.halts.push(overflow.clone());
        j.snapshots.push(snapshot_doc("a"));
        j.schemas.push(schema("a"));
        let events = r.run(vec![j], Arc::new(NoHooks)).await;
        assert!(
            matches!(&events[..], [WriterEvent::Durable(p)] if *p == lsn(200)),
            "{events:?}"
        );
        assert_eq!((rows(&r, "a"), rows(&r, "b")), (1, 1));
        let marker = r.state("a").await.marker.unwrap();
        assert_eq!(
            (
                marker.owner_epoch,
                marker.committed_position,
                marker.pipeline_id.as_str()
            ),
            (1, lsn(200), "p")
        );
        assert_eq!(r.checkpoint().await, Some(Lsn(200)));
        assert_eq!(r.docs.halts().await.unwrap().tables, vec![overflow]);
        let a = TableRef::new("public", "a");
        assert!(r.docs.table_snapshot(&a).await.unwrap().is_some());
        assert_eq!(
            r.docs.table_schema(&a).await.unwrap().unwrap().doc,
            schema("a")
        );
    }

    /// A MySQL pipeline's checkpoint is the position document, with the server the stream reads.
    #[tokio::test]
    async fn a_mysql_checkpoint_is_recorded_with_its_server() {
        let mut r = rig(&[]).await;
        let server = ServerIdentity::Mariadb { server_id: 7 };
        r.checkpoints = Store::Position;
        r.server = Some(server.clone());
        let at = SourcePosition::parse_tagged("mariadb-gtid:0-7-42").unwrap();
        let j = Job {
            change_sets: vec![],
            checkpoint: at.clone(),
            halts: vec![],
            snapshots: vec![],
            schemas: vec![],
            permit: None,
        };
        let events = r.run(vec![j], Arc::new(NoHooks)).await;
        assert!(
            matches!(&events[..], [WriterEvent::Durable(p)] if *p == at),
            "{events:?}"
        );
        let doc = r.docs.position().await.unwrap().unwrap().doc;
        assert_eq!(
            doc,
            PositionDoc {
                position: at,
                server
            }
        );
        assert!(r.docs.slot().await.unwrap().is_none(), "no slot document");
    }

    #[tokio::test]
    async fn a_crash_after_the_commit_leaves_the_checkpoint_behind() {
        let r = rig(&["a"]).await;
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let mut j = job(acc.flush());
        j.schemas.push(schema("a"));
        let crash = CrashAt(|p| matches!(p, HookPoint::SinkCommitted { .. }));
        let events = r.run(vec![j], Arc::new(crash)).await;
        assert!(
            matches!(&events[..], [WriterEvent::Failed(WorkerError::Crashed(_))]),
            "{events:?}"
        );
        assert_eq!(rows(&r, "a"), 1);
        assert_eq!(r.checkpoint().await, None, "the replay re-commits it");
    }

    #[tokio::test]
    async fn a_crash_after_the_checkpoint_leaves_progress_and_schemas_behind() {
        let r = rig(&["a"]).await;
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let mut j = job(acc.flush());
        j.snapshots.push(snapshot_doc("a"));
        j.schemas.push(schema("a"));
        let crash = CrashAt(|p| matches!(p, HookPoint::CheckpointPersisted { .. }));
        r.run(vec![j], Arc::new(crash)).await;
        assert_eq!(r.checkpoint().await, Some(Lsn(200)));
        let a = TableRef::new("public", "a");
        assert!(r.docs.table_snapshot(&a).await.unwrap().is_none());
        assert!(r.docs.table_schema(&a).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_transient_commit_is_retried_while_degraded() {
        let r = rig(&["a"]).await;
        r.sink
            .fail(SinkOp::Commit, None, 2, || SinkError::Transient {
                reason: "throttled".into(),
            });
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let events = r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        let shape: Vec<_> = events
            .iter()
            .map(|e| match e {
                WriterEvent::Degraded(Some(why)) => {
                    assert!(why.contains("throttled"), "{why}");
                    "degraded"
                }
                WriterEvent::Degraded(None) => "recovered",
                WriterEvent::Durable(_) => "durable",
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(shape, ["degraded", "degraded", "recovered", "durable"]);
        assert_eq!(rows(&r, "a"), 1);
    }

    /// Each commit that landed reports its rows, then the flush reports them all once it is
    /// durable. A set the sink already holds writes nothing: no commit is reported, the flush is.
    #[tokio::test]
    async fn a_flush_reports_each_commit_then_itself() {
        let r = rig(&["a", "b"]).await;
        let mut acc = accumulator(&["a", "b"], 100);
        txn(
            &mut acc,
            1,
            200,
            &[("a", 1, "x"), ("a", 2, "x"), ("b", 1, "y")],
        );
        let f = acc.flush();
        let again = job(crate::Flush {
            change_sets: f.change_sets.clone(),
            checkpoint: f.checkpoint.clone(),
        });
        r.run(vec![job(f), again], Arc::new(NoHooks)).await;
        assert_eq!(
            r.ears.heard(),
            [
                Heard::Committed { rows: 2 },
                Heard::Committed { rows: 1 },
                Heard::Flushed { rows: 3 },
                Heard::Flushed { rows: 3 },
            ]
        );
    }

    /// A commit's time covers its retries and their backoff (D-M10.2): three transient failures
    /// wait at least 0.5 + 1 + 2 ms before the attempt that lands.
    #[tokio::test]
    async fn a_commit_is_timed_with_its_retries() {
        let r = rig(&["a"]).await;
        r.sink
            .fail(SinkOp::Commit, None, 3, || SinkError::Transient {
                reason: "throttled".into(),
            });
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        let times = r.ears.commit_times();
        assert_eq!(times.len(), 1);
        assert!(
            times[0] >= Duration::from_micros(3500),
            "{:?} leaves out the backoff",
            times[0]
        );
    }

    /// Every failed attempt counts, the retried ones and the refusal.
    #[tokio::test]
    async fn every_failed_commit_attempt_is_reported() {
        let r = rig(&["a", "b"]).await;
        r.sink.fail(
            SinkOp::Commit,
            Some(&TableRef::new("public", "a")),
            2,
            || SinkError::Transient {
                reason: "throttled".into(),
            },
        );
        r.sink.fail(
            SinkOp::Commit,
            Some(&TableRef::new("public", "b")),
            1,
            || SinkError::Permanent {
                reason: "column mismatch".into(),
            },
        );
        let mut acc = accumulator(&["a", "b"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x"), ("b", 1, "y")]);
        r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        assert_eq!(
            r.ears.heard(),
            [
                Heard::CommitFailed,
                Heard::CommitFailed,
                Heard::Committed { rows: 1 },
                Heard::CommitFailed,
                Heard::Flushed { rows: 2 },
            ]
        );
    }

    #[tokio::test]
    async fn a_transient_state_write_is_retried_while_degraded() {
        let r = rig(&["a"]).await;
        r.store.fail_writes(SLOT_KEY, 2);
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let events = r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        assert_eq!(events.len(), 4, "{events:?}");
        assert!(matches!(events[3], WriterEvent::Durable(_)));
        assert_eq!(r.checkpoint().await, Some(Lsn(200)));
    }

    #[tokio::test]
    async fn a_refused_change_set_halts_its_table_and_its_later_sets_are_dropped() {
        let r = rig(&["a", "b"]).await;
        let a = TableRef::new("public", "a");
        r.sink
            .fail(SinkOp::Commit, Some(&a), 1, || SinkError::Permanent {
                reason: "column mismatch".into(),
            });
        let mut acc = accumulator(&["a", "b"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x"), ("b", 1, "y")]);
        let first = job(acc.flush());
        txn(&mut acc, 2, 300, &[("a", 2, "x"), ("b", 2, "y")]);
        let second = job(acc.flush());
        let events = r.run(vec![first, second], Arc::new(NoHooks)).await;
        let halt = TableHalt {
            table: a.clone(),
            reason: HaltReason::SinkRejected {
                detail: "column mismatch".into(),
            },
            at: lsn(100),
        };
        assert!(
            matches!(&events[..], [WriterEvent::Halted(h), WriterEvent::Durable(_), WriterEvent::Durable(p)] if *h == halt && *p == lsn(300)),
            "{events:?}"
        );
        assert_eq!((rows(&r, "a"), rows(&r, "b")), (0, 2));
        assert_eq!(r.docs.halts().await.unwrap().tables, vec![halt]);
    }

    fn progress_doc(t: &str, rows_done: u64) -> TableSnapshotDoc {
        TableSnapshotDoc {
            status: SnapshotStatus::InProgress,
            row_count: None,
            cursor: Some(SnapshotCursor {
                key: vec![],
                chunk_size: 1,
                rows_done,
                collation: None,
            }),
            ..snapshot_doc(t)
        }
    }

    /// A dump table's chunk the sink refuses: the table's snapshot documents in that job and the
    /// later ones are dropped, so it keeps its last one from before the refusal (never
    /// `Completed`), while the other table's documents advance. Each job is still durable once.
    #[tokio::test]
    async fn a_refused_tables_snapshot_docs_are_dropped_with_its_change_sets() {
        let r = rig(&["a", "b"]).await;
        let (a, b) = (TableRef::new("public", "a"), TableRef::new("public", "b"));
        let mut acc = accumulator(&["a", "b"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x"), ("b", 1, "y")]);
        let mut first = job(acc.flush());
        first.snapshots = vec![progress_doc("a", 1), progress_doc("b", 1)];
        r.run(vec![first], Arc::new(NoHooks)).await;
        r.sink
            .fail(SinkOp::Commit, Some(&a), 1, || SinkError::Permanent {
                reason: "refused".into(),
            });
        txn(&mut acc, 2, 300, &[("a", 2, "x"), ("b", 2, "y")]);
        let mut refused = job(acc.flush());
        refused.snapshots = vec![progress_doc("a", 2), progress_doc("b", 2)];
        txn(&mut acc, 3, 400, &[("b", 3, "y")]);
        let mut last = job(acc.flush());
        last.snapshots = vec![snapshot_doc("a"), snapshot_doc("b")];
        let events = r.run(vec![refused, last], Arc::new(NoHooks)).await;
        assert!(
            matches!(
                &events[..],
                [WriterEvent::Halted(h), WriterEvent::Durable(_), WriterEvent::Durable(p)]
                    if h.table == a && *p == lsn(400)
            ),
            "{events:?}"
        );
        let doc_a = r.docs.table_snapshot(&a).await.unwrap().unwrap().doc;
        assert_eq!(doc_a, progress_doc("a", 1), "the last before the refusal");
        let doc_b = r.docs.table_snapshot(&b).await.unwrap().unwrap().doc;
        assert_eq!(doc_b, snapshot_doc("b"));
    }

    #[tokio::test]
    async fn a_fenced_commit_stops_the_writer() {
        let r = rig(&["a"]).await;
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let first = job(acc.flush());
        txn(&mut acc, 2, 300, &[("a", 2, "x")]);
        let second = job(acc.flush());
        r.run(vec![first], Arc::new(NoHooks)).await;
        r.sink.take_over(&TableRef::new("public", "a"), 2);
        let events = r.run(vec![second], Arc::new(NoHooks)).await;
        assert!(
            matches!(
                &events[..],
                [WriterEvent::Failed(WorkerError::Fenced {
                    current_epoch: 2,
                    ..
                })]
            ),
            "{events:?}"
        );
        assert_eq!(r.checkpoint().await, Some(Lsn(200)));
    }

    #[tokio::test]
    async fn a_lost_lease_stops_the_writer_before_it_commits() {
        let r = rig(&["a"]).await;
        r.guard.0.store(true, Ordering::SeqCst);
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let events = r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        assert!(
            matches!(&events[..], [WriterEvent::Failed(WorkerError::LeaseLost)]),
            "{events:?}"
        );
        assert_eq!(rows(&r, "a"), 0);
    }

    #[tokio::test]
    async fn a_lease_lost_while_a_state_write_retries_stops_the_writer() {
        let r = rig(&["a"]).await;
        r.store.fail_writes(SLOT_KEY, 50);
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        // The commit and the checkpoint's first two attempts pass the guard; the lease lapses
        // while the slot write is being retried.
        let guard = Arc::new(Lapses(AtomicUsize::new(4)));
        let events = r
            .run_guarded(vec![job(acc.flush())], Arc::new(NoHooks), guard)
            .await;
        assert!(
            matches!(
                events.last(),
                Some(WriterEvent::Failed(WorkerError::LeaseLost))
            ),
            "{events:?}"
        );
        assert_eq!(rows(&r, "a"), 1);
        assert_eq!(
            r.checkpoint().await,
            None,
            "no checkpoint without the lease"
        );
    }

    #[tokio::test]
    async fn a_missing_table_stops_the_writer() {
        let r = rig(&[]).await;
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let events = r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        assert!(
            matches!(
                &events[..],
                [WriterEvent::Failed(WorkerError::Sink(
                    SinkError::NoTable { .. }
                ))]
            ),
            "{events:?}"
        );
        assert_eq!(r.checkpoint().await, None);
    }
}
