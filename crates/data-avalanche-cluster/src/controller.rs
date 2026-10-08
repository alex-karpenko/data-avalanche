//! One pipeline's controller (Clustering §20, Pipeline_Lifecycle §5): the only writer of the
//! pipeline's lease. While its target is "own" it claims the lease, then runs the pipeline's
//! worker under it and reconciles the worker toward the persisted desired state, polled every
//! `lease_renewal_interval` (Q-M6b-2). A worker that ends with an error is rerun, stopped for the
//! lease, or left in `ERROR` (Q-M6b-3).

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use data_avalanche_config::Pipeline;
use data_avalanche_core::{Sink, SourceConnector};
use data_avalanche_pipeline::{
    Backoff, Command, Exit, Hooks, Phase, PipelineWorker, Recovery, Tuning, WorkerDeps,
    WorkerError, WorkerHandle, WorkerStatus,
};
use data_avalanche_state::{DesiredState, Holder, Leases, PipelineDocs};
use data_avalanche_telemetry::{Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, Totals};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::{Claim, Claimed, LeaseRun, LeaseTiming, RenewEnd, RenewState, release};

/// A worker's source and sink, built afresh for every run.
#[derive(Clone)]
pub struct Parts {
    pub source: Arc<dyn SourceConnector>,
    pub sink: Arc<dyn Sink>,
    pub hooks: Arc<dyn Hooks>,
}

impl Debug for Parts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Parts").finish_non_exhaustive()
    }
}

/// Builds a pipeline's [`Parts`]: the binary links the real source and sink.
pub trait WorkerFactory: Send + Sync + Debug {
    /// # Errors
    /// The parts can't be built from the config: the pipeline goes to `ERROR`.
    fn build(&self, pipeline: &Pipeline) -> Result<Parts, String>;
}

/// What the reconciler wants of a controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    Own,
    DontOwn,
}

/// The pipeline's actual state, as this node sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Actual {
    /// Not owned here: `holder` has the lease, or the target is "don't own".
    Standby {
        holder: Option<String>,
    },
    /// The worker runs Phase 0.
    Starting,
    Running,
    Paused,
    Stopped,
    /// Every table is a `dump_*` table whose snapshot is done: the worker ended.
    Dumped,
    /// The worker ended with an error worth another run; it reruns after a backoff.
    Retrying {
        error: String,
    },
    /// The worker ended with an error an operator must act on (Q-M6b-3): no rerun until the
    /// process restarts, or a stop then a resume.
    Error {
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ControllerStatus {
    pub actual: Option<Actual>,
    pub owner_epoch: Option<u64>,
    pub lease: Option<RenewState>,
    pub worker: Option<WorkerStatus>,
}

/// A controller's timings.
#[derive(Debug, Clone)]
pub struct ControllerSettings {
    pub timing: LeaseTiming,
    /// How long a stop or a hand-off waits for the worker to drain before it is cut off.
    pub drain_timeout: Duration,
    /// Between reruns of a worker that ended with a transient error.
    pub restart: Backoff,
    pub tuning: Tuning,
}

/// What every controller of a node shares.
#[derive(Debug, Clone)]
pub struct ControllerDeps {
    /// The leases in `globals.state`.
    pub leases: Leases,
    /// This node as a lease holder.
    pub holder: Holder,
    pub factory: Arc<dyn WorkerFactory>,
    /// The node's registry.
    pub metrics: Metrics,
    pub settings: ControllerSettings,
    /// Caps the workers in Phase 0 at once (`max_concurrent_takeovers`).
    pub starts: Arc<Semaphore>,
}

/// One pipeline's controller.
#[derive(Debug)]
pub struct PipelineController {
    pipeline: Pipeline,
    docs: PipelineDocs,
    leases: Leases,
    holder: Holder,
    factory: Arc<dyn WorkerFactory>,
    node: Metrics,
    metrics: Arc<PipelineMetrics>,
    settings: ControllerSettings,
    starts: Arc<Semaphore>,
    status: watch::Sender<ControllerStatus>,
}

/// Why the owned phase ended.
enum Left {
    /// The lease is gone: claim it again.
    Lost,
    /// The target became "don't own": the lease is released.
    Released,
    /// The target channel closed: the process is shutting down.
    Shutdown,
}

/// What woke the owned loop.
enum Event {
    Lost,
    Target { closed: bool },
    Ended(Result<Result<Exit, WorkerError>, tokio::task::JoinError>),
    Status { open: bool },
    Start(OwnedSemaphorePermit),
    Due,
    Poll,
}

/// A running worker.
struct Running {
    handle: WorkerHandle,
    task: JoinHandle<Result<Exit, WorkerError>>,
    sink: Arc<dyn Sink>,
    /// Held while the worker is in Phase 0.
    permit: Option<OwnedSemaphorePermit>,
    /// A pause was sent and not yet undone.
    paused: bool,
    /// The warnings already logged.
    logged: HashSet<String>,
}

impl PipelineController {
    /// `docs` are the pipeline's state documents.
    pub fn new(
        pipeline: Pipeline,
        docs: PipelineDocs,
        deps: &ControllerDeps,
    ) -> (Self, watch::Receiver<ControllerStatus>) {
        let (status, rx) = watch::channel(ControllerStatus::default());
        let labels = PipelineLabels {
            pipeline: pipeline.name.clone(),
            source_host: pipeline.source.host.clone(),
            source_db: pipeline.source.db.clone(),
        };
        let metrics = Arc::new(
            deps.metrics
                .pipeline(labels, pipeline.sink.metrics.per_table),
        );
        (
            Self {
                pipeline,
                docs,
                leases: deps.leases.clone(),
                holder: deps.holder.clone(),
                factory: Arc::clone(&deps.factory),
                node: deps.metrics.clone(),
                metrics,
                settings: deps.settings.clone(),
                starts: Arc::clone(&deps.starts),
                status,
            },
            rx,
        )
    }

    /// Runs until the target channel closes; the lease is released on the way out.
    pub async fn run(self, mut target: watch::Receiver<Target>) {
        let mut claim = Claim::new(
            self.leases.clone(),
            &self.pipeline.id,
            self.holder.clone(),
            self.settings.timing,
        );
        loop {
            if *target.borrow_and_update() == Target::DontOwn {
                self.standby(None);
                if target.changed().await.is_err() {
                    return;
                }
                continue;
            }
            let wait = match claim.step().await {
                Ok(Claimed::Mine { lease, written }) => {
                    tracing::info!(
                        pipeline = %self.pipeline.name,
                        owner_epoch = lease.doc.owner_epoch,
                        "lease acquired"
                    );
                    let run = LeaseRun::start(
                        self.leases.clone(),
                        lease,
                        self.holder.clone(),
                        self.settings.timing,
                        written,
                        Arc::clone(&self.metrics),
                    );
                    match self.owned(run, &mut target).await {
                        Left::Shutdown => return,
                        // Not this node's until a step says otherwise, though claiming fails.
                        Left::Lost => {
                            self.standby(None);
                            continue;
                        }
                        Left::Released => continue,
                    }
                }
                Ok(Claimed::HeldBy {
                    node_id,
                    takeover_at,
                }) => {
                    self.standby(Some(node_id));
                    takeover_at.min(Instant::now() + self.settings.timing.renewal_interval)
                }
                Err(e) => {
                    tracing::warn!(pipeline = %self.pipeline.name, "lease claim failed: {e}");
                    Instant::now() + self.settings.timing.renewal_interval
                }
            };
            tokio::select! {
                () = tokio::time::sleep_until(wait) => {}
                r = target.changed() => if r.is_err() { return },
            }
        }
    }

    /// Holds the lease and reconciles the worker until the lease is lost or let go.
    async fn owned(&self, run: LeaseRun, target: &mut watch::Receiver<Target>) -> Left {
        self.node.owned_by(1);
        let left = self.reconcile(run, target).await;
        self.node.owned_by(-1);
        left
    }

    async fn reconcile(&self, run: LeaseRun, target: &mut watch::Receiver<Target>) -> Left {
        let mut worker: Option<Running> = None;
        let mut error: Option<String> = None;
        let mut dumped = false;
        let mut retry: Option<(Instant, String)> = None;
        let mut failures: u32 = 0;
        // Unknown until a read succeeds: nothing starts on a guess (a stopped or paused pipeline
        // must not run because its state document was unreadable once).
        let mut desired = self.desired().await;
        let mut poll = tokio::time::interval(self.settings.timing.renewal_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        poll.tick().await;
        loop {
            if desired == Some(DesiredState::Stopped) {
                if let Some(w) = worker.take() {
                    // Cut off or not, the worker and its writer are gone: the lease stays held.
                    self.drain(w).await;
                }
                // A stop clears an error: the resume after it runs the pipeline again.
                error = None;
                dumped = false;
                retry = None;
                failures = 0;
            }
            if let Some(w) = worker.as_mut() {
                match desired {
                    Some(DesiredState::Paused) if !w.paused => {
                        let _ = w.handle.control.send(Command::Pause).await;
                        w.paused = true;
                    }
                    Some(DesiredState::Running) if w.paused => {
                        let _ = w.handle.control.send(Command::Resume).await;
                        w.paused = false;
                    }
                    _ => {}
                }
            }
            let due = retry.as_ref().map(|(at, _)| *at);
            // A worker may run: no error, not dumped, and a desired state read that isn't
            // "stopped". Only then does a retry's time matter (else its arm would be ready at
            // every turn).
            let may_run = worker.is_none()
                && error.is_none()
                && !dumped
                && desired.is_some_and(|d| d != DesiredState::Stopped);
            let want_start = may_run && due.is_none_or(|at| Instant::now() >= at);
            self.publish(
                &run,
                worker.as_ref(),
                desired,
                error.as_deref(),
                dumped,
                retry.as_ref().map(|(_, e)| e.as_str()),
            );
            let starts = Arc::clone(&self.starts);
            let event = {
                let (task, status) = match worker.as_mut() {
                    Some(Running { task, handle, .. }) => (Some(task), Some(&mut handle.status)),
                    None => (None, None),
                };
                tokio::select! {
                    biased;
                    () = run.guard.lost() => Event::Lost,
                    r = target.changed() => Event::Target { closed: r.is_err() },
                    ended = async {
                        match task {
                            Some(task) => task.await,
                            None => std::future::pending().await,
                        }
                    } => Event::Ended(ended),
                    changed = async {
                        match status {
                            Some(status) => status.changed().await.is_ok(),
                            None => std::future::pending().await,
                        }
                    } => Event::Status { open: changed },
                    permit = starts.acquire_owned(), if want_start => {
                        Event::Start(permit.expect("the start semaphore isn't closed"))
                    }
                    () = async {
                        match due {
                            Some(at) => tokio::time::sleep_until(at).await,
                            None => std::future::pending().await,
                        }
                    }, if may_run && !want_start => Event::Due,
                    _ = poll.tick() => Event::Poll,
                }
            };
            match event {
                Event::Lost => {
                    if let Some(w) = worker.take() {
                        w.task.abort();
                        // Ended (dropped) before the next term starts: its writer goes with it.
                        let _ = w.task.await;
                        self.metrics.worker_ended();
                    }
                    let reason = match run.stop().await {
                        RenewEnd::Lost { reason } => reason,
                        RenewEnd::Stopped(_) => "the lease's deadline passed".into(),
                    };
                    tracing::error!(pipeline = %self.pipeline.name, "lease lost: {reason}");
                    return Left::Lost;
                }
                Event::Target { closed } => {
                    if closed || *target.borrow() == Target::DontOwn {
                        let drained = match worker.take() {
                            Some(w) => self.drain(w).await,
                            None => true,
                        };
                        if drained {
                            self.give_back(run).await;
                        } else {
                            // Clustering §8.3: a worker cut off may have left a write in
                            // flight; the lease expires instead of being handed over now.
                            let _ = run.stop().await;
                            tracing::warn!(
                                pipeline = %self.pipeline.name,
                                "lease left to expire after the drain was cut off"
                            );
                        }
                        self.standby(None);
                        return if closed {
                            Left::Shutdown
                        } else {
                            Left::Released
                        };
                    }
                }
                Event::Ended(ended) => {
                    let mut w = worker.take().expect("a worker ended");
                    self.observe(&mut w);
                    self.metrics.worker_ended();
                    let ended = ended.unwrap_or_else(|e| {
                        Err(WorkerError::Refused(format!(
                            "the worker task ended abnormally: {e}"
                        )))
                    });
                    match ended {
                        Ok(Exit::Dumped) => dumped = true,
                        Ok(Exit::Drained) => {
                            retry = Some((Instant::now(), "the worker drained unasked".into()));
                        }
                        Err(e) => match e.recovery() {
                            Recovery::Retry => {
                                let at = Instant::now() + self.settings.restart.delay(failures);
                                failures = failures.saturating_add(1);
                                tracing::warn!(
                                    pipeline = %self.pipeline.name,
                                    "worker ended, rerun after a backoff: {e}"
                                );
                                retry = Some((at, e.to_string()));
                            }
                            Recovery::Lease => {
                                tracing::error!(
                                    pipeline = %self.pipeline.name,
                                    "worker stopped for the lease: {e}"
                                );
                                let _ = run.stop().await;
                                return Left::Lost;
                            }
                            Recovery::Error => {
                                tracing::error!(
                                    pipeline = %self.pipeline.name,
                                    "pipeline in ERROR: {e}"
                                );
                                error = Some(e.to_string());
                            }
                        },
                    }
                }
                Event::Status { open } => {
                    if open && let Some(w) = worker.as_mut() {
                        if w.handle.status.borrow().phase != Phase::Starting {
                            // Phase 0 is over: another worker may start, and the run counts
                            // as a success.
                            w.permit = None;
                            failures = 0;
                            retry = None;
                        }
                        self.observe(w);
                    }
                }
                Event::Start(permit) => match self.start(&run, permit) {
                    Ok(w) => worker = Some(w),
                    Err(e) => {
                        tracing::error!(pipeline = %self.pipeline.name, "pipeline in ERROR: {e}");
                        error = Some(e);
                    }
                },
                Event::Due => {}
                Event::Poll => {
                    desired = self.desired().await.or(desired);
                    if let Some(w) = worker.as_mut() {
                        self.observe(w);
                    }
                }
            }
        }
    }

    /// The persisted desired state, `None` on a failed read. A pipeline never started has none
    /// yet: it runs.
    async fn desired(&self) -> Option<DesiredState> {
        match self.docs.state().await {
            Ok(Some(v)) => Some(v.doc.desired_state),
            Ok(None) => Some(DesiredState::Running),
            Err(e) => {
                tracing::warn!(pipeline = %self.pipeline.name, "desired state unread: {e}");
                None
            }
        }
    }

    fn start(&self, run: &LeaseRun, permit: OwnedSemaphorePermit) -> Result<Running, String> {
        let parts = self.factory.build(&self.pipeline)?;
        let deps = WorkerDeps {
            source: parts.source,
            sink: Arc::clone(&parts.sink),
            docs: self.docs.clone(),
            owner_epoch: run.owner_epoch,
            guard: run.guard.clone(),
            hooks: parts.hooks,
        };
        let (worker, handle) =
            PipelineWorker::new(&self.pipeline, deps, self.settings.tuning.clone());
        tracing::info!(pipeline = %self.pipeline.name, "worker started");
        Ok(Running {
            handle,
            task: tokio::spawn(worker.run()),
            sink: parts.sink,
            permit: Some(permit),
            paused: false,
            logged: HashSet::new(),
        })
    }

    /// A stop or a hand-off: the worker flushes and ends within `drain_timeout`, else it is cut
    /// off (what it read past the durable checkpoint is read again by the next run). True when
    /// it ended on its own; either way it has ended, its writer with it, when this returns.
    async fn drain(&self, mut w: Running) -> bool {
        let ended = tokio::time::timeout(self.settings.drain_timeout, async {
            let _ = w.handle.control.send(Command::Drain).await;
            (&mut w.task).await
        })
        .await;
        let drained = match ended {
            Ok(Ok(Ok(_))) => {
                tracing::info!(pipeline = %self.pipeline.name, "worker drained");
                true
            }
            Ok(Ok(Err(e))) => {
                tracing::warn!(pipeline = %self.pipeline.name, "worker ended draining: {e}");
                true
            }
            Ok(Err(e)) => {
                tracing::error!(pipeline = %self.pipeline.name, "worker task: {e}");
                true
            }
            Err(_) => {
                tracing::warn!(
                    pipeline = %self.pipeline.name,
                    "worker didn't drain within {:?}; cut off",
                    self.settings.drain_timeout
                );
                w.task.abort();
                let _ = (&mut w.task).await;
                false
            }
        };
        self.observe(&mut w);
        self.metrics.worker_ended();
        drained
    }

    async fn give_back(&self, run: LeaseRun) {
        match run.stop().await {
            RenewEnd::Stopped(lease) => match release(&self.leases, &lease).await {
                Ok(()) => tracing::info!(pipeline = %self.pipeline.name, "lease released"),
                Err(e) => tracing::warn!(
                    pipeline = %self.pipeline.name,
                    "lease not released ({e}); it expires instead"
                ),
            },
            RenewEnd::Lost { reason } => {
                tracing::warn!(pipeline = %self.pipeline.name, "lease lost: {reason}");
            }
        }
    }

    /// The worker's status into the metrics, and its new warnings into the log.
    fn observe(&self, w: &mut Running) {
        let st = w.handle.status.borrow().clone();
        for warning in &st.warnings {
            if w.logged.insert(warning.clone()) {
                tracing::warn!(pipeline = %self.pipeline.name, "{warning}");
            }
        }
        let m = &self.metrics;
        m.wal_lag_bytes(st.retained_bytes);
        m.halted_tables(st.halted.len());
        m.reconcile_log(st.reconcile_log.entries, st.reconcile_log.keys);
        let rows: Vec<(String, u64)> = st
            .snapshots
            .iter()
            .map(|(t, p)| (t.to_string(), p.rows_done))
            .collect();
        m.snapshot_rows_done(&rows);
        let sink = w.sink.stats();
        m.totals(Totals {
            source_reconnects: st.reconnects,
            sink_merge_files_rewritten: sink.files_rewritten,
            sink_merge_rows_copied: sink.rows_copied,
        });
        self.status.send_modify(|s| s.worker = Some(st));
    }

    fn standby(&self, holder: Option<String>) {
        self.metrics.remove();
        self.status.send_replace(ControllerStatus {
            actual: Some(Actual::Standby { holder }),
            ..ControllerStatus::default()
        });
    }

    fn publish(
        &self,
        run: &LeaseRun,
        worker: Option<&Running>,
        desired: Option<DesiredState>,
        error: Option<&str>,
        dumped: bool,
        retrying: Option<&str>,
    ) {
        let phase = worker.map(|w| w.handle.status.borrow().phase.clone());
        let actual = if let Some(e) = error {
            Actual::Error {
                error: e.to_owned(),
            }
        } else if dumped {
            Actual::Dumped
        } else {
            match (desired, phase) {
                (Some(DesiredState::Stopped), _) => Actual::Stopped,
                (_, Some(Phase::Paused)) => Actual::Paused,
                (_, Some(Phase::Starting)) => Actual::Starting,
                (_, Some(_)) => Actual::Running,
                (_, None) => match retrying {
                    Some(e) => Actual::Retrying {
                        error: e.to_owned(),
                    },
                    None => Actual::Starting,
                },
            }
        };
        self.metrics.status(match &actual {
            Actual::Paused => PipelineStatus::Paused,
            Actual::Stopped | Actual::Dumped => PipelineStatus::Stopped,
            Actual::Error { .. } => PipelineStatus::Error,
            _ => PipelineStatus::Running,
        });
        self.status.send_modify(|s| {
            s.actual = Some(actual);
            s.owner_epoch = Some(run.owner_epoch);
            s.lease = Some(run.guard.state());
        });
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::{SinkError, SourceError, TableRef};
    use data_avalanche_testkit::{FakeTxn, Method, SinkOp};

    use super::*;
    use crate::test_support::{
        Fault, Kind, ScriptedStore, TestFactory, deps, eventually, holder, leases, node, pipeline,
        pipeline_docs, pipeline_docs_on, row, until,
    };

    /// A real two-second watchdog over `wait` on the paused clock: a controller that spins keeps
    /// the paused clock still, and the test fails instead of hanging.
    async fn idles_through(wait: Duration) {
        let (spun, spinning) = tokio::sync::oneshot::channel::<()>();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(2));
            let _ = spun.send(());
        });
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            _ = spinning => panic!("the controller spins: the paused clock stood still"),
        }
    }

    fn t(name: &str) -> TableRef {
        TableRef::new("public", name)
    }

    fn is(actual: Actual) -> impl Fn(&ControllerStatus) -> bool {
        move |s| s.actual.as_ref() == Some(&actual)
    }

    fn line(text: &str, metric: &str) -> bool {
        text.lines().any(|l| l == metric)
    }

    const P: &str = r#"pipeline="p",source_host="127.0.0.1",source_db="avalanche""#;

    #[tokio::test(start_paused = true)]
    async fn an_owned_pipeline_runs_under_its_lease() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        f.source.load(&t("a"), vec![row(1)]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(2)));
        eventually("rows land", || f.ids("a") == [1, 2]).await;
        assert_eq!(n.status.borrow().owner_epoch, Some(1));
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(lease.doc.is_held_by(&holder("n1")));
        let text = n.metrics.encode();
        assert!(
            line(
                &text,
                &format!(r#"data_avalanche_pipeline_status{{{P},status="running"}} 1"#)
            ),
            "{text}"
        );
        assert!(line(
            &text,
            &format!(r#"data_avalanche_lease_state{{{P},state="held"}} 1"#)
        ));
        assert!(line(&text, "data_avalanche_owned_pipelines 1"), "{text}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_pause_parks_the_worker_and_a_resume_reads_on() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        docs.set_desired_state(DesiredState::Paused).await.unwrap();
        until(&mut n.status, "paused", is(Actual::Paused)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(f.ids("a").is_empty(), "nothing is read while paused");
        assert!(line(
            &n.metrics.encode(),
            &format!(r#"data_avalanche_pipeline_status{{{P},status="paused"}} 1"#)
        ));
        docs.set_desired_state(DesiredState::Running).await.unwrap();
        until(&mut n.status, "running", is(Actual::Running)).await;
        eventually("the row lands", || f.ids("a") == [1]).await;
        assert_eq!(f.builds(), 1, "the same worker: the source stayed warm");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stop_drains_the_worker_and_a_resume_runs_a_new_one() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
        until(&mut n.status, "stopped", is(Actual::Stopped)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(f.ids("a").is_empty());
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(
            lease.doc.is_held_by(&holder("n1")),
            "a stopped pipeline keeps its lease"
        );
        docs.set_desired_state(DesiredState::Running).await.unwrap();
        eventually("the row lands", || f.ids("a") == [1]).await;
        assert_eq!(f.builds(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_paused_pipeline_starts_paused_after_a_restart() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        docs.set_desired_state(DesiredState::Paused).await.unwrap();
        until(&mut n.status, "paused", is(Actual::Paused)).await;
        drop(n.target);
        n.task.await.unwrap();
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(lease.doc.released, "a shutdown releases the lease");

        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "paused", is(Actual::Paused)).await;
        assert_eq!(n.status.borrow().owner_epoch, Some(2));
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(f.ids("a").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_node_waits_then_takes_over_when_the_first_dies() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut a = node("n1", &store, &docs, &f);
        until(&mut a.status, "running", is(Actual::Running)).await;
        let mut b = node("n2", &store, &docs, &f);
        until(
            &mut b.status,
            "standby",
            is(Actual::Standby {
                holder: Some("n1".into()),
            }),
        )
        .await;
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(f.builds(), 1, "a live holder keeps it");
        a.task.abort();
        until(&mut b.status, "running on n2", is(Actual::Running)).await;
        assert_eq!(b.status.borrow().owner_epoch, Some(2));
        f.source.commit(FakeTxn::new().insert(&t("a"), row(7)));
        eventually("the row lands", || f.ids("a") == [7]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_worker_error_holds_until_a_stop_and_a_resume() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        f.source.fail(
            Method::Preflight,
            SourceError::Permanent("no privilege".into()),
        );
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "error", |s| {
            matches!(&s.actual, Some(Actual::Error { error }) if error.contains("no privilege"))
        })
        .await;
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(f.builds(), 1, "not rerun");
        assert!(line(
            &n.metrics.encode(),
            &format!(r#"data_avalanche_pipeline_status{{{P},status="error"}} 1"#)
        ));
        docs.open("p", "delta").await.unwrap();
        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
        until(&mut n.status, "stopped", is(Actual::Stopped)).await;
        docs.set_desired_state(DesiredState::Running).await.unwrap();
        until(&mut n.status, "running", is(Actual::Running)).await;
        assert_eq!(f.builds(), 2);
    }

    /// The state store fails Phase 0's first write: the worker ends, and runs again.
    #[tokio::test(start_paused = true)]
    async fn a_transient_worker_error_reruns_it_after_a_backoff() {
        let store = ScriptedStore::new();
        let state = ScriptedStore::new();
        let docs = pipeline_docs_on(&state);
        state.fail_next(Fault {
            kind: Kind::Transient,
            lands: false,
        });
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(
            &mut n.status,
            "retrying",
            |s| matches!(&s.actual, Some(Actual::Retrying { error }) if error.contains("injected")),
        )
        .await;
        until(&mut n.status, "running", is(Actual::Running)).await;
        assert_eq!(f.builds(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_lease_stops_the_worker_and_the_lease_is_claimed_again() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        store.fail_always(Some(Fault {
            kind: Kind::Transient,
            lands: false,
        }));
        until(&mut n.status, "standby", |s| {
            matches!(s.actual, Some(Actual::Standby { .. }))
        })
        .await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(
            f.ids("a").is_empty(),
            "nothing is committed without the lease"
        );
        store.fail_always(None);
        until(&mut n.status, "running again", is(Actual::Running)).await;
        assert_eq!(n.status.borrow().owner_epoch, Some(2));
        eventually("the row lands", || f.ids("a") == [1]).await;
    }

    /// A stop drains: what the worker read is committed before it ends, though the commit
    /// waits a while.
    #[tokio::test(start_paused = true)]
    async fn a_stop_drains_what_the_worker_read() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut d = deps("n1", &store, &f);
        d.settings.drain_timeout = Duration::from_secs(60);
        let (c, mut status) = PipelineController::new(pipeline(), docs.clone(), &d);
        let (_target, rx) = watch::channel(Target::Own);
        let _task = tokio::spawn(c.run(rx));
        until(&mut status, "running", is(Actual::Running)).await;
        f.sink.hold_commits();
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        eventually("the commit waits", || f.sink.waiting_commits() == 1).await;
        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
        // Past the next poll: the drain has begun, and waits for the commit.
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert!(f.ids("a").is_empty());
        f.sink.release_commits();
        until(&mut status, "stopped", is(Actual::Stopped)).await;
        assert_eq!(f.ids("a"), [1], "drained before it stopped");
    }

    /// A drain cut off at `drain_timeout` ends the worker and its writer, and leaves the lease to
    /// expire (Clustering §8.3): nothing the worker read lands afterwards, and nobody can take
    /// the lease before its holder's deadline.
    #[tokio::test(start_paused = true)]
    async fn a_cut_off_drain_ends_the_writer_and_leaves_the_lease_to_expire() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        f.sink
            .fail(SinkOp::Commit, None, 1500, || SinkError::Transient {
                reason: "down".into(),
            });
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        tokio::time::sleep(Duration::from_millis(500)).await;
        n.target.send_replace(Target::DontOwn);
        until(
            &mut n.status,
            "standby",
            is(Actual::Standby { holder: None }),
        )
        .await;
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(lease.doc.is_held_by(&holder("n1")), "not released");
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(
            f.ids("a").is_empty(),
            "the cut-off writer committed nothing"
        );
    }

    /// A worker that ends for the lease (a newer epoch wrote a table) goes back to claiming: the
    /// next term runs under a new epoch, never `ERROR`.
    #[tokio::test(start_paused = true)]
    async fn a_worker_fenced_goes_back_to_the_lease() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        eventually("the first row lands", || f.ids("a") == [1]).await;
        f.sink.take_over(&t("a"), 2);
        f.source.commit(FakeTxn::new().insert(&t("a"), row(2)));
        until(&mut n.status, "running under epoch 2", |s| {
            s.actual == Some(Actual::Running) && s.owner_epoch == Some(2)
        })
        .await;
        eventually("the second row lands", || f.ids("a") == [1, 2]).await;
    }

    /// A worker that failed transiently, then a start that fails for good: `ERROR` holds, and
    /// the controller waits idle (its retry's time is past, but nothing may start).
    #[tokio::test(start_paused = true)]
    async fn an_error_after_a_retry_idles() {
        #[derive(Debug)]
        struct SecondFails(TestFactory);
        impl WorkerFactory for SecondFails {
            fn build(&self, p: &Pipeline) -> Result<Parts, String> {
                let parts = self.0.build(p)?;
                if self.0.builds() >= 2 {
                    return Err("boom".into());
                }
                Ok(parts)
            }
        }
        let store = ScriptedStore::new();
        let state = ScriptedStore::new();
        let docs = pipeline_docs_on(&state);
        state.fail_next(Fault {
            kind: Kind::Transient,
            lands: false,
        });
        let f = TestFactory::new(&["a"]);
        let mut d = deps("n1", &store, &f);
        d.factory = Arc::new(SecondFails(f.clone()));
        let (c, mut status) = PipelineController::new(pipeline(), docs, &d);
        let (_target, rx) = watch::channel(Target::Own);
        let _task = tokio::spawn(c.run(rx));
        until(
            &mut status,
            "error",
            |s| matches!(&s.actual, Some(Actual::Error { error }) if error.contains("boom")),
        )
        .await;
        idles_through(Duration::from_secs(10)).await;
        assert_eq!(f.builds(), 2);
    }

    /// The desired state couldn't be read when the lease was taken: nothing starts until a read
    /// says what to run (here, nothing: the pipeline is stopped).
    #[tokio::test(start_paused = true)]
    async fn an_unread_desired_state_starts_nothing() {
        let store = ScriptedStore::new();
        let state = ScriptedStore::new();
        let docs = pipeline_docs_on(&state);
        docs.open("p", "delta").await.unwrap();
        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
        state.fail_reads(1);
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "stopped", is(Actual::Stopped)).await;
        assert_eq!(f.builds(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dont_own_target_drains_releases_and_drops_the_series() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        n.target.send_replace(Target::DontOwn);
        until(
            &mut n.status,
            "standby",
            is(Actual::Standby { holder: None }),
        )
        .await;
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(lease.doc.released);
        let text = n.metrics.encode();
        assert!(!text.contains(P), "{text}");
        assert!(line(&text, "data_avalanche_owned_pipelines 0"), "{text}");
        n.target.send_replace(Target::Own);
        until(&mut n.status, "running", is(Actual::Running)).await;
        assert_eq!(n.status.borrow().owner_epoch, Some(2));
    }
}
