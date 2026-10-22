//! One pipeline's controller (Clustering §20, Pipeline_Lifecycle §5): the only writer of the
//! pipeline's lease. While its target is "own" it claims the lease, then runs the pipeline's
//! worker under it and reconciles the worker toward the persisted desired state, polled every
//! `lease_renewal_interval` (Q-M6b-2). A worker that ends with an error is rerun, stopped for the
//! lease, or left in `ERROR` (Q-M6b-3).

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use data_avalanche_config::Pipeline;
use data_avalanche_core::{Sink, SourceConnector, SourceError};
use data_avalanche_pipeline::{
    Backoff, Command, CommitGuard, Exit, Hooks, Phase, PipelineWorker, Recovery, Tuning,
    WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
};
use data_avalanche_state::{DesiredState, Holder, Leases, PipelineDocs};
use data_avalanche_telemetry::{Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, Totals};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
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

/// A reload's change to one pipeline (config §12.3), for its controller.
#[derive(Debug, Clone)]
pub enum Update {
    /// Only reloadable fields changed (class R): the running worker takes them in place, and
    /// every later run starts with them.
    Reconfigure(Box<Pipeline>),
    /// A restart-only field changed (class PR, §12.4): the running worker is discarded, and the
    /// next one replays from the checkpoint with the new pipeline and its state documents, under
    /// the same lease. A pipeline in `ERROR` runs again, the new config may be the fix; the
    /// desired state stands, so a paused or stopped pipeline stays so.
    Restart(Box<Pipeline>, PipelineDocs),
    /// Gone from the config: the worker drains, what the source created for the pipeline goes
    /// (`auto_cleanup`), the lease is released, and the controller ends.
    Remove,
    /// `globals.cluster.drain_timeout`, reloaded.
    DrainTimeout(Duration),
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
    /// process restarts, or a stop then a resume. `code` names a pipeline-scope halt
    /// (`BINLOG_PAYLOAD_INVALID`, `BINLOG_SOURCE_CHANGED`; Q-M7b-4).
    Error {
        error: String,
        code: Option<String>,
    },
}

/// Why the pipeline is in `ERROR`: the error, and the halt's code when it names one.
#[derive(Debug, Clone)]
struct Failure {
    error: String,
    code: Option<String>,
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
    /// Each Phase 0 starts after a random wait below this, so starts that the cap lets through
    /// together don't reconnect together (Clustering §20.2).
    pub start_jitter: Duration,
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
    /// Caps the workers handed off at once (`max_concurrent_handoffs`, Clustering §8.5).
    pub handoffs: Arc<Semaphore>,
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
    handoffs: Arc<Semaphore>,
    status: watch::Sender<ControllerStatus>,
    /// A reload's updates, and the sender [`Self::updater`] hands out: it is kept here, so the
    /// channel never closes.
    updates: mpsc::UnboundedReceiver<Update>,
    updater: mpsc::UnboundedSender<Update>,
}

/// Why the owned phase ended.
enum Left {
    /// The lease is gone: claim it again.
    Lost,
    /// A table was written at `current_epoch`, past this lease's epoch: claim again, and take
    /// this process's own lease over above it (DA-CLUS-14).
    Fenced { current_epoch: u64 },
    /// The target became "don't own": the lease is released.
    Released,
    /// The target channel closed: the process is shutting down.
    Shutdown,
    /// A reload removed the pipeline: it is let go of for good.
    Removed,
}

/// A random wait in `[0, max)`.
fn jitter(max: Duration) -> Duration {
    let nanos = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX);
    match nanos {
        0 => Duration::ZERO,
        n => Duration::from_nanos(
            getrandom::u64().expect("the OS random number generator is unavailable") % n,
        ),
    }
}

/// What woke the owned loop.
enum Event {
    Lost,
    Target { closed: bool },
    Ended(Result<Result<Exit, WorkerError>, tokio::task::JoinError>),
    Status { open: bool },
    Start(OwnedSemaphorePermit),
    HandOff(OwnedSemaphorePermit),
    Due,
    Poll,
    Update(Update),
}

/// A pipeline's series labels.
fn labels(p: &Pipeline) -> PipelineLabels {
    PipelineLabels {
        pipeline: p.name.clone(),
        source_host: p.source.host.clone(),
        source_db: p.source.db.clone(),
    }
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
    /// The worker's reconnects already logged.
    reconnects: u64,
}

/// A worker never outlives its controller: a controller that panics drops its `Running`, and
/// the worker task goes with it before the next controller claims the lease (DA-OPS-7).
impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The owned-pipelines gauge for as long as it lives: a panic or an abort decrements it too.
struct Owned(Metrics);

impl Owned {
    fn new(node: &Metrics) -> Self {
        node.owned_by(1);
        Self(node.clone())
    }
}

impl Drop for Owned {
    fn drop(&mut self) {
        self.0.owned_by(-1);
    }
}

impl PipelineController {
    /// `docs` are the pipeline's state documents.
    pub fn new(
        pipeline: Pipeline,
        docs: PipelineDocs,
        deps: &ControllerDeps,
    ) -> (Self, watch::Receiver<ControllerStatus>) {
        let (status, rx) = watch::channel(ControllerStatus::default());
        (Self::reporting(pipeline, docs, deps, status), rx)
    }

    /// A controller that publishes its status into `status`: a controller started again after
    /// its task ended keeps its predecessor's channel.
    pub fn reporting(
        pipeline: Pipeline,
        docs: PipelineDocs,
        deps: &ControllerDeps,
        status: watch::Sender<ControllerStatus>,
    ) -> Self {
        let metrics = Arc::new(
            deps.metrics
                .pipeline(labels(&pipeline), pipeline.sink.metrics.per_table),
        );
        let (updater, updates) = mpsc::unbounded_channel();
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
            handoffs: Arc::clone(&deps.handoffs),
            status,
            updates,
            updater,
        }
    }

    /// Where a reload's updates for this controller go.
    pub fn updater(&self) -> mpsc::UnboundedSender<Update> {
        self.updater.clone()
    }

    /// Runs until the target channel closes, or until a reload removes the pipeline; the lease
    /// is released on the way out.
    pub async fn run(mut self, mut target: watch::Receiver<Target>) {
        let mut claim = Claim::new(
            self.leases.clone(),
            &self.pipeline.id,
            self.holder.clone(),
            self.settings.timing,
        );
        loop {
            if *target.borrow_and_update() == Target::DontOwn {
                self.standby(None);
                tokio::select! {
                    r = target.changed() => if r.is_err() { return },
                    Some(u) = self.updates.recv() => if self.take(u) { return },
                }
                continue;
            }
            let wait = match claim.step().await {
                Ok(Claimed::Mine {
                    lease,
                    written,
                    jumped,
                }) => {
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
                    match self.owned(run, jumped, &mut target).await {
                        Left::Shutdown | Left::Removed => return,
                        Left::Fenced { current_epoch } => {
                            claim.at_least(current_epoch.saturating_add(1));
                            self.standby(None);
                            continue;
                        }
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
                Some(u) = self.updates.recv() => if self.take(u) { return },
            }
        }
    }

    /// A reload's update while this node holds no lease for the pipeline: true when it removed
    /// the pipeline.
    fn take(&mut self, update: Update) -> bool {
        match update {
            Update::Reconfigure(p) => self.reconfigure(*p),
            Update::Restart(p, docs) => {
                self.reconfigure(*p);
                self.docs = docs;
            }
            Update::Remove => {
                self.metrics.forget();
                tracing::info!(pipeline = %self.pipeline.name, "pipeline removed");
                return true;
            }
            Update::DrainTimeout(d) => self.settings.drain_timeout = d,
        }
        false
    }

    /// The reloaded pipeline, for every later run; its series take a new name, or a new
    /// `per_table`.
    fn reconfigure(&mut self, p: Pipeline) {
        self.metrics.relabel(labels(&p), p.sink.metrics.per_table);
        self.pipeline = p;
    }

    /// Holds the lease and reconciles the worker until the lease is lost or let go.
    /// `jumped`: the term's epoch was raised past the lease's to clear a table's (DA-CLUS-14).
    async fn owned(
        &mut self,
        run: LeaseRun,
        jumped: bool,
        target: &mut watch::Receiver<Target>,
    ) -> Left {
        let _owned = Owned::new(&self.node);
        self.reconcile(run, jumped, target).await
    }

    async fn reconcile(
        &mut self,
        run: LeaseRun,
        jumped: bool,
        target: &mut watch::Receiver<Target>,
    ) -> Left {
        let mut jumped = jumped;
        let mut worker: Option<Running> = None;
        let mut error: Option<Failure> = None;
        let mut dumped = false;
        let mut retry: Option<(Instant, String)> = None;
        let mut failures: u32 = 0;
        // One lease term: a stale reader is ended at most once in it, by whichever of its
        // workers meets it first, a stop and a resume included (DA-SRC-28).
        let reader_ended = Arc::new(AtomicBool::new(false));
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
                // A stop clears an error: the resume after it runs the pipeline again. It also
                // spends the jump: the next fence climbs once more, as the ERROR's remedy says.
                error = None;
                jumped = false;
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
            // "Don't own": with nothing to drain the lease goes back now; a worker is handed off
            // once a hand-off permit is free, unless the target turns back to "own" first.
            let hand_off = *target.borrow() == Target::DontOwn;
            if hand_off && worker.is_none() {
                self.give_back(run).await;
                self.standby(None);
                return Left::Released;
            }
            let due = retry.as_ref().map(|(at, _)| *at);
            // A worker may run: no error, not dumped, and a desired state read that isn't
            // "stopped". Only then does a retry's time matter (else its arm would be ready at
            // every turn).
            let may_run = worker.is_none()
                && !hand_off
                && error.is_none()
                && !dumped
                && desired.is_some_and(|d| d != DesiredState::Stopped);
            let want_start = may_run && due.is_none_or(|at| Instant::now() >= at);
            self.publish(
                &run,
                worker.as_ref(),
                desired,
                error.as_ref(),
                dumped,
                retry.as_ref().map(|(_, e)| e.as_str()),
            );
            let starts = Arc::clone(&self.starts);
            let handoffs = Arc::clone(&self.handoffs);
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
                    permit = handoffs.acquire_owned(), if hand_off => {
                        Event::HandOff(permit.expect("the hand-off semaphore isn't closed"))
                    }
                    () = async {
                        match due {
                            Some(at) => tokio::time::sleep_until(at).await,
                            None => std::future::pending().await,
                        }
                    }, if may_run && !want_start => Event::Due,
                    _ = poll.tick() => Event::Poll,
                    Some(u) = self.updates.recv() => Event::Update(u),
                }
            };
            match event {
                Event::Lost => {
                    if let Some(mut w) = worker.take() {
                        w.task.abort();
                        // Ended (dropped) before the next term starts: its writer goes with it.
                        let _ = (&mut w.task).await;
                        self.metrics.worker_ended();
                    }
                    let reason = match run.stop().await {
                        RenewEnd::Lost { reason } => reason,
                        RenewEnd::Stopped(_) => "the lease's deadline passed".into(),
                    };
                    tracing::error!(pipeline = %self.pipeline.name, "lease lost: {reason}");
                    return Left::Lost;
                }
                // A shutdown drains at once; "don't own" waits for its hand-off permit.
                Event::Target { closed: true } => {
                    self.let_go(run, worker.take()).await;
                    return Left::Shutdown;
                }
                Event::Target { closed: false } => {}
                Event::HandOff(_permit) => {
                    self.let_go(run, worker.take()).await;
                    return Left::Released;
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
                            Recovery::Lease => match e {
                                // DA-CLUS-14: the term already jumped past the table's epoch,
                                // and the table moved past it again. Another lease store
                                // writes to this lake; climbing on would defeat the fence.
                                WorkerError::Fenced {
                                    table,
                                    current_epoch,
                                } if jumped => {
                                    let why = format!(
                                        "{table} was written at owner epoch {current_epoch}, past this lease's {}, which had already been raised past the table's: another lease store (another globals.state) writes to this lake. Let one cluster write it, then stop and resume the pipeline",
                                        run.owner_epoch
                                    );
                                    tracing::error!(pipeline = %self.pipeline.name, "pipeline in ERROR: {why}");
                                    error = Some(Failure {
                                        error: why,
                                        code: None,
                                    });
                                }
                                WorkerError::Fenced { current_epoch, .. } => {
                                    tracing::error!(
                                        pipeline = %self.pipeline.name,
                                        "worker stopped for the lease: {e}"
                                    );
                                    let _ = run.stop().await;
                                    return Left::Fenced { current_epoch };
                                }
                                e => {
                                    tracing::error!(
                                        pipeline = %self.pipeline.name,
                                        "worker stopped for the lease: {e}"
                                    );
                                    let _ = run.stop().await;
                                    return Left::Lost;
                                }
                            },
                            Recovery::Error => {
                                let code = e.code().map(str::to_owned);
                                tracing::error!(
                                    pipeline = %self.pipeline.name,
                                    code = code.as_deref(),
                                    "pipeline in ERROR: {e}"
                                );
                                error = Some(Failure {
                                    error: e.to_string(),
                                    code,
                                });
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
                Event::Start(permit) => match self.start(&run, &reader_ended, permit) {
                    Ok(w) => worker = Some(w),
                    Err(e) => {
                        tracing::error!(pipeline = %self.pipeline.name, "pipeline in ERROR: {e}");
                        error = Some(Failure {
                            error: e,
                            code: None,
                        });
                    }
                },
                Event::Due => {}
                Event::Poll => {
                    desired = self.desired().await.or(desired);
                    if let Some(w) = worker.as_mut() {
                        self.observe(w);
                    }
                }
                Event::Update(Update::Reconfigure(p)) => {
                    self.reconfigure(*p);
                    if let Some(w) = worker.as_ref() {
                        let p = Box::new(self.pipeline.clone());
                        let _ = w.handle.control.send(Command::Reconfigure(p)).await;
                    }
                }
                Event::Update(Update::Restart(p, docs)) => {
                    self.reconfigure(*p);
                    self.docs = docs;
                    // Discard and replay (§12.4): what the worker read past the durable
                    // checkpoint is read again by the next one.
                    if let Some(mut w) = worker.take() {
                        w.task.abort();
                        let _ = (&mut w.task).await;
                        self.metrics.worker_ended();
                    }
                    tracing::info!(pipeline = %self.pipeline.name, "restarted for a reload");
                    error = None;
                    dumped = false;
                    retry = None;
                    failures = 0;
                    desired = self.desired().await;
                }
                Event::Update(Update::Remove) => {
                    self.remove(run, worker.take()).await;
                    return Left::Removed;
                }
                Event::Update(Update::DrainTimeout(d)) => self.settings.drain_timeout = d,
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

    /// `reader_ended`: the term's [`WorkerDeps::reader_ended`].
    fn start(
        &self,
        run: &LeaseRun,
        reader_ended: &Arc<AtomicBool>,
        permit: OwnedSemaphorePermit,
    ) -> Result<Running, String> {
        let parts = self.factory.build(&self.pipeline)?;
        let deps = WorkerDeps {
            source: parts.source,
            sink: Arc::clone(&parts.sink),
            docs: self.docs.clone(),
            owner_epoch: run.owner_epoch,
            guard: run.guard.clone(),
            hooks: parts.hooks,
            reader_ended: Arc::clone(reader_ended),
        };
        let (worker, handle) =
            PipelineWorker::new(&self.pipeline, deps, self.settings.tuning.clone());
        let jitter = jitter(self.settings.start_jitter);
        tracing::info!(pipeline = %self.pipeline.name, "worker started");
        Ok(Running {
            handle,
            task: tokio::spawn(async move {
                tokio::time::sleep(jitter).await;
                worker.run().await
            }),
            sink: parts.sink,
            permit: Some(permit),
            paused: false,
            logged: HashSet::new(),
            reconnects: 0,
        })
    }

    /// A stop or a hand-off: the worker flushes and ends within `drain_timeout`, else it is cut
    /// off (what it read past the durable checkpoint is read again by the next run). True when
    /// it ended on its own; either way the worker has ended when this returns. A cut-off
    /// worker's writer is aborted with it, not waited for.
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

    /// Drains the worker and releases the lease; a worker cut off leaves it to expire.
    async fn let_go(&self, run: LeaseRun, worker: Option<Running>) {
        let drained = match worker {
            Some(w) => self.drain(w).await,
            None => true,
        };
        if drained {
            self.give_back(run).await;
        } else {
            // Clustering §8.3: a worker cut off may have left a write in flight; the lease
            // expires instead of being handed over now.
            let _ = run.stop().await;
            tracing::warn!(
                pipeline = %self.pipeline.name,
                "lease left to expire after the drain was cut off"
            );
        }
        self.standby(None);
    }

    /// A pipeline gone from the config (§12.3): the worker drains, what the source created for
    /// the pipeline goes, and the lease is released. A drain cut off, or a lease lost meanwhile,
    /// leaves the source as it is: a cut-off worker may still hold it, a lost lease's holder runs
    /// it.
    async fn remove(&self, run: LeaseRun, worker: Option<Running>) {
        let drained = match worker {
            Some(w) => self.drain(w).await,
            None => true,
        };
        if !drained {
            let _ = run.stop().await;
            tracing::warn!(
                pipeline = %self.pipeline.name,
                "removed with its drain cut off: the lease expires, and what the source created for the pipeline stays"
            );
        } else if run.guard.may_commit() {
            self.teardown().await;
            self.give_back(run).await;
        } else {
            let _ = run.stop().await;
            tracing::warn!(
                pipeline = %self.pipeline.name,
                "removed after its lease was lost: what the source created for the pipeline stays"
            );
        }
        self.metrics.forget();
        tracing::info!(pipeline = %self.pipeline.name, "pipeline removed");
    }

    /// The source's `teardown` (PostgreSQL's `auto_cleanup`: the slot and the publication),
    /// tried again while the source is busy or doesn't answer, within the contended window: the
    /// stream a drain closed may hold the slot a moment longer.
    async fn teardown(&self) {
        let source = match self.factory.build(&self.pipeline) {
            Ok(parts) => parts.source,
            Err(e) => {
                tracing::warn!(pipeline = %self.pipeline.name, "no source to clean up: {e}");
                return;
            }
        };
        let since = Instant::now();
        let mut attempt = 0;
        loop {
            match source.teardown().await {
                Ok(()) => return,
                Err(SourceError::Transient(_) | SourceError::Contended(_))
                    if since.elapsed() < self.settings.tuning.contended_window =>
                {
                    tokio::time::sleep(self.settings.restart.delay(attempt)).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(e) => {
                    tracing::warn!(
                        pipeline = %self.pipeline.name,
                        "what the source created for the pipeline stays ({e}): remove it by hand"
                    );
                    return;
                }
            }
        }
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

    /// The worker's status into the metrics, and its new warnings and reconnects into the log.
    fn observe(&self, w: &mut Running) {
        let st = w.handle.status.borrow().clone();
        for warning in &st.warnings {
            if w.logged.insert(warning.clone()) {
                tracing::warn!(pipeline = %self.pipeline.name, "{warning}");
            }
        }
        if st.reconnects > w.reconnects {
            w.reconnects = st.reconnects;
            if let Phase::Reconnecting { error } = &st.phase {
                tracing::warn!(pipeline = %self.pipeline.name, "reconnecting to the source: {error}");
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
        error: Option<&Failure>,
        dumped: bool,
        retrying: Option<&str>,
    ) {
        let phase = worker.map(|w| w.handle.status.borrow().phase.clone());
        let actual = if let Some(f) = error {
            Actual::Error {
                error: f.error.clone(),
                code: f.code.clone(),
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

    #[test]
    fn a_start_jitter_stays_below_its_bound() {
        assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
        let max = Duration::from_millis(500);
        assert!((0..200).all(|_| jitter(max) < max));
    }

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

    /// A pipeline-scope halt's code rides the `ERROR` state (Q-M7b-4).
    #[tokio::test(start_paused = true)]
    async fn a_pipeline_halt_publishes_its_code() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        f.source.fail(
            Method::Prepare,
            SourceError::Permanent(
                "BINLOG_SOURCE_CHANGED: binlog position f:4 was recorded on another server".into(),
            ),
        );
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "error", |s| {
            matches!(
                &s.actual,
                Some(Actual::Error { code: Some(c), .. }) if c == "BINLOG_SOURCE_CHANGED"
            )
        })
        .await;
        assert!(line(
            &n.metrics.encode(),
            &format!(r#"data_avalanche_pipeline_status{{{P},status="error"}} 1"#)
        ));
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
            matches!(&s.actual, Some(Actual::Error { error, .. }) if error.contains("no privilege"))
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

    /// The stale reader is ended at most once a lease term (DA-SRC-28): a worker that gave up on
    /// a busy slot is rerun under the same lease, and the rerun waits the slot out too, though
    /// it is a new worker.
    #[tokio::test(start_paused = true)]
    async fn a_stale_reader_is_ended_once_a_lease_term() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let busy = |f: &TestFactory| {
            for _ in 0..10_000 {
                f.source
                    .fail(Method::OpenStream, SourceError::Contended("active".into()));
            }
            f.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
        };
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        // A reconnect meets the busy slot: its reader is ended, and the worker streams on.
        busy(&f);
        eventually("the reader is ended", || f.source.readers_ended() == 1).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        eventually("the row lands", || f.ids("a") == [1]).await;
        assert_eq!(f.builds(), 1);
        // The slot is busy again: the worker gives up after `contended_window` (200 ms), and is
        // rerun after the restart backoff (100 ms first); the rerun meets the busy slot.
        busy(&f);
        until(
            &mut n.status,
            "retrying",
            |s| matches!(&s.actual, Some(Actual::Retrying { error }) if error.contains("active")),
        )
        .await;
        eventually("the rerun meets the busy slot and gives up", || {
            f.source.readers_ended() > 1 || f.builds() >= 3
        })
        .await;
        assert_eq!(f.source.readers_ended(), 1, "not ended again in the term");
        assert_eq!(n.status.borrow().owner_epoch, Some(1), "the same term");
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
        let lost = format!(r#"data_avalanche_lease_renew_failures_total{{{P},class="lost"}} 1"#);
        let text = n.metrics.encode();
        assert!(line(&text, &lost), "the loss outlives the standby\n{text}");
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
        let text = n.metrics.encode();
        assert!(line(&text, &lost), "the next term counts on\n{text}");
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

    /// A worker that ends for the lease (a newer epoch wrote a table) goes back to claiming. The
    /// lease still names this node, at epoch 1, while the table was written at 5 (a moved or
    /// restored `globals.state`, DA-CLUS-14): the next term jumps to 6 at once, not one epoch
    /// per worker run.
    #[tokio::test(start_paused = true)]
    async fn a_worker_fenced_goes_back_to_the_lease_above_the_tables_epoch() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        eventually("the first row lands", || f.ids("a") == [1]).await;
        f.sink.take_over(&t("a"), 5);
        f.source.commit(FakeTxn::new().insert(&t("a"), row(2)));
        until(&mut n.status, "running under epoch 6", |s| {
            s.actual == Some(Actual::Running) && s.owner_epoch == Some(6)
        })
        .await;
        eventually("the second row lands", || f.ids("a") == [1, 2]).await;
        assert_eq!(f.builds(), 2, "one worker run per fence");
    }

    /// The table moves past the jumped epoch again: another lease store writes to the lake.
    /// The pipeline goes to `ERROR` and keeps its lease, rather than climbing.
    #[tokio::test(start_paused = true)]
    async fn a_second_fence_past_a_jumped_epoch_is_an_error() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        eventually("the first row lands", || f.ids("a") == [1]).await;
        f.sink.take_over(&t("a"), 5);
        f.source.commit(FakeTxn::new().insert(&t("a"), row(2)));
        until(&mut n.status, "running under epoch 6", |s| {
            s.owner_epoch == Some(6)
        })
        .await;
        eventually("the second row lands", || f.ids("a") == [1, 2]).await;
        f.sink.take_over(&t("a"), 9);
        f.source.commit(FakeTxn::new().insert(&t("a"), row(3)));
        until(&mut n.status, "ERROR", |s| {
            matches!(s.actual, Some(Actual::Error { .. }))
        })
        .await;
        let Some(Actual::Error { error, code }) = n.status.borrow().actual.clone() else {
            unreachable!()
        };
        assert!(error.contains("another lease store"), "{error}");
        assert_eq!(code, None);
        assert_eq!(n.status.borrow().owner_epoch, Some(6), "the lease is kept");
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(f.builds(), 2, "no climb");
        assert_eq!(f.ids("a"), [1, 2]);
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert_eq!((lease.doc.owner_epoch, lease.doc.released), (6, false));
    }

    /// The ERROR's remedy: a stop and a resume. The stop spends the jump, so the resume's fence
    /// (the table at 9, the lease at 6) climbs once more, to 10.
    #[tokio::test(start_paused = true)]
    async fn a_stop_and_resume_after_the_error_jumps_once_more() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        eventually("the first row lands", || f.ids("a") == [1]).await;
        f.sink.take_over(&t("a"), 5);
        f.source.commit(FakeTxn::new().insert(&t("a"), row(2)));
        until(&mut n.status, "running under epoch 6", |s| {
            s.owner_epoch == Some(6)
        })
        .await;
        eventually("the second row lands", || f.ids("a") == [1, 2]).await;
        f.sink.take_over(&t("a"), 9);
        f.source.commit(FakeTxn::new().insert(&t("a"), row(3)));
        until(&mut n.status, "ERROR", |s| {
            matches!(s.actual, Some(Actual::Error { .. }))
        })
        .await;
        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
        until(&mut n.status, "stopped", is(Actual::Stopped)).await;
        docs.set_desired_state(DesiredState::Running).await.unwrap();
        until(&mut n.status, "running under epoch 10", |s| {
            s.actual == Some(Actual::Running) && s.owner_epoch == Some(10)
        })
        .await;
        eventually("the third row lands", || f.ids("a") == [1, 2, 3]).await;
        assert_eq!(
            f.builds(),
            4,
            "one run per epoch, and the resume's fenced one"
        );
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert_eq!((lease.doc.owner_epoch, lease.doc.released), (10, false));
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
            |s| matches!(&s.actual, Some(Actual::Error { error, .. }) if error.contains("boom")),
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

    /// A reload's reloadable change reaches the running worker in place (config §1.2): the same
    /// worker, the source told, and the series under the new name only.
    #[tokio::test(start_paused = true)]
    async fn a_reconfigure_reaches_the_running_worker() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        let mut renamed = pipeline();
        renamed.name = "renamed".into();
        n.updates
            .send(Update::Reconfigure(Box::new(renamed)))
            .unwrap();
        eventually("the source takes it", || f.source.reconfigures() == 1).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        eventually("the row lands", || f.ids("a") == [1]).await;
        assert_eq!(f.builds(), 1, "the same worker");
        let text = n.metrics.encode();
        assert!(!text.contains(r#"pipeline="p""#), "{text}");
        assert!(
            text.contains(r#"data_avalanche_pipeline_status{pipeline="renamed","#),
            "{text}"
        );
    }

    /// A restart-only change discards the worker and runs a new one from the checkpoint, under
    /// the same lease.
    #[tokio::test(start_paused = true)]
    async fn a_restart_reruns_the_worker_under_the_same_lease() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        n.updates
            .send(Update::Restart(Box::new(pipeline()), docs.clone()))
            .unwrap();
        eventually("a new worker", || f.builds() == 2).await;
        until(&mut n.status, "running again", is(Actual::Running)).await;
        assert_eq!(n.status.borrow().owner_epoch, Some(1), "the same lease");
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        eventually("the row lands", || f.ids("a") == [1]).await;
    }

    /// A restart runs a pipeline in `ERROR` again: the reloaded config may be the fix.
    #[tokio::test(start_paused = true)]
    async fn a_restart_runs_a_pipeline_in_error_again() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        f.source.fail(
            Method::Preflight,
            SourceError::Permanent("no privilege".into()),
        );
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "error", |s| {
            matches!(&s.actual, Some(Actual::Error { .. }))
        })
        .await;
        n.updates
            .send(Update::Restart(Box::new(pipeline()), docs.clone()))
            .unwrap();
        until(&mut n.status, "running", is(Actual::Running)).await;
        assert_eq!(f.builds(), 2);
    }

    /// A restart doesn't resume a paused pipeline: the new worker parks.
    #[tokio::test(start_paused = true)]
    async fn a_restart_keeps_a_paused_pipeline_paused() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        docs.set_desired_state(DesiredState::Paused).await.unwrap();
        until(&mut n.status, "paused", is(Actual::Paused)).await;
        n.updates
            .send(Update::Restart(Box::new(pipeline()), docs.clone()))
            .unwrap();
        eventually("a new worker", || f.builds() == 2).await;
        until(&mut n.status, "paused again", is(Actual::Paused)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(f.ids("a").is_empty(), "nothing is read while paused");
    }

    /// A removed pipeline's worker drains, its source cleans up, its lease is released and its
    /// series go; the controller ends.
    #[tokio::test(start_paused = true)]
    async fn a_removed_pipeline_drains_cleans_up_and_releases() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        n.updates.send(Update::Remove).unwrap();
        tokio::time::timeout(Duration::from_secs(60), &mut n.task)
            .await
            .expect("the controller ends")
            .unwrap();
        assert_eq!(f.ids("a"), [1], "drained");
        assert_eq!(f.source.teardowns(), 1);
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(lease.doc.released);
        let text = n.metrics.encode();
        assert!(!text.contains(P), "{text}");
        assert!(line(&text, "data_avalanche_owned_pipelines 0"), "{text}");
    }

    /// A pipeline removed where it is on standby ends there: the holder cleans up, not this
    /// node.
    #[tokio::test(start_paused = true)]
    async fn a_pipeline_removed_on_standby_just_ends() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut a = node("n1", &store, &docs, &f);
        until(&mut a.status, "running", is(Actual::Running)).await;
        let mut b = node("n2", &store, &docs, &f);
        until(&mut b.status, "standby", |s| {
            matches!(s.actual, Some(Actual::Standby { .. }))
        })
        .await;
        b.updates.send(Update::Remove).unwrap();
        tokio::time::timeout(Duration::from_secs(60), &mut b.task)
            .await
            .expect("the standby controller ends")
            .unwrap();
        assert_eq!(f.source.teardowns(), 0);
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(lease.doc.is_held_by(&holder("n1")), "n1 still holds it");
    }

    /// A reloaded `drain_timeout` is the next drain's: a stop waits for the commit past the
    /// timeout the controller started with.
    #[tokio::test(start_paused = true)]
    async fn a_reloaded_drain_timeout_is_the_next_drains() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        n.updates
            .send(Update::DrainTimeout(Duration::from_secs(60)))
            .unwrap();
        f.sink.hold_commits();
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        eventually("the commit waits", || f.sink.waiting_commits() == 1).await;
        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_ne!(
            n.status.borrow().actual,
            Some(Actual::Stopped),
            "past the 5 s it started with, the drain still waits"
        );
        f.sink.release_commits();
        until(&mut n.status, "stopped", is(Actual::Stopped)).await;
        assert_eq!(f.ids("a"), [1], "drained before it stopped");
    }
}
