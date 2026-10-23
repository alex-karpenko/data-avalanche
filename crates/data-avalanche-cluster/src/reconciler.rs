//! The node's reconciler (Clustering §8.1, §20). It runs one controller per pipeline and tells
//! each whether to own its pipeline: "own" when the ring over the members places the pipeline on
//! this node. It never touches a lease; each controller claims and lets go of its own.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use data_avalanche_config::{Cluster, Pipeline, StateLocation};
use data_avalanche_state::{Holder, PipelineDocs};
use data_avalanche_storage::{Location, colocated_state, explicit_state, join};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};

use crate::{
    Actual, ClusterError, ControllerDeps, ControllerStatus, Members, PipelineController, Placement,
    Target, Update,
};

/// This node as a lease holder (Clustering §3.4, D-1): `node_id` is
/// `globals.cluster.memberlist.node_name`, else the OS hostname; the incarnation is the process's
/// start in unix milliseconds, so a restarted process is another holder.
///
/// # Errors
/// No `node_name` and no usable hostname.
pub fn node_identity(cluster: &Cluster, started: SystemTime) -> Result<Holder, ClusterError> {
    let node_id = match &cluster.memberlist.node_name {
        Some(name) => name.clone(),
        None => gethostname::gethostname()
            .into_string()
            .map_err(|h| ClusterError::NodeName(format!("the hostname {h:?} isn't UTF-8")))?,
    };
    if node_id.is_empty() {
        return Err(ClusterError::NodeName("the hostname is empty".into()));
    }
    let incarnation = started
        .duration_since(UNIX_EPOCH)
        .map_err(|e| ClusterError::NodeName(format!("the clock is before 1970: {e}")))?
        .as_millis();
    Ok(Holder {
        node_id,
        incarnation: u64::try_from(incarnation).unwrap_or(u64::MAX),
    })
}

/// The reconciler's settings, from `globals.cluster`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerSettings {
    pub tokens: u32,
    pub stabilization: Duration,
    /// Targets are recomputed, and ended controllers started again, at least this often
    /// (`lease_renewal_interval`).
    pub resync: Duration,
}

impl ReconcilerSettings {
    pub fn new(cluster: &Cluster) -> Self {
        Self {
            tokens: cluster.tokens,
            stabilization: cluster.rebalance_stabilization,
            resync: cluster.lease_renewal_interval,
        }
    }
}

/// A reload's change to one pipeline (config §12.3).
#[derive(Debug, Clone)]
pub enum PipelineReload {
    /// A new pipeline: its controller starts, on standby until the ring places it here.
    Add(Pipeline, PipelineDocs),
    /// The pipeline of this id is gone: [`Update::Remove`].
    Remove(String),
    /// [`Update::Reconfigure`].
    Reconfigure(Pipeline),
    /// [`Update::Restart`].
    Restart(Pipeline, PipelineDocs),
}

/// What a config generation changes on this node's reconciler: its pipelines, and
/// `globals.cluster`, whose reloadable fields (`rebalance_stabilization`,
/// `max_concurrent_takeovers`, `max_concurrent_handoffs`, `drain_timeout`) take effect.
#[derive(Debug, Clone)]
pub struct Reload {
    pub pipelines: Vec<PipelineReload>,
    pub cluster: Cluster,
}

/// Hands a reload to a running reconciler, from a task that outlives a borrow of it (the
/// config coordinator).
#[derive(Debug, Clone)]
pub struct Reloader(pub(crate) mpsc::UnboundedSender<Reload>);

impl Reloader {
    /// False when the reconciler has stopped.
    pub fn apply(&self, reload: Reload) -> bool {
        self.0.send(reload).is_ok()
    }
}

#[derive(Debug)]
struct Controlled {
    pipeline: Pipeline,
    docs: PipelineDocs,
    /// The removed pipelines it takes the place of ([`PipelineController::following`]): kept,
    /// so a controller started again after a panic waits for them too.
    following: Vec<String>,
    target: watch::Sender<Target>,
    status: watch::Sender<ControllerStatus>,
    task: JoinHandle<()>,
    /// The running controller's updates.
    updates: mpsc::UnboundedSender<Update>,
}

/// A controller dropped is aborted, its worker with it: a crash, its lease left to expire.
impl Drop for Controlled {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Controlled {
    fn new(
        pipeline: Pipeline,
        docs: PipelineDocs,
        following: Vec<String>,
        deps: &ControllerDeps,
    ) -> Self {
        let mut c = Self {
            pipeline,
            docs,
            following,
            target: watch::channel(Target::DontOwn).0,
            status: watch::channel(ControllerStatus::default()).0,
            task: tokio::spawn(async {}),
            updates: mpsc::unbounded_channel().0,
        };
        c.spawn(deps);
        c
    }

    fn spawn(&mut self, deps: &ControllerDeps) {
        let controller = PipelineController::reporting(
            self.pipeline.clone(),
            self.docs.clone(),
            deps,
            self.status.clone(),
        )
        .following(self.following.clone());
        self.updates = controller.updater();
        self.task = tokio::spawn(controller.run(self.target.subscribe()));
    }

    /// Tells a removed pipeline's controller, and waits for it to let go of the pipeline. Its
    /// target stays open meanwhile: a closed one would be a shutdown, which releases the lease
    /// without cleaning up. Dropped unfinished (a reconciler dropped), it aborts the controller
    /// as any [`Controlled`] does.
    async fn remove(mut self) {
        let _ = self.updates.send(Update::Remove);
        if let Err(e) = (&mut self.task).await {
            tracing::error!("a removed pipeline's controller ended abnormally: {e}");
        }
    }
}

/// A permit pool resized by a reload (`max_concurrent_takeovers`, `max_concurrent_handoffs`).
/// More permits are added at once; fewer are taken back as permits come free, so a permit in
/// use is never taken away, and until then the pool may let a few more through.
#[derive(Debug)]
struct Cap {
    pool: Arc<Semaphore>,
    size: usize,
    /// Permits to take back as they come free.
    owed: usize,
}

impl Cap {
    /// `pool` with no permit in use: its size is what it has available.
    fn new(pool: &Arc<Semaphore>) -> Self {
        Self {
            pool: Arc::clone(pool),
            size: pool.available_permits(),
            owed: 0,
        }
    }

    fn resize(&mut self, size: usize) {
        if size >= self.size {
            let more = size - self.size;
            let repaid = more.min(self.owed);
            self.owed -= repaid;
            self.pool.add_permits(more - repaid);
        } else {
            self.owed += self.size - size;
        }
        self.size = size;
        self.settle();
    }

    /// Takes back the owed permits that are free now.
    fn settle(&mut self) {
        if self.owed > 0 {
            self.owed -= self.pool.forget_permits(self.owed);
        }
    }
}

/// The controllers of every configured pipeline, and the task that sets their targets from the
/// ring. Dropped without a [`Self::shutdown`], it stops as a crash would: every controller and
/// worker is aborted and every lease is left to expire.
#[derive(Debug)]
pub struct ClusterReconciler {
    /// Each pipeline's controller status, as reloads add and remove pipelines.
    statuses: watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
    reloads: mpsc::UnboundedSender<Reload>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Drop for ClusterReconciler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ClusterReconciler {
    /// Starts a controller for each pipeline (with its state documents), its target "don't own"
    /// until the ring over `members` gives it to this node.
    pub fn start(
        pipelines: Vec<(Pipeline, PipelineDocs)>,
        deps: &ControllerDeps,
        members: watch::Receiver<Members>,
        settings: ReconcilerSettings,
    ) -> Self {
        let controllers: BTreeMap<String, Controlled> = pipelines
            .into_iter()
            .map(|(pipeline, docs)| {
                let id = pipeline.id.clone();
                (id, Controlled::new(pipeline, docs, Vec::new(), deps))
            })
            .collect();
        let (statuses_tx, statuses) = watch::channel(status_map(&controllers));
        let (reloads, reloaded) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(reconcile(
            Loop {
                controllers,
                deps: deps.clone(),
                takeovers: Cap::new(&deps.starts),
                handoffs: Cap::new(&deps.handoffs),
                statuses: statuses_tx,
                removing: JoinSet::new(),
                returning: BTreeMap::new(),
            },
            members,
            settings,
            reloaded,
            stopped,
        ));
        Self {
            statuses,
            reloads,
            stop: Some(stop),
            task,
        }
    }

    /// Each pipeline's controller status, by pipeline id, as of now.
    pub fn statuses(&self) -> BTreeMap<String, watch::Receiver<ControllerStatus>> {
        self.statuses.borrow().clone()
    }

    /// [`Self::statuses`] as reloads change them.
    #[cfg(test)]
    pub(crate) fn watch_statuses(
        &self,
    ) -> watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>> {
        self.statuses.clone()
    }

    /// Where a config generation's changes go.
    pub fn reloader(&self) -> Reloader {
        Reloader(self.reloads.clone())
    }

    /// Waits until this node holds no pipeline: every controller is on standby.
    pub async fn released(&self) {
        for status in self.statuses().values() {
            let mut status = status.clone();
            let _ = status
                .wait_for(|s| matches!(s.actual, Some(Actual::Standby { .. })))
                .await;
        }
    }

    /// Lets every pipeline go: each controller drains its worker (within `drain_timeout`) and
    /// releases its lease, so the next process acquires it without waiting.
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Err(e) = (&mut self.task).await {
            tracing::error!("the cluster reconciler ended abnormally: {e}");
        }
    }
}

/// True when `added` takes a place `removed` held, so that it starts only once `removed` is let go
/// of: an `id` changed, delete then recreate (config §9.1). The same PostgreSQL slot or
/// publication on the same server, the same sink location, or the same state location.
fn shares(removed: &Pipeline, added: &Pipeline) -> bool {
    let (a, b) = (&removed.source, &added.source);
    let slot = match (&a.postgres, &b.postgres) {
        (Some(x), Some(y)) => {
            a.host == b.host
                && a.port == b.port
                && a.db == b.db
                && (x.slot == y.slot || x.publication == y.publication)
        }
        _ => false,
    };
    let sink = removed.sink.bucket == added.sink.bucket
        && join(&[&removed.sink.prefix]) == join(&[&added.sink.prefix]);
    slot || sink || state_location(removed) == state_location(added)
}

/// Where a pipeline's state lives (config §10): co-located state is under its sink's prefix.
fn state_location(p: &Pipeline) -> Location {
    match &p.state {
        StateLocation::CoLocated => colocated_state(&p.sink.bucket, &p.sink.prefix),
        StateLocation::Explicit {
            bucket,
            parent_prefix,
            prefix,
        } => explicit_state(bucket, parent_prefix.as_deref(), prefix),
    }
}

fn status_map(
    controllers: &BTreeMap<String, Controlled>,
) -> BTreeMap<String, watch::Receiver<ControllerStatus>> {
    controllers
        .iter()
        .map(|(id, c)| (id.clone(), c.status.subscribe()))
        .collect()
}

/// The reconcile loop's state.
struct Loop {
    controllers: BTreeMap<String, Controlled>,
    /// The settings new and started-again controllers take: a reload changes them.
    deps: ControllerDeps,
    takeovers: Cap,
    handoffs: Cap,
    statuses: watch::Sender<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
    /// The controllers of removed pipelines, draining and letting go; each ends with its
    /// pipeline's id.
    removing: JoinSet<String>,
    /// The ids whose removal runs, each with the pipeline a later generation added back under
    /// it: that one starts when the removal ends, so one controller runs a pipeline at a time.
    returning: BTreeMap<String, Option<(Pipeline, PipelineDocs)>>,
}

impl Loop {
    /// A config generation's changes (config §12.3). A removed pipeline's controller is told
    /// and left to finish; it is no longer placed or started again.
    fn reload(&mut self, reload: Reload, placement: &mut Placement) {
        let c = &reload.cluster;
        placement.set_stabilization(c.rebalance_stabilization);
        self.takeovers
            .resize(usize::try_from(c.max_concurrent_takeovers).unwrap_or(usize::MAX));
        self.handoffs
            .resize(usize::try_from(c.max_concurrent_handoffs).unwrap_or(usize::MAX));
        if self.deps.settings.drain_timeout != c.drain_timeout {
            self.deps.settings.drain_timeout = c.drain_timeout;
            for controlled in self.controllers.values() {
                let _ = controlled
                    .updates
                    .send(Update::DrainTimeout(c.drain_timeout));
            }
        }
        // The pipelines this reload removes, as configured: an added one that takes the place of
        // one of them follows it.
        let removed: Vec<Pipeline> = reload
            .pipelines
            .iter()
            .filter_map(|change| match change {
                PipelineReload::Remove(id) => match self.controllers.get(id) {
                    Some(c) => Some(c.pipeline.clone()),
                    None => self.returning.get(id)?.as_ref().map(|(p, _)| p.clone()),
                },
                _ => None,
            })
            .collect();
        for change in reload.pipelines {
            match change {
                PipelineReload::Add(pipeline, docs) => {
                    let id = pipeline.id.clone();
                    // Not a change a diff makes; the old controller is let go of, as for a
                    // removal.
                    if let Some(old) = self.controllers.remove(&id) {
                        self.remove(old);
                    }
                    if let Some(back) = self.returning.get_mut(&id) {
                        tracing::info!(
                            pipeline = %pipeline.name,
                            "pipeline added back: it starts once its removal ends"
                        );
                        *back = Some((pipeline, docs));
                    } else {
                        tracing::info!(pipeline = %pipeline.name, "pipeline added");
                        let following = removed
                            .iter()
                            .filter(|r| r.id != id && shares(r, &pipeline))
                            .map(|r| r.id.clone())
                            .collect();
                        let added = Controlled::new(pipeline, docs, following, &self.deps);
                        self.controllers.insert(id, added);
                    }
                }
                PipelineReload::Remove(id) => {
                    if let Some(old) = self.controllers.remove(&id) {
                        self.remove(old);
                    } else if let Some(back) = self.returning.get_mut(&id) {
                        *back = None;
                    }
                }
                PipelineReload::Reconfigure(pipeline) => {
                    if let Some(c) = self.controllers.get_mut(&pipeline.id) {
                        c.pipeline = pipeline.clone();
                        let _ = c.updates.send(Update::Reconfigure(Box::new(pipeline)));
                    } else if let Some(Some(back)) = self.returning.get_mut(&pipeline.id) {
                        back.0 = pipeline;
                    }
                }
                PipelineReload::Restart(pipeline, docs) => {
                    if let Some(c) = self.controllers.get_mut(&pipeline.id) {
                        c.pipeline = pipeline.clone();
                        c.docs = docs.clone();
                        let _ = c.updates.send(Update::Restart(Box::new(pipeline), docs));
                    } else if let Some(Some(back)) = self.returning.get_mut(&pipeline.id) {
                        *back = (pipeline, docs);
                    }
                }
            }
        }
        self.statuses.send_replace(status_map(&self.controllers));
    }

    /// Lets a removed pipeline's controller finish, in the background.
    fn remove(&mut self, old: Controlled) {
        let id = old.pipeline.id.clone();
        self.returning.entry(id.clone()).or_insert(None);
        self.removing.spawn(async move {
            old.remove().await;
            id
        });
    }

    /// A removal ended: the pipeline added back under its id, if any, starts now.
    fn removed(&mut self, id: &str) {
        if let Some(Some((pipeline, docs))) = self.returning.remove(id) {
            tracing::info!(pipeline = %pipeline.name, "pipeline added back, its removal done");
            let added = Controlled::new(pipeline, docs, Vec::new(), &self.deps);
            self.controllers.insert(id.to_owned(), added);
            self.statuses.send_replace(status_map(&self.controllers));
        }
    }
}

/// The reconcile loop (Clustering §8.1, §20.1): on every membership change, every due ring
/// addition, every reload and every `resync`, each pipeline's target is "own" iff the ring
/// places it here.
async fn reconcile(
    mut state: Loop,
    mut members: watch::Receiver<Members>,
    settings: ReconcilerSettings,
    mut reloads: mpsc::UnboundedReceiver<Reload>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut placement = Placement::new(settings.tokens, settings.stabilization);
    let mut resync = tokio::time::interval(settings.resync);
    resync.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut gossiping = true;
    loop {
        let view = members.borrow_and_update().clone();
        if placement.update(&view, Instant::now()) {
            tracing::info!(
                nodes = ?placement.ring().nodes(),
                generation = placement.generation(),
                "the ring changed"
            );
        }
        let deps = &state.deps;
        deps.metrics.set_cluster_members(view.nodes.len());
        deps.metrics.set_ring_generation(placement.generation());
        state.takeovers.settle();
        state.handoffs.settle();
        for (id, c) in &mut state.controllers {
            let want = if placement.ring().owner(id) == Some(view.me.as_str()) {
                Target::Own
            } else {
                Target::DontOwn
            };
            c.target
                .send_if_modified(|t| std::mem::replace(t, want) != want);
            // A controller ends only when its target closes: one that ended panicked
            // (DA-OPS-7). Its worker went with it; the next one takes the lease over.
            if c.task.is_finished() {
                tracing::error!(
                    pipeline = %c.pipeline.name,
                    "the pipeline's controller ended unasked; it is started again"
                );
                c.spawn(deps);
            }
        }
        let due = placement.next_due();
        tokio::select! {
            _ = &mut stop => break,
            Some(reload) = reloads.recv() => state.reload(reload, &mut placement),
            // A removed pipeline's controller that ended.
            Some(ended) = state.removing.join_next() => {
                if let Ok(id) = ended {
                    state.removed(&id);
                }
            }
            changed = members.changed(), if gossiping => {
                gossiping = changed.is_ok();
                if !gossiping {
                    tracing::error!("the gossip stopped: placement keeps its last view");
                }
            }
            () = async {
                match due {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {}
            _ = resync.tick() => {}
        }
    }
    // Every target closes at once (a replaced sender is dropped): the controllers drain
    // together, and the removed pipelines' controllers finish.
    let mut controllers: Vec<Controlled> = state.controllers.into_values().collect();
    for c in &mut controllers {
        c.target = watch::channel(Target::DontOwn).0;
    }
    for c in &mut controllers {
        if let Err(e) = (&mut c.task).await {
            tracing::error!("a pipeline controller ended abnormally: {e}");
        }
    }
    while state.removing.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use data_avalanche_core::{SourceError, TableRef};
    use data_avalanche_testkit::{FakeTxn, Method};

    use super::*;
    use crate::test_support::{
        ByPipeline, STABLE, ScriptedStore, TestFactory, alone, config, deps, eventually, leases,
        members, pipeline_docs, row, until,
    };
    use crate::{Parts, Phase, Ring, WorkerFactory};

    fn running(s: &ControllerStatus) -> bool {
        s.actual == Some(Actual::Running)
    }

    fn standby(s: &ControllerStatus) -> bool {
        matches!(s.actual, Some(Actual::Standby { .. }))
    }

    fn reconciler_settings() -> ReconcilerSettings {
        ReconcilerSettings {
            tokens: 128,
            stabilization: STABLE,
            resync: Duration::from_secs(2),
        }
    }

    /// A factory per pipeline id of `ids`, and the deps that build from them.
    fn factories(
        store: &Arc<ScriptedStore>,
        ids: &[&str],
    ) -> (BTreeMap<String, TestFactory>, ControllerDeps) {
        let fs: BTreeMap<String, TestFactory> = ids
            .iter()
            .map(|id| ((*id).to_owned(), TestFactory::new(&["a"])))
            .collect();
        let mut d = deps("n1", store, &fs[ids[0]]);
        d.factory = Arc::new(ByPipeline(fs.clone()));
        (fs, d)
    }

    fn pipelines(ids: &[&str]) -> Vec<(Pipeline, PipelineDocs)> {
        let mut cfg = config(ids).pipelines;
        ids.iter()
            .map(|id| (cfg.remove(*id).unwrap(), pipeline_docs()))
            .collect()
    }

    /// Two pipelines `p` and `q` on a lone node; `starts` workers may be in Phase 0 at once.
    fn two(
        store: &Arc<ScriptedStore>,
        starts: usize,
    ) -> (ClusterReconciler, TestFactory, TestFactory, ControllerDeps) {
        let (fs, mut d) = factories(store, &["p", "q"]);
        d.starts = Arc::new(tokio::sync::Semaphore::new(starts));
        let (_, view) = watch::channel(alone("n1"));
        let r = ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
        (r, fs["p"].clone(), fs["q"].clone(), d)
    }

    #[test]
    fn the_node_is_its_node_name_else_its_hostname() {
        let started = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        let mut cluster = config(&["p"]).globals.cluster;
        let h = node_identity(&cluster, started).unwrap();
        assert_eq!(
            (h.node_id.as_str(), h.incarnation),
            ("n1", 1_700_000_000_123)
        );
        cluster.memberlist.node_name = None;
        let h = node_identity(&cluster, started).unwrap();
        assert_eq!(h.node_id, gethostname::gethostname().into_string().unwrap());
    }

    #[test]
    fn the_settings_come_from_the_cluster() {
        let c = config(&["p"]).globals.cluster;
        let s = ReconcilerSettings::new(&c);
        assert_eq!(
            (s.tokens, s.stabilization, s.resync),
            (
                c.tokens,
                c.rebalance_stabilization,
                c.lease_renewal_interval
            )
        );
    }

    #[tokio::test(start_paused = true)]
    async fn every_pipeline_runs_here_and_a_shutdown_releases_every_lease() {
        let store = ScriptedStore::new();
        let (r, fp, fq, d) = two(&store, 2);
        for (_, mut s) in r.statuses() {
            until(&mut s, "running", running).await;
        }
        fp.source
            .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(1)));
        fq.source
            .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(2)));
        eventually("both land", || fp.ids("a") == [1] && fq.ids("a") == [2]).await;
        let text = d.metrics.encode();
        for line in [
            "data_avalanche_owned_pipelines 2",
            "data_avalanche_cluster_members 1",
            "data_avalanche_ring_generation 1",
        ] {
            assert!(text.lines().any(|l| l == line), "{line}: {text}");
        }
        r.shutdown().await;
        for id in ["p", "q"] {
            let lease = leases(&store).observe(id).await.unwrap().unwrap();
            assert!(lease.doc.released, "{id}");
        }
        assert!(
            d.metrics
                .encode()
                .lines()
                .any(|l| l == "data_avalanche_owned_pipelines 0")
        );
    }

    /// A reconciler dropped without a shutdown is a crash: nothing releases or renews.
    #[tokio::test(start_paused = true)]
    async fn a_reconciler_dropped_is_a_crash_and_its_leases_stay() {
        let store = ScriptedStore::new();
        let (r, _, _, _) = two(&store, 2);
        for (_, mut s) in r.statuses() {
            until(&mut s, "running", running).await;
        }
        drop(r);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let held = || async {
            let mut v = Vec::new();
            for id in ["p", "q"] {
                let lease = leases(&store).observe(id).await.unwrap().unwrap();
                assert!(!lease.doc.released, "{id}");
                v.push(lease.version);
            }
            v
        };
        let before = held().await;
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(held().await, before, "no renewal after the crash");
    }

    /// `max_concurrent_takeovers` 1: while `p`'s Phase 0 retries its preflight, `q` builds no
    /// worker; it starts once `p` streams.
    #[tokio::test(start_paused = true)]
    async fn one_phase_0_at_a_time_holds_the_other_start() {
        let store = ScriptedStore::new();
        let (fs, mut d) = factories(&store, &["p", "q"]);
        let (fp, fq) = (fs["p"].clone(), fs["q"].clone());
        // About 30 s of Phase 0 (a 5 ms backoff each).
        for _ in 0..6_000 {
            fp.source
                .fail(Method::Preflight, SourceError::Transient("slow".into()));
        }
        d.starts = Arc::new(tokio::sync::Semaphore::new(1));
        // `q` is claimed later: another holder's lease, taken over after a lease_duration
        // (15 s); `p`'s lease is free, claimed at once, and its Phase 0 takes the permit.
        let n0 = crate::test_support::holder("n0");
        leases(&store)
            .acquire("q", &n0, Duration::from_secs(1), chrono::Utc::now())
            .await
            .unwrap();
        let (_, view) = watch::channel(alone("n1"));
        let r = ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
        let mut st = r.statuses();
        eventually("p is in Phase 0", || fp.builds() == 1).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            st["p"].borrow().actual,
            Some(Actual::Starting),
            "p still retries its preflight"
        );
        until(st.get_mut("q").unwrap(), "q claimed", |s| {
            s.owner_epoch.is_some()
        })
        .await;
        assert_eq!(fq.builds(), 0, "q waits for the permit");
        until(st.get_mut("p").unwrap(), "p running", running).await;
        until(st.get_mut("q").unwrap(), "q running", running).await;
        assert_eq!(fq.builds(), 1);
        r.shutdown().await;
    }

    /// A node joins: after `rebalance_stabilization` the pipelines the ring gives it are
    /// drained and released here, the others keep running.
    #[tokio::test(start_paused = true)]
    async fn a_joiner_gets_its_pipelines_after_the_stabilization() {
        let ids = ["orders", "events", "customers", "users"];
        let store = ScriptedStore::new();
        let (_, d) = factories(&store, &ids);
        let (tx, view) = watch::channel(alone("n1"));
        let r = ClusterReconciler::start(pipelines(&ids), &d, view, reconciler_settings());
        let mut st = r.statuses();
        for s in st.values_mut() {
            until(s, "running", running).await;
        }
        let two = Ring::new(["n1", "n2"], 128);
        let moving: Vec<&str> = ids
            .iter()
            .copied()
            .filter(|id| two.owner(id) == Some("n2"))
            .collect();
        assert!(!moving.is_empty() && moving.len() < ids.len(), "{moving:?}");
        tx.send_replace(members(
            "n1",
            &[("n1", Phase::Active, false), ("n2", Phase::Active, false)],
        ));
        tokio::time::sleep(STABLE - Duration::from_secs(1)).await;
        assert!(
            st.values().all(|s| running(&s.borrow())),
            "nothing moves before 15 s"
        );
        for id in &moving {
            until(st.get_mut(*id).unwrap(), "handed off", standby).await;
            let lease = leases(&store).observe(id).await.unwrap().unwrap();
            assert!(lease.doc.released, "{id}");
        }
        for id in ids.iter().filter(|id| !moving.contains(id)) {
            assert!(running(&st[*id].borrow()), "{id} stays");
        }
        assert!(
            d.metrics
                .encode()
                .lines()
                .any(|l| l == "data_avalanche_ring_generation 2")
        );
        r.shutdown().await;
    }

    /// `max_concurrent_handoffs` 1: two hand-offs whose drains hang run one after the other,
    /// each cut off at `drain_timeout` (5 s); with 2 they run together.
    #[tokio::test(start_paused = true)]
    async fn the_hand_off_cap_drains_one_pipeline_at_a_time() {
        for (cap, least, most) in [(1, 10, 11), (2, 5, 6)] {
            let store = ScriptedStore::new();
            let (fs, mut d) = factories(&store, &["p", "q"]);
            d.handoffs = Arc::new(tokio::sync::Semaphore::new(cap));
            let (tx, view) = watch::channel(alone("n1"));
            let r =
                ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
            let mut st = r.statuses();
            for s in st.values_mut() {
                until(s, "running", running).await;
            }
            for (i, f) in (1..).zip(fs.values()) {
                f.sink.hold_commits();
                f.source
                    .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(i)));
            }
            eventually("both commits wait", || {
                fs.values().all(|f| f.sink.waiting_commits() == 1)
            })
            .await;
            let t = Instant::now();
            tx.send_replace(members("n1", &[("n1", Phase::Draining, false)]));
            r.released().await;
            let took = t.elapsed();
            assert!(
                took >= Duration::from_secs(least) && took < Duration::from_secs(most),
                "cap {cap}: {took:?}"
            );
            for f in fs.values() {
                f.sink.release_commits();
            }
            r.shutdown().await;
        }
    }

    /// A factory whose first build panics, as a bug in a controller would.
    #[derive(Debug)]
    struct PanicsOnce(TestFactory, AtomicBool);

    impl WorkerFactory for PanicsOnce {
        fn build(&self, p: &Pipeline) -> Result<Parts, String> {
            assert!(self.1.swap(true, Ordering::SeqCst), "a bug");
            self.0.build(p)
        }
    }

    /// DA-OPS-7: a controller that panics is started again at the next resync; it takes its
    /// own lease over under the next epoch.
    #[tokio::test(start_paused = true)]
    async fn a_controller_that_panics_is_started_again() {
        let store = ScriptedStore::new();
        let (fs, mut d) = factories(&store, &["p"]);
        d.factory = Arc::new(PanicsOnce(fs["p"].clone(), AtomicBool::new(false)));
        let (_, view) = watch::channel(alone("n1"));
        let r = ClusterReconciler::start(pipelines(&["p"]), &d, view, reconciler_settings());
        let mut st = r.statuses();
        until(st.get_mut("p").unwrap(), "running", running).await;
        assert_eq!(st["p"].borrow().owner_epoch, Some(2));
        assert_eq!(fs["p"].builds(), 1, "the panicking build counted none");
        let text = d.metrics.encode();
        assert!(
            text.lines()
                .any(|l| l == "data_avalanche_owned_pipelines 1"),
            "{text}"
        );
        r.shutdown().await;
    }

    /// `globals.cluster` of the test config, with the drain timeout the tests' controllers
    /// start with (5 s).
    fn cluster() -> Cluster {
        let mut c = config(&["p"]).globals.cluster;
        c.drain_timeout = Duration::from_secs(5);
        c
    }

    /// A reload adds a pipeline, which runs here (the ring places everything on a lone node),
    /// and removes one, which is drained, cleaned up and released, before a shutdown ends; the
    /// third runs on.
    #[tokio::test(start_paused = true)]
    async fn a_reload_adds_and_removes_pipelines() {
        let store = ScriptedStore::new();
        let (fs, d) = factories(&store, &["p", "q", "r"]);
        let (_, view) = watch::channel(alone("n1"));
        let r = ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
        for (_, mut s) in r.statuses() {
            until(&mut s, "running", running).await;
        }
        let added = pipelines(&["r"]).remove(0);
        assert!(r.reloader().apply(Reload {
            pipelines: vec![
                PipelineReload::Add(added.0, added.1),
                PipelineReload::Remove("q".into()),
            ],
            cluster: cluster(),
        }));
        eventually("r in, q out", || {
            let st = r.statuses();
            st.contains_key("r") && !st.contains_key("q")
        })
        .await;
        until(r.statuses().get_mut("r").unwrap(), "r running", running).await;
        fs["r"]
            .source
            .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(1)));
        eventually("r's row lands", || fs["r"].ids("a") == [1]).await;
        assert!(running(&r.statuses()["p"].borrow()), "p runs on");
        r.shutdown().await;
        assert_eq!(fs["q"].source.teardowns(), 1, "q cleaned up");
        assert_eq!(fs["p"].source.teardowns(), 0);
        let q = leases(&store).observe("q").await.unwrap().unwrap();
        assert!(q.doc.released);
    }

    /// A pipeline added back while its removal runs starts once the removal ends: the old
    /// controller drains, tears down and releases first, so one controller runs it at a time.
    #[tokio::test(start_paused = true)]
    async fn a_pipeline_added_back_during_its_removal_starts_after_it() {
        let store = ScriptedStore::new();
        let (fs, d) = factories(&store, &["p"]);
        let f = fs["p"].clone();
        let started = pipelines(&["p"]);
        let back = started[0].clone();
        let (_, view) = watch::channel(alone("n1"));
        let r = ClusterReconciler::start(started, &d, view, reconciler_settings());
        until(r.statuses().get_mut("p").unwrap(), "running", running).await;
        f.sink.hold_commits();
        f.source
            .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(1)));
        eventually("the commit waits", || f.sink.waiting_commits() == 1).await;
        for change in [
            PipelineReload::Remove("p".into()),
            PipelineReload::Add(back.0, back.1),
        ] {
            r.reloader().apply(Reload {
                pipelines: vec![change],
                cluster: cluster(),
            });
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(
            !r.statuses().contains_key("p"),
            "not while its removal runs"
        );
        assert_eq!(f.builds(), 1);
        f.sink.release_commits();
        eventually("p is back", || r.statuses().contains_key("p")).await;
        assert_eq!(f.source.teardowns(), 1, "torn down before it is back");
        until(r.statuses().get_mut("p").unwrap(), "running again", running).await;
        r.shutdown().await;
    }

    /// `x` and `y` of the test config share nothing; each change to `y` below shares one place
    /// of `x`'s.
    #[test]
    fn a_pipeline_shares_a_slot_a_publication_a_sink_or_a_state() {
        let mut cfg = config(&["x", "y"]).pipelines;
        let (x, y) = (cfg.remove("x").unwrap(), cfg.remove("y").unwrap());
        assert!(!shares(&x, &y));
        let pg = |f: fn(&mut data_avalanche_config::Postgres)| {
            let mut y = y.clone();
            f(y.source.postgres.as_mut().unwrap());
            y
        };
        assert!(shares(&x, &pg(|p| p.slot = "x_slot".into())));
        assert!(shares(&x, &pg(|p| p.publication = "x_pub".into())));
        // The same names on another server, or in another database, are other objects.
        let mut other = pg(|p| p.slot = "x_slot".into());
        other.source.port += 1;
        assert!(!shares(&x, &other));
        let mut other = pg(|p| p.publication = "x_pub".into());
        other.source.host = "10.0.0.9".into();
        assert!(!shares(&x, &other));
        let mut other = pg(|p| p.slot = "x_slot".into());
        other.source.db = "other".into();
        assert!(!shares(&x, &other));
        // MySQL has no slot: the same server is no share; its sink and its state are.
        let mysql = |p: &Pipeline| {
            let mut p = p.clone();
            p.source.kind = data_avalanche_config::SourceType::Mysql;
            p.source.postgres = None;
            p
        };
        assert!(!shares(&mysql(&x), &mysql(&y)));
        let mut sink = mysql(&y);
        sink.sink.prefix = "x/".into();
        assert!(shares(&mysql(&x), &sink));
        // The same prefix in another bucket is another sink, and another co-located state.
        let mut other = y.clone();
        other.sink.bucket = "other".into();
        other.sink.prefix = "x".into();
        assert!(!shares(&x, &other));
        let explicit = StateLocation::Explicit {
            bucket: "app-state".into(),
            parent_prefix: None,
            prefix: "s".into(),
        };
        let (mut xs, mut ys) = (mysql(&x), mysql(&y));
        xs.state = explicit.clone();
        ys.state = explicit;
        assert!(shares(&xs, &ys));
        // Co-located state is under the sink: `x`'s, when `y` keeps its explicit state there.
        ys.state = StateLocation::Explicit {
            bucket: "lake".into(),
            parent_prefix: None,
            prefix: "x/.pipeline-state".into(),
        };
        assert!(shares(&mysql(&x), &ys));
    }

    /// Builds as [`ByPipeline`] does, and records, at each build of `follower`'s parts, how many
    /// teardowns `leader`'s source has had.
    #[derive(Debug)]
    struct Ordered {
        by: ByPipeline,
        follower: &'static str,
        leader: &'static str,
        seen: Arc<Mutex<Vec<usize>>>,
    }

    impl WorkerFactory for Ordered {
        fn build(&self, p: &Pipeline) -> Result<Parts, String> {
            if p.id == self.follower {
                let torn = self.by.0[self.leader].source.teardowns();
                self.seen.lock().unwrap().push(torn);
            }
            self.by.build(p)
        }
    }

    /// `x` runs on a lone node, its commit held so that its removal's drain waits (up to
    /// 60 s); `y`'s parts are built through [`Ordered`].
    async fn x_with_its_drain_held(
        store: &Arc<ScriptedStore>,
    ) -> (
        ClusterReconciler,
        BTreeMap<String, TestFactory>,
        Arc<Mutex<Vec<usize>>>,
    ) {
        let (fs, mut d) = factories(store, &["x", "y"]);
        let seen = Arc::new(Mutex::new(Vec::new()));
        d.factory = Arc::new(Ordered {
            by: ByPipeline(fs.clone()),
            follower: "y",
            leader: "x",
            seen: Arc::clone(&seen),
        });
        d.settings.drain_timeout = Duration::from_secs(60);
        let (_, view) = watch::channel(alone("n1"));
        let r = ClusterReconciler::start(pipelines(&["x"]), &d, view, reconciler_settings());
        until(r.statuses().get_mut("x").unwrap(), "x running", running).await;
        let fx = &fs["x"];
        fx.sink.hold_commits();
        fx.source
            .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(1)));
        eventually("x's commit waits", || fx.sink.waiting_commits() == 1).await;
        (r, fs, seen)
    }

    /// One reload adds `y` and removes `x`, with the 60 s drain timeout.
    fn replace_x(r: &ClusterReconciler, y: Pipeline, docs: PipelineDocs) {
        let mut c = cluster();
        c.drain_timeout = Duration::from_secs(60);
        assert!(r.reloader().apply(Reload {
            pipelines: vec![
                PipelineReload::Add(y, docs),
                PipelineReload::Remove("x".into()),
            ],
            cluster: c,
        }));
    }

    /// An `id` change that keeps the slot (config §9.1: delete, then recreate): the pipeline
    /// under the new id waits while the removed one drains, its lease renewed, and starts once
    /// the removal tore the source down and released the lease.
    #[tokio::test(start_paused = true)]
    async fn a_pipeline_taking_a_removed_ones_slot_starts_once_the_removal_lets_go() {
        let store = ScriptedStore::new();
        let (r, fs, seen) = x_with_its_drain_held(&store).await;
        let (mut y, docs) = pipelines(&["y"]).remove(0);
        y.source.postgres.as_mut().unwrap().slot = "x_slot".into();
        replace_x(&r, y, docs);
        eventually("y in", || r.statuses().contains_key("y")).await;
        // Past two lease durations: x's lease is renewed meanwhile, so it isn't let go.
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(fs["y"].builds(), 0, "not while x's lease is held");
        assert!(leases(&store).observe("y").await.unwrap().is_none());
        let x = leases(&store).observe("x").await.unwrap().unwrap();
        assert!(!x.doc.released);
        fs["x"].sink.release_commits();
        until(r.statuses().get_mut("y").unwrap(), "y running", running).await;
        assert_eq!(
            *seen.lock().unwrap(),
            [1],
            "x torn down before y's first build"
        );
        let x = leases(&store).observe("x").await.unwrap().unwrap();
        assert!(x.doc.released);
        let y = leases(&store).observe("y").await.unwrap().unwrap();
        assert!(y.doc.is_held_by(&crate::test_support::holder("n1")));
        r.shutdown().await;
    }

    /// The wait is a follower's only: a pipeline added that shares nothing with the removed one
    /// starts while the removal still drains.
    #[tokio::test(start_paused = true)]
    async fn a_pipeline_added_that_shares_nothing_starts_during_a_removal() {
        let store = ScriptedStore::new();
        let (r, fs, seen) = x_with_its_drain_held(&store).await;
        let (y, docs) = pipelines(&["y"]).remove(0);
        replace_x(&r, y, docs);
        eventually("y in", || r.statuses().contains_key("y")).await;
        until(r.statuses().get_mut("y").unwrap(), "y running", running).await;
        assert_eq!(fs["x"].sink.waiting_commits(), 1, "x still drains");
        assert_eq!(*seen.lock().unwrap(), [0]);
        fs["x"].sink.release_commits();
        r.shutdown().await;
        assert_eq!(fs["x"].source.teardowns(), 1);
    }

    /// A reload's in-place change reaches the running worker; a restart-only one runs a new
    /// worker.
    #[tokio::test(start_paused = true)]
    async fn a_reload_reconfigures_one_pipeline_and_restarts_another() {
        let store = ScriptedStore::new();
        let (fs, d) = factories(&store, &["p", "q"]);
        let (fp, fq) = (fs["p"].clone(), fs["q"].clone());
        let started = pipelines(&["p", "q"]);
        let q_docs = started[1].1.clone();
        let (_, view) = watch::channel(alone("n1"));
        let r = ClusterReconciler::start(started, &d, view, reconciler_settings());
        for (_, mut s) in r.statuses() {
            until(&mut s, "running", running).await;
        }
        let mut cfg = config(&["p", "q"]).pipelines;
        r.reloader().apply(Reload {
            pipelines: vec![
                PipelineReload::Reconfigure(cfg.remove("p").unwrap()),
                PipelineReload::Restart(cfg.remove("q").unwrap(), q_docs),
            ],
            cluster: cluster(),
        });
        eventually("p reconfigured, q rebuilt", || {
            fp.source.reconfigures() == 1 && fq.builds() == 2
        })
        .await;
        until(r.statuses().get_mut("q").unwrap(), "q running", running).await;
        assert_eq!(fp.builds(), 1, "p's worker runs on");
        r.shutdown().await;
    }

    /// A reloaded `max_concurrent_handoffs` caps the next hand-offs: from 1 to 2 the two hung
    /// drains run together, from 2 to 1 one after the other (each cut off at 5 s).
    #[tokio::test(start_paused = true)]
    async fn a_reloaded_hand_off_cap_caps_the_next_hand_offs() {
        for (from, to, least, most) in [(1, 2, 5, 6), (2, 1, 10, 11)] {
            let store = ScriptedStore::new();
            let (fs, mut d) = factories(&store, &["p", "q"]);
            d.handoffs = Arc::new(tokio::sync::Semaphore::new(from));
            let (tx, view) = watch::channel(alone("n1"));
            let r =
                ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
            let mut st = r.statuses();
            for s in st.values_mut() {
                until(s, "running", running).await;
            }
            let mut c = cluster();
            c.max_concurrent_handoffs = to;
            r.reloader().apply(Reload {
                pipelines: vec![],
                cluster: c,
            });
            for (i, f) in (1..).zip(fs.values()) {
                f.sink.hold_commits();
                f.source
                    .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(i)));
            }
            eventually("both commits wait", || {
                fs.values().all(|f| f.sink.waiting_commits() == 1)
            })
            .await;
            let t = Instant::now();
            tx.send_replace(members("n1", &[("n1", Phase::Draining, false)]));
            r.released().await;
            let took = t.elapsed();
            assert!(
                took >= Duration::from_secs(least) && took < Duration::from_secs(most),
                "cap {from} to {to}: {took:?}"
            );
            for f in fs.values() {
                f.sink.release_commits();
            }
            r.shutdown().await;
        }
    }

    /// A reloaded `rebalance_stabilization` is the next joiner's wait.
    #[tokio::test(start_paused = true)]
    async fn a_reloaded_stabilization_is_the_next_joiners_wait() {
        let ids = ["orders", "events", "customers", "users"];
        let store = ScriptedStore::new();
        let (_, d) = factories(&store, &ids);
        let (tx, view) = watch::channel(alone("n1"));
        let r = ClusterReconciler::start(pipelines(&ids), &d, view, reconciler_settings());
        let mut st = r.statuses();
        for s in st.values_mut() {
            until(s, "running", running).await;
        }
        let mut c = cluster();
        c.rebalance_stabilization = Duration::from_secs(3);
        r.reloader().apply(Reload {
            pipelines: vec![],
            cluster: c,
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let two = Ring::new(["n1", "n2"], 128);
        let moving: Vec<&str> = ids
            .iter()
            .copied()
            .filter(|id| two.owner(id) == Some("n2"))
            .collect();
        let t = Instant::now();
        tx.send_replace(members(
            "n1",
            &[("n1", Phase::Active, false), ("n2", Phase::Active, false)],
        ));
        for id in &moving {
            until(st.get_mut(*id).unwrap(), "handed off", standby).await;
        }
        assert!(t.elapsed() < STABLE, "{:?}", t.elapsed());
        r.shutdown().await;
    }
}
