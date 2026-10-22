//! The node's reconciler (Clustering §8.1, §20). It runs one controller per pipeline and tells
//! each whether to own its pipeline: "own" when the ring over the members places the pipeline on
//! this node. It never touches a lease; each controller claims and lets go of its own.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use data_avalanche_config::{Cluster, Pipeline};
use data_avalanche_state::{Holder, PipelineDocs};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::{
    Actual, ClusterError, ControllerDeps, ControllerStatus, Members, PipelineController, Placement,
    Target,
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

#[derive(Debug)]
struct Controlled {
    pipeline: Pipeline,
    docs: PipelineDocs,
    target: watch::Sender<Target>,
    status: watch::Sender<ControllerStatus>,
    task: JoinHandle<()>,
}

/// A controller dropped is aborted, its worker with it: a crash, its lease left to expire.
impl Drop for Controlled {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Controlled {
    fn spawn(&mut self, deps: &ControllerDeps) {
        let controller = PipelineController::reporting(
            self.pipeline.clone(),
            self.docs.clone(),
            deps,
            self.status.clone(),
        );
        self.task = tokio::spawn(controller.run(self.target.subscribe()));
    }
}

/// The controllers of every configured pipeline, and the task that sets their targets from the
/// ring. Dropped without a [`Self::shutdown`], it stops as a crash would: every controller and
/// worker is aborted and every lease is left to expire.
#[derive(Debug)]
pub struct ClusterReconciler {
    statuses: BTreeMap<String, watch::Receiver<ControllerStatus>>,
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
        let mut statuses = BTreeMap::new();
        let controllers = pipelines
            .into_iter()
            .map(|(pipeline, docs)| {
                let id = pipeline.id.clone();
                let (status, rx) = watch::channel(ControllerStatus::default());
                statuses.insert(id.clone(), rx);
                let mut c = Controlled {
                    pipeline,
                    docs,
                    target: watch::channel(Target::DontOwn).0,
                    status,
                    task: tokio::spawn(async {}),
                };
                c.spawn(deps);
                (id, c)
            })
            .collect();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(reconcile(
            controllers,
            deps.clone(),
            members,
            settings,
            stopped,
        ));
        Self {
            statuses,
            stop: Some(stop),
            task,
        }
    }

    /// Each pipeline's controller status, by pipeline id.
    pub fn statuses(&self) -> BTreeMap<String, watch::Receiver<ControllerStatus>> {
        self.statuses.clone()
    }

    /// Waits until this node holds no pipeline: every controller is on standby.
    pub async fn released(&self) {
        for status in self.statuses.values() {
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

/// The reconcile loop (Clustering §8.1, §20.1): on every membership change, every due ring
/// addition and every `resync`, each pipeline's target is "own" iff the ring places it here.
async fn reconcile(
    mut controllers: BTreeMap<String, Controlled>,
    deps: ControllerDeps,
    mut members: watch::Receiver<Members>,
    settings: ReconcilerSettings,
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
        deps.metrics.set_cluster_members(view.nodes.len());
        deps.metrics.set_ring_generation(placement.generation());
        for (id, c) in &mut controllers {
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
                c.spawn(&deps);
            }
        }
        let due = placement.next_due();
        tokio::select! {
            _ = &mut stop => break,
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
    // together.
    let mut controllers: Vec<Controlled> = controllers.into_values().collect();
    for c in &mut controllers {
        c.target = watch::channel(Target::DontOwn).0;
    }
    for c in &mut controllers {
        if let Err(e) = (&mut c.task).await {
            tracing::error!("a pipeline controller ended abnormally: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

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
}
