//! The node's reconciler (Clustering §8.1, §20). It runs one controller per pipeline and tells
//! each whether to own its pipeline. M6b's ring holds this node alone, so one tick sets every
//! target to "own"; M8 derives the targets from membership.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use data_avalanche_config::{Cluster, Pipeline};
use data_avalanche_state::{Holder, PipelineDocs};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{ControllerDeps, ControllerStatus, PipelineController, Target};

/// This node as a lease holder (Clustering §3.4, D-1): `node_id` is
/// `globals.cluster.memberlist.node_name`, else the OS hostname; the incarnation is the process's
/// start in unix milliseconds, so a restarted process is another holder.
///
/// # Errors
/// No `node_name` and no usable hostname.
pub fn node_identity(cluster: &Cluster, started: SystemTime) -> Result<Holder, String> {
    let node_id = match &cluster.memberlist.node_name {
        Some(name) => name.clone(),
        None => gethostname::gethostname().into_string().map_err(|h| {
            format!("the hostname {h:?} isn't UTF-8: set globals.cluster.memberlist.node_name")
        })?,
    };
    if node_id.is_empty() {
        return Err("the hostname is empty: set globals.cluster.memberlist.node_name".into());
    }
    let incarnation = started
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("the clock is before 1970: {e}"))?
        .as_millis();
    Ok(Holder {
        node_id,
        incarnation: u64::try_from(incarnation).unwrap_or(u64::MAX),
    })
}

#[derive(Debug)]
struct Controlled {
    target: watch::Sender<Target>,
    status: watch::Receiver<ControllerStatus>,
    task: JoinHandle<()>,
}

/// The controllers of every configured pipeline.
#[derive(Debug)]
pub struct ClusterReconciler {
    controllers: BTreeMap<String, Controlled>,
}

impl ClusterReconciler {
    /// Starts a controller for each pipeline (with its state documents) and runs one reconcile
    /// tick of a self-only ring: every pipeline's target is "own".
    pub fn start(pipelines: Vec<(Pipeline, PipelineDocs)>, deps: &ControllerDeps) -> Self {
        deps.metrics.set_cluster_members(1);
        let controllers = pipelines
            .into_iter()
            .map(|(pipeline, docs)| {
                let id = pipeline.id.clone();
                let (controller, status) = PipelineController::new(pipeline, docs, deps);
                let (target, rx) = watch::channel(Target::Own);
                let task = tokio::spawn(controller.run(rx));
                (
                    id,
                    Controlled {
                        target,
                        status,
                        task,
                    },
                )
            })
            .collect();
        Self { controllers }
    }

    /// Each pipeline's controller status, by pipeline id.
    pub fn statuses(&self) -> BTreeMap<String, watch::Receiver<ControllerStatus>> {
        self.controllers
            .iter()
            .map(|(id, c)| (id.clone(), c.status.clone()))
            .collect()
    }

    /// Lets every pipeline go: each controller drains its worker (within `drain_timeout`) and
    /// releases its lease, so the next process acquires it without waiting.
    pub async fn shutdown(self) {
        let tasks: Vec<JoinHandle<()>> = self
            .controllers
            .into_values()
            .map(|c| {
                drop(c.target);
                c.task
            })
            .collect();
        for task in tasks {
            if let Err(e) = task.await {
                tracing::error!("a pipeline controller ended abnormally: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use data_avalanche_core::{SourceError, TableRef};
    use data_avalanche_testkit::{FakeTxn, Method};

    use super::*;
    use crate::test_support::{
        ScriptedStore, TestFactory, config, deps, eventually, leases, pipeline_docs, row, until,
    };
    use crate::{Actual, Parts, WorkerFactory};

    /// One [`TestFactory`] per pipeline id.
    #[derive(Debug)]
    struct ByPipeline(BTreeMap<String, TestFactory>);

    impl WorkerFactory for ByPipeline {
        fn build(&self, p: &Pipeline) -> Result<Parts, String> {
            self.0[&p.id].build(p)
        }
    }

    fn running(s: &ControllerStatus) -> bool {
        s.actual == Some(Actual::Running)
    }

    /// Two pipelines `p` and `q` on one node; `starts` workers may be in Phase 0 at once.
    fn two(
        store: &Arc<ScriptedStore>,
        starts: usize,
    ) -> (ClusterReconciler, TestFactory, TestFactory, ControllerDeps) {
        let (fp, fq) = (TestFactory::new(&["a"]), TestFactory::new(&["a"]));
        let mut d = deps("n1", store, &fp);
        d.factory = Arc::new(ByPipeline(BTreeMap::from([
            ("p".to_owned(), fp.clone()),
            ("q".to_owned(), fq.clone()),
        ])));
        d.starts = Arc::new(tokio::sync::Semaphore::new(starts));
        let pipelines = config(&["p", "q"])
            .pipelines
            .into_values()
            .map(|p| (p, pipeline_docs()))
            .collect();
        (ClusterReconciler::start(pipelines, &d), fp, fq, d)
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
        assert!(
            text.lines()
                .any(|l| l == "data_avalanche_owned_pipelines 2"),
            "{text}"
        );
        assert!(
            text.lines()
                .any(|l| l == "data_avalanche_cluster_members 1"),
            "{text}"
        );
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

    /// `max_concurrent_takeovers` 1: while `p`'s Phase 0 retries its preflight, `q` builds no
    /// worker; it starts once `p` streams.
    #[tokio::test(start_paused = true)]
    async fn one_phase_0_at_a_time_holds_the_other_start() {
        let store = ScriptedStore::new();
        let (fp, fq) = (TestFactory::new(&["a"]), TestFactory::new(&["a"]));
        for _ in 0..3_000 {
            fp.source
                .fail(Method::Preflight, SourceError::Transient("slow".into()));
        }
        let mut d = deps("n1", &store, &fp);
        d.factory = Arc::new(ByPipeline(BTreeMap::from([
            ("p".to_owned(), fp.clone()),
            ("q".to_owned(), fq.clone()),
        ])));
        d.starts = Arc::new(tokio::sync::Semaphore::new(1));
        let mut cfg = config(&["p", "q"]).pipelines;
        // `p` first: the controllers start in pipeline-id order, and `p`'s takes the permit.
        let pipelines = vec![
            (cfg.remove("p").unwrap(), pipeline_docs()),
            (cfg.remove("q").unwrap(), pipeline_docs()),
        ];
        let r = ClusterReconciler::start(pipelines, &d);
        let mut st = r.statuses();
        eventually("p is in Phase 0", || fp.builds() == 1).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            st["p"].borrow().actual,
            Some(Actual::Starting),
            "p still retries its preflight"
        );
        assert_eq!(fq.builds(), 0, "q waits for the permit");
        until(st.get_mut("p").unwrap(), "p running", running).await;
        until(st.get_mut("q").unwrap(), "q running", running).await;
        assert_eq!(fq.builds(), 1);
        r.shutdown().await;
    }
}
