//! The config coordinator (Clustering §9): the config generation this node runs. It adopts each
//! newer generation published to `globals.state`, which a member's gossiped generation announces
//! or its poll finds, and a reload publishes the node's file as the next generation. Each node
//! resolves a generation's text in its own environment and applies the difference to its own
//! controllers (config §12); nothing is proxied.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::Duration;

use chrono::Utc;
use data_avalanche_config::{
    ConfigDiff, Env, FingerprintKey, Fingerprints, Globals, Pipeline, PipelineChange, Report,
    ResolvedConfig, diff, diff_with, load_posted_str, load_str,
};
use data_avalanche_core::TableRef;
use data_avalanche_state::{
    ConfigGeneration, ConfigGenerations, GENERATION_KEY, GenerationDoc, PipelineDocs, StateError,
    Versioned, config_hash,
};
use data_avalanche_telemetry::Metrics;
use thiserror::Error;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::{Advertiser, Members, PipelineReload, Reload, Reloader, WorkerFactory};

/// A reload re-reads the generation and tries again this many times when another node publishes
/// first.
pub const PUBLISH_ATTEMPTS: u32 = 5;

/// A pipeline's state documents, at its state location (config §10).
pub type DocsFn = dyn Fn(&Pipeline) -> Result<PipelineDocs, String> + Send + Sync;

/// What the coordinator works with.
#[derive(Clone)]
pub struct CoordinatorDeps {
    pub generations: ConfigGenerations,
    /// This node's environment: each generation's text is resolved in it.
    pub env: Arc<dyn Env + Send + Sync>,
    /// The anchor's: the running config was loaded with it, and both sides of a diff must be.
    pub key: FingerprintKey,
    /// The node that publishes (`written_by`).
    pub node_id: String,
    pub docs: Arc<DocsFn>,
    /// Builds a pipeline's source for the table-set check (config §1.2).
    pub factory: Arc<dyn WorkerFactory>,
    pub reloader: Reloader,
    pub advertiser: Advertiser,
    pub metrics: Metrics,
    /// What a generation's globals change outside the cluster: the binary's log filter and the
    /// API's auth.
    pub globals: Arc<dyn Fn(&Globals) + Send + Sync>,
}

impl Debug for CoordinatorDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoordinatorDeps")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

/// A reload's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reloaded {
    /// The generation the cluster runs now.
    pub generation: u64,
    /// False when the file's text is that generation's already: nothing was published.
    pub published: bool,
}

/// What a reload would do here ([`ConfigCoordinator::check`]).
#[derive(Debug, Clone)]
pub struct Checked {
    /// The generation the changes are against: the one this node runs.
    pub generation: u64,
    /// The text's warnings.
    pub warnings: Report,
    pub changes: ConfigDiff,
}

/// Why a reload published nothing.
#[derive(Debug, Error)]
pub enum ReloadError {
    /// The file doesn't load.
    #[error("the config is invalid")]
    Invalid(Report),
    /// A field only a process restart applies changed (V-061, config §1.2).
    #[error("the config changes a field only a process restart applies")]
    Rejected(Report),
    /// Other nodes published first, every time.
    #[error("other nodes published {PUBLISH_ATTEMPTS} times while this reload tried")]
    Contended,
    /// `globals.state` couldn't be read or written.
    #[error("the config generation: {0}")]
    State(#[from] StateError),
    /// The node is shutting down.
    #[error("the node is shutting down")]
    Stopped,
}

/// The generation this node runs.
struct Running {
    config: ResolvedConfig,
    generation: Versioned<GenerationDoc>,
    /// A newer generation this node couldn't apply: logged once, tried again at each poll.
    failed: Option<u64>,
    /// [`ConfigCoordinator::stop`] ran: nothing is adopted or published any more.
    stopped: bool,
}

struct Inner {
    deps: CoordinatorDeps,
    /// One adoption or reload at a time.
    running: Mutex<Running>,
    applied: watch::Sender<u64>,
}

/// The node's config generation at run time. Dropped, it stops watching.
#[derive(Debug)]
pub struct ConfigCoordinator {
    inner: Arc<Inner>,
    stop: watch::Sender<bool>,
    task: StdMutex<Option<JoinHandle<()>>>,
}

impl Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("applied", &*self.applied.borrow())
            .finish_non_exhaustive()
    }
}

impl Drop for ConfigCoordinator {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().unwrap_or_else(PoisonError::into_inner) {
            task.abort();
        }
    }
}

impl ConfigCoordinator {
    /// Runs `config`, `generation`'s text as loaded with the anchor's key, and watches for newer
    /// generations: every `poll`, and whenever a member gossips one newer than this node's.
    pub fn start(
        config: ResolvedConfig,
        generation: Versioned<GenerationDoc>,
        deps: CoordinatorDeps,
        members: watch::Receiver<Members>,
        poll: Duration,
    ) -> Self {
        let n = generation.doc.generation;
        deps.metrics.set_config_generation(n);
        let inner = Arc::new(Inner {
            deps,
            running: Mutex::new(Running {
                config,
                generation,
                failed: None,
                stopped: false,
            }),
            applied: watch::channel(n).0,
        });
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(watch_generations(
            Arc::clone(&inner),
            members,
            poll,
            stopped,
        ));
        Self {
            inner,
            stop,
            task: StdMutex::new(Some(task)),
        }
    }

    /// The generation this node runs.
    pub fn applied(&self) -> u64 {
        *self.inner.applied.borrow()
    }

    /// The config this node runs: its generation's text as resolved here.
    pub async fn config(&self) -> ResolvedConfig {
        self.inner.running.lock().await.config.clone()
    }

    /// The generation `globals.state` names now, `None` before the first bootstrap.
    ///
    /// # Errors
    /// The pointer or its snapshot can't be read.
    pub async fn latest(&self) -> Result<Option<u64>, StateError> {
        Ok(self
            .inner
            .deps
            .generations
            .current()
            .await?
            .map(|g| g.pointer.doc.generation))
    }

    /// A reload (Clustering §9.2, config §12.1): `text`, the node's file read again, is checked
    /// against the generation the cluster runs (adopted first when newer), published as the
    /// next one, and applied here; the others adopt it from `globals.state`. The text of the
    /// generation itself is published again when what it resolves to here changed (a `${VAR}`,
    /// a credentials file): every node resolves it anew.
    ///
    /// # Errors
    /// [`ReloadError`]: nothing was published.
    pub async fn reload(&self, text: &str) -> Result<Reloaded, ReloadError> {
        let d = &self.inner.deps;
        let mut running = self.inner.running.lock().await;
        if running.stopped {
            return Err(ReloadError::Stopped);
        }
        let new = load_str(text, &*d.env, &d.key)
            .map_err(ReloadError::Invalid)?
            .config;
        for _ in 0..PUBLISH_ATTEMPTS {
            let current = d
                .generations
                .current()
                .await?
                .ok_or_else(|| StateError::Corrupt {
                    key: GENERATION_KEY.to_owned(),
                    kind: "config generation",
                    reason: "missing, though this node started from one".to_owned(),
                })?;
            let n = current.pointer.doc.generation;
            if n > running.generation.doc.generation {
                self.inner.adopt(&mut running, current.clone()).await;
            }
            let changes = diff(&running.config, &new, &|_, _| None);
            if changes.is_rejected() {
                return Err(ReloadError::Rejected(changes.report));
            }
            if config_hash(text) == current.pointer.doc.hash && changes.is_empty() {
                return Ok(Reloaded {
                    generation: n,
                    published: false,
                });
            }
            match d
                .generations
                .publish(&current.pointer, text, &d.node_id, Utc::now())
                .await
            {
                Ok(published) => {
                    let generation = published.pointer.doc.generation;
                    tracing::info!(generation, "published config generation {generation}");
                    self.inner.adopt(&mut running, published).await;
                    return Ok(Reloaded {
                        generation,
                        published: true,
                    });
                }
                // Another node published first, or the outcome is unknown: read again.
                Err(e) if e.is_race() => {}
                Err(e) => return Err(e.into()),
            }
        }
        Err(ReloadError::Contended)
    }

    /// What a reload of `text` would change on this node, nothing published (`POST
    /// /v1/config/validate`): the text loaded in this node's environment and diffed against
    /// the generation this node runs, which may lag the cluster's ([`Checked::generation`]
    /// says which it was). A table-pattern change counts as a restart here; the reload's
    /// adoption asks the source whether the table set changed (config §1.2). An adoption or a
    /// reload isn't held up by it: the running config is copied, and the lock let go.
    /// `fingerprints` says whether a secret or a file that differs only by its value counts as
    /// a change ([`Fingerprints`]). This is for the node's own file; a text a caller supplies
    /// goes through [`Self::check_posted`].
    ///
    /// # Errors
    /// `Invalid`, `Rejected` or `Stopped`, as a reload's.
    pub async fn check(
        &self,
        text: &str,
        fingerprints: Fingerprints,
    ) -> Result<Checked, ReloadError> {
        self.check_with(text, fingerprints, false).await
    }

    /// [`Self::check`] for a text a caller supplies (`POST /v1/config/validate` with a body):
    /// secrets are not compared with the running ones, so that the check doesn't tell whether a
    /// guess is the running secret, and a pipeline's `id` is taken as written
    /// ([`load_posted_str`]), so that a `${VAR}` in it can't read this node's environment
    /// through the answer, which names a pipeline by its id.
    ///
    /// # Errors
    /// `Invalid`, `Rejected` or `Stopped`, as a reload's.
    pub async fn check_posted(&self, text: &str) -> Result<Checked, ReloadError> {
        self.check_with(text, Fingerprints::Ignored, true).await
    }

    async fn check_with(
        &self,
        text: &str,
        fingerprints: Fingerprints,
        posted: bool,
    ) -> Result<Checked, ReloadError> {
        let d = &self.inner.deps;
        let (config, generation) = {
            let running = self.inner.running.lock().await;
            if running.stopped {
                return Err(ReloadError::Stopped);
            }
            (running.config.clone(), running.generation.doc.generation)
        };
        let loaded = if posted {
            load_posted_str(text, &*d.env, &d.key)
        } else {
            load_str(text, &*d.env, &d.key)
        }
        .map_err(ReloadError::Invalid)?;
        let changes = diff_with(&config, &loaded.config, &|_, _| None, fingerprints);
        if changes.is_rejected() {
            return Err(ReloadError::Rejected(changes.report));
        }
        let mut warnings = Report::default();
        for w in loaded.warnings {
            warnings.push(w);
        }
        Ok(Checked {
            generation,
            warnings,
            changes,
        })
    }

    /// Stops watching, after the adoption or reload under way: none follows, and a reload is
    /// [`ReloadError::Stopped`].
    pub async fn stop(&self) {
        self.inner.running.lock().await.stopped = true;
        self.stop.send_replace(true);
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl Inner {
    /// Applies generation `g` (config §12): a generation that doesn't load here, or that changes
    /// a restart-only field as this node resolves it, isn't, and this node runs on what it
    /// runs.
    async fn adopt(&self, running: &mut Running, g: ConfigGeneration) {
        let d = &self.deps;
        let n = g.pointer.doc.generation;
        let refused = |running: &mut Running, why: String| {
            if running.failed != Some(n) {
                tracing::error!(
                    generation = n,
                    "config generation {n} can't be applied on this node, which runs {}: {why}",
                    running.generation.doc.generation
                );
            }
            running.failed = Some(n);
        };
        let new = match load_str(&g.text, &*d.env, &d.key) {
            Ok(loaded) => loaded.config,
            Err(report) => return refused(running, lines(&report)),
        };
        let sets = table_sets(&*d.factory, &running.config, &new).await;
        let changes = diff(&running.config, &new, &|_, p| {
            sets.get(&p.id).copied().flatten()
        });
        if changes.is_rejected() {
            return refused(running, lines(&changes.report));
        }
        let pipelines = match self.pipelines(&new, &changes) {
            Ok(pipelines) => pipelines,
            Err(why) => return refused(running, why),
        };
        d.reloader.apply(Reload {
            pipelines,
            cluster: new.globals.cluster.clone(),
        });
        (d.globals)(&new.globals);
        d.advertiser.set_config_generation(n);
        d.metrics.set_config_generation(n);
        tracing::info!(
            generation = n,
            "applied config generation {n}: {}",
            summary(&changes)
        );
        running.config = new;
        running.generation = g.pointer;
        running.failed = None;
        self.applied.send_replace(n);
    }

    /// The reconciler's share of a diff: each pipeline added (with its state documents),
    /// removed, changed in place, or restarted (with its documents, which may have moved).
    fn pipelines(
        &self,
        new: &ResolvedConfig,
        changes: &ConfigDiff,
    ) -> Result<Vec<PipelineReload>, String> {
        let by_id = |id: &str| {
            new.pipelines
                .values()
                .find(|p| p.id == id)
                .cloned()
                .ok_or_else(|| format!("pipeline id {id} isn't in the generation"))
        };
        let docs = |p: &Pipeline| (self.deps.docs)(p);
        changes
            .pipelines
            .iter()
            .map(|change| {
                Ok(match change {
                    PipelineChange::Added { id, .. } => {
                        let p = by_id(id)?;
                        let d = docs(&p)?;
                        PipelineReload::Add(p, d)
                    }
                    PipelineChange::Removed { id, .. } => PipelineReload::Remove(id.clone()),
                    PipelineChange::Modified {
                        id, restart: true, ..
                    } => {
                        let p = by_id(id)?;
                        let d = docs(&p)?;
                        PipelineReload::Restart(p, d)
                    }
                    PipelineChange::Modified { id, .. } => PipelineReload::Reconfigure(by_id(id)?),
                })
            })
            .collect()
    }
}

/// A report's errors, a line each, for the log.
fn lines(report: &Report) -> String {
    report
        .errors
        .iter()
        .map(|d| format!("{} at {}: {}", d.rule, d.path, d.message))
        .collect::<Vec<_>>()
        .join("; ")
}

/// What a generation changed, in a few words.
fn summary(changes: &ConfigDiff) -> String {
    let mut added = 0;
    let mut removed = 0;
    let mut changed = 0;
    let mut restarted = 0;
    for c in &changes.pipelines {
        match c {
            PipelineChange::Added { .. } => added += 1,
            PipelineChange::Removed { .. } => removed += 1,
            PipelineChange::Modified { restart, .. } => {
                changed += 1;
                restarted += usize::from(*restart);
            }
        }
    }
    format!(
        "{added} pipelines added, {removed} removed, {changed} changed ({restarted} restarted), {} global fields",
        changes.globals.len()
    )
}

/// For each pipeline whose `tables.include` or `tables.exclude` changed: whether its resolved
/// table set changed, as its source answers now (config §1.2). `None` when either side can't be
/// resolved: a restart, which is always safe.
async fn table_sets(
    factory: &dyn WorkerFactory,
    old: &ResolvedConfig,
    new: &ResolvedConfig,
) -> BTreeMap<String, Option<bool>> {
    let mut out = BTreeMap::new();
    for p in new.pipelines.values() {
        let Some(o) = old.pipelines.values().find(|o| o.id == p.id) else {
            continue;
        };
        if o.tables.include == p.tables.include && o.tables.exclude == p.tables.exclude {
            continue;
        }
        let changed = match (resolved(factory, o).await, resolved(factory, p).await) {
            (Some(before), Some(after)) => Some(before != after),
            _ => None,
        };
        out.insert(p.id.clone(), changed);
    }
    out
}

/// The tables `p` names, as its source resolves them now: replicated or halted.
async fn resolved(factory: &dyn WorkerFactory, p: &Pipeline) -> Option<BTreeSet<TableRef>> {
    let source = factory.build(p).ok()?.source;
    let t = source.resolve_tables().await.ok()?;
    Some(
        t.tables
            .iter()
            .map(|s| s.table().clone())
            .chain(t.halted.iter().map(|(table, _)| table.clone()))
            .collect(),
    )
}

/// Adopts each newer generation: at every `poll`, and when a member gossips one newer than this
/// node's.
async fn watch_generations(
    inner: Arc<Inner>,
    mut members: watch::Receiver<Members>,
    poll: Duration,
    mut stop: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(poll);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await;
    let mut gossiping = true;
    let mut unread = false;
    loop {
        tokio::select! {
            _ = stop.wait_for(|s| *s) => return,
            _ = tick.tick() => {}
            changed = members.changed(), if gossiping => {
                if changed.is_err() {
                    gossiping = false;
                    continue;
                }
                if members.borrow_and_update().newest_generation() <= *inner.applied.borrow() {
                    continue;
                }
            }
        }
        match inner.deps.generations.current().await {
            Ok(Some(g)) => {
                unread = false;
                let mut running = inner.running.lock().await;
                if !running.stopped && g.pointer.doc.generation > running.generation.doc.generation
                {
                    inner.adopt(&mut running, g).await;
                }
            }
            Ok(None) => unread = false,
            Err(e) => {
                if !unread {
                    tracing::warn!("the config generation can't be read: {e}; read again later");
                }
                unread = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bytes::Bytes;
    use data_avalanche_config::LogFilter;
    use data_avalanche_state::DocStore;
    use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
    use object_store::memory::InMemory;
    use tokio::sync::mpsc;

    use super::*;
    use crate::test_support::{
        Hub, at, config_text, eventually, gossip_settings, members, pipeline_docs, schema,
    };
    use crate::{Gossip, GossipKey, NodeIdentity, NodeMeta, Parts, Phase};

    /// Each build's source holds the tables its `tables.include` names: `public.*` is `a` and
    /// `b`, `public.<t>` is `t`. Its builds are counted.
    #[derive(Debug, Default)]
    struct ByInclude(std::sync::atomic::AtomicUsize);

    impl WorkerFactory for ByInclude {
        fn build(&self, p: &Pipeline) -> Result<Parts, String> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let names: Vec<String> = p
                .tables
                .include
                .iter()
                .flat_map(|i| match i.strip_prefix("public.") {
                    Some("*") => vec!["a".to_owned(), "b".to_owned()],
                    Some(t) => vec![t.to_owned()],
                    None => vec![],
                })
                .collect();
            Ok(Parts {
                source: Arc::new(data_avalanche_testkit::FakeSource::new(
                    names.iter().map(|t| schema(t)).collect(),
                )),
                sink: Arc::new(data_avalanche_testkit::MemorySink::new()),
                hooks: Arc::new(data_avalanche_pipeline::NoHooks),
            })
        }
    }

    /// An environment a test changes.
    #[derive(Debug, Default)]
    struct Vars(StdMutex<BTreeMap<String, String>>);

    impl Env for Vars {
        fn var(&self, name: &str) -> Option<String> {
            self.0.lock().unwrap().get(name).cloned()
        }
    }

    impl Vars {
        fn set(&self, name: &str, value: &str) {
            self.0.lock().unwrap().insert(name.into(), value.into());
        }
    }

    /// `globals.state` where another node publishes `next` just before this node's next pointer
    /// update, once armed: that update loses the race.
    #[derive(Debug)]
    struct Racing {
        inner: Arc<dyn VersionedStore>,
        next: StdMutex<Option<String>>,
    }

    impl Racing {
        fn new() -> Arc<Self> {
            let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
            Arc::new(Self {
                inner: Arc::new(store),
                next: StdMutex::new(None),
            })
        }

        fn arm(&self, text: &str) {
            *self.next.lock().unwrap() = Some(text.to_owned());
        }
    }

    #[async_trait]
    impl VersionedStore for Racing {
        async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
            self.inner.get(key).await
        }
        async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
            self.inner.create(key, body).await
        }
        async fn update(
            &self,
            key: &str,
            expected: &ObjectVersion,
            body: Bytes,
        ) -> Result<ObjectVersion, StoreError> {
            let next = if key == GENERATION_KEY {
                self.next.lock().unwrap().take()
            } else {
                None
            };
            if let Some(text) = next {
                let other = ConfigGenerations::new(DocStore::new(Arc::clone(&self.inner)));
                let current = other.current().await.unwrap().unwrap();
                other
                    .publish(&current.pointer, &text, "n2", Utc::now())
                    .await
                    .unwrap();
            }
            self.inner.update(key, expected, body).await
        }
        async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
            self.inner.put(key, body).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            self.inner.list(prefix).await
        }
    }

    /// A lone node `n1` running generation 1 of its text: its reloads go to `reloads`, its
    /// generation to its own gossip, its globals to `globals`; `members` stands in for the
    /// gossip's view, so a test can make a member gossip a generation.
    struct Lone {
        generations: ConfigGenerations,
        vars: Arc<Vars>,
        coordinator: ConfigCoordinator,
        reloads: mpsc::UnboundedReceiver<Reload>,
        gossip: Gossip,
        metrics: Metrics,
        members: watch::Sender<Members>,
        globals: Arc<StdMutex<Vec<Globals>>>,
    }

    const POLL: Duration = Duration::from_secs(2);

    /// [`start`] on `config_text(ids)`, an empty environment and `globals.state` in memory.
    async fn lone(ids: &[&str]) -> Lone {
        start(&config_text(ids), Vars::default(), Racing::new()).await
    }

    async fn start(text: &str, vars: Vars, store: Arc<dyn VersionedStore>) -> Lone {
        let generations = ConfigGenerations::new(DocStore::new(store));
        let (first, created) = generations
            .bootstrap_or_adopt(text, "n1", Utc::now())
            .await
            .unwrap();
        assert!(created);
        let key = FingerprintKey::from_bytes([1; 32]);
        let vars = Arc::new(vars);
        let config = load_str(text, &*vars, &key).unwrap().config;
        let hub = Hub::new();
        let (port, inbox) = hub.port(at(1));
        let me = NodeIdentity {
            node_id: "n1".into(),
            incarnation: 1,
            rev: 0,
            addr: at(1),
            meta: NodeMeta {
                phase: Phase::Active,
                config_generation: 1,
                build: "1.0.0".into(),
            },
        };
        let gossip = Gossip::start(
            me,
            GossipKey::derive(&[7; 32]),
            gossip_settings(),
            Arc::new(port),
            inbox,
        );
        let (reloader, reloads) = mpsc::unbounded_channel();
        let metrics = Metrics::new();
        let globals = Arc::new(StdMutex::new(Vec::new()));
        let seen = Arc::clone(&globals);
        let deps = CoordinatorDeps {
            generations: generations.clone(),
            env: Arc::clone(&vars) as Arc<dyn Env + Send + Sync>,
            key,
            node_id: "n1".into(),
            docs: Arc::new(|_| Ok(pipeline_docs())),
            factory: Arc::new(ByInclude::default()),
            reloader: Reloader(reloader),
            advertiser: gossip.advertiser(),
            metrics: metrics.clone(),
            globals: Arc::new(move |g| seen.lock().unwrap().push(g.clone())),
        };
        let (members_tx, members_rx) =
            watch::channel(members("n1", &[("n1", Phase::Active, false)]));
        let coordinator = ConfigCoordinator::start(config, first.pointer, deps, members_rx, POLL);
        Lone {
            generations,
            vars,
            coordinator,
            reloads,
            gossip,
            metrics,
            members: members_tx,
            globals,
        }
    }

    impl Lone {
        /// Another node publishes `text` on top of the generation `globals.state` names.
        async fn publish_elsewhere(&self, text: &str) -> u64 {
            let current = self.generations.current().await.unwrap().unwrap();
            self.generations
                .publish(&current.pointer, text, "n2", Utc::now())
                .await
                .unwrap()
                .pointer
                .doc
                .generation
        }

        /// The next reload the reconciler got.
        async fn reload(&mut self) -> Reload {
            tokio::time::timeout(Duration::from_secs(60), self.reloads.recv())
                .await
                .expect("a reload")
                .expect("the coordinator runs")
        }

        /// The generation the node gossips.
        fn gossiped(&self) -> u64 {
            self.gossip.members().borrow().nodes["n1"]
                .meta
                .config_generation
        }
    }

    fn kinds(r: &Reload) -> Vec<String> {
        r.pipelines
            .iter()
            .map(|p| match p {
                PipelineReload::Add(p, _) => format!("add {}", p.id),
                PipelineReload::Remove(id) => format!("remove {id}"),
                PipelineReload::Reconfigure(p) => format!("reconfigure {}", p.id),
                PipelineReload::Restart(p, _) => format!("restart {}", p.id),
            })
            .collect()
    }

    /// A generation another node published is adopted at the next poll: its pipelines' changes
    /// go to the reconciler, the node gossips and shows the new generation.
    #[tokio::test(start_paused = true)]
    async fn a_generation_published_elsewhere_is_adopted_at_the_next_poll() {
        let mut n = lone(&["p", "q"]).await;
        let text = config_text(&["p", "r"]).replace(
            "prefix: p, delta: {}, flush: { seconds: 0 }",
            "prefix: p, delta: {}, flush: { seconds: 0, rows: 5 }",
        );
        assert_eq!(n.publish_elsewhere(&text).await, 2);
        let r = n.reload().await;
        let mut got = kinds(&r);
        got.sort();
        assert_eq!(got, ["add r", "reconfigure p", "remove q"]);
        assert_eq!(n.coordinator.applied(), 2);
        eventually("n1 gossips 2", || n.gossiped() == 2).await;
        assert!(
            n.metrics
                .encode()
                .lines()
                .any(|l| l == "data_avalanche_config_generation 2")
        );
    }

    /// A member that gossips a newer generation brings it in at once, not at the next poll.
    #[tokio::test(start_paused = true)]
    async fn a_gossiped_generation_is_adopted_without_waiting_for_the_poll() {
        let mut n = lone(&["p"]).await;
        // Just after a poll: the next one is almost two seconds away.
        tokio::time::sleep(POLL + Duration::from_millis(10)).await;
        n.publish_elsewhere(&config_text(&["p", "q"])).await;
        let mut view = members(
            "n1",
            &[("n1", Phase::Active, false), ("n2", Phase::Active, false)],
        );
        view.nodes.get_mut("n2").unwrap().meta.config_generation = 2;
        let t = tokio::time::Instant::now();
        n.members.send_replace(view);
        let r = n.reload().await;
        assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
        assert_eq!(kinds(&r), ["add q"]);
    }

    /// A check says what a reload would change, and publishes nothing.
    #[tokio::test(start_paused = true)]
    async fn a_check_says_what_a_reload_would_change_and_publishes_nothing() {
        let n = lone(&["p"]).await;
        let checked = n
            .coordinator
            .check(&config_text(&["p", "q"]), Fingerprints::Compared)
            .await
            .unwrap();
        assert_eq!(checked.generation, 1);
        assert!(checked.warnings.is_ok());
        assert!(
            matches!(&checked.changes.pipelines[..], [PipelineChange::Added { id, .. }] if id == "q"),
            "{:?}",
            checked.changes
        );
        let same = n
            .coordinator
            .check(&config_text(&["p"]), Fingerprints::Compared)
            .await
            .unwrap();
        assert!(same.changes.is_empty());
        // A text a caller supplies: whether its secret is the running one isn't told.
        let guess = config_text(&["p"]).replace("password: avalanche", "password: a-guess");
        let check = |f| n.coordinator.check(&guess, f);
        assert!(
            check(Fingerprints::Ignored)
                .await
                .unwrap()
                .changes
                .is_empty()
        );
        assert!(
            !check(Fingerprints::Compared)
                .await
                .unwrap()
                .changes
                .is_empty()
        );
        let restart_only = config_text(&["p"]).replace(
            "  state: { bucket: app-state }\n",
            "  state: { bucket: app-state }\n  api: { address: 0.0.0.0:9090 }\n",
        );
        match n
            .coordinator
            .check(&restart_only, Fingerprints::Compared)
            .await
        {
            Err(ReloadError::Rejected(r)) => assert!(r.has("V-061"), "{r:?}"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            n.coordinator
                .check("pipelines: [", Fingerprints::Compared)
                .await,
            Err(ReloadError::Invalid(_))
        ));
        let pointer = n.generations.current().await.unwrap().unwrap().pointer;
        assert_eq!(pointer.doc.generation, 1, "nothing published");
        n.coordinator.stop().await;
        assert!(matches!(
            n.coordinator
                .check(&config_text(&["p"]), Fingerprints::Compared)
                .await,
            Err(ReloadError::Stopped)
        ));
    }

    /// A reload publishes the file as the next generation and applies it here: the globals
    /// hook sees it, the reconciler gets the changes.
    #[tokio::test(start_paused = true)]
    async fn a_reload_publishes_and_applies_the_next_generation() {
        let mut n = lone(&["p"]).await;
        let text = config_text(&["p"]).replace(
            "  state: { bucket: app-state }\n",
            "  state: { bucket: app-state }\n  log: { level: debug }\n",
        );
        let out = n.coordinator.reload(&text).await.unwrap();
        assert_eq!(
            out,
            Reloaded {
                generation: 2,
                published: true
            }
        );
        let pointer = n.generations.current().await.unwrap().unwrap().pointer;
        assert_eq!(
            (pointer.doc.generation, pointer.doc.written_by.as_str()),
            (2, "n1")
        );
        let r = n.reload().await;
        assert!(r.pipelines.is_empty(), "{:?}", kinds(&r));
        assert_eq!(
            n.globals
                .lock()
                .unwrap()
                .last()
                .map(|g| g.log.level.clone()),
            Some(LogFilter("debug".into()))
        );
        assert_eq!(n.coordinator.applied(), 2);
    }

    /// The file as the cluster runs it already: nothing is published.
    #[tokio::test(start_paused = true)]
    async fn an_unchanged_file_publishes_nothing() {
        let n = lone(&["p"]).await;
        let out = n.coordinator.reload(&config_text(&["p"])).await.unwrap();
        assert_eq!(
            out,
            Reloaded {
                generation: 1,
                published: false
            }
        );
        let current = n.generations.current().await.unwrap().unwrap();
        assert_eq!(current.pointer.doc.generation, 1);
    }

    /// A restart-only change (V-061) and a file that doesn't load are refused; nothing is
    /// published.
    #[tokio::test(start_paused = true)]
    async fn a_refused_reload_publishes_nothing() {
        let n = lone(&["p"]).await;
        let tokens = config_text(&["p"]).replace(
            "cluster: { memberlist: { node_name: n1 } }",
            "cluster: { tokens: 64, memberlist: { node_name: n1 } }",
        );
        match n.coordinator.reload(&tokens).await {
            Err(ReloadError::Rejected(report)) => {
                assert!(
                    report
                        .errors
                        .iter()
                        .any(|d| d.rule == "V-061" && d.path == "globals.cluster.tokens"),
                    "{report:?}"
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            n.coordinator.reload("pipelines: [").await,
            Err(ReloadError::Invalid(_))
        ));
        let current = n.generations.current().await.unwrap().unwrap();
        assert_eq!(current.pointer.doc.generation, 1);
    }

    /// A reload while another node's newer generation isn't adopted yet: it is adopted first,
    /// the file is checked against it, and published on top.
    #[tokio::test(start_paused = true)]
    async fn a_reload_publishes_on_top_of_a_newer_generation() {
        let mut n = lone(&["p"]).await;
        n.publish_elsewhere(&config_text(&["p", "q"])).await;
        let out = n
            .coordinator
            .reload(&config_text(&["p", "q", "r"]))
            .await
            .unwrap();
        assert_eq!(out.generation, 3);
        assert_eq!(kinds(&n.reload().await), ["add q"]);
        assert_eq!(kinds(&n.reload().await), ["add r"]);
        assert_eq!(n.coordinator.applied(), 3);
    }

    /// A generation this node can't resolve (a `${VAR}` its environment lacks) isn't applied:
    /// it runs on its own until a generation it can apply.
    #[tokio::test(start_paused = true)]
    async fn a_generation_that_doesnt_load_here_waits_for_the_next() {
        let mut n = lone(&["p"]).await;
        let unset = config_text(&["p", "q"]).replace(
            "  q:\n    id: q\n    source:\n      host: 127.0.0.1",
            "  q:\n    id: q\n    source:\n      host: ${Q_HOST}",
        );
        assert_eq!(n.publish_elsewhere(&unset).await, 2);
        tokio::time::sleep(POLL * 3).await;
        assert_eq!(n.coordinator.applied(), 1);
        assert!(n.reloads.try_recv().is_err(), "nothing applied");
        assert_eq!(n.gossiped(), 1);
        assert_eq!(n.publish_elsewhere(&config_text(&["p", "q"])).await, 3);
        assert_eq!(kinds(&n.reload().await), ["add q"]);
        assert_eq!(n.coordinator.applied(), 3);
    }

    /// `tables.include` changed (config §1.2): in place when the source resolves the same
    /// tables, a restart when it resolves others.
    #[tokio::test(start_paused = true)]
    async fn a_tables_change_restarts_only_when_the_set_changes() {
        let mut n = lone(&["p"]).await;
        let same = config_text(&["p"]).replace("[public.*]", "[public.a, public.b]");
        n.coordinator.reload(&same).await.unwrap();
        assert_eq!(kinds(&n.reload().await), ["reconfigure p"]);
        let fewer = config_text(&["p"]).replace("[public.*]", "[public.a]");
        n.coordinator.reload(&fewer).await.unwrap();
        assert_eq!(kinds(&n.reload().await), ["restart p"]);
    }

    /// Once stopped, for the leave: no generation is adopted, a reload is refused.
    #[tokio::test(start_paused = true)]
    async fn a_stopped_coordinator_adopts_and_publishes_nothing() {
        let mut n = lone(&["p"]).await;
        n.coordinator.stop().await;
        n.publish_elsewhere(&config_text(&["p", "q"])).await;
        tokio::time::sleep(POLL * 3).await;
        assert_eq!(n.coordinator.applied(), 1);
        assert!(n.reloads.try_recv().is_err(), "nothing applied");
        for text in [config_text(&["p", "r"]), "pipelines: [".to_owned()] {
            assert!(matches!(
                n.coordinator.reload(&text).await,
                Err(ReloadError::Stopped)
            ));
        }
        assert_eq!(n.coordinator.config().await.pipelines.len(), 1);
    }

    /// Two reloads at once on two nodes: this one's pointer update loses the race, so it reads
    /// the winner's generation, adopts it, checks the file against it and publishes on top.
    #[tokio::test(start_paused = true)]
    async fn a_reload_that_loses_the_race_publishes_on_top_of_the_winner() {
        let racing = Racing::new();
        let mut n = start(
            &config_text(&["p"]),
            Vars::default(),
            Arc::clone(&racing) as Arc<dyn VersionedStore>,
        )
        .await;
        racing.arm(&config_text(&["p", "q"]));
        let out = n
            .coordinator
            .reload(&config_text(&["p", "q", "r"]))
            .await
            .unwrap();
        assert_eq!(
            out,
            Reloaded {
                generation: 3,
                published: true
            }
        );
        assert_eq!(kinds(&n.reload().await), ["add q"]);
        assert_eq!(kinds(&n.reload().await), ["add r"]);
        let pointer = n.generations.current().await.unwrap().unwrap().pointer;
        assert_eq!(
            (pointer.doc.generation, pointer.doc.written_by.as_str()),
            (3, "n1")
        );
    }

    /// The generation's own text, read again after what it resolves to changed here (config
    /// §12.1: a `${VAR}`, a credentials file): published again, so every node resolves it anew.
    #[tokio::test(start_paused = true)]
    async fn the_same_text_is_published_again_when_what_it_resolves_to_changed() {
        let text = config_text(&["p"]).replace("password: avalanche", "password: ${P_PASSWORD}");
        let vars = Vars::default();
        vars.set("P_PASSWORD", "one");
        let mut n = start(&text, vars, Racing::new()).await;
        assert_eq!(
            n.coordinator.reload(&text).await.unwrap(),
            Reloaded {
                generation: 1,
                published: false
            }
        );
        n.vars.set("P_PASSWORD", "two");
        assert_eq!(
            n.coordinator.reload(&text).await.unwrap(),
            Reloaded {
                generation: 2,
                published: true
            }
        );
        assert_eq!(kinds(&n.reload().await), ["restart p"]);
    }
}
