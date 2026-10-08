//! A store whose writes fail on cue, and the lease fixtures the tests share.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use data_avalanche_state::{Acquired, DocStore, Holder, LeaseDoc, Leases, Versioned};
use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
use data_avalanche_telemetry::{Metrics, PipelineLabels, PipelineMetrics};
use object_store::memory::InMemory;

use crate::LeaseTiming;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Transient,
    Contention,
    Permanent,
    /// The write never answers.
    Hang,
    /// The write lands after this long, and answers.
    Slow(Duration),
}

/// A failed write. `lands`: the write goes through, then the error comes back (an unknown
/// outcome, as S3 409 or a timeout after the PUT).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Fault {
    pub kind: Kind,
    pub lands: bool,
}

impl Fault {
    fn error(self, key: &str) -> StoreError {
        let key = key.to_owned();
        match self.kind {
            Kind::Transient => StoreError::Transient {
                key,
                reason: "injected".into(),
            },
            Kind::Contention => StoreError::Contention { key },
            Kind::Permanent | Kind::Hang | Kind::Slow(_) => StoreError::Permanent {
                key,
                reason: "injected".into(),
            },
        }
    }
}

#[derive(Debug, Default)]
struct Script {
    /// The next writes' faults, in order.
    next: VecDeque<Fault>,
    /// Every write fails so, once `next` is empty.
    always: Option<Fault>,
    /// How long every read takes.
    read_delay: Duration,
    /// The next reads that fail transiently.
    failed_reads: usize,
}

/// An in-memory store whose writes (`create`, `update`, `put`) fail as scripted.
#[derive(Debug)]
pub(crate) struct ScriptedStore {
    inner: ObjectStoreVersioned,
    script: Mutex<Script>,
}

impl ScriptedStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals")
                .expect("a valid root"),
            script: Mutex::new(Script::default()),
        })
    }

    pub fn fail_next(&self, fault: Fault) {
        self.lock().next.push_back(fault);
    }

    pub fn fail_always(&self, fault: Option<Fault>) {
        self.lock().always = fault;
    }

    /// Every read from now on answers after `delay`.
    pub fn slow_reads(&self, delay: Duration) {
        self.lock().read_delay = delay;
    }

    /// The next `n` reads fail transiently.
    pub fn fail_reads(&self, n: usize) {
        self.lock().failed_reads = n;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Script> {
        self.script
            .lock()
            .expect("no test panics while holding the lock")
    }

    fn fault(&self) -> Option<Fault> {
        let mut s = self.lock();
        s.next.pop_front().or(s.always)
    }

    async fn write<F>(&self, key: &str, write: F) -> Result<ObjectVersion, StoreError>
    where
        F: Future<Output = Result<ObjectVersion, StoreError>>,
    {
        match self.fault() {
            None => write.await,
            Some(Fault {
                kind: Kind::Hang, ..
            }) => std::future::pending().await,
            Some(Fault {
                kind: Kind::Slow(after),
                ..
            }) => {
                tokio::time::sleep(after).await;
                write.await
            }
            Some(f) if f.lands => {
                write.await?;
                Err(f.error(key))
            }
            Some(f) => Err(f.error(key)),
        }
    }
}

#[async_trait]
impl VersionedStore for ScriptedStore {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
        let (delay, fail) = {
            let mut s = self.lock();
            let fail = s.failed_reads > 0;
            s.failed_reads = s.failed_reads.saturating_sub(1);
            (s.read_delay, fail)
        };
        tokio::time::sleep(delay).await;
        if fail {
            return Err(StoreError::Transient {
                key: key.to_owned(),
                reason: "injected".into(),
            });
        }
        self.inner.get(key).await
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.write(key, self.inner.create(key, body)).await
    }

    async fn update(
        &self,
        key: &str,
        expected: &ObjectVersion,
        body: Bytes,
    ) -> Result<ObjectVersion, StoreError> {
        self.write(key, self.inner.update(key, expected, body))
            .await
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.write(key, self.inner.put(key, body)).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix).await
    }
}

/// Balanced defaults (§18): 15 s, renewed every 5 s, deadline at 12.75 s.
pub(crate) fn timing() -> LeaseTiming {
    LeaseTiming {
        duration: Duration::from_secs(15),
        renewal_interval: Duration::from_secs(5),
        safety_margin: 0.15,
    }
}

pub(crate) fn holder(node: &str) -> Holder {
    Holder {
        node_id: node.to_owned(),
        incarnation: 1,
    }
}

pub(crate) fn leases(store: &Arc<ScriptedStore>) -> Leases {
    Leases::new(DocStore::new(Arc::clone(store) as Arc<dyn VersionedStore>))
}

pub(crate) async fn acquired(leases: &Leases, node: &str) -> Versioned<LeaseDoc> {
    match leases
        .acquire("p", &holder(node), timing().duration, Utc::now())
        .await
        .expect("acquired")
    {
        Acquired::Mine(lease) => lease,
        Acquired::HeldBy(other) => panic!("held by {other:?}"),
    }
}

pub(crate) fn metrics() -> (Metrics, Arc<PipelineMetrics>) {
    let m = Metrics::new();
    let p = m.pipeline(
        PipelineLabels {
            pipeline: "p".into(),
            source_host: "h".into(),
            source_db: "d".into(),
        },
        false,
    );
    (m, Arc::new(p))
}

/// The test pipeline `p` over `public.*` of a fake PostgreSQL source, flushing at every boundary.
pub(crate) fn pipeline() -> data_avalanche_config::Pipeline {
    let yaml = r#"
config_version: 1
globals:
  state: { bucket: app-state }
pipelines:
  p:
    id: p
    source:
      host: 127.0.0.1
      port: 55418
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: { publication: p_pub, slot: p_slot }
    tables:
      include: [public.*]
      snapshots: { mode: initial }
    sink: { type: delta, bucket: lake, delta: {}, flush: { seconds: 0 } }
"#;
    data_avalanche_config::load_str(
        yaml,
        &std::collections::BTreeMap::new(),
        &data_avalanche_config::FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
    .config
    .pipelines
    .into_values()
    .next()
    .expect("one pipeline")
}

/// `public.<name>`: `id bigint` (the key) and `doc text`.
pub(crate) fn schema(name: &str) -> data_avalanche_core::TableSchema {
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, TableRef, TableSchema};
    TableSchema::new(
        TableRef::new("public", name),
        vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("doc", V::Text, "text"),
        ],
        vec!["id".into()],
    )
    .expect("a valid schema")
}

pub(crate) fn row(id: i64) -> data_avalanche_core::Row {
    use data_avalanche_core::{ColumnValue, Row};
    Row(vec![
        ("id".into(), ColumnValue::Int64(id)),
        ("doc".into(), ColumnValue::Text("x".into())),
    ])
}

/// Builds parts over one fake source and one sink, and counts its builds.
#[derive(Debug, Clone)]
pub(crate) struct TestFactory {
    pub source: data_avalanche_testkit::FakeSource,
    pub sink: data_avalanche_testkit::MemorySink,
    pub builds: Arc<std::sync::atomic::AtomicUsize>,
}

impl TestFactory {
    pub fn new(tables: &[&str]) -> Self {
        Self {
            source: data_avalanche_testkit::FakeSource::new(
                tables.iter().map(|t| schema(t)).collect(),
            ),
            sink: data_avalanche_testkit::MemorySink::new(),
            builds: Arc::default(),
        }
    }

    pub fn builds(&self) -> usize {
        self.builds.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The sink's `id`s of `public.<table>`, in key order.
    pub fn ids(&self, table: &str) -> Vec<i64> {
        use arrow_array::{Array, Int64Array};
        let Some(b) = self
            .sink
            .content(&data_avalanche_core::TableRef::new("public", table))
        else {
            return vec![];
        };
        let ids = b
            .column_by_name("id")
            .expect("an id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bigint ids");
        assert_eq!(ids.null_count(), 0);
        ids.values().to_vec()
    }
}

impl crate::WorkerFactory for TestFactory {
    fn build(&self, _: &data_avalanche_config::Pipeline) -> Result<crate::Parts, String> {
        self.builds
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(crate::Parts {
            source: Arc::new(self.source.clone()),
            sink: Arc::new(self.sink.clone()),
            hooks: Arc::new(data_avalanche_pipeline::NoHooks),
        })
    }
}

/// Fast worker retries; the lease keeps its Balanced timings (the tests run on paused time).
pub(crate) fn settings() -> crate::ControllerSettings {
    use data_avalanche_pipeline::{Backoff, Tuning};
    let ms = |first, max| Backoff {
        first: Duration::from_millis(first),
        max: Duration::from_millis(max),
    };
    crate::ControllerSettings {
        timing: timing(),
        drain_timeout: Duration::from_secs(5),
        restart: ms(100, 1_000),
        tuning: Tuning {
            source_backoff: ms(1, 5),
            sink_backoff: ms(1, 5),
            in_flight_backoff: ms(1, 5),
            contended_window: Duration::from_millis(200),
            tick: Duration::from_millis(10),
            augment_attempts: 3,
        },
    }
}

/// The pipeline's state documents, in memory.
pub(crate) fn pipeline_docs() -> data_avalanche_state::PipelineDocs {
    let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/p").expect("a root");
    data_avalanche_state::PipelineDocs::new(DocStore::new(Arc::new(store)))
}

/// The pipeline's state documents on `store`, whose writes fail on cue.
pub(crate) fn pipeline_docs_on(store: &Arc<ScriptedStore>) -> data_avalanche_state::PipelineDocs {
    data_avalanche_state::PipelineDocs::new(DocStore::new(
        Arc::clone(store) as Arc<dyn VersionedStore>
    ))
}

/// A controller running as `node`, its target "own".
#[derive(Debug)]
pub(crate) struct Node {
    pub status: tokio::sync::watch::Receiver<crate::ControllerStatus>,
    pub target: tokio::sync::watch::Sender<crate::Target>,
    pub task: tokio::task::JoinHandle<()>,
    pub metrics: Metrics,
}

pub(crate) fn deps(
    name: &str,
    store: &Arc<ScriptedStore>,
    factory: &TestFactory,
) -> crate::ControllerDeps {
    crate::ControllerDeps {
        leases: leases(store),
        holder: holder(name),
        factory: Arc::new(factory.clone()),
        metrics: Metrics::new(),
        settings: settings(),
        starts: Arc::new(tokio::sync::Semaphore::new(1)),
    }
}

pub(crate) fn node(
    name: &str,
    store: &Arc<ScriptedStore>,
    docs: &data_avalanche_state::PipelineDocs,
    factory: &TestFactory,
) -> Node {
    let deps = deps(name, store, factory);
    let (controller, status) = crate::PipelineController::new(pipeline(), docs.clone(), &deps);
    let (target, rx) = tokio::sync::watch::channel(crate::Target::Own);
    Node {
        status,
        target,
        task: tokio::spawn(controller.run(rx)),
        metrics: deps.metrics,
    }
}

/// Waits (on the test clock) until the controller's status satisfies `cond`.
pub(crate) async fn until(
    status: &mut tokio::sync::watch::Receiver<crate::ControllerStatus>,
    what: &str,
    cond: impl Fn(&crate::ControllerStatus) -> bool,
) {
    let waited = tokio::time::timeout(Duration::from_secs(120), status.wait_for(|s| cond(s)))
        .await
        .map(|r| r.map(|_| ()));
    match waited {
        Ok(Ok(())) => {}
        Ok(Err(_)) => panic!("the controller ended before {what}"),
        Err(_) => panic!("timed out waiting until {what}: {:?}", *status.borrow()),
    }
}

/// Waits (on the test clock) until `cond` holds, checking every 10 ms.
pub(crate) async fn eventually(what: &str, cond: impl Fn() -> bool) {
    for _ in 0..12_000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting until {what}");
}
