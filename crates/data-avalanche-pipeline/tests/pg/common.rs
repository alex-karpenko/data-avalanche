//! Shared fixtures: each test owns a schema `pl_<test>`, a slot `pl_<test>` and a publication
//! `pl_<test>_pub`, dropped and re-created at its start, so a failed run leaves nothing that
//! breaks the next one.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, StringArray};
use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::TableRef;
use data_avalanche_pipeline::{
    Backoff, Command, Exit, Hooks, Phase, PipelineWorker, Tuning, WorkerDeps, WorkerError,
    WorkerHandle, WorkerStatus,
};
use data_avalanche_source_pg::PgConnector;
use data_avalanche_state::{DocStore, PipelineDocs};
use data_avalanche_storage::ObjectStoreVersioned;
use data_avalanche_testkit::MemorySink;
use object_store::memory::InMemory;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418).
pub fn port() -> u16 {
    std::env::var("DATA_AVALANCHE_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55418)
}

pub fn dsn() -> String {
    format!(
        "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
        port()
    )
}

/// A superuser connection for test setup and writes.
pub async fn admin() -> Client {
    let (client, conn) = tokio_postgres::connect(&dsn(), NoTls)
        .await
        .expect("connect to test PostgreSQL");
    tokio::spawn(conn);
    client
}

/// The test's names: schema and slot `pl_<test>`, publication `pl_<test>_pub`.
pub struct Names {
    pub schema: String,
    pub slot: String,
    pub publication: String,
}

impl Names {
    pub fn table(&self, name: &str) -> TableRef {
        TableRef::new(&self.schema, name)
    }
}

/// Drops what an earlier run of `test` left and creates its schema again, then runs `ddl` in it.
pub async fn fresh(admin: &Client, test: &str, ddl: &str) -> Names {
    let n = Names {
        schema: format!("pl_{test}"),
        slot: format!("pl_{test}"),
        publication: format!("pl_{test}_pub"),
    };
    drop_slot(admin, &n.slot).await;
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {pubn};
             DROP SCHEMA IF EXISTS {s} CASCADE;
             CREATE SCHEMA {s};
             SET search_path = {s};
             {ddl}
             RESET search_path;",
            pubn = n.publication,
            s = n.schema
        ))
        .await
        .expect("test DDL");
    n
}

/// A pipeline on the test server reading `include`, with `snapshots` settings (YAML flow
/// entries; the mode defaults to `never`). It flushes at every transaction.
pub fn pipeline(n: &Names, include: &[&str], snapshots: &str) -> Pipeline {
    pipeline_with(n, include, snapshots, "")
}

/// [`pipeline`] with more pipeline keys (YAML lines indented four spaces, each starting with a
/// newline).
pub fn pipeline_with(n: &Names, include: &[&str], snapshots: &str, more: &str) -> Pipeline {
    let include: Vec<String> = include
        .iter()
        .map(|i| format!("{}.{i}", n.schema))
        .collect();
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: p
    source:
      host: 127.0.0.1
      port: {port}
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: {{ publication: {publication}, slot: {slot}, heartbeat: 1s }}
    tables:
      include: [{include}]
      snapshots: {{ {snapshots} }}
    sink: {{ type: delta, bucket: lake, delta: {{}}, flush: {{ seconds: 0 }} }}{more}
"#,
        port = port(),
        publication = n.publication,
        slot = n.slot,
        include = include.join(", "),
    );
    load_str(
        &yaml,
        &BTreeMap::new(),
        &FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
    .config
    .pipelines
    .into_values()
    .next()
    .expect("one pipeline")
}

/// The sink and the state documents a pipeline keeps across its runs.
#[derive(Clone)]
pub struct Lake {
    pub sink: MemorySink,
    pub docs: PipelineDocs,
}

impl Lake {
    pub fn new() -> Self {
        let store =
            ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/p").expect("a root");
        Self {
            sink: MemorySink::new(),
            docs: PipelineDocs::new(DocStore::new(Arc::new(store))),
        }
    }

    /// The table's `(id, v)` rows in the sink, in key order.
    pub fn rows(&self, table: &TableRef) -> Vec<(i64, Option<String>)> {
        let Some(b) = self.sink.content(table) else {
            return vec![];
        };
        let ids = b
            .column_by_name("id")
            .expect("an id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bigint ids");
        let vs = b
            .column_by_name("v")
            .expect("a v column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a text v");
        (0..b.num_rows())
            .map(|i| (ids.value(i), vs.is_valid(i).then(|| vs.value(i).to_owned())))
            .collect()
    }
}

/// The table's `(id, v)` rows in the source, in key order.
pub async fn source_rows(admin: &Client, table: &TableRef) -> Vec<(i64, Option<String>)> {
    admin
        .query(&format!("SELECT id, v FROM {table} ORDER BY id"), &[])
        .await
        .expect("read the source table")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Waits up to 60 s until the sink's `table` holds what the source's does. The stream reports a
/// server's position only at its own commits and at keep-alives, so a test waits for the data
/// itself, not for a WAL position.
pub async fn converge(admin: &Client, lake: &Lake, table: &TableRef) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let want = source_rows(admin, table).await;
        let have = lake.rows(table);
        if have == want {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            assert_eq!(have, want, "{table} doesn't converge");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Fast enough for a test: reconnects and checks within tenths of a second.
pub fn tuning() -> Tuning {
    let short = Backoff {
        first: Duration::from_millis(100),
        max: Duration::from_secs(1),
    };
    Tuning {
        source_backoff: short,
        sink_backoff: short,
        in_flight_backoff: short,
        tick: Duration::from_millis(100),
        ..Tuning::default()
    }
}

/// A running worker.
pub struct Running {
    pub handle: WorkerHandle,
    pub task: JoinHandle<Result<Exit, WorkerError>>,
}

/// Starts a worker of `p` over `lake`, with `hooks` when given.
pub fn start(p: &Pipeline, lake: &Lake, hooks: Option<Arc<dyn Hooks>>) -> Running {
    data_avalanche_testkit::install_crypto_provider();
    let source = PgConnector::new(p).expect("connector");
    let mut deps = WorkerDeps::new(
        Arc::new(source),
        Arc::new(lake.sink.clone()),
        lake.docs.clone(),
    );
    if let Some(h) = hooks {
        deps.hooks = h;
    }
    let (worker, handle) = PipelineWorker::new(p, deps, tuning());
    Running {
        handle,
        task: tokio::spawn(worker.run()),
    }
}

impl Running {
    /// Waits until the worker streams and no snapshot runs (a finished one is durable).
    pub async fn snapshotted(&self) {
        self.wait("the snapshots are done", |st| {
            matches!(st.phase, Phase::Streaming) && st.snapshots.is_empty()
        })
        .await;
    }

    /// Waits up to 60 s for the status to satisfy `f`.
    pub async fn wait(&self, what: &str, f: impl FnMut(&WorkerStatus) -> bool) {
        let mut st = self.handle.status.clone();
        let waited = tokio::time::timeout(Duration::from_secs(60), st.wait_for(f))
            .await
            .map(|r| r.is_ok());
        match waited {
            Ok(true) => {}
            Ok(false) => panic!("{what}: the worker ended: {:?}", *st.borrow()),
            Err(_) => panic!("{what}: timed out: {:?}", *st.borrow()),
        }
    }

    /// Drains the worker and returns how it ended.
    pub async fn drain(self) -> Result<Exit, WorkerError> {
        let _ = self.handle.control.send(Command::Drain).await;
        self.end().await
    }

    /// Waits for the worker to end on its own.
    pub async fn end(self) -> Result<Exit, WorkerError> {
        tokio::time::timeout(Duration::from_secs(60), self.task)
            .await
            .expect("the worker ends")
            .expect("the worker task doesn't panic")
    }
}

/// Drops the replication slot `slot`, once no walsender holds it.
pub async fn drop_slot(admin: &Client, slot: &str) {
    for i in 0..100 {
        let pid: Option<i32> = admin
            .query_one(
                "SELECT (SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot],
            )
            .await
            .expect("read the test slot")
            .get(0);
        let Some(pid) = pid else { break };
        if i == 50 {
            admin
                .execute("SELECT pg_terminate_backend($1)", &[&pid])
                .await
                .expect("terminate the walsender");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .expect("drop the test slot");
}

/// The test's slot, publication and schema, dropped at the end of a passing test.
pub async fn cleanup(admin: &Client, n: &Names) {
    drop_slot(admin, &n.slot).await;
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {}; DROP SCHEMA IF EXISTS {} CASCADE;",
            n.publication, n.schema
        ))
        .await
        .expect("drop the test schema");
}
