//! One E2E test's world: a schema `e2e_<test>` with its slot and publication, a RustFS prefix for
//! `globals.state` and the lake, a config file, and the binary as a child process.

use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use data_avalanche_core::TableRef;
use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig};
use data_avalanche_state::{DocStore, LeaseDoc, Leases, PipelineDocs, TableSnapshotDoc, Versioned};
use data_avalanche_storage::{Backend, ObjectStoreVersioned, colocated_state, table_root};
use data_avalanche_testkit::endpoints::{
    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
};
use data_avalanche_testkit::{delete_prefix, stores, unique_prefix};
use futures_util::TryStreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::{Client, NoTls};

/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418).
pub fn pg_port() -> u16 {
    std::env::var("DATA_AVALANCHE_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55418)
}

pub async fn admin() -> Client {
    let dsn = format!(
        "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
        pg_port()
    );
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect to test PostgreSQL");
    tokio::spawn(conn);
    client
}

/// Polls `cond` every 100 ms until it holds, for at most `within`.
pub async fn eventually<F, Fut>(what: &str, within: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let until = Instant::now() + within;
    while !cond().await {
        assert!(Instant::now() < until, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free port")
        .port()
}

/// What a test sets beyond the defaults (YAML flow entries, each starting with `, `).
#[derive(Debug, Clone, Default)]
pub struct Options<'a> {
    /// `tables` entries, e.g. `, snapshots: { mode: never }`.
    pub tables: &'a str,
    /// `sink` entries, e.g. `, flush: { rows: 10 }`.
    pub sink: &'a str,
    /// `source.postgres` entries.
    pub postgres: &'a str,
}

pub struct E2e {
    pub test: String,
    pub schema: String,
    prefix: String,
    port: u16,
    dir: tempfile::TempDir,
    pub admin: Client,
}

impl E2e {
    /// Drops what an earlier run of `test` left in PostgreSQL, creates schema `e2e_<test>` and
    /// runs `ddl` in it, and writes the config.
    pub async fn new(test: &str, ddl: &str, o: Options<'_>) -> Self {
        let admin = admin().await;
        let schema = format!("e2e_{test}");
        drop_slot(&admin, &schema).await;
        admin
            .batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS {schema}_pub;
                 DROP SCHEMA IF EXISTS {schema} CASCADE;
                 CREATE SCHEMA {schema};
                 SET search_path = {schema};
                 {ddl}
                 RESET search_path;"
            ))
            .await
            .expect("test DDL");
        let e = Self {
            test: test.to_owned(),
            schema,
            prefix: unique_prefix(&format!("e2e-{test}")),
            port: free_port(),
            dir: tempfile::tempdir().expect("a temp dir"),
            admin,
        };
        std::fs::write(e.config_path(), e.yaml(&o)).expect("the config file");
        e
    }

    pub fn config_path(&self) -> PathBuf {
        self.dir.path().join("config.yaml")
    }

    fn yaml(&self, o: &Options<'_>) -> String {
        format!(
            r#"config_version: 1
globals:
  cloud: aws
  state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
  api: {{ address: "127.0.0.1:{port}" }}
  log: {{ level: "info,buoyant_kernel=warn", format: compact }}
  cluster: {{ lease_duration: 6s, lease_renewal_interval: 2s, drain_timeout: 30s }}
pipelines:
  {test}:
    id: {test}
    source:
      host: 127.0.0.1
      port: {pg}
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: {{ publication: {schema}_pub, slot: {schema}{postgres} }}
    tables: {{ include: [{schema}.*]{tables} }}
    sink: {{ type: delta, bucket: {bucket}, prefix: {prefix}/lake, delta: {{}}, flush: {{ seconds: 1 }}{sink} }}
"#,
            bucket = TEST_BUCKET,
            prefix = self.prefix,
            port = self.port,
            test = self.test,
            pg = pg_port(),
            schema = self.schema,
            postgres = o.postgres,
            tables = o.tables,
            sink = o.sink,
        )
    }

    /// Starts the binary; `env` adds variables (a crash point).
    pub fn start(&self, env: &[(&str, &str)]) -> Proc {
        let log = self.dir.path().join(format!(
            "run-{}.log",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let out = std::fs::File::create(&log).expect("the log file");
        let err = out.try_clone().expect("the log file");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_data-avalanche"));
        cmd.args(["run", "--config"])
            .arg(self.config_path())
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("AWS_ACCESS_KEY_ID", RUSTFS_ACCESS_KEY)
            .env("AWS_SECRET_ACCESS_KEY", RUSTFS_SECRET_KEY)
            .env("AWS_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL", RUSTFS_URL)
            .env("AWS_ALLOW_HTTP", "true")
            .env("RUST_BACKTRACE", "1")
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err);
        for (k, v) in env {
            cmd.env(k, v);
        }
        Proc {
            child: cmd.spawn().expect("the binary starts"),
            port: self.port,
            log,
        }
    }

    pub fn table(&self, name: &str) -> TableRef {
        TableRef::new(&self.schema, name)
    }

    /// A reader of the pipeline's Delta tables on RustFS.
    pub fn lake(&self) -> DeltaSink {
        let opt = |k: &str, v: &str| (k.to_owned(), v.to_owned());
        DeltaSink::new(DeltaSinkConfig {
            pipeline_id: self.test.clone(),
            location: DeltaLocation::Bucket {
                backend: Backend::S3,
                bucket: TEST_BUCKET.into(),
                prefix: self.lake_prefix(),
                options: vec![
                    opt("endpoint", RUSTFS_URL),
                    opt("allow_http", "true"),
                    opt("region", "us-east-1"),
                    opt("access_key_id", RUSTFS_ACCESS_KEY),
                    opt("secret_access_key", RUSTFS_SECRET_KEY),
                ],
            },
            table: Default::default(),
            merge_memory_limit: None,
        })
    }

    /// The table's rows in the lake, `None` before it exists.
    pub async fn content(&self, table: &str) -> Option<RecordBatch> {
        self.lake()
            .content(&self.table(table))
            .await
            .expect("the lake reads")
    }

    /// The table's `id`s in the lake, in key order.
    pub async fn ids(&self, table: &str) -> Vec<i64> {
        match self.content(table).await {
            None => vec![],
            Some(b) => {
                let ids = b
                    .column_by_name("id")
                    .expect("an id column")
                    .as_primitive::<Int64Type>();
                assert_eq!(ids.null_count(), 0);
                ids.values().to_vec()
            }
        }
    }

    /// The source table's `id`s, in key order.
    pub async fn source_ids(&self, table: &str) -> Vec<i64> {
        self.admin
            .query(
                &format!("SELECT id FROM {}.{table} ORDER BY id", self.schema),
                &[],
            )
            .await
            .expect("the source reads")
            .iter()
            .map(|r| r.get::<_, i64>(0))
            .collect()
    }

    /// Waits until the lake's `table` has exactly the source's ids.
    pub async fn converged(&self, table: &str) {
        let want = self.source_ids(table).await;
        let what = format!("{table} has the source's {} rows", want.len());
        eventually(&what, Duration::from_secs(60), async || {
            self.ids(table).await == want
        })
        .await;
    }

    /// Waits until the pipeline's slot streams to a walsender: from here on, a change is read.
    pub async fn streaming(&self) {
        eventually("the slot streams", Duration::from_secs(60), async || {
            self.admin
                .query_opt(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND active",
                    &[&self.schema],
                )
                .await
                .expect("slots read")
                .is_some()
        })
        .await;
    }

    pub async fn exec(&self, sql: &str) {
        self.admin
            .batch_execute(&format!(
                "SET search_path = {}; {sql}; RESET search_path;",
                self.schema
            ))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    fn docs_at(&self, root: &str) -> DocStore {
        let store = ObjectStoreVersioned::new(stores::rustfs(), root).expect("a root");
        DocStore::new(std::sync::Arc::new(store))
    }

    /// The pipeline's lease in `globals.state`.
    pub async fn lease(&self) -> Option<Versioned<LeaseDoc>> {
        Leases::new(self.docs_at(&format!("{}/globals", self.prefix)))
            .observe(&self.test)
            .await
            .expect("the lease reads")
    }

    /// The pipeline's own documents, co-located with the lake (`<sink.prefix>/.pipeline-state`).
    pub fn docs(&self) -> PipelineDocs {
        let at = colocated_state(TEST_BUCKET, &self.lake_prefix());
        PipelineDocs::new(self.docs_at(&at.root))
    }

    /// The table's snapshot document.
    pub async fn snapshot(&self, table: &str) -> Option<TableSnapshotDoc> {
        self.docs()
            .table_snapshot(&self.table(table))
            .await
            .expect("the snapshot document reads")
            .map(|v| v.doc)
    }

    /// Waits until the table's snapshot is completed (`completed_at` set, and not `before`'s),
    /// and returns its document.
    pub async fn completed(
        &self,
        table: &str,
        before: Option<&TableSnapshotDoc>,
    ) -> TableSnapshotDoc {
        let before = before.and_then(|b| b.completed_at);
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(s) = self.snapshot(table).await
                && s.completed_at.is_some()
                && s.completed_at != before
            {
                return s;
            }
            assert!(
                Instant::now() < until,
                "timed out waiting for {table}'s snapshot"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn lake_prefix(&self) -> String {
        format!("{}/lake", self.prefix)
    }

    fn table_root(&self, table: &str) -> String {
        table_root(TEST_BUCKET, &self.lake_prefix(), &self.schema, table).root
    }

    /// The Delta table's commits: its `_delta_log/*.json` files.
    pub async fn commits(&self, table: &str) -> usize {
        let log = object_store::path::Path::from(format!("{}/_delta_log", self.table_root(table)));
        let files: Vec<_> = stores::rustfs()
            .list(Some(&log))
            .try_collect()
            .await
            .expect("the log lists");
        files
            .iter()
            .filter(|m| m.location.extension() == Some("json"))
            .count()
    }

    /// Deletes the Delta table from the lake, as an operator would.
    pub async fn drop_lake_table(&self, table: &str) {
        delete_prefix(stores::rustfs().as_ref(), &self.table_root(table)).await;
    }

    /// The `data_avalanche_pipeline_status` line for `status`, set.
    pub fn status(&self, status: &str) -> String {
        format!(
            r#"data_avalanche_pipeline_status{{pipeline="{}",source_host="127.0.0.1",source_db="avalanche",status="{status}"}} 1"#,
            self.test
        )
    }

    /// Drops the slot, the publication, the schema and the prefix.
    pub async fn cleanup(self) {
        drop_slot(&self.admin, &self.schema).await;
        self.admin
            .batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS {s}_pub; DROP SCHEMA IF EXISTS {s} CASCADE;",
                s = self.schema
            ))
            .await
            .expect("cleanup");
        delete_prefix(stores::rustfs().as_ref(), &self.prefix).await;
    }
}

/// Drops the slot, waiting for a walsender still holding it to go.
async fn drop_slot(admin: &Client, slot: &str) {
    for _ in 0..100 {
        let r = admin
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
                 WHERE slot_name = $1 AND NOT active",
                &[&slot],
            )
            .await;
        let left = admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .expect("slots read")
            .get::<_, i64>(0);
        if r.is_ok() && left == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("slot {slot} stays active");
}

/// The binary as a child process; killed when dropped.
pub struct Proc {
    child: Child,
    port: u16,
    log: PathBuf,
}

impl Proc {
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    async fn get(&self, path: &str) -> Option<(u16, String)> {
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .ok()?;
        s.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .ok()?;
        let mut out = String::new();
        s.read_to_string(&mut out).await.ok()?;
        let (head, body) = out.split_once("\r\n\r\n")?;
        Some((head.get(9..12)?.parse().ok()?, body.to_owned()))
    }

    /// Waits for `/health/ready`.
    pub async fn ready(&mut self) {
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait().expect("the child") {
                panic!(
                    "the binary exited ({status}) before it was ready:\n{}",
                    self.log()
                );
            }
            if let Some((200, _)) = self.get("/health/ready").await {
                return;
            }
            assert!(
                Instant::now() < until,
                "not ready within 60 s:\n{}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn metrics(&self) -> String {
        self.get("/metrics").await.expect("/metrics answers").1
    }

    /// Waits until the metrics have a line equal to `line`.
    pub async fn metric(&self, line: &str) {
        let what = format!("the metric line {line}");
        eventually(&what, Duration::from_secs(60), async || {
            self.metrics().await.lines().any(|l| l == line)
        })
        .await;
    }

    /// SIGTERM, then the exit (within 60 s).
    pub async fn terminate(mut self) -> ExitStatus {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("kill runs");
        assert!(sent.success());
        self.exit().await
    }

    /// Waits for the process to end by itself (a crash point), within 60 s.
    pub async fn exit(&mut self) -> ExitStatus {
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait().expect("the child") {
                return status;
            }
            assert!(
                Instant::now() < until,
                "still running after 60 s:\n{}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for Proc {
    /// A failing test shows the process's log.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            eprintln!("---- data-avalanche log ----\n{}", self.log());
        }
    }
}
