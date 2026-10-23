//! One E2E test's world on the source under test ([`Source::from_env`]): PostgreSQL's schema
//! `e2e_<test>` with its slot and publication, or MySQL's database `e2e_<test>` with a user of
//! that name; a RustFS prefix for `globals.state` and the lake, a config file, and the binary as
//! a child process.

use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use data_avalanche_core::{Sink, SourcePosition, TableRef};
use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig};
use data_avalanche_state::{
    DocStore, HaltsDoc, LeaseDoc, Leases, PipelineDocs, TableSnapshotDoc, Versioned,
};
use data_avalanche_storage::{Backend, ObjectStoreVersioned, colocated_state, table_root};
use data_avalanche_testkit::endpoints::{
    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
};
use data_avalanche_testkit::{delete_prefix, stores, unique_prefix};
use futures_util::TryStreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub use crate::source::{Db, Source};

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

/// A free UDP port on loopback: a node's gossip port.
pub fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .and_then(|s| s.local_addr())
        .expect("a free UDP port")
        .port()
}

/// What a test sets beyond the defaults (YAML flow entries, each starting with `, `).
#[derive(Debug, Clone, Default)]
pub struct Options<'a> {
    /// `tables` entries, e.g. `, snapshots: { mode: never }`. `@SCHEMA@.` qualifies a table
    /// name as the source's patterns do: `e2e_<test>.` on PostgreSQL, nothing on MySQL (V-064).
    pub tables: &'a str,
    /// `sink` entries, e.g. `, flush: { rows: 10 }`.
    pub sink: &'a str,
    /// `source` keys beyond the connection, one YAML line, e.g. `timeouts: { command: 2s }`.
    pub source: &'a str,
    /// `source.postgres` entries, on PostgreSQL.
    pub postgres: &'a str,
    /// `source.mysql` entries, on MySQL.
    pub mysql: &'a str,
    /// The port the pipeline connects to, when not the server's (a proxy in front of it).
    pub port: Option<u16>,
    /// Pipeline keys beyond `source`, `tables` and `sink`, one YAML line, e.g.
    /// `schema_evolution: { rescan_interval: 2s }`.
    pub pipeline: &'a str,
    /// `globals.cluster` entries, each `, key: value` a line of the block, e.g.
    /// `, rebalance_stabilization: 3s`.
    pub cluster: &'a str,
    /// `globals.cluster.memberlist` entries, e.g. `, join_members: ["127.0.0.1:7946"]`.
    pub memberlist: &'a str,
    /// More pipelines, YAML under `pipelines:` (2-space indent); `@SCHEMA@`, `@PREFIX@` and
    /// `@BUCKET@` are replaced, `@INCLUDE@` by the pattern of every table of the test, and a line
    /// `@SOURCE(<suffix>)@` by the source block ([`E2e::source_block`]).
    pub pipelines: &'a str,
}

/// The rows [`E2e::insert_series`] gives a key `g`.
#[derive(Debug, Clone, Copy)]
pub enum Doc {
    /// `'row <g>'`.
    Row,
    /// `n` times `x`.
    Pad(u32),
}

pub struct E2e {
    pub test: String,
    /// PostgreSQL's schema, MySQL's database: `e2e_<test>`.
    pub schema: String,
    pub source: Source,
    prefix: String,
    /// The first node's (`n1`) API port.
    port: u16,
    dir: tempfile::TempDir,
    /// The config every node runs; its per-node values come from `E2E_*` variables, as a
    /// cluster's one config takes them from each pod's environment (D-3: a node runs the
    /// cluster's generation, not its own file).
    template: String,
    pub db: Db,
}

impl E2e {
    /// Drops what an earlier run of `test` left in the source, creates its schema (PostgreSQL)
    /// or database and user (MySQL), runs `ddl` in it, and writes the config. On PostgreSQL it
    /// resets the server-wide `max_slot_wal_keep_size` first.
    pub async fn new(test: &str, ddl: &str, o: Options<'_>) -> Self {
        let source = Source::from_env();
        let db = Db::connect(source).await;
        let schema = format!("e2e_{test}");
        match source {
            Source::Postgres { .. } => {
                // A scenario sets it (guards.rs) and restores it, but a run killed in between
                // leaves it set for the next. ALTER SYSTEM runs outside a transaction: one per
                // call.
                for sql in [
                    "ALTER SYSTEM RESET max_slot_wal_keep_size",
                    "SELECT pg_reload_conf()",
                ] {
                    db.exec(sql).await.expect(sql);
                }
                drop_slot(&db, &schema).await;
                db.exec(&format!(
                    "DROP PUBLICATION IF EXISTS {schema}_pub;
                     DROP SCHEMA IF EXISTS {schema} CASCADE;
                     CREATE SCHEMA {schema};
                     SET search_path = {schema};
                     {ddl}
                     RESET search_path;"
                ))
                .await
                .expect("test DDL");
            }
            Source::Mysql(_) => {
                // The pipeline's own user: its binlog dump is the one in the process list with
                // that user ([`Self::stream_open`]).
                db.exec(&format!(
                    "DROP DATABASE IF EXISTS {schema};
                     CREATE DATABASE {schema};
                     DROP USER IF EXISTS '{schema}'@'%';
                     CREATE USER '{schema}'@'%' IDENTIFIED BY 'avalanche';
                     GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '{schema}'@'%';"
                ))
                .await
                .expect("the test database");
                if !ddl.is_empty() {
                    db.exec(&format!("USE {schema}; {ddl}"))
                        .await
                        .expect("test DDL");
                }
            }
        }
        let mut e = Self {
            test: test.to_owned(),
            schema,
            source,
            prefix: unique_prefix(&format!("e2e-{test}")),
            port: free_port(),
            dir: tempfile::tempdir().expect("a temp dir"),
            template: String::new(),
            db,
        };
        e.template = e.yaml(&o);
        std::fs::write(e.config_path(), &e.template).expect("the config file");
        e
    }

    fn config_path(&self) -> PathBuf {
        self.dir.path().join("config.yaml")
    }

    /// The config file every node reads.
    pub fn config(&self) -> String {
        std::fs::read_to_string(self.config_path()).expect("the config file")
    }

    /// Rewrites the config file: what the next reload publishes (Clustering §9.2).
    pub fn write_config(&self, text: &str) {
        std::fs::write(self.config_path(), text).expect("the config file");
    }

    /// The `source` block of a pipeline named `test` (on PostgreSQL the slot and publication
    /// `e2e_<test>` take `suffix`; on MySQL it is ignored, both pipelines share the test's
    /// database and user), indented for a pipeline under `pipelines:`; `extra` is one more line of
    /// `source` keys and `postgres` / `mysql` their flow entries.
    pub fn source_block(
        &self,
        suffix: &str,
        port: Option<u16>,
        extra: &str,
        postgres: &str,
        mysql: &str,
    ) -> String {
        let s = &self.schema;
        match self.source {
            Source::Postgres { port: pg } => format!(
                "    source:
      host: 127.0.0.1
      port: {port}
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      {extra}
      postgres: {{ publication: {s}{suffix}_pub, slot: {s}{suffix}{postgres} }}",
                port = port.unwrap_or(pg),
            ),
            Source::Mysql(t) => format!(
                "    source:
      type: mysql
      host: 127.0.0.1
      port: {port}
      db: {s}
      user: {s}
      password: avalanche
      ssl_mode: disable
      {extra}
      mysql: {{ replication_type: {repl}{mysql} }}",
                port = port.unwrap_or(t.port),
                repl = t.replication,
            ),
        }
    }

    /// `@SCHEMA@.` as the source's table patterns qualify a name.
    fn qualify(&self, text: &str) -> String {
        let q = match self.source {
            Source::Postgres { .. } => format!("{}.", self.schema),
            Source::Mysql(_) => String::new(),
        };
        text.replace("@SCHEMA@.", &q)
    }

    fn yaml(&self, o: &Options<'_>) -> String {
        let include = self.qualify("@SCHEMA@.*");
        let mut more = o.pipelines.to_owned();
        while let Some(at) = more.find("@SOURCE(") {
            let end = at + more[at..].find(")@").expect("@SOURCE(<suffix>)@");
            let block = self.source_block(&more[at + 8..end], None, "", "", "");
            more.replace_range(at..end + 2, &block);
        }
        let more = more
            .replace("@INCLUDE@", &include)
            .replace("@SCHEMA@", &self.schema)
            .replace("@PREFIX@", &self.prefix)
            .replace("@BUCKET@", TEST_BUCKET);
        format!(
            r#"config_version: 1
globals:
  cloud: aws
  state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
  api: {{ address: "127.0.0.1:${{E2E_API_PORT}}" }}
  log: {{ level: info, format: compact }}
  cluster:
    lease_duration: 6s
    lease_renewal_interval: 2s
    drain_timeout: 30s{cluster}
    memberlist: {{ node_name: "${{E2E_NODE}}", bind_addr: [127.0.0.1], bind_port: "${{E2E_GOSSIP_PORT}}", advertise_addr: 127.0.0.1, advertise_port: "${{E2E_ADVERTISE_PORT}}"{memberlist} }}
pipelines:
  {test}:
    id: {test}
{source}
    tables: {{ include: ['{include}']{tables} }}
    sink: {{ type: delta, bucket: {bucket}, prefix: {prefix}/lake, delta: {{}}, flush: {{ seconds: 1 }}{sink} }}
    {pipeline}
{more}
"#,
            bucket = TEST_BUCKET,
            prefix = self.prefix,
            test = self.test,
            source = self.source_block("", o.port, o.source, o.postgres, o.mysql),
            tables = self.qualify(o.tables),
            sink = o.sink,
            pipeline = o.pipeline,
            cluster = o.cluster.replace(", ", "\n    "),
            memberlist = o.memberlist,
        )
    }

    /// Starts the binary as node `n1`; `env` adds variables (a crash point).
    pub fn start(&self, env: &[(&str, &str)]) -> Proc {
        self.spawn("n1", self.port, env)
    }

    /// Starts the binary as another node, with its own name and API port.
    pub fn start_node(&self, node: &str) -> Proc {
        self.start_node_with(node, &[])
    }

    /// [`Self::start_node`] with more variables (a node's gossip ports).
    pub fn start_node_with(&self, node: &str, env: &[(&str, &str)]) -> Proc {
        self.spawn(node, free_port(), env)
    }

    /// Runs node `node` with its API on `port`, gossiping alone on a loopback port of its own;
    /// `env` adds variables (and may set `E2E_ADVERTISE_PORT`).
    fn spawn(&self, node: &str, port: u16, env: &[(&str, &str)]) -> Proc {
        let gossip = free_udp_port().to_string();
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
            .env("E2E_NODE", node)
            .env("E2E_API_PORT", port.to_string())
            .env("E2E_GOSSIP_PORT", &gossip)
            .env("E2E_ADVERTISE_PORT", &gossip)
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err);
        for (k, v) in env {
            cmd.env(k, v);
        }
        Proc {
            child: cmd.spawn().expect("the binary starts"),
            port,
            log,
        }
    }

    pub fn table(&self, name: &str) -> TableRef {
        TableRef::new(&self.schema, name)
    }

    /// A reader of the pipeline's Delta tables on RustFS.
    pub fn lake(&self) -> DeltaSink {
        self.lake_in("lake")
    }

    /// [`Self::lake`] of the pipeline whose sink prefix ends in `lake` (`lake`, `lake_b`).
    pub fn lake_in(&self, lake: &str) -> DeltaSink {
        let opt = |k: &str, v: &str| (k.to_owned(), v.to_owned());
        DeltaSink::new(DeltaSinkConfig {
            pipeline_id: self.test.clone(),
            location: DeltaLocation::Bucket {
                backend: Backend::S3,
                bucket: TEST_BUCKET.into(),
                prefix: format!("{}/{lake}", self.prefix),
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
        self.ids_in("lake", table).await
    }

    /// [`Self::ids`] in the lake `lake`.
    pub async fn ids_in(&self, lake: &str, table: &str) -> Vec<i64> {
        let content = self
            .lake_in(lake)
            .content(&self.table(table))
            .await
            .expect("the lake reads");
        match content {
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
        self.source_rows(table, &["id"])
            .await
            .iter()
            .map(|r| r[0].parse().expect("an id"))
            .collect()
    }

    /// The lake's `cols` of `table` as text (`NULL` for a null), in key order.
    pub async fn rows(&self, table: &str, cols: &[&str]) -> Vec<Vec<String>> {
        self.try_rows(table, cols)
            .await
            .unwrap_or_else(|| panic!("the lake's {table} has no column among {cols:?}"))
    }

    /// `rows`, or `None` when the lake's `table` lacks one of `cols` (a pipeline that lags behind
    /// a DDL: DA-X-14). A missing table is no rows.
    pub async fn try_rows(&self, table: &str, cols: &[&str]) -> Option<Vec<Vec<String>>> {
        let Some(b) = self.content(table).await else {
            return Some(vec![]);
        };
        let options = FormatOptions::default().with_null("NULL");
        let columns: Vec<_> = cols
            .iter()
            .map(|c| {
                let a = b.column_by_name(c)?;
                Some(ArrayFormatter::try_new(a.as_ref(), &options).expect("a formatter"))
            })
            .collect::<Option<_>>()?;
        Some(
            (0..b.num_rows())
                .map(|i| columns.iter().map(|f| f.value(i).to_string()).collect())
                .collect(),
        )
    }

    /// The source's `cols` of `table` as the server prints them (`NULL` for a null), in key
    /// order.
    pub async fn source_rows(&self, table: &str, cols: &[&str]) -> Vec<Vec<String>> {
        let sql = format!(
            "SELECT {} FROM {}.{table} ORDER BY id",
            cols.join(", "),
            self.schema
        );
        self.db
            .text(&sql)
            .await
            .expect("the source reads")
            .into_iter()
            .map(|r| {
                r.into_iter()
                    .map(|c| c.unwrap_or_else(|| "NULL".into()))
                    .collect()
            })
            .collect()
    }

    /// Waits until the lake's `table` has the source's rows, compared on `cols`.
    pub async fn mirrored(&self, table: &str, cols: &[&str]) {
        let want = self.source_rows(table, cols).await;
        let what = format!("{table} has the source's {} rows on {cols:?}", want.len());
        eventually(&what, Duration::from_secs(60), async || {
            self.try_rows(table, cols).await.as_ref() == Some(&want)
        })
        .await;
    }

    /// Waits until the lake's `table` has exactly the source's ids.
    pub async fn converged(&self, table: &str) {
        self.converged_in("lake", table, Duration::from_secs(60))
            .await;
    }

    /// [`Self::converged`] in the lake `lake`, waiting at most `within`.
    pub async fn converged_in(&self, lake: &str, table: &str, within: Duration) {
        let want = self.source_ids(table).await;
        let what = format!("{lake}/{table} has the source's {} rows", want.len());
        eventually(&what, within, async || {
            self.ids_in(lake, table).await == want
        })
        .await;
    }

    /// Whether the source streams to the pipeline now: PostgreSQL's slot has a walsender,
    /// MySQL runs a binlog dump for the pipeline's user.
    pub async fn stream_open(&self) -> bool {
        let sql = match self.source {
            Source::Postgres { .. } => format!(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}' AND active",
                self.schema
            ),
            Source::Mysql(_) => format!(
                "SELECT 1 FROM information_schema.PROCESSLIST
                 WHERE USER = '{}' AND COMMAND LIKE 'Binlog Dump%'",
                self.schema
            ),
        };
        self.db.any(&sql).await
    }

    /// Waits until the source streams to the pipeline: from here on, a change is read.
    pub async fn streaming(&self) {
        eventually("the source streams", Duration::from_secs(60), async || {
            self.stream_open().await
        })
        .await;
    }

    /// The position the table's latest commit marker names.
    pub async fn marker(&self, table: &str) -> SourcePosition {
        self.lake()
            .table_state(&self.table(table))
            .await
            .expect("the lake reads")
            .marker
            .expect("a commit marker")
            .committed_position
    }

    /// Whether the source's acknowledged position has reached `at`: PostgreSQL's slot, MySQL's
    /// checkpoint (the binlog keeps no position for the pipeline: the checkpoint is its ack).
    pub async fn acknowledges(&self, at: &SourcePosition) -> bool {
        match self.source {
            Source::Postgres { .. } => {
                self.db
                    .any(&format!(
                        "SELECT 1 FROM pg_replication_slots
                         WHERE slot_name = '{}' AND confirmed_flush_lsn >= '{}'::pg_lsn",
                        self.schema,
                        at.untagged()
                    ))
                    .await
            }
            Source::Mysql(_) => self
                .docs()
                .position()
                .await
                .expect("the position reads")
                .is_some_and(|v| v.doc.position.covers(at).expect("one dialect")),
        }
    }

    /// Waits until the source's acknowledged position reaches `at`: everything before it is
    /// committed and checkpointed.
    pub async fn acknowledged(&self, at: &SourcePosition) {
        let what = format!("the source acknowledges {at}");
        eventually(&what, Duration::from_secs(60), async || {
            self.acknowledges(at).await
        })
        .await;
    }

    /// Runs `sql` in the test's schema or database.
    pub async fn exec(&self, sql: &str) {
        let wrapped = match self.source {
            Source::Postgres { .. } => {
                format!(
                    "SET search_path = {}; {sql}; RESET search_path;",
                    self.schema
                )
            }
            Source::Mysql(_) => format!("USE {}; {sql}", self.schema),
        };
        self.db
            .exec(&wrapped)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Inserts keys 1..=`n` into `table` (`id`, `doc`) in one statement: one transaction.
    pub async fn insert_series(&self, table: &str, n: u64, doc: Doc) {
        self.exec(&self.series(table, n, doc)).await;
    }

    /// The statement of [`Self::insert_series`] (MySQL: with the session setting before it).
    pub fn series(&self, table: &str, n: u64, doc: Doc) -> String {
        match (self.source, doc) {
            (Source::Postgres { .. }, Doc::Row) => {
                format!("INSERT INTO {table} SELECT g, 'row ' || g FROM generate_series(1, {n}) g")
            }
            (Source::Postgres { .. }, Doc::Pad(w)) => format!(
                "INSERT INTO {table} SELECT g, repeat('x', {w}) FROM generate_series(1, {n}) g"
            ),
            (Source::Mysql(t), doc) => {
                let doc = match doc {
                    Doc::Row => "CONCAT('row ', g)".to_owned(),
                    Doc::Pad(w) => format!("REPEAT('x', {w})"),
                };
                // The recursion's ceiling, 1000 by default on both: MySQL's
                // `cte_max_recursion_depth`, MariaDB's `max_recursive_iterations`.
                let var = if t.mariadb {
                    "max_recursive_iterations"
                } else {
                    "cte_max_recursion_depth"
                };
                let depth = format!("SET SESSION {var} = {n}; ");
                format!(
                    "{depth}INSERT INTO {table} (id, doc) WITH RECURSIVE s (g) AS
                     (SELECT 1 UNION ALL SELECT g + 1 FROM s WHERE g < {n}) SELECT g, {doc} FROM s"
                )
            }
        }
    }

    fn docs_at(&self, root: &str) -> DocStore {
        let store = ObjectStoreVersioned::new(stores::rustfs(), root).expect("a root");
        DocStore::new(std::sync::Arc::new(store))
    }

    /// The pipeline's lease in `globals.state`.
    pub async fn lease(&self) -> Option<Versioned<LeaseDoc>> {
        self.lease_of(&self.test).await
    }

    /// Pipeline `id`'s lease in `globals.state`.
    pub async fn lease_of(&self, id: &str) -> Option<Versioned<LeaseDoc>> {
        Leases::new(self.docs_at(&format!("{}/globals", self.prefix)))
            .observe(id)
            .await
            .expect("the lease reads")
    }

    /// Drops the slot and publication of the pipeline whose source block took `suffix`
    /// ([`Options::pipelines`]' `@SOURCE(<suffix>)@`), on PostgreSQL; [`Self::cleanup`] drops
    /// only the test's own.
    pub async fn drop_source(&self, suffix: &str) {
        if let Source::Postgres { .. } = self.source {
            let name = format!("{}{suffix}", self.schema);
            drop_slot(&self.db, &name).await;
            self.db
                .exec(&format!("DROP PUBLICATION IF EXISTS {name}_pub"))
                .await
                .expect("the publication drops");
        }
    }

    /// On PostgreSQL, whether the slot of the pipeline whose source block took `suffix` exists;
    /// `None` on MySQL, which has none.
    pub async fn slot_exists(&self, suffix: &str) -> Option<bool> {
        let Source::Postgres { .. } = self.source else {
            return None;
        };
        let slot = format!("{}{suffix}", self.schema);
        let n = self
            .db
            .pg()
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .expect("slots read")
            .get::<_, i64>(0);
        Some(n > 0)
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

    /// The pipeline's halt registry.
    pub async fn halts(&self) -> HaltsDoc {
        self.docs().halts().await.expect("the halts read")
    }

    /// The Delta table's commits in version order, each its actions.
    pub async fn log(&self, table: &str) -> Vec<Vec<serde_json::Value>> {
        self.log_in("lake", table).await
    }

    /// [`Self::log`] in the lake `lake`.
    pub async fn log_in(&self, lake: &str, table: &str) -> Vec<Vec<serde_json::Value>> {
        let store = stores::rustfs();
        let root = table_root(
            TEST_BUCKET,
            &format!("{}/{lake}", self.prefix),
            &self.schema,
            table,
        )
        .root;
        let log = object_store::path::Path::from(format!("{root}/_delta_log"));
        let mut files: Vec<_> = store
            .list(Some(&log))
            .try_collect::<Vec<_>>()
            .await
            .expect("the log lists")
            .into_iter()
            .map(|m| m.location)
            .filter(|l| l.extension() == Some("json"))
            .collect();
        files.sort();
        let mut commits = vec![];
        for f in files {
            let bytes = store
                .get_opts(&f, object_store::GetOptions::default())
                .await
                .expect("a commit")
                .bytes()
                .await
                .expect("a commit");
            let text = String::from_utf8(bytes.to_vec()).expect("UTF-8");
            commits.push(
                text.lines()
                    .map(|l| serde_json::from_str(l).expect("an action"))
                    .collect(),
            );
        }
        commits
    }

    fn lake_prefix(&self) -> String {
        format!("{}/lake", self.prefix)
    }

    fn table_root(&self, table: &str) -> String {
        table_root(TEST_BUCKET, &self.lake_prefix(), &self.schema, table).root
    }

    /// The Delta table's root as an `s3://` URL.
    pub fn table_url(&self, table: &str) -> String {
        format!("s3://{TEST_BUCKET}/{}", self.table_root(table))
    }

    /// The Delta table's commits: its `_delta_log/*.json` files.
    pub async fn commits(&self, table: &str) -> usize {
        self.log(table).await.len()
    }

    /// Deletes the Delta table from the lake, as an operator would.
    pub async fn drop_lake_table(&self, table: &str) {
        delete_prefix(stores::rustfs().as_ref(), &self.table_root(table)).await;
    }

    /// The metrics' `source_db` label: PostgreSQL's database, MySQL's (the test's).
    pub fn source_db(&self) -> &str {
        match self.source {
            Source::Postgres { .. } => "avalanche",
            Source::Mysql(_) => &self.schema,
        }
    }

    /// The `data_avalanche_pipeline_status` line of pipeline `pipeline` for `status`, set.
    pub fn status_of(&self, pipeline: &str, status: &str) -> String {
        format!(
            r#"data_avalanche_pipeline_status{{pipeline="{pipeline}",source_host="127.0.0.1",source_db="{}",status="{status}"}} 1"#,
            self.source_db()
        )
    }

    /// The `data_avalanche_pipeline_status` line for `status`, set.
    pub fn status(&self, status: &str) -> String {
        self.status_of(&self.test, status)
    }

    /// The labels of the pipeline's series.
    pub fn labels(&self) -> String {
        format!(
            r#"pipeline="{}",source_host="127.0.0.1",source_db="{}""#,
            self.test,
            self.source_db()
        )
    }

    /// Drops what the test made in the source, and the prefix.
    pub async fn cleanup(self) {
        let s = &self.schema;
        match self.source {
            Source::Postgres { .. } => {
                drop_slot(&self.db, s).await;
                self.db
                    .exec(&format!(
                        "DROP PUBLICATION IF EXISTS {s}_pub; DROP SCHEMA IF EXISTS {s} CASCADE;"
                    ))
                    .await
                    .expect("cleanup");
            }
            Source::Mysql(_) => self
                .db
                .exec(&format!(
                    "DROP DATABASE IF EXISTS {s}; DROP USER IF EXISTS '{s}'@'%';"
                ))
                .await
                .expect("cleanup"),
        }
        delete_prefix(stores::rustfs().as_ref(), &self.prefix).await;
    }
}

/// Drops the slot, waiting for a walsender still holding it to go.
async fn drop_slot(db: &Db, slot: &str) {
    let admin = db.pg();
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
        self.request("GET", path).await
    }

    /// A request without a body: the status code and the body.
    async fn request(&self, method: &str, path: &str) -> Option<(u16, String)> {
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .ok()?;
        s.write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
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

    /// `POST /v1/config/reload`: the status code and the JSON body.
    pub async fn reload(&self) -> (u16, serde_json::Value) {
        let (code, body) = self
            .request("POST", "/v1/config/reload")
            .await
            .expect("the reload answers");
        let v = serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}"));
        (code, v)
    }

    /// `GET /v1/config/status`'s JSON body.
    pub async fn config_status(&self) -> serde_json::Value {
        let (code, body) = self
            .get("/v1/config/status")
            .await
            .expect("the status answers");
        assert_eq!(code, 200, "{body}");
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}"))
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

    /// Sends `signal` (`STOP`, `CONT`): a stop-the-world pause and its end.
    pub fn signal(&self, signal: &str) {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args([&format!("-{signal}"), &pid])
            .status()
            .expect("kill runs");
        assert!(sent.success(), "kill -{signal}");
    }

    /// `kill -9`, as a host failure would.
    pub async fn kill(mut self) -> ExitStatus {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args(["-KILL", &pid])
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
