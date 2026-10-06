//! Fixtures for the crate's unit tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array, StringArray};
use async_trait::async_trait;
use bytes::Bytes;
use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ColumnSchema, ColumnValue, ColumnValueType as V, Op, Row, Sink, SourcePosition,
    SourceSeq, TableRef, TableSchema, TableSpec, TransactionBoundary,
};
use data_avalanche_state::{DocStore, PipelineDocs, SlotDoc, SnapshotStatus, TableSnapshotDoc};
use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
use data_avalanche_testkit::{FakeSource, MemorySink};
use object_store::memory::InMemory;
use tokio::sync::{mpsc, watch};

use crate::session::Core;
use crate::settings::Settings;
use crate::status::Streak;
use crate::{
    Accumulator, Backoff, Command, CommitGuard, FlushPolicy, HookAction, HookPoint, Hooks, Lineage,
    NoHooks, Tuning, Unguarded, WorkerStatus,
};

/// A PostgreSQL → Delta pipeline `p` reading `public.*`, with more `tables` keys (YAML lines
/// indented six spaces), more `sink` and `postgres` keys (YAML flow entries).
pub(crate) fn pipeline(tables: &str, sink: &str, postgres: &str) -> Pipeline {
    Yaml {
        tables,
        sink,
        postgres,
        ..Yaml::default()
    }
    .build()
}

/// The test pipeline's extra YAML. `pipeline`, `source` and `tables` are lines (each starting
/// with a newline, indented four, six and six spaces); `sink` and `postgres` are flow entries.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Yaml<'a> {
    pub pipeline: &'a str,
    pub source: &'a str,
    pub postgres: &'a str,
    pub tables: &'a str,
    pub sink: &'a str,
}

impl Yaml<'_> {
    pub fn build(self) -> Pipeline {
        let sep = |s: &str| {
            if s.is_empty() {
                String::new()
            } else {
                format!(", {s}")
            }
        };
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
      port: 55418
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: {{ publication: p_pub, slot: p_slot{postgres} }}{source}
    tables:
      include: [public.*]{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}}{sink} }}{top}
"#,
            postgres = sep(self.postgres),
            source = self.source,
            tables = self.tables,
            sink = sep(self.sink),
            top = self.pipeline,
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
}

/// `public.<name>`: `id bigint` (the key) and `doc text`.
pub(crate) fn schema(name: &str) -> TableSchema {
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

pub(crate) fn lsn(n: u64) -> SourcePosition {
    SourcePosition::Pg(Lsn(n))
}

pub(crate) fn row(id: i64, doc: &str) -> Row {
    Row(vec![
        ("id".into(), ColumnValue::Int64(id)),
        ("doc".into(), ColumnValue::Text(doc.into())),
    ])
}

/// An accumulator over `public.<names>` resuming at `from`, flushing at every boundary.
pub(crate) fn accumulator(names: &[&str], from: u64) -> Accumulator {
    let lineage = Lineage {
        source_db: "db".into(),
        pipeline: "p".into(),
    };
    let policy = FlushPolicy {
        seconds: Some(0),
        rows: None,
        bytes: None,
    };
    let mut a = Accumulator::new(lineage, policy, lsn(from));
    for n in names {
        a.add_table(schema(n), &[]).expect("a new table");
    }
    a
}

/// One committed transaction of inserts, each `(table, id, doc)`, at `pos`.
pub(crate) fn txn(a: &mut Accumulator, xid: u64, pos: u64, rows: &[(&str, i64, &str)]) {
    let now = Instant::now();
    a.begin(xid).expect("no open transaction");
    for (i, (t, id, doc)) in rows.iter().enumerate() {
        let e = CdcEvent {
            op: Op::Insert,
            table: TableRef::new("public", *t),
            before: None,
            after: Some(row(*id, doc)),
            source_pos: lsn(pos),
            change_seq: SourceSeq::Pg(Lsn(pos - 10 + i as u64)),
            xid,
            commit_ts_micros: 1,
        };
        a.apply(&e, now).expect("a valid event");
    }
    let b = TransactionBoundary {
        xid,
        commit_pos: lsn(pos),
        commit_ts_micros: 1,
    };
    a.commit(&b, now).expect("the open transaction");
}

/// Pipeline documents over an in-memory store, and the store for faults.
pub(crate) fn docs() -> (PipelineDocs, Arc<FlakyStore>) {
    let inner = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/p").expect("a root");
    let store = Arc::new(FlakyStore {
        inner,
        failing: Mutex::new(vec![]),
    });
    (PipelineDocs::new(DocStore::new(store.clone())), store)
}

/// A store whose next writes to a key fail as `Transient`, before they change anything.
#[derive(Debug)]
pub(crate) struct FlakyStore {
    inner: ObjectStoreVersioned,
    failing: Mutex<Vec<(String, usize)>>,
}

impl FlakyStore {
    pub(crate) fn fail_writes(&self, key: &str, times: usize) {
        self.failing
            .lock()
            .expect("no test panics while holding the lock")
            .push((key.to_owned(), times));
    }

    fn fault(&self, key: &str) -> Result<(), StoreError> {
        let mut failing = self
            .failing
            .lock()
            .expect("no test panics while holding the lock");
        match failing.iter_mut().find(|(k, n)| k == key && *n > 0) {
            Some((_, n)) => {
                *n -= 1;
                Err(StoreError::Transient {
                    key: key.to_owned(),
                    reason: "injected".into(),
                })
            }
            None => Ok(()),
        }
    }
}

#[async_trait]
impl VersionedStore for FlakyStore {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
        self.inner.get(key).await
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.fault(key)?;
        self.inner.create(key, body).await
    }

    async fn update(
        &self,
        key: &str,
        expected: &ObjectVersion,
        body: Bytes,
    ) -> Result<ObjectVersion, StoreError> {
        self.fault(key)?;
        self.inner.update(key, expected, body).await
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.fault(key)?;
        self.inner.put(key, body).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix).await
    }
}

/// Crashes at the `n`th hook point `when` matches (1 = the first).
#[derive(Debug)]
pub(crate) struct CrashAfter {
    pub when: fn(&HookPoint) -> bool,
    pub n: usize,
    pub seen: AtomicUsize,
}

impl CrashAfter {
    pub fn new(when: fn(&HookPoint) -> bool, n: usize) -> Self {
        Self {
            when,
            n,
            seen: AtomicUsize::new(0),
        }
    }
}

impl Hooks for CrashAfter {
    fn at(&self, point: &HookPoint) -> HookAction {
        if (self.when)(point) && self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.n {
            HookAction::Crash
        } else {
            HookAction::Continue
        }
    }
}

/// Crashes at the first hook point `when` matches.
#[derive(Debug)]
pub(crate) struct CrashAt(pub fn(&HookPoint) -> bool);

impl Hooks for CrashAt {
    fn at(&self, point: &HookPoint) -> HookAction {
        if (self.0)(point) {
            HookAction::Crash
        } else {
            HookAction::Continue
        }
    }
}

/// A commit guard the test turns off.
#[derive(Debug, Default)]
pub(crate) struct Switch(pub AtomicBool);

impl CommitGuard for Switch {
    fn may_commit(&self) -> bool {
        !self.0.load(Ordering::SeqCst)
    }
}

/// Fast retries and ticks for tests.
pub(crate) fn fast() -> Tuning {
    let ms = |first, max| Backoff {
        first: Duration::from_millis(first),
        max: Duration::from_millis(max),
    };
    Tuning {
        source_backoff: ms(1, 4),
        sink_backoff: ms(1, 4),
        in_flight_backoff: ms(1, 4),
        contended_window: Duration::from_millis(200),
        tick: Duration::from_millis(5),
        augment_attempts: 3,
    }
}

/// Polls `cond` until it holds; panics after five seconds.
pub(crate) async fn wait_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// A source, a sink and the pipeline's state documents.
#[derive(Debug, Clone)]
pub(crate) struct World {
    pub source: FakeSource,
    pub sink: MemorySink,
    pub docs: PipelineDocs,
}

impl World {
    /// A source holding `tables` (each [`schema`]).
    pub fn new(tables: &[&str]) -> Self {
        Self::with(tables.iter().map(|t| schema(t)).collect())
    }

    pub fn with(tables: Vec<TableSchema>) -> Self {
        let (docs, _) = docs();
        Self {
            source: FakeSource::new(tables),
            sink: MemorySink::new(),
            docs,
        }
    }

    /// What Phase 0 leaves for a stream session over `tables`: the sink tables, the cached
    /// schemas and the checkpoint at the log's end.
    pub async fn started(&self, tables: &[&str]) {
        for t in tables {
            let s = self.source.schema(&TableRef::new("public", *t));
            self.sink
                .ensure_table(&TableSpec::new(&s, vec![]).expect("a spec"))
                .await
                .expect("a table");
            self.docs.save_table_schema(&s).await.expect("cached");
        }
        let SourcePosition::Pg(end) = self.source.wal_end() else {
            unreachable!("the fake speaks PostgreSQL")
        };
        self.docs
            .save_slot(&SlotDoc {
                slot_name: "p_slot".into(),
                confirmed_flush_lsn: end,
            })
            .await
            .expect("the checkpoint");
    }

    /// Requests a snapshot of `tables`, as Phase 0 does: `in_progress` with no cursor.
    pub async fn request(&self, tables: &[&str]) {
        for t in tables {
            self.docs
                .save_table_snapshot(&TableSnapshotDoc {
                    table: TableRef::new("public", *t),
                    status: SnapshotStatus::InProgress,
                    start_position: Some(self.source.wal_end()),
                    cursor: None,
                    row_count: None,
                    completed_at: None,
                })
                .await
                .expect("requested");
        }
    }

    /// A worker core over `tables` with `p`'s settings.
    pub fn core(
        &self,
        p: &Pipeline,
        tables: &[&str],
    ) -> (Core, mpsc::Sender<Command>, watch::Receiver<WorkerStatus>) {
        let (control, control_rx) = mpsc::channel(8);
        let (status, status_rx) = watch::channel(WorkerStatus::default());
        let core = Core {
            settings: Settings::new(p),
            source: Arc::new(self.source.clone()),
            sink: Arc::new(self.sink.clone()),
            docs: self.docs.clone(),
            owner_epoch: 1,
            guard: Arc::new(Unguarded),
            hooks: Arc::new(NoHooks),
            tuning: fast(),
            control: control_rx,
            status,
            tracked: tables.iter().map(|t| TableRef::new("public", *t)).collect(),
            dump_only: false,
            snapshot_started: BTreeMap::new(),
            drifted: BTreeSet::new(),
            rescan_failing: Streak::default(),
            requery_failing: BTreeMap::new(),
        };
        (core, control, status_rx)
    }

    /// The table's text `column` in the sink, in key order.
    pub fn text(&self, table: &str, column: &str) -> Vec<Option<String>> {
        let Some(b) = self.sink.content(&TableRef::new("public", table)) else {
            return vec![];
        };
        let values = b
            .column_by_name(column)
            .expect("the column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a text column");
        (0..values.len())
            .map(|i| values.is_valid(i).then(|| values.value(i).to_owned()))
            .collect()
    }

    /// The table's `id` values in the sink, in key order.
    pub fn ids(&self, table: &str) -> Vec<i64> {
        let Some(b) = self.sink.content(&TableRef::new("public", table)) else {
            return vec![];
        };
        let ids = b
            .column_by_name("id")
            .expect("an id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bigint ids");
        ids.values().to_vec()
    }
}

pub(crate) fn t(name: &str) -> TableRef {
    TableRef::new("public", name)
}
