//! Fixtures for the crate's unit tests.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ColumnSchema, ColumnValue, ColumnValueType as V, Op, Row, SourcePosition, SourceSeq,
    TableRef, TableSchema, TransactionBoundary,
};
use data_avalanche_state::{DocStore, PipelineDocs};
use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
use object_store::memory::InMemory;

use crate::{Accumulator, CommitGuard, FlushPolicy, HookAction, HookPoint, Hooks, Lineage};

/// A PostgreSQL → Delta pipeline `p` reading `public.*`, with more `tables` keys (YAML lines
/// indented six spaces), more `sink` and `postgres` keys (YAML flow entries).
pub(crate) fn pipeline(tables: &str, sink: &str, postgres: &str) -> Pipeline {
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
      postgres: {{ publication: p_pub, slot: p_slot{postgres} }}
    tables:
      include: [public.*]{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}}{sink} }}
"#,
        postgres = sep(postgres),
        sink = sep(sink),
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
