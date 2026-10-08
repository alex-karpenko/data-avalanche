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
