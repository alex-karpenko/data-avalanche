//! The owner's side of the on-demand snapshot jobs (Data-Flow §On-Demand Snapshot). The
//! worker's start takes a job's queued tables (`data-avalanche-pipeline`); a job asked through
//! the API comes with a restart under its id, which runs that start. The owner follows each
//! running table to its end, at its poll, from the table's snapshot document and the halts.

use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use data_avalanche_core::TableRef;
use data_avalanche_state::{
    HaltsDoc, JobTableStatus, PipelineDocs, SnapshotJobDoc, SnapshotStatus, StateError,
    TableSnapshotDoc, Versioned,
};

/// What a running job table's documents say of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Progress {
    Running { rows_done: u64 },
    Completed { rows: u64 },
    Failed(String),
}

/// A halt of the table fails it, with the halt's code; else its snapshot document says.
pub(crate) fn progress(
    table: &TableRef,
    halts: &HaltsDoc,
    doc: Option<&TableSnapshotDoc>,
) -> Progress {
    if let Some(h) = halts.tables.iter().find(|h| &h.table == table) {
        return Progress::Failed(format!("{}: {}", h.reason.code(), h.reason));
    }
    match doc {
        Some(d) if d.status == SnapshotStatus::InProgress => Progress::Running {
            rows_done: d.cursor.as_ref().map_or(0, |c| c.rows_done),
        },
        Some(d) if d.status == SnapshotStatus::Completed => Progress::Completed {
            rows: d.row_count.unwrap_or(0),
        },
        _ => Progress::Failed("its snapshot stopped before it completed".into()),
    }
}

fn apply(job: &mut SnapshotJobDoc, seen: &BTreeMap<TableRef, Progress>) {
    for t in &mut job.tables {
        if t.status != JobTableStatus::Running {
            continue;
        }
        match seen.get(&t.table) {
            Some(Progress::Running { rows_done }) => t.rows_done = *rows_done,
            Some(Progress::Completed { rows }) => {
                t.status = JobTableStatus::Completed;
                t.rows_done = *rows;
            }
            Some(Progress::Failed(why)) => {
                t.status = JobTableStatus::Failed;
                t.error = Some(why.clone());
            }
            None => {}
        }
    }
}

/// The unfinished jobs this owner follows; `None` until a listing succeeds.
#[derive(Debug, Default)]
pub(crate) struct Jobs(Option<BTreeSet<String>>);

impl Jobs {
    /// Lists the unfinished jobs: when the term starts, and after a restart taken (a job's
    /// restart). A failed listing is tried again at the next poll.
    pub async fn list(&mut self, docs: &PipelineDocs, pipeline: &str) {
        self.0 = match docs.unfinished_snapshot_jobs().await {
            Ok(jobs) => Some(jobs.into_iter().map(|j| j.job_id).collect()),
            Err(e) => {
                tracing::warn!(
                    pipeline,
                    "snapshot jobs unlisted, listed at the next poll: {e}"
                );
                None
            }
        };
    }

    /// One poll: each job's running tables, from their documents, into the job; a job that
    /// ended, or is gone, is followed no more.
    pub async fn poll(&mut self, docs: &PipelineDocs, pipeline: &str) {
        if self.0.is_none() {
            self.list(docs, pipeline).await;
        }
        let Some(ids) = self.0.as_mut() else {
            return;
        };
        if ids.is_empty() {
            return;
        }
        // The jobs before the halts: the worker's start clears a table's halt, then records it
        // running, so a table read running here had its halt cleared before the halts are read.
        let mut jobs = vec![];
        let mut ended = vec![];
        for id in ids.iter() {
            match docs.snapshot_job(id).await {
                Ok(Some(v)) => jobs.push(v),
                Ok(None) => {
                    tracing::info!(pipeline, job = %id, "snapshot job gone");
                    ended.push(id.clone());
                }
                Err(e) => {
                    tracing::warn!(pipeline, job = %id, "snapshot job unfollowed this poll: {e}");
                }
            }
        }
        for id in ended.drain(..) {
            ids.remove(&id);
        }
        if jobs.is_empty() {
            return;
        }
        let halts = match docs.halts().await {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(
                    pipeline,
                    "halts unread, snapshot jobs followed at the next poll: {e}"
                );
                return;
            }
        };
        for v in jobs {
            let id = v.doc.job_id.clone();
            match follow(docs, v, &halts).await {
                Ok(Some(job)) => {
                    tracing::info!(pipeline, job = %id, status = ?job.status, "snapshot job ended");
                    ended.push(id);
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(pipeline, job = %id, "snapshot job unfollowed this poll: {e}");
                }
            }
        }
        for id in ended {
            ids.remove(&id);
        }
    }
}

/// Brings job `v`, as just read, up to date; `Some` once it has ended.
async fn follow(
    docs: &PipelineDocs,
    v: Versioned<SnapshotJobDoc>,
    halts: &HaltsDoc,
) -> Result<Option<SnapshotJobDoc>, StateError> {
    if v.doc.is_finished() {
        return Ok(Some(v.doc));
    }
    let mut seen = BTreeMap::new();
    for t in v
        .doc
        .tables
        .iter()
        .filter(|t| t.status == JobTableStatus::Running)
    {
        let doc = docs.table_snapshot(&t.table).await?.map(|v| v.doc);
        seen.insert(t.table.clone(), progress(&t.table, halts, doc.as_ref()));
    }
    let mut next = v.doc.clone();
    apply(&mut next, &seen);
    next.settle(Utc::now());
    if next == v.doc {
        return Ok(None);
    }
    let job = docs
        .update_snapshot_job(&v.doc.job_id, |j| {
            apply(j, &seen);
            j.settle(Utc::now());
        })
        .await?;
    Ok(job.filter(SnapshotJobDoc::is_finished))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use data_avalanche_core::HaltReason;
    use data_avalanche_state::{DocStore, HALTS_KEY, JobStatus, SnapshotCursor, TableHalt};
    use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
    use object_store::memory::InMemory;

    use super::*;

    /// A store that, once armed, does the worker's start right after its next `halts.json`
    /// read: it clears the table's halt, then records the table running.
    #[derive(Debug)]
    struct Starting {
        inner: Arc<dyn VersionedStore>,
        armed: Mutex<Option<(String, TableRef)>>,
    }

    #[async_trait]
    impl VersionedStore for Starting {
        async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
            let read = self.inner.get(key).await;
            let armed = if key == HALTS_KEY {
                self.armed.lock().unwrap().take()
            } else {
                None
            };
            if let Some((job, table)) = armed {
                let worker = PipelineDocs::new(DocStore::new(Arc::clone(&self.inner)));
                worker.clear_halt(&table).await.unwrap();
                worker
                    .update_snapshot_job(&job, |j| {
                        for t in &mut j.tables {
                            t.status = JobTableStatus::Running;
                        }
                        j.settle(Utc::now());
                    })
                    .await
                    .unwrap();
            }
            read
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
            self.inner.update(key, expected, body).await
        }
        async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
            self.inner.put(key, body).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            self.inner.list(prefix).await
        }
    }

    fn starting() -> (Arc<Starting>, PipelineDocs) {
        let inner: Arc<dyn VersionedStore> =
            Arc::new(ObjectStoreVersioned::new(Arc::new(InMemory::new()), "p").unwrap());
        let store = Arc::new(Starting {
            inner,
            armed: Mutex::new(None),
        });
        let docs = PipelineDocs::new(DocStore::new(Arc::clone(&store) as Arc<dyn VersionedStore>));
        (store, docs)
    }

    #[tokio::test]
    async fn a_halt_the_job_s_start_cleared_doesn_t_fail_the_job() {
        let (store, docs) = starting();
        let a = TableRef::new("public", "a");
        docs.record_halt(&TableHalt {
            table: a.clone(),
            reason: HaltReason::TableDropped,
            at: "pg:0/10".parse().unwrap(),
        })
        .await
        .unwrap();
        let mut running = doc(SnapshotStatus::InProgress);
        running.start_position = Some("pg:0/20".parse().unwrap());
        docs.save_table_snapshot(&running).await.unwrap();
        let job = SnapshotJobDoc::new("j1".into(), vec![a.clone()], "api".into(), Utc::now());
        docs.create_snapshot_job(&job).await.unwrap();
        let mut jobs = Jobs::default();
        jobs.list(&docs, "p").await;

        // The worker's start clears the halt, then records the table running, while the poll
        // reads.
        *store.armed.lock().unwrap() = Some(("j1".into(), a.clone()));
        jobs.poll(&docs, "p").await;
        jobs.poll(&docs, "p").await;

        let job = docs.snapshot_job("j1").await.unwrap().unwrap().doc;
        assert_eq!(job.status, JobStatus::Running, "{job:?}");
        assert_eq!(job.tables[0].status, JobTableStatus::Running, "{job:?}");
        assert_eq!(job.tables[0].error, None);
        assert!(docs.halts().await.unwrap().tables.is_empty());
    }

    #[tokio::test]
    async fn a_job_that_is_gone_is_followed_no_more() {
        let (_, docs) = starting();
        let mut jobs = Jobs(Some(BTreeSet::from(["gone".to_owned()])));
        jobs.poll(&docs, "p").await;
        assert_eq!(jobs.0, Some(BTreeSet::new()));
    }

    fn doc(status: SnapshotStatus) -> TableSnapshotDoc {
        TableSnapshotDoc {
            table: TableRef::new("public", "a"),
            status,
            start_position: None,
            cursor: None,
            row_count: None,
            completed_at: None,
        }
    }

    #[test]
    fn a_running_table_s_progress_is_its_documents() {
        let a = TableRef::new("public", "a");
        let none = HaltsDoc::default();
        let mut running = doc(SnapshotStatus::InProgress);
        assert_eq!(
            progress(&a, &none, Some(&running)),
            Progress::Running { rows_done: 0 }
        );
        running.cursor = Some(SnapshotCursor {
            key: vec![],
            chunk_size: 10,
            rows_done: 20,
            collation: None,
        });
        assert_eq!(
            progress(&a, &none, Some(&running)),
            Progress::Running { rows_done: 20 }
        );
        let mut done = doc(SnapshotStatus::Completed);
        done.row_count = Some(25);
        assert_eq!(
            progress(&a, &none, Some(&done)),
            Progress::Completed { rows: 25 }
        );
        for stopped in [Some(doc(SnapshotStatus::None)), None] {
            assert_eq!(
                progress(&a, &none, stopped.as_ref()),
                Progress::Failed("its snapshot stopped before it completed".into())
            );
        }
        let halted = HaltsDoc {
            tables: vec![TableHalt {
                table: a.clone(),
                reason: HaltReason::SnapshotTimeout { limit_secs: 60 },
                at: "pg:0/10".parse().unwrap(),
            }],
        };
        let Progress::Failed(why) = progress(&a, &halted, Some(&running)) else {
            panic!("a halt fails the table")
        };
        assert!(why.starts_with("SNAPSHOT_TIMEOUT: "), "{why}");
    }
}
