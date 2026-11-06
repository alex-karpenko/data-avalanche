//! Snapshot documents under the pipeline's state location (Data-Flow §State Layout,
//! Snapshot_Resumability §2): one per table, `snapshots/<schema>.<table>.json`, and one per
//! on-demand job, `snapshots/jobs/<job_id>.json`.

use chrono::{DateTime, Utc};
use data_avalanche_core::{SourcePosition, TableRef};
use data_avalanche_storage::escape_segment;
use serde::{Deserialize, Serialize};

use crate::doc::{Document, StateError, Versioned};
use crate::pipeline::PipelineDocs;
use crate::tables::table_key;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStatus {
    None,
    InProgress,
    Completed,
    Broken,
}

/// One identifier column of the last row of the last committed chunk. `val` is the value as JSON
/// (number, string, bool); the source connector converts it back to a typed value (M5/M7).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorKey {
    pub col: String,
    pub val: serde_json::Value,
    /// The source column type (`int8`, `uuid`, `varchar`, …).
    #[serde(rename = "type")]
    pub ty: String,
}

/// Where a chunked snapshot resumes (Snapshot_Resumability §2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotCursor {
    /// The identifier tuple, in key order.
    pub key: Vec<CursorKey>,
    pub chunk_size: u64,
    pub rows_done: u64,
    /// The binary collation the keyset ordering is pinned to (`C`, `utf8mb4_bin`); `None` when no
    /// key column is text.
    pub collation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSnapshotDoc {
    /// The table this document is for (its key escapes the names; this keeps them verbatim).
    pub table: TableRef,
    pub status: SnapshotStatus,
    /// The stream position when the snapshot started (PG: the slot's start LSN).
    pub start_position: Option<SourcePosition>,
    /// `None` before the first chunk and after completion.
    pub cursor: Option<SnapshotCursor>,
    pub row_count: Option<u64>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl Document for TableSnapshotDoc {
    const KIND: &'static str = "table snapshot";
    const SCHEMA_VERSION: u64 = 1;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// No table has started yet.
    Queued,
    Running,
    /// Every table completed.
    Completed,
    /// Every table ended, and one or more failed.
    Failed,
}

/// Where one requested table of a job is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobTableStatus {
    /// The owner hasn't started it yet.
    Queued,
    /// Its snapshot runs: its document is `in_progress`.
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobTable {
    pub table: TableRef,
    pub status: JobTableStatus,
    pub rows_done: u64,
    /// Why it failed.
    pub error: Option<String>,
}

/// An on-demand snapshot job (Data-Flow §On-Demand Snapshot); survives restart and failover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotJobDoc {
    pub job_id: String,
    pub status: JobStatus,
    pub tables: Vec<JobTable>,
    pub requested_at: DateTime<Utc>,
    pub requested_by: String,
    pub finished_at: Option<DateTime<Utc>>,
    /// Which tables failed, once the job ended.
    pub error: Option<String>,
    /// Each table's sink table is emptied before its first chunk (DA-SNAP-15, Q-M12-7). Written
    /// only when set: a v0.1.0 reader refuses unknown fields.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncate: bool,
}

impl Document for SnapshotJobDoc {
    const KIND: &'static str = "snapshot job";
    const SCHEMA_VERSION: u64 = 1;
}

impl SnapshotJobDoc {
    /// A job just asked for: it and every table queued.
    pub fn new(
        job_id: String,
        tables: Vec<TableRef>,
        requested_by: String,
        requested_at: DateTime<Utc>,
    ) -> Self {
        Self {
            job_id,
            status: JobStatus::Queued,
            tables: tables
                .into_iter()
                .map(|table| JobTable {
                    table,
                    status: JobTableStatus::Queued,
                    rows_done: 0,
                    error: None,
                })
                .collect(),
            requested_at,
            requested_by,
            finished_at: None,
            error: None,
            truncate: false,
        }
    }

    /// The job empties each table's sink table before its first chunk.
    #[must_use]
    pub fn truncating(mut self) -> Self {
        self.truncate = true;
        self
    }

    /// Completed or failed: the job has ended, and [`Self::settle`] leaves it as it is.
    pub fn is_finished(&self) -> bool {
        matches!(self.status, JobStatus::Completed | JobStatus::Failed)
    }

    /// Sets the job's status from its tables': queued while every table is, running while one
    /// is queued or running, then completed or failed, at `now`. A finished job stays as it
    /// ended. True when it changed.
    pub fn settle(&mut self, now: DateTime<Utc>) -> bool {
        if self.is_finished() {
            return false;
        }
        let is = |s: JobTableStatus| move |t: &JobTable| t.status == s;
        let status = if self.tables.iter().all(is(JobTableStatus::Queued)) {
            JobStatus::Queued
        } else if self
            .tables
            .iter()
            .any(|t| matches!(t.status, JobTableStatus::Queued | JobTableStatus::Running))
        {
            JobStatus::Running
        } else if self.tables.iter().all(is(JobTableStatus::Completed)) {
            JobStatus::Completed
        } else {
            JobStatus::Failed
        };
        if status == self.status {
            return false;
        }
        self.status = status;
        if self.is_finished() {
            self.finished_at = Some(now);
            let failed: Vec<String> = self
                .tables
                .iter()
                .filter(|t| t.status == JobTableStatus::Failed)
                .map(|t| match &t.error {
                    Some(e) => format!("{} ({e})", t.table),
                    None => t.table.to_string(),
                })
                .collect();
            if !failed.is_empty() {
                self.error = Some(format!(
                    "{} of {} tables failed: {}",
                    failed.len(),
                    self.tables.len(),
                    failed.join(", ")
                ));
            }
        }
        true
    }
}

/// `snapshots/<schema>.<table>.json`. Each name is escaped as a key segment and its dots become
/// `%2E`, so the `.` between them is unambiguous (`a.b` + `c` ≠ `a` + `b.c`).
pub fn table_snapshot_key(table: &TableRef) -> String {
    table_key("snapshots", table)
}

pub const JOBS_PREFIX: &str = "snapshots/jobs";

/// How many ended jobs a pipeline keeps; the owner deletes the older ones when a job ends.
pub const KEPT_ENDED_JOBS: usize = 100;

/// `snapshots/jobs/<job_id>.json`.
pub fn job_key(job_id: &str) -> String {
    format!("{JOBS_PREFIX}/{}.json", escape_segment(job_id))
}

impl PipelineDocs {
    /// # Errors
    /// A storage or decode failure; `Corrupt` when the stored document names another table.
    pub async fn table_snapshot(
        &self,
        table: &TableRef,
    ) -> Result<Option<Versioned<TableSnapshotDoc>>, StateError> {
        let key = table_snapshot_key(table);
        let found = self.docs.load::<TableSnapshotDoc>(&key).await?;
        if let Some(v) = &found
            && &v.doc.table != table
        {
            return Err(StateError::Corrupt {
                key,
                kind: TableSnapshotDoc::KIND,
                reason: format!("it is for table {}, not {table}", v.doc.table),
            });
        }
        Ok(found)
    }

    /// Checkpoints a table's snapshot state (once per chunk, Snapshot_Resumability §5).
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn save_table_snapshot(
        &self,
        doc: &TableSnapshotDoc,
    ) -> Result<Versioned<TableSnapshotDoc>, StateError> {
        let key = table_snapshot_key(&doc.table);
        self.docs
            .modify(&key, |_: Option<TableSnapshotDoc>| Ok(doc.clone()))
            .await
    }

    /// Persists a new job before the API responds.
    ///
    /// # Errors
    /// `Store(Conflict)` when a job with this ID exists, or a storage failure.
    pub async fn create_snapshot_job(
        &self,
        doc: &SnapshotJobDoc,
    ) -> Result<Versioned<SnapshotJobDoc>, StateError> {
        let version = self.docs.create(&job_key(&doc.job_id), doc).await?;
        Ok(Versioned {
            doc: doc.clone(),
            version,
        })
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn save_snapshot_job(
        &self,
        doc: &SnapshotJobDoc,
    ) -> Result<Versioned<SnapshotJobDoc>, StateError> {
        self.docs
            .modify(&job_key(&doc.job_id), |_: Option<SnapshotJobDoc>| {
                Ok(doc.clone())
            })
            .await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn snapshot_job(
        &self,
        job_id: &str,
    ) -> Result<Option<Versioned<SnapshotJobDoc>>, StateError> {
        self.docs.load(&job_key(job_id)).await
    }

    /// Changes job `job_id` with `f`, read-modify-write; `None` when there is no such job.
    ///
    /// # Errors
    /// A storage or decode failure, or the last race error once the attempts run out.
    pub async fn update_snapshot_job<F>(
        &self,
        job_id: &str,
        mut f: F,
    ) -> Result<Option<SnapshotJobDoc>, StateError>
    where
        F: FnMut(&mut SnapshotJobDoc) + Send,
    {
        let key = job_key(job_id);
        if self.docs.load::<SnapshotJobDoc>(&key).await?.is_none() {
            return Ok(None);
        }
        let v = self
            .docs
            .modify(&key, |cur: Option<SnapshotJobDoc>| {
                let Some(mut doc) = cur else {
                    return Err(StateError::Corrupt {
                        key: key.clone(),
                        kind: SnapshotJobDoc::KIND,
                        reason: "deleted while it was updated".into(),
                    });
                };
                f(&mut doc);
                Ok(doc)
            })
            .await?;
        Ok(Some(v.doc))
    }

    /// Deletes the ended jobs past the newest `keep`, newest by `finished_at` (DA-SNAP-16); an
    /// unfinished job is never deleted. The number deleted.
    ///
    /// # Errors
    /// A storage failure.
    pub async fn prune_snapshot_jobs(&self, keep: usize) -> Result<usize, StateError> {
        let mut ended: Vec<SnapshotJobDoc> = self
            .snapshot_jobs()
            .await?
            .into_iter()
            .map(|v| v.doc)
            .filter(SnapshotJobDoc::is_finished)
            .collect();
        ended.sort_by(|a, b| (b.finished_at, &b.job_id).cmp(&(a.finished_at, &a.job_id)));
        let mut deleted = 0;
        for job in ended.iter().skip(keep) {
            self.docs.store().delete(&job_key(&job.job_id)).await?;
            deleted += 1;
        }
        Ok(deleted)
    }

    /// The jobs not finished yet, oldest request first.
    ///
    /// # Errors
    /// As [`Self::snapshot_jobs`].
    pub async fn unfinished_snapshot_jobs(&self) -> Result<Vec<SnapshotJobDoc>, StateError> {
        Ok(self
            .snapshot_jobs()
            .await?
            .into_iter()
            .map(|v| v.doc)
            .filter(|j| !j.is_finished())
            .collect())
    }

    /// Every job of this pipeline, oldest request first. A job document this build can't decode
    /// (another release's, or a damaged one) is skipped with a warning (DA-SNAP-20).
    ///
    /// # Errors
    /// A storage failure.
    pub async fn snapshot_jobs(&self) -> Result<Vec<Versioned<SnapshotJobDoc>>, StateError> {
        let mut jobs = vec![];
        for key in self.docs.store().list(JOBS_PREFIX).await? {
            match self.docs.load::<SnapshotJobDoc>(&key).await {
                Ok(Some(job)) => jobs.push(job),
                Ok(None) => {}
                Err(e @ (StateError::Corrupt { .. } | StateError::UnsupportedVersion { .. })) => {
                    tracing::warn!("snapshot job {key} skipped: {e}");
                }
                Err(e) => return Err(e),
            }
        }
        jobs.sort_by(|a, b| {
            (a.doc.requested_at, &a.doc.job_id).cmp(&(b.doc.requested_at, &b.doc.job_id))
        });
        Ok(jobs)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;
    use serde_json::json;

    use super::*;
    use crate::doc::DocStore;

    fn pipeline() -> PipelineDocs {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/orders").unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(store)))
    }

    fn t(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, s).unwrap()
    }

    fn in_progress(table: TableRef) -> TableSnapshotDoc {
        TableSnapshotDoc {
            table,
            status: SnapshotStatus::InProgress,
            start_position: Some("pg:0/1A2B3C4".parse().unwrap()),
            cursor: Some(SnapshotCursor {
                key: vec![
                    CursorKey {
                        col: "tenant_id".into(),
                        val: json!(42),
                        ty: "int8".into(),
                    },
                    CursorKey {
                        col: "id".into(),
                        val: json!("9f1c"),
                        ty: "uuid".into(),
                    },
                ],
                chunk_size: 10_000,
                rows_done: 4_530_000,
                collation: None,
            }),
            row_count: None,
            completed_at: None,
        }
    }

    #[test]
    fn table_snapshot_keys() {
        let key = |s: &str, n: &str| table_snapshot_key(&TableRef::new(s, n));
        assert_eq!(key("public", "orders"), "snapshots/public.orders.json");
        assert_eq!(key("a.b", "c"), "snapshots/a%2Eb.c.json");
        assert_eq!(key("a", "b.c"), "snapshots/a.b%2Ec.json");
        assert_eq!(key("sales", "a/b 50%"), "snapshots/sales.a%2Fb 50%25.json");
        assert_eq!(key("продажи", "заказы"), "snapshots/продажи.заказы.json");
        assert_eq!(job_key("0199-abc"), "snapshots/jobs/0199-abc.json");
    }

    #[test]
    fn table_snapshot_json_shape() {
        let doc = in_progress(TableRef::new("public", "orders"));
        assert_eq!(
            String::from_utf8(crate::doc::encode(&doc).to_vec()).unwrap(),
            r#"{
  "version": 1,
  "table": {
    "schema": "public",
    "name": "orders"
  },
  "status": "in_progress",
  "start_position": "pg:0/1A2B3C4",
  "cursor": {
    "key": [
      {
        "col": "tenant_id",
        "val": 42,
        "type": "int8"
      },
      {
        "col": "id",
        "val": "9f1c",
        "type": "uuid"
      }
    ],
    "chunk_size": 10000,
    "rows_done": 4530000,
    "collation": null
  },
  "row_count": null,
  "completed_at": null
}"#
        );
    }

    #[tokio::test]
    async fn table_snapshot_checkpoints_then_completes() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        assert!(p.table_snapshot(&orders).await.unwrap().is_none());
        let mut doc = in_progress(orders.clone());
        p.save_table_snapshot(&doc).await.unwrap();
        assert_eq!(p.table_snapshot(&orders).await.unwrap().unwrap().doc, doc);
        doc.status = SnapshotStatus::Completed;
        doc.cursor = None;
        doc.row_count = Some(4_531_234);
        doc.completed_at = Some(t(9));
        p.save_table_snapshot(&doc).await.unwrap();
        assert_eq!(p.table_snapshot(&orders).await.unwrap().unwrap().doc, doc);
    }

    #[tokio::test]
    async fn dotted_names_do_not_collide() {
        let p = pipeline();
        let (x, y) = (TableRef::new("a.b", "c"), TableRef::new("a", "b.c"));
        p.save_table_snapshot(&in_progress(x.clone()))
            .await
            .unwrap();
        assert!(p.table_snapshot(&y).await.unwrap().is_none());
        p.save_table_snapshot(&in_progress(y.clone()))
            .await
            .unwrap();
        assert_eq!(p.table_snapshot(&x).await.unwrap().unwrap().doc.table, x);
        assert_eq!(p.table_snapshot(&y).await.unwrap().unwrap().doc.table, y);
    }

    #[tokio::test]
    async fn a_document_for_another_table_is_corrupt() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        let body = crate::doc::encode(&in_progress(TableRef::new("public", "invoices")));
        p.docs
            .store()
            .put(&table_snapshot_key(&orders), body)
            .await
            .unwrap();
        let e = p.table_snapshot(&orders).await.unwrap_err();
        assert!(
            e.to_string()
                .contains("it is for table public.invoices, not public.orders"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn an_unknown_status_is_corrupt() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        let text =
            String::from_utf8(crate::doc::encode(&in_progress(orders.clone())).to_vec()).unwrap();
        let bad = text.replace("in_progress", "paused");
        p.docs
            .store()
            .put(&table_snapshot_key(&orders), Bytes::from(bad))
            .await
            .unwrap();
        assert!(matches!(
            p.table_snapshot(&orders).await.unwrap_err(),
            StateError::Corrupt { .. }
        ));
    }

    fn job(id: &str, at: u32) -> SnapshotJobDoc {
        SnapshotJobDoc::new(
            id.into(),
            vec![TableRef::new("public", "orders")],
            "admin".into(),
            t(at),
        )
    }

    #[tokio::test]
    async fn jobs_persist_and_list_in_request_order() {
        let p = pipeline();
        assert!(p.snapshot_jobs().await.unwrap().is_empty());
        p.create_snapshot_job(&job("b-job", 1)).await.unwrap();
        p.create_snapshot_job(&job("a-job", 2)).await.unwrap();
        assert!(
            p.create_snapshot_job(&job("a-job", 3))
                .await
                .unwrap_err()
                .is_conflict(),
            "job IDs are unique"
        );
        let mut done = job("b-job", 1);
        done.status = JobStatus::Failed;
        done.finished_at = Some(t(5));
        done.error = Some("table public.orders not found".into());
        p.save_snapshot_job(&done).await.unwrap();
        let listed: Vec<_> = p
            .snapshot_jobs()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.doc)
            .collect();
        assert_eq!(listed, vec![done.clone(), job("a-job", 2)]);
        assert_eq!(p.snapshot_job("b-job").await.unwrap().unwrap().doc, done);
        assert!(p.snapshot_job("nope").await.unwrap().is_none());
        p.save_table_snapshot(&in_progress(TableRef::new("public", "jobs")))
            .await
            .unwrap();
        assert_eq!(
            p.snapshot_jobs().await.unwrap().len(),
            2,
            "a table named `jobs` isn't a job"
        );
    }

    fn two_tables() -> SnapshotJobDoc {
        SnapshotJobDoc::new(
            "orders~0199".into(),
            vec![
                TableRef::new("public", "orders"),
                TableRef::new("public", "items"),
            ],
            "ops".into(),
            t(0),
        )
    }

    #[test]
    fn a_job_json_shape() {
        let mut doc = two_tables();
        doc.tables[0].status = JobTableStatus::Running;
        doc.tables[0].rows_done = 20_000;
        doc.tables[1].status = JobTableStatus::Failed;
        doc.tables[1].error = Some("no such table".into());
        assert!(doc.settle(t(1)));
        assert_eq!(
            String::from_utf8(crate::doc::encode(&doc).to_vec()).unwrap(),
            r#"{
  "version": 1,
  "job_id": "orders~0199",
  "status": "running",
  "tables": [
    {
      "table": {
        "schema": "public",
        "name": "orders"
      },
      "status": "running",
      "rows_done": 20000,
      "error": null
    },
    {
      "table": {
        "schema": "public",
        "name": "items"
      },
      "status": "failed",
      "rows_done": 0,
      "error": "no such table"
    }
  ],
  "requested_at": "2026-09-28T10:00:00Z",
  "requested_by": "ops",
  "finished_at": null,
  "error": null
}"#
        );
        // A job that keeps the rows writes no `truncate` key: a v0.1.0 reader refuses unknown
        // fields, so it still reads the job. It reads back as one that keeps the rows.
        let kept = String::from_utf8(crate::doc::encode(&doc).to_vec()).unwrap();
        assert!(!kept.contains("truncate"), "{kept}");
        let read: SnapshotJobDoc = crate::doc::decode("j", kept.as_bytes()).unwrap();
        assert!(!read.truncate);
        // A job that empties its tables writes `truncate: true`, and reads back so.
        let emptying = doc.clone().truncating();
        let text = String::from_utf8(crate::doc::encode(&emptying).to_vec()).unwrap();
        assert!(text.contains("\"truncate\": true"), "{text}");
        let read: SnapshotJobDoc = crate::doc::decode("j", text.as_bytes()).unwrap();
        assert!(read.truncate);
    }

    /// A job is queued until a table starts, running while one is queued or running, then
    /// completed when every table completed and failed otherwise, naming the failed tables.
    #[test]
    fn a_job_settles_once_every_table_ended() {
        let mut doc = two_tables();
        assert!(!doc.settle(t(1)), "every table queued: still queued");
        assert_eq!(doc.status, JobStatus::Queued);
        doc.tables[0].status = JobTableStatus::Running;
        assert!(doc.settle(t(1)));
        assert_eq!(doc.status, JobStatus::Running);
        assert!(!doc.is_finished());
        doc.tables[0].status = JobTableStatus::Completed;
        assert!(!doc.settle(t(2)), "a table still queued: still running");
        doc.tables[1].status = JobTableStatus::Completed;
        assert!(doc.settle(t(3)));
        assert_eq!(doc.status, JobStatus::Completed);
        assert_eq!(doc.finished_at, Some(t(3)));
        assert_eq!(doc.error, None);
        assert!(doc.is_finished());
        assert!(!doc.settle(t(4)), "a finished job stays as it ended");
        assert_eq!(doc.finished_at, Some(t(3)));

        let mut doc = two_tables();
        doc.tables[0].status = JobTableStatus::Completed;
        doc.tables[1].status = JobTableStatus::Failed;
        doc.tables[1].error = Some("TABLE_DROPPED".into());
        assert!(doc.settle(t(5)));
        assert_eq!(doc.status, JobStatus::Failed);
        assert_eq!(doc.finished_at, Some(t(5)));
        assert_eq!(
            doc.error.as_deref(),
            Some("1 of 2 tables failed: public.items (TABLE_DROPPED)")
        );

        let mut doc = two_tables();
        doc.tables[0].status = JobTableStatus::Failed;
        doc.tables[1].status = JobTableStatus::Running;
        assert!(doc.settle(t(6)));
        assert_eq!(
            doc.status,
            JobStatus::Running,
            "a table failed, one running"
        );
        assert_eq!((doc.finished_at, doc.error.as_deref()), (None, None));
        doc.tables[1].status = JobTableStatus::Failed;
        doc.tables[1].error = Some("TABLE_DROPPED".into());
        assert!(doc.settle(t(7)));
        assert_eq!(doc.status, JobStatus::Failed, "every table failed");
        assert_eq!(
            doc.error.as_deref(),
            Some("2 of 2 tables failed: public.orders, public.items (TABLE_DROPPED)"),
            "a table failed with no error is named alone"
        );

        let mut doc = two_tables();
        doc.tables.clear();
        assert!(!doc.settle(t(8)), "no tables: queued, as it was");
        assert_eq!(doc.status, JobStatus::Queued);
    }

    #[tokio::test]
    async fn the_unfinished_jobs_leave_out_the_ended_ones() {
        let p = pipeline();
        p.create_snapshot_job(&job("a", 1)).await.unwrap();
        p.create_snapshot_job(&job("b", 2)).await.unwrap();
        p.create_snapshot_job(&job("c", 3)).await.unwrap();
        let ended = p
            .update_snapshot_job("b", |j| {
                j.tables[0].status = JobTableStatus::Completed;
                j.settle(t(4));
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ended.status, JobStatus::Completed);
        assert!(
            p.update_snapshot_job("nope", |_| {})
                .await
                .unwrap()
                .is_none(),
            "no such job"
        );
        let ids: Vec<String> = p
            .unfinished_snapshot_jobs()
            .await
            .unwrap()
            .into_iter()
            .map(|j| j.job_id)
            .collect();
        assert_eq!(ids, ["a", "c"]);
    }

    fn ended(id: &str, at: u32, finished: u32) -> SnapshotJobDoc {
        let mut j = job(id, at);
        j.status = JobStatus::Completed;
        j.finished_at = Some(t(finished));
        j
    }

    #[tokio::test]
    async fn pruning_keeps_the_newest_ended_jobs_and_every_unfinished_one() {
        let p = pipeline();
        p.create_snapshot_job(&ended("old", 1, 9)).await.unwrap();
        p.create_snapshot_job(&ended("newer", 2, 10)).await.unwrap();
        p.create_snapshot_job(&ended("newest", 3, 11))
            .await
            .unwrap();
        p.create_snapshot_job(&job("queued", 0)).await.unwrap();
        assert_eq!(p.prune_snapshot_jobs(2).await.unwrap(), 1);
        let ids: Vec<String> = p
            .snapshot_jobs()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.doc.job_id)
            .collect();
        assert_eq!(
            ids,
            ["queued", "newer", "newest"],
            "finished first is pruned first"
        );
        assert!(p.snapshot_job("old").await.unwrap().is_none());
        assert_eq!(p.prune_snapshot_jobs(2).await.unwrap(), 0);
        assert_eq!(p.prune_snapshot_jobs(0).await.unwrap(), 2);
        assert_eq!(
            p.snapshot_jobs().await.unwrap().len(),
            1,
            "a queued job stays"
        );
    }

    #[tokio::test]
    async fn an_undecodable_job_is_skipped_by_the_listing() {
        let p = pipeline();
        p.create_snapshot_job(&job("good", 1)).await.unwrap();
        let text = String::from_utf8(crate::doc::encode(&job("bad", 2)).to_vec()).unwrap();
        let bad = text.replacen('{', "{\"from_a_newer_release\": 1, ", 1);
        p.docs
            .store()
            .put(&job_key("bad"), Bytes::from(bad))
            .await
            .unwrap();
        assert!(matches!(
            p.snapshot_job("bad").await.unwrap_err(),
            StateError::Corrupt { .. }
        ));
        let ids: Vec<String> = p
            .snapshot_jobs()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.doc.job_id)
            .collect();
        assert_eq!(ids, ["good"]);
        assert_eq!(p.unfinished_snapshot_jobs().await.unwrap().len(), 1);
    }
}
