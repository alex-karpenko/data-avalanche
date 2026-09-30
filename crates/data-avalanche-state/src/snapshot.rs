//! Snapshot documents under the pipeline's state location (Data-Flow §State Layout,
//! Snapshot_Resumability §2): one per table, `snapshots/<schema>.<table>.json`, and one per
//! on-demand job, `snapshots/jobs/<job_id>.json`.

use chrono::{DateTime, Utc};
use data_avalanche_core::{SourcePosition, TableRef};
use data_avalanche_storage::escape_segment;
use serde::{Deserialize, Serialize};

use crate::doc::{Document, StateError, Versioned};
use crate::pipeline::PipelineDocs;

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
    Queued,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobTable {
    pub table: TableRef,
    pub status: SnapshotStatus,
    pub rows_done: u64,
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
    pub error: Option<String>,
}

impl Document for SnapshotJobDoc {
    const KIND: &'static str = "snapshot job";
    const SCHEMA_VERSION: u64 = 1;
}

/// `snapshots/<schema>.<table>.json`. Each name is escaped as a key segment and its dots become
/// `%2E`, so the `.` between them is unambiguous (`a.b` + `c` ≠ `a` + `b.c`).
pub fn table_snapshot_key(table: &TableRef) -> String {
    let part = |s: &str| escape_segment(s).replace('.', "%2E");
    format!(
        "snapshots/{}.{}.json",
        part(&table.schema),
        part(&table.name)
    )
}

pub const JOBS_PREFIX: &str = "snapshots/jobs";

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

    /// Every job of this pipeline, oldest request first.
    ///
    /// # Errors
    /// A storage or decode failure (one bad job document fails the listing).
    pub async fn snapshot_jobs(&self) -> Result<Vec<Versioned<SnapshotJobDoc>>, StateError> {
        let mut jobs = vec![];
        for key in self.docs.store().list(JOBS_PREFIX).await? {
            if let Some(job) = self.docs.load::<SnapshotJobDoc>(&key).await? {
                jobs.push(job);
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
        SnapshotJobDoc {
            job_id: id.into(),
            status: JobStatus::Queued,
            tables: vec![JobTable {
                table: TableRef::new("public", "orders"),
                status: SnapshotStatus::None,
                rows_done: 0,
            }],
            requested_at: t(at),
            requested_by: "admin".into(),
            finished_at: None,
            error: None,
        }
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
}
