//! The owner's side of the on-demand snapshot jobs (Data-Flow §On-Demand Snapshot). The
//! worker's start takes a job's queued tables (`data-avalanche-pipeline`); a job asked through
//! the API comes with a restart under its id, which runs that start. The owner follows each
//! running table to its end, at its poll, from the table's snapshot document and the halts.

use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use data_avalanche_core::TableRef;
use data_avalanche_state::{
    HaltsDoc, JobTableStatus, PipelineDocs, SnapshotJobDoc, SnapshotStatus, StateError,
    TableSnapshotDoc,
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
    /// ended is followed no more.
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
        let mut ended = vec![];
        for id in ids.iter() {
            match follow(docs, id, &halts).await {
                Ok(Some(job)) => {
                    tracing::info!(pipeline, job = %id, status = ?job.status, "snapshot job ended");
                    ended.push(id.clone());
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

/// Brings job `id` up to date; `Some` once it has ended (or is gone).
async fn follow(
    docs: &PipelineDocs,
    id: &str,
    halts: &HaltsDoc,
) -> Result<Option<SnapshotJobDoc>, StateError> {
    let Some(v) = docs.snapshot_job(id).await? else {
        return Ok(None);
    };
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
        .update_snapshot_job(id, |j| {
            apply(j, &seen);
            j.settle(Utc::now());
        })
        .await?;
    Ok(job.filter(SnapshotJobDoc::is_finished))
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::HaltReason;
    use data_avalanche_state::{SnapshotCursor, TableHalt};

    use super::*;

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
