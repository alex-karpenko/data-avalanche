//! Phase 0's part of the on-demand snapshot jobs (Data-Flow §On-Demand Snapshot). A job's
//! queued tables are asked for: each start snapshots them from their first row, a halted one
//! with its halt cleared and its schema baselined to the catalog's. A table whose job snapshot
//! runs keeps running whatever its snapshot mode. Phase 0 records what it did with each asked
//! table; the owner's controller follows a running one to its end.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use data_avalanche_core::TableRef;
use data_avalanche_state::{JobTable, JobTableStatus, PipelineDocs, SnapshotJobDoc, StateError};

/// The unfinished jobs, as a start read them.
#[derive(Debug, Default)]
pub(crate) struct Jobs {
    jobs: Vec<SnapshotJobDoc>,
}

/// What a start did with the job tables, for [`Jobs::record`].
#[derive(Debug, Default)]
pub(crate) struct Taken {
    /// The asked tables whose copy began.
    pub started: BTreeSet<TableRef>,
    /// The asked or running tables that failed, with their reason.
    pub failed: BTreeMap<TableRef, String>,
    /// The source's row estimates of the started tables, where it has one.
    pub estimates: BTreeMap<TableRef, u64>,
    /// The started tables whose recorded halt the start cleared, with the halt's code.
    pub from_halt: BTreeMap<TableRef, String>,
    /// A cancelled job's running tables whose copy completed before a start took the cancel,
    /// with their rows: they end completed, not cancelled.
    pub completed: BTreeMap<TableRef, u64>,
    /// A cancelled job's stopped copies, with the rows their snapshot documents' cursors count:
    /// a table ends cancelled with the larger of that and the rows it recorded.
    pub cancelled_rows: BTreeMap<TableRef, u64>,
}

/// A cancelled job's running copy of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Cancelling {
    pub job: String,
    /// The job's start cleared a halt of the table: stopped, the table halts again.
    pub from_halt: bool,
    /// The job empties its sink tables first: a copy it began leaves the table holding part of
    /// its rows, so a stopped copy halts it too (S-M13c.2).
    pub truncate: bool,
    /// The rows the table's snapshot document counts, filled in by the start that reads it: the
    /// follower's last poll may predate them.
    pub rows_done: u64,
}

impl Jobs {
    pub async fn load(docs: &PipelineDocs) -> Result<Self, StateError> {
        Ok(Self {
            jobs: docs.unfinished_snapshot_jobs().await?,
        })
    }

    /// The jobs still asked to run: a cancelled one's tables are neither started nor resumed.
    fn live(&self) -> impl Iterator<Item = &SnapshotJobDoc> {
        self.jobs.iter().filter(|j| j.cancel.is_none())
    }

    fn tables(&self, status: JobTableStatus) -> BTreeSet<TableRef> {
        self.live()
            .flat_map(|j| &j.tables)
            .filter(|t| t.status == status)
            .map(|t| t.table.clone())
            .collect()
    }

    /// The tables a job asks for that no start has taken yet.
    pub fn asked(&self) -> BTreeSet<TableRef> {
        self.tables(JobTableStatus::Queued)
    }

    /// The tables whose job snapshot runs.
    pub fn running(&self) -> BTreeSet<TableRef> {
        self.tables(JobTableStatus::Running)
    }

    /// The asked tables of the jobs that empty their sink tables first (DA-SNAP-15).
    pub fn truncating(&self) -> BTreeSet<TableRef> {
        self.live()
            .filter(|j| j.truncate)
            .flat_map(|j| &j.tables)
            .filter(|t| t.status == JobTableStatus::Queued)
            .map(|t| t.table.clone())
            .collect()
    }

    /// The running tables of the cancelled jobs that no live job asks for or runs (D-M13c.2):
    /// their copy stops at this start; one of a truncating job, or one brought back from a
    /// halt, halts its table. A queued table whose halt a start cleared and noted
    /// (see [`Jobs::note_cleared`]) counts too: that start crashed before it recorded the job,
    /// so the copy may have begun.
    pub fn cancelling(&self) -> BTreeMap<TableRef, Cancelling> {
        let (asked, running) = (self.asked(), self.running());
        self.jobs
            .iter()
            .filter(|j| j.cancel.is_some())
            .flat_map(|j| j.tables.iter().map(move |t| (j, t)))
            .filter(|(_, t)| match t.status {
                JobTableStatus::Running => true,
                JobTableStatus::Queued => t.from_halt.is_some(),
                _ => false,
            })
            .filter(|(_, t)| !asked.contains(&t.table) && !running.contains(&t.table))
            .map(|(j, t)| {
                let c = Cancelling {
                    job: j.job_id.clone(),
                    from_halt: t.from_halt.is_some(),
                    truncate: j.truncate,
                    rows_done: 0,
                };
                (t.table.clone(), c)
            })
            .collect()
    }

    /// Notes, on each live job, the halt a start clears for its queued table (`cleared` maps
    /// the table to the halt's code), before the clear (D-M13c.2): a restart after a crash in
    /// between still knows the copy came back from a halt. The status stays queued. A job is
    /// written only when one of its tables changes, so the call is idempotent.
    pub async fn note_cleared(
        &self,
        docs: &PipelineDocs,
        cleared: &BTreeMap<TableRef, String>,
    ) -> Result<(), StateError> {
        if cleared.is_empty() {
            return Ok(());
        }
        let unnoted = |job: &SnapshotJobDoc| {
            job.tables.iter().any(|t| {
                t.status == JobTableStatus::Queued
                    && cleared
                        .get(&t.table)
                        .is_some_and(|code| t.from_halt.as_deref() != Some(code.as_str()))
            })
        };
        for job in self.live() {
            // Read fresh: this job's doc may have been noted by an earlier call.
            let Some(v) = docs.snapshot_job(&job.job_id).await? else {
                continue;
            };
            if !unnoted(&v.doc) {
                continue;
            }
            docs.update_snapshot_job(&job.job_id, |j| {
                for t in &mut j.tables {
                    if t.status != JobTableStatus::Queued {
                        continue;
                    }
                    if let Some(code) = cleared.get(&t.table) {
                        t.from_halt = Some(code.clone());
                    }
                }
            })
            .await?;
        }
        Ok(())
    }

    /// Records what this start did with the job tables, at `now`: the asked ones started run,
    /// with their estimate and the halt cleared for them; the failed ones (asked, or running
    /// and gone) failed with their reason. An asked table neither started nor failed stays
    /// queued. A cancelled job's unended tables end cancelled.
    pub async fn record(
        &self,
        docs: &PipelineDocs,
        taken: &Taken,
        now: DateTime<Utc>,
    ) -> Result<(), StateError> {
        let decided = |cancelled: bool, t: &JobTable| match t.status {
            _ if cancelled => !t.status.is_ended(),
            JobTableStatus::Queued => {
                taken.started.contains(&t.table) || taken.failed.contains_key(&t.table)
            }
            JobTableStatus::Running => taken.failed.contains_key(&t.table),
            _ => false,
        };
        for job in &self.jobs {
            let cancelled = job.cancel.is_some();
            if !job.tables.iter().any(|t| decided(cancelled, t)) {
                continue;
            }
            docs.update_snapshot_job(&job.job_id, |j| {
                for t in &mut j.tables {
                    if !decided(cancelled, t) {
                        continue;
                    }
                    if cancelled {
                        match taken.completed.get(&t.table) {
                            Some(rows) if t.status == JobTableStatus::Running => {
                                t.rows_done = *rows;
                                t.end(JobTableStatus::Completed, None, now);
                            }
                            _ => {
                                // The cursor's rows, when the follower's last poll was behind.
                                if let Some(rows) = taken.cancelled_rows.get(&t.table) {
                                    t.rows_done = t.rows_done.max(*rows);
                                }
                                t.end(JobTableStatus::Cancelled, None, now);
                            }
                        }
                    } else if let Some(e) = taken.failed.get(&t.table) {
                        t.end(JobTableStatus::Failed, Some(e.clone()), now);
                    } else {
                        t.status = JobTableStatus::Running;
                        t.started_at = Some(now);
                        t.estimate = taken.estimates.get(&t.table).copied();
                        // A halt noted before a crash stays noted (see `note_cleared`).
                        t.from_halt = taken
                            .from_halt
                            .get(&t.table)
                            .cloned()
                            .or_else(|| t.from_halt.clone());
                    }
                }
                j.settle(now);
            })
            .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use chrono::Utc;
    use data_avalanche_config::Pipeline;
    use data_avalanche_core::{
        ColumnSchema, ColumnValue, ColumnValueType as V, HaltReason, Row, SourceError, TableSchema,
    };
    use data_avalanche_state::{
        CursorKey, JobCancel, JobStatus, JobTableStatus, SnapshotCursor, SnapshotJobDoc,
        SnapshotStatus, TableHalt, TableSnapshotDoc,
    };
    use data_avalanche_testkit::{FakeTxn, Method};
    use tokio::sync::watch;

    use super::Jobs;
    use crate::test_support::{World, Yaml, fast, row, t, wait_until};
    use crate::{Command, Exit, Phase, PipelineWorker, WorkerDeps, WorkerError, WorkerStatus};

    fn never(chunk_size: usize) -> Pipeline {
        let tables = format!("\n      snapshots: {{ mode: never, chunk_size: {chunk_size} }}");
        Yaml {
            tables: &tables,
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build()
    }

    fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
        ids.map(|id| row(id, "x")).collect()
    }

    async fn ask(w: &World, id: &str, tables: &[&str]) {
        let doc = SnapshotJobDoc::new(
            id.into(),
            tables.iter().map(|n| t(n)).collect(),
            "ops".into(),
            Utc::now(),
        );
        w.docs.create_snapshot_job(&doc).await.unwrap();
    }

    async fn ask_truncating(w: &World, id: &str, tables: &[&str]) {
        let doc = SnapshotJobDoc::new(
            id.into(),
            tables.iter().map(|n| t(n)).collect(),
            "ops".into(),
            Utc::now(),
        )
        .truncating();
        w.docs.create_snapshot_job(&doc).await.unwrap();
    }

    async fn job(w: &World, id: &str) -> SnapshotJobDoc {
        w.docs.snapshot_job(id).await.unwrap().unwrap().doc
    }

    /// Runs a worker over `w` until `drive` returns, then drains it.
    async fn run<F: Future<Output = ()>>(
        w: &World,
        p: &Pipeline,
        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
    ) -> (Result<Exit, WorkerError>, WorkerStatus) {
        let deps = WorkerDeps::new(
            Arc::new(w.source.clone()),
            Arc::new(w.sink.clone()),
            w.docs.clone(),
        );
        let (worker, handle) = PipelineWorker::new(p, deps, fast());
        let status = handle.status.clone();
        let driver = async move {
            drive(handle.status.clone()).await;
            let _ = handle.control.send(Command::Drain).await;
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        let last = status.borrow().clone();
        (end, last)
    }

    /// A stream session streams, and everything up to the log's end is durable.
    async fn streamed(status: &watch::Receiver<WorkerStatus>, w: &World) {
        let end = w.source.wal_end();
        wait_until("the log's end is durable", || {
            let st = status.borrow();
            st.phase == Phase::Streaming && st.checkpoint.as_ref() == Some(&end)
        })
        .await;
    }

    /// A first run under `mode: never`: the slot, the sink tables, the schemas, the checkpoint.
    async fn established(w: &World) {
        let (end, _) = run(w, &never(10), async |status| streamed(&status, w).await).await;
        end.unwrap();
    }

    async fn snapshot_status(w: &World, table: &str) -> SnapshotStatus {
        w.docs
            .table_snapshot(&t(table))
            .await
            .unwrap()
            .unwrap()
            .doc
            .status
    }

    /// A streamed table asked for is read again from its first row: rows the stream never
    /// carried (loaded behind its back) reach the sink. The job's table is running; the owner's
    /// controller sees it to its end.
    #[tokio::test]
    async fn a_job_snapshots_a_streamed_table_again_from_scratch() {
        let w = World::new(&["a", "b"]);
        established(&w).await;
        w.source
            .commit(FakeTxn::new().insert(&t("a"), row(1, "streamed")));
        w.source.load(&t("a"), rows(5..=7));
        ask(&w, "p~1", &["a"]).await;
        let (end, st) = run(&w, &never(10), async |status| {
            wait_until("the rows land", || w.ids("a") == [1, 5, 6, 7]).await;
            streamed(&status, &w).await;
        })
        .await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::Completed);
        assert_eq!(w.source.reads(&t("b")), 0, "only the job's table");
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Running);
        assert_eq!(j.tables[0].status, JobTableStatus::Running);
        assert!(st.warnings.is_empty(), "{:?}", st.warnings);
    }

    /// DA-SRC-32: a source change repositions every table the lake holds, a job's copy too,
    /// which the pipeline neither resolves nor caches: its marker leaves the old server's
    /// positions, so a later copy or stream of it isn't skipped below them.
    #[tokio::test]
    async fn a_source_change_repositions_a_copy_the_pipeline_doesnt_track() {
        use data_avalanche_core::position::Lsn;
        use data_avalanche_core::{ServerIdentity, Sink, SourcePosition};
        let pg = |timeline| ServerIdentity::Postgres {
            system_identifier: 7,
            timeline,
        };
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        w.source.set_server(Some(pg(1)));
        established(&w).await;
        w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
        w.source.load(&t("x"), rows(1..=3));
        ask(&w, "p~0", &["x"]).await;
        let (end, _) = run(&w, &never(10), async |status| {
            wait_until("x is copied", || w.ids("x") == [1, 2, 3]).await;
            streamed(&status, &w).await;
            wait_until("the copy is done", || status.borrow().snapshots.is_empty()).await;
        })
        .await;
        end.unwrap();
        let old = w.sink.table_state(&t("x")).await.unwrap().marker.unwrap();
        w.source.fail_over(pg(2), Lsn(0x100));
        let (end, _) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        let moved = w.sink.table_state(&t("x")).await.unwrap().marker.unwrap();
        assert_eq!(moved.committed_position, SourcePosition::Pg(Lsn(0x100)));
        assert_ne!(moved, old);
    }

    /// DA-SNAP-15: a job asked with `truncate` empties its tables' sink tables before their
    /// first chunk, so a row deleted at the source while the pipeline didn't stream it goes, in
    /// a streamed table and in a copy alike. The other tables keep their rows, and a later start
    /// doesn't empty the table again: the job's copy has begun.
    #[tokio::test]
    async fn a_truncating_job_drops_the_rows_deleted_meanwhile() {
        let w = World::new(&["a", "b", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        w.source.commit(
            FakeTxn::new()
                .insert(&t("a"), row(1, "x"))
                .insert(&t("a"), row(2, "x"))
                .insert(&t("a"), row(3, "x"))
                .insert(&t("b"), row(9, "x")),
        );
        w.source.load(&t("x"), rows(1..=3));
        ask(&w, "p~0", &["x"]).await;
        let (end, _) = run(&w, &never(10), async |status| {
            wait_until("x is copied", || w.ids("x") == [1, 2, 3]).await;
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2, 3]);
        w.source.forget(&t("a"), &row(2, "x"));
        w.source.forget(&t("x"), &row(3, "x"));
        ask_truncating(&w, "p~1", &["a", "x"]).await;
        let (end, _) = run(&w, &never(10), async |status| {
            wait_until("the copies land", || {
                w.ids("a") == [1, 3] && w.ids("x") == [1, 2]
            })
            .await;
            streamed(&status, &w).await;
            wait_until("the copies are done", || {
                status.borrow().snapshots.is_empty()
            })
            .await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("b"), [9], "only the job's tables");
        assert_eq!(
            job(&w, "p~1").await.tables[0].status,
            JobTableStatus::Running
        );
        w.source.forget(&t("a"), &row(3, "x"));
        w.source.commit(FakeTxn::new().insert(&t("a"), row(4, "x")));
        let (end, _) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 3, 4], "not emptied again");
    }

    /// DA-SRC-25 × DA-SNAP-15: where names fold, a table whose key changed case alone keeps
    /// the cached spelling, which its sink table records. A truncating job on it empties the
    /// sink table under that key, as the session then copies it: the start succeeds, where the
    /// catalog's spelling would have the sink refuse the truncate for good and end every start.
    #[tokio::test]
    async fn a_truncating_job_keys_a_recased_key_as_the_sink_table_does() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.commit(
            FakeTxn::new()
                .insert(&t("a"), row(1, "x"))
                .insert(&t("a"), row(2, "x")),
        );
        let (end, _) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2]);
        w.source.fold_column_names();
        w.source.alter(
            &t("a"),
            TableSchema::new(
                t("a"),
                vec![
                    ColumnSchema::new("ID", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("doc", V::Text, "text"),
                ],
                vec!["ID".into()],
            )
            .unwrap(),
        );
        ask_truncating(&w, "p~1", &["a"]).await;
        let (mut core, _control, _status) = w.core(&never(10), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        assert!(w.ids("a").is_empty(), "emptied: {:?}", w.ids("a"));
        assert_eq!(
            job(&w, "p~1").await.tables[0].status,
            JobTableStatus::Running
        );
    }

    /// DA-SRC-25 × DA-SNAP-15: a truncating job on a table whose sink table records the key
    /// spelled otherwise than the session's, by case alone where names fold, doesn't empty it.
    /// Here the key changed case alone, so the session keeps the cached spelling (`id`), and the
    /// operator made the sink table again under the catalog's (`ID`) and put a row in it. No
    /// sink accepts a key that differs by case alone, and the session never takes the sink's
    /// spelling, so every chunk of the copy would be refused: the table halts `SINK_REJECTED` at
    /// the start with its rows intact, its job table fails, and the start goes on (final review
    /// I-A, m-1).
    #[tokio::test]
    async fn a_truncating_job_halts_a_table_its_sink_table_keys_spelled_otherwise() {
        use arrow_array::Int64Array;
        use data_avalanche_core::position::Lsn;
        use data_avalanche_core::{Op, PositionRange, Sink, SourceSeq, TableChangeSet, TableSpec};

        use crate::batch::{Lineage, MAX_BATCH_BYTES, build};
        use crate::buffer::{CollapsedRow, RowKind};
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
        let (end, _) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        let marked = w.sink.marker(&t("a")).unwrap();
        w.source.fold_column_names();
        let recased = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("ID", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
            ],
            vec!["ID".into()],
        )
        .unwrap();
        w.source.alter(&t("a"), recased.clone());
        w.sink.drop_table(&t("a"));
        w.sink
            .ensure_table(&TableSpec::new(&recased, vec![]).unwrap())
            .await
            .unwrap();
        // The operator's row, committed outside the pipeline.
        let at = marked.committed_position.clone();
        let (upserts, _) = build(
            &recased,
            &[0],
            vec![CollapsedRow {
                kind: RowKind::Upsert,
                values: vec![ColumnValue::Int64(7), ColumnValue::Text("x".into())],
                op: Op::Insert,
                commit_ts_micros: 0,
                position: at.clone(),
                seq: SourceSeq::Pg(Lsn(0)),
            }],
            &Lineage {
                source_db: "shop".into(),
                pipeline: "p".into(),
            },
            MAX_BATCH_BYTES,
        );
        let columns = w.sink.table_state(&t("a")).await.unwrap().schema.unwrap();
        let cs = TableChangeSet {
            table: t("a"),
            schema: columns,
            identifier: vec!["ID".into()],
            truncate: false,
            upserts,
            deletes: vec![],
            position: PositionRange {
                start: at.clone(),
                end: at.clone(),
                last_change: at,
            },
            split: None,
            snapshot: false,
            reposition: false,
        };
        w.sink.commit(cs, marked.clone()).await.unwrap();
        let ids = || {
            let b = w.sink.content(&t("a")).unwrap();
            let ids = b.column_by_name("ID").unwrap();
            ids.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        assert_eq!(ids(), [7]);
        ask_truncating(&w, "p~1", &["a"]).await;
        let (mut core, _control, status) = w.core(&never(10), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        assert_eq!(ids(), [7], "not emptied");
        let state = w.sink.table_state(&t("a")).await.unwrap();
        assert_eq!(state.identifier, ["ID"], "the sink table's own key");
        assert_eq!(state.marker, Some(marked), "nothing committed");
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1, "{halts:?}");
        assert!(!halts[0].copy);
        let HaltReason::SinkRejected { detail } = &halts[0].reason else {
            panic!("{halts:?}");
        };
        assert!(
            detail.contains(r#"["id"]"#) && detail.contains(r#"["ID"]"#),
            "{detail}"
        );
        assert!(status.borrow().halted.contains_key(&t("a")));
        let j = job(&w, "p~1").await;
        assert_eq!(j.tables[0].status, JobTableStatus::Failed);
        let error = j.tables[0].error.clone().unwrap();
        assert!(error.starts_with("SINK_REJECTED: "), "{error}");
    }

    /// DA-SRC-25 × DA-SNAP-15: a truncating job on a table whose sink table records another key
    /// than the session's schema (here a halt cleared by the job baselines a key moved from `id`
    /// to `doc`, the columns unchanged, while the sink table stays keyed `id`) doesn't empty
    /// it: the session's commits would be refused for good, so the table halts `SINK_REJECTED`
    /// at the start with its rows intact, its job table fails, and the start goes on (final
    /// review M-A).
    #[tokio::test]
    async fn a_truncating_job_halts_a_table_its_sink_table_keys_otherwise() {
        let keyed = |key: &str| {
            TableSchema::new(
                t("a"),
                vec![
                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("doc", V::Text, "text").not_null(),
                ],
                vec![key.into()],
            )
            .unwrap()
        };
        let w = World::new(&["a", "b"]);
        w.source.alter(&t("a"), keyed("id"));
        established(&w).await;
        w.source.commit(
            FakeTxn::new()
                .insert(&t("a"), row(1, "x"))
                .insert(&t("a"), row(2, "y")),
        );
        let (end, _) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2]);
        w.docs
            .record_halt(&TableHalt {
                table: t("a"),
                reason: HaltReason::UnsafeDdl {
                    detail: "the key changed".into(),
                },
                at: w.source.wal_end(),
                copy: false,
            })
            .await
            .unwrap();
        w.source.alter(&t("a"), keyed("doc"));
        ask_truncating(&w, "p~1", &["a", "b"]).await;
        let (mut core, _control, status) = w.core(&never(10), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        assert_eq!(w.ids("a"), [1, 2], "not emptied");
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1, "{halts:?}");
        let HaltReason::SinkRejected { detail } = &halts[0].reason else {
            panic!("{halts:?}");
        };
        assert!(
            detail.contains(r#"["doc"]"#) && detail.contains(r#"["id"]"#),
            "{detail}"
        );
        assert!(status.borrow().halted.contains_key(&t("a")));
        let j = job(&w, "p~1").await;
        assert_eq!(j.tables[0].status, JobTableStatus::Failed);
        let error = j.tables[0].error.clone().unwrap();
        assert!(error.starts_with("SINK_REJECTED: "), "{error}");
        assert_eq!(j.tables[1].status, JobTableStatus::Running, "b goes on");
    }

    /// DA-SRC-25 × DA-SNAP-15: the same for a job's copy, a table the config doesn't name: its
    /// sink table, made by an earlier copy, is keyed `id`, and the source moved the key to `doc`,
    /// so the copy is described keyed `doc`. The table isn't emptied. Its halt is recorded as the
    /// copy's, so it isn't a halted table of the pipeline (not in the status); the job's table
    /// fails with the reason, which is where an operator sees it. The start goes on (final review
    /// M-A, m-3).
    #[tokio::test]
    async fn a_truncating_job_halts_a_copy_its_sink_table_keys_otherwise() {
        let keyed = |key: &str| {
            TableSchema::new(
                t("x"),
                vec![
                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("doc", V::Text, "text").not_null(),
                ],
                vec![key.into()],
            )
            .unwrap()
        };
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        w.source.alter(&t("x"), keyed("id"));
        established(&w).await;
        w.source.load(&t("x"), rows(1..=2));
        ask(&w, "p~0", &["x"]).await;
        let (end, _) = run(&w, &never(10), async |status| {
            wait_until("x is copied", || w.ids("x") == [1, 2]).await;
            streamed(&status, &w).await;
            wait_until("the copy is done", || status.borrow().snapshots.is_empty()).await;
        })
        .await;
        end.unwrap();
        w.source.alter(&t("x"), keyed("doc"));
        ask_truncating(&w, "p~1", &["x"]).await;
        let (mut core, _control, status) = w.core(&never(10), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        assert_eq!(w.ids("x"), [1, 2], "not emptied");
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1, "{halts:?}");
        assert_eq!(halts[0].table, t("x"));
        assert!(halts[0].copy, "the copy's halt");
        let HaltReason::SinkRejected { detail } = &halts[0].reason else {
            panic!("{halts:?}");
        };
        assert!(
            detail.contains(r#"["doc"]"#) && detail.contains(r#"["id"]"#),
            "{detail}"
        );
        assert!(
            status.borrow().halted.is_empty(),
            "not the pipeline's: {:?}",
            status.borrow().halted
        );
        let j = job(&w, "p~1").await;
        assert_eq!(j.tables[0].status, JobTableStatus::Failed);
        let error = j.tables[0].error.clone().unwrap();
        assert!(error.starts_with("SINK_REJECTED: "), "{error}");
    }

    fn noted(id: i64, note: Option<&str>) -> Row {
        Row(vec![
            ("id".into(), ColumnValue::Int64(id)),
            ("doc".into(), ColumnValue::Text("x".into())),
            (
                "note".into(),
                note.map_or(ColumnValue::Null, |n| ColumnValue::Text(n.into())),
            ),
        ])
    }

    fn with_note(name: &str) -> TableSchema {
        TableSchema::new(
            t(name),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    /// A halted table asked for (dropped and created again, here with a column more) is
    /// streamed again: its halt is cleared, its cached schema is the catalog's (the new table's
    /// source id), its sink table takes the new column, and its rows are read.
    #[tokio::test]
    async fn a_job_clears_a_halt_and_baselines_the_table() {
        let w = World::new(&["a"]);
        established(&w).await;
        let old = w.source.schema(&t("a"));
        w.docs
            .record_halt(&TableHalt {
                table: t("a"),
                reason: HaltReason::TableDropped,
                at: w.source.wal_end(),
                copy: false,
            })
            .await
            .unwrap();
        w.source.drop_table(&t("a"));
        w.source.create_table(with_note("a"));
        w.source.load(&t("a"), vec![noted(1, None), noted(2, None)]);
        ask(&w, "p~1", &["a"]).await;
        let (end, st) = run(&w, &never(10), async |status| {
            wait_until("the rows land", || w.ids("a") == [1, 2]).await;
            w.source
                .commit(FakeTxn::new().insert(&t("a"), noted(3, Some("n"))));
            wait_until("the change streams", || w.ids("a") == [1, 2, 3]).await;
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        assert!(w.docs.halts().await.unwrap().tables.is_empty());
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("a")));
        assert_ne!(cached.source_id(), old.source_id());
        assert_eq!(w.text("a", "note"), [None, None, Some("n".into())]);
        assert_eq!(
            job(&w, "p~1").await.tables[0].status,
            JobTableStatus::Running
        );
    }

    /// A table asked for that was dropped and created again while the pipeline was down, its
    /// halt not recorded yet, halts at this start and fails in its job (`TABLE_DROPPED`): the
    /// stream's position may predate the drop. A job asked again clears the halt.
    #[tokio::test]
    async fn a_table_created_again_halts_and_fails_its_job() {
        let w = World::new(&["a", "b"]);
        established(&w).await;
        w.source.drop_table(&t("a"));
        w.source.create_table(with_note("a"));
        w.source.load(&t("a"), vec![noted(1, None)]);
        ask(&w, "p~1", &["a"]).await;
        let (end, st) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(st.halted.get(&t("a")), Some(&HaltReason::TableDropped));
        let j = job(&w, "p~1").await;
        assert_eq!(j.tables[0].status, JobTableStatus::Failed);
        let error = j.tables[0].error.clone().unwrap();
        assert!(error.starts_with("TABLE_DROPPED: "), "{error}");
        ask(&w, "p~2", &["a"]).await;
        let (end, st) = run(&w, &never(10), async |status| {
            wait_until("the rows land", || w.ids("a") == [1]).await;
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("a")));
    }

    /// A table asked for that halts at this start (here its identifier changed) fails in its
    /// job, with the halt's reason; the job ends once no table is left.
    #[tokio::test]
    async fn a_job_table_that_halts_at_the_start_fails() {
        let w = World::new(&["a", "b"]);
        established(&w).await;
        w.docs
            .record_halt(&TableHalt {
                table: t("a"),
                reason: HaltReason::UnsafeDdl {
                    detail: "the key changed".into(),
                },
                at: w.source.wal_end(),
                copy: false,
            })
            .await
            .unwrap();
        // The key moves to `doc`: an identifier change is unsafe (`Schema_Change…` §7.2).
        let rekeyed = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text").not_null(),
            ],
            vec!["doc".into()],
        )
        .unwrap();
        w.source.alter(&t("a"), rekeyed);
        ask(&w, "p~1", &["a"]).await;
        let (end, st) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert!(st.halted.contains_key(&t("a")));
        let j = job(&w, "p~1").await;
        assert_eq!(j.tables[0].status, JobTableStatus::Failed);
        let error = j.tables[0].error.clone().unwrap();
        assert!(error.starts_with("UNSAFE_DDL: "), "{error}");
        assert_eq!(j.status, JobStatus::Failed);
        assert!(j.finished_at.is_some());
    }

    /// A job's table whose snapshot runs keeps running at the next start, from its cursor,
    /// even under `mode: never`, which abandons a snapshot in progress.
    #[tokio::test]
    async fn a_running_job_table_resumes_from_its_cursor() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=4));
        ask(&w, "p~1", &["a"]).await;
        w.docs
            .update_snapshot_job("p~1", |j| {
                j.tables[0].status = JobTableStatus::Running;
                j.settle(Utc::now());
            })
            .await
            .unwrap();
        w.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: t("a"),
                status: SnapshotStatus::InProgress,
                start_position: Some(w.source.wal_end()),
                cursor: Some(SnapshotCursor {
                    key: vec![CursorKey {
                        col: "id".into(),
                        val: 2.into(),
                        ty: "bigint".into(),
                    }],
                    chunk_size: 2,
                    rows_done: 2,
                    collation: None,
                }),
                row_count: None,
                completed_at: None,
            })
            .await
            .unwrap();
        let (end, _) = run(&w, &never(2), async |status| {
            wait_until("the rest lands", || w.ids("a") == [3, 4]).await;
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::Completed);
        assert_eq!(
            w.ids("a"),
            [3, 4],
            "rows 1 and 2 were read by an earlier run"
        );
        assert_eq!(
            job(&w, "p~1").await.tables[0].status,
            JobTableStatus::Running
        );
    }

    /// A table outside the config is copied (Q-M9b-3): described by its name, its sink table
    /// created, its rows read, and never streamed: a change made while the copy runs, to a row
    /// its first chunk read before, isn't in the sink. Its schema isn't cached, so a later start
    /// doesn't take it for a table gone; once its copy is done, a restart doesn't read it again.
    #[tokio::test]
    async fn a_table_outside_the_config_is_copied_and_not_streamed() {
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        w.source.load(&t("x"), rows(1..=3));
        ask(&w, "p~1", &["x"]).await;
        w.source.hold_reads();
        let (end, st) = run(&w, &never(2), async |status| {
            wait_until("the first chunk is read", || w.source.reads(&t("x")) == 1).await;
            w.source.commit(
                FakeTxn::new()
                    .update(&t("x"), row(1, "changed"))
                    .insert(&t("a"), row(1, "streamed")),
            );
            w.source.release_reads();
            wait_until("the copy lands", || w.ids("x") == [1, 2, 3]).await;
            wait_until("the stream moves on", || w.ids("a") == [1]).await;
            streamed(&status, &w).await;
            // Its rows land before its last chunk read (an empty one) marks it done.
            wait_until("the copy is done", || status.borrow().snapshots.is_empty()).await;
        })
        .await;
        end.unwrap();
        assert_eq!(
            w.text("x", "doc"),
            [Some("x".into()), Some("x".into()), Some("x".into())],
            "a copy isn't streamed"
        );
        assert_eq!(snapshot_status(&w, "x").await, SnapshotStatus::Completed);
        assert!(w.docs.table_schema(&t("x")).await.unwrap().is_none());
        assert!(st.halted.is_empty() && st.warnings.is_empty(), "{st:?}");
        assert_eq!(
            job(&w, "p~1").await.tables[0].status,
            JobTableStatus::Running
        );
        let reads = w.source.reads(&t("x"));
        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(
            w.source.reads(&t("x")),
            reads,
            "a copy done isn't read again"
        );
        assert!(st.halted.is_empty(), "{:?}", st.halted);
    }

    /// A copy whose snapshot runs resumes from its cursor at the next start: its table is
    /// described again, its schema being cached nowhere.
    #[tokio::test]
    async fn a_running_copy_resumes_from_its_cursor() {
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        w.source.load(&t("x"), rows(1..=4));
        ask(&w, "p~1", &["x"]).await;
        w.docs
            .update_snapshot_job("p~1", |j| {
                j.tables[0].status = JobTableStatus::Running;
                j.settle(Utc::now());
            })
            .await
            .unwrap();
        let spec = data_avalanche_core::TableSpec::new(&w.source.schema(&t("x")), vec![]).unwrap();
        data_avalanche_core::Sink::ensure_table(&w.sink, &spec)
            .await
            .unwrap();
        w.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: t("x"),
                status: SnapshotStatus::InProgress,
                start_position: Some(w.source.wal_end()),
                cursor: Some(SnapshotCursor {
                    key: vec![CursorKey {
                        col: "id".into(),
                        val: 2.into(),
                        ty: "bigint".into(),
                    }],
                    chunk_size: 2,
                    rows_done: 2,
                    collation: None,
                }),
                row_count: None,
                completed_at: None,
            })
            .await
            .unwrap();
        let (end, _) = run(&w, &never(2), async |status| {
            wait_until("the rest lands", || w.ids("x") == [3, 4]).await;
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(snapshot_status(&w, "x").await, SnapshotStatus::Completed);
    }

    /// A table the source doesn't have fails in its job, as does a running copy whose table is
    /// gone at the next start; nothing is halted for them, as they aren't the pipeline's.
    #[tokio::test]
    async fn a_table_the_source_lacks_fails_in_its_job() {
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        ask(&w, "p~1", &["missing"]).await;
        ask(&w, "p~2", &["x"]).await;
        w.docs
            .update_snapshot_job("p~2", |j| {
                j.tables[0].status = JobTableStatus::Running;
                j.settle(Utc::now());
            })
            .await
            .unwrap();
        w.source.drop_table(&t("x"));
        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        for id in ["p~1", "p~2"] {
            let j = job(&w, id).await;
            assert_eq!(j.status, JobStatus::Failed, "{id}");
            assert_eq!(j.tables[0].error.as_deref(), Some("no such table"), "{id}");
        }
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        assert!(w.docs.halts().await.unwrap().tables.is_empty());
    }

    /// A copy whose chunk read fails for good (a table the role can't read) halts, alone: the
    /// stream goes on, and the copy's halt is recorded with the error.
    #[tokio::test]
    async fn a_copy_that_can_t_be_read_halts_alone() {
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        w.source.load(&t("x"), rows(1..=3));
        ask(&w, "p~1", &["x"]).await;
        w.source.fail(
            Method::ReadChunk,
            SourceError::Permanent("permission denied for table x".into()),
        );
        let (end, st) = run(&w, &never(2), async |status| {
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(1, "streamed")));
            wait_until("the stream moves on", || w.ids("a") == [1]).await;
            streamed(&status, &w).await;
        })
        .await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert!(
            st.halted.is_empty(),
            "a copy's halt isn't shown: {:?}",
            st.halted
        );
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1, "{halts:?}");
        assert_eq!(halts[0].table, t("x"));
        assert_eq!(halts[0].reason.code(), "TABLE_NOT_REPLICABLE");
        assert!(halts[0].copy, "it fails its job, marked a copy's");
        assert!(w.ids("x").is_empty());
    }

    /// A copy's halt stays out of the halted tables after a reconnect: the session that opens
    /// next reads the recorded halts, the copy's among them, and shows only the pipeline's. Nor
    /// is the halted copy read again: its job failed it.
    #[tokio::test]
    async fn a_copy_s_halt_stays_out_of_the_halted_tables_after_a_reconnect() {
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        w.source.load(&t("x"), rows(1..=3));
        ask(&w, "p~1", &["x"]).await;
        w.source.fail(
            Method::ReadChunk,
            SourceError::Permanent("permission denied for table x".into()),
        );
        let (end, st) = run(&w, &never(2), async |status| {
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(1, "streamed")));
            wait_until("the stream moves on", || w.ids("a") == [1]).await;
            streamed(&status, &w).await;
            let halts = w.docs.halts().await.unwrap().tables;
            assert!(halts.iter().any(|h| h.copy), "recorded first: {halts:?}");
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
            wait_until("a reconnect", || status.borrow().reconnects >= 1).await;
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(2, "streamed")));
            wait_until("the next session streams", || w.ids("a") == [1, 2]).await;
            streamed(&status, &w).await;
            assert!(
                status.borrow().halted.is_empty(),
                "a copy's halt isn't shown after a reconnect: {:?}",
                status.borrow().halted
            );
            assert!(
                !status.borrow().snapshots.contains_key(&t("x")),
                "the halted copy isn't a snapshot again: {:?}",
                status.borrow().snapshots
            );
        })
        .await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert!(st.reconnects >= 1);
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1, "{halts:?}");
        assert_eq!(halts[0].table, t("x"));
        assert!(halts[0].copy, "recorded as a copy's");
        assert!(w.ids("x").is_empty(), "the halted copy isn't read again");
    }

    /// A copy that halts in a run (here its table is dropped while it is read) fails in its
    /// job at the next start, with the halt's reason, and its halt goes: no job can clear the
    /// halt of a table gone, and the pipeline doesn't replicate it.
    #[tokio::test]
    async fn a_copy_halted_in_a_run_fails_its_job_and_its_halt_goes() {
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        w.source.load(&t("x"), rows(1..=3));
        ask(&w, "p~1", &["x"]).await;
        w.source.hold_reads();
        let (end, _) = run(&w, &never(2), async |status| {
            wait_until("the first chunk is read", || w.source.reads(&t("x")) == 1).await;
            w.source.drop_table(&t("x"));
            w.source.release_reads();
            wait_until("the copy halts", || {
                !status.borrow().snapshots.contains_key(&t("x"))
            })
            .await;
            assert!(
                status.borrow().halted.is_empty(),
                "a copy's halt isn't shown"
            );
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(1, "streamed")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.docs.halts().await.unwrap().tables.len(), 1);
        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert!(w.docs.halts().await.unwrap().tables.is_empty());
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Failed);
        let error = j.tables[0].error.clone().unwrap();
        assert!(error.starts_with("TABLE_DROPPED: "), "{error}");
    }

    /// A running copy with a recorded halt isn't read again at the next start: it fails in its
    /// job with the halt's reason.
    #[tokio::test]
    async fn a_running_copy_halted_before_isn_t_read_again() {
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        w.source.load(&t("x"), rows(1..=4));
        ask(&w, "p~1", &["x"]).await;
        w.docs
            .update_snapshot_job("p~1", |j| {
                j.tables[0].status = JobTableStatus::Running;
                j.settle(Utc::now());
            })
            .await
            .unwrap();
        w.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: t("x"),
                status: SnapshotStatus::InProgress,
                start_position: Some(w.source.wal_end()),
                cursor: None,
                row_count: None,
                completed_at: None,
            })
            .await
            .unwrap();
        w.docs
            .record_halt(&TableHalt {
                table: t("x"),
                reason: HaltReason::SnapshotTimeout { limit_secs: 60 },
                at: w.source.wal_end(),
                copy: false,
            })
            .await
            .unwrap();
        let (end, _) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.source.reads(&t("x")), 0, "a halted copy isn't read again");
        assert!(w.ids("x").is_empty());
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Failed);
        let error = j.tables[0].error.clone().unwrap();
        assert!(error.starts_with("SNAPSHOT_TIMEOUT: "), "{error}");
    }

    /// Job `id`'s table `table` is copying: running, its document in progress past two rows,
    /// brought back from a `from_halt` halt when set.
    async fn copying(w: &World, id: &str, table: &str, from_halt: Option<&str>) {
        w.docs
            .update_snapshot_job(id, |j| {
                let jt = j.tables.iter_mut().find(|jt| jt.table == t(table)).unwrap();
                jt.status = JobTableStatus::Running;
                jt.from_halt = from_halt.map(Into::into);
                j.settle(Utc::now());
            })
            .await
            .unwrap();
        w.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: t(table),
                status: SnapshotStatus::InProgress,
                start_position: Some(w.source.wal_end()),
                cursor: Some(SnapshotCursor {
                    key: vec![CursorKey {
                        col: "id".into(),
                        val: 2.into(),
                        ty: "bigint".into(),
                    }],
                    chunk_size: 2,
                    rows_done: 2,
                    collation: None,
                }),
                row_count: None,
                completed_at: None,
            })
            .await
            .unwrap();
    }

    async fn cancel(w: &World, id: &str) {
        w.docs
            .update_snapshot_job(id, |j| {
                j.cancel = Some(JobCancel {
                    by: "ops".into(),
                    role: "operator".into(),
                    at: Utc::now(),
                    note: None,
                });
            })
            .await
            .unwrap();
    }

    /// D-M13c.2: a cancelled job's running copy of a streamed table stops at the next start:
    /// no chunk is read, its document is abandoned, the table streams on, and the job ends
    /// cancelled.
    #[tokio::test]
    async fn a_cancelled_copy_stops_and_the_table_streams_on() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=4));
        ask(&w, "p~1", &["a"]).await;
        copying(&w, "p~1", "a", None).await;
        cancel(&w, "p~1").await;
        let (end, st) = run(&w, &never(2), async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(9, "x")));
            wait_until("the change streams", || w.ids("a") == [9]).await;
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.source.reads(&t("a")), 0, "the copy isn't resumed");
        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::None);
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Cancelled);
        assert!(j.finished_at.is_some());
        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
        assert!(j.tables[0].ended_at.is_some());
        assert_eq!(j.error, None);
    }

    /// D-M13c.2: a cancelled copy of a table its job brought back from a halt halts it again,
    /// `COPY_CANCELLED` naming the job: the lake holds a part of its rows.
    #[tokio::test]
    async fn a_cancelled_copy_of_a_halted_table_halts_it_again() {
        let w = World::new(&["a", "b"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=4));
        ask(&w, "p~1", &["a"]).await;
        copying(&w, "p~1", "a", Some("TABLE_DROPPED")).await;
        cancel(&w, "p~1").await;
        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        let reason = HaltReason::CopyCancelled { job: "p~1".into() };
        assert_eq!(st.halted.get(&t("a")), Some(&reason));
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1, "{halts:?}");
        assert_eq!(halts[0].reason, reason);
        assert!(!halts[0].copy, "the pipeline's table");
        assert_eq!(w.source.reads(&t("a")), 0);
        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::None);
        assert_eq!(job(&w, "p~1").await.status, JobStatus::Cancelled);
    }

    /// D-M13c.2: a cancelled job whose tables are queued never starts them.
    #[tokio::test]
    async fn a_cancelled_queued_job_never_starts() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=3));
        ask(&w, "p~1", &["a"]).await;
        cancel(&w, "p~1").await;
        let (end, _) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.source.reads(&t("a")), 0);
        assert!(w.ids("a").is_empty());
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Cancelled);
        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
        assert_eq!(j.tables[0].started_at, None);
    }

    /// D-M13c.2: a cancelled copy of a table outside the config isn't read again, and no halt
    /// is recorded for it.
    #[tokio::test]
    async fn a_cancelled_copy_outside_the_config_isn_t_read() {
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        w.source.load(&t("x"), rows(1..=4));
        ask(&w, "p~1", &["x"]).await;
        copying(&w, "p~1", "x", None).await;
        cancel(&w, "p~1").await;
        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.source.reads(&t("x")), 0);
        assert_eq!(snapshot_status(&w, "x").await, SnapshotStatus::None);
        assert!(w.docs.halts().await.unwrap().tables.is_empty());
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        assert_eq!(job(&w, "p~1").await.status, JobStatus::Cancelled);
    }

    /// A copy that completed before a start took its job's cancel ends completed, with its
    /// rows: the lake holds them all.
    #[tokio::test]
    async fn a_copy_completed_before_the_cancel_ends_completed() {
        let w = World::new(&["a"]);
        established(&w).await;
        ask(&w, "p~1", &["a"]).await;
        copying(&w, "p~1", "a", Some("TABLE_DROPPED")).await;
        w.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: t("a"),
                status: SnapshotStatus::Completed,
                start_position: None,
                cursor: None,
                row_count: Some(4),
                completed_at: Some(Utc::now()),
            })
            .await
            .unwrap();
        cancel(&w, "p~1").await;
        let (mut core, _control, status) = w.core(&never(2), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        assert!(status.borrow().halted.is_empty(), "not halted again");
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Completed);
        assert_eq!(j.tables[0].status, JobTableStatus::Completed);
        assert_eq!(j.tables[0].rows_done, 4);
    }

    /// A job asked for a table after another's cancel, before a start took it, copies the table
    /// again from its first row; the cancelled job ends cancelled, and nothing halts.
    #[tokio::test]
    async fn a_job_asked_after_a_cancel_copies_the_table_again() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=4));
        ask(&w, "p~1", &["a"]).await;
        copying(&w, "p~1", "a", Some("TABLE_DROPPED")).await;
        cancel(&w, "p~1").await;
        ask(&w, "p~2", &["a"]).await;
        let (end, st) = run(&w, &never(2), async |status| {
            wait_until("the copy lands", || w.ids("a") == [1, 2, 3, 4]).await;
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        assert_eq!(job(&w, "p~1").await.status, JobStatus::Cancelled);
        assert_eq!(
            job(&w, "p~2").await.tables[0].status,
            JobTableStatus::Running
        );
    }

    /// A start that takes a job's tables records when each began, the source's estimate of
    /// its rows (where it has one) and the halt it cleared for it.
    #[tokio::test]
    async fn a_started_table_records_its_start_estimate_and_halt() {
        let w = World::new(&["a", "b"]);
        established(&w).await;
        w.docs
            .record_halt(&TableHalt {
                table: t("a"),
                reason: HaltReason::SnapshotTimeout { limit_secs: 60 },
                at: w.source.wal_end(),
                copy: false,
            })
            .await
            .unwrap();
        w.source
            .set_row_estimates(std::collections::BTreeMap::from([(t("a"), 1234)]));
        ask(&w, "p~1", &["a", "b"]).await;
        let before = Utc::now();
        let (mut core, _control, _status) = w.core(&never(2), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        let j = job(&w, "p~1").await;
        let (a, b) = (&j.tables[0], &j.tables[1]);
        assert_eq!(a.status, JobTableStatus::Running);
        assert!(a.started_at.is_some_and(|at| at >= before), "{a:?}");
        assert_eq!(a.estimate, Some(1234));
        assert_eq!(a.from_halt.as_deref(), Some("SNAPSHOT_TIMEOUT"));
        assert!(b.started_at.is_some());
        assert_eq!(b.estimate, None, "the source has none");
        assert_eq!(b.from_halt, None);
    }

    /// The source's estimates are a nicety: when it can't give them, the start takes the
    /// tables all the same, without one.
    #[tokio::test]
    async fn an_estimate_failure_doesn_t_fail_the_start() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source
            .set_row_estimates(std::collections::BTreeMap::from([(t("a"), 3)]));
        w.source.fail(
            Method::RowEstimates,
            SourceError::Permanent("permission denied for pg_class".into()),
        );
        ask(&w, "p~1", &["a"]).await;
        let (mut core, _control, _status) = w.core(&never(2), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        let a = &job(&w, "p~1").await.tables[0];
        assert_eq!(a.status, JobTableStatus::Running);
        assert_eq!(a.estimate, None);
    }

    /// Post-crash state (fix round 1): job `id`'s table `table` is still queued, its start
    /// having noted the `code` halt it cleared on the job before it crashed. The table's copy
    /// document is in progress past two rows, and no halt is recorded.
    async fn crashed_copy(w: &World, id: &str, table: &str, code: &str) {
        w.docs
            .update_snapshot_job(id, |j| {
                let jt = j.tables.iter_mut().find(|jt| jt.table == t(table)).unwrap();
                jt.from_halt = Some(code.into());
            })
            .await
            .unwrap();
        w.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: t(table),
                status: SnapshotStatus::InProgress,
                start_position: Some(w.source.wal_end()),
                cursor: Some(SnapshotCursor {
                    key: vec![CursorKey {
                        col: "id".into(),
                        val: 2.into(),
                        ty: "bigint".into(),
                    }],
                    chunk_size: 2,
                    rows_done: 2,
                    collation: None,
                }),
                row_count: None,
                completed_at: None,
            })
            .await
            .unwrap();
    }

    /// Fix round 1: a halt a start clears is noted on its job before the clear, so a restart
    /// after a crash still knows the copy came back from a halt; a cancel after that halts the
    /// table again.
    #[tokio::test]
    async fn a_halt_cleared_before_a_crash_is_kept_by_the_restart() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=4));
        ask(&w, "p~1", &["a"]).await;
        crashed_copy(&w, "p~1", "a", "SNAPSHOT_TIMEOUT").await;
        let (mut core, _control, _status) = w.core(&never(2), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        let a = &job(&w, "p~1").await.tables[0];
        assert_eq!(a.status, JobTableStatus::Running);
        assert_eq!(a.from_halt.as_deref(), Some("SNAPSHOT_TIMEOUT"), "{a:?}");
        cancel(&w, "p~1").await;
        let (mut core, _control, _status) = w.core(&never(2), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        let reason = HaltReason::CopyCancelled { job: "p~1".into() };
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1, "{halts:?}");
        assert_eq!(halts[0].reason, reason);
        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::None);
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Cancelled);
        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
    }

    /// Fix round 1: a job cancelled after a crash between a halt's clear and its job's record
    /// (its table still queued, the halt noted) halts the table again when the next start
    /// stops the copy.
    #[tokio::test]
    async fn a_cancel_after_a_crash_between_the_clear_and_the_record_halts_the_table() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=4));
        ask(&w, "p~1", &["a"]).await;
        crashed_copy(&w, "p~1", "a", "SNAPSHOT_TIMEOUT").await;
        cancel(&w, "p~1").await;
        let (mut core, _control, _status) = w.core(&never(2), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        let reason = HaltReason::CopyCancelled { job: "p~1".into() };
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1, "{halts:?}");
        assert_eq!(halts[0].reason, reason);
        assert_eq!(w.source.reads(&t("a")), 0);
        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::None);
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Cancelled);
        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
    }

    /// Fix round 1: noting a cleared halt writes its job once; a second note with the same map
    /// writes nothing, and the table stays queued.
    #[tokio::test]
    async fn noting_a_cleared_halt_twice_writes_the_job_once() {
        let w = World::new(&["a"]);
        established(&w).await;
        ask(&w, "p~1", &["a"]).await;
        let cleared = BTreeMap::from([(t("a"), "SNAPSHOT_TIMEOUT".to_owned())]);
        let before = w.docs.snapshot_job("p~1").await.unwrap().unwrap().version;
        Jobs::load(&w.docs)
            .await
            .unwrap()
            .note_cleared(&w.docs, &cleared)
            .await
            .unwrap();
        let noted = w.docs.snapshot_job("p~1").await.unwrap().unwrap();
        assert_ne!(noted.version, before, "the job is written");
        assert_eq!(noted.doc.tables[0].status, JobTableStatus::Queued);
        assert_eq!(
            noted.doc.tables[0].from_halt.as_deref(),
            Some("SNAPSHOT_TIMEOUT")
        );
        Jobs::load(&w.docs)
            .await
            .unwrap()
            .note_cleared(&w.docs, &cleared)
            .await
            .unwrap();
        let again = w.docs.snapshot_job("p~1").await.unwrap().unwrap();
        assert_eq!(
            again.version, noted.version,
            "the second note writes nothing"
        );
    }

    /// D-M13c.2, S-M13c.2: a cancelled copy of a table a truncating job had begun to copy halts
    /// it, `COPY_CANCELLED` naming the job: the job emptied the sink table, which holds part of
    /// its rows. The copy is abandoned, and the job's table ends cancelled. (A job that doesn't
    /// truncate halts nothing: `a_cancelled_copy_stops_and_the_table_streams_on`.)
    #[tokio::test]
    async fn a_cancelled_copy_of_a_truncating_job_halts_its_table() {
        let w = World::new(&["a", "b"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=4));
        ask_truncating(&w, "p~1", &["a"]).await;
        copying(&w, "p~1", "a", None).await;
        cancel(&w, "p~1").await;
        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        let reason = HaltReason::CopyCancelled { job: "p~1".into() };
        assert_eq!(st.halted.get(&t("a")), Some(&reason));
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1, "{halts:?}");
        assert_eq!(halts[0].reason, reason);
        assert!(!halts[0].copy, "the pipeline's table");
        assert_eq!(w.source.reads(&t("a")), 0);
        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::None);
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Cancelled);
        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
    }

    /// S-M13c.2: only a table the pipeline replicates halts: a truncating job's copy of a table
    /// outside the config is abandoned, and nothing halts.
    #[tokio::test]
    async fn a_cancelled_truncating_copy_outside_the_config_halts_nothing() {
        let w = World::new(&["a", "x"]);
        w.source.outside_config(&t("x"));
        established(&w).await;
        w.source.load(&t("x"), rows(1..=4));
        ask_truncating(&w, "p~1", &["x"]).await;
        copying(&w, "p~1", "x", None).await;
        cancel(&w, "p~1").await;
        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.source.reads(&t("x")), 0);
        assert_eq!(snapshot_status(&w, "x").await, SnapshotStatus::None);
        assert!(w.docs.halts().await.unwrap().tables.is_empty());
        assert!(st.halted.is_empty(), "{:?}", st.halted);
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Cancelled);
        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
    }

    /// D-M13c.2: a start that crashed after recording the `COPY_CANCELLED` halt, before the
    /// job's table ended, doesn't record it again: the next start leaves the halt as it was and
    /// ends the table cancelled.
    #[tokio::test]
    async fn a_copy_cancelled_halt_recorded_already_isn_t_recorded_again() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=4));
        ask(&w, "p~1", &["a"]).await;
        copying(&w, "p~1", "a", Some("TABLE_DROPPED")).await;
        cancel(&w, "p~1").await;
        let recorded = TableHalt {
            table: t("a"),
            reason: HaltReason::CopyCancelled { job: "p~1".into() },
            at: w.source.wal_end(),
            copy: false,
        };
        w.docs.record_halt(&recorded).await.unwrap();
        let (mut core, _control, _status) = w.core(&never(2), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        assert_eq!(w.docs.halts().await.unwrap().tables, vec![recorded]);
        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::None);
        let j = job(&w, "p~1").await;
        assert_eq!(j.status, JobStatus::Cancelled);
        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
    }

    /// A cancelled copy keeps the rows its snapshot document's cursor counts: the follower's
    /// last poll may predate them, and the lake holds them (`lake` reads `partial`, not `empty`).
    #[tokio::test]
    async fn a_cancelled_copy_keeps_the_rows_its_cursor_counts() {
        let w = World::new(&["a"]);
        established(&w).await;
        w.source.load(&t("a"), rows(1..=4));
        ask_truncating(&w, "p~1", &["a"]).await;
        copying(&w, "p~1", "a", None).await;
        assert_eq!(job(&w, "p~1").await.tables[0].rows_done, 0, "last polled");
        cancel(&w, "p~1").await;
        let (mut core, _control, _status) = w.core(&never(2), &[]);
        crate::worker::start(&mut core, None).await.unwrap();
        let j = job(&w, "p~1").await;
        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
        assert_eq!(j.tables[0].rows_done, 2, "the cursor's count");
    }
}
