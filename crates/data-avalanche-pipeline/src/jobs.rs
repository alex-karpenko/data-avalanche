//! Phase 0's part of the on-demand snapshot jobs (Data-Flow §On-Demand Snapshot). A job's
//! queued tables are asked for: each start snapshots them from their first row, a halted one
//! with its halt cleared and its schema baselined to the catalog's. A table whose job snapshot
//! runs keeps running whatever its snapshot mode. Phase 0 records what it did with each asked
//! table; the owner's controller follows a running one to its end.

use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use data_avalanche_core::TableRef;
use data_avalanche_state::{JobTable, JobTableStatus, PipelineDocs, SnapshotJobDoc, StateError};

/// The unfinished jobs, as a start read them.
#[derive(Debug, Default)]
pub(crate) struct Jobs {
    jobs: Vec<SnapshotJobDoc>,
}

impl Jobs {
    pub async fn load(docs: &PipelineDocs) -> Result<Self, StateError> {
        Ok(Self {
            jobs: docs.unfinished_snapshot_jobs().await?,
        })
    }

    fn tables(&self, status: JobTableStatus) -> BTreeSet<TableRef> {
        self.jobs
            .iter()
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

    /// Records what this start did with the job tables: the asked ones `started` run, and the
    /// `failed` ones (asked, or running and gone) failed with their reason. An asked table
    /// neither started nor failed stays queued.
    pub async fn record(
        &self,
        docs: &PipelineDocs,
        started: &BTreeSet<TableRef>,
        failed: &BTreeMap<TableRef, String>,
    ) -> Result<(), StateError> {
        let decided = |t: &JobTable| match t.status {
            JobTableStatus::Queued => started.contains(&t.table) || failed.contains_key(&t.table),
            JobTableStatus::Running => failed.contains_key(&t.table),
            _ => false,
        };
        for job in &self.jobs {
            if !job.tables.iter().any(decided) {
                continue;
            }
            docs.update_snapshot_job(&job.job_id, |j| {
                for t in &mut j.tables {
                    if !decided(t) {
                        continue;
                    }
                    if let Some(e) = failed.get(&t.table) {
                        t.status = JobTableStatus::Failed;
                        t.error = Some(e.clone());
                    } else {
                        t.status = JobTableStatus::Running;
                    }
                }
                j.settle(Utc::now());
            })
            .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use data_avalanche_config::Pipeline;
    use data_avalanche_core::{
        ColumnSchema, ColumnValue, ColumnValueType as V, HaltReason, Row, TableSchema,
    };
    use data_avalanche_state::{
        CursorKey, JobStatus, JobTableStatus, SnapshotCursor, SnapshotJobDoc, SnapshotStatus,
        TableHalt, TableSnapshotDoc,
    };
    use data_avalanche_testkit::FakeTxn;
    use tokio::sync::watch;

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
}
