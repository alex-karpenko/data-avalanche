//! [`DeltaSink`]: the `Sink` contract (core `sink.rs`, Data-Flow Phase 4) on Delta tables.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use data_avalanche_core::{
    CommitMarker, CommitOutcome, EvolveOutcome, SchemaDelta, SchemaOp, Sink, SinkCapabilities,
    SinkError, SinkKind, SinkTableState, SourcePosition, TableChangeSet, TableRef, TableSpec,
    TransformKind, WriteMode,
};
use deltalake::DeltaTableError;
use deltalake::kernel::Transaction;
use deltalake::kernel::transaction::{CommitConflictError, CommitProperties, TransactionError};
use deltalake::operations::merge::MergeMetrics;
use tokio::sync::OwnedMutexGuard;

use crate::DeltaSinkConfig;
use crate::error::{permanent, sink_error};
use crate::evolve::evolve;
use crate::marker::{app_id, commit_metadata};
use crate::table::TableHandle;
use crate::write::{check_columns, marker_only, merge, truncate};

/// How many times one `commit` call re-reads the table and tries again after losing a race.
pub const COMMIT_ATTEMPTS: usize = 3;

/// What the sink has done since it was built (the merge metrics of Data-Flow §Delta Lake
/// realization, item 4; M10 exports them).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeltaStats {
    pub commits: u64,
    pub skipped: u64,
    pub fenced: u64,
    /// Attempts repeated after a lost race.
    pub retries: u64,
    pub rows_inserted: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
    /// Rows rewritten unchanged because they share a file with a changed row
    /// (`data_avalanche_sink_merge_rows_copied`).
    pub rows_copied: u64,
    pub files_added: u64,
    /// Files a MERGE replaced (`data_avalanche_sink_merge_files_rewritten`).
    pub files_removed: u64,
}

impl DeltaStats {
    fn add(&mut self, m: &MergeMetrics) {
        self.rows_inserted += m.num_target_rows_inserted as u64;
        self.rows_updated += m.num_target_rows_updated as u64;
        self.rows_deleted += m.num_target_rows_deleted as u64;
        self.rows_copied += m.num_target_rows_copied as u64;
        self.files_added += m.num_target_files_added as u64;
        self.files_removed += m.num_target_files_removed as u64;
    }
}

type Slot = Arc<tokio::sync::Mutex<Option<TableHandle>>>;

/// The Delta Lake sink. One per pipeline; calls on one table run one at a time.
#[derive(Debug)]
pub struct DeltaSink {
    config: DeltaSinkConfig,
    capabilities: SinkCapabilities,
    options: HashMap<String, String>,
    tables: Mutex<HashMap<TableRef, Slot>>,
    stats: Mutex<DeltaStats>,
}

enum Step {
    Done(CommitOutcome),
    /// Lost a race with another commit: re-read the table and try again.
    Retry(String),
}

/// A lost race is retried; any other failure is the call's result. The races are the conflicts
/// another commit landing first causes (deltalake-core 1.0 `conflict_checker.rs`; a
/// `ProtocolChanged` re-reads, and an unsupported new protocol then fails as
/// `UnsupportedTableFeatures`) and a version taken first. A `Predicate` conflict or a corrupt
/// snapshot fails the same way again, so `sink_error` classes it.
fn race_or_fail(e: DeltaTableError) -> Result<Step, SinkError> {
    use CommitConflictError as C;
    use TransactionError as T;
    match &e {
        DeltaTableError::Transaction {
            source:
                T::CommitConflict(
                    C::ConcurrentAppend
                    | C::ConcurrentDeleteRead
                    | C::ConcurrentDeleteDelete
                    | C::MetadataChanged
                    | C::ConcurrentTransaction
                    | C::ProtocolChanged(_),
                )
                | T::MaxCommitAttempts(_)
                | T::VersionAlreadyExists(_),
        }
        | DeltaTableError::VersionAlreadyExists(_) => Ok(Step::Retry(e.to_string())),
        _ => Err(sink_error(e)),
    }
}

impl DeltaSink {
    pub fn new(config: DeltaSinkConfig) -> Self {
        static HANDLERS: Once = Once::new();
        HANDLERS.call_once(|| {
            deltalake::aws::register_handlers(None);
            deltalake::gcp::register_handlers(None);
            deltalake::azure::register_handlers(None);
        });
        let options = config.location.storage_options(std::env::vars());
        Self {
            capabilities: SinkCapabilities {
                write_modes: [WriteMode::Mirror].into(),
                schema_ops: [SchemaOp::AddColumn].into(),
                partition_transforms: [TransformKind::Identity].into(),
            },
            options,
            config,
            tables: Mutex::default(),
            stats: Mutex::default(),
        }
    }

    pub fn stats(&self) -> DeltaStats {
        *self.stats.lock().expect("no panic while holding the stats")
    }

    fn count(&self, f: impl FnOnce(&mut DeltaStats)) {
        f(&mut self.stats.lock().expect("no panic while holding the stats"));
    }

    /// The table's handle, opened on first use and held for the call.
    async fn lock(
        &self,
        table: &TableRef,
    ) -> Result<OwnedMutexGuard<Option<TableHandle>>, SinkError> {
        let slot = {
            let mut tables = self.tables.lock().expect("no panic while holding the map");
            Arc::clone(tables.entry(table.clone()).or_default())
        };
        let mut guard = slot.lock_owned().await;
        if guard.is_none() {
            let url = self.config.location.table_url(table);
            *guard = Some(TableHandle::open(url, self.options.clone()).await?);
        }
        Ok(guard)
    }

    /// The table's rows in Hop 2 terms and identifier order (every column, lineage included);
    /// `None` when the table doesn't exist. The Delta counterpart of `MemorySink::content`.
    pub async fn content(&self, table: &TableRef) -> Result<Option<RecordBatch>, SinkError> {
        let mut guard = self.lock(table).await?;
        let h = guard.as_mut().expect("opened by lock");
        if !h.refresh().await? {
            return Ok(None);
        }
        h.content().await.map(Some)
    }

    fn props(
        &self,
        epoch: u64,
        position: Option<&SourcePosition>,
    ) -> Result<CommitProperties, SinkError> {
        let version =
            i64::try_from(epoch).map_err(|_| permanent(format!("owner epoch {epoch}")))?;
        Ok(CommitProperties::default()
            .with_application_transaction(Transaction::new(
                app_id(&self.config.pipeline_id),
                version,
            ))
            .with_metadata(commit_metadata(&self.config.pipeline_id, epoch, position)))
    }

    /// One try at a commit, from the fence to the marker. `refresh` re-reads the table first;
    /// only a test leaves it stale, to lose a race on purpose.
    async fn attempt(
        &self,
        h: &mut TableHandle,
        cs: &TableChangeSet,
        marker: &CommitMarker,
        refresh: bool,
    ) -> Result<Step, SinkError> {
        if refresh && !h.refresh().await? {
            return Err(SinkError::NoTable {
                table: cs.table.clone(),
            });
        }
        let pipeline = &self.config.pipeline_id;
        let epoch = h.fence_epoch(pipeline).await?;
        if let Some(current) = epoch.filter(|e| *e > marker.owner_epoch) {
            return Ok(Step::Done(CommitOutcome::Fenced {
                current_epoch: current,
            }));
        }
        let stored = h.stored_position(pipeline).await?;
        if let Some(position) = &stored {
            let stored_marker = CommitMarker {
                owner_epoch: epoch.unwrap_or(marker.owner_epoch),
                committed_position: position.clone(),
                pipeline_id: pipeline.clone(),
            };
            if stored_marker.covers(cs)? {
                return Ok(Step::Done(CommitOutcome::SkippedCovered));
            }
        }
        let snapshot = h.table().snapshot().map_err(sink_error)?;
        let types = check_columns(cs, snapshot.schema().as_ref())?;
        let position = match &stored {
            Some(p) => p.union(&cs.position.end)?,
            None => cs.position.end.clone(),
        };
        let mut table = h.table().clone();
        if cs.truncate {
            // The clear carries the fence but no position: a crash before the MERGE replays both.
            table = match truncate(table, self.props(marker.owner_epoch, None)?).await {
                Ok(t) => t,
                Err(e) => return race_or_fail(e),
            };
            h.set_table(table.clone());
        }
        let before = table.version();
        if cs.upsert_rows() + cs.delete_rows() > 0 {
            let props = self.props(marker.owner_epoch, Some(&position))?;
            match merge(table, cs, &types, props, self.config.merge_memory_limit).await {
                Ok((t, metrics)) => {
                    self.count(|s| s.add(&metrics));
                    table = t;
                }
                Err(e) => return race_or_fail(e),
            }
            h.set_table(table.clone());
        }
        let version = if table.version() != before {
            table.version().expect("a written table is loaded")
        } else {
            // Nothing changed a row: a marker-only commit still moves the position.
            let props = self.props(marker.owner_epoch, Some(&position))?;
            let epoch = i64::try_from(marker.owner_epoch).unwrap_or(i64::MAX);
            match marker_only(&table, props, &app_id(pipeline), epoch).await {
                Ok(v) => v,
                Err(e) => return race_or_fail(e),
            }
        };
        h.remember(version, position);
        Ok(Step::Done(CommitOutcome::Committed { version }))
    }
}

#[async_trait]
impl Sink for DeltaSink {
    fn kind(&self) -> SinkKind {
        SinkKind::Delta
    }

    fn capabilities(&self) -> &SinkCapabilities {
        &self.capabilities
    }

    async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError> {
        let mut guard = self.lock(table).await?;
        let h = guard.as_mut().expect("opened by lock");
        if !h.refresh().await? {
            if let Some(reason) = h.foreign_format().await? {
                return Err(permanent(reason));
            }
            return Ok(SinkTableState {
                exists: false,
                schema: None,
                marker: None,
            });
        }
        let pipeline = &self.config.pipeline_id;
        let epoch = h.fence_epoch(pipeline).await?;
        let position = h.stored_position(pipeline).await?;
        Ok(SinkTableState {
            exists: true,
            schema: Some(h.schema()?),
            marker: epoch
                .zip(position)
                .map(|(owner_epoch, committed_position)| CommitMarker {
                    owner_epoch,
                    committed_position,
                    pipeline_id: pipeline.clone(),
                }),
        })
    }

    async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError> {
        let mut guard = self.lock(&spec.table).await?;
        let h = guard.as_mut().expect("opened by lock");
        if h.refresh().await? {
            return Ok(());
        }
        if let Some(reason) = h.foreign_format().await? {
            return Err(permanent(reason));
        }
        h.create(spec, &self.config.table).await
    }

    async fn evolve(
        &self,
        table: &TableRef,
        delta: &SchemaDelta,
    ) -> Result<EvolveOutcome, SinkError> {
        let mut guard = self.lock(table).await?;
        let h = guard.as_mut().expect("opened by lock");
        if !h.refresh().await? {
            return Err(SinkError::NoTable {
                table: table.clone(),
            });
        }
        let (t, outcome) =
            evolve(h.table().clone(), delta, self.config.table.timestamp_ntz_as).await?;
        h.set_table(t);
        Ok(outcome)
    }

    async fn commit(
        &self,
        cs: TableChangeSet,
        marker: CommitMarker,
    ) -> Result<CommitOutcome, SinkError> {
        cs.validate()?;
        if marker.committed_position != cs.position.end {
            return Err(permanent(format!(
                "the marker's position {} isn't the change set's end {}",
                marker.committed_position, cs.position.end
            )));
        }
        if marker.pipeline_id != self.config.pipeline_id {
            return Err(permanent(format!(
                "the marker is pipeline {:?}'s, this sink writes for {:?}",
                marker.pipeline_id, self.config.pipeline_id
            )));
        }
        let mut guard = self.lock(&cs.table).await?;
        let h = guard.as_mut().expect("opened by lock");
        let mut lost = String::new();
        for attempt in 0..COMMIT_ATTEMPTS {
            if attempt > 0 {
                self.count(|s| s.retries += 1);
            }
            match self.attempt(h, &cs, &marker, true).await? {
                Step::Done(outcome) => {
                    self.count(|s| match outcome {
                        CommitOutcome::Committed { .. } => s.commits += 1,
                        CommitOutcome::SkippedCovered => s.skipped += 1,
                        CommitOutcome::Fenced { .. } => s.fenced += 1,
                    });
                    return Ok(outcome);
                }
                Step::Retry(reason) => lost = reason,
            }
        }
        Err(SinkError::Transient {
            reason: format!("the commit lost {COMMIT_ATTEMPTS} races in a row, last: {lost}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::position::Lsn;

    use super::*;
    use crate::TableDefaults;
    use crate::test_support::{change_set, local, marker, orders, spec, upserts};

    fn sink(dir: &tempfile::TempDir) -> DeltaSink {
        DeltaSink::new(DeltaSinkConfig {
            pipeline_id: "p".into(),
            location: local(dir),
            table: TableDefaults::default(),
            merge_memory_limit: None,
        })
    }

    fn rows(end: u64, ids: &[i64]) -> TableChangeSet {
        let full = data_avalanche_core::upsert_schema(&orders());
        let rows: Vec<_> = ids.iter().map(|i| (*i, Some("x"))).collect();
        change_set(end, vec![upserts(&full, &rows, None)], vec![])
    }

    /// Commits through `attempt` on the sink's current handle without re-reading it first.
    async fn stale_attempt(s: &DeltaSink, cs: &TableChangeSet, epoch: u64) -> Step {
        let mut guard = s.lock(&cs.table).await.unwrap();
        let h = guard.as_mut().unwrap();
        s.attempt(h, cs, &marker(epoch, cs), false).await.unwrap()
    }

    #[tokio::test]
    async fn a_zombie_that_loses_the_race_is_fenced_on_the_retry() {
        let dir = tempfile::tempdir().unwrap();
        let (zombie, owner) = (sink(&dir), sink(&dir));
        zombie.ensure_table(&spec(&orders())).await.unwrap();
        let first = rows(10, &[1]);
        zombie
            .commit(first.clone(), marker(5, &first))
            .await
            .unwrap();
        // The zombie holds the table at epoch 5; the new owner commits at epoch 6.
        let theirs = rows(20, &[2]);
        owner
            .commit(theirs.clone(), marker(6, &theirs))
            .await
            .unwrap();
        // Without re-reading, the fence pre-check passes and delta-rs refuses the commit: the
        // same app id committed since the zombie's snapshot (probe P4, stale case).
        let late = rows(15, &[3]);
        match stale_attempt(&zombie, &late, 5).await {
            Step::Retry(reason) => assert!(reason.contains("Concurrent transaction"), "{reason}"),
            Step::Done(o) => panic!("{o:?}"),
        }
        // The retry re-reads the table and the pre-check fences it.
        let out = zombie.commit(late.clone(), marker(5, &late)).await.unwrap();
        assert_eq!(out, CommitOutcome::Fenced { current_epoch: 6 });
        assert_eq!(zombie.stats().fenced, 1);
    }

    #[tokio::test]
    async fn a_race_with_a_foreign_optimize_is_retried_and_lands() {
        let dir = tempfile::tempdir().unwrap();
        let s = sink(&dir);
        s.ensure_table(&spec(&orders())).await.unwrap();
        for (end, id) in [(10, 1), (20, 2)] {
            let cs = rows(end, &[id]);
            s.commit(cs.clone(), marker(1, &cs)).await.unwrap();
        }
        // Someone compacts the table behind the sink's back.
        let url = local(&dir).table_url(&orders().table().clone());
        let t = deltalake::open_table(url).await.unwrap();
        t.optimize().await.unwrap();
        let update = rows(30, &[1]);
        match stale_attempt(&s, &update, 1).await {
            Step::Retry(reason) => assert!(reason.contains("commit"), "{reason}"),
            Step::Done(o) => panic!("{o:?}"),
        }
        let out = s.commit(update.clone(), marker(1, &update)).await.unwrap();
        assert!(matches!(out, CommitOutcome::Committed { .. }), "{out:?}");
        let state = s.table_state(&update.table).await.unwrap();
        assert_eq!(
            state.marker.unwrap().committed_position,
            SourcePosition::Pg(Lsn(30))
        );
    }

    #[test]
    fn only_lost_races_are_retried() {
        use CommitConflictError as C;
        let conflict = |c| DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(c),
        };
        assert!(matches!(
            race_or_fail(conflict(C::ConcurrentTransaction)),
            Ok(Step::Retry(_))
        ));
        assert!(matches!(
            race_or_fail(DeltaTableError::VersionAlreadyExists(3)),
            Ok(Step::Retry(_))
        ));
        let predicate = conflict(C::Predicate {
            source: "a bad predicate".into(),
        });
        assert!(matches!(
            race_or_fail(predicate),
            Err(SinkError::Permanent { .. })
        ));
    }

    /// A set without the lineage columns fails the same way on every replay: `Permanent`, and no
    /// version is written.
    #[tokio::test]
    async fn a_set_without_the_lineage_columns_is_permanent() {
        let dir = tempfile::tempdir().unwrap();
        let s = sink(&dir);
        s.ensure_table(&spec(&orders())).await.unwrap();
        let fields: Vec<_> = data_avalanche_core::upsert_schema(&orders())
            .fields()
            .iter()
            .filter(|f| !f.name().starts_with('_'))
            .cloned()
            .collect();
        let narrow = Arc::new(arrow_schema::Schema::new(fields));
        let mut cs = change_set(10, vec![upserts(&narrow, &[(1, Some("a"))], None)], vec![]);
        cs.schema = narrow;
        let err = s.commit(cs.clone(), marker(1, &cs)).await.unwrap_err();
        assert!(matches!(err, SinkError::Permanent { .. }), "{err}");
        let state = s.table_state(&cs.table).await.unwrap();
        assert_eq!(state.marker, None, "nothing committed");
    }

    #[tokio::test]
    async fn a_marker_of_another_pipeline_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let s = sink(&dir);
        s.ensure_table(&spec(&orders())).await.unwrap();
        let cs = rows(10, &[1]);
        let mut m = marker(1, &cs);
        m.pipeline_id = "other".into();
        let err = s.commit(cs, m).await.unwrap_err();
        assert!(err.to_string().contains("\"other\""), "{err}");
    }
}
