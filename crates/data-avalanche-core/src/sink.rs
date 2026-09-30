//! The sink contract (Multi_Sink §6.3–6.4, Data-Flow §Sink contract): the one trait every table
//! format implements, the capability vocabulary checked at pipeline start, and the fence and guard
//! rules every implementation applies the same way.

use std::collections::BTreeSet;
use std::fmt;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use thiserror::Error;

use crate::{
    ChangeSetError, HaltReason, PositionError, SchemaDelta, SourcePosition, TableChangeSet,
    TableRef, TableSchema, upsert_schema,
};

/// The table format a sink writes (`sink.type`). The text is what pipeline state records as
/// `sink_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SinkKind {
    Delta,
    Iceberg,
}

impl SinkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delta => "delta",
            Self::Iceberg => "iceberg",
        }
    }
}

impl fmt::Display for SinkKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WriteMode {
    Mirror,
    Changelog,
}

/// Schema operations a sink can apply in place (`evolve` → `Applied`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SchemaOp {
    AddColumn,
    DropColumn,
    RenameColumn,
    WidenType,
    DropNotNull,
}

/// A partition transform (config `tables.partition_by`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartitionTransform {
    Identity,
    Bucket { n: u32 },
    Truncate { width: u32 },
    Year,
    Month,
    Day,
    Hour,
}

/// A [`PartitionTransform`] without its parameter: what a sink declares it supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TransformKind {
    Identity,
    Bucket,
    Truncate,
    Year,
    Month,
    Day,
    Hour,
}

impl PartitionTransform {
    pub fn kind(self) -> TransformKind {
        match self {
            Self::Identity => TransformKind::Identity,
            Self::Bucket { .. } => TransformKind::Bucket,
            Self::Truncate { .. } => TransformKind::Truncate,
            Self::Year => TransformKind::Year,
            Self::Month => TransformKind::Month,
            Self::Day => TransformKind::Day,
            Self::Hour => TransformKind::Hour,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionField {
    pub column: String,
    pub transform: PartitionTransform,
}

/// What a sink supports, fixed per build and backend (Multi_Sink §6.4).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkCapabilities {
    pub write_modes: BTreeSet<WriteMode>,
    pub schema_ops: BTreeSet<SchemaOp>,
    pub partition_transforms: BTreeSet<TransformKind>,
}

impl SinkCapabilities {
    /// The pipeline-start assertion (Data-Flow Phase 0 step 4): `None` when the sink supports the
    /// write mode and every partition transform, else the first missing capability, named.
    pub fn refusal(&self, mode: WriteMode, partition_by: &[PartitionField]) -> Option<String> {
        if !self.write_modes.contains(&mode) {
            return Some(format!("write mode {mode:?} isn't supported by this sink"));
        }
        partition_by
            .iter()
            .find(|p| !self.partition_transforms.contains(&p.transform.kind()))
            .map(|p| {
                format!(
                    "partition transform {:?} (column {:?}) isn't supported by this sink",
                    p.transform, p.column
                )
            })
    }
}

/// What `ensure_table` creates when the table is absent.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSpec {
    pub table: TableRef,
    /// [`upsert_schema`] of the source table.
    pub schema: SchemaRef,
    pub identifier: Vec<String>,
    pub partition_by: Vec<PartitionField>,
}

impl TableSpec {
    pub fn new(
        schema: &TableSchema,
        partition_by: Vec<PartitionField>,
    ) -> Result<Self, HaltReason> {
        if let Some(p) = partition_by
            .iter()
            .find(|p| schema.column(&p.column).is_none())
        {
            return Err(HaltReason::SchemaMismatch {
                column: p.column.clone(),
                detail: "a partition column that isn't in the table".into(),
            });
        }
        Ok(Self {
            table: schema.table().clone(),
            schema: upsert_schema(schema),
            identifier: schema.identifier().to_vec(),
            partition_by,
        })
    }
}

/// Written atomically with every commit, read back by `table_state` (Delivery §3, Clustering §19).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitMarker {
    pub owner_epoch: u64,
    pub committed_position: SourcePosition,
    pub pipeline_id: String,
}

impl CommitMarker {
    /// The fence: a commit carrying `ours` must be refused when the table's stored marker (`self`)
    /// was written by a newer owner.
    pub fn fences(&self, ours: &CommitMarker) -> bool {
        self.owner_epoch > ours.owner_epoch
    }

    /// The position guard: the stored marker (`self`) says the table already holds `cs`, so the
    /// commit is skipped (Delivery §3). Committing again would only repeat an idempotent write,
    /// except for replayed history, which the guard is what keeps out.
    ///
    /// A split slice ends at its pinned `txn_start`, which the marker equals from the transaction's
    /// first slice on, so "covers the end" can't tell a slice still owed from one already applied.
    /// It is skipped only when the marker is strictly past `txn_start`: the transaction's commit (or
    /// a later one) has landed, and re-applying the slice would roll its rows back to their state
    /// inside the transaction.
    pub fn covers(&self, cs: &TableChangeSet) -> Result<bool, PositionError> {
        let stored = &self.committed_position;
        match &cs.split {
            Some(split) => Ok(stored.covers(&split.txn_start)? && !split.txn_start.covers(stored)?),
            None => stored.covers(&cs.position.end),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SinkTableState {
    /// The sink has the table (Delta: a `_delta_log`; Iceberg: a catalog entry).
    pub exists: bool,
    pub schema: Option<SchemaRef>,
    /// The marker of the latest commit this pipeline made to the table.
    pub marker: Option<CommitMarker>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvolveOutcome {
    Applied,
    /// Applied another way, with a visible cost (e.g. soft-drop: the column stays, written NULL).
    Degraded {
        warning: String,
    },
    /// The pipeline halts the table (`Schema_Change…` §7.1).
    Unsupported {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed {
        version: u64,
    },
    /// The guard found the range already committed; nothing was written.
    SkippedCovered,
    /// A newer owner has written the table; nothing was written and this worker must stop the
    /// pipeline (Clustering §19).
    Fenced {
        current_epoch: u64,
    },
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error(transparent)]
    InvalidChangeSet(#[from] ChangeSetError),
    #[error(transparent)]
    Position(#[from] PositionError),
    #[error("table {table} doesn't exist in the sink")]
    NoTable { table: TableRef },
    /// Worth retrying: the store or the catalog didn't answer, or a commit lost a race.
    #[error("transient sink failure: {reason}")]
    Transient { reason: String },
    #[error("sink failure: {reason}")]
    Permanent { reason: String },
}

impl SinkError {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient { .. })
    }
}

/// One table format (Multi_Sink §6.3). Every method is per table; `commit` is atomic per table,
/// idempotent under replay, and applies the fence ([`CommitMarker::fences`]) before the guard
/// ([`CommitMarker::covers`]).
#[async_trait]
pub trait Sink: Send + Sync {
    fn kind(&self) -> SinkKind;
    fn capabilities(&self) -> &SinkCapabilities;
    async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError>;
    /// Creates the table when it's absent; a present table is left as it is.
    async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError>;
    async fn evolve(
        &self,
        table: &TableRef,
        delta: &SchemaDelta,
    ) -> Result<EvolveOutcome, SinkError>;
    async fn commit(
        &self,
        cs: TableChangeSet,
        marker: CommitMarker,
    ) -> Result<CommitOutcome, SinkError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::Lsn;
    use crate::{ColumnSchema, ColumnValueType as V, PositionRange, SplitInfo};

    fn table() -> TableSchema {
        TableSchema::new(
            TableRef::new("public", "orders"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("at", V::Timestamp { with_tz: true }, "timestamptz"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn marker(epoch: u64, lsn: u64) -> CommitMarker {
        CommitMarker {
            owner_epoch: epoch,
            committed_position: SourcePosition::Pg(Lsn(lsn)),
            pipeline_id: "p".into(),
        }
    }

    fn change_set(end: u64, split: Option<SplitInfo>) -> TableChangeSet {
        let t = table();
        TableChangeSet {
            table: t.table().clone(),
            schema: upsert_schema(&t),
            identifier: vec!["id".into()],
            truncate: false,
            upserts: vec![],
            deletes: vec![],
            position: PositionRange {
                start: SourcePosition::Pg(Lsn(1)),
                end: SourcePosition::Pg(Lsn(end)),
            },
            split,
        }
    }

    #[test]
    fn sink_kind_text_is_the_recorded_sink_type() {
        assert_eq!(SinkKind::Delta.to_string(), "delta");
        assert_eq!(SinkKind::Iceberg.as_str(), "iceberg");
    }

    #[test]
    fn capabilities_name_what_they_lack() {
        let caps = SinkCapabilities {
            write_modes: [WriteMode::Mirror].into(),
            partition_transforms: [TransformKind::Identity].into(),
            ..Default::default()
        };
        let day = PartitionField {
            column: "at".into(),
            transform: PartitionTransform::Day,
        };
        let id = PartitionField {
            column: "id".into(),
            transform: PartitionTransform::Identity,
        };
        assert_eq!(
            caps.refusal(WriteMode::Mirror, std::slice::from_ref(&id)),
            None
        );
        let r = caps.refusal(WriteMode::Changelog, &[]).unwrap();
        assert!(r.contains("Changelog"), "{r}");
        let r = caps.refusal(WriteMode::Mirror, &[id, day]).unwrap();
        assert!(r.contains("Day") && r.contains("\"at\""), "{r}");
    }

    #[test]
    fn table_spec_takes_the_upsert_schema_and_checks_partition_columns() {
        let spec = TableSpec::new(
            &table(),
            vec![PartitionField {
                column: "at".into(),
                transform: PartitionTransform::Month,
            }],
        )
        .unwrap();
        assert_eq!(spec.schema, upsert_schema(&table()));
        assert_eq!(spec.identifier, ["id"]);
        let err = TableSpec::new(
            &table(),
            vec![PartitionField {
                column: "nope".into(),
                transform: PartitionTransform::Identity,
            }],
        )
        .unwrap_err();
        assert_eq!(err.code(), "SCHEMA_MISMATCH");
    }

    #[test]
    fn only_a_newer_owner_fences() {
        let stored = marker(5, 100);
        assert!(stored.fences(&marker(4, 200)));
        assert!(
            !stored.fences(&marker(5, 200)),
            "the same owner commits again"
        );
        assert!(
            !stored.fences(&marker(6, 50)),
            "a takeover commits over the old owner"
        );
    }

    #[test]
    fn the_guard_skips_covered_ranges() {
        let stored = marker(1, 100);
        assert!(stored.covers(&change_set(100, None)).unwrap());
        assert!(stored.covers(&change_set(60, None)).unwrap());
        assert!(!stored.covers(&change_set(101, None)).unwrap());
    }

    #[test]
    fn a_slice_is_skipped_only_once_its_transaction_has_landed() {
        // A slice of a transaction that started at 90 and commits at 110.
        let skips = |stored: u64| {
            let split = SplitInfo {
                txn_start: SourcePosition::Pg(Lsn(90)),
                slice: 2,
            };
            marker(1, stored)
                .covers(&change_set(90, Some(split)))
                .unwrap()
        };
        assert!(!skips(90), "pinned at the start: the slice may be owed");
        assert!(!skips(80), "nothing of the transaction has landed");
        assert!(skips(110), "the transaction landed: replay would roll back");
        assert!(skips(200), "later commits landed: replay would roll back");
    }

    #[test]
    fn a_guard_across_dialects_is_an_error() {
        let mut stored = marker(1, 100);
        stored.committed_position =
            SourcePosition::parse_tagged("binlog:mysql-bin.000001:4").unwrap();
        assert!(stored.covers(&change_set(100, None)).is_err());
    }

    #[test]
    fn the_trait_is_object_safe() {
        fn _takes(_: &dyn Sink) {}
    }
}
