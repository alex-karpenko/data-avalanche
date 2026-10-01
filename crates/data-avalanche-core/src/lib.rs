//! Domain types shared by every data-avalanche crate. Nothing in this crate performs I/O.
//!
//! Public-API conventions (every data-avalanche library crate follows them):
//! - every public type is re-exported at the crate root, so callers import `data_avalanche_core::X`;
//! - a type with a canonical text form implements `Display` and `FromStr`, and the two round-trip;
//! - errors are `thiserror` types named after what failed (`DecimalError`, `PositionError`), and
//!   `FromStr::Err` is that type.

pub mod change_set;
pub mod classify;
pub mod columnar;
pub mod decimal;
pub mod event;
pub mod logical;
pub mod position;
pub mod schema;
pub mod sink;
pub mod source;
pub mod table;
pub mod value;

pub use change_set::{ChangeSetError, PositionRange, SplitInfo, TableChangeSet};
pub use classify::{DeltaClass, SchemaChange, SchemaDelta, classify};
pub use columnar::{
    LIST_ELEMENT, LOGICAL_TAG, UNCHANGED_MASK_PREFIX, added_column_field, arrow_type, column_field,
    delete_schema, lineage_fields, logical_tag, unchanged_mask_field, upsert_schema,
};
pub use decimal::{Decimal, DecimalError, MAX_DECIMAL_PRECISION};
pub use event::{CdcEvent, EventShapeError, Op, SourceSeq, TransactionBoundary};
pub use logical::{LogicalType, LogicalTypeError, MAX_LIST_DEPTH};
pub use position::{PositionError, SourcePosition};
pub use schema::{ColumnSchema, HaltReason, TableSchema, canonical_logical, hop2_compatible};
pub use sink::{
    CommitMarker, CommitOutcome, EvolveOutcome, PartitionField, PartitionTransform, SchemaOp, Sink,
    SinkCapabilities, SinkError, SinkKind, SinkTableState, TableSpec, TransformKind, WriteMode,
};
pub use source::{
    ChangeStream, Chunk, KeysetCursor, Preflight, Prepared, RescanReport, ResolvedTables,
    SchemaHint, SnapshotReason, SourceConnector, SourceError, StartPlan, StreamItem,
    identifier_values,
};
pub use table::{
    INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
};
pub use value::{ColumnValue, ColumnValueType, Row, time_text};
