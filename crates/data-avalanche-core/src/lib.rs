//! Domain types shared by every data-avalanche crate. Nothing in this crate performs I/O.
//!
//! Public-API conventions (every data-avalanche library crate follows them):
//! - every public type is re-exported at the crate root, so callers import `data_avalanche_core::X`;
//! - a type with a canonical text form implements `Display` and `FromStr`, and the two round-trip;
//! - errors are `thiserror` types named after what failed (`DecimalError`, `PositionError`), and
//!   `FromStr::Err` is that type.

pub mod decimal;
pub mod event;
pub mod logical;
pub mod position;
pub mod table;
pub mod value;

pub use decimal::{Decimal, DecimalError, MAX_DECIMAL_PRECISION};
pub use event::{CdcEvent, EventShapeError, Op, SourceSeq, TransactionBoundary};
pub use logical::{LogicalType, LogicalTypeError, MAX_LIST_DEPTH};
pub use position::{PositionError, SourcePosition};
pub use table::{
    INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
};
pub use value::{ColumnValue, ColumnValueType, Row};
