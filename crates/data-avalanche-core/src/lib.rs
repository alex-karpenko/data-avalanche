//! Domain types shared by every data-avalanche crate. Nothing in this crate performs I/O.

pub mod decimal;
pub mod logical;
pub mod position;
pub mod table;
pub mod value;

pub use decimal::{Decimal, DecimalError, MAX_DECIMAL_PRECISION};
pub use logical::{LogicalType, LogicalTypeError};
pub use table::{
    INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
};
pub use value::{ColumnValue, ColumnValueType, Row};
