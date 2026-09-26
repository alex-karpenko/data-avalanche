//! Domain types shared by every data-avalanche crate. Nothing in this crate performs I/O.

pub mod table;

pub use table::{
    INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
};
