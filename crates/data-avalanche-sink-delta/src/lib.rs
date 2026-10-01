//! The Delta Lake realization of the [`data_avalanche_core::Sink`] trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.
//!
//! - [`DeltaSink`] commits each `TableChangeSet` as one MERGE, fenced by a `txn`
//!   app-transaction and positioned by `commitInfo` ([`app_id`], [`POSITION_KEY`]).
//! - Hop 3b (Hop 2 Arrow ↔ Delta types and values) is internal; a column's logical type is
//!   kept in its field metadata under [`TYPE_KEY`], and dates and timestamps are clamped to
//!   Delta's range ([`DELTA_MIN_DATE`] … [`DELTA_MAX_MICROS`]).
//! - Tables are path-based (no catalog in v1), under the location config §10 names.

mod config;
mod convert;
mod error;
mod evolve;
mod marker;
mod sink;
mod table;
#[cfg(test)]
mod test_support;
mod types;
mod write;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
pub use convert::{DELTA_MAX_DATE, DELTA_MAX_MICROS, DELTA_MIN_DATE, DELTA_MIN_MICROS};
pub use marker::{EPOCH_KEY, PIPELINE_KEY, POSITION_KEY, app_id};
pub use sink::{COMMIT_ATTEMPTS, DeltaSink, DeltaStats};
pub use table::IDENTIFIER_KEY;
pub use types::TYPE_KEY;
pub use write::DELETE_FLAG;
