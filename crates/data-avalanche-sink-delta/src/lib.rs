//! The Delta Lake realization of the [`data_avalanche_core::Sink`] trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.
//!
//! - [`DeltaSink`] commits each `TableChangeSet` as one MERGE, fenced by a `txn`
//!   app-transaction and positioned by `commitInfo` ([`marker`]).
//! - [`types`] and [`convert`] are Hop 3b: Hop 2 Arrow ↔ Delta types and values.
//! - Tables are path-based (no catalog in v1), under the location config §10 names.

mod config;
pub mod convert;
mod error;
mod evolve;
pub mod marker;
mod sink;
mod table;
#[cfg(test)]
mod test_support;
pub mod types;
mod write;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
pub use sink::{COMMIT_ATTEMPTS, DeltaSink, DeltaStats};
pub use table::IDENTIFIER_KEY;
pub use write::DELETE_FLAG;
