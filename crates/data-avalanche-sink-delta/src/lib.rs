//! The Delta Lake realization of the `data_avalanche_core::Sink` trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.

mod config;
pub mod convert;
mod error;
pub mod marker;
#[allow(dead_code)] // `DeltaSink` (Task 9) is the caller
mod table;
#[cfg(test)]
mod test_support;
pub mod types;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
pub use table::IDENTIFIER_KEY;
