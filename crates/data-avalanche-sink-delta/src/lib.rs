//! The Delta Lake realization of the `data_avalanche_core::Sink` trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.

mod config;
pub mod types;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
