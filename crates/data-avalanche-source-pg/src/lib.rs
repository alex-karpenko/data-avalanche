//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

pub mod catalog;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod connect;
mod connector;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod error;
pub mod pgoutput;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod position;
mod slot;
#[cfg(test)]
mod test_support;
mod tls;
pub mod types;
pub mod value;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
pub use connector::{MIN_SERVER_VERSION, PgConnector};
