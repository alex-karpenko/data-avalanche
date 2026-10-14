//! The MySQL and MariaDB realization of `data_avalanche_core::SourceConnector` (roadmap M7): the
//! row-based binlog over `mysql_async` in GTID or file/position mode (S-MY10), binlog values
//! decoded to `ColumnValue`s (`Type_Mapping.md`), catalog channels A, C and D over
//! `information_schema` (`Schema_Change…` §3) and keyset snapshot chunks read with a locking read
//! (`Snapshot_Resumability…` §1.1, DA-SNAP-10).

mod connect;
mod connector;
mod error;
pub mod server;

pub use connect::SESSION_SETUP;
pub use connector::MySqlConnector;
pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
