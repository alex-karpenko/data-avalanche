//! The MySQL and MariaDB realization of `data_avalanche_core::SourceConnector` (roadmap M7): the
//! row-based binlog over `mysql_async` in GTID or file/position mode (S-MY10), binlog values
//! decoded to `ColumnValue`s (`Type_Mapping.md`), catalog channels A, C and D over
//! `information_schema` (`Schema_Change…` §3) and keyset snapshot chunks read with a locking read
//! (`Snapshot_Resumability…` §1.1, DA-SNAP-10).

pub mod binlog;
pub mod catalog;
mod connect;
mod connector;
mod error;
pub mod json;
pub mod position;
pub mod server;
pub mod types;
pub mod value;

pub use binlog::{MariaGtidEvent, Offsets, decompress, rotate_name};
pub use catalog::{Described, Listed, Notes, Planned, Selection, plan_table, select, table_key};
pub use connect::SESSION_SETUP;
pub use connector::MySqlConnector;
pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
pub use types::{CatalogColumn, ColumnPlan, members, plan_column};
pub use value::{Charset, Decode, Input, ValueError, decode, decode_after, int_type};
