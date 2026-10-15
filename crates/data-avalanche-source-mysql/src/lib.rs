//! The MySQL and MariaDB realization of `data_avalanche_core::SourceConnector` (roadmap M7): the
//! row-based binlog over `mysql_async` in GTID or file/position mode (S-MY10), binlog values
//! decoded to `ColumnValue`s (`Type_Mapping.md`), catalog channels A, C and D over
//! `information_schema` (`Schema_Change…` §3) and keyset snapshot chunks read with a locking read
//! (`Snapshot_Resumability…` §1.1, DA-SNAP-10).

pub mod binlog;
pub mod catalog;
mod connect;
mod connector;
pub mod ddl;
mod error;
pub mod json;
pub mod position;
pub mod schema;
pub mod server;
mod snapshot;
pub mod start;
mod stream;
pub mod types;
pub mod value;

pub use binlog::{MariaGtidEvent, Offsets, decompress, rotate_name};
pub use catalog::{Described, Listed, Notes, Planned, Selection, plan_table, select, table_key};
pub use connect::SESSION_SETUP;
pub use connector::MySqlConnector;
pub use ddl::{Altered, Ddl, Renamed, Scope, classify, renamed, shadow_of};
pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
pub use schema::{diff, narrowed, scope, scope_of};
pub use server::{Flavour, Fold, Settings, check_settings, grants_replication, version_of};
pub use start::{dialect_change, purge_action, same_server};
pub use stream::MySqlStream;
pub use types::{CatalogColumn, ColumnPlan, members, plan_column};
pub use value::{Charset, Decode, Input, ValueError, decode, decode_after, int_type};
