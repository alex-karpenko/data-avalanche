//! The slice-1 E2E suite (roadmap §5.3): the `data-avalanche` binary as a process, a source and
//! RustFS from `test-infra/compose.yaml`. The source is PostgreSQL (`DATA_AVALANCHE_PG_PORT`,
//! else 18), or with `DATA_AVALANCHE_E2E_SOURCE=mysql` the MySQL or MariaDB server
//! `DATA_AVALANCHE_MYSQL` names (else `mysql-84`); a scenario of one source only skips itself on
//! the other (Q-M7b-1). Needs the `e2e-hooks` feature and `DATA_AVALANCHE_IT=1`:
//!
//! `cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

mod crashes;
mod guards;
mod harness;
mod lifecycle;
mod mysql;
mod proxy;
mod schema;
mod smoke;
mod snapshots;
mod source;
mod streaming;
mod toast;
mod types;
