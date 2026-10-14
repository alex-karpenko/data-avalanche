//! The MySQL and MariaDB source against a live server (`test-infra/compose.yaml`): the target is
//! `DATA_AVALANCHE_MYSQL` — `mysql-84` (default), `mysql-84-nogtid`, `mariadb-118` and
//! `mariadb-118-binlog` on every change, `mariadb-114` and `mariadb-1011` weekly. Each test
//! returns at once without `DATA_AVALANCHE_IT`.

mod common;
mod position;
mod preflight;
mod tls;
