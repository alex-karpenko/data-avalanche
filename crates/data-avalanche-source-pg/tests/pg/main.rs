//! The PostgreSQL source against a live server (`test-infra/compose.yaml`): `postgres-18` on every
//! change, PostgreSQL 14–17 nightly (`DATA_AVALANCHE_PG_PORT`). Each test returns at once without
//! `DATA_AVALANCHE_IT`.

mod common;
mod resolve;
mod slot;
mod stream;
