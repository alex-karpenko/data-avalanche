//! The pipeline worker against a live PostgreSQL (`test-infra/compose.yaml`): `postgres-18` on
//! every change, PostgreSQL 14–17 weekly (`DATA_AVALANCHE_PG_PORT`). The sink and the state
//! documents are in memory. Each test returns at once without `DATA_AVALANCHE_IT`.

mod common;
mod recovery;
mod schema;
mod snapshot;
mod stream;
