//! The slice-1 E2E suite (roadmap §5.3): the `data-avalanche` binary as a process, PostgreSQL
//! (`DATA_AVALANCHE_PG_PORT`, else 18) and RustFS from `test-infra/compose.yaml`. Needs the
//! `e2e-hooks` feature and `DATA_AVALANCHE_IT=1`:
//!
//! `cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

mod harness;
mod smoke;
mod snapshots;
mod streaming;
mod toast;
