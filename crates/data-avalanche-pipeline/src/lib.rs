//! The data path between a source and a sink (Data-Flow Phase 3): per-table buffers that collapse
//! change events by key as they arrive, and the accumulator that tracks transactions and decides
//! when to flush.
//!
//! The accumulator performs no I/O and reads no clock. The pipeline worker drives it: it reads
//! the source, runs the snapshots, hands change sets to the sink and persists the checkpoint.

pub mod accumulator;
pub mod backoff;
pub mod batch;
pub mod buffer;
#[allow(
    dead_code,
    reason = "the worker reads it (Task 11); the allow goes there"
)]
mod decide;
pub mod error;
pub mod hooks;
pub mod policy;
#[allow(
    dead_code,
    reason = "the worker runs it (Task 11); the allow goes there"
)]
mod session;
#[allow(
    dead_code,
    reason = "the worker reads it (Task 11); the allow goes there"
)]
mod settings;
pub mod status;
#[cfg(test)]
mod test_support;
#[allow(
    dead_code,
    reason = "the worker runs it (Task 11); the allow goes there"
)]
mod writer;

pub use accumulator::{Accumulator, Flush};
pub use backoff::Backoff;
pub use batch::{Lineage, MAX_BATCH_BYTES};
pub use buffer::{CollapsedRow, RowKind, TableBuffer};
pub use error::{AccumulatorError, WorkerError};
pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
pub use policy::{FlushPolicy, FlushReason, TxnLimits};
pub use status::{Command, Exit, Phase, SnapshotProgress, Tuning, WorkerStatus};
