//! The data path between a source and a sink (Data-Flow Phase 3): per-table buffers that collapse
//! change events by key as they arrive, and the accumulator that tracks transactions and decides
//! when to flush.
//!
//! The accumulator performs no I/O and reads no clock. The pipeline worker drives it: it reads
//! the source, runs the snapshots, hands change sets to the sink and persists the checkpoint.

pub mod accumulator;
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
    reason = "the worker reads it (Task 11); the allow goes there"
)]
mod settings;
#[cfg(test)]
mod test_support;

pub use accumulator::{Accumulator, Flush};
pub use batch::{Lineage, MAX_BATCH_BYTES};
pub use buffer::{CollapsedRow, RowKind, TableBuffer};
pub use error::{AccumulatorError, WorkerError};
pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
pub use policy::{FlushPolicy, FlushReason, TxnLimits};
