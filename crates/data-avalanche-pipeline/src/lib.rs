//! The data path between a source and a sink (Data-Flow Phase 3): per-table buffers that collapse
//! change events by key as they arrive, and the accumulator that tracks transactions and decides
//! when to flush.
//!
//! The accumulator performs no I/O and reads no clock. The [`PipelineWorker`] drives it: it reads
//! the source, runs the snapshots, hands change sets to the sink and persists the checkpoint.

pub mod accumulator;
pub mod backoff;
pub mod batch;
pub mod buffer;
mod cursor;
mod decide;
pub mod error;
pub mod hooks;
pub mod policy;
mod reconcile;
mod schema_change;
mod session;
mod settings;
pub mod status;
#[cfg(test)]
mod test_support;
pub mod worker;
mod writer;

pub use accumulator::{Accumulator, Flush};
pub use backoff::Backoff;
pub use batch::{Lineage, MAX_BATCH_BYTES};
pub use buffer::{CollapsedRow, RowKind, TableBuffer};
pub use error::{AccumulatorError, Recovery, WorkerError};
pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
pub use policy::{FlushPolicy, FlushReason, TxnLimits};
pub use status::{Command, Exit, Phase, ReconcileLog, SnapshotProgress, Tuning, WorkerStatus};
pub use worker::{PipelineWorker, WorkerDeps, WorkerHandle};
