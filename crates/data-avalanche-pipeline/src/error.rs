use data_avalanche_core::{EventShapeError, HaltReason, PositionError, TableRef};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AccumulatorError {
    /// The table can't continue; the pipeline halts it (Data-Flow §Halt).
    #[error("table {table} halts: {reason}")]
    Halt { table: TableRef, reason: HaltReason },
    /// The source produced an event of the wrong shape: a source-connector bug.
    #[error(transparent)]
    Event(#[from] EventShapeError),
    /// An event of a table the accumulator doesn't track.
    #[error("an event of untracked table {table}")]
    UnknownTable { table: TableRef },
    /// The stream broke its own rules (nested transactions, a commit of another transaction, a
    /// position going backwards): a source-connector bug.
    #[error("stream protocol violation: {detail}")]
    Protocol { detail: String },
    #[error(transparent)]
    Position(#[from] PositionError),
    /// A barrier (TRUNCATE, schema change) met committed changes of the table: flush them and
    /// commit the change sets, then apply the barrier again (seal → flush → apply).
    #[error("table {table} has committed changes to flush before the barrier")]
    FlushRequired { table: TableRef },
    /// The open transaction outgrew `max_transaction_buffer_bytes` with splitting off: the
    /// pipeline halts rather than exhaust the worker's memory (Large_Transaction §2).
    #[error(
        "a transaction buffers {bytes} bytes, over max_transaction_buffer_bytes {limit}; set max_transaction_batch_rows to split it"
    )]
    TransactionTooLarge { bytes: usize, limit: u64 },
}
