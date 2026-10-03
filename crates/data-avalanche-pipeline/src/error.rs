use std::time::Duration;

use data_avalanche_core::{
    EventShapeError, HaltReason, PositionError, SinkError, SourceError, TableRef,
};
use data_avalanche_state::StateError;
use thiserror::Error;

use crate::hooks::HookPoint;

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
    /// With splitting on, the open transaction reached `max_transaction_buffer_bytes`: nothing
    /// changed; split, commit the slice, then apply the same event again. The split empties the
    /// open transaction, so the retry is buffered (Large_Transaction §1).
    #[error(
        "the open transaction reached max_transaction_buffer_bytes: split it, then apply again"
    )]
    SplitRequired,
    /// The open transaction outgrew `max_transaction_buffer_bytes` with splitting off: the
    /// pipeline halts rather than exhaust the worker's memory (Large_Transaction §2).
    #[error(
        "a transaction buffers {bytes} bytes, over max_transaction_buffer_bytes {limit}; set max_transaction_batch_rows to split it"
    )]
    TransactionTooLarge { bytes: usize, limit: u64 },
}

/// Why a pipeline worker stopped (Data-Flow §Error Paths). A transient source or sink failure
/// never ends the worker: it retries on its own.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// Phase 0 refused the pipeline (config §13.4, D-V1.4, a capability the sink lacks): an
    /// operator must change the config or the state.
    #[error("the pipeline is refused: {0}")]
    Refused(String),
    /// A permanent source failure: configuration, privilege, server setting, protocol.
    #[error(transparent)]
    Source(SourceError),
    /// The source's stream position stayed held by another reader for the whole retry window.
    #[error("the stream position stayed held by another reader for {waited:?}: {detail}")]
    Contended { waited: Duration, detail: String },
    /// A sink failure no table halt contains: the sink lost a table it created, or refused a
    /// change set as malformed (a pipeline bug).
    #[error(transparent)]
    Sink(SinkError),
    #[error(transparent)]
    State(#[from] StateError),
    /// A newer owner wrote the table; this worker must stop (Clustering §19).
    #[error("table {table} has a newer owner (epoch {current_epoch}): this worker stops")]
    Fenced { table: TableRef, current_epoch: u64 },
    /// The commit guard refused a commit: the lease is no longer held.
    #[error("the lease is no longer held: this worker stops before its next commit")]
    LeaseLost,
    /// One transaction outgrew `max_transaction_buffer_bytes` with splitting off
    /// (Large_Transaction §2).
    #[error(
        "a transaction buffers {bytes} bytes, over max_transaction_buffer_bytes {limit}; set max_transaction_batch_rows to split it"
    )]
    TransactionTooLarge { bytes: usize, limit: u64 },
    /// The slot retains more WAL than `max_wal_lag_bytes` (`Postgres_Slot…` D-15.4).
    #[error("the slot retains {lag} bytes of WAL, over max_wal_lag_bytes {limit}")]
    WalLag { lag: u64, limit: u64 },
    /// The stream or the accumulator broke an invariant: a bug.
    #[error(transparent)]
    Accumulator(AccumulatorError),
    /// A test hook stopped the worker (`HookAction::Crash`).
    #[error("crashed at {0:?}")]
    Crashed(HookPoint),
}

impl From<AccumulatorError> for WorkerError {
    fn from(e: AccumulatorError) -> Self {
        match e {
            AccumulatorError::TransactionTooLarge { bytes, limit } => {
                Self::TransactionTooLarge { bytes, limit }
            }
            e => Self::Accumulator(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_transaction_too_large_is_its_own_stop() {
        let e = WorkerError::from(AccumulatorError::TransactionTooLarge {
            bytes: 2048,
            limit: 1024,
        });
        assert!(matches!(
            e,
            WorkerError::TransactionTooLarge {
                bytes: 2048,
                limit: 1024
            }
        ));
        let e = WorkerError::from(AccumulatorError::Protocol {
            detail: "nested".into(),
        });
        assert!(matches!(e, WorkerError::Accumulator(_)), "{e}");
    }
}
