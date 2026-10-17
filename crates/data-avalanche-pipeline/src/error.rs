use std::time::Duration;

use data_avalanche_core::{
    EventShapeError, HaltReason, PositionError, SinkError, SourceError, TableRef,
};
use data_avalanche_state::StateError;
use data_avalanche_storage::StoreError;
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
    /// The commit guard refused a commit or a write: the lease is no longer held.
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

/// What the pipeline's controller does once its worker ends with an error (Q-M6b-3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// Run the worker again, Phase 0 included, after a backoff: the storage or the source may
    /// answer next time.
    Retry,
    /// The lease is gone, or a newer owner wrote a table: stop and go back to the lease.
    Lease,
    /// The pipeline is in `ERROR` until an operator acts (the process restarts, or M9's
    /// restart): rerunning it would fail the same way.
    Error,
}

impl WorkerError {
    /// The code of a pipeline-scope halt (`BINLOG_PAYLOAD_INVALID`, `BINLOG_SOURCE_CHANGED`): a
    /// permanent source error whose message names one ([`SourceError::halt_code`]).
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Source(e) => e.halt_code(),
            _ => None,
        }
    }

    pub fn recovery(&self) -> Recovery {
        match self {
            Self::State(StateError::Store(
                StoreError::Transient { .. } | StoreError::Contention { .. },
            ))
            | Self::Contended { .. } => Recovery::Retry,
            Self::Source(e) if e.is_transient() || matches!(e, SourceError::Contended(_)) => {
                Recovery::Retry
            }
            Self::Sink(e) if e.is_transient() => Recovery::Retry,
            Self::Fenced { .. } | Self::LeaseLost => Recovery::Lease,
            Self::Refused(_)
            | Self::Source(_)
            | Self::Sink(_)
            | Self::State(_)
            | Self::TransactionTooLarge { .. }
            | Self::WalLag { .. }
            | Self::Accumulator(_)
            | Self::Crashed(_) => Recovery::Error,
        }
    }
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
    fn a_source_halt_names_its_code() {
        let halt = WorkerError::Source(SourceError::Permanent(
            "BINLOG_SOURCE_CHANGED: binlog position f:4 was recorded on another server".into(),
        ));
        assert_eq!(halt.code(), Some("BINLOG_SOURCE_CHANGED"));
        assert_eq!(halt.recovery(), Recovery::Error);
        let plain = WorkerError::Source(SourceError::Permanent("no privilege".into()));
        assert_eq!(plain.code(), None);
        assert_eq!(WorkerError::Refused("BINLOG_X: y".into()).code(), None);
    }

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

    #[test]
    fn each_error_names_its_recovery() {
        let store = |e| WorkerError::State(StateError::Store(e));
        let cases = [
            (
                store(StoreError::Transient {
                    key: "k".into(),
                    reason: "503".into(),
                }),
                Recovery::Retry,
            ),
            (
                WorkerError::Contended {
                    waited: Duration::from_secs(120),
                    detail: "55006".into(),
                },
                Recovery::Retry,
            ),
            (
                WorkerError::Source(SourceError::Transient("reset".into())),
                Recovery::Retry,
            ),
            // Phase 0's `prepare` met the slot active for another PID.
            (
                WorkerError::Source(SourceError::Contended("slot active".into())),
                Recovery::Retry,
            ),
            (
                WorkerError::Sink(SinkError::Transient {
                    reason: "503".into(),
                }),
                Recovery::Retry,
            ),
            (
                WorkerError::Fenced {
                    table: TableRef::new("public", "a"),
                    current_epoch: 2,
                },
                Recovery::Lease,
            ),
            (WorkerError::LeaseLost, Recovery::Lease),
            (WorkerError::Refused("iceberg".into()), Recovery::Error),
            (
                WorkerError::Source(SourceError::Permanent("privilege".into())),
                Recovery::Error,
            ),
            (
                WorkerError::Sink(SinkError::Permanent {
                    reason: "malformed".into(),
                }),
                Recovery::Error,
            ),
            (
                WorkerError::WalLag {
                    lag: 2048,
                    limit: 1024,
                },
                Recovery::Error,
            ),
            (
                WorkerError::TransactionTooLarge {
                    bytes: 2048,
                    limit: 1024,
                },
                Recovery::Error,
            ),
        ];
        for (e, want) in cases {
            assert_eq!(e.recovery(), want, "{e}");
        }
    }
}
