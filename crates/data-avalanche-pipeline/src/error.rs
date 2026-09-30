use data_avalanche_core::{EventShapeError, HaltReason, TableRef};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AccumulatorError {
    /// The table can't continue; the pipeline halts it (Data-Flow §Halt).
    #[error("table {table} halts: {reason}")]
    Halt { table: TableRef, reason: HaltReason },
    /// The source produced an event of the wrong shape: a source-connector bug.
    #[error(transparent)]
    Event(#[from] EventShapeError),
}
