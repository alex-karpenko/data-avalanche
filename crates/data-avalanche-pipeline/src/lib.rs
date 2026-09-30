//! The data path between a source and a sink (Data-Flow Phase 3): per-table buffers that collapse
//! change events by key as they arrive, and the accumulator that tracks transactions and decides
//! when to flush.
//!
//! Nothing here performs I/O or reads a clock: the orchestrator passes events in, reads change
//! sets out and hands them to a [`data_avalanche_core::Sink`].

pub mod accumulator;
pub mod batch;
pub mod buffer;
pub mod error;
pub mod policy;

pub use accumulator::{Accumulator, Flush};
pub use batch::{Lineage, MAX_BATCH_BYTES};
pub use buffer::{CollapsedRow, RowKind, TableBuffer};
pub use error::AccumulatorError;
pub use policy::{FlushPolicy, FlushReason};
