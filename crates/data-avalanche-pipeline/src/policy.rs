//! When to flush (config §9.4.1): OR semantics, first trigger wins; `None` disables a trigger.

/// The resolved `sink.flush` block. `seconds: Some(0)` flushes at every transaction boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushPolicy {
    pub seconds: Option<u64>,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
}

impl Default for FlushPolicy {
    /// The config defaults: 30 s, 100 000 rows, 512 MiB.
    fn default() -> Self {
        Self {
            seconds: Some(30),
            rows: Some(100_000),
            bytes: Some(512 << 20),
        }
    }
}

/// The resolved `sink.max_transaction_batch_rows` and `sink.max_transaction_buffer_bytes`
/// (Large_Transaction §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnLimits {
    /// Split a transaction into slices of this many events. `None`: never split.
    pub batch_rows: Option<u64>,
    /// The open transaction's buffer ceiling: the pipeline halts when splitting is off, and
    /// splits early when it is on.
    pub buffer_bytes: u64,
}

impl Default for TxnLimits {
    /// The config defaults: no splitting, 1 GiB.
    fn default() -> Self {
        Self {
            batch_rows: None,
            buffer_bytes: 1 << 30,
        }
    }
}

/// Why a flush is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// The open transaction reached a [`TxnLimits`] ceiling: flush a slice
    /// ([`crate::Accumulator::split`]) rather than a regular flush.
    Split,
    /// `seconds: 0`: every transaction boundary.
    Boundary,
    /// The oldest flushable event is `seconds` old.
    Age,
    Rows,
    Bytes,
}
