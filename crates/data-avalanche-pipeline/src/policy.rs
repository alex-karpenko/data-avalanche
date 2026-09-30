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

/// Why a flush is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// `seconds: 0`: every transaction boundary.
    Boundary,
    /// The oldest flushable event is `seconds` old.
    Age,
    Rows,
    Bytes,
}
