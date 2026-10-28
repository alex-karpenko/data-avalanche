//! What the worker reports as it happens: the timings and the counts the metrics' histograms and
//! event counters take (System-Overview §Observability catalogue). Its state (the lag, the
//! buffer, the snapshots) is in [`crate::WorkerStatus`] instead, which the owner samples.

use std::fmt::Debug;
use std::time::Duration;

/// The owner's ears; every method defaults to nothing, and none may block.
pub trait Observer: Send + Sync + Debug {
    /// A flush is durable: the rows of its change sets, and the time from when the writer took
    /// it up until then.
    fn flushed(&self, rows: u64, took: Duration) {
        let _ = (rows, took);
    }

    /// A table's change set was committed: the rows it wrote, and how long the attempt that
    /// landed took. A set the sink skipped as already covered wrote nothing and isn't reported.
    fn committed(&self, rows: u64, took: Duration) {
        let _ = (rows, took);
    }

    /// A commit attempt failed: a transient failure that is retried, or the sink's refusal.
    fn commit_failed(&self) {}

    /// A snapshot chunk was read from the source.
    fn chunk_read(&self, took: Duration) {
        let _ = took;
    }
}

/// Hears nothing: a worker without an owner's metrics (tests).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoObserver;

impl Observer for NoObserver {}
