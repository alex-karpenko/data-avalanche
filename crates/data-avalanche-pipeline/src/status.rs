//! What a running worker shows and takes: its status, the commands it obeys, why it ended, and
//! the timings its retries use.

use std::collections::BTreeMap;
use std::time::Duration;

use data_avalanche_core::{HaltReason, SourcePosition, TableRef};
use tokio::sync::watch;

use crate::Backoff;

/// A command to a running worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Finish the open transaction, flush, make it durable, close the stream and end with
    /// [`Exit::Drained`] (pause, stop and failover hand-off, Pipeline_Lifecycle §4).
    Drain,
    /// Flush now, whatever the flush policy says.
    Flush,
}

/// How a worker ended without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// A [`Command::Drain`] (or the handle dropped): everything read is durable.
    Drained,
    /// Every table is a `dump_*` table whose snapshot is done.
    Dumped,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Phase {
    #[default]
    Starting,
    Streaming,
    /// The stream failed transiently; the worker reopens it after a backoff.
    Reconnecting {
        error: String,
    },
    Draining,
}

/// A running table snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotProgress {
    /// Rows emitted, this run and the runs before it.
    pub rows_done: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct WorkerStatus {
    pub phase: Phase,
    /// The stream's frontier: the last complete transaction (or keep-alive) read.
    pub read: Option<SourcePosition>,
    /// The durable checkpoint: what the source was last told it may release.
    pub checkpoint: Option<SourcePosition>,
    /// The log the source keeps for the pipeline, read every heartbeat (PostgreSQL: the slot's
    /// retained WAL, `data_avalanche_wal_lag_bytes`).
    pub retained_bytes: Option<u64>,
    /// Sink commits or state writes are failing and being retried (the last error).
    pub degraded: Option<String>,
    pub halted: BTreeMap<TableRef, HaltReason>,
    pub snapshots: BTreeMap<TableRef, SnapshotProgress>,
    /// Warnings since the start: drift, a `never` gap, a degraded evolve.
    pub warnings: Vec<String>,
}

/// A check that keeps failing warns once: the first failure of a streak adds a status warning,
/// and nothing more is added until a success ends the streak.
#[derive(Debug, Default)]
pub(crate) struct Streak {
    failures: u32,
}

impl Streak {
    pub fn failed(
        &mut self,
        status: &watch::Sender<WorkerStatus>,
        warning: impl FnOnce() -> String,
    ) {
        self.failures = self.failures.saturating_add(1);
        if self.failures == 1 {
            let warning = warning();
            status.send_modify(|st| st.warnings.push(warning));
        }
    }

    pub fn succeeded(&mut self) {
        self.failures = 0;
    }

    /// The streak's failures so far; 0 when it isn't failing.
    pub fn failures(&self) -> u32 {
        self.failures
    }
}

/// The worker's internal timings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuning {
    /// Between stream sessions after a transient source failure (Data-Flow §Source Disconnect).
    pub source_backoff: Backoff,
    /// Between attempts of a failing sink commit or state write.
    pub sink_backoff: Backoff,
    /// Between checks that a snapshot table's in-flight writers have finished (DA-SNAP-9).
    pub in_flight_backoff: Backoff,
    /// How long a busy stream position is retried (`Postgres_Slot…` D-15.5).
    pub contended_window: Duration,
    /// How often the age flush policy and the snapshot timeout are checked.
    pub tick: Duration,
    /// Attempts of a schema signal's catalog read before the table halts (`Schema_Change…` §3 C).
    pub augment_attempts: u32,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            source_backoff: Backoff::default(),
            sink_backoff: Backoff::default(),
            in_flight_backoff: Backoff {
                first: Duration::from_millis(500),
                max: Duration::from_secs(10),
            },
            contended_window: Duration::from_secs(120),
            tick: Duration::from_secs(1),
            augment_attempts: 3,
        }
    }
}
