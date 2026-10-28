//! What a running worker shows and takes: its status, the commands it obeys, why it ended, and
//! the timings its retries use.

use std::collections::BTreeMap;
use std::time::Duration;

use data_avalanche_config::Pipeline;
use data_avalanche_core::{HaltReason, Op, SourcePosition, TableRef};
use tokio::sync::watch;

use crate::Backoff;

/// A command to a running worker.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Finish the open transaction, flush, make it durable, close the stream and end with
    /// [`Exit::Drained`] (stop and failover hand-off, Pipeline_Lifecycle §2, §4).
    Drain,
    /// Flush now, whatever the flush policy says.
    Flush,
    /// Finish the open transaction, flush and make it durable, then hold: nothing more is read
    /// from the stream, no chunk is read, no rescan runs, and the snapshot timeout's clock
    /// stops. The source connection stays open (warm) and the WAL-lag guard keeps watching
    /// (Pipeline_Lifecycle §2 `PAUSED`, Q-M6b-1).
    Pause,
    /// Read again after a [`Command::Pause`].
    Resume,
    /// A reload's settings (config §1.2, class R), taken in place: the flush policy, the
    /// transaction limits, the buffer, the name in the rows' lineage, the snapshot parallelism and
    /// chunk size, the heartbeat, the WAL-lag limit and schema evolution, and the source's own
    /// (`SourceConnector::reconfigure`). The pipeline's other fields are restart-only: a change
    /// to one restarts the worker instead.
    Reconfigure(Box<Pipeline>),
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
    /// The stream failed transiently, or another reader holds its position; the worker reopens
    /// it after a backoff.
    Reconnecting {
        error: String,
    },
    Draining,
    /// Everything read is durable and nothing more is read until [`Command::Resume`]; the
    /// source connection stays open.
    Paused,
}

/// A running table snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotProgress {
    /// Rows emitted, this run and the runs before it.
    pub rows_done: u64,
    /// It reads nothing until something else ends: a transaction in flight at the session's
    /// start that writes the table, or a schema change a rescan found
    /// (`data_avalanche_snapshot_state{state="waiting"}`).
    pub waiting: bool,
}

/// Rows read since the worker started (`data_avalanche_rows_read_total{op}`): the stream's
/// changes by operation, and the snapshot chunks' rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RowsRead {
    pub insert: u64,
    pub update: u64,
    pub delete: u64,
    pub snapshot: u64,
}

impl RowsRead {
    pub fn count(&mut self, op: Op, n: u64) {
        let total = match op {
            Op::Insert => &mut self.insert,
            Op::Update => &mut self.update,
            Op::Delete => &mut self.delete,
            Op::Snapshot => &mut self.snapshot,
        };
        *total += n;
    }
}

/// The snapshot reconciler's log (DA-SNAP-9): the stream transactions kept to check chunks
/// against, and the keys they wrote in the tables being snapshotted. It grows while a table's
/// reads wait (an in-flight writer, a held table) and empties once no snapshot runs. Unbounded
/// for now: M6b reports it as a gauge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReconcileLog {
    /// Logged transactions.
    pub entries: usize,
    /// The keys they wrote, summed over the transactions.
    pub keys: usize,
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
    /// How far the stream reads behind the source's newest change, read every heartbeat (MySQL
    /// and MariaDB, `data_avalanche_binlog_lag_seconds`).
    pub source_behind: Option<Duration>,
    /// Sink commits or state writes are failing and being retried (the last error).
    pub degraded: Option<String>,
    pub halted: BTreeMap<TableRef, HaltReason>,
    pub snapshots: BTreeMap<TableRef, SnapshotProgress>,
    /// The snapshot reconciler's log, as of the last stream boundary or tick.
    pub reconcile_log: ReconcileLog,
    /// Warnings since the start: drift, a `never` gap, a degraded evolve. Each text once, the
    /// latest [`MAX_WARNINGS`]; add them with [`WorkerStatus::warn`].
    pub warnings: Vec<String>,
    /// Warnings dropped to keep [`MAX_WARNINGS`].
    pub warnings_dropped: u64,
    /// Stream sessions reopened after a source failure (`data_avalanche_source_reconnects_total`).
    pub reconnects: u64,
    /// The source commit time, in microseconds since the Unix epoch, of the oldest transaction
    /// read and not yet durable in the sink; `None` when none waits. Its age is
    /// `data_avalanche_replication_lag_seconds`. Snapshot rows have no commit time and don't
    /// count. As of the last stream boundary, durable flush or heartbeat.
    pub unsynced_since_micros: Option<i64>,
    pub rows_read: RowsRead,
    /// Bytes the accumulator buffers and hasn't handed to the writer yet, the open transaction's
    /// included (`data_avalanche_transaction_buffer_bytes`). As of the last stream boundary or
    /// heartbeat.
    pub buffer_bytes: u64,
    /// Per table, the sink's columns the source table no longer has: soft-dropped, kept and
    /// written NULL (`data_avalanche_sink_soft_dropped_columns`). Counted at the start and after
    /// each evolve.
    pub soft_dropped: BTreeMap<TableRef, usize>,
}

/// The warnings a status keeps.
pub const MAX_WARNINGS: usize = 50;

impl WorkerStatus {
    /// Adds a warning as the latest. One the status already shows moves to the end instead: a
    /// rerun of Phase 0, a reconnect or a new failing streak that finds the same thing again adds
    /// nothing. Past [`MAX_WARNINGS`] the oldest goes.
    pub fn warn(&mut self, warning: String) {
        if let Some(at) = self.warnings.iter().position(|w| *w == warning) {
            let again = self.warnings.remove(at);
            self.warnings.push(again);
            return;
        }
        self.warnings.push(warning);
        if self.warnings.len() > MAX_WARNINGS {
            self.warnings.remove(0);
            self.warnings_dropped += 1;
        }
    }
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
            status.send_modify(|st| st.warn(warning));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_warning_shows_once_as_of_its_latest() {
        let mut st = WorkerStatus::default();
        st.warn("a".into());
        st.warn("b".into());
        st.warn("a".into());
        assert_eq!(st.warnings, ["b", "a"]);
        assert_eq!(st.warnings_dropped, 0);
    }

    #[test]
    fn past_the_cap_the_oldest_warning_goes() {
        let mut st = WorkerStatus::default();
        for i in 0..MAX_WARNINGS + 2 {
            st.warn(format!("w{i}"));
        }
        assert_eq!(st.warnings.len(), MAX_WARNINGS);
        assert_eq!(st.warnings.first().map(String::as_str), Some("w2"));
        assert_eq!(st.warnings_dropped, 2);
    }
}
