//! What a running worker shows and takes: its status, the commands it obeys, why it ended, and
//! the timings its retries use.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use chrono::{DateTime, Utc};
use data_avalanche_config::Pipeline;
use data_avalanche_core::{CommitCost, HaltReason, Op, SourceDetail, SourcePosition, TableRef};
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
    /// The checkpoint the worker's first stream session started from. A durable checkpoint
    /// past it is the run's progress: the controller halts a pipeline whose runs keep finding
    /// another server without any (DA-SRC-34).
    pub started_from: Option<SourcePosition>,
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
    /// What the stream sessions before a reconnect read and left pending: the sink doesn't hold
    /// it, and this session reads it again from the checkpoint. Kept until this session's stream
    /// has read as far again ([`WorkerStatus::pending_since_micros`]).
    pub left_pending: Option<LeftPending>,
    /// The last time the stream held nothing read and not yet durable, its earlier sessions'
    /// changes included: lake readers saw every change read by then. Set while the stream runs
    /// (streaming or draining), as of the last stream boundary, durable flush or heartbeat; a
    /// reconnect, a pause or the worker's end keeps it. What readers see when the stream isn't
    /// read (S-M13d.3).
    pub caught_up_at: Option<DateTime<Utc>>,
    pub rows_read: RowsRead,
    /// Bytes the accumulator buffers and hasn't handed to the writer yet, the open transaction's
    /// included (`data_avalanche_transaction_buffer_bytes`). As of the last stream boundary or
    /// heartbeat.
    pub buffer_bytes: u64,
    /// Per table, the sink's columns the source table no longer has: soft-dropped, kept and
    /// written NULL (`data_avalanche_sink_soft_dropped_columns`). Counted at the start and after
    /// each evolve.
    pub soft_dropped: BTreeMap<TableRef, usize>,
    /// The tables the last Phase 0 tracks: the pipeline's page lists them.
    pub tracked: BTreeSet<TableRef>,
    /// The `dump_*` tables this worker doesn't stream: a `dump_initial` table the sink had at the
    /// start, or a dump whose snapshot ended. The pipeline's page lists them `copied`.
    pub copied: BTreeSet<TableRef>,
    /// Per table, what this worker committed to it since it started: the pipeline's page.
    pub tables: BTreeMap<TableRef, TableFigures>,
    /// What the source keeps for the pipeline, as of `source_read_at`: read every heartbeat. A
    /// failed read keeps the last one.
    pub source: Option<SourceDetail>,
    pub source_read_at: Option<DateTime<Utc>>,
}

/// What a stream session left pending when a reconnect ended it (DA-OBS-8).
#[derive(Debug, Clone, PartialEq)]
pub struct LeftPending {
    /// The source commit time, in microseconds since the Unix epoch, of the oldest change it read
    /// that the sink doesn't hold.
    pub since_micros: i64,
    /// How far it had read: its frontier, else its checkpoint.
    pub read: Option<SourcePosition>,
}

/// One commit the writer made to a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCommit {
    pub table: TableRef,
    /// The rows it wrote, upserts and deletes.
    pub rows: u64,
    /// A snapshot chunk's rows, not the stream's changes.
    pub snapshot: bool,
    pub version: u64,
    pub cost: CommitCost,
    pub at: DateTime<Utc>,
}

/// The window [`TableFigures::changes_per_minute`] counts over.
pub const CHANGES_WINDOW: Duration = Duration::from_secs(60);

/// A table's figures since the worker started, from its commits.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableFigures {
    /// The stream's changes committed to it; a snapshot's rows don't count.
    pub changes: u64,
    /// The commits of the last [`CHANGES_WINDOW`] that carried changes: when, and how many.
    recent: VecDeque<(DateTime<Utc>, u64)>,
    pub last_commit: Option<DateTime<Utc>>,
    /// The table's version after the last commit.
    pub version: Option<u64>,
    /// The cost of the stream's last commit that wrote rows (a snapshot's don't count): a MERGE's
    /// files rewritten, and the table's files after it.
    pub cost: Option<CommitCost>,
}

impl TableFigures {
    fn add(&mut self, c: &TableCommit) {
        self.last_commit = Some(c.at);
        self.version = Some(c.version);
        // A snapshot chunk's commit isn't the table's cost: that is its stream's last MERGE.
        if c.rows > 0 && !c.snapshot {
            self.cost = Some(c.cost);
        }
        if !c.snapshot && c.rows > 0 {
            self.changes += c.rows;
            self.recent.push_back((c.at, c.rows));
        }
        let window = chrono::Duration::from_std(CHANGES_WINDOW).expect("a minute");
        while self
            .recent
            .front()
            .is_some_and(|(at, _)| *at <= c.at - window)
        {
            self.recent.pop_front();
        }
    }

    /// The changes committed in the [`CHANGES_WINDOW`] before `now`.
    #[must_use]
    pub fn changes_per_minute(&self, now: DateTime<Utc>) -> u64 {
        let window = chrono::Duration::from_std(CHANGES_WINDOW).expect("a minute");
        self.recent
            .iter()
            .filter(|(at, _)| *at > now - window)
            .map(|(_, rows)| rows)
            .sum()
    }
}

/// The warnings a status keeps.
pub const MAX_WARNINGS: usize = 50;

impl WorkerStatus {
    /// Counts a reconnect: the new session has read nothing yet and holds nothing pending, so
    /// the frontier and the pending start go with the count (DA-OBS-8). What the old session left
    /// pending moves to [`WorkerStatus::left_pending`], with the older start and the further
    /// frontier of the two; [`WorkerStatus::caught_up_at`] stays.
    pub fn reconnect(&mut self) {
        self.reconnects += 1;
        let read = self.read.take().or_else(|| self.checkpoint.clone());
        let own = self
            .unsynced_since_micros
            .take()
            .map(|since_micros| LeftPending { since_micros, read });
        self.left_pending = match (self.left_pending.take(), own) {
            (Some(k), Some(o)) => {
                let read = match (&o.read, &k.read) {
                    (Some(or), Some(kr)) if or.covers(kr).unwrap_or(false) => o.read.clone(),
                    (Some(_), None) => o.read.clone(),
                    _ => k.read.clone(),
                };
                Some(LeftPending {
                    since_micros: k.since_micros.min(o.since_micros),
                    read,
                })
            }
            (k, o) => k.or(o),
        };
    }

    /// The commit time of the oldest change read and not yet durable, this session's or one an
    /// earlier session left: what lake readers lack. `None` when nothing is known to wait.
    #[must_use]
    pub fn pending_since_micros(&self) -> Option<i64> {
        [
            self.unsynced_since_micros,
            self.left_pending.as_ref().map(|l| l.since_micros),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// The stream's frontier and pending start were updated at `at`: what an earlier session
    /// left goes once this one has read as far, and a running stream with nothing pending is
    /// caught up as of `at`. Positions that don't compare say the earlier changes are gone.
    pub(crate) fn progressed(&mut self, at: DateTime<Utc>) {
        let past = self
            .left_pending
            .as_ref()
            .is_some_and(|l| match (&self.read, &l.read) {
                (Some(read), Some(left)) => read.covers(left).unwrap_or(true),
                (Some(_), None) => true,
                (None, _) => false,
            });
        if past {
            self.left_pending = None;
        }
        let runs = matches!(self.phase, Phase::Streaming | Phase::Draining);
        if runs && self.pending_since_micros().is_none() {
            self.caught_up_at = Some(at);
        }
    }

    /// A commit the writer made: the table's figures take it.
    pub fn committed(&mut self, c: &TableCommit) {
        self.tables.entry(c.table.clone()).or_default().add(c);
    }

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

    fn commit(rows: u64, snapshot: bool, secs: i64) -> TableCommit {
        TableCommit {
            table: TableRef::new("public", "t"),
            rows,
            snapshot,
            version: secs as u64,
            cost: CommitCost {
                files_rewritten: rows,
                files: Some(10),
            },
            at: DateTime::from_timestamp(secs, 0).unwrap(),
        }
    }

    #[test]
    fn a_table_s_figures_follow_its_commits() {
        let mut st = WorkerStatus::default();
        st.committed(&commit(5, false, 1_000));
        st.committed(&commit(100, true, 1_030));
        st.committed(&commit(3, false, 1_050));
        // A marker-only commit moves the version, not the cost.
        st.committed(&commit(0, false, 1_055));
        let f = &st.tables[&TableRef::new("public", "t")];
        assert_eq!(f.changes, 8, "a snapshot's rows aren't changes");
        assert_eq!(f.version, Some(1_055));
        assert_eq!(f.last_commit, DateTime::from_timestamp(1_055, 0));
        assert_eq!(
            f.cost,
            Some(CommitCost {
                files_rewritten: 3,
                files: Some(10)
            })
        );
        let at = |secs| DateTime::from_timestamp(secs, 0).unwrap();
        assert_eq!(f.changes_per_minute(at(1_055)), 8);
        assert_eq!(
            f.changes_per_minute(at(1_060)),
            3,
            "the first is a minute old"
        );
        assert_eq!(f.changes_per_minute(at(1_200)), 0);
    }

    /// A snapshot commit moves the version and the last commit, not the cost: a re-snapshot's
    /// chunks rewrite what they copy, and the table's cost is its stream's last MERGE.
    #[test]
    fn a_snapshot_s_commit_isn_t_the_table_s_cost() {
        let mut st = WorkerStatus::default();
        st.committed(&commit(3, false, 1_000));
        st.committed(&commit(500, true, 1_010));
        let f = &st.tables[&TableRef::new("public", "t")];
        assert_eq!(
            (f.version, f.last_commit),
            (Some(1_010), DateTime::from_timestamp(1_010, 0))
        );
        assert_eq!(
            f.cost,
            Some(CommitCost {
                files_rewritten: 3,
                files: Some(10)
            })
        );
    }

    /// What a session left pending at a reconnect counts until the next one reads as far; a
    /// running stream with nothing pending is caught up, a paused one isn't.
    #[test]
    fn a_reconnect_keeps_what_readers_lack_until_read_again() {
        use data_avalanche_core::position::Lsn;
        let at = |secs| DateTime::from_timestamp(secs, 0).unwrap();
        let mut st = WorkerStatus {
            phase: Phase::Streaming,
            ..WorkerStatus::default()
        };
        st.progressed(at(10));
        assert_eq!(st.caught_up_at, Some(at(10)));
        st.read = Some(SourcePosition::Pg(Lsn(0x50)));
        st.unsynced_since_micros = Some(11_000_000);
        st.progressed(at(12));
        assert_eq!(st.caught_up_at, Some(at(10)), "a change waits");
        st.reconnect();
        assert_eq!((st.read.clone(), st.unsynced_since_micros), (None, None));
        assert_eq!(st.pending_since_micros(), Some(11_000_000));
        st.phase = Phase::Streaming;
        st.read = Some(SourcePosition::Pg(Lsn(0x40)));
        st.progressed(at(20));
        assert_eq!(st.caught_up_at, Some(at(10)), "not read as far again");
        st.read = Some(SourcePosition::Pg(Lsn(0x50)));
        st.progressed(at(21));
        assert_eq!(
            (st.left_pending.clone(), st.caught_up_at),
            (None, Some(at(21)))
        );
        st.phase = Phase::Paused;
        st.progressed(at(30));
        assert_eq!(st.caught_up_at, Some(at(21)), "a paused stream isn't read");
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
