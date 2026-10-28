//! The process's metrics (System-Overview §Observability catalogue): one Prometheus registry, the
//! node's gauges, and a handle per pipeline that sets that pipeline's series.
//!
//! Every pipeline series carries `{pipeline, source_host, source_db}`; a `table` label is added
//! only with `sink.metrics.per_table`, and only to `snapshot_rows_done` and `snapshot_state`. The
//! sink's commit series add `sink_type`. Counter families get the OpenMetrics `_total` suffix on
//! their samples (`data_avalanche_source_reconnects_total`).

use std::collections::BTreeSet;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

mod traces;

pub use traces::{Traces, TracesError, shown, trace_ids};

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

/// A series' labels: the pipeline's three, then the family's own.
type Labels = Vec<(&'static str, String)>;

type HistogramFamily = Family<Labels, Histogram, fn() -> Histogram>;

/// A gauge of seconds.
type Seconds = Gauge<f64, AtomicU64>;

/// The OpenMetrics text content type `/metrics` answers with.
pub const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// `data_avalanche_pipeline_status{status}`: the pipeline's actual state (Pipeline_Lifecycle §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Running,
    Paused,
    Stopped,
    /// The worker ended with an error an operator must act on (Q-M6b-3).
    Error,
}

impl PipelineStatus {
    const ALL: [Self; 4] = [Self::Running, Self::Paused, Self::Stopped, Self::Error];

    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Error => "error",
        }
    }
}

/// `data_avalanche_lease_state{state}` (Clustering §19).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    Held,
    Degraded,
    Lost,
}

impl LeaseState {
    const ALL: [Self; 3] = [Self::Held, Self::Degraded, Self::Lost];

    fn label(self) -> &'static str {
        match self {
            Self::Held => "held",
            Self::Degraded => "degraded",
            Self::Lost => "lost",
        }
    }
}

/// `data_avalanche_lease_renew_failures_total{class}` (Clustering §19).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewFailure {
    /// The store didn't answer: the renewer retries until the deadline.
    Transient,
    /// Concurrent conditional writes on the lease (`StoreError::Contention`): the outcome is
    /// unknown, and the renewer reads the lease before it retries. A lease that really changed
    /// (a 412) counts as `Lost`.
    Conflict,
    /// The lease is gone: the deadline passed, or the lease was taken, deleted or refused for
    /// good.
    Lost,
}

impl RenewFailure {
    const ALL: [Self; 3] = [Self::Transient, Self::Conflict, Self::Lost];

    fn label(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Conflict => "conflict",
            Self::Lost => "lost",
        }
    }
}

/// `data_avalanche_rows_read_total{op}`: a change's operation, a snapshot's row included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOp {
    Insert,
    Update,
    Delete,
    Snapshot,
}

impl RowOp {
    const ALL: [Self; 4] = [Self::Insert, Self::Update, Self::Delete, Self::Snapshot];

    fn label(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Snapshot => "snapshot",
        }
    }
}

/// `data_avalanche_snapshot_state{state}`: where a running table snapshot is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotState {
    /// It reads its chunks, or waits for its turn (`snapshot_parallelism`).
    Running,
    /// It reads nothing until something else ends: transactions in flight at the session's start
    /// that write it, or a schema change a rescan found.
    Waiting,
}

impl SnapshotState {
    const ALL: [Self; 2] = [Self::Running, Self::Waiting];

    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Waiting => "waiting",
        }
    }
}

/// The labels every series of a pipeline carries, and the sink type its commit series add.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineLabels {
    pub pipeline: String,
    pub source_host: String,
    pub source_db: String,
    /// `sink.type` (`delta`): the `sink_type` label of the commit series.
    pub sink_type: String,
}

impl PipelineLabels {
    fn with(&self, extra: &[(&'static str, &str)]) -> Labels {
        let mut labels = vec![
            ("pipeline", self.pipeline.clone()),
            ("source_host", self.source_host.clone()),
            ("source_db", self.source_db.clone()),
        ];
        labels.extend(extra.iter().map(|(k, v)| (*k, (*v).to_owned())));
        labels
    }

    fn sink(&self) -> Labels {
        self.with(&[("sink_type", &self.sink_type)])
    }
}

/// Rows read by operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowsRead {
    pub insert: u64,
    pub update: u64,
    pub delete: u64,
    pub snapshot: u64,
}

impl RowsRead {
    fn get(&self, op: RowOp) -> u64 {
        match op {
            RowOp::Insert => self.insert,
            RowOp::Update => self.update,
            RowOp::Delete => self.delete,
            RowOp::Snapshot => self.snapshot,
        }
    }
}

/// Monotonic totals a worker reports since it started. A new worker starts them at zero again;
/// the counters they feed don't.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    pub source_reconnects: u64,
    pub sink_merge_files_rewritten: u64,
    pub sink_merge_rows_copied: u64,
    pub rows_read: RowsRead,
}

#[derive(Debug)]
struct Families {
    pipeline_status: Family<Labels, Gauge>,
    lease_state: Family<Labels, Gauge>,
    lease_renew_failures: Family<Labels, Counter>,
    lease_renew_latency: HistogramFamily,
    owned_pipelines: Gauge,
    cluster_members: Gauge,
    config_generation: Gauge,
    ring_generation: Gauge,
    wal_lag_bytes: Family<Labels, Gauge>,
    source_reconnects: Family<Labels, Counter>,
    snapshot_rows_done: Family<Labels, Gauge>,
    halted_tables: Family<Labels, Gauge>,
    reconcile_log_entries: Family<Labels, Gauge>,
    reconcile_log_keys: Family<Labels, Gauge>,
    sink_merge_files_rewritten: Family<Labels, Counter>,
    sink_merge_rows_copied: Family<Labels, Counter>,
    replication_lag: Family<Labels, Seconds>,
    rows_read: Family<Labels, Counter>,
    rows_written: Family<Labels, Counter>,
    flush_duration: HistogramFamily,
    flush_rows: HistogramFamily,
    transaction_buffer_bytes: Family<Labels, Gauge>,
    binlog_lag: Family<Labels, Seconds>,
    snapshot_state: Family<Labels, Gauge>,
    snapshot_chunk_duration: HistogramFamily,
    sink_commit_duration: HistogramFamily,
    sink_commit_failures: Family<Labels, Counter>,
    sink_soft_dropped_columns: Family<Labels, Gauge>,
}

fn renew_latency() -> Histogram {
    // 5 ms … ~10 s.
    Histogram::new(exponential_buckets(0.005, 2.0, 12))
}

/// A flush, a commit, a chunk read: 5 ms … ~82 s.
fn duration() -> Histogram {
    Histogram::new(exponential_buckets(0.005, 2.0, 15))
}

/// A flush's rows: 1 … ~1 M.
fn rows() -> Histogram {
    Histogram::new(exponential_buckets(1.0, 4.0, 11))
}

/// The process's registry; cheap to clone.
#[derive(Debug, Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    families: Arc<Families>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let families = Families {
            pipeline_status: Family::default(),
            lease_state: Family::default(),
            lease_renew_failures: Family::default(),
            lease_renew_latency: Family::new_with_constructor(renew_latency),
            owned_pipelines: Gauge::default(),
            cluster_members: Gauge::default(),
            config_generation: Gauge::default(),
            ring_generation: Gauge::default(),
            wal_lag_bytes: Family::default(),
            source_reconnects: Family::default(),
            snapshot_rows_done: Family::default(),
            halted_tables: Family::default(),
            reconcile_log_entries: Family::default(),
            reconcile_log_keys: Family::default(),
            sink_merge_files_rewritten: Family::default(),
            sink_merge_rows_copied: Family::default(),
            replication_lag: Family::default(),
            rows_read: Family::default(),
            rows_written: Family::default(),
            flush_duration: Family::new_with_constructor(duration),
            flush_rows: Family::new_with_constructor(rows),
            transaction_buffer_bytes: Family::default(),
            binlog_lag: Family::default(),
            snapshot_state: Family::default(),
            snapshot_chunk_duration: Family::new_with_constructor(duration),
            sink_commit_duration: Family::new_with_constructor(duration),
            sink_commit_failures: Family::default(),
            sink_soft_dropped_columns: Family::default(),
        };
        let f = &families;
        let mut r = Registry::with_prefix("data_avalanche");
        r.register(
            "pipeline_status",
            "The pipeline's state: 1 on its current status",
            f.pipeline_status.clone(),
        );
        r.register(
            "lease_state",
            "The pipeline lease's state: 1 on its current state",
            f.lease_state.clone(),
        );
        r.register(
            "lease_renew_failures",
            "Failed lease renewals, by class",
            f.lease_renew_failures.clone(),
        );
        r.register(
            "lease_renew_latency_seconds",
            "How long a lease renewal took",
            f.lease_renew_latency.clone(),
        );
        r.register(
            "owned_pipelines",
            "Pipelines this node holds the lease of",
            f.owned_pipelines.clone(),
        );
        r.register(
            "cluster_members",
            "Members of the cluster this node sees",
            f.cluster_members.clone(),
        );
        r.register(
            "config_generation",
            "The config generation this node applies",
            f.config_generation.clone(),
        );
        r.register(
            "ring_generation",
            "Changes of the ring's nodes since this node started",
            f.ring_generation.clone(),
        );
        r.register(
            "wal_lag_bytes",
            "WAL the pipeline's replication slot retains",
            f.wal_lag_bytes.clone(),
        );
        r.register(
            "source_reconnects",
            "Stream sessions reopened after a source failure",
            f.source_reconnects.clone(),
        );
        r.register(
            "snapshot_rows_done",
            "Rows the running snapshots have emitted",
            f.snapshot_rows_done.clone(),
        );
        r.register(
            "halted_tables",
            "Tables the pipeline has halted",
            f.halted_tables.clone(),
        );
        r.register(
            "reconcile_log_entries",
            "Stream transactions the snapshot reconciler keeps (DA-SNAP-11)",
            f.reconcile_log_entries.clone(),
        );
        r.register(
            "reconcile_log_keys",
            "Keys the snapshot reconciler's transactions wrote (DA-SNAP-11)",
            f.reconcile_log_keys.clone(),
        );
        r.register(
            "sink_merge_files_rewritten",
            "Files a MERGE replaced or a truncate removed",
            f.sink_merge_files_rewritten.clone(),
        );
        r.register(
            "sink_merge_rows_copied",
            "Rows a MERGE rewrote unchanged because they share a file with a changed row",
            f.sink_merge_rows_copied.clone(),
        );
        r.register(
            "replication_lag_seconds",
            "Age of the oldest change read and not yet in the sink; 0 when none waits",
            f.replication_lag.clone(),
        );
        r.register(
            "rows_read",
            "Rows read from the source: the stream's changes by operation, and snapshot rows",
            f.rows_read.clone(),
        );
        r.register(
            "rows_written",
            "Rows the sink's commits wrote (upserts and deletes)",
            f.rows_written.clone(),
        );
        r.register(
            "flush_duration_seconds",
            "How long a flush took, from its hand-off to the writer until it was durable",
            f.flush_duration.clone(),
        );
        r.register("flush_rows", "Rows a flush carried", f.flush_rows.clone());
        r.register(
            "transaction_buffer_bytes",
            "Bytes the accumulator buffers and hasn't handed to the writer yet",
            f.transaction_buffer_bytes.clone(),
        );
        r.register(
            "binlog_lag_seconds",
            "How far the binlog stream reads behind the server's newest event; 0 once at it",
            f.binlog_lag.clone(),
        );
        r.register(
            "snapshot_state",
            "Running table snapshots, by state",
            f.snapshot_state.clone(),
        );
        r.register(
            "snapshot_chunk_duration_seconds",
            "How long a snapshot chunk's read took",
            f.snapshot_chunk_duration.clone(),
        );
        r.register(
            "sink_commit_duration_seconds",
            "How long a table's commit to the sink took",
            f.sink_commit_duration.clone(),
        );
        r.register(
            "sink_commit_failures",
            "Commits to the sink that failed: each retried attempt, and each refusal",
            f.sink_commit_failures.clone(),
        );
        r.register(
            "sink_soft_dropped_columns",
            "Sink columns the source no longer has: kept, written NULL",
            f.sink_soft_dropped_columns.clone(),
        );
        Self {
            registry: Arc::new(r),
            families: Arc::new(families),
        }
    }

    /// The OpenMetrics text `/metrics` serves ([`CONTENT_TYPE`]).
    pub fn encode(&self) -> String {
        let mut out = String::new();
        encode(&mut out, &self.registry).expect("writing to a String doesn't fail");
        out
    }

    /// One more pipeline owned here (`1`), or one fewer (`-1`).
    pub fn owned_by(&self, delta: i64) {
        self.families.owned_pipelines.inc_by(delta);
    }

    pub fn set_cluster_members(&self, n: usize) {
        self.families.cluster_members.set(gauge(n as u64));
    }

    pub fn set_config_generation(&self, generation: u64) {
        self.families.config_generation.set(gauge(generation));
    }

    pub fn set_ring_generation(&self, generation: u64) {
        self.families.ring_generation.set(gauge(generation));
    }

    /// The handle that sets one pipeline's series. `per_table` is `sink.metrics.per_table`.
    pub fn pipeline(&self, labels: PipelineLabels, per_table: bool) -> PipelineMetrics {
        PipelineMetrics {
            families: Arc::clone(&self.families),
            labels: Mutex::new((labels, per_table)),
            seen: Mutex::new(Seen::default()),
        }
    }
}

fn gauge(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[derive(Debug, Default)]
struct Seen {
    totals: Totals,
    /// The tables `snapshot_rows_done` has a series of (with `per_table`).
    tables: BTreeSet<String>,
    /// The tables `snapshot_state` has series of (with `per_table`).
    state_tables: BTreeSet<String>,
}

/// One pipeline's series. The controller that owns the pipeline sets them; [`Self::remove`]
/// drops them, but the renew failures, once the node no longer owns it.
#[derive(Debug)]
pub struct PipelineMetrics {
    families: Arc<Families>,
    /// The labels, and `sink.metrics.per_table`: a reload may change either
    /// ([`Self::relabel`]).
    labels: Mutex<(PipelineLabels, bool)>,
    seen: Mutex<Seen>,
}

impl PipelineMetrics {
    fn labels(&self) -> PipelineLabels {
        self.labels
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0
            .clone()
    }

    fn per_table(&self) -> bool {
        self.labels.lock().unwrap_or_else(PoisonError::into_inner).1
    }

    fn base(&self) -> Labels {
        self.labels().with(&[])
    }

    /// A renamed pipeline, or a reloaded `sink.metrics.per_table` (config §12.3): every series
    /// under the old labels goes, the renew failures too, and what is set next carries the new
    /// ones. The labels in force again change nothing.
    pub fn relabel(&self, labels: PipelineLabels, per_table: bool) {
        if *self.labels.lock().unwrap_or_else(PoisonError::into_inner)
            == (labels.clone(), per_table)
        {
            return;
        }
        self.forget();
        *self.labels.lock().unwrap_or_else(PoisonError::into_inner) = (labels, per_table);
    }

    /// A pipeline gone from the config: every series goes, the renew failures too.
    pub fn forget(&self) {
        self.remove();
        let labels = self.labels();
        for c in RenewFailure::ALL {
            self.families
                .lease_renew_failures
                .remove(&labels.with(&[("class", c.label())]));
        }
    }

    pub fn status(&self, status: PipelineStatus) {
        for s in PipelineStatus::ALL {
            let labels = self.labels().with(&[("status", s.label())]);
            self.families
                .pipeline_status
                .get_or_create(&labels)
                .set(i64::from(s == status));
        }
    }

    pub fn lease(&self, state: LeaseState) {
        for s in LeaseState::ALL {
            let labels = self.labels().with(&[("state", s.label())]);
            self.families
                .lease_state
                .get_or_create(&labels)
                .set(i64::from(s == state));
        }
    }

    pub fn renew_failed(&self, class: RenewFailure) {
        let labels = self.labels().with(&[("class", class.label())]);
        self.families
            .lease_renew_failures
            .get_or_create(&labels)
            .inc();
    }

    pub fn renewed(&self, took: Duration) {
        self.families
            .lease_renew_latency
            .get_or_create(&self.base())
            .observe(took.as_secs_f64());
    }

    /// `None` drops the series: the source doesn't report it, or its read failed.
    pub fn wal_lag_bytes(&self, bytes: Option<u64>) {
        match bytes {
            Some(b) => {
                self.families
                    .wal_lag_bytes
                    .get_or_create(&self.base())
                    .set(gauge(b));
            }
            None => {
                self.families.wal_lag_bytes.remove(&self.base());
            }
        }
    }

    /// The running snapshots' rows, per table: one series per table with `per_table`, else their
    /// sum. A table whose snapshot ended loses its series.
    pub fn snapshot_rows_done(&self, rows: &[(String, u64)]) {
        let family = &self.families.snapshot_rows_done;
        if !self.per_table() {
            let sum = rows.iter().map(|(_, n)| *n).sum();
            family.get_or_create(&self.base()).set(gauge(sum));
            return;
        }
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let now: BTreeSet<String> = rows.iter().map(|(t, _)| t.clone()).collect();
        for gone in seen.tables.difference(&now) {
            family.remove(&self.labels().with(&[("table", gone)]));
        }
        for (table, n) in rows {
            family
                .get_or_create(&self.labels().with(&[("table", table)]))
                .set(gauge(*n));
        }
        seen.tables = now;
    }

    pub fn halted_tables(&self, n: usize) {
        self.families
            .halted_tables
            .get_or_create(&self.base())
            .set(gauge(n as u64));
    }

    pub fn reconcile_log(&self, entries: usize, keys: usize) {
        let f = &self.families;
        f.reconcile_log_entries
            .get_or_create(&self.base())
            .set(gauge(entries as u64));
        f.reconcile_log_keys
            .get_or_create(&self.base())
            .set(gauge(keys as u64));
    }

    /// The age of the oldest change read and not yet durable in the sink (zero when none waits).
    pub fn replication_lag(&self, lag: Duration) {
        self.families
            .replication_lag
            .get_or_create(&self.base())
            .set(lag.as_secs_f64());
    }

    pub fn transaction_buffer_bytes(&self, bytes: u64) {
        self.families
            .transaction_buffer_bytes
            .get_or_create(&self.base())
            .set(gauge(bytes));
    }

    /// `None` drops the series: the source isn't a binlog, or its read failed.
    pub fn binlog_lag(&self, lag: Option<Duration>) {
        match lag {
            Some(l) => {
                self.families
                    .binlog_lag
                    .get_or_create(&self.base())
                    .set(l.as_secs_f64());
            }
            None => {
                self.families.binlog_lag.remove(&self.base());
            }
        }
    }

    /// The running snapshots' states: with `per_table`, each table's series is 1 on its state; else
    /// each state counts its tables. A table whose snapshot ended loses its series.
    pub fn snapshot_state(&self, tables: &[(String, SnapshotState)]) {
        let family = &self.families.snapshot_state;
        if !self.per_table() {
            for s in SnapshotState::ALL {
                let n = tables.iter().filter(|(_, t)| *t == s).count();
                family
                    .get_or_create(&self.labels().with(&[("state", s.label())]))
                    .set(gauge(n as u64));
            }
            return;
        }
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let now: BTreeSet<String> = tables.iter().map(|(t, _)| t.clone()).collect();
        for gone in seen.state_tables.difference(&now) {
            for s in SnapshotState::ALL {
                family.remove(&self.labels().with(&[("table", gone), ("state", s.label())]));
            }
        }
        for (table, state) in tables {
            for s in SnapshotState::ALL {
                family
                    .get_or_create(
                        &self
                            .labels()
                            .with(&[("table", table), ("state", s.label())]),
                    )
                    .set(i64::from(s == *state));
            }
        }
        seen.state_tables = now;
    }

    pub fn soft_dropped_columns(&self, n: usize) {
        self.families
            .sink_soft_dropped_columns
            .get_or_create(&self.base())
            .set(gauge(n as u64));
    }

    /// A flush became durable: its rows, and how long it took.
    pub fn flushed(&self, rows: u64, took: Duration) {
        let f = &self.families;
        let base = self.base();
        f.flush_rows.get_or_create(&base).observe(rows as f64);
        f.flush_duration
            .get_or_create(&base)
            .observe(took.as_secs_f64());
    }

    /// A table's commit landed: the rows it wrote, and how long it took.
    pub fn committed(&self, rows: u64, took: Duration) {
        let f = &self.families;
        f.rows_written.get_or_create(&self.base()).inc_by(rows);
        f.sink_commit_duration
            .get_or_create(&self.labels().sink())
            .observe(took.as_secs_f64());
    }

    /// A commit attempt failed: retried, or refused.
    pub fn commit_failed(&self) {
        self.families
            .sink_commit_failures
            .get_or_create(&self.labels().sink())
            .inc();
    }

    pub fn chunk_read(&self, took: Duration) {
        self.families
            .snapshot_chunk_duration
            .get_or_create(&self.base())
            .observe(took.as_secs_f64());
    }

    /// The running worker's totals. A total below the last one seen is a new worker's: it counts
    /// from zero.
    pub fn totals(&self, now: Totals) {
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let last = seen.totals;
        let f = &self.families;
        let base = self.base();
        let add = |counter: &Family<Labels, Counter>, now: u64, last: u64| {
            let by = if now >= last { now - last } else { now };
            counter.get_or_create(&base).inc_by(by);
        };
        add(
            &f.source_reconnects,
            now.source_reconnects,
            last.source_reconnects,
        );
        add(
            &f.sink_merge_files_rewritten,
            now.sink_merge_files_rewritten,
            last.sink_merge_files_rewritten,
        );
        add(
            &f.sink_merge_rows_copied,
            now.sink_merge_rows_copied,
            last.sink_merge_rows_copied,
        );
        for op in RowOp::ALL {
            let (now, last) = (now.rows_read.get(op), last.rows_read.get(op));
            let by = if now >= last { now - last } else { now };
            f.rows_read
                .get_or_create(&self.labels().with(&[("op", op.label())]))
                .inc_by(by);
        }
        seen.totals = now;
    }

    /// The worker that reported the totals so far ended: the next one counts from zero.
    pub fn worker_ended(&self) {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .totals = Totals::default();
    }

    /// Drops the pipeline's series: the node no longer owns it. The renew failures stay, a
    /// counter for the node's whole run: the loss that ends an ownership is still scraped after
    /// it, and a later term counts on.
    pub fn remove(&self) {
        let f = &self.families;
        let base = self.base();
        for s in PipelineStatus::ALL {
            f.pipeline_status
                .remove(&self.labels().with(&[("status", s.label())]));
        }
        for s in LeaseState::ALL {
            f.lease_state
                .remove(&self.labels().with(&[("state", s.label())]));
        }
        for family in [
            &f.lease_renew_latency,
            &f.flush_duration,
            &f.flush_rows,
            &f.snapshot_chunk_duration,
        ] {
            family.remove(&base);
        }
        f.sink_commit_duration.remove(&self.labels().sink());
        for family in [
            &f.wal_lag_bytes,
            &f.halted_tables,
            &f.reconcile_log_entries,
            &f.reconcile_log_keys,
            &f.snapshot_rows_done,
            &f.transaction_buffer_bytes,
            &f.sink_soft_dropped_columns,
        ] {
            family.remove(&base);
        }
        for family in [&f.replication_lag, &f.binlog_lag] {
            family.remove(&base);
        }
        for family in [
            &f.source_reconnects,
            &f.sink_merge_files_rewritten,
            &f.sink_merge_rows_copied,
            &f.rows_written,
        ] {
            family.remove(&base);
        }
        f.sink_commit_failures.remove(&self.labels().sink());
        for op in RowOp::ALL {
            f.rows_read
                .remove(&self.labels().with(&[("op", op.label())]));
        }
        for s in SnapshotState::ALL {
            f.snapshot_state
                .remove(&self.labels().with(&[("state", s.label())]));
        }
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        for table in std::mem::take(&mut seen.tables) {
            f.snapshot_rows_done
                .remove(&self.labels().with(&[("table", &table)]));
        }
        for table in std::mem::take(&mut seen.state_tables) {
            for s in SnapshotState::ALL {
                f.snapshot_state.remove(
                    &self
                        .labels()
                        .with(&[("table", &table), ("state", s.label())]),
                );
            }
        }
        seen.totals = Totals::default();
    }
}
