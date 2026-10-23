//! The process's metrics (System-Overview §Observability catalogue): one Prometheus registry, the
//! node's gauges, and a handle per pipeline that sets that pipeline's series. M6b exports what the
//! control plane and the worker know so far; M10 completes the catalogue.
//!
//! Every pipeline series carries `{pipeline, source_host, source_db}`; a `table` label is added
//! only with `sink.metrics.per_table`, and only to `snapshot_rows_done`. Counter families get the
//! OpenMetrics `_total` suffix on their samples (`data_avalanche_source_reconnects_total`).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

/// A series' labels: the pipeline's three, then the family's own.
type Labels = Vec<(&'static str, String)>;

type HistogramFamily = Family<Labels, Histogram, fn() -> Histogram>;

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

/// The labels every series of a pipeline carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineLabels {
    pub pipeline: String,
    pub source_host: String,
    pub source_db: String,
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
}

/// Monotonic totals a worker reports since it started. A new worker starts them at zero again;
/// the counters they feed don't.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    pub source_reconnects: u64,
    pub sink_merge_files_rewritten: u64,
    pub sink_merge_rows_copied: u64,
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
}

fn renew_latency() -> Histogram {
    // 5 ms … ~10 s.
    Histogram::new(exponential_buckets(0.005, 2.0, 12))
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
    tables: BTreeSet<String>,
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
        f.lease_renew_latency.remove(&base);
        for family in [
            &f.wal_lag_bytes,
            &f.halted_tables,
            &f.reconcile_log_entries,
            &f.reconcile_log_keys,
            &f.snapshot_rows_done,
        ] {
            family.remove(&base);
        }
        for family in [
            &f.source_reconnects,
            &f.sink_merge_files_rewritten,
            &f.sink_merge_rows_copied,
        ] {
            family.remove(&base);
        }
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        for table in std::mem::take(&mut seen.tables) {
            f.snapshot_rows_done
                .remove(&self.labels().with(&[("table", &table)]));
        }
        seen.totals = Totals::default();
    }
}
