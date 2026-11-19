//! The history sampler (Admin_Console §7, D-M13e.3): every [`SAMPLE_EVERY`] each node samples the
//! series it owns, those of the pipelines whose lease it holds (their tables' every
//! [`TABLE_SAMPLE_EVERY`], and a table new to the term's chunk at once) and its own, into the
//! current hour's chunks. A chunk is written when it gets its first sample or a new series, then
//! every [`WRITE_EVERY`] while it changes, and once more when its hour or its lease term ends. A write that fails is tried again at the next sample; the samples wait
//! in memory, never holding up a pipeline. Another task expires the days past
//! `globals.console.history.retention`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use data_avalanche_core::TableRef;
use data_avalanche_state::{
    CLOSED_AFTER, ChartChunk, ChartScope, Charts, SAMPLE_EVERY, TABLE_SAMPLE_EVERY, chart_key,
    hour_of,
};
use data_avalanche_telemetry::{Metrics, ProcessFigures};
use futures_util::{StreamExt, stream};
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;

use crate::controller::{Actual, ControllerStatus};

/// Between two writes of a chunk that keeps changing.
pub const WRITE_EVERY: Duration = Duration::from_secs(60);

/// How long after its hour ends a chunk that can't be written is still kept for another try;
/// past it, its samples are dropped with a warning. A reader takes an hour as final only
/// [`CLOSED_AFTER`] after it ended, past this: the two move together.
pub const UNWRITTEN_KEPT: Duration = Duration::from_secs(3600);

// No reader holds an hour as final while its writer may still write it.
const _: () = assert!(UNWRITTEN_KEPT.as_secs() < CLOSED_AFTER.as_secs());

/// How often the expired days are deleted.
pub const HISTORY_PRUNE_EVERY: Duration = Duration::from_secs(3600);

/// How many chunks are written at once.
const WRITE_CONCURRENCY: usize = 8;

/// A chunk being sampled, or ended and waiting for its last write.
#[derive(Debug)]
struct Open {
    chunk: ChartChunk,
    /// Samples since the last write.
    dirty: bool,
    /// A series it didn't have at the last write: written at the next call.
    fresh: bool,
    /// The last write; `None` before the first.
    written_at: Option<DateTime<Utc>>,
    /// Its hour or its lease term ended: written once more, then dropped.
    ended: bool,
}

/// The node's sampler: what it samples and holds until written.
#[derive(Debug)]
pub struct Sampler {
    charts: Arc<Charts>,
    node: String,
    incarnation: u64,
    /// By chunk key.
    open: BTreeMap<String, Open>,
    /// Per pipeline, its snapshots' rows done at the last sample, and its term's writer.
    copied: BTreeMap<String, (String, DateTime<Utc>, u64)>,
}

/// The values one sample adds to a scope: the scope's own series, then each table's.
type Values = (
    Vec<(&'static str, f64)>,
    Vec<(String, Vec<(&'static str, f64)>)>,
);

impl Sampler {
    /// The sampler of node `node`, whose process is `incarnation`.
    pub fn new(charts: Arc<Charts>, node: &str, incarnation: u64) -> Self {
        Self {
            charts,
            node: node.to_owned(),
            incarnation,
            open: BTreeMap::new(),
            copied: BTreeMap::new(),
        }
    }

    /// Samples at `now`: each pipeline in `statuses` this node runs (its tables too when
    /// `tables`), and the node, from `process` and the slowest lease renewal since the last
    /// sample. A chunk of a scope this sample doesn't reach (a lease gone) or of another hour or
    /// term ends.
    pub fn sample(
        &mut self,
        now: DateTime<Utc>,
        statuses: &BTreeMap<String, ControllerStatus>,
        process: &ProcessFigures,
        slowest_renewal: Option<Duration>,
        tables: bool,
    ) {
        let hour = hour_of(now);
        let t = u16::try_from((now - hour).num_seconds()).unwrap_or(u16::MAX);
        let mut current = Vec::new();
        for (id, s) in statuses {
            let (Some(epoch), Some(actual)) = (s.owner_epoch, s.actual.as_ref()) else {
                continue;
            };
            if matches!(actual, Actual::Standby { .. }) {
                continue;
            }
            let writer = format!("e{epoch}");
            let scope = ChartScope::Pipeline(id.clone());
            let known: BTreeSet<String> = self
                .open
                .get(&chart_key(&scope, hour, &writer))
                .map(|o| o.chunk.tables.keys().cloned().collect())
                .unwrap_or_default();
            let values = self.pipeline(id, &writer, s, now, tables, &known);
            current.push(self.add(scope, hour, &writer, t, values));
        }
        let r = &process.reading;
        let node = [
            ("memory_bytes", r.memory_bytes.map(|b| b as f64)),
            ("cpu_cores", process.cpu_cores),
            (
                "lease_renew_seconds",
                slowest_renewal.map(|d| d.as_secs_f64()),
            ),
        ];
        let values = (
            node.into_iter()
                .filter_map(|(n, v)| v.map(|v| (n, v)))
                .collect(),
            Vec::new(),
        );
        let writer = format!("i{}", self.incarnation);
        let scope = ChartScope::Node(self.node.clone());
        current.push(self.add(scope, hour, &writer, t, values));
        for (key, open) in &mut self.open {
            open.ended = !current.contains(key);
        }
        self.copied
            .retain(|id, _| statuses.get(id).is_some_and(|s| s.worker.is_some()));
    }

    /// A pipeline's values at `now`, from its controller's status.
    fn pipeline(
        &mut self,
        id: &str,
        writer: &str,
        s: &ControllerStatus,
        now: DateTime<Utc>,
        tables: bool,
        known: &BTreeSet<String>,
    ) -> Values {
        let Some(w) = s.worker.as_ref() else {
            return (Vec::new(), Vec::new());
        };
        let mut own = Vec::new();
        if let Some(fresh) = s.fresh_as_of(now) {
            own.push(("lag_seconds", (now - fresh).as_seconds_f64().max(0.0)));
        }
        let changes: u64 = w.tables.values().map(|f| f.changes_per_minute(now)).sum();
        own.push(("changes_per_minute", changes as f64));
        if let Some(b) = w.retained_bytes {
            own.push(("wal_held_bytes", b as f64));
        }
        if let Some(d) = w.source_behind {
            own.push(("binlog_lag_seconds", d.as_secs_f64()));
        }
        if w.snapshots.is_empty() {
            self.copied.remove(id);
        } else {
            let rows: u64 = w.snapshots.values().map(|p| p.rows_done).sum();
            if let Some((was_writer, at, was)) = self.copied.get(id)
                && was_writer == writer
                && rows >= *was
                && now > *at
            {
                let secs = (now - *at).as_seconds_f64();
                own.push(("snapshot_rows_per_second", (rows - was) as f64 / secs));
            }
            self.copied
                .insert(id.to_owned(), (writer.to_owned(), now, rows));
        }
        let mut each = Vec::new();
        // Every table the worker tracks or committed to; one the term's chunk hasn't sampled yet
        // at once, the others every [`TABLE_SAMPLE_EVERY`].
        let names: BTreeSet<&TableRef> = w.tracked.iter().chain(w.tables.keys()).collect();
        for table in names {
            let name = table.to_string();
            if !tables && known.contains(&name) {
                continue;
            }
            let f = w.tables.get(table);
            let changes = f.map_or(0, |f| f.changes_per_minute(now));
            let mut v = vec![("changes_per_minute", changes as f64)];
            if let Some(n) = f.and_then(|f| f.most_rewritten(now)) {
                v.push(("files_rewritten", n as f64));
            }
            if let Some(d) = f.and_then(|f| f.slowest_commit(now)) {
                v.push(("commit_seconds", d.as_secs_f64()));
            }
            each.push((name, v));
        }
        (own, each)
    }

    /// Adds `values` at `t` to `scope`'s chunk of `hour` by `writer`, and returns its key. No
    /// values open no chunk.
    fn add(
        &mut self,
        scope: ChartScope,
        hour: DateTime<Utc>,
        writer: &str,
        t: u16,
        (own, tables): Values,
    ) -> String {
        let chunk = ChartChunk::new(scope, hour, writer);
        let key = chunk.key();
        if own.is_empty() && tables.iter().all(|(_, v)| v.is_empty()) {
            return key;
        }
        let open = self.open.entry(key.clone()).or_insert(Open {
            chunk,
            dirty: false,
            fresh: false,
            written_at: None,
            ended: false,
        });
        for (name, v) in own {
            open.fresh |= !open.chunk.series.contains_key(name);
            open.chunk.series.entry(name.into()).or_default().push(t, v);
            open.dirty = true;
        }
        for (table, values) in tables {
            let series = open.chunk.tables.entry(table).or_default();
            for (name, v) in values {
                open.fresh |= !series.contains_key(name);
                series.entry(name.into()).or_default().push(t, v);
                open.dirty = true;
            }
        }
        key
    }

    /// Writes the chunks due at `now`: one never written, one with a new series, one that changed
    /// [`WRITE_EVERY`] or more after its last write, and one that ended. A written chunk that ended is dropped; one
    /// that can't be written stays for the next call, until [`UNWRITTEN_KEPT`] after its hour.
    /// Returns how many writes failed.
    pub async fn write(&mut self, now: DateTime<Utc>) -> usize {
        let every = TimeDelta::from_std(WRITE_EVERY).expect("a minute");
        let due: Vec<(String, ChartChunk)> = self
            .open
            .iter()
            .filter(|(_, o)| {
                o.dirty && (o.ended || o.fresh || o.written_at.is_none_or(|w| now - w >= every))
            })
            .map(|(k, o)| (k.clone(), o.chunk.clone()))
            .collect();
        let charts = Arc::clone(&self.charts);
        let results: Vec<(String, bool)> = stream::iter(due)
            .map(|(key, chunk)| {
                let charts = Arc::clone(&charts);
                async move {
                    match charts.write(&chunk).await {
                        Ok(()) => (key, true),
                        Err(e) => {
                            tracing::warn!(key, "chart history unwritten, tried again later: {e}");
                            (key, false)
                        }
                    }
                }
            })
            .buffer_unordered(WRITE_CONCURRENCY)
            .collect()
            .await;
        let mut failed = 0;
        for (key, ok) in results {
            if let Some(o) = self.open.get_mut(&key) {
                if ok {
                    o.dirty = false;
                    o.fresh = false;
                    o.written_at = Some(now);
                } else {
                    failed += 1;
                }
            }
        }
        let kept = TimeDelta::hours(1) + TimeDelta::from_std(UNWRITTEN_KEPT).expect("an hour");
        self.open.retain(|key, o| {
            if !o.ended {
                true
            } else if !o.dirty {
                false
            } else if now - o.chunk.hour > kept {
                tracing::warn!(
                    key,
                    "chart history dropped unwritten: the store kept failing"
                );
                false
            } else {
                true
            }
        });
        failed
    }

    /// The chunks held in memory, written or not.
    pub fn held(&self) -> usize {
        self.open.len()
    }
}

/// Samples every [`SAMPLE_EVERY`] (the tables every [`TABLE_SAMPLE_EVERY`]) the controllers
/// `statuses` lists and the process's `figures`, and writes what is due, until the task is
/// aborted.
pub async fn sample_history(
    mut sampler: Sampler,
    statuses: watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
    process: watch::Receiver<ProcessFigures>,
    metrics: Metrics,
) {
    let per_table = (TABLE_SAMPLE_EVERY.as_secs() / SAMPLE_EVERY.as_secs()).max(1);
    let mut every = tokio::time::interval(SAMPLE_EVERY);
    every.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut ticks: u64 = 0;
    loop {
        every.tick().await;
        let now = Utc::now();
        let current: BTreeMap<String, ControllerStatus> = statuses
            .borrow()
            .iter()
            .map(|(id, s)| (id.clone(), s.borrow().clone()))
            .collect();
        let figures = *process.borrow();
        sampler.sample(
            now,
            &current,
            &figures,
            metrics.take_slowest_renewal(),
            ticks.is_multiple_of(per_table),
        );
        sampler.write(now).await;
        ticks += 1;
    }
}

/// Deletes the days past the retention `retention` holds now, every [`HISTORY_PRUNE_EVERY`] (the
/// first a minute after the start), until the task is aborted. Every node runs it.
pub async fn expire_history(charts: Arc<Charts>, retention: watch::Receiver<Duration>) {
    tokio::time::sleep(Duration::from_secs(60)).await;
    loop {
        let keep = *retention.borrow();
        match charts.prune(keep, Utc::now()).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(deleted = n, "expired chart history deleted"),
            Err(e) => tracing::warn!("expired chart history undeleted, tried again later: {e}"),
        }
        tokio::time::sleep(HISTORY_PRUNE_EVERY).await;
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use data_avalanche_core::{CommitCost, TableRef};
    use data_avalanche_pipeline::{Phase, SnapshotProgress, TableCommit, WorkerStatus};
    use data_avalanche_state::DocStore;
    use data_avalanche_storage::VersionedStore;
    use data_avalanche_telemetry::Reading;

    use super::*;
    use crate::test_support::{Fault, Kind, ScriptedStore};

    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 18, h, m, s).unwrap()
    }

    fn sampler() -> (Sampler, Arc<Charts>, Arc<ScriptedStore>) {
        let store = ScriptedStore::new();
        let dyn_store: Arc<dyn VersionedStore> = store.clone();
        let charts = Arc::new(Charts::new(DocStore::new(dyn_store)));
        (
            Sampler::new(Arc::clone(&charts), "node-a", 7),
            charts,
            store,
        )
    }

    /// A running pipeline's status at `now`: 30 s behind, one table with two commits, the slot
    /// holding 4 KiB.
    fn running(epoch: u64, now: DateTime<Utc>) -> ControllerStatus {
        let mut w = WorkerStatus {
            phase: Phase::Streaming,
            retained_bytes: Some(4096),
            ..WorkerStatus::default()
        };
        for (rows, secs, millis) in [(40, 20, 120), (20, 10, 900)] {
            w.committed(&TableCommit {
                table: TableRef::new("public", "orders"),
                rows,
                snapshot: false,
                version: 3,
                cost: CommitCost {
                    files_rewritten: rows / 10,
                    files: Some(12),
                },
                at: now - TimeDelta::seconds(secs),
                took: Duration::from_millis(millis),
            });
        }
        ControllerStatus {
            actual: Some(Actual::Running),
            owner_epoch: Some(epoch),
            worker: Some(w),
            pending_since_micros: Some((now - TimeDelta::seconds(30)).timestamp_micros()),
            ..ControllerStatus::default()
        }
    }

    fn statuses(list: &[(&str, ControllerStatus)]) -> BTreeMap<String, ControllerStatus> {
        list.iter()
            .map(|(id, s)| ((*id).to_owned(), s.clone()))
            .collect()
    }

    fn figures(memory: u64, cores: f64) -> ProcessFigures {
        ProcessFigures {
            reading: Reading {
                memory_bytes: Some(memory),
                ..Reading::default()
            },
            cpu_cores: Some(cores),
        }
    }

    async fn stored(charts: &Charts, scope: &ChartScope, now: DateTime<Utc>) -> Vec<ChartChunk> {
        charts
            .read(
                scope,
                now - TimeDelta::hours(2),
                now + TimeDelta::hours(1),
                now,
            )
            .await
            .unwrap()
            .iter()
            .map(|c| (**c).clone())
            .collect()
    }

    #[tokio::test]
    async fn a_held_pipeline_and_the_node_are_sampled_and_written_at_once() {
        let (mut s, charts, _) = sampler();
        let now = at(9, 0, 15);
        let st = statuses(&[
            ("orders", running(4, now)),
            // A standby controller is another node's pipeline.
            (
                "other",
                ControllerStatus {
                    actual: Some(Actual::Standby { holder: None }),
                    ..ControllerStatus::default()
                },
            ),
        ]);
        s.sample(
            now,
            &st,
            &figures(1 << 30, 0.5),
            Some(Duration::from_millis(41)),
            true,
        );
        assert_eq!(s.write(now).await, 0);
        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), now).await;
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].writer, "e4");
        let series = |name: &str| (p[0].series[name].t.clone(), p[0].series[name].v.clone());
        assert_eq!(series("lag_seconds"), (vec![15], vec![30.0]));
        assert_eq!(series("changes_per_minute"), (vec![15], vec![60.0]));
        assert_eq!(series("wal_held_bytes"), (vec![15], vec![4096.0]));
        assert!(!p[0].series.contains_key("binlog_lag_seconds"));
        let t = &p[0].tables["public.orders"];
        assert_eq!(t["changes_per_minute"].v, [60.0]);
        assert_eq!(t["files_rewritten"].v, [4.0]);
        assert_eq!(t["commit_seconds"].v, [0.9]);
        assert!(
            stored(&charts, &ChartScope::Pipeline("other".into()), now)
                .await
                .is_empty()
        );
        let n = stored(&charts, &ChartScope::Node("node-a".into()), now).await;
        assert_eq!(n[0].writer, "i7");
        assert_eq!(n[0].series["memory_bytes"].v, [f64::from(1u32 << 30)]);
        assert_eq!(n[0].series["cpu_cores"].v, [0.5]);
        assert_eq!(n[0].series["lease_renew_seconds"].v, [0.041]);
    }

    #[tokio::test]
    async fn a_chunk_is_written_each_minute_while_it_changes() {
        let (mut s, charts, _) = sampler();
        let none = ProcessFigures::default();
        let mut now = at(9, 0, 0);
        let mut writes = Vec::new();
        for _ in 0..8 {
            let st = statuses(&[("orders", running(4, now))]);
            s.sample(now, &st, &none, None, false);
            s.write(now).await;
            let p = stored(&charts, &ChartScope::Pipeline("orders".into()), now).await;
            writes.push(p[0].series["lag_seconds"].t.len());
            now += TimeDelta::seconds(15);
        }
        // Written with the first sample, then a minute later, and a minute after that.
        assert_eq!(writes, [1, 1, 1, 1, 5, 5, 5, 5]);
        // A table is sampled when it is new to the chunk, then only when asked.
        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), now).await;
        assert_eq!(p[0].tables["public.orders"]["changes_per_minute"].t, [0]);
    }

    /// A table the worker starts tracking is sampled at once, and its chunk written at the next
    /// write rather than a minute after the last: a page sees a new table within a sample.
    #[tokio::test]
    async fn a_table_new_to_the_term_is_sampled_and_written_at_once() {
        let (mut s, charts, _) = sampler();
        let none = ProcessFigures::default();
        let t1 = at(9, 0, 0);
        s.sample(
            t1,
            &statuses(&[("orders", running(4, t1))]),
            &none,
            None,
            false,
        );
        s.write(t1).await;
        let t2 = at(9, 0, 15);
        let mut st = running(4, t2);
        st.worker
            .as_mut()
            .unwrap()
            .tracked
            .insert(TableRef::new("public", "items"));
        s.sample(t2, &statuses(&[("orders", st)]), &none, None, false);
        s.write(t2).await;
        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), t2).await;
        let items = &p[0].tables["public.items"];
        assert_eq!(items["changes_per_minute"].t, [15]);
        assert_eq!(items["changes_per_minute"].v, [0.0], "no commit yet");
        assert!(!items.contains_key("commit_seconds"));
        assert_eq!(p[0].tables["public.orders"]["changes_per_minute"].t, [0]);
    }

    /// A failover in the hour: the next term writes its own chunk, and the last one of the
    /// earlier term is written once more and let go.
    #[tokio::test]
    async fn a_new_term_or_hour_ends_the_chunk_before_it() {
        let (mut s, charts, _) = sampler();
        let none = ProcessFigures::default();
        let t1 = at(9, 59, 30);
        s.sample(
            t1,
            &statuses(&[("orders", running(4, t1))]),
            &none,
            None,
            false,
        );
        s.write(t1).await;
        let t2 = at(9, 59, 45);
        s.sample(
            t2,
            &statuses(&[("orders", running(4, t2))]),
            &none,
            None,
            false,
        );
        s.write(t2).await;
        // The hour ends: its chunk is written with its last sample.
        let t3 = at(10, 0, 0);
        s.sample(
            t3,
            &statuses(&[("orders", running(4, t3))]),
            &none,
            None,
            false,
        );
        s.write(t3).await;
        // The lease moves to epoch 5 on this node: the epoch-4 chunk ends.
        let t4 = at(10, 0, 15);
        s.sample(
            t4,
            &statuses(&[("orders", running(5, t4))]),
            &none,
            None,
            false,
        );
        s.write(t4).await;
        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), t4).await;
        let got: Vec<(DateTime<Utc>, &str, Vec<u16>)> = p
            .iter()
            .map(|c| (c.hour, c.writer.as_str(), c.series["lag_seconds"].t.clone()))
            .collect();
        assert_eq!(
            got,
            [
                (at(9, 0, 0), "e4", vec![3570, 3585]),
                (at(10, 0, 0), "e4", vec![0]),
                (at(10, 0, 0), "e5", vec![15]),
            ]
        );
        // The lease lost: the pipeline's chunks are let go once written.
        let t5 = at(10, 0, 30);
        s.sample(t5, &statuses(&[]), &none, None, false);
        s.write(t5).await;
        assert_eq!(s.held(), 0);
    }

    /// A storage outage: the samples wait in memory and land with the next write that goes
    /// through; an hour that never could is dropped an hour after it ended.
    #[tokio::test]
    async fn samples_wait_through_a_storage_outage() {
        let (mut s, charts, store) = sampler();
        let none = ProcessFigures::default();
        store.fail_always(Some(Fault {
            kind: Kind::Transient,
            lands: false,
        }));
        let t1 = at(9, 0, 0);
        s.sample(
            t1,
            &statuses(&[("orders", running(4, t1))]),
            &none,
            None,
            false,
        );
        assert_eq!(s.write(t1).await, 1);
        let t2 = at(9, 0, 15);
        s.sample(
            t2,
            &statuses(&[("orders", running(4, t2))]),
            &none,
            None,
            false,
        );
        assert_eq!(s.write(t2).await, 1, "tried again at the next sample");
        store.fail_always(None);
        let t3 = at(9, 0, 30);
        s.sample(
            t3,
            &statuses(&[("orders", running(4, t3))]),
            &none,
            None,
            false,
        );
        assert_eq!(s.write(t3).await, 0);
        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), t3).await;
        assert_eq!(p[0].series["lag_seconds"].t, [0, 15, 30]);
        // Down again to the hour's end: the 9 o'clock chunk's last samples can't be written.
        store.fail_always(Some(Fault {
            kind: Kind::Transient,
            lands: false,
        }));
        let late = at(9, 59, 45);
        s.sample(
            late,
            &statuses(&[("orders", running(4, late))]),
            &none,
            None,
            false,
        );
        s.write(late).await;
        let t4 = at(10, 0, 0);
        s.sample(
            t4,
            &statuses(&[("orders", running(4, t4))]),
            &none,
            None,
            false,
        );
        s.write(t4).await;
        assert_eq!(s.held(), 2);
        let t5 = at(11, 0, 1);
        s.sample(t5, &statuses(&[]), &none, None, false);
        s.write(t5).await;
        assert_eq!(
            s.held(),
            1,
            "the 9 o'clock chunk dropped, the 10 o'clock one kept"
        );
    }

    /// A snapshot's rate is its rows done between two samples of one term: none at the first
    /// sample, nor across terms.
    #[tokio::test]
    async fn a_snapshot_s_rate_is_its_rows_between_samples() {
        let (mut s, charts, _) = sampler();
        let none = ProcessFigures::default();
        let copying = |epoch, now, rows| {
            let mut st = running(epoch, now);
            st.worker.as_mut().unwrap().snapshots.insert(
                TableRef::new("public", "orders"),
                SnapshotProgress {
                    rows_done: rows,
                    waiting: false,
                },
            );
            st
        };
        let t1 = at(9, 0, 0);
        s.sample(
            t1,
            &statuses(&[("orders", copying(4, t1, 1_000))]),
            &none,
            None,
            false,
        );
        let t2 = at(9, 0, 15);
        s.sample(
            t2,
            &statuses(&[("orders", copying(4, t2, 31_000))]),
            &none,
            None,
            false,
        );
        let t3 = at(9, 0, 30);
        s.sample(
            t3,
            &statuses(&[("orders", copying(5, t3, 40_000))]),
            &none,
            None,
            false,
        );
        s.write(t3).await;
        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), t3).await;
        let rate = |writer: &str| {
            p.iter()
                .find(|c| c.writer == writer)
                .and_then(|c| c.series.get("snapshot_rows_per_second"))
                .map(|r| r.v.clone())
        };
        assert_eq!(rate("e4"), Some(vec![2_000.0]));
        assert_eq!(rate("e5"), None);
    }
}
