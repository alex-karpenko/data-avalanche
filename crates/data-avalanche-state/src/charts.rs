//! The chart history (Admin_Console §7): what each node samples of the series it owns, kept in
//! `globals.state` as `history/<YYYY-MM-DD>/<scope>/<HH>-<writer>.json`. One object holds one
//! scope's hour as one writer saw it: a pipeline's holder over one lease term (`e<epoch>`), or a
//! node's process (`i<incarnation>`). Its writer rewrites it while the hour lasts, so no two
//! writers share an object and a failover's two terms are both kept. A day lists its scope's hours
//! in one listing, and a whole day expires by deleting its folder.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, NaiveDate, TimeDelta, Timelike, Utc};
use data_avalanche_storage::escape_segment;
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};

use crate::{DocStore, Document, StateError};

/// The folder of the chart history, under `globals.state`.
pub const CHARTS_PREFIX: &str = "history";

/// How many chunks a read loads at once.
const READ_CONCURRENCY: usize = 16;

/// The most ended hours a [`Charts`] keeps in memory; past it, the oldest go first.
pub const CLOSED_KEPT: usize = 4096;

/// How long after its hour ends a chunk is taken as final and kept in memory: only once no writer
/// may still write it. A writer tries an ended chunk again until the sampler's `UNWRITTEN_KEPT`
/// (an hour, `data_avalanche_cluster::UNWRITTEN_KEPT`) after its hour ended, so this is that hour
/// and a margin; the two move together.
pub const CLOSED_AFTER: Duration = Duration::from_secs(65 * 60);

/// What a series belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeriesScope {
    Pipeline,
    /// A pipeline's table: kept in the pipeline's chunks.
    Table,
    Node,
}

/// How samples that fall in one step of a coarser answer fold into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fold {
    Max,
    Mean,
}

/// One series the history keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Series {
    pub name: &'static str,
    pub scope: SeriesScope,
    pub unit: &'static str,
    pub fold: Fold,
    /// Between two samples.
    pub every: Duration,
}

/// Between two samples of a pipeline's or a node's series.
pub const SAMPLE_EVERY: Duration = Duration::from_secs(15);

/// Between two samples of a table's series.
pub const TABLE_SAMPLE_EVERY: Duration = Duration::from_secs(60);

const fn series(name: &'static str, scope: SeriesScope, unit: &'static str, fold: Fold) -> Series {
    Series {
        name,
        scope,
        unit,
        fold,
        every: match scope {
            SeriesScope::Table => TABLE_SAMPLE_EVERY,
            SeriesScope::Pipeline | SeriesScope::Node => SAMPLE_EVERY,
        },
    }
}

/// Every series the history keeps (D-M13e.2).
pub const SERIES: &[Series] = &[
    series("lag_seconds", SeriesScope::Pipeline, "seconds", Fold::Max),
    series(
        "changes_per_minute",
        SeriesScope::Pipeline,
        "changes/min",
        Fold::Mean,
    ),
    series("wal_held_bytes", SeriesScope::Pipeline, "bytes", Fold::Max),
    series(
        "binlog_lag_seconds",
        SeriesScope::Pipeline,
        "seconds",
        Fold::Max,
    ),
    series(
        "snapshot_rows_per_second",
        SeriesScope::Pipeline,
        "rows/s",
        Fold::Mean,
    ),
    series(
        "changes_per_minute",
        SeriesScope::Table,
        "changes/min",
        Fold::Mean,
    ),
    series("files_rewritten", SeriesScope::Table, "files", Fold::Max),
    series("commit_seconds", SeriesScope::Table, "seconds", Fold::Max),
    series("memory_bytes", SeriesScope::Node, "bytes", Fold::Max),
    series("cpu_cores", SeriesScope::Node, "cores", Fold::Mean),
    series(
        "lease_renew_seconds",
        SeriesScope::Node,
        "seconds",
        Fold::Max,
    ),
];

/// The series `name` of `scope`.
pub fn series_of(scope: SeriesScope, name: &str) -> Option<&'static Series> {
    SERIES.iter().find(|s| s.scope == scope && s.name == name)
}

/// Whose history a chunk is.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum ChartScope {
    /// A pipeline, by id: its series and its tables'.
    Pipeline(String),
    /// A node, by its name in the cluster.
    Node(String),
}

impl ChartScope {
    fn path(&self) -> String {
        match self {
            Self::Pipeline(id) => format!("pipelines/{}", escape_segment(id)),
            Self::Node(name) => format!("nodes/{}", escape_segment(name)),
        }
    }
}

/// The start of the hour `at` falls in.
pub fn hour_of(at: DateTime<Utc>) -> DateTime<Utc> {
    at.with_nanosecond(0)
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_minute(0))
        .expect("an hour's start exists")
}

/// The object of `scope`'s hour `hour` as `writer` writes it.
pub fn chart_key(scope: &ChartScope, hour: DateTime<Utc>, writer: &str) -> String {
    format!(
        "{CHARTS_PREFIX}/{}/{}/{}-{}.json",
        hour.format("%Y-%m-%d"),
        scope.path(),
        hour.format("%H"),
        escape_segment(writer)
    )
}

/// A series' samples in one hour: each at `t` seconds into the hour, of value `v`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Points {
    pub t: Vec<u16>,
    pub v: Vec<f64>,
}

impl Points {
    pub fn push(&mut self, t: u16, v: f64) {
        self.t.push(t);
        self.v.push(v);
    }

    pub fn is_empty(&self) -> bool {
        self.t.is_empty()
    }

    /// Each sample, `(seconds into the hour, value)`; a document whose two lists differ in length
    /// stops at the shorter.
    pub fn iter(&self) -> impl Iterator<Item = (u16, f64)> + '_ {
        self.t.iter().copied().zip(self.v.iter().copied())
    }
}

/// One scope's hour as one writer sampled it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChartChunk {
    pub scope: ChartScope,
    /// The hour's start.
    pub hour: DateTime<Utc>,
    /// The lease term (`e<epoch>`) or the process (`i<incarnation>`) that sampled it.
    pub writer: String,
    /// The scope's own series, by name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub series: BTreeMap<String, Points>,
    /// A pipeline's tables' series: by table (its config key), then by name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tables: BTreeMap<String, BTreeMap<String, Points>>,
}

impl Document for ChartChunk {
    const KIND: &'static str = "chart history";
    const SCHEMA_VERSION: u64 = 1;
}

impl ChartChunk {
    pub fn new(scope: ChartScope, hour: DateTime<Utc>, writer: &str) -> Self {
        Self {
            scope,
            hour,
            writer: writer.to_owned(),
            series: BTreeMap::new(),
            tables: BTreeMap::new(),
        }
    }

    pub fn key(&self) -> String {
        chart_key(&self.scope, self.hour, &self.writer)
    }

    pub fn is_empty(&self) -> bool {
        self.series.values().all(Points::is_empty)
            && self
                .tables
                .values()
                .all(|s| s.values().all(Points::is_empty))
    }
}

/// The chart history in `globals.state`. Keeps the ended hours it read in memory, as they no
/// longer change.
#[derive(Debug)]
pub struct Charts {
    docs: DocStore,
    closed: Mutex<BTreeMap<String, Arc<ChartChunk>>>,
}

impl Charts {
    pub fn new(docs: DocStore) -> Self {
        Self {
            docs,
            closed: Mutex::new(BTreeMap::new()),
        }
    }

    /// Writes `chunk` over what its writer wrote of the hour before, and lets go of the copy of it
    /// held as final, if any.
    ///
    /// # Errors
    /// A storage failure.
    pub async fn write(&self, chunk: &ChartChunk) -> Result<(), StateError> {
        let key = chunk.key();
        let written = self.docs.put(&key, chunk).await.map(|_| ());
        self.closed.lock().expect("unpoisoned").remove(&key);
        written
    }

    /// `scope`'s chunks of the hours that meet `[since, until)`, each writer's apart, by hour. A
    /// chunk that can't be read (another schema version, corrupt) is left out with a warning.
    ///
    /// # Errors
    /// A storage failure.
    pub async fn read(
        &self,
        scope: &ChartScope,
        since: DateTime<Utc>,
        until: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Vec<Arc<ChartChunk>>, StateError> {
        if since >= until {
            return Ok(Vec::new());
        }
        let store = self.docs.store();
        let path = scope.path();
        let mut keys = Vec::new();
        let mut day = hour_of(since).date_naive();
        let last = (until - TimeDelta::microseconds(1)).date_naive();
        while day <= last {
            let folder = format!("{CHARTS_PREFIX}/{}/{path}", day.format("%Y-%m-%d"));
            for key in store.list(&folder).await? {
                let hour = key
                    .strip_prefix(&format!("{folder}/"))
                    .and_then(|name| name.get(..2))
                    .and_then(|h| h.parse::<u32>().ok())
                    .and_then(|h| day.and_hms_opt(h, 0, 0))
                    .map(|t| t.and_utc());
                if let Some(hour) = hour
                    && key.ends_with(".json")
                    && hour < until
                    && hour + TimeDelta::hours(1) > since
                {
                    keys.push((hour, key));
                }
            }
            day = day.succ_opt().expect("a next day");
        }
        let closed_before = now - TimeDelta::from_std(CLOSED_AFTER).expect("minutes");
        let read: Vec<Option<Arc<ChartChunk>>> = stream::iter(keys)
            .map(|(hour, key)| async move {
                let kept = hour + TimeDelta::hours(1) <= closed_before;
                if kept && let Some(c) = self.closed.lock().expect("unpoisoned").get(&key) {
                    return Ok(Some(Arc::clone(c)));
                }
                match self.docs.load::<ChartChunk>(&key).await {
                    Ok(Some(v)) => {
                        let chunk = Arc::new(v.doc);
                        if kept {
                            let mut closed = self.closed.lock().expect("unpoisoned");
                            closed.insert(key, Arc::clone(&chunk));
                            while closed.len() > CLOSED_KEPT {
                                // Keys start with the day: the first is of the oldest day.
                                closed.pop_first();
                            }
                        }
                        Ok(Some(chunk))
                    }
                    Ok(None) => Ok(None),
                    Err(StateError::Store(e)) => Err(StateError::Store(e)),
                    Err(e) => {
                        tracing::warn!(key, "a chart history chunk left out: {e}");
                        Ok(None)
                    }
                }
            })
            .buffered(READ_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(read.into_iter().flatten().collect())
    }

    /// Deletes every chunk of the days that ended `retention` or more before `now`, and returns
    /// how many, as [`crate::Events::prune`] does: whole days, each in one batched delete; safe
    /// on every node at once.
    ///
    /// # Errors
    /// A storage failure; what was deleted before it stays deleted.
    pub async fn prune(
        &self,
        retention: Duration,
        now: DateTime<Utc>,
    ) -> Result<usize, StateError> {
        let Some(cutoff) = TimeDelta::from_std(retention)
            .ok()
            .and_then(|r| now.checked_sub_signed(r))
        else {
            return Ok(0);
        };
        let keep_from = cutoff.format("%Y-%m-%d").to_string();
        let store = self.docs.store();
        let mut deleted = 0;
        for day in store.folders(CHARTS_PREFIX).await? {
            if day.parse::<NaiveDate>().is_err() || day >= keep_from {
                continue;
            }
            let keys = store.list(&format!("{CHARTS_PREFIX}/{day}")).await?;
            store.delete_many(&keys).await?;
            deleted += keys.len();
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use data_avalanche_storage::{ObjectStoreVersioned, VersionedStore};
    use object_store::memory::InMemory;

    use super::*;

    fn charts() -> (Charts, Arc<dyn VersionedStore>) {
        let store: Arc<dyn VersionedStore> =
            Arc::new(ObjectStoreVersioned::new(Arc::new(InMemory::new()), "g").unwrap());
        (Charts::new(DocStore::new(Arc::clone(&store))), store)
    }

    fn at(d: u32, h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, d, h, m, 0).unwrap()
    }

    fn chunk(scope: &ChartScope, hour: DateTime<Utc>, writer: &str, v: f64) -> ChartChunk {
        let mut c = ChartChunk::new(scope.clone(), hour, writer);
        c.series
            .entry("lag_seconds".into())
            .or_default()
            .push(15, v);
        c
    }

    #[test]
    fn a_chunk_is_keyed_by_day_scope_hour_and_writer() {
        let p = ChartScope::Pipeline("orders/eu".into());
        assert_eq!(
            chart_key(&p, at(18, 9, 0), "e7"),
            "history/2026-11-18/pipelines/orders%2Feu/09-e7.json"
        );
        let n = ChartScope::Node("node-a".into());
        assert_eq!(
            chart_key(&n, at(18, 23, 0), "i3"),
            "history/2026-11-18/nodes/node-a/23-i3.json"
        );
        assert_eq!(hour_of(at(18, 9, 41)), at(18, 9, 0));
    }

    #[tokio::test]
    async fn a_read_returns_every_writer_of_the_hours_it_meets() {
        let (charts, _) = charts();
        let p = ChartScope::Pipeline("p".into());
        let other = ChartScope::Pipeline("q".into());
        for c in [
            chunk(&p, at(17, 23, 0), "e1", 1.0),
            chunk(&p, at(18, 0, 0), "e1", 2.0),
            // A failover in the hour: the next term writes its own chunk.
            chunk(&p, at(18, 0, 0), "e2", 3.0),
            chunk(&p, at(18, 2, 0), "e2", 4.0),
            chunk(&other, at(18, 0, 0), "e1", 9.0),
        ] {
            charts.write(&c).await.unwrap();
        }
        let read = charts
            .read(&p, at(17, 23, 30), at(18, 1, 0), at(18, 3, 0))
            .await
            .unwrap();
        let got: Vec<(DateTime<Utc>, &str)> =
            read.iter().map(|c| (c.hour, c.writer.as_str())).collect();
        assert_eq!(
            got,
            [
                (at(17, 23, 0), "e1"),
                (at(18, 0, 0), "e1"),
                (at(18, 0, 0), "e2")
            ]
        );
        // An hour that only touches the range's end is left out.
        assert!(
            charts
                .read(&p, at(18, 1, 0), at(18, 2, 0), at(18, 3, 0))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn an_ended_hour_is_read_once_and_the_current_one_every_time() {
        let (charts, store) = charts();
        // Another node's writer, over the same store.
        let writer = Charts::new(DocStore::new(Arc::clone(&store)));
        let p = ChartScope::Pipeline("p".into());
        writer
            .write(&chunk(&p, at(18, 0, 0), "e1", 1.0))
            .await
            .unwrap();
        writer
            .write(&chunk(&p, at(18, 1, 0), "e1", 1.0))
            .await
            .unwrap();
        // The 0 o'clock hour ended over CLOSED_AFTER ago; the 1 o'clock one not yet.
        let now = at(18, 2, 30);
        charts.read(&p, at(18, 0, 0), now, now).await.unwrap();
        // Both rewritten behind the reader's back: only the hour not yet final is read again.
        writer
            .write(&chunk(&p, at(18, 0, 0), "e1", 5.0))
            .await
            .unwrap();
        writer
            .write(&chunk(&p, at(18, 1, 0), "e1", 6.0))
            .await
            .unwrap();
        let read = charts.read(&p, at(18, 0, 0), now, now).await.unwrap();
        let values: Vec<f64> = read.iter().map(|c| c.series["lag_seconds"].v[0]).collect();
        assert_eq!(values, [1.0, 6.0]);
        // A chunk that can't be read is left out.
        store
            .put(
                &chart_key(&p, at(18, 1, 0), "e9"),
                bytes::Bytes::from_static(b"{not json"),
            )
            .await
            .unwrap();
        assert_eq!(
            charts.read(&p, at(18, 0, 0), now, now).await.unwrap().len(),
            2
        );
    }

    /// A writer tries an ended hour again until `UNWRITTEN_KEPT` after it ended (a store that
    /// recovers from an outage): a read before then doesn't take the hour as final.
    #[tokio::test]
    async fn an_hour_its_writer_may_still_write_is_read_again() {
        let (charts, store) = charts();
        let writer = Charts::new(DocStore::new(Arc::clone(&store)));
        let p = ChartScope::Pipeline("p".into());
        writer
            .write(&chunk(&p, at(18, 0, 0), "e1", 1.0))
            .await
            .unwrap();
        // Half an hour after the hour ended.
        let now = at(18, 1, 30);
        charts.read(&p, at(18, 0, 0), now, now).await.unwrap();
        writer
            .write(&chunk(&p, at(18, 0, 0), "e1", 5.0))
            .await
            .unwrap();
        let now = at(18, 1, 45);
        let read = charts.read(&p, at(18, 0, 0), now, now).await.unwrap();
        assert_eq!(read[0].series["lag_seconds"].v, [5.0]);
    }

    /// A node's own write of an hour it holds as final replaces what it holds.
    #[tokio::test]
    async fn a_write_lets_go_of_the_hour_held_as_final() {
        let (charts, _) = charts();
        let p = ChartScope::Pipeline("p".into());
        charts
            .write(&chunk(&p, at(18, 0, 0), "e1", 1.0))
            .await
            .unwrap();
        let now = at(18, 3, 0);
        charts.read(&p, at(18, 0, 0), now, now).await.unwrap();
        charts
            .write(&chunk(&p, at(18, 0, 0), "e1", 5.0))
            .await
            .unwrap();
        let read = charts.read(&p, at(18, 0, 0), now, now).await.unwrap();
        assert_eq!(read[0].series["lag_seconds"].v, [5.0]);
    }

    #[tokio::test]
    async fn whole_days_past_the_retention_are_deleted() {
        let (charts, store) = charts();
        let p = ChartScope::Pipeline("p".into());
        let n = ChartScope::Node("node-a".into());
        for c in [
            chunk(&p, at(16, 22, 0), "e1", 1.0),
            chunk(&n, at(16, 23, 0), "i1", 1.0),
            chunk(&p, at(17, 9, 0), "e1", 1.0),
            chunk(&p, at(18, 9, 0), "e1", 1.0),
        ] {
            charts.write(&c).await.unwrap();
        }
        // 24 h before 2026-11-18 10:00 is the 17th: the 16th ended past it.
        let deleted = charts
            .prune(Duration::from_secs(24 * 3600), at(18, 10, 0))
            .await
            .unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(
            store.folders(CHARTS_PREFIX).await.unwrap(),
            ["2026-11-17", "2026-11-18"]
        );
        assert_eq!(charts.prune(Duration::MAX, at(18, 10, 0)).await.unwrap(), 0);
    }

    #[test]
    fn the_series_are_named_once_per_scope() {
        for s in SERIES {
            assert_eq!(
                SERIES
                    .iter()
                    .filter(|o| o.scope == s.scope && o.name == s.name)
                    .count(),
                1,
                "{}",
                s.name
            );
        }
        assert_eq!(
            series_of(SeriesScope::Table, "commit_seconds").map(|s| s.every),
            Some(TABLE_SAMPLE_EVERY)
        );
        assert!(series_of(SeriesScope::Node, "lag_seconds").is_none());
    }
}
