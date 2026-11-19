//! `GET /v1/history` (Admin_Console §7, D-M13e.5): the series the nodes sampled, read by any node
//! from the chart history in `globals.state`, over a range and at a step. A pipeline's series
//! follow it across its lease terms, a node's across its processes.

use axum::extract::{RawQuery, State};
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, TimeDelta, Utc};
use data_avalanche_config::{Pipeline, ResolvedConfig};
use data_avalanche_state::{
    ChartChunk, ChartScope, Fold, SERIES, Series, SeriesScope, StateError, series_of,
};
use futures_util::{StreamExt, TryStreamExt, stream};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::events::time;
use crate::snapshots::{READ_AT_ONCE, named_table};
use crate::{Api, Failure, Node, body, starting};

/// The query's parameters, as a page and `curl` write them.
pub const PARAMETERS: &[(&str, &str)] = &[
    (
        "pipeline",
        "a pipeline's id, or several comma-separated; without it or `node`, every pipeline",
    ),
    (
        "table",
        "a table of the one pipeline asked, as its config key: the table's series",
    ),
    ("node", "a node's name: the node's series"),
    (
        "series",
        "the series, comma-separated; by default every series of what is asked",
    ),
    (
        "since",
        "from this time (RFC 3339); an hour before `until` by default",
    ),
    ("until", "to this time (RFC 3339); now by default"),
    (
        "step",
        "seconds between points; by default, and at the finest, the samples' own, coarsened to at most 1440 points",
    ),
];

/// The most points a series has in an answer.
pub const MAX_POINTS: i64 = 1440;

/// The longest range a query asks.
pub const MAX_SPAN: TimeDelta = TimeDelta::days(31);

/// What a query asks the series of.
#[derive(Debug)]
enum Asked {
    Pipelines(Vec<Pipeline>),
    /// A pipeline's table: as asked (its config key), and as the history keeps it.
    Table(Box<Pipeline>, String, String),
    Node(String),
}

#[derive(Debug)]
struct Query {
    asked: Asked,
    series: Vec<&'static Series>,
    since: DateTime<Utc>,
    until: DateTime<Utc>,
    /// Seconds between points.
    step: i64,
}

fn bad(error: impl Into<String>) -> Failure {
    Failure::new(StatusCode::BAD_REQUEST, error)
}

/// The query of `params`, each once, against `config` at `now`.
fn query(
    params: &[(String, String)],
    config: &ResolvedConfig,
    now: DateTime<Utc>,
) -> Result<Query, Failure> {
    let mut given = std::collections::BTreeMap::new();
    for (name, value) in params {
        if !PARAMETERS.iter().any(|(n, _)| n == name) {
            return Err(bad(format!(
                "unknown parameter {name:?}: the history takes {}",
                PARAMETERS
                    .iter()
                    .map(|(n, _)| *n)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        if given.insert(name.as_str(), value.trim()).is_some() {
            return Err(bad(format!("{name} is given twice")));
        }
    }
    let list = |v: &str| -> Vec<String> {
        v.split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect()
    };
    let pipeline = |id: &str| {
        config
            .pipelines
            .values()
            .find(|p| p.id == id)
            .cloned()
            .ok_or_else(|| {
                Failure::new(
                    StatusCode::NOT_FOUND,
                    format!("no pipeline {id:?} in the config generation this node runs"),
                )
            })
    };
    let asked = match (
        given.get("pipeline").copied(),
        given.get("table").copied(),
        given.get("node").copied(),
    ) {
        (_, _, Some(_)) if given.contains_key("pipeline") || given.contains_key("table") => {
            return Err(bad("node is asked alone, without pipeline or table"));
        }
        (_, _, Some("")) => return Err(bad("node is a node's name")),
        (_, _, Some(node)) => Asked::Node(node.to_owned()),
        (Some(ids), Some(table), None) => {
            let ids = list(ids);
            let [id] = ids.as_slice() else {
                return Err(bad("table is asked of one pipeline"));
            };
            let p = pipeline(id)?;
            let t = named_table(&p, table).map_err(bad)?;
            Asked::Table(Box::new(p), table.to_owned(), t.to_string())
        }
        (None, Some(_), None) => return Err(bad("table is asked of one pipeline")),
        (Some(ids), None, None) => {
            let ids = list(ids);
            if ids.is_empty() {
                return Err(bad("pipeline names a pipeline's id"));
            }
            Asked::Pipelines(
                ids.iter()
                    .map(|id| pipeline(id))
                    .collect::<Result<_, _>>()?,
            )
        }
        (None, None, None) => {
            let mut all: Vec<Pipeline> = config.pipelines.values().cloned().collect();
            all.sort_by(|a, b| a.id.cmp(&b.id));
            Asked::Pipelines(all)
        }
    };
    let scope = match asked {
        Asked::Pipelines(_) => SeriesScope::Pipeline,
        Asked::Table(..) => SeriesScope::Table,
        Asked::Node(_) => SeriesScope::Node,
    };
    let series: Vec<&'static Series> = match given.get("series") {
        None => SERIES.iter().filter(|s| s.scope == scope).collect(),
        Some(names) => {
            let names = list(names);
            if names.is_empty() {
                return Err(bad("series names a series"));
            }
            let mut out = Vec::new();
            for name in names {
                let Some(s) = series_of(scope, &name) else {
                    return Err(bad(format!(
                        "no series {name:?} of a {}: {}",
                        match scope {
                            SeriesScope::Pipeline => "pipeline",
                            SeriesScope::Table => "table",
                            SeriesScope::Node => "node",
                        },
                        SERIES
                            .iter()
                            .filter(|s| s.scope == scope)
                            .map(|s| s.name)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                };
                if !out.contains(&s) {
                    out.push(s);
                }
            }
            out
        }
    };
    let until = match given.get("until") {
        Some(v) => time("until", v)?.min(now),
        None => now,
    };
    let since = match given.get("since") {
        Some(v) => time("since", v)?,
        None => until - TimeDelta::hours(1),
    };
    if since >= until {
        return Err(bad("since is before until, and until isn't past now"));
    }
    if until - since > MAX_SPAN {
        return Err(bad(format!(
            "the range is at most {} days",
            MAX_SPAN.num_days()
        )));
    }
    let span = (until - since).num_seconds().max(1);
    let native = series
        .iter()
        .map(|s| i64::try_from(s.every.as_secs()).unwrap_or(i64::MAX))
        .max()
        .unwrap_or(1);
    let coarse = (span + MAX_POINTS - 1) / MAX_POINTS;
    let default = native.max((coarse + native - 1) / native * native);
    let step = match given.get("step") {
        None => default,
        Some(v) => {
            let asked: i64 = v
                .parse()
                .ok()
                .filter(|n| (1..=span).contains(n))
                .ok_or_else(|| bad("step is a number of seconds within the range"))?;
            asked.max(default)
        }
    };
    Ok(Query {
        asked,
        series,
        since,
        until,
        step,
    })
}

/// The samples of series `name` in `chunks` (of a table when `table` names it) within
/// `[from, until)`, folded into points at multiples of `step`, oldest first.
fn points(
    chunks: &[Arc<ChartChunk>],
    table: Option<&str>,
    s: &Series,
    from: DateTime<Utc>,
    until: DateTime<Utc>,
    step: i64,
) -> Vec<(i64, f64)> {
    let (from, until) = (from.timestamp(), until.timestamp());
    let mut samples: Vec<(i64, f64)> = chunks
        .iter()
        .filter_map(|c| {
            let series = match table {
                None => c.series.get(s.name),
                Some(t) => c.tables.get(t).and_then(|t| t.get(s.name)),
            };
            series.map(|p| (c.hour.timestamp(), p))
        })
        .flat_map(|(hour, p)| p.iter().map(move |(t, v)| (hour + i64::from(t), v)))
        .filter(|(t, v)| (from..until).contains(t) && v.is_finite())
        .collect();
    samples.sort_by_key(|(t, _)| *t);
    let mut out: Vec<(i64, f64, usize)> = Vec::new();
    for (t, v) in samples {
        let at = t.div_euclid(step) * step;
        match out.last_mut() {
            Some((last, folded, n)) if *last == at => {
                *folded = match s.fold {
                    Fold::Max => folded.max(v),
                    Fold::Mean => *folded + v,
                };
                *n += 1;
            }
            _ => out.push((at, v, 1)),
        }
    }
    out.into_iter()
        .map(|(at, v, n)| match s.fold {
            Fold::Max => (at, v),
            Fold::Mean => (at, v / n as f64),
        })
        .collect()
}

/// One series of the answer.
fn series_json(who: Value, s: &Series, points: &[(i64, f64)]) -> Value {
    let mut v = who;
    v["name"] = json!(s.name);
    v["unit"] = json!(s.unit);
    v["points"] = json!(
        points
            .iter()
            .map(|(t, v)| json!([t, v]))
            .collect::<Vec<_>>()
    );
    v
}

/// Reads `scope`'s chunks over `[from, until)`; none when the range is empty.
async fn chunks(
    node: &Node,
    scope: ChartScope,
    from: DateTime<Utc>,
    until: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Vec<Arc<ChartChunk>>, StateError> {
    if from >= until {
        return Ok(Vec::new());
    }
    node.charts.read(&scope, from, until, now).await
}

/// The answer to `q` at `now`: each asked thing's series, kept since `kept_since`.
async fn answer(
    node: &Node,
    q: &Query,
    kept_since: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Vec<Value>, StateError> {
    let from = q.since.max(kept_since);
    let fold = |who: Value, chunks: &[Arc<ChartChunk>], table: Option<&str>| {
        q.series
            .iter()
            .map(|s| {
                let p = points(chunks, table, s, from, q.until, q.step);
                series_json(who.clone(), s, &p)
            })
            .collect::<Vec<_>>()
    };
    Ok(match &q.asked {
        Asked::Node(name) => {
            let c = chunks(node, ChartScope::Node(name.clone()), from, q.until, now).await?;
            fold(json!({ "node": name }), &c, None)
        }
        Asked::Table(p, key, kept) => {
            let c = chunks(node, ChartScope::Pipeline(p.id.clone()), from, q.until, now).await?;
            fold(json!({ "pipeline": p.id, "table": key }), &c, Some(kept))
        }
        Asked::Pipelines(ps) => {
            let scopes: Vec<ChartScope> = ps
                .iter()
                .map(|p| ChartScope::Pipeline(p.id.clone()))
                .collect();
            let until = q.until;
            let read: Vec<Vec<Arc<ChartChunk>>> = stream::iter(scopes)
                .map(|scope| chunks(node, scope, from, until, now))
                .buffered(READ_AT_ONCE)
                .try_collect()
                .await?;
            ps.iter()
                .zip(read)
                .flat_map(|(p, c)| fold(json!({ "pipeline": p.id }), &c, None))
                .collect()
        }
    })
}

pub(crate) async fn read(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let params: Vec<(String, String)> =
        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    let config = node.coordinator.config().await;
    let now = Utc::now();
    let q = match query(&params, &config, now) {
        Ok(q) => q,
        Err(f) => return f.into(),
    };
    let retention = config.globals.console.history.retention;
    let kept_since = now - TimeDelta::from_std(retention).unwrap_or(MAX_SPAN);
    match answer(node, &q, kept_since, now).await {
        Ok(series) => body(
            StatusCode::OK,
            &json!({
                "since": q.since,
                "until": q.until,
                "step": q.step,
                "kept_since": kept_since,
                "series": series,
            }),
        ),
        Err(e) => body(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({ "error": format!("the chart history: {e}") }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DurationRound, TimeZone};
    use data_avalanche_state::{Charts, DocStore, Points};
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;
    use crate::test_support::{config, json_of, open, served, served_with};

    fn params(q: &str) -> Vec<(String, String)> {
        url::form_urlencoded::parse(q.as_bytes())
            .into_owned()
            .collect()
    }

    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 18, h, m, s).unwrap()
    }

    #[test]
    fn a_query_names_what_it_asks_and_its_step() {
        let c = config();
        let now = at(12, 0, 0);
        let q = query(&params(""), &c, now).unwrap();
        assert!(matches!(&q.asked, Asked::Pipelines(p) if p.len() == 1));
        assert_eq!(q.series.len(), 5);
        assert_eq!((q.since, q.until, q.step), (at(11, 0, 0), now, 15));
        let q = query(
            &params("pipeline=orders&table=public.orders&since=2026-11-17T12:00:00Z"),
            &c,
            now,
        )
        .unwrap();
        assert!(
            matches!(&q.asked, Asked::Table(_, key, kept) if key == "public.orders" && kept == "public.orders")
        );
        // A day at a minute a sample: 1440 points.
        assert_eq!(q.step, 60);
        let q = query(
            &params("node=node-b&series=cpu_cores,memory_bytes&since=2026-11-11T12:00:00Z"),
            &c,
            now,
        )
        .unwrap();
        // A week: 420 s, a multiple of 15 s, keeps 1440 points.
        assert_eq!((q.series.len(), q.step), (2, 420));
        let q = query(&params("node=n&step=1"), &c, now).unwrap();
        assert_eq!(q.step, 15, "never below the samples' own");
        let week = "node=n&since=2026-11-11T12:00:00Z";
        let q = query(&params(&format!("{week}&step=15")), &c, now).unwrap();
        assert_eq!(q.step, 420, "an asked step keeps at most 1440 points");
        let q = query(&params(&format!("{week}&step=3600")), &c, now).unwrap();
        assert_eq!(q.step, 3600, "a coarser step is kept");
        let q = query(&params("until=2026-11-19T00:00:00Z"), &c, now).unwrap();
        assert_eq!(q.until, now, "never past now");
    }

    #[test]
    fn a_query_that_isn_t_one_is_refused() {
        let c = config();
        let now = at(12, 0, 0);
        for (q, code, says) in [
            ("node=a&pipeline=orders", 400, "node is asked alone"),
            ("table=public.orders", 400, "one pipeline"),
            (
                "pipeline=orders,orders&table=public.orders",
                400,
                "one pipeline",
            ),
            ("pipeline=orders&table=orders", 400, "schema.table"),
            ("pipeline=nope", 404, "no pipeline \"nope\""),
            (
                "series=memory_bytes",
                400,
                "no series \"memory_bytes\" of a pipeline",
            ),
            ("since=2026-11-18T12:00:00Z", 400, "since is before until"),
            ("since=2026-10-01T00:00:00Z", 400, "at most 31 days"),
            ("since=yesterday", 400, "RFC 3339"),
            ("step=0", 400, "step"),
            ("limit=3", 400, "unknown parameter"),
            ("node=a&node=b", 400, "given twice"),
        ] {
            let f = query(&params(q), &c, now).unwrap_err();
            assert_eq!(f.code.as_u16(), code, "{q}");
            assert!(f.error.contains(says), "{q}: {}", f.error);
        }
    }

    fn chunk(scope: ChartScope, hour: DateTime<Utc>, writer: &str) -> ChartChunk {
        ChartChunk::new(scope, hour, writer)
    }

    fn pts(list: &[(u16, f64)]) -> Points {
        let mut p = Points::default();
        for (t, v) in list {
            p.push(*t, *v);
        }
        p
    }

    /// Each term's samples meet in one series; a coarser step folds a gauge to its peak and a
    /// rate to its mean.
    #[test]
    fn samples_fold_into_points_at_the_step() {
        let p = ChartScope::Pipeline("orders".into());
        let mut a = chunk(p.clone(), at(9, 0, 0), "e4");
        a.series
            .insert("lag_seconds".into(), pts(&[(0, 2.0), (15, 9.0), (30, 3.0)]));
        a.series
            .insert("changes_per_minute".into(), pts(&[(0, 10.0), (15, 20.0)]));
        let mut b = chunk(p, at(9, 0, 0), "e5");
        b.series
            .insert("lag_seconds".into(), pts(&[(45, 1.0), (60, 7.0)]));
        let chunks = [Arc::new(a), Arc::new(b)];
        let lag = series_of(SeriesScope::Pipeline, "lag_seconds").unwrap();
        let base = at(9, 0, 0).timestamp();
        assert_eq!(
            points(&chunks, None, lag, at(9, 0, 0), at(10, 0, 0), 15),
            [
                (base, 2.0),
                (base + 15, 9.0),
                (base + 30, 3.0),
                (base + 45, 1.0),
                (base + 60, 7.0)
            ]
        );
        assert_eq!(
            points(&chunks, None, lag, at(9, 0, 0), at(10, 0, 0), 60),
            [(base, 9.0), (base + 60, 7.0)]
        );
        // Within [from, until): the first sample before it, the last at its end.
        assert_eq!(
            points(&chunks, None, lag, at(9, 0, 15), at(9, 1, 0), 15),
            [(base + 15, 9.0), (base + 30, 3.0), (base + 45, 1.0)]
        );
        let rate = series_of(SeriesScope::Pipeline, "changes_per_minute").unwrap();
        assert_eq!(
            points(&chunks, None, rate, at(9, 0, 0), at(10, 0, 0), 60),
            [(base, 15.0)]
        );
    }

    async fn written(charts: &Charts, chunks: Vec<ChartChunk>) {
        for c in chunks {
            charts.write(&c).await.unwrap();
        }
    }

    #[tokio::test]
    async fn the_history_answers_from_storage_through_any_node() {
        let charts = Arc::new(Charts::new(DocStore::new(Arc::new(
            ObjectStoreVersioned::new(Arc::new(InMemory::new()), "g").unwrap(),
        ))));
        let now = Utc::now();
        let hour = now.duration_trunc(TimeDelta::hours(1)).unwrap();
        let offset = |t: DateTime<Utc>| u16::try_from((t - hour).num_seconds()).unwrap();
        // Two samples within the last minute of the current hour, or of the hour before.
        let (t1, t2) = (now - TimeDelta::seconds(40), now - TimeDelta::seconds(25));
        let hour = if t1 < hour {
            hour - TimeDelta::hours(1)
        } else {
            hour
        };
        let mut p = chunk(ChartScope::Pipeline("orders".into()), hour, "e2");
        p.series.insert(
            "lag_seconds".into(),
            pts(&[(offset(t1), 4.0), (offset(t2), 6.0)]),
        );
        p.tables.insert(
            "public.orders".into(),
            [("commit_seconds".to_owned(), pts(&[(offset(t2), 0.5)]))].into(),
        );
        let mut n = chunk(ChartScope::Node("node-b".into()), hour, "i3");
        n.series
            .insert("cpu_cores".into(), pts(&[(offset(t1), 0.25)]));
        written(&charts, vec![p, n]).await;
        let (addr, _lone, _dir) =
            served_with("http://127.0.0.1:1", open(), |node| node.charts = charts).await;
        let (code, v) = json_of(
            addr,
            "GET",
            "/v1/history?pipeline=orders&series=lag_seconds",
        )
        .await;
        assert_eq!(code, 200, "{v}");
        let s = &v["series"][0];
        assert_eq!(
            (&s["pipeline"], &s["name"], &s["unit"]),
            (&json!("orders"), &json!("lag_seconds"), &json!("seconds"))
        );
        let values: Vec<f64> = s["points"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p[1].as_f64().unwrap())
            .collect();
        assert_eq!(values, [4.0, 6.0]);
        assert_eq!(v["step"], 15);
        let (_, v) = json_of(
            addr,
            "GET",
            "/v1/history?pipeline=orders&table=public.orders&series=commit_seconds",
        )
        .await;
        assert_eq!(v["series"][0]["table"], "public.orders");
        assert_eq!(v["series"][0]["points"][0][1], 0.5);
        // Another node's history: its process wrote it, any node reads it.
        let (_, v) = json_of(addr, "GET", "/v1/history?node=node-b").await;
        let names: Vec<&str> = v["series"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["memory_bytes", "cpu_cores", "lease_renew_seconds"]);
        assert_eq!(v["series"][1]["points"][0][1], 0.25);
        assert_eq!(v["series"][0]["points"], json!([]));
    }

    /// Past the retention nothing is shown, though storage still holds the day: the history's
    /// start is `kept_since`, and the range before it is Prometheus's.
    #[tokio::test]
    async fn nothing_older_than_the_retention_is_shown() {
        let (addr, lone, _dir) = served("http://127.0.0.1:1", open()).await;
        let now = Utc::now();
        let old = now - TimeDelta::hours(30);
        let hour = old.duration_trunc(TimeDelta::hours(1)).unwrap();
        let mut p = chunk(ChartScope::Pipeline("orders".into()), hour, "e1");
        p.series.insert("lag_seconds".into(), pts(&[(0, 1.0)]));
        written(&lone.node.charts, vec![p]).await;
        let since = (now - TimeDelta::hours(31)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let (code, v) = json_of(
            addr,
            "GET",
            &format!("/v1/history?pipeline=orders&series=lag_seconds&since={since}"),
        )
        .await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(v["series"][0]["points"], json!([]));
        let kept = DateTime::parse_from_rfc3339(v["kept_since"].as_str().unwrap()).unwrap();
        let expected = now - TimeDelta::hours(24);
        assert!(
            (kept.with_timezone(&Utc) - expected).num_seconds().abs() < 5,
            "{v}"
        );
    }
}
