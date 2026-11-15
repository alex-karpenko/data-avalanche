//! `GET /v1/events` (Admin_Console §6): the cluster's timeline, newest first, under filters; a
//! page that stopped early names where to go on (`next`, passed back as `after`).

use axum::extract::{RawQuery, State};
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Utc};
use data_avalanche_state::{EventDoc, EventQuery, MAX_EVENTS};
use serde_json::{Value, json};

use crate::pipelines::unavailable;
use crate::{Api, Failure, body, starting};

/// The query's parameters, as a page and `curl` write them.
pub const PARAMETERS: &[(&str, &str)] = &[
    ("since", "events at or after this time (RFC 3339)"),
    ("until", "events before this time (RFC 3339)"),
    (
        "kind",
        "a kind or a family (`action`, `lease.takeover`); repeat it for several",
    ),
    (
        "object",
        "a pipeline's id, or an object's name (a table, a job, a node)",
    ),
    ("actor", "the name of who caused it"),
    ("node", "the node that recorded it"),
    ("q", "text in the event or its note, without case"),
    ("limit", "at most this many (1–1000, 100 by default)"),
    ("after", "the `next` of the page before: go on from there"),
];

/// The default page.
const DEFAULT_LIMIT: usize = 100;

fn time(name: &str, value: &str) -> Result<DateTime<Utc>, Failure> {
    DateTime::parse_from_rfc3339(value)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| {
            Failure::new(
                StatusCode::BAD_REQUEST,
                format!("{name} is an RFC 3339 time (2026-11-14T13:55:00Z): {e}"),
            )
        })
}

/// The query of `params`, each once but `kind`.
fn query(params: &[(String, String)]) -> Result<EventQuery, Failure> {
    let mut q = EventQuery {
        limit: DEFAULT_LIMIT,
        ..EventQuery::default()
    };
    let mut seen = Vec::new();
    for (name, value) in params {
        if name != "kind" {
            if seen.contains(name) {
                return Err(Failure::new(
                    StatusCode::BAD_REQUEST,
                    format!("{name} is given twice"),
                ));
            }
            seen.push(name.clone());
        }
        let value = value.trim();
        match name.as_str() {
            "since" => q.since = Some(time(name, value)?),
            "until" => q.until = Some(time(name, value)?),
            "kind" => q.kinds.extend(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|k| !k.is_empty())
                    .map(str::to_owned),
            ),
            "object" => q.object = Some(value.to_owned()),
            "actor" => q.actor = Some(value.to_owned()),
            "node" => q.node = Some(value.to_owned()),
            "q" => q.text = Some(value.to_owned()),
            "limit" => {
                q.limit = value
                    .parse()
                    .ok()
                    .filter(|n| (1..=MAX_EVENTS).contains(n))
                    .ok_or_else(|| {
                        Failure::new(
                            StatusCode::BAD_REQUEST,
                            format!("limit is 1 to {MAX_EVENTS}"),
                        )
                    })?;
            }
            "after" => q.after = Some(value.to_owned()),
            other => {
                return Err(Failure::new(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "unknown parameter {other:?}: the events take {}",
                        PARAMETERS
                            .iter()
                            .map(|(n, _)| *n)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            }
        }
    }
    if let (Some(s), Some(u)) = (q.since, q.until)
        && s >= u
    {
        return Err(Failure::new(
            StatusCode::BAD_REQUEST,
            "since is before until",
        ));
    }
    Ok(q)
}

pub(crate) fn event_json(key: &str, e: &EventDoc) -> Value {
    let mut v = serde_json::to_value(e).expect("an event serializes");
    v["key"] = json!(key);
    v
}

pub(crate) async fn list(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let params: Vec<(String, String)> =
        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    let q = match query(&params) {
        Ok(q) => q,
        Err(f) => return f.into(),
    };
    match node.events.list(&q).await {
        Ok(page) => body(
            StatusCode::OK,
            &json!({
                "events": page.events.iter().map(|(k, e)| event_json(k, e)).collect::<Vec<_>>(),
                "next": page.next,
            }),
        ),
        Err(e) => unavailable(&e),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use data_avalanche_state::{Actor, EventSeverity};

    use super::*;
    use crate::test_support::{CONFIG, api, json_of, lone, open, serve};

    fn at(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 14, h, m, 0).unwrap()
    }

    #[test]
    fn the_parameters_are_checked() {
        let p = |pairs: &[(&str, &str)]| {
            query(
                &pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect::<Vec<_>>(),
            )
        };
        let q = p(&[("kind", "action, lease.takeover"), ("kind", "table")]).unwrap();
        assert_eq!(q.kinds, ["action", "lease.takeover", "table"]);
        assert_eq!(q.limit, 100);
        assert_eq!(
            p(&[("since", "2026-11-14T13:00:00+02:00")]).unwrap().since,
            Some(at(11, 0))
        );
        for bad in [
            &[("since", "yesterday")][..],
            &[("limit", "0")],
            &[("limit", "1001")],
            &[("actor", "a"), ("actor", "b")],
            &[("color", "red")],
            &[
                ("since", "2026-11-14T14:00:00Z"),
                ("until", "2026-11-14T13:00:00Z"),
            ],
        ] {
            let e = p(bad).unwrap_err();
            assert_eq!(e.code, StatusCode::BAD_REQUEST, "{bad:?}");
        }
    }

    #[tokio::test]
    async fn the_events_are_listed_filtered_and_paged() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file).await;
        let mut pause = EventDoc::new(
            at(10, 0),
            "node-a",
            "action.pipeline.pause",
            EventSeverity::Info,
            "pause orders",
        );
        pause.pipeline = Some("orders".into());
        pause.actor = Some(Actor {
            name: "Oleksii".into(),
            role: "operator".into(),
        });
        let halt = EventDoc::new(
            at(13, 41),
            "node-b",
            "table.halted",
            EventSeverity::Error,
            "public.refunds halted",
        );
        for e in [&pause, &halt] {
            lone.events.record(e).await.unwrap();
        }
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        let addr = serve(a).await;

        let (code, v) = json_of(addr, "GET", "/v1/events").await;
        assert_eq!(code, 200, "{v}");
        let kinds: Vec<_> = v["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["kind"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(kinds, ["table.halted", "action.pipeline.pause"]);
        assert_eq!(v["next"], Value::Null);
        let first = &v["events"][1];
        assert_eq!(first["actor"]["name"], "Oleksii");
        assert_eq!(first["severity"], "info");
        assert!(
            first["key"]
                .as_str()
                .unwrap()
                .starts_with("events/2026-11-14/")
        );

        let (_, v) = json_of(addr, "GET", "/v1/events?actor=Oleksii&kind=action").await;
        assert_eq!(v["events"].as_array().unwrap().len(), 1);
        let (_, v) = json_of(addr, "GET", "/v1/events?limit=1").await;
        let next = v["next"].as_str().expect("a next page").to_owned();
        let (_, v) = json_of(addr, "GET", &format!("/v1/events?after={next}")).await;
        assert_eq!(v["events"][0]["kind"], "action.pipeline.pause");
        let (code, v) = json_of(addr, "GET", "/v1/events?until=soon").await;
        assert_eq!(code, 400, "{v}");
    }
}
