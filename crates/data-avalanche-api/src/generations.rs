//! `GET /v1/config/generations` and `GET /v1/config/generations/{generation}` (Admin_Console §4
//! Config, D-M13b.9): the cluster's config generations, newest first, each with who published it
//! and why; and one generation with its text, its secrets masked and its comments shown empty
//! (D-M13b.10).

use std::sync::Arc;

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::Response;
use data_avalanche_state::{Listed, StateError};
use serde_json::{Value, json};

use crate::{Api, Failure, body, starting};

/// The list's parameters.
pub const PARAMETERS: &[(&str, &str)] = &[
    (
        "before",
        "generations older than this one: the `next` of the page before",
    ),
    ("limit", "at most this many (1–100, 20 by default)"),
];

pub const DEFAULT_LIMIT: usize = 20;

/// How many snapshots the list reads at once for their fingerprints.
const FINGERPRINT_READS: usize = 16;
pub const MAX_LIMIT: usize = 100;

fn query(raw: Option<&str>) -> Result<(Option<u64>, usize), Failure> {
    let mut before = None;
    let mut limit = DEFAULT_LIMIT;
    for (name, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
        match &*name {
            "before" => {
                // Generations are numbered from 1, as the OpenAPI document's minimum says.
                before = Some(
                    value
                        .trim()
                        .parse::<u64>()
                        .ok()
                        .filter(|n| *n >= 1)
                        .ok_or_else(|| {
                            Failure::new(
                                StatusCode::BAD_REQUEST,
                                "before is a generation's number, 1 or more",
                            )
                        })?,
                );
            }
            "limit" => {
                limit = value
                    .trim()
                    .parse()
                    .ok()
                    .filter(|n| (1..=MAX_LIMIT).contains(n))
                    .ok_or_else(|| {
                        Failure::new(
                            StatusCode::BAD_REQUEST,
                            format!("limit is 1 to {MAX_LIMIT}"),
                        )
                    })?;
            }
            other => {
                return Err(Failure::new(
                    StatusCode::BAD_REQUEST,
                    format!("unknown parameter {other:?}: the generations take before, limit"),
                ));
            }
        }
    }
    Ok((before, limit))
}

/// A generation as listed; `hash` is its text's fingerprint under the cluster's key, never the
/// text's plain hash: a viewer reads the masked text, and could check a guessed secret against a
/// plain one (Admin_Console §4 Config).
fn listed_json(l: &Listed, hash: Option<String>) -> Value {
    let mut v = json!({
        "generation": l.generation,
        "hash": hash,
        "current": l.current,
    });
    if let Some(r) = &l.record {
        v["node"] = json!(r.node);
        v["at"] = json!(r.at);
        v["by"] = json!(r.by);
        v["note"] = json!(r.note);
        v["summary"] = json!(r.summary);
    }
    v
}

fn unreadable(e: &StateError) -> Response {
    body(
        StatusCode::SERVICE_UNAVAILABLE,
        &json!({ "error": format!("the config generations can't be read: {e}") }),
    )
}

/// The generations, newest first; `next` names the page after whenever this one is full (there
/// may be none after it).
pub(crate) async fn list(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (before, limit) = match query(raw.as_deref()) {
        Ok(q) => q,
        Err(f) => return f.into(),
    };
    match node.coordinator.generations().history(before, limit).await {
        Ok(page) => {
            let next = (page.len() == limit)
                .then(|| page.last().map(|l| l.generation))
                .flatten();
            // Each text's fingerprint: read FINGERPRINT_READS snapshots at a time the first
            // time, kept after.
            let mut generations = Vec::with_capacity(page.len());
            for chunk in page.chunks(FINGERPRINT_READS) {
                let mut reads = tokio::task::JoinSet::new();
                for (i, l) in chunk.iter().enumerate() {
                    let (coordinator, n, hash) =
                        (Arc::clone(&node.coordinator), l.generation, l.hash.clone());
                    reads.spawn(async move {
                        let fp = match hash {
                            Some(h) => match coordinator.generation_fingerprint(n, &h).await {
                                Ok(fp) => Some(fp),
                                Err(e) => {
                                    tracing::debug!("config generation {n}'s text: {e}");
                                    None
                                }
                            },
                            None => None,
                        };
                        (i, fp)
                    });
                }
                let mut read = reads.join_all().await;
                read.sort_unstable_by_key(|(i, _)| *i);
                for (l, (_, fp)) in chunk.iter().zip(read) {
                    generations.push(listed_json(l, fp));
                }
            }
            body(
                StatusCode::OK,
                &json!({ "generations": generations, "next": next }),
            )
        }
        Err(e) => unreadable(&e),
    }
}

/// A generation's text, or the answer that says why there is none.
async fn text_of(node: &crate::Node, n: u64) -> Result<String, Box<Response>> {
    match node.coordinator.generations().one(n).await {
        Ok(None) => Err(Box::new(
            Failure::new(StatusCode::NOT_FOUND, format!("no config generation {n}")).into(),
        )),
        Ok(Some((_, None))) => Err(Box::new(ambiguous(n))),
        Ok(Some((_, Some(text)))) => Ok(text),
        Err(e) => Err(Box::new(unreadable(&e))),
    }
}

fn ambiguous(n: u64) -> Response {
    Failure::new(
        StatusCode::CONFLICT,
        format!(
            "config generation {n} has several snapshots and no record of which one it is: a publish that lost its race left one behind"
        ),
    )
    .into()
}

/// `GET /v1/config/diff?from=&to=` (D-M13b.11): what differs between two generations as written.
pub(crate) async fn diff(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let (mut from, mut to) = (None, None);
    for (name, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
        let slot = match &*name {
            "from" => &mut from,
            "to" => &mut to,
            other => {
                return Failure::new(
                    StatusCode::BAD_REQUEST,
                    format!("unknown parameter {other:?}: the diff takes from, to"),
                )
                .into();
            }
        };
        let Ok(n) = value.trim().parse::<u64>() else {
            return Failure::new(
                StatusCode::BAD_REQUEST,
                format!("{name} is a generation's number"),
            )
            .into();
        };
        *slot = Some(n);
    }
    let (Some(from), Some(to)) = (from, to) else {
        return Failure::new(
            StatusCode::BAD_REQUEST,
            "the diff takes from and to, both generations",
        )
        .into();
    };
    let before = match text_of(node, from).await {
        Ok(t) => t,
        Err(r) => return *r,
    };
    let after = match text_of(node, to).await {
        Ok(t) => t,
        Err(r) => return *r,
    };
    match node.coordinator.diff_written(&before, &after) {
        Ok(d) => body(
            StatusCode::OK,
            &json!({
                "from": from,
                "to": to,
                "pipelines": { "added": d.added, "removed": d.removed },
                "changes": d.changes,
            }),
        ),
        Err(e) => Failure::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("the generations' texts can't be compared: {e}"),
        )
        .into(),
    }
}

/// One generation and its text, secrets masked and comments shown empty.
pub(crate) async fn one(State(api): State<Api>, Path(generation): Path<String>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let Ok(n) = generation.parse::<u64>() else {
        return Failure::new(StatusCode::BAD_REQUEST, "a generation is a number").into();
    };
    match node.coordinator.generations().one(n).await {
        Ok(None) => Failure::new(StatusCode::NOT_FOUND, format!("no config generation {n}")).into(),
        Ok(Some((_, None))) => ambiguous(n),
        Ok(Some((listed, Some(text)))) => {
            let fingerprint = node.coordinator.fingerprint(&text);
            // A text comes with its hash: `one` reads it by that hash.
            let hash = listed.hash.clone().unwrap_or_default();
            match node.coordinator.mask_generation(&hash, text).await {
                Ok(masked) => {
                    let mut v = listed_json(&listed, Some(fingerprint));
                    v["text"] = json!(masked);
                    body(StatusCode::OK, &v)
                }
                Err(e) => Failure::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("config generation {n}'s text isn't shown: {e}"),
                )
                .into(),
            }
        }
        Err(e) => unreadable(&e),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::AtomicBool;

    use data_avalanche_config::MASK;
    use data_avalanche_telemetry::Metrics;

    use super::*;
    use crate::test_support::{CONFIG, json_of, lone, open, request, serve};

    /// More generations than are read at once are each listed with their own text's keyed
    /// fingerprint, in order.
    #[tokio::test]
    async fn every_listed_generation_has_its_own_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let lone = lone(dir.path().join("config.yaml")).await;
        let g = lone.node.coordinator.generations();
        let mut texts = vec![CONFIG.to_owned()];
        let mut base = g.current().await.unwrap().unwrap().pointer;
        for i in 0..FINGERPRINT_READS + 4 {
            let text = format!("{CONFIG}# {i}\n");
            base = g
                .publish(&base, &text, "node-a", chrono::Utc::now())
                .await
                .unwrap()
                .pointer;
            texts.push(text);
        }
        let node = Arc::new(OnceLock::new());
        node.set(lone.node.clone()).unwrap();
        let addr = serve(Api {
            metrics: Metrics::new(),
            ready: Arc::new(AtomicBool::new(true)),
            node,
            auth: open(),
        })
        .await;
        let (code, v) = json_of(addr, "GET", "/v1/config/generations?limit=100").await;
        assert_eq!(code, 200, "{v}");
        let key = data_avalanche_config::FingerprintKey::from_bytes([1; 32]);
        let want: Vec<Value> = texts
            .iter()
            .rev()
            .map(|t| json!(data_avalanche_config::text_fingerprint(&key, t)))
            .collect();
        let got: Vec<Value> = v["generations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g["hash"].clone())
            .collect();
        assert_eq!(got, want);
    }

    /// Each generation is listed with who published it and why; one generation's text shows
    /// its secrets masked.
    #[tokio::test]
    async fn a_generation_is_listed_with_its_publisher_and_shown_masked() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file.clone()).await;
        let node = Arc::new(OnceLock::new());
        node.set(lone.node.clone()).unwrap();
        let addr = serve(Api {
            metrics: Metrics::new(),
            ready: Arc::new(AtomicBool::new(true)),
            node,
            auth: open(),
        })
        .await;
        std::fs::write(
            &file,
            CONFIG.replace("drain_timeout: 45s", "drain_timeout: 40s")
                + "# the old password: pw-before\n",
        )
        .unwrap();
        let (code, _, _) = request(
            addr,
            "POST",
            "/v1/config/reload",
            &[],
            r#"{"note": "shorter drain"}"#,
        )
        .await;
        assert_eq!(code, 202);

        let (code, v) = json_of(addr, "GET", "/v1/config/generations").await;
        assert_eq!(code, 200, "{v}");
        let gens = v["generations"].as_array().unwrap();
        assert_eq!(gens.len(), 2, "{v}");
        assert_eq!(
            (
                &gens[0]["generation"],
                &gens[0]["current"],
                &gens[0]["note"]
            ),
            (&json!(2), &json!(true), &json!("shorter drain"))
        );
        assert_eq!(gens[0]["by"]["name"], "anonymous", "{v}");
        assert_eq!(gens[0]["node"], "node-a");
        assert_eq!(gens[1]["summary"], "bootstrap");
        assert_eq!(v["next"], Value::Null);
        let (_, v) = json_of(addr, "GET", "/v1/config/generations?limit=1").await;
        assert_eq!(
            (v["generations"][0]["generation"].clone(), v["next"].clone()),
            (json!(2), json!(2))
        );
        let (_, v) = json_of(addr, "GET", "/v1/config/generations?before=2").await;
        assert_eq!(v["generations"][0]["generation"], 1);

        let (code, v) = json_of(addr, "GET", "/v1/config/generations/2").await;
        assert_eq!(code, 200, "{v}");
        let text = v["text"].as_str().unwrap();
        assert!(text.contains("drain_timeout: 40s"), "{text}");
        assert!(
            text.contains(&format!("password: \"{MASK} blake3:")),
            "{text}"
        );
        assert!(!text.contains("password: pw"), "{text}");
        assert!(text.ends_with("\n#\n"), "its comment shown empty: {text}");
        assert!(!text.contains("pw-before"), "{text}");

        let (code, v) = json_of(addr, "GET", "/v1/config/diff?from=1&to=2").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(
            v,
            json!({
                "from": 1, "to": 2,
                "pipelines": { "added": [], "removed": [] },
                "changes": [{
                    "path": "globals.cluster.drain_timeout", "class": "in_place",
                    "before": "45s", "after": "40s",
                }],
            })
        );

        for (path, want) in [
            ("/v1/config/diff?from=1&to=3", 404),
            ("/v1/config/diff?from=1", 400),
            ("/v1/config/diff?from=1&to=x", 400),
            ("/v1/config/generations/3", 404),
            ("/v1/config/generations/x", 400),
            ("/v1/config/generations?limit=0", 400),
            ("/v1/config/generations?before=0", 400),
            ("/v1/config/generations?after=1", 400),
        ] {
            assert_eq!(json_of(addr, "GET", path).await.0, want, "{path}");
        }
    }
}
