//! Attribution (Admin_Console §5–§6): every route that changes something records what it did, to
//! what, for whom, with the caller's optional `note`, and how it ended, in the cluster's events.
//! The name and role are the server's own verification ([`Principal`]); nothing in the request
//! can claim them. A caller below the route's role is recorded too ([`denied`]), within
//! [`DENIALS_BURST`] at once and one a second after on a node, so a script hammering a route
//! can't fill the timeline.

use std::time::Instant;

use axum::body::{Body, to_bytes};
use axum::extract::{RawPathParams, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use data_avalanche_cluster::Recorder;
use data_avalanche_state::{Actor, EventSeverity};
use serde_json::Value;

use crate::{Failure, Gate, Principal};

/// The longest note, in characters.
pub const MAX_NOTE: usize = 500;

/// The largest request or answer body the recording reads.
const MAX_BODY: usize = 1 << 20;

/// Refused attempts a node records at once, then one a second; past that they are only logged.
pub const DENIALS_BURST: u32 = 30;

/// What a mutating route records: its event kind under `action.`, and the verb a page shows
/// (`pause` → "pause orders").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recorded {
    pub kind: &'static str,
    pub verb: &'static str,
}

/// The `note` in a JSON body: absent, or a string of at most [`MAX_NOTE`] characters without
/// control characters but newlines; blank is none. An empty body has none.
///
/// # Errors
/// What is wrong with it, for a 422.
pub fn note_of(body: &[u8]) -> Result<Option<String>, String> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    let v: Value = serde_json::from_slice(body).map_err(|e| format!("the body isn't JSON: {e}"))?;
    let Some(obj) = v.as_object() else {
        return Err("the body is a JSON object".into());
    };
    match obj.get("note") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(n)) => {
            let n = n.trim();
            if n.chars().count() > MAX_NOTE {
                Err(format!("the note is longer than {MAX_NOTE} characters"))
            } else if n.chars().any(|c| c.is_control() && c != '\n') {
                Err("the note holds a control character".into())
            } else if n.is_empty() {
                Ok(None)
            } else {
                Ok(Some(n.to_owned()))
            }
        }
        Some(_) => Err("the note is a string".into()),
    }
}

/// How an answer ended, for the record; `None` for one that changed nothing worth recording
/// (an unknown pipeline, a malformed request, a node still starting).
fn outcome(code: StatusCode, answer: &Value) -> Option<(&'static str, EventSeverity)> {
    match code.as_u16() {
        200..=299 if answer["unchanged"] == true => Some(("unchanged", EventSeverity::Info)),
        200..=299 => Some(("accepted", EventSeverity::Info)),
        409 | 422 => Some(("refused", EventSeverity::Warning)),
        _ => None,
    }
}

fn target(
    recorder: &Recorder,
    kind: &str,
    params: &[(String, String)],
) -> (Option<String>, String) {
    let pipeline = params
        .iter()
        .find(|(k, _)| k == "id")
        .map(|(_, v)| v.clone());
    let named = match (&pipeline, kind.split('.').next()) {
        (Some(p), _) => p.clone(),
        (None, Some("node")) => recorder.node().to_owned(),
        _ => String::new(),
    };
    (pipeline, named)
}

/// The recording around a mutating route: reads the note (422 on a bad one), runs the route,
/// records how it ended.
pub(crate) async fn record(State(gate): State<Gate>, req: Request, next: Next) -> Response {
    let (Some(recorded), Some(node), Some(by)) = (
        gate.route.records,
        gate.node.get(),
        req.extensions().get::<Principal>().cloned(),
    ) else {
        return next.run(req).await;
    };
    let (mut parts, req_body) = req.into_parts();
    let Ok(bytes) = to_bytes(req_body, MAX_BODY).await else {
        return Failure::new(StatusCode::PAYLOAD_TOO_LARGE, "the body is too large").into();
    };
    let note = match note_of(&bytes) {
        Ok(note) => note,
        Err(why) => return Failure::new(StatusCode::UNPROCESSABLE_ENTITY, why).into(),
    };
    let params: Vec<(String, String)> = {
        use axum::extract::FromRequestParts;
        RawPathParams::from_request_parts(&mut parts, &())
            .await
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect()
            })
            .unwrap_or_default()
    };
    let answer = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    let (head, answer_body) = answer.into_parts();
    let Ok(answer_bytes) = to_bytes(answer_body, MAX_BODY).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let v: Value = serde_json::from_slice(&answer_bytes).unwrap_or(Value::Null);
    if let Some((outcome, severity)) = outcome(head.status, &v) {
        let recorder = &node.recorder;
        let (pipeline, named) = target(recorder, recorded.kind, &params);
        let mut event = recorder.event(
            &format!("action.{}", recorded.kind),
            severity,
            format!("{} {named}", recorded.verb).trim_end().to_owned(),
        );
        event.pipeline = pipeline;
        event.object = v["job_id"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| v["generation"].as_u64().map(|g| g.to_string()))
            .or_else(|| (recorded.kind.starts_with("node.")).then(|| recorder.node().to_owned()));
        event.actor = Some(Actor {
            name: by.name.clone(),
            role: by.role.to_string(),
        });
        event.note = note;
        event.outcome = Some(outcome.to_owned());
        if let Some(e) = v["error"].as_str() {
            event.detail.insert("error".into(), e.to_owned());
        }
        recorder.record(event);
    }
    Response::from_parts(head, Body::from(answer_bytes))
}

/// Records a caller turned away from a mutating route for its role (a 403), within the node's
/// rate.
pub(crate) fn denied(gate: &Gate, by: &Principal, path: &str) {
    let (Some(recorded), Some(node)) = (gate.route.records, gate.node.get()) else {
        return;
    };
    if node.denials.take(Instant::now()).is_err() {
        tracing::info!(by = %by.name, path, "a refused attempt not recorded: past the node's rate");
        return;
    }
    let recorder = &node.recorder;
    let mut event = recorder.event(
        "action.denied",
        EventSeverity::Warning,
        format!(
            "{} refused: needs {}",
            format!("{} {}", recorded.verb, path_target(path, recorder)).trim_end(),
            gate.route.role
        ),
    );
    event.pipeline = path
        .strip_prefix("/v1/pipelines/")
        .and_then(|rest| rest.split('/').next())
        .map(str::to_owned);
    event.object = Some(path.to_owned());
    event.actor = Some(Actor {
        name: by.name.clone(),
        role: by.role.to_string(),
    });
    event.outcome = Some("denied".into());
    event.detail.insert("action".into(), recorded.kind.into());
    recorder.record(event);
}

fn path_target(path: &str, recorder: &Recorder) -> String {
    if let Some(rest) = path.strip_prefix("/v1/pipelines/") {
        rest.split('/').next().unwrap_or_default().to_owned()
    } else if path.starts_with("/v1/cluster/") || path.starts_with("/v1/admin/") {
        recorder.node().to_owned()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use data_avalanche_state::{EventDoc, EventQuery, Events};
    use tempfile::TempDir;

    use super::*;
    use crate::test_support::{CONFIG, Issuer, Lone, api, lone, open, request, serve};
    use crate::{Api, Auth};

    /// The lone node's API under `auth`, its pipeline `orders` known.
    async fn served(auth: Arc<Auth>) -> (SocketAddr, Lone, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file).await;
        let a: Api = api(auth);
        a.node.set(lone.node.clone()).unwrap();
        (serve(a).await, lone, dir)
    }

    /// The events once there are `n`, newest first.
    async fn events(events: &Events, n: usize) -> Vec<EventDoc> {
        for _ in 0..200 {
            let page = events.list(&EventQuery::default()).await.unwrap();
            if page.events.len() >= n {
                return page.events.into_iter().map(|(_, e)| e).collect();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("fewer than {n} events");
    }

    #[tokio::test]
    async fn an_action_is_recorded_with_its_actor_note_and_outcome() {
        let (addr, lone, _dir) = served(open()).await;
        let json = [("Content-Type", "application/json")];
        let (code, _, body) = request(
            addr,
            "POST",
            "/v1/pipelines/orders/pause",
            &json,
            r#"{"note": "rotate the replica's certificate"}"#,
        )
        .await;
        assert_eq!(code, 202, "{body}");
        let (code, _, _) = request(addr, "POST", "/v1/pipelines/orders/pause", &[], "").await;
        assert_eq!(code, 200, "paused already: no body, no note");
        let (code, _, body) = request(addr, "POST", "/v1/pipelines/orders/restart", &[], "").await;
        assert_eq!(code, 409, "{body}");
        let got = events(&lone.events, 3).await;
        let summary: Vec<_> = got
            .iter()
            .map(|e| (e.kind.as_str(), e.outcome.as_deref(), e.severity))
            .collect();
        assert_eq!(
            summary,
            [
                (
                    "action.pipeline.restart",
                    Some("refused"),
                    EventSeverity::Warning
                ),
                (
                    "action.pipeline.pause",
                    Some("unchanged"),
                    EventSeverity::Info
                ),
                (
                    "action.pipeline.pause",
                    Some("accepted"),
                    EventSeverity::Info
                ),
            ]
        );
        let paused = &got[2];
        assert_eq!(paused.text, "pause orders");
        assert_eq!(paused.pipeline.as_deref(), Some("orders"));
        assert_eq!(paused.node, "node-a");
        assert_eq!(
            paused.actor,
            Some(Actor {
                name: "anonymous".into(),
                role: "admin".into()
            }),
            "auth none: anonymous, an admin"
        );
        assert_eq!(
            paused.note.as_deref(),
            Some("rotate the replica's certificate")
        );
        assert!(
            got[0].detail["error"].contains("resume it first"),
            "the refusal's reason: {:?}",
            got[0].detail
        );
    }

    #[tokio::test]
    async fn a_bad_note_is_refused_before_the_route_runs() {
        let (addr, lone, _dir) = served(open()).await;
        let long = format!(r#"{{"note": "{}"}}"#, "x".repeat(MAX_NOTE + 1));
        let (code, _, body) = request(addr, "POST", "/v1/pipelines/orders/stop", &[], &long).await;
        assert_eq!(code, 422, "{body}");
        assert!(body.contains("longer than 500"), "{body}");
        let config = lone.node.coordinator.config().await;
        let docs = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
        assert_eq!(
            docs.state().await.unwrap(),
            None,
            "the pipeline wasn't stopped"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let page = lone.events.list(&EventQuery::default()).await.unwrap();
        assert!(page.events.is_empty(), "nothing happened: {page:?}");
    }

    #[tokio::test]
    async fn a_snapshot_job_keeps_its_note_and_the_event_names_it() {
        let (addr, lone, _dir) = served(open()).await;
        let config = lone.node.coordinator.config().await;
        let docs = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
        docs.open("orders", "delta").await.unwrap();
        lone.leases
            .acquire(
                "orders",
                &data_avalanche_state::Holder {
                    node_id: "node-a".into(),
                    incarnation: 1,
                },
                Duration::from_secs(30),
                chrono::Utc::now(),
            )
            .await
            .unwrap();
        let (code, _, body) = request(
            addr,
            "POST",
            "/v1/pipelines/orders/snapshots",
            &[],
            r#"{"tables": ["public.orders"], "note": " backfill after the outage "}"#,
        )
        .await;
        assert_eq!(code, 202, "{body}");
        let job_id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["job_id"]
            .as_str()
            .unwrap()
            .to_owned();
        let job = docs.snapshot_job(&job_id).await.unwrap().unwrap().doc;
        assert_eq!(job.note.as_deref(), Some("backfill after the outage"));
        let got = events(&lone.events, 1).await;
        assert_eq!(got[0].kind, "action.snapshot.start");
        assert_eq!(got[0].object.as_deref(), Some(job_id.as_str()));
        assert_eq!(got[0].text, "start a snapshot job on orders");
    }

    #[tokio::test]
    async fn a_caller_below_the_role_is_recorded_as_denied() {
        let idp = Issuer::start().await;
        let (addr, lone, _dir) = served(Arc::new(Auth::new(&idp.api("roles")))).await;
        let token = format!("Bearer {}", idp.token(serde_json::json!(["viewer"])));
        let auth = [("Authorization", token.as_str())];
        let (code, _, _) = request(addr, "POST", "/v1/admin/shutdown", &auth, "").await;
        assert_eq!(code, 403);
        let (code, _, _) = request(addr, "GET", "/v1/pipelines", &auth, "").await;
        assert_eq!(code, 200, "reading isn't recorded");
        let got = events(&lone.events, 1).await;
        assert_eq!(got.len(), 1, "{got:?}");
        let e = &got[0];
        assert_eq!(e.kind, "action.denied");
        assert_eq!(e.text, "shut down node-a refused: needs admin");
        assert_eq!(e.outcome.as_deref(), Some("denied"));
        assert_eq!(e.object.as_deref(), Some("/v1/admin/shutdown"));
        assert_eq!(e.actor.as_ref().map(|a| a.role.as_str()), Some("viewer"));
        assert_eq!(
            *lone.stops.borrow(),
            crate::Stop::Run,
            "nothing was shut down"
        );
    }

    /// A script hammering a route its role doesn't allow is recorded for a burst, then once a
    /// second: every call is still refused, and the timeline isn't flooded.
    #[tokio::test]
    async fn a_flood_of_refused_calls_is_recorded_for_a_burst_only() {
        let idp = Issuer::start().await;
        let (addr, lone, _dir) = served(Arc::new(Auth::new(&idp.api("roles")))).await;
        let token = format!("Bearer {}", idp.token(serde_json::json!(["viewer"])));
        let auth = [("Authorization", token.as_str())];
        let started = std::time::Instant::now();
        for _ in 0..DENIALS_BURST + 10 {
            let (code, _, _) = request(addr, "POST", "/v1/admin/shutdown", &auth, "").await;
            assert_eq!(code, 403);
        }
        let refilled = started.elapsed().as_secs() + 1;
        let burst = usize::try_from(DENIALS_BURST).unwrap();
        events(&lone.events, burst).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let got = events(&lone.events, burst).await.len();
        assert!(
            got <= burst + usize::try_from(refilled).unwrap(),
            "{got} recorded of {} refused",
            burst + 10
        );
    }

    #[test]
    fn a_note_is_optional_trimmed_and_bounded() {
        assert_eq!(note_of(b""), Ok(None));
        assert_eq!(note_of(b"  \n"), Ok(None));
        assert_eq!(note_of(b"{}"), Ok(None));
        assert_eq!(note_of(br#"{"note": null}"#), Ok(None));
        assert_eq!(note_of(br#"{"note": "   "}"#), Ok(None));
        assert_eq!(
            note_of(br#"{"note": " apply sink.prefix\nthen check "}"#),
            Ok(Some("apply sink.prefix\nthen check".into()))
        );
        assert_eq!(
            note_of(br#"{"tables": ["public.orders"], "note": "backfill"}"#),
            Ok(Some("backfill".into())),
            "beside a route's own fields"
        );
        let long = format!(r#"{{"note": "{}"}}"#, "é".repeat(MAX_NOTE + 1));
        assert!(
            note_of(long.as_bytes())
                .unwrap_err()
                .contains("longer than 500")
        );
        let exact = format!(r#"{{"note": "{}"}}"#, "é".repeat(MAX_NOTE));
        assert!(
            note_of(exact.as_bytes()).is_ok(),
            "500 characters, not bytes"
        );
        assert!(note_of(br#"{"note": "a\u0007b"}"#).is_err());
        assert!(note_of(br#"{"note": 5}"#).is_err());
        assert!(note_of(b"[]").is_err());
        assert!(note_of(b"note").is_err());
    }
}
