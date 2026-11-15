//! Attribution (Admin_Console §5–§6): every route that changes something records what it did, to
//! what, for whom, with the caller's optional `note`, and how it ended, in the cluster's events.
//! The name and role are the server's own verification ([`Principal`]); nothing in the request
//! can claim them. A caller below the route's role, or with no role at all, is recorded too
//! ([`denied`]), within [`DENIALS_BURST`] at once and one a second after on a node, and with what
//! it aimed at cut to [`MAX_TARGET`] characters, so a script hammering a route can't fill the
//! timeline. The route's answer never depends on the recording: one too large to read passes
//! through as it is, recorded by its status alone.

use std::borrow::Cow;
use std::time::Instant;

use axum::body::{Body, Bytes, HttpBody, to_bytes};
use axum::extract::{RawPathParams, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use data_avalanche_state::{Actor, EventSeverity};
use http_body_util::LengthLimitError;
use serde_json::Value;

use crate::{Failure, Gate, Node, Principal};

/// The longest note, in characters.
pub const MAX_NOTE: usize = 500;

/// The largest request or answer body the recording reads.
const MAX_BODY: usize = 1 << 20;

/// Refused attempts a node records at once, then one a second; past that they are only logged.
pub const DENIALS_BURST: u32 = 30;

/// The most characters of a refused attempt's path, and of the name it aimed at, that are
/// recorded or logged; a longer one is cut and ends in `…`.
pub const MAX_TARGET: usize = 128;

/// The most characters of a refusal's reason recorded with an action; a longer one is cut and
/// ends in `…`.
pub const MAX_ERROR: usize = 1000;

/// What a mutating route records: its event kind under `action.`, and the verb a page shows
/// (`pause` → "pause orders").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recorded {
    pub kind: &'static str,
    pub verb: &'static str,
}

/// `s` with at most `max` characters: a longer one is cut on a character boundary and ends in
/// `…`.
fn clipped(s: &str, max: usize) -> Cow<'_, str> {
    match s.char_indices().nth(max) {
        None => Cow::Borrowed(s),
        Some((at, _)) => Cow::Owned(format!("{}…", &s[..at])),
    }
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
    let v: Value =
        serde_json::from_slice(body).map_err(|e| format!("the body must be JSON: {e}"))?;
    let Some(obj) = v.as_object() else {
        return Err("the body must be a JSON object".into());
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
        Some(_) => Err("the note must be a string".into()),
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

/// Whether `e`, or an error it came from, is a body read past its length limit.
fn over_limit(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut at = Some(e);
    while let Some(e) = at {
        if e.is::<LengthLimitError>() {
            return true;
        }
        at = e.source();
    }
    false
}

/// A request body of at most [`MAX_BODY`] bytes.
///
/// # Errors
/// 413 past the limit; 400 for a body that can't be read otherwise.
async fn read_body(body: Body) -> Result<Bytes, Failure> {
    to_bytes(body, MAX_BODY).await.map_err(|e| {
        if over_limit(&e) {
            Failure::new(StatusCode::PAYLOAD_TOO_LARGE, "the body is over 1 MiB")
        } else {
            Failure::new(StatusCode::BAD_REQUEST, "the body couldn't be read")
        }
    })
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
    let bytes = match read_body(req_body).await {
        Ok(bytes) => bytes,
        Err(f) => return f.into(),
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
    let action = Action {
        recorded,
        by,
        params,
        note,
    };
    let answer = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    let (head, answer_body) = answer.into_parts();
    // An answer that may be larger than the recording reads passes through unread.
    if answer_body
        .size_hint()
        .upper()
        .is_none_or(|n| n > MAX_BODY as u64)
    {
        action.note_outcome(node, head.status, &Value::Null);
        return Response::from_parts(head, answer_body);
    }
    let Ok(answer_bytes) = to_bytes(answer_body, MAX_BODY).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let v: Value = serde_json::from_slice(&answer_bytes).unwrap_or(Value::Null);
    action.note_outcome(node, head.status, &v);
    Response::from_parts(head, Body::from(answer_bytes))
}

/// An action under way through a mutating route: what it records, who asked and why.
struct Action {
    recorded: Recorded,
    by: Principal,
    params: Vec<(String, String)>,
    note: Option<String>,
}

impl Action {
    /// Records how the action ended: its answer's `status`, and the `answer` read (`null` when
    /// it wasn't).
    fn note_outcome(self, node: &Node, status: StatusCode, answer: &Value) {
        let Some((outcome, severity)) = outcome(status, answer) else {
            return;
        };
        let recorder = &node.recorder;
        let pipeline = self
            .params
            .iter()
            .find(|(k, _)| k == "id")
            .map(|(_, v)| v.clone());
        let named = match (&pipeline, self.recorded.kind.split('.').next()) {
            (Some(p), _) => p.clone(),
            (None, Some("node")) => recorder.node().to_owned(),
            _ => String::new(),
        };
        let mut event = recorder.event(
            &format!("action.{}", self.recorded.kind),
            severity,
            format!("{} {named}", self.recorded.verb)
                .trim_end()
                .to_owned(),
        );
        event.pipeline = pipeline;
        event.object = answer["job_id"]
            .as_str()
            .map(str::to_owned)
            .or_else(|| answer["generation"].as_u64().map(|g| g.to_string()))
            .or_else(|| {
                (self.recorded.kind.starts_with("node.")).then(|| recorder.node().to_owned())
            });
        event.actor = Some(Actor {
            name: self.by.name,
            role: self.by.role.to_string(),
        });
        event.note = self.note;
        event.outcome = Some(outcome.to_owned());
        if let Some(e) = answer["error"].as_str() {
            event
                .detail
                .insert("error".into(), clipped(e, MAX_ERROR).into_owned());
        }
        recorder.record(event);
    }
}

/// Records a caller turned away from a mutating route (a 403), named `name`, of `role` (`none`
/// for a token with no role this node maps), within the node's rate. The path and the name it
/// aims at are cut to [`MAX_TARGET`] characters, and the pipeline is recorded only when the path
/// names one in the generation this node runs.
pub(crate) async fn denied(gate: &Gate, name: &str, role: &str, path: &str) {
    let (Some(recorded), Some(node)) = (gate.route.records, gate.node.get()) else {
        return;
    };
    let shown = clipped(path, MAX_TARGET);
    if node.denials.take(Instant::now()).is_err() {
        tracing::info!(by = %name, path = %shown, "a refused attempt not recorded: past the node's rate");
        return;
    }
    let recorder = &node.recorder;
    let segment = path
        .strip_prefix("/v1/pipelines/")
        .map(|rest| rest.split('/').next().unwrap_or_default());
    let pipeline = match segment {
        Some(id) if known(node, id).await => Some(id.to_owned()),
        _ => None,
    };
    let target = match segment {
        Some(id) => clipped(id, MAX_TARGET).into_owned(),
        None if path.starts_with("/v1/cluster/") || path.starts_with("/v1/admin/") => {
            recorder.node().to_owned()
        }
        None if let Some(rest) = path.strip_prefix("/v1/nodes/") => {
            clipped(rest.split('/').next().unwrap_or_default(), MAX_TARGET).into_owned()
        }
        None => String::new(),
    };
    let mut event = recorder.event(
        "action.denied",
        EventSeverity::Warning,
        format!(
            "{} refused: needs {}",
            format!("{} {target}", recorded.verb).trim_end(),
            gate.route.role
        ),
    );
    event.pipeline = pipeline;
    event.object = Some(shown.into_owned());
    event.actor = Some(Actor {
        name: name.to_owned(),
        role: role.to_owned(),
    });
    event.outcome = Some("denied".into());
    event.detail.insert("action".into(), recorded.kind.into());
    recorder.record(event);
}

/// Whether `id` names a pipeline in the generation this node runs.
async fn known(node: &Node, id: &str) -> bool {
    node.coordinator
        .config()
        .await
        .pipelines
        .values()
        .any(|p| p.id == id)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use data_avalanche_state::{EventDoc, EventQuery, Events};
    use tempfile::TempDir;

    use super::*;
    use crate::test_support::{
        CONFIG, Issuer, Lone, USERNAME_CLAIM, api, lone, open, request, serve,
    };
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
        let (addr, lone, _dir) =
            served(Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
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

        // A pipeline this node runs is named; any other path is recorded cut short, no pipeline.
        let (code, _, _) = request(addr, "POST", "/v1/pipelines/orders/pause", &auth, "").await;
        assert_eq!(code, 403);
        let long = "p".repeat(5000);
        let (code, _, _) = request(
            addr,
            "POST",
            &format!("/v1/pipelines/{long}/pause"),
            &auth,
            "",
        )
        .await;
        assert_eq!(code, 403);
        let got = events(&lone.events, 3).await;
        let (made_up, orders) = (&got[0], &got[1]);
        assert_eq!(orders.pipeline.as_deref(), Some("orders"));
        assert_eq!(orders.text, "pause orders refused: needs operator");
        assert_eq!(made_up.pipeline, None, "not a pipeline this node runs");
        let object = made_up.object.as_deref().unwrap();
        assert_eq!(object.chars().count(), MAX_TARGET + 1, "{object}");
        assert!(object.starts_with("/v1/pipelines/ppp"), "{object}");
        assert!(object.ends_with('…'), "{object}");
        assert!(
            made_up.text.chars().count() < 200,
            "the text's target is cut too: {}",
            made_up.text
        );
        assert!(made_up.text.contains("…"), "{}", made_up.text);
    }

    /// A verified token whose roles this node maps to nothing is an outsider: refused, and
    /// recorded under its name with role `none`.
    #[tokio::test]
    async fn a_token_with_no_mapped_role_is_recorded_as_denied() {
        let idp = Issuer::start().await;
        let (addr, lone, _dir) =
            served(Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
        let token = format!("Bearer {}", idp.token(serde_json::json!(["cdc-reader"])));
        let auth = [("Authorization", token.as_str())];
        let (code, _, body) = request(addr, "POST", "/v1/pipelines/orders/pause", &auth, "").await;
        assert_eq!(code, 403);
        assert!(body.contains("name no role this node maps"), "{body}");
        let got = events(&lone.events, 1).await;
        assert_eq!(got.len(), 1, "{got:?}");
        let e = &got[0];
        assert_eq!(e.kind, "action.denied");
        assert_eq!(
            e.actor,
            Some(Actor {
                name: "alice".into(),
                role: "none".into()
            })
        );
        assert_eq!(e.pipeline.as_deref(), Some("orders"));
        let (code, _, _) = request(addr, "GET", "/v1/pipelines", &auth, "").await;
        assert_eq!(code, 403);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            events(&lone.events, 1).await.len(),
            1,
            "a route that changes nothing records nothing"
        );
    }

    /// An answer larger than the recording reads reaches the caller as the route made it, and
    /// the action is still recorded, by its status.
    #[tokio::test]
    async fn a_large_answer_passes_through_and_is_recorded_by_its_status() {
        use std::sync::OnceLock;

        use axum::middleware::from_fn_with_state;

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file).await;
        let node = Arc::new(OnceLock::new());
        node.set(lone.node.clone()).unwrap();
        let route = crate::routes()
            .into_iter()
            .find(|r| r.path == "/v1/pipelines/{id}/pause")
            .unwrap();
        let gate = Gate {
            auth: open(),
            route,
            node,
        };
        let big = MAX_BODY + 1;
        let app = axum::Router::new().route(
            route.path,
            axum::routing::post(move || async move { (StatusCode::ACCEPTED, "x".repeat(big)) })
                .route_layer(from_fn_with_state(gate.clone(), record))
                .route_layer(from_fn_with_state(gate, crate::authorize)),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        let (code, _, body) = request(
            addr,
            "POST",
            "/v1/pipelines/orders/pause",
            &[],
            r#"{"note": "a big one"}"#,
        )
        .await;
        assert_eq!(code, 202);
        assert_eq!(body.len(), big, "the answer, whole");
        let got = events(&lone.events, 1).await;
        let e = &got[0];
        assert_eq!(e.kind, "action.pipeline.pause");
        assert_eq!(e.outcome.as_deref(), Some("accepted"));
        assert_eq!(e.note.as_deref(), Some("a big one"));
        assert!(e.detail.is_empty(), "{:?}", e.detail);
    }

    #[tokio::test]
    async fn a_body_past_the_limit_is_413_and_an_unreadable_one_400() {
        let over = read_body(Body::from(vec![b' '; MAX_BODY + 1]))
            .await
            .unwrap_err();
        assert_eq!(over.code, StatusCode::PAYLOAD_TOO_LARGE);
        let broken = Body::from_stream(futures_util::stream::iter([Err::<Bytes, _>(
            std::io::Error::other("the client went away"),
        )]));
        let unread = read_body(broken).await.unwrap_err();
        assert_eq!(unread.code, StatusCode::BAD_REQUEST);
        assert_eq!(unread.error, "the body couldn't be read");
        assert_eq!(
            read_body(Body::from("{}")).await.unwrap(),
            Bytes::from("{}")
        );
    }

    #[test]
    fn a_long_text_is_clipped_on_a_character_boundary() {
        assert_eq!(clipped("orders", 6), "orders");
        assert_eq!(clipped("ééééé", 3), "ééé…");
        assert_eq!(clipped("", 0), "");
        assert_eq!(clipped("ab", 0), "…");
    }

    /// A script hammering a route its role doesn't allow is recorded for a burst, then once a
    /// second: every call is still refused, and the timeline isn't flooded.
    #[tokio::test]
    async fn a_flood_of_refused_calls_is_recorded_for_a_burst_only() {
        let idp = Issuer::start().await;
        let (addr, lone, _dir) =
            served(Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
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
        assert_eq!(
            note_of(br#"{"note": 5}"#),
            Err("the note must be a string".into())
        );
        assert_eq!(note_of(b"[]"), Err("the body must be a JSON object".into()));
        assert!(
            note_of(b"note")
                .unwrap_err()
                .starts_with("the body must be JSON")
        );
    }
}
