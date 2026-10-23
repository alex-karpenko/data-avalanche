//! The management API on `globals.api.address` (System-Overview §Management API endpoints,
//! API_RBAC_and_Versioning): liveness, readiness and the metrics, unversioned and open, and the
//! `/v1` endpoints. A request may land on any node: what a pipeline's owner must do goes through
//! the pipeline's state documents, which the owner polls, and a status is read from them too
//! (D-M9a.2).
//!
//! Every `/v1` route takes the least role in API_RBAC §2 ([`routes`]): no credentials, or ones
//! that don't verify, are 401 with a `WWW-Authenticate` challenge; a caller below the route's
//! role is 403.

mod auth;
mod cluster;
mod config;
mod openapi;
mod pipelines;
mod server;
#[cfg(test)]
mod test_support;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use data_avalanche_cluster::{ConfigCoordinator, DocsFn, Members};
use data_avalanche_config::Role;
use data_avalanche_state::Leases;
use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
use serde_json::{Value, json};
use tokio::sync::watch;

pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, Principal, REFETCH_AFTER};
pub use cluster::{Control, Stop};
pub use openapi::openapi;
pub use server::{Bound, HANDSHAKE_TIMEOUT, ServeError, bind};

use crate::auth::Denied;

/// What the routes read.
#[derive(Debug, Clone)]
pub struct Api {
    pub metrics: Metrics,
    /// Startup is done and every pipeline's controller runs (not: every pipeline is healthy).
    pub ready: Arc<AtomicBool>,
    /// Set once the node runs its config generation; the `/v1` routes answer 503 before.
    pub node: Arc<OnceLock<Node>>,
    pub auth: Arc<Auth>,
}

/// What the `/v1` routes act on, once the node runs its config generation.
#[derive(Clone)]
pub struct Node {
    pub coordinator: Arc<ConfigCoordinator>,
    pub members: watch::Receiver<Members>,
    /// The node's file, read again by each reload (config §12.1).
    pub file: PathBuf,
    /// A pipeline's state documents.
    pub docs: Arc<DocsFn>,
    /// The leases in `globals.state`: who holds each pipeline.
    pub leases: Leases,
    /// The node's leave and shutdown.
    pub control: Control,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("file", &self.file)
            .field("leases", &self.leases)
            .finish_non_exhaustive()
    }
}

/// A `/v1` route: the least role that may call it (API_RBAC §2), and what the OpenAPI document
/// says of it ([`openapi`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Route {
    pub method: &'static str,
    /// As the router matches it: `{id}`, a pipeline's id, for a path parameter.
    pub path: &'static str,
    pub role: Role,
    pub summary: &'static str,
    /// The request body's content type, when it takes one.
    pub body: Option<&'static str>,
    /// Its own answers; 401, 403 and 503 are every route's.
    pub answers: &'static [(u16, &'static str)],
}

const ASKED: (u16, &str) = (202, "asked: the owner acts at its next poll");
const UNKNOWN: (u16, &str) = (404, "no such pipeline in the generation this node runs");

/// Every `/v1` route, with its handler.
#[allow(clippy::too_many_lines)]
fn v1() -> Vec<(Route, MethodRouter<Api>)> {
    use Role::{Admin, Operator, Viewer};
    let r = |method, path, role, summary, answers| Route {
        method,
        path,
        role,
        summary,
        body: None,
        answers,
    };
    vec![
        (
            Route {
                body: Some("application/yaml"),
                ..r(
                    "POST",
                    "/v1/config/validate",
                    Operator,
                    "Check a config, the body or else the node's file: what a reload of it would change. Nothing is published",
                    &[
                        (200, "it loads: its warnings and the changes"),
                        (
                            422,
                            "invalid, or it changes a restart-only field: the report",
                        ),
                    ],
                )
            },
            post(config::validate),
        ),
        (
            r(
                "POST",
                "/v1/config/reload",
                Operator,
                "Publish the node's file as the next config generation",
                &[
                    (200, "the file is the generation the cluster runs"),
                    (202, "published: the generation"),
                    (409, "other nodes published first, every time"),
                    (
                        422,
                        "invalid, or it changes a restart-only field: the report",
                    ),
                ],
            ),
            post(config::reload),
        ),
        (
            r(
                "GET",
                "/v1/config/status",
                Viewer,
                "The generation the cluster runs, and each member's",
                &[(200, "the generations")],
            ),
            get(config::status),
        ),
        (
            r(
                "GET",
                "/v1/cluster/status",
                Viewer,
                "The members this node sees, and each pipeline's lease holder",
                &[(200, "the members and the holders")],
            ),
            get(cluster::status),
        ),
        (
            r(
                "GET",
                "/v1/pipelines",
                Viewer,
                "Every pipeline: its desired and actual states and its holder",
                &[(200, "the pipelines")],
            ),
            get(pipelines::list),
        ),
        (
            r(
                "GET",
                "/v1/pipelines/{id}",
                Viewer,
                "A pipeline: its states, its holder, its position and its halted tables",
                &[(200, "the pipeline"), UNKNOWN],
            ),
            get(pipelines::get),
        ),
        (
            r(
                "POST",
                "/v1/pipelines/{id}/pause",
                Operator,
                "Pause: flush to a transaction boundary, then read nothing; the connection and the lease stay",
                &[
                    (200, "paused already"),
                    (202, "the desired state is paused"),
                    UNKNOWN,
                    (409, "the pipeline is stopped"),
                ],
            ),
            post(pipelines::pause),
        ),
        (
            r(
                "POST",
                "/v1/pipelines/{id}/stop",
                Operator,
                "Stop: flush, and close the source connection; the lease stays",
                &[
                    (200, "stopped already"),
                    (202, "the desired state is stopped"),
                    UNKNOWN,
                ],
            ),
            post(pipelines::stop),
        ),
        (
            r(
                "POST",
                "/v1/pipelines/{id}/resume",
                Operator,
                "Resume a paused or stopped pipeline",
                &[
                    (200, "running already"),
                    (202, "the desired state is running"),
                    UNKNOWN,
                ],
            ),
            post(pipelines::resume),
        ),
        (
            r(
                "POST",
                "/v1/pipelines/{id}/restart",
                Operator,
                "Restart: discard what is unflushed and replay from the checkpoint, under the same lease; a pipeline in ERROR runs again",
                &[
                    ASKED,
                    UNKNOWN,
                    (409, "paused, stopped, or no node holds the lease"),
                ],
            ),
            post(pipelines::restart),
        ),
        (
            r(
                "POST",
                "/v1/pipelines/{id}/flush",
                Operator,
                "Flush now",
                &[
                    (200, "paused: flushed already"),
                    ASKED,
                    UNKNOWN,
                    (409, "stopped, or no node holds the lease"),
                ],
            ),
            post(pipelines::flush),
        ),
        (
            r(
                "POST",
                "/v1/cluster/leave",
                Admin,
                "Leave the cluster: hand every pipeline off; the process stays up, not ready",
                &[(202, "the leave is under way"), (409, "leaving already")],
            ),
            post(cluster::leave),
        ),
        (
            r(
                "POST",
                "/v1/admin/shutdown",
                Admin,
                "Shut down: leave the cluster, then end the process",
                &[
                    (202, "the shutdown is under way"),
                    (409, "shutting down already"),
                ],
            ),
            post(cluster::shutdown),
        ),
        (
            r(
                "GET",
                "/v1/openapi.json",
                Viewer,
                "This document",
                &[(200, "the OpenAPI document")],
            ),
            get(openapi::serve),
        ),
    ]
}

/// Every `/v1` route and its role.
pub fn routes() -> Vec<Route> {
    v1().into_iter().map(|(r, _)| r).collect()
}

pub fn router(api: Api) -> Router {
    let mut router = Router::new()
        .route("/health/live", get(|| async { "live\n" }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics));
    for (r, handler) in v1() {
        let gate = (Arc::clone(&api.auth), r.role);
        router = router.route(
            r.path,
            handler.route_layer(from_fn_with_state(gate, authorize)),
        );
    }
    router.with_state(api)
}

/// Lets a caller through to a route that takes `role` or less, with its [`Principal`] in the
/// request's extensions.
async fn authorize(
    State((auth, role)): State<(Arc<Auth>, Role)>,
    mut req: Request,
    next: Next,
) -> Response {
    match auth.verify(req.headers()).await {
        Ok(p) if p.role >= role => {
            req.extensions_mut().insert(p);
            next.run(req).await
        }
        Ok(p) => body(
            StatusCode::FORBIDDEN,
            &json!({ "error": format!("this needs the {role} role; {} is {}", p.name, p.role) }),
        ),
        Err(Denied::Forbidden(why)) => body(StatusCode::FORBIDDEN, &json!({ "error": why })),
        Err(Denied::Unauthenticated { challenge, why }) => {
            let mut r = body(StatusCode::UNAUTHORIZED, &json!({ "error": why }));
            r.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static(challenge),
            );
            r
        }
        Err(Denied::Unavailable(why)) => {
            body(StatusCode::SERVICE_UNAVAILABLE, &json!({ "error": why }))
        }
    }
}

/// A JSON answer.
pub(crate) fn body(code: StatusCode, v: &Value) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{v}\n"),
    )
        .into_response()
}

/// A request that can't be served: its code and `{"error": …}`.
#[derive(Debug)]
pub(crate) struct Failure {
    code: StatusCode,
    error: String,
}

impl Failure {
    pub(crate) fn new(code: StatusCode, error: impl Into<String>) -> Self {
        Self {
            code,
            error: error.into(),
        }
    }
}

impl From<Failure> for Response {
    fn from(f: Failure) -> Self {
        body(f.code, &json!({ "error": f.error }))
    }
}

pub(crate) fn starting() -> Response {
    body(
        StatusCode::SERVICE_UNAVAILABLE,
        &json!({ "error": "the node is starting" }),
    )
}

async fn ready(State(api): State<Api>) -> impl IntoResponse {
    if api.ready.load(Ordering::SeqCst) {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

async fn metrics(State(api): State<Api>) -> impl IntoResponse {
    ([(header::CONTENT_TYPE, CONTENT_TYPE)], api.metrics.encode())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Issuer, api, get, json_of, request, serve};

    /// API_RBAC §2's matrix, for the routes this build serves.
    #[test]
    fn the_routes_take_the_roles_of_api_rbac() {
        use Role::{Admin, Operator, Viewer};
        let table: Vec<_> = routes()
            .into_iter()
            .map(|r| (r.method, r.path, r.role))
            .collect();
        assert_eq!(
            table,
            [
                ("POST", "/v1/config/validate", Operator),
                ("POST", "/v1/config/reload", Operator),
                ("GET", "/v1/config/status", Viewer),
                ("GET", "/v1/cluster/status", Viewer),
                ("GET", "/v1/pipelines", Viewer),
                ("GET", "/v1/pipelines/{id}", Viewer),
                ("POST", "/v1/pipelines/{id}/pause", Operator),
                ("POST", "/v1/pipelines/{id}/stop", Operator),
                ("POST", "/v1/pipelines/{id}/resume", Operator),
                ("POST", "/v1/pipelines/{id}/restart", Operator),
                ("POST", "/v1/pipelines/{id}/flush", Operator),
                ("POST", "/v1/cluster/leave", Admin),
                ("POST", "/v1/admin/shutdown", Admin),
                ("GET", "/v1/openapi.json", Viewer),
            ]
        );
    }

    /// Every `/v1` route: 401 without a token or with one that doesn't verify, 403 below its
    /// role or with no role mapped, through at its role and above. Through means past the gate:
    /// the node doesn't run its generation here, so most routes answer 503 themselves.
    #[tokio::test]
    async fn every_route_turns_away_callers_below_its_role() {
        let idp = Issuer::start().await;
        let addr = serve(api(Arc::new(Auth::new(&idp.api("roles"))))).await;
        let callers = [
            ("viewer", Some(Role::Viewer)),
            ("operator", Some(Role::Operator)),
            ("admin", Some(Role::Admin)),
            ("someone-else", None),
        ];
        for r in routes() {
            let path = r.path.replace("{id}", "orders");
            let (code, head, _) = request(addr, r.method, &path, &[], "").await;
            assert_eq!(code, 401, "{} {path} without a token", r.method);
            assert!(
                head.to_ascii_lowercase()
                    .contains(r#"www-authenticate: bearer realm="data-avalanche""#),
                "{head}"
            );
            let bad = [("Authorization", "Bearer not.a.token")];
            let (code, _, _) = request(addr, r.method, &path, &bad, "").await;
            assert_eq!(code, 401, "{} {path} with a bad token", r.method);
            for (claim, role) in callers {
                let token = format!("Bearer {}", idp.token(serde_json::json!([claim])));
                let auth = [("Authorization", token.as_str())];
                let (code, _, body) = request(addr, r.method, &path, &auth, "").await;
                if role.is_some_and(|role| role >= r.role) {
                    assert!(
                        code != 401 && code != 403,
                        "{} {path} as {claim}: {code} {body}",
                        r.method
                    );
                    // The handler's own 503, not the gate's.
                    if code == 503 {
                        assert_eq!(
                            body, "{\"error\":\"the node is starting\"}\n",
                            "{} {path} as {claim}",
                            r.method
                        );
                    }
                } else {
                    assert_eq!(code, 403, "{} {path} as {claim}: {body}", r.method);
                }
            }
        }
        assert_eq!(get(addr, "/health/live").await.0, 200, "probes are open");
        assert_eq!(get(addr, "/metrics").await.0, 200);
    }

    #[tokio::test]
    async fn the_listener_answers_health_and_metrics() {
        let metrics = Metrics::new();
        metrics.set_cluster_members(1);
        let ready = Arc::new(AtomicBool::new(false));
        let addr = serve(Api {
            metrics,
            ready: Arc::clone(&ready),
            ..api(crate::test_support::open())
        })
        .await;

        assert_eq!(get(addr, "/health/live").await.0, 200);
        assert_eq!(
            get(addr, "/health/ready").await.0,
            503,
            "not before startup ends"
        );
        ready.store(true, Ordering::SeqCst);
        assert_eq!(get(addr, "/health/ready").await.0, 200);
        let (code, head, body) = get(addr, "/metrics").await;
        assert_eq!(code, 200);
        assert!(
            head.to_ascii_lowercase()
                .contains("content-type: application/openmetrics-text"),
            "{head}"
        );
        assert!(body.contains("data_avalanche_cluster_members 1"), "{body}");
        assert!(body.ends_with("# EOF\n"), "{body}");
        assert_eq!(
            json_of(addr, "POST", "/v1/config/reload").await.0,
            503,
            "not before the node runs its generation"
        );
        assert_eq!(json_of(addr, "GET", "/v1/config/status").await.0, 503);
    }
}
