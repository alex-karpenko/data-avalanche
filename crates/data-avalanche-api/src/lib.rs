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
mod config;
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
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("file", &self.file)
            .field("leases", &self.leases)
            .finish_non_exhaustive()
    }
}

/// A `/v1` route and the least role that may call it (API_RBAC §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Route {
    pub method: &'static str,
    /// As the router matches it: `{id}` for a path parameter.
    pub path: &'static str,
    pub role: Role,
}

const fn route(method: &'static str, path: &'static str, role: Role) -> Route {
    Route { method, path, role }
}

/// Every `/v1` route, with its handler.
fn v1() -> Vec<(Route, MethodRouter<Api>)> {
    vec![
        (
            route("POST", "/v1/config/reload", Role::Operator),
            post(config::reload),
        ),
        (
            route("GET", "/v1/config/status", Role::Viewer),
            get(config::status),
        ),
        (
            route("GET", "/v1/pipelines", Role::Viewer),
            get(pipelines::list),
        ),
        (
            route("GET", "/v1/pipelines/{id}", Role::Viewer),
            get(pipelines::get),
        ),
        (
            route("POST", "/v1/pipelines/{id}/pause", Role::Operator),
            post(pipelines::pause),
        ),
        (
            route("POST", "/v1/pipelines/{id}/stop", Role::Operator),
            post(pipelines::stop),
        ),
        (
            route("POST", "/v1/pipelines/{id}/resume", Role::Operator),
            post(pipelines::resume),
        ),
        (
            route("POST", "/v1/pipelines/{id}/restart", Role::Operator),
            post(pipelines::restart),
        ),
        (
            route("POST", "/v1/pipelines/{id}/flush", Role::Operator),
            post(pipelines::flush),
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
        use Role::{Operator, Viewer};
        let table: Vec<_> = routes()
            .into_iter()
            .map(|r| (r.method, r.path, r.role))
            .collect();
        assert_eq!(
            table,
            [
                ("POST", "/v1/config/reload", Operator),
                ("GET", "/v1/config/status", Viewer),
                ("GET", "/v1/pipelines", Viewer),
                ("GET", "/v1/pipelines/{id}", Viewer),
                ("POST", "/v1/pipelines/{id}/pause", Operator),
                ("POST", "/v1/pipelines/{id}/stop", Operator),
                ("POST", "/v1/pipelines/{id}/resume", Operator),
                ("POST", "/v1/pipelines/{id}/restart", Operator),
                ("POST", "/v1/pipelines/{id}/flush", Operator),
            ]
        );
    }

    /// Every `/v1` route: 401 without a token or with one that doesn't verify, 403 below its
    /// role or with no role mapped, through at its role and above. Through means past the gate:
    /// the node doesn't run its generation here, so the route itself answers 503.
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
                    assert_eq!(code, 503, "{} {path} as {claim}: {body}", r.method);
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
