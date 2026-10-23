//! The management API on `globals.api.address` (System-Overview §Management API endpoints,
//! API_RBAC_and_Versioning): liveness, readiness and the metrics, unversioned and open, and the
//! `/v1` endpoints. A request may land on any node: what a pipeline's owner must do goes through
//! the pipeline's state documents, which the owner polls, and a status is read from them too
//! (D-M9a.2).

mod config;
mod server;
#[cfg(test)]
mod test_support;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use data_avalanche_cluster::{ConfigCoordinator, Members};
use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
use serde_json::{Value, json};
use tokio::sync::watch;

pub use server::{Bound, HANDSHAKE_TIMEOUT, ServeError, bind};

/// What the routes read.
#[derive(Debug, Clone)]
pub struct Api {
    pub metrics: Metrics,
    /// Startup is done and every pipeline's controller runs (not: every pipeline is healthy).
    pub ready: Arc<AtomicBool>,
    /// Set once the node runs its config generation; the `/v1` routes answer 503 before.
    pub node: Arc<OnceLock<Node>>,
}

/// What the `/v1` routes act on, once the node runs its config generation.
#[derive(Debug, Clone)]
pub struct Node {
    pub coordinator: Arc<ConfigCoordinator>,
    pub members: watch::Receiver<Members>,
    /// The node's file, read again by each reload (config §12.1).
    pub file: PathBuf,
}

pub fn router(api: Api) -> Router {
    Router::new()
        .route("/health/live", get(|| async { "live\n" }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/config/reload", post(config::reload))
        .route("/v1/config/status", get(config::status))
        .with_state(api)
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
    use crate::test_support::{get, json_of, serve};

    #[tokio::test]
    async fn the_listener_answers_health_and_metrics() {
        let metrics = Metrics::new();
        metrics.set_cluster_members(1);
        let ready = Arc::new(AtomicBool::new(false));
        let addr = serve(Api {
            metrics,
            ready: Arc::clone(&ready),
            node: Arc::default(),
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
