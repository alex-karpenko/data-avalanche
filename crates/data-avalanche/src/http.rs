//! The minimal listener on `globals.api.address`: liveness, readiness and the metrics, all
//! unauthenticated (System-Overview §Observability, API_RBAC §2). The `/v1` API, its auth and TLS
//! arrive with M9.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use data_avalanche_config::HostPort;
use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
use tokio::net::TcpListener;

use crate::run::Fatal;

/// What the routes read.
#[derive(Debug, Clone)]
pub(crate) struct Http {
    pub metrics: Metrics,
    /// Startup is done and every pipeline's controller runs (not: every pipeline is healthy).
    pub ready: Arc<AtomicBool>,
}

pub(crate) fn router(http: Http) -> Router {
    Router::new()
        .route("/health/live", get(|| async { "live\n" }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(http)
}

async fn ready(State(http): State<Http>) -> impl IntoResponse {
    if http.ready.load(Ordering::SeqCst) {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

async fn metrics(State(http): State<Http>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, CONTENT_TYPE)],
        http.metrics.encode(),
    )
}

/// # Errors
/// The address can't be bound.
pub(crate) async fn bind(address: &HostPort) -> Result<TcpListener, Fatal> {
    TcpListener::bind((address.host.as_str(), address.port))
        .await
        .map_err(|e| {
            Fatal::Startup(format!(
                "globals.api.address {}:{} can't be bound: {e}",
                address.host, address.port
            ))
        })
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// A plain HTTP/1.1 GET: the status code, the headers, the body.
    async fn get(addr: std::net::SocketAddr, path: &str) -> (u16, String, String) {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        let (head, body) = out.split_once("\r\n\r\n").unwrap();
        let code = head[9..12].parse().unwrap();
        (code, head.to_owned(), body.to_owned())
    }

    #[tokio::test]
    async fn the_listener_answers_health_and_metrics() {
        let metrics = Metrics::new();
        metrics.set_cluster_members(1);
        let ready = Arc::new(AtomicBool::new(false));
        let listener = bind(&HostPort {
            host: "127.0.0.1".into(),
            port: 0,
        })
        .await
        .unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(Http {
            metrics,
            ready: Arc::clone(&ready),
        });
        tokio::spawn(async move { axum::serve(listener, app).await });

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
        assert_eq!(get(addr, "/v1/pipelines").await.0, 404, "the API is M9's");
    }
}
