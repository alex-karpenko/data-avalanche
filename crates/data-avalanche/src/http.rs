//! The minimal listener on `globals.api.address`: liveness, readiness, the metrics, and the
//! config reload and its status (Clustering §9.2), all unauthenticated (System-Overview
//! §Observability, API_RBAC §2). The rest of the `/v1` API, its auth and TLS arrive with M9.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use data_avalanche_cluster::{ConfigCoordinator, Members, ReloadError, Reloaded};
use data_avalanche_config::HostPort;
use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::run::Fatal;

/// What the routes read.
#[derive(Debug, Clone)]
pub(crate) struct Http {
    pub metrics: Metrics,
    /// Startup is done and every pipeline's controller runs (not: every pipeline is healthy).
    pub ready: Arc<AtomicBool>,
    /// Set once the node runs its config generation; the config routes answer 503 before.
    pub config: Arc<OnceLock<ConfigApi>>,
}

/// What the config routes act on.
#[derive(Debug, Clone)]
pub(crate) struct ConfigApi {
    pub coordinator: Arc<ConfigCoordinator>,
    pub members: watch::Receiver<Members>,
    /// The node's file, read again by each reload (config §12.1).
    pub file: PathBuf,
}

pub(crate) fn router(http: Http) -> Router {
    Router::new()
        .route("/health/live", get(|| async { "live\n" }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/config/reload", post(reload))
        .route("/v1/config/status", get(status))
        .with_state(http)
}

fn body(code: StatusCode, v: &Value) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{v}\n"),
    )
        .into_response()
}

fn starting() -> Response {
    body(
        StatusCode::SERVICE_UNAVAILABLE,
        &json!({ "error": "the node is starting" }),
    )
}

/// Publishes the node's file as the next config generation: 202 with it, 200 when the file is
/// the generation the cluster runs already, 422 with the validation report when it is invalid or
/// changes a field only a process restart applies.
async fn reload(State(http): State<Http>) -> Response {
    let Some(api) = http.config.get() else {
        return starting();
    };
    let text = match tokio::fs::read_to_string(&api.file).await {
        Ok(text) => text,
        Err(e) => {
            return body(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({ "error": format!("cannot read config file {}: {e}", api.file.display()) }),
            );
        }
    };
    match api.coordinator.reload(&text).await {
        Ok(Reloaded {
            generation,
            published: true,
        }) => body(StatusCode::ACCEPTED, &json!({ "generation": generation })),
        Ok(Reloaded { generation, .. }) => body(
            StatusCode::OK,
            &json!({ "generation": generation, "unchanged": true }),
        ),
        Err(e) => {
            let code = match &e {
                ReloadError::Invalid(_) | ReloadError::Rejected(_) => {
                    StatusCode::UNPROCESSABLE_ENTITY
                }
                ReloadError::Contended => StatusCode::CONFLICT,
                ReloadError::State(_) | ReloadError::Stopped => StatusCode::SERVICE_UNAVAILABLE,
            };
            let mut v = json!({ "error": e.to_string() });
            if let ReloadError::Invalid(r) | ReloadError::Rejected(r) = &e {
                v["report"] = r.to_json();
            }
            body(code, &v)
        }
    }
}

/// The generation the cluster runs, and each member's applied one: the reload is complete when
/// every member's is it.
async fn status(State(http): State<Http>) -> Response {
    let Some(api) = http.config.get() else {
        return starting();
    };
    let applied = api.coordinator.applied();
    let generation = match api.coordinator.latest().await {
        Ok(latest) => latest.unwrap_or(applied),
        Err(e) => {
            return body(
                StatusCode::SERVICE_UNAVAILABLE,
                &json!({ "error": format!("the config generation: {e}") }),
            );
        }
    };
    let nodes: BTreeMap<String, u64> = api
        .members
        .borrow()
        .nodes
        .iter()
        .map(|(name, m)| (name.clone(), m.meta.config_generation))
        .collect();
    let converged = nodes.values().all(|n| *n == generation);
    body(
        StatusCode::OK,
        &json!({
            "generation": generation,
            "applied": applied,
            "nodes": nodes,
            "converged": converged,
        }),
    )
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
    use std::net::SocketAddr;
    use std::time::Duration;

    use data_avalanche_cluster::{
        ClusterReconciler, ControllerDeps, CoordinatorDeps, Gossip, GossipKey, GossipSettings,
        NodeIdentity, NodeMeta, Phase, ReconcilerSettings, Udp,
    };
    use data_avalanche_config::{FingerprintKey, ProcessEnv, load_str};
    use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases};
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Semaphore;

    use super::*;
    use crate::Factory;
    use crate::run::tests::CONFIG;

    /// A plain HTTP/1.1 request without a body: the status code, the headers, the body.
    async fn send(addr: SocketAddr, method: &str, path: &str) -> (u16, String, String) {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        let (head, body) = out.split_once("\r\n\r\n").unwrap();
        let code = head[9..12].parse().unwrap();
        (code, head.to_owned(), body.to_owned())
    }

    async fn get(addr: SocketAddr, path: &str) -> (u16, String, String) {
        send(addr, "GET", path).await
    }

    /// The code and the JSON body.
    async fn json_of(addr: SocketAddr, method: &str, path: &str) -> (u16, Value) {
        let (code, _, body) = send(addr, method, path).await;
        (
            code,
            serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}")),
        )
    }

    /// The listener on a free port.
    async fn serve(http: Http) -> SocketAddr {
        let listener = bind(&HostPort {
            host: "127.0.0.1".into(),
            port: 0,
        })
        .await
        .unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router(http)).await });
        addr
    }

    #[tokio::test]
    async fn the_listener_answers_health_and_metrics() {
        let metrics = Metrics::new();
        metrics.set_cluster_members(1);
        let ready = Arc::new(AtomicBool::new(false));
        let addr = serve(Http {
            metrics,
            ready: Arc::clone(&ready),
            config: Arc::default(),
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
        assert_eq!(get(addr, "/v1/pipelines").await.0, 404, "the API is M9's");
        assert_eq!(
            json_of(addr, "POST", "/v1/config/reload").await.0,
            503,
            "not before the node runs its generation"
        );
        assert_eq!(json_of(addr, "GET", "/v1/config/status").await.0, 503);
    }

    /// A lone node `node-a` running generation 1 of [`CONFIG`], read from `file`: its gossip on
    /// a loopback port, its reloads reaching a reconciler without pipelines.
    async fn lone(file: PathBuf) -> (ConfigApi, ClusterReconciler, Gossip) {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        let docs = DocStore::new(Arc::new(store));
        let generations = ConfigGenerations::new(docs.clone());
        let (first, _) = generations
            .bootstrap_or_adopt(CONFIG, "node-a", chrono::Utc::now())
            .await
            .unwrap();
        let key = FingerprintKey::from_bytes([1; 32]);
        let config = load_str(CONFIG, &ProcessEnv, &key).unwrap().config;
        let (udp, inbox) = Udp::bind(&["127.0.0.1:0".parse().unwrap()]).await.unwrap();
        let me = NodeIdentity {
            node_id: "node-a".into(),
            incarnation: 1,
            rev: 0,
            addr: udp.local_addrs()[0],
            meta: NodeMeta {
                phase: Phase::Active,
                config_generation: 1,
                build: "1.0.0".into(),
            },
        };
        let gossip = Gossip::start(
            me,
            GossipKey::derive(&[7; 32]),
            GossipSettings::new(&config.globals.cluster.memberlist),
            Arc::new(udp),
            inbox,
        );
        let metrics = Metrics::new();
        let deps = ControllerDeps {
            leases: Leases::new(docs),
            holder: Holder {
                node_id: "node-a".into(),
                incarnation: 1,
            },
            factory: Arc::new(Factory::new(&config.globals).unwrap()),
            metrics: metrics.clone(),
            settings: crate::run::settings(&config),
            starts: Arc::new(Semaphore::new(1)),
            handoffs: Arc::new(Semaphore::new(1)),
        };
        let reconciler = ClusterReconciler::start(
            vec![],
            &deps,
            gossip.members(),
            ReconcilerSettings::new(&config.globals.cluster),
        );
        let coordinator = ConfigCoordinator::start(
            config,
            first.pointer,
            CoordinatorDeps {
                generations,
                env: Arc::new(ProcessEnv),
                key,
                node_id: "node-a".into(),
                docs: Arc::new(|p| Err(p.id.clone())),
                factory: deps.factory,
                reloader: reconciler.reloader(),
                advertiser: gossip.advertiser(),
                metrics,
                globals: Arc::new(|_| {}),
            },
            gossip.members(),
            Duration::from_secs(10),
        );
        let api = ConfigApi {
            coordinator: Arc::new(coordinator),
            members: gossip.members(),
            file,
        };
        (api, reconciler, gossip)
    }

    /// The reload and its status (Clustering §9.2): 200 for the file the cluster runs, 202 and
    /// the next generation for a change, 422 with the report for a file that is invalid or
    /// changes a restart-only field, 503 once the node stops.
    #[tokio::test]
    async fn a_reload_publishes_the_file_and_the_status_follows() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let (api, _reconciler, _gossip) = lone(file.clone()).await;
        let coordinator = Arc::clone(&api.coordinator);
        let config = Arc::new(OnceLock::new());
        config.set(api).unwrap();
        let addr = serve(Http {
            metrics: Metrics::new(),
            ready: Arc::new(AtomicBool::new(true)),
            config,
        })
        .await;

        let (code, v) = json_of(addr, "GET", "/v1/config/status").await;
        assert_eq!(code, 200);
        assert_eq!(
            v,
            json!({ "generation": 1, "applied": 1, "nodes": { "node-a": 1 }, "converged": true })
        );
        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
        assert_eq!(
            (code, v),
            (200, json!({ "generation": 1, "unchanged": true }))
        );

        std::fs::write(
            &file,
            CONFIG.replace("drain_timeout: 45s", "drain_timeout: 40s"),
        )
        .unwrap();
        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
        assert_eq!((code, v), (202, json!({ "generation": 2 })));
        let mut status = Value::Null;
        for _ in 0..500 {
            status = json_of(addr, "GET", "/v1/config/status").await.1;
            if status["converged"] == json!(true) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            status,
            json!({ "generation": 2, "applied": 2, "nodes": { "node-a": 2 }, "converged": true })
        );

        std::fs::write(
            &file,
            CONFIG.replace("lease_duration: 30s", "tokens: 64, lease_duration: 30s"),
        )
        .unwrap();
        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
        assert_eq!(code, 422, "{v}");
        assert_eq!(v["report"]["errors"][0]["rule"], "V-061", "{v}");
        assert_eq!(
            v["report"]["errors"][0]["path"], "globals.cluster.tokens",
            "{v}"
        );

        std::fs::write(&file, "pipelines: [").unwrap();
        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
        assert_eq!(code, 422, "{v}");
        assert_eq!(v["report"]["valid"], false, "{v}");

        coordinator.stop().await;
        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
        assert_eq!(code, 503, "{v}");
        assert_eq!(coordinator.latest().await.unwrap(), Some(2));
    }
}
