//! `/v1/config`: the reload and its status (Clustering §9.2).

use std::collections::BTreeMap;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use data_avalanche_cluster::{ReloadError, Reloaded};
use serde_json::json;

use crate::{Api, body, starting};

/// Publishes the node's file as the next config generation: 202 with it, 200 when the file is
/// the generation the cluster runs already, 422 with the validation report when it is invalid or
/// changes a field only a process restart applies.
pub(crate) async fn reload(State(api): State<Api>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let text = match tokio::fs::read_to_string(&node.file).await {
        Ok(text) => text,
        Err(e) => {
            return body(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({ "error": format!("cannot read config file {}: {e}", node.file.display()) }),
            );
        }
    };
    match node.coordinator.reload(&text).await {
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
pub(crate) async fn status(State(api): State<Api>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let applied = node.coordinator.applied();
    let generation = match node.coordinator.latest().await {
        Ok(latest) => latest.unwrap_or(applied),
        Err(e) => {
            return body(
                StatusCode::SERVICE_UNAVAILABLE,
                &json!({ "error": format!("the config generation: {e}") }),
            );
        }
    };
    let nodes: BTreeMap<String, u64> = node
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;

    use data_avalanche_telemetry::Metrics;
    use serde_json::Value;

    use super::*;
    use crate::test_support::{CONFIG, json_of, lone, serve};

    /// The reload and its status (Clustering §9.2): 200 for the file the cluster runs, 202 and
    /// the next generation for a change, 422 with the report for a file that is invalid or
    /// changes a restart-only field, 503 once the node stops.
    #[tokio::test]
    async fn a_reload_publishes_the_file_and_the_status_follows() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file.clone()).await;
        let coordinator = Arc::clone(&lone.node.coordinator);
        let node = Arc::new(OnceLock::new());
        node.set(lone.node.clone()).unwrap();
        let addr = serve(Api {
            metrics: Metrics::new(),
            ready: Arc::new(AtomicBool::new(true)),
            node,
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
