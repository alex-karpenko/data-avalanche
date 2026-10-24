//! `/v1/config`: the check, the reload and its status (Clustering §9.2).

use std::collections::BTreeMap;

use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use data_avalanche_cluster::{Checked, ReloadError, Reloaded};
use data_avalanche_config::Fingerprints;
use serde_json::json;

use crate::{Api, Failure, Node, Principal, body, starting};

/// A refused reload or check: 422 with the report, 409 after the retries, 503 when
/// `globals.state` fails or the node stops.
fn refused(e: &ReloadError) -> Response {
    let code = match e {
        ReloadError::Invalid(_) | ReloadError::Rejected(_) => StatusCode::UNPROCESSABLE_ENTITY,
        ReloadError::Contended => StatusCode::CONFLICT,
        ReloadError::State(_) | ReloadError::Stopped => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut v = json!({ "error": e.to_string() });
    if let ReloadError::Invalid(r) | ReloadError::Rejected(r) = e {
        v["report"] = r.to_json();
    }
    body(code, &v)
}

/// The node's file (config §12.1), or the 500 of its read.
async fn node_file(node: &Node) -> Result<String, Failure> {
    tokio::fs::read_to_string(&node.file).await.map_err(|e| {
        Failure::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cannot read config file {}: {e}", node.file.display()),
        )
    })
}

/// Checks a config and publishes nothing (D-M9a.7): the request's body, or the node's file when
/// the body is empty, loaded in this node's environment and diffed against the generation it
/// runs. 200 with the warnings and what a reload would change; 422 with the report when it is
/// invalid or changes a field only a process restart applies. The resolved config, secrets
/// and all, is not echoed. A body's secrets aren't compared with the running ones
/// (`secrets_compared: false`): whether a guess is the running secret isn't told. Every validate
/// is logged at info with its caller, its source, the body's size and the answer's status
/// (DA-API-20).
pub(crate) async fn validate(
    State(api): State<Api>,
    Extension(by): Extension<Principal>,
    text: String,
) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let posted = !text.trim().is_empty();
    let bytes = text.len();
    let response: Response = 'checked: {
        let (checked, fingerprints) = if posted {
            (
                node.coordinator.check_posted(&text).await,
                Fingerprints::Ignored,
            )
        } else {
            match node_file(node).await {
                Ok(file) => (node.coordinator.check(&file).await, Fingerprints::Compared),
                Err(failed) => break 'checked failed.into(),
            }
        };
        match checked {
            Ok(Checked {
                generation,
                warnings,
                changes,
            }) => body(
                StatusCode::OK,
                &json!({
                    "valid": true,
                    "against_generation": generation,
                    "warnings": warnings.to_json()["warnings"],
                    "secrets_compared": fingerprints == Fingerprints::Compared,
                    "changes": { "globals": changes.globals, "pipelines": changes.pipelines },
                }),
            ),
            Err(e) => refused(&e),
        }
    };
    tracing::info!(
        by = %by.name,
        source = if posted { "body" } else { "node file" },
        bytes = bytes,
        status = response.status().as_u16(),
        "config validate through the API"
    );
    response
}

/// Publishes the node's file as the next config generation: 202 with it, 200 when the file is
/// the generation the cluster runs already, 422 with the validation report when it is invalid or
/// changes a field only a process restart applies.
pub(crate) async fn reload(State(api): State<Api>) -> Response {
    let Some(node) = api.node.get() else {
        return starting();
    };
    let text = match node_file(node).await {
        Ok(text) => text,
        Err(failed) => return failed.into(),
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
        Err(e) => refused(&e),
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
    use crate::test_support::{CONFIG, Vars, api, json_of, lone, lone_with, open, request, serve};

    #[tokio::test]
    async fn a_check_says_what_a_reload_would_change() {
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
            auth: crate::test_support::open(),
        })
        .await;
        let check = |text: String| async move {
            let (code, _, body) = request(addr, "POST", "/v1/config/validate", &[], &text).await;
            (code, serde_json::from_str::<Value>(&body).unwrap())
        };

        let (code, v) = check(String::new()).await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(
            v,
            json!({ "valid": true, "against_generation": 1, "warnings": [],
                    "secrets_compared": true,
                    "changes": { "globals": [], "pipelines": [] } }),
            "the node's file, unchanged"
        );
        let (code, v) = check(CONFIG.replace("prefix: shop", "prefix: shop2")).await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(v["changes"]["pipelines"][0]["change"], "modified", "{v}");
        assert_eq!(v["changes"]["pipelines"][0]["restart"], true, "{v}");

        let (code, v) =
            check(CONFIG.replace("lease_duration: 30s", "tokens: 64, lease_duration: 30s")).await;
        assert_eq!(code, 422, "{v}");
        assert_eq!(v["report"]["errors"][0]["rule"], "V-061", "{v}");
        let (code, v) = check("pipelines: [".into()).await;
        assert_eq!((code, &v["report"]["valid"]), (422, &json!(false)), "{v}");
        let generation = lone.node.coordinator.latest().await.unwrap();
        assert_eq!(generation, Some(1), "nothing published");
    }

    /// A body is the caller's: the check doesn't tell whether a secret in it is the running one
    /// (an oracle for a guess otherwise). The node's own file is compared in full, so a rotated
    /// secret shows. No answer holds a secret's value.
    #[tokio::test]
    async fn a_check_of_a_body_does_not_tell_whether_its_secret_is_the_running_one() {
        let templated = CONFIG.replace("password: pw", "password: ${ORDERS_PASSWORD}");
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, &templated).unwrap();
        let vars = Arc::new(Vars::default());
        vars.set("ORDERS_PASSWORD", "running-secret-1");
        let lone = lone_with(file, &templated, Arc::clone(&vars) as _).await;
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        let addr = serve(a).await;
        let check = |text: String| async move {
            let (code, _, body) = request(addr, "POST", "/v1/config/validate", &[], &text).await;
            assert!(
                !body.contains("running-secret") && !body.contains("wrong-guess"),
                "no secret's value: {body}"
            );
            assert_eq!(code, 200, "{body}");
            serde_json::from_str::<Value>(&body).unwrap()
        };
        let with =
            |password: &str| CONFIG.replace("password: pw", &format!("password: {password}"));

        let right = check(with("running-secret-1")).await;
        let wrong = check(with("wrong-guess")).await;
        assert_eq!(right["changes"], wrong["changes"]);
        assert_eq!(right["changes"], json!({ "globals": [], "pipelines": [] }));
        assert_eq!(right["secrets_compared"], false);
        assert_eq!(wrong["secrets_compared"], false);

        // The node's file, its secret rotated in the environment.
        vars.set("ORDERS_PASSWORD", "running-secret-2");
        let v = check(String::new()).await;
        assert_eq!(v["secrets_compared"], true, "{v}");
        assert_eq!(
            v["changes"]["pipelines"][0]["fields"][0]["path"],
            "pipelines.orders.source.credentials.password",
            "{v}"
        );
    }

    /// A posted body's pipeline id is taken as written (DA-API-19): the answer names a pipeline
    /// by its id, so expanding a `${VAR}` there would read this node's environment. The node's
    /// own file is the operator's, and expands.
    #[tokio::test]
    async fn a_posted_body_s_templated_id_is_refused_without_its_value() {
        let templated = CONFIG.replace("id: orders", "id: ${PIPE_ID}");
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, &templated).unwrap();
        let vars = Arc::new(Vars::default());
        vars.set("PIPE_ID", "sekret-value-1");
        let lone = lone_with(file, &templated, Arc::clone(&vars) as _).await;
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        let addr = serve(a).await;

        // A body adding a pipeline whose id is a reference.
        let added = format!(
            "{CONFIG}  shipments:\n    id: ${{PIPE_ID}}\n    source:\n      host: db.example.com\n      db: ship\n      user: cdc\n      password: pw\n      postgres: {{ publication: s_pub, slot: s_slot }}\n    tables:\n      include: [public.shipments]\n    sink:\n      type: delta\n      bucket: lake\n      prefix: ship\n      delta: {{}}\n"
        );
        let (code, _, body) = request(addr, "POST", "/v1/config/validate", &[], &added).await;
        assert!(!body.contains("sekret-value-1"), "{body}");
        assert_eq!(code, 422, "{body}");
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["report"]["errors"][0]["rule"], "V-055", "{v}");
        assert_eq!(
            v["report"]["errors"][0]["path"], "pipelines.shipments.id",
            "{v}"
        );

        // The node's file, whose id is a reference too, is validated as before.
        let (code, _, body) = request(addr, "POST", "/v1/config/validate", &[], "").await;
        assert_eq!(code, 200, "{body}");
    }

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
            auth: crate::test_support::open(),
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
