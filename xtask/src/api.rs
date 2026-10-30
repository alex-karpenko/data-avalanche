//! `reference/api.md`: the management API's `/v1` routes, read from the OpenAPI document the
//! node serves (`data_avalanche_api::openapi`), and `openapi.json` beside the site's pages.

use std::fmt::Write;

use serde_json::Value;

use crate::{BASE, cell, header};

/// The document's tags, in the page's order, with what each group does.
const TAGS: &[(&str, &str)] = &[
    (
        "pipelines",
        "Pipelines: their state, and the lifecycle commands.",
    ),
    ("snapshots", "Snapshot jobs started through the API."),
    (
        "config",
        "The config generation the cluster runs: validate, publish, status.",
    ),
    ("cluster", "Membership and leases."),
    ("admin", "The node's process."),
    ("openapi.json", "The document this page is rendered from."),
];

fn openapi_value() -> Value {
    serde_json::to_value(data_avalanche_api::openapi()).expect("the document serializes")
}

/// `openapi.json` as the node serves it, pretty-printed by utoipa (as the API crate's own
/// `openapi.json` is): a `serde_json::Value` would order its keys as the build's `serde_json`
/// features say, and a workspace build turns on `preserve_order` (DataFusion does).
pub(crate) fn document() -> String {
    data_avalanche_api::openapi()
        .to_pretty_json()
        .expect("the document serializes")
        + "\n"
}

/// An object's entries sorted by key, whatever order the build's `serde_json` keeps them in.
fn sorted(v: &Value) -> Vec<(&String, &Value)> {
    let mut entries: Vec<_> = v.as_object().expect("an object").iter().collect();
    entries.sort_by_key(|(k, _)| *k);
    entries
}

pub(crate) fn page() -> String {
    let doc = openapi_value();
    let mut operations: Vec<(&str, &str, &Value)> = Vec::new();
    for (path, item) in sorted(&doc["paths"]) {
        for (method, op) in sorted(item) {
            operations.push((path, method, op));
        }
    }

    let mut out = header(
        "Management API",
        "The node's REST API: every /v1 route, its least role, its parameters and its answers.",
        3,
        "the OpenAPI document (`data_avalanche_api::openapi`)",
    );
    let _ = write!(
        out,
        "Every node serves the API on `globals.api.address`. {} The \
         [`openapi.json`]({BASE}/openapi.json) this page is rendered from is \
         `GET /v1/openapi.json` on a node.\n\n\
         **Roles.** A route's *least role* is the lowest that may call it: `viewer` reads, \
         `operator` also runs the pipeline commands, snapshots and config reloads, `admin` also \
         makes a node leave the cluster or shut down. With `auth: none` every caller is `admin`; with `auth: basic` the one user \
         is `admin`; with `jwt` or `oauth2` the token's roles are mapped by `roles_claim` and \
         `role_mapping`.\n\n\
         **Answers.** Every body is a JSON object; a refusal carries an `error` string. Every \
         route can also answer `503` (the node is starting or stopping, or its state can't be \
         read) and, unless `auth` is `none`, `401` (no credentials, or ones that don't verify) \
         and `403` (the caller's role is below the route's). A `202` means accepted, not done: \
         a pipeline command or a snapshot job is carried out by the node that holds the \
         pipeline's lease, a reload is published, a leave or a shutdown is under way on the \
         node called.\n\n\
         **Outside `/v1`**, with no credentials: `GET /health/live` (the process answers), \
         `GET /health/ready` (`200` once the node runs its config, `503` before and while it \
         stops) and `GET /metrics` (Prometheus text).\n\n",
        cell(doc["info"]["description"].as_str().unwrap_or_default()),
    );
    for (tag, about) in TAGS {
        let _ = writeln!(out, "## {tag}\n\n{about}\n");
        for (path, method, op) in operations.iter().filter(|(_, _, op)| op["tags"][0] == *tag) {
            let _ = writeln!(
                out,
                "### `{} {path}`\n\n{}\n\nLeast role: `{}`.\n",
                method.to_uppercase(),
                op["summary"].as_str().expect("a summary"),
                op["x-min-role"].as_str().expect("a role"),
            );
            for p in op["parameters"].as_array().into_iter().flatten() {
                let _ = writeln!(
                    out,
                    "- Path parameter `{}`: {}.",
                    p["name"].as_str().expect("a name"),
                    p["description"].as_str().unwrap_or_default(),
                );
            }
            if let Some(body) = op["requestBody"]["content"].as_object() {
                let types: Vec<String> = body.keys().map(|t| format!("`{t}`")).collect();
                let _ = writeln!(out, "- Body: {}.", types.join(", "));
            }
            out.push_str("\n| Status | When |\n|---|---|\n");
            for (code, answer) in sorted(&op["responses"]) {
                if ["401", "403", "503"].contains(&code.as_str()) {
                    continue;
                }
                let _ = writeln!(
                    out,
                    "| `{code}` | {} |",
                    cell(answer["description"].as_str().unwrap_or_default()),
                );
            }
            out.push('\n');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_tag_of_the_document_has_a_section() {
        let doc = openapi_value();
        for item in doc["paths"].as_object().unwrap().values() {
            for op in item.as_object().unwrap().values() {
                let tag = op["tags"][0].as_str().unwrap();
                assert!(
                    TAGS.iter().any(|(t, _)| *t == tag),
                    "{tag}: no section in TAGS"
                );
            }
        }
    }
}
