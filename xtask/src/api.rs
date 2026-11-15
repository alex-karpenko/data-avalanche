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
    (
        "nodes",
        "Any member, through any node: its detail, and the actions only it takes, sent to it with the caller's own credentials. A node that doesn't answer is named (`502`, `504`).",
    ),
    (
        "events",
        "The cluster's timeline and audit trail, kept for `globals.console.events.retention`.",
    ),
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
         **Who did it.** Every route that changes something takes an optional JSON body \
         `{{\"note\": \"…\"}}` and records the action in the cluster's events: what, to what, \
         the caller's name and role as the node verified them, the note, and how it ended \
         (`accepted`, `unchanged`, `refused`). A caller turned away for its role, or with no \
         role mapped, is recorded too (`action.denied`). A note longer than 500 characters, \
         not a string or holding a control character, or a body that isn't a JSON object, is \
         `422`; a body over 1 MiB is `413`; either way nothing is done or recorded.\n\n\
         **Hashes.** A hash the API shows of a config text (a generation's `hash`, a node's \
         `file_hash`, `file.hash` and `file.generation_hash`) is the text's fingerprint keyed \
         with the cluster's fingerprint key, never its plain hash, the gossiped ones its first \
         16 hex: comparable across the nodes of one cluster only, and no help to someone \
         guessing a secret of a masked text.\n\n\
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
                    "- {} parameter `{}`: {}.",
                    if p["in"] == "query" { "Query" } else { "Path" },
                    p["name"].as_str().expect("a name"),
                    p["description"].as_str().unwrap_or_default(),
                );
            }
            if let Some(body) = op["requestBody"]["content"].as_object() {
                let types: Vec<String> = body.keys().map(|t| format!("`{t}`")).collect();
                let _ = writeln!(out, "- Body: {}.", types.join(", "));
                if let Some(d) = op["requestBody"]["description"].as_str() {
                    let _ = writeln!(out, "- {d}.");
                }
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
