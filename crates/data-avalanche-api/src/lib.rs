//! The management API on `globals.api.address` (System-Overview §Management API endpoints,
//! API_RBAC_and_Versioning): liveness, readiness and the metrics, unversioned and open, and the
//! `/v1` endpoints. A request may land on any node: what a pipeline's owner must do goes through
//! the pipeline's state documents, which the owner polls, and a status is read from them too
//! (D-M9a.2).
//!
//! Every `/v1` route takes the least role in API_RBAC §2 ([`routes`]): no credentials, or ones
//! that don't verify, are 401 with a `WWW-Authenticate` challenge; a caller below the route's
//! role, or with no role mapped, is 403, and recorded on a route that changes something.

mod actions;
mod auth;
mod cluster;
mod config;
mod events;
mod generations;
mod history;
mod limit;
mod live;
mod nodes;
mod openapi;
mod peers;
mod pipelines;
mod prometheus;
mod server;
mod snapshots;
mod tables;
#[cfg(test)]
mod test_support;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use data_avalanche_cluster::{
    ConfigCoordinator, ControllerStatus, DocsFn, GossipFigures, Members, Recorder, WorkerFactory,
};
use data_avalanche_config::Role;
use data_avalanche_state::{Charts, Events, Leases};
use data_avalanche_telemetry::{CONTENT_TYPE, Metrics, ProcessFigures};
use serde_json::{Value, json};
use tokio::sync::watch;

pub use actions::{DENIALS_BURST, MAX_ERROR, MAX_NOTE, MAX_TARGET, Recorded, note_of};
pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER};
pub use cluster::{Control, Stop};
pub use history::{MAX_POINTS, MAX_SPAN};
pub use limit::{RateLimit, VALIDATE_BURST, VALIDATE_PER_SECOND};
pub use live::FRESH_WITHIN;
pub use openapi::openapi;
pub use peers::{
    ACTION_LIMIT, ANSWERED_BY, DETAIL_LIMIT, FORWARDED_BY, MAX_ANSWER, Peers, PeersError,
    STATUS_LIMIT, TARGET,
};
pub use prometheus::{
    MAX_PROMETHEUS_ANSWER, MIN_RATE_WINDOW, Outcome, PrometheusClient, PrometheusError,
    PrometheusReader, promql,
};
pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
pub use snapshots::{EndedJobs, READ_AT_ONCE};
pub use tables::{FACTS_FOR, FACTS_WITHIN, Lake};

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
    /// The posted-body validates' limit ([`RateLimit::validate`], DA-API-20).
    pub validates: Arc<RateLimit>,
    /// Records the actions taken through the API, and the refused attempts (Admin_Console §6).
    pub recorder: Recorder,
    /// The cluster's timeline, read by `GET /v1/events`.
    pub events: Arc<Events>,
    /// The refused attempts recorded: [`RateLimit::denials`].
    pub denials: Arc<RateLimit>,
    /// Asks another node on a caller's behalf (D-M13b.4).
    pub peers: Peers,
    /// The process's memory and CPU, sampled.
    pub process: watch::Receiver<ProcessFigures>,
    /// What the gossip saw of its probes.
    pub gossip: watch::Receiver<GossipFigures>,
    /// The ended snapshot jobs `GET /v1/snapshots` read: they don't change.
    pub ended_jobs: Arc<EndedJobs>,
    /// The status of each pipeline this node's controllers run: the lease holder answers a
    /// pipeline's live figures from it (Admin_Console §4).
    pub statuses: watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
    /// Builds a pipeline's sink, which reads a table's Delta facts and columns on request.
    pub factory: Arc<dyn WorkerFactory>,
    /// The tables' facts and columns read, each kept [`FACTS_FOR`].
    pub lake: Arc<Lake>,
    /// The chart history every node samples into `globals.state` (Admin_Console §7).
    pub charts: Arc<Charts>,
    /// Reads a pipeline's series older than the history kept from `globals.console.prometheus`.
    pub prometheus: Arc<PrometheusReader>,
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
    /// What it records, when it changes something (Admin_Console §5).
    pub records: Option<Recorded>,
    /// Sent on to the node it names (`/v1/nodes/{node}/…`), which records the action itself:
    /// this node records only a refused attempt of its own gate.
    pub forwards: bool,
}

/// What a route's gate and recording know: the auth, the route, and the node once it runs.
#[derive(Debug, Clone)]
pub(crate) struct Gate {
    auth: Arc<Auth>,
    route: Route,
    node: Arc<OnceLock<Node>>,
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
        records: None,
        forwards: false,
    };
    // A node's route, sent on to the node it names.
    let fwd = |route: Route| Route {
        forwards: true,
        ..route
    };
    const FORWARDED: [(u16, &str); 5] = [
        (404, "no member of that name in this node's view"),
        (421, "a request forwarded to a node that isn't its target"),
        (502, "the node didn't answer"),
        (503, "the member gossips no API address"),
        (504, "the node didn't answer within the time limit"),
    ];
    // A mutating route: an optional JSON body with a `note`, and what it records.
    let act = |kind, verb, route: Route| Route {
        body: Some(route.body.unwrap_or("application/json")),
        records: Some(Recorded { kind, verb }),
        ..route
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
                        (
                            429,
                            "posted bodies past the node's rate: `Retry-After` says when",
                        ),
                    ],
                )
            },
            post(config::validate),
        ),
        (
            act(
                "config.reload",
                "reload the config",
                r(
                    "POST",
                    "/v1/config/reload",
                    Operator,
                    "Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start",
                    &[
                        (200, "the file is the generation the cluster runs"),
                        (
                            202,
                            "published: the generation, and this node's fields pending a restart",
                        ),
                        (400, "a query other than restart=accept"),
                        (409, "other nodes published first, every time"),
                        (
                            422,
                            "invalid, or it changes a restart-only field without restart=accept, or a field the cluster keeps for good: the report",
                        ),
                    ],
                ),
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
                "/v1/config/diff",
                Viewer,
                "What differs between two config generations as written: each change's path, what a reload of it does, before and after, secrets and a URL's credentials masked",
                &[
                    (200, "the pipelines added and removed, and the changes"),
                    (400, "from or to missing, or not a number"),
                    (404, "no such generation"),
                    (409, "several snapshots of one and no record of which it is"),
                    (500, "a text couldn't be compared"),
                    (503, "the config generations can't be read"),
                ],
            ),
            get(generations::diff),
        ),
        (
            r(
                "GET",
                "/v1/config/nodes",
                Viewer,
                "Each member's config generation, whether its own file is the cluster's generation, and its fields waiting for a restart",
                &[(
                    200,
                    "the generation the cluster runs, its `hash` (its text's keyed fingerprint, comparable across nodes of one cluster only), and each member: `file` compares the keyed fingerprints of the member's file and of that text",
                )],
            ),
            get(nodes::files),
        ),
        (
            r(
                "GET",
                "/v1/config/generations",
                Viewer,
                "The config generations, newest first: who published each, why, and what it changed",
                &[
                    (
                        200,
                        "the generations, each `hash` its text's keyed fingerprint (comparable across nodes of one cluster only), and `next` when the page is full",
                    ),
                    (400, "a parameter that isn't one, or isn't in its form"),
                    (503, "the config generations can't be read"),
                ],
            ),
            get(generations::list),
        ),
        (
            r(
                "GET",
                "/v1/config/generations/{generation}",
                Viewer,
                "One config generation and its text, its secrets masked and its comments shown empty",
                &[
                    (
                        200,
                        "the generation and its text, a URL's credentials masked (`••••••@`) and every comment shown empty (`#`); `hash` is the text's keyed fingerprint, comparable across nodes of one cluster only",
                    ),
                    (400, "a generation that isn't a number"),
                    (404, "no such generation"),
                    (
                        409,
                        "several snapshots of it and no record of which one it is",
                    ),
                    (
                        500,
                        "its text couldn't be masked, so it isn't shown: it has a merge key (`<<`), a tag (a `!` after a space), a directive or a byte-order mark, or a secret or a comment the masker can't place",
                    ),
                    (503, "the config generations can't be read"),
                ],
            ),
            get(generations::one),
        ),
        (
            r(
                "GET",
                "/v1/cluster/status",
                Viewer,
                "The members this node sees, and each pipeline's lease holder",
                &[(
                    200,
                    "the members and the holders; a member's `file_hash` is its file's keyed fingerprint (first 16 hex), comparable across nodes of one cluster only",
                )],
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
                "GET",
                "/v1/pipelines/{id}/live",
                Viewer,
                "What the pipeline's worker shows now, from the node that runs it: its phase, lag, source detail, and each table's state, figures and freshness; another node asks the lease holder",
                &[
                    (200, "the worker's figures"),
                    UNKNOWN,
                    (409, "no node holds the lease"),
                    (
                        421,
                        "a forwarded request to a node that doesn't run the worker",
                    ),
                    (502, "the holder didn't answer"),
                    (
                        503,
                        "the holder can't be asked: not a member, or no API address",
                    ),
                    (504, "the holder didn't answer in time"),
                ],
            ),
            get(live::get),
        ),
        (
            r(
                "GET",
                "/v1/pipelines/{id}/tables/{table}",
                Viewer,
                "A table of the pipeline, named as its config names it: what the worker shows of it, its halt and when it began, the Delta table's facts (read on request, kept 30 s), its columns at the source against Delta's, and the snapshot jobs that copied it",
                &[
                    (200, "the table"),
                    (400, "a table name not in the source's form"),
                    (404, "no such pipeline, or no trace of the table in it"),
                ],
            ),
            get(tables::get),
        ),
        (
            r(
                "GET",
                "/v1/tables",
                Viewer,
                "Every pipeline's tables, worst first, as each lease holder's worker shows them: by listed state (halted, stale, snapshot, costly, streaming, copied), pipeline, and text in the name; a pipeline whose holder can't be asked lists its halted tables from storage and is named under `unreachable`",
                &[
                    (200, "the tables, with counts by listed state"),
                    (400, "a parameter that isn't one, or a state that isn't"),
                    (404, "no such pipeline in the generation this node runs"),
                ],
            ),
            get(tables::all),
        ),
        (
            act(
                "pipeline.pause",
                "pause",
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
            ),
            post(pipelines::pause),
        ),
        (
            act(
                "pipeline.stop",
                "stop",
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
            ),
            post(pipelines::stop),
        ),
        (
            act(
                "pipeline.resume",
                "resume",
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
            ),
            post(pipelines::resume),
        ),
        (
            act(
                "pipeline.restart",
                "restart",
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
            ),
            post(pipelines::restart),
        ),
        (
            act(
                "pipeline.flush",
                "flush",
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
            ),
            post(pipelines::flush),
        ),
        (
            act(
                "snapshot.start",
                "start a snapshot job on",
                Route {
                    body: Some("application/json"),
                    ..r(
                        "POST",
                        "/v1/pipelines/{id}/snapshots",
                        Operator,
                        "Snapshot tables on demand, `{\"tables\": [...]}`, each named exactly; one outside the config is copied, not streamed",
                        &[
                            (
                                202,
                                "the job is queued: the owner restarts the worker, which takes its tables",
                            ),
                            UNKNOWN,
                            (
                                409,
                                "paused, stopped, no node holds the lease, or an unfinished job holds a table",
                            ),
                            (
                                422,
                                "the body names no table, a glob, or a table not in the source's form",
                            ),
                        ],
                    )
                },
            ),
            post(snapshots::ask),
        ),
        (
            r(
                "GET",
                "/v1/pipelines/{id}/snapshots",
                Viewer,
                "The pipeline's snapshot jobs, oldest first",
                &[(200, "the jobs"), UNKNOWN],
            ),
            get(snapshots::list),
        ),
        (
            r(
                "GET",
                "/v1/snapshots",
                Viewer,
                "Every pipeline's snapshot jobs, newest request first: by status, pipeline and who asked; a pipeline whose jobs can't be read is named under `unreadable`",
                &[
                    (200, "the jobs"),
                    (400, "a parameter that isn't one, or a status that isn't"),
                    (404, "no such pipeline in the generation this node runs"),
                ],
            ),
            get(snapshots::all),
        ),
        (
            r(
                "GET",
                "/v1/snapshots/{job_id}",
                Viewer,
                "A snapshot job: its status and each table's",
                &[(200, "the job"), (404, "no such job")],
            ),
            get(snapshots::get),
        ),
        (
            act(
                "snapshot.cancel",
                "cancel snapshot job",
                Route {
                    body: Some("application/json"),
                    ..r(
                        "POST",
                        "/v1/snapshots/{job_id}/cancel",
                        Operator,
                        "Cancel a job: the pipeline's next start ends its unended tables; a table it was bringing back from a halt, or a truncating job had begun to copy, halts, COPY_CANCELLED",
                        &[
                            (200, "cancelled already"),
                            (
                                202,
                                "cancelling: the owner restarts the worker, or the pipeline's next start ends it",
                            ),
                            (404, "no such job"),
                            (409, "the job has ended"),
                            (422, "the body isn't `{\"note\": ...}`"),
                        ],
                    )
                },
            ),
            post(snapshots::cancel),
        ),
        (
            act(
                "snapshot.retry",
                "retry snapshot job",
                Route {
                    body: Some("application/json"),
                    ..r(
                        "POST",
                        "/v1/snapshots/{job_id}/retry",
                        Operator,
                        "Retry an ended job: a new job of its tables, `{\"tables\": \"all\" | \"not_copied\", \"truncate\": ...}`, that records the job it retries",
                        &[
                            (202, "the new job is queued"),
                            (404, "no such job"),
                            (
                                409,
                                "the job hasn't ended, it left no table to copy, the pipeline doesn't run, no node holds its lease, or an unfinished job holds a table",
                            ),
                            (422, "the body isn't of that form"),
                        ],
                    )
                },
            ),
            post(snapshots::retry),
        ),
        (
            act(
                "node.leave",
                "leave the cluster:",
                r(
                    "POST",
                    "/v1/cluster/leave",
                    Admin,
                    "Leave the cluster: hand every pipeline off; the process stays up, not ready",
                    &[(202, "the leave is under way"), (409, "leaving already")],
                ),
            ),
            post(cluster::leave),
        ),
        (
            act(
                "node.shutdown",
                "shut down",
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
            ),
            post(cluster::shutdown),
        ),
        (
            r(
                "GET",
                "/v1/nodes/{node}",
                Viewer,
                "A node's detail: its build and start, its file against its generation, its process and gossip figures, the leases it holds; another node's is fetched from it",
                &[
                    (
                        200,
                        "the node's detail; `file.hash` and `file.generation_hash` are keyed fingerprints (first 16 hex), comparable across nodes of one cluster only",
                    ),
                    FORWARDED[0],
                    FORWARDED[1],
                    FORWARDED[2],
                    FORWARDED[3],
                    FORWARDED[4],
                ],
            ),
            get(nodes::get),
        ),
        (
            fwd(act(
                "node.leave",
                "leave the cluster:",
                r(
                    "POST",
                    "/v1/nodes/{node}/leave",
                    Admin,
                    "Ask a node to leave the cluster, through any node: its own `POST /v1/cluster/leave`",
                    &[
                        (202, "the leave is under way"),
                        (409, "leaving already"),
                        FORWARDED[0],
                        FORWARDED[1],
                        FORWARDED[2],
                        FORWARDED[3],
                        FORWARDED[4],
                    ],
                ),
            )),
            post(nodes::leave),
        ),
        (
            fwd(act(
                "node.shutdown",
                "shut down",
                r(
                    "POST",
                    "/v1/nodes/{node}/shutdown",
                    Admin,
                    "Ask a node to shut down, through any node: its own `POST /v1/admin/shutdown`",
                    &[
                        (202, "the shutdown is under way"),
                        (409, "shutting down already"),
                        FORWARDED[0],
                        FORWARDED[1],
                        FORWARDED[2],
                        FORWARDED[3],
                        FORWARDED[4],
                    ],
                ),
            )),
            post(nodes::shutdown),
        ),
        (
            fwd(Route {
                body: Some("application/yaml"),
                ..r(
                    "POST",
                    "/v1/nodes/{node}/config/validate",
                    Operator,
                    "Check a node's own file (an empty body), or the body, on that node: its own `POST /v1/config/validate`",
                    &[
                        (200, "it loads: its warnings and the changes"),
                        (
                            422,
                            "invalid, or it changes a restart-only field: the report",
                        ),
                        (429, "posted bodies past the node's rate"),
                        FORWARDED[0],
                        FORWARDED[1],
                        FORWARDED[2],
                        FORWARDED[3],
                        FORWARDED[4],
                    ],
                )
            }),
            post(nodes::validate),
        ),
        (
            fwd(act(
                "config.reload",
                "reload the config",
                r(
                    "POST",
                    "/v1/nodes/{node}/config/reload",
                    Operator,
                    "Publish a node's own file as the next generation, through any node: its own `POST /v1/config/reload`, with the query",
                    &[
                        (200, "the file is the generation the cluster runs"),
                        (202, "published"),
                        (409, "other nodes published first, every time"),
                        (
                            422,
                            "invalid, or a restart-only field without restart=accept",
                        ),
                        FORWARDED[0],
                        FORWARDED[1],
                        FORWARDED[2],
                        FORWARDED[3],
                        FORWARDED[4],
                    ],
                ),
            )),
            post(nodes::reload),
        ),
        (
            fwd(r(
                "GET",
                "/v1/nodes/{node}/config/status",
                Viewer,
                "A node's generation and its own restart-pending fields: its own `GET /v1/config/status`",
                &[
                    (200, "the node's config status"),
                    FORWARDED[0],
                    FORWARDED[1],
                    FORWARDED[2],
                    FORWARDED[3],
                    FORWARDED[4],
                ],
            )),
            get(nodes::config_status),
        ),
        (
            r(
                "GET",
                "/v1/events",
                Viewer,
                "The cluster's events and audit trail, newest first: actions with who took them, lease hand-offs, halts, schema changes, snapshot jobs, members, config generations",
                &[
                    (200, "the events, and `next` when there are more"),
                    (400, "a parameter that isn't one, or isn't in its form"),
                ],
            ),
            get(events::list),
        ),
        (
            r(
                "GET",
                "/v1/history",
                Viewer,
                "The series the nodes sampled, over a range: a pipeline's, a table's or a node's, from any node",
                &[
                    (
                        200,
                        "each series' points, `[unix seconds, value]`, oldest first; `kept_since`: where the history kept starts",
                    ),
                    (400, "a parameter that isn't one, or isn't in its form"),
                    (404, "no such pipeline in the generation this node runs"),
                    (503, "the chart history can't be read"),
                ],
            ),
            get(history::read),
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
        let gate = Gate {
            auth: Arc::clone(&api.auth),
            route: r,
            node: Arc::clone(&api.node),
        };
        // The gate runs first (the outer layer), then the recording.
        let handler = if r.records.is_some() && !r.forwards {
            handler.route_layer(from_fn_with_state(gate.clone(), actions::record))
        } else {
            handler
        };
        router = router.route(
            r.path,
            handler.route_layer(from_fn_with_state(gate, authorize)),
        );
    }
    router
        .layer(from_fn_with_state(api.clone(), misdirected))
        .with_state(api)
}

/// A request forwarded to this node for another one (D-M13b.4: an address since reused, a
/// renamed pod) is refused before anything reads it.
async fn misdirected(State(api): State<Api>, req: Request, next: Next) -> Response {
    let target = req.headers().get(peers::TARGET).cloned();
    if let (Some(target), Some(node)) = (target, api.node.get()) {
        let me = node.members.borrow().me.clone();
        if target.as_bytes() != me.as_bytes() {
            return body(
                StatusCode::MISDIRECTED_REQUEST,
                &json!({ "error": format!("this is {me}, not the node the request was sent to") }),
            );
        }
    }
    next.run(req).await
}

/// Lets a caller through to a route that takes `role` or less, with its [`Principal`] in the
/// request's extensions.
async fn authorize(State(gate): State<Gate>, mut req: Request, next: Next) -> Response {
    let role = gate.route.role;
    match gate.auth.verify(req.headers()).await {
        Ok(p) if p.role >= role => {
            req.extensions_mut().insert(p);
            next.run(req).await
        }
        Ok(p) => {
            let path = req.uri().path().to_owned();
            actions::denied(&gate, &p.name, &p.role.to_string(), &path).await;
            body(
                StatusCode::FORBIDDEN,
                &json!({ "error": format!("this needs the {role} role; {} is {}", p.name, p.role) }),
            )
        }
        Err(Denied::Forbidden { name, why }) => {
            let path = req.uri().path().to_owned();
            actions::denied(&gate, &name, "none", &path).await;
            body(StatusCode::FORBIDDEN, &json!({ "error": why }))
        }
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
                ("GET", "/v1/config/diff", Viewer),
                ("GET", "/v1/config/nodes", Viewer),
                ("GET", "/v1/config/generations", Viewer),
                ("GET", "/v1/config/generations/{generation}", Viewer),
                ("GET", "/v1/cluster/status", Viewer),
                ("GET", "/v1/pipelines", Viewer),
                ("GET", "/v1/pipelines/{id}", Viewer),
                ("GET", "/v1/pipelines/{id}/live", Viewer),
                ("GET", "/v1/pipelines/{id}/tables/{table}", Viewer),
                ("GET", "/v1/tables", Viewer),
                ("POST", "/v1/pipelines/{id}/pause", Operator),
                ("POST", "/v1/pipelines/{id}/stop", Operator),
                ("POST", "/v1/pipelines/{id}/resume", Operator),
                ("POST", "/v1/pipelines/{id}/restart", Operator),
                ("POST", "/v1/pipelines/{id}/flush", Operator),
                ("POST", "/v1/pipelines/{id}/snapshots", Operator),
                ("GET", "/v1/pipelines/{id}/snapshots", Viewer),
                ("GET", "/v1/snapshots", Viewer),
                ("GET", "/v1/snapshots/{job_id}", Viewer),
                ("POST", "/v1/snapshots/{job_id}/cancel", Operator),
                ("POST", "/v1/snapshots/{job_id}/retry", Operator),
                ("POST", "/v1/cluster/leave", Admin),
                ("POST", "/v1/admin/shutdown", Admin),
                ("GET", "/v1/nodes/{node}", Viewer),
                ("POST", "/v1/nodes/{node}/leave", Admin),
                ("POST", "/v1/nodes/{node}/shutdown", Admin),
                ("POST", "/v1/nodes/{node}/config/validate", Operator),
                ("POST", "/v1/nodes/{node}/config/reload", Operator),
                ("GET", "/v1/nodes/{node}/config/status", Viewer),
                ("GET", "/v1/events", Viewer),
                ("GET", "/v1/history", Viewer),
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
        let addr = serve(api(Arc::new(Auth::new(
            &idp.api("roles"),
            crate::test_support::USERNAME_CLAIM,
        ))))
        .await;
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
