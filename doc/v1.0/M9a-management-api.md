---
updated: 2026-10-23
domain: Architecture
tags: [v1, implementation-plan, m9a, api, auth, rbac, jwt, oauth2, tls, openapi, lifecycle, cluster, e2e]
resolves: [DA-API-11, DA-API-12]
---

# M9a — Management API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The management API of System-Overview's endpoint table, behind auth and roles, on any
node of the cluster:
- a listener with TLS (`globals.api.tls`);
- auth `none`, `basic`, `jwt` (JWKS) and `oauth2` (OIDC discovery), with `role_mapping`, and the
  role matrix of API_RBAC §2 (401 vs 403);
- the pipelines list and status, with halted tables;
- pause, stop and resume, with `409` where Pipeline_Lifecycle §3 forbids; restart and flush;
- config validate, and config reload and status (from M8b) behind auth;
- cluster status, leave and shutdown;
- the OpenAPI document at `GET /v1/openapi.json`.

M9 is split (Q-M9-A). **M9a** (this plan) is the API above. **M9b** is the on-demand snapshot
jobs: the worker's chunk loop for requested tables, tables outside the config, jobs persisted
under `snapshots/jobs/` and resumed by the owner, and the re-snapshot that clears a persisted
halt. M9b's routes are absent in M9a.

**Architecture:** A request may land on any node; nothing is proxied (Q-M9-B). What a pipeline's
owner must do goes through the pipeline's state documents, which the owner polls already
(Pipeline_Lifecycle §5):

- **The documents** (Task 1, `data-avalanche-state`): pause, stop and resume are a checked
  compare-and-swap of `desired_state` in `pipeline-state.json`. Two new documents sit beside it.
  `status.json` is the owner's view: actual state, error, halt code, degraded. `requests.json`
  holds the last restart and the last flush asked, each with an id.
- **The owner** (Task 2, the controller): at each poll it writes `status.json` when it changed,
  and takes a request whose id it hasn't seen this term. A restart discards the worker and runs a
  new one under the same lease; a flush is `Command::Flush`.
- **The API crate** (Tasks 3–7, new `data-avalanche-api`, roadmap §2):
  - the listener and its TLS, with the health, metrics and config routes moved from the binary
    (Task 3);
  - the auth and one route table that drives the router, the role gate and the OpenAPI document
    (Task 4);
  - the pipeline routes, which read the lease and the documents from storage (Task 5);
  - config validate, cluster status, leave and shutdown (Task 6);
  - the OpenAPI document (Task 7).
- **The binary** wires the crate. It swaps the auth on each config generation. Its run loop
  takes a leave or a shutdown from the API as it takes SIGTERM: a leave stays up, a shutdown
  exits.
- **The E2E suite** (Task 8): TLS and tokens on one node, and on three node processes any node
  drives the holder and a leave stays up.

**Tech Stack:** Rust 1.98.1 (edition 2024).
- **New crates, pinned in the workspace:**
  - `jsonwebtoken =11.1.0` (`aws_lc_rs`, no default features);
  - `reqwest 0.13.5` (`rustls`), already in the lockfile, for the JWKS and discovery;
  - `tokio-rustls 0.26.5` (`aws_lc_rs`, `tls12`), already in the lockfile;
  - `utoipa =6.0.0` (`macros`: the crate doesn't build without it, though no macro is used);
  - `base64 0.22.1` and `subtle 2.6.1`, already in the lockfile.
- **Moved:** `axum` (0.8.9) from the binary into the API crate.
- **Test-only:** `rcgen` (in the workspace since M2) generates the certificates and the ES256
  keys at run time.

**Spec:**
- `doc/v1.0/00-roadmap.md`:
  - §2, the `data-avalanche-api` crate and its place in the dependency order;
  - §3.5 and §3.7, the interfaces this plan extends;
  - §4 M9: scope and exit, "an endpoint × role matrix test (401 vs 403); lifecycle transitions
    and `409`s; the OpenAPI document is validated and snapshot-tested";
  - §9.2 Q-M9-2.
- `doc/specs/System-Overview.md` §Management API endpoints: the routes and their roles.
- `doc/research/API_RBAC_and_Versioning.md`: §1 roles, §2 the matrix, §3 binding per auth mode,
  §4 `role_mapping`, §5 V-052.
- `doc/specs/Configuration_Specification.md`:
  - §6.3 `globals.api` and its mutability (auth R, address and TLS PROC);
  - §6.3.1 `jwt` and `oauth2`;
  - §6.3.2 `tls`;
  - §4.4 PEM values;
  - V-049…V-053, V-065, V-099, V-100.
- `doc/research/Pipeline_Lifecycle_State_Machine.md`: §2 the states, §3 the transitions and `ERROR`,
  §4 persistence, §5 the controller, §6 the API.
- `doc/specs/Data-Flow.md` §State Layout in Cloud Storage: the per-pipeline documents.
- `doc/research/Clustering_Architecture_Research.md`: §9.2 (no proxy; every node reads
  `globals.state`), §20 (the controller), the leave (D-M8a.13).
- `doc/v1.0/M8a-membership.md` D-M8a.13 (the leave), `doc/v1.0/M8b-config-generations.md`
  D-M8b.7 and D-M8b.8 (the reload and its answers).

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`. The API crate also uses `state`, `config`, `core` and `telemetry`.
  `testkit` is a dev-dependency only.
- **One rustls provider:** every TLS config (the listener, the JWKS client, the test clients)
  names `aws_lc_rs` explicitly (DA-DEP-5).
- **Unversioned and open** (API_RBAC §2): `/health/live`, `/health/ready`, `/metrics` never
  challenge. Every `/v1` route takes the least role of API_RBAC §2.
- **No proxy, no node-to-node HTTP** (Q-M9-B): a request is answered from storage and the gossip
  by the node it reached.
- **Credentials:** tests generate their certificates and keys at run time and write them to temp
  dirs; nothing is committed. No secret appears in a log, an answer or a doc: the validate route
  never returns the resolved config, and a 401's reason names what failed, not the token. Never
  print the files under `test-infra/.tls/`.
- **Time in tests:** the controller's tests run on tokio's paused clock (`start_paused = true`);
  the API's tests, the listener's and the E2E suite run in real time on loopback.
- **Test gates:**
  - Plain `cargo test` stays hermetic: the API's tests serve their JWKS on loopback, and the
    state documents use `InMemory`.
  - The E2E suite needs `DATA_AVALANCHE_IT=1` and the `e2e-hooks` feature, runs with
    `--test-threads=1`, and runs on either source (`DATA_AVALANCHE_E2E_SOURCE`).
- **Public-API conventions:** public types are re-exported at the crate root. Errors are
  `thiserror` types (`ServeError`).
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready;
  a step's diffs apply in the order given, after the earlier steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt a person running the API in production. Each is pinned
by a test in the task after the arrow.

1. **A request on a node that doesn't hold the pipeline** (a load balancer in front of the
   cluster). The node must not act on the pipeline itself. It records the intent in storage and
   answers, and the holder acts at its next poll. A status read there must not show a former
   owner's view as current → Task 2 (`a_restart_asked_runs_a_new_worker_under_the_same_lease`),
   Task 5 (`a_pipeline_s_status_is_read_from_its_state_and_its_lease`), Task 8
   (`any_nodes_api_drives_the_holder`).
2. **An asker retrying after a timeout** (a write that landed but whose answer was lost). The
   retry must not restart or flush twice, nor turn a pause into a refused transition → Task 1
   (`a_request_whose_write_landed_unseen_is_recorded_once`,
   `a_transition_whose_write_landed_unseen_is_a_change`).
3. **A request left over from an earlier owner** (a restart asked, then the holder dies before it
   polls). The next owner starts a new worker anyway and must not restart it again on arrival →
   Task 2 (`requests_from_before_the_term_are_not_taken`).
4. **The identity provider rotates its signing key, or is down.** A token under the new key must
   verify without a restart, a flood of unknown key ids must not hammer the IdP, and an IdP that
   doesn't answer must be a 503 the client can retry, not a 401 that says the token is bad →
   Task 4 (`a_rotated_key_is_fetched_when_a_token_names_it`, `without_its_keys_no_token_verifies`).
5. **A token that must not pass:** expired, another issuer or audience, a key the issuer never
   published, a shared-secret (HS256) token, a token without a mapped role → Task 4
   (`a_token_that_doesnt_verify_is_refused`, `a_token_s_highest_mapped_role_is_the_caller_s`),
   and every route below its role → Task 4 (`every_route_turns_away_callers_below_its_role`).

## Decisions and rulings

**Asked and answered** (2026-10-23, all four "Recommended"):

| # | Question | Answer |
|---|---|---|
| Q-M9-A | One M9 or two? | **Split.** M9a = the API (this plan). M9b = the on-demand snapshot jobs |
| Q-M9-B | How does a request on one node reach the pipeline's owner? | **Through storage.** Pause, stop and resume set the desired state with a compare-and-swap; restart and flush are requests the owner takes at its next poll (202); the owner writes a small status document; any node answers a GET from storage |
| Q-M9-2 | `GET /v1/snapshots/{job_id}` on any node? | **The job id embeds the pipeline id** (`<pipeline_id>~<ulid>`-like), so any node reads the job from storage; no spec edit. Built in M9b |
| Q-M9-C | What do leave and shutdown do? | **Leave = the M8a leave, and the process stays up**: it owns nothing, isn't ready, still answers the API; a restart joins again. **Shutdown = the SIGTERM path**: the leave, then exit 0 |

**Rulings where the plan decides** (each pinned by a test):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M9a.1 | **The listener.** A new crate, `data-avalanche-api`, owns the router and the listener; the binary's `http.rs` moves there. With `globals.api.tls` the listener is TLS only: tokio-rustls behind axum's `Listener`, `aws_lc_rs`, ALPN `http/1.1`, each handshake in its own task bounded by `HANDSHAKE_TIMEOUT` = 10 s. A certificate or key that doesn't load is refused at bind (`ServeError::Tls`, a config error) | Roadmap §2 names the crate. A slow or silent client must not stall the accept loop. TLS is PROC (config §6.3.2), so it's set once | Task 3 (`with_tls_the_listener_serves_only_tls`, `a_key_that_isnt_one_is_refused_at_bind`), Task 8 |
| D-M9a.2 | **Routing through storage** (Q-M9-B). No node calls another. A pipeline's status is read from its lease (`globals.state`) and its documents; `status.json` is trusted only while its `owner_epoch` and holder are the live lease's. Otherwise the actual state is `starting` (a lease, no status of that term yet) or `unowned` (no lease, or a released one) | A former owner's status after a takeover would show a dead node's view. The holder writes, every node reads | Task 5 (`a_pipeline_s_status_is_read_from_its_state_and_its_lease`), Task 8 |
| D-M9a.3 | **The transitions** (Pipeline_Lifecycle §3). Pause, stop and resume compare-and-swap `desired_state`. The same state answers 200 `{unchanged: true}`; only a pause of a stopped pipeline is refused, 409 with the current state. An unknown storage outcome that then finds the target state counts as this change | Lifecycle §3: "Illegal transitions (e.g. `pause` on a `STOPPED` pipeline) return `409`". A repeated call that changes nothing isn't illegal, and a retrying client must not see a 409 for its own success | Task 1 (`the_desired_state_moves_as_the_lifecycle_allows`, `a_transition_whose_write_landed_unseen_is_a_change`), Task 5 (`pause_stop_and_resume_move_the_desired_state`) |
| D-M9a.4 | **Restart and flush are requests** in `requests.json`: the last of each kind, `{id, at}`, the id a fresh UUID v4 the asker picks, so a retried write sets the same value. A restart is refused (409) when the desired state is paused or stopped, or no node holds the lease. A flush of a paused pipeline answers 200 unchanged (it flushed when it paused); a stopped or unheld one is 409. **A restart runs a pipeline in `ERROR` again**: its desired state is running, and Lifecycle §3 names "M9's `restart`" as a way out of `ERROR` | System-Overview: restart is "`RUNNING` only … otherwise `409`" and Lifecycle §3 "on `PAUSED`/`STOPPED` it is rejected"; read here as the *desired* state, since `ERROR` is an actual state under a desired `running` (confirmed 2026-10-23, below the table) | Task 2 (`a_restart_asked_runs_a_pipeline_in_error_again`, `a_flush_asked_flushes_at_once`), Task 5 (`restart_and_flush_are_asked_of_the_lease_holder`) |
| D-M9a.5 | **The owner's side.** It writes `status.json` at a poll when the status changed (`written_at` aside), and retries a failed write at the next. It reads `requests.json` when its term starts: that read is the baseline, and a request in it isn't taken. At each poll a changed restart id discards the worker (abort, then await it) and runs a new one under the same lease, clearing `ERROR`, the dump flag and the retry state; a changed flush id sends `Command::Flush`. M8b's `Update::Restart` shares the discard | The first poll comes a renewal interval after the term starts: without the early read, a request made in that window would be taken as the baseline and lost. A new term runs a new worker, so an older request is done already | Task 2 (all five new tests) |
| D-M9a.6 | **Auth** (API_RBAC §3–§4). `none`: every caller is `admin` (`anonymous`). `basic`: the one user, constant-time compared, is `admin`. `jwt`/`oauth2`: a bearer token verified with the issuer's JWKS (RS*, PS*, ES*, EdDSA; HS* refused, since no shared secret is configured), `iss`, `aud` when set, `exp` with 60 s leeway; `oauth2` without `jwks_url` reads `discovery_url` (V-051 requires one of them), whose `issuer` must be the configured one. Keys are fetched under `FETCH_TIMEOUT` = 10 s, cached, fetched again for an unknown `kid` at most every `REFETCH_AFTER` = 30 s. Roles: the dotted `roles_claim` path, an array or one string, mapped by `role_mapping` (identity when empty), the maximum wins; none mapped → 403. 401 carries `WWW-Authenticate` `Basic` or `Bearer` with `realm="data-avalanche"`; keys that can't be fetched → 503. The auth follows each config generation (R): `Auth::set`, from the coordinator's globals callback, keeps the keys unless the verification settings changed | API_RBAC §3: "A token with no recognized role is … 403". A 503 tells the client the token may be fine. Token introspection is not built: config §6.3.1 makes `oauth2` "superset of jwt for … discovery", and a JWT verifies locally | Task 4 (all eight auth tests), Task 8 (`the_api_takes_tls_and_tokens_and_a_reload_of_its_roles`) |
| D-M9a.7 | **Config validate** (`POST /v1/config/validate`, operator). The body's YAML, or the node's file when the body is empty, goes through the reload's checks without the publish (`ConfigCoordinator::check`): 200 `{valid: true, against_generation, warnings, changes: {globals, pipelines}}`; invalid, or a PROC change (V-061), 422 `{error, report}`; a stopped coordinator or a failing `globals.state` 503. It **never returns the resolved config**. Table-pattern changes count as restarts (the catalog isn't read) | API_RBAC §2 makes validate operator because "it echoes the *resolved* config, which can contain resolved secret material". What a reload would do answers the operator's question without any secret | Task 6 (`a_check_says_what_a_reload_would_change_and_publishes_nothing`, `a_check_says_what_a_reload_would_change`) |
| D-M9a.8 | **Leave and shutdown** (Q-M9-C). `Control` holds `Stop { Run < Leave < Shutdown }` and only escalates: a second ask, or a leave after a shutdown, is 409. The binary's run loop waits on SIGTERM/SIGINT or the control. A leave sets not-ready, stops the coordinator and runs the M8a leave once, then stays up: the API answers, `/health/ready` is 503. A shutdown, or a signal, leaves if it hasn't, then ends the listener and exits 0 | An admin draining a node before maintenance wants it to stay reachable; a restart rejoins. SIGTERM's path is the one already proven (M8a) | Task 6 (`a_leave_then_a_shutdown_is_asked_once_each`, `a_leave_and_a_shutdown_are_asked_of_the_binary`), Task 8 (`a_leave_through_the_api_hands_off_and_stays_up`) |
| D-M9a.9 | **The OpenAPI document** (OpenAPI 3.1), built with utoipa's builders from the same route table as the router and the role gate, served at `GET /v1/openapi.json` to viewers. Each operation: summary, tag, `x-min-role`, the `basic` and `bearer` schemes, the `{id}` parameter, the body's content type, its answers plus the common 401/403/503. The answers' fields are not modelled (a follow-up). The document is snapshot-tested against `crates/data-avalanche-api/openapi.json` (`DATA_AVALANCHE_BLESS=1` rewrites it) | One table means a route can't be served without being documented, nor gated at another role than the document says. Roadmap §4 M9's exit | Task 7 (all three tests) |
| D-M9a.10 | **Cluster status** (`GET /v1/cluster/status`, viewer): the members this node's gossip sees (`node`, `addr`, `phase`, `suspect`, `config_generation`, `build`) and each configured pipeline's live lease holder and epoch | Answered from the gossip and `globals.state`, like the config status (D-M8b.8) | Task 6 (`the_cluster_status_names_the_members_and_the_holders`), Task 8 |

**Confirmed 2026-10-23 (D-M9a.4):** System-Overview's row says restart is "`RUNNING` only: …
otherwise `409`". `RUNNING` is the desired state, so a pipeline whose desired state is running but
whose actual state is `ERROR` (or retrying) can be restarted; that's the way out of `ERROR`
Lifecycle §3 foresees ("Until M9's `restart`, …"). The proposed System-Overview patch states it.

**Measured while planning** (2026-10-23, recorded in the tasks):

- Every task was built in a scratch workspace on this branch's base, test first. Each task's test
  stage was committed on its own and run (Step 2's output), then its implementation (Step 4's).
- The plan's code blocks were replayed onto the base with `git apply` and compared with the
  scratch workspace: the 69 blocks apply in order and give a tree identical to the workspace's last commit.
- Mutation checks, each run against its test:
  - Task 1: a pause allowed from `STOPPED` fails `the_desired_state_moves_as_the_lifecycle_allows`;
    an unknown outcome not taken as the change fails `a_transition_whose_write_landed_unseen_is_a_change`.
  - Task 2: without the requests read at the term's start, a restart asked before the first poll
    is taken as the baseline and lost: `a_restart_asked_runs_a_new_worker_under_the_same_lease`
    fails. Without the `ERROR` reset, `a_restart_asked_runs_a_pipeline_in_error_again` fails.
  - Task 4: the lowest mapped role instead of the highest fails
    `a_token_s_highest_mapped_role_is_the_caller_s`; HS256 let through fails
    `a_token_that_doesnt_verify_is_refused`.
  - Task 5: `status.json` trusted on its holder alone, or on its epoch alone, fails
    `a_pipeline_s_status_is_read_from_its_state_and_its_lease`. (Planning found the epoch-only
    gap: the test now has the holder's own status of an earlier term.)
  - Task 8: the binary without the auth swap in the globals callback fails
    `the_api_takes_tls_and_tokens_and_a_reload_of_its_roles`; a leave that exits fails
    `a_leave_through_the_api_hands_off_and_stays_up`.
- The E2E scenarios (Task 8):
  - The four API scenarios pass on PostgreSQL 18 (94.6 s) and `mysql-84` (106.5 s), and on `mysql-84-nogtid` (94.1 s) and `mariadb-118` (95.6 s).
  - The whole E2E suite on PostgreSQL 18: 42 of 42 (552 s).
  - The workspace: 1202 tests at the base, 1236 after Task 7.

**Documents this plan changes:**

- Applied at plan approval, in an `infra:` and a `gap:` commit:
  - Roadmap:
    - §2: the `data-avalanche-api` crate's dependencies (`state`, `telemetry` beside `cluster`).
    - §3.5: `status.json`, `requests.json` and `PipelineDocs::{status, save_status, requests,
      request, transition}` (D-M9a.2…5).
    - §3.7: the controller writes the status and takes the requests (D-M9a.5);
      `ConfigCoordinator::check` (D-M9a.7).
    - §3.12: the listener and the routes move to `data-avalanche-api` (D-M9a.1).
    - §4 M9: the split (Q-M9-A): M9a's scope here, M9b's (the snapshot jobs) left.
    - §9: D-IMPL-19 for D-M9a.1…10. §9.1: Q-M9-A, Q-M9-B, Q-M9-C. §9.2: Q-M9-2 answered.
  - `Pipeline_Lifecycle_State_Machine.md`, as built: §3 the same-state answer, restart from
    `ERROR`, the flush of a paused or stopped pipeline; §5 the routing through storage and the
    two documents.
  - `API_RBAC_and_Versioning.md` §2: the `GET /v1/openapi.json` row (viewer); §3 the 503 when
    the keys can't be fetched, HS* refused.
  - `doc/v1.0/README.md`: the M9a row, and an M9b row (not planned yet).
  - `CDC_Design_Gap_Analysis_v1.md`, a dated section for M9a's plan:
    - DA-API-12 goes ✅: the reload and status endpoints are behind the operator and viewer roles
      (Task 4's matrix test);
    - DA-API-11 goes ✅: a requested restart, recorded by id, takes a pipeline out of `ERROR`
      without depending on the controller's sampling (D-M9a.4, D-M9a.5). A stop and a resume
      inside one poll still aren't seen apart, and needn't be;
    - new gaps, if the review finds any.
- **Spec edits, approved and applied 2026-10-23** (`spec:` commit):
  - `System-Overview.md` §Management API endpoints:
    - the validate row: the body or the node's file; no resolved config returned (D-M9a.7);
    - the restart row: desired state `RUNNING`, `ERROR` included (D-M9a.4, raised above);
    - the leave and shutdown rows (Q-M9-C, D-M9a.8);
    - a new row, `GET /v1/openapi.json`, viewer (D-M9a.9);
    - the paragraph under the table: the same state isn't an error (D-M9a.3); any node answers,
      the owner acts at its poll (Q-M9-B).
  - `Data-Flow.md` §State Layout: `status.json` and `requests.json` (D-M9a.2, D-M9a.4).
- `test-infra/README.md`: the API scenarios (Task 8).

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-state/src/lifecycle.rs` (new), `lib.rs` | `StatusDoc`, `RequestsDoc`, `Request`, `Action`, `ActualState`, `Transition`; the `PipelineDocs` methods |
| `crates/data-avalanche-cluster/src/controller.rs` | The status written at the poll; the requests baselined at the term's start and taken; `discard` |
| `crates/data-avalanche-cluster/src/coordinator.rs`, `lib.rs` | `ConfigCoordinator::check`, `Checked` |
| `crates/data-avalanche-api/` (new): `Cargo.toml`, `openapi.json` | The crate; the OpenAPI snapshot |
| `crates/data-avalanche-api/src/lib.rs` | `Api`, `Node`, `Route`, `routes`, `router`, the role gate, health and metrics |
| `crates/data-avalanche-api/src/server.rs` | `bind`, `Bound`, `ServeError`, the TLS listener |
| `crates/data-avalanche-api/src/auth.rs` | `Auth`, `Principal`, the verifiers and the key cache |
| `crates/data-avalanche-api/src/pipelines.rs` | The pipeline routes |
| `crates/data-avalanche-api/src/config.rs` | Config validate, reload and status |
| `crates/data-avalanche-api/src/cluster.rs` | Cluster status, `Control`, `Stop`, leave and shutdown |
| `crates/data-avalanche-api/src/openapi.rs` | `openapi()`, the route |
| `crates/data-avalanche-api/src/test_support.rs` | The routes' test node (`Lone`), the request helpers, the test issuer |
| `crates/data-avalanche/src/{run,lib}.rs`, `Cargo.toml`; `http.rs` (deleted) | The API wired: the bind, the auth swap, the docs and leases, the control in the run loop |
| `Cargo.toml`, `Cargo.lock` | The new crate and its pins |
| `crates/data-avalanche/tests/e2e/{harness,api,idp,cluster,lifecycle,main}.rs` | The API scenarios and their helpers |
| `test-infra/README.md` | The API scenarios |

---
### Task 1: The lifecycle documents (`data-avalanche-state`)

**Files:**
- Modify: `crates/data-avalanche-state/src/lib.rs`
- Create: `crates/data-avalanche-state/src/lifecycle.rs`

**Interfaces:**
- Consumes: `PipelineDocs`, `DesiredState`, `PipelineStateDoc`, `Document`, `Versioned`,
  `MODIFY_ATTEMPTS` (M2, M6b).
- Produces (`lifecycle.rs`, re-exported at the crate root):
  - `STATUS_KEY = "status.json"` and `REQUESTS_KEY = "requests.json"`, at the pipeline's state
    location;
  - `ActualState { Starting, Running, Paused, Stopped, Dumped, Retrying, Error }` (snake_case);
  - `StatusDoc { holder_node_id, owner_epoch, actual, error, code, degraded, written_at }`;
  - `Request { id: String, at: DateTime<Utc> }` and
    `RequestsDoc { restart: Option<Request>, flush: Option<Request> }`;
  - `Action { Restart, Flush }`;
  - `Transition { Changed { from }, Unchanged, Refused { current } }`;
  - on `PipelineDocs`: `status() -> Result<Option<Versioned<StatusDoc>>, StateError>`,
    `save_status(&StatusDoc) -> Result<Versioned<StatusDoc>, StateError>`,
    `requests() -> Result<RequestsDoc, StateError>` (default when absent),
    `request(Action, Request) -> Result<(), StateError>`,
    `transition(DesiredState) -> Result<Transition, StateError>`.

D-M9a.2, D-M9a.3. Two new documents, not new fields: `PipelineStateDoc` is
`deny_unknown_fields` with a strict schema version, so a field added there would refuse every
older reader.

- `save_status` writes unconditionally: only the lease holder writes it, and a reader trusts it
  only under the live lease's epoch (Task 5).
- `request` sets the action's slot with a load-then-update loop. The asker picks the id, so a
  write that landed unseen (a contention outcome) is found on the retry and not written twice.
- `transition` follows Pipeline_Lifecycle §3: the same state answers `Unchanged`; only a pause of
  a stopped pipeline is `Refused`. An unknown outcome that then finds `to` is the change.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index a55843d..7be181c 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -34,6 +34,7 @@ mod anchor;
 mod doc;
 mod generation;
 mod lease;
+mod lifecycle;
 mod pipeline;
 mod snapshot;
 mod tables;
```

Create `crates/data-avalanche-state/src/lifecycle.rs`:

```rust
//! The management API's per-pipeline documents (Pipeline_Lifecycle §4–§5, D-M9a.2): the owner's
//! view of the pipeline, `status.json`, and the one-off actions asked of it, `requests.json`.
//! Any node answers a status from them, and any node records a request; the owner's controller
//! writes the status and takes the requests at its poll. The desired-state transitions of
//! pause, stop and resume are here too.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;
    use crate::doc::DocStore;
    use crate::test_support::WriteThenLoseResponse;

    fn pipeline() -> PipelineDocs {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/orders").unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(store)))
    }

    fn t(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 23, 10, 0, s).unwrap()
    }

    fn request(id: &str) -> Request {
        Request {
            id: id.into(),
            at: t(0),
        }
    }

    #[tokio::test]
    async fn a_status_is_written_and_read_back() {
        let docs = pipeline();
        assert_eq!(docs.status().await.unwrap(), None);
        let status = StatusDoc {
            holder_node_id: "node-a".into(),
            owner_epoch: 3,
            actual: ActualState::Error,
            error: Some("binlog payload invalid".into()),
            code: Some("BINLOG_PAYLOAD_INVALID".into()),
            degraded: None,
            written_at: t(1),
        };
        docs.save_status(&status).await.unwrap();
        let again = StatusDoc {
            actual: ActualState::Stopped,
            error: None,
            code: None,
            ..status
        };
        docs.save_status(&again).await.unwrap();
        assert_eq!(docs.status().await.unwrap().unwrap().doc, again);
        let raw = docs.docs.store().get(STATUS_KEY).await.unwrap().unwrap().0;
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(v["version"], 1);
        assert_eq!(v["actual"], "stopped");
    }

    #[tokio::test]
    async fn a_request_is_the_latest_of_its_kind() {
        let docs = pipeline();
        assert_eq!(docs.requests().await.unwrap(), RequestsDoc::default());
        docs.request(Action::Restart, request("r1")).await.unwrap();
        docs.request(Action::Flush, request("f1")).await.unwrap();
        docs.request(Action::Restart, request("r2")).await.unwrap();
        let r = docs.requests().await.unwrap();
        assert_eq!(r.restart.unwrap().id, "r2");
        assert_eq!(r.flush.unwrap().id, "f1");
    }

    #[tokio::test]
    async fn a_request_whose_write_landed_unseen_is_recorded_once() {
        let inner = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/orders").unwrap();
        let docs = PipelineDocs::new(DocStore::new(Arc::new(WriteThenLoseResponse::new(inner))));
        docs.request(Action::Flush, request("f1")).await.unwrap();
        assert_eq!(docs.requests().await.unwrap().flush.unwrap().id, "f1");
    }

    #[tokio::test]
    async fn the_desired_state_moves_as_the_lifecycle_allows() {
        let docs = pipeline();
        assert!(
            docs.transition(DesiredState::Paused).await.is_err(),
            "never opened"
        );
        docs.open("orders", "delta").await.unwrap();
        use DesiredState::{Paused, Running, Stopped};
        assert_eq!(
            docs.transition(Paused).await.unwrap(),
            Transition::Changed { from: Running }
        );
        assert_eq!(
            docs.transition(Paused).await.unwrap(),
            Transition::Unchanged
        );
        assert_eq!(
            docs.transition(Stopped).await.unwrap(),
            Transition::Changed { from: Paused }
        );
        assert_eq!(
            docs.transition(Paused).await.unwrap(),
            Transition::Refused { current: Stopped }
        );
        assert_eq!(
            docs.transition(Running).await.unwrap(),
            Transition::Changed { from: Stopped }
        );
        assert_eq!(
            docs.transition(Running).await.unwrap(),
            Transition::Unchanged
        );
        assert_eq!(
            docs.state().await.unwrap().unwrap().doc.desired_state,
            Running
        );
    }

    #[tokio::test]
    async fn a_transition_whose_write_landed_unseen_is_a_change() {
        let mem = Arc::new(InMemory::new());
        let plain = ObjectStoreVersioned::new(mem.clone(), "state/orders").unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(plain)))
            .open("orders", "delta")
            .await
            .unwrap();
        let inner = ObjectStoreVersioned::new(mem, "state/orders").unwrap();
        let docs = PipelineDocs::new(DocStore::new(Arc::new(
            WriteThenLoseResponse::new(inner).on_key(PIPELINE_STATE_KEY),
        )));
        assert_eq!(
            docs.transition(DesiredState::Stopped).await.unwrap(),
            Transition::Changed {
                from: DesiredState::Running
            }
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state --lib lifecycle::`

Expected: FAIL to compile: the lifecycle types and the `PipelineDocs` methods don't exist.

```text
error[E0432]: unresolved import `DesiredState`
  --> crates/data-avalanche-state/src/lifecycle.rs:91:13
error[E0425]: cannot find type `PipelineDocs` in this scope
error[E0433]: cannot find type `PipelineDocs` in this scope
error[E0425]: cannot find type `DateTime` in this scope
error[E0425]: cannot find type `Utc` in this scope
error[E0425]: cannot find value `Utc` in this scope
error[E0425]: cannot find type `Request` in this scope
error[E0422]: cannot find struct, variant or union type `Request` in this scope
error[E0422]: cannot find struct, variant or union type `StatusDoc` in this scope
error[E0425]: cannot find value `STATUS_KEY` in this scope
error[E0433]: cannot find type `DesiredState` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 7be181c..8d8ba48 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -28,7 +28,8 @@
 //! state or a changed `sink_type`), the desired state, `replication/slot.json` (PostgreSQL, with
 //! the intent recorded before a first start creates the slot), `replication/position.json` (MySQL /
 //! MariaDB, with the source server identity), per-table snapshot state, on-demand snapshot jobs,
-//! the schema cache (`schemas/<schema>.<table>.json`) and the halt registry (`halts.json`).
+//! the schema cache (`schemas/<schema>.<table>.json`), the halt registry (`halts.json`), and the
+//! management API's `status.json` and `requests.json` with the desired state's transitions.
 
 mod anchor;
 mod doc;
@@ -48,6 +49,9 @@ pub use generation::{
     config_snapshot_key,
 };
 pub use lease::{Acquired, Holder, LeaseDoc, Leases, lease_key};
+pub use lifecycle::{
+    Action, ActualState, REQUESTS_KEY, Request, RequestsDoc, STATUS_KEY, StatusDoc, Transition,
+};
 // The position document's server is core's type (M7a): the source reports it, the state records it.
 pub use data_avalanche_core::ServerIdentity;
 pub use pipeline::{
```

Change `crates/data-avalanche-state/src/lifecycle.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lifecycle.rs b/crates/data-avalanche-state/src/lifecycle.rs
index 19bcb75..7755da0 100644
--- a/crates/data-avalanche-state/src/lifecycle.rs
+++ b/crates/data-avalanche-state/src/lifecycle.rs
@@ -4,6 +4,200 @@
 //! writes the status and takes the requests at its poll. The desired-state transitions of
 //! pause, stop and resume are here too.
 
+use chrono::{DateTime, Utc};
+use serde::{Deserialize, Serialize};
+
+use crate::doc::{Document, MODIFY_ATTEMPTS, StateError, Versioned};
+use crate::pipeline::{DesiredState, PIPELINE_STATE_KEY, PipelineDocs, PipelineStateDoc};
+
+pub const STATUS_KEY: &str = "status.json";
+pub const REQUESTS_KEY: &str = "requests.json";
+
+/// The pipeline's actual state, as its owner last saw it.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(rename_all = "snake_case")]
+pub enum ActualState {
+    Starting,
+    Running,
+    Paused,
+    Stopped,
+    /// Every table is a `dump_*` table whose snapshot is done.
+    Dumped,
+    /// The worker ended with an error worth another run.
+    Retrying,
+    /// An operator must act: a stop, then a resume.
+    Error,
+}
+
+/// `status.json`: what the lease holder last wrote. It is the holder's only while
+/// `owner_epoch` is the lease's: a status a former owner wrote is stale.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct StatusDoc {
+    pub holder_node_id: String,
+    pub owner_epoch: u64,
+    pub actual: ActualState,
+    /// Why the pipeline is retrying or in `ERROR`.
+    pub error: Option<String>,
+    /// A pipeline-scope halt's code (`BINLOG_PAYLOAD_INVALID`, …).
+    pub code: Option<String>,
+    /// Sink commits or state writes are failing and being retried (the last error).
+    pub degraded: Option<String>,
+    pub written_at: DateTime<Utc>,
+}
+
+impl Document for StatusDoc {
+    const KIND: &'static str = "pipeline status";
+    const SCHEMA_VERSION: u64 = 1;
+}
+
+/// One request: an id the asker chose, so a write that landed unseen is set again, not repeated.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct Request {
+    pub id: String,
+    pub at: DateTime<Utc>,
+}
+
+/// `requests.json`: the last restart and the last flush asked of the pipeline. The owner takes
+/// one whose id it hasn't seen in its lease term; what is there when its term starts is taken as
+/// seen (a new term starts a new worker anyway).
+#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct RequestsDoc {
+    pub restart: Option<Request>,
+    pub flush: Option<Request>,
+}
+
+impl Document for RequestsDoc {
+    const KIND: &'static str = "pipeline requests";
+    const SCHEMA_VERSION: u64 = 1;
+}
+
+/// A one-off action (Pipeline_Lifecycle §3): not a state.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum Action {
+    /// Discard what is unflushed and replay from the checkpoint.
+    Restart,
+    /// Flush now.
+    Flush,
+}
+
+/// What a pause, stop or resume did.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum Transition {
+    Changed {
+        from: DesiredState,
+    },
+    /// The pipeline is in that state already.
+    Unchanged,
+    /// Pipeline_Lifecycle §3 forbids it: a pause of a stopped pipeline.
+    Refused {
+        current: DesiredState,
+    },
+}
+
+/// Pipeline_Lifecycle §3: pause from `RUNNING`, stop from `RUNNING` or `PAUSED`, resume from
+/// `PAUSED` or `STOPPED`.
+fn allowed(from: DesiredState, to: DesiredState) -> bool {
+    !(from == DesiredState::Stopped && to == DesiredState::Paused)
+}
+
+impl PipelineDocs {
+    /// # Errors
+    /// A storage or decode failure.
+    pub async fn status(&self) -> Result<Option<Versioned<StatusDoc>>, StateError> {
+        self.docs.load(STATUS_KEY).await
+    }
+
+    /// # Errors
+    /// A storage or decode failure.
+    pub async fn save_status(&self, doc: &StatusDoc) -> Result<Versioned<StatusDoc>, StateError> {
+        self.docs
+            .modify(STATUS_KEY, |_: Option<StatusDoc>| Ok(doc.clone()))
+            .await
+    }
+
+    /// The requests, none before the first.
+    ///
+    /// # Errors
+    /// A storage or decode failure.
+    pub async fn requests(&self) -> Result<RequestsDoc, StateError> {
+        Ok(self
+            .docs
+            .load::<RequestsDoc>(REQUESTS_KEY)
+            .await?
+            .map(|v| v.doc)
+            .unwrap_or_default())
+    }
+
+    /// Records `request` as the latest of its kind.
+    ///
+    /// # Errors
+    /// A storage or decode failure.
+    pub async fn request(&self, action: Action, request: Request) -> Result<(), StateError> {
+        self.docs
+            .modify(REQUESTS_KEY, |cur: Option<RequestsDoc>| {
+                let mut doc = cur.unwrap_or_default();
+                match action {
+                    Action::Restart => doc.restart = Some(request.clone()),
+                    Action::Flush => doc.flush = Some(request.clone()),
+                }
+                Ok(doc)
+            })
+            .await?;
+        Ok(())
+    }
+
+    /// Sets the desired state to `to` when Pipeline_Lifecycle §3 allows it from the current one.
+    /// The pipeline must be opened ([`Self::open`]).
+    ///
+    /// # Errors
+    /// `Corrupt` when the pipeline was never opened, a storage or decode failure, or the last
+    /// race error once the attempts run out.
+    pub async fn transition(&self, to: DesiredState) -> Result<Transition, StateError> {
+        // After an unknown outcome the write may have landed: finding `to` then is this change.
+        let mut landed: Option<DesiredState> = None;
+        for attempt in 1..=MODIFY_ATTEMPTS {
+            let Some(Versioned { doc, version }) = self
+                .docs
+                .load::<PipelineStateDoc>(PIPELINE_STATE_KEY)
+                .await?
+            else {
+                return Err(StateError::Corrupt {
+                    key: PIPELINE_STATE_KEY.into(),
+                    kind: PipelineStateDoc::KIND,
+                    reason: "missing; open the pipeline first".into(),
+                });
+            };
+            let from = doc.desired_state;
+            if from == to {
+                return Ok(match landed {
+                    Some(from) => Transition::Changed { from },
+                    None => Transition::Unchanged,
+                });
+            }
+            if !allowed(from, to) {
+                return Ok(Transition::Refused { current: from });
+            }
+            let next = PipelineStateDoc {
+                desired_state: to,
+                ..doc
+            };
+            match self.docs.update(PIPELINE_STATE_KEY, &version, &next).await {
+                Ok(_) => return Ok(Transition::Changed { from }),
+                Err(e) if e.is_race() && attempt < MODIFY_ATTEMPTS => {
+                    if !e.is_conflict() {
+                        landed = Some(from);
+                    }
+                }
+                Err(e) => return Err(e),
+            }
+        }
+        unreachable!("the last attempt returns")
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::Arc;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state --lib lifecycle::`

Expected: PASS, 5 tests.

```text
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 66 filtered out; finished in 0.00s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1207 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state/src/lib.rs \
  crates/data-avalanche-state/src/lifecycle.rs
git commit -m "feat(state): status.json and requests.json — the owner's view, the restart and flush asked of it, and the desired state's transitions"
```

---

### Task 2: The owner writes its status and takes the requests (`data-avalanche-cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/controller.rs`

**Interfaces:**
- Consumes: Task 1's `StatusDoc`, `ActualState`, `RequestsDoc`, `PipelineDocs::{save_status,
  requests}`.
- Produces: no new public item. The controller, while it holds the lease:
  - writes `status.json` at each poll when the status changed (ignoring `written_at`), and once
    more after a failed write;
  - takes a restart request whose id it hasn't seen this term: the worker is discarded (abort,
    await) and a new one runs under the same lease, with `ERROR`, the dump flag and the retry
    state cleared; logs `restarted on request`;
  - takes a flush request: `Command::Flush` to the running worker; logs `flushed on request`.

D-M9a.4, D-M9a.5. The requests are read once when the term starts (before the first
poll, which comes a renewal interval later) and that read is the baseline: a request that
predates the term is not taken, since the new term starts a new worker anyway. The same discard
now serves a reload's `Update::Restart` (M8b). A restart asked of a pipeline in `ERROR` runs it
again: Pipeline_Lifecycle §3 names "M9's `restart`" as a way out of `ERROR` (D-M9a.4).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 1ae8278..671b9c9 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1102,6 +1102,7 @@ impl PipelineController {
 #[cfg(test)]
 mod tests {
     use data_avalanche_core::{SinkError, SourceError, TableRef};
+    use data_avalanche_state::Action;
     use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
 
     use super::*;
@@ -1217,6 +1218,128 @@ mod tests {
         assert_eq!(f.builds(), 2);
     }
 
+    /// Waits (on the test clock) until `status.json` satisfies `cond`.
+    async fn status_doc_until(
+        docs: &PipelineDocs,
+        what: &str,
+        cond: impl Fn(&StatusDoc) -> bool,
+    ) -> data_avalanche_state::Versioned<StatusDoc> {
+        for _ in 0..1_200 {
+            if let Some(v) = docs.status().await.unwrap()
+                && cond(&v.doc)
+            {
+                return v;
+            }
+            tokio::time::sleep(Duration::from_millis(100)).await;
+        }
+        panic!("timed out waiting until {what}: {:?}", docs.status().await);
+    }
+
+    fn asked(id: &str) -> data_avalanche_state::Request {
+        data_avalanche_state::Request {
+            id: id.into(),
+            at: chrono::Utc::now(),
+        }
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn the_owner_writes_its_status_when_it_changes() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        let running =
+            status_doc_until(&docs, "running", |d| d.actual == ActualState::Running).await;
+        assert_eq!(running.doc.holder_node_id, "n1");
+        assert_eq!(running.doc.owner_epoch, 1);
+        assert_eq!((running.doc.error, running.doc.code), (None, None));
+        tokio::time::sleep(Duration::from_secs(30)).await;
+        assert_eq!(
+            docs.status().await.unwrap().unwrap().version,
+            running.version,
+            "an unchanged status isn't written again"
+        );
+        docs.set_desired_state(DesiredState::Paused).await.unwrap();
+        status_doc_until(&docs, "paused", |d| d.actual == ActualState::Paused).await;
+        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
+        status_doc_until(&docs, "stopped", |d| d.actual == ActualState::Stopped).await;
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn a_restart_asked_runs_a_new_worker_under_the_same_lease() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        docs.request(Action::Restart, asked("r1")).await.unwrap();
+        eventually("a second worker", || f.builds() == 2).await;
+        until(&mut n.status, "running again", is(Actual::Running)).await;
+        assert_eq!(n.status.borrow().owner_epoch, Some(1));
+        tokio::time::sleep(Duration::from_secs(30)).await;
+        assert_eq!(f.builds(), 2, "one request, one restart");
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
+        eventually("the row lands", || f.ids("a") == [1]).await;
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn a_restart_asked_runs_a_pipeline_in_error_again() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        f.source.fail(
+            Method::Preflight,
+            SourceError::Permanent("no privilege".into()),
+        );
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "error", |s| {
+            matches!(&s.actual, Some(Actual::Error { .. }))
+        })
+        .await;
+        let error = status_doc_until(&docs, "error", |d| d.actual == ActualState::Error).await;
+        assert!(
+            error.doc.error.as_deref().unwrap().contains("no privilege"),
+            "{:?}",
+            error.doc
+        );
+        docs.request(Action::Restart, asked("r1")).await.unwrap();
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        assert_eq!(f.builds(), 2);
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn a_flush_asked_flushes_at_once() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut p = pipeline();
+        p.sink.flush.seconds = Some(3_600);
+        let deps = deps("n1", &store, &f);
+        let (controller, mut status) = PipelineController::new(p, docs.clone(), &deps);
+        let (_target, rx) = watch::channel(Target::Own);
+        let _task = tokio::spawn(controller.run(rx));
+        until(&mut status, "running", is(Actual::Running)).await;
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
+        tokio::time::sleep(Duration::from_secs(30)).await;
+        assert!(f.ids("a").is_empty(), "not flushed before the hour");
+        docs.request(Action::Flush, asked("f1")).await.unwrap();
+        eventually("the row lands", || f.ids("a") == [1]).await;
+        assert_eq!(f.builds(), 1);
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn requests_from_before_the_term_are_not_taken() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        docs.request(Action::Restart, asked("r1")).await.unwrap();
+        let f = TestFactory::new(&["a"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        tokio::time::sleep(Duration::from_secs(30)).await;
+        assert_eq!(f.builds(), 1);
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_paused_pipeline_starts_paused_after_a_restart() {
         let store = ScriptedStore::new();
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib controller::`

Expected: FAIL to compile: the tests name `StatusDoc` and `ActualState`, which the controller doesn't import yet.

```text
error[E0425]: cannot find type `StatusDoc` in this scope
    --> crates/data-avalanche-cluster/src/controller.rs:1225:24
error[E0433]: cannot find type `ActualState` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 671b9c9..47efcdf 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -2,7 +2,9 @@
 //! pipeline's lease. While its target is "own" it claims the lease, then runs the pipeline's
 //! worker under it and reconciles the worker toward the persisted desired state, polled every
 //! `lease_renewal_interval` (Q-M6b-2). A worker that ends with an error is rerun, stopped for the
-//! lease, or left in `ERROR` (Q-M6b-3).
+//! lease, or left in `ERROR` (Q-M6b-3). At the same poll the owner writes the pipeline's status,
+//! `status.json`, when it changed, and takes the restart and the flush the management API asked
+//! for in `requests.json` (D-M9a.2).
 
 use std::collections::{BTreeMap, BTreeSet, HashSet};
 use std::fmt::Debug;
@@ -16,7 +18,9 @@ use data_avalanche_pipeline::{
     Backoff, Command, CommitGuard, Exit, Hooks, Phase, PipelineWorker, Recovery, Tuning,
     WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
 };
-use data_avalanche_state::{DesiredState, Holder, Leases, PipelineDocs};
+use data_avalanche_state::{
+    ActualState, DesiredState, Holder, Leases, PipelineDocs, RequestsDoc, StatusDoc,
+};
 use data_avalanche_storage::ObjectVersion;
 use data_avalanche_telemetry::{Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, Totals};
 use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
@@ -248,6 +252,52 @@ enum Event {
     Update(Update),
 }
 
+/// What [`PipelineController::asked`] found.
+#[derive(Debug, Default)]
+struct Asked {
+    restart: bool,
+    flush: bool,
+}
+
+/// The status this owner last wrote.
+#[derive(Debug, Default)]
+struct Reported {
+    last: Option<StatusDoc>,
+    /// The last write failed: the next failure isn't warned of again.
+    failing: bool,
+}
+
+/// `status.json` for an owned pipeline's status; `None` before the first publish.
+fn status_doc(node_id: &str, s: &ControllerStatus) -> Option<StatusDoc> {
+    let (actual, error, code) = match s.actual.clone()? {
+        Actual::Standby { .. } => return None,
+        Actual::Starting => (ActualState::Starting, None, None),
+        Actual::Running => (ActualState::Running, None, None),
+        Actual::Paused => (ActualState::Paused, None, None),
+        Actual::Stopped => (ActualState::Stopped, None, None),
+        Actual::Dumped => (ActualState::Dumped, None, None),
+        Actual::Retrying { error } => (ActualState::Retrying, Some(error), None),
+        Actual::Error { error, code } => (ActualState::Error, Some(error), code),
+    };
+    Some(StatusDoc {
+        holder_node_id: node_id.to_owned(),
+        owner_epoch: s.owner_epoch?,
+        actual,
+        error,
+        code,
+        degraded: s.worker.as_ref().and_then(|w| w.degraded.clone()),
+        written_at: chrono::Utc::now(),
+    })
+}
+
+/// The same status, whenever it was written.
+fn same(a: &StatusDoc, b: &StatusDoc) -> bool {
+    StatusDoc {
+        written_at: b.written_at,
+        ..a.clone()
+    } == *b
+}
+
 /// A pipeline's series labels.
 fn labels(p: &Pipeline) -> PipelineLabels {
     PipelineLabels {
@@ -570,6 +620,11 @@ impl PipelineController {
         // Unknown until a read succeeds: nothing starts on a guess (a stopped or paused pipeline
         // must not run because its state document was unreadable once).
         let mut desired = self.desired().await;
+        // What is asked already belongs to an earlier term: only later requests are taken. Read
+        // now, not at the first poll a renewal interval away, which would take a request made
+        // meanwhile as seen. A failed read leaves the first poll's read to take note.
+        let mut seen = self.docs.requests().await.ok();
+        let mut reported = Reported::default();
         let mut poll = tokio::time::interval(self.settings.timing.renewal_interval);
         poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
         poll.tick().await;
@@ -792,6 +847,29 @@ impl PipelineController {
                     if let Some(w) = worker.as_mut() {
                         self.observe(w);
                     }
+                    self.report(&mut reported).await;
+                    let asked = self.asked(&mut seen).await;
+                    // As a reload's restart: a pipeline in ERROR runs again (Pipeline_Lifecycle
+                    // §3), and a paused or stopped one stays so (the API refuses those).
+                    if asked.restart {
+                        self.discard(&mut worker).await;
+                        tracing::info!(pipeline = %self.pipeline.name, "restarted on request");
+                        error = None;
+                        dumped = false;
+                        retry = None;
+                        failures = 0;
+                    }
+                    if asked.flush {
+                        match worker.as_ref() {
+                            Some(w) => {
+                                let _ = w.handle.control.send(Command::Flush).await;
+                                tracing::info!(pipeline = %self.pipeline.name, "flushed on request");
+                            }
+                            None => {
+                                tracing::info!(pipeline = %self.pipeline.name, "a flush was asked, but no worker runs");
+                            }
+                        }
+                    }
                 }
                 Event::Update(Update::Reconfigure(p)) => {
                     self.reconfigure(*p);
@@ -803,13 +881,7 @@ impl PipelineController {
                 Event::Update(Update::Restart(p, docs)) => {
                     self.reconfigure(*p);
                     self.docs = docs;
-                    // Discard and replay (§12.4): what the worker read past the durable
-                    // checkpoint is read again by the next one.
-                    if let Some(mut w) = worker.take() {
-                        w.task.abort();
-                        let _ = (&mut w.task).await;
-                        self.metrics.worker_ended();
-                    }
+                    self.discard(&mut worker).await;
                     tracing::info!(pipeline = %self.pipeline.name, "restarted for a reload");
                     error = None;
                     dumped = false;
@@ -826,6 +898,62 @@ impl PipelineController {
         }
     }
 
+    /// Discard and replay (config §12.4): what the worker read past the durable checkpoint is
+    /// read again by the next one. False when no worker runs.
+    async fn discard(&self, worker: &mut Option<Running>) -> bool {
+        let Some(mut w) = worker.take() else {
+            return false;
+        };
+        w.task.abort();
+        // Ended (dropped) before the next worker starts: its writer goes with it.
+        let _ = (&mut w.task).await;
+        self.metrics.worker_ended();
+        true
+    }
+
+    /// Writes `status.json` when what this owner reports changed since its last write. A failed
+    /// write is warned of once, and tried again at the next poll.
+    async fn report(&self, reported: &mut Reported) {
+        let status = self.status.borrow().clone();
+        let Some(doc) = status_doc(&self.holder.node_id, &status) else {
+            return;
+        };
+        if reported.last.as_ref().is_some_and(|last| same(last, &doc)) {
+            return;
+        }
+        match self.docs.save_status(&doc).await {
+            Ok(_) => {
+                reported.last = Some(doc);
+                reported.failing = false;
+            }
+            Err(e) => {
+                if !reported.failing {
+                    tracing::warn!(pipeline = %self.pipeline.name, "status unwritten, written at the next poll: {e}");
+                }
+                reported.failing = true;
+            }
+        }
+    }
+
+    /// The restart and the flush asked since the last read. The first read of a term only takes
+    /// note of what is there.
+    async fn asked(&self, seen: &mut Option<RequestsDoc>) -> Asked {
+        let now = match self.docs.requests().await {
+            Ok(now) => now,
+            Err(e) => {
+                tracing::warn!(pipeline = %self.pipeline.name, "requests unread: {e}");
+                return Asked::default();
+            }
+        };
+        let Some(before) = seen.replace(now.clone()) else {
+            return Asked::default();
+        };
+        Asked {
+            restart: now.restart.is_some() && now.restart != before.restart,
+            flush: now.flush.is_some() && now.flush != before.flush,
+        }
+    }
+
     /// The persisted desired state, `None` on a failed read. A pipeline never started has none
     /// yet: it runs.
     async fn desired(&self) -> Option<DesiredState> {
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib controller::`

Expected: PASS, 36 tests.

```text
test result: ok. 36 passed; 0 failed; 0 ignored; 0 measured; 95 filtered out; finished in 0.44s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1212 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/controller.rs
git commit -m "feat(cluster): the owner writes status.json when its status changes and takes the restart and flush requests of its term"
```

---

### Task 3: The API crate and its listener, with TLS (`data-avalanche-api`, the binary)

**Files:**
- Modify: `Cargo.lock`
- Modify: `Cargo.toml`
- Create: `crates/data-avalanche-api/Cargo.toml`
- Create: `crates/data-avalanche-api/src/config.rs`
- Create: `crates/data-avalanche-api/src/lib.rs`
- Create: `crates/data-avalanche-api/src/server.rs`
- Create: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche/Cargo.toml`
- Delete: `crates/data-avalanche/src/http.rs` (its routes and tests move to the API crate)
- Modify: `crates/data-avalanche/src/lib.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: the binary's `http.rs` (M8b), which moves here and is deleted;
  `ConfigCoordinator` (M8b), `Metrics` (M6b), `TlsIdentity` (M1).
- Produces (`data-avalanche-api`):
  - `bind(&HostPort, Option<&TlsIdentity>) -> Result<Bound, ServeError>`;
    `Bound::local_addr()`, `Bound::serve(self, Router, stop: impl Future<Output = ()> + Send +
    'static) -> io::Result<()>`;
  - `ServeError { Bind { address, source }, Tls(String) }`; `HANDSHAKE_TIMEOUT` = 10 s;
  - `Api { metrics, ready, node: Arc<OnceLock<Node>> }`, `Node { coordinator, members, file }`
    (Tasks 4–6 add fields), `router(Api) -> Router`;
  - the moved routes: `/health/live`, `/health/ready`, `/metrics`, `POST /v1/config/reload`,
    `GET /v1/config/status`, with their answers unchanged.
- The binary: `ServeError::Tls` is a config error, `Bind` a startup error; the M8b refusal of
  `globals.api.tls` ("from M9") goes.

D-M9a.1. TLS is tokio-rustls behind axum 0.8's `Listener` trait, on the explicit
`aws_lc_rs` provider (DA-DEP-5), ALPN `http/1.1`. Each handshake runs in its own task, bounded
by `HANDSHAKE_TIMEOUT`, so a client that connects and says nothing doesn't hold up the accept
loop. A key or certificate that doesn't parse is refused at bind, before the node joins.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index e4a54c1..17a397c 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1689,9 +1689,9 @@ version = "0.1.0"
 dependencies = [
  "arrow-array",
  "arrow-cast",
- "axum",
  "chrono",
  "clap",
+ "data-avalanche-api",
  "data-avalanche-cluster",
  "data-avalanche-config",
  "data-avalanche-core",
@@ -1717,6 +1717,30 @@ dependencies = [
  "tracing-subscriber",
 ]
 
+[[package]]
+name = "data-avalanche-api"
+version = "0.1.0"
+dependencies = [
+ "axum",
+ "chrono",
+ "data-avalanche-cluster",
+ "data-avalanche-config",
+ "data-avalanche-pipeline",
+ "data-avalanche-state",
+ "data-avalanche-storage",
+ "data-avalanche-telemetry",
+ "object_store 0.14.2",
+ "rcgen",
+ "rustls",
+ "rustls-pki-types",
+ "serde_json",
+ "tempfile",
+ "thiserror 2.0.21",
+ "tokio",
+ "tokio-rustls",
+ "tracing",
+]
+
 [[package]]
 name = "data-avalanche-cluster"
 version = "0.1.0"
```

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 92056bf..e80a5cc 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -28,6 +28,7 @@ data-avalanche-source-pg = { path = "crates/data-avalanche-source-pg" }
 data-avalanche-source-mysql = { path = "crates/data-avalanche-source-mysql" }
 data-avalanche-telemetry = { path = "crates/data-avalanche-telemetry" }
 data-avalanche-cluster = { path = "crates/data-avalanche-cluster" }
+data-avalanche-api = { path = "crates/data-avalanche-api" }
 thiserror = "2"
 serde = { version = "1", features = ["derive"] }
 serde_json = { version = "1", features = ["arbitrary_precision"] }
@@ -79,6 +80,8 @@ prometheus-client = "=0.25.1"
 gethostname = "1.1"
 clap = { version = "4.6.7", features = ["derive"] }
 axum = { version = "0.8.9", default-features = false, features = ["http1", "tokio"] }
+# The management API (M9a, roadmap §2.1): TLS on the listener, on the workspace's rustls.
+tokio-rustls = { version = "0.26.5", default-features = false, features = ["aws_lc_rs", "tls12"] }
 # The cluster (M8a, roadmap §2.1): the ring's hash (the anchor's `hash_algo: xxh3`); the SWIM
 # core the gossip drives (Q-M8-1, probe `prototypes/cluster-capability`) and its random source;
 # the host's addresses, for an unset `advertise_addr`.
```

Create `crates/data-avalanche-api/Cargo.toml`:

```toml
[package]
name = "data-avalanche-api"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "The data-avalanche management API: the REST endpoints, their auth and roles, TLS and OpenAPI."

[lints]
workspace = true

[dependencies]
axum.workspace = true
data-avalanche-cluster.workspace = true
data-avalanche-config.workspace = true
data-avalanche-telemetry.workspace = true
rustls.workspace = true
rustls-pki-types.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["net", "sync", "fs"] }
tokio-rustls.workspace = true
tracing.workspace = true

[dev-dependencies]
chrono.workspace = true
data-avalanche-pipeline.workspace = true
data-avalanche-state.workspace = true
data-avalanche-storage.workspace = true
object_store.workspace = true
rcgen.workspace = true
tempfile.workspace = true
tokio = { workspace = true, features = ["io-util"] }
```

Create `crates/data-avalanche-api/src/config.rs`:

```rust
//! `/v1/config`: the reload and its status (Clustering §9.2).

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
```

Create `crates/data-avalanche-api/src/lib.rs`:

```rust
//! The management API on `globals.api.address` (System-Overview §Management API endpoints,
//! API_RBAC_and_Versioning): liveness, readiness and the metrics, unversioned and open, and the
//! `/v1` endpoints. A request may land on any node: what a pipeline's owner must do goes through
//! the pipeline's state documents, which the owner polls, and a status is read from them too
//! (D-M9a.2).

mod config;
mod server;
#[cfg(test)]
mod test_support;

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
```

Create `crates/data-avalanche-api/src/server.rs`:

```rust
//! The listener on `globals.api.address`: plain HTTP, or TLS with `globals.api.tls` (config
//! §6.3.2). Both are PROC: a reload changes neither. A TLS handshake runs off the accept loop,
//! bounded by [`HANDSHAKE_TIMEOUT`], so a client that never finishes it holds up no other.

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, OnceLock};

    use data_avalanche_config::{FingerprintKey, Origin, Pem, Secret};
    use data_avalanche_telemetry::Metrics;
    use rustls::RootCertStore;
    use rustls::pki_types::ServerName;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::TlsConnector;

    use super::*;
    use crate::{Api, router};

    /// A throwaway certificate for `localhost`, generated at run time.
    fn identity() -> (TlsIdentity, CertificateDer<'static>) {
        let c = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = FingerprintKey::from_bytes([1; 32]);
        let pem = |text: String| Pem {
            origin: Origin::Inline,
            text: Secret::new(text, &key),
        };
        let der = c.cert.der().clone();
        (
            TlsIdentity {
                cert: pem(c.cert.pem()),
                key: pem(c.signing_key.serialize_pem()),
            },
            der,
        )
    }

    async fn plain(addr: SocketAddr) -> String {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /health/live HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = vec![];
        let _ = s.read_to_end(&mut out).await;
        String::from_utf8_lossy(&out).into_owned()
    }

    #[tokio::test]
    async fn with_tls_the_listener_serves_only_tls() {
        let (id, der) = identity();
        let bound = bind(
            &HostPort {
                host: "127.0.0.1".into(),
                port: 0,
            },
            Some(&id),
        )
        .await
        .unwrap();
        let addr = bound.local_addr().unwrap();
        let api = Api {
            metrics: Metrics::new(),
            ready: Arc::new(AtomicBool::new(true)),
            node: Arc::new(OnceLock::new()),
        };
        tokio::spawn(bound.serve(router(api), std::future::pending()));

        let mut roots = RootCertStore::empty();
        roots.add(der).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let tcp = TcpStream::connect(addr).await.unwrap();
        let mut tls = TlsConnector::from(Arc::new(client))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        tls.write_all(b"GET /health/live HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut out = String::new();
        tls.read_to_string(&mut out).await.unwrap();
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        assert!(out.ends_with("live\n"), "{out}");

        let answer = plain(addr).await;
        assert!(!answer.contains("HTTP/1.1 200"), "{answer}");
    }

    #[tokio::test]
    async fn a_key_that_isnt_one_is_refused_at_bind() {
        let (mut id, _) = identity();
        id.key = id.cert.clone();
        let err = bind(
            &HostPort {
                host: "127.0.0.1".into(),
                port: 0,
            },
            Some(&id),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().starts_with("globals.api.tls: key"), "{err}");
    }
}
```

Create `crates/data-avalanche-api/src/test_support.rs`:

```rust
//! Test-only helpers shared across `data-avalanche-api`'s unit tests: a client, the listener on a
//! free port, and a lone node.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use data_avalanche_cluster::{
    ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
    Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Parts, Phase,
    ReconcilerSettings, Udp, WorkerFactory,
};
use data_avalanche_config::{FingerprintKey, HostPort, Pipeline, ProcessEnv, load_str};
use data_avalanche_pipeline::{Backoff, Tuning};
use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases};
use data_avalanche_storage::ObjectStoreVersioned;
use data_avalanche_telemetry::Metrics;
use object_store::memory::InMemory;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

use crate::{Api, Node, bind, router};

pub(crate) const CONFIG: &str = r#"
config_version: 1
globals:
  cloud: aws
  state: { bucket: app-state, prefix: cluster-a }
  cluster: { lease_duration: 30s, lease_renewal_interval: 10s, drain_timeout: 45s, max_concurrent_takeovers: 2 }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: pw
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      prefix: shop
      delta: { checkpoint_interval: 20, timestamp_ntz_as: string, merge_memory_limit: 64MiB }
"#;

/// A plain HTTP/1.1 request: the status code, the headers, the body.
pub(crate) async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String, String) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n");
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
    s.write_all(head.as_bytes()).await.unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    let (head, body) = out.split_once("\r\n\r\n").unwrap();
    let code = head[9..12].parse().unwrap();
    (code, head.to_owned(), body.to_owned())
}

pub(crate) async fn get(addr: SocketAddr, path: &str) -> (u16, String, String) {
    request(addr, "GET", path, &[], "").await
}

/// The code and the JSON body of a request without one.
pub(crate) async fn json_of(addr: SocketAddr, method: &str, path: &str) -> (u16, Value) {
    let (code, _, body) = request(addr, method, path, &[], "").await;
    (
        code,
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}")),
    )
}

/// The listener on a free loopback port, plain HTTP.
pub(crate) async fn serve(api: Api) -> SocketAddr {
    let bound = bind(
        &HostPort {
            host: "127.0.0.1".into(),
            port: 0,
        },
        None,
    )
    .await
    .unwrap();
    let addr = bound.local_addr().unwrap();
    tokio::spawn(bound.serve(router(api), std::future::pending()));
    addr
}

/// Builds no worker: the lone node runs no pipeline.
#[derive(Debug)]
struct NoWorkers;

impl WorkerFactory for NoWorkers {
    fn build(&self, p: &Pipeline) -> Result<Parts, String> {
        Err(format!("{}: the tests run no worker", p.id))
    }
}

/// A lone node `node-a` running generation 1 of [`CONFIG`], read from `file`: its gossip on a
/// loopback port, its reloads reaching a reconciler without pipelines.
pub(crate) struct Lone {
    pub node: Node,
    pub _reconciler: ClusterReconciler,
    pub _gossip: Gossip,
}

pub(crate) async fn lone(file: PathBuf) -> Lone {
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
    let c = &config.globals.cluster;
    let deps = ControllerDeps {
        leases: Leases::new(docs),
        holder: Holder {
            node_id: "node-a".into(),
            incarnation: 1,
        },
        factory: Arc::new(NoWorkers),
        metrics: metrics.clone(),
        settings: ControllerSettings {
            timing: LeaseTiming::new(c),
            drain_timeout: c.drain_timeout,
            restart: Backoff::default(),
            start_jitter: Duration::ZERO,
            tuning: Tuning::default(),
        },
        starts: Arc::new(Semaphore::new(1)),
        handoffs: Arc::new(Semaphore::new(1)),
    };
    let reconciler =
        ClusterReconciler::start(vec![], &deps, gossip.members(), ReconcilerSettings::new(c));
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
    Lone {
        node: Node {
            coordinator: Arc::new(coordinator),
            members: gossip.members(),
            file,
        },
        _reconciler: reconciler,
        _gossip: gossip,
    }
}
```

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
index fa61451..e50cdb0 100644
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -10,9 +10,9 @@ description = "The data-avalanche process: the CLI, the wiring of sources, sinks
 workspace = true
 
 [dependencies]
-axum.workspace = true
 chrono.workspace = true
 clap.workspace = true
+data-avalanche-api.workspace = true
 data-avalanche-cluster.workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index aa60480..c50f0d8 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -278,8 +278,6 @@ async fn shutdown_signal() {
 pub(crate) mod tests {
     use std::collections::BTreeMap;
 
-    use data_avalanche_config::{Origin, Pem, Secret, TlsIdentity};
-
     use super::*;
 
     pub(crate) const CONFIG: &str = r#"
@@ -329,22 +327,6 @@ pipelines:
         assert!(text.contains("bogus"), "{text}");
     }
 
-    #[test]
-    fn api_tls_is_refused_until_m9() {
-        let mut c = config();
-        assert_eq!(unsupported(&c), None);
-        let key = FingerprintKey::from_bytes([1; 32]);
-        let pem = || Pem {
-            origin: Origin::Inline,
-            text: Secret::new("-----BEGIN-----", &key),
-        };
-        c.globals.api.tls = Some(TlsIdentity {
-            cert: pem(),
-            key: pem(),
-        });
-        assert!(unsupported(&c).unwrap().contains("M9"));
-    }
-
     #[test]
     fn a_leave_waits_a_drain_timeout_a_hand_off_round_and_two_renewals() {
         let mut c = config();
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-api`

Expected: FAIL to compile: the crate's `bind`, `Api` and `router` don't exist.

```text
error[E0432]: unresolved imports `crate::Api`, `crate::router`
  --> crates/data-avalanche-api/src/server.rs:18:17
error[E0432]: unresolved imports `crate::Api`, `crate::Node`, `crate::bind`, `crate::router`
error: cannot find macro `json` in this scope
error[E0422]: cannot find struct, variant or union type `Api` in this scope
error[E0425]: cannot find type `TlsIdentity` in this scope
error[E0425]: cannot find type `CertificateDer` in this scope
error[E0422]: cannot find struct, variant or union type `TlsIdentity` in this scope
error[E0425]: cannot find type `SocketAddr` in this scope
error[E0433]: cannot find type `TcpStream` in this scope
error[E0422]: cannot find struct, variant or union type `HostPort` in this scope
error[E0433]: cannot find type `Arc` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-api/src/config.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index 4052144..eb618db 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -1,5 +1,92 @@
 //! `/v1/config`: the reload and its status (Clustering §9.2).
 
+use std::collections::BTreeMap;
+
+use axum::extract::State;
+use axum::http::StatusCode;
+use axum::response::Response;
+use data_avalanche_cluster::{ReloadError, Reloaded};
+use serde_json::json;
+
+use crate::{Api, body, starting};
+
+/// Publishes the node's file as the next config generation: 202 with it, 200 when the file is
+/// the generation the cluster runs already, 422 with the validation report when it is invalid or
+/// changes a field only a process restart applies.
+pub(crate) async fn reload(State(api): State<Api>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let text = match tokio::fs::read_to_string(&node.file).await {
+        Ok(text) => text,
+        Err(e) => {
+            return body(
+                StatusCode::INTERNAL_SERVER_ERROR,
+                &json!({ "error": format!("cannot read config file {}: {e}", node.file.display()) }),
+            );
+        }
+    };
+    match node.coordinator.reload(&text).await {
+        Ok(Reloaded {
+            generation,
+            published: true,
+        }) => body(StatusCode::ACCEPTED, &json!({ "generation": generation })),
+        Ok(Reloaded { generation, .. }) => body(
+            StatusCode::OK,
+            &json!({ "generation": generation, "unchanged": true }),
+        ),
+        Err(e) => {
+            let code = match &e {
+                ReloadError::Invalid(_) | ReloadError::Rejected(_) => {
+                    StatusCode::UNPROCESSABLE_ENTITY
+                }
+                ReloadError::Contended => StatusCode::CONFLICT,
+                ReloadError::State(_) | ReloadError::Stopped => StatusCode::SERVICE_UNAVAILABLE,
+            };
+            let mut v = json!({ "error": e.to_string() });
+            if let ReloadError::Invalid(r) | ReloadError::Rejected(r) = &e {
+                v["report"] = r.to_json();
+            }
+            body(code, &v)
+        }
+    }
+}
+
+/// The generation the cluster runs, and each member's applied one: the reload is complete when
+/// every member's is it.
+pub(crate) async fn status(State(api): State<Api>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let applied = node.coordinator.applied();
+    let generation = match node.coordinator.latest().await {
+        Ok(latest) => latest.unwrap_or(applied),
+        Err(e) => {
+            return body(
+                StatusCode::SERVICE_UNAVAILABLE,
+                &json!({ "error": format!("the config generation: {e}") }),
+            );
+        }
+    };
+    let nodes: BTreeMap<String, u64> = node
+        .members
+        .borrow()
+        .nodes
+        .iter()
+        .map(|(name, m)| (name.clone(), m.meta.config_generation))
+        .collect();
+    let converged = nodes.values().all(|n| *n == generation);
+    body(
+        StatusCode::OK,
+        &json!({
+            "generation": generation,
+            "applied": applied,
+            "nodes": nodes,
+            "converged": converged,
+        }),
+    )
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::atomic::AtomicBool;
```

Change `crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 735f6ff..c407900 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -9,6 +9,80 @@ mod server;
 #[cfg(test)]
 mod test_support;
 
+use std::path::PathBuf;
+use std::sync::atomic::{AtomicBool, Ordering};
+use std::sync::{Arc, OnceLock};
+
+use axum::Router;
+use axum::extract::State;
+use axum::http::{StatusCode, header};
+use axum::response::{IntoResponse, Response};
+use axum::routing::{get, post};
+use data_avalanche_cluster::{ConfigCoordinator, Members};
+use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
+use serde_json::{Value, json};
+use tokio::sync::watch;
+
+pub use server::{Bound, HANDSHAKE_TIMEOUT, ServeError, bind};
+
+/// What the routes read.
+#[derive(Debug, Clone)]
+pub struct Api {
+    pub metrics: Metrics,
+    /// Startup is done and every pipeline's controller runs (not: every pipeline is healthy).
+    pub ready: Arc<AtomicBool>,
+    /// Set once the node runs its config generation; the `/v1` routes answer 503 before.
+    pub node: Arc<OnceLock<Node>>,
+}
+
+/// What the `/v1` routes act on, once the node runs its config generation.
+#[derive(Debug, Clone)]
+pub struct Node {
+    pub coordinator: Arc<ConfigCoordinator>,
+    pub members: watch::Receiver<Members>,
+    /// The node's file, read again by each reload (config §12.1).
+    pub file: PathBuf,
+}
+
+pub fn router(api: Api) -> Router {
+    Router::new()
+        .route("/health/live", get(|| async { "live\n" }))
+        .route("/health/ready", get(ready))
+        .route("/metrics", get(metrics))
+        .route("/v1/config/reload", post(config::reload))
+        .route("/v1/config/status", get(config::status))
+        .with_state(api)
+}
+
+/// A JSON answer.
+pub(crate) fn body(code: StatusCode, v: &Value) -> Response {
+    (
+        code,
+        [(header::CONTENT_TYPE, "application/json")],
+        format!("{v}\n"),
+    )
+        .into_response()
+}
+
+pub(crate) fn starting() -> Response {
+    body(
+        StatusCode::SERVICE_UNAVAILABLE,
+        &json!({ "error": "the node is starting" }),
+    )
+}
+
+async fn ready(State(api): State<Api>) -> impl IntoResponse {
+    if api.ready.load(Ordering::SeqCst) {
+        (StatusCode::OK, "ready\n")
+    } else {
+        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
+    }
+}
+
+async fn metrics(State(api): State<Api>) -> impl IntoResponse {
+    ([(header::CONTENT_TYPE, CONTENT_TYPE)], api.metrics.encode())
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

Change `crates/data-avalanche-api/src/server.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/server.rs b/crates/data-avalanche-api/src/server.rs
index 88dab3f..a63cd82 100644
--- a/crates/data-avalanche-api/src/server.rs
+++ b/crates/data-avalanche-api/src/server.rs
@@ -2,6 +2,171 @@
 //! §6.3.2). Both are PROC: a reload changes neither. A TLS handshake runs off the accept loop,
 //! bounded by [`HANDSHAKE_TIMEOUT`], so a client that never finishes it holds up no other.
 
+use std::io;
+use std::net::SocketAddr;
+use std::sync::Arc;
+use std::time::Duration;
+
+use axum::Router;
+use axum::serve::Listener;
+use data_avalanche_config::{HostPort, TlsIdentity};
+use rustls::ServerConfig;
+use rustls_pki_types::pem::PemObject;
+use rustls_pki_types::{CertificateDer, PrivateKeyDer};
+use thiserror::Error;
+use tokio::net::{TcpListener, TcpStream};
+use tokio::sync::mpsc;
+use tokio_rustls::TlsAcceptor;
+use tokio_rustls::server::TlsStream;
+
+/// How long a client has to finish its TLS handshake.
+pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
+
+#[derive(Debug, Error)]
+pub enum ServeError {
+    #[error("globals.api.address {address} can't be bound: {source}")]
+    Bind {
+        address: String,
+        #[source]
+        source: io::Error,
+    },
+    #[error("globals.api.tls: {0}")]
+    Tls(String),
+}
+
+/// The bound listener: plain, or TLS.
+pub struct Bound {
+    tcp: TcpListener,
+    tls: Option<TlsAcceptor>,
+}
+
+impl std::fmt::Debug for Bound {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("Bound")
+            .field("tcp", &self.tcp)
+            .field("tls", &self.tls.is_some())
+            .finish()
+    }
+}
+
+/// Binds `address`, with TLS when `tls` is set.
+///
+/// # Errors
+/// The address can't be bound, or the certificate or key isn't usable.
+pub async fn bind(address: &HostPort, tls: Option<&TlsIdentity>) -> Result<Bound, ServeError> {
+    let tls = tls.map(acceptor).transpose()?;
+    let tcp = TcpListener::bind((address.host.as_str(), address.port))
+        .await
+        .map_err(|source| ServeError::Bind {
+            address: format!("{}:{}", address.host, address.port),
+            source,
+        })?;
+    Ok(Bound { tcp, tls })
+}
+
+impl Bound {
+    /// # Errors
+    /// The socket's address can't be read.
+    pub fn local_addr(&self) -> io::Result<SocketAddr> {
+        self.tcp.local_addr()
+    }
+
+    /// Serves `router` until `stop` resolves, then lets the open requests finish.
+    ///
+    /// # Errors
+    /// The server failed.
+    pub async fn serve(
+        self,
+        router: Router,
+        stop: impl Future<Output = ()> + Send + 'static,
+    ) -> io::Result<()> {
+        match self.tls {
+            None => {
+                axum::serve(self.tcp, router)
+                    .with_graceful_shutdown(stop)
+                    .await
+            }
+            Some(tls) => {
+                let listener = TlsListener::start(self.tcp, tls)?;
+                axum::serve(listener, router)
+                    .with_graceful_shutdown(stop)
+                    .await
+            }
+        }
+    }
+}
+
+/// The server's TLS setup: HTTP/1.1 only, as the listener serves it.
+fn acceptor(id: &TlsIdentity) -> Result<TlsAcceptor, ServeError> {
+    let certs = CertificateDer::pem_slice_iter(id.cert.text.expose().as_bytes())
+        .collect::<Result<Vec<_>, _>>()
+        .map_err(|e| ServeError::Tls(format!("cert: {e}")))?;
+    let key = PrivateKeyDer::from_pem_slice(id.key.text.expose().as_bytes())
+        .map_err(|e| ServeError::Tls(format!("key: {e}")))?;
+    // The provider is named, not the process default: two are linked (DA-DEP-5).
+    let mut config = ServerConfig::builder_with_provider(Arc::new(
+        rustls::crypto::aws_lc_rs::default_provider(),
+    ))
+    .with_safe_default_protocol_versions()
+    .map_err(|e| ServeError::Tls(e.to_string()))?
+    .with_no_client_auth()
+    .with_single_cert(certs, key)
+    .map_err(|e| ServeError::Tls(e.to_string()))?;
+    config.alpn_protocols = vec![b"http/1.1".to_vec()];
+    Ok(TlsAcceptor::from(Arc::new(config)))
+}
+
+/// Accepted connections, each handed over once its handshake is done.
+struct TlsListener {
+    addr: SocketAddr,
+    done: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
+}
+
+impl TlsListener {
+    fn start(tcp: TcpListener, tls: TlsAcceptor) -> io::Result<Self> {
+        let addr = tcp.local_addr()?;
+        let (tx, done) = mpsc::channel(64);
+        tokio::spawn(async move {
+            let mut tcp = tcp;
+            loop {
+                // Retries a failed accept after a pause, as axum's plain listener does.
+                let (stream, peer) = Listener::accept(&mut tcp).await;
+                let (tls, handed) = (tls.clone(), tx.clone());
+                tokio::spawn(async move {
+                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.accept(stream)).await {
+                        Ok(Ok(stream)) => {
+                            let _ = handed.send((stream, peer)).await;
+                        }
+                        Ok(Err(e)) => tracing::debug!(%peer, "TLS handshake failed: {e}"),
+                        Err(_) => tracing::debug!(%peer, "TLS handshake timed out"),
+                    }
+                });
+                if tx.is_closed() {
+                    return;
+                }
+            }
+        });
+        Ok(Self { addr, done })
+    }
+}
+
+impl Listener for TlsListener {
+    type Io = TlsStream<TcpStream>;
+    type Addr = SocketAddr;
+
+    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
+        match self.done.recv().await {
+            Some(conn) => conn,
+            // The accept loop ended: nothing more comes.
+            None => std::future::pending().await,
+        }
+    }
+
+    fn local_addr(&self) -> io::Result<Self::Addr> {
+        Ok(self.addr)
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::atomic::AtomicBool;
```

Change `crates/data-avalanche/src/http.rs` (the diff deletes it; `git apply` removes the file):

```diff
diff --git a/crates/data-avalanche/src/http.rs b/crates/data-avalanche/src/http.rs
deleted file mode 100644
index 996879c..0000000
--- a/crates/data-avalanche/src/http.rs
+++ /dev/null
@@ -1,426 +0,0 @@
-//! The minimal listener on `globals.api.address`: liveness, readiness, the metrics, and the
-//! config reload and its status (Clustering §9.2), all unauthenticated (System-Overview
-//! §Observability, API_RBAC §2). The rest of the `/v1` API, its auth and TLS arrive with M9.
-
-use std::collections::BTreeMap;
-use std::path::PathBuf;
-use std::sync::atomic::{AtomicBool, Ordering};
-use std::sync::{Arc, OnceLock};
-
-use axum::Router;
-use axum::extract::State;
-use axum::http::{StatusCode, header};
-use axum::response::{IntoResponse, Response};
-use axum::routing::{get, post};
-use data_avalanche_cluster::{ConfigCoordinator, Members, ReloadError, Reloaded};
-use data_avalanche_config::HostPort;
-use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
-use serde_json::{Value, json};
-use tokio::net::TcpListener;
-use tokio::sync::watch;
-
-use crate::run::Fatal;
-
-/// What the routes read.
-#[derive(Debug, Clone)]
-pub(crate) struct Http {
-    pub metrics: Metrics,
-    /// Startup is done and every pipeline's controller runs (not: every pipeline is healthy).
-    pub ready: Arc<AtomicBool>,
-    /// Set once the node runs its config generation; the config routes answer 503 before.
-    pub config: Arc<OnceLock<ConfigApi>>,
-}
-
-/// What the config routes act on.
-#[derive(Debug, Clone)]
-pub(crate) struct ConfigApi {
-    pub coordinator: Arc<ConfigCoordinator>,
-    pub members: watch::Receiver<Members>,
-    /// The node's file, read again by each reload (config §12.1).
-    pub file: PathBuf,
-}
-
-pub(crate) fn router(http: Http) -> Router {
-    Router::new()
-        .route("/health/live", get(|| async { "live\n" }))
-        .route("/health/ready", get(ready))
-        .route("/metrics", get(metrics))
-        .route("/v1/config/reload", post(reload))
-        .route("/v1/config/status", get(status))
-        .with_state(http)
-}
-
-fn body(code: StatusCode, v: &Value) -> Response {
-    (
-        code,
-        [(header::CONTENT_TYPE, "application/json")],
-        format!("{v}\n"),
-    )
-        .into_response()
-}
-
-fn starting() -> Response {
-    body(
-        StatusCode::SERVICE_UNAVAILABLE,
-        &json!({ "error": "the node is starting" }),
-    )
-}
-
-/// Publishes the node's file as the next config generation: 202 with it, 200 when the file is
-/// the generation the cluster runs already, 422 with the validation report when it is invalid or
-/// changes a field only a process restart applies.
-async fn reload(State(http): State<Http>) -> Response {
-    let Some(api) = http.config.get() else {
-        return starting();
-    };
-    let text = match tokio::fs::read_to_string(&api.file).await {
-        Ok(text) => text,
-        Err(e) => {
-            return body(
-                StatusCode::INTERNAL_SERVER_ERROR,
-                &json!({ "error": format!("cannot read config file {}: {e}", api.file.display()) }),
-            );
-        }
-    };
-    match api.coordinator.reload(&text).await {
-        Ok(Reloaded {
-            generation,
-            published: true,
-        }) => body(StatusCode::ACCEPTED, &json!({ "generation": generation })),
-        Ok(Reloaded { generation, .. }) => body(
-            StatusCode::OK,
-            &json!({ "generation": generation, "unchanged": true }),
-        ),
-        Err(e) => {
-            let code = match &e {
-                ReloadError::Invalid(_) | ReloadError::Rejected(_) => {
-                    StatusCode::UNPROCESSABLE_ENTITY
-                }
-                ReloadError::Contended => StatusCode::CONFLICT,
-                ReloadError::State(_) | ReloadError::Stopped => StatusCode::SERVICE_UNAVAILABLE,
-            };
-            let mut v = json!({ "error": e.to_string() });
-            if let ReloadError::Invalid(r) | ReloadError::Rejected(r) = &e {
-                v["report"] = r.to_json();
-            }
-            body(code, &v)
-        }
-    }
-}
-
-/// The generation the cluster runs, and each member's applied one: the reload is complete when
-/// every member's is it.
-async fn status(State(http): State<Http>) -> Response {
-    let Some(api) = http.config.get() else {
-        return starting();
-    };
-    let applied = api.coordinator.applied();
-    let generation = match api.coordinator.latest().await {
-        Ok(latest) => latest.unwrap_or(applied),
-        Err(e) => {
-            return body(
-                StatusCode::SERVICE_UNAVAILABLE,
-                &json!({ "error": format!("the config generation: {e}") }),
-            );
-        }
-    };
-    let nodes: BTreeMap<String, u64> = api
-        .members
-        .borrow()
-        .nodes
-        .iter()
-        .map(|(name, m)| (name.clone(), m.meta.config_generation))
-        .collect();
-    let converged = nodes.values().all(|n| *n == generation);
-    body(
-        StatusCode::OK,
-        &json!({
-            "generation": generation,
-            "applied": applied,
-            "nodes": nodes,
-            "converged": converged,
-        }),
-    )
-}
-
-async fn ready(State(http): State<Http>) -> impl IntoResponse {
-    if http.ready.load(Ordering::SeqCst) {
-        (StatusCode::OK, "ready\n")
-    } else {
-        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
-    }
-}
-
-async fn metrics(State(http): State<Http>) -> impl IntoResponse {
-    (
-        [(header::CONTENT_TYPE, CONTENT_TYPE)],
-        http.metrics.encode(),
-    )
-}
-
-/// # Errors
-/// The address can't be bound.
-pub(crate) async fn bind(address: &HostPort) -> Result<TcpListener, Fatal> {
-    TcpListener::bind((address.host.as_str(), address.port))
-        .await
-        .map_err(|e| {
-            Fatal::Startup(format!(
-                "globals.api.address {}:{} can't be bound: {e}",
-                address.host, address.port
-            ))
-        })
-}
-
-#[cfg(test)]
-mod tests {
-    use std::net::SocketAddr;
-    use std::time::Duration;
-
-    use data_avalanche_cluster::{
-        ClusterReconciler, ControllerDeps, CoordinatorDeps, Gossip, GossipKey, GossipSettings,
-        NodeIdentity, NodeMeta, Phase, ReconcilerSettings, Udp,
-    };
-    use data_avalanche_config::{FingerprintKey, ProcessEnv, load_str};
-    use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases};
-    use data_avalanche_storage::ObjectStoreVersioned;
-    use object_store::memory::InMemory;
-    use tokio::io::{AsyncReadExt, AsyncWriteExt};
-    use tokio::sync::Semaphore;
-
-    use super::*;
-    use crate::Factory;
-    use crate::run::tests::CONFIG;
-
-    /// A plain HTTP/1.1 request without a body: the status code, the headers, the body.
-    async fn send(addr: SocketAddr, method: &str, path: &str) -> (u16, String, String) {
-        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
-        s.write_all(
-            format!(
-                "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
-            )
-            .as_bytes(),
-        )
-        .await
-        .unwrap();
-        let mut out = String::new();
-        s.read_to_string(&mut out).await.unwrap();
-        let (head, body) = out.split_once("\r\n\r\n").unwrap();
-        let code = head[9..12].parse().unwrap();
-        (code, head.to_owned(), body.to_owned())
-    }
-
-    async fn get(addr: SocketAddr, path: &str) -> (u16, String, String) {
-        send(addr, "GET", path).await
-    }
-
-    /// The code and the JSON body.
-    async fn json_of(addr: SocketAddr, method: &str, path: &str) -> (u16, Value) {
-        let (code, _, body) = send(addr, method, path).await;
-        (
-            code,
-            serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}")),
-        )
-    }
-
-    /// The listener on a free port.
-    async fn serve(http: Http) -> SocketAddr {
-        let listener = bind(&HostPort {
-            host: "127.0.0.1".into(),
-            port: 0,
-        })
-        .await
-        .unwrap();
-        let addr = listener.local_addr().unwrap();
-        tokio::spawn(async move { axum::serve(listener, router(http)).await });
-        addr
-    }
-
-    #[tokio::test]
-    async fn the_listener_answers_health_and_metrics() {
-        let metrics = Metrics::new();
-        metrics.set_cluster_members(1);
-        let ready = Arc::new(AtomicBool::new(false));
-        let addr = serve(Http {
-            metrics,
-            ready: Arc::clone(&ready),
-            config: Arc::default(),
-        })
-        .await;
-
-        assert_eq!(get(addr, "/health/live").await.0, 200);
-        assert_eq!(
-            get(addr, "/health/ready").await.0,
-            503,
-            "not before startup ends"
-        );
-        ready.store(true, Ordering::SeqCst);
-        assert_eq!(get(addr, "/health/ready").await.0, 200);
-        let (code, head, body) = get(addr, "/metrics").await;
-        assert_eq!(code, 200);
-        assert!(
-            head.to_ascii_lowercase()
-                .contains("content-type: application/openmetrics-text"),
-            "{head}"
-        );
-        assert!(body.contains("data_avalanche_cluster_members 1"), "{body}");
-        assert!(body.ends_with("# EOF\n"), "{body}");
-        assert_eq!(get(addr, "/v1/pipelines").await.0, 404, "the API is M9's");
-        assert_eq!(
-            json_of(addr, "POST", "/v1/config/reload").await.0,
-            503,
-            "not before the node runs its generation"
-        );
-        assert_eq!(json_of(addr, "GET", "/v1/config/status").await.0, 503);
-    }
-
-    /// A lone node `node-a` running generation 1 of [`CONFIG`], read from `file`: its gossip on
-    /// a loopback port, its reloads reaching a reconciler without pipelines.
-    async fn lone(file: PathBuf) -> (ConfigApi, ClusterReconciler, Gossip) {
-        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
-        let docs = DocStore::new(Arc::new(store));
-        let generations = ConfigGenerations::new(docs.clone());
-        let (first, _) = generations
-            .bootstrap_or_adopt(CONFIG, "node-a", chrono::Utc::now())
-            .await
-            .unwrap();
-        let key = FingerprintKey::from_bytes([1; 32]);
-        let config = load_str(CONFIG, &ProcessEnv, &key).unwrap().config;
-        let (udp, inbox) = Udp::bind(&["127.0.0.1:0".parse().unwrap()]).await.unwrap();
-        let me = NodeIdentity {
-            node_id: "node-a".into(),
-            incarnation: 1,
-            rev: 0,
-            addr: udp.local_addrs()[0],
-            meta: NodeMeta {
-                phase: Phase::Active,
-                config_generation: 1,
-                build: "1.0.0".into(),
-            },
-        };
-        let gossip = Gossip::start(
-            me,
-            GossipKey::derive(&[7; 32]),
-            GossipSettings::new(&config.globals.cluster.memberlist),
-            Arc::new(udp),
-            inbox,
-        );
-        let metrics = Metrics::new();
-        let deps = ControllerDeps {
-            leases: Leases::new(docs),
-            holder: Holder {
-                node_id: "node-a".into(),
-                incarnation: 1,
-            },
-            factory: Arc::new(Factory::new(&config.globals).unwrap()),
-            metrics: metrics.clone(),
-            settings: crate::run::settings(&config),
-            starts: Arc::new(Semaphore::new(1)),
-            handoffs: Arc::new(Semaphore::new(1)),
-        };
-        let reconciler = ClusterReconciler::start(
-            vec![],
-            &deps,
-            gossip.members(),
-            ReconcilerSettings::new(&config.globals.cluster),
-        );
-        let coordinator = ConfigCoordinator::start(
-            config,
-            first.pointer,
-            CoordinatorDeps {
-                generations,
-                env: Arc::new(ProcessEnv),
-                key,
-                node_id: "node-a".into(),
-                docs: Arc::new(|p| Err(p.id.clone())),
-                factory: deps.factory,
-                reloader: reconciler.reloader(),
-                advertiser: gossip.advertiser(),
-                metrics,
-                globals: Arc::new(|_| {}),
-            },
-            gossip.members(),
-            Duration::from_secs(10),
-        );
-        let api = ConfigApi {
-            coordinator: Arc::new(coordinator),
-            members: gossip.members(),
-            file,
-        };
-        (api, reconciler, gossip)
-    }
-
-    /// The reload and its status (Clustering §9.2): 200 for the file the cluster runs, 202 and
-    /// the next generation for a change, 422 with the report for a file that is invalid or
-    /// changes a restart-only field, 503 once the node stops.
-    #[tokio::test]
-    async fn a_reload_publishes_the_file_and_the_status_follows() {
-        let dir = tempfile::tempdir().unwrap();
-        let file = dir.path().join("config.yaml");
-        std::fs::write(&file, CONFIG).unwrap();
-        let (api, _reconciler, _gossip) = lone(file.clone()).await;
-        let coordinator = Arc::clone(&api.coordinator);
-        let config = Arc::new(OnceLock::new());
-        config.set(api).unwrap();
-        let addr = serve(Http {
-            metrics: Metrics::new(),
-            ready: Arc::new(AtomicBool::new(true)),
-            config,
-        })
-        .await;
-
-        let (code, v) = json_of(addr, "GET", "/v1/config/status").await;
-        assert_eq!(code, 200);
-        assert_eq!(
-            v,
-            json!({ "generation": 1, "applied": 1, "nodes": { "node-a": 1 }, "converged": true })
-        );
-        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
-        assert_eq!(
-            (code, v),
-            (200, json!({ "generation": 1, "unchanged": true }))
-        );
-
-        std::fs::write(
-            &file,
-            CONFIG.replace("drain_timeout: 45s", "drain_timeout: 40s"),
-        )
-        .unwrap();
-        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
-        assert_eq!((code, v), (202, json!({ "generation": 2 })));
-        let mut status = Value::Null;
-        for _ in 0..500 {
-            status = json_of(addr, "GET", "/v1/config/status").await.1;
-            if status["converged"] == json!(true) {
-                break;
-            }
-            tokio::time::sleep(Duration::from_millis(10)).await;
-        }
-        assert_eq!(
-            status,
-            json!({ "generation": 2, "applied": 2, "nodes": { "node-a": 2 }, "converged": true })
-        );
-
-        std::fs::write(
-            &file,
-            CONFIG.replace("lease_duration: 30s", "tokens: 64, lease_duration: 30s"),
-        )
-        .unwrap();
-        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
-        assert_eq!(code, 422, "{v}");
-        assert_eq!(v["report"]["errors"][0]["rule"], "V-061", "{v}");
-        assert_eq!(
-            v["report"]["errors"][0]["path"], "globals.cluster.tokens",
-            "{v}"
-        );
-
-        std::fs::write(&file, "pipelines: [").unwrap();
-        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
-        assert_eq!(code, 422, "{v}");
-        assert_eq!(v["report"]["valid"], false, "{v}");
-
-        coordinator.stop().await;
-        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
-        assert_eq!(code, 503, "{v}");
-        assert_eq!(coordinator.latest().await.unwrap(), Some(2));
-    }
-}
```

Change `crates/data-avalanche/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche/src/lib.rs b/crates/data-avalanche/src/lib.rs
index 4183633..23e9fe5 100644
--- a/crates/data-avalanche/src/lib.rs
+++ b/crates/data-avalanche/src/lib.rs
@@ -6,7 +6,6 @@
 mod cli;
 #[cfg(feature = "e2e-hooks")]
 pub mod crash;
-mod http;
 mod logging;
 mod run;
 mod startup;
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index c50f0d8..dc86589 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -1,11 +1,11 @@
 //! `data-avalanche run`.
 
-use std::future::IntoFuture;
 use std::net::SocketAddr;
 use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::{Arc, OnceLock};
 use std::time::{Duration, SystemTime};
 
+use data_avalanche_api::{Api, Node, ServeError, bind, router};
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
@@ -21,7 +21,6 @@ use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
 use tokio::sync::{Semaphore, oneshot};
 
-use crate::http::{self, ConfigApi, Http};
 use crate::startup::{Buckets, join};
 use crate::{Factory, RunArgs, logging};
 
@@ -65,14 +64,6 @@ pub(crate) fn render(report: &Report) -> String {
     report.iter().map(line).collect::<Vec<_>>().join("\n")
 }
 
-/// What this build doesn't serve yet, refused before anything starts.
-pub(crate) fn unsupported(config: &ResolvedConfig) -> Option<String> {
-    config.globals.api.tls.as_ref().map(|_| {
-        "globals.api.tls: the API listener serves TLS from M9; this build serves plain HTTP only"
-            .to_owned()
-    })
-}
-
 /// The controllers' timings from `globals.cluster`.
 pub(crate) fn settings(config: &ResolvedConfig) -> ControllerSettings {
     let c = &config.globals.cluster;
@@ -114,37 +105,33 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     for w in &first.warnings {
         tracing::warn!("config: {}", line(w));
     }
-    if let Some(why) = unsupported(&first.config) {
-        return Err(Fatal::Config(why));
-    }
     // The listener answers from here: live at once, ready once every controller runs.
     let metrics = Metrics::new();
     let ready = Arc::new(AtomicBool::new(false));
-    let config_api = Arc::new(OnceLock::new());
-    let listener = http::bind(&first.config.globals.api.address).await?;
+    let node = Arc::new(OnceLock::new());
+    let api = &first.config.globals.api;
+    let bound = bind(&api.address, api.tls.as_ref())
+        .await
+        .map_err(|e| match e {
+            ServeError::Tls(_) => Fatal::Config(e.to_string()),
+            ServeError::Bind { .. } => Fatal::Startup(e.to_string()),
+        })?;
     let (stop_http, http_stopped) = oneshot::channel::<()>();
-    let server = tokio::spawn(
-        axum::serve(
-            listener,
-            http::router(Http {
-                metrics: metrics.clone(),
-                ready: Arc::clone(&ready),
-                config: Arc::clone(&config_api),
-            }),
-        )
-        .with_graceful_shutdown(async {
+    let server = tokio::spawn(bound.serve(
+        router(Api {
+            metrics: metrics.clone(),
+            ready: Arc::clone(&ready),
+            node: Arc::clone(&node),
+        }),
+        async {
             let _ = http_stopped.await;
-        })
-        .into_future(),
-    );
+        },
+    ));
     let holder = node_identity(&first.config.globals.cluster, started)
         .map_err(|e| Fatal::Config(e.to_string()))?;
     let buckets = Arc::new(Buckets::new(&first.config.globals));
     let joined = join(&buckets, &holder.node_id, &text, &first.config).await?;
     let config = joined.config;
-    if let Some(why) = unsupported(&config) {
-        return Err(Fatal::Config(why));
-    }
     logger.apply(&config.globals.log);
     tracing::info!(
         node = %holder.node_id,
@@ -231,7 +218,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         gossip.members(),
         c.lease_renewal_interval,
     ));
-    let _ = config_api.set(ConfigApi {
+    let _ = node.set(Node {
         coordinator: Arc::clone(&coordinator),
         members: gossip.members(),
         file: args.config.clone(),
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-api`

Expected: PASS, 4 tests.

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Run: `cargo test -p data-avalanche --lib`

Expected: PASS, 18 tests.

```text
test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1213 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-api/Cargo.toml \
  crates/data-avalanche-api/src/config.rs \
  crates/data-avalanche-api/src/lib.rs \
  crates/data-avalanche-api/src/server.rs \
  crates/data-avalanche-api/src/test_support.rs \
  crates/data-avalanche/Cargo.toml \
  crates/data-avalanche/src/http.rs \
  crates/data-avalanche/src/lib.rs \
  crates/data-avalanche/src/run.rs
git commit -m "feat(api): the data-avalanche-api crate — the listener with TLS, health, metrics, config reload and status, moved from the binary"
```

---

### Task 4: Auth and the role gate (`data-avalanche-api`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `Cargo.toml`
- Modify: `crates/data-avalanche-api/Cargo.toml`
- Create: `crates/data-avalanche-api/src/auth.rs`
- Modify: `crates/data-avalanche-api/src/config.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/server.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche-cluster/src/coordinator.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: `data_avalanche_config::{Api, ApiAuth, Credentials, Jwt, ApiOauth2, Role}`
  (M1); Task 3's `Api` and `router`.
- Produces:
  - `Auth::new(&config::Api) -> Auth`, `Auth::set(&self, &config::Api)` (a reload; the keys are
    kept unless the verification settings changed);
  - `Principal { name: String, role: Role }`, in each `/v1` request's extensions;
  - `FETCH_TIMEOUT` = 10 s, `REFETCH_AFTER` = 30 s;
  - `Route { method, path, role, summary, body, answers }` and `routes() -> Vec<Route>`, the one
    table that drives the router, the role gate and (Task 7) the OpenAPI document;
  - `Api.auth: Arc<Auth>`.
- The binary builds `Auth` from the file's `globals.api`, sets the adopted generation's at the
  join, and sets each later generation's from the coordinator's globals callback.

D-M9a.6. API_RBAC §3: `none` lets every caller in as `admin` (`anonymous`);
`basic` checks the one user with a constant-time compare (`subtle`) and makes it `admin`;
`jwt` and `oauth2` verify a bearer token against the issuer's JWKS (`jsonwebtoken`, asymmetric
algorithms only: an HS* token is refused) and check `iss`, `aud` when configured, and `exp`.
`oauth2` without a `jwks_url` reads the discovery document, whose `issuer` must be the configured
one. The keys are fetched with `reqwest` (rustls) under `FETCH_TIMEOUT`, cached, and fetched again
for an unknown `kid` at most every `REFETCH_AFTER`. The roles are read at the dotted
`roles_claim` path (an array or one string), mapped by `role_mapping` (identity when empty), and
the maximum wins; none mapped is 403. 401 carries `WWW-Authenticate: Basic` or `Bearer`
`realm="data-avalanche"`; keys that can't be fetched are 503.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 17a397c..1cc6e6b 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -918,6 +918,8 @@ dependencies = [
  "percent-encoding",
  "pin-project-lite",
  "serde_core",
+ "serde_json",
+ "serde_path_to_error",
  "sync_wrapper",
  "tokio",
  "tower",
@@ -1722,6 +1724,7 @@ name = "data-avalanche-api"
 version = "0.1.0"
 dependencies = [
  "axum",
+ "base64 0.22.1",
  "chrono",
  "data-avalanche-cluster",
  "data-avalanche-config",
@@ -1729,11 +1732,15 @@ dependencies = [
  "data-avalanche-state",
  "data-avalanche-storage",
  "data-avalanche-telemetry",
+ "jsonwebtoken",
  "object_store 0.14.2",
  "rcgen",
+ "reqwest 0.13.5",
  "rustls",
  "rustls-pki-types",
+ "serde",
  "serde_json",
+ "subtle",
  "tempfile",
  "thiserror 2.0.21",
  "tokio",
@@ -3742,6 +3749,22 @@ dependencies = [
  "wasm-bindgen",
 ]
 
+[[package]]
+name = "jsonwebtoken"
+version = "11.1.0"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "e75fe14a82d81e5f5af639997db37d8b96045938a7ac6ab18cdbe1c7467e05e1"
+dependencies = [
+ "aws-lc-rs",
+ "base64 0.22.1",
+ "getrandom 0.2.17",
+ "js-sys",
+ "serde",
+ "serde_json",
+ "signature",
+ "zeroize",
+]
+
 [[package]]
 name = "keyed_priority_queue"
 version = "0.4.2"
@@ -4876,6 +4899,15 @@ dependencies = [
  "rand_core 0.9.5",
 ]
 
+[[package]]
+name = "rand_core"
+version = "0.6.4"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "ec0be4795e2f6a28069bec0b5ff3e2ac9bafc99e6a9a7dc3547996c5c816922c"
+dependencies = [
+ "getrandom 0.2.17",
+]
+
 [[package]]
 name = "rand_core"
 version = "0.9.5"
@@ -5367,6 +5399,17 @@ dependencies = [
  "zmij",
 ]
 
+[[package]]
+name = "serde_path_to_error"
+version = "0.1.20"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "10a9ff822e371bb5403e391ecd83e182e0e77ba7f6fe0160b795797109d1b457"
+dependencies = [
+ "itoa",
+ "serde",
+ "serde_core",
+]
+
 [[package]]
 name = "serde_urlencoded"
 version = "0.7.1"
@@ -5437,6 +5480,15 @@ dependencies = [
  "libc",
 ]
 
+[[package]]
+name = "signature"
+version = "2.2.0"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "77549399552de45a898a580c1b41d445bf730df867cc44e6c0233bbc4b8329de"
+dependencies = [
+ "rand_core 0.6.4",
+]
+
 [[package]]
 name = "simd-adler32"
 version = "0.3.10"
```

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index e80a5cc..8bd7944 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -82,6 +82,12 @@ clap = { version = "4.6.7", features = ["derive"] }
 axum = { version = "0.8.9", default-features = false, features = ["http1", "tokio"] }
 # The management API (M9a, roadmap §2.1): TLS on the listener, on the workspace's rustls.
 tokio-rustls = { version = "0.26.5", default-features = false, features = ["aws_lc_rs", "tls12"] }
+# Its auth (config §6.3): JWT verification on aws-lc-rs, the JWKS and OIDC discovery fetched with
+# the reqwest object_store already builds, and basic auth's decode and constant-time compare.
+jsonwebtoken = { version = "=11.1.0", default-features = false, features = ["aws_lc_rs"] }
+reqwest = { version = "0.13.5", default-features = false, features = ["rustls"] }
+base64 = "0.22.1"
+subtle = "2.6.1"
 # The cluster (M8a, roadmap §2.1): the ring's hash (the anchor's `hash_algo: xxh3`); the SWIM
 # core the gossip drives (Q-M8-1, probe `prototypes/cluster-capability`) and its random source;
 # the host's addresses, for an unset `advertise_addr`.
```

Change `crates/data-avalanche-api/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-api/Cargo.toml b/crates/data-avalanche-api/Cargo.toml
index c6ceda3..7432066 100644
--- a/crates/data-avalanche-api/Cargo.toml
+++ b/crates/data-avalanche-api/Cargo.toml
@@ -11,18 +11,24 @@ workspace = true
 
 [dependencies]
 axum.workspace = true
+base64.workspace = true
 data-avalanche-cluster.workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-telemetry.workspace = true
+jsonwebtoken.workspace = true
+reqwest.workspace = true
 rustls.workspace = true
 rustls-pki-types.workspace = true
+serde.workspace = true
 serde_json.workspace = true
+subtle.workspace = true
 thiserror.workspace = true
 tokio = { workspace = true, features = ["net", "sync", "fs"] }
 tokio-rustls.workspace = true
 tracing.workspace = true
 
 [dev-dependencies]
+axum = { workspace = true, features = ["json"] }
 chrono.workspace = true
 data-avalanche-pipeline.workspace = true
 data-avalanche-state.workspace = true
```

Create `crates/data-avalanche-api/src/auth.rs`:

```rust
//! Who calls (config §6.3, API_RBAC §3–§4). `none`: every caller is `admin`. `basic`: the one
//! user is `admin`. `jwt` and `oauth2`: a bearer token signed by a key of the issuer's JWKS, its
//! roles read at `roles_claim` and mapped by `role_mapping` (identity when empty), the highest
//! one taken; a token with no role it maps is refused (403), not taken as `viewer`. `oauth2`
//! finds the JWKS through OIDC discovery when it has no `jwks_url`; tokens are verified locally,
//! never introspected (no client credentials are configured). The settings are R: [`Auth::set`]
//! takes a reload's.

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use axum::http::HeaderValue;
    use data_avalanche_config::{ApiOauth2, FingerprintKey, Secret};
    use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
    use serde_json::json;

    use super::*;
    use crate::test_support::{Issuer, config, sign_with};

    fn bearer(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        h
    }

    fn denied_401(r: Result<Principal, Denied>) -> String {
        match r {
            Err(Denied::Unauthenticated { why, .. }) => why,
            other => panic!("not a 401: {other:?}"),
        }
    }

    #[tokio::test]
    async fn without_auth_every_caller_is_admin() {
        let auth = Auth::new(&config().globals.api);
        let p = auth.verify(&HeaderMap::new()).await.unwrap();
        assert_eq!((p.name.as_str(), p.role), ("anonymous", Role::Admin));
    }

    #[tokio::test]
    async fn basic_lets_its_one_user_in_as_admin() {
        let mut api = config().globals.api;
        api.auth = ApiAuth::Basic;
        api.credentials = Some(Credentials::Inline {
            user: "ops".into(),
            password: Secret::new("s3cret", &FingerprintKey::from_bytes([1; 32])),
        });
        let auth = Auth::new(&api);
        let with = |user_pass: &str| {
            let mut h = HeaderMap::new();
            let v = format!("basic {}", STANDARD.encode(user_pass));
            h.insert(AUTHORIZATION, HeaderValue::from_str(&v).unwrap());
            h
        };
        let p = auth.verify(&with("ops:s3cret")).await.unwrap();
        assert_eq!((p.name.as_str(), p.role), ("ops", Role::Admin));
        for wrong in ["ops:nope", "nobody:s3cret", "ops", "ops:s3cret:"] {
            denied_401(auth.verify(&with(wrong)).await);
        }
        match auth.verify(&HeaderMap::new()).await {
            Err(Denied::Unauthenticated { challenge, .. }) => assert_eq!(challenge, BASIC),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_token_s_highest_mapped_role_is_the_caller_s() {
        let idp = Issuer::start().await;
        let auth = Auth::new(&idp.api("roles"));
        let role = |roles| {
            let token = idp.token(roles);
            let auth = &auth;
            async move { auth.verify(&bearer(&token)).await }
        };
        assert_eq!(role(json!(["viewer"])).await.unwrap().role, Role::Viewer);
        let p = role(json!(["viewer", "admin", "other"])).await.unwrap();
        assert_eq!((p.name.as_str(), p.role), ("alice", Role::Admin));
        assert_eq!(role(json!("operator")).await.unwrap().role, Role::Operator);
        assert!(matches!(
            role(json!(["cdc-admin"])).await,
            Err(Denied::Forbidden(_))
        ));
        assert!(matches!(role(json!([])).await, Err(Denied::Forbidden(_))));

        // Keycloak's realm roles, mapped: the three names themselves no longer count.
        let mut api = idp.api("realm_access.roles");
        api.role_mapping = BTreeMap::from([
            (Role::Viewer, vec!["readonly".into()]),
            (Role::Admin, vec!["cdc-admin".into()]),
        ]);
        let auth = Auth::new(&api);
        let realm = |roles| {
            let mut claims = idp.claims(json!(null));
            claims["realm_access"] = json!({ "roles": roles });
            bearer(&idp.sign("k1", &claims))
        };
        let p = auth.verify(&realm(json!(["readonly", "cdc-admin"]))).await;
        assert_eq!(p.unwrap().role, Role::Admin);
        assert_eq!(
            auth.verify(&realm(json!(["readonly"]))).await.unwrap().role,
            Role::Viewer
        );
        assert!(matches!(
            auth.verify(&realm(json!(["admin"]))).await,
            Err(Denied::Forbidden(_))
        ));
    }

    #[tokio::test]
    async fn a_token_that_doesnt_verify_is_refused() {
        let idp = Issuer::start().await;
        let auth = Auth::new(&idp.api("roles"));
        let now = jsonwebtoken::get_current_timestamp();
        let with = |f: &dyn Fn(&mut Value)| {
            let mut c = idp.claims(json!(["admin"]));
            f(&mut c);
            bearer(&idp.sign("k1", &c))
        };
        let expired = with(&|c| c["exp"] = json!(now - 120));
        assert!(denied_401(auth.verify(&expired).await).contains("Expired"));
        denied_401(
            auth.verify(&with(&|c| c["iss"] = json!("https://elsewhere")))
                .await,
        );
        denied_401(
            auth.verify(&with(&|c| c["aud"] = json!("another-api")))
                .await,
        );
        denied_401(
            auth.verify(&with(&|c| c.as_object_mut().unwrap().clear()))
                .await,
        );
        // Signed by a key the issuer never published, under a key id it does.
        let stranger = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        let forged = sign_with(&stranger, Some("k1"), &idp.claims(json!(["admin"])));
        denied_401(auth.verify(&bearer(&forged)).await);
        // A shared-secret token: nothing here holds a secret to check it with.
        let hs = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &idp.claims(json!(["admin"])),
            &jsonwebtoken::EncodingKey::from_secret(b"guess"),
        )
        .unwrap();
        assert!(denied_401(auth.verify(&bearer(&hs)).await).contains("shared-secret"));
        denied_401(auth.verify(&bearer("not.a.token")).await);
        match auth.verify(&HeaderMap::new()).await {
            Err(Denied::Unauthenticated { challenge, .. }) => assert_eq!(challenge, BEARER),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_rotated_key_is_fetched_when_a_token_names_it() {
        let idp = Issuer::start().await;
        let auth = Auth::with_refetch_after(&idp.api("roles"), Duration::ZERO);
        auth.verify(&bearer(&idp.token(json!(["viewer"]))))
            .await
            .unwrap();
        auth.verify(&bearer(&idp.token(json!(["viewer"]))))
            .await
            .unwrap();
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 1, "the JWKS is cached");
        idp.add_key("k2");
        let rotated = idp.sign("k2", &idp.claims(json!(["viewer"])));
        auth.verify(&bearer(&rotated)).await.unwrap();
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unknown_key_ids_fetch_at_most_once_a_refetch_interval() {
        let idp = Issuer::start().await;
        let auth = Auth::new(&idp.api("roles"));
        auth.verify(&bearer(&idp.token(json!(["viewer"]))))
            .await
            .unwrap();
        idp.add_key("k2");
        for _ in 0..5 {
            let made_up = idp.sign("k2", &idp.claims(json!(["viewer"])));
            denied_401(auth.verify(&bearer(&made_up)).await);
        }
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn oauth2_finds_the_keys_by_discovery_from_its_issuer() {
        let idp = Issuer::start().await;
        let mut api = config().globals.api;
        api.auth = ApiAuth::Oauth2;
        let oauth2 = |issuer: &str| ApiOauth2 {
            issuer: issuer.into(),
            audience: Some("cdc".into()),
            jwks_url: None,
            discovery_url: Some(format!("{}/.well-known/openid-configuration", idp.url)),
            roles_claim: "roles".into(),
        };
        api.oauth2 = Some(oauth2(&idp.url));
        let token = bearer(&idp.token(json!(["operator"])));
        let p = Auth::new(&api).verify(&token).await.unwrap();
        assert_eq!(p.role, Role::Operator);

        // A discovery document of another issuer is not this one's.
        api.oauth2 = Some(oauth2("https://idp.example.com"));
        match Auth::new(&api).verify(&token).await {
            Err(Denied::Unavailable(why)) => assert!(why.contains("not the configured"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn without_its_keys_no_token_verifies() {
        let idp = Issuer::start().await;
        let mut api = idp.api("roles");
        // A port nothing listens on.
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/jwks", closed.local_addr().unwrap());
        drop(closed);
        api.jwt.as_mut().unwrap().jwks_url = url;
        let r = Auth::new(&api)
            .verify(&bearer(&idp.token(json!(["admin"]))))
            .await;
        assert!(matches!(r, Err(Denied::Unavailable(_))), "{r:?}");
    }

    #[tokio::test]
    async fn a_reload_keeps_the_keys_unless_the_auth_settings_change() {
        let idp = Issuer::start().await;
        let api = idp.api("roles");
        let auth = Auth::new(&api);
        let token = bearer(&idp.token(json!(["viewer"])));
        auth.verify(&token).await.unwrap();
        let mut moved = api.clone();
        moved.address.port = 9_999;
        auth.set(&moved);
        auth.verify(&token).await.unwrap();
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 1);
        let mut other = api.clone();
        other.jwt.as_mut().unwrap().audience = Some("another-api".into());
        auth.set(&other);
        denied_401(auth.verify(&token).await);
        assert_eq!(idp.fetches.load(Ordering::SeqCst), 2);
        auth.set(&config().globals.api);
        assert_eq!(
            auth.verify(&HeaderMap::new()).await.unwrap().role,
            Role::Admin
        );
    }
}
```

Change `crates/data-avalanche-api/src/config.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index eb618db..9c42635 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -115,6 +115,7 @@ mod tests {
             metrics: Metrics::new(),
             ready: Arc::new(AtomicBool::new(true)),
             node,
+            auth: crate::test_support::open(),
         })
         .await;
 
```

Change `crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index c407900..9036204 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -4,6 +4,7 @@
 //! the pipeline's state documents, which the owner polls, and a status is read from them too
 //! (D-M9a.2).
 
+mod auth;
 mod config;
 mod server;
 #[cfg(test)]
@@ -86,7 +87,64 @@ async fn metrics(State(api): State<Api>) -> impl IntoResponse {
 #[cfg(test)]
 mod tests {
     use super::*;
-    use crate::test_support::{get, json_of, serve};
+    use crate::test_support::{Issuer, api, get, json_of, request, serve};
+
+    /// API_RBAC §2's matrix, for the routes this build serves.
+    #[test]
+    fn the_routes_take_the_roles_of_api_rbac() {
+        use Role::{Operator, Viewer};
+        let table: Vec<_> = routes()
+            .into_iter()
+            .map(|r| (r.method, r.path, r.role))
+            .collect();
+        assert_eq!(
+            table,
+            [
+                ("POST", "/v1/config/reload", Operator),
+                ("GET", "/v1/config/status", Viewer),
+            ]
+        );
+    }
+
+    /// Every `/v1` route: 401 without a token or with one that doesn't verify, 403 below its
+    /// role or with no role mapped, through at its role and above. Through means past the gate:
+    /// the node doesn't run its generation here, so the route itself answers 503.
+    #[tokio::test]
+    async fn every_route_turns_away_callers_below_its_role() {
+        let idp = Issuer::start().await;
+        let addr = serve(api(Arc::new(Auth::new(&idp.api("roles"))))).await;
+        let callers = [
+            ("viewer", Some(Role::Viewer)),
+            ("operator", Some(Role::Operator)),
+            ("admin", Some(Role::Admin)),
+            ("someone-else", None),
+        ];
+        for r in routes() {
+            let path = r.path.replace("{id}", "orders");
+            let (code, head, _) = request(addr, r.method, &path, &[], "").await;
+            assert_eq!(code, 401, "{} {path} without a token", r.method);
+            assert!(
+                head.to_ascii_lowercase()
+                    .contains(r#"www-authenticate: bearer realm="data-avalanche""#),
+                "{head}"
+            );
+            let bad = [("Authorization", "Bearer not.a.token")];
+            let (code, _, _) = request(addr, r.method, &path, &bad, "").await;
+            assert_eq!(code, 401, "{} {path} with a bad token", r.method);
+            for (claim, role) in callers {
+                let token = format!("Bearer {}", idp.token(serde_json::json!([claim])));
+                let auth = [("Authorization", token.as_str())];
+                let (code, _, body) = request(addr, r.method, &path, &auth, "").await;
+                if role.is_some_and(|role| role >= r.role) {
+                    assert_eq!(code, 503, "{} {path} as {claim}: {body}", r.method);
+                } else {
+                    assert_eq!(code, 403, "{} {path} as {claim}: {body}", r.method);
+                }
+            }
+        }
+        assert_eq!(get(addr, "/health/live").await.0, 200, "probes are open");
+        assert_eq!(get(addr, "/metrics").await.0, 200);
+    }
 
     #[tokio::test]
     async fn the_listener_answers_health_and_metrics() {
@@ -96,7 +154,7 @@ mod tests {
         let addr = serve(Api {
             metrics,
             ready: Arc::clone(&ready),
-            node: Arc::default(),
+            ..api(crate::test_support::open())
         })
         .await;
 
```

Change `crates/data-avalanche-api/src/server.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/server.rs b/crates/data-avalanche-api/src/server.rs
index a63cd82..86c7206 100644
--- a/crates/data-avalanche-api/src/server.rs
+++ b/crates/data-avalanche-api/src/server.rs
@@ -227,6 +227,7 @@ mod tests {
             metrics: Metrics::new(),
             ready: Arc::new(AtomicBool::new(true)),
             node: Arc::new(OnceLock::new()),
+            auth: crate::test_support::open(),
         };
         tokio::spawn(bound.serve(router(api), std::future::pending()));
 
```

Change `crates/data-avalanche-api/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index c3cdb48..e71f13b 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -1,27 +1,38 @@
 //! Test-only helpers shared across `data-avalanche-api`'s unit tests: a client, the listener on a
-//! free port, and a lone node.
+//! free port, a lone node, and an identity provider with keys made at run time.
 
 use std::net::SocketAddr;
 use std::path::PathBuf;
-use std::sync::Arc;
+use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
+use std::sync::{Arc, Mutex};
 use std::time::Duration;
 
+use axum::Json;
+use axum::extract::State;
+use axum::routing::get as get_route;
+use base64::Engine;
+use base64::engine::general_purpose::URL_SAFE_NO_PAD;
+
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Parts, Phase,
     ReconcilerSettings, Udp, WorkerFactory,
 };
-use data_avalanche_config::{FingerprintKey, HostPort, Pipeline, ProcessEnv, load_str};
+use data_avalanche_config::{
+    ApiAuth, FingerprintKey, HostPort, Jwt, Pipeline, ProcessEnv, ResolvedConfig, load_str,
+};
 use data_avalanche_pipeline::{Backoff, Tuning};
 use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases};
 use data_avalanche_storage::ObjectStoreVersioned;
 use data_avalanche_telemetry::Metrics;
+use jsonwebtoken::{Algorithm, EncodingKey, Header};
 use object_store::memory::InMemory;
-use serde_json::Value;
+use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
+use serde_json::{Value, json};
 use tokio::io::{AsyncReadExt, AsyncWriteExt};
 use tokio::sync::Semaphore;
 
-use crate::{Api, Node, bind, router};
+use crate::{Api, Auth, Node, bind, router};
 
 pub(crate) const CONFIG: &str = r#"
 config_version: 1
@@ -47,6 +58,27 @@ pipelines:
       delta: { checkpoint_interval: 20, timestamp_ntz_as: string, merge_memory_limit: 64MiB }
 "#;
 
+pub(crate) fn config() -> ResolvedConfig {
+    load_str(CONFIG, &ProcessEnv, &FingerprintKey::from_bytes([1; 32]))
+        .unwrap()
+        .config
+}
+
+/// `globals.api` of [`CONFIG`]: `auth: none`.
+pub(crate) fn open() -> Arc<Auth> {
+    Arc::new(Auth::new(&config().globals.api))
+}
+
+/// The routes' state before the node runs its generation: the `/v1` routes answer 503.
+pub(crate) fn api(auth: Arc<Auth>) -> Api {
+    Api {
+        metrics: Metrics::new(),
+        ready: Arc::new(AtomicBool::new(true)),
+        node: Arc::default(),
+        auth,
+    }
+}
+
 /// A plain HTTP/1.1 request: the status code, the headers, the body.
 pub(crate) async fn request(
     addr: SocketAddr,
@@ -195,3 +227,119 @@ pub(crate) async fn lone(file: PathBuf) -> Lone {
         _gossip: gossip,
     }
 }
+
+/// An identity provider: ES256 keys made at run time, its JWKS at `/jwks` and its OIDC discovery
+/// document at `/.well-known/openid-configuration`, on a loopback port.
+pub(crate) struct Issuer {
+    pub url: String,
+    keys: Arc<Mutex<Vec<(String, KeyPair)>>>,
+    /// JWKS fetches so far.
+    pub fetches: Arc<AtomicUsize>,
+}
+
+#[derive(Clone)]
+struct Served {
+    url: String,
+    keys: Arc<Mutex<Vec<(String, KeyPair)>>>,
+    fetches: Arc<AtomicUsize>,
+}
+
+fn jwk(kid: &str, key: &KeyPair) -> Value {
+    // An uncompressed P-256 point: 0x04, then x and y.
+    let raw = key.public_key_raw();
+    json!({
+        "kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": kid,
+        "x": URL_SAFE_NO_PAD.encode(&raw[1..33]),
+        "y": URL_SAFE_NO_PAD.encode(&raw[33..65]),
+    })
+}
+
+async fn jwks(State(s): State<Served>) -> Json<Value> {
+    s.fetches.fetch_add(1, Ordering::SeqCst);
+    let keys = s.keys.lock().unwrap();
+    Json(json!({ "keys": keys.iter().map(|(kid, k)| jwk(kid, k)).collect::<Vec<_>>() }))
+}
+
+async fn discovery(State(s): State<Served>) -> Json<Value> {
+    Json(json!({ "issuer": s.url, "jwks_uri": format!("{}/jwks", s.url) }))
+}
+
+impl Issuer {
+    /// Serves one key, `k1`.
+    pub async fn start() -> Self {
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let url = format!("http://{}", listener.local_addr().unwrap());
+        let keys = Arc::new(Mutex::new(vec![]));
+        let fetches = Arc::new(AtomicUsize::new(0));
+        let served = Served {
+            url: url.clone(),
+            keys: Arc::clone(&keys),
+            fetches: Arc::clone(&fetches),
+        };
+        let app = axum::Router::new()
+            .route("/jwks", get_route(jwks))
+            .route("/.well-known/openid-configuration", get_route(discovery))
+            .with_state(served);
+        tokio::spawn(async move { axum::serve(listener, app).await });
+        let issuer = Self { url, keys, fetches };
+        issuer.add_key("k1");
+        issuer
+    }
+
+    /// A key rotation: the JWKS serves one more key.
+    pub fn add_key(&self, kid: &str) {
+        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
+        self.keys.lock().unwrap().push((kid.to_owned(), key));
+    }
+
+    /// `globals.api.jwt` for this issuer, its tokens for audience `cdc`.
+    pub fn jwt(&self, roles_claim: &str) -> Jwt {
+        Jwt {
+            issuer: self.url.clone(),
+            audience: Some("cdc".into()),
+            jwks_url: format!("{}/jwks", self.url),
+            roles_claim: roles_claim.into(),
+        }
+    }
+
+    /// `globals.api` with `auth: jwt` for this issuer.
+    pub fn api(&self, roles_claim: &str) -> data_avalanche_config::Api {
+        let mut api = config().globals.api;
+        api.auth = ApiAuth::Jwt;
+        api.jwt = Some(self.jwt(roles_claim));
+        api
+    }
+
+    /// Claims this issuer's verifier takes, for `sub` alice, expiring in ten minutes.
+    pub fn claims(&self, roles: Value) -> Value {
+        let now = jsonwebtoken::get_current_timestamp();
+        json!({ "iss": self.url, "aud": "cdc", "sub": "alice", "exp": now + 600, "roles": roles })
+    }
+
+    /// `claims` signed with key `kid`.
+    pub fn sign(&self, kid: &str, claims: &Value) -> String {
+        let keys = self.keys.lock().unwrap();
+        let (_, key) = keys
+            .iter()
+            .find(|(k, _)| k == kid)
+            .expect("a key of this issuer");
+        sign_with(key, Some(kid), claims)
+    }
+
+    /// A token with `roles`, signed with `k1`.
+    pub fn token(&self, roles: Value) -> String {
+        self.sign("k1", &self.claims(roles))
+    }
+}
+
+/// `claims` signed with `key` (ES256).
+pub(crate) fn sign_with(key: &KeyPair, kid: Option<&str>, claims: &Value) -> String {
+    let mut header = Header::new(Algorithm::ES256);
+    header.kid = kid.map(str::to_owned);
+    jsonwebtoken::encode(
+        &header,
+        claims,
+        &EncodingKey::from_ec_der(&key.serialize_der()),
+    )
+    .unwrap()
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-api`

Expected: FAIL to compile: `Auth`, `Principal`, `routes` and the test issuer's items don't exist.

```text
error[E0432]: unresolved import `crate::Auth`
  --> crates/data-avalanche-api/src/test_support.rs:35:18
error[E0432]: unresolved import `Role`
error[E0425]: cannot find type `HeaderMap` in this scope
error[E0425]: cannot find value `AUTHORIZATION` in this scope
error[E0425]: cannot find type `Principal` in this scope
error[E0425]: cannot find type `Denied` in this scope
error[E0425]: cannot find value `STANDARD` in this scope
error[E0425]: cannot find value `BASIC` in this scope
error[E0433]: cannot find type `BTreeMap` in this scope
error[E0425]: cannot find type `Value` in this scope
error[E0425]: cannot find value `BEARER` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-api/src/auth.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/auth.rs b/crates/data-avalanche-api/src/auth.rs
index ff707db..e4af245 100644
--- a/crates/data-avalanche-api/src/auth.rs
+++ b/crates/data-avalanche-api/src/auth.rs
@@ -6,6 +6,417 @@
 //! never introspected (no client credentials are configured). The settings are R: [`Auth::set`]
 //! takes a reload's.
 
+use std::collections::BTreeMap;
+use std::sync::{Arc, PoisonError, RwLock};
+use std::time::{Duration, Instant};
+
+use axum::http::HeaderMap;
+use axum::http::header::AUTHORIZATION;
+use base64::Engine;
+use base64::engine::general_purpose::STANDARD;
+use data_avalanche_config::{Api as ApiConfig, ApiAuth, Credentials, Role};
+use jsonwebtoken::jwk::JwkSet;
+use jsonwebtoken::{AlgorithmFamily, DecodingKey, Validation, decode, decode_header};
+use serde_json::Value;
+use subtle::ConstantTimeEq;
+use tokio::sync::Mutex;
+
+/// A JWKS fetch, or a discovery, takes at most this long.
+pub const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
+/// A token signed by a key the cached JWKS lacks fetches the JWKS again, at most this often
+/// (a key rotation; a flood of tokens with made-up key ids fetches nothing more).
+pub const REFETCH_AFTER: Duration = Duration::from_secs(30);
+
+/// An authenticated caller.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct Principal {
+    /// The basic user, the token's `sub`, or `anonymous`.
+    pub name: String,
+    pub role: Role,
+}
+
+/// Why a caller is turned away.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) enum Denied {
+    /// 401: no credentials, or ones that don't verify. `challenge` is `WWW-Authenticate`.
+    Unauthenticated {
+        challenge: &'static str,
+        why: String,
+    },
+    /// 403: verified, but no role this endpoint takes.
+    Forbidden(String),
+    /// 503: the issuer's keys can't be fetched, so no token can be verified.
+    Unavailable(String),
+}
+
+const BASIC: &str = r#"Basic realm="data-avalanche""#;
+const BEARER: &str = r#"Bearer realm="data-avalanche""#;
+
+/// The settings that decide who calls: `auth` and what it reads.
+#[derive(Debug, Clone, PartialEq)]
+struct Settings {
+    auth: ApiAuth,
+    credentials: Option<Credentials>,
+    jwt: Option<data_avalanche_config::Jwt>,
+    oauth2: Option<data_avalanche_config::ApiOauth2>,
+    role_mapping: BTreeMap<Role, Vec<String>>,
+}
+
+impl Settings {
+    fn of(api: &ApiConfig) -> Self {
+        Self {
+            auth: api.auth,
+            credentials: api.credentials.clone(),
+            jwt: api.jwt.clone(),
+            oauth2: api.oauth2.clone(),
+            role_mapping: api.role_mapping.clone(),
+        }
+    }
+}
+
+#[derive(Debug)]
+enum Mode {
+    Open,
+    Basic(Credentials),
+    Token(Verifier),
+}
+
+#[derive(Debug)]
+struct Current {
+    settings: Settings,
+    mode: Arc<Mode>,
+}
+
+/// The node's authentication, swapped by a reload.
+#[derive(Debug)]
+pub struct Auth {
+    current: RwLock<Current>,
+    refetch_after: Duration,
+}
+
+impl Auth {
+    pub fn new(api: &ApiConfig) -> Self {
+        Self::with_refetch_after(api, REFETCH_AFTER)
+    }
+
+    pub(crate) fn with_refetch_after(api: &ApiConfig, refetch_after: Duration) -> Self {
+        let settings = Settings::of(api);
+        Self {
+            current: RwLock::new(Current {
+                mode: Arc::new(mode(&settings, refetch_after)),
+                settings,
+            }),
+            refetch_after,
+        }
+    }
+
+    /// Takes a reload's settings. Unchanged ones keep the cached JWKS.
+    pub fn set(&self, api: &ApiConfig) {
+        let settings = Settings::of(api);
+        let mut current = self.current.write().unwrap_or_else(PoisonError::into_inner);
+        if current.settings != settings {
+            current.mode = Arc::new(mode(&settings, self.refetch_after));
+            current.settings = settings;
+        }
+    }
+
+    /// The caller of a request with these headers.
+    pub(crate) async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
+        let mode = Arc::clone(
+            &self
+                .current
+                .read()
+                .unwrap_or_else(PoisonError::into_inner)
+                .mode,
+        );
+        match &*mode {
+            Mode::Open => Ok(Principal {
+                name: "anonymous".into(),
+                role: Role::Admin,
+            }),
+            Mode::Basic(credentials) => basic(credentials, headers),
+            Mode::Token(verifier) => verifier.verify(headers).await,
+        }
+    }
+}
+
+fn mode(s: &Settings, refetch_after: Duration) -> Mode {
+    let token = |issuer: &str, audience: &Option<String>, roles_claim: &str, keys| {
+        Mode::Token(Verifier {
+            issuer: issuer.to_owned(),
+            audience: audience.clone(),
+            roles_claim: roles_claim.to_owned(),
+            mapping: s.role_mapping.clone(),
+            keys: Keys::new(keys, refetch_after),
+        })
+    };
+    match (s.auth, &s.credentials, &s.jwt, &s.oauth2) {
+        (ApiAuth::Basic, Some(c), _, _) => Mode::Basic(c.clone()),
+        (ApiAuth::Jwt, _, Some(j), _) => token(
+            &j.issuer,
+            &j.audience,
+            &j.roles_claim,
+            KeySource::Jwks(j.jwks_url.clone()),
+        ),
+        (ApiAuth::Oauth2, _, _, Some(o)) => token(
+            &o.issuer,
+            &o.audience,
+            &o.roles_claim,
+            match (&o.jwks_url, &o.discovery_url) {
+                (Some(url), _) => KeySource::Jwks(url.clone()),
+                // V-051 requires one of them.
+                (None, url) => KeySource::Discovery {
+                    url: url.clone().unwrap_or_default(),
+                    issuer: o.issuer.clone(),
+                },
+            },
+        ),
+        // V-049…V-051 require the settings each mode reads; none means open.
+        _ => Mode::Open,
+    }
+}
+
+fn unauthenticated(challenge: &'static str, why: impl Into<String>) -> Denied {
+    Denied::Unauthenticated {
+        challenge,
+        why: why.into(),
+    }
+}
+
+/// The value of `Authorization: <scheme> <value>`, the scheme matched without case.
+fn credential<'h>(headers: &'h HeaderMap, scheme: &str) -> Option<&'h str> {
+    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
+    let (given, rest) = value.split_once(' ')?;
+    given.eq_ignore_ascii_case(scheme).then(|| rest.trim())
+}
+
+fn basic(c: &Credentials, headers: &HeaderMap) -> Result<Principal, Denied> {
+    let given = credential(headers, "Basic")
+        .ok_or_else(|| unauthenticated(BASIC, "basic credentials are required"))?;
+    let decoded = STANDARD
+        .decode(given)
+        .ok()
+        .and_then(|b| String::from_utf8(b).ok())
+        .ok_or_else(|| unauthenticated(BASIC, "the basic credentials don't decode"))?;
+    let (user, password) = decoded
+        .split_once(':')
+        .ok_or_else(|| unauthenticated(BASIC, "the basic credentials don't decode"))?;
+    // Both compared in full, whichever differs.
+    let user_ok = user.as_bytes().ct_eq(c.user().as_bytes());
+    let password_ok = password.as_bytes().ct_eq(c.password().expose().as_bytes());
+    if bool::from(user_ok & password_ok) {
+        Ok(Principal {
+            name: user.to_owned(),
+            role: Role::Admin,
+        })
+    } else {
+        Err(unauthenticated(BASIC, "wrong user or password"))
+    }
+}
+
+#[derive(Debug)]
+struct Verifier {
+    issuer: String,
+    audience: Option<String>,
+    roles_claim: String,
+    mapping: BTreeMap<Role, Vec<String>>,
+    keys: Keys,
+}
+
+impl Verifier {
+    async fn verify(&self, headers: &HeaderMap) -> Result<Principal, Denied> {
+        let token = credential(headers, "Bearer")
+            .ok_or_else(|| unauthenticated(BEARER, "a bearer token is required"))?;
+        let header = decode_header(token)
+            .map_err(|e| unauthenticated(BEARER, format!("not a token: {e}")))?;
+        if AlgorithmFamily::Hmac == header.alg.family() {
+            return Err(unauthenticated(
+                BEARER,
+                format!(
+                    "{:?} is a shared-secret algorithm: tokens are verified by the issuer's public keys",
+                    header.alg
+                ),
+            ));
+        }
+        let key = self.keys.key(header.kid.as_deref()).await?;
+        let mut validation = Validation::new(header.alg);
+        validation.set_issuer(&[&self.issuer]);
+        match &self.audience {
+            Some(aud) => validation.set_audience(&[aud]),
+            None => validation.validate_aud = false,
+        }
+        let claims = decode::<Value>(token, &key, &validation)
+            .map_err(|e| unauthenticated(BEARER, format!("the token doesn't verify: {e}")))?
+            .claims;
+        let name = claims
+            .get("sub")
+            .and_then(Value::as_str)
+            .unwrap_or("unknown")
+            .to_owned();
+        let role = roles(&claims, &self.roles_claim)
+            .iter()
+            .filter_map(|r| map(&self.mapping, r))
+            .max()
+            .ok_or_else(|| {
+                Denied::Forbidden(format!(
+                    "the token's {} name no role this node maps",
+                    self.roles_claim
+                ))
+            })?;
+        Ok(Principal { name, role })
+    }
+}
+
+/// The strings at a dotted path (`realm_access.roles`): a list of them, or one.
+fn roles(claims: &Value, path: &str) -> Vec<String> {
+    let mut at = claims;
+    for part in path.split('.') {
+        match at.get(part) {
+            Some(next) => at = next,
+            None => return vec![],
+        }
+    }
+    match at {
+        Value::String(s) => vec![s.clone()],
+        Value::Array(items) => items
+            .iter()
+            .filter_map(|v| v.as_str().map(str::to_owned))
+            .collect(),
+        _ => vec![],
+    }
+}
+
+/// An IdP role's local role: `role_mapping`, or the three names themselves without one.
+fn map(mapping: &BTreeMap<Role, Vec<String>>, idp: &str) -> Option<Role> {
+    if mapping.is_empty() {
+        return idp.parse().ok();
+    }
+    mapping
+        .iter()
+        .filter(|(_, names)| names.iter().any(|n| n == idp))
+        .map(|(role, _)| *role)
+        .max()
+}
+
+#[derive(Debug, Clone)]
+enum KeySource {
+    Jwks(String),
+    /// OIDC discovery: the document's `jwks_uri`, once its `issuer` is found to be the
+    /// configured one (OIDC Discovery §4.3).
+    Discovery {
+        url: String,
+        issuer: String,
+    },
+}
+
+#[derive(Debug)]
+struct Keys {
+    source: KeySource,
+    http: reqwest::Client,
+    cache: Mutex<Cache>,
+    refetch_after: Duration,
+}
+
+#[derive(Debug, Default)]
+struct Cache {
+    set: Option<JwkSet>,
+    fetched: Option<Instant>,
+}
+
+impl Keys {
+    fn new(source: KeySource, refetch_after: Duration) -> Self {
+        Self {
+            source,
+            refetch_after,
+            http: reqwest::Client::builder()
+                .timeout(FETCH_TIMEOUT)
+                .build()
+                .expect("the HTTP client builds"),
+            cache: Mutex::new(Cache::default()),
+        }
+    }
+
+    /// The key a token names (`kid`; without one, the set's only key), fetching the JWKS when
+    /// it isn't cached, or lacks the key and was fetched [`REFETCH_AFTER`] ago or earlier.
+    async fn key(&self, kid: Option<&str>) -> Result<DecodingKey, Denied> {
+        let mut cache = self.cache.lock().await;
+        if let Some(key) = cache.set.as_ref().and_then(|s| find(s, kid)) {
+            return key;
+        }
+        if cache
+            .fetched
+            .is_none_or(|at| at.elapsed() >= self.refetch_after)
+        {
+            cache.fetched = Some(Instant::now());
+            match self.fetch().await {
+                Ok(set) => cache.set = Some(set),
+                Err(why) => {
+                    tracing::warn!("the API's token keys: {why}");
+                    if cache.set.is_none() {
+                        return Err(Denied::Unavailable(why));
+                    }
+                }
+            }
+        }
+        cache
+            .set
+            .as_ref()
+            .and_then(|s| find(s, kid))
+            .unwrap_or_else(|| {
+                Err(unauthenticated(
+                    BEARER,
+                    format!("no key {kid:?} in the issuer's JWKS"),
+                ))
+            })
+    }
+
+    async fn fetch(&self) -> Result<JwkSet, String> {
+        let url = match &self.source {
+            KeySource::Jwks(url) => url.clone(),
+            KeySource::Discovery { url, issuer } => {
+                let doc: Value = self.get(url).await?;
+                let found = doc.get("issuer").and_then(Value::as_str);
+                if found != Some(issuer.as_str()) {
+                    return Err(format!(
+                        "{url}: the issuer is {found:?}, not the configured {issuer:?}"
+                    ));
+                }
+                doc.get("jwks_uri")
+                    .and_then(Value::as_str)
+                    .ok_or_else(|| format!("{url}: no jwks_uri"))?
+                    .to_owned()
+            }
+        };
+        self.get(&url).await
+    }
+
+    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, String> {
+        let body = self
+            .http
+            .get(url)
+            .send()
+            .await
+            .and_then(reqwest::Response::error_for_status)
+            .map_err(|e| format!("{url}: {e}"))?
+            .bytes()
+            .await
+            .map_err(|e| format!("{url}: {e}"))?;
+        serde_json::from_slice(&body).map_err(|e| format!("{url}: {e}"))
+    }
+}
+
+fn find(set: &JwkSet, kid: Option<&str>) -> Option<Result<DecodingKey, Denied>> {
+    let jwk = match kid {
+        Some(kid) => set.find(kid)?,
+        None if set.keys.len() == 1 => &set.keys[0],
+        None => return None,
+    };
+    Some(
+        DecodingKey::from_jwk(jwk)
+            .map_err(|e| unauthenticated(BEARER, format!("the issuer's key: {e}"))),
+    )
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::atomic::Ordering;
```

Change `crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 9036204..5255ae6 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -3,6 +3,10 @@
 //! `/v1` endpoints. A request may land on any node: what a pipeline's owner must do goes through
 //! the pipeline's state documents, which the owner polls, and a status is read from them too
 //! (D-M9a.2).
+//!
+//! Every `/v1` route takes the least role in API_RBAC §2 ([`routes`]): no credentials, or ones
+//! that don't verify, are 401 with a `WWW-Authenticate` challenge; a caller below the route's
+//! role is 403.
 
 mod auth;
 mod config;
@@ -15,17 +19,22 @@ use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::{Arc, OnceLock};
 
 use axum::Router;
-use axum::extract::State;
-use axum::http::{StatusCode, header};
+use axum::extract::{Request, State};
+use axum::http::{HeaderValue, StatusCode, header};
+use axum::middleware::{Next, from_fn_with_state};
 use axum::response::{IntoResponse, Response};
-use axum::routing::{get, post};
+use axum::routing::{MethodRouter, get, post};
 use data_avalanche_cluster::{ConfigCoordinator, Members};
+use data_avalanche_config::Role;
 use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
 use serde_json::{Value, json};
 use tokio::sync::watch;
 
+pub use auth::{Auth, FETCH_TIMEOUT, Principal, REFETCH_AFTER};
 pub use server::{Bound, HANDSHAKE_TIMEOUT, ServeError, bind};
 
+use crate::auth::Denied;
+
 /// What the routes read.
 #[derive(Debug, Clone)]
 pub struct Api {
@@ -34,6 +43,7 @@ pub struct Api {
     pub ready: Arc<AtomicBool>,
     /// Set once the node runs its config generation; the `/v1` routes answer 503 before.
     pub node: Arc<OnceLock<Node>>,
+    pub auth: Arc<Auth>,
 }
 
 /// What the `/v1` routes act on, once the node runs its config generation.
@@ -45,14 +55,82 @@ pub struct Node {
     pub file: PathBuf,
 }
 
+/// A `/v1` route and the least role that may call it (API_RBAC §2).
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub struct Route {
+    pub method: &'static str,
+    /// As the router matches it: `{id}` for a path parameter.
+    pub path: &'static str,
+    pub role: Role,
+}
+
+const fn route(method: &'static str, path: &'static str, role: Role) -> Route {
+    Route { method, path, role }
+}
+
+/// Every `/v1` route, with its handler.
+fn v1() -> Vec<(Route, MethodRouter<Api>)> {
+    vec![
+        (
+            route("POST", "/v1/config/reload", Role::Operator),
+            post(config::reload),
+        ),
+        (
+            route("GET", "/v1/config/status", Role::Viewer),
+            get(config::status),
+        ),
+    ]
+}
+
+/// Every `/v1` route and its role.
+pub fn routes() -> Vec<Route> {
+    v1().into_iter().map(|(r, _)| r).collect()
+}
+
 pub fn router(api: Api) -> Router {
-    Router::new()
+    let mut router = Router::new()
         .route("/health/live", get(|| async { "live\n" }))
         .route("/health/ready", get(ready))
-        .route("/metrics", get(metrics))
-        .route("/v1/config/reload", post(config::reload))
-        .route("/v1/config/status", get(config::status))
-        .with_state(api)
+        .route("/metrics", get(metrics));
+    for (r, handler) in v1() {
+        let gate = (Arc::clone(&api.auth), r.role);
+        router = router.route(
+            r.path,
+            handler.route_layer(from_fn_with_state(gate, authorize)),
+        );
+    }
+    router.with_state(api)
+}
+
+/// Lets a caller through to a route that takes `role` or less, with its [`Principal`] in the
+/// request's extensions.
+async fn authorize(
+    State((auth, role)): State<(Arc<Auth>, Role)>,
+    mut req: Request,
+    next: Next,
+) -> Response {
+    match auth.verify(req.headers()).await {
+        Ok(p) if p.role >= role => {
+            req.extensions_mut().insert(p);
+            next.run(req).await
+        }
+        Ok(p) => body(
+            StatusCode::FORBIDDEN,
+            &json!({ "error": format!("this needs the {role} role; {} is {}", p.name, p.role) }),
+        ),
+        Err(Denied::Forbidden(why)) => body(StatusCode::FORBIDDEN, &json!({ "error": why })),
+        Err(Denied::Unauthenticated { challenge, why }) => {
+            let mut r = body(StatusCode::UNAUTHORIZED, &json!({ "error": why }));
+            r.headers_mut().insert(
+                header::WWW_AUTHENTICATE,
+                HeaderValue::from_static(challenge),
+            );
+            r
+        }
+        Err(Denied::Unavailable(why)) => {
+            body(StatusCode::SERVICE_UNAVAILABLE, &json!({ "error": why }))
+        }
+    }
 }
 
 /// A JSON answer.
```

Change `crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 211eac8..98deea2 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -50,7 +50,8 @@ pub struct CoordinatorDeps {
     pub reloader: Reloader,
     pub advertiser: Advertiser,
     pub metrics: Metrics,
-    /// What a generation's globals change outside the cluster: the binary's log filter.
+    /// What a generation's globals change outside the cluster: the binary's log filter and the
+    /// API's auth.
     pub globals: Arc<dyn Fn(&Globals) + Send + Sync>,
 }
 
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index dc86589..52bed0a 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -5,7 +5,7 @@ use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::{Arc, OnceLock};
 use std::time::{Duration, SystemTime};
 
-use data_avalanche_api::{Api, Node, ServeError, bind, router};
+use data_avalanche_api::{Api, Auth, Node, ServeError, bind, router};
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
@@ -110,6 +110,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     let ready = Arc::new(AtomicBool::new(false));
     let node = Arc::new(OnceLock::new());
     let api = &first.config.globals.api;
+    let auth = Arc::new(Auth::new(api));
     let bound = bind(&api.address, api.tls.as_ref())
         .await
         .map_err(|e| match e {
@@ -122,6 +123,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             metrics: metrics.clone(),
             ready: Arc::clone(&ready),
             node: Arc::clone(&node),
+            auth: Arc::clone(&auth),
         }),
         async {
             let _ = http_stopped.await;
@@ -133,6 +135,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     let joined = join(&buckets, &holder.node_id, &text, &first.config).await?;
     let config = joined.config;
     logger.apply(&config.globals.log);
+    auth.set(&config.globals.api);
     tracing::info!(
         node = %holder.node_id,
         incarnation = holder.incarnation,
@@ -199,7 +202,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     );
     // Each newer config generation, this node's reloads included (Clustering §9).
     let docs = Arc::clone(&buckets);
-    let log = Arc::clone(&logger);
+    let (log, reloaded_auth) = (Arc::clone(&logger), Arc::clone(&auth));
     let coordinator = Arc::new(ConfigCoordinator::start(
         config.clone(),
         joined.generation,
@@ -213,7 +216,10 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             reloader: reconciler.reloader(),
             advertiser: gossip.advertiser(),
             metrics: metrics.clone(),
-            globals: Arc::new(move |g| log.apply(&g.log)),
+            globals: Arc::new(move |g| {
+                log.apply(&g.log);
+                reloaded_auth.set(&g.api);
+            }),
         },
         gossip.members(),
         c.lease_renewal_interval,
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-api`

Expected: PASS, 15 tests.

```text
test result: ok. 15 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Run: `cargo test -p data-avalanche --lib`

Expected: PASS, 18 tests.

```text
test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1224 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-api/Cargo.toml \
  crates/data-avalanche-api/src/auth.rs \
  crates/data-avalanche-api/src/config.rs \
  crates/data-avalanche-api/src/lib.rs \
  crates/data-avalanche-api/src/server.rs \
  crates/data-avalanche-api/src/test_support.rs \
  crates/data-avalanche-cluster/src/coordinator.rs \
  crates/data-avalanche/src/run.rs
git commit -m "feat(api): auth none, basic, jwt and oauth2 with role_mapping; the route table and its role gate; the binary swaps the auth on a reload"
```

---

### Task 5: The pipeline endpoints (`data-avalanche-api`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `crates/data-avalanche-api/Cargo.toml`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Create: `crates/data-avalanche-api/src/pipelines.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: Task 1's documents and `transition`; `Leases::observe` (M2); the coordinator's
  `DocsFn` (M8b); Task 4's `Principal`.
- Produces: `Node.docs: Arc<DocsFn>` and `Node.leases: Leases`; the routes
  - `GET /v1/pipelines`, `GET /v1/pipelines/{id}` (viewer);
  - `POST /v1/pipelines/{id}/{pause,stop,resume}` (operator): 202
    `{desired_state, previous}`, 200 `{desired_state, unchanged: true}`, 409
    `{error, desired_state}`;
  - `POST /v1/pipelines/{id}/{restart,flush}` (operator): 202 `{request}`, 409, and a flush of a
    paused pipeline 200 `{desired_state: "paused", unchanged: true}`;
  - 404 for an id the node's generation doesn't run; 503 when the state can't be read.

D-M9a.2, D-M9a.3, D-M9a.4. Any node answers. A status comes from storage: the lease
(holder, epoch, renewal, released), `status.json` when its epoch and holder are the live lease's
(otherwise `starting`, or `unowned` without a live lease), the position (`position.json`, or the
slot's LSN unless it is the creation intent), and `halts.json`'s tables. A restart is refused
(409) when the desired state is paused or stopped, or no node holds the lease; a flush when the
pipeline is stopped or unheld.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 1cc6e6b..fdd332b 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1728,6 +1728,7 @@ dependencies = [
  "chrono",
  "data-avalanche-cluster",
  "data-avalanche-config",
+ "data-avalanche-core",
  "data-avalanche-pipeline",
  "data-avalanche-state",
  "data-avalanche-storage",
@@ -1746,6 +1747,7 @@ dependencies = [
  "tokio",
  "tokio-rustls",
  "tracing",
+ "uuid",
 ]
 
 [[package]]
```

Change `crates/data-avalanche-api/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-api/Cargo.toml b/crates/data-avalanche-api/Cargo.toml
index 7432066..2f7a6b4 100644
--- a/crates/data-avalanche-api/Cargo.toml
+++ b/crates/data-avalanche-api/Cargo.toml
@@ -12,8 +12,11 @@ workspace = true
 [dependencies]
 axum.workspace = true
 base64.workspace = true
+chrono.workspace = true
 data-avalanche-cluster.workspace = true
 data-avalanche-config.workspace = true
+data-avalanche-core.workspace = true
+data-avalanche-state.workspace = true
 data-avalanche-telemetry.workspace = true
 jsonwebtoken.workspace = true
 reqwest.workspace = true
@@ -26,12 +29,11 @@ thiserror.workspace = true
 tokio = { workspace = true, features = ["net", "sync", "fs"] }
 tokio-rustls.workspace = true
 tracing.workspace = true
+uuid.workspace = true
 
 [dev-dependencies]
 axum = { workspace = true, features = ["json"] }
-chrono.workspace = true
 data-avalanche-pipeline.workspace = true
-data-avalanche-state.workspace = true
 data-avalanche-storage.workspace = true
 object_store.workspace = true
 rcgen.workspace = true
```

Change `crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 5255ae6..c4387c6 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -10,6 +10,7 @@
 
 mod auth;
 mod config;
+mod pipelines;
 mod server;
 #[cfg(test)]
 mod test_support;
@@ -180,6 +181,13 @@ mod tests {
             [
                 ("POST", "/v1/config/reload", Operator),
                 ("GET", "/v1/config/status", Viewer),
+                ("GET", "/v1/pipelines", Viewer),
+                ("GET", "/v1/pipelines/{id}", Viewer),
+                ("POST", "/v1/pipelines/{id}/pause", Operator),
+                ("POST", "/v1/pipelines/{id}/stop", Operator),
+                ("POST", "/v1/pipelines/{id}/resume", Operator),
+                ("POST", "/v1/pipelines/{id}/restart", Operator),
+                ("POST", "/v1/pipelines/{id}/flush", Operator),
             ]
         );
     }
```

Create `crates/data-avalanche-api/src/pipelines.rs`:

```rust
//! `/v1/pipelines` (Pipeline_Lifecycle §3, §6): the status any node reads from the pipeline's
//! state documents and its lease, and the lifecycle actions. Pause, stop and resume set the
//! persisted desired state; restart and flush are requests the owner takes at its next poll.
//! Both answer 202 once recorded: the owner acts within a `lease_renewal_interval`.

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use data_avalanche_core::{HaltReason, TableRef};
    use data_avalanche_state::{Acquired, Holder, SlotDoc, TableHalt};
    use tempfile::TempDir;

    use super::*;
    use crate::test_support::{CONFIG, Lone, api, json_of, lone, open, serve};

    /// A lone node's API: the pipeline `orders` of [`CONFIG`], its state in memory.
    async fn served() -> (SocketAddr, Lone, PipelineDocs, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file).await;
        let config = lone.node.coordinator.config().await;
        let docs = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        (serve(a).await, lone, docs, dir)
    }

    async fn held_by(lone: &Lone, node: &str) -> data_avalanche_state::Versioned<LeaseDoc> {
        let holder = Holder {
            node_id: node.into(),
            incarnation: 1,
        };
        match lone
            .leases
            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
            .await
            .unwrap()
        {
            Acquired::Mine(l) => l,
            Acquired::HeldBy(l) => panic!("held by {l:?}"),
        }
    }

    fn written(node: &str, epoch: u64, actual: ActualState) -> StatusDoc {
        StatusDoc {
            holder_node_id: node.into(),
            owner_epoch: epoch,
            actual,
            error: None,
            code: None,
            degraded: None,
            written_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn a_pipeline_s_status_is_read_from_its_state_and_its_lease() {
        let (addr, lone, docs, _dir) = served().await;
        let (code, v) = json_of(addr, "GET", "/v1/pipelines/nope").await;
        assert_eq!(code, 404, "{v}");

        let (code, v) = json_of(addr, "GET", "/v1/pipelines/orders").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(v["desired_state"], "running", "never started: it runs");
        assert_eq!(v["actual_state"], "unowned");
        assert_eq!(v["holder"], Value::Null);
        assert_eq!(v["position"], Value::Null);
        assert_eq!(v["halted_tables"], json!([]));
        assert_eq!(
            v["source"],
            json!({ "type": "postgres", "host": "db.example.com", "port": 5432, "db": "shop" })
        );
        assert_eq!(
            v["sink"],
            json!({ "type": "delta", "bucket": "lake", "prefix": "shop" })
        );

        let lease = held_by(&lone, "node-b").await;
        let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
        assert_eq!(v["actual_state"], "starting", "held, nothing reported yet");
        assert_eq!(v["holder"]["node"], "node-b");
        assert_eq!(v["holder"]["owner_epoch"], 1);

        docs.save_status(&written("node-b", 1, ActualState::Running))
            .await
            .unwrap();
        let lsn = "0/16B3748".parse().unwrap();
        docs.save_slot(&SlotDoc {
            slot_name: "orders_slot".into(),
            confirmed_flush_lsn: lsn,
        })
        .await
        .unwrap();
        docs.record_halt(&TableHalt {
            table: TableRef::new("public", "orders"),
            reason: HaltReason::TableDropped,
            at: SourcePosition::Pg(lsn),
        })
        .await
        .unwrap();
        let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
        assert_eq!(v["actual_state"], "running");
        assert_eq!(v["position"], "pg:0/16B3748");
        assert_eq!(v["halted_tables"][0]["table"], "public.orders");
        assert_eq!(v["halted_tables"][0]["code"], "TABLE_DROPPED");
        assert_eq!(v["halted_tables"][0]["at"], "pg:0/16B3748");
        assert!(
            v["halted_tables"][0]["detail"]
                .as_str()
                .unwrap()
                .contains("dropped"),
            "{v}"
        );

        let failed = StatusDoc {
            error: Some("no privilege".into()),
            code: Some("BINLOG_SOURCE_CHANGED".into()),
            ..written("node-b", 1, ActualState::Error)
        };
        docs.save_status(&failed).await.unwrap();
        let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
        assert_eq!(
            (&v["actual_state"], &v["error"], &v["code"]),
            (
                &json!("error"),
                &json!("no privilege"),
                &json!("BINLOG_SOURCE_CHANGED")
            )
        );

        // Another node's status, or the holder's own of an earlier term, is not this term's.
        for (node, epoch) in [("node-a", 1), ("node-b", 0)] {
            docs.save_status(&written(node, epoch, ActualState::Stopped))
                .await
                .unwrap();
            let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
            assert_eq!(v["actual_state"], "starting", "{node} at epoch {epoch}");
        }

        lone.leases.release(&lease, Utc::now()).await.unwrap();
        let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
        assert_eq!(v["actual_state"], "unowned");
        assert_eq!(v["holder"]["released"], true);
    }

    #[tokio::test]
    async fn the_list_has_every_pipeline_s_summary() {
        let (addr, lone, docs, _dir) = served().await;
        held_by(&lone, "node-b").await;
        docs.save_status(&written("node-b", 1, ActualState::Paused))
            .await
            .unwrap();
        let (code, v) = json_of(addr, "GET", "/v1/pipelines").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(
            v,
            json!({ "pipelines": [{
                "id": "orders", "name": "orders", "desired_state": "running",
                "actual_state": "paused", "holder": "node-b",
            }] })
        );
    }

    #[tokio::test]
    async fn pause_stop_and_resume_move_the_desired_state() {
        let (addr, _lone, docs, _dir) = served().await;
        let post = |action: &str| {
            let path = format!("/v1/pipelines/orders/{action}");
            async move { json_of(addr, "POST", &path).await }
        };
        assert_eq!(
            post("pause").await,
            (
                202,
                json!({ "desired_state": "paused", "previous": "running" })
            )
        );
        assert_eq!(
            post("pause").await,
            (200, json!({ "desired_state": "paused", "unchanged": true }))
        );
        assert_eq!(post("stop").await.0, 202);
        let (code, v) = post("pause").await;
        assert_eq!(code, 409, "{v}");
        assert_eq!(v["desired_state"], "stopped");
        assert_eq!(
            post("resume").await,
            (
                202,
                json!({ "desired_state": "running", "previous": "stopped" })
            )
        );
        let state = docs.state().await.unwrap().unwrap().doc;
        assert_eq!(state.desired_state, DesiredState::Running);
        assert_eq!(state.sink_type, "delta", "opened for the pipeline");
        assert_eq!(
            json_of(addr, "POST", "/v1/pipelines/nope/pause").await.0,
            404
        );
    }

    #[tokio::test]
    async fn restart_and_flush_are_asked_of_the_lease_holder() {
        let (addr, lone, docs, _dir) = served().await;
        let post = |action: &str| {
            let path = format!("/v1/pipelines/orders/{action}");
            async move { json_of(addr, "POST", &path).await }
        };
        let (code, v) = post("restart").await;
        assert_eq!(code, 409, "{v}");
        assert!(v["error"].as_str().unwrap().contains("lease"), "{v}");

        held_by(&lone, "node-b").await;
        let (code, v) = post("restart").await;
        assert_eq!(code, 202, "{v}");
        let asked = docs.requests().await.unwrap();
        assert_eq!(
            Some(&v["request"]),
            asked.restart.map(|r| json!(r.id)).as_ref()
        );
        let (code, v) = post("flush").await;
        assert_eq!(code, 202, "{v}");
        assert_eq!(
            json!(docs.requests().await.unwrap().flush.unwrap().id),
            v["request"]
        );

        assert_eq!(post("pause").await.0, 202);
        let (code, v) = post("restart").await;
        assert_eq!((code, &v["desired_state"]), (409, &json!("paused")), "{v}");
        assert_eq!(
            post("flush").await,
            (200, json!({ "desired_state": "paused", "unchanged": true }))
        );
        assert_eq!(post("stop").await.0, 202);
        assert_eq!(post("flush").await.0, 409);
        assert_eq!(post("restart").await.0, 409);
    }
}
```

Change `crates/data-avalanche-api/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index e71f13b..cd18963 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -22,7 +22,7 @@ use data_avalanche_config::{
     ApiAuth, FingerprintKey, HostPort, Jwt, Pipeline, ProcessEnv, ResolvedConfig, load_str,
 };
 use data_avalanche_pipeline::{Backoff, Tuning};
-use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases};
+use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases, PipelineDocs};
 use data_avalanche_storage::ObjectStoreVersioned;
 use data_avalanche_telemetry::Metrics;
 use jsonwebtoken::{Algorithm, EncodingKey, Header};
@@ -144,13 +144,17 @@ impl WorkerFactory for NoWorkers {
 /// loopback port, its reloads reaching a reconciler without pipelines.
 pub(crate) struct Lone {
     pub node: Node,
+    /// The leases the node reads: a test takes them as another node would.
+    pub leases: Leases,
     pub _reconciler: ClusterReconciler,
     pub _gossip: Gossip,
 }
 
 pub(crate) async fn lone(file: PathBuf) -> Lone {
-    let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
+    let mem = Arc::new(InMemory::new());
+    let store = ObjectStoreVersioned::new(mem.clone(), "globals").unwrap();
     let docs = DocStore::new(Arc::new(store));
+    let leases = Leases::new(docs.clone());
     let generations = ConfigGenerations::new(docs.clone());
     let (first, _) = generations
         .bootstrap_or_adopt(CONFIG, "node-a", chrono::Utc::now())
@@ -180,7 +184,7 @@ pub(crate) async fn lone(file: PathBuf) -> Lone {
     let metrics = Metrics::new();
     let c = &config.globals.cluster;
     let deps = ControllerDeps {
-        leases: Leases::new(docs),
+        leases: leases.clone(),
         holder: Holder {
             node_id: "node-a".into(),
             incarnation: 1,
@@ -222,7 +226,15 @@ pub(crate) async fn lone(file: PathBuf) -> Lone {
             coordinator: Arc::new(coordinator),
             members: gossip.members(),
             file,
+            docs: Arc::new(move |p| {
+                let root = format!("lake/{}", p.sink.prefix);
+                let store =
+                    ObjectStoreVersioned::new(mem.clone(), &root).map_err(|e| e.to_string())?;
+                Ok(PipelineDocs::new(DocStore::new(Arc::new(store))))
+            }),
+            leases: leases.clone(),
         },
+        leases,
         _reconciler: reconciler,
         _gossip: gossip,
     }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-api -- pipelines::`

Expected: FAIL to compile: `Node.docs`, `Node.leases` and the handlers don't exist.

```text
error: cannot find macro `json` in this scope
   --> crates/data-avalanche-api/src/pipelines.rs:236:19
error[E0425]: cannot find type `PipelineDocs` in this scope
error[E0425]: cannot find type `LeaseDoc` in this scope
error[E0425]: cannot find type `ActualState` in this scope
error[E0425]: cannot find type `StatusDoc` in this scope
error[E0422]: cannot find struct, variant or union type `StatusDoc` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index c4387c6..17dcfe5 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -25,8 +25,9 @@ use axum::http::{HeaderValue, StatusCode, header};
 use axum::middleware::{Next, from_fn_with_state};
 use axum::response::{IntoResponse, Response};
 use axum::routing::{MethodRouter, get, post};
-use data_avalanche_cluster::{ConfigCoordinator, Members};
+use data_avalanche_cluster::{ConfigCoordinator, DocsFn, Members};
 use data_avalanche_config::Role;
+use data_avalanche_state::Leases;
 use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
 use serde_json::{Value, json};
 use tokio::sync::watch;
@@ -48,12 +49,25 @@ pub struct Api {
 }
 
 /// What the `/v1` routes act on, once the node runs its config generation.
-#[derive(Debug, Clone)]
+#[derive(Clone)]
 pub struct Node {
     pub coordinator: Arc<ConfigCoordinator>,
     pub members: watch::Receiver<Members>,
     /// The node's file, read again by each reload (config §12.1).
     pub file: PathBuf,
+    /// A pipeline's state documents.
+    pub docs: Arc<DocsFn>,
+    /// The leases in `globals.state`: who holds each pipeline.
+    pub leases: Leases,
+}
+
+impl std::fmt::Debug for Node {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("Node")
+            .field("file", &self.file)
+            .field("leases", &self.leases)
+            .finish_non_exhaustive()
+    }
 }
 
 /// A `/v1` route and the least role that may call it (API_RBAC §2).
@@ -80,6 +94,34 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             route("GET", "/v1/config/status", Role::Viewer),
             get(config::status),
         ),
+        (
+            route("GET", "/v1/pipelines", Role::Viewer),
+            get(pipelines::list),
+        ),
+        (
+            route("GET", "/v1/pipelines/{id}", Role::Viewer),
+            get(pipelines::get),
+        ),
+        (
+            route("POST", "/v1/pipelines/{id}/pause", Role::Operator),
+            post(pipelines::pause),
+        ),
+        (
+            route("POST", "/v1/pipelines/{id}/stop", Role::Operator),
+            post(pipelines::stop),
+        ),
+        (
+            route("POST", "/v1/pipelines/{id}/resume", Role::Operator),
+            post(pipelines::resume),
+        ),
+        (
+            route("POST", "/v1/pipelines/{id}/restart", Role::Operator),
+            post(pipelines::restart),
+        ),
+        (
+            route("POST", "/v1/pipelines/{id}/flush", Role::Operator),
+            post(pipelines::flush),
+        ),
     ]
 }
 
@@ -144,6 +186,28 @@ pub(crate) fn body(code: StatusCode, v: &Value) -> Response {
         .into_response()
 }
 
+/// A request that can't be served: its code and `{"error": …}`.
+#[derive(Debug)]
+pub(crate) struct Failure {
+    code: StatusCode,
+    error: String,
+}
+
+impl Failure {
+    pub(crate) fn new(code: StatusCode, error: impl Into<String>) -> Self {
+        Self {
+            code,
+            error: error.into(),
+        }
+    }
+}
+
+impl From<Failure> for Response {
+    fn from(f: Failure) -> Self {
+        body(f.code, &json!({ "error": f.error }))
+    }
+}
+
 pub(crate) fn starting() -> Response {
     body(
         StatusCode::SERVICE_UNAVAILABLE,
```

Change `crates/data-avalanche-api/src/pipelines.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index a4aa1ed..978fa32 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -3,6 +3,345 @@
 //! persisted desired state; restart and flush are requests the owner takes at its next poll.
 //! Both answer 202 once recorded: the owner acts within a `lease_renewal_interval`.
 
+use axum::Extension;
+use axum::extract::{Path, State};
+use axum::http::StatusCode;
+use axum::response::Response;
+use chrono::Utc;
+use data_avalanche_config::Pipeline;
+use data_avalanche_core::SourcePosition;
+use data_avalanche_state::{
+    Action, ActualState, DesiredState, LeaseDoc, PipelineDocs, Request, StateError, StatusDoc,
+    Transition,
+};
+use serde_json::{Value, json};
+
+use crate::{Api, Failure, Node, Principal, body, starting};
+
+/// What a status read failed on: 503, the store is the answer.
+fn unavailable(e: &StateError) -> Response {
+    body(
+        StatusCode::SERVICE_UNAVAILABLE,
+        &json!({ "error": format!("the pipeline's state: {e}") }),
+    )
+}
+
+/// The pipeline with this id in the generation the node runs, and its state documents.
+async fn find(node: &Node, id: &str) -> Result<(Pipeline, PipelineDocs), Failure> {
+    let config = node.coordinator.config().await;
+    let Some(p) = config.pipelines.values().find(|p| p.id == id).cloned() else {
+        return Err(Failure::new(
+            StatusCode::NOT_FOUND,
+            format!("no pipeline {id:?} in the config generation this node runs"),
+        ));
+    };
+    let docs = (node.docs)(&p).map_err(|e| {
+        Failure::new(
+            StatusCode::INTERNAL_SERVER_ERROR,
+            format!("the pipeline's state location: {e}"),
+        )
+    })?;
+    Ok((p, docs))
+}
+
+fn desired_name(d: DesiredState) -> &'static str {
+    match d {
+        DesiredState::Running => "running",
+        DesiredState::Paused => "paused",
+        DesiredState::Stopped => "stopped",
+    }
+}
+
+fn actual_name(a: ActualState) -> &'static str {
+    match a {
+        ActualState::Starting => "starting",
+        ActualState::Running => "running",
+        ActualState::Paused => "paused",
+        ActualState::Stopped => "stopped",
+        ActualState::Dumped => "dumped",
+        ActualState::Retrying => "retrying",
+        ActualState::Error => "error",
+    }
+}
+
+/// The actual state: `unowned` when no node holds the lease, the holder's `status.json` when
+/// it wrote one in this lease term, else `starting`.
+fn actual<'s>(
+    lease: Option<&LeaseDoc>,
+    status: Option<&'s StatusDoc>,
+) -> (&'static str, Option<&'s StatusDoc>) {
+    let Some(lease) = lease.filter(|l| !l.released) else {
+        return ("unowned", None);
+    };
+    match status
+        .filter(|s| s.owner_epoch == lease.owner_epoch && s.holder_node_id == lease.holder_node_id)
+    {
+        Some(s) => (actual_name(s.actual), Some(s)),
+        None => ("starting", None),
+    }
+}
+
+/// The documents a status is read from.
+struct Read {
+    desired: DesiredState,
+    lease: Option<LeaseDoc>,
+    status: Option<StatusDoc>,
+}
+
+async fn read(node: &Node, p: &Pipeline, docs: &PipelineDocs) -> Result<Read, StateError> {
+    let (state, lease, status) =
+        tokio::join!(docs.state(), node.leases.observe(&p.id), docs.status());
+    Ok(Read {
+        desired: state?.map_or(DesiredState::Running, |v| v.doc.desired_state),
+        lease: lease?.map(|v| v.doc),
+        status: status?.map(|v| v.doc),
+    })
+}
+
+fn summary(p: &Pipeline, r: &Read) -> Value {
+    let (actual, _) = actual(r.lease.as_ref(), r.status.as_ref());
+    json!({
+        "id": p.id,
+        "name": p.name,
+        "desired_state": desired_name(r.desired),
+        "actual_state": actual,
+        "holder": r.lease.as_ref().filter(|l| !l.released).map(|l| l.holder_node_id.clone()),
+    })
+}
+
+/// Every pipeline of the generation the node runs, by id.
+pub(crate) async fn list(State(api): State<Api>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let config = node.coordinator.config().await;
+    let mut pipelines: Vec<&Pipeline> = config.pipelines.values().collect();
+    pipelines.sort_by(|a, b| a.id.cmp(&b.id));
+    let mut out = vec![];
+    for p in pipelines {
+        let docs = match (node.docs)(p) {
+            Ok(docs) => docs,
+            Err(e) => {
+                return body(
+                    StatusCode::INTERNAL_SERVER_ERROR,
+                    &json!({ "error": format!("{}: the state location: {e}", p.id) }),
+                );
+            }
+        };
+        match read(node, p, &docs).await {
+            Ok(r) => out.push(summary(p, &r)),
+            Err(e) => return unavailable(&e),
+        }
+    }
+    body(StatusCode::OK, &json!({ "pipelines": out }))
+}
+
+/// One pipeline: its desired and actual states, the holder of its lease, its position, and
+/// its halted tables with their reasons (System-Overview's endpoint table).
+pub(crate) async fn get(State(api): State<Api>, Path(id): Path<String>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (p, docs) = match find(node, &id).await {
+        Ok(found) => found,
+        Err(f) => return f.into(),
+    };
+    let read_all = async {
+        let r = read(node, &p, &docs).await?;
+        let (slot, position, halts) = tokio::join!(docs.slot(), docs.position(), docs.halts());
+        let position = match (slot?, position?) {
+            (_, Some(v)) => Some(v.doc.position),
+            (Some(v), None) if !v.doc.is_intent() => {
+                Some(SourcePosition::Pg(v.doc.confirmed_flush_lsn))
+            }
+            _ => None,
+        };
+        Ok::<_, StateError>((r, position, halts?))
+    };
+    let (r, position, halts) = match read_all.await {
+        Ok(all) => all,
+        Err(e) => return unavailable(&e),
+    };
+    let (actual, status) = actual(r.lease.as_ref(), r.status.as_ref());
+    let mut v = summary(&p, &r);
+    v["source"] = json!({
+        "type": p.source.kind.as_str(),
+        "host": p.source.host,
+        "port": p.source.port,
+        "db": p.source.db,
+    });
+    v["sink"] =
+        json!({ "type": p.sink.kind.as_str(), "bucket": p.sink.bucket, "prefix": p.sink.prefix });
+    v["actual_state"] = json!(actual);
+    v["error"] = json!(status.and_then(|s| s.error.clone()));
+    v["code"] = json!(status.and_then(|s| s.code.clone()));
+    v["degraded"] = json!(status.and_then(|s| s.degraded.clone()));
+    v["status_written_at"] = json!(status.map(|s| s.written_at));
+    v["holder"] = match &r.lease {
+        Some(l) => json!({
+            "node": l.holder_node_id,
+            "owner_epoch": l.owner_epoch,
+            "renewed_at": l.renewed_at,
+            "released": l.released,
+        }),
+        None => Value::Null,
+    };
+    v["position"] = json!(position.map(|p| p.to_string()));
+    v["halted_tables"] = json!(
+        halts
+            .tables
+            .iter()
+            .map(|h| json!({
+                "table": h.table.to_string(),
+                "code": h.reason.code(),
+                "detail": h.reason.to_string(),
+                "at": h.at.to_string(),
+            }))
+            .collect::<Vec<_>>()
+    );
+    body(StatusCode::OK, &v)
+}
+
+/// Pause, stop or resume: 202 when the desired state changed, 200 when it was that already,
+/// 409 for a pause of a stopped pipeline.
+async fn transition(api: &Api, id: &str, to: DesiredState, by: &Principal) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (p, docs) = match find(node, id).await {
+        Ok(found) => found,
+        Err(f) => return f.into(),
+    };
+    let opened = docs.open(&p.id, p.sink.kind.as_str()).await;
+    let done = match opened {
+        Ok(_) => docs.transition(to).await,
+        Err(e) => Err(e),
+    };
+    let to_name = desired_name(to);
+    match done {
+        Ok(Transition::Changed { from }) => {
+            tracing::info!(pipeline = %p.name, by = %by.name, from = desired_name(from), "desired state set to {to_name} through the API");
+            body(
+                StatusCode::ACCEPTED,
+                &json!({ "desired_state": to_name, "previous": desired_name(from) }),
+            )
+        }
+        Ok(Transition::Unchanged) => body(
+            StatusCode::OK,
+            &json!({ "desired_state": to_name, "unchanged": true }),
+        ),
+        Ok(Transition::Refused { current }) => body(
+            StatusCode::CONFLICT,
+            &json!({
+                "error": format!("a {} pipeline can't be set {to_name}; resume it first", desired_name(current)),
+                "desired_state": desired_name(current),
+            }),
+        ),
+        Err(e @ (StateError::PipelineIdMismatch { .. } | StateError::SinkTypeMismatch { .. })) => {
+            body(StatusCode::CONFLICT, &json!({ "error": e.to_string() }))
+        }
+        Err(e) => unavailable(&e),
+    }
+}
+
+pub(crate) async fn pause(
+    State(api): State<Api>,
+    Path(id): Path<String>,
+    Extension(by): Extension<Principal>,
+) -> Response {
+    transition(&api, &id, DesiredState::Paused, &by).await
+}
+
+pub(crate) async fn stop(
+    State(api): State<Api>,
+    Path(id): Path<String>,
+    Extension(by): Extension<Principal>,
+) -> Response {
+    transition(&api, &id, DesiredState::Stopped, &by).await
+}
+
+pub(crate) async fn resume(
+    State(api): State<Api>,
+    Path(id): Path<String>,
+    Extension(by): Extension<Principal>,
+) -> Response {
+    transition(&api, &id, DesiredState::Running, &by).await
+}
+
+/// Restart or flush, asked of the lease holder. Refused (409) with no holder, and when the
+/// desired state rules it out: a restart of a paused or stopped pipeline, a flush of a stopped
+/// one. A paused pipeline is flushed already: 200.
+async fn ask(api: &Api, id: &str, action: Action, by: &Principal) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (p, docs) = match find(node, id).await {
+        Ok(found) => found,
+        Err(f) => return f.into(),
+    };
+    let r = match read(node, &p, &docs).await {
+        Ok(r) => r,
+        Err(e) => return unavailable(&e),
+    };
+    let what = match action {
+        Action::Restart => "restart",
+        Action::Flush => "flush",
+    };
+    let conflict = |why: String| {
+        body(
+            StatusCode::CONFLICT,
+            &json!({ "error": why, "desired_state": desired_name(r.desired) }),
+        )
+    };
+    match (action, r.desired) {
+        (_, DesiredState::Stopped) | (Action::Restart, DesiredState::Paused) => {
+            return conflict(format!(
+                "a {} pipeline can't {what}; resume it first",
+                desired_name(r.desired)
+            ));
+        }
+        (Action::Flush, DesiredState::Paused) => {
+            return body(
+                StatusCode::OK,
+                &json!({ "desired_state": "paused", "unchanged": true }),
+            );
+        }
+        (_, DesiredState::Running) => {}
+    }
+    if r.lease.as_ref().is_none_or(|l| l.released) {
+        return conflict(format!(
+            "no node holds the pipeline's lease, so none can {what} it"
+        ));
+    }
+    let request = Request {
+        id: uuid::Uuid::new_v4().to_string(),
+        at: Utc::now(),
+    };
+    match docs.request(action, request.clone()).await {
+        Ok(()) => {
+            tracing::info!(pipeline = %p.name, by = %by.name, request = %request.id, "{what} asked through the API");
+            body(StatusCode::ACCEPTED, &json!({ "request": request.id }))
+        }
+        Err(e) => unavailable(&e),
+    }
+}
+
+pub(crate) async fn restart(
+    State(api): State<Api>,
+    Path(id): Path<String>,
+    Extension(by): Extension<Principal>,
+) -> Response {
+    ask(&api, &id, Action::Restart, &by).await
+}
+
+pub(crate) async fn flush(
+    State(api): State<Api>,
+    Path(id): Path<String>,
+    Extension(by): Extension<Principal>,
+) -> Response {
+    ask(&api, &id, Action::Flush, &by).await
+}
+
 #[cfg(test)]
 mod tests {
     use std::net::SocketAddr;
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 52bed0a..a06312c 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -8,7 +8,7 @@ use std::time::{Duration, SystemTime};
 use data_avalanche_api::{Api, Auth, Node, ServeError, bind, router};
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
-    Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
+    DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
     ReconcilerSettings, Seeds, Udp, advertise, discover, interfaces, join as join_gossip, leave,
     node_identity,
 };
@@ -202,6 +202,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     );
     // Each newer config generation, this node's reloads included (Clustering §9).
     let docs = Arc::clone(&buckets);
+    let docs: Arc<DocsFn> = Arc::new(move |p| docs.pipeline_docs(p).map_err(|e| e.to_string()));
     let (log, reloaded_auth) = (Arc::clone(&logger), Arc::clone(&auth));
     let coordinator = Arc::new(ConfigCoordinator::start(
         config.clone(),
@@ -211,7 +212,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             env: Arc::new(ProcessEnv),
             key: FingerprintKey::from_bytes(joined.fingerprint_key),
             node_id: deps.holder.node_id.clone(),
-            docs: Arc::new(move |p| docs.pipeline_docs(p).map_err(|e| e.to_string())),
+            docs: Arc::clone(&docs),
             factory: Arc::clone(&deps.factory),
             reloader: reconciler.reloader(),
             advertiser: gossip.advertiser(),
@@ -228,6 +229,8 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         coordinator: Arc::clone(&coordinator),
         members: gossip.members(),
         file: args.config.clone(),
+        docs,
+        leases: deps.leases.clone(),
     });
     ready.store(true, Ordering::SeqCst);
     shutdown_signal().await;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-api`

Expected: PASS, 19 tests.

```text
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Run: `cargo test -p data-avalanche --lib`

Expected: PASS, 18 tests.

```text
test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1228 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche-api/Cargo.toml \
  crates/data-avalanche-api/src/lib.rs \
  crates/data-avalanche-api/src/pipelines.rs \
  crates/data-avalanche-api/src/test_support.rs \
  crates/data-avalanche/src/run.rs
git commit -m "feat(api): the pipelines list and status from storage; pause, stop and resume; the restart and flush requests"
```

---

### Task 6: Config validate, cluster status, leave and shutdown (`data-avalanche-api`, `data-avalanche-cluster`, the binary)

**Files:**
- Create: `crates/data-avalanche-api/src/cluster.rs`
- Modify: `crates/data-avalanche-api/src/config.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche-cluster/src/coordinator.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: `ConfigCoordinator` (M8b), `Members` (M8a), Task 4's `Principal`.
- Produces:
  - `ConfigCoordinator::check(&self, text: &str) -> Result<Checked, ReloadError>` and
    `Checked { generation, warnings: Report, changes: ConfigDiff }`, exported: the reload's checks
    without the publish;
  - `POST /v1/config/validate` (operator): the body's YAML, or the node's file when the body is
    empty; 200 `{valid, against_generation, warnings, changes: {globals, pipelines}}`, 422 with
    the report, 409 when rejected… as the reload answers;
  - `GET /v1/cluster/status` (viewer): `{node, members: [{node, addr, phase, suspect,
    config_generation, build}], pipelines: [{id, holder, owner_epoch}]}`;
  - `Stop { Run, Leave, Shutdown }` (ordered), `Control::new() -> (Control,
    watch::Receiver<Stop>)`, `Node.control`; `POST /v1/cluster/leave` and
    `POST /v1/admin/shutdown` (admin): 202, then 409 once asked.
- The binary's run loop waits on SIGTERM/SIGINT or the control: a leave runs the M8a leave once
  and stays up (not ready); a shutdown, or a signal, leaves if it hasn't and exits 0.

D-M9a.7, D-M9a.8. The validate answers what a reload of the text would change and never
the resolved config: API_RBAC §2 makes it operator because the resolved config holds secrets,
and the answer has no need for them. Table-pattern changes count as restarts here, since the
catalog isn't read. After a leave the node owns nothing and answers its API; a restart of the
process joins again.

- [ ] **Step 1: Write the failing tests**

Create `crates/data-avalanche-api/src/cluster.rs`:

```rust
//! `/v1/cluster` and `/v1/admin`: the members this node sees and who holds each pipeline, and
//! the node's own leave and shutdown (D-M9a.8). A leave hands every pipeline off and leaves the
//! gossip, and the process stays up: not ready, owning nothing, still answering the API, until
//! it is stopped; a restart joins again. A shutdown is what SIGTERM does: the leave, then the
//! process ends.

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use data_avalanche_state::Holder;

    use super::*;
    use crate::test_support::{CONFIG, api, json_of, lone, open, serve};

    #[test]
    fn a_leave_then_a_shutdown_is_asked_once_each() {
        let (control, watched) = Control::new();
        assert!(control.ask(Stop::Leave));
        assert!(!control.ask(Stop::Leave));
        assert_eq!(*watched.borrow(), Stop::Leave);
        assert!(control.ask(Stop::Shutdown));
        assert!(!control.ask(Stop::Leave), "a shutdown includes the leave");
        assert!(!control.ask(Stop::Shutdown));
        assert_eq!(*watched.borrow(), Stop::Shutdown);
    }

    #[tokio::test]
    async fn the_cluster_status_names_the_members_and_the_holders() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file).await;
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        let addr = serve(a).await;

        let (code, v) = json_of(addr, "GET", "/v1/cluster/status").await;
        assert_eq!(code, 200, "{v}");
        assert_eq!(v["node"], "node-a");
        assert_eq!(v["members"][0]["node"], "node-a");
        assert_eq!(v["members"][0]["phase"], "active");
        assert_eq!(v["members"][0]["config_generation"], 1);
        assert_eq!(
            v["pipelines"],
            json!([{ "id": "orders", "holder": null, "owner_epoch": null }])
        );
        let holder = Holder {
            node_id: "node-b".into(),
            incarnation: 1,
        };
        lone.leases
            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
            .await
            .unwrap();
        let v = json_of(addr, "GET", "/v1/cluster/status").await.1;
        assert_eq!(
            v["pipelines"],
            json!([{ "id": "orders", "holder": "node-b", "owner_epoch": 1 }])
        );
    }

    #[tokio::test]
    async fn a_leave_and_a_shutdown_are_asked_of_the_binary() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.yaml");
        std::fs::write(&file, CONFIG).unwrap();
        let lone = lone(file).await;
        let watched = lone.stops.clone();
        let a = api(open());
        a.node.set(lone.node.clone()).unwrap();
        let addr = serve(a).await;

        assert_eq!(
            json_of(addr, "POST", "/v1/cluster/leave").await,
            (202, json!({ "leaving": true }))
        );
        assert_eq!(*watched.borrow(), Stop::Leave);
        assert_eq!(json_of(addr, "POST", "/v1/cluster/leave").await.0, 409);
        assert_eq!(
            json_of(addr, "POST", "/v1/admin/shutdown").await,
            (202, json!({ "shutting_down": true }))
        );
        assert_eq!(*watched.borrow(), Stop::Shutdown);
        assert_eq!(json_of(addr, "POST", "/v1/admin/shutdown").await.0, 409);
    }
}
```

Change `crates/data-avalanche-api/src/config.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index 9c42635..bbfd1f5 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -97,7 +97,54 @@ mod tests {
     use serde_json::Value;
 
     use super::*;
-    use crate::test_support::{CONFIG, json_of, lone, serve};
+    use crate::test_support::{CONFIG, json_of, lone, request, serve};
+
+    #[tokio::test]
+    async fn a_check_says_what_a_reload_would_change() {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, CONFIG).unwrap();
+        let lone = lone(file.clone()).await;
+        let node = Arc::new(OnceLock::new());
+        node.set(lone.node.clone()).unwrap();
+        let addr = serve(Api {
+            metrics: Metrics::new(),
+            ready: Arc::new(AtomicBool::new(true)),
+            node,
+            auth: crate::test_support::open(),
+        })
+        .await;
+        let check = |text: String| async move {
+            let (code, _, body) = request(addr, "POST", "/v1/config/validate", &[], &text).await;
+            (code, serde_json::from_str::<Value>(&body).unwrap())
+        };
+
+        let (code, v) = check(String::new()).await;
+        assert_eq!(code, 200, "{v}");
+        assert_eq!(
+            v,
+            json!({ "valid": true, "against_generation": 1, "warnings": [],
+                    "changes": { "globals": [], "pipelines": [] } }),
+            "the node's file, unchanged"
+        );
+        let (code, v) = check(CONFIG.replace("prefix: shop", "prefix: shop2")).await;
+        assert_eq!(code, 200, "{v}");
+        assert_eq!(v["changes"]["pipelines"][0]["change"], "modified", "{v}");
+        assert_eq!(v["changes"]["pipelines"][0]["restart"], true, "{v}");
+        assert!(
+            !v.to_string().contains("\"password\""),
+            "no resolved config: {v}"
+        );
+
+        let (code, v) =
+            check(CONFIG.replace("lease_duration: 30s", "tokens: 64, lease_duration: 30s")).await;
+        assert_eq!(code, 422, "{v}");
+        assert_eq!(v["report"]["errors"][0]["rule"], "V-061", "{v}");
+        let (code, v) = check("pipelines: [".into()).await;
+        assert_eq!((code, &v["report"]["valid"]), (422, &json!(false)), "{v}");
+        let generation = lone.node.coordinator.latest().await.unwrap();
+        assert_eq!(generation, Some(1), "nothing published");
+    }
 
     /// The reload and its status (Clustering §9.2): 200 for the file the cluster runs, 202 and
     /// the next generation for a change, 422 with the report for a file that is invalid or
```

Change `crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 17dcfe5..147bc45 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -9,6 +9,7 @@
 //! role is 403.
 
 mod auth;
+mod cluster;
 mod config;
 mod pipelines;
 mod server;
@@ -235,7 +236,7 @@ mod tests {
     /// API_RBAC §2's matrix, for the routes this build serves.
     #[test]
     fn the_routes_take_the_roles_of_api_rbac() {
-        use Role::{Operator, Viewer};
+        use Role::{Admin, Operator, Viewer};
         let table: Vec<_> = routes()
             .into_iter()
             .map(|r| (r.method, r.path, r.role))
@@ -243,8 +244,10 @@ mod tests {
         assert_eq!(
             table,
             [
+                ("POST", "/v1/config/validate", Operator),
                 ("POST", "/v1/config/reload", Operator),
                 ("GET", "/v1/config/status", Viewer),
+                ("GET", "/v1/cluster/status", Viewer),
                 ("GET", "/v1/pipelines", Viewer),
                 ("GET", "/v1/pipelines/{id}", Viewer),
                 ("POST", "/v1/pipelines/{id}/pause", Operator),
@@ -252,6 +255,8 @@ mod tests {
                 ("POST", "/v1/pipelines/{id}/resume", Operator),
                 ("POST", "/v1/pipelines/{id}/restart", Operator),
                 ("POST", "/v1/pipelines/{id}/flush", Operator),
+                ("POST", "/v1/cluster/leave", Admin),
+                ("POST", "/v1/admin/shutdown", Admin),
             ]
         );
     }
```

Change `crates/data-avalanche-api/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index cd18963..16c7840 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -32,7 +32,7 @@ use serde_json::{Value, json};
 use tokio::io::{AsyncReadExt, AsyncWriteExt};
 use tokio::sync::Semaphore;
 
-use crate::{Api, Auth, Node, bind, router};
+use crate::{Api, Auth, Control, Node, Stop, bind, router};
 
 pub(crate) const CONFIG: &str = r#"
 config_version: 1
@@ -146,6 +146,8 @@ pub(crate) struct Lone {
     pub node: Node,
     /// The leases the node reads: a test takes them as another node would.
     pub leases: Leases,
+    /// What the API asked of the node.
+    pub stops: tokio::sync::watch::Receiver<Stop>,
     pub _reconciler: ClusterReconciler,
     pub _gossip: Gossip,
 }
@@ -221,6 +223,7 @@ pub(crate) async fn lone(file: PathBuf) -> Lone {
         gossip.members(),
         Duration::from_secs(10),
     );
+    let (control, stops) = Control::new();
     Lone {
         node: Node {
             coordinator: Arc::new(coordinator),
@@ -233,8 +236,10 @@ pub(crate) async fn lone(file: PathBuf) -> Lone {
                 Ok(PipelineDocs::new(DocStore::new(Arc::new(store))))
             }),
             leases: leases.clone(),
+            control,
         },
         leases,
+        stops,
         _reconciler: reconciler,
         _gossip: gossip,
     }
```

Change `crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 98deea2..4f44990 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -764,6 +764,45 @@ mod tests {
         assert_eq!(kinds(&r), ["add q"]);
     }
 
+    /// A check says what a reload would change, and publishes nothing.
+    #[tokio::test(start_paused = true)]
+    async fn a_check_says_what_a_reload_would_change_and_publishes_nothing() {
+        let n = lone(&["p"]).await;
+        let checked = n
+            .coordinator
+            .check(&config_text(&["p", "q"]))
+            .await
+            .unwrap();
+        assert_eq!(checked.generation, 1);
+        assert!(checked.warnings.is_ok());
+        assert!(
+            matches!(&checked.changes.pipelines[..], [PipelineChange::Added { id, .. }] if id == "q"),
+            "{:?}",
+            checked.changes
+        );
+        let same = n.coordinator.check(&config_text(&["p"])).await.unwrap();
+        assert!(same.changes.is_empty());
+        let restart_only = config_text(&["p"]).replace(
+            "  state: { bucket: app-state }\n",
+            "  state: { bucket: app-state }\n  api: { address: 0.0.0.0:9090 }\n",
+        );
+        match n.coordinator.check(&restart_only).await {
+            Err(ReloadError::Rejected(r)) => assert!(r.has("V-061"), "{r:?}"),
+            other => panic!("{other:?}"),
+        }
+        assert!(matches!(
+            n.coordinator.check("pipelines: [").await,
+            Err(ReloadError::Invalid(_))
+        ));
+        let pointer = n.generations.current().await.unwrap().unwrap().pointer;
+        assert_eq!(pointer.doc.generation, 1, "nothing published");
+        n.coordinator.stop().await;
+        assert!(matches!(
+            n.coordinator.check(&config_text(&["p"])).await,
+            Err(ReloadError::Stopped)
+        ));
+    }
+
     /// A reload publishes the file as the next generation and applies it here: the globals
     /// hook sees it, the reconciler gets the changes.
     #[tokio::test(start_paused = true)]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-api -- config:: cluster::`

Expected: FAIL to compile: `check`, `Checked`, `Control`, `Stop` and the handlers don't exist.

```text
error[E0432]: unresolved imports `crate::Control`, `crate::Stop`
  --> crates/data-avalanche-api/src/test_support.rs:35:24
error: cannot find macro `json` in this scope
error[E0560]: struct `Node` has no field named `control`
error[E0433]: cannot find type `Control` in this scope
error[E0433]: cannot find type `Stop` in this scope
```

Run: `cargo test -p data-avalanche-cluster --lib coordinator::`

Expected: FAIL to compile: `check`, `Checked`, `Control`, `Stop` and the handlers don't exist.

```text
error[E0599]: no method named `check` found for struct `coordinator::ConfigCoordinator` in the current scope
   --> crates/data-avalanche-cluster/src/coordinator.rs:773:14
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-api/src/cluster.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/cluster.rs b/crates/data-avalanche-api/src/cluster.rs
index 6819ebb..caccab0 100644
--- a/crates/data-avalanche-api/src/cluster.rs
+++ b/crates/data-avalanche-api/src/cluster.rs
@@ -4,6 +4,142 @@
 //! it is stopped; a restart joins again. A shutdown is what SIGTERM does: the leave, then the
 //! process ends.
 
+use axum::Extension;
+use axum::extract::State;
+use axum::http::StatusCode;
+use axum::response::Response;
+use data_avalanche_cluster::Phase;
+use serde_json::{Value, json};
+use tokio::sync::watch;
+
+use crate::{Api, Principal, body, starting};
+
+/// What the API asked of the node, which the binary carries out; each includes the ones before.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
+pub enum Stop {
+    /// Nothing yet.
+    Run,
+    /// Leave the cluster, and stay up.
+    Leave,
+    /// Leave the cluster, and end.
+    Shutdown,
+}
+
+/// The node's leave and shutdown, as the API asks for them.
+#[derive(Debug, Clone)]
+pub struct Control {
+    asked: watch::Sender<Stop>,
+}
+
+impl Control {
+    /// The control, and what the binary watches.
+    pub fn new() -> (Self, watch::Receiver<Stop>) {
+        let (asked, watched) = watch::channel(Stop::Run);
+        (Self { asked }, watched)
+    }
+
+    /// Asks for `stop` unless it, or more, is asked already: false then.
+    fn ask(&self, stop: Stop) -> bool {
+        self.asked.send_if_modified(|now| {
+            let more = stop > *now;
+            if more {
+                *now = stop;
+            }
+            more
+        })
+    }
+}
+
+fn phase(p: Phase) -> &'static str {
+    match p {
+        Phase::Joining => "joining",
+        Phase::Active => "active",
+        Phase::Draining => "draining",
+        Phase::Leaving => "leaving",
+    }
+}
+
+/// The members this node sees, and each pipeline's lease holder.
+pub(crate) async fn status(State(api): State<Api>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let members = node.members.borrow().clone();
+    let nodes: Vec<Value> = members
+        .nodes
+        .iter()
+        .map(|(name, m)| {
+            json!({
+                "node": name,
+                "addr": m.addr.to_string(),
+                "phase": phase(m.meta.phase),
+                "suspect": m.suspect,
+                "config_generation": m.meta.config_generation,
+                "build": m.meta.build,
+            })
+        })
+        .collect();
+    let config = node.coordinator.config().await;
+    let mut pipelines = vec![];
+    let mut ids: Vec<&str> = config.pipelines.values().map(|p| p.id.as_str()).collect();
+    ids.sort_unstable();
+    for id in ids {
+        let lease = match node.leases.observe(id).await {
+            Ok(lease) => lease.map(|v| v.doc).filter(|l| !l.released),
+            Err(e) => {
+                return body(
+                    StatusCode::SERVICE_UNAVAILABLE,
+                    &json!({ "error": format!("the lease of {id}: {e}") }),
+                );
+            }
+        };
+        pipelines.push(json!({
+            "id": id,
+            "holder": lease.as_ref().map(|l| l.holder_node_id.clone()),
+            "owner_epoch": lease.as_ref().map(|l| l.owner_epoch),
+        }));
+    }
+    body(
+        StatusCode::OK,
+        &json!({ "node": members.me, "members": nodes, "pipelines": pipelines }),
+    )
+}
+
+/// 202 once the leave is asked; 409 when this node leaves, or shuts down, already.
+pub(crate) async fn leave(State(api): State<Api>, Extension(by): Extension<Principal>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    if node.control.ask(Stop::Leave) {
+        tracing::info!(by = %by.name, "leaving the cluster, asked through the API");
+        body(StatusCode::ACCEPTED, &json!({ "leaving": true }))
+    } else {
+        body(
+            StatusCode::CONFLICT,
+            &json!({ "error": "this node leaves the cluster already" }),
+        )
+    }
+}
+
+/// 202 once the shutdown is asked; 409 when it is asked already.
+pub(crate) async fn shutdown(
+    State(api): State<Api>,
+    Extension(by): Extension<Principal>,
+) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    if node.control.ask(Stop::Shutdown) {
+        tracing::info!(by = %by.name, "shutting down, asked through the API");
+        body(StatusCode::ACCEPTED, &json!({ "shutting_down": true }))
+    } else {
+        body(
+            StatusCode::CONFLICT,
+            &json!({ "error": "this node shuts down already" }),
+        )
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::time::Duration;
```

Change `crates/data-avalanche-api/src/config.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index bbfd1f5..6b3fae4 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -1,15 +1,70 @@
-//! `/v1/config`: the reload and its status (Clustering §9.2).
+//! `/v1/config`: the check, the reload and its status (Clustering §9.2).
 
 use std::collections::BTreeMap;
 
 use axum::extract::State;
 use axum::http::StatusCode;
 use axum::response::Response;
-use data_avalanche_cluster::{ReloadError, Reloaded};
+use data_avalanche_cluster::{Checked, ReloadError, Reloaded};
 use serde_json::json;
 
 use crate::{Api, body, starting};
 
+/// A refused reload or check: 422 with the report, 409 after the retries, 503 when
+/// `globals.state` fails or the node stops.
+fn refused(e: &ReloadError) -> Response {
+    let code = match e {
+        ReloadError::Invalid(_) | ReloadError::Rejected(_) => StatusCode::UNPROCESSABLE_ENTITY,
+        ReloadError::Contended => StatusCode::CONFLICT,
+        ReloadError::State(_) | ReloadError::Stopped => StatusCode::SERVICE_UNAVAILABLE,
+    };
+    let mut v = json!({ "error": e.to_string() });
+    if let ReloadError::Invalid(r) | ReloadError::Rejected(r) = e {
+        v["report"] = r.to_json();
+    }
+    body(code, &v)
+}
+
+/// Checks a config and publishes nothing (D-M9a.7): the request's body, or the node's file when
+/// the body is empty, loaded in this node's environment and diffed against the generation it
+/// runs. 200 with the warnings and what a reload would change; 422 with the report when it is
+/// invalid or changes a field only a process restart applies. The resolved config, secrets
+/// and all, is not echoed.
+pub(crate) async fn validate(State(api): State<Api>, text: String) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let text = if text.trim().is_empty() {
+        match tokio::fs::read_to_string(&node.file).await {
+            Ok(text) => text,
+            Err(e) => {
+                return body(
+                    StatusCode::INTERNAL_SERVER_ERROR,
+                    &json!({ "error": format!("cannot read config file {}: {e}", node.file.display()) }),
+                );
+            }
+        }
+    } else {
+        text
+    };
+    match node.coordinator.check(&text).await {
+        Ok(Checked {
+            generation,
+            warnings,
+            changes,
+        }) => body(
+            StatusCode::OK,
+            &json!({
+                "valid": true,
+                "against_generation": generation,
+                "warnings": warnings.to_json()["warnings"],
+                "changes": { "globals": changes.globals, "pipelines": changes.pipelines },
+            }),
+        ),
+        Err(e) => refused(&e),
+    }
+}
+
 /// Publishes the node's file as the next config generation: 202 with it, 200 when the file is
 /// the generation the cluster runs already, 422 with the validation report when it is invalid or
 /// changes a field only a process restart applies.
@@ -35,20 +90,7 @@ pub(crate) async fn reload(State(api): State<Api>) -> Response {
             StatusCode::OK,
             &json!({ "generation": generation, "unchanged": true }),
         ),
-        Err(e) => {
-            let code = match &e {
-                ReloadError::Invalid(_) | ReloadError::Rejected(_) => {
-                    StatusCode::UNPROCESSABLE_ENTITY
-                }
-                ReloadError::Contended => StatusCode::CONFLICT,
-                ReloadError::State(_) | ReloadError::Stopped => StatusCode::SERVICE_UNAVAILABLE,
-            };
-            let mut v = json!({ "error": e.to_string() });
-            if let ReloadError::Invalid(r) | ReloadError::Rejected(r) = &e {
-                v["report"] = r.to_json();
-            }
-            body(code, &v)
-        }
+        Err(e) => refused(&e),
     }
 }
 
```

Change `crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 147bc45..0ef963b 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -34,6 +34,7 @@ use serde_json::{Value, json};
 use tokio::sync::watch;
 
 pub use auth::{Auth, FETCH_TIMEOUT, Principal, REFETCH_AFTER};
+pub use cluster::{Control, Stop};
 pub use server::{Bound, HANDSHAKE_TIMEOUT, ServeError, bind};
 
 use crate::auth::Denied;
@@ -60,6 +61,8 @@ pub struct Node {
     pub docs: Arc<DocsFn>,
     /// The leases in `globals.state`: who holds each pipeline.
     pub leases: Leases,
+    /// The node's leave and shutdown.
+    pub control: Control,
 }
 
 impl std::fmt::Debug for Node {
@@ -87,6 +90,10 @@ const fn route(method: &'static str, path: &'static str, role: Role) -> Route {
 /// Every `/v1` route, with its handler.
 fn v1() -> Vec<(Route, MethodRouter<Api>)> {
     vec![
+        (
+            route("POST", "/v1/config/validate", Role::Operator),
+            post(config::validate),
+        ),
         (
             route("POST", "/v1/config/reload", Role::Operator),
             post(config::reload),
@@ -95,6 +102,10 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             route("GET", "/v1/config/status", Role::Viewer),
             get(config::status),
         ),
+        (
+            route("GET", "/v1/cluster/status", Role::Viewer),
+            get(cluster::status),
+        ),
         (
             route("GET", "/v1/pipelines", Role::Viewer),
             get(pipelines::list),
@@ -123,6 +134,14 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             route("POST", "/v1/pipelines/{id}/flush", Role::Operator),
             post(pipelines::flush),
         ),
+        (
+            route("POST", "/v1/cluster/leave", Role::Admin),
+            post(cluster::leave),
+        ),
+        (
+            route("POST", "/v1/admin/shutdown", Role::Admin),
+            post(cluster::shutdown),
+        ),
     ]
 }
 
```

Change `crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 4f44990..9f4c192 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -72,6 +72,16 @@ pub struct Reloaded {
     pub published: bool,
 }
 
+/// What a reload would do here ([`ConfigCoordinator::check`]).
+#[derive(Debug, Clone)]
+pub struct Checked {
+    /// The generation the changes are against: the one this node runs.
+    pub generation: u64,
+    /// The text's warnings.
+    pub warnings: Report,
+    pub changes: ConfigDiff,
+}
+
 /// Why a reload published nothing.
 #[derive(Debug, Error)]
 pub enum ReloadError {
@@ -256,6 +266,35 @@ impl ConfigCoordinator {
         Err(ReloadError::Contended)
     }
 
+    /// What a reload of `text` would change on this node, nothing published (`POST
+    /// /v1/config/validate`): the text loaded in this node's environment and diffed against
+    /// the generation this node runs. A table-pattern change counts as a restart here; the
+    /// reload's adoption asks the source whether the table set changed (config §1.2).
+    ///
+    /// # Errors
+    /// `Invalid`, `Rejected` or `Stopped`, as a reload's.
+    pub async fn check(&self, text: &str) -> Result<Checked, ReloadError> {
+        let d = &self.inner.deps;
+        let running = self.inner.running.lock().await;
+        if running.stopped {
+            return Err(ReloadError::Stopped);
+        }
+        let loaded = load_str(text, &*d.env, &d.key).map_err(ReloadError::Invalid)?;
+        let changes = diff(&running.config, &loaded.config, &|_, _| None);
+        if changes.is_rejected() {
+            return Err(ReloadError::Rejected(changes.report));
+        }
+        let mut warnings = Report::default();
+        for w in loaded.warnings {
+            warnings.push(w);
+        }
+        Ok(Checked {
+            generation: running.generation.doc.generation,
+            warnings,
+            changes,
+        })
+    }
+
     /// Stops watching, after the adoption or reload under way: none follows, and a reload is
     /// [`ReloadError::Stopped`].
     pub async fn stop(&self) {
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 471aa6b..b62e6d0 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -26,7 +26,7 @@ pub use controller::{
     Target, Update, WorkerFactory,
 };
 pub use coordinator::{
-    ConfigCoordinator, CoordinatorDeps, DocsFn, PUBLISH_ATTEMPTS, ReloadError, Reloaded,
+    Checked, ConfigCoordinator, CoordinatorDeps, DocsFn, PUBLISH_ATTEMPTS, ReloadError, Reloaded,
 };
 pub use discovery::{Seeds, advertise, discover, interfaces};
 pub use error::ClusterError;
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index a06312c..ab4c29c 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -5,7 +5,7 @@ use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::{Arc, OnceLock};
 use std::time::{Duration, SystemTime};
 
-use data_avalanche_api::{Api, Auth, Node, ServeError, bind, router};
+use data_avalanche_api::{Api, Auth, Control, Node, ServeError, Stop, bind, router};
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
@@ -86,7 +86,8 @@ pub(crate) fn leave_within(config: &ResolvedConfig) -> Duration {
     c.drain_timeout.saturating_mul(rounds) + 2 * c.lease_renewal_interval
 }
 
-/// Runs every pipeline until SIGTERM or SIGINT.
+/// Runs every pipeline until SIGTERM, SIGINT or a shutdown through the API. A leave through the
+/// API leaves the cluster first and keeps the process up (D-M9a.8).
 ///
 /// # Errors
 /// [`Fatal`]: the config is refused, or `globals.state` can't be joined.
@@ -225,22 +226,46 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         gossip.members(),
         c.lease_renewal_interval,
     ));
+    let (control, mut asked) = Control::new();
     let _ = node.set(Node {
         coordinator: Arc::clone(&coordinator),
         members: gossip.members(),
         file: args.config.clone(),
         docs,
         leases: deps.leases.clone(),
+        control,
     });
     ready.store(true, Ordering::SeqCst);
-    shutdown_signal().await;
-    tracing::info!("shutting down: handing off every pipeline and releasing its lease");
-    ready.store(false, Ordering::SeqCst);
-    discovery.abort();
-    // No generation is adopted during the leave; it hands off what the last one configures.
-    coordinator.stop().await;
-    let config = coordinator.config().await;
-    leave(gossip, reconciler, leave_within(&config)).await;
+    let mut cluster = Some((gossip, reconciler, discovery));
+    loop {
+        let stop = tokio::select! {
+            () = shutdown_signal() => Stop::Shutdown,
+            changed = asked.changed() => match changed {
+                Ok(()) => *asked.borrow_and_update(),
+                Err(_) => Stop::Shutdown,
+            },
+        };
+        if stop == Stop::Run {
+            continue;
+        }
+        if let Some((gossip, reconciler, discovery)) = cluster.take() {
+            tracing::info!(
+                "leaving the cluster: handing off every pipeline and releasing its lease"
+            );
+            ready.store(false, Ordering::SeqCst);
+            discovery.abort();
+            // No generation is adopted during the leave; it hands off what the last one
+            // configures.
+            coordinator.stop().await;
+            let config = coordinator.config().await;
+            leave(gossip, reconciler, leave_within(&config)).await;
+        }
+        if stop == Stop::Shutdown {
+            break;
+        }
+        tracing::info!("left the cluster; the process stays up until it is stopped");
+    }
+    tracing::info!("shutting down");
     let _ = stop_http.send(());
     let _ = server.await;
     tracing::info!("stopped");
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-api`

Expected: PASS, 23 tests.

```text
test result: ok. 23 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Run: `cargo test -p data-avalanche-cluster --lib coordinator::`

Expected: PASS, 12 tests.

```text
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 120 filtered out; finished in 0.03s
```

Run: `cargo test -p data-avalanche --lib`

Expected: PASS, 18 tests.

```text
test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1233 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/cluster.rs \
  crates/data-avalanche-api/src/config.rs \
  crates/data-avalanche-api/src/lib.rs \
  crates/data-avalanche-api/src/test_support.rs \
  crates/data-avalanche-cluster/src/coordinator.rs \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche/src/run.rs
git commit -m "feat(api): config validate, cluster status, leave and shutdown; the binary stays up after a leave"
```

---

### Task 7: The OpenAPI document (`data-avalanche-api`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `Cargo.toml`
- Modify: `crates/data-avalanche-api/Cargo.toml`
- Create: `crates/data-avalanche-api/openapi.json`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Create: `crates/data-avalanche-api/src/openapi.rs`

**Interfaces:**
- Consumes: Task 4's route table.
- Produces: `openapi() -> utoipa::openapi::OpenApi`, served at `GET /v1/openapi.json` (viewer);
  the snapshot `crates/data-avalanche-api/openapi.json`, which `DATA_AVALANCHE_BLESS=1` rewrites.

D-M9a.9. The document is built from the route table with utoipa's builders, so it can't
drift from the router: each operation has its summary, its least role (`x-min-role`, and the
`basic` and `bearer` security schemes), the `{id}` parameter, the request body's content type,
and its answers with the common 401, 403 and 503. The answers' fields are not modelled (a
follow-up). utoipa 6 doesn't build without its `macros` feature, though no macro is used.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index fdd332b..fc07699 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1747,6 +1747,7 @@ dependencies = [
  "tokio",
  "tokio-rustls",
  "tracing",
+ "utoipa",
  "uuid",
 ]
 
@@ -3643,6 +3644,8 @@ checksum = "cc4e190f5d26ca7051642629da2c52fc03bde85a03197c99408dcd291734c855"
 dependencies = [
  "equivalent",
  "hashbrown 0.17.1",
+ "serde",
+ "serde_core",
 ]
 
 [[package]]
@@ -6269,6 +6272,29 @@ version = "0.2.2"
 source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "06abde3611657adf66d383f00b093d7faecc7fa57071cce2578660c9f1010821"
 
+[[package]]
+name = "utoipa"
+version = "6.0.0"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "8765fe27aeff71012a3f90fa474cf1df863a7d0dd81ed25de4e63fff2544994f"
+dependencies = [
+ "indexmap",
+ "serde",
+ "serde_json",
+ "utoipa-gen",
+]
+
+[[package]]
+name = "utoipa-gen"
+version = "6.0.1"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "d935f1c83fdf8b88f09bbe050d0cea78ea4afee6d7b0317403228c1200b2c8ab"
+dependencies = [
+ "proc-macro2",
+ "quote",
+ "syn 3.0.6",
+]
+
 [[package]]
 name = "uuid"
 version = "1.26.1"
```

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 8bd7944..f08ce37 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -88,6 +88,9 @@ jsonwebtoken = { version = "=11.1.0", default-features = false, features = ["aws
 reqwest = { version = "0.13.5", default-features = false, features = ["rustls"] }
 base64 = "0.22.1"
 subtle = "2.6.1"
+# Its OpenAPI document (roadmap §4 M9), built from the route table with the builders. 6.0.0
+# doesn't build without `macros`: its schema module names a type only that feature defines.
+utoipa = { version = "=6.0.0", default-features = false, features = ["macros"] }
 # The cluster (M8a, roadmap §2.1): the ring's hash (the anchor's `hash_algo: xxh3`); the SWIM
 # core the gossip drives (Q-M8-1, probe `prototypes/cluster-capability`) and its random source;
 # the host's addresses, for an unset `advertise_addr`.
```

Change `crates/data-avalanche-api/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-api/Cargo.toml b/crates/data-avalanche-api/Cargo.toml
index 2f7a6b4..39b0afd 100644
--- a/crates/data-avalanche-api/Cargo.toml
+++ b/crates/data-avalanche-api/Cargo.toml
@@ -29,6 +29,7 @@ thiserror.workspace = true
 tokio = { workspace = true, features = ["net", "sync", "fs"] }
 tokio-rustls.workspace = true
 tracing.workspace = true
+utoipa.workspace = true
 uuid.workspace = true
 
 [dev-dependencies]
```

Create `crates/data-avalanche-api/openapi.json`:

```text
{
  "openapi": "3.1.0",
  "info": {
    "title": "data-avalanche management API",
    "description": "`globals.api.auth` decides which credentials a node takes: none, basic (one user, admin), or a bearer JWT whose roles are mapped (config §6.3).",
    "version": "0.1.0"
  },
  "paths": {
    "/v1/admin/shutdown": {
      "post": {
        "tags": [
          "admin"
        ],
        "summary": "Shut down: leave the cluster, then end the process",
        "responses": {
          "202": {
            "description": "the shutdown is under way",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "409": {
            "description": "shutting down already",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "admin"
      }
    },
    "/v1/cluster/leave": {
      "post": {
        "tags": [
          "cluster"
        ],
        "summary": "Leave the cluster: hand every pipeline off; the process stays up, not ready",
        "responses": {
          "202": {
            "description": "the leave is under way",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "409": {
            "description": "leaving already",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "admin"
      }
    },
    "/v1/cluster/status": {
      "get": {
        "tags": [
          "cluster"
        ],
        "summary": "The members this node sees, and each pipeline's lease holder",
        "responses": {
          "200": {
            "description": "the members and the holders",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "viewer"
      }
    },
    "/v1/config/reload": {
      "post": {
        "tags": [
          "config"
        ],
        "summary": "Publish the node's file as the next config generation",
        "responses": {
          "200": {
            "description": "the file is the generation the cluster runs",
            "content": {
              "application/json": {}
            }
          },
          "202": {
            "description": "published: the generation",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "409": {
            "description": "other nodes published first, every time",
            "content": {
              "application/json": {}
            }
          },
          "422": {
            "description": "invalid, or it changes a restart-only field: the report",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "operator"
      }
    },
    "/v1/config/status": {
      "get": {
        "tags": [
          "config"
        ],
        "summary": "The generation the cluster runs, and each member's",
        "responses": {
          "200": {
            "description": "the generations",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "viewer"
      }
    },
    "/v1/config/validate": {
      "post": {
        "tags": [
          "config"
        ],
        "summary": "Check a config, the body or else the node's file: what a reload of it would change. Nothing is published",
        "requestBody": {
          "content": {
            "application/yaml": {}
          },
          "required": false
        },
        "responses": {
          "200": {
            "description": "it loads: its warnings and the changes",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "422": {
            "description": "invalid, or it changes a restart-only field: the report",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "operator"
      }
    },
    "/v1/openapi.json": {
      "get": {
        "tags": [
          "openapi.json"
        ],
        "summary": "This document",
        "responses": {
          "200": {
            "description": "the OpenAPI document",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "viewer"
      }
    },
    "/v1/pipelines": {
      "get": {
        "tags": [
          "pipelines"
        ],
        "summary": "Every pipeline: its desired and actual states and its holder",
        "responses": {
          "200": {
            "description": "the pipelines",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "viewer"
      }
    },
    "/v1/pipelines/{id}": {
      "get": {
        "tags": [
          "pipelines"
        ],
        "summary": "A pipeline: its states, its holder, its position and its halted tables",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "description": "the pipeline's id",
            "required": true,
            "schema": {
              "type": "string"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "the pipeline",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "404": {
            "description": "no such pipeline in the generation this node runs",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "viewer"
      }
    },
    "/v1/pipelines/{id}/flush": {
      "post": {
        "tags": [
          "pipelines"
        ],
        "summary": "Flush now",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "description": "the pipeline's id",
            "required": true,
            "schema": {
              "type": "string"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "paused: flushed already",
            "content": {
              "application/json": {}
            }
          },
          "202": {
            "description": "asked: the owner acts at its next poll",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "404": {
            "description": "no such pipeline in the generation this node runs",
            "content": {
              "application/json": {}
            }
          },
          "409": {
            "description": "stopped, or no node holds the lease",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "operator"
      }
    },
    "/v1/pipelines/{id}/pause": {
      "post": {
        "tags": [
          "pipelines"
        ],
        "summary": "Pause: flush to a transaction boundary, then read nothing; the connection and the lease stay",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "description": "the pipeline's id",
            "required": true,
            "schema": {
              "type": "string"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "paused already",
            "content": {
              "application/json": {}
            }
          },
          "202": {
            "description": "the desired state is paused",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "404": {
            "description": "no such pipeline in the generation this node runs",
            "content": {
              "application/json": {}
            }
          },
          "409": {
            "description": "the pipeline is stopped",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "operator"
      }
    },
    "/v1/pipelines/{id}/restart": {
      "post": {
        "tags": [
          "pipelines"
        ],
        "summary": "Restart: discard what is unflushed and replay from the checkpoint, under the same lease; a pipeline in ERROR runs again",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "description": "the pipeline's id",
            "required": true,
            "schema": {
              "type": "string"
            }
          }
        ],
        "responses": {
          "202": {
            "description": "asked: the owner acts at its next poll",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "404": {
            "description": "no such pipeline in the generation this node runs",
            "content": {
              "application/json": {}
            }
          },
          "409": {
            "description": "paused, stopped, or no node holds the lease",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "operator"
      }
    },
    "/v1/pipelines/{id}/resume": {
      "post": {
        "tags": [
          "pipelines"
        ],
        "summary": "Resume a paused or stopped pipeline",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "description": "the pipeline's id",
            "required": true,
            "schema": {
              "type": "string"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "running already",
            "content": {
              "application/json": {}
            }
          },
          "202": {
            "description": "the desired state is running",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "404": {
            "description": "no such pipeline in the generation this node runs",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "operator"
      }
    },
    "/v1/pipelines/{id}/stop": {
      "post": {
        "tags": [
          "pipelines"
        ],
        "summary": "Stop: flush, and close the source connection; the lease stays",
        "parameters": [
          {
            "name": "id",
            "in": "path",
            "description": "the pipeline's id",
            "required": true,
            "schema": {
              "type": "string"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "stopped already",
            "content": {
              "application/json": {}
            }
          },
          "202": {
            "description": "the desired state is stopped",
            "content": {
              "application/json": {}
            }
          },
          "401": {
            "description": "no credentials, or ones that don't verify",
            "content": {
              "application/json": {}
            }
          },
          "403": {
            "description": "the caller's role is below the route's",
            "content": {
              "application/json": {}
            }
          },
          "404": {
            "description": "no such pipeline in the generation this node runs",
            "content": {
              "application/json": {}
            }
          },
          "503": {
            "description": "the node is starting or stopping, or its state can't be read",
            "content": {
              "application/json": {}
            }
          }
        },
        "security": [
          {
            "basic": []
          },
          {
            "bearer": []
          }
        ],
        "x-min-role": "operator"
      }
    }
  },
  "components": {
    "securitySchemes": {
      "basic": {
        "type": "http",
        "scheme": "basic"
      },
      "bearer": {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT"
      }
    }
  }
}
```

Change `crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 0ef963b..ff5fde3 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -11,6 +11,7 @@
 mod auth;
 mod cluster;
 mod config;
+mod openapi;
 mod pipelines;
 mod server;
 #[cfg(test)]
@@ -276,13 +277,14 @@ mod tests {
                 ("POST", "/v1/pipelines/{id}/flush", Operator),
                 ("POST", "/v1/cluster/leave", Admin),
                 ("POST", "/v1/admin/shutdown", Admin),
+                ("GET", "/v1/openapi.json", Viewer),
             ]
         );
     }
 
     /// Every `/v1` route: 401 without a token or with one that doesn't verify, 403 below its
     /// role or with no role mapped, through at its role and above. Through means past the gate:
-    /// the node doesn't run its generation here, so the route itself answers 503.
+    /// the node doesn't run its generation here, so most routes answer 503 themselves.
     #[tokio::test]
     async fn every_route_turns_away_callers_below_its_role() {
         let idp = Issuer::start().await;
@@ -310,7 +312,11 @@ mod tests {
                 let auth = [("Authorization", token.as_str())];
                 let (code, _, body) = request(addr, r.method, &path, &auth, "").await;
                 if role.is_some_and(|role| role >= r.role) {
-                    assert_eq!(code, 503, "{} {path} as {claim}: {body}", r.method);
+                    assert!(
+                        code != 401 && code != 403,
+                        "{} {path} as {claim}: {code} {body}",
+                        r.method
+                    );
                 } else {
                     assert_eq!(code, 403, "{} {path} as {claim}: {body}", r.method);
                 }
```

Create `crates/data-avalanche-api/src/openapi.rs`:

```rust
//! The OpenAPI 3.1 document of the `/v1` routes, `GET /v1/openapi.json` (D-M9a.9). It is built
//! from the route table ([`crate::routes`]), as the router and its role gate are: each
//! operation's summary, its least role (`x-min-role`, and the basic or bearer credentials it
//! takes), its path parameter, its request body and its answers. The answers' bodies are JSON
//! objects, an `error` string on a refusal; their fields are not modelled.

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::test_support::{api, json_of, open, serve as listen};

    const SNAPSHOT: &str = include_str!("../openapi.json");

    /// The document as it is checked in, `openapi.json`. `DATA_AVALANCHE_BLESS=1` writes it
    /// anew, for a change meant to change the API.
    #[test]
    fn the_document_is_the_one_checked_in() {
        let now = openapi().to_pretty_json().unwrap() + "\n";
        if std::env::var_os("DATA_AVALANCHE_BLESS").is_some() {
            std::fs::write(concat!(env!("CARGO_MANIFEST_DIR"), "/openapi.json"), &now).unwrap();
            return;
        }
        assert!(
            now == SNAPSHOT,
            "the OpenAPI document changed; if that is meant, run the test with DATA_AVALANCHE_BLESS=1 and check the diff of crates/data-avalanche-api/openapi.json"
        );
    }

    /// Every route is an operation of the document, with its role, and the document is a
    /// valid one as utoipa reads it back: OpenAPI 3.1, every security requirement a scheme of
    /// its components.
    #[test]
    fn every_route_is_in_the_document_with_its_role() {
        let doc: Value = serde_json::to_value(openapi()).unwrap();
        assert_eq!(doc["openapi"], "3.1.0");
        let back: OpenApi = serde_json::from_value(doc.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(back).unwrap(),
            doc,
            "it reads back as it is"
        );
        let schemes = doc["components"]["securitySchemes"].as_object().unwrap();
        let mut operations = 0;
        for (path, item) in doc["paths"].as_object().unwrap() {
            for (method, op) in item.as_object().unwrap() {
                operations += 1;
                let r = routes()
                    .into_iter()
                    .find(|r| r.path == path && r.method.eq_ignore_ascii_case(method))
                    .unwrap_or_else(|| panic!("{method} {path} is no route"));
                assert_eq!(op["x-min-role"], r.role.to_string(), "{method} {path}");
                for requirement in op["security"].as_array().unwrap() {
                    for name in requirement.as_object().unwrap().keys() {
                        assert!(schemes.contains_key(name), "{name}");
                    }
                }
                for code in ["401", "403", "503"] {
                    assert!(
                        op["responses"].get(code).is_some(),
                        "{method} {path} {code}"
                    );
                }
                if path.contains("{id}") {
                    assert_eq!(op["parameters"][0]["name"], "id", "{method} {path}");
                }
            }
        }
        assert_eq!(operations, routes().len());
    }

    #[tokio::test]
    async fn the_document_is_served() {
        let addr = listen(api(open())).await;
        let (code, v) = json_of(addr, "GET", "/v1/openapi.json").await;
        assert_eq!(code, 200);
        assert_eq!(v, serde_json::to_value(openapi()).unwrap());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-api -- openapi::`

Expected: FAIL to compile: `openapi` and the routes' `summary`, `body` and `answers` don't exist.

```text
error[E0425]: cannot find type `OpenApi` in this scope
  --> crates/data-avalanche-api/src/openapi.rs:38:19
error[E0425]: cannot find function `routes` in this scope
error[E0425]: cannot find function `openapi` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index ff5fde3..28d9b43 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -36,6 +36,7 @@ use tokio::sync::watch;
 
 pub use auth::{Auth, FETCH_TIMEOUT, Principal, REFETCH_AFTER};
 pub use cluster::{Control, Stop};
+pub use openapi::openapi;
 pub use server::{Bound, HANDSHAKE_TIMEOUT, ServeError, bind};
 
 use crate::auth::Denied;
@@ -75,74 +76,219 @@ impl std::fmt::Debug for Node {
     }
 }
 
-/// A `/v1` route and the least role that may call it (API_RBAC §2).
+/// A `/v1` route: the least role that may call it (API_RBAC §2), and what the OpenAPI document
+/// says of it ([`openapi`]).
 #[derive(Debug, Clone, Copy, PartialEq, Eq)]
 pub struct Route {
     pub method: &'static str,
-    /// As the router matches it: `{id}` for a path parameter.
+    /// As the router matches it: `{id}`, a pipeline's id, for a path parameter.
     pub path: &'static str,
     pub role: Role,
+    pub summary: &'static str,
+    /// The request body's content type, when it takes one.
+    pub body: Option<&'static str>,
+    /// Its own answers; 401, 403 and 503 are every route's.
+    pub answers: &'static [(u16, &'static str)],
 }
 
-const fn route(method: &'static str, path: &'static str, role: Role) -> Route {
-    Route { method, path, role }
-}
+const ASKED: (u16, &str) = (202, "asked: the owner acts at its next poll");
+const UNKNOWN: (u16, &str) = (404, "no such pipeline in the generation this node runs");
 
 /// Every `/v1` route, with its handler.
+#[allow(clippy::too_many_lines)]
 fn v1() -> Vec<(Route, MethodRouter<Api>)> {
+    use Role::{Admin, Operator, Viewer};
+    let r = |method, path, role, summary, answers| Route {
+        method,
+        path,
+        role,
+        summary,
+        body: None,
+        answers,
+    };
     vec![
         (
-            route("POST", "/v1/config/validate", Role::Operator),
+            Route {
+                body: Some("application/yaml"),
+                ..r(
+                    "POST",
+                    "/v1/config/validate",
+                    Operator,
+                    "Check a config, the body or else the node's file: what a reload of it would change. Nothing is published",
+                    &[
+                        (200, "it loads: its warnings and the changes"),
+                        (
+                            422,
+                            "invalid, or it changes a restart-only field: the report",
+                        ),
+                    ],
+                )
+            },
             post(config::validate),
         ),
         (
-            route("POST", "/v1/config/reload", Role::Operator),
+            r(
+                "POST",
+                "/v1/config/reload",
+                Operator,
+                "Publish the node's file as the next config generation",
+                &[
+                    (200, "the file is the generation the cluster runs"),
+                    (202, "published: the generation"),
+                    (409, "other nodes published first, every time"),
+                    (
+                        422,
+                        "invalid, or it changes a restart-only field: the report",
+                    ),
+                ],
+            ),
             post(config::reload),
         ),
         (
-            route("GET", "/v1/config/status", Role::Viewer),
+            r(
+                "GET",
+                "/v1/config/status",
+                Viewer,
+                "The generation the cluster runs, and each member's",
+                &[(200, "the generations")],
+            ),
             get(config::status),
         ),
         (
-            route("GET", "/v1/cluster/status", Role::Viewer),
+            r(
+                "GET",
+                "/v1/cluster/status",
+                Viewer,
+                "The members this node sees, and each pipeline's lease holder",
+                &[(200, "the members and the holders")],
+            ),
             get(cluster::status),
         ),
         (
-            route("GET", "/v1/pipelines", Role::Viewer),
+            r(
+                "GET",
+                "/v1/pipelines",
+                Viewer,
+                "Every pipeline: its desired and actual states and its holder",
+                &[(200, "the pipelines")],
+            ),
             get(pipelines::list),
         ),
         (
-            route("GET", "/v1/pipelines/{id}", Role::Viewer),
+            r(
+                "GET",
+                "/v1/pipelines/{id}",
+                Viewer,
+                "A pipeline: its states, its holder, its position and its halted tables",
+                &[(200, "the pipeline"), UNKNOWN],
+            ),
             get(pipelines::get),
         ),
         (
-            route("POST", "/v1/pipelines/{id}/pause", Role::Operator),
+            r(
+                "POST",
+                "/v1/pipelines/{id}/pause",
+                Operator,
+                "Pause: flush to a transaction boundary, then read nothing; the connection and the lease stay",
+                &[
+                    (200, "paused already"),
+                    (202, "the desired state is paused"),
+                    UNKNOWN,
+                    (409, "the pipeline is stopped"),
+                ],
+            ),
             post(pipelines::pause),
         ),
         (
-            route("POST", "/v1/pipelines/{id}/stop", Role::Operator),
+            r(
+                "POST",
+                "/v1/pipelines/{id}/stop",
+                Operator,
+                "Stop: flush, and close the source connection; the lease stays",
+                &[
+                    (200, "stopped already"),
+                    (202, "the desired state is stopped"),
+                    UNKNOWN,
+                ],
+            ),
             post(pipelines::stop),
         ),
         (
-            route("POST", "/v1/pipelines/{id}/resume", Role::Operator),
+            r(
+                "POST",
+                "/v1/pipelines/{id}/resume",
+                Operator,
+                "Resume a paused or stopped pipeline",
+                &[
+                    (200, "running already"),
+                    (202, "the desired state is running"),
+                    UNKNOWN,
+                ],
+            ),
             post(pipelines::resume),
         ),
         (
-            route("POST", "/v1/pipelines/{id}/restart", Role::Operator),
+            r(
+                "POST",
+                "/v1/pipelines/{id}/restart",
+                Operator,
+                "Restart: discard what is unflushed and replay from the checkpoint, under the same lease; a pipeline in ERROR runs again",
+                &[
+                    ASKED,
+                    UNKNOWN,
+                    (409, "paused, stopped, or no node holds the lease"),
+                ],
+            ),
             post(pipelines::restart),
         ),
         (
-            route("POST", "/v1/pipelines/{id}/flush", Role::Operator),
+            r(
+                "POST",
+                "/v1/pipelines/{id}/flush",
+                Operator,
+                "Flush now",
+                &[
+                    (200, "paused: flushed already"),
+                    ASKED,
+                    UNKNOWN,
+                    (409, "stopped, or no node holds the lease"),
+                ],
+            ),
             post(pipelines::flush),
         ),
         (
-            route("POST", "/v1/cluster/leave", Role::Admin),
+            r(
+                "POST",
+                "/v1/cluster/leave",
+                Admin,
+                "Leave the cluster: hand every pipeline off; the process stays up, not ready",
+                &[(202, "the leave is under way"), (409, "leaving already")],
+            ),
             post(cluster::leave),
         ),
         (
-            route("POST", "/v1/admin/shutdown", Role::Admin),
+            r(
+                "POST",
+                "/v1/admin/shutdown",
+                Admin,
+                "Shut down: leave the cluster, then end the process",
+                &[
+                    (202, "the shutdown is under way"),
+                    (409, "shutting down already"),
+                ],
+            ),
             post(cluster::shutdown),
         ),
+        (
+            r(
+                "GET",
+                "/v1/openapi.json",
+                Viewer,
+                "This document",
+                &[(200, "the OpenAPI document")],
+            ),
+            get(openapi::serve),
+        ),
     ]
 }
 
```

Change `crates/data-avalanche-api/src/openapi.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index 061f295..219f26f 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -4,6 +4,119 @@
 //! takes), its path parameter, its request body and its answers. The answers' bodies are JSON
 //! objects, an `error` string on a refusal; their fields are not modelled.
 
+use axum::http::StatusCode;
+use axum::response::Response;
+use utoipa::openapi::extensions::ExtensionsBuilder;
+use utoipa::openapi::path::{HttpMethod, OperationBuilder, ParameterBuilder, ParameterIn};
+use utoipa::openapi::request_body::RequestBodyBuilder;
+use utoipa::openapi::schema::{ObjectBuilder, Type};
+use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme};
+use utoipa::openapi::{
+    ComponentsBuilder, ContentBuilder, InfoBuilder, OpenApi, OpenApiBuilder, Paths, Required,
+    ResponseBuilder,
+};
+
+use crate::{Route, body, routes};
+
+/// Every route's answers besides its own.
+const COMMON: [(u16, &str); 3] = [
+    (401, "no credentials, or ones that don't verify"),
+    (403, "the caller's role is below the route's"),
+    (
+        503,
+        "the node is starting or stopping, or its state can't be read",
+    ),
+];
+
+fn operation(r: &Route) -> utoipa::openapi::path::Operation {
+    let role = r.role.to_string();
+    let mut op = OperationBuilder::new()
+        .summary(Some(r.summary))
+        .tag(r.path.split('/').nth(2).unwrap_or("v1"))
+        .security(SecurityRequirement::new("basic", Vec::<String>::new()))
+        .security(SecurityRequirement::new("bearer", Vec::<String>::new()))
+        .extensions(Some(
+            ExtensionsBuilder::new().add("x-min-role", role).build(),
+        ));
+    if r.path.contains("{id}") {
+        op = op.parameter(
+            ParameterBuilder::new()
+                .name("id")
+                .parameter_in(ParameterIn::Path)
+                .required(Required::True)
+                .description(Some("the pipeline's id"))
+                .schema(Some(ObjectBuilder::new().schema_type(Type::String))),
+        );
+    }
+    if let Some(content_type) = r.body {
+        op = op.request_body(Some(
+            RequestBodyBuilder::new()
+                .required(Some(Required::False))
+                .content(content_type, ContentBuilder::new().build())
+                .build(),
+        ));
+    }
+    let mut answers = r.answers.to_vec();
+    answers.extend(COMMON);
+    answers.sort_unstable();
+    for (code, description) in answers {
+        op = op.response(
+            code.to_string(),
+            ResponseBuilder::new()
+                .description(description)
+                .content("application/json", ContentBuilder::new().build()),
+        );
+    }
+    op.build()
+}
+
+/// The document.
+pub fn openapi() -> OpenApi {
+    let mut paths = Paths::new();
+    for r in routes() {
+        let method = match r.method {
+            "GET" => HttpMethod::Get,
+            "POST" => HttpMethod::Post,
+            other => unreachable!("no {other} route"),
+        };
+        paths.add_path_operation(r.path, vec![method], operation(&r));
+    }
+    let components = ComponentsBuilder::new()
+        .security_scheme(
+            "basic",
+            SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Basic).build()),
+        )
+        .security_scheme(
+            "bearer",
+            SecurityScheme::Http(
+                HttpBuilder::new()
+                    .scheme(HttpAuthScheme::Bearer)
+                    .bearer_format("JWT")
+                    .build(),
+            ),
+        )
+        .build();
+    OpenApiBuilder::new()
+        .info(
+            InfoBuilder::new()
+                .title("data-avalanche management API")
+                .version(env!("CARGO_PKG_VERSION"))
+                .description(Some(
+                    "`globals.api.auth` decides which credentials a node takes: none, basic \
+                     (one user, admin), or a bearer JWT whose roles are mapped (config §6.3).",
+                ))
+                .build(),
+        )
+        .paths(paths)
+        .components(Some(components))
+        .build()
+}
+
+pub(crate) async fn serve() -> Response {
+    let doc = serde_json::to_value(openapi()).expect("the document serializes");
+    body(StatusCode::OK, &doc)
+}
+
 #[cfg(test)]
 mod tests {
     use serde_json::Value;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-api`

Expected: PASS, 26 tests.

```text
test result: ok. 26 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1236 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-api/Cargo.toml \
  crates/data-avalanche-api/openapi.json \
  crates/data-avalanche-api/src/lib.rs \
  crates/data-avalanche-api/src/openapi.rs
git commit -m "feat(api): the OpenAPI document from the route table, served at /v1/openapi.json and snapshot-tested"
```

---

### Task 8: The E2E scenarios (`data-avalanche` tests)

**Files:**
- Modify: `Cargo.lock`
- Modify: `crates/data-avalanche/Cargo.toml`
- Create: `crates/data-avalanche/tests/e2e/api.rs`
- Modify: `crates/data-avalanche/tests/e2e/cluster.rs`
- Modify: `crates/data-avalanche/tests/e2e/harness.rs`
- Create: `crates/data-avalanche/tests/e2e/idp.rs`
- Modify: `crates/data-avalanche/tests/e2e/lifecycle.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`
- Modify: `test-infra/README.md`

**Interfaces:**
- Consumes: the binary of Tasks 3–7.
- Produces (test-only):
  - `Options.api`, `globals.api` entries; `E2e::trust(CertificateDer)`: the nodes started after
    it are reached over TLS;
  - `Proc::{send, authorize, call, ask, shows, answers}`: a request with a bearer and a body, the
    token of the later requests, a JSON call, a pipeline action, a wait for an actual state, a
    status code;
  - `idp.rs`: `Idp::{start, api, token}` (an ES256 key and its JWKS on loopback) and
    `tls_files(dir) -> (String, CertificateDer)` (a certificate for `127.0.0.1`).

The lifecycle scenario now drives pause, stop and resume through the API instead of
writing the desired state itself. Three new scenarios:
- `api.rs`: one node over TLS with `auth: jwt`. No token is 401 with the bearer challenge, a
  viewer reads and may not pause, an operator pauses; a reload that maps `ops` to operator takes
  effect on the running node.
- `cluster.rs`, `any_nodes_api_drives_the_holder`: pause, resume, restart and flush through nodes
  that don't hold the pipeline; the restart keeps the lease and the epoch.
- `cluster.rs`, `a_leave_through_the_api_hands_off_and_stays_up`: the leaver's pipelines move, it
  stays live and not ready and answers; the shutdown exits 0.

- [ ] **Step 1: Write the tests**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index fc07699..c098a10 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1691,6 +1691,7 @@ version = "0.1.0"
 dependencies = [
  "arrow-array",
  "arrow-cast",
+ "base64 0.22.1",
  "chrono",
  "clap",
  "data-avalanche-api",
@@ -1707,14 +1708,17 @@ dependencies = [
  "data-avalanche-testkit",
  "flate2",
  "futures-util",
+ "jsonwebtoken",
  "mysql_async",
  "object_store 0.14.2",
+ "rcgen",
  "rustls",
  "serde_json",
  "tempfile",
  "thiserror 2.0.21",
  "tokio",
  "tokio-postgres",
+ "tokio-rustls",
  "tracing",
  "tracing-subscriber",
 ]
```

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
index e50cdb0..d255d5c 100644
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -38,12 +38,16 @@ e2e-hooks = []
 [dev-dependencies]
 arrow-array.workspace = true
 arrow-cast.workspace = true
+base64.workspace = true
 data-avalanche-testkit.workspace = true
 flate2.workspace = true
 futures-util.workspace = true
+jsonwebtoken.workspace = true
 mysql_async.workspace = true
+rcgen.workspace = true
 tempfile.workspace = true
 tokio-postgres.workspace = true
+tokio-rustls.workspace = true
 
 [[test]]
 name = "e2e"
```

Create `crates/data-avalanche/tests/e2e/api.rs`:

```rust
//! The management API with its credentials (M9a): TLS on the listener, bearer tokens from an
//! issuer's JWKS, the roles they carry, and a `role_mapping` a reload swaps in.

use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, Proc, eventually};
use crate::idp::{Idp, tls_files};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

/// The status code of a request with `token`.
async fn code(p: &Proc, token: Option<&str>, method: &str, path: &str) -> u16 {
    p.send(token, method, path, "")
        .await
        .unwrap_or_else(|| panic!("{method} {path} answers"))
        .0
}

/// Over TLS, a request without a token is 401 with the bearer challenge, a viewer reads and
/// may not pause, an operator pauses; a reload that maps `ops` to the operator role takes
/// effect on the running node without a restart.
#[tokio::test]
async fn the_api_takes_tls_and_tokens_and_a_reload_of_its_roles() {
    require_it!();
    let idp = Idp::start().await;
    let dir = tempfile::tempdir().expect("a temp dir");
    let (tls, cert) = tls_files(dir.path());
    let api = format!("{}{tls}", idp.api(""));
    let mut e = E2e::new(
        "api_tls",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            api: &api,
            ..Options::default()
        },
    )
    .await;
    e.trust(cert);
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    let id = e.test.clone();
    let path = format!("/v1/pipelines/{id}");
    let pause = format!("{path}/pause");

    let (status, head, _) = p.send(None, "GET", &path, "").await.expect("answers");
    assert_eq!(status, 401);
    assert!(
        head.to_ascii_lowercase()
            .contains("www-authenticate: bearer realm=\"data-avalanche\""),
        "{head}"
    );
    assert_eq!(code(&p, Some("not.a.token"), "GET", &path).await, 401);
    let viewer = idp.token(&["viewer"]);
    let operator = idp.token(&["operator"]);
    assert_eq!(code(&p, Some(&viewer), "GET", &path).await, 200);
    assert_eq!(code(&p, Some(&viewer), "POST", &pause).await, 403);
    assert_eq!(code(&p, Some(&operator), "POST", &pause).await, 202);
    p.authorize(&viewer);
    p.shows(&id, "paused").await;
    assert_eq!(
        code(&p, Some(&operator), "POST", &format!("{path}/resume")).await,
        202
    );
    p.shows(&id, "running").await;

    // `role_mapping` follows a reload (R): `ops` is the operator role, and the three names
    // themselves no longer count.
    let mapped = format!("{}{tls}", idp.api(", role_mapping: { operator: [ops] }"));
    e.write_config(&e.config().replace(&api, &mapped));
    p.authorize(&operator);
    let (status, v) = p.reload().await;
    assert_eq!(status, 202, "{v}");
    let ops = idp.token(&["ops"]);
    eventually(
        "the reload's role mapping applies",
        Duration::from_secs(30),
        async || code(&p, Some(&viewer), "GET", &path).await == 403,
    )
    .await;
    assert_eq!(code(&p, Some(&operator), "POST", &pause).await, 403);
    assert_eq!(code(&p, Some(&ops), "POST", &pause).await, 202);
    p.authorize(&ops);
    p.shows(&id, "paused").await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
```

Change `crates/data-avalanche/tests/e2e/cluster.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/cluster.rs b/crates/data-avalanche/tests/e2e/cluster.rs
index 62648ec..af17625 100644
--- a/crates/data-avalanche/tests/e2e/cluster.rs
+++ b/crates/data-avalanche/tests/e2e/cluster.rs
@@ -3,7 +3,8 @@
 //! spread over them. A crash (`kill -9`), a leave (SIGTERM), a partition (the forwarders drop a
 //! side's packets) and a stop-the-world pause (SIGSTOP) each end with every pipeline held by one
 //! live node, every row in the lake, and owner epochs that never go back in the Delta log. A
-//! reload on one node reaches the three (Clustering §9.2).
+//! reload on one node reaches the three (Clustering §9.2). Any node's API drives a pipeline its
+//! holder runs, and a leave through the API hands off and keeps the process up (M9a).
 
 use std::collections::BTreeMap;
 use std::time::{Duration, Instant};
@@ -11,6 +12,7 @@ use std::time::{Duration, Instant};
 use crate::forwarder::Forwarder;
 use data_avalanche_cluster::Ring;
 use data_avalanche_testkit::require_it;
+use serde_json::json;
 
 use crate::harness::{E2e, Options, Proc, eventually, free_udp_port};
 
@@ -145,6 +147,13 @@ impl Cluster {
         self.nodes.iter().filter(|n| n.proc.is_some()).collect()
     }
 
+    fn proc(&self, name: &str) -> &Proc {
+        self.nodes[self.index(name)]
+            .proc
+            .as_ref()
+            .expect("a live node")
+    }
+
     fn index(&self, name: &str) -> usize {
         self.nodes
             .iter()
@@ -526,3 +535,124 @@ async fn a_reload_reaches_every_node() {
     );
     c.stop().await;
 }
+
+/// The API of nodes that don't hold the test's pipeline pauses, resumes, restarts and flushes
+/// it: the holder takes each at its next poll, a restart under the lease it holds, and any node
+/// reads the pipeline's status from storage.
+#[tokio::test]
+async fn any_nodes_api_drives_the_holder() {
+    require_it!();
+    let mut c = Cluster::new("cluster_api").await;
+    c.start_all().await;
+    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
+    c.landed().await;
+    let id = c.e.test.clone();
+    let (holder, epoch) = c.holders().await[&id].clone();
+    let others: Vec<&Proc> = c
+        .nodes
+        .iter()
+        .filter(|n| n.name != holder)
+        .map(|n| n.proc.as_ref().unwrap())
+        .collect();
+    let (via, reader) = (others[0], others[1]);
+
+    assert_eq!(via.ask(&id, "pause").await.0, 202);
+    reader.shows(&id, "paused").await;
+    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
+    tokio::time::sleep(Duration::from_secs(3)).await;
+    assert_eq!(c.e.ids("items").await, [1], "nothing is read while paused");
+    assert_eq!(
+        via.ask(&id, "flush").await,
+        (200, json!({ "desired_state": "paused", "unchanged": true }))
+    );
+    assert_eq!(via.ask(&id, "resume").await.0, 202);
+    reader.shows(&id, "running").await;
+    c.e.converged("items").await;
+
+    let (code, v) = via.ask(&id, "restart").await;
+    assert_eq!(code, 202, "{v}");
+    let held = c.proc(&holder);
+    eventually(
+        "the holder restarts it",
+        Duration::from_secs(30),
+        async || held.log().contains("restarted on request"),
+    )
+    .await;
+    c.e.exec("INSERT INTO items VALUES (3, 'c')").await;
+    assert_eq!(via.ask(&id, "flush").await.0, 202);
+    eventually(
+        "the holder flushes it",
+        Duration::from_secs(30),
+        async || held.log().contains("flushed on request"),
+    )
+    .await;
+    c.e.converged("items").await;
+    reader.shows(&id, "running").await;
+    let v = reader.call("GET", &format!("/v1/pipelines/{id}")).await.1;
+    assert_eq!(
+        (&v["holder"]["node"], &v["holder"]["owner_epoch"]),
+        (&json!(holder), &json!(epoch)),
+        "the same lease: {v}"
+    );
+    assert_eq!(c.holders().await[&id], (holder.clone(), epoch));
+
+    let (code, v) = reader.call("GET", "/v1/cluster/status").await;
+    assert_eq!(code, 200, "{v}");
+    assert_eq!(v["members"].as_array().map(Vec::len), Some(3), "{v}");
+    let row = v["pipelines"]
+        .as_array()
+        .unwrap()
+        .iter()
+        .find(|p| p["id"] == json!(id))
+        .cloned();
+    assert_eq!(
+        row,
+        Some(json!({ "id": id, "holder": holder, "owner_epoch": epoch }))
+    );
+    c.landed().await;
+    c.stop().await;
+}
+
+/// `POST /v1/cluster/leave` hands the node's pipelines off without a lease wait and keeps the
+/// process up: live, not ready, still answering; `POST /v1/admin/shutdown` then ends it with
+/// the exit code 0.
+#[tokio::test]
+async fn a_leave_through_the_api_hands_off_and_stays_up() {
+    require_it!();
+    let mut c = Cluster::new("cluster_leave").await;
+    c.start_all().await;
+    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
+    c.landed().await;
+    let (leaver, _) = c.holders().await[&c.e.test].clone();
+    let i = c.index(&leaver);
+    let mut p = c.nodes[i].proc.take().unwrap();
+
+    assert_eq!(
+        p.call("POST", "/v1/cluster/leave").await,
+        (202, json!({ "leaving": true }))
+    );
+    c.settled().await;
+    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
+    c.landed().await;
+    assert!(
+        c.holders().await.values().all(|(h, _)| *h != leaver),
+        "{leaver} holds nothing"
+    );
+    assert_eq!(p.answers("/health/live").await, Some(200));
+    assert_eq!(p.answers("/health/ready").await, Some(503));
+    p.metric("data_avalanche_owned_pipelines 0").await;
+    assert_eq!(p.call("POST", "/v1/cluster/leave").await.0, 409);
+    assert!(
+        p.log()
+            .contains("left the cluster; the process stays up until it is stopped"),
+        "{}",
+        p.log()
+    );
+
+    assert_eq!(
+        p.call("POST", "/v1/admin/shutdown").await,
+        (202, json!({ "shutting_down": true }))
+    );
+    assert!(p.exit().await.success(), "{}", p.log());
+    c.stop().await;
+}
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index 21c7651..4e2ba53 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -5,6 +5,7 @@
 
 use std::path::PathBuf;
 use std::process::{Child, Command, ExitStatus, Stdio};
+use std::sync::Arc;
 use std::time::{Duration, Instant};
 
 use arrow_array::cast::AsArray;
@@ -22,7 +23,9 @@ use data_avalanche_testkit::endpoints::{
 };
 use data_avalanche_testkit::{delete_prefix, stores, unique_prefix};
 use futures_util::TryStreamExt;
-use tokio::io::{AsyncReadExt, AsyncWriteExt};
+use rustls::pki_types::{CertificateDer, ServerName};
+use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
+use tokio_rustls::TlsConnector;
 
 pub use crate::source::{Db, Source};
 
@@ -82,6 +85,8 @@ pub struct Options<'a> {
     /// `@BUCKET@` are replaced, `@INCLUDE@` by the pattern of every table of the test, and a line
     /// `@SOURCE(<suffix>)@` by the source block ([`E2e::source_block`]).
     pub pipelines: &'a str,
+    /// `globals.api` entries beyond the address, e.g. `, auth: jwt, jwt: { … }`.
+    pub api: &'a str,
 }
 
 /// The rows [`E2e::insert_series`] gives a key `g`.
@@ -107,6 +112,14 @@ pub struct E2e {
     /// cluster's generation, not its own file).
     template: String,
     pub db: Db,
+    /// How the tests reach each node's API.
+    client: Client,
+}
+
+/// How a test reaches a node's API: plain HTTP, or TLS trusting one certificate.
+#[derive(Clone, Default)]
+struct Client {
+    tls: Option<TlsConnector>,
 }
 
 impl E2e {
@@ -168,6 +181,7 @@ impl E2e {
             dir: tempfile::tempdir().expect("a temp dir"),
             template: String::new(),
             db,
+            client: Client::default(),
         };
         e.template = e.yaml(&o);
         std::fs::write(e.config_path(), &e.template).expect("the config file");
@@ -258,7 +272,7 @@ impl E2e {
 globals:
   cloud: aws
   state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
-  api: {{ address: "127.0.0.1:${{E2E_API_PORT}}" }}
+  api: {{ address: "127.0.0.1:${{E2E_API_PORT}}"{api} }}
   log: {{ level: info, format: compact }}
   cluster:
     lease_duration: 6s
@@ -283,6 +297,7 @@ pipelines:
             pipeline = o.pipeline,
             cluster = o.cluster.replace(", ", "\n    "),
             memberlist = o.memberlist,
+            api = o.api,
         )
     }
 
@@ -339,9 +354,26 @@ pipelines:
             child: cmd.spawn().expect("the binary starts"),
             port,
             log,
+            client: self.client.clone(),
+            token: None,
         }
     }
 
+    /// The nodes started from now on serve their API with TLS (`globals.api.tls`), on a
+    /// certificate for `127.0.0.1` the tests trust.
+    pub fn trust(&mut self, cert: CertificateDer<'static>) {
+        let mut roots = rustls::RootCertStore::empty();
+        roots.add(cert).expect("the certificate");
+        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
+            rustls::crypto::aws_lc_rs::default_provider(),
+        ))
+        .with_safe_default_protocol_versions()
+        .expect("TLS versions")
+        .with_root_certificates(roots)
+        .with_no_client_auth();
+        self.client.tls = Some(TlsConnector::from(Arc::new(config)));
+    }
+
     pub fn table(&self, name: &str) -> TableRef {
         TableRef::new(&self.schema, name)
     }
@@ -862,6 +894,17 @@ pub struct Proc {
     child: Child,
     port: u16,
     log: PathBuf,
+    client: Client,
+    /// The bearer token of [`Self::call`].
+    token: Option<String>,
+}
+
+/// One HTTP/1.1 exchange on `s`: the answer's text.
+async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, request: &[u8]) -> Option<String> {
+    s.write_all(request).await.ok()?;
+    let mut out = String::new();
+    s.read_to_string(&mut out).await.ok()?;
+    Some(out)
 }
 
 impl Proc {
@@ -873,23 +916,79 @@ impl Proc {
         self.request("GET", path).await
     }
 
-    /// A request without a body: the status code and the body.
+    /// A request without a body, with the token of [`Self::authorize`]: the status code and
+    /// the body.
     async fn request(&self, method: &str, path: &str) -> Option<(u16, String)> {
-        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
+        let (code, _, body) = self.send(self.token.as_deref(), method, path, "").await?;
+        Some((code, body))
+    }
+
+    /// A request with `token` as its bearer and `body` (YAML) as its body: the status code,
+    /// the head and the body. `None` when nothing answers.
+    pub async fn send(
+        &self,
+        token: Option<&str>,
+        method: &str,
+        path: &str,
+        body: &str,
+    ) -> Option<(u16, String, String)> {
+        let auth = token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
+        let request = format!(
+            "{method} {path} HTTP/1.1\r\nHost: t\r\n{auth}Content-Type: application/yaml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
+            body.len()
+        );
+        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
             .await
             .ok()?;
-        s.write_all(
-            format!(
-                "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
-            )
-            .as_bytes(),
-        )
-        .await
-        .ok()?;
-        let mut out = String::new();
-        s.read_to_string(&mut out).await.ok()?;
+        let out = match &self.client.tls {
+            None => exchange(tcp, request.as_bytes()).await?,
+            Some(tls) => {
+                let name = ServerName::IpAddress(std::net::Ipv4Addr::LOCALHOST.into());
+                exchange(tls.connect(name, tcp).await.ok()?, request.as_bytes()).await?
+            }
+        };
         let (head, body) = out.split_once("\r\n\r\n")?;
-        Some((head.get(9..12)?.parse().ok()?, body.to_owned()))
+        Some((
+            head.get(9..12)?.parse().ok()?,
+            head.to_owned(),
+            body.to_owned(),
+        ))
+    }
+
+    /// [`Self::call`] and the other requests carry `token` as their bearer from now on.
+    pub fn authorize(&mut self, token: &str) {
+        self.token = Some(token.to_owned());
+    }
+
+    /// A request without a body: the status code and the JSON body.
+    pub async fn call(&self, method: &str, path: &str) -> (u16, serde_json::Value) {
+        let (code, body) = self
+            .request(method, path)
+            .await
+            .unwrap_or_else(|| panic!("{method} {path} answers"));
+        let v = serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}"));
+        (code, v)
+    }
+
+    /// `GET path`'s status code, `None` when nothing answers.
+    pub async fn answers(&self, path: &str) -> Option<u16> {
+        self.get(path).await.map(|(code, _)| code)
+    }
+
+    /// `POST /v1/pipelines/<id>/<action>`: the status code and the JSON body.
+    pub async fn ask(&self, id: &str, action: &str) -> (u16, serde_json::Value) {
+        self.call("POST", &format!("/v1/pipelines/{id}/{action}"))
+            .await
+    }
+
+    /// Waits until `GET /v1/pipelines/<id>` shows `actual` as the pipeline's actual state: what
+    /// its owner wrote in `status.json`, under the live lease.
+    pub async fn shows(&self, id: &str, actual: &str) {
+        let what = format!("{id} shows {actual}");
+        eventually(&what, Duration::from_secs(60), async || {
+            self.call("GET", &format!("/v1/pipelines/{id}")).await.1["actual_state"] == actual
+        })
+        .await;
     }
 
     /// Waits for `/health/ready`.
@@ -920,22 +1019,14 @@ impl Proc {
 
     /// `POST /v1/config/reload`: the status code and the JSON body.
     pub async fn reload(&self) -> (u16, serde_json::Value) {
-        let (code, body) = self
-            .request("POST", "/v1/config/reload")
-            .await
-            .expect("the reload answers");
-        let v = serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}"));
-        (code, v)
+        self.call("POST", "/v1/config/reload").await
     }
 
     /// `GET /v1/config/status`'s JSON body.
     pub async fn config_status(&self) -> serde_json::Value {
-        let (code, body) = self
-            .get("/v1/config/status")
-            .await
-            .expect("the status answers");
-        assert_eq!(code, 200, "{body}");
-        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}"))
+        let (code, v) = self.call("GET", "/v1/config/status").await;
+        assert_eq!(code, 200, "{v}");
+        v
     }
 
     /// Waits until the metrics have a line equal to `line`.
```

Create `crates/data-avalanche/tests/e2e/idp.rs`:

```rust
//! The API's credentials for a scenario, generated at run time: a token issuer for `auth: jwt`
//! (one ES256 key, its JWKS served on loopback) and a self-signed certificate for
//! `globals.api.tls`.

use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
use rustls::pki_types::CertificateDer;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

/// Issues the tokens a node with [`Idp::api`] takes.
pub struct Idp {
    issuer: String,
    key: KeyPair,
    served: JoinHandle<()>,
}

impl Idp {
    /// Serves the JWKS, at any path, on a loopback port.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
        // An uncompressed P-256 point: 0x04, then x and y.
        let raw = key.public_key_raw();
        let jwks = json!({ "keys": [{
            "kty": "EC", "crv": "P-256", "use": "sig", "alg": "ES256", "kid": "k1",
            "x": URL_SAFE_NO_PAD.encode(&raw[1..33]),
            "y": URL_SAFE_NO_PAD.encode(&raw[33..65]),
        }] })
        .to_string();
        let served = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else {
                    return;
                };
                let jwks = jwks.clone();
                tokio::spawn(async move {
                    let mut head = [0; 4096];
                    let _ = s.read(&mut head).await;
                    let answer = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{jwks}",
                        jwks.len()
                    );
                    let _ = s.write_all(answer.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        Self {
            issuer,
            key,
            served,
        }
    }

    /// `globals.api` entries: `auth: jwt` for this issuer, audience `cdc`, the roles in the
    /// `roles` claim; `more` adds entries (a `role_mapping`).
    pub fn api(&self, more: &str) -> String {
        format!(
            r#", auth: jwt, jwt: {{ issuer: "{i}", audience: cdc, jwks_url: "{i}/jwks", roles_claim: roles }}{more}"#,
            i = self.issuer
        )
    }

    /// A token for `alice` with `roles`, valid ten minutes.
    pub fn token(&self, roles: &[&str]) -> String {
        let now = jsonwebtoken::get_current_timestamp();
        let claims = json!({
            "iss": self.issuer, "aud": "cdc", "sub": "alice", "exp": now + 600, "roles": roles,
        });
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some("k1".into());
        jsonwebtoken::encode(
            &header,
            &claims,
            &EncodingKey::from_ec_der(&self.key.serialize_der()),
        )
        .unwrap()
    }
}

impl Drop for Idp {
    fn drop(&mut self) {
        self.served.abort();
    }
}

/// A certificate for `127.0.0.1` and its key, written to `dir`: the `globals.api` entry that
/// serves them, and the certificate to trust.
pub fn tls_files(dir: &Path) -> (String, CertificateDer<'static>) {
    let c = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let (cert, key) = (dir.join("api.crt"), dir.join("api.key"));
    std::fs::write(&cert, c.cert.pem()).unwrap();
    std::fs::write(&key, c.signing_key.serialize_pem()).unwrap();
    let entry = format!(
        r#", tls: {{ enabled: true, cert: "{}", key: "{}" }}"#,
        cert.display(),
        key.display()
    );
    (entry, c.cert.der().clone())
}
```

Change `crates/data-avalanche/tests/e2e/lifecycle.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/lifecycle.rs b/crates/data-avalanche/tests/e2e/lifecycle.rs
index aefc9be..5e22a2b 100644
--- a/crates/data-avalanche/tests/e2e/lifecycle.rs
+++ b/crates/data-avalanche/tests/e2e/lifecycle.rs
@@ -3,7 +3,6 @@
 
 use std::time::Duration;
 
-use data_avalanche_state::DesiredState;
 use data_avalanche_testkit::require_it;
 
 use crate::harness::{E2e, Options, eventually};
@@ -18,7 +17,7 @@ fn never() -> Options<'static> {
 }
 
 /// Pause holds the stream with the source connection open; stop closes it and is kept across a
-/// restart; resume reads on from where each left off.
+/// restart; resume reads on from where each left off. Each is asked through the API.
 #[tokio::test]
 async fn pause_stop_and_resume_hold_across_a_restart() {
     require_it!();
@@ -39,11 +38,9 @@ async fn pause_stop_and_resume_hold_across_a_restart() {
     e.exec("INSERT INTO items VALUES (1, 'a')").await;
     e.converged("items").await;
 
-    e.docs()
-        .set_desired_state(DesiredState::Paused)
-        .await
-        .expect("paused");
+    assert_eq!(p.ask(&e.test, "pause").await.0, 202);
     p.metric(&e.status("paused")).await;
+    p.shows(&e.test, "paused").await;
     e.exec("INSERT INTO items VALUES (2, 'b')").await;
     tokio::time::sleep(Duration::from_secs(3)).await;
     assert_eq!(e.ids("items").await, [1], "nothing is read while paused");
@@ -52,17 +49,11 @@ async fn pause_stop_and_resume_hold_across_a_restart() {
         "a paused pipeline keeps the source warm"
     );
 
-    e.docs()
-        .set_desired_state(DesiredState::Running)
-        .await
-        .expect("running");
+    assert_eq!(p.ask(&e.test, "resume").await.0, 202);
     p.metric(&e.status("running")).await;
     e.converged("items").await;
 
-    e.docs()
-        .set_desired_state(DesiredState::Stopped)
-        .await
-        .expect("stopped");
+    assert_eq!(p.ask(&e.test, "stop").await.0, 202);
     p.metric(&e.status("stopped")).await;
     eventually(
         "the stream is closed",
@@ -76,6 +67,12 @@ async fn pause_stop_and_resume_hold_across_a_restart() {
     let mut p = e.start(&[]);
     p.ready().await;
     p.metric(&e.status("stopped")).await;
+    p.shows(&e.test, "stopped").await;
+    assert_eq!(
+        p.ask(&e.test, "pause").await.0,
+        409,
+        "a stopped pipeline is resumed, not paused"
+    );
     tokio::time::sleep(Duration::from_secs(3)).await;
     assert_eq!(
         e.ids("items").await,
@@ -84,10 +81,7 @@ async fn pause_stop_and_resume_hold_across_a_restart() {
     );
     assert!(!e.stream_open().await);
 
-    e.docs()
-        .set_desired_state(DesiredState::Running)
-        .await
-        .expect("running");
+    assert_eq!(p.ask(&e.test, "resume").await.0, 202);
     p.metric(&e.status("running")).await;
     e.converged("items").await;
     assert!(p.terminate().await.success());
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index 7e5d64b..befeae7 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -6,11 +6,13 @@
 //!
 //! `cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`
 
+mod api;
 mod cluster;
 mod crashes;
 mod forwarder;
 mod guards;
 mod harness;
+mod idp;
 mod lifecycle;
 mod mysql;
 mod proxy;
```

- [ ] **Step 2: Build the suite**

Run: `cargo build -q -p data-avalanche --features e2e-hooks --tests`

Expected: it builds. The behaviour is Tasks 3–7's, so the scenarios pass on arrival (Step 4).

- [ ] **Step 3: Document the suite**

Change `test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
index cab6d44..9390816 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -100,8 +100,11 @@ and SIGSTOP the nodes. They need no service beyond the source and RustFS.
 
 The reload scenarios (`cluster.rs`'s `a_reload_reaches_every_node`, and `reload.rs`) rewrite that
 one config file and call `POST /v1/config/reload` on one node; the others adopt the new config
-generation from `globals.state`, and `GET /v1/config/status` reports each node's. The endpoints
-are unauthenticated until M9.
+generation from `globals.state`, and `GET /v1/config/status` reports each node's. Those
+scenarios run the API with `auth: none`. The management API scenarios (M9a) need nothing more:
+`api.rs` serves the API over TLS with a certificate generated at run time and checks bearer
+tokens against a JWKS the test serves on loopback (`idp.rs`); `cluster.rs` drives a pipeline
+through a node that doesn't hold it, and leaves and shuts a node down through the API.
 
 ## GCS: a real bucket, not an emulator
 
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 api:: lifecycle::pause_stop cluster::any_nodes cluster::a_leave_through`

Expected: PASS, 4 tests.

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 38 filtered out; finished in 93.00s
```

Run: `DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 api:: lifecycle::pause_stop cluster::any_nodes cluster::a_leave_through`

Expected: PASS, 4 tests.

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 38 filtered out; finished in 106.45s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1236 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche/Cargo.toml \
  crates/data-avalanche/tests/e2e/api.rs \
  crates/data-avalanche/tests/e2e/cluster.rs \
  crates/data-avalanche/tests/e2e/harness.rs \
  crates/data-avalanche/tests/e2e/idp.rs \
  crates/data-avalanche/tests/e2e/lifecycle.rs \
  crates/data-avalanche/tests/e2e/main.rs \
  test-infra/README.md
git commit -m "test(e2e): the API over TLS with tokens and a reloaded role mapping; any node drives the holder; a leave through the API stays up and a shutdown exits"
```

---
## Exit

- [x] Every `/v1` route of System-Overview's table except the snapshot jobs (M9b) answers on any node, behind its least role; the matrix test turns away every caller below it, 401 vs 403 (Tasks 4–7).
- [x] Auth `none`, `basic`, `jwt` and `oauth2` with `role_mapping`, and the auth follows a reload (Tasks 4, 8).
- [x] TLS on the listener (Tasks 3, 8).
- [x] Pause, stop and resume move the desired state, with `409` where Lifecycle §3 forbids; restart and flush are taken by the holder under its lease (Tasks 1, 2, 5, 8).
- [x] The pipelines list and status come from storage, with the holder, the position and the halted tables (Task 5).
- [x] Config validate publishes nothing and returns no resolved config; cluster status names the members and the holders; a leave stays up, a shutdown exits 0 (Tasks 6, 8).
- [x] The OpenAPI document is snapshot-tested, read back by utoipa, and checked for every route's role, security scheme and common answers (Task 7). (Reworded at the whole-branch review: the test doesn't validate against the OAS 3.1 schema, DA-API-18.)
- [ ] The E2E suite passes on PostgreSQL 18 and, through `ci.yml`'s jobs, on `mysql-84`, `mysql-84-nogtid` and `mariadb-118`. (Measured locally on PostgreSQL 18 and `mysql-84`, 42/42 each at 9a4e5f0; `mysql-84-nogtid` and `mariadb-118` run in the PR's CI.)
- [x] Every Review Focus line has its passing test.
- [x] The documents of "Documents this plan changes" are updated, and the approved spec edits applied.
- [x] The workspace gates pass: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`, `cargo test --workspace`.

## Deviations during execution

Recorded from the SDD ledger. Each task's code was applied as verified. Every change below came
from a review, and each fix is its own commit after the task's. From Task 5 on, the plan's
`pub use auth::{…}` context line lacks Task 4's `KEYS_MAX_AGE`: that one line was applied by hand
in Tasks 6 and 7, and no other hunk was adapted.

| Task | Commit | Deviation |
|---|---|---|
| 2 | aeab120 | A reload's `Update::Restart` at a new state location takes the status and request baselines anew (`reported` reset, `seen` read from the new documents): the plan kept the old location's, so the new location got no status until it changed, and a request already there would have been taken. Test `a_reload_restart_at_new_state_writes_its_status_there`. |
| 4 | f729d9f | jsonwebtoken checks `iss` and `aud` only when a token carries them, so a token without them passed. Now `iss` is always required, and `aud` whenever an audience is configured. While no key set was ever fetched the answer is 503 throughout (it was 401 "no key" within `REFETCH_AFTER` of a failed first fetch). The key set has a maximum age, `KEYS_MAX_AGE` = 10 min: an older set is fetched again, and a failed refetch keeps the old keys, retried at most every `REFETCH_AFTER` (D-M9a.6 as built). `Auth::with_timing` (crate-private) sets the times in tests; `Mode::Token` boxes its verifier; the test issuer gained `remove_key` and `set_down`. |
| 6 | 5005b82, 4e6e498 | The plan's run loop built the signal future inside its `select!` on every pass, so a SIGTERM that arrived during an API-asked leave was lost (tokio delivers a signal to the listeners registered when its driver runs) and the process hung until SIGKILL. The stop sources are now one `Stops`, built before the loop, its signal future pinned across passes and polled first (`biased`), so the first wait registers the listeners. `Control::ask` is public for the binary's test. Test `a_signal_during_a_leave_still_stops_the_process` sends SIGTERM to the test process during a leave and yields to the runtime before it waits again; without the yield the bug's shape passed it (4e6e498). |

**The whole-branch review** (1b97efc..e76d55a): no Critical. Five Important findings, all fixed
before the PR in one fix wave, together with the cheap minors the review triaged as "fix in this
PR". The scoped re-review found every finding addressed and no new Critical or Important breakage.

| Commit | Fix |
|---|---|
| 1d71c9a | **Validate of a posted body is no longer an oracle for a running secret.** A `Secret` serializes as its keyed fingerprint and the diff lists every path whose value differs, so a body repeating a guess at a secret's path told by its absence from `changes` that the guess was right. With a body, two fingerprints (`blake3:` strings, secrets and file fingerprints alike) now compare equal: `diff_with(…, Fingerprints::Ignored)` in the config crate, `ConfigCoordinator::check_posted` (as named after 620fc41; `check` always compares), and `"secrets_compared": false` in the answer. Validate of the node's own file compares them as before. Test `a_check_of_a_body_does_not_tell_whether_its_secret_is_the_running_one`, which also replaces a non-leak assertion that couldn't fail. |
| d56b7cc | **A JWKS fetch no longer holds the key cache.** The fetch held the cache's lock, so any caller with a made-up `kid` (no credentials needed) made a hanging IdP stall every bearer verification for up to `FETCH_TIMEOUT` once per `REFETCH_AFTER`. Now one fetch runs at a time in its own task, outside the lock. A caller whose key is cached verifies at once, even from an old set while its refetch runs; a caller whose key isn't cached waits for that one fetch. A `kid` missing after a failed (or throttled) refetch answers 503, not 401. Tests: `old_keys_verify_at_once_while_the_issuer_hangs`, `a_made_up_key_id_does_not_hold_up_a_good_token`, `a_key_missing_after_a_failed_refetch_is_unavailable`, `after_a_failed_refetch_an_unknown_key_id_waits_the_refetch_interval`, `without_an_audience_a_token_without_aud_verifies`; the test issuer gained a hang mode. |
| 768fc1b | **A reload keeps the cached keys unless their source changes** (the issuer, `jwks_url` or `discovery_url`). A `role_mapping`, `roles_claim` or `audience` change used to start with no keys, so a reload during an IdP outage made every bearer caller a 503. Test `a_reload_of_the_role_mapping_keeps_the_keys`. |
| 5ac4975 | **`roles_claim` names the claim by its literal key first**, then as a dotted path: Auth0's namespaced claims (`https://example.com/roles`) were split at their dots and never found. Test `a_namespaced_roles_claim_is_read_by_its_name`. |
| dafe228 | Auth settings that don't hold together fail closed (every request 503, logged), where they used to fall open to `none`. `nbf` is checked (60 s leeway). `basic` compares BLAKE3 digests in constant time. The key-fetch 503 names no URL or error (logged at warn). The gate test requires the handler's own 503 body. |
| 67d87c5 | `save_status` keeps a stored status of a higher `owner_epoch`, so a stalled former owner can't overwrite its successor's for the rest of the term. The 202s of leave and shutdown name the node. `check` loads the body outside the coordinator's lock. "Restarted on request" is logged only when a worker was discarded. A stopped TLS listener lets its port go without waiting for a connection, and a cert PEM with no certificate is refused at bind. The paused-flush test asserts that no request was written; one helper reads the node's file for validate and reload. |
| 9a4e5f0 | 67d87c5's guard made the holder's-own-earlier-term case of Task 5's status test a refused write, so it passed for the wrong reason. The case is its own test now, `a_holder_s_status_of_an_earlier_term_is_not_this_term_s`, which fails with the epoch check removed from `actual()`. |

**After the PR was opened** (2026-10-24, on Oleksii's request): the final re-review's DA-API-19,
found outside the fix diff, fixed in the same PR.

| Commit | Fix |
|---|---|
| 8d9be8d | **Validate of a posted body takes a pipeline's `id` as written.** It used to expand `${VAR}` in a posted body's `id` and echo the result in `changes` (`Added { id }`, `pipelines.<id>.…`), and derive a MySQL `server_id` from it, so an operator could read any node environment variable that fits V-055's pattern. Now `load_posted_str` refuses a templated pipeline `id` or `defaults.id` with V-055 at its path, showing only the written text, before any rule runs. `ConfigCoordinator::check_posted` uses it; the node-file validate and the reload keep `load_str`. Tests `a_posted_body_s_templated_id_is_refused_without_its_value` (fails with the posted path wired to `load_str`, showing the value) and the config crate's posted-id tests. The review tried aliases, merge keys, `defaults`, partial templates, quoting, tags, several documents and a sequence of pipelines: none gets through. |
| 620fc41 | The MySQL posted-id test sets up the V-120 clash it guards against (it fails by showing the derived number with the refusal removed), the `$${` escape is tested, and `check` no longer takes `Fingerprints`: only `check_posted` ignores them. |

**Reworded, not built:** the Exit's "the OpenAPI document is validated" (and roadmap §4 M9's).
Task 7's test reads the document back through utoipa and checks every route's role, security
scheme and common answers. It doesn't validate against the OAS 3.1 schema: that needs a vendored
schema and a new dependency, so it is a follow-up (DA-API-18).

Left for later:
- **New gaps** (gap tracker §41.1):
  - DA-API-13: a crashed holder reads as live until takeover;
  - DA-API-14: a left node keeps its last generation's auth;
  - DA-API-15: a retried restart or flush call asks twice;
  - DA-API-16: the JWKS fetch accepts `http`, follows redirects and reads an unbounded body;
  - DA-API-17: no connection limit on the TLS listener;
  - DA-API-18: OAS 3.1 schema validation;
  - DA-API-20: validate's verdicts on a posted body's other `${VAR}` leaves (V-063, the
    uniqueness rules, pattern and range checks, the diff's paths) answer yes or no about a node
    variable, one request at a time; nothing is echoed. (DA-API-19, the echoed id, is fixed above.)
- From the re-review, in the fix wave's code:
  - a panic inside the spawned JWKS fetch would leave it marked in flight, so no fetch would run
    again until a restart;
  - an externally deleted lease restarts at epoch 1, and `save_status` then keeps the old,
    higher-epoch status until the new epoch passes it (the sink's epoch fence trips in that
    scenario anyway).
- The review's other minors:
  - `/v1/pipelines` and `/v1/cluster/status` read one pipeline at a time, and one storage error
    fails the whole list;
  - a flush taken while no worker runs is a logged no-op, answered 202 (Lifecycle §3 says so);
  - a crashed holder's 202s are absorbed by the next term's baseline (DA-API-13);
  - the snapshot test prints no diff on a mismatch, and a crate version bump needs a bless;
  - the per-route answer lists in the OpenAPI document are kept by hand (D-M9a.9);
  - a `kid` present but undecodable counts as found until `KEYS_MAX_AGE`;
  - no test of a status or requests document at an unsupported version, of a failed status write
    retried at the next poll, of the 503 paths of validate and the pipeline routes, or of the
    `position.json` (MySQL) position branch;
  - the E2E's TLS client panics instead of retrying a node briefly not answering.

**Counts.** The workspace runs more tests than the plan says from Task 2 on (Task 2's fix +1,
Task 4's +2, Task 6's +1): 1213, 1214, 1227, 1231, 1237, 1240 and 1240 after Tasks 2–8, against
the plan's 1212, 1213, 1224, 1228, 1233, 1236 and 1236; after the whole-branch review's fixes,
1254; after DA-API-19's fix, 1257.

**Measured at the end of execution.** At 9a4e5f0 (the whole-branch review's fixes), the full E2E
suite passed 42/42 on PostgreSQL 18 (530.89 s) and on MySQL 8.4 (477.34 s), with no stray
process. The workspace runs 1254 tests, and the gates are clean (`cargo fmt --all --check`, both
clippy runs). The fix wave's run of the four API scenarios on PostgreSQL 18 passed 4/4. At e76d55a
(Task 8), the four API scenarios passed on PostgreSQL 18 (99.67 s) and MySQL 8.4 (98.78 s).
`mysql-84-nogtid` and `mariadb-118` run in the PR's CI.
At 620fc41 (DA-API-19's fix), the workspace runs 1257 tests and the gates are clean; the change is
confined to validate of a posted body, which the E2E suite doesn't drive.
