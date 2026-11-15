---
updated: 2026-11-14
domain: Architecture
tags: [v1.1, implementation-plan, m13a, console, events, audit-trail, attribution, api, cluster, config]
---

# M13a — The Console's Events and Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The first server milestone of the 1.1.0 admin console (Admin_Console §14), its
foundations before any page:
- `globals.console` and the console's `oauth2` sign-in fields parse, resolve and validate
  (V-128…V-136); nothing serves `/ui/` yet;
- one durable timeline per cluster under `globals.state` (§6), written by every node and readable
  from any, expired by `globals.console.events.retention`;
- every mutating API call records who made it (name and role, from the server's verification), an
  optional `note`, and how it ended; a call refused for its role (`403`) is recorded too (§5, §6);
- the cluster records what it does on its own: lease hand-offs and failovers, pipeline errors and
  recoveries, halts and their clearing, schema changes, snapshot job ends, members joining,
  leaving and going suspect, config generations, removals (§6);
- `GET /v1/events` lists the timeline, newest first, through filters and pages;
- the documentation site describes it, and an alert watches the events a node drops.

**Architecture:**

- **Config** (Task 1, `data-avalanche-config`): the raw and resolved `globals.console`
  (`enabled`, `session_key`, `public_url`, `session_ttl`, `username_claim`, `history.retention`,
  `events.retention`, `prometheus`), the `oauth2` fields the console's sign-in needs
  (`client_id`, `client_secret`, `scopes`, `idp_name`), their field docs and mutability, and the
  rules V-128…V-136 exactly as Configuration_Specification states them.
- **The store** (Task 2, `data-avalanche-state`, `data-avalanche-storage`): one small document per
  event, `events/<YYYY-MM-DD>/<HHMMSS.ffffff>Z-<seq:06>-<node>.json`. Object storage has no append;
  a key orders by time, so a day lists in order and expires whole. `VersionedStore::folders`
  lists a prefix's next segments (the days) in one delimiter listing.
- **The recorder** (Task 3, `data-avalanche-cluster`, `data-avalanche-telemetry`): whatever
  records an event queues it without waiting (1024 at most); one task per node writes them,
  retrying a failing store at 200 ms, 1 s and 5 s, then dropping and counting. Another task
  deletes the expired days hourly. Two node counters: written and dropped.
- **Attribution** (Task 4, `data-avalanche-api`): a middleware inside the role gate on each
  mutating route reads the optional `note` from the JSON body (`422` past 500 characters, before
  the route runs), runs the route, and records the outcome (`accepted`, `unchanged`, `refused`).
  The role gate records a `403` on those routes as `action.denied`, at most 30 at once and one a
  second after. A snapshot job keeps its note. `GET /v1/events` is the read side.
- **The cluster's own events** (Task 5, `data-avalanche-cluster`, `-pipeline`, `-core`): each owner
  records its lease (`Claimed::Mine` names the node a failover took it from), the state changes its
  status writes show, the halts its workers add or clear against the halts recorded when its term
  began, the snapshot jobs that end, applied and degraded schema changes (a new
  `Observer::schema_evolved`), and its removals. The coordinator records a publish once and a
  refusal once per generation and node. A membership change is recorded by one node: the first by
  name of the active, unsuspected members other than the one that changed.
- **The site** (Task 6): a page for the timeline, the counters and an alert on dropped events.
- **The E2E suite** (Task 7): three nodes; a pause with a note, a kill of the holder, and the
  timeline read back from a survivor.

**Tech Stack:** Rust 1.100.0 (edition 2024). No new crate: the state crate takes `futures-util`
from its dev-dependencies into its dependencies, and the API crate takes the workspace's `url`
(form decoding of the query); `Cargo.lock` gains the one `url` edge.

**Spec:**
- `doc/research/Admin_Console.md`: §5 (actions and attribution), §6 (events and audit trail), §11
  (configuration surface), §12 (server work), §13 (left to the builder: the event layout, the
  endpoint paths, the milestone split), §14 (one 1.1.0 release, several milestones).
- `doc/specs/System-Overview.md`: §Management REST API (every mutating call recorded, with an
  optional `note`), §Web console, §Management API endpoints, §Observability catalogue.
- `doc/specs/Configuration_Specification.md`: `globals.console`, `globals.api.oauth2` (with
  `client_id`, `client_secret`, `scopes`, `idp_name`), V-128…V-136.
- `doc/research/API_RBAC_and_Versioning.md` §2 (roles; reads are viewer).
- `doc/research/Clustering_Architecture_Research.md` (membership, suspicion, the lease and its
  takeover), `doc/research/Pipeline_Lifecycle_State_Machine.md` (actual states).
- `doc/v1.0/M9a-management-api.md` (the route table, the OpenAPI snapshot), `M9b-snapshot-jobs.md`
  (the jobs the owner follows), `M8b-config-generations.md` (publish and adopt).

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`; `telemetry` beside them. `testkit` is a dev-dependency only.
- **Recording never waits and never fails what it records** (Admin_Console §8's anti-goal): an
  action's answer doesn't depend on the event's write; an event that can't be written is dropped,
  logged and counted.
- **Rolling upgrade from 1.0.x:** a 1.0 reader refuses unknown fields (`deny_unknown_fields`), so
  a new field of an existing state document is written only when set (`SnapshotJobDoc::note`).
  The events are new documents no 1.0 node reads. Nothing in M13a changes the gossip.
- **The name and role are the server's**, from the verified principal (`anonymous` without
  authentication), never from the request.
- **Time in tests:** the recorder's and the controller's tests run on tokio's paused clock
  (`start_paused = true`); the API's tests and the E2E suite run in real time.
- **Test gates:** plain `cargo test` stays hermetic (`InMemory` stores, the testkit's fake source
  and sink). The E2E suite needs `DATA_AVALANCHE_IT=1` (`test-infra/up.sh`) and the `e2e-hooks`
  feature, and runs with `--test-threads=1`. The alert rules run under promtool in CI's check job.
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`): the generated
  reference (`cargo xtask docs`) and the pages Task 6 writes.
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `thiserror` types; no new error type is needed.
- Code blocks are the verified content, already `rustfmt`-formatted. Every block is a `git
  apply`-ready diff, one per file; a step's diffs apply in the order given, after the earlier
  steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

The inputs most likely to hurt a person running 1.1.0 in production, each pinned by a test in the
task after the arrow.

1. **The state bucket throttles or fails** (a 503 storm, a credentials rotation). Pausing a
   pipeline must still answer at once and work; events are retried, then dropped and counted,
   never queued without bound → Task 3 (`a_failing_store_is_retried_then_the_event_lands`,
   `a_store_that_keeps_failing_drops_the_event_and_counts_it`, `a_full_queue_drops_without_waiting`).
2. **A script hammers a mutating route with a viewer token.** Every call gets `403`; the timeline
   records a burst of 30 and then one a second, not one object per call → Task 4
   (`a_flood_of_refused_calls_is_recorded_for_a_burst_only`, `a_caller_below_the_role_is_recorded_as_denied`).
3. **A note that is too long, not a string, or sent beside a route's own body** (a snapshot's
   tables). Too long or not a string is `422` before the route runs and changes nothing; beside the
   route's fields it is taken and the route reads its own → Task 4
   (`a_note_is_optional_trimmed_and_bounded`, `a_bad_note_is_refused_before_the_route_runs`,
   `a_snapshot_job_keeps_its_note_and_the_event_names_it`).
4. **A failing source in a retry loop, a takeover, a restart.** The halts every new worker reloads,
   a pause carried over to a new owner, and each rerun of a failing worker must not record again;
   a new halt, an error and the recovery must → Task 5
   (`the_halt_watch_records_new_and_cleared_halts_against_the_recorded`,
   `the_state_watch_records_failures_and_recoveries_not_reruns`,
   `the_owner_records_its_lease_halts_jobs_and_errors`).
5. **A node dies in a three-node cluster.** Both survivors see it go; the timeline must say so
   once, name the node the takeover came from, and be readable from any survivor → Task 5
   (`one_member_records_each_membership_change`), Task 7
   (`the_timeline_records_who_paused_and_a_failover`).

## Decisions and rulings

Settled with Oleksii before this plan: the console is switched on in the config
(`globals.console.enabled`, default off) and served under `/ui/`; the Helm chart and the site are
updated with it; refused attempts are recorded (mockup round, 2026-11-14); the API address goes
into the gossip with a version bump (M13b). The rest are Claude's calls under Admin_Console §13;
Oleksii approved the plan with them, and the spec patch S-M13a.1, on 2026-11-14.

- **D-M13a.1 The milestones of 1.1.0.** M13a (this plan: config, events, attribution,
  `GET /v1/events`); M13b (the owner-served pipeline detail and per-table figures, source detail,
  all tables, all snapshot jobs, job cancel and retry with the "copy cancelled" halt, node leave and
  shutdown through any node, config generations and diff with each node's file status, process and
  gossip figures, the gossip's `api_url` and file hash under the MAC label's v2); M13c (history,
  the Prometheus reader, the SSE stream); M14 (sign-in and sessions); M15 (the frame and Overview,
  serving `/ui/`, the chart's console values and NOTES); M16 (the detail pages and actions).
  Nothing is tagged until M16 merges.
- **D-M13a.2 The layout.** One document per event (`Document` kind `event`, version 1) at
  `events/<day>/<HHMMSS.ffffff>Z-<seq:06>-<escaped node>.json`. The sequence is the node's, and a
  key taken already (two events in one microsecond) takes the next. A listing reads the days newest
  first, a day's keys in reverse, and at most 5000 documents (`MAX_SCANNED`); a page that stops
  early returns its last key as `next`. Expiry deletes whole days older than now minus the
  retention, on every node hourly (a day deleted twice is no error); an event lives at least the
  retention and at most a day more.
- **D-M13a.3 Never waited on.** A bounded queue (1024) with `try_send`; one writer task per node;
  retries 200 ms, 1 s, 5 s; then the event is dropped. Each drop is logged and counted
  (`data_avalanche_events_dropped_total`). A crash loses the queue.
- **D-M13a.4 The note and the outcome.** `note` is an optional string field of a JSON object body,
  trimmed, at most 500 characters (`422` otherwise), read beside any field the route takes. The
  outcome follows the answer: `2xx` is `accepted` (`unchanged` when the answer says so), `409` and
  `422` are `refused` (with the error in `detail.error`); any other answer (an unknown pipeline, a
  node still starting) records nothing. The event is `action.<object>.<verb>`
  (`action.pipeline.pause`, `action.snapshot.start`, `action.config.reload`, `action.node.leave`,
  `action.node.shutdown`) at the node that answered, its pipeline from the path's `id`, its object
  the job, the generation or the node.
- **D-M13a.5 Refused attempts.** The role gate records a `403` on a mutating route as
  `action.denied` (warning), naming the path and the role it needs, within `RateLimit::denials()`
  (30, then one a second, per node); past it, the attempt is logged only.
- **D-M13a.6 Who records what.** The owner records its pipeline's events; the coordinator records a
  publish where it happened, and a generation a node can't apply at that node, once per
  generation. A membership change is recorded only by a node active in both views, and only by the
  first by name of the active, unsuspected members other than the subject; views converge, so one
  node records it (a view that hasn't may record it twice or not at all, accepted).
- **D-M13a.7 What is news to an owner.** States: `Starting` is passed over, so a rerun after a
  backoff shows no change; the first state of a term is recorded only when it is `Error` or
  `Retrying`; `Running` is recorded only back from `Error`, `Retrying`, `Paused` or `Stopped`.
  Halts: `halts.json` read when the term starts is the baseline (unread, the first streaming
  worker's halts are taken as it); a table in a worker's `halted` not in the baseline is
  `table.halted`; one missing once a worker streams is `table.resumed`. A schema change the sink
  refuses halts the table and is recorded as that halt, not as a schema event.
- **D-M13a.8 `GET /v1/events`** (viewer): `since`, `until` (RFC 3339), `kind` (repeatable or comma
  separated; a family matches its members: `action` matches `action.pipeline.pause`), `object` (a
  pipeline id or an object's name), `actor` (exact), `node`, `q` (text or note, ignoring case),
  `limit` (1–1000, 100 by default), `after`. A parameter given twice (but `kind`), an unknown one, or
  `since` not before `until` is `400`.
- **D-M13a.9 The chart in M13a.** The chart passes the config through as written, so the console's
  keys need no chart change now; its values comments and NOTES for `/ui/` come with M15, which
  serves it.
- **D-M13a.10 The alert.** `DataAvalancheEventsDropped`: any drop in 15 minutes, a warning; the
  events page is its runbook.

### Spec patch for approval (S-M13a.1)

Approved by Oleksii and applied 2026-11-14 (49e5ee4): these three edits to
`doc/specs/System-Overview.md`, which the code below implements.

1. The endpoint table, after "List snapshot jobs":

   `| Events | GET /v1/events | viewer | the cluster's timeline (actions with their caller, role, note and outcome; refused attempts; the cluster's own events), newest first; filters since, until, kind (a kind or a family), object, actor, node, q; limit 1–1000 (100); a page that stopped early names its next, passed back as after. Kept for globals.console.events.retention |`

2. The Management REST API bullet "every mutating call is recorded…" gains: "; the `note` is a
   string field of the call's JSON body, at most 500 characters (`422` otherwise); a call refused
   for its role (`403`) is recorded too".

3. The observability catalogue gains a row:
   `| Events | data_avalanche_events_written_total, data_avalanche_events_dropped_total (the node's writes to the cluster's timeline; no labels) |`

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-config/src/{raw,walk,resolved,build,mutability,field_docs,registry}.rs`, `rules/{domain,auth}.rs` | `globals.console`, the `oauth2` sign-in fields, their docs and mutability, V-128…V-136 |
| `crates/data-avalanche-config/tests/{load,rules/auth,rules/domain}.rs` | Their defaults, secrets and rule cases |
| `xtask/src/config.rs`, `docs/src/content/docs/reference/{configuration,validation-rules}.md` | The reference's blocks, regenerated |
| `crates/data-avalanche-storage/src/store.rs`, `tests/versioned_store.rs` | `VersionedStore::folders` |
| `crates/data-avalanche-state/src/events.rs` (new), `lib.rs`, `Cargo.toml` | `EventDoc`, `Actor`, `EventSeverity`, `EventQuery`, `EventPage`, `Events::{record, list, prune}` |
| `crates/data-avalanche-telemetry/src/lib.rs`, `tests/{metrics,catalogue}.rs` | The two event counters |
| `crates/data-avalanche-cluster/src/events.rs` (new), `lib.rs` | `Recorder`, `expire_events`; `StateWatch`, `HaltWatch`, `member_changes`, `records`, `record_members` |
| `crates/data-avalanche-api/src/{actions,events}.rs` (new), `lib.rs`, `limit.rs`, `snapshots.rs`, `openapi.rs`, `openapi.json`, `test_support.rs`, `Cargo.toml` | The recording middleware, the `403` record, `GET /v1/events`, the note of a job |
| `crates/data-avalanche-state/src/snapshot.rs` | `SnapshotJobDoc::note` |
| `xtask/src/api.rs`, `docs/src/content/docs/reference/api.md`, `docs/public/openapi.json` | The API reference: who did it, the body's note, query parameters |
| `crates/data-avalanche-core/src/classify.rs` | `Display for SchemaChange` |
| `crates/data-avalanche-pipeline/src/{observe,schema_change,session,test_support,worker}.rs` | `Observer::schema_evolved` and its call |
| `crates/data-avalanche-cluster/src/{claim,controller,jobs,coordinator,test_support}.rs` | `Claimed::Mine::taken_from`; the owner's and the coordinator's events; `Jobs::poll` returns the ended jobs |
| `crates/data-avalanche/src/run.rs` | The node's store, recorder, expiry and membership tasks, wired |
| `deploy/prometheus/{alerts,alerts.test}.yaml` | `DataAvalancheEventsDropped` |
| `docs/src/content/docs/deployment/{events,observability}.md` | The events page; the counters and the alert |
| `crates/data-avalanche/tests/e2e/{events,main}.rs` | The three-node timeline scenario |

---

### Task 1: The console's configuration (`data-avalanche-config`)

**Files:**
- Modify: `crates/data-avalanche-config/src/{raw,walk,resolved,build,mutability,field_docs,registry}.rs`, `src/rules/{domain,auth}.rs`
- Modify: `xtask/src/config.rs`, `crates/data-avalanche-api/src/auth.rs` (its tests' `ApiOauth2` literals)
- Regenerate: `docs/src/content/docs/reference/{configuration,validation-rules}.md` (`cargo xtask docs`)
- Test: `crates/data-avalanche-config/tests/{load,rules/auth,rules/domain}.rs`

**Interfaces:**
- Consumes: the config crate's raw/resolved/walk pattern and `build.rs` helpers (M1), the rule
  registry, `FIELD_DOCS`, the mutability table.
- Produces (re-exported at the crate root):
  - `Globals::console: Console`; `Console { enabled: bool, session_key: Option<Secret>, public_url:
    Option<String>, session_ttl: Duration, username_claim: String, history: Retention, events:
    Retention, prometheus: Option<Prometheus> }`; `Retention { retention: Duration }`;
  - `Prometheus { url, auth: PrometheusAuth, headers: BTreeMap<String, Secret>, ca: Option<Pem>,
    extra_labels: BTreeMap<String, String>, timeout: Duration }`; `PrometheusAuth { None, Basic { user,
    password: Secret }, Bearer { token: Secret } }`;
  - `ApiOauth2` gains `client_id: Option<String>`, `client_secret: Option<Secret>`, `scopes:
    Vec<String>` (default `[openid, profile]`), `idp_name: Option<String>`;
  - defaults: `session_ttl` 8 h, `username_claim` `preferred_username`, history 24 h, events 30 d,
    Prometheus timeout 5 s; `globals.console` is restart-free (`R`).

V-128…V-136 are the spec's text and ranges (V-132: `session_ttl` 5 min–7 d, history 1 h–30 d,
events 1 d–400 d; V-134 a warning). V-126's path also covers `globals.console.prometheus.headers`;
the registry holds 130 rules.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/auth.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/auth.rs b/crates/data-avalanche-api/src/auth.rs
index cf4a0e9..dd1f6b2 100644
--- a/crates/data-avalanche-api/src/auth.rs
+++ b/crates/data-avalanche-api/src/auth.rs
@@ -688,6 +688,10 @@ mod tests {
                 jwks_url: None,
                 discovery_url: None,
                 roles_claim: "roles".into(),
+                client_id: None,
+                client_secret: None,
+                scopes: Vec::new(),
+                idp_name: None,
             });
             api
         };
@@ -887,6 +891,10 @@ mod tests {
             jwks_url: None,
             discovery_url: Some(format!("{}/.well-known/openid-configuration", idp.url)),
             roles_claim: "roles".into(),
+            client_id: None,
+            client_secret: None,
+            scopes: Vec::new(),
+            idp_name: None,
         };
         api.oauth2 = Some(oauth2(&idp.url));
         let token = bearer(&idp.token(json!(["operator"])));
```

`crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index 7c52ad5..52293f7 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1081,7 +1081,7 @@ mod tests {
 
     #[test]
     fn has_every_spec_rule_once_in_order() {
-        assert_eq!(RULES.len(), 121);
+        assert_eq!(RULES.len(), 130);
         assert!(
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
```

`crates/data-avalanche-config/tests/load.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/load.rs b/crates/data-avalanche-config/tests/load.rs
index 0ebcfe5..85aeefa 100644
--- a/crates/data-avalanche-config/tests/load.rs
+++ b/crates/data-avalanche-config/tests/load.rs
@@ -126,6 +126,24 @@ fn applies_spec_defaults() {
             headers: Default::default(),
         }
     );
+    assert_eq!(
+        g.console,
+        Console {
+            enabled: false,
+            session_key: None,
+            public_url: None,
+            session_ttl: Duration::from_secs(8 * 3600),
+            username_claim: "preferred_username".into(),
+            history: Retention {
+                retention: Duration::from_secs(24 * 3600)
+            },
+            events: Retention {
+                retention: Duration::from_secs(30 * 24 * 3600)
+            },
+            prometheus: None,
+        },
+        "config §6.8's defaults: the console off, its records kept 24 h and 30 days"
+    );
     let p = &cfg.pipelines["orders"];
     assert_eq!(p.source.ssl_mode, SslMode::Require);
     assert_eq!(p.source.timeouts.snapshot, Duration::from_secs(6 * 3600));
@@ -1192,3 +1210,44 @@ fn a_posted_templated_mysql_id_derives_no_server_id() {
     assert!(!shown.contains("derived"), "{shown}");
     assert!(!shown.contains("sekret-value-1"), "{shown}");
 }
+
+/// Config §6.8: the console's secrets resolve as secrets (shown by fingerprint), Prometheus takes
+/// its bearer token and labels, and the oauth2 sign-in fields keep their defaults.
+#[test]
+fn console_settings_resolve_with_their_secrets_hidden() {
+    let yaml = MINIMAL.replace(
+        "globals:\n  state: { bucket: app-state }",
+        "globals:\n  state: { bucket: app-state }\n  api:\n    auth: oauth2\n    oauth2: { issuer: https://idp, jwks_url: https://idp/certs, client_id: cdc-console }\n  console:\n    enabled: true\n    session_key: ${KEY}\n    public_url: https://cdc.example.com\n    prometheus:\n      url: https://prom\n      bearer_token: ${PROM}\n      headers: { x-scope-orgid: shop }\n      extra_labels: { cluster: prod-eu }",
+    );
+    let key = "k".repeat(32);
+    let cfg = load_str(
+        &yaml,
+        &env(&[("PW", "pw"), ("KEY", key.as_str()), ("PROM", "pr0m")]),
+        &fp_key(),
+    )
+    .unwrap_or_else(|r| panic!("{r:#?}"))
+    .config;
+    let c = &cfg.globals.console;
+    assert!(c.enabled);
+    assert_eq!(
+        c.session_key.as_ref().map(|k| k.expose()),
+        Some(key.as_str())
+    );
+    let p = c.prometheus.as_ref().expect("prometheus");
+    assert_eq!(p.url, "https://prom");
+    match &p.auth {
+        PrometheusAuth::Bearer { token } => assert_eq!(token.expose(), "pr0m"),
+        other => panic!("{other:?}"),
+    }
+    assert_eq!(p.extra_labels["cluster"], "prod-eu");
+    assert_eq!(p.timeout, Duration::from_secs(5));
+    let o = cfg.globals.api.oauth2.as_ref().expect("oauth2");
+    assert_eq!(o.client_id.as_deref(), Some("cdc-console"));
+    assert_eq!(o.scopes, ["openid", "profile"]);
+    assert_eq!(o.idp_name, None);
+    let shown = serde_json::to_string(&cfg.globals.console).unwrap();
+    assert!(
+        !shown.contains(&key) && !shown.contains("pr0m") && shown.contains("blake3:"),
+        "{shown}"
+    );
+}
```

`crates/data-avalanche-config/tests/rules/auth.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/auth.rs b/crates/data-avalanche-config/tests/rules/auth.rs
index 28880f1..1b62857 100644
--- a/crates/data-avalanche-config/tests/rules/auth.rs
+++ b/crates/data-avalanche-config/tests/rules/auth.rs
@@ -168,5 +168,87 @@ pub fn cases() -> Vec<Case> {
                 "role_mapping": {"admin": ["x"]}
             }))
         }),
+        case("V-133", Error, "client_secret without client_id", || {
+            pg_delta().set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "jwks_url": "https://idp/certs", "client_secret": "s"}}))
+        }),
+        case("V-133", Error, "scopes without client_id", || {
+            pg_delta().set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "jwks_url": "https://idp/certs", "scopes": ["openid"]}}))
+        }),
+        case("V-133", Silent, "a confidential console client", || {
+            pg_delta().set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "jwks_url": "https://idp/certs", "client_id": "cdc-console", "client_secret": "s", "scopes": ["openid", "email"]}}))
+        }),
+        case("V-134", Warning, "a session key without auth", || {
+            pg_delta().set(
+                "/globals/console",
+                json!({"session_key": "0123456789abcdef0123456789abcdef"}),
+            )
+        }),
+        case("V-134", Warning, "a session key under jwt", || {
+            pg_delta()
+                .set("/globals/api", json!({"auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"}}))
+                .set("/globals/console", json!({"enabled": true, "session_key": "0123456789abcdef0123456789abcdef"}))
+        }),
+        case("V-134", Silent, "a session key under basic", || {
+            pg_delta()
+                .set(
+                    "/globals/api",
+                    json!({"auth": "basic", "user": "a", "password": "b"}),
+                )
+                .set(
+                    "/globals/console",
+                    json!({"enabled": true, "session_key": "0123456789abcdef0123456789abcdef"}),
+                )
+        }),
+        case(
+            "V-135",
+            Error,
+            "the console under basic without a session key",
+            || {
+                pg_delta()
+                    .set(
+                        "/globals/api",
+                        json!({"auth": "basic", "user": "a", "password": "b"}),
+                    )
+                    .set("/globals/console", json!({"enabled": true}))
+            },
+        ),
+        case("V-135", Silent, "a disabled console under basic", || {
+            pg_delta()
+                .set(
+                    "/globals/api",
+                    json!({"auth": "basic", "user": "a", "password": "b"}),
+                )
+                .set("/globals/console", json!({"enabled": false}))
+        }),
+        case("V-135", Silent, "the console under jwt", || {
+            pg_delta()
+                .set("/globals/api", json!({"auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"}}))
+                .set("/globals/console", json!({"enabled": true}))
+        }),
+        case(
+            "V-136",
+            Error,
+            "the console under oauth2 without public_url",
+            || {
+                pg_delta()
+                .set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "jwks_url": "https://idp/certs", "client_id": "c"}}))
+                .set("/globals/console", json!({"enabled": true, "session_key": "0123456789abcdef0123456789abcdef"}))
+            },
+        ),
+        case(
+            "V-136",
+            Error,
+            "the console under oauth2 without client_id",
+            || {
+                pg_delta()
+                .set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "jwks_url": "https://idp/certs"}}))
+                .set("/globals/console", json!({"enabled": true, "session_key": "0123456789abcdef0123456789abcdef", "public_url": "https://cdc.example.com"}))
+            },
+        ),
+        case("V-136", Silent, "the console under oauth2", || {
+            pg_delta()
+                .set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "jwks_url": "https://idp/certs", "client_id": "c", "idp_name": "Company SSO"}}))
+                .set("/globals/console", json!({"enabled": true, "session_key": "0123456789abcdef0123456789abcdef", "public_url": "https://cdc.example.com"}))
+        }),
     ]
 }
```

`crates/data-avalanche-config/tests/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/domain.rs b/crates/data-avalanche-config/tests/rules/domain.rs
index 724b144..1c062b5 100644
--- a/crates/data-avalanche-config/tests/rules/domain.rs
+++ b/crates/data-avalanche-config/tests/rules/domain.rs
@@ -281,6 +281,75 @@ pub fn cases() -> Vec<Case> {
                 json!({"otlp_endpoint": "https://otel:4318", "headers": {"authorization": "Bearer a\nx-evil: 1"}}),
             )
         }),
+        case("V-126", Error, "a prometheus header with a newline", || {
+            pg_delta().set(
+                "/globals/console/prometheus",
+                json!({"url": "https://prom", "headers": {"x-scope-orgid": "a\nb"}}),
+            )
+        }),
+        case("V-128", Error, "a short session key", || {
+            pg_delta().set("/globals/console/session_key", json!("too-short"))
+        }),
+        case("V-128", Silent, "a 32-byte session key", || {
+            pg_delta().set(
+                "/globals/console/session_key",
+                json!("0123456789abcdef0123456789abcdef"),
+            )
+        }),
+        case("V-129", Error, "a public_url with a query", || {
+            pg_delta().set(
+                "/globals/console/public_url",
+                json!("https://cdc.example.com/?x=1"),
+            )
+        }),
+        case("V-129", Error, "a public_url without a scheme", || {
+            pg_delta().set("/globals/console/public_url", json!("cdc.example.com"))
+        }),
+        case("V-129", Silent, "a public_url under a path", || {
+            pg_delta().set(
+                "/globals/console/public_url",
+                json!("https://example.com/cdc"),
+            )
+        }),
+        case("V-130", Error, "a prometheus url without a scheme", || {
+            pg_delta().set("/globals/console/prometheus", json!({"url": "prom:9090"}))
+        }),
+        case("V-130", Silent, "a prometheus url", || {
+            pg_delta().set(
+                "/globals/console/prometheus",
+                json!({"url": "http://prometheus.observability:9090"}),
+            )
+        }),
+        case("V-131", Error, "basic and a bearer token", || {
+            pg_delta().set("/globals/console/prometheus", json!({"url": "https://prom", "basic": {"user": "u", "password": "p"}, "bearer_token": "t"}))
+        }),
+        case("V-131", Error, "a reserved label", || {
+            pg_delta().set(
+                "/globals/console/prometheus",
+                json!({"url": "https://prom", "extra_labels": {"__name__": "x"}}),
+            )
+        }),
+        case("V-131", Error, "a label with a dash", || {
+            pg_delta().set(
+                "/globals/console/prometheus",
+                json!({"url": "https://prom", "extra_labels": {"k8s-cluster": "x"}}),
+            )
+        }),
+        case("V-131", Silent, "a bearer token and labels", || {
+            pg_delta().set("/globals/console/prometheus", json!({"url": "https://prom", "bearer_token": "t", "extra_labels": {"cluster": "prod-eu", "_env": "p"}}))
+        }),
+        case("V-132", Error, "a session_ttl of a minute", || {
+            pg_delta().set("/globals/console/session_ttl", json!("1m"))
+        }),
+        case("V-132", Error, "a history kept 31 days", || {
+            pg_delta().set("/globals/console/history", json!({"retention": "31d"}))
+        }),
+        case("V-132", Error, "events kept 12 hours", || {
+            pg_delta().set("/globals/console/events", json!({"retention": "12h"}))
+        }),
+        case("V-132", Silent, "the bounds", || {
+            pg_delta().set("/globals/console", json!({"session_ttl": "7d", "history": {"retention": "1h"}, "events": {"retention": "400d"}}))
+        }),
         case("V-126", Silent, "a bearer header", || {
             pg_delta().set(
                 "/globals/tracing",
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`

Expected: FAIL. The first errors, as measured:

```text
error[E0422]: cannot find struct, variant or union type `Console` in this scope
   --> crates/data-avalanche-config/tests/load.rs:131:9
error[E0422]: cannot find struct, variant or union type `Retention` in this scope
   --> crates/data-avalanche-config/tests/load.rs:137:22
error[E0609]: no field `console` on type `&Globals`
   --> crates/data-avalanche-config/tests/load.rs:130:11
error[E0609]: no field `console` on type `Globals`
   --> crates/data-avalanche-config/tests/load.rs:1230:26
error[E0609]: no field `client_id` on type `&ApiOauth2`
   --> crates/data-avalanche-config/tests/load.rs:1245:18
error[E0609]: no field `scopes` on type `&ApiOauth2`
   --> crates/data-avalanche-config/tests/load.rs:1246:18
…
```

- [ ] **Step 3: Implement**

`crates/data-avalanche-config/src/build.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/build.rs b/crates/data-avalanche-config/src/build.rs
index 48c3965..6098e09 100644
--- a/crates/data-avalanche-config/src/build.rs
+++ b/crates/data-avalanche-config/src/build.rs
@@ -266,9 +266,10 @@ fn read_or_report<T>(
 
 // ---- globals ----
 
-/// `globals.tracing.headers`, each value a secret. The structural pass builds none (no
-/// fingerprint key), though each value's template is still checked.
-fn tracing_headers(
+/// A map whose values are secrets (`globals.tracing.headers`, `globals.console.prometheus.headers`).
+/// The structural pass builds none (no fingerprint key), though each value's template is still
+/// checked.
+fn secret_map(
     ctx: &mut Ctx,
     headers: &Option<BTreeMap<String, Leaf<String>>>,
     at: &At,
@@ -287,6 +288,90 @@ fn tracing_headers(
     out
 }
 
+/// An optional secret. The structural pass builds none (no fingerprint key), though the value's
+/// template is still checked.
+fn secret(ctx: &mut Ctx, leaf: &L<String>, at: &At, key: &str) -> Option<Option<Secret>> {
+    let Some(l) = leaf else { return Some(None) };
+    let text = ctx.text(l, &at.key(key))?;
+    ctx.fingerprint_key().map(|k| Some(Secret::new(text, k)))
+}
+
+/// `globals.console` (§6.8).
+fn console(c: &RawConsole, at: &At, ctx: &mut Ctx) -> Option<Console> {
+    let enabled = or(ctx, &c.enabled, at, "enabled", false);
+    let session_key = secret(ctx, &c.session_key, at, "session_key");
+    let public_url = opt(ctx, &c.public_url, at, "public_url");
+    let session_ttl = or(
+        ctx,
+        &c.session_ttl,
+        at,
+        "session_ttl",
+        Duration::from_secs(8 * 3600),
+    );
+    let username_claim = or(
+        ctx,
+        &c.username_claim,
+        at,
+        "username_claim",
+        "preferred_username".to_string(),
+    );
+    let mut retention = |r: &Option<RawRetention>, key: &str, default: Duration| {
+        let r = r.clone().unwrap_or_default();
+        or(ctx, &r.retention, &at.key(key), "retention", default)
+            .map(|retention| Retention { retention })
+    };
+    let history = retention(&c.history, "history", Duration::from_secs(24 * 3600));
+    let events = retention(&c.events, "events", Duration::from_secs(30 * 24 * 3600));
+    let prometheus = match &c.prometheus {
+        None => Some(None),
+        Some(p) => prometheus(p, &at.key("prometheus"), ctx).map(Some),
+    };
+    Some(Console {
+        enabled: enabled?,
+        session_key: session_key?,
+        public_url: public_url?,
+        session_ttl: session_ttl?,
+        username_claim: username_claim?,
+        history: history?,
+        events: events?,
+        prometheus: prometheus?,
+    })
+}
+
+fn prometheus(p: &RawPrometheus, at: &At, ctx: &mut Ctx) -> Option<Prometheus> {
+    let url = req(ctx, &p.url, at, "url");
+    let basic = p.basic.as_ref().map(|b| {
+        let at = at.key("basic");
+        let user = req(ctx, &b.user, &at, "user");
+        let password = secret(ctx, &b.password, &at, "password");
+        Some((user?, password??))
+    });
+    let token = secret(ctx, &p.bearer_token, at, "bearer_token");
+    let headers = secret_map(ctx, &p.headers, &at.key("headers"));
+    let tls = p.tls.clone().unwrap_or_default();
+    let ca = pem(ctx, &tls.ca, &at.key("tls"), "ca", PemKind::Certificates);
+    let extra_labels = map(ctx, &p.extra_labels, at, "extra_labels");
+    let timeout = or(ctx, &p.timeout, at, "timeout", Duration::from_secs(5));
+    // V-131 reports `basic` with `bearer_token`; the result keeps neither then.
+    let auth = match (basic, token?) {
+        (None, None) => PrometheusAuth::None,
+        (Some(b), None) => {
+            let (user, password) = b?;
+            PrometheusAuth::Basic { user, password }
+        }
+        (None, Some(token)) => PrometheusAuth::Bearer { token },
+        (Some(_), Some(_)) => return None,
+    };
+    Some(Prometheus {
+        url: url?,
+        auth,
+        headers: headers?,
+        ca: ca?,
+        extra_labels: extra_labels?,
+        timeout: timeout?,
+    })
+}
+
 fn globals(g: &RawGlobals, ctx: &mut Ctx) -> Option<Globals> {
     let at = At::config(YamlPath::root().key("globals"));
     let cloud = or(ctx, &g.cloud, &at, "cloud", Cloud::Aws);
@@ -319,9 +404,14 @@ fn globals(g: &RawGlobals, ctx: &mut Ctx) -> Option<Globals> {
         "sample_ratio",
         ratio_default,
     );
-    let headers = tracing_headers(ctx, &tracing.headers, &tracing_at.key("headers"));
+    let headers = secret_map(ctx, &tracing.headers, &tracing_at.key("headers"));
     let azure = g.azure.clone().unwrap_or_default();
     let account_name = opt(ctx, &azure.account_name, &at.key("azure"), "account_name");
+    let console = console(
+        &g.console.clone().unwrap_or_default(),
+        &at.key("console"),
+        ctx,
+    );
     Some(Globals {
         cloud: cloud?,
         state: GlobalState {
@@ -342,6 +432,7 @@ fn globals(g: &RawGlobals, ctx: &mut Ctx) -> Option<Globals> {
         azure: Azure {
             account_name: account_name?,
         },
+        console: console?,
     })
 }
 
@@ -388,12 +479,23 @@ fn api(a: &RawApi, at: &At, ctx: &mut Ctx) -> Option<Api> {
         let jwks_url = opt(ctx, &o.jwks_url, &at, "jwks_url");
         let discovery_url = opt(ctx, &o.discovery_url, &at, "discovery_url");
         let roles_claim = or(ctx, &o.roles_claim, &at, "roles_claim", "roles".to_string());
+        let client_id = opt(ctx, &o.client_id, &at, "client_id");
+        let client_secret = secret(ctx, &o.client_secret, &at, "client_secret");
+        let scopes = match &o.scopes {
+            None => Some(vec!["openid".to_string(), "profile".to_string()]),
+            Some(_) => list(ctx, &o.scopes, &at, "scopes"),
+        };
+        let idp_name = opt(ctx, &o.idp_name, &at, "idp_name");
         Some(ApiOauth2 {
             issuer: issuer?,
             audience: audience?,
             jwks_url: jwks_url?,
             discovery_url: discovery_url?,
             roles_claim: roles_claim?,
+            client_id: client_id?,
+            client_secret: client_secret?,
+            scopes: scopes?,
+            idp_name: idp_name?,
         })
     });
     let role_mapping = role_mapping(ctx, &a.role_mapping, &at.key("role_mapping"));
```

`crates/data-avalanche-config/src/field_docs.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
index b20726f..fd08651 100644
--- a/crates/data-avalanche-config/src/field_docs.rs
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -128,6 +128,26 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
         Value("roles"),
         "The claim, a dotted JSON path, that holds the caller's roles.",
     ),
+    f(
+        "globals.api.oauth2.client_id",
+        Required,
+        "The web console's client at the identity provider, for its sign-in (authorization code with PKCE). Token verification doesn't use it. Needed for the console under `oauth2` (V-136).",
+    ),
+    f(
+        "globals.api.oauth2.client_secret",
+        Required,
+        "The console client's secret, only for a client the provider registered as confidential. A secret; needs `client_id` (V-133).",
+    ),
+    f(
+        "globals.api.oauth2.scopes",
+        Note("openid, profile"),
+        "The scopes the console's sign-in asks for. Needs `client_id` (V-133).",
+    ),
+    f(
+        "globals.api.oauth2.idp_name",
+        Note("the issuer's host name"),
+        "The provider's name on the console's sign-in page (`Continue with Company SSO`).",
+    ),
     f(
         "globals.api.role_mapping.*",
         Note("each role maps to itself"),
@@ -293,6 +313,81 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
         Note("from the credential chain"),
         "The Azure storage account of every container (`cloud: azure`).",
     ),
+    f(
+        "globals.console.enabled",
+        Value("false"),
+        "Serves the web console under `/ui/` on the API's listener. A reload turns it on or off.",
+    ),
+    f(
+        "globals.console.session_key",
+        Required,
+        "Encrypts and signs the console's session cookie: at least 32 bytes, the same on every node, set through `${ENV}` and never kept in object storage (V-128). Required with the console on under `basic` or `oauth2` (V-135); a change signs everyone out.",
+    ),
+    f(
+        "globals.console.public_url",
+        Required,
+        "The console's address as people reach it (`https://cdc.example.com`), for the OAuth2 redirect. Required with the console on under `oauth2` (V-136).",
+    ),
+    f(
+        "globals.console.session_ttl",
+        Value("8h"),
+        "How long a session lasts without activity, from 5 minutes to 7 days (V-132).",
+    ),
+    f(
+        "globals.console.username_claim",
+        Value("preferred_username"),
+        "The token claim shown and recorded as the person's name (`jwt`, `oauth2`); `sub` when the token lacks it.",
+    ),
+    f(
+        "globals.console.history.retention",
+        Value("24h"),
+        "How long each node keeps the built-in chart history in `globals.state`, from 1 hour to 30 days (V-132). Older ranges come from `prometheus`.",
+    ),
+    f(
+        "globals.console.events.retention",
+        Value("30d"),
+        "How long the cluster's events and audit trail are kept in `globals.state`, from 1 to 400 days (V-132).",
+    ),
+    f(
+        "globals.console.prometheus.url",
+        Required,
+        "Where chart ranges older than the built-in history come from: Prometheus's base URL, http or https (V-130). Absent, the console uses its built-in history only.",
+    ),
+    f(
+        "globals.console.prometheus.basic.user",
+        Required,
+        "Basic authentication's user; exclusive with `bearer_token` (V-131).",
+    ),
+    f(
+        "globals.console.prometheus.basic.password",
+        Required,
+        "Basic authentication's password, a secret.",
+    ),
+    f(
+        "globals.console.prometheus.bearer_token",
+        Required,
+        "A bearer token, a secret; exclusive with `basic` (V-131).",
+    ),
+    f(
+        "globals.console.prometheus.headers.*",
+        Note("none"),
+        "Headers sent with every query, a map of name to value (a tenant header). The values are secrets (V-126).",
+    ),
+    f(
+        "globals.console.prometheus.tls.ca",
+        Note("the system's roots"),
+        "The CA certificates the Prometheus server's certificate is checked against.",
+    ),
+    f(
+        "globals.console.prometheus.extra_labels.*",
+        Note("none"),
+        "Label matchers added to every query (`cluster: prod-eu`), for labels a collector adds or renames. Prometheus label names, not starting with `__` (V-131).",
+    ),
+    f(
+        "globals.console.prometheus.timeout",
+        Note("5s"),
+        "How long one query may take.",
+    ),
     // ---- a pipeline (config §9) ----
     f(
         "pipelines.*.id",
```

`crates/data-avalanche-config/src/mutability.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/mutability.rs b/crates/data-avalanche-config/src/mutability.rs
index 23d0b6a..d686dfc 100644
--- a/crates/data-avalanche-config/src/mutability.rs
+++ b/crates/data-avalanche-config/src/mutability.rs
@@ -41,6 +41,7 @@ pub(crate) const CLASSES: &[(&str, Mutability)] = &[
     ("globals.cluster.drain_timeout", R),
     ("globals.tracing", R),
     ("globals.azure", Proc),
+    ("globals.console", R),
     ("pipelines.*.name", R),
     // Diffs match pipelines by `id`, so a field-level `id` change never happens (an `id` change is
     // remove + add, §9.1). This entry exists only so `unclassified_paths` covers the path.
```

`crates/data-avalanche-config/src/raw.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/raw.rs b/crates/data-avalanche-config/src/raw.rs
index e13da19..d688545 100644
--- a/crates/data-avalanche-config/src/raw.rs
+++ b/crates/data-avalanche-config/src/raw.rs
@@ -38,6 +38,7 @@ pub(crate) struct RawGlobals {
     pub cluster: Option<RawCluster>,
     pub tracing: Option<RawTracing>,
     pub azure: Option<RawAzure>,
+    pub console: Option<RawConsole>,
 }
 
 #[derive(Debug, Clone, Default, Deserialize)]
@@ -78,6 +79,10 @@ pub(crate) struct RawApiOauth2 {
     pub jwks_url: L<String>,
     pub discovery_url: L<String>,
     pub roles_claim: L<String>,
+    pub client_id: L<String>,
+    pub client_secret: L<String>,
+    pub scopes: Option<Vec<Leaf<String>>>,
+    pub idp_name: L<String>,
 }
 
 #[derive(Debug, Clone, Default, Deserialize)]
@@ -143,6 +148,51 @@ pub(crate) struct RawAzure {
     pub account_name: L<String>,
 }
 
+#[derive(Debug, Clone, Default, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub(crate) struct RawConsole {
+    pub enabled: L<bool>,
+    pub session_key: L<String>,
+    pub public_url: L<String>,
+    pub session_ttl: L<Duration>,
+    pub username_claim: L<String>,
+    pub history: Option<RawRetention>,
+    pub events: Option<RawRetention>,
+    pub prometheus: Option<RawPrometheus>,
+}
+
+/// `globals.console.history` and `globals.console.events`.
+#[derive(Debug, Clone, Default, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub(crate) struct RawRetention {
+    pub retention: L<Duration>,
+}
+
+#[derive(Debug, Clone, Default, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub(crate) struct RawPrometheus {
+    pub url: L<String>,
+    pub basic: Option<RawBasic>,
+    pub bearer_token: L<String>,
+    pub headers: Option<BTreeMap<String, Leaf<String>>>,
+    pub tls: Option<RawCa>,
+    pub extra_labels: Option<BTreeMap<String, Leaf<String>>>,
+    pub timeout: L<Duration>,
+}
+
+#[derive(Debug, Clone, Default, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub(crate) struct RawBasic {
+    pub user: L<String>,
+    pub password: L<String>,
+}
+
+#[derive(Debug, Clone, Default, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub(crate) struct RawCa {
+    pub ca: L<String>,
+}
+
 // ---- pipelines and defaults (§7, §9) ----
 
 #[derive(Debug, Clone, Default, Deserialize)]
```

`crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index 52293f7..fec7f6d 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1057,7 +1057,7 @@ pub const RULES: &[RuleMeta] = &[
         Error,
         L::Config,
         Config,
-        r#"globals.tracing.headers"#,
+        r#"globals.tracing.headers, globals.console.prometheus.headers"#,
         r#"Each name an HTTP token, each value visible ASCII, spaces and tabs (DA-OBS-7)."#,
     ),
     rule(
@@ -1068,6 +1068,78 @@ pub const RULES: &[RuleMeta] = &[
         r#"tables.column_type_overrides keys"#,
         r#"The key's column names no column of its table (on MySQL compared without case, as MySQL compares column names): the override changes nothing (DA-SRC-35). (Emitted at schema resolution.)"#,
     ),
+    rule(
+        "V-128",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.console.session_key"#,
+        r#"At least 32 bytes after substitution."#,
+    ),
+    rule(
+        "V-129",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.console.public_url"#,
+        r#"An absolute http/https URL without query or fragment."#,
+    ),
+    rule(
+        "V-130",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.console.prometheus.url"#,
+        r#"An absolute http/https URL."#,
+    ),
+    rule(
+        "V-131",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.console.prometheus"#,
+        r#"basic and bearer_token are exclusive; extra_labels names match [a-zA-Z_][a-zA-Z0-9_]* and don't start with __."#,
+    ),
+    rule(
+        "V-132",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.console durations"#,
+        r#"session_ttl 5 min – 7 days; history.retention 1 h – 30 days; events.retention 1 – 400 days."#,
+    ),
+    rule(
+        "V-133",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.api.oauth2.client_secret / scopes"#,
+        r#"Need client_id."#,
+    ),
+    rule(
+        "V-134",
+        Warning,
+        L::Config,
+        Config,
+        r#"globals.console.session_key with api.auth ∈ {none, jwt}"#,
+        r#"Unused: these modes have no sessions."#,
+    ),
+    rule(
+        "V-135",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.console.enabled = true with api.auth ∈ {basic, oauth2}"#,
+        r#"Requires globals.console.session_key."#,
+    ),
+    rule(
+        "V-136",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.console.enabled = true with api.auth = oauth2"#,
+        r#"Requires globals.console.public_url and globals.api.oauth2.client_id."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
```

`crates/data-avalanche-config/src/resolved.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/resolved.rs b/crates/data-avalanche-config/src/resolved.rs
index 1428343..fc5b5b4 100644
--- a/crates/data-avalanche-config/src/resolved.rs
+++ b/crates/data-avalanche-config/src/resolved.rs
@@ -43,6 +43,7 @@ pub struct Globals {
     pub cluster: Cluster,
     pub tracing: Tracing,
     pub azure: Azure,
+    pub console: Console,
 }
 
 #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
@@ -117,6 +118,13 @@ pub struct ApiOauth2 {
     pub jwks_url: Option<String>,
     pub discovery_url: Option<String>,
     pub roles_claim: String,
+    /// The web console's client at the provider (§6.3.1); token verification doesn't use it.
+    pub client_id: Option<String>,
+    /// Only for a client the provider registered as confidential.
+    pub client_secret: Option<Secret>,
+    pub scopes: Vec<String>,
+    /// The provider's name on the sign-in page; `None` shows the issuer's host.
+    pub idp_name: Option<String>,
 }
 
 #[derive(Debug, Clone, PartialEq, Serialize)]
@@ -183,6 +191,51 @@ pub struct Azure {
     pub account_name: Option<String>,
 }
 
+/// `globals.console` (§6.8): the web console under `/ui/`, and the retention of the events and
+/// the chart history it reads (recorded whether it is on or off).
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+pub struct Console {
+    pub enabled: bool,
+    /// Encrypts and signs the session cookie (`basic`, `oauth2`).
+    pub session_key: Option<Secret>,
+    pub public_url: Option<String>,
+    #[serde(serialize_with = "ser_duration")]
+    pub session_ttl: Duration,
+    pub username_claim: String,
+    pub history: Retention,
+    pub events: Retention,
+    /// `None`: the built-in history only.
+    pub prometheus: Option<Prometheus>,
+}
+
+#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
+pub struct Retention {
+    #[serde(serialize_with = "ser_duration")]
+    pub retention: Duration,
+}
+
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+pub struct Prometheus {
+    pub url: String,
+    pub auth: PrometheusAuth,
+    /// Sent with every query; the values are secrets.
+    pub headers: BTreeMap<String, Secret>,
+    /// `None`: the system's roots.
+    pub ca: Option<Pem>,
+    /// Matchers added to every query: `name="value"`.
+    pub extra_labels: BTreeMap<String, String>,
+    #[serde(serialize_with = "ser_duration")]
+    pub timeout: Duration,
+}
+
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+#[serde(tag = "kind", rename_all = "snake_case")]
+pub enum PrometheusAuth {
+    None,
+    Basic { user: String, password: Secret },
+    Bearer { token: Secret },
+}
+
 // ---- pipelines ----
 
 #[derive(Debug, Clone, PartialEq, Serialize)]
```

`crates/data-avalanche-config/src/rules/auth.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/auth.rs b/crates/data-avalanche-config/src/rules/auth.rs
index a18cfcf..93baf83 100644
--- a/crates/data-avalanche-config/src/rules/auth.rs
+++ b/crates/data-avalanche-config/src/rules/auth.rs
@@ -61,6 +61,7 @@ pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut
             ctx,
         );
     }
+    console(g, ctx);
     for (name, p) in merged {
         let is_iceberg = p
             .sink
@@ -341,3 +342,87 @@ fn management_api(a: &RawApi, at: &At, ctx: &mut Ctx) {
         require(ctx, "V-053", &at.key("tls"), "key", &tls.key, "api TLS");
     }
 }
+
+/// §6.8 and §6.3.1: V-133…V-136, the console's needs from the auth mode.
+fn console(g: &RawGlobals, ctx: &mut Ctx) {
+    let api_at = At::config(YamlPath::root().key("globals").key("api"));
+    let console_at = At::config(YamlPath::root().key("globals").key("console"));
+    let default_api = RawApi::default();
+    let a = g.api.as_ref().unwrap_or(&default_api);
+    let auth = if a.auth.is_none() {
+        Some(ApiAuth::None)
+    } else {
+        ctx.peek(&a.auth)
+    };
+    // V-133
+    if let Some(o) = &a.oauth2 {
+        let o_at = api_at.key("oauth2");
+        if o.client_id.is_none() {
+            for (key, present, loc) in [
+                (
+                    "client_secret",
+                    o.client_secret.is_some(),
+                    o.client_secret.as_ref().and_then(|l| l.loc()),
+                ),
+                ("scopes", o.scopes.is_some(), None),
+            ] {
+                if present {
+                    ctx.error(
+                        "V-133",
+                        &o_at.key(key),
+                        loc,
+                        format!("oauth2.{key} needs client_id"),
+                    );
+                }
+            }
+        }
+    }
+    let default_console = RawConsole::default();
+    let c = g.console.as_ref().unwrap_or(&default_console);
+    // V-134
+    if let Some(l) = &c.session_key
+        && matches!(auth, Some(ApiAuth::None | ApiAuth::Jwt))
+    {
+        ctx.warning(
+            "V-134",
+            &console_at.key("session_key"),
+            l.loc(),
+            "session_key is unused: with api auth none or jwt the console has no sessions",
+        );
+    }
+    let enabled = c.enabled.is_some() && ctx.peek(&c.enabled) == Some(true);
+    if !enabled {
+        return;
+    }
+    // V-135
+    if matches!(auth, Some(ApiAuth::Basic | ApiAuth::Oauth2))
+        && missing(ctx, &c.session_key, &console_at.key("session_key"))
+    {
+        ctx.error(
+            "V-135",
+            &console_at.key("session_key"),
+            c.enabled.as_ref().and_then(|l| l.loc()),
+            "the console with api auth basic or oauth2 needs session_key",
+        );
+    }
+    // V-136
+    if auth == Some(ApiAuth::Oauth2) {
+        if missing(ctx, &c.public_url, &console_at.key("public_url")) {
+            ctx.error(
+                "V-136",
+                &console_at.key("public_url"),
+                c.enabled.as_ref().and_then(|l| l.loc()),
+                "the console with api auth oauth2 needs public_url",
+            );
+        }
+        let client_id = a.oauth2.as_ref().and_then(|o| o.client_id.clone());
+        if missing(ctx, &client_id, &api_at.key("oauth2").key("client_id")) {
+            ctx.error(
+                "V-136",
+                &api_at.key("oauth2").key("client_id"),
+                c.enabled.as_ref().and_then(|l| l.loc()),
+                "the console with api auth oauth2 needs oauth2.client_id",
+            );
+        }
+    }
+}
```

`crates/data-avalanche-config/src/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/domain.rs b/crates/data-avalanche-config/src/rules/domain.rs
index 1fc778c..020b810 100644
--- a/crates/data-avalanche-config/src/rules/domain.rs
+++ b/crates/data-avalanche-config/src/rules/domain.rs
@@ -1,6 +1,7 @@
 //! §13.3 value domain. Enumerations are checked by coercion (each enum carries its V-rule, see
 //! `crate::enums`); this module checks ranges, reserved values and the v1 rejections.
 
+use std::collections::BTreeMap;
 use std::time::Duration;
 
 use super::pipeline_at;
@@ -227,36 +228,11 @@ fn globals(g: &RawGlobals, ctx: &mut Ctx) {
             format!("sample_ratio must be 0..=1, got {}", shown_value(l, &r)),
         );
     }
-    // V-126: a header HTTP takes. The value isn't echoed: it is a secret.
     if let Some(headers) = g.tracing.as_ref().and_then(|t| t.headers.as_ref()) {
-        let at = root.key("tracing").key("headers");
-        for (name, leaf) in headers {
-            let token = !name.is_empty()
-                && name
-                    .bytes()
-                    .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b));
-            if !token {
-                ctx.error(
-                    "V-126",
-                    &at.key(name),
-                    leaf.loc(),
-                    format!("header name {name:?} isn't an HTTP token"),
-                );
-                continue;
-            }
-            if let Some(value) = ctx.text(leaf, &at.key(name))
-                && !value
-                    .bytes()
-                    .all(|b| b == b'\t' || (b' '..=b'~').contains(&b))
-            {
-                ctx.error(
-                    "V-126",
-                    &at.key(name),
-                    leaf.loc(),
-                    format!("header {name}'s value holds a control or non-ASCII character"),
-                );
-            }
-        }
+        http_headers(ctx, headers, &root.key("tracing").key("headers"));
+    }
+    if let Some(c) = &g.console {
+        console(c, &root.key("console"), ctx);
     }
     // V-101
     if let Some(a) = &g.azure
@@ -613,3 +589,173 @@ fn catalog(c: &RawCatalog, at: &At, ctx: &mut Ctx, cross_field: bool) {
         ctx.error("V-116", &at.key("rest").key("auth"), loc, "rest auth sigv4 is rejected: the pinned iceberg-rust REST client has no SigV4 signing (DA-CAT-7); on AWS use catalog.glue");
     }
 }
+
+/// V-126: a header HTTP takes. The value isn't echoed: it is a secret.
+fn http_headers(ctx: &mut Ctx, headers: &BTreeMap<String, Leaf<String>>, at: &At) {
+    for (name, leaf) in headers {
+        let token = !name.is_empty()
+            && name
+                .bytes()
+                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b));
+        if !token {
+            ctx.error(
+                "V-126",
+                &at.key(name),
+                leaf.loc(),
+                format!("header name {name:?} isn't an HTTP token"),
+            );
+            continue;
+        }
+        if let Some(value) = ctx.text(leaf, &at.key(name))
+            && !value
+                .bytes()
+                .all(|b| b == b'\t' || (b' '..=b'~').contains(&b))
+        {
+            ctx.error(
+                "V-126",
+                &at.key(name),
+                leaf.loc(),
+                format!("header {name}'s value holds a control or non-ASCII character"),
+            );
+        }
+    }
+}
+
+/// Whether `text` is an absolute `http`/`https` URL with a host.
+fn http_url(text: &str) -> bool {
+    let lower = text.to_ascii_lowercase();
+    let rest = lower
+        .strip_prefix("https://")
+        .or_else(|| lower.strip_prefix("http://"));
+    rest.is_some_and(|r| {
+        let host = r.split(['/', '?', '#']).next().unwrap_or("");
+        !host.is_empty() && !host.contains(char::is_whitespace)
+    })
+}
+
+/// §6.8: V-128…V-132, and V-126 on the Prometheus headers. URLs aren't echoed: they may come
+/// from a variable.
+fn console(c: &RawConsole, at: &At, ctx: &mut Ctx) {
+    // V-128
+    if let Some(l) = &c.session_key
+        && let Some(key) = ctx.text(l, &at.key("session_key"))
+        && key.len() < 32
+    {
+        ctx.error(
+            "V-128",
+            &at.key("session_key"),
+            l.loc(),
+            format!(
+                "session_key holds {} bytes; it needs at least 32",
+                key.len()
+            ),
+        );
+    }
+    // V-129
+    if let Some(l) = &c.public_url
+        && let Some(url) = ctx.text(l, &at.key("public_url"))
+        && (!http_url(url.trim()) || url.contains(['?', '#']))
+    {
+        ctx.error(
+            "V-129",
+            &at.key("public_url"),
+            l.loc(),
+            "public_url must be an absolute http or https URL without a query or a fragment",
+        );
+    }
+    // V-132
+    let range =
+        |ctx: &mut Ctx, leaf: &Option<Leaf<Duration>>, at: &At, low: u64, high: u64, what: &str| {
+            if let Some((d, loc)) = val(ctx, leaf, at)
+                && !(Duration::from_secs(low)..=Duration::from_secs(high)).contains(&d)
+            {
+                ctx.error(
+                    "V-132",
+                    at,
+                    loc,
+                    format!(
+                        "{what} must be {} to {}, got {}",
+                        crate::units::format_duration(Duration::from_secs(low)),
+                        crate::units::format_duration(Duration::from_secs(high)),
+                        crate::units::format_duration(d)
+                    ),
+                );
+            }
+        };
+    const HOUR: u64 = 3600;
+    const DAY: u64 = 24 * HOUR;
+    range(
+        ctx,
+        &c.session_ttl,
+        &at.key("session_ttl"),
+        5 * 60,
+        7 * DAY,
+        "session_ttl",
+    );
+    if let Some(h) = &c.history {
+        range(
+            ctx,
+            &h.retention,
+            &at.key("history").key("retention"),
+            HOUR,
+            30 * DAY,
+            "history.retention",
+        );
+    }
+    if let Some(e) = &c.events {
+        range(
+            ctx,
+            &e.retention,
+            &at.key("events").key("retention"),
+            DAY,
+            400 * DAY,
+            "events.retention",
+        );
+    }
+    let Some(p) = &c.prometheus else {
+        return;
+    };
+    let p_at = at.key("prometheus");
+    // V-130
+    if let Some(l) = &p.url
+        && let Some(url) = ctx.text(l, &p_at.key("url"))
+        && !http_url(url.trim())
+    {
+        ctx.error(
+            "V-130",
+            &p_at.key("url"),
+            l.loc(),
+            "prometheus.url must be an absolute http or https URL",
+        );
+    }
+    // V-131
+    if p.basic.is_some() && p.bearer_token.is_some() {
+        ctx.error(
+            "V-131",
+            &p_at,
+            p.bearer_token.as_ref().and_then(|l| l.loc()),
+            "prometheus takes basic or bearer_token, not both",
+        );
+    }
+    for (name, leaf) in p.extra_labels.iter().flatten() {
+        let mut bytes = name.bytes();
+        let label = bytes
+            .next()
+            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
+            && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
+            && !name.starts_with("__");
+        if !label {
+            ctx.error(
+                "V-131",
+                &p_at.key("extra_labels").key(name),
+                leaf.loc(),
+                format!(
+                    "extra_labels key {name:?} isn't a Prometheus label name (or starts with __)"
+                ),
+            );
+        }
+    }
+    if let Some(headers) = &p.headers {
+        http_headers(ctx, headers, &p_at.key("headers"));
+    }
+}
```

`crates/data-avalanche-config/src/walk.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/walk.rs b/crates/data-avalanche-config/src/walk.rs
index 1780547..f707723 100644
--- a/crates/data-avalanche-config/src/walk.rs
+++ b/crates/data-avalanche-config/src/walk.rs
@@ -238,7 +238,8 @@ walk_fields!(RawGlobals {
     log,
     cluster,
     tracing,
-    azure
+    azure,
+    console
 });
 walk_fields!(RawGlobalState { bucket, prefix });
 walk_fields!(RawApi {
@@ -263,7 +264,11 @@ walk_fields!(RawApiOauth2 {
     audience,
     jwks_url,
     discovery_url,
-    roles_claim
+    roles_claim,
+    client_id,
+    client_secret,
+    scopes,
+    idp_name
 });
 walk_fields!(RawApiTls { enabled, cert, key });
 walk_fields!(RawLog { level, format });
@@ -301,6 +306,28 @@ walk_fields!(RawTracing {
     headers
 });
 walk_fields!(RawAzure { account_name });
+walk_fields!(RawConsole {
+    enabled,
+    session_key,
+    public_url,
+    session_ttl,
+    username_claim,
+    history,
+    events,
+    prometheus
+});
+walk_fields!(RawRetention { retention });
+walk_fields!(RawPrometheus {
+    url,
+    basic,
+    bearer_token,
+    headers,
+    tls,
+    extra_labels,
+    timeout
+});
+walk_fields!(RawBasic { user, password });
+walk_fields!(RawCa { ca });
 
 // `pipelines`/`defaults` (§7, §9): the three union types have a hand-written `Merge` (config §8.4)
 // and so never went through `merge_struct!`; `RawPartitionField` is never merged as a struct at
```

`docs/src/content/docs/reference/configuration.md`:

```diff
diff --git a/docs/src/content/docs/reference/configuration.md b/docs/src/content/docs/reference/configuration.md
index 0d94f8f..df2b3cb 100644
--- a/docs/src/content/docs/reference/configuration.md
+++ b/docs/src/content/docs/reference/configuration.md
@@ -61,7 +61,7 @@ Token verification for `auth: jwt`.
 
 ## `globals.api.oauth2`
 
-Token verification for `auth: oauth2`.
+Token verification for `auth: oauth2`, and the web console's sign-in client.
 
 | Key | Type | Default | On reload | Rules | Description |
 |---|---|---|---|---|---|
@@ -70,6 +70,10 @@ Token verification for `auth: oauth2`.
 | `jwks_url` | string | — | applied |  | Where the API fetches the signing keys; optional when `discovery_url` is set. |
 | `discovery_url` | string | — | applied |  | The issuer's OpenID discovery document, which names its keys. |
 | `roles_claim` | string | `roles` | applied |  | The claim, a dotted JSON path, that holds the caller's roles. |
+| `client_id` | string | — | applied |  | The web console's client at the identity provider, for its sign-in (authorization code with PKCE). Token verification doesn't use it. Needed for the console under `oauth2` (V-136). |
+| `client_secret` | string | — | applied | [V-133](/data-avalanche/reference/validation-rules/#v-133) | The console client's secret, only for a client the provider registered as confidential. A secret; needs `client_id` (V-133). |
+| `scopes` | list of string | openid, profile | applied |  | The scopes the console's sign-in asks for. Needs `client_id` (V-133). |
+| `idp_name` | string | the issuer's host name | applied |  | The provider's name on the console's sign-in page (`Continue with Company SSO`). |
 
 ## `globals.api.tls`
 
@@ -147,6 +151,67 @@ Azure Blob Storage (`cloud: azure`).
 |---|---|---|---|---|---|
 | `account_name` | string | from the credential chain | process restart | [V-101](/data-avalanche/reference/validation-rules/#v-101) | The Azure storage account of every container (`cloud: azure`). |
 
+## `globals.console`
+
+The web console under `/ui/` (off by default), and how long the events and the chart history it reads are kept: they are recorded whether the console is on or off.
+
+Rules for the whole block: [V-132](/data-avalanche/reference/validation-rules/#v-132).
+
+| Key | Type | Default | On reload | Rules | Description |
+|---|---|---|---|---|---|
+| `enabled` | boolean | `false` | applied | [V-135](/data-avalanche/reference/validation-rules/#v-135) [V-136](/data-avalanche/reference/validation-rules/#v-136) | Serves the web console under `/ui/` on the API's listener. A reload turns it on or off. |
+| `session_key` | string | — | applied | [V-128](/data-avalanche/reference/validation-rules/#v-128) [V-134](/data-avalanche/reference/validation-rules/#v-134) | Encrypts and signs the console's session cookie: at least 32 bytes, the same on every node, set through `${ENV}` and never kept in object storage (V-128). Required with the console on under `basic` or `oauth2` (V-135); a change signs everyone out. |
+| `public_url` | string | — | applied | [V-129](/data-avalanche/reference/validation-rules/#v-129) | The console's address as people reach it (`https://cdc.example.com`), for the OAuth2 redirect. Required with the console on under `oauth2` (V-136). |
+| `session_ttl` | duration | `8h` | applied |  | How long a session lasts without activity, from 5 minutes to 7 days (V-132). |
+| `username_claim` | string | `preferred_username` | applied |  | The token claim shown and recorded as the person's name (`jwt`, `oauth2`); `sub` when the token lacks it. |
+
+## `globals.console.history`
+
+The built-in chart history each node keeps.
+
+| Key | Type | Default | On reload | Rules | Description |
+|---|---|---|---|---|---|
+| `retention` | duration | `24h` | applied |  | How long each node keeps the built-in chart history in `globals.state`, from 1 hour to 30 days (V-132). Older ranges come from `prometheus`. |
+
+## `globals.console.events`
+
+The cluster's events and audit trail.
+
+| Key | Type | Default | On reload | Rules | Description |
+|---|---|---|---|---|---|
+| `retention` | duration | `30d` | applied |  | How long the cluster's events and audit trail are kept in `globals.state`, from 1 to 400 days (V-132). |
+
+## `globals.console.prometheus`
+
+Where chart ranges older than the built-in history come from. Absent, the console uses its built-in history only, without a warning.
+
+Rules for the whole block: [V-131](/data-avalanche/reference/validation-rules/#v-131).
+
+| Key | Type | Default | On reload | Rules | Description |
+|---|---|---|---|---|---|
+| `url` | string | — | applied | [V-130](/data-avalanche/reference/validation-rules/#v-130) | Where chart ranges older than the built-in history come from: Prometheus's base URL, http or https (V-130). Absent, the console uses its built-in history only. |
+| `bearer_token` | string | — | applied |  | A bearer token, a secret; exclusive with `basic` (V-131). |
+| `headers.<key>` | map of string | none | applied |  | Headers sent with every query, a map of name to value (a tenant header). The values are secrets (V-126). |
+| `extra_labels.<key>` | map of string | none | applied |  | Label matchers added to every query (`cluster: prod-eu`), for labels a collector adds or renames. Prometheus label names, not starting with `__` (V-131). |
+| `timeout` | duration | 5s | applied |  | How long one query may take. |
+
+## `globals.console.prometheus.basic`
+
+Basic authentication to Prometheus.
+
+| Key | Type | Default | On reload | Rules | Description |
+|---|---|---|---|---|---|
+| `user` | string | — | applied |  | Basic authentication's user; exclusive with `bearer_token` (V-131). |
+| `password` | string | — | applied |  | Basic authentication's password, a secret. |
+
+## `globals.console.prometheus.tls`
+
+TLS to Prometheus.
+
+| Key | Type | Default | On reload | Rules | Description |
+|---|---|---|---|---|---|
+| `ca` | string | the system's roots | applied |  | The CA certificates the Prometheus server's certificate is checked against. |
+
 ## `pipelines.<name>`
 
 One pipeline: a source database, its tables, and the sink they replicate to. The map's key is the pipeline's name.
```

`docs/src/content/docs/reference/validation-rules.md`:

```diff
diff --git a/docs/src/content/docs/reference/validation-rules.md b/docs/src/content/docs/reference/validation-rules.md
index 80a4e44..c0e3cac 100644
--- a/docs/src/content/docs/reference/validation-rules.md
+++ b/docs/src/content/docs/reference/validation-rules.md
@@ -131,5 +131,14 @@ A config with any **error** is rejected whole: at startup the node refuses to st
 | <span id="v-123"></span>V-123 | error | `source.postgres.source_change_action` | ∈ {error, re_snapshot}. |
 | <span id="v-124"></span>V-124 | warning | `source.ssl_mode = verify-ca, when source.type = mysql` | The host name is checked too, as with verify-full (DA-SRC-22). |
 | <span id="v-125"></span>V-125 | warning | `globals.api.jwt.jwks_url, globals.api.oauth2.jwks_url and discovery_url` | Not https: the issuer's keys could be swapped on the way (DA-API-16). |
-| <span id="v-126"></span>V-126 | error | `globals.tracing.headers` | Each name an HTTP token, each value visible ASCII, spaces and tabs (DA-OBS-7). |
+| <span id="v-126"></span>V-126 | error | `globals.tracing.headers, globals.console.prometheus.headers` | Each name an HTTP token, each value visible ASCII, spaces and tabs (DA-OBS-7). |
 | <span id="v-127"></span>V-127 | warning | `tables.column_type_overrides keys` | The key's column names no column of its table (on MySQL compared without case, as MySQL compares column names): the override changes nothing (DA-SRC-35). (Emitted at schema resolution.) |
+| <span id="v-128"></span>V-128 | error | `globals.console.session_key` | At least 32 bytes after substitution. |
+| <span id="v-129"></span>V-129 | error | `globals.console.public_url` | An absolute http/https URL without query or fragment. |
+| <span id="v-130"></span>V-130 | error | `globals.console.prometheus.url` | An absolute http/https URL. |
+| <span id="v-131"></span>V-131 | error | `globals.console.prometheus` | basic and bearer_token are exclusive; extra_labels names match [a-zA-Z_][a-zA-Z0-9_]* and don't start with __. |
+| <span id="v-132"></span>V-132 | error | `globals.console durations` | session_ttl 5 min – 7 days; history.retention 1 h – 30 days; events.retention 1 – 400 days. |
+| <span id="v-133"></span>V-133 | error | `globals.api.oauth2.client_secret / scopes` | Need client_id. |
+| <span id="v-134"></span>V-134 | warning | `globals.console.session_key with api.auth ∈ {none, jwt}` | Unused: these modes have no sessions. |
+| <span id="v-135"></span>V-135 | error | `globals.console.enabled = true with api.auth ∈ {basic, oauth2}` | Requires globals.console.session_key. |
+| <span id="v-136"></span>V-136 | error | `globals.console.enabled = true with api.auth = oauth2` | Requires globals.console.public_url and globals.api.oauth2.client_id. |
```

`xtask/src/config.rs`:

```diff
diff --git a/xtask/src/config.rs b/xtask/src/config.rs
index 4590155..4b7aa84 100644
--- a/xtask/src/config.rs
+++ b/xtask/src/config.rs
@@ -29,7 +29,7 @@ const BLOCKS: &[(&str, &str)] = &[
     ("globals.api.jwt", "Token verification for `auth: jwt`."),
     (
         "globals.api.oauth2",
-        "Token verification for `auth: oauth2`.",
+        "Token verification for `auth: oauth2`, and the web console's sign-in client.",
     ),
     ("globals.api.tls", "TLS for the API's listener."),
     ("globals.log", "The node's log."),
@@ -46,6 +46,27 @@ const BLOCKS: &[(&str, &str)] = &[
         "OTLP/HTTP traces of flushes, commits, snapshot chunks and schema changes.",
     ),
     ("globals.azure", "Azure Blob Storage (`cloud: azure`)."),
+    (
+        "globals.console",
+        "The web console under `/ui/` (off by default), and how long the events and the chart history it reads are kept: they are recorded whether the console is on or off.",
+    ),
+    (
+        "globals.console.history",
+        "The built-in chart history each node keeps.",
+    ),
+    (
+        "globals.console.events",
+        "The cluster's events and audit trail.",
+    ),
+    (
+        "globals.console.prometheus",
+        "Where chart ranges older than the built-in history come from. Absent, the console uses its built-in history only, without a warning.",
+    ),
+    (
+        "globals.console.prometheus.basic",
+        "Basic authentication to Prometheus.",
+    ),
+    ("globals.console.prometheus.tls", "TLS to Prometheus."),
     (
         "pipelines.*",
         "One pipeline: a source database, its tables, and the sink they replicate to. The map's key is the pipeline's name.",
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`

Expected: PASS, 161 tests.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings && cargo test --workspace`

Expected: no output from the first three; every test binary passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config crates/data-avalanche-api/src/auth.rs xtask/src/config.rs docs/src/content/docs/reference
git commit -m "feat(config): globals.console and the console's oauth2 sign-in fields, V-128…V-136"
```

---

### Task 2: The event store (`data-avalanche-state`, `data-avalanche-storage`)

**Files:**
- Create: `crates/data-avalanche-state/src/events.rs`
- Modify: `crates/data-avalanche-state/src/lib.rs`, `crates/data-avalanche-state/Cargo.toml`
- Modify: `crates/data-avalanche-storage/src/store.rs`
- Test: `crates/data-avalanche-storage/tests/versioned_store.rs` (every backend, under `DATA_AVALANCHE_IT`)

**Interfaces:**
- Consumes: `DocStore`, `Document`, `StateError` (M2); `escape_segment`.
- Produces (re-exported at the crate roots):
  - `VersionedStore::folders(&self, prefix: &str) -> Result<Vec<String>, StoreError>`: a default
    over `list`, and `ObjectStoreVersioned`'s over `list_with_delimiter`;
  - `EVENTS_PREFIX = "events"`, `MAX_EVENTS = 1000`, `MAX_SCANNED = 5000`;
  - `EventSeverity { Info, Warning, Error }` (snake_case), `Actor { name, role }`;
  - `EventDoc { at, kind, severity, node, pipeline: Option, object: Option, actor: Option<Actor>,
    note: Option, outcome: Option, text, detail: BTreeMap<String, String> }`, absent options not
    written; `EventDoc::new(at, node, kind, severity, text)`;
  - `EventQuery { since, until, kinds: Vec<String>, object, actor, node, text, limit, after }`
    (`Default`), `EventPage { events: Vec<(String, EventDoc)>, next: Option<String> }`;
  - `event_key(at, seq, node) -> String`;
  - `Events::new(docs: DocStore)`, `record(&self, &EventDoc) -> Result<String, StateError>` (the
    key), `list(&self, &EventQuery) -> Result<EventPage, StateError>`, `prune(&self, retention:
    Duration, now) -> Result<usize, StateError>`.

D-M13a.2.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-state/src/events.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/events.rs b/crates/data-avalanche-state/src/events.rs
new file mode 100644
index 0000000..11a7738
--- /dev/null
+++ b/crates/data-avalanche-state/src/events.rs
@@ -0,0 +1,214 @@
+#[cfg(test)]
+mod tests {
+    use std::sync::Arc;
+
+    use chrono::TimeZone;
+    use data_avalanche_storage::{ObjectStoreVersioned, VersionedStore};
+    use object_store::memory::InMemory;
+
+    use super::*;
+
+    fn at(day: u32, h: u32, m: u32) -> DateTime<Utc> {
+        Utc.with_ymd_and_hms(2026, 11, day, h, m, 0).unwrap()
+    }
+
+    fn events() -> (Events, Arc<dyn VersionedStore>) {
+        let store: Arc<dyn VersionedStore> =
+            Arc::new(ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state").unwrap());
+        (Events::new(DocStore::new(Arc::clone(&store))), store)
+    }
+
+    fn event(t: DateTime<Utc>, kind: &str, node: &str) -> EventDoc {
+        EventDoc::new(
+            t,
+            node,
+            kind,
+            EventSeverity::Info,
+            format!("{kind} on {node}"),
+        )
+    }
+
+    #[test]
+    fn a_key_orders_by_time_and_names_the_day() {
+        let k = event_key(at(14, 13, 55), 7, "node/b");
+        assert_eq!(k, "events/2026-11-14/135500.000000Z-000007-node%2Fb.json");
+        assert!(event_key(at(14, 9, 0), 0, "z") < event_key(at(14, 10, 0), 0, "a"));
+        assert!(event_key(at(13, 23, 59), 9, "z") < event_key(at(14, 0, 0), 0, "a"));
+    }
+
+    #[tokio::test]
+    async fn events_list_newest_first_through_the_filters() {
+        let (ev, _) = events();
+        let mut pause = event(at(13, 10, 0), "action.pipeline.pause", "node-a");
+        pause.pipeline = Some("orders".into());
+        pause.actor = Some(Actor {
+            name: "Oleksii".into(),
+            role: "operator".into(),
+        });
+        pause.note = Some("Backfill after the outage".into());
+        let mut halt = event(at(14, 13, 41), "table.halted", "node-b");
+        halt.pipeline = Some("orders".into());
+        halt.object = Some("public.refunds".into());
+        halt.severity = EventSeverity::Error;
+        let join = event(at(14, 13, 52), "member.joined", "node-c");
+        for e in [&pause, &halt, &join] {
+            ev.record(e).await.unwrap();
+        }
+        let list = |q: EventQuery| {
+            let ev = &ev;
+            async move {
+                ev.list(&q)
+                    .await
+                    .unwrap()
+                    .events
+                    .into_iter()
+                    .map(|(_, e)| e.kind)
+                    .collect::<Vec<_>>()
+            }
+        };
+        assert_eq!(
+            list(EventQuery::default()).await,
+            ["member.joined", "table.halted", "action.pipeline.pause"]
+        );
+        let kinds = |k: &[&str]| EventQuery {
+            kinds: k.iter().map(|s| s.to_string()).collect(),
+            ..EventQuery::default()
+        };
+        assert_eq!(list(kinds(&["action"])).await, ["action.pipeline.pause"]);
+        assert!(
+            list(kinds(&["act"])).await.is_empty(),
+            "a family is a whole segment"
+        );
+        assert_eq!(
+            list(kinds(&["table.halted", "member"])).await,
+            ["member.joined", "table.halted"]
+        );
+        let q = |f: fn(&mut EventQuery)| {
+            let mut q = EventQuery::default();
+            f(&mut q);
+            q
+        };
+        assert_eq!(
+            list(q(|q| q.object = Some("orders".into()))).await,
+            ["table.halted", "action.pipeline.pause"]
+        );
+        assert_eq!(
+            list(q(|q| q.object = Some("public.refunds".into()))).await,
+            ["table.halted"]
+        );
+        assert_eq!(
+            list(q(|q| q.actor = Some("Oleksii".into()))).await,
+            ["action.pipeline.pause"]
+        );
+        assert_eq!(
+            list(q(|q| q.node = Some("node-c".into()))).await,
+            ["member.joined"]
+        );
+        assert_eq!(
+            list(q(|q| q.text = Some("OUTAGE".into()))).await,
+            ["action.pipeline.pause"],
+            "the note, without case"
+        );
+        assert_eq!(
+            list(EventQuery {
+                since: Some(at(14, 0, 0)),
+                until: Some(at(14, 13, 50)),
+                ..EventQuery::default()
+            })
+            .await,
+            ["table.halted"],
+            "since inclusive, until exclusive"
+        );
+    }
+
+    #[tokio::test]
+    async fn a_page_hands_back_where_it_stopped() {
+        let (ev, _) = events();
+        for (day, h) in [(12, 1), (13, 1), (13, 2), (14, 1)] {
+            ev.record(&event(at(day, h, 0), "member.joined", "a"))
+                .await
+                .unwrap();
+        }
+        let first = ev
+            .list(&EventQuery {
+                limit: 2,
+                ..EventQuery::default()
+            })
+            .await
+            .unwrap();
+        let times: Vec<_> = first.events.iter().map(|(_, e)| e.at).collect();
+        assert_eq!(times, [at(14, 1, 0), at(13, 2, 0)]);
+        let next = first.next.expect("more to come");
+        let rest = ev
+            .list(&EventQuery {
+                after: Some(next),
+                ..EventQuery::default()
+            })
+            .await
+            .unwrap();
+        let times: Vec<_> = rest.events.iter().map(|(_, e)| e.at).collect();
+        assert_eq!(times, [at(13, 1, 0), at(12, 1, 0)]);
+        assert_eq!(rest.next, None);
+    }
+
+    #[tokio::test]
+    async fn two_events_in_one_microsecond_both_land() {
+        let (ev, store) = events();
+        let e = event(at(14, 1, 0), "member.joined", "a");
+        let k1 = ev.record(&e).await.unwrap();
+        // Another process of the same node, its sequence starting over.
+        let k2 = Events::new(DocStore::new(Arc::clone(&store)))
+            .record(&e)
+            .await
+            .unwrap();
+        assert_ne!(k1, k2);
+        assert_eq!(
+            ev.list(&EventQuery::default()).await.unwrap().events.len(),
+            2
+        );
+    }
+
+    #[tokio::test]
+    async fn an_unreadable_event_is_left_out_not_fatal() {
+        let (ev, store) = events();
+        ev.record(&event(at(14, 1, 0), "member.joined", "a"))
+            .await
+            .unwrap();
+        store
+            .put(
+                "events/2026-11-14/020000.000000Z-000000-x.json",
+                bytes::Bytes::from_static(br#"{"version": 2, "future": true}"#),
+            )
+            .await
+            .unwrap();
+        let page = ev.list(&EventQuery::default()).await.unwrap();
+        assert_eq!(page.events.len(), 1);
+    }
+
+    #[tokio::test]
+    async fn prune_deletes_whole_days_past_the_retention() {
+        let (ev, store) = events();
+        for day in [10, 11, 12, 13, 14] {
+            ev.record(&event(at(day, 12, 0), "member.joined", "a"))
+                .await
+                .unwrap();
+        }
+        // 2 days back from the 14th, 06:00 is the 12th, 06:00: the 10th and the 11th have ended.
+        let deleted = ev
+            .prune(Duration::from_secs(2 * 86_400), at(14, 6, 0))
+            .await
+            .unwrap();
+        assert_eq!(deleted, 2);
+        assert_eq!(
+            store.folders(EVENTS_PREFIX).await.unwrap(),
+            ["2026-11-12", "2026-11-13", "2026-11-14"]
+        );
+        assert_eq!(
+            ev.prune(Duration::from_secs(2 * 86_400), at(14, 6, 0))
+                .await
+                .unwrap(),
+            0,
+            "again: nothing left to delete"
+        );
+    }
+}
```

`crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 5d63fc4..7853305 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -42,6 +42,7 @@ mod snapshot;
 mod tables;
 #[cfg(test)]
 mod test_support;
+mod events;
 
 pub use anchor::{ANCHOR_KEY, Anchor, AnchorDoc, HashAlgo, establish_anchor};
 pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
```

`crates/data-avalanche-storage/src/store.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/store.rs b/crates/data-avalanche-storage/src/store.rs
index d9fb369..5026302 100644
--- a/crates/data-avalanche-storage/src/store.rs
+++ b/crates/data-avalanche-storage/src/store.rs
@@ -350,6 +350,59 @@ mod tests {
         assert!(s.list("none").await.unwrap().is_empty());
     }
 
+    /// A store that lists through [`VersionedStore::folders`]' default, over `list`.
+    #[derive(Debug)]
+    struct ListOnly(ObjectStoreVersioned);
+
+    #[async_trait]
+    impl VersionedStore for ListOnly {
+        async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
+            self.0.get(key).await
+        }
+        async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
+            self.0.create(key, body).await
+        }
+        async fn update(
+            &self,
+            key: &str,
+            expected: &ObjectVersion,
+            body: Bytes,
+        ) -> Result<ObjectVersion, StoreError> {
+            self.0.update(key, expected, body).await
+        }
+        async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
+            self.0.put(key, body).await
+        }
+        async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
+            self.0.list(prefix).await
+        }
+        async fn delete(&self, key: &str) -> Result<(), StoreError> {
+            self.0.delete(key).await
+        }
+    }
+
+    #[tokio::test]
+    async fn folders_lists_each_next_segment_once() {
+        let s = store("root");
+        for k in [
+            "events/2026-11-14/b.json",
+            "events/2026-11-13/a.json",
+            "events/2026-11-14/a.json",
+            "events/loose.json",
+            "eventsx/2026-01-01/c.json",
+        ] {
+            s.put(k, b("x")).await.unwrap();
+        }
+        let want = vec!["2026-11-13", "2026-11-14"];
+        assert_eq!(s.folders("events").await.unwrap(), want, "object_store's listing");
+        assert_eq!(
+            ListOnly(s.clone()).folders("events").await.unwrap(),
+            want,
+            "the trait's default, over list"
+        );
+        assert!(s.folders("none").await.unwrap().is_empty());
+    }
+
     #[tokio::test]
     async fn roots_are_isolated() {
         let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
```

`crates/data-avalanche-storage/tests/versioned_store.rs`:

```diff
diff --git a/crates/data-avalanche-storage/tests/versioned_store.rs b/crates/data-avalanche-storage/tests/versioned_store.rs
index 6dae080..9404c04 100644
--- a/crates/data-avalanche-storage/tests/versioned_store.rs
+++ b/crates/data-avalanche-storage/tests/versioned_store.rs
@@ -75,6 +75,15 @@ async fn check(store: Arc<dyn ObjectStore>, test: &str) {
         ]
     );
     assert_eq!(s.get("doc.json").await.unwrap().unwrap().0, b("two"));
+    // One listing of the folders under a prefix (the events' days, M13a): each once, sorted, no
+    // object directly under it, and nothing from a sibling prefix.
+    s.put("snapshotsx/x.json", b("x")).await.unwrap();
+    assert_eq!(
+        s.folders("snapshots").await.unwrap(),
+        vec!["jobs", "tables"],
+        "the folders under snapshots/"
+    );
+    assert!(s.folders("missing").await.unwrap().is_empty());
 
     s.delete("snapshots/jobs/j1.json").await.expect("delete");
     assert_eq!(s.get("snapshots/jobs/j1.json").await.unwrap(), None);
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state --lib events:: && cargo test -p data-avalanche-storage folders`

Expected: FAIL. The first errors, as measured:

```text
error[E0425]: cannot find type `DateTime` in this scope
   --> crates/data-avalanche-state/src/events.rs:11:40
error[E0425]: cannot find type `Utc` in this scope
   --> crates/data-avalanche-state/src/events.rs:11:49
error[E0425]: cannot find value `Utc` in this scope
   --> crates/data-avalanche-state/src/events.rs:12:9
error[E0425]: cannot find type `Events` in this scope
   --> crates/data-avalanche-state/src/events.rs:15:21
error[E0433]: cannot find type `DocStore` in this scope
   --> crates/data-avalanche-state/src/events.rs:18:22
error[E0425]: cannot find type `EventDoc` in this scope
   --> crates/data-avalanche-state/src/events.rs:21:59
…
```

- [ ] **Step 3: Implement**

`crates/data-avalanche-state/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-state/Cargo.toml b/crates/data-avalanche-state/Cargo.toml
index a59a551..9c58f49 100644
--- a/crates/data-avalanche-state/Cargo.toml
+++ b/crates/data-avalanche-state/Cargo.toml
@@ -15,6 +15,7 @@ bytes.workspace = true
 chrono.workspace = true
 data-avalanche-core.workspace = true
 data-avalanche-storage.workspace = true
+futures-util.workspace = true
 serde.workspace = true
 serde_json.workspace = true
 thiserror.workspace = true
@@ -24,6 +25,5 @@ tracing.workspace = true
 async-trait.workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-testkit.workspace = true
-futures-util.workspace = true
 object_store.workspace = true
 tokio.workspace = true
```

`crates/data-avalanche-state/src/events.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/events.rs b/crates/data-avalanche-state/src/events.rs
index 11a7738..f1bf646 100644
--- a/crates/data-avalanche-state/src/events.rs
+++ b/crates/data-avalanche-state/src/events.rs
@@ -1,3 +1,312 @@
+//! The cluster's events and audit trail (Admin_Console §6): one small document per event under
+//! `globals.state`, `events/<YYYY-MM-DD>/<HHMMSS.ffffff>Z-<seq>-<node>.json`. Object storage has no
+//! append, so each event is its own object, created once and never changed. A key orders by time,
+//! so a day lists in order, and a whole day expires by deleting its folder.
+
+use std::collections::BTreeMap;
+use std::sync::atomic::{AtomicU64, Ordering};
+use std::time::Duration;
+
+use chrono::{DateTime, NaiveDate, Utc};
+use data_avalanche_storage::{StoreError, escape_segment};
+use futures_util::{StreamExt, TryStreamExt, stream};
+use serde::{Deserialize, Serialize};
+
+use crate::{DocStore, Document, StateError};
+
+/// The folder of every event, under `globals.state`.
+pub const EVENTS_PREFIX: &str = "events";
+
+/// The most events one [`Events::list`] returns.
+pub const MAX_EVENTS: usize = 1000;
+
+/// The most event documents one [`Events::list`] reads: a narrow filter over a long range stops
+/// here and hands back where it stopped.
+pub const MAX_SCANNED: usize = 5000;
+
+/// How many documents a listing reads at once.
+const READ_CONCURRENCY: usize = 16;
+
+/// How serious an event is. A person's or a token's action carries an [`Actor`] besides.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
+#[serde(rename_all = "snake_case")]
+pub enum EventSeverity {
+    Info,
+    Warning,
+    Error,
+}
+
+/// Who caused an event: the name and role the server verified (never what a page says).
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct Actor {
+    pub name: String,
+    pub role: String,
+}
+
+/// One event.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct EventDoc {
+    pub at: DateTime<Utc>,
+    /// Dotted, the family first: `action.pipeline.pause`, `lease.takeover`, `table.halted`.
+    pub kind: String,
+    pub severity: EventSeverity,
+    /// The node that recorded it.
+    pub node: String,
+    /// The pipeline it concerns, by id.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub pipeline: Option<String>,
+    /// What else it concerns, by name: a table, a job id, a node, a config generation.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub object: Option<String>,
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub actor: Option<Actor>,
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub note: Option<String>,
+    /// How it ended: `accepted`, `refused`, `done`.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub outcome: Option<String>,
+    /// The sentence a page shows.
+    pub text: String,
+    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
+    pub detail: BTreeMap<String, String>,
+}
+
+impl Document for EventDoc {
+    const KIND: &'static str = "event";
+    const SCHEMA_VERSION: u64 = 1;
+}
+
+impl EventDoc {
+    /// An event at `at`, recorded by `node`, with nothing else set.
+    pub fn new(
+        at: DateTime<Utc>,
+        node: &str,
+        kind: &str,
+        severity: EventSeverity,
+        text: impl Into<String>,
+    ) -> Self {
+        Self {
+            at,
+            kind: kind.to_owned(),
+            severity,
+            node: node.to_owned(),
+            pipeline: None,
+            object: None,
+            actor: None,
+            note: None,
+            outcome: None,
+            text: text.into(),
+            detail: BTreeMap::new(),
+        }
+    }
+}
+
+/// What [`Events::list`] returns: the newest first.
+#[derive(Debug, Clone, Default, PartialEq, Eq)]
+pub struct EventQuery {
+    /// Events at or after.
+    pub since: Option<DateTime<Utc>>,
+    /// Events before.
+    pub until: Option<DateTime<Utc>>,
+    /// Kinds, each a whole kind or a family: `action` matches `action.pipeline.pause`.
+    pub kinds: Vec<String>,
+    /// A pipeline id, or an object's name.
+    pub object: Option<String>,
+    /// An actor's name.
+    pub actor: Option<String>,
+    /// The recording node.
+    pub node: Option<String>,
+    /// Text in the event's sentence or note, without case.
+    pub text: Option<String>,
+    /// At most this many; 0 and anything above [`MAX_EVENTS`] mean [`MAX_EVENTS`].
+    pub limit: usize,
+    /// Continue after this key, from a page's `next`.
+    pub after: Option<String>,
+}
+
+/// One page of events.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct EventPage {
+    /// Each with its key, newest first.
+    pub events: Vec<(String, EventDoc)>,
+    /// Set when the listing stopped early: pass it as [`EventQuery::after`] for the rest.
+    pub next: Option<String>,
+}
+
+/// The key of an event at `at`: `events/<day>/<HHMMSS.ffffff>Z-<seq>-<node>.json`.
+pub fn event_key(at: DateTime<Utc>, seq: u64, node: &str) -> String {
+    format!(
+        "{EVENTS_PREFIX}/{}/{}Z-{seq:06}-{}.json",
+        at.format("%Y-%m-%d"),
+        at.format("%H%M%S%.6f"),
+        escape_segment(node),
+    )
+}
+
+/// The cluster's events in `globals.state`.
+#[derive(Debug)]
+pub struct Events {
+    docs: DocStore,
+    seq: AtomicU64,
+}
+
+impl Events {
+    pub fn new(docs: DocStore) -> Self {
+        Self {
+            docs,
+            seq: AtomicU64::new(0),
+        }
+    }
+
+    /// Writes `event` under a new key; a key taken already (another process of the same node in
+    /// the same microsecond) takes the next sequence number.
+    ///
+    /// # Errors
+    /// A storage failure.
+    pub async fn record(&self, event: &EventDoc) -> Result<String, StateError> {
+        loop {
+            let seq = self.seq.fetch_add(1, Ordering::Relaxed) % 1_000_000;
+            let key = event_key(event.at, seq, &event.node);
+            match self.docs.create(&key, event).await {
+                Ok(_) => return Ok(key),
+                Err(StateError::Store(StoreError::Conflict { .. })) => {}
+                Err(e) => return Err(e),
+            }
+        }
+    }
+
+    /// The events `query` selects, newest first. Reads the days from `until` back to `since`
+    /// (or to the oldest), stopping once it has `limit` events or has read [`MAX_SCANNED`]
+    /// documents; then [`EventPage::next`] says where to go on. A document that can't be read
+    /// (another schema version, corrupt) is left out with a warning.
+    ///
+    /// # Errors
+    /// A storage failure.
+    pub async fn list(&self, query: &EventQuery) -> Result<EventPage, StateError> {
+        let limit = match query.limit {
+            0 => MAX_EVENTS,
+            n => n.min(MAX_EVENTS),
+        };
+        let store = self.docs.store();
+        let mut days = store.folders(EVENTS_PREFIX).await?;
+        days.retain(|d| d.parse::<NaiveDate>().is_ok());
+        days.reverse();
+        let first_day = query.until.map(|t| t.format("%Y-%m-%d").to_string());
+        let last_day = query.since.map(|t| t.format("%Y-%m-%d").to_string());
+        let mut out = Vec::new();
+        let mut scanned = 0;
+        for day in days {
+            if first_day.as_ref().is_some_and(|f| day > *f)
+                || query.after.as_ref().is_some_and(|a| {
+                    a.strip_prefix(&format!("{EVENTS_PREFIX}/"))
+                        .is_some_and(|rest| *rest < *day)
+                })
+            {
+                continue;
+            }
+            if last_day.as_ref().is_some_and(|l| day < *l) {
+                break;
+            }
+            let mut keys = store.list(&format!("{EVENTS_PREFIX}/{day}")).await?;
+            keys.retain(|k| k.ends_with(".json"));
+            keys.reverse();
+            if let Some(after) = &query.after {
+                keys.retain(|k| k < after);
+            }
+            for chunk in keys.chunks(READ_CONCURRENCY * 4) {
+                let read: Vec<(String, Option<EventDoc>)> = stream::iter(chunk.iter().cloned())
+                    .map(|key| async move {
+                        match self.docs.load::<EventDoc>(&key).await {
+                            Ok(doc) => Ok((key, doc.map(|v| v.doc))),
+                            Err(StateError::Store(e)) => Err(StateError::Store(e)),
+                            Err(e) => {
+                                tracing::warn!(key, "an event left out of the listing: {e}");
+                                Ok((key, None))
+                            }
+                        }
+                    })
+                    .buffered(READ_CONCURRENCY)
+                    .try_collect()
+                    .await?;
+                for (key, doc) in read {
+                    scanned += 1;
+                    if let Some(doc) = doc
+                        && matches(query, &doc)
+                    {
+                        out.push((key.clone(), doc));
+                    }
+                    if out.len() == limit || scanned == MAX_SCANNED {
+                        return Ok(EventPage {
+                            events: out,
+                            next: Some(key),
+                        });
+                    }
+                }
+            }
+        }
+        Ok(EventPage {
+            events: out,
+            next: None,
+        })
+    }
+
+    /// Deletes every event of the days that ended `retention` or more before `now`, and returns
+    /// how many. Whole days only: an event lives at least `retention`, at most a day more. Safe
+    /// on every node at once.
+    ///
+    /// # Errors
+    /// A storage failure; what was deleted before it stays deleted.
+    pub async fn prune(
+        &self,
+        retention: Duration,
+        now: DateTime<Utc>,
+    ) -> Result<usize, StateError> {
+        let cutoff = now - chrono::Duration::from_std(retention).unwrap_or(chrono::Duration::MAX);
+        let keep_from = cutoff.format("%Y-%m-%d").to_string();
+        let store = self.docs.store();
+        let mut deleted = 0;
+        for day in store.folders(EVENTS_PREFIX).await? {
+            if day.parse::<NaiveDate>().is_err() || day >= keep_from {
+                continue;
+            }
+            for key in store.list(&format!("{EVENTS_PREFIX}/{day}")).await? {
+                store.delete(&key).await?;
+                deleted += 1;
+            }
+        }
+        Ok(deleted)
+    }
+}
+
+fn matches(q: &EventQuery, e: &EventDoc) -> bool {
+    let kind = q.kinds.is_empty()
+        || q.kinds.iter().any(|k| {
+            e.kind == *k
+                || e.kind
+                    .strip_prefix(k.as_str())
+                    .is_some_and(|rest| rest.starts_with('.'))
+        });
+    let text = q.text.as_ref().is_none_or(|t| {
+        let t = t.to_lowercase();
+        e.text.to_lowercase().contains(&t)
+            || e.note
+                .as_ref()
+                .is_some_and(|n| n.to_lowercase().contains(&t))
+    });
+    kind && text
+        && q.since.is_none_or(|s| e.at >= s)
+        && q.until.is_none_or(|u| e.at < u)
+        && q.object
+            .as_ref()
+            .is_none_or(|o| e.pipeline.as_ref() == Some(o) || e.object.as_ref() == Some(o))
+        && q.actor
+            .as_ref()
+            .is_none_or(|a| e.actor.as_ref().is_some_and(|x| x.name == *a))
+        && q.node.as_ref().is_none_or(|n| e.node == *n)
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::Arc;
```

`crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 7853305..3783958 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -14,6 +14,8 @@
 //!   with a random key, establish the anchor, run the config crate's `check_cluster_anchor`,
 //!   adopt or bootstrap the config generation ([`ConfigGenerations::bootstrap_or_adopt`]), then
 //!   load its text with the anchor's key (`tests/startup.rs` runs the whole sequence).
+//! - [`Events`]: `events/<YYYY-MM-DD>/…json`, the cluster's events and audit trail, one document
+//!   per event, a day expiring whole (Admin_Console §6).
 //! - [`ConfigGenerations`]: `config/generation.json` (CAS pointer) and content-addressed
 //!   `config/snapshots/<NNNNNN>-<hash>.yaml` holding the raw config text. A starting node adopts
 //!   the current generation; the first one bootstraps generation 1 (Clustering §9.3).
@@ -33,6 +35,7 @@
 
 mod anchor;
 mod doc;
+mod events;
 mod generation;
 mod lease;
 mod lifecycle;
@@ -42,10 +45,13 @@ mod snapshot;
 mod tables;
 #[cfg(test)]
 mod test_support;
-mod events;
 
 pub use anchor::{ANCHOR_KEY, Anchor, AnchorDoc, HashAlgo, establish_anchor};
 pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
+pub use events::{
+    Actor, EVENTS_PREFIX, EventDoc, EventPage, EventQuery, EventSeverity, Events, MAX_EVENTS,
+    MAX_SCANNED, event_key,
+};
 pub use generation::{
     ConfigGeneration, ConfigGenerations, GENERATION_KEY, GenerationDoc, config_hash,
     config_snapshot_key,
```

`crates/data-avalanche-storage/src/store.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/store.rs b/crates/data-avalanche-storage/src/store.rs
index 5026302..49c13b5 100644
--- a/crates/data-avalanche-storage/src/store.rs
+++ b/crates/data-avalanche-storage/src/store.rs
@@ -90,6 +90,25 @@ pub trait VersionedStore: Send + Sync + std::fmt::Debug {
     /// Every key under `prefix/`, sorted (`a/b` lists `a/b/c`, not `a/bc`); `""` lists the whole
     /// root.
     async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
+    /// The next segment of every key under `prefix/`, once each and sorted: `events` lists
+    /// `events/2026-11-14/x.json` as `2026-11-14`. One listing of the folders, not of every key,
+    /// where the store can (the default lists every key).
+    async fn folders(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
+        let under = if prefix.is_empty() {
+            String::new()
+        } else {
+            format!("{prefix}/")
+        };
+        let mut out: Vec<String> = self
+            .list(prefix)
+            .await?
+            .iter()
+            .filter_map(|k| k.strip_prefix(&under))
+            .filter_map(|rest| rest.split_once('/').map(|(folder, _)| folder.to_owned()))
+            .collect();
+        out.dedup();
+        Ok(out)
+    }
     /// Deletes unconditionally; an absent object is no error. For documents no one updates any
     /// more (an ended snapshot job, DA-SNAP-16): never a lease or a pointer (deleting a lease
     /// restarts its epoch).
@@ -250,6 +269,26 @@ impl VersionedStore for ObjectStoreVersioned {
         Ok(keys)
     }
 
+    async fn folders(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
+        let path = if prefix.is_empty() {
+            self.root.clone()
+        } else {
+            self.path(prefix)?
+        };
+        let listed = self
+            .store
+            .list_with_delimiter(Some(&path))
+            .await
+            .map_err(|e| map_error(Op::List, prefix, e))?;
+        let mut out: Vec<String> = listed
+            .common_prefixes
+            .iter()
+            .filter_map(|p| p.filename().map(str::to_owned))
+            .collect();
+        out.sort();
+        Ok(out)
+    }
+
     async fn delete(&self, key: &str) -> Result<(), StoreError> {
         let path = self.path(key)?;
         match self.store.delete(&path).await {
@@ -394,7 +433,11 @@ mod tests {
             s.put(k, b("x")).await.unwrap();
         }
         let want = vec!["2026-11-13", "2026-11-14"];
-        assert_eq!(s.folders("events").await.unwrap(), want, "object_store's listing");
+        assert_eq!(
+            s.folders("events").await.unwrap(),
+            want,
+            "object_store's listing"
+        );
         assert_eq!(
             ListOnly(s.clone()).folders("events").await.unwrap(),
             want,
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state --lib events:: && cargo test -p data-avalanche-storage folders`

Expected: PASS, 7 tests.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings && cargo test --workspace`

Expected: no output from the first three; every test binary passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state crates/data-avalanche-storage
git commit -m "feat(state): the cluster's events in globals.state, a day expiring whole; VersionedStore::folders"
```

---

### Task 3: The recorder and its counters (`data-avalanche-cluster`, `data-avalanche-telemetry`)

**Files:**
- Create: `crates/data-avalanche-cluster/src/events.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-telemetry/src/lib.rs`
- Test: `crates/data-avalanche-telemetry/tests/metrics.rs`

**Interfaces:**
- Consumes: Task 2's `Events`, `EventDoc`, `EventSeverity`; `Metrics`.
- Produces:
  - `Metrics::event_written()`, `Metrics::event_dropped()`: `data_avalanche_events_written_total`,
    `data_avalanche_events_dropped_total`, node counters without labels;
  - `EVENT_QUEUE = 1024`, `WRITE_RETRIES = [200 ms, 1 s, 5 s]`, `PRUNE_EVERY = 3600 s`;
  - `Recorder` (`Clone`): `start(events: Arc<Events>, node: &str, metrics: Metrics) -> (Recorder,
    JoinHandle<()>)`, `off(node: &str) -> Recorder` (writes nothing), `node(&self) -> &str`,
    `event(&self, kind, severity, text) -> EventDoc` (now, this node), `record(&self, EventDoc)`
    (never waits);
  - `expire_events(events: Arc<Events>, retention: watch::Receiver<Duration>)`: the first prune a
    minute after the start, then every `PRUNE_EVERY`.

D-M13a.3.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-cluster/src/events.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
new file mode 100644
index 0000000..719d8ac
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -0,0 +1,124 @@
+#[cfg(test)]
+mod tests {
+    use std::sync::atomic::{AtomicUsize, Ordering};
+
+    use async_trait::async_trait;
+    use bytes::Bytes;
+    use data_avalanche_state::{DocStore, EventQuery};
+    use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
+    use object_store::memory::InMemory;
+
+    use super::*;
+
+    /// A store whose first `fail` creates fail.
+    #[derive(Debug)]
+    struct Failing {
+        inner: ObjectStoreVersioned,
+        fail: AtomicUsize,
+    }
+
+    #[async_trait]
+    impl VersionedStore for Failing {
+        async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
+            self.inner.get(key).await
+        }
+        async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
+            if self
+                .fail
+                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
+                .is_ok()
+            {
+                return Err(StoreError::Transient {
+                    key: key.into(),
+                    reason: "injected".into(),
+                });
+            }
+            self.inner.create(key, body).await
+        }
+        async fn update(
+            &self,
+            key: &str,
+            expected: &ObjectVersion,
+            body: Bytes,
+        ) -> Result<ObjectVersion, StoreError> {
+            self.inner.update(key, expected, body).await
+        }
+        async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
+            self.inner.put(key, body).await
+        }
+        async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
+            self.inner.list(prefix).await
+        }
+        async fn delete(&self, key: &str) -> Result<(), StoreError> {
+            self.inner.delete(key).await
+        }
+    }
+
+    fn store(fail: usize) -> Arc<Events> {
+        let inner = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state").unwrap();
+        Arc::new(Events::new(DocStore::new(Arc::new(Failing {
+            inner,
+            fail: AtomicUsize::new(fail),
+        }))))
+    }
+
+    /// The texts once `n` events are written, waiting up to 20 s (of paused time: the retries').
+    async fn written(events: &Events, n: usize) -> Vec<String> {
+        for _ in 0..200 {
+            let page = events.list(&EventQuery::default()).await.unwrap();
+            if page.events.len() >= n {
+                return page.events.into_iter().map(|(_, e)| e.text).collect();
+            }
+            tokio::time::sleep(Duration::from_millis(100)).await;
+        }
+        panic!("fewer than {n} events written");
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn a_failing_store_is_retried_then_the_event_lands() {
+        let events = store(2);
+        let metrics = Metrics::new();
+        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone());
+        r.record(r.event("member.joined", EventSeverity::Info, "node-c joined"));
+        assert_eq!(written(&events, 1).await, ["node-c joined"]);
+        assert!(
+            metrics
+                .encode()
+                .contains("data_avalanche_events_written_total 1")
+        );
+    }
+
+    #[tokio::test(start_paused = true)]
+    async fn a_store_that_keeps_failing_drops_the_event_and_counts_it() {
+        let events = store(WRITE_RETRIES.len() + 1);
+        let metrics = Metrics::new();
+        let (r, _task) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone());
+        r.record(r.event("member.joined", EventSeverity::Info, "lost"));
+        r.record(r.event("member.joined", EventSeverity::Info, "kept"));
+        assert_eq!(written(&events, 1).await, ["kept"]);
+        let text = metrics.encode();
+        assert!(
+            text.contains("data_avalanche_events_dropped_total 1"),
+            "{text}"
+        );
+    }
+
+    #[tokio::test]
+    async fn a_full_queue_drops_without_waiting() {
+        let metrics = Metrics::new();
+        let (tx, _rx) = mpsc::channel(1);
+        let r = Recorder {
+            node: "node-a".into(),
+            tx: Some(tx),
+            metrics: metrics.clone(),
+        };
+        r.record(r.event("a.b", EventSeverity::Info, "1"));
+        r.record(r.event("a.b", EventSeverity::Info, "2"));
+        assert!(
+            metrics
+                .encode()
+                .contains("data_avalanche_events_dropped_total 1")
+        );
+        Recorder::off("node-a").record(r.event("a.b", EventSeverity::Info, "nowhere"));
+    }
+}
```

`crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index c0d9bc8..e202a2f 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -20,6 +20,7 @@ pub mod ring;
 #[cfg(test)]
 mod test_support;
 pub mod wire;
+pub mod events;
 
 pub use claim::{Claim, Claimed, release};
 pub use controller::{
```

`crates/data-avalanche-telemetry/tests/metrics.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/metrics.rs b/crates/data-avalanche-telemetry/tests/metrics.rs
index b1dfa3c..6cfdf69 100644
--- a/crates/data-avalanche-telemetry/tests/metrics.rs
+++ b/crates/data-avalanche-telemetry/tests/metrics.rs
@@ -371,3 +371,21 @@ fn the_node_reports_its_config_generation() {
         "{text}"
     );
 }
+
+/// M13a: the event recorder's two node counters, without labels.
+#[test]
+fn the_event_counters_count() {
+    let m = Metrics::new();
+    m.event_written();
+    m.event_written();
+    m.event_dropped();
+    let text = m.encode();
+    assert_eq!(
+        samples(&text, "data_avalanche_events_written_total"),
+        ["data_avalanche_events_written_total 2"]
+    );
+    assert_eq!(
+        samples(&text, "data_avalanche_events_dropped_total"),
+        ["data_avalanche_events_dropped_total 1"]
+    );
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib events:: && cargo test -p data-avalanche-telemetry --test metrics`

Expected: FAIL. The first errors, as measured:

```text
error[E0425]: cannot find type `Arc` in this scope
   --> crates/data-avalanche-cluster/src/events.rs:57:30
error[E0425]: cannot find type `Events` in this scope
   --> crates/data-avalanche-cluster/src/events.rs:57:34
error[E0433]: cannot find type `Arc` in this scope
   --> crates/data-avalanche-cluster/src/events.rs:58:47
error[E0433]: cannot find type `Duration` in this scope
   --> crates/data-avalanche-cluster/src/events.rs:72:32
error[E0425]: cannot find value `WRITE_RETRIES` in this scope
   --> crates/data-avalanche-cluster/src/events.rs:93:28
error[E0433]: cannot find module or crate `mpsc` in this scope
   --> crates/data-avalanche-cluster/src/events.rs:109:25
…
```

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/events.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
index 719d8ac..7a52726 100644
--- a/crates/data-avalanche-cluster/src/events.rs
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -1,3 +1,125 @@
+//! The recorder (Admin_Console §6): whatever records an event hands it over without waiting. One
+//! task per node writes them to the cluster's timeline in `globals.state`, retrying a failing
+//! store a few times; an event it can't write is counted (`data_avalanche_events_dropped_total`)
+//! and logged, never allowed to slow or fail what it records. Another task expires the days past
+//! `globals.console.events.retention`.
+
+use std::sync::Arc;
+use std::time::Duration;
+
+use chrono::Utc;
+use data_avalanche_state::{EventDoc, EventSeverity, Events};
+use data_avalanche_telemetry::Metrics;
+use tokio::sync::{mpsc, watch};
+use tokio::task::JoinHandle;
+
+/// Events waiting to be written; past this, new ones are dropped and counted.
+pub const EVENT_QUEUE: usize = 1024;
+
+/// The waits between a failed write and its next attempt; after the last, the event is dropped.
+pub const WRITE_RETRIES: [Duration; 3] = [
+    Duration::from_millis(200),
+    Duration::from_secs(1),
+    Duration::from_secs(5),
+];
+
+/// How often the expired days are deleted.
+pub const PRUNE_EVERY: Duration = Duration::from_secs(3600);
+
+/// Hands events to the node's writer; cheap to clone.
+#[derive(Debug, Clone)]
+pub struct Recorder {
+    node: Arc<str>,
+    tx: Option<mpsc::Sender<EventDoc>>,
+    metrics: Metrics,
+}
+
+impl Recorder {
+    /// The recorder of `node`, and its writer task.
+    pub fn start(events: Arc<Events>, node: &str, metrics: Metrics) -> (Self, JoinHandle<()>) {
+        let (tx, rx) = mpsc::channel(EVENT_QUEUE);
+        let task = tokio::spawn(write(events, rx, metrics.clone()));
+        (
+            Self {
+                node: node.into(),
+                tx: Some(tx),
+                metrics,
+            },
+            task,
+        )
+    }
+
+    /// A recorder that writes nothing (tests, a node without a timeline).
+    pub fn off(node: &str) -> Self {
+        Self {
+            node: node.into(),
+            tx: None,
+            metrics: Metrics::new(),
+        }
+    }
+
+    pub fn node(&self) -> &str {
+        &self.node
+    }
+
+    /// An event of this node, now.
+    pub fn event(&self, kind: &str, severity: EventSeverity, text: impl Into<String>) -> EventDoc {
+        EventDoc::new(Utc::now(), &self.node, kind, severity, text)
+    }
+
+    /// Queues `event`; never waits. A full queue drops it, counted and logged.
+    pub fn record(&self, event: EventDoc) {
+        let Some(tx) = &self.tx else {
+            return;
+        };
+        if let Err(e) = tx.try_send(event) {
+            self.metrics.event_dropped();
+            let event = match e {
+                mpsc::error::TrySendError::Full(ev) | mpsc::error::TrySendError::Closed(ev) => ev,
+            };
+            tracing::warn!(kind = %event.kind, "an event dropped, the recorder's queue is full or closed: {}", event.text);
+        }
+    }
+}
+
+async fn write(events: Arc<Events>, mut rx: mpsc::Receiver<EventDoc>, metrics: Metrics) {
+    while let Some(event) = rx.recv().await {
+        let mut waits = WRITE_RETRIES.iter();
+        loop {
+            match events.record(&event).await {
+                Ok(_) => {
+                    metrics.event_written();
+                    break;
+                }
+                Err(e) => match waits.next() {
+                    Some(wait) => tokio::time::sleep(*wait).await,
+                    None => {
+                        metrics.event_dropped();
+                        tracing::warn!(kind = %event.kind, "an event dropped, the store keeps failing: {e}: {}", event.text);
+                        break;
+                    }
+                },
+            }
+        }
+    }
+}
+
+/// Deletes the days past the retention `retention` holds now, every [`PRUNE_EVERY`] (the first a
+/// minute after the start), until the task is aborted. Every node runs it: a day deleted twice
+/// is no error.
+pub async fn expire_events(events: Arc<Events>, retention: watch::Receiver<Duration>) {
+    tokio::time::sleep(Duration::from_secs(60)).await;
+    loop {
+        let keep = *retention.borrow();
+        match events.prune(keep, Utc::now()).await {
+            Ok(0) => {}
+            Ok(n) => tracing::info!(deleted = n, "expired events deleted"),
+            Err(e) => tracing::warn!("expired events undeleted, tried again later: {e}"),
+        }
+        tokio::time::sleep(PRUNE_EVERY).await;
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::atomic::{AtomicUsize, Ordering};
```

`crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index e202a2f..277b4a3 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -10,6 +10,7 @@ pub mod controller;
 pub mod coordinator;
 pub mod discovery;
 mod error;
+pub mod events;
 pub mod gossip;
 mod jobs;
 pub mod lease;
@@ -20,7 +21,6 @@ pub mod ring;
 #[cfg(test)]
 mod test_support;
 pub mod wire;
-pub mod events;
 
 pub use claim::{Claim, Claimed, release};
 pub use controller::{
@@ -34,6 +34,7 @@ pub use coordinator::{
 };
 pub use discovery::{Seeds, advertise, discover, interfaces};
 pub use error::ClusterError;
+pub use events::{EVENT_QUEUE, PRUNE_EVERY, Recorder, WRITE_RETRIES, expire_events};
 pub use gossip::{
     Advertiser, Announcer, Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase,
 };
```

`crates/data-avalanche-telemetry/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/src/lib.rs b/crates/data-avalanche-telemetry/src/lib.rs
index e17757c..84fed04 100644
--- a/crates/data-avalanche-telemetry/src/lib.rs
+++ b/crates/data-avalanche-telemetry/src/lib.rs
@@ -211,6 +211,8 @@ struct Families {
     cluster_members: Gauge,
     config_generation: Gauge,
     ring_generation: Gauge,
+    events_written: Counter,
+    events_dropped: Counter,
     wal_lag_bytes: Family<Labels, Gauge>,
     source_reconnects: Family<Labels, Counter>,
     snapshot_rows_done: Family<Labels, Gauge>,
@@ -272,6 +274,8 @@ impl Metrics {
             cluster_members: Gauge::default(),
             config_generation: Gauge::default(),
             ring_generation: Gauge::default(),
+            events_written: Counter::default(),
+            events_dropped: Counter::default(),
             wal_lag_bytes: Family::default(),
             source_reconnects: Family::default(),
             snapshot_rows_done: Family::default(),
@@ -325,6 +329,16 @@ impl Metrics {
             "Members of the cluster this node sees",
             f.cluster_members.clone(),
         );
+        r.register(
+            "events_written",
+            "Events this node wrote to the cluster's timeline",
+            f.events_written.clone(),
+        );
+        r.register(
+            "events_dropped",
+            "Events this node couldn't write: its queue was full, or the store kept failing",
+            f.events_dropped.clone(),
+        );
         r.register(
             "config_generation",
             "The config generation this node applies",
@@ -457,6 +471,16 @@ impl Metrics {
         self.families.config_generation.set(gauge(generation));
     }
 
+    /// An event written to the cluster's timeline.
+    pub fn event_written(&self) {
+        self.families.events_written.inc();
+    }
+
+    /// An event not written: its queue was full, or the store kept failing.
+    pub fn event_dropped(&self) {
+        self.families.events_dropped.inc();
+    }
+
     pub fn set_ring_generation(&self, generation: u64) {
         self.families.ring_generation.set(gauge(generation));
     }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib events:: && cargo test -p data-avalanche-telemetry --test metrics`

Expected: PASS, 16 tests.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings && cargo test --workspace`

Expected: no output from the first three; every test binary passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster crates/data-avalanche-telemetry
git commit -m "feat(cluster): the event recorder, never waited on, and the expiry of old days; two event counters"
```

---

### Task 4: Who did it, and `GET /v1/events` (`data-avalanche-api`)

**Files:**
- Create: `crates/data-avalanche-api/src/actions.rs`, `crates/data-avalanche-api/src/events.rs`
- Modify: `crates/data-avalanche-api/src/{lib,limit,snapshots,openapi,test_support}.rs`, `Cargo.toml`, `openapi.json` (blessed: `DATA_AVALANCHE_BLESS=1`)
- Modify: `crates/data-avalanche-state/src/snapshot.rs` (`SnapshotJobDoc::note`)
- Modify: `crates/data-avalanche/src/run.rs` (the node's store, recorder, expiry; the reload's retention)
- Modify: `xtask/src/api.rs`; regenerate `docs/src/content/docs/reference/api.md`, `docs/public/openapi.json` (`cargo xtask docs`)

**Interfaces:**
- Consumes: Task 2's `Events`, `EventQuery`, `EventDoc`, `Actor`; Task 3's `Recorder`,
  `expire_events`; Task 1's `globals.console.events.retention`; M9a's route table, `Auth`,
  `Principal`, `RateLimit`.
- Produces:
  - `MAX_NOTE = 500`, `DENIALS_BURST = 30`, `Recorded { kind: &'static str, verb: &'static str }`,
    `note_of(body: &[u8]) -> Result<Option<String>, String>` (re-exported);
  - `RateLimit::denials()` (30, then 1 a second);
  - `Node` gains `recorder: Recorder`, `events: Arc<Events>`, `denials: Arc<RateLimit>`;
  - a route's `records: Option<Recorded>`; the router's layers: the role gate outside, the
    recording inside; `GET /v1/events` (viewer) with `events::PARAMETERS` in the OpenAPI document;
  - `SnapshotJobDoc::note: Option<String>`, written only when set.

D-M13a.4, D-M13a.5, D-M13a.8. The query is decoded with `url::form_urlencoded` from `RawQuery` (the
workspace's `axum` has no `query` feature).

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/actions.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/actions.rs b/crates/data-avalanche-api/src/actions.rs
new file mode 100644
index 0000000..158595e
--- /dev/null
+++ b/crates/data-avalanche-api/src/actions.rs
@@ -0,0 +1,242 @@
+#[cfg(test)]
+mod tests {
+    use std::net::SocketAddr;
+    use std::sync::Arc;
+    use std::time::Duration;
+
+    use data_avalanche_state::{EventDoc, EventQuery, Events};
+    use tempfile::TempDir;
+
+    use super::*;
+    use crate::test_support::{CONFIG, Issuer, Lone, api, lone, open, request, serve};
+    use crate::{Api, Auth};
+
+    /// The lone node's API under `auth`, its pipeline `orders` known.
+    async fn served(auth: Arc<Auth>) -> (SocketAddr, Lone, TempDir) {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, CONFIG).unwrap();
+        let lone = lone(file).await;
+        let a: Api = api(auth);
+        a.node.set(lone.node.clone()).unwrap();
+        (serve(a).await, lone, dir)
+    }
+
+    /// The events once there are `n`, newest first.
+    async fn events(events: &Events, n: usize) -> Vec<EventDoc> {
+        for _ in 0..200 {
+            let page = events.list(&EventQuery::default()).await.unwrap();
+            if page.events.len() >= n {
+                return page.events.into_iter().map(|(_, e)| e).collect();
+            }
+            tokio::time::sleep(Duration::from_millis(10)).await;
+        }
+        panic!("fewer than {n} events");
+    }
+
+    #[tokio::test]
+    async fn an_action_is_recorded_with_its_actor_note_and_outcome() {
+        let (addr, lone, _dir) = served(open()).await;
+        let json = [("Content-Type", "application/json")];
+        let (code, _, body) = request(
+            addr,
+            "POST",
+            "/v1/pipelines/orders/pause",
+            &json,
+            r#"{"note": "rotate the replica's certificate"}"#,
+        )
+        .await;
+        assert_eq!(code, 202, "{body}");
+        let (code, _, _) = request(addr, "POST", "/v1/pipelines/orders/pause", &[], "").await;
+        assert_eq!(code, 200, "paused already: no body, no note");
+        let (code, _, body) = request(addr, "POST", "/v1/pipelines/orders/restart", &[], "").await;
+        assert_eq!(code, 409, "{body}");
+        let got = events(&lone.events, 3).await;
+        let summary: Vec<_> = got
+            .iter()
+            .map(|e| (e.kind.as_str(), e.outcome.as_deref(), e.severity))
+            .collect();
+        assert_eq!(
+            summary,
+            [
+                (
+                    "action.pipeline.restart",
+                    Some("refused"),
+                    EventSeverity::Warning
+                ),
+                (
+                    "action.pipeline.pause",
+                    Some("unchanged"),
+                    EventSeverity::Info
+                ),
+                (
+                    "action.pipeline.pause",
+                    Some("accepted"),
+                    EventSeverity::Info
+                ),
+            ]
+        );
+        let paused = &got[2];
+        assert_eq!(paused.text, "pause orders");
+        assert_eq!(paused.pipeline.as_deref(), Some("orders"));
+        assert_eq!(paused.node, "node-a");
+        assert_eq!(
+            paused.actor,
+            Some(Actor {
+                name: "anonymous".into(),
+                role: "admin".into()
+            }),
+            "auth none: anonymous, an admin"
+        );
+        assert_eq!(
+            paused.note.as_deref(),
+            Some("rotate the replica's certificate")
+        );
+        assert!(
+            got[0].detail["error"].contains("resume it first"),
+            "the refusal's reason: {:?}",
+            got[0].detail
+        );
+    }
+
+    #[tokio::test]
+    async fn a_bad_note_is_refused_before_the_route_runs() {
+        let (addr, lone, _dir) = served(open()).await;
+        let long = format!(r#"{{"note": "{}"}}"#, "x".repeat(MAX_NOTE + 1));
+        let (code, _, body) = request(addr, "POST", "/v1/pipelines/orders/stop", &[], &long).await;
+        assert_eq!(code, 422, "{body}");
+        assert!(body.contains("longer than 500"), "{body}");
+        let config = lone.node.coordinator.config().await;
+        let docs = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
+        assert_eq!(
+            docs.state().await.unwrap(),
+            None,
+            "the pipeline wasn't stopped"
+        );
+        tokio::time::sleep(Duration::from_millis(100)).await;
+        let page = lone.events.list(&EventQuery::default()).await.unwrap();
+        assert!(page.events.is_empty(), "nothing happened: {page:?}");
+    }
+
+    #[tokio::test]
+    async fn a_snapshot_job_keeps_its_note_and_the_event_names_it() {
+        let (addr, lone, _dir) = served(open()).await;
+        let config = lone.node.coordinator.config().await;
+        let docs = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
+        docs.open("orders", "delta").await.unwrap();
+        lone.leases
+            .acquire(
+                "orders",
+                &data_avalanche_state::Holder {
+                    node_id: "node-a".into(),
+                    incarnation: 1,
+                },
+                Duration::from_secs(30),
+                chrono::Utc::now(),
+            )
+            .await
+            .unwrap();
+        let (code, _, body) = request(
+            addr,
+            "POST",
+            "/v1/pipelines/orders/snapshots",
+            &[],
+            r#"{"tables": ["public.orders"], "note": " backfill after the outage "}"#,
+        )
+        .await;
+        assert_eq!(code, 202, "{body}");
+        let job_id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["job_id"]
+            .as_str()
+            .unwrap()
+            .to_owned();
+        let job = docs.snapshot_job(&job_id).await.unwrap().unwrap().doc;
+        assert_eq!(job.note.as_deref(), Some("backfill after the outage"));
+        let got = events(&lone.events, 1).await;
+        assert_eq!(got[0].kind, "action.snapshot.start");
+        assert_eq!(got[0].object.as_deref(), Some(job_id.as_str()));
+        assert_eq!(got[0].text, "start a snapshot job on orders");
+    }
+
+    #[tokio::test]
+    async fn a_caller_below_the_role_is_recorded_as_denied() {
+        let idp = Issuer::start().await;
+        let (addr, lone, _dir) = served(Arc::new(Auth::new(&idp.api("roles")))).await;
+        let token = format!("Bearer {}", idp.token(serde_json::json!(["viewer"])));
+        let auth = [("Authorization", token.as_str())];
+        let (code, _, _) = request(addr, "POST", "/v1/admin/shutdown", &auth, "").await;
+        assert_eq!(code, 403);
+        let (code, _, _) = request(addr, "GET", "/v1/pipelines", &auth, "").await;
+        assert_eq!(code, 200, "reading isn't recorded");
+        let got = events(&lone.events, 1).await;
+        assert_eq!(got.len(), 1, "{got:?}");
+        let e = &got[0];
+        assert_eq!(e.kind, "action.denied");
+        assert_eq!(e.text, "shut down node-a refused: needs admin");
+        assert_eq!(e.outcome.as_deref(), Some("denied"));
+        assert_eq!(e.object.as_deref(), Some("/v1/admin/shutdown"));
+        assert_eq!(e.actor.as_ref().map(|a| a.role.as_str()), Some("viewer"));
+        assert_eq!(
+            *lone.stops.borrow(),
+            crate::Stop::Run,
+            "nothing was shut down"
+        );
+    }
+
+    /// A script hammering a route its role doesn't allow is recorded for a burst, then once a
+    /// second: every call is still refused, and the timeline isn't flooded.
+    #[tokio::test]
+    async fn a_flood_of_refused_calls_is_recorded_for_a_burst_only() {
+        let idp = Issuer::start().await;
+        let (addr, lone, _dir) = served(Arc::new(Auth::new(&idp.api("roles")))).await;
+        let token = format!("Bearer {}", idp.token(serde_json::json!(["viewer"])));
+        let auth = [("Authorization", token.as_str())];
+        let started = std::time::Instant::now();
+        for _ in 0..DENIALS_BURST + 10 {
+            let (code, _, _) = request(addr, "POST", "/v1/admin/shutdown", &auth, "").await;
+            assert_eq!(code, 403);
+        }
+        let refilled = started.elapsed().as_secs() + 1;
+        let burst = usize::try_from(DENIALS_BURST).unwrap();
+        events(&lone.events, burst).await;
+        tokio::time::sleep(Duration::from_millis(200)).await;
+        let got = events(&lone.events, burst).await.len();
+        assert!(
+            got <= burst + usize::try_from(refilled).unwrap(),
+            "{got} recorded of {} refused",
+            burst + 10
+        );
+    }
+
+    #[test]
+    fn a_note_is_optional_trimmed_and_bounded() {
+        assert_eq!(note_of(b""), Ok(None));
+        assert_eq!(note_of(b"  \n"), Ok(None));
+        assert_eq!(note_of(b"{}"), Ok(None));
+        assert_eq!(note_of(br#"{"note": null}"#), Ok(None));
+        assert_eq!(note_of(br#"{"note": "   "}"#), Ok(None));
+        assert_eq!(
+            note_of(br#"{"note": " apply sink.prefix\nthen check "}"#),
+            Ok(Some("apply sink.prefix\nthen check".into()))
+        );
+        assert_eq!(
+            note_of(br#"{"tables": ["public.orders"], "note": "backfill"}"#),
+            Ok(Some("backfill".into())),
+            "beside a route's own fields"
+        );
+        let long = format!(r#"{{"note": "{}"}}"#, "é".repeat(MAX_NOTE + 1));
+        assert!(
+            note_of(long.as_bytes())
+                .unwrap_err()
+                .contains("longer than 500")
+        );
+        let exact = format!(r#"{{"note": "{}"}}"#, "é".repeat(MAX_NOTE));
+        assert!(
+            note_of(exact.as_bytes()).is_ok(),
+            "500 characters, not bytes"
+        );
+        assert!(note_of(br#"{"note": "a\u0007b"}"#).is_err());
+        assert!(note_of(br#"{"note": 5}"#).is_err());
+        assert!(note_of(b"[]").is_err());
+        assert!(note_of(b"note").is_err());
+    }
+}
```

`crates/data-avalanche-api/src/events.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/events.rs b/crates/data-avalanche-api/src/events.rs
new file mode 100644
index 0000000..2394f79
--- /dev/null
+++ b/crates/data-avalanche-api/src/events.rs
@@ -0,0 +1,107 @@
+#[cfg(test)]
+mod tests {
+    use chrono::TimeZone;
+    use data_avalanche_state::{Actor, EventSeverity};
+
+    use super::*;
+    use crate::test_support::{CONFIG, api, json_of, lone, open, serve};
+
+    fn at(h: u32, m: u32) -> DateTime<Utc> {
+        Utc.with_ymd_and_hms(2026, 11, 14, h, m, 0).unwrap()
+    }
+
+    #[test]
+    fn the_parameters_are_checked() {
+        let p = |pairs: &[(&str, &str)]| {
+            query(
+                &pairs
+                    .iter()
+                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
+                    .collect::<Vec<_>>(),
+            )
+        };
+        let q = p(&[("kind", "action, lease.takeover"), ("kind", "table")]).unwrap();
+        assert_eq!(q.kinds, ["action", "lease.takeover", "table"]);
+        assert_eq!(q.limit, 100);
+        assert_eq!(
+            p(&[("since", "2026-11-14T13:00:00+02:00")]).unwrap().since,
+            Some(at(11, 0))
+        );
+        for bad in [
+            &[("since", "yesterday")][..],
+            &[("limit", "0")],
+            &[("limit", "1001")],
+            &[("actor", "a"), ("actor", "b")],
+            &[("color", "red")],
+            &[
+                ("since", "2026-11-14T14:00:00Z"),
+                ("until", "2026-11-14T13:00:00Z"),
+            ],
+        ] {
+            let e = p(bad).unwrap_err();
+            assert_eq!(e.code, StatusCode::BAD_REQUEST, "{bad:?}");
+        }
+    }
+
+    #[tokio::test]
+    async fn the_events_are_listed_filtered_and_paged() {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, CONFIG).unwrap();
+        let lone = lone(file).await;
+        let mut pause = EventDoc::new(
+            at(10, 0),
+            "node-a",
+            "action.pipeline.pause",
+            EventSeverity::Info,
+            "pause orders",
+        );
+        pause.pipeline = Some("orders".into());
+        pause.actor = Some(Actor {
+            name: "Oleksii".into(),
+            role: "operator".into(),
+        });
+        let halt = EventDoc::new(
+            at(13, 41),
+            "node-b",
+            "table.halted",
+            EventSeverity::Error,
+            "public.refunds halted",
+        );
+        for e in [&pause, &halt] {
+            lone.events.record(e).await.unwrap();
+        }
+        let a = api(open());
+        a.node.set(lone.node.clone()).unwrap();
+        let addr = serve(a).await;
+
+        let (code, v) = json_of(addr, "GET", "/v1/events").await;
+        assert_eq!(code, 200, "{v}");
+        let kinds: Vec<_> = v["events"]
+            .as_array()
+            .unwrap()
+            .iter()
+            .map(|e| e["kind"].as_str().unwrap().to_owned())
+            .collect();
+        assert_eq!(kinds, ["table.halted", "action.pipeline.pause"]);
+        assert_eq!(v["next"], Value::Null);
+        let first = &v["events"][1];
+        assert_eq!(first["actor"]["name"], "Oleksii");
+        assert_eq!(first["severity"], "info");
+        assert!(
+            first["key"]
+                .as_str()
+                .unwrap()
+                .starts_with("events/2026-11-14/")
+        );
+
+        let (_, v) = json_of(addr, "GET", "/v1/events?actor=Oleksii&kind=action").await;
+        assert_eq!(v["events"].as_array().unwrap().len(), 1);
+        let (_, v) = json_of(addr, "GET", "/v1/events?limit=1").await;
+        let next = v["next"].as_str().expect("a next page").to_owned();
+        let (_, v) = json_of(addr, "GET", &format!("/v1/events?after={next}")).await;
+        assert_eq!(v["events"][0]["kind"], "action.pipeline.pause");
+        let (code, v) = json_of(addr, "GET", "/v1/events?until=soon").await;
+        assert_eq!(code, 400, "{v}");
+    }
+}
```

`crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 53e5b01..1541553 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -18,6 +18,8 @@ mod server;
 mod snapshots;
 #[cfg(test)]
 mod test_support;
+mod actions;
+mod events;
 
 use std::path::PathBuf;
 use std::sync::atomic::{AtomicBool, Ordering};
```

`crates/data-avalanche-api/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index b338c71..007c8b0 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -17,13 +17,15 @@ use base64::engine::general_purpose::URL_SAFE_NO_PAD;
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Parts, Phase,
-    ReconcilerSettings, Udp, WorkerFactory, removed_pipelines,
+    ReconcilerSettings, Recorder, Udp, WorkerFactory, removed_pipelines,
 };
 use data_avalanche_config::{
     ApiAuth, Env, FingerprintKey, HostPort, Jwt, Pipeline, ProcessEnv, ResolvedConfig, load_str,
 };
 use data_avalanche_pipeline::{Backoff, Tuning};
-use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases, PipelineDocs, Removals};
+use data_avalanche_state::{
+    ConfigGenerations, DocStore, Events, Holder, Leases, PipelineDocs, Removals,
+};
 use data_avalanche_storage::ObjectStoreVersioned;
 use data_avalanche_telemetry::Metrics;
 use jsonwebtoken::{Algorithm, EncodingKey, Header};
@@ -145,6 +147,8 @@ impl WorkerFactory for NoWorkers {
 /// loopback port, its reloads reaching a reconciler without pipelines.
 pub(crate) struct Lone {
     pub node: Node,
+    /// The node's timeline: what the API recorded.
+    pub events: Arc<Events>,
     /// The leases the node reads: a test takes them as another node would.
     pub leases: Leases,
     /// What the API asked of the node.
@@ -206,6 +210,8 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
         inbox,
     );
     let metrics = Metrics::new();
+    let events = Arc::new(Events::new(docs.clone()));
+    let (recorder, _) = Recorder::start(Arc::clone(&events), "node-a", metrics.clone());
     let c = &config.globals.cluster;
     let removals = Removals::new(docs.clone());
     let deps = ControllerDeps {
@@ -269,7 +275,11 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             leases: leases.clone(),
             control,
             validates: Arc::new(crate::RateLimit::validate()),
+            recorder,
+            denials: Arc::new(crate::RateLimit::denials()),
+            events: Arc::clone(&events),
         },
+        events,
         leases,
         stops,
         _reconciler: reconciler,
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-api`

Expected: FAIL. The first errors, as measured:

```text
error[E0422]: cannot find struct, variant or union type `Actor` in this scope
   --> crates/data-avalanche-api/src/actions.rs:85:18
error[E0425]: cannot find value `MAX_NOTE` in this scope
   --> crates/data-avalanche-api/src/actions.rs:105:62
error[E0425]: cannot find value `DENIALS_BURST` in this scope
   --> crates/data-avalanche-api/src/actions.rs:194:21
error[E0425]: cannot find type `DateTime` in this scope
   --> crates/data-avalanche-api/src/events.rs:9:30
error[E0425]: cannot find type `Utc` in this scope
   --> crates/data-avalanche-api/src/events.rs:9:39
error[E0425]: cannot find value `Utc` in this scope
   --> crates/data-avalanche-api/src/events.rs:10:9
…
```

- [ ] **Step 3: Implement**

`Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index eb446b0..b77c42d 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1809,6 +1809,7 @@ dependencies = [
  "tokio",
  "tokio-rustls",
  "tracing",
+ "url",
  "utoipa",
  "uuid",
 ]
```

`crates/data-avalanche-api/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-api/Cargo.toml b/crates/data-avalanche-api/Cargo.toml
index 72e50c7..1d7f040 100644
--- a/crates/data-avalanche-api/Cargo.toml
+++ b/crates/data-avalanche-api/Cargo.toml
@@ -30,6 +30,7 @@ thiserror.workspace = true
 tokio = { workspace = true, features = ["net", "sync", "fs"] }
 tokio-rustls.workspace = true
 tracing.workspace = true
+url.workspace = true
 utoipa.workspace = true
 uuid.workspace = true
 
```

`crates/data-avalanche-api/openapi.json`:

```diff
diff --git a/crates/data-avalanche-api/openapi.json b/crates/data-avalanche-api/openapi.json
index 2c71717..f59c042 100644
--- a/crates/data-avalanche-api/openapi.json
+++ b/crates/data-avalanche-api/openapi.json
@@ -12,6 +12,13 @@
           "admin"
         ],
         "summary": "Shut down: leave the cluster, then end the process",
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "202": {
             "description": "the shutdown is under way",
@@ -37,6 +44,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -61,6 +74,13 @@
           "cluster"
         ],
         "summary": "Leave the cluster: hand every pipeline off; the process stays up, not ready",
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "202": {
             "description": "the leave is under way",
@@ -86,6 +106,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -153,6 +179,13 @@
           "config"
         ],
         "summary": "Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start",
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "the file is the generation the cluster runs",
@@ -318,6 +351,138 @@
         "x-min-role": "operator"
       }
     },
+    "/v1/events": {
+      "get": {
+        "tags": [
+          "events"
+        ],
+        "summary": "The cluster's events and audit trail, newest first: actions with who took them, lease hand-offs, halts, schema changes, snapshot jobs, members, config generations",
+        "parameters": [
+          {
+            "name": "since",
+            "in": "query",
+            "description": "events at or after this time (RFC 3339)",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "until",
+            "in": "query",
+            "description": "events before this time (RFC 3339)",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "kind",
+            "in": "query",
+            "description": "a kind or a family (`action`, `lease.takeover`); repeat it for several",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "object",
+            "in": "query",
+            "description": "a pipeline's id, or an object's name (a table, a job, a node)",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "actor",
+            "in": "query",
+            "description": "the name of who caused it",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "node",
+            "in": "query",
+            "description": "the node that recorded it",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "q",
+            "in": "query",
+            "description": "text in the event or its note, without case",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "limit",
+            "in": "query",
+            "description": "at most this many (1–1000, 100 by default)",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "after",
+            "in": "query",
+            "description": "the `next` of the page before: go on from there",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the events, and `next` when there are more",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "a parameter that isn't one, or isn't in its form",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
     "/v1/openapi.json": {
       "get": {
         "tags": [
@@ -481,6 +646,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "paused: flushed already",
@@ -518,6 +690,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -553,6 +731,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "paused already",
@@ -590,6 +775,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -625,6 +816,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "202": {
             "description": "asked: the owner acts at its next poll",
@@ -656,6 +854,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -691,6 +895,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "running already",
@@ -722,6 +933,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -816,6 +1033,7 @@
           }
         ],
         "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
           "content": {
             "application/json": {}
           },
@@ -893,6 +1111,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "stopped already",
@@ -924,6 +1149,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
```

`crates/data-avalanche-api/src/actions.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/actions.rs b/crates/data-avalanche-api/src/actions.rs
index 158595e..eb20428 100644
--- a/crates/data-avalanche-api/src/actions.rs
+++ b/crates/data-avalanche-api/src/actions.rs
@@ -1,3 +1,208 @@
+//! Attribution (Admin_Console §5–§6): every route that changes something records what it did, to
+//! what, for whom, with the caller's optional `note`, and how it ended, in the cluster's events.
+//! The name and role are the server's own verification ([`Principal`]); nothing in the request
+//! can claim them. A caller below the route's role is recorded too ([`denied`]), within
+//! [`DENIALS_BURST`] at once and one a second after on a node, so a script hammering a route
+//! can't fill the timeline.
+
+use std::time::Instant;
+
+use axum::body::{Body, to_bytes};
+use axum::extract::{RawPathParams, Request, State};
+use axum::http::StatusCode;
+use axum::middleware::Next;
+use axum::response::{IntoResponse, Response};
+use data_avalanche_cluster::Recorder;
+use data_avalanche_state::{Actor, EventSeverity};
+use serde_json::Value;
+
+use crate::{Failure, Gate, Principal};
+
+/// The longest note, in characters.
+pub const MAX_NOTE: usize = 500;
+
+/// The largest request or answer body the recording reads.
+const MAX_BODY: usize = 1 << 20;
+
+/// Refused attempts a node records at once, then one a second; past that they are only logged.
+pub const DENIALS_BURST: u32 = 30;
+
+/// What a mutating route records: its event kind under `action.`, and the verb a page shows
+/// (`pause` → "pause orders").
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub struct Recorded {
+    pub kind: &'static str,
+    pub verb: &'static str,
+}
+
+/// The `note` in a JSON body: absent, or a string of at most [`MAX_NOTE`] characters without
+/// control characters but newlines; blank is none. An empty body has none.
+///
+/// # Errors
+/// What is wrong with it, for a 422.
+pub fn note_of(body: &[u8]) -> Result<Option<String>, String> {
+    if body.iter().all(u8::is_ascii_whitespace) {
+        return Ok(None);
+    }
+    let v: Value = serde_json::from_slice(body).map_err(|e| format!("the body isn't JSON: {e}"))?;
+    let Some(obj) = v.as_object() else {
+        return Err("the body is a JSON object".into());
+    };
+    match obj.get("note") {
+        None | Some(Value::Null) => Ok(None),
+        Some(Value::String(n)) => {
+            let n = n.trim();
+            if n.chars().count() > MAX_NOTE {
+                Err(format!("the note is longer than {MAX_NOTE} characters"))
+            } else if n.chars().any(|c| c.is_control() && c != '\n') {
+                Err("the note holds a control character".into())
+            } else if n.is_empty() {
+                Ok(None)
+            } else {
+                Ok(Some(n.to_owned()))
+            }
+        }
+        Some(_) => Err("the note is a string".into()),
+    }
+}
+
+/// How an answer ended, for the record; `None` for one that changed nothing worth recording
+/// (an unknown pipeline, a malformed request, a node still starting).
+fn outcome(code: StatusCode, answer: &Value) -> Option<(&'static str, EventSeverity)> {
+    match code.as_u16() {
+        200..=299 if answer["unchanged"] == true => Some(("unchanged", EventSeverity::Info)),
+        200..=299 => Some(("accepted", EventSeverity::Info)),
+        409 | 422 => Some(("refused", EventSeverity::Warning)),
+        _ => None,
+    }
+}
+
+fn target(
+    recorder: &Recorder,
+    kind: &str,
+    params: &[(String, String)],
+) -> (Option<String>, String) {
+    let pipeline = params
+        .iter()
+        .find(|(k, _)| k == "id")
+        .map(|(_, v)| v.clone());
+    let named = match (&pipeline, kind.split('.').next()) {
+        (Some(p), _) => p.clone(),
+        (None, Some("node")) => recorder.node().to_owned(),
+        _ => String::new(),
+    };
+    (pipeline, named)
+}
+
+/// The recording around a mutating route: reads the note (422 on a bad one), runs the route,
+/// records how it ended.
+pub(crate) async fn record(State(gate): State<Gate>, req: Request, next: Next) -> Response {
+    let (Some(recorded), Some(node), Some(by)) = (
+        gate.route.records,
+        gate.node.get(),
+        req.extensions().get::<Principal>().cloned(),
+    ) else {
+        return next.run(req).await;
+    };
+    let (mut parts, req_body) = req.into_parts();
+    let Ok(bytes) = to_bytes(req_body, MAX_BODY).await else {
+        return Failure::new(StatusCode::PAYLOAD_TOO_LARGE, "the body is too large").into();
+    };
+    let note = match note_of(&bytes) {
+        Ok(note) => note,
+        Err(why) => return Failure::new(StatusCode::UNPROCESSABLE_ENTITY, why).into(),
+    };
+    let params: Vec<(String, String)> = {
+        use axum::extract::FromRequestParts;
+        RawPathParams::from_request_parts(&mut parts, &())
+            .await
+            .map(|p| {
+                p.iter()
+                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
+                    .collect()
+            })
+            .unwrap_or_default()
+    };
+    let answer = next
+        .run(Request::from_parts(parts, Body::from(bytes)))
+        .await;
+    let (head, answer_body) = answer.into_parts();
+    let Ok(answer_bytes) = to_bytes(answer_body, MAX_BODY).await else {
+        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
+    };
+    let v: Value = serde_json::from_slice(&answer_bytes).unwrap_or(Value::Null);
+    if let Some((outcome, severity)) = outcome(head.status, &v) {
+        let recorder = &node.recorder;
+        let (pipeline, named) = target(recorder, recorded.kind, &params);
+        let mut event = recorder.event(
+            &format!("action.{}", recorded.kind),
+            severity,
+            format!("{} {named}", recorded.verb).trim_end().to_owned(),
+        );
+        event.pipeline = pipeline;
+        event.object = v["job_id"]
+            .as_str()
+            .map(str::to_owned)
+            .or_else(|| v["generation"].as_u64().map(|g| g.to_string()))
+            .or_else(|| (recorded.kind.starts_with("node.")).then(|| recorder.node().to_owned()));
+        event.actor = Some(Actor {
+            name: by.name.clone(),
+            role: by.role.to_string(),
+        });
+        event.note = note;
+        event.outcome = Some(outcome.to_owned());
+        if let Some(e) = v["error"].as_str() {
+            event.detail.insert("error".into(), e.to_owned());
+        }
+        recorder.record(event);
+    }
+    Response::from_parts(head, Body::from(answer_bytes))
+}
+
+/// Records a caller turned away from a mutating route for its role (a 403), within the node's
+/// rate.
+pub(crate) fn denied(gate: &Gate, by: &Principal, path: &str) {
+    let (Some(recorded), Some(node)) = (gate.route.records, gate.node.get()) else {
+        return;
+    };
+    if node.denials.take(Instant::now()).is_err() {
+        tracing::info!(by = %by.name, path, "a refused attempt not recorded: past the node's rate");
+        return;
+    }
+    let recorder = &node.recorder;
+    let mut event = recorder.event(
+        "action.denied",
+        EventSeverity::Warning,
+        format!(
+            "{} refused: needs {}",
+            format!("{} {}", recorded.verb, path_target(path, recorder)).trim_end(),
+            gate.route.role
+        ),
+    );
+    event.pipeline = path
+        .strip_prefix("/v1/pipelines/")
+        .and_then(|rest| rest.split('/').next())
+        .map(str::to_owned);
+    event.object = Some(path.to_owned());
+    event.actor = Some(Actor {
+        name: by.name.clone(),
+        role: by.role.to_string(),
+    });
+    event.outcome = Some("denied".into());
+    event.detail.insert("action".into(), recorded.kind.into());
+    recorder.record(event);
+}
+
+fn path_target(path: &str, recorder: &Recorder) -> String {
+    if let Some(rest) = path.strip_prefix("/v1/pipelines/") {
+        rest.split('/').next().unwrap_or_default().to_owned()
+    } else if path.starts_with("/v1/cluster/") || path.starts_with("/v1/admin/") {
+        recorder.node().to_owned()
+    } else {
+        String::new()
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::net::SocketAddr;
```

`crates/data-avalanche-api/src/events.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/events.rs b/crates/data-avalanche-api/src/events.rs
index 2394f79..e33426f 100644
--- a/crates/data-avalanche-api/src/events.rs
+++ b/crates/data-avalanche-api/src/events.rs
@@ -1,3 +1,150 @@
+//! `GET /v1/events` (Admin_Console §6): the cluster's timeline, newest first, under filters; a
+//! page that stopped early names where to go on (`next`, passed back as `after`).
+
+use axum::extract::{RawQuery, State};
+use axum::http::StatusCode;
+use axum::response::Response;
+use chrono::{DateTime, Utc};
+use data_avalanche_state::{EventDoc, EventQuery, MAX_EVENTS};
+use serde_json::{Value, json};
+
+use crate::pipelines::unavailable;
+use crate::{Api, Failure, body, starting};
+
+/// The query's parameters, as a page and `curl` write them.
+pub const PARAMETERS: &[(&str, &str)] = &[
+    ("since", "events at or after this time (RFC 3339)"),
+    ("until", "events before this time (RFC 3339)"),
+    (
+        "kind",
+        "a kind or a family (`action`, `lease.takeover`); repeat it for several",
+    ),
+    (
+        "object",
+        "a pipeline's id, or an object's name (a table, a job, a node)",
+    ),
+    ("actor", "the name of who caused it"),
+    ("node", "the node that recorded it"),
+    ("q", "text in the event or its note, without case"),
+    ("limit", "at most this many (1–1000, 100 by default)"),
+    ("after", "the `next` of the page before: go on from there"),
+];
+
+/// The default page.
+const DEFAULT_LIMIT: usize = 100;
+
+fn time(name: &str, value: &str) -> Result<DateTime<Utc>, Failure> {
+    DateTime::parse_from_rfc3339(value)
+        .map(|t| t.with_timezone(&Utc))
+        .map_err(|e| {
+            Failure::new(
+                StatusCode::BAD_REQUEST,
+                format!("{name} is an RFC 3339 time (2026-11-14T13:55:00Z): {e}"),
+            )
+        })
+}
+
+/// The query of `params`, each once but `kind`.
+fn query(params: &[(String, String)]) -> Result<EventQuery, Failure> {
+    let mut q = EventQuery {
+        limit: DEFAULT_LIMIT,
+        ..EventQuery::default()
+    };
+    let mut seen = Vec::new();
+    for (name, value) in params {
+        if name != "kind" {
+            if seen.contains(name) {
+                return Err(Failure::new(
+                    StatusCode::BAD_REQUEST,
+                    format!("{name} is given twice"),
+                ));
+            }
+            seen.push(name.clone());
+        }
+        let value = value.trim();
+        match name.as_str() {
+            "since" => q.since = Some(time(name, value)?),
+            "until" => q.until = Some(time(name, value)?),
+            "kind" => q.kinds.extend(
+                value
+                    .split(',')
+                    .map(str::trim)
+                    .filter(|k| !k.is_empty())
+                    .map(str::to_owned),
+            ),
+            "object" => q.object = Some(value.to_owned()),
+            "actor" => q.actor = Some(value.to_owned()),
+            "node" => q.node = Some(value.to_owned()),
+            "q" => q.text = Some(value.to_owned()),
+            "limit" => {
+                q.limit = value
+                    .parse()
+                    .ok()
+                    .filter(|n| (1..=MAX_EVENTS).contains(n))
+                    .ok_or_else(|| {
+                        Failure::new(
+                            StatusCode::BAD_REQUEST,
+                            format!("limit is 1 to {MAX_EVENTS}"),
+                        )
+                    })?;
+            }
+            "after" => q.after = Some(value.to_owned()),
+            other => {
+                return Err(Failure::new(
+                    StatusCode::BAD_REQUEST,
+                    format!(
+                        "unknown parameter {other:?}: the events take {}",
+                        PARAMETERS
+                            .iter()
+                            .map(|(n, _)| *n)
+                            .collect::<Vec<_>>()
+                            .join(", ")
+                    ),
+                ));
+            }
+        }
+    }
+    if let (Some(s), Some(u)) = (q.since, q.until)
+        && s >= u
+    {
+        return Err(Failure::new(
+            StatusCode::BAD_REQUEST,
+            "since is before until",
+        ));
+    }
+    Ok(q)
+}
+
+pub(crate) fn event_json(key: &str, e: &EventDoc) -> Value {
+    let mut v = serde_json::to_value(e).expect("an event serializes");
+    v["key"] = json!(key);
+    v
+}
+
+pub(crate) async fn list(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let params: Vec<(String, String)> =
+        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
+            .into_owned()
+            .collect();
+    let q = match query(&params) {
+        Ok(q) => q,
+        Err(f) => return f.into(),
+    };
+    match node.events.list(&q).await {
+        Ok(page) => body(
+            StatusCode::OK,
+            &json!({
+                "events": page.events.iter().map(|(k, e)| event_json(k, e)).collect::<Vec<_>>(),
+                "next": page.next,
+            }),
+        ),
+        Err(e) => unavailable(&e),
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use chrono::TimeZone;
```

`crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 1541553..2456c87 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -8,9 +8,11 @@
 //! that don't verify, are 401 with a `WWW-Authenticate` challenge; a caller below the route's
 //! role is 403.
 
+mod actions;
 mod auth;
 mod cluster;
 mod config;
+mod events;
 mod limit;
 mod openapi;
 mod pipelines;
@@ -18,8 +20,6 @@ mod server;
 mod snapshots;
 #[cfg(test)]
 mod test_support;
-mod actions;
-mod events;
 
 use std::path::PathBuf;
 use std::sync::atomic::{AtomicBool, Ordering};
@@ -31,13 +31,14 @@ use axum::http::{HeaderValue, StatusCode, header};
 use axum::middleware::{Next, from_fn_with_state};
 use axum::response::{IntoResponse, Response};
 use axum::routing::{MethodRouter, get, post};
-use data_avalanche_cluster::{ConfigCoordinator, DocsFn, Members};
+use data_avalanche_cluster::{ConfigCoordinator, DocsFn, Members, Recorder};
 use data_avalanche_config::Role;
-use data_avalanche_state::Leases;
+use data_avalanche_state::{Events, Leases};
 use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
 use serde_json::{Value, json};
 use tokio::sync::watch;
 
+pub use actions::{DENIALS_BURST, MAX_NOTE, Recorded, note_of};
 pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER};
 pub use cluster::{Control, Stop};
 pub use limit::{RateLimit, VALIDATE_BURST, VALIDATE_PER_SECOND};
@@ -72,6 +73,12 @@ pub struct Node {
     pub control: Control,
     /// The posted-body validates' limit ([`RateLimit::validate`], DA-API-20).
     pub validates: Arc<RateLimit>,
+    /// Records the actions taken through the API, and the refused attempts (Admin_Console §6).
+    pub recorder: Recorder,
+    /// The cluster's timeline, read by `GET /v1/events`.
+    pub events: Arc<Events>,
+    /// The refused attempts recorded: [`RateLimit::denials`].
+    pub denials: Arc<RateLimit>,
 }
 
 impl std::fmt::Debug for Node {
@@ -96,6 +103,16 @@ pub struct Route {
     pub body: Option<&'static str>,
     /// Its own answers; 401, 403 and 503 are every route's.
     pub answers: &'static [(u16, &'static str)],
+    /// What it records, when it changes something (Admin_Console §5).
+    pub records: Option<Recorded>,
+}
+
+/// What a route's gate and recording know: the auth, the route, and the node once it runs.
+#[derive(Debug, Clone)]
+pub(crate) struct Gate {
+    auth: Arc<Auth>,
+    route: Route,
+    node: Arc<OnceLock<Node>>,
 }
 
 const ASKED: (u16, &str) = (202, "asked: the owner acts at its next poll");
@@ -112,6 +129,13 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
         summary,
         body: None,
         answers,
+        records: None,
+    };
+    // A mutating route: an optional JSON body with a `note`, and what it records.
+    let act = |kind, verb, route: Route| Route {
+        body: Some(route.body.unwrap_or("application/json")),
+        records: Some(Recorded { kind, verb }),
+        ..route
     };
     vec![
         (
@@ -138,24 +162,28 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             post(config::validate),
         ),
         (
-            r(
-                "POST",
-                "/v1/config/reload",
-                Operator,
-                "Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start",
-                &[
-                    (200, "the file is the generation the cluster runs"),
-                    (
-                        202,
-                        "published: the generation, and this node's fields pending a restart",
-                    ),
-                    (400, "a query other than restart=accept"),
-                    (409, "other nodes published first, every time"),
-                    (
-                        422,
-                        "invalid, or it changes a restart-only field without restart=accept, or a field the cluster keeps for good: the report",
-                    ),
-                ],
+            act(
+                "config.reload",
+                "reload the config",
+                r(
+                    "POST",
+                    "/v1/config/reload",
+                    Operator,
+                    "Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start",
+                    &[
+                        (200, "the file is the generation the cluster runs"),
+                        (
+                            202,
+                            "published: the generation, and this node's fields pending a restart",
+                        ),
+                        (400, "a query other than restart=accept"),
+                        (409, "other nodes published first, every time"),
+                        (
+                            422,
+                            "invalid, or it changes a restart-only field without restart=accept, or a field the cluster keeps for good: the report",
+                        ),
+                    ],
+                ),
             ),
             post(config::reload),
         ),
@@ -200,102 +228,126 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             get(pipelines::get),
         ),
         (
-            r(
-                "POST",
-                "/v1/pipelines/{id}/pause",
-                Operator,
-                "Pause: flush to a transaction boundary, then read nothing; the connection and the lease stay",
-                &[
-                    (200, "paused already"),
-                    (202, "the desired state is paused"),
-                    UNKNOWN,
-                    (409, "the pipeline is stopped"),
-                ],
+            act(
+                "pipeline.pause",
+                "pause",
+                r(
+                    "POST",
+                    "/v1/pipelines/{id}/pause",
+                    Operator,
+                    "Pause: flush to a transaction boundary, then read nothing; the connection and the lease stay",
+                    &[
+                        (200, "paused already"),
+                        (202, "the desired state is paused"),
+                        UNKNOWN,
+                        (409, "the pipeline is stopped"),
+                    ],
+                ),
             ),
             post(pipelines::pause),
         ),
         (
-            r(
-                "POST",
-                "/v1/pipelines/{id}/stop",
-                Operator,
-                "Stop: flush, and close the source connection; the lease stays",
-                &[
-                    (200, "stopped already"),
-                    (202, "the desired state is stopped"),
-                    UNKNOWN,
-                ],
+            act(
+                "pipeline.stop",
+                "stop",
+                r(
+                    "POST",
+                    "/v1/pipelines/{id}/stop",
+                    Operator,
+                    "Stop: flush, and close the source connection; the lease stays",
+                    &[
+                        (200, "stopped already"),
+                        (202, "the desired state is stopped"),
+                        UNKNOWN,
+                    ],
+                ),
             ),
             post(pipelines::stop),
         ),
         (
-            r(
-                "POST",
-                "/v1/pipelines/{id}/resume",
-                Operator,
-                "Resume a paused or stopped pipeline",
-                &[
-                    (200, "running already"),
-                    (202, "the desired state is running"),
-                    UNKNOWN,
-                ],
+            act(
+                "pipeline.resume",
+                "resume",
+                r(
+                    "POST",
+                    "/v1/pipelines/{id}/resume",
+                    Operator,
+                    "Resume a paused or stopped pipeline",
+                    &[
+                        (200, "running already"),
+                        (202, "the desired state is running"),
+                        UNKNOWN,
+                    ],
+                ),
             ),
             post(pipelines::resume),
         ),
         (
-            r(
-                "POST",
-                "/v1/pipelines/{id}/restart",
-                Operator,
-                "Restart: discard what is unflushed and replay from the checkpoint, under the same lease; a pipeline in ERROR runs again",
-                &[
-                    ASKED,
-                    UNKNOWN,
-                    (409, "paused, stopped, or no node holds the lease"),
-                ],
+            act(
+                "pipeline.restart",
+                "restart",
+                r(
+                    "POST",
+                    "/v1/pipelines/{id}/restart",
+                    Operator,
+                    "Restart: discard what is unflushed and replay from the checkpoint, under the same lease; a pipeline in ERROR runs again",
+                    &[
+                        ASKED,
+                        UNKNOWN,
+                        (409, "paused, stopped, or no node holds the lease"),
+                    ],
+                ),
             ),
             post(pipelines::restart),
         ),
         (
-            r(
-                "POST",
-                "/v1/pipelines/{id}/flush",
-                Operator,
-                "Flush now",
-                &[
-                    (200, "paused: flushed already"),
-                    ASKED,
-                    UNKNOWN,
-                    (409, "stopped, or no node holds the lease"),
-                ],
-            ),
-            post(pipelines::flush),
-        ),
-        (
-            Route {
-                body: Some("application/json"),
-                ..r(
+            act(
+                "pipeline.flush",
+                "flush",
+                r(
                     "POST",
-                    "/v1/pipelines/{id}/snapshots",
+                    "/v1/pipelines/{id}/flush",
                     Operator,
-                    "Snapshot tables on demand, `{\"tables\": [...]}`, each named exactly; one outside the config is copied, not streamed",
+                    "Flush now",
                     &[
-                        (
-                            202,
-                            "the job is queued: the owner restarts the worker, which takes its tables",
-                        ),
+                        (200, "paused: flushed already"),
+                        ASKED,
                         UNKNOWN,
-                        (
-                            409,
-                            "paused, stopped, no node holds the lease, or an unfinished job holds a table",
-                        ),
-                        (
-                            422,
-                            "the body names no table, a glob, or a table not in the source's form",
-                        ),
+                        (409, "stopped, or no node holds the lease"),
                     ],
-                )
-            },
+                ),
+            ),
+            post(pipelines::flush),
+        ),
+        (
+            act(
+                "snapshot.start",
+                "start a snapshot job on",
+                Route {
+                    body: Some("application/json"),
+                    ..r(
+                        "POST",
+                        "/v1/pipelines/{id}/snapshots",
+                        Operator,
+                        "Snapshot tables on demand, `{\"tables\": [...]}`, each named exactly; one outside the config is copied, not streamed",
+                        &[
+                            (
+                                202,
+                                "the job is queued: the owner restarts the worker, which takes its tables",
+                            ),
+                            UNKNOWN,
+                            (
+                                409,
+                                "paused, stopped, no node holds the lease, or an unfinished job holds a table",
+                            ),
+                            (
+                                422,
+                                "the body names no table, a glob, or a table not in the source's form",
+                            ),
+                        ],
+                    )
+                },
+            ),
             post(snapshots::ask),
         ),
         (
@@ -319,27 +371,48 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             get(snapshots::get),
         ),
         (
-            r(
-                "POST",
-                "/v1/cluster/leave",
-                Admin,
-                "Leave the cluster: hand every pipeline off; the process stays up, not ready",
-                &[(202, "the leave is under way"), (409, "leaving already")],
+            act(
+                "node.leave",
+                "leave the cluster:",
+                r(
+                    "POST",
+                    "/v1/cluster/leave",
+                    Admin,
+                    "Leave the cluster: hand every pipeline off; the process stays up, not ready",
+                    &[(202, "the leave is under way"), (409, "leaving already")],
+                ),
             ),
             post(cluster::leave),
         ),
+        (
+            act(
+                "node.shutdown",
+                "shut down",
+                r(
+                    "POST",
+                    "/v1/admin/shutdown",
+                    Admin,
+                    "Shut down: leave the cluster, then end the process",
+                    &[
+                        (202, "the shutdown is under way"),
+                        (409, "shutting down already"),
+                    ],
+                ),
+            ),
+            post(cluster::shutdown),
+        ),
         (
             r(
-                "POST",
-                "/v1/admin/shutdown",
-                Admin,
-                "Shut down: leave the cluster, then end the process",
+                "GET",
+                "/v1/events",
+                Viewer,
+                "The cluster's events and audit trail, newest first: actions with who took them, lease hand-offs, halts, schema changes, snapshot jobs, members, config generations",
                 &[
-                    (202, "the shutdown is under way"),
-                    (409, "shutting down already"),
+                    (200, "the events, and `next` when there are more"),
+                    (400, "a parameter that isn't one, or isn't in its form"),
                 ],
             ),
-            post(cluster::shutdown),
+            get(events::list),
         ),
         (
             r(
@@ -365,7 +438,17 @@ pub fn router(api: Api) -> Router {
         .route("/health/ready", get(ready))
         .route("/metrics", get(metrics));
     for (r, handler) in v1() {
-        let gate = (Arc::clone(&api.auth), r.role);
+        let gate = Gate {
+            auth: Arc::clone(&api.auth),
+            route: r,
+            node: Arc::clone(&api.node),
+        };
+        // The gate runs first (the outer layer), then the recording.
+        let handler = if r.records.is_some() {
+            handler.route_layer(from_fn_with_state(gate.clone(), actions::record))
+        } else {
+            handler
+        };
         router = router.route(
             r.path,
             handler.route_layer(from_fn_with_state(gate, authorize)),
@@ -376,20 +459,20 @@ pub fn router(api: Api) -> Router {
 
 /// Lets a caller through to a route that takes `role` or less, with its [`Principal`] in the
 /// request's extensions.
-async fn authorize(
-    State((auth, role)): State<(Arc<Auth>, Role)>,
-    mut req: Request,
-    next: Next,
-) -> Response {
-    match auth.verify(req.headers()).await {
+async fn authorize(State(gate): State<Gate>, mut req: Request, next: Next) -> Response {
+    let role = gate.route.role;
+    match gate.auth.verify(req.headers()).await {
         Ok(p) if p.role >= role => {
             req.extensions_mut().insert(p);
             next.run(req).await
         }
-        Ok(p) => body(
-            StatusCode::FORBIDDEN,
-            &json!({ "error": format!("this needs the {role} role; {} is {}", p.name, p.role) }),
-        ),
+        Ok(p) => {
+            actions::denied(&gate, &p, req.uri().path());
+            body(
+                StatusCode::FORBIDDEN,
+                &json!({ "error": format!("this needs the {role} role; {} is {}", p.name, p.role) }),
+            )
+        }
         Err(Denied::Forbidden(why)) => body(StatusCode::FORBIDDEN, &json!({ "error": why })),
         Err(Denied::Unauthenticated { challenge, why }) => {
             let mut r = body(StatusCode::UNAUTHORIZED, &json!({ "error": why }));
@@ -488,6 +571,7 @@ mod tests {
                 ("GET", "/v1/snapshots/{job_id}", Viewer),
                 ("POST", "/v1/cluster/leave", Admin),
                 ("POST", "/v1/admin/shutdown", Admin),
+                ("GET", "/v1/events", Viewer),
                 ("GET", "/v1/openapi.json", Viewer),
             ]
         );
```

`crates/data-avalanche-api/src/limit.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/limit.rs b/crates/data-avalanche-api/src/limit.rs
index 42fcca1..867b943 100644
--- a/crates/data-avalanche-api/src/limit.rs
+++ b/crates/data-avalanche-api/src/limit.rs
@@ -34,6 +34,11 @@ impl RateLimit {
         Self::new(VALIDATE_BURST, VALIDATE_PER_SECOND)
     }
 
+    /// The refused attempts a node records ([`crate::DENIALS_BURST`], then one a second).
+    pub fn denials() -> Self {
+        Self::new(crate::DENIALS_BURST, 1)
+    }
+
     /// Takes a token at `now`; `Err` with the wait until one is there.
     ///
     /// # Errors
```

`crates/data-avalanche-api/src/openapi.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index f3b6621..36810e6 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -53,15 +53,37 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
             );
         }
     }
+    if r.path == "/v1/events" {
+        for (name, description) in crate::events::PARAMETERS {
+            op = op.parameter(
+                ParameterBuilder::new()
+                    .name(*name)
+                    .parameter_in(ParameterIn::Query)
+                    .required(Required::False)
+                    .description(Some(*description))
+                    .schema(Some(ObjectBuilder::new().schema_type(Type::String))),
+            );
+        }
+    }
     if let Some(content_type) = r.body {
-        op = op.request_body(Some(
-            RequestBodyBuilder::new()
-                .required(Some(Required::False))
-                .content(content_type, ContentBuilder::new().build())
-                .build(),
-        ));
+        let mut body = RequestBodyBuilder::new()
+            .required(Some(Required::False))
+            .content(content_type, ContentBuilder::new().build());
+        if r.records.is_some() {
+            body = body.description(Some(format!(
+                "`note`: why, kept with the action's event (optional, at most {} characters)",
+                crate::MAX_NOTE
+            )));
+        }
+        op = op.request_body(Some(body.build()));
     }
     let mut answers = r.answers.to_vec();
+    if r.records.is_some() && !answers.iter().any(|(code, _)| *code == 422) {
+        answers.push((
+            422,
+            "the note is longer than 500 characters, or not a string",
+        ));
+    }
     answers.extend(COMMON);
     answers.sort_unstable();
     for (code, description) in answers {
```

`crates/data-avalanche-api/src/snapshots.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index 910dfd7..1d87fd9 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -55,11 +55,14 @@ struct Asked {
     /// Empty each table's sink table before its copy (DA-SNAP-15).
     #[serde(default)]
     truncate: bool,
+    /// Why, kept with the job and its events (checked by the recording, Admin_Console §5).
+    #[serde(default)]
+    note: Option<String>,
 }
 
 /// The body's tables, each once, in its order: named as the source's per-table config keys,
 /// no globs; and whether the job empties them first.
-fn tables(p: &Pipeline, text: &str) -> Result<(Vec<TableRef>, bool), String> {
+fn tables(p: &Pipeline, text: &str) -> Result<(Vec<TableRef>, bool, Option<String>), String> {
     let asked: Asked = serde_json::from_str(text).map_err(|e| {
         format!("the body is {{\"tables\": [\"<table>\", ...], \"truncate\": false}}: {e}")
     })?;
@@ -90,7 +93,12 @@ fn tables(p: &Pipeline, text: &str) -> Result<(Vec<TableRef>, bool), String> {
             out.push(table);
         }
     }
-    Ok((out, asked.truncate))
+    // The recording checked it already (a 422 before this route runs).
+    let note = asked
+        .note
+        .map(|n| n.trim().to_owned())
+        .filter(|n| !n.is_empty());
+    Ok((out, asked.truncate, note))
 }
 
 fn job_json(p: &Pipeline, j: &SnapshotJobDoc) -> Value {
@@ -139,7 +147,7 @@ pub(crate) async fn ask(
         Ok(found) => found,
         Err(f) => return f.into(),
     };
-    let (tables, truncate) = match tables(&p, &text) {
+    let (tables, truncate, note) = match tables(&p, &text) {
         Ok(asked) => asked,
         Err(why) => return Failure::new(StatusCode::UNPROCESSABLE_ENTITY, why).into(),
     };
@@ -192,6 +200,7 @@ pub(crate) async fn ask(
     }
     let mut job = SnapshotJobDoc::new(new_job_id(&p.id), tables, by.name.clone(), Utc::now());
     job.truncate = truncate;
+    job.note = note;
     if let Err(e) = docs.create_snapshot_job(&job).await {
         return unavailable(&e);
     }
```

`crates/data-avalanche-state/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index 31809ab..a56cf1e 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -114,6 +114,9 @@ pub struct SnapshotJobDoc {
     /// only when set: a v0.1.0 reader refuses unknown fields.
     #[serde(default, skip_serializing_if = "std::ops::Not::not")]
     pub truncate: bool,
+    /// Why it was asked (Admin_Console §5). Written only when set, as `truncate`.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub note: Option<String>,
 }
 
 impl Document for SnapshotJobDoc {
@@ -146,6 +149,7 @@ impl SnapshotJobDoc {
             finished_at: None,
             error: None,
             truncate: false,
+            note: None,
         }
     }
 
```

`crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index b5f9d52..fa083c1 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -11,14 +11,14 @@ use data_avalanche_api::{Api, Auth, Control, Node, RateLimit, ServeError, Stop,
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
-    ReconcilerSettings, Seeds, Udp, advertise, discover, interfaces, join as join_gossip, leave,
-    node_identity, removed_pipelines,
+    ReconcilerSettings, Recorder, Seeds, Udp, advertise, discover, expire_events, interfaces,
+    join as join_gossip, leave, node_identity, removed_pipelines,
 };
 use data_avalanche_config::{
     Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
 };
 use data_avalanche_pipeline::{Backoff, Tuning};
-use data_avalanche_state::{ConfigGenerations, Leases, Removals};
+use data_avalanche_state::{ConfigGenerations, Events, Leases, Removals};
 use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
 use tokio::sync::{Semaphore, oneshot, watch};
@@ -149,6 +149,12 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         "joined the cluster"
     );
 
+    // The cluster's events (Admin_Console §6): recorded from here, expired on every node.
+    let events = Arc::new(Events::new(joined.globals.clone()));
+    let (recorder, _writer) =
+        Recorder::start(Arc::clone(&events), &holder.node_id, metrics.clone());
+    let (retention, kept) = watch::channel(config.globals.console.events.retention);
+    let _expiry = tokio::spawn(expire_events(Arc::clone(&events), kept));
     let c = &config.globals.cluster;
     let docs = Arc::clone(&buckets);
     let docs: Arc<DocsFn> = Arc::new(move |p| docs.pipeline_docs(p).map_err(|e| e.to_string()));
@@ -236,6 +242,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
                 log.apply(&g.log);
                 log.trace(&g.tracing);
                 reloaded_auth.set(&g.api);
+                retention.send_replace(g.console.events.retention);
             }),
         },
         gossip.members(),
@@ -250,6 +257,9 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         leases: deps.leases.clone(),
         control,
         validates: Arc::new(RateLimit::validate()),
+        recorder,
+        denials: Arc::new(RateLimit::denials()),
+        events: Arc::clone(&events),
     });
     ready.store(true, Ordering::SeqCst);
     let mut cluster = Some((gossip, reconciler, discovery));
```

`docs/public/openapi.json`:

```diff
diff --git a/docs/public/openapi.json b/docs/public/openapi.json
index 2c71717..f59c042 100644
--- a/docs/public/openapi.json
+++ b/docs/public/openapi.json
@@ -12,6 +12,13 @@
           "admin"
         ],
         "summary": "Shut down: leave the cluster, then end the process",
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "202": {
             "description": "the shutdown is under way",
@@ -37,6 +44,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -61,6 +74,13 @@
           "cluster"
         ],
         "summary": "Leave the cluster: hand every pipeline off; the process stays up, not ready",
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "202": {
             "description": "the leave is under way",
@@ -86,6 +106,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -153,6 +179,13 @@
           "config"
         ],
         "summary": "Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start",
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "the file is the generation the cluster runs",
@@ -318,6 +351,138 @@
         "x-min-role": "operator"
       }
     },
+    "/v1/events": {
+      "get": {
+        "tags": [
+          "events"
+        ],
+        "summary": "The cluster's events and audit trail, newest first: actions with who took them, lease hand-offs, halts, schema changes, snapshot jobs, members, config generations",
+        "parameters": [
+          {
+            "name": "since",
+            "in": "query",
+            "description": "events at or after this time (RFC 3339)",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "until",
+            "in": "query",
+            "description": "events before this time (RFC 3339)",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "kind",
+            "in": "query",
+            "description": "a kind or a family (`action`, `lease.takeover`); repeat it for several",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "object",
+            "in": "query",
+            "description": "a pipeline's id, or an object's name (a table, a job, a node)",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "actor",
+            "in": "query",
+            "description": "the name of who caused it",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "node",
+            "in": "query",
+            "description": "the node that recorded it",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "q",
+            "in": "query",
+            "description": "text in the event or its note, without case",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "limit",
+            "in": "query",
+            "description": "at most this many (1–1000, 100 by default)",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          },
+          {
+            "name": "after",
+            "in": "query",
+            "description": "the `next` of the page before: go on from there",
+            "required": false,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the events, and `next` when there are more",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "a parameter that isn't one, or isn't in its form",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "401": {
+            "description": "no credentials, or ones that don't verify",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "403": {
+            "description": "the caller's role is below the route's",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "503": {
+            "description": "the node is starting or stopping, or its state can't be read",
+            "content": {
+              "application/json": {}
+            }
+          }
+        },
+        "security": [
+          {
+            "basic": []
+          },
+          {
+            "bearer": []
+          }
+        ],
+        "x-min-role": "viewer"
+      }
+    },
     "/v1/openapi.json": {
       "get": {
         "tags": [
@@ -481,6 +646,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "paused: flushed already",
@@ -518,6 +690,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -553,6 +731,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "paused already",
@@ -590,6 +775,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -625,6 +816,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "202": {
             "description": "asked: the owner acts at its next poll",
@@ -656,6 +854,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -691,6 +895,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "running already",
@@ -722,6 +933,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
@@ -816,6 +1033,7 @@
           }
         ],
         "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
           "content": {
             "application/json": {}
           },
@@ -893,6 +1111,13 @@
             }
           }
         ],
+        "requestBody": {
+          "description": "`note`: why, kept with the action's event (optional, at most 500 characters)",
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
         "responses": {
           "200": {
             "description": "stopped already",
@@ -924,6 +1149,12 @@
               "application/json": {}
             }
           },
+          "422": {
+            "description": "the note is longer than 500 characters, or not a string",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
```

`docs/src/content/docs/reference/api.md`:

```diff
diff --git a/docs/src/content/docs/reference/api.md b/docs/src/content/docs/reference/api.md
index 2f20ad8..d8f5e9f 100644
--- a/docs/src/content/docs/reference/api.md
+++ b/docs/src/content/docs/reference/api.md
@@ -14,6 +14,8 @@ Every node serves the API on `globals.api.address`. `globals.api.auth` decides w
 
 **Answers.** Every body is a JSON object; a refusal carries an `error` string. Every route can also answer `503` (the node is starting or stopping, or its state can't be read) and, unless `auth` is `none`, `401` (no credentials, or ones that don't verify) and `403` (the caller's role is below the route's). A `202` means accepted, not done: a pipeline command or a snapshot job is carried out by the node that holds the pipeline's lease, a reload is published, a leave or a shutdown is under way on the node called.
 
+**Who did it.** Every route that changes something takes an optional JSON body `{"note": "…"}` and records the action in the cluster's events: what, to what, the caller's name and role as the node verified them, the note, and how it ended (`accepted`, `unchanged`, `refused`). A caller turned away for its role is recorded too (`action.denied`). A note longer than 500 characters, or not a string, is `422`, and nothing is done.
+
 **Outside `/v1`**, with no credentials: `GET /health/live` (the process answers), `GET /health/ready` (`200` once the node runs its config, `503` before and while it stops) and `GET /metrics` (Prometheus text).
 
 ## pipelines
@@ -51,6 +53,8 @@ Flush now
 Least role: `operator`.
 
 - Path parameter `id`: the pipeline's id.
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
 
 | Status | When |
 |---|---|
@@ -58,6 +62,7 @@ Least role: `operator`.
 | `202` | asked: the owner acts at its next poll |
 | `404` | no such pipeline in the generation this node runs |
 | `409` | stopped, or no node holds the lease |
+| `422` | the note is longer than 500 characters, or not a string |
 
 ### `POST /v1/pipelines/{id}/pause`
 
@@ -66,6 +71,8 @@ Pause: flush to a transaction boundary, then read nothing; the connection and th
 Least role: `operator`.
 
 - Path parameter `id`: the pipeline's id.
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
 
 | Status | When |
 |---|---|
@@ -73,6 +80,7 @@ Least role: `operator`.
 | `202` | the desired state is paused |
 | `404` | no such pipeline in the generation this node runs |
 | `409` | the pipeline is stopped |
+| `422` | the note is longer than 500 characters, or not a string |
 
 ### `POST /v1/pipelines/{id}/restart`
 
@@ -81,12 +89,15 @@ Restart: discard what is unflushed and replay from the checkpoint, under the sam
 Least role: `operator`.
 
 - Path parameter `id`: the pipeline's id.
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
 
 | Status | When |
 |---|---|
 | `202` | asked: the owner acts at its next poll |
 | `404` | no such pipeline in the generation this node runs |
 | `409` | paused, stopped, or no node holds the lease |
+| `422` | the note is longer than 500 characters, or not a string |
 
 ### `POST /v1/pipelines/{id}/resume`
 
@@ -95,12 +106,15 @@ Resume a paused or stopped pipeline
 Least role: `operator`.
 
 - Path parameter `id`: the pipeline's id.
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
 
 | Status | When |
 |---|---|
 | `200` | running already |
 | `202` | the desired state is running |
 | `404` | no such pipeline in the generation this node runs |
+| `422` | the note is longer than 500 characters, or not a string |
 
 ### `GET /v1/pipelines/{id}/snapshots`
 
@@ -123,6 +137,7 @@ Least role: `operator`.
 
 - Path parameter `id`: the pipeline's id.
 - Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
 
 | Status | When |
 |---|---|
@@ -138,12 +153,15 @@ Stop: flush, and close the source connection; the lease stays
 Least role: `operator`.
 
 - Path parameter `id`: the pipeline's id.
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
 
 | Status | When |
 |---|---|
 | `200` | stopped already |
 | `202` | the desired state is stopped |
 | `404` | no such pipeline in the generation this node runs |
+| `422` | the note is longer than 500 characters, or not a string |
 
 ## snapshots
 
@@ -172,6 +190,8 @@ Publish the node's file as the next config generation; `?restart=accept` publish
 
 Least role: `operator`.
 
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
 
 | Status | When |
 |---|---|
@@ -216,11 +236,14 @@ Leave the cluster: hand every pipeline off; the process stays up, not ready
 
 Least role: `admin`.
 
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
 
 | Status | When |
 |---|---|
 | `202` | the leave is under way |
 | `409` | leaving already |
+| `422` | the note is longer than 500 characters, or not a string |
 
 ### `GET /v1/cluster/status`
 
@@ -243,11 +266,39 @@ Shut down: leave the cluster, then end the process
 
 Least role: `admin`.
 
+- Body: `application/json`.
+- `note`: why, kept with the action's event (optional, at most 500 characters).
 
 | Status | When |
 |---|---|
 | `202` | the shutdown is under way |
 | `409` | shutting down already |
+| `422` | the note is longer than 500 characters, or not a string |
+
+## events
+
+The cluster's timeline and audit trail, kept for `globals.console.events.retention`.
+
+### `GET /v1/events`
+
+The cluster's events and audit trail, newest first: actions with who took them, lease hand-offs, halts, schema changes, snapshot jobs, members, config generations
+
+Least role: `viewer`.
+
+- Query parameter `since`: events at or after this time (RFC 3339).
+- Query parameter `until`: events before this time (RFC 3339).
+- Query parameter `kind`: a kind or a family (`action`, `lease.takeover`); repeat it for several.
+- Query parameter `object`: a pipeline's id, or an object's name (a table, a job, a node).
+- Query parameter `actor`: the name of who caused it.
+- Query parameter `node`: the node that recorded it.
+- Query parameter `q`: text in the event or its note, without case.
+- Query parameter `limit`: at most this many (1–1000, 100 by default).
+- Query parameter `after`: the `next` of the page before: go on from there.
+
+| Status | When |
+|---|---|
+| `200` | the events, and `next` when there are more |
+| `400` | a parameter that isn't one, or isn't in its form |
 
 ## openapi.json
 
```

`xtask/src/api.rs`:

```diff
diff --git a/xtask/src/api.rs b/xtask/src/api.rs
index ca10f06..cc8f146 100644
--- a/xtask/src/api.rs
+++ b/xtask/src/api.rs
@@ -20,6 +20,10 @@ const TAGS: &[(&str, &str)] = &[
     ),
     ("cluster", "Membership and leases."),
     ("admin", "The node's process."),
+    (
+        "events",
+        "The cluster's timeline and audit trail, kept for `globals.console.events.retention`.",
+    ),
     ("openapi.json", "The document this page is rendered from."),
 ];
 
@@ -76,6 +80,12 @@ pub(crate) fn page() -> String {
          a pipeline command or a snapshot job is carried out by the node that holds the \
          pipeline's lease, a reload is published, a leave or a shutdown is under way on the \
          node called.\n\n\
+         **Who did it.** Every route that changes something takes an optional JSON body \
+         `{{\"note\": \"…\"}}` and records the action in the cluster's events: what, to what, \
+         the caller's name and role as the node verified them, the note, and how it ended \
+         (`accepted`, `unchanged`, `refused`). A caller turned away for its role is recorded \
+         too (`action.denied`). A note longer than 500 characters, or not a string, is `422`, \
+         and nothing is done.\n\n\
          **Outside `/v1`**, with no credentials: `GET /health/live` (the process answers), \
          `GET /health/ready` (`200` once the node runs its config, `503` before and while it \
          stops) and `GET /metrics` (Prometheus text).\n\n",
@@ -94,7 +104,8 @@ pub(crate) fn page() -> String {
             for p in op["parameters"].as_array().into_iter().flatten() {
                 let _ = writeln!(
                     out,
-                    "- Path parameter `{}`: {}.",
+                    "- {} parameter `{}`: {}.",
+                    if p["in"] == "query" { "Query" } else { "Path" },
                     p["name"].as_str().expect("a name"),
                     p["description"].as_str().unwrap_or_default(),
                 );
@@ -102,6 +113,9 @@ pub(crate) fn page() -> String {
             if let Some(body) = op["requestBody"]["content"].as_object() {
                 let types: Vec<String> = body.keys().map(|t| format!("`{t}`")).collect();
                 let _ = writeln!(out, "- Body: {}.", types.join(", "));
+                if let Some(d) = op["requestBody"]["description"].as_str() {
+                    let _ = writeln!(out, "- {d}.");
+                }
             }
             out.push_str("\n| Status | When |\n|---|---|\n");
             for (code, answer) in sorted(&op["responses"]) {
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-api`

Expected: PASS, 66 tests.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings && cargo test --workspace`

Expected: no output from the first three; every test binary passes.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock crates/data-avalanche-api crates/data-avalanche-state/src/snapshot.rs crates/data-avalanche/src/run.rs xtask/src/api.rs docs/src/content/docs/reference/api.md docs/public/openapi.json
git commit -m "feat(api): every mutating call recorded with who, why and how it ended, refused attempts too; GET /v1/events"
```

---

### Task 5: The cluster's own events (`data-avalanche-cluster`, `-pipeline`, `-core`)

**Files:**
- Modify: `crates/data-avalanche-core/src/classify.rs` (`Display for SchemaChange`)
- Modify: `crates/data-avalanche-pipeline/src/{observe,schema_change,session}.rs`; tests: `test_support.rs`, `worker.rs`
- Modify: `crates/data-avalanche-cluster/src/{events,claim,controller,jobs,coordinator,lib}.rs`; tests: `test_support.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`, `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: Task 3's `Recorder`; `StatusDoc`, `ActualState`, `HaltsDoc`, `SnapshotJobDoc`;
  `Members`, `Member`, `NodeMeta`, the gossip `Phase`; the worker's `Phase` and `halted`.
- Produces:
  - `Observer::schema_evolved(&self, table: &TableRef, changes: &[SchemaChange], warning:
    Option<&str>)` (default nothing), called for an applied or degraded evolve; `Evolver::observer`;
  - `Claimed::Mine { …, taken_from: Option<String> }`: the node a failover took the lease from;
  - `ControllerDeps::recorder`, `CoordinatorDeps::recorder: Recorder`;
  - `Recorder::of_pipeline(&self, id, kind, severity, text) -> EventDoc`;
  - `Jobs::poll(…) -> Vec<SnapshotJobDoc>`: the jobs that ended at this poll;
  - crate-private: `Found`, `StateWatch::changes`, `HaltWatch::{recorded, changes}`,
    `member_changes`, `records`; public `record_members(recorder, members)`, spawned by the binary.
- The kinds: `lease.{acquired,takeover,released,lost}`, `pipeline.{error,retrying,running,paused,
  stopped,dumped,degraded,recovered,removed}`, `table.{halted,resumed}`,
  `schema.{applied,degraded}`, `job.{completed,failed}`,
  `member.{joined,restarted,draining,left,suspect,alive,down}`, `config.{published,refused}`.

D-M13a.6, D-M13a.7. The controller's and coordinator's tests that check events record to a
timeline of their own (`recorded_node`, the coordinator's `Lone::events`), so the writes don't spend
the faults a `ScriptedStore` injects for the other tests.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 007c8b0..185f7cf 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -238,6 +238,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
         },
         starts: Arc::new(Semaphore::new(1)),
         handoffs: Arc::new(Semaphore::new(1)),
+        recorder: recorder.clone(),
     };
     let reconciler =
         ClusterReconciler::start(vec![], &deps, gossip.members(), ReconcilerSettings::new(c));
@@ -256,6 +257,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             advertiser: gossip.advertiser(),
             metrics,
             globals: Arc::new(|_| {}),
+            recorder: recorder.clone(),
         },
         gossip.members(),
         Duration::from_secs(10),
```

`crates/data-avalanche-cluster/src/claim.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/claim.rs b/crates/data-avalanche-cluster/src/claim.rs
index 8291778..a183ead 100644
--- a/crates/data-avalanche-cluster/src/claim.rs
+++ b/crates/data-avalanche-cluster/src/claim.rs
@@ -237,6 +237,13 @@ mod tests {
         }
     }
 
+    fn taken_from(c: &Claimed) -> Option<&str> {
+        match c {
+            Claimed::Mine { taken_from, .. } => taken_from.as_deref(),
+            Claimed::HeldBy { .. } => panic!("{c:?}"),
+        }
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_free_lease_is_acquired_at_once() {
         let store = ScriptedStore::new();
@@ -248,7 +255,9 @@ mod tests {
         let store = ScriptedStore::new();
         let lease = acquired(&leases(&store), "a").await;
         release(&leases(&store), &lease).await.unwrap();
-        assert_eq!(epoch(&claim(&store, "b").step().await.unwrap()), 2);
+        let mine = claim(&store, "b").step().await.unwrap();
+        assert_eq!(epoch(&mine), 2);
+        assert_eq!(taken_from(&mine), None, "given back, not taken");
     }
 
     #[tokio::test(start_paused = true)]
@@ -289,6 +298,11 @@ mod tests {
         advance(10).await;
         let mine = b.step().await.unwrap();
         assert_eq!(epoch(&mine), 2);
+        assert_eq!(
+            taken_from(&mine),
+            Some("a"),
+            "a failover names whom it took over from"
+        );
         let stored = leases(&store).observe("p").await.unwrap().unwrap();
         assert!(stored.doc.is_held_by(&holder("b")));
     }
```

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index a1d3ca3..58e9020 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1658,9 +1658,9 @@ mod tests {
 
     use super::*;
     use crate::test_support::{
-        Fault, Kind, ScriptedStore, TestFactory, acquired, deps, eventually, holder, leases,
-        metrics, node, pipeline, pipeline_docs, pipeline_docs_on, removals, row, timing, tombstone,
-        until,
+        Fault, Kind, ScriptedStore, TestFactory, acquired, deps, events_until, eventually, holder,
+        leases, metrics, node, pipeline, pipeline_docs, pipeline_docs_on, recorded_node, removals,
+        row, timing, tombstone, until,
     };
 
     /// A real two-second watchdog over `wait` on the paused clock: a controller that spins keeps
@@ -2332,6 +2332,55 @@ mod tests {
         assert_eq!(f.builds(), 2, "the job's restart");
     }
 
+    /// The owner records its lease, its tables' halts and its jobs' ends, each about its object
+    /// (Admin_Console §6); then its error and its recovery.
+    #[tokio::test(start_paused = true)]
+    async fn the_owner_records_its_lease_halts_jobs_and_errors() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a", "b"]);
+        let (mut n, events) = recorded_node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        f.source.fail(
+            Method::ReadChunk,
+            SourceError::TableHalt {
+                table: t("a"),
+                reason: data_avalanche_core::HaltReason::TableDropped,
+            },
+        );
+        ask_job(&docs, "p~1", &["a"]).await;
+        docs.request(Action::Restart, asked("p~1")).await.unwrap();
+        let has =
+            |seen: &[(String, Option<String>)], kind: &str| seen.iter().any(|(k, _)| k == kind);
+        events_until(&events, "the halt", |s| has(s, "table.halted")).await;
+        // The halt is recorded with the next flush: the job then fails.
+        f.source.commit(FakeTxn::new().insert(&t("b"), row(1)));
+        let seen = events_until(&events, "the job's end", |s| has(s, "job.failed")).await;
+        assert_eq!(seen[0], ("lease.acquired".to_owned(), None));
+        assert!(
+            seen.contains(&("table.halted".to_owned(), Some("public.a".to_owned()))),
+            "{seen:?}"
+        );
+        assert!(
+            seen.contains(&("job.failed".to_owned(), Some("p~1".to_owned()))),
+            "{seen:?}"
+        );
+
+        f.source.fail(
+            Method::Preflight,
+            SourceError::Permanent("no privilege".into()),
+        );
+        docs.request(Action::Restart, asked("r2")).await.unwrap();
+        events_until(&events, "the error", |s| has(s, "pipeline.error")).await;
+        docs.request(Action::Restart, asked("r3")).await.unwrap();
+        let seen = events_until(&events, "the recovery", |s| has(s, "pipeline.running")).await;
+        let halted = seen.iter().filter(|(k, _)| k == "table.halted").count();
+        assert_eq!(
+            halted, 1,
+            "a halt carried into the next worker isn't news: {seen:?}"
+        );
+    }
+
     /// A job's table that halts while its snapshot runs fails, with the halt's code, once the
     /// halt is recorded (with the next flush, here a change of another table's), and the job
     /// ends failed.
```

`crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 2c19b9f..6f3924a 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -834,10 +834,38 @@ mod tests {
         metrics: Metrics,
         members: watch::Sender<Members>,
         globals: Arc<StdMutex<Vec<Globals>>>,
+        /// The timeline, in a store of its own.
+        events: Arc<data_avalanche_state::Events>,
     }
 
     const POLL: Duration = Duration::from_secs(2);
 
+    impl Lone {
+        /// The events' kinds and objects once `n` are written (on the test clock).
+        async fn recorded(&self, n: usize) -> Vec<(String, Option<String>, String)> {
+            for _ in 0..100 {
+                let page = self
+                    .events
+                    .list(&data_avalanche_state::EventQuery {
+                        limit: 1000,
+                        ..Default::default()
+                    })
+                    .await
+                    .unwrap();
+                if page.events.len() >= n {
+                    return page
+                        .events
+                        .into_iter()
+                        .rev()
+                        .map(|(_, e)| (e.kind, e.object, e.text))
+                        .collect();
+                }
+                tokio::time::sleep(Duration::from_millis(100)).await;
+            }
+            panic!("fewer than {n} events");
+        }
+    }
+
     /// [`start`] on `config_text(ids)`, an empty environment and `globals.state` in memory.
     async fn lone(ids: &[&str]) -> Lone {
         start(&config_text(ids), Vars::default(), Racing::new()).await
@@ -878,6 +906,15 @@ mod tests {
         let metrics = Metrics::new();
         let globals = Arc::new(StdMutex::new(Vec::new()));
         let seen = Arc::clone(&globals);
+        let events = Arc::new(data_avalanche_state::Events::new(
+            data_avalanche_state::DocStore::new(Arc::new(
+                data_avalanche_storage::ObjectStoreVersioned::new(
+                    Arc::new(object_store::memory::InMemory::new()),
+                    "state",
+                )
+                .unwrap(),
+            )),
+        ));
         let deps = CoordinatorDeps {
             generations: generations.clone(),
             removals: removals.clone(),
@@ -890,6 +927,7 @@ mod tests {
             advertiser: gossip.advertiser(),
             metrics: metrics.clone(),
             globals: Arc::new(move |g| seen.lock().unwrap().push(g.clone())),
+            recorder: Recorder::start(Arc::clone(&events), "n1", Metrics::new()).0,
         };
         let (members_tx, members_rx) =
             watch::channel(members("n1", &[("n1", Phase::Active, false)]));
@@ -904,6 +942,7 @@ mod tests {
             metrics,
             members: members_tx,
             globals,
+            events,
         }
     }
 
@@ -1163,6 +1202,17 @@ mod tests {
             Some(LogFilter("debug".into()))
         );
         assert_eq!(n.coordinator.applied(), 2);
+        let [(kind, object, text)] = &n.recorded(1).await[..] else {
+            panic!("one event")
+        };
+        assert_eq!(
+            (kind.as_str(), object.as_deref()),
+            ("config.published", Some("2"))
+        );
+        assert!(
+            text.starts_with("config generation 2 published by n1: "),
+            "{text}"
+        );
     }
 
     /// The file as the cluster runs it already: nothing is published.
@@ -1307,6 +1357,19 @@ mod tests {
         assert_eq!(n.publish_elsewhere(&unset).await, 2);
         tokio::time::sleep(POLL * 3).await;
         assert_eq!(n.coordinator.applied(), 1);
+        let refused = n.recorded(1).await;
+        assert_eq!(refused.len(), 1, "once, though polled again: {refused:?}");
+        assert_eq!(
+            (refused[0].0.as_str(), refused[0].1.as_deref()),
+            ("config.refused", Some("2"))
+        );
+        assert!(
+            refused[0]
+                .2
+                .starts_with("n1 can't apply config generation 2, and runs 1: "),
+            "{}",
+            refused[0].2
+        );
         assert!(n.reloads.try_recv().is_err(), "nothing applied");
         assert_eq!(n.gossiped(), 1);
         assert_eq!(n.publish_elsewhere(&config_text(&["p", "q"])).await, 3);
```

`crates/data-avalanche-cluster/src/events.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
index 7a52726..a75bd43 100644
--- a/crates/data-avalanche-cluster/src/events.rs
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -225,6 +225,202 @@ mod tests {
         );
     }
 
+    fn status(actual: ActualState, error: Option<&str>, degraded: Option<&str>) -> StatusDoc {
+        StatusDoc {
+            holder_node_id: "node-a".into(),
+            owner_epoch: 1,
+            actual,
+            error: error.map(str::to_owned),
+            code: None,
+            degraded: degraded.map(str::to_owned),
+            written_at: Utc::now(),
+        }
+    }
+
+    fn kinds(found: &[Found]) -> Vec<&'static str> {
+        found.iter().map(|f| f.kind).collect()
+    }
+
+    #[test]
+    fn the_state_watch_records_failures_and_recoveries_not_reruns() {
+        use ActualState::{Error, Paused, Retrying, Running, Starting};
+        let mut w = StateWatch::default();
+        assert!(
+            w.changes("orders", &status(Paused, None, None)).is_empty(),
+            "a carried-over pause"
+        );
+        assert_eq!(
+            kinds(&w.changes("orders", &status(Running, None, None))),
+            ["pipeline.running"]
+        );
+        let retry = w.changes("orders", &status(Retrying, Some("refused"), None));
+        assert_eq!(retry[0].text, "orders failed and is retried: refused");
+        assert!(
+            w.changes("orders", &status(Starting, None, None))
+                .is_empty()
+        );
+        assert!(
+            w.changes("orders", &status(Retrying, Some("refused"), None))
+                .is_empty(),
+            "the rerun failed alike"
+        );
+        assert_eq!(
+            kinds(&w.changes("orders", &status(Error, Some("gone"), None))),
+            ["pipeline.error"]
+        );
+        assert_eq!(
+            kinds(&w.changes("orders", &status(Running, None, Some("503")))),
+            ["pipeline.running", "pipeline.degraded"]
+        );
+        assert_eq!(
+            kinds(&w.changes("orders", &status(Running, None, None))),
+            ["pipeline.recovered"]
+        );
+        let mut fresh = StateWatch::default();
+        assert_eq!(
+            kinds(&fresh.changes("orders", &status(Error, Some("gone"), None))),
+            ["pipeline.error"]
+        );
+    }
+
+    fn halted(tables: &[&str]) -> BTreeMap<TableRef, HaltReason> {
+        tables
+            .iter()
+            .map(|t| (TableRef::new("public", *t), HaltReason::TableDropped))
+            .collect()
+    }
+
+    #[test]
+    fn the_halt_watch_records_new_and_cleared_halts_against_the_recorded() {
+        let recorded = HaltsDoc {
+            tables: vec![data_avalanche_state::TableHalt {
+                table: TableRef::new("public", "a"),
+                reason: HaltReason::TableDropped,
+                at: data_avalanche_core::SourcePosition::Pg(data_avalanche_core::position::Lsn(1)),
+                copy: false,
+            }],
+        };
+        let mut w = HaltWatch::recorded(&recorded);
+        assert!(
+            w.changes(&Phase::Starting, &halted(&[])).is_empty(),
+            "a starting worker shows none yet"
+        );
+        assert!(
+            w.changes(&Phase::Streaming, &halted(&["a"])).is_empty(),
+            "recorded already"
+        );
+        let new = w.changes(&Phase::Streaming, &halted(&["a", "b"]));
+        assert_eq!(kinds(&new), ["table.halted"]);
+        assert_eq!(new[0].object.as_deref(), Some("public.b"));
+        assert_eq!(
+            new[0].text,
+            format!(
+                "public.b halted: TABLE_DROPPED: {}",
+                HaltReason::TableDropped
+            )
+        );
+        let cleared = w.changes(&Phase::Streaming, &halted(&["b"]));
+        assert_eq!(kinds(&cleared), ["table.resumed"]);
+        assert_eq!(cleared[0].object.as_deref(), Some("public.a"));
+
+        let mut unread = HaltWatch::default();
+        assert!(
+            unread
+                .changes(&Phase::Streaming, &halted(&["a"]))
+                .is_empty(),
+            "taken as recorded"
+        );
+        assert_eq!(
+            kinds(&unread.changes(&Phase::Streaming, &halted(&["a", "c"]))),
+            ["table.halted"]
+        );
+    }
+
+    fn members(me: &str, nodes: &[(&str, u64, bool, NodePhase)]) -> Members {
+        Members {
+            me: me.into(),
+            nodes: nodes
+                .iter()
+                .map(|(n, inc, suspect, phase)| {
+                    (
+                        (*n).to_owned(),
+                        crate::gossip::Member {
+                            addr: "127.0.0.1:7946".parse().unwrap(),
+                            incarnation: *inc,
+                            meta: crate::gossip::NodeMeta {
+                                phase: *phase,
+                                config_generation: 1,
+                                build: "1.1.0".into(),
+                            },
+                            suspect: *suspect,
+                        },
+                    )
+                })
+                .collect(),
+        }
+    }
+
+    #[test]
+    fn one_member_records_each_membership_change() {
+        use NodePhase::{Active, Joining, Leaving};
+        let before = members(
+            "a",
+            &[
+                ("a", 1, false, Active),
+                ("b", 1, false, Active),
+                ("c", 1, false, Leaving),
+            ],
+        );
+        let after = members(
+            "a",
+            &[
+                ("a", 1, false, Active),
+                ("b", 1, true, Active),
+                ("d", 1, false, Joining),
+            ],
+        );
+        let changes = member_changes(&before, &after);
+        let seen: Vec<_> = changes.iter().map(|(n, c)| (n.as_str(), c.kind)).collect();
+        assert_eq!(
+            seen,
+            [
+                ("b", "member.suspect"),
+                ("d", "member.joined"),
+                ("c", "member.left")
+            ]
+        );
+        assert_eq!(changes[1].1.text, "d joined (1.1.0)");
+        for (subject, _) in &changes {
+            assert!(
+                records(&before, &after, subject),
+                "a is the first active other than {subject}"
+            );
+        }
+        let at_b = |m: &Members| Members {
+            me: "b".into(),
+            ..m.clone()
+        };
+        assert!(
+            !records(&at_b(&before), &at_b(&after), "d"),
+            "b leaves it to a"
+        );
+        assert!(
+            !records(&before, &after, "a"),
+            "a's own change is b's or no one's"
+        );
+        let joining = members("d", &[("a", 1, false, Active), ("d", 1, false, Joining)]);
+        assert!(
+            !records(&joining, &joining, "a"),
+            "a joining node records nothing"
+        );
+        let down = member_changes(
+            &after,
+            &members("a", &[("a", 1, false, Active), ("d", 2, false, Active)]),
+        );
+        let seen: Vec<_> = down.iter().map(|(n, c)| (n.as_str(), c.kind)).collect();
+        assert_eq!(seen, [("d", "member.restarted"), ("b", "member.down")]);
+    }
+
     #[tokio::test]
     async fn a_full_queue_drops_without_waiting() {
         let metrics = Metrics::new();
```

`crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index 809bb14..ad6e2d2 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -500,6 +500,7 @@ pub(crate) fn deps(
         handoffs: Arc::new(tokio::sync::Semaphore::new(2)),
         removals: removals(store),
         removed: removed(),
+        recorder: crate::Recorder::off(name),
     }
 }
 
@@ -509,7 +510,57 @@ pub(crate) fn node(
     docs: &data_avalanche_state::PipelineDocs,
     factory: &TestFactory,
 ) -> Node {
-    let deps = deps(name, store, factory);
+    node_of(deps(name, store, factory), docs)
+}
+
+/// [`node`], its events recorded to a timeline of their own (not on `store`, whose faults the
+/// tests count).
+pub(crate) fn recorded_node(
+    name: &str,
+    store: &Arc<ScriptedStore>,
+    docs: &data_avalanche_state::PipelineDocs,
+    factory: &TestFactory,
+) -> (Node, Arc<data_avalanche_state::Events>) {
+    let events = Arc::new(data_avalanche_state::Events::new(DocStore::new(Arc::new(
+        ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state").unwrap(),
+    ))));
+    let mut deps = deps(name, store, factory);
+    deps.recorder = crate::Recorder::start(Arc::clone(&events), name, Metrics::new()).0;
+    (node_of(deps, docs), events)
+}
+
+/// The kinds and objects recorded so far, oldest first, once `cond` holds of them (on the test
+/// clock).
+pub(crate) async fn events_until(
+    events: &data_avalanche_state::Events,
+    what: &str,
+    cond: impl Fn(&[(String, Option<String>)]) -> bool,
+) -> Vec<(String, Option<String>)> {
+    let mut last = Vec::new();
+    for _ in 0..1200 {
+        let page = events
+            .list(&data_avalanche_state::EventQuery {
+                limit: 1000,
+                ..Default::default()
+            })
+            .await
+            .unwrap();
+        let mut seen: Vec<_> = page
+            .events
+            .into_iter()
+            .map(|(_, e)| (e.kind, e.object))
+            .collect();
+        seen.reverse();
+        if cond(&seen) {
+            return seen;
+        }
+        last = seen;
+        tokio::time::sleep(Duration::from_millis(100)).await;
+    }
+    panic!("no {what} recorded: {last:?}");
+}
+
+fn node_of(deps: crate::ControllerDeps, docs: &data_avalanche_state::PipelineDocs) -> Node {
     let (controller, status) = crate::PipelineController::new(pipeline(), docs.clone(), &deps);
     let (target, rx) = tokio::sync::watch::channel(crate::Target::Own);
     Node {
@@ -894,6 +945,7 @@ impl TestCluster {
                 advertiser: gossip.advertiser(),
                 metrics: deps.metrics.clone(),
                 globals: Arc::new(|_| {}),
+                recorder: crate::Recorder::off(name),
             },
             gossip.members(),
             timing().renewal_interval,
```

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 3042d7d..a2ca9fa 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -2750,6 +2750,15 @@ mod tests {
         let [apply] = &spans.named("schema.apply")[..] else {
             panic!("one apply")
         };
+        assert_eq!(
+            w.ears.evolved(),
+            [(
+                "public.a".to_owned(),
+                vec!["add column note (text)".to_owned()],
+                None
+            )],
+            "the owner hears the evolve"
+        );
         assert_eq!(apply.parent, None, "a trace of its own");
         for (field, value) in [
             ("pipeline", "p"),
```

`crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
index 07c5444..00b31e8 100644
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -11,8 +11,8 @@ use bytes::Bytes;
 use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    CdcEvent, ColumnSchema, ColumnValue, ColumnValueType as V, Op, Row, Sink, SourcePosition,
-    SourceSeq, TableRef, TableSchema, TableSpec, TransactionBoundary,
+    CdcEvent, ColumnSchema, ColumnValue, ColumnValueType as V, Op, Row, SchemaChange, Sink,
+    SourcePosition, SourceSeq, TableRef, TableSchema, TableSpec, TransactionBoundary,
 };
 use data_avalanche_state::{DocStore, PipelineDocs, SlotDoc, SnapshotStatus, TableSnapshotDoc};
 use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
@@ -25,7 +25,7 @@ use crate::settings::Settings;
 use crate::status::Streak;
 use crate::{
     Accumulator, Backoff, Command, CommitGuard, FlushPolicy, HookAction, HookPoint, Hooks, Lineage,
-    NoHooks, NoObserver, Observer, RowsRead, Tuning, Unguarded, WorkerStatus,
+    NoHooks, Observer, RowsRead, Tuning, Unguarded, WorkerStatus,
 };
 
 /// A span a test saw: its name, its parent's, and its fields as last recorded.
@@ -155,11 +155,19 @@ pub(crate) enum Heard {
     ChunkRead,
 }
 
+/// An evolve heard: the table, its changes in words, and a degraded one's warning.
+pub(crate) type Evolved = (String, Vec<String>, Option<String>);
+
 /// An observer that keeps what it hears, in order.
 #[derive(Debug, Default)]
-pub(crate) struct Ears(Mutex<Vec<Heard>>, Mutex<Vec<Duration>>);
+pub(crate) struct Ears(Mutex<Vec<Heard>>, Mutex<Vec<Duration>>, Mutex<Vec<Evolved>>);
 
 impl Ears {
+    /// Each evolve heard.
+    pub fn evolved(&self) -> Vec<Evolved> {
+        self.2.lock().unwrap().clone()
+    }
+
     pub fn heard(&self) -> Vec<Heard> {
         self.0.lock().unwrap().clone()
     }
@@ -187,6 +195,14 @@ impl Observer for Ears {
     fn chunk_read(&self, _: Duration) {
         self.0.lock().unwrap().push(Heard::ChunkRead);
     }
+
+    fn schema_evolved(&self, table: &TableRef, changes: &[SchemaChange], warning: Option<&str>) {
+        self.2.lock().unwrap().push((
+            table.to_string(),
+            changes.iter().map(ToString::to_string).collect(),
+            warning.map(str::to_owned),
+        ));
+    }
 }
 
 /// A PostgreSQL → Delta pipeline `p` reading `public.*`, with more `tables` keys (YAML lines
@@ -519,6 +535,8 @@ pub(crate) struct World {
     pub source: FakeSource,
     pub sink: MemorySink,
     pub docs: PipelineDocs,
+    /// The observer of every core [`Self::core`] makes.
+    pub ears: Arc<Ears>,
 }
 
 impl World {
@@ -533,6 +551,7 @@ impl World {
             source: FakeSource::new(tables),
             sink: MemorySink::new(),
             docs,
+            ears: Arc::default(),
         }
     }
 
@@ -593,7 +612,7 @@ impl World {
             owner_epoch: 1,
             guard: Arc::new(Unguarded),
             hooks: Arc::new(NoHooks),
-            observer: Arc::new(NoObserver),
+            observer: self.ears.clone(),
             tuning: fast(),
             control: control_rx,
             status,
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index a73afdc..90f6463 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -2438,6 +2438,7 @@ mod tests {
             source: w.source.clone(),
             sink: w.sink.clone(),
             docs: docs().0,
+            ears: Arc::default(),
         };
         let (end, st) = run(&wiped, &mode("mode: initial"), async |status| {
             streamed(&status, &wiped).await;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib && cargo test -p data-avalanche-pipeline --lib added_column`

Expected: FAIL. The first errors, as measured:

```text
error[E0432]: unresolved import `ActualState`
   --> crates/data-avalanche-cluster/src/events.rs:246:13
error[E0432]: unresolved import `NodePhase`
   --> crates/data-avalanche-cluster/src/events.rs:365:13
error[E0433]: cannot find type `Recorder` in this scope
   --> crates/data-avalanche-cluster/src/coordinator.rs:930:23
error[E0425]: cannot find type `ActualState` in this scope
   --> crates/data-avalanche-cluster/src/events.rs:228:23
error[E0425]: cannot find type `StatusDoc` in this scope
   --> crates/data-avalanche-cluster/src/events.rs:228:84
error[E0422]: cannot find struct, variant or union type `StatusDoc` in this scope
   --> crates/data-avalanche-cluster/src/events.rs:229:9
…
```

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/claim.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/claim.rs b/crates/data-avalanche-cluster/src/claim.rs
index a183ead..879369c 100644
--- a/crates/data-avalanche-cluster/src/claim.rs
+++ b/crates/data-avalanche-cluster/src/claim.rs
@@ -17,10 +17,14 @@ use crate::LeaseTiming;
 pub enum Claimed {
     /// This node holds the lease; `written` is when its write was sent (the renewer's first
     /// `last_renew_local`). `jumped`: the floor ([`Claim::at_least`]) set its epoch.
+    /// `taken_from`: the node whose live lease this one took over, after it went a full
+    /// `lease_duration` unrenewed (a failover); `None` for a free or given-back lease, or this
+    /// process's own earlier term.
     Mine {
         lease: Versioned<LeaseDoc>,
         written: Instant,
         jumped: bool,
+        taken_from: Option<String>,
     },
     /// Another holder has it. Unless it renews, a step at or after `takeover_at` takes it over.
     HeldBy {
@@ -97,6 +101,7 @@ impl Claim {
                     lease,
                     written: sent,
                     jumped: false,
+                    taken_from: None,
                 });
             }
             Acquired::HeldBy(seen) => seen,
@@ -126,6 +131,8 @@ impl Claim {
                 });
             }
         }
+        let taken_from =
+            (!seen.doc.is_held_by(&self.holder)).then(|| seen.doc.holder_node_id.clone());
         // The floor holds until a takeover lands: a failed write tries it again.
         let floor = self.floor;
         let proposed = seen.doc.owner_epoch.checked_add(1).map(|e| e.max(floor));
@@ -143,6 +150,7 @@ impl Claim {
                     lease,
                     written: sent,
                     jumped,
+                    taken_from,
                 })
             }
             // The holder renewed, or another challenger won: watch again.
@@ -170,6 +178,7 @@ impl Claim {
                             lease,
                             written: sent,
                             jumped,
+                            taken_from,
                         })
                     }
                     _ => Err(e),
```

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 58e9020..ad6f36a 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -13,14 +13,16 @@ use std::sync::atomic::AtomicBool;
 use std::time::{Duration, SystemTime, UNIX_EPOCH};
 
 use data_avalanche_config::Pipeline;
-use data_avalanche_core::{Sink, SourceConnector, SourceError, SourcePosition};
+use data_avalanche_core::{
+    SchemaChange, Sink, SourceConnector, SourceError, SourcePosition, TableRef,
+};
 use data_avalanche_pipeline::{
     Backoff, Command, CommitGuard, Exit, Hooks, Observer, Phase, PipelineWorker, Recovery, Tuning,
     WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
 };
 use data_avalanche_state::{
-    ActualState, DesiredState, Holder, Leases, PipelineDocs, RemovalDoc, Removals, RequestsDoc,
-    StatusDoc,
+    ActualState, DesiredState, EventSeverity, Holder, JobStatus, Leases, PipelineDocs, RemovalDoc,
+    Removals, RequestsDoc, SnapshotJobDoc, StatusDoc,
 };
 use data_avalanche_telemetry::{
     Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, RowsRead, SnapshotState, Totals,
@@ -29,6 +31,7 @@ use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
 use tokio::task::JoinHandle;
 use tokio::time::{Instant, MissedTickBehavior};
 
+use crate::events::{Found, HaltWatch, Recorder, StateWatch};
 use crate::jobs::Jobs;
 use crate::reconciler::{same_slot, shares};
 use crate::{Claim, Claimed, LeaseRun, LeaseTiming, RenewEnd, RenewState, release};
@@ -186,6 +189,8 @@ pub struct ControllerDeps {
     pub removals: Removals,
     /// A removed pipeline, from the generation its tombstone names, as this node resolves it.
     pub removed: RemovedPipelines,
+    /// Where the owner's events go (Admin_Console §6).
+    pub recorder: Recorder,
 }
 
 /// Why a removal tombstone's pipeline didn't resolve ([`RemovedPipelines::resolve`]).
@@ -276,6 +281,9 @@ pub struct PipelineController {
     updater: mpsc::UnboundedSender<Update>,
     removals: Removals,
     removed: RemovedPipelines,
+    recorder: Recorder,
+    /// This term's halted tables, against which a worker's are compared.
+    halts: std::sync::Mutex<HaltWatch>,
 }
 
 /// Why the owned phase ended.
@@ -340,6 +348,8 @@ struct Reported {
     last: Option<StatusDoc>,
     /// The last write failed: the next failure isn't warned of again.
     failing: bool,
+    /// The state the writes showed, for the events.
+    state: StateWatch,
 }
 
 /// `status.json` for an owned pipeline's status; `None` before the first publish.
@@ -392,25 +402,55 @@ fn labels(p: &Pipeline) -> PipelineLabels {
     }
 }
 
-/// The worker's flushes, commits and chunk reads, into the pipeline's series.
+/// The worker's flushes, commits and chunk reads, into the pipeline's series; its schema
+/// changes, into the events.
 #[derive(Debug)]
-struct Observed(Arc<PipelineMetrics>);
+struct Observed {
+    metrics: Arc<PipelineMetrics>,
+    recorder: Recorder,
+    /// The pipeline's id and name.
+    pipeline: (String, String),
+}
 
 impl Observer for Observed {
     fn flushed(&self, rows: u64, took: Duration) {
-        self.0.flushed(rows, took);
+        self.metrics.flushed(rows, took);
     }
 
     fn committed(&self, rows: u64, took: Duration) {
-        self.0.committed(rows, took);
+        self.metrics.committed(rows, took);
     }
 
     fn commit_failed(&self) {
-        self.0.commit_failed();
+        self.metrics.commit_failed();
     }
 
     fn chunk_read(&self, took: Duration) {
-        self.0.chunk_read(took);
+        self.metrics.chunk_read(took);
+    }
+
+    fn schema_evolved(&self, table: &TableRef, changes: &[SchemaChange], warning: Option<&str>) {
+        let (id, name) = &self.pipeline;
+        let what = changes
+            .iter()
+            .map(ToString::to_string)
+            .collect::<Vec<_>>()
+            .join(", ");
+        let (kind, severity, text) = match warning {
+            None => (
+                "schema.applied",
+                EventSeverity::Info,
+                format!("{name}: {table} evolved: {what}"),
+            ),
+            Some(w) => (
+                "schema.degraded",
+                EventSeverity::Warning,
+                format!("{name}: {table} evolved in part: {what}; {w}"),
+            ),
+        };
+        let mut e = self.recorder.of_pipeline(id, kind, severity, text);
+        e.object = Some(table.to_string());
+        self.recorder.record(e);
     }
 }
 
@@ -586,6 +626,8 @@ impl PipelineController {
             updater,
             removals: deps.removals.clone(),
             removed: deps.removed.clone(),
+            recorder: deps.recorder.clone(),
+            halts: std::sync::Mutex::default(),
         }
     }
 
@@ -625,12 +667,14 @@ impl PipelineController {
                     lease,
                     written,
                     jumped,
+                    taken_from,
                 }) => {
                     tracing::info!(
                         pipeline = %self.pipeline.name,
                         owner_epoch = lease.doc.owner_epoch,
                         "lease acquired"
                     );
+                    self.leased(lease.doc.owner_epoch, taken_from.as_deref());
                     let run = LeaseRun::start(
                         self.leases.clone(),
                         lease,
@@ -842,6 +886,15 @@ impl PipelineController {
         let mut jobs = Jobs::default();
         jobs.list(&self.docs, &self.pipeline.name).await;
         let mut reported = Reported::default();
+        // The halts recorded as the term starts: a worker's later ones are news. Unread, the
+        // first streaming worker's are taken as them.
+        *self.halts.lock().expect("the halts") = match self.docs.halts().await {
+            Ok(h) => HaltWatch::recorded(&h),
+            Err(e) => {
+                tracing::warn!(pipeline = %self.pipeline.name, "the halts unread at the term's start: {e}");
+                HaltWatch::default()
+            }
+        };
         let mut poll = tokio::time::interval(self.settings.timing.renewal_interval);
         poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
         poll.tick().await;
@@ -951,6 +1004,7 @@ impl PipelineController {
                         RenewEnd::Stopped(_) => "the lease's deadline passed".into(),
                     };
                     tracing::error!(pipeline = %self.pipeline.name, "lease lost: {reason}");
+                    self.lost(&reason);
                     return Left::Lost;
                 }
                 // A shutdown drains at once; "don't own" waits for its hand-off permit.
@@ -1126,7 +1180,9 @@ impl PipelineController {
                             }
                         }
                     }
-                    jobs.poll(&self.docs, &self.pipeline.name).await;
+                    for job in jobs.poll(&self.docs, &self.pipeline.name).await {
+                        self.job_ended(&job);
+                    }
                 }
                 Event::Update(Update::Reconfigure(p)) => {
                     self.reconfigure(*p);
@@ -1185,6 +1241,9 @@ impl PipelineController {
         }
         match self.docs.save_status(&doc).await {
             Ok(_) => {
+                for f in reported.state.changes(&self.pipeline.name, &doc) {
+                    self.found(f);
+                }
                 reported.last = Some(doc);
                 reported.failing = false;
             }
@@ -1244,7 +1303,11 @@ impl PipelineController {
             owner_epoch: run.owner_epoch,
             guard: run.guard.clone(),
             hooks: parts.hooks,
-            observer: Arc::new(Observed(Arc::clone(&self.metrics))),
+            observer: Arc::new(Observed {
+                metrics: Arc::clone(&self.metrics),
+                recorder: self.recorder.clone(),
+                pipeline: (self.pipeline.id.clone(), self.pipeline.name.clone()),
+            }),
             reader_ended: Arc::clone(reader_ended),
         };
         let (worker, handle) =
@@ -1337,24 +1400,42 @@ impl PipelineController {
             Some(w) => self.drain(w, &mut Lag::default()).await,
             None => true,
         };
-        if !drained {
+        let name = &self.pipeline.name;
+        let (severity, text) = if !drained {
             let _ = run.stop().await;
             tracing::warn!(
-                pipeline = %self.pipeline.name,
+                pipeline = %name,
                 "removed with its drain cut off: the lease expires, then what the source created for the pipeline is torn down"
             );
+            (
+                EventSeverity::Warning,
+                format!(
+                    "{name} removed with its drain cut off: what its source holds for it is torn down once its lease expires"
+                ),
+            )
         } else if self.teardown(&*run.guard).await {
             self.removal_done().await;
             self.give_back(run).await;
+            (
+                EventSeverity::Info,
+                format!("{name} removed: drained, and what its source held for it torn down"),
+            )
         } else {
             let _ = run.stop().await;
             tracing::warn!(
-                pipeline = %self.pipeline.name,
+                pipeline = %name,
                 "removed after its lease was lost: what the source created for the pipeline stays to its holder"
             );
-        }
+            (
+                EventSeverity::Warning,
+                format!(
+                    "{name} removed after its lease was lost: what its source holds for it is torn down by the next holder"
+                ),
+            )
+        };
         self.metrics.forget();
-        tracing::info!(pipeline = %self.pipeline.name, "pipeline removed");
+        tracing::info!(pipeline = %name, "pipeline removed");
+        self.record("pipeline.removed", severity, text);
     }
 
     /// Marks the removal's tombstone done: what follows the pipeline may start. Not written, the
@@ -1524,7 +1605,14 @@ impl PipelineController {
     async fn give_back(&self, run: LeaseRun) {
         match run.stop().await {
             RenewEnd::Stopped(lease) => match release(&self.leases, &lease).await {
-                Ok(()) => tracing::info!(pipeline = %self.pipeline.name, "lease released"),
+                Ok(()) => {
+                    tracing::info!(pipeline = %self.pipeline.name, "lease released");
+                    self.record(
+                        "lease.released",
+                        EventSeverity::Info,
+                        format!("{} let go by {}", self.pipeline.name, self.holder.node_id),
+                    );
+                }
                 Err(e) => tracing::warn!(
                     pipeline = %self.pipeline.name,
                     "lease not released ({e}); it expires instead"
@@ -1532,13 +1620,96 @@ impl PipelineController {
             },
             RenewEnd::Lost { reason } => {
                 tracing::warn!(pipeline = %self.pipeline.name, "lease lost: {reason}");
+                self.lost(&reason);
             }
         }
     }
 
+    /// Records one of the pipeline's events.
+    fn record(&self, kind: &str, severity: EventSeverity, text: String) {
+        self.recorder.record(
+            self.recorder
+                .of_pipeline(&self.pipeline.id, kind, severity, text),
+        );
+    }
+
+    /// Records what a watcher found.
+    fn found(&self, f: Found) {
+        let mut e = self
+            .recorder
+            .of_pipeline(&self.pipeline.id, f.kind, f.severity, f.text);
+        e.object = f.object;
+        self.recorder.record(e);
+    }
+
+    /// The lease is this node's at `epoch`: acquired, or taken over from `from` (a failover).
+    fn leased(&self, epoch: u64, from: Option<&str>) {
+        let (name, node) = (&self.pipeline.name, &self.holder.node_id);
+        match from {
+            Some(from) => self.record(
+                "lease.takeover",
+                EventSeverity::Warning,
+                format!("{name} taken over by {node} from {from}, whose lease went unrenewed (owner epoch {epoch})"),
+            ),
+            None => self.record(
+                "lease.acquired",
+                EventSeverity::Info,
+                format!("{name} runs on {node} (owner epoch {epoch})"),
+            ),
+        }
+    }
+
+    fn lost(&self, reason: &str) {
+        self.record(
+            "lease.lost",
+            EventSeverity::Error,
+            format!(
+                "{} lost its lease on {}: {reason}",
+                self.pipeline.name, self.holder.node_id
+            ),
+        );
+    }
+
+    /// A snapshot job that ended, completed or failed.
+    fn job_ended(&self, job: &SnapshotJobDoc) {
+        let short: String = job.job_id.chars().take(8).collect();
+        let name = &self.pipeline.name;
+        let (kind, severity, text) = match job.status {
+            JobStatus::Failed => (
+                "job.failed",
+                EventSeverity::Error,
+                format!(
+                    "snapshot job {short} of {name} failed: {}",
+                    job.error.as_deref().unwrap_or("a table failed")
+                ),
+            ),
+            _ => (
+                "job.completed",
+                EventSeverity::Info,
+                format!(
+                    "snapshot job {short} of {name} completed: {} tables copied",
+                    job.tables.len()
+                ),
+            ),
+        };
+        let mut e = self
+            .recorder
+            .of_pipeline(&self.pipeline.id, kind, severity, text);
+        e.object = Some(job.job_id.clone());
+        self.recorder.record(e);
+    }
+
     /// The worker's status into the metrics, and its new warnings and reconnects into the log.
     fn observe(&self, w: &mut Running, pending: &mut Lag) {
         let st = w.handle.status.borrow().clone();
+        let halts = self
+            .halts
+            .lock()
+            .expect("the halts")
+            .changes(&st.phase, &st.halted);
+        for f in halts {
+            self.found(f);
+        }
         for warning in &st.warnings {
             if w.logged.insert(warning.clone()) {
                 tracing::warn!(pipeline = %self.pipeline.name, "{warning}");
```

`crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 6f3924a..1f1fa12 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -16,8 +16,8 @@ use data_avalanche_config::{
 };
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{
-    ConfigGeneration, ConfigGenerations, GENERATION_KEY, GenerationDoc, PipelineDocs, RemovalDoc,
-    Removals, StateError, Versioned, config_hash,
+    ConfigGeneration, ConfigGenerations, EventSeverity, GENERATION_KEY, GenerationDoc,
+    PipelineDocs, RemovalDoc, Removals, StateError, Versioned, config_hash,
 };
 use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
@@ -26,7 +26,7 @@ use tokio::task::JoinHandle;
 use tokio::time::MissedTickBehavior;
 
 use crate::{
-    Advertiser, Configured, CurrentPipeline, Members, PipelineReload, Reload, Reloader,
+    Advertiser, Configured, CurrentPipeline, Members, PipelineReload, Recorder, Reload, Reloader,
     RemovedPipelines, ResolveError, Resolved, WorkerFactory,
 };
 
@@ -59,6 +59,8 @@ pub struct CoordinatorDeps {
     /// What a generation's globals change outside the cluster: the binary's log filter and the
     /// API's auth.
     pub globals: Arc<dyn Fn(&Globals) + Send + Sync>,
+    /// Where a publish, and a generation this node can't apply, are recorded (Admin_Console §6).
+    pub recorder: Recorder,
 }
 
 impl Debug for CoordinatorDeps {
@@ -284,6 +286,17 @@ impl ConfigCoordinator {
                 Ok(published) => {
                     let generation = published.pointer.doc.generation;
                     tracing::info!(generation, "published config generation {generation}");
+                    let mut e = d.recorder.event(
+                        "config.published",
+                        EventSeverity::Info,
+                        format!(
+                            "config generation {generation} published by {}: {}",
+                            d.node_id,
+                            summary(&changes)
+                        ),
+                    );
+                    e.object = Some(generation.to_string());
+                    d.recorder.record(e);
                     self.inner.adopt(&mut running, published).await;
                     return Ok(Reloaded {
                         generation,
@@ -410,6 +423,16 @@ impl Inner {
                     "config generation {n} can't be applied on this node, which runs {}: {why}",
                     running.generation.doc.generation
                 );
+                let mut e = d.recorder.event(
+                    "config.refused",
+                    EventSeverity::Error,
+                    format!(
+                        "{} can't apply config generation {n}, and runs {}: {why}",
+                        d.node_id, running.generation.doc.generation
+                    ),
+                );
+                e.object = Some(n.to_string());
+                d.recorder.record(e);
             }
             running.failed = Some(n);
         };
```

`crates/data-avalanche-cluster/src/events.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
index a75bd43..655445c 100644
--- a/crates/data-avalanche-cluster/src/events.rs
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -4,15 +4,20 @@
 //! and logged, never allowed to slow or fail what it records. Another task expires the days past
 //! `globals.console.events.retention`.
 
+use std::collections::BTreeMap;
 use std::sync::Arc;
 use std::time::Duration;
 
 use chrono::Utc;
-use data_avalanche_state::{EventDoc, EventSeverity, Events};
+use data_avalanche_core::{HaltReason, TableRef};
+use data_avalanche_pipeline::Phase;
+use data_avalanche_state::{ActualState, EventDoc, EventSeverity, Events, HaltsDoc, StatusDoc};
 use data_avalanche_telemetry::Metrics;
 use tokio::sync::{mpsc, watch};
 use tokio::task::JoinHandle;
 
+use crate::gossip::{Members, Phase as NodePhase};
+
 /// Events waiting to be written; past this, new ones are dropped and counted.
 pub const EVENT_QUEUE: usize = 1024;
 
@@ -67,6 +72,19 @@ impl Recorder {
         EventDoc::new(Utc::now(), &self.node, kind, severity, text)
     }
 
+    /// An event of pipeline `id` (the console links it), now.
+    pub fn of_pipeline(
+        &self,
+        id: &str,
+        kind: &str,
+        severity: EventSeverity,
+        text: impl Into<String>,
+    ) -> EventDoc {
+        let mut e = self.event(kind, severity, text);
+        e.pipeline = Some(id.to_owned());
+        e
+    }
+
     /// Queues `event`; never waits. A full queue drops it, counted and logged.
     pub fn record(&self, event: EventDoc) {
         let Some(tx) = &self.tx else {
@@ -120,6 +138,272 @@ pub async fn expire_events(events: Arc<Events>, retention: watch::Receiver<Durat
     }
 }
 
+/// One event a watcher found: its kind, severity, object and text.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) struct Found {
+    pub kind: &'static str,
+    pub severity: EventSeverity,
+    pub object: Option<String>,
+    pub text: String,
+}
+
+fn found(kind: &'static str, severity: EventSeverity, text: String) -> Found {
+    Found {
+        kind,
+        severity,
+        object: None,
+        text,
+    }
+}
+
+/// The owner's pipeline state as its status writes show it, over one lease term. `Starting` is
+/// passed over, so a worker rerun after a backoff shows no change; the term's first state is
+/// news only when it is a failure (a carried-over pause isn't).
+#[derive(Debug, Default)]
+pub(crate) struct StateWatch {
+    shown: Option<ActualState>,
+    degraded: bool,
+}
+
+impl StateWatch {
+    /// What `after`, just written, changed for pipeline `name`.
+    pub fn changes(&mut self, name: &str, after: &StatusDoc) -> Vec<Found> {
+        use EventSeverity::{Error, Info, Warning};
+        let mut out = Vec::new();
+        let why = || {
+            let error = after.error.as_deref().unwrap_or("no reason given");
+            match &after.code {
+                Some(code) => format!("{code}: {error}"),
+                None => error.to_owned(),
+            }
+        };
+        let state = after.actual;
+        if state != ActualState::Starting && self.shown.as_ref() != Some(&state) {
+            let first = self.shown.is_none();
+            let back = matches!(
+                self.shown,
+                Some(
+                    ActualState::Error
+                        | ActualState::Retrying
+                        | ActualState::Paused
+                        | ActualState::Stopped
+                )
+            );
+            match state {
+                ActualState::Error => out.push(found(
+                    "pipeline.error",
+                    Error,
+                    format!("{name} is in ERROR: {}", why()),
+                )),
+                ActualState::Retrying => out.push(found(
+                    "pipeline.retrying",
+                    Warning,
+                    format!("{name} failed and is retried: {}", why()),
+                )),
+                ActualState::Paused if !first => {
+                    out.push(found("pipeline.paused", Info, format!("{name} is paused")))
+                }
+                ActualState::Stopped if !first => out.push(found(
+                    "pipeline.stopped",
+                    Info,
+                    format!("{name} is stopped"),
+                )),
+                ActualState::Dumped if !first => out.push(found(
+                    "pipeline.dumped",
+                    Info,
+                    format!("{name} copied every table: its dump is done"),
+                )),
+                ActualState::Running if back => out.push(found(
+                    "pipeline.running",
+                    Info,
+                    format!("{name} runs again"),
+                )),
+                _ => {}
+            }
+            self.shown = Some(state);
+        }
+        match (&after.degraded, self.degraded) {
+            (Some(d), false) => {
+                out.push(found(
+                    "pipeline.degraded",
+                    Warning,
+                    format!("{name}: sink commits or state writes fail, and are retried: {d}"),
+                ));
+                self.degraded = true;
+            }
+            (None, true) => {
+                out.push(found(
+                    "pipeline.recovered",
+                    Info,
+                    format!("{name}: sink commits and state writes succeed again"),
+                ));
+                self.degraded = false;
+            }
+            _ => {}
+        }
+        out
+    }
+}
+
+/// The pipeline's halted tables over one lease term, against the halts recorded when it began:
+/// a table halted since is news, and so is one a later worker streams without (its halt
+/// cleared). Every worker shows the recorded halts once it streams, and only adds to them.
+#[derive(Debug, Default)]
+pub(crate) struct HaltWatch(Option<BTreeMap<TableRef, HaltReason>>);
+
+impl HaltWatch {
+    /// The halts recorded as the term starts (a job's copy isn't the pipeline's).
+    pub fn recorded(halts: &HaltsDoc) -> Self {
+        Self(Some(
+            halts
+                .tables
+                .iter()
+                .filter(|h| !h.copy)
+                .map(|h| (h.table.clone(), h.reason.clone()))
+                .collect(),
+        ))
+    }
+
+    /// What a worker's `halted`, in `phase`, changed. Without the recorded halts (unread), the
+    /// first streaming worker's are taken as them.
+    pub fn changes(
+        &mut self,
+        phase: &Phase,
+        halted: &BTreeMap<TableRef, HaltReason>,
+    ) -> Vec<Found> {
+        let streaming = *phase == Phase::Streaming;
+        let Some(known) = self.0.as_mut() else {
+            if streaming {
+                self.0 = Some(halted.clone());
+            }
+            return Vec::new();
+        };
+        let mut out = Vec::new();
+        for (table, reason) in halted {
+            if known.get(table) != Some(reason) {
+                known.insert(table.clone(), reason.clone());
+                out.push(Found {
+                    object: Some(table.to_string()),
+                    ..found(
+                        "table.halted",
+                        EventSeverity::Error,
+                        format!("{table} halted: {}: {reason}", reason.code()),
+                    )
+                });
+            }
+        }
+        if streaming {
+            known.retain(|table, _| {
+                let kept = halted.contains_key(table);
+                if !kept {
+                    out.push(Found {
+                        object: Some(table.to_string()),
+                        ..found(
+                            "table.resumed",
+                            EventSeverity::Info,
+                            format!("{table} runs again: its halt is cleared"),
+                        )
+                    });
+                }
+                kept
+            });
+        }
+        out
+    }
+}
+
+/// What changed between two views of the members, each change with the member it is about.
+/// Recorded by one node only ([`records`]).
+pub(crate) fn member_changes(before: &Members, after: &Members) -> Vec<(String, Found)> {
+    use EventSeverity::{Info, Warning};
+    let mut out = Vec::new();
+    for (name, m) in &after.nodes {
+        let change = match before.nodes.get(name) {
+            None => Some(found(
+                "member.joined",
+                Info,
+                format!("{name} joined ({})", m.meta.build),
+            )),
+            Some(was) if was.incarnation != m.incarnation => Some(found(
+                "member.restarted",
+                Info,
+                format!("{name} restarted ({})", m.meta.build),
+            )),
+            Some(was) if !was.suspect && m.suspect => Some(found(
+                "member.suspect",
+                Warning,
+                format!("{name} is suspect: it stopped answering probes"),
+            )),
+            Some(was) if was.suspect && !m.suspect => {
+                Some(found("member.alive", Info, format!("{name} answers again")))
+            }
+            Some(was)
+                if was.meta.phase != NodePhase::Draining && m.meta.phase == NodePhase::Draining =>
+            {
+                Some(found(
+                    "member.draining",
+                    Info,
+                    format!("{name} shuts down: it hands its pipelines off"),
+                ))
+            }
+            Some(_) => None,
+        };
+        out.extend(change.map(|c| (name.clone(), c)));
+    }
+    for (name, was) in &before.nodes {
+        if !after.nodes.contains_key(name) {
+            let c = if was.meta.phase == NodePhase::Leaving {
+                found("member.left", Info, format!("{name} left the cluster"))
+            } else {
+                found(
+                    "member.down",
+                    Warning,
+                    format!("{name} is down: it stopped answering and was declared failed"),
+                )
+            };
+            out.push((name.clone(), c));
+        }
+    }
+    for (name, c) in &mut out {
+        c.object = Some(name.clone());
+    }
+    out
+}
+
+/// Whether this node records a change about `subject`: it is active in both views, and the
+/// first by name of the active, unsuspected members other than `subject`. Views converge, so
+/// one node records it; a view that hasn't may record it twice or not at all.
+pub(crate) fn records(before: &Members, after: &Members, subject: &str) -> bool {
+    let active = |m: &Members| {
+        m.nodes
+            .get(&m.me)
+            .is_some_and(|n| n.meta.phase == NodePhase::Active)
+    };
+    active(before)
+        && active(after)
+        && after
+            .nodes
+            .iter()
+            .find(|(name, m)| *name != subject && !m.suspect && m.meta.phase == NodePhase::Active)
+            .is_some_and(|(name, _)| *name == after.me)
+}
+
+/// Records the members' changes this node is the one to record, until the gossip stops.
+pub async fn record_members(recorder: Recorder, mut members: watch::Receiver<Members>) {
+    let mut before = members.borrow_and_update().clone();
+    while members.changed().await.is_ok() {
+        let after = members.borrow_and_update().clone();
+        for (subject, c) in member_changes(&before, &after) {
+            if records(&before, &after, &subject) {
+                let mut e = recorder.event(c.kind, c.severity, c.text);
+                e.object = c.object;
+                recorder.record(e);
+            }
+        }
+        before = after;
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::atomic::{AtomicUsize, Ordering};
```

`crates/data-avalanche-cluster/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/jobs.rs b/crates/data-avalanche-cluster/src/jobs.rs
index 03b62d4..e20ab18 100644
--- a/crates/data-avalanche-cluster/src/jobs.rs
+++ b/crates/data-avalanche-cluster/src/jobs.rs
@@ -81,16 +81,17 @@ impl Jobs {
     }
 
     /// One poll: each job's running tables, from their documents, into the job; a job that
-    /// ended, or is gone, is followed no more.
-    pub async fn poll(&mut self, docs: &PipelineDocs, pipeline: &str) {
+    /// ended, or is gone, is followed no more. The jobs that ended at this poll.
+    pub async fn poll(&mut self, docs: &PipelineDocs, pipeline: &str) -> Vec<SnapshotJobDoc> {
+        let mut done = Vec::new();
         if self.0.is_none() {
             self.list(docs, pipeline).await;
         }
         let Some(ids) = self.0.as_mut() else {
-            return;
+            return done;
         };
         if ids.is_empty() {
-            return;
+            return done;
         }
         // The jobs before the halts: the worker's start clears a table's halt, then records it
         // running, so a table read running here had its halt cleared before the halts are read.
@@ -112,7 +113,7 @@ impl Jobs {
             ids.remove(&id);
         }
         if jobs.is_empty() {
-            return;
+            return done;
         }
         let halts = match docs.halts().await {
             Ok(h) => h,
@@ -121,7 +122,7 @@ impl Jobs {
                     pipeline,
                     "halts unread, snapshot jobs followed at the next poll: {e}"
                 );
-                return;
+                return done;
             }
         };
         for v in jobs {
@@ -130,6 +131,7 @@ impl Jobs {
                 Ok(Some(job)) => {
                     tracing::info!(pipeline, job = %id, status = ?job.status, "snapshot job ended");
                     ended.push(id);
+                    done.push(job);
                 }
                 Ok(None) => {}
                 Err(e) => {
@@ -138,7 +140,7 @@ impl Jobs {
             }
         }
         if ended.is_empty() {
-            return;
+            return done;
         }
         for id in ended {
             ids.remove(&id);
@@ -148,6 +150,7 @@ impl Jobs {
             Ok(n) => tracing::info!(pipeline, deleted = n, "ended snapshot jobs pruned"),
             Err(e) => tracing::warn!(pipeline, "ended snapshot jobs unpruned: {e}"),
         }
+        done
     }
 }
 
```

`crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 277b4a3..3ebd504 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -34,7 +34,9 @@ pub use coordinator::{
 };
 pub use discovery::{Seeds, advertise, discover, interfaces};
 pub use error::ClusterError;
-pub use events::{EVENT_QUEUE, PRUNE_EVERY, Recorder, WRITE_RETRIES, expire_events};
+pub use events::{
+    EVENT_QUEUE, PRUNE_EVERY, Recorder, WRITE_RETRIES, expire_events, record_members,
+};
 pub use gossip::{
     Advertiser, Announcer, Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase,
 };
```

`crates/data-avalanche-core/src/classify.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/classify.rs b/crates/data-avalanche-core/src/classify.rs
index 5c084ea..a9652e6 100644
--- a/crates/data-avalanche-core/src/classify.rs
+++ b/crates/data-avalanche-core/src/classify.rs
@@ -39,6 +39,32 @@ pub enum SchemaChange {
     Unclassifiable(String),
 }
 
+impl std::fmt::Display for SchemaChange {
+    /// The change in a few words, as an event or a log line names it (`add column note (text)`).
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        match self {
+            Self::AddColumn(c) => write!(f, "add column {} ({})", c.name, c.source_type),
+            Self::DropColumn(c) => write!(f, "drop column {c}"),
+            Self::RenameColumn { from, to, .. } => write!(f, "rename column {from} to {to}"),
+            Self::ChangeType {
+                column, from, to, ..
+            } => write!(
+                f,
+                "change column {column} from {} to {}",
+                from.source_type, to.source_type
+            ),
+            Self::SetNotNull(c) => write!(f, "set column {c} not null"),
+            Self::DropNotNull(c) => write!(f, "drop not null on column {c}"),
+            Self::IdentifierChanged => f.write_str("change the identifier"),
+            Self::TableDropped => f.write_str("drop the table"),
+            Self::EnumValuesAppended(c) => write!(f, "append values to column {c}"),
+            Self::EnumValuesChanged(c) => write!(f, "change the values of column {c}"),
+            Self::NoTargetEffect(what) => write!(f, "{what} (no effect on the target)"),
+            Self::Unclassifiable(what) => write!(f, "unclassified: {what}"),
+        }
+    }
+}
+
 /// The changes to one table at one stream position.
 #[derive(Debug, Clone, PartialEq, Eq)]
 pub struct SchemaDelta {
```

`crates/data-avalanche-pipeline/src/observe.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/observe.rs b/crates/data-avalanche-pipeline/src/observe.rs
index 42f3dda..4d0c37c 100644
--- a/crates/data-avalanche-pipeline/src/observe.rs
+++ b/crates/data-avalanche-pipeline/src/observe.rs
@@ -5,6 +5,8 @@
 use std::fmt::Debug;
 use std::time::Duration;
 
+use data_avalanche_core::{SchemaChange, TableRef};
+
 /// The owner's ears; every method defaults to nothing, and none may block.
 pub trait Observer: Send + Sync + Debug {
     /// A flush is durable: the rows of its change sets, and the time from when the writer took
@@ -27,6 +29,12 @@ pub trait Observer: Send + Sync + Debug {
     fn chunk_read(&self, took: Duration) {
         let _ = took;
     }
+
+    /// The sink evolved `table` by `changes`: fully, or degraded, with `warning` saying what it
+    /// left undone. A change the sink refused halts the table instead, and isn't reported here.
+    fn schema_evolved(&self, table: &TableRef, changes: &[SchemaChange], warning: Option<&str>) {
+        let _ = (table, changes, warning);
+    }
 }
 
 /// Hears nothing: a worker without an owner's metrics (tests).
```

`crates/data-avalanche-pipeline/src/schema_change.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/schema_change.rs b/crates/data-avalanche-pipeline/src/schema_change.rs
index 237dd40..f79be02 100644
--- a/crates/data-avalanche-pipeline/src/schema_change.rs
+++ b/crates/data-avalanche-pipeline/src/schema_change.rs
@@ -14,7 +14,7 @@ use tokio::sync::watch;
 use tracing::Instrument;
 use tracing::field::Empty;
 
-use crate::{CommitGuard, Tuning, WorkerError, WorkerStatus};
+use crate::{CommitGuard, Observer, Tuning, WorkerError, WorkerStatus};
 
 /// The sink table's columns `source` doesn't have, the lineage and internal columns aside:
 /// soft-dropped, kept and written NULL (`data_avalanche_sink_soft_dropped_columns`).
@@ -156,6 +156,8 @@ pub(crate) struct Evolver<'a> {
     pub tuning: &'a Tuning,
     pub evolution: bool,
     pub status: &'a watch::Sender<WorkerStatus>,
+    /// Hears each evolve the sink applied, fully or degraded.
+    pub observer: &'a dyn Observer,
 }
 
 impl Evolver<'_> {
@@ -217,10 +219,18 @@ impl Evolver<'_> {
             changes = safe.len(),
             outcome = Empty,
         );
+        let changes = safe.clone();
         let applied = self.apply(table, safe).instrument(span.clone()).await;
         let outcome = match &applied {
-            Ok(Ok(None)) => "applied",
-            Ok(Ok(Some(_))) => "degraded",
+            Ok(Ok(None)) => {
+                self.observer.schema_evolved(table, &changes, None);
+                "applied"
+            }
+            Ok(Ok(Some(warning))) => {
+                self.observer
+                    .schema_evolved(table, &changes, Some(warning.as_str()));
+                "degraded"
+            }
             Ok(Err(_)) => "halted",
             Err(_) => "failed",
         };
```

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index a2ca9fa..ce3840c 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -140,6 +140,7 @@ impl Core {
             tuning: &self.tuning,
             evolution: self.settings.evolution,
             status: &self.status,
+            observer: &*self.observer,
         }
     }
 }
```

`crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index fa083c1..6b6633e 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -12,7 +12,7 @@ use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
     ReconcilerSettings, Recorder, Seeds, Udp, advertise, discover, expire_events, interfaces,
-    join as join_gossip, leave, node_identity, removed_pipelines,
+    join as join_gossip, leave, node_identity, record_members, removed_pipelines,
 };
 use data_avalanche_config::{
     Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
@@ -172,6 +172,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         factory: Arc::new(Factory::new(&config.globals).map_err(Fatal::Config)?),
         metrics: metrics.clone(),
         settings: settings(&config),
+        recorder: recorder.clone(),
         starts: Arc::new(Semaphore::new(
             usize::try_from(c.max_concurrent_takeovers).unwrap_or(usize::MAX),
         )),
@@ -212,6 +213,8 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     let key = GossipKey::derive(&joined.fingerprint_key);
     let gossip = Gossip::start(me, key, GossipSettings::new(m), Arc::new(udp), inbox);
     tracing::info!(%addr, bound = ?bind, "gossip started");
+    // Joins, suspicions and departures, each recorded by one node (Admin_Console §6).
+    let _membership = tokio::spawn(record_members(recorder.clone(), gossip.members()));
     let seeds = Seeds::new(c);
     let seeded = !seeds.is_empty();
     let discovery = discover(seeds, own, gossip.announcer(), gossip.members());
@@ -244,6 +247,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
                 reloaded_auth.set(&g.api);
                 retention.send_replace(g.console.events.retention);
             }),
+            recorder: recorder.clone(),
         },
         gossip.members(),
         c.lease_renewal_interval,
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib && cargo test -p data-avalanche-pipeline --lib added_column`

Expected: PASS, 180 tests.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings && cargo test --workspace`

Expected: no output from the first three; every test binary passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core crates/data-avalanche-pipeline crates/data-avalanche-cluster crates/data-avalanche-api/src/test_support.rs crates/data-avalanche/src/run.rs
git commit -m "feat(cluster): the cluster's own events — leases, pipeline states, halts, schema changes, jobs, members, generations, removals"
```

---

### Task 6: The site, the counters' catalogue and the alert

**Files:**
- Create: `docs/src/content/docs/deployment/events.md`
- Modify: `docs/src/content/docs/deployment/observability.md`
- Modify: `deploy/prometheus/alerts.yaml`; test: `deploy/prometheus/alerts.test.yaml`
- Test: `crates/data-avalanche-telemetry/tests/catalogue.rs` (S-M13a.1's catalogue row)

**Interfaces:**
- Consumes: Task 3's counters; the kinds of Tasks 4 and 5; D-M13a.8's parameters.
- Produces: `DataAvalancheEventsDropped` (`increase(data_avalanche_events_dropped_total[15m]) > 0`,
  warning); the page `/deployment/events/` (order 4).

The site builds with its link check (`npx astro build` in `docs/`).

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-telemetry/tests/catalogue.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/catalogue.rs b/crates/data-avalanche-telemetry/tests/catalogue.rs
index bd56122..d148002 100644
--- a/crates/data-avalanche-telemetry/tests/catalogue.rs
+++ b/crates/data-avalanche-telemetry/tests/catalogue.rs
@@ -62,6 +62,9 @@ const CATALOGUE: &[(&str, Option<&[&str]>)] = &[
     // Status
     ("data_avalanche_pipeline_status", Some(&["status"])),
     ("data_avalanche_halted_tables", Some(&[])),
+    // Events (Admin_Console §6)
+    ("data_avalanche_events_written_total", None),
+    ("data_avalanche_events_dropped_total", None),
 ];
 
 /// The per-table series `sink.metrics.per_table` adds `table` to, with their other labels.
@@ -213,7 +216,7 @@ fn the_alert_rules_name_only_catalogued_series_and_labels() {
         "/../../deploy/prometheus/alerts.yaml"
     ));
     let named = selectors(rules);
-    assert_eq!(named.len(), 5, "{named:?}");
+    assert_eq!(named.len(), 6, "{named:?}");
     for (name, labels) in named {
         let (_, extra) = CATALOGUE
             .iter()
```

`deploy/prometheus/alerts.test.yaml`:

```diff
diff --git a/deploy/prometheus/alerts.test.yaml b/deploy/prometheus/alerts.test.yaml
index 4ba24b8..b97afa8 100644
--- a/deploy/prometheus/alerts.test.yaml
+++ b/deploy/prometheus/alerts.test.yaml
@@ -15,6 +15,8 @@ tests:
         values: "0 1"
       - series: 'data_avalanche_wal_lag_bytes{pipeline="orders",source_host="db1",source_db="shop"}'
         values: "1073741824 21474836480x15"
+      - series: 'data_avalanche_events_dropped_total{instance="node-a:8080"}'
+        values: "0x20 3x20"
     alert_rule_test:
       - eval_time: 5m
         alertname: DataAvalancheReplicationLag
@@ -80,3 +82,14 @@ tests:
               source_db: shop
             exp_annotations:
               summary: "orders's slot retains 20GiB of WAL"
+      - eval_time: 15m
+        alertname: DataAvalancheEventsDropped
+        exp_alerts: []
+      - eval_time: 22m
+        alertname: DataAvalancheEventsDropped
+        exp_alerts:
+          - exp_labels:
+              severity: warning
+              instance: "node-a:8080"
+            exp_annotations:
+              summary: "node-a:8080 dropped events from the cluster's timeline"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-telemetry --test catalogue && docker run --rm -v "$PWD/deploy/prometheus:/rules:ro" --entrypoint promtool prom/prometheus:v3.16.0 test rules /rules/alerts.test.yaml`

Expected: FAIL. The failing test, as measured:

```text
test the_alert_rules_name_only_catalogued_series_and_labels ... FAILED
test per_table_adds_the_table_label_to_the_snapshot_series_only ... ok
test every_mandatory_series_is_exported_with_its_labels ... ok
thread 'the_alert_rules_name_only_catalogued_series_and_labels' (3011051) panicked at crates/data-avalanche-telemetry/tests/catalogue.rs:219:5:
assertion `left == right` failed: [("data_avalanche_replication_lag_seconds", []), ("data_avalanche_lease_state", ["state"]), ("data_avalanche_halted_tables", []), ("data_avalanche_pipeline_status", ["status"]), ("data_avalanche_wal_lag_bytes", [])]
  left: 5
test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

- [ ] **Step 3: Implement**

`deploy/prometheus/alerts.yaml`:

```diff
diff --git a/deploy/prometheus/alerts.yaml b/deploy/prometheus/alerts.yaml
index 34b016a..6fccb0b 100644
--- a/deploy/prometheus/alerts.yaml
+++ b/deploy/prometheus/alerts.yaml
@@ -39,6 +39,15 @@ groups:
         annotations:
           summary: "{{ $labels.pipeline }} is in ERROR"
 
+      # Events the node couldn't write to the cluster's timeline (Admin_Console §6): the store
+      # kept failing, or more came than the writer keeps up with. The trail has gaps.
+      - alert: DataAvalancheEventsDropped
+        expr: increase(data_avalanche_events_dropped_total[15m]) > 0
+        labels:
+          severity: warning
+        annotations:
+          summary: "{{ $labels.instance }} dropped events from the cluster's timeline"
+
       # The WAL a PostgreSQL slot retains for the pipeline: it fills the server's disk.
       - alert: DataAvalancheWalLag
         expr: data_avalanche_wal_lag_bytes > 10 * 1024 * 1024 * 1024
```

`docs/src/content/docs/deployment/events.md`:

```diff
diff --git a/docs/src/content/docs/deployment/events.md b/docs/src/content/docs/deployment/events.md
new file mode 100644
index 0000000..fcf4a0f
--- /dev/null
+++ b/docs/src/content/docs/deployment/events.md
@@ -0,0 +1,96 @@
+---
+title: Events and audit trail
+description: The cluster's timeline of what happened and who did it, how to read it, and how long it is kept.
+sidebar:
+  order: 4
+---
+
+Every cluster keeps one timeline of what happened to it: the actions people took through the API,
+the attempts it refused, and what the cluster did on its own. Any node reads it, from the
+cluster's state location (`globals.state`), and `GET /v1/events` returns it.
+
+## Who did it
+
+Every call that changes something records who made it and, if they gave one, why:
+
+```sh
+curl -X POST https://avalanche.example.com:8080/v1/pipelines/orders-pg/pause \
+  -u oleksii:… -H 'Content-Type: application/json' \
+  -d '{"note": "the warehouse migration, back by 15:00"}'
+```
+
+The event names the caller and their role as the server verified them (`anonymous` when the API
+has no authentication), the note, and how the call ended: `accepted`, `unchanged` (the pipeline
+was paused already) or `refused` (a `409` or `422`, with the reason). A note is optional, at most
+500 characters; any other body field is the endpoint's own, and a request without a body works
+as before.
+
+A caller whose role is too low for a call (a `403`) is recorded too, as `action.denied`, so the
+trail shows attempts as well as changes. A node records at most 30 of those at once and then one
+a second; the rest are logged only.
+
+## What is recorded
+
+| Kind | Severity | When |
+|---|---|---|
+| `action.<object>.<verb>` | info, warning when refused | an API action: `pipeline.pause`, `.stop`, `.resume`, `.restart`, `.flush`, `snapshot.start`, `config.reload`, `node.leave`, `node.shutdown` |
+| `action.denied` | warning | a call refused for the caller's role |
+| `lease.acquired`, `lease.takeover`, `lease.released`, `lease.lost` | info, warning, info, error | a pipeline starts on a node; is taken over from a node that stopped renewing (a failover); is handed off; is lost |
+| `pipeline.error`, `pipeline.retrying`, `pipeline.running` | error, warning, info | a pipeline goes into `ERROR`; fails and is retried; runs again after either, or after a pause or a stop |
+| `pipeline.paused`, `pipeline.stopped`, `pipeline.dumped` | info | the owner carried out a pause or a stop; a dump finished |
+| `pipeline.degraded`, `pipeline.recovered` | warning, info | sink commits or state writes start failing and being retried; succeed again |
+| `pipeline.removed` | info, warning | a pipeline gone from the config is drained and its source's slot and publication torn down; a warning when that waits for its lease to expire |
+| `table.halted`, `table.resumed` | error, info | a table halts, with its reason; runs again after a snapshot job brought it back |
+| `schema.applied`, `schema.degraded` | info, warning | a source schema change is evolved into the sink, fully or in part (a change the sink refuses halts the table) |
+| `job.completed`, `job.failed` | info, error | a snapshot job ends |
+| `member.joined`, `member.restarted`, `member.draining`, `member.left` | info | a node joins, restarts, starts handing its pipelines off, leaves |
+| `member.suspect`, `member.alive`, `member.down` | warning, info, warning | a node stops answering probes, answers again, is declared failed |
+| `config.published` | info | a config generation is published |
+| `config.refused` | error | a node can't apply a generation, and runs the one before |
+
+Each event has its time, the node that recorded it, its pipeline when it has one, its object (a
+table, a job, a node, a generation), and its text. A change the whole cluster sees, such as a
+node going suspect, is recorded by one node.
+
+## Reading it
+
+`GET /v1/events` (any role) returns the newest first, 100 at a time:
+
+```sh
+curl -s 'https://avalanche.example.com:8080/v1/events?kind=action&actor=oleksii&since=2026-11-14T00:00:00Z'
+```
+
+| Parameter | Filters on |
+|---|---|
+| `since`, `until` | the time, RFC 3339: from `since`, before `until` |
+| `kind` | a kind or a family: `action`, `lease.takeover`; repeat it, or separate with commas, for several |
+| `object` | a pipeline's id, or an object's name: `public.orders`, a job's id, a node |
+| `actor` | who caused it: the name as recorded, exactly |
+| `node` | the node that recorded it |
+| `q` | text in the event or its note, ignoring case |
+| `limit` | 1 to 1000 |
+| `after` | the `next` of the page before |
+
+A page that stopped before the end of what matches says where to go on: its `next`, passed back as
+`after`. A query looks at no more than 5000 events; past that, `next` takes it further.
+
+## How long it is kept
+
+`globals.console.events.retention`, 30 days by default (1 day to 400 days). Every node deletes
+the days past it once an hour, and a reload changes it. The events of one day are one folder,
+`events/<YYYY-MM-DD>/`, in the state location, so a day expires whole.
+
+## Keep the state location private
+
+Events name people and describe your pipelines, their tables and their failures. Keep
+`globals.state` where only the cluster and its operators can read it, not in a bucket or prefix
+your lake's readers have access to.
+
+## When the timeline has gaps
+
+Recording an event never waits on the storage, and never fails or slows what it records. A node
+retries a write that fails three times over about 6 seconds, then drops the event; and when events
+come faster than it writes them (more than 1024 waiting), it drops the new ones. Each drop is
+logged and counted in `data_avalanche_events_dropped_total`, and the
+[`DataAvalancheEventsDropped`](/data-avalanche/deployment/observability/#alerts) alert fires on
+it. A node that crashes loses the events it hadn't written yet.
```

`docs/src/content/docs/deployment/observability.md`:

```diff
diff --git a/docs/src/content/docs/deployment/observability.md b/docs/src/content/docs/deployment/observability.md
index 6a714bd..0e39a3b 100644
--- a/docs/src/content/docs/deployment/observability.md
+++ b/docs/src/content/docs/deployment/observability.md
@@ -11,7 +11,7 @@ sidebar:
 exposes the pipelines it owns. Every pipeline series has the labels `pipeline`, `source_host` and
 `source_db`; the node's own gauges (`data_avalanche_cluster_members`,
 `data_avalanche_owned_pipelines`, `data_avalanche_ring_generation`,
-`data_avalanche_config_generation`) have none. `sink.metrics.per_table: true` adds a `table` label
+`data_avalanche_config_generation`) and its event counters have none. `sink.metrics.per_table: true` adds a `table` label
 to the snapshot gauges.
 
 | Family | Metrics |
@@ -22,6 +22,7 @@ to the snapshot gauges.
 | Sink | `data_avalanche_sink_commit_duration_seconds{sink_type}`, `data_avalanche_sink_commit_failures_total`, `data_avalanche_sink_merge_files_rewritten_total`, `data_avalanche_sink_merge_rows_copied_total`, `data_avalanche_sink_soft_dropped_columns` |
 | Cluster | `data_avalanche_lease_state`, `data_avalanche_lease_renew_failures_total`, `data_avalanche_lease_renew_latency_seconds`, `data_avalanche_cluster_members`, `data_avalanche_owned_pipelines`, `data_avalanche_ring_generation` |
 | Status | `data_avalanche_pipeline_status` (running, paused, stopped, error), `data_avalanche_halted_tables` |
+| Events | `data_avalanche_events_written_total`, `data_avalanche_events_dropped_total`: the node's writes to the cluster's [timeline](/data-avalanche/deployment/events/) |
 
 The ones to watch first:
 
@@ -53,6 +54,7 @@ is a starting set of Prometheus rules, with `promtool` tests beside it:
 | `DataAvalancheTablesHalted` | a halted table | [A halted table](/data-avalanche/runbooks/halted-table/) |
 | `DataAvalanchePipelineError` | a pipeline in `error` | [A halted table](/data-avalanche/runbooks/halted-table/#a-pipeline-in-error) |
 | `DataAvalancheWalLag` | a slot holds more than 10 GiB of WAL for 10 minutes | [A lost position](/data-avalanche/runbooks/position-lost/) |
+| `DataAvalancheEventsDropped` | a node dropped events from the cluster's timeline in the last 15 minutes | [Events](/data-avalanche/deployment/events/#when-the-timeline-has-gaps) |
 
 ## The log
 
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-telemetry --test catalogue && docker run --rm -v "$PWD/deploy/prometheus:/rules:ro" --entrypoint promtool prom/prometheus:v3.16.0 test rules /rules/alerts.test.yaml`

Expected: PASS, 3 tests and promtool's `SUCCESS`.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings && cargo test --workspace`

Expected: no output from the first three; every test binary passes.

- [ ] **Step 5: Commit**

```bash
git add docs deploy/prometheus crates/data-avalanche-telemetry/tests/catalogue.rs
git commit -m "docs: the events page, the event counters and the dropped-events alert"
```

---

### Task 7: The timeline on three nodes (E2E)

**Files:**
- Create: `crates/data-avalanche/tests/e2e/events.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the E2E harness's `Cluster` (three nodes, `kill`, `settled`, `holders`), `Proc::{send,
  call, ask, shows}`; Tasks 4 and 5's events.
- Produces: `the_timeline_records_who_paused_and_a_failover`.

The behaviour landed in Tasks 4 and 5: this task has no implementation step, and its scenario
passes at once.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche/tests/e2e/events.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/events.rs b/crates/data-avalanche/tests/e2e/events.rs
new file mode 100644
index 0000000..d911dc9
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/events.rs
@@ -0,0 +1,89 @@
+//! The cluster's timeline (Admin_Console §6) on three nodes: a pause asked of one node with a
+//! note is recorded with who asked and why; the holder is killed, and the takeover, naming the
+//! node it took over from, and the node's going down are recorded, the latter by one node; any
+//! live node reads it all back through `GET /v1/events`.
+
+use std::time::Duration;
+
+use data_avalanche_testkit::require_it;
+use serde_json::Value;
+
+use crate::harness::{Proc, eventually};
+use crate::nodes::Cluster;
+
+/// The events `query` selects, as `node` lists them.
+async fn listed(node: &Proc, query: &str) -> Vec<Value> {
+    let (code, v) = node.call("GET", &format!("/v1/events?{query}")).await;
+    assert_eq!(code, 200, "{v}");
+    v["events"].as_array().expect("events").clone()
+}
+
+#[tokio::test]
+async fn the_timeline_records_who_paused_and_a_failover() {
+    require_it!();
+    let mut c = Cluster::new("events_timeline").await;
+    c.start_all().await;
+    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
+    c.landed().await;
+    let id = c.e.test.clone();
+    let path = format!("/v1/pipelines/{id}/pause");
+    let first = c.nodes[0].proc.as_ref().expect("a live node");
+    let (code, _, body) = first
+        .send(None, "POST", &path, r#"{"note": "the e2e pause"}"#)
+        .await
+        .expect("an answer");
+    assert_eq!(code, 202, "{body}");
+    first.shows(&id, "paused").await;
+    assert_eq!(first.ask(&id, "resume").await.0, 202);
+    first.shows(&id, "running").await;
+
+    let victim = c.holders().await[&id].0.clone();
+    let i = c.index(&victim);
+    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
+    c.settled().await;
+    let reader = c.live()[0].proc.as_ref().expect("a live node");
+
+    let actions = listed(reader, &format!("kind=action&object={id}")).await;
+    let kinds: Vec<&str> = actions
+        .iter()
+        .map(|e| e["kind"].as_str().unwrap())
+        .collect();
+    assert_eq!(
+        kinds,
+        ["action.pipeline.resume", "action.pipeline.pause"],
+        "newest first"
+    );
+    let pause = &actions[1];
+    assert_eq!(pause["actor"]["name"], "anonymous");
+    assert_eq!(pause["note"], "the e2e pause");
+    assert_eq!(pause["outcome"], "accepted");
+    assert_eq!(pause["node"], c.nodes[0].name.as_str());
+
+    eventually("the takeover", Duration::from_secs(30), async || {
+        listed(reader, &format!("kind=lease.takeover&object={id}"))
+            .await
+            .first()
+            .is_some_and(|e| {
+                e["text"]
+                    .as_str()
+                    .unwrap()
+                    .contains(&format!("from {victim}"))
+            })
+    })
+    .await;
+    eventually("the node down", Duration::from_secs(60), async || {
+        !listed(reader, &format!("kind=member.down&object={victim}"))
+            .await
+            .is_empty()
+    })
+    .await;
+    // Both live nodes saw it go: one recorded it.
+    tokio::time::sleep(Duration::from_secs(5)).await;
+    assert_eq!(
+        listed(reader, &format!("kind=member.down&object={victim}"))
+            .await
+            .len(),
+        1
+    );
+    c.stop().await;
+}
```

`crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index db20925..f6fcb21 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -9,6 +9,7 @@
 mod api;
 mod cluster;
 mod crashes;
+mod events;
 mod failover;
 mod forwarder;
 mod guards;
```

- [ ] **Step 2: Run the scenario**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e events:: -- --test-threads=1`

Expected: PASS at once (no implementation step: the behaviour is the earlier tasks'):

```text
test events::the_timeline_records_who_paused_and_a_failover ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 51 filtered out; finished in 46.34s
```

- [ ] **Step 3: Implement**

Nothing: the scenario passes on Tasks 1–6.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e events:: -- --test-threads=1`

Expected: PASS, 1 test.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings && cargo test --workspace`

Expected: no output from the first three; every test binary passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests/e2e
git commit -m "test(e2e): the timeline on three nodes — a pause with its note, a failover, a node down recorded once"
```

---

## Exit

- [ ] Every task's tests pass, and `cargo test --workspace` (1559 tests at the plan's
  verification).
- [ ] The E2E suite on PostgreSQL 18 (52 scenarios at the plan's verification, the new one among
  them: 52/52 in 670 s).
- [ ] `promtool test rules` on the alert rules; the site builds with its link check.
- [ ] Every mutating route of the OpenAPI document records (Task 4's route table), and
  `GET /v1/events` is in it as viewer.
- [x] S-M13a.1 applied (49e5ee4).
- [ ] The PR's CI green on every target.

## Deviations during execution

None yet.
