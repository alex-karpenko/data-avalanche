---
updated: 2026-11-04
domain: Architecture
tags: [v1, implementation-plan, m12, m12b, hardening, cluster, discovery, lease, config-reload, removal, snapshot-jobs, api, rate-limit, observability, otlp, audit, compile-time]
resolves: [DA-CLUS-15, DA-API-13, DA-OPS-9, DA-OPS-10, DA-OPS-12, DA-SNAP-15, DA-SNAP-16, DA-SNAP-19, DA-SNAP-20, DA-SNAP-21, DA-SNAP-11, DA-SNAP-12, DA-API-16, DA-API-17, DA-API-18, DA-API-20, DA-OBS-7, DA-OBS-8, DA-OBS-9, DA-SNAP-7, DA-X-13]
---

# M12b — Cluster, Control, API and Observability Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** the second of M12's parts (Q-M12-4, Q-M12-8): every open cluster, control, API and
observability gap closed (Q-M12-1; tracker §56), the dependency audit clean or triaged, and the
compile time measured. That covers:
- nodes that formed separate gossip groups, merged;
- a lease holder that is gone, shown as stale;
- a pipeline removed while no node held it, torn down, and the pipeline that reuses its resources
  sequenced after it on every node;
- a process-restart key written as a literal, changed by a reload and a rolling restart;
- a snapshot job that empties the sink table first (`truncate: true`, Q-M12-7);
- the snapshot job's loose ends: retention, an undecodable document, a copy's halt, a lost
  restart;
- the reconciler's log and the PostgreSQL chunk reads, bounded and parallel;
- the API's key fetch, connections, validate rate and OpenAPI document;
- the trace export's headers, the lag across sessions, the JSON log's trace ids end to end.

**Architecture:** the work is in the control plane (`cluster`, `api`, `state`, `config`), with
three pieces that need more than a local fix:

- **The removal tombstone** (Tasks 3–4). A node that adopts a generation which drops pipeline
  ids first writes `removals/<id>.json` in `globals.state` (`RemovalDoc { pipeline_id,
  generation, hash, removed_at, done_at }`: the last generation that had the id), then applies
  the removal. The controller that tears a pipeline down
  marks its tombstone done. Every 30 s (`SWEEP_INTERVAL`) a node reads the pending tombstones and
  tears down, under the lease, each one the ring places on it that no controller holds. A
  controller waits, before its first claim, for every pending tombstone of its own id or of a
  pipeline it shares a slot, publication, sink or state location with.
- **Restart-only fields on accept** (Task 5). `POST /v1/config/reload?restart=accept` publishes a
  generation with process-restart changes. A running node adopts it and keeps the values it
  started with (`RestartFields`, listed in `restart_pending`); its next start takes them. The
  fields the cluster keeps for good (`FIXED_FIELDS`) are refused even then.
- **The job's truncate** (Task 6). `SnapshotJobDoc.truncate`; the worker commits a snapshot
  change set with `truncate` and no rows for each started table of a truncating job, before it
  records the table running. `validate` accepts that shape, and the guard never skips it.

The rest are local fixes, each pinned by a test.

**Tech Stack:** Rust 1.98.1. One new dev-dependency, `boon =0.6.1` (the OAS 3.1 schema check), and
`rcgen` and `reqwest` (`blocking`) as `telemetry`'s dev-dependencies (both already in the lock).
Lockfile bumps for the audit (Task 14). The planning's probes ran on test-infra's PostgreSQL 18
and the Jaeger collector.

**Spec:**
- `doc/specs/Configuration_Specification.md`: §1.2 (mutability classes), §6.3 (`globals.api`
  auth), §6.6 (`globals.tracing`), §11 (whole-config rejection), §12.3 (reconcile: removed,
  modified), V-061, and the new V-125 and V-126.
- `doc/specs/Data-Flow.md`: Phase 4 (the guard), On-Demand Snapshot.
- `doc/specs/System-Overview.md`: the management API's endpoint table.
- `doc/research/Clustering_Architecture_Research.md` §3.5 and D-4 (discovery), §8 (leases), §9
  (config generations).
- `doc/research/API_RBAC_and_Versioning.md` §2, §7.
- `doc/research/Observability_Metrics_and_Tracing.md` (the lag, the spans, the correlation).
- `doc/v1.0/00-roadmap.md`: §3 (interfaces), §4 M12.

**Spec edits (approved with this plan, applied before Task 1):** the patch below, drafted on
scratch copies of the three specs.

- **Config §1.2, §11, §12.3, V-061:** a reload with `?restart=accept` publishes a PROC change,
  applied at each node's next start; the fields the cluster keeps for good are refused even then.
- **Config §12.3:** the removal tombstone replaces the orphan-cleanup design item.
- **Config §6.6:** `globals.tracing.headers` (secrets, R, V-126).
- **Config §13.4:** V-125 (a non-`https` key URL warns) and V-126; changelog item 31.
- **Data-Flow:** the job's `truncate` in On-Demand Snapshot, and in Phase 4's never-skipped line.
- **System-Overview:** the reload, validate, status and snapshot rows.

`doc/specs/Configuration_Specification.md`:

```diff
diff --git a/doc/specs/Configuration_Specification.md b/doc/specs/Configuration_Specification.md
--- a/doc/specs/Configuration_Specification.md
+++ b/doc/specs/Configuration_Specification.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-11-03
+updated: 2026-11-04
 domain: Architecture
 status: Specification (supersedes the Configuration_file_model.md draft)
 tags: [architecture, configuration, spec, validation, multi-sink, iceberg, delta-lake]
@@ -51,7 +51,7 @@ to it triggers:
 |---|---|---|
 | **R** | Reloadable | Applied in place, no interruption. (e.g., `log.level`, `sink.flush.*`, `tables.include`.) |
 | **PR** | Pipeline-restart | The affected pipeline(s) are restarted per the restart policy (§12.4): drain/flush, release resources, re-create. Other pipelines keep running. (e.g., `source.host`, `sink.iceberg.catalog.*`.) |
-| **PROC** | Process-restart | Cannot be hot-applied. The reload is **rejected** with an error naming the field; the operator must restart the worker process. (e.g., `cluster.tokens`, `api.address`, memberlist binding.) |
+| **PROC** | Process-restart | Cannot be hot-applied. The reload is **rejected** with an error naming the field (V-061), unless it is asked with `?restart=accept`: the generation is then published, each running node keeps the value it started with (the field is listed in its `restart_pending`, `GET /v1/config/status`), and a node takes the new value at its next start, a rolling restart. (e.g., `api.address`, memberlist binding, `cluster.lease_renewal_interval`.) The fields the cluster keeps for good — `config_version`, `globals.cloud`, `globals.state`, `globals.azure`, `cluster.tokens`, `cluster.lease_duration`, `cluster.lock_backend` — are refused even then: a new value needs a new `globals.state`, every node stopped (DA-OPS-12). |
 
 The reload engine computes the highest class touched per scope and acts accordingly. A reload
 that touches **only** R-class fields never interrupts data flow.
@@ -487,6 +487,7 @@ When tracing is active, `trace_id` / `span_id` are injected into JSON log record
 |---|---|---|---|---|---|
 | `otlp_endpoint` | URL | no | — | R | OTLP/HTTP (protobuf) collector base URL, e.g. `http://otel-collector:4318`; spans are sent to `<endpoint>/v1/traces`. **Absent ⇒ tracing disabled** (zero overhead, no error). An endpoint the exporter can't use is a warning at run time, and the one in force stays. |
 | `sample_ratio` | float [0,1] | no | `0.0` (no endpoint) / `0.05` (endpoint set) | R | Head sampling ratio (V-097). `> 0` without an endpoint warns (V-098). |
+| `headers` | map<string, string> | no | `{}` | R | Sent with each export: a hosted collector's token or API key, e.g. `authorization: "Bearer ${OTLP_TOKEN}"`. The values are secrets (§4: fingerprinted, never shown). Each name an HTTP token, each value visible ASCII, spaces and tabs (V-126). |
 
 ### 6.7 `globals.azure` (`cloud = azure` only)
 
@@ -1065,7 +1066,9 @@ Other notes:
   block other pipelines. (Resolution checks that a credential is *present and well-formed*, not
   that it is *accepted* by the server.)
 - On reload, a change to any **PROC** field is rejected (V-061) with a message naming the field;
-  the previous config stays in effect (a reload-time instance of whole-config rejection).
+  the previous config stays in effect (a reload-time instance of whole-config rejection). A
+  reload asked with `?restart=accept` publishes it, except for the fields the cluster keeps for
+  good (§1.2), and each node applies it at its next start.
 
 ### 11.1 Resolved config at runtime
 
@@ -1153,14 +1156,24 @@ highest class touched.
 
 - **Added** (`id` new): start it (Phase 0, `Data-Flow.md`), snapshot per `snapshot_mode`.
 - **Removed** (`id` gone): stop, drain/flush, drop DB resources if `auto_cleanup`, retain
-  Iceberg artifacts (never deleted, per `System-Overview.md`). Orphan-cleanup of resources for
-  a tombstoned `id` whose owner was offline is a design item (gap 2.4).
+  Iceberg artifacts (never deleted, per `System-Overview.md`). The node that adopts the
+  generation first records a **removal tombstone** in `globals.state` (`removals/<id>.json`: the
+  id and the last generation that had it) before it applies the removal; a tombstone that can't
+  be written refuses the adoption, which is tried again. The pipeline's owner tears it down and
+  marks the tombstone done. A removal no node owns (its owner was down, or a hand-off was between
+  release and claim) is torn down by the node the ring places the id on: every 30 s a node reads
+  the pending tombstones, resolves each one's pipeline from its generation, claims the lease once
+  it is free, tears it down as `auto_cleanup` says, and marks it done (DA-OPS-9). A pipeline that
+  shares the removed one's id, slot, publication, sink or state location waits, before it claims
+  its lease, until every such tombstone is done, on every node, whatever reload or start brought
+  it (DA-OPS-10).
 - **Renamed** (key changed, `id` same): pure rename; the `_pipeline` label changes, no
   resource churn.
 - **`id` changed**: delete-and-recreate (drop old resources per `auto_cleanup`, start fresh).
 - **Modified, only R fields**: applied in place.
 - **Modified, any PR field**: restart that pipeline per §12.4.
-- **Modified, any PROC field**: reload rejected (V-061).
+- **Modified, any PROC field**: reload rejected (V-061), or with `?restart=accept` published and
+  applied at each node's next start (§1.2).
 
 ### 12.4 Pipeline restart policy (in-flight batches)
 
@@ -1289,6 +1302,8 @@ errors.
 | V-029 | `sink.iceberg.catalog.rest.auth = sigv4` | Requires `globals.cloud = aws`. | error | pipeline |
 | V-030 | `source.ssl_mode = allow` | Invalid for MySQL. | error | pipeline |
 | V-124 | `source.ssl_mode = verify-ca`, when `source.type = mysql` | The host name is checked too, as with `verify-full` (DA-SRC-22). | warning | pipeline |
+| V-125 | `globals.api.jwt.jwks_url`, `globals.api.oauth2.jwks_url`, `discovery_url` | Not `https`: the issuer's keys could be swapped on the way (DA-API-16). | warning | config |
+| V-126 | `globals.tracing.headers` | Each name an HTTP token; each value visible ASCII, spaces and tabs (DA-OBS-7). | error | config |
 | V-073 | `globals.cluster.lease_renewal_interval` | Must be ≤ `lease_duration / 3` (the holder must get ≥2 renew attempts inside the renewal safety deadline). **Hard error** — promoted from the previously-advisory guidance (Clustering doc §18.2/§19.6, D-8). | error | config |
 | V-033 | `tables.snapshots.overrides` keys | Must be full `schema.table`; globs forbidden. | error | pipeline |
 | V-034 | `tables.snapshots.overrides` keys | Key not in the resolved include/exclude set. | warning | pipeline |
@@ -1351,7 +1366,7 @@ errors.
 
 | ID | Path / Scope | Rule | Severity | Locus |
 |---|---|---|---|---|
-| V-061 | any PROC-class field | Changed during a live reload. | error | reload |
+| V-061 | any PROC-class field | Changed during a live reload, without `?restart=accept`; a field the cluster keeps for good (§1.2) even with it. | error | reload |
 | V-062 | `globals.cluster.tokens` | Differs from the cluster-established value in `globals.state`. | error | config |
 | V-076 | `globals.cluster.lock_backend` | Differs from the cluster-established value in the `globals.state` anchor. | error | config |
 | V-094 | `globals.cluster.lease_duration` | Differs from the cluster-established value in the `globals.state` anchor (`lease_duration_ms`). | error | config |
@@ -1634,6 +1649,12 @@ Every deviation, so nothing is silent:
     recovered by `re_snapshot` (it halted with no recovery); MariaDB's check by
     `BINLOG_GTID_POS`. §9.1 `ssl_mode`: MySQL `verify-ca` checks the host name too, warning
     **V-124**.
+31. **M12b planning (2026-11-04, Q-M12b-1…4)** — §1.2 PROC, §11 and §12.3: a reload with
+    `?restart=accept` publishes a process-restart field, applied at each node's next start
+    (DA-OPS-12); the cluster-pinned fields are refused even then (**V-061**). §12.3: the removal
+    tombstone replaces the orphan-cleanup design item (DA-OPS-9, DA-OPS-10). §6.6 new
+    `globals.tracing.headers` (secrets, R, **V-126**). §6.3: a non-`https` key URL warns
+    (**V-125**).
 
 None of these change the **decided** catalog-auth schema (hive Thrift `none/ldap/kerberos`;
 rest HTTP `none/bearer/oauth2/sigv4`) — those are reproduced verbatim from
```

`doc/specs/Data-Flow.md`:

```diff
diff --git a/doc/specs/Data-Flow.md b/doc/specs/Data-Flow.md
--- a/doc/specs/Data-Flow.md
+++ b/doc/specs/Data-Flow.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-11-03
+updated: 2026-11-04
 domain: Architecture
 tags: [architecture, data-flow, pipeline, cdc, iceberg, delta-lake, multi-sink, arrow, snapshot, replication]
 sources: []
@@ -530,7 +530,7 @@ the table is already committed and is skipped. `last_change_for_table` is the co
 flush's last transaction that wrote the table, not the flush frontier: a replay's flush may read past
 the marker (a keep-alive, another table's transaction) while carrying only transactions the table has.
 A snapshot change set (a chunk's rows, flagged
-`snapshot`) is never skipped: its rows are read now, at a frontier the marker may already cover; the
+`snapshot`, or a job's `truncate` with no rows) is never skipped: its rows are read now, at a frontier the marker may already cover; the
 fence still applies. A split slice (`split` set) ends at its transaction's
 pinned start, which the stored position equals from the first slice on; a slice is skipped only
 when the stored position is strictly past that start (the transaction's commit, or a later one, has
@@ -738,8 +738,9 @@ Triggered via the management REST API (`POST /v1/pipelines/{id}/snapshots`; see
 endpoint table):
 
 ```
-Request payload: { tables: ["schema.orders", "schema.products"] }
-                  (exact table names, no globs; may include tables not in pipeline config)
+Request payload: { tables: ["schema.orders", "schema.products"], truncate: false }
+                  (exact table names, no globs; may include tables not in pipeline config;
+                   truncate: empty each table's sink table first, default false)
 
 1. Validate: tables list non-empty, no globs.
    Persist a job record snapshots/jobs/<job_id>.json (status queued) and request a restart of the
@@ -749,7 +750,10 @@ Request payload: { tables: ["schema.orders", "schema.products"] }
    schema (Schema_Change_Detection_and_Evolution.md §8.2). The stream resumes from the confirmed
    position, as after any restart. For each table: run the Phase-1 chunk loop (watermark lw/hw +
    keyset chunks) WITHOUT pausing the stream. Tables already in the streaming set keep streaming;
-   the snapshot reconciles against the live stream exactly as at startup.
+   the snapshot reconciles against the live stream exactly as at startup. A `truncate` job first
+   empties each of its tables' sink table: a snapshot change set with `truncate` and no rows,
+   committed (fenced) before the start records the table running, so a crash in between empties
+   it again and a restart after it resumes the copy without emptying it.
 3. Tables not in the pipeline config: snapshot is written to the sink but the table is not added to
    the pipeline's streaming set. Such a table is described by its exact name and copied: read
    chunk by chunk, never streamed, so not reconciled.
@@ -766,8 +770,9 @@ cursors, like the Phase-1 snapshot does after a crash.
 
 There is **no stream pause and no `pause_lsn + 1` assumption**: watermarks reconcile re-snapshotted
 rows against concurrent changes, and idempotent upserts mean re-reading existing keys re-writes the
-same rows. A job never truncates the target: a row deleted at the source while the table wasn't
-streamed stays. This is also the mechanism for **re-snapshot-after-unsafe-DDL** (`Schema_Change_Detection_
+same rows. A job without `truncate` never empties the target: a row deleted at the source while the
+table wasn't streamed stays. With `truncate`, such a row goes, at a price: the sink table is empty,
+then partly copied, until the job completes (DA-SNAP-15). This is also the mechanism for **re-snapshot-after-unsafe-DDL** (`Schema_Change_Detection_
 and_Evolution.md` §8.2).
 
 ---
```

`doc/specs/System-Overview.md`:

```diff
diff --git a/doc/specs/System-Overview.md b/doc/specs/System-Overview.md
--- a/doc/specs/System-Overview.md
+++ b/doc/specs/System-Overview.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-10-28
+updated: 2026-11-04
 domain: Architecture
 tags: [architecture, design, system, multi-sink, iceberg, delta-lake, v1-scope]
 sources: []
@@ -121,12 +121,12 @@ Functional endpoints are versioned under `/v1`; probes and metrics are unversion
 |---|---|---|---|
 | Liveness / readiness | `GET /health/live`, `GET /health/ready` | none | |
 | Metrics | `GET /metrics` | none | Prometheus exposition |
-| Config validate | `POST /v1/config/validate` | operator | dry-run of the full load pipeline (parse → validate → resolve) of the body, or of the node's file when the body is empty: the report and what a reload would change; the resolved config is not returned; nothing is published |
-| Config reload | `POST /v1/config/reload` | operator | publishes a new config generation cluster-wide |
+| Config validate | `POST /v1/config/validate` | operator | dry-run of the full load pipeline (parse → validate → resolve) of the body, or of the node's file when the body is empty: the report and what a reload would change; the resolved config is not returned; nothing is published. A node takes posted bodies at 1 per second after a burst of 5: past it, `429` with `Retry-After` |
+| Config reload | `POST /v1/config/reload` | operator | publishes a new config generation cluster-wide; a process-restart field is refused unless `?restart=accept`, and then applied at each node's next start (`restart_pending`) |
 | Config status | `GET /v1/config/status` | viewer | |
 | Cluster status | `GET /v1/cluster/status` | viewer | |
-| Pipelines status | `GET /v1/pipelines`, `GET /v1/pipelines/{id}` | viewer | `desired_state`, `actual_state`, positions, per-table status (halted tables with their reason) |
-| Request snapshot | `POST /v1/pipelines/{id}/snapshots` | operator | tables list (the per-table config keys), no globs; refused (`409`) unless the desired state is `RUNNING` and a node holds the lease, as a restart (`ERROR` included), or while an unfinished job holds one of the tables; `202` with the job's id. The job runs in a restarted worker. It upserts: a row deleted at the source before the job stays (drop or move the sink table first to start clean) |
+| Pipelines status | `GET /v1/pipelines`, `GET /v1/pipelines/{id}` | viewer | `desired_state`, `actual_state`, positions, per-table status (halted tables with their reason); the lease holder flagged `stale` when it isn't a member the node's gossip sees (gone, or an earlier process of a restarted node), `suspect` while gossip suspects it |
+| Request snapshot | `POST /v1/pipelines/{id}/snapshots` | operator | tables list (the per-table config keys), no globs; refused (`409`) unless the desired state is `RUNNING` and a node holds the lease, as a restart (`ERROR` included), or while an unfinished job holds one of the tables; `202` with the job's id. The job runs in a restarted worker. It upserts: a row deleted at the source before the job stays, unless the job is asked with `truncate: true`, which empties each sink table first. A pipeline keeps its 100 newest ended jobs |
 | Snapshot job status | `GET /v1/snapshots/{job_id}` | viewer | |
 | List snapshot jobs | `GET /v1/pipelines/{id}/snapshots` | viewer | persisted in the pipeline state location; survive restart and failover |
 | Pipeline pause | `POST /v1/pipelines/{id}/pause` | operator | `RUNNING → PAUSED`: flush to a transaction boundary and commit; source connection kept warm; lease kept |
```

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`, which includes the staleness tests of the generated reference
    (`xtask/tests/docs.rs`) and the OpenAPI snapshot: a task that changes `FIELD_DOCS`, the rule
    registry or a route runs `cargo xtask docs` (and, for a route,
    `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi` first) and commits what
    they rewrite.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`; the source crates depend on `core` and `config`, never on the pipeline.
  `testkit` is a dev-dependency only. No new crate.
- **State compatibility:** documents written by v0.1.0 still read. New fields are
  `#[serde(default)]`, and skipped when empty where a v0.1.0 reader would refuse them
  (`TableHalt.copy`, `SnapshotJobDoc.truncate`). `removals/` is a new prefix. No
  `SCHEMA_VERSION` bump.
- **Limits are constants** (Q-M12b-4), each `pub` and re-exported: `VALIDATE_BURST = 5`,
  `VALIDATE_PER_SECOND = 1`, `MAX_CONNECTIONS = 1024`, `KEPT_ENDED_JOBS = 100`,
  `MAX_KEY_DOCUMENT = 1 << 20`, `IDLE_READS = 4`, `SWEEP_INTERVAL = 30 s`.
- **Secrets stay secrets:** a tracing header's value is a `Secret`, never logged, never echoed by
  a rule's message, fingerprinted like the others.
- **Test gates:**
  - Plain `cargo test` stays hermetic.
  - The ITs need `DATA_AVALANCHE_IT=1` (`test-infra/up.sh`); Task 9's runs on PostgreSQL 18.
  - The `tls::` ITs read test-infra's `.tls`, so they run from the repository's checkout only;
    the planning's runs (a scratch workspace) skip them (`--skip tls::`), and Task 9 doesn't
    touch them.
  - The E2E suite (`--features e2e-hooks --test e2e`) runs on test-infra; Task 13's Jaeger
    scenario needs the `jaeger` service.
- **Public-API conventions:** public types and functions are re-exported at the crate root;
  errors are `thiserror` types (none new).
- Code blocks are the verified content, already `rustfmt`-formatted. Every block is a `git
  apply`-ready diff, one per file; a step's diffs apply in the order given, after the earlier
  steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt a person running a cluster through a change. Each is
pinned by a test in the task after the arrow.

1. **A pipeline removed while its owner is down, then added back under the same id.** The old
   slot and publication must be torn down once, by one node, before the new pipeline claims; the
   new one must not start on resources the teardown then drops → Task 3
   (`a_removal_no_node_removed_is_torn_down_where_the_ring_places_it`,
   `a_removal_held_by_a_gone_node_is_torn_down_once_its_lease_expires`), Task 4
   (`a_pipeline_added_after_the_removal_it_follows_waits_for_it`,
   `a_node_started_during_a_removal_waits_for_it`).
2. **A rolling restart after `?restart=accept`.** A node must not apply a restart-only value it
   didn't start with, and the cluster's own values must stay refused → Task 5
   (`a_restart_field_is_published_when_accepted_and_pending_until_the_next_start`,
   `a_generation_with_a_restart_field_is_adopted_with_it_pending`,
   `only_the_cluster_s_own_fields_are_fixed`, Azure's storage account among them).
3. **A `truncate` job that crashes mid-copy.** The table must not be emptied a second time once
   its copy started, and must be emptied again if the crash came before → Task 6
   (`a_job_s_truncate_empties_the_table_and_keeps_the_marker`,
   `a_truncating_job_drops_the_rows_deleted_meanwhile`).
4. **A snapshot read that waits on a lock.** The other tables' reads and the stream's catalog
   queries must go on → Task 9 (`a_waiting_chunk_read_holds_up_no_other_read`).
5. **A collector token rotated by reload.** The new header must reach the next export over
   `https`, and the old exporter be dropped → Task 12
   (`an_https_collector_takes_the_export_with_its_headers`).

## Decisions and rulings

The questions Oleksii answered while planning (roadmap §9.1):

| ID | Decision |
|---|---|
| Q-M12-8 | M12 grows to four plans: the source and sink gaps M12a's review recorded (DA-SRC-25, 31…36, DA-SNAP-22, DA-SINK-12) get a plan of their own, M12d, after M12b and before M12c's acceptance. DA-OPS-14 stays with M12c's load runs |
| Q-M12b-1 | DA-OBS-7, DA-OPS-12 and DA-API-13 are built in M12b, not closed with a pointer |
| Q-M12b-2 | DA-OPS-12: a reload accepts restart-only changes on request (`?restart=accept`), and each node's next start applies them |
| Q-M12b-3 | DA-API-13: a holder is stale by gossip membership (its node and incarnation not a member in the answering node's view), suspect while SWIM suspects it |
| Q-M12b-4 | The limits are constants, not config: validate 1/s after a burst of 5 (`429` with `Retry-After`); 1024 connections; a pipeline's newest 100 ended jobs kept; 1 MiB for a key document |

The rulings this plan makes (roadmap D-IMPL-25):

| ID | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M12b.1 | **Every node asks its seeds every 5 s**, `static` and `dns` alike, and announces only to the resolved addresses that aren't members of its view | A group with a peer never asked again, so groups formed apart stayed apart. An announce to a member is wasted, so members are left out; SWIM's push/pull merges the rest | Task 1 (`groups_that_each_have_a_peer_ask_their_seeds_again_and_merge`) |
| D-M12b.2 | **A holder is judged by the answering node's gossip** (Q-M12b-3): `stale` when its node and incarnation aren't a member (gone, or an earlier process), `suspect` when SWIM suspects it; a released lease is neither. `Member` carries the incarnation | Membership is what each node has without asking another, and no clock is compared. A restarted node is a new incarnation: its old lease reads stale until taken over | Task 2 (`a_holder_that_isnt_a_member_reads_as_stale`, `a_suspected_member_holds_its_lease_as_suspect`) |
| D-M12b.3 | **A removal is a tombstone in `globals.state`**, written by the adopting node before it applies the generation (a failed write refuses the adoption, retried at the next poll); `record` keeps one under way and replaces a done one. The tearing-down controller marks it done; tombstones are never deleted | Every node sees `globals.state`; a tombstone outlives the node that saw the removal. Marked, not deleted: one per id ever removed, and a record of what went | Task 3 (`a_removal_is_recorded_with_the_generation_that_last_had_it`) |
| D-M12b.4 | **A removal no controller saw is torn down by the node the ring places its id on**, swept every 30 s (`SWEEP_INTERVAL`): it resolves the pipeline from the tombstone's generation text in its own environment, claims the lease once free (an expired one included), tears down per `auto_cleanup`, releases, marks done | The ring gives one candidate per id without coordination; the lease makes the teardown single-writer as any run is | Task 3 (`a_removal_no_node_removed_is_torn_down_where_the_ring_places_it`, `a_removal_held_by_a_gone_node_is_torn_down_once_its_lease_expires`, `a_removal_is_taken_up_only_where_the_ring_places_it`) |
| D-M12b.5 | **A controller waits for the removals it follows before its first claim**: every pending tombstone of its id, or of a pipeline it `shares` a slot, publication, sink or state location with. An unreadable tombstone list is waited for; a tombstone that doesn't resolve is logged and not waited for. The reconciler's same-reload `Following` is removed | The wait must hold on every node and across reloads and starts; only the tombstone is visible everywhere. A tombstone no node can resolve can't be torn down here either, so waiting on it would hold the pipeline for good | Task 4 (`a_pipeline_waits_until_its_own_id_s_removal_is_done`, `a_removal_that_doesnt_resolve_or_share_isnt_waited_for`, `a_pipeline_added_after_the_removal_it_follows_waits_for_it`, `a_node_started_during_a_removal_waits_for_it`, `a_follower_removed_while_it_waits_ends`) |
| D-M12b.6 | **`?restart=accept` publishes restart-only changes; the node keeps what it started with** (Q-M12b-2). The coordinator lists the fields that differ from its start (`restart_pending`, per node); `FIXED_FIELDS` are refused even with it. Any other query on the reload is `400` | A running node can't rebind its ports or change its anchor; the next start reads the generation whole. The fixed fields locate or define the cluster itself | Task 5 (`a_restart_field_is_published_when_accepted_and_pending_until_the_next_start`, `a_generation_with_a_restart_field_is_adopted_with_it_pending`, `only_the_cluster_s_own_fields_are_fixed`) |
| D-M12b.7 | **A job's truncate is a snapshot change set with `truncate` and no rows**, committed per started table of a truncating job before the start records the table running | One commit path: fenced, retried and counted as any commit. Its order makes a crash before the record truncate again, and a crash after resume without truncating | Task 6 (`a_truncating_job_drops_the_rows_deleted_meanwhile`, `a_job_s_truncate_empties_the_table_and_keeps_the_marker`) |
| D-M12b.8 | **Ended jobs are pruned when a job ends** to the newest 100 by `finished_at` (Q-M12b-4); `VersionedStore::delete` is unconditional, for documents no one updates any more | The owner already polls the jobs; pruning there needs no new task. An unconditional delete is safe for an ended job, never for a lease or a pointer (the doc comment says so) | Task 7 (`pruning_keeps_the_newest_ended_jobs_and_every_unfinished_one`, `a_job_that_ends_prunes_the_oldest_ended_jobs`, `delete_removes_and_a_missing_object_is_no_error`) |
| D-M12b.9 | **A copy's halt is flagged, not cleared**: `TableHalt.copy`, kept out of the status's halted tables, the metric and the API | The job table already fails with the reason; the flag keeps the record without counting a table the pipeline doesn't stream | Task 7 (`a_copy_that_can_t_be_read_halts_alone`, and the API's halted-tables test with a copy's halt) |
| D-M12b.10 | **The reconciler keeps one entry per key**, under its latest writer | A row is kept only when the read saw the latest writer; older writers decide nothing. The bound is the distinct keys written during the wait | Task 8 (`a_key_written_again_is_logged_once_under_its_latest_writer`, `a_writer_keeps_the_keys_no_later_one_wrote`) |
| D-M12b.11 | **A chunk read takes its own connection**, at most 4 kept idle (`IDLE_READS`); the timeout starts once it has one; on timeout it cancels and drops its own | The catalog connection stays the stream's; V-038's budget (about three connections a snapshotting table) already allows it | Task 9 (`a_waiting_chunk_read_holds_up_no_other_read`) |
| D-M12b.12 | **The key fetch follows no redirect and reads at most 1 MiB**; a non-`https` key URL is a warning (V-125), not an error | A JWKS is a few kB; a redirect could move the trust to another host. A warning, as V-052, since a mesh may terminate TLS | Task 10 (`a_key_fetch_follows_no_redirect_and_reads_a_bounded_body`, V-125's cases) |
| D-M12b.13 | **At most 1024 connections a node**: a permit taken before the accept, held by the connection's stream; a connection that reads and writes nothing for 60 s (`IDLE_TIMEOUT`) is closed (amended in execution, approved 2026-11-05) | Waiting at the accept leaves the backlog to the kernel; a permit inside the stream frees on any close path. axum 0.8's server gives hyper no timer, so without the idle deadline 1024 silent sockets would hold every place for good | Task 10 (`past_the_cap_a_connection_waits_for_one_to_close`, `an_idle_connection_is_closed_and_gives_its_place_back`, `a_connection_in_use_outlives_the_idle_timeout`) |
| D-M12b.14 | **Posted validates share a node-wide bucket**: 5, refilled at 1 a second; `429` with `Retry-After`. The node's own file is unlimited | The guess is about the node's variables, whoever asks; the file holds no guess | Task 11 (`posted_validates_past_the_burst_wait`) |
| D-M12b.15 | **The OpenAPI document is checked by `boon` against the vendored OAS 3.1 schema** (`schema-2022-10-07.json`, the plain schema) | Offline and exact-pinned; the dialect schema's deep Schema-object checks need a meta-schema resolver for little gain | Task 11 (`the_document_is_valid_oas_3_1`) |
| D-M12b.16 | **Tracing headers are secrets keyed into the exporter** (the endpoint and the headers in force); V-126 checks their syntax and never echoes a value; the environment's `OTEL_EXPORTER_OTLP_HEADERS` wins for a name it sets | A rotated token must reach the next export without a restart; the exporter's own merge order (opentelemetry-otlp 0.33.1) puts the environment last | Task 12 (`an_https_collector_takes_the_export_with_its_headers`, `tracing_headers_resolve_as_secrets`, V-126's cases) |
| D-M12b.17 | **The lag keeps an earlier session's start** until the new session's frontier reaches the kept one; a worker's first status or a rise in `reconnects` starts a session | Both windows read 0 because the new session's first durable change hid an older pending one | Task 13 (`the_kept_lag_start_waits_for_the_next_session_to_read_as_far`) |
| D-M12b.18 | **The audit is enforced here, not in CI**: the lock on the patched releases, three advisories triaged in `.cargo/audit.toml` with their reason and their lift | No release of deltalake's `object_store` 0.13 takes quick-xml's fix; the CI step stays non-blocking (roadmap §4 M12) | Task 14 (`cargo audit` clean) |
| D-M12b.19 | **The dev profile: workspace crates at `debug = "line-tables-only"`; dependencies stay at `opt-level = 0`.** CI unchanged; deltalake stays `=1.0.0` | Measured (Task 15): line tables keep the build times and save 1 GB of target, which CI's disk needs; `opt-level = 1` for dependencies cuts the heaviest suites from 60 s to 42 s but takes a clean build from 588 s to 1028 s, paid at every dependency bump and cache miss. CI's critical path is the k3s image's release build, which a shared cargo cache doesn't reach | Task 15 (the measurements) |

**Measured while planning** (2026-11-04, recorded in the tasks):

- Every task was built in a scratch workspace on this branch's base, test first. Each task's test
  stage was committed on its own and run (Step 2's output), then its implementation (Step 4's).
- **The replay.** The plan's diff blocks, applied in order with `git apply` to the plan commit (this
  branch's base, the roadmap, README and tracker §56 edits on it) and the spec patch, with Task
  11's schema fetched, give a tree identical to the last stage: 149 blocks, no fuzz. Each stage
  ran the suites of every crate its task touches with `DATA_AVALANCHE_IT=1` (PostgreSQL 18), and
  Tasks 2, 5 and 13 their E2E scenarios.
- **The last stage's full run:** `cargo fmt`, both clippy runs and 1434 workspace tests pass; with
  `DATA_AVALANCHE_IT=1`, 1418 (`tls::` skipped); the E2E suite, 48 scenarios, on PostgreSQL 18
  (M12b changes no MySQL path; the PR's `e2e-mysql` jobs run the three MySQL targets); `cargo
  xtask docs` leaves the references unchanged; the site builds with its link validation; `cargo
  audit` clean.
- **`cargo audit`** on the base: 17 vulnerabilities, 23 warnings (rustls 0.23.45, rustls-webpki,
  webpki-roots, quick-xml 0.39, the zstd crates, ctutils; zstd-safe and tokio's `AsyncFd`
  unsound, proc-macro-error2 unmaintained, yoke-derive yanked). After Task 14's `cargo update`,
  three, triaged: no `object_store` 0.13 release takes quick-xml 0.41, and `deltalake-core` 1.1.3
  still requires `object_store ^0.13.2` (crates.io, 2026-11-04).
- **The compile time** (Task 15): a clean `cargo build --workspace --all-targets` takes 588 s on 12 cores;
  `aws-lc-sys`'s build script 110 s, `datafusion` 89 s, `deltalake-core` 74 s; the workspace's own
  units 925 of 3046 unit-seconds. A rebuild after touching `core` or `pipeline`, 37–40 s.
  `line-tables-only` for the workspace crates: 576 s, 40 s, 1 GB less (taken); `opt-level = 1`
  for the dependencies: 1028 s clean, the `sink-delta` and `pipeline` suites 60 s → 42 s (not
  taken).
- **opentelemetry-otlp 0.33.1's headers:** `with_headers` sets the configured map, then the
  exporter extends it with `OTEL_EXPORTER_OTLP_HEADERS` (`add_header_from_string`), so the
  environment wins for a name both set. Recorded in the docs, not changed.
- **The OAS 3.1 schema** fetched from `https://spec.openapis.org/oas/3.1/schema/2022-10-07` is
  byte-identical to the vendored file (sha256 `da01ba28…314ed0`).
- **Found while building, fixed before presenting** (each in its task, with a test):
  - The site's link validation caught a link to the renamed "Keys a reload refuses" section
    (`configuration/overview.md`, whose class table now names `?restart=accept`) (Task 16).
  - Task 2's `holder_stale` in `GET /v1/cluster/status` broke the E2E
    `any_nodes_api_drives_the_holder`, which pinned the row's old shape (found by Task 5's E2E
    run). Task 2 updates it, and its Step 4 runs it.
  - `globals.azure.account_name` names the storage account that holds `globals.state` on Azure,
    so it joins the fields `?restart=accept` still refuses (Task 5, a case in
    `only_the_cluster_s_own_fields_are_fixed`).
  - The spec draft first said a configured tracing header wins over the environment's; the
    exporter's merge order says otherwise: the spec row leaves the environment out, and the
    operator page says the environment wins (Task 16).

## File Structure

| File | Task | Change |
|---|---|---|
| `.cargo/audit.toml` | 14 | create |
| `Cargo.lock` | 3, 11, 12, 14 | modify |
| `Cargo.toml` | 11, 15 | modify |
| `crates/data-avalanche-api/Cargo.toml` | 11 | modify |
| `crates/data-avalanche-api/oas/schema-2022-10-07.json` | 11 | create |
| `crates/data-avalanche-api/openapi.json` | 5, 11 | modify |
| `crates/data-avalanche-api/src/auth.rs` | 10 | modify |
| `crates/data-avalanche-api/src/cluster.rs` | 2 | modify |
| `crates/data-avalanche-api/src/config.rs` | 5, 11 | modify |
| `crates/data-avalanche-api/src/lib.rs` | 5, 11 | modify |
| `crates/data-avalanche-api/src/limit.rs` | 11 | create |
| `crates/data-avalanche-api/src/openapi.rs` | 11 | modify |
| `crates/data-avalanche-api/src/pipelines.rs` | 2, 7 | modify |
| `crates/data-avalanche-api/src/server.rs` | 10 | modify |
| `crates/data-avalanche-api/src/snapshots.rs` | 6, 7 | modify |
| `crates/data-avalanche-api/src/test_support.rs` | 3, 11 | modify |
| `crates/data-avalanche-cluster/src/cluster_tests.rs` | 5 | modify |
| `crates/data-avalanche-cluster/src/controller.rs` | 3, 4, 13 | modify |
| `crates/data-avalanche-cluster/src/coordinator.rs` | 3, 5, 7 | modify |
| `crates/data-avalanche-cluster/src/discovery.rs` | 1 | modify |
| `crates/data-avalanche-cluster/src/gossip.rs` | 2 | modify |
| `crates/data-avalanche-cluster/src/jobs.rs` | 7 | modify |
| `crates/data-avalanche-cluster/src/lib.rs` | 3, 5 | modify |
| `crates/data-avalanche-cluster/src/reconciler.rs` | 3, 4 | modify |
| `crates/data-avalanche-cluster/src/test_support.rs` | 2, 3, 7 | modify |
| `crates/data-avalanche-config/src/build.rs` | 12 | modify |
| `crates/data-avalanche-config/src/diff.rs` | 5 | modify |
| `crates/data-avalanche-config/src/field_docs.rs` | 12 | modify |
| `crates/data-avalanche-config/src/lib.rs` | 5 | modify |
| `crates/data-avalanche-config/src/raw.rs` | 12 | modify |
| `crates/data-avalanche-config/src/registry.rs` | 10, 12 | modify |
| `crates/data-avalanche-config/src/resolved.rs` | 12 | modify |
| `crates/data-avalanche-config/src/rules/auth.rs` | 10 | modify |
| `crates/data-avalanche-config/src/rules/domain.rs` | 12 | modify |
| `crates/data-avalanche-config/src/walk.rs` | 12 | modify |
| `crates/data-avalanche-config/tests/diff.rs` | 5 | modify |
| `crates/data-avalanche-config/tests/load.rs` | 12 | modify |
| `crates/data-avalanche-config/tests/rules/auth.rs` | 10 | modify |
| `crates/data-avalanche-config/tests/rules/domain.rs` | 12 | modify |
| `crates/data-avalanche-core/src/change_set.rs` | 6 | modify |
| `crates/data-avalanche-pipeline/src/jobs.rs` | 6, 7 | modify |
| `crates/data-avalanche-pipeline/src/reconcile.rs` | 8 | modify |
| `crates/data-avalanche-pipeline/src/session.rs` | 7 | modify |
| `crates/data-avalanche-pipeline/src/session/snapshot.rs` | 8 | modify |
| `crates/data-avalanche-pipeline/src/test_support.rs` | 7 | modify |
| `crates/data-avalanche-pipeline/src/worker.rs` | 6, 7 | modify |
| `crates/data-avalanche-pipeline/src/writer.rs` | 7, 13 | modify |
| `crates/data-avalanche-sink-delta/src/sink.rs` | 6 | modify |
| `crates/data-avalanche-source-pg/src/connect.rs` | 9 | modify |
| `crates/data-avalanche-source-pg/src/lib.rs` | 9 | modify |
| `crates/data-avalanche-source-pg/src/snapshot.rs` | 9 | modify |
| `crates/data-avalanche-source-pg/tests/pg/snapshot.rs` | 9 | modify |
| `crates/data-avalanche-state/Cargo.toml` | 3 | modify |
| `crates/data-avalanche-state/src/generation.rs` | 3 | modify |
| `crates/data-avalanche-state/src/lib.rs` | 3, 7 | modify |
| `crates/data-avalanche-state/src/removal.rs` | 3 | create |
| `crates/data-avalanche-state/src/snapshot.rs` | 6, 7 | modify |
| `crates/data-avalanche-state/src/tables.rs` | 7 | modify |
| `crates/data-avalanche-state/src/test_support.rs` | 7 | modify |
| `crates/data-avalanche-storage/src/store.rs` | 7 | modify |
| `crates/data-avalanche-storage/tests/versioned_store.rs` | 7 | modify |
| `crates/data-avalanche-telemetry/Cargo.toml` | 12 | modify |
| `crates/data-avalanche-telemetry/src/traces.rs` | 12 | modify |
| `crates/data-avalanche-telemetry/tests/traces.rs` | 12 | modify |
| `crates/data-avalanche-testkit/src/fake_source.rs` | 6 | modify |
| `crates/data-avalanche-testkit/src/lib.rs` | 12 | modify |
| `crates/data-avalanche-testkit/src/otlp.rs` | 12 | modify |
| `crates/data-avalanche/src/logging.rs` | 12 | modify |
| `crates/data-avalanche/src/run.rs` | 3, 11 | modify |
| `crates/data-avalanche/tests/e2e/cluster.rs` | 2, 5 | modify |
| `crates/data-avalanche/tests/e2e/harness.rs` | 13 | modify |
| `crates/data-avalanche/tests/e2e/observability.rs` | 13 | modify |
| `crates/data-avalanche/tests/e2e/reload.rs` | 5 | modify |
| `doc/analysis/CDC_Design_Gap_Analysis_v1.md` | 16 | modify |
| `doc/research/API_RBAC_and_Versioning.md` | 16 | modify |
| `doc/research/Clustering_Architecture_Research.md` | 16 | modify |
| `docs/public/openapi.json` | 5, 11 | modify |
| `docs/src/content/docs/architecture/delivery.md` | 16 | modify |
| `docs/src/content/docs/architecture/snapshots.md` | 16 | modify |
| `docs/src/content/docs/configuration/cluster.md` | 16 | modify |
| `docs/src/content/docs/configuration/overview.md` | 16 | modify |
| `docs/src/content/docs/deployment/kubernetes.md` | 16 | modify |
| `docs/src/content/docs/deployment/observability.md` | 16 | modify |
| `docs/src/content/docs/reference/api.md` | 5, 11 | modify |
| `docs/src/content/docs/reference/configuration.md` | 12 | modify |
| `docs/src/content/docs/reference/validation-rules.md` | 10, 12 | modify |
| `docs/src/content/docs/runbooks/config-changes.md` | 16 | modify |
| `docs/src/content/docs/runbooks/failover.md` | 16 | modify |
| `docs/src/content/docs/runbooks/halted-table.md` | 16 | modify |
| `docs/src/content/docs/runbooks/position-lost.mdx` | 16 | modify |
| `docs/src/content/docs/runbooks/snapshot-jobs.md` | 16 | modify |

---

### Task 1: Groups that formed apart merge (`cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/discovery.rs`
- Test: `crates/data-avalanche-cluster/src/discovery.rs`

**Gaps:** DA-CLUS-15.

**Interfaces:**
- Consumes: `discover(seeds, me, members, announcer)`, `Seeds::resolve`, `Members.nodes[*].addr` (M8a).
- Produces: `discover` resolves the seeds every `REDISCOVER` (5 s), whatever the peer count, and announces to the addresses that aren't members of the node's view.

A node asked its seeds again only while it had no peer. Pods started together that each
found some, not all, of the others settled in groups that never merged, since each group had a
peer. Every round now resolves the seeds and announces to those the membership doesn't hold: a
member's address costs nothing, and SWIM's push/pull merges the groups once one announce lands.
The test cuts the in-memory hub between two pairs, lets each form a group of two, then heals
the link: one group of four within `REDISCOVER` and 2 s.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-cluster/src/discovery.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/discovery.rs b/crates/data-avalanche-cluster/src/discovery.rs
index 7209444..ef47a46 100644
--- a/crates/data-avalanche-cluster/src/discovery.rs
+++ b/crates/data-avalanche-cluster/src/discovery.rs
@@ -231,6 +231,60 @@ mod tests {
         );
     }
 
+    /// DA-CLUS-15: nodes that settled in two groups, each with a peer, ask their seeds again and
+    /// merge once they can reach each other.
+    #[tokio::test(start_paused = true)]
+    async fn groups_that_each_have_a_peer_ask_their_seeds_again_and_merge() {
+        use crate::test_support::{Hub, at, gossip_settings};
+        let hub = Hub::new();
+        let start = |name: &str, n: u8| {
+            let (port, inbox) = hub.port(at(n));
+            let me = crate::NodeIdentity {
+                node_id: name.into(),
+                incarnation: 1,
+                rev: 0,
+                addr: at(n),
+                meta: crate::NodeMeta {
+                    phase: crate::Phase::Active,
+                    config_generation: 1,
+                    build: "1.0.0".into(),
+                },
+            };
+            let key = crate::GossipKey::derive(&[7; 32]);
+            crate::Gossip::start(me, key, gossip_settings(), std::sync::Arc::new(port), inbox)
+        };
+        let seeds = Seeds::Static(
+            (1..=4)
+                .map(|n| HostPort {
+                    host: at(n).ip().to_string(),
+                    port: at(n).port(),
+                })
+                .collect(),
+        );
+        hub.cut(&[at(1), at(2)], &[at(3), at(4)]);
+        let nodes = [start("a", 1), start("b", 2), start("c", 3), start("d", 4)];
+        let _discovery: Vec<_> = nodes
+            .iter()
+            .zip(1..)
+            .map(|(g, n)| discover(seeds.clone(), vec![at(n)], g.announcer(), g.members()))
+            .collect();
+        eventually("two groups of two", || {
+            nodes.iter().all(|g| g.members().borrow().peers() == 1)
+        })
+        .await;
+        hub.heal();
+        let t = tokio::time::Instant::now();
+        eventually("one group of four", || {
+            nodes.iter().all(|g| g.members().borrow().peers() == 3)
+        })
+        .await;
+        assert!(
+            t.elapsed() <= REDISCOVER + Duration::from_secs(2),
+            "{:?}",
+            t.elapsed()
+        );
+    }
+
     /// A node that started before its seed asks again while alone, and joins once the seed is
     /// up.
     #[tokio::test(start_paused = true)]
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-cluster --no-fail-fast
```

Expected: FAIL, the new tests:
  - `discovery::tests::groups_that_each_have_a_peer_ask_their_seeds_again_and_merge`

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/discovery.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/discovery.rs b/crates/data-avalanche-cluster/src/discovery.rs
index ef47a46..80061e0 100644
--- a/crates/data-avalanche-cluster/src/discovery.rs
+++ b/crates/data-avalanche-cluster/src/discovery.rs
@@ -1,8 +1,10 @@
 //! How a node finds its peers (Clustering §3.5, §10.2, D-4) and the address it gives them.
 //! `static` asks `memberlist.join_members`; `dns` asks every address `discovery_dns_name`
-//! resolves to (a Kubernetes headless Service), at `advertise_port`. Seeds are asked at start and
-//! again every [`REDISCOVER`] while the node has no peer, so a node that lost them all, or
-//! started before them, finds its way back; once joined, SWIM carries the membership.
+//! resolves to (a Kubernetes headless Service), at `advertise_port`. Seeds are resolved at start
+//! and again every [`REDISCOVER`], and asked when they aren't members: a node that lost its peers,
+//! or started before them, finds its way back, and groups that formed apart (pods started together
+//! that each resolved some of the others, DA-CLUS-15) merge. SWIM carries the membership between
+//! the members.
 //!
 //! A seed is named by the address its peer advertises: that peer's `advertise_addr` at
 //! `advertise_port`. foca accepts an announce only when it is addressed to the receiver's own
@@ -20,7 +22,7 @@ use tokio::task::JoinHandle;
 use crate::gossip::Announcer;
 use crate::{ClusterError, Members};
 
-/// How often a node without peers asks its seeds again.
+/// How often a node resolves its seeds again, and asks those that aren't members.
 pub const REDISCOVER: Duration = Duration::from_secs(5);
 
 /// Where a node's seeds come from.
@@ -69,7 +71,7 @@ impl Seeds {
     }
 }
 
-/// Asks the seeds at once, then every [`REDISCOVER`] while the node has no peer.
+/// Asks the seeds at once, then every [`REDISCOVER`] the seeds that aren't members.
 pub fn discover(
     seeds: Seeds,
     me: Vec<SocketAddr>,
@@ -78,12 +80,14 @@ pub fn discover(
 ) -> JoinHandle<()> {
     tokio::spawn(async move {
         loop {
-            if members.borrow().peers() == 0 {
-                let addrs = seeds.resolve(&me).await;
-                if !addrs.is_empty() {
-                    tracing::debug!(?addrs, "asking the gossip seeds");
-                    announcer.announce(addrs);
-                }
+            let mut addrs = seeds.resolve(&me).await;
+            {
+                let view = members.borrow();
+                addrs.retain(|a| !view.nodes.values().any(|m| m.addr == *a));
+            }
+            if !addrs.is_empty() {
+                tracing::debug!(?addrs, "asking the gossip seeds that aren't members");
+                announcer.announce(addrs);
             }
             tokio::time::sleep(REDISCOVER).await;
         }
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-cluster --no-fail-fast
```

Expected: PASS, 145 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/discovery.rs
git commit -m "fix(cluster): every node asks the seeds that aren't members, so groups formed apart merge (DA-CLUS-15)"
```

---

### Task 2: A gone holder reads as stale (`cluster`, `api`)

**Files:**
- Modify: `crates/data-avalanche-api/src/cluster.rs`, `crates/data-avalanche-api/src/pipelines.rs`, `crates/data-avalanche-cluster/src/gossip.rs`, `crates/data-avalanche-cluster/src/test_support.rs`
- Test: `crates/data-avalanche-api/src/cluster.rs`, `crates/data-avalanche-api/src/pipelines.rs`, `crates/data-avalanche/tests/e2e/cluster.rs`

**Gaps:** DA-API-13.

**Interfaces:**
- Consumes: `Members`, `Member { addr, meta, suspect }` (M8a); `LeaseDoc.holder_node_id`, `holder_incarnation` (M2).
- Produces:
  - `Member.incarnation: u64`, set from the gossip identity;
  - in `api`: `enum Liveness { Live, Suspect, Stale }` and `fn liveness(members: &Members, lease: &LeaseDoc) -> Liveness`;
  - `GET /v1/pipelines/{id}`'s `holder.stale` and `holder.suspect`; `holder_stale` in `GET /v1/pipelines` and `GET /v1/cluster/status` (null when no lease is held).

A holder that crashed without releasing its lease read as live until another node took
over. The answering node now judges the holder by its own gossip view (Q-M12b-3): the lease names
a node and its process (`holder_incarnation`); if that pair isn't a member, the holder is gone,
or is an earlier process of a restarted node, and reads `stale`. A member SWIM suspects reads
`suspect`. No clock is compared, so skew between nodes can't flip it. A released lease is
neither.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/cluster.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/cluster.rs b/crates/data-avalanche-api/src/cluster.rs
index 3f4a136..80d725a 100644
--- a/crates/data-avalanche-api/src/cluster.rs
+++ b/crates/data-avalanche-api/src/cluster.rs
@@ -190,7 +190,7 @@ mod tests {
         assert_eq!(v["members"][0]["config_generation"], 1);
         assert_eq!(
             v["pipelines"],
-            json!([{ "id": "orders", "holder": null, "owner_epoch": null }])
+            json!([{ "id": "orders", "holder": null, "owner_epoch": null, "holder_stale": null }])
         );
         let holder = Holder {
             node_id: "node-b".into(),
@@ -203,7 +203,7 @@ mod tests {
         let v = json_of(addr, "GET", "/v1/cluster/status").await.1;
         assert_eq!(
             v["pipelines"],
-            json!([{ "id": "orders", "holder": "node-b", "owner_epoch": 1 }])
+            json!([{ "id": "orders", "holder": "node-b", "owner_epoch": 1, "holder_stale": true }])
         );
     }
 
```

`crates/data-avalanche-api/src/pipelines.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index 39e844c..a2d54df 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -372,9 +372,17 @@ mod tests {
     }
 
     async fn held_by(lone: &Lone, node: &str) -> data_avalanche_state::Versioned<LeaseDoc> {
+        held_by_process(lone, node, 1).await
+    }
+
+    async fn held_by_process(
+        lone: &Lone,
+        node: &str,
+        incarnation: u64,
+    ) -> data_avalanche_state::Versioned<LeaseDoc> {
         let holder = Holder {
             node_id: node.into(),
-            incarnation: 1,
+            incarnation,
         };
         match lone
             .leases
@@ -490,6 +498,73 @@ mod tests {
         assert_eq!(v["holder"]["released"], true);
     }
 
+    /// DA-API-13: a holder that isn't a member in this node's view, its node gone or another
+    /// process of it running, reads as stale; this node's own process doesn't. The lone node is
+    /// `node-a`, incarnation 1.
+    #[tokio::test]
+    async fn a_holder_that_isnt_a_member_reads_as_stale() {
+        let (addr, lone, _docs, _dir) = served().await;
+        let stale = async || {
+            let one = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
+            let list = json_of(addr, "GET", "/v1/pipelines").await.1;
+            let cluster = json_of(addr, "GET", "/v1/cluster/status").await.1;
+            assert_eq!(one["holder"]["suspect"], false, "{one}");
+            assert_eq!(list["pipelines"][0]["holder_stale"], one["holder"]["stale"]);
+            assert_eq!(
+                cluster["pipelines"][0]["holder_stale"],
+                one["holder"]["stale"]
+            );
+            one["holder"]["stale"].clone()
+        };
+        let lease = held_by_process(&lone, "node-b", 1).await;
+        assert_eq!(stale().await, true, "a node that isn't a member");
+        lone.leases.release(&lease, Utc::now()).await.unwrap();
+        let v = json_of(addr, "GET", "/v1/pipelines").await.1;
+        assert_eq!(v["pipelines"][0]["holder_stale"], Value::Null, "{v}");
+        let lease = held_by_process(&lone, "node-a", 2).await;
+        assert_eq!(stale().await, true, "another process of this node");
+        lone.leases.release(&lease, Utc::now()).await.unwrap();
+        held_by_process(&lone, "node-a", 1).await;
+        assert_eq!(stale().await, false, "this node's own process");
+    }
+
+    #[test]
+    fn a_suspected_member_holds_its_lease_as_suspect() {
+        let member = |incarnation, suspect| data_avalanche_cluster::Member {
+            addr: "127.0.0.1:7946".parse().unwrap(),
+            incarnation,
+            meta: data_avalanche_cluster::NodeMeta {
+                phase: data_avalanche_cluster::Phase::Active,
+                config_generation: 1,
+                build: "1.0.0".into(),
+            },
+            suspect,
+        };
+        let members = |m| Members {
+            me: "node-a".into(),
+            nodes: [("node-b".to_owned(), m)].into(),
+        };
+        let lease = LeaseDoc {
+            pipeline_id: "orders".into(),
+            holder_node_id: "node-b".into(),
+            holder_incarnation: 7,
+            owner_epoch: 1,
+            acquired_at: Utc::now(),
+            renewed_at: Utc::now(),
+            lease_duration_ms: 15_000,
+            released: false,
+        };
+        assert_eq!(liveness(&members(member(7, false)), &lease), Liveness::Live);
+        assert_eq!(
+            liveness(&members(member(7, true)), &lease),
+            Liveness::Suspect
+        );
+        assert_eq!(
+            liveness(&members(member(8, false)), &lease),
+            Liveness::Stale
+        );
+    }
+
     #[tokio::test]
     async fn a_holder_s_status_of_an_earlier_term_is_not_this_term_s() {
         let (addr, lone, docs, _dir) = served().await;
@@ -523,7 +598,7 @@ mod tests {
             v,
             json!({ "pipelines": [{
                 "id": "orders", "name": "orders", "desired_state": "running",
-                "actual_state": "paused", "holder": "node-b",
+                "actual_state": "paused", "holder": "node-b", "holder_stale": true,
             }] })
         );
     }
```

`crates/data-avalanche/tests/e2e/cluster.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/cluster.rs b/crates/data-avalanche/tests/e2e/cluster.rs
index a35cb14..f73a55c 100644
--- a/crates/data-avalanche/tests/e2e/cluster.rs
+++ b/crates/data-avalanche/tests/e2e/cluster.rs
@@ -605,9 +605,10 @@ async fn any_nodes_api_drives_the_holder() {
         .iter()
         .find(|p| p["id"] == json!(id))
         .cloned();
+    // A live holder isn't stale (DA-API-13).
     assert_eq!(
         row,
-        Some(json!({ "id": id, "holder": holder, "owner_epoch": epoch }))
+        Some(json!({ "id": id, "holder": holder, "owner_epoch": epoch, "holder_stale": false }))
     );
     c.landed().await;
     c.stop().await;
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-api -p data-avalanche-cluster --no-fail-fast
cargo test -p data-avalanche --features e2e-hooks --test e2e --no-fail-fast -- --test-threads=1 cluster::any_nodes_api_drives_the_holder
```

Expected: FAIL to compile:
  - `error[E0422]: cannot find struct, variant or union type `Members` in this scope (crates/data-avalanche-api/src/pipelines.rs)`
  - `error[E0560]: struct `Member` has no field named `incarnation` (crates/data-avalanche-api/src/pipelines.rs)`
  - `error[E0425]: cannot find function `liveness` in this scope (crates/data-avalanche-api/src/pipelines.rs)`
  - `error[E0433]: cannot find type `Liveness` in this scope (crates/data-avalanche-api/src/pipelines.rs)`

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/cluster.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/cluster.rs b/crates/data-avalanche-api/src/cluster.rs
index 80d725a..a955239 100644
--- a/crates/data-avalanche-api/src/cluster.rs
+++ b/crates/data-avalanche-api/src/cluster.rs
@@ -12,6 +12,7 @@ use data_avalanche_cluster::Phase;
 use serde_json::{Value, json};
 use tokio::sync::watch;
 
+use crate::pipelines::{Liveness, liveness};
 use crate::{Api, Principal, body, starting};
 
 /// What the API asked of the node, which the binary carries out; each includes the ones before.
@@ -97,6 +98,9 @@ pub(crate) async fn status(State(api): State<Api>) -> Response {
             "id": id,
             "holder": lease.as_ref().map(|l| l.holder_node_id.clone()),
             "owner_epoch": lease.as_ref().map(|l| l.owner_epoch),
+            "holder_stale": lease
+                .as_ref()
+                .map(|l| liveness(&members, l) == Liveness::Stale),
         }));
     }
     body(
```

`crates/data-avalanche-api/src/pipelines.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index a2d54df..1ad5b3d 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -8,6 +8,7 @@ use axum::extract::{Path, State};
 use axum::http::StatusCode;
 use axum::response::Response;
 use chrono::Utc;
+use data_avalanche_cluster::Members;
 use data_avalanche_config::Pipeline;
 use data_avalanche_core::SourcePosition;
 use data_avalanche_state::{
@@ -102,14 +103,42 @@ pub(crate) async fn read(
     })
 }
 
-fn summary(p: &Pipeline, r: &Read) -> Value {
+/// A lease holder as this node's gossip sees it (DA-API-13).
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub(crate) enum Liveness {
+    /// The holder's node, in the same process (its incarnation), is a member.
+    Live,
+    /// A member that SWIM suspects of having failed.
+    Suspect,
+    /// Not a member: the node is gone, or another process of it runs. Its lease reads as held
+    /// until it expires and another node takes it over.
+    Stale,
+}
+
+/// How `lease`'s holder stands in `members`, judged by membership, not by clocks.
+pub(crate) fn liveness(members: &Members, lease: &LeaseDoc) -> Liveness {
+    match members.nodes.get(&lease.holder_node_id) {
+        Some(m) if m.incarnation == lease.holder_incarnation => {
+            if m.suspect {
+                Liveness::Suspect
+            } else {
+                Liveness::Live
+            }
+        }
+        _ => Liveness::Stale,
+    }
+}
+
+fn summary(p: &Pipeline, r: &Read, members: &Members) -> Value {
     let (actual, _) = actual(r.lease.as_ref(), r.status.as_ref());
+    let held = r.lease.as_ref().filter(|l| !l.released);
     json!({
         "id": p.id,
         "name": p.name,
         "desired_state": desired_name(r.desired),
         "actual_state": actual,
-        "holder": r.lease.as_ref().filter(|l| !l.released).map(|l| l.holder_node_id.clone()),
+        "holder": held.map(|l| l.holder_node_id.clone()),
+        "holder_stale": held.map(|l| liveness(members, l) == Liveness::Stale),
     })
 }
 
@@ -121,6 +150,7 @@ pub(crate) async fn list(State(api): State<Api>) -> Response {
     let config = node.coordinator.config().await;
     let mut pipelines: Vec<&Pipeline> = config.pipelines.values().collect();
     pipelines.sort_by(|a, b| a.id.cmp(&b.id));
+    let members = node.members.borrow().clone();
     let mut out = vec![];
     for p in pipelines {
         let docs = match (node.docs)(p) {
@@ -133,7 +163,7 @@ pub(crate) async fn list(State(api): State<Api>) -> Response {
             }
         };
         match read(node, p, &docs).await {
-            Ok(r) => out.push(summary(p, &r)),
+            Ok(r) => out.push(summary(p, &r, &members)),
             Err(e) => return unavailable(&e),
         }
     }
@@ -167,7 +197,8 @@ pub(crate) async fn get(State(api): State<Api>, Path(id): Path<String>) -> Respo
         Err(e) => return unavailable(&e),
     };
     let (actual, status) = actual(r.lease.as_ref(), r.status.as_ref());
-    let mut v = summary(&p, &r);
+    let members = node.members.borrow().clone();
+    let mut v = summary(&p, &r, &members);
     v["source"] = json!({
         "type": p.source.kind.as_str(),
         "host": p.source.host,
@@ -182,12 +213,17 @@ pub(crate) async fn get(State(api): State<Api>, Path(id): Path<String>) -> Respo
     v["degraded"] = json!(status.and_then(|s| s.degraded.clone()));
     v["status_written_at"] = json!(status.map(|s| s.written_at));
     v["holder"] = match &r.lease {
-        Some(l) => json!({
-            "node": l.holder_node_id,
-            "owner_epoch": l.owner_epoch,
-            "renewed_at": l.renewed_at,
-            "released": l.released,
-        }),
+        Some(l) => {
+            let live = liveness(&members, l);
+            json!({
+                "node": l.holder_node_id,
+                "owner_epoch": l.owner_epoch,
+                "renewed_at": l.renewed_at,
+                "released": l.released,
+                "stale": !l.released && live == Liveness::Stale,
+                "suspect": !l.released && live == Liveness::Suspect,
+            })
+        }
         None => Value::Null,
     };
     v["position"] = json!(position.map(|p| p.to_string()));
```

`crates/data-avalanche-cluster/src/gossip.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index f88d527..dee4777 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -88,6 +88,8 @@ impl foca::Identity for NodeIdentity {
 #[derive(Debug, Clone, PartialEq, Eq)]
 pub struct Member {
     pub addr: SocketAddr,
+    /// The member's process (its start in unix milliseconds): a lease's holder names it too.
+    pub incarnation: u64,
     pub meta: NodeMeta,
     /// Suspected of having failed; still a member (D-11: suspects stay on the ring).
     pub suspect: bool,
@@ -138,6 +140,7 @@ fn members_of<'a>(
         .map(|(name, (id, s))| {
             let member = Member {
                 addr: id.addr,
+                incarnation: id.incarnation,
                 meta: id.meta.clone(),
                 suspect: s == State::Suspect,
             };
@@ -146,6 +149,7 @@ fn members_of<'a>(
         .collect();
     let mine = Member {
         addr: me.addr,
+        incarnation: me.incarnation,
         meta: me.meta.clone(),
         suspect: false,
     };
```

`crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index 2e16c5a..e163f6c 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -520,6 +520,7 @@ pub(crate) fn members(me: &str, nodes: &[(&str, crate::Phase, bool)]) -> crate::
         .map(|((name, phase, suspect), n)| {
             let member = crate::Member {
                 addr: at(n),
+                incarnation: 1,
                 meta: crate::NodeMeta {
                     phase: *phase,
                     config_generation: 1,
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-api -p data-avalanche-cluster --no-fail-fast
cargo test -p data-avalanche --features e2e-hooks --test e2e --no-fail-fast -- --test-threads=1 cluster::any_nodes_api_drives_the_holder
```

Expected: PASS, 195 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/cluster.rs \
  crates/data-avalanche-api/src/pipelines.rs \
  crates/data-avalanche-cluster/src/gossip.rs \
  crates/data-avalanche-cluster/src/test_support.rs \
  crates/data-avalanche/tests/e2e/cluster.rs
git commit -m "feat(api): a lease holder that isn't a member reads as stale, a suspected one as suspect (DA-API-13)"
```

---

### Task 3: The removal tombstone (`state`, `cluster`, `api`, the binary)

**Files:**
- Create: `crates/data-avalanche-state/src/removal.rs`
- Modify: `Cargo.lock`, `crates/data-avalanche-api/src/test_support.rs`, `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-cluster/src/coordinator.rs`, `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-cluster/src/reconciler.rs`, `crates/data-avalanche-cluster/src/test_support.rs`, `crates/data-avalanche-state/Cargo.toml`, `crates/data-avalanche-state/src/generation.rs`, `crates/data-avalanche-state/src/lib.rs`, `crates/data-avalanche/src/run.rs`
- Test: `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-cluster/src/coordinator.rs`, `crates/data-avalanche-cluster/src/reconciler.rs`

**Gaps:** DA-OPS-9.

**Interfaces:**
- Consumes: `ConfigGenerations` (M8b); `Coordinator::adopt`, the reconciler's `Loop::reload` and `Update::Remove`, `PipelineController::remove` (M8a/M8b); `Ring::owner`.
- Produces:
  - `state`: `REMOVALS_PREFIX`, `removal_key(id)`, `RemovalDoc { pipeline_id, generation, hash, removed_at, done_at }` with `is_done()`, and `Removals::{new, record, pending, all_pending, done}`; `ConfigGenerations::text(generation, hash)`;
  - `cluster`: `ControllerDeps.removals: Removals`, `ControllerDeps.removed: RemovedPipelines` (`RemovedPipelines::resolve(doc) -> Result<(Pipeline, PipelineDocs), String>`), `removed_pipelines(generations, env, key, docs)`; `CoordinatorDeps.removals`; `SWEEP_INTERVAL = 30 s`; `PipelineController::remove() -> bool` (whether it tore down) and `remove_orphan()`.

A removal used to be applied only by a controller that ran the pipeline when its node
adopted the generation. With the owner down, or the lease between release and claim, no node tore
it down: the slot stayed, pinning WAL. The coordinator now records a tombstone for each id the new
generation drops before it applies anything; a tombstone that can't be written refuses the
adoption, which the next poll tries again (`record` keeps one under way, so every node writing
the same one is harmless). The controller that tears down marks it done. The reconciler sweeps
every 30 s: a pending tombstone the ring places on this node, with no controller of its own,
gets an orphan controller. It resolves the pipeline from the tombstone's generation in the node's
environment, claims the lease once it is free (an expired lease included), tears down as
`auto_cleanup` says, releases, and marks the tombstone done. Tombstones are marked done, not
deleted: one per id ever removed, an audit trail of what went and when.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 909eba6..01b6458 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1364,7 +1364,7 @@ mod tests {
     use super::*;
     use crate::test_support::{
         Fault, Kind, ScriptedStore, TestFactory, deps, eventually, holder, leases, node, pipeline,
-        pipeline_docs, pipeline_docs_on, row, until,
+        pipeline_docs, pipeline_docs_on, removals, row, tombstone, until,
     };
 
     /// A real two-second watchdog over `wait` on the paused clock: a controller that spins keeps
@@ -2530,6 +2530,7 @@ mod tests {
         let mut n = node("n1", &store, &docs, &f);
         until(&mut n.status, "running", is(Actual::Running)).await;
         f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
+        removals(&store).record(&tombstone("p")).await.unwrap();
         n.updates.send(Update::Remove).unwrap();
         tokio::time::timeout(Duration::from_secs(60), &mut n.task)
             .await
@@ -2539,6 +2540,11 @@ mod tests {
         assert_eq!(f.source.teardowns(), 1);
         let lease = leases(&store).observe("p").await.unwrap().unwrap();
         assert!(lease.doc.released);
+        assert_eq!(
+            removals(&store).pending("p").await.unwrap(),
+            None,
+            "the removal is done (DA-OPS-9)"
+        );
         let text = n.metrics.encode();
         assert!(!text.contains(P), "{text}");
         assert!(line(&text, "data_avalanche_owned_pipelines 0"), "{text}");
@@ -2704,6 +2710,7 @@ mod tests {
     #[tokio::test(start_paused = true)]
     async fn a_removal_with_its_drain_cut_off_leaves_the_source() {
         let store = ScriptedStore::new();
+        removals(&store).record(&tombstone("p")).await.unwrap();
         let f = TestFactory::new(&["a"]);
         let mut d = deps("n1", &store, &f);
         d.settings.drain_timeout = Duration::from_secs(1);
@@ -2721,6 +2728,10 @@ mod tests {
             .expect("the controller ends")
             .unwrap();
         assert_eq!(f.source.teardowns(), 0, "the source is left as it is");
+        assert!(
+            removals(&store).pending("p").await.unwrap().is_some(),
+            "the tombstone stays, for the sweep once the lease expires"
+        );
         let lease = leases(&store).observe("p").await.unwrap().unwrap();
         assert!(!lease.doc.released, "the lease expires, not released");
         assert!(lease.doc.is_held_by(&holder("n1")));
```

`crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index c358ea1..9d17157 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -676,6 +676,7 @@ mod tests {
     /// gossip's view, so a test can make a member gossip a generation.
     struct Lone {
         generations: ConfigGenerations,
+        removals: Removals,
         vars: Arc<Vars>,
         coordinator: ConfigCoordinator,
         reloads: mpsc::UnboundedReceiver<Reload>,
@@ -693,7 +694,8 @@ mod tests {
     }
 
     async fn start(text: &str, vars: Vars, store: Arc<dyn VersionedStore>) -> Lone {
-        let generations = ConfigGenerations::new(DocStore::new(store));
+        let generations = ConfigGenerations::new(DocStore::new(Arc::clone(&store)));
+        let removals = Removals::new(DocStore::new(store));
         let (first, created) = generations
             .bootstrap_or_adopt(text, "n1", Utc::now())
             .await
@@ -728,6 +730,7 @@ mod tests {
         let seen = Arc::clone(&globals);
         let deps = CoordinatorDeps {
             generations: generations.clone(),
+            removals: removals.clone(),
             env: Arc::clone(&vars) as Arc<dyn Env + Send + Sync>,
             key,
             node_id: "n1".into(),
@@ -743,6 +746,7 @@ mod tests {
         let coordinator = ConfigCoordinator::start(config, first.pointer, deps, members_rx, POLL);
         Lone {
             generations,
+            removals,
             vars,
             coordinator,
             reloads,
@@ -818,6 +822,35 @@ mod tests {
         );
     }
 
+    /// DA-OPS-9: a generation that removes a pipeline records its tombstone before the
+    /// reconciler hears of it, naming the generation that last had it; that generation's text
+    /// resolves it here.
+    #[tokio::test(start_paused = true)]
+    async fn a_removal_is_recorded_with_the_generation_that_last_had_it() {
+        let mut n = lone(&["p", "q"]).await;
+        let first = n.generations.current().await.unwrap().unwrap().pointer;
+        assert_eq!(n.publish_elsewhere(&config_text(&["p"])).await, 2);
+        let r = n.reload().await;
+        assert_eq!(kinds(&r), ["remove q"]);
+        let t = n.removals.pending("q").await.unwrap().expect("recorded");
+        assert_eq!((t.generation, &t.hash), (1, &first.doc.hash));
+        assert_eq!(n.removals.pending("p").await.unwrap(), None);
+        let removed = removed_pipelines(
+            n.generations.clone(),
+            Arc::clone(&n.vars) as Arc<dyn Env + Send + Sync>,
+            FingerprintKey::from_bytes([1; 32]),
+            Arc::new(|_| Ok(pipeline_docs())),
+        );
+        let (q, _) = removed.resolve(t.clone()).await.unwrap();
+        assert_eq!(q.id, "q");
+        let wrong = RemovalDoc {
+            pipeline_id: "nope".into(),
+            ..t
+        };
+        let e = removed.resolve(wrong).await.unwrap_err();
+        assert!(e.contains("has no pipeline nope"), "{e}");
+    }
+
     /// A member that gossips a newer generation brings it in at once, not at the next poll.
     #[tokio::test(start_paused = true)]
     async fn a_gossiped_generation_is_adopted_without_waiting_for_the_poll() {
```

`crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 8d3c085..78e4de7 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -578,8 +578,8 @@ mod tests {
 
     use super::*;
     use crate::test_support::{
-        ByPipeline, STABLE, ScriptedStore, TestFactory, alone, config, deps, eventually, leases,
-        members, pipeline_docs, row, until,
+        ByPipeline, STABLE, ScriptedStore, TestFactory, acquired, alone, config, deps, eventually,
+        leases, members, pipeline_docs, removals, row, timing, tombstone, until,
     };
     use crate::{Parts, Phase, Ring, WorkerFactory};
 
@@ -632,6 +632,79 @@ mod tests {
         (r, fs["p"].clone(), fs["q"].clone(), d)
     }
 
+    /// DA-OPS-9: a removal no node removed (no node held the lease when the generation came) is
+    /// taken up by the node the ring places its id on: the lease claimed, the source torn down,
+    /// the lease released, the tombstone done. One that doesn't resolve here waits, and doesn't
+    /// hold up the others.
+    #[tokio::test(start_paused = true)]
+    async fn a_removal_no_node_removed_is_torn_down_where_the_ring_places_it() {
+        let store = ScriptedStore::new();
+        let (fs, d) = factories(&store, &["p"]);
+        let r = removals(&store);
+        r.record(&tombstone("p")).await.unwrap();
+        r.record(&tombstone("unresolvable")).await.unwrap();
+        let (_, view) = watch::channel(alone("n1"));
+        let _r = ClusterReconciler::start(vec![], &d, view, reconciler_settings());
+        eventually("p is torn down", || fs["p"].source.teardowns() == 1).await;
+        tokio::time::sleep(Duration::from_secs(1)).await;
+        assert_eq!(r.pending("p").await.unwrap(), None, "p's removal done");
+        let lease = leases(&store).observe("p").await.unwrap().unwrap();
+        assert!(lease.doc.released, "{lease:?}");
+        tokio::time::sleep(SWEEP_INTERVAL * 2).await;
+        assert_eq!(fs["p"].source.teardowns(), 1, "once");
+        assert!(r.pending("unresolvable").await.unwrap().is_some());
+    }
+
+    /// A removal whose lease a gone node holds (its owner crashed, or its drain was cut off)
+    /// waits for the lease to expire, then is taken over and torn down.
+    #[tokio::test(start_paused = true)]
+    async fn a_removal_held_by_a_gone_node_is_torn_down_once_its_lease_expires() {
+        let store = ScriptedStore::new();
+        let (fs, d) = factories(&store, &["p"]);
+        let gone = acquired(&leases(&store), "n9").await;
+        removals(&store).record(&tombstone("p")).await.unwrap();
+        let (_, view) = watch::channel(alone("n1"));
+        let _r = ClusterReconciler::start(vec![], &d, view, reconciler_settings());
+        tokio::time::sleep(timing().duration / 2).await;
+        assert_eq!(fs["p"].source.teardowns(), 0, "the lease holds");
+        eventually("p is torn down", || fs["p"].source.teardowns() == 1).await;
+        let lease = leases(&store).observe("p").await.unwrap().unwrap();
+        assert_eq!(
+            lease.doc.owner_epoch,
+            gone.doc.owner_epoch + 1,
+            "taken over"
+        );
+        assert!(lease.doc.released);
+    }
+
+    /// The node the ring doesn't place a removed id on leaves it to the one it does.
+    #[tokio::test(start_paused = true)]
+    async fn a_removal_is_taken_up_only_where_the_ring_places_it() {
+        let store = ScriptedStore::new();
+        let ids = ["p", "q", "r", "s", "t", "u", "v", "w"];
+        let (fs, d) = factories(&store, &ids);
+        let view = members(
+            "n1",
+            &[("n1", Phase::Active, false), ("n2", Phase::Active, false)],
+        );
+        let ring = Ring::new(["n1", "n2"], 128);
+        let (here, there): (Vec<&str>, Vec<&str>) =
+            ids.iter().partition(|id| ring.owner(id) == Some("n1"));
+        assert!(!here.is_empty() && !there.is_empty(), "{here:?} {there:?}");
+        for id in ids {
+            removals(&store).record(&tombstone(id)).await.unwrap();
+        }
+        let (_, view) = watch::channel(view);
+        let _r = ClusterReconciler::start(vec![], &d, view, reconciler_settings());
+        for id in &here {
+            eventually("torn down here", || fs[*id].source.teardowns() == 1).await;
+        }
+        tokio::time::sleep(SWEEP_INTERVAL * 2).await;
+        for id in &there {
+            assert_eq!(fs[*id].source.teardowns(), 0, "{id} is n2's");
+        }
+    }
+
     #[test]
     fn the_node_is_its_node_name_else_its_hostname() {
         let started = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-state -p data-avalanche-cluster -p data-avalanche-api -p data-avalanche --no-fail-fast
```

Expected: FAIL to compile:
  - `error[E0432]: unresolved imports `crate::test_support::removals`, `crate::test_support::tombstone` (crates/data-avalanche-cluster/src/controller.rs)`
  - `error[E0432]: unresolved imports `crate::test_support::removals`, `crate::test_support::tombstone` (crates/data-avalanche-cluster/src/reconciler.rs)`
  - `error[E0425]: cannot find type `Removals` in this scope (crates/data-avalanche-cluster/src/coordinator.rs)`
  - `error[E0422]: cannot find struct, variant or union type `RemovalDoc` in this scope (crates/data-avalanche-cluster/src/coordinator.rs)`
  - … (4 more)

- [ ] **Step 3: Implement**

`Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 6ebaeb7..c84b0b3 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1935,6 +1935,7 @@ dependencies = [
  "serde_json",
  "thiserror 2.0.21",
  "tokio",
+ "tracing",
 ]
 
 [[package]]
```

`crates/data-avalanche-api/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 2746a52..627a35e 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -17,13 +17,13 @@ use base64::engine::general_purpose::URL_SAFE_NO_PAD;
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Parts, Phase,
-    ReconcilerSettings, Udp, WorkerFactory,
+    ReconcilerSettings, Udp, WorkerFactory, removed_pipelines,
 };
 use data_avalanche_config::{
     ApiAuth, Env, FingerprintKey, HostPort, Jwt, Pipeline, ProcessEnv, ResolvedConfig, load_str,
 };
 use data_avalanche_pipeline::{Backoff, Tuning};
-use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases, PipelineDocs};
+use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases, PipelineDocs, Removals};
 use data_avalanche_storage::ObjectStoreVersioned;
 use data_avalanche_telemetry::Metrics;
 use jsonwebtoken::{Algorithm, EncodingKey, Header};
@@ -207,8 +207,16 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
     );
     let metrics = Metrics::new();
     let c = &config.globals.cluster;
+    let removals = Removals::new(docs.clone());
     let deps = ControllerDeps {
         leases: leases.clone(),
+        removals: removals.clone(),
+        removed: removed_pipelines(
+            generations.clone(),
+            Arc::clone(&env),
+            key,
+            Arc::new(|p| Err(p.id.clone())),
+        ),
         holder: Holder {
             node_id: "node-a".into(),
             incarnation: 1,
@@ -232,6 +240,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
         first.pointer,
         CoordinatorDeps {
             generations,
+            removals,
             env,
             key,
             node_id: "node-a".into(),
```

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 01b6458..cf55560 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -19,7 +19,8 @@ use data_avalanche_pipeline::{
     WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
 };
 use data_avalanche_state::{
-    ActualState, DesiredState, Holder, Leases, PipelineDocs, RequestsDoc, StatusDoc,
+    ActualState, DesiredState, Holder, Leases, PipelineDocs, RemovalDoc, Removals, RequestsDoc,
+    StatusDoc,
 };
 use data_avalanche_storage::ObjectVersion;
 use data_avalanche_telemetry::{
@@ -149,6 +150,35 @@ pub struct ControllerDeps {
     pub starts: Arc<Semaphore>,
     /// Caps the workers handed off at once (`max_concurrent_handoffs`, Clustering §8.5).
     pub handoffs: Arc<Semaphore>,
+    /// The removal tombstones in `globals.state` (DA-OPS-9).
+    pub removals: Removals,
+    /// A removed pipeline, from the generation its tombstone names, as this node resolves it.
+    pub removed: RemovedPipelines,
+}
+
+/// [`RemovedPipelines`]' answer: the pipeline and its state documents.
+pub type Resolved = std::pin::Pin<
+    Box<dyn std::future::Future<Output = Result<(Pipeline, PipelineDocs), String>> + Send>,
+>;
+
+/// Resolves a removal tombstone's pipeline, with its state documents, from the generation that
+/// last had it, in this node's environment (the coordinator builds it; `coordinator.rs`
+/// `removed_pipelines`).
+#[derive(Clone)]
+pub struct RemovedPipelines(pub Arc<dyn Fn(RemovalDoc) -> Resolved + Send + Sync>);
+
+impl Debug for RemovedPipelines {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("RemovedPipelines").finish_non_exhaustive()
+    }
+}
+
+impl RemovedPipelines {
+    /// # Errors
+    /// The generation's text is unread, or doesn't resolve here, or doesn't have the pipeline.
+    pub async fn resolve(&self, doc: RemovalDoc) -> Result<(Pipeline, PipelineDocs), String> {
+        (self.0)(doc).await
+    }
 }
 
 /// One pipeline's controller.
@@ -172,6 +202,7 @@ pub struct PipelineController {
     /// The removed pipelines this one takes the place of: their leases are let go of before
     /// this one's is first claimed ([`Self::following`]).
     following: Following,
+    removals: Removals,
 }
 
 /// The ids of the removed pipelines a pipeline takes the place of (config §9.1: an `id` change
@@ -462,6 +493,7 @@ impl PipelineController {
             updates,
             updater,
             following: Following::default(),
+            removals: deps.removals.clone(),
         }
     }
 
@@ -987,7 +1019,9 @@ impl PipelineController {
                     desired = self.desired().await;
                 }
                 Event::Update(Update::Remove) => {
-                    self.remove(run, worker.take()).await;
+                    if self.remove(run, worker.take()).await {
+                        self.removal_done().await;
+                    }
                     return Left::Removed;
                 }
                 Event::Update(Update::DrainTimeout(d)) => self.settings.drain_timeout = d,
@@ -1162,31 +1196,101 @@ impl PipelineController {
     }
 
     /// A pipeline gone from the config (§12.3): the worker drains, what the source created for
-    /// the pipeline goes, and the lease is released. A drain cut off, or a lease lost meanwhile,
-    /// leaves the source as it is: a cut-off worker may still hold it, a lost lease's holder runs
-    /// it.
-    async fn remove(&self, run: LeaseRun, worker: Option<Running>) {
+    /// the pipeline goes, and the lease is released; true then. A drain cut off, or a lease lost
+    /// meanwhile, leaves the source as it is: a cut-off worker may still hold it, a lost lease's
+    /// holder runs it. Its tombstone stays, and the node the ring places the id on tears it down
+    /// once the lease is let go of ([`Self::remove_orphan`]).
+    async fn remove(&self, run: LeaseRun, worker: Option<Running>) -> bool {
         let drained = match worker {
             Some(w) => self.drain(w, &mut None).await,
             None => true,
         };
-        if !drained {
+        let done = if !drained {
             let _ = run.stop().await;
             tracing::warn!(
                 pipeline = %self.pipeline.name,
-                "removed with its drain cut off: the lease expires, and what the source created for the pipeline stays"
+                "removed with its drain cut off: the lease expires, then what the source created for the pipeline is torn down"
             );
+            false
         } else if self.teardown(&*run.guard).await {
             self.give_back(run).await;
+            true
         } else {
             let _ = run.stop().await;
             tracing::warn!(
                 pipeline = %self.pipeline.name,
-                "removed after its lease was lost: what the source created for the pipeline stays"
+                "removed after its lease was lost: what the source created for the pipeline stays to its holder"
             );
-        }
+            false
+        };
         self.metrics.forget();
         tracing::info!(pipeline = %self.pipeline.name, "pipeline removed");
+        done
+    }
+
+    /// Marks the removal's tombstone done: what follows the pipeline may start. Not written, the
+    /// tombstone stays, and the next sweep removes the pipeline again (nothing left to tear down).
+    async fn removal_done(&self) {
+        if let Err(e) = self
+            .removals
+            .done(&self.pipeline.id, chrono::Utc::now())
+            .await
+        {
+            tracing::warn!(
+                pipeline = %self.pipeline.name,
+                "the removal's tombstone isn't marked done ({e}): a later sweep does it"
+            );
+        }
+    }
+
+    /// A removed pipeline that no node removed (DA-OPS-9): no node held its lease at the
+    /// removal, or its owner's drain was cut off. Its lease is claimed (taken over once it
+    /// expires), what the source created for it torn down, the lease released, and its tombstone
+    /// marked done. Ends early when the tombstone is done meanwhile: its holder removed it.
+    pub(crate) async fn remove_orphan(self) {
+        let mut claim = Claim::new(
+            self.leases.clone(),
+            &self.pipeline.id,
+            self.holder.clone(),
+            self.settings.timing,
+        );
+        loop {
+            match self.removals.pending(&self.pipeline.id).await {
+                Ok(Some(_)) => {}
+                Ok(None) => return,
+                Err(e) => {
+                    tracing::warn!(pipeline = %self.pipeline.name, "the removal's tombstone unread: {e}")
+                }
+            }
+            let wait = match claim.step().await {
+                Ok(Claimed::Mine { lease, written, .. }) => {
+                    tracing::info!(
+                        pipeline = %self.pipeline.name,
+                        "a removed pipeline no node removed: its lease is taken, to tear it down"
+                    );
+                    let run = LeaseRun::start(
+                        self.leases.clone(),
+                        lease,
+                        self.holder.clone(),
+                        self.settings.timing,
+                        written,
+                        Arc::clone(&self.metrics),
+                    );
+                    if self.remove(run, None).await {
+                        self.removal_done().await;
+                    }
+                    return;
+                }
+                Ok(Claimed::HeldBy { takeover_at, .. }) => {
+                    takeover_at.min(Instant::now() + self.settings.timing.renewal_interval)
+                }
+                Err(e) => {
+                    tracing::warn!(pipeline = %self.pipeline.name, "lease claim failed: {e}");
+                    Instant::now() + self.settings.timing.renewal_interval
+                }
+            };
+            tokio::time::sleep_until(wait).await;
+        }
     }
 
     /// The source's `teardown` (PostgreSQL's `auto_cleanup`: the slot and the publication),
```

`crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 9d17157..aa64cef 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -16,8 +16,8 @@ use data_avalanche_config::{
 };
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{
-    ConfigGeneration, ConfigGenerations, GENERATION_KEY, GenerationDoc, PipelineDocs, StateError,
-    Versioned, config_hash,
+    ConfigGeneration, ConfigGenerations, GENERATION_KEY, GenerationDoc, PipelineDocs, RemovalDoc,
+    Removals, StateError, Versioned, config_hash,
 };
 use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
@@ -25,7 +25,9 @@ use tokio::sync::{Mutex, watch};
 use tokio::task::JoinHandle;
 use tokio::time::MissedTickBehavior;
 
-use crate::{Advertiser, Members, PipelineReload, Reload, Reloader, WorkerFactory};
+use crate::{
+    Advertiser, Members, PipelineReload, Reload, Reloader, RemovedPipelines, WorkerFactory,
+};
 
 /// A reload re-reads the generation and tries again this many times when another node publishes
 /// first.
@@ -38,6 +40,9 @@ pub type DocsFn = dyn Fn(&Pipeline) -> Result<PipelineDocs, String> + Send + Syn
 #[derive(Clone)]
 pub struct CoordinatorDeps {
     pub generations: ConfigGenerations,
+    /// The removal tombstones: a generation that removes a pipeline records one first
+    /// (DA-OPS-9).
+    pub removals: Removals,
     /// This node's environment: each generation's text is resolved in it.
     pub env: Arc<dyn Env + Send + Sync>,
     /// The anchor's: the running config was loaded with it, and both sides of a diff must be.
@@ -377,6 +382,22 @@ impl Inner {
             Ok(pipelines) => pipelines,
             Err(why) => return refused(running, why),
         };
+        // The removals' tombstones go first: a removal is torn down by whichever node can, and
+        // what takes a removed pipeline's place waits for it, even if this node stops now.
+        for change in &pipelines {
+            if let PipelineReload::Remove(id) = change {
+                let tombstone = RemovalDoc {
+                    pipeline_id: id.clone(),
+                    generation: running.generation.doc.generation,
+                    hash: running.generation.doc.hash.clone(),
+                    removed_at: Utc::now(),
+                    done_at: None,
+                };
+                if let Err(e) = d.removals.record(&tombstone).await {
+                    return refused(running, format!("the removal of {id} isn't recorded: {e}"));
+                }
+            }
+        }
         d.reloader.apply(Reload {
             pipelines,
             cluster: new.globals.cluster.clone(),
@@ -435,6 +456,40 @@ impl Inner {
     }
 }
 
+/// A removal tombstone's pipeline, from the text of the generation it names, loaded in `env`
+/// with the anchor's `key`, and its state documents (`docs`).
+pub fn removed_pipelines(
+    generations: ConfigGenerations,
+    env: Arc<dyn Env + Send + Sync>,
+    key: FingerprintKey,
+    docs: Arc<DocsFn>,
+) -> RemovedPipelines {
+    RemovedPipelines(Arc::new(move |doc: RemovalDoc| {
+        let (generations, env, docs) = (generations.clone(), Arc::clone(&env), Arc::clone(&docs));
+        Box::pin(async move {
+            let text = generations
+                .text(doc.generation, &doc.hash)
+                .await
+                .map_err(|e| e.to_string())?;
+            let config = load_str(&text, &*env, &key)
+                .map_err(|report| lines(&report))?
+                .config;
+            let pipeline = config
+                .pipelines
+                .into_values()
+                .find(|p| p.id == doc.pipeline_id)
+                .ok_or_else(|| {
+                    format!(
+                        "generation {} has no pipeline {}",
+                        doc.generation, doc.pipeline_id
+                    )
+                })?;
+            let d = docs(&pipeline)?;
+            Ok((pipeline, d))
+        })
+    }))
+}
+
 /// A report's errors, a line each, for the log.
 fn lines(report: &Report) -> String {
     report
```

`crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 0d33836..7c75ec6 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -24,10 +24,11 @@ pub mod wire;
 pub use claim::{Claim, Claimed, release};
 pub use controller::{
     Actual, ControllerDeps, ControllerSettings, ControllerStatus, Parts, PipelineController,
-    Target, Update, WorkerFactory,
+    RemovedPipelines, Resolved, Target, Update, WorkerFactory,
 };
 pub use coordinator::{
     Checked, ConfigCoordinator, CoordinatorDeps, DocsFn, PUBLISH_ATTEMPTS, ReloadError, Reloaded,
+    removed_pipelines,
 };
 pub use discovery::{Seeds, advertise, discover, interfaces};
 pub use error::ClusterError;
@@ -38,7 +39,8 @@ pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
 pub use node::{JOIN_WAIT, join, leave};
 pub use placement::Placement;
 pub use reconciler::{
-    ClusterReconciler, PipelineReload, ReconcilerSettings, Reload, Reloader, node_identity,
+    ClusterReconciler, PipelineReload, ReconcilerSettings, Reload, Reloader, SWEEP_INTERVAL,
+    node_identity,
 };
 pub use ring::Ring;
 pub use wire::{GossipKey, Udp, Wire};
```

`crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 78e4de7..5ec6655 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -7,7 +7,7 @@ use std::sync::Arc;
 use std::time::{Duration, SystemTime, UNIX_EPOCH};
 
 use data_avalanche_config::{Cluster, Pipeline, StateLocation};
-use data_avalanche_state::{Holder, PipelineDocs};
+use data_avalanche_state::{Holder, PipelineDocs, RemovalDoc, StateError};
 use data_avalanche_storage::{Location, colocated_state, explicit_state, join};
 use tokio::sync::{Semaphore, mpsc, oneshot, watch};
 use tokio::task::{JoinHandle, JoinSet};
@@ -19,6 +19,10 @@ use crate::{
     Target, Update,
 };
 
+/// How often a node lists the removal tombstones, to take up those of the ids the ring places on
+/// it that no node removed (DA-OPS-9).
+pub const SWEEP_INTERVAL: Duration = Duration::from_secs(30);
+
 /// This node as a lease holder (Clustering §3.4, D-1): `node_id` is
 /// `globals.cluster.memberlist.node_name`, else the OS hostname; the incarnation is the process's
 /// start in unix milliseconds, so a restarted process is another holder.
@@ -255,6 +259,9 @@ impl ClusterReconciler {
                 statuses: statuses_tx,
                 removing: JoinSet::new(),
                 returning: BTreeMap::new(),
+                sweeping: JoinSet::new(),
+                orphans: JoinSet::new(),
+                orphaned: std::collections::BTreeSet::new(),
             },
             members,
             settings,
@@ -363,6 +370,11 @@ struct Loop {
     /// The ids whose removal runs, each with the pipeline a later generation added back under
     /// it: that one starts when the removal ends, so one controller runs a pipeline at a time.
     returning: BTreeMap<String, Option<(Pipeline, PipelineDocs)>>,
+    /// The listing of the removal tombstones under way, one at a time.
+    sweeping: JoinSet<Result<Vec<RemovalDoc>, StateError>>,
+    /// The removals no node removed that this node took up; each ends with its id.
+    orphans: JoinSet<String>,
+    orphaned: std::collections::BTreeSet<String>,
 }
 
 impl Loop {
@@ -472,6 +484,26 @@ impl Loop {
         });
     }
 
+    /// The tombstones a sweep listed (DA-OPS-9): each removal of an id the ring places here,
+    /// that this node neither runs nor took up already, is taken up ([`orphan`]).
+    fn sweep(&mut self, pending: Vec<RemovalDoc>, placement: &Placement, me: &str) {
+        for doc in pending {
+            let id = doc.pipeline_id.clone();
+            if self.returning.contains_key(&id)
+                || self.orphaned.contains(&id)
+                || placement.ring().owner(&id) != Some(me)
+            {
+                continue;
+            }
+            self.orphaned.insert(id.clone());
+            let deps = self.deps.clone();
+            self.orphans.spawn(async move {
+                orphan(doc, deps).await;
+                id
+            });
+        }
+    }
+
     /// A removal ended: the pipeline added back under its id, if any, starts now.
     fn removed(&mut self, id: &str) {
         if let Some(Some((pipeline, docs))) = self.returning.remove(id) {
@@ -483,6 +515,24 @@ impl Loop {
     }
 }
 
+/// A removal no node removed, taken up by the node the ring places its id on: its pipeline
+/// resolved from the generation its tombstone names, then removed under its lease
+/// ([`PipelineController::remove_orphan`]). One that doesn't resolve here waits for the next
+/// sweep.
+async fn orphan(doc: RemovalDoc, deps: ControllerDeps) {
+    let (id, generation) = (doc.pipeline_id.clone(), doc.generation);
+    match deps.removed.resolve(doc).await {
+        Ok((pipeline, docs)) => {
+            let (c, _status) = PipelineController::new(pipeline, docs, &deps);
+            c.remove_orphan().await;
+        }
+        Err(e) => tracing::warn!(
+            pipeline_id = %id,
+            "a removed pipeline waits for its teardown: generation {generation}, which last had it, doesn't resolve here: {e}"
+        ),
+    }
+}
+
 /// The reconcile loop (Clustering §8.1, §20.1): on every membership change, every due ring
 /// addition, every reload and every `resync`, each pipeline's target is "own" iff the ring
 /// places it here.
@@ -496,6 +546,8 @@ async fn reconcile(
     let mut placement = Placement::new(settings.tokens, settings.stabilization);
     let mut resync = tokio::time::interval(settings.resync);
     resync.set_missed_tick_behavior(MissedTickBehavior::Delay);
+    let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
+    sweep.set_missed_tick_behavior(MissedTickBehavior::Delay);
     let mut gossiping = true;
     loop {
         let view = members.borrow_and_update().clone();
@@ -552,8 +604,26 @@ async fn reconcile(
                 }
             } => {}
             _ = resync.tick() => {}
+            _ = sweep.tick(), if state.sweeping.is_empty() => {
+                let removals = state.deps.removals.clone();
+                state.sweeping.spawn(async move { removals.all_pending().await });
+            }
+            Some(listed) = state.sweeping.join_next() => match listed {
+                Ok(Ok(pending)) => {
+                    let me = members.borrow().me.clone();
+                    state.sweep(pending, &placement, &me);
+                }
+                Ok(Err(e)) => tracing::warn!("the removal tombstones unread: {e}"),
+                Err(e) => tracing::error!("the removal tombstones' listing ended abnormally: {e}"),
+            },
+            Some(ended) = state.orphans.join_next() => {
+                if let Ok(id) = ended {
+                    state.orphaned.remove(&id);
+                }
+            }
         }
     }
+    state.orphans.abort_all();
     // Every target closes at once (a replaced sender is dropped): the controllers drain
     // together, and the removed pipelines' controllers finish.
     let mut controllers: Vec<Controlled> = state.controllers.into_values().collect();
```

`crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index e163f6c..5fa8b71 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -9,7 +9,7 @@ use std::time::Duration;
 use async_trait::async_trait;
 use bytes::Bytes;
 use chrono::Utc;
-use data_avalanche_state::{Acquired, DocStore, Holder, LeaseDoc, Leases, Versioned};
+use data_avalanche_state::{Acquired, DocStore, Holder, LeaseDoc, Leases, Removals, Versioned};
 use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
 use data_avalanche_telemetry::{Metrics, PipelineLabels, PipelineMetrics};
 use object_store::memory::InMemory;
@@ -196,6 +196,38 @@ pub(crate) fn leases(store: &Arc<ScriptedStore>) -> Leases {
     Leases::new(DocStore::new(Arc::clone(store) as Arc<dyn VersionedStore>))
 }
 
+pub(crate) fn removals(store: &Arc<ScriptedStore>) -> Removals {
+    Removals::new(DocStore::new(Arc::clone(store) as Arc<dyn VersionedStore>))
+}
+
+/// A removal of `id` under way, from generation 1.
+pub(crate) fn tombstone(id: &str) -> data_avalanche_state::RemovalDoc {
+    data_avalanche_state::RemovalDoc {
+        pipeline_id: id.into(),
+        generation: 1,
+        hash: "blake3:00".into(),
+        removed_at: Utc::now(),
+        done_at: None,
+    }
+}
+
+/// Resolves a tombstone's pipeline as [`config`] of its id has it, with fresh documents; the
+/// id `unresolvable` doesn't resolve.
+pub(crate) fn removed() -> crate::RemovedPipelines {
+    crate::RemovedPipelines(Arc::new(|doc: data_avalanche_state::RemovalDoc| {
+        Box::pin(async move {
+            if doc.pipeline_id == "unresolvable" {
+                return Err("no such variable".to_owned());
+            }
+            let p = config(&[&doc.pipeline_id])
+                .pipelines
+                .remove(&doc.pipeline_id)
+                .unwrap();
+            Ok((p, pipeline_docs()))
+        })
+    }))
+}
+
 pub(crate) async fn acquired(leases: &Leases, node: &str) -> Versioned<LeaseDoc> {
     match leases
         .acquire("p", &holder(node), timing().duration, Utc::now())
@@ -401,6 +433,8 @@ pub(crate) fn deps(
         settings: settings(),
         starts: Arc::new(tokio::sync::Semaphore::new(1)),
         handoffs: Arc::new(tokio::sync::Semaphore::new(2)),
+        removals: removals(store),
+        removed: removed(),
     }
 }
 
@@ -776,6 +810,7 @@ impl TestCluster {
             first.pointer,
             crate::CoordinatorDeps {
                 generations: self.generations.clone(),
+                removals: deps.removals.clone(),
                 env: Arc::new(env),
                 key,
                 node_id: name.to_owned(),
```

`crates/data-avalanche-state/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-state/Cargo.toml b/crates/data-avalanche-state/Cargo.toml
index 17b7389..a59a551 100644
--- a/crates/data-avalanche-state/Cargo.toml
+++ b/crates/data-avalanche-state/Cargo.toml
@@ -18,6 +18,7 @@ data-avalanche-storage.workspace = true
 serde.workspace = true
 serde_json.workspace = true
 thiserror.workspace = true
+tracing.workspace = true
 
 [dev-dependencies]
 async-trait.workspace = true
```

`crates/data-avalanche-state/src/generation.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/generation.rs b/crates/data-avalanche-state/src/generation.rs
index 3b85f79..622bc74 100644
--- a/crates/data-avalanche-state/src/generation.rs
+++ b/crates/data-avalanche-state/src/generation.rs
@@ -68,22 +68,31 @@ impl ConfigGenerations {
         let Some(pointer) = self.docs.load::<GenerationDoc>(GENERATION_KEY).await? else {
             return Ok(None);
         };
-        let key = config_snapshot_key(pointer.doc.generation, &pointer.doc.hash);
+        let text = self.text(pointer.doc.generation, &pointer.doc.hash).await?;
+        Ok(Some(ConfigGeneration { pointer, text }))
+    }
+
+    /// The text of generation `generation`, whose hash is `hash`: snapshots are immutable and
+    /// kept, so an earlier generation's is read too (a removal tombstone names one).
+    ///
+    /// # Errors
+    /// A storage failure; `Corrupt` when the snapshot is missing or its text doesn't match
+    /// `hash`.
+    pub async fn text(&self, generation: u64, hash: &str) -> Result<String, StateError> {
+        let key = config_snapshot_key(generation, hash);
         let corrupt = |reason: &str| StateError::Corrupt {
             key: key.clone(),
             kind: "config snapshot",
             reason: reason.to_owned(),
         };
         let Some((body, _)) = self.docs.store().get(&key).await? else {
-            return Err(corrupt("missing; config/generation.json points at it"));
+            return Err(corrupt("missing"));
         };
         let text = String::from_utf8(body.to_vec()).map_err(|_| corrupt("not UTF-8"))?;
-        if config_hash(&text) != pointer.doc.hash {
-            return Err(corrupt(
-                "its text doesn't match the hash in config/generation.json",
-            ));
+        if config_hash(&text) != hash {
+            return Err(corrupt("its text doesn't match the hash it is named by"));
         }
-        Ok(Some(ConfigGeneration { pointer, text }))
+        Ok(text)
     }
 
     /// Cold start (§9.3): adopts the current generation if there is one; otherwise publishes
```

`crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 89aabe8..d9840d6 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -37,6 +37,7 @@ mod generation;
 mod lease;
 mod lifecycle;
 mod pipeline;
+mod removal;
 mod snapshot;
 mod tables;
 #[cfg(test)]
@@ -58,6 +59,7 @@ pub use pipeline::{
     DesiredState, PIPELINE_STATE_KEY, POSITION_KEY, PipelineDocs, PipelineStateDoc, PositionDoc,
     SLOT_KEY, SlotDoc,
 };
+pub use removal::{REMOVALS_PREFIX, RemovalDoc, Removals, removal_key};
 pub use snapshot::{
     CursorKey, JOBS_PREFIX, JobStatus, JobTable, JobTableStatus, SnapshotCursor, SnapshotJobDoc,
     SnapshotStatus, TableSnapshotDoc, job_key, table_snapshot_key,
```

`crates/data-avalanche-state/src/removal.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/removal.rs b/crates/data-avalanche-state/src/removal.rs
new file mode 100644
index 0000000..f5a77bf
--- /dev/null
+++ b/crates/data-avalanche-state/src/removal.rs
@@ -0,0 +1,199 @@
+//! Removal tombstones in `globals.state` (config §12.3, DA-OPS-9, DA-OPS-10): `removals/<id>.json`
+//! for each pipeline a config generation removed, until what its source created for it is torn
+//! down and its lease let go. Every node that applies the generation records it; the removal's
+//! owner, or the node the ring places the id on when no node held it, marks it done. A pipeline
+//! that takes the removed one's place waits until it is done. The store has no delete, so a done
+//! tombstone stays, and a later removal of the same id records it again.
+
+use chrono::{DateTime, Utc};
+use data_avalanche_storage::escape_segment;
+use serde::{Deserialize, Serialize};
+
+use crate::doc::{DocStore, Document, StateError};
+
+pub const REMOVALS_PREFIX: &str = "removals";
+
+/// `removals/<id>.json`.
+pub fn removal_key(pipeline_id: &str) -> String {
+    format!("{REMOVALS_PREFIX}/{}.json", escape_segment(pipeline_id))
+}
+
+/// A removed pipeline: the generation that last had it (whose text every node resolves it
+/// from), and when its removal was done.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct RemovalDoc {
+    pub pipeline_id: String,
+    /// The last generation that had the pipeline, and its text's hash.
+    pub generation: u64,
+    pub hash: String,
+    pub removed_at: DateTime<Utc>,
+    /// Set once the source's resources are torn down (`auto_cleanup`) and the lease let go.
+    pub done_at: Option<DateTime<Utc>>,
+}
+
+impl Document for RemovalDoc {
+    const KIND: &'static str = "removal";
+    const SCHEMA_VERSION: u64 = 1;
+}
+
+impl RemovalDoc {
+    pub fn is_done(&self) -> bool {
+        self.done_at.is_some()
+    }
+}
+
+/// The removal tombstones, over the `globals.state` documents.
+#[derive(Debug, Clone)]
+pub struct Removals {
+    docs: DocStore,
+}
+
+impl Removals {
+    /// `docs` is rooted at the `globals.state` location.
+    pub fn new(docs: DocStore) -> Self {
+        Self { docs }
+    }
+
+    /// Records `doc`'s removal. One under way is kept, whoever recorded it first (every node
+    /// records the same generation's); a done one is replaced: the id was added again, then
+    /// removed again.
+    ///
+    /// # Errors
+    /// A storage or decode failure.
+    pub async fn record(&self, doc: &RemovalDoc) -> Result<(), StateError> {
+        let key = removal_key(&doc.pipeline_id);
+        self.docs
+            .modify::<RemovalDoc, _>(&key, |current| {
+                Ok(match current {
+                    Some(c) if !c.is_done() => c,
+                    _ => doc.clone(),
+                })
+            })
+            .await?;
+        Ok(())
+    }
+
+    /// The removal of `pipeline_id` under way, if any.
+    ///
+    /// # Errors
+    /// A storage or decode failure.
+    pub async fn pending(&self, pipeline_id: &str) -> Result<Option<RemovalDoc>, StateError> {
+        Ok(self
+            .docs
+            .load::<RemovalDoc>(&removal_key(pipeline_id))
+            .await?
+            .map(|v| v.doc)
+            .filter(|d| !d.is_done()))
+    }
+
+    /// Every removal under way. A tombstone that doesn't decode is logged and skipped: it can't
+    /// be torn down, and it mustn't stop the others.
+    ///
+    /// # Errors
+    /// A storage failure listing or reading them.
+    pub async fn all_pending(&self) -> Result<Vec<RemovalDoc>, StateError> {
+        let mut out = vec![];
+        for key in self.docs.store().list(REMOVALS_PREFIX).await? {
+            match self.docs.load::<RemovalDoc>(&key).await {
+                Ok(Some(v)) if !v.doc.is_done() => out.push(v.doc),
+                Ok(_) => {}
+                Err(e @ (StateError::Corrupt { .. } | StateError::UnsupportedVersion { .. })) => {
+                    tracing::warn!("removal tombstone {key} skipped: {e}");
+                }
+                Err(e) => return Err(e),
+            }
+        }
+        Ok(out)
+    }
+
+    /// Marks the removal of `pipeline_id` done at `now`. Absent or done already: nothing.
+    ///
+    /// # Errors
+    /// A storage or decode failure.
+    pub async fn done(&self, pipeline_id: &str, now: DateTime<Utc>) -> Result<(), StateError> {
+        let key = removal_key(pipeline_id);
+        if self.pending(pipeline_id).await?.is_none() {
+            return Ok(());
+        }
+        self.docs
+            .modify::<RemovalDoc, _>(&key, |current| {
+                let mut d = current.ok_or_else(|| StateError::Corrupt {
+                    key: key.clone(),
+                    kind: RemovalDoc::KIND,
+                    reason: "gone while marked done".into(),
+                })?;
+                d.done_at.get_or_insert(now);
+                Ok(d)
+            })
+            .await?;
+        Ok(())
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use std::sync::Arc;
+
+    use chrono::TimeZone;
+    use data_avalanche_storage::ObjectStoreVersioned;
+    use object_store::memory::InMemory;
+
+    use super::*;
+
+    fn removals() -> Removals {
+        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
+        Removals::new(DocStore::new(Arc::new(store)))
+    }
+
+    fn t(s: u32) -> DateTime<Utc> {
+        Utc.with_ymd_and_hms(2026, 11, 4, 10, 0, s).unwrap()
+    }
+
+    fn removed(id: &str, generation: u64) -> RemovalDoc {
+        RemovalDoc {
+            pipeline_id: id.into(),
+            generation,
+            hash: format!("blake3:{generation}"),
+            removed_at: t(0),
+            done_at: None,
+        }
+    }
+
+    #[test]
+    fn a_removal_is_keyed_by_its_escaped_id() {
+        assert_eq!(removal_key("orders"), "removals/orders.json");
+        assert_eq!(removal_key("a/b"), "removals/a%2Fb.json");
+    }
+
+    /// Every node records the generation's removal: the first record stands while it is under
+    /// way, and a done one is replaced by the next removal of the id.
+    #[tokio::test]
+    async fn the_first_record_stands_until_its_removal_is_done() {
+        let r = removals();
+        r.record(&removed("p", 3)).await.unwrap();
+        r.record(&removed("p", 4)).await.unwrap();
+        assert_eq!(r.pending("p").await.unwrap(), Some(removed("p", 3)));
+        r.done("p", t(5)).await.unwrap();
+        r.done("p", t(9)).await.unwrap();
+        assert_eq!(r.pending("p").await.unwrap(), None);
+        assert!(r.all_pending().await.unwrap().is_empty());
+        r.record(&removed("p", 7)).await.unwrap();
+        assert_eq!(r.pending("p").await.unwrap(), Some(removed("p", 7)));
+        r.done("nope", t(5)).await.unwrap();
+    }
+
+    #[tokio::test]
+    async fn every_pending_removal_is_listed_and_a_corrupt_one_skipped() {
+        let r = removals();
+        r.record(&removed("p", 3)).await.unwrap();
+        r.record(&removed("q", 3)).await.unwrap();
+        r.done("q", t(1)).await.unwrap();
+        r.docs
+            .store()
+            .put("removals/bad.json", bytes::Bytes::from_static(b"{"))
+            .await
+            .unwrap();
+        assert_eq!(r.all_pending().await.unwrap(), [removed("p", 3)]);
+    }
+}
```

`crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 53d3e55..e1f460a 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -12,13 +12,13 @@ use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
     ReconcilerSettings, Seeds, Udp, advertise, discover, interfaces, join as join_gossip, leave,
-    node_identity,
+    node_identity, removed_pipelines,
 };
 use data_avalanche_config::{
     Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
 };
 use data_avalanche_pipeline::{Backoff, Tuning};
-use data_avalanche_state::{ConfigGenerations, Leases};
+use data_avalanche_state::{ConfigGenerations, Leases, Removals};
 use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
 use tokio::sync::{Semaphore, oneshot, watch};
@@ -150,8 +150,18 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     );
 
     let c = &config.globals.cluster;
+    let docs = Arc::clone(&buckets);
+    let docs: Arc<DocsFn> = Arc::new(move |p| docs.pipeline_docs(p).map_err(|e| e.to_string()));
+    let fingerprints = FingerprintKey::from_bytes(joined.fingerprint_key);
     let deps = ControllerDeps {
         leases: Leases::new(joined.globals.clone()),
+        removals: Removals::new(joined.globals.clone()),
+        removed: removed_pipelines(
+            ConfigGenerations::new(joined.globals.clone()),
+            Arc::new(ProcessEnv),
+            fingerprints,
+            Arc::clone(&docs),
+        ),
         holder,
         factory: Arc::new(Factory::new(&config.globals).map_err(Fatal::Config)?),
         metrics: metrics.clone(),
@@ -207,16 +217,15 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         ReconcilerSettings::new(c),
     );
     // Each newer config generation, this node's reloads included (Clustering §9).
-    let docs = Arc::clone(&buckets);
-    let docs: Arc<DocsFn> = Arc::new(move |p| docs.pipeline_docs(p).map_err(|e| e.to_string()));
     let (log, reloaded_auth) = (Arc::clone(&logger), Arc::clone(&auth));
     let coordinator = Arc::new(ConfigCoordinator::start(
         config.clone(),
         joined.generation,
         CoordinatorDeps {
             generations: ConfigGenerations::new(joined.globals.clone()),
+            removals: deps.removals.clone(),
             env: Arc::new(ProcessEnv),
-            key: FingerprintKey::from_bytes(joined.fingerprint_key),
+            key: fingerprints,
             node_id: deps.holder.node_id.clone(),
             docs: Arc::clone(&docs),
             factory: Arc::clone(&deps.factory),
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-state -p data-avalanche-cluster -p data-avalanche-api -p data-avalanche --no-fail-fast
```

Expected: PASS, 328 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche-api/src/test_support.rs \
  crates/data-avalanche-cluster/src/controller.rs \
  crates/data-avalanche-cluster/src/coordinator.rs \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/reconciler.rs \
  crates/data-avalanche-cluster/src/test_support.rs \
  crates/data-avalanche-state/Cargo.toml \
  crates/data-avalanche-state/src/generation.rs \
  crates/data-avalanche-state/src/lib.rs \
  crates/data-avalanche-state/src/removal.rs \
  crates/data-avalanche/src/run.rs
git commit -m "feat(cluster): a removed pipeline's tombstone, torn down by the node the ring places it on when no owner saw the removal (DA-OPS-9)"
```

---

### Task 4: A pipeline waits for the removal it follows (`cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-cluster/src/reconciler.rs`
- Test: `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-cluster/src/reconciler.rs`

**Gaps:** DA-OPS-10.

**Interfaces:**
- Consumes: Task 3's `Removals::{pending, all_pending}`, `RemovedPipelines::resolve`; the reconciler's `shares(removed, added)`.
- Produces:
  - the controller's `follow()`, run before its first claim: it waits (polling every `lease_renewal_interval`) until no pending tombstone is of its id or of a pipeline it `shares` resources with; a tombstone that can't be read is waited for, one that doesn't resolve is logged and not waited for;
  - the reconciler's `Following` (the same-reload sequencing) is gone: the tombstone covers it, on every node.

An `id` change that kept its slot, publication, sink or state was sequenced only within the
reload that made it, on the nodes that applied it: a node that started later, or a later reload,
let the new pipeline claim while the old one's teardown could still drop its slot. The wait moves
into the controller and keys on the tombstones, which every node sees, whatever reload or start
created the controller. A removal stopped while it waits (the follower itself removed) ends.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index cf55560..109a295 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -2678,72 +2678,52 @@ mod tests {
         assert!(lease.doc.is_held_by(&holder("n1")), "n1 still holds it");
     }
 
-    /// A followed lease whose holder never renews is let go of once it stayed unchanged for a
-    /// full lease duration (15 s), and one never written at once: only then is the pipeline's
-    /// own lease claimed. The followed lease itself is left as it is.
+    /// DA-OPS-10: a pipeline whose own id's removal is under way (removed, then added again)
+    /// waits on standby, its lease unclaimed, until the removal is done, then claims within a
+    /// renewal interval.
     #[tokio::test(start_paused = true)]
-    async fn a_followed_lease_never_renewed_is_let_go_after_a_lease_duration() {
+    async fn a_pipeline_waits_until_its_own_id_s_removal_is_done() {
         let store = ScriptedStore::new();
         let f = TestFactory::new(&["a"]);
-        leases(&store)
-            .acquire(
-                "x",
-                &holder("n0"),
-                Duration::from_secs(15),
-                chrono::Utc::now(),
-            )
-            .await
-            .unwrap();
+        removals(&store).record(&tombstone("p")).await.unwrap();
         let d = deps("n1", &store, &f);
         let (c, mut status) = PipelineController::new(pipeline(), pipeline_docs(), &d);
-        let c = c.following(Following::new(["x".into(), "never".into()]));
         let (_target, rx) = watch::channel(Target::Own);
-        let start = Instant::now();
         let _task = tokio::spawn(c.run(rx));
         until(&mut status, "standby", is(Actual::Standby { holder: None })).await;
-        tokio::time::sleep(Duration::from_secs(14)).await;
+        tokio::time::sleep(Duration::from_secs(60)).await;
         assert!(leases(&store).observe("p").await.unwrap().is_none());
         assert_eq!(f.builds(), 0);
+        removals(&store)
+            .done("p", chrono::Utc::now())
+            .await
+            .unwrap();
+        let start = Instant::now();
         until(&mut status, "running", is(Actual::Running)).await;
-        let took = start.elapsed();
         assert!(
-            took >= Duration::from_secs(15) && took < Duration::from_secs(21),
-            "{took:?}"
+            start.elapsed() <= Duration::from_secs(6),
+            "{:?}",
+            start.elapsed()
         );
-        let x = leases(&store).observe("x").await.unwrap().unwrap();
-        assert!(x.doc.is_held_by(&holder("n0")) && !x.doc.released);
     }
 
-    /// A controller started again after its wait was done (as after a panic) shares the list
-    /// its predecessor cleared: it doesn't wait again, though the followed id's lease is held by
-    /// then.
+    /// A removal under way that doesn't resolve here can't be compared with the pipeline: it
+    /// isn't waited for. Nor is one that shares nothing with it.
     #[tokio::test(start_paused = true)]
-    async fn a_follower_started_again_after_its_wait_waits_no_more() {
+    async fn a_removal_that_doesnt_resolve_or_share_isnt_waited_for() {
         let store = ScriptedStore::new();
-        let docs = pipeline_docs();
         let f = TestFactory::new(&["a"]);
-        let d = deps("n1", &store, &f);
-        let following = Following::new(["x".into()]);
-        let (c, mut status) = PipelineController::new(pipeline(), docs.clone(), &d);
-        let (_target, rx) = watch::channel(Target::Own);
-        let task = tokio::spawn(c.following(following.clone()).run(rx));
-        until(&mut status, "running", is(Actual::Running)).await;
-        assert!(following.ids().is_empty(), "cleared where it is shared");
-        task.abort();
-        leases(&store)
-            .acquire(
-                "x",
-                &holder("n0"),
-                Duration::from_secs(15),
-                chrono::Utc::now(),
-            )
+        removals(&store)
+            .record(&tombstone("unresolvable"))
             .await
             .unwrap();
-        let (c, mut status) = PipelineController::new(pipeline(), docs, &d);
+        removals(&store).record(&tombstone("x")).await.unwrap();
+        let d = deps("n1", &store, &f);
+        let (c, mut status) = PipelineController::new(pipeline(), pipeline_docs(), &d);
         let (_target, rx) = watch::channel(Target::Own);
         let start = Instant::now();
-        let _task = tokio::spawn(c.following(following).run(rx));
-        until(&mut status, "running again", is(Actual::Running)).await;
+        let _task = tokio::spawn(c.run(rx));
+        until(&mut status, "running", is(Actual::Running)).await;
         assert!(
             start.elapsed() < Duration::from_secs(5),
             "{:?}",
@@ -2751,24 +2731,14 @@ mod tests {
         );
     }
 
-    /// A pipeline removed while it waits for the pipelines it takes the place of ends there,
-    /// its lease never claimed.
+    /// A pipeline removed while it waits for a removal ends there, its lease never claimed.
     #[tokio::test(start_paused = true)]
     async fn a_follower_removed_while_it_waits_ends() {
         let store = ScriptedStore::new();
         let f = TestFactory::new(&["a"]);
-        leases(&store)
-            .acquire(
-                "x",
-                &holder("n0"),
-                Duration::from_secs(15),
-                chrono::Utc::now(),
-            )
-            .await
-            .unwrap();
+        removals(&store).record(&tombstone("p")).await.unwrap();
         let d = deps("n1", &store, &f);
         let (c, mut status) = PipelineController::new(pipeline(), pipeline_docs(), &d);
-        let c = c.following(Following::new(["x".into()]));
         let updates = c.updater();
         let (_target, rx) = watch::channel(Target::Own);
         let mut task = tokio::spawn(c.run(rx));
@@ -2814,7 +2784,6 @@ mod tests {
     #[tokio::test(start_paused = true)]
     async fn a_removal_with_its_drain_cut_off_leaves_the_source() {
         let store = ScriptedStore::new();
-        removals(&store).record(&tombstone("p")).await.unwrap();
         let f = TestFactory::new(&["a"]);
         let mut d = deps("n1", &store, &f);
         d.settings.drain_timeout = Duration::from_secs(1);
@@ -2826,6 +2795,7 @@ mod tests {
         f.sink.hold_commits();
         f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
         eventually("the commit waits", || f.sink.waiting_commits() == 1).await;
+        removals(&store).record(&tombstone("p")).await.unwrap();
         updates.send(Update::Remove).unwrap();
         tokio::time::timeout(Duration::from_secs(60), &mut task)
             .await
```

`crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 5ec6655..3a5018a 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -1210,8 +1210,15 @@ mod tests {
         (r, fs, seen)
     }
 
-    /// One reload adds `y` and removes `x`, with the 60 s drain timeout.
-    fn replace_x(r: &ClusterReconciler, y: Pipeline, docs: PipelineDocs) {
+    /// One reload adds `y` and removes `x`, with the 60 s drain timeout; `x`'s tombstone first,
+    /// as the coordinator records it.
+    async fn replace_x(
+        store: &Arc<ScriptedStore>,
+        r: &ClusterReconciler,
+        y: Pipeline,
+        docs: PipelineDocs,
+    ) {
+        removals(store).record(&tombstone("x")).await.unwrap();
         let mut c = cluster();
         c.drain_timeout = Duration::from_secs(60);
         assert!(r.reloader().apply(Reload {
@@ -1232,7 +1239,7 @@ mod tests {
         let (r, fs, seen) = x_with_its_drain_held(&store).await;
         let (mut y, docs) = pipelines(&["y"]).remove(0);
         y.source.postgres.as_mut().unwrap().slot = "x_slot".into();
-        replace_x(&r, y, docs);
+        replace_x(&store, &r, y, docs).await;
         eventually("y in", || r.statuses().contains_key("y")).await;
         // Past two lease durations: x's lease is renewed meanwhile, so it isn't let go.
         tokio::time::sleep(Duration::from_secs(30)).await;
@@ -1261,7 +1268,7 @@ mod tests {
         let store = ScriptedStore::new();
         let (r, fs, seen) = x_with_its_drain_held(&store).await;
         let (y, docs) = pipelines(&["y"]).remove(0);
-        replace_x(&r, y, docs);
+        replace_x(&store, &r, y, docs).await;
         eventually("y in", || r.statuses().contains_key("y")).await;
         until(r.statuses().get_mut("y").unwrap(), "y running", running).await;
         assert_eq!(fs["x"].sink.waiting_commits(), 1, "x still drains");
@@ -1271,67 +1278,68 @@ mod tests {
         assert_eq!(fs["x"].source.teardowns(), 1);
     }
 
-    /// A followed id that a later generation adds again names another pipeline: the wait on it
-    /// ends, though the lease under that id is still held (here by the removal, still draining;
-    /// next by the pipeline added under the id, which would renew it for good).
+    /// DA-OPS-10: the removal and the add in two generations. `y`, added after `x`'s removal
+    /// began, waits for it all the same: the tombstone, not the reload, says what it follows.
     #[tokio::test(start_paused = true)]
-    async fn a_wait_on_an_id_a_later_generation_adds_again_ends() {
+    async fn a_pipeline_added_after_the_removal_it_follows_waits_for_it() {
         let store = ScriptedStore::new();
         let (r, fs, seen) = x_with_its_drain_held(&store).await;
-        let (mut y, docs) = pipelines(&["y"]).remove(0);
-        y.source.postgres.as_mut().unwrap().slot = "x_slot".into();
-        replace_x(&r, y, docs);
-        eventually("y in", || r.statuses().contains_key("y")).await;
-        tokio::time::sleep(Duration::from_secs(10)).await;
-        assert_eq!(fs["y"].builds(), 0, "y waits for x");
-        let (x, x_docs) = pipelines(&["x"]).remove(0);
+        removals(&store).record(&tombstone("x")).await.unwrap();
         let mut c = cluster();
         c.drain_timeout = Duration::from_secs(60);
         assert!(r.reloader().apply(Reload {
-            pipelines: vec![PipelineReload::Add(x, x_docs)],
+            pipelines: vec![PipelineReload::Remove("x".into())],
+            cluster: c.clone(),
+        }));
+        let (mut y, docs) = pipelines(&["y"]).remove(0);
+        y.source.postgres.as_mut().unwrap().slot = "x_slot".into();
+        assert!(r.reloader().apply(Reload {
+            pipelines: vec![PipelineReload::Add(y, docs)],
             cluster: c,
         }));
+        eventually("y in", || r.statuses().contains_key("y")).await;
+        tokio::time::sleep(Duration::from_secs(30)).await;
+        assert_eq!(fs["y"].builds(), 0, "not while x's removal runs");
+        fs["x"].sink.release_commits();
         until(r.statuses().get_mut("y").unwrap(), "y running", running).await;
         assert_eq!(
-            fs["x"].sink.waiting_commits(),
-            1,
-            "x's removal still drains"
+            *seen.lock().unwrap(),
+            [1],
+            "x torn down before y's first build"
         );
-        assert_eq!(*seen.lock().unwrap(), [0]);
-        fs["x"].sink.release_commits();
         r.shutdown().await;
     }
 
-    /// An `Add` for a pipeline that runs already (not a change a diff makes) restarts it with the
-    /// added config under the same lease: nothing is torn down.
+    /// DA-OPS-10: a node that starts while a removal it doesn't run is under way (here a
+    /// crashed holder's, its lease held) waits for it before it runs the pipeline that takes
+    /// the removed one's place, and runs the others at once.
     #[tokio::test(start_paused = true)]
-    async fn an_add_for_a_running_pipeline_restarts_it() {
+    async fn a_node_started_during_a_removal_waits_for_it() {
         let store = ScriptedStore::new();
-        let (fs, d) = factories(&store, &["p"]);
-        let f = fs["p"].clone();
-        let started = pipelines(&["p"]);
-        let (mut p, docs) = started[0].clone();
+        let (fs, d) = factories(&store, &["x", "y", "z"]);
+        leases(&store)
+            .acquire(
+                "x",
+                &crate::test_support::holder("n9"),
+                timing().duration,
+                chrono::Utc::now(),
+            )
+            .await
+            .unwrap();
+        removals(&store).record(&tombstone("x")).await.unwrap();
+        let mut ps = pipelines(&["y", "z"]);
+        ps[0].0.source.postgres.as_mut().unwrap().slot = "x_slot".into();
         let (_, view) = watch::channel(alone("n1"));
-        let r = ClusterReconciler::start(started, &d, view, reconciler_settings());
-        until(r.statuses().get_mut("p").unwrap(), "running", running).await;
-        let before = leases(&store).observe("p").await.unwrap().unwrap();
-        p.name = "renamed".into();
-        assert!(r.reloader().apply(Reload {
-            pipelines: vec![PipelineReload::Add(p, docs)],
-            cluster: cluster(),
-        }));
-        eventually("a new worker", || f.builds() >= 2).await;
-        assert_eq!(f.source.teardowns(), 0, "nothing torn down");
-        until(r.statuses().get_mut("p").unwrap(), "running again", running).await;
-        let lease = leases(&store).observe("p").await.unwrap().unwrap();
-        assert!(!lease.doc.released);
+        let r = ClusterReconciler::start(ps, &d, view, reconciler_settings());
+        until(r.statuses().get_mut("z").unwrap(), "z running", running).await;
+        assert_eq!(fs["y"].builds(), 0, "y waits for x's removal");
+        until(r.statuses().get_mut("y").unwrap(), "y running", running).await;
         assert_eq!(
-            lease.doc.owner_epoch, before.doc.owner_epoch,
-            "the same lease"
+            fs["x"].source.teardowns(),
+            1,
+            "x torn down by the sweep first"
         );
-        assert_eq!(f.builds(), 2);
-        let text = d.metrics.encode();
-        assert!(text.contains(r#"pipeline="renamed""#), "{text}");
+        assert_eq!(removals(&store).pending("x").await.unwrap(), None);
         r.shutdown().await;
     }
 
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-cluster --no-fail-fast
```

Expected: FAIL, the new tests:
  - `controller::tests::a_follower_removed_while_it_waits_ends`
  - `controller::tests::a_pipeline_waits_until_its_own_id_s_removal_is_done`
  - `reconciler::tests::a_node_started_during_a_removal_waits_for_it`
  - `reconciler::tests::a_pipeline_added_after_the_removal_it_follows_waits_for_it`

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 109a295..19b2750 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -6,10 +6,10 @@
 //! `status.json`, when it changed, takes the restart and the flush the management API asked for
 //! in `requests.json` (D-M9a.2), and follows the on-demand snapshot jobs to their end.
 
-use std::collections::{BTreeMap, BTreeSet, HashSet};
+use std::collections::{BTreeMap, HashSet};
 use std::fmt::Debug;
+use std::sync::Arc;
 use std::sync::atomic::AtomicBool;
-use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
 use std::time::{Duration, SystemTime, UNIX_EPOCH};
 
 use data_avalanche_config::Pipeline;
@@ -22,7 +22,6 @@ use data_avalanche_state::{
     ActualState, DesiredState, Holder, Leases, PipelineDocs, RemovalDoc, Removals, RequestsDoc,
     StatusDoc,
 };
-use data_avalanche_storage::ObjectVersion;
 use data_avalanche_telemetry::{
     Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, RowsRead, SnapshotState, Totals,
 };
@@ -31,6 +30,7 @@ use tokio::task::JoinHandle;
 use tokio::time::{Instant, MissedTickBehavior};
 
 use crate::jobs::Jobs;
+use crate::reconciler::shares;
 use crate::{Claim, Claimed, LeaseRun, LeaseTiming, RenewEnd, RenewState, release};
 
 /// A worker's source and sink, built afresh for every run.
@@ -199,42 +199,8 @@ pub struct PipelineController {
     /// channel never closes.
     updates: mpsc::UnboundedReceiver<Update>,
     updater: mpsc::UnboundedSender<Update>,
-    /// The removed pipelines this one takes the place of: their leases are let go of before
-    /// this one's is first claimed ([`Self::following`]).
-    following: Following,
     removals: Removals,
-}
-
-/// The ids of the removed pipelines a pipeline takes the place of (config §9.1: an `id` change
-/// is delete, then recreate), shared by its reconciler and its controller. The controller clears
-/// it once their leases are let go of, the reconciler drops an id a later generation adds again,
-/// and a controller started again after a panic waits only for what is left.
-#[derive(Debug, Clone, Default)]
-pub(crate) struct Following(Arc<Mutex<BTreeSet<String>>>);
-
-impl Following {
-    pub(crate) fn new(ids: impl IntoIterator<Item = String>) -> Self {
-        Self(Arc::new(Mutex::new(ids.into_iter().collect())))
-    }
-
-    /// The ids, as of now.
-    pub(crate) fn ids(&self) -> Vec<String> {
-        self.lock().iter().cloned().collect()
-    }
-
-    fn clear(&self) {
-        self.lock().clear();
-    }
-
-    /// Ends the wait on `id`: a followed id that a later generation adds again names another
-    /// pipeline, the one this pipeline took the place of having been removed in an earlier one.
-    pub(crate) fn forget(&self, id: &str) {
-        self.lock().remove(id);
-    }
-
-    fn lock(&self) -> MutexGuard<'_, BTreeSet<String>> {
-        self.0.lock().unwrap_or_else(PoisonError::into_inner)
-    }
+    removed: RemovedPipelines,
 }
 
 /// Why the owned phase ended.
@@ -492,20 +458,11 @@ impl PipelineController {
             status,
             updates,
             updater,
-            following: Following::default(),
             removals: deps.removals.clone(),
+            removed: deps.removed.clone(),
         }
     }
 
-    /// A pipeline that takes the place of the removed pipelines `ids`, in the same reload: a
-    /// changed `id` that keeps the slot, the publication, the sink or the state (config §9.1,
-    /// delete then recreate). Its lease is first claimed once theirs are let go of.
-    #[must_use]
-    pub(crate) fn following(mut self, following: Following) -> Self {
-        self.following = following;
-        self
-    }
-
     /// Where a reload's updates for this controller go.
     pub fn updater(&self) -> mpsc::UnboundedSender<Update> {
         self.updater.clone()
@@ -520,6 +477,7 @@ impl PipelineController {
             self.holder.clone(),
             self.settings.timing,
         );
+        let mut followed = false;
         loop {
             if *target.borrow_and_update() == Target::DontOwn {
                 self.standby(None);
@@ -529,9 +487,9 @@ impl PipelineController {
                 }
                 continue;
             }
-            if !self.following.ids().is_empty() {
+            if !followed {
                 match self.follow(&mut target).await {
-                    Followed::LetGo => {}
+                    Followed::LetGo => followed = true,
                     Followed::DontOwn => continue,
                     Followed::End => return,
                 }
@@ -590,74 +548,50 @@ impl PipelineController {
         }
     }
 
-    /// Waits until the lease of every pipeline this one takes the place of is let go of: absent,
-    /// released, or unchanged for a full `lease_duration` on this node's clock (its holder is
-    /// gone; the takeover's rule, Clustering §5.3). The leases are read every
-    /// `renewal_interval`, the ids as the reconciler leaves them; meanwhile the controller is on
-    /// standby, and takes its target and its updates. Past three lease durations the wait is
-    /// logged as a warning, once a lease duration.
+    /// Before its first claim, waits while a removal it follows is under way (config §12.3,
+    /// DA-OPS-10): the removal of its own id, or of a pipeline it takes the place of ([`shares`]:
+    /// an `id` changed, delete then recreate, keeping the slot, the publication, the sink or the
+    /// state). The removal's tombstone says when it is done, whichever node does it, and whether
+    /// the removal came in this generation, an earlier one, or before this node started. The
+    /// tombstones are read every `renewal_interval`; meanwhile the controller is on standby, and
+    /// takes its target and its updates. Past three lease durations the wait is logged as a
+    /// warning, once a lease duration.
     async fn follow(&mut self, target: &mut watch::Receiver<Target>) -> Followed {
-        tracing::info!(
-            pipeline = %self.pipeline.name,
-            follows = ?self.following.ids(),
-            "pipeline waits for the removal of the pipelines it takes the place of"
-        );
-        self.standby(None);
         let timing = self.settings.timing;
         let started = Instant::now();
         let mut warned: Option<Instant> = None;
-        // Each held lease's version, and when this node first read it.
-        let mut watched: BTreeMap<String, (ObjectVersion, Instant)> = BTreeMap::new();
+        let mut first = true;
         loop {
-            // Each id still held, with its holder.
-            let mut held: BTreeMap<String, String> = BTreeMap::new();
-            for id in self.following.ids() {
-                match self.leases.observe(&id).await {
-                    Ok(None) => {}
-                    Ok(Some(lease)) if lease.doc.released => {}
-                    Ok(Some(lease)) => {
-                        let read = Instant::now();
-                        let since = match watched.get(&id) {
-                            Some((version, since)) if *version == lease.version => *since,
-                            _ => {
-                                watched.insert(id.clone(), (lease.version, read));
-                                read
-                            }
-                        };
-                        if read < since + timing.duration {
-                            held.insert(id, lease.doc.holder_node_id);
-                        }
-                    }
-                    Err(e) => {
-                        tracing::warn!(
-                            pipeline = %self.pipeline.name,
-                            follows = %id,
-                            "the lease of a pipeline this one takes the place of unread: {e}"
-                        );
-                        held.insert(id, "unread".into());
-                    }
+            let held = self.followed().await;
+            if held.is_empty() {
+                if !first {
+                    tracing::info!(
+                        pipeline = %self.pipeline.name,
+                        "the removals this pipeline follows are done: it claims its lease"
+                    );
                 }
+                return Followed::LetGo;
+            }
+            if first {
+                first = false;
+                tracing::info!(
+                    pipeline = %self.pipeline.name,
+                    follows = ?held,
+                    "pipeline waits for the removals it follows"
+                );
+                self.standby(None);
             }
             let now = Instant::now();
-            if !held.is_empty()
-                && now >= started + timing.duration * 3
+            if now >= started + timing.duration * 3
                 && warned.is_none_or(|at| now >= at + timing.duration)
             {
                 warned = Some(now);
                 tracing::warn!(
                     pipeline = %self.pipeline.name,
                     waiting_for = ?held,
-                    "pipeline still waits for the removal of the pipelines it takes the place of: their leases are held"
+                    "pipeline still waits for the removals it follows"
                 );
             }
-            if held.is_empty() {
-                self.following.clear();
-                tracing::info!(
-                    pipeline = %self.pipeline.name,
-                    "the pipelines this one takes the place of let go: it claims its lease"
-                );
-                return Followed::LetGo;
-            }
             tokio::select! {
                 () = tokio::time::sleep(timing.renewal_interval) => {}
                 r = target.changed() => {
@@ -673,6 +607,39 @@ impl PipelineController {
         }
     }
 
+    /// The removals under way this pipeline follows, by id, each with why. A tombstone that
+    /// doesn't resolve here can't be compared, and isn't followed (logged); unread tombstones
+    /// are followed, as "unread".
+    async fn followed(&self) -> BTreeMap<String, String> {
+        let pending = match self.removals.all_pending().await {
+            Ok(pending) => pending,
+            Err(e) => {
+                tracing::warn!(pipeline = %self.pipeline.name, "the removal tombstones unread: {e}");
+                return BTreeMap::from([("*".to_owned(), "unread".to_owned())]);
+            }
+        };
+        let mut held = BTreeMap::new();
+        for doc in pending {
+            let id = doc.pipeline_id.clone();
+            if id == self.pipeline.id {
+                held.insert(id, "its own id".to_owned());
+                continue;
+            }
+            match self.removed.resolve(doc).await {
+                Ok((removed, _)) if shares(&removed, &self.pipeline) => {
+                    held.insert(id, "takes its place".to_owned());
+                }
+                Ok(_) => {}
+                Err(e) => tracing::warn!(
+                    pipeline = %self.pipeline.name,
+                    removed = %id,
+                    "a removal under way isn't compared with this pipeline: it doesn't resolve here: {e}"
+                ),
+            }
+        }
+        held
+    }
+
     /// A reload's update while this node holds no lease for the pipeline: true when it removed
     /// the pipeline.
     fn take(&mut self, update: Update) -> bool {
```

`crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 3a5018a..bd21c16 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -13,7 +13,6 @@ use tokio::sync::{Semaphore, mpsc, oneshot, watch};
 use tokio::task::{JoinHandle, JoinSet};
 use tokio::time::{Instant, MissedTickBehavior};
 
-use crate::controller::Following;
 use crate::{
     Actual, ClusterError, ControllerDeps, ControllerStatus, Members, PipelineController, Placement,
     Target, Update,
@@ -107,9 +106,6 @@ impl Reloader {
 struct Controlled {
     pipeline: Pipeline,
     docs: PipelineDocs,
-    /// The removed pipelines it takes the place of ([`PipelineController::following`]), shared
-    /// with its controller: one started again after a panic waits for what is left of them.
-    following: Following,
     target: watch::Sender<Target>,
     status: watch::Sender<ControllerStatus>,
     task: JoinHandle<()>,
@@ -125,16 +121,10 @@ impl Drop for Controlled {
 }
 
 impl Controlled {
-    fn new(
-        pipeline: Pipeline,
-        docs: PipelineDocs,
-        following: Following,
-        deps: &ControllerDeps,
-    ) -> Self {
+    fn new(pipeline: Pipeline, docs: PipelineDocs, deps: &ControllerDeps) -> Self {
         let mut c = Self {
             pipeline,
             docs,
-            following,
             target: watch::channel(Target::DontOwn).0,
             status: watch::channel(ControllerStatus::default()).0,
             task: tokio::spawn(async {}),
@@ -150,8 +140,7 @@ impl Controlled {
             self.docs.clone(),
             deps,
             self.status.clone(),
-        )
-        .following(self.following.clone());
+        );
         self.updates = controller.updater();
         self.task = tokio::spawn(controller.run(self.target.subscribe()));
     }
@@ -241,10 +230,7 @@ impl ClusterReconciler {
             .into_iter()
             .map(|(pipeline, docs)| {
                 let id = pipeline.id.clone();
-                (
-                    id,
-                    Controlled::new(pipeline, docs, Following::default(), deps),
-                )
+                (id, Controlled::new(pipeline, docs, deps))
             })
             .collect();
         let (statuses_tx, statuses) = watch::channel(status_map(&controllers));
@@ -316,10 +302,10 @@ impl ClusterReconciler {
     }
 }
 
-/// True when `added` takes a place `removed` held, so that it starts only once `removed` is let go
-/// of: an `id` changed, delete then recreate (config §9.1). The same PostgreSQL slot or
-/// publication on the same server, the same sink location, or the same state location.
-fn shares(removed: &Pipeline, added: &Pipeline) -> bool {
+/// True when `added` takes a place `removed` held, so that it starts only once `removed`'s
+/// removal is done: an `id` changed, delete then recreate (config §9.1). The same PostgreSQL
+/// slot or publication on the same server, the same sink location, or the same state location.
+pub(crate) fn shares(removed: &Pipeline, added: &Pipeline) -> bool {
     let (a, b) = (&removed.source, &added.source);
     let slot = match (&a.postgres, &b.postgres) {
         (Some(x), Some(y)) => {
@@ -395,28 +381,10 @@ impl Loop {
                     .send(Update::DrainTimeout(c.drain_timeout));
             }
         }
-        // The pipelines this reload removes, as configured: an added one that takes the place of
-        // one of them follows it.
-        let removed: Vec<Pipeline> = reload
-            .pipelines
-            .iter()
-            .filter_map(|change| match change {
-                PipelineReload::Remove(id) => match self.controllers.get(id) {
-                    Some(c) => Some(c.pipeline.clone()),
-                    None => self.returning.get(id)?.as_ref().map(|(p, _)| p.clone()),
-                },
-                _ => None,
-            })
-            .collect();
         for change in reload.pipelines {
             match change {
                 PipelineReload::Add(pipeline, docs) => {
                     let id = pipeline.id.clone();
-                    // A followed id that a later generation adds again names another pipeline:
-                    // the one each follower took the place of was removed in an earlier one.
-                    for c in self.controllers.values() {
-                        c.following.forget(&id);
-                    }
                     // Not a change a diff makes. A removal would tear down what the source
                     // created for the pipeline; a restart under its lease leaves it.
                     if let Some(c) = self.controllers.get_mut(&id) {
@@ -435,13 +403,7 @@ impl Loop {
                         *back = Some((pipeline, docs));
                     } else {
                         tracing::info!(pipeline = %pipeline.name, "pipeline added");
-                        let following = Following::new(
-                            removed
-                                .iter()
-                                .filter(|r| r.id != id && shares(r, &pipeline))
-                                .map(|r| r.id.clone()),
-                        );
-                        let added = Controlled::new(pipeline, docs, following, &self.deps);
+                        let added = Controlled::new(pipeline, docs, &self.deps);
                         self.controllers.insert(id, added);
                     }
                 }
@@ -508,7 +470,7 @@ impl Loop {
     fn removed(&mut self, id: &str) {
         if let Some(Some((pipeline, docs))) = self.returning.remove(id) {
             tracing::info!(pipeline = %pipeline.name, "pipeline added back, its removal done");
-            let added = Controlled::new(pipeline, docs, Following::default(), &self.deps);
+            let added = Controlled::new(pipeline, docs, &self.deps);
             self.controllers.insert(id.to_owned(), added);
             self.statuses.send_replace(status_map(&self.controllers));
         }
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-cluster --no-fail-fast
```

Expected: PASS, 149 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/controller.rs \
  crates/data-avalanche-cluster/src/reconciler.rs
git commit -m "feat(cluster): a pipeline waits for every pending removal of its id or its resources, on every node (DA-OPS-10)"
```

---

### Task 5: Restart-only changes on accept (`config`, `cluster`, `api`)

**Files:**
- Modify: `crates/data-avalanche-api/openapi.json`, `crates/data-avalanche-api/src/config.rs`, `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-cluster/src/cluster_tests.rs`, `crates/data-avalanche-cluster/src/coordinator.rs`, `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-config/src/diff.rs`, `crates/data-avalanche-config/src/lib.rs`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`
- Test: `crates/data-avalanche-api/src/config.rs`, `crates/data-avalanche-cluster/src/coordinator.rs`, `crates/data-avalanche-config/tests/diff.rs`, `crates/data-avalanche/tests/e2e/cluster.rs`, `crates/data-avalanche/tests/e2e/reload.rs`

**Gaps:** DA-OPS-12.

**Interfaces:**
- Consumes: `ConfigDiff`, V-061 (M8b); `Coordinator::reload`, `adopt` (M8b); the reload route.
- Produces:
  - `config`: `FIXED_FIELDS` (`config_version`, `globals.state`, `globals.cloud`, `globals.azure`, `globals.cluster.tokens`, `globals.cluster.lease_duration`, `globals.cluster.lock_backend`) and `ConfigDiff::fixed() -> Vec<&str>`; V-061's message says how to accept, or that the field is kept for good;
  - `cluster`: `enum RestartFields { Refuse, Accept }`, `Coordinator::reload(text, restart)`, `Reloaded.restart_pending: Vec<String>`, `Coordinator::restart_pending()`; `adopt` no longer refuses a PROC change, it keeps the started value and lists the field;
  - `api`: `POST /v1/config/reload?restart=accept` (any other query is `400`), `restart_pending` in the reload's answer and in `GET /v1/config/status`.

A process-restart key written as a literal couldn't change once the cluster had a
generation: a reload refused it, and a restarted node adopted the stored generation, not its file
(Q-M12b-2). A reload asked with `restart=accept` now publishes it. Each running node adopts the
generation and keeps the values it started with for those fields: the coordinator compares the
generation with the config the node started on (`Running.started`) and lists the fields that
differ. A node's next start resolves the generation and runs it whole. The cluster's own fields
(where its state lives, Azure's storage account, the anchor's values) are refused even then: a new
value is a new cluster.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/config.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index 887f823..1247e1b 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -313,12 +313,15 @@ mod tests {
         assert_eq!(code, 200);
         assert_eq!(
             v,
-            json!({ "generation": 1, "applied": 1, "nodes": { "node-a": 1 }, "converged": true })
+            json!({ "generation": 1, "applied": 1, "nodes": { "node-a": 1 }, "converged": true, "restart_pending": [] })
         );
         let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
         assert_eq!(
             (code, v),
-            (200, json!({ "generation": 1, "unchanged": true }))
+            (
+                200,
+                json!({ "generation": 1, "unchanged": true, "restart_pending": [] })
+            )
         );
 
         std::fs::write(
@@ -327,7 +330,10 @@ mod tests {
         )
         .unwrap();
         let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
-        assert_eq!((code, v), (202, json!({ "generation": 2 })));
+        assert_eq!(
+            (code, v),
+            (202, json!({ "generation": 2, "restart_pending": [] }))
+        );
         let mut status = Value::Null;
         for _ in 0..500 {
             status = json_of(addr, "GET", "/v1/config/status").await.1;
@@ -338,7 +344,7 @@ mod tests {
         }
         assert_eq!(
             status,
-            json!({ "generation": 2, "applied": 2, "nodes": { "node-a": 2 }, "converged": true })
+            json!({ "generation": 2, "applied": 2, "nodes": { "node-a": 2 }, "converged": true, "restart_pending": [] })
         );
 
         std::fs::write(
@@ -353,6 +359,33 @@ mod tests {
             v["report"]["errors"][0]["path"], "globals.cluster.tokens",
             "{v}"
         );
+        let (code, v) = json_of(addr, "POST", "/v1/config/reload?restart=accept").await;
+        assert_eq!(code, 422, "the anchor's tokens, even accepted: {v}");
+
+        // DA-OPS-12: a restart-only field, accepted, is published and pending here.
+        std::fs::write(
+            &file,
+            CONFIG.replace("lease_renewal_interval: 10s", "lease_renewal_interval: 8s"),
+        )
+        .unwrap();
+        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
+        assert_eq!(code, 422, "{v}");
+        let (code, _) = json_of(addr, "POST", "/v1/config/reload?restart=later").await;
+        assert_eq!(code, 400);
+        let (code, v) = json_of(addr, "POST", "/v1/config/reload?restart=accept").await;
+        assert_eq!(
+            (code, v),
+            (
+                202,
+                json!({ "generation": 3, "restart_pending": ["globals.cluster.lease_renewal_interval"] })
+            )
+        );
+        let v = json_of(addr, "GET", "/v1/config/status").await.1;
+        assert_eq!(
+            v["restart_pending"],
+            json!(["globals.cluster.lease_renewal_interval"]),
+            "{v}"
+        );
 
         std::fs::write(&file, "pipelines: [").unwrap();
         let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
@@ -362,6 +395,6 @@ mod tests {
         coordinator.stop().await;
         let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
         assert_eq!(code, 503, "{v}");
-        assert_eq!(coordinator.latest().await.unwrap(), Some(2));
+        assert_eq!(coordinator.latest().await.unwrap(), Some(3));
     }
 }
```

`crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index aa64cef..c0b8e9b 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -991,12 +991,17 @@ mod tests {
             "  state: { bucket: app-state }\n",
             "  state: { bucket: app-state }\n  log: { level: debug }\n",
         );
-        let out = n.coordinator.reload(&text).await.unwrap();
+        let out = n
+            .coordinator
+            .reload(&text, RestartFields::Refuse)
+            .await
+            .unwrap();
         assert_eq!(
             out,
             Reloaded {
                 generation: 2,
-                published: true
+                published: true,
+                restart_pending: vec![],
             }
         );
         let pointer = n.generations.current().await.unwrap().unwrap().pointer;
@@ -1021,12 +1026,17 @@ mod tests {
     #[tokio::test(start_paused = true)]
     async fn an_unchanged_file_publishes_nothing() {
         let n = lone(&["p"]).await;
-        let out = n.coordinator.reload(&config_text(&["p"])).await.unwrap();
+        let out = n
+            .coordinator
+            .reload(&config_text(&["p"]), RestartFields::Refuse)
+            .await
+            .unwrap();
         assert_eq!(
             out,
             Reloaded {
                 generation: 1,
-                published: false
+                published: false,
+                restart_pending: vec![],
             }
         );
         let current = n.generations.current().await.unwrap().unwrap();
@@ -1042,7 +1052,7 @@ mod tests {
             "cluster: { memberlist: { node_name: n1 } }",
             "cluster: { tokens: 64, memberlist: { node_name: n1 } }",
         );
-        match n.coordinator.reload(&tokens).await {
+        match n.coordinator.reload(&tokens, RestartFields::Refuse).await {
             Err(ReloadError::Rejected(report)) => {
                 assert!(
                     report
@@ -1055,13 +1065,76 @@ mod tests {
             other => panic!("{other:?}"),
         }
         assert!(matches!(
-            n.coordinator.reload("pipelines: [").await,
+            n.coordinator
+                .reload("pipelines: [", RestartFields::Refuse)
+                .await,
             Err(ReloadError::Invalid(_))
         ));
         let current = n.generations.current().await.unwrap().unwrap();
         assert_eq!(current.pointer.doc.generation, 1);
     }
 
+    /// DA-OPS-12: a restart-only field is published when the reload accepts it. Every node
+    /// adopts the generation and keeps the field pending until its next start; a node started on
+    /// the generation has nothing pending. A field the cluster keeps for good (the anchor's
+    /// `tokens`) is refused all the same.
+    #[tokio::test(start_paused = true)]
+    async fn a_restart_field_is_published_when_accepted_and_pending_until_the_next_start() {
+        let mut n = lone(&["p"]).await;
+        let renew = config_text(&["p"]).replace(
+            "cluster: { memberlist: { node_name: n1 } }",
+            "cluster: { lease_renewal_interval: 4s, memberlist: { node_name: n1 } }",
+        );
+        assert!(matches!(
+            n.coordinator.reload(&renew, RestartFields::Refuse).await,
+            Err(ReloadError::Rejected(_))
+        ));
+        let out = n
+            .coordinator
+            .reload(&renew, RestartFields::Accept)
+            .await
+            .unwrap();
+        assert_eq!(
+            out,
+            Reloaded {
+                generation: 2,
+                published: true,
+                restart_pending: vec!["globals.cluster.lease_renewal_interval".into()],
+            }
+        );
+        assert!(n.reload().await.pipelines.is_empty());
+        assert_eq!(n.coordinator.applied(), 2);
+        assert_eq!(
+            n.coordinator.restart_pending().await,
+            ["globals.cluster.lease_renewal_interval"]
+        );
+        let tokens = renew.replace("cluster: {", "cluster: { tokens: 64,");
+        assert!(matches!(
+            n.coordinator.reload(&tokens, RestartFields::Accept).await,
+            Err(ReloadError::Rejected(_))
+        ));
+        let started = start(&renew, Vars::default(), Racing::new()).await;
+        assert!(started.coordinator.restart_pending().await.is_empty());
+    }
+
+    /// A generation published elsewhere that changes a restart-only field is adopted: its other
+    /// changes apply, the field waits for the next start.
+    #[tokio::test(start_paused = true)]
+    async fn a_generation_with_a_restart_field_is_adopted_with_it_pending() {
+        let mut n = lone(&["p"]).await;
+        let text = config_text(&["p", "q"]).replace(
+            "cluster: { memberlist: { node_name: n1 } }",
+            "cluster: { lease_renewal_interval: 4s, memberlist: { node_name: n1 } }",
+        );
+        assert_eq!(n.publish_elsewhere(&text).await, 2);
+        assert_eq!(kinds(&n.reload().await), ["add q"]);
+        assert_eq!(n.coordinator.applied(), 2);
+        assert_eq!(
+            n.coordinator.restart_pending().await,
+            ["globals.cluster.lease_renewal_interval"]
+        );
+    }
+
     /// A reload while another node's newer generation isn't adopted yet: it is adopted first,
     /// the file is checked against it, and published on top.
     #[tokio::test(start_paused = true)]
@@ -1070,7 +1143,7 @@ mod tests {
         n.publish_elsewhere(&config_text(&["p", "q"])).await;
         let out = n
             .coordinator
-            .reload(&config_text(&["p", "q", "r"]))
+            .reload(&config_text(&["p", "q", "r"]), RestartFields::Refuse)
             .await
             .unwrap();
         assert_eq!(out.generation, 3);
@@ -1104,10 +1177,16 @@ mod tests {
     async fn a_tables_change_restarts_only_when_the_set_changes() {
         let mut n = lone(&["p"]).await;
         let same = config_text(&["p"]).replace("[public.*]", "[public.a, public.b]");
-        n.coordinator.reload(&same).await.unwrap();
+        n.coordinator
+            .reload(&same, RestartFields::Refuse)
+            .await
+            .unwrap();
         assert_eq!(kinds(&n.reload().await), ["reconfigure p"]);
         let fewer = config_text(&["p"]).replace("[public.*]", "[public.a]");
-        n.coordinator.reload(&fewer).await.unwrap();
+        n.coordinator
+            .reload(&fewer, RestartFields::Refuse)
+            .await
+            .unwrap();
         assert_eq!(kinds(&n.reload().await), ["restart p"]);
     }
 
@@ -1122,7 +1201,7 @@ mod tests {
         assert!(n.reloads.try_recv().is_err(), "nothing applied");
         for text in [config_text(&["p", "r"]), "pipelines: [".to_owned()] {
             assert!(matches!(
-                n.coordinator.reload(&text).await,
+                n.coordinator.reload(&text, RestartFields::Refuse).await,
                 Err(ReloadError::Stopped)
             ));
         }
@@ -1143,14 +1222,15 @@ mod tests {
         racing.arm(&config_text(&["p", "q"]));
         let out = n
             .coordinator
-            .reload(&config_text(&["p", "q", "r"]))
+            .reload(&config_text(&["p", "q", "r"]), RestartFields::Refuse)
             .await
             .unwrap();
         assert_eq!(
             out,
             Reloaded {
                 generation: 3,
-                published: true
+                published: true,
+                restart_pending: vec![],
             }
         );
         assert_eq!(kinds(&n.reload().await), ["add q"]);
@@ -1171,18 +1251,26 @@ mod tests {
         vars.set("P_PASSWORD", "one");
         let mut n = start(&text, vars, Racing::new()).await;
         assert_eq!(
-            n.coordinator.reload(&text).await.unwrap(),
+            n.coordinator
+                .reload(&text, RestartFields::Refuse)
+                .await
+                .unwrap(),
             Reloaded {
                 generation: 1,
-                published: false
+                published: false,
+                restart_pending: vec![],
             }
         );
         n.vars.set("P_PASSWORD", "two");
         assert_eq!(
-            n.coordinator.reload(&text).await.unwrap(),
+            n.coordinator
+                .reload(&text, RestartFields::Refuse)
+                .await
+                .unwrap(),
             Reloaded {
                 generation: 2,
-                published: true
+                published: true,
+                restart_pending: vec![],
             }
         );
         assert_eq!(kinds(&n.reload().await), ["restart p"]);
```

`crates/data-avalanche-config/tests/diff.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/diff.rs b/crates/data-avalanche-config/tests/diff.rs
index cd121ba..898058d 100644
--- a/crates/data-avalanche-config/tests/diff.rs
+++ b/crates/data-avalanche-config/tests/diff.rs
@@ -85,6 +85,41 @@ fn proc_change_rejects_the_reload_with_v061() {
         ("V-061", "globals.cluster.tokens")
     );
     assert_eq!(d.globals[0].class, Mutability::Proc);
+    assert_eq!(d.fixed(), ["globals.cluster.tokens"], "the anchor pins it");
+    assert!(e.message.contains("new globals.state"), "{}", e.message);
+}
+
+/// DA-OPS-12: a restart-only field a restart can change isn't fixed; where the state lives and
+/// what the anchor pins are.
+#[test]
+fn only_the_cluster_s_own_fields_are_fixed() {
+    let with = |cluster: &str| {
+        MINIMAL.replace(
+            "state: { bucket: app-state }",
+            &format!("state: {{ bucket: app-state }}\n  cluster: {cluster}"),
+        )
+    };
+    let renew = diff(
+        &load(MINIMAL),
+        &load(&with("{ lease_renewal_interval: 4s }")),
+        &unknown,
+    );
+    assert!(renew.is_rejected());
+    assert!(renew.fixed().is_empty(), "{:?}", renew.globals);
+    let message = &renew.report.errors[0].message;
+    assert!(message.contains("?restart=accept"), "{message}");
+    let moved = MINIMAL.replace("bucket: app-state", "bucket: other-state");
+    let d = diff(&load(MINIMAL), &load(&moved), &unknown);
+    assert_eq!(d.fixed(), ["globals.state.bucket"]);
+    // The storage account holds `globals.state` on Azure: another one is another cluster.
+    let azure = |account: &str| {
+        MINIMAL.replace(
+            "state: { bucket: app-state }",
+            &format!("cloud: azure\n  azure: {{ account_name: {account} }}\n  state: {{ bucket: app-state }}"),
+        )
+    };
+    let d = diff(&load(&azure("one")), &load(&azure("two")), &unknown);
+    assert_eq!(d.fixed(), ["globals.azure.account_name"]);
 }
 
 #[test]
```

`crates/data-avalanche/tests/e2e/cluster.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/cluster.rs b/crates/data-avalanche/tests/e2e/cluster.rs
index f73a55c..8ebafdb 100644
--- a/crates/data-avalanche/tests/e2e/cluster.rs
+++ b/crates/data-avalanche/tests/e2e/cluster.rs
@@ -482,7 +482,10 @@ async fn a_reload_reaches_every_node() {
     let (code, v) = c.nodes[1].proc.as_ref().unwrap().reload().await;
     assert_eq!(
         (code, &v),
-        (202, &serde_json::json!({ "generation": 2 })),
+        (
+            202,
+            &serde_json::json!({ "generation": 2, "restart_pending": [] })
+        ),
         "{v}"
     );
 
```

`crates/data-avalanche/tests/e2e/reload.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/reload.rs b/crates/data-avalanche/tests/e2e/reload.rs
index 01bffe1..4722090 100644
--- a/crates/data-avalanche/tests/e2e/reload.rs
+++ b/crates/data-avalanche/tests/e2e/reload.rs
@@ -54,7 +54,10 @@ async fn a_reload_publishes_the_file_and_a_restart_keeps_the_generation() {
     let (code, v) = p.reload().await;
     assert_eq!(
         (code, &v),
-        (202, &serde_json::json!({ "generation": 2 })),
+        (
+            202,
+            &serde_json::json!({ "generation": 2, "restart_pending": [] })
+        ),
         "{v}"
     );
     let (code, v) = p.reload().await;
@@ -62,7 +65,7 @@ async fn a_reload_publishes_the_file_and_a_restart_keeps_the_generation() {
         (code, &v),
         (
             200,
-            &serde_json::json!({ "generation": 2, "unchanged": true })
+            &serde_json::json!({ "generation": 2, "unchanged": true, "restart_pending": [] })
         ),
         "the file is the generation already"
     );
@@ -105,7 +108,10 @@ async fn a_reload_publishes_the_file_and_a_restart_keeps_the_generation() {
     let (code, v) = p.reload().await;
     assert_eq!(
         (code, &v),
-        (202, &serde_json::json!({ "generation": 3 })),
+        (
+            202,
+            &serde_json::json!({ "generation": 3, "restart_pending": [] })
+        ),
         "{v}"
     );
     p.metric("data_avalanche_config_generation 3").await;
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-config -p data-avalanche-cluster -p data-avalanche-api -p data-avalanche -p xtask --no-fail-fast
cargo test -p data-avalanche --features e2e-hooks --test e2e --no-fail-fast -- --test-threads=1 reload:: cluster::
```

Expected: FAIL to compile:
  - `error[E0599]: no method named `fixed` found for struct `ConfigDiff` in the current scope (crates/data-avalanche-config/tests/diff.rs)`

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/openapi.json`:

```diff
diff --git a/crates/data-avalanche-api/openapi.json b/crates/data-avalanche-api/openapi.json
index 9f29d58..dc52eb1 100644
--- a/crates/data-avalanche-api/openapi.json
+++ b/crates/data-avalanche-api/openapi.json
@@ -152,7 +152,7 @@
         "tags": [
           "config"
         ],
-        "summary": "Publish the node's file as the next config generation",
+        "summary": "Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start",
         "responses": {
           "200": {
             "description": "the file is the generation the cluster runs",
@@ -161,7 +161,13 @@
             }
           },
           "202": {
-            "description": "published: the generation",
+            "description": "published: the generation, and this node's fields pending a restart",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "a query other than restart=accept",
             "content": {
               "application/json": {}
             }
@@ -185,7 +191,7 @@
             }
           },
           "422": {
-            "description": "invalid, or it changes a restart-only field: the report",
+            "description": "invalid, or it changes a restart-only field without restart=accept, or a field the cluster keeps for good: the report",
             "content": {
               "application/json": {}
             }
```

`crates/data-avalanche-api/src/config.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index 1247e1b..52540c8 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -2,10 +2,10 @@
 
 use std::collections::BTreeMap;
 
-use axum::extract::{Extension, State};
+use axum::extract::{Extension, RawQuery, State};
 use axum::http::StatusCode;
 use axum::response::Response;
-use data_avalanche_cluster::{Checked, ReloadError, Reloaded};
+use data_avalanche_cluster::{Checked, ReloadError, Reloaded, RestartFields};
 use data_avalanche_config::Fingerprints;
 use serde_json::json;
 
@@ -96,8 +96,20 @@ pub(crate) async fn validate(
 
 /// Publishes the node's file as the next config generation: 202 with it, 200 when the file is
 /// the generation the cluster runs already, 422 with the validation report when it is invalid or
-/// changes a field only a process restart applies.
-pub(crate) async fn reload(State(api): State<Api>) -> Response {
+/// changes a field only a process restart applies. With `?restart=accept` such a field is
+/// published, and each node applies it at its next start (DA-OPS-12); a field the cluster keeps
+/// for good is refused all the same. Both answers name the fields this node has pending.
+pub(crate) async fn reload(State(api): State<Api>, RawQuery(query): RawQuery) -> Response {
+    let restart = match query.as_deref() {
+        None | Some("") => RestartFields::Refuse,
+        Some("restart=accept") => RestartFields::Accept,
+        Some(other) => {
+            return body(
+                StatusCode::BAD_REQUEST,
+                &json!({ "error": format!("unknown query {other:?}: the reload takes ?restart=accept only") }),
+            );
+        }
+    };
     let Some(node) = api.node.get() else {
         return starting();
     };
@@ -105,21 +117,30 @@ pub(crate) async fn reload(State(api): State<Api>) -> Response {
         Ok(text) => text,
         Err(failed) => return failed.into(),
     };
-    match node.coordinator.reload(&text).await {
+    match node.coordinator.reload(&text, restart).await {
         Ok(Reloaded {
             generation,
             published: true,
-        }) => body(StatusCode::ACCEPTED, &json!({ "generation": generation })),
-        Ok(Reloaded { generation, .. }) => body(
+            restart_pending,
+        }) => body(
+            StatusCode::ACCEPTED,
+            &json!({ "generation": generation, "restart_pending": restart_pending }),
+        ),
+        Ok(Reloaded {
+            generation,
+            restart_pending,
+            ..
+        }) => body(
             StatusCode::OK,
-            &json!({ "generation": generation, "unchanged": true }),
+            &json!({ "generation": generation, "unchanged": true, "restart_pending": restart_pending }),
         ),
         Err(e) => refused(&e),
     }
 }
 
 /// The generation the cluster runs, and each member's applied one: the reload is complete when
-/// every member's is it.
+/// every member's is it. `restart_pending` names this node's restart-only fields that wait for its
+/// next start (DA-OPS-12): each node answers for itself.
 pub(crate) async fn status(State(api): State<Api>) -> Response {
     let Some(node) = api.node.get() else {
         return starting();
@@ -149,6 +170,7 @@ pub(crate) async fn status(State(api): State<Api>) -> Response {
             "applied": applied,
             "nodes": nodes,
             "converged": converged,
+            "restart_pending": node.coordinator.restart_pending().await,
         }),
     )
 }
```

`crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 6390c72..4605ac5 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -132,14 +132,18 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
                 "POST",
                 "/v1/config/reload",
                 Operator,
-                "Publish the node's file as the next config generation",
+                "Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start",
                 &[
                     (200, "the file is the generation the cluster runs"),
-                    (202, "published: the generation"),
+                    (
+                        202,
+                        "published: the generation, and this node's fields pending a restart",
+                    ),
+                    (400, "a query other than restart=accept"),
                     (409, "other nodes published first, every time"),
                     (
                         422,
-                        "invalid, or it changes a restart-only field: the report",
+                        "invalid, or it changes a restart-only field without restart=accept, or a field the cluster keeps for good: the report",
                     ),
                 ],
             ),
```

`crates/data-avalanche-cluster/src/cluster_tests.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/cluster_tests.rs b/crates/data-avalanche-cluster/src/cluster_tests.rs
index 6aefe83..9b6da67 100644
--- a/crates/data-avalanche-cluster/src/cluster_tests.rs
+++ b/crates/data-avalanche-cluster/src/cluster_tests.rs
@@ -207,7 +207,11 @@ async fn a_reload_reaches_every_node_and_each_pipeline_runs_once() {
             "prefix: orders, delta: {}, flush: { seconds: 0, rows: 5 }",
         )
         .replace("prefix: customers,", "prefix: customers-v2,");
-    let out = b.coordinator.reload(&text).await.unwrap();
+    let out = b
+        .coordinator
+        .reload(&text, crate::RestartFields::Refuse)
+        .await
+        .unwrap();
     assert_eq!((out.generation, out.published), (2, true));
     eventually("every node applies generation 2", || {
         nodes.iter().all(|n| n.coordinator.applied() == 2)
```

`crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index c0b8e9b..f583840 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -11,8 +11,8 @@ use std::time::Duration;
 
 use chrono::Utc;
 use data_avalanche_config::{
-    ConfigDiff, Env, FingerprintKey, Fingerprints, Globals, Pipeline, PipelineChange, Report,
-    ResolvedConfig, diff, diff_with, load_posted_str, load_str,
+    ConfigDiff, Env, FingerprintKey, Fingerprints, Globals, Mutability, Pipeline, PipelineChange,
+    Report, ResolvedConfig, diff, diff_with, load_posted_str, load_str,
 };
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{
@@ -68,13 +68,26 @@ impl Debug for CoordinatorDeps {
     }
 }
 
-/// A reload's outcome.
+/// Whether a reload may publish a change to a field only a process restart applies (config
+/// §1.2, V-061; DA-OPS-12).
 #[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum RestartFields {
+    /// Refused, naming the field.
+    Refuse,
+    /// Published: every node applies the generation's other changes at once, and the field at
+    /// its next start, when it adopts the generation (`POST /v1/config/reload?restart=accept`).
+    Accept,
+}
+
+/// A reload's outcome.
+#[derive(Debug, Clone, PartialEq, Eq)]
 pub struct Reloaded {
     /// The generation the cluster runs now.
     pub generation: u64,
     /// False when the file's text is that generation's already: nothing was published.
     pub published: bool,
+    /// The fields this node applies at its next start ([`ConfigCoordinator::restart_pending`]).
+    pub restart_pending: Vec<String>,
 }
 
 /// What a reload would do here ([`ConfigCoordinator::check`]).
@@ -109,6 +122,8 @@ pub enum ReloadError {
 
 /// The generation this node runs.
 struct Running {
+    /// The config the process started with: its restart-only fields are the ones in force.
+    started: ResolvedConfig,
     config: ResolvedConfig,
     generation: Versioned<GenerationDoc>,
     /// A newer generation this node couldn't apply: logged once, tried again at each poll.
@@ -163,6 +178,7 @@ impl ConfigCoordinator {
         let inner = Arc::new(Inner {
             deps,
             running: Mutex::new(Running {
+                started: config.clone(),
                 config,
                 generation,
                 failed: None,
@@ -214,9 +230,16 @@ impl ConfigCoordinator {
     /// generation itself is published again when what it resolves to here changed (a `${VAR}`,
     /// a credentials file): every node resolves it anew.
     ///
+    /// A change to a field only a process restart applies is refused unless `restart` is
+    /// [`RestartFields::Accept`] (DA-OPS-12).
+    ///
     /// # Errors
     /// [`ReloadError`]: nothing was published.
-    pub async fn reload(&self, text: &str) -> Result<Reloaded, ReloadError> {
+    pub async fn reload(
+        &self,
+        text: &str,
+        restart: RestartFields,
+    ) -> Result<Reloaded, ReloadError> {
         let d = &self.inner.deps;
         let mut running = self.inner.running.lock().await;
         if running.stopped {
@@ -240,13 +263,16 @@ impl ConfigCoordinator {
                 self.inner.adopt(&mut running, current.clone()).await;
             }
             let changes = diff(&running.config, &new, &|_, _| None);
-            if changes.is_rejected() {
+            if changes.is_rejected()
+                && (restart == RestartFields::Refuse || !changes.fixed().is_empty())
+            {
                 return Err(ReloadError::Rejected(changes.report));
             }
             if config_hash(text) == current.pointer.doc.hash && changes.is_empty() {
                 return Ok(Reloaded {
                     generation: n,
                     published: false,
+                    restart_pending: running.restart_pending(),
                 });
             }
             match d
@@ -261,6 +287,7 @@ impl ConfigCoordinator {
                     return Ok(Reloaded {
                         generation,
                         published: true,
+                        restart_pending: running.restart_pending(),
                     });
                 }
                 // Another node published first, or the outcome is unknown: read again.
@@ -334,6 +361,13 @@ impl ConfigCoordinator {
         })
     }
 
+    /// The restart-only fields the generation this node runs changed since the process started
+    /// (DA-OPS-12): a published `?restart=accept` reload, or a per-node `${VAR}`. They take
+    /// effect when the node starts again.
+    pub async fn restart_pending(&self) -> Vec<String> {
+        self.inner.running.lock().await.restart_pending()
+    }
+
     /// Stops watching, after the adoption or reload under way: none follows, and a reload is
     /// [`ReloadError::Stopped`].
     pub async fn stop(&self) {
@@ -350,10 +384,21 @@ impl ConfigCoordinator {
     }
 }
 
+impl Running {
+    fn restart_pending(&self) -> Vec<String> {
+        diff(&self.started, &self.config, &|_, _| None)
+            .globals
+            .into_iter()
+            .filter(|f| f.class == Mutability::Proc)
+            .map(|f| f.path)
+            .collect()
+    }
+}
+
 impl Inner {
-    /// Applies generation `g` (config §12): a generation that doesn't load here, or that changes
-    /// a restart-only field as this node resolves it, isn't, and this node runs on what it
-    /// runs.
+    /// Applies generation `g` (config §12): a generation that doesn't load here isn't, and this
+    /// node runs on what it runs. A change to a restart-only field is the next start's
+    /// (DA-OPS-12): the rest applies now, and the node says what waits for its restart.
     async fn adopt(&self, running: &mut Running, g: ConfigGeneration) {
         let d = &self.deps;
         let n = g.pointer.doc.generation;
@@ -375,9 +420,6 @@ impl Inner {
         let changes = diff(&running.config, &new, &|_, p| {
             sets.get(&p.id).copied().flatten()
         });
-        if changes.is_rejected() {
-            return refused(running, lines(&changes.report));
-        }
         let pipelines = match self.pipelines(&new, &changes) {
             Ok(pipelines) => pipelines,
             Err(why) => return refused(running, why),
@@ -413,6 +455,14 @@ impl Inner {
         running.config = new;
         running.generation = g.pointer;
         running.failed = None;
+        let pending = running.restart_pending();
+        if changes.is_rejected() && !pending.is_empty() {
+            tracing::warn!(
+                generation = n,
+                "config generation {n} changes fields this node applies at its next start: {}",
+                pending.join(", ")
+            );
+        }
         self.applied.send_replace(n);
     }
 
```

`crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 7c75ec6..1a8a9db 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -28,7 +28,7 @@ pub use controller::{
 };
 pub use coordinator::{
     Checked, ConfigCoordinator, CoordinatorDeps, DocsFn, PUBLISH_ATTEMPTS, ReloadError, Reloaded,
-    removed_pipelines,
+    RestartFields, removed_pipelines,
 };
 pub use discovery::{Seeds, advertise, discover, interfaces};
 pub use error::ClusterError;
```

`crates/data-avalanche-config/src/diff.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/diff.rs b/crates/data-avalanche-config/src/diff.rs
index 7858ff0..d952bc7 100644
--- a/crates/data-avalanche-config/src/diff.rs
+++ b/crates/data-avalanche-config/src/diff.rs
@@ -50,6 +50,21 @@ pub struct ConfigDiff {
     pub report: Report,
 }
 
+/// Restart-only fields no restart changes in a running cluster (DA-OPS-12): where its state lives
+/// and how it is reached (the cloud, the state's location, Azure's storage account), which each
+/// node reads from its own file before it adopts a generation, and the values the cluster anchor
+/// pins (`tokens`, `lease_duration`, `lock_backend`). A reload never publishes a change to one,
+/// even one that accepts restart-only changes.
+pub const FIXED_FIELDS: &[&str] = &[
+    "config_version",
+    "globals.state",
+    "globals.cloud",
+    "globals.azure",
+    "globals.cluster.tokens",
+    "globals.cluster.lease_duration",
+    "globals.cluster.lock_backend",
+];
+
 impl ConfigDiff {
     pub fn is_empty(&self) -> bool {
         self.globals.is_empty() && self.pipelines.is_empty()
@@ -59,6 +74,23 @@ impl ConfigDiff {
     pub fn is_rejected(&self) -> bool {
         !self.report.is_ok()
     }
+
+    /// The changed fields of [`FIXED_FIELDS`].
+    pub fn fixed(&self) -> Vec<&str> {
+        self.globals
+            .iter()
+            .map(|f| f.path.as_str())
+            .filter(|path| is_fixed(path))
+            .collect()
+    }
+}
+
+/// `path` (dotted) is one of [`FIXED_FIELDS`], or inside one.
+fn is_fixed(path: &str) -> bool {
+    FIXED_FIELDS.iter().any(|fixed| {
+        path.strip_prefix(fixed)
+            .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
+    })
 }
 
 /// Flattens a serialized value into `path → leaf`. Objects (structs and maps) recurse; arrays are
@@ -177,9 +209,15 @@ pub fn diff_with(
                 rule: "V-061".into(),
                 severity: Severity::Error,
                 loc: None,
-                message: format!(
-                    "{text} changed: it needs a process restart, so the reload is rejected"
-                ),
+                message: if is_fixed(&text) {
+                    format!(
+                        "{text} changed: the cluster keeps it for good, so the reload is rejected; a new value needs a new globals.state, with every node stopped"
+                    )
+                } else {
+                    format!(
+                        "{text} changed: it needs a process restart, so the reload is rejected; reload with ?restart=accept to publish it, and each node applies it at its next start"
+                    )
+                },
             });
         }
         out.globals.push(FieldChange { path: text, class });
```

`crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 7ed1fec..688700d 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -93,7 +93,9 @@ mod walk;
 pub use build::derived_server_id;
 pub use coerce::LogFilter;
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
-pub use diff::{ConfigDiff, FieldChange, Fingerprints, PipelineChange, diff, diff_with};
+pub use diff::{
+    ConfigDiff, FIXED_FIELDS, FieldChange, Fingerprints, PipelineChange, diff, diff_with,
+};
 pub use external::{
     ClusterAnchor, check_cluster_anchor, check_partitioning, check_table_set,
     check_unbounded_numeric, glob_match, key_table,
```

`docs/public/openapi.json`:

```diff
diff --git a/docs/public/openapi.json b/docs/public/openapi.json
index 9f29d58..dc52eb1 100644
--- a/docs/public/openapi.json
+++ b/docs/public/openapi.json
@@ -152,7 +152,7 @@
         "tags": [
           "config"
         ],
-        "summary": "Publish the node's file as the next config generation",
+        "summary": "Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start",
         "responses": {
           "200": {
             "description": "the file is the generation the cluster runs",
@@ -161,7 +161,13 @@
             }
           },
           "202": {
-            "description": "published: the generation",
+            "description": "published: the generation, and this node's fields pending a restart",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "400": {
+            "description": "a query other than restart=accept",
             "content": {
               "application/json": {}
             }
@@ -185,7 +191,7 @@
             }
           },
           "422": {
-            "description": "invalid, or it changes a restart-only field: the report",
+            "description": "invalid, or it changes a restart-only field without restart=accept, or a field the cluster keeps for good: the report",
             "content": {
               "application/json": {}
             }
```

`docs/src/content/docs/reference/api.md`:

```diff
diff --git a/docs/src/content/docs/reference/api.md b/docs/src/content/docs/reference/api.md
index e4b70c0..ff937e9 100644
--- a/docs/src/content/docs/reference/api.md
+++ b/docs/src/content/docs/reference/api.md
@@ -168,7 +168,7 @@ The config generation the cluster runs: validate, publish, status.
 
 ### `POST /v1/config/reload`
 
-Publish the node's file as the next config generation
+Publish the node's file as the next config generation; `?restart=accept` publishes a restart-only field, applied at each node's next start
 
 Least role: `operator`.
 
@@ -176,9 +176,10 @@ Least role: `operator`.
 | Status | When |
 |---|---|
 | `200` | the file is the generation the cluster runs |
-| `202` | published: the generation |
+| `202` | published: the generation, and this node's fields pending a restart |
+| `400` | a query other than restart=accept |
 | `409` | other nodes published first, every time |
-| `422` | invalid, or it changes a restart-only field: the report |
+| `422` | invalid, or it changes a restart-only field without restart=accept, or a field the cluster keeps for good: the report |
 
 ### `GET /v1/config/status`
 
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-config -p data-avalanche-cluster -p data-avalanche-api -p data-avalanche -p xtask --no-fail-fast
cargo test -p data-avalanche --features e2e-hooks --test e2e --no-fail-fast -- --test-threads=1 reload:: cluster::
```

Expected: PASS, 408 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/openapi.json \
  crates/data-avalanche-api/src/config.rs \
  crates/data-avalanche-api/src/lib.rs \
  crates/data-avalanche-cluster/src/cluster_tests.rs \
  crates/data-avalanche-cluster/src/coordinator.rs \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-config/src/diff.rs \
  crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/tests/diff.rs \
  crates/data-avalanche/tests/e2e/cluster.rs \
  crates/data-avalanche/tests/e2e/reload.rs \
  docs/public/openapi.json \
  docs/src/content/docs/reference/api.md
git commit -m "feat(cluster): a reload with restart=accept publishes restart-only changes, applied at each node's next start (DA-OPS-12)"
```

---

### Task 6: A snapshot job's `truncate` (`core`, `state`, `pipeline`, the sink, `api`)

**Files:**
- Modify: `crates/data-avalanche-api/src/snapshots.rs`, `crates/data-avalanche-core/src/change_set.rs`, `crates/data-avalanche-pipeline/src/jobs.rs`, `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-state/src/snapshot.rs`, `crates/data-avalanche-testkit/src/fake_source.rs`
- Test: `crates/data-avalanche-api/src/snapshots.rs`, `crates/data-avalanche-core/src/change_set.rs`, `crates/data-avalanche-pipeline/src/jobs.rs`, `crates/data-avalanche-sink-delta/src/sink.rs`, `crates/data-avalanche-state/src/snapshot.rs`

**Gaps:** DA-SNAP-15.

**Interfaces:**
- Consumes: `SnapshotJobDoc`, `Jobs` (M9b); `TableChangeSet::validate`, `CommitMarker::covers` (M3); the worker's `reposition` empty commit (M12a).
- Produces:
  - `SnapshotJobDoc.truncate: bool` (`serde(default)`, skipped when false) and `SnapshotJobDoc::truncating(self) -> Self`;
  - `validate` accepts a `snapshot` change set with `truncate` and no rows;
  - `Jobs::truncating() -> BTreeSet<TableRef>`; the worker's `Empty` (`Reposition`, `Truncate`) and `commit_empty`, `reposition` rewritten on it;
  - `POST /v1/pipelines/{id}/snapshots` takes `truncate` (default false); a job's JSON shows it;
  - testkit: `FakeSource::forget(table, key)`.

A job upserted, so a row deleted at the source while its table wasn't streamed stayed in
the lake (Q-M12-7). A job asked with `truncate` now empties each of its tables' sink table first:
a snapshot change set with `truncate` and no rows, a metadata-only delete on the fenced path. It
is committed before the table's start is recorded, so a crash in between empties it again, and a
restart after it resumes the copy without emptying. Flagged `snapshot`, it is never skipped by
the guard, and the marker stays.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/snapshots.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index e69da5a..03b0a75 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -342,6 +342,8 @@ mod tests {
             ]
         );
         assert_eq!(docs.requests().await.unwrap().restart.unwrap().id, id);
+        assert!(!job.truncate, "a job keeps the rows unless asked");
+        assert_eq!(v["truncate"], false);
 
         let (code, one) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
         assert_eq!(code, 200, "{one}");
@@ -409,6 +411,20 @@ mod tests {
         assert_eq!(code, 202, "{v}");
     }
 
+    /// DA-SNAP-15: `truncate: true` is recorded with the job and shown with it.
+    #[tokio::test]
+    async fn a_job_asked_with_truncate_records_it() {
+        let (addr, lone, docs, _dir) = served().await;
+        held(&lone).await;
+        let (code, v) = ask(addr, r#"{"tables": ["public.orders"], "truncate": true}"#).await;
+        assert_eq!(code, 202, "{v}");
+        assert_eq!(v["truncate"], true);
+        let id = v["job_id"].as_str().unwrap();
+        assert!(docs.snapshot_job(id).await.unwrap().unwrap().doc.truncate);
+        let (_, one) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
+        assert_eq!(one["truncate"], true, "{one}");
+    }
+
     /// The body names tables, each exactly (System-Overview: "tables list, no globs"), as the
     /// source's per-table config keys do: PostgreSQL `schema.table`.
     #[tokio::test]
@@ -422,7 +438,8 @@ mod tests {
             r#"{"tables": []}"#,
             r#"{"tables": ["public.*"]}"#,
             r#"{"tables": ["orders"]}"#,
-            r#"{"tables": ["public.orders"], "truncate": true}"#,
+            r#"{"tables": ["public.orders"], "truncate": "yes"}"#,
+            r#"{"tables": ["public.orders"], "drop": true}"#,
         ] {
             let (code, v) = ask(addr, body).await;
             assert_eq!(code, 422, "{body:?}: {v}");
```

`crates/data-avalanche-core/src/change_set.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/change_set.rs b/crates/data-avalanche-core/src/change_set.rs
index f657807..77a3be0 100644
--- a/crates/data-avalanche-core/src/change_set.rs
+++ b/crates/data-avalanche-core/src/change_set.rs
@@ -367,6 +367,11 @@ mod tests {
         let mut c = cs(vec![upserts(&[1], None)], vec![]);
         c.snapshot = true;
         c.validate().unwrap();
+        // A job's truncate (DA-SNAP-15): a snapshot set that empties the table, without rows.
+        let mut c = cs(vec![], vec![]);
+        c.snapshot = true;
+        c.truncate = true;
+        c.validate().unwrap();
         let refused = |f: fn(&mut TableChangeSet)| {
             let mut c = cs(vec![upserts(&[1], None)], vec![]);
             c.snapshot = true;
```

`crates/data-avalanche-pipeline/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
index 9e0f675..8cfff76 100644
--- a/crates/data-avalanche-pipeline/src/jobs.rs
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -123,6 +123,17 @@ mod tests {
         w.docs.create_snapshot_job(&doc).await.unwrap();
     }
 
+    async fn ask_truncating(w: &World, id: &str, tables: &[&str]) {
+        let doc = SnapshotJobDoc::new(
+            id.into(),
+            tables.iter().map(|n| t(n)).collect(),
+            "ops".into(),
+            Utc::now(),
+        )
+        .truncating();
+        w.docs.create_snapshot_job(&doc).await.unwrap();
+    }
+
     async fn job(w: &World, id: &str) -> SnapshotJobDoc {
         w.docs.snapshot_job(id).await.unwrap().unwrap().doc
     }
@@ -200,6 +211,59 @@ mod tests {
         assert!(st.warnings.is_empty(), "{:?}", st.warnings);
     }
 
+    /// DA-SNAP-15: a job asked with `truncate` empties its tables' sink tables before their
+    /// first chunk, so a row deleted at the source while the pipeline didn't stream it goes, in
+    /// a streamed table and in a copy alike. The other tables keep their rows, and a later start
+    /// doesn't empty the table again: the job's copy has begun.
+    #[tokio::test]
+    async fn a_truncating_job_drops_the_rows_deleted_meanwhile() {
+        let w = World::new(&["a", "b", "x"]);
+        w.source.outside_config(&t("x"));
+        established(&w).await;
+        w.source.commit(
+            FakeTxn::new()
+                .insert(&t("a"), row(1, "x"))
+                .insert(&t("a"), row(2, "x"))
+                .insert(&t("a"), row(3, "x"))
+                .insert(&t("b"), row(9, "x")),
+        );
+        w.source.load(&t("x"), rows(1..=3));
+        ask(&w, "p~0", &["x"]).await;
+        let (end, _) = run(&w, &never(10), async |status| {
+            wait_until("x is copied", || w.ids("x") == [1, 2, 3]).await;
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(w.ids("a"), [1, 2, 3]);
+        w.source.forget(&t("a"), &row(2, "x"));
+        w.source.forget(&t("x"), &row(3, "x"));
+        ask_truncating(&w, "p~1", &["a", "x"]).await;
+        let (end, _) = run(&w, &never(10), async |status| {
+            wait_until("the copies land", || {
+                w.ids("a") == [1, 3] && w.ids("x") == [1, 2]
+            })
+            .await;
+            streamed(&status, &w).await;
+            wait_until("the copies are done", || {
+                status.borrow().snapshots.is_empty()
+            })
+            .await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(w.ids("b"), [9], "only the job's tables");
+        assert_eq!(
+            job(&w, "p~1").await.tables[0].status,
+            JobTableStatus::Running
+        );
+        w.source.forget(&t("a"), &row(3, "x"));
+        w.source.commit(FakeTxn::new().insert(&t("a"), row(4, "x")));
+        let (end, _) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
+        end.unwrap();
+        assert_eq!(w.ids("a"), [1, 3, 4], "not emptied again");
+    }
+
     fn noted(id: i64, note: Option<&str>) -> Row {
         Row(vec![
             ("id".into(), ColumnValue::Int64(id)),
```

`crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
index 2fdfc89..862b33d 100644
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -856,6 +856,32 @@ mod tests {
         assert_eq!(ids(&s).await, [1]);
     }
 
+    /// DA-SNAP-15: a job's truncate, a snapshot set with no rows, empties the table with its
+    /// DELETE alone, fenced, and isn't skipped though the marker covers its position; the marker
+    /// stays.
+    #[tokio::test]
+    async fn a_job_s_truncate_empties_the_table_and_keeps_the_marker() {
+        let dir = tempfile::tempdir().unwrap();
+        let s = sink(&dir);
+        s.ensure_table(&spec(&orders())).await.unwrap();
+        let seed = rows(20, &[1, 2]);
+        s.commit(seed.clone(), marker(1, &seed)).await.unwrap();
+        let mut t = change_set(10, vec![], vec![]);
+        t.truncate = true;
+        t.snapshot = true;
+        t.validate().unwrap();
+        assert!(matches!(
+            s.commit(t.clone(), marker(1, &t)).await.unwrap(),
+            CommitOutcome::Committed { .. }
+        ));
+        assert!(ids(&s).await.is_empty());
+        let state = s.table_state(orders().table()).await.unwrap();
+        assert_eq!(
+            state.marker.map(|m| m.committed_position),
+            Some(seed.position.end.clone())
+        );
+    }
+
     /// Q-M5-19: a key outside the Delta range halts the set before its truncate's DELETE, so the
     /// table isn't left cleared.
     #[tokio::test]
```

`crates/data-avalanche-state/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index 137be49..6d691ba 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -607,9 +607,17 @@ mod tests {
   "requested_at": "2026-09-28T10:00:00Z",
   "requested_by": "ops",
   "finished_at": null,
-  "error": null
+  "error": null,
+  "truncate": false
 }"#
         );
+        // A job v0.1.0 wrote, without `truncate`, reads as one that keeps the rows.
+        let old = String::from_utf8(crate::doc::encode(&doc).to_vec())
+            .unwrap()
+            .replace(",\n  \"truncate\": false", "");
+        assert!(!old.contains("truncate"), "{old}");
+        let read: SnapshotJobDoc = crate::doc::decode("j", old.as_bytes()).unwrap();
+        assert!(!read.truncate);
     }
 
     /// A job is queued until a table starts, running while one is queued or running, then
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-core -p data-avalanche-state -p data-avalanche-pipeline -p data-avalanche-sink-delta -p data-avalanche-api --no-fail-fast
```

Expected: FAIL to compile:
  - `error[E0609]: no field `truncate` on type `snapshot::SnapshotJobDoc` (crates/data-avalanche-state/src/snapshot.rs)`

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/snapshots.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index 03b0a75..604c8a6 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -51,13 +51,17 @@ fn table_name(p: &Pipeline, t: &TableRef) -> String {
 #[serde(deny_unknown_fields)]
 struct Asked {
     tables: Vec<String>,
+    /// Empty each table's sink table before its copy (DA-SNAP-15).
+    #[serde(default)]
+    truncate: bool,
 }
 
 /// The body's tables, each once, in its order: named as the source's per-table config keys,
-/// no globs.
-fn tables(p: &Pipeline, text: &str) -> Result<Vec<TableRef>, String> {
-    let asked: Asked = serde_json::from_str(text)
-        .map_err(|e| format!("the body is {{\"tables\": [\"<table>\", ...]}}: {e}"))?;
+/// no globs; and whether the job empties them first.
+fn tables(p: &Pipeline, text: &str) -> Result<(Vec<TableRef>, bool), String> {
+    let asked: Asked = serde_json::from_str(text).map_err(|e| {
+        format!("the body is {{\"tables\": [\"<table>\", ...], \"truncate\": false}}: {e}")
+    })?;
     if asked.tables.is_empty() {
         return Err("the body names no table".into());
     }
@@ -85,7 +89,7 @@ fn tables(p: &Pipeline, text: &str) -> Result<Vec<TableRef>, String> {
             out.push(table);
         }
     }
-    Ok(out)
+    Ok((out, asked.truncate))
 }
 
 fn job_json(p: &Pipeline, j: &SnapshotJobDoc) -> Value {
@@ -115,6 +119,7 @@ fn job_json(p: &Pipeline, j: &SnapshotJobDoc) -> Value {
         "requested_by": j.requested_by,
         "finished_at": j.finished_at,
         "error": j.error,
+        "truncate": j.truncate,
     })
 }
 
@@ -133,8 +138,8 @@ pub(crate) async fn ask(
         Ok(found) => found,
         Err(f) => return f.into(),
     };
-    let tables = match tables(&p, &text) {
-        Ok(tables) => tables,
+    let (tables, truncate) = match tables(&p, &text) {
+        Ok(asked) => asked,
         Err(why) => return Failure::new(StatusCode::UNPROCESSABLE_ENTITY, why).into(),
     };
     let r = match read(node, &p, &docs).await {
@@ -179,7 +184,8 @@ pub(crate) async fn ask(
             ));
         }
     }
-    let job = SnapshotJobDoc::new(new_job_id(&p.id), tables, by.name.clone(), Utc::now());
+    let mut job = SnapshotJobDoc::new(new_job_id(&p.id), tables, by.name.clone(), Utc::now());
+    job.truncate = truncate;
     if let Err(e) = docs.create_snapshot_job(&job).await {
         return unavailable(&e);
     }
@@ -196,7 +202,7 @@ pub(crate) async fn ask(
             }),
         );
     }
-    tracing::info!(pipeline = %p.name, by = %by.name, job = %job.job_id, tables = job.tables.len(), "snapshot job asked through the API");
+    tracing::info!(pipeline = %p.name, by = %by.name, job = %job.job_id, tables = job.tables.len(), truncate, "snapshot job asked through the API");
     let v = job_json(&p, &job);
     body(
         StatusCode::ACCEPTED,
@@ -204,6 +210,7 @@ pub(crate) async fn ask(
             "job_id": v["job_id"],
             "status": v["status"],
             "tables": v["tables"].as_array().into_iter().flatten().map(|t| t["table"].clone()).collect::<Vec<_>>(),
+            "truncate": truncate,
         }),
     )
 }
```

`crates/data-avalanche-core/src/change_set.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/change_set.rs b/crates/data-avalanche-core/src/change_set.rs
index 77a3be0..67793f0 100644
--- a/crates/data-avalanche-core/src/change_set.rs
+++ b/crates/data-avalanche-core/src/change_set.rs
@@ -47,7 +47,8 @@ pub struct SplitInfo {
 /// - identifier values are never NULL;
 /// - the range doesn't run backwards, `last_change` is inside it, and a slice's range ends at its
 ///   `txn_start`;
-/// - a snapshot set only upserts: no deletes, no truncate, not a slice.
+/// - a snapshot set only upserts: no deletes, not a slice, and a truncate only without rows (a
+///   job's truncate, DA-SNAP-15).
 #[derive(Debug, Clone)]
 pub struct TableChangeSet {
     pub table: TableRef,
@@ -181,9 +182,13 @@ impl TableChangeSet {
                 )));
             }
         }
-        if self.snapshot && (self.truncate || self.split.is_some() || self.delete_rows() > 0) {
+        if self.snapshot
+            && (self.split.is_some()
+                || self.delete_rows() > 0
+                || (self.truncate && self.upsert_rows() > 0))
+        {
             return Err(err(
-                "a snapshot set carries upserts only: no deletes, truncate or slice".into(),
+                "a snapshot set carries upserts only: no deletes or slice, and a truncate only without rows".into(),
             ));
         }
         if let Some(split) = &self.split
```

`crates/data-avalanche-pipeline/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
index 8cfff76..b201acc 100644
--- a/crates/data-avalanche-pipeline/src/jobs.rs
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -42,6 +42,17 @@ impl Jobs {
         self.tables(JobTableStatus::Running)
     }
 
+    /// The asked tables of the jobs that empty their sink tables first (DA-SNAP-15).
+    pub fn truncating(&self) -> BTreeSet<TableRef> {
+        self.jobs
+            .iter()
+            .filter(|j| j.truncate)
+            .flat_map(|j| &j.tables)
+            .filter(|t| t.status == JobTableStatus::Queued)
+            .map(|t| t.table.clone())
+            .collect()
+    }
+
     /// Records what this start did with the job tables: the asked ones `started` run, and the
     /// `failed` ones (asked, or running and gone) failed with their reason. An asked table
     /// neither started nor failed stays queued.
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index d0c9a78..297d890 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -342,25 +342,39 @@ where
     }
 }
 
-/// Moves the table's marker onto `at`, the new server's position (DA-SRC-20): an empty change set
-/// that replaces the marker instead of joining it. A table the lake doesn't hold, or holds no
-/// marker for, has nothing to move.
-async fn reposition(
+/// A change set that carries no rows, committed at Phase 0.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+enum Empty {
+    /// Moves the table's marker onto `at`, the new server's position (DA-SRC-20): the marker is
+    /// replaced instead of joined. A table the lake holds no marker for has nothing to move.
+    Reposition,
+    /// Empties the sink table before a job's copy (DA-SNAP-15): a metadata-only delete, fenced,
+    /// the marker joined with `at` as any commit's.
+    Truncate,
+}
+
+/// Commits an [`Empty`] change set to `schema`'s sink table at `at`. A table the lake doesn't
+/// hold has nothing to commit.
+async fn commit_empty(
     core: &Core,
     tuning: &Tuning,
     schema: &TableSchema,
     at: &SourcePosition,
+    kind: Empty,
 ) -> Result<(), WorkerError> {
     let table = schema.table();
     let state = sink_retry(tuning, || core.may_write(), || core.sink.table_state(table)).await?;
-    let (true, Some(_), Some(columns)) = (state.exists, state.marker, state.schema) else {
+    let (true, Some(columns)) = (state.exists, state.schema) else {
         return Ok(());
     };
+    if kind == Empty::Reposition && state.marker.is_none() {
+        return Ok(());
+    }
     let cs = TableChangeSet {
         table: table.clone(),
         schema: columns,
         identifier: schema.identifier().to_vec(),
-        truncate: false,
+        truncate: kind == Empty::Truncate,
         upserts: vec![],
         deletes: vec![],
         position: PositionRange {
@@ -369,8 +383,9 @@ async fn reposition(
             last_change: at.clone(),
         },
         split: None,
-        snapshot: false,
-        reposition: true,
+        // A truncate is never skipped as covered: the job asked for it now.
+        snapshot: kind == Empty::Truncate,
+        reposition: kind == Empty::Reposition,
     };
     let marker = CommitMarker {
         owner_epoch: core.owner_epoch,
@@ -388,8 +403,8 @@ async fn reposition(
             table: table.clone(),
             current_epoch,
         }),
-        // A reposition is never skipped as covered (`covers` never skips one): the arm is
-        // unreachable, and harmless.
+        // Neither is ever skipped as covered (`covers` skips neither a reposition nor a
+        // snapshot's set): the arm is unreachable, and harmless.
         CommitOutcome::Committed { .. } | CommitOutcome::SkippedCovered => Ok(()),
     }
 }
@@ -915,6 +930,15 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
             .filter(|(t, _)| asked.contains(t))
             .map(|(t, reason)| (t.clone(), format!("{}: {reason}", reason.code()))),
     );
+    // A truncating job empties each table it starts before the table's first chunk. Repeated
+    // only while the job still has the table queued, before any chunk is read: once recorded
+    // running below, a later start resumes the copy instead (DA-SNAP-15).
+    let truncating = jobs.truncating();
+    for table in started.iter().filter(|t| truncating.contains(*t)) {
+        if let Some(schema) = resolved_tables.get(table).or_else(|| copies.get(table)) {
+            commit_empty(core, &tuning, schema, &at, Empty::Truncate).await?;
+        }
+    }
     core.may_write()?;
     jobs.record(&core.docs, &started, &failed).await?;
     // A copy's halt goes once its job carries it: the pipeline doesn't replicate the table, and
@@ -938,7 +962,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         let tables: BTreeSet<&TableRef> = resolved_tables.keys().chain(cached.keys()).collect();
         for table in tables {
             if let Some(schema) = cached.get(table).or_else(|| resolved_tables.get(table)) {
-                reposition(core, &tuning, schema, &at).await?;
+                commit_empty(core, &tuning, schema, &at, Empty::Reposition).await?;
             }
         }
     }
```

`crates/data-avalanche-state/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index 6d691ba..efc3256 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -110,6 +110,9 @@ pub struct SnapshotJobDoc {
     pub finished_at: Option<DateTime<Utc>>,
     /// Which tables failed, once the job ended.
     pub error: Option<String>,
+    /// Each table's sink table is emptied before its first chunk (DA-SNAP-15, Q-M12-7).
+    #[serde(default)]
+    pub truncate: bool,
 }
 
 impl Document for SnapshotJobDoc {
@@ -141,9 +144,17 @@ impl SnapshotJobDoc {
             requested_by,
             finished_at: None,
             error: None,
+            truncate: false,
         }
     }
 
+    /// The job empties each table's sink table before its first chunk.
+    #[must_use]
+    pub fn truncating(mut self) -> Self {
+        self.truncate = true;
+        self
+    }
+
     /// Completed or failed: the job has ended, and [`Self::settle`] leaves it as it is.
     pub fn is_finished(&self) -> bool {
         matches!(self.status, JobStatus::Completed | JobStatus::Failed)
```

`crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index 52310a6..9af9a1e 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -352,6 +352,15 @@ impl FakeSource {
         }
     }
 
+    /// Deletes `key`'s row from `table` without logging the change: a row deleted while the
+    /// pipeline didn't stream the table.
+    pub fn forget(&self, table: &TableRef, key: &Row) {
+        let mut db = self.db();
+        let schema = db.schema(table);
+        let key = key_of(&schema, key);
+        db.rows.get_mut(table).expect("a table").remove(&key);
+    }
+
     /// The schema the catalog has for `table` now, with its source id.
     pub fn schema(&self, table: &TableRef) -> TableSchema {
         self.db().schema(table)
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-core -p data-avalanche-state -p data-avalanche-pipeline -p data-avalanche-sink-delta -p data-avalanche-api --no-fail-fast
```

Expected: PASS, 649 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/snapshots.rs \
  crates/data-avalanche-core/src/change_set.rs \
  crates/data-avalanche-pipeline/src/jobs.rs \
  crates/data-avalanche-pipeline/src/worker.rs \
  crates/data-avalanche-sink-delta/src/sink.rs \
  crates/data-avalanche-state/src/snapshot.rs \
  crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "feat(pipeline): a snapshot job asked with truncate empties each sink table before its copy (DA-SNAP-15)"
```

---

### Task 7: The jobs' loose ends (`storage`, `state`, `pipeline`, `cluster`, `api`)

**Files:**
- Modify: `crates/data-avalanche-api/src/pipelines.rs`, `crates/data-avalanche-api/src/snapshots.rs`, `crates/data-avalanche-cluster/src/jobs.rs`, `crates/data-avalanche-cluster/src/test_support.rs`, `crates/data-avalanche-pipeline/src/session.rs`, `crates/data-avalanche-pipeline/src/test_support.rs`, `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-pipeline/src/writer.rs`, `crates/data-avalanche-state/src/lib.rs`, `crates/data-avalanche-state/src/snapshot.rs`, `crates/data-avalanche-state/src/tables.rs`, `crates/data-avalanche-state/src/test_support.rs`, `crates/data-avalanche-storage/src/store.rs`
- Test: `crates/data-avalanche-api/src/pipelines.rs`, `crates/data-avalanche-api/src/snapshots.rs`, `crates/data-avalanche-cluster/src/coordinator.rs`, `crates/data-avalanche-cluster/src/jobs.rs`, `crates/data-avalanche-pipeline/src/jobs.rs`, `crates/data-avalanche-pipeline/src/writer.rs`, `crates/data-avalanche-state/src/snapshot.rs`, `crates/data-avalanche-state/src/tables.rs`, `crates/data-avalanche-storage/src/store.rs`, `crates/data-avalanche-storage/tests/versioned_store.rs`

**Gaps:** DA-SNAP-16, DA-SNAP-19, DA-SNAP-20, DA-SNAP-21.

**Interfaces:**
- Consumes: `VersionedStore` (M2); `PipelineDocs::snapshot_jobs`, `TableHalt` (M9b); the owner's job poll; the snapshot route's `409`.
- Produces:
  - `VersionedStore::delete(key)` (an absent object is `Ok`), on every store and wrapper, and `Op::Delete`;
  - `KEPT_ENDED_JOBS = 100`, `PipelineDocs::prune_snapshot_jobs(keep) -> Result<usize>` (ended jobs, newest `finished_at` first; the rest deleted); the owner's poll prunes after a job ends;
  - `snapshot_jobs` skips a `Corrupt` or `UnsupportedVersion` document with a warning;
  - `TableHalt.copy: bool` (`serde(default)`, skipped when false), set on a copy's halt (the session's and the writer's, through `WriterCtx.copies`); copies are kept out of `status.halted`, the metric and the API's `halted_tables`;
  - a `409` against a queued job writes its restart again (`Request { id: job_id, at: requested_at }`), unless that is the one recorded.

Four small gaps of M9b's jobs. Ended jobs accumulated, and every list and every ask read
them all: a pipeline now keeps its 100 newest ended jobs, pruned as each job ends (an unfinished
one is never pruned). A job document a downgraded node couldn't decode stopped the pipeline's
start: the listing skips it, with a warning. A copy's halt (a table outside the config) showed as
a halted table until the next start: it is flagged `copy` and fails the job alone. A job whose
restart request was lost stayed queued: asking for its table again writes the restart again.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/pipelines.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index 1ad5b3d..924adcd 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -486,11 +486,25 @@ mod tests {
             table: TableRef::new("public", "orders"),
             reason: HaltReason::TableDropped,
             at: SourcePosition::Pg(lsn),
+            copy: false,
+        })
+        .await
+        .unwrap();
+        docs.record_halt(&TableHalt {
+            table: TableRef::new("public", "audit"),
+            reason: HaltReason::TableDropped,
+            at: SourcePosition::Pg(lsn),
+            copy: true,
         })
         .await
         .unwrap();
         let v = json_of(addr, "GET", "/v1/pipelines/orders").await.1;
         assert_eq!(v["actual_state"], "running");
+        assert_eq!(
+            v["halted_tables"].as_array().unwrap().len(),
+            1,
+            "a copy's halt isn't shown: {v}"
+        );
         assert_eq!(v["position"], "pg:0/16B3748");
         assert_eq!(v["halted_tables"][0]["table"], "public.orders");
         assert_eq!(v["halted_tables"][0]["code"], "TABLE_DROPPED");
```

`crates/data-avalanche-api/src/snapshots.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index 604c8a6..9b13544 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -395,7 +395,18 @@ mod tests {
             error.contains("public.orders") && error.contains(&first),
             "{v}"
         );
-        assert!(error.contains("the pipeline's next start"), "queued: {v}");
+        assert!(error.contains("its restart is asked again"), "queued: {v}");
+        // DA-SNAP-21: a job whose restart was lost gets it again when its table is asked.
+        let other = data_avalanche_state::Request {
+            id: "someone-else".into(),
+            at: Utc::now(),
+        };
+        docs.request(data_avalanche_state::Action::Restart, other)
+            .await
+            .unwrap();
+        let (code, _) = ask(addr, r#"{"tables": ["public.orders"]}"#).await;
+        assert_eq!(code, 409);
+        assert_eq!(docs.requests().await.unwrap().restart.unwrap().id, first);
         docs.update_snapshot_job(&first, |j| {
             j.tables[0].status = JobTableStatus::Running;
             j.settle(Utc::now());
```

`crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index f583840..15d2b6a 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -774,6 +774,9 @@ mod tests {
         async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
             self.inner.list(prefix).await
         }
+        async fn delete(&self, key: &str) -> Result<(), StoreError> {
+            self.inner.delete(key).await
+        }
     }
 
     /// A lone node `n1` running generation 1 of its text: its reloads go to `reloads`, its
```

`crates/data-avalanche-cluster/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/jobs.rs b/crates/data-avalanche-cluster/src/jobs.rs
index ff08e68..3b26f6f 100644
--- a/crates/data-avalanche-cluster/src/jobs.rs
+++ b/crates/data-avalanche-cluster/src/jobs.rs
@@ -239,6 +239,9 @@ mod tests {
         async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
             self.inner.list(prefix).await
         }
+        async fn delete(&self, key: &str) -> Result<(), StoreError> {
+            self.inner.delete(key).await
+        }
     }
 
     fn starting() -> (Arc<Starting>, PipelineDocs) {
@@ -260,6 +263,7 @@ mod tests {
             table: a.clone(),
             reason: HaltReason::TableDropped,
             at: "pg:0/10".parse().unwrap(),
+            copy: false,
         })
         .await
         .unwrap();
@@ -292,6 +296,46 @@ mod tests {
         assert_eq!(jobs.0, Some(BTreeSet::new()));
     }
 
+    /// A job that ends at a poll deletes the ended jobs past the newest [`KEPT_ENDED_JOBS`]
+    /// (DA-SNAP-16).
+    #[tokio::test]
+    async fn a_job_that_ends_prunes_the_oldest_ended_jobs() {
+        let (_, docs) = starting();
+        let a = TableRef::new("public", "a");
+        let t0 = Utc::now() - chrono::Duration::days(1);
+        for i in 0..KEPT_ENDED_JOBS {
+            let mut old =
+                SnapshotJobDoc::new(format!("old-{i:03}"), vec![a.clone()], "api".into(), t0);
+            old.status = JobStatus::Completed;
+            old.finished_at = Some(t0 + chrono::Duration::seconds(i as i64));
+            docs.create_snapshot_job(&old).await.unwrap();
+        }
+        let mut job = SnapshotJobDoc::new("j1".into(), vec![a.clone()], "api".into(), Utc::now());
+        job.status = JobStatus::Running;
+        job.tables[0].status = JobTableStatus::Running;
+        docs.create_snapshot_job(&job).await.unwrap();
+        let mut done = doc(SnapshotStatus::Completed);
+        done.row_count = Some(3);
+        docs.save_table_snapshot(&done).await.unwrap();
+        let mut jobs = Jobs::default();
+        jobs.list(&docs, "p").await;
+        jobs.poll(&docs, "p").await;
+
+        let ids: Vec<String> = docs
+            .snapshot_jobs()
+            .await
+            .unwrap()
+            .into_iter()
+            .map(|v| v.doc.job_id)
+            .collect();
+        assert_eq!(ids.len(), KEPT_ENDED_JOBS);
+        assert!(
+            !ids.contains(&"old-000".to_owned()),
+            "the oldest ended job goes"
+        );
+        assert!(ids.contains(&"j1".to_owned()) && ids.contains(&"old-001".to_owned()));
+    }
+
     fn doc(status: SnapshotStatus) -> TableSnapshotDoc {
         TableSnapshotDoc {
             table: TableRef::new("public", "a"),
@@ -339,6 +383,7 @@ mod tests {
                 table: a.clone(),
                 reason: HaltReason::SnapshotTimeout { limit_secs: 60 },
                 at: "pg:0/10".parse().unwrap(),
+                copy: false,
             }],
         };
         let Progress::Failed(why) = progress(&a, &halted, Some(&running)) else {
```

`crates/data-avalanche-pipeline/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
index b201acc..3cb8e9e 100644
--- a/crates/data-avalanche-pipeline/src/jobs.rs
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -312,6 +312,7 @@ mod tests {
                 table: t("a"),
                 reason: HaltReason::TableDropped,
                 at: w.source.wal_end(),
+                copy: false,
             })
             .await
             .unwrap();
@@ -383,6 +384,7 @@ mod tests {
                     detail: "the key changed".into(),
                 },
                 at: w.source.wal_end(),
+                copy: false,
             })
             .await
             .unwrap();
@@ -604,10 +606,6 @@ mod tests {
             SourceError::Permanent("permission denied for table x".into()),
         );
         let (end, st) = run(&w, &never(2), async |status| {
-            wait_until("the copy halts", || {
-                status.borrow().halted.contains_key(&t("x"))
-            })
-            .await;
             w.source
                 .commit(FakeTxn::new().insert(&t("a"), row(1, "streamed")));
             wait_until("the stream moves on", || w.ids("a") == [1]).await;
@@ -615,16 +613,16 @@ mod tests {
         })
         .await;
         assert_eq!(end.unwrap(), Exit::Drained);
-        assert_eq!(
-            st.halted.get(&t("x")),
-            Some(&HaltReason::NotReplicable {
-                reason: "permission denied for table x".into()
-            })
+        assert!(
+            st.halted.is_empty(),
+            "a copy's halt isn't shown: {:?}",
+            st.halted
         );
         let halts = w.docs.halts().await.unwrap().tables;
         assert_eq!(halts.len(), 1, "{halts:?}");
         assert_eq!(halts[0].table, t("x"));
         assert_eq!(halts[0].reason.code(), "TABLE_NOT_REPLICABLE");
+        assert!(halts[0].copy, "it fails its job, marked a copy's");
         assert!(w.ids("x").is_empty());
     }
 
@@ -644,9 +642,13 @@ mod tests {
             w.source.drop_table(&t("x"));
             w.source.release_reads();
             wait_until("the copy halts", || {
-                status.borrow().halted.contains_key(&t("x"))
+                !status.borrow().snapshots.contains_key(&t("x"))
             })
             .await;
+            assert!(
+                status.borrow().halted.is_empty(),
+                "a copy's halt isn't shown"
+            );
             w.source
                 .commit(FakeTxn::new().insert(&t("a"), row(1, "streamed")));
             streamed(&status, &w).await;
@@ -696,6 +698,7 @@ mod tests {
                 table: t("x"),
                 reason: HaltReason::SnapshotTimeout { limit_secs: 60 },
                 at: w.source.wal_end(),
+                copy: false,
             })
             .await
             .unwrap();
```

`crates/data-avalanche-pipeline/src/writer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index 9254a8e..dfd14a9 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -427,6 +427,7 @@ mod tests {
                 hooks,
                 observer: self.ears.clone(),
                 backoff: FAST,
+                copies: BTreeSet::new(),
             };
             let (jobs_tx, jobs_rx) = mpsc::unbounded_channel();
             let (events_tx, mut events_rx) = mpsc::unbounded_channel();
@@ -497,6 +498,7 @@ mod tests {
             table: TableRef::new("public", "c"),
             reason: HaltReason::NumericOverflow { column: "n".into() },
             at: lsn(100),
+            copy: false,
         };
         j.halts.push(overflow.clone());
         j.snapshots.push(snapshot_doc("a"));
@@ -791,6 +793,7 @@ mod tests {
                 detail: "column mismatch".into(),
             },
             at: lsn(100),
+            copy: false,
         };
         assert!(
             matches!(&events[..], [WriterEvent::Halted(h), WriterEvent::Durable(_), WriterEvent::Durable(p)] if *h == halt && *p == lsn(300)),
```

`crates/data-avalanche-state/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index efc3256..174feb7 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -722,4 +722,69 @@ mod tests {
             .collect();
         assert_eq!(ids, ["a", "c"]);
     }
+
+    fn ended(id: &str, at: u32, finished: u32) -> SnapshotJobDoc {
+        let mut j = job(id, at);
+        j.status = JobStatus::Completed;
+        j.finished_at = Some(t(finished));
+        j
+    }
+
+    #[tokio::test]
+    async fn pruning_keeps_the_newest_ended_jobs_and_every_unfinished_one() {
+        let p = pipeline();
+        p.create_snapshot_job(&ended("old", 1, 9)).await.unwrap();
+        p.create_snapshot_job(&ended("newer", 2, 10)).await.unwrap();
+        p.create_snapshot_job(&ended("newest", 3, 11))
+            .await
+            .unwrap();
+        p.create_snapshot_job(&job("queued", 0)).await.unwrap();
+        assert_eq!(p.prune_snapshot_jobs(2).await.unwrap(), 1);
+        let ids: Vec<String> = p
+            .snapshot_jobs()
+            .await
+            .unwrap()
+            .into_iter()
+            .map(|v| v.doc.job_id)
+            .collect();
+        assert_eq!(
+            ids,
+            ["queued", "newer", "newest"],
+            "finished first is pruned first"
+        );
+        assert!(p.snapshot_job("old").await.unwrap().is_none());
+        assert_eq!(p.prune_snapshot_jobs(2).await.unwrap(), 0);
+        assert_eq!(p.prune_snapshot_jobs(0).await.unwrap(), 2);
+        assert_eq!(
+            p.snapshot_jobs().await.unwrap().len(),
+            1,
+            "a queued job stays"
+        );
+    }
+
+    #[tokio::test]
+    async fn an_undecodable_job_is_skipped_by_the_listing() {
+        let p = pipeline();
+        p.create_snapshot_job(&job("good", 1)).await.unwrap();
+        let text = String::from_utf8(crate::doc::encode(&job("bad", 2)).to_vec()).unwrap();
+        let bad = text.replacen('{', "{\"from_a_newer_release\": 1, ", 1);
+        p.docs
+            .store()
+            .put(&job_key("bad"), Bytes::from(bad))
+            .await
+            .unwrap();
+        assert!(matches!(
+            p.snapshot_job("bad").await.unwrap_err(),
+            StateError::Corrupt { .. }
+        ));
+        let ids: Vec<String> = p
+            .snapshot_jobs()
+            .await
+            .unwrap()
+            .into_iter()
+            .map(|v| v.doc.job_id)
+            .collect();
+        assert_eq!(ids, ["good"]);
+        assert_eq!(p.unfinished_snapshot_jobs().await.unwrap().len(), 1);
+    }
 }
```

`crates/data-avalanche-state/src/tables.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/tables.rs b/crates/data-avalanche-state/src/tables.rs
index 9026db2..44235b8 100644
--- a/crates/data-avalanche-state/src/tables.rs
+++ b/crates/data-avalanche-state/src/tables.rs
@@ -305,6 +305,7 @@ mod tests {
             table: TableRef::new("public", name),
             reason,
             at: "pg:0/16B3748".parse().unwrap(),
+            copy: false,
         }
     }
 
```

`crates/data-avalanche-storage/src/store.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/store.rs b/crates/data-avalanche-storage/src/store.rs
index 988f602..78b8ac7 100644
--- a/crates/data-avalanche-storage/src/store.rs
+++ b/crates/data-avalanche-storage/src/store.rs
@@ -300,6 +300,15 @@ mod tests {
         assert!(s.get("other").await.unwrap().is_none());
     }
 
+    #[tokio::test]
+    async fn delete_removes_and_a_missing_object_is_no_error() {
+        let s = store("root");
+        s.put("k", b("one")).await.unwrap();
+        s.delete("k").await.unwrap();
+        assert!(s.get("k").await.unwrap().is_none());
+        s.delete("k").await.unwrap();
+    }
+
     #[tokio::test]
     async fn put_overwrites_unconditionally() {
         let s = store("root");
```

`crates/data-avalanche-storage/tests/versioned_store.rs`:

```diff
diff --git a/crates/data-avalanche-storage/tests/versioned_store.rs b/crates/data-avalanche-storage/tests/versioned_store.rs
index ef81315..6dae080 100644
--- a/crates/data-avalanche-storage/tests/versioned_store.rs
+++ b/crates/data-avalanche-storage/tests/versioned_store.rs
@@ -76,6 +76,12 @@ async fn check(store: Arc<dyn ObjectStore>, test: &str) {
     );
     assert_eq!(s.get("doc.json").await.unwrap().unwrap().0, b("two"));
 
+    s.delete("snapshots/jobs/j1.json").await.expect("delete");
+    assert_eq!(s.get("snapshots/jobs/j1.json").await.unwrap(), None);
+    s.delete("snapshots/jobs/j1.json")
+        .await
+        .expect("a delete of a missing object succeeds");
+
     delete_prefix(store.as_ref(), &prefix).await;
 }
 
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-storage -p data-avalanche-state -p data-avalanche-pipeline -p data-avalanche-cluster -p data-avalanche-api --no-fail-fast
```

Expected: FAIL to compile:
  - `error[E0599]: no method named `delete` found for struct `store::ObjectStoreVersioned` in the current scope (crates/data-avalanche-storage/src/store.rs)`
  - `error[E0599]: no method named `delete` found for struct `ObjectStoreVersioned` in the current scope (crates/data-avalanche-storage/tests/versioned_store.rs)`

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/pipelines.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index 924adcd..b2876e9 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -227,10 +227,12 @@ pub(crate) async fn get(State(api): State<Api>, Path(id): Path<String>) -> Respo
         None => Value::Null,
     };
     v["position"] = json!(position.map(|p| p.to_string()));
+    // A copy's halt fails its job table, which says why (DA-SNAP-19).
     v["halted_tables"] = json!(
         halts
             .tables
             .iter()
+            .filter(|h| !h.copy)
             .map(|h| json!({
                 "table": h.table.to_string(),
                 "code": h.reason.code(),
```

`crates/data-avalanche-api/src/snapshots.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index 9b13544..910dfd7 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -15,7 +15,8 @@ use chrono::Utc;
 use data_avalanche_config::{Pipeline, SourceType, key_table};
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{
-    Action, DesiredState, JobStatus, JobTableStatus, Request, SnapshotJobDoc,
+    Action, DesiredState, JobStatus, JobTableStatus, PipelineDocs, Request, SnapshotJobDoc,
+    StateError,
 };
 use serde::Deserialize;
 use serde_json::{Value, json};
@@ -173,9 +174,14 @@ pub(crate) async fn ask(
                 && tables.contains(&t.table)
         }) {
             let queued = if j.status == JobStatus::Queued {
-                " (it is queued: a queued job runs at the pipeline's next start, a restart)"
+                // Its restart may not have been written (DA-SNAP-21): ask it again, under the
+                // job's id, so the owner takes it at its next poll.
+                match ask_restart_again(&docs, j).await {
+                    Ok(()) => " (it is queued: its restart is asked again, and it runs at the pipeline's next start)".to_owned(),
+                    Err(e) => format!(" (it is queued, and its restart can't be asked again ({e}): it runs at the pipeline's next start)"),
+                }
             } else {
-                ""
+                String::new()
             };
             return conflict(format!(
                 "job {} snapshots {} already; ask again once it ended{queued}",
@@ -215,6 +221,18 @@ pub(crate) async fn ask(
     )
 }
 
+/// Writes queued job `j`'s restart again, unless it is the restart recorded already.
+async fn ask_restart_again(docs: &PipelineDocs, j: &SnapshotJobDoc) -> Result<(), StateError> {
+    let restart = Request {
+        id: j.job_id.clone(),
+        at: j.requested_at,
+    };
+    if docs.requests().await?.restart.as_ref() == Some(&restart) {
+        return Ok(());
+    }
+    docs.request(Action::Restart, restart).await
+}
+
 /// `GET /v1/pipelines/{id}/snapshots`: every job of the pipeline, oldest request first.
 pub(crate) async fn list(State(api): State<Api>, Path(id): Path<String>) -> Response {
     let Some(node) = api.node.get() else {
```

`crates/data-avalanche-cluster/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/jobs.rs b/crates/data-avalanche-cluster/src/jobs.rs
index 3b26f6f..03b62d4 100644
--- a/crates/data-avalanche-cluster/src/jobs.rs
+++ b/crates/data-avalanche-cluster/src/jobs.rs
@@ -8,8 +8,8 @@ use std::collections::{BTreeMap, BTreeSet};
 use chrono::Utc;
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{
-    HaltsDoc, JobTableStatus, PipelineDocs, SnapshotJobDoc, SnapshotStatus, StateError,
-    TableSnapshotDoc, Versioned,
+    HaltsDoc, JobTableStatus, KEPT_ENDED_JOBS, PipelineDocs, SnapshotJobDoc, SnapshotStatus,
+    StateError, TableSnapshotDoc, Versioned,
 };
 
 /// What a running job table's documents say of it.
@@ -137,9 +137,17 @@ impl Jobs {
                 }
             }
         }
+        if ended.is_empty() {
+            return;
+        }
         for id in ended {
             ids.remove(&id);
         }
+        match docs.prune_snapshot_jobs(KEPT_ENDED_JOBS).await {
+            Ok(0) => {}
+            Ok(n) => tracing::info!(pipeline, deleted = n, "ended snapshot jobs pruned"),
+            Err(e) => tracing::warn!(pipeline, "ended snapshot jobs unpruned: {e}"),
+        }
     }
 }
 
```

`crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index 5fa8b71..e5a0336 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -193,6 +193,9 @@ impl VersionedStore for ScriptedStore {
         }
         self.inner.list(prefix).await
     }
+    async fn delete(&self, key: &str) -> Result<(), StoreError> {
+        self.inner.delete(key).await
+    }
 }
 
 /// Balanced defaults (§18): 15 s, renewed every 5 s, deadline at 12.75 s.
```

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 48b07f2..444d560 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -283,6 +283,7 @@ impl<'c> Session<'c> {
                     table,
                     reason,
                     at: checkpoint.clone(),
+                    copy: false,
                 }),
                 Err(e) => return Err(e.into()),
             }
@@ -297,6 +298,7 @@ impl<'c> Session<'c> {
                     table,
                     reason,
                     at: checkpoint.clone(),
+                    copy: true,
                 }),
                 Err(e) => return Err(e.into()),
             }
@@ -337,6 +339,7 @@ impl<'c> Session<'c> {
             hooks: core.hooks.clone(),
             observer: core.observer.clone(),
             backoff: core.tuning.sink_backoff,
+            copies: copies.keys().cloned().collect(),
         };
         let writer = tokio::spawn(writer::run(ctx, jobs_rx, events_tx));
         let rows = buffer_permits(core.settings.buffer_rows);
@@ -412,7 +415,9 @@ impl<'c> Session<'c> {
                 WriterEvent::Failed(e) => return Err(e),
                 WriterEvent::Halted(h) => self.core.update(|st| {
                     st.snapshots.remove(&h.table);
-                    st.halted.insert(h.table, h.reason);
+                    if !h.copy {
+                        st.halted.insert(h.table, h.reason);
+                    }
                 }),
                 WriterEvent::Degraded(why) => self.core.update(|st| st.degraded = why),
                 WriterEvent::Durable(cp) => {
@@ -596,7 +601,9 @@ impl<'c> Session<'c> {
                 self.hold_ended();
                 self.core.update(|st| {
                     st.snapshots.remove(&h.table);
-                    st.halted.insert(h.table, h.reason);
+                    if !h.copy {
+                        st.halted.insert(h.table, h.reason);
+                    }
                 });
                 Ok(None)
             }
@@ -1164,7 +1171,7 @@ impl<'c> Session<'c> {
         }
         self.acc.halt_table(&table);
         self.schemas.remove(&table);
-        self.copies.remove(&table);
+        let copy = self.copies.remove(&table).is_some();
         self.snaps.stop(&table);
         self.core.writers_waited.remove(&table);
         self.hold_ended();
@@ -1172,10 +1179,13 @@ impl<'c> Session<'c> {
             table: table.clone(),
             reason: reason.clone(),
             at: self.acc.checkpoint().clone(),
+            copy,
         });
         self.core.update(|st| {
             st.snapshots.remove(&table);
-            st.halted.insert(table, reason);
+            if !copy {
+                st.halted.insert(table, reason);
+            }
         });
         Ok(())
     }
```

`crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
index 5fca0b2..145c32b 100644
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -419,6 +419,9 @@ impl VersionedStore for FlakyStore {
     async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
         self.inner.list(prefix).await
     }
+    async fn delete(&self, key: &str) -> Result<(), StoreError> {
+        self.inner.delete(key).await
+    }
 }
 
 /// Crashes at the `n`th hook point `when` matches (1 = the first).
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 297d890..6f13aa1 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -869,6 +869,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
                 table: table.clone(),
                 reason: reason.clone(),
                 at: at.clone(),
+                copy: false,
             })
             .await?;
     }
```

`crates/data-avalanche-pipeline/src/writer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index dfd14a9..c00a796 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -75,6 +75,8 @@ pub(crate) struct WriterCtx {
     pub hooks: Arc<dyn Hooks>,
     pub observer: Arc<dyn Observer>,
     pub backoff: Backoff,
+    /// The run's copies (Q-M9b-3): a halt of one is marked a copy's.
+    pub copies: BTreeSet<TableRef>,
 }
 
 impl std::fmt::Debug for WriterCtx {
@@ -169,6 +171,7 @@ impl Writer {
             match self.commit(cs).await? {
                 Some(detail) => {
                     let halt = TableHalt {
+                        copy: self.ctx.copies.contains(&table),
                         table: table.clone(),
                         reason: HaltReason::SinkRejected { detail },
                         at,
```

`crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index d9840d6..5d63fc4 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -61,7 +61,7 @@ pub use pipeline::{
 };
 pub use removal::{REMOVALS_PREFIX, RemovalDoc, Removals, removal_key};
 pub use snapshot::{
-    CursorKey, JOBS_PREFIX, JobStatus, JobTable, JobTableStatus, SnapshotCursor, SnapshotJobDoc,
-    SnapshotStatus, TableSnapshotDoc, job_key, table_snapshot_key,
+    CursorKey, JOBS_PREFIX, JobStatus, JobTable, JobTableStatus, KEPT_ENDED_JOBS, SnapshotCursor,
+    SnapshotJobDoc, SnapshotStatus, TableSnapshotDoc, job_key, table_snapshot_key,
 };
 pub use tables::{HALTS_KEY, HaltsDoc, SCHEMAS_PREFIX, TableHalt, table_schema_key};
```

`crates/data-avalanche-state/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index 174feb7..6d7e60c 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -217,6 +217,9 @@ pub fn table_snapshot_key(table: &TableRef) -> String {
 
 pub const JOBS_PREFIX: &str = "snapshots/jobs";
 
+/// How many ended jobs a pipeline keeps; the owner deletes the older ones when a job ends.
+pub const KEPT_ENDED_JOBS: usize = 100;
+
 /// `snapshots/jobs/<job_id>.json`.
 pub fn job_key(job_id: &str) -> String {
     format!("{JOBS_PREFIX}/{}.json", escape_segment(job_id))
@@ -327,6 +330,28 @@ impl PipelineDocs {
         Ok(Some(v.doc))
     }
 
+    /// Deletes the ended jobs past the newest `keep`, newest by `finished_at` (DA-SNAP-16); an
+    /// unfinished job is never deleted. The number deleted.
+    ///
+    /// # Errors
+    /// A storage failure.
+    pub async fn prune_snapshot_jobs(&self, keep: usize) -> Result<usize, StateError> {
+        let mut ended: Vec<SnapshotJobDoc> = self
+            .snapshot_jobs()
+            .await?
+            .into_iter()
+            .map(|v| v.doc)
+            .filter(SnapshotJobDoc::is_finished)
+            .collect();
+        ended.sort_by(|a, b| (b.finished_at, &b.job_id).cmp(&(a.finished_at, &a.job_id)));
+        let mut deleted = 0;
+        for job in ended.iter().skip(keep) {
+            self.docs.store().delete(&job_key(&job.job_id)).await?;
+            deleted += 1;
+        }
+        Ok(deleted)
+    }
+
     /// The jobs not finished yet, oldest request first.
     ///
     /// # Errors
@@ -341,15 +366,21 @@ impl PipelineDocs {
             .collect())
     }
 
-    /// Every job of this pipeline, oldest request first.
+    /// Every job of this pipeline, oldest request first. A job document this build can't decode
+    /// (another release's, or a damaged one) is skipped with a warning (DA-SNAP-20).
     ///
     /// # Errors
-    /// A storage or decode failure (one bad job document fails the listing).
+    /// A storage failure.
     pub async fn snapshot_jobs(&self) -> Result<Vec<Versioned<SnapshotJobDoc>>, StateError> {
         let mut jobs = vec![];
         for key in self.docs.store().list(JOBS_PREFIX).await? {
-            if let Some(job) = self.docs.load::<SnapshotJobDoc>(&key).await? {
-                jobs.push(job);
+            match self.docs.load::<SnapshotJobDoc>(&key).await {
+                Ok(Some(job)) => jobs.push(job),
+                Ok(None) => {}
+                Err(e @ (StateError::Corrupt { .. } | StateError::UnsupportedVersion { .. })) => {
+                    tracing::warn!("snapshot job {key} skipped: {e}");
+                }
+                Err(e) => return Err(e),
             }
         }
         jobs.sort_by(|a, b| {
```

`crates/data-avalanche-state/src/tables.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/tables.rs b/crates/data-avalanche-state/src/tables.rs
index 44235b8..f5d2dad 100644
--- a/crates/data-avalanche-state/src/tables.rs
+++ b/crates/data-avalanche-state/src/tables.rs
@@ -49,6 +49,10 @@ pub struct TableHalt {
     pub reason: HaltReason,
     /// The sink holds the table's changes through this position; later ones were dropped.
     pub at: SourcePosition,
+    /// A job's copy of a table outside the config (Q-M9b-3): it fails its job table, and it
+    /// is kept out of the halted tables shown (DA-SNAP-19). The next start clears it.
+    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
+    pub copy: bool,
 }
 
 /// `halts.json`: the tracked tables that halted, in table order.
```

`crates/data-avalanche-state/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/test_support.rs b/crates/data-avalanche-state/src/test_support.rs
index fac01d3..c6e4dff 100644
--- a/crates/data-avalanche-state/src/test_support.rs
+++ b/crates/data-avalanche-state/src/test_support.rs
@@ -105,4 +105,7 @@ impl VersionedStore for WriteThenLoseResponse {
     async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
         self.inner.list(prefix).await
     }
+    async fn delete(&self, key: &str) -> Result<(), StoreError> {
+        self.inner.delete(key).await
+    }
 }
```

`crates/data-avalanche-storage/src/store.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/store.rs b/crates/data-avalanche-storage/src/store.rs
index 78b8ac7..d9fb369 100644
--- a/crates/data-avalanche-storage/src/store.rs
+++ b/crates/data-avalanche-storage/src/store.rs
@@ -7,7 +7,8 @@ use bytes::Bytes;
 use futures_util::TryStreamExt;
 use object_store::path::Path;
 use object_store::{
-    GetOptions, ObjectMeta, ObjectStore, PutMode, PutOptions, PutPayload, PutResult, UpdateVersion,
+    GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
+    PutResult, UpdateVersion,
 };
 
 /// The version token of a stored object: whatever the backend compares on a conditional update.
@@ -89,6 +90,10 @@ pub trait VersionedStore: Send + Sync + std::fmt::Debug {
     /// Every key under `prefix/`, sorted (`a/b` lists `a/b/c`, not `a/bc`); `""` lists the whole
     /// root.
     async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
+    /// Deletes unconditionally; an absent object is no error. For documents no one updates any
+    /// more (an ended snapshot job, DA-SNAP-16): never a lease or a pointer (deleting a lease
+    /// restarts its epoch).
+    async fn delete(&self, key: &str) -> Result<(), StoreError>;
 }
 
 /// [`VersionedStore`] over an `object_store` bucket, rooted at a key prefix (`""` = bucket root).
@@ -105,6 +110,7 @@ enum Op {
     Update,
     Put,
     List,
+    Delete,
 }
 
 impl ObjectStoreVersioned {
@@ -243,6 +249,14 @@ impl VersionedStore for ObjectStoreVersioned {
         keys.sort();
         Ok(keys)
     }
+
+    async fn delete(&self, key: &str) -> Result<(), StoreError> {
+        let path = self.path(key)?;
+        match self.store.delete(&path).await {
+            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
+            Err(e) => Err(map_error(Op::Delete, key, e)),
+        }
+    }
 }
 
 #[cfg(test)]
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-storage -p data-avalanche-state -p data-avalanche-pipeline -p data-avalanche-cluster -p data-avalanche-api --no-fail-fast
```

Expected: PASS, 593 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/pipelines.rs \
  crates/data-avalanche-api/src/snapshots.rs \
  crates/data-avalanche-cluster/src/coordinator.rs \
  crates/data-avalanche-cluster/src/jobs.rs \
  crates/data-avalanche-cluster/src/test_support.rs \
  crates/data-avalanche-pipeline/src/jobs.rs \
  crates/data-avalanche-pipeline/src/session.rs \
  crates/data-avalanche-pipeline/src/test_support.rs \
  crates/data-avalanche-pipeline/src/worker.rs \
  crates/data-avalanche-pipeline/src/writer.rs \
  crates/data-avalanche-state/src/lib.rs \
  crates/data-avalanche-state/src/snapshot.rs \
  crates/data-avalanche-state/src/tables.rs \
  crates/data-avalanche-state/src/test_support.rs \
  crates/data-avalanche-storage/src/store.rs \
  crates/data-avalanche-storage/tests/versioned_store.rs
git commit -m "feat(state): ended jobs pruned past the newest 100, an undecodable one skipped, a copy's halt kept out of the halted tables, a lost restart asked again (DA-SNAP-16, 19, 20, 21)"
```

---

### Task 8: The reconciler's log, one entry per key (`pipeline`)

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/reconcile.rs`
- Test: `crates/data-avalanche-pipeline/src/reconcile.rs`, `crates/data-avalanche-pipeline/src/session/snapshot.rs`

**Gaps:** DA-SNAP-11.

**Interfaces:**
- Consumes: `Reconciler::{log, retain, clear}` (M6a); `data_avalanche_reconcile_log_keys` (M6b).
- Produces: the log keeps each `(table, key)` under its latest writer only (`writers: HashMap<(TableRef, Key), u64>`); `forget` moves a key off an older entry and drops an emptied one.

While a snapshotting table's reads wait, every stream transaction that wrote it was logged
with its keys, so a hot key written a million times was logged a million times. A chunk row for
a key is kept only when the read saw the key's latest writer, and then the row holds the latest
change already: an older writer of the key decides nothing. The log now holds each key once,
under its latest writer, so its size is bounded by the distinct keys written during the wait.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-pipeline/src/reconcile.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/reconcile.rs b/crates/data-avalanche-pipeline/src/reconcile.rs
index c3460fd..ccb8f60 100644
--- a/crates/data-avalanche-pipeline/src/reconcile.rs
+++ b/crates/data-avalanche-pipeline/src/reconcile.rs
@@ -314,7 +314,7 @@ mod tests {
         let mut r = Reconciler::default();
         let c = r.issue();
         r.applied(700, wrote("a", &[1, 2]));
-        r.applied(701, wrote("a", &[2, 3]));
+        r.applied(701, wrote("a", &[3, 4]));
         r.applied(702, Touched::default());
         assert_eq!(
             r.size(),
@@ -346,4 +346,53 @@ mod tests {
         assert_eq!(ids(&kept), [1, 3]);
         assert!(r.logged().is_empty(), "no read is open");
     }
+
+    /// DA-SNAP-11: a key is logged under its latest writer only, so the log holds each key
+    /// once however often it is written while the reads wait.
+    #[test]
+    fn a_key_written_again_is_logged_once_under_its_latest_writer() {
+        let mut r = Reconciler::default();
+        for xid in 700..800 {
+            r.applied(xid, wrote("a", &[1, 2]));
+        }
+        r.applied(800, wrote("b", &[1]));
+        assert_eq!(
+            r.size(),
+            ReconcileLog {
+                entries: 2,
+                keys: 3
+            }
+        );
+        assert_eq!(r.logged(), [799, 800]);
+        let c = r.issue();
+        let kept = r.reconcile(c, &t("a"), Some(&snap(801, &[799])), chunk(&[1, 2, 3]));
+        assert_eq!(ids(&kept), [3], "the latest writer wasn't seen");
+        let c = r.issue();
+        let kept = r.reconcile(c, &t("a"), Some(&snap(801, &[])), chunk(&[1, 2, 3]));
+        assert_eq!(
+            ids(&kept),
+            [1, 2, 3],
+            "the latest writer was seen: its rows are current"
+        );
+        assert!(r.logged().is_empty());
+        r.applied(900, wrote("a", &[1]));
+        assert_eq!(
+            r.logged(),
+            [900],
+            "a key whose writer went is logged afresh"
+        );
+    }
+
+    /// A writer that keeps some of its keys stays, with those keys.
+    #[test]
+    fn a_writer_keeps_the_keys_no_later_one_wrote() {
+        let mut r = Reconciler::default();
+        r.applied(700, wrote("a", &[1, 2]));
+        r.applied(701, wrote("a", &[2]));
+        assert_eq!(r.logged(), [700, 701]);
+        assert_eq!(r.size().keys, 2);
+        let c = r.issue();
+        let kept = r.reconcile(c, &t("a"), Some(&snap(702, &[700])), chunk(&[1, 2]));
+        assert_eq!(ids(&kept), [2], "700 still drops key 1; 701 was seen");
+    }
 }
```

`crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
index 6598134..2852907 100644
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -1883,9 +1883,10 @@ mod tests {
                     .update(&t("a"), row(2, "s")),
             );
             w.source.commit(FakeTxn::new().update(&t("a"), row(2, "t")));
+            // Key 2 is logged under its latest writer only (DA-SNAP-11).
             let logged = ReconcileLog {
                 entries: 2,
-                keys: 3,
+                keys: 2,
             };
             wait_until("the log is reported", || {
                 status.borrow().reconcile_log == logged
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-pipeline --no-fail-fast
```

Expected: FAIL, the new tests:
  - `reconcile::tests::a_key_written_again_is_logged_once_under_its_latest_writer`
  - `reconcile::tests::a_writer_keeps_the_keys_no_later_one_wrote`
  - `session::snapshot::tests::the_reconciler_logs_size_is_reported`

- [ ] **Step 3: Implement**

`crates/data-avalanche-pipeline/src/reconcile.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/reconcile.rs b/crates/data-avalanche-pipeline/src/reconcile.rs
index ccb8f60..699ecce 100644
--- a/crates/data-avalanche-pipeline/src/reconcile.rs
+++ b/crates/data-avalanche-pipeline/src/reconcile.rs
@@ -14,6 +14,11 @@
 //! saw. A source that can't say what a chunk's read saw ([`Chunk::visibility`] `None`) gets the
 //! window rule: the transactions applied after the chunk was issued.
 //!
+//! A key is logged under its latest writer only (DA-SNAP-11): a chunk row for the key is kept
+//! only when the read saw that writer, and then it holds the latest change already, so an older
+//! writer of the key decides nothing. The log holds each key written once, however often it is
+//! written while a read waits.
+//!
 //! [`Chunk::visibility`]: data_avalanche_core::Chunk::visibility
 
 use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
@@ -71,6 +76,8 @@ pub(crate) struct Reconciler {
     keys: usize,
     /// The chunks being read: id → when issued.
     outstanding: BTreeMap<u64, u64>,
+    /// Each logged key's latest writer, by its `at`.
+    writers: HashMap<(TableRef, Key), u64>,
 }
 
 impl Reconciler {
@@ -83,6 +90,13 @@ impl Reconciler {
     pub fn applied(&mut self, xid: u64, touched: Touched) {
         let at = self.tick();
         if !touched.is_empty() {
+            for (table, keys) in &touched.keys {
+                for key in keys {
+                    if let Some(older) = self.writers.insert((table.clone(), key.clone()), at) {
+                        self.forget(older, table, key);
+                    }
+                }
+            }
             self.keys += touched.key_count();
             self.log.push(Entry {
                 at,
@@ -93,6 +107,25 @@ impl Reconciler {
         }
     }
 
+    /// Takes `key` of `table` out of the entry applied at `at`; an entry left with nothing goes.
+    fn forget(&mut self, at: u64, table: &TableRef, key: &Key) {
+        let Ok(i) = self.log.binary_search_by_key(&at, |e| e.at) else {
+            return;
+        };
+        let e = &mut self.log[i];
+        if let Some(keys) = e.touched.keys.get_mut(table)
+            && keys.remove(key)
+        {
+            self.keys -= 1;
+            if keys.is_empty() {
+                e.touched.keys.remove(table);
+            }
+        }
+        if e.touched.is_empty() {
+            self.log.remove(i);
+        }
+    }
+
     /// A chunk read starts.
     pub fn issue(&mut self) -> Ticket {
         let issued = self.tick();
@@ -150,6 +183,7 @@ impl Reconciler {
         };
         let horizon = self.outstanding.values().min().copied().unwrap_or(u64::MAX);
         let keys = &mut self.keys;
+        let writers = &mut self.writers;
         self.log.retain(|e| {
             let keep = match visibility {
                 Some(_) => e.seen.is_none_or(|seen| seen >= horizon),
@@ -157,6 +191,11 @@ impl Reconciler {
             };
             if !keep {
                 *keys -= e.touched.key_count();
+                for (table, ks) in &e.touched.keys {
+                    for k in ks {
+                        writers.remove(&(table.clone(), k.clone()));
+                    }
+                }
             }
             keep
         });
@@ -168,6 +207,7 @@ impl Reconciler {
         self.log.clear();
         self.keys = 0;
         self.outstanding.clear();
+        self.writers.clear();
     }
 
     /// The log's size: its transactions and their keys.
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-pipeline --no-fail-fast
```

Expected: PASS, 264 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline/src/reconcile.rs \
  crates/data-avalanche-pipeline/src/session/snapshot.rs
git commit -m "fix(pipeline): the reconciler logs a key once, under its latest writer (DA-SNAP-11)"
```

---

### Task 9: PostgreSQL chunk reads on their own connections (`source-pg`)

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/connect.rs`, `crates/data-avalanche-source-pg/src/lib.rs`, `crates/data-avalanche-source-pg/src/snapshot.rs`
- Test: `crates/data-avalanche-source-pg/tests/pg/snapshot.rs`

**Gaps:** DA-SNAP-12.

**Interfaces:**
- Consumes: the source's `Connector` and its catalog connection, `bounded`, `abandon` (M5, M12a).
- Produces: `IDLE_READS = 4`; `Connector::read(what, after, sql) -> Result<Vec<SimpleQueryMessage>, SourceError>` (an idle connection or a new one; the timeout counted once it has one; on timeout it cancels and drops its own connection); `cancel()` factored out of `abandon()`.

Chunk reads shared the one catalog connection, so with `parallelism > 1` they ran one after
another, and a read queued behind a slow one spent its timeout waiting. Each chunk read now takes
a connection of its own: an idle one, or a new one. Up to 4 idle ones are kept. The catalog
connection stays the stream's (channel C, the re-queries). The read's `statement_timeout` is set
per transaction (`SET LOCAL`), and its deadline starts once it has a connection.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-source-pg/tests/pg/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/snapshot.rs b/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
index 81ad2ae..c1f019d 100644
--- a/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
@@ -590,3 +590,79 @@ async fn a_timed_out_chunk_frees_the_connection() {
     locker.batch_execute("ROLLBACK").await.unwrap();
     cleanup(&a, &n).await;
 }
+
+/// DA-SNAP-12: chunk reads run on connections of their own, so a read that waits (here on a
+/// lock) holds up neither another table's read nor a catalog call, and each read's timeout
+/// starts when it runs. Idle read connections are kept for the next reads, up to IDLE_READS.
+#[tokio::test]
+async fn a_waiting_chunk_read_holds_up_no_other_read() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(
+        &a,
+        "chunks_parallel",
+        "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY);
+         INSERT INTO a VALUES (1); INSERT INTO b VALUES (1), (2);",
+    )
+    .await;
+    let p = pipeline(&n, &["*"], "", "");
+    let c = std::sync::Arc::new(connector(&p));
+    let tables = resolved(&c).await.tables;
+    let of = |name: &str| {
+        tables
+            .iter()
+            .find(|t| t.table().name == name)
+            .unwrap()
+            .clone()
+    };
+    let (ta, tb) = (of("a"), of("b"));
+    let locker = admin().await;
+    locker
+        .batch_execute(&format!(
+            "BEGIN; LOCK TABLE {}.a IN ACCESS EXCLUSIVE MODE",
+            n.schema
+        ))
+        .await
+        .unwrap();
+    let waiting = tokio::spawn({
+        let c = c.clone();
+        let ta = ta.clone();
+        async move { c.read_chunk(&ta, None, 10).await }
+    });
+    tokio::time::sleep(Duration::from_millis(300)).await;
+    let b = tokio::time::timeout(Duration::from_secs(2), c.read_chunk(&tb, None, 10))
+        .await
+        .expect("table b's read doesn't wait for table a's")
+        .unwrap();
+    assert_eq!(b.rows.len(), 2);
+    tokio::time::timeout(Duration::from_secs(2), c.current_position())
+        .await
+        .expect("the catalog connection is free")
+        .unwrap();
+    assert!(
+        !waiting.is_finished(),
+        "table a's read still waits on the lock"
+    );
+    locker.batch_execute("ROLLBACK").await.unwrap();
+    assert_eq!(waiting.await.unwrap().unwrap().rows.len(), 1);
+    let connections = async || {
+        a.query_one(
+            "SELECT count(*) FROM pg_stat_activity \
+             WHERE application_name = $1 AND datname = current_database()",
+            &[&data_avalanche_source_pg::APPLICATION_NAME],
+        )
+        .await
+        .unwrap()
+        .get::<_, i64>(0)
+    };
+    let before = connections().await;
+    for _ in 0..3 {
+        c.read_chunk(&tb, None, 10).await.unwrap();
+    }
+    assert_eq!(
+        connections().await,
+        before,
+        "a read takes an idle connection"
+    );
+    cleanup(&admin().await, &n).await;
+}
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-source-pg --no-fail-fast -- --skip tls::
```

Expected: FAIL, the new tests:
  - `snapshot::a_waiting_chunk_read_holds_up_no_other_read`

- [ ] **Step 3: Implement**

`crates/data-avalanche-source-pg/src/connect.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connect.rs b/crates/data-avalanche-source-pg/src/connect.rs
index 5671a70..09565bd 100644
--- a/crates/data-avalanche-source-pg/src/connect.rs
+++ b/crates/data-avalanche-source-pg/src/connect.rs
@@ -1,17 +1,18 @@
 //! The two connections to the source: the catalog connection (`tokio-postgres`: catalog channels,
 //! snapshot chunks, slot and publication DDL) and the replication connection
 //! (`pgwire-replication`). Both run with the same pinned session settings, so the text the
-//! decoder parses has one form (D-M5.5).
+//! decoder parses has one form (D-M5.5). Snapshot chunks are read on connections of their own,
+//! one per concurrent read, kept idle between reads (DA-SNAP-12).
 
 use std::future::Future;
-use std::sync::Arc;
+use std::sync::{Arc, PoisonError};
 use std::time::Duration;
 
 use data_avalanche_config::{Source, SslMode, Timeouts};
 use data_avalanche_core::SourceError;
 use pgwire_replication::{Lsn, ReplicationConfig};
 use tokio::sync::Mutex;
-use tokio_postgres::Client;
+use tokio_postgres::{Client, SimpleQueryMessage};
 use tokio_postgres_rustls::MakeRustlsConnect;
 
 use crate::error::{sql_error, timed_out};
@@ -23,6 +24,10 @@ pub(crate) const SERVER_FIRST: Duration = Duration::from_secs(5);
 
 /// `d` as a server-side timeout in milliseconds: at least 1 (0 turns a timeout off) and at most
 /// what the setting holds (`i32::MAX`).
+/// The most idle chunk-read connections kept: as many as `snapshots.parallelism` reads without a
+/// warning (V-038).
+pub const IDLE_READS: usize = 4;
+
 pub(crate) fn server_millis(d: Duration) -> u128 {
     d.as_millis().clamp(1, i32::MAX as u128)
 }
@@ -51,6 +56,8 @@ pub(crate) struct Connector {
     files: PemFiles,
     tls: Option<MakeRustlsConnect>,
     client: Mutex<Option<Arc<Client>>>,
+    /// Chunk-read connections between reads (DA-SNAP-12).
+    reads: std::sync::Mutex<Vec<Client>>,
 }
 
 impl Connector {
@@ -77,6 +84,7 @@ impl Connector {
             files,
             tls,
             client: Mutex::new(None),
+            reads: std::sync::Mutex::new(Vec::new()),
         })
     }
 
@@ -179,6 +187,54 @@ impl Connector {
         }
     }
 
+    /// Runs `sql` on a chunk-read connection of its own, under a client-side timeout from when
+    /// it has one: an idle one, or one connected for it within `timeouts.connect`. Reads don't
+    /// queue behind one another, and a timeout abandons only its own connection (DA-SNAP-12).
+    pub(crate) async fn read(
+        &self,
+        what: &str,
+        after: Duration,
+        sql: &str,
+    ) -> Result<Vec<SimpleQueryMessage>, SourceError> {
+        let idle = {
+            let mut reads = self.reads.lock().unwrap_or_else(PoisonError::into_inner);
+            reads.retain(|c| !c.is_closed());
+            reads.pop()
+        };
+        let client = match idle {
+            Some(c) => c,
+            None => self.connect().await?,
+        };
+        match tokio::time::timeout(after, client.simple_query(sql)).await {
+            Ok(r) => {
+                if !client.is_closed() {
+                    let mut reads = self.reads.lock().unwrap_or_else(PoisonError::into_inner);
+                    if reads.len() < IDLE_READS {
+                        reads.push(client);
+                    }
+                }
+                r.map_err(sql_error)
+            }
+            Err(_) => {
+                self.cancel(&client).await;
+                Err(timed_out(what, after))
+            }
+        }
+    }
+
+    /// Asks the server to cancel what `client` runs, within `timeouts.connect`; a cancel that
+    /// fails changes nothing.
+    async fn cancel(&self, client: &Client) {
+        let token = client.cancel_token();
+        let _ = tokio::time::timeout(self.timeouts.connect, async {
+            match &self.tls {
+                None => token.cancel_query(tokio_postgres::NoTls).await,
+                Some(tls) => token.cancel_query(tls.clone()).await,
+            }
+        })
+        .await;
+    }
+
     /// Drops `client` from the cache and asks the server to cancel what it runs there, within
     /// `timeouts.connect`; a cancel that fails changes nothing. A client another call abandoned
     /// already, and one connected since, are left alone. Calls that share the connection can see
@@ -191,14 +247,7 @@ impl Connector {
             }
             *slot = None;
         }
-        let token = client.cancel_token();
-        let _ = tokio::time::timeout(self.timeouts.connect, async {
-            match &self.tls {
-                None => token.cancel_query(tokio_postgres::NoTls).await,
-                Some(tls) => token.cancel_query(tls.clone()).await,
-            }
-        })
-        .await;
+        self.cancel(client).await;
     }
 
     /// The replication connection for `slot` and `publication`, starting after `start`. The status
```

`crates/data-avalanche-source-pg/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/lib.rs b/crates/data-avalanche-source-pg/src/lib.rs
index 5bd1218..0b0aa52 100644
--- a/crates/data-avalanche-source-pg/src/lib.rs
+++ b/crates/data-avalanche-source-pg/src/lib.rs
@@ -20,7 +20,7 @@ pub mod types;
 pub mod value;
 
 pub use catalog::{Attribute, Described, Notes, Relation, Selection};
-pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
+pub use connect::{APPLICATION_NAME, IDLE_READS, SESSION_OPTIONS};
 pub use connector::{MIN_SERVER_VERSION, PgConnector};
 pub use pgoutput::{Datum, Message, OldKind, RelationColumn};
 pub use snapshot::FETCH_BATCH;
```

`crates/data-avalanche-source-pg/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/snapshot.rs b/crates/data-avalanche-source-pg/src/snapshot.rs
index 1a2ce40..917cb94 100644
--- a/crates/data-avalanche-source-pg/src/snapshot.rs
+++ b/crates/data-avalanche-source-pg/src/snapshot.rs
@@ -528,8 +528,16 @@ impl PgConnector {
             "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
              SELECT pg_catalog.pg_current_snapshot()::text; {sql}"
         );
-        let messages = simple_query(self, "read a snapshot chunk", table, &sql)
+        let command = self.conn.timeouts.command;
+        let sql = format!(
+            "SET LOCAL statement_timeout = {}; {sql}",
+            server_millis(command)
+        );
+        let messages = self
+            .conn
+            .read("read a snapshot chunk", command + SERVER_FIRST, &sql)
             .await
+            .map_err(|e| dropped(table, e))
             .map_err(|e| chunk_error(table, e))?;
         // Each SELECT's rows follow its row description: the snapshot's, then the chunk's.
         let mut selects = messages
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-source-pg --no-fail-fast -- --skip tls::
```

Expected: PASS, 160 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/connect.rs \
  crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/snapshot.rs \
  crates/data-avalanche-source-pg/tests/pg/snapshot.rs
git commit -m "feat(source-pg): each chunk read runs on a connection of its own, up to 4 kept idle (DA-SNAP-12)"
```

---

### Task 10: The key fetch and the connection cap (`config`, `api`)

**Files:**
- Modify: `crates/data-avalanche-api/src/auth.rs`, `crates/data-avalanche-api/src/server.rs`, `crates/data-avalanche-config/src/registry.rs`, `crates/data-avalanche-config/src/rules/auth.rs`, `docs/src/content/docs/reference/validation-rules.md`
- Test: `crates/data-avalanche-api/src/auth.rs`, `crates/data-avalanche-api/src/server.rs`, `crates/data-avalanche-config/src/registry.rs`, `crates/data-avalanche-config/tests/rules/auth.rs`

**Gaps:** DA-API-16, DA-API-17.

**Interfaces:**
- Consumes: the API's `Keys` fetch (JWKS, discovery) and its `reqwest` client; `server::serve`, the TLS listener (M9a); the rule registry.
- Produces:
  - `MAX_KEY_DOCUMENT = 1 << 20`; the key client follows no redirect (a `3xx` is an error), and reads at most that many bytes (by `content-length`, and while reading chunks);
  - V-125 (warning, config): `globals.api.jwt.jwks_url`, `globals.api.oauth2.jwks_url` or `discovery_url` not `https`;
  - `MAX_CONNECTIONS = 1024`; `Counted<S>`, a stream that holds a semaphore permit; the plain listener (`Capped`) and the TLS one take a permit before they accept; `Bound.max_connections` (tests lower it).

The JWKS and discovery fetches accepted `http`, followed redirects and read any body, so a
redirect or a huge answer from the network path could swap the keys or exhaust memory. The
listener spawned a task per connection with no cap. A fetch now refuses a redirect and reads at
most 1 MiB; a non-`https` key URL warns (V-125, as V-052 does for the API's own TLS). Past 1024
open connections a node stops accepting until one closes (Q-M12b-4).

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/auth.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/auth.rs b/crates/data-avalanche-api/src/auth.rs
index eb5ad87..b9fc97b 100644
--- a/crates/data-avalanche-api/src/auth.rs
+++ b/crates/data-avalanche-api/src/auth.rs
@@ -581,6 +581,60 @@ mod tests {
         }
     }
 
+    /// DA-API-16: a key document's fetch follows no redirect and reads at most
+    /// [`MAX_KEY_DOCUMENT`] bytes.
+    #[tokio::test]
+    async fn a_key_fetch_follows_no_redirect_and_reads_a_bounded_body() {
+        use axum::response::{IntoResponse, Redirect};
+        let app = axum::Router::new()
+            .route(
+                "/moved",
+                axum::routing::get(|| async { Redirect::temporary("/keys") }),
+            )
+            .route("/keys", axum::routing::get(|| async { r#"{"keys": []}"# }))
+            .route(
+                "/big",
+                axum::routing::get(|| async { "x".repeat(MAX_KEY_DOCUMENT + 1).into_response() }),
+            );
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let url = format!("http://{}", listener.local_addr().unwrap());
+        tokio::spawn(async move { axum::serve(listener, app).await });
+        // A chunked body, no length announced.
+        let raw = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let streamed = format!("http://{}/streamed", raw.local_addr().unwrap());
+        tokio::spawn(async move {
+            use tokio::io::{AsyncReadExt, AsyncWriteExt};
+            let (mut sock, _) = raw.accept().await.unwrap();
+            let mut request = [0; 1024];
+            let _ = sock.read(&mut request).await;
+            let head = "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n";
+            sock.write_all(head.as_bytes()).await.unwrap();
+            let chunk = format!("1000\r\n{}\r\n", " ".repeat(4096));
+            for _ in 0..=MAX_KEY_DOCUMENT / 4096 {
+                if sock.write_all(chunk.as_bytes()).await.is_err() {
+                    return;
+                }
+            }
+            let _ = sock.write_all(b"0\r\n\r\n").await;
+        });
+        let timing = Timing {
+            refetch_after: Duration::ZERO,
+            max_age: Duration::from_secs(60),
+        };
+        let keys = Keys::new(&url, KeySource::Jwks(String::new()), timing);
+        let set: JwkSet = keys.get(&format!("{url}/keys")).await.unwrap();
+        assert!(set.keys.is_empty());
+        let e = keys
+            .get::<JwkSet>(&format!("{url}/moved"))
+            .await
+            .unwrap_err();
+        assert!(e.contains("redirects aren't followed"), "{e}");
+        for long in [format!("{url}/big"), streamed] {
+            let e = keys.get::<JwkSet>(&long).await.unwrap_err();
+            assert!(e.contains("longer than 1048576 bytes"), "{long}: {e}");
+        }
+    }
+
     #[tokio::test]
     async fn without_auth_every_caller_is_admin() {
         let auth = Auth::new(&config().globals.api);
```

`crates/data-avalanche-api/src/server.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/server.rs b/crates/data-avalanche-api/src/server.rs
index 75b021d..e7960c1 100644
--- a/crates/data-avalanche-api/src/server.rs
+++ b/crates/data-avalanche-api/src/server.rs
@@ -294,6 +294,54 @@ mod tests {
         assert!(free, "{addr} is still bound");
     }
 
+    /// DA-API-17: past the cap, a new connection waits until an open one closes; plain and TLS
+    /// alike, a TLS handshake holding a place.
+    #[tokio::test]
+    async fn past_the_cap_a_connection_waits_for_one_to_close() {
+        let (id, _) = identity();
+        for tls in [None, Some(&id)] {
+            let mut bound = bind(
+                &HostPort {
+                    host: "127.0.0.1".into(),
+                    port: 0,
+                },
+                tls,
+            )
+            .await
+            .unwrap();
+            bound.max_connections = 2;
+            let addr = bound.local_addr().unwrap();
+            let api = crate::test_support::api(crate::test_support::open());
+            tokio::spawn(bound.serve(router(api), std::future::pending()));
+            // Two connections that say nothing: a TLS one holds its place in the handshake.
+            let held = [
+                TcpStream::connect(addr).await.unwrap(),
+                TcpStream::connect(addr).await.unwrap(),
+            ];
+            tokio::time::sleep(Duration::from_millis(100)).await;
+            let mut third = TcpStream::connect(addr).await.unwrap();
+            third
+                .write_all(b"GET /health/live HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
+                .await
+                .unwrap();
+            let mut byte = [0; 1];
+            let waited =
+                tokio::time::timeout(Duration::from_millis(300), third.read(&mut byte)).await;
+            assert!(
+                waited.is_err(),
+                "tls {}: the third is answered at once",
+                tls.is_some()
+            );
+            drop(held);
+            let answered = tokio::time::timeout(Duration::from_secs(2), third.read(&mut byte))
+                .await
+                .expect("the third is taken once one closes");
+            if tls.is_none() {
+                assert_eq!(answered.unwrap(), 1, "plain: an answer");
+            }
+        }
+    }
+
     #[tokio::test]
     async fn a_cert_without_a_certificate_is_refused_at_bind() {
         let (mut id, _) = identity();
```

`crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index f044561..a2592c9 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1057,7 +1057,7 @@ mod tests {
 
     #[test]
     fn has_every_spec_rule_once_in_order() {
-        assert_eq!(RULES.len(), 118);
+        assert_eq!(RULES.len(), 119);
         assert!(
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
```

`crates/data-avalanche-config/tests/rules/auth.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/auth.rs b/crates/data-avalanche-config/tests/rules/auth.rs
index f7ccb13..28880f1 100644
--- a/crates/data-avalanche-config/tests/rules/auth.rs
+++ b/crates/data-avalanche-config/tests/rules/auth.rs
@@ -120,6 +120,15 @@ pub fn cases() -> Vec<Case> {
         case("V-051", Silent, "oauth2 with discovery", || {
             pg_delta().set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "discovery_url": "https://idp/.well-known/openid-configuration"}}))
         }),
+        case("V-125", Warning, "jwt keys over http", || {
+            pg_delta().set("/globals/api", json!({"auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "http://idp/certs"}}))
+        }),
+        case("V-125", Warning, "oauth2 discovery over http", || {
+            pg_delta().set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "discovery_url": "HTTP://idp/.well-known/openid-configuration"}}))
+        }),
+        case("V-125", Silent, "oauth2 keys over https", || {
+            pg_delta().set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "jwks_url": "HTTPS://idp/certs"}}))
+        }),
         case("V-052", Warning, "jwt without tls", || {
             pg_delta().set("/globals/api", json!({"auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"}}))
         }),
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-config -p data-avalanche-api -p xtask --no-fail-fast
```

Expected: FAIL to compile:
  - `error[E0425]: cannot find value `MAX_KEY_DOCUMENT` in this scope (crates/data-avalanche-api/src/auth.rs)`
  - `error[E0609]: no field `max_connections` on type `server::Bound` (crates/data-avalanche-api/src/server.rs)`

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/auth.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/auth.rs b/crates/data-avalanche-api/src/auth.rs
index b9fc97b..f6a7aa2 100644
--- a/crates/data-avalanche-api/src/auth.rs
+++ b/crates/data-avalanche-api/src/auth.rs
@@ -23,6 +23,9 @@ use tokio::sync::watch;
 
 /// A JWKS fetch, or a discovery, takes at most this long.
 pub const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
+
+/// The largest JWKS or discovery document read (DA-API-16): a longer one fails the fetch.
+pub const MAX_KEY_DOCUMENT: usize = 1 << 20;
 /// A token signed by a key the cached JWKS lacks fetches the JWKS again, at most this often
 /// (a key rotation; a flood of tokens with made-up key ids fetches nothing more).
 pub const REFETCH_AFTER: Duration = Duration::from_secs(30);
@@ -415,6 +418,8 @@ impl Keys {
             timing,
             http: reqwest::Client::builder()
                 .timeout(FETCH_TIMEOUT)
+                // The key URLs are the configured ones: a redirect is refused (DA-API-16).
+                .redirect(reqwest::redirect::Policy::none())
                 .build()
                 .expect("the HTTP client builds"),
             cache: Mutex::new(Cache::default()),
@@ -526,17 +531,35 @@ impl Keys {
         self.get(&url).await
     }
 
+    /// The document at `url`: a redirect isn't followed, and a body past
+    /// [`MAX_KEY_DOCUMENT`] isn't read (DA-API-16).
     async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, String> {
-        let body = self
+        let mut r = self
             .http
             .get(url)
             .send()
             .await
             .and_then(reqwest::Response::error_for_status)
-            .map_err(|e| format!("{url}: {e}"))?
-            .bytes()
-            .await
             .map_err(|e| format!("{url}: {e}"))?;
+        if r.status().is_redirection() {
+            return Err(format!(
+                "{url}: answered {}, and redirects aren't followed: configure the URL it names",
+                r.status()
+            ));
+        }
+        let too_long = || format!("{url}: the document is longer than {MAX_KEY_DOCUMENT} bytes");
+        if r.content_length()
+            .is_some_and(|n| n > MAX_KEY_DOCUMENT as u64)
+        {
+            return Err(too_long());
+        }
+        let mut body = Vec::new();
+        while let Some(chunk) = r.chunk().await.map_err(|e| format!("{url}: {e}"))? {
+            if body.len() + chunk.len() > MAX_KEY_DOCUMENT {
+                return Err(too_long());
+            }
+            body.extend_from_slice(&chunk);
+        }
         serde_json::from_slice(&body).map_err(|e| format!("{url}: {e}"))
     }
 }
```

`crates/data-avalanche-api/src/server.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/server.rs b/crates/data-avalanche-api/src/server.rs
index e7960c1..4c2e509 100644
--- a/crates/data-avalanche-api/src/server.rs
+++ b/crates/data-avalanche-api/src/server.rs
@@ -1,10 +1,14 @@
 //! The listener on `globals.api.address`: plain HTTP, or TLS with `globals.api.tls` (config
 //! §6.3.2). Both are PROC: a reload changes neither. A TLS handshake runs off the accept loop,
-//! bounded by [`HANDSHAKE_TIMEOUT`], so a client that never finishes it holds up no other.
+//! bounded by [`HANDSHAKE_TIMEOUT`], so a client that never finishes it holds up no other. At
+//! most [`MAX_CONNECTIONS`] are open at once, handshakes included (DA-API-17): past it, a new
+//! connection waits in the socket's backlog until one closes.
 
 use std::io;
 use std::net::SocketAddr;
+use std::pin::Pin;
 use std::sync::Arc;
+use std::task::{Context, Poll};
 use std::time::Duration;
 
 use axum::Router;
@@ -14,14 +18,18 @@ use rustls::ServerConfig;
 use rustls_pki_types::pem::PemObject;
 use rustls_pki_types::{CertificateDer, PrivateKeyDer};
 use thiserror::Error;
+use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
 use tokio::net::{TcpListener, TcpStream};
-use tokio::sync::mpsc;
+use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
 use tokio_rustls::TlsAcceptor;
 use tokio_rustls::server::TlsStream;
 
 /// How long a client has to finish its TLS handshake.
 pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
 
+/// The most connections open at once (DA-API-17).
+pub const MAX_CONNECTIONS: usize = 1024;
+
 #[derive(Debug, Error)]
 pub enum ServeError {
     #[error("globals.api.address {address} can't be bound: {source}")]
@@ -38,6 +46,8 @@ pub enum ServeError {
 pub struct Bound {
     tcp: TcpListener,
     tls: Option<TlsAcceptor>,
+    /// [`MAX_CONNECTIONS`]; a test lowers it.
+    max_connections: usize,
 }
 
 impl std::fmt::Debug for Bound {
@@ -61,7 +71,11 @@ pub async fn bind(address: &HostPort, tls: Option<&TlsIdentity>) -> Result<Bound
             address: format!("{}:{}", address.host, address.port),
             source,
         })?;
-    Ok(Bound { tcp, tls })
+    Ok(Bound {
+        tcp,
+        tls,
+        max_connections: MAX_CONNECTIONS,
+    })
 }
 
 impl Bound {
@@ -80,14 +94,19 @@ impl Bound {
         router: Router,
         stop: impl Future<Output = ()> + Send + 'static,
     ) -> io::Result<()> {
+        let open = Arc::new(Semaphore::new(self.max_connections));
         match self.tls {
             None => {
-                axum::serve(self.tcp, router)
+                let listener = Capped {
+                    tcp: self.tcp,
+                    open,
+                };
+                axum::serve(listener, router)
                     .with_graceful_shutdown(stop)
                     .await
             }
             Some(tls) => {
-                let listener = TlsListener::start(self.tcp, tls)?;
+                let listener = TlsListener::start(self.tcp, tls, open)?;
                 axum::serve(listener, router)
                     .with_graceful_shutdown(stop)
                     .await
@@ -96,6 +115,76 @@ impl Bound {
     }
 }
 
+/// A connection with its place among the open ones, given back when it closes.
+pub(crate) struct Counted<S> {
+    io: S,
+    _open: OwnedSemaphorePermit,
+}
+
+impl<S: AsyncRead + Unpin> AsyncRead for Counted<S> {
+    fn poll_read(
+        mut self: Pin<&mut Self>,
+        cx: &mut Context<'_>,
+        buf: &mut ReadBuf<'_>,
+    ) -> Poll<io::Result<()>> {
+        Pin::new(&mut self.io).poll_read(cx, buf)
+    }
+}
+
+impl<S: AsyncWrite + Unpin> AsyncWrite for Counted<S> {
+    fn poll_write(
+        mut self: Pin<&mut Self>,
+        cx: &mut Context<'_>,
+        buf: &[u8],
+    ) -> Poll<io::Result<usize>> {
+        Pin::new(&mut self.io).poll_write(cx, buf)
+    }
+
+    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
+        Pin::new(&mut self.io).poll_flush(cx)
+    }
+
+    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
+        Pin::new(&mut self.io).poll_shutdown(cx)
+    }
+
+    fn poll_write_vectored(
+        mut self: Pin<&mut Self>,
+        cx: &mut Context<'_>,
+        bufs: &[io::IoSlice<'_>],
+    ) -> Poll<io::Result<usize>> {
+        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
+    }
+
+    fn is_write_vectored(&self) -> bool {
+        self.io.is_write_vectored()
+    }
+}
+
+/// The plain listener, capped: it accepts only while a place is free.
+struct Capped {
+    tcp: TcpListener,
+    open: Arc<Semaphore>,
+}
+
+impl Listener for Capped {
+    type Io = Counted<TcpStream>;
+    type Addr = SocketAddr;
+
+    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
+        let permit = Arc::clone(&self.open)
+            .acquire_owned()
+            .await
+            .expect("the semaphore is never closed");
+        let (io, peer) = Listener::accept(&mut self.tcp).await;
+        (Counted { io, _open: permit }, peer)
+    }
+
+    fn local_addr(&self) -> io::Result<Self::Addr> {
+        self.tcp.local_addr()
+    }
+}
+
 /// The server's TLS setup: HTTP/1.1 only, as the listener serves it.
 fn acceptor(id: &TlsIdentity) -> Result<TlsAcceptor, ServeError> {
     let certs = CertificateDer::pem_slice_iter(id.cert.text.expose().as_bytes())
@@ -122,18 +211,25 @@ fn acceptor(id: &TlsIdentity) -> Result<TlsAcceptor, ServeError> {
 /// Accepted connections, each handed over once its handshake is done.
 struct TlsListener {
     addr: SocketAddr,
-    done: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
+    done: mpsc::Receiver<(Counted<TlsStream<TcpStream>>, SocketAddr)>,
 }
 
 impl TlsListener {
-    fn start(tcp: TcpListener, tls: TlsAcceptor) -> io::Result<Self> {
+    fn start(tcp: TcpListener, tls: TlsAcceptor, open: Arc<Semaphore>) -> io::Result<Self> {
         let addr = tcp.local_addr()?;
         let (tx, done) = mpsc::channel(64);
         tokio::spawn(async move {
             let mut tcp = tcp;
             loop {
-                // Retries a failed accept after a pause, as axum's plain listener does. Ends
-                // once the server is done with the listener, so the port is let go at once.
+                // A place first, handshakes included; then the accept, retried after a pause
+                // as axum's plain listener does. Ends once the server is done with the
+                // listener, so the port is let go at once.
+                let permit = tokio::select! {
+                    permit = Arc::clone(&open).acquire_owned() => {
+                        permit.expect("the semaphore is never closed")
+                    }
+                    () = tx.closed() => return,
+                };
                 let (stream, peer) = tokio::select! {
                     accepted = Listener::accept(&mut tcp) => accepted,
                     () = tx.closed() => return,
@@ -141,8 +237,8 @@ impl TlsListener {
                 let (tls, handed) = (tls.clone(), tx.clone());
                 tokio::spawn(async move {
                     match tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.accept(stream)).await {
-                        Ok(Ok(stream)) => {
-                            let _ = handed.send((stream, peer)).await;
+                        Ok(Ok(io)) => {
+                            let _ = handed.send((Counted { io, _open: permit }, peer)).await;
                         }
                         Ok(Err(e)) => tracing::debug!(%peer, "TLS handshake failed: {e}"),
                         Err(_) => tracing::debug!(%peer, "TLS handshake timed out"),
@@ -155,7 +251,7 @@ impl TlsListener {
 }
 
 impl Listener for TlsListener {
-    type Io = TlsStream<TcpStream>;
+    type Io = Counted<TlsStream<TcpStream>>;
     type Addr = SocketAddr;
 
     async fn accept(&mut self) -> (Self::Io, Self::Addr) {
```

`crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index a2592c9..9cc057c 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1044,6 +1044,14 @@ pub const RULES: &[RuleMeta] = &[
         r#"source.ssl_mode = verify-ca, when source.type = mysql"#,
         r#"The host name is checked too, as with verify-full (DA-SRC-22)."#,
     ),
+    rule(
+        "V-125",
+        Warning,
+        L::Config,
+        Config,
+        r#"globals.api.jwt.jwks_url, globals.api.oauth2.jwks_url and discovery_url"#,
+        r#"Not https: the issuer's keys could be swapped on the way (DA-API-16)."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
```

`crates/data-avalanche-config/src/rules/auth.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/auth.rs b/crates/data-avalanche-config/src/rules/auth.rs
index 0c9e4a1..a18cfcf 100644
--- a/crates/data-avalanche-config/src/rules/auth.rs
+++ b/crates/data-avalanche-config/src/rules/auth.rs
@@ -223,6 +223,29 @@ fn rest(r: &RawRest, at: &At, cloud: Option<Cloud>, ctx: &mut Ctx) {
     }
 }
 
+/// V-125: a key URL that isn't `https` lets anyone on the way swap the issuer's keys. The URL
+/// isn't echoed: it may come from a variable.
+fn plain_key_url(ctx: &mut Ctx, at: &At, leaf: &L) {
+    let Some(l) = leaf else {
+        return;
+    };
+    let Some(text) = ctx.text(l, at) else {
+        return;
+    };
+    let text = text.trim();
+    let https = text
+        .get(..8)
+        .is_some_and(|s| s.eq_ignore_ascii_case("https://"));
+    if !text.is_empty() && !https {
+        ctx.warning(
+            "V-125",
+            at,
+            l.loc(),
+            "the issuer's keys are fetched without TLS: anyone on the way can swap them; use an https URL",
+        );
+    }
+}
+
 fn management_api(a: &RawApi, at: &At, ctx: &mut Ctx) {
     let auth = if a.auth.is_none() {
         Some(ApiAuth::None)
@@ -263,12 +286,15 @@ fn management_api(a: &RawApi, at: &At, ctx: &mut Ctx) {
                 &j.jwks_url,
                 "api auth jwt",
             );
+            plain_key_url(ctx, &at.key("jwt").key("jwks_url"), &j.jwks_url);
         }
         // V-051
         Some(ApiAuth::Oauth2) => {
             let o = a.oauth2.clone().unwrap_or_default();
             let o_at = at.key("oauth2");
             require(ctx, "V-051", &o_at, "issuer", &o.issuer, "api auth oauth2");
+            plain_key_url(ctx, &o_at.key("jwks_url"), &o.jwks_url);
+            plain_key_url(ctx, &o_at.key("discovery_url"), &o.discovery_url);
             if missing(ctx, &o.jwks_url, &o_at.key("jwks_url"))
                 && missing(ctx, &o.discovery_url, &o_at.key("discovery_url"))
             {
```

`docs/src/content/docs/reference/validation-rules.md`:

```diff
diff --git a/docs/src/content/docs/reference/validation-rules.md b/docs/src/content/docs/reference/validation-rules.md
index d4fdafd..cc519e9 100644
--- a/docs/src/content/docs/reference/validation-rules.md
+++ b/docs/src/content/docs/reference/validation-rules.md
@@ -130,3 +130,4 @@ A config with any **error** is rejected whole: at startup the node refuses to st
 | <span id="v-122"></span>V-122 | error | `globals.cluster.memberlist timers` | gossip_interval, probe_interval, probe_timeout and push_pull_interval above zero; gossip_nodes and retransmit_factor at least 1; probe_timeout below probe_interval. |
 | <span id="v-123"></span>V-123 | error | `source.postgres.source_change_action` | ∈ {error, re_snapshot}. |
 | <span id="v-124"></span>V-124 | warning | `source.ssl_mode = verify-ca, when source.type = mysql` | The host name is checked too, as with verify-full (DA-SRC-22). |
+| <span id="v-125"></span>V-125 | warning | `globals.api.jwt.jwks_url, globals.api.oauth2.jwks_url and discovery_url` | Not https: the issuer's keys could be swapped on the way (DA-API-16). |
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-config -p data-avalanche-api -p xtask --no-fail-fast
```

Expected: PASS, 214 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/auth.rs \
  crates/data-avalanche-api/src/server.rs \
  crates/data-avalanche-config/src/registry.rs \
  crates/data-avalanche-config/src/rules/auth.rs \
  crates/data-avalanche-config/tests/rules/auth.rs \
  docs/src/content/docs/reference/validation-rules.md
git commit -m "feat(api): the key fetch follows no redirect and reads at most 1 MiB (V-125); at most 1024 connections (DA-API-16, DA-API-17)"
```

---

### Task 11: The validate rate limit and the OAS 3.1 check (`api`)

**Files:**
- Create: `crates/data-avalanche-api/oas/schema-2022-10-07.json`, `crates/data-avalanche-api/src/limit.rs`
- Modify: `Cargo.lock`, `Cargo.toml`, `crates/data-avalanche-api/Cargo.toml`, `crates/data-avalanche-api/openapi.json`, `crates/data-avalanche-api/src/config.rs`, `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-api/src/test_support.rs`, `crates/data-avalanche/src/run.rs`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`
- Test: `crates/data-avalanche-api/src/config.rs`, `crates/data-avalanche-api/src/openapi.rs`

**Gaps:** DA-API-20, DA-API-18.

**Interfaces:**
- Consumes: the validate route (M9a); `Node` (M9a); the OpenAPI document and its snapshot test (M9a).
- Produces:
  - `limit.rs`: `RateLimit::{new(burst, per_second), validate(), take(now) -> Result<(), Duration>}`, `VALIDATE_BURST = 5`, `VALIDATE_PER_SECOND = 1`; `Node.validates: Arc<RateLimit>`;
  - a posted body takes a token, or answers `429` with `Retry-After` (whole seconds, rounded up); the node's own file (no body) is unlimited; the route table lists `429`;
  - `crates/data-avalanche-api/oas/schema-2022-10-07.json` (the OAS 3.1 schema, vendored) and the `boon =0.6.1` dev-dependency.

Validate of a posted body answers yes or no about the node's `${VAR}`s (V-063, the uniqueness
rules), one request at a time, so its rate bounds an online guess (Oleksii's choice, 2026-10-24).
A node-wide token bucket takes a posted body at 1 a second after a burst of 5. The OpenAPI
document was snapshot-tested and read back by utoipa, never checked against the OAS 3.1 schema:
a test now validates it with `boon` against the vendored schema (the plain one, without the
dialect's deep Schema-object checks).

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/config.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index 52540c8..5870fb2 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -276,6 +276,34 @@ mod tests {
         );
     }
 
+    /// DA-API-20: a node takes posted bodies at a bounded rate, node-wide: past the burst, 429
+    /// with `Retry-After`. The node's own file is checked all the same.
+    #[tokio::test]
+    async fn posted_validates_past_the_burst_wait() {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, CONFIG).unwrap();
+        let lone = lone(file).await;
+        let a = api(open());
+        a.node.set(lone.node.clone()).unwrap();
+        let addr = serve(a).await;
+        for _ in 0..crate::VALIDATE_BURST {
+            let (code, _, body) =
+                request(addr, "POST", "/v1/config/validate", &[], "pipelines: [").await;
+            assert_eq!(code, 422, "{body}");
+        }
+        let (code, head, body) =
+            request(addr, "POST", "/v1/config/validate", &[], "pipelines: [").await;
+        assert_eq!(code, 429, "{body}");
+        assert!(
+            head.to_ascii_lowercase().contains("retry-after: 1\r\n"),
+            "{head}"
+        );
+        assert!(body.contains("try again in 1 s"), "{body}");
+        let (code, _, body) = request(addr, "POST", "/v1/config/validate", &[], "").await;
+        assert_eq!(code, 200, "the node's file: {body}");
+    }
+
     /// A posted body's pipeline id is taken as written (DA-API-19): the answer names a pipeline
     /// by its id, so expanding a `${VAR}` there would read this node's environment. The node's
     /// own file is the operator's, and expands.
```

`crates/data-avalanche-api/src/openapi.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index cdd9952..f3b6621 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -190,6 +190,29 @@ mod tests {
         assert_eq!(operations, routes().len());
     }
 
+    /// DA-API-18: the document is valid against the OAS 3.1 schema, vendored in `oas/` (the
+    /// 2022-10-07 one, `https://spec.openapis.org/oas/3.1/schema/2022-10-07`).
+    #[test]
+    fn the_document_is_valid_oas_3_1() {
+        const ID: &str = "https://spec.openapis.org/oas/3.1/schema/2022-10-07";
+        let schema: Value =
+            serde_json::from_str(include_str!("../oas/schema-2022-10-07.json")).unwrap();
+        let mut schemas = boon::Schemas::new();
+        let mut compiler = boon::Compiler::new();
+        compiler.add_resource(ID, schema).unwrap();
+        let oas = compiler.compile(ID, &mut schemas).unwrap();
+        let doc = serde_json::to_value(openapi()).unwrap();
+        if let Err(e) = schemas.validate(&doc, oas) {
+            panic!("the document isn't valid OpenAPI 3.1: {e:#}");
+        }
+        let mut broken = doc;
+        broken["paths"]["/v1/pipelines"]["get"]["responses"] = serde_json::json!("none");
+        assert!(
+            schemas.validate(&broken, oas).is_err(),
+            "the schema catches a broken document"
+        );
+    }
+
     #[tokio::test]
     async fn the_document_is_served() {
         let addr = listen(api(open())).await;
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-api -p data-avalanche -p xtask --no-fail-fast
```

Expected: FAIL to compile:
  - `error: couldn't read `crates/data-avalanche-api/src/../oas/schema-2022-10-07.json`: No such file or directory (os error 2) (crates/data-avalanche-api/src/openapi.rs)`
  - `error[E0433]: cannot find module or crate `boon` in this scope (crates/data-avalanche-api/src/openapi.rs)`
  - `error[E0425]: cannot find value `VALIDATE_BURST` in the crate root (crates/data-avalanche-api/src/config.rs)`

- [ ] **Step 3: Implement**

Vendor the OAS 3.1 schema, byte for byte as published (Apache-2.0):

```bash
mkdir -p crates/data-avalanche-api/oas
curl -fsSL https://spec.openapis.org/oas/3.1/schema/2022-10-07 -o crates/data-avalanche-api/oas/schema-2022-10-07.json
shasum -a 256 crates/data-avalanche-api/oas/schema-2022-10-07.json
# da01ba28852cac0de53893797cb8d1942bc3b05084f526dcc216717dec314ed0
```

`Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index c84b0b3..15314f0 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -143,6 +143,12 @@ version = "1.0.104"
 source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "330a5ed07fa54e4702c9d6c4174f74427fc0ef6e214bbd677ae50a5099946470"
 
+[[package]]
+name = "appendlist"
+version = "1.4.0"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "e149dc73cd30538307e7ffa2acd3d2221148eaeed4871f246657b1c3eaa1cbd2"
+
 [[package]]
 name = "ar_archive_writer"
 version = "0.5.3"
@@ -1068,6 +1074,32 @@ dependencies = [
  "hybrid-array",
 ]
 
+[[package]]
+name = "boon"
+version = "0.6.1"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "baa187da765010b70370368c49f08244b1ae5cae1d5d33072f76c8cb7112fe3e"
+dependencies = [
+ "ahash",
+ "appendlist",
+ "base64 0.22.1",
+ "fluent-uri",
+ "idna",
+ "once_cell",
+ "percent-encoding",
+ "regex",
+ "regex-syntax",
+ "serde",
+ "serde_json",
+ "url",
+]
+
+[[package]]
+name = "borrow-or-share"
+version = "0.2.4"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "dc0b364ead1874514c8c2855ab558056ebfeb775653e7ae45ff72f28f8f3166c"
+
 [[package]]
 name = "brotli"
 version = "8.0.4"
@@ -1730,6 +1762,7 @@ dependencies = [
  "axum",
  "base64 0.22.1",
  "blake3",
+ "boon",
  "chrono",
  "data-avalanche-cluster",
  "data-avalanche-config",
@@ -3084,6 +3117,16 @@ dependencies = [
  "zlib-rs",
 ]
 
+[[package]]
+name = "fluent-uri"
+version = "0.3.2"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "1918b65d96df47d3591bed19c5cca17e3fa5d0707318e4b5ef2eae01764df7e5"
+dependencies = [
+ "borrow-or-share",
+ "ref-cast",
+]
+
 [[package]]
 name = "fnv"
 version = "1.0.7"
@@ -5092,6 +5135,26 @@ dependencies = [
  "thiserror 2.0.21",
 ]
 
+[[package]]
+name = "ref-cast"
+version = "1.0.27"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "7e440fb4e4b4147295338efb76001ab9e4efc0e5839df2c47fc5ac2381d365c3"
+dependencies = [
+ "ref-cast-impl",
+]
+
+[[package]]
+name = "ref-cast-impl"
+version = "1.0.27"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "92ecd8964f8453721699a1ed72037b0db49ce2f5a5138486ee89bed6f67cdf3a"
+dependencies = [
+ "proc-macro2",
+ "quote",
+ "syn 3.0.6",
+]
+
 [[package]]
 name = "regex"
 version = "1.13.1"
```

`Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 2b68606..e5b75e2 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -58,6 +58,8 @@ async-trait = "0.1"
 bytes = "1"
 futures-util = { version = "0.3", default-features = false, features = ["std"] }
 rcgen = { version = "0.14", default-features = false, features = ["aws_lc_rs", "pem"] }
+# The OpenAPI document checked against the vendored OAS 3.1 schema (DA-API-18), a dev-dependency.
+boon = "=0.6.1"
 # One arrow major workspace-wide (roadmap §2, D-MS.14): 59, as deltalake 1.0 resolves it.
 arrow-array = "59"
 arrow-buffer = "59"
```

`crates/data-avalanche-api/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-api/Cargo.toml b/crates/data-avalanche-api/Cargo.toml
index ce9e581..72e50c7 100644
--- a/crates/data-avalanche-api/Cargo.toml
+++ b/crates/data-avalanche-api/Cargo.toml
@@ -35,6 +35,7 @@ uuid.workspace = true
 
 [dev-dependencies]
 axum = { workspace = true, features = ["json"] }
+boon.workspace = true
 data-avalanche-pipeline.workspace = true
 data-avalanche-storage.workspace = true
 object_store.workspace = true
```

`crates/data-avalanche-api/openapi.json`:

```diff
diff --git a/crates/data-avalanche-api/openapi.json b/crates/data-avalanche-api/openapi.json
index dc52eb1..b1b1d92 100644
--- a/crates/data-avalanche-api/openapi.json
+++ b/crates/data-avalanche-api/openapi.json
@@ -294,6 +294,12 @@
               "application/json": {}
             }
           },
+          "429": {
+            "description": "posted bodies past the node's rate: `Retry-After` says when",
+            "content": {
+              "application/json": {}
+            }
+          },
           "503": {
             "description": "the node is starting or stopping, or its state can't be read",
             "content": {
```

`crates/data-avalanche-api/src/config.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/config.rs b/crates/data-avalanche-api/src/config.rs
index 5870fb2..ceca7eb 100644
--- a/crates/data-avalanche-api/src/config.rs
+++ b/crates/data-avalanche-api/src/config.rs
@@ -1,9 +1,10 @@
 //! `/v1/config`: the check, the reload and its status (Clustering §9.2).
 
 use std::collections::BTreeMap;
+use std::time::Instant;
 
 use axum::extract::{Extension, RawQuery, State};
-use axum::http::StatusCode;
+use axum::http::{HeaderValue, StatusCode, header};
 use axum::response::Response;
 use data_avalanche_cluster::{Checked, ReloadError, Reloaded, RestartFields};
 use data_avalanche_config::Fingerprints;
@@ -43,7 +44,8 @@ async fn node_file(node: &Node) -> Result<String, Failure> {
 /// and all, is not echoed. A body's secrets aren't compared with the running ones
 /// (`secrets_compared: false`): whether a guess is the running secret isn't told. Every validate
 /// is logged at info with its caller, its source, the body's size and the answer's status
-/// (DA-API-20).
+/// (DA-API-20). A node takes posted bodies at [`crate::VALIDATE_PER_SECOND`] after a burst of
+/// [`crate::VALIDATE_BURST`]: past it, 429 with `Retry-After`.
 pub(crate) async fn validate(
     State(api): State<Api>,
     Extension(by): Extension<Principal>,
@@ -55,6 +57,16 @@ pub(crate) async fn validate(
     let posted = !text.trim().is_empty();
     let bytes = text.len();
     let response: Response = 'checked: {
+        if posted && let Err(wait) = node.validates.take(Instant::now()) {
+            let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
+            let mut r = body(
+                StatusCode::TOO_MANY_REQUESTS,
+                &json!({ "error": format!("too many posted configs validated: try again in {secs} s") }),
+            );
+            r.headers_mut()
+                .insert(header::RETRY_AFTER, HeaderValue::from(secs));
+            break 'checked r;
+        }
         let (checked, fingerprints) = if posted {
             (
                 node.coordinator.check_posted(&text).await,
```

`crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 4605ac5..a5b1ac7 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -11,6 +11,7 @@
 mod auth;
 mod cluster;
 mod config;
+mod limit;
 mod openapi;
 mod pipelines;
 mod server;
@@ -35,10 +36,11 @@ use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
 use serde_json::{Value, json};
 use tokio::sync::watch;
 
-pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, Principal, REFETCH_AFTER};
+pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER};
 pub use cluster::{Control, Stop};
+pub use limit::{RateLimit, VALIDATE_BURST, VALIDATE_PER_SECOND};
 pub use openapi::openapi;
-pub use server::{Bound, HANDSHAKE_TIMEOUT, ServeError, bind};
+pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
 
 use crate::auth::Denied;
 
@@ -66,6 +68,8 @@ pub struct Node {
     pub leases: Leases,
     /// The node's leave and shutdown.
     pub control: Control,
+    /// The posted-body validates' limit ([`RateLimit::validate`], DA-API-20).
+    pub validates: Arc<RateLimit>,
 }
 
 impl std::fmt::Debug for Node {
@@ -122,6 +126,10 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
                             422,
                             "invalid, or it changes a restart-only field: the report",
                         ),
+                        (
+                            429,
+                            "posted bodies past the node's rate: `Retry-After` says when",
+                        ),
                     ],
                 )
             },
```

`crates/data-avalanche-api/src/limit.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/limit.rs b/crates/data-avalanche-api/src/limit.rs
new file mode 100644
index 0000000..16595b9
--- /dev/null
+++ b/crates/data-avalanche-api/src/limit.rs
@@ -0,0 +1,82 @@
+//! A token bucket on the posted-body validates (DA-API-20): each one's verdicts tell a little
+//! about the node's variables, so they are spent at a bounded rate, node-wide.
+
+use std::sync::{Mutex, PoisonError};
+use std::time::{Duration, Instant};
+
+/// Posted-body validates taken at once before the rate applies.
+pub const VALIDATE_BURST: u32 = 5;
+/// Posted-body validates a node takes per second, once the burst is spent.
+pub const VALIDATE_PER_SECOND: u32 = 1;
+
+/// A token bucket: `burst` tokens, refilled at `per_second`.
+#[derive(Debug)]
+pub struct RateLimit {
+    burst: f64,
+    per_second: f64,
+    /// The tokens left, as of the instant.
+    state: Mutex<(f64, Instant)>,
+}
+
+impl RateLimit {
+    /// Full: `burst` takes succeed at once.
+    pub fn new(burst: u32, per_second: u32) -> Self {
+        Self {
+            burst: f64::from(burst),
+            per_second: f64::from(per_second),
+            state: Mutex::new((f64::from(burst), Instant::now())),
+        }
+    }
+
+    /// The posted-body validates' limit: [`VALIDATE_BURST`] at [`VALIDATE_PER_SECOND`].
+    pub fn validate() -> Self {
+        Self::new(VALIDATE_BURST, VALIDATE_PER_SECOND)
+    }
+
+    /// Takes a token at `now`; `Err` with the wait until one is there.
+    ///
+    /// # Errors
+    /// The bucket is empty: the wait until the next token.
+    pub fn take(&self, now: Instant) -> Result<(), Duration> {
+        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
+        let (tokens, at) = *state;
+        let refilled = now.saturating_duration_since(at).as_secs_f64() * self.per_second;
+        let tokens = (tokens + refilled).min(self.burst);
+        if tokens >= 1.0 {
+            *state = (tokens - 1.0, now);
+            Ok(())
+        } else {
+            *state = (tokens, now);
+            Err(Duration::from_secs_f64((1.0 - tokens) / self.per_second))
+        }
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+
+    #[test]
+    fn the_burst_is_taken_at_once_then_the_rate_applies() {
+        let limit = RateLimit::new(5, 1);
+        let t0 = Instant::now();
+        for _ in 0..5 {
+            limit.take(t0).unwrap();
+        }
+        let wait = limit.take(t0).unwrap_err();
+        assert_eq!(wait, Duration::from_secs(1));
+        let wait = limit.take(t0 + Duration::from_millis(400)).unwrap_err();
+        assert!(
+            wait > Duration::from_millis(590) && wait <= Duration::from_millis(600),
+            "{wait:?}"
+        );
+        limit.take(t0 + Duration::from_secs(1)).unwrap();
+        limit.take(t0 + Duration::from_secs(1)).unwrap_err();
+        for _ in 0..5 {
+            limit.take(t0 + Duration::from_secs(60)).unwrap();
+        }
+        limit
+            .take(t0 + Duration::from_secs(60))
+            .expect_err("refilled to the burst, no more");
+    }
+}
```

`crates/data-avalanche-api/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 627a35e..b338c71 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -268,6 +268,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             }),
             leases: leases.clone(),
             control,
+            validates: Arc::new(crate::RateLimit::validate()),
         },
         leases,
         stops,
```

`crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index e1f460a..b5f9d52 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -7,7 +7,7 @@ use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::{Arc, OnceLock};
 use std::time::{Duration, SystemTime};
 
-use data_avalanche_api::{Api, Auth, Control, Node, ServeError, Stop, bind, router};
+use data_avalanche_api::{Api, Auth, Control, Node, RateLimit, ServeError, Stop, bind, router};
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
@@ -249,6 +249,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         docs,
         leases: deps.leases.clone(),
         control,
+        validates: Arc::new(RateLimit::validate()),
     });
     ready.store(true, Ordering::SeqCst);
     let mut cluster = Some((gossip, reconciler, discovery));
```

`docs/public/openapi.json`:

```diff
diff --git a/docs/public/openapi.json b/docs/public/openapi.json
index dc52eb1..b1b1d92 100644
--- a/docs/public/openapi.json
+++ b/docs/public/openapi.json
@@ -294,6 +294,12 @@
               "application/json": {}
             }
           },
+          "429": {
+            "description": "posted bodies past the node's rate: `Retry-After` says when",
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
index ff937e9..2f20ad8 100644
--- a/docs/src/content/docs/reference/api.md
+++ b/docs/src/content/docs/reference/api.md
@@ -204,6 +204,7 @@ Least role: `operator`.
 |---|---|
 | `200` | it loads: its warnings and the changes |
 | `422` | invalid, or it changes a restart-only field: the report |
+| `429` | posted bodies past the node's rate: `Retry-After` says when |
 
 ## cluster
 
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-api -p data-avalanche -p xtask --no-fail-fast
```

Expected: PASS, 97 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-api/Cargo.toml \
  crates/data-avalanche-api/oas/schema-2022-10-07.json \
  crates/data-avalanche-api/openapi.json \
  crates/data-avalanche-api/src/config.rs \
  crates/data-avalanche-api/src/lib.rs \
  crates/data-avalanche-api/src/limit.rs \
  crates/data-avalanche-api/src/openapi.rs \
  crates/data-avalanche-api/src/test_support.rs \
  crates/data-avalanche/src/run.rs \
  docs/public/openapi.json \
  docs/src/content/docs/reference/api.md
git commit -m "feat(api): posted validates rate-limited (429, Retry-After); the OpenAPI document checked against the OAS 3.1 schema (DA-API-20, DA-API-18)"
```

---

### Task 12: The trace export's headers (`config`, `telemetry`, the binary, `testkit`)

**Files:**
- Modify: `Cargo.lock`, `crates/data-avalanche-config/src/build.rs`, `crates/data-avalanche-config/src/field_docs.rs`, `crates/data-avalanche-config/src/raw.rs`, `crates/data-avalanche-config/src/registry.rs`, `crates/data-avalanche-config/src/resolved.rs`, `crates/data-avalanche-config/src/rules/domain.rs`, `crates/data-avalanche-config/src/walk.rs`, `crates/data-avalanche-telemetry/Cargo.toml`, `crates/data-avalanche-telemetry/src/traces.rs`, `crates/data-avalanche-testkit/src/lib.rs`, `crates/data-avalanche-testkit/src/otlp.rs`, `crates/data-avalanche/src/logging.rs`, `docs/src/content/docs/reference/configuration.md`, `docs/src/content/docs/reference/validation-rules.md`
- Test: `crates/data-avalanche-config/src/registry.rs`, `crates/data-avalanche-config/tests/load.rs`, `crates/data-avalanche-config/tests/rules/domain.rs`, `crates/data-avalanche-telemetry/src/traces.rs`, `crates/data-avalanche-telemetry/tests/traces.rs`, `crates/data-avalanche/src/logging.rs`

**Gaps:** DA-OBS-7.

**Interfaces:**
- Consumes: `globals.tracing` (M10); `Traces::set` and its exporter swap; testkit's `otlp_collector` (M10).
- Produces:
  - config: `globals.tracing.headers: map<string, string>` (`RawTracing.headers`, `Tracing.headers: BTreeMap<String, Secret>`), R, V-126 (error: each name an HTTP token, each value visible ASCII, spaces and tabs; the value never echoed);
  - `Traces::set(endpoint, headers: &BTreeMap<String, String>, sample_ratio)`: the exporter in force is keyed by the endpoint and the headers, so a rotated token builds a new one;
  - testkit: `OtlpRequest.headers`, `OtlpRequest::header(name)`, `otlp_collector_tls(cert_pem, key_pem)`.

The export sent no headers, so a hosted collector that wants a token couldn't take the
traces, and an `https` endpoint was untested (Q-M12b-1). `globals.tracing.headers` carries them,
each value a secret resolved as the others are (`${VAR}`), applied on reload. A test serves OTLP
over TLS with a throwaway certificate: an exporter that doesn't trust it fails, one that does
delivers the headers, and a reload with a new token reaches the next export.
`OTEL_EXPORTER_OTLP_HEADERS` is still read by the exporter, and wins for a name it sets too
(measured in opentelemetry-otlp 0.33.1: it extends the configured map).

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index 9cc057c..ee33a9e 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1065,7 +1065,7 @@ mod tests {
 
     #[test]
     fn has_every_spec_rule_once_in_order() {
-        assert_eq!(RULES.len(), 119);
+        assert_eq!(RULES.len(), 120);
         assert!(
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
```

`crates/data-avalanche-config/tests/load.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/load.rs b/crates/data-avalanche-config/tests/load.rs
index 947a55b..0ebcfe5 100644
--- a/crates/data-avalanche-config/tests/load.rs
+++ b/crates/data-avalanche-config/tests/load.rs
@@ -122,7 +122,8 @@ fn applies_spec_defaults() {
         g.tracing,
         Tracing {
             otlp_endpoint: None,
-            sample_ratio: 0.0
+            sample_ratio: 0.0,
+            headers: Default::default(),
         }
     );
     let p = &cfg.pipelines["orders"];
@@ -791,6 +792,30 @@ fn unreadable_config_file_is_a_parse_error() {
     );
 }
 
+/// DA-OBS-7: `globals.tracing.headers` resolve as secrets: the value is the variable's, and the
+/// resolved config shows it only as a fingerprint.
+#[test]
+fn tracing_headers_resolve_as_secrets() {
+    let yaml = MINIMAL.replace(
+        "globals:\n  state: { bucket: app-state }",
+        "globals:\n  state: { bucket: app-state }\n  tracing:\n    otlp_endpoint: https://otel:4318\n    headers: { authorization: \"Bearer ${OTLP_TOKEN}\" }",
+    );
+    let cfg = load_str(
+        &yaml,
+        &env(&[("PW", "pw"), ("OTLP_TOKEN", "t0k")]),
+        &fp_key(),
+    )
+    .unwrap_or_else(|r| panic!("{r:#?}"))
+    .config;
+    let headers = &cfg.globals.tracing.headers;
+    assert_eq!(headers["authorization"].expose(), "Bearer t0k");
+    let shown = serde_json::to_string(&cfg.globals.tracing).unwrap();
+    assert!(
+        !shown.contains("t0k") && shown.contains("blake3:"),
+        "{shown}"
+    );
+}
+
 // FU-C (config §11 Phase 2): an optional value that resolves to empty is absent, so its default
 // applies — for a tri-state field, the default, not `null`.
 
@@ -808,6 +833,7 @@ fn empty_optional_otlp_endpoint_is_absent() {
         Tracing {
             otlp_endpoint: None,
             sample_ratio: 0.0,
+            headers: Default::default(),
         },
         "an otlp_endpoint that resolves empty is absent, exactly as if it were never written"
     );
```

`crates/data-avalanche-config/tests/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/domain.rs b/crates/data-avalanche-config/tests/rules/domain.rs
index 1dbdf20..724b144 100644
--- a/crates/data-avalanche-config/tests/rules/domain.rs
+++ b/crates/data-avalanche-config/tests/rules/domain.rs
@@ -269,6 +269,24 @@ pub fn cases() -> Vec<Case> {
                 json!({"otlp_endpoint": "http://otel:4317", "sample_ratio": 1}),
             )
         }),
+        case("V-126", Error, "header name with a space", || {
+            pg_delta().set(
+                "/globals/tracing",
+                json!({"otlp_endpoint": "https://otel:4318", "headers": {"x tenant": "a"}}),
+            )
+        }),
+        case("V-126", Error, "header value with a newline", || {
+            pg_delta().set(
+                "/globals/tracing",
+                json!({"otlp_endpoint": "https://otel:4318", "headers": {"authorization": "Bearer a\nx-evil: 1"}}),
+            )
+        }),
+        case("V-126", Silent, "a bearer header", || {
+            pg_delta().set(
+                "/globals/tracing",
+                json!({"otlp_endpoint": "https://otel:4318", "headers": {"authorization": "Bearer t0k", "x-scope-orgid": "shop"}}),
+            )
+        }),
         case("V-101", Error, "account with a dash", || {
             pg_delta().set("/globals/azure/account_name", json!("my-account"))
         }),
```

`crates/data-avalanche-telemetry/src/traces.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/src/traces.rs b/crates/data-avalanche-telemetry/src/traces.rs
index 8b7aa42..b710a2d 100644
--- a/crates/data-avalanche-telemetry/src/traces.rs
+++ b/crates/data-avalanche-telemetry/src/traces.rs
@@ -449,7 +449,7 @@ mod tests {
         let (url, requests) = otlp_collector_answering(status.clone());
         let endpoint = url.replace("http://", "http://u:secret@");
         let traces = Traces::new("node-1");
-        traces.set(Some(&endpoint), 1.0).unwrap();
+        traces.set(Some(&endpoint), &BTreeMap::new(), 1.0).unwrap();
         let lines = Lines::default();
         let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(lines.clone()));
         let export = || {
@@ -485,8 +485,8 @@ mod tests {
         assert!(export().is_err());
         assert_eq!(lines.at(tracing::Level::WARN).len(), 2);
 
-        traces.set(None, 1.0).unwrap();
-        traces.set(Some(&endpoint), 1.0).unwrap();
+        traces.set(None, &BTreeMap::new(), 1.0).unwrap();
+        traces.set(Some(&endpoint), &BTreeMap::new(), 1.0).unwrap();
         assert!(export().is_err());
         assert_eq!(lines.at(tracing::Level::WARN).len(), 3, "a new exporter");
         assert!(
@@ -513,12 +513,49 @@ mod tests {
         assert!(health.failed(at(10)), "the first failure after a success");
     }
 
+    /// DA-OBS-7: an `https` collector takes the export over TLS, with the configured headers; a
+    /// client that doesn't trust the collector's certificate exports nothing. A change of the
+    /// headers alone makes a new exporter.
+    #[test]
+    fn an_https_collector_takes_the_export_with_its_headers() {
+        let c = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
+        let (url, requests) = data_avalanche_testkit::otlp_collector_tls(
+            &c.cert.pem(),
+            &c.signing_key.serialize_pem(),
+        );
+        let headers = BTreeMap::from([
+            ("authorization".to_owned(), "Bearer t0k".to_owned()),
+            ("x-tenant".to_owned(), "shop".to_owned()),
+        ]);
+
+        let untrusting = Traces::new("node-1");
+        untrusting.set(Some(&url), &headers, 1.0).unwrap();
+        assert!(block_on(untrusting.switch.export(Vec::new())).is_err());
+
+        let mut traces = Traces::new("node-1");
+        traces.roots = vec![c.cert.der().to_vec()];
+        traces.set(Some(&url), &headers, 1.0).unwrap();
+        block_on(traces.switch.export(Vec::new())).unwrap();
+        let r = requests.recv_timeout(Duration::from_secs(10)).unwrap();
+        assert_eq!(r.path, "/v1/traces");
+        assert_eq!(r.header("authorization"), Some("Bearer t0k"));
+        assert_eq!(r.header("x-tenant"), Some("shop"));
+        assert!(!format!("{traces:?}").contains("t0k"), "{traces:?}");
+
+        let rotated = BTreeMap::from([("authorization".to_owned(), "Bearer n3w".to_owned())]);
+        traces.set(Some(&url), &rotated, 1.0).unwrap();
+        block_on(traces.switch.export(Vec::new())).unwrap();
+        let r = requests.recv_timeout(Duration::from_secs(10)).unwrap();
+        assert_eq!(r.header("authorization"), Some("Bearer n3w"));
+        assert_eq!(r.header("x-tenant"), None);
+    }
+
     #[test]
     fn the_debug_of_traces_leaves_out_the_endpoint_s_credentials() {
         let (url, _requests) = data_avalanche_testkit::otlp_collector();
         let traces = Traces::new("node-1");
         let endpoint = format!("{}/?token=t0k", url.replace("http://", "http://u:secret@"));
-        traces.set(Some(&endpoint), 1.0).unwrap();
+        traces.set(Some(&endpoint), &BTreeMap::new(), 1.0).unwrap();
         let text = format!("{traces:?}");
         assert!(text.contains(&url.replace("http://", "")), "{text}");
         assert!(!text.contains("secret"), "{text}");
```

`crates/data-avalanche-telemetry/tests/traces.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/traces.rs b/crates/data-avalanche-telemetry/tests/traces.rs
index df8d992..dbf79c0 100644
--- a/crates/data-avalanche-telemetry/tests/traces.rs
+++ b/crates/data-avalanche-telemetry/tests/traces.rs
@@ -1,6 +1,7 @@
 //! The OTLP/HTTP export (config §6.6): a sampled span reaches the collector's `/v1/traces`; one
 //! not sampled doesn't; a span's ids are known only when it is sampled.
 
+use std::collections::BTreeMap;
 use std::time::Duration;
 
 use data_avalanche_telemetry::{Traces, trace_ids};
@@ -26,7 +27,11 @@ fn flush(traces: &Traces) -> Option<(String, String)> {
 fn a_sampled_flush_is_exported_to_the_collector() {
     let (url, requests) = collector();
     let traces = Traces::new("node-1");
-    assert!(traces.set(Some(&format!("{url}/")), 1.0).unwrap());
+    assert!(
+        traces
+            .set(Some(&format!("{url}/")), &BTreeMap::new(), 1.0)
+            .unwrap()
+    );
     let ids = flush(&traces);
     traces.shutdown();
     let request = requests.recv_timeout(Duration::from_secs(10)).unwrap();
@@ -48,7 +53,7 @@ fn a_sampled_flush_is_exported_to_the_collector() {
 fn a_flush_not_sampled_is_neither_exported_nor_named() {
     let (url, requests) = collector();
     let traces = Traces::new("node-1");
-    traces.set(Some(&url), 0.0).unwrap();
+    traces.set(Some(&url), &BTreeMap::new(), 0.0).unwrap();
     assert_eq!(flush(&traces), None);
     traces.shutdown();
     assert!(requests.recv_timeout(Duration::from_millis(500)).is_err());
@@ -60,16 +65,16 @@ fn a_new_endpoint_takes_the_next_spans_and_none_takes_none() {
     let (first, to_first) = collector();
     let (second, to_second) = collector();
     let traces = Traces::new("node-1");
-    traces.set(Some(&first), 1.0).unwrap();
+    traces.set(Some(&first), &BTreeMap::new(), 1.0).unwrap();
     flush(&traces);
     traces.flush();
     assert!(to_first.recv_timeout(Duration::from_secs(10)).is_ok());
-    traces.set(Some(&second), 1.0).unwrap();
+    traces.set(Some(&second), &BTreeMap::new(), 1.0).unwrap();
     flush(&traces);
     traces.flush();
     assert!(to_second.recv_timeout(Duration::from_secs(10)).is_ok());
     assert!(to_first.try_recv().is_err());
-    assert!(!traces.set(None, 1.0).unwrap());
+    assert!(!traces.set(None, &BTreeMap::new(), 1.0).unwrap());
     assert_eq!(flush(&traces), None, "off: nothing is sampled");
     traces.shutdown();
     assert!(to_second.recv_timeout(Duration::from_millis(500)).is_err());
@@ -79,8 +84,10 @@ fn a_new_endpoint_takes_the_next_spans_and_none_takes_none() {
 fn an_endpoint_that_isn_t_a_url_is_refused_and_the_one_in_force_stays() {
     let (url, requests) = collector();
     let traces = Traces::new("node-1");
-    traces.set(Some(&url), 1.0).unwrap();
-    let e = traces.set(Some("not a url"), 1.0).unwrap_err();
+    traces.set(Some(&url), &BTreeMap::new(), 1.0).unwrap();
+    let e = traces
+        .set(Some("not a url"), &BTreeMap::new(), 1.0)
+        .unwrap_err();
     assert!(e.to_string().contains("not a url"), "{e}");
     flush(&traces);
     traces.shutdown();
@@ -95,7 +102,7 @@ fn a_collector_that_never_answers_doesn_t_hold_the_spans_up() {
     let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
     let url = format!("http://{}", silent.local_addr().unwrap());
     let traces = Traces::new("node-1");
-    traces.set(Some(&url), 1.0).unwrap();
+    traces.set(Some(&url), &BTreeMap::new(), 1.0).unwrap();
     let started = std::time::Instant::now();
     for _ in 0..5_000 {
         flush(&traces);
```

`crates/data-avalanche/src/logging.rs`:

```diff
diff --git a/crates/data-avalanche/src/logging.rs b/crates/data-avalanche/src/logging.rs
index efc0e48..d719a72 100644
--- a/crates/data-avalanche/src/logging.rs
+++ b/crates/data-avalanche/src/logging.rs
@@ -502,6 +502,7 @@ mod tests {
             logger.trace(&Tracing {
                 otlp_endpoint: Some(url.clone()),
                 sample_ratio: 1.0,
+                headers: Default::default(),
             });
             tracing::info_span!(target: "deltalake_core", "a.library.span").in_scope(|| {});
             tracing::info_span!("after.endpoint").in_scope(|| {});
@@ -530,6 +531,7 @@ mod tests {
             logger.trace(&Tracing {
                 otlp_endpoint: endpoint,
                 sample_ratio: 1.0,
+                headers: Default::default(),
             });
             tracing::info_span!("pipeline.flush", pipeline = "orders").in_scope(|| {
                 tracing::info_span!("sink.commit", table = "public.a").in_scope(|| {
@@ -560,6 +562,7 @@ mod tests {
             logger.trace(&Tracing {
                 otlp_endpoint: Some(url),
                 sample_ratio: 1.0,
+                headers: Default::default(),
             });
             tracing::info_span!("pipeline.flush", pipeline = "orders").in_scope(|| {
                 tracing::info_span!("sink.commit", table = "public.a").in_scope(|| {
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-config -p data-avalanche-telemetry -p data-avalanche -p xtask --no-fail-fast
```

Expected: FAIL to compile:
  - `error[E0433]: cannot find type `BTreeMap` in this scope (crates/data-avalanche-telemetry/src/traces.rs)`
  - `error[E0425]: cannot find function `otlp_collector_tls` in crate `data_avalanche_testkit` (crates/data-avalanche-telemetry/src/traces.rs)`
  - `error[E0061]: this method takes 2 arguments but 3 arguments were supplied (crates/data-avalanche-telemetry/tests/traces.rs)`
  - `error[E0061]: this method takes 2 arguments but 3 arguments were supplied (crates/data-avalanche-telemetry/src/traces.rs)`
  - … (4 more)

- [ ] **Step 3: Implement**

`Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 15314f0..6aa60dc 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1993,6 +1993,8 @@ dependencies = [
  "opentelemetry-otlp",
  "opentelemetry_sdk",
  "prometheus-client",
+ "rcgen",
+ "reqwest 0.13.5",
  "thiserror 2.0.21",
  "tokio",
  "tracing",
```

`crates/data-avalanche-config/src/build.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/build.rs b/crates/data-avalanche-config/src/build.rs
index aa79e04..48c3965 100644
--- a/crates/data-avalanche-config/src/build.rs
+++ b/crates/data-avalanche-config/src/build.rs
@@ -266,6 +266,27 @@ fn read_or_report<T>(
 
 // ---- globals ----
 
+/// `globals.tracing.headers`, each value a secret. The structural pass builds none (no
+/// fingerprint key), though each value's template is still checked.
+fn tracing_headers(
+    ctx: &mut Ctx,
+    headers: &Option<BTreeMap<String, Leaf<String>>>,
+    at: &At,
+) -> Option<BTreeMap<String, Secret>> {
+    let mut out = Some(BTreeMap::new());
+    for (name, leaf) in headers.iter().flatten() {
+        let value = ctx.text(leaf, &at.key(name));
+        out = match (out, value, ctx.fingerprint_key()) {
+            (Some(mut m), Some(v), Some(key)) => {
+                m.insert(name.clone(), Secret::new(v, key));
+                Some(m)
+            }
+            _ => None,
+        };
+    }
+    out
+}
+
 fn globals(g: &RawGlobals, ctx: &mut Ctx) -> Option<Globals> {
     let at = At::config(YamlPath::root().key("globals"));
     let cloud = or(ctx, &g.cloud, &at, "cloud", Cloud::Aws);
@@ -298,6 +319,7 @@ fn globals(g: &RawGlobals, ctx: &mut Ctx) -> Option<Globals> {
         "sample_ratio",
         ratio_default,
     );
+    let headers = tracing_headers(ctx, &tracing.headers, &tracing_at.key("headers"));
     let azure = g.azure.clone().unwrap_or_default();
     let account_name = opt(ctx, &azure.account_name, &at.key("azure"), "account_name");
     Some(Globals {
@@ -315,6 +337,7 @@ fn globals(g: &RawGlobals, ctx: &mut Ctx) -> Option<Globals> {
         tracing: Tracing {
             otlp_endpoint: otlp_endpoint?,
             sample_ratio: sample_ratio?,
+            headers: headers?,
         },
         azure: Azure {
             account_name: account_name?,
```

`crates/data-avalanche-config/src/field_docs.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
index 153ec52..8139831 100644
--- a/crates/data-avalanche-config/src/field_docs.rs
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -283,6 +283,11 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
         Note("0.05 with an endpoint, else 0"),
         "The share of flushes traced, from 0 to 1.",
     ),
+    f(
+        "globals.tracing.headers.*",
+        Note("none"),
+        "Headers sent with each export, a map of name to value: a hosted collector's token or API key (`authorization: Bearer ${OTLP_TOKEN}`). The values are secrets (V-126).",
+    ),
     f(
         "globals.azure.account_name",
         Note("from the credential chain"),
```

`crates/data-avalanche-config/src/raw.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/raw.rs b/crates/data-avalanche-config/src/raw.rs
index b171528..e13da19 100644
--- a/crates/data-avalanche-config/src/raw.rs
+++ b/crates/data-avalanche-config/src/raw.rs
@@ -134,6 +134,7 @@ pub(crate) struct RawMemberlist {
 pub(crate) struct RawTracing {
     pub otlp_endpoint: L<String>,
     pub sample_ratio: L<f64>,
+    pub headers: Option<BTreeMap<String, Leaf<String>>>,
 }
 
 #[derive(Debug, Clone, Default, Deserialize)]
```

`crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index ee33a9e..480fc46 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1052,6 +1052,14 @@ pub const RULES: &[RuleMeta] = &[
         r#"globals.api.jwt.jwks_url, globals.api.oauth2.jwks_url and discovery_url"#,
         r#"Not https: the issuer's keys could be swapped on the way (DA-API-16)."#,
     ),
+    rule(
+        "V-126",
+        Error,
+        L::Config,
+        Config,
+        r#"globals.tracing.headers"#,
+        r#"Each name an HTTP token, each value visible ASCII, spaces and tabs (DA-OBS-7)."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
```

`crates/data-avalanche-config/src/resolved.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/resolved.rs b/crates/data-avalanche-config/src/resolved.rs
index c43bf8b..1428343 100644
--- a/crates/data-avalanche-config/src/resolved.rs
+++ b/crates/data-avalanche-config/src/resolved.rs
@@ -172,6 +172,9 @@ pub struct Tracing {
     /// `None` = tracing disabled.
     pub otlp_endpoint: Option<String>,
     pub sample_ratio: f64,
+    /// Sent with each export (DA-OBS-7): a hosted collector's token or API key. Secrets: shown
+    /// and diffed by fingerprint.
+    pub headers: BTreeMap<String, Secret>,
 }
 
 #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
```

`crates/data-avalanche-config/src/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/domain.rs b/crates/data-avalanche-config/src/rules/domain.rs
index da5343c..1fc778c 100644
--- a/crates/data-avalanche-config/src/rules/domain.rs
+++ b/crates/data-avalanche-config/src/rules/domain.rs
@@ -227,6 +227,37 @@ fn globals(g: &RawGlobals, ctx: &mut Ctx) {
             format!("sample_ratio must be 0..=1, got {}", shown_value(l, &r)),
         );
     }
+    // V-126: a header HTTP takes. The value isn't echoed: it is a secret.
+    if let Some(headers) = g.tracing.as_ref().and_then(|t| t.headers.as_ref()) {
+        let at = root.key("tracing").key("headers");
+        for (name, leaf) in headers {
+            let token = !name.is_empty()
+                && name
+                    .bytes()
+                    .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b));
+            if !token {
+                ctx.error(
+                    "V-126",
+                    &at.key(name),
+                    leaf.loc(),
+                    format!("header name {name:?} isn't an HTTP token"),
+                );
+                continue;
+            }
+            if let Some(value) = ctx.text(leaf, &at.key(name))
+                && !value
+                    .bytes()
+                    .all(|b| b == b'\t' || (b' '..=b'~').contains(&b))
+            {
+                ctx.error(
+                    "V-126",
+                    &at.key(name),
+                    leaf.loc(),
+                    format!("header {name}'s value holds a control or non-ASCII character"),
+                );
+            }
+        }
+    }
     // V-101
     if let Some(a) = &g.azure
         && let Some((name, loc)) = val(ctx, &a.account_name, &root.key("azure").key("account_name"))
```

`crates/data-avalanche-config/src/walk.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/walk.rs b/crates/data-avalanche-config/src/walk.rs
index 9e584e3..1780547 100644
--- a/crates/data-avalanche-config/src/walk.rs
+++ b/crates/data-avalanche-config/src/walk.rs
@@ -297,7 +297,8 @@ walk_fields!(RawMemberlist {
 });
 walk_fields!(RawTracing {
     otlp_endpoint,
-    sample_ratio
+    sample_ratio,
+    headers
 });
 walk_fields!(RawAzure { account_name });
 
```

`crates/data-avalanche-telemetry/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-telemetry/Cargo.toml b/crates/data-avalanche-telemetry/Cargo.toml
index 12100b0..ceaac9b 100644
--- a/crates/data-avalanche-telemetry/Cargo.toml
+++ b/crates/data-avalanche-telemetry/Cargo.toml
@@ -21,4 +21,6 @@ tracing-subscriber = { workspace = true, features = ["registry"] }
 
 [dev-dependencies]
 data-avalanche-testkit.workspace = true
+rcgen.workspace = true
+reqwest = { workspace = true, features = ["blocking"] }
 tokio = { workspace = true, features = ["rt", "macros", "net", "io-util", "time"] }
```

`crates/data-avalanche-telemetry/src/traces.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/src/traces.rs b/crates/data-avalanche-telemetry/src/traces.rs
index b710a2d..00015f9 100644
--- a/crates/data-avalanche-telemetry/src/traces.rs
+++ b/crates/data-avalanche-telemetry/src/traces.rs
@@ -5,8 +5,10 @@
 //! One provider and one `tracing` layer serve the whole run: a reload of `globals.tracing` swaps
 //! the exporter behind them and the sampling ratio (D-M10.6). A layer can't be reloaded: the
 //! logs find a span's ids by downcasting to it, which a reload layer doesn't pass through. The
-//! batch exporter runs on a thread of its own and exports with a blocking client.
+//! batch exporter runs on a thread of its own and exports with a blocking client. Each export
+//! carries `globals.tracing.headers` (DA-OBS-7): a hosted collector's token or API key.
 
+use std::collections::{BTreeMap, HashMap};
 use std::fmt;
 use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
 use std::sync::{Arc, PoisonError, RwLock};
@@ -14,7 +16,7 @@ use std::time::{Duration, Instant};
 
 use opentelemetry::trace::{Link, SpanKind, TraceContextExt, TraceId, TracerProvider};
 use opentelemetry::{Context, KeyValue};
-use opentelemetry_otlp::WithExportConfig;
+use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
 use opentelemetry_sdk::Resource;
 use opentelemetry_sdk::error::OTelSdkResult;
 use opentelemetry_sdk::trace::{
@@ -216,8 +218,11 @@ pub struct Traces {
     provider: SdkTracerProvider,
     switch: Switch,
     ratio: Ratio,
-    /// The endpoint in force.
-    endpoint: RwLock<Option<String>>,
+    /// The endpoint in force, with its headers.
+    endpoint: RwLock<Option<(String, BTreeMap<String, String>)>>,
+    /// Roots the exporter's client trusts besides the platform's: a test's certificate.
+    #[cfg(test)]
+    roots: Vec<Vec<u8>>,
 }
 
 /// Shows the endpoint as it may be logged ([`shown`]): an operator may put credentials in it.
@@ -227,8 +232,8 @@ impl fmt::Debug for Traces {
             .endpoint
             .read()
             .unwrap_or_else(PoisonError::into_inner)
-            .as_deref()
-            .map(shown);
+            .as_ref()
+            .map(|(e, _)| shown(e));
         f.debug_struct("Traces")
             .field("endpoint", &endpoint)
             .field("sample_ratio", &self.ratio.get())
@@ -263,23 +268,31 @@ impl Traces {
             switch,
             ratio,
             endpoint: RwLock::default(),
+            #[cfg(test)]
+            roots: Vec::new(),
         }
     }
 
-    /// Puts `globals.tracing` in force: spans go to `endpoint`'s `/v1/traces`, `sample_ratio` of
-    /// the traces; `None` exports nothing. The same endpoint again keeps its exporter. Returns
-    /// whether traces are exported now.
+    /// Puts `globals.tracing` in force: spans go to `endpoint`'s `/v1/traces` with `headers` on
+    /// each export, `sample_ratio` of the traces; `None` exports nothing. The same endpoint and
+    /// headers again keep their exporter. Returns whether traces are exported now.
     ///
     /// # Errors
     /// [`TracesError::Endpoint`]: `endpoint` isn't a URL the exporter takes; what was in force
     /// stays.
-    pub fn set(&self, endpoint: Option<&str>, sample_ratio: f64) -> Result<bool, TracesError> {
+    pub fn set(
+        &self,
+        endpoint: Option<&str>,
+        headers: &BTreeMap<String, String>,
+        sample_ratio: f64,
+    ) -> Result<bool, TracesError> {
         let mut current = self
             .endpoint
             .write()
             .unwrap_or_else(PoisonError::into_inner);
-        if current.as_deref() != endpoint {
-            let exporter = endpoint.map(|e| self.exporter(e)).transpose()?;
+        let wanted = endpoint.map(|e| (e.to_owned(), headers.clone()));
+        if *current != wanted {
+            let exporter = endpoint.map(|e| self.exporter(e, headers)).transpose()?;
             let old = std::mem::replace(
                 &mut self
                     .switch
@@ -294,7 +307,7 @@ impl Traces {
             if let Some(old) = old {
                 std::thread::spawn(move || drop(old));
             }
-            *current = endpoint.map(str::to_owned);
+            *current = wanted;
         }
         let ratio = if endpoint.is_some() {
             sample_ratio
@@ -305,16 +318,46 @@ impl Traces {
         Ok(endpoint.is_some())
     }
 
-    fn exporter(&self, endpoint: &str) -> Result<Exporter, TracesError> {
+    /// A blocking client that also trusts `roots` (DER), built off any runtime's thread as the
+    /// exporter builds its own.
+    #[cfg(test)]
+    fn trusting(roots: &[Vec<u8>]) -> reqwest::blocking::Client {
+        let certs: Vec<reqwest::Certificate> = roots
+            .iter()
+            .map(|der| reqwest::Certificate::from_der(der).unwrap())
+            .collect();
+        std::thread::spawn(move || {
+            reqwest::blocking::Client::builder()
+                .tls_certs_merge(certs)
+                .build()
+                .unwrap()
+        })
+        .join()
+        .unwrap()
+    }
+
+    /// An exporter to `endpoint` with `headers`. The exporter drops a header whose name or value
+    /// HTTP doesn't take: the config refuses them (V-126).
+    fn exporter(
+        &self,
+        endpoint: &str,
+        headers: &BTreeMap<String, String>,
+    ) -> Result<Exporter, TracesError> {
         let url = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
-        let mut exporter = opentelemetry_otlp::SpanExporter::builder()
+        let builder = opentelemetry_otlp::SpanExporter::builder()
             .with_http()
             .with_endpoint(url)
-            .build()
-            .map_err(|e| TracesError::Endpoint {
-                endpoint: endpoint.to_owned(),
-                reason: e.to_string(),
-            })?;
+            .with_headers(headers.clone().into_iter().collect::<HashMap<_, _>>());
+        #[cfg(test)]
+        let builder = if self.roots.is_empty() {
+            builder
+        } else {
+            builder.with_http_client(Self::trusting(&self.roots))
+        };
+        let mut exporter = builder.build().map_err(|e| TracesError::Endpoint {
+            endpoint: endpoint.to_owned(),
+            reason: e.to_string(),
+        })?;
         let resource = self
             .switch
             .0
```

`crates/data-avalanche-testkit/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/lib.rs b/crates/data-avalanche-testkit/src/lib.rs
index 25c636e..39a3635 100644
--- a/crates/data-avalanche-testkit/src/lib.rs
+++ b/crates/data-avalanche-testkit/src/lib.rs
@@ -9,7 +9,7 @@ pub mod memory_sink;
 pub mod otlp;
 pub use fake_source::{FakeSource, FakeTxn, Method};
 pub use memory_sink::{MemorySink, SinkOp};
-pub use otlp::{OtlpRequest, otlp_collector, otlp_collector_answering};
+pub use otlp::{OtlpRequest, otlp_collector, otlp_collector_answering, otlp_collector_tls};
 
 use futures_util::TryStreamExt;
 use object_store::ObjectStore;
```

`crates/data-avalanche-testkit/src/otlp.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/otlp.rs b/crates/data-avalanche-testkit/src/otlp.rs
index ccd700e..ce9e98d 100644
--- a/crates/data-avalanche-testkit/src/otlp.rs
+++ b/crates/data-avalanche-testkit/src/otlp.rs
@@ -1,15 +1,21 @@
 //! A fake OTLP/HTTP collector (config §6.6), on a thread of its own: it answers every request
-//! 200 (or the status it is given) and hands over its path and body.
+//! 200 (or the status it is given) and hands over its path, headers and body. Plain HTTP, or TLS
+//! with a certificate the test makes ([`otlp_collector_tls`]).
 
 use std::io::{BufRead, BufReader, Read, Write};
 use std::net::TcpListener;
 use std::sync::atomic::{AtomicU16, Ordering};
 use std::sync::{Arc, mpsc};
 
+use rustls::pki_types::pem::PemObject;
+use rustls::pki_types::{CertificateDer, PrivateKeyDer};
+
 /// One export the collector took.
 #[derive(Debug, Clone)]
 pub struct OtlpRequest {
     pub path: String,
+    /// Each header, its name lowercased, its value trimmed, in the order sent.
+    pub headers: Vec<(String, String)>,
     /// The protobuf body: span names and attribute values are in it verbatim.
     pub body: Vec<u8>,
 }
@@ -20,6 +26,15 @@ impl OtlpRequest {
     pub fn has(&self, text: &str) -> bool {
         self.body.windows(text.len()).any(|w| w == text.as_bytes())
     }
+
+    /// The value of header `name` (lowercase).
+    #[must_use]
+    pub fn header(&self, name: &str) -> Option<&str> {
+        self.headers
+            .iter()
+            .find(|(n, _)| n == name)
+            .map(|(_, v)| v.as_str())
+    }
 }
 
 /// A collector on `127.0.0.1`: its base URL and the requests it takes.
@@ -42,31 +57,99 @@ pub fn otlp_collector_answering(status: Arc<AtomicU16>) -> (String, mpsc::Receiv
     let (tx, rx) = mpsc::channel();
     std::thread::spawn(move || {
         for conn in listener.incoming() {
-            let mut conn = conn.unwrap();
-            let mut reader = BufReader::new(conn.try_clone().unwrap());
-            let mut line = String::new();
-            reader.read_line(&mut line).unwrap();
-            let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
-            let mut length = 0;
-            loop {
-                let mut header = String::new();
-                reader.read_line(&mut header).unwrap();
-                if header == "\r\n" {
-                    break;
-                }
-                if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
-                    length = v.trim().parse().unwrap();
-                }
+            if !answer(conn.unwrap(), &status, &tx) {
+                return;
             }
-            let mut body = vec![0; length];
-            reader.read_exact(&mut body).unwrap();
-            let status = status.load(Ordering::Relaxed);
-            conn.write_all(format!("HTTP/1.1 {status} S\r\ncontent-length: 0\r\n\r\n").as_bytes())
-                .unwrap();
-            if tx.send(OtlpRequest { path, body }).is_err() {
+        }
+    });
+    (url, rx)
+}
+
+/// A collector over TLS with the certificate `cert_pem` (for `localhost`) and its key: its base
+/// URL, `https://localhost:<port>`, and the requests it takes. A connection whose handshake fails
+/// (a client that doesn't trust the certificate) is dropped.
+///
+/// # Panics
+/// As [`otlp_collector`], and when the certificate or key doesn't parse.
+#[must_use]
+pub fn otlp_collector_tls(cert_pem: &str, key_pem: &str) -> (String, mpsc::Receiver<OtlpRequest>) {
+    let certs = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
+        .collect::<Result<Vec<_>, _>>()
+        .unwrap();
+    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).unwrap();
+    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
+        rustls::crypto::aws_lc_rs::default_provider(),
+    ))
+    .with_safe_default_protocol_versions()
+    .unwrap()
+    .with_no_client_auth()
+    .with_single_cert(certs, key)
+    .unwrap();
+    let config = Arc::new(config);
+    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
+    let url = format!(
+        "https://localhost:{}",
+        listener.local_addr().unwrap().port()
+    );
+    let (tx, rx) = mpsc::channel();
+    let status = Arc::new(AtomicU16::new(200));
+    std::thread::spawn(move || {
+        for conn in listener.incoming() {
+            let tls = rustls::ServerConnection::new(Arc::clone(&config)).unwrap();
+            let mut stream = rustls::StreamOwned::new(tls, conn.unwrap());
+            // The handshake runs on the first read: a client that refuses it ends here.
+            if stream.conn.complete_io(&mut stream.sock).is_err() {
+                continue;
+            }
+            if !answer(stream, &status, &tx) {
                 return;
             }
         }
     });
     (url, rx)
 }
+
+/// Reads one request from `conn`, answers it, and hands it over; false once nobody takes them.
+fn answer<S: Read + Write>(
+    mut conn: S,
+    status: &AtomicU16,
+    tx: &mpsc::Sender<OtlpRequest>,
+) -> bool {
+    let (path, headers, body) = {
+        let mut reader = BufReader::new(&mut conn);
+        let mut line = String::new();
+        if reader.read_line(&mut line).is_err() {
+            return true;
+        }
+        let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
+        let mut headers = vec![];
+        let mut length = 0;
+        loop {
+            let mut header = String::new();
+            reader.read_line(&mut header).unwrap();
+            if header == "\r\n" || header.is_empty() {
+                break;
+            }
+            if let Some((name, value)) = header.split_once(':') {
+                let (name, value) = (name.trim().to_ascii_lowercase(), value.trim().to_owned());
+                if name == "content-length" {
+                    length = value.parse().unwrap();
+                }
+                headers.push((name, value));
+            }
+        }
+        let mut body = vec![0; length];
+        reader.read_exact(&mut body).unwrap();
+        (path, headers, body)
+    };
+    let status = status.load(Ordering::Relaxed);
+    conn.write_all(format!("HTTP/1.1 {status} S\r\ncontent-length: 0\r\n\r\n").as_bytes())
+        .unwrap();
+    let _ = conn.flush();
+    tx.send(OtlpRequest {
+        path,
+        headers,
+        body,
+    })
+    .is_ok()
+}
```

`crates/data-avalanche/src/logging.rs`:

```diff
diff --git a/crates/data-avalanche/src/logging.rs b/crates/data-avalanche/src/logging.rs
index d719a72..04ee609 100644
--- a/crates/data-avalanche/src/logging.rs
+++ b/crates/data-avalanche/src/logging.rs
@@ -183,6 +183,7 @@ where
         tracing: Mutex::new(Tracing {
             otlp_endpoint: None,
             sample_ratio: 0.0,
+            headers: Default::default(),
         }),
     };
     Ok((dispatch, logger))
@@ -296,7 +297,12 @@ impl Logger {
             return;
         }
         let endpoint = tracing.otlp_endpoint.as_deref();
-        match self.traces.set(endpoint, tracing.sample_ratio) {
+        let headers = tracing
+            .headers
+            .iter()
+            .map(|(name, value)| (name.clone(), value.expose().to_owned()))
+            .collect();
+        match self.traces.set(endpoint, &headers, tracing.sample_ratio) {
             Ok(on) => {
                 let spans = if on { traced() } else { Targets::new() };
                 if let Err(e) = self.spans.reload(spans) {
```

`docs/src/content/docs/reference/configuration.md`:

```diff
diff --git a/docs/src/content/docs/reference/configuration.md b/docs/src/content/docs/reference/configuration.md
index 45e2d5e..0e3e545 100644
--- a/docs/src/content/docs/reference/configuration.md
+++ b/docs/src/content/docs/reference/configuration.md
@@ -137,6 +137,7 @@ OTLP/HTTP traces of flushes, commits, snapshot chunks and schema changes.
 |---|---|---|---|---|---|
 | `otlp_endpoint` | string | — | applied |  | The OTLP/HTTP collector's base URL (`http://otel-collector:4318`); spans go to `<endpoint>/v1/traces`. Absent turns tracing off. |
 | `sample_ratio` | number | 0.05 with an endpoint, else 0 | applied | [V-097](/data-avalanche/reference/validation-rules/#v-097) [V-098](/data-avalanche/reference/validation-rules/#v-098) | The share of flushes traced, from 0 to 1. |
+| `headers.<key>` | map of string | none | applied |  | Headers sent with each export, a map of name to value: a hosted collector's token or API key (`authorization: Bearer ${OTLP_TOKEN}`). The values are secrets (V-126). |
 
 ## `globals.azure`
 
```

`docs/src/content/docs/reference/validation-rules.md`:

```diff
diff --git a/docs/src/content/docs/reference/validation-rules.md b/docs/src/content/docs/reference/validation-rules.md
index cc519e9..216c1e3 100644
--- a/docs/src/content/docs/reference/validation-rules.md
+++ b/docs/src/content/docs/reference/validation-rules.md
@@ -131,3 +131,4 @@ A config with any **error** is rejected whole: at startup the node refuses to st
 | <span id="v-123"></span>V-123 | error | `source.postgres.source_change_action` | ∈ {error, re_snapshot}. |
 | <span id="v-124"></span>V-124 | warning | `source.ssl_mode = verify-ca, when source.type = mysql` | The host name is checked too, as with verify-full (DA-SRC-22). |
 | <span id="v-125"></span>V-125 | warning | `globals.api.jwt.jwks_url, globals.api.oauth2.jwks_url and discovery_url` | Not https: the issuer's keys could be swapped on the way (DA-API-16). |
+| <span id="v-126"></span>V-126 | error | `globals.tracing.headers` | Each name an HTTP token, each value visible ASCII, spaces and tabs (DA-OBS-7). |
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-config -p data-avalanche-telemetry -p data-avalanche -p xtask --no-fail-fast
```

Expected: PASS, 228 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche-config/src/build.rs \
  crates/data-avalanche-config/src/field_docs.rs \
  crates/data-avalanche-config/src/raw.rs \
  crates/data-avalanche-config/src/registry.rs \
  crates/data-avalanche-config/src/resolved.rs \
  crates/data-avalanche-config/src/rules/domain.rs \
  crates/data-avalanche-config/src/walk.rs \
  crates/data-avalanche-config/tests/load.rs \
  crates/data-avalanche-config/tests/rules/domain.rs \
  crates/data-avalanche-telemetry/Cargo.toml \
  crates/data-avalanche-telemetry/src/traces.rs \
  crates/data-avalanche-telemetry/tests/traces.rs \
  crates/data-avalanche-testkit/src/lib.rs \
  crates/data-avalanche-testkit/src/otlp.rs \
  crates/data-avalanche/src/logging.rs \
  docs/src/content/docs/reference/configuration.md \
  docs/src/content/docs/reference/validation-rules.md
git commit -m "feat(telemetry): the OTLP export sends globals.tracing.headers, secrets checked by V-126; an https collector tested (DA-OBS-7)"
```

---

### Task 13: The lag across sessions, and the JSON log's ids end to end (`cluster`, `pipeline`, the binary)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-pipeline/src/writer.rs`
- Test: `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/observability.rs`

**Gaps:** DA-OBS-8, DA-OBS-9.

**Interfaces:**
- Consumes: the controller's lag (`WorkerStatus.lag_since`, `frontier`, `reconnects`) (M10); the writer's flush span (M10); the E2E harness and its Jaeger scenario (M10).
- Produces:
  - in the controller: `struct Lag { kept, own }` with `since()`, `roll()`, `carry(session, st)`; `Running.lag_session` replaces `lag_shown`;
  - the writer logs `flush durable` (`rows`, `checkpoint`) at `debug` inside the flush span;
  - the E2E harness's `Options.log` (the log's format and filter).

The lag read 0 early in two windows: a restarted worker that made its first re-read change
durable before re-reading the next, and a source reconnect inside a worker. The controller now
keeps what an earlier session left pending (`kept`) until the new session's stream has read as
far: a worker's first status, or a rise in `reconnects`, rolls the session. The JSON log's
`trace_id`/`span_id` were tested under a scoped dispatch only: the Jaeger E2E now runs the binary
with JSON logs and matches a logged flush's ids to a trace Jaeger holds.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 19b2750..720dee0 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1522,46 +1522,59 @@ mod tests {
         );
     }
 
-    /// A worker that follows one which left a change pending keeps its start until its stream has
-    /// read as far with nothing pending; one that has shown a change pending is the lag itself.
+    /// A session that follows one which left a change pending keeps that start until its stream
+    /// has read as far, even once its own first re-read change is durable (DA-OBS-8); a source
+    /// reconnect starts a new session as a new worker does.
     #[test]
-    fn the_kept_lag_start_waits_for_the_next_worker_to_read_as_far() {
+    fn the_kept_lag_start_waits_for_the_next_session_to_read_as_far() {
         let at = |lsn| Some(SourcePosition::Pg(data_avalanche_core::position::Lsn(lsn)));
-        let kept = Some(Pending {
-            since: 1,
-            read: at(100),
-        });
-        let status = |read, unsynced| WorkerStatus {
+        let status = |read, unsynced, reconnects| WorkerStatus {
             read,
             checkpoint: at(40),
             unsynced_since_micros: unsynced,
+            reconnects,
             ..WorkerStatus::default()
         };
-        let (mut pending, mut shown) = (kept.clone(), false);
+        // The first worker reads to 100 with a change of time 1 pending, then ends.
+        let mut lag = Lag::default();
+        let mut session = None;
+        lag.carry(&mut session, &status(at(100), Some(1), 0));
+        assert_eq!(lag.since(), Some(1));
+
+        // The next worker: nothing read yet, then short of 100: the kept start holds.
+        let mut session = None;
         for (read, what) in [(None, "nothing read yet"), (at(60), "short of it")] {
-            carry(&mut pending, &mut shown, &status(read, None));
-            assert_eq!(pending, kept, "{what}");
+            lag.carry(&mut session, &status(read, None, 0));
+            assert_eq!(lag.since(), Some(1), "{what}");
         }
-        carry(&mut pending, &mut shown, &status(at(100), None));
-        assert_eq!(pending, None, "read as far, nothing pending");
+        // It re-reads the change, makes it durable before reading on: still pending at 100.
+        lag.carry(&mut session, &status(at(60), Some(1), 0));
+        lag.carry(&mut session, &status(at(60), None, 0));
+        assert_eq!(
+            lag.since(),
+            Some(1),
+            "its first change durable, not yet at 100"
+        );
+        lag.carry(&mut session, &status(at(100), None, 0));
+        assert_eq!(lag.since(), None, "read as far, nothing pending");
 
-        let (mut pending, mut shown) = (kept.clone(), false);
-        carry(&mut pending, &mut shown, &status(at(60), Some(7)));
+        // A reconnect inside one worker: the new session starts with nothing pending.
+        lag.carry(&mut session, &status(at(150), Some(5), 0));
+        lag.carry(&mut session, &status(at(120), None, 1));
         assert_eq!(
-            pending,
-            Some(Pending {
-                since: 7,
-                read: at(60)
-            }),
-            "read again: the worker's own"
+            lag.since(),
+            Some(5),
+            "the reconnected session hasn't read to 150"
         );
-        carry(&mut pending, &mut shown, &status(at(60), None));
-        assert_eq!(pending, None, "then durable: the worker says so");
+        lag.carry(&mut session, &status(at(140), Some(6), 1));
+        assert_eq!(lag.since(), Some(5), "the older of the two");
+        lag.carry(&mut session, &status(at(160), None, 1));
+        assert_eq!(lag.since(), None);
 
-        let (mut pending, mut shown) = (None, false);
-        carry(&mut pending, &mut shown, &status(None, Some(9)));
+        let mut lag = Lag::default();
+        lag.carry(&mut None, &status(None, Some(9), 0));
         assert_eq!(
-            pending.unwrap().read,
+            lag.own.unwrap().read,
             at(40),
             "before a frontier, the checkpoint"
         );
```

`crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index 0bc64ce..49b6491 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -89,6 +89,8 @@ pub struct Options<'a> {
     pub api: &'a str,
     /// More `globals` keys, one YAML line, e.g. `tracing: { otlp_endpoint: … }`.
     pub globals: &'a str,
+    /// `globals.log`, a YAML flow map; `{ level: info, format: compact }` when empty.
+    pub log: &'a str,
 }
 
 /// The rows [`E2e::insert_series`] gives a key `g`.
@@ -279,7 +281,7 @@ globals:
   cloud: aws
   state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
   api: {{ address: "127.0.0.1:${{E2E_API_PORT}}"{api} }}
-  log: {{ level: info, format: compact }}
+  log: {log}
   cluster:
     lease_duration: 6s
     lease_renewal_interval: 2s
@@ -306,6 +308,11 @@ pipelines:
             memberlist = o.memberlist,
             api = o.api,
             globals = o.globals,
+            log = if o.log.is_empty() {
+                "{ level: info, format: compact }"
+            } else {
+                o.log
+            },
         )
     }
 
```

`crates/data-avalanche/tests/e2e/observability.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/observability.rs b/crates/data-avalanche/tests/e2e/observability.rs
index 8812505..a339372 100644
--- a/crates/data-avalanche/tests/e2e/observability.rs
+++ b/crates/data-avalanche/tests/e2e/observability.rs
@@ -267,7 +267,9 @@ fn roots<'a>(spans: &'a [Span], name: &str, pipeline: &str) -> Vec<(&'a Span, Ve
 
 /// Config §6.6: with an endpoint and every trace sampled, a flush is a trace whose root carries
 /// the pipeline and whose children are its tables' commits; a snapshot chunk is a trace of its
-/// own; the node is the resource's `service.instance.id`.
+/// own; the node is the resource's `service.instance.id`. The binary logs in JSON, and a line
+/// logged in a flush carries that flush's trace and span ids (DA-OBS-9: the process's global
+/// dispatch, end to end).
 #[tokio::test]
 async fn the_flushes_commits_and_chunks_are_traced_to_the_collector() {
     require_it!();
@@ -277,6 +279,7 @@ async fn the_flushes_commits_and_chunks_are_traced_to_the_collector() {
         ITEMS,
         Options {
             globals: &tracing,
+            log: r#"{ level: "info,data_avalanche_pipeline::writer=debug", format: json }"#,
             ..options()
         },
     )
@@ -318,6 +321,27 @@ async fn the_flushes_commits_and_chunks_are_traced_to_the_collector() {
         .into_iter()
         .next()
         .expect("a flush root, as the wait saw");
+    // DA-OBS-9: the flushes' JSON lines name their traces, and one of them is a trace Jaeger has.
+    let traced_flushes: Vec<(String, String)> = roots(&flushes, "pipeline.flush", &e.test)
+        .iter()
+        .map(|(f, _)| (f.trace.to_ascii_lowercase(), f.id.to_ascii_lowercase()))
+        .collect();
+    let logged: Vec<(String, String)> = p
+        .log()
+        .lines()
+        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
+        .filter(|v| v["fields"]["message"] == "flush durable" && v["fields"]["pipeline"] == e.test)
+        .filter_map(|v| {
+            Some((
+                v["trace_id"].as_str()?.to_ascii_lowercase(),
+                v["span_id"].as_str()?.to_ascii_lowercase(),
+            ))
+        })
+        .collect();
+    assert!(
+        logged.iter().any(|ids| traced_flushes.contains(ids)),
+        "a logged flush's ids are a traced flush's: logged {logged:?}, traced {traced_flushes:?}"
+    );
     assert_eq!(
         flush.resource.get("service.name").map(String::as_str),
         Some("data-avalanche")
```

- [ ] **Step 2: Run them to see them fail**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-cluster -p data-avalanche-pipeline -p data-avalanche --no-fail-fast
cargo test -p data-avalanche --features e2e-hooks --test e2e --no-fail-fast -- --test-threads=1 observability::
```

Expected: FAIL to compile:
  - `error[E0433]: cannot find type `Lag` in this scope (crates/data-avalanche-cluster/src/controller.rs)`

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 720dee0..a4544cd 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -347,28 +347,69 @@ struct Pending {
     read: Option<SourcePosition>,
 }
 
-/// The kept lag's start after a worker's status `st`. A worker that has shown a change pending
-/// (`shown`) is the lag from then on. Before that, a worker that follows an earlier one is
-/// re-reading from the checkpoint, and the start the earlier one left stays: its changes still
-/// count until this one has read as far again with nothing pending (its stream's frontier at or
-/// past the kept one). A change it reads again first makes it the lag's source instead.
-fn carry(pending: &mut Option<Pending>, shown: &mut bool, st: &WorkerStatus) {
-    *shown |= st.unsynced_since_micros.is_some();
-    if *shown {
-        *pending = st.unsynced_since_micros.map(|since| Pending {
+/// The lag's start over one lease term (DA-OBS-8): the oldest change still pending, of the
+/// session running now (`own`) and of the sessions before it (`kept`: earlier workers', and this
+/// worker's before a source reconnect). A session re-reads from the checkpoint, so what an
+/// earlier one left counts until this one's stream has read as far again (its frontier at or
+/// past the kept one), even when it makes its own first changes durable before that.
+#[derive(Debug, Clone, Default, PartialEq)]
+struct Lag {
+    kept: Option<Pending>,
+    own: Option<Pending>,
+}
+
+impl Lag {
+    /// The oldest pending change's commit time.
+    fn since(&self) -> Option<i64> {
+        [&self.kept, &self.own]
+            .into_iter()
+            .flatten()
+            .map(|p| p.since)
+            .min()
+    }
+
+    /// A session ended (its worker, or its stream at a reconnect): what it had pending is kept,
+    /// with the older start and the further frontier of the two.
+    fn roll(&mut self) {
+        self.kept = match (self.kept.take(), self.own.take()) {
+            (Some(k), Some(o)) => {
+                let read = match (&o.read, &k.read) {
+                    (Some(or), Some(kr)) if or.covers(kr).unwrap_or(false) => o.read.clone(),
+                    (Some(_), None) => o.read.clone(),
+                    _ => k.read.clone(),
+                };
+                Some(Pending {
+                    since: k.since.min(o.since),
+                    read,
+                })
+            }
+            (k, o) => k.or(o),
+        };
+    }
+
+    /// Takes a worker's status `st`; `session` is the reconnect count of the session this
+    /// worker's lag last saw, `None` before its first status.
+    fn carry(&mut self, session: &mut Option<u64>, st: &WorkerStatus) {
+        if session.is_none_or(|n| st.reconnects > n) {
+            self.roll();
+        }
+        *session = Some(st.reconnects);
+        self.own = st.unsynced_since_micros.map(|since| Pending {
             since,
             read: st.read.clone().or_else(|| st.checkpoint.clone()),
         });
-        return;
-    }
-    let caught_up = pending.as_ref().is_some_and(|p| match (&st.read, &p.read) {
-        // Positions that don't compare (another source kind) say the kept changes are gone.
-        (Some(read), Some(kept)) => read.covers(kept).unwrap_or(true),
-        (Some(_), None) => true,
-        (None, _) => false,
-    });
-    if caught_up {
-        *pending = None;
+        let caught_up = self
+            .kept
+            .as_ref()
+            .is_some_and(|p| match (&st.read, &p.read) {
+                // Positions that don't compare (another source kind) say the kept changes are gone.
+                (Some(read), Some(kept)) => read.covers(kept).unwrap_or(true),
+                (Some(_), None) => true,
+                (None, _) => false,
+            });
+        if caught_up {
+            self.kept = None;
+        }
     }
 }
 
@@ -392,8 +433,9 @@ struct Running {
     logged: HashSet<String>,
     /// The worker's reconnects already logged.
     reconnects: u64,
-    /// It has shown a change pending: its status is the lag's start from then on ([`carry`]).
-    lag_shown: bool,
+    /// The reconnect count of the session the lag last saw ([`Lag::carry`]); `None` before
+    /// this worker's first status.
+    lag_session: Option<u64>,
 }
 
 /// A worker never outlives its controller: a controller that panics drops its `Running`, and
@@ -692,7 +734,7 @@ impl PipelineController {
         let mut failures: u32 = 0;
         // The lag's start across this term's workers: what one read and didn't make durable still
         // counts while none runs and while the next one reads it again (I2).
-        let mut pending: Option<Pending> = None;
+        let mut pending = Lag::default();
         // One lease term: a stale reader is ended at most once in it, by whichever of its
         // workers meets it first, a stop and a resume included (DA-SRC-28).
         let reader_ended = Arc::new(AtomicBool::new(false));
@@ -931,7 +973,7 @@ impl PipelineController {
                         // No worker runs (a restart's backoff, ERROR): the lag grows on.
                         None => self
                             .metrics
-                            .replication_lag(lag(pending.as_ref().map(|p| p.since), now_micros())),
+                            .replication_lag(lag(pending.since(), now_micros())),
                     }
                     self.report(&mut reported).await;
                     let asked = self.asked(&mut seen).await;
@@ -998,7 +1040,7 @@ impl PipelineController {
 
     /// Discard and replay (config §12.4): what the worker read past the durable checkpoint is
     /// read again by the next one. False when no worker runs.
-    async fn discard(&self, worker: &mut Option<Running>, pending: &mut Option<Pending>) -> bool {
+    async fn discard(&self, worker: &mut Option<Running>, pending: &mut Lag) -> bool {
         let Some(mut w) = worker.take() else {
             return false;
         };
@@ -1099,7 +1141,7 @@ impl PipelineController {
             paused: false,
             logged: HashSet::new(),
             reconnects: 0,
-            lag_shown: false,
+            lag_session: None,
         })
     }
 
@@ -1107,7 +1149,7 @@ impl PipelineController {
     /// off (what it read past the durable checkpoint is read again by the next run). True when
     /// it ended on its own; either way the worker has ended when this returns. A cut-off
     /// worker's writer is aborted with it, not waited for.
-    async fn drain(&self, mut w: Running, pending: &mut Option<Pending>) -> bool {
+    async fn drain(&self, mut w: Running, pending: &mut Lag) -> bool {
         let ended = tokio::time::timeout(self.settings.drain_timeout, async {
             let _ = w.handle.control.send(Command::Drain).await;
             (&mut w.task).await
@@ -1145,7 +1187,7 @@ impl PipelineController {
     /// Drains the worker and releases the lease; a worker cut off leaves it to expire.
     async fn let_go(&self, run: LeaseRun, worker: Option<Running>) {
         let drained = match worker {
-            Some(w) => self.drain(w, &mut None).await,
+            Some(w) => self.drain(w, &mut Lag::default()).await,
             None => true,
         };
         if drained {
@@ -1169,7 +1211,7 @@ impl PipelineController {
     /// once the lease is let go of ([`Self::remove_orphan`]).
     async fn remove(&self, run: LeaseRun, worker: Option<Running>) -> bool {
         let drained = match worker {
-            Some(w) => self.drain(w, &mut None).await,
+            Some(w) => self.drain(w, &mut Lag::default()).await,
             None => true,
         };
         let done = if !drained {
@@ -1313,7 +1355,7 @@ impl PipelineController {
     }
 
     /// The worker's status into the metrics, and its new warnings and reconnects into the log.
-    fn observe(&self, w: &mut Running, pending: &mut Option<Pending>) {
+    fn observe(&self, w: &mut Running, pending: &mut Lag) {
         let st = w.handle.status.borrow().clone();
         for warning in &st.warnings {
             if w.logged.insert(warning.clone()) {
@@ -1329,8 +1371,8 @@ impl PipelineController {
         let m = &self.metrics;
         m.wal_lag_bytes(st.retained_bytes);
         m.binlog_lag(st.source_behind);
-        carry(pending, &mut w.lag_shown, &st);
-        m.replication_lag(lag(pending.as_ref().map(|p| p.since), now_micros()));
+        pending.carry(&mut w.lag_session, &st);
+        m.replication_lag(lag(pending.since(), now_micros()));
         m.transaction_buffer_bytes(st.buffer_bytes);
         m.soft_dropped_columns(st.soft_dropped.values().sum());
         m.halted_tables(st.halted.len());
```

`crates/data-avalanche-pipeline/src/writer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index c00a796..db26d33 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -190,6 +190,8 @@ impl Writer {
         self.hook(HookPoint::CheckpointPersisted {
             position: job.checkpoint.clone(),
         })?;
+        // Inside the flush's span: a JSON line carries its trace (DA-OBS-9).
+        tracing::debug!(rows, checkpoint = %job.checkpoint, "flush durable");
         // A refused table's progress isn't in the sink: its document stays as it was. The table
         // is in `rejected` already when this job's change set was the one refused.
         for doc in &job.snapshots {
```

- [ ] **Step 4: Run them to see them pass**

Run (with `DATA_AVALANCHE_IT=1`):

```bash
cargo test -p data-avalanche-cluster -p data-avalanche-pipeline -p data-avalanche --no-fail-fast
cargo test -p data-avalanche --features e2e-hooks --test e2e --no-fail-fast -- --test-threads=1 observability::
```

Expected: PASS, 456 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/controller.rs \
  crates/data-avalanche-pipeline/src/writer.rs \
  crates/data-avalanche/tests/e2e/harness.rs \
  crates/data-avalanche/tests/e2e/observability.rs
git commit -m "fix(cluster): the lag keeps an earlier session's start until the stream reads as far; the JSON log's ids matched to Jaeger end to end (DA-OBS-8, DA-OBS-9)"
```

---

### Task 14: The dependency audit

**Files:**
- Create: `.cargo/audit.toml`
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: `Cargo.lock`; CI's non-blocking `cargo audit` step (M0).
- Produces: the lockfile on the patched releases; `.cargo/audit.toml`, the triaged advisories with the reason each stays and what lifts it.

`cargo audit` on the base found 17 vulnerabilities and 23 warnings. All but three are fixed
by semver-compatible releases: `cargo update` of the named packages moves the lock. The three
left are triaged: quick-xml 0.39.4 (two advisories) comes only through deltalake's
`object_store` 0.13 (roadmap R-13), which reads the lake's own S3 and Azure answers; no 0.13
release takes the fix, and deltalake 1.1.3 still requires `^0.13.2`. proc-macro-error2 is a
build-time macro of `validator_derive`. CI's audit step stays non-blocking (roadmap §4 M12): this
task is where the audit is enforced.

- [ ] **Step 1: Implement**

The lock moves by `cargo update -p` of the advised packages (rustls 0.23.46, rustls-webpki
0.103.16, webpki-roots and webpki-root-certs 1.0.11, ctutils 0.4.8, zstd 0.13.4 and 0.14.3,
zstd-safe 7.4.0 and 8.2.0, zstd-sys 2.1.2, tokio 1.54.1, yoke-derive 0.8.4). The diff below is
the result: apply it as is, since a later `cargo update` may pick newer patch releases.

`.cargo/audit.toml`:

```diff
diff --git a/.cargo/audit.toml b/.cargo/audit.toml
new file mode 100644
index 0000000..e78f9d5
--- /dev/null
+++ b/.cargo/audit.toml
@@ -0,0 +1,16 @@
+# `cargo audit` (RustSec): the advisories triaged and accepted (M12b Task 14, roadmap §4 M12's
+# dependency audit). Every other advisory fails the CI step. Each entry names why it stays and
+# what lifts it; re-check at every deltalake release.
+[advisories]
+ignore = [
+    # quick-xml 0.39.4: memory exhaustion through namespace declarations in `NsReader`, and a
+    # quadratic check of a start tag's duplicate attributes. Only deltalake's object_store 0.13
+    # links it (roadmap R-13), to read the S3 and Azure list and error answers of the lake's own
+    # store: a configured endpoint, not an attacker's. No 0.13 release takes quick-xml 0.41, and
+    # deltalake 1.1.3 still requires object_store ^0.13.2 (checked 2026-11-04).
+    "RUSTSEC-2026-0194",
+    "RUSTSEC-2026-0195",
+    # proc-macro-error2 2.0.1, unmaintained: a build-time proc macro of validator_derive 0.19,
+    # through deltalake-core. Nothing of it runs in the binary.
+    "RUSTSEC-2026-0173",
+]
```

`Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 6aa60dc..266a9ee 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -309,7 +309,7 @@ dependencies = [
  "arrow-select",
  "flatbuffers",
  "lz4_flex",
- "zstd 0.13.3",
+ "zstd 0.13.4",
 ]
 
 [[package]]
@@ -1406,8 +1406,8 @@ dependencies = [
  "flate2",
  "liblzma",
  "memchr",
- "zstd 0.14.0",
- "zstd-safe 8.0.0",
+ "zstd 0.14.3",
+ "zstd-safe 8.2.0",
 ]
 
 [[package]]
@@ -1627,9 +1627,9 @@ checksum = "7a949c44fcacbbbb7ada007dc7acb34603dd97cd47de5d054f2b6493ecebb483"
 
 [[package]]
 name = "ctutils"
-version = "0.4.2"
+version = "0.4.8"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "7d5515a3834141de9eafb9717ad39eea8247b5674e6066c404e8c4b365d2a29e"
+checksum = "261af4dc7a06ea8a527a8717d8cbba3417530d6ea14e1edb3de6b67806be474f"
 dependencies = [
  "cmov",
 ]
@@ -1947,7 +1947,7 @@ dependencies = [
  "tokio-postgres",
  "tokio-postgres-rustls",
  "uuid",
- "webpki-roots 1.0.9",
+ "webpki-roots 1.0.11",
 ]
 
 [[package]]
@@ -2077,7 +2077,7 @@ dependencies = [
  "tokio",
  "url",
  "uuid",
- "zstd 0.13.3",
+ "zstd 0.13.4",
 ]
 
 [[package]]
@@ -2201,7 +2201,7 @@ dependencies = [
  "tokio",
  "tokio-util",
  "url",
- "zstd 0.13.3",
+ "zstd 0.13.4",
 ]
 
 [[package]]
@@ -4163,7 +4163,7 @@ dependencies = [
  "tokio-util",
  "twox-hash",
  "url",
- "webpki-roots 1.0.9",
+ "webpki-roots 1.0.11",
 ]
 
 [[package]]
@@ -4192,7 +4192,7 @@ dependencies = [
  "sha2 0.10.9",
  "thiserror 2.0.21",
  "uuid",
- "zstd 0.13.3",
+ "zstd 0.13.4",
 ]
 
 [[package]]
@@ -4572,7 +4572,7 @@ dependencies = [
  "snap",
  "tokio",
  "twox-hash",
- "zstd 0.13.3",
+ "zstd 0.13.4",
 ]
 
 [[package]]
@@ -5341,9 +5341,9 @@ dependencies = [
 
 [[package]]
 name = "rustls"
-version = "0.23.45"
+version = "0.23.46"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "0d41d731c7d2f962d1ccc364cec258de3c0e93b38c2fb3ba97ac74513048d634"
+checksum = "48e13bd8c0e9365c43cfa5c9e8f9ad49d3c8444926c9aac819e0e4dc503c8fdf"
 dependencies = [
  "aws-lc-rs",
  "log",
@@ -5406,9 +5406,9 @@ checksum = "eec689c0bc40ff2458a5977b6619cb718087084a18e02a131c599b62d05e1a5f"
 
 [[package]]
 name = "rustls-webpki"
-version = "0.103.15"
+version = "0.103.16"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "f3c3cf1d8b1e7d4927e2d154c3fcb02979afb9939629c62cd9048d4f07b60ac2"
+checksum = "4443b1b6d39d7c663139942cc4558262562d2c84123bb6282ef177009ca8b281"
 dependencies = [
  "aws-lc-rs",
  "ring",
@@ -5911,7 +5911,7 @@ source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "32497e9a4c7b38532efcdebeef879707aa9f794296a4f0244f6f69e9bc8574bd"
 dependencies = [
  "fastrand",
- "getrandom 0.4.3",
+ "getrandom 0.3.4",
  "once_cell",
  "rustix",
  "windows-sys 0.61.2",
@@ -6053,9 +6053,9 @@ dependencies = [
 
 [[package]]
 name = "tokio"
-version = "1.53.1"
+version = "1.54.1"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "202caea871b69668250d242070849eb495be178ed697a3e98aebce5bc81a0bed"
+checksum = "b52fd3255258d26fac7264ab5a76b53c13367136dc011d3d6ad4f8ed070396a3"
 dependencies = [
  "bytes",
  "libc",
@@ -6694,9 +6694,9 @@ dependencies = [
 
 [[package]]
 name = "webpki-root-certs"
-version = "1.0.9"
+version = "1.0.11"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "b96554aa2acc8ccdb7e1c9a58a7a68dd5d13bccc69cd124cb09406db612a1c9b"
+checksum = "e49d0e65abc00c142e8f9428da991da9f1a250c39be97459901eb12b9b49f62a"
 dependencies = [
  "rustls-pki-types",
 ]
@@ -6707,14 +6707,14 @@ version = "0.26.11"
 source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "521bc38abb08001b01866da9f51eb7c5d647a19260e00054a8c7fd5f9e57f7a9"
 dependencies = [
- "webpki-roots 1.0.9",
+ "webpki-roots 1.0.11",
 ]
 
 [[package]]
 name = "webpki-roots"
-version = "1.0.9"
+version = "1.0.11"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "7dcd9d09a39985f5344844e66b0c530a33843579125f23e21e9f0f220850f22a"
+checksum = "f5f280bbb2b287bc8e7de7e3852945a0ff50d1bbcb5ed42b6c7a568c546df171"
 dependencies = [
  "rustls-pki-types",
 ]
@@ -7071,9 +7071,9 @@ dependencies = [
 
 [[package]]
 name = "yoke-derive"
-version = "0.8.3"
+version = "0.8.4"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "33811428bee40dbceb6d545e95754741d17a6aef9a4849f0fd62e2ba4f412a78"
+checksum = "ec8ebde2db3681e8c9980cc27822030e68752690ddfa9473e739aeb4dbde6d71"
 dependencies = [
  "proc-macro2",
  "quote",
@@ -7195,45 +7195,45 @@ checksum = "29666d0abbfad1e3dc4dcf6144730dd3a3ab225bbbdac83319345b1b44ccfc1b"
 
 [[package]]
 name = "zstd"
-version = "0.13.3"
+version = "0.13.4"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "e91ee311a569c327171651566e07972200e76fcfe2242a4fa446149a3881c08a"
+checksum = "cdb02c3132d251ab1606e9fc913a7da6c6ed08cc263df15edf082e1dc497d157"
 dependencies = [
- "zstd-safe 7.3.0",
+ "zstd-safe 7.4.0",
 ]
 
 [[package]]
 name = "zstd"
-version = "0.14.0"
+version = "0.14.3"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "bf06bd8162af0734b344780deb55b42a2429ae430870d13fcc12f238e880fe6e"
+checksum = "db3ae9bf90af6e40197bef767eeefc46ad6397420fe873830bd85e5fbbfb3c70"
 dependencies = [
- "zstd-safe 8.0.0",
+ "zstd-safe 8.2.0",
 ]
 
 [[package]]
 name = "zstd-safe"
-version = "7.3.0"
+version = "7.4.0"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "64d80649ab6db9d9f6f9c80a40becd948eda4714a0a5ac8c4d157a32231c7882"
+checksum = "1c8ea04668f5de8d8d51051714ba560c9536519cadcdb8829bba4fe2ad159b03"
 dependencies = [
  "zstd-sys",
 ]
 
 [[package]]
 name = "zstd-safe"
-version = "8.0.0"
+version = "8.2.0"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "ae42c0555055784c70058d19ba8e275528e8a99a706684868ace5da4e716a4ab"
+checksum = "ded520f16b29e1a9b771c722221b2b0a0398c2316d9bb3c42b458a5414d7434b"
 dependencies = [
  "zstd-sys",
 ]
 
 [[package]]
 name = "zstd-sys"
-version = "2.1.0+zstd.1.5.7"
+version = "2.1.2+zstd.1.5.7"
 source = "registry+https://github.com/rust-lang/crates.io-index"
-checksum = "0ef0a8027ec3ee71300ab3bcbcd0393f434aa72b91ca6d635a39941deae8eea0"
+checksum = "481ada9c543b2323cbb7bbdbb5249177fbf0188ed959ed2c15e4b22421c84ed3"
 dependencies = [
  "cc",
  "pkg-config",
```

- [ ] **Step 2: Run them to see them pass**

Run:

```bash
cargo audit
cargo build --workspace --all-targets --locked
cargo test --workspace
```

Expected: `cargo audit` reports no vulnerability or warning (the three ignored advisories are
listed as ignored); the build and the tests pass.

- [ ] **Step 3: Commit**

```bash
git add .cargo/audit.toml \
  Cargo.lock
git commit -m "build: cargo audit — the lockfile on the patched releases, the three remaining advisories triaged"
```

---

### Task 15: The compile time, measured

**Files:**
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: the workspace's dev profile (`[profile.dev.package."*"] debug = false`, M6a).
- Produces: `[profile.dev] debug = "line-tables-only"` in the workspace `Cargo.toml`.

The roadmap asks for the compile time and the dependency tree to be checked against a
`cargo build --timings` measurement before the release (Q-M5-24), with three levers to weigh.
They were measured on the planning machine (12 cores, macOS), each from a clean target directory,
on this plan's last stage:

| Profile | Clean build, `--workspace --all-targets` | After touching `core` / `pipeline` | Target dir | `sink-delta` + `pipeline` tests |
|---|---|---|---|---|
| as on the base (full debug info for the workspace crates) | 588 s | 37 s / 40 s | 10 GB | 60 s |
| `[profile.dev] debug = "line-tables-only"` | 576 s | 40 s / 39 s | 9.0 GB | — |
| dependencies at `opt-level = 1` | 1028 s | 29 s / — | 11 GB | 42 s |

What dominates a clean build (`--timings`, unit-seconds of 3046 over 700 units):
`aws-lc-sys`'s build script 110 s (a C build, through rustls and `deltalake-aws`'s `aws-config`),
`datafusion` 89 s, `deltalake-core` 74 s, `datafusion-catalog` 62 s, `sqlparser` 57 s, `parquet`
57 s; the workspace's own units 925 s. Linking isn't a unit of its own; a test binary's link is
inside its unit.

**Taken:** line tables for the workspace crates. The build times don't move, the target directory
is 1 GB smaller (CI's `integration` job already frees the runner's disk to fit its builds), and a
backtrace keeps its file and line. **Not taken:** `opt-level = 1` for the dependencies. The
heaviest suites run 18 s faster, but a clean build takes 440 s longer, paid at every dependency
bump (Task 14's alone) and every CI cache miss (D-M12b.19).

The rest of the roadmap's list:
- **CI:** not changed. On M12a's PR (run 41822813507) the path is `check` (21 min) then `k3s` (33
  min, 23 of them the image's release build in Docker, which a shared cargo cache doesn't reach);
  `integration` (30 min) runs beside it. Sharing `check`'s build with `integration` would shorten
  a job off the critical path. Revisit in M12c, when the real-cloud jobs join.
- **Upstream** (crates.io, 2026-11-04): `deltalake` 1.1.2 and `deltalake-core` 1.1.3 still take
  DataFusion's default features and require `object_store ^0.13.2`; `deltalake-aws` 1.0.0 (the
  newest) requires `aws-config` and `aws-sdk-sts` unconditionally. Nothing to take yet; the pin
  stays `=1.0.0` (roadmap R-13).
- **The M6a dependency rule** (§2) still holds: of the workspace's crates, only `sink-delta` and
  the binary link deltalake (`cargo tree -i deltalake-core -e normal --workspace`).

- [ ] **Step 1: The profile**

`Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index e5b75e2..13f62b8 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -108,6 +108,10 @@ foca = { version = "=2.0.0", default-features = false, features = ["std", "serde
 rand = { version = "0.10", default-features = false, features = ["std", "std_rng", "sys_rng"] }
 
 # Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
-# CI runners. Workspace crates keep it.
+# CI runners. Workspace crates keep line tables only, for backtraces: full debug info costs about
+# 1 GB of target directory and no build time (measured in M12b, Task 15).
 [profile.dev.package."*"]
 debug = false
+
+[profile.dev]
+debug = "line-tables-only"
```
- [ ] **Step 2: Check the gates under it**

Run:

```bash
cargo build --workspace --all-targets
cargo test --workspace
```

Expected: PASS, the same counts as Task 14's tree; `target/debug` about 1 GB smaller than before
the change, on a clean build.

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "build: the workspace crates' dev profile keeps line tables only (M12b's compile-time measurement)"
```

---

### Task 16: The docs, the research and the tracker

**Files:**
- Modify: `doc/analysis/CDC_Design_Gap_Analysis_v1.md`, `doc/research/API_RBAC_and_Versioning.md`, `doc/research/Clustering_Architecture_Research.md`, `docs/src/content/docs/architecture/delivery.md`, `docs/src/content/docs/architecture/snapshots.md`, `docs/src/content/docs/configuration/cluster.md`, `docs/src/content/docs/configuration/overview.md`, `docs/src/content/docs/deployment/kubernetes.md`, `docs/src/content/docs/deployment/observability.md`, `docs/src/content/docs/runbooks/config-changes.md`, `docs/src/content/docs/runbooks/failover.md`, `docs/src/content/docs/runbooks/halted-table.md`, `docs/src/content/docs/runbooks/position-lost.mdx`, `docs/src/content/docs/runbooks/snapshot-jobs.md`

**Gaps:** DA-SNAP-7, DA-X-13 (closed with pointers), and the pointers for every gap Tasks 1–13
closed.

**Interfaces:**
- Consumes: what Tasks 1–15 built, by the names their Interfaces give.
- Produces: the operator pages, the Clustering and API research's "as built" notes, and the
  tracker's §57.

The operator pages say what changed, where an operator meets it:
- `runbooks/config-changes.md`: removing a pipeline (the tombstone, the sweep, the wait), keys
  applied at the next start (`restart=accept`, `restart_pending`, the fields kept for good), the
  validate limit;
- `runbooks/snapshot-jobs.md`: `truncate`, the retention, a copy's halt, a lost restart asked
  again;
- `runbooks/failover.md`: `holder_stale` and `holder.suspect`; groups that merge on their own;
- `configuration/cluster.md`, `deployment/kubernetes.md`: discovery asks again every 5 s (the
  caution goes);
- `configuration/overview.md`: reusing an id or its resources is safe (the caution becomes a note);
- `deployment/observability.md`: the lag across sessions, the flush log line, the headers;
- `architecture/snapshots.md`, `architecture/delivery.md`, `runbooks/halted-table.md`,
  `runbooks/position-lost.mdx`: `truncate` in place of "drop or move the Delta table", the read
  pool, a snapshot and the source's log.

DA-SNAP-7 closes with a pointer: an incremental snapshot runs beside the stream, whose position
advances with each flush, so a slow snapshot holds back only what the stream hasn't flushed; the
WAL guard and `binlog_purge_action` cover the rest. DA-X-13 closes with a pointer: the extra
commit was never seen again from M7a on.

- [ ] **Step 1: The operator pages**

`docs/src/content/docs/architecture/delivery.md`:

```diff
diff --git a/docs/src/content/docs/architecture/delivery.md b/docs/src/content/docs/architecture/delivery.md
index 5df40c2..28e5040 100644
--- a/docs/src/content/docs/architecture/delivery.md
+++ b/docs/src/content/docs/architecture/delivery.md
@@ -46,4 +46,5 @@ rows it had, and the pipeline's other tables go on. It never skips the change an
 A snapshot row is written as an upsert too, and a change streamed while the snapshot runs wins over
 the snapshot's copy of the same row (see [Snapshots](/data-avalanche/architecture/snapshots/)). A
 snapshot never deletes: a row deleted at the source while its table wasn't being streamed stays in
-the lake, through a copy too. To remove such rows, drop or move the Delta table before copying it.
+the lake, through a copy too. To remove such rows, copy the table with a snapshot job asked with
+`"truncate": true`, which empties the Delta table first.
```
`docs/src/content/docs/architecture/snapshots.md`:

```diff
diff --git a/docs/src/content/docs/architecture/snapshots.md b/docs/src/content/docs/architecture/snapshots.md
index cee4209..f6235b8 100644
--- a/docs/src/content/docs/architecture/snapshots.md
+++ b/docs/src/content/docs/architecture/snapshots.md
@@ -41,7 +41,12 @@ flowchart TB
 - A row the stream changes while the snapshot runs converges: the streamed change wins over the
   chunk's older copy.
 - `parallelism` (1) tables are snapshotted at once; one table is read in order. On PostgreSQL each
-  table being snapshotted holds about three connections: more than 4 at once warns (V-038).
+  table being snapshotted holds about three connections: more than 4 at once warns (V-038). Each
+  chunk read runs on a connection of its own, so a read that waits (on a lock, say) holds up
+  neither the other tables' reads nor the stream's catalog queries.
+- A snapshot doesn't hold the source's log back: the stream goes on beside it and its position
+  advances with each flush, so the WAL or binlog a slow snapshot keeps is only what the stream
+  hasn't flushed yet.
 - `source.timeouts.snapshot` (6 h) bounds one table's snapshot in one run; past it the table halts
   `SNAPSHOT_TIMEOUT`. Raise it and start a job for the table.
 
@@ -56,7 +61,8 @@ pipeline's jobs. Jobs are kept in the pipeline's state and survive restarts and
   job's tables from their first row, whatever their mode.
 - A job's table that is halted has its halt cleared and its schema taken afresh from the catalog.
 - A table the config doesn't replicate is copied once, and not streamed.
-- A job **upserts**: it never deletes. A row deleted at the source while the table was halted, or
-  not replicated, stays in the Delta table. To start clean, drop or move the Delta table first.
+- A job **upserts**: a row deleted at the source while the table was halted, or not replicated,
+  stays in the Delta table. Ask the job with `"truncate": true` to empty each of its Delta tables
+  first; until the job completes, they are empty or partly copied.
 
 [Snapshot jobs](/data-avalanche/runbooks/snapshot-jobs/) is the runbook.
```
`docs/src/content/docs/configuration/cluster.md`:

```diff
diff --git a/docs/src/content/docs/configuration/cluster.md b/docs/src/content/docs/configuration/cluster.md
index a429cc2..3bd0fe3 100644
--- a/docs/src/content/docs/configuration/cluster.md
+++ b/docs/src/content/docs/configuration/cluster.md
@@ -63,14 +63,8 @@ pipelines: {}
 - `discovery: dns` resolves `discovery_dns_name`, a Kubernetes headless Service, and joins every
   address it returns. The [Helm chart](/data-avalanche/deployment/kubernetes/) sets it up.
 
-:::caution[Pods started together with DNS discovery]
-A node asks DNS again only while it has no peer. When four or more pods start at once, nodes that
-each resolved some of the others can settle in separate groups that never merge: each group
-places every pipeline, the leases keep one writer per pipeline, but the groups contend for them.
-After a cold start, check that `GET /v1/cluster/status` on any node lists every pod. If it
-doesn't, restart the pods of the smaller group one at a time: a starting node resolves the name
-again.
-:::
+Either way, a node resolves its seeds again every 5 s and asks the ones that aren't members yet,
+so pods started together that each found only some of the others merge into one cluster.
 
 ## Settings fixed for the cluster's life
 
```
`docs/src/content/docs/configuration/overview.md`:

```diff
diff --git a/docs/src/content/docs/configuration/overview.md b/docs/src/content/docs/configuration/overview.md
index 5c54373..05f1791 100644
--- a/docs/src/content/docs/configuration/overview.md
+++ b/docs/src/content/docs/configuration/overview.md
@@ -71,11 +71,11 @@ table prefix (`sink.prefix`) and the default MySQL `server_id` derive from it. C
 deletes the pipeline and creates a new one: a new state, and a new snapshot per its
 `snapshots.mode`. Lower-case letters, digits, `_` and `-`, at most 63 characters (V-055).
 
-:::caution[Reusing an id or its resources]
+:::note[Reusing an id or its resources]
 When a pipeline is removed and another one takes its `id`, slot, publication, sink prefix or state
-location, start the new one only after the old one's lease is released (watch
-`GET /v1/cluster/status`), or in the same reload as the removal. A node that starts or joins in
-between, or a second reload, can start the new pipeline while the old one still drains.
+location, the new one waits until the old one is torn down, on whichever node it starts, whatever
+reload or start brought it (see
+[Remove a pipeline](/data-avalanche/runbooks/config-changes/#remove-a-pipeline)).
 :::
 
 ## How defaults merge
@@ -138,7 +138,7 @@ of the [reference](/data-avalanche/reference/configuration/) has a reload class:
 | applied | in place, at once |
 | pipeline restart | that pipeline stops, drops what it hasn't committed, and starts again from its last checkpoint with the new value; the replay is idempotent, and the other pipelines don't notice |
 | pipeline restart, if the table set changes | a restart only when the tables the patterns match change |
-| process restart | the reload is refused, naming the key (V-061); see [Config changes](/data-avalanche/runbooks/config-changes/#keys-a-reload-refuses) |
+| process restart | the reload is refused, naming the key (V-061), unless asked with `?restart=accept`: then each node applies it at its next start; see [Config changes](/data-avalanche/runbooks/config-changes/#keys-applied-at-the-next-start) |
 
 A node that restarts takes the config the cluster runs, not its own file: see
 [Config changes](/data-avalanche/runbooks/config-changes/).
```
`docs/src/content/docs/deployment/kubernetes.md`:

```diff
diff --git a/docs/src/content/docs/deployment/kubernetes.md b/docs/src/content/docs/deployment/kubernetes.md
index 1d40e62..061de92 100644
--- a/docs/src/content/docs/deployment/kubernetes.md
+++ b/docs/src/content/docs/deployment/kubernetes.md
@@ -93,8 +93,8 @@ rolls. It doesn't wait for them: lower it to roll faster, at the cost of moving
 twice.
 
 The pods start in parallel (`podManagementPolicy: Parallel`), since a pod's name, not its start
-order, places it. With four or more pods started at once, check after a cold start that every node
-sees every other: see [the caution on DNS discovery](/data-avalanche/configuration/cluster/#discovery).
+order, places it. Pods that started together and found only some of the others merge within
+seconds: each one resolves the Service again every 5 s ([discovery](/data-avalanche/configuration/cluster/#discovery)).
 
 ## TLS for the API
 
```
`docs/src/content/docs/deployment/observability.md`:

````diff
diff --git a/docs/src/content/docs/deployment/observability.md b/docs/src/content/docs/deployment/observability.md
index e8c29d0..45b4e0a 100644
--- a/docs/src/content/docs/deployment/observability.md
+++ b/docs/src/content/docs/deployment/observability.md
@@ -26,7 +26,9 @@ to the snapshot gauges.
 The ones to watch first:
 
 - **`data_avalanche_replication_lag_seconds`**: the age of the oldest change read but not yet
-  committed; `0` when nothing waits. v1's target is a p99 under 60 s at steady state, and back
+  committed; `0` when nothing waits. After a failover, a restart or a source reconnect, it keeps
+  counting from the oldest change the earlier session left uncommitted until the new one has read
+  that far again. v1's target is a p99 under 60 s at steady state, and back
   under it within 2 minutes of a node failure. A snapshot, a transaction larger than the flush
   bounds, and heavy random-key MERGEs on large tables are outside the target.
 - **`data_avalanche_wal_lag_bytes`**: the WAL a PostgreSQL slot holds on the server's disk.
@@ -59,4 +61,21 @@ on reload, and `--log-level` / `--log-format` override them. A log line never ca
 With `globals.tracing.otlp_endpoint` set, a node sends spans over OTLP/HTTP (protobuf) to
 `<endpoint>/v1/traces`: one per flush and its commits, per snapshot chunk, per schema change
 applied, never one per row. `sample_ratio` (0.05 with an endpoint) samples them. A JSON log line
-written inside a sampled span carries its `trace_id` and `span_id`.
+written inside a sampled span carries its `trace_id` and `span_id`, so a flush's log lines (at
+`debug` for `data_avalanche_pipeline::writer`) lead to its trace.
+
+A hosted collector that needs a token takes it in `globals.tracing.headers`, sent with each
+export. The values are secrets: they never reach the log or the API's answers. Write them as
+`${VAR}`, so that the token stays out of the config text:
+
+```yaml
+globals:
+  tracing:
+    otlp_endpoint: https://otlp.example.com
+    headers:
+      authorization: "Bearer ${OTLP_TOKEN}"
+```
+
+`OTEL_EXPORTER_OTLP_HEADERS` in a node's environment (`key=value,…`) is sent too, and wins over a
+configured header of the same name. Use an `https` endpoint: a token sent over `http` can be read
+on the way.
````
`docs/src/content/docs/runbooks/config-changes.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/config-changes.md b/docs/src/content/docs/runbooks/config-changes.md
index 1b9db21..a91af51 100644
--- a/docs/src/content/docs/runbooks/config-changes.md
+++ b/docs/src/content/docs/runbooks/config-changes.md
@@ -1,6 +1,6 @@
 ---
 title: Config changes
-description: Validating and reloading a config, why a restart doesn't apply one, rotating secrets, and the keys a reload refuses.
+description: Validating and reloading a config, why a restart doesn't apply one, removing a pipeline, rotating secrets, and the keys a reload refuses or applies at the next start.
 sidebar:
   order: 7
 ---
@@ -35,7 +35,9 @@ without a restart, so a pipeline in `error` stays there: once the new generation
 `POST /v1/pipelines/<id>/restart` it.
 
 Both calls need the `operator` role. Any node will do, but it publishes **its own file**: send the
-reload to a node whose file is the one you changed.
+reload to a node whose file is the one you changed. A node checks at most 5 posted configs at once,
+then one a second: past that, the validate answers `429` with `Retry-After`. Checking the node's
+own file (no body) has no limit.
 
 ## A restart doesn't apply a config
 
@@ -52,6 +54,20 @@ literal that differs from the generation is ignored, with a warning naming it; `
 `api.address` are the node's file's. A reload from a node is refused until its file matches the
 generation on these keys.
 
+## Remove a pipeline
+
+Delete it from the file and reload. The node that adopts the generation first writes a removal
+record in `globals.state` (`removals/<id>.json`); the pipeline's owner then drains it, drops its
+slot and publication as `auto_cleanup` says, and marks the record done. The sink's tables stay.
+
+The record is what makes a removal survive a node that is down: if no node holds the pipeline
+when the generation lands (its owner was stopped, or the lease was between two nodes), the node
+the ring places the id on finds the pending record within 30 s, takes the lease once it is free,
+and tears the pipeline down itself. A pipeline added back with the same id, or a new one on the
+removed one's slot, publication, sink or state location, waits for the record to be done before
+it starts, whichever node it lands on. A record that can't be read or resolved is waited for, and
+the node logs why.
+
 ## Rotate a secret
 
 - **A credentials file** (`credentials_file`): replace its contents on every node (a mounted
@@ -62,32 +78,34 @@ generation on these keys.
 - **The API's TLS certificate** (`globals.api.tls.cert` and `key`): replace the files or the
   variables, then restart the nodes one at a time. A reload meanwhile is refused, naming the key.
 
-## Keys a reload refuses
+## Keys applied at the next start
 
 A reload refuses a change to a key whose class is *process restart*
-([the reference](/data-avalanche/reference/configuration/) marks them), naming it (V-061). Because
-a restarted node adopts the cluster's generation, restarting doesn't apply such a change either:
+([the reference](/data-avalanche/reference/configuration/) marks them), naming it (V-061). Most
+of them (`lease_renewal_interval`, `lease_safety_margin`, `discovery`, `discovery_dns_name`,
+`join_members`, the gossip's ports and timers, `globals.api.tls`) can still be changed:
+
+1. Reload with `restart=accept`:
+
+   ```sh
+   curl -s -X POST 'https://cdc-0.example.internal/v1/config/reload?restart=accept'
+   ```
+
+   The generation is published. Every running node adopts it but keeps the values it started
+   with for those keys; everything else in the reload applies as usual.
+2. Restart the nodes one at a time. Each one takes the new values as it starts.
+3. `GET /v1/config/status` on each node lists, in `restart_pending`, the keys it still runs with
+   the old value. The rollout is done when every node's list is empty.
+
+Some keys are refused even with `restart=accept`, because the cluster keeps them for good:
 
 - `tokens`, `lock_backend` and `lease_duration` are pinned in the cluster's anchor and never
   change for that `globals.state`.
-- `cloud`, `globals.state` and `globals.azure` say where the cluster is: changing them makes a new
-  cluster.
-- The rest (`lease_renewal_interval`, `lease_safety_margin`, `discovery`, `discovery_dns_name`,
-  `join_members`, the gossip's ports and timers, `globals.api.tls`) change only through each
-  node's environment. Write the ones you may want to change as `${VAR}`, with a
-  default, from the start, and change the variable with a rolling restart:
-
-  ```yaml
-  globals:
-    cluster:
-      lease_renewal_interval: ${LEASE_RENEWAL_INTERVAL:-5s}
-  ```
-
-  A key already written as a literal can't be changed in a running cluster. The way out is a new
-  cluster: stop every node, then start them with a new `globals.state.prefix`, so that the file
-  bootstraps a new anchor and generation. Never run nodes on the old and the new location at
-  once: they would be two clusters holding the same pipelines. This path isn't covered by the
-  project's tests yet.
+- `config_version`, `cloud`, `globals.state` and `globals.azure` say what and where the cluster
+  is: changing them makes a new cluster. Stop every node, then start them with a new
+  `globals.state.prefix`, so that the file bootstraps a new anchor and generation. Never run
+  nodes on the old and the new location at once: they would be two clusters holding the same
+  pipelines. This path isn't covered by the project's tests yet.
 
 `node_name` and `globals.api.address` are read from each node's own file when it starts, so a
 restart applies a changed value; a reload is still refused while the generation holds another
````
`docs/src/content/docs/runbooks/failover.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/failover.md b/docs/src/content/docs/runbooks/failover.md
index b4cfdc3..add0c7c 100644
--- a/docs/src/content/docs/runbooks/failover.md
+++ b/docs/src/content/docs/runbooks/failover.md
@@ -13,7 +13,11 @@ curl -s https://cdc.example.internal/v1/cluster/status
 ```
 
 It lists the members this node sees, with their state, and each pipeline's lease holder. Ask more
-than one node: they should agree.
+than one node: they should agree. A holder marked `holder_stale` (in `GET /v1/pipelines/<id>`:
+`holder.stale`) isn't a member this node sees, or is an earlier process of a restarted node: its
+lease still reads as held until it expires and another node takes it over. `holder.suspect` means
+the gossip suspects the holder of having failed. Both come from this node's view of the
+membership, not from clocks.
 
 ## A node died
 
@@ -45,10 +49,10 @@ slower failover; it is fixed for the life of a cluster's `globals.state`.
 
 ## The nodes split into groups
 
-With `discovery: dns`, nodes started together can form groups that don't see each other: each
-group's `members` lists only its own, and the groups contend for the leases. Restart the pods of
-the smaller group one at a time. See
-[the caution on DNS discovery](/data-avalanche/configuration/cluster/#discovery).
+Nodes that formed groups that don't see each other (each group's `members` lists only its own)
+merge on their own: every node resolves its seeds again every 5 s and asks those that aren't
+members. Groups that stay apart for more than a few seconds can't reach each other: check the
+network between them, and that each node's `advertise_addr` is the address its peers reach.
 
 ## Taking a node out
 
````
`docs/src/content/docs/runbooks/halted-table.md`:

```diff
diff --git a/docs/src/content/docs/runbooks/halted-table.md b/docs/src/content/docs/runbooks/halted-table.md
index f89d3a4..3026ae0 100644
--- a/docs/src/content/docs/runbooks/halted-table.md
+++ b/docs/src/content/docs/runbooks/halted-table.md
@@ -55,8 +55,9 @@ streams it again. Follow it with `GET /v1/snapshots/<job_id>` (see
 [Snapshot jobs](/data-avalanche/runbooks/snapshot-jobs/)).
 
 :::caution[Rows deleted while the table was halted]
-A job upserts every row it reads and deletes nothing. A row deleted at the source while the table
-was halted stays in the Delta table. To start clean, drop or move the Delta table before the job.
+A job upserts every row it reads. A row deleted at the source while the table was halted stays in
+the Delta table, unless the job is asked with `"truncate": true`: it empties the Delta table before
+its copy, so readers see it empty, then partly copied, until the job completes.
 :::
 
 :::note[A table created again while the pipeline was down]
```
`docs/src/content/docs/runbooks/position-lost.mdx`:

```diff
diff --git a/docs/src/content/docs/runbooks/position-lost.mdx b/docs/src/content/docs/runbooks/position-lost.mdx
index 8f3a4c2..588e108 100644
--- a/docs/src/content/docs/runbooks/position-lost.mdx
+++ b/docs/src/content/docs/runbooks/position-lost.mdx
@@ -19,8 +19,8 @@ the changes before now are missing, and you copy the tables yourself with a
 
 :::caution[A fresh copy keeps rows deleted in the gap]
 The copy upserts every row it reads and deletes nothing: a row deleted at the source during the
-gap stays in the Delta table. When the tables must be exact, stop the pipeline, drop or move its
-Delta tables, then copy them.
+gap stays in the Delta table. When the tables must be exact, copy them with a snapshot job asked
+with `"truncate": true`, which empties each Delta table before its copy.
 :::
 
 <Tabs>
@@ -81,8 +81,8 @@ as at a restart, and recovers as in step 2.
    you start a snapshot job for each table.
 3. Rows the old server wrote that the promoted one never received (the changes written after the
    replica fell behind or was cut off) stay in the Delta tables. The copy only upserts, so it
-   doesn't delete them; they are the changes the failover lost. To start clean, stop the pipeline,
-   drop or move the Delta tables, then let it copy them afresh, as in the caution above and in
+   doesn't delete them; they are the changes the failover lost. To start clean, copy the tables
+   with a snapshot job asked with `"truncate": true`, as in the caution above and in
    [A halted table](/data-avalanche/runbooks/halted-table/#3-copy-the-table-again).
 
 Leave `source_change_action` at `re_snapshot` only where a failover should recover by itself.
```
`docs/src/content/docs/runbooks/snapshot-jobs.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/snapshot-jobs.md b/docs/src/content/docs/runbooks/snapshot-jobs.md
index 51939d7..b57c8cb 100644
--- a/docs/src/content/docs/runbooks/snapshot-jobs.md
+++ b/docs/src/content/docs/runbooks/snapshot-jobs.md
@@ -21,12 +21,19 @@ curl -s -X POST https://cdc.example.internal/v1/pipelines/shop/snapshots \
   bare name on MySQL. No patterns.
 - `202` answers with the job's id, `<pipeline id>~<suffix>`.
 - `409`: the pipeline isn't running (paused, stopped), no node holds its lease, or an unfinished
-  job already holds one of the tables.
+  job already holds one of the tables. When that job is still queued, the call also asks the
+  pipeline's restart for it again, in case the first ask was lost: the job starts at the owner's
+  next poll.
 - `422`: no table, a pattern, or a name not in the source's form.
 
 The call needs the `operator` role. Any node accepts it; the pipeline's owner restarts the
 pipeline to take the job, which replays from the last checkpoint and is harmless.
 
+To make the lake an exact copy, add `"truncate": true`: each of the job's tables is emptied in the
+lake before its copy starts, so rows deleted at the source while the table wasn't streamed go.
+Until the job completes, those tables are empty or partly copied: readers see that. A job
+interrupted by a crash after the truncate goes on copying; it doesn't empty the table again.
+
 ## Follow it
 
 ```sh
@@ -35,19 +42,22 @@ curl -s https://cdc.example.internal/v1/pipelines/shop/snapshots
 ```
 
 A job's status, and each of its tables', is kept in the pipeline's state: it survives restarts and
-failover, and a job interrupted by a crash goes on from its last chunk.
+failover, and a job interrupted by a crash goes on from its last chunk. A pipeline keeps its 100
+newest ended jobs (completed, failed or cancelled); older ones are deleted as each job ends. A
+job document the node can't read is left out of the list, with a warning in the log.
 
 ## What a job does
 
 - Copies each table from its first row, whatever the table's `snapshots.mode`.
 - For a halted table: clears the halt, takes the schema from the source's catalog, and streams the
   table again once it's copied.
-- For a table the config doesn't replicate: copies it once, and doesn't stream it.
+- For a table the config doesn't replicate: copies it once, and doesn't stream it. If that copy
+  fails, the job fails with the reason; the table isn't counted as a halted table of the pipeline.
 
 ## What it doesn't do
 
-- **Delete.** A job upserts. Rows deleted at the source while the table wasn't streamed stay in
-  the lake: drop or move the Delta table first when you need an exact copy.
+- **Delete**, unless asked to. A job upserts. Rows deleted at the source while the table wasn't
+  streamed stay in the lake, unless the job is asked with `truncate`.
 - **Stop the stream.** The pipeline keeps streaming every table; a change to a row the job is
   copying wins over the job's copy.
 
````

- [ ] **Step 2: The research**

`doc/research/API_RBAC_and_Versioning.md`:

```diff
diff --git a/doc/research/API_RBAC_and_Versioning.md b/doc/research/API_RBAC_and_Versioning.md
index 516a165..0d743b5 100644
--- a/doc/research/API_RBAC_and_Versioning.md
+++ b/doc/research/API_RBAC_and_Versioning.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-10-27
+updated: 2026-11-04
 domain: Auth
 tags: [rbac, api, authorization, roles, jwt, oauth2, versioning, management-api, cdc]
 resolves: [G-16, G-9.1]
@@ -191,3 +191,5 @@ min-role column; the new lifecycle endpoints (G-NEW-15) inherit operator from th
 | D-16.3 binding | none = open; basic = admin; jwt/oauth2 = mapped claim roles, max wins; 401 vs 403 distinguished |
 | D-16.4 IdP mapping | configurable `role_mapping`, default identity on the three names |
 | D-16.5 TLS rule | V-052 stays a **warning** (upstream TLS termination is common in k8s) |
+| D-16.6 key fetch (M12b, DA-API-16) | A JWKS or discovery fetch follows no redirect (a `3xx` is refused) and reads at most 1 MiB; a non-`https` key URL warns (V-125) |
+| D-16.7 limits (M12b, DA-API-17, DA-API-20) | Constants, not config: at most 1024 open connections a node (past it, the accept waits for one to close), one idle for 60 s closed; a posted `validate` body takes a token from a node-wide bucket of 5, refilled at 1 a second, else `429` with `Retry-After` |
```
`doc/research/Clustering_Architecture_Research.md`:

```diff
diff --git a/doc/research/Clustering_Architecture_Research.md b/doc/research/Clustering_Architecture_Research.md
index f05d046..193f874 100644
--- a/doc/research/Clustering_Architecture_Research.md
+++ b/doc/research/Clustering_Architecture_Research.md
@@ -1,7 +1,7 @@
 ---
 title: Clustering Architecture Research
 status: Research / Design proposal (input for further elaboration)
-updated: 2026-10-22
+updated: 2026-11-04
 domain: Architecture / Distributed Systems
 tags: [clustering, gossip, swim, memberlist, consistent-hashing, lease, split-brain, rebalancing, config-propagation]
 closes_or_advances: [G-8, G-9, G-2, G-17, G-23, G-36, "OQ-CL1", "OQ-CL3", "OQ-CL4", "OQ-API3", "OQ-P3", "5.1", "5.2"]
@@ -755,6 +755,38 @@ generation, keeps running its own, and tries again at each poll, and the status
 not converged. A node whose own file differs from the generation in a per-node key is warned at
 startup (DA-OPS-8), since a reload from it would be refused.
 
+**Rolling a PROC change (M12b, DA-OPS-12).** Rejecting every PROC change made a key written as a
+literal impossible to change: a restarted node adopts the stored generation, not its file. A reload
+asked with `?restart=accept` now publishes it: each running node adopts the generation, keeps the
+PROC values it started with (it lists the fields in `restart_pending`, `GET /v1/config/status`, per
+node), and a node takes them at its next start, so a rolling restart applies the change. The
+fields the cluster keeps for good (`config_version`, `globals.cloud`, `globals.state`,
+`globals.azure`, and the anchor's `tokens`, `lease_duration`, `lock_backend`) are refused even
+then (config spec §1.2, V-061): a new value is a new cluster.
+
+### 9.5 Removal tombstones (M12b, DA-OPS-9, DA-OPS-10)
+A removed pipeline used to be torn down only by a controller that ran it when its node adopted the
+generation. A removal that no running controller saw (its owner was down, or the lease was between
+release and claim) left its slot and publication behind, and a pipeline added back on the same
+resources raced the teardown. Now:
+
+- The node that adopts a generation which drops ids first writes `removals/<id>.json` in
+  `globals.state` for each (the id, the last generation that had it, when), a create-if-absent
+  write; a tombstone that can't be written refuses the adoption, which is tried again at the next
+  poll. Then it applies the removal as before.
+- The controller that tears the pipeline down (drain, `auto_cleanup`, release) marks its tombstone
+  done (`done_at`).
+- Every 30 s each node reads the pending tombstones. For one whose id the ring places on it and no
+  live controller holds, it resolves the pipeline from the tombstone's generation, claims the lease
+  once it is free, tears the pipeline down, and marks the tombstone done.
+- A controller, before its first claim, waits until every pending tombstone of its own id, or of a
+  pipeline it `shares` resources with (slot, publication, sink, state location), is done. A
+  tombstone that can't be read is waited for; one whose generation doesn't resolve is logged and
+  not waited for, since nothing could tear it down on this node either.
+
+Tombstones are marked done, not deleted: their number is bounded by the distinct ids ever removed,
+and they record what was removed and when.
+
 ---
 
 ## 10. Single-node mode and seamless scale-out (requirement 6, closes OQ-CL4 / 4.4)
@@ -791,6 +823,12 @@ seed list:
 > periodically**, not just at boot, so a node that has lost all peers can re-bootstrap; once gossip is
 > joined, SWIM carries membership and DNS is only the bootstrap/rejoin path. These fields are already
 > reserved in config spec §16 (G-9 row).
+>
+> **As built (M12b, DA-CLUS-15):** asking the seeds only while a node had no peer let pods started
+> together settle in groups that never merged (each group had a peer). Every node, `static` or
+> `dns`, now resolves its seeds every 5 s (`REDISCOVER`) and announces to the addresses that aren't
+> members of its view; an address that is a member costs nothing. SWIM still carries the
+> membership; the seeds only join groups.
 
 ### 10.3 The N=1 ↔ N>1 symmetry
 The architecture is *identical* at every cluster size; only the membership set differs. That symmetry
```

- [ ] **Step 3: The tracker**

§57 closes each gap with its pointer, every domain row is closed in place, and the dashboard is
recounted: 10 open (M12d's and M12c's), none partial.

`doc/analysis/CDC_Design_Gap_Analysis_v1.md`:

```diff
diff --git a/doc/analysis/CDC_Design_Gap_Analysis_v1.md b/doc/analysis/CDC_Design_Gap_Analysis_v1.md
index 3956465..b3b1b49 100644
--- a/doc/analysis/CDC_Design_Gap_Analysis_v1.md
+++ b/doc/analysis/CDC_Design_Gap_Analysis_v1.md
@@ -652,6 +652,39 @@ The planning measured:
 Not taken: DA-SRC-25, 31…36, DA-SNAP-22 and DA-SINK-12 (M12d, Q-M12-8); DA-OPS-14 (M12c's load
 runs). No count changes.
 
+## 57. M12b control hardening — the gaps closed (2026-11-04)
+
+M12b's tasks (`doc/v1.0/M12b-control-hardening.md`) closed the gaps §56 named. The operator pages
+say what changed: `runbooks/config-changes.md` (removing a pipeline, keys applied at the next
+start, the validate limit), `runbooks/snapshot-jobs.md` (`truncate`, retention, a copy's halt, a
+lost restart), `runbooks/failover.md` (`holder_stale`, groups that merge), `configuration/cluster.md`
+and `deployment/kubernetes.md` (discovery), `deployment/observability.md` (the lag, the headers,
+the flush log) and `architecture/snapshots.md` (the read pool, the log's retention, `truncate`).
+
+| ID | Change | Evidence |
+|---|---|---|
+| DA-CLUS-15 | 🟥 → ✅ | `discovery.rs` `discover`: every node resolves its seeds each `REDISCOVER` (5 s) and announces to those that aren't members; `groups_that_each_have_a_peer_ask_their_seeds_again_and_merge`. The docs' caution is gone (`configuration/cluster.md`, `runbooks/failover.md`) |
+| DA-API-13 | 🟥 → ✅ | `pipelines.rs` `liveness`: `holder.stale` (its node and incarnation not a member of the answering node's gossip) and `holder.suspect`, `holder_stale` in the lists; `Member.incarnation`; `a_holder_that_isnt_a_member_reads_as_stale`, `a_suspected_member_holds_its_lease_as_suspect` |
+| DA-OPS-9 | 🟥 → ✅ | `state` `removal.rs` (`removals/<id>.json`), recorded by the coordinator before it adopts; the reconciler's sweep (`SWEEP_INTERVAL`) and the controller's `remove_orphan`; config §12.3; `a_removal_no_node_removed_is_torn_down_where_the_ring_places_it`, `a_removal_held_by_a_gone_node_is_torn_down_once_its_lease_expires` |
+| DA-OPS-10 | 🔄 → ✅ | the controller's `follow()` waits, before its first claim, for every pending tombstone of its id or of a pipeline it shares resources with, on every node; `a_pipeline_added_after_the_removal_it_follows_waits_for_it`, `a_node_started_during_a_removal_waits_for_it` |
+| DA-OPS-12 | 🟥 → ✅ | `POST /v1/config/reload?restart=accept` (Q-M12b-2): published, each node's `restart_pending` until its next start; `FIXED_FIELDS` refused even then (config §1.2, V-061); `a_restart_field_is_published_when_accepted_and_pending_until_the_next_start`; `runbooks/config-changes.md` |
+| DA-SNAP-15 | 🔄 → ✅ | `truncate: true` on the job (Q-M12-7): an empty snapshot change set with `truncate`, committed before the start's record (Data-Flow On-Demand Snapshot); `a_truncating_job_drops_the_rows_deleted_meanwhile`, `a_job_s_truncate_empties_the_table_and_keeps_the_marker` |
+| DA-SNAP-16 | 🟥 → ✅ | `KEPT_ENDED_JOBS` (100), `prune_snapshot_jobs` after each ended job, `VersionedStore::delete`; `pruning_keeps_the_newest_ended_jobs_and_every_unfinished_one`, `a_job_that_ends_prunes_the_oldest_ended_jobs` |
+| DA-SNAP-19 | 🟥 → ✅ | `TableHalt.copy`: a copy's halt fails its job table and is kept out of the halted tables, the metric and the API |
+| DA-SNAP-20 | 🟥 → ✅ | `snapshot_jobs` skips an undecodable job with a warning; `an_undecodable_job_is_skipped_by_the_listing` |
+| DA-SNAP-21 | 🟥 → ✅ | a `409` against a queued job writes its restart again (`ask_restart_again`) |
+| DA-SNAP-11 | 🟥 → ✅ | `reconcile.rs`: a key logged once, under its latest writer, the log bounded by the distinct keys written during the wait; `a_key_written_again_is_logged_once_under_its_latest_writer` |
+| DA-SNAP-12 | 🟥 → ✅ | `connect.rs` `Connector::read`: each chunk read on a connection of its own, `IDLE_READS` (4) kept; `a_waiting_chunk_read_holds_up_no_other_read` |
+| DA-API-16 | 🟥 → ✅ | `auth.rs`: no redirect, `MAX_KEY_DOCUMENT` (1 MiB); V-125 warns on a non-`https` key URL; `a_key_fetch_follows_no_redirect_and_reads_a_bounded_body` |
+| DA-API-17 | 🟥 → ✅ | `server.rs` `MAX_CONNECTIONS` (1024), a permit before each accept, and `IDLE_TIMEOUT` (60 s) closing a connection that moves nothing; `past_the_cap_a_connection_waits_for_one_to_close`, `an_idle_connection_is_closed_and_gives_its_place_back` |
+| DA-API-18 | 🟥 → ✅ | `the_document_is_valid_oas_3_1`: the document checked by `boon` against the vendored OAS 3.1 schema |
+| DA-API-20 | 🟥 → ✅ | `limit.rs`: posted validates at 1 a second after a burst of 5, `429` with `Retry-After` (Q-M12b-4); `posted_validates_past_the_burst_wait`. The node's own file stays unlimited |
+| DA-OBS-7 | 🟥 → ✅ | `globals.tracing.headers` (secrets, R, V-126); the exporter rebuilt when they change; `an_https_collector_takes_the_export_with_its_headers`; `deployment/observability.md` |
+| DA-OBS-8 | 🟥 → ✅ | the controller's `Lag { kept, own }`: an earlier session's start kept until the new stream reads as far; `the_kept_lag_start_waits_for_the_next_session_to_read_as_far` |
+| DA-OBS-9 | 🟥 → ✅ | the Jaeger E2E runs the binary with JSON logs and matches a logged flush's `trace_id`/`span_id` to a trace Jaeger holds (`observability.rs`) |
+| DA-SNAP-7 | 🔄 → ✅ | closed with a pointer: an incremental snapshot runs beside the stream, whose position advances with each flush, so a slow snapshot holds back only the WAL or binlog the stream hasn't flushed; the guard (`max_wal_lag_bytes`) and `binlog_purge_action` cover the rest (`architecture/snapshots.md`) |
+| DA-X-13 | 🟥 → ✅ | closed with a pointer: not seen again in any E2E run, local or CI, from M7a through M12b (the plans' measurements and deviations record none) |
+
 ---
 
 ### Counts (by status)
@@ -663,13 +696,13 @@ runs). No count changes.
 > M7a's plan (§32), 2026-10-16 for M7a's exit (§33), 2026-10-17 for M7b's plan (§34),
 > 2026-10-20 for M7b's exit (§35), 2026-10-21 for M8a's plan (§36), 2026-10-22 for M8a's exit (§37),
 > 2026-10-22 for M8b's plan (§38), 2026-10-23 for M8b's exit (§39), 2026-10-23 for M9a's plan
-> (§40), 2026-10-23 for M9a's exit (§41), 2026-10-27 for M9b's plan (§42), 2026-10-28 for M9b's exit (§43), 2026-10-28 for M10's plan (§44), and 2026-10-29 for M10's exit (§45), and 2026-10-29 for M11a's plan (§46), and 2026-10-29 for M11a's whole-branch review (§47), and 2026-10-29 for M11a's exit (§48), and 2026-10-29 for M11b's plan (§49), and 2026-10-30 for M11b's pages (§50), and 2026-10-30 for M11b's exit and v0.1.0 (§51), and 2026-10-30 for M12a's plan (§52), and 2026-11-04 for M12a's gaps closed (§53), and 2026-11-04 for M12a's whole-branch review (§54), and 2026-11-04 for M12a's exit (§55).
+> (§40), 2026-10-23 for M9a's exit (§41), 2026-10-27 for M9b's plan (§42), 2026-10-28 for M9b's exit (§43), 2026-10-28 for M10's plan (§44), and 2026-10-29 for M10's exit (§45), and 2026-10-29 for M11a's plan (§46), and 2026-10-29 for M11a's whole-branch review (§47), and 2026-10-29 for M11a's exit (§48), and 2026-10-29 for M11b's plan (§49), and 2026-10-30 for M11b's pages (§50), and 2026-10-30 for M11b's exit and v0.1.0 (§51), and 2026-10-30 for M12a's plan (§52), and 2026-11-04 for M12a's gaps closed (§53), and 2026-11-04 for M12a's whole-branch review (§54), and 2026-11-04 for M12a's exit (§55), and 2026-11-04 for M12b's gaps closed (§57).
 
 | Status | Count (v1 scope) | Notes |
 |---|---|---|
 | ✅ Resolved | the large majority | Design closed by a research/spec doc or a probe. |
-| 🔄 Partial | 3 | DA-SRC-9 ✅ 2026-11-04 (§53); DA-OPS-10 and DA-SNAP-15 🔄 2026-10-30 (§50); DA-SCHEMA-10 ✅ 2026-10-28 (M9b merged, §43); DA-SNAP-7 (doesn't block the plan); DA-OPS-8 ✅ 2026-10-30 (§50; 🔄 2026-10-22, §38); DA-SNAP-10 ✅ 2026-10-16 (M7a merged, §33). DA-MS-3 is a decided caveat. DA-X-8 ✅ 2026-09-26; DA-SRC-16 and DA-X-12 ✅ 2026-10-13 (§30). |
-| 🟥 Open (v1) | **28** | DA-SRC-32, 33, 34, 35, 36, DA-OPS-14, DA-SNAP-22 and DA-SINK-12 🆕 2026-11-04 (§54.1). Recounted 2026-11-04 (§54): the 27 🟥 domain rows (DA-CLUS-15; DA-SNAP-11, 12, 16, 19, 20, 21, 22; DA-SRC-25, 31, 32, 33, 34, 35, 36; DA-SINK-12; DA-OBS-7, 8, 9; DA-API-13, 16, 17, 18, 20; DA-OPS-9, 12, 14) and DA-X-13. DA-SRC-13, 14, 15, 17, 18, 19, 20, 21, 22, 24, 26, 29, 30, DA-SNAP-13, DA-SNAP-14, DA-OPS-13 and DA-X-15 ✅ 2026-11-04 (§53), 17 rows; DA-SRC-25 stays open (withdrawn in Task 6's review); DA-SRC-31 🆕 2026-11-04 (§53.1). Recounted 2026-11-04 (§53): the 19 🟥 domain rows (DA-CLUS-15; DA-SNAP-11, 12, 16, 19, 20, 21; DA-SRC-25, 31; DA-OBS-7, 8, 9; DA-API-13, 16, 17, 18, 20; DA-OPS-9, 12) and DA-X-13. DA-X-15 🆕 2026-10-30 (§52.1). Recounted 2026-10-30 (§52): the 34 domain rows and DA-X-13. DA-OPS-11 ✅ 2026-10-30 (§51: `v0.1.0` released, the packages public); DA-SRC-30 🆕 2026-10-30 (§50.1); DA-OPS-10, DA-API-14, DA-API-15, DA-SNAP-15, DA-SNAP-17 and DA-SNAP-18 leave 2026-10-30 (§50; DA-OPS-10 and DA-SNAP-15 as 🔄); DA-CLUS-15 stays; DA-OPS-12, DA-OPS-13 and DA-SRC-29 🆕 2026-10-29 (§49.1); DA-CLUS-15 🆕 2026-10-29 (§47.1); DA-OPS-11 🆕 2026-10-29 (§46.1), extended (§47), the manual run done (§48); DA-OBS-8 and DA-OBS-9 🆕 2026-10-29 (§45.1); DA-OBS-7 🆕 2026-10-28 (§44.1); DA-SNAP-18…21 🆕 2026-10-28 (§43.1); DA-SNAP-15…17 🆕 2026-10-27 (§42.1); DA-API-13…18 🆕 2026-10-23 and DA-API-20 🆕 2026-10-24 (§41.1); DA-API-19 🆕 2026-10-23, ✅ 2026-10-24 (§41); DA-SRC-13…15, 17, 18, carried from M5 (§27); DA-SNAP-11…13 and DA-SRC-19 🆕 2026-10-07 (§28.2); DA-SRC-20 🆕 2026-10-13 (§30.1); DA-SRC-21 🆕 2026-10-14 (§31.2); DA-SRC-22, DA-SNAP-14 and DA-X-13 🆕 2026-10-14 (§32.1); DA-SRC-24…26 🆕 2026-10-16 (§33.1); DA-OBS-6 🆕 2026-10-17 (§34.1), ✅ 2026-10-29 (M10 merged, §45); DA-OPS-9 🆕 2026-10-22 (§38.1); DA-OPS-10 🆕 2026-10-23 (§39.1). DA-OPS-8 🔄 2026-10-22 (§38). DA-API-11 and DA-API-12 ✅ 2026-10-23 (§40). DA-CLUS-14, DA-OPS-7 and DA-SRC-28 ✅ 2026-10-22 (§37). DA-SRC-27 and DA-X-14 ✅ 2026-10-20 (§35). DA-SNAP-9 ✅ 2026-10-03 (§28). The design decisions were all made 2026-09-26 (§26.5, `V1_Scope_Decisions.md`), and the deferrals confirmed. |
+| 🔄 Partial | 0 | DA-OPS-10, DA-SNAP-15 and DA-SNAP-7 ✅ 2026-11-04 (§57). DA-SRC-9 ✅ 2026-11-04 (§53); DA-OPS-10 and DA-SNAP-15 🔄 2026-10-30 (§50); DA-SCHEMA-10 ✅ 2026-10-28 (M9b merged, §43); DA-SNAP-7 (doesn't block the plan); DA-OPS-8 ✅ 2026-10-30 (§50; 🔄 2026-10-22, §38); DA-SNAP-10 ✅ 2026-10-16 (M7a merged, §33). DA-MS-3 is a decided caveat. DA-X-8 ✅ 2026-09-26; DA-SRC-16 and DA-X-12 ✅ 2026-10-13 (§30). |
+| 🟥 Open (v1) | **10** | Recounted 2026-11-04 (§57): the 10 🟥 domain rows (DA-SNAP-22; DA-SRC-25, 31, 32, 33, 34, 35, 36; DA-SINK-12; DA-OPS-14), M12d's and M12c's (Q-M12-8). DA-CLUS-15, DA-SNAP-11, 12, 16, 19, 20, 21, DA-OBS-7, 8, 9, DA-API-13, 16, 17, 18, 20, DA-OPS-9, 12 and DA-X-13 ✅ 2026-11-04 (§57), 18 rows. DA-SRC-32, 33, 34, 35, 36, DA-OPS-14, DA-SNAP-22 and DA-SINK-12 🆕 2026-11-04 (§54.1). Recounted 2026-11-04 (§54): the 27 🟥 domain rows (DA-CLUS-15; DA-SNAP-11, 12, 16, 19, 20, 21, 22; DA-SRC-25, 31, 32, 33, 34, 35, 36; DA-SINK-12; DA-OBS-7, 8, 9; DA-API-13, 16, 17, 18, 20; DA-OPS-9, 12, 14) and DA-X-13. DA-SRC-13, 14, 15, 17, 18, 19, 20, 21, 22, 24, 26, 29, 30, DA-SNAP-13, DA-SNAP-14, DA-OPS-13 and DA-X-15 ✅ 2026-11-04 (§53), 17 rows; DA-SRC-25 stays open (withdrawn in Task 6's review); DA-SRC-31 🆕 2026-11-04 (§53.1). Recounted 2026-11-04 (§53): the 19 🟥 domain rows (DA-CLUS-15; DA-SNAP-11, 12, 16, 19, 20, 21; DA-SRC-25, 31; DA-OBS-7, 8, 9; DA-API-13, 16, 17, 18, 20; DA-OPS-9, 12) and DA-X-13. DA-X-15 🆕 2026-10-30 (§52.1). Recounted 2026-10-30 (§52): the 34 domain rows and DA-X-13. DA-OPS-11 ✅ 2026-10-30 (§51: `v0.1.0` released, the packages public); DA-SRC-30 🆕 2026-10-30 (§50.1); DA-OPS-10, DA-API-14, DA-API-15, DA-SNAP-15, DA-SNAP-17 and DA-SNAP-18 leave 2026-10-30 (§50; DA-OPS-10 and DA-SNAP-15 as 🔄); DA-CLUS-15 stays; DA-OPS-12, DA-OPS-13 and DA-SRC-29 🆕 2026-10-29 (§49.1); DA-CLUS-15 🆕 2026-10-29 (§47.1); DA-OPS-11 🆕 2026-10-29 (§46.1), extended (§47), the manual run done (§48); DA-OBS-8 and DA-OBS-9 🆕 2026-10-29 (§45.1); DA-OBS-7 🆕 2026-10-28 (§44.1); DA-SNAP-18…21 🆕 2026-10-28 (§43.1); DA-SNAP-15…17 🆕 2026-10-27 (§42.1); DA-API-13…18 🆕 2026-10-23 and DA-API-20 🆕 2026-10-24 (§41.1); DA-API-19 🆕 2026-10-23, ✅ 2026-10-24 (§41); DA-SRC-13…15, 17, 18, carried from M5 (§27); DA-SNAP-11…13 and DA-SRC-19 🆕 2026-10-07 (§28.2); DA-SRC-20 🆕 2026-10-13 (§30.1); DA-SRC-21 🆕 2026-10-14 (§31.2); DA-SRC-22, DA-SNAP-14 and DA-X-13 🆕 2026-10-14 (§32.1); DA-SRC-24…26 🆕 2026-10-16 (§33.1); DA-OBS-6 🆕 2026-10-17 (§34.1), ✅ 2026-10-29 (M10 merged, §45); DA-OPS-9 🆕 2026-10-22 (§38.1); DA-OPS-10 🆕 2026-10-23 (§39.1). DA-OPS-8 🔄 2026-10-22 (§38). DA-API-11 and DA-API-12 ✅ 2026-10-23 (§40). DA-CLUS-14, DA-OPS-7 and DA-SRC-28 ✅ 2026-10-22 (§37). DA-SRC-27 and DA-X-14 ✅ 2026-10-20 (§35). DA-SNAP-9 ✅ 2026-10-03 (§28). The design decisions were all made 2026-09-26 (§26.5, `V1_Scope_Decisions.md`), and the deferrals confirmed. |
 | 🧪 Verify (v1) | **0** | DA-SRC-10/11 proven 2026-09-26 (§21). |
 | ⏭ v2 | 13 | Iceberg-sink items (§26.2). |
 | 🟢 Spec-merge debt | **0** | All applied 2026-09-26 (§26.6): §19.3 S-1…S-6, the DA-X-8 config residual, the D-V1 patches and the Data-Flow Phase 2a probe details. |
@@ -809,7 +842,7 @@ paper** and is this report's top blocker.
 | DA-CLUS-12 | `kubernetes` lock_backend (K8s Lease API) | 🟢 | ✅ (deferred) | Reserved as a future `LeaseStore` impl; **only `storage` is built for v1** (V-075). A genuine but deliberately deferred scope item. **✅ 2026-09-26 — deferral confirmed (`V1_Scope_Decisions.md` §2).** | D-2 |
 | DA-CLUS-13 | Per-source-endpoint takeover bulkhead | 🟢 | ✅ (deferred) | §20.2 notes the sharper risk (many pipelines on one source DB reconnecting together) as a **future** per-endpoint concurrency group; v1 cap is per-node-global. **✅ 2026-09-26 — deferral confirmed (`V1_Scope_Decisions.md` §2).** | D-10 |
 | DA-CLUS-14 | A lease store and a lake that disagree on the epoch (a moved, restored or deleted `globals.state`) | 🟢 | ✅ | ✅ 2026-10-22 (§37): a fenced worker's claim takes its own lease over at `max(stored + 1, current_epoch + 1)` in one step (`Leases::take_over_at`, `Claim::at_least`); a second fence after that jump is an `ERROR` naming another lease store, with the lease kept; a stop spends the jump, so the ERROR's remedy (stop, resume) climbs once more. Another holder's lease is never touched by the floor. M8a Task 8 (D-M8a.14) | — |
-| DA-CLUS-15 | 🆕 DNS discovery asks again only while a node has no peer | 🟢 | 🟥 | §47.1. With four or more pods started together (the chart's `Parallel`), nodes that each found some, not all, of the others can form groups that never merge. **2026-10-30 (§50): stays 🟥;** `configuration/cluster.md` and `runbooks/failover.md` warn, give the check (`GET /v1/cluster/status` on each node) and the remedy; the fix stays in the code. | — |
+| DA-CLUS-15 | 🆕 DNS discovery asks again only while a node has no peer | 🟢 | ✅ | §47.1. With four or more pods started together (the chart's `Parallel`), nodes that each found some, not all, of the others can form groups that never merge. **2026-10-30 (§50): stays 🟥;** `configuration/cluster.md` and `runbooks/failover.md` warn, give the check (`GET /v1/cluster/status` on each node) and the remedy; the fix stays in the code. **✅ 2026-11-04 (§57):** `discovery.rs` `discover`: every node resolves its seeds each `REDISCOVER` (5 s) and announces to those that aren't members; `groups_that_each_have_a_peer_ask_their_seeds_again_and_merge`. The docs' caution is gone (`configuration/cluster.md`, `runbooks/failover.md`) | — |
 
 ---
 
@@ -828,21 +861,21 @@ read-point hazard. This is a *change* to `Data-Flow.md` Phase 1, so it carries s
 | DA-SNAP-4 | Snapshot parallelism within a pipeline | 🟢 | ✅ | §5 — reuses `parallelism`; disjoint tables per worker; serial keyset within a table. | OQ-S4 |
 | DA-SNAP-5 | Keyset collation correctness (text identifiers) | 🟡 | ✅ | §2 collation pin — binary collation (`COLLATE "C"` / `utf8mb4_bin`) for the pagination predicate, recorded in the cursor. Subtle but load-bearing. | — |
 | DA-SNAP-6 | `snapshot.chunk_size` config field | 🟢 | ✅ | Default 10000; promotion into config owed (§19). Promoted 2026-09-26 as `tables.snapshots.chunk_size` (V-108). | G-6 |
-| DA-SNAP-7 | Snapshot vs WAL/binlog retention coupling | 🟡 | 🔄 | §7 cross-ref — a slow snapshot lengthens the stream backlog; interacts with the WAL-lag guard (G-22) and binlog purge (G-26). Designed but the interaction tuning is an operational note, not a hard rule. | — |
+| DA-SNAP-7 | Snapshot vs WAL/binlog retention coupling | 🟡 | ✅ | §7 cross-ref — a slow snapshot lengthens the stream backlog; interacts with the WAL-lag guard (G-22) and binlog purge (G-26). Designed but the interaction tuning is an operational note, not a hard rule. **✅ 2026-11-04 (§57):** closed with a pointer: an incremental snapshot runs beside the stream, whose position advances with each flush, so a slow snapshot holds back only the WAL or binlog the stream hasn't flushed; the guard (`max_wal_lag_bytes`) and `binlog_purge_action` cover the rest (`architecture/snapshots.md`) | — |
 | DA-SNAP-8 | PK-less table under a **dump-only** mode (snapshot, never stream) | 🟡 | ✅ | The PK-less halt rule (DA-DEDUP-3) is justified by equality-delete keying — but a `dump_*` table is insert-only and never deletes, so the keying argument doesn't apply. Whether a dump-only PK-less table may be snapshotted (or still halts) is **unspecified**. Newly surfaced edge case. **2026-09-26 tidy:** decided by spec: the identifier precondition (config §11.2) applies in every mode — explicit PK-less table halts, glob-matched one is skipped; `tables.surrogate_key` struck (G-30). Dump-only PK-less tables are not supported in v1. | — |
 | DA-SNAP-9 | The snapshot watermark can't order a chunk's MVCC snapshot against the stream | 🟡 | ✅ | Closed 2026-10-03 (§28): each chunk read records its `pg_current_snapshot()`, the writers in flight at the session's start are waited for, and the reconciler drops a chunk row a stream transaction the read didn't see already wrote (Snapshot_Resumability §1.1; Data-Flow Phase 1, S-T). Measured: M6a Task 13's busy-snapshot and stalled-synchronous-commit tests. | — |
 | DA-SNAP-10 | MySQL's window rule may miss a transaction the binlog delivered before InnoDB made it visible | 🟡 | ✅ | §28.1. MySQL/MariaDB chunks carry no read visibility, so the reconciler uses the window rule: only transactions applied after a read was issued are filtered. Group commit makes the binlog readable (sync stage) before the InnoDB commit (commit stage); semi-sync `AFTER_SYNC` waits for the replica ack between them, up to `rpl_semi_sync_source_timeout`. A transaction applied from the binlog before a chunk is issued can then still be invisible to that chunk's read, and a stale chunk row would undo it, the MySQL analogue of DA-SNAP-9's stalled synchronous commit. Reasoned (M6a Task 9 review), not measured. Close before M7: an S-MY probe that stalls a semi-sync ack, then the fix (e.g. a per-read visibility signal, or waiting for in-flight commits as Q-M6a-2 does on PostgreSQL). **🔄 2026-10-14 (§31): measured and decided.** S-MY11 measured it on MySQL 8.4 and MariaDB 10.11 / 11.8 (semi-sync AFTER_SYNC; MariaDB's default AFTER_COMMIT has no gap). Fix (Q-M7-3): a MySQL chunk reads with `LOCK IN SHARE MODE` in its short READ COMMITTED transaction, which waits for the stalled commit and sees it (S-MY11; its cost, S-MY11b); `Data-Flow.md` Phase 1 step 2b/2c, `Snapshot_Resumability…` §1.1. Closes when M7 builds it with its test. **2026-10-14 (§32):** built and tested in M7a's plan (Task 10, `a_locking_read_waits_for_a_writer_of_its_rows`, on MySQL 8.4 and MariaDB 11.8); ✅ when M7a merges. **✅ 2026-10-16:** M7a merged (PR #9) with the locking chunk read and its test. | — |
-| DA-SNAP-11 | 🆕 The reconciler's log is unbounded while a snapshotting table's reads wait | 🟢 | 🟥 | §28.2. Every stream transaction that writes a snapshotting table is logged with its keys until a chunk proves it visible. While the table's reads wait (an in-flight writer at session start, a drift hold), no chunk is reconciled and the log grows with every key written, for up to `timeouts.snapshot`. M6a warns once per wait and reports the log's size in the status (M6b's gauge); bounding it (coalesce per table, or skip logging until a read can be issued) is open. | — |
-| DA-SNAP-12 | 🆕 `parallelism > 1` reads chunks one after another on PostgreSQL | 🟢 | 🟥 | §28.2. Chunk reads share the one catalog connection, so they pipeline rather than run in parallel; each read's client-side bound starts at spawn, so a read queued behind slow chunks can time out and the abandoned connection fails its neighbours. Fix: a small read pool or a connection per concurrent read. | — |
+| DA-SNAP-11 | 🆕 The reconciler's log is unbounded while a snapshotting table's reads wait | 🟢 | ✅ | §28.2. Every stream transaction that writes a snapshotting table is logged with its keys until a chunk proves it visible. While the table's reads wait (an in-flight writer at session start, a drift hold), no chunk is reconciled and the log grows with every key written, for up to `timeouts.snapshot`. M6a warns once per wait and reports the log's size in the status (M6b's gauge); bounding it (coalesce per table, or skip logging until a read can be issued) is open. **✅ 2026-11-04 (§57):** `reconcile.rs`: a key logged once, under its latest writer, the log bounded by the distinct keys written during the wait; `a_key_written_again_is_logged_once_under_its_latest_writer` | — |
+| DA-SNAP-12 | 🆕 `parallelism > 1` reads chunks one after another on PostgreSQL | 🟢 | ✅ | §28.2. Chunk reads share the one catalog connection, so they pipeline rather than run in parallel; each read's client-side bound starts at spawn, so a read queued behind slow chunks can time out and the abandoned connection fails its neighbours. Fix: a small read pool or a connection per concurrent read. **✅ 2026-11-04 (§57):** `connect.rs` `Connector::read`: each chunk read on a connection of its own, `IDLE_READS` (4) kept; `a_waiting_chunk_read_holds_up_no_other_read` | — |
 | DA-SNAP-14 | 🆕 An `ENUM` key whose members are reordered during a snapshot | 🟢 | ✅ | §32.1. An `ENUM` key sorts by member index, and a chunk resumes after its cursor's label at that label's index under the members read for the chunk. A reorder mid-snapshot changes the order, so the next chunk can skip or repeat rows. The stream halts the table at the reorder (§7.2), but a chunk read between the DDL and the signal doesn't notice: the read plan compares value types, text for an `ENUM` either way. **✅ 2026-11-04 (§53):** MySQL `snapshot.rs` `moved`: a reordered, removed or renamed `ENUM`/`SET` key member is a change under the snapshot (the stream halts the table `UNSAFE_DDL`) | — |
 | DA-SNAP-13 | 🆕 A wiped state with an existing sink table baselines nothing | 🟢 | ✅ | §28.2. With no cached schema, Phase 0 doesn't baseline (§8.2) against the sink table's schema; a shape mismatch surfaces later as `SINK_REJECTED` (visible, not silent). Only the adopted-orphan case with a cache is covered. **✅ 2026-11-04 (§53):** `schema_change.rs` `missing_columns`, the worker's baseline against the sink table; `a_wiped_state_baselines_against_the_sink_table`; a sink column matching a source column by case alone halts `SCHEMA_MISMATCH` (Delta names are case-insensitive): `a_sink_column_matching_by_case_alone_halts` | — |
-| DA-SNAP-15 | 🆕 A job upserts: a row deleted at the source while its table wasn't streamed stays | 🟡 | 🔄 | §42.1. Q-M9b-2: no truncate. M11's docs (drop or move the Delta table first); a truncate option later. **🔄 2026-10-30 (§50):** documented in `architecture/snapshots.md`, `runbooks/snapshot-jobs.md`, `runbooks/halted-table.md`; the truncate option stays open. | — |
-| DA-SNAP-16 | 🆕 Job documents accumulate, and the list and the ask's check read every one | 🟢 | 🟥 | §42.1. A retention, or an index of the unfinished jobs. | — |
+| DA-SNAP-15 | 🆕 A job upserts: a row deleted at the source while its table wasn't streamed stays | 🟡 | ✅ | §42.1. Q-M9b-2: no truncate. M11's docs (drop or move the Delta table first); a truncate option later. **🔄 2026-10-30 (§50):** documented in `architecture/snapshots.md`, `runbooks/snapshot-jobs.md`, `runbooks/halted-table.md`; the truncate option stays open. **✅ 2026-11-04 (§57):** `truncate: true` on the job (Q-M12-7): an empty snapshot change set with `truncate`, committed before the start's record (Data-Flow On-Demand Snapshot); `a_truncating_job_drops_the_rows_deleted_meanwhile`, `a_job_s_truncate_empties_the_table_and_keeps_the_marker` | — |
+| DA-SNAP-16 | 🆕 Job documents accumulate, and the list and the ask's check read every one | 🟢 | ✅ | §42.1. A retention, or an index of the unfinished jobs. **✅ 2026-11-04 (§57):** `KEPT_ENDED_JOBS` (100), `prune_snapshot_jobs` after each ended job, `VersionedStore::delete`; `pruning_keeps_the_newest_ended_jobs_and_every_unfinished_one`, `a_job_that_ends_prunes_the_oldest_ended_jobs` | — |
 | DA-SNAP-17 | 🆕 A job table's halt fails the job only with the session's next flush | 🟢 | ✅ | §42.1. Write a job table's halt at once, or M11's docs. **✅ 2026-10-30 (§50, documented):** `runbooks/snapshot-jobs.md`. | — |
 | DA-SNAP-18 | 🆕 A table created again while the pipeline was down takes two jobs | 🟢 | ✅ | §43.1. The first fails it `TABLE_DROPPED`, the second clears the halt. M11's docs, or one pass once proven. **✅ 2026-10-30 (§50, documented):** `runbooks/halted-table.md`. | — |
-| DA-SNAP-19 | 🆕 A copy's halt shows as halted until the pipeline's next start | 🟢 | 🟥 | §43.1. Clear it when the owner fails the job table. | — |
-| DA-SNAP-20 | 🆕 An undecodable job document stops the pipeline's start | 🟢 | 🟥 | §43.1. Skip it with a warning. | — |
-| DA-SNAP-21 | 🆕 A job whose restart wasn't written stays queued until the next start | 🟢 | 🟥 | §43.1. Write the restart again when asked again. | — |
+| DA-SNAP-19 | 🆕 A copy's halt shows as halted until the pipeline's next start | 🟢 | ✅ | §43.1. Clear it when the owner fails the job table. **✅ 2026-11-04 (§57):** `TableHalt.copy`: a copy's halt fails its job table and is kept out of the halted tables, the metric and the API | — |
+| DA-SNAP-20 | 🆕 An undecodable job document stops the pipeline's start | 🟢 | ✅ | §43.1. Skip it with a warning. **✅ 2026-11-04 (§57):** `snapshot_jobs` skips an undecodable job with a warning; `an_undecodable_job_is_skipped_by_the_listing` | — |
+| DA-SNAP-21 | 🆕 A job whose restart wasn't written stays queued until the next start | 🟢 | ✅ | §43.1. Write the restart again when asked again. **✅ 2026-11-04 (§57):** a `409` against a queued job writes its restart again (`ask_restart_again`) | — |
 | DA-SNAP-22 | 🆕 The wiped-state baseline's limits | 🟢 | 🆕 🟥 | §54.1. An uncached `Plan::Resume` table whose sink table exists isn't baselined; a non-null sink key column the source lacks is accepted until the first MERGE fails (`SINK_REJECTED`). Also the via-root repair's re-snapshot of every table, kept conservative until measured. Candidate: baseline any uncached table the sink holds; refuse a non-null sink field the source lacks. | — |
 
 ---
@@ -970,9 +1003,9 @@ endpoint), and trace-id/pipeline injection into JSON logs.
 | DA-OBS-4 | Log/trace correlation | 🟢 | ✅ | §4 — inject `trace_id`/`span_id`/`pipeline` into JSON logs. | §10.3 |
 | DA-OBS-5 | **Numeric end-to-end replication-lag SLA** | 🟡 | ✅ | `Clustering…` §18.6 explicitly leaves OQ-OB2 open as a *tuning input*: a concrete max-lag number is not fixed, which is what grounds the lease-timing profile and flush/checkpoint cadence. Decide a number (and whether failover-time carries its own SLA distinct from steady-state). **✅ 2026-09-26 — D-V1.3 (`V1_Scope_Decisions.md`):** steady-state p99 ≤ 60s, failover ≤ 2 min, at default config; confirm by v1 load test. | OQ-OB2 |
 | DA-OBS-6 | 🆕 The text log writes ANSI colour sequences to a file | 🟢 | ✅ | §34.1. The binary's `compact` and `pretty` logs colour their output whether or not stderr is a terminal (`tracing-subscriber` 0.3.23's default; `crates/data-avalanche/src/logging.rs`). A log collector or a file gets escape sequences around the level and the fields: a search for `code="BINLOG_…"` misses them. The E2E suite strips them (M7b Task 7). Fix: colour only when stderr is a terminal (and keep `NO_COLOR`); M10 (`globals.log`) or earlier. M10 Task 7 fixes it (§44). **✅ 2026-10-29** — M10 merged (PR #15, `550474c`), its CI green (§45). | — |
-| DA-OBS-7 | 🆕 The OTLP export sends no headers | 🟢 | 🟥 | §44.1. `globals.tracing` has no headers, so a collector that wants a token can't take the traces; an `https` endpoint goes through `reqwest` 0.13 with rustls and is untested. Next: `globals.tracing.headers` (secret-resolved) and an `https` IT, after v1 unless needed | — |
-| DA-OBS-8 | 🆕 The lag reads `0` early in two short windows | 🟢 | 🟥 | §45.1. A restarted worker that makes its first re-read change durable before re-reading the next, and a source reconnect inside one worker, show `0` until the change is read again (about a heartbeat on MySQL; on PostgreSQL the re-decode of a large transaction). Next: keep the kept start until the frontier covers it; carry the pending start across a reconnect | — |
-| DA-OBS-9 | 🆕 The JSON correlation's global-dispatch path is untested end to end | 🟢 | 🟥 | §45.1. The logger's tests run under a scoped dispatch; the binary's `set_global_default` path holds by reading `tracing-core` 0.1.36. Next: a JSON-logged line matched to a Jaeger trace in the E2E | — |
+| DA-OBS-7 | 🆕 The OTLP export sends no headers | 🟢 | ✅ | §44.1. `globals.tracing` has no headers, so a collector that wants a token can't take the traces; an `https` endpoint goes through `reqwest` 0.13 with rustls and is untested. Next: `globals.tracing.headers` (secret-resolved) and an `https` IT, after v1 unless needed **✅ 2026-11-04 (§57):** `globals.tracing.headers` (secrets, R, V-126); the exporter rebuilt when they change; `an_https_collector_takes_the_export_with_its_headers`; `deployment/observability.md` | — |
+| DA-OBS-8 | 🆕 The lag reads `0` early in two short windows | 🟢 | ✅ | §45.1. A restarted worker that makes its first re-read change durable before re-reading the next, and a source reconnect inside one worker, show `0` until the change is read again (about a heartbeat on MySQL; on PostgreSQL the re-decode of a large transaction). Next: keep the kept start until the frontier covers it; carry the pending start across a reconnect **✅ 2026-11-04 (§57):** the controller's `Lag { kept, own }`: an earlier session's start kept until the new stream reads as far; `the_kept_lag_start_waits_for_the_next_session_to_read_as_far` | — |
+| DA-OBS-9 | 🆕 The JSON correlation's global-dispatch path is untested end to end | 🟢 | ✅ | §45.1. The logger's tests run under a scoped dispatch; the binary's `set_global_default` path holds by reading `tracing-core` 0.1.36. Next: a JSON-logged line matched to a Jaeger trace in the E2E **✅ 2026-11-04 (§57):** the Jaeger E2E runs the binary with JSON logs and matches a logged flush's `trace_id`/`span_id` to a trace Jaeger holds (`observability.rs`) | — |
 
 ---
 
@@ -1002,14 +1035,14 @@ unauthenticated. The auth *mechanism* (jwt/oauth2 verification params) was alrea
 | DA-API-10 | Reload must not resurrect a paused/stopped pipeline | 🟡 | ✅ | §4 — runtime `desired_state` wins over config's implicit "should run." | §9.6 |
 | DA-API-12 | 🆕 The reload and status endpoints answer without authentication until M9 | 🟢 | ✅ | §40 (M9a's plan): every `/v1` route takes its least role (API_RBAC §2), reload operator and status viewer; M9a Task 4's matrix test turns away every caller below it. §38.1: Q-M8b-1: `POST /v1/config/reload` and `GET /v1/config/status` serve on `globals.api.address` like `/metrics`, before M9's auth and RBAC. A caller can publish the file that is on the node, not a text of its own. M9 puts them behind the operator and viewer roles (API_RBAC §2). | — |
 | DA-API-11 | 🆕 A desired-state change missed between two polls | 🟢 | ✅ | §40 (M9a's plan): a restart request, recorded by id in `requests.json`, takes a pipeline out of `ERROR` without depending on the controller's sampling (D-M9a.4, D-M9a.5); a stop and a resume inside one poll still look like no change, and needn't be seen apart. §30.1: The controller samples `pipeline-state.json` once per renewal interval, so a stop and a resume between two polls don't clear `ERROR` (Lifecycle §3). The M9 API wakes the controller, or the document carries a generation the controller compares. | — |
-| DA-API-13 | 🆕 A crashed holder's unreleased lease reads as live until takeover | 🟢 | 🟥 | §41.1 (M9a's exit): GET shows its last status, a restart or flush answers `202` (absorbed by the next term); a `stale` hint needs a skew-tolerant liveness notion. | — |
+| DA-API-13 | 🆕 A crashed holder's unreleased lease reads as live until takeover | 🟢 | ✅ | §41.1 (M9a's exit): GET shows its last status, a restart or flush answers `202` (absorbed by the next term); a `stale` hint needs a skew-tolerant liveness notion. **✅ 2026-11-04 (§57):** `pipelines.rs` `liveness`: `holder.stale` (its node and incarnation not a member of the answering node's gossip) and `holder.suspect`, `holder_stale` in the lists; `Member.incarnation`; `a_holder_that_isnt_a_member_reads_as_stale`, `a_suspected_member_holds_its_lease_as_suspect` | — |
 | DA-API-14 | 🆕 A node that left the cluster keeps its last generation's auth | 🟢 | ✅ | §41.1: its coordinator has stopped, so a later reload's roles or credentials don't reach it; refuse its mutating routes, or document it (M11). **✅ 2026-10-30 (§50, documented):** `runbooks/failover.md`. | — |
 | DA-API-15 | 🆕 A retried restart or flush call asks twice | 🟢 | ✅ | §41.1: the request id is per HTTP call; an optional client-supplied id, or M11's docs. **✅ 2026-10-30 (§50, documented):** `runbooks/pipeline-commands.md`. | — |
-| DA-API-16 | 🆕 The JWKS fetch accepts http, follows redirects, reads an unbounded body | 🟢 | 🟥 | §41.1: a V-rule warning for a non-`https` key URL, no redirects, a body cap. | — |
-| DA-API-17 | 🆕 No connection limit on the TLS listener | 🟢 | 🟥 | §41.1: a handshake task per connection, each bounded at 10 s. | — |
-| DA-API-18 | 🆕 The OpenAPI document isn't validated against the OAS 3.1 schema | 🟢 | 🟥 | §41.1: snapshot-tested and read back by utoipa only. | — |
+| DA-API-16 | 🆕 The JWKS fetch accepts http, follows redirects, reads an unbounded body | 🟢 | ✅ | §41.1: a V-rule warning for a non-`https` key URL, no redirects, a body cap. **✅ 2026-11-04 (§57):** `auth.rs`: no redirect, `MAX_KEY_DOCUMENT` (1 MiB); V-125 warns on a non-`https` key URL; `a_key_fetch_follows_no_redirect_and_reads_a_bounded_body` | — |
+| DA-API-17 | 🆕 No connection limit on the TLS listener | 🟢 | ✅ | §41.1: a handshake task per connection, each bounded at 10 s. **✅ 2026-11-04 (§57):** `server.rs` `MAX_CONNECTIONS` (1024), a permit before each accept, and `IDLE_TIMEOUT` (60 s) closing a connection that moves nothing; `past_the_cap_a_connection_waits_for_one_to_close`, `an_idle_connection_is_closed_and_gives_its_place_back` | — |
+| DA-API-18 | 🆕 The OpenAPI document isn't validated against the OAS 3.1 schema | 🟢 | ✅ | §41.1: snapshot-tested and read back by utoipa only. **✅ 2026-11-04 (§57):** `the_document_is_valid_oas_3_1`: the document checked by `boon` against the vendored OAS 3.1 schema | — |
 | DA-API-19 | 🆕 Validate of a posted body echoes a `${VAR}`-resolved pipeline `id` | 🟡 | ✅ | §41 (2026-10-24, on Oleksii's request, PR #13): a posted body's templated `id` or `defaults.id` is refused with V-055 before any rule runs, so no expanded id is echoed or used (`load_posted_str`, `ConfigCoordinator::check_posted`); the node-file validate and the reload are unchanged. §41.1: an operator could read a node env var fitting V-055's pattern. | — |
-| DA-API-20 | 🆕 Validate's verdicts on a posted body's other `${VAR}` leaves answer yes or no about a variable | 🟢 | 🟥 | §41.1: V-063 (set or not), the uniqueness rules V-058/V-059/V-120, pattern and range checks, and the diff's field paths tell, one request at a time, whether a variable is set or equals a value; nothing is echoed. Every validate is logged with its caller (2144392); M12 rate-limits validate. | — |
+| DA-API-20 | 🆕 Validate's verdicts on a posted body's other `${VAR}` leaves answer yes or no about a variable | 🟢 | ✅ | §41.1: V-063 (set or not), the uniqueness rules V-058/V-059/V-120, pattern and range checks, and the diff's field paths tell, one request at a time, whether a variable is set or equals a value; nothing is echoed. Every validate is logged with its caller (2144392); M12 rate-limits validate. **✅ 2026-11-04 (§57):** `limit.rs`: posted validates at 1 a second after a burst of 5, `429` with `Retry-After` (Q-M12b-4); `posted_validates_past_the_burst_wait`. The node's own file stays unlimited | — |
 
 ---
 
@@ -1027,12 +1060,12 @@ pinning is the remaining cross-cutting concern.
 | DA-OPS-3 | `glue.strip_schema` collision handling | 🟢 | ✅ | G-11.3 — collision = config error; non-stripped = `schema__table`. | G-11.3 |
 | DA-OPS-4 | Sink/catalog unavailability → DEGRADED back-pressure | 🟡 | ✅ | G-27/G-35 — stop consuming once buffers fill, protect slot/WAL via G-22; recover on next commit; catalog circuit breaker. | G-27, G-35 |
 | DA-OPS-5 | k8s deployment as StatefulSet (not Deployment) | 🟡 | ✅ | Clustering §3.4/§10.2 — stable `node_name` via Downward API; headless Service discovery. | D-1, D-4 |
-| DA-OPS-10 | 🆕 An `id` change that keeps its resources is sequenced only within one reload, on the nodes that applied it | 🟢 | 🔄 | §39.1. M8b's whole-branch review: an added pipeline sharing a removed one's slot, publication, sink or state waits for the removed one's lease. A node that starts during the wait, a removal and an add in two reloads, a cut-off drain, or the old id reused while the removal drains let it start early. **🔄 2026-10-30 (§50):** `configuration/overview.md` says to wait for the old lease's release, or to do both in one reload; the tombstone stays. | — |
+| DA-OPS-10 | 🆕 An `id` change that keeps its resources is sequenced only within one reload, on the nodes that applied it | 🟢 | ✅ | §39.1. M8b's whole-branch review: an added pipeline sharing a removed one's slot, publication, sink or state waits for the removed one's lease. A node that starts during the wait, a removal and an add in two reloads, a cut-off drain, or the old id reused while the removal drains let it start early. **🔄 2026-10-30 (§50):** `configuration/overview.md` says to wait for the old lease's release, or to do both in one reload; the tombstone stays. **✅ 2026-11-04 (§57):** the controller's `follow()` waits, before its first claim, for every pending tombstone of its id or of a pipeline it shares resources with, on every node; `a_pipeline_added_after_the_removal_it_follows_waits_for_it`, `a_node_started_during_a_removal_waits_for_it` | — |
 | DA-OPS-11 | 🆕 The release workflow's first full run is the first tag's | 🟢 | ✅ | §46.1. `release.yml` (M11a) runs on a `v*` tag or by hand, and only from `main`: before M12's `v1.0.0` only its builds are proven. The first push creates the GHCR packages private. Next: a manual run on `main` after M11a merges; the packages made public after the first tag §47: the repository is private, so the attestation is skipped (BuildKit's in-registry provenance and the SBOM stay) and the `ubuntu-24.04-arm` runner is unproven until `weekly.yml` is dispatched. Oleksii's ruling 2026-10-29: the repository goes public (done, after PR #17's README). §48: the manual run on `main` green (run 40918381277). Next: after the first tag, make the image and chart packages public. **✅ 2026-10-30 (§51):** `v0.1.0` released by `release.yml` (run 41034298202), its image attested, the image and chart packages public | — |
-| DA-OPS-12 | 🆕 A process-restart key written as a literal can't change once a generation exists | 🟢 | 🟥 | §49.1. A reload refuses it (V-061) and a restarted node adopts the stored generation; only `${VAR}` values and file contents change, by a rolling restart. The docs give the workaround. | — |
+| DA-OPS-12 | 🆕 A process-restart key written as a literal can't change once a generation exists | 🟢 | ✅ | §49.1. A reload refuses it (V-061) and a restarted node adopts the stored generation; only `${VAR}` values and file contents change, by a rolling restart. The docs give the workaround. **✅ 2026-11-04 (§57):** `POST /v1/config/reload?restart=accept` (Q-M12b-2): published, each node's `restart_pending` until its next start; `FIXED_FIELDS` refused even then (config §1.2, V-061); `a_restart_field_is_published_when_accepted_and_pending_until_the_next_start`; `runbooks/config-changes.md` | — |
 | DA-OPS-13 | 🆕 Two operator-facing texts are wrong (the orphan-slot refusal's key, the CLI's `about`) | 🟢 | ✅ | §49.1. `slot.on_orphan` for `slot_on_orphan`; MySQL and MariaDB missing from the description. **✅ 2026-11-04 (§53):** `slot.rs` names `slot_on_orphan`; `cli.rs` `about` names the three sources (`the_help_names_every_source`) | — |
 | DA-OPS-14 | 🆕 MariaDB file mode's boundary check costs a binlog read at every stream open | 🟢 | 🆕 🟥 | §54.1. `BINLOG_GTID_POS(file, pos)` scans the file up to `pos` at every start and reconnect. Measure in M12c's load runs; remember the answer for `(file, pos)` within a run if it shows. | — |
-| DA-OPS-9 | 🆕 A pipeline removed while no node holds its lease isn't torn down | 🟢 | 🟥 | §38.1. Q-M8b-3: the owner drains, tears down (`auto_cleanup`) and releases. With no owner at the removal (it is down, or a hand-off is between release and claim), every node's controller is on standby and just ends, so the PostgreSQL slot and publication stay and the slot keeps WAL (bounded by `max_slot_wal_keep_size`). Config §12.3's "orphan-cleanup … whose owner was offline" design item. | — |
+| DA-OPS-9 | 🆕 A pipeline removed while no node holds its lease isn't torn down | 🟢 | ✅ | §38.1. Q-M8b-3: the owner drains, tears down (`auto_cleanup`) and releases. With no owner at the removal (it is down, or a hand-off is between release and claim), every node's controller is on standby and just ends, so the PostgreSQL slot and publication stay and the slot keeps WAL (bounded by `max_slot_wal_keep_size`). Config §12.3's "orphan-cleanup … whose owner was offline" design item. **✅ 2026-11-04 (§57):** `state` `removal.rs` (`removals/<id>.json`), recorded by the coordinator before it adopts; the reconciler's sweep (`SWEEP_INTERVAL`) and the controller's `remove_orphan`; config §12.3; `a_removal_no_node_removed_is_torn_down_where_the_ring_places_it`, `a_removal_held_by_a_gone_node_is_torn_down_once_its_lease_expires` | — |
 | DA-OPS-8 | Per-node literals in a node's own file are ignored once a generation exists | 🟢 | ✅ | §36.1, §38. A node runs the cluster's generation resolved with its own environment (D-3); per-node values go through `${VAR}` (config §6.5.1). M8b warns at startup naming each per-node key that differs, and which value runs; the operator docs stay with M11. **✅ 2026-10-30 (§50):** the operator docs say it, in `configuration/cluster.md` and `runbooks/config-changes.md`: the gossip addresses (`bind_addr`, `bind_port`, `advertise_addr`, `advertise_port`) are the generation's, so a literal there is ignored with a warning; `node_name` and `api.address` are the node's file's. | — |
 | DA-OPS-7 | A controller task that panics goes unnoticed | 🟢 | ✅ | ✅ 2026-10-22 (§37): the reconciler supervises its controllers. One that ended unasked (a panic, a return) is logged and started again on its status channel, and a dropped reconciler counts as a crash; the owned-pipelines gauge is held by a guard, so a panic doesn't over-count it. Readiness after a controller panic is not in M8a (DA-OPS-7's other half: M10). M8a Task 6 (D-M8a.11, `a_controller_that_panics_is_started_again`) | — |
 | DA-OPS-6 | "Restart ≠ apply config" operator footgun | 🟢 | ✅(documented) | Clustering §9.3 D-3 — cloud-generation-wins; a pod restart does **not** apply ConfigMap edits, only `POST /reload` does. Document loudly. | D-3 |
```

- [ ] **Step 4: Check the site and the references**

Run:

```bash
cargo xtask docs && git diff --exit-code docs/src/content/docs/reference docs/public
(cd docs && npm ci && npm run build)
cargo test -p xtask
```

Expected: no reference page changes; the site builds, its link validation passing (the new
`#remove-a-pipeline` anchor included); xtask's tests pass, the docs' config examples among them.

- [ ] **Step 5: Commit**

```bash
git add doc/analysis/CDC_Design_Gap_Analysis_v1.md \
  doc/research/API_RBAC_and_Versioning.md \
  doc/research/Clustering_Architecture_Research.md \
  docs/src/content/docs/architecture/delivery.md \
  docs/src/content/docs/architecture/snapshots.md \
  docs/src/content/docs/configuration/cluster.md \
  docs/src/content/docs/configuration/overview.md \
  docs/src/content/docs/deployment/kubernetes.md \
  docs/src/content/docs/deployment/observability.md \
  docs/src/content/docs/runbooks/config-changes.md \
  docs/src/content/docs/runbooks/failover.md \
  docs/src/content/docs/runbooks/halted-table.md \
  docs/src/content/docs/runbooks/position-lost.mdx \
  docs/src/content/docs/runbooks/snapshot-jobs.md
git commit -m "docs: the M12b changes — removing a pipeline, keys applied at the next start, a job's truncate, a stale holder, discovery, the trace headers; research: the tombstone and restart=accept as built; gap: v1 §57 — the M12b gaps closed"
```


---

## Exit

- [ ] Every task's Step 4 passes, and the whole E2E suite on PostgreSQL 18, `mysql-84`,
  `mysql-84-nogtid` and `mariadb-118` (locally, and the PR's `integration` and `e2e-mysql` jobs).
- [ ] `cargo audit` clean under `.cargo/audit.toml`.
- [ ] The site builds (`docs.yml`), and `cargo xtask docs` leaves the references unchanged.
- [ ] The whole-branch review's findings fixed or recorded; the PR's CI green.
- [ ] The tracker's §57 rows stand when M12b merges (the exit's tracker section).

## Deviations during execution

- **Task 3** (review fix, `45056a6`): the sweep's orphan claimed with this node's own `Holder`,
  and `Claim::step` takes over a lease naming its own holder at once (its own earlier term). An
  orphan could so take the lease of a live controller of the same id on the same node and tear
  the pipeline down under it: a re-added id whose tombstone is still pending, or a sweep landing
  between a tombstone's write and its reload. The orphan now claims with `Claim::patient()`: a
  lease naming its own holder is watched as another holder's is, and taken over only after a full
  `lease_duration` without a renewal; the live controller's claim is unchanged. Tests
  `a_patient_claim_waits_for_its_own_holders_lease` (`claim.rs`) and
  `an_orphan_waits_for_this_nodes_own_live_holder` (`controller.rs`, which fails without the fix).
  The sweep still takes up a tombstone of an id this node runs: Task 4's re-added controller
  waits on that tombstone, so skipping it would deadlock. Task 4's blocks apply unchanged on top.
- **Task 4** (review fix, `72c5028`): `RemovedPipelines::resolve` returned a plain `String` error,
  so a tombstone whose generation could not be read (a transient storage error) was logged and
  skipped, and a re-added controller's wait for it ended early. `resolve` now returns
  `ResolveError { Unread, Invalid }`: an unread generation, or an unread tombstone listing, holds
  the wait and is retried; only an invalid one (bad config text, a missing pipeline, bad docs) is
  logged and skipped. Tests `a_removal_whose_generation_is_unread_is_waited_for` and
  `an_unread_tombstone_list_is_waited_for` (`controller.rs`, with a test-only
  `ScriptedStore::fail_lists`). The implementation had also dropped the existing test
  `an_add_for_a_running_pipeline_restarts_it` (`reconciler.rs`); it is restored verbatim. Task 5's
  blocks apply unchanged on top.
- **Task 6** (review fix, `7d14c44`): the plan's code gave `SnapshotJobDoc.truncate` only
  `#[serde(default)]`, so every job document was written with `"truncate": false`, against this
  plan's own State compatibility constraint: v0.1.0's `SnapshotJobDoc` refuses unknown fields, so a
  v0.1.0 node (a rolling upgrade, a rollback) would refuse every job this version writes. It is now
  `skip_serializing_if = "std::ops::Not::not"`; the encode test expects no `truncate` key for a job
  that keeps its rows, and `"truncate": true`, read back, for `truncating()`. Task 4's fix
  (`ScriptedStore::fail_lists`) moved `ScriptedStore::list`, so Task 7's `test_support.rs` block
  (`delete`) is rebased onto its new context; its change is the same. Tasks 7–16's blocks apply
  in order on `7d14c44`.
- **Task 9** (review fix, `053c03a`): the plan's `connect.rs` block put `IDLE_READS` between
  `server_millis`'s doc comment and the function, so the doc landed on the constant. The constant
  now sits above it, each under its own comment. No code changes. Tasks 10–16's blocks apply in
  order on `053c03a`.
- **Task 10** (review fix, `ab4b997`, approved by Oleksii 2026-11-05): the cap came with no idle
  limit. axum 0.8.9's `serve` builds hyper's connection builder without a timer
  (`serve/mod.rs:391`), so hyper's header-read timeout never fires, and 1024 connections that say
  nothing (opened by anyone who reaches the port, before any auth) held every place for good,
  `/health/live` included. `Counted` now closes a connection that reads and writes nothing for
  `IDLE_TIMEOUT` (60 s): a silent one, a kept-alive one between requests, or a request whose
  handler answers nothing for that long (the API has no streaming answers). `Bound.idle_timeout`
  (tests lower it), and `Bound`'s `Debug` shows both limits. Tests
  `an_idle_connection_is_closed_and_gives_its_place_back` (plain and TLS; fails without the
  deadline) and `a_connection_in_use_outlives_the_idle_timeout`. Ruling D-M12b.13 amended; Task
  11's re-export adds `IDLE_TIMEOUT`; the research and tracker rows for DA-API-17 name it.
