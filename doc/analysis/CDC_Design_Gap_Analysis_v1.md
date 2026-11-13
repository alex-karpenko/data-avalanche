---
updated: 2026-11-13
domain: Architecture
tags: [gap-analysis, design, re-derivation, spec-readiness, blockers, open-decisions]
resolves: []
sources: [Sources_Index_v3.md]
---

# CDC System — Design Gap Analysis (fresh re-derivation, v1)

> **This document is a from-scratch re-evaluation.** It deliberately ignores the content of
> `CDC_Gap_Analysis_Report.md` (v9) and re-derives the gap set directly from the four spec
> documents and the thirteen research documents as they stand on 2026-06-20. The legacy report is
> retained unchanged as the historical audit trail. Where a re-derived gap maps onto a legacy ID,
> the legacy ID is shown in *(parentheses)* so the two trackers can be reconciled.
>
> Purpose, per the brief: **prepare a strong foundation for application-specification elaboration.**
> The lens is comprehensive — every domain is mapped, including what is already resolved (with the
> resolving document cited) and everything still open, risky, or contradictory.

---

## 1. Method & inputs

This analysis was produced by reading, in full:

**Specs (Oleksii owns; source of truth):**
`System-Overview.md`, `Data-Flow.md`, `Configuration_Specification.md`, `Schema_Change_Detection_and_Evolution.md`.

**Research (Claude authors; Oleksii reviews):**
`Type_Mapping.md`, `Catalog_Auth_Research.md`, `Clustering_Architecture_Research.md`,
`Snapshot_Resumability_and_Consistency.md`, `Ghost_Row_Dedup_and_Primary_Keys.md`,
`Delivery_Guarantee_and_Position_Guard.md`, `Pipeline_Lifecycle_State_Machine.md`,
`Postgres_Slot_and_Publication_Lifecycle.md`, `MySQL_DDL_Classifier.md`,
`Iceberg_Maintenance.md`, `Iceberg_Partitioning_and_Layout_Evolution.md`,
`Large_Transaction_Handling.md`, `Observability_Metrics_and_Tracing.md`,
`Operational_and_Scope_Gaps_Batch.md`, `API_RBAC_and_Versioning.md`.

**Reference (read-only):** `Sources_Index_v3.md` (50-source index; crate versions & external protocols).

Each domain section below states the **design state**, then a **gap table**. A "gap" here is any
question whose answer the application specification will need: an open decision, an unverified
external dependency, a contradiction between two documents, an unhandled edge case, or spec text
that no longer matches the agreed design.

---

## 2. Legend

### Severity
- 🔴 **Blocker** — implementation/spec of the affected component cannot responsibly start until this is closed.
- 🟡 **High** — a wrong or missing answer causes correctness bugs or data loss in some path.
- 🟢 **Medium** — degrades performance, operability, or document coherence; not a correctness risk.

### Status
- ✅ **Resolved** — a spec or research document fully closes it; only execution/ratification remains.
- 🔄 **Partial** — design decided, but a sub-question, verification, or spec-merge is still open.
- 🟥 **Open** — no settled answer yet; genuine work for the spec-elaboration phase.
- 🧪 **Verify** — design is settled *on paper* but rests on an unproven external capability (a pinned
  crate version or a cloud-storage guarantee) that must be empirically confirmed before it can be trusted.
- ⏭ **v2** — out of v1 scope: deferred with the Iceberg sink (2026-09-26). Kept, not closed — this is the
  v2 backlog (§26.2).

A 🧪 item is *not* a design gap — it is an execution gate. They are tracked separately (§21) because
they are the items most likely to invalidate a "design complete" claim if they fail.

### ID scheme
Fresh, domain-prefixed, sequential: `DA-<DOMAIN>-<n>`. Domains: `SYS`, `CFG`, `FLOW`, `TYPE`,
`SCHEMA`, `CLUS`, `SNAP`, `DEDUP`, `CAT`, `SINK`, `SRC`, `OBS`, `API`, `OPS`, `DEP`.

---

## 3. Executive dashboard

The design corpus is **substantially complete**. Of the original blocker set, the clustering,
type-mapping, schema-evolution, snapshot, dedup/delivery, lifecycle, slot-lifecycle, catalog-auth,
RBAC, observability, and operational domains are all closed *on paper*. What remains is concentrated
in three buckets:

1. **Empirical capability gates (🧪)** — a handful of external guarantees that must be proven before
   the code that depends on them can be trusted. These are the single biggest risk to a
   "design-complete" claim. See §21.
2. **Spec-merge debt (🟢, mostly)** — a large volume of agreed decisions that live only in research
   docs and have not yet been folded into the authoritative specs (especially `Data-Flow.md`, which
   still describes a superseded snapshot model, and `Configuration_Specification.md` §16's reserved
   field list). See §19.
3. **A short list of genuinely open design questions (🟥)** — mostly scoping/SLA decisions and a few
   newly-surfaced edge cases. See §20 and §18.

### Remaining blockers (🔴)

| ID | Title | Why it blocks |
|---|---|---|
| ~~DA-SINK-1~~ → re-scoped 2026-09-26 | *No longer a system blocker — see the multi-sink note below.* **iceberg-rust cannot commit deletes/overwrites — the MOR-equality-delete sink (and its COW fallback) is not buildable on any published version** | Confirmed 2026-06 on 0.9.1, rechecked 2026-07-22 on 0.10.0, rechecked 2026-08-23 on 0.10.1, **rechecked 2026-09-19 on compiled 0.10.1 and the v0.11.0-rc.1/rc.2 surfaces — still true**, **rechecked 2026-11-13 on the published 0.11.0 and `main` — still true (§66)**: `EqualityDeleteFileWriter` exists but the `Transaction` API is **append-only** (`fast_append` data files + metadata actions; no row-delta/overwrite/rewrite/merge-append). A CDC sink that applies UPDATE/DELETE cannot be built on the current public API. Upstream epic apache/iceberg-rust#2186 is **open**; the §24.2.3 "`OverwriteAction` merged" reading is **retracted** (PR #2185 is open/unmerged — §24.2.4). **The sink spec must gate on a future iceberg-rust version or adopt §24.2.1 option 2–4.** *(G-13 §8; details §24.2.1, rechecks §24.2.2/§24.2.3, latest §24.2.4)* |

> **MULTI-SINK RE-SCOPE (2026-09-26) — no system-level 🔴 remains on paper.**
> `research/Multi_Sink_Architecture_and_Delta_Lake.md` (decisions D-MS.1–D-MS.14 signed off) makes the
> sink pluggable (`sink.type`, required, no default) and adopts **Delta Lake via delta-rs 1.0** as a
> first-class v1 sink: MERGE commits deletes + upserts atomically, which iceberg-rust cannot. DA-SINK-1
> is re-scoped to **DA-SINK-1i — "Iceberg sink, mirror mode" capability**, enforced at pipeline start
> by the per-sink capability assertion instead of blocking the design. The Delta sink's own gates
> (DA-MS-1…DA-MS-4, §25) are **all proven as of 2026-09-26** — including commit atomicity on real S3,
> GCS and Azure (P8); P6 (MERGE cost under random-key churn) added the one scope caveat (D-MS.15). See §25.

> **DA-CLUS-1 (storage-CAS conformance) — CLOSED 2026-06.** Proven against **real S3, GCS, and Azure**:
> S3 T1–T9 (versioned + non-versioned), GCS T1–T10 (incl. the generation-CAS gate T10), Azure T1–T9.
> The design changed under contact with real services — `lock_backend: storage` is a **per-backend
> `LeaseStore`**: OpenDAL for S3, native `google-cloud-storage` for GCS, native Blob-REST+SAS for Azure
> (OpenDAL 0.57 advertises `write_with_if_match` only for S3). One trait, §5.3 protocol unchanged. The
> conformance harness `prototypes/lease-conformance/` is the durable proof. **DA-SINK-1 is now the only
> remaining 🔴** (itself re-scoped 2026-09-26 — see the note above). See §24.1.

Everything else is 🧪 (other capability gates), 🟡/🟢 with a decided design, or 🟥 scoping work that
does not block starting the specification.

> **v1 readiness (2026-09-26) — Iceberg sink deferred to v2.** No 🔴 remains in the sink, cluster or config
> design; all Delta gates are proven on S3, GCS and Azure, and the **source-side crates are proven**
> too (DA-SRC-10/11, same day). The v1 open set: four v1 decisions + MariaDB scope confirmation, the
> DA-SRC-12 payload guard, and the spec debt. See §22 (re-assessment) and §26.
> **Later the same day, all v1 decisions were signed off** (`V1_Scope_Decisions.md`, D-V1.1–D-V1.6 + the
> five deferrals, §26.5). **What remains before the v1 plan is spec debt only.**
> **The spec debt was applied the same day (§26.6): the corpus is ready for v1 implementation planning.**
> **v1 implementation plan drafted in `doc/v1.0/` (roadmap + M0); its spec edits and the product
> rename to `data-avalanche` were applied the same day (§26.7).**

## 39. M8b config generations — the exit (2026-10-23)

M8b (`doc/v1.0/M8b-config-generations.md`) was executed 2026-10-22…23: eight tasks, a review fix
on Task 3 (a removal's teardown checks the lease before every attempt), then a whole-branch
review with no Critical. Its findings were fixed before the PR, in two rounds (the plan's
"Deviations during execution"). The main one was a plan gap: config §9.1 makes an `id` change a
delete, then a recreate, but the new pipeline started beside the removed one's drain and teardown,
on the same PostgreSQL slot when the operator kept it (the slot has no default). Oleksii chose to
sequence it. No spec edit. The E2E suite (39 scenarios) passes on PostgreSQL 18 and MySQL 8.4; the
other CI targets run on the PR.

| ID | Change | Evidence |
|---|---|---|
| DA-OPS-8 | 🔄 (stays): the startup warning is built; the operator docs stay with M11 | `startup.rs` `the_per_node_keys_apart_from_the_generation_are_named` |
| DA-OPS-9 | 🟥 (stays): a removal with no owner is torn down by no one; a removal whose drain was cut off leaves the source too, now pinned | `controller.rs` `a_removal_with_its_drain_cut_off_leaves_the_source` |
| DA-API-10 | ✅ (stays): a reload's restart keeps the desired state | `controller.rs` `a_restart_keeps_a_paused_pipeline_paused`, `a_restart_runs_a_pipeline_in_error_again` |
| DA-API-12 | 🟥 (stays): the reload and status endpoints are unauthenticated until M9 | — |

### 39.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-OPS-10 | An `id` change that keeps its slot, publication, sink or state is sequenced only within one reload, on the nodes that applied it: the added pipeline waits for the removed one's lease. It starts early when a node starts or joins during the wait, when the removal and the add come in two reloads, when the removal's drain was cut off (the slot stays, and `slot_on_orphan: halt` halts on it), or when a later reload reuses the old id while the removal drains (by up to `drain_timeout` + `contended_window`) | 🟢 | M8b's whole-branch review | A removal tombstone in `globals.state` that every node reads, shared with DA-OPS-9's; until then M11's docs: wait for the old lease's release before reusing the id or its resources. **🔄 2026-10-30 (§50):** documented in `configuration/overview.md`; the tombstone stays |


## 40. M9a management API — the plan (2026-10-23)

Oleksii answered Q-M9-A, Q-M9-B, Q-M9-C and Q-M9-2 (2026-10-23, all "Recommended"): M9 split into
M9a (the API) and M9b (the snapshot jobs); a request reaches the owner through storage, with no
proxy; a leave keeps the process up and a shutdown is SIGTERM's path; a snapshot job's id embeds
its pipeline's. Oleksii confirmed D-M9a.4: restart's "`RUNNING` only" is the desired state, so a
restart takes a pipeline out of `ERROR`. M9a's plan (`doc/v1.0/M9a-management-api.md`) was built
task by task in a scratch workspace and replayed from its code blocks to an identical tree. Two
spec patches came with it, approved and applied the same day: System-Overview's endpoint table (validate,
restart, leave, shutdown, `GET /v1/openapi.json`) and Data-Flow's state layout (`status.json`,
`requests.json`).

| ID | Change | Evidence |
|---|---|---|
| DA-API-12 | 🟥 → ✅: the reload and status endpoints sit behind the operator and viewer roles with every other `/v1` route | M9a D-M9a.6; Task 4 (`every_route_turns_away_callers_below_its_role`, `the_routes_take_the_roles_of_api_rbac`) |
| DA-API-11 | 🟥 → ✅: a requested restart takes a pipeline out of `ERROR`, recorded by id and taken at the owner's next poll, so it can't be missed between two polls | M9a D-M9a.4, D-M9a.5; Task 2 (`a_restart_asked_runs_a_pipeline_in_error_again`) |
| DA-API-7 | ✅ (restated): the lifecycle as built — the same state answers 200 unchanged, only a pause of a stopped pipeline is 409, a flush of a paused one is 200 unchanged | M9a D-M9a.3, D-M9a.4; Task 1 (`the_desired_state_moves_as_the_lifecycle_allows`), Task 5 (`pause_stop_and_resume_move_the_desired_state`, `restart_and_flush_are_asked_of_the_lease_holder`) |

Found while planning, and in the plan: a request made between the owner's term start and its
first poll (a renewal interval later) would have been taken as the baseline and lost (D-M9a.5); a
status of the holder's own earlier term would have read as current (D-M9a.2, now in Task 5's
test); and utoipa 6 doesn't build without its `macros` feature (D-M9a.9).

### 40.1 New gaps

None.

## 41. M9a management API — the exit (2026-10-23)

M9a (`doc/v1.0/M9a-management-api.md`) was executed 2026-10-23: eight tasks, review fixes on
Tasks 2, 4 and 6, then a whole-branch review with no Critical. Its five Important findings were
fixed before the PR (the plan's "Deviations during execution"):
- validate of a posted body was an oracle for a guess at a running secret;
- a hanging IdP stalled token checks through the key cache's lock;
- a role-mapping reload dropped the cached keys;
- a namespaced `roles_claim` couldn't be read;
- the OpenAPI "validated" claim was more than the test checked (reworded, DA-API-18).

No spec edit beyond the two approved with the plan. The API scenarios pass on PostgreSQL 18 and
MySQL 8.4; the other CI targets run on the PR.

On 2026-10-24, on Oleksii's request, DA-API-19 (found by the final re-review) was fixed in the same
PR: a posted body's templated pipeline `id` or `defaults.id` is refused with V-055 before any rule
runs, so the resolved value is never echoed in `changes` or used to derive a MySQL `server_id`.
Its review tried every bypass it could name (aliases, merge keys, `defaults`, partial templates,
quoting, tags, several documents) and found none; the yes-or-no verdicts it noted on other
`${VAR}` leaves are DA-API-20.

| ID | Change | Evidence |
|---|---|---|
| DA-API-4 | ✅ (restated): validate never returns the resolved config, and with a posted body it doesn't compare secret or file fingerprints, so it can't confirm a guessed secret; a body can still make the node read files it names (existence and parse only, API_RBAC §2) | M9a D-M9a.7; `config.rs` `a_check_of_a_body_does_not_tell_whether_its_secret_is_the_running_one` |
| DA-API-3 | ✅ (restated): the role binding as built — `iss` always and `aud` when configured, `nbf`, HS* refused, a namespaced `roles_claim` read by its name, settings that don't hold together refuse every request | M9a D-M9a.6; `auth.rs` tests (API_RBAC §3's as-built note) |
| DA-API-12 | ✅ (stays): every `/v1` route behind its least role, the matrix test over every route and role | Task 4, `every_route_turns_away_callers_below_its_role` |
| DA-API-19 | 🆕 → ✅ (2026-10-24): a posted body's templated `id` is refused before any rule runs | `config.rs` `a_posted_body_s_templated_id_is_refused_without_its_value`; `tests/load.rs` (the V-120 clash, the `$${` escape) |

### 41.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-API-13 | A holder that crashed without releasing its lease reads as live until another node takes over: `GET /v1/pipelines/{id}` shows its last status, and a restart or flush answers `202` (harmless: the next term's baseline absorbs it and starts a fresh worker). The answer carries `holder.renewed_at` and `status_written_at`; the list view carries neither | 🟢 | M9a's whole-branch review (Task 5) | A `stale` hint needs a skew-tolerant notion of a live lease (`renewed_at` is the holder's wall clock; controllers judge by versions over local time, Clustering §5.3); until then M11's docs |
| DA-API-14 | A node that left the cluster through the API stays up with the auth of its last generation: its coordinator has stopped, so a later reload's `role_mapping` or credentials don't reach it, and its mutating routes still write the desired state | 🟢 | M9a's whole-branch review | Answer the mutating routes `503` "this node left the cluster", or document it in M11. **✅ 2026-10-30 (§50, documented):** `runbooks/failover.md` |
| DA-API-15 | A restart or flush gets a new request id per HTTP call: a client that retries after losing the answer asks twice, and the owner may restart twice (an extra replay, no loss) | 🟢 | M9a's whole-branch review | An optional client-supplied request id, or M11's docs. **✅ 2026-10-30 (§50, documented):** `runbooks/pipeline-commands.md` |
| DA-API-16 | The JWKS and discovery fetches accept `http`, follow redirects, and read an unbounded body | 🟢 | M9a's Task 4 review | A V-rule warning for a non-`https` key URL (as V-052), no redirects, a body cap |
| DA-API-17 | The TLS listener spawns a task per connection for its handshake (each bounded at 10 s); nothing caps the number of connections | 🟢 | M9a's Task 3 review | A connection limit on the listener |
| DA-API-18 | The OpenAPI document is snapshot-tested and read back by utoipa, not validated against the OAS 3.1 schema | 🟢 | M9a's whole-branch review | A dev-check against the vendored OAS 3.1 schema |
| DA-API-19 ✅ 2026-10-24 (§41) | Validate of a posted body echoes an added pipeline's resolved `id` and `name`: a body with `id: ${VAR}` shows the value of any node environment variable that fits V-055's pattern (`^[a-z0-9][a-z0-9_-]{0,62}$`, lowercase hex tokens included), and `db: ${VAR}` tells whether a variable equals the running value. Reload reads only the node's file, so only validate is open to it | 🟡 | M9a's final re-review (outside the fix diff) | Resolve a posted body's `${…}` in identity and other echoed fields as written (no expansion), or refuse `${…}` outside credential fields in a posted body; until then API_RBAC §2's note |
| DA-API-20 | Validate of a posted body still checks its other `${VAR}` leaves, so a verdict answers yes or no about a node variable: V-063 whether it is set; V-058, V-059 and V-120 whether it equals another pipeline's slot, publication or `server_id`; a pattern or range check its shape; the diff's field paths whether it equals the running value. One bit per request, online guessing only; no value is echoed. Since 2144392 every validate is logged with its caller, its source, the body's size and the answer's status | 🟢 | DA-API-19's review (2026-10-24) | M12: a rate limit on validate (Oleksii's choice, 2026-10-24; roadmap §4 M12) |

## 42. M9b snapshot jobs — the plan (2026-10-27)

Oleksii answered Q-M9b-1…4 (2026-10-27; three "Recommended", Q-M9b-4 not) and approved the plan
and its spec patches the same day. A job is asked as a restart under its id, and the new worker's
Phase 0 takes its tables. A job upserts, with no truncate. A table outside the config is copied,
not streamed. A job is asked only of a running pipeline whose lease a node holds, `ERROR`
included. M9b's plan (`doc/v1.0/M9b-snapshot-jobs.md`) was built task by task in a scratch
workspace and replayed from its code blocks to an identical tree. The spec patches, applied the
same day: System-Overview's request-snapshot row, and Data-Flow's On-Demand Snapshot Flow and
the job document's fields.

| ID | Change | Evidence |
|---|---|---|
| DA-SCHEMA-10 | 🆕 → 🔄: fixed in the plan (Task 2), ✅ when M9b merges | M9b D-M9b.10; `tests/mysql/schema.rs` `a_baseline_takes_the_whole_table_from_the_catalog`; the E2E halt scenario on `mysql-84`, `mysql-84-nogtid`, `mariadb-118` |

Found while planning, and in the plan: MySQL's channel C refused the column hint every baseline
passes, so a MySQL table whose columns changed while it couldn't be replayed halted `UNSAFE_DDL`
instead of being baselined (DA-SCHEMA-10, since M7b); and the kill scenario's first draft would
have passed against a job already ended.

### 42.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SCHEMA-10 | MySQL's channel C refused a `SchemaHint::Columns` (`a column hint on a MySQL source`), and Phase 0's baseline passes one: a fresh start (a purged binlog, an adopted orphan) over a MySQL table whose columns changed halted it `UNSAFE_DDL` instead of baselining it (Schema_Change §8.2), and so would every job that clears a halt | 🟡 | M9b's planning E2E (the halt scenario on every MySQL target) | Fixed in M9b Task 2: a column hint is the whole table, no rename proven (`Scope::Table`) |
| DA-SNAP-15 | A job upserts (Q-M9b-2): a row deleted at the source while its table was halted or not streamed stays in the lake (a ghost row), and a job doesn't remove it | 🟡 | Q-M9b-2 | M11's docs: drop or move the Delta table before the job; a truncate option (a metadata-only delete before the first chunk, reconciled with the stream) later. **🔄 2026-10-30 (§50):** documented in `architecture/snapshots.md`, `runbooks/snapshot-jobs.md`, `runbooks/halted-table.md`; the truncate option stays open |
| DA-SNAP-16 | Job documents accumulate: nothing removes an ended one, and the list route and the ask's `409` check read every job of the pipeline | 🟢 | M9b's planning | A retention (by count or age), or an index of the unfinished jobs |
| DA-SNAP-17 | A job table that halts while its snapshot runs fails in the job only once the halt reaches `halts.json`, with the session's next flush | 🟢 | M9b's planning (Task 5's test commits to another table to flush) | Write a halt of a job table at once, or M11's docs. **✅ 2026-10-30 (§50, documented):** `runbooks/snapshot-jobs.md` |

---

## 43. M9b snapshot jobs — the exit (2026-10-28)

M9b (`doc/v1.0/M9b-snapshot-jobs.md`) was executed 2026-10-27…28: seven tasks, review fixes on
Tasks 3 and 7, then a whole-branch review with no Critical. Its three Important findings were
fixed before the PR (the plan's "Deviations during execution"):
- the owner read the halts before the job, so a halt the job's start had just cleared could fail
  that re-snapshot;
- a copy's halt stayed in `halts.json`, the status and the metric, and for a dropped table no job
  could clear it;
- a copy's chunk read that failed for good (no `SELECT` on a table outside the config) put the
  pipeline in `ERROR` at every restart while the job ran.

No spec edit beyond the two approved with the plan. 1289 workspace tests; the job scenarios pass
on PostgreSQL 18, `mysql-84`, `mysql-84-nogtid` and `mariadb-118`, and the E2E suite (45
scenarios) on PostgreSQL 18, at the review fixes' head.

| ID | Change | Evidence |
|---|---|---|
| DA-SCHEMA-10 | 🔄 (stays): built in Task 2, ✅ when M9b merges | `tests/mysql/schema.rs` `a_baseline_takes_the_whole_table_from_the_catalog`; the E2E halt scenario on every MySQL target |
| DA-SCHEMA-10 | 🔄 → ✅ 2026-10-28: M9b merged (PR #14, `90b114c`) | PR CI run 40664901310, green on `mysql-84`, `mysql-84-nogtid` and `mariadb-118` |

### 43.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SNAP-18 | A table dropped and created again while the pipeline was down, with no halt recorded, takes two jobs: the first halts it `TABLE_DROPPED` at its start and fails it, the second clears the halt | 🟢 | M9b Task 3's review (40180b1) | M11's docs; or halt and clear in one pass, once replaying the old table's events after the checkpoint against the new schema is proven safe. **✅ 2026-10-30 (§50, documented):** `runbooks/halted-table.md` |
| DA-SNAP-19 | A copy that halts in the session shows among the halted tables (the status, the `halted_tables` metric) until the pipeline's next start clears it | 🟢 | M9b's whole-branch review | Clear a copy's halt when the owner fails its job table, or keep copies out of the halt registry |
| DA-SNAP-20 | A job document that can't be decoded (`deny_unknown_fields`, e.g. after a downgrade) stops the pipeline at its start (`Corrupt` → `ERROR`), and the routes answer 503 | 🟢 | M9b's whole-branch review | Skip an undecodable job at the start and in the owner's poll, with a warning |
| DA-SNAP-21 | A job whose restart request wasn't written (a failed write after the job's, or a crash between them) stays queued until the pipeline's next start, and its tables answer `409` meanwhile; the `409` says so | 🟢 | M9b Task 6's review | Write the restart again under a queued job's id when it is asked again |

## 44. M10 observability — the plan (2026-10-28)

`doc/v1.0/M10-observability.md`, approved 2026-10-28 with its two spec patches (System-Overview
§Observability catalogue: the definitions, `per_table` on the snapshot series only, OTLP/HTTP;
config §6.6: the endpoint's base URL and port 4318). Nine tasks: the catalogue's missing series,
the worker's events and status, MySQL's binlog lag, the controller, OTLP traces, the binary's
logger, example alert rules, the E2E against a Jaeger container. The owner's answers are
Q-M10-1…4; the rulings D-M10.1…16 are in the plan (roadmap D-IMPL-21).

| ID | Change | Evidence |
|---|---|---|
| DA-OBS-6 | 🟥 (stays): fixed by M10 Task 7 (colour only when stderr is a terminal, `NO_COLOR` kept); ✅ when M10 merges | `logging.rs` `a_text_log_is_in_colour_only_when_asked`, `colour_needs_a_terminal_and_no_no_color`; the MySQL E2E halt scenarios search the log as written |
| DA-OBS-1…4 | ✅ (stay): built as designed, with the definitions the spec now carries | the catalogue test; the tracing E2E (`the_flushes_commits_and_chunks_are_traced_to_the_collector`) |

### 44.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-OBS-7 | The OTLP export sends no headers: a collector that wants a token or an API key (a hosted backend) can't take the traces; an `https` endpoint is untested | 🟢 | M10 planning | `globals.tracing.headers` (secret-resolved, R), and an `https` IT; after v1 unless a deployment needs it |

## 45. M10 observability — the exit (2026-10-29)

M10 (`doc/v1.0/M10-observability.md`) was executed 2026-10-28…29: nine tasks, review fixes on
Tasks 2, 3, 4, 6, 7 and 9, then a whole-branch review with no Critical. Its two Important
findings were fixed before the PR (the plan's "Deviations during execution"):
- a failed OTLP export logged nothing (the SDK's `otel_error!` is a no-op without
  `internal-logs`), against config §6.6's run-time warning; it is now warned of, rate-limited,
  without the endpoint's credentials;
- `replication_lag_seconds` froze while no worker ran and dropped to `0` at the next worker's
  start; the controller now keeps the lag's start across a restart within the lease term.

One spec edit beyond the two approved with the plan: System-Overview's `binlog_lag_seconds` of an
idle stream (Task 4, Oleksii's ruling, approved and applied). 1332 workspace tests; the
observability scenarios pass on PostgreSQL 18, `mysql-84`, `mysql-84-nogtid` and `mariadb-118`,
and the E2E suite (46 scenarios) on PostgreSQL 18.

| ID | Change | Evidence |
|---|---|---|
| DA-OBS-6 | 🟥 (stays): fixed by Task 7 (0859843); ✅ when M10 merges | as §44 |
| DA-OBS-6 | 🟥 → ✅ 2026-10-29: M10 merged (PR #15, `550474c`) | PR CI run 40799494685, green on every target |

### 45.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-OBS-8 | `replication_lag_seconds` reads `0` early in two windows until the change is read again: a restarted worker that makes its first re-read change durable before re-reading the next, and a source reconnect inside one worker (its new session starts with nothing pending) | 🟢 | M10's whole-branch re-review | Keep the controller's kept start until the worker's frontier covers it; carry the session's pending start across a reconnect |
| DA-OBS-9 | The JSON log's `trace_id`/`span_id` are tested under a scoped dispatch only; the binary's global dispatch (`set_global_default`, the weak dispatch) has no end-to-end test | 🟢 | M10's whole-branch review | In the Jaeger E2E, log in JSON and match a line's `trace_id` to a trace the collector returned |

## 46. M11a packaging — the plan (2026-10-29)

`doc/v1.0/M11a-packaging.md`, approved 2026-10-29 with no spec edit; M11 split in two (Q-M11-A),
M11b (the documentation site) to follow. Four tasks: the image on distroless `cc` (Debian 13, as
65532, amd64 and arm64; Q-M11-3), the Helm chart (a StatefulSet named for the ring, DNS discovery,
the config a map or text), the smoke deploy on test-infra's `k3s` service (Q-M11-4), and the
release to GHCR on a `v*` tag (Q-M11-1). The rulings D-M11a.1…9 are in the plan (roadmap
D-IMPL-22).

### 46.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-OPS-11 | The release workflow's first full run is the first tag's (M12's `v1.0.0`): it can't run before it is on `main`, and until then only its builds are proven (the amd64 image per PR, the arm64 one weekly). Its first push creates the GHCR packages private | 🟢 | M11a planning | A manual run on `main` after M11a merges (builds both images, packages the chart, pushes nothing); after the first tag, make the image and chart packages public in the repository's package settings |

## 47. M11a packaging — the whole-branch review (2026-10-29)

M11a was executed 2026-10-29: four tasks, one fix round (Task 4: the SBOM step's GHCR login), and a
whole-branch review whose three Important findings were fixed before the PR (the plan's
"Deviations during execution"): the chart is pushed after the image's tags are joined, the
attestation runs only on a public repository, and the `k3s` job's timeout is 120 minutes (the
`type=gha` cache keeps the image's layers, not the Dockerfile's cache mounts). No spec edit.

- **DA-OPS-11** (🟥, extended): the repository is private. GitHub offers artifact attestations to
  a private repository only on Enterprise Cloud, so `release.yml` skips the attestation there; the
  image keeps BuildKit's in-registry provenance (`mode=max`) and the SBOM. Whether the
  `ubuntu-24.04-arm` runner serves a private repository is unproven until `weekly.yml`'s
  `image-arm64` is dispatched. Oleksii's ruling (2026-10-29): the repository goes public, which
  turns on the attestation and the free arm64 runner and lets a `helm install` pull the packages
  without `imagePullSecrets`, after a root README warns that it isn't ready for production.

### 47.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-CLUS-15 | DNS discovery asks its seeds again only while a node has no peer (`discovery.rs`, M8a). With four or more pods started together (the chart's `podManagementPolicy: Parallel`), nodes that each resolved some, not all, of the others can settle in groups that never merge: each has a peer, so none asks again. Each group's ring places every pipeline: the storage lease keeps one holder per pipeline, but the groups contend for the leases and a group's failover can't see the other's nodes | 🟢 | M11a's whole-branch review (declined to judge: M8 code) | Ask the seeds again on a timer while the DNS name returns an address that isn't a member, or join every resolved address each round; the k3s scenario with four pods |

## 48. M11a packaging — the exit (2026-10-29)

M11a (`doc/v1.0/M11a-packaging.md`) merged 2026-10-29 (PR #16, `7f57d98`), its PR CI green on
every target (run 40903743634; the `k3s` job 41 min with the image's cold build). The arm64 image
built on `ubuntu-24.04-arm` (`weekly.yml` dispatched on the branch, run 40903481644, 33 min). The
repository went public the same day, after a root README warning that it isn't ready for
production (PR #17, `fe07e19`), so `release.yml`'s attestation runs on a tag.

| ID | Change | Evidence |
|---|---|---|
| DA-OPS-11 | 🟥 (stays): the manual run on `main` done, both images built and the chart packaged, nothing pushed; the repository public, so the first tag attests the image. What stays: the GHCR image and chart packages, created private by the first tag's push, made public in the package settings | `release.yml` run 40918381277 (17 min) |

## 49. M11b documentation — the plan (2026-10-29)

`doc/v1.0/M11b-docs.md`, approved 2026-10-29 with no spec edit. Four tasks: the config crate lists
and documents its keys (`config_keys()`, `FIELD_DOCS`); `cargo xtask docs` renders the
configuration reference, the validation rules and the API reference from the code; a Starlight
site (Q-M11b-1, `research/Documentation_Site_Generator.md`) of 25 hand-written pages with its links
validated at build, every config example validated, deployed to GitHub Pages from `main`; and the
doc obligations below closed in the tracker. The rulings D-M11b.1…11 are in the plan (roadmap
D-IMPL-23). The planning ran the getting-started walk-through on test-infra's PostgreSQL 18 and
RustFS, and the pages say what it showed: a managed publication needs the tables' owner, a DBA's
slot is an orphan at the first start (`slot_on_orphan: adopt_resnapshot`), and a reload doesn't
restart a pipeline in `error`. No spec edit: a suspected `:4317` in the config spec's §14 example
was a stale reading (it has said OTLP/HTTP's `:4318` since M10, `c418c8a`).

### 49.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-OPS-12 | A process-restart key written as a literal can't change once the cluster has a config generation: a reload refuses it (V-061, `diff.rs`), and a restarted node adopts the stored generation (`startup.rs`). Only a `${VAR}` value, which each node resolves from its environment, or a referenced file's contents change, by a rolling restart. Stuck that way: `lease_renewal_interval`, `lease_safety_margin`, `discovery`, `discovery_dns_name`, `join_members`, the memberlist timers, `globals.api.tls.enabled`, `globals.azure.account_name` (`tokens`, `lock_backend` and `lease_duration` are pinned in the anchor by design) | 🟢 | M11b planning (the reload runbook) | A path for such a change (a generation published with every node's agreement, or an operator command that re-bootstraps the generation); until then the docs: write them as `${VAR:-default}`, else a new `globals.state` with every node stopped |
| DA-OPS-13 | Two operator-facing texts are wrong: the orphan-slot refusal says `slot.on_orphan: adopt_resnapshot`, but the key is `source.postgres.slot_on_orphan` (`slot.rs`; seen in the planning run); the CLI's `about` says "from PostgreSQL into Delta Lake", leaving out MySQL and MariaDB (`cli.rs`) | 🟢 | M11b planning | Two one-line fixes (M12, or a fix PR) |
| DA-SRC-29 | `BINLOG_SOURCE_CHANGED` without GTIDs has no in-place recovery: its message says "switch to replication_type: gtid or re-snapshot", and `same_server` doesn't follow `binlog_purge_action`. (A pipeline in `error` keeps its lease and its desired state `running`, so a snapshot job is accepted (`snapshots.rs`) and its restart clears the error; `same_server` then halts it again with `BINLOG_SOURCE_CHANGED`, so the gap stands) | 🟢 | M11b planning (the lost-position runbook) | `binlog_purge_action: re_snapshot` to cover a changed server too, or a re-seed command for a pipeline in `error`; until then the docs: switch to `gtid` with `re_snapshot`, or replace the pipeline (a new `id` and prefix) |

## 50. M11b documentation — the pages (2026-10-30)

M11b's documentation site (`docs/`, Starlight, Q-M11b-1): 25 hand-written pages plus the generated
reference, links validated at build, every config example validated by a test, deployed to
`https://alex-karpenko.github.io/data-avalanche/` from `main` by `docs.yml` after the merge. The
pages close the doc obligations the tracker carried for M11:

| ID | Change | Evidence |
|---|---|---|
| DA-OPS-8 | 🔄 → ✅: the operator docs say a node runs the generation, per-node values go through `${VAR}`, the gossip addresses are the generation's, so a literal there is ignored with a warning (`node_name` and `api.address` are the node's file's), and a reload from that node is refused | `configuration/cluster.md`, `runbooks/config-changes.md` |
| DA-OPS-10 | 🟥 → 🔄: the docs say to wait for the old lease's release before reusing an `id` or its resources, or to do both in one reload; the tombstone stays | `configuration/overview.md` |
| DA-API-14 | 🟥 → ✅ (documented): a node that left keeps its last generation's auth and still takes commands; stop it soon after | `runbooks/failover.md` |
| DA-API-15 | 🟥 → ✅ (documented): a retried `restart` or `flush` asks twice, which costs a replay, never data | `runbooks/pipeline-commands.md` |
| DA-SNAP-15 | 🟥 → 🔄: the docs say a job upserts, rows deleted meanwhile stay, drop or move the table first; the truncate option stays open | `architecture/snapshots.md`, `runbooks/snapshot-jobs.md`, `runbooks/halted-table.md` |
| DA-SNAP-17 | 🟥 → ✅ (documented): a job table's halt fails the job at the next flush | `runbooks/snapshot-jobs.md` |
| DA-SNAP-18 | 🟥 → ✅ (documented): a table created again while the pipeline was down takes two jobs | `runbooks/halted-table.md` |
| DA-CLUS-15 | 🟥 (stays): the docs warn and give the check (`GET /v1/cluster/status` on each node) and the remedy (restart the smaller group's pods); the fix stays in the code | `configuration/cluster.md`, `runbooks/failover.md` |

In the roadmap's risk table (§6), R-5 now points at `configuration/sink.md` (the flush guidance)
and R-12 at `configuration/storage.md` (the non-ASCII limit) and `configuration/sink.md` (the date
clamp).

The whole-branch review corrected the recovery pages: a recovery's snapshot follows
`snapshots.mode` (under `never`, the default, none is taken), and the spec was patched to match
(config spec §15 item 29).

### 50.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SRC-30 | Under `slot_management: require_existing` a lost slot (invalidated or missing) has no in-place recovery: a slot the DBA creates again has a `confirmed_flush_lsn` past the pipeline's checkpoint, so it is lost again and refused again (`slot.rs`), and the refusal's text ("recreate it and restart: the pipeline re-snapshots") says otherwise | 🟢 | the M11b whole-branch review | The refusal's text (with DA-OPS-13), and a way to adopt a DBA's new slot (for example the orphan path for a slot past the checkpoint); until then the docs: drop the slot, set `managed` in one reload, then `require_existing` again (`runbooks/position-lost.mdx`) |


## 51. M11b documentation — the exit, and v0.1.0 (2026-10-30)

M11b (`doc/v1.0/M11b-docs.md`) merged 2026-10-30 (PR #18, `554e5a9`). Its PR CI was green on every
target (`ci.yml` run 41020210010; `docs.yml` run 41020209936). The `k3s` job's first run had found
the image's build context missing `xtask/`, which was fixed before the merge (`110d951`, with a
packaging test). The site deploys from `main` (`docs.yml` run 41025275696) to
<https://alex-karpenko.github.io/data-avalanche/>. `v0.1.0` was tagged on `554e5a9` after a manual
`release.yml` run on `main` (41031315279), and released (run 41034298202). The release published:
- the image `0.1.0`, `0.1` and `latest`, for amd64 and arm64;
- its build-provenance attestation, which `gh attestation verify` accepts;
- the chart `oci://ghcr.io/alex-karpenko/charts/data-avalanche:0.1.0`;
- the GitHub release.

Both packages are public: an anonymous pull of either manifest answers 200.

| ID | Change | Evidence |
|---|---|---|
| DA-OPS-11 | 🟥 → ✅: the release's first full run, on `v0.1.0`; the packages public | `release.yml` run 41034298202; `gh attestation verify oci://ghcr.io/alex-karpenko/data-avalanche:0.1.0` |

## 52. M12a source hardening — the plan (2026-10-30)

M12 is split in three (Q-M12-4): M12a, source correctness and recovery; M12b, cluster, control, API
and observability hardening, with `cargo audit` and the compile time; M12c, the v1 acceptance and
`v1.0.0`. Every open gap is processed in M12, fixed or closed with a pointer (Q-M12-1).
`doc/v1.0/M12a-source-hardening.md` is M12a's plan: twelve tasks for the source gaps below, and a
spec patch (config §9.1, §9.2.1, §9.2.2, V-123, V-124; Data-Flow Phase 2a/2b, the sink contract's
`reposition`), approved with the plan. The rulings (Q-M12a-1…4, D-M12a.*) are in the plan, the
milestone's (Q-M12-1…7) in the roadmap.

The planning measured:
- **A PostgreSQL 18 promotion** (a `pg_basebackup` standby): the promoted server keeps the
  primary's system identifier and moves to timeline 2 at once; the logical slot isn't on it (no
  slot sync). So the checkpoint records both, and a recovery moves every table's marker onto the
  new server (the guard keeps the union of positions, which skips a lagging replica's LSNs
  otherwise). The failover E2E fails without the reset (the guard skips the streamed row).
- **MariaDB 11.8 rollbacks** in ROW format: a transaction with a non-transactional table's rows
  logs those first, in a group of their own; the transactional group, without `FL_TRANSACTIONAL`,
  logs the undone rows, then ``ROLLBACK TO `s` `` or `ROLLBACK`. A group of InnoDB tables alone
  never logs undone rows.
- **`BINLOG_GTID_POS(file, pos)`** on MariaDB: NULL for an offset off an event boundary, a missing
  file or an offset past the end; a value at a rotated file's end.

The rows below were already closed elsewhere and their dated rows never updated; they are now (no
count changes, none was counted open):

| ID | Change | Evidence |
|---|---|---|
| DA-SYS-7 | §26.3's row 🆕 🟥 → ✅ | §4 (D-V1.4, 2026-09-26); `wiring.rs` refuses `sink.type: iceberg` naming v2 |
| DA-SRC-12 | §26.3's row 🆕 🟥 → ✅ | §14 (D-V1.6); M7a's payload guard, M7b's E2E (§35) |
| DA-SRC-16 | §27.1's row 🆕 🟥 → ✅ | §14 and §30 (M6b Task 22) |

**Recount.** The open set was 35: the 34 🟥 rows of the domain tables (§4–§18) and DA-X-13, which
has only its §32.1 row. The dated sections' stale rows above had made a count of the table rows
read 37. With DA-X-15 (§52.1) it is 36.

**What M12a takes** (each closed with a pointer at its exit):

| ID | Task |
|---|---|
| DA-SRC-20 | 1–2: the PostgreSQL checkpoint records the server, `WAL_SOURCE_CHANGED`, `source.postgres.source_change_action`, the markers moved onto the new server |
| DA-SRC-29, DA-SRC-21 | 3: a changed MySQL server follows `binlog_purge_action`; MariaDB's position checked by `BINLOG_GTID_POS` |
| DA-SRC-30, DA-OPS-13 | 4: a DBA's slot made again is adopted under `slot_on_orphan: adopt_resnapshot`; the refusal's key and the CLI's `about` |
| DA-SRC-26 | 5: a MariaDB rollback drops the rows it undid |
| DA-SRC-24, DA-SRC-22 | 6: a Galera node, `verify-ca` on MySQL (V-124, fails closed; the `mysql_async` fix re-checked at each upgrade). DA-SRC-25 (a case-only rename) was withdrawn in the task's review and stays open (§53) |
| DA-SRC-14, DA-SRC-19 | 7: `bit`/`varbit` keys refused; a table's chunk-read error halts the table |
| DA-SNAP-13, DA-SNAP-14, DA-SRC-15 | 8: a wiped state baselines against the sink table; an `ENUM` key reordered; the collation edges |
| DA-SRC-13 | 9: the publication's blind spots |
| DA-SRC-17, DA-SRC-9 | 10: the small PostgreSQL edges; one catalog connection serves the re-queries |
| DA-SRC-18 | 11: the verifier and `require` tests, a composite `date` key, `postgres-tls` on the anchor, the failover E2E on a `replica` compose pair |
| DA-X-15 | 11: the jobs test waits for the copy's completion |

### 52.1 New gap

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-X-15 | `jobs::tests::a_table_outside_the_config_is_copied_and_not_streamed` fails now and then (2 runs in 30 on the base, 7 in 30 beside a build): it drains the worker once the copied rows land, but the copy is marked done only at its last chunk read, an empty one, so a drain in between leaves its snapshot `InProgress` | 🟢 | M12a planning (the gates, under load) | The test waits until the worker reports no snapshot running (30 runs in 30 then); M12a Task 11 |

## 53. M12a source hardening — the gaps closed (2026-11-04)

M12a's tasks (`doc/v1.0/M12a-source-hardening.md`) closed the source gaps §52 named. The
operator pages say what changed: `configuration/sources.md` (a failover, a standby, what the
publication publishes, `verify-ca` on MySQL, MariaDB's savepoints and Galera, the MySQL position
checks),
`runbooks/position-lost.mdx` (a failover, a DBA's slot made again, a renamed publication, a changed
MySQL server), `runbooks/source-privileges.mdx` (a filtered publication, a privilege lost later,
a collation's schema, a Galera node, a standby), `runbooks/halted-table.md` (the new halt causes)
and `runbooks/upgrade.md` (a `bit`/`varbit` key, a standby).

| ID | Change | Evidence |
|---|---|---|
| DA-SRC-20 | 🟥 → ✅ | `slot.rs` `source_change` (`WAL_SOURCE_CHANGED`, `source_change_action`), `worker.rs` `reposition`; E2E `failover.rs` |
| DA-SRC-29 | 🟥 → ✅ | `start.rs` `MySqlConnector::changed` follows `binlog_purge_action`; a checkpoint's file the server never held (`never_held`: another base name, or after its newest binlog) is another server's, not a purge; found at a reconnect, `SourceError::SourceChanged` is `Retry`, so the pipeline reruns its start; ITs `a_checkpoint_from_another_server`, `a_file_the_server_could_not_have_purged_is_another_server_s` |
| DA-SRC-21 | 🟥 → ✅ | `start.rs` `at_boundary` (`BINLOG_GTID_POS`) and `never_held`, one `foreign_position` at the start and at the dump's open (and so at a reconnect, which follows `binlog_purge_action` through `SourceError::SourceChanged` → `Retry`); IT `a_mariadb_position_off_an_event_boundary_is_another_server_s`. Left: a server with the same `server_id` and binlog file names whose file has an event boundary at the offset can't be told apart (`gtid` mode doesn't depend on it) |
| DA-SRC-30 | 🟥 → ✅ | `slot.rs` `decide`: adopted under `slot_on_orphan: adopt_resnapshot`, else refused with the key named; IT `require_existing_never_creates` |
| DA-OPS-13 | 🟥 → ✅ | `slot.rs` names `slot_on_orphan`; `cli.rs` `about` names the three sources (`the_help_names_every_source`) |
| DA-SRC-26 | 🟥 → ✅ | `stream.rs` `Held` (savepoints), `roll_back`: a rollback drops the changes it undid and keeps the halts it covered; a `ROLLBACK TO` a savepoint the stream didn't see halts (when changes are held); the hold counts decoded bytes against `sink.max_transaction_buffer_bytes`; ITs `a_rollback_drops_the_rows_it_undid`, `a_rollback_past_the_hold_limit_halts` |
| DA-SRC-24 | 🟥 → ✅ | `server.rs` `check_coverage` refuses a Galera node without `log_slave_updates` |
| DA-SRC-25 | 🟥 (stays) | withdrawn in Task 6's review: `keep_names` leaked into the snapshot's column match and the worker's drift check; a case-only rename halts the table, as before M12a |
| DA-SRC-22 | 🟥 → ✅ | documented, fails closed: V-124 warns on `verify-ca` for MySQL/MariaDB; `connect.rs` names the re-check at each `mysql_async` upgrade (pinned by `verify_ca_by_an_unnamed_address_is_refused_like_verify_full`) |
| DA-SRC-14 | 🟥 → ✅ | `catalog.rs` refuses a `bit`/`varbit`/PostGIS key (`IDENTIFIER_NOT_USABLE`) unless read as text (`column_type_overrides: string`); the Delta column then changes from `binary` to `string` (`runbooks/upgrade.md`) |
| DA-SRC-19 | 🟥 → ✅ | `snapshot.rs` (both sources) `table_scoped`: a table's SQLSTATE / MySQL error halts the table (`TABLE_NOT_REPLICABLE`), in the chunk read only; a `re_query` fetch's permanent error ends the session (`a_requery_error_of_the_table_ends_the_session`). Oleksii's trade-off (D-M12a.8): 42501, 1142 and 1143 stay table-scoped, so a role-wide or schema-wide revoke halts every table, each copied again once the grant is back. **Measured 2026-11-04 (§54, M-5):** on MySQL 8.4 and MariaDB 11.8 a lost table `SELECT` with no other privilege on the table halts it `TABLE_DROPPED`, not `TABLE_NOT_REPLICABLE`: `information_schema` hides the table, so the start doesn't list it, the rescan reports it dropped and the chunk's read plan finds no table; with another privilege kept (an `INSERT`) the read fails 1142, `TABLE_NOT_REPLICABLE` (IT `a_lost_select_halts_the_table_as_dropped_unless_another_privilege_shows_it`; `runbooks/source-privileges.mdx`, `runbooks/halted-table.md` say so). A grant probe that tells the two apart is DA-SRC-36 |
| DA-SNAP-13 | 🟥 → ✅ | `schema_change.rs` `missing_columns`, the worker's baseline against the sink table; `a_wiped_state_baselines_against_the_sink_table`; a sink column matching a source column by case alone halts `SCHEMA_MISMATCH` (Delta names are case-insensitive): `a_sink_column_matching_by_case_alone_halts` |
| DA-SNAP-14 | 🟥 → ✅ | MySQL `snapshot.rs` `moved`: a reordered, removed or renamed `ENUM`/`SET` key member is a change under the snapshot (the stream halts the table `UNSAFE_DDL`) |
| DA-SRC-15 | 🟥 → ✅ | `KeyCollation::Own` (a domain's collation, schema-qualified), the index-collation warning, `limit 0` refused; the byte budget documented (`chunk_size` per table), `E''` literals unreachable (`standard_conforming_strings` pinned); the `re_query` re-read uses the key's collation (`a_domain_key_declared_with_another_collation_is_re_read`) |
| DA-SRC-13 | 🟥 → ✅ | `slot.rs` `publication_shape`: under `require_existing` or `FOR ALL TABLES` a tracked table published with a row filter or a column list halts itself (`halt_filtered_tables`, `TABLE_NOT_REPLICABLE`: publish it whole in one transaction, then re-snapshot it) and the other tables go on; stored generated columns are refused pipeline-wide; under `managed` the publication is set to exactly the tracked set (a partitioned root or a `TABLES IN SCHEMA` member is replaced by the tracked tables, a table published before is added by name without a copy); extra tables warned (a managed one drops them); a change to what a managed publication publishes of a tracked table (a filter, a column list, a partition-root flag, the operations, stored generated columns) recreates the slot and re-snapshots the pipeline's tables as `snapshots.mode` says (`prepare`, `rewritten`); ITs `a_publication_s_blind_spots_are_refused_warned_or_repaired`, `a_managed_publication_of_a_partitioned_root_is_set_to_the_tracked_leaves` |
| DA-SRC-17 | 🟥 → ✅ | fixed: the table-set errors all reported, the server's detail and hint, a standby refused on every version (decoding on one is later work, DA-SRC-31), the bounded call's one deadline, a date or time past PostgreSQL's range refused (the `civil_days` bound, BC included); the rest are not reachable from a real server (PostgreSQL doesn't produce them): loose parsing, hstore/PostGIS matched by name, cyclic domains in type planning, `convert` without a scale guard, a `string` override on a `"char"` above 127 on PG 14, `char(n)` padding under `string`, an interval's fields narrowed, the dropped-orphan retry reporting `FirstStart` (D-M12a.13); and by design: a glob table set `UNLOGGED` mid-resolve halts (fails closed: PostgreSQL doesn't replicate an unlogged table, `catalog.rs`), and the catalog driver's errors now reach the error text with the server's detail and hint (`db_message`), though the crate logs nothing of its own |
| DA-SRC-18 | 🟥 → ✅ | `tls.rs` verifier tests, `require_encrypts_both_connections_without_verifying`, `keys.rs`, `postgres-tls` on `x-pg`, the `replica` profile and `failover.rs` |
| DA-X-15 | 🆕 → ✅ | `jobs.rs` `a_table_outside_the_config_is_copied_and_not_streamed` waits for the copy's completion |
| DA-SRC-9 | 🔄 → ✅ | decided: one catalog connection per pipeline serves channel C, the chunks and the re-queries (batched per commit); V-077 warns; throughput measured in M12c |

### 53.1 New gap

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SRC-31 | Decoding on a PostgreSQL standby (16 and later) | 🟢 | M12a Task 10 (Oleksii's ruling, 2026-11-04) | v1 refuses a server in recovery (Configuration §11.2): the source reads `pg_current_wal_lsn()`, which a standby refuses, for the snapshot watermark, the lag read and the orphan advance, and may change the publication, which a read-only standby can't. Decoding on a standby needs the primary's position for those reads, and a publication made on the primary. Later work |

## 54. M12a whole-branch review (2026-11-04)

The whole-branch review of M12a (`554e5a9..aab0e3b`) found no Critical issue, one Important and
ten Minors. Oleksii approved the Important fix with its spec lines (Data-Flow Phase 2a, Source
Disconnect, Phase 4's reset scope; `c2a9395`). Fixed before the PR:

- **I-1, a PostgreSQL stream reconnect never re-read the server.** A failover while the pipeline
  ran could reach a server with a slot of the pipeline's name that can't serve the position: the
  `PositionUnreachable` path dropped and made the slot there, with no reposition, and the guard
  skipped the promoted server's changes below the lake's markers, whatever
  `source_change_action` said. Now the stream's open (`PgConnector::open_stream_at`) and the reset
  of a lost position (`reset_position(lost)`) read the server first (`slot.rs` `reconnect_check`):
  another system or timeline halts `WAL_SOURCE_CHANGED` under `error`, the start's halt, and asks a
  rerun of Phase 0 under `re_snapshot` (`SourceError::SourceChanged` → `Retry`), before a slot is
  touched. MySQL's `reset_position` ignores the lost checkpoint's server (a GTID set travels; file
  mode checks the server at the dump's open). Tests: the worker's
  `a_failover_mid_run_reruns_phase_0_instead_of_resetting_the_slot`,
  `a_failover_mid_run_halts_under_error`, `a_reconnect_to_another_server_reruns_phase_0`; the
  source-pg IT `a_reconnect_to_another_server_halts_or_reruns_phase_0`; the failover E2E.
- **The Minors:** a same-named slot of another plugin or database refused at a source change too
  (M-3); a `system_identifier` at or past 2^63, which PostgreSQL prints negative, parsed (M-4); the
  stale texts (the orphan refusal under `require_existing`, the `(None, None)` refusal's remedy,
  the advance's label, `Prepared.server`'s doc, the Percona XtraDB comment, `upgrade.md`'s
  re-snapshot condition; M-6); the standby refusal's run of spaces (M-7); `FakeSource` checks a
  PostgreSQL checkpoint's server at every `prepare`, stream open and reset, and D-M12a.2's
  crash-repeat is tested (`a_crash_between_the_resets_and_the_checkpoint_repositions_again`, M-10);
  the binlog-lag test's second bound made relative; M-8's plan row and research §4.
- **M-5, measured:** a lost table `SELECT` on MySQL 8.4 and MariaDB 11.8 halts the table
  `TABLE_DROPPED` when the user holds no other privilege on it (`information_schema` hides it), and
  `TABLE_NOT_REPLICABLE` (1142) when another privilege keeps it visible. The pages and DA-SRC-19 say
  so; telling the two apart is DA-SRC-36.

The controller's ruling on Task 9's deferred partition item: a managed repair of a via-root
publication re-snapshots every table, even a leaf the publication names itself. Kept, as the
conservative answer: under via-root `pg_publication_tables` lists only the root, so narrowing it
needs a measurement first (DA-SNAP-22's row records it).

### 54.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SRC-32 | A source-change reset misses sink tables the pipeline neither resolves nor caches. The reset runs over the tables resolved at that start and the cached schemas (Data-Flow Phase 4, narrowed 2026-11-04). A sink table outside both, such as a job's copy outside the config later added to it, or a wiped state's table halted at resolve, keeps the old server's marker: PostgreSQL then skips its stream changes below it; MySQL file mode fails the commit (`DifferentBinlogSeries`). And a reposition's permanent sink error ends every start (M-2): a wiped state whose sink table is keyed otherwise halts the table at the baseline (`SCHEMA_MISMATCH`) but the reposition still commits to it, and its "keyed by" error repeats at each start without naming the cause (fails closed; it needs a source change, a wiped state and a re-keyed table together) | 🟢 | the M12a whole-branch review (M-1, M-2) | List the sink's tables under the pipeline's prefix at a `SourceChanged` start, or record the source-change epoch and reposition a table when it is next tracked; skip the reposition of a table halted `SCHEMA_MISMATCH` in that pass, or turn a permanent reposition error into the table's `SINK_REJECTED` halt |
| DA-SRC-33 | A PostgreSQL 17+ failover slot is treated as a source change. A failover slot synced to the standby, with `synchronized_standby_slots` set on the primary, survives a promotion intact, yet every new timeline counts as a source change: the pipeline halts `WAL_SOURCE_CHANGED`, and `re_snapshot` drops or advances the slot and copies every table | 🟢 | the M12a whole-branch review (deferred item 65) | Under `require_existing`, accept a new timeline when the slot is `failover`/`synced` and its `confirmed_flush` is at or below the timeline's switch point |
| DA-SRC-34 | File mode under `re_snapshot` loops on a host that alternates between servers. Each Phase 0 recovers on one server, the dump's open finds the other (`SourceChanged`, a rerun), and every table's marker is reset each time while nothing streams | 🟢 | the M12a whole-branch review (deferred item 85) | Halt `BINLOG_SOURCE_CHANGED` after N consecutive `SourceChanged` reruns without a durable checkpoint, naming the alternation; the docs: point a file-mode pipeline at one server |
| DA-SRC-35 | MySQL's per-column config keys match column names with case. An override or a `primary_key` entry spelled in another case than the catalog's doesn't apply, though MySQL column names compare without case | 🟢 | the M12a whole-branch review (Task 6's minors) | Match through `Fold`, and warn (V-034-like) on a key that matches no column |
| DA-SRC-36 | A lost MySQL `SELECT` reads as a dropped table. With no other privilege on the table, `information_schema` hides it, so the table halts `TABLE_DROPPED` (measured on MySQL 8.4 and MariaDB 11.8, M-5), and the halt's text says the table was dropped at the source | 🟢 | the M12a whole-branch review (M-5) | A grant probe in the no-table path (`read_plan`, the rescan), for example `SELECT 1 FROM t LIMIT 0`: measure first whether it tells a dropped table (1146) from a lost privilege (1142) for a user without database-level privileges, since the server may answer 1142 for both to hide a table's existence; until then the docs (`runbooks/halted-table.md`: check the grants) |
| DA-OPS-14 | MariaDB file mode's boundary check costs a binlog read at every stream open. `BINLOG_GTID_POS(file, pos)` scans the file up to `pos`, up to `max_binlog_size`, at every start and reconnect | 🟢 | the M12a whole-branch review (deferred item 87) | Measure in M12c's load runs, and remember the answer for `(file, pos)` within a run if it shows |
| DA-SNAP-22 | The wiped-state baseline's limits. A `Plan::Resume` table with no cached schema whose sink table exists (one re-added to the config) isn't baselined. A sink table holding a non-null key column the source no longer has is accepted, and the first MERGE fails (`SINK_REJECTED`). And a managed repair of a via-root publication re-snapshots every table, even a leaf the publication names itself (kept: `pg_publication_tables` lists only the root under via-root, so narrowing it needs a measurement first) | 🟢 | the M12a whole-branch review (Task 8's M-6; Task 9's partition item) | Baseline any uncached table the sink holds; refuse a non-null sink field the source lacks; measure what a via-root publication reports for a leaf it names, then narrow the re-snapshot |
| DA-SINK-12 | A case-only `ADD COLUMN` loops the evolver. On the stream path, an `ADD COLUMN` whose name matches a Delta column by case alone (PostgreSQL `"Note"` beside `note`) is rejected by delta-rs as a duplicate. The error is transient, so the evolver retries without end and the pipeline stalls without halting. Predates M12a | 🟢 | the M12a whole-branch review (M-9) | The evolver applies `missing_columns`' case rule and halts `SCHEMA_MISMATCH`; first measure delta-rs's case folding (ASCII or Unicode) and match it: changing the comparison unmeasured could swap the loop for a false halt |

## 55. M12a source hardening — the exit (2026-11-04)

M12a (`doc/v1.0/M12a-source-hardening.md`) merged 2026-11-04 (PR #19, `0309b38`), its PR CI green
on every target (`ci.yml` run 41822813507: `check`, `integration`, `e2e-mysql` on `mysql-84`,
`mysql-84-nogtid` and `mariadb-118`, `k3s` 33 min; `docs.yml` run 41822813341), and the site
deployed from `main` (`docs.yml` run 41834111658). The rows §53 closed stand as written; no count
changes.

M12's split grows to four plans (Oleksii, 2026-11-04; roadmap Q-M12-8): M12b keeps the roadmap's
cluster, control, API and observability scope and builds DA-OBS-7, DA-OPS-12 and DA-API-13 rather
than closing them with a pointer; the gaps M12a's review recorded in the source and the sink
(DA-SRC-25, 31…36, DA-SNAP-22, DA-SINK-12) get a plan of their own after M12b and before the
acceptance. DA-OPS-14 stays with the acceptance's load runs.

## 56. M12b control hardening — the plan (2026-11-04)

`doc/v1.0/M12b-control-hardening.md` is M12b's plan: sixteen tasks for the gaps below, and a spec
patch (config §1.2, §6.6, §11, §12.3, V-061, V-125, V-126; Data-Flow Phase 4 and On-Demand
Snapshot; System-Overview's API table), approved with the plan. The rulings (Q-M12b-1…4,
D-M12b.*) are in the plan, Q-M12-8 (M12's fourth plan, M12d) in the roadmap.

The planning measured:
- **`cargo audit`** on the base: 17 vulnerabilities and 23 warnings. `cargo update` of the patched
  releases leaves three, triaged (Task 14): quick-xml 0.39.4's two (only deltalake's
  `object_store` 0.13 links it; no 0.13 release takes the fix, and deltalake 1.1.3 still
  requires `^0.13.2`) and proc-macro-error2 (build time only).
- **The compile time** (Task 15): a clean `cargo build --workspace --all-targets` takes 588 s on
  12 cores (`aws-lc-sys`'s build script 110 s, `datafusion` 89 s, `deltalake-core` 74 s; the
  workspace's own units 925 of 3046 unit-seconds), a rebuild after touching `core` 37–40 s.
  `debug = "line-tables-only"` for the workspace crates keeps those times with 1 GB less target
  (adopted); `opt-level = 1` for the dependencies takes the clean build to 1028 s for the
  heaviest suites' 60 s → 42 s (not adopted).
- **opentelemetry-otlp 0.33.1** merges `OTEL_EXPORTER_OTLP_HEADERS` after the configured headers:
  the environment wins for a name both set.

**What M12b takes** (each closed with a pointer at its exit):

| ID | Task |
|---|---|
| DA-CLUS-15 | 1: every node asks the seeds that aren't members, so groups formed apart merge |
| DA-API-13 | 2: a holder judged by gossip membership, `holder.stale` and `holder.suspect` (Q-M12b-3) |
| DA-OPS-9 | 3: the removal tombstone in `globals.state`, torn down where the ring places it when no owner saw the removal |
| DA-OPS-10 | 4: a pipeline waits for every pending removal of its id or its resources, on every node |
| DA-OPS-12 | 5: `?restart=accept`, applied at each node's next start; the cluster's own fields refused (Q-M12b-2) |
| DA-SNAP-15 | 6: the job's `truncate` (Q-M12-7) |
| DA-SNAP-16, 19, 20, 21 | 7: the newest 100 ended jobs kept, an undecodable job skipped, a copy's halt flagged, a lost restart asked again |
| DA-SNAP-11 | 8: a key logged once, under its latest writer |
| DA-SNAP-12 | 9: a PostgreSQL chunk read on a connection of its own |
| DA-API-16, DA-API-17 | 10: the key fetch (no redirect, 1 MiB, V-125); 1024 connections |
| DA-API-20, DA-API-18 | 11: posted validates at 1/s after 5 (`429`); the OAS 3.1 schema check |
| DA-OBS-7 | 12: `globals.tracing.headers` (V-126), an `https` collector tested |
| DA-OBS-8, DA-OBS-9 | 13: the lag across sessions; the JSON log's ids matched to Jaeger |
| DA-SNAP-7, DA-X-13 | 16: closed with pointers (the stream runs beside a snapshot; the commit count not seen again) |

Not taken: DA-SRC-25, 31…36, DA-SNAP-22 and DA-SINK-12 (M12d, Q-M12-8); DA-OPS-14 (M12c's load
runs). No count changes.

## 57. M12b control hardening — the gaps closed (2026-11-06)

M12b's tasks (`doc/v1.0/M12b-control-hardening.md`) closed the gaps §56 named. The operator pages
say what changed: `runbooks/config-changes.md` (removing a pipeline, keys applied at the next
start, the validate limit), `runbooks/snapshot-jobs.md` (`truncate`, retention, a copy's halt, a
lost restart), `runbooks/failover.md` (`holder_stale`, groups that merge), `configuration/cluster.md`
and `deployment/kubernetes.md` (discovery), `deployment/observability.md` (the lag, the headers,
the flush log) and `architecture/snapshots.md` (the read pool, the log's retention, `truncate`).

Found while executing (the plan's "Deviations during execution", each fixed with a test): the
sweep's orphan claims patiently, never taking a lease from its own node's live controller (Task
3); a removal whose generation is unread is waited for (Task 4); a job's `truncate` is written
only when set, so v0.1.0 still reads the job (Task 6); an API connection idle for 60 s is closed,
so silent ones can't hold every place under the cap (Task 10, D-M12b.13 amended); a tracing
header's value passes the exporter's URL-decoding unchanged (Task 12); a reconnect clears the
worker's frontier with its count, so the lag keeps the old session's start (Task 13). The audit
(Task 14) took arrow and parquet 59.3.2 for RUSTSEC-2026-0442 and triaged RUSTSEC-2026-0441
(pin-utils), both published after the plan: four advisories triaged in `.cargo/audit.toml`,
`cargo audit` clean.

| ID | Change | Evidence |
|---|---|---|
| DA-CLUS-15 | 🟥 → ✅ | `discovery.rs` `discover`: every node resolves its seeds each `REDISCOVER` (5 s) and announces to those that aren't members; `groups_that_each_have_a_peer_ask_their_seeds_again_and_merge`. The docs' caution is gone (`configuration/cluster.md`, `runbooks/failover.md`) |
| DA-API-13 | 🟥 → ✅ | `pipelines.rs` `liveness`: `holder.stale` (its node and incarnation not a member of the answering node's gossip) and `holder.suspect`, `holder_stale` in the lists; `Member.incarnation`; `a_holder_that_isnt_a_member_reads_as_stale`, `a_suspected_member_holds_its_lease_as_suspect` |
| DA-OPS-9 | 🟥 → ✅ | `state` `removal.rs` (`removals/<id>.json`), recorded by the coordinator before it adopts; the reconciler's sweep (`SWEEP_INTERVAL`) and the controller's `remove_orphan`; config §12.3; `a_removal_no_node_removed_is_torn_down_where_the_ring_places_it`, `a_removal_held_by_a_gone_node_is_torn_down_once_its_lease_expires`; an orphan, holding the lease, marks a removal the current generation undoes done without a teardown, and tears nothing down while that generation can't be read or load (`an_undone_removal_tears_nothing_down_at_a_hand_off`, `an_undone_removal_tears_nothing_down_at_a_failover`, `an_orphan_marks_a_removal_the_current_generation_undoes_done_without_a_teardown`, `an_orphan_tears_down_a_removal_the_current_generation_doesnt_undo`, `an_orphan_tears_nothing_down_when_the_current_generation_is_unread_or_invalid`); the owner marks it done before it releases the lease (`an_owners_removal_and_an_orphan_of_it_on_one_node_tear_down_once`); that done is conditional on the tombstone's version, which every record moves (its `records` counter): `an_undone_removal_recorded_again_meanwhile_stays_pending`, `state` `removal.rs` `a_record_that_keeps_a_removal_still_changes_its_bytes`, `a_conditional_done_loses_to_a_record_since_its_read`, `a_record_without_its_counter_reads_as_zero`, and `state` `tests/removals.rs` (RustFS, Azurite) |
| DA-OPS-10 | 🔄 → ✅ | the controller's `follow()` waits, before its first claim, for every pending tombstone of its id or of a pipeline it shares resources with, on every node; `a_pipeline_added_after_the_removal_it_follows_waits_for_it`, `a_node_started_during_a_removal_waits_for_it` |
| DA-OPS-12 | 🟥 → ✅ | `POST /v1/config/reload?restart=accept` (Q-M12b-2): published, each node's `restart_pending` until its next start; `FIXED_FIELDS` refused even then (config §1.2, V-061); `a_restart_field_is_published_when_accepted_and_pending_until_the_next_start`; `runbooks/config-changes.md` |
| DA-SNAP-15 | 🔄 → ✅ | `truncate: true` on the job (Q-M12-7): an empty snapshot change set with `truncate`, committed before the start's record (Data-Flow On-Demand Snapshot); `a_truncating_job_drops_the_rows_deleted_meanwhile`, `a_job_s_truncate_empties_the_table_and_keeps_the_marker` |
| DA-SNAP-16 | 🟥 → ✅ | `KEPT_ENDED_JOBS` (100), `prune_snapshot_jobs` after each ended job, `VersionedStore::delete`; `pruning_keeps_the_newest_ended_jobs_and_every_unfinished_one`, `a_job_that_ends_prunes_the_oldest_ended_jobs` |
| DA-SNAP-19 | 🟥 → ✅ | `TableHalt.copy`: a copy's halt fails its job table and is kept out of the halted tables, the metric and the API (`jobs.rs` `a_copy_that_can_t_be_read_halts_alone`, `a_copy_halted_in_a_run_fails_its_job_and_its_halt_goes`, and after a reconnect `a_copy_s_halt_stays_out_of_the_halted_tables_after_a_reconnect`) |
| DA-SNAP-20 | 🟥 → ✅ | `snapshot_jobs` skips an undecodable job with a warning; `an_undecodable_job_is_skipped_by_the_listing` |
| DA-SNAP-21 | 🟥 → ✅ | a `409` against a queued job writes its restart again (`ask_restart_again`) |
| DA-SNAP-11 | 🟥 → ✅ | `reconcile.rs`: a key logged once, under its latest writer, the log bounded by the distinct keys written during the wait; `a_key_written_again_is_logged_once_under_its_latest_writer` |
| DA-SNAP-12 | 🟥 → ✅ | `connect.rs` `Connector::read`: each chunk read on a connection of its own, `IDLE_READS` (4) kept; `a_waiting_chunk_read_holds_up_no_other_read` |
| DA-API-16 | 🟥 → ✅ | `auth.rs`: no redirect, `MAX_KEY_DOCUMENT` (1 MiB); V-125 warns on a non-`https` key URL; `a_key_fetch_follows_no_redirect_and_reads_a_bounded_body` |
| DA-API-17 | 🟥 → ✅ | `server.rs` `MAX_CONNECTIONS` (1024), a permit before each accept, and `IDLE_TIMEOUT` (60 s) closing a connection that moves nothing; `past_the_cap_a_connection_waits_for_one_to_close`, `an_idle_connection_is_closed_and_gives_its_place_back`. The trade-offs: a handler silent for 60 s has its connection cut (the client sees a reset), and a client that sends a byte within every 60 s holds its place |
| DA-API-18 | 🟥 → ✅ | `the_document_is_valid_oas_3_1`: the document checked by `boon` against the vendored OAS 3.1 schema |
| DA-API-20 | 🟥 → ✅ | `limit.rs`: posted validates at 1 a second after a burst of 5, `429` with `Retry-After` (Q-M12b-4); `posted_validates_past_the_burst_wait`. The node's own file stays unlimited |
| DA-OBS-7 | 🟥 → ✅ | `globals.tracing.headers` (secrets, R, V-126); the exporter rebuilt when they change; `an_https_collector_takes_the_export_with_its_headers`; a value reaches the collector as written (`%` passed as `%25` through the exporter's URL-decoding: `a_header_value_with_a_percent_reaches_the_collector_as_written`); `deployment/observability.md` |
| DA-OBS-8 | 🟥 → ✅ | the controller's `Lag { kept, own }`: an earlier session's start kept until the new stream reads as far; `the_kept_lag_start_waits_for_the_next_session_to_read_as_far`; a reconnect clears the worker's frontier and pending start with its count (`WorkerStatus::reconnect`, `a_reconnect_clears_the_frontier_and_the_pending_start`) |
| DA-OBS-9 | 🟥 → ✅ | the Jaeger E2E runs the binary with JSON logs and matches a logged flush's `trace_id`/`span_id` to a trace Jaeger holds (`observability.rs`) |
| DA-SNAP-7 | 🔄 → ✅ | closed with a pointer: an incremental snapshot runs beside the stream, whose position advances with each flush, so a slow snapshot holds back only the WAL or binlog the stream hasn't flushed; the guard (`max_wal_lag_bytes`) and `binlog_purge_action` cover the rest (`architecture/snapshots.md`) |
| DA-X-13 | 🟥 → ✅ | closed with a pointer: not seen again in any E2E run, local or CI, from M7a through M12b (the plans' measurements and deviations record none) |

## 58. M12b control hardening — the exit (2026-11-06)

M12b (`doc/v1.0/M12b-control-hardening.md`) merged 2026-11-06 (PR #20, `6370ef0`), its PR CI green
on every target (`ci.yml` run 42215952227: `check`, `integration`, `e2e-mysql` on `mysql-84`,
`mysql-84-nogtid` and `mariadb-118`, `k3s` 24 min; `docs.yml` run 42215952232), and the site
deployed from `main` (`docs.yml` run 42220732173). The first CI run failed `integration` on
`sync_commit`'s `a_stalled_synchronous_commit_survives_the_chunk_that_missed_it`: its first sample
raced the snapshot's last chunk read, which ends after the stalled writer appears now that a chunk
reads on a connection of its own (DA-SNAP-12). The test waits for the snapshot to stop before it
checks the stall (`131f9eb`, test only; the plan's deviations record it). The rows §57 closed
stand as written; no count changes. Next: M12d, the source and sink gaps of §54.1 (Q-M12-8).

## 59. M12d source and sink follow-ups — the plan (2026-11-06)

`doc/v1.0/M12d-source-sink-followups.md` is M12d's plan: nine tasks for the gaps below, and five
spec patches (config §9.2.1, §10, §13 V-127; Schema_Change §7.2; Data-Flow Phases 2a, 2b and 4),
drafted in the plan for approval. The rulings are Q-M12d-1…5 and D-M12d.1…11.

The planning measured:
- **delta-rs folds column names with Unicode lower-casing** (`buoyant_kernel` 0.28.1
  `StructType::try_new`): `ÄRGER` beside `ärger` is refused. DA-SINK-12 is wider than §54.1 said:
  a PostgreSQL table with both `note` and `"Note"` loops at create too.
- **MySQL 8.4 and MariaDB 11.8** answer 1142 to `SELECT 1 … LIMIT 0` for a dropped table and an
  ungranted one alike when the user has no database-level privilege. 1146 needs a privilege on the
  database, so DA-SRC-36 can say "dropped, or `SELECT` lost" but not which.
- **PostgreSQL 18, via-root:** `pg_publication_tables` lists only the root, even for a leaf the
  publication names; `pg_publication_rel` lists both. The repair needs to copy only those leaves
  (DA-SNAP-22).
- **PostgreSQL 18 slot synchronization** (a new `postgres-sync-*` pair in test-infra):
  - a failover slot's first copy waits until its `catalog_xmin` passes the standby's;
  - a promoted standby's slot is `failover`, `synced` and streaming, at the primary's
    `confirmed_flush`;
  - the sync worker never takes a slot past the standby's WAL, so a synced position is at or
    before the switch point. DA-SRC-33 needs no `TIMELINE_HISTORY` (Q-M12d-5).

**What M12d takes** (each closed with a pointer at its exit):

| ID | Task |
|---|---|
| DA-SINK-12 | 1: `differ_by_case_alone` (Unicode), refused at create (`SINK_REJECTED`) and evolve (`EVOLUTION_UNSUPPORTED`) |
| DA-SRC-35 | 2: MySQL column keys matched without case; V-127 for a key naming no column (both sources) |
| DA-SRC-25 | 3: `column_names_fold`; MySQL's `keep_names` through the diff's matching; a case-only change `NoTargetEffect` (Q-M12d-3) |
| DA-SRC-36 | 4: the 1142/1146 probe on the rescan paths; `TABLE_DROPPED` kept, a status warning beside it (Q-M12d-4) |
| DA-SNAP-22 | 5: any first-tracked sink table baselined; a non-null sink column the source lacks halts; the via-root repair adds only the leaves it hid |
| DA-SRC-32 | 6: `Sink::held_tables`, the recorded key; every held table repositioned; a refused reposition halts `SINK_REJECTED` |
| DA-SRC-34 | 7: three `SourceChanged` runs in a row without a durable checkpoint halt the pipeline |
| DA-SRC-33 | 8: managed failover slots on 17+; `prepare_marked` with the lake's highest marker; a promotion the slot followed continues (Q-M12d-2, Q-M12d-5) |
| DA-SRC-31 | ⏭ v2 (Q-M12d-1): logical decoding on a standby; the source reads a primary in v1 |

Not taken: DA-OPS-14 (M12c's load runs). No count changes.

## 60. M12d source and sink follow-ups — the gaps closed (2026-11-10)

M12d's tasks (`doc/v1.0/M12d-source-sink-followups.md`, executed 2026-11-07…11-10) closed the gaps
§59 named, and Task 9 applied the spec patches: P-1…P-5, with P-2 and P-5 reworded during execution
(the slot confirmed **exactly** at the checkpoint, with no lake marker past it), and P-6, approved
during execution (Data-Flow Source Disconnect: a reconnect to the same system on a later timeline
reruns Phase 0 under either action). The research: `Postgres_Slot_and_Publication_Lifecycle.md` §8
(failover slots) and §4 (the via-root repair), `MySQL_DDL_Classifier.md` §7 (names the server
compares without case), `Delivery_Guarantee_and_Position_Guard.md` §8 (the reposition set, the
alternation), `Multi_Sink_Architecture_and_Delta_Lake.md` §6.3, §7.3 (Delta's case folding,
`held_tables`). The operator pages: `runbooks/position-lost.mdx` (a failover slot carries the stream
over, whether the pipeline ran or not; a planned switchover continues, a crash under load usually
halts; a down standby stalls the pipeline; every Delta table repositioned; the alternation halt),
`runbooks/halted-table.md` (case clashes, the `1142` warning, a non-null column the source lacks, a
refused reposition), `configuration/sources.md` (failover slots, the continuation, the via-root
repair's copy), `configuration/sink.md` (column keys, V-127) and the generated reference
(`source_change_action`).

Found while executing (the plan's "Deviations during execution"):
- **The snapshot read under the catalog's spelling** (Task 3, Critical in its review; the planning
  reference had it too): after a case-only change, rows, key columns and the chunk cursor were named
  otherwise than the cached schema. The read plan now takes the cached name, `scope` spells the
  identifier as the result's column, and a Phase 0 baseline of a fold-only difference caches the
  sink's spelling.
- **The hidden-table probe failed the rescan on any other error** (Task 4): a dropped schema (1049)
  made the rescan fail and `TABLE_DROPPED` never came. `hidden()` now returns a bool and logs any
  other answer as not hidden (source-mysql gains `tracing`).
- **The held-table listing read every log page** (Task 6): the `_delta_log/` probe now reads the first
  listing entry; `unescape_table_segment` accepts only the canonical form.
- **A running pipeline's failover still halted** (Task 8, Important in its review): the continuation
  was evaluated only at a start, so a reconnect to the promoted server halted `WAL_SOURCE_CHANGED`
  under the default `error`. Oleksii's ruling: the reconnect reruns the start under either action
  (P-6); the E2E keeps the pipeline running through the promotion (`Proxy::tcp`).
- **`continues` accepted a slot past the checkpoint** (Task 8), which then re-snapshotted silently.
  It now requires the slot exactly at the checkpoint, and the warning is logged only on an actual
  resume; P-2 and P-5 reworded to match (Oleksii, 2026-11-10).

| ID | Change | Evidence |
|---|---|---|
| DA-SINK-12 | 🟥 → ✅ | Task 1: `differ_by_case_alone` (Unicode); `create` → `SINK_REJECTED`, `evolve` → `EVOLUTION_UNSUPPORTED` naming both, `missing_columns` → `SCHEMA_MISMATCH`; `a_table_whose_columns_differ_by_case_alone_is_refused`, `a_column_that_differs_from_one_by_case_alone_is_unsupported`, `a_sink_column_matching_by_case_alone_halts`, `delta_rs_refuses_a_column_beside_one_that_differs_by_unicode_case` |
| DA-SRC-35 | 🟥 → ✅ | Task 2: `column_override` (MySQL keys without case), `check_column_keys`, V-127 on both sources; `column_keys_match_as_the_source_compares_names_v127`, IT `column_keys_name_columns_without_case` |
| DA-SRC-25 | 🟥 → ✅ | Task 3: `column_names_fold`, `same_columns`, `NoTargetEffect`, `keep_names` through the diff's matching, the snapshot under the cached names; IT `a_change_of_case_alone_keeps_the_cached_name`, `a_rename_onto_a_dropped_name_keeps_its_new_spelling`, `a_change_of_case_alone_is_no_drift_where_names_fold`, `a_baseline_of_a_case_only_change_caches_the_sink_spelling` |
| DA-SRC-36 | 🟥 → ✅ | Task 4: the `SELECT 1 … LIMIT 0` probe on the rescan paths, a `1142` status warning, `TABLE_DROPPED` kept; ITs `a_table_the_user_can_no_longer_see_reads_as_dropped_with_a_warning`, `a_table_whose_schema_is_gone_is_still_reported_dropped`; `a_gone_tables_rescan_warnings_reach_the_status`, `a_periodic_rescans_warnings_reach_the_status` |
| DA-SNAP-22 | 🟥 → ✅ | Task 5: any first-tracked sink table baselined; a non-null sink column the source lacks halts; the via-root repair copies only the leaves it hid; `a_table_first_tracked_at_a_resume_is_baselined_against_its_sink_table`, `a_sink_table_with_a_non_null_column_the_source_lacks_halts`, IT `a_via_root_repair_copies_only_the_partitions_it_hid` |
| DA-SRC-32 | 🟥 → ✅ | Task 6: `Sink::held_tables`, `SinkTableState::identifier`, the reposition over held ∪ resolved ∪ cached, a refusal halts `SINK_REJECTED`; `a_source_change_repositions_a_copy_the_pipeline_doesnt_track`, `a_refused_reposition_halts_that_table_and_the_start_goes_on`, `a_held_table_with_no_key_is_warned_about`, `the_sink_lists_the_tables_it_holds` |
| DA-SRC-34 | 🟥 → ✅ | Task 7: `started_from`, `SOURCE_CHANGES_IN_A_ROW` (3), `alternating`; `runs_that_keep_finding_another_server_halt`, `a_durable_checkpoint_starts_the_count_of_server_changes_again` |
| DA-SRC-33 | 🟥 → ✅ | Task 8: failover slots on 17+, `prepare_marked`, `continues`, the reconnect's rerun (P-6), the checkpoint re-recorded; `a_promotion_the_slot_followed_continues_the_stream`, `a_start_on_another_server_tells_the_source_the_highest_marker`, `the_highest_marker_counts_held_tables_and_skips_other_dialects`, `a_promotion_the_slot_followed_resumes_on_the_new_server`, `a_reconnect_to_the_promoted_server_reruns_the_start_which_continues`, IT `a_managed_slot_is_a_failover_slot_from_postgresql_17`, E2E `a_promotion_the_failover_slot_followed_continues_the_stream`. Measured on PostgreSQL 18; 17 by the same mechanism, per the PostgreSQL docs |
| DA-SRC-31 | 🟥 → ⏭ v2 | Q-M12d-1: decoding on a standby deferred; what v2 needs in `Postgres_Slot…` §8.6 |

### 60.1 New gaps (M12d's known limitations)

| ID | Title | Sev | Note |
|---|---|---|---|
| DA-SRC-37 | A refused reposition of a table the pipeline doesn't track is never retried | 🟢 | Task 6 review M-1. Its halt goes at the next start, and the checkpoint already names the new server; tracked later, the table keeps the old server's marker. Documented |
| DA-SNAP-23 | On MySQL the first-met baseline halts a column whose name changed only its case | 🟢 | Task 5 review. Conservative (D-M12d.1); the cached path accepts it |
| DA-SRC-38 | The alternation count never decays; a promoted-reconnect alternation halts without a code | 🟢 | Task 7 review, Task 8 re-review N1. Documented: stop and resume |

Also left, recorded in the research and not tracked as gaps: MySQL's accent-insensitive column
comparison and two override keys of one column that differ by case alone (DA-SRC-35); a crash of a
busy primary usually lags the synced slot, so it halts (`Postgres_Slot…` §8.5); the failover E2E
runs on PostgreSQL 18 only, 17 resting on the same mechanism per the PostgreSQL docs (folded into
DA-SRC-33's rationale in Task 9's review).
The minor findings
the task reviews deferred are listed in the plan's deviations for the whole-branch review.

## 61. M12d whole-branch review — DA-SRC-25 extended, one gap found (2026-11-11)

M12d's whole-branch review returned "with fixes", with one Important finding: since Task 3, a
truncating job on a MySQL table whose key column changed its case alone put the pipeline in `ERROR`
at every start (the truncate keyed the table by the catalog's spelling, which the sink table
doesn't hold). Four fix waves followed, each re-reviewed; the fourth re-review found no Critical or
Important finding. Oleksii approved two more spec patches: P-7 (Data-Flow Source Disconnect: the
alternation halt covers every source) and P-8 with its amendment (Schema Change Channel A and §8.2:
at a replayable resume, a sink table that is absent is made from the cached schema for a table
tracked before, and from the queried baseline for a table first tracked at a resume). The fixes and
the known limitations are in the plan's "Deviations during execution" and "Known limitations".

| ID | Change | Evidence |
|---|---|---|
| DA-SRC-25 | ✅ (extended) | The truncate takes the key the sink table records; a session key that differs from it in any way, case included, halts that table `SINK_REJECTED` without emptying it, its job fails and the start goes on (`a_truncating_job_halts_a_table_its_sink_table_keys_spelled_otherwise`, `a_truncating_job_halts_a_copy_its_sink_table_keys_otherwise`); a sink table dropped while the pipeline was down is made again under the cached spelling (P-8; E2E `a_deleted_table_of_a_recased_key_is_made_again_under_the_cached_spelling`); the first-met baseline halts name a table the pipeline meets for the first time (DA-SNAP-22's texts) |

### 61.1 New gaps

| ID | Title | Sev | Note |
|---|---|---|---|
| DA-SRC-39 | A session that ends on an error drops the PostgreSQL stream without closing it | 🟢 | Found while fixing a PostgreSQL 14 test flake (`093168b`). Predates M12d |

## 62. M12d source and sink follow-ups — the exit (2026-11-11)

M12d (`doc/v1.0/M12d-source-sink-followups.md`) merged 2026-11-11 (PR #21, `1ba6377`), its PR CI green
on every target (`ci.yml` run 43018425423: `check`, `integration`, `e2e-mysql` on `mysql-84`,
`mysql-84-nogtid` and `mariadb-118`, `k3s` 4 min; `docs.yml` run 43018425437), and the site
deployed from `main` (`docs.yml` run 43022584237). The rows §60 and §61 closed stand as written; no
count changes. Next: M12c, the acceptance and `v1.0.0` (Q-M12-8), with the open rows: DA-OPS-14 (its
load runs) and DA-SRC-37, 38, 39, DA-SNAP-23 (Q-M12-1: fixed, or closed with a pointer).

## 63. M12c v1 acceptance — the gaps closed (2026-11-11)

M12c's tasks (`doc/v1.0/M12c-acceptance.md`, executed 2026-11-11) closed the five rows §62 left
open: four with code, one by measurement. Task 7 applied the spec patches P-1…P-3, approved
2026-11-11: Data-Flow Phase 2b and Source Disconnect (the alternation window and its code, P-1),
Data-Flow Phase 4 (a refused reposition's halt stays while the sink holds the table, P-2), and
Schema Change Detection §7.2 (the first-met baseline keeps the target's spelling, P-3). The
research: `Delivery_Guarantee_and_Position_Guard.md` §8, `MySQL_DDL_Classifier.md` §7,
`Postgres_Slot_and_Publication_Lifecycle.md` §4. The operator pages: `runbooks/halted-table.md`
and `runbooks/position-lost.mdx`. The store switch, the cloud subset and the acceptance runs are in
`test-infra/README.md` and `doc/v1.0/README.md`.

| ID | Change | Evidence |
|---|---|---|
| DA-SRC-39 | 🟥 → ✅ | Task 1 (`caae852`): a session closes its stream on every exit, an error's too; `a_session_that_fails_closes_its_stream` |
| DA-SRC-38 | 🟥 → ✅ | Task 2 (`f700a10`): the count starts at 1 again when a change comes more than `SOURCE_CHANGE_WINDOW` (10 minutes) after the one before (D-M12c.2); a PostgreSQL promoted reconnect's message carries `WAL_SOURCE_CHANGED` (D-M12c.3); `server_changes_far_apart_dont_halt`, `an_alternation_halts_with_the_code_its_change_names`, the failover E2E; P-1 |
| DA-SRC-37 | 🟥 → ✅ | Task 3 (`48c8897`): a refused reposition's halt (its detail begins `the source changed, and its reposition fails`, the code's `REPOSITION_REFUSED`) of an untracked table stays while the sink holds it (D-M12c.4); `a_refused_repositions_halt_stays_while_the_lake_holds_the_table`; P-2 |
| DA-SNAP-23 | 🟥 → ✅ | Task 4 (`7c33b77`): where names fold (MySQL), a sink column matching a source column by case alone is that column, written under the sink's spelling (D-M12c.5); `where_names_fold_a_sink_column_matching_by_case_alone_is_the_column`, `a_first_met_table_whose_names_changed_case_alone_is_baselined_where_names_fold`, E2E `a_lost_state_meets_a_recased_column_as_the_same_column` (`mysql-84`, `mariadb-118`); P-3 |
| DA-OPS-14 | 🟥 → ✅ | Closed by measurement, no code (D-M12c.1): `BINLOG_GTID_POS` costs 0 to 1 ms with MariaDB 11.8's `binlog_gtid_index`, and about 1.4 ms per MB of file on 10.11 without it, about 1.4 s for a full 1 GiB file, once per stream open; no cache (`doc/v1.0/M12c-acceptance.md` §Measured in planning) |

The open count goes from 5 to 0 (v1). The acceptance runs themselves (Tasks 5, 6 and 8) are the
exit's evidence, not gaps.

## 64. M12c v1 acceptance — the runs (2026-11-13)

M12c's Task 8 ran the acceptance runs (`cargo xtask bench|chaos|soak`, release mode) on Oleksii's
machine at commit `89d7e1c`, against test-infra's PostgreSQL 18, MySQL 8.4 and RustFS, at the
product's defaults. The report, with the machine and every measurement, is
`doc/v1.0/v1-acceptance.md`; roadmap §8 checks the items it shows met. Every run passed; no
target was missed, and no gap surfaced.

| Run | PostgreSQL 18 | MySQL 8.4 |
|---|---|---|
| Steady lag p99 (D-V1.3, ≤ 60 s) | 30.9 s | 30.8 s |
| Failover recovery (D-V1.3, ≤ 120 s) | 52.5 s | 53.7 s |
| MERGE under random-key churn (R-5, DA-MS-3) | 20 MERGEs, 1.18 s mean, 13.0 MB written each on a 13.1 MB table | 20 MERGEs, 1.38 s mean, 11.1 MB written each |
| Chaos (4 runs: storage outages shorter and longer than the lease, a source restart, a partition) | 4/4, no loss | 4/4, no loss |
| Soak, 24 h with churn (node 1 RSS ≤ 2×) | 24/24 checks, 143 churn steps, 17.0M changes, RSS 0.90× | — |

R-5 is measured (DA-MS-3's row carries the numbers; it stays the decided caveat, D-MS.15). The
cloud subset ran on GCS from Oleksii's machine (4/4 on each source, Task 5), then 4/4 on S3, GCS
and Azure on each source in `cloud.yml` (run 43375990336) and on GCS in PR #22's `e2e-gcs`. The open count stays 0 (v1).


## 65. v1.0.0 released (2026-11-13)

M12c merged (PR #22, `81ce996`), its CI and `cloud.yml` green, and `v1.0.0` tagged on `main`:
`release.yml` run 43388418482 published the image `ghcr.io/<owner>/data-avalanche` `1.0.0`/`1.0`/
`latest` for amd64 and arm64 with its attestation, the chart `oci://ghcr.io/<owner>/charts/data-avalanche`
`1.0.0`, and the GitHub release with the SBOM. Roadmap §8's checklist is complete; the acceptance
report is `doc/v1.0/v1-acceptance.md`. No gap opened or closed: the open count stays 0 (v1), and
the ⏭ v2 rows (the Iceberg sink's, DA-SRC-31) are the next version's backlog.

## 66. iceberg-rust recheck — still append-only (2026-11-13)

Rechecked against the published `iceberg` **0.11.0** crate (crates.io, 2026-11-03; MSRV 1.95) and
upstream `main` at `fa961f5c` (2026-11-13). **The Iceberg sink stays blocked: v2 waits for a
release that commits deletes.**

- **DA-SINK-1i (G1).** 0.11.0's `Transaction` has the same actions as 0.10.1: `fast_append`,
  `update_schema`, `update_table_properties`, `replace_sort_order`, `update_location`,
  `update_statistics`, `upgrade_table_version`, `expire_snapshots`. `fast_append` refuses delete
  files ("Only data content type is allowed for fast append"). `main` has the same public actions.
  Groundwork merged on `main` only (not in 0.11.0): the stateful `TransactionAction` (RFC #2620,
  #3265), a snapshot operation that reports removed files (#3253), a simple snapshot producer
  (#3474), and a copy-on-write rewrite primitive `iceberg::cow_rewrite` (#2752, merged 2026-10-01 —
  correcting the probe README, which had it open), which writes replacement files but "does not
  commit metadata by itself". In flight, all open: #3564 MergingSnapshotProducer (changes
  requested), #3300 overwrite action on it (changes requested; continues the closed #2185), #2203
  RowDelta (awaiting review — the MOR path), #3046 RewriteFiles (awaiting review). Epic #2186 open.
- **DA-CAT-6.** HMS `update_table` still returns `FeatureUnsupported` (0.11.0 and `main`).
- **DA-CAT-7.** `main` has a SigV4 request signer (`SigV4Signer`, #3082, merged 2026-10-09; not in
  0.11.0). The auth manager (#3092) and enabling it from catalog properties (#2660, draft) are
  open, so `rest.auth: sigv4` is still not configurable through the catalog.
- **DA-SCHEMA-8.** `UpdateSchemaAction` has `add_column` and `delete_column` (0.11.0 and `main`); no
  rename, no type promotion.
- **DA-TYPE-5.** `main` folds the Arrow `arrow.uuid` extension type into an Iceberg `uuid`; 0.11.0
  doesn't.
- **DA-SINK-10.** Still `expire_snapshots` only; the COW primitive has no rewrite-files commit yet
  (#3046 open).
- **DA-DEP-1 — a new constraint.** 0.11.0 builds on arrow **58**, `main` on arrow **60**; the
  workspace is on arrow **59** (deltalake 1.0 and 1.1.x both require 59). No iceberg-rust release
  shares the workspace's arrow major, so v2 also needs a deltalake on the same arrow major as the
  iceberg release it pins (D-MS.14, one arrow major).

Next recheck: when #3564 and #3300 merge (the COW mirror), and when #2203 merges (the MOR default);
then run G1 from `prototypes/iceberg-capability/README.md` against `main`. No count changes: the
rows stay ⏭ v2.

## 67. 1.0.1 dependency refresh — the plan (2026-11-13)

`doc/v1.0/v1.0.1-dependency-refresh.md`: every dependency to its newest stable version, and a release
gate (`cargo xtask deps --check` in `release.yml`) with written holds in `dependency-exceptions.yaml`.
Verified in scratch: Rust 1.100.0 needs two `fetch_update` → `try_update` renames; deltalake 1.1
compiles unchanged and now reports the truncate-vs-OPTIMIZE race as a conflict itself (probe P11a's
gap in 1.0; our post-DELETE check stays as a backstop); pgwire-replication 0.5, serde-saphyr 2,
sqlparser 0.64, prometheus-client 0.26, base64 0.23 and getrandom 0.4 compile unchanged with their
suites green. Held: arrow 60 (deltalake-core 1.1.4 requires `^59`), mysql_common 0.38 (mysql_async
0.37.1 requires `^0.37`). No gap opened or closed.

---

### Counts (by status)

> Recounted 2026-09-26 (v1 readiness tidy, §26); the 2026-06 figures are superseded. Open count
> updated 2026-10-02 for the gaps M5 carried (§27), 2026-10-03 for DA-SNAP-9 and DA-SNAP-10 (§28),
> 2026-10-07 for the M6a whole-branch review (§28.2), 2026-10-08 for M6b's planning findings
> (§29), 2026-10-13 for M6b's exit (§30), 2026-10-14 for M7's planning (§31), 2026-10-14 for
> M7a's plan (§32), 2026-10-16 for M7a's exit (§33), 2026-10-17 for M7b's plan (§34),
> 2026-10-20 for M7b's exit (§35), 2026-10-21 for M8a's plan (§36), 2026-10-22 for M8a's exit (§37),
> 2026-10-22 for M8b's plan (§38), 2026-10-23 for M8b's exit (§39), 2026-10-23 for M9a's plan
> (§40), 2026-10-23 for M9a's exit (§41), 2026-10-27 for M9b's plan (§42), 2026-10-28 for M9b's exit (§43), 2026-10-28 for M10's plan (§44), and 2026-10-29 for M10's exit (§45), and 2026-10-29 for M11a's plan (§46), and 2026-10-29 for M11a's whole-branch review (§47), and 2026-10-29 for M11a's exit (§48), and 2026-10-29 for M11b's plan (§49), and 2026-10-30 for M11b's pages (§50), and 2026-10-30 for M11b's exit and v0.1.0 (§51), and 2026-10-30 for M12a's plan (§52), and 2026-11-04 for M12a's gaps closed (§53), and 2026-11-04 for M12a's whole-branch review (§54), and 2026-11-04 for M12a's exit (§55), and 2026-11-06 for M12b's gaps closed (§57), and 2026-11-06 for M12b's exit (§58), and 2026-11-06 for M12d's plan (§59), and 2026-11-10 for M12d's gaps closed (§60), and 2026-11-11 for M12d's whole-branch review (§61), and 2026-11-11 for M12d's exit (§62), and 2026-11-11 for M12c's gaps closed (§63), and 2026-11-13 for M12c's acceptance runs (§64, no change), and 2026-11-13 for v1.0.0's release (§65, no change), and 2026-11-13 for the iceberg-rust recheck (§66, no change), and 2026-11-13 for 1.0.1's plan (§67, no change).

| Status | Count (v1 scope) | Notes |
|---|---|---|
| ✅ Resolved | the large majority | Design closed by a research/spec doc or a probe. |
| 🔄 Partial | 0 | DA-OPS-10, DA-SNAP-15 and DA-SNAP-7 ✅ 2026-11-06 (§57). DA-SRC-9 ✅ 2026-11-04 (§53); DA-OPS-10 and DA-SNAP-15 🔄 2026-10-30 (§50); DA-SCHEMA-10 ✅ 2026-10-28 (M9b merged, §43); DA-SNAP-7 (doesn't block the plan); DA-OPS-8 ✅ 2026-10-30 (§50; 🔄 2026-10-22, §38); DA-SNAP-10 ✅ 2026-10-16 (M7a merged, §33). DA-MS-3 is a decided caveat. DA-X-8 ✅ 2026-09-26; DA-SRC-16 and DA-X-12 ✅ 2026-10-13 (§30). |
| 🟥 Open (v1) | **0** | DA-OPS-14, DA-SRC-37, 38, 39 and DA-SNAP-23 ✅ 2026-11-11 (§63), 5 rows. Recounted 2026-11-11 (§61): the 5 🟥 domain rows (DA-OPS-14; DA-SRC-37, 38, 39; DA-SNAP-23); DA-SRC-39 🆕 2026-11-11 (§61.1). Recounted 2026-11-10 (§60): the 4 🟥 domain rows (DA-OPS-14, M12c's; DA-SRC-37, DA-SRC-38, DA-SNAP-23, 🆕 2026-11-10, §60.1). DA-SINK-12, DA-SRC-25, 32, 33, 34, 35, 36 and DA-SNAP-22 ✅ 2026-11-10 (§60), 8 rows; DA-SRC-31 ⏭ v2 (Q-M12d-1). Recounted 2026-11-06 (§57): the 10 🟥 domain rows (DA-SNAP-22; DA-SRC-25, 31, 32, 33, 34, 35, 36; DA-SINK-12; DA-OPS-14), M12d's and M12c's (Q-M12-8). DA-CLUS-15, DA-SNAP-11, 12, 16, 19, 20, 21, DA-OBS-7, 8, 9, DA-API-13, 16, 17, 18, 20, DA-OPS-9, 12 and DA-X-13 ✅ 2026-11-06 (§57), 18 rows. DA-SRC-32, 33, 34, 35, 36, DA-OPS-14, DA-SNAP-22 and DA-SINK-12 🆕 2026-11-04 (§54.1). Recounted 2026-11-04 (§54): the 27 🟥 domain rows (DA-CLUS-15; DA-SNAP-11, 12, 16, 19, 20, 21, 22; DA-SRC-25, 31, 32, 33, 34, 35, 36; DA-SINK-12; DA-OBS-7, 8, 9; DA-API-13, 16, 17, 18, 20; DA-OPS-9, 12, 14) and DA-X-13. DA-SRC-13, 14, 15, 17, 18, 19, 20, 21, 22, 24, 26, 29, 30, DA-SNAP-13, DA-SNAP-14, DA-OPS-13 and DA-X-15 ✅ 2026-11-04 (§53), 17 rows; DA-SRC-25 stays open (withdrawn in Task 6's review); DA-SRC-31 🆕 2026-11-04 (§53.1). Recounted 2026-11-04 (§53): the 19 🟥 domain rows (DA-CLUS-15; DA-SNAP-11, 12, 16, 19, 20, 21; DA-SRC-25, 31; DA-OBS-7, 8, 9; DA-API-13, 16, 17, 18, 20; DA-OPS-9, 12) and DA-X-13. DA-X-15 🆕 2026-10-30 (§52.1). Recounted 2026-10-30 (§52): the 34 domain rows and DA-X-13. DA-OPS-11 ✅ 2026-10-30 (§51: `v0.1.0` released, the packages public); DA-SRC-30 🆕 2026-10-30 (§50.1); DA-OPS-10, DA-API-14, DA-API-15, DA-SNAP-15, DA-SNAP-17 and DA-SNAP-18 leave 2026-10-30 (§50; DA-OPS-10 and DA-SNAP-15 as 🔄); DA-CLUS-15 stays; DA-OPS-12, DA-OPS-13 and DA-SRC-29 🆕 2026-10-29 (§49.1); DA-CLUS-15 🆕 2026-10-29 (§47.1); DA-OPS-11 🆕 2026-10-29 (§46.1), extended (§47), the manual run done (§48); DA-OBS-8 and DA-OBS-9 🆕 2026-10-29 (§45.1); DA-OBS-7 🆕 2026-10-28 (§44.1); DA-SNAP-18…21 🆕 2026-10-28 (§43.1); DA-SNAP-15…17 🆕 2026-10-27 (§42.1); DA-API-13…18 🆕 2026-10-23 and DA-API-20 🆕 2026-10-24 (§41.1); DA-API-19 🆕 2026-10-23, ✅ 2026-10-24 (§41); DA-SRC-13…15, 17, 18, carried from M5 (§27); DA-SNAP-11…13 and DA-SRC-19 🆕 2026-10-07 (§28.2); DA-SRC-20 🆕 2026-10-13 (§30.1); DA-SRC-21 🆕 2026-10-14 (§31.2); DA-SRC-22, DA-SNAP-14 and DA-X-13 🆕 2026-10-14 (§32.1); DA-SRC-24…26 🆕 2026-10-16 (§33.1); DA-OBS-6 🆕 2026-10-17 (§34.1), ✅ 2026-10-29 (M10 merged, §45); DA-OPS-9 🆕 2026-10-22 (§38.1); DA-OPS-10 🆕 2026-10-23 (§39.1). DA-OPS-8 🔄 2026-10-22 (§38). DA-API-11 and DA-API-12 ✅ 2026-10-23 (§40). DA-CLUS-14, DA-OPS-7 and DA-SRC-28 ✅ 2026-10-22 (§37). DA-SRC-27 and DA-X-14 ✅ 2026-10-20 (§35). DA-SNAP-9 ✅ 2026-10-03 (§28). The design decisions were all made 2026-09-26 (§26.5, `V1_Scope_Decisions.md`), and the deferrals confirmed. |
| 🧪 Verify (v1) | **0** | DA-SRC-10/11 proven 2026-09-26 (§21). |
| ⏭ v2 | 14 | Iceberg-sink items (§26.2); DA-SRC-31, decoding on a standby (§60, Q-M12d-1). |
| 🟢 Spec-merge debt | **0** | All applied 2026-09-26 (§26.6): §19.3 S-1…S-6, the DA-X-8 config residual, the D-V1 patches and the Data-Flow Phase 2a probe details. |

---

## 4. Architecture & system scope (SYS)

**State.** `System-Overview.md` fixes the top-level shape: Rust + Arrow + Iceberg + memberlist +
tokio; pipeline as the unit of replication; per-table halt philosophy; "no local state, everything
durable to cloud storage"; app creates/destroys its own DB resources but never deletes Iceberg
artifacts. Most of the original "Open Questions" block in that doc has since been answered by the
research corpus.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SYS-1 | Two-plane architecture (AP coordination ring + CP lease) | 🔴→✅ | ✅ | `Clustering_Architecture_Research.md` §1 — ring decides *should-own*, lease decides *may-run*. | OQ-CL1 |
| DA-SYS-2 | Single-node = one-member cluster, same code path | 🟡 | ✅ | Clustering §10; lease stays on to guard rolling-deploy double-singleton. | OQ-CL4 |
| DA-SYS-3 | "App never deletes Iceberg artifacts" vs maintenance/orphan cleanup ownership | 🟢 | ✅ | `Iceberg_Maintenance.md` — v1 maintenance is external (operator/Spark/Trino); app exposes signals + reserves `sink.maintenance`. | OQ-M2, G-25 |
| DA-SYS-4 | Reserved metadata column collision (`_op/_ts/_lsn/_source_db/_pipeline` vs a source column of the same name) | 🟡 | ✅ | Config spec §5 only says operators "should be aware." **No defined runtime behavior** if a source table actually has an `_op` column — rename? prefix-escape? halt? Needs a rule. See DA-FLOW-5. **✅ 2026-09-26 — D-V1.1 (`V1_Scope_Decisions.md`):** halt the table with `RESERVED_COLUMN_COLLISION` (case-insensitive; checked at resolution + evolution); no escaping. | — |
| DA-SYS-5 | Lineage column `_seq` (per-change order) is required by dedup but absent from the documented lineage column set | 🟡 | ✅ | `Ghost_Row_Dedup_and_Primary_Keys.md` §4 introduces `_seq`; `Data-Flow.md`'s lineage table lists only five `_`-columns and `CdcEvent` carries only commit `source_pos`. The per-change position must be added to both. See DA-FLOW-2. **2026-09-26 tidy:** `Data-Flow.md` lineage table lists `_seq`; `CdcEvent.change_seq` feeds the Phase-3 keep-last collapse. | — |
| DA-SYS-6 | `snapshot_mode: only` referenced but not in the enum | 🟢 | ✅(moot) | The enum is `never\|initial\|always\|dump_always\|dump_initial` (config §9.3). The stray "only" in System-Overview's OQ-M2 is superseded by `dump_*`. Drop the dangling reference. | OQ-M2 |
| DA-SYS-7 | **v1 scope: the Iceberg sink is deferred to v2** — record it in `System-Overview.md`, and decide how v1 treats `sink.type: iceberg` | 🟢 | ✅ | Today the config accepts `iceberg` and the pipeline is refused at start by the capability assertion (DA-SINK-1i). Options: (a) keep that — the enum stays stable across v1→v2; (b) validation error "not available in v1". Recommendation: (a) with an explicit start-refusal message naming v2. **✅ 2026-09-26 — D-V1.4 (`V1_Scope_Decisions.md`):** option (a) — enum kept, refused at start naming v2. Scope statement → spec debt. | — |

---

## 5. Configuration model (CFG)

**State.** `Configuration_Specification.md` is the most mature spec: full type vocabulary, env-var
grammar, two-phase validation (structural Phase 1 / resolution Phase 2), the R/PR/PROC mutability
model, merge/union semantics, the 80+ rule master validation table (§13), and a complete annotated
example. The catalog-auth and schema-evolution surfaces are already merged. The principal CFG gaps
are **not** in the model itself but in **reserved fields awaiting promotion** (§16 of that spec) and
in keeping the table synchronized with research outputs.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-CFG-1 | Config data model fully specified | 🔴→✅ | ✅ | The whole `Configuration_Specification.md`. | OQ-C2, G-1f |
| DA-CFG-2 | Env-var substitution grammar (`${VAR}`, `:-default`, `$$` escape, coercion order) | 🟡 | ✅ | Config §4.3. | — |
| DA-CFG-3 | Secret **rotation** without process restart | 🟡 | ✅ | `credentials_file`/`token_file` (§4.6) + resolved-config diffing (§11.1). | OQ-C3 (partial) |
| DA-CFG-4 | External secret-manager integration (Vault / AWS SM / GCP SM) | 🟢 | ✅ (out of v1) | Only env-var and file-content rotation are designed. Direct secret-manager integration is **undecided** — likely out-of-scope-for-v1, but say so explicitly (file-on-tmpfs by a sidecar is the implied pattern). **✅ 2026-09-26 — confirmed out of v1 (`V1_Scope_Decisions.md` §2).** | OQ-C3 (remainder) |
| DA-CFG-5 | Flush-trigger multi-criteria semantics (OR, first-wins, tri-state null) | 🟡 | ✅ | Config §9.4.1; `Data-Flow.md` §Flush decision. | OQ-P1 |
| DA-CFG-6 | Reserved cluster fields not yet promoted into `globals.cluster` table | 🟢 | ✅ | `lease_duration`, `lease_renewal_interval`, `lease_safety_margin`, `rebalance_stabilization`, `max_concurrent_handoffs`, `max_concurrent_takeovers`, `drain_timeout`, `discovery`, `discovery_dns_name`, `memberlist.lifeguard_enabled` are designed (Clustering §11/§19/§20) but still sit in config §16. Promotion owed. See §19. **Promoted 2026-09-26** — config §6.5 / §6.5.1 (V-094–V-096). | G-8, G-9 |
| DA-CFG-7 | Reserved data-plane fields not yet promoted | 🟢 | ✅ | `tables.primary_key`, `tables.partitioning`, `snapshot.chunk_size`, `sink.max_transaction_batch_rows`, `sink.max_transaction_buffer_bytes`, `sink.buffer_capacity_rows`, `source.postgres.max_wal_lag_bytes`, `source.mysql.binlog_purge_action`, slot-lifecycle fields, `globals.tracing.*`, `sink.metrics.per_table`, `globals.api.role_mapping`, `globals.azure.account_name`, `catalog.retry`, `sink.maintenance`, `glue.strip_schema`. Designed in research; promotion owed. See §19. **Promoted 2026-09-26** — config §6.3/§6.6/§6.7/§9.2/§9.3/§9.4/§9.5 (V-097–V-113); `sink.maintenance` stays reserved per sink in §16; `primary_key`/`partitioning` specified as per-table maps; PG orphan policy named `slot_on_orphan` (`slot` is the name string). | many |
| DA-CFG-8 | Reserved fields now **rejected/moot** and to be struck from §16 | 🟢 | ✅ | `tables.surrogate_key` (G-30 rejected, `Ghost_Row…` §3) and `source.consistent_snapshot` (G-7 moot — incremental snapshot has no weaker mode, `Snapshot_Resumability…` §3). Remove from the deferred list. **Struck from §16 2026-09-26.** | G-30, G-7 |
| DA-CFG-9 | `lease_renewal_interval ≤ lease_duration/3` enforcement | 🟡 | ✅ | Promoted to hard error V-073 (config §13.4); rationale in Clustering §19.3/§19.6. | D-8 |
| DA-CFG-10 | Config reload cross-worker propagation contract | 🔴→✅ | ✅ | Clustering §9 — generation pointer in `globals.state` + gossip nudge; no proxy. | OQ-C1, G-2, G-17, OQ-API3 |

---

## 6. Data flow & pipeline execution (FLOW)

**State.** `Data-Flow.md` defines the four stages, the `CdcEvent` boundary type, per-source decode,
the accumulator, the Iceberg write path, offset acknowledgment, and the task/concurrency model. It is
also the spec that has **drifted most** from the agreed design: it still documents the *blocking
consistent snapshot* (superseded by the incremental chunked model), the scalar `committed_lsn` guard
(generalized to `committed_position`), and a redundant `pipelines/<id>/` state path. These are
documentation gaps, not design gaps, but they are load-bearing for anyone reading the spec as truth.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-FLOW-1 | In-flight buffer handling on pipeline restart | 🟡 | ✅ | Config §12.4 discard-and-replay default; `Pipeline_Lifecycle_State_Machine.md`. | OQ-P2 |
| DA-FLOW-2 | `CdcEvent` lacks a **per-change** position distinct from commit `source_pos` | 🟡 | ✅ | Dedup keep-last needs `_seq` = PG change LSN / MySQL `(binlog_file,end_log_pos)` (`Ghost_Row…` §4); `CdcEvent` currently carries only commit `source_pos` + `xid`. The struct and the accumulator contract must add the per-change sequence. Surfaced by reading FLOW+DEDUP together. **2026-09-26 tidy:** `CdcEvent.change_seq: SourceSeq` (PG change-LSN / MySQL binlog file+pos) in `Data-Flow.md` §CdcEvent; collapse in Phase 3. | — |
| DA-FLOW-3 | Backpressure / bounded channels | 🟡 | ✅ | `Operational…` G-27 — all inter-task channels bounded; `sink.buffer_capacity_rows`. | G-27 |
| DA-FLOW-4 | Phase-1 snapshot model in spec is superseded | 🟢 | ✅ | `Snapshot_Resumability…` replaces blocking COPY/EXPORT_SNAPSHOT with the stream-first incremental chunk loop; `Data-Flow.md` Phase 1 + On-Demand section must be rewritten. Spec-merge debt (§19). **2026-09-26 tidy:** `Data-Flow.md` Phase 1 is the incremental chunked, stream-first model (DF-1/DF-2 applied 2026-06-20). | G-6 |
| DA-FLOW-5 | `TRUNCATE` manifestation in the Iceberg mirror (both engines) | 🟡 | ✅ (Delta) · ⏭ v2 (Iceberg) | `MySQL_DDL_Classifier.md` §3 says TRUNCATE → "table-clear (Iceberg DELETE/overwrite all rows or fresh snapshot boundary), flagged for the sink-writer contract." `Data-Flow.md` Phase 2a lists the PG `T` message but no sink action is specified. The concrete sink behavior (and PG/MySQL parity) is undefined. **2026-09-26 tidy:** `Data-Flow.md` Phase 3 TRUNCATE barrier + Delta realization (metadata-only delete-all, probe P5); the Iceberg overwrite-to-empty goes with the Iceberg sink to v2. | — |
| DA-FLOW-6 | PG `DROP TABLE` detection (no in-band DDL on pgoutput) | 🟡 | ✅ | MySQL DROP is caught by channel B (`QUERY_EVENT`). PG drops are **not** replicated as DDL; the table simply stops emitting and is caught by channel D idle-rescan / channel A on restart (`Schema_Change…` §2.1). The *latency and exact reaction* (halt vs retain) for a PG drop of a tracked table is not spelled out as crisply as MySQL's. Worth an explicit line. **✅ 2026-09-26 — D-V1.2 (`V1_Scope_Decisions.md`):** OID gone from `pg_class` (channel D / A) → halt the table `TABLE_DROPPED`, Delta kept; latency ≤ `rescan_interval`; re-created table does not auto-resume. | — |
| DA-FLOW-7 | `committed_lsn` scalar generalized to engine-tagged `committed_position` | 🟡 | ✅(design) | `Delivery_Guarantee…` §4 — per-table HWM `cdc.committed_position`; `Data-Flow.md` wording owed (§19). | G-14 |
| DA-FLOW-8 | Shutdown / drain ordering | 🟡 | ✅ | `Data-Flow.md` §Concurrency + Clustering §8.3 drain protocol (adds explicit lease release). | OQ-CL3 |

---

## 7. Type mapping (TYPE)

**State.** `Type_Mapping.md` closes the three-hop chain (DB wire → `ColumnValue` → Arrow → Iceberg)
with full PG and MySQL tables, four `ColumnValue` enum extensions, an unsupported-type fallback chain,
and per-column overrides. Essentially complete; residual items are spec-merge and one crate annotation.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-TYPE-1 | Full DB→Arrow→Iceberg mapping table | 🔴→✅ | ✅ | `Type_Mapping.md`. | G-18, OQ-DT1 |
| DA-TYPE-2 | `ColumnValue` enum extensions (tz flag, typed `Array`, `Time`, `UInt64`) | 🟡 | ✅ | `Type_Mapping.md` §extensions; reflected in `Data-Flow.md` enum. | G-NEW-22 |
| DA-TYPE-3 | `BIGINT UNSIGNED` / `numeric(P>38)` overflow handling | 🟡 | ✅ | `decimal(20,0)` for the former; `numeric_overflow_action: string\|error` for the latter. | — |
| DA-TYPE-4 | `TINYINT(1)`→bool detection requiring `information_schema` | 🟢 | ✅ | `tinyint1_as_bool` + `tinyint1_detection`; G-NEW-21 dropped the stale events-mode warning. | G-NEW-21 |
| DA-TYPE-5 | `FixedSizeBinary(16)` → Iceberg `uuid` needs explicit iceberg-rust annotation | 🟢 | ⏭ v2 | `Type_Mapping.md` Hop-3 table flags this as the one Arrow type needing a field-level `{"iceberg.type":"uuid"}` hint — verify the mechanism exists in the pinned crate. See §21. **2026-09-26 tidy:** Iceberg-sink only — deferred with the Iceberg sink to v2. **2026-11-13 (§66):** `main` maps the Arrow `arrow.uuid` extension type to Iceberg `uuid`; 0.11.0 doesn't. | — |
| DA-TYPE-6 | Merge the four TYPE config fields + 4 validation rules into the master table | 🟢 | ✅ | Already largely done (V-078..V-082 present); confirm `column_type_overrides`, `numeric_overflow_action`, `toast_strategy`, `tinyint1_*` all merged (G-NEW-21). Verified 2026-09-26: V-078…V-082 present; V-078 now uses the sink-neutral logical type grammar. | G-NEW-21 |
| DA-TYPE-7 | TOAST unchanged-column strategy (`carry_forward` vs `re_query`) | 🟡 | ✅ | `source.toast_strategy` (PG only); V-077/V-080. Re-query pool-saturation is a warning, not a halt. | — |

---

## 8. Schema change detection & evolution (SCHEMA)

**State.** `Schema_Change_Detection_and_Evolution.md` is a complete, reviewed design: the
four-channel detection model (A startup / B in-band trigger / C catalog augment / D idle rescan), the
§0 ordering invariant (apply in stream order, scoped to the change, never fast-forward to "now"), the
§7.2 safe/unsafe/ignorable DDL classification table, the apply protocol (seal→flush→evolve→rebuild→
resume), idempotent replay, and rename resolution. The MySQL classifier is separately de-risked.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SCHEMA-1 | DDL during live replication — apply protocol | 🔴→✅ | ✅ | §8 detect-at-boundary protocol; startup & non-replayable-gap → re-snapshot (§8.2). | OQ-DT2 |
| DA-SCHEMA-2 | Supported evolution operations (safe/unsafe set) | 🟡 | ✅ | §7.1–§7.2 classification table = the G-19 contract. | OQ-DT3, G-19 |
| DA-SCHEMA-3 | Rename vs drop+add disambiguation | 🟡 | ✅ | §6 — PG `pg_attribute.attnum`; MySQL `RENAME`/`CHANGE` keyword; degrade to drop+add+warn only when unprovable. | DT-R2 |
| DA-SCHEMA-4 | MySQL DDL classifier scope & failure mode | 🟡 | ✅ | `MySQL_DDL_Classifier.md` — two-tier fail-soft (sqlparser-rs → regex → channel C); halt only if no table name recoverable. De-risked from its v7 blocker elevation. | G-19a |
| DA-SCHEMA-5 | Channel-C/A/D require SELECT on `information_schema`/catalogs | 🟡 | ✅ | §9 precondition; checked at pipeline start (runtime, not config). Reuse replication creds. | DT-R8 |
| DA-SCHEMA-6 | `method` field removed; detection is fixed four-channel model | 🟢 | ✅ | §10; config V-024/V-031/V-032 deleted, `interval`→`rescan_interval`. | DT-R1 |
| DA-SCHEMA-7 | ENUM/SET representation (string vs ordinal) | 🟢 | ✅ | `ENUM→string`, `SET→list<string>`; append-only value adds Ignorable, reorder/remove Unsafe. | DT-R5 |
| DA-SCHEMA-8 | iceberg-rust drop/rename/type-promotion support | 🟡 | ⏭ v2 | §7.1 implementation note — only `add_column` is mature; **verify** drop/rename/promotion against the pinned crate, route unsupported ops to halt. See §21. **2026-09-26 tidy:** Iceberg-sink only — deferred with the Iceberg sink to v2. **2026-11-13 (§66):** `add_column` and `delete_column` only; no rename or promotion. | — |
| DA-SCHEMA-9 | Idempotent DDL replay + checkpoint-after-evolve ordering | 🟡 | ✅ | §8.3 — re-read live Iceberg schema, skip if already applied; advance offset only after schema commit. | — |
| DA-SCHEMA-10 | 🆕 MySQL's baseline halted a table whose columns changed | 🟡 | ✅ | §42.1. MySQL's channel C refused the column hint Phase 0's baseline passes, so a fresh start (or a job clearing a halt) halted the table `UNSAFE_DDL` instead of baselining it (§8.2). Fixed in M9b Task 2 (a column hint is the whole table, no rename proven). **✅ 2026-10-28** — M9b merged (PR #14, `90b114c`), its CI green on every MySQL target (§43). | — |

---

## 9. Clustering & coordination (CLUS)

**State.** `Clustering_Architecture_Research.md` is the deepest doc: two-plane model, derived ring,
NodeMeta schema, SWIM-vs-Scuttlebutt decision (keep SWIM+Lifeguard), the storage CAS lease with a
clock-skew-safe acquire/renew/takeover protocol, the renewal-failure taxonomy + safety deadline
(§19), the `ClusterReconciler`+`PipelineController` split (§20.1), takeover rate-limiting (§20.2),
flap immunity (§20.3), config propagation, single-node mode, and the `globals.state` object layout.
All design decisions D-1..D-11 are resolved on paper; **D-5 is the one item that cannot be closed on
paper** and is this report's top blocker.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-CLUS-1 | **Conditional-write CAS conformance on S3+GCS+Azure** | 🔴→✅ | ✅ | **CLOSED 2026-06 — all three backends proven against real services (§24.1); the notes below are the historical trail.** | Clustering §12/§17 — lease rests entirely on atomic CAS on one object; **GCS uses *generation*, not ETag, for write preconditions** (the trap). Trait + T1–T10 suite at `prototypes/lease-conformance/`. **S3 proven (Route A) and GCS proven (Route B native `google-cloud-storage` SDK, T1–T10 incl. the generation-CAS gate), 2026-06** — findings folded into §24.1. **Only Azure still owed.** The last clustering item not closeable on paper. | D-5 |
| DA-CLUS-2 | Split-brain exclusive ownership (the lease) | 🔴→✅ | ✅ | §5 storage CAS lease; ring is scheduling, lease is safety. | G-8, OQ-CL1 |
| DA-CLUS-3 | Sink-side fencing for a zombie holder | 🟡 | ✅(design) | §5.5 — `owner_epoch` in Iceberg snapshot summary + Iceberg atomic-commit fence + `_lsn` dedup (defense in depth). Depends on DA-SINK-2 (snapshot-summary persistence). | D-7 |
| DA-CLUS-4 | Lease renewal failure taxonomy (412 vs transient) + safety deadline | 🟡 | ✅ | §19 — `deadline = last_renew_local + lease_duration − guard`; renewer state machine; commit gating. Prevents a transient S3 blip from stopping a healthy (esp. single-node) worker. | D-8 |
| DA-CLUS-5 | Rebalancing / graceful drain / handoff | 🔴→✅ | ✅ | §8.3 drain (flush→commit→checkpoint→release lease) + §8.5 rate-limiting; planned vs unplanned paths separated. | G-9, OQ-CL3 |
| DA-CLUS-6 | Reconciler ↔ renewer orchestration (head-of-line blocking) | 🟡 | ✅ | §20.1 — `ClusterReconciler` (ring only) + per-pipeline `PipelineController` (sole lease-writer); level-triggered + periodic resync. | D-9 |
| DA-CLUS-7 | Unplanned-takeover storm rate-limiting | 🟢 | ✅ | §20.2 — `max_concurrent_takeovers` (default 4) caps concurrent Phase-0 starts, with jitter. | D-10 |
| DA-CLUS-8 | Flap immunity (suspect stays in ring; symmetric stabilization) | 🟢 | ✅ | §20.3 — only `dead`/`left` removes from ring; `rebalance_stabilization` on all additions incl. rejoins. | D-11 |
| DA-CLUS-9 | `node_id` stability / StatefulSet mandate | 🟡 | ✅ | §3.4 D-1 — `node_id = memberlist.node_name`; k8s StatefulSet pod name; standalone persisted UUID; `incarnation` distinguishes a fresh process. | D-1, OQ-P3 |
| DA-CLUS-10 | Cluster-wide-consistent anchors (`tokens`, `lease_duration`, `lock_backend`, `hash_algo`) | 🟡 | ✅ | §12 anchor; refuse-to-start on mismatch (V-026/V-062/V-075/V-076). | G-36, D-2 |
| DA-CLUS-11 | Lease timing defaults vs failover SLA | 🟡 | ✅ | §18 D-6 — **Balanced** ratified (`lease_duration=15s`, `renewal=5s`, ~20–22s unowned window). Tunable. | D-6 |
| DA-CLUS-12 | `kubernetes` lock_backend (K8s Lease API) | 🟢 | ✅ (deferred) | Reserved as a future `LeaseStore` impl; **only `storage` is built for v1** (V-075). A genuine but deliberately deferred scope item. **✅ 2026-09-26 — deferral confirmed (`V1_Scope_Decisions.md` §2).** | D-2 |
| DA-CLUS-13 | Per-source-endpoint takeover bulkhead | 🟢 | ✅ (deferred) | §20.2 notes the sharper risk (many pipelines on one source DB reconnecting together) as a **future** per-endpoint concurrency group; v1 cap is per-node-global. **✅ 2026-09-26 — deferral confirmed (`V1_Scope_Decisions.md` §2).** | D-10 |
| DA-CLUS-14 | A lease store and a lake that disagree on the epoch (a moved, restored or deleted `globals.state`) | 🟢 | ✅ | ✅ 2026-10-22 (§37): a fenced worker's claim takes its own lease over at `max(stored + 1, current_epoch + 1)` in one step (`Leases::take_over_at`, `Claim::at_least`); a second fence after that jump is an `ERROR` naming another lease store, with the lease kept; a stop spends the jump, so the ERROR's remedy (stop, resume) climbs once more. Another holder's lease is never touched by the floor. M8a Task 8 (D-M8a.14) | — |
| DA-CLUS-15 | 🆕 DNS discovery asks again only while a node has no peer | 🟢 | ✅ | §47.1. With four or more pods started together (the chart's `Parallel`), nodes that each found some, not all, of the others can form groups that never merge. **2026-10-30 (§50): stays 🟥;** `configuration/cluster.md` and `runbooks/failover.md` warn, give the check (`GET /v1/cluster/status` on each node) and the remedy; the fix stays in the code. **✅ 2026-11-06 (§57):** `discovery.rs` `discover`: every node resolves its seeds each `REDISCOVER` (5 s) and announces to those that aren't members; `groups_that_each_have_a_peer_ask_their_seeds_again_and_merge`. The docs' caution is gone (`configuration/cluster.md`, `runbooks/failover.md`) | — |

---

## 10. Snapshotting (SNAP)

**State.** `Snapshot_Resumability_and_Consistency.md` replaces the blocking snapshot with an
**incremental chunked, stream-first** model (DBLog/Debezium-style watermarks + PK-upsert
reconciliation), which simultaneously closes resumability, multi-table consistency, and the on-demand
read-point hazard. This is a *change* to `Data-Flow.md` Phase 1, so it carries spec-merge debt.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SNAP-1 | Mid-snapshot crash resumability (cursor format) | 🔴→✅ | ✅ | §2 typed identifier-tuple keyset cursor; checkpoint per chunk; resume idempotent. | G-6, OQ-S1 |
| DA-SNAP-2 | Multi-table consistency without a global lock | 🟡 | ✅ | §3 — convergent, not frozen-instant; no `RELOAD`/`LOCK TABLES`/`EXPORT_SNAPSHOT` needed; no `consistent_snapshot` toggle. | G-7, OQ-S2 |
| DA-SNAP-3 | On-demand snapshot read-point (the "pause_lsn+1" hazard) | 🟡 | ✅ | §4 — on-demand is the §1 chunk loop with **no stream pause**; watermarks reconcile. | G-20, OQ-S3 |
| DA-SNAP-4 | Snapshot parallelism within a pipeline | 🟢 | ✅ | §5 — reuses `parallelism`; disjoint tables per worker; serial keyset within a table. | OQ-S4 |
| DA-SNAP-5 | Keyset collation correctness (text identifiers) | 🟡 | ✅ | §2 collation pin — binary collation (`COLLATE "C"` / `utf8mb4_bin`) for the pagination predicate, recorded in the cursor. Subtle but load-bearing. | — |
| DA-SNAP-6 | `snapshot.chunk_size` config field | 🟢 | ✅ | Default 10000; promotion into config owed (§19). Promoted 2026-09-26 as `tables.snapshots.chunk_size` (V-108). | G-6 |
| DA-SNAP-7 | Snapshot vs WAL/binlog retention coupling | 🟡 | ✅ | §7 cross-ref — a slow snapshot lengthens the stream backlog; interacts with the WAL-lag guard (G-22) and binlog purge (G-26). Designed but the interaction tuning is an operational note, not a hard rule. **✅ 2026-11-06 (§57):** closed with a pointer: an incremental snapshot runs beside the stream, whose position advances with each flush, so a slow snapshot holds back only the WAL or binlog the stream hasn't flushed; the guard (`max_wal_lag_bytes`) and `binlog_purge_action` cover the rest (`architecture/snapshots.md`) | — |
| DA-SNAP-8 | PK-less table under a **dump-only** mode (snapshot, never stream) | 🟡 | ✅ | The PK-less halt rule (DA-DEDUP-3) is justified by equality-delete keying — but a `dump_*` table is insert-only and never deletes, so the keying argument doesn't apply. Whether a dump-only PK-less table may be snapshotted (or still halts) is **unspecified**. Newly surfaced edge case. **2026-09-26 tidy:** decided by spec: the identifier precondition (config §11.2) applies in every mode — explicit PK-less table halts, glob-matched one is skipped; `tables.surrogate_key` struck (G-30). Dump-only PK-less tables are not supported in v1. | — |
| DA-SNAP-9 | The snapshot watermark can't order a chunk's MVCC snapshot against the stream | 🟡 | ✅ | Closed 2026-10-03 (§28): each chunk read records its `pg_current_snapshot()`, the writers in flight at the session's start are waited for, and the reconciler drops a chunk row a stream transaction the read didn't see already wrote (Snapshot_Resumability §1.1; Data-Flow Phase 1, S-T). Measured: M6a Task 13's busy-snapshot and stalled-synchronous-commit tests. | — |
| DA-SNAP-10 | MySQL's window rule may miss a transaction the binlog delivered before InnoDB made it visible | 🟡 | ✅ | §28.1. MySQL/MariaDB chunks carry no read visibility, so the reconciler uses the window rule: only transactions applied after a read was issued are filtered. Group commit makes the binlog readable (sync stage) before the InnoDB commit (commit stage); semi-sync `AFTER_SYNC` waits for the replica ack between them, up to `rpl_semi_sync_source_timeout`. A transaction applied from the binlog before a chunk is issued can then still be invisible to that chunk's read, and a stale chunk row would undo it, the MySQL analogue of DA-SNAP-9's stalled synchronous commit. Reasoned (M6a Task 9 review), not measured. Close before M7: an S-MY probe that stalls a semi-sync ack, then the fix (e.g. a per-read visibility signal, or waiting for in-flight commits as Q-M6a-2 does on PostgreSQL). **🔄 2026-10-14 (§31): measured and decided.** S-MY11 measured it on MySQL 8.4 and MariaDB 10.11 / 11.8 (semi-sync AFTER_SYNC; MariaDB's default AFTER_COMMIT has no gap). Fix (Q-M7-3): a MySQL chunk reads with `LOCK IN SHARE MODE` in its short READ COMMITTED transaction, which waits for the stalled commit and sees it (S-MY11; its cost, S-MY11b); `Data-Flow.md` Phase 1 step 2b/2c, `Snapshot_Resumability…` §1.1. Closes when M7 builds it with its test. **2026-10-14 (§32):** built and tested in M7a's plan (Task 10, `a_locking_read_waits_for_a_writer_of_its_rows`, on MySQL 8.4 and MariaDB 11.8); ✅ when M7a merges. **✅ 2026-10-16:** M7a merged (PR #9) with the locking chunk read and its test. | — |
| DA-SNAP-11 | 🆕 The reconciler's log is unbounded while a snapshotting table's reads wait | 🟢 | ✅ | §28.2. Every stream transaction that writes a snapshotting table is logged with its keys until a chunk proves it visible. While the table's reads wait (an in-flight writer at session start, a drift hold), no chunk is reconciled and the log grows with every key written, for up to `timeouts.snapshot`. M6a warns once per wait and reports the log's size in the status (M6b's gauge); bounding it (coalesce per table, or skip logging until a read can be issued) is open. **✅ 2026-11-06 (§57):** `reconcile.rs`: a key logged once, under its latest writer, the log bounded by the distinct keys written during the wait; `a_key_written_again_is_logged_once_under_its_latest_writer` | — |
| DA-SNAP-12 | 🆕 `parallelism > 1` reads chunks one after another on PostgreSQL | 🟢 | ✅ | §28.2. Chunk reads share the one catalog connection, so they pipeline rather than run in parallel; each read's client-side bound starts at spawn, so a read queued behind slow chunks can time out and the abandoned connection fails its neighbours. Fix: a small read pool or a connection per concurrent read. **✅ 2026-11-06 (§57):** `connect.rs` `Connector::read`: each chunk read on a connection of its own, `IDLE_READS` (4) kept; `a_waiting_chunk_read_holds_up_no_other_read` | — |
| DA-SNAP-14 | 🆕 An `ENUM` key whose members are reordered during a snapshot | 🟢 | ✅ | §32.1. An `ENUM` key sorts by member index, and a chunk resumes after its cursor's label at that label's index under the members read for the chunk. A reorder mid-snapshot changes the order, so the next chunk can skip or repeat rows. The stream halts the table at the reorder (§7.2), but a chunk read between the DDL and the signal doesn't notice: the read plan compares value types, text for an `ENUM` either way. **✅ 2026-11-04 (§53):** MySQL `snapshot.rs` `moved`: a reordered, removed or renamed `ENUM`/`SET` key member is a change under the snapshot (the stream halts the table `UNSAFE_DDL`) | — |
| DA-SNAP-13 | 🆕 A wiped state with an existing sink table baselines nothing | 🟢 | ✅ | §28.2. With no cached schema, Phase 0 doesn't baseline (§8.2) against the sink table's schema; a shape mismatch surfaces later as `SINK_REJECTED` (visible, not silent). Only the adopted-orphan case with a cache is covered. **✅ 2026-11-04 (§53):** `schema_change.rs` `missing_columns`, the worker's baseline against the sink table; `a_wiped_state_baselines_against_the_sink_table`; a sink column matching a source column by case alone halts `SCHEMA_MISMATCH` (Delta names are case-insensitive): `a_sink_column_matching_by_case_alone_halts` | — |
| DA-SNAP-15 | 🆕 A job upserts: a row deleted at the source while its table wasn't streamed stays | 🟡 | ✅ | §42.1. Q-M9b-2: no truncate. M11's docs (drop or move the Delta table first); a truncate option later. **🔄 2026-10-30 (§50):** documented in `architecture/snapshots.md`, `runbooks/snapshot-jobs.md`, `runbooks/halted-table.md`; the truncate option stays open. **✅ 2026-11-06 (§57):** `truncate: true` on the job (Q-M12-7): an empty snapshot change set with `truncate`, committed before the start's record (Data-Flow On-Demand Snapshot); `a_truncating_job_drops_the_rows_deleted_meanwhile`, `a_job_s_truncate_empties_the_table_and_keeps_the_marker` | — |
| DA-SNAP-16 | 🆕 Job documents accumulate, and the list and the ask's check read every one | 🟢 | ✅ | §42.1. A retention, or an index of the unfinished jobs. **✅ 2026-11-06 (§57):** `KEPT_ENDED_JOBS` (100), `prune_snapshot_jobs` after each ended job, `VersionedStore::delete`; `pruning_keeps_the_newest_ended_jobs_and_every_unfinished_one`, `a_job_that_ends_prunes_the_oldest_ended_jobs` | — |
| DA-SNAP-17 | 🆕 A job table's halt fails the job only with the session's next flush | 🟢 | ✅ | §42.1. Write a job table's halt at once, or M11's docs. **✅ 2026-10-30 (§50, documented):** `runbooks/snapshot-jobs.md`. | — |
| DA-SNAP-18 | 🆕 A table created again while the pipeline was down takes two jobs | 🟢 | ✅ | §43.1. The first fails it `TABLE_DROPPED`, the second clears the halt. M11's docs, or one pass once proven. **✅ 2026-10-30 (§50, documented):** `runbooks/halted-table.md`. | — |
| DA-SNAP-19 | 🆕 A copy's halt shows as halted until the pipeline's next start | 🟢 | ✅ | §43.1. Clear it when the owner fails the job table. **✅ 2026-11-06 (§57):** `TableHalt.copy`: a copy's halt fails its job table and is kept out of the halted tables, the metric and the API (`jobs.rs` `a_copy_that_can_t_be_read_halts_alone`, `a_copy_halted_in_a_run_fails_its_job_and_its_halt_goes`, and after a reconnect `a_copy_s_halt_stays_out_of_the_halted_tables_after_a_reconnect`) | — |
| DA-SNAP-20 | 🆕 An undecodable job document stops the pipeline's start | 🟢 | ✅ | §43.1. Skip it with a warning. **✅ 2026-11-06 (§57):** `snapshot_jobs` skips an undecodable job with a warning; `an_undecodable_job_is_skipped_by_the_listing` | — |
| DA-SNAP-21 | 🆕 A job whose restart wasn't written stays queued until the next start | 🟢 | ✅ | §43.1. Write the restart again when asked again. **✅ 2026-11-06 (§57):** a `409` against a queued job writes its restart again (`ask_restart_again`) | — |
| DA-SNAP-22 | 🆕 The wiped-state baseline's limits | 🟢 | ✅ | §54.1. An uncached `Plan::Resume` table whose sink table exists isn't baselined; a non-null sink key column the source lacks is accepted until the first MERGE fails (`SINK_REJECTED`). Also the via-root repair's re-snapshot of every table, kept conservative until measured. Candidate: baseline any uncached table the sink holds; refuse a non-null sink field the source lacks. **✅ 2026-11-10 (§60):** the baseline arm is `None if state.exists` (any first-tracked sink table, a resume's too: `a_table_first_tracked_at_a_resume_is_baselined_against_its_sink_table`); `missing_columns` halts `SCHEMA_MISMATCH` on a non-null sink column the source lacks (`a_sink_table_with_a_non_null_column_the_source_lacks_halts`); a via-root repair is no longer `rewritten`, its hidden leaves are in `added` and copied alone (IT `a_via_root_repair_copies_only_the_partitions_it_hid`, PostgreSQL 18 and 14). Left: DA-SNAP-23 | — |
| DA-SNAP-23 | 🆕 On MySQL the first-met baseline halts a column whose name changed only its case | 🟢 | ✅ | §60.1. `missing_columns` folds as Delta does and doesn't ask `column_names_fold`, so a sink table the pipeline first meets (a wiped state, a job's copy) whose source column was recased meanwhile halts `SCHEMA_MISMATCH`, which the cached path (DA-SRC-25) accepts. Conservative (D-M12d.1): a halt, no loss; drop the Delta table and copy it again. Candidate: take the sink's spelling where the source folds names. **✅ 2026-11-11 (§63):** where `column_names_fold` holds, a sink column that matches a source column by case alone is that column: no add, written under the sink's spelling, in the key too (Task 4, `7c33b77`, D-M12c.5; `where_names_fold_a_sink_column_matching_by_case_alone_is_the_column`, `a_first_met_table_whose_names_changed_case_alone_is_baselined_where_names_fold`, E2E `a_lost_state_meets_a_recased_column_as_the_same_column` on `mysql-84` and `mariadb-118`); PostgreSQL still halts, naming both spellings. Spec: Schema Change §7.2 (P-3). | — |

---

## 11. Delivery guarantee, dedup & primary keys (DEDUP)

**State.** `Ghost_Row_Dedup_and_Primary_Keys.md` (G-13/G-30) and
`Delivery_Guarantee_and_Position_Guard.md` (G-14) together make the pipeline **row-level idempotent**:
MOR equality-delete keyed on the table identifier, intra-batch collapse to ≤1 delete + ≤1 data record
per key ordered by `_seq`, mirror-delete semantics (no `D` data row), and a `SourcePosition`
abstraction (numeric LSN vs GTID-set subset/union/intersect). Idempotency is the correctness basis;
the position guard is demoted to optimization + checkpoint-advance.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-DEDUP-1 | Ghost-row / equality-delete keying correctness | 🔴→✅ | ✅ | §0 strictly-less-than rule; in-batch collapse §4. | G-13 |
| DA-DEDUP-2 | Delivery-guarantee position comparison (LSN vs GTID-set) | 🔴→✅ | ✅ | `SourcePosition` trait; canonical GTID-set serialization; per-table HWM; resume = laggard (min/intersect). | G-14 |
| DA-DEDUP-3 | PK-less tables | 🟡 | ✅(by rejection) | §3 — halt; `surrogate_key` removed; accepted limitation (genuinely key-less tables can't replicate). | G-30 |
| DA-DEDUP-4 | Identifier source & override | 🟡 | ✅ | Inferred PK via channel C; `tables.primary_key` override (not a surrogate); Iceberg identifier constraints (top-level, primitive, NOT NULL, non-floating). | D-13.1/7 |
| DA-DEDUP-5 | Before-image / REPLICA IDENTITY contract | 🟡 | ✅ | §2 — identifier ⊆ replica-identity (PG); MySQL FULL image already required; halt with named columns if unmet. | D-13.4 |
| DA-DEDUP-6 | Key-mutating UPDATE (PK value changes) | 🟡 | ✅ | §4 — delete(old key) + data(new key); reuses before-image. | D-13.3 |
| DA-DEDUP-7 | Cross-table atomic visibility **not** guaranteed | 🟡 | ✅ (scoped-out) | `Delivery…` §5 / `Large_Transaction…` — each table is its own Iceberg commit; cross-table atomicity is explicitly out of scope; would need a multi-table-commit catalog (Nessie). Documented limitation, reopen path noted. **✅ 2026-09-26 — confirmed out of scope (`V1_Scope_Decisions.md` §2).** | D-14.5, G-25-noted |

---

## 12. Catalog & authentication (CAT)

**State.** `Catalog_Auth_Research.md` closed G-1g and the schema is fully merged into config §9.5 and
the §13 validation rules (V-040..V-048). Hive Thrift `none/ldap/kerberos`; REST HTTP
`none/bearer/oauth2/sigv4`; the deprecated built-in `/v1/oauth/tokens` warning.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-CAT-1 | Catalog auth model (Hive SASL / REST OAuth2/bearer/sigv4) | 🔴→✅ | ✅ | `Catalog_Auth_Research.md` §2–§3; config §9.5. | G-1g |
| DA-CAT-2 | Primary catalog vs multi-catalog at launch | 🟢 | ✅ | All three backends (glue/hive/rest) are first-class via the union; choice is per-pipeline. | OQ-IC2 |
| DA-CAT-3 | OAuth2 must point at an external IdP | 🟡 | ✅ | `token_url` external; V-048 warns on the deprecated built-in endpoint. | — |
| DA-CAT-4 | Credential vending interaction with the sink credential model | 🟢 | ⏭ v2 | §3.5 is informational only — if the catalog vends short-lived table-scoped storage creds, the `globals.cloud` chain becomes optional, but **how the writer consumes vended creds is not specified**. Minor; flag for the sink-writer contract. **2026-09-26 tidy:** Iceberg-sink only — deferred with the Iceberg sink to v2. | — |
| DA-CAT-5 | Catalog unavailability retry policy (separate from object storage) | 🟡 | ✅ | `Operational…` G-35 — own retry + circuit breaker + DEGRADED; optional `catalog.retry`. | G-35 |

---

## 13. Sink / Iceberg writer (SINK)

**State.** The writer contract is spread across `Ghost_Row_Dedup…` (equality-delete keying),
`Iceberg_Partitioning…` (partition spec + before-image routing), `Iceberg_Maintenance…` (external
maintenance + signals), `Large_Transaction…` (split commits), and `Delivery…` (snapshot-summary
properties). The design is coherent; the open items are the **two crate-capability gates** that the
writer's correctness literally depends on.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SINK-1 | **Equality-delete writer assigns delete files the enclosing commit's sequence number** | 🔴→🟡 | ⏭ v2 | **Superseded:** the real finding is "no delete-commit action at all" (§24.2.1–§24.2.4). **Re-scoped 2026-09-26 to DA-SINK-1i (Iceberg mirror-mode capability)** by the multi-sink design (§25); Delta sink is the v1 mirror. Original note: **2026-09-26 tidy:** Iceberg-sink only — deferred with the Iceberg sink to v2. | `Ghost_Row…` §8 — the entire strictly-less-than ghost-row guarantee assumes this iceberg-rust 0.9.0 behavior; **verify before building the writer**. If unsupported/mis-sequenced, need position-deletes-after-read or a different strategy. | G-13 §8 |
| DA-SINK-2 | Custom snapshot-summary properties (`cdc.owner_epoch`, `cdc.committed_position`) preserved across commits | 🟡 | ✅ (append path) | Required by CLUS fencing (D-7) and DEDUP guard (D-14.4); `Clustering…` §5.5 flags "verify iceberg-rust preserves custom snapshot-summary properties." See §21. **2026-09-26 (probe G2b):** the `cdc.owner_epoch` table-property carrier commits in the same metadata commit as the append, all-or-nothing, and survives foreign commits + expiry. Delete-commit path still waits on DA-SINK-1i. | D-7 **2026-09-26 tidy:** delete-commit path ⏭ v2 with the Iceberg sink. |
| DA-SINK-3 | Write strategy COW vs MOR | 🟡 | ✅ | MOR + equality deletes is the default (`Ghost_Row…` §5); COW new-tables-only. | OQ-IC1 |
| DA-SINK-4 | MOR↔COW strategy change migration | 🟢 | ✅ | `Iceberg_Partitioning…` §4 (G-12a) — new-data-only; explicit compaction/re-snapshot to migrate; PR. | G-12a |
| DA-SINK-5 | Partition spec config + evolution | 🟢 | ✅ | `Iceberg_Partitioning…` §1/§3 (G-24) — `tables.partitioning`; default unpartitioned; spec evolution = new-data-only. | G-24 |
| DA-SINK-6 | Partition-key-changing UPDATE (cross-partition ghost) | 🟡 | ✅ | §2 (D-24) — equality-delete routed to the **old** partition via before-image; same machinery as a PK mutation. **Sink-writer contract addition.** | G-24 |
| DA-SINK-7 | Large-transaction vs txn-atomic-flush tension | 🟡 | ✅ | `Large_Transaction…` (G-29) — bounded split commits (default off → atomic), `max_transaction_buffer_bytes` halt-backstop; crash-safe via idempotency. | G-29 |
| DA-SINK-8 | Spill-to-disk for oversized txns | 🟢 | ✅ (deferred) | `Large_Transaction…` §5 — future `sink.large_txn_mode: split\|spill`; out of scope for v1. **✅ 2026-09-26 — deferral confirmed (`V1_Scope_Decisions.md` §2).** | G-29 §5 |
| DA-SINK-9 | Iceberg maintenance ownership (compaction/expiry/orphans) | 🟢 | ✅ | `Iceberg_Maintenance…` (G-25) — external for v1; app exposes signals + reserves `sink.maintenance`; built-in gated on iceberg-rust rewrite support. | G-25 |
| DA-SINK-10 | iceberg-rust compaction/rewrite API availability | 🟢 | ⏭ v2 | Gates any **future** built-in maintenance (deferred anyway). Verify against pinned crate. **2026-09-26 tidy:** Iceberg-sink only — deferred with the Iceberg sink to v2. **2026-11-13 (§66):** a COW rewrite primitive is on `main` (#2752), with no rewrite-files commit (#3046 open). | G-25 §14 |
| DA-SINK-11 | Exactly-once vs at-least-once into Iceberg | 🟡 | ✅ | Effectively exactly-once-effect via idempotency (DEDUP) + position guard; at-least-once delivery, dedup absorbs replays. | OQ-IC3 |
| DA-SINK-12 | 🆕 A case-only `ADD COLUMN` loops the evolver | 🟢 | ✅ | §54.1. delta-rs rejects an `ADD COLUMN` matching a Delta column by case alone as a duplicate; transient, so the evolver retries without end. Predates M12a. Candidate: `missing_columns`' case rule in the evolver, halting `SCHEMA_MISMATCH`, after measuring delta-rs's case folding (ASCII or Unicode). **✅ 2026-11-10 (§60):** wider than recorded: a table created with two such columns looped too. `core::differ_by_case_alone` (Unicode `to_lowercase`, as `buoyant_kernel` 0.28.1 folds; `delta_rs_refuses_a_column_beside_one_that_differs_by_unicode_case`): `create` refuses a case clash `Permanent` (`SINK_REJECTED`, `a_table_whose_columns_differ_by_case_alone_is_refused`), `evolve` returns `Unsupported` naming both spellings (`EVOLUTION_UNSUPPORTED`, `a_column_that_differs_from_one_by_case_alone_is_unsupported`), `missing_columns` halts `SCHEMA_MISMATCH` (`a_sink_column_matching_by_case_alone_halts`); Schema_Change §7.2 (P-4) | — |

---

## 14. Source connection lifecycle (SRC)

**State.** `Postgres_Slot_and_Publication_Lifecycle.md` (G-15) closes the PG runtime lifecycle;
`Operational…` covers MySQL binlog purge (G-26) and the WAL-lag guard (G-22). MariaDB is in scope via
the MySQL path with one genuine correctness dependency.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SRC-1 | PG slot existence state machine + orphan policy | 🟡 | ✅ | §1 — `slot.on_orphan: halt(default)\|adopt_resnapshot`; resume-without-snapshot rejected. | G-15 |
| DA-SRC-2 | Slot creation responsibility | 🟡 | ✅ | §2 — `slot_management: managed(default)\|require_existing`; the latter forces `auto_cleanup=false`. | G-15 |
| DA-SRC-3 | Slot loss / WAL-gone recovery | 🟡 | ✅ | §3 — recreate slot + incremental re-snapshot (G-6); warn/metric. | G-15 |
| DA-SRC-4 | Publication reconciliation | 🟢 | ✅ | §4 — app-maintained explicit table list; `ALTER PUBLICATION` on table-set change; never `FOR ALL TABLES`. | G-15 |
| DA-SRC-5 | Abandoned-slot WAL-lag guard | 🟡 | ✅ | `Operational…` G-22 — `data_avalanche_wal_lag_bytes` always emitted; `max_wal_lag_bytes` opt-in halt. | G-22 |
| DA-SRC-6 | MySQL purged-binlog handling | 🟡 | ✅ | `Operational…` G-26 — `binlog_purge_action: error(default)\|re_snapshot`. | G-26 |
| DA-SRC-7 | External same-name slot collisions undetectable | 🟢 | ✅(limitation) | §5 — PG slots carry no ownership tag; mitigated by V-058 uniqueness + operator discipline; documented, not papered over. | G-15 |
| DA-SRC-8 | MariaDB GTID dialect (`domain-server-sequence`) | 🟡 | ✅ | `Operational…` G-38 — MariaDB accepted via the MySQL path, but the `SourcePosition` MySQL impl **must support both GTID dialects** (MariaDB domains vs MySQL UUIDs); flagged as a **new sub-research item** and the one genuine correctness dependency. **2026-09-26: feasible — proven** (S-MY9, `mysql_async`): MariaDB GTID-positioned resume via `@mariadb_slave_capability`/`@slave_connect_state`/`@slave_gtid_strict_mode`; per-txn GTIDs decode exactly (last = `gtid_binlog_pos`) with a ~10-line parser. Remaining: confirm MariaDB in v1 scope (recommended). **✅ 2026-09-26 — D-V1.5 (`V1_Scope_Decisions.md`):** MariaDB in v1; dialect selected at connect. | G-38 |
| DA-SRC-9 | TOAST re-query control-connection pool sizing | 🟢 | ✅ | V-077 warns on `re_query`+high-volume snapshot; concrete pool sizing is an implementation detail, not yet fixed. **✅ 2026-11-04 (§53):** decided: one catalog connection per pipeline serves channel C, the chunks and the re-queries (batched per commit); V-077 warns; throughput measured in M12c | — |
| DA-SRC-10 | **PG replication client crate capability** (`pgwire-replication` named in Sources_Index; never pinned, compiled or probed) | 🔴 | ✅ | Needed: pgoutput proto v1–v4 incl. streamed in-progress txns (`Stream Start/Stop/Commit/Abort`), `Relation`/`Type`/`Truncate` messages, unchanged-TOAST marker, standby status feedback (flush-LSN ack), slot `START_REPLICATION` from an LSN, TLS, keepalives. Probe: `prototypes/source-capability/` (S-gates) against local PostgreSQL. A negative result means writing/forking the protocol layer — a plan-shaping item. **Scaffolded 2026-09-26, source-read:** the crate is **transport-only** — pgoutput decoding (Relation/Insert/Update/Delete/Truncate/Type/Origin) is a stated non-goal, so **the decoder is ours** (bounded v1 work item); `proto_version '1'` is hard-coded with no `streaming` (large txns arrive whole at commit — fine for the design, but `Data-Flow.md` Phase 2a says `'2'`). Gates S-PG0…S-PG8 in `prototypes/source-capability/README.md`. **Proven 2026-09-26** (`prototypes/source-capability/`, PG 17): S-PG0…S-PG8 9/9 — transport, our pgoutput decoder (Relation/Type/Origin/Insert/Update none·K·O/Delete K·O/Truncate), unchanged-TOAST `u`, 50k-row txn whole at commit under proto v1, restart/resume at-least-once, slow consumer survives `wal_sender_timeout`, live `ALTER PUBLICATION ADD TABLE`, 18-type text decode, TLS 1.3 verify-full. **Pin `pgwire-replication =0.4.1`; the pgoutput decoder is a v1 work item.** Implementation rules: `with_wakeup_interval` ≈ status interval (idle acks otherwise lag up to 10 s); pin the walsender session (`TimeZone=UTC`, `DateStyle=ISO`, `IntervalStyle=iso_8601`, `extra_float_digits=3`). | — |
| DA-SRC-11 | **MySQL binlog client crate capability** (`mysql-binlog-connector-rust` named in Sources_Index; never pinned, compiled or probed) | 🔴 | ✅ | Needed: GTID-set positioning (`COM_BINLOG_DUMP_GTID`), `ROW` events with FULL image, `TABLE_MAP` optional metadata (column names/signedness, `binlog_row_metadata=FULL`), `QUERY_EVENT` (DDL classifier input), CRC32 checksums, heartbeats, MySQL 8.4 `caching_sha2_password` auth + TLS; MariaDB GTID dialect if in v1 scope (DA-SRC-8). Probe: `prototypes/source-capability/` against local MySQL (and MariaDB). **Scaffolded 2026-09-26, source-read:** two candidates probed side by side — (a) `mysql-binlog-connector-rust` 0.3.3 (TLS looks absent, claims ≤ MySQL 8.0.31, has MariaDB GTID events) vs (b) `mysql_async` 0.37.1 binlog stream (TLS ✅, active; MariaDB GTID events not found in `mysql_common`). Neither complete on paper. Gates S-MY0…S-MY9. **Proven 2026-09-26** (MySQL 8.4 + MariaDB 11.4): **pin `mysql_async =0.37.1`** — S-MY0…S-MY9 10/10 (GTID resume, FULL images, optional metadata incl. UNSIGNED, DDL `QUERY_EVENT`, heartbeats via `@master_heartbeat_period`, zstd payloads, type decode, TLS 1.3, MariaDB GTID). **Reject** `mysql-binlog-connector-rust` 0.3.3: no TLS, no MariaDB GTID positioning, `is_signed` carries the *unsigned* bit, `BIGINT UNSIGNED` as `-1`. Own code: MariaDB `GTID_EVENT` (type 162) decode; payload guard (DA-SRC-12). | — |
| DA-SRC-12 | **`mysql_async` silently drops a compressed transaction it cannot decompress** | 🟡 | ✅ (guard designed) | Found 2026-09-26 (source-read, `mysql_common` 0.37.3 / `mysql_async` 0.37.1): on a zstd error `TransactionPayloadEvent::danger_decompress` returns an **empty buffer**, and `mysql_async` ignores a payload-header parse error (`/* TODO: Log the error */`) — the transaction's rows vanish with no stream error. Rare (corrupt payload / unsupported algorithm) but silent data loss. Guard (v1): validate every `TRANSACTION_PAYLOAD` ourselves — decompressed length == `uncompressed_size`, ≥ 1 inner event — and halt the pipeline on mismatch; offer the fix upstream. **✅ 2026-09-26 — D-V1.6 (`V1_Scope_Decisions.md`):** validate each payload; pipeline halt `BINLOG_PAYLOAD_INVALID`; report upstream. **2026-10-14 (§32):** M7a's plan builds it as a count-down of the payload's `uncompressed_size` by the events it yields (Task 6, measured equal to the decompressed length on MySQL 8.4), halting with a `BINLOG_PAYLOAD_INVALID: ` message; the fault-injection E2E is M7b's. | — |
| DA-SRC-13 | Publication blind spots | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-11-04 (§53):** `slot.rs` `publication_shape`: under `require_existing` or `FOR ALL TABLES` a tracked table published with a row filter or a column list halts itself (`halt_filtered_tables`, `TABLE_NOT_REPLICABLE`: publish it whole in one transaction, then re-snapshot it) and the other tables go on; stored generated columns are refused pipeline-wide; under `managed` the publication is set to exactly the tracked set (a partitioned root or a `TABLES IN SCHEMA` member is replaced by the tracked tables, a table published before is added by name without a copy); extra tables warned (a managed one drops them); a change to what a managed publication publishes of a tracked table (a filter, a column list, a partition-root flag, the operations, stored generated columns) recreates the slot and re-snapshots the pipeline's tables as `snapshots.mode` says (`prepare`, `rewritten`); ITs `a_publication_s_blind_spots_are_refused_warned_or_repaired`, `a_managed_publication_of_a_partitioned_root_is_set_to_the_tracked_leaves` | — |
| DA-SRC-14 | `bit` / `varbit` / PostGIS keys | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-11-04 (§53):** `catalog.rs` refuses a `bit`/`varbit`/PostGIS key (`IDENTIFIER_NOT_USABLE`) unless read as text (`column_type_overrides: string`); the Delta column then changes from `binary` to `string` (`runbooks/upgrade.md`) | — |
| DA-SRC-15 | Snapshot collation and chunk edges | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-11-04 (§53):** `KeyCollation::Own` (a domain's collation, schema-qualified), the index-collation warning, `limit 0` refused; the byte budget documented (`chunk_size` per table), `E''` literals unreachable (`standard_conforming_strings` pinned); the `re_query` re-read uses the key's collation (`a_domain_key_declared_with_another_collation_is_re_read`) | — |
| DA-SRC-16 | The replication connection's timeouts | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-10-13** — M6b Task 22: the stream watchdog ends a session whose stream is silent while the slot finds no reader, and the catalog connect is bounded by `timeouts.connect` (§30). | — |
| DA-SRC-17 | Source edges PostgreSQL doesn't produce, and small gaps | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-11-04 (§53):** fixed: the table-set errors all reported, the server's detail and hint, a standby refused on every version (decoding on one is later work, DA-SRC-31), the bounded call's one deadline, a date or time past PostgreSQL's range refused (the `civil_days` bound, BC included); the rest are not reachable from a real server (PostgreSQL doesn't produce them): loose parsing, hstore/PostGIS matched by name, cyclic domains in type planning, `convert` without a scale guard, a `string` override on a `"char"` above 127 on PG 14, `char(n)` padding under `string`, an interval's fields narrowed, the dropped-orphan retry reporting `FirstStart` (D-M12a.13); and by design: a glob table set `UNLOGGED` mid-resolve halts (fails closed: PostgreSQL doesn't replicate an unlogged table, `catalog.rs`), and the catalog driver's errors now reach the error text with the server's detail and hint (`db_message`), though the crate logs nothing of its own | — |
| DA-SRC-18 | M5 test gaps | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-11-04 (§53):** `tls.rs` verifier tests, `require_encrypts_both_connections_without_verifying`, `keys.rs`, `postgres-tls` on `x-pg`, the `replica` profile and `failover.rs` | — |
| DA-SRC-19 | 🆕 Which permanent chunk-read errors are table-scoped | 🟢 | ✅ | §28.2. A permanent chunk-read error ends the whole pipeline (D-M6a.7); a retyped key's 42883/42804 would loop it through restarts though only that table's read is broken. A non-drop 42703 (e.g. a quoting bug) now retries as "changed under the snapshot" until `SNAPSHOT_TIMEOUT`, with a warning per streak. Rule per SQLSTATE which errors halt the table and which end the pipeline, with MySQL's in view (M7/M9). **✅ 2026-11-04 (§53):** `snapshot.rs` (both sources) `table_scoped`: a table's SQLSTATE / MySQL error halts the table (`TABLE_NOT_REPLICABLE`), in the chunk read only; a `re_query` fetch's permanent error ends the session (`a_requery_error_of_the_table_ends_the_session`). Oleksii's trade-off (D-M12a.8): 42501, 1142 and 1143 stay table-scoped, so a role-wide or schema-wide revoke halts every table, each copied again once the grant is back. **Measured 2026-11-04 (§54, M-5):** on MySQL 8.4 and MariaDB 11.8 a lost table `SELECT` with no other privilege on the table halts it `TABLE_DROPPED`, not `TABLE_NOT_REPLICABLE`: `information_schema` hides the table, so the start doesn't list it, the rescan reports it dropped and the chunk's read plan finds no table; with another privilege kept (an `INSERT`) the read fails 1142, `TABLE_NOT_REPLICABLE` (IT `a_lost_select_halts_the_table_as_dropped_unless_another_privilege_shows_it`; `runbooks/source-privileges.mdx`, `runbooks/halted-table.md` say so). A grant probe that tells the two apart is DA-SRC-36 | — |
| DA-SRC-20 | 🆕 A PostgreSQL source failover moves positions backwards | 🟡 | ✅ | §30.1. The position guard assumes an LSN only grows. After a failover to a replica whose WAL ends behind the table's marker, the new primary reuses LSNs the lake has already marked, and the guard skips its changes, so data is lost. `Delivery_Guarantee_and_Position_Guard.md` §6 designs a source failover for MySQL only (a new `server_uuid`). Design the PostgreSQL case: detect the timeline or system identifier change, then halt or re-snapshot. **✅ 2026-11-04 (§53):** `slot.rs` `source_change` (`WAL_SOURCE_CHANGED`, `source_change_action`), `worker.rs` `reposition`; E2E `failover.rs` **2026-11-04 (§54, I-1):** a stream reconnect and a lost position's reset read the server too (`reconnect_check`): another system or timeline halts or reruns Phase 0 before a slot is dropped or made there | — |
| DA-SRC-22 | 🆕 MySQL `ssl_mode: verify-ca` checks the host name too | 🟢 | ✅ | §32.1. `mysql_async` 0.37.1 skips the name check only for an error whose text contains `NotValidForName`; rustls 0.23 prints `certificate not valid for name`. So `verify-ca` acts as `verify-full` and refuses a connection by an address the certificate doesn't name. Fails closed (pinned by a test in M7a); M5 met the same in the PostgreSQL replication connection. **✅ 2026-11-04 (§53):** documented, fails closed: V-124 warns on `verify-ca` for MySQL/MariaDB; `connect.rs` names the re-check at each `mysql_async` upgrade (pinned by `verify_ca_by_an_unnamed_address_is_refused_like_verify_full`) | — |
| DA-SRC-23 | 🆕 `mysql_common` reads MariaDB's compressed events as nothing | 🟡 | ✅ | §32. With `log_bin_compress` on, MariaDB writes `QUERY_COMPRESSED` and `*_ROWS_COMPRESSED_V1` events (165–168); `mysql_common` 0.37.3 returns no data for them, a silent loss (measured on 11.8 and 10.11). Closed by design (Q-M7a-5): the source decompresses them itself (M7a Task 6); the v2 types (169–171), never seen, halt. | — |
| DA-SRC-21 | 🆕 MariaDB's file/position identity is `@@server_id` alone, and ids repeat across servers | 🟢 | ✅ | §31.2. A pipeline in `binlog` mode pointed at another MariaDB with the same `server_id` resumes at a file and offset that mean nothing there, and BINLOG_SOURCE_CHANGED doesn't fire. Candidate check: compare `BINLOG_GTID_POS(file, pos)` with the GTID state recorded at the position. GTID mode (the default) doesn't depend on it. **✅ 2026-11-04 (§53):** `start.rs` `at_boundary` (`BINLOG_GTID_POS`) and `never_held`, one `foreign_position` at the start and at the dump's open (and so at a reconnect, which follows `binlog_purge_action` through `SourceError::SourceChanged` → `Retry`); IT `a_mariadb_position_off_an_event_boundary_is_another_server_s`. Left: a server with the same `server_id` and binlog file names whose file has an event boundary at the offset can't be told apart (`gtid` mode doesn't depend on it) | — |
| DA-SRC-24 | 🆕 A Galera node with `log_slave_updates` off isn't detected as a replica | 🟢 | ✅ | §33.1. The binlog-coverage precondition (config §11.2) reads `SHOW ALL REPLICAS STATUS` when `log_slave_updates` is off; a Galera node applies its peers' writesets without a replica connection, so the status is empty and the check passes, while its binlog lacks the other nodes' changes. Check `wsrep_on` too. **✅ 2026-11-04 (§53):** `server.rs` `check_coverage` refuses a Galera node without `log_slave_updates` | — |
| DA-SRC-25 | 🆕 A case-only column rename reads as a rename | 🟢 | ✅ | §33.1. Column names compare without case (as MySQL's do), so a column whose name changed in case alone diffs as a proven `RenameColumn`; the Delta sink doesn't apply renames, so the table halts or degrades for what is, to MySQL, the same column. Candidate: a case-only rename updates the cached name without an evolution. **Tried in M12a and withdrawn (2026-11-03, §53):** keeping the cached name (`keep_names`) leaked into every case-sensitive column match: the snapshot's read plan retried forever, the worker's drift check warned at every start, and a rename next to a drop took the dropped column's name. It needs a source-aware column compare (a `SourceConnector` method) and a case-insensitive name lookup through source-mysql; until then a case-only rename halts the table (fails closed; drop or move the Delta table, then copy it again). **✅ 2026-11-10 (§60):** the source-aware compare: `SourceConnector::column_names_fold` (MySQL: true) drives the worker's drift compare (`same_columns`); `diff` notes a case-only change as `NoTargetEffect`; `keep_names` keeps the cached spelling through the diff's own matching (`a_rename_onto_a_dropped_name_keeps_its_new_spelling`); the snapshot reads under the cached names and a fold-only baseline caches the sink's spelling (IT `a_change_of_case_alone_keeps_the_cached_name`, a recased key and a resumed cursor; `a_change_of_case_alone_is_no_drift_where_names_fold`, `a_baseline_of_a_case_only_change_caches_the_sink_spelling`); Schema_Change §7.2 (P-4). **Whole-branch review (2026-11-11, §61):** the case-only change reached a truncating job, which keyed the table by the catalog's spelling and put the pipeline in `ERROR` at every start. The truncate now takes the key the sink table records, and a session key that differs from it in any way, case included, halts that table `SINK_REJECTED` without emptying it; the job fails and the start goes on (`a_truncating_job_halts_a_table_its_sink_table_keys_spelled_otherwise`, `a_truncating_job_halts_a_copy_its_sink_table_keys_otherwise`). A sink table dropped while the pipeline was down is made again under the cached spelling (Schema_Change Channel A, §8.2, P-8; E2E `a_deleted_table_of_a_recased_key_is_made_again_under_the_cached_spelling`). Left: DA-SNAP-23; a `partition_by` column absent from the cached schema halts such a remade table `SCHEMA_MISMATCH` (a clearing job recovers it; the plan's known limitations) | — |
| DA-SRC-26 | 🆕 A MariaDB rollback after tracked rows halts the pipeline | 🟢 | ✅ | §33.1. MariaDB logs the rows a rollback undid in a transaction with non-transactional changes, then `ROLLBACK TO` or `ROLLBACK` (measured, 10.11–11.8); M7a halts on one with tracked rows, since the stream already emitted them. Fail-closed, but a false halt for a tracked MyISAM table (whose rows stand) and an outage for an application using savepoints across engines. Candidate: hold a transaction's rows until its end, drop those after the savepoint, keep a non-transactional table's. **✅ 2026-11-04 (§53):** `stream.rs` `Held` (savepoints), `roll_back`: a rollback drops the changes it undid and keeps the halts it covered; a `ROLLBACK TO` a savepoint the stream didn't see halts (when changes are held); the hold counts decoded bytes against `sink.max_transaction_buffer_bytes`; ITs `a_rollback_drops_the_rows_it_undid`, `a_rollback_past_the_hold_limit_halts` | — |
| DA-SRC-27 | A MySQL stream ended once with "Packets out of sync" | 🟢 | ✅ | ✅ 2026-10-20 (§35): the candidate cause was real and is fixed. `Connector::command` now takes the catalog connection out of its slot for a call and puts it back only when the call completes, so a call dropped mid-query drops its connection; a transient error names the call. The likeliest trigger was the session's `select!` dropping `MySqlStream::next()` during a DDL's catalog read. An IT drops a call mid-`SLEEP(3)`: on the old code the next query waited 2.8 s for the abandoned reply, now it answers at once (`connect.rs` `a_call_dropped_mid_query_does_not_poison_the_next`). That this was the observed occurrence is inferred, not reproduced; reopen if CI logs it again. The original record: in M7b Task 7's E2E run on `mysql-84` the worker logged `reconnecting to the source: Packets out of sync` about 3 s after the pipeline started (`mysql_async`'s `PacketOutOfSync`: a connection read a reply it didn't expect). The reconnect recovered and nothing was lost. The log doesn't name the failing call, and the E2E harness prints a test's log only when it fails, so the frequency is unknown. A candidate cause, unproven: `Connector::command` keeps the shared catalog connection in its slot while a query runs, so a caller dropped mid-query (the session's heartbeat read, aborted when a session ends; an aborted chunk read) leaves a connection with an unread reply for the next query. It doesn't obviously fit this case (no earlier session had ended in the process). Next: name the call in the reconnect's log; take the connection out of the slot for a call and put it back only when the call completes | — |
| DA-SRC-28 | A takeover after a pause can't stream on PostgreSQL until the old walsender times out | 🟢 | ✅ | ✅ 2026-10-22 (§37): on the first busy attempt of a spell, a worker that holds its lease ends the slot's walsender of the pipeline's own role (`pg_terminate_backend(pid, 5000)`), then retries from the first backoff step. At most once a lease term, so two holders that each believe they hold the lease (two lease stores on one slot) can't end each other in turn: the later spell gives up with `Contended`. Another role's reader is never ended. The E2E pause scenario's wait went from 150 s to the suite's 60 s (4 scenarios: 189 s → 141 s). M8a Task 13 (D-M8a.22) | — |
| DA-SRC-29 | 🆕 `BINLOG_SOURCE_CHANGED` without GTIDs has no in-place recovery | 🟢 | ✅ | §49.1. A pipeline in `error` keeps its lease and its desired state `running`, so a snapshot job is accepted (`snapshots.rs:150-157`) and its restart clears the error; `same_server` then halts it again with `BINLOG_SOURCE_CHANGED`, and doesn't follow `binlog_purge_action`. The docs: `gtid` with `re_snapshot`, or a new pipeline. **✅ 2026-11-04 (§53):** `start.rs` `MySqlConnector::changed` follows `binlog_purge_action`; a checkpoint's file the server never held (`never_held`: another base name, or after its newest binlog) is another server's, not a purge; found at a reconnect, `SourceError::SourceChanged` is `Retry`, so the pipeline reruns its start; ITs `a_checkpoint_from_another_server`, `a_file_the_server_could_not_have_purged_is_another_server_s` | — |
| DA-SRC-30 | 🆕 Under `slot_management: require_existing` a lost PostgreSQL slot has no in-place recovery | 🟢 | ✅ | §50.1. A slot the DBA creates again is past the pipeline's checkpoint and refused (`slot.rs`); the refusal's text says otherwise. The docs: drop the slot, switch to `managed` in one reload, and set `require_existing` back (`runbooks/position-lost.mdx`). **✅ 2026-11-04 (§53):** `slot.rs` `decide`: adopted under `slot_on_orphan: adopt_resnapshot`, else refused with the key named; IT `require_existing_never_creates` | — |
| DA-SRC-31 | 🆕 Decoding on a PostgreSQL standby (16+) | 🟢 | ⏭ v2 | §53.1. v1 refuses a server in recovery (Configuration §11.2): the source reads `pg_current_wal_lsn()` (refused during recovery) for the snapshot watermark, the lag read and the orphan advance, and may change the publication. Decoding on a standby needs the primary's position for those, and a publication made on the primary. Later work. **⏭ 2026-11-10 (§60, Q-M12d-1):** deferred past v1; the source reads a primary. v2 needs the standby's `hot_standby_feedback`, a slot made on the standby (PostgreSQL 16+), `pg_log_standby_snapshot()` on the primary for the slot's first consistent point, and DA-SRC-33's timeline handling (`Postgres_Slot…` §8.6) | — |
| DA-SRC-32 | 🆕 A source-change reset misses sink tables the pipeline neither resolves nor caches | 🟢 | ✅ | §54.1. The reset covers the resolved and the cached tables (Data-Flow Phase 4, narrowed 2026-11-04): another sink table keeps the old server's marker (PostgreSQL skips below it; MySQL file mode fails the commit). A reposition's permanent sink error (a wiped state, a re-keyed sink table) ends every start, failing closed (M-2). Candidate: list the sink's tables at a `SourceChanged` start, or reposition a table when next tracked; skip or halt a reposition that can't commit. **✅ 2026-11-10 (§60):** `Sink::held_tables` (Delta: `<schema>/<table>/` roots with a `_delta_log` under the prefix, `the_sink_lists_the_tables_it_holds`) and `SinkTableState::identifier`; a source change repositions held ∪ resolved ∪ cached, each under its recorded key (`a_source_change_repositions_a_copy_the_pipeline_doesnt_track`); a `Permanent` refusal halts that table `SINK_REJECTED` and the start goes on (`a_refused_reposition_halts_that_table_and_the_start_goes_on`); a held table with no key is skipped with a warning (`a_held_table_with_no_key_is_warned_about`); Data-Flow Phase 4 (P-5). Left: DA-SRC-37 | — |
| DA-SRC-33 | 🆕 A PostgreSQL 17+ failover slot is treated as a source change | 🟢 | ✅ | §54.1. A synced failover slot survives a promotion intact, yet the new timeline halts `WAL_SOURCE_CHANGED` (or `re_snapshot` copies every table). Candidate: under `require_existing`, accept a new timeline for a `failover`/`synced` slot whose `confirmed_flush` is at or below the switch point. **✅ 2026-11-10 (§60):** Q-M12d-2, Q-M12d-5: a managed slot is a failover slot on 17+ (`a_managed_slot_is_a_failover_slot_from_postgresql_17`, on 14, 17, 18); `Preflight::server`, the worker's `highest_marker` and `prepare_marked`; `slot::continues` (same system, later timeline, `failover` and `synced` and not invalidated, no lake marker past the checkpoint, confirmed exactly at it: `a_promotion_the_slot_followed_continues_the_stream`); a reconnect to a promoted server of the same system reruns the start under either action (P-6, `a_reconnect_to_the_promoted_server_reruns_the_start_which_continues`); a continued resume records the new server (`a_promotion_the_slot_followed_resumes_on_the_new_server`); E2E `a_promotion_the_failover_slot_followed_continues_the_stream` on the `postgres-sync-*` pair, the pipeline running through the promotion; config §9.2.1 (P-1, P-2), Data-Flow Phase 2a and Source Disconnect (P-5, P-6). Measured on PostgreSQL 18; 17 by the same mechanism, per the PostgreSQL docs (a promoted standby's slot keeps `synced`; if it didn't, the start halts or recovers as configured, never loses data) | — |
| DA-SRC-34 | 🆕 File mode under `re_snapshot` loops on a host alternating between servers | 🟢 | ✅ | §54.1. Each Phase 0 recovers on one server, the dump's open finds the other, and every marker is reset each time while nothing streams. Candidate: halt `BINLOG_SOURCE_CHANGED` after N `SourceChanged` reruns without a durable checkpoint; the docs: one server per file-mode pipeline. **✅ 2026-11-10 (§60):** `WorkerStatus::started_from`; the controller counts runs ending `SourceChanged` with no durable checkpoint past their start, and at `SOURCE_CHANGES_IN_A_ROW` (3) puts the pipeline in `ERROR` with the change's code, naming the alternation (`runs_that_keep_finding_another_server_halt`, `a_durable_checkpoint_starts_the_count_of_server_changes_again`); PostgreSQL too; Data-Flow Phase 2b (P-5); `runbooks/position-lost.mdx`. Left: DA-SRC-38 | — |
| DA-SRC-35 | 🆕 MySQL's per-column config keys match column names with case | 🟢 | ✅ | §54.1. A `column_type_overrides` or `primary_key` entry in another case than the catalog's doesn't apply. Candidate: match through `Fold`; warn on a key that matches no column. **✅ 2026-11-10 (§60):** `config::column_override` matches a MySQL key's column without case (catalog plan, snapshot, stream); `primary_key` entries map to the catalog's spelling; V-127 warns at schema resolution on both sources (`check_column_keys`); `column_keys_match_as_the_source_compares_names_v127`, IT `column_keys_name_columns_without_case` (MySQL 8.4, MariaDB 11.8); config §10, §13 (P-3). Left: MySQL's accent-insensitive comparison, and two keys of one column that differ by case alone (the first in map order wins) | — |
| DA-SRC-36 | 🆕 A lost MySQL `SELECT` reads as a dropped table | 🟢 | ✅ | §54.1. Measured on MySQL 8.4 and MariaDB 11.8 (M-5): with no other privilege on the table, `information_schema` hides it and it halts `TABLE_DROPPED`. Candidate: a grant probe in the no-table path, measured first (the server may answer 1142 for a dropped table too); the docs say to check the grants. **✅ 2026-11-10 (§60):** Q-M12d-4: `TABLE_DROPPED` kept (a unit variant); on the rescan paths a table gone from the catalog is probed (`SELECT 1 … LIMIT 0`); a `1142` adds a status warning that the user may have lost `SELECT` (`RescanReport::warnings`), any other answer reads as not hidden and is logged; ITs `a_table_the_user_can_no_longer_see_reads_as_dropped_with_a_warning`, `a_table_whose_schema_is_gone_is_still_reported_dropped`; `a_gone_tables_rescan_warnings_reach_the_status`, `a_periodic_rescans_warnings_reach_the_status`; Schema_Change §7.2 (P-4). Its limit, measured: without a database-level privilege a dropped table answers 1142 too, so the warning says "one or the other" | — |
| DA-SRC-37 | 🆕 A refused reposition of a table the pipeline doesn't track is never retried | 🟢 | ✅ | §60.1. Task 6's halt (`SINK_REJECTED`, "its reposition fails") of a held table neither configured nor cached is cleared at the next start, and the checkpoint already names the new server, so no start repositions it again. Tracked later (under `snapshots: never`), it keeps the old server's marker and skips the new one's changes up to it. Holding the checkpoint back would fail every start on an always-refusing table. The docs: drop or move such a Delta table before tracking it (`runbooks/halted-table.md`). Candidate: keep the halt of a refused reposition until a job or the operator clears it. **✅ 2026-11-11 (§63):** a refused reposition's halt (its detail begins `the source changed, and its reposition fails`, the code's `REPOSITION_REFUSED`) of an untracked table stays at later starts while the sink still holds the table; a snapshot job's copy, or the table leaving the lake, ends it (Task 3, `48c8897`, D-M12c.4; `a_refused_repositions_halt_stays_while_the_lake_holds_the_table`). Spec: Data-Flow Phase 4 (P-2). | — |
| DA-SRC-38 | 🆕 The alternation count never decays, and a promoted-reconnect alternation halts without a code | 🟢 | ✅ | §60.1. DA-SRC-34's count starts again only at a durable checkpoint past a run's start, another kind of end, or a stop: three genuine failovers days apart on a source that writes nothing in between halt the pipeline. And a PostgreSQL reconnect to a promoted server of the same system asks the rerun with an un-prefixed message (P-6), so three such reconnects in a row halt with no `WAL_SOURCE_CHANGED` code in the status. The docs: stop and resume. Candidate: decay the count with time; fall back to the source's change code. **✅ 2026-11-11 (§63):** the count starts at 1 again when a server change comes more than `SOURCE_CHANGE_WINDOW` (10 minutes) after the one before, and a PostgreSQL promoted reconnect's message carries `WAL_SOURCE_CHANGED` (Task 2, `f700a10`, D-M12c.2/.3; `server_changes_far_apart_dont_halt`, `an_alternation_halts_with_the_code_its_change_names`, the failover E2E). Spec: Data-Flow Phase 2b and Source Disconnect (P-1). | — |
| DA-SRC-39 | 🆕 A session that ends on an error drops the PostgreSQL stream without closing it | 🟢 | ✅ | §61.1. `Session::run` (`data-avalanche-pipeline/src/session.rs`) closes the stream only on a clean finish: when the event loop fails, or the writer reports `Failed` while the session drains, the stream is dropped. A dropped `pgwire-replication` client detaches its worker, which takes the slot again about 80 ms later, so a reconnect within that window can find the slot still active (SQLSTATE `55006`) and fail. Seen in the PostgreSQL 14 reconnect test's cleanup (`093168b` closes each stream there); how the controller's retry absorbs it after a failed session isn't measured. Candidate: close the stream on every exit of `run`. **✅ 2026-11-11 (§63):** a session closes its stream on every exit, an error's too (Task 1, `caae852`; `a_session_that_fails_closes_its_stream`). | — |

---

## 15. Observability (OBS)

**State.** `Observability_Metrics_and_Tracing.md` (G-33/G-32) defines the mandatory metric catalogue,
the pipeline-level-default / per-table-opt-in cardinality policy, OTLP tracing (disabled when no
endpoint), and trace-id/pipeline injection into JSON logs.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-OBS-1 | Mandatory Prometheus metric catalogue | 🟡 | ✅ | §2 — replication/source/snapshot/Iceberg+maintenance/clustering/status families. | G-33, OQ-OB3 |
| DA-OBS-2 | Metric cardinality policy | 🟢 | ✅ | §1 — `{pipeline,source_host,source_db}` default; `table` label only when `sink.metrics.per_table=true`. | G-33 |
| DA-OBS-3 | Distributed tracing | 🟢 | ✅ | §3 (G-32) — `globals.tracing.otlp_endpoint` (absent⇒off) + `sample_ratio`; batch/commit-granularity spans, no per-event spans. | G-32, OQ-OB1 |
| DA-OBS-4 | Log/trace correlation | 🟢 | ✅ | §4 — inject `trace_id`/`span_id`/`pipeline` into JSON logs. | §10.3 |
| DA-OBS-5 | **Numeric end-to-end replication-lag SLA** | 🟡 | ✅ | `Clustering…` §18.6 explicitly leaves OQ-OB2 open as a *tuning input*: a concrete max-lag number is not fixed, which is what grounds the lease-timing profile and flush/checkpoint cadence. Decide a number (and whether failover-time carries its own SLA distinct from steady-state). **✅ 2026-09-26 — D-V1.3 (`V1_Scope_Decisions.md`):** steady-state p99 ≤ 60s, failover ≤ 2 min, at default config; confirm by v1 load test. | OQ-OB2 |
| DA-OBS-6 | 🆕 The text log writes ANSI colour sequences to a file | 🟢 | ✅ | §34.1. The binary's `compact` and `pretty` logs colour their output whether or not stderr is a terminal (`tracing-subscriber` 0.3.23's default; `crates/data-avalanche/src/logging.rs`). A log collector or a file gets escape sequences around the level and the fields: a search for `code="BINLOG_…"` misses them. The E2E suite strips them (M7b Task 7). Fix: colour only when stderr is a terminal (and keep `NO_COLOR`); M10 (`globals.log`) or earlier. M10 Task 7 fixes it (§44). **✅ 2026-10-29** — M10 merged (PR #15, `550474c`), its CI green (§45). | — |
| DA-OBS-7 | 🆕 The OTLP export sends no headers | 🟢 | ✅ | §44.1. `globals.tracing` has no headers, so a collector that wants a token can't take the traces; an `https` endpoint goes through `reqwest` 0.13 with rustls and is untested. Next: `globals.tracing.headers` (secret-resolved) and an `https` IT, after v1 unless needed **✅ 2026-11-06 (§57):** `globals.tracing.headers` (secrets, R, V-126); the exporter rebuilt when they change; `an_https_collector_takes_the_export_with_its_headers`; a value reaches the collector as written (`%` passed as `%25` through the exporter's URL-decoding: `a_header_value_with_a_percent_reaches_the_collector_as_written`); `deployment/observability.md` | — |
| DA-OBS-8 | 🆕 The lag reads `0` early in two short windows | 🟢 | ✅ | §45.1. A restarted worker that makes its first re-read change durable before re-reading the next, and a source reconnect inside one worker, show `0` until the change is read again (about a heartbeat on MySQL; on PostgreSQL the re-decode of a large transaction). Next: keep the kept start until the frontier covers it; carry the pending start across a reconnect **✅ 2026-11-06 (§57):** the controller's `Lag { kept, own }`: an earlier session's start kept until the new stream reads as far; `the_kept_lag_start_waits_for_the_next_session_to_read_as_far`; a reconnect clears the worker's frontier and pending start with its count (`WorkerStatus::reconnect`, `a_reconnect_clears_the_frontier_and_the_pending_start`) | — |
| DA-OBS-9 | 🆕 The JSON correlation's global-dispatch path is untested end to end | 🟢 | ✅ | §45.1. The logger's tests run under a scoped dispatch; the binary's `set_global_default` path holds by reading `tracing-core` 0.1.36. Next: a JSON-logged line matched to a Jaeger trace in the E2E **✅ 2026-11-06 (§57):** the Jaeger E2E runs the binary with JSON logs and matches a logged flush's `trace_id`/`span_id` to a trace Jaeger holds (`observability.rs`) | — |

---

## 16. Management API & security (API)

**State.** `API_RBAC_and_Versioning.md` (G-16/G-9.1) closes the RBAC matrix: three nested roles
(`admin ⊇ operator ⊇ viewer`), an endpoint→min-role table under `/v1`, per-auth-mode role binding,
configurable IdP role mapping, and the versioning scheme. Health/metrics are unversioned and
unauthenticated. The auth *mechanism* (jwt/oauth2 verification params) was already in config §6.3.1.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-API-1 | RBAC role→permission matrix | 🟡 | ✅ | `API_RBAC…` §2 — endpoint table; reload/validate = operator; leave/shutdown = admin. | G-16, OQ-API2 |
| DA-API-2 | API versioning | 🟢 | ✅ | `/v1` for functional endpoints; health/metrics unversioned. | G-9.1 |
| DA-API-3 | Role binding per auth mode + IdP role mapping | 🟡 | ✅ | §3/§4 — none=open, basic=admin, jwt/oauth2=mapped claim (max wins); 401 vs 403 distinguished; configurable `role_mapping`. | D-16.3/4 |
| DA-API-4 | `config/validate` may echo resolved secrets → operator-only | 🟡 | ✅ | §2 note — validate is operator, not viewer, because the resolved config can contain secret material. | — |
| DA-API-5 | TLS / mTLS for API and cluster-internal transport | 🟡 | ✅ | API TLS config §6.3.2; memberlist TLS §6.5.2 (CA presence ⇒ mTLS). V-052 "auth without TLS" stays a **warning** (upstream termination in k8s is common). | OQ-API1, D-16.5 |
| DA-API-6 | Lifecycle endpoints inherit operator role | 🟢 | ✅ | `Pipeline_Lifecycle…` §6 — pause/resume/restart/stop/flush all operator. | G-NEW-15 |

### Pipeline lifecycle state machine (sub-domain of API + FLOW)

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-API-7 | Pause/stop/resume/restart/flush state machine | 🟡 | ✅ | `Pipeline_Lifecycle_State_Machine.md` — persisted desired-state (`RUNNING/PAUSED/STOPPED`), owner reconciles; survives failover & reload. | G-NEW-15 |
| DA-API-8 | STOPPED keeps its lease (owner idle) | 🟢 | ✅ | §2 — avoids ring churn, immediate resume; cost = one idle renewal. | D-NEW15.1 |
| DA-API-9 | PAUSE flushes to a clean boundary (no at-risk buffer at failover) | 🟡 | ✅ | §2/§4 — resume is fast (warm, no buffer reconcile). | D-NEW15.3 |
| DA-API-10 | Reload must not resurrect a paused/stopped pipeline | 🟡 | ✅ | §4 — runtime `desired_state` wins over config's implicit "should run." | §9.6 |
| DA-API-12 | 🆕 The reload and status endpoints answer without authentication until M9 | 🟢 | ✅ | §40 (M9a's plan): every `/v1` route takes its least role (API_RBAC §2), reload operator and status viewer; M9a Task 4's matrix test turns away every caller below it. §38.1: Q-M8b-1: `POST /v1/config/reload` and `GET /v1/config/status` serve on `globals.api.address` like `/metrics`, before M9's auth and RBAC. A caller can publish the file that is on the node, not a text of its own. M9 puts them behind the operator and viewer roles (API_RBAC §2). | — |
| DA-API-11 | 🆕 A desired-state change missed between two polls | 🟢 | ✅ | §40 (M9a's plan): a restart request, recorded by id in `requests.json`, takes a pipeline out of `ERROR` without depending on the controller's sampling (D-M9a.4, D-M9a.5); a stop and a resume inside one poll still look like no change, and needn't be seen apart. §30.1: The controller samples `pipeline-state.json` once per renewal interval, so a stop and a resume between two polls don't clear `ERROR` (Lifecycle §3). The M9 API wakes the controller, or the document carries a generation the controller compares. | — |
| DA-API-13 | 🆕 A crashed holder's unreleased lease reads as live until takeover | 🟢 | ✅ | §41.1 (M9a's exit): GET shows its last status, a restart or flush answers `202` (absorbed by the next term); a `stale` hint needs a skew-tolerant liveness notion. **✅ 2026-11-06 (§57):** `pipelines.rs` `liveness`: `holder.stale` (its node and incarnation not a member of the answering node's gossip) and `holder.suspect`, `holder_stale` in the lists; `Member.incarnation`; `a_holder_that_isnt_a_member_reads_as_stale`, `a_suspected_member_holds_its_lease_as_suspect` | — |
| DA-API-14 | 🆕 A node that left the cluster keeps its last generation's auth | 🟢 | ✅ | §41.1: its coordinator has stopped, so a later reload's roles or credentials don't reach it; refuse its mutating routes, or document it (M11). **✅ 2026-10-30 (§50, documented):** `runbooks/failover.md`. | — |
| DA-API-15 | 🆕 A retried restart or flush call asks twice | 🟢 | ✅ | §41.1: the request id is per HTTP call; an optional client-supplied id, or M11's docs. **✅ 2026-10-30 (§50, documented):** `runbooks/pipeline-commands.md`. | — |
| DA-API-16 | 🆕 The JWKS fetch accepts http, follows redirects, reads an unbounded body | 🟢 | ✅ | §41.1: a V-rule warning for a non-`https` key URL, no redirects, a body cap. **✅ 2026-11-06 (§57):** `auth.rs`: no redirect, `MAX_KEY_DOCUMENT` (1 MiB); V-125 warns on a non-`https` key URL; `a_key_fetch_follows_no_redirect_and_reads_a_bounded_body` | — |
| DA-API-17 | 🆕 No connection limit on the TLS listener | 🟢 | ✅ | §41.1: a handshake task per connection, each bounded at 10 s. **✅ 2026-11-06 (§57):** `server.rs` `MAX_CONNECTIONS` (1024), a permit before each accept, and `IDLE_TIMEOUT` (60 s) closing a connection that moves nothing; `past_the_cap_a_connection_waits_for_one_to_close`, `an_idle_connection_is_closed_and_gives_its_place_back`. The trade-offs: a handler silent for 60 s has its connection cut (the client sees a reset), and a client that sends a byte within every 60 s holds its place | — |
| DA-API-18 | 🆕 The OpenAPI document isn't validated against the OAS 3.1 schema | 🟢 | ✅ | §41.1: snapshot-tested and read back by utoipa only. **✅ 2026-11-06 (§57):** `the_document_is_valid_oas_3_1`: the document checked by `boon` against the vendored OAS 3.1 schema | — |
| DA-API-19 | 🆕 Validate of a posted body echoes a `${VAR}`-resolved pipeline `id` | 🟡 | ✅ | §41 (2026-10-24, on Oleksii's request, PR #13): a posted body's templated `id` or `defaults.id` is refused with V-055 before any rule runs, so no expanded id is echoed or used (`load_posted_str`, `ConfigCoordinator::check_posted`); the node-file validate and the reload are unchanged. §41.1: an operator could read a node env var fitting V-055's pattern. | — |
| DA-API-20 | 🆕 Validate's verdicts on a posted body's other `${VAR}` leaves answer yes or no about a variable | 🟢 | ✅ | §41.1: V-063 (set or not), the uniqueness rules V-058/V-059/V-120, pattern and range checks, and the diff's field paths tell, one request at a time, whether a variable is set or equals a value; nothing is echoed. Every validate is logged with its caller (2144392); M12 rate-limits validate. **✅ 2026-11-06 (§57):** `limit.rs`: posted validates at 1 a second after a burst of 5, `429` with `Retry-After` (Q-M12b-4); `posted_validates_past_the_burst_wait`. The node's own file stays unlimited | — |

---

## 17. Operational, deployment & dependencies (OPS / DEP)

**State.** `Operational_and_Scope_Gaps_Batch.md` resolves a batch of smaller gaps (Azure creds,
catalog retry, IAM matrix, MariaDB scope, strip_schema collisions, backpressure-on-sink-failure).
Deployment topology (StatefulSet, headless Service) is fixed by the clustering doc. Dependency
pinning is the remaining cross-cutting concern.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-OPS-1 | Azure credentials & container model | 🟢 | ✅ | `Operational…` G-34 — SDK default chain; optional `azure.account_name`; bucket=container. | G-34 |
| DA-OPS-2 | Minimum IAM permissions per provider | 🟢 | ✅ | G-37 — documented S3/GCS/Azure permission sets (runbook prerequisite, no config). | G-37 |
| DA-OPS-3 | `glue.strip_schema` collision handling | 🟢 | ✅ | G-11.3 — collision = config error; non-stripped = `schema__table`. | G-11.3 |
| DA-OPS-4 | Sink/catalog unavailability → DEGRADED back-pressure | 🟡 | ✅ | G-27/G-35 — stop consuming once buffers fill, protect slot/WAL via G-22; recover on next commit; catalog circuit breaker. | G-27, G-35 |
| DA-OPS-5 | k8s deployment as StatefulSet (not Deployment) | 🟡 | ✅ | Clustering §3.4/§10.2 — stable `node_name` via Downward API; headless Service discovery. | D-1, D-4 |
| DA-OPS-10 | 🆕 An `id` change that keeps its resources is sequenced only within one reload, on the nodes that applied it | 🟢 | ✅ | §39.1. M8b's whole-branch review: an added pipeline sharing a removed one's slot, publication, sink or state waits for the removed one's lease. A node that starts during the wait, a removal and an add in two reloads, a cut-off drain, or the old id reused while the removal drains let it start early. **🔄 2026-10-30 (§50):** `configuration/overview.md` says to wait for the old lease's release, or to do both in one reload; the tombstone stays. **✅ 2026-11-06 (§57):** the controller's `follow()` waits, before its first claim, for every pending tombstone of its id or of a pipeline it shares resources with, on every node; `a_pipeline_added_after_the_removal_it_follows_waits_for_it`, `a_node_started_during_a_removal_waits_for_it` | — |
| DA-OPS-11 | 🆕 The release workflow's first full run is the first tag's | 🟢 | ✅ | §46.1. `release.yml` (M11a) runs on a `v*` tag or by hand, and only from `main`: before M12's `v1.0.0` only its builds are proven. The first push creates the GHCR packages private. Next: a manual run on `main` after M11a merges; the packages made public after the first tag §47: the repository is private, so the attestation is skipped (BuildKit's in-registry provenance and the SBOM stay) and the `ubuntu-24.04-arm` runner is unproven until `weekly.yml` is dispatched. Oleksii's ruling 2026-10-29: the repository goes public (done, after PR #17's README). §48: the manual run on `main` green (run 40918381277). Next: after the first tag, make the image and chart packages public. **✅ 2026-10-30 (§51):** `v0.1.0` released by `release.yml` (run 41034298202), its image attested, the image and chart packages public | — |
| DA-OPS-12 | 🆕 A process-restart key written as a literal can't change once a generation exists | 🟢 | ✅ | §49.1. A reload refuses it (V-061) and a restarted node adopts the stored generation; only `${VAR}` values and file contents change, by a rolling restart. The docs give the workaround. **✅ 2026-11-06 (§57):** `POST /v1/config/reload?restart=accept` (Q-M12b-2): published, each node's `restart_pending` until its next start; `FIXED_FIELDS` refused even then (config §1.2, V-061); `a_restart_field_is_published_when_accepted_and_pending_until_the_next_start`; `runbooks/config-changes.md` | — |
| DA-OPS-13 | 🆕 Two operator-facing texts are wrong (the orphan-slot refusal's key, the CLI's `about`) | 🟢 | ✅ | §49.1. `slot.on_orphan` for `slot_on_orphan`; MySQL and MariaDB missing from the description. **✅ 2026-11-04 (§53):** `slot.rs` names `slot_on_orphan`; `cli.rs` `about` names the three sources (`the_help_names_every_source`) | — |
| DA-OPS-14 | 🆕 MariaDB file mode's boundary check costs a binlog read at every stream open | 🟢 | ✅ | §54.1. `BINLOG_GTID_POS(file, pos)` scans the file up to `pos` at every start and reconnect. Measure in M12c's load runs; remember the answer for `(file, pos)` within a run if it shows. **✅ 2026-11-11 (§63), by measurement (D-M12c.1):** `BINLOG_GTID_POS` costs 0 to 1 ms on MariaDB 11.8.9 (`binlog_gtid_index`, files to 272 MB) and about 1.4 ms per MB on 10.11.19 without the index (98 to 102 ms at 73 MB; a full 1 GiB file about 1.4 s), once per stream open; no cache. | — |
| DA-OPS-9 | 🆕 A pipeline removed while no node holds its lease isn't torn down | 🟢 | ✅ | §38.1. Q-M8b-3: the owner drains, tears down (`auto_cleanup`) and releases. With no owner at the removal (it is down, or a hand-off is between release and claim), every node's controller is on standby and just ends, so the PostgreSQL slot and publication stay and the slot keeps WAL (bounded by `max_slot_wal_keep_size`). Config §12.3's "orphan-cleanup … whose owner was offline" design item. **✅ 2026-11-06 (§57):** `state` `removal.rs` (`removals/<id>.json`), recorded by the coordinator before it adopts; the reconciler's sweep (`SWEEP_INTERVAL`) and the controller's `remove_orphan`; config §12.3; `a_removal_no_node_removed_is_torn_down_where_the_ring_places_it`, `a_removal_held_by_a_gone_node_is_torn_down_once_its_lease_expires`; an orphan, holding the lease, marks a removal the current generation undoes done without a teardown, and tears nothing down while that generation can't be read or load (`an_undone_removal_tears_nothing_down_at_a_hand_off`, `an_undone_removal_tears_nothing_down_at_a_failover`, `an_orphan_marks_a_removal_the_current_generation_undoes_done_without_a_teardown`, `an_orphan_tears_down_a_removal_the_current_generation_doesnt_undo`, `an_orphan_tears_nothing_down_when_the_current_generation_is_unread_or_invalid`); the owner marks it done before it releases the lease (`an_owners_removal_and_an_orphan_of_it_on_one_node_tear_down_once`); that done is conditional on the tombstone's version, which every record moves (its `records` counter): `an_undone_removal_recorded_again_meanwhile_stays_pending`, `state` `removal.rs` `a_record_that_keeps_a_removal_still_changes_its_bytes`, `a_conditional_done_loses_to_a_record_since_its_read`, `a_record_without_its_counter_reads_as_zero`, and `state` `tests/removals.rs` (RustFS, Azurite) | — |
| DA-OPS-8 | Per-node literals in a node's own file are ignored once a generation exists | 🟢 | ✅ | §36.1, §38. A node runs the cluster's generation resolved with its own environment (D-3); per-node values go through `${VAR}` (config §6.5.1). M8b warns at startup naming each per-node key that differs, and which value runs; the operator docs stay with M11. **✅ 2026-10-30 (§50):** the operator docs say it, in `configuration/cluster.md` and `runbooks/config-changes.md`: the gossip addresses (`bind_addr`, `bind_port`, `advertise_addr`, `advertise_port`) are the generation's, so a literal there is ignored with a warning; `node_name` and `api.address` are the node's file's. | — |
| DA-OPS-7 | A controller task that panics goes unnoticed | 🟢 | ✅ | ✅ 2026-10-22 (§37): the reconciler supervises its controllers. One that ended unasked (a panic, a return) is logged and started again on its status channel, and a dropped reconciler counts as a crash; the owned-pipelines gauge is held by a guard, so a panic doesn't over-count it. Readiness after a controller panic is not in M8a (DA-OPS-7's other half: M10). M8a Task 6 (D-M8a.11, `a_controller_that_panics_is_started_again`) | — |
| DA-OPS-6 | "Restart ≠ apply config" operator footgun | 🟢 | ✅(documented) | Clustering §9.3 D-3 — cloud-generation-wins; a pod restart does **not** apply ConfigMap edits, only `POST /reload` does. Document loudly. | D-3 |
| DA-DEP-1 | iceberg-rust pinned version & capabilities | 🟡 | ⏭ v2 | `iceberg` 0.10.1, MSRV 1.94 (main has moved to 1.95, #3083; Sources_Index needs refreshing from 1.92). Multiple capability gates ride on the exact version: DA-SINK-1, DA-SINK-2, DA-SCHEMA-8, DA-TYPE-5, DA-SINK-10. Pin and assert at startup. **2026-09-26 tidy:** Iceberg-sink only — deferred with the Iceberg sink to v2. **2026-11-13 (§66):** iceberg 0.11.0 is MSRV 1.95 on arrow 58, `main` on arrow 60; the workspace is on arrow 59 — no release shares it. | — |
| DA-DEP-2 | OpenDAL version exposing conditional writes | 🔴 | ✅ | Clustering §17.2 — `if_match` on writes landed late-2024/early-2025; the `iceberg-storage-opendal` transitive pin must be ≥ that, and capabilities asserted at startup (refuse to start if absent). Part of DA-CLUS-1. **2026-09-26 tidy:** superseded — DA-CLUS-1 closed with a per-backend `LeaseStore` (OpenDAL only on S3, where `if_match` exists; native SDKs for GCS/Azure); the Delta sink uses object_store conditional puts (P8), not OpenDAL. | D-5 |
| DA-DEP-3 | `sqlparser-rs` (MySqlDialect) pin | 🟢 | ✅ | `MySQL_DDL_Classifier.md` §5 — now on a fail-soft path, so a coverage gap degrades rather than halts; de-risked. | G-19a |
| DA-DEP-4 | `memberlist` crate (Lifeguard, TLS/QUIC transport) | 🟢 | ✅ | Sources_Index; Clustering §2 — features confirmed available. | G-23 |
| DA-DEP-5 | Two rustls crypto providers in one process (`ring` via pgwire-replication, `aws-lc-rs` via mysql_async) | 🟢 | 🆕 ✅(rule) | Found 2026-09-26 (S-MY8 with both features built): rustls cannot pick a provider and **panics at the first TLS handshake**. Rule: install one process-wide `CryptoProvider` at startup, before any TLS client (sources, deltalake/object_store). | — |

---

## 18. Cross-cutting & newly-surfaced gaps

These are not contained in any single domain — they emerge from reading two or more documents
together and finding a seam. They are the items most worth attention in spec elaboration because no
single research doc "owns" them.

### DA-X-1 — `CdcEvent` is missing the per-change sequence the dedup design requires 🟡 ✅

> **2026-09-26 tidy:** Closed: `CdcEvent.change_seq` + persisted `_seq` lineage column are in `Data-Flow.md` (DF-5).
`Ghost_Row_Dedup_and_Primary_Keys.md` §4 keeps-last by a **per-change** `_seq` (PG: the change's own
WAL LSN; MySQL: `(binlog_file, end_log_pos)` or GTID + intra-txn event index), explicitly *finer* than
the commit position. But `Data-Flow.md`'s `CdcEvent` carries only `source_pos` (commit LSN/GTID) and
`xid`. **The boundary type and the accumulator contract must add a per-change position**, and the
lineage columns (`_op/_ts/_lsn/_source_db/_pipeline`) must decide whether `_seq` is also persisted as a
column (it is referenced as one in the dedup doc but absent from the lineage table). Resolving this is a
prerequisite to specifying both the decoder output and the sink collapse step. *(Links DA-FLOW-2,
DA-SYS-5.)*

### DA-X-2 — The Iceberg snapshot-summary carries three independent CDC properties; their coexistence is assumed, not specified 🟡 ⏭ v2

> **2026-09-26 tidy:** Iceberg-sink only — v2.
Three separate designs write into the Iceberg snapshot summary: `cdc.owner_epoch` (clustering fence,
D-7), `cdc.committed_position` (delivery guard, D-14.4), and the implicit per-table HWM. Each doc
individually says "verify iceberg-rust preserves custom snapshot-summary properties across commits."
**No doc owns the combined property set or its schema.** Spec elaboration should define one
`cdc.*` snapshot-summary schema and a single verification that all of it survives commits. *(Links
DA-SINK-2, DA-CLUS-3, DA-FLOW-7.)*

### DA-X-3 — Equality-delete partition routing by before-image is a writer-contract obligation referenced from two docs but defined in neither's spec 🟡 ⏭ v2

> **2026-09-26 tidy:** Iceberg-sink only (equality deletes) — v2. The Delta MERGE matches on the key across partitions, so it has no equivalent obligation.
`Iceberg_Partitioning…` §2 (D-24) requires the writer to route an equality-delete to the **old**
partition (from the before-image) when an UPDATE changes a partition value; `Ghost_Row…` §4 requires
the analogous old-key delete for PK mutation. Both are "sink-writer contract additions" with no single
authoritative sink-writer contract document yet. Spec elaboration needs a **consolidated sink-writer
contract** (keying, collapse, partition routing, delete-file sequencing, snapshot-summary properties,
truncate handling) — today it is scattered across five research docs.

### DA-X-4 — Reserved metadata-column collision has no runtime rule 🟡 ✅

> **✅ 2026-09-26 — D-V1.1 (`V1_Scope_Decisions.md`):** the table halts with `RESERVED_COLUMN_COLLISION`; no escaping.
*(= DA-SYS-4, restated as cross-cutting.)* If a source table legitimately has a column named `_op`,
`_ts`, `_lsn`, `_source_db`, `_pipeline`, or a future `_seq`, the accumulator injects a colliding
column. Config §5 only says operators "should be aware." Decide the rule: reject/halt the table, or
escape source columns (e.g. double-underscore prefix as in `glue.strip_schema` collisions). Until
decided, the writer behavior is undefined.

### DA-X-5 — `TRUNCATE` sink semantics undefined and possibly engine-asymmetric 🟡 ✅ (Delta) · ⏭ v2 (Iceberg)

> **2026-09-26 tidy:** Closed for v1: `Data-Flow.md` TRUNCATE barrier, one semantics for PG + MySQL; Delta = metadata-only delete-all (P5). Iceberg realization → v2.
*(= DA-FLOW-5.)* MySQL's classifier maps TRUNCATE to a "table-clear" but defers the actual Iceberg
operation to "the sink-writer contract"; PG's `T` truncate message is decoded but no sink action is
written down. Define one cross-engine truncate→Iceberg mapping (overwrite-to-empty vs delete-all vs a
snapshot boundary marker) and confirm it is idempotent under replay like everything else.

### DA-X-6 — PK-less semantics interact with dump-only modes 🟡 ✅

> **2026-09-26 tidy:** Closed by spec (uniform rule): the identifier precondition applies in every mode (config §11.2). See DA-SNAP-8.
*(= DA-SNAP-8.)* "PK-less halts" is justified by equality-delete keying, but `dump_always` /
`dump_initial` tables are insert-only and never delete — so the keying argument does not bind. The
spec should state explicitly whether a dump-only table may be PK-less (snapshot as plain inserts,
`identifier-field-ids` unset) or still halts for uniformity.

### DA-X-7 — MariaDB GTID dialect is the one open correctness dependency in an otherwise "in-scope" claim 🟡 ✅

> **2026-09-26:** proven feasible with `mysql_async` (S-MY9) — see DA-SRC-8. **✅ MariaDB in v1 — D-V1.5 (`V1_Scope_Decisions.md`).**
*(= DA-SRC-8.)* G-38 accepts MariaDB "best-effort via the MySQL path," but the `SourcePosition` MySQL
implementation must handle MariaDB's `domain-server-sequence` GTID, which is a *different* set algebra
from MySQL's `server-uuid:tx`. This is flagged as a new sub-research item and is the single place where
"in scope" hides real work. Either schedule the sub-research or narrow the v1 scope statement.

### DA-X-8 — Three specs disagree on the per-pipeline state path; one canonical layout is needed 🟢 ✅

> **✅ 2026-09-26:** the three stale config references are removed (config §10 note, `id` field text, §17) — §26.6.

> **2026-09-26 tidy:** Layout decided and in `Data-Flow.md` §State Layout (no `pipelines/<id>/` segment). **Residual:** three stale references in `Configuration_Specification.md` — line ~606 (`pipelines/<id>/`), the obsolete "Reconcile with Data-Flow" note (~960–965), and the §17 cross-reference (~1553). Spec edit owed (§26.4, needs approval).
`Data-Flow.md` roots per-pipeline state at `…/pipelines/<id>/`; `Configuration_Specification.md` §10
and `Operational…` G-NEW-20 remove that segment (state already roots at `sink.prefix`/`state.prefix` =
`id`). Until `Data-Flow.md` is re-rooted, implementers reading the two specs will build different key
layouts. Low severity, high confusion potential. *(Spec-merge debt, §19.)*

### DA-X-9 — "Idempotency is correctness; the position guard is an optimization" is a load-bearing systemic claim worth a single statement 🟢 ✅(but distributed)
The whole exactly-once story (Delivery §1, Large-Txn §1, Snapshot §1, Lifecycle replay) rests on
row-level PK-upsert idempotency being the correctness mechanism, with positions/guards/leases as
optimizations and fences layered on top. This is stated correctly in each doc but never in one place as
*the* system invariant. A one-paragraph "correctness model" at the top of the application spec would
prevent a future contributor from re-introducing a correctness dependency on batch alignment or
position arithmetic.

---

## 19. Spec-merge debt (decided in research, not yet in the authoritative specs)

These are **not open design questions** — every item is decided. They are the gap between the research
corpus and the Oleksii-owned spec docs. Each is a "propose-only" edit the research docs already
flagged. Carrying them across keeps the specs usable as the single source of truth and is the
lowest-risk, highest-leverage work before elaboration. Per project rules, these are proposed, not
applied without instruction.

### 19.1 `Configuration_Specification.md`

> **APPLIED 2026-09-26** — C-1…C-16 promoted into `Configuration_Specification.md` (§15 item 20, rules
> V-094…V-113). Adjusted in the merge: C-3 partitioning is sink-neutral (V-086 per-sink check);
> C-13 `catalog.retry` lands at `sink.iceberg.catalog.retry`; C-14 `sink.maintenance` stays reserved as
> `sink.iceberg.maintenance` / `sink.delta.maintenance`; C-15 at `sink.iceberg.catalog.glue.strip_schema`;
> C-16 verified. Identifier/partition column checks are start-time preconditions (§11.2), not config rules.

| # | Edit | Source |
|---|---|---|
| C-1 | Promote the 10 reserved `globals.cluster.*` fields from §16 into the §6.5 field tables (lease/rebalance/discovery/lifeguard). | Clustering §11/§19.6/§20.4 |
| C-2 | Promote `tables.primary_key` (PR); **remove** `tables.surrogate_key` from §16. | Ghost_Row §7 |
| C-3 | Promote `tables.partitioning` (PR, default none) + its 4 validation rules. | Partitioning §5 |
| C-4 | Promote `snapshot.chunk_size` (R, default 10000, ≥1). | Snapshot §7 |
| C-5 | Promote `sink.max_transaction_batch_rows` (R, null) + `sink.max_transaction_buffer_bytes` (R, ~1GiB) + rules. | Large_Txn §4 |
| C-6 | Promote `sink.buffer_capacity_rows` (R, ~200000). | Operational G-27 |
| C-7 | Promote `source.postgres.max_wal_lag_bytes` (R, null) + floor rule; `source.postgres.slot.on_orphan` and `slot_management` (+ the `require_existing`⇒`auto_cleanup=false` rule). | Operational G-22, Slot_Lifecycle §6 |
| C-8 | Promote `source.mysql.binlog_purge_action` (R, `error`). | Operational G-26 |
| C-9 | **Remove** `source.consistent_snapshot` from §16 (moot — no weaker snapshot mode exists). | Snapshot §3 |
| C-10 | Promote `globals.tracing.otlp_endpoint` + `sample_ratio` (R) + rules; `sink.metrics.per_table` (R, false). | Observability §5 |
| C-11 | Promote `globals.api.role_mapping` + 2 rules; reference the RBAC matrix. | API_RBAC §6 |
| C-12 | Promote `globals.azure.account_name` (R) + rule. | Operational G-34 |
| C-13 | Add optional `catalog.retry { max_attempts, base_backoff, max_backoff }` (R). | Operational G-35 |
| C-14 | Reserve `sink.maintenance` block (disabled in v1). | Maintenance §4 |
| C-15 | Add `glue.strip_schema` + collision-error validation rule. | Operational G-11.3 |
| C-16 | Merge the four Type_Mapping config fields & rules if any remain unmerged (verify V-078..V-082 cover all). | Type_Mapping §Validation |

### 19.2 `Data-Flow.md` (the most-drifted spec)

> **APPLIED 2026-06-20.** All nine landed: DF-1/DF-2/DF-4/DF-5/DF-7/DF-8/DF-9 as edits, DF-3 subsumed by
> DF-1, DF-6 was already correct in `Data-Flow.md` (its residual is a stale note in config §10). Patch
> record: git history (commit on 2026-06-20). The DA-FLOW-2/4/7 and DA-X-1/3 items above are now
> reflected in the spec.

| # | Edit | Source |
|---|---|---|
| DF-1 | Rewrite **Phase 1** from blocking COPY/EXPORT_SNAPSHOT to the stream-first incremental chunk loop (watermarks + PK-upsert reconciliation). | Snapshot §1 |
| DF-2 | Rewrite the **On-Demand Snapshot** section: drop "pause / `pause_lsn + 1`"; use the watermark chunk loop with no stream pause. | Snapshot §4 |
| DF-3 | Remove MySQL `RELOAD`/`LOCK TABLES` from snapshot preconditions. | Snapshot §3 |
| DF-4 | Generalize the `committed_lsn` scalar guard to engine-tagged `cdc.committed_position` (per-table HWM; resume = min/intersect across tables). | Delivery §3/§4 |
| DF-5 | Add the per-change `_seq` to `CdcEvent` and the dedup collapse to Phase 3/4 (mirror-delete semantics, ≤1 delete + ≤1 data per key). | Ghost_Row §4 (see DA-X-1) |
| DF-6 | Re-root per-pipeline state directly under the per-pipeline location; drop the `pipelines/<id>/` segment. | Config §10, Operational G-NEW-20 |
| DF-7 | Add the split-commit rule + checkpoint-pinned-at-txn-start for oversized transactions. | Large_Txn §1 |
| DF-8 | Note pause/stop flush-to-boundary and desired-state reconciliation in the shutdown/handoff section. | Lifecycle §6 |
| DF-9 | State the consolidated **sink-writer contract** (keying, collapse, partition-routing-by-before-image, delete-file sequencing, snapshot-summary properties, truncate). | DA-X-3 |

### 19.3 `System-Overview.md`

> **APPLIED 2026-09-26** (§26.6) — S-1…S-6 as proposed, plus the v1 scope section (D-V1.3/4/5). S-1 became
> a resolved-questions table (every OQ → answer → doc); S-2 an endpoint table with `GET /v1/pipelines`
> and `/v1/pipelines/{id}` added (they were missing from the RBAC matrix — now added there too).

| # | Edit | Source |
|---|---|---|
| S-1 | Replace the resolved "Open Questions" block with pointers to the closing docs (most OQ-* are now closed). | this report |
| S-2 | Add the `/v1` API path list + min-role column; annotate lifecycle endpoints with the state machine. | API_RBAC §6, Lifecycle |
| S-3 | Add the observability catalogue + cardinality policy summary. | Observability |
| S-4 | State external-maintenance-for-v1 + the exposed when-needed signals. | Maintenance §5 |
| S-5 | Document the `node_name` sourcing rule + StatefulSet mandate; the "restart ≠ apply config" footgun. | Clustering §3.4/§9.3 |
| S-6 | Fix the typo `worked` → `worker that` (line ~241 region) and drop the dangling `snapshot_mode: only`. | OQ-M1/M2 |

> `Schema_Change_Detection_and_Evolution.md` is already aligned with the config spec (the `method`
> removal and `rescan_interval` rename are reflected in §13). No further schema-doc merge owed.

---

## 20. Open-decisions register (genuinely undecided, 🟥)

The short list of questions spec elaboration must actually *decide* (everything else is resolved or is
merge/verify work). Ordered roughly by leverage.

> **2026-09-26: every v1 row below is decided** — `V1_Scope_Decisions.md` (D-V1.1–D-V1.6 + deferrals). The register is
> kept as the audit trail; no 🟥 decision remains for v1.

| ID | Decision | Recommendation / starting position |
|---|---|---|
| ~~DA-OBS-5~~ | Fix a **numeric end-to-end replication-lag SLA**, and whether failover-time has its own SLA. | Pick a steady-state max-lag (e.g. 60s fits Balanced lease defaults) + a separate, looser failover-time target (~20–30s); load-test to confirm. Grounds lease/flush/checkpoint tuning. **✅ Decided 2026-09-26 — D-V1.3: 60s p99 steady / 2 min failover (`V1_Scope_Decisions.md`).** |
| ~~DA-X-1 / DA-FLOW-2~~  | `CdcEvent` per-change sequence field + whether `_seq` is a persisted column. | Add `change_seq: SourceSeq` to `CdcEvent`; persist `_seq` as a 6th lineage column (downstream dedup auditability). **✅ Closed 2026-09-26: in `Data-Flow.md` (`change_seq`, `_seq`).** |
| ~~DA-X-4 / DA-SYS-4~~ | Runtime rule for source columns colliding with reserved `_`-names. | Halt the table with a clear error by default; offer a configurable escape-prefix if demand appears. **✅ Decided 2026-09-26 — D-V1.1: halt the table (`V1_Scope_Decisions.md`).** |
| ~~DA-X-5 / DA-FLOW-5~~  | Cross-engine `TRUNCATE` → Iceberg semantics. | Map to an overwrite-to-empty (or delete-all-by-no-key snapshot boundary); define once, same for PG + MySQL; keep idempotent. **✅ Closed 2026-09-26 for v1 (Delta; TRUNCATE barrier); Iceberg → ⏭ v2.** |
| ~~DA-X-6 / DA-SNAP-8~~  | PK-less table under dump-only modes. | Allow dump-only PK-less tables as plain inserts (no `identifier-field-ids`); they never stream/delete so the keying rule doesn't bind. (Or halt for uniformity — decide.) **✅ Closed 2026-09-26: uniform identifier rule (config §11.2).** |
| ~~DA-SRC-8 / DA-X-7~~ | MariaDB GTID dialect support in `SourcePosition`. | Schedule the sub-research; detect MariaDB at connect and select the GTID parser; otherwise narrow v1 scope to MySQL-only. **2026-09-26: proven feasible (S-MY9) — recommendation: keep MariaDB in v1.** **✅ Decided 2026-09-26 — D-V1.5: MariaDB in v1 (`V1_Scope_Decisions.md`).** |
| ~~DA-CFG-4~~ | External secret-manager integration. | Declare out of scope for v1; the file-content-rotation path (sidecar writes the file) is the supported pattern. **✅ Decided 2026-09-26 — confirmed out of v1 (`V1_Scope_Decisions.md`).** |
| ~~DA-DEDUP-7~~ | Cross-table atomic visibility. | Keep out of scope; document the per-table-only guarantee; reopen only if a multi-table-commit catalog (Nessie) becomes a requirement. **✅ Decided 2026-09-26 — confirmed out of scope (`V1_Scope_Decisions.md`).** |
| ~~DA-CLUS-12~~ | `kubernetes` lock_backend. | Defer; `storage` CAS lease is the v1 backend (already enforced by V-075). **✅ Decided 2026-09-26 — deferral confirmed (`V1_Scope_Decisions.md`).** |
| ~~DA-CLUS-13~~ | Per-source-endpoint takeover bulkhead. | Defer; per-node-global cap ships in v1; add per-endpoint group if reconnect storms are observed. **✅ Decided 2026-09-26 — deferral confirmed (`V1_Scope_Decisions.md`).** |
| ~~DA-SINK-8~~ | Spill-to-disk for oversized transactions. | Defer to a future `sink.large_txn_mode: spill`; v1 uses split-commits or sized memory. **✅ Decided 2026-09-26 — deferral confirmed (`V1_Scope_Decisions.md`).** |
| ~~DA-CAT-4~~  | How the writer consumes catalog-vended credentials. | Specify in the sink-writer contract if credential vending is targeted for v1; otherwise note as future. **⏭ v2 (Iceberg sink).** |
| ~~DA-FLOW-6~~ | PG `DROP TABLE` reaction + detection latency (no in-band DDL on pgoutput). | Detect via channel D rescan (table gone from catalog / publication); halt the table with `TABLE_DROPPED`, never auto-drop the Delta table. State the latency bound = rescan interval. **✅ Decided 2026-09-26 — D-V1.2: halt the table, Delta kept (`V1_Scope_Decisions.md`).** |
| ~~DA-SYS-7~~ | v1 handling of `sink.type: iceberg` (Iceberg sink → v2). | Keep accepted by the config; refuse at start with an explicit "Iceberg sink ships in v2" message (enum stable across versions). **✅ Decided 2026-09-26 — D-V1.4: enum kept, refused at start (`V1_Scope_Decisions.md`).** |

---

## 21. Capability & verification gates (🧪 — must be proven, cannot be closed on paper)

These are the items most likely to invalidate a "design complete" claim. Each depends on a real
behavior of a pinned crate or a cloud service. **Recommendation: stand up the prototypes/gates below
before (or in parallel with) writing the implementation spec, because a negative result changes the
design, not just the code.**

| ID | Gate | Depends on | If it fails |
|---|---|---|---|
| DA-CLUS-1 / DA-DEP-2 ✅ closed 2026-06 | Atomic CAS (create-if-absent, swap-if-version, conditional-delete) on **S3, GCS, Azure** — especially the **GCS generation (not ETag)** mapping. | OpenDAL ≥ late-2024 + per-backend behavior. | The storage lease is unsafe; either drop to native SDKs per backend, or reconsider the lock backend. **Top blocker.** Suite scaffolded at `prototypes/lease-conformance/` (T1–T10); T4/T5/T10 must run against real services. |
| ⏭ v2 · DA-SINK-1 | Equality-delete writer assigns delete files the **enclosing commit's sequence number** | iceberg-rust 0.9.0. **Confirmed ABSENT on 0.9.1/0.10.0/0.10.1 and the v0.11.0-rc.1/rc.2 surfaces (2026-09-19, compiled crate).** | The ghost-row strictly-less-than guarantee breaks; need position-deletes-after-read or a different write strategy. **Blocks the Iceberg mirror only** since the 2026-09-26 multi-sink re-scope (§25). |
| ✅ append path (G2/G2b/G2c) · delete path ⏭ v2 · DA-SINK-2 / DA-X-2 | Custom `cdc.*` **snapshot-summary properties preserved across commits** | iceberg-rust 0.9.0. **Append-path mechanism flash-verified on 0.10.1 (2026-09-19): `fast_append().set_snapshot_properties(...)` + catalog reload round-trip in `prototypes/iceberg-capability`.** Full gate (carrier across delete commits) deferred to the delete-commit capability. | `owner_epoch` fence + `committed_position` guard lose their carrier; need a side-channel (e.g. table properties) or an alternate fence. |
| ⏭ v2 · DA-SCHEMA-8 | `drop_column` / `rename_column` / type-promotion transactions supported. | iceberg-rust 0.9.0. **`update_schema` confirmed present on 0.10.1 (2026-09-19); the open question narrowed to drop/rename/promotion extent.** | Route the unsupported op to "halt table" until the crate catches up (graceful, already the fallback). |
| ⏭ v2 · DA-TYPE-5 | `FixedSizeBinary(16)` → Iceberg `uuid` annotation mechanism. | iceberg-rust 0.9.0. | UUID columns fall back to `fixed(16)`/`binary`; cosmetic, low risk. |
| ⏭ v2 · DA-SINK-10 | Compaction / rewrite API availability. | iceberg-rust (future). | Built-in maintenance stays deferred (already external for v1); no v1 impact. |
| DA-MS-1…4 ✅ closed 2026-09-26 | Delta sink: MERGE atomicity + idempotent replay, `txn` epoch fence, cost model, commit atomicity on **real S3, GCS, Azure** (P1–P10). | delta-rs 1.0.0 / object_store 0.13 | — (proven; `prototypes/delta-capability/`) |
| ✅ DA-SRC-10 (S-PG0…8, 2026-09-26) | PG logical-replication client: pgoutput v1–v4 (streamed in-progress txns), Relation/Type/Truncate, unchanged-TOAST, standby-status flush-LSN ack, TLS. | `pgwire-replication` (unpinned) | Write/fork the pgoutput protocol layer over `tokio-postgres` replication mode — a large v1 work item. |
| ✅ DA-SRC-11 (S-MY0…9, 2026-09-26; `mysql_async`) | MySQL binlog client: GTID-set dump, FULL row images, TABLE_MAP optional metadata, QUERY_EVENT, CRC32, heartbeats, `caching_sha2_password` + TLS; MariaDB GTID if in scope. | `mysql-binlog-connector-rust` (unpinned) | Fork/extend the crate or implement the binlog protocol; or narrow v1 to PG-only. |

> **2026-09-26 — v1 view.** With the Iceberg sink in v2, the only v1 gates were the **source-side
> crates** (DA-SRC-10/11) — **both proven the same day** (`prototypes/source-capability/`). No v1 🧪
> gate remains. Everything Iceberg below is the v2 gate list.

A single **iceberg-rust capability probe** (one integration test exercising equality-delete
sequencing, snapshot-summary persistence, schema-evolution ops, and the uuid annotation against the
pinned version) would close five of these six at once. The storage-CAS conformance suite closes the
sixth (and the top blocker).

> **Each gate is elaborated in §24 (Appendix A)** — mechanism, exact claim to prove, verification
> plan with pass/fail criteria, design fallback, and redesign-risk impact. §24.0 triages which gates
> can force a redesign (only DA-CLUS-1 and DA-SINK-1) versus which degrade gracefully.

---

## 22. Spec-readiness assessment

> **v1 re-assessment (2026-09-26; Iceberg sink deferred to v2) — close, not yet.** The sink, the lease and
> the config/schema/snapshot design are proven or specified. Before the v1 implementation plan:
> (1) ~~source-side capability gates DA-SRC-10/11~~ ✅ proven the same day (the pgoutput decoder is
> ours — a bounded v1 work item); (2) **v1 decisions** — DA-SRC-8 (MariaDB in v1, informed by (1)),
> DA-SYS-4 (reserved-column collision), DA-FLOW-6 (PG DROP), DA-SYS-7 (Iceberg handling in v1),
> DA-OBS-5 (lag target — non-blocking); confirm the §20 deferrals; (3) **spec debt** — §19.3
> System-Overview S-1…S-6 (still unapplied) + the DA-X-8 config residual. Details: §26. The verdict
> below is the 2026-06 assessment, kept as the audit trail.
> **Update (same day):** (2) is done — all v1 decisions signed off (§26.5). Only (3), spec debt, remains
> before the v1 implementation plan. **(3) done too (§26.6) — ready for v1 implementation planning.**

**Verdict: the foundation is strong; design is essentially complete and the application specification
can begin now**, provided the work is sequenced so the two 🔴 verification gates are run early (a
negative result there is the only thing that would force a design change rather than a spec edit).

Readiness by area:

- **Ready to specify immediately:** configuration model, catalog auth, RBAC/API, schema-evolution
  detection/apply, type mapping, snapshot model, dedup/keying, clustering protocol, lifecycle state
  machine, observability, source-lifecycle. All have a closing research doc.
- **Specify against a verification gate:** the sink writer (DA-SINK-1, DA-SINK-2) and the storage
  lease (DA-CLUS-1). Write the contract, but treat the crate/cloud behavior as an assumption to be
  proven, with the documented fallback paths in the spec.
- **Decide first (small, bounded):** the ~12 items in §20 — most have a clear recommended default
  already; a half-day of decisions unblocks them.
- **Mechanical, do anytime:** the §19 spec-merge debt — best done before elaboration so the specs are
  internally consistent when the spec author reads them.

Suggested sequence: (1) run the two verification gates / probes; (2) make the §20 decisions; (3) apply
the §19 merges to the specs; (4) write the consolidated sink-writer contract (DA-X-3); (5) proceed to
full application specification.

---

## 23. Document inventory & authority

| Document | Role | Authoritative for | Edit policy |
|---|---|---|---|
| `specs/System-Overview.md` | spec | top-level requirements, scope | Oleksii owns; propose-only |
| `specs/Data-Flow.md` | spec | pipeline stages, CdcEvent, state layout | Oleksii owns; propose-only (most merge debt, §19.2) |
| `specs/Configuration_Specification.md` | spec | config model, validation, mutability | Oleksii owns; propose-only |
| `specs/Schema_Change_Detection_and_Evolution.md` | spec | detection channels, DDL classification, apply | reviewed & approved |
| `research/Type_Mapping.md` | research | three-hop type mapping | Claude authors |
| `research/Catalog_Auth_Research.md` | research | catalog auth schema | Claude authors |
| `research/Clustering_Architecture_Research.md` | research | two-plane clustering, lease, propagation | Claude authors |
| `research/Snapshot_Resumability_and_Consistency.md` | research | incremental chunked snapshot | Claude authors |
| `research/Ghost_Row_Dedup_and_Primary_Keys.md` | research | equality-delete keying, PK policy | Claude authors |
| `research/Delivery_Guarantee_and_Position_Guard.md` | research | SourcePosition, committed_position | Claude authors |
| `research/Pipeline_Lifecycle_State_Machine.md` | research | pause/stop/resume/restart/flush | Claude authors |
| `research/Postgres_Slot_and_Publication_Lifecycle.md` | research | PG slot/publication runtime lifecycle | Claude authors |
| `research/MySQL_DDL_Classifier.md` | research | channel-B classifier | Claude authors |
| `research/Iceberg_Maintenance.md` | research | maintenance ownership + signals | Claude authors |
| `research/Iceberg_Partitioning_and_Layout_Evolution.md` | research | partitioning, MOR↔COW change | Claude authors |
| `research/Large_Transaction_Handling.md` | research | split commits, memory backstop | Claude authors |
| `research/Observability_Metrics_and_Tracing.md` | research | metrics catalogue, tracing | Claude authors |
| `research/Operational_and_Scope_Gaps_Batch.md` | research | WAL/binlog guards, Azure, MariaDB, IAM | Claude authors |
| `research/API_RBAC_and_Versioning.md` | research | RBAC matrix, versioning | Claude authors |
| `sources/Sources_Index_v3.md` | reference | 50-source index, crate versions | read-only |
| `analysis/CDC_Gap_Analysis_Report.md` | analysis | legacy v9 tracker | retained as audit trail |
| `analysis/CDC_Design_Gap_Analysis_v1.md` | analysis | **this fresh re-derivation** | Claude maintains |

### Cross-reference: this report's IDs ↔ legacy IDs

The *(parenthetical)* legacy IDs in every gap table above are the bridge back to the v9 tracker. Notable
legacy blockers and their current status here: `G-8`→DA-CLUS-2 ✅, `G-9`→DA-CLUS-5 ✅, `G-13`→DA-DEDUP-1
✅ (+ DA-SINK-1 🧪), `G-14`→DA-DEDUP-2 ✅, `G-6`→DA-SNAP-1 ✅, `G-15`→DA-SRC-1..4 ✅, `G-16`→DA-API-1 ✅,
`G-18`→DA-TYPE-1 ✅, `G-19/19a/19b`→DA-SCHEMA-* ✅, `D-5`→DA-CLUS-1 🧪 (still the top blocker),
`D-6`→DA-CLUS-11 ✅. The legacy "remaining blockers" list (D-5, D-6, G-13/G-30, G-14, G-6, G-11, G-15,
G-16, G-NEW-20/21/22) is now **all resolved on paper except D-5 (DA-CLUS-1)** — plus the newly-explicit
crate gate DA-SINK-1, which the legacy report folded into G-13's implementation note.

---

## 24. Appendix A — Blocker & capability-gate deep-dive (verification + fallback)

This appendix elaborates the six items in §21 that cannot be closed on paper. For each: the
**mechanism** it underpins, the **exact claim** that must be true, a concrete **verification plan**
(harness + pass/fail), the **design fallback** if it proves false, and the **redesign-risk impact**.

### 24.0 Triage — only two of the six can force a redesign

The six gates are not equal. Two of them, if they fail, invalidate a core design choice and force a
different architecture (🔴). The other four have a benign fallback that is *already part of the
agreed design*, so a negative result degrades a feature or a defense layer without reopening a
decision (🧪).

| Gate | DA-ID | If it fails… | Redesign? |
|---|---|---|---|
| Storage CAS conformance | DA-CLUS-1 | the storage lease is unsafe (esp. silent on GCS) | **Yes → native-SDK lease path** 🔴 |
| Equality-delete sequencing | DA-SINK-1 | the MOR ghost-row guarantee breaks | **Yes → position-delete or COW writer** 🔴 |
| Snapshot-summary persistence | DA-SINK-2 | lose the sink-side fence + dedup optimization | No — idempotency is correctness; fence is defense-in-depth 🧪 |
| Schema-evolution ops | DA-SCHEMA-8 | unsupported op can't auto-apply | No — halt-the-table is already the safety branch 🧪 |
| uuid annotation | DA-TYPE-5 | uuid stored as `fixed(16)`/`string` | No — cosmetic logical-type loss 🧪 |
| Compaction API | DA-SINK-10 | no built-in maintenance | No — external maintenance is the v1 plan 🧪 |

The practical consequence: **focus verification effort on DA-CLUS-1 and DA-SINK-1.** The other four
are worth a single confirmatory test each, but their failure paths are already designed.

---

### 24.1 DA-CLUS-1 — Atomic compare-and-swap conformance on S3 / GCS / Azure 🔴

**Mechanism.** The entire single-writer safety guarantee (`Clustering…` §5) is a per-pipeline lease
object mutated only by atomic compare-and-swap. Everything reduces to three operations on one object:
**acquire** = create-if-absent, **renew/takeover** = swap-if-version-matches, **release** =
conditional delete.

**Exact claim (per backend).**

| Backend | Create-if-absent | CAS update | Version token | Conflict code |
|---|---|---|---|---|
| S3 | `If-None-Match: *` | `If-Match: <etag>` | **ETag** | 412 (sometimes 409 `ConditionalRequestConflict` → retry) |
| GCS | `x-goog-if-generation-match: 0` | `x-goog-if-generation-match: <generation>` | **generation (int64), NOT ETag** | 412 |
| Azure Blob | `If-None-Match: *` | `If-Match: <etag>` | **ETag** | 412 |

The load-bearing risk is the **GCS trap**: GCS honors ETag-match only on *reads*; object-modifying
writes must use the **generation** precondition. An abstraction that feeds the GCS read-ETag into a
"write if-match" silently performs **no CAS** and breaks single-writer on GCS with no error.

**Verification plan.** The `LeaseStore` trait + the T1–T10 conformance suite already scaffolded at
`prototypes/lease-conformance/`:

- T1–T3, T6–T8: functional acquire/swap/release happy paths (emulators OK: MinIO / fake-gcs-server / Azurite).
- **T4** swap-with-stale-version → Conflict, and **T5** N concurrent swaps on the same version →
  exactly one Ok — these two *prove* "at most one writer."
- **T9** S3 `409 ConditionalRequestConflict` surfaces as a retryable Conflict (not a hard error).
- **T10** the token fed back into `swap` on GCS is the **generation**, not the read ETag — the gating
  assertion for the trap.
- Startup guard: assert `op.info().full_capability()` advertises `write_with_if_not_exists` and
  `write_with_if_match`; **refuse to start** if a backend build lacks them. (Real-S3 finding: S3 reports
  `write_with_if_none_match = false` — create-if-absent is the dedicated `if_not_exists` capability; and
  OpenDAL has no conditional delete, so release tombstones via `write_with(...).if_match(...)`.)

**Pass/fail.** T4, T5, T10 **must pass against the real services** (not just emulators — emulators
implement preconditions loosely). T10 failing means the OpenDAL→GCS mapping is wrong and the storage
lease is unsafe on GCS. Pin OpenDAL ≥ the release where `if_match`-on-writes landed (late-2024 /
early-2025).

**Fallback.** The `LeaseStore` trait already isolates this. If OpenDAL lacks a capability or mis-maps a
backend, **drop that backend to its native SDK** behind the same trait (Route B, `Clustering…` §17.3):
`aws-sdk-s3` (`if_none_match`/`if_match`), the GCS SDK (`if_generation_match`), `azure_storage_blob`
(`If-None-Match`/`If-Match`). This is a per-backend escape hatch, not a redesign of the lease protocol.
The protocol itself (acquire/renew/takeover, §5.3) is unchanged. The optional `kubernetes` lock_backend
(D-2) remains a separate future `LeaseStore` impl for k8s.

**Impact.** 🔴 because a *silent* failure on GCS would corrupt single-owner safety in production. But
the cost of the fallback is bounded (native SDK for one backend), so it is a "must verify," not a
"design may collapse."

**S3 run outcome (2026-06) — PROVEN.** T1–T9 pass against real AWS S3, on both versioning-enabled and
non-versioned buckets. Two findings were folded back into the harness and the clustering doc §17.1/§17.2:
(1) create-if-absent is OpenDAL's **`if_not_exists`** capability — S3 reports `write_with_if_none_match =
false`; (2) the `if_match` CAS token must be the **ETag** on S3/Azure (the **generation** on GCS) — a
versioning-enabled bucket populates `version()` with the version-id, which `If-Match` rejects, so the
token kind is fixed by backend. Bucket versioning is **not required** (best kept off for the lease prefix).

**GCS run outcome (2026-06) — Route A does NOT work on GCS (the trap, confirmed early).** OpenDAL's GCS
backend reports **`write_with_if_match = false`** — the conformance suite fails at the *static* capability
assertion, before any network/auth call (so it is **not** a credentials or bucket-access problem). Root
cause: GCS write preconditions are **generation-based** (`ifGenerationMatch`), not ETag/`If-Match`; OpenDAL
exposes the generation only as `if_not_exists` (=0) for *create* and has no arbitrary generation-match
write option, so it cannot express the lease's **CAS update** (renew/takeover) on GCS. **Resolution
(the documented escape hatch fires): implement the GCS `LeaseStore` over the native GCS SDK**
(`google-cloud-storage`, `ifGenerationMatch` for both create=0 and swap=N; token = generation), behind the
same trait. Route A (OpenDAL) covers **S3 + Azure**; **GCS = Route B**. Net design change: `lock_backend:
storage` uses OpenDAL for S3/Azure and the native GCS SDK for GCS — a per-backend `LeaseStore` impl, the
protocol (§5.3) unchanged.

**GCS run outcome (2026-06) — PROVEN (Route B).** The native-SDK `GcsLeaseStore`
(`prototypes/lease-conformance/` `--features gcs`, `ifGenerationMatch`) passes **T1–T10 against real GCS,
including T10 — the generation-CAS gate** (a stale generation is correctly rejected). This was the single
riskiest assumption in the lease design; it now holds with a native SDK.

> **SDK-port note (2026-06) — re-verified.** GCS was first proven on yoshidan's `google-cloud-storage`
> 0.24, then migrated to the **official `google-cloud-storage` 1.x** (`googleapis/google-cloud-rust`:
> `Storage::builder()` / `write_object().set_if_generation_match()` / `read_object()`), with `reqwest`
> bumped to 0.13. **T1–T10 re-run green against real GCS on 1.15**, so the port is confirmed (incl. the
> t10 generation-CAS gate). 1.x quirk recorded: a failed `ifGenerationMatch` surfaces as `code UNKNOWN`
> ("…pre-conditions you specified did not hold"), not a clean 412, so the conflict check matches that text.

**Azure run outcome (2026-06) — OpenDAL 0.57 azblob ALSO lacks `write_with_if_match`.** The printed
matrix is `if_not_exists=true if_match=false if_none_match=true`, so OpenDAL can't do the lease's CAS
update on Azure either — the same gap class as GCS, but here it is an **OpenDAL limitation, not an Azure
one** (Azure Blob natively supports `If-Match` conditional PUT; OpenDAL 0.57 just hasn't wired it into
the azblob write path). The project is pinned to OpenDAL 0.57 (shared with the iceberg sink), so bumping
is not an option. Net: **on 0.57, Route A (OpenDAL) covers S3 only; both GCS and Azure use native Route
B.** Azure is now implemented as a native `AzureLeaseStore` (`--features azure`): **raw Blob REST over
`reqwest` + SAS**, `If-None-Match: *` (create) / `If-Match: <etag>` (CAS), ETag token — same trait, same
T1–T10 suite. (Raw REST + SAS was chosen over the Azure SDK to avoid SDK-API churn and request signing.)

**Status: CLOSED 2026-06 — all three backends proven against real services.** Azure native Route B
(`--features azure`, Blob REST + SAS) passes T1–T9 against a real Azure container; S3 (T1–T9, versioned +
non-versioned) and GCS (T1–T10, incl. the generation-CAS gate) already proven. DA-CLUS-1 is closed; the
`prototypes/lease-conformance/` suite is the durable regression proof for all three backends.

> **Architecture note (lock_backend: storage).** The portable storage lease is now a **per-backend
> `LeaseStore`**: OpenDAL for S3, native `google-cloud-storage` for GCS, native Blob-REST for Azure —
> all behind one trait, the §5.3 protocol unchanged. "OpenDAL as the single abstraction for all three"
> did not survive contact with real services; the trait-with-per-backend-impls did.

---

### 24.2 DA-SINK-1 — Equality-delete files carry the enclosing commit's sequence number 🔴

**Mechanism.** An Iceberg equality delete applies **only to data files whose data-sequence-number is
strictly less than the delete file's** (`Ghost_Row…` §0). This one property makes the in-batch upsert
correct: a "delete old key + insert new key" written in the *same* commit shares one sequence number,
so the delete purges every **prior-commit** version of the key while sparing the **same-commit**
insert. If the writer mis-sequences — gives the same-commit insert a lower sequence than its delete —
the row that must survive is purged, or (the documented ghost-row bug) a row that must disappear
survives forever.

**Exact claim.** iceberg-rust 0.9.0's equality-delete writer assigns delete files **the enclosing
commit's data-sequence-number** (equal to the data files committed in the same transaction), and the
strictly-less-than rule holds on read.

**Verification plan.** A focused integration test against the *pinned* crate:

1. Create a MOR table with `identifier-field-ids` set.
2. Commit C1: insert key `K=1` (data seq `s1`).
3. Commit C2 (single transaction): equality-delete `K=1` **and** insert `K=1` with a new image.
4. **Assert (read):** the table shows exactly the C2 image of `K=1` — the same-commit delete did **not**
   remove the same-commit insert, and it **did** remove C1's version.
5. **Assert (metadata):** the C2 delete file's sequence number equals C2's data sequence number.
6. **Cross-read with a mature engine** (Spark or Trino Iceberg) to confirm the on-disk result, not just
   iceberg-rust's own reader — the two must agree.
7. Ordering test: insert/delete of the same key spread across three commits resolves to the last op.

Suggested home: a new `prototypes/iceberg-capability/` test crate, pinned to the exact iceberg-rust
version, parallel to `prototypes/lease-conformance/`.

**Pass/fail.** Step 4 + step 6 are the gate. If the same-commit insert is purged, or a Spark read
disagrees with iceberg-rust's, the equality-delete path is unusable as designed.

**Fallback (in priority order).**

1. **Position deletes after a read.** For each affected key, read the data file(s), compute row
   positions, write position-delete files. Correct, but adds a read per affected file and is
   partition-scoped — materially costlier on write, and changes the §4 collapse contract.
2. **COW for affected tables.** Read-modify-rewrite whole data files (`Ghost_Row…` §5 already notes COW
   carries the same identifier rules). Highest write cost; reserved as the heavy fallback.
3. **Manual sequence assignment** if the crate exposes per-file sequence control — keep equality
   deletes but set the sequence explicitly.
4. **Pin/upgrade or contribute upstream** to a crate version that sequences correctly.

**Impact.** 🔴 and the single most consequential sink gate: a failure cascades into `Ghost_Row…`
(keying/collapse), `Iceberg_Partitioning…` (before-image partition routing of deletes),
`Iceberg_Maintenance…` (delete-file compaction), and `Delivery…` (the per-table guard rides the same
writer). All of those assume equality deletes work. The position-delete fallback keeps correctness but
is a real writer redesign — hence verify **before** the sink writer is specified in detail.

---

#### 24.2.1 RESULT (2026-06) — 🔴 **iceberg-rust 0.9.1 cannot commit deletes at all. This is worse than a sequencing bug.**

The probe was answered by reading the pinned crate's public API (`iceberg` 0.9.1, the latest 0.9). The
question is no longer "does the equality-delete writer sequence correctly" — it is "**can deletes be
committed at all?**", and the answer is **no**:

- **The writer exists, the commit path does not.** `iceberg::writer::base_writer` does expose
  `EqualityDeleteFileWriter`, `PositionDeleteFileWriter`, and `DataFileWriter` — so delete files can be
  *written*. But the **`Transaction` commit API exposes only**: `fast_append()` (→ `add_data_files(...)`,
  **data files only**), `update_table_properties`, `replace_sort_order`, `update_location`,
  `update_statistics`, `upgrade_table_version`, `commit`. There is **no row-delta / overwrite / rewrite
  / delete-commit action, and no `update_schema`.** The transaction module contains only
  `Transaction`, `ActionCommit`, and the `ApplyTransactionAction` trait. The doc's own example states
  fast-append "will not rewrite or append to existing metadata."
- **Consequence.** iceberg-rust 0.9.1's public write API is **append-only**. You can ingest data files;
  you **cannot commit equality-delete files, position-delete files, overwrites, or file rewrites.**
  The MOR-equality-delete sink (the system default) is **not implementable** on 0.9.1 — and the **COW
  fallback is equally blocked**, because COW also needs an overwrite/replace-files commit, which is
  absent. A CDC sink whose entire purpose is to apply UPDATEs and DELETEs cannot be built on this crate
  version through the public API. This is the position-delete/COW fallback from the list above being
  *unavailable*, not just costly.

**Options (all require a decision before the sink is specced):**

1. **Track iceberg-rust and pin a version that adds delete/overwrite commits.** The `fast_append`
   naming and the active write-path work upstream imply row-delta/MOR commit support is coming; this is
   the lowest-friction path but is **schedule-dependent on an external project**. Make the sink spec
   explicitly gated on "iceberg-rust ≥ the release that lands delete commits," and re-run this probe then.
2. **Drop to a lower-level commit** — assemble the snapshot/manifest with delete files via iceberg-rust's
   spec types directly (below the `Transaction` convenience API), if those internals are public enough.
   High effort and fragile across versions; essentially re-implementing the delete-commit action.
3. **Bridge the commit to another Iceberg implementation** (PyIceberg / Java) for the delete/upsert
   commit while keeping the Rust capture/decode/accumulate path. Contradicts the "Rust-native, no JVM"
   design tenet; heavy operationally.
4. **Contribute the delete-commit (row-delta) action upstream** to iceberg-rust. Most aligned with the
   design, real engineering + review-cycle cost, benefits the ecosystem.
5. **Restrict v1 to append-only** (snapshot/insert-only tables) — **not viable** for a CDC system that
   must mirror updates and deletes; listed only to reject it.

**This finding outranks the original DA-SINK-1 framing and should headline the sink section of the
application spec:** the sink's core write strategy (MOR equality deletes, and its COW fallback) is **not
buildable on the pinned iceberg-rust**, so the spec must either gate on a future crate version (option 1)
or commit to options 2–4. The capture→decode→accumulate pipeline is unaffected; the gap is entirely at
the Iceberg *commit* boundary. The probe (`prototypes/iceberg-capability/`) records this; re-run it
against any newer iceberg-rust to detect when delete commits land.

> **DECISION (2026-06) — Option 1: track iceberg-rust; gate the sink spec on a delete-commit-capable
> release.** The sink writer's detailed spec is **deferred** until iceberg-rust ships a delete-aware
> commit action (row-delta / overwrite / merge-append). Capture/decode/accumulate spec work may
> proceed; the *commit* layer waits. **Revisit ~2026-07-21**: re-check the iceberg-rust `Transaction`
> API (and the `iceberg-capability` canary) for a delete-commit action. A scheduled reminder is set.
> If, by the revisit, no delete-commit support has landed and the timeline matters, escalate to
> option 4 (contribute upstream) or 2 (low-level commit).

> **Knock-on:** the same "no `update_schema` / no rewrite" gap means **DA-SCHEMA-8** (schema evolution
> via transaction) and **DA-SINK-10** (compaction/rewrite) are *also* absent from 0.9.1's public API —
> see §24.4 and §24.6. DA-SINK-2 (snapshot-summary properties) is moot until a delete-commit path exists.

---

#### 24.2.2 RECHECK (2026-07-22) — still 🔴 **no delete-commit action; `iceberg` 0.10.0 checked**

Scheduled recheck per the 2026-06 decision's "revisit ~2026-07-21" trigger. Latest published crate at
recheck time: **`iceberg` 0.10.0** (crates.io `max_stable_version`; published 2026-07-21, one day before
this recheck — so this is a fresh release, not a stale read).

**`Transaction` API in 0.10.0** (`docs.rs/iceberg/0.10.0/iceberg/transaction/struct.Transaction.html`):
`new`, `upgrade_table_version`, `update_table_properties`, `update_schema`, `fast_append`,
`replace_sort_order`, `update_location`, `update_statistics`, **`expire_snapshots`** (new in this
release), `commit`. `fast_append` still only exposes `add_data_files(...)` — no `row_delta`,
`overwrite`, `rewrite_files`, or `merge_append` action exists anywhere on `Transaction`.

**Result: NO — delete-commit support has not landed.** The MOR-equality-delete sink (and its COW
fallback) remain un-buildable on the public `Transaction` API for the same reason recorded in §24.2.1.
The one addition since the June check is `expire_snapshots` (a maintenance action — relevant to
DA-SINK-10/`Iceberg_Maintenance…`, not to DA-SINK-1).

**Correction to §24.2.1:** `update_schema` is *not* missing — it was present on `Transaction` in 0.9.1
at recheck time (and remains in 0.10.0). The June finding that "no `update_schema`" existed appears to
have been an error in that pass (possibly checking against `rust.iceberg.apache.org`'s
then-lagging docs mirror rather than the published crate); DA-SCHEMA-8's knock-on framing in §24.2.1
and §24.4 should be read as "unverified pending the DA-SCHEMA-8 probe," not "absent from the API."

**Action taken:** `prototypes/iceberg-capability/Cargo.toml` bumped `iceberg` from `"0.9"` to `"0.10"`.
Could not run `cargo build -p iceberg-capability --features probe` as a canary in this session — no
Rust toolchain available in the execution sandbox. **Recommend running that build locally** before
relying on the bump; the probe bodies are still `todo!()` scaffolds either way (§24.2's verification
plan / G1 probe), so a green build only confirms the dependency bump resolves, not that the delete
sequencing behavior holds.

**Decision unchanged:** still tracking iceberg-rust (option 1). No new revisit date is fixed by this
recheck; re-run when the next iceberg-rust minor ships, or sooner if upstream issue/PR activity signals
a row-delta or overwrite commit action landing.

---

#### 24.2.3 RECHECK (2026-08-23) — still 🔴 **no delete-commit action; `iceberg` 0.10.1 checked; upstream CoW work spotted**

Automated recheck (scheduled task `iceberg-delete-commit-recheck`). Latest published crate at recheck
time: **`iceberg` 0.10.1** (crates.io `max_stable_version`; published 2026-08-01, three weeks before
this recheck).

**`Transaction` API in 0.10.1** (`docs.rs/iceberg/0.10.1/iceberg/transaction/struct.Transaction.html`,
cross-checked against the crate's own `src/transaction/mod.rs` source on docs.rs): `new`,
`upgrade_table_version`, `update_table_properties`, `update_schema`, `fast_append`,
`replace_sort_order`, `update_location`, `update_statistics`, `expire_snapshots`, `commit` — unchanged
from the 0.10.0 set in §24.2.2. `fast_append` still only exposes `add_data_files(...)`. No `row_delta`,
`overwrite`, `rewrite_files`, or `merge_append` method exists on `Transaction`, and the `transaction`
module's public struct list (`ActionCommit`, `AddColumn`, `Transaction`) has no `OverwriteAction` or
`RowDeltaAction` type exported.

**Result: NO — delete-commit support has still not landed in a published release.** The
MOR-equality-delete sink (and its COW fallback) remain un-buildable on the public `Transaction` API,
same root cause as §24.2.1/§24.2.2.

**New signal worth tracking closely.** ⚠️ *Superseded 2026-09-19 — the merge claims below were
upstream-reported and are RETRACTED by direct verification (§24.2.4): PR #2185 is open/unmerged,
and no delete-aware action exists on `main` or in the v0.11.0-rc.* releases. Kept as the historical
record.* `apache/iceberg-rust` issue **#2186** ("Copy-on-Write and Merge-on-Read support", opened
2026-02-27) is an epic explicitly tracking this gap upstream:

- Copy-on-Write: **`OverwriteAction` (PR #2185, "add OverwriteAction with CoW delete support" —
  overwrite semantics with manifest rewriting for deleted entries) is checked off as merged** in the
  epic's task list. Conflict detection, filter-based overwrite, and rewrite-based compaction are still
  open (unchecked).
- Merge-on-Read: `RowDeltaAction` (position/equality delete file write path), scan-side delete
  reconciliation, and V3 Deletion Vectors are **all unchecked / not started.**

Despite `OverwriteAction` being marked merged in the epic, it does **not** appear anywhere in the
0.10.1 public API checked above — no `overwrite()` method on `Transaction`, no `OverwriteAction` struct
in the `transaction` module docs, and no such module in the `src/transaction/mod.rs` source. Most
likely explanation: merged to `main` after the 0.10.1 release cut (2026-08-01) and not yet published to
crates.io, or not yet wired into the public `Transaction` surface even on `main`. This could not be
confirmed further without a GitHub API/commit-date check beyond what was available this session — treat
the merge status as **upstream-reported, not independently confirmed against a tagged release.**

**Practical read:** if `OverwriteAction` does ship in the next `iceberg` release, it would very plausibly
unblock **option 2 from §24.2.1 — the COW fallback** (whole-file overwrite/replace), since "overwrite
semantics with manifest rewriting for deleted entries" is exactly the commit primitive COW needs. It
would **not** by itself unblock the MOR-equality-delete default path, which needs `RowDeltaAction`
(still unstarted per the epic). This raises the value of re-checking on a **shorter cadence** than "next
minor" — the next `iceberg` patch/minor release is worth checking specifically for `OverwriteAction`
landing, independent of `RowDeltaAction` progress.

**Action taken:** none to `prototypes/iceberg-capability/Cargo.toml` — its existing dependency pin
(`version = "0.10"`, i.e. `^0.10`) already resolves to 0.10.1, so no bump was needed for the version
requirement itself; only the comment's "latest checked" note would go stale, not the resolution. Did
**not** attempt `cargo build -p iceberg-capability --features probe` — no Rust toolchain available in
this session's execution sandbox (same limitation as §24.2.2). **Recommend running that build locally**
before relying on the pin, and recommend implementing the `iceberg-capability` G1 probe (equality-delete
same-commit sequencing, §24.2's verification plan) once a delete-aware commit action is confirmed
available, per the scheduled task's standing instruction.

**Decision unchanged:** still tracking iceberg-rust (option 1 in §24.2.1). Recommend the next recheck
trigger on **either** the next `iceberg` crate release **or** direct confirmation (via the GitHub repo)
that `OverwriteAction`/`RowDeltaAction` landed in a tagged version — whichever comes first — rather than
waiting a full minor-release cycle as before.

---

#### 24.2.4 RECHECK (2026-09-19) — still 🔴 **no delete-commit action in any release, incl. v0.11.0 RCs; §24.2.3's "OverwriteAction merged" lead retracted; G2 flash-verified on the append path**

This session is the **first recheck with a Rust toolchain available**, so the finding is now backed by
the **compiled crate** (plus the repo tags), not docs.rs alone. `prototypes/iceberg-capability`
`Cargo.lock` was bumped 0.9.1 → 0.10.1, `cargo build --features probe` **compiles green**, and the
append-path probe **passes**.

**The delete-commit question, checked three independent ways — still NO:**

1. **Crates.io latest remains `iceberg` 0.10.1** (published 2026-08-01; rust-version **1.94**). No
   0.11.0 published; RCs are GitHub tags only.
2. **Compiled 0.10.1 source** (`~/.cargo/registry/…/iceberg-0.10.1/src/transaction/`): `Transaction`
   exposes `new`, `upgrade_table_version`, `update_table_properties`, `update_schema`, `fast_append`,
   `replace_sort_order`, `update_location`, `update_statistics`, `expire_snapshots`, `commit` — same
   set as §24.2.2/§24.2.3. `FastAppendAction` accepts **data files only** and produces an
   `Operation::Append` snapshot with **no delete entries** (`delete_entries` → empty; the §24.2.3
   append.md source confirms a delete-only-manifest-carry-forward fix #2148, but that is a preservation
   fix, not a delete-commit action). Optional **snapshot-summary properties are supported**
   (`set_snapshot_properties`, §24.2.4 G2 below).
3. **v0.11.0-rc.1 / v0.11.0-rc.2** (git tags, cut after the "Bump version to 0.11.0" commit of
   2026-09-04; MSRV raised to 1.95 per #3083): their own `src/transaction/mod.rs` reads the **same
   append-only method set**. **Conclusion: the imminent 0.11.0 will NOT unblock the sink.**
   The next trigger is a PR merge, not a release.

**Retraction — §24.2.3's "OverwriteAction merged" was wrong.** apache/iceberg-rust **PR #2185**
("feat(transaction): add OverwriteAction with CoW delete support") is **open, unmerged** (GitHub API,
2026-09-19: `merged: False`). The epic #2186's task-list checkbox was inaccurate/optimistic. Lesson
recorded: **an epic checkbox is never merge proof; the PR's own `merged` state is the source of truth.**
The §24.2.3 "upstream-reported, not independently confirmed" caveat was therefore right to keep — and
the independent confirmation now refutes the report.

**RowDelta work exists; none merged.**
`#2203` (RowDelta, the original MoR action), **open** since 2026-03, last active 2026-08-31 (594 insertions, 5 files).
`#2785` (RowDeltaAction: commit data + delete files in one snapshot) and `#2678` (RowDelta deletion
vectors) both **closed unmerged** (2026-09-02) — superseded in favour of the new base work below.
**`#3046` "MergingSnapshotProducer and RewriteFilesAction"** (open, 861 insertions, updated
2026-09-18) — the author states it is *the first in a series* to add compaction/rewrite support
(issue #1607); **follow-ups will add OverwriteFiles, RowDelta, and conflict detection**. This is the
foundation any delete-aware `Transaction` action must sit on, so it is the single most important PR
to watch. Related: `#2752` (core COW rewrite primitive, open) and RFC **`#2620`** "Stateful
Transaction Action" (opened 2026-09-19 — may re-shape the whole action API). MoR *read*-side
groundwork landed for the 0.11.0 window (V3 deletion-vector scan PRs [2/N]/[3/N], `_row_id`
synthesis on read, #3058) — read-side only, not a write/commit capability.

**G2 (DA-SINK-2) flash-verified on the append path — de-risked.**
`prototypes/iceberg-capability` now has its first **real passing probe**
(`tests/probe_append_and_summary.rs`): MemoryCatalog + two `fast_append` commits. Findings:
(a) data files **and** arbitrary `cdc.*` snapshot-summary properties commit **atomically** in one
`Transaction`; (b) they read back from the **catalog-reloaded** table, i.e. they survive the metadata
commit + reload; (c) a second commit re-asserting a monotonic `cdc.owner_epoch` lands on the new
snapshot while the first snapshot keeps its old value. **This is exactly the carrier + monotonic-read
semantics the clustering fence (D-7) requires** — on the one commit path that exists today. The full
DA-SINK-2 gate ("does the carrier survive *delete* commits too") remains deferred to the
delete-commit capability, but the mechanism exists and §24.3's fallbacks are unchanged.

**Other gates touched by 0.10.1:** `update_schema` **confirmed present** (G3's concern narrows to the
drop/rename/type-promotion *extent* — DA-SCHEMA-8 stays 🧪 until probed); `expire_snapshots` exists
(maintenance — positive signal for DA-SINK-10's future built-in path, still external for v1).

**Decision unchanged: option 1 (track iceberg-rust; gate the sink spec on a delete-commit-capable
release).** Revised re-check trigger: watch **#3046 merge on `main`** and then its promised
OverwriteFiles/RowDelta follow-ups appearing on the `Transaction` surface; validate in a **tagged
release** before trusting it (0.11.0-RCs prove a tag can lag a merge, and a merge can lag a release —
both directions of lag observed). The scheduled-recheck cadence stays in effect.

---

### 24.3 DA-SINK-2 — Custom `cdc.*` snapshot-summary properties persist across commits 🧪

**Mechanism.** Two orthogonal sink-side mechanisms live in the Iceberg snapshot summary string map:
`cdc.owner_epoch` (the clustering zombie-fence, D-7) and `cdc.committed_position` (the delivery
guard / per-table durable HWM, D-14.4). On each commit the writer reads the latest snapshot's summary,
checks `latest_committed_epoch`/`committed_position`, and writes the updated values into the new
snapshot's summary. Iceberg snapshot summaries are **per-snapshot**, so the writer must *re-assert*
these on every commit (they are not auto-inherited).

**Exact claim.** iceberg-rust lets the writer (a) read the latest snapshot's summary before committing
and (b) set arbitrary string properties on the snapshot it creates, atomically with the data commit.

**Verification plan.** In `prototypes/iceberg-capability/`:

1. Commit with `cdc.owner_epoch="5"`, `cdc.committed_position="0/1A2B3C4"`; read them back from the
   snapshot summary.
2. Commit again with `owner_epoch="6"`; assert the monotonic read (`latest > mine → abort`) is
   answerable from the summary.
3. Confirm a commit that does **not** set the property does not silently strand the running value —
   i.e. the writer can read the prior summary and carry it forward (since summaries don't inherit).

**Pass/fail.** If custom summary properties are dropped or unreadable across commits, the in-summary
fence/guard has no carrier.

**Fallback.**

1. **Iceberg table properties** instead of the snapshot summary — but confirm the property update
   commits **atomically with the data files** (it must be the same metadata commit to be a real fence,
   not a separate write).
2. **Drop `owner_epoch` to advisory.** The design already states it is **defense-in-depth, not a
   correctness prerequisite** (`Clustering…` §5.5): the metadata-pointer CAS (layer 2) + `_lsn`/PK-upsert
   idempotency (layer 4) preserve correctness without it. Losing the sink-side fence only re-widens the
   rare "zombie commits in the gap before the new owner's first commit" window, which idempotency
   absorbs.
3. **Move `committed_position` to per-table state storage** (the per-pipeline `state` location) instead
   of the snapshot summary. Since `Delivery…` §1 demotes the guard to an *optimization* (idempotency is
   the correctness basis, D-14.3), a state-storage HWM is a fully adequate home; the only loss is reading
   the durable position straight from the table.

**Impact.** 🧪 not 🔴 — correctness is anchored in idempotency, so every fallback degrades a defense
layer or an optimization, never a guarantee.

---

### 24.4 DA-SCHEMA-8 — iceberg-rust supports drop / rename / type-promotion transactions 🧪

**Mechanism.** The safe-evolution set (`Schema_Change…` §7.1) is `add_column` (mature), plus
`drop_column`, `rename_column`, and type-promotion (`int→long`, `float→double`, `decimal` precision
widen). Rename correctness rests on Iceberg's **field-ID stability** (a rename keeps the field ID and
all history).

**Exact claim.** The pinned crate's table-transaction API exposes `drop_column`, `rename_column`, and
`update_column` (promotion), committing them as metadata-only ops with field IDs preserved on rename.

**Verification plan.** In `prototypes/iceberg-capability/`: apply each op to a table; assert (a) rename
preserves the field ID (write under the old name, rename, read history under the new name), and (b)
promotion is metadata-only (no data rewrite). 

**Fallback.** Already the agreed safety branch (`Schema_Change…` §7.1 implementation note): **route any
unsupported op into the "halt table" branch** until the crate catches up. A halted table surfaces an
error + metric; the operator re-snapshots into a fresh table if needed. Rename specifically degrades to
the documented `drop+add` + warning (history NULL under the new name) when `rename_column` is
unavailable.

**Impact.** 🧪 — the fallback is the existing design's fail-safe; no redesign.

---

### 24.5 DA-TYPE-5 — `FixedSizeBinary(16)` → Iceberg `uuid` annotation 🧪

**Mechanism.** iceberg-rust auto-maps most Arrow types, but `FixedSizeBinary(16)` is ambiguous between
`uuid` and `fixed(16)` and needs an explicit field-level hint (`{"iceberg.type":"uuid"}` or
`IcebergType::Uuid` in the schema builder) — `Type_Mapping.md` Hop-3.

**Exact claim.** The pinned crate honors a field-level annotation that forces `FixedSizeBinary(16)` →
Iceberg `uuid`.

**Verification plan.** Build a schema with a uuid column using the hint; assert the resulting Iceberg
field type is `uuid` (not `fixed(16)`); round-trip a value through write + read.

**Fallback.** Store uuid as `fixed(16)` (raw 16 bytes, data fully intact, downstream loses only the
`uuid` logical type) or as canonical `string` (text uuid, human-readable, slightly larger). Both are
lossless of *data*; only the logical type annotation is lost.

**Impact.** 🧪, cosmetic — purely a downstream logical-type nicety.

---

### 24.6 DA-SINK-10 — Compaction / rewrite API availability 🧪 (deferred)

**Mechanism.** Built-in maintenance (delete-file compaction, data-file compaction, expire snapshots,
orphan removal) needs iceberg-rust rewrite APIs. v1 maintenance is **external** (Spark/Trino), so this
gate only affects a *future* built-in scheduler (`sink.maintenance`, reserved & disabled in v1).

**Exact claim.** A future crate version exposes `rewrite_data_files` / `rewrite_position_delete_files` /
`expire_snapshots` equivalents.

**Verification plan.** Periodically re-check the crate's API surface; only build `sink.maintenance` once
available, starting with the MOR-critical delete-file + data compaction (the hybrid first step,
`Iceberg_Maintenance…` §4).

**Fallback.** External maintenance — already the v1 plan. The app's only obligation is to never assume
maintenance has run (planning/writes tolerate deep delete stacks and many small files) and to emit the
when-needed signals (`cdc_iceberg_delete_files`, etc.).

**Impact.** 🧪 and deferred — **no v1 impact.**

---

### 24.7 Consolidated execution plan

All six gates close with **two prototypes**, both pinned to the exact dependency versions:

1. **`prototypes/lease-conformance/`** (runs against real services) — the T1–T10 storage-CAS suite →
   closes **DA-CLUS-1**. **S3 done** (Route A, T1–T9, versioned + non-versioned) **and GCS done**
   (Route B native SDK, T1–T10 incl. the generation-CAS gate), 2026-06. **Only a real Azure run remains.**
2. **`prototypes/iceberg-capability/`** (recommended new) — one integration test binary pinned to
   iceberg-rust 0.9.0, exercising: equality-delete same-commit sequencing + Spark/Trino cross-read
   (**DA-SINK-1**), custom snapshot-summary round-trip (**DA-SINK-2**), drop/rename/promote with
   field-ID preservation (**DA-SCHEMA-8**), and the uuid annotation (**DA-TYPE-5**). DA-SINK-10 is a
   passive API-surface check in the same crate, deferred.

**Recommended sequencing for spec elaboration.** Run both prototypes early. DA-CLUS-1 and DA-SINK-1 are
the only two whose failure changes the architecture (native-SDK lease; position-delete/COW writer), so
they gate the **lease** and **sink-writer** portions of the application spec respectively. The other
four can be written against the assumed capability with the documented fallback noted inline, because
their failure paths (halt, cosmetic, defer, advisory) are already part of the design.

**Config/spec follow-ups owed (propose-only):** add a startup assertion requirement —
*the worker asserts the storage-CAS capabilities (DA-CLUS-1) and the iceberg-rust capability set
(DA-SINK-1/2, DA-SCHEMA-8) at boot and refuses to start if a required capability is absent* — to
`System-Overview.md` (operational preconditions) and reference it from `Configuration_Specification.md`
§11.2. This makes a missing capability a fail-fast at startup rather than a first-write surprise.

---

## 25. Multi-sink re-scope & Delta Lake sink (2026-09-26)

Source: `research/Multi_Sink_Architecture_and_Delta_Lake.md` — all decisions **D-MS.1–D-MS.14 signed off
2026-09-26** (D-MS.4 corrected by Oleksii: `sink.type` is **required, no default**, V-091).

### 25.1 What changed

- **Sink is pluggable per pipeline** (`sink.type: iceberg | delta`, required). One sink per pipeline in
  v1 (D-MS.2); positions kept per (sink, table) so fan-out is additive later.
- **Collapse moves to the core** (D-MS.3): sinks receive a sink-neutral `TableChangeSet`; the old
  Iceberg "sink-writer contract" becomes the Iceberg *realization* of a neutral `Sink` trait contract
  (DA-X-3 ✅ by construction, pending spec merge).
- **Per-sink capabilities + startup assertion**: a config needing a capability the compiled-in sink
  lacks is refused at pipeline start with a named capability. This is what turns DA-SINK-1 from a
  design blocker into a runtime capability.
- **Delta Lake (delta-rs 1.0.0) is a first-class v1 sink** (D-MS.1): one MERGE per table per flush
  (delete + upsert + insert), `txn` app-transaction as the epoch fence carrier, `commitInfo` metadata as
  the position carrier, delete-all TRUNCATE, checkpoints + log cleanup built-in, OPTIMIZE/VACUUM external.

### 25.2 Gap status changes

| ID | Change |
|---|---|
| DA-SINK-1 | 🔴 → re-scoped as **DA-SINK-1i** 🟡: Iceberg **mirror** mode capability-gated on an iceberg-rust delete-commit action; tracking upstream continues (D-MS.10). No longer blocks the system. |
| DA-X-3 | 🔄 → ✅: neutral sink contract + per-sink realizations — merged into `Data-Flow.md` Phase 4 (2026-09-26). |
| DA-X-5 / DA-FLOW-5 | 🟥 → ✅(design): TRUNCATE is a barrier (seal→flush→clear); Delta = delete-all (metadata-only), Iceberg = overwrite-to-empty (with the mirror capability). |
| DA-X-6 / DA-SNAP-8 | 🟥 → 🔄: PK-less tables allowed **only in `write_mode: changelog`** (D-MS.6); changelog reserved in config, implemented after the Delta mirror (D-MS.5). Dump-only + mirror still halts. |
| DA-SINK-2 | scope widened: marker carrier is now a per-sink responsibility (`CommitMarker`). |
| DA-DEP-1 | + **single-Arrow rule** (D-MS.14): arrow 59 workspace-wide; iceberg sink moves to iceberg 0.11 (arrow 59); FFI bridge only as a temporary escape hatch. |

### 25.3 New gaps

| ID | Title | Sev | Status | Note |
|---|---|---|---|---|
| DA-X-10 | **Sink marker disarmed by foreign commits** *(confirmed empirically 2026-09-26, delta probe P3: latest commit after OPTIMIZE carries no marker)* — an external compaction/OPTIMIZE/expire commit carries no `owner_epoch`/`committed_position`; "read the latest snapshot's marker" then returns nothing and the fence silently disarms | 🟡 | ✅(design) | Applies to the Iceberg design as-is. Rule: read the marker from the **latest commit authored by this application** (walk back), or use a carrier persisted in table state (Delta `txn`). Research §8. **Resolved in research 2026-09-26:** Iceberg epoch mirrored to a `cdc.owner_epoch` table property in the same transaction (authoritative; atomicity ✅ probed 2026-09-26, `prototypes/iceberg-capability` G2b), position via own-snapshot walk-back by `cdc.pipeline`, none retained = not covered (`Clustering…` §5.5, `Delivery…` §3–§4, `Iceberg_Maintenance.md` §2). Delta: `txn` + `commitInfo` walk-back (probed). Spec follow-up applied 2026-09-26: `Data-Flow.md` Phase 4 Iceberg item 5 names the table-property carrier. |
| DA-MS-1 | Delta MERGE atomic delete+upsert, idempotent replay (probes P1/P2/P5) | 🔴→✅ | ✅ | **Proven 2026-09-26** on local fs (`prototypes/delta-capability/`): one MERGE = one version; replay identical; truncate metadata-only. Real-cloud atomicity is DA-MS-4. |
| DA-MS-2 | Delta epoch fence via `txn` (conflict on same `app_id`, survives foreign OPTIMIZE) (P3/P4) | 🟡→✅ | ✅ | **Proven 2026-09-26**: stale zombie rejected (`Concurrent transaction failed`), reloaded zombie fenced by pre-check; `txn` survives a foreign OPTIMIZE; commitInfo walk-back recovers the position. |
| DA-MS-3 | Delta MERGE write amplification under random-key churn (P6) | 🟡 | 🔄 | **Measured 2026-09-26**: 1M rows/40 files, 5k updates — sequential keys rewrite 1 file (20k rows); random keys rewrite **all 40 files (995k rows, ≈200×)**. Not a correctness risk; a scope caveat for large tables with spread-out updates. **D-MS.15 decided (b)**: v1 = documented caveat + rows-copied/files-rewritten metrics + flush guidance; post-v1 = deletion-vector writer (own probe). **Measured end to end 2026-11-11** (M12c, `doc/v1.0/v1-acceptance.md` §R-5, §64): a 1M-key, 13 MB table churned at random keys at 1000 changes/s; every 30 s flush rewrote close to the whole table (11–13 MB, about six files) in a 1.2–1.4 s MERGE, on PostgreSQL 18 and MySQL 8.4. The caveat stands as decided. |
| DA-MS-4 | Delta commit atomicity on real S3/GCS/Azure via conditional put (P8) | 🔴→✅ | ✅ | Same capability class as DA-CLUS-1 (proven for the lease). **S3 proven 2026-09-26** (`prototypes/delta-capability/tests/probe_cloud_commit.rs`, us-east-1): delta-rs 1.0 commits with one `If-None-Match: *` PUT (no DynamoDB); raw put-if-absent 160 contenders → 10/10 single winners; same-version MERGE race → exactly one version with the winner's content only; concurrent owners → exactly one lands (fence alone, P8e: `txn` app-id conflict); concurrent foreign appends → all land, none lost. **GCS proven 2026-09-26** (`gs://leasetest`, same probe, `--features gcs`): commit = one `x-goog-if-generation-match: 0` PUT; identical results P8a–e; no 429 surfaced to the caller in 160 racing creates. **Azure Blob proven 2026-09-26** (`dataavalanchetest`, SAS, `--features azure`): commit = one `If-None-Match: *` Put Blob; identical results P8a–e. **Closed on all three clouds.** Residual (not a gap): the Azure account is flat-namespace — ADLS Gen2 (HNS) uses the same Blob endpoint + precondition, re-run to confirm if an HNS account becomes the target. |
| DA-MS-5 | Delta schema evolution gaps (no drop/rename/widen in delta-rs) | 🟢 | ✅(design) | Soft-drop + warn (D-MS.9); widen → halt. |
| DA-MS-7 | delta-rs 1.0 refuses ADD COLUMN on column-mapping (`name`) tables (probe P7b) | 🟡 | ✅ | Would disable the only Delta evolution op. **R-1 decided 2026-09-26**: `sink.delta.column_mapping` default `none`, `name` rejected in v1 (V-092). |
| DA-MS-8 | **`timestamp_ntz` raises the Delta table protocol to reader v3 / writer v7** (probe P10) | 🟡 | ✅ | Any source `timestamp without time zone` column (common in PG/MySQL) makes the Delta table require `timestampNtz` table-feature support; engines without it cannot read the table at all. Without such a column the table stays at reader 1 / writer 2 (P10b). Options: (a) accept + document reader requirements; (b) map `timestamp_ntz` → Delta `timestamp` (UTC-interpreted wall clock; keeps the protocol minimal, loses the no-tz marker); (c) → string; (d) a `sink.delta` option choosing (a)/(b). **Recommendation: (d) with default (a)** — correct semantics by default, an explicit escape hatch for legacy-reader estates. **Decided 2026-09-26 (D-MS.16): (d) with default (a)** — `sink.delta.timestamp_ntz_as: timestamp_ntz | timestamp | string` (config §9.4.3, V-114). |
| DA-MS-9 | Hop-3b type mapping verified end-to-end (probe P10) | 🟢 | ✅ | All types exact through append + MERGE; independent DuckDB (delta-kernel) cross-read agrees; tz vs ntz semantics preserved. |
| DA-DEP-1b | Single arrow major across sinks (P9) | 🟢 | ✅(resolution) | deltalake 1.0 + iceberg `main` resolve to one arrow/parquet 59.3; joint compile not yet run. |
| DA-MS-6 | Spec-merge debt from the multi-sink design | 🟢 | ✅ | **Applied 2026-09-26**: System-Overview (Sinks block, pluggable format), Data-Flow (Phase 0 sink preflight, Phase 3 collapse + `TableChangeSet` + TRUNCATE barrier, Phase 4 sink contract + Iceberg/Delta realizations), Config (`sink.type` required, `sink.iceberg`/`sink.delta`, catalog → `sink.iceberg.catalog`, pipeline-level `schema_evolution`, sink preconditions §11.2, V-083…V-093), Schema §7 (Applied/Degraded/Unsupported, Delta column), Type_Mapping (logical tags, Hop 3b). |

| DA-X-11 | **iceberg-rust `Transaction::commit` rebases a stale zombie instead of rejecting it** *(probe G2b-3, iceberg 0.10.1, 2026-09-26)* — `do_commit` reloads the table and re-applies the actions on **every** attempt (not only on retry), so requirements are derived from the fresh metadata: a zombie holding a stale `Table` lands on top of the new owner's snapshot and regresses `cdc.owner_epoch` 6 → 5. Invalidates `Clustering…` §5.5 layer 2 ("zombie's commit conflicts and fails") and the TOCTOU-free reading of layer 3 | 🟡 | ✅(research) | **Fix, public API only (G2b-4):** commit through a per-commit catalog view whose `load_table` returns the base the fence was checked against → `fast_append`'s `assert-ref-snapshot-id(main)` makes it a real CAS; conflict surfaced non-retryable; the sink owns reload → re-check → re-pin. Proven: stale zombie rejected with neither property nor snapshot applied; foreign append under the pin → conflict → re-pin succeeds; foreign metadata-only edit not clobbered. Residual: a new owner's metadata-only commit doesn't move `main`, so one pinned zombie commit can still land — same window as "before the new owner's first data commit", absorbed by idempotency. `Clustering…` §5.5 / D-7 updated. Spec applied 2026-09-26: `Data-Flow.md` "Sink Commit Failure" names the pin. **Real catalog (probe G2c, AWS Glue + S3, 2026-09-26):** the pinned commit is a CAS — 6 rounds × 8 concurrent pinned writers → exactly 1 winner per round, **all 42 losers stopped by Glue's `VersionId` lock** (`ConcurrentModificationException`; the client-side requirement check alone stopped none), nothing of theirs applied; a stale pinned zombie rejected; the plain-`Transaction::commit` zombie rebase reproduced on Glue (property 603 → 1). S3 Tables uses the same pattern (`versionToken`; source-read, not run). REST / HMS: see DA-CAT-7 / DA-CAT-6. |
| DA-CAT-6 | **iceberg-rust HMS catalog cannot commit** — `iceberg-catalog-hms` `update_table` returns `FeatureUnsupported` (0.10.1 and `main`, source-read 2026-09-26) | 🟡 | ⏭ v2 | **Gated 2026-09-26: config V-115 rejects `catalog.hive`** until a pinned release can commit (probe to lift). Every Iceberg sink commit is an `update_table`, so `sink.iceberg.catalog.type: hive` is unusable on the pinned crate (create/load work; commit does not). Moot for v1 while the Iceberg sink is gated (DA-SINK-1i), but it must join the Iceberg sink's **capability assertion** (per catalog backend). Upstream: #3104 (HMS config refactor) is the only open HMS PR; no commit path in flight. Options: capability-gate `hive` · contribute `update_table` (HMS `alter_table` with an env-context `metadata_location` expectation, as Java's `HiveTableOperations` does — needs a lock or `expected_parameter_key`) · route HMS estates via a REST catalog in front of HMS. **2026-09-26 tidy:** Iceberg-sink only — deferred to v2; V-115/V-116 keep the gate in the config meanwhile. **2026-11-13 (§66):** still `FeatureUnsupported` on 0.11.0 and `main`. |
| DA-CAT-7 | **iceberg-rust REST client has no SigV4** (0.10.1, source-read 2026-09-26) | 🟡 | ⏭ v2 | **Gated 2026-09-26: config V-116 rejects `rest.auth: sigv4`** until a pinned release carries it (probe to lift). `sink.iceberg.catalog.rest.auth: sigv4` (config §9.5.3, `Catalog_Auth_Research.md` §3.2) is not realizable on the pinned crate, so the AWS Glue Iceberg REST and S3 Tables REST endpoints are unreachable through `iceberg-catalog-rest`. Upstream: open PRs **#3082** (SigV4 request signer) and **#3092** (SigV4 auth manager). Meanwhile the native `glue` catalog (probed, G2c) and the native `iceberg-catalog-s3tables` crate cover AWS. Capability-gate `rest`+`sigv4` until a release carries it. REST commit atomicity itself is server-side (409 on failed requirements) — not probed. **2026-09-26 tidy:** Iceberg-sink only — deferred to v2; V-115/V-116 keep the gate in the config meanwhile. **2026-11-13 (§66):** `main` has the signer (#3082); the auth manager #3092 and #2660 are open — still not configurable. |
| DA-CAT-8 | Losing / rejected commits leave orphan files | 🟢 | 🆕 ✅(measured) | G2c: every attempt writes its manifest + manifest list before the catalog decides, and every attempt that passes the client check also writes a `metadata.json`; iceberg-rust does not delete them on failure. 48 racing attempts → 42 orphan `metadata.json` + 86 orphan manifests. Bounded by conflict rate (the pipeline lease makes conflicts rare in practice); reclaimed by external `remove_orphan_files` (`Iceberg_Maintenance.md` §1). No design change. |

### 25.4 Next

1. ~~`prototypes/delta-capability/` — probes P1–P7, P10~~ ✅ 2026-09-26; **P8 ✅ on S3, GCS and Azure** (2026-09-26, DA-MS-4 closed). Every Delta gate is now proven. P10 finding DA-MS-8 decided (D-MS.16: `sink.delta.timestamp_ntz_as`, default `timestamp_ntz`).
2. ~~Spec patches (DA-MS-6)~~ ✅ applied 2026-09-26.
3. Implementation order: core → Delta sink (mirror) → Iceberg sink (changelog; mirror when capable).
   **v1 consequence:** with Iceberg mirror capability-gated (DA-SINK-1i) and changelog deferred
   (D-MS.5), an Iceberg-sink pipeline is refused at start in v1 until either lands.
4. ~~Iceberg probe: `cdc.owner_epoch` property + append atomicity~~ ✅ 2026-09-26 (G2b); surfaced
   DA-X-11 (zombie rebase) — fixed in research; `Data-Flow.md` wording applied.
5. ~~Real-catalog requirement atomicity~~ ✅ 2026-09-26 on **AWS Glue** (G2c, `VersionId` CAS). Surfaced
   DA-CAT-6 (HMS cannot commit) and DA-CAT-7 (REST has no SigV4) from the crate sources — both
   join the Iceberg sink's per-catalog capability assertion. Spec applied 2026-09-26: V-115
   (`catalog.hive` rejected) and V-116 (`rest.auth: sigv4` rejected) — both lifted by a passing probe.


---

## 26. v1 readiness tidy — Iceberg sink deferred to v2 (2026-09-26)

Trigger: the decision to ship **v1 with the Delta sink only** and move the Iceberg sink to v2, plus a
readiness check before the v1 implementation plan. Every open row was re-checked against the current
specs; rows that later spec work had already closed were stale and are closed here, with a pointer.

### 26.1 Stale rows closed (already resolved in the specs)

| ID | Closed by |
|---|---|
| DA-SYS-5, DA-FLOW-2, DA-X-1 | `Data-Flow.md` — `CdcEvent.change_seq`, `_seq` lineage column, Phase-3 keep-last collapse |
| DA-FLOW-4 | `Data-Flow.md` Phase 1 — incremental chunked, stream-first snapshot (DF-1/DF-2) |
| DA-FLOW-5, DA-X-5 | `Data-Flow.md` TRUNCATE barrier; Delta metadata-only delete-all (P5) — **Iceberg part → v2** |
| DA-SNAP-8, DA-X-6 | Config §11.2 identifier precondition applies in every mode (uniform rule; G-30 struck) |
| DA-DEP-2 | Superseded by DA-CLUS-1 (per-backend `LeaseStore`) and the Delta sink's object_store puts (P8) |
| DA-X-8 | Layout decided in `Data-Flow.md`; stays 🔄 for three stale config references (§26.4) |

### 26.2 Deferred to v2 (⏭ v2 — the Iceberg-sink backlog)

DA-SINK-1 (→ DA-SINK-1i, mirror capability), DA-SINK-2 (delete-commit path; append path ✅),
DA-SINK-10, DA-SCHEMA-8, DA-TYPE-5, DA-DEP-1, DA-CAT-4, DA-CAT-6, DA-CAT-7, DA-X-2, DA-X-3, and the
Iceberg halves of DA-FLOW-5 / DA-X-5. Kept as rows (audit trail + v2 starting point), not closed. The
research, the iceberg-capability probes (G1–G2c) and the V-115/V-116 gates carry over unchanged.

### 26.3 New gaps

| ID | Title | Sev | Status | Note |
|---|---|---|---|---|
| DA-SRC-10 | PG replication client crate capability (`pgwire-replication`) | 🔴 | ✅ | Never pinned, compiled or probed. The sink side had three probe rounds; the source side had none. Probe: `prototypes/source-capability/` S-gates. **Proven same day** — S-PG0…8 9/9; decoder is ours. |
| DA-SRC-11 | MySQL binlog client crate capability (`mysql-binlog-connector-rust`) | 🔴 | ✅ | Same. Also decides DA-SRC-8 (MariaDB GTID dialect) in practice. **Proven same day with `mysql_async`** (S-MY0…9 10/10); `mysql-binlog-connector-rust` rejected. |
| DA-SYS-7 | v1 scope statement + v1 handling of `sink.type: iceberg` | 🟢 | ✅ | Recommendation: keep the enum, refuse at start naming v2 (§20). **✅ 2026-09-26** (D-V1.4, §4; this row updated 2026-10-30, §52): the pipeline is refused at start naming v2 (`wiring.rs`). |
| DA-SRC-12 | `mysql_async` silently drops an undecompressable compressed transaction | 🟡 | ✅ | Guard in our source: validate each `TRANSACTION_PAYLOAD`, halt on mismatch. **✅** (D-V1.6; this row updated 2026-10-30, §52): built in M7a (`binlog.rs`, `BINLOG_PAYLOAD_INVALID`), its fault-injection E2E green since M7b (§35). |
| DA-DEP-5 | Two rustls crypto providers → panic at first handshake | 🟢 | 🆕 ✅(rule) | Install a process-wide `CryptoProvider` at startup. |

### 26.4 What stands between here and the v1 implementation plan

1. ~~**Source-side gates** DA-SRC-10/11~~ ✅ 2026-09-26 — PG 9/9, MySQL/MariaDB 10/10
   (`prototypes/source-capability/`). Pins: `pgwire-replication =0.4.1` (+ our pgoutput decoder),
   `mysql_async =0.37.1` (+ MariaDB GTID decode, payload guard DA-SRC-12). Plan-shaping result: the
   **pgoutput decoder** is a v1 work item (bounded; probe version ~150 lines).
2. ~~**v1 decisions** (§20): DA-SRC-8 MariaDB in v1 (now proven feasible — recommend in), DA-SYS-4 reserved-column collision,
   DA-FLOW-6 PG DROP reaction, DA-SYS-7 Iceberg handling, DA-OBS-5 lag target (non-blocking); confirm
   the deferrals DA-CFG-4, DA-CLUS-12, DA-CLUS-13, DA-SINK-8, DA-DEDUP-7.~~ ✅ 2026-09-26 — §26.5.
3. ~~**Spec debt** (needs approval): §19.3 System-Overview S-1…S-6 (never applied — the Open Questions
   block and the Iceberg-only framing are stale) + the DA-X-8 residual in `Configuration_Specification.md`
   (~606, ~960–965, ~1553) + the DA-SYS-7 scope statement + two `Data-Flow.md` Phase 2a source
   details from the probes: `proto_version '2'` → `'1'` (what the pinned crate sends; identical
   messages without `streaming`) and the pinned walsender session options (F-PG-session). Plus the
   five patches the v1 decisions imply (`V1_Scope_Decisions.md` §3).~~ ✅ 2026-09-26 — §26.6.

Not blocking: DA-MS-3 (accepted cost caveat, D-MS.15), DA-SRC-9 (implementation detail), DA-SNAP-7
(retention coupling — designed; the tuning is an operational note), the optional ADLS Gen2 re-run of P8.

### 26.5 v1 decisions signed off (2026-09-26)

All recommendations accepted as proposed. Recorded in `research/V1_Scope_Decisions.md`; the spec
changes they imply are listed there (§3) and are pending approval.

| Decision | Gap | Outcome |
|---|---|---|
| D-V1.1 | DA-SYS-4 / DA-X-4 | Reserved lineage-column (or `__avalanche_*`) collision, compared case-insensitively → the table halts with `RESERVED_COLUMN_COLLISION` (checked at resolution and evolution); no escaping. v2 reopen path: rename *our* columns via a pipeline setting. |
| D-V1.2 | DA-FLOW-6 | PG `DROP TABLE`: relation OID gone from `pg_class` (channel D / A) → the table halts with `TABLE_DROPPED`; the Delta table is never auto-dropped; latency ≤ `rescan_interval`; a re-created table does not auto-resume. Same as the MySQL rule. |
| D-V1.3 | DA-OBS-5 (OQ-OB2) | Lag targets at default config: steady-state p99 ≤ 60s; failover ≤ 2 min. Grounds the ratified Balanced lease and the 30s flush; confirm by v1 load test. |
| D-V1.4 | DA-SYS-7 | `sink.type: iceberg` stays valid in the config; refused at pipeline start: "Iceberg sink ships in v2". Scope statement → System-Overview. |
| D-V1.5 | DA-SRC-8 / DA-X-7 | MariaDB in v1; dialect detected at connect; `SourcePosition` carries either GTID form. |
| D-V1.6 | DA-SRC-12 | Validate every `TRANSACTION_PAYLOAD` (length == `uncompressed_size`, ≥ 1 inner event) → **pipeline** halt `BINLOG_PAYLOAD_INVALID`; report upstream. |
| Deferrals | DA-CFG-4, DA-CLUS-12, DA-CLUS-13, DA-SINK-8, DA-DEDUP-7 | Confirmed out of v1; rows kept as the reopen path. |

**Result:** no 🔴, 🧪 or 🟥 item remains in v1 scope. The only work before the v1 implementation plan is
the spec debt in §26.4 item 3.

### 26.6 Spec debt applied (2026-09-26)

Approved and applied as drafted: the §26.4 item-3 spec debt and the patches implied by §26.5.

| Spec | What changed |
|---|---|
| `System-Overview.md` | v1 scope section (Delta only, Iceberg v2, MariaDB, lag targets, deferrals); S-1 Open Questions → resolved-questions table; S-2 endpoint table (min role + lifecycle semantics); S-3 metric catalogue + cardinality; S-4 external maintenance; S-5 lease/ownership, generation-pointer config, StatefulSet + "restart ≠ reload"; S-6 typos, dangling `snapshot_mode: only` gone; on-demand snapshot no longer "pauses" the stream (DF-2). |
| `Configuration_Specification.md` | §5 reserved-column collision rule + §11.2 start-time row (D-V1.1); `sink.type` v1 note, new §11.2 sink row, and **V-083 lets `iceberg` pass in v1** so one Iceberg pipeline is refused at start instead of rejecting the whole config (§11 is all-or-nothing) — D-V1.4; DA-X-8 residual removed; §15 item 21. |
| `Schema_Change_Detection_and_Evolution.md` | PG `DROP TABLE` in §2.1 and channels A/D; §7.2 rows `DROP TABLE` → `TABLE_DROPPED` and reserved-name ADD/RENAME → `RESERVED_COLUMN_COLLISION` (D-V1.1/2). |
| `Data-Flow.md` | Phase 0 reserved-column check + Iceberg refusal; Phase 2a `proto_version '1'`, session pinning, status timer / idle wakeup, probe-verified protocol note; Phase 2b MariaDB GTID dialect + own `GTID_EVENT` decode, heartbeat, `TRANSACTION_PAYLOAD` check → pipeline halt `BINLOG_PAYLOAD_INVALID` (D-V1.5/6); `gtid.json` covers both dialects. |

Research follow-ups in the same change: `API_RBAC_and_Versioning.md` §2 gains the two pipeline-status
rows; `Observability_Metrics_and_Tracing.md` renames the commit metrics to `cdc_sink_*{sink_type}` and
adds the Delta MERGE metrics (Iceberg MOR metrics marked ⏭ v2). DA-X-8 → ✅.

**Result:** no 🔴 / 🧪 / 🟥 in v1 scope and no spec-merge debt. **The design corpus is ready for the v1
implementation plan.**

### 26.7 Implementation-plan spec edits and product naming (2026-09-26)

The v1 implementation plan (`doc/v1.0/00-roadmap.md`, `M0-foundations.md`) was reviewed. Approved and
applied: roadmap §7 S-A…S-G, the answers to Q-M5-1 / Q-M7-1 / Q-M7-2 / Q-M9-1 (roadmap §9.1) and
D-IMPL-7 (naming).

| Spec | What changed |
|---|---|
| `Configuration_Specification.md` | `sink.flush.seconds` default 60 → **30** (§8.5, §9.4.1, §14; S-A); `source.mysql.replication_type` default `binlog` → **`gtid`**, with `binlog` = file/position mode that halts on a changed source server (S-B, Q-M7-2); §12.1 / §12.4 design-item notes point to Clustering §9 and Pipeline_Lifecycle (S-E); reserved prefix `__data_avalanche_`; metric names. |
| `Data-Flow.md` | Flush default 30s; file/position mode (Phase 0/1 watermark `SHOW BINARY LOG STATUS`, Phase 2b server-identity check → pipeline halt `BINLOG_SOURCE_CHANGED`, `_seq` = file index + `end_log_pos`; S-C, Q-M7-2); `replication/position.json` replaces `gtid.json`; lineage list adds `_seq`/`_pipeline` and Phase 5 states the flush-frontier checkpoint rule (S-F, D-IMPL-2); TOAST carry-forward mechanism (S-G, D-IMPL-3); on-demand snapshot endpoint `POST /v1/pipelines/{id}/snapshots` (S-D) with jobs persisted under `snapshots/jobs/` and listed after restart/failover (Q-M9-1); `ColumnValue` `Timestamp { micros: i64 }` / `Date(i32)` so PG edge values can clamp (Q-M5-1); Delta names below. |
| `System-Overview.md` | `GET /v1/pipelines/{id}/snapshots` (viewer) (Q-M9-1); metric names. |
| `Configuration_Specification.md` §9.3 | A `string` override of a MySQL/MariaDB date/time column carries the raw server text, the remedy for `INVALID_DATE_VALUE` (Q-M7-1). |
| `Schema_Change_Detection_and_Evolution.md` | Reserved prefix `__data_avalanche_*`. |

**Naming (D-IMPL-7), across specs and research docs:** metric prefix `cdc_` → `data_avalanche_`;
Delta `app_id` `data-avalanche/<pipeline_id>/owner`; `commitInfo` keys and the Arrow field tag
`data-avalanche.*`; internal column prefix `__data_avalanche_` (MERGE flag `__data_avalanche_del`).
Earlier dated sections of this tracker keep the names as they were then. `prototypes/` (probe
evidence as run) and the legacy report are unchanged; Iceberg's `cdc.owner_epoch` property is v2.

Research follow-ups: `Type_Mapping.md` names `mysql_async` / `mysql_common` instead of
`mysql-binlog-connector-rust` and gains §Date/time edge values (PG ±infinity clamps to the Arrow
min/max; MySQL/MariaDB all-zero dates → NULL, zero-in-part or impossible dates → table halt
`INVALID_DATE_VALUE`, after measuring that MariaDB 11.8 accepts them by default);
`API_RBAC_and_Versioning.md` gains the snapshot-job list row; `Delivery_Guarantee…` points to
`position.json`.

**Result:** no gap status changes. One question remains open for M9 planning: Q-M9-2 (resolving a
job ID to its pipeline for `GET /v1/snapshots/{job_id}`).

## 27. M5 PostgreSQL source — carried from its whole-branch review (2026-10-02)

M5 (`doc/v1.0/M5-pg-source.md`) built `data-avalanche-source-pg`. Its rulings are the plan's
Decisions table (Q-M5-1…Q-M5-20, D-M5.1…D-M5.19). The whole-branch review's Critical and Important
findings were fixed before the PR (Q-M5-20), except the watermark, which needs M6's merge window
and a spec amendment. A second round fixed what the scoped re-review found (Q-M5-21), and a third its residual (Q-M5-22). These are the
items it carried. They are mirrored in §10 and §14.

### 27.1 New gaps

| ID | Title | Sev | Status | Note |
|---|---|---|---|---|
| DA-SNAP-9 | The snapshot watermark can't order a chunk's MVCC snapshot against the stream | 🟡 | ✅ (§28) | `pg_current_wal_lsn()` watermarks miss a commit whose WAL precedes `lw` but which isn't visible to the chunk yet (synchronous replication widens it): a stale chunk row can overwrite a newer streamed change. Fix in M6: watermarks from `pg_current_snapshot()` in `Chunk`, the window by xid; amend Snapshot_Resumability §1. Reasoned, not measured. |
| DA-SRC-13 | Publication blind spots | 🟢 | ✅ | Unchecked at reconcile: PG15+ row filters and column lists (a column list makes channel C halt at every signal), PG18 `publish_generated_columns`, extra tables under `require_existing` / `FOR ALL TABLES` (D-M5.14 says warn), a managed `DROP TABLE` of a `TABLES IN SCHEMA` member (42704), `via_root` refused when only the leaf is published, the old publication after a rename. **✅ 2026-11-04** (§53; the evidence is in §53's table). |
| DA-SRC-14 | `bit` / `varbit` / PostGIS keys | 🟢 | ✅ | `text_of` isn't valid input for those types, so a snapshot's second chunk fails `Permanent`. Refuse such keys at resolution, or render them. **✅ 2026-11-04** (§53; the evidence is in §53's table). |
| DA-SRC-15 | Snapshot collation and chunk edges | 🟢 | ✅ | A domain with its own collation in a column declared with another (42P22 from the second chunk); the cursor's collation name isn't schema-qualified; a `COLLATE "C"` identity index on a non-C column isn't warned; no byte budget per chunk; `limit` 0 reads as the end; no `E''` literals. **✅ 2026-11-04** (§53; the evidence is in §53's table). |
| DA-SRC-16 | The replication connection's timeouts | 🟢 | ✅ | `pgwire-replication` sets no TCP keep-alive, connect timeout or idle deadline: a black-holed peer stalls `next` until the kernel gives up. M6: a watchdog of a few heartbeats; read-ahead is 8192 events, not bytes (`with_buffer_size`). | **✅ 2026-10-13** (§30, §14; this row updated 2026-10-30, §52): the stream watchdog and the bounded catalog connect (M6b Task 22). |
| DA-SRC-17 | Source edges PostgreSQL doesn't produce, and small gaps | 🟢 | ✅ | Crafted date/time text can overflow `i64`; loose parsing; hstore/PostGIS matched by name; no cycle guard in type planning; `convert` without a scale guard; a `string` override on a `"char"` above 127 halts on PG14 only; `char(n)` under `string` keeps its spaces; an interval's fields narrowed aren't seen; the catalog driver's errors and the server's detail aren't reported (no logging in the crate yet); a retry after dropping an invalidated orphan reports `FirstStart`; only the first V-113 error; a standby isn't refused by name; a glob table set `UNLOGGED` mid-resolve halts; `bounded` takes the connection-cache lock before its timer starts, so a call queued behind a reconnect waits longer than its budget (latency only). **✅ 2026-11-04** (§53): fixed: the table-set errors all reported, the server's detail and hint, a standby refused on every version (decoding on one is later work, DA-SRC-31), the bounded call's one deadline, a date or time past PostgreSQL's range refused (the `civil_days` bound, BC included); the rest are not reachable from a real server (PostgreSQL doesn't produce them): loose parsing, hstore/PostGIS matched by name, cyclic domains in type planning, `convert` without a scale guard, a `string` override on a `"char"` above 127 on PG 14, `char(n)` padding under `string`, an interval's fields narrowed, the dropped-orphan retry reporting `FirstStart` (D-M12a.13); and by design: a glob table set `UNLOGGED` mid-resolve halts (fails closed: PostgreSQL doesn't replicate an unlogged table, `catalog.rs`), and the catalog driver's errors now reach the error text with the server's detail and hint (`db_message`), though the crate logs nothing of its own |
| DA-SRC-18 | M5 test gaps | 🟢 | ✅ | No unit test of the `ChainOnly` / `Unverified` slot verifiers; no `require`-mode integration test; no Delta sink test of a composite or `Date` key end to end; the `postgres-tls` compose service drifts from the `x-pg` anchor (`test-infra/compose.yaml`). **✅ 2026-11-04** (§53; the evidence is in §53's table). |

---

## 28. M6a pipeline worker — DA-SNAP-9 closed (2026-10-03)

M6a (`doc/v1.0/M6a-pipeline-worker.md`, approved 2026-10-03) builds the pipeline worker. Its
rulings are the plan's Decisions (Q-M6a-1…8, D-M6a.1…11). Spec edits S-R…S-U are applied
(`Data-Flow.md` §State Layout, §Sink Commit Failure, Phase 1 step 2, Phase 4 §Position guard;
`Configuration_Specification.md` V-118).

| ID | Change | Evidence |
|---|---|---|
| DA-SNAP-9 | 🟥 → ✅. A chunk is read in a short REPEATABLE READ transaction that records `pg_current_snapshot()`. A table's reads wait for the writers in flight at the session's start (Q-M6a-2). At emission, a chunk row is dropped when a stream transaction the read didn't see already wrote its key, and the chunk is dropped on an unseen TRUNCATE; MySQL (no read visibility) gets the window rule. Chunks commit as `snapshot` sets the guard never skips (Q-M6a-6). `Snapshot_Resumability_and_Consistency.md` §1.1 records the model | Measured on PostgreSQL 18 (M6a Tasks 2, 9, 10, 13): the busy-snapshot test (40,000 rows, four writers) fails without the reconciler's filtering; the stalled-synchronous-commit test fails without it and under the window rule |
| DA-SRC-16 | Stays 🟥. M6a rules the stream watchdog out (D-M6a.9: the spec has none, and a server keep-alive may come only every `wal_sender_timeout / 2`). A black-holed peer's stall is not measured; revisit with the M12 chaos tests | — |

Also settled in M6a, with no gap of their own: the sink's `Transient` retries without limit
(DEGRADED, backpressure; S-S); `timeouts.snapshot` ≥ 10 s (V-118); a table whose schema drifted
while the pipeline was down is rescanned before its snapshot reads (Q-M6a-8); a failed chunk read
on an idle table triggers a rescan (the M5-carried item).

### 28.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SNAP-10 | MySQL's window rule may miss a transaction the binlog delivered before InnoDB made it visible (group commit's sync stage before its commit stage; semi-sync `AFTER_SYNC` holds the gap open for the replica ack) | 🟡 | M6a Task 9 review (the reconciler implements the spec's window rule exactly) | An S-MY probe before M7: stall a semi-sync ack, apply the binlog event, read a chunk issued after it; then rule on the fix. Owner agreed to record it, 2026-10-03 |

### 28.2 New gaps from the M6a whole-branch review (2026-10-07)

M6a was executed 2026-10-03…07: 16 tasks, six post-Task-16 fix batches from the task reviews'
owner rulings, and a whole-branch review whose Critical and Important findings were fixed before
the PR (the plan's "Deviations during execution"). Spec edits: Schema_Change §8.1 (classify first;
an unsafe change flushes before it halts; a change applies at its stream position), Data-Flow
§TOAST (`re_query`'s fallback) and Phase 0 step 4 (a table the sink can't represent halts that
table). These items the review recorded rather than fixed (owner ruling 2026-10-07):

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SNAP-11 | The reconciler's log is unbounded while a snapshotting table's reads wait | 🟢 | Whole-branch review I3 (Task 9 deferral) | Bound it (coalesce per table, or skip logging until a read can be issued — check against the stalled synchronous commit first); M6b exports the reported size as a gauge |
| DA-SNAP-12 | `parallelism > 1` reads chunks one after another on PostgreSQL, and a queued read spends its timeout budget | 🟢 | Whole-branch review M3 | A small read pool, or a connection per concurrent read |
| DA-SNAP-13 | A wiped state with an existing sink table baselines nothing | 🟢 | Whole-branch review M6 (Task 11 deferral) | Baseline against the sink's schema when no cache exists, or document the `SINK_REJECTED` outcome |
| DA-SRC-19 | Which permanent chunk-read errors are table-scoped | 🟢 | Whole-branch review M13, Batch F N-1 | Rule per SQLSTATE in M7/M9 |

Carried to M6b as notes, not gaps: the status's warnings grow and repeat across Phase 0 reruns
and reconnects (presentation and caps belong to the HTTP status); `finish()` doesn't acknowledge the durable
checkpoint and the error path doesn't close the stream (harmless: the next session starts from the
slot document); `until_durable` polls neither control nor the stream (a drain during a long sink
outage inside a schema signal waits for the sink); the Delta sink has no test of a snapshot set
committed at a covered frontier (the slice-1 E2E suite on RustFS should emit a chunk on an idle
source).

---

## 29. M6b single-node control — the planning findings answered (2026-10-08)

M6b's plan (`doc/v1.0/M6b-single-node-control.md`, in review) put six findings to the owner, who
answered them on 2026-10-08 (the plan's Q-M6b-9…13). Spec edits approved and applied:
`Configuration_Specification.md` §9.4.3 and V-119 (`sink.delta.merge_memory_limit`) and the
`System-Overview.md` Observability catalogue (the exposed sample names); with the new tasks,
`Data-Flow.md` Phase 4 (the guard compares a table's last change) and notes in
`Configuration_Specification.md` §6.4, §9.2 and §9.2.1. The plan was approved the same day.

| ID | Change | Evidence |
|---|---|---|
| DA-SRC-16 | 🟥 → 🔄. Q-M6b-11 reverses D-M6a.9: M6b Task 22 builds a stream watchdog. A stream silent for 3 heartbeats while 3 heartbeat reads of the slot in a row fail or find no reader (`active`) ends the session, and the worker reconnects; a read still out counts neither way (it may wait behind a chunk read). The catalog connect is bounded by `timeouts.connect` as a whole. Closes when the task lands. Still open: no TCP keep-alive on the replication socket (it wouldn't catch a frozen server, which answers TCP), and the read-ahead counts events, not bytes | Measured 2026-10-08, PostgreSQL 14 frozen with `docker pause` for 150 s under a running pipeline: status `running`, no reconnect, nothing logged; tokio-postgres's `connect_timeout` bounds only the TCP connect, so a reconnect waits in the startup. With the task (scratch): the E2E scenario reconnects during the freeze, logs the reason, and converges after the unpause |
| DA-SNAP-11 | Stays 🟥, advanced: M6b Task 7 exports the reconciler log's size (`data_avalanche_reconcile_log_entries`, `data_avalanche_reconcile_log_keys`). Bounding the log stays open | M6b plan, Task 7 |

Also answered, with no gap of their own: the Delta libraries log only warnings at a bare `info`,
and the sink no longer provokes the kernel's ERROR on an absent table (Q-M6b-10, Task 23); a stop
and a resume rerun an ERROR pipeline, as a process restart would, until M9's restart verb
(Q-M6b-12, `Pipeline_Lifecycle_State_Machine.md` §3).

### 29.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-X-12 | 🔄 The position guard compared a change set's end, the flush frontier, so a replay after a crash was rarely skipped: keep-alives and other databases' WAL move the frontier past the table's marker. The whole replay was merged again: correct (the MERGE is idempotent, Data-Flow: "optimization, not correctness"), but at full cost | 🟢 | M6b planning, the crash scenarios (marker `0/409EA098` against a replay ending at `0/409EA1B0`) | M6b Task 21: the set carries its table's last change, which the guard compares; the marker stays the frontier, as Data-Flow's checkpoint rule requires. Measured in scratch: the replay was a third Delta commit before, and is skipped after. Closes when the task lands (the Data-Flow Phase 4 patch is applied) |

## 30. M6b single-node control — the exit (2026-10-13)

M6b (`doc/v1.0/M6b-single-node-control.md`) was executed 2026-10-08…13: 24 tasks, review fixes
on Tasks 1, 6, 8, 9, 10, 17, 19, 20 and 22, and a whole-branch review that found no Critical and
one Important issue (CI linted neither the `e2e-hooks` feature nor its crash parser), fixed
before the PR with the cheap Minors (the plan's "Deviations during execution"). The slice-1 E2E
suite (26 scenarios) passes on PostgreSQL 18 and 14 with RustFS.

| ID | Change | Evidence |
|---|---|---|
| DA-X-12 | 🔄 → ✅. The change set carries its table's last change (`PositionRange.last_change`), which the position guard compares; the marker stays the flush frontier. A replay after a crash is skipped. The guard assumes source positions never move backwards; a PostgreSQL failover breaks that assumption, which is tracked as DA-SRC-20 (§30.1) | M6b Task 21; the E2E crash scenario asserts that the replay adds no Delta commit |
| DA-SRC-16 | 🔄 → ✅. The stream watchdog (3 silent heartbeats while 3 heartbeat reads of the slot fail or find no reader) ends the session and the worker reconnects; a paused session doesn't read the stream and is never ended as silent; the catalog connect is bounded by `timeouts.connect`. Not built, by design: a TCP keep-alive on the replication socket (a frozen server still answers TCP), and a byte-sized read-ahead (it counts 8192 events) | M6b Task 22 and its review fix; the E2E scenario freezes PostgreSQL with `docker pause`, and the pipeline reconnects and converges after the unpause |

### 30.1 New gaps from the M6b whole-branch review

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-CLUS-14 | A lease store and a lake that disagree on the epoch (a moved, restored or deleted `globals.state`): a fenced worker re-takes its own lease at the stored epoch + 1 and climbs one worker run per epoch; with two live lease stores on one lake, the climb defeats the fence | 🟢 | Whole-branch review M-1 (the Task 9 "deleted lease" note) | Carry the fence's `current_epoch` into the next takeover, or go to `ERROR` naming the disagreement (M8) |
| DA-API-11 | A desired-state change missed between two polls: a stop and a resume inside one renewal interval don't clear `ERROR` | 🟢 | Whole-branch review M-8 | The M9 API wakes the controller, or the state document carries a generation |
| DA-OPS-7 | A controller task that panics goes unnoticed until shutdown, and readiness stays 200 | 🟢 | Whole-branch review M-7 | Supervise the controller tasks, or fail readiness (M8/M10) |
| DA-SRC-20 | A PostgreSQL source failover moves positions backwards: after a failover to a replica whose WAL ends behind the table's marker, the guard skips the new primary's changes at LSNs the lake already marked (data loss) | 🟡 | Task 21 review M4; owner ruling 2026-10-13 (its own gap, not a DA-X-12 note) | Design the PostgreSQL case beside MySQL's (`Delivery_Guarantee…` §6): detect the timeline or system-identifier change, then halt or re-snapshot |

Carried as notes, not gaps: the cluster crate's public API returns `String` errors (M8 adds callers);
a second signal does nothing during the drain, and a listener that dies at runtime goes unlogged
(M10); the E2E suite lacks two System-Overview table-state cases, its TRUNCATE check would pass
under a rewrite, and DuckDB fetches its Delta extension from the network.

---

## 31. M7 MySQL source — the planning findings (2026-10-14)

M7 planning ran the position probes S-MY10a…k, S-MY11 and S-MY11b (`prototypes/source-capability`,
`tests/probe_mysql_position.rs`, 12/12 on MySQL 8.4 with GTID on and off, MariaDB 10.11, 11.4 and
11.8). Oleksii answered the four questions they raised (Q-M7-3…6), and the spec patches were
applied (`Data-Flow.md`, `Configuration_Specification.md`, 2026-10-14).

| ID | Change | Evidence |
|---|---|---|
| DA-SNAP-10 | 🟥 → 🔄. Measured: the stream delivers a semi-sync AFTER_SYNC commit ~5 ms after it is written, a plain read misses it for the whole stall, and the status position moves past it. Decided (Q-M7-3): MySQL chunk reads take `LOCK IN SHARE MODE` in their READ COMMITTED transaction; the window rule stays. The locking read needs only `SELECT`, waits for any open writer of its rows up to `innodb_lock_wait_timeout` (error 1205 → the chunk is read again), and blocks only writers of its rows. Closes with M7's test | S-MY11, S-MY11b; `Data-Flow.md` Phase 1 step 2b/2c; `Snapshot_Resumability…` §1.1 |

### 31.1 The design points the probes settled (no new gaps)

- **Per-change order (Q-M7-4).** The header `end_log_pos` is 0 inside MariaDB 11.4+ transactions and
  inside MySQL compressed payloads, so `_seq` taken from the header would put a compressed
  transaction's rows before earlier changes, and the keep-last collapse would keep a stale row.
  `_seq` is (file index, computed end offset); `_lsn` is the transaction's GTID, or its start
  `file:pos` in `binlog` mode (`Data-Flow.md` Phase 2b and lineage, roadmap D-IMPL-5).
- **The replica id (Q-M7-5).** A second dump under one replica `server_id` ends the first, so
  `source.mysql.server_id` (derived from the pipeline `id` by default) is unique per source server
  (V-120).
- **The heartbeat (Q-M7-6).** `source.mysql.heartbeat` sets `@master_heartbeat_period` and paces the
  stream watchdog (DA-SRC-16), whose evidence on MySQL is the server listing this pipeline's dump
  (V-121).
- **A purged start position** fails with error 1236, which MySQL also uses for the same-replica-id
  kick: the source tells a purge by a pre-check (`SHOW BINARY LOGS`, `@@gtid_purged`,
  `BINLOG_GTID_POS`), never by the code alone.
- **MariaDB's identity** is `@@server_id` alone (no `server_uuid`; `server_uid` changes on restart),
  and server ids repeat across servers (every probe server has id 1): DA-SRC-21 below.
- **A stalled commit before the start position.** In `binlog` mode and on MariaDB the start position
  can already include a commit not yet visible, which the stream then never delivers. A snapshot's
  locking read waits for it; with `snapshot.mode: never` it is missed, as is everything before the
  start. Not a gap: `never` starts from the server's position by definition.

### 31.2 New gap

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SRC-21 | MariaDB in `binlog` (file/position) mode identifies its server by `@@server_id` alone, and ids repeat across servers: a pipeline pointed at another server with the same id resumes at a file and offset that mean nothing there, and BINLOG_SOURCE_CHANGED (`Data-Flow.md` Phase 2b) doesn't fire | 🟢 | S-MY10a (every probe MariaDB has `server_id` 1; no `server_uuid`; `server_uid` changes on restart) | A stronger check: MariaDB writes GTIDs in every mode, so record the GTID state at the position and compare `BINLOG_GTID_POS(file, pos)` on resume. Decide in M7 or later; GTID mode (the default) doesn't depend on it |

## 32. M7a MySQL source — the plan (2026-10-14)

M7 was split (Q-M7a-1): M7a is `data-avalanche-source-mysql` and its tests, M7b the worker's MySQL
checkpoint, the wiring and the parity E2E. M7a's plan (`doc/v1.0/M7a-mysql-source.md`) was built
task by task in a scratch workspace and replayed from its code blocks; Oleksii answered Q-M7a-1…9
and approved the plan and two spec patches (`Schema_Change…` §7.2: the online-migration cut-over
swap is followed, partition operations that change rows without row events halt; config §9.2.2:
the dump is watched by its connection id).

| ID | Change | Evidence |
|---|---|---|
| DA-SNAP-10 | 🔄 (stays): the locking chunk read is built and tested; ✅ when M7a merges | M7a Task 10 |
| DA-SRC-12 | ✅ (stays): the payload guard is built and unit-tested; its fault-injection E2E is M7b's | M7a Task 6, Task 13 |
| DA-SRC-21 | 🟥 (stays): M7a keeps `@@server_id` as MariaDB's identity, and halts on a checkpoint without one | M7a Task 14 |
| DA-SRC-23 | 🆕 ✅: MariaDB's compressed events, read as nothing by `mysql_common`, are decompressed by the source | M7a Task 6, Task 14 (`compressed_events_decode`) |

### 32.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SRC-22 | MySQL `verify-ca` checks the host name too (`mysql_async` 0.37.1's name-error match misses rustls 0.23's text): it fails closed, so a `verify-ca` connection by an unnamed address is refused | 🟢 | M7a Task 4 (pinned by `verify_ca_by_an_unnamed_address_is_refused_like_verify_full`) | An upstream fix, or our own rustls verifier for `verify-ca`; the test notices when an upgrade lifts it |
| DA-SNAP-14 | An `ENUM` key sorts by member index; a member reorder between the DDL and the stream's signal can make the next chunk skip or repeat rows (the chunk's read plan compares value types only) | 🟢 | M7a Task 10 (reasoned, not measured) | Record the members with the cursor and halt the table when they differ, or key an `ENUM` cursor by label |
| DA-X-13 | One slice-1 E2E run beside a heavy build failed `streaming::a_large_transaction_lands_at_once`: the 50,000-row transaction counted more than one Delta commit, though its rows were never seen in part. Alone and in a full run without the load it passed | 🟢 | M7a planning, on the final scratch tree | Watch CI; if it recurs, find what made the second commit (a marker or position commit beside the transaction's). **✅ 2026-11-06 (§57):** closed with a pointer: not seen again in any E2E run, local or CI, from M7a through M12b |

---

## 33. M7a MySQL source — the exit (2026-10-16)

M7a (`doc/v1.0/M7a-mysql-source.md`) was executed 2026-10-14…16: 16 tasks, review fixes on Tasks
4, 6, 7, 11, 12 and 13, then a whole-branch review with one Critical and five Important findings,
all fixed before the PR, as were its re-review's residuals (the plan's "Deviations during
execution"). The Critical: ENUM and SET labels lost their case in the stream. Two spec patches
(Oleksii, 2026-10-15/16): `binlog_row_metadata = FULL` is required, and the binlog must carry the
source's changes (no binlog filter; a replica logs what it applies). The crate's suites pass on
MySQL 8.4 (GTIDs on and off) and MariaDB 11.8 (both modes), 11.4 and 10.11.

| ID | Change | Evidence |
|---|---|---|
| DA-SNAP-10 | 🔄 → ✅ (2026-10-16, M7a merged, PR #9): the locking chunk read is built and tested | M7a Task 10 |
| DA-SRC-12 | ✅ (stays): the fault-injection E2E is M7b's | M7a Task 6, Task 13 |
| DT-R6 | Resolved: `binlog_row_metadata = FULL` is required, and each table map is checked against the decode schema (names, signedness, character sets, ENUM/SET labels). A positional decode can't fall back to catalog names: a catalog ahead of the stream would read same-typed columns into each other | `Schema_Change…` §2.2, §9, §11; config §11.2; `tests/row_metadata.rs` |

### 33.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SRC-24 | A Galera node with `log_slave_updates` off isn't detected as a replica: its replication status is empty, so the binlog-coverage check passes while the binlog lacks its peers' changes | 🟢 | The whole-branch re-review | Read `wsrep_on` beside the replica status |
| DA-SRC-25 | A case-only column rename reads as a proven rename, which the Delta sink doesn't apply (the table halts or degrades) | 🟢 | The fix wave (column names compare without case) | Update the cached name without an evolution |
| DA-SRC-26 | A MariaDB rollback after tracked rows halts the pipeline: MariaDB logs the undone rows in a transaction with non-transactional changes, and the stream has already emitted them | 🟢 | The fix wave's measurement (10.11, 11.4, 11.8) | Hold a transaction's rows to its end and drop those a savepoint rollback undid |

Carried to M7b in the plan's exit checklist: an error when `server_uuid` reads NULL, a
file/position resume IT, the pipeline dropping a halted table's buffered rows, and `re_snapshot`
at start end to end through the worker.

---

## 34. M7b MySQL pipeline — the plan (2026-10-17)

M7b's plan (`doc/v1.0/M7b-mysql-pipeline.md`) wires the MySQL source into the worker and the
binary and runs the slice-1 E2E suite on MySQL and MariaDB. It was built task by task in a scratch
workspace and replayed from its code blocks to identical trees. Oleksii answered Q-M7b-1…4 and
approved the plan; it needs no spec change (Data-Flow Phase 5 and §State Layout already describe
`replication/position.json` as the position with its server).

Two design points surfaced while planning and are fixed in the plan, not recorded as gaps:

- A reconnect opens the stream without `prepare`, so in file/position mode a failover behind the
  pipeline's address would have streamed another server's binlog from a position that means
  nothing there. The dump now checks the server on its own connection (D-M7b.7).
- A position lost while running was replaced without `prepare`, so the new checkpoint had no
  server. `reset_position` now returns the server with the position (D-M7b.1).

| ID | Change | Evidence |
|---|---|---|
| DA-SRC-12 | ✅ (stays): the fault-injection E2E is planned. A test-only proxy corrupts a compressed transaction with its CRC32 recomputed; measured, `mysql_common` 0.37.3 checks no dump event's checksum, so only the payload guard catches it | M7b Task 7 (`a_corrupted_compressed_transaction_halts_the_pipeline`) |
| DA-SRC-21 | 🟥 (stays): MariaDB's identity is `@@server_id`, and every test-infra server runs `--server-id=1`, so the failover E2E runs on MySQL only (`mysql-84-nogtid` to `mysql-84`) | M7b D-M7b.15 |

### 34.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-OBS-6 | The binary's text log (`compact`, `pretty`) writes ANSI colour sequences to stderr when it isn't a terminal, so a file or a log collector holds escape sequences around the level and the fields | 🟢 | M7b planning: the E2E suite's search for the halt's `code` field missed the log line until the sequences were stripped | Colour only when stderr is a terminal; M10 (`globals.log`) or earlier |
| DA-SRC-27 | One MySQL stream session ended with `Packets out of sync` (`mysql_async`), 3 s after the pipeline started on `mysql-84`; the reconnect recovered, nothing was lost. Cause unknown: the log doesn't name the call. Candidate: a catalog query whose caller is dropped mid-call leaves the shared connection out of sync (`Connector::command` keeps it in its slot) | 🟢 | M7b Task 7's E2E run (`schema::columns_evolve_and_a_truncate_clears_the_table`, once in four runs on `mysql-84`) | Name the failing call in the reconnect log; return the connection to the slot only when a call completes; watch CI |
| DA-X-14 | The E2E helper that reads the lake's rows panics when the lake lacks a column, inside `mirrored`'s wait: a pipeline that lags behind a DDL (a reconnect, a slow flush) fails the scenario instead of being waited for. On either source, since M6b | 🟢 | The same run: after DA-SRC-27's reconnect, `the lake's items has no column extra`; alone on rerun the scenario passed | Treat a missing column as "not yet" in the wait (`harness.rs` `rows`) |

---

## 35. M7b MySQL pipeline — the exit (2026-10-20)

M7b (`doc/v1.0/M7b-mysql-pipeline.md`) was executed 2026-10-17…20: eight tasks, one review fix on
Task 6, then a whole-branch review with no Critical, one Important and five Minor findings. The
Important finding and four of the Minors were fixed before the PR (the plan's "Deviations during
execution"); the fifth Minor was this exit's documents. The slice-1 E2E suite passes on
PostgreSQL 18, MySQL 8.4 (GTIDs on and off) and MariaDB 11.8, and the source's suites on MySQL 8.4
and MariaDB 11.8. No spec change.

| ID | Change | Evidence |
|---|---|---|
| DA-SRC-27 | 🟥 → ✅: a catalog call dropped mid-query left its connection in the shared slot with the reply unread, for the next query to read. The likeliest trigger was a DDL's catalog read inside `MySqlStream::next()`, which the session's `select!` drops when another branch fires. The connection now leaves the slot for the call and goes back only when the call completes; a transient error names the call. That this caused the one logged occurrence is inferred | `connect.rs` `a_call_dropped_mid_query_does_not_poison_the_next` (old code: the next query waited 2.8 s for the abandoned reply); the DDL scenario 5 of 5 alone on `mysql-84` |
| DA-X-14 | 🟥 → ✅: the E2E wait (`mirrored`) treats a lake column it doesn't have yet as "not yet" (`try_rows`); `rows` still panics for a direct assertion | `harness.rs`; the E2E suite on the four targets |
| DA-SRC-12 | ✅ (stays): the fault-injection E2E passes on MySQL 8.4 and MariaDB 11.8 | `mysql.rs` `a_corrupted_compressed_transaction_halts_the_pipeline` |

Decided (Oleksii, 2026-10-20): a server that reports no identity (`@@server_uuid` / `@@server_id`
NULL or empty) gets no halt code, and the spec keeps its two (`BINLOG_PAYLOAD_INVALID`,
`BINLOG_SOURCE_CHANGED`). No real server answers that way: MySQL has set `@@server_uuid` since
5.6 (generated at first start, read-only), and MariaDB's `@@server_id` is always a number. The
error guards only something odd on the wire (a proxy, a fork), and its message names the
variable. GTID mode still requires the identity, though it checks none (a GTID set means the same
on every server). Relaxing that would be code for a case that doesn't occur.

---

## 36. M8a cluster membership — the plan (2026-10-21)

M8's probe gate (`prototypes/cluster-capability`, 2026-10-20) failed `memberlist` 0.8.5 (C13:
reaping corrupts its member table) and passed `foca` 2.0 (F1–F8). Oleksii answered Q-M8-1…4
(2026-10-20): foca with our own actor, packets MAC'd with a key from the anchor, M8 split into
M8a and M8b, the multi-node E2E as three host processes. M8a's plan
(`doc/v1.0/M8a-membership.md`) was built task by task in a scratch workspace and replayed from its
code blocks to an identical tree. Its config patch (§6.5.1 for foca, §6.5.2 and V-054 removed,
V-122) and the System-Overview rows are spec edits, approved separately.

| ID | Change | Evidence |
|---|---|---|
| DA-DEP-4 | ✅ (restated): the gossip crate is `foca` `=2.0.0`, not `memberlist` (it failed the gate). No Lifeguard: SWIM accuracy stays a latency concern (Clustering §3.6, §8.4) | probe C13, F1–F8; roadmap R-1 realized |
| DA-API-5 | ✅ (restated): the cluster transport is authenticated by a keyed BLAKE3 MAC from the anchor's `fingerprint_key`, not TLS; packets aren't encrypted (they carry no data or secrets). Config §6.5.2 and V-054 go | M8a D-M8a.3; Task 3 (`another_anchor_is_another_key`), Task 4 (`a_node_with_another_key_cant_join`) |
| DA-CLUS-8 | ✅ (stays): D-11 as built. A ruling beyond §20.3: a `Draining` node is off the ring at once (Clustering §3.3 kept it until its hand-offs completed) | M8a D-M8a.7; Task 5 |
| DA-CLUS-14 | 🟥 (stays until the exit): planned. The fence's `current_epoch` becomes the claim's floor; one jump, and a second fence after it is an `ERROR` naming another lease store | M8a D-M8a.14; Task 8 |
| DA-OPS-7 | 🟥 (stays until the exit): planned. A controller that ended unasked is logged and started again on its status channel | M8a D-M8a.11; Task 6 (`a_controller_that_panics_is_started_again`) |

### 36.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-SRC-28 | A takeover from a holder that stopped without closing its connection (a stop-the-world pause, a VM freeze, a network black hole to the source) can't stream on PostgreSQL until the old walsender ends at `wal_sender_timeout` (60 s by default), then waits for its next busy-slot retry, up to a 60 s backoff step: about two minutes of lag after the lease's 15 s | 🟢 | M8a planning: the E2E pause scenario (a SIGSTOP past the lease) converged after more than 60 s in two of twelve runs on PostgreSQL | Owner's call (2026-10-21, Q-M8-5): end the stale walsender. Planned as M8a Task 13 (D-M8a.22): the lease holder ends the slot's walsender of its own role, once a busy spell |
| DA-OPS-8 | A node runs the cluster's config generation, resolved with its own environment (D-3), so per-node values written as literals in each node's own file (`node_name`, `advertise_addr`, ports) are ignored once a generation exists: the node takes another node's values, and may exit on a port it doesn't own | 🟢 | M8a planning: a second E2E node with its own file exited "Address already in use" on the first node's API port | Per-node values through `${VAR}` (config §6.5.1, M8a's patch); the operator docs (M11); M8b: warn at startup when the node's file and the adopted generation differ in a per-node key |

Not reported upstream (Oleksii, 2026-10-21: it won't be): the `memberlist-core` 0.8.5 `node_map`
bug (C13, probe README finding 1).

---

## 37. M8a cluster membership — the exit (2026-10-22)

M8a (`doc/v1.0/M8a-membership.md`) was executed 2026-10-21…22: thirteen tasks, review fixes on
Tasks 5, 6, 8, 9, 10, 12 and 13, then a whole-branch review with no Critical and two Important
findings, both fixed before the PR (the plan's "Deviations during execution"). One spec edit,
approved: `join_members` seeds are peers' advertised addresses (config §6.5.1), since foca drops
an announce addressed elsewhere. The E2E suite (37 scenarios, the four cluster scenarios on three
node processes included) passes on PostgreSQL 18 and MySQL 8.4; the other CI targets run on the
PR.

| ID | Change | Evidence |
|---|---|---|
| DA-CLUS-14 | 🟥 → ✅: the claim's floor and one jump; a second fence is an `ERROR` naming another lease store; a stop spends the jump | `claim.rs` `its_own_lease_is_taken_over_at_the_floor`, `the_floor_leaves_another_holders_lease_alone`; `controller.rs` `a_second_fence_past_a_jumped_epoch_is_an_error`, `a_stop_and_resume_after_the_error_jumps_once_more` |
| DA-OPS-7 | 🟥 → ✅ (the supervision half): a controller that ended unasked is started again; the owned gauge survives a panic. Readiness after a panic stays with M10 | `reconciler.rs` `a_controller_that_panics_is_started_again` |
| DA-SRC-28 | 🟥 → ✅: a busy slot's stale walsender of the pipeline's own role is ended under the lease, at most once a lease term (amended twice during execution: "once a busy spell" let the ended side end the other back; "once a worker run" didn't hold across the controller's reruns) | `worker.rs` `a_busy_slot_s_stale_reader_is_ended`, `an_ended_reader_that_comes_back_is_not_ended_again`, `a_worker_without_its_lease_ends_no_reader`; `controller.rs` `a_stale_reader_is_ended_once_a_lease_term`; source-pg IT `the_slot_s_stale_reader_is_ended`; E2E `a_paused_holder_loses_its_pipelines_and_rejoins` |
| DA-CLUS-8 | ✅ (stays): D-11 as built; a ring whose nodes all go is refilled in the same update | `placement.rs` `a_ring_whose_nodes_all_go_is_filled_at_once` |
| DA-OPS-8 | 🟥 (stays): `holder.node_id` still comes from the node's own file, bind and advertise from the adopted generation; M8b | — |

Noted for M11's operator docs: a node's worst-case stop is now bounded, at
`drain_timeout × ceil(pipelines / max_concurrent_handoffs) + 2 × lease_renewal_interval` plus one
`drain_timeout` (130 s with one pipeline at the defaults: 60 s + 2 × 5 s, then 60 s), above Kubernetes' default 30 s
`terminationGracePeriodSeconds`.

## 38. M8b config generations — the plan (2026-10-22)

Oleksii answered Q-M8b-1…3 (2026-10-22, all "Recommended"): the reload and status endpoints now,
unauthenticated until M9; every R field applied in place; a removed pipeline cleaned up by its
owner. M8b's plan (`doc/v1.0/M8b-config-generations.md`) was built task by task in a scratch
workspace and replayed from its code blocks to an identical tree. No spec edit: the specs leave
the endpoints' answers open.

| ID | Change | Evidence |
|---|---|---|
| DA-OPS-8 | 🟥 → 🔄: a node warns at startup naming each per-node key whose value in its file differs from the adopted generation's, which value it runs, and that a reload from it is refused (V-061) until they match. The operator docs stay with M11 | M8b D-M8b.10; Task 7 (`the_per_node_keys_apart_from_the_generation_are_named`) |
| DA-API-10 | ✅ (restated): a reload's restart keeps the desired state, so a paused or stopped pipeline stays so; `ERROR` is cleared, since the new config may be the fix | M8b D-M8b.3; Task 3 (`a_restart_keeps_a_paused_pipeline_paused`, `a_restart_runs_a_pipeline_in_error_again`) |

Found while planning, and in the plan: a pipeline added back while its removal runs would have
run beside its draining controller, whose teardown drops the slot (D-M8b.4); the same text with a
rotated secret would have been answered "unchanged" (D-M8b.7, config §12.1); and a reload's
table patterns would not have reached the source's later rescans (D-M8b.1).

### 38.1 New gaps

| ID | Gap | Severity | Surfaced by | Next |
|---|---|---|---|---|
| DA-OPS-9 | A pipeline removed while no node holds its lease (its owner is down, or a hand-off is between the release and the claim) is torn down by no one: every node's controller is on standby and just ends, so its PostgreSQL slot and publication stay, and the slot keeps WAL | 🟢 | Q-M8b-3; config §12.3's design item | A tombstone for a removed id that the node the ring would place it on cleans up; until then the operator drops the slot (M11's docs) |
| DA-API-12 | `POST /v1/config/reload` and `GET /v1/config/status` answer without authentication until M9. A caller who reaches `globals.api.address` can publish the file that is on the node (not a text of its own) and read each node's generation | 🟢 | Q-M8b-1 | M9: the operator and viewer roles (API_RBAC §2) |
