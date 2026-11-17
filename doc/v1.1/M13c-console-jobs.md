---
updated: 2026-11-17
domain: Architecture
tags: [v1.1, implementation-plan, m13c, console, snapshot-jobs, cancel, retry, api, pipeline, cluster, state, source]
---

# M13c — The Console's Snapshot Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The third server milestone of the 1.1.0 admin console (Admin_Console §14), what the
Snapshots page and a job's page read and do:
- every pipeline's jobs in one list, filtered by status, pipeline and who asked;
- a job's figures: each table's rows against the source's estimate, when its copy began and
  ended, what lake readers see of it, and the job's rate and ETA;
- cancel a job (a table it was bringing back from a halt halts again, `COPY_CANCELLED`) and retry
  an ended one;
- carried in from M13b and M13a: a scheme-relative `//user:pass@host` masked, and the events'
  expiry deleting a day in one batch;
- the site and the E2E suite follow.

**Architecture:**

- **The job's record** (Task 1, `data-avalanche-state`, `-core`, `-cluster`): a job gains its
  cancel (who, role, when, note), the job it retries, the caller's role, and for each table when its
  copy began and ended, the source's estimate of its rows, and the halt the job cleared for it.
  The new statuses are `cancelled` (job and table). A new halt reason, `COPY_CANCELLED`. The owner
  records `job.cancelled`.
- **Row estimates** (Task 2, the sources): `SourceConnector::row_estimates`, from the catalog's
  statistics (PostgreSQL `pg_class.reltuples`, MySQL `information_schema.TABLES.TABLE_ROWS`).
- **A cancel at the start** (Task 3, `data-avalanche-pipeline`): a cancelled job's tables are
  neither started nor resumed by Phase 0. Their in-progress snapshot documents are abandoned, as a
  snapshot the mode no longer asks for is. A table the job had cleared a halt for halts again. The
  start records each table it takes with its time, estimate and cleared halt.
- **The owner follows** (Task 4, `data-avalanche-cluster`): a cancelled job's stopped copy isn't
  taken for a failure; a job a start ended is reported even when a listing came between; a table's
  end is timed.
- **The API** (Tasks 5, 6, `data-avalanche-api`): `POST /v1/snapshots/{job_id}/cancel` and
  `…/retry`; the job's answer gains its figures; `GET /v1/snapshots` reads every pipeline's jobs, 16
  pipelines at a time, the ended jobs once (they don't change).
- **Carried in** (Task 7, `-config`, `-storage`, `-state`): the masker's URL rule takes a `//` that
  starts a value or a word; `VersionedStore::delete_many` (`object_store`'s `delete_stream`, a
  thousand keys a request on S3) for the events' expiry.
- **The site** (Task 8) and **the E2E suite** (Task 9): a job bringing a dropped table back is
  cancelled on a paused pipeline, the table halts `COPY_CANCELLED`, and a retry copies it.

**Tech Stack:** Rust 1.100.0 (edition 2024). No new crate: the API crate takes the workspace's
`futures-util` as a dependency (it was a dev-dependency).

**Spec:**
- `doc/research/Admin_Console.md`: §3 (the Snapshots page and a job's page), §5 (job actions:
  cancel, with "copy cancelled", and retry), §12 (all jobs, rows against estimate, rate, ETA), §13
  (endpoint paths are the builder's), §14.
- The approved mockup `doc/v1.1/mockups/08-snapshots.html` (the list, a job's page, the cancel and
  retry dialogs).
- `doc/specs/System-Overview.md` §Management API endpoints; `doc/specs/Data-Flow.md` §On-Demand
  Snapshot Flow (both amended by S-M13c.1, below).
- `doc/research/Snapshot_Resumability_and_Consistency.md` (cursors, abandoned snapshots),
  `doc/research/API_RBAC_and_Versioning.md` §2 (roles).
- `doc/v1.0/M9b-snapshot-jobs.md` (jobs, Phase 0, the follower), `doc/v1.0/M12b-control-hardening.md`
  (truncating jobs, DA-SNAP-15/16/21), `doc/v1.1/M13a-console-events.md` (the recorder, its
  deviations), `doc/v1.1/M13b-console-nodes.md` (the masker, its deviations).

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`; the sources beside the pipeline. `testkit` is a dev-dependency only.
- **State documents stay readable both ways within 1.1:** a job's new fields are written only when
  set (`skip_serializing_if`), as M12b's `truncate` is. A 1.0 node refuses a job that holds one
  (`deny_unknown_fields`) and skips it with a warning (DA-SNAP-20); 1.0 and 1.1 nodes never run
  together (M13b's gossip v2).
- **A cancel never deletes and never stops the stream:** it ends copies; the lake keeps what they
  wrote, and every configured table that isn't halted streams on.
- **The new halt is the pipeline's:** `COPY_CANCELLED` is recorded for a table the config
  replicates, never for a job's copy of a table outside it.
- **Time in tests:** the pipeline's and the follower's tests run on the fake source and in-memory
  state; the API's tests and the E2E suite run in real time.
- **Test gates:** plain `cargo test` stays hermetic. The sources' integration tests and the
  storage conformance need `DATA_AVALANCHE_IT=1` (`test-infra/up.sh`); the E2E suite needs
  `DATA_AVALANCHE_IT=1` and the `e2e-hooks` feature, and runs with `--test-threads=1`.
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`).
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `thiserror` types.
- Code blocks are the verified content, already `rustfmt`-formatted. Every block is a `git
  apply`-ready diff, one per file; a step's diffs apply in the order given, after the earlier
  steps'. The OpenAPI document and the site's API reference are regenerated, not written (Tasks 5,
  6).
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

The inputs most likely to hurt a person running 1.1.0 in production, each pinned by a test in the
task after the arrow.

1. **A cancel that races the copy's end.** The copy completes just before the start takes the
   cancel: the table ends `completed`, its lake table whole, and isn't halted `COPY_CANCELLED` →
   Task 3 (`a_copy_completed_before_the_cancel_ends_completed`).
2. **The owner reads a stopped copy as a failure.** Between the start abandoning a cancelled
   copy's document (or halting its table) and recording the job, the owner's poll must not end the
   table `failed` → Task 4 (`a_cancelled_job_s_stopped_copy_isn_t_failed`).
3. **A cancel when no worker runs, or one that's paused.** A stopped pipeline, or one whose lease
   no node holds, records the cancel and applies it at its next start, with no restart asked; a
   paused one restarts and stays paused → Task 5 (`a_cancel_restarts_only_a_worker_that_runs`),
   Task 9 (`a_cancelled_copy_halts_its_table_and_a_retry_copies_it`).
4. **A new job for the table between the cancel and the start.** The new job copies the table from
   its first row; the cancelled one neither stops it nor halts it → Task 3
   (`a_job_asked_after_a_cancel_copies_the_table_again`).
5. **A source that can't estimate** (a PostgreSQL table never analyzed, a role that can't read the
   catalog's statistics). The start goes on; the job shows no estimate and no ETA, never a wrong
   one → Task 2 (`row_estimates_are_the_catalog_s_once_a_table_is_analyzed`), Task 3
   (`an_estimate_failure_doesn_t_fail_the_start`), Task 5
   (`a_job_s_pace_is_its_rate_and_the_time_its_estimates_leave`).

## Decisions and rulings

Settled with Oleksii before this plan (2026-11-17): M13c is split, the snapshot jobs first; a
cancel goes through a restart; a cancel with no worker running is recorded and taken at the next
start; a retry follows the mockup. The rest are Claude's calls under Admin_Console §13.

- **D-M13c.1 The milestones of 1.1.0, again.** M13a (merged, #27), M13b (merged, #28), M13c (this
  plan: snapshot jobs), M13d (the owner-served pipeline detail and per-table figures, source
  detail, all tables), M13e (the history sampler and store, the Prometheus reader, the SSE stream),
  M14 (sign-in and sessions), M15 (the frame and Overview, serving `/ui/`), M16 (the detail pages
  and actions). Nothing is tagged until M16 merges. The job page's "effect on the pipeline" (rows a
  second, lag before and during) reads M13e's history.
- **D-M13c.2 A cancel goes through a restart.** `POST /v1/snapshots/{job_id}/cancel` (operator)
  writes `cancel: {by, role, at, note}` into the job, then, where a worker runs (the desired state
  isn't `STOPPED` and a node holds the lease; a paused pipeline included, whose new worker pauses
  again), asks a restart under `<job_id>/cancel`. The start that takes it (Phase 0):
  - leaves the cancelled jobs out of `asked`, `running` and `truncating`;
  - takes as stopping each running table of a cancelled job that no live job asks for or runs, and
    whose snapshot document is `in_progress`. Its document is abandoned (status `none`): a
    configured table streams on, a copy isn't read again;
  - halts a stopping table it had cleared a halt for (`from_halt`), when the config replicates it,
    `COPY_CANCELLED { job }`: the lake holds part of its rows, and its stream position may predate
    them. A copy's halt isn't recorded (the pipeline doesn't replicate it);
  - ends the cancelled jobs' unended tables `cancelled`, but a running one whose document says
    `completed` ends `completed`, with its rows (the copy finished before the cancel);
  - is idempotent across a crash: the halt is recorded before the job, and a halt recorded already
    isn't recorded again.
  A job settles `cancelled` when a table was cancelled; one whose every table completed first
  settles `completed`, its cancel kept.
- **D-M13c.3 A cancel with no worker.** `202` with `"restart": false`; the job reads `cancelling`
  until the pipeline's next start ends it. `200` (`unchanged`) for a job cancelled already; `409`
  for one that ended; `404` for no such job.
- **D-M13c.4 Retry.** `POST /v1/snapshots/{job_id}/retry` (operator), body `{"tables": "all" |
  "not_copied", "truncate": bool, "note": string}`, each optional: `not_copied` (the tables that
  didn't complete) by default, `truncate` the old job's by default. Only an ended job (`409`
  otherwise, and when no table is left to copy). The new job is asked as any job (the same `409`s
  for a pipeline that doesn't run, no lease holder, a table an unfinished job holds), records
  `retry_of`, and the answer names it. Its action's object is the new job; its text names the old
  one.
- **D-M13c.5 A table's timing and estimate.** The start that takes a table records `started_at`,
  the source's `estimate` (`row_estimates` of the started tables, at once; a failure is logged and
  leaves none) and `from_halt` (the code of the halt it cleared). The follower records `ended_at`
  for a table it ends; the start for one it ends. PostgreSQL's estimate is `reltuples` of a table
  or a partitioned table, only once it is `>= 0` (never analyzed is `-1`); MySQL's is
  `TABLE_ROWS`, approximate on InnoDB.
- **D-M13c.6 A job's pace.** `rate`: the rows its tables copied over the time since its first table
  began, in whole rows a second, from 60 s on (`RATE_AFTER`; a shorter run says little), while it
  runs. `eta_seconds`: the rows its unended tables' estimates leave at that rate, rounded up; none
  when an unended table has no estimate. The job's, not each table's: a job's tables are copied one
  after another through one reader.
- **D-M13c.7 What lake readers see** (`lake`, each table): `complete` once copied; `as_before` when
  the job doesn't truncate, or the copy hasn't begun (the table holds what it held); for a
  truncating job's table whose copy began and didn't complete, `empty` before its first rows and
  `partial` after. A table a 1.0 node started records no `started_at`: running or completed counts
  as begun.
- **D-M13c.8 `cancelling`** is the API's name for an unfinished job with a cancel; it isn't
  stored.
- **D-M13c.9 Every pipeline's jobs.** `GET /v1/snapshots` (viewer): every pipeline of the
  generation, 16 at a time (`READ_AT_ONCE`), newest request first; `status` (several,
  comma-separated: `queued`, `running`, `cancelling`, `completed`, `failed`, `cancelled`),
  `pipeline` (`404` when the generation has none of that id), `by`; `400` for another parameter, a
  repeated one or an unknown status. A pipeline whose jobs can't be read is named under
  `unreadable` with its error, and the rest are listed. Each request lists each pipeline's jobs
  once, and reads only the documents it doesn't keep: a node keeps the ended jobs it read, by
  pipeline and key (`EndedJobs`), and drops one a listing no longer holds (pruned).
- **D-M13c.10 The follower.** A listing adds to the jobs followed (it used to replace them), so a
  job a start ended between two listings (a cancel's restart, then another job's) is still
  reported at the next poll. A cancelled job's running table is never failed by the follower: an
  abandoned document or a `COPY_CANCELLED` halt is the cancel's, and the start ends it.
- **D-M13c.11 Recording a job's actions.** A route under `/v1/snapshots/{job_id}` records the job
  as what it acted on and the pipeline its id names; a refused attempt there (`403`) names the job,
  and the pipeline when the generation has it.
- **D-M13c.12 The job page's events** come from the timeline (`GET /v1/events?object=<job id>`:
  who asked, who cancelled, the end) and from the job itself (each table's `started_at`,
  `ended_at`, `rows_done`, the job's `truncate`): M13c adds no per-table events.
- **D-M13c.13 Carried in.** The masker's URL rule (M13b): a `//` after a `:`, or at the start of
  the value or of a word, opens an authority; a `//` inside a path doesn't. Event expiry (M13a):
  `VersionedStore::delete_many(&[String])`, by default one key at a time, on `object_store` one
  `delete_stream` (S3 deletes a thousand keys a request; Azure and GCS as `object_store` does); a
  key already gone is no error, the first failure ends it. `Events::prune` lists a day and deletes
  it in one call. Every node still prunes (each prune is now a listing and a batch a day).

### Spec patch for approval (S-M13c.1)

Approved by Oleksii 2026-11-17 with the plan; applied as execution starts, as its own commit `spec: S-M13c.1 — cancel and retry a snapshot
job; every pipeline's jobs; a job's figures`, once Oleksii approves it and before Task 1. These
edits to two spec files are what the code below implements:

1. `doc/specs/System-Overview.md`, the endpoint table: the job status row says what a job holds,
   and three rows follow the job list (all jobs, cancel, retry).
2. `doc/specs/Data-Flow.md` §On-Demand Snapshot Flow: step 2 says what the start records for
   each table; a paragraph on cancel and retry follows the one on persistence.

`doc/specs/System-Overview.md`:

````diff
diff --git a/doc/specs/System-Overview.md b/doc/specs/System-Overview.md
index 51ac767..511e464 100644
--- a/doc/specs/System-Overview.md
+++ b/doc/specs/System-Overview.md
@@ -137,8 +137,11 @@ Functional endpoints are versioned under `/v1`; probes and metrics are unversion
 | Config nodes | `GET /v1/config/nodes` | viewer | each member's generation, whether its own file is the cluster's generation, and its restart-pending fields |
 | Pipelines status | `GET /v1/pipelines`, `GET /v1/pipelines/{id}` | viewer | `desired_state`, `actual_state`, positions, per-table status (halted tables with their reason); the lease holder flagged `stale` when it isn't a member the node's gossip sees (gone, or an earlier process of a restarted node), `suspect` while gossip suspects it |
 | Request snapshot | `POST /v1/pipelines/{id}/snapshots` | operator | tables list (the per-table config keys), no globs; refused (`409`) unless the desired state is `RUNNING` and a node holds the lease, as a restart (`ERROR` included), or while an unfinished job holds one of the tables; `202` with the job's id. The job runs in a restarted worker. It upserts: a row deleted at the source before the job stays, unless the job is asked with `truncate: true`, which empties each sink table first. A pipeline keeps its 100 newest ended jobs |
-| Snapshot job status | `GET /v1/snapshots/{job_id}` | viewer | |
+| Snapshot job status | `GET /v1/snapshots/{job_id}` | viewer | the job's status (`queued`, `running`, `cancelling`, `completed`, `failed`, `cancelled`), who asked, their role and note, the job it retries, who cancelled it and why; each table's status, rows copied, the source's row estimate, when its copy began and ended, the halt the job cleared for it, and what lake readers see of it (`complete`, `partial`, `empty`, `as_before`); the job's rate and ETA once it has run a minute |
 | List snapshot jobs | `GET /v1/pipelines/{id}/snapshots` | viewer | persisted in the pipeline state location; survive restart and failover |
+| All snapshot jobs | `GET /v1/snapshots` | viewer | every pipeline's jobs, newest request first; filters `status` (several, comma-separated), `pipeline`, `by`; a pipeline whose jobs can't be read is named under `unreadable` |
+| Cancel snapshot job | `POST /v1/snapshots/{job_id}/cancel` | operator | records who cancelled the job and why, then restarts the pipeline's worker where one runs (the desired state isn't `STOPPED` and a node holds the lease): that start ends the job's unended tables `cancelled`, and a table the job was bringing back from a halt halts again (`COPY_CANCELLED`); without a worker, the next start does. `202` `cancelling`; `200` when cancelled already; `409` once the job ended |
+| Retry snapshot job | `POST /v1/snapshots/{job_id}/retry` | operator | a new job of an ended job's tables (`tables`: `not_copied`, the default, or `all`), truncating as the old one did unless `truncate` says; it records the job it retries, and is otherwise asked as `Request snapshot` is. `409` while the job hasn't ended, or when no table is left to copy |
 | Events | `GET /v1/events` | viewer | the cluster's timeline (actions with their caller, role, note and outcome; refused attempts; the cluster's own events), newest first; filters `since`, `until`, `kind` (a kind or a family), `object`, `actor`, `node`, `q`; `limit` 1–1000 (100); a page that stopped early names its `next`, passed back as `after`. Kept for `globals.console.events.retention` |
 | Pipeline pause | `POST /v1/pipelines/{id}/pause` | operator | `RUNNING → PAUSED`: flush to a transaction boundary and commit; source connection kept warm; lease kept |
 | Pipeline stop | `POST /v1/pipelines/{id}/stop` | operator | `RUNNING/PAUSED → STOPPED`: flush, close the source connection (PG slot kept); lease kept |
````

`doc/specs/Data-Flow.md`:

````diff
diff --git a/doc/specs/Data-Flow.md b/doc/specs/Data-Flow.md
index 470bb23..edf6d8a 100644
--- a/doc/specs/Data-Flow.md
+++ b/doc/specs/Data-Flow.md
@@ -764,7 +764,10 @@ Request payload: { tables: ["schema.orders", "schema.products"], truncate: false
    schema (Schema_Change_Detection_and_Evolution.md §8.2). The stream resumes from the confirmed
    position, as after any restart. For each table: run the Phase-1 chunk loop (watermark lw/hw +
    keyset chunks) WITHOUT pausing the stream. Tables already in the streaming set keep streaming;
-   the snapshot reconciles against the live stream exactly as at startup. A `truncate` job first
+   the snapshot reconciles against the live stream exactly as at startup. The start records, for
+   each table it takes, when the copy began, the source's estimate of its rows (PostgreSQL
+   `pg_class.reltuples`, MySQL `information_schema.TABLES.TABLE_ROWS`; none when the source has
+   none or can't say, which doesn't fail the start) and the halt it cleared. A `truncate` job first
    empties each of its tables' sink table: a snapshot change set with `truncate` and no rows,
    committed (fenced) before the start records the table running, so a crash in between empties
    it again and a restart after it resumes the copy without emptying it.
@@ -782,6 +785,18 @@ Jobs are persisted in the per-pipeline state location, so they are **listed afte
 failover**. A job that was running continues on the (new) owner from the persisted per-table chunk
 cursors, like the Phase-1 snapshot does after a crash.
 
+**Cancel and retry.** `POST /v1/snapshots/{job_id}/cancel` records the cancel in the job (who,
+their role, when, why) and asks the pipeline's worker to restart, as a job does; the job reads
+`cancelling` until a start takes the cancel. That start's Phase 0 neither starts nor resumes the
+job's tables. A queued table ends `cancelled`. A running table's snapshot state is abandoned, as a
+snapshot its mode no longer asks for is: the table streams on, and ends `cancelled`. A table the
+job's start had brought back from a halt halts again, `COPY_CANCELLED`, naming the job: its sink
+table holds part of its rows, and its stream position may predate them, so copying it again takes
+a new job. A table whose copy completed before the start took the cancel ends `completed`. With
+no worker running (a stopped pipeline, or one whose lease no node holds), the next start takes the
+cancel. `POST /v1/snapshots/{job_id}/retry` asks a new job of an ended job's tables, those that
+didn't complete unless it asks for all, and records the job it retries.
+
 There is **no stream pause and no `pause_lsn + 1` assumption**: watermarks reconcile re-snapshotted
 rows against concurrent changes, and idempotent upserts mean re-reading existing keys re-writes the
 same rows. A job without `truncate` never empties the target: a row deleted at the source while the
````

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-state/src/{snapshot,lib}.rs` | A job's cancel, retry, role and per-table timing; `cancelled`; `snapshot_jobs_reusing` (Tasks 1, 6) |
| `crates/data-avalanche-core/src/{schema,source}.rs` | `COPY_CANCELLED`; `SourceConnector::row_estimates` (Tasks 1, 2) |
| `crates/data-avalanche-source-pg/src/connector.rs`, `tests/pg/snapshot.rs` | PostgreSQL's estimates (Task 2) |
| `crates/data-avalanche-source-mysql/src/{catalog,connector}.rs`, `tests/mysql/snapshot.rs` | MySQL's estimates (Task 2) |
| `crates/data-avalanche-testkit/src/fake_source.rs` | The fake source's estimates and their fault (Task 2) |
| `crates/data-avalanche-pipeline/src/{jobs,worker}.rs` | The cancel at the start; `Taken`, the start's record (Task 3) |
| `crates/data-avalanche-cluster/src/{jobs,controller}.rs` | The follower; `job.cancelled` (Tasks 1, 4) |
| `crates/data-avalanche-api/src/{snapshots,actions,lib,openapi,test_support}.rs`, `Cargo.toml`, `openapi.json` | Cancel, retry, all jobs, a job's figures; recording a job's actions (Tasks 1, 5, 6) |
| `crates/data-avalanche/src/run.rs` | The node's `EndedJobs` (Task 6) |
| `crates/data-avalanche-config/src/mask.rs` | Scheme-relative URLs (Task 7) |
| `crates/data-avalanche-storage/src/store.rs`, `tests/versioned_store.rs`; `crates/data-avalanche-state/src/events.rs` | `delete_many`; the expiry's batches (Task 7) |
| `docs/src/content/docs/{runbooks/snapshot-jobs,runbooks/halted-table,deployment/events,reference/api}.md`, `docs/public/openapi.json` | The pages (Tasks 5, 6, 8) |
| `crates/data-avalanche/tests/e2e/jobs.rs` | The cancel and retry scenario (Task 9) |

---

### Task 1: A job's cancel, retry and timing; `COPY_CANCELLED` (`data-avalanche-state`, `-core`, `-cluster`, `-api`)

**Files:**
- Modify: `crates/data-avalanche-api/src/snapshots.rs`, `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-core/src/schema.rs`, `crates/data-avalanche-state/src/lib.rs`, `crates/data-avalanche-state/src/snapshot.rs`

**Interfaces:**
- Consumes: `SnapshotJobDoc`, `JobTable`, `JobStatus`, `JobTableStatus`, `settle` (M9b); `HaltReason`
  (M4); the controller's `job_event` (M13a).
- Produces (state crate root): `JobStatus::Cancelled`, `JobTableStatus::Cancelled`,
  `JobTableStatus::is_ended(self) -> bool`; `JobTable::{started_at, ended_at}:
  Option<DateTime<Utc>>`, `JobTable::estimate: Option<u64>`, `JobTable::from_halt:
  Option<String>`, `JobTable::queued(TableRef) -> Self`, `JobTable::end(&mut self,
  JobTableStatus, Option<String>, DateTime<Utc>)`; `JobCancel { by, role, at, note }`;
  `SnapshotJobDoc::{requested_role, retry_of}: Option<String>`, `SnapshotJobDoc::cancel:
  Option<JobCancel>`. `settle` ends a job `Cancelled` when a table was. Core:
  `HaltReason::CopyCancelled { job: String }`, code `COPY_CANCELLED`. The controller records
  `job.cancelled` (warning); a job's short id is its suffix's first 8 characters.

- [ ] **Step 1: Write the failing tests**

The tests: `a_cancelled_table_cancels_the_job`, `the_fields_1_1_added_are_written_only_when_set` (state); the halt codes' list and JSON (core); `job_event`'s cancelled case and short id (cluster).

`crates/data-avalanche-cluster/src/controller.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 82c7c21..142d8d4 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1834,7 +1858,7 @@ mod tests {
     use data_avalanche_state::Action;
     use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
 
-    use data_avalanche_state::{JobStatus, JobTableStatus, SnapshotJobDoc};
+    use data_avalanche_state::{JobCancel, JobStatus, JobTableStatus, SnapshotJobDoc};
 
     use super::*;
     use crate::test_support::{
@@ -2483,7 +2507,7 @@ mod tests {
     #[test]
     fn a_job_is_recorded_only_once_it_ended() {
         let mut job = SnapshotJobDoc::new(
-            "0123456789".into(),
+            "orders~0123456789".into(),
             vec![t("a"), t("b")],
             "ops".into(),
             chrono::Utc::now(),
@@ -2511,6 +2535,24 @@ mod tests {
                 "snapshot job 01234567 of orders failed: public.b failed".into()
             ))
         );
+        job.status = JobStatus::Cancelled;
+        job.tables[0].status = JobTableStatus::Completed;
+        job.tables[1].status = JobTableStatus::Cancelled;
+        job.cancel = Some(JobCancel {
+            by: "Oleksii".into(),
+            role: "operator".into(),
+            at: chrono::Utc::now(),
+            note: None,
+        });
+        assert_eq!(
+            job_event("orders", &job),
+            Some((
+                "job.cancelled",
+                EventSeverity::Warning,
+                "snapshot job 01234567 of orders cancelled by Oleksii (operator): 1 of 2 tables copied"
+                    .into()
+            ))
+        );
     }
 
     async fn ask_job(docs: &PipelineDocs, id: &str, tables: &[&str]) {
````

`crates/data-avalanche-core/src/schema.rs`:

````diff
diff --git a/crates/data-avalanche-core/src/schema.rs b/crates/data-avalanche-core/src/schema.rs
index b3a63d7..e10dd86 100644
--- a/crates/data-avalanche-core/src/schema.rs
+++ b/crates/data-avalanche-core/src/schema.rs
@@ -508,6 +514,9 @@ mod tests {
             HaltReason::NotReplicable { reason: "r".into() },
             HaltReason::SinkRejected { detail: "d".into() },
             HaltReason::SnapshotTimeout { limit_secs: 21_600 },
+            HaltReason::CopyCancelled {
+                job: "orders~0199".into(),
+            },
         ]
     }
 
@@ -546,6 +555,17 @@ mod tests {
             serde_json::to_string(&HaltReason::TableDropped).unwrap(),
             r#"{"code":"TABLE_DROPPED"}"#
         );
+        let cancelled = HaltReason::CopyCancelled {
+            job: "orders~0199".into(),
+        };
+        assert_eq!(
+            serde_json::to_string(&cancelled).unwrap(),
+            r#"{"code":"COPY_CANCELLED","job":"orders~0199"}"#
+        );
+        assert_eq!(
+            cancelled.to_string(),
+            "snapshot job orders~0199 was cancelled before it copied the table; copy it again"
+        );
         let unknown =
             serde_json::from_str::<HaltReason>(r#"{"code":"UNSAFE_DDL","detail":"x","y":1}"#);
         // A unit variant's extra fields go unchecked: serde's internally tagged enums don't
````

`crates/data-avalanche-state/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 38e7b6a..f8f9f81 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -70,7 +70,7 @@ pub use pipeline::{
 };
 pub use removal::{REMOVALS_PREFIX, RemovalDoc, Removals, removal_key};
 pub use snapshot::{
-    CursorKey, JOBS_PREFIX, JobStatus, JobTable, JobTableStatus, KEPT_ENDED_JOBS, SnapshotCursor,
-    SnapshotJobDoc, SnapshotStatus, TableSnapshotDoc, job_key, table_snapshot_key,
+    CursorKey, JOBS_PREFIX, JobCancel, JobStatus, JobTable, JobTableStatus, KEPT_ENDED_JOBS,
+    SnapshotCursor, SnapshotJobDoc, SnapshotStatus, TableSnapshotDoc, job_key, table_snapshot_key,
 };
 pub use tables::{HALTS_KEY, HaltsDoc, SCHEMAS_PREFIX, TableHalt, table_schema_key};
````

`crates/data-avalanche-state/src/snapshot.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index a56cf1e..6258d43 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -731,6 +803,107 @@ mod tests {
         assert_eq!(doc.status, JobStatus::Queued);
     }
 
+    /// A job whose table was cancelled ends cancelled once no table is queued or running,
+    /// whatever its other tables did; a job whose tables all completed before its cancel was
+    /// taken completed.
+    #[test]
+    fn a_cancelled_table_cancels_the_job() {
+        let mut doc = two_tables();
+        doc.cancel = Some(JobCancel {
+            by: "Oleksii".into(),
+            role: "operator".into(),
+            at: t(1),
+            note: None,
+        });
+        doc.tables[0].status = JobTableStatus::Running;
+        assert!(doc.settle(t(1)));
+        assert_eq!(doc.status, JobStatus::Running, "asked, not yet taken");
+        doc.tables[0].end(JobTableStatus::Completed, None, t(2));
+        doc.tables[1].end(JobTableStatus::Cancelled, None, t(2));
+        assert!(doc.settle(t(2)));
+        assert_eq!(doc.status, JobStatus::Cancelled);
+        assert_eq!((doc.finished_at, doc.error.as_deref()), (Some(t(2)), None));
+        assert_eq!(doc.tables[1].ended_at, Some(t(2)));
+        assert!(doc.is_finished());
+
+        let mut doc = two_tables();
+        doc.tables[0].end(JobTableStatus::Failed, Some("TABLE_DROPPED".into()), t(3));
+        doc.tables[1].end(JobTableStatus::Cancelled, None, t(3));
+        assert!(doc.settle(t(3)));
+        assert_eq!(
+            doc.status,
+            JobStatus::Cancelled,
+            "a cancel ends it, a failure or not"
+        );
+        assert_eq!(
+            doc.error.as_deref(),
+            Some("1 of 2 tables failed: public.orders (TABLE_DROPPED)")
+        );
+
+        let mut doc = two_tables();
+        doc.cancel = Some(JobCancel {
+            by: "Oleksii".into(),
+            role: "operator".into(),
+            at: t(4),
+            note: Some("too late".into()),
+        });
+        for table in &mut doc.tables {
+            table.end(JobTableStatus::Completed, None, t(4));
+        }
+        assert!(doc.settle(t(4)));
+        assert_eq!(
+            doc.status,
+            JobStatus::Completed,
+            "the cancel came after the copies"
+        );
+    }
+
+    /// The fields 1.1 added are written only when set, so a job asked without them reads as a
+    /// 1.0 job did; set, they read back.
+    #[test]
+    fn the_fields_1_1_added_are_written_only_when_set() {
+        let doc = two_tables();
+        let text = String::from_utf8(crate::doc::encode(&doc).to_vec()).unwrap();
+        for key in [
+            "started_at",
+            "ended_at",
+            "estimate",
+            "from_halt",
+            "requested_role",
+            "retry_of",
+            "cancel",
+        ] {
+            assert!(!text.contains(key), "{key} in {text}");
+        }
+        let mut doc = two_tables();
+        doc.requested_role = Some("operator".into());
+        doc.retry_of = Some("orders~0198".into());
+        doc.cancel = Some(JobCancel {
+            by: "ci-bot".into(),
+            role: "operator".into(),
+            at: t(2),
+            note: Some("maintenance".into()),
+        });
+        doc.tables[0].started_at = Some(t(1));
+        doc.tables[0].estimate = Some(4_200_000);
+        doc.tables[0].from_halt = Some("UNSAFE_DDL".into());
+        doc.tables[0].end(JobTableStatus::Cancelled, None, t(2));
+        let text = String::from_utf8(crate::doc::encode(&doc).to_vec()).unwrap();
+        let read: SnapshotJobDoc = crate::doc::decode("j", text.as_bytes()).unwrap();
+        assert_eq!(read, doc);
+        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
+        assert_eq!(
+            json["cancel"],
+            json!({
+                "by": "ci-bot", "role": "operator", "at": "2026-09-28T10:00:02Z",
+                "note": "maintenance",
+            })
+        );
+        assert_eq!(json["tables"][0]["status"], "cancelled");
+        assert_eq!(json["tables"][0]["from_halt"], "UNSAFE_DDL");
+        assert_eq!(json["tables"][0]["estimate"], 4_200_000);
+    }
+
     #[tokio::test]
     async fn the_unfinished_jobs_leave_out_the_ended_ones() {
         let p = pipeline();
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-state -p data-avalanche-core -p data-avalanche-cluster`
Expected: the build fails: no variant `Cancelled` in `JobTableStatus`, no `JobCancel`, no variant `CopyCancelled` in `HaltReason`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/snapshots.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index 1d87fd9..cc93f27 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -107,12 +107,14 @@ fn job_json(p: &Pipeline, j: &SnapshotJobDoc) -> Value {
         JobStatus::Running => "running",
         JobStatus::Completed => "completed",
         JobStatus::Failed => "failed",
+        JobStatus::Cancelled => "cancelled",
     };
     let table_status = |s: JobTableStatus| match s {
         JobTableStatus::Queued => "queued",
         JobTableStatus::Running => "running",
         JobTableStatus::Completed => "completed",
         JobTableStatus::Failed => "failed",
+        JobTableStatus::Cancelled => "cancelled",
     };
     json!({
         "job_id": j.job_id,
````

`crates/data-avalanche-cluster/src/controller.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 82c7c21..142d8d4 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -21,8 +21,8 @@ use data_avalanche_pipeline::{
     WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
 };
 use data_avalanche_state::{
-    ActualState, DesiredState, EventSeverity, Holder, JobStatus, Leases, PipelineDocs, RemovalDoc,
-    Removals, RequestsDoc, SnapshotJobDoc, StatusDoc,
+    ActualState, DesiredState, EventSeverity, Holder, JobStatus, JobTableStatus, Leases,
+    PipelineDocs, RemovalDoc, Removals, RequestsDoc, SnapshotJobDoc, StatusDoc,
 };
 use data_avalanche_telemetry::{
     Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, RowsRead, SnapshotState, Totals,
@@ -1803,9 +1803,14 @@ impl PipelineController {
 
 /// The event of a snapshot job of pipeline `name` by its status: its kind, severity and text once
 /// it ended; nothing while it is queued or running. Every status is named, so a new one is
-/// decided here, not taken for an ending.
+/// decided here, not taken for an ending. The job is named as the console shows it: the first 8
+/// characters of its id's suffix (the uuid after `<pipeline>~`).
 fn job_event(name: &str, job: &SnapshotJobDoc) -> Option<(&'static str, EventSeverity, String)> {
-    let short: String = job.job_id.chars().take(8).collect();
+    let suffix = job
+        .job_id
+        .split_once('~')
+        .map_or(job.job_id.as_str(), |(_, s)| s);
+    let short: String = suffix.chars().take(8).collect();
     match job.status {
         JobStatus::Queued | JobStatus::Running => None,
         JobStatus::Completed => Some((
@@ -1824,6 +1829,25 @@ fn job_event(name: &str, job: &SnapshotJobDoc) -> Option<(&'static str, EventSev
                 job.error.as_deref().unwrap_or("a table failed")
             ),
         )),
+        JobStatus::Cancelled => {
+            let copied = job
+                .tables
+                .iter()
+                .filter(|t| t.status == JobTableStatus::Completed)
+                .count();
+            let by = job
+                .cancel
+                .as_ref()
+                .map_or(String::new(), |c| format!(" by {} ({})", c.by, c.role));
+            Some((
+                "job.cancelled",
+                EventSeverity::Warning,
+                format!(
+                    "snapshot job {short} of {name} cancelled{by}: {copied} of {} tables copied",
+                    job.tables.len()
+                ),
+            ))
+        }
     }
 }
 
````

`crates/data-avalanche-core/src/schema.rs`:

````diff
diff --git a/crates/data-avalanche-core/src/schema.rs b/crates/data-avalanche-core/src/schema.rs
index b3a63d7..e10dd86 100644
--- a/crates/data-avalanche-core/src/schema.rs
+++ b/crates/data-avalanche-core/src/schema.rs
@@ -147,6 +147,11 @@ pub enum HaltReason {
     )]
     #[serde(rename = "SNAPSHOT_TIMEOUT")]
     SnapshotTimeout { limit_secs: u64 },
+    /// A snapshot job bringing the table back from a halt was cancelled before its copy ended
+    /// (Admin_Console §5): the sink holds a partial copy, so the table stays halted.
+    #[error("snapshot job {job} was cancelled before it copied the table; copy it again")]
+    #[serde(rename = "COPY_CANCELLED")]
+    CopyCancelled { job: String },
 }
 
 impl HaltReason {
@@ -167,6 +172,7 @@ impl HaltReason {
             Self::NotReplicable { .. } => "TABLE_NOT_REPLICABLE",
             Self::SinkRejected { .. } => "SINK_REJECTED",
             Self::SnapshotTimeout { .. } => "SNAPSHOT_TIMEOUT",
+            Self::CopyCancelled { .. } => "COPY_CANCELLED",
         }
     }
 }
````

`crates/data-avalanche-state/src/snapshot.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index a56cf1e..6258d43 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -74,6 +74,8 @@ pub enum JobStatus {
     Completed,
     /// Every table ended, and one or more failed.
     Failed,
+    /// Every table ended, and one or more was cancelled ([`SnapshotJobDoc::cancel`]).
+    Cancelled,
 }
 
 /// Where one requested table of a job is.
@@ -86,6 +88,18 @@ pub enum JobTableStatus {
     Running,
     Completed,
     Failed,
+    /// The job was cancelled before the table's copy ended (or began).
+    Cancelled,
+}
+
+impl JobTableStatus {
+    /// Completed, failed or cancelled: the table's part of the job is over.
+    pub fn is_ended(self) -> bool {
+        match self {
+            Self::Queued | Self::Running => false,
+            Self::Completed | Self::Failed | Self::Cancelled => true,
+        }
+    }
 }
 
 #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
@@ -96,6 +110,55 @@ pub struct JobTable {
     pub rows_done: u64,
     /// Why it failed.
     pub error: Option<String>,
+    /// When a start took the table and its copy began. The fields below are written only when
+    /// set, as the job's `truncate`.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub started_at: Option<DateTime<Utc>>,
+    /// When it completed, failed or was cancelled.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub ended_at: Option<DateTime<Utc>>,
+    /// The source's estimate of the table's rows when its copy began, for a progress and an ETA.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub estimate: Option<u64>,
+    /// The code of the halt the job's start cleared: the job was bringing the table back from it.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub from_halt: Option<String>,
+}
+
+impl JobTable {
+    /// A table just asked for: queued, nothing done.
+    pub fn queued(table: TableRef) -> Self {
+        Self {
+            table,
+            status: JobTableStatus::Queued,
+            rows_done: 0,
+            error: None,
+            started_at: None,
+            ended_at: None,
+            estimate: None,
+            from_halt: None,
+        }
+    }
+
+    /// Ends the table's part of the job as `status`, at `now`, with `error` when it failed.
+    pub fn end(&mut self, status: JobTableStatus, error: Option<String>, now: DateTime<Utc>) {
+        debug_assert!(status.is_ended(), "{status:?} doesn't end a table");
+        self.status = status;
+        self.error = error;
+        self.ended_at = Some(now);
+    }
+}
+
+/// Who cancelled a job, when and why (Admin_Console §5). The start that takes it ends the job's
+/// tables that haven't ended (D-M13c.2).
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct JobCancel {
+    pub by: String,
+    pub role: String,
+    pub at: DateTime<Utc>,
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub note: Option<String>,
 }
 
 /// An on-demand snapshot job (Data-Flow §On-Demand Snapshot); survives restart and failover.
@@ -117,6 +180,15 @@ pub struct SnapshotJobDoc {
     /// Why it was asked (Admin_Console §5). Written only when set, as `truncate`.
     #[serde(default, skip_serializing_if = "Option::is_none")]
     pub note: Option<String>,
+    /// The role of the caller who asked for it. Written only when set, as `truncate`.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub requested_role: Option<String>,
+    /// The ended job this one retries (`POST /v1/snapshots/{job_id}/retry`).
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub retry_of: Option<String>,
+    /// The job was asked to stop: the pipeline's next start ends its unended tables.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub cancel: Option<JobCancel>,
 }
 
 impl Document for SnapshotJobDoc {
@@ -135,21 +207,16 @@ impl SnapshotJobDoc {
         Self {
             job_id,
             status: JobStatus::Queued,
-            tables: tables
-                .into_iter()
-                .map(|table| JobTable {
-                    table,
-                    status: JobTableStatus::Queued,
-                    rows_done: 0,
-                    error: None,
-                })
-                .collect(),
+            tables: tables.into_iter().map(JobTable::queued).collect(),
             requested_at,
             requested_by,
             finished_at: None,
             error: None,
             truncate: false,
             note: None,
+            requested_role: None,
+            retry_of: None,
+            cancel: None,
         }
     }
 
@@ -160,14 +227,17 @@ impl SnapshotJobDoc {
         self
     }
 
-    /// Completed or failed: the job has ended, and [`Self::settle`] leaves it as it is.
+    /// Completed, failed or cancelled: the job has ended, and [`Self::settle`] leaves it as it is.
     pub fn is_finished(&self) -> bool {
-        matches!(self.status, JobStatus::Completed | JobStatus::Failed)
+        match self.status {
+            JobStatus::Queued | JobStatus::Running => false,
+            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled => true,
+        }
     }
 
     /// Sets the job's status from its tables': queued while every table is, running while one
-    /// is queued or running, then completed or failed, at `now`. A finished job stays as it
-    /// ended. True when it changed.
+    /// is queued or running, then cancelled when a table was, else completed or failed, at
+    /// `now`. A finished job stays as it ended. True when it changed.
     pub fn settle(&mut self, now: DateTime<Utc>) -> bool {
         if self.is_finished() {
             return false;
@@ -181,6 +251,8 @@ impl SnapshotJobDoc {
             .any(|t| matches!(t.status, JobTableStatus::Queued | JobTableStatus::Running))
         {
             JobStatus::Running
+        } else if self.tables.iter().any(is(JobTableStatus::Cancelled)) {
+            JobStatus::Cancelled
         } else if self.tables.iter().all(is(JobTableStatus::Completed)) {
             JobStatus::Completed
         } else {
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-state -p data-avalanche-core -p data-avalanche-cluster`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/snapshots.rs crates/data-avalanche-cluster/src/controller.rs crates/data-avalanche-core/src/schema.rs crates/data-avalanche-state/src/lib.rs crates/data-avalanche-state/src/snapshot.rs
git commit -m "feat(state): a snapshot job's cancel, retry and per-table timing; COPY_CANCELLED"
```

---

### Task 2: The source's row estimates (`data-avalanche-core`, `-source-pg`, `-source-mysql`, `-testkit`)

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs`, `crates/data-avalanche-source-mysql/src/catalog.rs`, `crates/data-avalanche-source-mysql/src/connector.rs`, `crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs`, `crates/data-avalanche-source-pg/src/connector.rs`, `crates/data-avalanche-source-pg/tests/pg/snapshot.rs`, `crates/data-avalanche-testkit/src/fake_source.rs`

**Interfaces:**
- Consumes: `SourceConnector` (M3); the PostgreSQL and MySQL catalog queries (M5, M10).
- Produces: `SourceConnector::row_estimates(&self, &[TableRef]) -> Result<BTreeMap<TableRef,
  u64>, SourceError>`, by default empty; PostgreSQL from `pg_class.reltuples` (a relation of kind
  `r` or `p`, and only once it has one: `reltuples >= 0`); MySQL from
  `information_schema.TABLES.TABLE_ROWS` in `source.db`, in chunks of `NAMES_PER_QUERY`. Testkit:
  `Method::RowEstimates`, `FakeSource::set_row_estimates(BTreeMap<TableRef, u64>)`.

- [ ] **Step 1: Write the failing tests**

The tests: `row_estimates_are_the_catalog_s_once_a_table_is_analyzed` (PostgreSQL), `row_estimates_are_information_schema_s` (MySQL).

`crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs`:

````diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs b/crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs
index e398e66..824e904 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs
@@ -33,6 +33,39 @@ async fn walk(c: &MySqlConnector, t: &TableSchema, limit: usize) -> (Vec<Row>, V
     }
 }
 
+/// A snapshot job's progress estimate (D-M13c.5): `TABLE_ROWS`, an estimate InnoDB samples, after
+/// `ANALYZE TABLE`; a table of another database, or not there, is left out.
+#[tokio::test]
+async fn row_estimates_are_information_schema_s() {
+    require_it!();
+    let (n, mut a) = fresh(
+        "row_estimates",
+        "CREATE TABLE t (id INT PRIMARY KEY);
+         INSERT INTO t WITH RECURSIVE s (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 1000)
+           SELECT i FROM s;
+         ANALYZE TABLE t;",
+    )
+    .await;
+    a.query_drop("ANALYZE TABLE t").await.unwrap();
+    let c = connector(&pipeline(&n, &["t"], "", ""));
+    let t = data_avalanche_core::TableRef::new(n.db.clone(), "t");
+    let got = c
+        .row_estimates(&[
+            t.clone(),
+            data_avalanche_core::TableRef::new(n.db.clone(), "nope"),
+            data_avalanche_core::TableRef::new("other", "t"),
+        ])
+        .await
+        .unwrap();
+    let rows = got.get(&t).copied().unwrap_or_else(|| panic!("{got:?}"));
+    assert!(
+        (500..=2000).contains(&rows),
+        "an estimate near 1000: {rows}"
+    );
+    assert_eq!(got.len(), 1, "{got:?}");
+    cleanup(&n).await;
+}
+
 /// Key types and values chosen where a cursor parameter could go wrong: past `i64`, digits a
 /// double loses, case and accents under the column's collation, latin1 above ASCII, trailing zero
 /// bytes, the clock ranges' ends, negative times, ENUM (upper-case labels too) and SET in member
````

`crates/data-avalanche-source-pg/tests/pg/snapshot.rs`:

````diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/snapshot.rs b/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
index c1f019d..2fa8df6 100644
--- a/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
@@ -69,6 +69,32 @@ async fn a_chunk_carries_its_snapshot_and_in_flight_lists_open_writers() {
     cleanup(&a, &n).await;
 }
 
+/// A snapshot job's progress estimate (D-M13c.5): `reltuples` once the table is analyzed; a table
+/// never analyzed (`-1`) or not there is left out.
+#[tokio::test]
+async fn row_estimates_are_the_catalog_s_once_a_table_is_analyzed() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(
+        &a,
+        "row_estimates",
+        "CREATE TABLE t (id int PRIMARY KEY);
+         CREATE TABLE never (id int PRIMARY KEY);
+         INSERT INTO t SELECT generate_series(1, 1000);
+         ANALYZE t;",
+    )
+    .await;
+    let c = connector(&pipeline(&n, &["t"], "", ""));
+    let got = c
+        .row_estimates(&[n.table("t"), n.table("never"), n.table("nope")])
+        .await
+        .unwrap();
+    assert_eq!(got.get(&n.table("t")), Some(&1000), "{got:?}");
+    assert_eq!(got.len(), 1, "never analyzed, or not there: {got:?}");
+    assert!(c.row_estimates(&[]).await.unwrap().is_empty());
+    cleanup(&a, &n).await;
+}
+
 /// Final review I3: a table dropped under the snapshot halts that table (`TABLE_DROPPED`), and so
 /// does a key column that lost NOT NULL (`IDENTIFIER_NOT_USABLE`): a NULL key sorts after every
 /// cursor, and its rows would be skipped. Neither stops the pipeline.
````

- [ ] **Step 2: Run them to see them fail**

Run: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg row_estimates && DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-mysql --test mysql row_estimates`
Expected: the build fails: no method `row_estimates`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-core/src/source.rs`:

````diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index 65be8c8..d4b392c 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -2,6 +2,7 @@
 //! source engine. Each source crate implements it; nothing here performs I/O.
 
 use std::any::Any;
+use std::collections::BTreeMap;
 use std::fmt;
 use std::time::Duration;
 
@@ -433,6 +434,18 @@ pub trait SourceConnector: Send + Sync {
         Ok(None)
     }
 
+    /// The source's estimate of each table's rows, for a snapshot job's progress and ETA
+    /// (PostgreSQL `pg_class.reltuples`, MySQL `information_schema.TABLES.TABLE_ROWS`). A table
+    /// with none (never analyzed, not a table, not there) is left out. An estimate, never a count:
+    /// it can be far off.
+    async fn row_estimates(
+        &self,
+        tables: &[TableRef],
+    ) -> Result<BTreeMap<TableRef, u64>, SourceError> {
+        let _ = tables;
+        Ok(BTreeMap::new())
+    }
+
     /// Up to `limit` rows of `table` after `after`, in identifier order.
     async fn read_chunk(
         &self,
````

`crates/data-avalanche-source-mysql/src/catalog.rs`:

````diff
diff --git a/crates/data-avalanche-source-mysql/src/catalog.rs b/crates/data-avalanche-source-mysql/src/catalog.rs
index 5495266..a3b66e2 100644
--- a/crates/data-avalanche-source-mysql/src/catalog.rs
+++ b/crates/data-avalanche-source-mysql/src/catalog.rs
@@ -434,6 +434,47 @@ impl MySqlConnector {
         Ok(out)
     }
 
+    /// `information_schema.TABLES.TABLE_ROWS` of the tables of `source.db` among `tables`: InnoDB's
+    /// is a sampled estimate, which MySQL may cache (`information_schema_stats_expiry`). A table of
+    /// another database, or with none (NULL), is left out.
+    pub async fn row_estimates(
+        &self,
+        tables: &[TableRef],
+    ) -> Result<BTreeMap<TableRef, u64>, SourceError> {
+        let db = self.pipeline.source.db.clone();
+        let names: Vec<String> = tables
+            .iter()
+            .filter(|t| t.schema == db)
+            .map(|t| t.name.clone())
+            .collect();
+        let mut out = BTreeMap::new();
+        for chunk in names.chunks(NAMES_PER_QUERY) {
+            let list = vec!["?"; chunk.len()].join(", ");
+            let mut params: Vec<mysql_async::Value> = vec![db.clone().into()];
+            params.extend(chunk.iter().map(|n| n.clone().into()));
+            let rows: Vec<(String, Option<u64>)> = self
+                .conn
+                .command("estimate the tables' rows", move |c| {
+                    Box::pin(async move {
+                        c.exec(
+                            format!(
+                                "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES
+                                  WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ({list})"
+                            ),
+                            params,
+                        )
+                        .await
+                    })
+                })
+                .await?;
+            out.extend(
+                rows.into_iter()
+                    .filter_map(|(name, rows)| Some((TableRef::new(db.clone(), name), rows?))),
+            );
+        }
+        Ok(out)
+    }
+
     /// An on-demand snapshot's tables, by exact name (Data-Flow §On-Demand Snapshot): the
     /// config's patterns and its table-set checks don't apply.
     pub async fn describe_tables(
````

`crates/data-avalanche-source-mysql/src/connector.rs`:

````diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
index 5569f6f..5077c89 100644
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -2,6 +2,7 @@
 
 use data_avalanche_config::{BinlogPurgeAction, Mysql, Pipeline, SourceType, Tables};
 use std::any::Any;
+use std::collections::BTreeMap;
 use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
 use std::sync::{Arc, Mutex, PoisonError};
 use std::time::Duration;
@@ -449,6 +450,13 @@ impl SourceConnector for MySqlConnector {
         MySqlConnector::describe_tables(self, tables).await
     }
 
+    async fn row_estimates(
+        &self,
+        tables: &[TableRef],
+    ) -> Result<BTreeMap<TableRef, u64>, SourceError> {
+        MySqlConnector::row_estimates(self, tables).await
+    }
+
     async fn prepare(
         &self,
         checkpoint: Option<&Checkpoint>,
````

`crates/data-avalanche-source-pg/src/connector.rs`:

````diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index 4b45ac3..e8a6374 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -335,6 +335,45 @@ impl PgConnector {
             .await
     }
 
+    /// `pg_class.reltuples` of `tables`, once the table was analyzed or vacuumed: before that it
+    /// is `-1` (PostgreSQL 14 and later), and the table is left out, as one that isn't there.
+    pub async fn row_estimates(
+        &self,
+        tables: &[TableRef],
+    ) -> Result<BTreeMap<TableRef, u64>, SourceError> {
+        if tables.is_empty() {
+            return Ok(BTreeMap::new());
+        }
+        let schemas: Vec<&str> = tables.iter().map(|t| t.schema.as_str()).collect();
+        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
+        let client = self.conn.client().await?;
+        let rows = self
+            .conn
+            .command(
+                "estimate the tables' rows",
+                client.query(
+                    "SELECT n.nspname, c.relname, c.reltuples::float8
+                       FROM pg_catalog.pg_class c
+                       JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
+                       JOIN unnest($1::text[], $2::text[]) AS w(s, t)
+                         ON w.s = n.nspname AND w.t = c.relname
+                      WHERE c.relkind IN ('r', 'p') AND c.reltuples >= 0",
+                    &[&schemas, &names],
+                ),
+            )
+            .await?;
+        Ok(rows
+            .iter()
+            .map(|r| {
+                let rows: f64 = r.get(2);
+                (
+                    TableRef::new(r.get::<_, String>(0), r.get::<_, String>(1)),
+                    rows.round() as u64,
+                )
+            })
+            .collect())
+    }
+
     /// `pg_current_wal_lsn()`: a snapshot chunk's watermark (Snapshot_Resumability §1).
     pub async fn current_position(&self) -> Result<SourcePosition, SourceError> {
         let client = self.conn.client().await?;
@@ -414,6 +453,13 @@ impl SourceConnector for PgConnector {
         PgConnector::in_flight(self).await
     }
 
+    async fn row_estimates(
+        &self,
+        tables: &[TableRef],
+    ) -> Result<BTreeMap<TableRef, u64>, SourceError> {
+        PgConnector::row_estimates(self, tables).await
+    }
+
     async fn read_chunk(
         &self,
         table: &TableSchema,
````

`crates/data-avalanche-testkit/src/fake_source.rs`:

````diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index 176764c..cfc12a1 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -52,6 +52,7 @@ pub enum Method {
     Retained,
     CurrentPosition,
     InFlight,
+    RowEstimates,
     ReadChunk,
     FetchRows,
     OpenStream,
@@ -281,6 +282,8 @@ struct Db {
     marks: Vec<Option<SourcePosition>>,
     /// Column names compare without case, as MySQL's do (DA-SRC-25).
     fold: bool,
+    /// [`FakeSource::set_row_estimates`].
+    estimates: BTreeMap<TableRef, u64>,
 }
 
 /// The scripted source. Cloning shares the database.
@@ -346,6 +349,7 @@ impl FakeSource {
             prepares: vec![],
             marks: vec![],
             fold: false,
+            estimates: BTreeMap::new(),
         };
         for t in tables {
             db.create(t);
@@ -524,6 +528,11 @@ impl FakeSource {
         self.db().added = tables.to_vec();
     }
 
+    /// The rows `row_estimates` reports for these tables from now on (by default, none).
+    pub fn set_row_estimates(&self, estimates: BTreeMap<TableRef, u64>) {
+        self.db().estimates = estimates;
+    }
+
     /// The bytes `retained` reports from now on (by default, the log from the slot to its end).
     pub fn set_retained(&self, bytes: Option<u64>) {
         self.db().retained = Some(bytes);
@@ -1224,6 +1233,18 @@ impl SourceConnector for FakeSource {
         Ok(SourcePosition::Pg(self.db().wal_end))
     }
 
+    async fn row_estimates(
+        &self,
+        tables: &[TableRef],
+    ) -> Result<BTreeMap<TableRef, u64>, SourceError> {
+        self.take_fault(Method::RowEstimates)?;
+        let db = self.db();
+        Ok(tables
+            .iter()
+            .filter_map(|t| Some((t.clone(), *db.estimates.get(t)?)))
+            .collect())
+    }
+
     async fn in_flight(
         &self,
     ) -> Result<Option<Vec<data_avalanche_core::InFlightTxn>>, SourceError> {
````

- [ ] **Step 4: Run the tests and the gates**

Run: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg row_estimates && DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-mysql --test mysql row_estimates`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core/src/source.rs crates/data-avalanche-source-mysql/src/catalog.rs crates/data-avalanche-source-mysql/src/connector.rs crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs crates/data-avalanche-source-pg/src/connector.rs crates/data-avalanche-source-pg/tests/pg/snapshot.rs crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "feat(source): the source's estimate of a table's rows"
```

---

### Task 3: A cancel at the start; a copy's start, estimate and halt recorded (`data-avalanche-pipeline`)

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/jobs.rs`, `crates/data-avalanche-pipeline/src/worker.rs`

**Interfaces:**
- Consumes: Task 1's types; Task 2's `row_estimates` and `FakeSource::set_row_estimates`; the
  worker's Phase 0 (`start`), `Jobs` (M9b).
- Produces (crate-private): `Jobs::cancelling(&self) -> BTreeMap<TableRef, Cancelling>` (the
  running tables of the cancelled jobs that no live job asks for or runs), `Cancelling { job:
  String, from_halt: bool }`, `Taken { started, failed, estimates, from_halt, completed }`,
  `Jobs::record(&self, &PipelineDocs, &Taken, DateTime<Utc>)`. `asked`, `running` and
  `truncating` leave out the cancelled jobs.

- [ ] **Step 1: Write the failing tests**

The tests: `a_cancelled_copy_stops_and_the_table_streams_on`, `a_cancelled_copy_of_a_halted_table_halts_it_again`, `a_cancelled_queued_job_never_starts`, `a_cancelled_copy_outside_the_config_isn_t_read`, `a_copy_completed_before_the_cancel_ends_completed`, `a_job_asked_after_a_cancel_copies_the_table_again`, `a_started_table_records_its_start_estimate_and_halt`, `an_estimate_failure_doesn_t_fail_the_start`.

`crates/data-avalanche-pipeline/src/jobs.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
index 68f1181..999f226 100644
--- a/crates/data-avalanche-pipeline/src/jobs.rs
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -101,8 +163,8 @@ mod tests {
         ColumnSchema, ColumnValue, ColumnValueType as V, HaltReason, Row, SourceError, TableSchema,
     };
     use data_avalanche_state::{
-        CursorKey, JobStatus, JobTableStatus, SnapshotCursor, SnapshotJobDoc, SnapshotStatus,
-        TableHalt, TableSnapshotDoc,
+        CursorKey, JobCancel, JobStatus, JobTableStatus, SnapshotCursor, SnapshotJobDoc,
+        SnapshotStatus, TableHalt, TableSnapshotDoc,
     };
     use data_avalanche_testkit::{FakeTxn, Method};
     use tokio::sync::watch;
@@ -1065,4 +1127,248 @@ mod tests {
         let error = j.tables[0].error.clone().unwrap();
         assert!(error.starts_with("SNAPSHOT_TIMEOUT: "), "{error}");
     }
+
+    /// Job `id`'s table `table` is copying: running, its document in progress past two rows,
+    /// brought back from a `from_halt` halt when set.
+    async fn copying(w: &World, id: &str, table: &str, from_halt: Option<&str>) {
+        w.docs
+            .update_snapshot_job(id, |j| {
+                let jt = j.tables.iter_mut().find(|jt| jt.table == t(table)).unwrap();
+                jt.status = JobTableStatus::Running;
+                jt.from_halt = from_halt.map(Into::into);
+                j.settle(Utc::now());
+            })
+            .await
+            .unwrap();
+        w.docs
+            .save_table_snapshot(&TableSnapshotDoc {
+                table: t(table),
+                status: SnapshotStatus::InProgress,
+                start_position: Some(w.source.wal_end()),
+                cursor: Some(SnapshotCursor {
+                    key: vec![CursorKey {
+                        col: "id".into(),
+                        val: 2.into(),
+                        ty: "bigint".into(),
+                    }],
+                    chunk_size: 2,
+                    rows_done: 2,
+                    collation: None,
+                }),
+                row_count: None,
+                completed_at: None,
+            })
+            .await
+            .unwrap();
+    }
+
+    async fn cancel(w: &World, id: &str) {
+        w.docs
+            .update_snapshot_job(id, |j| {
+                j.cancel = Some(JobCancel {
+                    by: "ops".into(),
+                    role: "operator".into(),
+                    at: Utc::now(),
+                    note: None,
+                });
+            })
+            .await
+            .unwrap();
+    }
+
+    /// D-M13c.2: a cancelled job's running copy of a streamed table stops at the next start:
+    /// no chunk is read, its document is abandoned, the table streams on, and the job ends
+    /// cancelled.
+    #[tokio::test]
+    async fn a_cancelled_copy_stops_and_the_table_streams_on() {
+        let w = World::new(&["a"]);
+        established(&w).await;
+        w.source.load(&t("a"), rows(1..=4));
+        ask(&w, "p~1", &["a"]).await;
+        copying(&w, "p~1", "a", None).await;
+        cancel(&w, "p~1").await;
+        let (end, st) = run(&w, &never(2), async |status| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(9, "x")));
+            wait_until("the change streams", || w.ids("a") == [9]).await;
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(w.source.reads(&t("a")), 0, "the copy isn't resumed");
+        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::None);
+        assert!(st.halted.is_empty(), "{:?}", st.halted);
+        let j = job(&w, "p~1").await;
+        assert_eq!(j.status, JobStatus::Cancelled);
+        assert!(j.finished_at.is_some());
+        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
+        assert!(j.tables[0].ended_at.is_some());
+        assert_eq!(j.error, None);
+    }
+
+    /// D-M13c.2: a cancelled copy of a table its job brought back from a halt halts it again,
+    /// `COPY_CANCELLED` naming the job: the lake holds a part of its rows.
+    #[tokio::test]
+    async fn a_cancelled_copy_of_a_halted_table_halts_it_again() {
+        let w = World::new(&["a", "b"]);
+        established(&w).await;
+        w.source.load(&t("a"), rows(1..=4));
+        ask(&w, "p~1", &["a"]).await;
+        copying(&w, "p~1", "a", Some("TABLE_DROPPED")).await;
+        cancel(&w, "p~1").await;
+        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
+        end.unwrap();
+        let reason = HaltReason::CopyCancelled { job: "p~1".into() };
+        assert_eq!(st.halted.get(&t("a")), Some(&reason));
+        let halts = w.docs.halts().await.unwrap().tables;
+        assert_eq!(halts.len(), 1, "{halts:?}");
+        assert_eq!(halts[0].reason, reason);
+        assert!(!halts[0].copy, "the pipeline's table");
+        assert_eq!(w.source.reads(&t("a")), 0);
+        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::None);
+        assert_eq!(job(&w, "p~1").await.status, JobStatus::Cancelled);
+    }
+
+    /// D-M13c.2: a cancelled job whose tables are queued never starts them.
+    #[tokio::test]
+    async fn a_cancelled_queued_job_never_starts() {
+        let w = World::new(&["a"]);
+        established(&w).await;
+        w.source.load(&t("a"), rows(1..=3));
+        ask(&w, "p~1", &["a"]).await;
+        cancel(&w, "p~1").await;
+        let (end, _) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
+        end.unwrap();
+        assert_eq!(w.source.reads(&t("a")), 0);
+        assert!(w.ids("a").is_empty());
+        let j = job(&w, "p~1").await;
+        assert_eq!(j.status, JobStatus::Cancelled);
+        assert_eq!(j.tables[0].status, JobTableStatus::Cancelled);
+        assert_eq!(j.tables[0].started_at, None);
+    }
+
+    /// D-M13c.2: a cancelled copy of a table outside the config isn't read again, and no halt
+    /// is recorded for it.
+    #[tokio::test]
+    async fn a_cancelled_copy_outside_the_config_isn_t_read() {
+        let w = World::new(&["a", "x"]);
+        w.source.outside_config(&t("x"));
+        established(&w).await;
+        w.source.load(&t("x"), rows(1..=4));
+        ask(&w, "p~1", &["x"]).await;
+        copying(&w, "p~1", "x", None).await;
+        cancel(&w, "p~1").await;
+        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
+        end.unwrap();
+        assert_eq!(w.source.reads(&t("x")), 0);
+        assert_eq!(snapshot_status(&w, "x").await, SnapshotStatus::None);
+        assert!(w.docs.halts().await.unwrap().tables.is_empty());
+        assert!(st.halted.is_empty(), "{:?}", st.halted);
+        assert_eq!(job(&w, "p~1").await.status, JobStatus::Cancelled);
+    }
+
+    /// A copy that completed before a start took its job's cancel ends completed, with its
+    /// rows: the lake holds them all.
+    #[tokio::test]
+    async fn a_copy_completed_before_the_cancel_ends_completed() {
+        let w = World::new(&["a"]);
+        established(&w).await;
+        ask(&w, "p~1", &["a"]).await;
+        copying(&w, "p~1", "a", Some("TABLE_DROPPED")).await;
+        w.docs
+            .save_table_snapshot(&TableSnapshotDoc {
+                table: t("a"),
+                status: SnapshotStatus::Completed,
+                start_position: None,
+                cursor: None,
+                row_count: Some(4),
+                completed_at: Some(Utc::now()),
+            })
+            .await
+            .unwrap();
+        cancel(&w, "p~1").await;
+        let (mut core, _control, status) = w.core(&never(2), &[]);
+        crate::worker::start(&mut core, None).await.unwrap();
+        assert!(status.borrow().halted.is_empty(), "not halted again");
+        let j = job(&w, "p~1").await;
+        assert_eq!(j.status, JobStatus::Completed);
+        assert_eq!(j.tables[0].status, JobTableStatus::Completed);
+        assert_eq!(j.tables[0].rows_done, 4);
+    }
+
+    /// A job asked for a table after another's cancel, before a start took it, copies the table
+    /// again from its first row; the cancelled job ends cancelled, and nothing halts.
+    #[tokio::test]
+    async fn a_job_asked_after_a_cancel_copies_the_table_again() {
+        let w = World::new(&["a"]);
+        established(&w).await;
+        w.source.load(&t("a"), rows(1..=4));
+        ask(&w, "p~1", &["a"]).await;
+        copying(&w, "p~1", "a", Some("TABLE_DROPPED")).await;
+        cancel(&w, "p~1").await;
+        ask(&w, "p~2", &["a"]).await;
+        let (end, st) = run(&w, &never(2), async |status| {
+            wait_until("the copy lands", || w.ids("a") == [1, 2, 3, 4]).await;
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert!(st.halted.is_empty(), "{:?}", st.halted);
+        assert_eq!(job(&w, "p~1").await.status, JobStatus::Cancelled);
+        assert_eq!(
+            job(&w, "p~2").await.tables[0].status,
+            JobTableStatus::Running
+        );
+    }
+
+    /// A start that takes a job's tables records when each began, the source's estimate of
+    /// its rows (where it has one) and the halt it cleared for it.
+    #[tokio::test]
+    async fn a_started_table_records_its_start_estimate_and_halt() {
+        let w = World::new(&["a", "b"]);
+        established(&w).await;
+        w.docs
+            .record_halt(&TableHalt {
+                table: t("a"),
+                reason: HaltReason::SnapshotTimeout { limit_secs: 60 },
+                at: w.source.wal_end(),
+                copy: false,
+            })
+            .await
+            .unwrap();
+        w.source
+            .set_row_estimates(std::collections::BTreeMap::from([(t("a"), 1234)]));
+        ask(&w, "p~1", &["a", "b"]).await;
+        let before = Utc::now();
+        let (mut core, _control, _status) = w.core(&never(2), &[]);
+        crate::worker::start(&mut core, None).await.unwrap();
+        let j = job(&w, "p~1").await;
+        let (a, b) = (&j.tables[0], &j.tables[1]);
+        assert_eq!(a.status, JobTableStatus::Running);
+        assert!(a.started_at.is_some_and(|at| at >= before), "{a:?}");
+        assert_eq!(a.estimate, Some(1234));
+        assert_eq!(a.from_halt.as_deref(), Some("SNAPSHOT_TIMEOUT"));
+        assert!(b.started_at.is_some());
+        assert_eq!(b.estimate, None, "the source has none");
+        assert_eq!(b.from_halt, None);
+    }
+
+    /// The source's estimates are a nicety: when it can't give them, the start takes the
+    /// tables all the same, without one.
+    #[tokio::test]
+    async fn an_estimate_failure_doesn_t_fail_the_start() {
+        let w = World::new(&["a"]);
+        established(&w).await;
+        w.source
+            .set_row_estimates(std::collections::BTreeMap::from([(t("a"), 3)]));
+        w.source.fail(
+            Method::RowEstimates,
+            SourceError::Permanent("permission denied for pg_class".into()),
+        );
+        ask(&w, "p~1", &["a"]).await;
+        let (mut core, _control, _status) = w.core(&never(2), &[]);
+        crate::worker::start(&mut core, None).await.unwrap();
+        let a = &job(&w, "p~1").await.tables[0];
+        assert_eq!(a.status, JobTableStatus::Running);
+        assert_eq!(a.estimate, None);
+    }
 }
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-pipeline --lib jobs::`
Expected: seven fail on their assertions (the cancel isn't read: the copies resume, the jobs stay running, nothing halts, no start is recorded); `an_estimate_failure_doesn_t_fail_the_start` passes already (it guards the change).

- [ ] **Step 3: Implement**

`crates/data-avalanche-pipeline/src/jobs.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
index 68f1181..999f226 100644
--- a/crates/data-avalanche-pipeline/src/jobs.rs
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -6,7 +6,7 @@
 
 use std::collections::{BTreeMap, BTreeSet};
 
-use chrono::Utc;
+use chrono::{DateTime, Utc};
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{JobTable, JobTableStatus, PipelineDocs, SnapshotJobDoc, StateError};
 
@@ -16,6 +16,30 @@ pub(crate) struct Jobs {
     jobs: Vec<SnapshotJobDoc>,
 }
 
+/// What a start did with the job tables, for [`Jobs::record`].
+#[derive(Debug, Default)]
+pub(crate) struct Taken {
+    /// The asked tables whose copy began.
+    pub started: BTreeSet<TableRef>,
+    /// The asked or running tables that failed, with their reason.
+    pub failed: BTreeMap<TableRef, String>,
+    /// The source's row estimates of the started tables, where it has one.
+    pub estimates: BTreeMap<TableRef, u64>,
+    /// The started tables whose recorded halt the start cleared, with the halt's code.
+    pub from_halt: BTreeMap<TableRef, String>,
+    /// A cancelled job's running tables whose copy completed before a start took the cancel,
+    /// with their rows: they end completed, not cancelled.
+    pub completed: BTreeMap<TableRef, u64>,
+}
+
+/// A cancelled job's running copy of a table.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) struct Cancelling {
+    pub job: String,
+    /// The job's start cleared a halt of the table: stopped, the table halts again.
+    pub from_halt: bool,
+}
+
 impl Jobs {
     pub async fn load(docs: &PipelineDocs) -> Result<Self, StateError> {
         Ok(Self {
@@ -23,9 +47,13 @@ impl Jobs {
         })
     }
 
+    /// The jobs still asked to run: a cancelled one's tables are neither started nor resumed.
+    fn live(&self) -> impl Iterator<Item = &SnapshotJobDoc> {
+        self.jobs.iter().filter(|j| j.cancel.is_none())
+    }
+
     fn tables(&self, status: JobTableStatus) -> BTreeSet<TableRef> {
-        self.jobs
-            .iter()
+        self.live()
             .flat_map(|j| &j.tables)
             .filter(|t| t.status == status)
             .map(|t| t.table.clone())
@@ -44,8 +72,7 @@ impl Jobs {
 
     /// The asked tables of the jobs that empty their sink tables first (DA-SNAP-15).
     pub fn truncating(&self) -> BTreeSet<TableRef> {
-        self.jobs
-            .iter()
+        self.live()
             .filter(|j| j.truncate)
             .flat_map(|j| &j.tables)
             .filter(|t| t.status == JobTableStatus::Queued)
@@ -53,37 +80,72 @@ impl Jobs {
             .collect()
     }
 
-    /// Records what this start did with the job tables: the asked ones `started` run, and the
-    /// `failed` ones (asked, or running and gone) failed with their reason. An asked table
-    /// neither started nor failed stays queued.
+    /// The running tables of the cancelled jobs that no live job asks for or runs (D-M13c.2):
+    /// their copy stops at this start.
+    pub fn cancelling(&self) -> BTreeMap<TableRef, Cancelling> {
+        let (asked, running) = (self.asked(), self.running());
+        self.jobs
+            .iter()
+            .filter(|j| j.cancel.is_some())
+            .flat_map(|j| j.tables.iter().map(move |t| (j, t)))
+            .filter(|(_, t)| t.status == JobTableStatus::Running)
+            .filter(|(_, t)| !asked.contains(&t.table) && !running.contains(&t.table))
+            .map(|(j, t)| {
+                let c = Cancelling {
+                    job: j.job_id.clone(),
+                    from_halt: t.from_halt.is_some(),
+                };
+                (t.table.clone(), c)
+            })
+            .collect()
+    }
+
+    /// Records what this start did with the job tables, at `now`: the asked ones started run,
+    /// with their estimate and the halt cleared for them; the failed ones (asked, or running
+    /// and gone) failed with their reason. An asked table neither started nor failed stays
+    /// queued. A cancelled job's unended tables end cancelled.
     pub async fn record(
         &self,
         docs: &PipelineDocs,
-        started: &BTreeSet<TableRef>,
-        failed: &BTreeMap<TableRef, String>,
+        taken: &Taken,
+        now: DateTime<Utc>,
     ) -> Result<(), StateError> {
-        let decided = |t: &JobTable| match t.status {
-            JobTableStatus::Queued => started.contains(&t.table) || failed.contains_key(&t.table),
-            JobTableStatus::Running => failed.contains_key(&t.table),
+        let decided = |cancelled: bool, t: &JobTable| match t.status {
+            _ if cancelled => !t.status.is_ended(),
+            JobTableStatus::Queued => {
+                taken.started.contains(&t.table) || taken.failed.contains_key(&t.table)
+            }
+            JobTableStatus::Running => taken.failed.contains_key(&t.table),
             _ => false,
         };
         for job in &self.jobs {
-            if !job.tables.iter().any(decided) {
+            let cancelled = job.cancel.is_some();
+            if !job.tables.iter().any(|t| decided(cancelled, t)) {
                 continue;
             }
             docs.update_snapshot_job(&job.job_id, |j| {
                 for t in &mut j.tables {
-                    if !decided(t) {
+                    if !decided(cancelled, t) {
                         continue;
                     }
-                    if let Some(e) = failed.get(&t.table) {
-                        t.status = JobTableStatus::Failed;
-                        t.error = Some(e.clone());
+                    if cancelled {
+                        match taken.completed.get(&t.table) {
+                            Some(rows) if t.status == JobTableStatus::Running => {
+                                t.rows_done = *rows;
+                                t.end(JobTableStatus::Completed, None, now);
+                            }
+                            _ => t.end(JobTableStatus::Cancelled, None, now),
+                        }
+                    } else if let Some(e) = taken.failed.get(&t.table) {
+                        t.end(JobTableStatus::Failed, Some(e.clone()), now);
                     } else {
                         t.status = JobTableStatus::Running;
+                        t.started_at = Some(now);
+                        t.estimate = taken.estimates.get(&t.table).copied();
+                        t.from_halt = taken.from_halt.get(&t.table).cloned();
                     }
                 }
-                j.settle(Utc::now());
+                j.settle(now);
             })
             .await?;
         }
````

`crates/data-avalanche-pipeline/src/worker.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 90f6463..ec24185 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -9,6 +9,7 @@ use std::sync::Arc;
 use std::sync::atomic::{AtomicBool, Ordering};
 use std::time::Instant;
 
+use chrono::Utc;
 use data_avalanche_config::{Pipeline, SnapshotMode};
 use data_avalanche_core::{
     Checkpoint, ColumnSchema, CommitMarker, CommitOutcome, HaltReason, PositionRange, SchemaHint,
@@ -20,7 +21,7 @@ use data_avalanche_storage::StoreError;
 use tokio::sync::{mpsc, watch};
 
 use crate::decide::{Decision, Facts, Plan, decide};
-use crate::jobs::Jobs;
+use crate::jobs::{Jobs, Taken};
 use crate::schema_change::{Planned, missing_columns, soft_dropped};
 use crate::session::{Core, End, Session};
 use crate::settings::Settings;
@@ -559,6 +560,21 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
     let jobs = Jobs::load(&core.docs).await?;
     let asked = jobs.asked();
     let running = jobs.running();
+    // A cancelled job's running copies stop here (D-M13c.2); one that completed before this
+    // start took the cancel ends completed.
+    let mut taken = Taken::default();
+    let mut cancelling = BTreeMap::new();
+    for (table, c) in jobs.cancelling() {
+        match core.docs.table_snapshot(&table).await?.map(|v| v.doc) {
+            Some(d) if d.status == SnapshotStatus::InProgress => {
+                cancelling.insert(table, c);
+            }
+            Some(d) if d.status == SnapshotStatus::Completed => {
+                taken.completed.insert(table, d.row_count.unwrap_or(0));
+            }
+            _ => {}
+        }
+    }
 
     // New halts, recorded before the checkpoint below.
     let mut halts: Vec<(TableRef, HaltReason)> = resolved.halted.clone();
@@ -574,6 +590,18 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         .cloned()
         .collect();
     let recorded: BTreeSet<TableRef> = recorded.difference(&cleared).cloned().collect();
+    // A stopped copy of a table its job brought back from a halt halts again: the lake holds a
+    // part of its rows, and its stream position may predate them (D-M13c.2).
+    for (table, c) in &cancelling {
+        if c.from_halt
+            && resolved_tables.contains_key(table)
+            && !recorded.contains(table)
+            && !halts.iter().any(|(t, _)| t == table)
+        {
+            let reason = HaltReason::CopyCancelled { job: c.job.clone() };
+            halts.push((table.clone(), reason));
+        }
+    }
     let gone: Vec<TableSchema> = cached
         .values()
         .filter(|c| !resolved_tables.contains_key(c.table()) && !recorded.contains(c.table()))
@@ -720,8 +748,9 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         let tracked_before = cached.contains_key(table);
         let facts = Facts {
             tracked_before,
-            // After a gap, a snapshot starts over: rows before its cursor may have changed.
-            in_progress: in_progress && !gap,
+            // After a gap, a snapshot starts over: rows before its cursor may have changed. A
+            // stopped copy isn't resumed: its document is abandoned.
+            in_progress: in_progress && !gap && !cancelling.contains_key(table),
             sink_has_table: state.exists,
             added: added.contains(table),
         };
@@ -974,6 +1003,13 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
             copies.insert(table, schema);
         }
     }
+    // The other stopped copies' documents go too: a halted table's, and a copy's, which no
+    // start reads again.
+    for table in cancelling.keys() {
+        if !requests.contains(table) && !abandoned.contains(table) {
+            abandoned.push(table.clone());
+        }
+    }
 
     // Persist, in this order: a restart in between redoes what follows.
     for (table, reason) in &halts {
@@ -1132,8 +1168,24 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
             .await?;
         }
     }
+    // The started tables' row estimates, for a progress and an ETA: a source that can't give
+    // them doesn't fail the start.
+    if !started.is_empty() {
+        let tables: Vec<TableRef> = started.iter().cloned().collect();
+        match source.row_estimates(&tables).await {
+            Ok(estimates) => taken.estimates = estimates,
+            Err(e) => tracing::warn!("the row estimates of the tables jobs start are unread: {e}"),
+        }
+    }
+    taken.from_halt = started
+        .iter()
+        .filter(|t| cleared.contains(*t))
+        .filter_map(|t| Some((t.clone(), recorded_halts.get(t)?.code().to_owned())))
+        .collect();
+    taken.started = started;
+    taken.failed = failed;
     core.may_write()?;
-    jobs.record(&core.docs, &started, &failed).await?;
+    jobs.record(&core.docs, &taken, Utc::now()).await?;
     // A copy's halt goes once its job carries it: the pipeline doesn't replicate the table, and
     // no job can clear the halt of a table gone. It is the halt of a table neither configured
     // nor cached (a table taken out of the config keeps its schema) nor read now.
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-pipeline --lib jobs::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline/src/jobs.rs crates/data-avalanche-pipeline/src/worker.rs
git commit -m "feat(pipeline): a cancelled job's copies stop at the start, a halted table's halts again; a copy's start, estimate and halt recorded"
```

---

### Task 4: The owner follows a cancel (`data-avalanche-cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/jobs.rs`

**Interfaces:**
- Consumes: Task 1's types; the owner's job follower (`jobs.rs`, M9b).
- Produces (crate-private): `Jobs { ids: BTreeSet<String>, listed: bool }`: a listing adds to the
  jobs followed. A table that ends records `ended_at`; a cancelled job's running table isn't failed
  by an abandoned document or a `COPY_CANCELLED` halt (the start ends it).

- [ ] **Step 1: Write the failing tests**

The tests: `a_table_that_ends_records_when`, `a_cancelled_job_s_stopped_copy_isn_t_failed`, `a_job_a_start_ended_between_listings_is_reported`; `a_job_that_is_gone_is_followed_no_more` takes the new fields.

`crates/data-avalanche-cluster/src/jobs.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/jobs.rs b/crates/data-avalanche-cluster/src/jobs.rs
index e20ab18..05705cf 100644
--- a/crates/data-avalanche-cluster/src/jobs.rs
+++ b/crates/data-avalanche-cluster/src/jobs.rs
@@ -195,7 +204,9 @@ mod tests {
     use async_trait::async_trait;
     use bytes::Bytes;
     use data_avalanche_core::HaltReason;
-    use data_avalanche_state::{DocStore, HALTS_KEY, JobStatus, SnapshotCursor, TableHalt};
+    use data_avalanche_state::{
+        DocStore, HALTS_KEY, JobCancel, JobStatus, SnapshotCursor, TableHalt,
+    };
     use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
     use object_store::memory::InMemory;
 
@@ -302,9 +313,108 @@ mod tests {
     #[tokio::test]
     async fn a_job_that_is_gone_is_followed_no_more() {
         let (_, docs) = starting();
-        let mut jobs = Jobs(Some(BTreeSet::from(["gone".to_owned()])));
+        let mut jobs = Jobs {
+            ids: BTreeSet::from(["gone".to_owned()]),
+            listed: true,
+        };
         jobs.poll(&docs, "p").await;
-        assert_eq!(jobs.0, Some(BTreeSet::new()));
+        assert!(jobs.ids.is_empty());
+    }
+
+    fn running_job(id: &str) -> SnapshotJobDoc {
+        let mut job = SnapshotJobDoc::new(
+            id.into(),
+            vec![TableRef::new("public", "a")],
+            "api".into(),
+            Utc::now(),
+        );
+        job.status = JobStatus::Running;
+        job.tables[0].status = JobTableStatus::Running;
+        job
+    }
+
+    /// A table that ends records when; the job's end is the poll's.
+    #[tokio::test]
+    async fn a_table_that_ends_records_when() {
+        let (_, docs) = starting();
+        docs.create_snapshot_job(&running_job("j1")).await.unwrap();
+        let mut done = doc(SnapshotStatus::Completed);
+        done.row_count = Some(3);
+        docs.save_table_snapshot(&done).await.unwrap();
+        let before = Utc::now();
+        let mut jobs = Jobs::default();
+        jobs.list(&docs, "p").await;
+        let ended = jobs.poll(&docs, "p").await;
+        assert_eq!(ended.len(), 1);
+        let t = &ended[0].tables[0];
+        assert_eq!(t.status, JobTableStatus::Completed);
+        assert_eq!(t.rows_done, 3);
+        assert!(t.ended_at.is_some_and(|at| at >= before), "{t:?}");
+        assert_eq!(t.ended_at, ended[0].finished_at);
+    }
+
+    /// D-M13c.2: a cancelled job's copy whose document the start abandoned, or whose table it
+    /// halted `COPY_CANCELLED`, isn't failed: the start ends it cancelled, and the poll after
+    /// reports the job so.
+    #[tokio::test]
+    async fn a_cancelled_job_s_stopped_copy_isn_t_failed() {
+        let (_, docs) = starting();
+        let a = TableRef::new("public", "a");
+        let mut job = running_job("j1");
+        job.cancel = Some(JobCancel {
+            by: "ops".into(),
+            role: "operator".into(),
+            at: Utc::now(),
+            note: None,
+        });
+        docs.create_snapshot_job(&job).await.unwrap();
+        docs.save_table_snapshot(&doc(SnapshotStatus::None))
+            .await
+            .unwrap();
+        docs.record_halt(&TableHalt {
+            table: a.clone(),
+            reason: HaltReason::CopyCancelled { job: "j1".into() },
+            at: "pg:0/10".parse().unwrap(),
+            copy: false,
+        })
+        .await
+        .unwrap();
+        let mut jobs = Jobs::default();
+        jobs.list(&docs, "p").await;
+        assert!(jobs.poll(&docs, "p").await.is_empty());
+        let j = docs.snapshot_job("j1").await.unwrap().unwrap().doc;
+        assert_eq!(j.tables[0].status, JobTableStatus::Running, "{j:?}");
+        // The start ends it.
+        docs.update_snapshot_job("j1", |j| {
+            j.tables[0].end(JobTableStatus::Cancelled, None, Utc::now());
+            j.settle(Utc::now());
+        })
+        .await
+        .unwrap();
+        let ended = jobs.poll(&docs, "p").await;
+        assert_eq!(ended.len(), 1);
+        assert_eq!(ended[0].status, JobStatus::Cancelled);
+    }
+
+    /// A job a start ended between two listings (a cancel's restart, then another job's) is
+    /// still reported: a listing adds to the jobs followed.
+    #[tokio::test]
+    async fn a_job_a_start_ended_between_listings_is_reported() {
+        let (_, docs) = starting();
+        docs.create_snapshot_job(&running_job("j1")).await.unwrap();
+        let mut jobs = Jobs::default();
+        jobs.list(&docs, "p").await;
+        docs.update_snapshot_job("j1", |j| {
+            j.tables[0].end(JobTableStatus::Cancelled, None, Utc::now());
+            j.settle(Utc::now());
+        })
+        .await
+        .unwrap();
+        jobs.list(&docs, "p").await;
+        let ended = jobs.poll(&docs, "p").await;
+        assert_eq!(ended.len(), 1, "{ended:?}");
+        assert_eq!(ended[0].job_id, "j1");
+        assert!(jobs.ids.is_empty());
     }
 
     /// A job that ends at a poll deletes the ended jobs past the newest [`KEPT_ENDED_JOBS`]
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-cluster --lib jobs::`
Expected: the build fails: no fields `ids`, `listed` in `Jobs`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/jobs.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/jobs.rs b/crates/data-avalanche-cluster/src/jobs.rs
index e20ab18..05705cf 100644
--- a/crates/data-avalanche-cluster/src/jobs.rs
+++ b/crates/data-avalanche-cluster/src/jobs.rs
@@ -5,7 +5,7 @@
 
 use std::collections::{BTreeMap, BTreeSet};
 
-use chrono::Utc;
+use chrono::{DateTime, Utc};
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{
     HaltsDoc, JobTableStatus, KEPT_ENDED_JOBS, PipelineDocs, SnapshotJobDoc, SnapshotStatus,
@@ -40,7 +40,10 @@ pub(crate) fn progress(
     }
 }
 
-fn apply(job: &mut SnapshotJobDoc, seen: &BTreeMap<TableRef, Progress>) {
+fn apply(job: &mut SnapshotJobDoc, seen: &BTreeMap<TableRef, Progress>, now: DateTime<Utc>) {
+    // A cancelled job's copies stop at the next start, which ends them: an abandoned document
+    // or a `COPY_CANCELLED` halt is then the cancel's, not a failure (D-M13c.2).
+    let cancelled = job.cancel.is_some();
     for t in &mut job.tables {
         if t.status != JobTableStatus::Running {
             continue;
@@ -48,48 +51,53 @@ fn apply(job: &mut SnapshotJobDoc, seen: &BTreeMap<TableRef, Progress>) {
         match seen.get(&t.table) {
             Some(Progress::Running { rows_done }) => t.rows_done = *rows_done,
             Some(Progress::Completed { rows }) => {
-                t.status = JobTableStatus::Completed;
                 t.rows_done = *rows;
+                t.end(JobTableStatus::Completed, None, now);
             }
-            Some(Progress::Failed(why)) => {
-                t.status = JobTableStatus::Failed;
-                t.error = Some(why.clone());
+            Some(Progress::Failed(why)) if !cancelled => {
+                t.end(JobTableStatus::Failed, Some(why.clone()), now);
             }
-            None => {}
+            Some(Progress::Failed(_)) | None => {}
         }
     }
 }
 
-/// The unfinished jobs this owner follows; `None` until a listing succeeds.
+/// The unfinished jobs this owner follows; none until a listing succeeds.
 #[derive(Debug, Default)]
-pub(crate) struct Jobs(Option<BTreeSet<String>>);
+pub(crate) struct Jobs {
+    ids: BTreeSet<String>,
+    /// A listing succeeded since the last that failed.
+    listed: bool,
+}
 
 impl Jobs {
     /// Lists the unfinished jobs: when the term starts, and after a restart taken (a job's
-    /// restart). A failed listing is tried again at the next poll.
+    /// restart). The jobs followed already stay: one a start ended since is reported at the
+    /// next poll. A failed listing is tried again at the next poll.
     pub async fn list(&mut self, docs: &PipelineDocs, pipeline: &str) {
-        self.0 = match docs.unfinished_snapshot_jobs().await {
-            Ok(jobs) => Some(jobs.into_iter().map(|j| j.job_id).collect()),
+        match docs.unfinished_snapshot_jobs().await {
+            Ok(jobs) => {
+                self.ids.extend(jobs.into_iter().map(|j| j.job_id));
+                self.listed = true;
+            }
             Err(e) => {
                 tracing::warn!(
                     pipeline,
                     "snapshot jobs unlisted, listed at the next poll: {e}"
                 );
-                None
+                self.listed = false;
             }
-        };
+        }
     }
 
     /// One poll: each job's running tables, from their documents, into the job; a job that
     /// ended, or is gone, is followed no more. The jobs that ended at this poll.
     pub async fn poll(&mut self, docs: &PipelineDocs, pipeline: &str) -> Vec<SnapshotJobDoc> {
         let mut done = Vec::new();
-        if self.0.is_none() {
+        if !self.listed {
             self.list(docs, pipeline).await;
         }
-        let Some(ids) = self.0.as_mut() else {
-            return done;
-        };
+        let ids = &mut self.ids;
         if ids.is_empty() {
             return done;
         }
@@ -173,16 +181,17 @@ async fn follow(
         let doc = docs.table_snapshot(&t.table).await?.map(|v| v.doc);
         seen.insert(t.table.clone(), progress(&t.table, halts, doc.as_ref()));
     }
+    let now = Utc::now();
     let mut next = v.doc.clone();
-    apply(&mut next, &seen);
-    next.settle(Utc::now());
+    apply(&mut next, &seen, now);
+    next.settle(now);
     if next == v.doc {
         return Ok(None);
     }
     let job = docs
         .update_snapshot_job(&v.doc.job_id, |j| {
-            apply(j, &seen);
-            j.settle(Utc::now());
+            apply(j, &seen, now);
+            j.settle(now);
         })
         .await?;
     Ok(job.filter(SnapshotJobDoc::is_finished))
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-cluster --lib jobs::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/jobs.rs
git commit -m "feat(cluster): the job follower keeps the jobs it follows, leaves a cancelled job's copies to the start, and times each table's end"
```

---

### Task 5: Cancel and retry through the API; a job's pace and lake (`data-avalanche-api`)

**Files:**
- Modify: `crates/data-avalanche-api/src/actions.rs`, `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-api/src/snapshots.rs`
- Generated: `crates/data-avalanche-api/openapi.json`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`

**Interfaces:**
- Consumes: Tasks 1, 3, 4; `find`, `read`, `unavailable` (M9a); the recording (M13a).
- Produces: `POST /v1/snapshots/{job_id}/cancel` (operator, recorded `snapshot.cancel`, verb
  `cancel snapshot job`); `POST /v1/snapshots/{job_id}/retry` (operator, recorded `snapshot.retry`,
  verb `retry snapshot job`); `snapshots::{job_pipeline (pub(crate)), status_name, lake, pace,
  job_of, start_job}`; a job's answer gains `rows_done`, `estimate`, `rate`, `eta_seconds`,
  `requested_role`, `note`, `retry_of`, `cancel`, and each table `estimate`, `started_at`,
  `ended_at`, `from_halt`, `lake`. A job route's action names the job and its pipeline.

- [ ] **Step 1: Write the failing tests**

The tests: `a_job_is_cancelled_through_a_restart`, `a_cancel_restarts_only_a_worker_that_runs`, `an_ended_job_is_retried_as_a_new_job`, `lake_says_what_lake_readers_see`, `a_job_s_pace_is_its_rate_and_the_time_its_estimates_leave` (`snapshots.rs`); `a_job_s_actions_name_the_job_and_its_pipeline` (`actions.rs`); the route table (`lib.rs`); `a_job_is_recorded_then_asked_of_the_holder` takes the new fields.

`crates/data-avalanche-api/src/actions.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/actions.rs b/crates/data-avalanche-api/src/actions.rs
index 0611bc6..5eb97a4 100644
--- a/crates/data-avalanche-api/src/actions.rs
+++ b/crates/data-avalanche-api/src/actions.rs
@@ -465,6 +479,86 @@ mod tests {
         assert_eq!(got[0].text, "start a snapshot job on orders");
     }
 
+    /// A job's cancel and retry are recorded naming the job and its pipeline; a retry's object
+    /// is the new job, its text the job it retries. A caller below the role is recorded naming
+    /// them too.
+    #[tokio::test]
+    async fn a_job_s_actions_name_the_job_and_its_pipeline() {
+        let idp = Issuer::start().await;
+        let (addr, lone, _dir) =
+            served(Arc::new(Auth::new(&idp.api("roles"), USERNAME_CLAIM))).await;
+        let config = lone.node.coordinator.config().await;
+        let docs = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
+        let mut job = data_avalanche_state::SnapshotJobDoc::new(
+            "orders~0123456789".into(),
+            vec![data_avalanche_core::TableRef::new("public", "orders")],
+            "ops".into(),
+            chrono::Utc::now(),
+        );
+        job.tables[0].status = data_avalanche_state::JobTableStatus::Failed;
+        job.settle(chrono::Utc::now());
+        docs.create_snapshot_job(&job).await.unwrap();
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
+        let viewer = format!("Bearer {}", idp.token(serde_json::json!(["viewer"])));
+        let (code, _, _) = request(
+            addr,
+            "POST",
+            "/v1/snapshots/orders~0123456789/cancel",
+            &[("Authorization", viewer.as_str())],
+            "",
+        )
+        .await;
+        assert_eq!(code, 403);
+        let operator = format!("Bearer {}", idp.token(serde_json::json!(["operator"])));
+        let auth = [("Authorization", operator.as_str())];
+        let path = "/v1/snapshots/orders~0123456789";
+        let (code, _, body) = request(addr, "POST", &format!("{path}/cancel"), &auth, "").await;
+        assert_eq!(code, 409, "ended: {body}");
+        let (code, _, body) = request(
+            addr,
+            "POST",
+            &format!("{path}/retry"),
+            &auth,
+            r#"{"note": "the replica is back"}"#,
+        )
+        .await;
+        assert_eq!(code, 202, "{body}");
+        let new = serde_json::from_str::<serde_json::Value>(&body).unwrap()["job_id"]
+            .as_str()
+            .unwrap()
+            .to_owned();
+        let got = events(&lone.events, 3).await;
+        let (retried, cancelled, denied) = (&got[0], &got[1], &got[2]);
+        assert_eq!(denied.kind, "action.denied");
+        assert_eq!(
+            denied.text,
+            "cancel snapshot job orders~0123456789 refused: needs operator"
+        );
+        assert_eq!(denied.pipeline.as_deref(), Some("orders"));
+        assert_eq!(cancelled.kind, "action.snapshot.cancel");
+        assert_eq!(cancelled.text, "cancel snapshot job orders~0123456789");
+        assert_eq!(cancelled.outcome.as_deref(), Some("refused"));
+        assert_eq!(cancelled.pipeline.as_deref(), Some("orders"));
+        assert_eq!(cancelled.object.as_deref(), Some("orders~0123456789"));
+        assert_eq!(retried.kind, "action.snapshot.retry");
+        assert_eq!(retried.text, "retry snapshot job orders~0123456789");
+        assert_eq!(retried.outcome.as_deref(), Some("accepted"));
+        assert_eq!(retried.pipeline.as_deref(), Some("orders"));
+        assert_eq!(retried.object.as_deref(), Some(new.as_str()));
+        assert_eq!(retried.note.as_deref(), Some("the replica is back"));
+    }
+
     #[tokio::test]
     async fn a_caller_below_the_role_is_recorded_as_denied() {
         let idp = Issuer::start().await;
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 1fbd084..4f6a6dd 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -834,6 +885,8 @@ mod tests {
                 ("POST", "/v1/pipelines/{id}/snapshots", Operator),
                 ("GET", "/v1/pipelines/{id}/snapshots", Viewer),
                 ("GET", "/v1/snapshots/{job_id}", Viewer),
+                ("POST", "/v1/snapshots/{job_id}/cancel", Operator),
+                ("POST", "/v1/snapshots/{job_id}/retry", Operator),
                 ("POST", "/v1/cluster/leave", Admin),
                 ("POST", "/v1/admin/shutdown", Admin),
                 ("GET", "/v1/nodes/{node}", Viewer),
````

`crates/data-avalanche-api/src/snapshots.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index cc93f27..3471a5f 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -291,7 +617,8 @@ mod tests {
     use chrono::Utc;
     use data_avalanche_core::TableRef;
     use data_avalanche_state::{
-        Acquired, DesiredState, Holder, JobStatus, JobTableStatus, PipelineDocs, SnapshotJobDoc,
+        Acquired, DesiredState, Holder, JobStatus, JobTable, JobTableStatus, PipelineDocs,
+        SnapshotJobDoc,
     };
     use serde_json::{Value, json};
     use tempfile::TempDir;
@@ -388,8 +715,29 @@ mod tests {
         assert_eq!(one["status"], "queued");
         assert_eq!(
             one["tables"][0],
-            json!({ "table": "public.orders", "status": "queued", "rows_done": 0, "error": null })
+            json!({
+                "table": "public.orders",
+                "status": "queued",
+                "rows_done": 0,
+                "estimate": null,
+                "started_at": null,
+                "ended_at": null,
+                "from_halt": null,
+                "lake": "as_before",
+                "error": null,
+            })
         );
+        assert_eq!(one["requested_role"], "admin", "auth none: an admin");
+        for unset in [
+            "rate",
+            "eta_seconds",
+            "estimate",
+            "retry_of",
+            "cancel",
+            "note",
+        ] {
+            assert_eq!(one[unset], Value::Null, "{unset}: {one}");
+        }
         let (code, all) = json_of(addr, "GET", "/v1/pipelines/orders/snapshots").await;
         assert_eq!(code, 200, "{all}");
         assert_eq!(all["jobs"], json!([one]));
@@ -541,4 +889,274 @@ mod tests {
         assert_eq!(code, 500, "{v}");
         assert!(v["error"].as_str().unwrap().contains("no bucket"), "{v}");
     }
+
+    async fn post(addr: SocketAddr, path: &str, body: &str) -> (u16, Value) {
+        let (code, _, text) = request(addr, "POST", path, &[], body).await;
+        let v = if text.is_empty() {
+            Value::Null
+        } else {
+            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"))
+        };
+        (code, v)
+    }
+
+    /// A job of `tables`, recorded as the API would, `status` each.
+    async fn recorded(docs: &PipelineDocs, id: &str, tables: &[(&str, JobTableStatus)]) {
+        let mut job = SnapshotJobDoc::new(
+            id.into(),
+            tables
+                .iter()
+                .map(|(n, _)| TableRef::new("public", *n))
+                .collect(),
+            "ops".into(),
+            Utc::now(),
+        );
+        for (t, (_, s)) in job.tables.iter_mut().zip(tables) {
+            t.status = *s;
+        }
+        job.settle(Utc::now());
+        docs.create_snapshot_job(&job).await.unwrap();
+    }
+
+    /// D-M13c.2: a cancel records who cancelled the job and why, then asks the holder's
+    /// restart, whose start ends it; the job reads `cancelling` until then. A second cancel
+    /// changes nothing; one of a job that ended is refused.
+    #[tokio::test]
+    async fn a_job_is_cancelled_through_a_restart() {
+        let (addr, lone, docs, _dir) = served().await;
+        held(&lone).await;
+        recorded(&docs, "orders~01", &[("orders", JobTableStatus::Running)]).await;
+        let (code, v) = post(
+            addr,
+            "/v1/snapshots/orders~01/cancel",
+            r#"{"note": " wrong table "}"#,
+        )
+        .await;
+        assert_eq!(code, 202, "{v}");
+        assert_eq!(v["status"], "cancelling");
+        assert_eq!(v["restart"], true);
+        assert_eq!(
+            docs.requests().await.unwrap().restart.unwrap().id,
+            "orders~01/cancel"
+        );
+        let job = docs.snapshot_job("orders~01").await.unwrap().unwrap().doc;
+        let cancel = job.cancel.unwrap();
+        assert_eq!(
+            (cancel.by.as_str(), cancel.role.as_str()),
+            ("anonymous", "admin")
+        );
+        assert_eq!(cancel.note.as_deref(), Some("wrong table"));
+        assert_eq!(job.status, JobStatus::Running, "the start ends it");
+        let (_, one) = json_of(addr, "GET", "/v1/snapshots/orders~01").await;
+        assert_eq!(one["status"], "cancelling", "{one}");
+        assert_eq!(one["cancel"]["by"], "anonymous");
+        assert_eq!(one["cancel"]["note"], "wrong table");
+
+        let (code, v) = post(addr, "/v1/snapshots/orders~01/cancel", "").await;
+        assert_eq!(code, 200, "{v}");
+        assert_eq!(v["unchanged"], true);
+
+        docs.update_snapshot_job("orders~01", |j| {
+            j.tables[0].end(JobTableStatus::Cancelled, None, Utc::now());
+            j.settle(Utc::now());
+        })
+        .await
+        .unwrap();
+        let (_, one) = json_of(addr, "GET", "/v1/snapshots/orders~01").await;
+        assert_eq!(one["status"], "cancelled", "{one}");
+        recorded(&docs, "orders~02", &[("orders", JobTableStatus::Completed)]).await;
+        let (code, v) = post(addr, "/v1/snapshots/orders~02/cancel", "").await;
+        assert_eq!(code, 409, "{v}");
+        assert!(
+            v["error"].as_str().unwrap().contains("ended (completed)"),
+            "{v}"
+        );
+    }
+
+    /// D-M13c.2: no worker runs a stopped pipeline, nor one whose lease no node holds: the
+    /// cancel is recorded, and its next start takes it. A paused pipeline's worker restarts.
+    #[tokio::test]
+    async fn a_cancel_restarts_only_a_worker_that_runs() {
+        let (addr, lone, docs, _dir) = served().await;
+        recorded(&docs, "orders~01", &[("orders", JobTableStatus::Running)]).await;
+        let (code, v) = post(addr, "/v1/snapshots/orders~01/cancel", "").await;
+        assert_eq!(code, 202, "{v}");
+        assert_eq!(v["restart"], false, "no holder: {v}");
+        assert!(docs.requests().await.unwrap().restart.is_none());
+        assert!(
+            docs.snapshot_job("orders~01")
+                .await
+                .unwrap()
+                .unwrap()
+                .doc
+                .cancel
+                .is_some()
+        );
+
+        held(&lone).await;
+        docs.open("orders", "delta").await.unwrap();
+        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
+        recorded(&docs, "orders~02", &[("items", JobTableStatus::Queued)]).await;
+        let (code, v) = post(addr, "/v1/snapshots/orders~02/cancel", "").await;
+        assert_eq!((code, &v["restart"]), (202, &json!(false)), "stopped: {v}");
+        assert!(docs.requests().await.unwrap().restart.is_none());
+
+        docs.set_desired_state(DesiredState::Paused).await.unwrap();
+        recorded(&docs, "orders~03", &[("customers", JobTableStatus::Queued)]).await;
+        let (code, v) = post(addr, "/v1/snapshots/orders~03/cancel", "").await;
+        assert_eq!((code, &v["restart"]), (202, &json!(true)), "paused: {v}");
+    }
+
+    /// A retry starts a new job of an ended one's tables, those not copied unless the body
+    /// asks for all, truncating as the old job did unless it says, and records the job it
+    /// retries. A job that hasn't ended, or left nothing to copy, is refused.
+    #[tokio::test]
+    async fn an_ended_job_is_retried_as_a_new_job() {
+        let (addr, lone, docs, _dir) = served().await;
+        held(&lone).await;
+        recorded(
+            &docs,
+            "orders~01",
+            &[
+                ("orders", JobTableStatus::Completed),
+                ("items", JobTableStatus::Failed),
+            ],
+        )
+        .await;
+        docs.update_snapshot_job("orders~01", |j| j.truncate = true)
+            .await
+            .unwrap();
+        let (code, v) = post(addr, "/v1/snapshots/orders~01/retry", "").await;
+        assert_eq!(code, 202, "{v}");
+        assert_eq!(v["retry_of"], "orders~01");
+        assert_eq!(v["tables"], json!(["public.items"]), "not copied: {v}");
+        assert_eq!(v["truncate"], true, "as the old job");
+        let id = v["job_id"].as_str().unwrap().to_owned();
+        let job = docs.snapshot_job(&id).await.unwrap().unwrap().doc;
+        assert_eq!(job.retry_of.as_deref(), Some("orders~01"));
+        assert_eq!(job.requested_role.as_deref(), Some("admin"));
+        assert_eq!(docs.requests().await.unwrap().restart.unwrap().id, id);
+        let (_, one) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
+        assert_eq!(one["retry_of"], "orders~01", "{one}");
+
+        // The new job holds public.items until it ends.
+        let (code, v) = post(
+            addr,
+            "/v1/snapshots/orders~01/retry",
+            r#"{"tables": "all"}"#,
+        )
+        .await;
+        assert_eq!(code, 409, "{v}");
+        docs.update_snapshot_job(&id, |j| {
+            j.tables[0].end(JobTableStatus::Completed, None, Utc::now());
+            j.settle(Utc::now());
+        })
+        .await
+        .unwrap();
+        let (code, v) = post(
+            addr,
+            "/v1/snapshots/orders~01/retry",
+            r#"{"tables": "all", "truncate": false, "note": "again"}"#,
+        )
+        .await;
+        assert_eq!(code, 202, "{v}");
+        assert_eq!(v["tables"], json!(["public.orders", "public.items"]));
+        assert_eq!(v["truncate"], false);
+        let again = v["job_id"].as_str().unwrap();
+        let job = docs.snapshot_job(again).await.unwrap().unwrap().doc;
+        assert_eq!(job.note.as_deref(), Some("again"));
+
+        let (code, v) = post(addr, &format!("/v1/snapshots/{again}/retry"), "").await;
+        assert_eq!(code, 409, "queued: {v}");
+        assert!(
+            v["error"].as_str().unwrap().contains("once it ended"),
+            "{v}"
+        );
+        let (code, v) = post(addr, &format!("/v1/snapshots/{id}/retry"), "").await;
+        assert_eq!(code, 409, "{v}");
+        assert!(v["error"].as_str().unwrap().contains("was copied"), "{v}");
+        for bad in [r#"{"tables": "some"}"#, r#"{"drop": true}"#, r#""x""#] {
+            let (code, v) = post(addr, "/v1/snapshots/orders~01/retry", bad).await;
+            assert_eq!(code, 422, "{bad}: {v}");
+        }
+        let (code, _) = post(addr, "/v1/snapshots/orders~09/retry", "").await;
+        assert_eq!(code, 404);
+        let (code, _) = post(addr, "/v1/snapshots/orders~09/cancel", "").await;
+        assert_eq!(code, 404);
+    }
+
+    fn table(status: JobTableStatus, rows_done: u64, started: bool) -> JobTable {
+        let mut t = JobTable::queued(TableRef::new("public", "a"));
+        t.status = status;
+        t.rows_done = rows_done;
+        t.started_at = started.then(Utc::now);
+        t
+    }
+
+    #[test]
+    fn lake_says_what_lake_readers_see() {
+        use JobTableStatus::{Cancelled, Completed, Failed, Queued, Running};
+        for truncate in [false, true] {
+            assert_eq!(
+                super::lake(truncate, &table(Completed, 5, true)),
+                "complete"
+            );
+            assert_eq!(super::lake(truncate, &table(Queued, 0, false)), "as_before");
+            assert_eq!(
+                super::lake(truncate, &table(Cancelled, 0, false)),
+                "as_before",
+                "cancelled before it began"
+            );
+        }
+        assert_eq!(super::lake(false, &table(Running, 5, true)), "as_before");
+        assert_eq!(super::lake(true, &table(Running, 0, true)), "empty");
+        assert_eq!(super::lake(true, &table(Running, 5, true)), "partial");
+        assert_eq!(super::lake(true, &table(Cancelled, 5, true)), "partial");
+        assert_eq!(super::lake(true, &table(Failed, 0, true)), "empty");
+        assert_eq!(
+            super::lake(true, &table(Running, 5, false)),
+            "partial",
+            "a job a 1.0 node started records no start"
+        );
+    }
+
+    #[test]
+    fn a_job_s_pace_is_its_rate_and_the_time_its_estimates_leave() {
+        let t0 = Utc::now();
+        let mut job = SnapshotJobDoc::new(
+            "orders~01".into(),
+            vec![
+                TableRef::new("public", "a"),
+                TableRef::new("public", "b"),
+                TableRef::new("public", "c"),
+            ],
+            "ops".into(),
+            t0,
+        );
+        for (t, (status, rows, estimate)) in job.tables.iter_mut().zip([
+            (JobTableStatus::Completed, 600, 600),
+            (JobTableStatus::Running, 400, 1000),
+            (JobTableStatus::Running, 0, 500),
+        ]) {
+            t.status = status;
+            t.rows_done = rows;
+            t.estimate = Some(estimate);
+            t.started_at = Some(t0);
+        }
+        job.settle(t0);
+        let at = |s: i64| t0 + chrono::TimeDelta::seconds(s);
+        assert_eq!(super::pace(&job, at(59)), (None, None), "under a minute");
+        // 1000 rows in 100 s; (1000 - 400) + 500 left.
+        assert_eq!(super::pace(&job, at(100)), (Some(10), Some(110)));
+        job.tables[2].estimate = None;
+        assert_eq!(
+            super::pace(&job, at(100)),
+            (Some(10), None),
+            "an unended table without an estimate"
+        );
+        job.tables[1].end(JobTableStatus::Cancelled, None, at(100));
+        job.tables[2].end(JobTableStatus::Cancelled, None, at(100));
+        job.settle(at(100));
+        assert_eq!(super::pace(&job, at(200)), (None, None), "ended");
+    }
 }
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api`
Expected: the build fails: no `lake`, `pace` in `snapshots`.

- [ ] **Step 3: Implement**

The OpenAPI document and the site's reference are generated, not written: after the diffs
below, run `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi` (it rewrites
`crates/data-avalanche-api/openapi.json`), then `cargo xtask docs` (it rewrites
`docs/public/openapi.json` and `docs/src/content/docs/reference/api.md`), and read their diffs:
only the routes this task adds or changes move.

`crates/data-avalanche-api/src/actions.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/actions.rs b/crates/data-avalanche-api/src/actions.rs
index 0611bc6..5eb97a4 100644
--- a/crates/data-avalanche-api/src/actions.rs
+++ b/crates/data-avalanche-api/src/actions.rs
@@ -203,14 +203,23 @@ impl Action {
             return;
         };
         let recorder = &node.recorder;
-        let pipeline = self
-            .params
-            .iter()
-            .find(|(k, _)| k == "id")
-            .map(|(_, v)| v.clone());
-        let named = match (&pipeline, self.recorded.kind.split('.').next()) {
-            (Some(p), _) => p.clone(),
-            (None, Some("node")) => recorder.node().to_owned(),
+        let param = |name: &str| {
+            self.params
+                .iter()
+                .find(|(k, _)| k == name)
+                .map(|(_, v)| v.clone())
+        };
+        // A job's route names the job, and the pipeline its id names.
+        let job = param("job_id");
+        let pipeline = param("id").or_else(|| {
+            job.as_deref()
+                .and_then(crate::snapshots::job_pipeline)
+                .map(str::to_owned)
+        });
+        let named = match (&job, &pipeline, self.recorded.kind.split('.').next()) {
+            (Some(j), _, _) => j.clone(),
+            (None, Some(p), _) => p.clone(),
+            (None, None, Some("node")) => recorder.node().to_owned(),
             _ => String::new(),
         };
         let mut event = recorder.event(
@@ -260,11 +269,16 @@ pub(crate) async fn denied(gate: &Gate, name: &str, role: &str, path: &str) {
     let segment = path
         .strip_prefix("/v1/pipelines/")
         .map(|rest| rest.split('/').next().unwrap_or_default());
-    let pipeline = match segment {
+    // A job's route: the job, and the pipeline its id names.
+    let job = path
+        .strip_prefix("/v1/snapshots/")
+        .map(|rest| rest.split('/').next().unwrap_or_default());
+    let named = segment.or_else(|| job.and_then(crate::snapshots::job_pipeline));
+    let pipeline = match named {
         Some(id) if known(node, id).await => Some(id.to_owned()),
         _ => None,
     };
-    let target = match segment {
+    let target = match segment.or(job) {
         Some(id) => clipped(id, MAX_TARGET).into_owned(),
         None if path.starts_with("/v1/cluster/") || path.starts_with("/v1/admin/") => {
             recorder.node().to_owned()
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 1fbd084..4f6a6dd 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -475,6 +475,57 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             get(snapshots::get),
         ),
+        (
+            act(
+                "snapshot.cancel",
+                "cancel snapshot job",
+                Route {
+                    body: Some("application/json"),
+                    ..r(
+                        "POST",
+                        "/v1/snapshots/{job_id}/cancel",
+                        Operator,
+                        "Cancel a job: the pipeline's next start ends its unended tables; a table it was bringing back from a halt halts again, COPY_CANCELLED",
+                        &[
+                            (200, "cancelled already"),
+                            (
+                                202,
+                                "cancelling: the owner restarts the worker, or the pipeline's next start ends it",
+                            ),
+                            (404, "no such job"),
+                            (409, "the job has ended"),
+                            (422, "the body isn't `{\"note\": ...}`"),
+                        ],
+                    )
+                },
+            ),
+            post(snapshots::cancel),
+        ),
+        (
+            act(
+                "snapshot.retry",
+                "retry snapshot job",
+                Route {
+                    body: Some("application/json"),
+                    ..r(
+                        "POST",
+                        "/v1/snapshots/{job_id}/retry",
+                        Operator,
+                        "Retry an ended job: a new job of its tables, `{\"tables\": \"all\" | \"not_copied\", \"truncate\": ...}`, that records the job it retries",
+                        &[
+                            (202, "the new job is queued"),
+                            (404, "no such job"),
+                            (
+                                409,
+                                "the job hasn't ended, it left no table to copy, the pipeline doesn't run, no node holds its lease, or an unfinished job holds a table",
+                            ),
+                            (422, "the body isn't of that form"),
+                        ],
+                    )
+                },
+            ),
+            post(snapshots::retry),
+        ),
         (
             act(
                 "node.leave",
````

`crates/data-avalanche-api/src/snapshots.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index cc93f27..3471a5f 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -11,18 +11,18 @@ use axum::Extension;
 use axum::extract::{Path, State};
 use axum::http::StatusCode;
 use axum::response::Response;
-use chrono::Utc;
+use chrono::{DateTime, Utc};
 use data_avalanche_config::{Pipeline, SourceType, key_table};
 use data_avalanche_core::TableRef;
 use data_avalanche_state::{
-    Action, DesiredState, JobStatus, JobTableStatus, PipelineDocs, Request, SnapshotJobDoc,
-    StateError,
+    Action, DesiredState, JobCancel, JobStatus, JobTable, JobTableStatus, PipelineDocs, Request,
+    SnapshotJobDoc, StateError,
 };
 use serde::Deserialize;
 use serde_json::{Value, json};
 
 use crate::pipelines::{desired_name, find, read, unavailable};
-use crate::{Api, Failure, Principal, body, starting};
+use crate::{Api, Failure, Node, Principal, body, starting};
 
 /// Between a job's pipeline id and its suffix: a pipeline id has none (V-055).
 const SEPARATOR: char = '~';
@@ -33,7 +33,7 @@ fn new_job_id(pipeline_id: &str) -> String {
 }
 
 /// The pipeline id a job id names.
-fn job_pipeline(job_id: &str) -> Option<&str> {
+pub(crate) fn job_pipeline(job_id: &str) -> Option<&str> {
     match job_id.split_once(SEPARATOR) {
         Some((p, suffix)) if !p.is_empty() && !suffix.is_empty() => Some(p),
         _ => None,
@@ -101,14 +101,71 @@ fn tables(p: &Pipeline, text: &str) -> Result<(Vec<TableRef>, bool, Option<Strin
     Ok((out, asked.truncate, note))
 }
 
-fn job_json(p: &Pipeline, j: &SnapshotJobDoc) -> Value {
-    let status = |s: JobStatus| match s {
+/// A job's status as the API names it: `cancelling` from the cancel until a start ends it.
+fn status_name(j: &SnapshotJobDoc) -> &'static str {
+    match j.status {
+        _ if j.cancel.is_some() && !j.is_finished() => "cancelling",
         JobStatus::Queued => "queued",
         JobStatus::Running => "running",
         JobStatus::Completed => "completed",
         JobStatus::Failed => "failed",
         JobStatus::Cancelled => "cancelled",
+    }
+}
+
+/// What lake readers see of a job's table (Admin_Console §3): `complete` once it is copied;
+/// while a truncating job's copy of it began and hasn't completed, `empty` before its first
+/// rows and `partial` after; else `as_before`, the rows it held before the job (a copy that
+/// doesn't truncate writes over them).
+fn lake(truncate: bool, t: &JobTable) -> &'static str {
+    let began = t.started_at.is_some()
+        || matches!(
+            t.status,
+            JobTableStatus::Running | JobTableStatus::Completed
+        );
+    match t.status {
+        JobTableStatus::Completed => "complete",
+        _ if !truncate || !began => "as_before",
+        _ if t.rows_done == 0 => "empty",
+        _ => "partial",
+    }
+}
+
+/// The least a job runs before its rate is shown: a shorter run's rate says little.
+const RATE_AFTER: chrono::TimeDelta = chrono::TimeDelta::seconds(60);
+
+/// A running job's copy rate, rows a second, since its first table began, and the seconds left
+/// at that rate from the source's estimates of its unended tables. No rate before
+/// [`RATE_AFTER`] or once it ended; no ETA without a rate or an estimate of every unended table.
+fn pace(j: &SnapshotJobDoc, now: DateTime<Utc>) -> (Option<u64>, Option<u64>) {
+    if j.is_finished() {
+        return (None, None);
+    }
+    let Some(first) = j.tables.iter().filter_map(|t| t.started_at).min() else {
+        return (None, None);
     };
+    let ran = now - first;
+    if ran < RATE_AFTER {
+        return (None, None);
+    }
+    let done: u64 = j.tables.iter().map(|t| t.rows_done).sum();
+    let rate = done * 1000
+        / u64::try_from(ran.num_milliseconds())
+            .unwrap_or(u64::MAX)
+            .max(1);
+    if rate == 0 {
+        return (Some(0), None);
+    }
+    let left: Option<u64> = j
+        .tables
+        .iter()
+        .filter(|t| !t.status.is_ended())
+        .map(|t| t.estimate.map(|e| e.saturating_sub(t.rows_done)))
+        .sum();
+    (Some(rate), left.map(|l| l.div_ceil(rate)))
+}
+
+fn job_json(p: &Pipeline, j: &SnapshotJobDoc, now: DateTime<Utc>) -> Value {
     let table_status = |s: JobTableStatus| match s {
         JobTableStatus::Queued => "queued",
         JobTableStatus::Running => "running",
@@ -116,18 +173,38 @@ fn job_json(p: &Pipeline, j: &SnapshotJobDoc) -> Value {
         JobTableStatus::Failed => "failed",
         JobTableStatus::Cancelled => "cancelled",
     };
+    let estimates: Vec<u64> = j.tables.iter().filter_map(|t| t.estimate).collect();
+    let (rate, eta) = pace(j, now);
     json!({
         "job_id": j.job_id,
         "pipeline": p.id,
-        "status": status(j.status),
+        "status": status_name(j),
         "tables": j.tables.iter().map(|t| json!({
             "table": table_name(p, &t.table),
             "status": table_status(t.status),
             "rows_done": t.rows_done,
+            "estimate": t.estimate,
+            "started_at": t.started_at,
+            "ended_at": t.ended_at,
+            "from_halt": t.from_halt,
+            "lake": lake(j.truncate, t),
             "error": t.error,
         })).collect::<Vec<_>>(),
+        "rows_done": j.tables.iter().map(|t| t.rows_done).sum::<u64>(),
+        "estimate": (!estimates.is_empty()).then(|| estimates.iter().sum::<u64>()),
+        "rate": rate,
+        "eta_seconds": eta,
         "requested_at": j.requested_at,
         "requested_by": j.requested_by,
+        "requested_role": j.requested_role,
+        "note": j.note,
+        "retry_of": j.retry_of,
+        "cancel": j.cancel.as_ref().map(|c| json!({
+            "by": c.by,
+            "role": c.role,
+            "at": c.at,
+            "note": c.note,
+        })),
         "finished_at": j.finished_at,
         "error": j.error,
         "truncate": j.truncate,
@@ -153,7 +230,39 @@ pub(crate) async fn ask(
         Ok(asked) => asked,
         Err(why) => return Failure::new(StatusCode::UNPROCESSABLE_ENTITY, why).into(),
     };
-    let r = match read(node, &p, &docs).await {
+    let start = Start {
+        tables,
+        truncate,
+        note,
+        retry_of: None,
+    };
+    start_job(node, &p, &docs, &by, start).await
+}
+
+/// A job to start: its tables, whether it empties them first, why, and the job it retries.
+struct Start {
+    tables: Vec<TableRef>,
+    truncate: bool,
+    note: Option<String>,
+    retry_of: Option<String>,
+}
+
+/// Records job `start` of pipeline `p`, asked by `by`, then its restart: 202 with the job. 409
+/// when the pipeline doesn't run, no node holds its lease, or an unfinished job holds a table.
+async fn start_job(
+    node: &Node,
+    p: &Pipeline,
+    docs: &PipelineDocs,
+    by: &Principal,
+    start: Start,
+) -> Response {
+    let Start {
+        tables,
+        truncate,
+        note,
+        retry_of,
+    } = start;
+    let r = match read(node, p, docs).await {
         Ok(r) => r,
         Err(e) => return unavailable(&e),
     };
@@ -186,7 +295,7 @@ pub(crate) async fn ask(
             let queued = if j.status == JobStatus::Queued {
                 // Its restart may not have been written (DA-SNAP-21): ask it again, under the
                 // job's id, so the owner takes it at its next poll.
-                match ask_restart_again(&docs, j).await {
+                match ask_restart_again(docs, j).await {
                     Ok(()) => " (it is queued: its restart is asked again, and it runs at the pipeline's next start)".to_owned(),
                     Err(e) => format!(" (it is queued, and its restart can't be asked again ({e}): it runs at the pipeline's next start)"),
                 }
@@ -196,13 +305,15 @@ pub(crate) async fn ask(
             return conflict(format!(
                 "job {} snapshots {} already; ask again once it ended{queued}",
                 j.job_id,
-                table_name(&p, &t.table)
+                table_name(p, &t.table)
             ));
         }
     }
     let mut job = SnapshotJobDoc::new(new_job_id(&p.id), tables, by.name.clone(), Utc::now());
     job.truncate = truncate;
     job.note = note;
+    job.requested_role = Some(by.role.to_string());
+    job.retry_of = retry_of;
     if let Err(e) = docs.create_snapshot_job(&job).await {
         return unavailable(&e);
     }
@@ -219,19 +330,243 @@ pub(crate) async fn ask(
             }),
         );
     }
-    tracing::info!(pipeline = %p.name, by = %by.name, job = %job.job_id, tables = job.tables.len(), truncate, "snapshot job asked through the API");
-    let v = job_json(&p, &job);
+    tracing::info!(pipeline = %p.name, by = %by.name, job = %job.job_id, tables = job.tables.len(), truncate, retry_of = ?job.retry_of, "snapshot job asked through the API");
+    let v = job_json(p, &job, Utc::now());
+    let mut answer = json!({
+        "job_id": v["job_id"],
+        "status": v["status"],
+        "tables": v["tables"].as_array().into_iter().flatten().map(|t| t["table"].clone()).collect::<Vec<_>>(),
+        "truncate": truncate,
+    });
+    if let Some(of) = &job.retry_of {
+        answer["retry_of"] = json!(of);
+    }
+    body(StatusCode::ACCEPTED, &answer)
+}
+
+/// The job `job_id` names, with its pipeline and that pipeline's state: 404 when the id names
+/// no pipeline of the generation, or no job of it; 503 when the state can't be read.
+async fn job_of(
+    node: &Node,
+    job_id: &str,
+) -> Result<(Pipeline, PipelineDocs, SnapshotJobDoc), Failure> {
+    let missing = || Failure::new(StatusCode::NOT_FOUND, format!("no snapshot job {job_id:?}"));
+    let pipeline = job_pipeline(job_id).ok_or_else(missing)?;
+    let (p, docs) = find(node, pipeline).await.map_err(|f| match f.code {
+        StatusCode::NOT_FOUND => missing(),
+        _ => f,
+    })?;
+    match docs.snapshot_job(job_id).await {
+        Ok(Some(v)) => Ok((p, docs, v.doc)),
+        Ok(None) => Err(missing()),
+        Err(e) => Err(Failure::new(
+            StatusCode::SERVICE_UNAVAILABLE,
+            format!("the pipeline's state: {e}"),
+        )),
+    }
+}
+
+/// A body of a note alone, or none.
+#[derive(Deserialize)]
+#[serde(deny_unknown_fields)]
+struct CancelBody {
+    /// Checked by the recording (a 422 before this route runs).
+    #[serde(default)]
+    note: Option<String>,
+}
+
+/// A JSON body as `T`, an empty one as `T`'s default: 422 with `form` when it isn't one.
+fn parsed<T: serde::de::DeserializeOwned>(text: &str, form: &str) -> Result<T, Failure> {
+    let text = if text.trim().is_empty() { "{}" } else { text };
+    serde_json::from_str(text).map_err(|e| {
+        Failure::new(
+            StatusCode::UNPROCESSABLE_ENTITY,
+            format!("the body is {form}: {e}"),
+        )
+    })
+}
+
+fn trimmed(note: Option<String>) -> Option<String> {
+    note.map(|n| n.trim().to_owned()).filter(|n| !n.is_empty())
+}
+
+/// `POST /v1/snapshots/{job_id}/cancel` (D-M13c.2): records who cancelled the job and why, then
+/// asks the pipeline's restart, whose start ends the job's unended tables; a copy of a table the
+/// job brought back from a halt halts it again, `COPY_CANCELLED`. A pipeline stopped, or whose
+/// lease no node holds, ends them at its next start. 202 `cancelling`; 200 when it was
+/// cancelled already; 409 once it ended.
+pub(crate) async fn cancel(
+    State(api): State<Api>,
+    Path(job_id): Path<String>,
+    Extension(by): Extension<Principal>,
+    text: String,
+) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (p, docs, job) = match job_of(node, &job_id).await {
+        Ok(found) => found,
+        Err(f) => return f.into(),
+    };
+    let note = match parsed::<CancelBody>(&text, "{\"note\": \"...\"} or empty") {
+        Ok(n) => trimmed(n.note),
+        Err(f) => return f.into(),
+    };
+    let ended = |j: &SnapshotJobDoc| {
+        body(
+            StatusCode::CONFLICT,
+            &json!({
+                "error": format!("job {job_id} ended ({}) already: there is nothing to cancel", status_name(j)),
+                "job_id": job_id,
+                "status": status_name(j),
+            }),
+        )
+    };
+    if job.is_finished() {
+        return ended(&job);
+    }
+    if job.cancel.is_some() {
+        return body(
+            StatusCode::OK,
+            &json!({ "job_id": job_id, "status": "cancelling", "unchanged": true }),
+        );
+    }
+    let cancel = JobCancel {
+        by: by.name.clone(),
+        role: by.role.to_string(),
+        at: Utc::now(),
+        note,
+    };
+    let updated = docs
+        .update_snapshot_job(&job_id, |j| {
+            if !j.is_finished() && j.cancel.is_none() {
+                j.cancel = Some(cancel.clone());
+            }
+        })
+        .await;
+    match updated {
+        Ok(Some(j)) if j.is_finished() => return ended(&j),
+        Ok(Some(_)) => {}
+        Ok(None) => {
+            return Failure::new(StatusCode::NOT_FOUND, format!("no snapshot job {job_id:?}"))
+                .into();
+        }
+        Err(e) => return unavailable(&e),
+    }
+    // A worker runs while the pipeline runs or is paused: its restart takes the cancel now.
+    let r = match read(node, &p, &docs).await {
+        Ok(r) => Some(r),
+        Err(e) => {
+            tracing::warn!(pipeline = %p.name, job = %job_id, "a cancel's restart unasked, the pipeline's state unread: {e}");
+            None
+        }
+    };
+    let runs = r.as_ref().is_some_and(|r| {
+        r.desired != DesiredState::Stopped && r.lease.as_ref().is_some_and(|l| !l.released)
+    });
+    let restart = Request {
+        id: format!("{job_id}/cancel"),
+        at: cancel.at,
+    };
+    let asked = runs
+        && {
+            match docs.request(Action::Restart, restart).await {
+                Ok(()) => true,
+                Err(e) => {
+                    tracing::warn!(pipeline = %p.name, job = %job_id, "a cancel's restart unasked: {e}");
+                    false
+                }
+            }
+        };
+    tracing::info!(pipeline = %p.name, by = %by.name, job = %job_id, restart = asked, "snapshot job cancelled through the API");
+    let detail = if asked {
+        "the owner restarts the pipeline's worker, whose start stops the job's copies"
+    } else {
+        "no worker runs the pipeline now: its next start stops the job's copies"
+    };
     body(
         StatusCode::ACCEPTED,
-        &json!({
-            "job_id": v["job_id"],
-            "status": v["status"],
-            "tables": v["tables"].as_array().into_iter().flatten().map(|t| t["table"].clone()).collect::<Vec<_>>(),
-            "truncate": truncate,
-        }),
+        &json!({ "job_id": job_id, "status": "cancelling", "restart": asked, "detail": detail }),
     )
 }
 
+/// Which of an ended job's tables a retry copies again.
+#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
+#[serde(rename_all = "snake_case")]
+enum Which {
+    All,
+    /// Those that didn't complete.
+    #[default]
+    NotCopied,
+}
+
+#[derive(Deserialize)]
+#[serde(deny_unknown_fields)]
+struct Retried {
+    #[serde(default)]
+    tables: Which,
+    /// The old job's when absent.
+    #[serde(default)]
+    truncate: Option<bool>,
+    #[serde(default)]
+    note: Option<String>,
+}
+
+/// `POST /v1/snapshots/{job_id}/retry`: a new job of an ended one's tables (`all`, or
+/// `not_copied`, the default), truncating as it did unless the body says, recording the job it
+/// retries; asked as `POST /v1/pipelines/{id}/snapshots` is. 409 for a job that hasn't ended,
+/// or with no table left to copy.
+pub(crate) async fn retry(
+    State(api): State<Api>,
+    Path(job_id): Path<String>,
+    Extension(by): Extension<Principal>,
+    text: String,
+) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (p, docs, job) = match job_of(node, &job_id).await {
+        Ok(found) => found,
+        Err(f) => return f.into(),
+    };
+    let form =
+        "{\"tables\": \"all\" | \"not_copied\", \"truncate\": true | false, \"note\": \"...\"}";
+    let asked = match parsed::<Retried>(&text, form) {
+        Ok(a) => a,
+        Err(f) => return f.into(),
+    };
+    let conflict = |why: String| {
+        body(
+            StatusCode::CONFLICT,
+            &json!({ "error": why, "job_id": job_id, "status": status_name(&job) }),
+        )
+    };
+    if !job.is_finished() {
+        return conflict(format!(
+            "job {job_id} is {}: retry it once it ended",
+            status_name(&job)
+        ));
+    }
+    let tables: Vec<TableRef> = job
+        .tables
+        .iter()
+        .filter(|t| asked.tables == Which::All || t.status != JobTableStatus::Completed)
+        .map(|t| t.table.clone())
+        .collect();
+    if tables.is_empty() {
+        return conflict(format!(
+            "every table of job {job_id} was copied; retry it with \"tables\": \"all\" to copy them again"
+        ));
+    }
+    let start = Start {
+        tables,
+        truncate: asked.truncate.unwrap_or(job.truncate),
+        note: trimmed(asked.note),
+        retry_of: Some(job_id.clone()),
+    };
+    start_job(node, &p, &docs, &by, start).await
+}
+
 /// Writes queued job `j`'s restart again, unless it is the restart recorded already.
 async fn ask_restart_again(docs: &PipelineDocs, j: &SnapshotJobDoc) -> Result<(), StateError> {
     let restart = Request {
@@ -253,10 +588,11 @@ pub(crate) async fn list(State(api): State<Api>, Path(id): Path<String>) -> Resp
         Ok(found) => found,
         Err(f) => return f.into(),
     };
+    let now = Utc::now();
     match docs.snapshot_jobs().await {
         Ok(jobs) => body(
             StatusCode::OK,
-            &json!({ "jobs": jobs.iter().map(|v| job_json(&p, &v.doc)).collect::<Vec<_>>() }),
+            &json!({ "jobs": jobs.iter().map(|v| job_json(&p, &v.doc, now)).collect::<Vec<_>>() }),
         ),
         Err(e) => unavailable(&e),
     }
@@ -267,19 +603,9 @@ pub(crate) async fn get(State(api): State<Api>, Path(job_id): Path<String>) -> R
     let Some(node) = api.node.get() else {
         return starting();
     };
-    let missing = || Failure::new(StatusCode::NOT_FOUND, format!("no snapshot job {job_id:?}"));
-    let Some(pipeline) = job_pipeline(&job_id) else {
-        return missing().into();
-    };
-    let (p, docs) = match find(node, pipeline).await {
-        Ok(found) => found,
-        Err(f) if f.code == StatusCode::NOT_FOUND => return missing().into(),
-        Err(f) => return f.into(),
-    };
-    match docs.snapshot_job(&job_id).await {
-        Ok(Some(v)) => body(StatusCode::OK, &job_json(&p, &v.doc)),
-        Ok(None) => missing().into(),
-        Err(e) => unavailable(&e),
+    match job_of(node, &job_id).await {
+        Ok((p, _, job)) => body(StatusCode::OK, &job_json(&p, &job, Utc::now())),
+        Err(f) => f.into(),
     }
 }
 
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/actions.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/snapshots.rs docs/public/openapi.json docs/src/content/docs/reference/api.md
git commit -m "feat(api): cancel and retry a snapshot job; a job's pace, what lake readers see, its cancel and retry"
```

---

### Task 6: Every pipeline's jobs (`data-avalanche-state`, `-api`, `data-avalanche`)

**Files:**
- Modify: `crates/data-avalanche-api/Cargo.toml`, `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-api/src/openapi.rs`, `crates/data-avalanche-api/src/snapshots.rs`, `crates/data-avalanche-api/src/test_support.rs`, `crates/data-avalanche-state/src/snapshot.rs`, `crates/data-avalanche/src/run.rs`
- Generated: `crates/data-avalanche-api/openapi.json`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`

**Interfaces:**
- Consumes: Task 5's `status_name`, `job_json`; `PipelineDocs::snapshot_jobs` (M9b).
- Produces: `PipelineDocs::snapshot_jobs_reusing(&self, impl Fn(&str) -> Option<SnapshotJobDoc>)
  -> Result<Vec<(String, SnapshotJobDoc)>, StateError>`; API crate root `EndedJobs` (Default) and
  `READ_AT_ONCE` (16); `Node::ended_jobs: Arc<EndedJobs>`; `GET /v1/snapshots` (viewer) with
  `status`, `pipeline`, `by`. The API crate's `futures-util` moves to its dependencies.

- [ ] **Step 1: Write the failing tests**

The tests: `jobs_known_by_their_key_are_not_read_again` (state); `the_jobs_of_every_pipeline_are_listed_and_filtered`, `an_ended_job_is_read_once_and_goes_once_pruned`, `a_pipeline_whose_jobs_can_t_be_read_is_named`, the route table (api).

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 4f6a6dd..3455735 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -884,6 +901,7 @@ mod tests {
                 ("POST", "/v1/pipelines/{id}/flush", Operator),
                 ("POST", "/v1/pipelines/{id}/snapshots", Operator),
                 ("GET", "/v1/pipelines/{id}/snapshots", Viewer),
+                ("GET", "/v1/snapshots", Viewer),
                 ("GET", "/v1/snapshots/{job_id}", Viewer),
                 ("POST", "/v1/snapshots/{job_id}/cancel", Operator),
                 ("POST", "/v1/snapshots/{job_id}/retry", Operator),
````

`crates/data-avalanche-api/src/snapshots.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index 3471a5f..20c4dab 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -617,8 +784,8 @@ mod tests {
     use chrono::Utc;
     use data_avalanche_core::TableRef;
     use data_avalanche_state::{
-        Acquired, DesiredState, Holder, JobStatus, JobTable, JobTableStatus, PipelineDocs,
-        SnapshotJobDoc,
+        Acquired, DesiredState, Holder, JobCancel, JobStatus, JobTable, JobTableStatus,
+        PipelineDocs, SnapshotJobDoc,
     };
     use serde_json::{Value, json};
     use tempfile::TempDir;
@@ -1159,4 +1326,174 @@ mod tests {
         job.settle(at(100));
         assert_eq!(super::pace(&job, at(200)), (None, None), "ended");
     }
+
+    /// [`CONFIG`] and a second pipeline, `billing`, its state elsewhere.
+    async fn served_two() -> (SocketAddr, Lone, PipelineDocs, PipelineDocs, TempDir) {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        let two = format!(
+            "{CONFIG}  billing:
+    id: billing
+    source:
+      host: db.example.com
+      db: billing
+      user: cdc
+      password: pw
+      postgres: {{ publication: billing_pub, slot: billing_slot }}
+    tables:
+      include: [public.invoices]
+    sink:
+      type: delta
+      bucket: lake
+      prefix: bill
+"
+        );
+        std::fs::write(&file, &two).unwrap();
+        let env = std::sync::Arc::new(data_avalanche_config::ProcessEnv);
+        let lone = crate::test_support::lone_with(file, &two, env).await;
+        let config = lone.node.coordinator.config().await;
+        let orders = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
+        let billing = (lone.node.docs)(&config.pipelines["billing"]).unwrap();
+        let a = api(open());
+        a.node.set(lone.node.clone()).unwrap();
+        (serve(a).await, lone, orders, billing, dir)
+    }
+
+    async fn listed(addr: SocketAddr, query: &str) -> (u16, Value) {
+        json_of(addr, "GET", &format!("/v1/snapshots{query}")).await
+    }
+
+    fn ids(v: &Value) -> Vec<String> {
+        v["jobs"]
+            .as_array()
+            .unwrap_or_else(|| panic!("{v}"))
+            .iter()
+            .map(|j| {
+                format!(
+                    "{} {}",
+                    j["job_id"].as_str().unwrap(),
+                    j["status"].as_str().unwrap()
+                )
+            })
+            .collect()
+    }
+
+    /// Every pipeline's jobs, newest request first, filtered by status (several at once),
+    /// pipeline and who asked.
+    #[tokio::test]
+    async fn the_jobs_of_every_pipeline_are_listed_and_filtered() {
+        let (addr, _lone, orders, billing, _dir) = served_two().await;
+        recorded(
+            &orders,
+            "orders~01",
+            &[("orders", JobTableStatus::Completed)],
+        )
+        .await;
+        recorded(
+            &billing,
+            "billing~02",
+            &[("invoices", JobTableStatus::Running)],
+        )
+        .await;
+        recorded(&orders, "orders~03", &[("orders", JobTableStatus::Queued)]).await;
+        orders
+            .update_snapshot_job("orders~03", |j| {
+                j.cancel = Some(JobCancel {
+                    by: "ops".into(),
+                    role: "operator".into(),
+                    at: Utc::now(),
+                    note: None,
+                });
+            })
+            .await
+            .unwrap();
+        let (code, v) = listed(addr, "").await;
+        assert_eq!(code, 200, "{v}");
+        assert_eq!(
+            ids(&v),
+            [
+                "orders~03 cancelling",
+                "billing~02 running",
+                "orders~01 completed"
+            ]
+        );
+        assert_eq!(v["jobs"][1]["pipeline"], "billing");
+        assert_eq!(v["unreadable"], json!([]));
+        let (_, v) = listed(addr, "?status=running,cancelling").await;
+        assert_eq!(ids(&v), ["orders~03 cancelling", "billing~02 running"]);
+        let (_, v) = listed(addr, "?pipeline=billing").await;
+        assert_eq!(ids(&v), ["billing~02 running"]);
+        let (_, v) = listed(addr, "?by=ops&status=completed").await;
+        assert_eq!(ids(&v), ["orders~01 completed"]);
+        let (_, v) = listed(addr, "?by=someone").await;
+        assert!(ids(&v).is_empty());
+        assert_eq!(listed(addr, "?pipeline=nope").await.0, 404);
+        for bad in ["?status=done", "?colour=red", "?by=a&by=b"] {
+            let (code, v) = listed(addr, bad).await;
+            assert_eq!(code, 400, "{bad}: {v}");
+        }
+    }
+
+    /// An ended job is read once: it doesn't change. A running one is read each time, and an
+    /// ended one pruned goes.
+    #[tokio::test]
+    async fn an_ended_job_is_read_once_and_goes_once_pruned() {
+        let (addr, _lone, orders, billing, _dir) = served_two().await;
+        recorded(
+            &orders,
+            "orders~01",
+            &[("orders", JobTableStatus::Completed)],
+        )
+        .await;
+        recorded(
+            &billing,
+            "billing~02",
+            &[("invoices", JobTableStatus::Running)],
+        )
+        .await;
+        assert_eq!(listed(addr, "").await.1["jobs"][1]["requested_by"], "ops");
+        for (docs, id) in [(&orders, "orders~01"), (&billing, "billing~02")] {
+            docs.update_snapshot_job(id, |j| j.requested_by = "changed".into())
+                .await
+                .unwrap();
+        }
+        let (_, v) = listed(addr, "").await;
+        assert_eq!(v["jobs"][0]["requested_by"], "changed", "running: {v}");
+        assert_eq!(v["jobs"][1]["requested_by"], "ops", "ended, kept: {v}");
+        assert_eq!(orders.prune_snapshot_jobs(0).await.unwrap(), 1);
+        let (_, v) = listed(addr, "").await;
+        assert_eq!(ids(&v), ["billing~02 running"]);
+    }
+
+    /// A pipeline whose jobs can't be read is named; the others' are listed.
+    #[tokio::test]
+    async fn a_pipeline_whose_jobs_can_t_be_read_is_named() {
+        let (_, lone, orders, _billing, _dir) = served_two().await;
+        recorded(
+            &orders,
+            "orders~01",
+            &[("orders", JobTableStatus::Completed)],
+        )
+        .await;
+        let mut node = lone.node.clone();
+        let docs = std::sync::Arc::clone(&node.docs);
+        node.docs = std::sync::Arc::new(move |p| match p.id.as_str() {
+            "billing" => Err("no bucket".into()),
+            _ => docs(p),
+        });
+        let a = api(open());
+        a.node.set(node).unwrap();
+        let addr = serve(a).await;
+        let (code, v) = listed(addr, "").await;
+        assert_eq!(code, 200, "{v}");
+        assert_eq!(ids(&v), ["orders~01 completed"]);
+        assert_eq!(v["unreadable"][0]["pipeline"], "billing");
+        assert!(
+            v["unreadable"][0]["error"]
+                .as_str()
+                .unwrap()
+                .contains("no bucket"),
+            "{v}"
+        );
+    }
 }
````

`crates/data-avalanche-state/src/snapshot.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index 6258d43..fb3ea00 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -1000,4 +1031,37 @@ mod tests {
         assert_eq!(ids, ["good"]);
         assert_eq!(p.unfinished_snapshot_jobs().await.unwrap().len(), 1);
     }
+
+    /// A job the caller knows by its key is taken from it, not read; the others are read; a
+    /// key it knows that the listing lacks isn't returned.
+    #[tokio::test]
+    async fn jobs_known_by_their_key_are_not_read_again() {
+        let p = pipeline();
+        let a = TableRef::new("public", "a");
+        let mut ended = SnapshotJobDoc::new("p~1".into(), vec![a.clone()], "ops".into(), t(1));
+        ended.tables[0].end(JobTableStatus::Completed, None, t(2));
+        ended.settle(t(2));
+        p.create_snapshot_job(&ended).await.unwrap();
+        let running = SnapshotJobDoc::new("p~2".into(), vec![a], "ops".into(), t(3));
+        p.create_snapshot_job(&running).await.unwrap();
+        let mut kept = ended.clone();
+        kept.requested_by = "kept".into();
+        let known = |key: &str| match key {
+            k if k == job_key("p~1") => Some(kept.clone()),
+            k if k == job_key("p~0") => panic!("not listed, not asked"),
+            _ => None,
+        };
+        let got = p.snapshot_jobs_reusing(known).await.unwrap();
+        let got: Vec<(&str, &str, &str)> = got
+            .iter()
+            .map(|(k, j)| (k.as_str(), j.job_id.as_str(), j.requested_by.as_str()))
+            .collect();
+        assert_eq!(
+            got,
+            [
+                ("snapshots/jobs/p~1.json", "p~1", "kept"),
+                ("snapshots/jobs/p~2.json", "p~2", "ops"),
+            ]
+        );
+    }
 }
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-state --lib snapshot:: && cargo test -p data-avalanche-api`
Expected: the build fails: no method `snapshot_jobs_reusing`.

- [ ] **Step 3: Implement**

The OpenAPI document and the site's reference are generated, not written: after the diffs
below, run `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi` (it rewrites
`crates/data-avalanche-api/openapi.json`), then `cargo xtask docs` (it rewrites
`docs/public/openapi.json` and `docs/src/content/docs/reference/api.md`), and read their diffs:
only the routes this task adds or changes move.

`crates/data-avalanche-api/Cargo.toml`:

````diff
diff --git a/crates/data-avalanche-api/Cargo.toml b/crates/data-avalanche-api/Cargo.toml
index c413898..7fe3161 100644
--- a/crates/data-avalanche-api/Cargo.toml
+++ b/crates/data-avalanche-api/Cargo.toml
@@ -19,6 +19,7 @@ data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
 data-avalanche-state.workspace = true
 data-avalanche-telemetry.workspace = true
+futures-util.workspace = true
 http-body-util.workspace = true
 jsonwebtoken.workspace = true
 reqwest.workspace = true
@@ -40,7 +41,6 @@ axum = { workspace = true, features = ["json"] }
 boon.workspace = true
 data-avalanche-pipeline.workspace = true
 data-avalanche-storage.workspace = true
-futures-util.workspace = true
 object_store.workspace = true
 rcgen.workspace = true
 tempfile.workspace = true
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 4f6a6dd..3455735 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -51,6 +51,7 @@ pub use peers::{
     STATUS_LIMIT, TARGET,
 };
 pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
+pub use snapshots::{EndedJobs, READ_AT_ONCE};
 
 use crate::auth::Denied;
 
@@ -92,6 +93,8 @@ pub struct Node {
     pub process: watch::Receiver<ProcessFigures>,
     /// What the gossip saw of its probes.
     pub gossip: watch::Receiver<GossipFigures>,
+    /// The ended snapshot jobs `GET /v1/snapshots` read: they don't change.
+    pub ended_jobs: Arc<EndedJobs>,
 }
 
 impl std::fmt::Debug for Node {
@@ -465,6 +468,20 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             get(snapshots::list),
         ),
+        (
+            r(
+                "GET",
+                "/v1/snapshots",
+                Viewer,
+                "Every pipeline's snapshot jobs, newest request first: by status, pipeline and who asked; a pipeline whose jobs can't be read is named under `unreadable`",
+                &[
+                    (200, "the jobs"),
+                    (400, "a parameter that isn't one, or a status that isn't"),
+                    (404, "no such pipeline in the generation this node runs"),
+                ],
+            ),
+            get(snapshots::all),
+        ),
         (
             r(
                 "GET",
````

`crates/data-avalanche-api/src/openapi.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index 2ad63b0..c3b7da7 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -101,6 +101,18 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
             );
         }
     }
+    if r.path == "/v1/snapshots" {
+        for (name, description) in crate::snapshots::PARAMETERS {
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
     if r.path == "/v1/events" {
         for (name, description) in crate::events::PARAMETERS {
             let schema = if *name == "limit" {
````

`crates/data-avalanche-api/src/snapshots.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index 3471a5f..20c4dab 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -8,7 +8,7 @@
 use std::collections::BTreeSet;
 
 use axum::Extension;
-use axum::extract::{Path, State};
+use axum::extract::{Path, RawQuery, State};
 use axum::http::StatusCode;
 use axum::response::Response;
 use chrono::{DateTime, Utc};
@@ -609,6 +609,173 @@ pub(crate) async fn get(State(api): State<Api>, Path(job_id): Path<String>) -> R
     }
 }
 
+/// How many pipelines' jobs `GET /v1/snapshots` reads at once.
+pub const READ_AT_ONCE: usize = 16;
+
+/// The statuses a job is listed by.
+const STATUSES: [&str; 6] = [
+    "queued",
+    "running",
+    "cancelling",
+    "completed",
+    "failed",
+    "cancelled",
+];
+
+/// `GET /v1/snapshots`'s query.
+pub(crate) const PARAMETERS: &[(&str, &str)] = &[
+    (
+        "status",
+        "queued, running, cancelling, completed, failed or cancelled; several comma-separated",
+    ),
+    ("pipeline", "a pipeline's id"),
+    ("by", "who asked for it"),
+];
+
+/// The ended snapshot jobs `GET /v1/snapshots` read, by pipeline and key: an ended job doesn't
+/// change, so it is read once. A job a later listing lacks (pruned) goes.
+#[derive(Debug, Default)]
+pub struct EndedJobs(
+    std::sync::Mutex<std::collections::BTreeMap<(String, String), SnapshotJobDoc>>,
+);
+
+impl EndedJobs {
+    fn known(&self, pipeline: &str, key: &str) -> Option<SnapshotJobDoc> {
+        let kept = self
+            .0
+            .lock()
+            .unwrap_or_else(std::sync::PoisonError::into_inner);
+        kept.get(&(pipeline.to_owned(), key.to_owned())).cloned()
+    }
+
+    /// Keeps pipeline `pipeline`'s ended jobs of `listed`, and only those.
+    fn keep(&self, pipeline: &str, listed: &[(String, SnapshotJobDoc)]) {
+        let mut kept = self
+            .0
+            .lock()
+            .unwrap_or_else(std::sync::PoisonError::into_inner);
+        kept.retain(|(p, _), _| p != pipeline);
+        for (key, job) in listed.iter().filter(|(_, j)| j.is_finished()) {
+            kept.insert((pipeline.to_owned(), key.clone()), job.clone());
+        }
+    }
+}
+
+/// `GET /v1/snapshots`'s filter: the statuses, the pipeline, who asked.
+#[derive(Debug, Default)]
+struct Filter {
+    statuses: Vec<String>,
+    pipeline: Option<String>,
+    by: Option<String>,
+}
+
+fn filter(params: &[(String, String)]) -> Result<Filter, Failure> {
+    let bad = |why: String| Failure::new(StatusCode::BAD_REQUEST, why);
+    let mut f = Filter::default();
+    let mut seen = Vec::new();
+    for (name, value) in params {
+        if seen.contains(name) {
+            return Err(bad(format!("{name} is given twice")));
+        }
+        seen.push(name.clone());
+        let value = value.trim().to_owned();
+        match name.as_str() {
+            "status" => {
+                for s in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
+                    if !STATUSES.contains(&s) {
+                        return Err(bad(format!(
+                            "status {s:?} isn't one of {}",
+                            STATUSES.join(", ")
+                        )));
+                    }
+                    f.statuses.push(s.to_owned());
+                }
+            }
+            "pipeline" => f.pipeline = Some(value),
+            "by" => f.by = Some(value),
+            other => {
+                return Err(bad(format!(
+                    "unknown parameter {other:?}: the jobs take status, pipeline, by"
+                )));
+            }
+        }
+    }
+    Ok(f)
+}
+
+/// Pipeline `p`'s jobs, each with its key, the ended ones [`EndedJobs`] keeps not read again.
+async fn jobs_of(node: &Node, p: &Pipeline) -> Result<Vec<(String, SnapshotJobDoc)>, String> {
+    let docs = (node.docs)(p).map_err(|e| format!("the state location: {e}"))?;
+    let jobs = docs
+        .snapshot_jobs_reusing(|key| node.ended_jobs.known(&p.id, key))
+        .await
+        .map_err(|e| e.to_string())?;
+    node.ended_jobs.keep(&p.id, &jobs);
+    Ok(jobs)
+}
+
+/// `GET /v1/snapshots`: the jobs of every pipeline of the generation, newest request first,
+/// filtered; the pipelines read [`READ_AT_ONCE`] at a time. A pipeline whose jobs can't be read
+/// is named under `unreadable`, and the others are listed.
+pub(crate) async fn all(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
+    use futures_util::StreamExt;
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let params: Vec<(String, String)> =
+        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
+            .into_owned()
+            .collect();
+    let f = match filter(&params) {
+        Ok(f) => f,
+        Err(f) => return f.into(),
+    };
+    let config = node.coordinator.config().await;
+    let mut pipelines: Vec<&Pipeline> = config
+        .pipelines
+        .values()
+        .filter(|p| f.pipeline.as_ref().is_none_or(|id| &p.id == id))
+        .collect();
+    if let Some(id) = &f.pipeline
+        && pipelines.is_empty()
+    {
+        return Failure::new(
+            StatusCode::NOT_FOUND,
+            format!("no pipeline {id:?} in the config generation this node runs"),
+        )
+        .into();
+    }
+    pipelines.sort_by(|a, b| a.id.cmp(&b.id));
+    // Owned: a stream over references doesn't make a `Send` future (rust-lang/rust#64552).
+    let owned: Vec<Pipeline> = pipelines.iter().map(|p| (*p).clone()).collect();
+    let read = futures_util::stream::iter(owned)
+        .map(|p| async move { jobs_of(node, &p).await })
+        .buffered(READ_AT_ONCE)
+        .collect::<Vec<_>>()
+        .await;
+    let now = Utc::now();
+    let mut jobs = vec![];
+    let mut unreadable = vec![];
+    for (p, got) in pipelines.into_iter().zip(read) {
+        match got {
+            Ok(listed) => jobs.extend(listed.into_iter().map(|(_, j)| (p, j))),
+            Err(error) => unreadable.push(json!({ "pipeline": p.id, "error": error })),
+        }
+    }
+    jobs.retain(|(_, j)| {
+        (f.statuses.is_empty() || f.statuses.iter().any(|s| s == status_name(j)))
+            && f.by.as_ref().is_none_or(|by| &j.requested_by == by)
+    });
+    jobs.sort_by(|(_, a), (_, b)| (b.requested_at, &b.job_id).cmp(&(a.requested_at, &a.job_id)));
+    body(
+        StatusCode::OK,
+        &json!({
+            "jobs": jobs.iter().map(|(p, j)| job_json(p, j, now)).collect::<Vec<_>>(),
+            "unreadable": unreadable,
+        }),
+    )
+}
+
 #[cfg(test)]
 mod tests {
     use std::net::SocketAddr;
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 04626d1..70a989a 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -288,6 +288,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             peers: crate::Peers::new("node-a", None).unwrap(),
             process: tokio::sync::watch::channel(Default::default()).1,
             gossip: gossip.figures(),
+            ended_jobs: Arc::default(),
         },
         events,
         leases,
````

`crates/data-avalanche-state/src/snapshot.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index 6258d43..fb3ea00 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -465,6 +465,37 @@ impl PipelineDocs {
         });
         Ok(jobs)
     }
+
+    /// Every job of this pipeline as [`Self::snapshot_jobs`] reads them, each with its key; but
+    /// a job `known` gives by its key is taken from there, not read (an ended job doesn't
+    /// change, so a reader may keep it).
+    ///
+    /// # Errors
+    /// As [`Self::snapshot_jobs`].
+    pub async fn snapshot_jobs_reusing(
+        &self,
+        known: impl Fn(&str) -> Option<SnapshotJobDoc>,
+    ) -> Result<Vec<(String, SnapshotJobDoc)>, StateError> {
+        let mut jobs = vec![];
+        for key in self.docs.store().list(JOBS_PREFIX).await? {
+            if let Some(job) = known(&key) {
+                jobs.push((key, job));
+                continue;
+            }
+            match self.docs.load::<SnapshotJobDoc>(&key).await {
+                Ok(Some(v)) => jobs.push((key, v.doc)),
+                Ok(None) => {}
+                Err(e @ (StateError::Corrupt { .. } | StateError::UnsupportedVersion { .. })) => {
+                    tracing::warn!("snapshot job {key} skipped: {e}");
+                }
+                Err(e) => return Err(e),
+            }
+        }
+        jobs.sort_by(|(_, a), (_, b)| {
+            (a.requested_at, &a.job_id).cmp(&(b.requested_at, &b.job_id))
+        });
+        Ok(jobs)
+    }
 }
 
 #[cfg(test)]
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index d9fb84a..7c533f0 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -305,6 +305,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         peers,
         process,
         gossip: gossip.figures(),
+        ended_jobs: Arc::default(),
     });
     ready.store(true, Ordering::SeqCst);
     let mut cluster = Some((gossip, reconciler, discovery));
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-state --lib snapshot:: && cargo test -p data-avalanche-api`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/Cargo.toml crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/openapi.rs crates/data-avalanche-api/src/snapshots.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche-state/src/snapshot.rs crates/data-avalanche/src/run.rs docs/public/openapi.json docs/src/content/docs/reference/api.md
git commit -m "feat(api): GET /v1/snapshots, every pipeline's jobs, the ended ones read once"
```

---

### Task 7: Carried in: scheme-relative URLs masked; event expiry batched (`data-avalanche-config`, `-storage`, `-state`)

**Files:**
- Modify: `crates/data-avalanche-config/src/mask.rs`, `crates/data-avalanche-state/src/events.rs`, `crates/data-avalanche-storage/src/store.rs`, `crates/data-avalanche-storage/tests/versioned_store.rs`

**Interfaces:**
- Consumes: the masker's `without_userinfo` (M13b); `Events::prune` (M13a); `VersionedStore`
  (M2).
- Produces: `VersionedStore::delete_many(&self, &[String]) -> Result<(), StoreError>` (default: one
  at a time; `ObjectStoreVersioned`: `object_store`'s `delete_stream`); `prune` deletes a day's
  events in one call. `without_userinfo` masks a `//user:pass@host` that starts the value or a
  word.

- [ ] **Step 1: Write the failing tests**

The tests: `a_scheme_relative_url_s_credentials_are_masked` (config); `delete_many_deletes_every_key` and the backends' conformance check (storage, RustFS and Azurite under `DATA_AVALANCHE_IT`, GCS under its gate); `prune_deletes_a_day_in_one_batch` (state).

`crates/data-avalanche-config/src/mask.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/mask.rs b/crates/data-avalanche-config/src/mask.rs
index 1fe6f8b..48cd710 100644
--- a/crates/data-avalanche-config/src/mask.rs
+++ b/crates/data-avalanche-config/src/mask.rs
@@ -1035,6 +1044,27 @@ pipelines:
         );
     }
 
+    /// A scheme-relative URL's credentials are masked too (`//user:pass@host`, which a key
+    /// without a scheme check takes); a `//` inside a path isn't a URL's.
+    #[test]
+    fn a_scheme_relative_url_s_credentials_are_masked() {
+        assert_eq!(
+            without_userinfo("//u:p@h:5432/db").as_deref(),
+            Some("//••••••@h:5432/db")
+        );
+        assert_eq!(
+            without_userinfo("see //u@h?x and https://a:b@c").as_deref(),
+            Some("see //••••••@h?x and https://••••••@c")
+        );
+        for kept in ["/data//x@y", "a//u:p@h", "//h/p@x", "//••••••@h"] {
+            assert_eq!(without_userinfo(kept), None, "{kept}");
+        }
+        let text = "config_version: 1\nglobals:\n  console:\n    prometheus: { url: '//prom:SECRETX@prom.example:9090' }\n";
+        let out = mask_secrets(text, &key()).unwrap();
+        assert!(!out.contains("SECRETX"), "{out}");
+        assert!(out.contains("//••••••@prom.example:9090"), "{out}");
+    }
+
     /// Only the secret keys are masked: a `user` written as the password's text stays shown, and
     /// the text is still shown (the E2E's sources write `user: avalanche`, `password: avalanche`).
     #[test]
````

`crates/data-avalanche-state/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/events.rs b/crates/data-avalanche-state/src/events.rs
index 790efdc..52427b3 100644
--- a/crates/data-avalanche-state/src/events.rs
+++ b/crates/data-avalanche-state/src/events.rs
@@ -645,4 +644,89 @@ mod tests {
             "again: nothing left to delete"
         );
     }
+
+    /// A store that counts its deletes: one at a time, and in batches (each batch's size).
+    #[derive(Debug)]
+    struct Counting {
+        inner: Arc<dyn VersionedStore>,
+        deletes: std::sync::atomic::AtomicUsize,
+        batches: std::sync::Mutex<Vec<usize>>,
+    }
+
+    #[async_trait::async_trait]
+    impl VersionedStore for Counting {
+        async fn get(
+            &self,
+            key: &str,
+        ) -> Result<Option<(bytes::Bytes, data_avalanche_storage::ObjectVersion)>, StoreError>
+        {
+            self.inner.get(key).await
+        }
+        async fn create(
+            &self,
+            key: &str,
+            body: bytes::Bytes,
+        ) -> Result<data_avalanche_storage::ObjectVersion, StoreError> {
+            self.inner.create(key, body).await
+        }
+        async fn update(
+            &self,
+            key: &str,
+            expected: &data_avalanche_storage::ObjectVersion,
+            body: bytes::Bytes,
+        ) -> Result<data_avalanche_storage::ObjectVersion, StoreError> {
+            self.inner.update(key, expected, body).await
+        }
+        async fn put(
+            &self,
+            key: &str,
+            body: bytes::Bytes,
+        ) -> Result<data_avalanche_storage::ObjectVersion, StoreError> {
+            self.inner.put(key, body).await
+        }
+        async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
+            self.inner.list(prefix).await
+        }
+        async fn folders(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
+            self.inner.folders(prefix).await
+        }
+        async fn delete(&self, key: &str) -> Result<(), StoreError> {
+            self.deletes.fetch_add(1, Ordering::SeqCst);
+            self.inner.delete(key).await
+        }
+        async fn delete_many(&self, keys: &[String]) -> Result<(), StoreError> {
+            self.batches.lock().unwrap().push(keys.len());
+            self.inner.delete_many(keys).await
+        }
+    }
+
+    /// Expiry deletes each day's events in one batch, not one key at a time (M13a's carry-in).
+    #[tokio::test]
+    async fn prune_deletes_a_day_in_one_batch() {
+        let (_, inner) = events();
+        let store = Arc::new(Counting {
+            inner,
+            deletes: std::sync::atomic::AtomicUsize::new(0),
+            batches: std::sync::Mutex::new(vec![]),
+        });
+        let ev = Events::new(DocStore::new(Arc::clone(&store) as Arc<dyn VersionedStore>));
+        for (day, n) in [(10, 3), (11, 1), (14, 2)] {
+            for i in 0..n {
+                ev.record(&event(at(day, 12, i), "member.joined", "a"))
+                    .await
+                    .unwrap();
+            }
+        }
+        let deleted = ev
+            .prune(Duration::from_secs(2 * 86_400), at(14, 6, 0))
+            .await
+            .unwrap();
+        assert_eq!(deleted, 4);
+        assert_eq!(*store.batches.lock().unwrap(), [3, 1]);
+        assert_eq!(
+            store.deletes.load(Ordering::SeqCst),
+            0,
+            "none one at a time"
+        );
+    }
 }
````

`crates/data-avalanche-storage/src/store.rs`:

````diff
diff --git a/crates/data-avalanche-storage/src/store.rs b/crates/data-avalanche-storage/src/store.rs
index 49c13b5..de2d778 100644
--- a/crates/data-avalanche-storage/src/store.rs
+++ b/crates/data-avalanche-storage/src/store.rs
@@ -446,6 +479,35 @@ mod tests {
         assert!(s.folders("none").await.unwrap().is_empty());
     }
 
+    /// The keys go, each once, in batches; a key already gone is no error; another root's
+    /// keys stay. The trait's default deletes them too.
+    #[tokio::test]
+    async fn delete_many_deletes_every_key() {
+        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
+        let s = ObjectStoreVersioned::new(mem.clone(), "a").unwrap();
+        let other = ObjectStoreVersioned::new(mem, "b").unwrap();
+        let keys: Vec<String> = (0..2500).map(|i| format!("events/d/{i:04}.json")).collect();
+        for k in keys.iter().take(3) {
+            s.put(k, b("x")).await.unwrap();
+            other.put(k, b("x")).await.unwrap();
+        }
+        s.delete_many(&keys).await.unwrap();
+        assert!(s.list("").await.unwrap().is_empty());
+        assert_eq!(
+            other.list("").await.unwrap().len(),
+            3,
+            "another root's stay"
+        );
+        other.delete_many(&[]).await.unwrap();
+        ListOnly(other.clone())
+            .delete_many(&keys[..2])
+            .await
+            .unwrap();
+        assert_eq!(other.list("").await.unwrap(), ["events/d/0002.json"]);
+        let e = s.delete_many(&["a//b".to_owned()]).await.unwrap_err();
+        assert!(matches!(e, StoreError::Permanent { .. }), "{e:?}");
+    }
+
     #[tokio::test]
     async fn roots_are_isolated() {
         let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
````

`crates/data-avalanche-storage/tests/versioned_store.rs`:

````diff
diff --git a/crates/data-avalanche-storage/tests/versioned_store.rs b/crates/data-avalanche-storage/tests/versioned_store.rs
index 9404c04..08f5e37 100644
--- a/crates/data-avalanche-storage/tests/versioned_store.rs
+++ b/crates/data-avalanche-storage/tests/versioned_store.rs
@@ -91,6 +91,21 @@ async fn check(store: Arc<dyn ObjectStore>, test: &str) {
         .await
         .expect("a delete of a missing object succeeds");
 
+    // A batched delete (the events' expiry, M13c): every key goes, a missing one included; a
+    // sibling prefix's keys stay.
+    let day: Vec<String> = (0..5)
+        .map(|i| format!("events/2026-11-01/{i}.json"))
+        .collect();
+    for k in &day[..4] {
+        s.put(k, b("e")).await.unwrap();
+    }
+    s.put("events/2026-11-02/0.json", b("e")).await.unwrap();
+    s.delete_many(&day).await.expect("a batched delete");
+    assert_eq!(
+        s.list("events").await.unwrap(),
+        vec!["events/2026-11-02/0.json"]
+    );
+
     delete_prefix(store.as_ref(), &prefix).await;
 }
 
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-config --lib mask:: && cargo test -p data-avalanche-storage && cargo test -p data-avalanche-state --lib events::`
Expected: the masker's test fails on `//u:p@h:5432/db` (left as written); the build of the storage and state tests fails: no method `delete_many`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-config/src/mask.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/mask.rs b/crates/data-avalanche-config/src/mask.rs
index 1fe6f8b..48cd710 100644
--- a/crates/data-avalanche-config/src/mask.rs
+++ b/crates/data-avalanche-config/src/mask.rs
@@ -398,16 +398,25 @@ struct Leaf {
 }
 
 /// `value` with the credentials of every URL in it masked (`https://••••••@host`): wherever a
-/// `://` is followed by an `@` before the next `/`, `?`, `#` or the value's end, what lies
-/// between them. A URL inside a `${VAR:-…}` default, or in any text, included. `None` when it
-/// has none, or they are masked already.
+/// `//` after a `:` (`scheme://`), or at the start of the value or of a word (a scheme-relative
+/// `//user:pass@host`), is followed by an `@` before the next `/`, `?`, `#` or the value's end,
+/// what lies between them. A URL inside a `${VAR:-…}` default, or in any text, included; a `//`
+/// inside a path isn't one. `None` when it has none, or they are masked already.
 fn without_userinfo(value: &str) -> Option<String> {
     let mut out = String::with_capacity(value.len());
     let mut rest = value;
     let mut changed = false;
-    while let Some(at) = rest.find("://") {
-        let (head, tail) = rest.split_at(at + 3);
+    while let Some(at) = rest.find("//") {
+        let before = rest[..at]
+            .chars()
+            .next_back()
+            .or_else(|| out.chars().next_back());
+        let (head, tail) = rest.split_at(at + 2);
         out.push_str(head);
+        if !before.is_none_or(|c| c == ':' || c.is_whitespace()) {
+            rest = tail;
+            continue;
+        }
         let authority = tail.split(['/', '?', '#']).next().unwrap_or(tail);
         match authority.rfind('@') {
             Some(end) if &authority[..end] != MASK => {
````

`crates/data-avalanche-state/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/events.rs b/crates/data-avalanche-state/src/events.rs
index 790efdc..52427b3 100644
--- a/crates/data-avalanche-state/src/events.rs
+++ b/crates/data-avalanche-state/src/events.rs
@@ -262,8 +262,8 @@ impl Events {
     }
 
     /// Deletes every event of the days that ended `retention` or more before `now`, and returns
-    /// how many. Whole days only: an event lives at least `retention`, at most a day more. Safe
-    /// on every node at once. A retention reaching past the earliest time a date can hold keeps
+    /// how many. Whole days only: an event lives at least `retention`, at most a day more; each
+    /// day's in one batched delete ([`VersionedStore::delete_many`]). Safe on every node at once. A retention reaching past the earliest time a date can hold keeps
     /// everything.
     ///
     /// # Errors
@@ -286,10 +286,9 @@ impl Events {
             if day.parse::<NaiveDate>().is_err() || day >= keep_from {
                 continue;
             }
-            for key in store.list(&format!("{EVENTS_PREFIX}/{day}")).await? {
-                store.delete(&key).await?;
-                deleted += 1;
-            }
+            let keys = store.list(&format!("{EVENTS_PREFIX}/{day}")).await?;
+            store.delete_many(&keys).await?;
+            deleted += keys.len();
         }
         Ok(deleted)
     }
````

`crates/data-avalanche-storage/src/store.rs`:

````diff
diff --git a/crates/data-avalanche-storage/src/store.rs b/crates/data-avalanche-storage/src/store.rs
index 49c13b5..de2d778 100644
--- a/crates/data-avalanche-storage/src/store.rs
+++ b/crates/data-avalanche-storage/src/store.rs
@@ -113,6 +113,15 @@ pub trait VersionedStore: Send + Sync + std::fmt::Debug {
     /// more (an ended snapshot job, DA-SNAP-16): never a lease or a pointer (deleting a lease
     /// restarts its epoch).
     async fn delete(&self, key: &str) -> Result<(), StoreError>;
+    /// Deletes every key of `keys` as [`Self::delete`] does: in batches where the store can (S3
+    /// deletes a thousand a request), else one at a time (the default). The first failure ends
+    /// it; the keys before it may be gone.
+    async fn delete_many(&self, keys: &[String]) -> Result<(), StoreError> {
+        for key in keys {
+            self.delete(key).await?;
+        }
+        Ok(())
+    }
 }
 
 /// [`VersionedStore`] over an `object_store` bucket, rooted at a key prefix (`""` = bucket root).
@@ -296,6 +305,30 @@ impl VersionedStore for ObjectStoreVersioned {
             Err(e) => Err(map_error(Op::Delete, key, e)),
         }
     }
+
+    async fn delete_many(&self, keys: &[String]) -> Result<(), StoreError> {
+        use futures_util::StreamExt;
+        let paths: Vec<object_store::Result<Path>> = keys
+            .iter()
+            .map(|k| self.path(k))
+            .collect::<Result<Vec<Path>, StoreError>>()?
+            .into_iter()
+            .map(Ok)
+            .collect();
+        let mut deleted = self
+            .store
+            .delete_stream(futures_util::stream::iter(paths).boxed());
+        while let Some(r) = deleted.next().await {
+            match r {
+                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
+                Err(e) => {
+                    let at = format!("{} keys from {}", keys.len(), keys[0]);
+                    return Err(map_error(Op::Delete, &at, e));
+                }
+            }
+        }
+        Ok(())
+    }
 }
 
 #[cfg(test)]
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-config --lib mask:: && cargo test -p data-avalanche-storage && cargo test -p data-avalanche-state --lib events::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

Also, with the emulators up: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-storage --test versioned_store` passes (RustFS and Azurite take the batched delete).

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/mask.rs crates/data-avalanche-state/src/events.rs crates/data-avalanche-storage/src/store.rs crates/data-avalanche-storage/tests/versioned_store.rs
git commit -m "fix: carried in from M13a and M13b, a scheme-relative URL's credentials masked and the events' expiry batched"
```

---

### Task 8: The site

**Files:**
- Modify: `docs/src/content/docs/deployment/events.md`, `docs/src/content/docs/runbooks/halted-table.md`, `docs/src/content/docs/runbooks/snapshot-jobs.md`

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces: the snapshot-jobs runbook's figures, cancel and retry; `COPY_CANCELLED` in the
  halted-table runbook; the new event kinds.

- [ ] **Step 1: Write the failing tests**

None: these are pages, and the site's build (Step 4) checks their links.

- [ ] **Step 2: Run them to see them fail**

Nothing runs here.

- [ ] **Step 3: Implement**

`docs/src/content/docs/deployment/events.md`:

````diff
diff --git a/docs/src/content/docs/deployment/events.md b/docs/src/content/docs/deployment/events.md
index d450de7..ddd128e 100644
--- a/docs/src/content/docs/deployment/events.md
+++ b/docs/src/content/docs/deployment/events.md
@@ -41,7 +41,7 @@ its pipeline only when it names one the cluster runs.
 
 | Kind | Severity | When |
 |---|---|---|
-| `action.<object>.<verb>` | info, warning when refused | an API action: `pipeline.pause`, `.stop`, `.resume`, `.restart`, `.flush`, `snapshot.start`, `config.reload`, `node.leave`, `node.shutdown` |
+| `action.<object>.<verb>` | info, warning when refused | an API action: `pipeline.pause`, `.stop`, `.resume`, `.restart`, `.flush`, `snapshot.start`, `snapshot.cancel`, `snapshot.retry`, `config.reload`, `node.leave`, `node.shutdown` |
 | `action.denied` | warning | a call refused for the caller's role |
 | `lease.acquired`, `lease.takeover`, `lease.released`, `lease.lost` | info, warning, info, error | a pipeline starts on a node; is taken over from a node that stopped renewing (a failover); is handed off; is lost |
 | `pipeline.error`, `pipeline.retrying`, `pipeline.running` | error, warning, info | a pipeline goes into `ERROR`; fails and is retried; runs again after either, or after a pause or a stop |
@@ -50,7 +50,7 @@ its pipeline only when it names one the cluster runs.
 | `pipeline.removed` | info, warning | a pipeline gone from the config is drained and its source's slot and publication torn down; a warning when that waits for its lease to expire |
 | `table.halted`, `table.resumed` | error, info | a table halts, with its reason; runs again after a snapshot job brought it back |
 | `schema.applied`, `schema.degraded` | info, warning | a source schema change is evolved into the sink, fully or in part (a change the sink refuses halts the table) |
-| `job.completed`, `job.failed` | info, error | a snapshot job ends |
+| `job.completed`, `job.failed`, `job.cancelled` | info, error, warning | a snapshot job ends; a cancelled one names who cancelled it and how many of its tables were copied |
 | `member.joined`, `member.restarted`, `member.draining`, `member.left` | info | a node joins, restarts, starts handing its pipelines off, leaves |
 | `member.suspect`, `member.alive`, `member.down` | warning, info, warning | a node stops answering probes, answers again, is declared failed |
 | `config.published` | info | a config generation is published |
````

`docs/src/content/docs/runbooks/halted-table.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/halted-table.md b/docs/src/content/docs/runbooks/halted-table.md
index d900504..7437343 100644
--- a/docs/src/content/docs/runbooks/halted-table.md
+++ b/docs/src/content/docs/runbooks/halted-table.md
@@ -40,6 +40,7 @@ Its `halted_tables` lists each halted table with its code and a detail.
 | `SCHEMA_MISMATCH` | a column whose type the stream and the table disagree on, or an override that can't apply; on PostgreSQL, a Delta column that matches a source column by case alone (Delta names are case-insensitive), the detail naming both spellings (on MySQL and MariaDB, whose column names are case-insensitive too, it is the same column, and keeps the Delta table's spelling); or a Delta table the pipeline meets for the first time (left by a state that's gone, or made by a job's copy) holding a non-null column the source lacks | correct the `column_type_overrides` entry; for a case-only match or a non-null column the source lacks, drop the Delta table and copy the table again |
 | `SINK_REJECTED` | Delta refused the table's changes for good, for example a MERGE over `merge_memory_limit`; a table whose two source columns differ by case alone, which a Delta table can't hold; a table whose reposition after a source change Delta refused (the detail says `its reposition fails`); or a table a truncating job asked for whose Delta table is keyed otherwise than the pipeline's, by other columns or the same spelled otherwise (the detail says `a truncating job`), which isn't emptied | the detail names it: raise the limit, or lower the `flush` bounds; rename one of the two source columns; for a truncating job's table, drop or move the Delta table, then ask the job again; for a refused reposition, fix what the detail names, or drop or move the Delta table, then copy the table again. A table the pipeline doesn't replicate is repositioned at that start (and again at a later source change, which doesn't end the halt), not at a later start from the same server, so its halt stays while the lake holds its Delta table: drop or move that table (the next start clears the halt), or add the table to the config and copy it again |
 | `SNAPSHOT_TIMEOUT` | the table's snapshot ran past `source.timeouts.snapshot` | raise the timeout |
+| `COPY_CANCELLED` | a snapshot job that was bringing the table back from a halt was cancelled before its copy completed (the detail names the job): the Delta table holds part of the table's rows | copy the table again (step 3), or retry the job (`POST /v1/snapshots/{job_id}/retry`, see [Snapshot jobs](/data-avalanche/runbooks/snapshot-jobs/#retry-a-job)) |
 
 A config fix goes through `POST /v1/config/reload`; most of these keys restart the pipeline.
 
````

`docs/src/content/docs/runbooks/snapshot-jobs.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/snapshot-jobs.md b/docs/src/content/docs/runbooks/snapshot-jobs.md
index 4d8961b..71986fe 100644
--- a/docs/src/content/docs/runbooks/snapshot-jobs.md
+++ b/docs/src/content/docs/runbooks/snapshot-jobs.md
@@ -1,6 +1,6 @@
 ---
 title: Snapshot jobs
-description: Copying tables on demand through the API, following a job, and what a job does and doesn't do.
+description: Copying tables on demand through the API, following, cancelling and retrying a job, and what a job does and doesn't do.
 sidebar:
   order: 6
 ---
@@ -43,8 +43,9 @@ move the Delta table, then ask the job again.
 ## Follow it
 
 ```sh
-curl -s https://cdc.example.internal/v1/snapshots/shop~0192f0c4-7b9e-7c3a-9d5e-2f0a1b3c4d5e
+curl -s https://cdc.example.internal/v1/snapshots/shop~0192f0c47b9e7c3a9d5e2f0a1b3c4d5e
 curl -s https://cdc.example.internal/v1/pipelines/shop/snapshots
+curl -s 'https://cdc.example.internal/v1/snapshots?status=running,queued'
 ```
 
 A job's status, and each of its tables', is kept in the pipeline's state: it survives restarts and
@@ -52,6 +53,69 @@ failover, and a job interrupted by a crash goes on from its last chunk. A pipeli
 newest ended jobs (completed, failed or cancelled); older ones are deleted as each job ends. A
 job document the node can't read is left out of the list, with a warning in the log.
 
+`GET /v1/snapshots` lists every pipeline's jobs, newest first, filtered by `status` (`queued`,
+`running`, `cancelling`, `completed`, `failed`, `cancelled`; several, comma-separated), `pipeline`
+and `by` (who asked). A pipeline whose jobs can't be read is named under `unreadable`.
+
+For each table a job shows its rows copied, the source's estimate of its rows when the copy began
+(PostgreSQL's `pg_class.reltuples`, which is `-1`, so absent, until the table was analyzed;
+MySQL's `information_schema.TABLES.TABLE_ROWS`, approximate on InnoDB), when the copy began and
+ended, the halt the job cleared for it (`from_halt`), and what lake readers see of it:
+
+| `lake` | Lake readers see |
+|---|---|
+| `complete` | the table, copied |
+| `as_before` | the rows it held before the job: the copy writes over them, or hasn't begun |
+| `empty` | nothing: a truncating job emptied it, and its copy hasn't written a row yet |
+| `partial` | part of the rows: a truncating job emptied it, and its copy didn't complete |
+
+After a minute, a running job shows its `rate` (rows a second since its first table began) and
+`eta_seconds`, from the rows its unended tables' estimates leave: an estimate moves, and so does
+the ETA. Without an estimate for each of them, there's no ETA.
+
+## Cancel a job
+
+```sh
+curl -s -X POST https://cdc.example.internal/v1/snapshots/shop~0192f0c47b9e7c3a9d5e2f0a1b3c4d5e/cancel \
+  -H 'content-type: application/json' -d '{"note": "the wrong table"}'
+```
+
+- `202`: the job reads `cancelling`. The pipeline's owner restarts its worker, which ends the job's
+  tables that haven't ended: their copy stops, and they read `cancelled`. A table whose copy had
+  completed stays copied. A paused pipeline restarts too, and stays paused.
+- A stopped pipeline, or one whose lease no node holds, runs no worker: the job reads `cancelling`
+  until the pipeline's next start ends it (`"restart": false` in the answer).
+- `200`: it was cancelled already. `409`: it has ended.
+
+What the cancelled tables hold:
+
+- A table the config replicates streams on. A copy that doesn't truncate leaves its rows as they
+  were, partly refreshed; one that truncates leaves the table empty or partly copied until a new
+  job copies it.
+- A table the job was bringing back from a halt halts again, `COPY_CANCELLED`, naming the job: its
+  Delta table holds part of its rows. Copy it again, or retry the job.
+- A table the config doesn't replicate isn't read again: it keeps the rows the copy wrote.
+
+The cancel is recorded in the events (`action.snapshot.cancel`, with the note), and the job's end
+too (`job.cancelled`).
+
+## Retry a job
+
+```sh
+curl -s -X POST https://cdc.example.internal/v1/snapshots/shop~0192f0c47b9e7c3a9d5e2f0a1b3c4d5e/retry \
+  -H 'content-type: application/json' -d '{"tables": "not_copied"}'
+```
+
+A retry starts a new job of an ended job's tables, as `POST /v1/pipelines/{id}/snapshots` does:
+
+- `tables`: `not_copied` (the default) for the tables that didn't complete, `all` for every one.
+- `truncate`: the old job's unless given.
+- `note`: why, kept with the new job and its event.
+
+The new job records the one it retries (`retry_of`). `409`: the job hasn't ended, it left no table
+to copy, or, as for any job, the pipeline doesn't run, no node holds its lease, or an unfinished
+job holds one of the tables.
+
 ## What a job does
 
 - Copies each table from its first row, whatever the table's `snapshots.mode`.
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cd docs && npm ci && npm run build`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add docs/src/content/docs/deployment/events.md docs/src/content/docs/runbooks/halted-table.md docs/src/content/docs/runbooks/snapshot-jobs.md
git commit -m "docs: cancel and retry a snapshot job, every pipeline's jobs, a job's figures; COPY_CANCELLED"
```

---

### Task 9: A cancel and a retry, end to end (E2E)

**Files:**
- Modify: `crates/data-avalanche/tests/e2e/jobs.rs`

**Interfaces:**
- Consumes: the E2E harness (`E2e`, `Proc::{call, send}`, `eventually`); Tasks 1–6.
- Produces: `a_cancelled_copy_halts_its_table_and_a_retry_copies_it`; `ended` also ends at
  `cancelled`.

- [ ] **Step 1: Write the failing tests**

The tests: `a_cancelled_copy_halts_its_table_and_a_retry_copies_it`.

`crates/data-avalanche/tests/e2e/jobs.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/jobs.rs b/crates/data-avalanche/tests/e2e/jobs.rs
index 349d6a8..a3cbe7d 100644
--- a/crates/data-avalanche/tests/e2e/jobs.rs
+++ b/crates/data-avalanche/tests/e2e/jobs.rs
@@ -1,6 +1,6 @@
 //! The on-demand snapshot jobs (M9b): a job asked through the API reads a streamed table again
 //! and copies one outside the config, clears a dropped table's halt, and outlives a kill of the
-//! process that ran it.
+//! process that ran it. A job is cancelled and retried (M13c).
 
 use std::cell::Cell;
 use std::time::Duration;
@@ -42,7 +42,7 @@ async fn ended(p: &Proc, id: &str) -> Value {
         Duration::from_secs(120),
         async || {
             let status = p.call("GET", &path).await.1["status"].clone();
-            status == "completed" || status == "failed"
+            status == "completed" || status == "failed" || status == "cancelled"
         },
     )
     .await;
@@ -241,3 +241,143 @@ async fn a_job_outlives_a_kill_of_its_process() {
     assert!(p.terminate().await.success());
     e.cleanup().await;
 }
+
+/// `POST path` with JSON `body`: the code and the answer.
+async fn post(p: &Proc, path: &str, body: &str) -> (u16, Value) {
+    let (code, _, text) = p
+        .send(None, "POST", path, body)
+        .await
+        .expect("the API answers");
+    (
+        code,
+        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}")),
+    )
+}
+
+/// D-M13c.2: a job bringing a dropped table back is cancelled partway, its pipeline paused: the
+/// restart's start ends it, and the table halts again, `COPY_CANCELLED`; the pipeline stays
+/// paused. The cancel and the job's end are in the events. Resumed, a retry of the job copies the
+/// table and clears the halt; `GET /v1/snapshots` lists both jobs, the retry first, with the
+/// source's row estimate.
+#[tokio::test]
+async fn a_cancelled_copy_halts_its_table_and_a_retry_copies_it() {
+    require_it!();
+    let e = E2e::new(
+        "jobs_cancel",
+        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
+         CREATE TABLE gone (id bigint PRIMARY KEY, doc text);",
+        Options {
+            tables: ", snapshots: { mode: never, chunk_size: 10 }",
+            pipeline: "schema_evolution: { rescan_interval: 2s }",
+            ..Options::default()
+        },
+    )
+    .await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.streaming().await;
+    e.exec("DROP TABLE gone; CREATE TABLE gone (id bigint PRIMARY KEY, doc text)")
+        .await;
+    e.insert_series("gone", 1_000, Doc::Row).await;
+    e.exec("ANALYZE gone").await;
+    eventually("gone halts", Duration::from_secs(60), async || {
+        e.halts()
+            .await
+            .tables
+            .iter()
+            .any(|h| h.table.name == "gone")
+    })
+    .await;
+
+    let (code, v) = ask(&p, &e, &["gone"]).await;
+    assert_eq!(code, 202, "{v}");
+    let id = v["job_id"].as_str().unwrap().to_owned();
+    let path = format!("/v1/snapshots/{id}");
+    eventually(
+        "the copy runs, partway",
+        Duration::from_secs(60),
+        async || {
+            let job = p.call("GET", &path).await.1;
+            let n = job["tables"][0]["rows_done"].as_u64().unwrap_or(0);
+            job["status"] == "running" && n > 0 && n < 1_000
+        },
+    )
+    .await;
+    let job = p.call("GET", &path).await.1;
+    assert_eq!(job["tables"][0]["from_halt"], "TABLE_DROPPED", "{job}");
+    assert!(job["tables"][0]["started_at"].is_string(), "{job}");
+    let pipeline = format!("/v1/pipelines/{}", e.test);
+    let (code, v) = post(&p, &format!("{pipeline}/pause"), "").await;
+    assert_eq!(code, 202, "{v}");
+
+    let (code, v) = post(
+        &p,
+        &format!("{path}/cancel"),
+        r#"{"note": "the wrong table"}"#,
+    )
+    .await;
+    assert_eq!(code, 202, "{v}");
+    assert_eq!(
+        v["restart"], true,
+        "a paused pipeline's worker restarts: {v}"
+    );
+    let job = ended(&p, &id).await;
+    assert_eq!(job["status"], "cancelled", "{job}");
+    assert_eq!(job["cancel"]["note"], "the wrong table", "{job}");
+    let halts = e.halts().await.tables;
+    let halt = halts
+        .iter()
+        .find(|h| h.table.name == "gone")
+        .expect("gone halts again");
+    assert_eq!(halt.reason.code(), "COPY_CANCELLED", "{halts:?}");
+    let (_, status) = p.call("GET", &pipeline).await;
+    assert_eq!(status["desired_state"], "paused", "{status}");
+    let events = format!("/v1/events?object={id}");
+    eventually(
+        "the cancel and the end are recorded",
+        Duration::from_secs(30),
+        async || {
+            let kinds: Vec<String> = p.call("GET", &events).await.1["events"]
+                .as_array()
+                .into_iter()
+                .flatten()
+                .map(|e| e["kind"].as_str().unwrap_or_default().to_owned())
+                .collect();
+            kinds.contains(&"action.snapshot.cancel".to_owned())
+                && kinds.contains(&"job.cancelled".to_owned())
+        },
+    )
+    .await;
+
+    let (code, v) = post(&p, &format!("{pipeline}/resume"), "").await;
+    assert_eq!(code, 202, "{v}");
+    let (code, v) = post(&p, &format!("{path}/retry"), "").await;
+    assert_eq!(code, 202, "{v}");
+    assert_eq!(v["retry_of"], id.as_str());
+    let retry = v["job_id"].as_str().unwrap().to_owned();
+    let job = ended(&p, &retry).await;
+    assert_eq!(job["status"], "completed", "{job}");
+    assert_eq!(job["tables"][0]["rows_done"], 1_000, "{job}");
+    assert_eq!(job["tables"][0]["estimate"], 1_000, "analyzed: {job}");
+    assert_eq!(job["tables"][0]["lake"], "complete", "{job}");
+    assert!(e.halts().await.tables.is_empty(), "the halt is cleared");
+    e.exec("INSERT INTO gone VALUES (1001, 'streamed')").await;
+    e.converged("gone").await;
+
+    let (code, all) = p
+        .call("GET", &format!("/v1/snapshots?pipeline={}", e.test))
+        .await;
+    assert_eq!(code, 200, "{all}");
+    let listed: Vec<(&str, &str)> = all["jobs"]
+        .as_array()
+        .unwrap()
+        .iter()
+        .map(|j| (j["job_id"].as_str().unwrap(), j["status"].as_str().unwrap()))
+        .collect();
+    assert_eq!(
+        listed,
+        [(retry.as_str(), "completed"), (id.as_str(), "cancelled")]
+    );
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
````

- [ ] **Step 2: Run them to see them fail**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e jobs:: -- --test-threads=1`
Expected: none: the behaviour landed in Tasks 1–6, and the scenario passes at once (48 s at the plan's verification, three runs of three).

- [ ] **Step 3: Implement**

Nothing: the behaviour landed in the earlier tasks.

- [ ] **Step 4: Run the tests and the gates**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e jobs:: -- --test-threads=1`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests/e2e/jobs.rs
git commit -m "test(e2e): a cancelled copy halts its table, and a retry copies it"
```

---
## Exit

- Every task committed, one commit each, after the spec patch's commit; the gates pass on the last
  one (`cargo fmt --all --check`, both clippy runs, `cargo test --workspace`).
- With the emulators up: the sources' `row_estimates` tests, the storage conformance (RustFS,
  Azurite), and the whole E2E suite on PostgreSQL 18 (`DATA_AVALANCHE_IT=1
  DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e --
  --test-threads=1`) pass.
- `cd docs && npm run build` passes its link check.
- At the plan's verification (the code above, replayed on `a843d4d`): `cargo test --workspace` 1665 passed; the E2E suite on PostgreSQL 18, 54 scenarios, passed (676 s); the sources' estimates on PostgreSQL 18 and MySQL 8.4 and the storage conformance on RustFS and Azurite passed; the site built (30 pages, link check clean).
- Then, on Oleksii's request: the whole-branch review and its fixes, the push, the PR.

## Deviations during execution

Each was found by the task's review and fixed in its fix round, unless it says otherwise. Later
tasks' blocks applied on top unchanged.

- **Task 3** (fix round, `e4bb58b`, approved by Oleksii): the start cleared a table's halt and
  recorded the job only later. A crash in between lost the cleared halt: the restart found no halt
  and the job still queued, so the job's table ran with no `from_halt`, and a cancel of it, before
  or after that restart, left the table unhalted with part of its rows. The start now notes the
  halt it clears on the job's queued table before the clear (`Jobs::note_cleared`, idempotent);
  `record` keeps a noted `from_halt`; `cancelling()` also takes a cancelled job's queued table
  with a noted halt, which halts `COPY_CANCELLED` as a running one does. Three tests added (a
  restart after the crash, a cancel before it, the note written once).
- **Task 5** (fix round, `02e30a5`): a job's `estimate` summed the estimates its tables had, so a
  job with a table the source couldn't estimate showed a total short of it (Review Focus 5: no
  figure rather than a wrong one). It is now the sum only when every table has an estimate, as
  `eta_seconds`; one test added (`a_job_s_estimate_is_none_unless_every_table_has_one`), at the
  end of `snapshots.rs`'s tests, so Task 6's second `snapshots.rs` hunk appends its tests after
  it.
