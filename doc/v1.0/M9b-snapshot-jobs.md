---
updated: 2026-10-27
domain: Architecture
tags: [v1, implementation-plan, m9b, api, snapshot, on-demand-snapshot, snapshot-jobs, halt, re-snapshot, cluster, e2e]
resolves: [DA-SCHEMA-10]
---

# M9b — On-Demand Snapshot Jobs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The on-demand snapshot jobs of System-Overview's endpoint table and Data-Flow §On-Demand
Snapshot, the second half of M9 (Q-M9-A):
- `POST /v1/pipelines/{id}/snapshots` asks for a job: exact table names, no globs; `202` with the
  job's id;
- a job reads each table again from its first row, alongside the live stream;
- a job of a halted table clears the halt and baselines the table to the catalog (the operator's
  re-snapshot of Schema_Change §8.2 and D-IMPL-13);
- a job's table outside the pipeline's config is copied into the lake and not streamed;
- jobs are persisted under `snapshots/jobs/`, listed by `GET /v1/pipelines/{id}/snapshots`, read
  by `GET /v1/snapshots/{job_id}` on any node (Q-M9-2), and a running job goes on after a restart
  or a failover from its tables' chunk cursors (Q-M9-1).

**Architecture:** A job is asked through storage, like M9a's restart (Q-M9-B), and runs in a new
worker's start (Q-M9b-1):

- **The documents** (Task 1, `data-avalanche-state`): `snapshots/jobs/<job_id>.json` gets a status
  per table (`queued`, `running`, `completed`, `failed`, with an error) and derives the job's
  status from them (`settle`). `update_snapshot_job` is its read-modify-write;
  `unfinished_snapshot_jobs` lists the jobs not ended yet.
- **The sources** (Task 2): `SourceConnector::describe_tables` describes tables by exact name,
  whatever the config's patterns include, for the tables outside the config. MySQL's channel C
  takes a column hint as a baseline to the catalog (the whole table); it refused it, so a fresh
  start over a drifted MySQL table halted it (DA-SCHEMA-10, found while planning).
- **The worker's start** (Tasks 3–4, `data-avalanche-pipeline`, Phase 0): it reads the unfinished
  jobs. A queued job table is snapshotted from its first row, whatever its snapshot mode. A halted
  one is baselined to the catalog first and its halt cleared. A running job table whose snapshot
  is in progress resumes from its cursor. A table outside the config is a **copy**: described by
  name, its sink table ensured, read like a dump, never streamed, its schema cached nowhere.
  Phase 0 records each queued table as running or failed.
- **The owner** (Task 5, `data-avalanche-cluster`): the controller lists the unfinished jobs when
  its term starts and when it takes a restart. At each poll it follows each running job table to
  its end, from the table's snapshot document and `halts.json`, and settles the job.
- **The API** (Task 6, `data-avalanche-api`): the three routes. A job is written `queued`, then a
  restart is requested under the job's id; the owner takes it at its next poll and runs a new
  worker, whose start takes the job. Refused (`409`) unless the desired state is running and a
  node holds the lease (Q-M9b-4), or while an unfinished job holds one of its tables.
- **The E2E suite** (Task 7): a streamed table read again and one outside the config copied; a
  dropped table's halt cleared by a job; a job outliving a `kill -9` of its process.

**Tech Stack:** Rust 1.98.1 (edition 2024). No new crate. `uuid`'s `v7` feature is added to the
workspace pin (the job id's suffix); `Cargo.lock` doesn't change.

**Spec:**
- `doc/v1.0/00-roadmap.md`: §3.5 (the state documents), §3.6 (the source trait), §3.7 (the
  controller), §3.11 (the worker), §4 M9 (scope: "snapshot jobs (on-demand chunk loop; jobs
  persisted under `snapshots/jobs/`, listed via `GET /v1/pipelines/{id}/snapshots` after a
  restart or failover, running jobs resumed by the owner — Q-M9-1)"), §9.1 Q-M9-2.
- `doc/specs/System-Overview.md`: §Data snapshotting "on request via management REST API" (the
  tables list without globs; alongside the live stream; tables not in the config snapshotted, not
  streamed); §Management API endpoints, the three snapshot rows.
- `doc/specs/Data-Flow.md`: §On-Demand Snapshot Flow; §State Layout, `snapshots/jobs/<job_id>.json`.
- `doc/specs/Schema_Change_Detection_and_Evolution.md`: §7.2 (the halts that need an operator
  re-snapshot: an identifier change, `TABLE_DROPPED`, `UNSAFE_DDL`), §8.2 (baseline the target to
  the queried schema and re-snapshot).
- `doc/research/Snapshot_Resumability_and_Consistency.md`: §1 the chunk loop, §2 the cursor, §4
  the on-demand snapshot (no stream pause; idempotent upserts).
- `doc/research/Pipeline_Lifecycle_State_Machine.md`: §3 restart (desired `RUNNING`, `ERROR`
  included), §5 routing through storage.
- `doc/research/API_RBAC_and_Versioning.md` §2: request snapshot operator; job status and list
  viewer.
- `doc/v1.0/M9a-management-api.md`: D-M9a.2 (routing through storage), D-M9a.4 and D-M9a.5 (the
  restart request, taken by id at the owner's poll), D-M9a.9 (the route table and the OpenAPI
  snapshot).
- `doc/v1.0/M6a-pipeline-worker.md`: D-M6a.1 (a persisted halt keeps the table halted at every
  start "until M9's re-snapshot"), Phase 0's plan and decisions.

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`; the source crates implement `core`'s trait. `testkit` is a
  dev-dependency only.
- **Through storage, no proxy** (Q-M9-B): the API writes the job and the restart request; only the
  lease holder's worker and controller act on them; any node reads a job by its id.
- **The table names a request takes are the per-table config keys** (config §9.3, the
  `snapshots.overrides` row; V-033, V-064): PostgreSQL `schema.table`, MySQL the bare table name
  of `source.db`. No glob (`*`), System-Overview: "non-empty tables list without globs".
- **No table is truncated, no sink table dropped** (System-Overview OQ-M2: "The app never deletes
  sink tables"; Q-M9b-2): a job upserts.
- **Time in tests:** the pipeline's and the controller's tests run on tokio's paused clock
  (`start_paused = true`); the API's tests and the E2E suite run in real time.
- **Test gates:**
  - Plain `cargo test` stays hermetic: the state documents use `InMemory`, the worker and the
    controller the testkit's fake source and in-memory sink.
  - The source ITs need `DATA_AVALANCHE_IT=1` (`test-infra/up.sh`); the MySQL ones run on the
    server `DATA_AVALANCHE_MYSQL` names.
  - The E2E suite needs `DATA_AVALANCHE_IT=1` and the `e2e-hooks` feature, runs with
    `--test-threads=1`, and runs on either source (`DATA_AVALANCHE_E2E_SOURCE`).
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `thiserror` types; no new error type is needed.
- Code blocks are the verified content, already `rustfmt`-formatted. Every block is a `git
  apply`-ready diff, one per file; a step's diffs apply in the order given, after the earlier
  steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt a person running snapshot jobs in production. Each is
pinned by a test in the task after the arrow.

1. **The process dies while a job reads** (a crash, an OOM kill, a failover). The next owner must
   list the job, resume its tables from their chunk cursors rather than start over or forget them,
   and end the job → Task 3 (`a_running_job_table_resumes_from_its_cursor`), Task 4
   (`a_running_copy_resumes_from_its_cursor`), Task 5 (`a_new_owner_follows_the_jobs_it_finds`),
   Task 7 (`a_job_outlives_a_kill_of_its_process`).
2. **A client retries the POST after losing the answer** (a timeout behind a load balancer).
   A second job of the same tables must not start beside the first: the retry gets `409` naming
   the job that holds them → Task 6 (`a_table_an_unfinished_job_holds_is_refused`).
3. **A halted table whose new shape can't be taken** (the key changed again, a type narrowed, the
   sink refuses it). The job must fail with the halt's code and reason, and the table stay halted,
   not stream half-converted → Task 3 (`a_job_table_that_halts_at_the_start_fails`); and a
   MySQL table baselined without a DDL statement must evolve, not halt → Task 2
   (`a_baseline_takes_the_whole_table_from_the_catalog`), Task 7
   (`a_job_clears_a_dropped_table_s_halt`, on every MySQL target).
4. **A table outside the config changes during its copy, or after.** Its changes must never be
   streamed into the lake later, by this run or a restart; a copy is a point-in-time read → Task 4
   (`a_table_outside_the_config_is_copied_and_not_streamed`), Task 7.
5. **A typo, or a table dropped while its copy runs.** The job must fail that table with the
   reason (`no such table`), finish the others, and record no halt the operator then has to clear
   → Task 4 (`a_table_the_source_lacks_fails_in_its_job`), Task 6
   (`a_request_names_its_tables_exactly`, for the names that can't be a table at all).

## Decisions and rulings

**Asked and answered** (2026-10-27; three "Recommended", Q-M9b-4 not):

| # | Question | Answer |
|---|---|---|
| Q-M9b-1 | How does a job reach the running worker? | **Restart the worker.** The API writes the job, then a restart request under the job's id; the owner takes it at its poll, and the new worker's Phase 0 takes the job's tables. No new command or document; `requests.json` is unchanged |
| Q-M9b-2 | Does a job truncate the sink table first? | **Upsert only, documented.** No truncate: a row deleted at the source while the table wasn't streamed stays in the lake. The remedy is to drop or move the Delta table first (a new gap, DA-SNAP-15) |
| Q-M9b-3 | What is a table outside the config? | **A copy, not streamed.** Described by exact name, its sink table ensured, read once like a dump; its schema is cached nowhere; it never joins the stream |
| Q-M9b-4 | Which pipelines take a job? | **`409` unless running**, as a restart (D-M9a.4): the desired state must be running and a node must hold the lease. `ERROR` is accepted: the job's restart runs the pipeline again |

**Rulings where the plan decides** (each pinned by a test):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M9b.1 | **The job id** is `<pipeline id>~<suffix>`, the suffix a UUID v7 in 32 lowercase hex digits. V-055 keeps `~` out of a pipeline id, so `GET /v1/snapshots/{job_id}` splits at the first `~` and reads that pipeline's state; a malformed id, or an unknown pipeline, is `404` | Q-M9-2: any node reads a job from storage. A v7 id sorts by time, as the "ulid-like" of Q-M9-2 asks, with the `uuid` crate already in the workspace | Task 6 (`a_job_is_found_by_its_id`) |
| D-M9b.2 | **The job document.** Each table has `status` (`queued`, `running`, `completed`, `failed`), `rows_done` and an `error`. `settle` derives the job's status: queued while every table is, running while one is queued or running, then completed when all completed, else failed. When it ends, `finished_at` is set and, if a table failed, `error` = `"<n> of <m> tables failed: <table> (<error>), …"`. An ended job never changes. The per-table status was `SnapshotStatus`; the schema version stays 1, since nothing wrote a job document before M9b | Data-Flow §State Layout names the fields ("tables, status, per-table progress, requested_at/by, finished_at"); a table's job outcome isn't its snapshot's (a table that failed in a job has no `failed` snapshot status) | Task 1 (all three tests) |
| D-M9b.3 | **A job's start** (Q-M9b-1). The API creates the job (`queued`), then requests a restart whose id is the job's. The owner lists the unfinished jobs when its term starts, when it takes a restart (before it discards the worker) and at a reload's restart; a failed listing is retried at the next poll. The new worker's Phase 0 takes the queued tables. This is M9a's restart: unflushed data is discarded and replayed from the confirmed position (Pipeline_Lifecycle §3), and the stream goes on from there. If the restart request can't be written after the job is, the answer is `503` with the job's id: the job runs at the pipeline's next start, and a retry is `409` naming it | A worker's tables are fixed at its start: a job taken by a running worker would need a new command and a mid-session Phase 0. The restart is proven (M9a), keeps every Phase 0 rule in one place, and the job's record survives the window before the owner's poll | Task 5 (`a_job_asked_runs_to_its_end`), Task 6 (`a_job_is_recorded_then_asked_of_the_holder`) |
| D-M9b.4 | **What the ask refuses** (Q-M9b-4). `409` unless the desired state is running and a node holds the lease (an unreleased lease), like a restart; a pipeline in `ERROR` is accepted. `409` while an unfinished job has one of the tables queued or running, naming that job. `422` for a body that isn't `{"tables": [...]}` (an unknown field, so a `truncate`, is refused), names no table, has a `*`, a PostgreSQL name without its schema, or a MySQL name with a `.`. A name given twice is taken once, in order. `404` for an unknown pipeline, `503` while the node starts. Two asks on two nodes at once can both pass the check: both jobs run, the same rows upserted twice | A job is a restart: a paused or stopped pipeline must not run. The `409` on a held table turns a client's retry into an answer that names the first job, not a second snapshot | Task 6 (`a_job_of_a_paused_or_stopped_pipeline_is_refused`, `a_table_an_unfinished_job_holds_is_refused`, `a_request_names_its_tables_exactly`) |
| D-M9b.5 | **Phase 0 and a job's table** (Task 3). A queued job table is snapshotted from its first row (`Snapshot { resume: false }`, the dump flag its mode's), whatever its snapshot mode decides. A running job table whose snapshot is in progress resumes from its cursor (`Snapshot { resume: !gap }`: after a gap it starts over, D-M6a). One no longer in progress goes by its mode, and the owner settles it from its snapshot document. Phase 0 records each queued table it requested as running, and each one it halted as failed, with `"<CODE>: <reason>"`; the halt stays | A job's `rows_done` must survive a crash, so a running job keeps its tables in the snapshot through any number of restarts. A table that can't start is the job's failure, visible in the job, and the halt is the table's | Task 3 (all four tests) |
| D-M9b.6 | **A halted table a job asks for** (Task 3). A recorded halt of a queued job table that the catalog still resolves is cleared: its cached schema takes the catalog's source id (`with_source_id`: a table created again is the table the catalog has now), then is baselined. The change from the cached schema to the catalog's is planned and evolved as at a fresh start (Schema_Change §8.2), and the new schema cached. The source-id `TABLE_DROPPED` check is skipped for a queued job table. Its halt is cleared after the schemas are cached, so a restart in between finds the table halted and asked for still, and baselines it again. An unsafe change halts it again, and the job fails it. The persist order: halts, snapshot documents, the abandoned ones, schemas, the cleared halts, the job record, the checkpoint | D-M6a.1 keeps a persisted halt "until M9's re-snapshot", and Schema_Change §7.2 makes the operator's re-snapshot the way out of `TABLE_DROPPED`, an identifier change and `UNSAFE_DDL` | Task 3 (`a_job_clears_a_halt_and_baselines_the_table`, `a_job_table_that_halts_at_the_start_fails`), Task 7 (`a_job_clears_a_dropped_table_s_halt`) |
| D-M9b.7 | **A job upserts** (Q-M9b-2). The sink table isn't truncated, and the request has no option for it. A row deleted at the source while the table was halted or not streamed stays in the lake. To start clean, the operator drops or moves the Delta table before asking; the job then creates it | Truncating means a metadata-only delete before the first chunk, reconciled with the stream's concurrent changes; the app never deletes sink data on its own (System-Overview OQ-M2) | Task 7 (`a_job_clears_a_dropped_table_s_halt`: the old row stays), Task 6 (`truncate` is `422`) |
| D-M9b.8 | **A table outside the config is a copy** (Q-M9b-3, Task 4). The job tables that are neither resolved nor halted by the config are described by name (`describe_tables`). One the source skips (no such table, a partitioned parent, another database) or halts fails in its job, and no halt is recorded. A copy's sink table is ensured when absent (a refusal fails it, `SINK_REJECTED`). A queued copy starts from its first row; a running one resumes from its cursor; one no longer in progress is left to the owner. In the session a copy is added to the accumulator and read as a dump, retired once durable. Its schema is kept apart from the streamed tables', so the stream drops its changes, and it is cached nowhere: it is described again at every start while it runs. A copy that halts in the session records a halt (it fails its job), and a later job asking for it clears it | System-Overview: "may contain table(s) which isn't present in the pipeline config - do snapshot but don't stream data". Without the stream there is nothing to reconcile, so a copy is a read of each chunk as of its read, not of one instant | Task 4 (all three tests), Task 7 (`a_job_reads_a_table_again_and_copies_one_outside_the_config`) |
| D-M9b.9 | **The owner follows the jobs** (Task 5). At each poll the controller reads `halts.json` once, then for each job it follows, each running table's progress: a halt fails it with `"<CODE>: <reason>"`; an in-progress snapshot gives `rows_done` from its cursor; a completed one completes it with `row_count`; anything else fails it ("its snapshot stopped before it completed"). A job is written only when it changed; one that ended is logged (`snapshot job ended`) and followed no more. A halt reaches `halts.json` with the session's next flush, so a job's failure shows then | The worker records only the start (D-M9b.5). The owner holds the lease and polls already, and the snapshot documents are the durable truth across workers and owners | Task 5 (all four tests) |
| D-M9b.10 | **Tables by name** (Task 2). `SourceConnector::describe_tables(&[TableRef])` describes the tables in the order given, whatever the config's patterns include, and skips the config's table-set checks. PostgreSQL: a missing table is skipped `no such table`, a partitioned parent skipped as `select` skips it, an unlogged table halted. MySQL: a table of another database is skipped (`not in source.db <db>`), a missing one skipped, a system-versioned one halted. Both halt a table without an identifier (`NO_IDENTIFIER`). **MySQL channel C** takes a `SchemaHint::Columns` as a baseline to the catalog: the whole table, no rename proven (`Scope::Table`) | Phase 0 needs a copy's schema, and the pattern resolver can't find a table the patterns exclude. The MySQL refusal (`a column hint on a MySQL source`) halted every MySQL baseline with a column change `UNSAFE_DDL`, a fresh start's (§8.2) included: DA-SCHEMA-10, found by this plan's E2E on MySQL | Task 2 (all six tests), Task 7 on every MySQL target |
| D-M9b.11 | **The routes** (Task 6), in M9a's route table, so the router, the role gate and the OpenAPI document take them at once: `POST /v1/pipelines/{id}/snapshots` (operator) answers `202` `{job_id, status, tables}`; `GET /v1/pipelines/{id}/snapshots` (viewer) `{"jobs": [...]}`, oldest request first; `GET /v1/snapshots/{job_id}` (viewer) the job: `{job_id, pipeline, status, tables: [{table, status, rows_done, error}], requested_at, requested_by, finished_at, error}`. Tables are named as the request names them. Each ask is logged with its caller. The OpenAPI document gains the `job_id` path parameter | API_RBAC §2's roles; D-M9a.9's one table | Task 6 (all five tests; the matrix and the OpenAPI snapshot tests) |

**Measured while planning** (2026-10-27, recorded in the tasks):

- Every task was built in a scratch workspace on this branch's base, test first. Each task's test
  stage was committed on its own and run (Step 2's output), then its implementation (Step 4's).
- The plan's code blocks were replayed onto the base with `git apply` and compared with the
  scratch workspace: the 44 blocks apply in order and give a tree identical to the workspace's last commit.
- Mutation checks, each run against its test:
  - Task 1: a job counted running only while a table is queued (not while one runs) fails `a_job_json_shape`.
  - Task 2: without the `read_change` change, `a_baseline_takes_the_whole_table_from_the_catalog` fails (`a column hint on a MySQL source`), and so does the E2E halt scenario on every MySQL target.
  - Task 3: the source-id `TABLE_DROPPED` check kept for an asked table, or the cleared halt left in `halts.json`, fails `a_job_clears_a_halt_and_baselines_the_table`.
  - Task 4: a copy's schema put among the streamed tables' fails `a_table_outside_the_config_is_copied_and_not_streamed` (the change made during the copy reaches the lake); a running copy the source lost not failed fails `a_table_the_source_lacks_fails_in_its_job`.
  - Task 5: no listing on a taken restart fails `a_job_asked_runs_to_its_end` and `a_job_table_that_halts_fails_the_job`; a halt not read as a failure fails `a_running_table_s_progress_is_its_documents` and `a_job_table_that_halts_fails_the_job`.
  - Task 6: no lease check fails `a_job_is_recorded_then_asked_of_the_holder`; no check of the unfinished jobs' tables fails `a_table_an_unfinished_job_holds_is_refused`.
  - Task 7: the kill scenario waited only for `rows_done > 0` while planning, which a job that had already ended would pass; it now waits for a `running` job partway through.
- The integration tests: the describe ITs pass on PostgreSQL 18 and on `mysql-84`,
  `mysql-84-nogtid` and `mariadb-118`, and the MySQL baseline IT on the three MySQL targets.
- The E2E scenarios (Task 7):
  - The three job scenarios pass on PostgreSQL 18 (77 s), `mysql-84` (78 s), `mysql-84-nogtid`
    (75 s) and `mariadb-118` (71 s). Before the MySQL fix (D-M9b.10), the halt scenario failed on
    all three MySQL targets: `UNSAFE_DDL: … the catalog read for a schema change failed: gone: a
    column hint on a MySQL source`.
  - The kill scenario kills the process while the job is `running` with `0 < rows_done < 1000`;
    it passed three runs out of three on PostgreSQL 18 (56–58 s each).
  - The whole E2E suite on PostgreSQL 18: 45 of 45 (665 s).
  - The workspace: 1257 tests at the base, 1282 after Task 6.

**Known edges, accepted** (each in the gap tracker or the deviations once found otherwise):

- A DDL inside a transaction, replayed after the checkpoint of the job's start, can find the
  cleared table's old shape and halt it again: the job fails it, and the operator asks again.
- A copy whose sink table exists with another schema is refused at its first commit
  (`SINK_REJECTED`): the job fails it, and the halt is recorded; drop or move the table and ask
  again (as DA-SNAP-13).
- A job's failure from a halt shows with the session's next flush (D-M9b.9) (DA-SNAP-17).
- Job documents accumulate: nothing removes an ended one, and the list and the ask's `409` check
  read every one (DA-SNAP-16).
- A job upserts (D-M9b.7, DA-SNAP-15).

**Documents this plan changes:**

- Applied at plan approval, in an `infra:`, a `research:` and a `gap:` commit:
  - Roadmap:
    - §3.5: `JobTableStatus`, `JobTable { table, status, rows_done, error }`,
      `SnapshotJobDoc::{new, is_finished, settle}`, `PipelineDocs::{update_snapshot_job,
      unfinished_snapshot_jobs}` (D-M9b.2).
    - §3.6: `describe_tables` (D-M9b.10); MySQL's channel C takes a column hint as a baseline.
    - §3.7: the controller lists the unfinished jobs and follows them (D-M9b.3, D-M9b.9).
    - §3.11: Phase 0 takes a job's tables, clears a halt, copies a table outside the config
      (D-M9b.5, D-M9b.6, D-M9b.8).
    - §3.12: the snapshot routes (D-M9b.11).
    - §4 M9: M9b's scope as built.
    - §9: D-IMPL-20 for D-M9b.1…11; §9.1 Q-M9b-1…4.
  - `Snapshot_Resumability_and_Consistency.md` §4, as built: the job's restart, a copy (no
    reconciliation), a job upserts.
  - `Pipeline_Lifecycle_State_Machine.md` §3 and §5: a job is asked as a restart and refused as one.
  - `API_RBAC_and_Versioning.md` §2: the three rows' answers.
  - `doc/v1.0/README.md`: the M9b row.
  - `CDC_Design_Gap_Analysis_v1.md`, §42 for M9b's plan:
    - DA-SCHEMA-10 🆕 (MySQL's baseline halted a drifted table), fixed by Task 2; ✅ when M9b merges;
    - DA-SNAP-15 🆕 (a job upserts: ghost rows), DA-SNAP-16 🆕 (job documents accumulate),
      DA-SNAP-17 🆕 (a job's failure shows with the next flush).
- **Proposed spec patches, for approval** (`spec:` commit, applied only once approved):
  - `System-Overview.md` §Management API endpoints, the request-snapshot row: "tables list, no
    globs" → "tables list (the per-table config keys), no globs; refused (`409`) unless the
    desired state is `RUNNING` and a node holds the lease, as a restart (`ERROR` included), or
    while an unfinished job holds one of the tables; `202` with the job's id. The job runs in a
    restarted worker. It upserts: a row deleted at the source before the job stays (drop or move
    the sink table first to start clean)".
  - `Data-Flow.md` §On-Demand Snapshot Flow:
    - step 1: "Persist a job record … (status queued) **and request a restart of the pipeline's
      worker under the job's id** before responding";
    - step 2: "the owner takes the restart at its next poll; the new worker's Phase 0 snapshots
      each table from its first row, a halted one with its halt cleared and its target
      baselined to the queried schema (§8.2). The stream resumes from the confirmed position, as
      after any restart";
    - step 3: "… a table not in the config is described by its exact name and copied: read
      chunk by chunk, never streamed, so not reconciled";
    - step 4: "the owner follows each table to completed or failed (a halt), from the table's
      snapshot state, and ends the job";
    - the paragraph under it: "Idempotent upserts mean re-reading existing keys re-writes the
      same rows" + "a job never truncates the target: a row deleted at the source while the
      table wasn't streamed stays".
  - `Data-Flow.md` §State Layout, `jobs/<job_id>.json`: "(tables, each with its status,
    rows_done and error; status; requested_at/by; finished_at; error)", the id
    `<pipeline_id>~<suffix>`.
- `test-infra/README.md`: the snapshot-job scenarios (Task 7).

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-state/src/snapshot.rs`, `lib.rs` | `JobTableStatus`, `JobTable`, `SnapshotJobDoc::{new, is_finished, settle}`, `PipelineDocs::{update_snapshot_job, unfinished_snapshot_jobs}` |
| `crates/data-avalanche-core/src/source.rs` | `SourceConnector::describe_tables` |
| `crates/data-avalanche-source-pg/src/{catalog,connector}.rs` | `catalog::named`; `describe_tables`; `resolve_tables` split into `relations` and `described` |
| `crates/data-avalanche-source-mysql/src/{catalog,connector,schema}.rs` | `catalog::named`; `describe_tables`; channel C's baseline |
| `crates/data-avalanche-testkit/src/fake_source.rs` | `FakeSource::outside_config`, `describe_tables` |
| `crates/data-avalanche-pipeline/src/jobs.rs` (new), `lib.rs` | `Jobs`: the unfinished jobs a start read, `asked`, `running`, `record` |
| `crates/data-avalanche-pipeline/src/worker.rs` | Phase 0: the job tables' decisions, the cleared halts, the copies |
| `crates/data-avalanche-pipeline/src/session.rs`, `session/snapshot.rs`, `test_support.rs` | `Core::copies`; the session reads copies as dumps, never streams them |
| `crates/data-avalanche-cluster/src/jobs.rs` (new), `lib.rs`, `controller.rs` | `Jobs` (list, poll), `progress`; the controller's calls |
| `crates/data-avalanche-api/src/snapshots.rs` (new), `lib.rs`, `pipelines.rs`, `openapi.rs`, `openapi.json` | The three routes; the shared lookups made `pub(crate)`; the `job_id` parameter |
| `crates/data-avalanche-config/src/{external,lib}.rs` | `key_table` made public |
| `Cargo.toml` | `uuid`'s `v7` feature |
| `crates/data-avalanche/tests/e2e/{jobs,main}.rs`, `test-infra/README.md` | The job scenarios |

---

### Task 1: The job documents (`data-avalanche-state`)

**Files:**
- Modify: `crates/data-avalanche-state/src/snapshot.rs`, `crates/data-avalanche-state/src/lib.rs`

**Interfaces:**
- Consumes: `SnapshotJobDoc`, `JobTable`, `JobStatus`, `job_key`, `PipelineDocs::{create_snapshot_job,
  snapshot_job, snapshot_jobs}`, `DocStore::modify` (M2).
- Produces (re-exported at the crate root):
  - `JobTableStatus { Queued, Running, Completed, Failed }` (snake_case);
  - `JobTable { table: TableRef, status: JobTableStatus, rows_done: u64, error: Option<String> }`;
  - `JobStatus` gains doc comments only;
  - `SnapshotJobDoc::new(job_id: String, tables: Vec<TableRef>, requested_by: String,
    requested_at: DateTime<Utc>) -> Self` (all queued), `is_finished(&self) -> bool`,
    `settle(&mut self, now: DateTime<Utc>) -> bool` (true when it changed);
  - `PipelineDocs::update_snapshot_job<F: FnMut(&mut SnapshotJobDoc) + Send>(&self, job_id: &str,
    f: F) -> Result<Option<SnapshotJobDoc>, StateError>` (`None` when there is no such job);
  - `PipelineDocs::unfinished_snapshot_jobs(&self) -> Result<Vec<SnapshotJobDoc>, StateError>`,
    oldest request first.

D-M9b.2. `update_snapshot_job` loads first, so an absent job is `None` and never created; inside
`modify`, a job deleted between the load and the write is `Corrupt` ("deleted while it was
updated").

- [ ] **Step 1: Write the failing tests**

The tests module's `job()` helper now builds its job with `SnapshotJobDoc::new`; three tests are
added: the JSON shape of a job, `settle` through every status, and the unfinished jobs.

`crates/data-avalanche-state/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index 3c740e0..afad372 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -366,19 +366,12 @@ mod tests {
     }
 
     fn job(id: &str, at: u32) -> SnapshotJobDoc {
-        SnapshotJobDoc {
-            job_id: id.into(),
-            status: JobStatus::Queued,
-            tables: vec![JobTable {
-                table: TableRef::new("public", "orders"),
-                status: SnapshotStatus::None,
-                rows_done: 0,
-            }],
-            requested_at: t(at),
-            requested_by: "admin".into(),
-            finished_at: None,
-            error: None,
-        }
+        SnapshotJobDoc::new(
+            id.into(),
+            vec![TableRef::new("public", "orders")],
+            "admin".into(),
+            t(at),
+        )
     }
 
     #[tokio::test]
@@ -418,4 +411,125 @@ mod tests {
             "a table named `jobs` isn't a job"
         );
     }
+
+    fn two_tables() -> SnapshotJobDoc {
+        SnapshotJobDoc::new(
+            "orders~0199".into(),
+            vec![
+                TableRef::new("public", "orders"),
+                TableRef::new("public", "items"),
+            ],
+            "ops".into(),
+            t(0),
+        )
+    }
+
+    #[test]
+    fn a_job_json_shape() {
+        let mut doc = two_tables();
+        doc.tables[0].status = JobTableStatus::Running;
+        doc.tables[0].rows_done = 20_000;
+        doc.tables[1].status = JobTableStatus::Failed;
+        doc.tables[1].error = Some("no such table".into());
+        assert!(doc.settle(t(1)));
+        assert_eq!(
+            String::from_utf8(crate::doc::encode(&doc).to_vec()).unwrap(),
+            r#"{
+  "version": 1,
+  "job_id": "orders~0199",
+  "status": "running",
+  "tables": [
+    {
+      "table": {
+        "schema": "public",
+        "name": "orders"
+      },
+      "status": "running",
+      "rows_done": 20000,
+      "error": null
+    },
+    {
+      "table": {
+        "schema": "public",
+        "name": "items"
+      },
+      "status": "failed",
+      "rows_done": 0,
+      "error": "no such table"
+    }
+  ],
+  "requested_at": "2026-09-28T10:00:00Z",
+  "requested_by": "ops",
+  "finished_at": null,
+  "error": null
+}"#
+        );
+    }
+
+    /// A job is queued until a table starts, running while one is queued or running, then
+    /// completed when every table completed and failed otherwise, naming the failed tables.
+    #[test]
+    fn a_job_settles_once_every_table_ended() {
+        let mut doc = two_tables();
+        assert!(!doc.settle(t(1)), "every table queued: still queued");
+        assert_eq!(doc.status, JobStatus::Queued);
+        doc.tables[0].status = JobTableStatus::Running;
+        assert!(doc.settle(t(1)));
+        assert_eq!(doc.status, JobStatus::Running);
+        assert!(!doc.is_finished());
+        doc.tables[0].status = JobTableStatus::Completed;
+        assert!(!doc.settle(t(2)), "a table still queued: still running");
+        doc.tables[1].status = JobTableStatus::Completed;
+        assert!(doc.settle(t(3)));
+        assert_eq!(doc.status, JobStatus::Completed);
+        assert_eq!(doc.finished_at, Some(t(3)));
+        assert_eq!(doc.error, None);
+        assert!(doc.is_finished());
+        assert!(!doc.settle(t(4)), "a finished job stays as it ended");
+        assert_eq!(doc.finished_at, Some(t(3)));
+
+        let mut doc = two_tables();
+        doc.tables[0].status = JobTableStatus::Completed;
+        doc.tables[1].status = JobTableStatus::Failed;
+        doc.tables[1].error = Some("TABLE_DROPPED".into());
+        assert!(doc.settle(t(5)));
+        assert_eq!(doc.status, JobStatus::Failed);
+        assert_eq!(doc.finished_at, Some(t(5)));
+        assert_eq!(
+            doc.error.as_deref(),
+            Some("1 of 2 tables failed: public.items (TABLE_DROPPED)")
+        );
+    }
+
+    #[tokio::test]
+    async fn the_unfinished_jobs_leave_out_the_ended_ones() {
+        let p = pipeline();
+        p.create_snapshot_job(&job("a", 1)).await.unwrap();
+        p.create_snapshot_job(&job("b", 2)).await.unwrap();
+        p.create_snapshot_job(&job("c", 3)).await.unwrap();
+        let ended = p
+            .update_snapshot_job("b", |j| {
+                j.tables[0].status = JobTableStatus::Completed;
+                j.settle(t(4));
+            })
+            .await
+            .unwrap()
+            .unwrap();
+        assert_eq!(ended.status, JobStatus::Completed);
+        assert!(
+            p.update_snapshot_job("nope", |_| {})
+                .await
+                .unwrap()
+                .is_none(),
+            "no such job"
+        );
+        let ids: Vec<String> = p
+            .unfinished_snapshot_jobs()
+            .await
+            .unwrap()
+            .into_iter()
+            .map(|j| j.job_id)
+            .collect();
+        assert_eq!(ids, ["a", "c"]);
+    }
 }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state --lib snapshot::`

Expected: FAIL to compile: `JobTableStatus`, `SnapshotJobDoc::{new, settle, is_finished}` and
`JobTable::error` don't exist.

```text
error[E0599]: no associated function or constant named `new` found for struct `snapshot::SnapshotJobDoc` in the current scope
   --> crates/data-avalanche-state/src/snapshot.rs:369:25
error[E0433]: cannot find type `JobTableStatus` in this scope
   --> crates/data-avalanche-state/src/snapshot.rs:430:32
error[E0609]: no field `error` on type `JobTable`
   --> crates/data-avalanche-state/src/snapshot.rs:433:23
error[E0599]: no method named `settle` found for struct `snapshot::SnapshotJobDoc` in the current scope
   --> crates/data-avalanche-state/src/snapshot.rs:434:21
error[E0599]: no method named `is_finished` found for struct `snapshot::SnapshotJobDoc` in the current scope
   --> crates/data-avalanche-state/src/snapshot.rs:479:22
…
```

- [ ] **Step 3: Implement**

`crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 8d8ba48..89aabe8 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -59,7 +59,7 @@ pub use pipeline::{
     SLOT_KEY, SlotDoc,
 };
 pub use snapshot::{
-    CursorKey, JOBS_PREFIX, JobStatus, JobTable, SnapshotCursor, SnapshotJobDoc, SnapshotStatus,
-    TableSnapshotDoc, job_key, table_snapshot_key,
+    CursorKey, JOBS_PREFIX, JobStatus, JobTable, JobTableStatus, SnapshotCursor, SnapshotJobDoc,
+    SnapshotStatus, TableSnapshotDoc, job_key, table_snapshot_key,
 };
 pub use tables::{HALTS_KEY, HaltsDoc, SCHEMAS_PREFIX, TableHalt, table_schema_key};
```

`crates/data-avalanche-state/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
index afad372..65532d7 100644
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -67,8 +67,23 @@ impl Document for TableSnapshotDoc {
 #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
 #[serde(rename_all = "snake_case")]
 pub enum JobStatus {
+    /// No table has started yet.
     Queued,
     Running,
+    /// Every table completed.
+    Completed,
+    /// Every table ended, and one or more failed.
+    Failed,
+}
+
+/// Where one requested table of a job is.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(rename_all = "snake_case")]
+pub enum JobTableStatus {
+    /// The owner hasn't started it yet.
+    Queued,
+    /// Its snapshot runs: its document is `in_progress`.
+    Running,
     Completed,
     Failed,
 }
@@ -77,8 +92,10 @@ pub enum JobStatus {
 #[serde(deny_unknown_fields)]
 pub struct JobTable {
     pub table: TableRef,
-    pub status: SnapshotStatus,
+    pub status: JobTableStatus,
     pub rows_done: u64,
+    /// Why it failed.
+    pub error: Option<String>,
 }
 
 /// An on-demand snapshot job (Data-Flow §On-Demand Snapshot); survives restart and failover.
@@ -91,6 +108,7 @@ pub struct SnapshotJobDoc {
     pub requested_at: DateTime<Utc>,
     pub requested_by: String,
     pub finished_at: Option<DateTime<Utc>>,
+    /// Which tables failed, once the job ended.
     pub error: Option<String>,
 }
 
@@ -99,6 +117,86 @@ impl Document for SnapshotJobDoc {
     const SCHEMA_VERSION: u64 = 1;
 }
 
+impl SnapshotJobDoc {
+    /// A job just asked for: it and every table queued.
+    pub fn new(
+        job_id: String,
+        tables: Vec<TableRef>,
+        requested_by: String,
+        requested_at: DateTime<Utc>,
+    ) -> Self {
+        Self {
+            job_id,
+            status: JobStatus::Queued,
+            tables: tables
+                .into_iter()
+                .map(|table| JobTable {
+                    table,
+                    status: JobTableStatus::Queued,
+                    rows_done: 0,
+                    error: None,
+                })
+                .collect(),
+            requested_at,
+            requested_by,
+            finished_at: None,
+            error: None,
+        }
+    }
+
+    pub fn is_finished(&self) -> bool {
+        matches!(self.status, JobStatus::Completed | JobStatus::Failed)
+    }
+
+    /// Sets the job's status from its tables': queued while every table is, running while one
+    /// is queued or running, then completed or failed, at `now`. A finished job stays as it
+    /// ended. True when it changed.
+    pub fn settle(&mut self, now: DateTime<Utc>) -> bool {
+        if self.is_finished() {
+            return false;
+        }
+        let is = |s: JobTableStatus| move |t: &JobTable| t.status == s;
+        let status = if self.tables.iter().all(is(JobTableStatus::Queued)) {
+            JobStatus::Queued
+        } else if self
+            .tables
+            .iter()
+            .any(|t| matches!(t.status, JobTableStatus::Queued | JobTableStatus::Running))
+        {
+            JobStatus::Running
+        } else if self.tables.iter().all(is(JobTableStatus::Completed)) {
+            JobStatus::Completed
+        } else {
+            JobStatus::Failed
+        };
+        if status == self.status {
+            return false;
+        }
+        self.status = status;
+        if self.is_finished() {
+            self.finished_at = Some(now);
+            let failed: Vec<String> = self
+                .tables
+                .iter()
+                .filter(|t| t.status == JobTableStatus::Failed)
+                .map(|t| match &t.error {
+                    Some(e) => format!("{} ({e})", t.table),
+                    None => t.table.to_string(),
+                })
+                .collect();
+            if !failed.is_empty() {
+                self.error = Some(format!(
+                    "{} of {} tables failed: {}",
+                    failed.len(),
+                    self.tables.len(),
+                    failed.join(", ")
+                ));
+            }
+        }
+        true
+    }
+}
+
 /// `snapshots/<schema>.<table>.json`. Each name is escaped as a key segment and its dots become
 /// `%2E`, so the `.` between them is unambiguous (`a.b` + `c` ≠ `a` + `b.c`).
 pub fn table_snapshot_key(table: &TableRef) -> String {
@@ -184,6 +282,53 @@ impl PipelineDocs {
         self.docs.load(&job_key(job_id)).await
     }
 
+    /// Changes job `job_id` with `f`, read-modify-write; `None` when there is no such job.
+    ///
+    /// # Errors
+    /// A storage or decode failure, or the last race error once the attempts run out.
+    pub async fn update_snapshot_job<F>(
+        &self,
+        job_id: &str,
+        mut f: F,
+    ) -> Result<Option<SnapshotJobDoc>, StateError>
+    where
+        F: FnMut(&mut SnapshotJobDoc) + Send,
+    {
+        let key = job_key(job_id);
+        if self.docs.load::<SnapshotJobDoc>(&key).await?.is_none() {
+            return Ok(None);
+        }
+        let v = self
+            .docs
+            .modify(&key, |cur: Option<SnapshotJobDoc>| {
+                let Some(mut doc) = cur else {
+                    return Err(StateError::Corrupt {
+                        key: key.clone(),
+                        kind: SnapshotJobDoc::KIND,
+                        reason: "deleted while it was updated".into(),
+                    });
+                };
+                f(&mut doc);
+                Ok(doc)
+            })
+            .await?;
+        Ok(Some(v.doc))
+    }
+
+    /// The jobs not finished yet, oldest request first.
+    ///
+    /// # Errors
+    /// As [`Self::snapshot_jobs`].
+    pub async fn unfinished_snapshot_jobs(&self) -> Result<Vec<SnapshotJobDoc>, StateError> {
+        Ok(self
+            .snapshot_jobs()
+            .await?
+            .into_iter()
+            .map(|v| v.doc)
+            .filter(|j| !j.is_finished())
+            .collect())
+    }
+
     /// Every job of this pipeline, oldest request first.
     ///
     /// # Errors
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state --lib snapshot::`

Expected: PASS, 10 tests (the three new ones among them).

Run: `cargo test --workspace`

Expected: PASS, every test binary (1260 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state
git commit -m "feat(state): the snapshot job documents — a status per table, the job settled from them, update and the unfinished jobs"
```

---

### Task 2: Tables by name, and MySQL's baseline (`core`, `source-pg`, `source-mysql`, `testkit`)

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs`
- Modify: `crates/data-avalanche-source-pg/src/{catalog,connector}.rs`
- Modify: `crates/data-avalanche-source-mysql/src/{catalog,connector,schema}.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`
- Test: `crates/data-avalanche-source-pg/tests/pg/resolve.rs`,
  `crates/data-avalanche-source-mysql/tests/mysql/{resolve,schema}.rs`

**Interfaces:**
- Consumes: `SourceConnector`, `ResolvedTables { tables, halted, skipped, warnings }` (M5);
  PostgreSQL's `catalog::{select, Relation, Selection}`, MySQL's `catalog::{Listed, Selection}`
  and `schema::{scope, diff}`, `ddl::Scope` (M5, M7a).
- Produces:
  - `SourceConnector::describe_tables(&self, tables: &[TableRef]) -> Result<ResolvedTables,
    SourceError>`, a required method: the tables by exact name, in the order given;
  - PostgreSQL: `catalog::named(tables: &[TableRef], relations: &[Relation]) -> Selection`,
    `PgConnector::describe_tables`; `resolve_tables` reads `relations()` and describes with
    `described(selection)`, which `describe_tables` shares;
  - MySQL: `catalog::named(db: &str, tables: &[TableRef], listed: &[Listed]) -> Selection`,
    `MySqlConnector::describe_tables`, the same split; `read_change` takes
    `SchemaHint::Columns` as `Scope::Table` with no renames;
  - testkit: `Method::DescribeTables`; `FakeSource::outside_config(&self, table: &TableRef)` (the
    table stays in the catalog, `resolve_tables` leaves it out, `describe_tables` finds it).

D-M9b.10. `named` keeps the given order and only reads the catalog lists; the table-set checks
`resolve_tables` runs on the config (V-079's numeric warning aside, which `described` still
returns to it) don't apply to named tables.

- [ ] **Step 1: Write the failing tests**

Unit tests of each `named`, the fake's `describe_tables`, an IT per source of `describe_tables`
(a table the patterns exclude described, one without a key halted, a missing one skipped), and
a MySQL IT of a baseline with no statement.

`crates/data-avalanche-source-mysql/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/catalog.rs b/crates/data-avalanche-source-mysql/src/catalog.rs
index a7d853c..929f4d6 100644
--- a/crates/data-avalanche-source-mysql/src/catalog.rs
+++ b/crates/data-avalanche-source-mysql/src/catalog.rs
@@ -383,4 +383,33 @@ mod tests {
         assert_eq!(globbed.skipped.len(), 1);
         assert_eq!(globbed.tables.len(), 3);
     }
+
+    /// An on-demand snapshot names its tables exactly, whatever the config's patterns say; a
+    /// table of another database isn't the source's.
+    #[test]
+    fn named_tables_are_selected_by_their_exact_names() {
+        let all = [listed("orders", false), listed("history", true)];
+        let s = named(
+            "shop",
+            &[
+                TableRef::new("shop", "orders"),
+                TableRef::new("shop", "history"),
+                TableRef::new("shop", "gone"),
+                TableRef::new("other", "orders"),
+            ],
+            &all,
+        );
+        assert_eq!(s.tables, vec![(listed("orders", false), true)]);
+        assert!(matches!(s.halted[0].1, HaltReason::NotReplicable { .. }));
+        assert_eq!(
+            s.skipped,
+            vec![
+                (TableRef::new("shop", "gone"), "no such table".to_string()),
+                (
+                    TableRef::new("other", "orders"),
+                    "not in source.db shop".to_string()
+                ),
+            ]
+        );
+    }
 }
```

`crates/data-avalanche-source-mysql/tests/mysql/resolve.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs b/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs
index 20184a7..9288a60 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs
@@ -271,3 +271,30 @@ async fn a_reloaded_include_takes_the_next_resolve() {
     assert_eq!(names(c.resolve_tables().await.unwrap()), [t("a")]);
     cleanup(&n).await;
 }
+
+/// An on-demand snapshot's tables, by exact name (Data-Flow §On-Demand Snapshot): one outside the
+/// config's patterns is described, one without a primary key halts, one that isn't there is
+/// skipped.
+#[tokio::test]
+async fn named_tables_are_described_whatever_the_config_includes() {
+    require_it!();
+    let (n, _a) = fresh(
+        "describe_named",
+        "CREATE TABLE orders (id INT PRIMARY KEY);
+         CREATE TABLE extra (id INT PRIMARY KEY, v TEXT);
+         CREATE TABLE nopk (msg TEXT);",
+    )
+    .await;
+    let c = connector(&pipeline(&n, &["orders"], "", ""));
+    let t = |name: &str| TableRef::new(&n.db, name);
+    let r = c
+        .describe_tables(&[t("extra"), t("nopk"), t("missing"), t("orders")])
+        .await
+        .expect("describe");
+    let names: Vec<_> = r.tables.iter().map(|s| s.table().clone()).collect();
+    assert_eq!(names, [t("extra"), t("orders")]);
+    assert_eq!(r.tables[0].identifier(), ["id"]);
+    assert_eq!(r.halted, [(t("nopk"), HaltReason::NoIdentifier)]);
+    assert_eq!(r.skipped, [(t("missing"), "no such table".to_string())]);
+    cleanup(&n).await;
+}
```

`crates/data-avalanche-source-mysql/tests/mysql/schema.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/schema.rs b/crates/data-avalanche-source-mysql/tests/mysql/schema.rs
index bd3b471..4921991 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/schema.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/schema.rs
@@ -61,6 +61,39 @@ async fn a_statement_s_change_is_scoped_to_its_columns() {
     cleanup(&n).await;
 }
 
+/// A baseline to the catalog (a fresh start, a job's cleared halt) has no statement: the whole
+/// table's change, with no rename proven.
+#[tokio::test]
+async fn a_baseline_takes_the_whole_table_from_the_catalog() {
+    require_it!();
+    let (n, mut a) = fresh(
+        "schema_baseline",
+        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b VARCHAR(20));",
+    )
+    .await;
+    let c = connector(&pipeline(&n, &["t"], "", ""));
+    let cached = c.resolve_tables().await.unwrap().tables.remove(0);
+    a.query_drop("ALTER TABLE t DROP COLUMN b, ADD COLUMN note TEXT, CHANGE COLUMN a aa INT")
+        .await
+        .unwrap();
+    let catalog = c.resolve_tables().await.unwrap().tables.remove(0);
+    let hint = SchemaHint::Columns(catalog.columns().to_vec());
+    let (delta, now) = c.augment(&cached, &hint).await.unwrap();
+    assert_eq!(now.unwrap().columns(), catalog.columns());
+    assert_eq!(
+        delta.changes,
+        [
+            SchemaChange::DropColumn("a".into()),
+            SchemaChange::DropColumn("b".into()),
+            SchemaChange::AddColumn(catalog.columns()[1].clone()),
+            SchemaChange::AddColumn(catalog.columns()[2].clone()),
+        ],
+        "{:?}",
+        delta.changes
+    );
+    cleanup(&n).await;
+}
+
 #[tokio::test]
 async fn a_dropped_or_swapped_table() {
     require_it!();
```

`crates/data-avalanche-source-pg/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/catalog.rs b/crates/data-avalanche-source-pg/src/catalog.rs
index e9c2484..be8f97f 100644
--- a/crates/data-avalanche-source-pg/src/catalog.rs
+++ b/crates/data-avalanche-source-pg/src/catalog.rs
@@ -693,4 +693,37 @@ mod tests {
             "{not_temporal}"
         );
     }
+
+    /// An on-demand snapshot names its tables exactly, whatever the config's patterns say
+    /// (Data-Flow §On-Demand Snapshot).
+    #[test]
+    fn named_tables_are_selected_by_their_exact_names() {
+        let relations = [
+            rel(1, "public", "orders", b'r'),
+            rel(2, "public", "events", b'p'),
+            Relation {
+                logged: false,
+                ..rel(3, "public", "cache", b'r')
+            },
+            rel(4, "audit", "orders", b'r'),
+        ];
+        let t = TableRef::new;
+        let sel = named(
+            &[
+                t("audit", "orders"),
+                t("public", "events"),
+                t("public", "cache"),
+                t("public", "missing"),
+            ],
+            &relations,
+        );
+        let got: Vec<(u32, bool)> = sel.tables.iter().map(|(r, e)| (r.oid, *e)).collect();
+        assert_eq!(got, [(4, true)]);
+        let skipped: Vec<&TableRef> = sel.skipped.iter().map(|(t, _)| t).collect();
+        assert_eq!(skipped, [&t("public", "events"), &t("public", "missing")]);
+        assert_eq!(sel.skipped[1].1, "no such table");
+        assert_eq!(sel.halted.len(), 1, "an unlogged table can't be read");
+        assert_eq!(sel.halted[0].0, t("public", "cache"));
+        assert!(sel.warnings.is_empty());
+    }
 }
```

`crates/data-avalanche-source-pg/tests/pg/resolve.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/resolve.rs b/crates/data-avalanche-source-pg/tests/pg/resolve.rs
index 29b80eb..3e13ef5 100644
--- a/crates/data-avalanche-source-pg/tests/pg/resolve.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/resolve.rs
@@ -271,3 +271,33 @@ async fn a_reloaded_include_takes_the_next_resolve() {
     assert_eq!(names(c.resolve_tables().await.unwrap()), [t("a")]);
     cleanup(&a, &n).await;
 }
+
+/// An on-demand snapshot's tables, by exact name (Data-Flow §On-Demand Snapshot): one outside the
+/// config's patterns is described, one without an identifier halts, one that isn't there is
+/// skipped. Nothing of the config's table-set checks applies.
+#[tokio::test]
+async fn named_tables_are_described_whatever_the_config_includes() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(
+        &a,
+        "describe_named",
+        "CREATE TABLE orders (id bigint PRIMARY KEY, note text);
+         CREATE TABLE extra (id int PRIMARY KEY, v text);
+         CREATE TABLE nopk (a int);",
+    )
+    .await;
+    let c = connector(&pipeline(&n, &["orders"], "", ""));
+    let t = |name: &str| n.table(name);
+    let r = c
+        .describe_tables(&[t("extra"), t("nopk"), t("missing"), t("orders")])
+        .await
+        .expect("describe");
+    let names: Vec<TableRef> = r.tables.iter().map(|s| s.table().clone()).collect();
+    assert_eq!(names, [t("extra"), t("orders")]);
+    assert_eq!(r.tables[0].identifier(), ["id"]);
+    assert!(r.tables[0].source_id().is_some(), "the table's oid");
+    assert_eq!(r.halted, [(t("nopk"), HaltReason::NoIdentifier)]);
+    assert_eq!(r.skipped, [(t("missing"), "no such table".to_string())]);
+    cleanup(&a, &n).await;
+}
```

`crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index e4b2c1a..25aa340 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -1676,4 +1676,36 @@ mod tests {
         src.release_stream();
         assert_eq!(s.next().await.unwrap(), StreamItem::KeepAlive(at));
     }
+
+    /// A table the config doesn't include stays in the catalog: the resolve leaves it out, a
+    /// describe by name finds it; a name the catalog lacks is skipped.
+    #[tokio::test]
+    async fn a_table_outside_the_config_is_described_by_its_name() {
+        let extra = TableRef::new("public", "extra");
+        let src = FakeSource::new(vec![
+            schema(),
+            TableSchema::new(
+                extra.clone(),
+                vec![ColumnSchema::new("id", V::Int64, "bigint").not_null()],
+                vec!["id".into()],
+            )
+            .unwrap(),
+        ]);
+        src.outside_config(&extra);
+        let resolved = src.resolve_tables().await.unwrap();
+        let names: Vec<&TableRef> = resolved.tables.iter().map(TableSchema::table).collect();
+        assert_eq!(names, [&t()]);
+        let missing = TableRef::new("public", "missing");
+        let described = src
+            .describe_tables(&[extra.clone(), missing.clone()])
+            .await
+            .unwrap();
+        assert_eq!(described.tables, [src.schema(&extra)]);
+        assert_eq!(described.skipped, [(missing, "no such table".to_string())]);
+        src.fail(
+            Method::DescribeTables,
+            SourceError::Transient("down".into()),
+        );
+        assert!(src.describe_tables(&[extra]).await.is_err());
+    }
 }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib catalog::`, then the same for
`data-avalanche-source-mysql`, then `cargo test -p data-avalanche-testkit --lib fake_source`

Expected: FAIL to compile: `named`, `describe_tables`, `outside_config` and
`Method::DescribeTables` don't exist.

```text
error[E0425]: cannot find function `named` in this scope
   --> crates/data-avalanche-source-pg/src/catalog.rs:711:19
error: could not compile `data-avalanche-source-pg` (lib test) due to 1 previous error

error[E0425]: cannot find function `named` in this scope
   --> crates/data-avalanche-source-mysql/src/catalog.rs:392:17
error: could not compile `data-avalanche-source-mysql` (lib test) due to 1 previous error

error[E0599]: no method named `outside_config` found for struct `fake_source::FakeSource` in the current scope
    --> crates/data-avalanche-testkit/src/fake_source.rs:1694:13
error[E0599]: no method named `describe_tables` found for struct `fake_source::FakeSource` in the current scope
    --> crates/data-avalanche-testkit/src/fake_source.rs:1700:14
error[E0599]: no variant, associated function, or constant named `DescribeTables` found for enum `fake_source::Method` in the current scope
    --> crates/data-avalanche-testkit/src/fake_source.rs:1706:21
error: could not compile `data-avalanche-testkit` (lib test) due to 4 previous errors
```

- [ ] **Step 3: Implement**

`crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index 242726a..bc97b48 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -325,6 +325,13 @@ pub trait SourceConnector: Send + Sync {
     /// Channel A: the configured table set, resolved and checked.
     async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError>;
 
+    /// An on-demand snapshot's tables, by exact name, whatever the config's patterns include
+    /// (Data-Flow §On-Demand Snapshot): `tables` are the ones to read; `halted` the ones the
+    /// source can't replicate (no identifier, unlogged, …); `skipped` a name the source doesn't
+    /// have, or one that isn't a table of its own (a partitioned parent). The config's table-set
+    /// checks don't apply: the tables needn't be the pipeline's.
+    async fn describe_tables(&self, tables: &[TableRef]) -> Result<ResolvedTables, SourceError>;
+
     /// Prepares the stream position for a start (PostgreSQL: the publication, then the slot state
     /// machine; MySQL: the checkpoint's server and whether its binlog is still there).
     /// `checkpoint` is the pipeline's durable position, `None` without state; `tables` is the
@@ -574,6 +581,9 @@ mod tests {
                 ..ResolvedTables::default()
             })
         }
+        async fn describe_tables(&self, _: &[TableRef]) -> Result<ResolvedTables, SourceError> {
+            Ok(ResolvedTables::default())
+        }
         async fn prepare(
             &self,
             checkpoint: Option<&Checkpoint>,
```

`crates/data-avalanche-source-mysql/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/catalog.rs b/crates/data-avalanche-source-mysql/src/catalog.rs
index 929f4d6..efc89b9 100644
--- a/crates/data-avalanche-source-mysql/src/catalog.rs
+++ b/crates/data-avalanche-source-mysql/src/catalog.rs
@@ -36,6 +36,36 @@ pub struct Selection {
 const VERSIONED: &str = "a MariaDB system-versioned table: its history rows and period columns \
     aren't replicated (DROP SYSTEM VERSIONING, or exclude it)";
 
+/// The tables an on-demand snapshot names, each by its exact name (Data-Flow §On-Demand
+/// Snapshot), in the order given. A table of another database than `db` (`source.db`) isn't the
+/// source's, a name the database lacks is skipped (`no such table`), and a system-versioned
+/// table halts.
+pub fn named(db: &str, tables: &[TableRef], listed: &[Listed]) -> Selection {
+    let mut out = Selection::default();
+    for t in tables {
+        if t.schema != db {
+            out.skipped
+                .push((t.clone(), format!("not in source.db {db}")));
+            continue;
+        }
+        let Some(l) = listed.iter().find(|l| &l.table == t) else {
+            out.skipped.push((t.clone(), "no such table".into()));
+            continue;
+        };
+        if l.versioned {
+            out.halted.push((
+                l.table.clone(),
+                HaltReason::NotReplicable {
+                    reason: VERSIONED.into(),
+                },
+            ));
+        } else {
+            out.tables.push((l.clone(), true));
+        }
+    }
+    out
+}
+
 /// The table's config key: its bare name (V-064).
 pub fn table_key(t: &TableRef) -> &str {
     &t.name
@@ -280,6 +310,33 @@ impl MySqlConnector {
         let listed = self.list_tables().await?;
         let live = self.live();
         let selection = select(&live.include, &live.exclude, &listed);
+        let mut out = self.described(selection).await?;
+        // Halted tables are in the set: their per-table config keys aren't stale (V-106).
+        let resolved: Vec<TableRef> = out
+            .tables
+            .iter()
+            .map(|s| s.table().clone())
+            .chain(out.halted.iter().map(|(t, _)| t.clone()))
+            .collect();
+        let report = check_table_set(&self.pipeline, &resolved);
+        if let Some(e) = report.errors.first() {
+            return Err(SourceError::Permanent(format!(
+                "{} {}: {}",
+                e.rule, e.path, e.message
+            )));
+        }
+        out.warnings.extend(
+            report
+                .warnings
+                .iter()
+                .map(|w| format!("{} {}: {}", w.rule, w.path, w.message)),
+        );
+        Ok(out)
+    }
+
+    /// The tables of `selection`, described: each with its schema, or halted, or skipped (a
+    /// table without a primary key that a glob found).
+    async fn described(&self, selection: Selection) -> Result<ResolvedTables, SourceError> {
         let names: Vec<String> = selection
             .tables
             .iter()
@@ -313,28 +370,19 @@ impl MySqlConnector {
                 Err(reason) => out.halted.push((listed.table, reason)),
             }
         }
-        // Halted tables are in the set: their per-table config keys aren't stale (V-106).
-        let resolved: Vec<TableRef> = out
-            .tables
-            .iter()
-            .map(|s| s.table().clone())
-            .chain(out.halted.iter().map(|(t, _)| t.clone()))
-            .collect();
-        let report = check_table_set(&self.pipeline, &resolved);
-        if let Some(e) = report.errors.first() {
-            return Err(SourceError::Permanent(format!(
-                "{} {}: {}",
-                e.rule, e.path, e.message
-            )));
-        }
-        out.warnings.extend(
-            report
-                .warnings
-                .iter()
-                .map(|w| format!("{} {}: {}", w.rule, w.path, w.message)),
-        );
         Ok(out)
     }
+
+    /// An on-demand snapshot's tables, by exact name (Data-Flow §On-Demand Snapshot): the
+    /// config's patterns and its table-set checks don't apply.
+    pub async fn describe_tables(
+        &self,
+        tables: &[TableRef],
+    ) -> Result<ResolvedTables, SourceError> {
+        let listed = self.list_tables().await?;
+        self.described(named(&self.pipeline.source.db, tables, &listed))
+            .await
+    }
 }
 
 #[cfg(test)]
```

`crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
index 1b261d0..ca329d9 100644
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -367,6 +367,10 @@ impl SourceConnector for MySqlConnector {
         MySqlConnector::resolve_tables(self).await
     }
 
+    async fn describe_tables(&self, tables: &[TableRef]) -> Result<ResolvedTables, SourceError> {
+        MySqlConnector::describe_tables(self, tables).await
+    }
+
     async fn prepare(
         &self,
         checkpoint: Option<&Checkpoint>,
```

`crates/data-avalanche-source-mysql/src/schema.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/schema.rs b/crates/data-avalanche-source-mysql/src/schema.rs
index 4da6779..748eaa0 100644
--- a/crates/data-avalanche-source-mysql/src/schema.rs
+++ b/crates/data-avalanche-source-mysql/src/schema.rs
@@ -19,7 +19,7 @@ use data_avalanche_core::{
     TableRef, TableSchema,
 };
 
-use crate::catalog::{plan_table, table_key};
+use crate::catalog::plan_table;
 use crate::connector::MySqlConnector;
 use crate::ddl::{Renamed, Scope, classify, renamed};
 use crate::server::Fold;
@@ -300,20 +300,15 @@ impl MySqlConnector {
         self.read_change(cached, hint).await
     }
 
-    /// The change the statement `hint` makes to `cached`, read from the catalog now. A table gone
-    /// from the catalog is dropped.
+    /// The change the statement `hint` makes to `cached`, read from the catalog now. A column
+    /// hint is a baseline to the catalog (a fresh start, a job's cleared halt): no statement, so
+    /// the whole table, with no rename proven. A table gone from the catalog is dropped.
     pub(crate) async fn read_change(
         &self,
         cached: &TableSchema,
         hint: &SchemaHint,
     ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
         let table = cached.table().clone();
-        let SchemaHint::Ddl(sql) = hint else {
-            return Err(SourceError::Permanent(format!(
-                "{}: a column hint on a MySQL source",
-                table_key(&table)
-            )));
-        };
         let halt = |reason| SourceError::TableHalt {
             table: table.clone(),
             reason,
@@ -332,7 +327,10 @@ impl MySqlConnector {
             ));
         };
         let now = now.map_err(halt)?;
-        let (columns, renames) = scope_of(sql, &table, self.shared.fold());
+        let (columns, renames) = match hint {
+            SchemaHint::Ddl(sql) => scope_of(sql, &table, self.shared.fold()),
+            SchemaHint::Columns(_) => (Scope::Table, vec![]),
+        };
         let new = scope(cached, &columns, &renames, &now).map_err(halt)?;
         let changes = diff(cached, &new, &renames);
         Ok((SchemaDelta { table, changes }, Some(new)))
```

`crates/data-avalanche-source-pg/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/catalog.rs b/crates/data-avalanche-source-pg/src/catalog.rs
index be8f97f..93cf452 100644
--- a/crates/data-avalanche-source-pg/src/catalog.rs
+++ b/crates/data-avalanche-source-pg/src/catalog.rs
@@ -65,11 +65,7 @@ pub fn select(include: &[String], exclude: &[String], relations: &[Relation]) ->
             continue;
         }
         if r.kind == b'p' {
-            out.skipped.push((
-                r.table.clone(),
-                "a partitioned table: its changes arrive under its partitions; include those"
-                    .into(),
-            ));
+            out.skipped.push((r.table.clone(), PARTITIONED.into()));
             continue;
         }
         if !r.logged {
@@ -89,6 +85,36 @@ pub fn select(include: &[String], exclude: &[String], relations: &[Relation]) ->
     out
 }
 
+/// The tables an on-demand snapshot names, each by its exact name (Data-Flow §On-Demand
+/// Snapshot), in the order given. A name the catalog lacks is skipped (`no such table`), a
+/// partitioned parent is skipped as [`select`] skips it, and an unlogged table halts.
+pub fn named(tables: &[TableRef], relations: &[Relation]) -> Selection {
+    let mut out = Selection::default();
+    for t in tables {
+        let Some(r) = relations.iter().find(|r| &r.table == t) else {
+            out.skipped.push((t.clone(), "no such table".into()));
+            continue;
+        };
+        if r.kind == b'p' {
+            out.skipped.push((r.table.clone(), PARTITIONED.into()));
+        } else if !r.logged {
+            out.halted.push((
+                r.table.clone(),
+                HaltReason::NotReplicable {
+                    reason: UNLOGGED.into(),
+                },
+            ));
+        } else {
+            out.tables.push((r.clone(), true));
+        }
+    }
+    out
+}
+
+/// Why a partitioned parent isn't a table of its own.
+const PARTITIONED: &str =
+    "a partitioned table: its changes arrive under its partitions; include those";
+
 /// One live, stored column (`attnum > 0`, not dropped, not generated: generated columns aren't in
 /// the stream).
 #[derive(Debug, Clone, PartialEq, Eq)]
```

`crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index 159208c..d86b546 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -204,13 +204,41 @@ impl PgConnector {
     /// table checked (config §11.2). A table without an identifier is skipped with a warning when
     /// a glob found it, and halts when its name was listed; every other problem halts the table.
     pub async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
-        let client = self.conn.client().await?;
-        let relations = self
-            .conn
-            .command("list tables", catalog::relations(&client))
-            .await?;
+        let relations = self.relations().await?;
         let live = self.live();
         let selection = catalog::select(&live.include, &live.exclude, &relations);
+        let (mut out, unconstrained) = self.described(selection).await?;
+        // Halted tables are in the set: their per-table config keys aren't stale (V-106).
+        let resolved: Vec<TableRef> = out
+            .tables
+            .iter()
+            .map(|s| s.table().clone())
+            .chain(out.halted.iter().map(|(t, _)| t.clone()))
+            .collect();
+        let mut report = check_table_set(&self.pipeline, &resolved);
+        report.extend(check_unbounded_numeric(&self.pipeline, &unconstrained));
+        if let Some(e) = report.errors.first() {
+            return Err(SourceError::Permanent(format!(
+                "{} {}: {}",
+                e.rule, e.path, e.message
+            )));
+        }
+        out.warnings.extend(
+            report
+                .warnings
+                .iter()
+                .map(|w| format!("{} {}: {}", w.rule, w.path, w.message)),
+        );
+        Ok(out)
+    }
+
+    /// The tables of `selection`, described: each with its schema, or halted, or skipped (a
+    /// table without an identifier that a glob found). Returns the unconstrained numeric columns
+    /// too, for V-079.
+    async fn described(
+        &self,
+        selection: catalog::Selection,
+    ) -> Result<(ResolvedTables, Vec<(TableRef, String)>), SourceError> {
         let oids: Vec<u32> = selection.tables.iter().map(|(r, _)| r.oid).collect();
         let mut schemas = self.schemas(&oids).await?;
         let mut out = ResolvedTables {
@@ -253,28 +281,24 @@ impl PgConnector {
                 Err(reason) => out.halted.push((relation.table, reason)),
             }
         }
-        // Halted tables are in the set: their per-table config keys aren't stale (V-106).
-        let resolved: Vec<TableRef> = out
-            .tables
-            .iter()
-            .map(|s| s.table().clone())
-            .chain(out.halted.iter().map(|(t, _)| t.clone()))
-            .collect();
-        let mut report = check_table_set(&self.pipeline, &resolved);
-        report.extend(check_unbounded_numeric(&self.pipeline, &unconstrained));
-        if let Some(e) = report.errors.first() {
-            return Err(SourceError::Permanent(format!(
-                "{} {}: {}",
-                e.rule, e.path, e.message
-            )));
-        }
-        out.warnings.extend(
-            report
-                .warnings
-                .iter()
-                .map(|w| format!("{} {}: {}", w.rule, w.path, w.message)),
-        );
-        Ok(out)
+        Ok((out, unconstrained))
+    }
+
+    /// An on-demand snapshot's tables, by exact name (Data-Flow §On-Demand Snapshot): the
+    /// config's patterns and its table-set checks don't apply.
+    pub async fn describe_tables(
+        &self,
+        tables: &[TableRef],
+    ) -> Result<ResolvedTables, SourceError> {
+        let relations = self.relations().await?;
+        Ok(self.described(catalog::named(tables, &relations)).await?.0)
+    }
+
+    async fn relations(&self) -> Result<Vec<catalog::Relation>, SourceError> {
+        let client = self.conn.client().await?;
+        self.conn
+            .command("list tables", catalog::relations(&client))
+            .await
     }
 
     /// `pg_current_wal_lsn()`: a snapshot chunk's watermark (Snapshot_Resumability §1).
@@ -303,6 +327,10 @@ impl SourceConnector for PgConnector {
         PgConnector::resolve_tables(self).await
     }
 
+    async fn describe_tables(&self, tables: &[TableRef]) -> Result<ResolvedTables, SourceError> {
+        PgConnector::describe_tables(self, tables).await
+    }
+
     async fn prepare(
         &self,
         checkpoint: Option<&Checkpoint>,
```

`crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index 25aa340..1c8dbce 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -14,6 +14,7 @@
 //!   change, `augment` diffs by column name and `rescan` reports it. Like PostgreSQL's, `augment`
 //!   keeps to the columns a `Columns` hint names, so a catalog ahead of the hint adds nothing.
 //!   [`FakeSource::after_next_rescan`] acts between a rescan and the position read after it.
+//! - [`FakeSource::outside_config`] keeps a table in the catalog that the config doesn't include.
 //! - [`FakeSource::fail`] makes the next call of one method fail; [`FakeSource::hold_reads`]
 //!   holds chunk reads after they took their snapshot, so a test can commit while one is out.
 //!   [`FakeSource::share_connection`] makes chunk reads and other calls wait on each other, as
@@ -44,6 +45,7 @@ use tokio::sync::Notify;
 pub enum Method {
     Preflight,
     ResolveTables,
+    DescribeTables,
     Prepare,
     ResetPosition,
     Retained,
@@ -218,6 +220,8 @@ impl std::fmt::Debug for Action {
 #[derive(Debug)]
 struct Db {
     catalog: BTreeMap<TableRef, TableSchema>,
+    /// [`FakeSource::outside_config`].
+    unconfigured: BTreeSet<TableRef>,
     next_source_id: u64,
     rows: BTreeMap<TableRef, BTreeMap<Key, Row>>,
     /// Stalled transactions: the rows they changed, as they were before (`None`: absent).
@@ -281,6 +285,7 @@ impl FakeSource {
     pub fn new(tables: Vec<TableSchema>) -> Self {
         let mut db = Db {
             catalog: BTreeMap::new(),
+            unconfigured: BTreeSet::new(),
             next_source_id: 1000,
             rows: BTreeMap::new(),
             stalled: BTreeMap::new(),
@@ -420,6 +425,12 @@ impl FakeSource {
         db.catalog.insert(table.clone(), schema);
     }
 
+    /// `table` stays in the catalog, but the config doesn't include it: `resolve_tables` leaves
+    /// it out, `describe_tables` finds it.
+    pub fn outside_config(&self, table: &TableRef) {
+        self.db().unconfigured.insert(table.clone());
+    }
+
     /// Drops `table` from the catalog (it streams nothing more).
     pub fn drop_table(&self, table: &TableRef) {
         let mut db = self.db();
@@ -907,12 +918,31 @@ impl SourceConnector for FakeSource {
 
     async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
         self.take_fault(Method::ResolveTables)?;
+        let db = self.db();
         Ok(ResolvedTables {
-            tables: self.db().catalog.values().cloned().collect(),
+            tables: db
+                .catalog
+                .values()
+                .filter(|t| !db.unconfigured.contains(t.table()))
+                .cloned()
+                .collect(),
             ..ResolvedTables::default()
         })
     }
 
+    async fn describe_tables(&self, tables: &[TableRef]) -> Result<ResolvedTables, SourceError> {
+        self.take_fault(Method::DescribeTables)?;
+        let db = self.db();
+        let mut out = ResolvedTables::default();
+        for t in tables {
+            match db.catalog.get(t) {
+                Some(schema) => out.tables.push(schema.clone()),
+                None => out.skipped.push((t.clone(), "no such table".into())),
+            }
+        }
+        Ok(out)
+    }
+
     async fn prepare(
         &self,
         checkpoint: Option<&Checkpoint>,
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib catalog:: && cargo test -p data-avalanche-source-mysql --lib catalog:: && cargo test -p data-avalanche-testkit --lib fake_source`

Expected: PASS.

Run (with `test-infra/up.sh`): `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg named_tables`, and for each of `mysql-84`, `mysql-84-nogtid` and `mariadb-118`:
`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=<target> cargo test -p data-avalanche-source-mysql --test mysql -- named_tables schema::`

Expected: PASS: the PostgreSQL IT, and on each MySQL target the describe IT and the four
`schema::` ITs (the baseline among them). Without the `read_change` change the baseline IT fails:

```text
test schema::a_baseline_takes_the_whole_table_from_the_catalog ... FAILED
called `Result::unwrap()` on an `Err` value: Permanent("t: a column hint on a MySQL source")
```

Run: `cargo test --workspace`

Expected: PASS (1266 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core crates/data-avalanche-source-pg crates/data-avalanche-source-mysql crates/data-avalanche-testkit
git commit -m "feat(source): describe tables by exact name for a snapshot job; MySQL's channel C takes a column hint as a baseline to the catalog (DA-SCHEMA-10)"
```

---

### Task 3: Phase 0 takes a job's tables (`data-avalanche-pipeline`)

**Files:**
- Create: `crates/data-avalanche-pipeline/src/jobs.rs`
- Modify: `crates/data-avalanche-pipeline/src/lib.rs`, `crates/data-avalanche-pipeline/src/worker.rs`

**Interfaces:**
- Consumes: Task 1's `PipelineDocs::{unfinished_snapshot_jobs, update_snapshot_job}`,
  `SnapshotJobDoc::settle`, `JobTableStatus`; Phase 0's `start(core, lost)`, `Decision`, `Plan`,
  `decide`, `Evolver::{plan, evolve}`, `TableSchema::with_source_id`, `PipelineDocs::clear_halt`
  (M6a, M6b).
- Produces (crate-private):
  - `jobs::Jobs { jobs: Vec<SnapshotJobDoc> }`: `load(docs: &PipelineDocs) -> Result<Self,
    StateError>`, `asked(&self) -> BTreeSet<TableRef>` (the queued job tables), `running(&self)
    -> BTreeSet<TableRef>`, `record(&self, docs, started: &BTreeSet<TableRef>, failed:
    &BTreeMap<TableRef, String>) -> Result<(), StateError>`;
  - in `start`: `cleared`, the halted asked tables Phase 0 baselines and clears.

D-M9b.5, D-M9b.6. The tests live in `jobs.rs` and drive the worker through `test_support`'s
`World`: `established()` runs a first start under mode `never` (a slot, a checkpoint), `ask`
writes a job, `snapshot_status` reads a table's snapshot document, `noted` and `with_note` build
the rows and the schema of a table that gained a `note` column.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-pipeline/src/jobs.rs` (new):

```diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
new file mode 100644
index 0000000..c07cfce
--- /dev/null
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -0,0 +1,279 @@
+//! Phase 0's part of the on-demand snapshot jobs (Data-Flow §On-Demand Snapshot).
+
+#[cfg(test)]
+mod tests {
+    use std::sync::Arc;
+
+    use chrono::Utc;
+    use data_avalanche_config::Pipeline;
+    use data_avalanche_core::{
+        ColumnSchema, ColumnValue, ColumnValueType as V, HaltReason, Row, TableSchema,
+    };
+    use data_avalanche_state::{
+        CursorKey, JobStatus, JobTableStatus, SnapshotCursor, SnapshotJobDoc, SnapshotStatus,
+        TableHalt, TableSnapshotDoc,
+    };
+    use data_avalanche_testkit::FakeTxn;
+    use tokio::sync::watch;
+
+    use crate::test_support::{World, Yaml, fast, row, t, wait_until};
+    use crate::{Command, Exit, Phase, PipelineWorker, WorkerDeps, WorkerError, WorkerStatus};
+
+    fn never(chunk_size: usize) -> Pipeline {
+        let tables = format!("\n      snapshots: {{ mode: never, chunk_size: {chunk_size} }}");
+        Yaml {
+            tables: &tables,
+            sink: "flush: { seconds: 0 }",
+            ..Yaml::default()
+        }
+        .build()
+    }
+
+    fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
+        ids.map(|id| row(id, "x")).collect()
+    }
+
+    async fn ask(w: &World, id: &str, tables: &[&str]) {
+        let doc = SnapshotJobDoc::new(
+            id.into(),
+            tables.iter().map(|n| t(n)).collect(),
+            "ops".into(),
+            Utc::now(),
+        );
+        w.docs.create_snapshot_job(&doc).await.unwrap();
+    }
+
+    async fn job(w: &World, id: &str) -> SnapshotJobDoc {
+        w.docs.snapshot_job(id).await.unwrap().unwrap().doc
+    }
+
+    /// Runs a worker over `w` until `drive` returns, then drains it.
+    async fn run<F: Future<Output = ()>>(
+        w: &World,
+        p: &Pipeline,
+        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
+    ) -> (Result<Exit, WorkerError>, WorkerStatus) {
+        let deps = WorkerDeps::new(
+            Arc::new(w.source.clone()),
+            Arc::new(w.sink.clone()),
+            w.docs.clone(),
+        );
+        let (worker, handle) = PipelineWorker::new(p, deps, fast());
+        let status = handle.status.clone();
+        let driver = async move {
+            drive(handle.status.clone()).await;
+            let _ = handle.control.send(Command::Drain).await;
+        };
+        let (end, ()) = tokio::join!(worker.run(), driver);
+        let last = status.borrow().clone();
+        (end, last)
+    }
+
+    /// A stream session streams, and everything up to the log's end is durable.
+    async fn streamed(status: &watch::Receiver<WorkerStatus>, w: &World) {
+        let end = w.source.wal_end();
+        wait_until("the log's end is durable", || {
+            let st = status.borrow();
+            st.phase == Phase::Streaming && st.checkpoint.as_ref() == Some(&end)
+        })
+        .await;
+    }
+
+    /// A first run under `mode: never`: the slot, the sink tables, the schemas, the checkpoint.
+    async fn established(w: &World) {
+        let (end, _) = run(w, &never(10), async |status| streamed(&status, w).await).await;
+        end.unwrap();
+    }
+
+    async fn snapshot_status(w: &World, table: &str) -> SnapshotStatus {
+        w.docs
+            .table_snapshot(&t(table))
+            .await
+            .unwrap()
+            .unwrap()
+            .doc
+            .status
+    }
+
+    /// A streamed table asked for is read again from its first row: rows the stream never
+    /// carried (loaded behind its back) reach the sink. The job's table is running; the owner's
+    /// controller sees it to its end.
+    #[tokio::test]
+    async fn a_job_snapshots_a_streamed_table_again_from_scratch() {
+        let w = World::new(&["a", "b"]);
+        established(&w).await;
+        w.source
+            .commit(FakeTxn::new().insert(&t("a"), row(1, "streamed")));
+        w.source.load(&t("a"), rows(5..=7));
+        ask(&w, "p~1", &["a"]).await;
+        let (end, st) = run(&w, &never(10), async |status| {
+            wait_until("the rows land", || w.ids("a") == [1, 5, 6, 7]).await;
+            streamed(&status, &w).await;
+        })
+        .await;
+        assert_eq!(end.unwrap(), Exit::Drained);
+        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::Completed);
+        assert_eq!(w.source.reads(&t("b")), 0, "only the job's table");
+        let j = job(&w, "p~1").await;
+        assert_eq!(j.status, JobStatus::Running);
+        assert_eq!(j.tables[0].status, JobTableStatus::Running);
+        assert!(st.warnings.is_empty(), "{:?}", st.warnings);
+    }
+
+    fn noted(id: i64, note: Option<&str>) -> Row {
+        Row(vec![
+            ("id".into(), ColumnValue::Int64(id)),
+            ("doc".into(), ColumnValue::Text("x".into())),
+            (
+                "note".into(),
+                note.map_or(ColumnValue::Null, |n| ColumnValue::Text(n.into())),
+            ),
+        ])
+    }
+
+    fn with_note(name: &str) -> TableSchema {
+        TableSchema::new(
+            t(name),
+            vec![
+                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
+                ColumnSchema::new("doc", V::Text, "text"),
+                ColumnSchema::new("note", V::Text, "text"),
+            ],
+            vec!["id".into()],
+        )
+        .unwrap()
+    }
+
+    /// A halted table asked for (dropped and created again, here with a column more) is
+    /// streamed again: its halt is cleared, its cached schema is the catalog's (the new table's
+    /// source id), its sink table takes the new column, and its rows are read.
+    #[tokio::test]
+    async fn a_job_clears_a_halt_and_baselines_the_table() {
+        let w = World::new(&["a"]);
+        established(&w).await;
+        let old = w.source.schema(&t("a"));
+        w.docs
+            .record_halt(&TableHalt {
+                table: t("a"),
+                reason: HaltReason::TableDropped,
+                at: w.source.wal_end(),
+            })
+            .await
+            .unwrap();
+        w.source.drop_table(&t("a"));
+        w.source.create_table(with_note("a"));
+        w.source.load(&t("a"), vec![noted(1, None), noted(2, None)]);
+        ask(&w, "p~1", &["a"]).await;
+        let (end, st) = run(&w, &never(10), async |status| {
+            wait_until("the rows land", || w.ids("a") == [1, 2]).await;
+            w.source
+                .commit(FakeTxn::new().insert(&t("a"), noted(3, Some("n"))));
+            wait_until("the change streams", || w.ids("a") == [1, 2, 3]).await;
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert!(st.halted.is_empty(), "{:?}", st.halted);
+        assert!(w.docs.halts().await.unwrap().tables.is_empty());
+        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
+        assert_eq!(cached, w.source.schema(&t("a")));
+        assert_ne!(cached.source_id(), old.source_id());
+        assert_eq!(w.text("a", "note"), [None, None, Some("n".into())]);
+        assert_eq!(
+            job(&w, "p~1").await.tables[0].status,
+            JobTableStatus::Running
+        );
+    }
+
+    /// A table asked for that halts at this start (here its identifier changed) fails in its
+    /// job, with the halt's reason; the job ends once no table is left.
+    #[tokio::test]
+    async fn a_job_table_that_halts_at_the_start_fails() {
+        let w = World::new(&["a", "b"]);
+        established(&w).await;
+        w.docs
+            .record_halt(&TableHalt {
+                table: t("a"),
+                reason: HaltReason::UnsafeDdl {
+                    detail: "the key changed".into(),
+                },
+                at: w.source.wal_end(),
+            })
+            .await
+            .unwrap();
+        // The key moves to `doc`: an identifier change is unsafe (`Schema_Change…` §7.2).
+        let rekeyed = TableSchema::new(
+            t("a"),
+            vec![
+                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
+                ColumnSchema::new("doc", V::Text, "text").not_null(),
+            ],
+            vec!["doc".into()],
+        )
+        .unwrap();
+        w.source.alter(&t("a"), rekeyed);
+        ask(&w, "p~1", &["a"]).await;
+        let (end, st) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
+        end.unwrap();
+        assert!(st.halted.contains_key(&t("a")));
+        let j = job(&w, "p~1").await;
+        assert_eq!(j.tables[0].status, JobTableStatus::Failed);
+        let error = j.tables[0].error.clone().unwrap();
+        assert!(error.starts_with("UNSAFE_DDL: "), "{error}");
+        assert_eq!(j.status, JobStatus::Failed);
+        assert!(j.finished_at.is_some());
+    }
+
+    /// A job's table whose snapshot runs keeps running at the next start, from its cursor,
+    /// even under `mode: never`, which abandons a snapshot in progress.
+    #[tokio::test]
+    async fn a_running_job_table_resumes_from_its_cursor() {
+        let w = World::new(&["a"]);
+        established(&w).await;
+        w.source.load(&t("a"), rows(1..=4));
+        ask(&w, "p~1", &["a"]).await;
+        w.docs
+            .update_snapshot_job("p~1", |j| {
+                j.tables[0].status = JobTableStatus::Running;
+                j.settle(Utc::now());
+            })
+            .await
+            .unwrap();
+        w.docs
+            .save_table_snapshot(&TableSnapshotDoc {
+                table: t("a"),
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
+        let (end, _) = run(&w, &never(2), async |status| {
+            wait_until("the rest lands", || w.ids("a") == [3, 4]).await;
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(snapshot_status(&w, "a").await, SnapshotStatus::Completed);
+        assert_eq!(
+            w.ids("a"),
+            [3, 4],
+            "rows 1 and 2 were read by an earlier run"
+        );
+        assert_eq!(
+            job(&w, "p~1").await.tables[0].status,
+            JobTableStatus::Running
+        );
+    }
+}
```

`crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
index f89a0c1..a1bca90 100644
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -14,6 +14,7 @@ mod cursor;
 mod decide;
 pub mod error;
 pub mod hooks;
+mod jobs;
 pub mod policy;
 mod reconcile;
 mod schema_change;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-pipeline --lib jobs::`

Expected: FAIL, 4 tests: the worker doesn't read the jobs, so no table is read again, the halt
stays, and the job stays queued.

```text
test jobs::tests::a_job_table_that_halts_at_the_start_fails ... FAILED
test jobs::tests::a_running_job_table_resumes_from_its_cursor ... FAILED
test jobs::tests::a_job_clears_a_halt_and_baselines_the_table ... FAILED
test jobs::tests::a_job_snapshots_a_streamed_table_again_from_scratch ... FAILED
thread 'jobs::tests::a_job_table_that_halts_at_the_start_fails' panicked at crates/data-avalanche-pipeline/src/jobs.rs:220:9:
assertion `left == right` failed
  left: Queued
thread 'jobs::tests::a_running_job_table_resumes_from_its_cursor' panicked at crates/data-avalanche-pipeline/src/test_support.rs:347:9:
timed out waiting until the rest lands
thread 'jobs::tests::a_job_clears_a_halt_and_baselines_the_table' panicked at crates/data-avalanche-pipeline/src/test_support.rs:347:9:
timed out waiting until the rows land
thread 'jobs::tests::a_job_snapshots_a_streamed_table_again_from_scratch' panicked at crates/data-avalanche-pipeline/src/test_support.rs:347:9:
timed out waiting until the rows land
test result: FAILED. 0 passed; 4 failed; 0 ignored; 0 measured; 216 filtered out; finished in 5.03s
```

- [ ] **Step 3: Implement**

`Jobs` in `jobs.rs`, and Phase 0 in `worker.rs`: the jobs loaded after the stored checkpoint;
`cleared` taken out of the recorded halts; the source-id check skipped for an asked table; the
decision overridden for an asked or a running table; the cleared table rebased and baselined; its
halt cleared after the schemas; the job record before the checkpoint.

`crates/data-avalanche-pipeline/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
index c07cfce..95560f0 100644
--- a/crates/data-avalanche-pipeline/src/jobs.rs
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -1,4 +1,83 @@
-//! Phase 0's part of the on-demand snapshot jobs (Data-Flow §On-Demand Snapshot).
+//! Phase 0's part of the on-demand snapshot jobs (Data-Flow §On-Demand Snapshot). A job's
+//! queued tables are asked for: each start snapshots them from their first row, a halted one
+//! with its halt cleared and its schema baselined to the catalog's. A table whose job snapshot
+//! runs keeps running whatever its snapshot mode. Phase 0 records what it did with each asked
+//! table; the owner's controller follows a running one to its end.
+
+use std::collections::{BTreeMap, BTreeSet};
+
+use chrono::Utc;
+use data_avalanche_core::TableRef;
+use data_avalanche_state::{JobTableStatus, PipelineDocs, SnapshotJobDoc, StateError};
+
+/// The unfinished jobs, as a start read them.
+#[derive(Debug, Default)]
+pub(crate) struct Jobs {
+    jobs: Vec<SnapshotJobDoc>,
+}
+
+impl Jobs {
+    pub async fn load(docs: &PipelineDocs) -> Result<Self, StateError> {
+        Ok(Self {
+            jobs: docs.unfinished_snapshot_jobs().await?,
+        })
+    }
+
+    fn tables(&self, status: JobTableStatus) -> BTreeSet<TableRef> {
+        self.jobs
+            .iter()
+            .flat_map(|j| &j.tables)
+            .filter(|t| t.status == status)
+            .map(|t| t.table.clone())
+            .collect()
+    }
+
+    /// The tables a job asks for that no start has taken yet.
+    pub fn asked(&self) -> BTreeSet<TableRef> {
+        self.tables(JobTableStatus::Queued)
+    }
+
+    /// The tables whose job snapshot runs.
+    pub fn running(&self) -> BTreeSet<TableRef> {
+        self.tables(JobTableStatus::Running)
+    }
+
+    /// Records what this start did with the asked tables: the `started` ones run, the `failed`
+    /// ones failed with their reason. A table neither started nor failed stays queued.
+    pub async fn record(
+        &self,
+        docs: &PipelineDocs,
+        started: &BTreeSet<TableRef>,
+        failed: &BTreeMap<TableRef, String>,
+    ) -> Result<(), StateError> {
+        let decided = |t: &TableRef| started.contains(t) || failed.contains_key(t);
+        for job in &self.jobs {
+            if !job
+                .tables
+                .iter()
+                .any(|t| t.status == JobTableStatus::Queued && decided(&t.table))
+            {
+                continue;
+            }
+            docs.update_snapshot_job(&job.job_id, |j| {
+                for t in &mut j.tables {
+                    if t.status != JobTableStatus::Queued {
+                        continue;
+                    }
+                    if started.contains(&t.table) {
+                        t.status = JobTableStatus::Running;
+                    } else if let Some(e) = failed.get(&t.table) {
+                        t.status = JobTableStatus::Failed;
+                        t.error = Some(e.clone());
+                    }
+                }
+                j.settle(Utc::now());
+            })
+            .await?;
+        }
+        Ok(())
+    }
+}
 
 #[cfg(test)]
 mod tests {
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index f7684a6..201dbd5 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -19,6 +19,7 @@ use data_avalanche_storage::StoreError;
 use tokio::sync::{mpsc, watch};
 
 use crate::decide::{Decision, Facts, Plan, decide};
+use crate::jobs::Jobs;
 use crate::schema_change::Planned;
 use crate::session::{Core, End, Session};
 use crate::settings::Settings;
@@ -394,6 +395,9 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         .map(|h| h.table)
         .collect();
     let stored = core.settings.checkpoint.load(&core.docs).await?;
+    let jobs = Jobs::load(&core.docs).await?;
+    let asked = jobs.asked();
+    let running = jobs.running();
 
     // New halts, recorded before the checkpoint below.
     let mut halts: Vec<(TableRef, HaltReason)> = resolved.halted.clone();
@@ -402,6 +406,13 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         .into_iter()
         .map(|t| (t.table().clone(), t))
         .collect();
+    // A job asks for a halted table: its halt is cleared once its schema is baselined.
+    let cleared: BTreeSet<TableRef> = recorded
+        .iter()
+        .filter(|t| asked.contains(*t) && resolved_tables.contains_key(*t))
+        .cloned()
+        .collect();
+    let recorded: BTreeSet<TableRef> = recorded.difference(&cleared).cloned().collect();
     let gone: Vec<TableSchema> = cached
         .values()
         .filter(|c| !resolved_tables.contains_key(c.table()) && !recorded.contains(c.table()))
@@ -432,7 +443,9 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         }
     }
     for (table, now) in &resolved_tables {
-        if let Some(c) = cached.get(table)
+        // A table asked for is baselined to the catalog's, a new source id included.
+        if !asked.contains(table)
+            && let Some(c) = cached.get(table)
             && c.source_id().is_some()
             && c.source_id() != now.source_id()
         {
@@ -530,7 +543,20 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
             sink_has_table: state.exists,
             added: added.contains(table),
         };
-        let (decision, warning) = decide(mode, plan, facts);
+        let dump = matches!(mode, SnapshotMode::DumpAlways | SnapshotMode::DumpInitial);
+        let (decision, warning) = if asked.contains(table) {
+            (
+                Decision::Snapshot {
+                    resume: false,
+                    dump,
+                },
+                None,
+            )
+        } else if running.contains(table) && in_progress {
+            (Decision::Snapshot { resume: !gap, dump }, None)
+        } else {
+            decide(mode, plan, facts)
+        };
         if let Some(w) = warning {
             warnings.push(format!("table {table}: {w}"));
         }
@@ -554,12 +580,15 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
             }
         };
         let mut schema = now.clone();
-        match cached.get(table) {
-            Some(c)
-                if state.exists
-                    && matches!(plan, Plan::Fresh(_))
-                    && c.columns() != now.columns() =>
-            {
+        let baseline = matches!(plan, Plan::Fresh(_)) || cleared.contains(table);
+        // A cleared table's change is planned under its source id now: one created again is
+        // the table the catalog has.
+        let rebased = cached.get(table).map(|c| match now.source_id() {
+            Some(id) if cleared.contains(table) => c.clone().with_source_id(id),
+            _ => c.clone(),
+        });
+        match rebased.as_ref() {
+            Some(c) if state.exists && baseline && c.columns() != now.columns() => {
                 let evolver = core.evolver();
                 let hint = SchemaHint::Columns(now.columns().to_vec());
                 match evolver.plan(c, &hint).await {
@@ -584,6 +613,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
                     }
                 }
             }
+            Some(_) if cleared.contains(table) => {}
             Some(c) if matches!(plan, Plan::Resume) && c.columns() != now.columns() => {
                 drifted.insert(table.clone());
                 let idle = match core.settings.rescan_interval {
@@ -612,7 +642,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
                 Err(e) => return Err(e),
             }
         }
-        if !tracked_before || matches!(plan, Plan::Fresh(_)) {
+        if !tracked_before || baseline {
             schemas.push(schema);
         }
         if request {
@@ -671,6 +701,26 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         core.may_write()?;
         core.docs.save_table_schema(schema).await?;
     }
+    // The cleared halts after the baselined schemas: a restart in between finds the table halted
+    // and asked for still, and baselines it again.
+    for table in &cleared {
+        if !halts.iter().any(|(t, _)| t == table) {
+            core.may_write()?;
+            core.docs.clear_halt(table).await?;
+        }
+    }
+    let started: BTreeSet<TableRef> = requests
+        .iter()
+        .filter(|t| asked.contains(*t))
+        .cloned()
+        .collect();
+    let failed: BTreeMap<TableRef, String> = halts
+        .iter()
+        .filter(|(t, _)| asked.contains(t))
+        .map(|(t, reason)| (t.clone(), format!("{}: {reason}", reason.code())))
+        .collect();
+    core.may_write()?;
+    jobs.record(&core.docs, &started, &failed).await?;
     if let Some(doc) = fresh {
         if core.hooks.at(&HookPoint::SchemasCached) == HookAction::Crash {
             return Err(WorkerError::Crashed(HookPoint::SchemasCached));
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-pipeline --lib jobs::`

Expected: PASS, 4 tests.

Run: `cargo test --workspace`

Expected: PASS (1270 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline
git commit -m "feat(pipeline): Phase 0 takes a job's tables — each read again from its first row, a halted one baselined and its halt cleared, a running one resumed"
```

---

### Task 4: A table outside the config is copied (`data-avalanche-pipeline`)

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/{jobs,worker,session,test_support}.rs`,
  `crates/data-avalanche-pipeline/src/session/snapshot.rs`

**Interfaces:**
- Consumes: Task 2's `describe_tables` and `FakeSource::outside_config`; Task 3's `Jobs`; the
  session's `Accumulator::add_table`, `Snapshots::load`, the dump's retirement (M6a, M6b).
- Produces (crate-private):
  - `Core::copies: BTreeMap<TableRef, TableSchema>`, the copies whose read runs, set by Phase 0;
  - the session's own `copies` map and `Session::read_schema(&self, table: &TableRef) ->
    Option<&TableSchema>` (a streamed table's schema, else a copy's);
  - `Jobs::record` also fails a running job table (a running copy the source lost).

D-M9b.8. The copy test holds the copy's reads (`hold_reads`) and changes a row of it meanwhile:
the change must not reach the lake, during the copy or after it.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-pipeline/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
index 95560f0..e94009e 100644
--- a/crates/data-avalanche-pipeline/src/jobs.rs
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -355,4 +355,130 @@ mod tests {
             JobTableStatus::Running
         );
     }
+
+    /// A table outside the config is copied (Q-M9b-3): described by its name, its sink table
+    /// created, its rows read, and never streamed: a change made while the copy runs, to a row
+    /// its first chunk read before, isn't in the sink. Its schema isn't cached, so a later start
+    /// doesn't take it for a table gone; once its copy is done, a restart doesn't read it again.
+    #[tokio::test]
+    async fn a_table_outside_the_config_is_copied_and_not_streamed() {
+        let w = World::new(&["a", "x"]);
+        w.source.outside_config(&t("x"));
+        established(&w).await;
+        w.source.load(&t("x"), rows(1..=3));
+        ask(&w, "p~1", &["x"]).await;
+        w.source.hold_reads();
+        let (end, st) = run(&w, &never(2), async |status| {
+            wait_until("the first chunk is read", || w.source.reads(&t("x")) == 1).await;
+            w.source.commit(
+                FakeTxn::new()
+                    .update(&t("x"), row(1, "changed"))
+                    .insert(&t("a"), row(1, "streamed")),
+            );
+            w.source.release_reads();
+            wait_until("the copy lands", || w.ids("x") == [1, 2, 3]).await;
+            wait_until("the stream moves on", || w.ids("a") == [1]).await;
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(
+            w.text("x", "doc"),
+            [Some("x".into()), Some("x".into()), Some("x".into())],
+            "a copy isn't streamed"
+        );
+        assert_eq!(snapshot_status(&w, "x").await, SnapshotStatus::Completed);
+        assert!(w.docs.table_schema(&t("x")).await.unwrap().is_none());
+        assert!(st.halted.is_empty() && st.warnings.is_empty(), "{st:?}");
+        assert_eq!(
+            job(&w, "p~1").await.tables[0].status,
+            JobTableStatus::Running
+        );
+        let reads = w.source.reads(&t("x"));
+        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
+        end.unwrap();
+        assert_eq!(
+            w.source.reads(&t("x")),
+            reads,
+            "a copy done isn't read again"
+        );
+        assert!(st.halted.is_empty(), "{:?}", st.halted);
+    }
+
+    /// A copy whose snapshot runs resumes from its cursor at the next start: its table is
+    /// described again, its schema being cached nowhere.
+    #[tokio::test]
+    async fn a_running_copy_resumes_from_its_cursor() {
+        let w = World::new(&["a", "x"]);
+        w.source.outside_config(&t("x"));
+        established(&w).await;
+        w.source.load(&t("x"), rows(1..=4));
+        ask(&w, "p~1", &["x"]).await;
+        w.docs
+            .update_snapshot_job("p~1", |j| {
+                j.tables[0].status = JobTableStatus::Running;
+                j.settle(Utc::now());
+            })
+            .await
+            .unwrap();
+        let spec = data_avalanche_core::TableSpec::new(&w.source.schema(&t("x")), vec![]).unwrap();
+        data_avalanche_core::Sink::ensure_table(&w.sink, &spec)
+            .await
+            .unwrap();
+        w.docs
+            .save_table_snapshot(&TableSnapshotDoc {
+                table: t("x"),
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
+        let (end, _) = run(&w, &never(2), async |status| {
+            wait_until("the rest lands", || w.ids("x") == [3, 4]).await;
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(snapshot_status(&w, "x").await, SnapshotStatus::Completed);
+    }
+
+    /// A table the source doesn't have fails in its job, as does a running copy whose table is
+    /// gone at the next start; nothing is halted for them, as they aren't the pipeline's.
+    #[tokio::test]
+    async fn a_table_the_source_lacks_fails_in_its_job() {
+        let w = World::new(&["a", "x"]);
+        w.source.outside_config(&t("x"));
+        established(&w).await;
+        ask(&w, "p~1", &["missing"]).await;
+        ask(&w, "p~2", &["x"]).await;
+        w.docs
+            .update_snapshot_job("p~2", |j| {
+                j.tables[0].status = JobTableStatus::Running;
+                j.settle(Utc::now());
+            })
+            .await
+            .unwrap();
+        w.source.drop_table(&t("x"));
+        let (end, st) = run(&w, &never(2), async |status| streamed(&status, &w).await).await;
+        end.unwrap();
+        for id in ["p~1", "p~2"] {
+            let j = job(&w, id).await;
+            assert_eq!(j.status, JobStatus::Failed, "{id}");
+            assert_eq!(j.tables[0].error.as_deref(), Some("no such table"), "{id}");
+        }
+        assert!(st.halted.is_empty(), "{:?}", st.halted);
+        assert!(w.docs.halts().await.unwrap().tables.is_empty());
+    }
 }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-pipeline --lib jobs::`

Expected: FAIL, 3 tests (Task 3's four pass): a table outside the config isn't described, so it
isn't read, and a missing one stays queued.

```text
test jobs::tests::a_table_the_source_lacks_fails_in_its_job ... FAILED
test jobs::tests::a_job_table_that_halts_at_the_start_fails ... ok
test jobs::tests::a_running_job_table_resumes_from_its_cursor ... ok
test jobs::tests::a_job_snapshots_a_streamed_table_again_from_scratch ... ok
test jobs::tests::a_job_clears_a_halt_and_baselines_the_table ... ok
test jobs::tests::a_table_outside_the_config_is_copied_and_not_streamed ... FAILED
test jobs::tests::a_running_copy_resumes_from_its_cursor ... FAILED
thread 'jobs::tests::a_table_the_source_lacks_fails_in_its_job' panicked at crates/data-avalanche-pipeline/src/jobs.rs:478:13:
assertion `left == right` failed: p~1
  left: Queued
thread 'jobs::tests::a_table_outside_the_config_is_copied_and_not_streamed' panicked at crates/data-avalanche-pipeline/src/test_support.rs:347:9:
timed out waiting until the first chunk is read
thread 'jobs::tests::a_running_copy_resumes_from_its_cursor' panicked at crates/data-avalanche-pipeline/src/test_support.rs:347:9:
timed out waiting until the rest lands
test result: FAILED. 4 passed; 3 failed; 0 ignored; 0 measured; 216 filtered out; finished in 5.01s
```

- [ ] **Step 3: Implement**

`crates/data-avalanche-pipeline/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
index e94009e..27c91aa 100644
--- a/crates/data-avalanche-pipeline/src/jobs.rs
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -8,7 +8,7 @@ use std::collections::{BTreeMap, BTreeSet};
 
 use chrono::Utc;
 use data_avalanche_core::TableRef;
-use data_avalanche_state::{JobTableStatus, PipelineDocs, SnapshotJobDoc, StateError};
+use data_avalanche_state::{JobTable, JobTableStatus, PipelineDocs, SnapshotJobDoc, StateError};
 
 /// The unfinished jobs, as a start read them.
 #[derive(Debug, Default)]
@@ -42,33 +42,34 @@ impl Jobs {
         self.tables(JobTableStatus::Running)
     }
 
-    /// Records what this start did with the asked tables: the `started` ones run, the `failed`
-    /// ones failed with their reason. A table neither started nor failed stays queued.
+    /// Records what this start did with the job tables: the asked ones `started` run, and the
+    /// `failed` ones (asked, or running and gone) failed with their reason. An asked table
+    /// neither started nor failed stays queued.
     pub async fn record(
         &self,
         docs: &PipelineDocs,
         started: &BTreeSet<TableRef>,
         failed: &BTreeMap<TableRef, String>,
     ) -> Result<(), StateError> {
-        let decided = |t: &TableRef| started.contains(t) || failed.contains_key(t);
+        let decided = |t: &JobTable| match t.status {
+            JobTableStatus::Queued => started.contains(&t.table) || failed.contains_key(&t.table),
+            JobTableStatus::Running => failed.contains_key(&t.table),
+            _ => false,
+        };
         for job in &self.jobs {
-            if !job
-                .tables
-                .iter()
-                .any(|t| t.status == JobTableStatus::Queued && decided(&t.table))
-            {
+            if !job.tables.iter().any(decided) {
                 continue;
             }
             docs.update_snapshot_job(&job.job_id, |j| {
                 for t in &mut j.tables {
-                    if t.status != JobTableStatus::Queued {
+                    if !decided(t) {
                         continue;
                     }
-                    if started.contains(&t.table) {
-                        t.status = JobTableStatus::Running;
-                    } else if let Some(e) = failed.get(&t.table) {
+                    if let Some(e) = failed.get(&t.table) {
                         t.status = JobTableStatus::Failed;
                         t.error = Some(e.clone());
+                    } else {
+                        t.status = JobTableStatus::Running;
                     }
                 }
                 j.settle(Utc::now());
```

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index e032114..fe7ac9f 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -49,6 +49,9 @@ pub(crate) struct Core {
     pub status: watch::Sender<WorkerStatus>,
     /// The tables Phase 0 tracks. A session streams each one unless it is halted or a done dump.
     pub tracked: BTreeSet<TableRef>,
+    /// A job's tables outside the config whose copy runs (Q-M9b-3), with their schemas as Phase
+    /// 0 described them: read like a dump, never streamed, their schemas cached nowhere.
+    pub copies: BTreeMap<TableRef, TableSchema>,
     /// Every table of the pipeline is a `dump_*` table: the worker ends once none is left.
     pub dump_only: bool,
     /// When each table's snapshot began in this run (`timeouts.snapshot`): at the first session
@@ -168,6 +171,8 @@ pub(crate) struct Session<'c> {
     acc: Accumulator,
     /// The streamed tables' schemas as of the frontier.
     schemas: BTreeMap<TableRef, TableSchema>,
+    /// [`Core::copies`] still being read.
+    copies: BTreeMap<TableRef, TableSchema>,
     stream: Box<dyn ChangeStream>,
     jobs: UnboundedSender<Job>,
     events: UnboundedReceiver<WriterEvent>,
@@ -257,7 +262,26 @@ impl<'c> Session<'c> {
                 Err(e) => return Err(e.into()),
             }
         }
-        let (mut snaps, retired, warnings) = Snapshots::load(core, &schemas).await?;
+        let mut copies = BTreeMap::new();
+        for (table, schema) in &core.copies {
+            match acc.add_table(schema.clone(), &s.delete_extra(schema)) {
+                Ok(()) => {
+                    copies.insert(table.clone(), schema.clone());
+                }
+                Err(AccumulatorError::Halt { table, reason }) => halts.push(TableHalt {
+                    table,
+                    reason,
+                    at: checkpoint.clone(),
+                }),
+                Err(e) => return Err(e.into()),
+            }
+        }
+        let read: BTreeMap<TableRef, TableSchema> = schemas
+            .iter()
+            .chain(&copies)
+            .map(|(t, s)| (t.clone(), s.clone()))
+            .collect();
+        let (mut snaps, retired, warnings) = Snapshots::load(core, &read).await?;
         snaps.hold(&core.drifted);
         for table in &retired {
             acc.halt_table(table);
@@ -305,6 +329,7 @@ impl<'c> Session<'c> {
             core,
             acc,
             schemas,
+            copies,
             stream,
             jobs,
             events,
@@ -537,6 +562,7 @@ impl<'c> Session<'c> {
             Some(WriterEvent::Halted(h)) => {
                 self.acc.halt_table(&h.table);
                 self.schemas.remove(&h.table);
+                self.copies.remove(&h.table);
                 self.snaps.stop(&h.table);
                 self.hold_ended();
                 self.core.update(|st| {
@@ -557,9 +583,10 @@ impl<'c> Session<'c> {
                         st.snapshots.remove(&table);
                     });
                     if dump {
-                        // A dump stops once its snapshot is durable.
+                        // A dump, or a copy, stops once its snapshot is durable.
                         self.acc.halt_table(&table);
                         self.schemas.remove(&table);
+                        self.copies.remove(&table);
                     }
                 }
                 if let Err(e) = self.stream.acknowledge(&cp).await {
@@ -1014,6 +1041,11 @@ impl<'c> Session<'c> {
         }
     }
 
+    /// The schema a snapshot reads `table` under: a streamed table's, or a copy's.
+    pub(super) fn read_schema(&self, table: &TableRef) -> Option<&TableSchema> {
+        self.schemas.get(table).or_else(|| self.copies.get(table))
+    }
+
     /// Every table is a dump, and every one is done or halted.
     fn dumped(&self) -> bool {
         self.core.dump_only && self.schemas.is_empty() && self.snaps.is_empty()
@@ -1054,6 +1086,7 @@ impl<'c> Session<'c> {
         }
         self.acc.halt_table(&table);
         self.schemas.remove(&table);
+        self.copies.remove(&table);
         self.snaps.stop(&table);
         self.core.writers_waited.remove(&table);
         self.hold_ended();
```

`crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
index bc0f1ec..f99171f 100644
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -99,8 +99,8 @@ impl std::fmt::Debug for Snapshots {
 }
 
 impl Snapshots {
-    /// The snapshot state of the streamed tables. Returns it with the `dump_*` tables whose
-    /// snapshot is done (they aren't streamed) and warnings.
+    /// The snapshot state of the streamed tables and the copies. Returns it with the `dump_*`
+    /// tables whose snapshot is done (they aren't streamed) and warnings.
     pub(super) async fn load(
         core: &super::Core,
         schemas: &BTreeMap<TableRef, TableSchema>,
@@ -112,7 +112,7 @@ impl Snapshots {
             let Some(v) = core.docs.table_snapshot(table).await? else {
                 continue;
             };
-            let dump = core.settings.is_dump(table);
+            let dump = core.settings.is_dump(table) || core.copies.contains_key(table);
             match v.doc.status {
                 SnapshotStatus::Completed if dump => {
                     retired.insert(table.clone());
@@ -352,7 +352,13 @@ impl Session<'_> {
             {
                 continue;
             }
-            let Some(schema) = self.schemas.get(table).cloned() else {
+            // The fields, not `read_schema`: the loop holds `snaps` mutably.
+            let Some(schema) = self
+                .schemas
+                .get(table)
+                .or_else(|| self.copies.get(table))
+                .cloned()
+            else {
                 continue;
             };
             let writers: Vec<String> = schema.source_id().map_or_else(Vec::new, |id| {
@@ -435,7 +441,7 @@ impl Session<'_> {
             result,
             read_at_micros,
         } = read;
-        let current = self.schemas.get(&table).cloned();
+        let current = self.read_schema(&table).cloned();
         let Some(snap) = self.snaps.tables.get_mut(&table) else {
             self.snaps.reconciler.cancel(ticket);
             return Ok(None);
@@ -593,7 +599,7 @@ impl Session<'_> {
             .iter()
             .filter(|(t, s)| {
                 !s.done
-                    && self.schemas.contains_key(*t)
+                    && self.read_schema(t).is_some()
                     && self
                         .core
                         .snapshot_started
```

`crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
index dd5355a..3cc80ef 100644
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -432,6 +432,7 @@ impl World {
             control: control_rx,
             status,
             tracked: tables.iter().map(|t| TableRef::new("public", *t)).collect(),
+            copies: BTreeMap::new(),
             dump_only: false,
             snapshot_started: BTreeMap::new(),
             drifted: BTreeSet::new(),
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 201dbd5..0c46421 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -106,6 +106,7 @@ impl PipelineWorker {
             control: control_rx,
             status: status_tx,
             tracked: BTreeSet::new(),
+            copies: BTreeMap::new(),
             dump_only: false,
             snapshot_started: BTreeMap::new(),
             drifted: BTreeSet::new(),
@@ -651,6 +652,69 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         tracked.insert(table.clone());
     }
 
+    // A job's tables outside the config: copies, described by name, read without the stream
+    // (Q-M9b-3). One running is described again at every start: its schema is cached nowhere.
+    let configured: BTreeSet<&TableRef> = resolved_tables
+        .keys()
+        .chain(resolved.halted.iter().map(|(t, _)| t))
+        .collect();
+    let outside: Vec<TableRef> = asked
+        .iter()
+        .chain(&running)
+        .filter(|t| !configured.contains(t))
+        .cloned()
+        .collect::<BTreeSet<_>>()
+        .into_iter()
+        .collect();
+    let mut failed: BTreeMap<TableRef, String> = BTreeMap::new();
+    let mut copies = BTreeMap::new();
+    let mut cleared = cleared;
+    if !outside.is_empty() {
+        let described = retry(&tuning, || source.describe_tables(&outside)).await?;
+        warnings.extend(described.warnings);
+        for (table, reason) in described.halted {
+            failed.insert(table, format!("{}: {reason}", reason.code()));
+        }
+        failed.extend(described.skipped);
+        for schema in described.tables {
+            let table = schema.table().clone();
+            let is_asked = asked.contains(&table);
+            if !is_asked {
+                let doc = core.docs.table_snapshot(&table).await?.map(|v| v.doc);
+                if !doc.is_some_and(|d| d.status == SnapshotStatus::InProgress) {
+                    continue;
+                }
+            }
+            let spec = match TableSpec::new(&schema, core.settings.partition_by(&table)) {
+                Ok(spec) => spec,
+                Err(reason) => {
+                    failed.insert(table, format!("{}: {reason}", reason.code()));
+                    continue;
+                }
+            };
+            let state = sink_retry(&tuning, || Ok(()), || sink.table_state(&table)).await?;
+            if !state.exists {
+                match sink_retry(&tuning, || core.may_write(), || sink.ensure_table(&spec)).await {
+                    Ok(()) => {}
+                    Err(WorkerError::Sink(SinkError::Permanent { reason })) => {
+                        let reason = HaltReason::SinkRejected { detail: reason };
+                        failed.insert(table, format!("{}: {reason}", reason.code()));
+                        continue;
+                    }
+                    Err(e) => return Err(e),
+                }
+            }
+            if is_asked {
+                // A copy halted in an earlier run: the job's start clears it.
+                if recorded.contains(&table) {
+                    cleared.insert(table.clone());
+                }
+                requests.push(table.clone());
+            }
+            copies.insert(table, schema);
+        }
+    }
+
     // Persist, in this order: a restart in between redoes what follows.
     for (table, reason) in &halts {
         core.may_write()?;
@@ -714,11 +778,12 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         .filter(|t| asked.contains(*t))
         .cloned()
         .collect();
-    let failed: BTreeMap<TableRef, String> = halts
-        .iter()
-        .filter(|(t, _)| asked.contains(t))
-        .map(|(t, reason)| (t.clone(), format!("{}: {reason}", reason.code())))
-        .collect();
+    failed.extend(
+        halts
+            .iter()
+            .filter(|(t, _)| asked.contains(t))
+            .map(|(t, reason)| (t.clone(), format!("{}: {reason}", reason.code()))),
+    );
     core.may_write()?;
     jobs.record(&core.docs, &started, &failed).await?;
     if let Some(doc) = fresh {
@@ -730,6 +795,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
     }
     core.server = server;
     core.tracked = tracked;
+    core.copies = copies;
     core.drifted = drifted;
     core.dump_only = dump_only;
     core.update(|st| warnings.into_iter().for_each(|w| st.warn(w)));
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-pipeline --lib jobs::`

Expected: PASS, 7 tests.

Run: `cargo test --workspace`

Expected: PASS (1273 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline
git commit -m "feat(pipeline): a job's tables outside the config are copied — described by name, read like a dump, never streamed, cached nowhere"
```

---

### Task 5: The owner follows the jobs (`data-avalanche-cluster`)

**Files:**
- Create: `crates/data-avalanche-cluster/src/jobs.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-cluster/src/controller.rs`

**Interfaces:**
- Consumes: Task 1's `PipelineDocs::{unfinished_snapshot_jobs, update_snapshot_job}`,
  `SnapshotJobDoc::{settle, is_finished}`; `PipelineDocs::{halts, table_snapshot, snapshot_job}`;
  the controller's term loop, its `asked.restart` and `Update::Restart` arms (M9a, M8b).
- Produces (crate-private, `jobs.rs`):
  - `Progress { Running { rows_done: u64 }, Completed { rows: u64 }, Failed(String) }`;
  - `progress(table: &TableRef, halts: &HaltsDoc, doc: Option<&TableSnapshotDoc>) -> Progress`;
  - `Jobs(Option<BTreeSet<String>>)`, the followed job ids, `None` until a listing succeeds:
    `list(&mut self, docs: &PipelineDocs, pipeline: &str)`, `poll(&mut self, docs, pipeline)`.

D-M9b.3, D-M9b.9. The controller calls `jobs.list` after its term's requests baseline, on a taken
restart before `discard`, and on `Update::Restart`; `jobs.poll` at the end of each poll. The
controller tests ask a job the way the API does (`ask_job`, then a restart request under its id)
and wait on the job document (`job_until`).

- [ ] **Step 1: Write the failing tests**

`jobs.rs` starts with its tests only; the controller gets three tests.

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index a97cf74..0f08932 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1239,6 +1239,8 @@ mod tests {
     use data_avalanche_state::Action;
     use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
 
+    use data_avalanche_state::{JobStatus, JobTableStatus, SnapshotJobDoc};
+
     use super::*;
     use crate::test_support::{
         Fault, Kind, ScriptedStore, TestFactory, deps, eventually, holder, leases, node, pipeline,
@@ -1474,6 +1476,104 @@ mod tests {
         assert_eq!(f.builds(), 1);
     }
 
+    /// Waits (on the test clock) until job `id` satisfies `cond`.
+    async fn job_until(
+        docs: &PipelineDocs,
+        id: &str,
+        what: &str,
+        cond: impl Fn(&SnapshotJobDoc) -> bool,
+    ) -> SnapshotJobDoc {
+        for _ in 0..1_200 {
+            if let Some(v) = docs.snapshot_job(id).await.unwrap()
+                && cond(&v.doc)
+            {
+                return v.doc;
+            }
+            tokio::time::sleep(Duration::from_millis(100)).await;
+        }
+        panic!(
+            "timed out waiting until {what}: {:?}",
+            docs.snapshot_job(id).await
+        );
+    }
+
+    async fn ask_job(docs: &PipelineDocs, id: &str, tables: &[&str]) {
+        let doc = SnapshotJobDoc::new(
+            id.into(),
+            tables.iter().map(|n| t(n)).collect(),
+            "ops".into(),
+            chrono::Utc::now(),
+        );
+        docs.create_snapshot_job(&doc).await.unwrap();
+    }
+
+    /// A job asked through the API: its document, then a restart under the job's id. The owner
+    /// restarts the worker, whose start takes the job's tables, and follows each to its end:
+    /// the job completes with the rows each table read.
+    #[tokio::test(start_paused = true)]
+    async fn a_job_asked_runs_to_its_end() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a", "b"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        f.source.load(&t("a"), (1..=3).map(row).collect());
+        ask_job(&docs, "p~1", &["a"]).await;
+        docs.request(Action::Restart, asked("p~1")).await.unwrap();
+        let done = job_until(&docs, "p~1", "the job ends", SnapshotJobDoc::is_finished).await;
+        assert_eq!(done.status, JobStatus::Completed);
+        assert_eq!(done.tables[0].status, JobTableStatus::Completed);
+        assert_eq!(done.tables[0].rows_done, 3);
+        assert!(done.finished_at.is_some());
+        assert_eq!(f.ids("a"), [1, 2, 3]);
+        assert_eq!(f.builds(), 2, "the job's restart");
+    }
+
+    /// A job's table that halts while its snapshot runs fails, with the halt's code, once the
+    /// halt is recorded (with the next flush, here a change of another table's), and the job
+    /// ends failed.
+    #[tokio::test(start_paused = true)]
+    async fn a_job_table_that_halts_fails_the_job() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a", "b"]);
+        let mut n = node("n1", &store, &docs, &f);
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
+        let done = job_until(&docs, "p~1", "the job ends", SnapshotJobDoc::is_finished).await;
+        assert_eq!(done.status, JobStatus::Failed);
+        let error = done.tables[0].error.clone().unwrap();
+        assert!(error.starts_with("TABLE_DROPPED: "), "{error}");
+        assert_eq!(
+            done.error.as_deref(),
+            Some(format!("1 of 1 tables failed: public.a ({error})").as_str())
+        );
+    }
+
+    /// A job left by an earlier owner is followed by the next: its term's first worker takes
+    /// the queued tables, and the owner sees them to their end without a request.
+    #[tokio::test(start_paused = true)]
+    async fn a_new_owner_follows_the_jobs_it_finds() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        f.source.load(&t("a"), (1..=2).map(row).collect());
+        ask_job(&docs, "p~1", &["a"]).await;
+        let _n = node("n1", &store, &docs, &f);
+        let done = job_until(&docs, "p~1", "the job ends", SnapshotJobDoc::is_finished).await;
+        assert_eq!(done.status, JobStatus::Completed);
+        assert_eq!(f.ids("a"), [1, 2]);
+        assert_eq!(f.builds(), 1);
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_paused_pipeline_starts_paused_after_a_restart() {
         let store = ScriptedStore::new();
```

`crates/data-avalanche-cluster/src/jobs.rs` (new):

```diff
diff --git a/crates/data-avalanche-cluster/src/jobs.rs b/crates/data-avalanche-cluster/src/jobs.rs
new file mode 100644
index 0000000..0e9ff16
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/jobs.rs
@@ -0,0 +1,67 @@
+//! The owner's side of the on-demand snapshot jobs (Data-Flow §On-Demand Snapshot). The
+//! worker's start takes a job's queued tables (`data-avalanche-pipeline`); a job asked through
+//! the API comes with a restart under its id, which runs that start. The owner follows each
+//! running table to its end, at its poll, from the table's snapshot document and the halts.
+
+#[cfg(test)]
+mod tests {
+    use data_avalanche_core::HaltReason;
+    use data_avalanche_state::{SnapshotCursor, TableHalt};
+
+    use super::*;
+
+    fn doc(status: SnapshotStatus) -> TableSnapshotDoc {
+        TableSnapshotDoc {
+            table: TableRef::new("public", "a"),
+            status,
+            start_position: None,
+            cursor: None,
+            row_count: None,
+            completed_at: None,
+        }
+    }
+
+    #[test]
+    fn a_running_table_s_progress_is_its_documents() {
+        let a = TableRef::new("public", "a");
+        let none = HaltsDoc::default();
+        let mut running = doc(SnapshotStatus::InProgress);
+        assert_eq!(
+            progress(&a, &none, Some(&running)),
+            Progress::Running { rows_done: 0 }
+        );
+        running.cursor = Some(SnapshotCursor {
+            key: vec![],
+            chunk_size: 10,
+            rows_done: 20,
+            collation: None,
+        });
+        assert_eq!(
+            progress(&a, &none, Some(&running)),
+            Progress::Running { rows_done: 20 }
+        );
+        let mut done = doc(SnapshotStatus::Completed);
+        done.row_count = Some(25);
+        assert_eq!(
+            progress(&a, &none, Some(&done)),
+            Progress::Completed { rows: 25 }
+        );
+        for stopped in [Some(doc(SnapshotStatus::None)), None] {
+            assert_eq!(
+                progress(&a, &none, stopped.as_ref()),
+                Progress::Failed("its snapshot stopped before it completed".into())
+            );
+        }
+        let halted = HaltsDoc {
+            tables: vec![TableHalt {
+                table: a.clone(),
+                reason: HaltReason::SnapshotTimeout { limit_secs: 60 },
+                at: "pg:0/10".parse().unwrap(),
+            }],
+        };
+        let Progress::Failed(why) = progress(&a, &halted, Some(&running)) else {
+            panic!("a halt fails the table")
+        };
+        assert!(why.starts_with("SNAPSHOT_TIMEOUT: "), "{why}");
+    }
+}
```

`crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index b62e6d0..0d33836 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -11,6 +11,7 @@ pub mod coordinator;
 pub mod discovery;
 mod error;
 pub mod gossip;
+mod jobs;
 pub mod lease;
 pub mod node;
 pub mod placement;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib -- jobs:: a_job a_new_owner`

Expected: FAIL to compile: `jobs.rs`'s `progress`, `Progress` and the names it uses aren't there.
With its test module left out, the three controller tests fail: nobody follows the job, so it
stays `running`.

```text
error[E0425]: cannot find type `SnapshotStatus` in this scope
  --> crates/data-avalanche-cluster/src/jobs.rs:13:20
error[E0425]: cannot find type `TableSnapshotDoc` in this scope
  --> crates/data-avalanche-cluster/src/jobs.rs:13:39
error[E0422]: cannot find struct, variant or union type `HaltsDoc` in this scope
  --> crates/data-avalanche-cluster/src/jobs.rs:55:22
error[E0425]: cannot find function `progress` in this scope
  --> crates/data-avalanche-cluster/src/jobs.rs:30:13
…

# with jobs.rs's test module left out:
test controller::tests::a_new_owner_follows_the_jobs_it_finds ... FAILED
test controller::tests::a_job_asked_runs_to_its_end ... FAILED
test controller::tests::a_job_table_that_halts_fails_the_job ... FAILED
thread 'controller::tests::a_job_asked_runs_to_its_end' panicked at crates/data-avalanche-cluster/src/controller.rs:1494:9:
timed out waiting until the job ends: Ok(Some(Versioned { doc: SnapshotJobDoc { job_id: "p~1", status: Running, tables: [JobTable { table: TableRef { schema: "public", name: "a" }, status: Running, rows_done: 0, error: None }], …
test result: FAILED. 0 passed; 3 failed; 0 ignored; 0 measured; 133 filtered out; finished in 0.58s
```

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 0f08932..725153c 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -3,8 +3,8 @@
 //! worker under it and reconciles the worker toward the persisted desired state, polled every
 //! `lease_renewal_interval` (Q-M6b-2). A worker that ends with an error is rerun, stopped for the
 //! lease, or left in `ERROR` (Q-M6b-3). At the same poll the owner writes the pipeline's status,
-//! `status.json`, when it changed, and takes the restart and the flush the management API asked
-//! for in `requests.json` (D-M9a.2).
+//! `status.json`, when it changed, takes the restart and the flush the management API asked for
+//! in `requests.json` (D-M9a.2), and follows the on-demand snapshot jobs to their end.
 
 use std::collections::{BTreeMap, BTreeSet, HashSet};
 use std::fmt::Debug;
@@ -27,6 +27,7 @@ use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
 use tokio::task::JoinHandle;
 use tokio::time::{Instant, MissedTickBehavior};
 
+use crate::jobs::Jobs;
 use crate::{Claim, Claimed, LeaseRun, LeaseTiming, RenewEnd, RenewState, release};
 
 /// A worker's source and sink, built afresh for every run.
@@ -624,6 +625,9 @@ impl PipelineController {
         // now, not at the first poll a renewal interval away, which would take a request made
         // meanwhile as seen. A failed read leaves the first poll's read to take note.
         let mut seen = self.docs.requests().await.ok();
+        // The jobs an earlier term left: this term's first worker takes their queued tables.
+        let mut jobs = Jobs::default();
+        jobs.list(&self.docs, &self.pipeline.name).await;
         let mut reported = Reported::default();
         let mut poll = tokio::time::interval(self.settings.timing.renewal_interval);
         poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
@@ -852,6 +856,8 @@ impl PipelineController {
                     // As a reload's restart: a pipeline in ERROR runs again (Pipeline_Lifecycle
                     // §3), and a paused or stopped one stays so (the API refuses those).
                     if asked.restart {
+                        // A snapshot job asks for its restart: the new worker takes its tables.
+                        jobs.list(&self.docs, &self.pipeline.name).await;
                         if self.discard(&mut worker).await {
                             tracing::info!(pipeline = %self.pipeline.name, "restarted on request");
                         } else {
@@ -873,6 +879,7 @@ impl PipelineController {
                             }
                         }
                     }
+                    jobs.poll(&self.docs, &self.pipeline.name).await;
                 }
                 Event::Update(Update::Reconfigure(p)) => {
                     self.reconfigure(*p);
@@ -887,6 +894,7 @@ impl PipelineController {
                     // The baselines belong to the state location: a reload may move it.
                     reported = Reported::default();
                     seen = self.docs.requests().await.ok();
+                    jobs.list(&self.docs, &self.pipeline.name).await;
                     self.discard(&mut worker).await;
                     tracing::info!(pipeline = %self.pipeline.name, "restarted for a reload");
                     error = None;
@@ -1548,6 +1556,14 @@ mod tests {
         );
         ask_job(&docs, "p~1", &["a"]).await;
         docs.request(Action::Restart, asked("p~1")).await.unwrap();
+        eventually("a second worker", || f.builds() == 2).await;
+        until(&mut n.status, "a halted", |s| {
+            s.worker
+                .as_ref()
+                .is_some_and(|w| w.halted.contains_key(&t("a")))
+        })
+        .await;
+        f.source.commit(FakeTxn::new().insert(&t("b"), row(1)));
         let done = job_until(&docs, "p~1", "the job ends", SnapshotJobDoc::is_finished).await;
         assert_eq!(done.status, JobStatus::Failed);
         let error = done.tables[0].error.clone().unwrap();
```

`crates/data-avalanche-cluster/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/jobs.rs b/crates/data-avalanche-cluster/src/jobs.rs
index 0e9ff16..344a8af 100644
--- a/crates/data-avalanche-cluster/src/jobs.rs
+++ b/crates/data-avalanche-cluster/src/jobs.rs
@@ -3,6 +3,161 @@
 //! the API comes with a restart under its id, which runs that start. The owner follows each
 //! running table to its end, at its poll, from the table's snapshot document and the halts.
 
+use std::collections::{BTreeMap, BTreeSet};
+
+use chrono::Utc;
+use data_avalanche_core::TableRef;
+use data_avalanche_state::{
+    HaltsDoc, JobTableStatus, PipelineDocs, SnapshotJobDoc, SnapshotStatus, StateError,
+    TableSnapshotDoc,
+};
+
+/// What a running job table's documents say of it.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) enum Progress {
+    Running { rows_done: u64 },
+    Completed { rows: u64 },
+    Failed(String),
+}
+
+/// A halt of the table fails it, with the halt's code; else its snapshot document says.
+pub(crate) fn progress(
+    table: &TableRef,
+    halts: &HaltsDoc,
+    doc: Option<&TableSnapshotDoc>,
+) -> Progress {
+    if let Some(h) = halts.tables.iter().find(|h| &h.table == table) {
+        return Progress::Failed(format!("{}: {}", h.reason.code(), h.reason));
+    }
+    match doc {
+        Some(d) if d.status == SnapshotStatus::InProgress => Progress::Running {
+            rows_done: d.cursor.as_ref().map_or(0, |c| c.rows_done),
+        },
+        Some(d) if d.status == SnapshotStatus::Completed => Progress::Completed {
+            rows: d.row_count.unwrap_or(0),
+        },
+        _ => Progress::Failed("its snapshot stopped before it completed".into()),
+    }
+}
+
+fn apply(job: &mut SnapshotJobDoc, seen: &BTreeMap<TableRef, Progress>) {
+    for t in &mut job.tables {
+        if t.status != JobTableStatus::Running {
+            continue;
+        }
+        match seen.get(&t.table) {
+            Some(Progress::Running { rows_done }) => t.rows_done = *rows_done,
+            Some(Progress::Completed { rows }) => {
+                t.status = JobTableStatus::Completed;
+                t.rows_done = *rows;
+            }
+            Some(Progress::Failed(why)) => {
+                t.status = JobTableStatus::Failed;
+                t.error = Some(why.clone());
+            }
+            None => {}
+        }
+    }
+}
+
+/// The unfinished jobs this owner follows; `None` until a listing succeeds.
+#[derive(Debug, Default)]
+pub(crate) struct Jobs(Option<BTreeSet<String>>);
+
+impl Jobs {
+    /// Lists the unfinished jobs: when the term starts, and after a restart taken (a job's
+    /// restart). A failed listing is tried again at the next poll.
+    pub async fn list(&mut self, docs: &PipelineDocs, pipeline: &str) {
+        self.0 = match docs.unfinished_snapshot_jobs().await {
+            Ok(jobs) => Some(jobs.into_iter().map(|j| j.job_id).collect()),
+            Err(e) => {
+                tracing::warn!(
+                    pipeline,
+                    "snapshot jobs unlisted, listed at the next poll: {e}"
+                );
+                None
+            }
+        };
+    }
+
+    /// One poll: each job's running tables, from their documents, into the job; a job that
+    /// ended is followed no more.
+    pub async fn poll(&mut self, docs: &PipelineDocs, pipeline: &str) {
+        if self.0.is_none() {
+            self.list(docs, pipeline).await;
+        }
+        let Some(ids) = self.0.as_mut() else {
+            return;
+        };
+        if ids.is_empty() {
+            return;
+        }
+        let halts = match docs.halts().await {
+            Ok(h) => h,
+            Err(e) => {
+                tracing::warn!(
+                    pipeline,
+                    "halts unread, snapshot jobs followed at the next poll: {e}"
+                );
+                return;
+            }
+        };
+        let mut ended = vec![];
+        for id in ids.iter() {
+            match follow(docs, id, &halts).await {
+                Ok(Some(job)) => {
+                    tracing::info!(pipeline, job = %id, status = ?job.status, "snapshot job ended");
+                    ended.push(id.clone());
+                }
+                Ok(None) => {}
+                Err(e) => {
+                    tracing::warn!(pipeline, job = %id, "snapshot job unfollowed this poll: {e}");
+                }
+            }
+        }
+        for id in ended {
+            ids.remove(&id);
+        }
+    }
+}
+
+/// Brings job `id` up to date; `Some` once it has ended (or is gone).
+async fn follow(
+    docs: &PipelineDocs,
+    id: &str,
+    halts: &HaltsDoc,
+) -> Result<Option<SnapshotJobDoc>, StateError> {
+    let Some(v) = docs.snapshot_job(id).await? else {
+        return Ok(None);
+    };
+    if v.doc.is_finished() {
+        return Ok(Some(v.doc));
+    }
+    let mut seen = BTreeMap::new();
+    for t in v
+        .doc
+        .tables
+        .iter()
+        .filter(|t| t.status == JobTableStatus::Running)
+    {
+        let doc = docs.table_snapshot(&t.table).await?.map(|v| v.doc);
+        seen.insert(t.table.clone(), progress(&t.table, halts, doc.as_ref()));
+    }
+    let mut next = v.doc.clone();
+    apply(&mut next, &seen);
+    next.settle(Utc::now());
+    if next == v.doc {
+        return Ok(None);
+    }
+    let job = docs
+        .update_snapshot_job(id, |j| {
+            apply(j, &seen);
+            j.settle(Utc::now());
+        })
+        .await?;
+    Ok(job.filter(SnapshotJobDoc::is_finished))
+}
+
 #[cfg(test)]
 mod tests {
     use data_avalanche_core::HaltReason;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib -- jobs:: a_job a_new_owner`

Expected: PASS, 4 tests.

Run: `cargo test --workspace`

Expected: PASS (1277 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster
git commit -m "feat(cluster): the owner lists the snapshot jobs at its term's start and at a restart, and follows each table to its end"
```

---

### Task 6: The snapshot routes (`data-avalanche-api`, `data-avalanche-config`)

**Files:**
- Create: `crates/data-avalanche-api/src/snapshots.rs`
- Modify: `crates/data-avalanche-api/src/{lib,pipelines,openapi}.rs`, `crates/data-avalanche-api/openapi.json`
- Modify: `crates/data-avalanche-config/src/{external,lib}.rs`, `Cargo.toml`

**Interfaces:**
- Consumes: Task 1's `SnapshotJobDoc::new`, `PipelineDocs::{create_snapshot_job,
  unfinished_snapshot_jobs, snapshot_jobs, snapshot_job}`, `JobTableStatus`; M9a's route table
  (`v1()`, `Route`, `r`), `Failure`, `body`, `starting`, `PipelineDocs::request(Action::Restart,
  Request)`.
- Produces:
  - `data_avalanche_config::key_table(p: &Pipeline, key: &str) -> Option<TableRef>`, now public
    (PG `schema.table`, MySQL `name` in `source.db`);
  - in `pipelines.rs`, now `pub(crate)`: `unavailable`, `find`, `desired_name`, `Read { desired,
    lease, status }`, `read`;
  - `snapshots.rs`: the handlers `ask`, `list`, `get`; `new_job_id(pipeline_id) -> String`,
    `job_pipeline(job_id) -> Option<&str>`, `tables(p, text) -> Result<Vec<TableRef>, String>`,
    `job_json(p, job) -> Value`;
  - the routes `POST /v1/pipelines/{id}/snapshots` (operator), `GET /v1/pipelines/{id}/snapshots`
    (viewer), `GET /v1/snapshots/{job_id}` (viewer); the OpenAPI `job_id` parameter.

D-M9b.1, D-M9b.4, D-M9b.11. The tests use M9a's `Lone` test node; the matrix test
(`the_routes_take_the_roles_of_api_rbac`) gets the three rows.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 3fcd77b..f81fb6a 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -14,6 +14,7 @@ mod config;
 mod openapi;
 mod pipelines;
 mod server;
+mod snapshots;
 #[cfg(test)]
 mod test_support;
 
@@ -421,6 +422,9 @@ mod tests {
                 ("POST", "/v1/pipelines/{id}/resume", Operator),
                 ("POST", "/v1/pipelines/{id}/restart", Operator),
                 ("POST", "/v1/pipelines/{id}/flush", Operator),
+                ("POST", "/v1/pipelines/{id}/snapshots", Operator),
+                ("GET", "/v1/pipelines/{id}/snapshots", Viewer),
+                ("GET", "/v1/snapshots/{job_id}", Viewer),
                 ("POST", "/v1/cluster/leave", Admin),
                 ("POST", "/v1/admin/shutdown", Admin),
                 ("GET", "/v1/openapi.json", Viewer),
```

`crates/data-avalanche-api/src/snapshots.rs` (new):

```diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
new file mode 100644
index 0000000..1c772d8
--- /dev/null
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -0,0 +1,206 @@
+//! The on-demand snapshot jobs (Data-Flow §On-Demand Snapshot).
+
+#[cfg(test)]
+mod tests {
+    use std::net::SocketAddr;
+    use std::time::Duration;
+
+    use chrono::Utc;
+    use data_avalanche_core::TableRef;
+    use data_avalanche_state::{
+        Acquired, DesiredState, Holder, JobStatus, JobTableStatus, PipelineDocs, SnapshotJobDoc,
+    };
+    use serde_json::{Value, json};
+    use tempfile::TempDir;
+
+    use crate::test_support::{CONFIG, Lone, api, json_of, lone, open, request, serve};
+
+    /// A lone node's API: the pipeline `orders` of [`CONFIG`], its state in memory.
+    async fn served() -> (SocketAddr, Lone, PipelineDocs, TempDir) {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, CONFIG).unwrap();
+        let lone = lone(file).await;
+        let config = lone.node.coordinator.config().await;
+        let docs = (lone.node.docs)(&config.pipelines["orders"]).unwrap();
+        let a = api(open());
+        a.node.set(lone.node.clone()).unwrap();
+        (serve(a).await, lone, docs, dir)
+    }
+
+    async fn held(lone: &Lone) {
+        let holder = Holder {
+            node_id: "node-b".into(),
+            incarnation: 1,
+        };
+        let got = lone
+            .leases
+            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
+            .await
+            .unwrap();
+        assert!(matches!(got, Acquired::Mine(_)));
+    }
+
+    async fn ask(addr: SocketAddr, body: &str) -> (u16, Value) {
+        let (code, _, text) = request(
+            addr,
+            "POST",
+            "/v1/pipelines/orders/snapshots",
+            &[("Content-Type", "application/json")],
+            body,
+        )
+        .await;
+        (
+            code,
+            serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}")),
+        )
+    }
+
+    /// A job is recorded queued, then asked of the lease holder as a restart under the job's
+    /// id; its id names the pipeline, so any node reads it.
+    #[tokio::test]
+    async fn a_job_is_recorded_then_asked_of_the_holder() {
+        let (addr, lone, docs, _dir) = served().await;
+        let tables = r#"{"tables": ["public.orders", "public.items", "public.orders"]}"#;
+        let (code, v) = ask(addr, tables).await;
+        assert_eq!(code, 409, "no holder: {v}");
+        assert!(v["error"].as_str().unwrap().contains("lease"), "{v}");
+        assert!(docs.snapshot_jobs().await.unwrap().is_empty());
+
+        held(&lone).await;
+        let (code, v) = ask(addr, tables).await;
+        assert_eq!(code, 202, "{v}");
+        let id = v["job_id"].as_str().unwrap().to_owned();
+        let (pipeline, suffix) = id.split_once('~').unwrap();
+        assert_eq!(pipeline, "orders");
+        assert_eq!(suffix.len(), 32, "{id}");
+        assert_eq!(v["status"], "queued");
+        assert_eq!(
+            v["tables"],
+            json!(["public.orders", "public.items"]),
+            "once each"
+        );
+
+        let job = docs.snapshot_job(&id).await.unwrap().unwrap().doc;
+        assert_eq!(job.status, JobStatus::Queued);
+        assert_eq!(job.requested_by, "anonymous");
+        assert_eq!(
+            job.tables
+                .iter()
+                .map(|t| t.table.clone())
+                .collect::<Vec<_>>(),
+            [
+                TableRef::new("public", "orders"),
+                TableRef::new("public", "items")
+            ]
+        );
+        assert_eq!(docs.requests().await.unwrap().restart.unwrap().id, id);
+
+        let (code, one) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
+        assert_eq!(code, 200, "{one}");
+        assert_eq!(one["job_id"], id.as_str());
+        assert_eq!(one["pipeline"], "orders");
+        assert_eq!(one["status"], "queued");
+        assert_eq!(
+            one["tables"][0],
+            json!({ "table": "public.orders", "status": "queued", "rows_done": 0, "error": null })
+        );
+        let (code, all) = json_of(addr, "GET", "/v1/pipelines/orders/snapshots").await;
+        assert_eq!(code, 200, "{all}");
+        assert_eq!(all["jobs"], json!([one]));
+    }
+
+    /// A job is asked of a running pipeline only (Q-M9b-4), as a restart is.
+    #[tokio::test]
+    async fn a_job_of_a_paused_or_stopped_pipeline_is_refused() {
+        let (addr, lone, docs, _dir) = served().await;
+        held(&lone).await;
+        docs.open("orders", "delta").await.unwrap();
+        for state in [DesiredState::Paused, DesiredState::Stopped] {
+            docs.set_desired_state(state).await.unwrap();
+            let (code, v) = ask(addr, r#"{"tables": ["public.orders"]}"#).await;
+            assert_eq!(code, 409, "{state:?}: {v}");
+        }
+        assert!(docs.snapshot_jobs().await.unwrap().is_empty());
+    }
+
+    /// A table an unfinished job holds is refused, naming the job; once that job ended, it can
+    /// be asked again.
+    #[tokio::test]
+    async fn a_table_an_unfinished_job_holds_is_refused() {
+        let (addr, lone, docs, _dir) = served().await;
+        held(&lone).await;
+        let (_, first) = ask(addr, r#"{"tables": ["public.orders"]}"#).await;
+        let first = first["job_id"].as_str().unwrap().to_owned();
+        let (code, v) = ask(addr, r#"{"tables": ["public.items", "public.orders"]}"#).await;
+        assert_eq!(code, 409, "{v}");
+        let error = v["error"].as_str().unwrap();
+        assert!(
+            error.contains("public.orders") && error.contains(&first),
+            "{v}"
+        );
+        docs.update_snapshot_job(&first, |j| {
+            j.tables[0].status = JobTableStatus::Completed;
+            j.settle(Utc::now());
+        })
+        .await
+        .unwrap();
+        let (code, v) = ask(addr, r#"{"tables": ["public.items", "public.orders"]}"#).await;
+        assert_eq!(code, 202, "{v}");
+    }
+
+    /// The body names tables, each exactly (System-Overview: "tables list, no globs"), as the
+    /// source's per-table config keys do: PostgreSQL `schema.table`.
+    #[tokio::test]
+    async fn a_request_names_its_tables_exactly() {
+        let (addr, lone, docs, _dir) = served().await;
+        held(&lone).await;
+        for body in [
+            "",
+            "not json",
+            r#"{"tables": "public.orders"}"#,
+            r#"{"tables": []}"#,
+            r#"{"tables": ["public.*"]}"#,
+            r#"{"tables": ["orders"]}"#,
+            r#"{"tables": ["public.orders"], "truncate": true}"#,
+        ] {
+            let (code, v) = ask(addr, body).await;
+            assert_eq!(code, 422, "{body:?}: {v}");
+            assert!(v["error"].as_str().is_some(), "{v}");
+        }
+        assert!(docs.snapshot_jobs().await.unwrap().is_empty());
+    }
+
+    /// A job id that names no pipeline of the generation, or no job of it, is 404; so is the
+    /// list of a pipeline that isn't there.
+    #[tokio::test]
+    async fn a_job_is_found_by_its_id() {
+        let (addr, _lone, docs, _dir) = served().await;
+        for id in ["nope", "other~0190", "orders~0190", "orders~"] {
+            let (code, v) = json_of(addr, "GET", &format!("/v1/snapshots/{id}")).await;
+            assert_eq!(code, 404, "{id}: {v}");
+        }
+        let mut job = SnapshotJobDoc::new(
+            "orders~0190".into(),
+            vec![TableRef::new("public", "orders")],
+            "ops".into(),
+            Utc::now(),
+        );
+        job.tables[0].status = JobTableStatus::Failed;
+        job.tables[0].error = Some("no such table".into());
+        job.settle(Utc::now());
+        docs.create_snapshot_job(&job).await.unwrap();
+        let (code, v) = json_of(addr, "GET", "/v1/snapshots/orders~0190").await;
+        assert_eq!(code, 200, "{v}");
+        assert_eq!(v["status"], "failed");
+        assert_eq!(
+            v["error"],
+            "1 of 1 tables failed: public.orders (no such table)"
+        );
+        assert!(v["finished_at"].is_string(), "{v}");
+        assert_eq!(
+            json_of(addr, "GET", "/v1/pipelines/nope/snapshots").await.0,
+            404
+        );
+    }
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-api --lib snapshots::`

Expected: FAIL, 5 tests: the routes aren't there, so every answer is a bare `404` with no JSON
body. `the_routes_take_the_roles_of_api_rbac` fails too: the route table lacks `lib.rs`'s three
new rows.

```text
test snapshots::tests::a_job_is_recorded_then_asked_of_the_holder ... FAILED
test snapshots::tests::a_table_an_unfinished_job_holds_is_refused ... FAILED
test snapshots::tests::a_job_is_found_by_its_id ... FAILED
test snapshots::tests::a_job_of_a_paused_or_stopped_pipeline_is_refused ... FAILED
test snapshots::tests::a_request_names_its_tables_exactly ... FAILED
thread 'snapshots::tests::a_request_names_its_tables_exactly' panicked at crates/data-avalanche-api/src/snapshots.rs:55:60:
EOF while parsing a value at line 1 column 0: 
thread 'snapshots::tests::a_job_is_found_by_its_id' panicked at crates/data-avalanche-api/src/test_support.rs:114:56:
EOF while parsing a value at line 1 column 0: 
test result: FAILED. 0 passed; 5 failed; 0 ignored; 0 measured; 41 filtered out; finished in 0.01s
```

- [ ] **Step 3: Implement**

The routes after flush in `v1()`; the OpenAPI document blessed with `DATA_AVALANCHE_BLESS=1
cargo test -p data-avalanche-api --lib openapi::` (its diff is below).

`Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index f08ce37..7890a87 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -33,7 +33,7 @@ thiserror = "2"
 serde = { version = "1", features = ["derive"] }
 serde_json = { version = "1", features = ["arbitrary_precision"] }
 chrono = { version = "0.4", default-features = false, features = ["std", "serde"] }
-uuid = { version = "1", features = ["serde", "v4"] }
+uuid = { version = "1", features = ["serde", "v4", "v7"] }
 proptest = "1"
 tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
 object_store = { version = "0.14.2", features = ["aws", "gcp", "azure"] }
```

`crates/data-avalanche-api/openapi.json`:

```diff
diff --git a/crates/data-avalanche-api/openapi.json b/crates/data-avalanche-api/openapi.json
index 7f69bb7..9f29d58 100644
--- a/crates/data-avalanche-api/openapi.json
+++ b/crates/data-avalanche-api/openapi.json
@@ -728,6 +728,142 @@
         "x-min-role": "operator"
       }
     },
+    "/v1/pipelines/{id}/snapshots": {
+      "get": {
+        "tags": [
+          "pipelines"
+        ],
+        "summary": "The pipeline's snapshot jobs, oldest first",
+        "parameters": [
+          {
+            "name": "id",
+            "in": "path",
+            "description": "the pipeline's id",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the jobs",
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
+          "404": {
+            "description": "no such pipeline in the generation this node runs",
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
+      },
+      "post": {
+        "tags": [
+          "pipelines"
+        ],
+        "summary": "Snapshot tables on demand, `{\"tables\": [...]}`, each named exactly; one outside the config is copied, not streamed",
+        "parameters": [
+          {
+            "name": "id",
+            "in": "path",
+            "description": "the pipeline's id",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "requestBody": {
+          "content": {
+            "application/json": {}
+          },
+          "required": false
+        },
+        "responses": {
+          "202": {
+            "description": "the job is queued: the owner restarts the worker, which takes its tables",
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
+          "404": {
+            "description": "no such pipeline in the generation this node runs",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "409": {
+            "description": "paused, stopped, no node holds the lease, or an unfinished job holds a table",
+            "content": {
+              "application/json": {}
+            }
+          },
+          "422": {
+            "description": "the body names no table, a glob, or a table not in the source's form",
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
+        "x-min-role": "operator"
+      }
+    },
     "/v1/pipelines/{id}/stop": {
       "post": {
         "tags": [
@@ -793,6 +929,66 @@
         ],
         "x-min-role": "operator"
       }
+    },
+    "/v1/snapshots/{job_id}": {
+      "get": {
+        "tags": [
+          "snapshots"
+        ],
+        "summary": "A snapshot job: its status and each table's",
+        "parameters": [
+          {
+            "name": "job_id",
+            "in": "path",
+            "description": "the snapshot job's id, `<pipeline id>~<suffix>`",
+            "required": true,
+            "schema": {
+              "type": "string"
+            }
+          }
+        ],
+        "responses": {
+          "200": {
+            "description": "the job",
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
+          "404": {
+            "description": "no such job",
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
     }
   },
   "components": {
```

`crates/data-avalanche-api/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index f81fb6a..6390c72 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -257,6 +257,53 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             post(pipelines::flush),
         ),
+        (
+            Route {
+                body: Some("application/json"),
+                ..r(
+                    "POST",
+                    "/v1/pipelines/{id}/snapshots",
+                    Operator,
+                    "Snapshot tables on demand, `{\"tables\": [...]}`, each named exactly; one outside the config is copied, not streamed",
+                    &[
+                        (
+                            202,
+                            "the job is queued: the owner restarts the worker, which takes its tables",
+                        ),
+                        UNKNOWN,
+                        (
+                            409,
+                            "paused, stopped, no node holds the lease, or an unfinished job holds a table",
+                        ),
+                        (
+                            422,
+                            "the body names no table, a glob, or a table not in the source's form",
+                        ),
+                    ],
+                )
+            },
+            post(snapshots::ask),
+        ),
+        (
+            r(
+                "GET",
+                "/v1/pipelines/{id}/snapshots",
+                Viewer,
+                "The pipeline's snapshot jobs, oldest first",
+                &[(200, "the jobs"), UNKNOWN],
+            ),
+            get(snapshots::list),
+        ),
+        (
+            r(
+                "GET",
+                "/v1/snapshots/{job_id}",
+                Viewer,
+                "A snapshot job: its status and each table's",
+                &[(200, "the job"), (404, "no such job")],
+            ),
+            get(snapshots::get),
+        ),
         (
             r(
                 "POST",
```

`crates/data-avalanche-api/src/openapi.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index 219f26f..cdd9952 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -38,15 +38,20 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
         .extensions(Some(
             ExtensionsBuilder::new().add("x-min-role", role).build(),
         ));
-    if r.path.contains("{id}") {
-        op = op.parameter(
-            ParameterBuilder::new()
-                .name("id")
-                .parameter_in(ParameterIn::Path)
-                .required(Required::True)
-                .description(Some("the pipeline's id"))
-                .schema(Some(ObjectBuilder::new().schema_type(Type::String))),
-        );
+    for (name, description) in [
+        ("id", "the pipeline's id"),
+        ("job_id", "the snapshot job's id, `<pipeline id>~<suffix>`"),
+    ] {
+        if r.path.contains(&format!("{{{name}}}")) {
+            op = op.parameter(
+                ParameterBuilder::new()
+                    .name(name)
+                    .parameter_in(ParameterIn::Path)
+                    .required(Required::True)
+                    .description(Some(description))
+                    .schema(Some(ObjectBuilder::new().schema_type(Type::String))),
+            );
+        }
     }
     if let Some(content_type) = r.body {
         op = op.request_body(Some(
@@ -175,8 +180,10 @@ mod tests {
                         "{method} {path} {code}"
                     );
                 }
-                if path.contains("{id}") {
-                    assert_eq!(op["parameters"][0]["name"], "id", "{method} {path}");
+                for name in ["id", "job_id"] {
+                    if path.contains(&format!("{{{name}}}")) {
+                        assert_eq!(op["parameters"][0]["name"], name, "{method} {path}");
+                    }
                 }
             }
         }
```

`crates/data-avalanche-api/src/pipelines.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index c29a2f4..5705307 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -19,7 +19,7 @@ use serde_json::{Value, json};
 use crate::{Api, Failure, Node, Principal, body, starting};
 
 /// What a status read failed on: 503, the store is the answer.
-fn unavailable(e: &StateError) -> Response {
+pub(crate) fn unavailable(e: &StateError) -> Response {
     body(
         StatusCode::SERVICE_UNAVAILABLE,
         &json!({ "error": format!("the pipeline's state: {e}") }),
@@ -27,7 +27,7 @@ fn unavailable(e: &StateError) -> Response {
 }
 
 /// The pipeline with this id in the generation the node runs, and its state documents.
-async fn find(node: &Node, id: &str) -> Result<(Pipeline, PipelineDocs), Failure> {
+pub(crate) async fn find(node: &Node, id: &str) -> Result<(Pipeline, PipelineDocs), Failure> {
     let config = node.coordinator.config().await;
     let Some(p) = config.pipelines.values().find(|p| p.id == id).cloned() else {
         return Err(Failure::new(
@@ -44,7 +44,7 @@ async fn find(node: &Node, id: &str) -> Result<(Pipeline, PipelineDocs), Failure
     Ok((p, docs))
 }
 
-fn desired_name(d: DesiredState) -> &'static str {
+pub(crate) fn desired_name(d: DesiredState) -> &'static str {
     match d {
         DesiredState::Running => "running",
         DesiredState::Paused => "paused",
@@ -82,13 +82,17 @@ fn actual<'s>(
 }
 
 /// The documents a status is read from.
-struct Read {
-    desired: DesiredState,
-    lease: Option<LeaseDoc>,
-    status: Option<StatusDoc>,
+pub(crate) struct Read {
+    pub desired: DesiredState,
+    pub lease: Option<LeaseDoc>,
+    pub status: Option<StatusDoc>,
 }
 
-async fn read(node: &Node, p: &Pipeline, docs: &PipelineDocs) -> Result<Read, StateError> {
+pub(crate) async fn read(
+    node: &Node,
+    p: &Pipeline,
+    docs: &PipelineDocs,
+) -> Result<Read, StateError> {
     let (state, lease, status) =
         tokio::join!(docs.state(), node.leases.observe(&p.id), docs.status());
     Ok(Read {
```

`crates/data-avalanche-api/src/snapshots.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index 1c772d8..26899d8 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -1,4 +1,243 @@
-//! The on-demand snapshot jobs (Data-Flow §On-Demand Snapshot).
+//! The on-demand snapshot jobs (Data-Flow §On-Demand Snapshot). A job is asked of a running
+//! pipeline whose lease a node holds, as a restart is (Q-M9b-4): its document is written
+//! `queued`, then a restart under the job's id, which the owner takes at its next poll. The new
+//! worker's start takes the job's tables and the owner follows them to their end. The job's id
+//! is `<pipeline id>~<suffix>` (Q-M9-2), so any node reads a job by its id from the pipeline's
+//! state.
+
+use std::collections::BTreeSet;
+
+use axum::Extension;
+use axum::extract::{Path, State};
+use axum::http::StatusCode;
+use axum::response::Response;
+use chrono::Utc;
+use data_avalanche_config::{Pipeline, SourceType, key_table};
+use data_avalanche_core::TableRef;
+use data_avalanche_state::{
+    Action, DesiredState, JobStatus, JobTableStatus, Request, SnapshotJobDoc,
+};
+use serde::Deserialize;
+use serde_json::{Value, json};
+
+use crate::pipelines::{desired_name, find, read, unavailable};
+use crate::{Api, Failure, Principal, body, starting};
+
+/// Between a job's pipeline id and its suffix: a pipeline id has none (V-055).
+const SEPARATOR: char = '~';
+
+/// A new job id: the pipeline's, then a UUID v7, time-ordered.
+fn new_job_id(pipeline_id: &str) -> String {
+    format!("{pipeline_id}{SEPARATOR}{}", uuid::Uuid::now_v7().simple())
+}
+
+/// The pipeline id a job id names.
+fn job_pipeline(job_id: &str) -> Option<&str> {
+    match job_id.split_once(SEPARATOR) {
+        Some((p, suffix)) if !p.is_empty() && !suffix.is_empty() => Some(p),
+        _ => None,
+    }
+}
+
+/// A table as the request and the answers name it: the source's per-table config key.
+fn table_name(p: &Pipeline, t: &TableRef) -> String {
+    match p.source.kind {
+        SourceType::Postgres => t.to_string(),
+        SourceType::Mysql => t.name.clone(),
+    }
+}
+
+#[derive(Deserialize)]
+#[serde(deny_unknown_fields)]
+struct Asked {
+    tables: Vec<String>,
+}
+
+/// The body's tables, each once, in its order: named as the source's per-table config keys,
+/// no globs.
+fn tables(p: &Pipeline, text: &str) -> Result<Vec<TableRef>, String> {
+    let asked: Asked = serde_json::from_str(text)
+        .map_err(|e| format!("the body is {{\"tables\": [\"<table>\", ...]}}: {e}"))?;
+    if asked.tables.is_empty() {
+        return Err("the body names no table".into());
+    }
+    let mut seen = BTreeSet::new();
+    let mut out = vec![];
+    for name in &asked.tables {
+        if name.contains('*') {
+            return Err(format!(
+                "{name:?}: a table is named exactly, without a glob"
+            ));
+        }
+        let table = match p.source.kind {
+            SourceType::Mysql if name.contains('.') => None,
+            _ => key_table(p, name),
+        }
+        .filter(|t| !t.schema.is_empty() && !t.name.is_empty())
+        .ok_or_else(|| match p.source.kind {
+            SourceType::Postgres => format!("{name:?}: a PostgreSQL table is schema.table"),
+            SourceType::Mysql => format!(
+                "{name:?}: a MySQL table is named without its database ({})",
+                p.source.db
+            ),
+        })?;
+        if seen.insert(table.clone()) {
+            out.push(table);
+        }
+    }
+    Ok(out)
+}
+
+fn job_json(p: &Pipeline, j: &SnapshotJobDoc) -> Value {
+    let status = |s: JobStatus| match s {
+        JobStatus::Queued => "queued",
+        JobStatus::Running => "running",
+        JobStatus::Completed => "completed",
+        JobStatus::Failed => "failed",
+    };
+    let table_status = |s: JobTableStatus| match s {
+        JobTableStatus::Queued => "queued",
+        JobTableStatus::Running => "running",
+        JobTableStatus::Completed => "completed",
+        JobTableStatus::Failed => "failed",
+    };
+    json!({
+        "job_id": j.job_id,
+        "pipeline": p.id,
+        "status": status(j.status),
+        "tables": j.tables.iter().map(|t| json!({
+            "table": table_name(p, &t.table),
+            "status": table_status(t.status),
+            "rows_done": t.rows_done,
+            "error": t.error,
+        })).collect::<Vec<_>>(),
+        "requested_at": j.requested_at,
+        "requested_by": j.requested_by,
+        "finished_at": j.finished_at,
+        "error": j.error,
+    })
+}
+
+/// `POST /v1/pipelines/{id}/snapshots`: 202 with the job, once its document and its restart
+/// are written.
+pub(crate) async fn ask(
+    State(api): State<Api>,
+    Path(id): Path<String>,
+    Extension(by): Extension<Principal>,
+    text: String,
+) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (p, docs) = match find(node, &id).await {
+        Ok(found) => found,
+        Err(f) => return f.into(),
+    };
+    let tables = match tables(&p, &text) {
+        Ok(tables) => tables,
+        Err(why) => return Failure::new(StatusCode::UNPROCESSABLE_ENTITY, why).into(),
+    };
+    let r = match read(node, &p, &docs).await {
+        Ok(r) => r,
+        Err(e) => return unavailable(&e),
+    };
+    let conflict = |why: String| {
+        body(
+            StatusCode::CONFLICT,
+            &json!({ "error": why, "desired_state": desired_name(r.desired) }),
+        )
+    };
+    if r.desired != DesiredState::Running {
+        return conflict(format!(
+            "a {} pipeline takes no snapshot job; resume it first",
+            desired_name(r.desired)
+        ));
+    }
+    if r.lease.as_ref().is_none_or(|l| l.released) {
+        return conflict("no node holds the pipeline's lease, so none can run a job".into());
+    }
+    let unfinished = match docs.unfinished_snapshot_jobs().await {
+        Ok(jobs) => jobs,
+        Err(e) => return unavailable(&e),
+    };
+    for j in &unfinished {
+        if let Some(t) = j.tables.iter().find(|t| {
+            matches!(t.status, JobTableStatus::Queued | JobTableStatus::Running)
+                && tables.contains(&t.table)
+        }) {
+            return conflict(format!(
+                "job {} snapshots {} already; ask again once it ended",
+                j.job_id,
+                table_name(&p, &t.table)
+            ));
+        }
+    }
+    let job = SnapshotJobDoc::new(new_job_id(&p.id), tables, by.name.clone(), Utc::now());
+    if let Err(e) = docs.create_snapshot_job(&job).await {
+        return unavailable(&e);
+    }
+    let restart = Request {
+        id: job.job_id.clone(),
+        at: job.requested_at,
+    };
+    if let Err(e) = docs.request(Action::Restart, restart).await {
+        return body(
+            StatusCode::SERVICE_UNAVAILABLE,
+            &json!({
+                "error": format!("the job is recorded, but its restart isn't ({e}): it runs at the pipeline's next start"),
+                "job_id": job.job_id,
+            }),
+        );
+    }
+    tracing::info!(pipeline = %p.name, by = %by.name, job = %job.job_id, tables = job.tables.len(), "snapshot job asked through the API");
+    let v = job_json(&p, &job);
+    body(
+        StatusCode::ACCEPTED,
+        &json!({
+            "job_id": v["job_id"],
+            "status": v["status"],
+            "tables": v["tables"].as_array().into_iter().flatten().map(|t| t["table"].clone()).collect::<Vec<_>>(),
+        }),
+    )
+}
+
+/// `GET /v1/pipelines/{id}/snapshots`: every job of the pipeline, oldest request first.
+pub(crate) async fn list(State(api): State<Api>, Path(id): Path<String>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (p, docs) = match find(node, &id).await {
+        Ok(found) => found,
+        Err(f) => return f.into(),
+    };
+    match docs.snapshot_jobs().await {
+        Ok(jobs) => body(
+            StatusCode::OK,
+            &json!({ "jobs": jobs.iter().map(|v| job_json(&p, &v.doc)).collect::<Vec<_>>() }),
+        ),
+        Err(e) => unavailable(&e),
+    }
+}
+
+/// `GET /v1/snapshots/{job_id}`: the job, read from the state of the pipeline its id names.
+pub(crate) async fn get(State(api): State<Api>, Path(job_id): Path<String>) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let missing = || Failure::new(StatusCode::NOT_FOUND, format!("no snapshot job {job_id:?}"));
+    let Some(pipeline) = job_pipeline(&job_id) else {
+        return missing().into();
+    };
+    let (p, docs) = match find(node, pipeline).await {
+        Ok(found) => found,
+        Err(_) => return missing().into(),
+    };
+    match docs.snapshot_job(&job_id).await {
+        Ok(Some(v)) => body(StatusCode::OK, &job_json(&p, &v.doc)),
+        Ok(None) => missing().into(),
+        Err(e) => unavailable(&e),
+    }
+}
 
 #[cfg(test)]
 mod tests {
```

`crates/data-avalanche-config/src/external.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/external.rs b/crates/data-avalanche-config/src/external.rs
index d300ffa..d8e566f 100644
--- a/crates/data-avalanche-config/src/external.rs
+++ b/crates/data-avalanche-config/src/external.rs
@@ -121,7 +121,8 @@ pub fn check_partitioning(
 }
 
 /// A per-table config key as a table: PG `schema.table`; MySQL `name` in `source.db` (§10).
-fn key_table(p: &Pipeline, key: &str) -> Option<TableRef> {
+/// `None` for a PostgreSQL key without its schema.
+pub fn key_table(p: &Pipeline, key: &str) -> Option<TableRef> {
     match p.source.kind {
         SourceType::Postgres => key.split_once('.').map(|(s, t)| TableRef::new(s, t)),
         SourceType::Mysql => Some(TableRef::new(p.source.db.clone(), key)),
```

`crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 3488424..0aeabee 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -94,7 +94,7 @@ pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use diff::{ConfigDiff, FieldChange, Fingerprints, PipelineChange, diff, diff_with};
 pub use external::{
     ClusterAnchor, check_cluster_anchor, check_partitioning, check_table_set,
-    check_unbounded_numeric, glob_match,
+    check_unbounded_numeric, glob_match, key_table,
 };
 pub use leaf::Loc;
 pub use load::{Loaded, load_file, load_posted_str, load_str, validate_str};
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-api`

Expected: PASS, 46 tests (the five new ones and the matrix and OpenAPI tests among them).

Run: `cargo test --workspace`

Expected: PASS (1282 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/data-avalanche-api crates/data-avalanche-config
git commit -m "feat(api): the snapshot job routes — ask for a job of a running pipeline, list a pipeline's jobs, read a job by its id on any node"
```

---

### Task 7: The E2E scenarios (`data-avalanche` tests)

**Files:**
- Create: `crates/data-avalanche/tests/e2e/jobs.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`, `test-infra/README.md`

**Interfaces:**
- Consumes: Tasks 1–6 through the binary's API; the harness's `E2e`, `Options`, `Proc::{send,
  call, kill, terminate}`, `eventually`, `E2e::{exec, ids, rows, halts, converged, streaming,
  insert_series}` (M6b–M9a).

- [ ] **Step 1: Write the scenarios**

- `a_job_reads_a_table_again_and_copies_one_outside_the_config`: under `mode: never` the rows of
  before the start aren't in the lake; a job of a streamed table and of one the config excludes
  reads both; later changes to the excluded one aren't streamed.
- `a_job_clears_a_dropped_table_s_halt`: a table dropped and created again with a new column
  halts `TABLE_DROPPED`; a job clears the halt and reads it; the old row stays (Q-M9b-2); the
  stream carries the table again.
- `a_job_outlives_a_kill_of_its_process`: 1000 rows in chunks of 10, `kill -9` while the job is
  `running` with `0 < rows_done < 1000`; the next process lists the job and ends it with 1000.

`crates/data-avalanche/tests/e2e/jobs.rs` (new):

```diff
diff --git a/crates/data-avalanche/tests/e2e/jobs.rs b/crates/data-avalanche/tests/e2e/jobs.rs
new file mode 100644
index 0000000..c597495
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/jobs.rs
@@ -0,0 +1,221 @@
+//! The on-demand snapshot jobs (M9b): a job asked through the API reads a streamed table again
+//! and copies one outside the config, clears a dropped table's halt, and outlives a kill of the
+//! process that ran it.
+
+use std::time::Duration;
+
+use data_avalanche_testkit::require_it;
+use serde_json::Value;
+
+use crate::harness::{Doc, E2e, Options, Proc, eventually};
+
+/// A table as a request names it: the source's per-table config key.
+fn key(e: &E2e, table: &str) -> String {
+    if e.source.is_postgres() {
+        format!("{}.{table}", e.schema)
+    } else {
+        table.to_owned()
+    }
+}
+
+/// `POST /v1/pipelines/<id>/snapshots` of `tables`: the code and the job.
+async fn ask(p: &Proc, e: &E2e, tables: &[&str]) -> (u16, Value) {
+    let names: Vec<String> = tables.iter().map(|t| key(e, t)).collect();
+    let body = serde_json::json!({ "tables": names }).to_string();
+    let path = format!("/v1/pipelines/{}/snapshots", e.test);
+    let (code, _, text) = p
+        .send(None, "POST", &path, &body)
+        .await
+        .expect("the API answers");
+    (
+        code,
+        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}")),
+    )
+}
+
+/// Waits until job `id` ended, and returns it.
+async fn ended(p: &Proc, id: &str) -> Value {
+    let path = format!("/v1/snapshots/{id}");
+    eventually(
+        &format!("job {id} ends"),
+        Duration::from_secs(120),
+        async || {
+            let status = p.call("GET", &path).await.1["status"].clone();
+            status == "completed" || status == "failed"
+        },
+    )
+    .await;
+    p.call("GET", &path).await.1
+}
+
+/// Under `mode: never` the rows there before the start stay out of the lake; a job reads the
+/// streamed table again and copies one the config excludes. The copy isn't streamed after.
+#[tokio::test]
+async fn a_job_reads_a_table_again_and_copies_one_outside_the_config() {
+    require_it!();
+    let e = E2e::new(
+        "jobs_read",
+        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
+         INSERT INTO items VALUES (1, 'a'), (2, 'b');
+         CREATE TABLE extra (id bigint PRIMARY KEY, doc text);
+         INSERT INTO extra VALUES (7, 'x');",
+        Options {
+            tables: ", exclude: ['@SCHEMA@.extra'], snapshots: { mode: never }",
+            ..Options::default()
+        },
+    )
+    .await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.streaming().await;
+    e.exec("INSERT INTO items VALUES (3, 'c')").await;
+    eventually(
+        "the streamed row lands",
+        Duration::from_secs(60),
+        async || e.ids("items").await == [3],
+    )
+    .await;
+
+    let (code, v) = ask(&p, &e, &["items", "extra"]).await;
+    assert_eq!(code, 202, "{v}");
+    let id = v["job_id"].as_str().unwrap().to_owned();
+    let job = ended(&p, &id).await;
+    assert_eq!(job["status"], "completed", "{job}");
+    assert_eq!(job["tables"][0]["rows_done"], 3, "{job}");
+    assert_eq!(job["tables"][1]["rows_done"], 1, "{job}");
+    assert_eq!(e.ids("items").await, [1, 2, 3]);
+    assert_eq!(e.ids("extra").await, [7]);
+
+    e.exec("INSERT INTO extra VALUES (8, 'y'); INSERT INTO items VALUES (4, 'd')")
+        .await;
+    e.converged("items").await;
+    assert_eq!(e.ids("extra").await, [7], "a copy isn't streamed");
+    let path = format!("/v1/pipelines/{}/snapshots", e.test);
+    let (code, all) = p.call("GET", &path).await;
+    assert_eq!(code, 200);
+    assert_eq!(all["jobs"][0]["job_id"], id.as_str(), "{all}");
+    assert!(e.halts().await.tables.is_empty());
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// A table dropped and created again halts (`TABLE_DROPPED`); a job clears the halt, reads the
+/// new table into the lake table, and the stream carries its changes again. The old table's
+/// row the new one hasn't stays in the lake: a job upserts (Q-M9b-2).
+#[tokio::test]
+async fn a_job_clears_a_dropped_table_s_halt() {
+    require_it!();
+    let e = E2e::new(
+        "jobs_halt",
+        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
+         CREATE TABLE gone (id bigint PRIMARY KEY, doc text);",
+        Options {
+            tables: ", snapshots: { mode: never }",
+            pipeline: "schema_evolution: { rescan_interval: 2s }",
+            ..Options::default()
+        },
+    )
+    .await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.streaming().await;
+    e.exec("INSERT INTO gone VALUES (1, 'old')").await;
+    e.converged("gone").await;
+    e.exec(
+        "DROP TABLE gone; CREATE TABLE gone (id bigint PRIMARY KEY, doc text, note text);
+         INSERT INTO gone VALUES (5, 'new', 'n')",
+    )
+    .await;
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
+    let job = ended(&p, v["job_id"].as_str().unwrap()).await;
+    assert_eq!(job["status"], "completed", "{job}");
+    assert!(e.halts().await.tables.is_empty(), "the halt is cleared");
+    assert_eq!(
+        e.ids("gone").await,
+        [1, 5],
+        "the old row stays: a job upserts"
+    );
+
+    e.exec("INSERT INTO gone VALUES (6, 'streamed', 'm')").await;
+    eventually(
+        "the stream carries gone again",
+        Duration::from_secs(60),
+        async || e.ids("gone").await == [1, 5, 6],
+    )
+    .await;
+    assert_eq!(
+        e.rows("gone", &["id", "note"]).await,
+        [
+            vec!["1".to_string(), "NULL".into()],
+            vec!["5".into(), "n".into()],
+            vec!["6".into(), "m".into()]
+        ]
+    );
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// A job whose snapshot runs when its process is killed is listed by the next process, which
+/// reads on from the table's cursor and ends the job.
+#[tokio::test]
+async fn a_job_outlives_a_kill_of_its_process() {
+    require_it!();
+    let e = E2e::new(
+        "jobs_kill",
+        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
+        Options {
+            tables: ", snapshots: { mode: never, chunk_size: 10 }",
+            ..Options::default()
+        },
+    )
+    .await;
+    e.insert_series("items", 1_000, Doc::Row).await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.streaming().await;
+    let (code, v) = ask(&p, &e, &["items"]).await;
+    assert_eq!(code, 202, "{v}");
+    let id = v["job_id"].as_str().unwrap().to_owned();
+    let path = format!("/v1/snapshots/{id}");
+    eventually(
+        "the job's table reads, partway",
+        Duration::from_secs(60),
+        async || {
+            let job = p.call("GET", &path).await.1;
+            job["status"] == "running"
+                && job["tables"][0]["rows_done"]
+                    .as_u64()
+                    .is_some_and(|n| n > 0 && n < 1_000)
+        },
+    )
+    .await;
+    p.kill().await;
+
+    let mut p = e.start(&[]);
+    p.ready().await;
+    let (code, all) = p
+        .call("GET", &format!("/v1/pipelines/{}/snapshots", e.test))
+        .await;
+    assert_eq!(code, 200);
+    assert_eq!(
+        all["jobs"][0]["job_id"],
+        id.as_str(),
+        "listed after the kill"
+    );
+    let job = ended(&p, &id).await;
+    assert_eq!(job["status"], "completed", "{job}");
+    assert_eq!(job["tables"][0]["rows_done"], 1_000, "{job}");
+    e.converged("items").await;
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
```

`crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index befeae7..d3dd0f9 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -13,6 +13,7 @@ mod forwarder;
 mod guards;
 mod harness;
 mod idp;
+mod jobs;
 mod lifecycle;
 mod mysql;
 mod proxy;
```

`test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
index 9390816..c9b78fa 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -104,7 +104,9 @@ generation from `globals.state`, and `GET /v1/config/status` reports each node's
 scenarios run the API with `auth: none`. The management API scenarios (M9a) need nothing more:
 `api.rs` serves the API over TLS with a certificate generated at run time and checks bearer
 tokens against a JWKS the test serves on loopback (`idp.rs`); `cluster.rs` drives a pipeline
-through a node that doesn't hold it, and leaves and shuts a node down through the API.
+through a node that doesn't hold it, and leaves and shuts a node down through the API. The
+snapshot-job scenarios (M9b, `jobs.rs`) need nothing more either: they ask for jobs through the
+API, drop and create a table again to halt it, and `kill -9` the process while a job reads.
 
 ## GCS: a real bucket, not an emulator
 
```

- [ ] **Step 2: Run the scenarios**

Run (with `test-infra/up.sh`):
`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e jobs:: -- --test-threads=1`,
then with `DATA_AVALANCHE_E2E_SOURCE=mysql` and each of `DATA_AVALANCHE_MYSQL=mysql-84`,
`mysql-84-nogtid`, `mariadb-118`.

Expected: PASS, 3 scenarios on each (about 70–80 s each run).

```text
test jobs::a_job_clears_a_dropped_table_s_halt ... ok
test jobs::a_job_outlives_a_kill_of_its_process ... ok
test jobs::a_job_reads_a_table_again_and_copies_one_outside_the_config ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 42 filtered out; finished in 77.42s
```

Run: the whole suite on PostgreSQL 18,
`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Expected: PASS, 45 scenarios.

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 3: Commit**

```bash
git add crates/data-avalanche/tests/e2e test-infra/README.md
git commit -m "test(e2e): the snapshot jobs — a table read again and one outside the config copied, a dropped table's halt cleared, a job outliving a kill"
```

---

## Exit

- [ ] Every task's tests pass, and `cargo test --workspace` (1282 tests).
- [ ] The source ITs on PostgreSQL 18, `mysql-84`, `mysql-84-nogtid` and `mariadb-118`.
- [ ] The job scenarios on PostgreSQL 18, `mysql-84`, `mysql-84-nogtid` and `mariadb-118`; the
  whole E2E suite on PostgreSQL 18.
- [ ] Roadmap §4 M9's exit, M9b's part: "snapshot jobs (on-demand chunk loop; jobs persisted
  under `snapshots/jobs/`, listed via `GET /v1/pipelines/{id}/snapshots` after a restart or
  failover, running jobs resumed by the owner — Q-M9-1)": Tasks 3–5 and 7.
- [ ] The OpenAPI snapshot carries the three routes, each with its role (Task 6).
- [ ] The PR's CI green on every target.

## Deviations during execution

None yet.
