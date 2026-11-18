---
updated: 2026-11-18
domain: Architecture
tags: [v1.1, implementation-plan, m13d, console, pipelines, tables, source-detail, delta, api, pipeline, cluster, state, source, sink]
---

# M13d — The Console's Pipelines and Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The fourth server milestone of the 1.1.0 admin console (Admin_Console §14). It covers
what the pipeline page, the Tables page and a table's page read:
- a pipeline's live figures, answered from the memory of the node that runs its worker and asked
  of any node;
- the source's detail: PostgreSQL's slot, the WAL it holds against `max_slot_wal_keep_size`, the
  confirmed position and the publication; MySQL's and MariaDB's position and binlog expiry;
- each table's figures: changes a minute, last commit and Delta version, the last MERGE's files
  rewritten against the table's files, freshness and staleness;
- a table's page: its halt and when it began, the Delta table's facts, its columns at the source
  against Delta's, and the jobs that copied it;
- every pipeline's tables in one list, worst first;
- the site and the E2E suite follow.

**Architecture:**

- **Halt time** (Task 1, `data-avalanche-state`): `halts.json` records when each halt began
  (`since`), written only when set. The pipeline status shows it.
- **Source detail** (Task 2, the sources): `SourceConnector::detail`. PostgreSQL reads one query
  over the slot, the publication and the WAL settings. MySQL and MariaDB read the server's
  position and `binlog_expire_logs_seconds`.
- **Commit cost and table facts** (Task 3, the sinks): `CommitOutcome::Committed` carries the
  files the commit rewrote and the table's files after it. `Sink::facts` reads a table's version,
  files, size and rows on request. Delta reads them from the snapshot's file list and statistics.
- **The worker's figures** (Task 4, `data-avalanche-pipeline`):
  - each commit updates the table's figures in `WorkerStatus`;
  - every heartbeat reads the source's detail, apart from the read the watchdog waits on;
  - `WorkerStatus` keeps the tracked set.
- **The API** (Tasks 5–7, `data-avalanche-api`):
  - `GET /v1/pipelines/{id}/live` is answered by the lease holder from its controllers' statuses;
    another node forwards the request once, with the caller's credentials;
  - `GET /v1/pipelines/{id}/tables/{table}` adds what storage and the lake hold. The lake is read
    through a sink the node's `WorkerFactory` builds for that read, and the result is kept 30 s;
  - `GET /v1/tables` asks each holder, 16 pipelines at a time.
- **The site** (Task 8) and **the E2E suite** (Task 9). On three nodes, a node that doesn't run
  the pipeline shows its figures, the table's page, a column dropped at the source as soft-dropped,
  and every pipeline's tables. The scenario runs on PostgreSQL and on all three MySQL targets.

**Tech Stack:** Rust 1.100.0 (edition 2024). No new crate. The API crate gains these dependencies,
all already in the workspace:
- `arrow-schema` and `data-avalanche-pipeline`, the second promoted from a dev-dependency;
- `data-avalanche-testkit`, as a dev-dependency.

**Spec:**
- `doc/research/Admin_Console.md`: §4 (the pipeline page: the tables, the source panel; the
  Tables page; a table's page), §12 (tables per pipeline and across pipelines, source detail),
  §13 (endpoint paths are the builder's), §14.
- The approved mockups `doc/v1.1/mockups/06-pipeline.html` (the tables, the source panel) and
  `07-tables.html` (every table, a table's page).
- `doc/specs/System-Overview.md` §Scope (the lag target), §Management API endpoints;
  `doc/specs/Data-Flow.md` §Sink contract, §State Layout in Cloud Storage. All are amended by S-M13d.1, below.
- `doc/research/Postgres_Slot_and_Publication_Lifecycle.md` (the slot, `max_slot_wal_keep_size`),
  `doc/research/Multi_Sink_Architecture_and_Delta_Lake.md` D-MS.15 (the MERGE's cost),
  `doc/research/API_RBAC_and_Versioning.md` §2 (roles).
- `doc/v1.1/M13b-console-nodes.md`: forwarding through any node, `Peers`, `DETAIL_LIMIT`.
- `doc/v1.1/M13c-console-jobs.md`: the jobs' reading, `EndedJobs`, `READ_AT_ONCE`.

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`, with the sources and the sinks beside the pipeline. `testkit` is only
  ever a dev-dependency.
- **State documents within 1.1:** `since` is written only when a halt has one. A 1.0 node refuses
  an unknown field (`deny_unknown_fields`), so after a rollback it can't read a halt registry
  that 1.1 wrote a halt into. That goes into the 1.1.0 release notes (D-M13d.2). 1.0 and 1.1
  nodes never run together (M13b's gossip v2).
- **The live figures are the worker's memory:** they start from zero with each worker (a restart,
  a failover). Nothing in this milestone writes them to storage; M13e's history samples them.
- **A request is forwarded at most once:** a forwarded request to a node that no longer runs the
  worker answers `421`, as M13b's node routes do.
- **No sink is kept across requests:** a table's facts are read through a sink built for the read,
  and only the facts are kept. Delta's table state is never held on every node that was asked.
- **Time in tests:** the pipeline's tests run on the fake source and the reference sink. The API's
  tests and the E2E suite run in real time.
- **Test gates:** plain `cargo test` stays hermetic. The sources' integration tests need
  `DATA_AVALANCHE_IT=1` (`test-infra/up.sh`), the MySQL ones on each `DATA_AVALANCHE_MYSQL`
  target. The E2E suite needs `DATA_AVALANCHE_IT=1` and the `e2e-hooks` feature, and runs with
  `--test-threads=1`, on PostgreSQL and on `DATA_AVALANCHE_E2E_SOURCE=mysql` (CI's e2e-mysql
  runs `mysql-84`, `mysql-84-nogtid` and `mariadb-118`).
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`).
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `thiserror` types.
- **Code blocks:**
  - They are the verified content, already `rustfmt`-formatted.
  - Every block is a `git apply`-ready diff, one per file.
  - A step's diffs apply in the order given, after the earlier steps'.
  - The OpenAPI document, the site's API reference and `Cargo.lock` are regenerated, not written
    (Tasks 5, 6, 7).
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt someone running 1.1.0 in production. Each is pinned by a
test in the task after the arrow.

1. **A holder that can't be asked.** It may have left the cluster, gossip no API address, or be
   cut off. The page must say why at once, and never hang or show another node's empty figures.
   - Task 5 (`without_a_holder_to_ask_the_answer_says_why`).
   - Task 7 (`a_pipeline_whose_holder_can_t_be_asked_lists_its_halts`): the list goes on with
     what storage holds.
2. **A lease that moved between the forward and the answer.** The old holder refuses a forwarded
   request (`421`) instead of answering with no figures or sending it on again → Task 5
   (`another_node_asks_the_lease_holder`).
3. **A false "stale".** A paused pipeline, a halted table or a table being copied is behind by
   design, and only a streaming table past the target is stale.
   - Task 5 (`nothing_is_stale_while_paused`; `the_node_that_runs_the_worker_answers_from_its_memory`
     checks the copy and the halt).
   - Task 7 (`a_table_behind_the_target_is_stale`).
4. **A halt that 1.0 recorded, or one recorded again.** A halt without a time reads with none, and a
   halt recorded again for the same reason after a crash keeps its first time → Task 1
   (`a_halt_without_a_time_reads`, `a_halt_records_when_it_began`).
5. **A big Delta table's page, loaded again and again.** Its facts are read once in 30 s, and a
   new generation reads them again (the sink may have moved) → Task 6
   (`facts_are_kept_for_their_while`).

## Decisions and rulings

Settled with Oleksii before this plan (2026-11-18):
- one milestone, not split;
- the Delta facts are read on request by the node asked, and kept 30 s;
- the halt time is recorded in the halt, in `halts.json`. This was confirmed again once its
  rollback cost was known.

The rest are Claude's calls under Admin_Console §13.

- **D-M13d.1 What this milestone serves, and what waits.**
  - Served here: the pipeline page's tables and source panel; the Tables page; a table's page:
    header, halt, columns, jobs.
  - The time series wait for M13e's history, which samples the figures this milestone exposes: the
    pipeline's lag chart, a table's "changes a minute, 6 h", its 24 h figures and its commit
    duration p99.
  - A table's history ("halted", "column added", "copied by job …") and the Tables page's "schema
    changed today" read the cluster's events (`GET /v1/events`: `table.halted`, `table.resumed`,
    `schema.applied`, `schema.degraded`).
  - The pipeline page's earlier jobs read `GET /v1/pipelines/{id}/snapshots`, and its resolved
    config M13b's generation text.
  - M13c's parked items belong to other pages: the jobs list's `limit`, and a queued job's rate of
    0 after a crash. None is carried in.
- **D-M13d.2 Halt time.**
  - `HaltsDoc::since: Vec<HaltSince { table, at }>`, kept in table order, written only when not
    empty.
  - `record_halt` stamps now for a table whose stored halt has another reason, or none. The same
    reason keeps the stored time, so a crash's re-record doesn't move it. `clear_halt` drops the
    time.
  - A halt that 1.0 recorded has no time, and the answers show none.
  - `GET /v1/pipelines/{id}`'s `halted_tables` gain `since`.
  - Rollback: a 1.0 node refuses a `halts.json` with `since`, so a pipeline with a halt that 1.1
    recorded can't start on 1.0 until `since` is removed from that file. This goes into the 1.1.0
    release notes, beside M13c's `COPY_CANCELLED`. Oleksii weighed it against a side document
    that 1.0 ignores, and kept the halt.
- **D-M13d.3 Source detail.** `SourceConnector::detail()`, by default `None`.
  - **When:** read at every heartbeat in its own `JoinSet`, so a stalled read holds up neither the
    stream watchdog nor the WAL guard. The answer is kept in `WorkerStatus::source`, with
    `source_read_at`; a failed read keeps the last one, whose time says how old it is.
  - **PostgreSQL:** one query on views every role reads (`pg_replication_slots`,
    `pg_publication`, `pg_publication_tables`):
    - the head, `pg_current_wal_lsn()` (a standby is refused at preflight, DA-SRC-17);
    - the slot: `active`, `confirmed_flush_lsn`, `restart_lsn`, `wal_status`, and the WAL held
      from `restart_lsn` to the head;
    - `max_slot_wal_keep_size` in bytes, `None` for `-1` (unlimited);
    - the publication and its table count.
    - Before the first start creates the slot and the publication, both are `None`.
  - **MySQL and MariaDB:** the head (`position_now`: the GTID set, or the file and offset) and
    `@@GLOBAL.binlog_expire_logs_seconds`, `None` for `0` (never expired). MariaDB 11.8 has the
    same variable. The stream's lag behind the head is the `behind` that `retained` already reads.
- **D-M13d.4 Per-table figures.**
  - **Commits:** each commit the writer makes reaches the worker's status as
    `WriterEvent::Committed(TableCommit)`. A table's `TableFigures` hold:
    - `changes`: the stream's rows committed since the worker started. A snapshot chunk's rows
      aren't changes; the job's figures count them;
    - the last minute's commits, which give `changes_per_minute`;
    - `last_commit` and `version`;
    - `cost`: the last commit that wrote rows. A marker-only commit moves the version, not the
      cost.
  - **Delta's cost** is the truncate's and the MERGE's removed files, and the table's file count
    after the commit (`log_data().num_files()`, in memory). `MemorySink` measures nothing
    (`CommitCost::default()`).
  - **Tracked set:** Phase 0 writes it into `WorkerStatus::tracked`, so a table with no commit yet
    is listed.
  - **Copied set** (S-M13d.2, a Task 5 fix round): `WorkerStatus::copied` holds the `dump_*` tables
    the worker doesn't stream. Phase 0 puts a `dump_initial` table the sink has there (it stops
    at once), and the session a dump of the config once its snapshot is durable. A copy outside
    the config never joins it.
- **D-M13d.5 Freshness and stale.**
  - A table's `fresh_as_of` is what lake readers see. For a streaming table, or one being copied,
    it is the pipeline's: now minus `replication_lag_seconds` (the source time of the oldest
    change not yet durable, or now when none waits; System-Overview's definition). For a halted
    table it is its halt's `since`. A `copied` table has none.
  - A table is `stale` when it streams, the controller runs, the worker streams, and `fresh_as_of`
    is over `FRESH_WITHIN` old: 60 s, the spec's lag target (Scope, p99 ≤ 60 s). There is no config
    knob.
  - A paused pipeline, a halted table, a table being copied and a `copied` one are never stale:
    the copy's readers see what its job says (`lake`).
- **D-M13d.6 The live route.** `GET /v1/pipelines/{id}/live` (viewer).
  - A node whose controller for the pipeline is past `Standby` answers from its status. It reads
    the halt registry for the halts' times; a failed read leaves the times out.
  - Any other node reads the lease and forwards the request once to the holder's gossiped API
    address, with the caller's credentials, within `DETAIL_LIMIT` (3 s), and passes the answer on.
  - The other answers:
    - `409`: no node holds the lease;
    - `503`, naming the node: the holder isn't a member this node sees, gossips no address, or is
      this node before its worker starts;
    - `502` or `504`: the holder didn't answer (`Peers::send`);
    - `421`: a forwarded request to a node that doesn't run the worker.
  - The answer: the node, `actual` and its error, `owner_epoch`, `worker` (phase, positions, lag,
    `fresh_as_of`, rows read, buffer, warnings), `source` (the detail, `read_at`, retained bytes,
    `behind_seconds`), and `tables`. Each table carries its state (`halted`, `snapshot`,
    `copied`, else `streaming`), halt, snapshot progress, figures, `soft_dropped`, `fresh_as_of`
    and `stale`.
  - The tables are the tracked, halted, copying and `copied` ones (S-M13d.2). A copy outside the
    config is listed while it copies; once it ends, or halts, its job tells, and its figures
    don't list it.
- **D-M13d.7 The Delta facts.**
  - `Sink::facts` reads, through a sink the node's `WorkerFactory` builds for that one read: the
    `location`, the latest `version`, `files`, `bytes` and `rows`. Rows are the files' `numRecords`
    summed, or none when a file has no count.
  - The table's columns (`table_state`) are read beside them.
  - Both are kept per pipeline and table for `FACTS_FOR` (30 s), and per config generation: a
    reload may move the sink. A failed read isn't kept.
  - No sink is kept (Global Constraints).
- **D-M13d.8 A table's page.** `GET /v1/pipelines/{id}/tables/{table}` (viewer).
  - The table is named as its config key: `schema.table` on PostgreSQL, the table alone on MySQL.
    A name not in that form is `400`. A table the pipeline has no trace of (no live row, halt,
    cached schema, Delta table or job) is `404`.
  - The answer: `node`; `live` (the table's row of the live answer), or `live_error` (`code`,
    `error`, `node`); `halt` (with `since`); `delta` and `read_at`, or `delta_error`; `columns`;
    and `jobs` (the jobs that copied it, newest first, each with what lake readers saw).
  - `columns` puts the source's (the pipeline's cached schema) against Delta's:
    - first the source's columns in order, each `kept` or `not_in_delta`;
    - then the columns only Delta has, `soft_dropped`;
    - then the lineage columns.
    - A Delta column shows its logical tag, a source column its catalog type. MySQL compares names
      without case (DA-SRC-25).
- **D-M13d.9 Every pipeline's tables.** `GET /v1/tables` (viewer).
  - Every pipeline of the generation is asked, 16 at a time (`READ_AT_ONCE`, as M13c's list).
  - Each row gains `pipeline`, `node`, `live` and `listed_as`, the first that fits: `halted`,
    `stale`, `snapshot`, `copied` (never `costly`), `costly` (a streaming table whose last MERGE
    rewrote more than half its files: random-key churn, D-MS.15), else `streaming`. The order is
    `halted`, `stale`, `snapshot`, `costly`, `streaming`, `copied`, then by pipeline and table.
  - `counts` are by `listed_as`, after the `pipeline` and `q` filters and before `state`.
  - Filters: `state` (several, comma-separated), `pipeline` (`404` when the generation has none),
    `q` (text in the table's name, any case). `400` for any other parameter, a repeated one or an
    unknown state.
  - A pipeline whose live figures can't be had is named under `unreachable` (`code`, `error`,
    `node`), and its halted tables are listed from storage, with `live: false`.
  - No `limit`, as `GET /v1/snapshots` has none. The console milestone that polls it decides.
- **D-M13d.10 Test support.** The node tests' fake peer, members and served node move to the API's
  `test_support`, which the new routes' tests share. The lone node's factory builds every
  pipeline on one `MemorySink` (`Lone::sink`).

### Spec patch for approval (S-M13d.1)

Approved by Oleksii 2026-11-18 with the plan. This patch is applied as execution starts, as its own commit, `spec: S-M13d.1 — a pipeline's live figures, a table's
page, every pipeline's tables; a halt's since; a commit's cost`, before Task 1. The code below implements these edits to two spec files:

1. `doc/specs/System-Overview.md`, the endpoint table:
   - the pipeline status row says a halted table shows since when;
   - three rows follow it: the live figures, a table, all tables;
   - the paragraph after the table names the exception to "a status is read from storage".
2. `doc/specs/Data-Flow.md`:
   - §Sink contract: `Committed{version, cost}`, and a table's facts on request;
   - §State Layout in Cloud Storage: `halts.json`'s `since`.

`doc/specs/System-Overview.md`:

````diff
diff --git a/doc/specs/System-Overview.md b/doc/specs/System-Overview.md
index 657d37c..d27bdc0 100644
--- a/doc/specs/System-Overview.md
+++ b/doc/specs/System-Overview.md
@@ -135,7 +135,10 @@ Functional endpoints are versioned under `/v1`; probes and metrics are unversion
 | Config generations | `GET /v1/config/generations`, `GET /v1/config/generations/{generation}` | viewer | newest first, each with who published it, its note and what it changed; `limit` 1–100 (20), `before`. One generation's text has its secrets masked: a `${VAR}` as written, a literal as the start of its fingerprint |
 | Config diff | `GET /v1/config/diff?from=&to=` | viewer | what differs between two generations as written, each change with what a reload of it does; secrets masked |
 | Config nodes | `GET /v1/config/nodes` | viewer | each member's generation, whether its own file is the cluster's generation, and its restart-pending fields |
-| Pipelines status | `GET /v1/pipelines`, `GET /v1/pipelines/{id}` | viewer | `desired_state`, `actual_state`, positions, per-table status (halted tables with their reason); the lease holder flagged `stale` when it isn't a member the node's gossip sees (gone, or an earlier process of a restarted node), `suspect` while gossip suspects it |
+| Pipelines status | `GET /v1/pipelines`, `GET /v1/pipelines/{id}` | viewer | `desired_state`, `actual_state`, positions, per-table status (halted tables with their reason and since when; a halt recorded before 1.1.0 has no time); the lease holder flagged `stale` when it isn't a member the node's gossip sees (gone, or an earlier process of a restarted node), `suspect` while gossip suspects it |
+| Pipeline live figures | `GET /v1/pipelines/{id}/live` | viewer | what the pipeline's worker shows now, answered from the memory of the node that holds its lease: phase, positions, lag and what lake readers see (`fresh_as_of`); the source's detail, read every heartbeat (PostgreSQL: the slot, the WAL it holds against `max_slot_wal_keep_size`, its confirmed position, the publication; MySQL/MariaDB: the server's position and binlog expiry); each table's state, changes since the worker started and in the last minute, last commit and Delta version, the last MERGE's files rewritten against the table's files, freshness, and `stale` (older than the lag target while the worker streams). Another node asks the holder with the caller's credentials: `409` no node holds the lease, `503` a holder it can't ask, `502`/`504` no answer, `421` a forwarded request to a node that no longer runs the worker |
+| Table detail | `GET /v1/pipelines/{id}/tables/{table}` | viewer | a table, named as its config key: the worker's figures of it, its halt and since when, the Delta table's location, version, files, bytes and rows (read on request by the node asked, kept 30 s), its columns at the source against Delta's (kept, soft-dropped, not yet added, lineage), and the snapshot jobs that copied it. `404` when the pipeline has no trace of the table |
+| All tables | `GET /v1/tables` | viewer | every pipeline's tables, worst first, as each holder's worker shows them, listed as `halted`, `stale`, `snapshot`, `costly` (the last MERGE rewrote over half the table's files) or `streaming`, with counts; filters `state` (several, comma-separated), `pipeline`, `q`; a pipeline whose holder can't be asked lists its halted tables from storage and is named under `unreachable` |
 | Request snapshot | `POST /v1/pipelines/{id}/snapshots` | operator | tables list (the per-table config keys), no globs; refused (`409`) unless the desired state is `RUNNING` and a node holds the lease, as a restart (`ERROR` included), or while an unfinished job holds one of the tables; `202` with the job's id. The job runs in a restarted worker. It upserts: a row deleted at the source before the job stays, unless the job is asked with `truncate: true`, which empties each sink table first. A pipeline keeps its 100 newest ended jobs |
 | Snapshot job status | `GET /v1/snapshots/{job_id}` | viewer | the job's status (`queued`, `running`, `cancelling`, `completed`, `failed`, `cancelled`), who asked, their role and note, the job it retries, who cancelled it and why; each table's status, rows copied, the source's row estimate, when its copy began and ended, the halt the job cleared for it, and what lake readers see of it (`complete`, `partial`, `empty`, `as_before`); the job's rate and ETA once it has run a minute |
 | List snapshot jobs | `GET /v1/pipelines/{id}/snapshots` | viewer | persisted in the pipeline state location; survive restart and failover |
@@ -152,7 +155,7 @@ Functional endpoints are versioned under `/v1`; probes and metrics are unversion
 | Shutdown | `POST /v1/admin/shutdown` | admin | as SIGTERM: the leave, then the process exits |
 | API description | `GET /v1/openapi.json` | viewer | the OpenAPI 3.1 document of the `/v1` endpoints, each with its min role |
 
-Pause/stop/resume set the pipeline's persisted **desired state**; it survives restarts, failover and config reloads (a reload never resumes a paused or stopped pipeline). Illegal transitions return `409 Conflict` with the current state; asking for the state the pipeline is in already changes nothing and is not an error. Any node answers any endpoint: a lifecycle action is recorded in the pipeline's state location and the pipeline's owner carries it out at its next poll (restart and flush answer `202`), and a status is read from the same documents.
+Pause/stop/resume set the pipeline's persisted **desired state**; it survives restarts, failover and config reloads (a reload never resumes a paused or stopped pipeline). Illegal transitions return `409 Conflict` with the current state; asking for the state the pipeline is in already changes nothing and is not an error. Any node answers any endpoint: a lifecycle action is recorded in the pipeline's state location and the pipeline's owner carries it out at its next poll (restart and flush answer `202`), and a status is read from the same documents. A pipeline's live figures, and its tables', are the exception: they are in the memory of the node that runs its worker, and the node asked passes the request on to it.
 
 ## Observability catalogue
 
````

`doc/specs/Data-Flow.md`:

````diff
diff --git a/doc/specs/Data-Flow.md b/doc/specs/Data-Flow.md
index fb270b9..e1a6d40 100644
--- a/doc/specs/Data-Flow.md
+++ b/doc/specs/Data-Flow.md
@@ -529,8 +529,10 @@ For every `TableChangeSet` the sink must:
    accumulator never knows the target format.
 7. **Tolerate unmaintained tables** — never assume compaction/expiry has run.
 
-Outcome per table: `Committed{version}` | `SkippedCovered` (position guard) | `Fenced` (stop the
-pipeline; lease lost).
+Outcome per table: `Committed{version, cost}` | `SkippedCovered` (position guard) | `Fenced` (stop
+the pipeline; lease lost). `cost` is the files the commit rewrote and the table's files after it, as
+far as the sink counts them. On request, a sink also reads a table's facts: its version, files,
+their size and its rows (Delta: from the files' statistics), for the table's page.
 
 **Position guard (optimization, not correctness):** before committing a table, the sink reads that
 table's own `committed_position` marker — an **engine-tagged** value (PG LSN, or **canonical GTID-set**
@@ -718,8 +720,10 @@ membership, the lease registry) lives separately under `globals.state` per
 │                                                  source id); written after the checkpoint that covers a
 │                                                  change; finds a table dropped and re-created while down
 ├── halts.json                                   — halted tables: table, code, detail, `at` (the sink holds
-│                                                  the table's changes through it); written before the
-│                                                  next checkpoint; a halted table stays halted at start
+│                                                  the table's changes through it), and when each halt
+│                                                  began (`since`, written only when there is one);
+│                                                  written before the next checkpoint; a halted table
+│                                                  stays halted at start
 └── snapshots/
     ├── <schema>.<table>.json                    — per-table snapshot state (a `.`, `/`, `%` or control character
     │                                              inside a name is escaped as `%XX` in the file name)
````

### Spec patch S-M13d.2 (approved during execution)

Raised by Task 5's review, approved by Oleksii 2026-11-18 ("State `copied`"). Applied as its own
commit, `spec: S-M13d.2 — a dump that isn't streamed is listed copied`, before Task 5's fix
round. A finished `dump_*` table stayed in the tracked set, and a finished or halted copy in the
figures, so both were listed `streaming`, and could be `stale`, though nothing streamed them.

`doc/specs/System-Overview.md`:

````diff
diff --git a/doc/specs/System-Overview.md b/doc/specs/System-Overview.md
index d27bdc0..c58dc02 100644
--- a/doc/specs/System-Overview.md
+++ b/doc/specs/System-Overview.md
@@ -136,9 +136,9 @@ Functional endpoints are versioned under `/v1`; probes and metrics are unversion
 | Config diff | `GET /v1/config/diff?from=&to=` | viewer | what differs between two generations as written, each change with what a reload of it does; secrets masked |
 | Config nodes | `GET /v1/config/nodes` | viewer | each member's generation, whether its own file is the cluster's generation, and its restart-pending fields |
 | Pipelines status | `GET /v1/pipelines`, `GET /v1/pipelines/{id}` | viewer | `desired_state`, `actual_state`, positions, per-table status (halted tables with their reason and since when; a halt recorded before 1.1.0 has no time); the lease holder flagged `stale` when it isn't a member the node's gossip sees (gone, or an earlier process of a restarted node), `suspect` while gossip suspects it |
-| Pipeline live figures | `GET /v1/pipelines/{id}/live` | viewer | what the pipeline's worker shows now, answered from the memory of the node that holds its lease: phase, positions, lag and what lake readers see (`fresh_as_of`); the source's detail, read every heartbeat (PostgreSQL: the slot, the WAL it holds against `max_slot_wal_keep_size`, its confirmed position, the publication; MySQL/MariaDB: the server's position and binlog expiry); each table's state, changes since the worker started and in the last minute, last commit and Delta version, the last MERGE's files rewritten against the table's files, freshness, and `stale` (older than the lag target while the worker streams). Another node asks the holder with the caller's credentials: `409` no node holds the lease, `503` a holder it can't ask, `502`/`504` no answer, `421` a forwarded request to a node that no longer runs the worker |
+| Pipeline live figures | `GET /v1/pipelines/{id}/live` | viewer | what the pipeline's worker shows now, answered from the memory of the node that holds its lease: phase, positions, lag and what lake readers see (`fresh_as_of`); the source's detail, read every heartbeat (PostgreSQL: the slot, the WAL it holds against `max_slot_wal_keep_size`, its confirmed position, the publication; MySQL/MariaDB: the server's position and binlog expiry); each table's state (`streaming`, `snapshot`, `halted`, or `copied`: a `dump_*` table whose copy ended, or a `dump_initial` one the sink had at the start, which isn't streamed; a snapshot job's table outside the config is listed while it copies), changes since the worker started and in the last minute, last commit and Delta version, the last MERGE's files rewritten against the table's files, freshness, and `stale` (older than the lag target while the worker streams). Another node asks the holder with the caller's credentials: `409` no node holds the lease, `503` a holder it can't ask, `502`/`504` no answer, `421` a forwarded request to a node that no longer runs the worker |
 | Table detail | `GET /v1/pipelines/{id}/tables/{table}` | viewer | a table, named as its config key: the worker's figures of it, its halt and since when, the Delta table's location, version, files, bytes and rows (read on request by the node asked, kept 30 s), its columns at the source against Delta's (kept, soft-dropped, not yet added, lineage), and the snapshot jobs that copied it. `404` when the pipeline has no trace of the table |
-| All tables | `GET /v1/tables` | viewer | every pipeline's tables, worst first, as each holder's worker shows them, listed as `halted`, `stale`, `snapshot`, `costly` (the last MERGE rewrote over half the table's files) or `streaming`, with counts; filters `state` (several, comma-separated), `pipeline`, `q`; a pipeline whose holder can't be asked lists its halted tables from storage and is named under `unreachable` |
+| All tables | `GET /v1/tables` | viewer | every pipeline's tables, worst first, as each holder's worker shows them, listed as `halted`, `stale`, `snapshot`, `costly` (the last MERGE rewrote over half the table's files), `streaming` or `copied`, with counts; filters `state` (several, comma-separated), `pipeline`, `q`; a pipeline whose holder can't be asked lists its halted tables from storage and is named under `unreachable` |
 | Request snapshot | `POST /v1/pipelines/{id}/snapshots` | operator | tables list (the per-table config keys), no globs; refused (`409`) unless the desired state is `RUNNING` and a node holds the lease, as a restart (`ERROR` included), or while an unfinished job holds one of the tables; `202` with the job's id. The job runs in a restarted worker. It upserts: a row deleted at the source before the job stays, unless the job is asked with `truncate: true`, which empties each sink table first. A pipeline keeps its 100 newest ended jobs |
 | Snapshot job status | `GET /v1/snapshots/{job_id}` | viewer | the job's status (`queued`, `running`, `cancelling`, `completed`, `failed`, `cancelled`), who asked, their role and note, the job it retries, who cancelled it and why; each table's status, rows copied, the source's row estimate, when its copy began and ended, the halt the job cleared for it, and what lake readers see of it (`complete`, `partial`, `empty`, `as_before`); the job's rate and ETA once it has run a minute |
 | List snapshot jobs | `GET /v1/pipelines/{id}/snapshots` | viewer | persisted in the pipeline state location; survive restart and failover |
````

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-state/src/{tables,lib}.rs` | `HaltSince`, `HaltsDoc::since` (Task 1) |
| `crates/data-avalanche-core/src/{source,sink,lib}.rs` | `SourceDetail` and its parts, `SourceConnector::detail`; `CommitCost`, `TableFacts`, `Sink::facts` (Tasks 2, 3) |
| `crates/data-avalanche-source-pg/src/{slot,connector}.rs`, `tests/pg/slot.rs` | PostgreSQL's detail (Task 2) |
| `crates/data-avalanche-source-mysql/src/{start,connector}.rs`, `tests/mysql/start.rs` | MySQL's and MariaDB's detail (Task 2) |
| `crates/data-avalanche-sink-delta/src/sink.rs`, `tests/sink/scenarios.rs` | Delta's commit cost and facts (Task 3) |
| `crates/data-avalanche-testkit/src/{memory_sink,fake_source}.rs` | The reference sink's facts; the fake source's detail (Tasks 3, 4) |
| `crates/data-avalanche-pipeline/src/{status,writer,session,worker,lib}.rs` | `TableCommit`, `TableFigures`, the status's figures, tracked set and source detail (Task 4); the copied set (Task 5's fix round) |
| `crates/data-avalanche-cluster/src/{reconciler,events,jobs}.rs` | `watch_statuses` public; the halts' new field in tests (Tasks 1, 5) |
| `crates/data-avalanche-api/src/{live,tables}.rs` | The live route; a table's page; every table; `Lake` (Tasks 5–7) |
| `crates/data-avalanche-api/src/{lib,pipelines,snapshots,nodes,test_support}.rs`, `Cargo.toml`, `openapi.json` | `Node`'s new fields, the routes, `since` in the status, shared helpers, the moved test support (Tasks 1, 5–7) |
| `crates/data-avalanche/src/run.rs` | The node's statuses, factory and lake (Tasks 5, 6) |
| `xtask/src/api.rs` | The API page's `tables` section (Task 7) |
| `docs/src/content/docs/runbooks/{pipeline-figures,halted-table}.md`, `docs/src/content/docs/reference/api.md`, `docs/public/openapi.json` | The pages (Tasks 5–8) |
| `crates/data-avalanche/tests/e2e/{console_pipelines,main}.rs` | The scenario on three nodes (Task 9) |

---

### Task 1: A halt records when it began (`data-avalanche-state`, `-cluster`, `-api`)

**Files:**
- Modify: `crates/data-avalanche-api/src/pipelines.rs`, `crates/data-avalanche-cluster/src/events.rs`, `crates/data-avalanche-cluster/src/jobs.rs`, `crates/data-avalanche-state/src/lib.rs`, `crates/data-avalanche-state/src/tables.rs`

**Interfaces:**
- Consumes: `HaltsDoc`, `TableHalt`, `record_halt`, `clear_halt` (M4, M9b).
- Produces (state crate root): `HaltSince { table: TableRef, at: DateTime<Utc> }`;
  `HaltsDoc::since: Vec<HaltSince>` (written only when not empty); `HaltsDoc::since(&self,
  &TableRef) -> Option<DateTime<Utc>>`. `record_halt` stamps now for a table whose stored halt has
  another reason or none, and keeps the stored time for the same reason; `clear_halt` drops it.
  `GET /v1/pipelines/{id}`'s `halted_tables` gain `since`.

- [ ] **Step 1: Write the failing tests**

The tests: `a_halt_records_when_it_began`, `a_halt_without_a_time_reads`, `halts_json_shape` (state); `a_pipeline_s_status_is_read_from_its_state_and_its_lease` (api).

`crates/data-avalanche-api/src/pipelines.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index 36f0e95..d3a4c95 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -511,6 +512,12 @@ mod tests {
         assert_eq!(v["halted_tables"][0]["table"], "public.orders");
         assert_eq!(v["halted_tables"][0]["code"], "TABLE_DROPPED");
         assert_eq!(v["halted_tables"][0]["at"], "pg:0/16B3748");
+        let since = docs
+            .halts()
+            .await
+            .unwrap()
+            .since(&TableRef::new("public", "orders"));
+        assert_eq!(v["halted_tables"][0]["since"], json!(since.unwrap()));
         assert!(
             v["halted_tables"][0]["detail"]
                 .as_str()
````

`crates/data-avalanche-cluster/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/events.rs b/crates/data-avalanche-cluster/src/events.rs
index 9166945..c0d54ef 100644
--- a/crates/data-avalanche-cluster/src/events.rs
+++ b/crates/data-avalanche-cluster/src/events.rs
@@ -668,6 +668,7 @@ mod tests {
                 at: data_avalanche_core::SourcePosition::Pg(data_avalanche_core::position::Lsn(1)),
                 copy: false,
             }],
+            since: vec![],
         };
         let mut w = HaltWatch::recorded(&recorded);
         assert!(
````

`crates/data-avalanche-cluster/src/jobs.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/jobs.rs b/crates/data-avalanche-cluster/src/jobs.rs
index 05705cf..ebad0e2 100644
--- a/crates/data-avalanche-cluster/src/jobs.rs
+++ b/crates/data-avalanche-cluster/src/jobs.rs
@@ -506,6 +506,7 @@ mod tests {
                 at: "pg:0/10".parse().unwrap(),
                 copy: false,
             }],
+            since: vec![],
         };
         let Progress::Failed(why) = progress(&a, &halted, Some(&running)) else {
             panic!("a halt fails the table")
````

`crates/data-avalanche-state/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index f8f9f81..3ebb94d 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -73,4 +73,4 @@ pub use snapshot::{
     CursorKey, JOBS_PREFIX, JobCancel, JobStatus, JobTable, JobTableStatus, KEPT_ENDED_JOBS,
     SnapshotCursor, SnapshotJobDoc, SnapshotStatus, TableSnapshotDoc, job_key, table_snapshot_key,
 };
-pub use tables::{HALTS_KEY, HaltsDoc, SCHEMAS_PREFIX, TableHalt, table_schema_key};
+pub use tables::{HALTS_KEY, HaltSince, HaltsDoc, SCHEMAS_PREFIX, TableHalt, table_schema_key};
````

`crates/data-avalanche-state/src/tables.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/tables.rs b/crates/data-avalanche-state/src/tables.rs
index f5d2dad..5f0abc2 100644
--- a/crates/data-avalanche-state/src/tables.rs
+++ b/crates/data-avalanche-state/src/tables.rs
@@ -344,8 +380,48 @@ mod tests {
         assert_eq!(p.halts().await.unwrap().tables, vec![overflow]);
     }
 
+    /// Admin_Console §4 (a table's page): a halt records when it began. Recorded again for the
+    /// same reason (a crash's repeat), it keeps that time; for another reason it starts again;
+    /// cleared, it goes.
+    #[tokio::test]
+    async fn a_halt_records_when_it_began() {
+        let p = pipeline();
+        let orders = TableRef::new("public", "orders");
+        let before = chrono::Utc::now();
+        p.record_halt(&halt("orders", HaltReason::TableDropped))
+            .await
+            .unwrap();
+        let first = p.halts().await.unwrap().since(&orders).unwrap();
+        assert!(first >= before && first <= chrono::Utc::now(), "{first}");
+        p.record_halt(&halt("orders", HaltReason::TableDropped))
+            .await
+            .unwrap();
+        assert_eq!(p.halts().await.unwrap().since(&orders), Some(first), "kept");
+        let other = HaltReason::UnsafeDdl {
+            detail: "narrowed".into(),
+        };
+        p.record_halt(&halt("orders", other)).await.unwrap();
+        let again = p.halts().await.unwrap().since(&orders).unwrap();
+        assert!(again >= first, "another reason starts again");
+        p.clear_halt(&orders).await.unwrap();
+        let doc = p.halts().await.unwrap();
+        assert_eq!(doc.since(&orders), None);
+        assert!(doc.since.is_empty(), "{doc:?}");
+    }
+
+    /// A 1.0 `halts.json` has no times: it reads, and its halts have none.
+    #[test]
+    fn a_halt_without_a_time_reads() {
+        let doc: HaltsDoc = serde_json::from_str(
+            r#"{"tables":[{"table":{"schema":"public","name":"orders"},"reason":{"code":"TABLE_DROPPED"},"at":"pg:0/16B3748"}]}"#,
+        )
+        .unwrap();
+        assert_eq!(doc.since(&TableRef::new("public", "orders")), None);
+    }
+
     #[test]
     fn halts_json_shape() {
+        let at: chrono::DateTime<chrono::Utc> = "2026-11-18T09:30:00Z".parse().unwrap();
         let doc = HaltsDoc {
             tables: vec![halt(
                 "orders",
@@ -353,6 +429,10 @@ mod tests {
                     column: "price".into(),
                 },
             )],
+            since: vec![HaltSince {
+                table: TableRef::new("public", "orders"),
+                at,
+            }],
         };
         assert_eq!(
             json(&doc),
@@ -370,6 +450,15 @@ mod tests {
       },
       "at": "pg:0/16B3748"
     }
+  ],
+  "since": [
+    {
+      "table": {
+        "schema": "public",
+        "name": "orders"
+      },
+      "at": "2026-11-18T09:30:00Z"
+    }
   ]
 }"#
         );
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-state --lib tables:: && cargo test -p data-avalanche-api --lib pipelines::`
Expected: the build fails: no `HaltSince`, no field `since` in `HaltsDoc`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/pipelines.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index 36f0e95..d3a4c95 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -238,6 +238,7 @@ pub(crate) async fn get(State(api): State<Api>, Path(id): Path<String>) -> Respo
                 "code": h.reason.code(),
                 "detail": h.reason.to_string(),
                 "at": h.at.to_string(),
+                "since": halts.since(&h.table),
             }))
             .collect::<Vec<_>>()
     );
````

`crates/data-avalanche-state/src/tables.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/tables.rs b/crates/data-avalanche-state/src/tables.rs
index f5d2dad..5f0abc2 100644
--- a/crates/data-avalanche-state/src/tables.rs
+++ b/crates/data-avalanche-state/src/tables.rs
@@ -10,6 +10,7 @@
 //! after it are dropped, so resuming it from a later checkpoint would skip them. A recorded halt
 //! keeps the table halted at every start until a re-snapshot clears it.
 
+use chrono::{DateTime, Utc};
 use data_avalanche_core::{HaltReason, SourcePosition, TableRef, TableSchema};
 use data_avalanche_storage::escape_segment;
 use serde::{Deserialize, Serialize};
@@ -55,11 +56,31 @@ pub struct TableHalt {
     pub copy: bool,
 }
 
+/// When a table's halt began (Admin_Console §4, a table's page).
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct HaltSince {
+    pub table: TableRef,
+    pub at: DateTime<Utc>,
+}
+
 /// `halts.json`: the tracked tables that halted, in table order.
 #[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct HaltsDoc {
     pub tables: Vec<TableHalt>,
+    /// When each halt began, in table order: written only when there is one, so a 1.0 node
+    /// reads a registry without halts. A halt a 1.0 node recorded has none.
+    #[serde(default, skip_serializing_if = "Vec::is_empty")]
+    pub since: Vec<HaltSince>,
+}
+
+impl HaltsDoc {
+    /// When the table's halt began, when it was recorded with its time.
+    #[must_use]
+    pub fn since(&self, table: &TableRef) -> Option<DateTime<Utc>> {
+        self.since.iter().find(|s| &s.table == table).map(|s| s.at)
+    }
 }
 
 impl Document for HaltsDoc {
@@ -136,17 +157,31 @@ impl PipelineDocs {
             .unwrap_or_default())
     }
 
-    /// Records a table's halt, replacing an earlier one of the same table.
+    /// Records a table's halt, replacing an earlier one of the same table, and when it began:
+    /// now, unless the table's recorded halt has the same reason (a repeat after a crash keeps
+    /// its time).
     ///
     /// # Errors
     /// A storage or decode failure.
     pub async fn record_halt(&self, halt: &TableHalt) -> Result<Versioned<HaltsDoc>, StateError> {
+        let now = Utc::now();
         self.docs
             .modify(HALTS_KEY, |cur: Option<HaltsDoc>| {
                 let mut doc = cur.unwrap_or_default();
+                let same = doc
+                    .tables
+                    .iter()
+                    .any(|h| h.table == halt.table && h.reason == halt.reason);
+                let at = doc.since(&halt.table).filter(|_| same).unwrap_or(now);
                 doc.tables.retain(|h| h.table != halt.table);
                 doc.tables.push(halt.clone());
                 doc.tables.sort_by(|a, b| a.table.cmp(&b.table));
+                doc.since.retain(|s| s.table != halt.table);
+                doc.since.push(HaltSince {
+                    table: halt.table.clone(),
+                    at,
+                });
+                doc.since.sort_by(|a, b| a.table.cmp(&b.table));
                 Ok(doc)
             })
             .await
@@ -161,6 +196,7 @@ impl PipelineDocs {
             .modify(HALTS_KEY, |cur: Option<HaltsDoc>| {
                 let mut doc = cur.unwrap_or_default();
                 doc.tables.retain(|h| &h.table != table);
+                doc.since.retain(|s| &s.table != table);
                 Ok(doc)
             })
             .await
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-state --lib tables:: && cargo test -p data-avalanche-api --lib pipelines::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/pipelines.rs crates/data-avalanche-cluster/src/events.rs crates/data-avalanche-cluster/src/jobs.rs crates/data-avalanche-state/src/lib.rs crates/data-avalanche-state/src/tables.rs
git commit -m "feat(state): a halt records when it began; the pipeline status shows it"
```

---

### Task 2: The source's detail (`data-avalanche-core`, `-source-pg`, `-source-mysql`)

**Files:**
- Modify: `crates/data-avalanche-core/src/lib.rs`, `crates/data-avalanche-core/src/source.rs`, `crates/data-avalanche-source-mysql/src/connector.rs`, `crates/data-avalanche-source-mysql/src/start.rs`, `crates/data-avalanche-source-mysql/tests/mysql/start.rs`, `crates/data-avalanche-source-pg/src/connector.rs`, `crates/data-avalanche-source-pg/src/slot.rs`, `crates/data-avalanche-source-pg/tests/pg/slot.rs`

**Interfaces:**
- Consumes: `SourceConnector` (M3), `PgConnector::slot_name`, `MySqlConnector::position_now`.
- Produces (core crate root): `SourceDetail` (`Postgres(PgDetail)` | `Mysql(MySqlDetail)`,
  serialized with `engine`), `PgDetail { head, slot: Option<SlotDetail>, max_slot_wal_keep_size:
  Option<u64>, publication: Option<PublicationDetail> }`, `SlotDetail { name, active,
  confirmed_flush, restart_lsn, wal_held_bytes, wal_status }`, `PublicationDetail { name, tables
  }`, `MySqlDetail { head, binlog_expire_seconds: Option<u64> }`;
  `SourceConnector::detail(&self) -> Result<Option<SourceDetail>, SourceError>`, by default
  `None`. PostgreSQL reads one query over `pg_replication_slots`, `pg_publication` and
  `pg_publication_tables`; MySQL and MariaDB `position_now` and
  `@@GLOBAL.binlog_expire_logs_seconds`.

- [ ] **Step 1: Write the failing tests**

The tests: `the_traits_work_as_trait_objects` (the default), `a_source_detail_names_its_engine` (core); `the_detail_shows_the_slot_and_the_publication` (PostgreSQL); `the_detail_shows_the_head_and_the_binlog_expiry` (MySQL and MariaDB).

`crates/data-avalanche-core/src/source.rs`:

````diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index d4b392c..8fe7da9 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -798,6 +861,44 @@ mod tests {
             None,
             "a source without chunk visibility"
         );
+        assert_eq!(
+            block_on(source.detail()).unwrap(),
+            None,
+            "a source that says nothing"
+        );
+    }
+
+    #[test]
+    fn a_source_detail_names_its_engine() {
+        let pg = SourceDetail::Postgres(PgDetail {
+            head: SourcePosition::Pg(Lsn(0x30)),
+            slot: Some(SlotDetail {
+                name: "da_orders".into(),
+                active: true,
+                confirmed_flush: Some(SourcePosition::Pg(Lsn(0x20))),
+                restart_lsn: Some(SourcePosition::Pg(Lsn(0x10))),
+                wal_held_bytes: Some(32),
+                wal_status: Some("reserved".into()),
+            }),
+            max_slot_wal_keep_size: None,
+            publication: Some(PublicationDetail {
+                name: "da_orders".into(),
+                tables: 14,
+            }),
+        });
+        let v = serde_json::to_value(&pg).unwrap();
+        assert_eq!(v["engine"], "postgres");
+        assert_eq!(v["head"], SourcePosition::Pg(Lsn(0x30)).to_string());
+        assert_eq!(v["slot"]["wal_held_bytes"], 32);
+        assert_eq!(v["max_slot_wal_keep_size"], serde_json::Value::Null);
+        assert_eq!(v["publication"]["tables"], 14);
+        let my = SourceDetail::Mysql(MySqlDetail {
+            head: SourcePosition::Pg(Lsn(0x30)),
+            binlog_expire_seconds: Some(2_592_000),
+        });
+        let v = serde_json::to_value(&my).unwrap();
+        assert_eq!(v["engine"], "mysql");
+        assert_eq!(v["binlog_expire_seconds"], 2_592_000);
     }
 
     #[test]
````

`crates/data-avalanche-source-mysql/tests/mysql/start.rs`:

````diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/start.rs b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
index b5266bb..4325001 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/start.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
@@ -5,7 +5,7 @@ use std::time::Duration;
 
 use data_avalanche_core::{
     CdcEvent, ChangeStream, Checkpoint, ColumnValue, ServerIdentity, SnapshotReason,
-    SourceConnector, SourceError, SourcePosition, StartPlan, StreamItem,
+    SourceConnector, SourceDetail, SourceError, SourcePosition, StartPlan, StreamItem,
 };
 use data_avalanche_source_mysql::{Flavour, Mode, MySqlConnector, MySqlStream};
 use data_avalanche_testkit::require_it;
@@ -278,6 +278,32 @@ async fn a_purged_file_follows_the_purge_action() {
     cleanup(&n).await;
 }
 
+/// The pipeline's page shows the server's write position, as the current position reads it, and
+/// how long the server keeps its binlogs (`0`: never expired, so none).
+#[tokio::test]
+async fn the_detail_shows_the_head_and_the_binlog_expiry() {
+    require_it!();
+    let (n, mut a) = fresh("start_detail", "CREATE TABLE t (id INT PRIMARY KEY)").await;
+    let c = connector(&pipeline(&n, &["t"], "", ""));
+    let Some(SourceDetail::Mysql(d)) = c.detail().await.unwrap() else {
+        panic!("a MySQL detail")
+    };
+    let now = c.current_position().await.unwrap();
+    assert_eq!(
+        std::mem::discriminant(&d.head),
+        std::mem::discriminant(&now),
+        "{:?} against {now:?}",
+        d.head
+    );
+    let expire: u64 = a
+        .query_first("SELECT @@GLOBAL.binlog_expire_logs_seconds")
+        .await
+        .unwrap()
+        .unwrap();
+    assert_eq!(d.binlog_expire_seconds, (expire > 0).then_some(expire));
+    cleanup(&n).await;
+}
+
 /// Polls `retained` until it reads `streamed`.
 async fn watch(c: &MySqlConnector, streamed: bool) {
     for _ in 0..50 {
````

`crates/data-avalanche-source-pg/tests/pg/slot.rs`:

````diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/slot.rs b/crates/data-avalanche-source-pg/tests/pg/slot.rs
index eb2ee40..905dab0 100644
--- a/crates/data-avalanche-source-pg/tests/pg/slot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/slot.rs
@@ -5,8 +5,8 @@ use std::time::Duration;
 
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    Checkpoint, HaltReason, ServerIdentity, SnapshotReason, SourceConnector, SourceError,
-    SourcePosition, StartPlan, TableRef,
+    Checkpoint, HaltReason, ServerIdentity, SnapshotReason, SourceConnector, SourceDetail,
+    SourceError, SourcePosition, StartPlan, TableRef,
 };
 use data_avalanche_testkit::require_it;
 use tokio_postgres::Client;
@@ -984,6 +984,51 @@ async fn a_root_publication_of_tracked_partitions_is_refused() {
     cleanup(&a, &n).await;
 }
 
+/// The pipeline's page shows the slot, the WAL it holds against `max_slot_wal_keep_size`, and the
+/// publication: none of them before the start creates them.
+#[tokio::test]
+async fn the_detail_shows_the_slot_and_the_publication() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(&a, "slot_detail", DDL).await;
+    let c = connector(&pipeline(&n, &["a", "b"], "", ""));
+    let Some(SourceDetail::Postgres(before)) = c.detail().await.unwrap() else {
+        panic!("a PostgreSQL detail")
+    };
+    assert_eq!((before.slot, before.publication), (None, None));
+    let (stream, _, _) = first_stream(&c).await;
+    let Some(SourceDetail::Postgres(d)) = c.detail().await.unwrap() else {
+        panic!("a PostgreSQL detail")
+    };
+    let slot = d.slot.expect("the slot");
+    assert_eq!(slot.name, n.slot);
+    assert_eq!(slot.wal_status.as_deref(), Some("reserved"));
+    assert!(slot.wal_held_bytes.is_some());
+    let (Some(SourcePosition::Pg(restart)), Some(SourcePosition::Pg(head))) =
+        (slot.restart_lsn, Some(d.head))
+    else {
+        panic!("positions")
+    };
+    assert!(restart <= head);
+    let publication = d.publication.expect("the publication");
+    assert_eq!(
+        (publication.name, publication.tables),
+        (n.publication.clone(), 2)
+    );
+    // The emulator keeps the default: unlimited.
+    let keep: i64 = a
+        .query_one(
+            "SELECT pg_size_bytes(current_setting('max_slot_wal_keep_size'))",
+            &[],
+        )
+        .await
+        .unwrap()
+        .get(0);
+    assert_eq!(d.max_slot_wal_keep_size, u64::try_from(keep).ok());
+    drop(stream);
+    cleanup(&a, &n).await;
+}
+
 /// The heartbeat's read says whether a stream reads the slot: the stream watchdog's evidence
 /// that a silent stream is still there (DA-SRC-16).
 #[tokio::test]
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-core --lib source:: && DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg slot::the_detail && DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-mysql --test mysql start::the_detail`
Expected: the build fails: no `SourceDetail`, no method `detail`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-core/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-core/src/lib.rs b/crates/data-avalanche-core/src/lib.rs
index 67f291e..b9094af 100644
--- a/crates/data-avalanche-core/src/lib.rs
+++ b/crates/data-avalanche-core/src/lib.rs
@@ -38,10 +38,10 @@ pub use sink::{
     WriteMode,
 };
 pub use source::{
-    ChangeStream, Checkpoint, Chunk, ChunkVisibility, InFlightTxn, KeysetCursor, Preflight,
-    Prepared, RescanReport, ResolvedTables, RetainedLog, SchemaHint, ServerIdentity,
-    SnapshotReason, SourceConnector, SourceError, StartPlan, StreamItem, XidSnapshot,
-    identifier_values,
+    ChangeStream, Checkpoint, Chunk, ChunkVisibility, InFlightTxn, KeysetCursor, MySqlDetail,
+    PgDetail, Preflight, Prepared, PublicationDetail, RescanReport, ResolvedTables, RetainedLog,
+    SchemaHint, ServerIdentity, SlotDetail, SnapshotReason, SourceConnector, SourceDetail,
+    SourceError, StartPlan, StreamItem, XidSnapshot, identifier_values,
 };
 pub use table::{
     INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, differ_by_case_alone,
````

`crates/data-avalanche-core/src/source.rs`:

````diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index d4b392c..8fe7da9 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -346,6 +346,63 @@ pub struct RetainedLog {
     pub behind: Option<Duration>,
 }
 
+/// What the source keeps for the pipeline, read every heartbeat for the pipeline's page (the
+/// console's source panel, Admin_Console §4). A figure the server doesn't give is `None`.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+#[serde(tag = "engine", rename_all = "snake_case")]
+pub enum SourceDetail {
+    Postgres(PgDetail),
+    /// MySQL and MariaDB.
+    Mysql(MySqlDetail),
+}
+
+/// PostgreSQL: the pipeline's slot and publication, and the server's write position.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+pub struct PgDetail {
+    /// The server's write position (`pg_current_wal_lsn()`).
+    pub head: SourcePosition,
+    /// The pipeline's slot; `None` when the server has none by its name.
+    pub slot: Option<SlotDetail>,
+    /// `max_slot_wal_keep_size` in bytes: the WAL a slot may hold before the server invalidates
+    /// it. `None` when unlimited (`-1`).
+    pub max_slot_wal_keep_size: Option<u64>,
+    /// The pipeline's publication; `None` when the server has none by its name.
+    pub publication: Option<PublicationDetail>,
+}
+
+/// A PostgreSQL replication slot (`pg_replication_slots`).
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+pub struct SlotDetail {
+    pub name: String,
+    /// A walsender reads it now.
+    pub active: bool,
+    /// The position the server was last told it may release (`confirmed_flush_lsn`).
+    pub confirmed_flush: Option<SourcePosition>,
+    /// The oldest WAL the slot keeps (`restart_lsn`).
+    pub restart_lsn: Option<SourcePosition>,
+    /// The WAL the slot holds on the server's disk: from `restart_lsn` to the write position.
+    pub wal_held_bytes: Option<u64>,
+    /// `reserved`, `extended`, `unreserved` or `lost`.
+    pub wal_status: Option<String>,
+}
+
+/// A PostgreSQL publication and how many tables it publishes.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+pub struct PublicationDetail {
+    pub name: String,
+    pub tables: u64,
+}
+
+/// MySQL and MariaDB: the server's binlog position and how long it keeps its binlogs.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+pub struct MySqlDetail {
+    /// The server's write position: its executed GTID set, or its binlog file and offset.
+    pub head: SourcePosition,
+    /// `binlog_expire_logs_seconds`: how long the server keeps a binlog. `None` when it never
+    /// expires them (`0`).
+    pub binlog_expire_seconds: Option<u64>,
+}
+
 #[derive(Debug, Clone, PartialEq, Eq, Default)]
 pub struct RescanReport {
     /// Tables gone from the source catalog: each halts with `TABLE_DROPPED`.
@@ -421,6 +478,12 @@ pub trait SourceConnector: Send + Sync {
     /// The log the source keeps for the pipeline. `None` when the source keeps nothing for it.
     async fn retained(&self) -> Result<Option<RetainedLog>, SourceError>;
 
+    /// What the source keeps for the pipeline, for the pipeline's page: read every heartbeat
+    /// beside [`Self::retained`]. `None` for a source that says nothing.
+    async fn detail(&self) -> Result<Option<SourceDetail>, SourceError> {
+        Ok(None)
+    }
+
     /// The source's current position: a snapshot chunk's low and high watermarks, and the point
     /// the stream must pass before a rescan's change is applied.
     async fn current_position(&self) -> Result<SourcePosition, SourceError>;
````

`crates/data-avalanche-source-mysql/src/connector.rs`:

````diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
index 5077c89..03507f8 100644
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -10,8 +10,8 @@ use std::time::Duration;
 use async_trait::async_trait;
 use data_avalanche_core::{
     ChangeStream, Checkpoint, Chunk, KeysetCursor, Preflight, Prepared, RescanReport,
-    ResolvedTables, RetainedLog, Row, SchemaDelta, SchemaHint, SourceConnector, SourceError,
-    SourcePosition, TableRef, TableSchema,
+    ResolvedTables, RetainedLog, Row, SchemaDelta, SchemaHint, SourceConnector, SourceDetail,
+    SourceError, SourcePosition, TableRef, TableSchema,
 };
 use mysql_async::prelude::Queryable;
 use tokio::sync::OnceCell;
@@ -476,6 +476,10 @@ impl SourceConnector for MySqlConnector {
         MySqlConnector::retained(self).await
     }
 
+    async fn detail(&self) -> Result<Option<SourceDetail>, SourceError> {
+        MySqlConnector::detail(self).await
+    }
+
     async fn current_position(&self) -> Result<SourcePosition, SourceError> {
         self.position_now().await
     }
````

`crates/data-avalanche-source-mysql/src/start.rs`:

````diff
diff --git a/crates/data-avalanche-source-mysql/src/start.rs b/crates/data-avalanche-source-mysql/src/start.rs
index 08a78bf..5e099cf 100644
--- a/crates/data-avalanche-source-mysql/src/start.rs
+++ b/crates/data-avalanche-source-mysql/src/start.rs
@@ -20,8 +20,8 @@ use std::time::{Duration, SystemTime, UNIX_EPOCH};
 use data_avalanche_config::BinlogPurgeAction;
 use data_avalanche_core::position::BinlogFilePos;
 use data_avalanche_core::{
-    Checkpoint, Prepared, RetainedLog, ServerIdentity, SnapshotReason, SourceError, SourcePosition,
-    StartPlan,
+    Checkpoint, MySqlDetail, Prepared, RetainedLog, ServerIdentity, SnapshotReason, SourceDetail,
+    SourceError, SourcePosition, StartPlan,
 };
 use mysql_async::prelude::Queryable;
 
@@ -427,6 +427,25 @@ impl MySqlConnector {
             behind,
         }))
     }
+
+    /// The server's write position and how long it keeps its binlogs, for the pipeline's page.
+    pub async fn detail(&self) -> Result<Option<SourceDetail>, SourceError> {
+        let head = self.position_now().await?;
+        let expire: Option<u64> = self
+            .conn
+            .command("read the binlog expiry", |c| {
+                Box::pin(async move {
+                    c.query_first("SELECT @@GLOBAL.binlog_expire_logs_seconds")
+                        .await
+                })
+            })
+            .await?;
+        Ok(Some(SourceDetail::Mysql(MySqlDetail {
+            head,
+            // 0: the server never expires its binlogs.
+            binlog_expire_seconds: expire.filter(|s| *s > 0),
+        })))
+    }
 }
 
 #[cfg(test)]
````

`crates/data-avalanche-source-pg/src/connector.rs`:

````diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index e8a6374..e8bf817 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -13,7 +13,7 @@ use data_avalanche_config::{
 use data_avalanche_core::{
     ChangeStream, Checkpoint, Chunk, HaltReason, InFlightTxn, KeysetCursor, Preflight, Prepared,
     RescanReport, ResolvedTables, RetainedLog, Row, SchemaDelta, SchemaHint, SourceConnector,
-    SourceError, SourcePosition, TableRef, TableSchema,
+    SourceDetail, SourceError, SourcePosition, TableRef, TableSchema,
 };
 use tokio::sync::Mutex;
 
@@ -445,6 +445,10 @@ impl SourceConnector for PgConnector {
         PgConnector::retained(self).await
     }
 
+    async fn detail(&self) -> Result<Option<SourceDetail>, SourceError> {
+        PgConnector::detail(self).await
+    }
+
     async fn current_position(&self) -> Result<SourcePosition, SourceError> {
         PgConnector::current_position(self).await
     }
````

`crates/data-avalanche-source-pg/src/slot.rs`:

````diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
index 29e2321..bd4265c 100644
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -7,8 +7,9 @@ use std::collections::BTreeSet;
 use data_avalanche_config::{SlotManagement, SlotOnOrphan, SourceChangeAction};
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    Checkpoint, HaltReason, Prepared, ResolvedTables, RetainedLog, ServerIdentity, SnapshotReason,
-    SourceError, SourcePosition, StartPlan, TableRef,
+    Checkpoint, HaltReason, PgDetail, Prepared, PublicationDetail, ResolvedTables, RetainedLog,
+    ServerIdentity, SlotDetail, SnapshotReason, SourceDetail, SourceError, SourcePosition,
+    StartPlan, TableRef,
 };
 use tokio_postgres::Client;
 
@@ -1115,6 +1116,62 @@ impl PgConnector {
             })
         }))
     }
+
+    /// The slot, the publication and the write position, for the pipeline's page: one query on
+    /// views every role reads.
+    pub async fn detail(&self) -> Result<Option<SourceDetail>, SourceError> {
+        let client = self.conn.client().await?;
+        let row = self
+            .conn
+            .command(
+                "read the slot and the publication",
+                client.query_one(
+                    "SELECT pg_current_wal_lsn()::text,
+                            pg_size_bytes(current_setting('max_slot_wal_keep_size'))::bigint,
+                            s.slot_name IS NOT NULL, s.active,
+                            s.confirmed_flush_lsn::text, s.restart_lsn::text,
+                            pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint,
+                            s.wal_status,
+                            p.pubname IS NOT NULL,
+                            (SELECT count(*) FROM pg_catalog.pg_publication_tables t
+                              WHERE t.pubname = $2)
+                       FROM (SELECT 1) one
+                       LEFT JOIN pg_catalog.pg_replication_slots s ON s.slot_name = $1
+                       LEFT JOIN pg_catalog.pg_publication p ON p.pubname = $2",
+                    &[&self.slot_name(), &self.pg.publication],
+                ),
+            )
+            .await?;
+        let lsn = |text: Option<String>| -> Result<Option<SourcePosition>, SourceError> {
+            text.map(|t| parse_lsn(&t).map(SourcePosition::Pg))
+                .transpose()
+        };
+        let slot = if row.get::<_, bool>(2) {
+            Some(SlotDetail {
+                name: self.slot_name().to_owned(),
+                active: row.get(3),
+                confirmed_flush: lsn(row.get(4))?,
+                restart_lsn: lsn(row.get(5))?,
+                wal_held_bytes: row
+                    .get::<_, Option<i64>>(6)
+                    .map(|b| u64::try_from(b).unwrap_or(0)),
+                wal_status: row.get(7),
+            })
+        } else {
+            None
+        };
+        let publication = row.get::<_, bool>(8).then(|| PublicationDetail {
+            name: self.pg.publication.clone(),
+            tables: u64::try_from(row.get::<_, i64>(9)).unwrap_or(0),
+        });
+        Ok(Some(SourceDetail::Postgres(PgDetail {
+            head: SourcePosition::Pg(parse_lsn(row.get(0))?),
+            slot,
+            // -1: unlimited.
+            max_slot_wal_keep_size: u64::try_from(row.get::<_, i64>(1)).ok(),
+            publication,
+        })))
+    }
 }
 
 #[cfg(test)]
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-core --lib source:: && DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg slot::the_detail && DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-mysql --test mysql start::the_detail`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

Run the MySQL one on each target too: `DATA_AVALANCHE_MYSQL=mysql-84`, `mysql-84-nogtid`, `mariadb-118`.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core/src/lib.rs crates/data-avalanche-core/src/source.rs crates/data-avalanche-source-mysql/src/connector.rs crates/data-avalanche-source-mysql/src/start.rs crates/data-avalanche-source-mysql/tests/mysql/start.rs crates/data-avalanche-source-pg/src/connector.rs crates/data-avalanche-source-pg/src/slot.rs crates/data-avalanche-source-pg/tests/pg/slot.rs
git commit -m "feat(source): the source's detail for the pipeline's page — PostgreSQL's slot and publication, MySQL's head and binlog expiry"
```

---

### Task 3: A commit's cost; a table's facts (`data-avalanche-core`, `-sink-delta`, `-testkit`)

**Files:**
- Modify: `crates/data-avalanche-core/src/lib.rs`, `crates/data-avalanche-core/src/sink.rs`, `crates/data-avalanche-sink-delta/src/sink.rs`, `crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`, `crates/data-avalanche-testkit/src/memory_sink.rs`

**Interfaces:**
- Consumes: `Sink`, `CommitOutcome` (M4); the Delta sink's commit and `with_table` (M4).
- Produces (core crate root): `CommitCost { files_rewritten: u64, files: Option<u64> }`
  (`Default`); `CommitOutcome::Committed { version, cost }`; `TableFacts { location:
  Option<String>, version, files: Option<u64>, bytes: Option<u64>, rows: Option<u64> }`
  (`Serialize`); `Sink::facts(&self, &TableRef) -> Result<Option<TableFacts>, SinkError>`, by
  default `None`. Delta: the truncate's and the MERGE's removed files, the snapshot's file count;
  facts from `log_data()` (`size`, `num_records`; no rows when a file has no count), `location`
  the table's URL. `MemorySink`: no cost; facts with the version and the rows.

- [ ] **Step 1: Write the failing tests**

The tests: `a_commit_reports_its_cost_and_the_facts_follow` (Delta); `upserts_and_deletes_mirror_by_key_and_replay_is_idempotent` reads the facts (`MemorySink`); the literals take `cost`.

`crates/data-avalanche-sink-delta/src/sink.rs`:

````diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
index 7ea423f..4882573 100644
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -944,12 +982,60 @@ mod tests {
         t.truncate = true;
         assert_eq!(
             s.commit(t.clone(), marker(1, &t)).await.unwrap(),
-            CommitOutcome::Committed { version: 1 }
+            CommitOutcome::Committed {
+                version: 1,
+                cost: CommitCost {
+                    files_rewritten: 0,
+                    files: Some(1)
+                }
+            }
         );
         assert_eq!(s.stats().retries, 0);
         assert_eq!(ids(&s).await, [1]);
     }
 
+    /// The table's page: a MERGE that rewrites a file says so, and the table's facts follow its
+    /// commits; an absent table has none.
+    #[tokio::test]
+    async fn a_commit_reports_its_cost_and_the_facts_follow() {
+        let dir = tempfile::tempdir().unwrap();
+        let s = sink(&dir);
+        assert_eq!(s.facts(orders().table()).await.unwrap(), None);
+        s.ensure_table(&spec(&orders())).await.unwrap();
+        let empty = s.facts(orders().table()).await.unwrap().expect("a table");
+        assert_eq!(
+            (empty.files, empty.bytes, empty.rows),
+            (Some(0), Some(0), Some(0))
+        );
+        let seed = rows(10, &[1, 2]);
+        let CommitOutcome::Committed { cost, .. } =
+            s.commit(seed.clone(), marker(1, &seed)).await.unwrap()
+        else {
+            panic!("committed")
+        };
+        assert_eq!(
+            cost,
+            CommitCost {
+                files_rewritten: 0,
+                files: Some(1)
+            }
+        );
+        let update = rows(20, &[1]);
+        let CommitOutcome::Committed { version, cost } =
+            s.commit(update.clone(), marker(1, &update)).await.unwrap()
+        else {
+            panic!("committed")
+        };
+        assert_eq!(cost.files_rewritten, 1, "the MERGE rewrote the seed's file");
+        let facts = s.facts(orders().table()).await.unwrap().expect("a table");
+        let url = local(&dir).table_url(orders().table()).unwrap();
+        assert_eq!(facts.location, Some(url.to_string()));
+        assert_eq!(facts.version, version);
+        assert_eq!(facts.files, cost.files);
+        assert_eq!(facts.rows, Some(2));
+        assert!(facts.bytes.is_some_and(|b| b > 0), "{facts:?}");
+    }
+
     /// DA-SNAP-15: a job's truncate, a snapshot set with no rows, empties the table with its
     /// DELETE alone, fenced, and isn't skipped though the marker covers its position; the marker
     /// stays.
@@ -988,7 +1074,7 @@ mod tests {
         s.ensure_table(&keyed).await.unwrap();
         let mut seed = rows(10, &[1]);
         seed.identifier = vec!["at".into()];
-        let CommitOutcome::Committed { version } =
+        let CommitOutcome::Committed { version, .. } =
             s.commit(seed.clone(), marker(1, &seed)).await.unwrap()
         else {
             panic!("committed")
@@ -1020,7 +1106,7 @@ mod tests {
         s.commit(seed.clone(), marker(1, &seed)).await.unwrap();
         let mut t = rows(20, &[2]);
         t.truncate = true;
-        let CommitOutcome::Committed { version } =
+        let CommitOutcome::Committed { version, .. } =
             s.commit(t.clone(), marker(1, &t)).await.unwrap()
         else {
             panic!("committed")
````

`crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`:

````diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs b/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
index 784c0d8..46d1034 100644
--- a/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
@@ -42,13 +42,13 @@ pub async fn mirror_and_replay(location: DeltaLocation) {
         vec![upserts(&[(1, Some("A")), (4, Some("d"))], None)],
         vec![deletes(&[2])],
     );
-    let CommitOutcome::Committed { version: v1 } = commit(&s, first.clone(), 1).await else {
+    let CommitOutcome::Committed { version: v1, .. } = commit(&s, first.clone(), 1).await else {
         panic!("first commit")
     };
-    assert_eq!(
+    assert!(matches!(
         commit(&s, second.clone(), 1).await,
-        CommitOutcome::Committed { version: v1 + 1 }
-    );
+        CommitOutcome::Committed { version, .. } if version == v1 + 1
+    ));
     let want = owned(&[(1, Some("A")), (3, Some("c")), (4, Some("d"))]);
     assert_eq!(rows(&s).await, want);
     assert_eq!(commit(&s, first, 1).await, CommitOutcome::SkippedCovered);
````

`crates/data-avalanche-testkit/src/memory_sink.rs`:

````diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
index 7de172c..f6630a3 100644
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -627,7 +644,10 @@ mod tests {
         );
         assert_eq!(
             commit(&sink, first.clone(), 1).await,
-            CommitOutcome::Committed { version: 1 }
+            CommitOutcome::Committed {
+                version: 1,
+                cost: CommitCost::default()
+            }
         );
         let second = cs(
             20,
@@ -636,6 +656,22 @@ mod tests {
         );
         commit(&sink, second.clone(), 1).await;
         assert_eq!(rows(&sink), owned(&[(2, Some("B"))]));
+        assert_eq!(
+            sink.facts(&first.table).await.unwrap(),
+            Some(TableFacts {
+                location: None,
+                version: 2,
+                files: None,
+                bytes: None,
+                rows: Some(1),
+            })
+        );
+        assert_eq!(
+            sink.facts(&TableRef::new("public", "absent"))
+                .await
+                .unwrap(),
+            None
+        );
 
         // Replay under the guard: skipped, nothing changes.
         assert_eq!(
@@ -735,7 +771,10 @@ mod tests {
         assert_eq!(sink.version(&t), Some(0), "nothing written");
         assert_eq!(
             sink.commit(set, m).await.unwrap(),
-            CommitOutcome::Committed { version: 1 }
+            CommitOutcome::Committed {
+                version: 1,
+                cost: CommitCost::default()
+            }
         );
         sink.fail(SinkOp::TableState, None, 1, || SinkError::Permanent {
             reason: "gone".into(),
@@ -822,7 +861,10 @@ mod tests {
         });
         assert_eq!(
             commit(&sink, slice.clone(), 1).await,
-            CommitOutcome::Committed { version: 2 },
+            CommitOutcome::Committed {
+                version: 2,
+                cost: CommitCost::default()
+            },
             "the marker equals the pinned start, so the slice is written"
         );
         commit(
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-sink-delta -p data-avalanche-testkit`
Expected: the build fails: no `CommitCost`, no `TableFacts`, no method `facts`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-core/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-core/src/lib.rs b/crates/data-avalanche-core/src/lib.rs
index b9094af..40a50d0 100644
--- a/crates/data-avalanche-core/src/lib.rs
+++ b/crates/data-avalanche-core/src/lib.rs
@@ -33,9 +33,9 @@ pub use logical::{LogicalType, LogicalTypeError, MAX_LIST_DEPTH};
 pub use position::{PositionError, SourcePosition};
 pub use schema::{ColumnSchema, HaltReason, TableSchema, canonical_logical, hop2_compatible};
 pub use sink::{
-    CommitMarker, CommitOutcome, EvolveOutcome, PartitionField, PartitionTransform, SchemaOp, Sink,
-    SinkCapabilities, SinkError, SinkKind, SinkStats, SinkTableState, TableSpec, TransformKind,
-    WriteMode,
+    CommitCost, CommitMarker, CommitOutcome, EvolveOutcome, PartitionField, PartitionTransform,
+    SchemaOp, Sink, SinkCapabilities, SinkError, SinkKind, SinkStats, SinkTableState, TableFacts,
+    TableSpec, TransformKind, WriteMode,
 };
 pub use source::{
     ChangeStream, Checkpoint, Chunk, ChunkVisibility, InFlightTxn, KeysetCursor, MySqlDetail,
````

`crates/data-avalanche-core/src/sink.rs`:

````diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
index 66d0951..480b3f2 100644
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -7,6 +7,7 @@ use std::fmt;
 
 use arrow_schema::SchemaRef;
 use async_trait::async_trait;
+use serde::Serialize;
 use thiserror::Error;
 
 use crate::{
@@ -251,10 +252,36 @@ pub struct SinkStats {
     pub rows_copied: u64,
 }
 
+/// What one commit cost, for the table's figures on the pipeline's page (D-MS.15: a MERGE that
+/// rewrites most of the table's files is the cost driver).
+#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
+pub struct CommitCost {
+    /// Files the commit replaced or removed.
+    pub files_rewritten: u64,
+    /// The table's files after the commit; `None` when the sink doesn't count them.
+    pub files: Option<u64>,
+}
+
+/// A table as the sink holds it now, read on request for the table's page. A figure the sink
+/// can't give is `None`.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+pub struct TableFacts {
+    /// Where the table lives (Delta: its root URL); `None` when the sink has no such place.
+    pub location: Option<String>,
+    /// The table's latest version.
+    pub version: u64,
+    pub files: Option<u64>,
+    /// The size of its data files.
+    pub bytes: Option<u64>,
+    /// Its rows, when every data file records its count.
+    pub rows: Option<u64>,
+}
+
 #[derive(Debug, Clone, PartialEq, Eq)]
 pub enum CommitOutcome {
     Committed {
         version: u64,
+        cost: CommitCost,
     },
     /// The guard found the range already committed; nothing was written.
     SkippedCovered,
@@ -305,6 +332,12 @@ pub trait Sink: Send + Sync {
         SinkStats::default()
     }
     async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError>;
+    /// The table as the sink holds it now, for the table's page; `None` when the table is absent
+    /// or the sink says nothing. Any node may ask: it reads the table, not the writer's memory.
+    async fn facts(&self, table: &TableRef) -> Result<Option<TableFacts>, SinkError> {
+        let _ = table;
+        Ok(None)
+    }
     /// The tables the sink holds at the pipeline's location (Delta: each `<schema>/<table>/`
     /// root with a `_delta_log` under the pipeline's `sink.prefix`), whether the pipeline
     /// resolves or caches them or not: a source change repositions every one (DA-SRC-32).
````

`crates/data-avalanche-sink-delta/src/sink.rs`:

````diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
index 7ea423f..4882573 100644
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -6,9 +6,9 @@ use std::sync::{Arc, Mutex, Once};
 use arrow_array::RecordBatch;
 use async_trait::async_trait;
 use data_avalanche_core::{
-    CommitMarker, CommitOutcome, EvolveOutcome, SchemaDelta, SchemaOp, Sink, SinkCapabilities,
-    SinkError, SinkKind, SinkStats, SinkTableState, SourcePosition, TableChangeSet, TableRef,
-    TableSpec, TransformKind, WriteMode,
+    CommitCost, CommitMarker, CommitOutcome, EvolveOutcome, SchemaDelta, SchemaOp, Sink,
+    SinkCapabilities, SinkError, SinkKind, SinkStats, SinkTableState, SourcePosition,
+    TableChangeSet, TableFacts, TableRef, TableSpec, TransformKind, WriteMode,
 };
 use deltalake::DeltaTableError;
 use deltalake::kernel::Transaction;
@@ -251,10 +251,12 @@ impl DeltaSink {
         let txn_version = i64::try_from(marker.owner_epoch)
             .map_err(|_| permanent(format!("owner epoch {}", marker.owner_epoch)))?;
         let mut table = h.table().clone();
+        let mut rewritten = 0;
         if cs.truncate {
             // The clear carries the fence but no position: a crash before the MERGE replays both.
             table = match truncate(table, self.props(marker.owner_epoch, txn_version, None)).await {
                 Ok((t, metrics)) => {
+                    rewritten += metrics.num_removed_files as u64;
                     self.count(|s| {
                         s.files_removed += metrics.num_removed_files as u64;
                         s.rows_deleted += metrics.num_deleted_rows.unwrap_or(0) as u64;
@@ -285,6 +287,7 @@ impl DeltaSink {
             let props = self.props(marker.owner_epoch, txn_version, Some(&position));
             match merge(table, cs, &types, props, self.config.merge_memory_limit).await {
                 Ok((t, metrics)) => {
+                    rewritten += metrics.num_target_files_removed as u64;
                     self.count(|s| s.add(&metrics));
                     table = t;
                 }
@@ -303,7 +306,17 @@ impl DeltaSink {
             }
         };
         h.remember(version, position);
-        Ok(Step::Done(CommitOutcome::Committed { version }))
+        let files = table
+            .snapshot()
+            .ok()
+            .map(|s| s.log_data().num_files() as u64);
+        Ok(Step::Done(CommitOutcome::Committed {
+            version,
+            cost: CommitCost {
+                files_rewritten: rewritten,
+                files,
+            },
+        }))
     }
 }
 
@@ -357,6 +370,31 @@ impl Sink for DeltaSink {
         .await
     }
 
+    /// The table's latest version, its files and their size, and its rows from the files'
+    /// `numRecords` statistics (none when a file has none).
+    async fn facts(&self, table: &TableRef) -> Result<Option<TableFacts>, SinkError> {
+        self.with_table(table, async |h| {
+            if !h.refresh().await? {
+                return Ok(None);
+            }
+            let snapshot = h.table().snapshot().map_err(sink_error)?;
+            let (mut files, mut bytes, mut rows) = (0, 0, Some(0));
+            for file in snapshot.log_data().iter() {
+                files += 1;
+                bytes += u64::try_from(file.size()).unwrap_or(0);
+                rows = rows.zip(file.num_records()).map(|(r, n)| r + n as u64);
+            }
+            Ok(Some(TableFacts {
+                location: Some(self.config.location.table_url(table)?.to_string()),
+                version: h.version(),
+                files: Some(files),
+                bytes: Some(bytes),
+                rows,
+            }))
+        })
+        .await
+    }
+
     /// Lists `<schema>/<table>/` under the sink root, two levels, and keeps each table root that
     /// holds a `_delta_log`. The co-located state directory isn't a schema; a segment the escape
     /// can't have made isn't a table of the pipeline's.
````

`crates/data-avalanche-testkit/src/memory_sink.rs`:

````diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
index 7de172c..f6630a3 100644
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -13,9 +13,10 @@ use arrow_row::{OwnedRow, RowConverter, SortField};
 use arrow_schema::{Field, Schema, SchemaRef};
 use async_trait::async_trait;
 use data_avalanche_core::{
-    CommitMarker, CommitOutcome, EvolveOutcome, SchemaChange, SchemaDelta, SchemaOp, Sink,
-    SinkCapabilities, SinkError, SinkKind, SinkTableState, SourcePosition, TableChangeSet,
-    TableRef, TableSpec, TransformKind, UNCHANGED_MASK_PREFIX, WriteMode, added_column_field,
+    CommitCost, CommitMarker, CommitOutcome, EvolveOutcome, SchemaChange, SchemaDelta, SchemaOp,
+    Sink, SinkCapabilities, SinkError, SinkKind, SinkTableState, SourcePosition, TableChangeSet,
+    TableFacts, TableRef, TableSpec, TransformKind, UNCHANGED_MASK_PREFIX, WriteMode,
+    added_column_field,
 };
 use tokio::sync::watch;
 
@@ -345,6 +346,18 @@ impl Sink for MemorySink {
         &self.capabilities
     }
 
+    /// The version and the rows; the reference sink keeps no files.
+    async fn facts(&self, table: &TableRef) -> Result<Option<TableFacts>, SinkError> {
+        self.fault(SinkOp::TableState, table)?;
+        Ok(self.tables().get(table).map(|t| TableFacts {
+            location: None,
+            version: t.version,
+            files: None,
+            bytes: None,
+            rows: Some(t.rows.len() as u64),
+        }))
+    }
+
     async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError> {
         self.fault(SinkOp::TableState, table)?;
         Ok(match self.tables().get(table) {
@@ -488,7 +501,11 @@ impl Sink for MemorySink {
             committed_position,
             ..marker
         });
-        Ok(CommitOutcome::Committed { version: t.version })
+        // The reference sink keeps no files: it doesn't measure a commit's cost.
+        Ok(CommitOutcome::Committed {
+            version: t.version,
+            cost: CommitCost::default(),
+        })
     }
 }
 
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-sink-delta -p data-avalanche-testkit`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core/src/lib.rs crates/data-avalanche-core/src/sink.rs crates/data-avalanche-sink-delta/src/sink.rs crates/data-avalanche-sink-delta/tests/sink/scenarios.rs crates/data-avalanche-testkit/src/memory_sink.rs
git commit -m "feat(sink): a commit reports its cost; a table's facts read on request (Delta: version, files, bytes, rows)"
```

---

### Task 4: Each table's figures; the source's detail every heartbeat (`data-avalanche-pipeline`, `-testkit`)

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/lib.rs`, `crates/data-avalanche-pipeline/src/session.rs`, `crates/data-avalanche-pipeline/src/status.rs`, `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-pipeline/src/writer.rs`, `crates/data-avalanche-testkit/src/fake_source.rs`

**Interfaces:**
- Consumes: Task 2's `detail`, Task 3's `CommitOutcome::Committed { version, cost }`; the
  writer (`WriterEvent`), the session's heartbeat (M6a, M10).
- Produces (pipeline crate root): `TableCommit { table, rows, snapshot, version, cost, at }`,
  `TableFigures { changes, last_commit, version, cost }` with `changes_per_minute(&self,
  DateTime<Utc>) -> u64`, `CHANGES_WINDOW` (60 s); `WorkerStatus::{tracked: BTreeSet<TableRef>,
  tables: BTreeMap<TableRef, TableFigures>, source: Option<SourceDetail>, source_read_at:
  Option<DateTime<Utc>>}`, `WorkerStatus::committed(&mut self, &TableCommit)`. The writer sends
  `WriterEvent::Committed`; the heartbeat reads `detail` in its own `JoinSet` (a stalled read holds
  up neither the watchdog nor the guard); a failed read keeps the last. Testkit:
  `FakeSource::set_detail(Option<SourceDetail>)`.

- [ ] **Step 1: Write the failing tests**

The tests: `a_table_s_figures_follow_its_commits` (status); `each_table_s_commits_show_in_its_figures`, `the_retained_wal_is_reported_and_bounded` reads the detail too (session); `the_start_counts_the_sink_columns_the_source_lacks` checks `tracked` (worker); the writer's harness and `a_durable_job_leaves_the_next_job_s_lag` take the new event.

`crates/data-avalanche-pipeline/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
index a162864..ea3f276 100644
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -37,6 +37,7 @@ pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
 pub use observe::{NoObserver, Observer};
 pub use policy::{FlushPolicy, FlushReason, TxnLimits};
 pub use status::{
-    Command, Exit, Phase, ReconcileLog, RowsRead, SnapshotProgress, Tuning, WorkerStatus,
+    CHANGES_WINDOW, Command, Exit, Phase, ReconcileLog, RowsRead, SnapshotProgress, TableCommit,
+    TableFigures, Tuning, WorkerStatus,
 };
 pub use worker::{PipelineWorker, WorkerDeps, WorkerHandle};
````

`crates/data-avalanche-pipeline/src/session.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index ce3840c..0763e60 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -1544,6 +1574,32 @@ mod tests {
         assert_eq!(st.rows_read.insert, 1);
     }
 
+    /// Each table's commits show in its figures: the changes, the version the sink reported,
+    /// and when.
+    #[tokio::test]
+    async fn each_table_s_commits_show_in_its_figures() {
+        let w = World::new(&["a", "b"]);
+        w.started(&["a", "b"]).await;
+        let before = chrono::Utc::now();
+        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
+            w.source.commit(
+                FakeTxn::new()
+                    .insert(&t("a"), row(1, "x"))
+                    .insert(&t("b"), row(1, "y")),
+            );
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
+            durable(&status, w.source.wal_end()).await;
+        })
+        .await;
+        end.unwrap();
+        let a = &st.tables[&t("a")];
+        assert_eq!((a.changes, a.version), (2, w.sink.version(&t("a"))));
+        assert_eq!(a.changes_per_minute(chrono::Utc::now()), 2);
+        assert!(a.last_commit.is_some_and(|at| at >= before), "{a:?}");
+        assert_eq!(a.cost, Some(data_avalanche_core::CommitCost::default()));
+        assert_eq!(st.tables[&t("b")].changes, 1);
+    }
+
     /// A column the source drops stays in the sink: the evolve's soft-drop is counted.
     #[tokio::test]
     async fn a_dropped_column_is_counted_soft_dropped() {
@@ -1613,9 +1669,15 @@ mod tests {
         pump(&mut s, &second).await;
         s.flush().unwrap();
         assert_eq!((s.handed, s.durable), (2, 0));
-        // The writer's events wait in the channel: take the first job's `Durable` only.
-        let ev = s.events.recv().await;
-        assert!(s.writer_event(ev).await.unwrap().is_none());
+        // The writer's events wait in the channel: take them up to the first job's `Durable`.
+        loop {
+            let ev = s.events.recv().await;
+            let durable = matches!(ev, Some(WriterEvent::Durable(_)));
+            assert!(s.writer_event(ev).await.unwrap().is_none());
+            if durable {
+                break;
+            }
+        }
         assert_eq!(s.durable, 1);
         assert_eq!(
             status.borrow().unsynced_since_micros,
@@ -2286,12 +2348,20 @@ mod tests {
         let (mut core, _control, status) = w.core(&p, &["a"]);
         core.settings.heartbeat = Duration::from_millis(5);
         let s = Session::open(&mut core).await.unwrap().unwrap();
+        let detail = SourceDetail::Mysql(data_avalanche_core::MySqlDetail {
+            head: w.source.wal_end(),
+            binlog_expire_seconds: Some(60),
+        });
         let driver = async {
             w.source.set_retained(Some(1024));
             w.source.set_behind(Some(Duration::from_secs(3)));
-            wait_until("the lag is reported", || {
+            w.source.set_detail(Some(detail.clone()));
+            wait_until("the lag and the detail are reported", || {
                 let st = status.borrow();
-                st.retained_bytes == Some(1024) && st.source_behind == Some(Duration::from_secs(3))
+                st.retained_bytes == Some(1024)
+                    && st.source_behind == Some(Duration::from_secs(3))
+                    && st.source.as_ref() == Some(&detail)
+                    && st.source_read_at.is_some()
             })
             .await;
             w.source.set_retained(Some(65 << 20));
````

`crates/data-avalanche-pipeline/src/status.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
index 45d3050..6cc9751 100644
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -255,6 +333,49 @@ mod tests {
         assert_eq!(st.warnings_dropped, 0);
     }
 
+    fn commit(rows: u64, snapshot: bool, secs: i64) -> TableCommit {
+        TableCommit {
+            table: TableRef::new("public", "t"),
+            rows,
+            snapshot,
+            version: secs as u64,
+            cost: CommitCost {
+                files_rewritten: rows,
+                files: Some(10),
+            },
+            at: DateTime::from_timestamp(secs, 0).unwrap(),
+        }
+    }
+
+    #[test]
+    fn a_table_s_figures_follow_its_commits() {
+        let mut st = WorkerStatus::default();
+        st.committed(&commit(5, false, 1_000));
+        st.committed(&commit(100, true, 1_030));
+        st.committed(&commit(3, false, 1_050));
+        // A marker-only commit moves the version, not the cost.
+        st.committed(&commit(0, false, 1_055));
+        let f = &st.tables[&TableRef::new("public", "t")];
+        assert_eq!(f.changes, 8, "a snapshot's rows aren't changes");
+        assert_eq!(f.version, Some(1_055));
+        assert_eq!(f.last_commit, DateTime::from_timestamp(1_055, 0));
+        assert_eq!(
+            f.cost,
+            Some(CommitCost {
+                files_rewritten: 3,
+                files: Some(10)
+            })
+        );
+        let at = |secs| DateTime::from_timestamp(secs, 0).unwrap();
+        assert_eq!(f.changes_per_minute(at(1_055)), 8);
+        assert_eq!(
+            f.changes_per_minute(at(1_060)),
+            3,
+            "the first is a minute old"
+        );
+        assert_eq!(f.changes_per_minute(at(1_200)), 0);
+    }
+
     #[test]
     fn past_the_cap_the_oldest_warning_goes() {
         let mut st = WorkerStatus::default();
````

`crates/data-avalanche-pipeline/src/worker.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index b5a7032..e667386 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -1422,6 +1425,7 @@ mod tests {
         .await;
         end.unwrap();
         assert_eq!(st.soft_dropped, BTreeMap::from([(t("a"), 1)]));
+        assert_eq!(st.tracked, BTreeSet::from([t("a")]), "Phase 0's tables");
     }
 
     #[tokio::test]
````

`crates/data-avalanche-pipeline/src/writer.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index ad43c91..cc3d190 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -441,9 +451,12 @@ mod tests {
             }
             drop(jobs_tx);
             run(ctx, jobs_rx, events_tx).await;
+            // The table figures' events are the session's tests'.
             let mut out = vec![];
             while let Some(e) = events_rx.recv().await {
-                out.push(e);
+                if !matches!(e, WriterEvent::Committed(_)) {
+                    out.push(e);
+                }
             }
             out
         }
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-pipeline --lib`
Expected: the build fails: no `TableCommit`, no fields `tables`, `tracked`, `source` in `WorkerStatus`, no method `set_detail`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-pipeline/src/session.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index ce3840c..0763e60 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -16,8 +16,9 @@ use std::time::{Duration, Instant};
 use data_avalanche_config::Pipeline;
 use data_avalanche_core::{
     CdcEvent, ChangeStream, ColumnValue, HaltReason, Op, RescanReport, RetainedLog, Row,
-    SchemaHint, ServerIdentity, Sink, SinkTableState, SourceConnector, SourceError, SourcePosition,
-    SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary, identifier_values,
+    SchemaHint, ServerIdentity, Sink, SinkTableState, SourceConnector, SourceDetail, SourceError,
+    SourcePosition, SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary,
+    identifier_values,
 };
 use data_avalanche_state::{PipelineDocs, TableHalt};
 use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
@@ -232,6 +233,9 @@ pub(crate) struct Session<'c> {
     /// The heartbeat's read of the source's log, one at a time, run beside the loop: a source
     /// that doesn't answer holds up neither the stream nor a drain (DA-SRC-16).
     retained_read: JoinSet<Result<Option<RetainedLog>, SourceError>>,
+    /// The heartbeat's read of the source's detail, apart from `retained_read`: a stalled one
+    /// holds up neither the watchdog nor the guard.
+    detail_read: JoinSet<Result<Option<SourceDetail>, SourceError>>,
     watchdog: Watchdog,
     /// A reload changed the heartbeat or the rescan interval: the loop re-arms its timers.
     retimed: bool,
@@ -406,6 +410,7 @@ impl<'c> Session<'c> {
             draining: false,
             paused: None,
             retained_read: JoinSet::new(),
+            detail_read: JoinSet::new(),
             watchdog: Watchdog::default(),
             retimed: false,
             open_since: None,
@@ -454,6 +459,7 @@ impl<'c> Session<'c> {
                     }
                 }),
                 WriterEvent::Degraded(why) => self.core.update(|st| st.degraded = why),
+                WriterEvent::Committed(c) => self.core.update(|st| st.committed(&c)),
                 WriterEvent::Durable(cp) => {
                     // A completion durable now ends its snapshot, as in the loop.
                     self.durable += 1;
@@ -552,6 +558,9 @@ impl<'c> Session<'c> {
                 read = self.retained_read.join_next(), if !self.retained_read.is_empty() => {
                     self.wal_lag(read.expect("not empty"))?;
                 }
+                read = self.detail_read.join_next(), if !self.detail_read.is_empty() => {
+                    self.source_detail(read.expect("not empty"));
+                }
                 _ = async { rescan.as_mut().expect("guarded").tick().await },
                     if rescan.is_some() && self.rescan.is_none() && !parked =>
                 {
@@ -645,6 +654,10 @@ impl<'c> Session<'c> {
                 self.core.update(|st| st.degraded = why);
                 Ok(None)
             }
+            Some(WriterEvent::Committed(c)) => {
+                self.core.update(|st| st.committed(&c));
+                Ok(None)
+            }
             Some(WriterEvent::Durable(cp)) => {
                 self.durable += 1;
                 debug_assert!(self.durable <= self.handed, "one Durable per job");
@@ -1066,6 +1079,10 @@ impl<'c> Session<'c> {
             self.retained_read
                 .spawn(async move { source.retained().await });
         }
+        if self.detail_read.is_empty() {
+            let source = self.core.source.clone();
+            self.detail_read.spawn(async move { source.detail().await });
+        }
         self.report_progress();
         let why = self.watchdog.beat(self.reads_stream())?;
         Some(End::Source(SourceError::Transient(why)))
@@ -1135,6 +1152,19 @@ impl<'c> Session<'c> {
         }
     }
 
+    /// A heartbeat's read of the source's detail came back: the status takes it, with when. A
+    /// failed read keeps the last one, whose time then says how old it is.
+    fn source_detail(&self, read: Result<Result<Option<SourceDetail>, SourceError>, JoinError>) {
+        match read {
+            Ok(Ok(detail)) => self.core.update(|st| {
+                st.source = detail;
+                st.source_read_at = Some(chrono::Utc::now());
+            }),
+            Ok(Err(e)) => tracing::debug!(error = %e, "the source's detail can't be read"),
+            Err(e) => tracing::debug!(error = %e, "the source's detail read ended"),
+        }
+    }
+
     /// The schema a snapshot reads `table` under: a streamed table's, or a copy's.
     pub(super) fn read_schema(&self, table: &TableRef) -> Option<&TableSchema> {
         self.schemas.get(table).or_else(|| self.copies.get(table))
````

`crates/data-avalanche-pipeline/src/status.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
index 45d3050..6cc9751 100644
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -1,11 +1,12 @@
 //! What a running worker shows and takes: its status, the commands it obeys, why it ended, and
 //! the timings its retries use.
 
-use std::collections::BTreeMap;
+use std::collections::{BTreeMap, BTreeSet, VecDeque};
 use std::time::Duration;
 
+use chrono::{DateTime, Utc};
 use data_avalanche_config::Pipeline;
-use data_avalanche_core::{HaltReason, Op, SourcePosition, TableRef};
+use data_avalanche_core::{CommitCost, HaltReason, Op, SourceDetail, SourcePosition, TableRef};
 use tokio::sync::watch;
 
 use crate::Backoff;
@@ -147,6 +148,78 @@ pub struct WorkerStatus {
     /// written NULL (`data_avalanche_sink_soft_dropped_columns`). Counted at the start and after
     /// each evolve.
     pub soft_dropped: BTreeMap<TableRef, usize>,
+    /// The tables the last Phase 0 tracks: the pipeline's page lists them.
+    pub tracked: BTreeSet<TableRef>,
+    /// Per table, what this worker committed to it since it started: the pipeline's page.
+    pub tables: BTreeMap<TableRef, TableFigures>,
+    /// What the source keeps for the pipeline, as of `source_read_at`: read every heartbeat. A
+    /// failed read keeps the last one.
+    pub source: Option<SourceDetail>,
+    pub source_read_at: Option<DateTime<Utc>>,
+}
+
+/// One commit the writer made to a table.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct TableCommit {
+    pub table: TableRef,
+    /// The rows it wrote, upserts and deletes.
+    pub rows: u64,
+    /// A snapshot chunk's rows, not the stream's changes.
+    pub snapshot: bool,
+    pub version: u64,
+    pub cost: CommitCost,
+    pub at: DateTime<Utc>,
+}
+
+/// The window [`TableFigures::changes_per_minute`] counts over.
+pub const CHANGES_WINDOW: Duration = Duration::from_secs(60);
+
+/// A table's figures since the worker started, from its commits.
+#[derive(Debug, Clone, PartialEq, Eq, Default)]
+pub struct TableFigures {
+    /// The stream's changes committed to it; a snapshot's rows don't count.
+    pub changes: u64,
+    /// The commits of the last [`CHANGES_WINDOW`] that carried changes: when, and how many.
+    recent: VecDeque<(DateTime<Utc>, u64)>,
+    pub last_commit: Option<DateTime<Utc>>,
+    /// The table's version after the last commit.
+    pub version: Option<u64>,
+    /// The cost of the last commit that wrote rows: a MERGE's files rewritten, and the table's
+    /// files after it.
+    pub cost: Option<CommitCost>,
+}
+
+impl TableFigures {
+    fn add(&mut self, c: &TableCommit) {
+        self.last_commit = Some(c.at);
+        self.version = Some(c.version);
+        if c.rows > 0 {
+            self.cost = Some(c.cost);
+        }
+        if !c.snapshot && c.rows > 0 {
+            self.changes += c.rows;
+            self.recent.push_back((c.at, c.rows));
+        }
+        let window = chrono::Duration::from_std(CHANGES_WINDOW).expect("a minute");
+        while self
+            .recent
+            .front()
+            .is_some_and(|(at, _)| *at <= c.at - window)
+        {
+            self.recent.pop_front();
+        }
+    }
+
+    /// The changes committed in the [`CHANGES_WINDOW`] before `now`.
+    #[must_use]
+    pub fn changes_per_minute(&self, now: DateTime<Utc>) -> u64 {
+        let window = chrono::Duration::from_std(CHANGES_WINDOW).expect("a minute");
+        self.recent
+            .iter()
+            .filter(|(at, _)| *at > now - window)
+            .map(|(_, rows)| rows)
+            .sum()
+    }
 }
 
 /// The warnings a status keeps.
@@ -161,6 +234,11 @@ impl WorkerStatus {
         self.unsynced_since_micros = None;
     }
 
+    /// A commit the writer made: the table's figures take it.
+    pub fn committed(&mut self, c: &TableCommit) {
+        self.tables.entry(c.table.clone()).or_default().add(c);
+    }
+
     /// Adds a warning as the latest. One the status already shows moves to the end instead: a
     /// rerun of Phase 0, a reconnect or a new failing streak that finds the same thing again adds
     /// nothing. Past [`MAX_WARNINGS`] the oldest goes.
````

`crates/data-avalanche-pipeline/src/worker.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index b5a7032..e667386 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -1297,11 +1297,14 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         doc.save(&core.docs).await?;
     }
     core.server = server;
+    core.update(|st| {
+        st.tracked.clone_from(&tracked);
+        warnings.into_iter().for_each(|w| st.warn(w));
+    });
     core.tracked = tracked;
     core.copies = copies;
     core.drifted = drifted;
     core.dump_only = dump_only;
-    core.update(|st| warnings.into_iter().for_each(|w| st.warn(w)));
     Ok(())
 }
 
````

`crates/data-avalanche-pipeline/src/writer.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index ad43c91..cc3d190 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -25,7 +25,7 @@ use tracing::field::Empty;
 
 use crate::backoff::Backoff;
 use crate::checkpoint::{Doc, Store};
-use crate::{CommitGuard, HookAction, HookPoint, Hooks, Observer, WorkerError};
+use crate::{CommitGuard, HookAction, HookPoint, Hooks, Observer, TableCommit, WorkerError};
 
 /// One flush and what becomes durable with it.
 #[derive(Debug)]
@@ -56,6 +56,8 @@ pub(crate) enum WriterEvent {
     /// job, in job order, a job with refused change sets included: the session counts them to
     /// know which jobs are durable (consecutive jobs may share a checkpoint).
     Durable(SourcePosition),
+    /// A change set is committed: the table's figures take it.
+    Committed(TableCommit),
     /// The writer stopped; no further event follows.
     Failed(WorkerError),
 }
@@ -248,7 +250,15 @@ impl Writer {
                 observer.commit_failed();
             }
             match outcome {
-                Ok(CommitOutcome::Committed { .. }) => {
+                Ok(CommitOutcome::Committed { version, cost }) => {
+                    let _ = self.events.send(WriterEvent::Committed(TableCommit {
+                        table: cs.table.clone(),
+                        rows,
+                        snapshot: cs.snapshot,
+                        version,
+                        cost,
+                        at: chrono::Utc::now(),
+                    }));
                     observer.committed(rows, at.elapsed());
                     span.record("outcome", "committed");
                     self.degraded(None);
````

`crates/data-avalanche-testkit/src/fake_source.rs`:

````diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index cfc12a1..94a3ff0 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -36,8 +36,8 @@ use data_avalanche_core::{
     CdcEvent, ChangeStream, Checkpoint, Chunk, ChunkVisibility, ColumnSchema, ColumnValue,
     HaltReason, InFlightTxn, KeysetCursor, Op, Preflight, Prepared, RescanReport, ResolvedTables,
     RetainedLog, Row, SchemaChange, SchemaDelta, SchemaHint, ServerIdentity, SnapshotReason,
-    SourceConnector, SourceError, SourcePosition, SourceSeq, StartPlan, StreamItem, TableRef,
-    TableSchema, TransactionBoundary, XidSnapshot, identifier_values,
+    SourceConnector, SourceDetail, SourceError, SourcePosition, SourceSeq, StartPlan, StreamItem,
+    TableRef, TableSchema, TransactionBoundary, XidSnapshot, identifier_values,
 };
 use tokio::sync::Notify;
 
@@ -240,6 +240,8 @@ struct Db {
     retained: Option<Option<u64>>,
     /// [`FakeSource::set_behind`].
     behind: Option<Duration>,
+    /// What `detail` reports (by default, nothing).
+    detail: Option<SourceDetail>,
     /// [`FakeSource::detach_stream`].
     detached: bool,
     /// [`FakeSource::stall_retained`].
@@ -320,6 +322,7 @@ impl FakeSource {
             added: vec![],
             retained: None,
             behind: None,
+            detail: None,
             detached: false,
             retained_stalled: false,
             faults: vec![],
@@ -543,6 +546,11 @@ impl FakeSource {
         self.db().behind = behind;
     }
 
+    /// What `detail` reports from now on (by default, nothing).
+    pub fn set_detail(&self, detail: Option<SourceDetail>) {
+        self.db().detail = detail;
+    }
+
     /// `retained` reports that no stream reads the slot, whatever the open streams do: a stream
     /// the source dropped without the client hearing of it (DA-SRC-16).
     pub fn detach_stream(&self) {
@@ -1210,6 +1218,10 @@ impl SourceConnector for FakeSource {
         })
     }
 
+    async fn detail(&self) -> Result<Option<SourceDetail>, SourceError> {
+        Ok(self.db().detail.clone())
+    }
+
     async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
         self.take_fault(Method::Retained)?;
         if self.db().retained_stalled {
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-pipeline --lib`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline/src/lib.rs crates/data-avalanche-pipeline/src/session.rs crates/data-avalanche-pipeline/src/status.rs crates/data-avalanche-pipeline/src/worker.rs crates/data-avalanche-pipeline/src/writer.rs crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "feat(pipeline): each table's figures from its commits, and the source's detail every heartbeat"
```

---

### Task 5: `GET /v1/pipelines/{id}/live` (`data-avalanche-api`, `-cluster`, `data-avalanche`)

**Files:**
- Create: `crates/data-avalanche-api/src/live.rs`
- Modify: `crates/data-avalanche-api/Cargo.toml`, `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-api/src/nodes.rs`, `crates/data-avalanche-api/src/snapshots.rs`, `crates/data-avalanche-api/src/test_support.rs`, `crates/data-avalanche-cluster/src/reconciler.rs`, `crates/data-avalanche/src/run.rs`
- Generated: `crates/data-avalanche-api/openapi.json`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`

**Interfaces:**
- Consumes: Task 1's `since`, Task 4's `WorkerStatus` figures; `ClusterReconciler::statuses`
  (M8); `Peers::send`, `DETAIL_LIMIT`, `FORWARDED_BY` (M13b); `find`, `unavailable` (M9a).
- Produces: `ClusterReconciler::watch_statuses` (public); `Node::statuses: watch::Receiver<
  BTreeMap<String, watch::Receiver<ControllerStatus>>>`; API crate root `FRESH_WITHIN` (60 s);
  `GET /v1/pipelines/{id}/live` (viewer); `snapshots::table_name` (crate). The API crate takes
  `data-avalanche-pipeline` as a dependency (it was a dev-dependency). The node tests' fake peer
  moves to `test_support` (`Seen`, `Log`, `fake`, `fake_answering`, `member`, `served`,
  `served_with`).

- [ ] **Step 1: Write the failing tests**

The tests: `the_node_that_runs_the_worker_answers_from_its_memory`, `nothing_is_stale_while_paused`, `another_node_asks_the_lease_holder`, `without_a_holder_to_ask_the_answer_says_why` (live); the route table.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index b7eef84..70492dc 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -894,6 +926,7 @@ mod tests {
                 ("GET", "/v1/cluster/status", Viewer),
                 ("GET", "/v1/pipelines", Viewer),
                 ("GET", "/v1/pipelines/{id}", Viewer),
+                ("GET", "/v1/pipelines/{id}/live", Viewer),
                 ("POST", "/v1/pipelines/{id}/pause", Operator),
                 ("POST", "/v1/pipelines/{id}/stop", Operator),
                 ("POST", "/v1/pipelines/{id}/resume", Operator),
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api`
Expected: the build fails: no field `statuses` in `Node`, no module `live`.

- [ ] **Step 3: Implement**

The OpenAPI document and the site's reference are generated, not written: after the diffs
below, run `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi` (it rewrites
`crates/data-avalanche-api/openapi.json`), then `cargo xtask docs` (it rewrites
`docs/public/openapi.json` and `docs/src/content/docs/reference/api.md`), and read their diffs:
only the routes this task adds or changes move. `Cargo.lock` follows the manifests on the first
build.

`crates/data-avalanche-api/Cargo.toml`:

````diff
diff --git a/crates/data-avalanche-api/Cargo.toml b/crates/data-avalanche-api/Cargo.toml
index e0830cd..b1147d3 100644
--- a/crates/data-avalanche-api/Cargo.toml
+++ b/crates/data-avalanche-api/Cargo.toml
@@ -17,6 +17,7 @@ chrono.workspace = true
 data-avalanche-cluster.workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
+data-avalanche-pipeline.workspace = true
 data-avalanche-state.workspace = true
 data-avalanche-telemetry.workspace = true
 futures-util.workspace = true
@@ -41,7 +42,6 @@ async-trait.workspace = true
 bytes.workspace = true
 axum = { workspace = true, features = ["json"] }
 boon.workspace = true
-data-avalanche-pipeline.workspace = true
 data-avalanche-storage.workspace = true
 object_store.workspace = true
 rcgen.workspace = true
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index b7eef84..70492dc 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -15,6 +15,7 @@ mod config;
 mod events;
 mod generations;
 mod limit;
+mod live;
 mod nodes;
 mod openapi;
 mod peers;
@@ -24,6 +25,7 @@ mod snapshots;
 #[cfg(test)]
 mod test_support;
 
+use std::collections::BTreeMap;
 use std::path::PathBuf;
 use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::{Arc, OnceLock};
@@ -34,7 +36,9 @@ use axum::http::{HeaderValue, StatusCode, header};
 use axum::middleware::{Next, from_fn_with_state};
 use axum::response::{IntoResponse, Response};
 use axum::routing::{MethodRouter, get, post};
-use data_avalanche_cluster::{ConfigCoordinator, DocsFn, GossipFigures, Members, Recorder};
+use data_avalanche_cluster::{
+    ConfigCoordinator, ControllerStatus, DocsFn, GossipFigures, Members, Recorder,
+};
 use data_avalanche_config::Role;
 use data_avalanche_state::{Events, Leases};
 use data_avalanche_telemetry::{CONTENT_TYPE, Metrics, ProcessFigures};
@@ -45,6 +49,7 @@ pub use actions::{DENIALS_BURST, MAX_ERROR, MAX_NOTE, MAX_TARGET, Recorded, note
 pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER};
 pub use cluster::{Control, Stop};
 pub use limit::{RateLimit, VALIDATE_BURST, VALIDATE_PER_SECOND};
+pub use live::FRESH_WITHIN;
 pub use openapi::openapi;
 pub use peers::{
     ACTION_LIMIT, ANSWERED_BY, DETAIL_LIMIT, FORWARDED_BY, MAX_ANSWER, Peers, PeersError,
@@ -95,6 +100,9 @@ pub struct Node {
     pub gossip: watch::Receiver<GossipFigures>,
     /// The ended snapshot jobs `GET /v1/snapshots` read: they don't change.
     pub ended_jobs: Arc<EndedJobs>,
+    /// The status of each pipeline this node's controllers run: the lease holder answers a
+    /// pipeline's live figures from it (Admin_Console §4).
+    pub statuses: watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
 }
 
 impl std::fmt::Debug for Node {
@@ -335,6 +343,30 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             get(pipelines::get),
         ),
+        (
+            r(
+                "GET",
+                "/v1/pipelines/{id}/live",
+                Viewer,
+                "What the pipeline's worker shows now, from the node that runs it: its phase, lag, source detail, and each table's state, figures and freshness; another node asks the lease holder",
+                &[
+                    (200, "the worker's figures"),
+                    UNKNOWN,
+                    (409, "no node holds the lease"),
+                    (
+                        421,
+                        "a forwarded request to a node that doesn't run the worker",
+                    ),
+                    (502, "the holder didn't answer"),
+                    (
+                        503,
+                        "the holder can't be asked: not a member, or no API address",
+                    ),
+                    (504, "the holder didn't answer in time"),
+                ],
+            ),
+            get(live::get),
+        ),
         (
             act(
                 "pipeline.pause",
````

`crates/data-avalanche-api/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/live.rs b/crates/data-avalanche-api/src/live.rs
new file mode 100644
index 0000000..25b3665
--- /dev/null
+++ b/crates/data-avalanche-api/src/live.rs
@@ -0,0 +1,489 @@
+//! `GET /v1/pipelines/{id}/live` (Admin_Console §4, D-M13d.6): what the pipeline's worker shows
+//! now, from the memory of the node that runs it. Any node answers: one that doesn't run the
+//! worker asks the lease holder, with the caller's own credentials.
+
+use std::collections::BTreeSet;
+use std::time::Duration;
+
+use axum::body::Bytes;
+use axum::extract::{Path, State};
+use axum::http::{HeaderMap, Method, StatusCode};
+use axum::response::Response;
+use chrono::{DateTime, Utc};
+use data_avalanche_cluster::{Actual, ControllerStatus};
+use data_avalanche_config::Pipeline;
+use data_avalanche_core::TableRef;
+use data_avalanche_pipeline::{Phase, WorkerStatus};
+use data_avalanche_state::{HaltsDoc, PipelineDocs};
+use serde_json::{Value, json};
+
+use crate::peers::{Asked, DETAIL_LIMIT, FORWARDED_BY};
+use crate::pipelines::{find, unavailable};
+use crate::snapshots::table_name;
+use crate::{Api, Node, body, starting};
+
+/// A table is stale when what lake readers see of it is older than this while its pipeline runs:
+/// the replication-lag target (System-Overview Scope, p99 ≤ 60 s).
+pub const FRESH_WITHIN: Duration = Duration::from_secs(60);
+
+fn phase_name(p: &Phase) -> &'static str {
+    match p {
+        Phase::Starting => "starting",
+        Phase::Streaming => "streaming",
+        Phase::Reconnecting { .. } => "reconnecting",
+        Phase::Draining => "draining",
+        Phase::Paused => "paused",
+    }
+}
+
+/// The controller's state, and its error.
+fn actual_of(a: &Actual) -> (&'static str, Option<&str>) {
+    match a {
+        Actual::Standby { .. } => ("standby", None),
+        Actual::Starting => ("starting", None),
+        Actual::Running => ("running", None),
+        Actual::Paused => ("paused", None),
+        Actual::Stopped => ("stopped", None),
+        Actual::Dumped => ("dumped", None),
+        Actual::Retrying { error } => ("retrying", Some(error)),
+        Actual::Error { error, .. } => ("error", Some(error)),
+    }
+}
+
+/// The pipeline's controller on this node, when it runs the pipeline: it holds the lease.
+fn running_here(node: &Node, id: &str) -> Option<ControllerStatus> {
+    let status = node.statuses.borrow().get(id)?.borrow().clone();
+    match status.actual {
+        None | Some(Actual::Standby { .. }) => None,
+        Some(_) => Some(status),
+    }
+}
+
+/// What lake readers see of the pipeline's tables as of now: every change read before the oldest
+/// one not yet durable (`replication_lag_seconds`, System-Overview), all of them when none waits.
+fn fresh_as_of(w: &WorkerStatus, now: DateTime<Utc>) -> DateTime<Utc> {
+    w.unsynced_since_micros
+        .and_then(DateTime::from_timestamp_micros)
+        .map_or(now, |at| at.min(now))
+}
+
+/// Each table the worker tracks, halts, copies or has committed to: its state, its figures, and
+/// how fresh it is. A halted table is as fresh as its halt (`halts`' times; none when unread). A
+/// streaming table is stale when that is older than [`FRESH_WITHIN`] while the worker streams.
+fn tables(
+    p: &Pipeline,
+    s: &ControllerStatus,
+    w: &WorkerStatus,
+    halts: Option<&HaltsDoc>,
+    now: DateTime<Utc>,
+) -> Vec<Value> {
+    let running = matches!(s.actual, Some(Actual::Running)) && w.phase == Phase::Streaming;
+    let fresh = fresh_as_of(w, now);
+    let window = chrono::Duration::from_std(FRESH_WITHIN).expect("a minute");
+    let all: BTreeSet<&TableRef> = w
+        .tracked
+        .iter()
+        .chain(w.halted.keys())
+        .chain(w.snapshots.keys())
+        .chain(w.tables.keys())
+        .collect();
+    all.into_iter()
+        .map(|t| {
+            let halt = w.halted.get(t);
+            let since = halts.and_then(|h| h.since(t));
+            let snapshot = w.snapshots.get(t);
+            let state = match (halt, snapshot) {
+                (Some(_), _) => "halted",
+                (None, Some(_)) => "snapshot",
+                (None, None) => "streaming",
+            };
+            let fresh = if halt.is_some() { since } else { Some(fresh) };
+            let figures = w.tables.get(t);
+            json!({
+                "table": table_name(p, t),
+                "state": state,
+                "halt": halt.map(|r| json!({
+                    "code": r.code(),
+                    "detail": r.to_string(),
+                    "since": since,
+                })),
+                "snapshot": snapshot.map(|s| json!({
+                    "rows_done": s.rows_done,
+                    "waiting": s.waiting,
+                })),
+                "changes": figures.map_or(0, |f| f.changes),
+                "changes_per_minute": figures.map_or(0, |f| f.changes_per_minute(now)),
+                "last_commit": figures.and_then(|f| f.last_commit),
+                "version": figures.and_then(|f| f.version),
+                "files_rewritten": figures.and_then(|f| f.cost).map(|c| c.files_rewritten),
+                "files": figures.and_then(|f| f.cost).and_then(|c| c.files),
+                "soft_dropped": w.soft_dropped.get(t).copied().unwrap_or(0),
+                "fresh_as_of": fresh,
+                // A copy's readers see what its job says (`lake`), not the stream's freshness.
+                "stale": state == "streaming" && running && fresh.is_some_and(|f| now - f > window),
+            })
+        })
+        .collect()
+}
+
+/// The answer of the node that runs the pipeline.
+fn answer(
+    me: &str,
+    p: &Pipeline,
+    s: &ControllerStatus,
+    halts: Option<&HaltsDoc>,
+    now: DateTime<Utc>,
+) -> Value {
+    let (actual, error) = s.actual.as_ref().map_or(("starting", None), actual_of);
+    let w = s.worker.as_ref();
+    let worker = w.map(|w| {
+        let fresh = fresh_as_of(w, now);
+        json!({
+            "phase": phase_name(&w.phase),
+            "reconnecting": match &w.phase {
+                Phase::Reconnecting { error } => Some(error.as_str()),
+                _ => None,
+            },
+            "read": w.read.as_ref().map(ToString::to_string),
+            "checkpoint": w.checkpoint.as_ref().map(ToString::to_string),
+            "lag_seconds": (now - fresh).as_seconds_f64(),
+            "fresh_as_of": fresh,
+            "degraded": w.degraded,
+            "reconnects": w.reconnects,
+            "rows_read": {
+                "insert": w.rows_read.insert,
+                "update": w.rows_read.update,
+                "delete": w.rows_read.delete,
+                "snapshot": w.rows_read.snapshot,
+            },
+            "buffer_bytes": w.buffer_bytes,
+            "warnings": w.warnings,
+        })
+    });
+    let source = w.map(|w| {
+        json!({
+            "detail": w.source,
+            "read_at": w.source_read_at,
+            "retained_bytes": w.retained_bytes,
+            "behind_seconds": w.source_behind.map(|d| d.as_secs_f64()),
+        })
+    });
+    json!({
+        "id": p.id,
+        "node": me,
+        "as_of": now,
+        "actual": actual,
+        "error": error,
+        "owner_epoch": s.owner_epoch,
+        "worker": worker,
+        "source": source,
+        "tables": w.map(|w| tables(p, s, w, halts, now)).unwrap_or_default(),
+    })
+}
+
+/// This node's answer when it runs the pipeline. A failed read of the halts leaves the halts'
+/// times out, not the answer.
+async fn here(node: &Node, p: &Pipeline, docs: &PipelineDocs) -> Option<Value> {
+    let s = running_here(node, &p.id)?;
+    let halts = match docs.halts().await {
+        Ok(h) => Some(h),
+        Err(e) => {
+            tracing::debug!("{}'s live figures, its halts: {e}", p.id);
+            None
+        }
+    };
+    let me = node.members.borrow().me.clone();
+    Some(answer(&me, p, &s, halts.as_ref(), Utc::now()))
+}
+
+/// The lease holder's answer to the caller's request, sent on: `409` when no node holds the
+/// lease, `503` naming a holder this node can't ask, and never a request that was forwarded
+/// already (`421`: the lease moved).
+async fn from_holder(node: &Node, id: &str, headers: &HeaderMap) -> Response {
+    let me = node.members.borrow().me.clone();
+    if headers.contains_key(FORWARDED_BY) {
+        return body(
+            StatusCode::MISDIRECTED_REQUEST,
+            &json!({ "error": format!("{me} doesn't run {id}'s worker: its lease moved") }),
+        );
+    }
+    let lease = match node.leases.observe(id).await {
+        Ok(l) => l.map(|v| v.doc).filter(|l| !l.released),
+        Err(e) => return unavailable(&e),
+    };
+    let Some(lease) = lease else {
+        return body(
+            StatusCode::CONFLICT,
+            &json!({ "error": format!("no node holds {id}'s lease: no worker runs it") }),
+        );
+    };
+    let holder = lease.holder_node_id;
+    let unreachable = |why: String| {
+        body(
+            StatusCode::SERVICE_UNAVAILABLE,
+            &json!({ "error": why, "node": holder }),
+        )
+    };
+    if holder == me {
+        return unreachable(format!(
+            "{me} holds {id}'s lease and doesn't run its worker yet"
+        ));
+    }
+    let member = node.members.borrow().nodes.get(&holder).cloned();
+    match member {
+        None => unreachable(format!(
+            "{holder} holds {id}'s lease and isn't a member this node sees"
+        )),
+        Some(m) if m.meta.api_url.is_empty() => unreachable(format!(
+            "{holder} holds {id}'s lease and gossips no API address"
+        )),
+        Some(m) => {
+            let asked = Asked::from(Method::GET, headers, Bytes::new());
+            let path = format!("/v1/pipelines/{id}/live");
+            node.peers
+                .send(&holder, &m.meta.api_url, &path, asked, DETAIL_LIMIT)
+                .await
+        }
+    }
+}
+
+/// `GET /v1/pipelines/{id}/live`.
+pub(crate) async fn get(
+    State(api): State<Api>,
+    Path(id): Path<String>,
+    headers: HeaderMap,
+) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (p, docs) = match find(node, &id).await {
+        Ok(found) => found,
+        Err(f) => return f.into(),
+    };
+    match here(node, &p, &docs).await {
+        Some(v) => body(StatusCode::OK, &v),
+        None => from_holder(node, &id, &headers).await,
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use std::collections::BTreeMap;
+
+    use data_avalanche_core::position::Lsn;
+    use data_avalanche_core::{
+        CommitCost, HaltReason, PgDetail, SourceDetail, SourcePosition, TableRef,
+    };
+    use data_avalanche_pipeline::{SnapshotProgress, TableCommit, WorkerStatus};
+    use data_avalanche_state::{Holder, TableHalt};
+    use tokio::sync::watch;
+
+    use super::*;
+    use crate::peers::{ANSWERED_BY, TARGET};
+    use crate::test_support::{fake_answering, open, request, served, served_with};
+
+    fn t(name: &str) -> TableRef {
+        TableRef::new("public", name)
+    }
+
+    /// A worker that streams `orders`, committed 5 changes to it, has `items` halted and copies
+    /// `audit`; its oldest change waiting is `lag` old.
+    fn worker(lag: chrono::Duration) -> WorkerStatus {
+        let now = Utc::now();
+        let mut w = WorkerStatus {
+            phase: Phase::Streaming,
+            unsynced_since_micros: Some((now - lag).timestamp_micros()),
+            tracked: [t("orders"), t("items")].into(),
+            halted: [(t("items"), HaltReason::TableDropped)].into(),
+            snapshots: [(
+                t("audit"),
+                SnapshotProgress {
+                    rows_done: 40,
+                    waiting: false,
+                },
+            )]
+            .into(),
+            source: Some(SourceDetail::Postgres(PgDetail {
+                head: SourcePosition::Pg(Lsn(0x30)),
+                slot: None,
+                max_slot_wal_keep_size: None,
+                publication: None,
+            })),
+            source_read_at: Some(now),
+            retained_bytes: Some(4096),
+            ..WorkerStatus::default()
+        };
+        w.committed(&TableCommit {
+            table: t("orders"),
+            rows: 5,
+            snapshot: false,
+            version: 18,
+            cost: CommitCost {
+                files_rewritten: 6,
+                files: Some(820),
+            },
+            at: now,
+        });
+        w
+    }
+
+    fn statuses(
+        actual: Actual,
+        worker: Option<WorkerStatus>,
+    ) -> watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>> {
+        let status = ControllerStatus {
+            actual: Some(actual),
+            owner_epoch: Some(7),
+            lease: None,
+            worker,
+        };
+        watch::channel(BTreeMap::from([(
+            "orders".to_owned(),
+            watch::channel(status).1,
+        )]))
+        .1
+    }
+
+    fn json(text: &str) -> Value {
+        serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
+    }
+
+    fn by_table<'v>(v: &'v Value, name: &str) -> &'v Value {
+        v["tables"]
+            .as_array()
+            .unwrap()
+            .iter()
+            .find(|t| t["table"] == name)
+            .unwrap_or_else(|| panic!("{name} in {v}"))
+    }
+
+    /// The node that runs the worker answers from its memory: each table's state, figures and
+    /// freshness; a halted table is as fresh as its halt, and isn't stale.
+    #[tokio::test]
+    async fn the_node_that_runs_the_worker_answers_from_its_memory() {
+        let lagging = statuses(Actual::Running, Some(worker(chrono::Duration::seconds(90))));
+        let (addr, lone, _dir) =
+            served_with("http://127.0.0.1:1", open(), |n| n.statuses = lagging).await;
+        let p = crate::test_support::config().pipelines["orders"].clone();
+        let docs = (lone.node.docs)(&p).unwrap();
+        docs.record_halt(&TableHalt {
+            table: t("items"),
+            reason: HaltReason::TableDropped,
+            at: SourcePosition::Pg(Lsn(0x20)),
+            copy: false,
+        })
+        .await
+        .unwrap();
+        let since = docs.halts().await.unwrap().since(&t("items")).unwrap();
+        let (code, _, text) = request(addr, "GET", "/v1/pipelines/orders/live", &[], "").await;
+        assert_eq!(code, 200, "{text}");
+        let v = json(&text);
+        assert_eq!(
+            (&v["node"], &v["actual"], &v["owner_epoch"]),
+            (&json!("node-a"), &json!("running"), &json!(7))
+        );
+        assert_eq!(v["worker"]["phase"], "streaming");
+        assert!(v["worker"]["lag_seconds"].as_f64().unwrap() >= 90.0, "{v}");
+        assert_eq!(v["source"]["detail"]["engine"], "postgres");
+        assert_eq!(v["source"]["retained_bytes"], 4096);
+        let orders = by_table(&v, "public.orders");
+        assert_eq!(orders["state"], "streaming");
+        assert_eq!(
+            (&orders["changes"], &orders["changes_per_minute"]),
+            (&json!(5), &json!(5))
+        );
+        assert_eq!(
+            (
+                &orders["version"],
+                &orders["files_rewritten"],
+                &orders["files"]
+            ),
+            (&json!(18), &json!(6), &json!(820))
+        );
+        assert_eq!(orders["stale"], true, "90 s behind, past {FRESH_WITHIN:?}");
+        let items = by_table(&v, "public.items");
+        assert_eq!(
+            (&items["state"], &items["halt"]["code"]),
+            (&json!("halted"), &json!("TABLE_DROPPED"))
+        );
+        assert_eq!(items["fresh_as_of"], json!(since));
+        assert_eq!(items["halt"]["since"], json!(since));
+        assert_eq!(items["stale"], false);
+        let audit = by_table(&v, "public.audit");
+        assert_eq!(
+            (&audit["state"], &audit["snapshot"]["rows_done"]),
+            (&json!("snapshot"), &json!(40))
+        );
+        assert_eq!(
+            audit["stale"], false,
+            "a copy's readers see what its job says"
+        );
+    }
+
+    /// A paused pipeline is behind by design: nothing is stale.
+    #[tokio::test]
+    async fn nothing_is_stale_while_paused() {
+        let mut w = worker(chrono::Duration::seconds(300));
+        w.phase = Phase::Paused;
+        let paused = statuses(Actual::Paused, Some(w));
+        let (addr, _lone, _dir) =
+            served_with("http://127.0.0.1:1", open(), |n| n.statuses = paused).await;
+        let (code, _, text) = request(addr, "GET", "/v1/pipelines/orders/live", &[], "").await;
+        assert_eq!(code, 200, "{text}");
+        assert_eq!(by_table(&json(&text), "public.orders")["stale"], false);
+    }
+
+    async fn hold(lone: &crate::test_support::Lone, holder: &str) {
+        let holder = Holder {
+            node_id: holder.into(),
+            incarnation: 1_731_600_000_000,
+        };
+        lone.leases
+            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
+            .await
+            .unwrap();
+    }
+
+    /// Another node asks the lease holder with the caller's credentials, and passes its answer
+    /// on; a forwarded request isn't forwarded again.
+    #[tokio::test]
+    async fn another_node_asks_the_lease_holder() {
+        let (b, log) =
+            fake_answering(Duration::ZERO, StatusCode::OK, json!({ "id": "orders" })).await;
+        let (addr, lone, _dir) = served(&b, open()).await;
+        hold(&lone, "node-b").await;
+        let auth = [("Authorization", "Bearer the-callers-token")];
+        let (code, head, text) = request(addr, "GET", "/v1/pipelines/orders/live", &auth, "").await;
+        assert_eq!((code, json(&text)), (200, json!({ "id": "orders" })));
+        assert!(
+            head.to_ascii_lowercase()
+                .contains(&format!("{ANSWERED_BY}: node-b")),
+            "{head}"
+        );
+        let seen = log.lock().unwrap()[0].clone();
+        assert_eq!(seen.uri, "/v1/pipelines/orders/live");
+        assert_eq!(
+            seen.authorization.as_deref(),
+            Some("Bearer the-callers-token")
+        );
+        assert_eq!(seen.target.as_deref(), Some("node-b"));
+        let again = [(FORWARDED_BY, "node-c"), (TARGET, "node-a")];
+        let (code, _, _) = request(addr, "GET", "/v1/pipelines/orders/live", &again, "").await;
+        assert_eq!(code, 421, "the lease moved");
+        assert_eq!(log.lock().unwrap().len(), 1);
+    }
+
+    /// No holder: 409. A holder that isn't a member, or this node holding the lease without a
+    /// worker yet: 503, naming it.
+    #[tokio::test]
+    async fn without_a_holder_to_ask_the_answer_says_why() {
+        let (addr, lone, _dir) = served("http://127.0.0.1:1", open()).await;
+        let (code, _, text) = request(addr, "GET", "/v1/pipelines/orders/live", &[], "").await;
+        assert_eq!(code, 409, "{text}");
+        hold(&lone, "node-z").await;
+        let (code, _, text) = request(addr, "GET", "/v1/pipelines/orders/live", &[], "").await;
+        assert_eq!((code, &json(&text)["node"]), (503, &json!("node-z")));
+        let (code, _, _) = request(addr, "GET", "/v1/pipelines/nope/live", &[], "").await;
+        assert_eq!(code, 404);
+    }
+}
````

`crates/data-avalanche-api/src/nodes.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/nodes.rs b/crates/data-avalanche-api/src/nodes.rs
index e9ecae6..65dec61 100644
--- a/crates/data-avalanche-api/src/nodes.rs
+++ b/crates/data-avalanche-api/src/nodes.rs
@@ -428,114 +428,26 @@ pub(crate) async fn files(State(api): State<Api>, headers: HeaderMap) -> Respons
 
 #[cfg(test)]
 mod tests {
-    use std::net::SocketAddr;
-    use std::sync::{Arc, Mutex};
+    use std::sync::Arc;
     use std::time::{Duration, Instant};
 
     use axum::Router;
     use axum::http::Uri;
-    use data_avalanche_cluster::{Member, Members, NodeMeta, file_hash};
+    use data_avalanche_cluster::file_hash;
     use data_avalanche_config::{
         FingerprintKey, Origin, Pem, Secret, TlsIdentity, text_fingerprint,
     };
     use data_avalanche_state::{EventDoc, EventQuery, Events, config_hash};
-    use tempfile::TempDir;
     use tokio::sync::watch;
 
     use super::*;
     use crate::peers::{ANSWERED_BY, Peers, TARGET};
     use crate::test_support::{
-        CONFIG, Issuer, Lone, USERNAME_CLAIM, api, lone, lone_with, open, request, serve,
+        CONFIG, Issuer, Log, Seen, USERNAME_CLAIM, api, fake, lone_with, member, open, request,
+        serve, served,
     };
     use crate::{Auth, Stop};
 
-    /// What a fake node saw of one request.
-    #[derive(Debug, Clone)]
-    struct Seen {
-        method: Method,
-        uri: String,
-        authorization: Option<String>,
-        target: Option<String>,
-        forwarded_by: Option<String>,
-        body: String,
-    }
-
-    type Log = Arc<Mutex<Vec<Seen>>>;
-
-    /// A fake node: answers every request `202 {"node": "node-b"}` after `delay`, keeping what it
-    /// saw.
-    async fn fake(delay: Duration) -> (String, Log) {
-        let log: Log = Arc::default();
-        let kept = Arc::clone(&log);
-        let app = Router::new().fallback(
-            move |method: Method, uri: Uri, headers: HeaderMap, bytes: Bytes| {
-                let kept = Arc::clone(&kept);
-                async move {
-                    let h = |n: &str| headers.get(n).map(|v| v.to_str().unwrap().to_owned());
-                    kept.lock().unwrap().push(Seen {
-                        method,
-                        uri: uri.to_string(),
-                        authorization: h("authorization"),
-                        target: h(TARGET),
-                        forwarded_by: h(FORWARDED_BY),
-                        body: String::from_utf8(bytes.to_vec()).unwrap(),
-                    });
-                    tokio::time::sleep(delay).await;
-                    body(StatusCode::ACCEPTED, &json!({ "node": "node-b" }))
-                }
-            },
-        );
-        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
-        let url = format!("http://{}", listener.local_addr().unwrap());
-        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
-        (url, log)
-    }
-
-    fn member(api_url: &str, phase: Phase) -> Member {
-        Member {
-            addr: "127.0.0.1:7946".parse().unwrap(),
-            incarnation: 1_731_600_000_000,
-            meta: NodeMeta {
-                phase,
-                config_generation: 1,
-                build: "1.1.0".into(),
-                api_url: api_url.into(),
-                file_hash: file_hash(&FingerprintKey::from_bytes([1; 32]), CONFIG),
-            },
-            suspect: false,
-        }
-    }
-
-    /// node-a's API under `auth`; its view holds itself at its own address and node-b at
-    /// `b_url`.
-    async fn served(b_url: &str, auth: Arc<Auth>) -> (SocketAddr, Lone, TempDir) {
-        let dir = tempfile::tempdir().unwrap();
-        let file = dir.path().join("config.yaml");
-        std::fs::write(&file, CONFIG).unwrap();
-        let mut lone = lone(file).await;
-        let (view, members) = watch::channel(Members {
-            me: "node-a".into(),
-            nodes: Default::default(),
-        });
-        lone.node.members = members;
-        let a = api(auth);
-        a.node.set(lone.node.clone()).unwrap();
-        let addr = serve(a).await;
-        view.send_replace(Members {
-            me: "node-a".into(),
-            nodes: [
-                (
-                    "node-a".to_owned(),
-                    member(&format!("http://{addr}"), Phase::Active),
-                ),
-                ("node-b".to_owned(), member(b_url, Phase::Active)),
-            ]
-            .into(),
-        });
-        std::mem::forget(view);
-        (addr, lone, dir)
-    }
-
     fn json(text: &str) -> Value {
         serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
     }
````

`crates/data-avalanche-api/src/snapshots.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index a4be6e1..2ce01da 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -41,7 +41,7 @@ pub(crate) fn job_pipeline(job_id: &str) -> Option<&str> {
 }
 
 /// A table as the request and the answers name it: the source's per-table config key.
-fn table_name(p: &Pipeline, t: &TableRef) -> String {
+pub(crate) fn table_name(p: &Pipeline, t: &TableRef) -> String {
     match p.source.kind {
         SourceType::Postgres => t.to_string(),
         SourceType::Mysql => t.name.clone(),
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 70a989a..a69759e 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -8,16 +8,18 @@ use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
 use std::sync::{Arc, Mutex};
 use std::time::Duration;
 
-use axum::Json;
+use axum::body::Bytes;
 use axum::extract::State;
+use axum::http::{HeaderMap, Method, StatusCode, Uri};
 use axum::routing::get as get_route;
+use axum::{Json, Router};
 use base64::Engine;
 use base64::engine::general_purpose::URL_SAFE_NO_PAD;
 
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
-    Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Parts, Phase,
-    ReconcilerSettings, Recorder, Udp, WorkerFactory, removed_pipelines,
+    Gossip, GossipKey, GossipSettings, LeaseTiming, Member, Members, NodeIdentity, NodeMeta, Parts,
+    Phase, ReconcilerSettings, Recorder, Udp, WorkerFactory, file_hash, removed_pipelines,
 };
 use data_avalanche_config::{
     ApiAuth, Env, FingerprintKey, HostPort, Jwt, Pipeline, ProcessEnv, ResolvedConfig, load_str,
@@ -32,9 +34,11 @@ use jsonwebtoken::{Algorithm, EncodingKey, Header};
 use object_store::memory::InMemory;
 use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
 use serde_json::{Value, json};
+use tempfile::TempDir;
 use tokio::io::{AsyncReadExt, AsyncWriteExt};
-use tokio::sync::Semaphore;
+use tokio::sync::{Semaphore, watch};
 
+use crate::peers::{FORWARDED_BY, TARGET};
 use crate::{Api, Auth, Control, Node, Stop, bind, router};
 
 pub(crate) const CONFIG: &str = r#"
@@ -289,6 +293,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             process: tokio::sync::watch::channel(Default::default()).1,
             gossip: gossip.figures(),
             ended_jobs: Arc::default(),
+            statuses: reconciler.watch_statuses(),
         },
         events,
         leases,
@@ -451,3 +456,109 @@ pub(crate) fn sign_with(key: &KeyPair, kid: Option<&str>, claims: &Value) -> Str
     )
     .unwrap()
 }
+
+/// What a fake node saw of one request.
+#[derive(Debug, Clone)]
+pub(crate) struct Seen {
+    pub method: Method,
+    pub uri: String,
+    pub authorization: Option<String>,
+    pub target: Option<String>,
+    pub forwarded_by: Option<String>,
+    pub body: String,
+}
+
+pub(crate) type Log = Arc<Mutex<Vec<Seen>>>;
+
+/// A fake node: answers every request `202 {"node": "node-b"}` after `delay`, keeping what it
+/// saw.
+pub(crate) async fn fake(delay: Duration) -> (String, Log) {
+    fake_answering(delay, StatusCode::ACCEPTED, json!({ "node": "node-b" })).await
+}
+
+/// A fake node: answers every request `code` with `answer` after `delay`, keeping what it saw.
+pub(crate) async fn fake_answering(
+    delay: Duration,
+    code: StatusCode,
+    answer: Value,
+) -> (String, Log) {
+    let log: Log = Arc::default();
+    let kept = Arc::clone(&log);
+    let app = Router::new().fallback(
+        move |method: Method, uri: Uri, headers: HeaderMap, bytes: Bytes| {
+            let (kept, answer) = (Arc::clone(&kept), answer.clone());
+            async move {
+                let h = |n: &str| headers.get(n).map(|v| v.to_str().unwrap().to_owned());
+                kept.lock().unwrap().push(Seen {
+                    method,
+                    uri: uri.to_string(),
+                    authorization: h("authorization"),
+                    target: h(TARGET),
+                    forwarded_by: h(FORWARDED_BY),
+                    body: String::from_utf8(bytes.to_vec()).unwrap(),
+                });
+                tokio::time::sleep(delay).await;
+                crate::body(code, &answer)
+            }
+        },
+    );
+    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+    let url = format!("http://{}", listener.local_addr().unwrap());
+    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
+    (url, log)
+}
+
+pub(crate) fn member(api_url: &str, phase: Phase) -> Member {
+    Member {
+        addr: "127.0.0.1:7946".parse().unwrap(),
+        incarnation: 1_731_600_000_000,
+        meta: NodeMeta {
+            phase,
+            config_generation: 1,
+            build: "1.1.0".into(),
+            api_url: api_url.into(),
+            file_hash: file_hash(&FingerprintKey::from_bytes([1; 32]), CONFIG),
+        },
+        suspect: false,
+    }
+}
+
+/// node-a's API under `auth`; its view holds itself at its own address and node-b at
+/// `b_url`.
+pub(crate) async fn served(b_url: &str, auth: Arc<Auth>) -> (SocketAddr, Lone, TempDir) {
+    served_with(b_url, auth, |_| {}).await
+}
+
+/// [`served`], its node changed by `change` before it serves.
+pub(crate) async fn served_with(
+    b_url: &str,
+    auth: Arc<Auth>,
+    change: impl FnOnce(&mut Node),
+) -> (SocketAddr, Lone, TempDir) {
+    let dir = tempfile::tempdir().unwrap();
+    let file = dir.path().join("config.yaml");
+    std::fs::write(&file, CONFIG).unwrap();
+    let mut lone = lone(file).await;
+    let (view, members) = watch::channel(Members {
+        me: "node-a".into(),
+        nodes: Default::default(),
+    });
+    lone.node.members = members;
+    change(&mut lone.node);
+    let a = api(auth);
+    a.node.set(lone.node.clone()).unwrap();
+    let addr = serve(a).await;
+    view.send_replace(Members {
+        me: "node-a".into(),
+        nodes: [
+            (
+                "node-a".to_owned(),
+                member(&format!("http://{addr}"), Phase::Active),
+            ),
+            ("node-b".to_owned(), member(b_url, Phase::Active)),
+        ]
+        .into(),
+    });
+    std::mem::forget(view);
+    (addr, lone, dir)
+}
````

`crates/data-avalanche-cluster/src/reconciler.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 17b75d6..bb16598 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -268,9 +268,9 @@ impl ClusterReconciler {
         self.statuses.borrow().clone()
     }
 
-    /// [`Self::statuses`] as reloads change them.
-    #[cfg(test)]
-    pub(crate) fn watch_statuses(
+    /// [`Self::statuses`] as reloads change them: the API answers a pipeline's live figures
+    /// from the one this node runs (Admin_Console §4).
+    pub fn watch_statuses(
         &self,
     ) -> watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>> {
         self.statuses.clone()
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 7c533f0..4a7e179 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -306,6 +306,8 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         process,
         gossip: gossip.figures(),
         ended_jobs: Arc::default(),
+        // The pipelines this node runs: their live figures are answered here (Admin_Console §4).
+        statuses: reconciler.watch_statuses(),
     });
     ready.store(true, Ordering::SeqCst);
     let mut cluster = Some((gossip, reconciler, discovery));
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
git add crates/data-avalanche-api/Cargo.toml crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/live.rs crates/data-avalanche-api/src/nodes.rs crates/data-avalanche-api/src/snapshots.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche-cluster/src/reconciler.rs crates/data-avalanche/src/run.rs docs/public/openapi.json docs/src/content/docs/reference/api.md
git commit -m "feat(api): GET /v1/pipelines/{id}/live — the worker's figures from the node that runs it, asked of any node"
```

---

### Task 6: A table's page (`data-avalanche-api`, `data-avalanche`)

**Files:**
- Create: `crates/data-avalanche-api/src/tables.rs`
- Modify: `crates/data-avalanche-api/Cargo.toml`, `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-api/src/live.rs`, `crates/data-avalanche-api/src/snapshots.rs`, `crates/data-avalanche-api/src/test_support.rs`, `crates/data-avalanche/src/run.rs`
- Generated: `crates/data-avalanche-api/openapi.json`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`

**Interfaces:**
- Consumes: Task 5's `live`; Task 3's `Sink::facts`; `WorkerFactory` (M8); `jobs_of`,
  `status_name`, `lake` (M13c); `PipelineDocs::{halts, table_schema}`.
- Produces: `GET /v1/pipelines/{id}/tables/{table}` (viewer); API crate root `Lake` (Default),
  `FACTS_FOR` (30 s); `Node::{factory: Arc<dyn WorkerFactory>, lake: Arc<Lake>}`;
  `live::live(&Node, &Pipeline, &PipelineDocs, &HeaderMap) -> Result<Value, Value>` (crate: the
  holder's figures, or why there are none: `code`, `error`, `node`); `snapshots::{named_table,
  table_status}` (crate). The API crate takes `arrow-schema`, and `data-avalanche-testkit` as a
  dev-dependency; `Lone::sink` is the `MemorySink` its node's factory builds on.

- [ ] **Step 1: Write the failing tests**

The tests: `a_table_s_page_brings_what_is_known_of_it`, `facts_are_kept_for_their_while`, `an_unknown_table_and_a_bad_name_are_refused` (tables); the route table.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 70492dc..179a886 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -927,6 +947,7 @@ mod tests {
                 ("GET", "/v1/pipelines", Viewer),
                 ("GET", "/v1/pipelines/{id}", Viewer),
                 ("GET", "/v1/pipelines/{id}/live", Viewer),
+                ("GET", "/v1/pipelines/{id}/tables/{table}", Viewer),
                 ("POST", "/v1/pipelines/{id}/pause", Operator),
                 ("POST", "/v1/pipelines/{id}/stop", Operator),
                 ("POST", "/v1/pipelines/{id}/resume", Operator),
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api`
Expected: the build fails: no fields `factory`, `lake` in `Node`, no module `tables`.

- [ ] **Step 3: Implement**

The OpenAPI document and the site's reference are generated, not written: after the diffs
below, run `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi` (it rewrites
`crates/data-avalanche-api/openapi.json`), then `cargo xtask docs` (it rewrites
`docs/public/openapi.json` and `docs/src/content/docs/reference/api.md`), and read their diffs:
only the routes this task adds or changes move. `Cargo.lock` follows the manifests on the first
build.

`crates/data-avalanche-api/Cargo.toml`:

````diff
diff --git a/crates/data-avalanche-api/Cargo.toml b/crates/data-avalanche-api/Cargo.toml
index b1147d3..e5675e6 100644
--- a/crates/data-avalanche-api/Cargo.toml
+++ b/crates/data-avalanche-api/Cargo.toml
@@ -10,6 +10,7 @@ description = "The data-avalanche management API: the REST endpoints, their auth
 workspace = true
 
 [dependencies]
+arrow-schema.workspace = true
 axum.workspace = true
 base64.workspace = true
 blake3.workspace = true
@@ -43,6 +44,7 @@ bytes.workspace = true
 axum = { workspace = true, features = ["json"] }
 boon.workspace = true
 data-avalanche-storage.workspace = true
+data-avalanche-testkit.workspace = true
 object_store.workspace = true
 rcgen.workspace = true
 tempfile.workspace = true
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 70492dc..179a886 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -22,6 +22,7 @@ mod peers;
 mod pipelines;
 mod server;
 mod snapshots;
+mod tables;
 #[cfg(test)]
 mod test_support;
 
@@ -37,7 +38,7 @@ use axum::middleware::{Next, from_fn_with_state};
 use axum::response::{IntoResponse, Response};
 use axum::routing::{MethodRouter, get, post};
 use data_avalanche_cluster::{
-    ConfigCoordinator, ControllerStatus, DocsFn, GossipFigures, Members, Recorder,
+    ConfigCoordinator, ControllerStatus, DocsFn, GossipFigures, Members, Recorder, WorkerFactory,
 };
 use data_avalanche_config::Role;
 use data_avalanche_state::{Events, Leases};
@@ -57,6 +58,7 @@ pub use peers::{
 };
 pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
 pub use snapshots::{EndedJobs, READ_AT_ONCE};
+pub use tables::{FACTS_FOR, Lake};
 
 use crate::auth::Denied;
 
@@ -103,6 +105,10 @@ pub struct Node {
     /// The status of each pipeline this node's controllers run: the lease holder answers a
     /// pipeline's live figures from it (Admin_Console §4).
     pub statuses: watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
+    /// Builds a pipeline's sink, which reads a table's Delta facts and columns on request.
+    pub factory: Arc<dyn WorkerFactory>,
+    /// The tables' facts and columns read, each kept [`FACTS_FOR`].
+    pub lake: Arc<Lake>,
 }
 
 impl std::fmt::Debug for Node {
@@ -367,6 +373,20 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             get(live::get),
         ),
+        (
+            r(
+                "GET",
+                "/v1/pipelines/{id}/tables/{table}",
+                Viewer,
+                "A table of the pipeline, named as its config names it: what the worker shows of it, its halt and when it began, the Delta table's facts (read on request, kept 30 s), its columns at the source against Delta's, and the snapshot jobs that copied it",
+                &[
+                    (200, "the table"),
+                    (400, "a table name not in the source's form"),
+                    (404, "no such pipeline, or no trace of the table in it"),
+                ],
+            ),
+            get(tables::get),
+        ),
         (
             act(
                 "pipeline.pause",
````

`crates/data-avalanche-api/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/live.rs b/crates/data-avalanche-api/src/live.rs
index 7293ca0..a830385 100644
--- a/crates/data-avalanche-api/src/live.rs
+++ b/crates/data-avalanche-api/src/live.rs
@@ -255,6 +255,35 @@ async fn from_holder(node: &Node, id: &str, headers: &HeaderMap) -> Response {
     }
 }
 
+/// The pipeline's live figures as a value: this node's, or its lease holder's. `Err` says why
+/// there are none: the answer's `code`, and its `error` and `node`.
+pub(crate) async fn live(
+    node: &Node,
+    p: &Pipeline,
+    docs: &PipelineDocs,
+    headers: &HeaderMap,
+) -> Result<Value, Value> {
+    if let Some(v) = here(node, p, docs).await {
+        return Ok(v);
+    }
+    let r = from_holder(node, &p.id, headers).await;
+    let code = r.status();
+    let read = axum::body::to_bytes(r.into_body(), crate::MAX_ANSWER).await;
+    let v: Value = match read {
+        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or(Value::Null),
+        Err(_) => Value::Null,
+    };
+    match code {
+        StatusCode::OK if v.is_object() => Ok(v),
+        StatusCode::OK => Err(json!({
+            "code": StatusCode::BAD_GATEWAY.as_u16(),
+            "error": "the holder's answer isn't the pipeline's figures",
+            "node": Value::Null,
+        })),
+        _ => Err(json!({ "code": code.as_u16(), "error": v["error"], "node": v["node"] })),
+    }
+}
+
 /// `GET /v1/pipelines/{id}/live`.
 pub(crate) async fn get(
     State(api): State<Api>,
````

`crates/data-avalanche-api/src/snapshots.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/snapshots.rs b/crates/data-avalanche-api/src/snapshots.rs
index 2ce01da..8916091 100644
--- a/crates/data-avalanche-api/src/snapshots.rs
+++ b/crates/data-avalanche-api/src/snapshots.rs
@@ -60,6 +60,22 @@ struct Asked {
     note: Option<String>,
 }
 
+/// The table `name` names, in the source's form: the per-table config key.
+pub(crate) fn named_table(p: &Pipeline, name: &str) -> Result<TableRef, String> {
+    match p.source.kind {
+        SourceType::Mysql if name.contains('.') => None,
+        _ => key_table(p, name),
+    }
+    .filter(|t| !t.schema.is_empty() && !t.name.is_empty())
+    .ok_or_else(|| match p.source.kind {
+        SourceType::Postgres => format!("{name:?}: a PostgreSQL table is schema.table"),
+        SourceType::Mysql => format!(
+            "{name:?}: a MySQL table is named without its database ({})",
+            p.source.db
+        ),
+    })
+}
+
 /// The body's tables, each once, in its order: named as the source's per-table config keys,
 /// no globs; and whether the job empties them first.
 fn tables(p: &Pipeline, text: &str) -> Result<(Vec<TableRef>, bool, Option<String>), String> {
@@ -77,18 +93,7 @@ fn tables(p: &Pipeline, text: &str) -> Result<(Vec<TableRef>, bool, Option<Strin
                 "{name:?}: a table is named exactly, without a glob"
             ));
         }
-        let table = match p.source.kind {
-            SourceType::Mysql if name.contains('.') => None,
-            _ => key_table(p, name),
-        }
-        .filter(|t| !t.schema.is_empty() && !t.name.is_empty())
-        .ok_or_else(|| match p.source.kind {
-            SourceType::Postgres => format!("{name:?}: a PostgreSQL table is schema.table"),
-            SourceType::Mysql => format!(
-                "{name:?}: a MySQL table is named without its database ({})",
-                p.source.db
-            ),
-        })?;
+        let table = named_table(p, name)?;
         if seen.insert(table.clone()) {
             out.push(table);
         }
@@ -102,7 +107,7 @@ fn tables(p: &Pipeline, text: &str) -> Result<(Vec<TableRef>, bool, Option<Strin
 }
 
 /// A job's status as the API names it: `cancelling` from the cancel until a start ends it.
-fn status_name(j: &SnapshotJobDoc) -> &'static str {
+pub(crate) fn status_name(j: &SnapshotJobDoc) -> &'static str {
     match j.status {
         _ if j.cancel.is_some() && !j.is_finished() => "cancelling",
         JobStatus::Queued => "queued",
@@ -117,7 +122,7 @@ fn status_name(j: &SnapshotJobDoc) -> &'static str {
 /// while a truncating job's copy of it began and hasn't completed, `empty` before its first
 /// rows and `partial` after; else `as_before`, the rows it held before the job (a copy that
 /// doesn't truncate writes over them).
-fn lake(truncate: bool, t: &JobTable) -> &'static str {
+pub(crate) fn lake(truncate: bool, t: &JobTable) -> &'static str {
     let began = t.started_at.is_some()
         || matches!(
             t.status,
@@ -165,14 +170,17 @@ fn pace(j: &SnapshotJobDoc, now: DateTime<Utc>) -> (Option<u64>, Option<u64>) {
     (Some(rate), left.map(|l| l.div_ceil(rate)))
 }
 
-fn job_json(p: &Pipeline, j: &SnapshotJobDoc, now: DateTime<Utc>) -> Value {
-    let table_status = |s: JobTableStatus| match s {
+pub(crate) fn table_status(s: JobTableStatus) -> &'static str {
+    match s {
         JobTableStatus::Queued => "queued",
         JobTableStatus::Running => "running",
         JobTableStatus::Completed => "completed",
         JobTableStatus::Failed => "failed",
         JobTableStatus::Cancelled => "cancelled",
-    };
+    }
+}
+
+fn job_json(p: &Pipeline, j: &SnapshotJobDoc, now: DateTime<Utc>) -> Value {
     let (rate, eta) = pace(j, now);
     json!({
         "job_id": j.job_id,
@@ -725,7 +733,10 @@ fn filter(params: &[(String, String)]) -> Result<Filter, Failure> {
 }
 
 /// Pipeline `p`'s jobs, each with its key, the ended ones [`EndedJobs`] keeps not read again.
-async fn jobs_of(node: &Node, p: &Pipeline) -> Result<Vec<(String, SnapshotJobDoc)>, String> {
+pub(crate) async fn jobs_of(
+    node: &Node,
+    p: &Pipeline,
+) -> Result<Vec<(String, SnapshotJobDoc)>, String> {
     let docs = (node.docs)(p).map_err(|e| format!("the state location: {e}"))?;
     let jobs = docs
         .snapshot_jobs_reusing(|key| node.ended_jobs.known(&p.id, key))
````

`crates/data-avalanche-api/src/tables.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/tables.rs b/crates/data-avalanche-api/src/tables.rs
new file mode 100644
index 0000000..99aaf29
--- /dev/null
+++ b/crates/data-avalanche-api/src/tables.rs
@@ -0,0 +1,458 @@
+//! A table's page (Admin_Console §4 Tables, D-M13d.5): what its pipeline's worker shows of it,
+//! its halt with the time it began, the Delta table's facts read on request, its columns at the
+//! source against Delta's, and the snapshot jobs that copied it.
+
+use std::collections::HashMap;
+use std::sync::Mutex;
+use std::time::{Duration, Instant};
+
+use arrow_schema::{Schema, SchemaRef};
+use axum::extract::{Path, State};
+use axum::http::{HeaderMap, StatusCode};
+use axum::response::Response;
+use chrono::{DateTime, Utc};
+use data_avalanche_config::{Pipeline, SourceType};
+use data_avalanche_core::{
+    INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, LOGICAL_TAG, TableFacts, TableRef, TableSchema,
+};
+use serde_json::{Value, json};
+
+use crate::live::live;
+use crate::pipelines::{find, unavailable};
+use crate::snapshots::{jobs_of, lake, named_table, status_name, table_name, table_status};
+use crate::{Api, Node, body, starting};
+
+/// How long a table's Delta facts and columns, once read, answer for it: the table's page reads
+/// them on request, and a reload of the page within this reads nothing again.
+pub const FACTS_FOR: Duration = Duration::from_secs(30);
+
+/// What the sink holds of a table, read at `at`.
+#[derive(Debug, Clone)]
+struct Read {
+    facts: Option<TableFacts>,
+    schema: Option<SchemaRef>,
+    at: DateTime<Utc>,
+    /// The config generation it was read under: a reload may move the table.
+    generation: u64,
+    kept: Instant,
+}
+
+/// The tables' Delta facts and columns this node read, each kept [`FACTS_FOR`].
+#[derive(Debug, Default)]
+pub struct Lake {
+    read: Mutex<HashMap<(String, TableRef), Read>>,
+}
+
+impl Lake {
+    fn kept(&self, id: &str, table: &TableRef, generation: u64) -> Option<Read> {
+        let read = self.read.lock().expect("the lake's reads");
+        read.get(&(id.to_owned(), table.clone()))
+            .filter(|r| r.generation == generation && r.kept.elapsed() < FACTS_FOR)
+            .cloned()
+    }
+
+    fn keep(&self, id: &str, table: &TableRef, r: Read) {
+        let mut read = self.read.lock().expect("the lake's reads");
+        read.retain(|_, r| r.kept.elapsed() < FACTS_FOR);
+        read.insert((id.to_owned(), table.clone()), r);
+    }
+
+    /// The sink's facts and columns of `table`: kept ones while fresh, else read through a sink
+    /// built for the pipeline now. A failed read isn't kept.
+    async fn read(
+        &self,
+        node: &Node,
+        p: &Pipeline,
+        table: &TableRef,
+        generation: u64,
+    ) -> Result<Read, String> {
+        if let Some(r) = self.kept(&p.id, table, generation) {
+            return Ok(r);
+        }
+        let sink = node.factory.build(p)?.sink;
+        let at = Utc::now();
+        let (facts, state) = tokio::join!(sink.facts(table), sink.table_state(table));
+        let r = Read {
+            facts: facts.map_err(|e| e.to_string())?,
+            schema: state.map_err(|e| e.to_string())?.schema,
+            at,
+            generation,
+            kept: Instant::now(),
+        };
+        self.keep(&p.id, table, r.clone());
+        Ok(r)
+    }
+}
+
+/// The type Delta's column has, as the sink records it: its logical tag, else its Arrow type.
+fn delta_type(f: &arrow_schema::Field) -> String {
+    f.metadata()
+        .get(LOGICAL_TAG)
+        .cloned()
+        .unwrap_or_else(|| f.data_type().to_string())
+}
+
+/// The table's columns at the source (the pipeline's cached schema) against Delta's: the source's
+/// in order, then the ones only Delta has (`soft_dropped`: kept, written NULL), then the lineage
+/// columns. MySQL compares names without case (DA-SRC-25).
+fn columns(source: Option<&TableSchema>, delta: Option<&Schema>, fold: bool) -> Vec<Value> {
+    let same = |a: &str, b: &str| {
+        if fold {
+            a.eq_ignore_ascii_case(b)
+        } else {
+            a == b
+        }
+    };
+    let fields: Vec<&arrow_schema::Field> = delta
+        .map(|s| s.fields().iter().map(AsRef::as_ref).collect())
+        .unwrap_or_default();
+    let field = |name: &str| fields.iter().find(|f| same(f.name(), name)).copied();
+    let mut out = vec![];
+    for c in source.map(TableSchema::columns).unwrap_or_default() {
+        let f = field(&c.name);
+        out.push(json!({
+            "name": c.name,
+            "source": c.source_type,
+            "delta": f.map(delta_type),
+            "key": source.is_some_and(|s| s.identifier().iter().any(|k| k == &c.name)),
+            "state": if f.is_some() || delta.is_none() { "kept" } else { "not_in_delta" },
+        }));
+    }
+    let lineage = |n: &str| LINEAGE_COLUMNS.contains(&n);
+    for f in &fields {
+        let in_source = source.is_some_and(|s| s.columns().iter().any(|c| same(&c.name, f.name())));
+        if in_source || lineage(f.name()) || f.name().starts_with(INTERNAL_COLUMN_PREFIX) {
+            continue;
+        }
+        out.push(json!({
+            "name": f.name(),
+            "source": null,
+            "delta": delta_type(f),
+            "key": false,
+            "state": if source.is_some() { "soft_dropped" } else { "kept" },
+        }));
+    }
+    for f in fields.iter().filter(|f| lineage(f.name())) {
+        out.push(json!({
+            "name": f.name(),
+            "source": null,
+            "delta": delta_type(f),
+            "key": false,
+            "state": "lineage",
+        }));
+    }
+    out
+}
+
+/// `GET /v1/pipelines/{id}/tables/{table}`: `404` for a table the pipeline has no trace of.
+pub(crate) async fn get(
+    State(api): State<Api>,
+    Path((id, name)): Path<(String, String)>,
+    headers: HeaderMap,
+) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let (p, docs) = match find(node, &id).await {
+        Ok(found) => found,
+        Err(f) => return f.into(),
+    };
+    let table = match named_table(&p, &name) {
+        Ok(t) => t,
+        Err(e) => return body(StatusCode::BAD_REQUEST, &json!({ "error": e })),
+    };
+    let name = table_name(&p, &table);
+    let generation = node.coordinator.generation().await.generation;
+    let (live, halts, cached, jobs, lake_read) = tokio::join!(
+        live(node, &p, &docs, &headers),
+        docs.halts(),
+        docs.table_schema(&table),
+        jobs_of(node, &p),
+        node.lake.read(node, &p, &table, generation),
+    );
+    let (halts, cached) = match (halts, cached) {
+        (Ok(h), Ok(c)) => (h, c.map(|v| v.doc)),
+        (Err(e), _) | (_, Err(e)) => return unavailable(&e),
+    };
+    let jobs = match jobs {
+        Ok(jobs) => jobs,
+        Err(e) => {
+            return body(
+                StatusCode::SERVICE_UNAVAILABLE,
+                &json!({ "error": format!("the pipeline's snapshot jobs: {e}") }),
+            );
+        }
+    };
+    let (holder, live_table, live_error) = match live {
+        Ok(v) => {
+            let t = v["tables"]
+                .as_array()
+                .and_then(|ts| ts.iter().find(|t| t["table"] == name.as_str()))
+                .cloned();
+            (v["node"].clone(), t, Value::Null)
+        }
+        Err(why) => (Value::Null, None, why),
+    };
+    let halt = halts.tables.iter().find(|h| h.table == table);
+    let (delta, delta_error) = match &lake_read {
+        Ok(r) => (
+            r.facts.as_ref().map(|f| {
+                json!({
+                    "location": f.location,
+                    "version": f.version,
+                    "files": f.files,
+                    "bytes": f.bytes,
+                    "rows": f.rows,
+                    "read_at": r.at,
+                })
+            }),
+            None,
+        ),
+        Err(e) => (None, Some(e.clone())),
+    };
+    let sink_schema = lake_read.as_ref().ok().and_then(|r| r.schema.clone());
+    let copied: Vec<Value> = jobs
+        .iter()
+        .rev()
+        .filter_map(|(_, j)| {
+            let t = j.tables.iter().find(|t| t.table == table)?;
+            Some(json!({
+                "job_id": j.job_id,
+                "status": status_name(j),
+                "requested_at": j.requested_at,
+                "requested_by": j.requested_by,
+                "note": j.note,
+                "truncate": j.truncate,
+                "table": {
+                    "status": table_status(t.status),
+                    "rows_done": t.rows_done,
+                    "started_at": t.started_at,
+                    "ended_at": t.ended_at,
+                    "lake": lake(j.truncate, t),
+                },
+            }))
+        })
+        .collect();
+    let known = live_table.is_some()
+        || halt.is_some()
+        || cached.is_some()
+        || lake_read.as_ref().is_ok_and(|r| r.facts.is_some())
+        || !copied.is_empty();
+    if !known {
+        return body(
+            StatusCode::NOT_FOUND,
+            &json!({ "error": format!("{} has no table {name}", p.id) }),
+        );
+    }
+    let fold = p.source.kind == SourceType::Mysql;
+    body(
+        StatusCode::OK,
+        &json!({
+            "pipeline": p.id,
+            "table": name,
+            "as_of": Utc::now(),
+            "node": holder,
+            "live": live_table,
+            "live_error": live_error,
+            "halt": halt.map(|h| json!({
+                "code": h.reason.code(),
+                "detail": h.reason.to_string(),
+                "at": h.at.to_string(),
+                "since": halts.since(&table),
+                "copy": h.copy,
+            })),
+            "delta": delta,
+            "delta_error": delta_error,
+            "columns": columns(cached.as_ref(), sink_schema.as_deref(), fold),
+            "jobs": copied,
+        }),
+    )
+}
+
+#[cfg(test)]
+mod tests {
+    use std::collections::BTreeMap;
+
+    use data_avalanche_cluster::{Actual, ControllerStatus};
+    use data_avalanche_core::position::Lsn;
+    use data_avalanche_core::{
+        ColumnSchema, ColumnValueType as V, HaltReason, Sink, SourcePosition, TableSpec,
+    };
+    use data_avalanche_pipeline::{Phase, WorkerStatus};
+    use data_avalanche_state::{SnapshotJobDoc, TableHalt};
+    use tokio::sync::watch;
+
+    use super::*;
+    use crate::test_support::{config, open, request, served, served_with};
+
+    fn t(name: &str) -> TableRef {
+        TableRef::new("public", name)
+    }
+
+    fn schema(table: &str, with_legacy: bool) -> TableSchema {
+        let mut columns = vec![
+            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
+            ColumnSchema::new("note", V::Text, "varchar(200)"),
+        ];
+        if with_legacy {
+            columns.push(ColumnSchema::new("legacy", V::Bool, "boolean"));
+        }
+        TableSchema::new(t(table), columns, vec!["id".into()]).unwrap()
+    }
+
+    fn json(text: &str) -> Value {
+        serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
+    }
+
+    /// The table's page: the worker's figures of it, its halt with when it began, the Delta
+    /// facts, the columns (one soft-dropped), and the jobs that copied it, newest first.
+    #[tokio::test]
+    async fn a_table_s_page_brings_what_is_known_of_it() {
+        let worker = WorkerStatus {
+            phase: Phase::Streaming,
+            tracked: [t("orders")].into(),
+            halted: [(t("orders"), HaltReason::TableDropped)].into(),
+            ..WorkerStatus::default()
+        };
+        let status = ControllerStatus {
+            actual: Some(Actual::Running),
+            owner_epoch: Some(4),
+            lease: None,
+            worker: Some(worker),
+        };
+        let statuses = watch::channel(BTreeMap::from([(
+            "orders".to_owned(),
+            watch::channel(status).1,
+        )]))
+        .1;
+        let (addr, lone, _dir) =
+            served_with("http://127.0.0.1:1", open(), |n| n.statuses = statuses).await;
+        let p = config().pipelines["orders"].clone();
+        let docs = (lone.node.docs)(&p).unwrap();
+        // Delta has the column the source dropped since.
+        lone.sink
+            .ensure_table(&TableSpec::new(&schema("orders", true), vec![]).unwrap())
+            .await
+            .unwrap();
+        docs.save_table_schema(&schema("orders", false))
+            .await
+            .unwrap();
+        docs.record_halt(&TableHalt {
+            table: t("orders"),
+            reason: HaltReason::TableDropped,
+            at: SourcePosition::Pg(Lsn(0x20)),
+            copy: false,
+        })
+        .await
+        .unwrap();
+        let now = Utc::now();
+        for (i, tables) in [vec![t("orders")], vec![t("items")], vec![t("orders")]]
+            .into_iter()
+            .enumerate()
+        {
+            let at = now + chrono::Duration::seconds(i as i64);
+            let job = SnapshotJobDoc::new(format!("orders~000{i}"), tables, "ops".into(), at);
+            docs.create_snapshot_job(&job).await.unwrap();
+        }
+        let (code, _, text) = request(
+            addr,
+            "GET",
+            "/v1/pipelines/orders/tables/public.orders",
+            &[],
+            "",
+        )
+        .await;
+        assert_eq!(code, 200, "{text}");
+        let v = json(&text);
+        assert_eq!(
+            (&v["node"], &v["table"]),
+            (&json!("node-a"), &json!("public.orders"))
+        );
+        assert_eq!(v["live"]["state"], "halted");
+        assert_eq!(v["halt"]["code"], "TABLE_DROPPED");
+        assert!(v["halt"]["since"].is_string(), "{v}");
+        assert_eq!(
+            (&v["delta"]["version"], &v["delta"]["rows"]),
+            (&json!(0), &json!(0))
+        );
+        let cols: Vec<(&str, &str)> = v["columns"]
+            .as_array()
+            .unwrap()
+            .iter()
+            .map(|c| (c["name"].as_str().unwrap(), c["state"].as_str().unwrap()))
+            .collect();
+        assert_eq!(
+            &cols[..3],
+            [("id", "kept"), ("note", "kept"), ("legacy", "soft_dropped")]
+        );
+        assert!(cols[3..].iter().all(|(_, s)| *s == "lineage"), "{cols:?}");
+        assert_eq!(v["columns"][0]["key"], true);
+        assert_eq!(v["columns"][1]["source"], "varchar(200)");
+        let jobs: Vec<&str> = v["jobs"]
+            .as_array()
+            .unwrap()
+            .iter()
+            .map(|j| j["job_id"].as_str().unwrap())
+            .collect();
+        assert_eq!(jobs, ["orders~0002", "orders~0000"]);
+    }
+
+    /// The facts are kept a while: a commit after the first read shows only once they expire.
+    #[test]
+    fn facts_are_kept_for_their_while() {
+        let lake = Lake::default();
+        let read = |kept| Read {
+            facts: None,
+            schema: None,
+            at: Utc::now(),
+            generation: 1,
+            kept,
+        };
+        lake.keep("orders", &t("a"), read(Instant::now()));
+        assert!(lake.kept("orders", &t("a"), 1).is_some());
+        assert!(
+            lake.kept("orders", &t("a"), 2).is_none(),
+            "another generation"
+        );
+        let old = Instant::now().checked_sub(FACTS_FOR).unwrap();
+        lake.keep("orders", &t("b"), read(old));
+        assert!(lake.kept("orders", &t("b"), 1).is_none(), "expired");
+    }
+
+    /// A table the pipeline has no trace of is 404; a name not in the source's form is 400. With
+    /// no node running the pipeline, the page says why it has no live figures.
+    #[tokio::test]
+    async fn an_unknown_table_and_a_bad_name_are_refused() {
+        let (addr, lone, _dir) = served("http://127.0.0.1:1", open()).await;
+        let (code, _, text) = request(
+            addr,
+            "GET",
+            "/v1/pipelines/orders/tables/public.nope",
+            &[],
+            "",
+        )
+        .await;
+        assert_eq!(code, 404, "{text}");
+        let (code, _, _) =
+            request(addr, "GET", "/v1/pipelines/orders/tables/orders", &[], "").await;
+        assert_eq!(code, 400, "a PostgreSQL table is schema.table");
+        lone.sink
+            .ensure_table(&TableSpec::new(&schema("orders", false), vec![]).unwrap())
+            .await
+            .unwrap();
+        let (code, _, text) = request(
+            addr,
+            "GET",
+            "/v1/pipelines/orders/tables/public.orders",
+            &[],
+            "",
+        )
+        .await;
+        assert_eq!(code, 200, "{text}");
+        let v = json(&text);
+        assert_eq!(
+            (&v["live"], &v["live_error"]["code"]),
+            (&Value::Null, &json!(409))
+        );
+    }
+}
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index a69759e..982fe0d 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -30,6 +30,7 @@ use data_avalanche_state::{
 };
 use data_avalanche_storage::ObjectStoreVersioned;
 use data_avalanche_telemetry::Metrics;
+use data_avalanche_testkit::{FakeSource, MemorySink};
 use jsonwebtoken::{Algorithm, EncodingKey, Header};
 use object_store::memory::InMemory;
 use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256};
@@ -150,6 +151,21 @@ impl WorkerFactory for NoWorkers {
     }
 }
 
+/// Builds every pipeline's parts on one shared [`MemorySink`]: a node reads tables' facts
+/// through it.
+#[derive(Debug)]
+struct Lakes(MemorySink);
+
+impl WorkerFactory for Lakes {
+    fn build(&self, _: &Pipeline) -> Result<Parts, String> {
+        Ok(Parts {
+            source: Arc::new(FakeSource::new(vec![])),
+            sink: Arc::new(self.0.clone()),
+            hooks: Arc::new(data_avalanche_pipeline::NoHooks),
+        })
+    }
+}
+
 /// A lone node `node-a` running generation 1 of [`CONFIG`], read from `file`: its gossip on a
 /// loopback port, its reloads reaching a reconciler without pipelines.
 pub(crate) struct Lone {
@@ -162,6 +178,8 @@ pub(crate) struct Lone {
     pub stops: tokio::sync::watch::Receiver<Stop>,
     pub _reconciler: ClusterReconciler,
     pub _gossip: Gossip,
+    /// The sink every pipeline's [`Node::factory`] builds: a test creates and commits its tables.
+    pub sink: MemorySink,
 }
 
 /// `${VAR}` values a test sets, and changes.
@@ -272,6 +290,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
         Duration::from_secs(10),
     );
     let (control, stops) = Control::new();
+    let sink = MemorySink::new();
     Lone {
         node: Node {
             coordinator: Arc::new(coordinator),
@@ -294,12 +313,15 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             gossip: gossip.figures(),
             ended_jobs: Arc::default(),
             statuses: reconciler.watch_statuses(),
+            factory: Arc::new(Lakes(sink.clone())),
+            lake: Arc::default(),
         },
         events,
         leases,
         stops,
         _reconciler: reconciler,
         _gossip: gossip,
+        sink,
     }
 }
 
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 4a7e179..72cead5 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -308,6 +308,9 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         ended_jobs: Arc::default(),
         // The pipelines this node runs: their live figures are answered here (Admin_Console §4).
         statuses: reconciler.watch_statuses(),
+        // A table's Delta facts, read by any node on request.
+        factory: Arc::clone(&deps.factory),
+        lake: Arc::default(),
     });
     ready.store(true, Ordering::SeqCst);
     let mut cluster = Some((gossip, reconciler, discovery));
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
git add crates/data-avalanche-api/Cargo.toml crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/live.rs crates/data-avalanche-api/src/snapshots.rs crates/data-avalanche-api/src/tables.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche/src/run.rs docs/public/openapi.json docs/src/content/docs/reference/api.md
git commit -m "feat(api): GET /v1/pipelines/{id}/tables/{table} — a table's figures, halt, Delta facts, columns and jobs"
```

---

### Task 7: `GET /v1/tables` (`data-avalanche-api`, `xtask`)

**Files:**
- Modify: `crates/data-avalanche-api/src/lib.rs`, `crates/data-avalanche-api/src/tables.rs`, `xtask/src/api.rs`
- Generated: `crates/data-avalanche-api/openapi.json`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`

**Interfaces:**
- Consumes: Task 6's `live`; `READ_AT_ONCE` (M13c).
- Produces: `GET /v1/tables` (viewer) with `state` (`halted`, `stale`, `snapshot`, `costly`,
  `streaming`, `copied`), `pipeline`, `q`; each row gains `pipeline`, `node`, `live`, `listed_as`. The
  site's API page gains a `tables` section.

- [ ] **Step 1: Write the failing tests**

The tests: `every_table_is_listed_worst_first`, `a_table_behind_the_target_is_stale`, `a_pipeline_whose_holder_can_t_be_asked_lists_its_halts` (tables); the route table; `every_tag_of_the_document_has_a_section` (xtask).

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 179a886..e1ffe94 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -948,6 +962,7 @@ mod tests {
                 ("GET", "/v1/pipelines/{id}", Viewer),
                 ("GET", "/v1/pipelines/{id}/live", Viewer),
                 ("GET", "/v1/pipelines/{id}/tables/{table}", Viewer),
+                ("GET", "/v1/tables", Viewer),
                 ("POST", "/v1/pipelines/{id}/pause", Operator),
                 ("POST", "/v1/pipelines/{id}/stop", Operator),
                 ("POST", "/v1/pipelines/{id}/resume", Operator),
````

`crates/data-avalanche-api/src/tables.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/tables.rs b/crates/data-avalanche-api/src/tables.rs
index 99aaf29..68ac1ab 100644
--- a/crates/data-avalanche-api/src/tables.rs
+++ b/crates/data-avalanche-api/src/tables.rs
@@ -455,4 +686,170 @@ mod tests {
             (&Value::Null, &json!(409))
         );
     }
+
+    fn commit(table: &str, rewritten: u64, files: u64) -> TableCommit {
+        TableCommit {
+            table: t(table),
+            rows: 1,
+            snapshot: false,
+            version: 3,
+            cost: CommitCost {
+                files_rewritten: rewritten,
+                files: Some(files),
+            },
+            at: Utc::now(),
+        }
+    }
+
+    /// A worker 90 s behind: `items` halted, `audit` copying, `events` rewriting most of its files
+    /// (stale too: so `quiet`, caught up, is the one streaming table), `orders` stale; `dumped` a
+    /// dump that ended, its copy rewriting every file.
+    fn statuses(
+        behind: i64,
+    ) -> watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>> {
+        let mut w = WorkerStatus {
+            phase: Phase::Streaming,
+            unsynced_since_micros: Some(
+                (Utc::now() - chrono::Duration::seconds(behind)).timestamp_micros(),
+            ),
+            tracked: [t("orders"), t("items"), t("events"), t("quiet")].into(),
+            copied: [t("dumped")].into(),
+            halted: [(t("items"), HaltReason::TableDropped)].into(),
+            snapshots: [(
+                t("audit"),
+                SnapshotProgress {
+                    rows_done: 1,
+                    waiting: false,
+                },
+            )]
+            .into(),
+            ..WorkerStatus::default()
+        };
+        w.committed(&commit("events", 8, 10));
+        w.committed(&commit("quiet", 1, 10));
+        w.committed(&commit("dumped", 10, 10));
+        let status = ControllerStatus {
+            actual: Some(Actual::Running),
+            owner_epoch: Some(4),
+            lease: None,
+            worker: Some(w),
+        };
+        watch::channel(BTreeMap::from([(
+            "orders".to_owned(),
+            watch::channel(status).1,
+        )]))
+        .1
+    }
+
+    fn names(v: &Value) -> Vec<(String, String)> {
+        v["tables"]
+            .as_array()
+            .unwrap()
+            .iter()
+            .map(|t| {
+                (
+                    t["table"].as_str().unwrap().to_owned(),
+                    t["listed_as"].as_str().unwrap().to_owned(),
+                )
+            })
+            .collect()
+    }
+
+    /// Every table, worst first, with counts by listed state; the filters keep their tables.
+    #[tokio::test]
+    async fn every_table_is_listed_worst_first() {
+        let behind = statuses(0);
+        let (addr, _lone, _dir) =
+            served_with("http://127.0.0.1:1", open(), |n| n.statuses = behind).await;
+        let (code, _, text) = request(addr, "GET", "/v1/tables", &[], "").await;
+        assert_eq!(code, 200, "{text}");
+        let v = json(&text);
+        let pairs = |p: &[(&str, &str)]| -> Vec<(String, String)> {
+            p.iter()
+                .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
+                .collect()
+        };
+        assert_eq!(
+            names(&v),
+            pairs(&[
+                ("public.items", "halted"),
+                ("public.audit", "snapshot"),
+                ("public.events", "costly"),
+                ("public.orders", "streaming"),
+                ("public.quiet", "streaming"),
+                ("public.dumped", "copied"),
+            ])
+        );
+        assert_eq!(v["tables"][0]["pipeline"], "orders");
+        assert_eq!(v["tables"][0]["node"], "node-a");
+        assert_eq!(
+            v["counts"],
+            json!({
+                "all": 6, "halted": 1, "stale": 0, "snapshot": 1, "costly": 1, "streaming": 2,
+                "copied": 1
+            })
+        );
+        assert_eq!(v["unreachable"], json!([]));
+        let (_, _, text) =
+            request(addr, "GET", "/v1/tables?state=halted,costly&q=EVE", &[], "").await;
+        assert_eq!(names(&json(&text)), pairs(&[("public.events", "costly")]));
+        let (code, _, _) = request(addr, "GET", "/v1/tables?state=gone", &[], "").await;
+        assert_eq!(code, 400);
+        let (code, _, _) = request(addr, "GET", "/v1/tables?pipeline=nope", &[], "").await;
+        assert_eq!(code, 404);
+    }
+
+    /// Past the lag target while streaming, a table is stale, listed after the halted ones.
+    #[tokio::test]
+    async fn a_table_behind_the_target_is_stale() {
+        let behind = statuses(90);
+        let (addr, _lone, _dir) =
+            served_with("http://127.0.0.1:1", open(), |n| n.statuses = behind).await;
+        let (_, _, text) = request(addr, "GET", "/v1/tables?state=stale", &[], "").await;
+        let v = json(&text);
+        let stale: Vec<String> = names(&v).into_iter().map(|(n, _)| n).collect();
+        assert_eq!(stale, ["public.events", "public.orders", "public.quiet"]);
+        assert_eq!(v["counts"]["stale"], 3);
+    }
+
+    /// A holder that can't be asked: the pipeline's halted tables come from storage, and the
+    /// pipeline is named under `unreachable` with why.
+    #[tokio::test]
+    async fn a_pipeline_whose_holder_can_t_be_asked_lists_its_halts() {
+        let (addr, lone, _dir) = served("http://127.0.0.1:1", open()).await;
+        let docs = (lone.node.docs)(&config().pipelines["orders"]).unwrap();
+        docs.record_halt(&TableHalt {
+            table: t("items"),
+            reason: HaltReason::TableDropped,
+            at: SourcePosition::Pg(Lsn(0x20)),
+            copy: false,
+        })
+        .await
+        .unwrap();
+        let holder = Holder {
+            node_id: "node-z".into(),
+            incarnation: 1,
+        };
+        lone.leases
+            .acquire("orders", &holder, Duration::from_secs(30), Utc::now())
+            .await
+            .unwrap();
+        let (code, _, text) = request(addr, "GET", "/v1/tables", &[], "").await;
+        assert_eq!(code, 200, "{text}");
+        let v = json(&text);
+        assert_eq!(
+            names(&v),
+            [("public.items".to_owned(), "halted".to_owned())]
+        );
+        assert_eq!(v["tables"][0]["live"], false);
+        assert!(v["tables"][0]["fresh_as_of"].is_string(), "{v}");
+        assert_eq!(
+            (
+                &v["unreachable"][0]["pipeline"],
+                &v["unreachable"][0]["code"],
+                &v["unreachable"][0]["node"]
+            ),
+            (&json!("orders"), &json!(503), &json!("node-z"))
+        );
+    }
 }
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api && cargo test -p xtask`
Expected: the three fail on `404` (no route `/v1/tables`).

- [ ] **Step 3: Implement**

The OpenAPI document and the site's reference are generated, not written: after the diffs
below, run `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi` (it rewrites
`crates/data-avalanche-api/openapi.json`), then `cargo xtask docs` (it rewrites
`docs/public/openapi.json` and `docs/src/content/docs/reference/api.md`), and read their diffs:
only the routes this task adds or changes move. `Cargo.lock` follows the manifests on the first
build.

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 179a886..e1ffe94 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -387,6 +387,20 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             get(tables::get),
         ),
+        (
+            r(
+                "GET",
+                "/v1/tables",
+                Viewer,
+                "Every pipeline's tables, worst first, as each lease holder's worker shows them: by listed state (halted, stale, snapshot, costly, streaming, copied), pipeline, and text in the name; a pipeline whose holder can't be asked lists its halted tables from storage and is named under `unreachable`",
+                &[
+                    (200, "the tables, with counts by listed state"),
+                    (400, "a parameter that isn't one, or a state that isn't"),
+                    (404, "no such pipeline in the generation this node runs"),
+                ],
+            ),
+            get(tables::all),
+        ),
         (
             act(
                 "pipeline.pause",
````

`crates/data-avalanche-api/src/tables.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/tables.rs b/crates/data-avalanche-api/src/tables.rs
index 99aaf29..68ac1ab 100644
--- a/crates/data-avalanche-api/src/tables.rs
+++ b/crates/data-avalanche-api/src/tables.rs
@@ -1,13 +1,15 @@
-//! A table's page (Admin_Console §4 Tables, D-M13d.5): what its pipeline's worker shows of it,
-//! its halt with the time it began, the Delta table's facts read on request, its columns at the
-//! source against Delta's, and the snapshot jobs that copied it.
+//! The tables (Admin_Console §4 Tables, D-M13d.5, D-M13d.7). Every pipeline's tables, each as
+//! its lease holder's worker shows it, or from storage when the holder can't be asked. A table's
+//! page: what its pipeline's worker shows of it, its halt with the time it began, the Delta
+//! table's facts read on request, its columns at the source against Delta's, and the snapshot
+//! jobs that copied it.
 
 use std::collections::HashMap;
 use std::sync::Mutex;
 use std::time::{Duration, Instant};
 
 use arrow_schema::{Schema, SchemaRef};
-use axum::extract::{Path, State};
+use axum::extract::{Path, RawQuery, State};
 use axum::http::{HeaderMap, StatusCode};
 use axum::response::Response;
 use chrono::{DateTime, Utc};
@@ -19,8 +21,10 @@ use serde_json::{Value, json};
 
 use crate::live::live;
 use crate::pipelines::{find, unavailable};
-use crate::snapshots::{jobs_of, lake, named_table, status_name, table_name, table_status};
-use crate::{Api, Node, body, starting};
+use crate::snapshots::{
+    READ_AT_ONCE, jobs_of, lake, named_table, status_name, table_name, table_status,
+};
+use crate::{Api, Failure, Node, body, starting};
 
 /// How long a table's Delta facts and columns, once read, answer for it: the table's page reads
 /// them on request, and a reload of the page within this reads nothing again.
@@ -269,17 +273,244 @@ pub(crate) async fn get(
     )
 }
 
+/// The states `GET /v1/tables` filters by, worst first: the order it lists them in. `costly` is a
+/// streaming table whose last MERGE rewrote more than half its files (D-MS.15); `copied`, a
+/// `dump_*` table that isn't streamed.
+const STATES: [&str; 6] = [
+    "halted",
+    "stale",
+    "snapshot",
+    "costly",
+    "streaming",
+    "copied",
+];
+
+/// `GET /v1/tables`'s filter: the states, the pipeline, and text the table's name holds.
+#[derive(Debug, Default)]
+struct Filter {
+    states: Vec<String>,
+    pipeline: Option<String>,
+    q: Option<String>,
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
+            "state" => {
+                for s in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
+                    if !STATES.contains(&s) {
+                        return Err(bad(format!(
+                            "state {s:?} isn't one of {}",
+                            STATES.join(", ")
+                        )));
+                    }
+                    f.states.push(s.to_owned());
+                }
+            }
+            "pipeline" => f.pipeline = Some(value),
+            "q" => f.q = Some(value.to_lowercase()).filter(|q| !q.is_empty()),
+            other => {
+                return Err(bad(format!(
+                    "unknown parameter {other:?}: the tables take state, pipeline, q"
+                )));
+            }
+        }
+    }
+    Ok(f)
+}
+
+/// The state a table is listed under: its own, `stale` for a stale one, `costly` for a streaming
+/// one whose last MERGE rewrote more than half its files. A `copied` one is neither.
+fn listed_state(t: &Value) -> &'static str {
+    let rewritten = t["files_rewritten"].as_u64();
+    let files = t["files"].as_u64();
+    match t["state"].as_str() {
+        Some("halted") => "halted",
+        _ if t["stale"] == true => "stale",
+        Some("snapshot") => "snapshot",
+        Some("copied") => "copied",
+        _ if rewritten
+            .zip(files)
+            .is_some_and(|(r, f)| f > 0 && r * 2 > f) =>
+        {
+            "costly"
+        }
+        _ => "streaming",
+    }
+}
+
+/// A pipeline's tables when its holder can't be asked: its halts, from storage.
+async fn stored(node: &Node, p: &Pipeline) -> Result<Vec<Value>, String> {
+    let docs = (node.docs)(p).map_err(|e| format!("the state location: {e}"))?;
+    let halts = docs.halts().await.map_err(|e| e.to_string())?;
+    Ok(halts
+        .tables
+        .iter()
+        .filter(|h| !h.copy)
+        .map(|h| {
+            let since = halts.since(&h.table);
+            json!({
+                "table": table_name(p, &h.table),
+                "state": "halted",
+                "halt": { "code": h.reason.code(), "detail": h.reason.to_string(), "since": since },
+                "fresh_as_of": since,
+                "stale": false,
+            })
+        })
+        .collect())
+}
+
+/// One pipeline's tables, each with its pipeline and node, and why the holder couldn't be asked.
+async fn of_pipeline(
+    node: &Node,
+    p: &Pipeline,
+    headers: &HeaderMap,
+) -> (Vec<Value>, Option<Value>) {
+    let docs = match (node.docs)(p) {
+        Ok(docs) => docs,
+        Err(e) => {
+            let why = json!({ "pipeline": p.id, "error": format!("the state location: {e}") });
+            return (vec![], Some(why));
+        }
+    };
+    let (rows, node_name, why) = match live(node, p, &docs, headers).await {
+        Ok(v) => {
+            let rows = v["tables"].as_array().cloned().unwrap_or_default();
+            (rows, v["node"].clone(), None)
+        }
+        Err(why) => match stored(node, p).await {
+            Ok(rows) => (rows, Value::Null, Some(why)),
+            Err(e) => (
+                vec![],
+                Value::Null,
+                Some(json!({ "code": why["code"], "error": e })),
+            ),
+        },
+    };
+    let rows = rows
+        .into_iter()
+        .map(|mut t| {
+            t["pipeline"] = json!(p.id);
+            t["node"] = node_name.clone();
+            t["live"] = json!(why.is_none());
+            t["listed_as"] = json!(listed_state(&t));
+            t
+        })
+        .collect();
+    let why = why.map(|mut w| {
+        w["pipeline"] = json!(p.id);
+        w
+    });
+    (rows, why)
+}
+
+/// `GET /v1/tables`: every pipeline's tables, worst first, filtered; each pipeline's holder asked
+/// [`READ_AT_ONCE`] at a time. A pipeline whose holder can't be asked lists its halted tables
+/// from storage, and is named under `unreachable` with why. `counts` are by listed state, of the
+/// tables the pipeline and `q` filters keep.
+pub(crate) async fn all(
+    State(api): State<Api>,
+    RawQuery(raw): RawQuery,
+    headers: HeaderMap,
+) -> Response {
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
+    let mut pipelines: Vec<Pipeline> = config
+        .pipelines
+        .values()
+        .filter(|p| f.pipeline.as_ref().is_none_or(|id| &p.id == id))
+        .cloned()
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
+    let headers = &headers;
+    let read = futures_util::stream::iter(pipelines)
+        .map(|p| async move { of_pipeline(node, &p, headers).await })
+        .buffered(READ_AT_ONCE)
+        .collect::<Vec<_>>()
+        .await;
+    let mut tables = vec![];
+    let mut unreachable = vec![];
+    for (rows, why) in read {
+        tables.extend(rows.into_iter().filter(|t| {
+            f.q.as_ref().is_none_or(|q| {
+                t["table"]
+                    .as_str()
+                    .is_some_and(|n| n.to_lowercase().contains(q.as_str()))
+            })
+        }));
+        unreachable.extend(why);
+    }
+    let mut counts = serde_json::Map::new();
+    counts.insert("all".into(), json!(tables.len()));
+    for state in STATES {
+        let n = tables.iter().filter(|t| t["listed_as"] == state).count();
+        counts.insert(state.into(), json!(n));
+    }
+    tables.retain(|t| f.states.is_empty() || f.states.iter().any(|s| t["listed_as"] == s.as_str()));
+    let rank = |t: &Value| {
+        STATES
+            .iter()
+            .position(|s| t["listed_as"] == *s)
+            .unwrap_or(STATES.len())
+    };
+    tables.sort_by(|a, b| {
+        (rank(a), a["pipeline"].as_str(), a["table"].as_str()).cmp(&(
+            rank(b),
+            b["pipeline"].as_str(),
+            b["table"].as_str(),
+        ))
+    });
+    body(
+        StatusCode::OK,
+        &json!({
+            "as_of": Utc::now(),
+            "counts": counts,
+            "tables": tables,
+            "unreachable": unreachable,
+        }),
+    )
+}
+
 #[cfg(test)]
 mod tests {
     use std::collections::BTreeMap;
 
     use data_avalanche_cluster::{Actual, ControllerStatus};
+    use data_avalanche_core::CommitCost;
     use data_avalanche_core::position::Lsn;
     use data_avalanche_core::{
         ColumnSchema, ColumnValueType as V, HaltReason, Sink, SourcePosition, TableSpec,
     };
-    use data_avalanche_pipeline::{Phase, WorkerStatus};
-    use data_avalanche_state::{SnapshotJobDoc, TableHalt};
+    use data_avalanche_pipeline::{Phase, SnapshotProgress, TableCommit, WorkerStatus};
+    use data_avalanche_state::{Holder, SnapshotJobDoc, TableHalt};
     use tokio::sync::watch;
 
     use super::*;
````

`xtask/src/api.rs`:

````diff
diff --git a/xtask/src/api.rs b/xtask/src/api.rs
index f2d3e82..8a5857c 100644
--- a/xtask/src/api.rs
+++ b/xtask/src/api.rs
@@ -13,6 +13,10 @@ const TAGS: &[(&str, &str)] = &[
         "pipelines",
         "Pipelines: their state, and the lifecycle commands.",
     ),
+    (
+        "tables",
+        "Every pipeline's tables, each as its lease holder's worker shows it now.",
+    ),
     ("snapshots", "Snapshot jobs started through the API."),
     (
         "config",
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api && cargo test -p xtask`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/tables.rs docs/public/openapi.json docs/src/content/docs/reference/api.md xtask/src/api.rs
git commit -m "feat(api): GET /v1/tables — every pipeline's tables, worst first, filtered; an unreachable holder's halts from storage"
```

---

### Task 8: The site

**Files:**
- Create: `docs/src/content/docs/runbooks/pipeline-figures.md`
- Modify: `docs/src/content/docs/runbooks/halted-table.md`

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces: the runbook `A pipeline's tables and figures`; a halt's `since` in the halted-table
  runbook.

- [ ] **Step 1: Write the failing tests**

None: these are pages, and the site's build (Step 4) checks their links.

- [ ] **Step 2: Run them to see them fail**

Nothing runs here.

- [ ] **Step 3: Implement**

`docs/src/content/docs/runbooks/halted-table.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/halted-table.md b/docs/src/content/docs/runbooks/halted-table.md
index 00bb060..ab29d1d 100644
--- a/docs/src/content/docs/runbooks/halted-table.md
+++ b/docs/src/content/docs/runbooks/halted-table.md
@@ -19,7 +19,10 @@ once the cause is fixed. The halt stays across restarts and failovers until then
 curl -s https://cdc.example.internal/v1/pipelines/shop
 ```
 
-Its `halted_tables` lists each halted table with its code and a detail.
+Its `halted_tables` lists each halted table with its code, a detail, and `since`, when it halted
+(none for a halt recorded before 1.1.0). The table's page,
+`GET /v1/pipelines/shop/tables/public.orders`, adds its columns at the source against Delta's and
+the jobs that copied it ([A pipeline's tables and figures](/data-avalanche/runbooks/pipeline-figures/)).
 
 ## 2. Fix the cause
 
````

`docs/src/content/docs/runbooks/pipeline-figures.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/pipeline-figures.md b/docs/src/content/docs/runbooks/pipeline-figures.md
new file mode 100644
index 0000000..556dbe8
--- /dev/null
+++ b/docs/src/content/docs/runbooks/pipeline-figures.md
@@ -0,0 +1,82 @@
+---
+title: A pipeline's tables and figures
+description: What a pipeline's worker shows now, a table's page, and every pipeline's tables, asked of any node.
+sidebar:
+  order: 9
+---
+
+`GET /v1/pipelines/<id>` reads a pipeline's state from storage. The routes here show what its
+worker holds in memory: they are answered by the node that holds the pipeline's lease. Any node
+takes them, with the `viewer` role, and asks the holder with your own credentials.
+
+## The worker, now
+
+```sh
+curl -s https://cdc.example.internal/v1/pipelines/shop/live
+```
+
+The answer names the `node` that runs the worker, its `actual` state and `owner_epoch`, and:
+
+- `worker`: its `phase`, the positions it has `read` and made durable (`checkpoint`),
+  `lag_seconds` and `fresh_as_of` (what lake readers see: every change older than the oldest one
+  not yet durable), the rows read since it started, and its warnings.
+- `source`: what the source keeps for the pipeline, read every heartbeat, with `read_at`.
+  PostgreSQL: the server's write position (`head`), the slot (`active`, `confirmed_flush`,
+  `restart_lsn`, `wal_held_bytes`, `wal_status`), `max_slot_wal_keep_size` (`null`: unlimited),
+  and the publication with its table count. MySQL and MariaDB: the server's `head` (its GTID set,
+  or binlog file and offset) and `binlog_expire_seconds` (`null`: never expired), and how far the
+  stream reads behind (`behind_seconds`).
+- `tables`: each table the worker tracks, halts or copies, with its `state` (`streaming`,
+  `halted`, `snapshot`, or `copied`: a `dump_*` table whose copy ended, or a `dump_initial` one
+  the lake had at the start, which isn't streamed), the changes committed since the worker
+  started, the changes of the last minute (`changes_per_minute`), its `last_commit` and Delta
+  `version`, the last MERGE's `files_rewritten` against the table's `files`, and its
+  `fresh_as_of`. A halted table is as fresh as its halt; a `copied` one has none. A streaming
+  table is **stale** when that is over 60 s old (the lag target) while the worker streams; a
+  paused pipeline is never stale. A snapshot job's table outside the config is listed while it
+  copies; afterwards its job tells ([Snapshot jobs](/data-avalanche/runbooks/snapshot-jobs/)).
+
+The figures start again with each worker: a restart or a failover sets the counts to zero.
+
+| Answer | Why |
+|---|---|
+| `409` | no node holds the lease: nothing runs the pipeline |
+| `503` | the holder can't be asked: it isn't a member this node sees, gossips no API address, or is this node before its worker starts (`node` names it) |
+| `502`, `504` | the holder didn't answer, or not within 3 s |
+| `421` | a request forwarded to a node that no longer runs the worker: the lease moved; ask again |
+
+## A table's page
+
+```sh
+curl -s https://cdc.example.internal/v1/pipelines/shop/tables/public.orders
+```
+
+Name the table as the pipeline's config does: `schema.table` for PostgreSQL, the table alone for
+MySQL. The page brings together:
+
+- `live`: the table's row of the worker's figures above, or `live_error` with why there is none;
+- `halt`: its code, detail and `since`, when it halted (none for a halt recorded before 1.1.0);
+- `delta`: the Delta table's `location`, `version`, `files`, `bytes` and `rows` (from the files'
+  statistics), read by the node you ask when you ask, and kept 30 s; `delta_error` when it
+  can't be read;
+- `columns`: each column at the source (as the pipeline last cached it) against Delta's. `state`
+  is `kept`, `soft_dropped` (gone at the source, kept in Delta and written NULL), `not_in_delta`
+  (not added yet), or `lineage`;
+- `jobs`: the snapshot jobs that copied it, newest first, with what lake readers saw of it.
+
+A table the pipeline has no trace of is `404`; a name not in the source's form is `400`.
+
+## Every pipeline's tables
+
+```sh
+curl -s 'https://cdc.example.internal/v1/tables?state=halted,stale'
+```
+
+Every pipeline's tables, worst first, each with its `pipeline`, `node` and `listed_as`:
+`halted`, `stale`, `snapshot`, `costly` (a streaming table whose last MERGE rewrote more than half
+its files: random-key churn, the cost driver), `streaming`, or `copied`. `counts` gives how many
+there are of each. Filter with `state` (comma-separated), `pipeline`, and `q` (text in the table's name, any
+case).
+
+A pipeline whose holder can't be asked is named under `unreachable` with why; its halted tables
+are listed from storage, with `live: false`.
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
git add docs/src/content/docs/runbooks/halted-table.md docs/src/content/docs/runbooks/pipeline-figures.md
git commit -m "docs: a pipeline's tables and figures — the live figures, a table's page, every pipeline's tables; a halt's since"
```

---

### Task 9: A pipeline's tables through any node, end to end (E2E)

**Files:**
- Create: `crates/data-avalanche/tests/e2e/console_pipelines.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the E2E harness (`Cluster`, `Proc::call`, `eventually`); Tasks 1–7.
- Produces: `a_pipeline_s_tables_are_shown_through_any_node`.

- [ ] **Step 1: Write the failing tests**

The tests: `a_pipeline_s_tables_are_shown_through_any_node`.

`crates/data-avalanche/tests/e2e/console_pipelines.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_pipelines.rs b/crates/data-avalanche/tests/e2e/console_pipelines.rs
new file mode 100644
index 0000000..1d12a3c
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/console_pipelines.rs
@@ -0,0 +1,111 @@
+//! A pipeline's tables and figures through any node (Admin_Console §4; M13d), on three nodes: a
+//! node that doesn't run the test's pipeline answers its live figures from the holder; the
+//! table's page brings the Delta table's facts and its columns, a column dropped at the source
+//! shown soft-dropped; every pipeline's tables are listed, each with the node that runs it.
+
+use std::time::Duration;
+
+use data_avalanche_testkit::require_it;
+use serde_json::{Value, json};
+
+use crate::harness::{Doc, eventually};
+use crate::nodes::Cluster;
+
+/// The state a column of the table's page is in.
+fn column<'v>(page: &'v Value, name: &str) -> &'v Value {
+    page["columns"]
+        .as_array()
+        .expect("columns")
+        .iter()
+        .find(|c| c["name"] == name)
+        .unwrap_or_else(|| panic!("column {name} in {page}"))
+}
+
+#[tokio::test]
+async fn a_pipeline_s_tables_are_shown_through_any_node() {
+    require_it!();
+    let mut c = Cluster::new("console_pipelines").await;
+    c.start_all().await;
+    let test = c.e.test.clone();
+    c.e.insert_series("items", 5, Doc::Row).await;
+    c.landed().await;
+    let holders = c.holders().await;
+    let holder = holders[&test].0.clone();
+    let other = ["n1", "n2", "n3"]
+        .into_iter()
+        .find(|n| *n != holder)
+        .expect("another node");
+    let o = c.proc(other);
+    let items = if c.e.source.is_postgres() {
+        format!("{}.items", c.e.schema)
+    } else {
+        "items".to_owned()
+    };
+
+    // The live figures, from the holder; the source's detail once a heartbeat read it.
+    let live = format!("/v1/pipelines/{test}/live");
+    eventually("the live figures", Duration::from_secs(60), async || {
+        let (code, v) = o.call("GET", &live).await;
+        code == 200 && !v["source"]["read_at"].is_null()
+    })
+    .await;
+    let (_, v) = o.call("GET", &live).await;
+    assert_eq!(
+        (&v["node"], &v["actual"]),
+        (&json!(holder), &json!("running")),
+        "{v}"
+    );
+    let row = v["tables"]
+        .as_array()
+        .unwrap()
+        .iter()
+        .find(|t| t["table"] == items.as_str())
+        .unwrap_or_else(|| panic!("{items} in {v}"))
+        .clone();
+    assert_eq!(row["state"], "streaming", "{row}");
+    assert!(row["changes"].as_u64() >= Some(5), "{row}");
+    assert!(row["version"].is_u64() && row["files"].is_u64(), "{row}");
+    let detail = &v["source"]["detail"];
+    if c.e.source.is_postgres() {
+        assert_eq!(detail["engine"], "postgres", "{v}");
+        assert_eq!(detail["slot"]["active"], true, "{v}");
+        assert_eq!(detail["publication"]["tables"], 1, "{v}");
+    } else {
+        assert_eq!(detail["engine"], "mysql", "{v}");
+        assert!(detail["head"].is_string(), "{v}");
+    }
+
+    // The table's page: the Delta facts and the columns.
+    let page = format!("/v1/pipelines/{test}/tables/{items}");
+    let (code, p) = o.call("GET", &page).await;
+    assert_eq!(code, 200, "{p}");
+    assert_eq!(p["node"], json!(holder), "{p}");
+    assert_eq!(p["delta"]["rows"], 5, "{p}");
+    assert!(
+        p["delta"]["version"].as_u64() >= row["version"].as_u64(),
+        "{p}"
+    );
+    assert_eq!(column(&p, "id")["key"], true, "{p}");
+    assert_eq!(column(&p, "doc")["state"], "kept", "{p}");
+    assert_eq!(column(&p, "_op")["state"], "lineage", "{p}");
+
+    // A column dropped at the source stays in Delta: soft-dropped, once the facts kept expire.
+    c.e.exec("ALTER TABLE items DROP COLUMN doc; INSERT INTO items VALUES (100)")
+        .await;
+    eventually("doc soft-dropped", Duration::from_secs(90), async || {
+        let (code, p) = o.call("GET", &page).await;
+        code == 200 && column(&p, "doc")["state"] == "soft_dropped"
+    })
+    .await;
+
+    // Every pipeline's tables, each with the node that runs it.
+    let (code, t) = o.call("GET", "/v1/tables").await;
+    assert_eq!(code, 200, "{t}");
+    assert_eq!(t["counts"]["all"], 3, "{t}");
+    assert_eq!(t["unreachable"], json!([]), "{t}");
+    for row in t["tables"].as_array().unwrap() {
+        let id = row["pipeline"].as_str().unwrap();
+        assert_eq!(row["node"], json!(holders[id].0), "{row}");
+    }
+    c.stop().await;
+}
````

`crates/data-avalanche/tests/e2e/main.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index e202b81..e04a52e 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -9,6 +9,7 @@
 mod api;
 mod cluster;
 mod console_nodes;
+mod console_pipelines;
 mod crashes;
 mod events;
 mod failover;
````

- [ ] **Step 2: Run them to see them fail**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e console_pipelines -- --test-threads=1`
Expected: none: the behaviour landed in Tasks 1–7, and the scenario passes at once (about 80 s at the plan's verification, on PostgreSQL 18, MySQL 8.4 with and without GTIDs, and MariaDB 11.8).

- [ ] **Step 3: Implement**

Nothing: the behaviour landed in the earlier tasks.

- [ ] **Step 4: Run the tests and the gates**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e console_pipelines -- --test-threads=1`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

Run it on MySQL too: `DATA_AVALANCHE_E2E_SOURCE=mysql` with `DATA_AVALANCHE_MYSQL=mysql-84`, `mysql-84-nogtid`, `mariadb-118`.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests/e2e/console_pipelines.rs crates/data-avalanche/tests/e2e/main.rs
git commit -m "test(e2e): a pipeline's tables and figures through any node — live, a table's page, every pipeline's tables"
```

---

## Exit

- Every task is committed, one commit each, after the spec patch's commit. The gates pass on the
  last one: `cargo fmt --all --check`, both clippy runs, `cargo test --workspace`.
- These pass with the emulators up:
  - the sources' `detail` tests on PostgreSQL 18 and on MySQL 8.4 (GTID and file/position) and
    MariaDB 11.8;
  - the whole E2E suite on PostgreSQL 18:
    `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`;
  - the same with `DATA_AVALANCHE_E2E_SOURCE=mysql`, on each `DATA_AVALANCHE_MYSQL` target.
- `cd docs && npm run build` passes its link check.
- At the plan's verification (the code above, replayed on `1a65b1d`):
  - `cargo test --workspace`: 1701 passed. fmt and both clippy runs are clean.
  - The E2E suite, 55 scenarios each: PostgreSQL 18 passed (745 s); MySQL 8.4 with GTIDs (699 s),
    MySQL 8.4 in file/position mode (704 s) and MariaDB 11.8 (697 s) passed. The new scenario took
    about 80 s on each.
  - The sources' `detail` tests passed on PostgreSQL 18 and on the three MySQL targets.
  - The site built, with a clean link check.
  - The plan's 61 diff blocks applied in order on `1a65b1d` and gave the verified tree.
- Then, on Oleksii's request: the whole-branch review and its fixes, the push, and the PR.

## Deviations during execution

### Task 5: the plan's text

- Step 2's expected failure: the step-1 diff adds the route table's row only, so the build passes
  and the route table's test fails at run time, not the build.
- The step-1 and step-3 `lib.rs` diffs were split from one diff. The implementer applied step 3
  before step 1. The committed tree is the verified one.

### Task 5: a fix round, the `copied` state (S-M13d.2)

Task 5's review found that the listing reported a finished `dump_*` table, and a finished or
halted copy outside the config, as `streaming`, and could call them `stale`, for the rest of the
worker's life. Oleksii chose a `copied` state (D-M13d.4–6, D-M13d.9, S-M13d.2). The fix lands on
Task 5's commit as its own commit, after the spec patch's; Tasks 7 and 8 above carry `copied` in
`GET /v1/tables` and the runbook.

- [ ] **Step 1: Write the failing tests**

The tests: `the_node_that_runs_the_worker_answers_from_its_memory` gains a `copied` table and a
copy that ended (live); `a_dump_beside_streaming_tables_shows_copied` (worker).

`crates/data-avalanche-api/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/live.rs b/crates/data-avalanche-api/src/live.rs
index 25b3665..7293ca0 100644
--- a/crates/data-avalanche-api/src/live.rs
+++ b/crates/data-avalanche-api/src/live.rs
@@ -294,6 +302,7 @@ mod tests {
             phase: Phase::Streaming,
             unsynced_since_micros: Some((now - lag).timestamp_micros()),
             tracked: [t("orders"), t("items")].into(),
+            copied: [t("dumped")].into(),
             halted: [(t("items"), HaltReason::TableDropped)].into(),
             snapshots: [(
                 t("audit"),
@@ -324,6 +333,15 @@ mod tests {
             },
             at: now,
         });
+        // A copy outside the config that ended: its job tells, the page doesn't list it.
+        w.committed(&TableCommit {
+            table: t("ended"),
+            rows: 3,
+            snapshot: true,
+            version: 2,
+            cost: CommitCost::default(),
+            at: now,
+        });
         w
     }
 
@@ -358,7 +376,8 @@ mod tests {
     }
 
     /// The node that runs the worker answers from its memory: each table's state, figures and
-    /// freshness; a halted table is as fresh as its halt, and isn't stale.
+    /// freshness; a halted table is as fresh as its halt, and isn't stale; a dump that ended is
+    /// `copied`, and a copy that ended isn't listed.
     #[tokio::test]
     async fn the_node_that_runs_the_worker_answers_from_its_memory() {
         let lagging = statuses(Actual::Running, Some(worker(chrono::Duration::seconds(90))));
@@ -418,6 +437,22 @@ mod tests {
             audit["stale"], false,
             "a copy's readers see what its job says"
         );
+        let dumped = by_table(&v, "public.dumped");
+        assert_eq!(
+            (&dumped["state"], &dumped["stale"], &dumped["fresh_as_of"]),
+            (&json!("copied"), &json!(false), &Value::Null),
+            "a dump that ended isn't streamed"
+        );
+        let listed: Vec<&Value> = v["tables"]
+            .as_array()
+            .unwrap()
+            .iter()
+            .map(|t| &t["table"])
+            .collect();
+        assert!(
+            !listed.contains(&&json!("public.ended")),
+            "a copy that ended: {listed:?}"
+        );
     }
 
     /// A paused pipeline is behind by design: nothing is stale.
````

`crates/data-avalanche-pipeline/src/worker.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index e667386..2da13cd 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -1991,6 +1996,26 @@ mod tests {
         assert!(w.source.opened().len() <= 1);
     }
 
+    /// A `dump_*` table beside streaming ones shows `copied` once it isn't streamed: the
+    /// `dump_initial` table the sink has at the start, the `dump_always` one once its snapshot is
+    /// durable (D-M13d.5).
+    #[tokio::test]
+    async fn a_dump_beside_streaming_tables_shows_copied() {
+        let w = World::new(&["a", "b", "c"]);
+        w.started(&["a", "b", "c"]).await;
+        w.source.load(&t("b"), rows(1..=2));
+        let p = mode("mode: never, overrides: { public.a: dump_initial, public.b: dump_always }");
+        let (end, st) = run(&w, &p, async |status| streamed(&status, &w).await).await;
+        end.unwrap();
+        assert_eq!(w.ids("b"), [1, 2]);
+        assert_eq!(st.copied, BTreeSet::from([t("a"), t("b")]));
+        assert_eq!(
+            st.tracked,
+            BTreeSet::from([t("b"), t("c")]),
+            "a never streams"
+        );
+    }
+
     #[tokio::test]
     async fn a_dump_always_table_dumps_and_ends() {
         let w = World::new(&["a"]);
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-pipeline --lib worker:: && cargo test -p data-avalanche-api --lib live::`
Expected: the build fails: no field `copied` in `WorkerStatus`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/live.rs b/crates/data-avalanche-api/src/live.rs
index 25b3665..7293ca0 100644
--- a/crates/data-avalanche-api/src/live.rs
+++ b/crates/data-avalanche-api/src/live.rs
@@ -80,12 +80,14 @@ fn tables(
     let running = matches!(s.actual, Some(Actual::Running)) && w.phase == Phase::Streaming;
     let fresh = fresh_as_of(w, now);
     let window = chrono::Duration::from_std(FRESH_WITHIN).expect("a minute");
+    // A copy outside the config is listed while it copies: once it ends, or halts, its job
+    // tells (its figures stay in `tables` until the worker ends).
     let all: BTreeSet<&TableRef> = w
         .tracked
         .iter()
         .chain(w.halted.keys())
         .chain(w.snapshots.keys())
-        .chain(w.tables.keys())
+        .chain(w.copied.iter())
         .collect();
     all.into_iter()
         .map(|t| {
@@ -95,9 +97,15 @@ fn tables(
             let state = match (halt, snapshot) {
                 (Some(_), _) => "halted",
                 (None, Some(_)) => "snapshot",
+                (None, None) if w.copied.contains(t) => "copied",
                 (None, None) => "streaming",
             };
-            let fresh = if halt.is_some() { since } else { Some(fresh) };
+            // A halted table is as fresh as its halt; a dump that ended, as its last copy.
+            let fresh = match state {
+                "halted" => since,
+                "copied" => None,
+                _ => Some(fresh),
+            };
             let figures = w.tables.get(t);
             json!({
                 "table": table_name(p, t),
````

`crates/data-avalanche-pipeline/src/session.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 0763e60..6adf0d3 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -464,9 +464,13 @@ impl<'c> Session<'c> {
                     // A completion durable now ends its snapshot, as in the loop.
                     self.durable += 1;
                     let done = self.snaps.durable(self.durable);
+                    let copies = &self.copies;
                     self.core.update(|st| {
-                        for (table, _) in done {
+                        for (table, dump) in done {
                             st.snapshots.remove(&table);
+                            if dump && !copies.contains_key(&table) {
+                                st.copied.insert(table);
+                            }
                         }
                         st.checkpoint = Some(cp);
                     });
@@ -669,14 +673,18 @@ impl<'c> Session<'c> {
                     self.unsynced.pop_front();
                 }
                 for (table, dump) in self.snaps.durable(self.durable) {
+                    // A dump, or a copy, stops once its snapshot is durable: the config's dump
+                    // shows `copied`, a copy outside the config leaves the pipeline's page.
+                    let copy = dump && self.copies.remove(&table).is_some();
                     self.core.update(|st| {
                         st.snapshots.remove(&table);
+                        if dump && !copy {
+                            st.copied.insert(table.clone());
+                        }
                     });
                     if dump {
-                        // A dump, or a copy, stops once its snapshot is durable.
                         self.acc.halt_table(&table);
                         self.schemas.remove(&table);
-                        self.copies.remove(&table);
                     }
                 }
                 if let Err(e) = self.stream.acknowledge(&cp).await {
````

`crates/data-avalanche-pipeline/src/status.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
index 6cc9751..68eab72 100644
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -150,6 +150,9 @@ pub struct WorkerStatus {
     pub soft_dropped: BTreeMap<TableRef, usize>,
     /// The tables the last Phase 0 tracks: the pipeline's page lists them.
     pub tracked: BTreeSet<TableRef>,
+    /// The `dump_*` tables this worker doesn't stream: a `dump_initial` table the sink had at the
+    /// start, or a dump whose snapshot ended. The pipeline's page lists them `copied`.
+    pub copied: BTreeSet<TableRef>,
     /// Per table, what this worker committed to it since it started: the pipeline's page.
     pub tables: BTreeMap<TableRef, TableFigures>,
     /// What the source keeps for the pipeline, as of `source_read_at`: read every heartbeat. A
````

`crates/data-avalanche-pipeline/src/worker.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index e667386..2da13cd 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -730,6 +730,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         .chain(halts.iter().map(|(t, _)| t.clone()))
         .collect();
     let mut tracked = BTreeSet::new();
+    let mut copied = BTreeSet::new();
     let mut drifted = BTreeSet::new();
     let mut requests = vec![];
     let mut abandoned = vec![];
@@ -778,7 +779,10 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
         // halted in this pass gets no `in_progress` document (final review M5).
         let request = matches!(decision, Decision::Snapshot { resume: false, .. });
         match decision {
-            Decision::Stop => continue,
+            Decision::Stop => {
+                copied.insert(table.clone());
+                continue;
+            }
             Decision::Stream if in_progress => abandoned.push(table.clone()),
             _ => {}
         }
@@ -1299,6 +1303,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
     core.server = server;
     core.update(|st| {
         st.tracked.clone_from(&tracked);
+        st.copied = copied;
         warnings.into_iter().for_each(|w| st.warn(w));
     });
     core.tracked = tracked;
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-pipeline --lib worker:: && cargo test -p data-avalanche-api --lib live::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes. The OpenAPI document doesn't move.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/live.rs crates/data-avalanche-pipeline/src/session.rs crates/data-avalanche-pipeline/src/status.rs crates/data-avalanche-pipeline/src/worker.rs
git commit -m "fix(api): a dump that ended is listed copied, a copy that ended isn't listed"
```
