---
updated: 2026-10-23
domain: Clustering
tags: [v1, implementation-plan, m8b, cluster, config-generation, reload, coordinator, mutability, api, e2e]
resolves: [DA-OPS-8]
sources: [Every System is a Log (Restate)]
---

# M8b — Config Generations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change a running cluster's config without a restart. An operator edits the config file
on a node and calls `POST /v1/config/reload` there. That node checks the file and publishes it to
`globals.state` as the next config generation. Every node then adopts that generation and applies
the difference to its own pipelines:
- a reloadable field (class R) changes in place, in the running worker;
- a restart-only field of a pipeline (class PR) restarts that pipeline under the lease it holds;
- an added pipeline starts where the ring places it;
- a removed pipeline is drained, torn down (`auto_cleanup`) and released by its owner;
- a field only a process restart applies (class PROC) refuses the reload, and nothing is
  published.

`GET /v1/config/status` reports each node's applied generation.

M8 is split (Q-M8-3). M8a, merged as PR #11, built the gossip, the ring, the leave and the
multi-node E2E. Its nodes adopt the cloud generation at a cold start (D-3) and never change it
after. **M8b** (this plan) covers config generations at run time and the reload E2E.

**Architecture:** The tasks go bottom-up, from the source to the binary.

- **The source** (Task 1): `SourceConnector::reconfigure` takes the reloaded pipeline. Each
  connector keeps its R settings under a lock and reads them at its next connection, prepare,
  resolve or teardown:
  - PostgreSQL: `heartbeat`, `slot_on_orphan`, `auto_cleanup`;
  - MySQL: `binlog_purge_action`, `heartbeat`;
  - both: the table patterns.
- **The worker** (Task 2): `Command::Reconfigure` hands the running worker the new pipeline. Its
  settings, its accumulator, its buffer permits and its timers take the R fields at once.
- **The controller** (Task 3) takes a reload's `Update` for its pipeline:
  - `Reconfigure`: the change goes to the running worker, and the metric series take the new
    name;
  - `Restart`: discard and replay, under the same lease;
  - `Remove`: drain, teardown, release;
  - `DrainTimeout`.
- **The reconciler** (Task 4) applies a `Reload`. It adds and removes controllers, sends each
  pipeline's update, resizes the hand-off and takeover caps, and sets the stabilization. A
  pipeline added back while its removal runs starts once that removal ends.
- **The coordinator** (Task 5, new `coordinator.rs`) runs the node's generation:
  - it adopts each newer generation, found by a poll every `lease_renewal_interval` or announced
    by a member's gossiped `config_generation`;
  - it resolves the generation in this node's environment, diffs it against the running config
    (config §12), and hands the changes to the reconciler;
  - it gossips the applied generation and sets a gauge;
  - its `reload` publishes the node's file with a compare-and-swap, and retries when another node
    published first.
- **Three in-process nodes** (Task 6): a reload on one node reaches all three. A removed pipeline
  is torn down once, an added one placed, an R change applied in place and a PR change restarted,
  and no pipeline ever runs on two nodes.
- **The binary** (Task 7):
  - the endpoints, unauthenticated until M9 (Q-M8b-1);
  - a log filter and format that a generation changes;
  - the coordinator started after the reconciler, and stopped before the leave;
  - DA-OPS-8's startup warning.
- **The E2E suite** (Task 8):
  - three node processes: a reload on `n2` removes, adds and renames;
  - one node: a PROC change is refused, a PR change restarts the pipeline under its lease, and a
    process restart runs the generation, not the file.

**Tech Stack:** Rust 1.98.1 (edition 2024). No new external crate. `serde_json` moves from the
binary's dev-dependencies to its dependencies: the endpoints write their JSON with it, since
axum's `json` feature stays off. The logger uses `tracing-subscriber`'s `reload` layers (its
`std` feature).

**Spec:**
- `doc/v1.0/00-roadmap.md`: §3.6, §3.7, §3.11 and §3.12 (the interfaces this plan extends); §4 M8
  (scope and exit: "a reload that propagates to every node"); §4 M9 (the config endpoints).
- `doc/v1.0/M8a-membership.md`: D-M8a.1 (`NodeMeta.config_generation`), D-M8a.9 and D-M8a.10
  (the reconciler and the hand-off cap), D-M8a.13 (the leave), D-M8a.18 (per-node values through
  `${VAR}`), DA-OPS-8.
- `doc/research/Clustering_Architecture_Research.md` §9:
  - §9.1: the generation pointer and its snapshots;
  - §9.2: the reload flow, with no proxy;
  - §9.3: cold-start precedence (D-3);
  - §9.4: the mutability classes.
- `doc/specs/Configuration_Specification.md`:
  - §1.2: the mutability classes;
  - §6.3, §6.5 and §9: the Mutability columns;
  - §11.1: a diff over resolved values;
  - §12: reload and reconciliation;
  - §13: V-061.
- `doc/specs/System-Overview.md`: the API table rows for config reload and config status.
- `doc/research/API_RBAC_and_Versioning.md` §2: their roles, which M9 enforces.
- `doc/research/Pipeline_Lifecycle_State_Machine.md` §3–§4: a restart keeps the desired state
  (DA-API-10).
- `doc/research/Postgres_Slot_and_Publication_Lifecycle.md`: `auto_cleanup` at a removal.

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `cluster → pipeline, state, storage, config, core,
  telemetry`. The binary depends on every crate. `testkit` is a dev-dependency only. `core` names
  no config type, which is why `SourceConnector::reconfigure` takes `&dyn Any`.
- **The generation** (Clustering §9.1, M2): `config/generation.json` points at an immutable
  `config/snapshots/<NNNNNN>-<hash>.yaml` holding the raw text. Each node resolves `${VAR}`s and
  files in its own environment, and loads with the anchor's `fingerprint_key`. Both sides of a
  diff are loaded with that key, so their secrets compare.
- **The endpoints are unauthenticated until M9** (Q-M8b-1), like `/metrics`. They serve plain
  HTTP on `globals.api.address`.
- **Time in tests:** unit and in-process tests run on tokio's paused clock
  (`start_paused = true`). The binary's listener tests and the E2E suite run in real time.
- **Test gates:**
  - Plain `cargo test` stays hermetic.
  - The source ITs (Task 1) need `DATA_AVALANCHE_IT=1`.
  - The E2E suite needs `DATA_AVALANCHE_IT=1` and the `e2e-hooks` feature, runs with
    `--test-threads=1`, and runs on either source (`DATA_AVALANCHE_E2E_SOURCE`).
- **Credentials:** the tests use the compose services' fixed test credentials only, and nothing
  else goes in code, docs or output. Tests generate their keys at run time. Never print the files
  under `test-infra/.tls/`.
- **Public-API conventions:** public types are re-exported at the crate root. Errors are
  `thiserror` types (`ReloadError`, new in Task 5).
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready;
  a step's diffs apply in the order given, after the earlier steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to break a reload in production. Each is pinned by a test in the
task after the arrow.

1. **Two operators reload on two nodes at once.** Each compare-and-swap is against the generation
   it read, so one of them loses. The loser must re-read, adopt the winner's generation, check its
   own file against it and publish on top. Neither generation is lost, and the loser doesn't
   report a failure → Task 5 (`a_reload_that_loses_the_race_publishes_on_top_of_the_winner`,
   `a_reload_publishes_on_top_of_a_newer_generation`).
2. **A pipeline removed and added back before its removal ends** (an operator reverts a removal).
   The old controller is still draining under the node's own lease, and its teardown drops the
   slot. A new controller started at once would run beside it, and lose its slot to the teardown.
   It must wait for the removal to end → Task 4
   (`a_pipeline_added_back_during_its_removal_starts_after_it`).
3. **A generation one node can't resolve** (a `${VAR}` set on the other nodes but not on this
   one). That node must keep running its generation, log the failure once, and try again at each
   poll; the others apply the generation. The status then shows the cluster not converged →
   Task 5 (`a_generation_that_doesnt_load_here_waits_for_the_next`), Task 7
   (`a_reload_publishes_the_file_and_the_status_follows`).
4. **The same text, re-read after a secret changed** (a rotated password in a `${VAR}` or a
   credentials file; config §12.1: "detected here even though the config text is unchanged").
   The text is published again, so every node resolves it anew and restarts what changed. It must
   not be answered "unchanged" → Task 5
   (`the_same_text_is_published_again_when_what_it_resolves_to_changed`).
5. **A restart-only change** (`tokens`, `globals.state`, a per-node literal such as `node_name`
   that differs from the generation's). The reload is refused with V-061, naming the field, and
   nothing is published. A node whose own file differs in a per-node key is warned at startup →
   Task 5 (`a_refused_reload_publishes_nothing`), Task 7
   (`a_reload_publishes_the_file_and_the_status_follows`,
   `the_per_node_keys_apart_from_the_generation_are_named`), Task 8
   (`a_reload_publishes_the_file_and_a_restart_keeps_the_generation`).

## Decisions and rulings

**Asked and answered** (2026-10-22, all three "Recommended"):

| # | Question | Answer |
|---|---|---|
| Q-M8b-1 | How is a reload triggered before M9's API? | **The endpoints now, auth in M9**: `POST /v1/config/reload` and `GET /v1/config/status` on the existing listener, unauthenticated like `/metrics` until M9 adds auth and RBAC. The E2E drives the real endpoint |
| Q-M8b-2 | Which R-class pipeline fields are applied in the running worker? | **All of them in place** (config §1.2), through `Command::Reconfigure` |
| Q-M8b-3 | Who cleans up a removed pipeline? | **Its owner**: drain, `teardown` (`auto_cleanup`), release. An owner offline at the removal leaves an orphan: a new gap (DA-OPS-9) |

**Rulings where the plan decides** (each pinned by a test):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M8b.1 | **A source takes its R fields at run time.** `SourceConnector::reconfigure(&self, pipeline: &dyn Any)`, a no-op by default, receives the reloaded `Pipeline`. PostgreSQL keeps `heartbeat`, `slot_on_orphan`, `auto_cleanup`, `tables.include` and `tables.exclude` in a `Live` under a lock. MySQL keeps `binlog_purge_action`, `heartbeat` and the patterns in the `Shared` it has with its stream's catalog connector. Each is read at the next connection, prepare, resolve or teardown; an open stream keeps what it opened with. Anything that isn't a `Pipeline` is ignored | `core` names no config type. The patterns are R while the table set they resolve to is the same (config §1.2), so a later rescan must use the new ones. `max_wal_lag_bytes` is the worker's (D-M8b.2) | Task 1 (`a_reload_reaches_the_reloadable_settings` in both crates, `a_teardown_after_a_reload_takes_its_auto_cleanup`; ITs `a_reloaded_orphan_policy_takes_the_next_prepare`, `a_reloaded_include_takes_the_next_resolve` on PostgreSQL and MySQL) |
| D-M8b.2 | **The worker takes R fields in place.** `Command::Reconfigure(Box<Pipeline>)`; `Command` is `Clone + PartialEq`, no longer `Copy`. `Settings::reconfigure` takes the pipeline's name for the lineage, `flush`, the transaction limits, `buffer_capacity_rows`, the snapshot `parallelism` and `chunk_size`, `heartbeat`, `max_wal_lag_bytes`, `rescan_interval` and `schema_evolution`. The session sets the accumulator's policy, limits and lineage. It installs a buffer semaphore of the new size, clamps each queued need to the new cap, and re-arms its timers; the source is told. A reload that arrives during a reconnect reaches the next session | The worker builds its `Settings` once (M6a); the session owns the accumulator and the timers. A queued need larger than a shrunk cap would wait forever | Task 2 (`a_reload_reaches_the_running_worker`, `a_reload_rearms_the_rescans`, `a_smaller_buffer_lets_a_waiting_flush_through`, `a_reload_during_a_reconnect_reaches_the_next_session`) |
| D-M8b.3 | **The controller's `Update`.** `Reconfigure`: the controller keeps the new pipeline for every later run, its metric series take the new name and `per_table` (`PipelineMetrics::relabel`), and a running worker gets `Command::Reconfigure`. `Restart`: discard and replay (§12.4). The worker is aborted and the next one runs from the checkpoint with the new pipeline and documents, under the same lease. `ERROR` is cleared, since the new config may be the fix, but the desired state stands: a paused or stopped pipeline stays so (DA-API-10). `Remove`, on the owner: drain (at most `drain_timeout`), then `teardown` through a source built from the latest pipeline, then release. The teardown is retried while the source is busy or doesn't answer, within `contended_window`. A drain cut off, or a lease lost meanwhile, leaves the source as it is and warns. On standby it just ends. Either way the series go (`forget`). `DrainTimeout` takes the reloaded value | Clustering §9.2: each owner reconciles its own pipelines. An aborted worker's unflushed events are read again by the next one, and the Phase-4 guard dedupes them. A drain that was cut off may leave a worker still holding the slot | Task 3 (all seven controller tests, and `a_relabelled_pipeline_drops_its_old_series`, `a_forgotten_pipeline_keeps_no_series`) |
| D-M8b.4 | **The reconciler's `Reload { pipelines, cluster }`**, sent through `Reloader::apply` (a channel into the reconcile loop). `Add` starts a controller on standby until the ring places it. `Remove` lets the controller finish in the background, and a shutdown waits for it. `Reconfigure` and `Restart` go to the controller. The globals: `rebalance_stabilization` becomes the next joiner's wait (`Placement::set_stabilization`). `max_concurrent_takeovers` and `max_concurrent_handoffs` resize their semaphores (`Cap`): more permits are added at once, fewer are taken back as they come free, so a permit in use is never taken away. `drain_timeout` goes to every controller. The statuses map follows. **A pipeline added back while its removal runs** waits in `returning` and starts when the removal ends; an update for it meanwhile changes what will start | A removed controller drains under the node's own lease and its teardown drops the slot, so a second controller of the same id must not run beside it | Task 4 (all six tests) |
| D-M8b.5 | **How a node learns of a generation.** It polls `generation.json` every `lease_renewal_interval`. It also reads it at once when a member gossips a `config_generation` newer than the one it applied (`Members::newest_generation`). The applied generation goes into its own `NodeMeta` (`Advertiser::set_config_generation`, a foca renew) and the gauge `data_avalanche_config_generation` | Clustering §9.2 step 5: the gossiped number is the nudge, the poll the backstop | Task 5 (`a_generation_published_elsewhere_is_adopted_at_the_next_poll`, `a_gossiped_generation_is_adopted_without_waiting_for_the_poll`, `a_config_generation_reaches_every_peer`), `the_node_reports_its_config_generation` |
| D-M8b.6 | **Adoption.** The generation's text is loaded in this node's environment with the anchor's key. For each pipeline whose `tables.include` or `exclude` changed, both table sets are resolved through the factory's source; if either can't be resolved, the change is treated as a restart. Then the diff. A text that doesn't load here, or a diff with a PROC change as this node resolves it, isn't applied: the node logs it at `ERROR` once per generation, runs what it runs, and tries again at each poll. Otherwise the pipelines' changes go to the reconciler, the globals to the hook (the log), and the generation to the gossip and the gauge | `PrIfTableSetChanges` needs the live catalog (config §1.2), and a restart is always safe. Per-node values resolved from `${VAR}`s differ by node, so one node can refuse what the others apply | Task 5 (`a_tables_change_restarts_only_when_the_set_changes`, `a_generation_that_doesnt_load_here_waits_for_the_next`) |
| D-M8b.7 | **The reload.** It holds the coordinator's lock, and a stopped coordinator refuses it (`Stopped`). Then: (1) the file is loaded; one that doesn't load is `Invalid`. (2) The current generation is read and adopted first when newer. (3) The file is diffed against it; a PROC change is `Rejected` (V-061). (4) If the text's hash is the generation's and the diff is empty, nothing is published (`published: false`). (5) Otherwise the file is published with a compare-and-swap. A lost race, or an outcome the store can't tell, goes back to (2), up to `PUBLISH_ATTEMPTS` = 5, then `Contended`. (6) The published generation is adopted here. The others adopt it from `globals.state` | Clustering §9.2–§9.4: the check runs before publishing, so the cluster never adopts a generation that changes a PROC field. The same text with a changed resolution must be published again (Review Focus 4) | Task 5 (`a_reload_publishes_and_applies_the_next_generation`, `an_unchanged_file_publishes_nothing`, `a_refused_reload_publishes_nothing`, `a_reload_publishes_on_top_of_a_newer_generation`, `a_reload_that_loses_the_race_publishes_on_top_of_the_winner`, `the_same_text_is_published_again_when_what_it_resolves_to_changed`, `a_stopped_coordinator_adopts_and_publishes_nothing`) |
| D-M8b.8 | **The endpoints** (Q-M8b-1). `POST /v1/config/reload` reads the node's file again (`run --config`) and answers: <ul><li>202 `{"generation": N}`;</li><li>200 `{"generation": N, "unchanged": true}`;</li><li>422 `{"error", "report"}` (`Report::to_json`) when the file is invalid or rejected;</li><li>409 when contended;</li><li>503 when `globals.state` fails, when the node is stopping, or before the coordinator runs;</li><li>500 when the file can't be read.</li></ul>`GET /v1/config/status` answers 200 `{"generation", "applied", "nodes": {name: generation}, "converged"}`: the generation `globals.state` names, this node's, each gossip member's, and whether every member's is the cluster's. The routes read the coordinator from a `OnceLock` set after startup | Clustering §9.2 step 6: the reload is complete when every live node reports N. The status reads the gossip, not each node over HTTP: no node calls another | Task 7 (`the_listener_answers_health_and_metrics`, `a_reload_publishes_the_file_and_the_status_follows`), Task 8 |
| D-M8b.9 | **The log** (`globals.log`, R). The subscriber's filter and format are `reload` layers. `Logger::apply` takes a generation's `globals.log`, except where `--log-level` or `--log-format` was given. The adopted generation's log applies right after the join; before that the file's is in force | The log is the one globals R field with a live effect outside the cluster crate (the API's auth and tracing come with M9 and M10) | Task 7 (`a_generation_changes_the_level_and_the_format`, `the_command_line_wins_over_a_generation`) |
| D-M8b.10 | **DA-OPS-8's warning.** At startup the node compares the per-node keys of its file with the adopted generation's: `memberlist.node_name`, `bind_addr`, `bind_port`, `advertise_addr`, `advertise_port`, and `api.address`. Each one that differs is named, with which value the node runs: its name and API address are the file's, its gossip addresses the generation's. The warning also says that a reload from this node is refused (V-061) until they match | The generation's per-node literals are another node's (D-M8a.18). A reload diffs the file against them, and `globals.cluster` and `api.address` are PROC | Task 7 (`the_per_node_keys_apart_from_the_generation_are_named`) |
| D-M8b.11 | **The shutdown order.** On SIGTERM the coordinator stops first: no generation is adopted during the leave, and a reload is answered 503. The leave's bound comes from the running generation (`ConfigCoordinator::config`), which may have added pipelines | A generation adopted halfway through a leave would start controllers that the leave doesn't hand off | Task 5 (`a_stopped_coordinator_adopts_and_publishes_nothing`), Task 7 |
| D-M8b.12 | **The E2E reload.** Three node processes. A reload on `n2`: <ul><li>removes `c`, whose owner releases its lease and drops its slot;</li><li>adds `d`, with an initial snapshot;</li><li>renames the test's pipeline (R): same lease and epoch, the new metric label.</li></ul>Every node reports generation 2. On one node: <ul><li>a PROC change is refused;</li><li>a PR change (`source.timeouts`) restarts the pipeline under the same epoch;</li><li>a process restart runs generation 2 with the file changed back, until a reload publishes generation 3</li></ul> | `data_avalanche_pipeline_status` reads `running` from the worker's start, before it reads. A `snapshots: never` pipeline started by a reload could miss a row inserted in that window, so `d` snapshots (measured on MariaDB: 1 of 2 runs missed the row) | Task 8 (`a_reload_reaches_every_node`, `a_reload_publishes_the_file_and_a_restart_keeps_the_generation`) |

**Every R field of config §1.2, and where it applies:**

| Field (`mutability.rs`) | Applied by |
|---|---|
| `globals.log` | the logger (D-M8b.9) |
| `globals.cluster.rebalance_stabilization`, `max_concurrent_handoffs`, `max_concurrent_takeovers`, `drain_timeout` | the reconciler (D-M8b.4) |
| `globals.api.auth`, `credentials`, `jwt`, `oauth2`, `role_mapping`; `globals.tracing` | nothing yet: the API's auth arrives with M9, tracing with M10 |
| `pipelines.*.name`, `sink.metrics` | the controller's series (D-M8b.3) and the worker's lineage (D-M8b.2) |
| `source.postgres.{auto_cleanup, heartbeat, slot_on_orphan}`, `source.mysql.{binlog_purge_action, heartbeat}` | the source (D-M8b.1) |
| `source.postgres.max_wal_lag_bytes`, `tables.snapshots.{parallelism, chunk_size}`, `sink.{flush, max_transaction_batch_rows, max_transaction_buffer_bytes, buffer_capacity_rows}`, `schema_evolution` | the worker (D-M8b.2) |
| `tables.include`, `tables.exclude` (R while the set is the same) | the source's next resolve (D-M8b.1); else a restart (D-M8b.6) |
| `sink.iceberg.*` | nothing: an Iceberg pipeline is refused in v1 |

**Measured while planning** (2026-10-22, recorded in the tasks):

- Every task was built in a scratch workspace on this branch's base, test first. The test stage of
  each task was committed on its own and run (Step 2's output), then the implementation (Step 4's).
- Mutation checks, each run against its test:
  - Task 1: the PostgreSQL resolve back on the built patterns fails
    `a_reloaded_include_takes_the_next_resolve`.
  - Task 2: without the buffer clamp, `a_smaller_buffer_lets_a_waiting_flush_through` fails; without
    the timers' re-arm, `a_reload_rearms_the_rescans` fails.
  - Task 3: without the `ERROR` reset, `a_restart_runs_a_pipeline_in_error_again` fails; without
    `DrainTimeout`, `a_reloaded_drain_timeout_is_the_next_drains` fails.
  - Task 4: an `Add` that doesn't wait for the removal fails
    `a_pipeline_added_back_during_its_removal_starts_after_it`.
  - Task 5: without the gossip nudge, `a_gossiped_generation_is_adopted_without_waiting_for_the_poll`
    fails. Answering "changed" for every table set fails `a_tables_change_restarts_only_when_the_set_changes`.
    Answering "unchanged" on the text alone fails
    `the_same_text_is_published_again_when_what_it_resolves_to_changed`. A lost race returned as an
    error fails `a_reload_that_loses_the_race_publishes_on_top_of_the_winner`.
- Found while building Task 1: the first draft handed `reconfigure` only the pipeline's source
  block, so a later resolve still used the table patterns the connector was built with. Task 1's
  ITs pin the whole-pipeline form; the first run of the task stages caught one slot IT still
  passing the block.
- The E2E scenarios (Task 8):
  - `a_reload_reaches_every_node` failed once in two runs on `mariadb-118`. `d` was started with
    `snapshots: never` and missed row 2: the row went in after `d` reported `running` but before
    it read its start position. With `d` on an initial snapshot (D-M8b.12) it passed 4 of 4 runs.
  - The reload scenarios pass on PostgreSQL 18, `mysql-84`, `mysql-84-nogtid` and `mariadb-118`.
  - The whole E2E suite on PostgreSQL 18 at the final tree: 39 of 39 (475 s).
- The workspace after each task: 1156, 1160, 1169, 1174, 1187, 1188, 1192 and 1192 tests (1150
  at the base, `40bb6a3`); `cargo fmt --all --check` and both clippy runs clean at every task.
- Replay: the plan's 58 code blocks, applied in order to a fresh tree at `40bb6a3`, give the
  built tree exactly (`git diff` empty).

**Documents this plan changes:**

- Applied at plan approval, in an `infra:` and a `gap:` commit:
  - Roadmap:
    - §3.6: `SourceConnector::reconfigure` (D-M8b.1).
    - §3.7: what M8b adds (D-M8b.3…7).
    - §3.11: `Command::Reconfigure` (D-M8b.2).
    - §3.12: the config generation gauge, the series relabelled and forgotten, the endpoints
      and the logger (D-M8b.8…10).
    - §4 M8: the M8b plan's scope.
    - §4 M9: the reload and status endpoints exist from M8b, unauthenticated; M9 puts them
      behind auth and RBAC and adds `validate`.
    - §9: D-IMPL-18 for D-M8b.1…12. §9.1: Q-M8b-1…3.
  - `Clustering_Architecture_Research.md` §9, as built:
    - the poll interval and the gossip nudge (§9.2 step 5);
    - the reload's answers (§9.2 step 6);
    - an adoption a node refuses (§9.4);
    - the same text published again when it resolves differently;
    - the coordinator stopped before the leave.
  - `doc/v1.0/README.md`: the M8b row.
  - `CDC_Design_Gap_Analysis_v1.md`: a dated section for M8b's plan:
    - DA-OPS-8 goes 🔄: the startup warning is planned, the operator docs stay with M11;
    - two new gaps: DA-OPS-9 (a pipeline removed while its owner is offline leaves its slot and
      publication) and DA-API-12 (the reload endpoint is unauthenticated until M9).
- **No spec edit.** The specs leave the endpoints' answers open: System-Overview's API table has
  the routes and roles, and Clustering §9.2 the 202. Config §12.3's "orphan-cleanup … whose owner
  was offline is a design item" becomes DA-OPS-9.
- `test-infra/README.md`: the reload scenarios (Task 8).

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-core/src/source.rs` | `SourceConnector::reconfigure` |
| `crates/data-avalanche-source-pg/src/{connector,slot,stream}.rs` | `Live`: the R settings and table patterns under a lock |
| `crates/data-avalanche-source-mysql/src/{connector,catalog,start,stream}.rs` | `Live` in `Shared`, with the stream's catalog connector |
| `crates/data-avalanche-pipeline/src/{status,settings,accumulator,session,worker}.rs` | `Command::Reconfigure`; `Settings::reconfigure`; the session's `reconfigure` (permits, timers, accumulator) |
| `crates/data-avalanche-testkit/src/fake_source.rs` | `reconfigures()`, `teardowns()` |
| `crates/data-avalanche-telemetry/src/lib.rs` | `PipelineMetrics::{relabel, forget}`; `Metrics::set_config_generation`, the gauge |
| `crates/data-avalanche-cluster/src/controller.rs` | `Update`; `updater`; reconfigure, restart, remove and teardown |
| `crates/data-avalanche-cluster/src/reconciler.rs` | `Reload`, `PipelineReload`, `Reloader`; `Loop`, `Cap`, removals and `returning` |
| `crates/data-avalanche-cluster/src/placement.rs` | `Placement::set_stabilization` |
| `crates/data-avalanche-cluster/src/gossip.rs` | `Advertiser`; `Members::newest_generation` |
| `crates/data-avalanche-cluster/src/coordinator.rs` (new) | `ConfigCoordinator`: adoption, the poll and the nudge, the reload |
| `crates/data-avalanche-cluster/src/{test_support,cluster_tests}.rs` | `TestCluster`'s coordinators; the three-node reload |
| `crates/data-avalanche/src/{http,logging,startup,run}.rs`, `Cargo.toml` | The endpoints; the `Logger`; `per_node`; the coordinator in `run` |
| `crates/data-avalanche/tests/e2e/{harness,cluster,reload,main}.rs` | The reload scenarios and their helpers |
| `test-infra/README.md` | The reload scenarios |

---
### Task 1: A source takes its reloadable settings at run time (`data-avalanche-core`, `data-avalanche-source-pg`, `data-avalanche-source-mysql`)

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs`
- Modify: `crates/data-avalanche-source-mysql/src/catalog.rs`
- Modify: `crates/data-avalanche-source-mysql/src/connector.rs`
- Modify: `crates/data-avalanche-source-mysql/src/start.rs`
- Modify: `crates/data-avalanche-source-mysql/src/stream.rs`
- Modify: `crates/data-avalanche-source-mysql/tests/mysql/resolve.rs`
- Modify: `crates/data-avalanche-source-pg/src/connector.rs`
- Modify: `crates/data-avalanche-source-pg/src/slot.rs`
- Modify: `crates/data-avalanche-source-pg/src/stream.rs`
- Modify: `crates/data-avalanche-source-pg/tests/pg/resolve.rs`
- Modify: `crates/data-avalanche-source-pg/tests/pg/slot.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `SourceConnector::reconfigure(&self, pipeline: &dyn std::any::Any)`, a default method that
    ignores its argument (`data-avalanche-core`, `source.rs`). The worker passes a
    `data_avalanche_config::Pipeline` (Task 2).
  - `PgConnector` and `MySqlConnector` implement it: a `Pipeline` with a PostgreSQL (MySQL)
    source replaces their `Live` settings; anything else is ignored.

D-M8b.1. A connector is built once per worker, from the pipeline as it was. Its reloadable
settings move into a `Live` struct under a `std::sync::Mutex`, which `reconfigure` replaces and
each use reads with `live()`:

- PostgreSQL: `heartbeat` (the next stream), `slot_on_orphan` (the next prepare), `auto_cleanup`
  (the teardown), `include` and `exclude` (the next `resolve_tables`).
- MySQL: `binlog_purge_action` (the next prepare and the open dump's purge error), `heartbeat`
  (the next stream), `include` and `exclude`. `Live` sits in the `Shared` the connector has with
  its stream's catalog connector (`sibling`), so both see a reload.

Everything else in the source block is PR (a restart) and stays as built: the slot and replica
id among it, as the unit tests check. The table patterns are R only while the set they resolve to
is unchanged (Task 5 decides). A later rescan must still use the new patterns, or it would pick up
a table the new config excludes; the two `resolve` ITs pin that.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index 9c5e839..fb0f999 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -665,6 +665,7 @@ mod tests {
     #[test]
     fn the_traits_work_as_trait_objects() {
         let source: Box<dyn SourceConnector> = Box::new(OneTxn);
+        source.reconfigure(&"a source with nothing to take ignores it");
         let prepared = block_on(source.prepare(None, &[])).unwrap();
         assert!(matches!(
             prepared.plan,
```

Change `crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
index f2ddee6..612d3fa 100644
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -388,3 +388,66 @@ impl SourceConnector for MySqlConnector {
         Ok(())
     }
 }
+
+#[cfg(test)]
+mod tests {
+    use std::collections::BTreeMap;
+    use std::time::Duration;
+
+    use data_avalanche_config::{BinlogPurgeAction, FingerprintKey, load_str};
+
+    use super::*;
+
+    fn pipeline() -> Pipeline {
+        let yaml = r#"
+config_version: 1
+globals:
+  state: { bucket: app-state }
+pipelines:
+  p:
+    id: p
+    source: { type: mysql, host: db.example.com, db: shop, user: cdc, password: pw, mysql: {} }
+    tables: { include: [t] }
+    sink: { type: delta, bucket: lake, delta: {} }
+"#;
+        load_str(yaml, &BTreeMap::new(), &FingerprintKey::from_bytes([1; 32]))
+            .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
+            .config
+            .pipelines
+            .into_values()
+            .next()
+            .unwrap()
+    }
+
+    /// A reload's purge action, heartbeat and table patterns reach the running connector and
+    /// the stream's catalog connector beside it; the replica id, restart-only, stays, and what
+    /// isn't a pipeline is ignored.
+    #[test]
+    fn a_reload_reaches_the_reloadable_settings() {
+        let p = pipeline();
+        let c = MySqlConnector::new(&p).unwrap();
+        let sibling = c.sibling().unwrap();
+        let mut next = p.clone();
+        let m = next.source.mysql.as_mut().unwrap();
+        m.binlog_purge_action = BinlogPurgeAction::ReSnapshot;
+        m.heartbeat = Duration::from_secs(7);
+        m.server_id = 4242;
+        next.tables.include = vec!["t".into(), "u".into()];
+        next.tables.exclude = vec!["v".into()];
+        c.reconfigure(&next);
+        let live = Live {
+            binlog_purge_action: BinlogPurgeAction::ReSnapshot,
+            heartbeat: Duration::from_secs(7),
+            include: vec!["t".into(), "u".into()],
+            exclude: vec!["v".into()],
+        };
+        assert_eq!(c.live(), live);
+        assert_eq!(sibling.live(), live, "the stream's catalog takes it too");
+        assert_eq!(
+            c.mysql.server_id,
+            p.source.mysql.as_ref().unwrap().server_id
+        );
+        c.reconfigure(&next.source);
+        assert_eq!(c.live(), live, "a source block isn't a pipeline");
+    }
+}
```

Change `crates/data-avalanche-source-mysql/tests/mysql/resolve.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs b/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs
index 71391ba..20184a7 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs
@@ -2,7 +2,7 @@
 //! halts and what is skipped, and each column plans per `Type_Mapping.md` (config §11.2).
 
 use data_avalanche_config::NumericOverflowAction;
-use data_avalanche_core::{ColumnValueType as V, HaltReason, TableRef};
+use data_avalanche_core::{ColumnValueType as V, HaltReason, SourceConnector, TableRef};
 use data_avalanche_source_mysql::{CatalogColumn, Flavour, plan_column, plan_table};
 use data_avalanche_testkit::require_it;
 
@@ -250,3 +250,24 @@ async fn mariadb_types_and_system_versioned_tables() {
     assert_eq!(r.skipped.len(), 1, "a glob skips it");
     cleanup(&n).await;
 }
+
+/// A reload's table patterns take the connector's next resolve (config §1.2: in place while the
+/// set they resolve to stays the same).
+#[tokio::test]
+async fn a_reloaded_include_takes_the_next_resolve() {
+    require_it!();
+    let (n, _a) = fresh(
+        "resolve_reload",
+        "CREATE TABLE a (id INT PRIMARY KEY); CREATE TABLE b (id INT PRIMARY KEY);",
+    )
+    .await;
+    let c = connector(&pipeline(&n, &["a", "b"], "", ""));
+    let names = |r: data_avalanche_core::ResolvedTables| -> Vec<TableRef> {
+        r.tables.iter().map(|s| s.table().clone()).collect()
+    };
+    let t = |name: &str| TableRef::new(&n.db, name);
+    assert_eq!(names(c.resolve_tables().await.unwrap()), [t("a"), t("b")]);
+    c.reconfigure(&pipeline(&n, &["a"], "", ""));
+    assert_eq!(names(c.resolve_tables().await.unwrap()), [t("a")]);
+    cleanup(&n).await;
+}
```

Change `crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index 1e88281..8da6b8d 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -332,6 +332,10 @@ impl SourceConnector for PgConnector {
 
 #[cfg(test)]
 mod tests {
+    use std::time::Duration;
+
+    use data_avalanche_config::SlotOnOrphan;
+
     use super::*;
     use crate::test_support::pipeline;
 
@@ -353,4 +357,47 @@ mod tests {
             SourceError::Permanent("pipeline p: the source isn't PostgreSQL".into())
         );
     }
+
+    /// A reload's heartbeat, orphan policy, cleanup and table patterns reach the running
+    /// connector; the slot, restart-only, stays, and what isn't a pipeline is ignored.
+    #[test]
+    fn a_reload_reaches_the_reloadable_settings() {
+        let p = pipeline("public.t");
+        let c = PgConnector::new(&p).unwrap();
+        let mut next = p.clone();
+        let pg = next.source.postgres.as_mut().unwrap();
+        pg.heartbeat = Duration::from_secs(7);
+        pg.slot_on_orphan = SlotOnOrphan::AdoptResnapshot;
+        pg.auto_cleanup = true;
+        pg.slot = "another_slot".into();
+        next.tables.include = vec!["public.t".into(), "public.u".into()];
+        next.tables.exclude = vec!["public.v".into()];
+        c.reconfigure(&next);
+        let live = Live {
+            heartbeat: Duration::from_secs(7),
+            slot_on_orphan: SlotOnOrphan::AdoptResnapshot,
+            auto_cleanup: true,
+            include: vec!["public.t".into(), "public.u".into()],
+            exclude: vec!["public.v".into()],
+        };
+        assert_eq!(c.live(), live);
+        assert_eq!(c.pg.slot, "p_slot");
+        c.reconfigure(&next.source);
+        assert_eq!(c.live(), live, "a source block isn't a pipeline");
+    }
+
+    /// A teardown after a reload turned `auto_cleanup` off keeps the slot: it doesn't even
+    /// connect (the server here doesn't answer).
+    #[tokio::test]
+    async fn a_teardown_after_a_reload_takes_its_auto_cleanup() {
+        let mut p = pipeline("public.t");
+        p.source.port = 1;
+        p.source.postgres.as_mut().unwrap().auto_cleanup = true;
+        let c = PgConnector::new(&p).unwrap();
+        assert!(c.teardown().await.is_err(), "it connects to clean up");
+        let mut next = p.clone();
+        next.source.postgres.as_mut().unwrap().auto_cleanup = false;
+        c.reconfigure(&next);
+        c.teardown().await.unwrap();
+    }
 }
```

Change `crates/data-avalanche-source-pg/tests/pg/resolve.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/resolve.rs b/crates/data-avalanche-source-pg/tests/pg/resolve.rs
index 664378b..29b80eb 100644
--- a/crates/data-avalanche-source-pg/tests/pg/resolve.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/resolve.rs
@@ -1,6 +1,8 @@
 //! Preflight and channel A: the globs against a live catalog (config §11.2).
 
-use data_avalanche_core::{ColumnValueType as V, HaltReason, LogicalType, SourcePosition};
+use data_avalanche_core::{
+    ColumnValueType as V, HaltReason, LogicalType, SourceConnector, SourcePosition, TableRef,
+};
 use data_avalanche_testkit::require_it;
 
 use crate::common::{admin, cleanup, connector, fresh, pipeline, resolved};
@@ -246,3 +248,26 @@ async fn the_current_position_moves_with_writes() {
     assert!(f > b, "{f} after {b}");
     cleanup(&a, &n).await;
 }
+
+/// A reload's table patterns take the connector's next resolve (config §1.2: in place while the
+/// set they resolve to stays the same).
+#[tokio::test]
+async fn a_reloaded_include_takes_the_next_resolve() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(
+        &a,
+        "resolve_reload",
+        "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY);",
+    )
+    .await;
+    let c = connector(&pipeline(&n, &["*"], "", ""));
+    let names = |r: data_avalanche_core::ResolvedTables| -> Vec<TableRef> {
+        r.tables.iter().map(|s| s.table().clone()).collect()
+    };
+    let t = |name: &str| TableRef::new(&n.schema, name);
+    assert_eq!(names(c.resolve_tables().await.unwrap()), [t("a"), t("b")]);
+    c.reconfigure(&pipeline(&n, &["a"], "", ""));
+    assert_eq!(names(c.resolve_tables().await.unwrap()), [t("a")]);
+    cleanup(&a, &n).await;
+}
```

Change `crates/data-avalanche-source-pg/tests/pg/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/slot.rs b/crates/data-avalanche-source-pg/tests/pg/slot.rs
index 1f1778a..0cd0834 100644
--- a/crates/data-avalanche-source-pg/tests/pg/slot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/slot.rs
@@ -5,7 +5,7 @@ use std::time::Duration;
 
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    Checkpoint, SnapshotReason, SourceError, SourcePosition, StartPlan, TableRef,
+    Checkpoint, SnapshotReason, SourceConnector, SourceError, SourcePosition, StartPlan, TableRef,
 };
 use data_avalanche_testkit::require_it;
 use tokio_postgres::Client;
@@ -139,6 +139,29 @@ async fn an_orphan_halts_by_default_and_is_adopted_on_request() {
     cleanup(&a, &n).await;
 }
 
+/// A reload's `slot_on_orphan` reaches the running connector: its next prepare adopts the slot
+/// the last one halted on.
+#[tokio::test]
+async fn a_reloaded_orphan_policy_takes_the_next_prepare() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(&a, "slot_orphan_reload", DDL).await;
+    a.execute(
+        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
+        &[&n.slot],
+    )
+    .await
+    .unwrap();
+    let tables = [n.table("a")];
+    let c = connector(&pipeline(&n, &["*"], "", ""));
+    permanent(c.prepare(None, &tables).await.unwrap_err(), "orphan");
+    let reloaded = pipeline(&n, &["*"], "", ", slot_on_orphan: adopt_resnapshot");
+    c.reconfigure(&reloaded);
+    let p = c.prepare(None, &tables).await.unwrap();
+    from(&p.plan, SnapshotReason::AdoptedOrphan);
+    cleanup(&a, &n).await;
+}
+
 #[tokio::test]
 async fn require_existing_never_creates() {
     require_it!();
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib connector::`

Expected: FAIL to compile: `reconfigure` isn't a method of the connectors, and `Live` doesn't exist.

```text
error[E0422]: cannot find struct, variant or union type `Live` in this scope
   --> crates/data-avalanche-source-pg/src/connector.rs:376:20
error[E0599]: no method named `reconfigure` found for struct `connector::PgConnector` in the current scope
error[E0599]: no method named `live` found for struct `connector::PgConnector` in the current scope
```

Run: `cargo test -p data-avalanche-source-mysql --lib connector::`

Expected: FAIL to compile: `reconfigure` isn't a method of the connectors, and `Live` doesn't exist.

```text
error[E0422]: cannot find struct, variant or union type `Live` in this scope
   --> crates/data-avalanche-source-mysql/src/connector.rs:438:20
error[E0599]: no method named `reconfigure` found for struct `connector::MySqlConnector` in the current scope
error[E0599]: no method named `live` found for struct `connector::MySqlConnector` in the current scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index fb0f999..242726a 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -1,6 +1,7 @@
 //! The source connector contract (roadmap §3.6, Data-Flow Phases 0–2): what the pipeline asks of a
 //! source engine. Each source crate implements it; nothing here performs I/O.
 
+use std::any::Any;
 use std::fmt;
 
 use async_trait::async_trait;
@@ -399,6 +400,15 @@ pub trait SourceConnector: Send + Sync {
     /// Removes what the source created for the pipeline (PostgreSQL `auto_cleanup`: the slot and
     /// the publication). Called when a pipeline is deleted.
     async fn teardown(&self) -> Result<(), SourceError>;
+
+    /// Takes a reload's settings while the source runs (config §1.2, class R): `pipeline` is
+    /// the reloaded pipeline, a `data_avalanche_config::Pipeline`, which this crate doesn't
+    /// name. A setting read when a connection opens, at a prepare, a resolve or a teardown is
+    /// used from the next one; an open stream keeps what it opened with. A source with nothing
+    /// to take keeps this default, which ignores it.
+    fn reconfigure(&self, pipeline: &dyn Any) {
+        let _ = pipeline;
+    }
 }
 
 /// The change stream of one [`SourceConnector::open_stream`] call.
```

Change `crates/data-avalanche-source-mysql/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/catalog.rs b/crates/data-avalanche-source-mysql/src/catalog.rs
index 0ca5805..a7d853c 100644
--- a/crates/data-avalanche-source-mysql/src/catalog.rs
+++ b/crates/data-avalanche-source-mysql/src/catalog.rs
@@ -278,8 +278,8 @@ impl MySqlConnector {
     /// skipped with a warning; one named without a glob halts.
     pub async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
         let listed = self.list_tables().await?;
-        let t = &self.pipeline.tables;
-        let selection = select(&t.include, &t.exclude, &listed);
+        let live = self.live();
+        let selection = select(&live.include, &live.exclude, &listed);
         let names: Vec<String> = selection
             .tables
             .iter()
@@ -296,7 +296,7 @@ impl MySqlConnector {
             let Some(d) = described.remove(&listed.table.name) else {
                 continue; // dropped since it was listed
             };
-            match plan_table(&d, t, self.tinyint1_as_bool()) {
+            match plan_table(&d, &self.pipeline.tables, self.tinyint1_as_bool()) {
                 Ok(p) => {
                     for (column, ty) in p.notes.read_as_text {
                         out.warnings.push(format!(
```

Change `crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
index 612d3fa..1b261d0 100644
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -1,8 +1,10 @@
 //! `MySqlConnector`: the MySQL or MariaDB source of one pipeline.
 
-use data_avalanche_config::{Mysql, Pipeline, SourceType};
+use data_avalanche_config::{BinlogPurgeAction, Mysql, Pipeline, SourceType, Tables};
+use std::any::Any;
 use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
-use std::sync::{Arc, Mutex};
+use std::sync::{Arc, Mutex, PoisonError};
+use std::time::Duration;
 
 use async_trait::async_trait;
 use data_avalanche_core::{
@@ -19,8 +21,32 @@ use crate::server::{
     grants_replication,
 };
 
+/// The settings a reload changes while the connector runs (config §1.2, class R).
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) struct Live {
+    /// From the next prepare, and for the open stream's next error.
+    pub binlog_purge_action: BinlogPurgeAction,
+    /// `@master_heartbeat_period`, from the next stream.
+    pub heartbeat: Duration,
+    /// `tables.include` and `tables.exclude`, from the next resolve: in place only while the
+    /// table set they resolve to is the same (the pipeline restarts otherwise).
+    pub include: Vec<String>,
+    pub exclude: Vec<String>,
+}
+
+impl Live {
+    fn of(m: &Mysql, tables: &Tables) -> Self {
+        Self {
+            binlog_purge_action: m.binlog_purge_action,
+            heartbeat: m.heartbeat,
+            include: tables.include.clone(),
+            exclude: tables.exclude.clone(),
+        }
+    }
+}
+
 /// What a connector and its stream share.
-#[derive(Debug, Default)]
+#[derive(Debug)]
 pub(crate) struct Shared {
     /// The change the stream read for a schema signal, kept for the pipeline's `augment` of the
     /// same signal: one catalog read per statement, and one answer for both.
@@ -29,6 +55,19 @@ pub(crate) struct Shared {
     pub(crate) dump: AtomicU32,
     /// `lower_case_table_names` isn't 0, as the stream read it at its open.
     fold: AtomicBool,
+    /// The reloadable settings in force.
+    live: Mutex<Live>,
+}
+
+impl Shared {
+    fn new(m: &Mysql, tables: &Tables) -> Self {
+        Self {
+            remembered: Mutex::default(),
+            dump: AtomicU32::default(),
+            fold: AtomicBool::default(),
+            live: Mutex::new(Live::of(m, tables)),
+        }
+    }
 }
 
 #[derive(Debug)]
@@ -100,13 +139,22 @@ impl MySqlConnector {
         };
         Ok(Self {
             pipeline: pipeline.clone(),
+            shared: Arc::new(Shared::new(&mysql, &pipeline.tables)),
             mysql,
             conn: Connector::new(source)?,
             version: OnceCell::new(),
-            shared: Arc::default(),
         })
     }
 
+    /// The reloadable settings in force, the same for every sibling.
+    pub(crate) fn live(&self) -> Live {
+        self.shared
+            .live
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner)
+            .clone()
+    }
+
     /// A connector for the stream's catalog reads: its own catalog connection, the same
     /// configuration, version and shared state.
     pub(crate) fn sibling(&self) -> Result<Self, SourceError> {
@@ -387,6 +435,18 @@ impl SourceConnector for MySqlConnector {
     async fn teardown(&self) -> Result<(), SourceError> {
         Ok(())
     }
+
+    fn reconfigure(&self, pipeline: &dyn Any) {
+        if let Some(p) = pipeline.downcast_ref::<Pipeline>()
+            && let Some(m) = p.source.mysql.as_ref()
+        {
+            *self
+                .shared
+                .live
+                .lock()
+                .unwrap_or_else(PoisonError::into_inner) = Live::of(m, &p.tables);
+        }
+    }
 }
 
 #[cfg(test)]
```

Change `crates/data-avalanche-source-mysql/src/start.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/start.rs b/crates/data-avalanche-source-mysql/src/start.rs
index 1c66f98..0cbfdd8 100644
--- a/crates/data-avalanche-source-mysql/src/start.rs
+++ b/crates/data-avalanche-source-mysql/src/start.rs
@@ -96,7 +96,7 @@ impl MySqlConnector {
         };
         if let Some(why) = lost {
             let position = cp.position.to_tagged();
-            if self.mysql.binlog_purge_action == BinlogPurgeAction::ReSnapshot {
+            if self.live().binlog_purge_action == BinlogPurgeAction::ReSnapshot {
                 return Ok(Prepared {
                     plan: StartPlan::Snapshot {
                         from: self.position_now().await?,
@@ -112,7 +112,7 @@ impl MySqlConnector {
                 });
             }
             return Err(purge_action(
-                self.mysql.binlog_purge_action,
+                self.live().binlog_purge_action,
                 SourceError::PositionUnreachable {
                     position,
                     detail: why,
```

Change `crates/data-avalanche-source-mysql/src/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/stream.rs b/crates/data-avalanche-source-mysql/src/stream.rs
index 15ce895..62673ff 100644
--- a/crates/data-avalanche-source-mysql/src/stream.rs
+++ b/crates/data-avalanche-source-mysql/src/stream.rs
@@ -702,13 +702,14 @@ impl MySqlConnector {
             tracked.insert(s.table().clone(), t);
         }
         let at = from.to_tagged();
-        let action = self.mysql.binlog_purge_action;
+        let live = self.live();
+        let action = live.binlog_purge_action;
         let err = |e| purge_action(action, mysql(e, Some(&at)));
         let mut conn = self.conn.connect().await?;
         let id = conn.id();
         conn.query_drop(format!(
             "SET @master_heartbeat_period = {}",
-            self.mysql.heartbeat.as_nanos()
+            live.heartbeat.as_nanos()
         ))
         .await
         .map_err(err)?;
@@ -1351,7 +1352,7 @@ impl ChangeStream for MySqlStream {
             let ev = match self.events.next().await {
                 Some(Ok(ev)) => ev,
                 Some(Err(e)) => {
-                    let action = self.catalog.mysql.binlog_purge_action;
+                    let action = self.catalog.live().binlog_purge_action;
                     return Err(purge_action(action, mysql(e, Some(&at))));
                 }
                 None => {
```

Change `crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index 8da6b8d..159208c 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -1,10 +1,13 @@
 //! `PgConnector`: the PostgreSQL source of one pipeline.
 
+use std::any::Any;
 use std::collections::BTreeMap;
+use std::sync::PoisonError;
+use std::time::Duration;
 
 use async_trait::async_trait;
 use data_avalanche_config::{
-    Pipeline, Postgres, SourceType, check_table_set, check_unbounded_numeric,
+    Pipeline, Postgres, SlotOnOrphan, SourceType, Tables, check_table_set, check_unbounded_numeric,
 };
 use data_avalanche_core::{
     ChangeStream, Checkpoint, Chunk, HaltReason, InFlightTxn, KeysetCursor, Preflight, Prepared,
@@ -21,15 +24,44 @@ use crate::types::TypeCatalog;
 /// The oldest server the source supports (P-8).
 pub const MIN_SERVER_VERSION: i32 = 140_000;
 
+/// The settings a reload changes while the connector runs (config §1.2, class R).
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) struct Live {
+    /// The replication connection's status interval, from the next stream.
+    pub heartbeat: Duration,
+    /// From the next prepare.
+    pub slot_on_orphan: SlotOnOrphan,
+    /// At the teardown.
+    pub auto_cleanup: bool,
+    /// `tables.include` and `tables.exclude`, from the next resolve: in place only while the
+    /// table set they resolve to is the same (the pipeline restarts otherwise).
+    pub include: Vec<String>,
+    pub exclude: Vec<String>,
+}
+
+impl Live {
+    fn of(pg: &Postgres, tables: &Tables) -> Self {
+        Self {
+            heartbeat: pg.heartbeat,
+            slot_on_orphan: pg.slot_on_orphan,
+            auto_cleanup: pg.auto_cleanup,
+            include: tables.include.clone(),
+            exclude: tables.exclude.clone(),
+        }
+    }
+}
+
 /// The PostgreSQL source of one pipeline (roadmap §3.6). Built from the resolved pipeline
 /// configuration; connections open on first use.
 #[derive(Debug)]
 pub struct PgConnector {
     pub(crate) pipeline: Pipeline,
+    /// As built: a reload's settings are in `live`.
     pub(crate) pg: Postgres,
     pub(crate) conn: Connector,
     /// `pg_type` rows fetched so far.
     pub(crate) types: Mutex<TypeCatalog>,
+    live: std::sync::Mutex<Live>,
 }
 
 impl PgConnector {
@@ -47,12 +79,21 @@ impl PgConnector {
         };
         Ok(Self {
             pipeline: pipeline.clone(),
+            live: std::sync::Mutex::new(Live::of(&pg, &pipeline.tables)),
             pg,
             conn: Connector::new(source)?,
             types: Mutex::new(TypeCatalog::new()),
         })
     }
 
+    /// The reloadable settings in force.
+    pub(crate) fn live(&self) -> Live {
+        self.live
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner)
+            .clone()
+    }
+
     /// The pipeline-level start checks (config §11.2, `Schema_Change…` §9): the server is
     /// PostgreSQL 14 or newer with `wal_level = logical`, the database encoding is UTF8 (D-M5.8),
     /// and the user can read the catalogs channels A/C/D query. A user without the `REPLICATION`
@@ -168,8 +209,8 @@ impl PgConnector {
             .conn
             .command("list tables", catalog::relations(&client))
             .await?;
-        let t = &self.pipeline.tables;
-        let selection = catalog::select(&t.include, &t.exclude, &relations);
+        let live = self.live();
+        let selection = catalog::select(&live.include, &live.exclude, &relations);
         let oids: Vec<u32> = selection.tables.iter().map(|(r, _)| r.oid).collect();
         let mut schemas = self.schemas(&oids).await?;
         let mut out = ResolvedTables {
@@ -328,6 +369,14 @@ impl SourceConnector for PgConnector {
     async fn teardown(&self) -> Result<(), SourceError> {
         PgConnector::teardown(self).await
     }
+
+    fn reconfigure(&self, pipeline: &dyn Any) {
+        if let Some(p) = pipeline.downcast_ref::<Pipeline>()
+            && let Some(pg) = p.source.postgres.as_ref()
+        {
+            *self.live.lock().unwrap_or_else(PoisonError::into_inner) = Live::of(pg, &p.tables);
+        }
+    }
 }
 
 #[cfg(test)]
```

Change `crates/data-avalanche-source-pg/src/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
index 50a8220..5d65c2e 100644
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -425,7 +425,7 @@ impl PgConnector {
             &self.pipeline.source.db,
             self.slot_name(),
             self.pg.slot_management,
-            self.pg.slot_on_orphan,
+            self.live().slot_on_orphan,
         ) {
             // pgoutput reads the publication as of each change's WAL position: the WAL from the
             // checkpoint up to now was decoded under the old publication, and what it didn't
@@ -502,7 +502,7 @@ impl PgConnector {
     /// otherwise (`require_existing` forces `auto_cleanup: false`, V-103). A slot still streaming
     /// is an error.
     pub async fn teardown(&self) -> Result<(), SourceError> {
-        if !self.pg.auto_cleanup || self.pg.slot_management == SlotManagement::RequireExisting {
+        if !self.live().auto_cleanup || self.pg.slot_management == SlotManagement::RequireExisting {
             return Ok(());
         }
         let client = self.conn.client().await?;
```

Change `crates/data-avalanche-source-pg/src/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/stream.rs b/crates/data-avalanche-source-pg/src/stream.rs
index 093b96a..4b1fe82 100644
--- a/crates/data-avalanche-source-pg/src/stream.rs
+++ b/crates/data-avalanche-source-pg/src/stream.rs
@@ -149,7 +149,7 @@ impl PgConnector {
             &self.pg.slot,
             &self.pg.publication,
             to_wire(start),
-            self.pg.heartbeat,
+            self.live().heartbeat,
         );
         let client = ReplicationClient::connect(config)
             .await
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib connector::`

Expected: PASS, 4 tests.

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 81 filtered out; finished in 0.00s
```

Run: `cargo test -p data-avalanche-source-mysql --lib connector::`

Expected: PASS, 1 test.

```text
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 87 filtered out; finished in 0.01s
```

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-source-pg --test pg -- slot::a_reloaded resolve::a_reloaded`

Expected: PASS, 2 tests.

```text
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 56 filtered out; finished in 0.19s
```

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_MYSQL=mysql-84 cargo test -p data-avalanche-source-mysql --test mysql -- resolve::a_reloaded`

Expected: PASS, 1 test.

```text
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 44 filtered out; finished in 0.13s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1156 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core/src/source.rs \
  crates/data-avalanche-source-mysql/src/catalog.rs \
  crates/data-avalanche-source-mysql/src/connector.rs \
  crates/data-avalanche-source-mysql/src/start.rs \
  crates/data-avalanche-source-mysql/src/stream.rs \
  crates/data-avalanche-source-mysql/tests/mysql/resolve.rs \
  crates/data-avalanche-source-pg/src/connector.rs \
  crates/data-avalanche-source-pg/src/slot.rs \
  crates/data-avalanche-source-pg/src/stream.rs \
  crates/data-avalanche-source-pg/tests/pg/resolve.rs \
  crates/data-avalanche-source-pg/tests/pg/slot.rs
git commit -m "feat(source): a reload's settings reach the running source — PostgreSQL's heartbeat, orphan policy, auto_cleanup and table patterns, MySQL's purge action, heartbeat and table patterns"
```

---

### Task 2: The worker takes R fields in place (`data-avalanche-pipeline`)

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/accumulator.rs`
- Modify: `crates/data-avalanche-pipeline/src/session.rs`
- Modify: `crates/data-avalanche-pipeline/src/settings.rs`
- Modify: `crates/data-avalanche-pipeline/src/status.rs`
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`

**Interfaces:**
- Consumes: `SourceConnector::reconfigure` (Task 1).
- Produces:
  - `Command::Reconfigure(Box<Pipeline>)`; `Command` derives `Debug, Clone, PartialEq` (no longer
    `Copy` or `Eq`).
  - `Settings::reconfigure(&mut self, p: &Pipeline)`.
  - `Accumulator::set_lineage(&mut self, lineage: Lineage)`.
  - testkit: `FakeSource::reconfigures(&self) -> usize`, the `reconfigure` calls so far.

D-M8b.2. The worker builds `Settings` once from the pipeline (M6a); `Settings::reconfigure`
copies in the R fields of a new `Settings::new(p)`, leaving the rest as built. The session's
`reconfigure` then:

- sets the accumulator's flush policy, transaction limits and lineage (the pipeline's name is in
  every row's lineage);
- installs a new buffer semaphore sized to the new `buffer_capacity_rows`, and clamps every queued
  need to the new cap, so a waiter larger than a shrunk buffer isn't stuck;
- marks the timers to be re-armed (`retimed`): the flush, rescan and heartbeat intervals take the
  new values from the next tick;
- tells the source (Task 1).

A `Reconfigure` that arrives while the worker reconnects (`drained_while`) is applied to the
`Core`, so the next session starts with it.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index a91b446..b0ce6c9 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -1955,4 +1955,147 @@ mod tests {
         let b = w.docs.table_snapshot(&t("b")).await.unwrap().unwrap().doc;
         assert_eq!(b.status, SnapshotStatus::InProgress);
     }
+
+    /// A pipeline with `sink` entries and pipeline-level `top` YAML (a line of its own).
+    fn reloadable(sink: &str, top: &str) -> Pipeline {
+        Yaml {
+            tables: "\n      snapshots: { mode: never }",
+            sink,
+            pipeline: top,
+            ..Yaml::default()
+        }
+        .build()
+    }
+
+    /// A reload reaches the running worker (config §1.2, class R): the new flush policy flushes
+    /// the row the old one held, the rows carry the new name, and the source takes its settings,
+    /// all in the same session.
+    #[tokio::test]
+    async fn a_reload_reaches_the_running_worker() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let held = reloadable("flush: { seconds: 3600, rows: 1000 }", "");
+        let (worker, handle) = PipelineWorker::new(&held, deps(&w), fast());
+        let driver = async {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            tokio::time::sleep(Duration::from_millis(100)).await;
+            assert!(w.ids("a").is_empty(), "held by the flush policy");
+            let mut next = reloadable("flush: { seconds: 0 }", "");
+            next.name = "renamed".into();
+            handle
+                .control
+                .send(Command::Reconfigure(Box::new(next)))
+                .await
+                .unwrap();
+            wait_until("the held row lands", || w.ids("a") == [1]).await;
+            assert_eq!(w.text("a", "_pipeline"), [Some("renamed".to_owned())]);
+            assert_eq!(w.source.reconfigures(), 1);
+            assert_eq!(w.source.opened().len(), 1, "the same session");
+            handle.control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(worker.run(), driver);
+        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
+    }
+
+    /// A reload re-arms the session's timers: periodic rescans, off when the session opened, run
+    /// at the new interval.
+    #[tokio::test]
+    async fn a_reload_rearms_the_rescans() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let off = "\n    schema_evolution: { rescan_interval: 0s }";
+        let p = reloadable("flush: { seconds: 0 }", off);
+        let (worker, handle) = PipelineWorker::new(&p, deps(&w), fast());
+        let driver = async {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            wait_until("streaming", || w.ids("a") == [1]).await;
+            tokio::time::sleep(Duration::from_millis(100)).await;
+            assert_eq!(w.source.rescans(), 0, "rescans are off");
+            let every = "\n    schema_evolution: { rescan_interval: 1s }";
+            let next = reloadable("flush: { seconds: 0 }", every);
+            handle
+                .control
+                .send(Command::Reconfigure(Box::new(next)))
+                .await
+                .unwrap();
+            wait_until("a rescan runs", || w.source.rescans() > 0).await;
+            handle.control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(worker.run(), driver);
+        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
+    }
+
+    /// A buffer made smaller than a flush that waits for room lets it through once there is room
+    /// under the new size: the wait is capped at the new size, so it can't wait for ever.
+    #[tokio::test]
+    async fn a_smaller_buffer_lets_a_waiting_flush_through() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let p = reloadable("flush: { seconds: 0 }, buffer_capacity_rows: 4", "");
+        let (worker, handle) = PipelineWorker::new(&p, deps(&w), fast());
+        let driver = async {
+            w.sink.hold_commits();
+            let mut four = FakeTxn::new();
+            for id in 1..=4 {
+                four = four.insert(&t("a"), row(id, "x"));
+            }
+            w.source.commit(four);
+            wait_until("the first flush waits", || w.sink.waiting_commits() == 1).await;
+            let mut three = FakeTxn::new();
+            for id in 5..=7 {
+                three = three.insert(&t("a"), row(id, "x"));
+            }
+            w.source.commit(three);
+            tokio::time::sleep(Duration::from_millis(100)).await;
+            let next = reloadable("flush: { seconds: 0 }, buffer_capacity_rows: 2", "");
+            handle
+                .control
+                .send(Command::Reconfigure(Box::new(next)))
+                .await
+                .unwrap();
+            w.sink.release_commits();
+            wait_until("every row lands", || w.ids("a") == [1, 2, 3, 4, 5, 6, 7]).await;
+            handle.control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(worker.run(), driver);
+        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
+    }
+
+    /// A reload that arrives between sessions (the reconnect's backoff) is the next session's.
+    #[tokio::test]
+    async fn a_reload_during_a_reconnect_reaches_the_next_session() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let tuning = Tuning {
+            source_backoff: Backoff {
+                first: Duration::from_millis(300),
+                max: Duration::from_millis(300),
+            },
+            ..fast()
+        };
+        let p = reloadable("flush: { seconds: 0 }", "");
+        let (worker, handle) = PipelineWorker::new(&p, deps(&w), tuning);
+        let driver = async {
+            w.source
+                .fail(Method::Next, SourceError::Transient("reset".into()));
+            wait_until("reconnecting", || {
+                matches!(handle.status.borrow().phase, Phase::Reconnecting { .. })
+            })
+            .await;
+            let mut next = reloadable("flush: { seconds: 0 }", "");
+            next.name = "renamed".into();
+            handle
+                .control
+                .send(Command::Reconfigure(Box::new(next)))
+                .await
+                .unwrap();
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            wait_until("the row lands", || w.ids("a") == [1]).await;
+            assert_eq!(w.text("a", "_pipeline"), [Some("renamed".to_owned())]);
+            assert_eq!(w.source.reconfigures(), 1);
+            handle.control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(worker.run(), driver);
+        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
+    }
 }
```

Change `crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index 44e39d8..787b407 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -253,6 +253,7 @@ struct Db {
     rescans: usize,
     in_flights: usize,
     readers_ended: usize,
+    reconfigures: usize,
     fetches: BTreeMap<TableRef, usize>,
     prepares: Vec<Option<Checkpoint>>,
 }
@@ -306,6 +307,7 @@ impl FakeSource {
             rescans: 0,
             in_flights: 0,
             readers_ended: 0,
+            reconfigures: 0,
             fetches: BTreeMap::new(),
             prepares: vec![],
         };
@@ -583,6 +585,11 @@ impl FakeSource {
         self.db().readers_ended
     }
 
+    /// `reconfigure` calls so far: a reload's settings handed to the running source.
+    pub fn reconfigures(&self) -> usize {
+        self.db().reconfigures
+    }
+
     /// `fetch_rows` calls for `table` so far, failed ones included.
     pub fn fetches(&self, table: &TableRef) -> usize {
         self.db().fetches.get(table).copied().unwrap_or(0)
@@ -1162,6 +1169,10 @@ impl SourceConnector for FakeSource {
     async fn teardown(&self) -> Result<(), SourceError> {
         Ok(())
     }
+
+    fn reconfigure(&self, _: &dyn std::any::Any) {
+        self.db().reconfigures += 1;
+    }
 }
 
 /// The stream of one `open_stream`: the log after its start, then whatever is committed later.
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-pipeline --lib worker::`

Expected: FAIL to compile: no `Command::Reconfigure`, no `FakeSource::reconfigures`.

```text
error[E0599]: no variant, associated function, or constant named `Reconfigure` found for enum `status::Command` in the current scope
    --> crates/data-avalanche-pipeline/src/worker.rs:1987:32
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-pipeline/src/accumulator.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/accumulator.rs b/crates/data-avalanche-pipeline/src/accumulator.rs
index a48e24c..979ad3b 100644
--- a/crates/data-avalanche-pipeline/src/accumulator.rs
+++ b/crates/data-avalanche-pipeline/src/accumulator.rs
@@ -77,6 +77,11 @@ impl Accumulator {
         self.policy = policy;
     }
 
+    /// The lineage the next flushes write (a renamed pipeline, config §12.3).
+    pub fn set_lineage(&mut self, lineage: Lineage) {
+        self.lineage = lineage;
+    }
+
     /// The transaction limits (reloadable, like the flush policy). The default never splits.
     pub fn set_txn_limits(&mut self, limits: TxnLimits) {
         self.limits = limits;
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 198db39..e032114 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -13,6 +13,7 @@ use std::collections::{BTreeMap, BTreeSet, VecDeque};
 use std::sync::Arc;
 use std::time::Instant;
 
+use data_avalanche_config::Pipeline;
 use data_avalanche_core::{
     CdcEvent, ChangeStream, ColumnValue, HaltReason, Op, RescanReport, RetainedLog, Row,
     SchemaHint, ServerIdentity, Sink, SourceConnector, SourceError, SourcePosition, SourceSeq,
@@ -103,6 +104,12 @@ impl Core {
         }
     }
 
+    /// A reload's settings (config §1.2, class R): the worker's, and the source's.
+    pub fn reconfigure(&mut self, p: &Pipeline) {
+        self.settings.reconfigure(p);
+        self.source.reconfigure(p);
+    }
+
     pub fn evolver(&self) -> Evolver<'_> {
         Evolver {
             source: &*self.source,
@@ -124,6 +131,24 @@ pub(crate) enum End {
     Source(SourceError),
 }
 
+/// `buffer_capacity_rows` as semaphore permits.
+fn buffer_permits(rows: u64) -> usize {
+    usize::try_from(rows)
+        .unwrap_or(usize::MAX)
+        .min(Semaphore::MAX_PERMITS)
+}
+
+/// The heartbeat's and the rescans' timers (none with rescans off), each first due one interval
+/// from now.
+fn timers(s: &Settings) -> (tokio::time::Interval, Option<tokio::time::Interval>) {
+    let every = |every| {
+        let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
+        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
+        timer
+    };
+    (every(s.heartbeat), s.rescan_interval.map(every))
+}
+
 fn protocol(detail: String) -> WorkerError {
     AccumulatorError::Protocol { detail }.into()
 }
@@ -184,6 +209,8 @@ pub(crate) struct Session<'c> {
     /// that doesn't answer holds up neither the stream nor a drain (DA-SRC-16).
     retained_read: JoinSet<Result<Option<RetainedLog>, SourceError>>,
     watchdog: Watchdog,
+    /// A reload changed the heartbeat or the rescan interval: the loop re-arms its timers.
+    retimed: bool,
 }
 
 impl<'c> Session<'c> {
@@ -262,9 +289,7 @@ impl<'c> Session<'c> {
             backoff: core.tuning.sink_backoff,
         };
         let writer = tokio::spawn(writer::run(ctx, jobs_rx, events_tx));
-        let rows = usize::try_from(core.settings.buffer_rows)
-            .unwrap_or(usize::MAX)
-            .min(Semaphore::MAX_PERMITS);
+        let rows = buffer_permits(core.settings.buffer_rows);
         core.update(|st| {
             st.phase = Phase::Streaming;
             st.checkpoint = Some(checkpoint.clone());
@@ -300,6 +325,7 @@ impl<'c> Session<'c> {
             paused: None,
             retained_read: JoinSet::new(),
             watchdog: Watchdog::default(),
+            retimed: false,
         }))
     }
 
@@ -355,16 +381,12 @@ impl<'c> Session<'c> {
     async fn event_loop(&mut self) -> Result<End, WorkerError> {
         let mut tick = tokio::time::interval(self.core.tuning.tick);
         tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
-        let every = self.core.settings.heartbeat;
-        let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
-        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
-        let mut rescan = self.core.settings.rescan_interval.map(|every| {
-            let mut rescan = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
-            rescan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
-            rescan
-        });
+        let (mut heartbeat, mut rescan) = timers(&self.core.settings);
         self.issue_reads();
         loop {
+            if std::mem::take(&mut self.retimed) {
+                (heartbeat, rescan) = timers(&self.core.settings);
+            }
             if self.dumped() {
                 self.flush()?;
                 if let Some(end) = self.until_durable().await? {
@@ -457,6 +479,25 @@ impl<'c> Session<'c> {
         }
     }
 
+    /// Takes a reload's settings in place. The buffer gets a semaphore of the new size: what is
+    /// handed on holds the old one's permits until it is durable, so for that while the buffer
+    /// may hold up to the old size's more; a flush waiting for room waits for at most the new
+    /// size, so it can't wait for ever.
+    fn reconfigure(&mut self, p: &Pipeline) {
+        self.core.reconfigure(p);
+        let s = &self.core.settings;
+        self.acc.set_policy(s.flush);
+        self.acc.set_txn_limits(s.limits);
+        self.acc.set_lineage(s.lineage.clone());
+        let rows = buffer_permits(s.buffer_rows);
+        self.permits = Arc::new(Semaphore::new(rows));
+        let cap = u32::try_from(rows).unwrap_or(u32::MAX);
+        for (_, need) in &mut self.queued {
+            *need = (*need).min(cap);
+        }
+        self.retimed = true;
+    }
+
     fn command(&mut self, cmd: Option<Command>) -> Result<(), WorkerError> {
         match cmd {
             Some(Command::Flush) => self.flush(),
@@ -469,6 +510,10 @@ impl<'c> Session<'c> {
                 self.core.pause = true;
                 Ok(())
             }
+            Some(Command::Reconfigure(p)) => {
+                self.reconfigure(&p);
+                Ok(())
+            }
             Some(Command::Resume) => {
                 self.core.pause = false;
                 if let Some(at) = self.paused.take() {
```

Change `crates/data-avalanche-pipeline/src/settings.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/settings.rs b/crates/data-avalanche-pipeline/src/settings.rs
index 1960ce0..eb91fa8 100644
--- a/crates/data-avalanche-pipeline/src/settings.rs
+++ b/crates/data-avalanche-pipeline/src/settings.rs
@@ -131,6 +131,21 @@ impl Settings {
         }
     }
 
+    /// Takes `p`'s reloadable fields (config §1.2, class R); the others stay as built.
+    pub fn reconfigure(&mut self, p: &Pipeline) {
+        let next = Self::new(p);
+        self.lineage.pipeline = next.lineage.pipeline;
+        self.flush = next.flush;
+        self.limits = next.limits;
+        self.buffer_rows = next.buffer_rows;
+        self.parallelism = next.parallelism;
+        self.chunk_size = next.chunk_size;
+        self.heartbeat = next.heartbeat;
+        self.max_wal_lag_bytes = next.max_wal_lag_bytes;
+        self.rescan_interval = next.rescan_interval;
+        self.evolution = next.evolution;
+    }
+
     /// The table's key in the `tables` maps: `schema.table` for PostgreSQL, the bare name for
     /// MySQL (V-064).
     fn key(&self, t: &TableRef) -> String {
```

Change `crates/data-avalanche-pipeline/src/status.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
index 89351e9..6c02a3c 100644
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -4,13 +4,14 @@
 use std::collections::BTreeMap;
 use std::time::Duration;
 
+use data_avalanche_config::Pipeline;
 use data_avalanche_core::{HaltReason, SourcePosition, TableRef};
 use tokio::sync::watch;
 
 use crate::Backoff;
 
 /// A command to a running worker.
-#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+#[derive(Debug, Clone, PartialEq)]
 pub enum Command {
     /// Finish the open transaction, flush, make it durable, close the stream and end with
     /// [`Exit::Drained`] (stop and failover hand-off, Pipeline_Lifecycle §2, §4).
@@ -24,6 +25,12 @@ pub enum Command {
     Pause,
     /// Read again after a [`Command::Pause`].
     Resume,
+    /// A reload's settings (config §1.2, class R), taken in place: the flush policy, the
+    /// transaction limits, the buffer, the name in the rows' lineage, the snapshot parallelism and
+    /// chunk size, the heartbeat, the WAL-lag limit and schema evolution, and the source's own
+    /// (`SourceConnector::reconfigure`). The pipeline's other fields are restart-only: a change
+    /// to one restarts the worker instead.
+    Reconfigure(Box<Pipeline>),
 }
 
 /// How a worker ended without an error.
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index b0ce6c9..f7684a6 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -249,9 +249,10 @@ async fn drained_while(core: &mut Core, delay: std::time::Duration) -> bool {
             () = &mut sleep => return false,
             cmd = core.control.recv() => match cmd {
                 Some(Command::Flush) => {}
-                // The next session parks (or reads) as it opens.
+                // The next session parks (or reads) as it opens, with the reload's settings.
                 Some(Command::Pause) => core.pause = true,
                 Some(Command::Resume) => core.pause = false,
+                Some(Command::Reconfigure(p)) => core.reconfigure(&p),
                 Some(Command::Drain) | None => return true,
             },
         }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-pipeline --lib worker::`

Expected: PASS, 42 tests.

```text
test result: ok. 42 passed; 0 failed; 0 ignored; 0 measured; 174 filtered out; finished in 1.13s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1160 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline/src/accumulator.rs \
  crates/data-avalanche-pipeline/src/session.rs \
  crates/data-avalanche-pipeline/src/settings.rs \
  crates/data-avalanche-pipeline/src/status.rs \
  crates/data-avalanche-pipeline/src/worker.rs \
  crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "feat(pipeline): Command::Reconfigure — the running worker takes a reload's R fields: settings, accumulator, buffer permits, timers, the source"
```

---

### Task 3: The controller takes a reload's update (`data-avalanche-cluster`, `data-avalanche-telemetry`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/controller.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche-cluster/src/test_support.rs`
- Modify: `crates/data-avalanche-telemetry/src/lib.rs`
- Modify: `crates/data-avalanche-telemetry/tests/metrics.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`

**Interfaces:**
- Consumes: `Command::Reconfigure` (Task 2); `SourceConnector::teardown` (M5).
- Produces:
  - `pub enum Update { Reconfigure(Box<Pipeline>), Restart(Box<Pipeline>, PipelineDocs), Remove, DrainTimeout(Duration) }`
    (`controller.rs`, re-exported).
  - `PipelineController::updater(&self) -> mpsc::UnboundedSender<Update>`; `run(mut self, target)`
    now also ends when an `Update::Remove` is done.
  - telemetry: `PipelineMetrics::relabel(&self, labels: PipelineLabels, per_table: bool)` and
    `PipelineMetrics::forget(&self)`.
  - testkit: `FakeSource::teardowns(&self) -> usize`.
  - test_support: `Node.updates`, a controller's updater.

D-M8b.3. A controller now selects on its updates too, whether it owns its pipeline or stands by.

- `Reconfigure`: the controller keeps the pipeline, relabels its series (`relabel`: every series
  under the old labels goes, renew failures included), and forwards `Command::Reconfigure` to a
  running worker.
- `Restart`: the controller takes the pipeline and documents, aborts the running worker, and lets
  the loop start the next one under the same `LeaseRun`. It clears `error`, `dumped`, the retry
  and the failure count, and reads the desired state again, so a paused or stopped pipeline stays
  so.
- `Remove` while owned: `remove` drains the worker (at most `drain_timeout`). If the drain finished
  and the guard may still commit, `teardown` builds a source from the latest pipeline, calls
  `teardown()`, and retries `Transient` or `Contended` errors, backing off by `restart`, within
  `tuning.contended_window`. Then the lease is released. A drain cut off, or a lost lease, stops
  the run and leaves the source as it is, with a warning. `forget` drops the series and the loop
  returns `Left::Removed`. On standby `Remove` just forgets the series and ends.
- `DrainTimeout` sets the controller's drain timeout.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 7370bdf..a17c509 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1341,4 +1341,166 @@ mod tests {
         until(&mut n.status, "running", is(Actual::Running)).await;
         assert_eq!(n.status.borrow().owner_epoch, Some(2));
     }
+
+    /// A reload's reloadable change reaches the running worker in place (config §1.2): the same
+    /// worker, the source told, and the series under the new name only.
+    #[tokio::test(start_paused = true)]
+    async fn a_reconfigure_reaches_the_running_worker() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        let mut renamed = pipeline();
+        renamed.name = "renamed".into();
+        n.updates
+            .send(Update::Reconfigure(Box::new(renamed)))
+            .unwrap();
+        eventually("the source takes it", || f.source.reconfigures() == 1).await;
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
+        eventually("the row lands", || f.ids("a") == [1]).await;
+        assert_eq!(f.builds(), 1, "the same worker");
+        let text = n.metrics.encode();
+        assert!(!text.contains(r#"pipeline="p""#), "{text}");
+        assert!(
+            text.contains(r#"data_avalanche_pipeline_status{pipeline="renamed","#),
+            "{text}"
+        );
+    }
+
+    /// A restart-only change discards the worker and runs a new one from the checkpoint, under
+    /// the same lease.
+    #[tokio::test(start_paused = true)]
+    async fn a_restart_reruns_the_worker_under_the_same_lease() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        n.updates
+            .send(Update::Restart(Box::new(pipeline()), docs.clone()))
+            .unwrap();
+        eventually("a new worker", || f.builds() == 2).await;
+        until(&mut n.status, "running again", is(Actual::Running)).await;
+        assert_eq!(n.status.borrow().owner_epoch, Some(1), "the same lease");
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
+        eventually("the row lands", || f.ids("a") == [1]).await;
+    }
+
+    /// A restart runs a pipeline in `ERROR` again: the reloaded config may be the fix.
+    #[tokio::test(start_paused = true)]
+    async fn a_restart_runs_a_pipeline_in_error_again() {
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
+        n.updates
+            .send(Update::Restart(Box::new(pipeline()), docs.clone()))
+            .unwrap();
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        assert_eq!(f.builds(), 2);
+    }
+
+    /// A restart doesn't resume a paused pipeline: the new worker parks.
+    #[tokio::test(start_paused = true)]
+    async fn a_restart_keeps_a_paused_pipeline_paused() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        docs.set_desired_state(DesiredState::Paused).await.unwrap();
+        until(&mut n.status, "paused", is(Actual::Paused)).await;
+        n.updates
+            .send(Update::Restart(Box::new(pipeline()), docs.clone()))
+            .unwrap();
+        eventually("a new worker", || f.builds() == 2).await;
+        until(&mut n.status, "paused again", is(Actual::Paused)).await;
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
+        tokio::time::sleep(Duration::from_secs(30)).await;
+        assert!(f.ids("a").is_empty(), "nothing is read while paused");
+    }
+
+    /// A removed pipeline's worker drains, its source cleans up, its lease is released and its
+    /// series go; the controller ends.
+    #[tokio::test(start_paused = true)]
+    async fn a_removed_pipeline_drains_cleans_up_and_releases() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
+        n.updates.send(Update::Remove).unwrap();
+        tokio::time::timeout(Duration::from_secs(60), &mut n.task)
+            .await
+            .expect("the controller ends")
+            .unwrap();
+        assert_eq!(f.ids("a"), [1], "drained");
+        assert_eq!(f.source.teardowns(), 1);
+        let lease = leases(&store).observe("p").await.unwrap().unwrap();
+        assert!(lease.doc.released);
+        let text = n.metrics.encode();
+        assert!(!text.contains(P), "{text}");
+        assert!(line(&text, "data_avalanche_owned_pipelines 0"), "{text}");
+    }
+
+    /// A pipeline removed where it is on standby ends there: the holder cleans up, not this
+    /// node.
+    #[tokio::test(start_paused = true)]
+    async fn a_pipeline_removed_on_standby_just_ends() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut a = node("n1", &store, &docs, &f);
+        until(&mut a.status, "running", is(Actual::Running)).await;
+        let mut b = node("n2", &store, &docs, &f);
+        until(&mut b.status, "standby", |s| {
+            matches!(s.actual, Some(Actual::Standby { .. }))
+        })
+        .await;
+        b.updates.send(Update::Remove).unwrap();
+        tokio::time::timeout(Duration::from_secs(60), &mut b.task)
+            .await
+            .expect("the standby controller ends")
+            .unwrap();
+        assert_eq!(f.source.teardowns(), 0);
+        let lease = leases(&store).observe("p").await.unwrap().unwrap();
+        assert!(lease.doc.is_held_by(&holder("n1")), "n1 still holds it");
+    }
+
+    /// A reloaded `drain_timeout` is the next drain's: a stop waits for the commit past the
+    /// timeout the controller started with.
+    #[tokio::test(start_paused = true)]
+    async fn a_reloaded_drain_timeout_is_the_next_drains() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        n.updates
+            .send(Update::DrainTimeout(Duration::from_secs(60)))
+            .unwrap();
+        f.sink.hold_commits();
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
+        eventually("the commit waits", || f.sink.waiting_commits() == 1).await;
+        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
+        tokio::time::sleep(Duration::from_secs(30)).await;
+        assert_ne!(
+            n.status.borrow().actual,
+            Some(Actual::Stopped),
+            "past the 5 s it started with, the drain still waits"
+        );
+        f.sink.release_commits();
+        until(&mut n.status, "stopped", is(Actual::Stopped)).await;
+        assert_eq!(f.ids("a"), [1], "drained before it stopped");
+    }
 }
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index 4d293e5..1e8423e 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -379,6 +379,7 @@ pub(crate) struct Node {
     pub target: tokio::sync::watch::Sender<crate::Target>,
     pub task: tokio::task::JoinHandle<()>,
     pub metrics: Metrics,
+    pub updates: tokio::sync::mpsc::UnboundedSender<crate::Update>,
 }
 
 pub(crate) fn deps(
@@ -409,6 +410,7 @@ pub(crate) fn node(
     Node {
         status,
         target,
+        updates: controller.updater(),
         task: tokio::spawn(controller.run(rx)),
         metrics: deps.metrics,
     }
```

Change `crates/data-avalanche-telemetry/tests/metrics.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/metrics.rs b/crates/data-avalanche-telemetry/tests/metrics.rs
index 39b2bfa..4348829 100644
--- a/crates/data-avalanche-telemetry/tests/metrics.rs
+++ b/crates/data-avalanche-telemetry/tests/metrics.rs
@@ -198,3 +198,54 @@ fn the_gauges_read_what_was_set_last() {
     p.wal_lag_bytes(None);
     assert!(!m.encode().contains("data_avalanche_wal_lag_bytes{"));
 }
+
+/// A renamed pipeline (config §12.3) loses every series under its old name, the renew failures
+/// too; what is set next carries the new name. The same labels again change nothing.
+#[test]
+fn a_relabelled_pipeline_drops_its_old_series() {
+    let m = Metrics::new();
+    let p = m.pipeline(labels(), false);
+    p.status(PipelineStatus::Running);
+    p.renew_failed(RenewFailure::Lost);
+    p.relabel(labels(), false);
+    assert_eq!(
+        samples(&m.encode(), "data_avalanche_lease_renew_failures_total"),
+        [format!(
+            r#"data_avalanche_lease_renew_failures_total{{{L},class="lost"}} 1"#
+        )],
+        "the same labels: nothing changed"
+    );
+    let renamed = PipelineLabels {
+        pipeline: "renamed".into(),
+        ..labels()
+    };
+    p.relabel(renamed, true);
+    p.status(PipelineStatus::Running);
+    p.snapshot_rows_done(&[("public.a".to_owned(), 5)]);
+    let text = m.encode();
+    assert!(!text.contains(r#"pipeline="orders""#), "{text}");
+    let new = r#"pipeline="renamed",source_host="db1",source_db="shop""#;
+    assert!(
+        text.lines()
+            .any(|l| l == format!(r#"data_avalanche_pipeline_status{{{new},status="running"}} 1"#)),
+        "{text}"
+    );
+    assert!(
+        text.lines()
+            .any(|l| l
+                == format!(r#"data_avalanche_snapshot_rows_done{{{new},table="public.a"}} 5"#)),
+        "per_table follows: {text}"
+    );
+}
+
+/// A pipeline removed from the config loses every series, the renew failures too.
+#[test]
+fn a_forgotten_pipeline_keeps_no_series() {
+    let m = Metrics::new();
+    let p = m.pipeline(labels(), false);
+    p.status(PipelineStatus::Running);
+    p.renew_failed(RenewFailure::Lost);
+    p.forget();
+    let text = m.encode();
+    assert!(!text.contains("orders"), "{text}");
+}
```

Change `crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index 787b407..ea3ee0d 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -254,6 +254,7 @@ struct Db {
     in_flights: usize,
     readers_ended: usize,
     reconfigures: usize,
+    teardowns: usize,
     fetches: BTreeMap<TableRef, usize>,
     prepares: Vec<Option<Checkpoint>>,
 }
@@ -308,6 +309,7 @@ impl FakeSource {
             in_flights: 0,
             readers_ended: 0,
             reconfigures: 0,
+            teardowns: 0,
             fetches: BTreeMap::new(),
             prepares: vec![],
         };
@@ -590,6 +592,11 @@ impl FakeSource {
         self.db().reconfigures
     }
 
+    /// `teardown` calls so far: a removed pipeline's cleanup.
+    pub fn teardowns(&self) -> usize {
+        self.db().teardowns
+    }
+
     /// `fetch_rows` calls for `table` so far, failed ones included.
     pub fn fetches(&self, table: &TableRef) -> usize {
         self.db().fetches.get(table).copied().unwrap_or(0)
@@ -1167,6 +1174,7 @@ impl SourceConnector for FakeSource {
     }
 
     async fn teardown(&self) -> Result<(), SourceError> {
+        self.db().teardowns += 1;
         Ok(())
     }
 
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib controller::`

Expected: FAIL to compile: no `Update`, no `updater`, no `relabel` or `forget`, no `FakeSource::teardowns`.

```text
error[E0425]: cannot find type `Update` in the crate root
   --> crates/data-avalanche-cluster/src/test_support.rs:382:60
error[E0433]: cannot find type `Update` in this scope
error[E0599]: no method named `updater` found for struct `controller::PipelineController` in the current scope
```

Run: `cargo test -p data-avalanche-telemetry --test metrics`

Expected: FAIL to compile: no `Update`, no `updater`, no `relabel` or `forget`, no `FakeSource::teardowns`.

```text
error[E0599]: no method named `relabel` found for struct `PipelineMetrics` in the current scope
   --> crates/data-avalanche-telemetry/tests/metrics.rs:210:7
error[E0599]: no method named `forget` found for struct `PipelineMetrics` in the current scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index a17c509..60880e4 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -11,14 +11,14 @@ use std::sync::atomic::AtomicBool;
 use std::time::Duration;
 
 use data_avalanche_config::Pipeline;
-use data_avalanche_core::{Sink, SourceConnector};
+use data_avalanche_core::{Sink, SourceConnector, SourceError};
 use data_avalanche_pipeline::{
-    Backoff, Command, Exit, Hooks, Phase, PipelineWorker, Recovery, Tuning, WorkerDeps,
-    WorkerError, WorkerHandle, WorkerStatus,
+    Backoff, Command, CommitGuard, Exit, Hooks, Phase, PipelineWorker, Recovery, Tuning,
+    WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
 };
 use data_avalanche_state::{DesiredState, Holder, Leases, PipelineDocs};
 use data_avalanche_telemetry::{Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, Totals};
-use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
+use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
 use tokio::task::JoinHandle;
 use tokio::time::{Instant, MissedTickBehavior};
 
@@ -45,6 +45,24 @@ pub trait WorkerFactory: Send + Sync + Debug {
     fn build(&self, pipeline: &Pipeline) -> Result<Parts, String>;
 }
 
+/// A reload's change to one pipeline (config §12.3), for its controller.
+#[derive(Debug, Clone)]
+pub enum Update {
+    /// Only reloadable fields changed (class R): the running worker takes them in place, and
+    /// every later run starts with them.
+    Reconfigure(Box<Pipeline>),
+    /// A restart-only field changed (class PR, §12.4): the running worker is discarded, and the
+    /// next one replays from the checkpoint with the new pipeline and its state documents, under
+    /// the same lease. A pipeline in `ERROR` runs again, the new config may be the fix; the
+    /// desired state stands, so a paused or stopped pipeline stays so.
+    Restart(Box<Pipeline>, PipelineDocs),
+    /// Gone from the config: the worker drains, what the source created for the pipeline goes
+    /// (`auto_cleanup`), the lease is released, and the controller ends.
+    Remove,
+    /// `globals.cluster.drain_timeout`, reloaded.
+    DrainTimeout(Duration),
+}
+
 /// What the reconciler wants of a controller.
 #[derive(Debug, Clone, Copy, PartialEq, Eq)]
 pub enum Target {
@@ -139,6 +157,10 @@ pub struct PipelineController {
     starts: Arc<Semaphore>,
     handoffs: Arc<Semaphore>,
     status: watch::Sender<ControllerStatus>,
+    /// A reload's updates, and the sender [`Self::updater`] hands out: it is kept here, so the
+    /// channel never closes.
+    updates: mpsc::UnboundedReceiver<Update>,
+    updater: mpsc::UnboundedSender<Update>,
 }
 
 /// Why the owned phase ended.
@@ -152,6 +174,8 @@ enum Left {
     Released,
     /// The target channel closed: the process is shutting down.
     Shutdown,
+    /// A reload removed the pipeline: it is let go of for good.
+    Removed,
 }
 
 /// A random wait in `[0, max)`.
@@ -175,6 +199,16 @@ enum Event {
     HandOff(OwnedSemaphorePermit),
     Due,
     Poll,
+    Update(Update),
+}
+
+/// A pipeline's series labels.
+fn labels(p: &Pipeline) -> PipelineLabels {
+    PipelineLabels {
+        pipeline: p.name.clone(),
+        source_host: p.source.host.clone(),
+        source_db: p.source.db.clone(),
+    }
 }
 
 /// A running worker.
@@ -235,15 +269,11 @@ impl PipelineController {
         deps: &ControllerDeps,
         status: watch::Sender<ControllerStatus>,
     ) -> Self {
-        let labels = PipelineLabels {
-            pipeline: pipeline.name.clone(),
-            source_host: pipeline.source.host.clone(),
-            source_db: pipeline.source.db.clone(),
-        };
         let metrics = Arc::new(
             deps.metrics
-                .pipeline(labels, pipeline.sink.metrics.per_table),
+                .pipeline(labels(&pipeline), pipeline.sink.metrics.per_table),
         );
+        let (updater, updates) = mpsc::unbounded_channel();
         Self {
             pipeline,
             docs,
@@ -256,11 +286,19 @@ impl PipelineController {
             starts: Arc::clone(&deps.starts),
             handoffs: Arc::clone(&deps.handoffs),
             status,
+            updates,
+            updater,
         }
     }
 
-    /// Runs until the target channel closes; the lease is released on the way out.
-    pub async fn run(self, mut target: watch::Receiver<Target>) {
+    /// Where a reload's updates for this controller go.
+    pub fn updater(&self) -> mpsc::UnboundedSender<Update> {
+        self.updater.clone()
+    }
+
+    /// Runs until the target channel closes, or until a reload removes the pipeline; the lease
+    /// is released on the way out.
+    pub async fn run(mut self, mut target: watch::Receiver<Target>) {
         let mut claim = Claim::new(
             self.leases.clone(),
             &self.pipeline.id,
@@ -270,8 +308,9 @@ impl PipelineController {
         loop {
             if *target.borrow_and_update() == Target::DontOwn {
                 self.standby(None);
-                if target.changed().await.is_err() {
-                    return;
+                tokio::select! {
+                    r = target.changed() => if r.is_err() { return },
+                    Some(u) = self.updates.recv() => if self.take(u) { return },
                 }
                 continue;
             }
@@ -295,7 +334,7 @@ impl PipelineController {
                         Arc::clone(&self.metrics),
                     );
                     match self.owned(run, jumped, &mut target).await {
-                        Left::Shutdown => return,
+                        Left::Shutdown | Left::Removed => return,
                         Left::Fenced { current_epoch } => {
                             claim.at_least(current_epoch.saturating_add(1));
                             self.standby(None);
@@ -324,14 +363,41 @@ impl PipelineController {
             tokio::select! {
                 () = tokio::time::sleep_until(wait) => {}
                 r = target.changed() => if r.is_err() { return },
+                Some(u) = self.updates.recv() => if self.take(u) { return },
+            }
+        }
+    }
+
+    /// A reload's update while this node holds no lease for the pipeline: true when it removed
+    /// the pipeline.
+    fn take(&mut self, update: Update) -> bool {
+        match update {
+            Update::Reconfigure(p) => self.reconfigure(*p),
+            Update::Restart(p, docs) => {
+                self.reconfigure(*p);
+                self.docs = docs;
             }
+            Update::Remove => {
+                self.metrics.forget();
+                tracing::info!(pipeline = %self.pipeline.name, "pipeline removed");
+                return true;
+            }
+            Update::DrainTimeout(d) => self.settings.drain_timeout = d,
         }
+        false
+    }
+
+    /// The reloaded pipeline, for every later run; its series take a new name, or a new
+    /// `per_table`.
+    fn reconfigure(&mut self, p: Pipeline) {
+        self.metrics.relabel(labels(&p), p.sink.metrics.per_table);
+        self.pipeline = p;
     }
 
     /// Holds the lease and reconciles the worker until the lease is lost or let go.
     /// `jumped`: the term's epoch was raised past the lease's to clear a table's (DA-CLUS-14).
     async fn owned(
-        &self,
+        &mut self,
         run: LeaseRun,
         jumped: bool,
         target: &mut watch::Receiver<Target>,
@@ -341,7 +407,7 @@ impl PipelineController {
     }
 
     async fn reconcile(
-        &self,
+        &mut self,
         run: LeaseRun,
         jumped: bool,
         target: &mut watch::Receiver<Target>,
@@ -450,6 +516,7 @@ impl PipelineController {
                         }
                     }, if may_run && !want_start => Event::Due,
                     _ = poll.tick() => Event::Poll,
+                    Some(u) = self.updates.recv() => Event::Update(u),
                 }
             };
             match event {
@@ -580,6 +647,35 @@ impl PipelineController {
                         self.observe(w);
                     }
                 }
+                Event::Update(Update::Reconfigure(p)) => {
+                    self.reconfigure(*p);
+                    if let Some(w) = worker.as_ref() {
+                        let p = Box::new(self.pipeline.clone());
+                        let _ = w.handle.control.send(Command::Reconfigure(p)).await;
+                    }
+                }
+                Event::Update(Update::Restart(p, docs)) => {
+                    self.reconfigure(*p);
+                    self.docs = docs;
+                    // Discard and replay (§12.4): what the worker read past the durable
+                    // checkpoint is read again by the next one.
+                    if let Some(mut w) = worker.take() {
+                        w.task.abort();
+                        let _ = (&mut w.task).await;
+                        self.metrics.worker_ended();
+                    }
+                    tracing::info!(pipeline = %self.pipeline.name, "restarted for a reload");
+                    error = None;
+                    dumped = false;
+                    retry = None;
+                    failures = 0;
+                    desired = self.desired().await;
+                }
+                Event::Update(Update::Remove) => {
+                    self.remove(run, worker.take()).await;
+                    return Left::Removed;
+                }
+                Event::Update(Update::DrainTimeout(d)) => self.settings.drain_timeout = d,
             }
         }
     }
@@ -691,6 +787,68 @@ impl PipelineController {
         self.standby(None);
     }
 
+    /// A pipeline gone from the config (§12.3): the worker drains, what the source created for
+    /// the pipeline goes, and the lease is released. A drain cut off, or a lease lost meanwhile,
+    /// leaves the source as it is: a cut-off worker may still hold it, a lost lease's holder runs
+    /// it.
+    async fn remove(&self, run: LeaseRun, worker: Option<Running>) {
+        let drained = match worker {
+            Some(w) => self.drain(w).await,
+            None => true,
+        };
+        if !drained {
+            let _ = run.stop().await;
+            tracing::warn!(
+                pipeline = %self.pipeline.name,
+                "removed with its drain cut off: the lease expires, and what the source created for the pipeline stays"
+            );
+        } else if run.guard.may_commit() {
+            self.teardown().await;
+            self.give_back(run).await;
+        } else {
+            let _ = run.stop().await;
+            tracing::warn!(
+                pipeline = %self.pipeline.name,
+                "removed after its lease was lost: what the source created for the pipeline stays"
+            );
+        }
+        self.metrics.forget();
+        tracing::info!(pipeline = %self.pipeline.name, "pipeline removed");
+    }
+
+    /// The source's `teardown` (PostgreSQL's `auto_cleanup`: the slot and the publication),
+    /// tried again while the source is busy or doesn't answer, within the contended window: the
+    /// stream a drain closed may hold the slot a moment longer.
+    async fn teardown(&self) {
+        let source = match self.factory.build(&self.pipeline) {
+            Ok(parts) => parts.source,
+            Err(e) => {
+                tracing::warn!(pipeline = %self.pipeline.name, "no source to clean up: {e}");
+                return;
+            }
+        };
+        let since = Instant::now();
+        let mut attempt = 0;
+        loop {
+            match source.teardown().await {
+                Ok(()) => return,
+                Err(SourceError::Transient(_) | SourceError::Contended(_))
+                    if since.elapsed() < self.settings.tuning.contended_window =>
+                {
+                    tokio::time::sleep(self.settings.restart.delay(attempt)).await;
+                    attempt = attempt.saturating_add(1);
+                }
+                Err(e) => {
+                    tracing::warn!(
+                        pipeline = %self.pipeline.name,
+                        "what the source created for the pipeline stays ({e}): remove it by hand"
+                    );
+                    return;
+                }
+            }
+        }
+    }
+
     async fn give_back(&self, run: LeaseRun) {
         match run.stop().await {
             RenewEnd::Stopped(lease) => match release(&self.leases, &lease).await {
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 1e65b61..a05c248 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -22,7 +22,7 @@ pub mod wire;
 pub use claim::{Claim, Claimed, release};
 pub use controller::{
     Actual, ControllerDeps, ControllerSettings, ControllerStatus, Parts, PipelineController,
-    Target, WorkerFactory,
+    Target, Update, WorkerFactory,
 };
 pub use discovery::{Seeds, advertise, discover, interfaces};
 pub use error::ClusterError;
```

Change `crates/data-avalanche-telemetry/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/src/lib.rs b/crates/data-avalanche-telemetry/src/lib.rs
index 7998603..a792c68 100644
--- a/crates/data-avalanche-telemetry/src/lib.rs
+++ b/crates/data-avalanche-telemetry/src/lib.rs
@@ -83,6 +83,8 @@ pub enum RenewFailure {
 }
 
 impl RenewFailure {
+    const ALL: [Self; 3] = [Self::Transient, Self::Conflict, Self::Lost];
+
     fn label(self) -> &'static str {
         match self {
             Self::Transient => "transient",
@@ -284,8 +286,7 @@ impl Metrics {
     pub fn pipeline(&self, labels: PipelineLabels, per_table: bool) -> PipelineMetrics {
         PipelineMetrics {
             families: Arc::clone(&self.families),
-            labels,
-            per_table,
+            labels: Mutex::new((labels, per_table)),
             seen: Mutex::new(Seen::default()),
         }
     }
@@ -306,19 +307,56 @@ struct Seen {
 #[derive(Debug)]
 pub struct PipelineMetrics {
     families: Arc<Families>,
-    labels: PipelineLabels,
-    per_table: bool,
+    /// The labels, and `sink.metrics.per_table`: a reload may change either
+    /// ([`Self::relabel`]).
+    labels: Mutex<(PipelineLabels, bool)>,
     seen: Mutex<Seen>,
 }
 
 impl PipelineMetrics {
+    fn labels(&self) -> PipelineLabels {
+        self.labels
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner)
+            .0
+            .clone()
+    }
+
+    fn per_table(&self) -> bool {
+        self.labels.lock().unwrap_or_else(PoisonError::into_inner).1
+    }
+
     fn base(&self) -> Labels {
-        self.labels.with(&[])
+        self.labels().with(&[])
+    }
+
+    /// A renamed pipeline, or a reloaded `sink.metrics.per_table` (config §12.3): every series
+    /// under the old labels goes, the renew failures too, and what is set next carries the new
+    /// ones. The labels in force again change nothing.
+    pub fn relabel(&self, labels: PipelineLabels, per_table: bool) {
+        if *self.labels.lock().unwrap_or_else(PoisonError::into_inner)
+            == (labels.clone(), per_table)
+        {
+            return;
+        }
+        self.forget();
+        *self.labels.lock().unwrap_or_else(PoisonError::into_inner) = (labels, per_table);
+    }
+
+    /// A pipeline gone from the config: every series goes, the renew failures too.
+    pub fn forget(&self) {
+        self.remove();
+        let labels = self.labels();
+        for c in RenewFailure::ALL {
+            self.families
+                .lease_renew_failures
+                .remove(&labels.with(&[("class", c.label())]));
+        }
     }
 
     pub fn status(&self, status: PipelineStatus) {
         for s in PipelineStatus::ALL {
-            let labels = self.labels.with(&[("status", s.label())]);
+            let labels = self.labels().with(&[("status", s.label())]);
             self.families
                 .pipeline_status
                 .get_or_create(&labels)
@@ -328,7 +366,7 @@ impl PipelineMetrics {
 
     pub fn lease(&self, state: LeaseState) {
         for s in LeaseState::ALL {
-            let labels = self.labels.with(&[("state", s.label())]);
+            let labels = self.labels().with(&[("state", s.label())]);
             self.families
                 .lease_state
                 .get_or_create(&labels)
@@ -337,7 +375,7 @@ impl PipelineMetrics {
     }
 
     pub fn renew_failed(&self, class: RenewFailure) {
-        let labels = self.labels.with(&[("class", class.label())]);
+        let labels = self.labels().with(&[("class", class.label())]);
         self.families
             .lease_renew_failures
             .get_or_create(&labels)
@@ -370,7 +408,7 @@ impl PipelineMetrics {
     /// sum. A table whose snapshot ended loses its series.
     pub fn snapshot_rows_done(&self, rows: &[(String, u64)]) {
         let family = &self.families.snapshot_rows_done;
-        if !self.per_table {
+        if !self.per_table() {
             let sum = rows.iter().map(|(_, n)| *n).sum();
             family.get_or_create(&self.base()).set(gauge(sum));
             return;
@@ -378,11 +416,11 @@ impl PipelineMetrics {
         let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
         let now: BTreeSet<String> = rows.iter().map(|(t, _)| t.clone()).collect();
         for gone in seen.tables.difference(&now) {
-            family.remove(&self.labels.with(&[("table", gone)]));
+            family.remove(&self.labels().with(&[("table", gone)]));
         }
         for (table, n) in rows {
             family
-                .get_or_create(&self.labels.with(&[("table", table)]))
+                .get_or_create(&self.labels().with(&[("table", table)]))
                 .set(gauge(*n));
         }
         seen.tables = now;
@@ -450,11 +488,11 @@ impl PipelineMetrics {
         let base = self.base();
         for s in PipelineStatus::ALL {
             f.pipeline_status
-                .remove(&self.labels.with(&[("status", s.label())]));
+                .remove(&self.labels().with(&[("status", s.label())]));
         }
         for s in LeaseState::ALL {
             f.lease_state
-                .remove(&self.labels.with(&[("state", s.label())]));
+                .remove(&self.labels().with(&[("state", s.label())]));
         }
         f.lease_renew_latency.remove(&base);
         for family in [
@@ -476,7 +514,7 @@ impl PipelineMetrics {
         let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
         for table in std::mem::take(&mut seen.tables) {
             f.snapshot_rows_done
-                .remove(&self.labels.with(&[("table", &table)]));
+                .remove(&self.labels().with(&[("table", &table)]));
         }
         seen.totals = Totals::default();
     }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib controller::`

Expected: PASS, 26 tests.

```text
test result: ok. 26 passed; 0 failed; 0 ignored; 0 measured; 72 filtered out; finished in 0.44s
```

Run: `cargo test -p data-avalanche-telemetry --test metrics`

Expected: PASS, 8 tests.

```text
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1169 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/controller.rs \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/test_support.rs \
  crates/data-avalanche-telemetry/src/lib.rs \
  crates/data-avalanche-telemetry/tests/metrics.rs \
  crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "feat(cluster): a controller takes a reload's Update — reconfigure in place, restart under the lease, remove with the source's teardown, the drain timeout; series relabelled and forgotten"
```

---

### Task 4: The reconciler applies a reload (`data-avalanche-cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche-cluster/src/placement.rs`
- Modify: `crates/data-avalanche-cluster/src/reconciler.rs`

**Interfaces:**
- Consumes: `Update` and `PipelineController::updater` (Task 3).
- Produces (`reconciler.rs`, re-exported):
  - `pub enum PipelineReload { Add(Pipeline, PipelineDocs), Remove(String), Reconfigure(Pipeline), Restart(Pipeline, PipelineDocs) }`
  - `pub struct Reload { pub pipelines: Vec<PipelineReload>, pub cluster: Cluster }`
  - `pub struct Reloader(pub(crate) mpsc::UnboundedSender<Reload>)` (`Debug, Clone`), with
    `Reloader::apply(&self, reload: Reload) -> bool` (false once the reconciler is gone).
  - `ClusterReconciler::reloader(&self) -> Reloader`. `statuses()` now reflects added and removed
    pipelines.
  - `Placement::set_stabilization(&mut self, stabilization: Duration)`.

D-M8b.4. The reconcile loop gets a third input, a channel of `Reload`s. Its state moves into a
`Loop` holding:

- the controllers, each `Controlled` keeping its pipeline, documents, target, status and updater;
- the `ControllerDeps` new controllers start with;
- two `Cap`s over the takeover and hand-off semaphores;
- the statuses `watch`;
- a `JoinSet` of removals, each ending with its pipeline's id;
- `returning`: the ids whose removal runs, each with the pipeline a later generation added back.

`Cap::resize` adds permits at once and owes the rest. `settle` takes owed permits back as they
come free (`forget_permits`), so a permit in use is never revoked.

`Controlled::remove(mut self)` sends `Update::Remove` and awaits the controller in place. Its
target stays open meanwhile: a dropped target would be read as a shutdown, which releases without
the teardown. A reconciler dropped mid-removal still aborts it, as a crash. A shutdown first
closes every target, then waits for the removals.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 94f9ab0..84cdb69 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -555,4 +555,201 @@ mod tests {
         );
         r.shutdown().await;
     }
+
+    /// `globals.cluster` of the test config, with the drain timeout the tests' controllers
+    /// start with (5 s).
+    fn cluster() -> Cluster {
+        let mut c = config(&["p"]).globals.cluster;
+        c.drain_timeout = Duration::from_secs(5);
+        c
+    }
+
+    /// A reload adds a pipeline, which runs here (the ring places everything on a lone node),
+    /// and removes one, which is drained, cleaned up and released, before a shutdown ends; the
+    /// third runs on.
+    #[tokio::test(start_paused = true)]
+    async fn a_reload_adds_and_removes_pipelines() {
+        let store = ScriptedStore::new();
+        let (fs, d) = factories(&store, &["p", "q", "r"]);
+        let (_, view) = watch::channel(alone("n1"));
+        let r = ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
+        for (_, mut s) in r.statuses() {
+            until(&mut s, "running", running).await;
+        }
+        let added = pipelines(&["r"]).remove(0);
+        assert!(r.reloader().apply(Reload {
+            pipelines: vec![
+                PipelineReload::Add(added.0, added.1),
+                PipelineReload::Remove("q".into()),
+            ],
+            cluster: cluster(),
+        }));
+        eventually("r in, q out", || {
+            let st = r.statuses();
+            st.contains_key("r") && !st.contains_key("q")
+        })
+        .await;
+        until(r.statuses().get_mut("r").unwrap(), "r running", running).await;
+        fs["r"]
+            .source
+            .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(1)));
+        eventually("r's row lands", || fs["r"].ids("a") == [1]).await;
+        assert!(running(&r.statuses()["p"].borrow()), "p runs on");
+        r.shutdown().await;
+        assert_eq!(fs["q"].source.teardowns(), 1, "q cleaned up");
+        assert_eq!(fs["p"].source.teardowns(), 0);
+        let q = leases(&store).observe("q").await.unwrap().unwrap();
+        assert!(q.doc.released);
+    }
+
+    /// A pipeline added back while its removal runs starts once the removal ends: the old
+    /// controller drains, tears down and releases first, so one controller runs it at a time.
+    #[tokio::test(start_paused = true)]
+    async fn a_pipeline_added_back_during_its_removal_starts_after_it() {
+        let store = ScriptedStore::new();
+        let (fs, d) = factories(&store, &["p"]);
+        let f = fs["p"].clone();
+        let started = pipelines(&["p"]);
+        let back = started[0].clone();
+        let (_, view) = watch::channel(alone("n1"));
+        let r = ClusterReconciler::start(started, &d, view, reconciler_settings());
+        until(r.statuses().get_mut("p").unwrap(), "running", running).await;
+        f.sink.hold_commits();
+        f.source
+            .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(1)));
+        eventually("the commit waits", || f.sink.waiting_commits() == 1).await;
+        for change in [
+            PipelineReload::Remove("p".into()),
+            PipelineReload::Add(back.0, back.1),
+        ] {
+            r.reloader().apply(Reload {
+                pipelines: vec![change],
+                cluster: cluster(),
+            });
+        }
+        tokio::time::sleep(Duration::from_secs(2)).await;
+        assert!(
+            !r.statuses().contains_key("p"),
+            "not while its removal runs"
+        );
+        assert_eq!(f.builds(), 1);
+        f.sink.release_commits();
+        eventually("p is back", || r.statuses().contains_key("p")).await;
+        assert_eq!(f.source.teardowns(), 1, "torn down before it is back");
+        until(r.statuses().get_mut("p").unwrap(), "running again", running).await;
+        r.shutdown().await;
+    }
+
+    /// A reload's in-place change reaches the running worker; a restart-only one runs a new
+    /// worker.
+    #[tokio::test(start_paused = true)]
+    async fn a_reload_reconfigures_one_pipeline_and_restarts_another() {
+        let store = ScriptedStore::new();
+        let (fs, d) = factories(&store, &["p", "q"]);
+        let (fp, fq) = (fs["p"].clone(), fs["q"].clone());
+        let started = pipelines(&["p", "q"]);
+        let q_docs = started[1].1.clone();
+        let (_, view) = watch::channel(alone("n1"));
+        let r = ClusterReconciler::start(started, &d, view, reconciler_settings());
+        for (_, mut s) in r.statuses() {
+            until(&mut s, "running", running).await;
+        }
+        let mut cfg = config(&["p", "q"]).pipelines;
+        r.reloader().apply(Reload {
+            pipelines: vec![
+                PipelineReload::Reconfigure(cfg.remove("p").unwrap()),
+                PipelineReload::Restart(cfg.remove("q").unwrap(), q_docs),
+            ],
+            cluster: cluster(),
+        });
+        eventually("p reconfigured, q rebuilt", || {
+            fp.source.reconfigures() == 1 && fq.builds() == 2
+        })
+        .await;
+        until(r.statuses().get_mut("q").unwrap(), "q running", running).await;
+        assert_eq!(fp.builds(), 1, "p's worker runs on");
+        r.shutdown().await;
+    }
+
+    /// A reloaded `max_concurrent_handoffs` caps the next hand-offs: from 1 to 2 the two hung
+    /// drains run together, from 2 to 1 one after the other (each cut off at 5 s).
+    #[tokio::test(start_paused = true)]
+    async fn a_reloaded_hand_off_cap_caps_the_next_hand_offs() {
+        for (from, to, least, most) in [(1, 2, 5, 6), (2, 1, 10, 11)] {
+            let store = ScriptedStore::new();
+            let (fs, mut d) = factories(&store, &["p", "q"]);
+            d.handoffs = Arc::new(tokio::sync::Semaphore::new(from));
+            let (tx, view) = watch::channel(alone("n1"));
+            let r =
+                ClusterReconciler::start(pipelines(&["p", "q"]), &d, view, reconciler_settings());
+            let mut st = r.statuses();
+            for s in st.values_mut() {
+                until(s, "running", running).await;
+            }
+            let mut c = cluster();
+            c.max_concurrent_handoffs = to;
+            r.reloader().apply(Reload {
+                pipelines: vec![],
+                cluster: c,
+            });
+            for (i, f) in (1..).zip(fs.values()) {
+                f.sink.hold_commits();
+                f.source
+                    .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(i)));
+            }
+            eventually("both commits wait", || {
+                fs.values().all(|f| f.sink.waiting_commits() == 1)
+            })
+            .await;
+            let t = Instant::now();
+            tx.send_replace(members("n1", &[("n1", Phase::Draining, false)]));
+            r.released().await;
+            let took = t.elapsed();
+            assert!(
+                took >= Duration::from_secs(least) && took < Duration::from_secs(most),
+                "cap {from} to {to}: {took:?}"
+            );
+            for f in fs.values() {
+                f.sink.release_commits();
+            }
+            r.shutdown().await;
+        }
+    }
+
+    /// A reloaded `rebalance_stabilization` is the next joiner's wait.
+    #[tokio::test(start_paused = true)]
+    async fn a_reloaded_stabilization_is_the_next_joiners_wait() {
+        let ids = ["orders", "events", "customers", "users"];
+        let store = ScriptedStore::new();
+        let (_, d) = factories(&store, &ids);
+        let (tx, view) = watch::channel(alone("n1"));
+        let r = ClusterReconciler::start(pipelines(&ids), &d, view, reconciler_settings());
+        let mut st = r.statuses();
+        for s in st.values_mut() {
+            until(s, "running", running).await;
+        }
+        let mut c = cluster();
+        c.rebalance_stabilization = Duration::from_secs(3);
+        r.reloader().apply(Reload {
+            pipelines: vec![],
+            cluster: c,
+        });
+        tokio::time::sleep(Duration::from_millis(100)).await;
+        let two = Ring::new(["n1", "n2"], 128);
+        let moving: Vec<&str> = ids
+            .iter()
+            .copied()
+            .filter(|id| two.owner(id) == Some("n2"))
+            .collect();
+        let t = Instant::now();
+        tx.send_replace(members(
+            "n1",
+            &[("n1", Phase::Active, false), ("n2", Phase::Active, false)],
+        ));
+        for id in &moving {
+            until(st.get_mut(*id).unwrap(), "handed off", standby).await;
+        }
+        assert!(t.elapsed() < STABLE, "{:?}", t.elapsed());
+        r.shutdown().await;
+    }
 }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib reconciler::`

Expected: FAIL to compile: no `Reload`, `PipelineReload` or `reloader`.

```text
error[E0422]: cannot find struct, variant or union type `Reload` in this scope
   --> crates/data-avalanche-cluster/src/reconciler.rs:580:36
error[E0599]: no method named `reloader` found for struct `reconciler::ClusterReconciler` in the current scope
error[E0433]: cannot find type `PipelineReload` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index a05c248..bd3ec00 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -32,6 +32,8 @@ pub use gossip::{
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
 pub use node::{JOIN_WAIT, join, leave};
 pub use placement::Placement;
-pub use reconciler::{ClusterReconciler, ReconcilerSettings, node_identity};
+pub use reconciler::{
+    ClusterReconciler, PipelineReload, ReconcilerSettings, Reload, Reloader, node_identity,
+};
 pub use ring::Ring;
 pub use wire::{GossipKey, Udp, Wire};
```

Change `crates/data-avalanche-cluster/src/placement.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/placement.rs b/crates/data-avalanche-cluster/src/placement.rs
index 560b4be..4a4d711 100644
--- a/crates/data-avalanche-cluster/src/placement.rs
+++ b/crates/data-avalanche-cluster/src/placement.rs
@@ -39,6 +39,12 @@ impl Placement {
         }
     }
 
+    /// A reloaded `rebalance_stabilization`: a joiner already waiting counts its wait against
+    /// it from the next update.
+    pub fn set_stabilization(&mut self, stabilization: Duration) {
+        self.stabilization = stabilization;
+    }
+
     /// Takes in `members` as of `now`. True when the ring's nodes changed.
     pub fn update(&mut self, members: &Members, now: Instant) -> bool {
         let eligible: Vec<&str> = members
```

Change `crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 84cdb69..aa3fb82 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -3,17 +3,18 @@
 //! this node. It never touches a lease; each controller claims and lets go of its own.
 
 use std::collections::BTreeMap;
+use std::sync::Arc;
 use std::time::{Duration, SystemTime, UNIX_EPOCH};
 
 use data_avalanche_config::{Cluster, Pipeline};
 use data_avalanche_state::{Holder, PipelineDocs};
-use tokio::sync::{oneshot, watch};
-use tokio::task::JoinHandle;
+use tokio::sync::{Semaphore, mpsc, oneshot, watch};
+use tokio::task::{JoinHandle, JoinSet};
 use tokio::time::{Instant, MissedTickBehavior};
 
 use crate::{
     Actual, ClusterError, ControllerDeps, ControllerStatus, Members, PipelineController, Placement,
-    Target,
+    Target, Update,
 };
 
 /// This node as a lease holder (Clustering §3.4, D-1): `node_id` is
@@ -62,6 +63,40 @@ impl ReconcilerSettings {
     }
 }
 
+/// A reload's change to one pipeline (config §12.3).
+#[derive(Debug, Clone)]
+pub enum PipelineReload {
+    /// A new pipeline: its controller starts, on standby until the ring places it here.
+    Add(Pipeline, PipelineDocs),
+    /// The pipeline of this id is gone: [`Update::Remove`].
+    Remove(String),
+    /// [`Update::Reconfigure`].
+    Reconfigure(Pipeline),
+    /// [`Update::Restart`].
+    Restart(Pipeline, PipelineDocs),
+}
+
+/// What a config generation changes on this node's reconciler: its pipelines, and
+/// `globals.cluster`, whose reloadable fields (`rebalance_stabilization`,
+/// `max_concurrent_takeovers`, `max_concurrent_handoffs`, `drain_timeout`) take effect.
+#[derive(Debug, Clone)]
+pub struct Reload {
+    pub pipelines: Vec<PipelineReload>,
+    pub cluster: Cluster,
+}
+
+/// Hands a reload to a running reconciler, from a task that outlives a borrow of it (the
+/// config coordinator).
+#[derive(Debug, Clone)]
+pub struct Reloader(mpsc::UnboundedSender<Reload>);
+
+impl Reloader {
+    /// False when the reconciler has stopped.
+    pub fn apply(&self, reload: Reload) -> bool {
+        self.0.send(reload).is_ok()
+    }
+}
+
 #[derive(Debug)]
 struct Controlled {
     pipeline: Pipeline,
@@ -69,6 +104,8 @@ struct Controlled {
     target: watch::Sender<Target>,
     status: watch::Sender<ControllerStatus>,
     task: JoinHandle<()>,
+    /// The running controller's updates.
+    updates: mpsc::UnboundedSender<Update>,
 }
 
 /// A controller dropped is aborted, its worker with it: a crash, its lease left to expire.
@@ -79,6 +116,19 @@ impl Drop for Controlled {
 }
 
 impl Controlled {
+    fn new(pipeline: Pipeline, docs: PipelineDocs, deps: &ControllerDeps) -> Self {
+        let mut c = Self {
+            pipeline,
+            docs,
+            target: watch::channel(Target::DontOwn).0,
+            status: watch::channel(ControllerStatus::default()).0,
+            task: tokio::spawn(async {}),
+            updates: mpsc::unbounded_channel().0,
+        };
+        c.spawn(deps);
+        c
+    }
+
     fn spawn(&mut self, deps: &ControllerDeps) {
         let controller = PipelineController::reporting(
             self.pipeline.clone(),
@@ -86,8 +136,62 @@ impl Controlled {
             deps,
             self.status.clone(),
         );
+        self.updates = controller.updater();
         self.task = tokio::spawn(controller.run(self.target.subscribe()));
     }
+
+    /// Tells a removed pipeline's controller, and waits for it to let go of the pipeline. Its
+    /// target stays open meanwhile: a closed one would be a shutdown, which releases the lease
+    /// without cleaning up. Dropped unfinished (a reconciler dropped), it aborts the controller
+    /// as any [`Controlled`] does.
+    async fn remove(mut self) {
+        let _ = self.updates.send(Update::Remove);
+        if let Err(e) = (&mut self.task).await {
+            tracing::error!("a removed pipeline's controller ended abnormally: {e}");
+        }
+    }
+}
+
+/// A permit pool resized by a reload (`max_concurrent_takeovers`, `max_concurrent_handoffs`).
+/// More permits are added at once; fewer are taken back as permits come free, so a permit in
+/// use is never taken away, and until then the pool may let a few more through.
+#[derive(Debug)]
+struct Cap {
+    pool: Arc<Semaphore>,
+    size: usize,
+    /// Permits to take back as they come free.
+    owed: usize,
+}
+
+impl Cap {
+    /// `pool` with no permit in use: its size is what it has available.
+    fn new(pool: &Arc<Semaphore>) -> Self {
+        Self {
+            pool: Arc::clone(pool),
+            size: pool.available_permits(),
+            owed: 0,
+        }
+    }
+
+    fn resize(&mut self, size: usize) {
+        if size >= self.size {
+            let more = size - self.size;
+            let repaid = more.min(self.owed);
+            self.owed -= repaid;
+            self.pool.add_permits(more - repaid);
+        } else {
+            self.owed += self.size - size;
+        }
+        self.size = size;
+        self.settle();
+    }
+
+    /// Takes back the owed permits that are free now.
+    fn settle(&mut self) {
+        if self.owed > 0 {
+            self.owed -= self.pool.forget_permits(self.owed);
+        }
+    }
 }
 
 /// The controllers of every configured pipeline, and the task that sets their targets from the
@@ -95,7 +199,9 @@ impl Controlled {
 /// worker is aborted and every lease is left to expire.
 #[derive(Debug)]
 pub struct ClusterReconciler {
-    statuses: BTreeMap<String, watch::Receiver<ControllerStatus>>,
+    /// Each pipeline's controller status, as reloads add and remove pipelines.
+    statuses: watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
+    reloads: mpsc::UnboundedSender<Reload>,
     stop: Option<oneshot::Sender<()>>,
     task: JoinHandle<()>,
 }
@@ -115,47 +221,49 @@ impl ClusterReconciler {
         members: watch::Receiver<Members>,
         settings: ReconcilerSettings,
     ) -> Self {
-        let mut statuses = BTreeMap::new();
-        let controllers = pipelines
+        let controllers: BTreeMap<String, Controlled> = pipelines
             .into_iter()
-            .map(|(pipeline, docs)| {
-                let id = pipeline.id.clone();
-                let (status, rx) = watch::channel(ControllerStatus::default());
-                statuses.insert(id.clone(), rx);
-                let mut c = Controlled {
-                    pipeline,
-                    docs,
-                    target: watch::channel(Target::DontOwn).0,
-                    status,
-                    task: tokio::spawn(async {}),
-                };
-                c.spawn(deps);
-                (id, c)
-            })
+            .map(|(pipeline, docs)| (pipeline.id.clone(), Controlled::new(pipeline, docs, deps)))
             .collect();
+        let (statuses_tx, statuses) = watch::channel(status_map(&controllers));
+        let (reloads, reloaded) = mpsc::unbounded_channel();
         let (stop, stopped) = oneshot::channel();
         let task = tokio::spawn(reconcile(
-            controllers,
-            deps.clone(),
+            Loop {
+                controllers,
+                deps: deps.clone(),
+                takeovers: Cap::new(&deps.starts),
+                handoffs: Cap::new(&deps.handoffs),
+                statuses: statuses_tx,
+                removing: JoinSet::new(),
+                returning: BTreeMap::new(),
+            },
             members,
             settings,
+            reloaded,
             stopped,
         ));
         Self {
             statuses,
+            reloads,
             stop: Some(stop),
             task,
         }
     }
 
-    /// Each pipeline's controller status, by pipeline id.
+    /// Each pipeline's controller status, by pipeline id, as of now.
     pub fn statuses(&self) -> BTreeMap<String, watch::Receiver<ControllerStatus>> {
-        self.statuses.clone()
+        self.statuses.borrow().clone()
+    }
+
+    /// Where a config generation's changes go.
+    pub fn reloader(&self) -> Reloader {
+        Reloader(self.reloads.clone())
     }
 
     /// Waits until this node holds no pipeline: every controller is on standby.
     pub async fn released(&self) {
-        for status in self.statuses.values() {
+        for status in self.statuses().values() {
             let mut status = status.clone();
             let _ = status
                 .wait_for(|s| matches!(s.actual, Some(Actual::Standby { .. })))
@@ -175,13 +283,128 @@ impl ClusterReconciler {
     }
 }
 
+fn status_map(
+    controllers: &BTreeMap<String, Controlled>,
+) -> BTreeMap<String, watch::Receiver<ControllerStatus>> {
+    controllers
+        .iter()
+        .map(|(id, c)| (id.clone(), c.status.subscribe()))
+        .collect()
+}
+
+/// The reconcile loop's state.
+struct Loop {
+    controllers: BTreeMap<String, Controlled>,
+    /// The settings new and started-again controllers take: a reload changes them.
+    deps: ControllerDeps,
+    takeovers: Cap,
+    handoffs: Cap,
+    statuses: watch::Sender<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
+    /// The controllers of removed pipelines, draining and letting go; each ends with its
+    /// pipeline's id.
+    removing: JoinSet<String>,
+    /// The ids whose removal runs, each with the pipeline a later generation added back under
+    /// it: that one starts when the removal ends, so one controller runs a pipeline at a time.
+    returning: BTreeMap<String, Option<(Pipeline, PipelineDocs)>>,
+}
+
+impl Loop {
+    /// A config generation's changes (config §12.3). A removed pipeline's controller is told
+    /// and left to finish; it is no longer placed or started again.
+    fn reload(&mut self, reload: Reload, placement: &mut Placement) {
+        let c = &reload.cluster;
+        placement.set_stabilization(c.rebalance_stabilization);
+        self.takeovers
+            .resize(usize::try_from(c.max_concurrent_takeovers).unwrap_or(usize::MAX));
+        self.handoffs
+            .resize(usize::try_from(c.max_concurrent_handoffs).unwrap_or(usize::MAX));
+        if self.deps.settings.drain_timeout != c.drain_timeout {
+            self.deps.settings.drain_timeout = c.drain_timeout;
+            for controlled in self.controllers.values() {
+                let _ = controlled
+                    .updates
+                    .send(Update::DrainTimeout(c.drain_timeout));
+            }
+        }
+        for change in reload.pipelines {
+            match change {
+                PipelineReload::Add(pipeline, docs) => {
+                    let id = pipeline.id.clone();
+                    // Not a change a diff makes; the old controller is let go of, as for a
+                    // removal.
+                    if let Some(old) = self.controllers.remove(&id) {
+                        self.remove(old);
+                    }
+                    if let Some(back) = self.returning.get_mut(&id) {
+                        tracing::info!(
+                            pipeline = %pipeline.name,
+                            "pipeline added back: it starts once its removal ends"
+                        );
+                        *back = Some((pipeline, docs));
+                    } else {
+                        tracing::info!(pipeline = %pipeline.name, "pipeline added");
+                        let added = Controlled::new(pipeline, docs, &self.deps);
+                        self.controllers.insert(id, added);
+                    }
+                }
+                PipelineReload::Remove(id) => {
+                    if let Some(old) = self.controllers.remove(&id) {
+                        self.remove(old);
+                    } else if let Some(back) = self.returning.get_mut(&id) {
+                        *back = None;
+                    }
+                }
+                PipelineReload::Reconfigure(pipeline) => {
+                    if let Some(c) = self.controllers.get_mut(&pipeline.id) {
+                        c.pipeline = pipeline.clone();
+                        let _ = c.updates.send(Update::Reconfigure(Box::new(pipeline)));
+                    } else if let Some(Some(back)) = self.returning.get_mut(&pipeline.id) {
+                        back.0 = pipeline;
+                    }
+                }
+                PipelineReload::Restart(pipeline, docs) => {
+                    if let Some(c) = self.controllers.get_mut(&pipeline.id) {
+                        c.pipeline = pipeline.clone();
+                        c.docs = docs.clone();
+                        let _ = c.updates.send(Update::Restart(Box::new(pipeline), docs));
+                    } else if let Some(Some(back)) = self.returning.get_mut(&pipeline.id) {
+                        *back = (pipeline, docs);
+                    }
+                }
+            }
+        }
+        self.statuses.send_replace(status_map(&self.controllers));
+    }
+
+    /// Lets a removed pipeline's controller finish, in the background.
+    fn remove(&mut self, old: Controlled) {
+        let id = old.pipeline.id.clone();
+        self.returning.entry(id.clone()).or_insert(None);
+        self.removing.spawn(async move {
+            old.remove().await;
+            id
+        });
+    }
+
+    /// A removal ended: the pipeline added back under its id, if any, starts now.
+    fn removed(&mut self, id: &str) {
+        if let Some(Some((pipeline, docs))) = self.returning.remove(id) {
+            tracing::info!(pipeline = %pipeline.name, "pipeline added back, its removal done");
+            let added = Controlled::new(pipeline, docs, &self.deps);
+            self.controllers.insert(id.to_owned(), added);
+            self.statuses.send_replace(status_map(&self.controllers));
+        }
+    }
+}
+
 /// The reconcile loop (Clustering §8.1, §20.1): on every membership change, every due ring
-/// addition and every `resync`, each pipeline's target is "own" iff the ring places it here.
+/// addition, every reload and every `resync`, each pipeline's target is "own" iff the ring
+/// places it here.
 async fn reconcile(
-    mut controllers: BTreeMap<String, Controlled>,
-    deps: ControllerDeps,
+    mut state: Loop,
     mut members: watch::Receiver<Members>,
     settings: ReconcilerSettings,
+    mut reloads: mpsc::UnboundedReceiver<Reload>,
     mut stop: oneshot::Receiver<()>,
 ) {
     let mut placement = Placement::new(settings.tokens, settings.stabilization);
@@ -197,9 +420,12 @@ async fn reconcile(
                 "the ring changed"
             );
         }
+        let deps = &state.deps;
         deps.metrics.set_cluster_members(view.nodes.len());
         deps.metrics.set_ring_generation(placement.generation());
-        for (id, c) in &mut controllers {
+        state.takeovers.settle();
+        state.handoffs.settle();
+        for (id, c) in &mut state.controllers {
             let want = if placement.ring().owner(id) == Some(view.me.as_str()) {
                 Target::Own
             } else {
@@ -214,12 +440,19 @@ async fn reconcile(
                     pipeline = %c.pipeline.name,
                     "the pipeline's controller ended unasked; it is started again"
                 );
-                c.spawn(&deps);
+                c.spawn(deps);
             }
         }
         let due = placement.next_due();
         tokio::select! {
             _ = &mut stop => break,
+            Some(reload) = reloads.recv() => state.reload(reload, &mut placement),
+            // A removed pipeline's controller that ended.
+            Some(ended) = state.removing.join_next() => {
+                if let Ok(id) = ended {
+                    state.removed(&id);
+                }
+            }
             changed = members.changed(), if gossiping => {
                 gossiping = changed.is_ok();
                 if !gossiping {
@@ -236,8 +469,8 @@ async fn reconcile(
         }
     }
     // Every target closes at once (a replaced sender is dropped): the controllers drain
-    // together.
-    let mut controllers: Vec<Controlled> = controllers.into_values().collect();
+    // together, and the removed pipelines' controllers finish.
+    let mut controllers: Vec<Controlled> = state.controllers.into_values().collect();
     for c in &mut controllers {
         c.target = watch::channel(Target::DontOwn).0;
     }
@@ -246,6 +479,7 @@ async fn reconcile(
             tracing::error!("a pipeline controller ended abnormally: {e}");
         }
     }
+    while state.removing.join_next().await.is_some() {}
 }
 
 #[cfg(test)]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib reconciler::`

Expected: PASS, 13 tests.

```text
test result: ok. 13 passed; 0 failed; 0 ignored; 0 measured; 90 filtered out; finished in 0.22s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1174 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/placement.rs \
  crates/data-avalanche-cluster/src/reconciler.rs
git commit -m "feat(cluster): the reconciler applies a Reload — pipelines added, removed and changed, caps resized, the stabilization set; a pipeline added back waits for its removal"
```

---

### Task 5: The config coordinator (`data-avalanche-cluster`, `data-avalanche-telemetry`)

**Files:**
- Create: `crates/data-avalanche-cluster/src/coordinator.rs`
- Modify: `crates/data-avalanche-cluster/src/gossip.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche-cluster/src/reconciler.rs`
- Modify: `crates/data-avalanche-cluster/src/test_support.rs`
- Modify: `crates/data-avalanche-telemetry/src/lib.rs`
- Modify: `crates/data-avalanche-telemetry/tests/metrics.rs`

**Interfaces:**
- Consumes: `Reload`, `PipelineReload`, `Reloader` (Task 4); `ConfigGenerations::{current, publish}`
  and `config_hash` (M2); `diff`, `load_str` (M1); `WorkerFactory` (M6b).
- Produces:
  - gossip: `pub struct Advertiser` (`Debug, Clone`) with `set_config_generation(&self, generation: u64)`;
    `Gossip::advertiser(&self) -> Advertiser`; `Members::newest_generation(&self) -> u64`.
  - telemetry: `Metrics::set_config_generation(&self, generation: u64)`, the gauge
    `data_avalanche_config_generation`.
  - `coordinator.rs` (re-exported):
    - `pub const PUBLISH_ATTEMPTS: u32 = 5;`
    - `pub type DocsFn = dyn Fn(&Pipeline) -> Result<PipelineDocs, String> + Send + Sync;`
    - `pub struct CoordinatorDeps { generations: ConfigGenerations, env: Arc<dyn Env + Send + Sync>, key: FingerprintKey, node_id: String, docs: Arc<DocsFn>, factory: Arc<dyn WorkerFactory>, reloader: Reloader, advertiser: Advertiser, metrics: Metrics, globals: Arc<dyn Fn(&Globals) + Send + Sync> }`
      (all fields `pub`; `Clone`)
    - `pub struct Reloaded { pub generation: u64, pub published: bool }`
    - `pub enum ReloadError { Invalid(Report), Rejected(Report), Contended, State(StateError), Stopped }`
    - `ConfigCoordinator::start(config: ResolvedConfig, generation: Versioned<GenerationDoc>, deps: CoordinatorDeps, members: watch::Receiver<Members>, poll: Duration) -> ConfigCoordinator`
    - `applied(&self) -> u64`; `async config(&self) -> ResolvedConfig`;
      `async latest(&self) -> Result<Option<u64>, StateError>`;
      `async reload(&self, text: &str) -> Result<Reloaded, ReloadError>`; `async stop(&self)`.
      Dropped, it aborts its task.
  - test_support: `config_text(ids) -> String`, the text `config(ids)` loads.

D-M8b.5, D-M8b.6, D-M8b.7, D-M8b.11. The coordinator holds the running config and its
generation's pointer under a tokio `Mutex`: one adoption or reload at a time. A task,
`watch_generations`, wakes at every `poll` tick, and on a members change whose newest gossiped
generation is above the applied one. It then reads `generation.json` and adopts a newer
generation. A failed read is warned once until a read succeeds.

`adopt` loads the text with this node's `env` and the anchor's `key`. It resolves the table sets
of the pipelines whose patterns changed (`table_sets`), diffs, and maps the diff to
`PipelineReload`s. Added and restarted pipelines get their documents from `docs`. A refusal is
logged at `ERROR` once per generation (`failed`). Otherwise the coordinator:

- sends the reconciler its changes;
- calls the `globals` hook;
- advertises the generation;
- sets the gauge and logs a one-line summary.

`stop` marks the coordinator stopped under the lock, then ends the task. An adoption or reload
under way finishes first, and none follows.

The gossip's `Advertiser` sends `Command::Generation` to the gossip actor. The actor sets
`NodeMeta.config_generation` and changes its identity at the next `rev` (foca's
`change_identity`), so the change reaches every peer.

- [ ] **Step 1: Write the failing tests**

Create `crates/data-avalanche-cluster/src/coordinator.rs`:

```rust
//! The config coordinator (Clustering §9): the config generation this node runs. It adopts each
//! newer generation published to `globals.state`, which a member's gossiped generation announces
//! or its poll finds, and a reload publishes the node's file as the next generation. Each node
//! resolves a generation's text in its own environment and applies the difference to its own
//! controllers (config §12); nothing is proxied.

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bytes::Bytes;
    use data_avalanche_config::LogFilter;
    use data_avalanche_state::DocStore;
    use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
    use object_store::memory::InMemory;
    use tokio::sync::mpsc;

    use super::*;
    use crate::test_support::{
        Hub, at, config_text, eventually, gossip_settings, members, pipeline_docs, schema,
    };
    use crate::{Gossip, GossipKey, NodeIdentity, NodeMeta, Parts, Phase};

    /// Each build's source holds the tables its `tables.include` names: `public.*` is `a` and
    /// `b`, `public.<t>` is `t`. Its builds are counted.
    #[derive(Debug, Default)]
    struct ByInclude(std::sync::atomic::AtomicUsize);

    impl WorkerFactory for ByInclude {
        fn build(&self, p: &Pipeline) -> Result<Parts, String> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let names: Vec<String> = p
                .tables
                .include
                .iter()
                .flat_map(|i| match i.strip_prefix("public.") {
                    Some("*") => vec!["a".to_owned(), "b".to_owned()],
                    Some(t) => vec![t.to_owned()],
                    None => vec![],
                })
                .collect();
            Ok(Parts {
                source: Arc::new(data_avalanche_testkit::FakeSource::new(
                    names.iter().map(|t| schema(t)).collect(),
                )),
                sink: Arc::new(data_avalanche_testkit::MemorySink::new()),
                hooks: Arc::new(data_avalanche_pipeline::NoHooks),
            })
        }
    }

    /// An environment a test changes.
    #[derive(Debug, Default)]
    struct Vars(StdMutex<BTreeMap<String, String>>);

    impl Env for Vars {
        fn var(&self, name: &str) -> Option<String> {
            self.0.lock().unwrap().get(name).cloned()
        }
    }

    impl Vars {
        fn set(&self, name: &str, value: &str) {
            self.0.lock().unwrap().insert(name.into(), value.into());
        }
    }

    /// `globals.state` where another node publishes `next` just before this node's next pointer
    /// update, once armed: that update loses the race.
    #[derive(Debug)]
    struct Racing {
        inner: Arc<dyn VersionedStore>,
        next: StdMutex<Option<String>>,
    }

    impl Racing {
        fn new() -> Arc<Self> {
            let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
            Arc::new(Self {
                inner: Arc::new(store),
                next: StdMutex::new(None),
            })
        }

        fn arm(&self, text: &str) {
            *self.next.lock().unwrap() = Some(text.to_owned());
        }
    }

    #[async_trait]
    impl VersionedStore for Racing {
        async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
            self.inner.get(key).await
        }
        async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
            self.inner.create(key, body).await
        }
        async fn update(
            &self,
            key: &str,
            expected: &ObjectVersion,
            body: Bytes,
        ) -> Result<ObjectVersion, StoreError> {
            let next = if key == GENERATION_KEY {
                self.next.lock().unwrap().take()
            } else {
                None
            };
            if let Some(text) = next {
                let other = ConfigGenerations::new(DocStore::new(Arc::clone(&self.inner)));
                let current = other.current().await.unwrap().unwrap();
                other
                    .publish(&current.pointer, &text, "n2", Utc::now())
                    .await
                    .unwrap();
            }
            self.inner.update(key, expected, body).await
        }
        async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
            self.inner.put(key, body).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
            self.inner.list(prefix).await
        }
    }

    /// A lone node `n1` running generation 1 of its text: its reloads go to `reloads`, its
    /// generation to its own gossip, its globals to `globals`; `members` stands in for the
    /// gossip's view, so a test can make a member gossip a generation.
    struct Lone {
        generations: ConfigGenerations,
        vars: Arc<Vars>,
        coordinator: ConfigCoordinator,
        reloads: mpsc::UnboundedReceiver<Reload>,
        gossip: Gossip,
        metrics: Metrics,
        members: watch::Sender<Members>,
        globals: Arc<StdMutex<Vec<Globals>>>,
    }

    const POLL: Duration = Duration::from_secs(2);

    /// [`start`] on `config_text(ids)`, an empty environment and `globals.state` in memory.
    async fn lone(ids: &[&str]) -> Lone {
        start(&config_text(ids), Vars::default(), Racing::new()).await
    }

    async fn start(text: &str, vars: Vars, store: Arc<dyn VersionedStore>) -> Lone {
        let generations = ConfigGenerations::new(DocStore::new(store));
        let (first, created) = generations
            .bootstrap_or_adopt(text, "n1", Utc::now())
            .await
            .unwrap();
        assert!(created);
        let key = FingerprintKey::from_bytes([1; 32]);
        let vars = Arc::new(vars);
        let config = load_str(text, &*vars, &key).unwrap().config;
        let hub = Hub::new();
        let (port, inbox) = hub.port(at(1));
        let me = NodeIdentity {
            node_id: "n1".into(),
            incarnation: 1,
            rev: 0,
            addr: at(1),
            meta: NodeMeta {
                phase: Phase::Active,
                config_generation: 1,
                build: "1.0.0".into(),
            },
        };
        let gossip = Gossip::start(
            me,
            GossipKey::derive(&[7; 32]),
            gossip_settings(),
            Arc::new(port),
            inbox,
        );
        let (reloader, reloads) = mpsc::unbounded_channel();
        let metrics = Metrics::new();
        let globals = Arc::new(StdMutex::new(Vec::new()));
        let seen = Arc::clone(&globals);
        let deps = CoordinatorDeps {
            generations: generations.clone(),
            env: Arc::clone(&vars) as Arc<dyn Env + Send + Sync>,
            key,
            node_id: "n1".into(),
            docs: Arc::new(|_| Ok(pipeline_docs())),
            factory: Arc::new(ByInclude::default()),
            reloader: Reloader(reloader),
            advertiser: gossip.advertiser(),
            metrics: metrics.clone(),
            globals: Arc::new(move |g| seen.lock().unwrap().push(g.clone())),
        };
        let (members_tx, members_rx) =
            watch::channel(members("n1", &[("n1", Phase::Active, false)]));
        let coordinator = ConfigCoordinator::start(config, first.pointer, deps, members_rx, POLL);
        Lone {
            generations,
            vars,
            coordinator,
            reloads,
            gossip,
            metrics,
            members: members_tx,
            globals,
        }
    }

    impl Lone {
        /// Another node publishes `text` on top of the generation `globals.state` names.
        async fn publish_elsewhere(&self, text: &str) -> u64 {
            let current = self.generations.current().await.unwrap().unwrap();
            self.generations
                .publish(&current.pointer, text, "n2", Utc::now())
                .await
                .unwrap()
                .pointer
                .doc
                .generation
        }

        /// The next reload the reconciler got.
        async fn reload(&mut self) -> Reload {
            tokio::time::timeout(Duration::from_secs(60), self.reloads.recv())
                .await
                .expect("a reload")
                .expect("the coordinator runs")
        }

        /// The generation the node gossips.
        fn gossiped(&self) -> u64 {
            self.gossip.members().borrow().nodes["n1"]
                .meta
                .config_generation
        }
    }

    fn kinds(r: &Reload) -> Vec<String> {
        r.pipelines
            .iter()
            .map(|p| match p {
                PipelineReload::Add(p, _) => format!("add {}", p.id),
                PipelineReload::Remove(id) => format!("remove {id}"),
                PipelineReload::Reconfigure(p) => format!("reconfigure {}", p.id),
                PipelineReload::Restart(p, _) => format!("restart {}", p.id),
            })
            .collect()
    }

    /// A generation another node published is adopted at the next poll: its pipelines' changes
    /// go to the reconciler, the node gossips and shows the new generation.
    #[tokio::test(start_paused = true)]
    async fn a_generation_published_elsewhere_is_adopted_at_the_next_poll() {
        let mut n = lone(&["p", "q"]).await;
        let text = config_text(&["p", "r"]).replace(
            "prefix: p, delta: {}, flush: { seconds: 0 }",
            "prefix: p, delta: {}, flush: { seconds: 0, rows: 5 }",
        );
        assert_eq!(n.publish_elsewhere(&text).await, 2);
        let r = n.reload().await;
        let mut got = kinds(&r);
        got.sort();
        assert_eq!(got, ["add r", "reconfigure p", "remove q"]);
        assert_eq!(n.coordinator.applied(), 2);
        eventually("n1 gossips 2", || n.gossiped() == 2).await;
        assert!(
            n.metrics
                .encode()
                .lines()
                .any(|l| l == "data_avalanche_config_generation 2")
        );
    }

    /// A member that gossips a newer generation brings it in at once, not at the next poll.
    #[tokio::test(start_paused = true)]
    async fn a_gossiped_generation_is_adopted_without_waiting_for_the_poll() {
        let mut n = lone(&["p"]).await;
        // Just after a poll: the next one is almost two seconds away.
        tokio::time::sleep(POLL + Duration::from_millis(10)).await;
        n.publish_elsewhere(&config_text(&["p", "q"])).await;
        let mut view = members(
            "n1",
            &[("n1", Phase::Active, false), ("n2", Phase::Active, false)],
        );
        view.nodes.get_mut("n2").unwrap().meta.config_generation = 2;
        let t = tokio::time::Instant::now();
        n.members.send_replace(view);
        let r = n.reload().await;
        assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
        assert_eq!(kinds(&r), ["add q"]);
    }

    /// A reload publishes the file as the next generation and applies it here: the globals
    /// hook sees it, the reconciler gets the changes.
    #[tokio::test(start_paused = true)]
    async fn a_reload_publishes_and_applies_the_next_generation() {
        let mut n = lone(&["p"]).await;
        let text = config_text(&["p"]).replace(
            "  state: { bucket: app-state }\n",
            "  state: { bucket: app-state }\n  log: { level: debug }\n",
        );
        let out = n.coordinator.reload(&text).await.unwrap();
        assert_eq!(
            out,
            Reloaded {
                generation: 2,
                published: true
            }
        );
        let pointer = n.generations.current().await.unwrap().unwrap().pointer;
        assert_eq!(
            (pointer.doc.generation, pointer.doc.written_by.as_str()),
            (2, "n1")
        );
        let r = n.reload().await;
        assert!(r.pipelines.is_empty(), "{:?}", kinds(&r));
        assert_eq!(
            n.globals
                .lock()
                .unwrap()
                .last()
                .map(|g| g.log.level.clone()),
            Some(LogFilter("debug".into()))
        );
        assert_eq!(n.coordinator.applied(), 2);
    }

    /// The file as the cluster runs it already: nothing is published.
    #[tokio::test(start_paused = true)]
    async fn an_unchanged_file_publishes_nothing() {
        let n = lone(&["p"]).await;
        let out = n.coordinator.reload(&config_text(&["p"])).await.unwrap();
        assert_eq!(
            out,
            Reloaded {
                generation: 1,
                published: false
            }
        );
        let current = n.generations.current().await.unwrap().unwrap();
        assert_eq!(current.pointer.doc.generation, 1);
    }

    /// A restart-only change (V-061) and a file that doesn't load are refused; nothing is
    /// published.
    #[tokio::test(start_paused = true)]
    async fn a_refused_reload_publishes_nothing() {
        let n = lone(&["p"]).await;
        let tokens = config_text(&["p"]).replace(
            "cluster: { memberlist: { node_name: n1 } }",
            "cluster: { tokens: 64, memberlist: { node_name: n1 } }",
        );
        match n.coordinator.reload(&tokens).await {
            Err(ReloadError::Rejected(report)) => {
                assert!(
                    report
                        .errors
                        .iter()
                        .any(|d| d.rule == "V-061" && d.path == "globals.cluster.tokens"),
                    "{report:?}"
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            n.coordinator.reload("pipelines: [").await,
            Err(ReloadError::Invalid(_))
        ));
        let current = n.generations.current().await.unwrap().unwrap();
        assert_eq!(current.pointer.doc.generation, 1);
    }

    /// A reload while another node's newer generation isn't adopted yet: it is adopted first,
    /// the file is checked against it, and published on top.
    #[tokio::test(start_paused = true)]
    async fn a_reload_publishes_on_top_of_a_newer_generation() {
        let mut n = lone(&["p"]).await;
        n.publish_elsewhere(&config_text(&["p", "q"])).await;
        let out = n
            .coordinator
            .reload(&config_text(&["p", "q", "r"]))
            .await
            .unwrap();
        assert_eq!(out.generation, 3);
        assert_eq!(kinds(&n.reload().await), ["add q"]);
        assert_eq!(kinds(&n.reload().await), ["add r"]);
        assert_eq!(n.coordinator.applied(), 3);
    }

    /// A generation this node can't resolve (a `${VAR}` its environment lacks) isn't applied:
    /// it runs on its own until a generation it can apply.
    #[tokio::test(start_paused = true)]
    async fn a_generation_that_doesnt_load_here_waits_for_the_next() {
        let mut n = lone(&["p"]).await;
        let unset = config_text(&["p", "q"]).replace(
            "  q:\n    id: q\n    source:\n      host: 127.0.0.1",
            "  q:\n    id: q\n    source:\n      host: ${Q_HOST}",
        );
        assert_eq!(n.publish_elsewhere(&unset).await, 2);
        tokio::time::sleep(POLL * 3).await;
        assert_eq!(n.coordinator.applied(), 1);
        assert!(n.reloads.try_recv().is_err(), "nothing applied");
        assert_eq!(n.gossiped(), 1);
        assert_eq!(n.publish_elsewhere(&config_text(&["p", "q"])).await, 3);
        assert_eq!(kinds(&n.reload().await), ["add q"]);
        assert_eq!(n.coordinator.applied(), 3);
    }

    /// `tables.include` changed (config §1.2): in place when the source resolves the same
    /// tables, a restart when it resolves others.
    #[tokio::test(start_paused = true)]
    async fn a_tables_change_restarts_only_when_the_set_changes() {
        let mut n = lone(&["p"]).await;
        let same = config_text(&["p"]).replace("[public.*]", "[public.a, public.b]");
        n.coordinator.reload(&same).await.unwrap();
        assert_eq!(kinds(&n.reload().await), ["reconfigure p"]);
        let fewer = config_text(&["p"]).replace("[public.*]", "[public.a]");
        n.coordinator.reload(&fewer).await.unwrap();
        assert_eq!(kinds(&n.reload().await), ["restart p"]);
    }

    /// Once stopped, for the leave: no generation is adopted, a reload is refused.
    #[tokio::test(start_paused = true)]
    async fn a_stopped_coordinator_adopts_and_publishes_nothing() {
        let mut n = lone(&["p"]).await;
        n.coordinator.stop().await;
        n.publish_elsewhere(&config_text(&["p", "q"])).await;
        tokio::time::sleep(POLL * 3).await;
        assert_eq!(n.coordinator.applied(), 1);
        assert!(n.reloads.try_recv().is_err(), "nothing applied");
        for text in [config_text(&["p", "r"]), "pipelines: [".to_owned()] {
            assert!(matches!(
                n.coordinator.reload(&text).await,
                Err(ReloadError::Stopped)
            ));
        }
        assert_eq!(n.coordinator.config().await.pipelines.len(), 1);
    }

    /// Two reloads at once on two nodes: this one's pointer update loses the race, so it reads
    /// the winner's generation, adopts it, checks the file against it and publishes on top.
    #[tokio::test(start_paused = true)]
    async fn a_reload_that_loses_the_race_publishes_on_top_of_the_winner() {
        let racing = Racing::new();
        let mut n = start(
            &config_text(&["p"]),
            Vars::default(),
            Arc::clone(&racing) as Arc<dyn VersionedStore>,
        )
        .await;
        racing.arm(&config_text(&["p", "q"]));
        let out = n
            .coordinator
            .reload(&config_text(&["p", "q", "r"]))
            .await
            .unwrap();
        assert_eq!(
            out,
            Reloaded {
                generation: 3,
                published: true
            }
        );
        assert_eq!(kinds(&n.reload().await), ["add q"]);
        assert_eq!(kinds(&n.reload().await), ["add r"]);
        let pointer = n.generations.current().await.unwrap().unwrap().pointer;
        assert_eq!(
            (pointer.doc.generation, pointer.doc.written_by.as_str()),
            (3, "n1")
        );
    }

    /// The generation's own text, read again after what it resolves to changed here (config
    /// §12.1: a `${VAR}`, a credentials file): published again, so every node resolves it anew.
    #[tokio::test(start_paused = true)]
    async fn the_same_text_is_published_again_when_what_it_resolves_to_changed() {
        let text = config_text(&["p"]).replace("password: avalanche", "password: ${P_PASSWORD}");
        let vars = Vars::default();
        vars.set("P_PASSWORD", "one");
        let mut n = start(&text, vars, Racing::new()).await;
        assert_eq!(
            n.coordinator.reload(&text).await.unwrap(),
            Reloaded {
                generation: 1,
                published: false
            }
        );
        n.vars.set("P_PASSWORD", "two");
        assert_eq!(
            n.coordinator.reload(&text).await.unwrap(),
            Reloaded {
                generation: 2,
                published: true
            }
        );
        assert_eq!(kinds(&n.reload().await), ["restart p"]);
    }
}
```

Change `crates/data-avalanche-cluster/src/gossip.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index b0ab645..a407aeb 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -543,6 +543,34 @@ mod tests {
         assert_eq!(names(&a), ["a", "b", "c"], "a phase change is no departure");
     }
 
+    /// The config generation a node applies reaches every peer (Clustering §9.1), and the
+    /// newest one any member applies is what a coordinator behind reads.
+    #[tokio::test(start_paused = true)]
+    async fn a_config_generation_reaches_every_peer() {
+        let hub = Hub::new();
+        let (a, b, c) = three(&hub).await;
+        assert_eq!(a.members().borrow().newest_generation(), 1);
+        b.advertiser().set_config_generation(2);
+        let generation = |g: &Gossip| {
+            g.members()
+                .borrow()
+                .nodes
+                .get("b")
+                .map(|m| m.meta.config_generation)
+        };
+        eventually("b at 2 on a and c", || {
+            generation(&a) == Some(2) && generation(&c) == Some(2)
+        })
+        .await;
+        assert_eq!(generation(&b), Some(2), "its own view");
+        assert_eq!(a.members().borrow().newest_generation(), 2);
+        assert_eq!(
+            names(&a),
+            ["a", "b", "c"],
+            "a new generation is no departure"
+        );
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_leave_is_seen_at_once() {
         let hub = Hub::new();
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index bd3ec00..ba0e1b0 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -17,6 +17,7 @@ pub mod reconciler;
 pub mod ring;
 #[cfg(test)]
 mod test_support;
+pub mod coordinator;
 pub mod wire;
 
 pub use claim::{Claim, Claimed, release};
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index 1e8423e..db677a0 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -223,6 +223,17 @@ pub(crate) fn metrics() -> (Metrics, Arc<PipelineMetrics>) {
 /// A config with one pipeline per id, each over `public.*` of a fake PostgreSQL source (slot
 /// `<id>_slot`), flushing at every boundary.
 pub(crate) fn config(ids: &[&str]) -> data_avalanche_config::ResolvedConfig {
+    data_avalanche_config::load_str(
+        &config_text(ids),
+        &std::collections::BTreeMap::new(),
+        &data_avalanche_config::FingerprintKey::from_bytes([1; 32]),
+    )
+    .unwrap_or_else(|r| panic!("test config: {r:?}"))
+    .config
+}
+
+/// [`config`]'s text.
+pub(crate) fn config_text(ids: &[&str]) -> String {
     let mut yaml = String::from(
         "config_version: 1\nglobals:\n  state: { bucket: app-state }\n  cluster: { memberlist: { node_name: n1 } }\npipelines:\n",
     );
@@ -245,13 +256,7 @@ pub(crate) fn config(ids: &[&str]) -> data_avalanche_config::ResolvedConfig {
 "#
         ));
     }
-    data_avalanche_config::load_str(
-        &yaml,
-        &std::collections::BTreeMap::new(),
-        &data_avalanche_config::FingerprintKey::from_bytes([1; 32]),
-    )
-    .unwrap_or_else(|r| panic!("test config: {r:?}"))
-    .config
+    yaml
 }
 
 /// The test pipeline `p`.
```

Change `crates/data-avalanche-telemetry/tests/metrics.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/metrics.rs b/crates/data-avalanche-telemetry/tests/metrics.rs
index 4348829..724408f 100644
--- a/crates/data-avalanche-telemetry/tests/metrics.rs
+++ b/crates/data-avalanche-telemetry/tests/metrics.rs
@@ -249,3 +249,16 @@ fn a_forgotten_pipeline_keeps_no_series() {
     let text = m.encode();
     assert!(!text.contains("orders"), "{text}");
 }
+
+/// The config generation this node applies (Clustering §9): a node behind the cluster shows it.
+#[test]
+fn the_node_reports_its_config_generation() {
+    let m = Metrics::new();
+    m.set_config_generation(3);
+    let text = m.encode();
+    assert!(
+        text.lines()
+            .any(|l| l == "data_avalanche_config_generation 3"),
+        "{text}"
+    );
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib -- coordinator:: gossip::`

Expected: FAIL to compile: the coordinator's types, `Advertiser` and `set_config_generation` don't exist.

```text
error[E0405]: cannot find trait `WorkerFactory` in this scope
  --> crates/data-avalanche-cluster/src/coordinator.rs:28:10
error[E0425]: cannot find type `Pipeline` in this scope
error[E0433]: cannot find type `Arc` in this scope
error[E0425]: cannot find type `StdMutex` in this scope
error[E0425]: cannot find type `BTreeMap` in this scope
error[E0405]: cannot find trait `Env` in this scope
error[E0425]: cannot find type `Arc` in this scope
error[E0425]: cannot find value `GENERATION_KEY` in this scope
error[E0425]: cannot find type `ConfigGenerations` in this scope
error[E0425]: cannot find type `ConfigCoordinator` in this scope
error[E0425]: cannot find type `Reload` in this scope
```

Run: `cargo test -p data-avalanche-telemetry --test metrics`

Expected: FAIL to compile: the coordinator's types, `Advertiser` and `set_config_generation` don't exist.

```text
error[E0599]: no method named `set_config_generation` found for struct `Metrics` in the current scope
   --> crates/data-avalanche-telemetry/tests/metrics.rs:257:7
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/coordinator.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/coordinator.rs b/crates/data-avalanche-cluster/src/coordinator.rs
index 542db86..211eac8 100644
--- a/crates/data-avalanche-cluster/src/coordinator.rs
+++ b/crates/data-avalanche-cluster/src/coordinator.rs
@@ -4,6 +4,480 @@
 //! resolves a generation's text in its own environment and applies the difference to its own
 //! controllers (config §12); nothing is proxied.
 
+use std::collections::{BTreeMap, BTreeSet};
+use std::fmt::Debug;
+use std::sync::{Arc, Mutex as StdMutex, PoisonError};
+use std::time::Duration;
+
+use chrono::Utc;
+use data_avalanche_config::{
+    ConfigDiff, Env, FingerprintKey, Globals, Pipeline, PipelineChange, Report, ResolvedConfig,
+    diff, load_str,
+};
+use data_avalanche_core::TableRef;
+use data_avalanche_state::{
+    ConfigGeneration, ConfigGenerations, GENERATION_KEY, GenerationDoc, PipelineDocs, StateError,
+    Versioned, config_hash,
+};
+use data_avalanche_telemetry::Metrics;
+use thiserror::Error;
+use tokio::sync::{Mutex, watch};
+use tokio::task::JoinHandle;
+use tokio::time::MissedTickBehavior;
+
+use crate::{Advertiser, Members, PipelineReload, Reload, Reloader, WorkerFactory};
+
+/// A reload re-reads the generation and tries again this many times when another node publishes
+/// first.
+pub const PUBLISH_ATTEMPTS: u32 = 5;
+
+/// A pipeline's state documents, at its state location (config §10).
+pub type DocsFn = dyn Fn(&Pipeline) -> Result<PipelineDocs, String> + Send + Sync;
+
+/// What the coordinator works with.
+#[derive(Clone)]
+pub struct CoordinatorDeps {
+    pub generations: ConfigGenerations,
+    /// This node's environment: each generation's text is resolved in it.
+    pub env: Arc<dyn Env + Send + Sync>,
+    /// The anchor's: the running config was loaded with it, and both sides of a diff must be.
+    pub key: FingerprintKey,
+    /// The node that publishes (`written_by`).
+    pub node_id: String,
+    pub docs: Arc<DocsFn>,
+    /// Builds a pipeline's source for the table-set check (config §1.2).
+    pub factory: Arc<dyn WorkerFactory>,
+    pub reloader: Reloader,
+    pub advertiser: Advertiser,
+    pub metrics: Metrics,
+    /// What a generation's globals change outside the cluster: the binary's log filter.
+    pub globals: Arc<dyn Fn(&Globals) + Send + Sync>,
+}
+
+impl Debug for CoordinatorDeps {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("CoordinatorDeps")
+            .field("node_id", &self.node_id)
+            .finish_non_exhaustive()
+    }
+}
+
+/// A reload's outcome.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub struct Reloaded {
+    /// The generation the cluster runs now.
+    pub generation: u64,
+    /// False when the file's text is that generation's already: nothing was published.
+    pub published: bool,
+}
+
+/// Why a reload published nothing.
+#[derive(Debug, Error)]
+pub enum ReloadError {
+    /// The file doesn't load.
+    #[error("the config is invalid")]
+    Invalid(Report),
+    /// A field only a process restart applies changed (V-061, config §1.2).
+    #[error("the config changes a field only a process restart applies")]
+    Rejected(Report),
+    /// Other nodes published first, every time.
+    #[error("other nodes published {PUBLISH_ATTEMPTS} times while this reload tried")]
+    Contended,
+    /// `globals.state` couldn't be read or written.
+    #[error("the config generation: {0}")]
+    State(#[from] StateError),
+    /// The node is shutting down.
+    #[error("the node is shutting down")]
+    Stopped,
+}
+
+/// The generation this node runs.
+struct Running {
+    config: ResolvedConfig,
+    generation: Versioned<GenerationDoc>,
+    /// A newer generation this node couldn't apply: logged once, tried again at each poll.
+    failed: Option<u64>,
+    /// [`ConfigCoordinator::stop`] ran: nothing is adopted or published any more.
+    stopped: bool,
+}
+
+struct Inner {
+    deps: CoordinatorDeps,
+    /// One adoption or reload at a time.
+    running: Mutex<Running>,
+    applied: watch::Sender<u64>,
+}
+
+/// The node's config generation at run time. Dropped, it stops watching.
+#[derive(Debug)]
+pub struct ConfigCoordinator {
+    inner: Arc<Inner>,
+    stop: watch::Sender<bool>,
+    task: StdMutex<Option<JoinHandle<()>>>,
+}
+
+impl Debug for Inner {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("Inner")
+            .field("applied", &*self.applied.borrow())
+            .finish_non_exhaustive()
+    }
+}
+
+impl Drop for ConfigCoordinator {
+    fn drop(&mut self) {
+        if let Some(task) = self.task.get_mut().unwrap_or_else(PoisonError::into_inner) {
+            task.abort();
+        }
+    }
+}
+
+impl ConfigCoordinator {
+    /// Runs `config`, `generation`'s text as loaded with the anchor's key, and watches for newer
+    /// generations: every `poll`, and whenever a member gossips one newer than this node's.
+    pub fn start(
+        config: ResolvedConfig,
+        generation: Versioned<GenerationDoc>,
+        deps: CoordinatorDeps,
+        members: watch::Receiver<Members>,
+        poll: Duration,
+    ) -> Self {
+        let n = generation.doc.generation;
+        deps.metrics.set_config_generation(n);
+        let inner = Arc::new(Inner {
+            deps,
+            running: Mutex::new(Running {
+                config,
+                generation,
+                failed: None,
+                stopped: false,
+            }),
+            applied: watch::channel(n).0,
+        });
+        let (stop, stopped) = watch::channel(false);
+        let task = tokio::spawn(watch_generations(
+            Arc::clone(&inner),
+            members,
+            poll,
+            stopped,
+        ));
+        Self {
+            inner,
+            stop,
+            task: StdMutex::new(Some(task)),
+        }
+    }
+
+    /// The generation this node runs.
+    pub fn applied(&self) -> u64 {
+        *self.inner.applied.borrow()
+    }
+
+    /// The config this node runs: its generation's text as resolved here.
+    pub async fn config(&self) -> ResolvedConfig {
+        self.inner.running.lock().await.config.clone()
+    }
+
+    /// The generation `globals.state` names now, `None` before the first bootstrap.
+    ///
+    /// # Errors
+    /// The pointer or its snapshot can't be read.
+    pub async fn latest(&self) -> Result<Option<u64>, StateError> {
+        Ok(self
+            .inner
+            .deps
+            .generations
+            .current()
+            .await?
+            .map(|g| g.pointer.doc.generation))
+    }
+
+    /// A reload (Clustering §9.2, config §12.1): `text`, the node's file read again, is checked
+    /// against the generation the cluster runs (adopted first when newer), published as the
+    /// next one, and applied here; the others adopt it from `globals.state`. The text of the
+    /// generation itself is published again when what it resolves to here changed (a `${VAR}`,
+    /// a credentials file): every node resolves it anew.
+    ///
+    /// # Errors
+    /// [`ReloadError`]: nothing was published.
+    pub async fn reload(&self, text: &str) -> Result<Reloaded, ReloadError> {
+        let d = &self.inner.deps;
+        let mut running = self.inner.running.lock().await;
+        if running.stopped {
+            return Err(ReloadError::Stopped);
+        }
+        let new = load_str(text, &*d.env, &d.key)
+            .map_err(ReloadError::Invalid)?
+            .config;
+        for _ in 0..PUBLISH_ATTEMPTS {
+            let current = d
+                .generations
+                .current()
+                .await?
+                .ok_or_else(|| StateError::Corrupt {
+                    key: GENERATION_KEY.to_owned(),
+                    kind: "config generation",
+                    reason: "missing, though this node started from one".to_owned(),
+                })?;
+            let n = current.pointer.doc.generation;
+            if n > running.generation.doc.generation {
+                self.inner.adopt(&mut running, current.clone()).await;
+            }
+            let changes = diff(&running.config, &new, &|_, _| None);
+            if changes.is_rejected() {
+                return Err(ReloadError::Rejected(changes.report));
+            }
+            if config_hash(text) == current.pointer.doc.hash && changes.is_empty() {
+                return Ok(Reloaded {
+                    generation: n,
+                    published: false,
+                });
+            }
+            match d
+                .generations
+                .publish(&current.pointer, text, &d.node_id, Utc::now())
+                .await
+            {
+                Ok(published) => {
+                    let generation = published.pointer.doc.generation;
+                    tracing::info!(generation, "published config generation {generation}");
+                    self.inner.adopt(&mut running, published).await;
+                    return Ok(Reloaded {
+                        generation,
+                        published: true,
+                    });
+                }
+                // Another node published first, or the outcome is unknown: read again.
+                Err(e) if e.is_race() => {}
+                Err(e) => return Err(e.into()),
+            }
+        }
+        Err(ReloadError::Contended)
+    }
+
+    /// Stops watching, after the adoption or reload under way: none follows, and a reload is
+    /// [`ReloadError::Stopped`].
+    pub async fn stop(&self) {
+        self.inner.running.lock().await.stopped = true;
+        self.stop.send_replace(true);
+        let task = self
+            .task
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner)
+            .take();
+        if let Some(task) = task {
+            let _ = task.await;
+        }
+    }
+}
+
+impl Inner {
+    /// Applies generation `g` (config §12): a generation that doesn't load here, or that changes
+    /// a restart-only field as this node resolves it, isn't, and this node runs on what it
+    /// runs.
+    async fn adopt(&self, running: &mut Running, g: ConfigGeneration) {
+        let d = &self.deps;
+        let n = g.pointer.doc.generation;
+        let refused = |running: &mut Running, why: String| {
+            if running.failed != Some(n) {
+                tracing::error!(
+                    generation = n,
+                    "config generation {n} can't be applied on this node, which runs {}: {why}",
+                    running.generation.doc.generation
+                );
+            }
+            running.failed = Some(n);
+        };
+        let new = match load_str(&g.text, &*d.env, &d.key) {
+            Ok(loaded) => loaded.config,
+            Err(report) => return refused(running, lines(&report)),
+        };
+        let sets = table_sets(&*d.factory, &running.config, &new).await;
+        let changes = diff(&running.config, &new, &|_, p| {
+            sets.get(&p.id).copied().flatten()
+        });
+        if changes.is_rejected() {
+            return refused(running, lines(&changes.report));
+        }
+        let pipelines = match self.pipelines(&new, &changes) {
+            Ok(pipelines) => pipelines,
+            Err(why) => return refused(running, why),
+        };
+        d.reloader.apply(Reload {
+            pipelines,
+            cluster: new.globals.cluster.clone(),
+        });
+        (d.globals)(&new.globals);
+        d.advertiser.set_config_generation(n);
+        d.metrics.set_config_generation(n);
+        tracing::info!(
+            generation = n,
+            "applied config generation {n}: {}",
+            summary(&changes)
+        );
+        running.config = new;
+        running.generation = g.pointer;
+        running.failed = None;
+        self.applied.send_replace(n);
+    }
+
+    /// The reconciler's share of a diff: each pipeline added (with its state documents),
+    /// removed, changed in place, or restarted (with its documents, which may have moved).
+    fn pipelines(
+        &self,
+        new: &ResolvedConfig,
+        changes: &ConfigDiff,
+    ) -> Result<Vec<PipelineReload>, String> {
+        let by_id = |id: &str| {
+            new.pipelines
+                .values()
+                .find(|p| p.id == id)
+                .cloned()
+                .ok_or_else(|| format!("pipeline id {id} isn't in the generation"))
+        };
+        let docs = |p: &Pipeline| (self.deps.docs)(p);
+        changes
+            .pipelines
+            .iter()
+            .map(|change| {
+                Ok(match change {
+                    PipelineChange::Added { id, .. } => {
+                        let p = by_id(id)?;
+                        let d = docs(&p)?;
+                        PipelineReload::Add(p, d)
+                    }
+                    PipelineChange::Removed { id, .. } => PipelineReload::Remove(id.clone()),
+                    PipelineChange::Modified {
+                        id, restart: true, ..
+                    } => {
+                        let p = by_id(id)?;
+                        let d = docs(&p)?;
+                        PipelineReload::Restart(p, d)
+                    }
+                    PipelineChange::Modified { id, .. } => PipelineReload::Reconfigure(by_id(id)?),
+                })
+            })
+            .collect()
+    }
+}
+
+/// A report's errors, a line each, for the log.
+fn lines(report: &Report) -> String {
+    report
+        .errors
+        .iter()
+        .map(|d| format!("{} at {}: {}", d.rule, d.path, d.message))
+        .collect::<Vec<_>>()
+        .join("; ")
+}
+
+/// What a generation changed, in a few words.
+fn summary(changes: &ConfigDiff) -> String {
+    let mut added = 0;
+    let mut removed = 0;
+    let mut changed = 0;
+    let mut restarted = 0;
+    for c in &changes.pipelines {
+        match c {
+            PipelineChange::Added { .. } => added += 1,
+            PipelineChange::Removed { .. } => removed += 1,
+            PipelineChange::Modified { restart, .. } => {
+                changed += 1;
+                restarted += usize::from(*restart);
+            }
+        }
+    }
+    format!(
+        "{added} pipelines added, {removed} removed, {changed} changed ({restarted} restarted), {} global fields",
+        changes.globals.len()
+    )
+}
+
+/// For each pipeline whose `tables.include` or `tables.exclude` changed: whether its resolved
+/// table set changed, as its source answers now (config §1.2). `None` when either side can't be
+/// resolved: a restart, which is always safe.
+async fn table_sets(
+    factory: &dyn WorkerFactory,
+    old: &ResolvedConfig,
+    new: &ResolvedConfig,
+) -> BTreeMap<String, Option<bool>> {
+    let mut out = BTreeMap::new();
+    for p in new.pipelines.values() {
+        let Some(o) = old.pipelines.values().find(|o| o.id == p.id) else {
+            continue;
+        };
+        if o.tables.include == p.tables.include && o.tables.exclude == p.tables.exclude {
+            continue;
+        }
+        let changed = match (resolved(factory, o).await, resolved(factory, p).await) {
+            (Some(before), Some(after)) => Some(before != after),
+            _ => None,
+        };
+        out.insert(p.id.clone(), changed);
+    }
+    out
+}
+
+/// The tables `p` names, as its source resolves them now: replicated or halted.
+async fn resolved(factory: &dyn WorkerFactory, p: &Pipeline) -> Option<BTreeSet<TableRef>> {
+    let source = factory.build(p).ok()?.source;
+    let t = source.resolve_tables().await.ok()?;
+    Some(
+        t.tables
+            .iter()
+            .map(|s| s.table().clone())
+            .chain(t.halted.iter().map(|(table, _)| table.clone()))
+            .collect(),
+    )
+}
+
+/// Adopts each newer generation: at every `poll`, and when a member gossips one newer than this
+/// node's.
+async fn watch_generations(
+    inner: Arc<Inner>,
+    mut members: watch::Receiver<Members>,
+    poll: Duration,
+    mut stop: watch::Receiver<bool>,
+) {
+    let mut tick = tokio::time::interval(poll);
+    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
+    tick.tick().await;
+    let mut gossiping = true;
+    let mut unread = false;
+    loop {
+        tokio::select! {
+            _ = stop.wait_for(|s| *s) => return,
+            _ = tick.tick() => {}
+            changed = members.changed(), if gossiping => {
+                if changed.is_err() {
+                    gossiping = false;
+                    continue;
+                }
+                if members.borrow_and_update().newest_generation() <= *inner.applied.borrow() {
+                    continue;
+                }
+            }
+        }
+        match inner.deps.generations.current().await {
+            Ok(Some(g)) => {
+                unread = false;
+                let mut running = inner.running.lock().await;
+                if !running.stopped && g.pointer.doc.generation > running.generation.doc.generation
+                {
+                    inner.adopt(&mut running, g).await;
+                }
+            }
+            Ok(None) => unread = false,
+            Err(e) => {
+                if !unread {
+                    tracing::warn!("the config generation can't be read: {e}; read again later");
+                }
+                unread = true;
+            }
+        }
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use async_trait::async_trait;
```

Change `crates/data-avalanche-cluster/src/gossip.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/gossip.rs b/crates/data-avalanche-cluster/src/gossip.rs
index a407aeb..f88d527 100644
--- a/crates/data-avalanche-cluster/src/gossip.rs
+++ b/crates/data-avalanche-cluster/src/gossip.rs
@@ -105,6 +105,15 @@ impl Members {
     pub fn peers(&self) -> usize {
         self.nodes.len().saturating_sub(1)
     }
+
+    /// The newest config generation a member applies, this node included (Clustering §9.1).
+    pub fn newest_generation(&self) -> u64 {
+        self.nodes
+            .values()
+            .map(|m| m.meta.config_generation)
+            .max()
+            .unwrap_or(0)
+    }
 }
 
 /// The members `me` sees in foca's membership state: each name's latest identity that isn't
@@ -213,6 +222,7 @@ const WARN_EVERY: Duration = Duration::from_secs(60);
 enum Command {
     Announce(Vec<SocketAddr>),
     Phase(Phase),
+    Generation(u64),
     Leave(oneshot::Sender<()>),
 }
 
@@ -228,6 +238,16 @@ impl Announcer {
     }
 }
 
+/// Tells the others the config generation this node applies; cheap to clone.
+#[derive(Debug, Clone)]
+pub struct Advertiser(mpsc::UnboundedSender<Command>);
+
+impl Advertiser {
+    pub fn set_config_generation(&self, generation: u64) {
+        let _ = self.0.send(Command::Generation(generation));
+    }
+}
+
 /// A running gossip. Dropping it stops the gossip without a leave, as a crash would.
 #[derive(Debug)]
 pub struct Gossip {
@@ -301,6 +321,12 @@ impl Gossip {
         let _ = self.commands.send(Command::Phase(phase));
     }
 
+    /// A handle that tells the others this node's config generation, for a task that outlives
+    /// a borrow of the gossip (the config coordinator).
+    pub fn advertiser(&self) -> Advertiser {
+        Advertiser(self.commands.clone())
+    }
+
     /// Stops the gossip as a crash would, keeping the handle: what it sends goes nowhere.
     #[cfg(test)]
     pub(crate) fn abort(&self) {
@@ -366,6 +392,19 @@ impl Actor {
                         log(self.swim.change_identity(next, &mut rt));
                         None
                     }
+                    Some(Command::Generation(config_generation)) => {
+                        let me = self.swim.identity();
+                        let next = NodeIdentity {
+                            rev: me.rev.wrapping_add(1),
+                            meta: NodeMeta {
+                                config_generation,
+                                ..me.meta.clone()
+                            },
+                            ..me.clone()
+                        };
+                        log(self.swim.change_identity(next, &mut rt));
+                        None
+                    }
                     Some(Command::Leave(done)) => {
                         log(self.swim.leave_cluster(&mut rt));
                         Some(Some(done))
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index ba0e1b0..471aa6b 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -7,6 +7,7 @@ pub mod claim;
 #[cfg(test)]
 mod cluster_tests;
 pub mod controller;
+pub mod coordinator;
 pub mod discovery;
 mod error;
 pub mod gossip;
@@ -17,7 +18,6 @@ pub mod reconciler;
 pub mod ring;
 #[cfg(test)]
 mod test_support;
-pub mod coordinator;
 pub mod wire;
 
 pub use claim::{Claim, Claimed, release};
@@ -25,10 +25,13 @@ pub use controller::{
     Actual, ControllerDeps, ControllerSettings, ControllerStatus, Parts, PipelineController,
     Target, Update, WorkerFactory,
 };
+pub use coordinator::{
+    ConfigCoordinator, CoordinatorDeps, DocsFn, PUBLISH_ATTEMPTS, ReloadError, Reloaded,
+};
 pub use discovery::{Seeds, advertise, discover, interfaces};
 pub use error::ClusterError;
 pub use gossip::{
-    Announcer, Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase,
+    Advertiser, Announcer, Gossip, GossipSettings, Member, Members, NodeIdentity, NodeMeta, Phase,
 };
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
 pub use node::{JOIN_WAIT, join, leave};
```

Change `crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index aa3fb82..77e416f 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -88,7 +88,7 @@ pub struct Reload {
 /// Hands a reload to a running reconciler, from a task that outlives a borrow of it (the
 /// config coordinator).
 #[derive(Debug, Clone)]
-pub struct Reloader(mpsc::UnboundedSender<Reload>);
+pub struct Reloader(pub(crate) mpsc::UnboundedSender<Reload>);
 
 impl Reloader {
     /// False when the reconciler has stopped.
```

Change `crates/data-avalanche-telemetry/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/src/lib.rs b/crates/data-avalanche-telemetry/src/lib.rs
index a792c68..68c6f4f 100644
--- a/crates/data-avalanche-telemetry/src/lib.rs
+++ b/crates/data-avalanche-telemetry/src/lib.rs
@@ -131,6 +131,7 @@ struct Families {
     lease_renew_latency: HistogramFamily,
     owned_pipelines: Gauge,
     cluster_members: Gauge,
+    config_generation: Gauge,
     ring_generation: Gauge,
     wal_lag_bytes: Family<Labels, Gauge>,
     source_reconnects: Family<Labels, Counter>,
@@ -169,6 +170,7 @@ impl Metrics {
             lease_renew_latency: Family::new_with_constructor(renew_latency),
             owned_pipelines: Gauge::default(),
             cluster_members: Gauge::default(),
+            config_generation: Gauge::default(),
             ring_generation: Gauge::default(),
             wal_lag_bytes: Family::default(),
             source_reconnects: Family::default(),
@@ -211,6 +213,11 @@ impl Metrics {
             "Members of the cluster this node sees",
             f.cluster_members.clone(),
         );
+        r.register(
+            "config_generation",
+            "The config generation this node applies",
+            f.config_generation.clone(),
+        );
         r.register(
             "ring_generation",
             "Changes of the ring's nodes since this node started",
@@ -278,6 +285,10 @@ impl Metrics {
         self.families.cluster_members.set(gauge(n as u64));
     }
 
+    pub fn set_config_generation(&self, generation: u64) {
+        self.families.config_generation.set(gauge(generation));
+    }
+
     pub fn set_ring_generation(&self, generation: u64) {
         self.families.ring_generation.set(gauge(generation));
     }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib -- coordinator:: gossip::`

Expected: PASS, 22 tests.

```text
test result: ok. 22 passed; 0 failed; 0 ignored; 0 measured; 93 filtered out; finished in 0.04s
```

Run: `cargo test -p data-avalanche-telemetry --test metrics`

Expected: PASS, 9 tests.

```text
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1187 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/coordinator.rs \
  crates/data-avalanche-cluster/src/gossip.rs \
  crates/data-avalanche-cluster/src/lib.rs \
  crates/data-avalanche-cluster/src/reconciler.rs \
  crates/data-avalanche-cluster/src/test_support.rs \
  crates/data-avalanche-telemetry/src/lib.rs \
  crates/data-avalanche-telemetry/tests/metrics.rs
git commit -m "feat(cluster): the config coordinator — each newer generation adopted (a poll, a gossiped nudge), a reload published with a CAS and retried on a lost race, the applied generation gossiped and gauged"
```

---

### Task 6: Three in-process nodes reload (`data-avalanche-cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/cluster_tests.rs`
- Modify: `crates/data-avalanche-cluster/src/reconciler.rs`
- Modify: `crates/data-avalanche-cluster/src/test_support.rs`

**Interfaces:**
- Consumes: `ConfigCoordinator` (Task 5), `ClusterReconciler::reloader` (Task 4).
- Produces (tests only):
  - `ClusterReconciler::watch_statuses(&self)` (`#[cfg(test)]`, crate-private): the statuses as
    reloads change them.
  - test_support: `TestCluster::with_spares(ids, spares)`; `TestCluster.generations`, one
    `ConfigGenerations` the nodes share; `TestNode.coordinator`.

Each `TestCluster` node now starts as the binary does after M8b: it adopts the config generation
(the first node bootstraps it from `config_text` of the cluster's ids), joins, starts its
reconciler on the generation's pipelines, then its coordinator, polling every
`lease_renewal_interval`. The checker that samples for double runs now reads each node's live
status map, so pipelines a reload adds are checked too. `with_spares` gives a pipeline a later
generation adds its source, sink and documents up front.

The scenario: five pipelines on three nodes. A reload on `b`:

- removes `payments`: its owner tears it down, and only its owner;
- adds `audit`, placed by the ring;
- changes `orders`' flush (R): reconfigured in place, no new build;
- moves `customers`' sink prefix (PR): one new build.

Every node applies generation 2, every node's gossip shows it, and the rows inserted after the
reload land in every live pipeline and not in `payments`.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/cluster_tests.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/cluster_tests.rs b/crates/data-avalanche-cluster/src/cluster_tests.rs
index 5aac270..6aefe83 100644
--- a/crates/data-avalanche-cluster/src/cluster_tests.rs
+++ b/crates/data-avalanche-cluster/src/cluster_tests.rs
@@ -7,7 +7,8 @@ use std::time::Duration;
 use tokio::time::Instant;
 
 use crate::test_support::{
-    LEAVE_WITHIN, STABLE, TestCluster, TestNode, at, eventually, runners, settles, timing,
+    LEAVE_WITHIN, STABLE, TestCluster, TestNode, at, config_text, eventually, runners, settles,
+    timing,
 };
 use crate::{Ring, leave};
 
@@ -181,3 +182,69 @@ async fn a_rolling_restart_keeps_every_pipeline_running_once() {
     eventually("every row lands", || c.landed(&all)).await;
     c.assert_ran_once();
 }
+
+/// A reload on `b` reaches every node (Clustering §9.2): `payments` is removed, its owner
+/// tearing it down once; `audit` is added and placed; `orders` takes a new flush in place;
+/// `customers` moves its sink prefix and restarts under its lease. Rows land throughout.
+#[tokio::test(start_paused = true)]
+async fn a_reload_reaches_every_node_and_each_pipeline_runs_once() {
+    let first = ["orders", "events", "customers", "users", "payments"];
+    let c = TestCluster::with_spares(&first, &["audit"]);
+    let (a, b, n3) = three(&c).await;
+    let nodes = [&a, &b, &n3];
+    c.insert(1);
+    let f = |id: &str| c.factories.0[id].clone();
+    eventually("row 1 lands", || {
+        first.iter().all(|id| f(id).ids("a") == [1])
+    })
+    .await;
+    let builds = |id: &str| f(id).builds.load(std::sync::atomic::Ordering::SeqCst);
+    let (orders, customers) = (builds("orders"), builds("customers"));
+
+    let text = config_text(&["orders", "events", "customers", "users", "audit"])
+        .replace(
+            "prefix: orders, delta: {}, flush: { seconds: 0 }",
+            "prefix: orders, delta: {}, flush: { seconds: 0, rows: 5 }",
+        )
+        .replace("prefix: customers,", "prefix: customers-v2,");
+    let out = b.coordinator.reload(&text).await.unwrap();
+    assert_eq!((out.generation, out.published), (2, true));
+    eventually("every node applies generation 2", || {
+        nodes.iter().all(|n| n.coordinator.applied() == 2)
+    })
+    .await;
+    settles(
+        "audit placed, payments gone",
+        &nodes,
+        placed(&["a", "b", "c"]),
+    )
+    .await;
+    let by = runners(&nodes);
+    assert!(!by.contains_key("payments"), "{by:?}");
+    assert_eq!(by["audit"].len(), 1, "{by:?}");
+    eventually("payments torn down", || {
+        f("payments").source.teardowns() == 1
+    })
+    .await;
+    assert_eq!(f("orders").source.reconfigures(), 1);
+    assert_eq!(builds("orders"), orders, "orders changed in place");
+    assert_eq!(builds("customers"), customers + 1, "customers restarted");
+
+    c.insert(2);
+    let live = ["orders", "events", "customers", "users", "audit"];
+    eventually("row 2 lands", || {
+        live.iter().all(|id| f(id).ids("a") == [1, 2])
+    })
+    .await;
+    assert_eq!(f("payments").ids("a"), [1]);
+    for n in nodes {
+        assert_eq!(
+            n.gossip.members().borrow().newest_generation(),
+            2,
+            "{}",
+            n.name
+        );
+    }
+    assert_eq!(f("payments").source.teardowns(), 1, "the owner alone");
+    c.assert_ran_once();
+}
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index db677a0..11a2231 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -590,13 +590,15 @@ pub(crate) const STABLE: Duration = Duration::from_secs(15);
 /// A leave's bound that no test's hand-offs reach.
 pub(crate) const LEAVE_WITHIN: Duration = Duration::from_secs(3600);
 
-/// In-process nodes: their gossip on one hub, their leases in one store, their pipelines'
-/// sources, sinks and state documents shared.
+/// In-process nodes: their gossip on one hub, their leases in one store, their config
+/// generations in another, their pipelines' sources, sinks and state documents shared.
 #[derive(Debug)]
 pub(crate) struct TestCluster {
     pub hub: Arc<Hub>,
     pub store: Arc<ScriptedStore>,
     pub factories: ByPipeline,
+    pub generations: data_avalanche_state::ConfigGenerations,
+    /// The pipelines of the first generation.
     ids: Vec<String>,
     docs: std::collections::BTreeMap<String, data_avalanche_state::PipelineDocs>,
     registry: Registry,
@@ -604,15 +606,12 @@ pub(crate) struct TestCluster {
     checker: tokio::task::JoinHandle<()>,
 }
 
-/// Every status a [`TestCluster`] started: node name, pipeline id, the controller's status.
-type Registry = Arc<
-    Mutex<
-        Vec<(
-            String,
-            String,
-            tokio::sync::watch::Receiver<crate::ControllerStatus>,
-        )>,
-    >,
+/// Each node a [`TestCluster`] started, by name: its controllers' statuses as reloads change
+/// them.
+type Registry = Arc<Mutex<Vec<(String, Statuses)>>>;
+
+type Statuses = tokio::sync::watch::Receiver<
+    std::collections::BTreeMap<String, tokio::sync::watch::Receiver<crate::ControllerStatus>>,
 >;
 
 impl Drop for TestCluster {
@@ -627,6 +626,7 @@ pub(crate) struct TestNode {
     pub name: String,
     pub gossip: crate::Gossip,
     pub reconciler: crate::ClusterReconciler,
+    pub coordinator: crate::ConfigCoordinator,
     pub addr: SocketAddr,
     /// The node's hand-off permits (`max_concurrent_handoffs`): a test that holds them all
     /// keeps every hand-off from starting.
@@ -642,10 +642,12 @@ async fn check_once_running(registry: Registry, violation: Arc<Mutex<Option<Stri
     loop {
         tokio::time::sleep(Duration::from_millis(10)).await;
         let mut by: std::collections::BTreeMap<String, Vec<String>> = Default::default();
-        for (node, id, rx) in registry.lock().unwrap().iter() {
-            let alive = rx.has_changed().is_ok();
-            if alive && matches!(rx.borrow().actual, Some(Actual::Starting | Actual::Running)) {
-                by.entry(id.clone()).or_default().push(node.clone());
+        for (node, statuses) in registry.lock().unwrap().iter() {
+            for (id, rx) in statuses.borrow().iter() {
+                let alive = rx.has_changed().is_ok();
+                if alive && matches!(rx.borrow().actual, Some(Actual::Starting | Actual::Running)) {
+                    by.entry(id.clone()).or_default().push(node.clone());
+                }
             }
         }
         let mut seen = violation.lock().unwrap();
@@ -665,6 +667,12 @@ impl TestCluster {
     /// that no pipeline is starting or running on two nodes (D-M8a.21's invariant, beyond
     /// `settles`); [`Self::assert_ran_once`] reports it. Runs inside a tokio test.
     pub fn new(ids: &[&str]) -> Self {
+        Self::with_spares(ids, &[])
+    }
+
+    /// [`Self::new`] with sources, sinks and state documents for `spares` too, pipelines a
+    /// later generation adds.
+    pub fn with_spares(ids: &[&str], spares: &[&str]) -> Self {
         let registry: Registry = Default::default();
         let violation: Arc<Mutex<Option<String>>> = Default::default();
         let checker = tokio::spawn(check_once_running(registry.clone(), violation.clone()));
@@ -676,19 +684,25 @@ impl TestCluster {
             store: ScriptedStore::new(),
             factories: ByPipeline(
                 ids.iter()
+                    .chain(spares)
                     .map(|id| ((*id).to_owned(), TestFactory::new(&["a"])))
                     .collect(),
             ),
+            generations: data_avalanche_state::ConfigGenerations::new(DocStore::new(Arc::new(
+                ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").expect("a root"),
+            ))),
             ids: ids.iter().map(|id| (*id).to_owned()).collect(),
             docs: ids
                 .iter()
+                .chain(spares)
                 .map(|id| ((*id).to_owned(), pipeline_docs()))
                 .collect(),
         }
     }
 
-    /// Starts `name` at `at(n)` as its `incarnation`, as the binary does: its gossip joining,
-    /// announced to `seed`, then [`crate::join`], then its reconciler.
+    /// Starts `name` at `at(n)` as its `incarnation`, as the binary does: the config generation
+    /// adopted (the first bootstrapped), its gossip joining, announced to `seed`, then
+    /// [`crate::join`], then its reconciler and its config coordinator.
     pub async fn start(
         &self,
         name: &str,
@@ -696,6 +710,17 @@ impl TestCluster {
         incarnation: u64,
         seed: Option<SocketAddr>,
     ) -> TestNode {
+        let ids: Vec<&str> = self.ids.iter().map(String::as_str).collect();
+        let (first, _) = self
+            .generations
+            .bootstrap_or_adopt(&config_text(&ids), name, Utc::now())
+            .await
+            .expect("a config generation");
+        let key = data_avalanche_config::FingerprintKey::from_bytes([1; 32]);
+        let env = std::collections::BTreeMap::<String, String>::new();
+        let config = data_avalanche_config::load_str(&first.text, &env, &key)
+            .unwrap_or_else(|r| panic!("generation: {r:?}"))
+            .config;
         let (port, inbox) = self.hub.port(at(n));
         let me = crate::NodeIdentity {
             node_id: name.to_owned(),
@@ -704,12 +729,17 @@ impl TestCluster {
             addr: at(n),
             meta: crate::NodeMeta {
                 phase: crate::Phase::Joining,
-                config_generation: 1,
+                config_generation: first.pointer.doc.generation,
                 build: "1.0.0".into(),
             },
         };
-        let key = crate::GossipKey::derive(&[7; 32]);
-        let gossip = crate::Gossip::start(me, key, gossip_settings(), Arc::new(port), inbox);
+        let gossip = crate::Gossip::start(
+            me,
+            crate::GossipKey::derive(&[7; 32]),
+            gossip_settings(),
+            Arc::new(port),
+            inbox,
+        );
         if let Some(seed) = seed {
             gossip.announce(vec![seed]);
         }
@@ -722,12 +752,10 @@ impl TestCluster {
         deps.holder.incarnation = incarnation;
         deps.factory = Arc::new(self.factories.clone());
         deps.starts = Arc::new(tokio::sync::Semaphore::new(4));
-        let ids: Vec<&str> = self.ids.iter().map(String::as_str).collect();
-        let mut cfg = config(&ids).pipelines;
-        let pipelines = self
-            .ids
-            .iter()
-            .map(|id| (cfg.remove(id).expect("configured"), self.docs[id].clone()))
+        let pipelines = config
+            .pipelines
+            .values()
+            .map(|p| (p.clone(), self.docs[&p.id].clone()))
             .collect();
         let settings = crate::ReconcilerSettings {
             tokens: 128,
@@ -736,16 +764,34 @@ impl TestCluster {
         };
         let reconciler =
             crate::ClusterReconciler::start(pipelines, &deps, gossip.members(), settings);
-        {
-            let mut registry = self.registry.lock().unwrap();
-            for (id, rx) in reconciler.statuses() {
-                registry.push((name.to_owned(), id, rx));
-            }
-        }
+        self.registry
+            .lock()
+            .unwrap()
+            .push((name.to_owned(), reconciler.watch_statuses()));
+        let docs = self.docs.clone();
+        let coordinator = crate::ConfigCoordinator::start(
+            config,
+            first.pointer,
+            crate::CoordinatorDeps {
+                generations: self.generations.clone(),
+                env: Arc::new(env),
+                key,
+                node_id: name.to_owned(),
+                docs: Arc::new(move |p| docs.get(&p.id).cloned().ok_or_else(|| p.id.clone())),
+                factory: Arc::new(self.factories.clone()),
+                reloader: reconciler.reloader(),
+                advertiser: gossip.advertiser(),
+                metrics: deps.metrics.clone(),
+                globals: Arc::new(|_| {}),
+            },
+            gossip.members(),
+            timing().renewal_interval,
+        );
         TestNode {
             name: name.to_owned(),
             gossip,
             reconciler,
+            coordinator,
             addr: at(n),
             handoffs: deps.handoffs,
         }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib cluster_tests::`

Expected: FAIL to compile: `watch_statuses` doesn't exist.

```text
error[E0599]: no method named `watch_statuses` found for struct `reconciler::ClusterReconciler` in the current scope
   --> crates/data-avalanche-cluster/src/test_support.rs:770:48
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
index 77e416f..b2c6058 100644
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -256,6 +256,14 @@ impl ClusterReconciler {
         self.statuses.borrow().clone()
     }
 
+    /// [`Self::statuses`] as reloads change them.
+    #[cfg(test)]
+    pub(crate) fn watch_statuses(
+        &self,
+    ) -> watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>> {
+        self.statuses.clone()
+    }
+
     /// Where a config generation's changes go.
     pub fn reloader(&self) -> Reloader {
         Reloader(self.reloads.clone())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib cluster_tests::`

Expected: PASS, 5 tests.

```text
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 111 filtered out; finished in 3.13s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1188 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/cluster_tests.rs \
  crates/data-avalanche-cluster/src/reconciler.rs \
  crates/data-avalanche-cluster/src/test_support.rs
git commit -m "test(cluster): a reload on one of three in-process nodes reaches every node — removed, added, changed in place and restarted, each pipeline on one node"
```

---

### Task 7: The binary: the reload and status endpoints, the log, the wiring (`data-avalanche`)

**Files:**
- Modify: `crates/data-avalanche/Cargo.toml`
- Modify: `crates/data-avalanche/src/http.rs`
- Modify: `crates/data-avalanche/src/logging.rs`
- Modify: `crates/data-avalanche/src/run.rs`
- Modify: `crates/data-avalanche/src/startup.rs`

**Interfaces:**
- Consumes: `ConfigCoordinator`, `CoordinatorDeps`, `ReloadError`, `Reloaded` (Task 5);
  `ClusterReconciler::reloader` (Task 4); `Gossip::advertiser` (Task 5).
- Produces (crate-private):
  - `http::Http.config: Arc<OnceLock<ConfigApi>>`; `http::ConfigApi { coordinator: Arc<ConfigCoordinator>, members: watch::Receiver<Members>, file: PathBuf }`;
    the routes `POST /v1/config/reload` and `GET /v1/config/status`.
  - `logging::Logger` with `apply(&self, log: &Log)`; `logging::build(log, args, writer) -> Result<(impl Subscriber, Logger), String>`;
    `logging::init(log, args) -> Result<Logger, String>`.
  - `startup::per_node(file: &ResolvedConfig, generation: &ResolvedConfig) -> Vec<String>`;
    `startup::Joined.generation: Versioned<GenerationDoc>` (was the number).

D-M8b.8, D-M8b.9, D-M8b.10, D-M8b.11. The listener starts before the join, as in M6b. The config
routes answer 503 until `run` sets the `OnceLock`, after the reconciler and the coordinator have
started. `run` builds the coordinator's dependencies from what startup already has:

- the anchor's key and the `globals.state` documents (`Joined`);
- `ProcessEnv`;
- the worker factory;
- `Buckets::pipeline_docs` for the documents;
- the reconciler's `Reloader` and the gossip's `Advertiser`;
- a `globals` hook that calls `Logger::apply`.

`init` returns the `Logger`. Its subscriber is a registry with a reloadable `EnvFilter`, then a
reloadable boxed `fmt` layer. The writer is a parameter of `build`, so the tests capture the
output with `with_default`. On SIGTERM: readiness off, discovery aborted, the coordinator
stopped, then `leave` with the bound of `coordinator.config()`.

`Cargo.toml`: `serde_json` moves to `[dependencies]` (`Cargo.lock` is unchanged; it was already a
dev-dependency).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche/src/http.rs`:

```diff
diff --git a/crates/data-avalanche/src/http.rs b/crates/data-avalanche/src/http.rs
index b8d29ec..174dab3 100644
--- a/crates/data-avalanche/src/http.rs
+++ b/crates/data-avalanche/src/http.rs
@@ -62,15 +62,32 @@ pub(crate) async fn bind(address: &HostPort) -> Result<TcpListener, Fatal> {
 
 #[cfg(test)]
 mod tests {
+    use std::net::SocketAddr;
+    use std::time::Duration;
+
+    use data_avalanche_cluster::{
+        ClusterReconciler, ControllerDeps, CoordinatorDeps, Gossip, GossipKey, GossipSettings,
+        NodeIdentity, NodeMeta, Phase, ReconcilerSettings, Udp,
+    };
+    use data_avalanche_config::{FingerprintKey, ProcessEnv, load_str};
+    use data_avalanche_state::{ConfigGenerations, DocStore, Holder, Leases};
+    use data_avalanche_storage::ObjectStoreVersioned;
+    use object_store::memory::InMemory;
     use tokio::io::{AsyncReadExt, AsyncWriteExt};
+    use tokio::sync::Semaphore;
 
     use super::*;
+    use crate::Factory;
+    use crate::run::tests::CONFIG;
 
-    /// A plain HTTP/1.1 GET: the status code, the headers, the body.
-    async fn get(addr: std::net::SocketAddr, path: &str) -> (u16, String, String) {
+    /// A plain HTTP/1.1 request without a body: the status code, the headers, the body.
+    async fn send(addr: SocketAddr, method: &str, path: &str) -> (u16, String, String) {
         let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
         s.write_all(
-            format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes(),
+            format!(
+                "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
+            )
+            .as_bytes(),
         )
         .await
         .unwrap();
@@ -81,11 +98,21 @@ mod tests {
         (code, head.to_owned(), body.to_owned())
     }
 
-    #[tokio::test]
-    async fn the_listener_answers_health_and_metrics() {
-        let metrics = Metrics::new();
-        metrics.set_cluster_members(1);
-        let ready = Arc::new(AtomicBool::new(false));
+    async fn get(addr: SocketAddr, path: &str) -> (u16, String, String) {
+        send(addr, "GET", path).await
+    }
+
+    /// The code and the JSON body.
+    async fn json_of(addr: SocketAddr, method: &str, path: &str) -> (u16, Value) {
+        let (code, _, body) = send(addr, method, path).await;
+        (
+            code,
+            serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}")),
+        )
+    }
+
+    /// The listener on a free port.
+    async fn serve(http: Http) -> SocketAddr {
         let listener = bind(&HostPort {
             host: "127.0.0.1".into(),
             port: 0,
@@ -93,11 +120,21 @@ mod tests {
         .await
         .unwrap();
         let addr = listener.local_addr().unwrap();
-        let app = router(Http {
+        tokio::spawn(async move { axum::serve(listener, router(http)).await });
+        addr
+    }
+
+    #[tokio::test]
+    async fn the_listener_answers_health_and_metrics() {
+        let metrics = Metrics::new();
+        metrics.set_cluster_members(1);
+        let ready = Arc::new(AtomicBool::new(false));
+        let addr = serve(Http {
             metrics,
             ready: Arc::clone(&ready),
-        });
-        tokio::spawn(async move { axum::serve(listener, app).await });
+            config: Arc::default(),
+        })
+        .await;
 
         assert_eq!(get(addr, "/health/live").await.0, 200);
         assert_eq!(
@@ -117,5 +154,162 @@ mod tests {
         assert!(body.contains("data_avalanche_cluster_members 1"), "{body}");
         assert!(body.ends_with("# EOF\n"), "{body}");
         assert_eq!(get(addr, "/v1/pipelines").await.0, 404, "the API is M9's");
+        assert_eq!(
+            json_of(addr, "POST", "/v1/config/reload").await.0,
+            503,
+            "not before the node runs its generation"
+        );
+        assert_eq!(json_of(addr, "GET", "/v1/config/status").await.0, 503);
+    }
+
+    /// A lone node `node-a` running generation 1 of [`CONFIG`], read from `file`: its gossip on
+    /// a loopback port, its reloads reaching a reconciler without pipelines.
+    async fn lone(file: PathBuf) -> (ConfigApi, ClusterReconciler, Gossip) {
+        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
+        let docs = DocStore::new(Arc::new(store));
+        let generations = ConfigGenerations::new(docs.clone());
+        let (first, _) = generations
+            .bootstrap_or_adopt(CONFIG, "node-a", chrono::Utc::now())
+            .await
+            .unwrap();
+        let key = FingerprintKey::from_bytes([1; 32]);
+        let config = load_str(CONFIG, &ProcessEnv, &key).unwrap().config;
+        let (udp, inbox) = Udp::bind(&["127.0.0.1:0".parse().unwrap()]).await.unwrap();
+        let me = NodeIdentity {
+            node_id: "node-a".into(),
+            incarnation: 1,
+            rev: 0,
+            addr: udp.local_addrs()[0],
+            meta: NodeMeta {
+                phase: Phase::Active,
+                config_generation: 1,
+                build: "1.0.0".into(),
+            },
+        };
+        let gossip = Gossip::start(
+            me,
+            GossipKey::derive(&[7; 32]),
+            GossipSettings::new(&config.globals.cluster.memberlist),
+            Arc::new(udp),
+            inbox,
+        );
+        let metrics = Metrics::new();
+        let deps = ControllerDeps {
+            leases: Leases::new(docs),
+            holder: Holder {
+                node_id: "node-a".into(),
+                incarnation: 1,
+            },
+            factory: Arc::new(Factory::new(&config.globals).unwrap()),
+            metrics: metrics.clone(),
+            settings: crate::run::settings(&config),
+            starts: Arc::new(Semaphore::new(1)),
+            handoffs: Arc::new(Semaphore::new(1)),
+        };
+        let reconciler = ClusterReconciler::start(
+            vec![],
+            &deps,
+            gossip.members(),
+            ReconcilerSettings::new(&config.globals.cluster),
+        );
+        let coordinator = ConfigCoordinator::start(
+            config,
+            first.pointer,
+            CoordinatorDeps {
+                generations,
+                env: Arc::new(ProcessEnv),
+                key,
+                node_id: "node-a".into(),
+                docs: Arc::new(|p| Err(p.id.clone())),
+                factory: deps.factory,
+                reloader: reconciler.reloader(),
+                advertiser: gossip.advertiser(),
+                metrics,
+                globals: Arc::new(|_| {}),
+            },
+            gossip.members(),
+            Duration::from_secs(10),
+        );
+        let api = ConfigApi {
+            coordinator: Arc::new(coordinator),
+            members: gossip.members(),
+            file,
+        };
+        (api, reconciler, gossip)
+    }
+
+    /// The reload and its status (Clustering §9.2): 200 for the file the cluster runs, 202 and
+    /// the next generation for a change, 422 with the report for a file that is invalid or
+    /// changes a restart-only field, 503 once the node stops.
+    #[tokio::test]
+    async fn a_reload_publishes_the_file_and_the_status_follows() {
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, CONFIG).unwrap();
+        let (api, _reconciler, _gossip) = lone(file.clone()).await;
+        let coordinator = Arc::clone(&api.coordinator);
+        let config = Arc::new(OnceLock::new());
+        config.set(api).unwrap();
+        let addr = serve(Http {
+            metrics: Metrics::new(),
+            ready: Arc::new(AtomicBool::new(true)),
+            config,
+        })
+        .await;
+
+        let (code, v) = json_of(addr, "GET", "/v1/config/status").await;
+        assert_eq!(code, 200);
+        assert_eq!(
+            v,
+            json!({ "generation": 1, "applied": 1, "nodes": { "node-a": 1 }, "converged": true })
+        );
+        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
+        assert_eq!(
+            (code, v),
+            (200, json!({ "generation": 1, "unchanged": true }))
+        );
+
+        std::fs::write(
+            &file,
+            CONFIG.replace("drain_timeout: 45s", "drain_timeout: 40s"),
+        )
+        .unwrap();
+        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
+        assert_eq!((code, v), (202, json!({ "generation": 2 })));
+        let mut status = Value::Null;
+        for _ in 0..500 {
+            status = json_of(addr, "GET", "/v1/config/status").await.1;
+            if status["converged"] == json!(true) {
+                break;
+            }
+            tokio::time::sleep(Duration::from_millis(10)).await;
+        }
+        assert_eq!(
+            status,
+            json!({ "generation": 2, "applied": 2, "nodes": { "node-a": 2 }, "converged": true })
+        );
+
+        std::fs::write(
+            &file,
+            CONFIG.replace("lease_duration: 30s", "tokens: 64, lease_duration: 30s"),
+        )
+        .unwrap();
+        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
+        assert_eq!(code, 422, "{v}");
+        assert_eq!(v["report"]["errors"][0]["rule"], "V-061", "{v}");
+        assert_eq!(
+            v["report"]["errors"][0]["path"], "globals.cluster.tokens",
+            "{v}"
+        );
+
+        std::fs::write(&file, "pipelines: [").unwrap();
+        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
+        assert_eq!(code, 422, "{v}");
+        assert_eq!(v["report"]["valid"], false, "{v}");
+
+        coordinator.stop().await;
+        let (code, v) = json_of(addr, "POST", "/v1/config/reload").await;
+        assert_eq!(code, 503, "{v}");
+        assert_eq!(coordinator.latest().await.unwrap(), Some(2));
     }
 }
```

Change `crates/data-avalanche/src/logging.rs`:

```diff
diff --git a/crates/data-avalanche/src/logging.rs b/crates/data-avalanche/src/logging.rs
index 7a2878d..02fa522 100644
--- a/crates/data-avalanche/src/logging.rs
+++ b/crates/data-avalanche/src/logging.rs
@@ -147,4 +147,85 @@ mod tests {
             ("debug".to_owned(), LogFormat::Compact)
         );
     }
+
+    /// A writer into a shared buffer.
+    #[derive(Debug, Clone, Default)]
+    struct Buf(std::sync::Arc<Mutex<Vec<u8>>>);
+
+    impl std::io::Write for Buf {
+        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
+            self.0.lock().unwrap().extend_from_slice(b);
+            Ok(b.len())
+        }
+        fn flush(&mut self) -> std::io::Result<()> {
+            Ok(())
+        }
+    }
+
+    impl<'w> MakeWriter<'w> for Buf {
+        type Writer = Buf;
+        fn make_writer(&'w self) -> Buf {
+            self.clone()
+        }
+    }
+
+    impl Buf {
+        /// The lines that contain `what`.
+        fn lines(&self, what: &str) -> Vec<String> {
+            String::from_utf8(self.0.lock().unwrap().clone())
+                .unwrap()
+                .lines()
+                .filter(|l| l.contains(what))
+                .map(str::to_owned)
+                .collect()
+        }
+    }
+
+    fn log(level: &str, format: LogFormat) -> Log {
+        Log {
+            level: LogFilter(level.into()),
+            format,
+        }
+    }
+
+    #[test]
+    fn a_generation_changes_the_level_and_the_format() {
+        let buf = Buf::default();
+        let (subscriber, logger) = build(
+            &log("info", LogFormat::Compact),
+            &args(None, None),
+            buf.clone(),
+        )
+        .unwrap();
+        tracing::subscriber::with_default(subscriber, || {
+            tracing::debug!("before");
+            logger.apply(&log("debug", LogFormat::Json));
+            tracing::debug!("after");
+        });
+        assert!(buf.lines("before").is_empty());
+        let after = buf.lines("after");
+        assert_eq!(after.len(), 1, "{after:?}");
+        let line: serde_json::Value = serde_json::from_str(&after[0]).unwrap();
+        assert_eq!(line["fields"]["message"], "after");
+    }
+
+    #[test]
+    fn the_command_line_wins_over_a_generation() {
+        let buf = Buf::default();
+        let cli = args(Some("info"), Some(LogFormatArg::Compact));
+        let (subscriber, logger) =
+            build(&log("info", LogFormat::Compact), &cli, buf.clone()).unwrap();
+        tracing::subscriber::with_default(subscriber, || {
+            logger.apply(&log("debug", LogFormat::Json));
+            tracing::debug!("quiet");
+            tracing::info!("loud");
+        });
+        assert!(buf.lines("quiet").is_empty());
+        let loud = buf.lines("loud");
+        assert_eq!(loud.len(), 1, "{loud:?}");
+        assert!(
+            serde_json::from_str::<serde_json::Value>(&loud[0]).is_err(),
+            "{loud:?}"
+        );
+    }
 }
```

Change `crates/data-avalanche/src/startup.rs`:

```diff
diff --git a/crates/data-avalanche/src/startup.rs b/crates/data-avalanche/src/startup.rs
index 40dcefb..986abff 100644
--- a/crates/data-avalanche/src/startup.rs
+++ b/crates/data-avalanche/src/startup.rs
@@ -271,4 +271,24 @@ mod tests {
             Ok(_) => panic!("a file on another backend joined the cluster"),
         }
     }
+
+    /// DA-OPS-8: the per-node keys a file has apart from the generation, and which one runs.
+    #[test]
+    fn the_per_node_keys_apart_from_the_generation_are_named() {
+        let generation = config();
+        assert!(per_node(&generation.clone(), &generation).is_empty());
+        let mut file = generation.clone();
+        file.globals.cluster.memberlist.node_name = Some("node-b".into());
+        file.globals.cluster.memberlist.bind_port = 7947;
+        file.globals.api.address.port = 8081;
+        file.globals.cluster.drain_timeout = Duration::from_secs(1);
+        assert_eq!(
+            per_node(&file, &generation),
+            [
+                "globals.cluster.memberlist.node_name (the file's)",
+                "globals.cluster.memberlist.bind_port (the generation's)",
+                "globals.api.address (the file's)",
+            ]
+        );
+    }
 }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche --lib`

Expected: FAIL to compile: no `Http.config` or `ConfigApi`, no `logging::build`, no `startup::per_node`.

```text
error: cannot find macro `json` in this scope
   --> crates/data-avalanche/src/http.rs:289:13
error[E0425]: cannot find type `Value` in this scope
error[E0425]: cannot find type `PathBuf` in this scope
error[E0425]: cannot find type `ConfigApi` in this scope
error[E0422]: cannot find struct, variant or union type `ConfigApi` in this scope
error[E0433]: cannot find type `OnceLock` in this scope
error[E0425]: cannot find type `Mutex` in this scope
error[E0405]: cannot find trait `MakeWriter` in this scope
error[E0433]: cannot find type `ConfigCoordinator` in this scope
error[E0433]: cannot find type `Value` in this scope
error[E0425]: cannot find function `build` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
index de83cf2..fa61451 100644
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -25,6 +25,7 @@ data-avalanche-storage.workspace = true
 data-avalanche-telemetry.workspace = true
 object_store.workspace = true
 rustls.workspace = true
+serde_json.workspace = true
 thiserror.workspace = true
 tokio = { workspace = true, features = ["signal", "net"] }
 tracing.workspace = true
@@ -41,7 +42,6 @@ data-avalanche-testkit.workspace = true
 flate2.workspace = true
 futures-util.workspace = true
 mysql_async.workspace = true
-serde_json.workspace = true
 tempfile.workspace = true
 tokio-postgres.workspace = true
 
```

Change `crates/data-avalanche/src/http.rs`:

```diff
diff --git a/crates/data-avalanche/src/http.rs b/crates/data-avalanche/src/http.rs
index 174dab3..996879c 100644
--- a/crates/data-avalanche/src/http.rs
+++ b/crates/data-avalanche/src/http.rs
@@ -1,18 +1,23 @@
-//! The minimal listener on `globals.api.address`: liveness, readiness and the metrics, all
-//! unauthenticated (System-Overview §Observability, API_RBAC §2). The `/v1` API, its auth and TLS
-//! arrive with M9.
+//! The minimal listener on `globals.api.address`: liveness, readiness, the metrics, and the
+//! config reload and its status (Clustering §9.2), all unauthenticated (System-Overview
+//! §Observability, API_RBAC §2). The rest of the `/v1` API, its auth and TLS arrive with M9.
 
-use std::sync::Arc;
+use std::collections::BTreeMap;
+use std::path::PathBuf;
 use std::sync::atomic::{AtomicBool, Ordering};
+use std::sync::{Arc, OnceLock};
 
 use axum::Router;
 use axum::extract::State;
 use axum::http::{StatusCode, header};
-use axum::response::IntoResponse;
-use axum::routing::get;
+use axum::response::{IntoResponse, Response};
+use axum::routing::{get, post};
+use data_avalanche_cluster::{ConfigCoordinator, Members, ReloadError, Reloaded};
 use data_avalanche_config::HostPort;
 use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
+use serde_json::{Value, json};
 use tokio::net::TcpListener;
+use tokio::sync::watch;
 
 use crate::run::Fatal;
 
@@ -22,6 +27,17 @@ pub(crate) struct Http {
     pub metrics: Metrics,
     /// Startup is done and every pipeline's controller runs (not: every pipeline is healthy).
     pub ready: Arc<AtomicBool>,
+    /// Set once the node runs its config generation; the config routes answer 503 before.
+    pub config: Arc<OnceLock<ConfigApi>>,
+}
+
+/// What the config routes act on.
+#[derive(Debug, Clone)]
+pub(crate) struct ConfigApi {
+    pub coordinator: Arc<ConfigCoordinator>,
+    pub members: watch::Receiver<Members>,
+    /// The node's file, read again by each reload (config §12.1).
+    pub file: PathBuf,
 }
 
 pub(crate) fn router(http: Http) -> Router {
@@ -29,9 +45,104 @@ pub(crate) fn router(http: Http) -> Router {
         .route("/health/live", get(|| async { "live\n" }))
         .route("/health/ready", get(ready))
         .route("/metrics", get(metrics))
+        .route("/v1/config/reload", post(reload))
+        .route("/v1/config/status", get(status))
         .with_state(http)
 }
 
+fn body(code: StatusCode, v: &Value) -> Response {
+    (
+        code,
+        [(header::CONTENT_TYPE, "application/json")],
+        format!("{v}\n"),
+    )
+        .into_response()
+}
+
+fn starting() -> Response {
+    body(
+        StatusCode::SERVICE_UNAVAILABLE,
+        &json!({ "error": "the node is starting" }),
+    )
+}
+
+/// Publishes the node's file as the next config generation: 202 with it, 200 when the file is
+/// the generation the cluster runs already, 422 with the validation report when it is invalid or
+/// changes a field only a process restart applies.
+async fn reload(State(http): State<Http>) -> Response {
+    let Some(api) = http.config.get() else {
+        return starting();
+    };
+    let text = match tokio::fs::read_to_string(&api.file).await {
+        Ok(text) => text,
+        Err(e) => {
+            return body(
+                StatusCode::INTERNAL_SERVER_ERROR,
+                &json!({ "error": format!("cannot read config file {}: {e}", api.file.display()) }),
+            );
+        }
+    };
+    match api.coordinator.reload(&text).await {
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
+async fn status(State(http): State<Http>) -> Response {
+    let Some(api) = http.config.get() else {
+        return starting();
+    };
+    let applied = api.coordinator.applied();
+    let generation = match api.coordinator.latest().await {
+        Ok(latest) => latest.unwrap_or(applied),
+        Err(e) => {
+            return body(
+                StatusCode::SERVICE_UNAVAILABLE,
+                &json!({ "error": format!("the config generation: {e}") }),
+            );
+        }
+    };
+    let nodes: BTreeMap<String, u64> = api
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
 async fn ready(State(http): State<Http>) -> impl IntoResponse {
     if http.ready.load(Ordering::SeqCst) {
         (StatusCode::OK, "ready\n")
```

Change `crates/data-avalanche/src/logging.rs`:

```diff
diff --git a/crates/data-avalanche/src/logging.rs b/crates/data-avalanche/src/logging.rs
index 02fa522..36b34f8 100644
--- a/crates/data-avalanche/src/logging.rs
+++ b/crates/data-avalanche/src/logging.rs
@@ -1,8 +1,14 @@
-//! Logs: `globals.log` (level, format), each overridden by the command line.
+//! Logs: `globals.log` (level, format), each overridden by the command line, and changed by a
+//! config generation (config §1.2: R).
+
+use std::sync::{Mutex, PoisonError};
 
 use data_avalanche_config::{Log, LogFormat};
-use tracing_subscriber::EnvFilter;
+use tracing::Subscriber;
 use tracing_subscriber::filter::LevelFilter;
+use tracing_subscriber::fmt::MakeWriter;
+use tracing_subscriber::layer::{Layered, SubscriberExt};
+use tracing_subscriber::{EnvFilter, Layer, Registry, reload};
 
 use crate::{LogFormatArg, RunArgs};
 
@@ -58,24 +64,111 @@ pub(crate) fn quieted(filter: &str) -> String {
     out
 }
 
-/// Installs the process's subscriber; logs go to stderr.
+type Filtered = Layered<reload::Layer<EnvFilter, Registry>, Registry>;
+type Format = Box<dyn Layer<Filtered> + Send + Sync>;
+
+/// The installed logger: its filter and its format change with each config generation's
+/// `globals.log`, except where the command line set them.
+pub(crate) struct Logger {
+    filter: reload::Handle<EnvFilter, Registry>,
+    format: reload::Handle<Format, Filtered>,
+    layer: Box<dyn Fn(LogFormat) -> Format + Send + Sync>,
+    args: RunArgs,
+    /// The level and the format in force.
+    current: Mutex<(String, LogFormat)>,
+}
+
+impl std::fmt::Debug for Logger {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("Logger")
+            .field("current", &self.current)
+            .finish_non_exhaustive()
+    }
+}
+
+fn filter(level: &str) -> Result<EnvFilter, String> {
+    EnvFilter::builder()
+        .parse(quieted(level))
+        .map_err(|e| format!("{level:?} isn't a log filter: {e}"))
+}
+
+fn layer<W>(format: LogFormat, writer: W) -> Format
+where
+    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
+{
+    let builder = tracing_subscriber::fmt::layer().with_writer(writer);
+    match format {
+        LogFormat::Json => builder.json().boxed(),
+        LogFormat::Compact => builder.compact().boxed(),
+        LogFormat::Pretty => builder.pretty().boxed(),
+    }
+}
+
+/// The process's subscriber, writing to `writer`, and its [`Logger`].
 ///
 /// # Errors
 /// A `--log-level` that isn't a filter (the config's was checked by V-017).
-pub(crate) fn init(log: &Log, args: &RunArgs) -> Result<(), String> {
+pub(crate) fn build<W>(
+    log: &Log,
+    args: &RunArgs,
+    writer: W,
+) -> Result<(impl Subscriber + Send + Sync + use<W>, Logger), String>
+where
+    W: for<'w> MakeWriter<'w> + Clone + Send + Sync + 'static,
+{
     let (level, format) = settings(log, args);
-    let filter = EnvFilter::builder()
-        .parse(quieted(&level))
-        .map_err(|e| format!("--log-level {level:?} isn't a log filter: {e}"))?;
-    let builder = tracing_subscriber::fmt()
-        .with_env_filter(filter)
-        .with_writer(std::io::stderr);
-    let installed = match format {
-        LogFormat::Json => builder.json().try_init(),
-        LogFormat::Compact => builder.compact().try_init(),
-        LogFormat::Pretty => builder.pretty().try_init(),
+    let (filter_layer, filter) =
+        reload::Layer::new(filter(&level).map_err(|e| format!("--log-level {e}"))?);
+    let (format_layer, format_handle) = reload::Layer::new(layer(format, writer.clone()));
+    let subscriber = tracing_subscriber::registry()
+        .with(filter_layer)
+        .with(format_layer);
+    let logger = Logger {
+        filter,
+        format: format_handle,
+        layer: Box::new(move |f| layer(f, writer.clone())),
+        args: args.clone(),
+        current: Mutex::new((level, format)),
     };
-    installed.map_err(|e| format!("cannot install the logger: {e}"))
+    Ok((subscriber, logger))
+}
+
+/// Installs the process's subscriber; logs go to stderr.
+///
+/// # Errors
+/// A `--log-level` that isn't a filter, or a subscriber is installed already.
+pub(crate) fn init(log: &Log, args: &RunArgs) -> Result<Logger, String> {
+    let (subscriber, logger) = build(log, args, std::io::stderr)?;
+    tracing::subscriber::set_global_default(subscriber)
+        .map_err(|e| format!("cannot install the logger: {e}"))?;
+    Ok(logger)
+}
+
+impl Logger {
+    /// Takes `log`, a config generation's, where the command line didn't set the level or the
+    /// format.
+    pub fn apply(&self, log: &Log) {
+        let (level, format) = settings(log, &self.args);
+        let mut current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
+        if level != current.0 {
+            match filter(&level).and_then(|f| self.filter.reload(f).map_err(|e| e.to_string())) {
+                Ok(()) => {
+                    tracing::info!("the log level is {level} now");
+                    current.0 = level;
+                }
+                Err(e) => tracing::warn!("the log level stays {}: {e}", current.0),
+            }
+        }
+        if format != current.1 {
+            match self.format.reload((self.layer)(format)) {
+                Ok(()) => {
+                    tracing::info!("the log format is {format} now");
+                    current.1 = format;
+                }
+                Err(e) => tracing::warn!("the log format stays {}: {e}", current.1),
+            }
+        }
+    }
 }
 
 #[cfg(test)]
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 235a269..aa60480 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -2,25 +2,26 @@
 
 use std::future::IntoFuture;
 use std::net::SocketAddr;
-use std::sync::Arc;
 use std::sync::atomic::{AtomicBool, Ordering};
+use std::sync::{Arc, OnceLock};
 use std::time::{Duration, SystemTime};
 
 use data_avalanche_cluster::{
-    ClusterReconciler, ControllerDeps, ControllerSettings, Gossip, GossipKey, GossipSettings,
-    LeaseTiming, NodeIdentity, NodeMeta, Phase, ReconcilerSettings, Seeds, Udp, advertise,
-    discover, interfaces, join as join_gossip, leave, node_identity,
+    ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
+    Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
+    ReconcilerSettings, Seeds, Udp, advertise, discover, interfaces, join as join_gossip, leave,
+    node_identity,
 };
 use data_avalanche_config::{
     Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
 };
 use data_avalanche_pipeline::{Backoff, Tuning};
-use data_avalanche_state::Leases;
+use data_avalanche_state::{ConfigGenerations, Leases};
 use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
 use tokio::sync::{Semaphore, oneshot};
 
-use crate::http::{self, Http};
+use crate::http::{self, ConfigApi, Http};
 use crate::startup::{Buckets, join};
 use crate::{Factory, RunArgs, logging};
 
@@ -109,7 +110,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     // 1. A first load with a throwaway key: only globals.state and the cluster settings matter.
     let first = load_str(&text, &ProcessEnv, &FingerprintKey::random())
         .map_err(|r| Fatal::Config(render(&r)))?;
-    logging::init(&first.config.globals.log, &args).map_err(Fatal::Config)?;
+    let logger = Arc::new(logging::init(&first.config.globals.log, &args).map_err(Fatal::Config)?);
     for w in &first.warnings {
         tracing::warn!("config: {}", line(w));
     }
@@ -119,6 +120,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     // The listener answers from here: live at once, ready once every controller runs.
     let metrics = Metrics::new();
     let ready = Arc::new(AtomicBool::new(false));
+    let config_api = Arc::new(OnceLock::new());
     let listener = http::bind(&first.config.globals.api.address).await?;
     let (stop_http, http_stopped) = oneshot::channel::<()>();
     let server = tokio::spawn(
@@ -127,6 +129,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             http::router(Http {
                 metrics: metrics.clone(),
                 ready: Arc::clone(&ready),
+                config: Arc::clone(&config_api),
             }),
         )
         .with_graceful_shutdown(async {
@@ -136,12 +139,13 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     );
     let holder = node_identity(&first.config.globals.cluster, started)
         .map_err(|e| Fatal::Config(e.to_string()))?;
-    let buckets = Buckets::new(&first.config.globals);
+    let buckets = Arc::new(Buckets::new(&first.config.globals));
     let joined = join(&buckets, &holder.node_id, &text, &first.config).await?;
     let config = joined.config;
     if let Some(why) = unsupported(&config) {
         return Err(Fatal::Config(why));
     }
+    logger.apply(&config.globals.log);
     tracing::info!(
         node = %holder.node_id,
         incarnation = holder.incarnation,
@@ -151,7 +155,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
 
     let c = &config.globals.cluster;
     let deps = ControllerDeps {
-        leases: Leases::new(joined.globals),
+        leases: Leases::new(joined.globals.clone()),
         holder,
         factory: Arc::new(Factory::new(&config.globals).map_err(Fatal::Config)?),
         metrics: metrics.clone(),
@@ -189,7 +193,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         addr,
         meta: NodeMeta {
             phase: Phase::Joining,
-            config_generation: joined.generation,
+            config_generation: joined.generation.doc.generation,
             build: env!("CARGO_PKG_VERSION").to_owned(),
         },
     };
@@ -206,11 +210,40 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         gossip.members(),
         ReconcilerSettings::new(c),
     );
+    // Each newer config generation, this node's reloads included (Clustering §9).
+    let docs = Arc::clone(&buckets);
+    let log = Arc::clone(&logger);
+    let coordinator = Arc::new(ConfigCoordinator::start(
+        config.clone(),
+        joined.generation,
+        CoordinatorDeps {
+            generations: ConfigGenerations::new(joined.globals.clone()),
+            env: Arc::new(ProcessEnv),
+            key: FingerprintKey::from_bytes(joined.fingerprint_key),
+            node_id: deps.holder.node_id.clone(),
+            docs: Arc::new(move |p| docs.pipeline_docs(p).map_err(|e| e.to_string())),
+            factory: Arc::clone(&deps.factory),
+            reloader: reconciler.reloader(),
+            advertiser: gossip.advertiser(),
+            metrics: metrics.clone(),
+            globals: Arc::new(move |g| log.apply(&g.log)),
+        },
+        gossip.members(),
+        c.lease_renewal_interval,
+    ));
+    let _ = config_api.set(ConfigApi {
+        coordinator: Arc::clone(&coordinator),
+        members: gossip.members(),
+        file: args.config.clone(),
+    });
     ready.store(true, Ordering::SeqCst);
     shutdown_signal().await;
     tracing::info!("shutting down: handing off every pipeline and releasing its lease");
     ready.store(false, Ordering::SeqCst);
     discovery.abort();
+    // No generation is adopted during the leave; it hands off what the last one configures.
+    coordinator.stop().await;
+    let config = coordinator.config().await;
     leave(gossip, reconciler, leave_within(&config)).await;
     let _ = stop_http.send(());
     let _ = server.await;
```

Change `crates/data-avalanche/src/startup.rs`:

```diff
diff --git a/crates/data-avalanche/src/startup.rs b/crates/data-avalanche/src/startup.rs
index 986abff..867147a 100644
--- a/crates/data-avalanche/src/startup.rs
+++ b/crates/data-avalanche/src/startup.rs
@@ -11,7 +11,8 @@ use data_avalanche_config::{
     StateLocation, check_cluster_anchor, load_str,
 };
 use data_avalanche_state::{
-    AnchorDoc, ConfigGenerations, DocStore, HashAlgo, PipelineDocs, establish_anchor,
+    AnchorDoc, ConfigGenerations, DocStore, GenerationDoc, HashAlgo, PipelineDocs, Versioned,
+    establish_anchor,
 };
 use data_avalanche_storage::{
     Backend, BucketSpec, Location, ObjectStoreVersioned, Timeouts, colocated_state, explicit_state,
@@ -107,7 +108,7 @@ pub(crate) struct Joined {
     /// The anchor's: the gossip's packets are authenticated with a key derived from it.
     pub fingerprint_key: [u8; 32],
     /// The config generation the node runs.
-    pub generation: u64,
+    pub generation: Versioned<GenerationDoc>,
 }
 
 /// The anchor's key prints as `..`, as `GossipKey`'s does: a `{joined:?}` logs no secret.
@@ -122,6 +123,48 @@ impl std::fmt::Debug for Joined {
     }
 }
 
+/// The per-node keys (config §6.5.1) whose value in this node's file isn't the generation's,
+/// each with the one the node takes: its name and its API address are the file's, its gossip's
+/// addresses the generation's (DA-OPS-8).
+pub(crate) fn per_node(file: &ResolvedConfig, generation: &ResolvedConfig) -> Vec<String> {
+    let (f, g) = (
+        &file.globals.cluster.memberlist,
+        &generation.globals.cluster.memberlist,
+    );
+    let keys = [
+        ("memberlist.node_name", f.node_name != g.node_name, "file"),
+        (
+            "memberlist.bind_addr",
+            f.bind_addr != g.bind_addr,
+            "generation",
+        ),
+        (
+            "memberlist.bind_port",
+            f.bind_port != g.bind_port,
+            "generation",
+        ),
+        (
+            "memberlist.advertise_addr",
+            f.advertise_addr != g.advertise_addr,
+            "generation",
+        ),
+        (
+            "memberlist.advertise_port",
+            f.advertise_port != g.advertise_port,
+            "generation",
+        ),
+    ];
+    let mut out: Vec<String> = keys
+        .iter()
+        .filter(|(_, differs, _)| *differs)
+        .map(|(key, _, from)| format!("globals.cluster.{key} (the {from}'s)"))
+        .collect();
+    if file.globals.api.address != generation.globals.api.address {
+        out.push("globals.api.address (the file's)".to_owned());
+    }
+    out
+}
+
 /// Steps 2–5 of the startup sequence; `first` is the config loaded with a throwaway key.
 pub(crate) async fn join(
     buckets: &Buckets,
@@ -173,12 +216,21 @@ pub(crate) async fn join(
         .config;
     // The buckets open the state on this file's backend and the sink opens the tables on the
     // generation's: the two must be one backend.
+    let n = generation.pointer.doc.generation;
+    let differ = per_node(first, &config);
+    if !differ.is_empty() {
+        tracing::warn!(
+            generation = n,
+            "this node's file and config generation {n} differ in per-node keys, {}: write per-node values as ${{VAR}} (config §6.5.1); a reload from this node is refused (V-061) until they match",
+            differ.join(", ")
+        );
+    }
     let (file, adopted) = (backend(g), backend(&config.globals));
     if file != adopted {
         return Err(Fatal::Config(format!(
             "this node's file names storage on {}, but the cluster runs config generation {} on {}: set globals.cloud and globals.azure in the file as the generation has them",
             named(&file),
-            generation.pointer.doc.generation,
+            n,
             named(&adopted)
         )));
     }
@@ -186,7 +238,7 @@ pub(crate) async fn join(
         config,
         globals,
         fingerprint_key: anchor.fingerprint_key,
-        generation: generation.pointer.doc.generation,
+        generation: generation.pointer,
     })
 }
 
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche --lib`

Expected: PASS, 21 tests.

```text
test result: ok. 21 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1192 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/Cargo.toml \
  crates/data-avalanche/src/http.rs \
  crates/data-avalanche/src/logging.rs \
  crates/data-avalanche/src/run.rs \
  crates/data-avalanche/src/startup.rs
git commit -m "feat(data-avalanche): POST /v1/config/reload and GET /v1/config/status, the log a generation changes, the coordinator between the join and the leave, DA-OPS-8's startup warning"
```

---

### Task 8: The reload E2E (`data-avalanche` tests)

**Files:**
- Modify: `crates/data-avalanche/tests/e2e/cluster.rs`
- Modify: `crates/data-avalanche/tests/e2e/harness.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`
- Create: `crates/data-avalanche/tests/e2e/reload.rs`
- Modify: `test-infra/README.md`

**Interfaces:**
- Consumes: the binary of Task 7.
- Produces (tests only): `E2e::config(&self) -> String`, `E2e::write_config(&self, text: &str)`,
  `E2e::slot_exists(&self, suffix: &str) -> Option<bool>` (`None` on MySQL); `Proc::reload(&self)
  -> (u16, serde_json::Value)`, `Proc::config_status(&self) -> serde_json::Value`. `Cluster`'s
  pipeline ids follow a reload (`Cluster.ids`, `lake(test, id)`).

D-M8b.12. The scenarios edit the one config file every node reads and call the endpoint on one
node; the others adopt the generation from `globals.state`. `cluster.rs`'s `Cluster` gets its
pipeline ids as a field, so `settled()` and `landed()` follow a reload, and `SPARES` drops `d`'s
slot before and after a run as it does `b`'s and `c`'s. `reload.rs` is the one-node scenario.

- [ ] **Step 1: Write the tests**

Change `crates/data-avalanche/tests/e2e/cluster.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/cluster.rs b/crates/data-avalanche/tests/e2e/cluster.rs
index d72741c..62648ec 100644
--- a/crates/data-avalanche/tests/e2e/cluster.rs
+++ b/crates/data-avalanche/tests/e2e/cluster.rs
@@ -2,7 +2,8 @@
 //! behind a [`Forwarder`], seeded with every node's forwarder. Three pipelines over one table
 //! spread over them. A crash (`kill -9`), a leave (SIGTERM), a partition (the forwarders drop a
 //! side's packets) and a stop-the-world pause (SIGSTOP) each end with every pipeline held by one
-//! live node, every row in the lake, and owner epochs that never go back in the Delta log.
+//! live node, every row in the lake, and owner epochs that never go back in the Delta log. A
+//! reload on one node reaches the three (Clustering §9.2).
 
 use std::collections::BTreeMap;
 use std::time::{Duration, Instant};
@@ -44,9 +45,23 @@ struct Node {
 /// The nodes are dropped (and print their logs on a failure) before the test's directory.
 struct Cluster {
     nodes: Vec<Node>,
+    /// The pipelines the config runs: the test's, `b`, `c`; a reload changes them.
+    ids: Vec<String>,
     e: E2e,
 }
 
+/// Each pipeline but the test's own has a source suffix and a lake of its name.
+const SPARES: [&str; 3] = ["b", "c", "d"];
+
+/// A pipeline's lake.
+fn lake(test: &str, id: &str) -> String {
+    if id == test {
+        "lake".to_owned()
+    } else {
+        format!("lake_{id}")
+    }
+}
+
 impl Cluster {
     /// `n1`, `n2` and `n3`, not started.
     async fn new(test: &str) -> Self {
@@ -81,10 +96,11 @@ impl Cluster {
             },
         )
         .await;
-        for suffix in ["_b", "_c"] {
-            e.drop_source(suffix).await;
+        for id in SPARES {
+            e.drop_source(&format!("_{id}")).await;
         }
-        Self { nodes, e }
+        let ids = vec![e.test.clone(), "b".into(), "c".into()];
+        Self { nodes, ids, e }
     }
 
     async fn start(&mut self, i: usize) {
@@ -108,8 +124,8 @@ impl Cluster {
         self.settled().await;
     }
 
-    fn ids(&self) -> [String; 3] {
-        [self.e.test.clone(), "b".into(), "c".into()]
+    fn ids(&self) -> Vec<String> {
+        self.ids.clone()
     }
 
     /// Each pipeline's lease: its holder and epoch, unless released.
@@ -188,7 +204,9 @@ impl Cluster {
 
     /// Every lake's `items` holds the source's rows, and its owner epochs never went back.
     async fn landed(&self) {
-        for lake in ["lake", "lake_b", "lake_c"] {
+        for id in self.ids() {
+            let lake = lake(&self.e.test, &id);
+            let lake = lake.as_str();
             self.e
                 .converged_in(lake, "items", Duration::from_secs(60))
                 .await;
@@ -220,8 +238,8 @@ impl Cluster {
                 assert!(p.terminate().await.success(), "{} stops", n.name);
             }
         }
-        for suffix in ["_b", "_c"] {
-            self.e.drop_source(suffix).await;
+        for id in SPARES {
+            self.e.drop_source(&format!("_{id}")).await;
         }
         self.e.cleanup().await;
     }
@@ -420,3 +438,91 @@ async fn a_paused_holder_loses_its_pipelines_and_rejoins() {
     c.landed().await;
     c.stop().await;
 }
+
+/// A reload on `n2` (Clustering §9.2): its file removes `c`, adds `d` and renames the test's
+/// pipeline (an R change, config §12.3). Every node applies generation 2 and reports it; `c`'s
+/// owner releases its lease and drops its slot (`auto_cleanup`); `d` runs on the node the ring
+/// places it on, its initial snapshot and its stream together holding every row; the renamed
+/// pipeline keeps its lease and epoch, and is scraped by its new name.
+#[tokio::test]
+async fn a_reload_reaches_every_node() {
+    require_it!();
+    let mut c = Cluster::new("cluster_reload").await;
+    c.start_all().await;
+    c.e.exec("INSERT INTO items VALUES (1, 'a')").await;
+    c.landed().await;
+    let before = c.holders().await;
+    let test = c.e.test.clone();
+
+    let text = c.e.config();
+    let at = text.find("  c:\n").expect("pipeline c");
+    let d = text[at..]
+        .replace("  c:\n    id: c\n", "  d:\n    id: d\n")
+        .replace(&format!("{}_c", c.e.schema), &format!("{}_d", c.e.schema))
+        .replace("/lake_c,", "/lake_d,")
+        // A pipeline reports `running` from its start, before it reads (the status metric):
+        // the snapshot takes what is in the table then, the stream what comes after.
+        .replace("snapshots: { mode: never }", "snapshots: { mode: initial }");
+    let renamed = format!("{test}_renamed");
+    let next = text[..at].replace(
+        &format!("  {test}:\n    id: {test}\n"),
+        &format!("  {renamed}:\n    id: {test}\n"),
+    ) + &d;
+    assert_ne!(next, text);
+    c.e.write_config(&next);
+    let (code, v) = c.nodes[1].proc.as_ref().unwrap().reload().await;
+    assert_eq!(
+        (code, &v),
+        (202, &serde_json::json!({ "generation": 2 })),
+        "{v}"
+    );
+
+    for n in c.live() {
+        let p = n.proc.as_ref().unwrap();
+        p.metric("data_avalanche_config_generation 2").await;
+    }
+    eventually(
+        "every node reports generation 2",
+        Duration::from_secs(30),
+        async || {
+            let s = c.nodes[0].proc.as_ref().unwrap().config_status().await;
+            s["converged"] == true
+                && s["generation"] == 2
+                && s["nodes"].as_object().map(|m| m.len()) == Some(3)
+        },
+    )
+    .await;
+    c.ids = vec![test.clone(), "b".into(), "d".into()];
+    c.settled().await;
+    let after = c.holders().await;
+    assert_eq!(
+        after[&test], before[&test],
+        "renamed in place: the lease and epoch stay"
+    );
+    assert_eq!(after["b"], before["b"]);
+    eventually(
+        "c's lease is released",
+        Duration::from_secs(30),
+        async || c.e.lease_of("c").await.is_some_and(|l| l.doc.released),
+    )
+    .await;
+    if let Some(exists) = c.e.slot_exists("_c").await {
+        assert!(!exists, "c's slot is dropped (auto_cleanup)");
+    }
+    let holder = c.index(&after[&test].0);
+    c.nodes[holder]
+        .proc
+        .as_ref()
+        .unwrap()
+        .metric(&c.e.status_of(&renamed, "running"))
+        .await;
+
+    c.e.exec("INSERT INTO items VALUES (2, 'b')").await;
+    c.landed().await;
+    assert_eq!(
+        c.e.ids_in(&lake(&test, "c"), "items").await,
+        [1],
+        "c is gone"
+    );
+    c.stop().await;
+}
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index 32169fa..21c7651 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -178,6 +178,16 @@ impl E2e {
         self.dir.path().join("config.yaml")
     }
 
+    /// The config file every node reads.
+    pub fn config(&self) -> String {
+        std::fs::read_to_string(self.config_path()).expect("the config file")
+    }
+
+    /// Rewrites the config file: what the next reload publishes (Clustering §9.2).
+    pub fn write_config(&self, text: &str) {
+        std::fs::write(self.config_path(), text).expect("the config file");
+    }
+
     /// The `source` block of a pipeline named `test` (on PostgreSQL the slot and publication
     /// `e2e_<test>` take `suffix`; on MySQL it is ignored, both pipelines share the test's
     /// database and user), indented for a pipeline under `pipelines:`; `extra` is one more line of
@@ -633,6 +643,26 @@ pipelines:
         }
     }
 
+    /// On PostgreSQL, whether the slot of the pipeline whose source block took `suffix` exists;
+    /// `None` on MySQL, which has none.
+    pub async fn slot_exists(&self, suffix: &str) -> Option<bool> {
+        let Source::Postgres { .. } = self.source else {
+            return None;
+        };
+        let slot = format!("{}{suffix}", self.schema);
+        let n = self
+            .db
+            .pg()
+            .query_one(
+                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
+                &[&slot],
+            )
+            .await
+            .expect("slots read")
+            .get::<_, i64>(0);
+        Some(n > 0)
+    }
+
     /// The pipeline's own documents, co-located with the lake (`<sink.prefix>/.pipeline-state`).
     pub fn docs(&self) -> PipelineDocs {
         let at = colocated_state(TEST_BUCKET, &self.lake_prefix());
@@ -840,11 +870,19 @@ impl Proc {
     }
 
     async fn get(&self, path: &str) -> Option<(u16, String)> {
+        self.request("GET", path).await
+    }
+
+    /// A request without a body: the status code and the body.
+    async fn request(&self, method: &str, path: &str) -> Option<(u16, String)> {
         let mut s = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
             .await
             .ok()?;
         s.write_all(
-            format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes(),
+            format!(
+                "{method} {path} HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
+            )
+            .as_bytes(),
         )
         .await
         .ok()?;
@@ -880,6 +918,26 @@ impl Proc {
         self.get("/metrics").await.expect("/metrics answers").1
     }
 
+    /// `POST /v1/config/reload`: the status code and the JSON body.
+    pub async fn reload(&self) -> (u16, serde_json::Value) {
+        let (code, body) = self
+            .request("POST", "/v1/config/reload")
+            .await
+            .expect("the reload answers");
+        let v = serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}"));
+        (code, v)
+    }
+
+    /// `GET /v1/config/status`'s JSON body.
+    pub async fn config_status(&self) -> serde_json::Value {
+        let (code, body) = self
+            .get("/v1/config/status")
+            .await
+            .expect("the status answers");
+        assert_eq!(code, 200, "{body}");
+        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}"))
+    }
+
     /// Waits until the metrics have a line equal to `line`.
     pub async fn metric(&self, line: &str) {
         let what = format!("the metric line {line}");
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index 6edb979..7e5d64b 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -14,6 +14,7 @@ mod harness;
 mod lifecycle;
 mod mysql;
 mod proxy;
+mod reload;
 mod schema;
 mod smoke;
 mod snapshots;
```

Create `crates/data-avalanche/tests/e2e/reload.rs`:

```rust
//! A reload on one node (config §12, Clustering §9): a restart-only change is refused and
//! nothing is published; a pipeline change restarts the pipeline under its lease; a restart runs
//! the cluster's generation, not the file (D-3), until a reload publishes the file.

use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

#[tokio::test]
async fn a_reload_publishes_the_file_and_a_restart_keeps_the_generation() {
    require_it!();
    let e = E2e::new(
        "reload_one",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.converged("items").await;
    let first = e.config();
    let epoch = e.lease().await.expect("a lease").doc.owner_epoch;

    // A field only a process restart applies (V-061): refused, nothing published.
    e.write_config(&first.replace("lease_duration: 6s", "tokens: 64\n    lease_duration: 6s"));
    let (code, v) = p.reload().await;
    assert_eq!(code, 422, "{v}");
    let errors = v["report"]["errors"].as_array().expect("the report");
    assert!(
        errors
            .iter()
            .any(|d| d["rule"] == "V-061" && d["path"] == "globals.cluster.tokens"),
        "{v}"
    );
    assert_eq!(p.config_status().await["generation"], 1);

    // A source field (PR): the pipeline restarts under the lease it holds.
    let timeouts = first.replacen(
        "      ssl_mode: disable\n",
        "      ssl_mode: disable\n      timeouts: { command: 45s }\n",
        1,
    );
    assert_ne!(timeouts, first);
    e.write_config(&timeouts);
    let (code, v) = p.reload().await;
    assert_eq!(
        (code, &v),
        (202, &serde_json::json!({ "generation": 2 })),
        "{v}"
    );
    let (code, v) = p.reload().await;
    assert_eq!(
        (code, &v),
        (
            200,
            &serde_json::json!({ "generation": 2, "unchanged": true })
        ),
        "the file is the generation already"
    );
    eventually(
        "the pipeline restarted",
        Duration::from_secs(30),
        async || p.log().contains("restarted for a reload"),
    )
    .await;
    p.metric("data_avalanche_config_generation 2").await;
    p.metric(&e.status("running")).await;
    e.exec("INSERT INTO items VALUES (2, 'b')").await;
    e.converged("items").await;
    assert_eq!(
        e.lease().await.expect("a lease").doc.owner_epoch,
        epoch,
        "restarted under the same lease"
    );

    // A restart doesn't apply the file: the node runs generation 2 until a reload.
    e.write_config(&first);
    assert!(p.terminate().await.success());
    let mut p = e.start(&[]);
    p.ready().await;
    let s = p.config_status().await;
    assert_eq!(
        (&s["generation"], &s["applied"]),
        (&2.into(), &2.into()),
        "{s}"
    );
    assert!(
        p.log().contains(
            "the cluster runs config generation 2 from globals.state, not this node's file"
        ),
        "{}",
        p.log()
    );
    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    e.converged("items").await;
    let (code, v) = p.reload().await;
    assert_eq!(
        (code, &v),
        (202, &serde_json::json!({ "generation": 3 })),
        "{v}"
    );
    p.metric("data_avalanche_config_generation 3").await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
```

- [ ] **Step 2: Build the suite**

Run: `cargo build -q -p data-avalanche --features e2e-hooks --tests`

Expected: it builds. The behaviour is Task 7's binary, so the scenarios pass on arrival (Step 4).

- [ ] **Step 3: Document the suite**

Change `test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
index ed21cfd..cab6d44 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -98,6 +98,11 @@ three nodes as three processes on loopback, each behind a test-only UDP forwarde
 (`forwarder.rs`) that drops a chosen side's gossip (a partition); they also `kill -9`, SIGTERM
 and SIGSTOP the nodes. They need no service beyond the source and RustFS.
 
+The reload scenarios (`cluster.rs`'s `a_reload_reaches_every_node`, and `reload.rs`) rewrite that
+one config file and call `POST /v1/config/reload` on one node; the others adopt the new config
+generation from `globals.state`, and `GET /v1/config/status` reports each node's. The endpoints
+are unauthenticated until M9.
+
 ## GCS: a real bucket, not an emulator
 
 `fake-gcs-server` 1.56.1 rejects `object_store`'s path-style PUT (`400 invalid uploadType`) and
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 reload`

Expected: PASS, 2 tests.

```text
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 37 filtered out; finished in 37.41s
```

Run: `cargo test --workspace`

Expected: PASS, every test binary (1192 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests/e2e/cluster.rs \
  crates/data-avalanche/tests/e2e/harness.rs \
  crates/data-avalanche/tests/e2e/main.rs \
  crates/data-avalanche/tests/e2e/reload.rs \
  test-infra/README.md
git commit -m "test(e2e): a reload on one of three node processes reaches every node; on one node a restart-only change is refused, a pipeline change restarts under its lease, and a restart keeps the generation"
```

---
## Exit

- [x] A reload on any node publishes the next config generation, and every node applies it (Tasks 5, 6, 8).
- [x] R fields change in place, in the running worker and source; a PR change restarts the pipeline under its lease; an added pipeline runs where the ring places it; a removed one is drained, torn down by its owner and released (Tasks 1–4, 6, 8).
- [x] A PROC change, or a file that doesn't load, is refused with its report, and nothing is published (Tasks 5, 7, 8).
- [x] `GET /v1/config/status` reports the cluster's generation and each member's applied one (Tasks 7, 8).
- [x] A process restart runs the cluster's generation, not the file; a node whose file differs from the generation in a per-node key is warned at startup (Tasks 7, 8).
- [x] The E2E suite passes on PostgreSQL 18 and, through `ci.yml`'s jobs, on `mysql-84`, `mysql-84-nogtid` and `mariadb-118`. (Measured locally on PostgreSQL 18 and `mysql-84`; every target passed in the PR's CI at 6227f06, run 39920129413.)
- [x] Every Review Focus line has its passing test.
- [x] The documents of "Documents this plan changes" are updated.
- [x] The workspace gates pass: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`, `cargo test --workspace`.

## Deviations during execution

Recorded from the SDD ledger. Each task's code was applied as verified; every change below came
from a review, and each fix is its own commit after the task's.

| Task | Commit | Deviation |
|---|---|---|
| 3 | 04e93c3 | A removal's teardown checks the lease (`may_commit`) before every attempt, not only before the first: the retries run up to `contended_window`, and a lease lost meanwhile would have let the teardown drop a new holder's slot. On a lost lease the teardown stops, warns "removed after its lease was lost" and releases nothing (D-M8b.3: "a lease lost meanwhile leaves the source as it is"). Test `a_teardown_stops_once_the_lease_is_lost`; the testkit's fake source records `Method::Teardown`. |

**The whole-branch review** (4578b24..a514627): no Critical; two Important findings and one Minor,
all three fixed before the PR. The scoped re-review of those fixes found one more Important, fixed
in a second round on Oleksii's go. The other minors are left for later, below.

| Commit | Fix |
|---|---|
| 775b2f2 | **An `id` change that keeps its resources is sequenced** (Oleksii's choice: sequence it; config §9.1, `id`: delete, then recreate). The diff gave `Remove(X)` and `Add(Y)`, and Y started at once, beside X's drain and teardown: with `slot_on_orphan: halt` Y refused X's slot as an orphan, otherwise X's teardown could drop the slot Y streamed from. Now, in one reload, an added Y *follows* a removed X when `shares(X, Y)`: both PostgreSQL with the same host, port and db and an equal slot or publication; the same sink bucket and prefix; or the same resolved state location (config §10: a co-located state is under the sink prefix). Y's controller waits, before its first claim, until X's lease is absent, released, or unchanged in version for a lease duration (Clustering §5.3's rule: versions, not clocks), read every `renewal_interval`; it still answers its target and updates meanwhile. Every node applies the same reload, so every node computes the same followers. Tests `a_pipeline_shares_a_slot_a_publication_a_sink_or_a_state`, `a_pipeline_taking_a_removed_ones_slot_starts_once_the_removal_lets_go` (X's teardown before Y's first build), `a_pipeline_added_that_shares_nothing_starts_during_a_removal`, `a_followed_lease_never_renewed_is_let_go_after_a_lease_duration`, `a_follower_removed_while_it_waits_ends`. |
| 92c5f2b | A removal whose drain was cut off by `drain_timeout` leaves the source and the lease as they are: no teardown, no release. Test `a_removal_with_its_drain_cut_off_leaves_the_source`, checked by inverting the branch (the test then fails). |
| 21083bd | An `Add` for an id that still has a controller restarts it with the added config (logged at `ERROR`), instead of removing it, which tore down its slot. The config diff never produces it; the fallback is no longer destructive. Test `an_add_for_a_running_pipeline_restarts_it` (no teardown, the same `owner_epoch`). |
| 1d4a13c | **A wait on a removed pipeline ends once a later generation adds its id again.** The re-review found that the list of followed ids, kept by the node for a respawn, was cleared only on the node that finished the wait: once a later reload added a new pipeline under X's id, its lease renewed for ever, and any later move or respawn of Y waited on it for ever, silently on standby. The list is now one `Following` shared by the reconciler's `Controlled` and the controller: every `Add` drops its id from every wait, and the list is cleared when a wait ends. A wait past three lease durations warns once a lease duration. Tests `a_wait_on_an_id_a_later_generation_adds_again_ends`, `a_follower_started_again_after_its_wait_waits_no_more`. |

Left for later:
- **What the `id`-change sequencing doesn't cover** (DA-OPS-10, gap tracker §39.1):
  - Only removals in the same reload count. A node that starts or joins during the wait builds Y
    without one, and an operator who removes X in one reload and adds Y in a later one gets none.
  - X's lease between two holders: released by one node, not yet claimed by a node that hasn't
    adopted the reload. Y can start while that node claims X briefly, then removes it. The window
    is one poll or one gossip nudge, as DA-OPS-9's hand-off.
  - A removal whose drain was cut off doesn't tear down (92c5f2b), so Y starts after a lease
    duration with X's slot still there, and with `slot_on_orphan: halt` halts on it.
  - A later reload that reuses X's id while X still drains ends the wait at once: Y may start up
    to `drain_timeout` + `contended_window` early. The alternative the re-review suggested, X's
    `owner_epoch` at the first read, would block a node that first reads after the reused X
    claimed, for ever again (ruling in the ledger).
  - An add-back under its own id waits for its own removal only, not also for another removed
    pipeline it shares with; a removal of a pending add-back is matched with the add-back's
    config.
  - The long-wait warning has no test (no log capture in these tests).
  - M11's operator docs: before reusing a removed pipeline's id, or adding in a later reload a
    pipeline that takes its slot, publication, sink or state, wait for its lease's release.
- The review's other minors, for a later milestone:
  - the session swaps its buffer semaphore and re-arms its timers on every reload, not only when
    the buffer or the intervals changed;
  - D-M8b.7's "the same text resolves differently" is diffed against the running config, which
    differs from the generation's when this node refused that generation;
  - a generation this node refused is tried again on every membership change, not only at the
    poll (two `resolve_tables` per changed pipeline each time);
  - the coordinator's lock is held across the catalog reads of `resolve_tables`, so `config()`
    and `stop()` wait behind them (bounded by the command timeout);
  - a generation pointer older than the applied one (a restored bucket) should be logged at
    `ERROR`; the applied number goes backward;
  - a loaded file's warnings are dropped, and its docs are checked only after it is published;
  - `latest()` reads the generation's snapshot text to answer its number;
  - DA-OPS-9's rolling-deploy variant: a removal adopted during a node's leave.

**Counts.** From Task 3 on, the workspace runs one more test than the plan says (Task 3's fix:
1170, 1175, 1188, 1189, 1193 and 1193); after the whole-branch review's fixes, 1202, against the
plan's 1192.

**Measured at the end of execution.** At 1d4a13c (the whole-branch review's fixes), the full E2E
suite passed 39/39 on PostgreSQL 18 (450.63 s) and on MySQL 8.4 (406.90 s), with no stray process;
the workspace runs 1202 tests and the gates are clean. Before the second round, at 21083bd, it
passed 39/39 on PostgreSQL 18 (452.95 s) and MySQL 8.4 (411.10 s). `mysql-84-nogtid` and
`mariadb-118` run in the PR's CI.
