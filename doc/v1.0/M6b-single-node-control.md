---
updated: 2026-10-08
domain: Clustering
tags: [v1, implementation-plan, m6b, single-node, lease, renewer, pipeline-controller, lifecycle, binary, http, metrics, e2e, delta, postgresql, position-guard, watchdog]
resolves: [DA-SNAP-11, DA-SRC-16, DA-X-12]
sources: []
---

# M6b — Single-Node Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put M6a's pipeline worker under control and ship slice 1 (PostgreSQL → Delta, one node).
This means:
- a lease per pipeline, kept by a renewer that answers the worker's `CommitGuard`;
- a `PipelineController` that runs the worker while it holds the lease and reconciles the
  persisted desired state (running, paused, stopped);
- a single-node `ClusterReconciler`;
- the binary `data-avalanche run --config <path>`, with a minimal HTTP listener
  (`/health/live`, `/health/ready`, `/metrics`) and the metrics for everything built so far;
- the five items carried from M4 and M6a's notes;
- the owner's answers to the planning findings (2026-10-08): the position guard skips a
  replayed transaction, a watchdog reconnects a stream whose source stopped answering, and the
  Delta libraries stay quiet at `info`.

The exit is the slice-1 E2E suite (roadmap §5.3, all 10 scenarios): the binary as a process
against PostgreSQL 18 and RustFS on every pull request, and against PostgreSQL 14–17 weekly.

**Architecture:** Small changes in five existing crates, then three new crates.

- **`data-avalanche-core`**:
  - A change set carries its table's last change (`PositionRange::last_change`), which the
    position guard compares; the marker still records the frontier (Q-M6b-9).
  - `SourceConnector::retained` reports the slot's retained WAL and whether a stream reads it.
  - `Sink::evolve` takes the writer's `owner_epoch` and can answer `EvolveOutcome::Fenced`
    (Q-M6b-7).
  - `Sink::stats` reports a sink's MERGE cost (`SinkStats`).
  - The `SinkTableState::marker` contract: `None` never means "never written".
- **`data-avalanche-sink-delta`**:
  - The evolve fence.
  - A table re-created behind a live handle is detected at refresh by the commit file's e-tag,
    and the handle is re-opened (Q-M6b-8). Any `Transient` drops the handle.
  - A table dropped since the refresh is `NoTable`.
  - A change set keyed otherwise than the table is refused.
  - A MERGE over `merge_memory_limit` is `Permanent`, so its table halts (Q-M6b-6).
  - A table is loaded only when its log exists, so a first start logs no kernel error.
- **`data-avalanche-source-pg`**: the catalog connect is bounded by `timeouts.connect` as a
  whole; the heartbeat's read of the slot reports `active`.
- **`data-avalanche-config`**: `sink.delta.merge_memory_limit` and V-119 (≥ 16 MiB when set).
- **`data-avalanche-pipeline`** (the worker):
  - `Command::Pause` and `Command::Resume`: the pause finishes the open transaction, flushes,
    makes it durable, then reads nothing, while the source connection stays open (Q-M6b-1).
  - Every Phase 0 state write is gated by the `CommitGuard`.
  - Warnings are kept once each and capped.
  - `WorkerError::recovery()` classifies every error as `Retry`, `Lease` or `Error` (Q-M6b-3).
  - The heartbeat's read runs beside the loop, and a watchdog ends a session whose stream is
    silent while the source doesn't answer, so the worker reconnects (Q-M6b-11, DA-SRC-16).
- **`data-avalanche-state`**: `PipelineDocs::state()` reads `pipeline-state.json`, which holds
  the desired state.
- **`data-avalanche-telemetry`** (new): the OpenMetrics registry and the M6b series. It depends
  on no workspace crate.
- **`data-avalanche-cluster`** (new):
  - `lease`: the renewer (Clustering §19) and the `LeaseGuard` it drives.
  - `claim`: acquire, takeover after a full lease duration, release.
  - `controller`: `PipelineController`, the only lease writer. It runs the worker, polls the
    desired state each `lease_renewal_interval` (Q-M6b-2), and maps the worker's errors to
    rerun, standby or ERROR.
  - `reconciler`: `ClusterReconciler` with a self-only ring, and this node's identity.
- **`data-avalanche`** (new, the binary):
  - the CLI, logs (a bare `info` caps the Delta libraries, Q-M6b-10), and the M2 startup
    sequence against real buckets;
  - the wiring of `source-pg` and `sink-delta` (the only crate that links both);
  - the HTTP listener and the signal handling;
  - behind the `e2e-hooks` feature, crash points that abort the process (Q-M6b-5).
- **Tests:**
  - Unit tests in every module. The cluster's run on tokio's paused clock against a scripted
    state store.
  - Live Delta tests on the local directory, RustFS and Azurite.
  - The pipeline's `tests/pg` gains a pause test.
  - `crates/data-avalanche/tests/e2e` runs the binary as a process against PostgreSQL and RustFS:
    26 scenarios, about 5 minutes. `ci.yml` runs it on every pull request, and `weekly.yml` runs
    it on PostgreSQL 14–17.

**Tech Stack:** Rust 1.98.1 (edition 2024). New external crates (exact pins in `Cargo.toml`):

| Crate | Pin | Used for |
|---|---|---|
| `prometheus-client` | `=0.25.1` | The metrics registry and its OpenMetrics text (telemetry) |
| `axum` | `0.8.9`, `default-features = false`, features `http1`, `tokio` | The HTTP listener |
| `clap` | `4.6.7`, feature `derive` | The command line |
| `gethostname` | `1.1` | The node's id when `memberlist.node_name` is unset |
| `tracing` | `0.1` | Logging from the controller, the reconciler and the binary |

Already in the workspace and newly used here: `tracing-subscriber` (features `fmt`, `json`
added), `tokio` (`signal`, `net`; `test-util` as a dev feature in `pipeline` and `cluster`),
`object_store`, `rustls`. Dev-only in the binary: `arrow-array`, `arrow-cast`, `futures-util`,
`serde_json`, `tempfile`, `tokio-postgres`. The E2E types scenario reads Delta with DuckDB
`1.5.6` through `uv`, as M4's cross-read does. The CI integration job already installs it.

**Spec:**
- `doc/v1.0/00-roadmap.md`: §2 (layout, dependency direction), §3.4, §3.7, §3.10, §3.11,
  §4 M6b (scope, "Carried from M4", exit), §5.3 (the 10 scenarios), §5.4 (CI).
- `doc/specs/System-Overview.md`: Management API endpoints (health, metrics), the
  Observability catalogue, lifecycle (desired state).
- `doc/specs/Configuration_Specification.md`: §6.3 `globals.api` (health and metrics
  unauthenticated, `tls`), §6.4 `globals.log`, §6.5 `globals.cluster` (`lease_duration`,
  `lease_renewal_interval`, `lease_safety_margin`, `max_concurrent_takeovers`,
  `drain_timeout`, `memberlist.node_name`), §9.2 `source` (`timeouts`), §9.2.1 `source.postgres`
  (`heartbeat`, `max_wal_lag_bytes`), §9.3
  `tables` (`snapshots.mode`), §9.4 `sink` (`max_transaction_*`), §9.4.3 `sink.delta`, §13.3,
  V-073.
- `doc/specs/Data-Flow.md`: Phase 0, Phase 4 (fence, position guard), Phase 5 (the checkpoint
  rule), §Error Paths.
- `doc/research/Clustering_Architecture_Research.md`: §3.4 (node id and incarnation), §5.2–5.3
  (the lease protocol), §10 (single-node), §19 (renew outcomes, the safety deadline, the
  renewer, commit gating), §20.1–20.2 (controller and reconciler; takeover rate limiting).
- `doc/research/Pipeline_Lifecycle_State_Machine.md`: §1–§5.
- `doc/research/Multi_Sink_Architecture_and_Delta_Lake.md` (the Delta realization) and
  `M4-delta-sink.md` "Deviations" (the carried items).
- `doc/research/Observability_Metrics_and_Tracing.md` (metric names and labels).
- `doc/research/Delivery_Guarantee_and_Position_Guard.md` §2–§3.
- `doc/v1.0/M6a-pipeline-worker.md` (the worker, its hooks, the notes carried here).

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. `cargo clippy --workspace
  --all-targets -- -D warnings` and `cargo fmt --all --check` must pass before every commit. The
  binary is also linted with its feature: `cargo clippy -p data-avalanche --features e2e-hooks
  --all-targets -- -D warnings`.
- **Dependency direction** (roadmap §2):
  - `cluster → pipeline, state, storage, config, core, telemetry`.
  - `telemetry` depends on no workspace crate.
  - Only `data-avalanche` links `sink-delta` and `source-pg`. No other crate takes either,
    even as a dev-dependency, apart from the pipeline's existing live tests on `source-pg`.
- **No write outside the lease:**
  - Every sink commit, every evolve and every state write, Phase 0's included, happens only
    while the `CommitGuard` allows it.
  - The guard refuses past its deadline on its own, with no renewer turn needed.
  - The controller is the only writer of the lease.
- **Ordering** (Data-Flow Phase 5, unchanged from M6a): commit, then halts, then the checkpoint,
  then the acknowledgment.
- **Timing tests** run on tokio's paused clock (`start_paused = true`) wherever the code under
  test uses tokio time. A real-clock wait in a unit test is a bug.
- **Test gates:**
  - Plain `cargo test` stays hermetic.
  - The live tests gate on `DATA_AVALANCHE_IT` (`test-infra/up.sh`); PostgreSQL is
    `postgres-18` or the port `DATA_AVALANCHE_PG_PORT` names.
  - The E2E binary needs `--features e2e-hooks` and `--test-threads=1`.
  - One E2E test changes a server-wide setting (`max_slot_wal_keep_size`), and it restores the
    setting before any assertion.
  - One E2E test freezes the PostgreSQL container (`docker pause`); a guard unpauses it on
    every path out of the test, a panic included.
- **`e2e-hooks` is test-only:** off by default and never in a release build. Without it, the
  process can't be crashed by an environment variable.
- **Credentials:** the tests use the compose services' fixed test credentials (testkit
  constants). No key, key path or secret goes in code, docs or output.
- **Public-API conventions:** public types are re-exported at their crate root. Library errors
  are `thiserror`. The binary's are `Fatal { Config, Startup }`, with exit codes 2 and 1.
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are
  `git apply`-ready. `Cargo.lock` follows the build and is committed with each task; it isn't
  shown.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs and failure modes most likely to bite an operator. Each is pinned by a test
in the task after the arrow.

1. **A node that lost its lease keeps writing.** This could be a sink commit, a Phase 0 state
   document, or an ADD COLUMN. A paused renewer, a hung store or a newer owner must stop every
   one of them before the successor writes.
   → Task 1 `an_evolve_of_a_table_a_newer_owner_wrote_is_fenced`; Task 6
   `a_lease_lost_before_any_phase_0_write_stops_it_there`; Task 8
   `the_guard_refuses_past_its_deadline_on_its_own`,
   `a_renewal_that_never_answers_loses_the_lease_at_the_deadline`.
2. **A renewal whose outcome is unknown.** The write timed out or failed, but it landed. The
   lease is still this node's: a false loss would stop a healthy pipeline, and a false hold would
   let two run.
   → Task 8 `a_renewal_that_landed_behind_an_error_is_still_held`; Task 9
   `a_takeover_that_landed_behind_an_error_is_this_nodes`.
3. **A paused pipeline.** It must cost nothing, keep its source connection past
   `wal_sender_timeout`, and resume where it stopped.
   → Task 5 `a_paused_session_idles`, `a_pause_keeps_the_connection_past_wal_sender_timeout`;
   Task 20 `pause_stop_and_resume_hold_across_a_restart`.
4. **A process killed at any point.** On restart the lake converges to the source with each key
   once: mid-snapshot, between a commit and its checkpoint, between the tables of one flush,
   between the checkpoint and the ack, and under load.
   → Task 17 (five scenarios), Task 16
   `a_split_transaction_is_replayed_after_a_crash_between_slices`.
5. **A Delta table changed behind the process.** It was dropped, re-created past the handle's
   version, or deleted from the lake: the next commit or snapshot must see the real table.
   → Task 2 `a_live_handle_recovers_from_a_table_recreated_past_its_version`,
   `a_table_dropped_since_the_refresh_is_no_table`; Task 15
   `dump_initial_dumps_until_the_lake_has_it`.
6. **A source that stops answering without closing the connection.** A frozen server or a
   dropped flow keeps the socket open: the pipeline must notice and reconnect, without mistaking
   an idle source for a dead one or a slow chunk read for a frozen server.
   → Task 22 `a_silent_stream_whose_source_doesn_t_answer_ends_the_session`,
   `silence_alone_is_an_idle_source`, `a_stalled_heartbeat_read_holds_up_nothing`,
   `a_source_that_stops_answering_is_reconnected_to`.

## Decisions and rulings

### Owner rulings (2026-10-07)

| # | Question | Ruling | Where |
|---|---|---|---|
| Q-M6b-1 | Does PAUSED close the source connection? | **Keep it warm.** `Command::Pause` finishes the open transaction, flushes, makes it durable, then reads nothing: no stream, no chunk, no rescan, and the snapshot timeout's clock stops. The connection stays open, and pgwire keeps sending standby status while the worker doesn't read. The WAL-lag guard keeps watching. `Command::Resume` reads on. | Task 5 |
| Q-M6b-2 | How does the controller learn the desired state? | **Poll `pipeline-state.json`** each resync tick (`lease_renewal_interval`). M9's API writes the same document. | Tasks 6, 10 |
| Q-M6b-3 | What does the controller do with a worker error? | **Classify** (`WorkerError::recovery()`). **Retry**: transient storage, source or sink errors, and `Contended`; the worker is rerun after a backoff. **Lease**: `Fenced` and `LeaseLost`; the worker stops and the controller goes back to claiming. **Error**: `Refused`, `TransactionTooLarge`, `WalLag`, permanent errors, `Accumulator` and `Crashed`; the pipeline is in ERROR (status and metric) until a stop and a resume, or a restart. ERROR isn't persisted. | Tasks 6, 10 |
| Q-M6b-4 | One plan or two? | **One plan, one PR.** | — |
| Q-M6b-5 | How do the E2E crash scenarios crash the process? | **A feature-gated abort.** Cargo feature `e2e-hooks`; `DATA_AVALANCHE_E2E_CRASH=<point>[:<n>]` aborts at the `n`th pass of a worker hook point. | Task 14 |
| Q-M6b-6 | A MERGE over `merge_memory_limit`? | **Halt the table.** The new key `sink.delta.merge_memory_limit` (V-119, ≥ 16 MiB when set). The DataFusion pool's refusal is `Permanent`, naming the key, so the writer halts the table `SINK_REJECTED`. Spec patch below. | Task 4 |
| Q-M6b-7 | Is `Sink::evolve` fenced? | **Yes.** It takes `owner_epoch`. A table a newer owner wrote answers `EvolveOutcome::Fenced` and changes nothing; the worker stops as after a fenced commit. | Task 1 |
| Q-M6b-8 | A Delta table re-created behind a live handle? | **Detect it at refresh.** The handle remembers the e-tag of the commit file at its version. Before an incremental update it HEADs that file: a different or missing file means another table, so the handle re-opens from scratch. Any `Transient` drops the handle (M4's ruling). Measured first: delta-rs merges the old file list with the new log, a commit lands on the wrong view, and the next read fails on a missing file. | Task 2 |

### Owner rulings on the planning findings (2026-10-08)

| # | Finding | Ruling | Where |
|---|---|---|---|
| Q-M6b-9 | F-1: the position guard rarely skips a replay | **Build the fix in M6b.** The guard compares the set's last change; the marker stays the frontier (Data-Flow's checkpoint rule). Gap DA-X-12 | Task 21 |
| Q-M6b-10 | F-2: Delta library log noise | **Hide it at the global `info` level.** A bare `info` (or quieter) caps the Delta libraries; the sink stops provoking the kernel's ERROR on an absent table | Task 23 |
| Q-M6b-11 | F-3: DA-SRC-16 | **In M6b.** A watchdog on the stream, decided by the heartbeat's read of the slot; the catalog connect bounded as a whole | Task 22 |
| Q-M6b-12 | F-4: a stop and a resume rerun an ERROR pipeline | **Accept and document** (option a): it behaves as a process restart until M9's restart verb. `Pipeline_Lifecycle_State_Machine.md` §3 names the transition | D-M6b.6 |
| Q-M6b-13 | F-5: catalogue names | **The catalogue is the exposed sample names.** The System-Overview patch, and the config patch (V-119), approved as drafted | Tasks 4, 7 |

### Decisions made while verifying (D-M6b)

- **D-M6b.1 Phase 0 writes are gated.** `Core::may_write` returns `LeaseLost` before each one:
  - opening the state, the slot intent, `prepare`, `ensure_table`;
  - halts, snapshot requests, abandoned snapshot documents and schema saves;
  - the final slot save, and `reset_position` after a lost position.

  A worker whose lease lapses mid-Phase-0 writes nothing more (Task 6).
- **D-M6b.2 Warnings once, capped.** `WorkerStatus::warn` keeps each text once: a repeat moves
  to the end. At most `MAX_WARNINGS = 50` are kept, and `warnings_dropped` counts the rest
  (M6a's note).
- **D-M6b.3 M6a's other notes need no change.**
  - "`finish()` doesn't acknowledge": the drain and dump paths acknowledge in the loop
    (`until_durable` → `writer_event`). `finish` only completes leftover jobs after a source
    failure, whose stream can't take an ack. Pinned by
    `a_drain_flushes_what_the_policy_held_back` (the last ack equals `wal_end`).
  - "`until_durable` doesn't poll control": a stop during a long sink outage is bounded by the
    controller's `drain_timeout`, after which the worker is aborted. Nothing past the durable
    checkpoint was acknowledged.
- **D-M6b.4 The renewer** (Clustering §19):
  - The deadline is `renewed + duration − duration × lease_safety_margin`, and each write is
    bounded by `timeout_at(deadline)`.
  - An unknown outcome (`Transient`, `Contention`) is observed before the next write. The lease
    is still this node's if and only if it names the same holder with the same epoch.
  - Failures are counted as `transient`, `conflict` or `lost`.
  - Degraded retries back off from `min(100 ms, interval/8)` to `interval/4`.
  - `LeaseGuard::may_commit` = not lost and now < deadline. It needs no renewer turn.
- **D-M6b.5 The claim** (Clustering §5.3):
  - A holder's lease is taken over only after a full `lease_duration` without a renewal, timed
    from when the read returned.
  - `release` treats a lease no longer this holder's (released by a write that landed, taken,
    deleted) as released.
- **D-M6b.6 The controller:**
  - It polls the desired state.
  - Stopped clears an ERROR: a stop and a resume rerun an ERROR pipeline, as a restart would
    (Q-M6b-12).
  - `Exit::Dumped` → `Actual::Dumped`, shown as `stopped`. The lease is kept, and a restart
    dumps again under `dump_always`.
  - Retry uses `Backoff` (1 s → 60 s).
  - Lease stops the renewer without a release (the lease is lost already).
  - `DontOwn` drains, releases and drops the pipeline's series.
  - A stop waits `drain_timeout` for the drain, then aborts the worker.
- **D-M6b.7 Phase 0 starts are capped and jittered** (Clustering §20.2):
  - At most `max_concurrent_takeovers` workers are in Phase 0 at once. The permit is held while
    the worker's phase is `Starting`.
  - Each start waits a random jitter below `start_jitter`, which is 500 ms in the binary.
- **D-M6b.8 The node's identity:** `memberlist.node_name`, else the hostname. The incarnation
  is the start time in unix milliseconds (Clustering §3.4).
- **D-M6b.9 The binary:**
  - The startup order is:
    1. read the file;
    2. the first load, with a throwaway key;
    3. logging;
    4. refuse what M6b doesn't serve;
    5. bind HTTP;
    6. the node's identity;
    7. the M2 join (anchor, check, adopt or bootstrap, reload with the anchor's key);
    8. the reconciler;
    9. ready.
  - `api.tls` is refused until M9.
  - An Iceberg pipeline gets a factory error, so it is in ERROR while the others run
    (scenario 9).
  - Exit codes: 2 for a config or command-line error, 1 for a startup error, 0 after SIGTERM or
    SIGINT. Every lease is released on the way out.
- **D-M6b.10 HTTP:**
  - `/health/live` answers once the listener is bound.
  - `/health/ready` answers once startup is done and the controllers run. It answers 503 again
    from the start of shutdown.
  - `/metrics` is the OpenMetrics text (`application/openmetrics-text; version=1.0.0`), which
    Prometheus scrapes.
  - Anything else is 404 until M9's `/v1`.
- **D-M6b.11 Metric names:**
  - Families are registered under the prefix `data_avalanche`.
  - Counters expose their samples with `_total` (OpenMetrics):
    - `source_reconnects_total`;
    - `lease_renew_failures_total{class}`;
    - `sink_merge_files_rewritten_total`;
    - `sink_merge_rows_copied_total`.
  - Enum gauges are one-hot: `pipeline_status{status}`, `lease_state{state}`.
  - Every pipeline series carries `{pipeline, source_host, source_db}`.
  - `halted_tables` and `reconcile_log_entries`/`reconcile_log_keys` are new, and the
    catalogue patch below adds them.
- **D-M6b.12 Logs:**
  - The binary installs the subscriber: `globals.log`, with the `--log-level` and
    `--log-format` overrides, written to stderr.
  - Roadmap §2 places "log setup" in `data-avalanche-telemetry`. It moves there with M10's
    tracing and OTLP; M6b's version reads the config's and the command line's types, which
    telemetry doesn't depend on.
- **D-M6b.13 The E2E harness:**
  - One schema, slot and publication per scenario (`e2e_<test>`), and one RustFS prefix per
    run.
  - It waits on observable state (the slot active, the slot's acknowledged LSN, a snapshot
    document completed, a metric line), never on a fixed sleep. The exception is "nothing
    happens": a 3 s wait before checking that a paused or stopped pipeline read nothing.
  - RustFS takes the standard `AWS_*` variables (`AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`), which
    both `object_store` 0.14 (state) and deltalake's 0.13 accept. Measured.

### Findings for the owner (answered 2026-10-08)

- **F-1 The position guard rarely skipped a replay after a crash** → Q-M6b-9, Task 21.
  - A table set's `position.end` is the flush's frontier, and keep-alives and other databases'
    WAL move it past the replayed transaction.
  - Measured (Task 17): marker `0/409EA098` against a replay ending at `0/409EA1B0`; marker
    `0/40C2CD78` against `0/40C2CDB0`, even with the replay flushed alone.
  - Correct, because the MERGE is idempotent, but the whole replay was re-merged.
  - Data-Flow's checkpoint rule sets each table's marker to the frontier, "including tables no
    transaction touched", so that an idle table can't pin the resume point. So the end stays the
    frontier, and the set carries its last change beside it for the guard.
- **F-2 Delta library log noise** → Q-M6b-10, Task 23. At `info` a restart run logged 25 lines,
  about 20 of them from `buoyant_kernel` and `deltalake_core`, plus the kernel's ERROR "No files
  in log segment" for every absent table a first start opens.
- **F-3 DA-SRC-16** → Q-M6b-11, Task 22. Measured: PostgreSQL 14 frozen for 150 s under a
  running pipeline; status `running`, no reconnect, nothing logged, and the catalog reconnect
  would have waited in the startup forever.
- **F-4 A stop and a resume rerun an ERROR pipeline** → Q-M6b-12.
- **F-5 Catalogue names** → Q-M6b-13.
- **F-6** Closed: `weekly.yml` ran green on `main` after M6a's merge (run 37740839634,
  2026-10-08, `f681150`, by manual dispatch). Its first run with M6b includes the E2E suite.

### Spec patches

Approved 2026-10-08 (Q-M6b-13) and applied:

`doc/specs/Configuration_Specification.md` (§9.4.3, §13.3):

```diff
@@ §9.4.3 `sink.delta` (iff `type = delta`)
 Settings apply when the sink **creates** a table; existing tables keep their properties.
+`merge_memory_limit` is the exception: it bounds every MERGE.
@@
+| `merge_memory_limit` | ByteSize \| null | no | `null` | PR | Memory ceiling of one table's MERGE (DataFusion's memory pool). `null` = no ceiling. A MERGE that needs more fails the same way on every replay, so the failure is permanent: the table halts `SINK_REJECTED`, its reason naming this key; raise it, or lower the `flush` bounds, then re-snapshot the table. ≥ 16 MiB when set (V-119). |
 | `table_properties` | map<string,string> | no | `{}` | PR (new tables) | …
@@ §13.3 Value domain
 | V-118 | `source.timeouts.snapshot` | ≥ 10s. | error | pipeline |
+| V-119 | `sink.delta.merge_memory_limit` | ≥ 16 MiB when set. | error | pipeline |
```

`doc/specs/System-Overview.md` (Observability catalogue):

```diff
+Names are the exposed sample names (OpenMetrics): a counter's name ends in `_total`.
+
 **Cardinality:** every metric carries …
-| Snapshot | `data_avalanche_snapshot_rows_done`, `data_avalanche_snapshot_state`, `data_avalanche_snapshot_chunk_duration_seconds` |
+| Snapshot | `data_avalanche_snapshot_rows_done`, `data_avalanche_snapshot_state`, `data_avalanche_snapshot_chunk_duration_seconds`, `data_avalanche_reconcile_log_entries`, `data_avalanche_reconcile_log_keys` (the snapshot reconciler's log, DA-SNAP-11) |
-| Sink | … `data_avalanche_sink_merge_files_rewritten`, `data_avalanche_sink_merge_rows_copied`, `data_avalanche_sink_soft_dropped_columns` |
+| Sink | … `data_avalanche_sink_merge_files_rewritten_total`, `data_avalanche_sink_merge_rows_copied_total`, `data_avalanche_sink_soft_dropped_columns` |
-| Status | `data_avalanche_pipeline_status` (running / paused / stopped / error) |
+| Status | `data_avalanche_pipeline_status` (running / paused / stopped / error), `data_avalanche_halted_tables` |
```

Proposed 2026-10-08 with Tasks 21–23, approved and applied the same day:

`doc/specs/Data-Flow.md` (Phase 4, position guard):

```diff
-string). If `committed_position.covers(flush.position_for_table)` (PG: `≥`; MySQL: GTID **subset**) the
-table is already committed and is skipped. A snapshot change set (a chunk's rows, flagged
+string). If `committed_position.covers(flush.last_change_for_table)` (PG: `≥`; MySQL: GTID **subset**)
+the table is already committed and is skipped. `last_change_for_table` is the commit position of the
+flush's last transaction that wrote the table, not the flush frontier: a replay's flush may read past
+the marker (a keep-alive, another table's transaction) while carrying only transactions the table has.
+A snapshot change set (a chunk's rows, flagged
```

`doc/specs/Configuration_Specification.md` (§6.4, §9.2, §9.2.1):

```diff
-| `level` | … | `trace`\|`debug`\|`info`\|`warn`\|`error`; per-module granularity allowed (`tracing` EnvFilter syntax, e.g. `info,rustcdc::sink=debug`). Hot-changeable. |
+| `level` | … | `trace`\|`debug`\|`info`\|`warn`\|`error`; per-module granularity allowed (`tracing` EnvFilter syntax, e.g. `info,rustcdc::sink=debug`). A bare level of `info` or quieter also caps the Delta libraries: `buoyant_kernel` and `deltalake_core` at `warn`, `deltalake_core::operations::merge` at `error`; a filter that names one of them is taken as written. Hot-changeable. |
-| `timeouts.connect` | Duration | no | `10s` | PR | |
+| `timeouts.connect` | Duration | no | `10s` | PR | The whole connection setup: TCP, TLS, startup and authentication. |
-| `heartbeat` | … | StandbyStatusUpdate interval; keeps the connection alive and prevents WAL-lag-as-inactivity. |
+| `heartbeat` | … | StandbyStatusUpdate interval; keeps the connection alive and prevents WAL-lag-as-inactivity. Also paces the stream watchdog: a stream silent for 3 heartbeats while the heartbeat's reads of the slot fail or find no reader is reconnected (DA-SRC-16). |
```

### Roadmap updates (applied 2026-10-08, with the plan's approval)

- §3.3: `PositionRange::last_change`, which the guard compares (`CommitMarker::covers`).
- §3.4:
  - `evolve(&self, table, delta, owner_epoch: u64)` and `EvolveOutcome::Fenced { current_epoch }`;
  - `fn stats(&self) -> SinkStats { files_rewritten, rows_copied }` (default zeros);
  - commit step 3b: a set keyed otherwise than the table's identifier is refused;
  - the `marker: None` contract.
- §3.6: `SourceConnector::retained` → `Option<RetainedLog { bytes, streamed }>`
  (was `retained_bytes`); the catalog connect bounded by `timeouts.connect`.
- §3.7: what M6b ships —
  - `LeaseTiming`, `LeaseRun`, `LeaseGuard` (`Held | Degraded | Lost`; "Renewing" is no
    state, since a renewal in flight leaves the state as it was);
  - `Claim`, `release`;
  - `PipelineController`, `WorkerFactory`, `ClusterReconciler::start`, `node_identity`.
- §3.10:
  - a MERGE over `merge_memory_limit` is `Permanent`;
  - a table is loaded only when its `_delta_log/` lists a log file;
  - the refresh's e-tag check and the re-open after any `Transient`;
  - `NoTable` for a table dropped since the refresh.
- §3.11:
  - `Command::{Pause, Resume}` and `Phase::Paused`;
  - `WorkerStatus { reconcile_log, warnings_dropped, reconnects }`;
  - `HookPoint::SchemasCached`;
  - `Recovery` and `WorkerError::recovery()`;
  - the stream watchdog (`SILENT_HEARTBEATS`), the heartbeat read beside the loop.
- A new §3.12 "Telemetry and the binary": `Metrics`, `PipelineMetrics`, `CONTENT_TYPE`;
  `run(RunArgs)`, `Fatal`, the exit codes, the HTTP endpoints, `DATA_AVALANCHE_E2E_CRASH`, the
  Delta libraries' log caps.
- §4 M6b: the exit as measured; D-IMPL-14 → this plan's Q-M6b-1…13 and D-M6b.1…13.
- §5.3 scenario 10: a frozen source reconnected to.

## File Structure

New crates (workspace members by `crates/*`; their `[workspace.dependencies]` entries are added by
the task that creates them):

```
crates/data-avalanche-telemetry/     Task 7
  src/lib.rs                         Metrics, PipelineMetrics, the series, OpenMetrics text
  tests/metrics.rs
crates/data-avalanche-cluster/       Tasks 8–11
  src/lib.rs
  src/lease.rs                       LeaseTiming, the renewer, LeaseGuard (CommitGuard), LeaseRun
  src/claim.rs                       Claim (acquire / takeover), release
  src/controller.rs                  PipelineController, WorkerFactory, Actual, ControllerDeps
                                     (each reconnect logged with its reason, Task 22)
  src/reconciler.rs                  ClusterReconciler (self-only ring), node_identity
  src/test_support.rs                ScriptedStore, test configs and factories (cfg(test))
crates/data-avalanche/               Tasks 12–20
  src/main.rs                        crypto provider, runtime, exit code
  src/lib.rs, cli.rs, logging.rs     the command line, globals.log (the Delta libraries' caps, Task 23)
  src/run.rs                         the startup sequence, signals, shutdown
  src/startup.rs                     buckets, state locations, the M2 join
  src/wiring.rs                      Factory: PgConnector + DeltaSink per pipeline
  src/http.rs                        /health/live, /health/ready, /metrics (Task 13)
  src/crash.rs                       DATA_AVALANCHE_E2E_CRASH (feature e2e-hooks, Task 14)
  tests/cli.rs                       exit codes of the built binary
  tests/e2e/main.rs, harness.rs      the E2E world: schema, slot, prefix, config, process
  tests/e2e/smoke.rs, snapshots.rs, streaming.rs, toast.rs, crashes.rs, schema.rs, types.rs,
  lifecycle.rs, guards.rs            scenarios 1–10 (Tasks 14–20; Tasks 21–23 extend crashes.rs,
                                     guards.rs and smoke.rs)
  tests/e2e/duckdb_read.py           DuckDB delta_scan on RustFS (Task 19)
```

Changed crates:

| Crate | Files | Tasks |
|---|---|---|
| `data-avalanche-core` | `src/sink.rs` (evolve's epoch, `Fenced`, the identifier step, `SinkStats`, the marker doc, the guard's last change), `src/change_set.rs` (`PositionRange::last_change`), `src/source.rs` (`RetainedLog`), `src/lib.rs` | 1, 2, 3, 7, 21, 22 |
| `data-avalanche-sink-delta` | `src/sink.rs`, `src/evolve.rs`, `src/table.rs` (the refresh check, `open`), `src/error.rs`, `src/config.rs`, `src/test_support.rs`, `tests/sink/{common,local,scenarios,stores}.rs` | 1, 2, 3, 4, 7, 21, 23 |
| `data-avalanche-source-pg` | `src/connect.rs` (the bounded connect), `src/slot.rs` + `src/connector.rs` (`retained`), `tests/pg/slot.rs` | 22 |
| `data-avalanche-config` | `src/{raw,merge,build,resolved,registry}.rs`, `src/rules/domain.rs`, `tests/load.rs`, `tests/rules/domain.rs` | 4 |
| `data-avalanche-pipeline` | `src/{session,session/snapshot,status,worker,error,hooks,lib,schema_change,test_support}.rs`, `src/accumulator.rs` + `src/buffer.rs` (the last change), `src/watchdog.rs` (new), `tests/pg/{main,common,pause}.rs`, `Cargo.toml` (tokio `test-util`, dev) | 1, 3, 5, 6, 21, 22 |
| `data-avalanche-state` | `src/pipeline.rs` (`PipelineDocs::state`) | 6 |
| `data-avalanche-testkit` | `src/memory_sink.rs` (the evolve fence, the identifier step), `src/fake_source.rs` (`retained`, `detach_stream`, `stall_retained`) | 1, 3, 21, 22 |
| CI | `.github/workflows/ci.yml` (Task 14), `weekly.yml` (Task 24) | 14, 24 |

---


### Task 1: Core and the Delta sink: a fenced evolve

**Files:**
- Modify: `crates/data-avalanche-core/src/sink.rs`, `crates/data-avalanche-pipeline/src/schema_change.rs`, `crates/data-avalanche-pipeline/src/session.rs`, `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-sink-delta/src/evolve.rs`, `crates/data-avalanche-sink-delta/src/sink.rs`, `crates/data-avalanche-sink-delta/tests/sink/local.rs`, `crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`, `crates/data-avalanche-testkit/src/memory_sink.rs`

**Interfaces:**
- Consumes: `Sink` (§3.4), `CommitMarker::fences`, the Delta fence (`txn` version for
  `app_id`, M4), `MemorySink` (testkit).
- Produces:
  - `Sink::evolve(&self, table: &TableRef, delta: &SchemaDelta, owner_epoch: u64) ->
    Result<EvolveOutcome, SinkError>`;
  - `EvolveOutcome::Fenced { current_epoch: u64 }`;
  - the worker passes its epoch, and maps `Fenced` to `WorkerError::Fenced` (the worker ends,
    as after a fenced commit).

Q-M6b-7: a zombie's ADD COLUMN must not alter a table its successor writes. The Delta sink
checks the `txn` fence before the metadata commit, and re-checks after a lost race (the commit
retries are M4's). `MemorySink` mirrors it, so the worker's test runs without Delta.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -1062,6 +1062,42 @@ mod tests {
         assert_eq!(w.ids("a"), [1]);
     }
 
+    /// A newer owner wrote the table: the evolve of a column the stream added is fenced, the
+    /// worker stops, and nothing of the transaction lands.
+    #[tokio::test]
+    async fn a_newer_owner_stops_the_worker_at_an_evolve() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let end = run_with(&mode("mode: never"), deps(&w), async |h| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            streamed(&h.status, &w).await;
+            w.sink.take_over(&t("a"), 9);
+            w.source.alter(&t("a"), noted());
+            let mut r = row(2, "x");
+            r.0.push(("note".into(), ColumnValue::Text("n".into())));
+            w.source.commit(FakeTxn::new().insert(&t("a"), r));
+            // The status channel closes when the worker ends.
+            let mut st = h.status.clone();
+            let _ =
+                tokio::time::timeout(std::time::Duration::from_secs(10), st.wait_for(|_| false))
+                    .await;
+        })
+        .await;
+        assert!(
+            matches!(
+                end,
+                Err(WorkerError::Fenced {
+                    current_epoch: 9,
+                    ..
+                })
+            ),
+            "{end:?}"
+        );
+        assert_eq!(w.ids("a"), [1]);
+        let content = w.sink.content(&t("a")).unwrap();
+        assert!(content.column_by_name("note").is_none(), "nothing evolved");
+    }
+
     #[tokio::test]
     async fn a_lost_lease_stops_the_worker_before_its_next_commit() {
         let w = World::new(&["a"]);
```

Change `crates/data-avalanche-sink-delta/src/evolve.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/evolve.rs b/crates/data-avalanche-sink-delta/src/evolve.rs
--- a/crates/data-avalanche-sink-delta/src/evolve.rs
+++ b/crates/data-avalanche-sink-delta/src/evolve.rs
@@ -134,9 +134,14 @@ mod tests {
     }
 
     async fn apply(h: &mut TableHandle, changes: Vec<SchemaChange>) -> EvolveOutcome {
-        let (table, outcome) = evolve(h.table().clone(), &delta(changes), NtzAs::default())
-            .await
-            .unwrap();
+        let (table, outcome) = evolve(
+            h.table().clone(),
+            &delta(changes),
+            NtzAs::default(),
+            CommitProperties::default(),
+        )
+        .await
+        .unwrap();
         h.set_table(table);
         outcome
     }
```

Change `crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -412,8 +412,8 @@ mod tests {
     use data_avalanche_core::position::Lsn;
 
     use super::*;
-    use crate::TableDefaults;
     use crate::test_support::{change_set, local, marker, orders, spec, upserts};
+    use crate::{NtzAs, TableDefaults};
 
     fn sink(dir: &tempfile::TempDir) -> DeltaSink {
         DeltaSink::new(DeltaSinkConfig {
@@ -466,6 +466,79 @@ mod tests {
         assert_eq!(zombie.stats().fenced, 1);
     }
 
+    fn add_note() -> SchemaDelta {
+        SchemaDelta {
+            table: orders().table().clone(),
+            changes: vec![data_avalanche_core::SchemaChange::AddColumn(
+                data_avalanche_core::ColumnSchema::new(
+                    "note",
+                    data_avalanche_core::ColumnValueType::Text,
+                    "text",
+                ),
+            )],
+        }
+    }
+
+    async fn columns(s: &DeltaSink) -> usize {
+        let state = s.table_state(orders().table()).await.unwrap();
+        state.schema.unwrap().fields().len()
+    }
+
+    #[tokio::test]
+    async fn an_evolve_of_a_table_a_newer_owner_wrote_is_fenced() {
+        let dir = tempfile::tempdir().unwrap();
+        let (zombie, owner) = (sink(&dir), sink(&dir));
+        zombie.ensure_table(&spec(&orders())).await.unwrap();
+        let first = rows(10, &[1]);
+        zombie
+            .commit(first.clone(), marker(5, &first))
+            .await
+            .unwrap();
+        let theirs = rows(20, &[2]);
+        owner
+            .commit(theirs.clone(), marker(6, &theirs))
+            .await
+            .unwrap();
+        let before = columns(&zombie).await;
+        let out = zombie.evolve(orders().table(), &add_note(), 5).await;
+        assert_eq!(out.unwrap(), EvolveOutcome::Fenced { current_epoch: 6 });
+        assert_eq!(columns(&zombie).await, before, "nothing added");
+        let out = owner.evolve(orders().table(), &add_note(), 6).await;
+        assert_eq!(out.unwrap(), EvolveOutcome::Applied);
+        assert_eq!(columns(&owner).await, before + 1);
+    }
+
+    #[tokio::test]
+    async fn an_evolve_that_races_a_newer_owners_commit_fails_and_its_retry_is_fenced() {
+        let dir = tempfile::tempdir().unwrap();
+        let (zombie, owner) = (sink(&dir), sink(&dir));
+        zombie.ensure_table(&spec(&orders())).await.unwrap();
+        let first = rows(10, &[1]);
+        zombie
+            .commit(first.clone(), marker(5, &first))
+            .await
+            .unwrap();
+        // The zombie read the table (and passed the fence) before the new owner's commit.
+        let stale = {
+            let mut guard = zombie.lock(orders().table()).await.unwrap();
+            guard.as_mut().unwrap().table().clone()
+        };
+        let theirs = rows(20, &[2]);
+        owner
+            .commit(theirs.clone(), marker(6, &theirs))
+            .await
+            .unwrap();
+        let props = zombie.props(&marker(5, &first), 5, None);
+        let err = evolve(stale, &add_note(), NtzAs::default(), props)
+            .await
+            .map(|(_, o)| o)
+            .unwrap_err();
+        assert!(err.is_transient(), "{err}");
+        assert!(err.to_string().contains("Concurrent transaction"), "{err}");
+        let out = zombie.evolve(orders().table(), &add_note(), 5).await;
+        assert_eq!(out.unwrap(), EvolveOutcome::Fenced { current_epoch: 6 });
+    }
+
     #[tokio::test]
     async fn a_race_with_a_foreign_optimize_is_retried_and_lands() {
         let dir = tempfile::tempdir().unwrap();
```

Change `crates/data-avalanche-sink-delta/tests/sink/local.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/local.rs b/crates/data-avalanche-sink-delta/tests/sink/local.rs
--- a/crates/data-avalanche-sink-delta/tests/sink/local.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/local.rs
@@ -84,7 +84,7 @@ async fn delta_rs_still_refuses_add_column_under_column_mapping() {
             "integer",
         ))],
     };
-    let err = s.evolve(&items(), &add).await.unwrap_err();
+    let err = s.evolve(&items(), &add, 1).await.unwrap_err();
     assert!(err.to_string().contains("Column mapping"), "{err}");
 }
 
```

Change `crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs b/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
--- a/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
@@ -176,7 +176,7 @@ pub async fn evolution(location: DeltaLocation) {
         ))],
     };
     assert_eq!(
-        s.evolve(&items(), &add).await.unwrap(),
+        s.evolve(&items(), &add, 1).await.unwrap(),
         EvolveOutcome::Applied
     );
     commit(
@@ -196,7 +196,7 @@ pub async fn evolution(location: DeltaLocation) {
         changes: vec![SchemaChange::DropColumn("doc".into())],
     };
     assert!(matches!(
-        s.evolve(&items(), &drop).await.unwrap(),
+        s.evolve(&items(), &drop, 1).await.unwrap(),
         EvolveOutcome::Degraded { .. }
     ));
     let mut narrow = cs(30, vec![], vec![]);
```

Change `crates/data-avalanche-testkit/src/memory_sink.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -351,12 +351,18 @@ impl Sink for MemorySink {
         &self,
         table: &TableRef,
         delta: &SchemaDelta,
+        owner_epoch: u64,
     ) -> Result<EvolveOutcome, SinkError> {
         self.fault(SinkOp::Evolve, table)?;
         let mut tables = self.tables();
         let t = tables.get_mut(table).ok_or_else(|| SinkError::NoTable {
             table: table.clone(),
         })?;
+        if let Some(stored) = t.marker.as_ref().filter(|m| m.owner_epoch > owner_epoch) {
+            return Ok(EvolveOutcome::Fenced {
+                current_epoch: stored.owner_epoch,
+            });
+        }
         let mut added = Vec::new();
         let mut warnings = Vec::new();
         for change in &delta.changes {
@@ -781,9 +787,12 @@ mod tests {
             V::Int32,
             "integer",
         ))]);
-        assert_eq!(sink.evolve(&t, &add).await.unwrap(), EvolveOutcome::Applied);
         assert_eq!(
-            sink.evolve(&t, &add).await.unwrap(),
+            sink.evolve(&t, &add, 1).await.unwrap(),
+            EvolveOutcome::Applied
+        );
+        assert_eq!(
+            sink.evolve(&t, &add, 1).await.unwrap(),
             EvolveOutcome::Applied,
             "replay"
         );
@@ -796,7 +805,7 @@ mod tests {
 
         let drop = delta(vec![SchemaChange::DropColumn("doc".into())]);
         assert!(matches!(
-            sink.evolve(&t, &drop).await.unwrap(),
+            sink.evolve(&t, &drop, 1).await.unwrap(),
             EvolveOutcome::Degraded { .. }
         ));
         let mut narrower = cs(20, vec![], vec![]);
@@ -821,11 +830,41 @@ mod tests {
             narrowed: false,
         }]);
         assert!(matches!(
-            sink.evolve(&t, &widen).await.unwrap(),
+            sink.evolve(&t, &widen, 1).await.unwrap(),
             EvolveOutcome::Unsupported { .. }
         ));
     }
 
+    #[tokio::test]
+    async fn an_evolve_of_a_table_a_newer_owner_wrote_is_fenced() {
+        let sink = sink().await;
+        let t = TableRef::new("public", "t");
+        commit(
+            &sink,
+            cs(10, vec![upserts(&[(1, Some("a"))], None)], vec![]),
+            2,
+        )
+        .await;
+        let add = SchemaDelta {
+            table: t.clone(),
+            changes: vec![SchemaChange::AddColumn(ColumnSchema::new(
+                "n",
+                V::Int32,
+                "integer",
+            ))],
+        };
+        assert_eq!(
+            sink.evolve(&t, &add, 1).await.unwrap(),
+            EvolveOutcome::Fenced { current_epoch: 2 }
+        );
+        let state = sink.table_state(&t).await.unwrap();
+        assert_eq!(state.schema.unwrap().fields().len(), 2, "nothing added");
+        assert_eq!(
+            sink.evolve(&t, &add, 2).await.unwrap(),
+            EvolveOutcome::Applied
+        );
+    }
+
     #[tokio::test]
     async fn contract_violations_are_errors() {
         let sink = sink().await;
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-testkit -p data-avalanche-pipeline -p data-avalanche-sink-delta -- evolve
```

Expected:

FAIL to compile (2 errors); the first:

```text
error[E0050]: method `evolve` has 4 parameters but the declaration in trait `evolve` has 3
error[E0599]: no variant named `Fenced` found for enum `EvolveOutcome`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -220,6 +220,11 @@ pub enum EvolveOutcome {
     Unsupported {
         reason: String,
     },
+    /// A newer owner has written the table: nothing changed, and this worker must stop the
+    /// pipeline, as after a fenced commit (Clustering §19).
+    Fenced {
+        current_epoch: u64,
+    },
 }
 
 #[derive(Debug, Clone, PartialEq, Eq)]
@@ -274,10 +279,14 @@ pub trait Sink: Send + Sync {
     async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError>;
     /// Creates the table when it's absent; a present table is left as it is.
     async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError>;
+    /// Applies a schema delta, idempotently. `owner_epoch` is the writer's lease epoch: a table a
+    /// newer owner has written is [`EvolveOutcome::Fenced`] and changes nothing, so a zombie
+    /// can't alter a table its successor writes (the fence of [`Sink::commit`]).
     async fn evolve(
         &self,
         table: &TableRef,
         delta: &SchemaDelta,
+        owner_epoch: u64,
     ) -> Result<EvolveOutcome, SinkError>;
     /// Commits one change set, in this order:
     /// 1. [`TableChangeSet::validate`] ([`SinkError::InvalidChangeSet`]);
```

Change `crates/data-avalanche-pipeline/src/schema_change.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/schema_change.rs b/crates/data-avalanche-pipeline/src/schema_change.rs
--- a/crates/data-avalanche-pipeline/src/schema_change.rs
+++ b/crates/data-avalanche-pipeline/src/schema_change.rs
@@ -21,6 +21,8 @@ pub(crate) struct Evolver<'a> {
     pub source: &'a dyn SourceConnector,
     pub sink: &'a dyn Sink,
     pub guard: &'a dyn CommitGuard,
+    /// The lease epoch the sink's evolve is fenced with.
+    pub owner_epoch: u64,
     pub tuning: &'a Tuning,
     pub evolution: bool,
     pub status: &'a watch::Sender<WorkerStatus>,
@@ -85,7 +87,7 @@ impl Evolver<'_> {
             if !self.guard.may_commit() {
                 return Err(WorkerError::LeaseLost);
             }
-            let outcome = self.sink.evolve(table, &delta).await;
+            let outcome = self.sink.evolve(table, &delta, self.owner_epoch).await;
             if attempt > 0 && outcome.is_ok() {
                 self.status.send_modify(|st| st.degraded = None);
             }
@@ -97,6 +99,12 @@ impl Evolver<'_> {
                 Ok(EvolveOutcome::Unsupported { reason }) => {
                     return Ok(Err(HaltReason::EvolutionUnsupported { detail: reason }));
                 }
+                Ok(EvolveOutcome::Fenced { current_epoch }) => {
+                    return Err(WorkerError::Fenced {
+                        table: table.clone(),
+                        current_epoch,
+                    });
+                }
                 Err(SinkError::Transient { reason }) => {
                     self.status
                         .send_modify(|st| st.degraded = Some(format!("table {table}: {reason}")));
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -90,6 +90,7 @@ impl Core {
             source: &*self.source,
             sink: &*self.sink,
             guard: &*self.guard,
+            owner_epoch: self.owner_epoch,
             tuning: &self.tuning,
             evolution: self.settings.evolution,
             status: &self.status,
```

Change `crates/data-avalanche-sink-delta/src/evolve.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/evolve.rs b/crates/data-avalanche-sink-delta/src/evolve.rs
--- a/crates/data-avalanche-sink-delta/src/evolve.rs
+++ b/crates/data-avalanche-sink-delta/src/evolve.rs
@@ -14,17 +14,19 @@ use data_avalanche_core::{
 };
 use deltalake::DeltaTable;
 use deltalake::kernel::StructField;
+use deltalake::kernel::transaction::CommitProperties;
 
 use crate::NtzAs;
 use crate::error::{permanent, sink_error};
 use crate::types::{delta_field, hop2_field};
 
-/// Applies `delta` to `table`. The table is returned as it is after the change (unchanged when
-/// nothing was added).
+/// Applies `delta` to `table`, committing with `props` (the caller's fence). The table is
+/// returned as it is after the change (unchanged when nothing was added).
 pub async fn evolve(
     table: DeltaTable,
     delta: &SchemaDelta,
     ntz: NtzAs,
+    props: CommitProperties,
 ) -> Result<(DeltaTable, EvolveOutcome), SinkError> {
     let schema = table.snapshot().map_err(sink_error)?.schema();
     let mut added: Vec<StructField> = Vec::new();
@@ -96,6 +98,7 @@ pub async fn evolve(
         table
             .add_columns()
             .with_fields(added)
+            .with_commit_properties(props)
             .await
             .map_err(sink_error)?
     };
```

Change `crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -337,6 +337,7 @@ impl Sink for DeltaSink {
         &self,
         table: &TableRef,
         delta: &SchemaDelta,
+        owner_epoch: u64,
     ) -> Result<EvolveOutcome, SinkError> {
         let mut guard = self.lock(table).await?;
         let h = guard.as_mut().expect("opened by lock");
@@ -345,8 +346,27 @@ impl Sink for DeltaSink {
                 table: table.clone(),
             });
         }
-        let (t, outcome) =
-            evolve(h.table().clone(), delta, self.config.table.timestamp_ntz_as).await?;
+        let pipeline = &self.config.pipeline_id;
+        if let Some(current) = h.fence_epoch(pipeline).await?.filter(|e| *e > owner_epoch) {
+            return Ok(EvolveOutcome::Fenced {
+                current_epoch: current,
+            });
+        }
+        // The ADD COLUMN carries the fence's `txn` (no position): a commit of a newer owner that
+        // lands after the check above conflicts with it (`ConcurrentTransaction`), and the retry
+        // finds the newer epoch.
+        let txn_version = i64::try_from(owner_epoch)
+            .map_err(|_| permanent(format!("owner epoch {owner_epoch}")))?;
+        let props = CommitProperties::default()
+            .with_application_transaction(Transaction::new(app_id(pipeline), txn_version))
+            .with_metadata(commit_metadata(pipeline, owner_epoch, None));
+        let (t, outcome) = evolve(
+            h.table().clone(),
+            delta,
+            self.config.table.timestamp_ntz_as,
+            props,
+        )
+        .await?;
         h.set_table(t);
         Ok(outcome)
     }
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-testkit -p data-avalanche-pipeline -p data-avalanche-sink-delta -- evolve
```

Expected:

PASS:

```text
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 168 filtered out; finished in 0.07s
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 59 filtered out; finished in 0.31s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 19 filtered out; finished in 0.00s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(core): a sink's evolve is fenced by the owner epoch (Q-M6b-7)"
```


### Task 2: The Delta sink: a stale handle re-opens, a dropped table is missing

**Files:**
- Modify: `crates/data-avalanche-core/src/sink.rs`, `crates/data-avalanche-sink-delta/src/sink.rs`, `crates/data-avalanche-sink-delta/src/table.rs`, `crates/data-avalanche-sink-delta/src/test_support.rs`, `crates/data-avalanche-sink-delta/tests/sink/local.rs`, `crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`, `crates/data-avalanche-sink-delta/tests/sink/stores.rs`

**Interfaces:**
- Consumes: the Delta table handle (`table.rs`, M4), `ObjectStore::head`.
- Produces:
  - At refresh, a handle whose commit file at its version changed (e-tag) or vanished is
    re-opened from scratch.
  - Any `Transient` from a table operation drops the cached handle.
  - A table dropped since the refresh is `SinkError::NoTable`, not `Permanent`.
  - The `SinkTableState::marker` doc: `None` says only that no positioned own commit is in
    reach.

Q-M6b-8 and three M4-carried items. Measured before the fix: after a table is dropped and
re-created past the handle's version, delta-rs's incremental update merges the old file list
with the new log. The next commit lands on that wrong view, and a read fails on a missing file.
The re-created table (`a_live_handle_recovers_from_a_table_recreated_past_its_version`) and the
replaced log (`a_refresh_reopens_a_table_whose_log_was_replaced`) run on the local directory,
RustFS and Azurite.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -560,6 +560,33 @@ mod tests {
         assert_eq!(out.unwrap(), EvolveOutcome::Fenced { current_epoch: 6 });
     }
 
+    /// A call that fails `Transient` drops the table's handle; the next call opens the table
+    /// afresh and lands.
+    #[tokio::test]
+    async fn a_transient_failure_drops_the_handle() {
+        let dir = tempfile::tempdir().unwrap();
+        let (s, other) = (sink(&dir), sink(&dir));
+        s.ensure_table(&spec(&orders())).await.unwrap();
+        let first = rows(10, &[1]);
+        s.commit(first.clone(), marker(1, &first)).await.unwrap();
+        let theirs = rows(20, &[2]);
+        other
+            .commit(theirs.clone(), marker(1, &theirs))
+            .await
+            .unwrap();
+        // Without re-reading, every attempt loses its race: `Transient`.
+        let late = rows(30, &[3]);
+        let err = s
+            .attempts(&late, &marker(1, &late), false)
+            .await
+            .unwrap_err();
+        assert!(err.is_transient(), "{err}");
+        let slot = s.tables.lock().unwrap().get(&late.table).cloned().unwrap();
+        assert!(slot.lock().await.is_none(), "the handle is dropped");
+        let out = s.commit(late.clone(), marker(1, &late)).await.unwrap();
+        assert!(matches!(out, CommitOutcome::Committed { .. }), "{out:?}");
+    }
+
     #[tokio::test]
     async fn a_race_with_a_foreign_optimize_is_retried_and_lands() {
         let dir = tempfile::tempdir().unwrap();
```

Change `crates/data-avalanche-sink-delta/src/table.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/table.rs b/crates/data-avalanche-sink-delta/src/table.rs
--- a/crates/data-avalanche-sink-delta/src/table.rs
+++ b/crates/data-avalanche-sink-delta/src/table.rs
@@ -564,6 +564,40 @@ mod tests {
         assert!(found.contains("Iceberg"), "{found}");
     }
 
+    /// The table is dropped after the handle's last refresh: the fence's read fails, the re-open
+    /// finds nothing, and the table is missing (`NoTable`), not a `Permanent` delta-rs error.
+    #[tokio::test]
+    async fn a_table_dropped_since_the_refresh_is_no_table() {
+        let dir = tempfile::tempdir().unwrap();
+        let mut h = created(&dir, &TableDefaults::default()).await;
+        own_append(&mut h, &[1], 4, 0x10).await;
+        std::fs::remove_dir_all(dir.path().join("public/orders")).unwrap();
+        let err = h.fence_epoch("p").await.unwrap_err();
+        assert!(
+            matches!(err, SinkError::NoTable { ref table } if table == &t()),
+            "{err}"
+        );
+    }
+
+    /// The handle notices a log that isn't the one it loaded, by the commit file it recorded.
+    #[tokio::test]
+    async fn a_refresh_reopens_a_table_whose_log_was_replaced() {
+        let dir = tempfile::tempdir().unwrap();
+        let mut h = created(&dir, &TableDefaults::default()).await;
+        own_append(&mut h, &[1], 4, 0x10).await;
+        assert!(h.refresh().await.unwrap());
+        assert!(h.same_log().await.unwrap());
+        std::fs::remove_dir_all(dir.path().join("public/orders")).unwrap();
+        let mut other = created(&dir, &TableDefaults::default()).await;
+        for id in 2..=4 {
+            own_append(&mut other, &[id], 1, 0x100).await;
+        }
+        assert!(!h.same_log().await.unwrap());
+        assert!(h.refresh().await.unwrap());
+        assert_eq!(h.version(), other.version());
+        assert!(h.same_log().await.unwrap());
+    }
+
     #[tokio::test]
     async fn the_marker_is_the_txn_epoch_and_the_last_own_position() {
         let dir = tempfile::tempdir().unwrap();
```

Change `crates/data-avalanche-sink-delta/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/test_support.rs b/crates/data-avalanche-sink-delta/src/test_support.rs
--- a/crates/data-avalanche-sink-delta/src/test_support.rs
+++ b/crates/data-avalanche-sink-delta/src/test_support.rs
@@ -44,9 +44,13 @@ pub(crate) fn local(dir: &tempfile::TempDir) -> DeltaLocation {
 }
 
 pub(crate) async fn handle(dir: &tempfile::TempDir, table: &TableRef) -> TableHandle {
-    TableHandle::open(local(dir).table_url(table).unwrap(), HashMap::new())
-        .await
-        .unwrap()
+    TableHandle::open(
+        table.clone(),
+        local(dir).table_url(table).unwrap(),
+        HashMap::new(),
+    )
+    .await
+    .unwrap()
 }
 
 /// One `orders` row: `ref` is 16 bytes of `id`, `at` is `id` ms, `doc` as given.
```

Change `crates/data-avalanche-sink-delta/tests/sink/local.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/local.rs b/crates/data-avalanche-sink-delta/tests/sink/local.rs
--- a/crates/data-avalanche-sink-delta/tests/sink/local.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/local.rs
@@ -285,3 +285,9 @@ async fn a_crash_after_the_truncate_delete_replays_both() {
         CommitOutcome::SkippedCovered
     );
 }
+
+#[tokio::test]
+async fn a_live_handle_recovers_from_a_table_recreated_past_its_version() {
+    let (_dir, location) = local();
+    scenarios::recreated_past_a_live_handle(location).await;
+}
```

Change `crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs b/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
--- a/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
@@ -6,6 +6,8 @@ use data_avalanche_core::{
 };
 use data_avalanche_sink_delta::{DeltaLocation, TableDefaults};
 use deltalake::DeltaTable;
+use deltalake::logstore::object_store::{ObjectStore, ObjectStoreExt};
+use futures_util::TryStreamExt;
 
 use crate::common::{commit, cs, deletes, items, lsn, owned, rows, schema, sink, spec, upserts};
 
@@ -269,3 +271,67 @@ pub async fn escaped_names(location: DeltaLocation, odd: TableRef) -> TableRef {
     assert_eq!(state.marker.unwrap().committed_position, lsn(10));
     odd
 }
+
+/// Removes every object of the table, as an operator dropping it does.
+async fn drop_table(location: &DeltaLocation) {
+    let store = foreign(location).await.log_store().object_store(None);
+    let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
+    for o in objects {
+        store.delete(&o.location).await.unwrap();
+    }
+}
+
+/// Another writer drops the table, re-creates it and commits past the live handle's version.
+/// Brought forward incrementally, the handle would hold the old table's files and miss the new
+/// one's first commits (Q-M6b-8). The sink sees the log is another log and re-opens the table, so
+/// an upsert of a key in a file the old view lacks updates it rather than adding it twice.
+pub async fn recreated_past_a_live_handle(location: DeltaLocation) {
+    let s = sink(location.clone(), TableDefaults::default());
+    s.ensure_table(&spec()).await.unwrap();
+    for (end, id) in [(10, 1), (20, 2), (30, 3)] {
+        commit(
+            &s,
+            cs(end, vec![upserts(&[(id, Some("a"))], None)], vec![]),
+            1,
+        )
+        .await;
+    }
+    drop_table(&location).await;
+    let other = sink(location, TableDefaults::default());
+    other.ensure_table(&spec()).await.unwrap();
+    for id in 1..=6 {
+        commit(
+            &other,
+            cs(
+                id as u64 * 10,
+                vec![upserts(&[(id, Some("o"))], None)],
+                vec![],
+            ),
+            1,
+        )
+        .await;
+    }
+    let out = commit(
+        &s,
+        cs(
+            1000,
+            vec![upserts(&[(2, Some("re")), (9, Some("re"))], None)],
+            vec![],
+        ),
+        1,
+    )
+    .await;
+    assert!(matches!(out, CommitOutcome::Committed { .. }), "{out:?}");
+    assert_eq!(
+        rows(&s).await,
+        owned(&[
+            (1, Some("o")),
+            (2, Some("re")),
+            (3, Some("o")),
+            (4, Some("o")),
+            (5, Some("o")),
+            (6, Some("o")),
+            (9, Some("re")),
+        ])
+    );
+}
```

Change `crates/data-avalanche-sink-delta/tests/sink/stores.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/stores.rs b/crates/data-avalanche-sink-delta/tests/sink/stores.rs
--- a/crates/data-avalanche-sink-delta/tests/sink/stores.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/stores.rs
@@ -28,6 +28,7 @@ async fn run_all(location: impl Fn(String) -> DeltaLocation, prefix: &str) {
     scenarios::fence_and_guard_survive_a_foreign_optimize(at("fence")).await;
     scenarios::truncate_then_rows(at("truncate")).await;
     scenarios::evolution(at("evolution")).await;
+    scenarios::recreated_past_a_live_handle(at("recreated")).await;
     scenarios::masks_and_empty_sets(at("masks")).await;
     scenarios::escaped_names(at("escaped"), scenarios::odd_names()).await;
 }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-sink-delta
```

Expected:

FAIL to compile (4 errors); the first:

```text
error[E0061]: this function takes 2 arguments but 3 arguments were supplied
error[E0599]: no method named `same_log` found for struct `table::TableHandle` in the current scope
… and 2 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -205,7 +205,11 @@ pub struct SinkTableState {
     /// lineage columns), plus every column `evolve` added. Soft-dropped columns stay. Not the
     /// sink's physical (Hop 3) types.
     pub schema: Option<SchemaRef>,
-    /// The marker of the latest commit this pipeline made to the table.
+    /// The marker of the latest positioned commit this pipeline made to the table. `None` says
+    /// only that no such commit is found, not that the pipeline never wrote the table: a
+    /// truncate's DELETE carries the fence but no position, so a crash before its MERGE leaves
+    /// an own commit without one, and a walk-back stops at a cleaned-up log. Nothing decides
+    /// "the pipeline never wrote here" from it.
     pub marker: Option<CommitMarker>,
 }
 
```

Change `crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -147,20 +147,36 @@ impl DeltaSink {
         let mut guard = slot.lock_owned().await;
         if guard.is_none() {
             let url = self.config.location.table_url(table)?;
-            *guard = Some(TableHandle::open(url, self.options.clone()).await?);
+            *guard = Some(TableHandle::open(table.clone(), url, self.options.clone()).await?);
         }
         Ok(guard)
     }
 
+    /// Runs `f` on the table's handle. A `Transient` failure drops the handle, so the next call
+    /// opens the table afresh: a handle delta-rs can't bring forward recovers without a restart.
+    async fn with_table<T>(
+        &self,
+        table: &TableRef,
+        f: impl AsyncFnOnce(&mut TableHandle) -> Result<T, SinkError>,
+    ) -> Result<T, SinkError> {
+        let mut guard = self.lock(table).await?;
+        let out = f(guard.as_mut().expect("opened by lock")).await;
+        if matches!(out, Err(SinkError::Transient { .. })) {
+            *guard = None;
+        }
+        out
+    }
+
     /// The table's rows in Hop 2 terms and identifier order (every column, lineage included);
     /// `None` when the table doesn't exist. The Delta counterpart of `MemorySink::content`.
     pub async fn content(&self, table: &TableRef) -> Result<Option<RecordBatch>, SinkError> {
-        let mut guard = self.lock(table).await?;
-        let h = guard.as_mut().expect("opened by lock");
-        if !h.refresh().await? {
-            return Ok(None);
-        }
-        h.content().await.map(Some)
+        self.with_table(table, async |h| {
+            if !h.refresh().await? {
+                return Ok(None);
+            }
+            h.content().await.map(Some)
+        })
+        .await
     }
 
     /// One write's commit properties: the fence's `txn` (the owner epoch as the `i64` version
@@ -293,44 +309,46 @@ impl Sink for DeltaSink {
     }
 
     async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError> {
-        let mut guard = self.lock(table).await?;
-        let h = guard.as_mut().expect("opened by lock");
-        if !h.refresh().await? {
-            if let Some(reason) = h.foreign_format().await? {
-                return Err(permanent(reason));
+        self.with_table(table, async |h| {
+            if !h.refresh().await? {
+                if let Some(reason) = h.foreign_format().await? {
+                    return Err(permanent(reason));
+                }
+                return Ok(SinkTableState {
+                    exists: false,
+                    schema: None,
+                    marker: None,
+                });
             }
-            return Ok(SinkTableState {
-                exists: false,
-                schema: None,
-                marker: None,
-            });
-        }
-        let pipeline = &self.config.pipeline_id;
-        let epoch = h.fence_epoch(pipeline).await?;
-        let position = h.stored_position(pipeline).await?;
-        Ok(SinkTableState {
-            exists: true,
-            schema: Some(h.schema()?),
-            marker: epoch
-                .zip(position)
-                .map(|(owner_epoch, committed_position)| CommitMarker {
-                    owner_epoch,
-                    committed_position,
-                    pipeline_id: pipeline.clone(),
-                }),
+            let pipeline = &self.config.pipeline_id;
+            let epoch = h.fence_epoch(pipeline).await?;
+            let position = h.stored_position(pipeline).await?;
+            Ok(SinkTableState {
+                exists: true,
+                schema: Some(h.schema()?),
+                marker: epoch
+                    .zip(position)
+                    .map(|(owner_epoch, committed_position)| CommitMarker {
+                        owner_epoch,
+                        committed_position,
+                        pipeline_id: pipeline.clone(),
+                    }),
+            })
         })
+        .await
     }
 
     async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError> {
-        let mut guard = self.lock(&spec.table).await?;
-        let h = guard.as_mut().expect("opened by lock");
-        if h.refresh().await? {
-            return Ok(());
-        }
-        if let Some(reason) = h.foreign_format().await? {
-            return Err(permanent(reason));
-        }
-        h.create(spec, &self.config.table).await
+        self.with_table(&spec.table, async |h| {
+            if h.refresh().await? {
+                return Ok(());
+            }
+            if let Some(reason) = h.foreign_format().await? {
+                return Err(permanent(reason));
+            }
+            h.create(spec, &self.config.table).await
+        })
+        .await
     }
 
     async fn evolve(
@@ -339,33 +357,34 @@ impl Sink for DeltaSink {
         delta: &SchemaDelta,
         owner_epoch: u64,
     ) -> Result<EvolveOutcome, SinkError> {
-        let mut guard = self.lock(table).await?;
-        let h = guard.as_mut().expect("opened by lock");
-        if !h.refresh().await? {
-            return Err(SinkError::NoTable {
-                table: table.clone(),
-            });
-        }
-        let pipeline = &self.config.pipeline_id;
-        if let Some(current) = h.fence_epoch(pipeline).await?.filter(|e| *e > owner_epoch) {
-            return Ok(EvolveOutcome::Fenced {
-                current_epoch: current,
-            });
-        }
-        // The ADD COLUMN carries the fence's `txn` (no position): a commit of a newer owner that
-        // lands after the check above conflicts with it (`ConcurrentTransaction`), and the retry
-        // finds the newer epoch.
-        let txn_version = i64::try_from(owner_epoch)
-            .map_err(|_| permanent(format!("owner epoch {owner_epoch}")))?;
-        let (t, outcome) = evolve(
-            h.table().clone(),
-            delta,
-            self.config.table.timestamp_ntz_as,
-            self.props(owner_epoch, txn_version, None),
-        )
-        .await?;
-        h.set_table(t);
-        Ok(outcome)
+        self.with_table(table, async |h| {
+            if !h.refresh().await? {
+                return Err(SinkError::NoTable {
+                    table: table.clone(),
+                });
+            }
+            let pipeline = &self.config.pipeline_id;
+            if let Some(current) = h.fence_epoch(pipeline).await?.filter(|e| *e > owner_epoch) {
+                return Ok(EvolveOutcome::Fenced {
+                    current_epoch: current,
+                });
+            }
+            // The ADD COLUMN carries the fence's `txn` (no position): a commit of a newer owner
+            // that lands after the check above conflicts with it (`ConcurrentTransaction`), and
+            // the retry finds the newer epoch.
+            let txn_version = i64::try_from(owner_epoch)
+                .map_err(|_| permanent(format!("owner epoch {owner_epoch}")))?;
+            let (t, outcome) = evolve(
+                h.table().clone(),
+                delta,
+                self.config.table.timestamp_ntz_as,
+                self.props(owner_epoch, txn_version, None),
+            )
+            .await?;
+            h.set_table(t);
+            Ok(outcome)
+        })
+        .await
     }
 
     async fn commit(
@@ -399,28 +418,29 @@ impl DeltaSink {
         marker: &CommitMarker,
         refresh: bool,
     ) -> Result<CommitOutcome, SinkError> {
-        let mut guard = self.lock(&cs.table).await?;
-        let h = guard.as_mut().expect("opened by lock");
-        let mut lost = String::new();
-        for attempt in 0..COMMIT_ATTEMPTS {
-            if attempt > 0 {
-                self.count(|s| s.retries += 1);
-            }
-            match self.attempt(h, cs, marker, refresh).await? {
-                Step::Done(outcome) => {
-                    self.count(|s| match outcome {
-                        CommitOutcome::Committed { .. } => s.commits += 1,
-                        CommitOutcome::SkippedCovered => s.skipped += 1,
-                        CommitOutcome::Fenced { .. } => s.fenced += 1,
-                    });
-                    return Ok(outcome);
+        self.with_table(&cs.table, async |h| {
+            let mut lost = String::new();
+            for attempt in 0..COMMIT_ATTEMPTS {
+                if attempt > 0 {
+                    self.count(|s| s.retries += 1);
+                }
+                match self.attempt(h, cs, marker, refresh).await? {
+                    Step::Done(outcome) => {
+                        self.count(|s| match outcome {
+                            CommitOutcome::Committed { .. } => s.commits += 1,
+                            CommitOutcome::SkippedCovered => s.skipped += 1,
+                            CommitOutcome::Fenced { .. } => s.fenced += 1,
+                        });
+                        return Ok(outcome);
+                    }
+                    Step::Retry(reason) => lost = reason,
                 }
-                Step::Retry(reason) => lost = reason,
             }
-        }
-        Err(SinkError::Transient {
-            reason: format!("the commit lost {COMMIT_ATTEMPTS} races in a row, last: {lost}"),
+            Err(SinkError::Transient {
+                reason: format!("the commit lost {COMMIT_ATTEMPTS} races in a row, last: {lost}"),
+            })
         })
+        .await
     }
 }
 
```

Change `crates/data-avalanche-sink-delta/src/table.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/table.rs b/crates/data-avalanche-sink-delta/src/table.rs
--- a/crates/data-avalanche-sink-delta/src/table.rs
+++ b/crates/data-avalanche-sink-delta/src/table.rs
@@ -7,11 +7,11 @@ use std::sync::Arc;
 use arrow_array::{ArrayRef, RecordBatch};
 use arrow_row::{RowConverter, SortField};
 use arrow_schema::SchemaRef;
-use data_avalanche_core::{PartitionTransform, SinkError, SourcePosition, TableSpec};
+use data_avalanche_core::{PartitionTransform, SinkError, SourcePosition, TableRef, TableSpec};
 use deltalake::DeltaTable;
 use deltalake::datafusion::prelude::SessionContext;
-use deltalake::logstore::object_store::ObjectStore;
 use deltalake::logstore::object_store::path::Path;
+use deltalake::logstore::object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
 use deltalake::protocol::SaveMode;
 use futures_util::TryStreamExt;
 use url::Url;
@@ -27,12 +27,24 @@ use crate::types::{column_logical, delta_field, hop2_schema, physical_type};
 pub const IDENTIFIER_KEY: &str = "data-avalanche.identifier";
 
 pub(crate) struct TableHandle {
+    /// The source table this Delta table mirrors.
+    name: TableRef,
     table: DeltaTable,
     /// The storage options the table was opened with, to re-open it from scratch.
     options: HashMap<String, String>,
     /// The newest commit of this pipeline the handle knows. The walk-back stops there, so a
     /// steady stream of own commits costs no reads.
     known: Option<Known>,
+    /// A commit file of the log the handle loaded, as the store described it then. A table
+    /// dropped and re-created behind the handle holds another file there, or none (Q-M6b-8).
+    seen: Option<Seen>,
+}
+
+/// One commit file's identity in the store: its e-tag, size and modification time.
+#[derive(Debug, Clone, PartialEq, Eq)]
+struct Seen {
+    version: u64,
+    meta: ObjectMeta,
 }
 
 /// An own commit the handle knows: the table it went to (`metaData.id`, new for a table dropped
@@ -47,6 +59,7 @@ struct Known {
 impl std::fmt::Debug for TableHandle {
     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
         f.debug_struct("TableHandle")
+            .field("name", &self.name)
             .field("url", self.table.table_url())
             .field("version", &self.table.version())
             .field("known", &self.known)
@@ -55,9 +68,13 @@ impl std::fmt::Debug for TableHandle {
 }
 
 impl TableHandle {
-    /// A handle on `url`; nothing is read yet. A local root is created first: delta-rs refuses a
-    /// `file://` location that doesn't exist.
-    pub async fn open(url: Url, options: HashMap<String, String>) -> Result<Self, SinkError> {
+    /// A handle on `url`, where `name` is mirrored; the table is loaded when it exists. A local
+    /// root is created first: delta-rs refuses a `file://` location that doesn't exist.
+    pub async fn open(
+        name: TableRef,
+        url: Url,
+        options: HashMap<String, String>,
+    ) -> Result<Self, SinkError> {
         if let Ok(path) = url.to_file_path() {
             std::fs::create_dir_all(&path)
                 .map_err(|e| permanent(format!("{}: {e}", path.display())))?;
@@ -65,11 +82,45 @@ impl TableHandle {
         let table = DeltaTable::try_from_url_with_storage_options(url, options.clone())
             .await
             .map_err(sink_error)?;
-        Ok(Self {
+        let mut handle = Self {
+            name,
             table,
             options,
             known: None,
-        })
+            seen: None,
+        };
+        handle.see().await?;
+        Ok(handle)
+    }
+
+    /// The store's description of the commit file at `version`; `None` when it's gone.
+    async fn describe(&self, version: u64) -> Result<Option<Seen>, SinkError> {
+        let store = self.table.log_store().object_store(None);
+        let path = Path::from(format!("_delta_log/{version:020}.json"));
+        match store.head(&path).await {
+            Ok(meta) => Ok(Some(Seen { version, meta })),
+            Err(deltalake::logstore::object_store::Error::NotFound { .. }) => Ok(None),
+            Err(e) => Err(sink_error(e.into())),
+        }
+    }
+
+    /// Records the loaded version's commit file, when nothing is recorded yet.
+    async fn see(&mut self) -> Result<(), SinkError> {
+        if self.seen.is_none()
+            && let Some(version) = self.table.version()
+        {
+            self.seen = self.describe(version).await?;
+        }
+        Ok(())
+    }
+
+    /// True while the recorded commit file is still the one the handle loaded: the log is the
+    /// same log. One HEAD per call.
+    async fn same_log(&self) -> Result<bool, SinkError> {
+        match &self.seen {
+            None => Ok(true),
+            Some(seen) => Ok(self.describe(seen.version).await?.as_ref() == Some(seen)),
+        }
     }
 
     pub fn table(&self) -> &DeltaTable {
@@ -84,15 +135,20 @@ impl TableHandle {
     /// Loads the newest version; `false` when the location holds no Delta table.
     ///
     /// A table dropped (and maybe re-created) behind the handle is another table: what the handle
-    /// knew of the old one is forgotten. When the loaded snapshot can't be brought forward (its
-    /// log is gone), the table is re-opened from scratch once.
+    /// knew of the old one is forgotten, and it is re-opened from scratch. That shows as a
+    /// recorded commit file the store now describes otherwise, or as a loaded snapshot that
+    /// can't be brought forward (its log is gone). Brought forward over a re-created table's
+    /// longer log, the old snapshot would hold the old table's files and miss the new one's
+    /// (Q-M6b-8).
     pub async fn refresh(&mut self) -> Result<bool, SinkError> {
         let log = self.table.log_store();
         if !log.is_delta_table_location().await.map_err(sink_error)? {
             self.known = None;
+            self.seen = None;
             return Ok(false);
         }
-        if self.table.update_incremental(None).await.is_ok() {
+        if self.same_log().await? && self.table.update_incremental(None).await.is_ok() {
+            self.see().await?;
             return Ok(true);
         }
         self.reopen().await
@@ -101,14 +157,8 @@ impl TableHandle {
     /// Re-opens the table from scratch (a fresh handle on the same URL and options, loaded) and
     /// forgets the known commit; `false` when the location holds no Delta table any more.
     async fn reopen(&mut self) -> Result<bool, SinkError> {
-        self.known = None;
-        let fresh = DeltaTable::try_from_url_with_storage_options(
-            self.table.table_url().clone(),
-            self.options.clone(),
-        )
-        .await
-        .map_err(sink_error)?;
-        self.table = fresh;
+        let url = self.table.table_url().clone();
+        *self = Self::open(self.name.clone(), url, self.options.clone()).await?;
         Ok(self.table.version().is_some())
     }
 
@@ -203,7 +253,8 @@ impl TableHandle {
                 self.table = table;
                 // A new table holds no own commit.
                 self.known = None;
-                Ok(())
+                self.seen = None;
+                self.see().await
             }
             // Another writer created it between the check and the commit.
             Err(e) => match self.refresh().await {
@@ -241,7 +292,12 @@ impl TableHandle {
         let version = match self.txn_version(pipeline_id).await {
             Ok(v) => v,
             Err(_) => {
-                self.reopen().await?;
+                // Dropped since the refresh: no table, not a delta-rs `NotATable` (`Permanent`).
+                if !self.reopen().await? {
+                    return Err(SinkError::NoTable {
+                        table: self.name.clone(),
+                    });
+                }
                 self.txn_version(pipeline_id).await?
             }
         };
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-sink-delta
```

Expected:

PASS:

```text
test result: ok. 67 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.82s
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.75s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "fix(sink-delta): a table re-created behind a live handle re-opens it; a transient failure re-opens; a table dropped since the refresh is missing (Q-M6b-8)"
```


### Task 3: A change set keyed otherwise than its table is refused

**Files:**
- Modify: `crates/data-avalanche-core/src/sink.rs`, `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-sink-delta/src/sink.rs`, `crates/data-avalanche-testkit/src/memory_sink.rs`

**Interfaces:**
- Consumes: the Delta table's recorded `data-avalanche.identifier` (M4), the `Sink::commit`
  contract (§3.4).
- Produces: commit step 5 checks the set's identifier against the table's: a mismatch is
  `Permanent` ("keyed by …"), and nothing is written. `MemorySink` does the same. The worker
  halts that table `SINK_REJECTED`, and the other tables go on.

M4-carried: a source key (or its override) changed since the table was created; merging on
the new key would duplicate rows. Refusing it halts one table and keeps the rest of the pipeline
running.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -1098,6 +1098,34 @@ mod tests {
         assert!(content.column_by_name("note").is_none(), "nothing evolved");
     }
 
+    /// The sink holds the table under another key (the source's key, or its override, changed
+    /// since the table was created): its first commit is refused, the table halts
+    /// `SINK_REJECTED` naming both keys, and the other tables go on.
+    #[tokio::test]
+    async fn a_sink_table_keyed_otherwise_halts_the_table() {
+        let w = World::new(&["a", "b"]);
+        let mut spec = TableSpec::new(&w.source.schema(&t("a")), vec![]).unwrap();
+        spec.identifier = vec!["id".into(), "doc".into()];
+        w.sink.ensure_table(&spec).await.unwrap();
+        let (end, st) = run(&w, &mode("mode: never"), async |status| {
+            w.source.commit(
+                FakeTxn::new()
+                    .insert(&t("a"), row(1, "x"))
+                    .insert(&t("b"), row(1, "x")),
+            );
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        match st.halted.get(&t("a")) {
+            Some(HaltReason::SinkRejected { detail }) => {
+                assert!(detail.contains("keyed by"), "{detail}");
+            }
+            other => panic!("{other:?}"),
+        }
+        assert_eq!(w.ids("b"), [1]);
+    }
+
     #[tokio::test]
     async fn a_lost_lease_stops_the_worker_before_its_next_commit() {
         let w = World::new(&["a"]);
```

Change `crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -606,6 +606,22 @@ mod tests {
         assert!(matches!(out, CommitOutcome::Committed { .. }), "{out:?}");
     }
 
+    /// A table created under another key refuses a set (the source's key, or its override,
+    /// changed since): merging on the set's key would duplicate rows. Nothing is written.
+    #[tokio::test]
+    async fn a_set_keyed_otherwise_than_the_table_is_refused() {
+        let dir = tempfile::tempdir().unwrap();
+        let s = sink(&dir);
+        s.ensure_table(&spec(&orders())).await.unwrap();
+        let mut cs = rows(10, &[1]);
+        cs.identifier = vec!["id".into(), "doc".into()];
+        let err = s.commit(cs.clone(), marker(1, &cs)).await.unwrap_err();
+        assert!(matches!(err, SinkError::Permanent { .. }), "{err}");
+        assert!(err.to_string().contains("keyed by"), "{err}");
+        let state = s.table_state(&cs.table).await.unwrap();
+        assert_eq!(state.marker, None, "nothing committed");
+    }
+
     #[tokio::test]
     async fn a_race_with_a_foreign_optimize_is_retried_and_lands() {
         let dir = tempfile::tempdir().unwrap();
@@ -802,14 +818,18 @@ mod tests {
     async fn a_truncate_set_with_a_key_outside_the_delta_range_changes_nothing() {
         let dir = tempfile::tempdir().unwrap();
         let s = sink(&dir);
-        s.ensure_table(&spec(&orders())).await.unwrap();
-        let seed = rows(10, &[1]);
+        // A table keyed by `at`, which is `id` milliseconds.
+        let mut keyed = spec(&orders());
+        keyed.identifier = vec!["at".into()];
+        s.ensure_table(&keyed).await.unwrap();
+        let mut seed = rows(10, &[1]);
+        seed.identifier = vec!["at".into()];
         let CommitOutcome::Committed { version } =
             s.commit(seed.clone(), marker(1, &seed)).await.unwrap()
         else {
             panic!("committed")
         };
-        // `at` is `id` milliseconds: past 9999-12-31, and the set's key.
+        // Past 9999-12-31, and the set's key.
         let mut t = rows(20, &[300_000_000_000_000]);
         t.truncate = true;
         t.identifier = vec!["at".into()];
```

Change `crates/data-avalanche-testkit/src/memory_sink.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -66,6 +66,12 @@ impl MemTable {
     /// Every column of the change set exists in the table with the same type; the table may have
     /// more (soft-dropped columns, written NULL).
     fn check_schema(&self, cs: &TableChangeSet) -> Result<(), SinkError> {
+        if cs.identifier != self.identifier {
+            return Err(permanent(format!(
+                "the change set is keyed by {:?}, the table by {:?}",
+                cs.identifier, self.identifier
+            )));
+        }
         for f in cs.schema.fields() {
             match self.schema.field_with_name(f.name()) {
                 Ok(t) if t.data_type() == f.data_type() => {}
@@ -865,6 +871,19 @@ mod tests {
         );
     }
 
+    /// A table keyed otherwise than the set (the source's key, or its override, changed since
+    /// the table was created) refuses it: merging on another key would duplicate rows.
+    #[tokio::test]
+    async fn a_set_keyed_otherwise_than_the_table_is_refused() {
+        let sink = sink().await;
+        let mut c = cs(10, vec![upserts(&[(1, Some("a"))], None)], vec![]);
+        c.identifier = vec!["id".into(), "doc".into()];
+        let err = sink.commit(c, marker(1, 10)).await.unwrap_err();
+        assert!(matches!(err, SinkError::Permanent { .. }), "{err}");
+        assert!(err.to_string().contains("keyed by"), "{err}");
+        assert_eq!(sink.version(&TableRef::new("public", "t")), Some(0));
+    }
+
     #[tokio::test]
     async fn contract_violations_are_errors() {
         let sink = sink().await;
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-testkit -p data-avalanche-pipeline -p data-avalanche-sink-delta -- keyed_otherwise
```

Expected:

FAIL:

```text
test sink::tests::a_set_keyed_otherwise_than_the_table_is_refused ... FAILED
called `Result::unwrap_err()` on an `Ok` value: Committed { version: 1 }
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 67 filtered out; finished in 0.18s
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -298,7 +298,8 @@ pub trait Sink: Send + Sync {
     /// 3. [`SinkError::NoTable`] when the table is absent;
     /// 4. the fence, then the guard, against the stored marker: a [`CommitOutcome::Fenced`] or
     ///    [`CommitOutcome::SkippedCovered`] commit changes nothing;
-    /// 5. every column of the set must be in the table with the same type;
+    /// 5. the set's identifier must be the one the table was created with, and every column of
+    ///    the set must be in the table with the same type;
     /// 6. apply, as one table version: the truncate clear, then the deletes, then the upserts;
     /// 7. store the marker with the **union** of the prior stored position and the new one.
     async fn commit(
```

Change `crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -231,6 +231,13 @@ impl DeltaSink {
                 return Ok(Step::Done(CommitOutcome::SkippedCovered));
             }
         }
+        let recorded = h.identifier()?;
+        if !recorded.is_empty() && recorded != cs.identifier {
+            return Err(permanent(format!(
+                "the change set is keyed by {:?}, the table by {:?}",
+                cs.identifier, recorded
+            )));
+        }
         let snapshot = h.table().snapshot().map_err(sink_error)?;
         let types = check_columns(cs, snapshot.schema().as_ref())?;
         // Before the truncate's DELETE: a key that halts the MERGE must not leave the table
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-testkit -p data-avalanche-pipeline -p data-avalanche-sink-delta -- keyed_otherwise
```

Expected:

PASS:

```text
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 174 filtered out; finished in 0.01s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 67 filtered out; finished in 0.03s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 20 filtered out; finished in 0.00s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "fix(sink-delta): a change set keyed otherwise than its table is refused, in the Delta and the reference sink"
```


### Task 4: Config and the Delta sink: `merge_memory_limit` (V-119)

**Files:**
- Modify: `crates/data-avalanche-config/src/build.rs`, `crates/data-avalanche-config/src/merge.rs`, `crates/data-avalanche-config/src/raw.rs`, `crates/data-avalanche-config/src/registry.rs`, `crates/data-avalanche-config/src/resolved.rs`, `crates/data-avalanche-config/src/rules/domain.rs`, `crates/data-avalanche-config/tests/load.rs`, `crates/data-avalanche-config/tests/rules/domain.rs`, `crates/data-avalanche-sink-delta/src/config.rs`, `crates/data-avalanche-sink-delta/src/error.rs`, `crates/data-avalanche-sink-delta/src/sink.rs`

**Interfaces:**
- Consumes: the config pipeline (raw → merge → build → rules, M1), `DeltaSinkConfig::
  merge_memory_limit` (M4).
- Produces:
  - `sink.delta.merge_memory_limit: ByteSize | null` (resolved `Delta::merge_memory_limit:
    Option<u64>`);
  - V-119 (≥ 16 MiB when set), registered;
  - in the Delta sink, DataFusion's "Resources exhausted" is `Permanent`, naming the key.

Q-M6b-6. The same change set needs the same memory on every replay, so a retry can't
succeed: `Permanent` lets the writer halt the table (`SINK_REJECTED`) instead of retrying
forever. The spec rows are in "Spec patches".

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1017,7 +1017,7 @@ mod tests {
 
     #[test]
     fn has_every_spec_rule_once_in_order() {
-        assert_eq!(RULES.len(), 113);
+        assert_eq!(RULES.len(), 114);
         assert!(
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
```

Change `crates/data-avalanche-config/tests/load.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/load.rs b/crates/data-avalanche-config/tests/load.rs
--- a/crates/data-avalanche-config/tests/load.rs
+++ b/crates/data-avalanche-config/tests/load.rs
@@ -186,6 +186,21 @@ fn absent_mysql_and_delta_blocks_take_every_default() {
     let delta = p.sink.delta.as_ref().unwrap();
     assert_eq!(delta.checkpoint_interval, 10);
     assert_eq!(delta.column_mapping, ColumnMapping::None);
+    assert_eq!(delta.merge_memory_limit, None, "no ceiling by default");
+}
+
+#[test]
+fn a_merge_memory_limit_resolves_to_bytes() {
+    let yaml = MINIMAL.replace("delta: {}", "delta: { merge_memory_limit: 256MiB }");
+    assert!(yaml.contains("merge_memory_limit"), "{yaml}");
+    let loaded =
+        load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_or_else(|r| panic!("{r:#?}"));
+    let delta = loaded.config.pipelines["orders"]
+        .sink
+        .delta
+        .clone()
+        .unwrap();
+    assert_eq!(delta.merge_memory_limit, Some(256 << 20));
 }
 
 #[test]
```

Change `crates/data-avalanche-config/tests/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/domain.rs b/crates/data-avalanche-config/tests/rules/domain.rs
--- a/crates/data-avalanche-config/tests/rules/domain.rs
+++ b/crates/data-avalanche-config/tests/rules/domain.rs
@@ -323,6 +323,15 @@ pub fn cases() -> Vec<Case> {
         case("V-118", Silent, "snapshot timeout 10s", || {
             pg_delta().set("/pipelines/p/source/timeouts/snapshot", json!("10s"))
         }),
+        case("V-119", Error, "merge memory 1MiB", || {
+            pg_delta().set("/pipelines/p/sink/delta/merge_memory_limit", json!("1MiB"))
+        }),
+        case("V-119", Silent, "merge memory 16MiB", || {
+            pg_delta().set("/pipelines/p/sink/delta/merge_memory_limit", json!("16MiB"))
+        }),
+        case("V-119", Silent, "merge memory null", || {
+            pg_delta().set("/pipelines/p/sink/delta/merge_memory_limit", json!(null))
+        }),
         case("V-118", Error, "snapshot timeout 9999ms", || {
             pg_delta().set("/pipelines/p/source/timeouts/snapshot", json!("9999ms"))
         }),
```

Change `crates/data-avalanche-sink-delta/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/error.rs b/crates/data-avalanche-sink-delta/src/error.rs
--- a/crates/data-avalanche-sink-delta/src/error.rs
+++ b/crates/data-avalanche-sink-delta/src/error.rs
@@ -97,5 +97,11 @@ mod tests {
             "External error: Invalid data found: 1 rows failed validation check.".into(),
         ));
         assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
+        // DataFusion's memory pool, past `merge_memory_limit`.
+        let e = sink_error(DeltaTableError::Generic(
+            "Resources exhausted: Failed to allocate additional 1.0 MB for HashJoinInput".into(),
+        ));
+        assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
+        assert!(e.to_string().contains("merge_memory_limit"), "{e}");
     }
 }
```

Change `crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -629,6 +629,33 @@ mod tests {
         assert_eq!(state.marker, None, "nothing committed");
     }
 
+    /// A MERGE over `merge_memory_limit` is refused `Permanent`, naming the key: retrying the same
+    /// set would fail the same way forever. Nothing is written.
+    #[tokio::test]
+    async fn a_merge_over_the_memory_limit_is_permanent() {
+        let dir = tempfile::tempdir().unwrap();
+        let s = DeltaSink::new(DeltaSinkConfig {
+            merge_memory_limit: Some(64 << 10),
+            ..sink(&dir).config
+        });
+        s.ensure_table(&spec(&orders())).await.unwrap();
+        let seed = rows(10, &(0..20_000).collect::<Vec<_>>());
+        let ok = sink(&dir);
+        ok.commit(seed.clone(), marker(1, &seed)).await.unwrap();
+        let again = rows(20, &(0..20_000).collect::<Vec<_>>());
+        let err = s
+            .commit(again.clone(), marker(1, &again))
+            .await
+            .unwrap_err();
+        assert!(matches!(err, SinkError::Permanent { .. }), "{err}");
+        assert!(err.to_string().contains("merge_memory_limit"), "{err}");
+        let state = s.table_state(&again.table).await.unwrap();
+        assert_eq!(
+            state.marker.unwrap().committed_position,
+            SourcePosition::Pg(Lsn(10))
+        );
+    }
+
     #[tokio::test]
     async fn a_race_with_a_foreign_optimize_is_retried_and_lands() {
         let dir = tempfile::tempdir().unwrap();
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-config &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-sink-delta -- merge_over
```

Expected:

FAIL to compile (2 errors); the first:

```text
error[E0609]: no field `merge_memory_limit` on type `&data_avalanche_config::Delta`
error[E0609]: no field `merge_memory_limit` on type `data_avalanche_config::Delta`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-config/src/build.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/build.rs b/crates/data-avalanche-config/src/build.rs
--- a/crates/data-avalanche-config/src/build.rs
+++ b/crates/data-avalanche-config/src/build.rs
@@ -1353,6 +1353,7 @@ fn delta(d: &RawDelta, at: &At, ctx: &mut Ctx) -> Option<Delta> {
         "type",
         DeltaCatalogType::None,
     );
+    let merge_memory_limit = tri(ctx, &d.merge_memory_limit, at, "merge_memory_limit", None);
     Some(Delta {
         column_mapping: column_mapping?,
         checkpoint_interval: checkpoint_interval?,
@@ -1360,6 +1361,7 @@ fn delta(d: &RawDelta, at: &At, ctx: &mut Ctx) -> Option<Delta> {
         timestamp_ntz_as: timestamp_ntz_as?,
         table_properties: table_properties?,
         catalog_type: catalog_type?,
+        merge_memory_limit: merge_memory_limit?.map(ByteSize::bytes),
     })
 }
 
```

Change `crates/data-avalanche-config/src/merge.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/merge.rs b/crates/data-avalanche-config/src/merge.rs
--- a/crates/data-avalanche-config/src/merge.rs
+++ b/crates/data-avalanche-config/src/merge.rs
@@ -177,7 +177,8 @@ merge_struct!(RawDelta {
     log_retention,
     timestamp_ntz_as,
     table_properties,
-    catalog
+    catalog,
+    merge_memory_limit
 });
 merge_struct!(RawDeltaCatalog { kind });
 merge_struct!(RawSchemaEvolution {
```

Change `crates/data-avalanche-config/src/raw.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/raw.rs b/crates/data-avalanche-config/src/raw.rs
--- a/crates/data-avalanche-config/src/raw.rs
+++ b/crates/data-avalanche-config/src/raw.rs
@@ -399,6 +399,8 @@ pub(crate) struct RawDelta {
     pub timestamp_ntz_as: L<TimestampNtzAs>,
     pub table_properties: Option<BTreeMap<String, Leaf<String>>>,
     pub catalog: Option<RawDeltaCatalog>,
+    #[serde(default, deserialize_with = "tri")]
+    pub merge_memory_limit: Tri<ByteSize>,
 }
 
 #[derive(Debug, Clone, Default, Deserialize)]
```

Change `crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1004,6 +1004,14 @@ pub const RULES: &[RuleMeta] = &[
         r#"source.timeouts.snapshot"#,
         r#"≥ 10s."#,
     ),
+    rule(
+        "V-119",
+        Error,
+        L::Pipeline,
+        Config,
+        r#"sink.delta.merge_memory_limit"#,
+        r#"≥ 16 MiB when set."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
```

Change `crates/data-avalanche-config/src/resolved.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/resolved.rs b/crates/data-avalanche-config/src/resolved.rs
--- a/crates/data-avalanche-config/src/resolved.rs
+++ b/crates/data-avalanche-config/src/resolved.rs
@@ -453,6 +453,9 @@ pub struct Delta {
     pub timestamp_ntz_as: TimestampNtzAs,
     pub table_properties: BTreeMap<String, String>,
     pub catalog_type: DeltaCatalogType,
+    /// The memory one MERGE may take (DataFusion's pool, R-6); `None`: no ceiling. A MERGE that
+    /// needs more halts its table.
+    pub merge_memory_limit: Option<u64>,
 }
 
 /// Where the pipeline's operational state lives (§9.6, §10).
```

Change `crates/data-avalanche-config/src/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/domain.rs b/crates/data-avalanche-config/src/rules/domain.rs
--- a/crates/data-avalanche-config/src/rules/domain.rs
+++ b/crates/data-avalanche-config/src/rules/domain.rs
@@ -389,6 +389,22 @@ fn sink(s: &RawSink, at: &At, ctx: &mut Ctx, cross_field: bool) {
     );
     if let Some(d) = &s.delta {
         let d_at = at.key("delta");
+        // V-119
+        let mem_at = d_at.key("merge_memory_limit");
+        if let Some(Some(l)) = &d.merge_memory_limit
+            && let Some(v) = ctx.value(l, &mem_at)
+            && v.bytes() < 16 * ByteSize::MIB
+        {
+            ctx.error(
+                "V-119",
+                &mem_at,
+                l.loc(),
+                format!(
+                    "merge_memory_limit must be at least 16MiB when set, got {}",
+                    shown_value(l, &v)
+                ),
+            );
+        }
         // V-092, V-088
         if let Some((ColumnMapping::Name, loc)) =
             val(ctx, &d.column_mapping, &d_at.key("column_mapping"))
```

Change `crates/data-avalanche-sink-delta/src/config.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/config.rs b/crates/data-avalanche-sink-delta/src/config.rs
--- a/crates/data-avalanche-sink-delta/src/config.rs
+++ b/crates/data-avalanche-sink-delta/src/config.rs
@@ -188,8 +188,9 @@ pub struct DeltaSinkConfig {
     pub pipeline_id: String,
     pub location: DeltaLocation,
     pub table: TableDefaults,
-    /// A DataFusion memory ceiling for one MERGE (R-6). A MERGE that needs more fails
-    /// (`Transient`) instead of growing the process. `None`: no ceiling.
+    /// A DataFusion memory ceiling for one MERGE (R-6, `sink.delta.merge_memory_limit`). A MERGE
+    /// that needs more fails `Permanent` instead of growing the process: the same set fails the
+    /// same way on every replay, so its table halts. `None`: no ceiling.
     pub merge_memory_limit: Option<usize>,
 }
 
```

Change `crates/data-avalanche-sink-delta/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/error.rs b/crates/data-avalanche-sink-delta/src/error.rs
--- a/crates/data-avalanche-sink-delta/src/error.rs
+++ b/crates/data-avalanche-sink-delta/src/error.rs
@@ -20,12 +20,25 @@ fn failed_validation(text: &str) -> bool {
     text.contains("failed validation check") || text.contains("validation check failed")
 }
 
+/// DataFusion's memory pool refused an allocation (`ResourcesExhausted`, which reaches us as
+/// `Generic` text). The pool is bounded only by `merge_memory_limit`, and the same set needs
+/// the same memory on every replay.
+fn out_of_memory(text: &str) -> bool {
+    text.contains("Resources exhausted")
+}
+
 pub(crate) fn sink_error(e: DeltaTableError) -> SinkError {
     use CommitConflictError as C;
     use DeltaTableError as E;
     let reason = e.to_string();
     match e {
         E::Generic(text) if failed_validation(&text) => SinkError::Permanent { reason },
+        E::Generic(text) if out_of_memory(&text) => SinkError::Permanent {
+            reason: format!(
+                "the MERGE needs more memory than sink.delta.merge_memory_limit allows (raise it, or \
+                 flush fewer rows per commit): {text}"
+            ),
+        },
         // The conflicts that aren't a lost race: retrying reads the same table and fails again.
         E::Transaction {
             source:
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-config &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-sink-delta -- merge_over
```

Expected:

PASS:

```text
test result: ok. 81 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 44 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.33s
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 67 filtered out; finished in 1.02s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(config): sink.delta.merge_memory_limit (V-119); a MERGE over it halts its table (Q-M6b-6)"
```


### Task 5: The worker: pause and resume keep the source warm

**Files:**
- Create: `crates/data-avalanche-pipeline/tests/pg/pause.rs`
- Modify: `crates/data-avalanche-pipeline/Cargo.toml`, `crates/data-avalanche-pipeline/src/session.rs`, `crates/data-avalanche-pipeline/src/session/snapshot.rs`, `crates/data-avalanche-pipeline/src/status.rs`, `crates/data-avalanche-pipeline/src/test_support.rs`, `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-pipeline/tests/pg/common.rs`, `crates/data-avalanche-pipeline/tests/pg/main.rs`

**Interfaces:**
- Consumes: `PipelineWorker`, `Command`, `Phase` (§3.11), the session's event loop and
  `session/snapshot`.
- Produces:
  - `Command::Pause`: finish the open transaction, flush, make durable, then read nothing.
  - `Command::Resume`: read on.
  - `Phase::Paused`.
  - A pause during a reconnect parks the next session; a drain while paused ends at once.
  - The snapshot timeout's clock stops while paused.

Q-M6b-1. While paused, the session reads nothing, so pgwire sends standby status on its own
and the walsender stays (`a_pause_keeps_the_connection_past_wal_sender_timeout` sets the role's
`wal_sender_timeout` to 2 s and pauses for 8 s: same `active_pid`). The permit arm waits only for
a queued job. An empty queue made it ready at every poll, and a paused session spun at ~190 % CPU
(found by Task 10's controller tests; `a_paused_session_idles` pins it on the paused clock).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-pipeline/Cargo.toml b/crates/data-avalanche-pipeline/Cargo.toml
--- a/crates/data-avalanche-pipeline/Cargo.toml
+++ b/crates/data-avalanche-pipeline/Cargo.toml
@@ -32,5 +32,5 @@ data-avalanche-testkit.workspace = true
 futures-util.workspace = true
 object_store.workspace = true
 proptest.workspace = true
-tokio.workspace = true
+tokio = { workspace = true, features = ["test-util"] }
 tokio-postgres.workspace = true
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -1214,6 +1214,110 @@ mod tests {
         assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6]);
     }
 
+    /// A pause arrives with a transaction open (a slice in the sink, the next waiting for room):
+    /// the session finishes the transaction and makes it durable, then reads nothing until it is
+    /// resumed.
+    #[tokio::test]
+    async fn a_pause_finishes_the_open_transaction_then_reads_nothing_until_resumed() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let p = pipeline(
+            "",
+            "flush: { seconds: 0 }, max_transaction_batch_rows: 2, buffer_capacity_rows: 1",
+            "",
+        );
+        let (mut core, control, status) = w.core(&p, &["a"]);
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        w.sink.hold_commits();
+        let mut txn = FakeTxn::new();
+        for id in 1..=6 {
+            txn = txn.insert(&t("a"), row(id, "x"));
+        }
+        w.source.commit(txn);
+        let first = w.source.wal_end();
+        let driver = async {
+            wait_until("a slice waits in the sink", || {
+                w.sink.waiting_commits() == 1
+            })
+            .await;
+            control.send(Command::Pause).await.unwrap();
+            w.sink.release_commits();
+            wait_until("paused", || status.borrow().phase == Phase::Paused).await;
+            assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6], "the transaction is in");
+            assert_eq!(status.borrow().checkpoint.as_ref(), Some(&first));
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(7, "x")));
+            let second = w.source.wal_end();
+            tokio::time::sleep(Duration::from_millis(50)).await;
+            assert_eq!(
+                status.borrow().read.as_ref(),
+                Some(&first),
+                "nothing is read while paused"
+            );
+            control.send(Command::Resume).await.unwrap();
+            durable(&status, second).await;
+            assert_eq!(status.borrow().phase, Phase::Streaming);
+            control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(s.run(), driver);
+        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
+        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6, 7]);
+    }
+
+    /// A paused session waits on its timers and nothing else: ten seconds pass on the paused
+    /// clock, which advances only while every task is idle. A session that spins keeps it still,
+    /// and a real two-second watchdog fails the test.
+    #[tokio::test(start_paused = true)]
+    async fn a_paused_session_idles() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let (mut core, control, status) = w.core(&flush_each(), &["a"]);
+        core.pause = true;
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        let driver = async {
+            wait_until("parked", || status.borrow().phase == Phase::Paused).await;
+            let (spun, spinning) = tokio::sync::oneshot::channel::<()>();
+            std::thread::spawn(move || {
+                std::thread::sleep(std::time::Duration::from_secs(2));
+                let _ = spun.send(());
+            });
+            tokio::select! {
+                () = tokio::time::sleep(Duration::from_secs(10)) => {}
+                _ = spinning => panic!("the paused session spins: the paused clock stood still"),
+            }
+            control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(s.run(), driver);
+        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
+    }
+
+    #[tokio::test]
+    async fn a_drain_while_paused_ends_at_once() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let (end, last) = session(&w, &flush_each(), &["a"], async |status| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            let first = w.source.wal_end();
+            durable(&status, first).await;
+        })
+        .await;
+        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
+        let _ = last;
+        let (mut core, control, status) = w.core(&flush_each(), &["a"]);
+        core.pause = true;
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        let driver = async {
+            wait_until("a session that opens paused parks", || {
+                status.borrow().phase == Phase::Paused
+            })
+            .await;
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
+            control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(s.run(), driver);
+        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
+        assert_eq!(w.ids("a"), [1], "the paused session read nothing more");
+    }
+
     #[tokio::test]
     async fn a_full_buffer_stops_the_reading() {
         let w = World::new(&["a"]);
```

Change `crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -654,7 +654,7 @@ mod tests {
     use super::*;
     use crate::session::Core;
     use crate::test_support::{World, Yaml, row, t, wait_until};
-    use crate::{Backoff, Command, Exit, Hooks, ReconcileLog, WorkerStatus};
+    use crate::{Backoff, Command, Exit, Hooks, Phase, ReconcileLog, WorkerStatus};
 
     fn chunks_of(n: u64) -> Pipeline {
         let tables = format!("\n      snapshots: {{ chunk_size: {n} }}");
@@ -688,6 +688,37 @@ mod tests {
         (end.unwrap(), last)
     }
 
+    /// A pause holds a running snapshot: no chunk is read, and its time doesn't count against
+    /// `timeouts.snapshot` (the clock moves on by the time spent paused).
+    #[tokio::test]
+    async fn a_pause_stops_the_snapshot_and_its_clock() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        w.source.load(&t("a"), rows(1..=5));
+        w.request(&["a"]).await;
+        let (mut core, control, status) = w.core(&chunks_of(2), &["a"]);
+        core.pause = true;
+        let opened = Instant::now();
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        let driver = async {
+            wait_until("parked", || status.borrow().phase == Phase::Paused).await;
+            tokio::time::sleep(Duration::from_millis(200)).await;
+            assert_eq!(w.source.reads(&t("a")), 0, "no chunk is read while paused");
+            control.send(Command::Resume).await.unwrap();
+            snapshot_done(&w, &status, "a").await;
+            control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(s.run(), driver);
+        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
+        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5]);
+        let started = core.snapshot_started[&t("a")];
+        assert!(
+            started.duration_since(opened) >= Duration::from_millis(200),
+            "the pause is added back: {:?}",
+            started.duration_since(opened)
+        );
+    }
+
     async fn snapshot_done(w: &World, status: &watch::Receiver<WorkerStatus>, table: &str) {
         let _ = w;
         wait_until("the snapshot's completion is durable", || {
```

Change `crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -411,6 +411,7 @@ impl World {
             wal_lag_failing: Streak::default(),
             writers_waited: BTreeSet::new(),
             read_failing: BTreeMap::new(),
+            pause: false,
         };
         (core, control, status_rx)
     }
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -609,8 +609,9 @@ mod tests {
     use data_avalanche_testkit::{FakeTxn, Method};
 
     use super::*;
-    use crate::HookPoint;
     use crate::test_support::{CrashAt, World, Yaml, fast, row, t, wait_until};
+    use crate::{Backoff, HookPoint};
+    use std::time::Duration;
 
     fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
         ids.map(|id| row(id, "x")).collect()
@@ -844,6 +845,44 @@ mod tests {
         assert_eq!(w.source.opened().len(), 2);
     }
 
+    /// A pause that arrives between sessions (the reconnect's backoff) holds the next session as
+    /// it opens; a resume reads again.
+    #[tokio::test]
+    async fn a_pause_during_a_reconnect_parks_the_next_session() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let tuning = Tuning {
+            source_backoff: Backoff {
+                first: Duration::from_millis(300),
+                max: Duration::from_millis(300),
+            },
+            ..fast()
+        };
+        let (worker, handle) = PipelineWorker::new(&mode("mode: never"), deps(&w), tuning);
+        let driver = async {
+            w.source
+                .fail(Method::Next, SourceError::Transient("reset".into()));
+            wait_until("reconnecting", || {
+                matches!(handle.status.borrow().phase, Phase::Reconnecting { .. })
+            })
+            .await;
+            handle.control.send(Command::Pause).await.unwrap();
+            wait_until("the next session parks", || {
+                handle.status.borrow().phase == Phase::Paused
+            })
+            .await;
+            assert_eq!(w.source.opened().len(), 2, "a new session, parked");
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            tokio::time::sleep(Duration::from_millis(50)).await;
+            assert!(w.ids("a").is_empty(), "nothing is read while paused");
+            handle.control.send(Command::Resume).await.unwrap();
+            wait_until("the row lands", || w.ids("a") == [1]).await;
+            handle.control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(worker.run(), driver);
+        assert!(matches!(end, Ok(Exit::Drained)), "{end:?}");
+    }
+
     #[tokio::test]
     async fn a_busy_slot_is_retried_for_a_while() {
         let w = World::new(&["a"]);
```

Change `crates/data-avalanche-pipeline/tests/pg/common.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/tests/pg/common.rs b/crates/data-avalanche-pipeline/tests/pg/common.rs
--- a/crates/data-avalanche-pipeline/tests/pg/common.rs
+++ b/crates/data-avalanche-pipeline/tests/pg/common.rs
@@ -99,6 +99,27 @@ pub fn pipeline_with(
     sink: &str,
     more: &str,
 ) -> Pipeline {
+    load_pipeline(&pipeline_yaml(n, include, snapshots, source, sink, more))
+}
+
+/// [`pipeline`] (mode `never`), connecting as `user` (password the same as the name).
+pub fn pipeline_as(n: &Names, include: &[&str], user: &str) -> Pipeline {
+    let yaml = pipeline_yaml(n, include, "mode: never", "", "", "").replace(
+        "user: avalanche\n      password: avalanche",
+        &format!("user: {user}\n      password: {user}"),
+    );
+    assert!(yaml.contains(&format!("user: {user}")), "{yaml}");
+    load_pipeline(&yaml)
+}
+
+fn pipeline_yaml(
+    n: &Names,
+    include: &[&str],
+    snapshots: &str,
+    source: &str,
+    sink: &str,
+    more: &str,
+) -> String {
     let include: Vec<String> = include
         .iter()
         .map(|i| format!("{}.{i}", n.schema))
@@ -129,17 +150,17 @@ pipelines:
         slot = n.slot,
         include = include.join(", "),
     );
-    load_str(
-        &yaml,
-        &BTreeMap::new(),
-        &FingerprintKey::from_bytes([1; 32]),
-    )
-    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
-    .config
-    .pipelines
-    .into_values()
-    .next()
-    .expect("one pipeline")
+    yaml
+}
+
+fn load_pipeline(yaml: &str) -> Pipeline {
+    load_str(yaml, &BTreeMap::new(), &FingerprintKey::from_bytes([1; 32]))
+        .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
+        .config
+        .pipelines
+        .into_values()
+        .next()
+        .expect("one pipeline")
 }
 
 /// The sink and the state documents a pipeline keeps across its runs.
```

Change `crates/data-avalanche-pipeline/tests/pg/main.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/tests/pg/main.rs b/crates/data-avalanche-pipeline/tests/pg/main.rs
--- a/crates/data-avalanche-pipeline/tests/pg/main.rs
+++ b/crates/data-avalanche-pipeline/tests/pg/main.rs
@@ -3,6 +3,7 @@
 //! documents are in memory. Each test returns at once without `DATA_AVALANCHE_IT`.
 
 mod common;
+mod pause;
 mod recovery;
 mod schema;
 mod snapshot;
```

Create `crates/data-avalanche-pipeline/tests/pg/pause.rs`:

```rust
//! A paused worker keeps its replication connection (Q-M6b-1): past `wal_sender_timeout` the
//! walsender is the same one, and a resume reads on. The test's own role sets
//! `wal_sender_timeout` for its connections (`ALTER ROLE … SET`), so the server's setting is
//! untouched.

use std::time::Duration;

use data_avalanche_pipeline::{Command, Exit, Phase};
use data_avalanche_testkit::require_it;
use tokio_postgres::Client;

use crate::common::{Lake, admin, cleanup, converge, fresh, pipeline_as, start};

const ROLE: &str = "pl_pause";

/// The walsender serving the slot now, if any.
async fn walsender(a: &Client, slot: &str) -> Option<i32> {
    a.query_one(
        "SELECT (SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1)",
        &[&slot],
    )
    .await
    .expect("read the slot")
    .get(0)
}

#[tokio::test]
async fn a_pause_keeps_the_connection_past_wal_sender_timeout() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "pause",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);",
    )
    .await;
    a.batch_execute(&format!(
        "DROP ROLE IF EXISTS {ROLE};
         CREATE ROLE {ROLE} SUPERUSER LOGIN PASSWORD '{ROLE}';
         ALTER ROLE {ROLE} SET wal_sender_timeout = '2s';"
    ))
    .await
    .expect("the test role");
    let (role, conn) = tokio_postgres::connect(
        &crate::common::dsn().replace(
            "user=avalanche password=avalanche",
            "user=pl_pause password=pl_pause",
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect as the test role");
    tokio::spawn(conn);
    let timeout: String = role
        .query_one("SHOW wal_sender_timeout", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(timeout, "2s", "the role's sessions time out after 2s");
    drop(role);
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(&pipeline_as(&n, &["t"], ROLE), &lake, None);
    // `mode: never`: a row is streamed only once the slot exists.
    run.wait("streaming", |st| st.phase == Phase::Streaming)
        .await;
    a.batch_execute(&format!("INSERT INTO {t} VALUES (1, 'a')"))
        .await
        .unwrap();
    converge(&a, &lake, &t).await;
    let before = walsender(&a, &n.slot).await.expect("a walsender");
    run.handle.control.send(Command::Pause).await.unwrap();
    run.wait("paused", |st| st.phase == Phase::Paused).await;
    // Four times the role's wal_sender_timeout.
    tokio::time::sleep(Duration::from_secs(8)).await;
    a.batch_execute(&format!("INSERT INTO {t} VALUES (2, 'b')"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(lake.rows(&t), [(1, Some("a".into()))], "paused");
    assert_eq!(
        walsender(&a, &n.slot).await,
        Some(before),
        "the same walsender: the connection outlived wal_sender_timeout"
    );
    run.handle.control.send(Command::Resume).await.unwrap();
    converge(&a, &lake, &t).await;
    assert_eq!(walsender(&a, &n.slot).await, Some(before));
    assert_eq!(run.drain().await.unwrap(), Exit::Drained);
    cleanup(&a, &n).await;
    a.batch_execute(&format!("DROP ROLE IF EXISTS {ROLE}"))
        .await
        .expect("drop the test role");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --lib &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --test pg -- --test-threads=1 pause
```

Expected:

FAIL to compile (14 errors); the first:

```text
error[E0609]: no field `pause` on type `Core`
error[E0599]: no variant, associated function, or constant named `Paused` found for enum `status::Phase` in the current scope
error[E0599]: no variant, associated function, or constant named `Resume` found for enum `status::Command` in the current scope
error[E0599]: no variant, associated function, or constant named `Pause` found for enum `status::Command` in the current scope
error[E0560]: struct `Core` has no field named `pause`
… and 9 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -78,6 +78,10 @@ pub(crate) struct Core {
     /// The tables whose chunk reads are failing, a streak each. Like `requery_failing` it outlives
     /// a session.
     pub read_failing: BTreeMap<TableRef, Streak>,
+    /// [`Command::Pause`] is in force: a session finishes its open transaction, makes it durable
+    /// and reads nothing more until [`Command::Resume`]. It outlives a session, so a reconnect
+    /// while paused stays paused.
+    pub pause: bool,
 }
 
 impl Core {
@@ -150,6 +154,8 @@ pub(crate) struct Session<'c> {
     )>,
     snaps: Snapshots,
     draining: bool,
+    /// When this session parked for [`Command::Pause`], with everything it read durable.
+    paused: Option<Instant>,
 }
 
 impl<'c> Session<'c> {
@@ -264,6 +270,7 @@ impl<'c> Session<'c> {
             rescan: None,
             snaps,
             draining: false,
+            paused: None,
         }))
     }
 
@@ -343,7 +350,18 @@ impl<'c> Session<'c> {
                 }
                 return Ok(End::Exit(Exit::Drained));
             }
-            let reading = self.queued.is_empty();
+            if self.core.pause && self.paused.is_none() && !self.acc.in_transaction() {
+                self.flush()?;
+                if let Some(end) = self.until_durable().await? {
+                    return Ok(end);
+                }
+                self.paused = Some(Instant::now());
+                self.core.update(|st| st.phase = Phase::Paused);
+            }
+            // Pausing (the open transaction still to finish) or paused: no chunk is read, no
+            // rescan runs, the snapshot clock stands. Paused: the stream isn't read either.
+            let parked = self.core.pause;
+            let reading = self.queued.is_empty() && self.paused.is_none();
             let need = self.queued.front().map_or(0, |(_, n)| *n);
             let permits = self.permits.clone();
             // Biased: control, the writer's events and the buffer's room first; then the chunk
@@ -361,10 +379,12 @@ impl<'c> Session<'c> {
                         return Ok(end);
                     }
                 }
-                p = permits.acquire_many_owned(need), if !reading => {
+                // Only for a queued job: an empty queue needs no permit, and would make this
+                // arm ready at every poll while paused.
+                p = permits.acquire_many_owned(need), if !self.queued.is_empty() => {
                     self.send_front(p.expect("the semaphore isn't closed"));
                 }
-                read = self.snaps.reads.join_next(), if !self.snaps.reads.is_empty() => {
+                read = self.snaps.reads.join_next(), if !self.snaps.reads.is_empty() && !parked => {
                     // Nothing aborts a read while the loop runs: one that ended without its
                     // chunk panicked, a bug, which ends the worker like the writer's.
                     let read = read.expect("not empty").map_err(|e| {
@@ -376,13 +396,13 @@ impl<'c> Session<'c> {
                 }
                 _ = tick.tick() => {
                     self.due(Instant::now())?;
-                    if let Some(end) = self.snapshot_tick().await? {
+                    if !parked && let Some(end) = self.snapshot_tick().await? {
                         return Ok(end);
                     }
                 }
                 _ = heartbeat.tick() => self.wal_lag().await?,
                 _ = async { rescan.as_mut().expect("guarded").tick().await },
-                    if rescan.is_some() && self.rescan.is_none() =>
+                    if rescan.is_some() && self.rescan.is_none() && !parked =>
                 {
                     if let Some(end) = self.rescan().await? {
                         return Ok(end);
@@ -408,6 +428,23 @@ impl<'c> Session<'c> {
                 self.core.update(|st| st.phase = Phase::Draining);
                 Ok(())
             }
+            Some(Command::Pause) => {
+                self.core.pause = true;
+                Ok(())
+            }
+            Some(Command::Resume) => {
+                self.core.pause = false;
+                if let Some(at) = self.paused.take() {
+                    // The snapshot timeout counts reading time: the pause is added back.
+                    let held = at.elapsed();
+                    for started in self.core.snapshot_started.values_mut() {
+                        *started += held;
+                    }
+                    self.core.update(|st| st.phase = Phase::Streaming);
+                    self.issue_reads();
+                }
+                Ok(())
+            }
         }
     }
 
```

Change `crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -335,7 +335,7 @@ impl Session<'_> {
     /// the buffer is full, none for a table an in-flight transaction writes. A table's wait for
     /// such writers warns once, naming them (final review I3); it ends once the table may be read.
     pub(super) fn issue_reads(&mut self) {
-        if !self.queued.is_empty() {
+        if !self.queued.is_empty() || self.core.pause {
             return;
         }
         let now = Instant::now();
```

Change `crates/data-avalanche-pipeline/src/status.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -13,10 +13,17 @@ use crate::Backoff;
 #[derive(Debug, Clone, Copy, PartialEq, Eq)]
 pub enum Command {
     /// Finish the open transaction, flush, make it durable, close the stream and end with
-    /// [`Exit::Drained`] (pause, stop and failover hand-off, Pipeline_Lifecycle §4).
+    /// [`Exit::Drained`] (stop and failover hand-off, Pipeline_Lifecycle §2, §4).
     Drain,
     /// Flush now, whatever the flush policy says.
     Flush,
+    /// Finish the open transaction, flush and make it durable, then hold: nothing more is read
+    /// from the stream, no chunk is read, no rescan runs, and the snapshot timeout's clock
+    /// stops. The source connection stays open (warm) and the WAL-lag guard keeps watching
+    /// (Pipeline_Lifecycle §2 `PAUSED`, Q-M6b-1).
+    Pause,
+    /// Read again after a [`Command::Pause`].
+    Resume,
 }
 
 /// How a worker ended without an error.
@@ -38,6 +45,9 @@ pub enum Phase {
         error: String,
     },
     Draining,
+    /// Everything read is durable and nothing more is read until [`Command::Resume`]; the
+    /// source connection stays open.
+    Paused,
 }
 
 /// A running table snapshot.
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -109,6 +109,7 @@ impl PipelineWorker {
             wal_lag_failing: Streak::default(),
             writers_waited: BTreeSet::new(),
             read_failing: BTreeMap::new(),
+            pause: false,
         };
         (Self { core }, WorkerHandle { control, status })
     }
@@ -193,6 +194,9 @@ async fn drained_while(core: &mut Core, delay: std::time::Duration) -> bool {
             () = &mut sleep => return false,
             cmd = core.control.recv() => match cmd {
                 Some(Command::Flush) => {}
+                // The next session parks (or reads) as it opens.
+                Some(Command::Pause) => core.pause = true,
+                Some(Command::Resume) => core.pause = false,
                 Some(Command::Drain) | None => return true,
             },
         }
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --lib &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --test pg -- --test-threads=1 pause
```

Expected:

PASS:

```text
test result: ok. 180 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.62s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out; finished in 9.02s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): pause and resume keep the source connection warm (Q-M6b-1)"
```


### Task 6: The worker under a lease: gated Phase 0, warnings once, classified errors

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/error.rs`, `crates/data-avalanche-pipeline/src/hooks.rs`, `crates/data-avalanche-pipeline/src/lib.rs`, `crates/data-avalanche-pipeline/src/session.rs`, `crates/data-avalanche-pipeline/src/session/snapshot.rs`, `crates/data-avalanche-pipeline/src/status.rs`, `crates/data-avalanche-pipeline/src/test_support.rs`, `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-state/src/pipeline.rs`

**Interfaces:**
- Consumes: `CommitGuard` (M6a), `WorkerStatus`, `WorkerError`.
- Produces:
  - `Core::may_write()` before every Phase 0 write and `reset_position` (D-M6b.1).
  - `WorkerStatus::warn(String)`, `MAX_WARNINGS = 50`, `WorkerStatus::{warnings_dropped,
    reconnects}`.
  - `pub enum Recovery { Retry, Lease, Error }` and `WorkerError::recovery()` (Q-M6b-3).
  - `PipelineDocs::state() -> Result<Option<Versioned<PipelineStateDoc>>, StateError>`.

Before M6b, `CommitGuard` gated sink commits and the writer's state writes, but Phase 0
wrote unguarded. A node whose lease lapsed during Phase 0 could still record a slot intent, or
reset a lost position over its successor's. `a_lease_lost_before_any_phase_0_write_stops_it_there`
counts the writes for a guard that allows exactly `n` of them, for `n` in 0..7. Mutation checks:
removing the `ensure_table` gate or the `reset_position` gate fails a test.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/error.rs b/crates/data-avalanche-pipeline/src/error.rs
--- a/crates/data-avalanche-pipeline/src/error.rs
+++ b/crates/data-avalanche-pipeline/src/error.rs
@@ -120,4 +120,71 @@ mod tests {
         });
         assert!(matches!(e, WorkerError::Accumulator(_)), "{e}");
     }
+
+    #[test]
+    fn each_error_names_its_recovery() {
+        let store = |e| WorkerError::State(StateError::Store(e));
+        let cases = [
+            (
+                store(StoreError::Transient {
+                    key: "k".into(),
+                    reason: "503".into(),
+                }),
+                Recovery::Retry,
+            ),
+            (
+                WorkerError::Contended {
+                    waited: Duration::from_secs(120),
+                    detail: "55006".into(),
+                },
+                Recovery::Retry,
+            ),
+            (
+                WorkerError::Source(SourceError::Transient("reset".into())),
+                Recovery::Retry,
+            ),
+            (
+                WorkerError::Sink(SinkError::Transient {
+                    reason: "503".into(),
+                }),
+                Recovery::Retry,
+            ),
+            (
+                WorkerError::Fenced {
+                    table: TableRef::new("public", "a"),
+                    current_epoch: 2,
+                },
+                Recovery::Lease,
+            ),
+            (WorkerError::LeaseLost, Recovery::Lease),
+            (WorkerError::Refused("iceberg".into()), Recovery::Error),
+            (
+                WorkerError::Source(SourceError::Permanent("privilege".into())),
+                Recovery::Error,
+            ),
+            (
+                WorkerError::Sink(SinkError::Permanent {
+                    reason: "malformed".into(),
+                }),
+                Recovery::Error,
+            ),
+            (
+                WorkerError::WalLag {
+                    lag: 2048,
+                    limit: 1024,
+                },
+                Recovery::Error,
+            ),
+            (
+                WorkerError::TransactionTooLarge {
+                    bytes: 2048,
+                    limit: 1024,
+                },
+                Recovery::Error,
+            ),
+        ];
+        for (e, want) in cases {
+            assert_eq!(e.recovery(), want, "{e}");
+        }
+    }
 }
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -1212,6 +1212,11 @@ mod tests {
         assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
         assert_eq!(w.ids("a"), [1]);
         assert_eq!(checkpoint(&w).await, lsn_of(&w.source.wal_end()));
+        assert_eq!(
+            w.source.acknowledged().last(),
+            Some(&w.source.wal_end()),
+            "the drain's last flush is acknowledged before the stream closes"
+        );
     }
 
     #[tokio::test]
@@ -1629,7 +1634,8 @@ mod tests {
 
     /// A table whose re_query fetch keeps failing: the streak's first commit tries three times
     /// and warns; the next tries once, with no backoff, and warns nothing more. A fetch that
-    /// succeeds ends the streak: the next failure tries three times and warns again.
+    /// succeeds ends the streak: the next failure tries three times and warns again, a warning
+    /// the status already shows, once.
     #[tokio::test]
     async fn a_failing_re_query_fetch_warns_once_per_streak_and_tries_once_while_failing() {
         let w = World::with(vec![with_v("a")]);
@@ -1673,7 +1679,7 @@ mod tests {
         .await;
         end.unwrap();
         assert!(st.halted.is_empty(), "{:?}", st.halted);
-        assert_eq!(st.warnings.len(), 2, "{:?}", st.warnings);
+        assert_eq!(st.warnings.len(), 1, "{:?}", st.warnings);
         assert_eq!(
             (w.text("a", "doc"), w.text("a", "v")),
             (vec![Some("new".to_owned())], vec![Some("4".to_owned())])
```

Change `crates/data-avalanche-pipeline/src/status.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -151,3 +151,28 @@ impl Default for Tuning {
         }
     }
 }
+#[cfg(test)]
+mod tests {
+    use super::*;
+
+    #[test]
+    fn a_warning_shows_once_as_of_its_latest() {
+        let mut st = WorkerStatus::default();
+        st.warn("a".into());
+        st.warn("b".into());
+        st.warn("a".into());
+        assert_eq!(st.warnings, ["b", "a"]);
+        assert_eq!(st.warnings_dropped, 0);
+    }
+
+    #[test]
+    fn past_the_cap_the_oldest_warning_goes() {
+        let mut st = WorkerStatus::default();
+        for i in 0..MAX_WARNINGS + 2 {
+            st.warn(format!("w{i}"));
+        }
+        assert_eq!(st.warnings.len(), MAX_WARNINGS);
+        assert_eq!(st.warnings.first().map(String::as_str), Some("w2"));
+        assert_eq!(st.warnings_dropped, 2);
+    }
+}
```

Change `crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -293,6 +293,18 @@ impl CommitGuard for Switch {
     }
 }
 
+/// A commit guard that answers yes `n` times, then no: the lease lapses at the `n + 1`th write.
+#[derive(Debug)]
+pub(crate) struct Countdown(pub AtomicUsize);
+
+impl CommitGuard for Countdown {
+    fn may_commit(&self) -> bool {
+        self.0
+            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
+            .is_ok()
+    }
+}
+
 /// Fast retries and ticks for tests.
 pub(crate) fn fast() -> Tuning {
     let ms = |first, max| Backoff {
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -613,8 +613,9 @@ mod tests {
     use data_avalanche_testkit::{FakeTxn, Method};
 
     use super::*;
-    use crate::test_support::{CrashAt, World, Yaml, fast, row, t, wait_until};
+    use crate::test_support::{Countdown, CrashAt, World, Yaml, fast, row, t, wait_until};
     use crate::{Backoff, HookPoint};
+    use std::sync::atomic::AtomicUsize;
     use std::time::Duration;
 
     fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
@@ -835,7 +836,7 @@ mod tests {
     async fn a_transient_failure_reopens_the_stream() {
         let w = World::new(&["a"]);
         w.started(&["a"]).await;
-        let (end, _) = run(&w, &mode("mode: never"), async |status| {
+        let (end, st) = run(&w, &mode("mode: never"), async |status| {
             w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
             streamed(&status, &w).await;
             w.source
@@ -847,6 +848,7 @@ mod tests {
         end.unwrap();
         assert_eq!(w.ids("a"), [1, 2]);
         assert_eq!(w.source.opened().len(), 2);
+        assert_eq!(st.reconnects, 1);
     }
 
     /// A pause that arrives between sessions (the reconnect's backoff) holds the next session as
@@ -1190,6 +1192,72 @@ mod tests {
         assert_eq!(w.ids("a"), [1]);
     }
 
+    /// Phase 0's writes so far on a first start of table `a`, in their order.
+    async fn phase_0_writes(w: &World) -> usize {
+        let slot = w.docs.slot().await.unwrap().map(|v| v.doc);
+        [
+            w.docs.state().await.unwrap().is_some(),
+            slot.is_some(),
+            !w.source.prepares().is_empty(),
+            w.sink.table_state(&t("a")).await.unwrap().exists,
+            w.docs.table_snapshot(&t("a")).await.unwrap().is_some(),
+            w.docs.table_schema(&t("a")).await.unwrap().is_some(),
+            slot.is_some_and(|d| !d.is_intent()),
+        ]
+        .into_iter()
+        .filter(|&written| written)
+        .count()
+    }
+
+    /// A first start writes the pipeline document, the slot intent, the slot (`prepare`), the
+    /// sink table, the snapshot request, the schema and the checkpoint: seven writes, each asked
+    /// of the guard. The lease lapsing before write `n + 1` leaves exactly `n`, and the next start
+    /// completes the pipeline.
+    #[tokio::test]
+    async fn a_lease_lost_before_any_phase_0_write_stops_it_there() {
+        for n in 0..7 {
+            let w = World::new(&["a"]);
+            w.source.load(&t("a"), rows(1..=3));
+            let p = mode("mode: initial");
+            let mut d = deps(&w);
+            d.guard = Arc::new(Countdown(AtomicUsize::new(n)));
+            let (worker, _handle) = PipelineWorker::new(&p, d, fast());
+            let end = tokio::time::timeout(Duration::from_secs(5), worker.run())
+                .await
+                .expect("the lease lapses within Phase 0");
+            assert!(matches!(end, Err(WorkerError::LeaseLost)), "{n}: {end:?}");
+            assert_eq!(phase_0_writes(&w).await, n);
+            let (end, _) = run(&w, &p, async |status| streamed(&status, &w).await).await;
+            assert_eq!(end.unwrap(), Exit::Drained, "{n}");
+            assert_eq!(w.ids("a"), [1, 2, 3], "{n}");
+        }
+    }
+
+    /// A lost stream position is replaced only under the lease: `reset_position` recreates the
+    /// slot, a write like Phase 0's.
+    #[tokio::test]
+    async fn a_lost_position_is_not_replaced_once_the_lease_lapsed() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let guard = Arc::new(crate::test_support::Switch::default());
+        let mut d = deps(&w);
+        d.guard = guard.clone();
+        let end = run_with(&mode("mode: never"), d, async |h| {
+            streamed(&h.status, &w).await;
+            guard.0.store(true, std::sync::atomic::Ordering::SeqCst);
+            w.source.fail(
+                Method::ResetPosition,
+                SourceError::Permanent("reset_position was called".into()),
+            );
+            w.source.lose_slot();
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            let mut st = h.status.clone();
+            let _ = tokio::time::timeout(Duration::from_secs(5), st.wait_for(|_| false)).await;
+        })
+        .await;
+        assert!(matches!(end, Err(WorkerError::LeaseLost)), "{end:?}");
+    }
+
     #[tokio::test]
     async fn a_flush_command_flushes_now() {
         let w = World::new(&["a"]);
```

Change `crates/data-avalanche-state/src/pipeline.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/pipeline.rs b/crates/data-avalanche-state/src/pipeline.rs
--- a/crates/data-avalanche-state/src/pipeline.rs
+++ b/crates/data-avalanche-state/src/pipeline.rs
@@ -294,6 +294,7 @@ mod tests {
             p.set_desired_state(DesiredState::Paused).await.is_err(),
             "not opened yet"
         );
+        assert!(p.state().await.unwrap().is_none(), "nothing created");
         p.open("orders", "delta").await.unwrap();
         for s in [
             DesiredState::Paused,
@@ -301,6 +302,7 @@ mod tests {
             DesiredState::Running,
         ] {
             assert_eq!(p.set_desired_state(s).await.unwrap().doc.desired_state, s);
+            assert_eq!(p.state().await.unwrap().unwrap().doc.desired_state, s);
             assert_eq!(
                 p.open("orders", "delta").await.unwrap().doc.desired_state,
                 s,
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline -p data-avalanche-state --lib
```

Expected:

FAIL to compile (24 errors); the first:

```text
error[E0599]: no method named `state` found for struct `pipeline::PipelineDocs` in the current scope
error[E0425]: cannot find value `MAX_WARNINGS` in this scope
error[E0599]: no method named `state` found for struct `data_avalanche_state::PipelineDocs` in the current scope
error[E0433]: cannot find type `Recovery` in this scope
error[E0599]: no method named `warn` found for struct `status::WorkerStatus` in the current scope
error[E0609]: no field `warnings_dropped` on type `status::WorkerStatus`
… and 18 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-pipeline/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/error.rs b/crates/data-avalanche-pipeline/src/error.rs
--- a/crates/data-avalanche-pipeline/src/error.rs
+++ b/crates/data-avalanche-pipeline/src/error.rs
@@ -4,6 +4,7 @@ use data_avalanche_core::{
     EventShapeError, HaltReason, PositionError, SinkError, SourceError, TableRef,
 };
 use data_avalanche_state::StateError;
+use data_avalanche_storage::StoreError;
 use thiserror::Error;
 
 use crate::hooks::HookPoint;
@@ -67,7 +68,7 @@ pub enum WorkerError {
     /// A newer owner wrote the table; this worker must stop (Clustering §19).
     #[error("table {table} has a newer owner (epoch {current_epoch}): this worker stops")]
     Fenced { table: TableRef, current_epoch: u64 },
-    /// The commit guard refused a commit: the lease is no longer held.
+    /// The commit guard refused a commit or a write: the lease is no longer held.
     #[error("the lease is no longer held: this worker stops before its next commit")]
     LeaseLost,
     /// One transaction outgrew `max_transaction_buffer_bytes` with splitting off
@@ -87,6 +88,41 @@ pub enum WorkerError {
     Crashed(HookPoint),
 }
 
+/// What the pipeline's controller does once its worker ends with an error (Q-M6b-3).
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum Recovery {
+    /// Run the worker again, Phase 0 included, after a backoff: the storage or the source may
+    /// answer next time.
+    Retry,
+    /// The lease is gone, or a newer owner wrote a table: stop and go back to the lease.
+    Lease,
+    /// The pipeline is in `ERROR` until an operator acts (the process restarts, or M9's
+    /// restart): rerunning it would fail the same way.
+    Error,
+}
+
+impl WorkerError {
+    pub fn recovery(&self) -> Recovery {
+        match self {
+            Self::State(StateError::Store(
+                StoreError::Transient { .. } | StoreError::Contention { .. },
+            ))
+            | Self::Contended { .. } => Recovery::Retry,
+            Self::Source(e) if e.is_transient() => Recovery::Retry,
+            Self::Sink(e) if e.is_transient() => Recovery::Retry,
+            Self::Fenced { .. } | Self::LeaseLost => Recovery::Lease,
+            Self::Refused(_)
+            | Self::Source(_)
+            | Self::Sink(_)
+            | Self::State(_)
+            | Self::TransactionTooLarge { .. }
+            | Self::WalLag { .. }
+            | Self::Accumulator(_)
+            | Self::Crashed(_) => Recovery::Error,
+        }
+    }
+}
+
 impl From<AccumulatorError> for WorkerError {
     fn from(e: AccumulatorError) -> Self {
         match e {
```

Change `crates/data-avalanche-pipeline/src/hooks.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/hooks.rs b/crates/data-avalanche-pipeline/src/hooks.rs
--- a/crates/data-avalanche-pipeline/src/hooks.rs
+++ b/crates/data-avalanche-pipeline/src/hooks.rs
@@ -5,9 +5,10 @@ use std::fmt::Debug;
 
 use data_avalanche_core::{SourcePosition, TableRef};
 
-/// Asked before every sink commit. M6b's lease renewer answers `false` once the lease's renewal
-/// safety deadline passed (Clustering §19): the worker then stops with `LeaseLost` and writes
-/// nothing more.
+/// Asked before every sink commit and state write, Phase 0's included (the slot, the sink table,
+/// the state documents). The lease renewer answers `false` once the lease's renewal safety
+/// deadline passed (Clustering §19): the worker then stops with `LeaseLost` and writes nothing
+/// more.
 pub trait CommitGuard: Send + Sync + Debug {
     fn may_commit(&self) -> bool;
 }
```

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -28,7 +28,7 @@ pub use accumulator::{Accumulator, Flush};
 pub use backoff::Backoff;
 pub use batch::{Lineage, MAX_BATCH_BYTES};
 pub use buffer::{CollapsedRow, RowKind, TableBuffer};
-pub use error::{AccumulatorError, WorkerError};
+pub use error::{AccumulatorError, Recovery, WorkerError};
 pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
 pub use policy::{FlushPolicy, FlushReason, TxnLimits};
 pub use status::{Command, Exit, Phase, ReconcileLog, SnapshotProgress, Tuning, WorkerStatus};
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -89,6 +89,16 @@ impl Core {
         self.status.send_modify(f);
     }
 
+    /// Asked before each write Phase 0 makes (state documents, the slot, a sink table), as the
+    /// writer asks before each commit: a worker whose lease lapsed writes nothing more.
+    pub fn may_write(&self) -> Result<(), WorkerError> {
+        if self.guard.may_commit() {
+            Ok(())
+        } else {
+            Err(WorkerError::LeaseLost)
+        }
+    }
+
     pub fn evolver(&self) -> Evolver<'_> {
         Evolver {
             source: &*self.source,
@@ -244,7 +254,7 @@ impl<'c> Session<'c> {
             for h in recorded.iter().chain(&halts) {
                 st.halted.insert(h.table.clone(), h.reason.clone());
             }
-            st.warnings.extend(warnings);
+            warnings.into_iter().for_each(|w| st.warn(w));
             // Replaced, not extended: a snapshot an earlier session finished, or ended with a
             // completion this session's state shows durable, is over.
             st.snapshots = snaps.progress().collect();
@@ -761,7 +771,7 @@ impl<'c> Session<'c> {
     fn held_round_ended(&mut self) {
         for (table, rounds) in self.snaps.held_round() {
             self.core.update(|st| {
-                st.warnings.push(format!(
+                st.warn(format!(
                     "table {table}: its snapshot is still held after {rounds} catalog rescans: its schema changed while the pipeline was down, and its cached schema hasn't caught up with the catalog (the catalog keeps changing, or the stream has yet to commit a change of it); it reads nothing until a rescan finds the cache up to date"
                 ));
             });
@@ -1085,7 +1095,7 @@ impl<'c> Session<'c> {
         if !safe.is_empty() {
             match self.core.evolver().evolve(&table, safe).await? {
                 Ok(None) => {}
-                Ok(Some(warning)) => self.core.update(|st| st.warnings.push(warning)),
+                Ok(Some(warning)) => self.core.update(|st| st.warn(warning)),
                 Err(reason) => {
                     self.halt(table, reason)?;
                     return Ok(None);
```

Change `crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -370,7 +370,7 @@ impl Session<'_> {
                         "table {table}: its snapshot waits for transactions that were in flight when the stream session started and write it (xids {}): it reads nothing until they end; a long-running or idle-in-transaction session holds one (pg_stat_activity, backend_xid), or a prepared transaction (pg_prepared_xacts)",
                         writers.join(", ")
                     );
-                    self.core.update(|st| st.warnings.push(warning));
+                    self.core.update(|st| st.warn(warning));
                 }
                 continue;
             }
```

Change `crates/data-avalanche-pipeline/src/status.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -85,8 +85,34 @@ pub struct WorkerStatus {
     pub snapshots: BTreeMap<TableRef, SnapshotProgress>,
     /// The snapshot reconciler's log, as of the last stream boundary or tick.
     pub reconcile_log: ReconcileLog,
-    /// Warnings since the start: drift, a `never` gap, a degraded evolve.
+    /// Warnings since the start: drift, a `never` gap, a degraded evolve. Each text once, the
+    /// latest [`MAX_WARNINGS`]; add them with [`WorkerStatus::warn`].
     pub warnings: Vec<String>,
+    /// Warnings dropped to keep [`MAX_WARNINGS`].
+    pub warnings_dropped: u64,
+    /// Stream sessions reopened after a source failure (`data_avalanche_source_reconnects_total`).
+    pub reconnects: u64,
+}
+
+/// The warnings a status keeps.
+pub const MAX_WARNINGS: usize = 50;
+
+impl WorkerStatus {
+    /// Adds a warning as the latest. One the status already shows moves to the end instead: a
+    /// rerun of Phase 0, a reconnect or a new failing streak that finds the same thing again adds
+    /// nothing. Past [`MAX_WARNINGS`] the oldest goes.
+    pub fn warn(&mut self, warning: String) {
+        if let Some(at) = self.warnings.iter().position(|w| *w == warning) {
+            let again = self.warnings.remove(at);
+            self.warnings.push(again);
+            return;
+        }
+        self.warnings.push(warning);
+        if self.warnings.len() > MAX_WARNINGS {
+            self.warnings.remove(0);
+            self.warnings_dropped += 1;
+        }
+    }
 }
 
 /// A check that keeps failing warns once: the first failure of a streak adds a status warning,
@@ -105,7 +131,7 @@ impl Streak {
         self.failures = self.failures.saturating_add(1);
         if self.failures == 1 {
             let warning = warning();
-            status.send_modify(|st| st.warnings.push(warning));
+            status.send_modify(|st| st.warn(warning));
         }
     }
 
@@ -151,6 +177,7 @@ impl Default for Tuning {
         }
     }
 }
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -141,6 +141,7 @@ impl PipelineWorker {
             if opened.elapsed() >= core.tuning.source_backoff.max {
                 attempt = 0;
             }
+            core.update(|st| st.reconnects += 1);
             match failed {
                 SourceError::Transient(error) => {
                     contended_since = None;
@@ -158,7 +159,8 @@ impl PipelineWorker {
                     let warning = format!(
                         "the source lost the stream position {position} ({detail}): the stream restarts at a new position and the tables are snapshotted again"
                     );
-                    core.update(|st| st.warnings.push(warning));
+                    core.update(|st| st.warn(warning));
+                    core.may_write()?;
                     let from = retry(&core.tuning, || core.source.reset_position()).await?;
                     start(core, Some(from)).await?;
                     attempt = 0;
@@ -271,6 +273,7 @@ pub(crate) async fn start(
         SinkKind::Delta => "delta",
         SinkKind::Iceberg => "iceberg",
     };
+    core.may_write()?;
     match core.docs.open(&s.pipeline_id, sink_type).await {
         Ok(_) => {}
         Err(e @ (StateError::SinkTypeMismatch { .. } | StateError::PipelineIdMismatch { .. })) => {
@@ -378,9 +381,12 @@ pub(crate) async fn start(
             if slot_doc.is_none()
                 && let Some(name) = &core.settings.slot_intent
             {
+                core.may_write()?;
                 core.docs.save_slot(&SlotDoc::intent(name)).await?;
             }
             let refs: Vec<TableRef> = resolved_tables.keys().cloned().collect();
+            // `prepare` may create, recreate or advance the slot.
+            core.may_write()?;
             let prepared = retry(&tuning, || source.prepare(checkpoint.as_ref(), &refs)).await?;
             warnings.extend(prepared.warnings);
             match prepared.plan {
@@ -515,6 +521,7 @@ pub(crate) async fn start(
             _ => {}
         }
         if !state.exists {
+            core.may_write()?;
             match sink_retry(&tuning, || sink.ensure_table(&spec)).await {
                 Ok(()) => {}
                 Err(SinkError::Permanent { reason }) => {
@@ -536,6 +543,7 @@ pub(crate) async fn start(
 
     // Persist, in this order: a restart in between redoes what follows.
     for (table, reason) in &halts {
+        core.may_write()?;
         core.docs
             .record_halt(&TableHalt {
                 table: table.clone(),
@@ -545,6 +553,7 @@ pub(crate) async fn start(
             .await?;
     }
     for table in &requests {
+        core.may_write()?;
         core.docs
             .save_table_snapshot(&TableSnapshotDoc {
                 table: table.clone(),
@@ -561,6 +570,7 @@ pub(crate) async fn start(
         core.snapshot_started.remove(table);
     }
     for table in &abandoned {
+        core.may_write()?;
         core.docs
             .save_table_snapshot(&TableSnapshotDoc {
                 table: table.clone(),
@@ -591,18 +601,20 @@ pub(crate) async fn start(
     // new checkpoint under the cache of before the baseline, as if the stream still held the
     // change.
     for schema in &schemas {
+        core.may_write()?;
         core.docs.save_table_schema(schema).await?;
     }
     if let Some(doc) = fresh {
         if core.hooks.at(&HookPoint::SchemasCached) == HookAction::Crash {
             return Err(WorkerError::Crashed(HookPoint::SchemasCached));
         }
+        core.may_write()?;
         core.docs.save_slot(&doc).await?;
     }
     core.tracked = tracked;
     core.drifted = drifted;
     core.dump_only = dump_only;
-    core.update(|st| st.warnings.extend(warnings));
+    core.update(|st| warnings.into_iter().for_each(|w| st.warn(w)));
     Ok(())
 }
 
```

Change `crates/data-avalanche-state/src/pipeline.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/pipeline.rs b/crates/data-avalanche-state/src/pipeline.rs
--- a/crates/data-avalanche-state/src/pipeline.rs
+++ b/crates/data-avalanche-state/src/pipeline.rs
@@ -166,6 +166,15 @@ impl PipelineDocs {
         }
     }
 
+    /// The pipeline's state document, read without creating it: `None` before the first start.
+    /// The controller polls it for the desired state (Q-M6b-2).
+    ///
+    /// # Errors
+    /// A storage or decode failure.
+    pub async fn state(&self) -> Result<Option<Versioned<PipelineStateDoc>>, StateError> {
+        self.docs.load(PIPELINE_STATE_KEY).await
+    }
+
     /// Records a pause / resume / stop (Pipeline_Lifecycle §4).
     ///
     /// # Errors
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline -p data-avalanche-state --lib
```

Expected:

PASS:

```text
test result: ok. 185 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.62s
test result: ok. 65 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): Phase 0 writes only under the lease; warnings once, capped; each error names its recovery"
```


### Task 7: Telemetry: the metrics registry and the M6b series

**Files:**
- Create: `crates/data-avalanche-telemetry/Cargo.toml`, `crates/data-avalanche-telemetry/src/lib.rs`, `crates/data-avalanche-telemetry/tests/metrics.rs`
- Modify: `Cargo.toml`, `crates/data-avalanche-core/src/lib.rs`, `crates/data-avalanche-core/src/sink.rs`, `crates/data-avalanche-sink-delta/src/sink.rs`, `crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`

**Interfaces:**
- Consumes: `Sink` (§3.4), `DeltaStats` (M4).
- Produces (`data-avalanche-telemetry`):
  - `Metrics::{new, encode, owned_by(i64), set_cluster_members(usize), pipeline(PipelineLabels,
    per_table) -> PipelineMetrics}`, `CONTENT_TYPE`.
  - `PipelineMetrics::{status, lease, renew_failed, renewed(Duration), wal_lag_bytes,
    snapshot_rows_done, halted_tables, reconcile_log, totals(Totals), worker_ended, remove}`.
  - `PipelineStatus`, `LeaseState`, `RenewFailure`, `PipelineLabels { pipeline, source_host,
    source_db }`, `Totals { source_reconnects, sink_merge_files_rewritten,
    sink_merge_rows_copied }`.
  - In core: `SinkStats { files_rewritten, rows_copied }` and `Sink::stats()` (zeros by
    default). The Delta sink fills it from `DeltaStats`.

Counters are fed by totals: a worker's counts restart at zero when it is rerun, so
`totals` adds the increase since the last report, and a smaller value starts a new worker's run.
`counters_keep_counting_across_workers` pins it. Enum gauges are one-hot. A pipeline the node
no longer owns loses its series.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -25,6 +25,7 @@ data-avalanche-state = { path = "crates/data-avalanche-state" }
 data-avalanche-pipeline = { path = "crates/data-avalanche-pipeline" }
 data-avalanche-sink-delta = { path = "crates/data-avalanche-sink-delta" }
 data-avalanche-source-pg = { path = "crates/data-avalanche-source-pg" }
+data-avalanche-telemetry = { path = "crates/data-avalanche-telemetry" }
 thiserror = "2"
 serde = { version = "1", features = ["derive"] }
 serde_json = { version = "1", features = ["arbitrary_precision"] }
@@ -64,6 +65,8 @@ url = "2"
 pgwire-replication = { version = "=0.4.1", default-features = false, features = ["tls-rustls", "scram", "md5"] }
 tokio-postgres-rustls = "0.14"
 webpki-roots = "1"
+# The binary's control plane (M6b): metrics (roadmap §2.1).
+prometheus-client = "=0.25.1"
 
 # Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
 # CI runners. Workspace crates keep it.
```

Change `crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs b/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
--- a/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/scenarios.rs
@@ -2,7 +2,7 @@
 
 use data_avalanche_core::{
     ColumnSchema, ColumnValueType, CommitOutcome, EvolveOutcome, SchemaChange, SchemaDelta, Sink,
-    TableRef, TableSchema, TableSpec,
+    SinkStats, TableRef, TableSchema, TableSpec,
 };
 use data_avalanche_sink_delta::{DeltaLocation, TableDefaults};
 use deltalake::DeltaTable;
@@ -144,6 +144,14 @@ pub async fn truncate_then_rows(location: DeltaLocation) {
         ),
         (1, 2, 1)
     );
+    // The sink-neutral totals the metrics read.
+    assert_eq!(
+        Sink::stats(&s),
+        SinkStats {
+            files_rewritten: after.files_removed,
+            rows_copied: after.rows_copied,
+        }
+    );
     let mut clear = cs(30, vec![], vec![]);
     clear.truncate = true;
     commit(&s, clear, 1).await;
```

Create `crates/data-avalanche-telemetry/Cargo.toml`:

```toml
[package]
name = "data-avalanche-telemetry"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "Metrics for data-avalanche: the Prometheus registry and the catalogue's series."

[lints]
workspace = true

[dependencies]
prometheus-client.workspace = true
```

Create `crates/data-avalanche-telemetry/tests/metrics.rs`:

```rust
use std::time::Duration;

use data_avalanche_telemetry::{
    LeaseState, Metrics, PipelineLabels, PipelineStatus, RenewFailure, Totals,
};

const L: &str = r#"pipeline="orders",source_host="db1",source_db="shop""#;

fn labels() -> PipelineLabels {
    PipelineLabels {
        pipeline: "orders".into(),
        source_host: "db1".into(),
        source_db: "shop".into(),
    }
}

/// The text's sample lines that start with `name`, sorted.
fn samples(text: &str, name: &str) -> Vec<String> {
    let mut lines: Vec<String> = text
        .lines()
        .filter(|l| !l.starts_with('#') && l.starts_with(name))
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

#[test]
fn the_status_and_the_lease_are_one_hot() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    p.status(PipelineStatus::Running);
    p.status(PipelineStatus::Paused);
    p.lease(LeaseState::Degraded);
    let text = m.encode();
    assert_eq!(
        samples(&text, "data_avalanche_pipeline_status"),
        [
            format!(r#"data_avalanche_pipeline_status{{{L},status="error"}} 0"#),
            format!(r#"data_avalanche_pipeline_status{{{L},status="paused"}} 1"#),
            format!(r#"data_avalanche_pipeline_status{{{L},status="running"}} 0"#),
            format!(r#"data_avalanche_pipeline_status{{{L},status="stopped"}} 0"#),
        ]
    );
    assert_eq!(
        samples(&text, "data_avalanche_lease_state"),
        [
            format!(r#"data_avalanche_lease_state{{{L},state="degraded"}} 1"#),
            format!(r#"data_avalanche_lease_state{{{L},state="held"}} 0"#),
            format!(r#"data_avalanche_lease_state{{{L},state="lost"}} 0"#),
        ]
    );
    assert!(text.ends_with("# EOF\n"), "{text}");
}

#[test]
fn renewals_count_their_failures_and_time_their_successes() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    p.renew_failed(RenewFailure::Transient);
    p.renew_failed(RenewFailure::Transient);
    p.renewed(Duration::from_millis(20));
    let text = m.encode();
    assert_eq!(
        samples(&text, "data_avalanche_lease_renew_failures_total"),
        [format!(
            r#"data_avalanche_lease_renew_failures_total{{{L},class="transient"}} 2"#
        )]
    );
    assert!(
        text.contains(&format!(
            "data_avalanche_lease_renew_latency_seconds_count{{{L}}} 1"
        )),
        "{text}"
    );
}

#[test]
fn counters_keep_counting_across_workers() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    let totals = |n| Totals {
        source_reconnects: n,
        sink_merge_files_rewritten: 10 * n,
        sink_merge_rows_copied: 100 * n,
    };
    p.totals(totals(2));
    p.totals(totals(3));
    p.worker_ended();
    p.totals(totals(1));
    let text = m.encode();
    assert_eq!(
        samples(&text, "data_avalanche_source_reconnects_total"),
        [format!("data_avalanche_source_reconnects_total{{{L}}} 4")]
    );
    assert_eq!(
        samples(&text, "data_avalanche_sink_merge_files_rewritten_total"),
        [format!(
            "data_avalanche_sink_merge_files_rewritten_total{{{L}}} 40"
        )]
    );
    assert_eq!(
        samples(&text, "data_avalanche_sink_merge_rows_copied_total"),
        [format!(
            "data_avalanche_sink_merge_rows_copied_total{{{L}}} 400"
        )]
    );
}

#[test]
fn snapshot_rows_are_summed_unless_per_table() {
    let m = Metrics::new();
    let rows = [("public.a".to_owned(), 5), ("public.b".to_owned(), 7)];
    m.pipeline(labels(), false).snapshot_rows_done(&rows);
    assert_eq!(
        samples(&m.encode(), "data_avalanche_snapshot_rows_done"),
        [format!("data_avalanche_snapshot_rows_done{{{L}}} 12")]
    );

    let m = Metrics::new();
    let p = m.pipeline(labels(), true);
    p.snapshot_rows_done(&rows);
    p.snapshot_rows_done(&rows[1..]);
    assert_eq!(
        samples(&m.encode(), "data_avalanche_snapshot_rows_done"),
        [format!(
            r#"data_avalanche_snapshot_rows_done{{{L},table="public.b"}} 7"#
        )],
        "a finished snapshot loses its series"
    );
}

#[test]
fn a_pipeline_the_node_no_longer_owns_loses_its_series() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), true);
    p.status(PipelineStatus::Running);
    p.lease(LeaseState::Held);
    p.renew_failed(RenewFailure::Conflict);
    p.renewed(Duration::from_millis(5));
    p.wal_lag_bytes(Some(4096));
    p.snapshot_rows_done(&[("public.a".to_owned(), 5)]);
    p.halted_tables(1);
    p.reconcile_log(3, 9);
    p.totals(Totals {
        source_reconnects: 1,
        ..Totals::default()
    });
    m.owned_by(1);
    m.set_cluster_members(1);
    p.remove();
    m.owned_by(-1);
    let text = m.encode();
    assert!(!text.contains("orders"), "{text}");
    assert!(text.contains("data_avalanche_owned_pipelines 0"), "{text}");
    assert!(text.contains("data_avalanche_cluster_members 1"), "{text}");
}

#[test]
fn the_gauges_read_what_was_set_last() {
    let m = Metrics::new();
    let p = m.pipeline(labels(), false);
    p.wal_lag_bytes(Some(4096));
    p.halted_tables(2);
    p.reconcile_log(3, 9);
    let text = m.encode();
    for line in [
        format!("data_avalanche_wal_lag_bytes{{{L}}} 4096"),
        format!("data_avalanche_halted_tables{{{L}}} 2"),
        format!("data_avalanche_reconcile_log_entries{{{L}}} 3"),
        format!("data_avalanche_reconcile_log_keys{{{L}}} 9"),
    ] {
        assert!(text.lines().any(|l| l == line), "{line}\n{text}");
    }
    p.wal_lag_bytes(None);
    assert!(!m.encode().contains("data_avalanche_wal_lag_bytes{"));
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-telemetry -p data-avalanche-sink-delta
```

Expected:

FAIL to compile (1 error); the first:

```text
error[E0432]: unresolved import `data_avalanche_telemetry`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/lib.rs b/crates/data-avalanche-core/src/lib.rs
--- a/crates/data-avalanche-core/src/lib.rs
+++ b/crates/data-avalanche-core/src/lib.rs
@@ -34,7 +34,8 @@ pub use position::{PositionError, SourcePosition};
 pub use schema::{ColumnSchema, HaltReason, TableSchema, canonical_logical, hop2_compatible};
 pub use sink::{
     CommitMarker, CommitOutcome, EvolveOutcome, PartitionField, PartitionTransform, SchemaOp, Sink,
-    SinkCapabilities, SinkError, SinkKind, SinkTableState, TableSpec, TransformKind, WriteMode,
+    SinkCapabilities, SinkError, SinkKind, SinkStats, SinkTableState, TableSpec, TransformKind,
+    WriteMode,
 };
 pub use source::{
     ChangeStream, Chunk, ChunkVisibility, InFlightTxn, KeysetCursor, Preflight, Prepared,
```

Change `crates/data-avalanche-core/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -231,6 +231,17 @@ pub enum EvolveOutcome {
     },
 }
 
+/// What a sink has done since it was built, for the metrics (System-Overview §Observability: the
+/// MERGE cost signals, D-MS.15). A sink that doesn't measure them reports zeros.
+#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
+pub struct SinkStats {
+    /// Files a commit replaced or removed (`data_avalanche_sink_merge_files_rewritten`).
+    pub files_rewritten: u64,
+    /// Rows rewritten unchanged because they share a file with a changed row
+    /// (`data_avalanche_sink_merge_rows_copied`).
+    pub rows_copied: u64,
+}
+
 #[derive(Debug, Clone, PartialEq, Eq)]
 pub enum CommitOutcome {
     Committed {
@@ -280,6 +291,10 @@ impl SinkError {
 pub trait Sink: Send + Sync {
     fn kind(&self) -> SinkKind;
     fn capabilities(&self) -> &SinkCapabilities;
+    /// Totals since the sink was built.
+    fn stats(&self) -> SinkStats {
+        SinkStats::default()
+    }
     async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError>;
     /// Creates the table when it's absent; a present table is left as it is.
     async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError>;
```

Change `crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -7,8 +7,8 @@ use arrow_array::RecordBatch;
 use async_trait::async_trait;
 use data_avalanche_core::{
     CommitMarker, CommitOutcome, EvolveOutcome, SchemaDelta, SchemaOp, Sink, SinkCapabilities,
-    SinkError, SinkKind, SinkTableState, SourcePosition, TableChangeSet, TableRef, TableSpec,
-    TransformKind, WriteMode,
+    SinkError, SinkKind, SinkStats, SinkTableState, SourcePosition, TableChangeSet, TableRef,
+    TableSpec, TransformKind, WriteMode,
 };
 use deltalake::DeltaTableError;
 use deltalake::kernel::Transaction;
@@ -315,6 +315,14 @@ impl Sink for DeltaSink {
         &self.capabilities
     }
 
+    fn stats(&self) -> SinkStats {
+        let s = DeltaSink::stats(self);
+        SinkStats {
+            files_rewritten: s.files_removed,
+            rows_copied: s.rows_copied,
+        }
+    }
+
     async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError> {
         self.with_table(table, async |h| {
             if !h.refresh().await? {
```

Create `crates/data-avalanche-telemetry/src/lib.rs`:

```rust
//! The process's metrics (System-Overview §Observability catalogue): one Prometheus registry, the
//! node's gauges, and a handle per pipeline that sets that pipeline's series. M6b exports what the
//! control plane and the worker know so far; M10 completes the catalogue.
//!
//! Every pipeline series carries `{pipeline, source_host, source_db}`; a `table` label is added
//! only with `sink.metrics.per_table`, and only to `snapshot_rows_done`. Counter families get the
//! OpenMetrics `_total` suffix on their samples (`data_avalanche_source_reconnects_total`).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

/// A series' labels: the pipeline's three, then the family's own.
type Labels = Vec<(&'static str, String)>;

type HistogramFamily = Family<Labels, Histogram, fn() -> Histogram>;

/// The OpenMetrics text content type `/metrics` answers with.
pub const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// `data_avalanche_pipeline_status{status}`: the pipeline's actual state (Pipeline_Lifecycle §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStatus {
    Running,
    Paused,
    Stopped,
    /// The worker ended with an error an operator must act on (Q-M6b-3).
    Error,
}

impl PipelineStatus {
    const ALL: [Self; 4] = [Self::Running, Self::Paused, Self::Stopped, Self::Error];

    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
            Self::Error => "error",
        }
    }
}

/// `data_avalanche_lease_state{state}` (Clustering §19).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    Held,
    Degraded,
    Lost,
}

impl LeaseState {
    const ALL: [Self; 3] = [Self::Held, Self::Degraded, Self::Lost];

    fn label(self) -> &'static str {
        match self {
            Self::Held => "held",
            Self::Degraded => "degraded",
            Self::Lost => "lost",
        }
    }
}

/// `data_avalanche_lease_renew_failures_total{class}` (Clustering §19).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenewFailure {
    /// The store didn't answer: the renewer retries until the deadline.
    Transient,
    /// The lease changed under the renewer.
    Conflict,
    /// The deadline passed.
    Lost,
}

impl RenewFailure {
    const ALL: [Self; 3] = [Self::Transient, Self::Conflict, Self::Lost];

    fn label(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Conflict => "conflict",
            Self::Lost => "lost",
        }
    }
}

/// The labels every series of a pipeline carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineLabels {
    pub pipeline: String,
    pub source_host: String,
    pub source_db: String,
}

impl PipelineLabels {
    fn with(&self, extra: &[(&'static str, &str)]) -> Labels {
        let mut labels = vec![
            ("pipeline", self.pipeline.clone()),
            ("source_host", self.source_host.clone()),
            ("source_db", self.source_db.clone()),
        ];
        labels.extend(extra.iter().map(|(k, v)| (*k, (*v).to_owned())));
        labels
    }
}

/// Monotonic totals a worker reports since it started. A new worker starts them at zero again;
/// the counters they feed don't.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    pub source_reconnects: u64,
    pub sink_merge_files_rewritten: u64,
    pub sink_merge_rows_copied: u64,
}

#[derive(Debug)]
struct Families {
    pipeline_status: Family<Labels, Gauge>,
    lease_state: Family<Labels, Gauge>,
    lease_renew_failures: Family<Labels, Counter>,
    lease_renew_latency: HistogramFamily,
    owned_pipelines: Gauge,
    cluster_members: Gauge,
    wal_lag_bytes: Family<Labels, Gauge>,
    source_reconnects: Family<Labels, Counter>,
    snapshot_rows_done: Family<Labels, Gauge>,
    halted_tables: Family<Labels, Gauge>,
    reconcile_log_entries: Family<Labels, Gauge>,
    reconcile_log_keys: Family<Labels, Gauge>,
    sink_merge_files_rewritten: Family<Labels, Counter>,
    sink_merge_rows_copied: Family<Labels, Counter>,
}

fn renew_latency() -> Histogram {
    // 5 ms … ~10 s.
    Histogram::new(exponential_buckets(0.005, 2.0, 12))
}

/// The process's registry; cheap to clone.
#[derive(Debug, Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    families: Arc<Families>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let families = Families {
            pipeline_status: Family::default(),
            lease_state: Family::default(),
            lease_renew_failures: Family::default(),
            lease_renew_latency: Family::new_with_constructor(renew_latency),
            owned_pipelines: Gauge::default(),
            cluster_members: Gauge::default(),
            wal_lag_bytes: Family::default(),
            source_reconnects: Family::default(),
            snapshot_rows_done: Family::default(),
            halted_tables: Family::default(),
            reconcile_log_entries: Family::default(),
            reconcile_log_keys: Family::default(),
            sink_merge_files_rewritten: Family::default(),
            sink_merge_rows_copied: Family::default(),
        };
        let f = &families;
        let mut r = Registry::with_prefix("data_avalanche");
        r.register(
            "pipeline_status",
            "The pipeline's state: 1 on its current status",
            f.pipeline_status.clone(),
        );
        r.register(
            "lease_state",
            "The pipeline lease's state: 1 on its current state",
            f.lease_state.clone(),
        );
        r.register(
            "lease_renew_failures",
            "Failed lease renewals, by class",
            f.lease_renew_failures.clone(),
        );
        r.register(
            "lease_renew_latency_seconds",
            "How long a lease renewal took",
            f.lease_renew_latency.clone(),
        );
        r.register(
            "owned_pipelines",
            "Pipelines this node holds the lease of",
            f.owned_pipelines.clone(),
        );
        r.register(
            "cluster_members",
            "Members of the cluster this node sees",
            f.cluster_members.clone(),
        );
        r.register(
            "wal_lag_bytes",
            "WAL the pipeline's replication slot retains",
            f.wal_lag_bytes.clone(),
        );
        r.register(
            "source_reconnects",
            "Stream sessions reopened after a source failure",
            f.source_reconnects.clone(),
        );
        r.register(
            "snapshot_rows_done",
            "Rows the running snapshots have emitted",
            f.snapshot_rows_done.clone(),
        );
        r.register(
            "halted_tables",
            "Tables the pipeline has halted",
            f.halted_tables.clone(),
        );
        r.register(
            "reconcile_log_entries",
            "Stream transactions the snapshot reconciler keeps (DA-SNAP-11)",
            f.reconcile_log_entries.clone(),
        );
        r.register(
            "reconcile_log_keys",
            "Keys the snapshot reconciler's transactions wrote (DA-SNAP-11)",
            f.reconcile_log_keys.clone(),
        );
        r.register(
            "sink_merge_files_rewritten",
            "Files a MERGE replaced or a truncate removed",
            f.sink_merge_files_rewritten.clone(),
        );
        r.register(
            "sink_merge_rows_copied",
            "Rows a MERGE rewrote unchanged because they share a file with a changed row",
            f.sink_merge_rows_copied.clone(),
        );
        Self {
            registry: Arc::new(r),
            families: Arc::new(families),
        }
    }

    /// The OpenMetrics text `/metrics` serves ([`CONTENT_TYPE`]).
    pub fn encode(&self) -> String {
        let mut out = String::new();
        encode(&mut out, &self.registry).expect("writing to a String doesn't fail");
        out
    }

    /// One more pipeline owned here (`1`), or one fewer (`-1`).
    pub fn owned_by(&self, delta: i64) {
        self.families.owned_pipelines.inc_by(delta);
    }

    pub fn set_cluster_members(&self, n: usize) {
        self.families.cluster_members.set(gauge(n as u64));
    }

    /// The handle that sets one pipeline's series. `per_table` is `sink.metrics.per_table`.
    pub fn pipeline(&self, labels: PipelineLabels, per_table: bool) -> PipelineMetrics {
        PipelineMetrics {
            families: Arc::clone(&self.families),
            labels,
            per_table,
            seen: Mutex::new(Seen::default()),
        }
    }
}

fn gauge(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[derive(Debug, Default)]
struct Seen {
    totals: Totals,
    tables: BTreeSet<String>,
}

/// One pipeline's series. The controller that owns the pipeline sets them; [`Self::remove`]
/// drops them once the node no longer owns it.
#[derive(Debug)]
pub struct PipelineMetrics {
    families: Arc<Families>,
    labels: PipelineLabels,
    per_table: bool,
    seen: Mutex<Seen>,
}

impl PipelineMetrics {
    fn base(&self) -> Labels {
        self.labels.with(&[])
    }

    pub fn status(&self, status: PipelineStatus) {
        for s in PipelineStatus::ALL {
            let labels = self.labels.with(&[("status", s.label())]);
            self.families
                .pipeline_status
                .get_or_create(&labels)
                .set(i64::from(s == status));
        }
    }

    pub fn lease(&self, state: LeaseState) {
        for s in LeaseState::ALL {
            let labels = self.labels.with(&[("state", s.label())]);
            self.families
                .lease_state
                .get_or_create(&labels)
                .set(i64::from(s == state));
        }
    }

    pub fn renew_failed(&self, class: RenewFailure) {
        let labels = self.labels.with(&[("class", class.label())]);
        self.families
            .lease_renew_failures
            .get_or_create(&labels)
            .inc();
    }

    pub fn renewed(&self, took: Duration) {
        self.families
            .lease_renew_latency
            .get_or_create(&self.base())
            .observe(took.as_secs_f64());
    }

    /// `None` drops the series: the source doesn't report it, or its read failed.
    pub fn wal_lag_bytes(&self, bytes: Option<u64>) {
        match bytes {
            Some(b) => {
                self.families
                    .wal_lag_bytes
                    .get_or_create(&self.base())
                    .set(gauge(b));
            }
            None => {
                self.families.wal_lag_bytes.remove(&self.base());
            }
        }
    }

    /// The running snapshots' rows, per table: one series per table with `per_table`, else their
    /// sum. A table whose snapshot ended loses its series.
    pub fn snapshot_rows_done(&self, rows: &[(String, u64)]) {
        let family = &self.families.snapshot_rows_done;
        if !self.per_table {
            let sum = rows.iter().map(|(_, n)| *n).sum();
            family.get_or_create(&self.base()).set(gauge(sum));
            return;
        }
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let now: BTreeSet<String> = rows.iter().map(|(t, _)| t.clone()).collect();
        for gone in seen.tables.difference(&now) {
            family.remove(&self.labels.with(&[("table", gone)]));
        }
        for (table, n) in rows {
            family
                .get_or_create(&self.labels.with(&[("table", table)]))
                .set(gauge(*n));
        }
        seen.tables = now;
    }

    pub fn halted_tables(&self, n: usize) {
        self.families
            .halted_tables
            .get_or_create(&self.base())
            .set(gauge(n as u64));
    }

    pub fn reconcile_log(&self, entries: usize, keys: usize) {
        let f = &self.families;
        f.reconcile_log_entries
            .get_or_create(&self.base())
            .set(gauge(entries as u64));
        f.reconcile_log_keys
            .get_or_create(&self.base())
            .set(gauge(keys as u64));
    }

    /// The running worker's totals. A total below the last one seen is a new worker's: it counts
    /// from zero.
    pub fn totals(&self, now: Totals) {
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let last = seen.totals;
        let f = &self.families;
        let base = self.base();
        let add = |counter: &Family<Labels, Counter>, now: u64, last: u64| {
            let by = if now >= last { now - last } else { now };
            counter.get_or_create(&base).inc_by(by);
        };
        add(
            &f.source_reconnects,
            now.source_reconnects,
            last.source_reconnects,
        );
        add(
            &f.sink_merge_files_rewritten,
            now.sink_merge_files_rewritten,
            last.sink_merge_files_rewritten,
        );
        add(
            &f.sink_merge_rows_copied,
            now.sink_merge_rows_copied,
            last.sink_merge_rows_copied,
        );
        seen.totals = now;
    }

    /// The worker that reported the totals so far ended: the next one counts from zero.
    pub fn worker_ended(&self) {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .totals = Totals::default();
    }

    /// Drops every series of the pipeline: the node no longer owns it.
    pub fn remove(&self) {
        let f = &self.families;
        let base = self.base();
        for s in PipelineStatus::ALL {
            f.pipeline_status
                .remove(&self.labels.with(&[("status", s.label())]));
        }
        for s in LeaseState::ALL {
            f.lease_state
                .remove(&self.labels.with(&[("state", s.label())]));
        }
        for c in RenewFailure::ALL {
            f.lease_renew_failures
                .remove(&self.labels.with(&[("class", c.label())]));
        }
        f.lease_renew_latency.remove(&base);
        for family in [
            &f.wal_lag_bytes,
            &f.halted_tables,
            &f.reconcile_log_entries,
            &f.reconcile_log_keys,
            &f.snapshot_rows_done,
        ] {
            family.remove(&base);
        }
        for family in [
            &f.source_reconnects,
            &f.sink_merge_files_rewritten,
            &f.sink_merge_rows_copied,
        ] {
            family.remove(&base);
        }
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        for table in std::mem::take(&mut seen.tables) {
            f.snapshot_rows_done
                .remove(&self.labels.with(&[("table", &table)]));
        }
        seen.totals = Totals::default();
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-telemetry -p data-avalanche-sink-delta
```

Expected:

PASS:

```text
test result: ok. 69 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.01s
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.40s
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(telemetry): the metrics registry and the M6b series; a sink reports its MERGE cost"
```


### Task 8: Cluster: the lease renewer and its commit guard

**Files:**
- Create: `crates/data-avalanche-cluster/Cargo.toml`, `crates/data-avalanche-cluster/src/lease.rs`, `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-cluster/src/test_support.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: `Leases` (`renew`, `observe`), `LeaseDoc`, `Holder` (state, M2), `CommitGuard`
  (M6a), `PipelineMetrics` (Task 7).
- Produces (`data-avalanche-cluster::lease`):
  - `LeaseTiming { duration, renewal_interval, safety_margin }` with `new(&Cluster)` and
    `deadline(renewed: Instant) -> Instant`.
  - `RenewState { Held, Degraded, Lost }`.
  - `LeaseGuard` (`state()`, `async lost()`, `impl CommitGuard`).
  - `LeaseRun { guard, owner_epoch, … }` with `start(leases, lease, holder, timing, written:
    Instant, metrics) -> Self` and `async stop() -> RenewEnd`.
  - `RenewEnd { Stopped(Versioned<LeaseDoc>), Lost { reason } }`.

Clustering §19, D-M6b.4. The deadline makes a slow store safe: each write is bounded by
it, and the guard refuses past it on its own, even when the renewer task never runs again. All
tests run on the paused clock against `ScriptedStore`, a store whose writes can fail
transiently, conflict, fail permanently, hang, or land behind an error.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -26,6 +26,7 @@ data-avalanche-pipeline = { path = "crates/data-avalanche-pipeline" }
 data-avalanche-sink-delta = { path = "crates/data-avalanche-sink-delta" }
 data-avalanche-source-pg = { path = "crates/data-avalanche-source-pg" }
 data-avalanche-telemetry = { path = "crates/data-avalanche-telemetry" }
+data-avalanche-cluster = { path = "crates/data-avalanche-cluster" }
 thiserror = "2"
 serde = { version = "1", features = ["derive"] }
 serde_json = { version = "1", features = ["arbitrary_precision"] }
```

Create `crates/data-avalanche-cluster/Cargo.toml`:

```toml
[package]
name = "data-avalanche-cluster"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "Pipeline ownership for data-avalanche: the lease and its renewer, the pipeline controller and the cluster reconciler."

[lints]
workspace = true

[dependencies]
chrono.workspace = true
data-avalanche-config.workspace = true
data-avalanche-pipeline.workspace = true
data-avalanche-state.workspace = true
data-avalanche-storage.workspace = true
data-avalanche-telemetry.workspace = true
tokio = { workspace = true, features = ["sync"] }

[dev-dependencies]
async-trait.workspace = true
bytes.workspace = true
object_store.workspace = true
tokio = { workspace = true, features = ["test-util"] }
```

Create `crates/data-avalanche-cluster/src/lease.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        Fault, Kind, ScriptedStore, acquired, holder, leases, metrics, timing,
    };

    async fn advance(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    fn start(
        store: &Arc<ScriptedStore>,
        lease: Versioned<LeaseDoc>,
    ) -> (LeaseRun, data_avalanche_telemetry::Metrics) {
        let (m, p) = metrics();
        let run = LeaseRun::start(
            leases(store),
            lease,
            holder("a"),
            timing(),
            Instant::now(),
            p,
        );
        (run, m)
    }

    #[tokio::test(start_paused = true)]
    async fn the_guard_refuses_past_its_deadline_on_its_own() {
        let guard = LeaseGuard::new(Instant::now() + Duration::from_secs(1));
        assert!(guard.may_commit());
        advance(1_000).await;
        assert_eq!(guard.state(), RenewState::Held, "no renewer runs");
        assert!(!guard.may_commit());
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_lease_is_renewed_every_interval() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        let first = lease.version.clone();
        let (run, m) = start(&store, lease);
        advance(16_000).await;
        assert_eq!(run.guard.state(), RenewState::Held);
        assert!(run.guard.may_commit());
        assert!(
            m.encode()
                .contains("data_avalanche_lease_renew_latency_seconds_count{pipeline=\"p\",source_host=\"h\",source_db=\"d\"} 3"),
            "{}",
            m.encode()
        );
        let RenewEnd::Stopped(last) = run.stop().await else {
            panic!("stopped")
        };
        let stored = leases(&store).observe("p").await.unwrap().unwrap();
        assert_ne!(stored.version, first);
        assert_eq!(stored, last, "the stop hands back the lease as stored");
        leases(&store).release(&last, Utc::now()).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_renewal_degrades_and_a_retry_restores_it() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Transient,
            lands: false,
        });
        let (run, m) = start(&store, lease);
        advance(5_010).await;
        assert_eq!(run.guard.state(), RenewState::Degraded);
        assert!(run.guard.may_commit(), "commits go on until the deadline");
        advance(1_000).await;
        assert_eq!(run.guard.state(), RenewState::Held);
        let text = m.encode();
        assert!(text.contains("class=\"transient\"} 1"), "{text}");
    }

    #[tokio::test(start_paused = true)]
    async fn renewals_that_keep_failing_lose_the_lease_at_the_deadline() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_always(Some(Fault {
            kind: Kind::Transient,
            lands: false,
        }));
        let (run, _m) = start(&store, lease);
        advance(12_740).await;
        assert_eq!(run.guard.state(), RenewState::Degraded);
        assert!(run.guard.may_commit());
        advance(20).await;
        assert!(!run.guard.may_commit(), "the deadline is 12.75 s");
        run.guard.lost().await;
        assert!(matches!(run.stop().await, RenewEnd::Lost { .. }));
    }

    /// The guard checks the deadline itself: a renewer stuck in a write doesn't extend it.
    #[tokio::test(start_paused = true)]
    async fn a_renewal_that_never_answers_loses_the_lease_at_the_deadline() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Hang,
            lands: false,
        });
        let (run, _m) = start(&store, lease);
        advance(12_740).await;
        assert!(run.guard.may_commit());
        advance(20).await;
        assert!(!run.guard.may_commit());
        run.guard.lost().await;
        match run.stop().await {
            RenewEnd::Lost { reason } => assert!(reason.contains("unanswered"), "{reason}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_taken_by_another_holder_is_lost_at_the_next_renewal() {
        let store = ScriptedStore::new();
        let ls = leases(&store);
        let lease = acquired(&ls, "a").await;
        let (run, _m) = start(&store, lease);
        let seen = ls.observe("p").await.unwrap().unwrap();
        ls.take_over(&seen, &holder("b"), timing().duration, Utc::now())
            .await
            .unwrap();
        advance(5_010).await;
        assert_eq!(run.guard.state(), RenewState::Lost);
        assert!(!run.guard.may_commit());
        match run.stop().await {
            RenewEnd::Lost { reason } => assert!(reason.contains("taken"), "{reason}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_renewal_that_landed_behind_an_error_is_still_held() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Contention,
            lands: true,
        });
        let (run, m) = start(&store, lease);
        advance(5_010).await;
        assert_eq!(run.guard.state(), RenewState::Degraded);
        advance(1_000).await;
        assert_eq!(run.guard.state(), RenewState::Held);
        assert!(m.encode().contains("class=\"conflict\"} 1"));
        advance(10_000).await;
        assert_eq!(run.guard.state(), RenewState::Held, "renewing on");
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_loses_the_lease_at_once() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Permanent,
            lands: false,
        });
        let (run, m) = start(&store, lease);
        advance(5_010).await;
        assert_eq!(run.guard.state(), RenewState::Lost);
        assert!(m.encode().contains("class=\"lost\"} 1"));
    }
}
```

Create `crates/data-avalanche-cluster/src/lib.rs`:

```rust
pub mod lease;
#[cfg(test)]
mod test_support;
```

Create `crates/data-avalanche-cluster/src/test_support.rs`:

```rust
//! A store whose writes fail on cue, and the lease fixtures the tests share.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use data_avalanche_state::{Acquired, DocStore, Holder, LeaseDoc, Leases, Versioned};
use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
use data_avalanche_telemetry::{Metrics, PipelineLabels, PipelineMetrics};
use object_store::memory::InMemory;

use crate::LeaseTiming;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Transient,
    Contention,
    Permanent,
    /// The write never answers.
    Hang,
}

/// A failed write. `lands`: the write goes through, then the error comes back (an unknown
/// outcome, as S3 409 or a timeout after the PUT).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Fault {
    pub kind: Kind,
    pub lands: bool,
}

impl Fault {
    fn error(self, key: &str) -> StoreError {
        let key = key.to_owned();
        match self.kind {
            Kind::Transient => StoreError::Transient {
                key,
                reason: "injected".into(),
            },
            Kind::Contention => StoreError::Contention { key },
            Kind::Permanent | Kind::Hang => StoreError::Permanent {
                key,
                reason: "injected".into(),
            },
        }
    }
}

#[derive(Debug, Default)]
struct Script {
    /// The next writes' faults, in order.
    next: VecDeque<Fault>,
    /// Every write fails so, once `next` is empty.
    always: Option<Fault>,
}

/// An in-memory store whose writes (`create`, `update`, `put`) fail as scripted.
#[derive(Debug)]
pub(crate) struct ScriptedStore {
    inner: ObjectStoreVersioned,
    script: Mutex<Script>,
}

impl ScriptedStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals")
                .expect("a valid root"),
            script: Mutex::new(Script::default()),
        })
    }

    pub fn fail_next(&self, fault: Fault) {
        self.lock().next.push_back(fault);
    }

    pub fn fail_always(&self, fault: Option<Fault>) {
        self.lock().always = fault;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Script> {
        self.script
            .lock()
            .expect("no test panics while holding the lock")
    }

    fn fault(&self) -> Option<Fault> {
        let mut s = self.lock();
        s.next.pop_front().or(s.always)
    }

    async fn write<F>(&self, key: &str, write: F) -> Result<ObjectVersion, StoreError>
    where
        F: Future<Output = Result<ObjectVersion, StoreError>>,
    {
        match self.fault() {
            None => write.await,
            Some(Fault {
                kind: Kind::Hang, ..
            }) => std::future::pending().await,
            Some(f) if f.lands => {
                write.await?;
                Err(f.error(key))
            }
            Some(f) => Err(f.error(key)),
        }
    }
}

#[async_trait]
impl VersionedStore for ScriptedStore {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
        self.inner.get(key).await
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.write(key, self.inner.create(key, body)).await
    }

    async fn update(
        &self,
        key: &str,
        expected: &ObjectVersion,
        body: Bytes,
    ) -> Result<ObjectVersion, StoreError> {
        self.write(key, self.inner.update(key, expected, body))
            .await
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.write(key, self.inner.put(key, body)).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix).await
    }
}

/// Balanced defaults (§18): 15 s, renewed every 5 s, deadline at 12.75 s.
pub(crate) fn timing() -> LeaseTiming {
    LeaseTiming {
        duration: Duration::from_secs(15),
        renewal_interval: Duration::from_secs(5),
        safety_margin: 0.15,
    }
}

pub(crate) fn holder(node: &str) -> Holder {
    Holder {
        node_id: node.to_owned(),
        incarnation: 1,
    }
}

pub(crate) fn leases(store: &Arc<ScriptedStore>) -> Leases {
    Leases::new(DocStore::new(Arc::clone(store) as Arc<dyn VersionedStore>))
}

pub(crate) async fn acquired(leases: &Leases, node: &str) -> Versioned<LeaseDoc> {
    match leases
        .acquire("p", &holder(node), timing().duration, Utc::now())
        .await
        .expect("acquired")
    {
        Acquired::Mine(lease) => lease,
        Acquired::HeldBy(other) => panic!("held by {other:?}"),
    }
}

pub(crate) fn metrics() -> (Metrics, Arc<PipelineMetrics>) {
    let m = Metrics::new();
    let p = m.pipeline(
        PipelineLabels {
            pipeline: "p".into(),
            source_host: "h".into(),
            source_db: "d".into(),
        },
        false,
    );
    (m, Arc::new(p))
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-cluster
```

Expected:

FAIL to compile (27 errors); the first:

```text
error[E0432]: unresolved import `crate::LeaseTiming`
error[E0433]: cannot find type `Duration` in this scope
error[E0425]: cannot find type `Arc` in this scope
error[E0425]: cannot find type `Versioned` in this scope
error[E0425]: cannot find type `LeaseDoc` in this scope
error[E0425]: cannot find type `LeaseRun` in this scope
… and 21 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/lease.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lease.rs b/crates/data-avalanche-cluster/src/lease.rs
--- a/crates/data-avalanche-cluster/src/lease.rs
+++ b/crates/data-avalanche-cluster/src/lease.rs
@@ -1,3 +1,313 @@
+//! A held lease and its renewer (Clustering §19). The renewer CAS-writes the lease every
+//! `lease_renewal_interval`; a write it can't get through degrades it, and it is lost once the
+//! safety deadline passes without one, or at once when the lease was taken or deleted. The
+//! [`LeaseGuard`] answers the worker's [`CommitGuard`] from the renewer's state and deadline.
+
+use std::sync::Arc;
+use std::time::Duration;
+
+use chrono::Utc;
+use data_avalanche_config::Cluster;
+use data_avalanche_pipeline::{Backoff, CommitGuard};
+use data_avalanche_state::{Holder, LeaseDoc, Leases, StateError, Versioned};
+use data_avalanche_storage::StoreError;
+use data_avalanche_telemetry::{LeaseState, PipelineMetrics, RenewFailure};
+use tokio::sync::{oneshot, watch};
+use tokio::task::JoinHandle;
+use tokio::time::Instant;
+
+/// The lease's timings (`globals.cluster`, Clustering §18–§19).
+#[derive(Debug, Clone, Copy, PartialEq)]
+pub struct LeaseTiming {
+    pub duration: Duration,
+    pub renewal_interval: Duration,
+    /// The fraction of `duration` kept as the guard before the safety deadline.
+    pub safety_margin: f64,
+}
+
+impl LeaseTiming {
+    pub fn new(cluster: &Cluster) -> Self {
+        Self {
+            duration: cluster.lease_duration,
+            renewal_interval: cluster.lease_renewal_interval,
+            safety_margin: cluster.lease_safety_margin,
+        }
+    }
+
+    /// `last_renew_local + lease_duration − guard`, `guard = lease_duration × lease_safety_margin`
+    /// (§19.3).
+    pub fn deadline(&self, renewed: Instant) -> Instant {
+        renewed + self.duration - self.duration.mul_f64(self.safety_margin)
+    }
+
+    /// The degraded renewer's retries: several fit in one renewal interval (§19.4).
+    fn retry(&self) -> Backoff {
+        Backoff {
+            first: Duration::from_millis(100).min(self.renewal_interval / 8),
+            max: self.renewal_interval / 4,
+        }
+    }
+}
+
+/// The renewer's state (§19.4); `Renewing` is the write in flight, not a state the guard sees.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum RenewState {
+    Held,
+    /// A renewal failed and is being retried; commits go on until the deadline.
+    Degraded,
+    /// Terminal for this ownership: nothing more is committed.
+    Lost,
+}
+
+#[derive(Debug, Clone, Copy)]
+struct Proof {
+    state: RenewState,
+    deadline: Instant,
+}
+
+/// The worker's [`CommitGuard`] over a held lease: `may_commit` = state ∈ {Held, Degraded} ∧
+/// now < deadline (§19.5), on the local clock.
+#[derive(Debug)]
+pub struct LeaseGuard {
+    proof: watch::Sender<Proof>,
+}
+
+impl LeaseGuard {
+    fn new(deadline: Instant) -> Self {
+        Self {
+            proof: watch::Sender::new(Proof {
+                state: RenewState::Held,
+                deadline,
+            }),
+        }
+    }
+
+    pub fn state(&self) -> RenewState {
+        self.proof.borrow().state
+    }
+
+    /// Waits until the renewer has lost the lease.
+    pub async fn lost(&self) {
+        let mut rx = self.proof.subscribe();
+        // The sender lives in `self`: the wait ends only with a state.
+        let _ = rx.wait_for(|p| p.state == RenewState::Lost).await;
+    }
+
+    fn set(&self, state: RenewState, deadline: Option<Instant>) {
+        self.proof.send_modify(|p| {
+            p.state = state;
+            if let Some(d) = deadline {
+                p.deadline = d;
+            }
+        });
+    }
+}
+
+impl CommitGuard for LeaseGuard {
+    fn may_commit(&self) -> bool {
+        let p = *self.proof.borrow();
+        p.state != RenewState::Lost && Instant::now() < p.deadline
+    }
+}
+
+/// How a renewer ended.
+#[derive(Debug)]
+pub enum RenewEnd {
+    /// Asked to stop: the lease as last written, for a release.
+    Stopped(Versioned<LeaseDoc>),
+    /// The lease is gone; nothing more may be committed.
+    Lost { reason: String },
+}
+
+/// A held lease with its renewer running.
+#[derive(Debug)]
+pub struct LeaseRun {
+    pub guard: Arc<LeaseGuard>,
+    /// The lease's `owner_epoch`: every commit carries it.
+    pub owner_epoch: u64,
+    stop: Option<oneshot::Sender<()>>,
+    task: JoinHandle<RenewEnd>,
+}
+
+impl LeaseRun {
+    /// Starts renewing `lease`, which this holder wrote at `written` (local clock, taken before
+    /// the write was sent).
+    pub fn start(
+        leases: Leases,
+        lease: Versioned<LeaseDoc>,
+        holder: Holder,
+        timing: LeaseTiming,
+        written: Instant,
+        metrics: Arc<PipelineMetrics>,
+    ) -> Self {
+        let guard = Arc::new(LeaseGuard::new(timing.deadline(written)));
+        metrics.lease(LeaseState::Held);
+        let owner_epoch = lease.doc.owner_epoch;
+        let (stop, stopped) = oneshot::channel();
+        let renewer = Renewer {
+            leases,
+            lease,
+            holder,
+            timing,
+            renewed: written,
+            guard: Arc::clone(&guard),
+            metrics,
+        };
+        Self {
+            guard,
+            owner_epoch,
+            stop: Some(stop),
+            task: tokio::spawn(renewer.run(stopped)),
+        }
+    }
+
+    /// Stops renewing; the lease as last written comes back unless it was lost meanwhile.
+    pub async fn stop(mut self) -> RenewEnd {
+        if let Some(stop) = self.stop.take() {
+            let _ = stop.send(());
+        }
+        match (&mut self.task).await {
+            Ok(end) => end,
+            Err(e) => RenewEnd::Lost {
+                reason: format!("the lease renewer ended abnormally: {e}"),
+            },
+        }
+    }
+}
+
+impl Drop for LeaseRun {
+    fn drop(&mut self) {
+        self.task.abort();
+    }
+}
+
+struct Renewer {
+    leases: Leases,
+    lease: Versioned<LeaseDoc>,
+    holder: Holder,
+    timing: LeaseTiming,
+    /// When the last successful write was sent.
+    renewed: Instant,
+    guard: Arc<LeaseGuard>,
+    metrics: Arc<PipelineMetrics>,
+}
+
+impl Renewer {
+    async fn run(mut self, mut stop: oneshot::Receiver<()>) -> RenewEnd {
+        let retry = self.timing.retry();
+        let mut failures: u32 = 0;
+        // The last write's outcome is unknown (contention, a transient error): it may have
+        // landed, so the lease is read before the next write.
+        let mut unknown = false;
+        let mut next = self.renewed + self.timing.renewal_interval;
+        loop {
+            let deadline = self.timing.deadline(self.renewed);
+            tokio::select! {
+                _ = &mut stop => return RenewEnd::Stopped(self.lease),
+                () = tokio::time::sleep_until(next.min(deadline)) => {}
+            }
+            if Instant::now() >= deadline {
+                let budget = deadline - self.renewed;
+                return self.lost(format!(
+                    "no renewal got through within the safety deadline ({budget:?} after the last one)"
+                ));
+            }
+            let sent = Instant::now();
+            // Bounded by the deadline: a write that hangs past it can't prove the lease anyway.
+            let attempt = tokio::time::timeout_at(deadline, async {
+                self.observe_if(unknown).await?;
+                self.leases
+                    .renew(&self.lease, Utc::now())
+                    .await
+                    .map_err(Failure::from)
+            })
+            .await;
+            let Ok(outcome) = attempt else {
+                return self.lost("the renewal was still unanswered at the safety deadline".into());
+            };
+            match outcome {
+                Ok(lease) => {
+                    self.lease = lease;
+                    self.renewed = sent;
+                    failures = 0;
+                    unknown = false;
+                    self.guard
+                        .set(RenewState::Held, Some(self.timing.deadline(sent)));
+                    self.metrics.lease(LeaseState::Held);
+                    self.metrics.renewed(sent.elapsed());
+                    next = sent + self.timing.renewal_interval;
+                }
+                Err(Failure::Retry { class }) => {
+                    unknown = true;
+                    self.metrics.renew_failed(class);
+                    self.guard.set(RenewState::Degraded, None);
+                    self.metrics.lease(LeaseState::Degraded);
+                    next = Instant::now() + retry.delay(failures);
+                    failures = failures.saturating_add(1);
+                }
+                Err(Failure::Lost(reason)) => return self.lost(reason),
+            }
+        }
+    }
+
+    /// After a write whose outcome is unknown: the lease is still this holder's when it names
+    /// this holder with the same epoch (a takeover changes both); it continues from what is
+    /// stored.
+    async fn observe_if(&mut self, unknown: bool) -> Result<(), Failure> {
+        if !unknown {
+            return Ok(());
+        }
+        match self.leases.observe(&self.lease.doc.pipeline_id).await {
+            Ok(Some(seen))
+                if seen.doc.is_held_by(&self.holder)
+                    && seen.doc.owner_epoch == self.lease.doc.owner_epoch =>
+            {
+                self.lease = seen;
+                Ok(())
+            }
+            Ok(Some(_)) => Err(Failure::Lost(
+                "the lease was taken by another holder".into(),
+            )),
+            Ok(None) => Err(Failure::Lost("the lease was deleted".into())),
+            // A failed read leaves the write before it unknown: the next attempt reads again.
+            Err(e) => Err(Failure::from(e)),
+        }
+    }
+
+    fn lost(self, reason: String) -> RenewEnd {
+        self.guard.set(RenewState::Lost, None);
+        self.metrics.renew_failed(RenewFailure::Lost);
+        self.metrics.lease(LeaseState::Lost);
+        RenewEnd::Lost { reason }
+    }
+}
+
+/// A failed renewal (§19.2).
+enum Failure {
+    /// Status unknown: retried until the deadline, reading the lease first (the write may have
+    /// gone through).
+    Retry { class: RenewFailure },
+    /// Taken (412), deleted (404) or a permanent fault (403): no retry.
+    Lost(String),
+}
+
+impl From<StateError> for Failure {
+    fn from(e: StateError) -> Self {
+        match e {
+            StateError::Store(StoreError::Transient { .. }) => Self::Retry {
+                class: RenewFailure::Transient,
+            },
+            StateError::Store(StoreError::Contention { .. }) => Self::Retry {
+                class: RenewFailure::Conflict,
+            },
+            StateError::Store(StoreError::Conflict { .. }) => {
+                Self::Lost("the lease was taken or deleted".into())
+            }
+            e => Self::Lost(format!("the lease can't be renewed: {e}")),
+        }
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -1,3 +1,10 @@
+//! Who runs each pipeline (Clustering §19–§20): the lease, its renewer and the commit guard it
+//! answers, the controller that runs a pipeline's worker while it holds the lease, and the
+//! reconciler that tells each controller whether to own. M6b runs them single-node: a self-only
+//! ring, so every pipeline's target is "own".
+
 pub mod lease;
 #[cfg(test)]
 mod test_support;
+
+pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-cluster
```

Expected:

PASS:

```text
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(cluster): the lease renewer and the commit guard it answers"
```


### Task 9: Cluster: acquire, take over, release

**Files:**
- Create: `crates/data-avalanche-cluster/src/claim.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`

**Interfaces:**
- Consumes: `Leases` (`acquire`, `observe`, `takeover`, `release`), `LeaseTiming` (Task 8).
- Produces: `Claim::new(leases, pipeline_id, holder, timing)`; `Claim::step() ->
  Result<Claimed, StateError>` with `Claimed::{Mine { lease, written: Instant }, HeldBy {
  node_id, takeover_at: Instant }}`; `release(&Leases, &Versioned<LeaseDoc>) ->
  Result<(), StateError>`.

Clustering §5.3, D-M6b.5. A takeover needs a full `lease_duration` without a renewal,
timed from when the read returned, so a renewal that lands during the read counts. `written` is
taken before the write is sent: the renewer's first deadline can't be later than the lease
allows.

- [ ] **Step 1: Write the failing tests**

Create `crates/data-avalanche-cluster/src/claim.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::LeaseRun;
    use crate::test_support::{
        Fault, Kind, ScriptedStore, acquired, holder, leases, metrics, timing,
    };

    fn claim(store: &std::sync::Arc<ScriptedStore>, node: &str) -> Claim {
        Claim::new(leases(store), "p", holder(node), timing())
    }

    async fn advance(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    fn epoch(c: &Claimed) -> u64 {
        match c {
            Claimed::Mine { lease, .. } => lease.doc.owner_epoch,
            Claimed::HeldBy { .. } => panic!("{c:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_free_lease_is_acquired_at_once() {
        let store = ScriptedStore::new();
        assert_eq!(epoch(&claim(&store, "b").step().await.unwrap()), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_released_lease_is_acquired_at_once_with_the_next_epoch() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        release(&leases(&store), &lease).await.unwrap();
        assert_eq!(epoch(&claim(&store, "b").step().await.unwrap()), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_that_renews_keeps_its_lease() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        let (_m, p) = metrics();
        let _run = LeaseRun::start(
            leases(&store),
            lease,
            holder("a"),
            timing(),
            Instant::now(),
            p,
        );
        let mut b = claim(&store, "b");
        for _ in 0..40 {
            match b.step().await.unwrap() {
                Claimed::HeldBy { node_id, .. } => assert_eq!(node_id, "a"),
                mine => panic!("took a live lease: {mine:?}"),
            }
            advance(1_000).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_holders_lease_is_taken_over_after_a_full_lease_duration() {
        let store = ScriptedStore::new();
        acquired(&leases(&store), "a").await;
        let mut b = claim(&store, "b");
        let start = Instant::now();
        let Claimed::HeldBy { takeover_at, .. } = b.step().await.unwrap() else {
            panic!("held by a")
        };
        assert_eq!(takeover_at, start + timing().duration);
        advance(14_990).await;
        assert!(matches!(b.step().await.unwrap(), Claimed::HeldBy { .. }));
        advance(10).await;
        let mine = b.step().await.unwrap();
        assert_eq!(epoch(&mine), 2);
        let stored = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(stored.doc.is_held_by(&holder("b")));
    }

    #[tokio::test(start_paused = true)]
    async fn a_takeover_that_landed_behind_an_error_is_this_nodes() {
        let store = ScriptedStore::new();
        acquired(&leases(&store), "a").await;
        let mut b = claim(&store, "b");
        b.step().await.unwrap();
        advance(15_000).await;
        store.fail_next(Fault {
            kind: Kind::Contention,
            lands: true,
        });
        assert_eq!(epoch(&b.step().await.unwrap()), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_takeover_that_failed_is_tried_again_at_the_next_step() {
        let store = ScriptedStore::new();
        acquired(&leases(&store), "a").await;
        let mut b = claim(&store, "b");
        b.step().await.unwrap();
        advance(15_000).await;
        store.fail_next(Fault {
            kind: Kind::Transient,
            lands: false,
        });
        assert!(b.step().await.is_err());
        assert_eq!(epoch(&b.step().await.unwrap()), 2, "no second wait");
    }

    #[tokio::test(start_paused = true)]
    async fn a_release_whose_write_landed_behind_an_error_is_done() {
        let store = ScriptedStore::new();
        let lease = acquired(&leases(&store), "a").await;
        store.fail_next(Fault {
            kind: Kind::Transient,
            lands: true,
        });
        release(&leases(&store), &lease).await.unwrap();
        assert!(
            leases(&store)
                .observe("p")
                .await
                .unwrap()
                .unwrap()
                .doc
                .released
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_release_of_a_lease_taken_meanwhile_leaves_it_to_its_holder() {
        let store = ScriptedStore::new();
        let ls = leases(&store);
        let lease = acquired(&ls, "a").await;
        let seen = ls.observe("p").await.unwrap().unwrap();
        ls.take_over(&seen, &holder("b"), timing().duration, chrono::Utc::now())
            .await
            .unwrap();
        release(&ls, &lease).await.unwrap();
        let stored = ls.observe("p").await.unwrap().unwrap();
        assert!(stored.doc.is_held_by(&holder("b")), "{stored:?}");
    }
}
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -6,5 +6,6 @@
 pub mod lease;
 #[cfg(test)]
 mod test_support;
+pub mod claim;
 
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-cluster
```

Expected:

FAIL to compile (13 errors); the first:

```text
error[E0425]: cannot find type `Claim` in this scope
error[E0425]: cannot find type `Claimed` in this scope
error[E0433]: cannot find type `Instant` in this scope
error[E0433]: cannot find type `Claim` in this scope
error[E0425]: cannot find function `release` in this scope
error[E0433]: cannot find type `Claimed` in this scope
… and 7 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/claim.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/claim.rs b/crates/data-avalanche-cluster/src/claim.rs
--- a/crates/data-avalanche-cluster/src/claim.rs
+++ b/crates/data-avalanche-cluster/src/claim.rs
@@ -1,3 +1,173 @@
+//! Getting a pipeline's lease and giving it back (Clustering §5.3, §8.3). A free lease (absent,
+//! or a released tombstone) is acquired at once. A live holder's lease is taken over only after
+//! it stayed unchanged for a full `lease_duration` on this node's clock: a holder that renews in
+//! the meantime keeps it.
+
+use chrono::Utc;
+use data_avalanche_state::{Acquired, Holder, LeaseDoc, Leases, StateError, Versioned};
+use data_avalanche_storage::ObjectVersion;
+use tokio::time::Instant;
+
+use crate::LeaseTiming;
+
+/// What one [`Claim::step`] found.
+#[derive(Debug)]
+pub enum Claimed {
+    /// This node holds the lease; `written` is when its write was sent (the renewer's first
+    /// `last_renew_local`).
+    Mine {
+        lease: Versioned<LeaseDoc>,
+        written: Instant,
+    },
+    /// Another holder has it. Unless it renews, a step at or after `takeover_at` takes it over.
+    HeldBy {
+        node_id: String,
+        takeover_at: Instant,
+    },
+}
+
+/// Acquire and takeover-evaluation for one pipeline, stepped by its controller.
+#[derive(Debug)]
+pub struct Claim {
+    leases: Leases,
+    pipeline_id: String,
+    holder: Holder,
+    timing: LeaseTiming,
+    /// The live holder's lease version this node watches, and when it first read it.
+    watched: Option<(ObjectVersion, Instant)>,
+}
+
+impl Claim {
+    pub fn new(leases: Leases, pipeline_id: &str, holder: Holder, timing: LeaseTiming) -> Self {
+        Self {
+            leases,
+            pipeline_id: pipeline_id.to_owned(),
+            holder,
+            timing,
+            watched: None,
+        }
+    }
+
+    /// One attempt: acquire a free lease, or watch a live one and take it over once it went a
+    /// full `lease_duration` without changing.
+    ///
+    /// # Errors
+    /// A storage failure; the next step tries again.
+    pub async fn step(&mut self) -> Result<Claimed, StateError> {
+        let sent = Instant::now();
+        let seen = match self
+            .leases
+            .acquire(
+                &self.pipeline_id,
+                &self.holder,
+                self.timing.duration,
+                Utc::now(),
+            )
+            .await?
+        {
+            Acquired::Mine(lease) => {
+                self.watched = None;
+                return Ok(Claimed::Mine {
+                    lease,
+                    written: sent,
+                });
+            }
+            Acquired::HeldBy(seen) => seen,
+        };
+        // The wait runs from when the version was read, never from before: a renewal that
+        // landed while the read was in flight must count.
+        let read = Instant::now();
+        let since = match &self.watched {
+            Some((version, since)) if *version == seen.version => *since,
+            _ => {
+                self.watched = Some((seen.version.clone(), read));
+                read
+            }
+        };
+        let takeover_at = since + self.timing.duration;
+        if read < takeover_at {
+            return Ok(Claimed::HeldBy {
+                node_id: seen.doc.holder_node_id,
+                takeover_at,
+            });
+        }
+        let sent = Instant::now();
+        match self
+            .leases
+            .take_over(&seen, &self.holder, self.timing.duration, Utc::now())
+            .await
+        {
+            Ok(lease) => {
+                self.watched = None;
+                Ok(Claimed::Mine {
+                    lease,
+                    written: sent,
+                })
+            }
+            // The holder renewed, or another challenger won: watch again.
+            Err(e) if e.is_conflict() => {
+                self.watched = None;
+                Ok(Claimed::HeldBy {
+                    node_id: seen.doc.holder_node_id,
+                    takeover_at: Instant::now() + self.timing.duration,
+                })
+            }
+            Err(e) => {
+                // An unknown outcome: the takeover landed if the lease names this holder with
+                // the epoch it proposed.
+                let now = self.leases.observe(&self.pipeline_id).await?;
+                match now {
+                    Some(lease)
+                        if lease.doc.is_held_by(&self.holder)
+                            && Some(lease.doc.owner_epoch)
+                                == seen.doc.owner_epoch.checked_add(1) =>
+                    {
+                        self.watched = None;
+                        Ok(Claimed::Mine {
+                            lease,
+                            written: sent,
+                        })
+                    }
+                    _ => Err(e),
+                }
+            }
+        }
+    }
+}
+
+/// Gives a held lease back as a tombstone (a planned stop or drain, §8.3): the next acquirer
+/// takes it without waiting. A failed write is checked against the stored lease: one that is no
+/// longer this holder's live lease (released by the write itself, taken, deleted) leaves nothing
+/// to give back; one that still is gets the write again.
+///
+/// # Errors
+/// A storage failure that outlasted the attempts.
+pub async fn release(leases: &Leases, lease: &Versioned<LeaseDoc>) -> Result<(), StateError> {
+    const ATTEMPTS: usize = 3;
+    let mut from = lease.clone();
+    let mut attempt = 0;
+    loop {
+        attempt += 1;
+        let Err(e) = leases.release(&from, Utc::now()).await else {
+            return Ok(());
+        };
+        match leases.observe(&lease.doc.pipeline_id).await? {
+            Some(now)
+                if !now.doc.released
+                    && now.doc.holder_node_id == lease.doc.holder_node_id
+                    && now.doc.holder_incarnation == lease.doc.holder_incarnation
+                    && now.doc.owner_epoch == lease.doc.owner_epoch =>
+            {
+                if attempt >= ATTEMPTS {
+                    return Err(e);
+                }
+                from = now;
+            }
+            _ => return Ok(()),
+        }
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::time::Duration;
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -3,9 +3,10 @@
 //! reconciler that tells each controller whether to own. M6b runs them single-node: a self-only
 //! ring, so every pipeline's target is "own".
 
+pub mod claim;
 pub mod lease;
 #[cfg(test)]
 mod test_support;
-pub mod claim;
 
+pub use claim::{Claim, Claimed, release};
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-cluster
```

Expected:

PASS:

```text
test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(cluster): acquire, takeover after a full lease duration, release"
```


### Task 10: Cluster: the pipeline controller

**Files:**
- Create: `crates/data-avalanche-cluster/src/controller.rs`
- Modify: `Cargo.toml`, `crates/data-avalanche-cluster/Cargo.toml`, `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-cluster/src/test_support.rs`

**Interfaces:**
- Consumes: `Claim`, `LeaseRun`, `LeaseGuard` (Tasks 8–9), `PipelineWorker`, `Command`,
  `Recovery` (Tasks 5–6), `PipelineDocs::state` (Task 6), `Metrics` (Task 7).
- Produces:
  - `Parts { source, sink, hooks }`.
  - `trait WorkerFactory { fn build(&self, &Pipeline) -> Result<Parts, String> }`.
  - `Target { Own, DontOwn }`.
  - `Actual { Standby { holder }, Starting, Running, Paused, Stopped, Dumped, Retrying { error },
    Error { error } }`, and `ControllerStatus { actual, owner_epoch, lease, worker }`.
  - `ControllerSettings { timing, drain_timeout, restart: Backoff, start_jitter, tuning }`.
  - `ControllerDeps { leases, holder, factory, metrics, settings, starts: Arc<Semaphore> }`.
  - `PipelineController::new(pipeline, docs, &deps) -> (Self, watch::Receiver<ControllerStatus>)`
    and `run(self, watch::Receiver<Target>)`.

Clustering §20.1 and Lifecycle §2–§5, with Q-M6b-2 and Q-M6b-3 and D-M6b.6. The
controller is the only lease writer. It claims, starts the renewer, builds and runs the worker,
and polls the desired state. It moves the worker with `Pause`, `Resume` and `Drain`, and maps an
error by `recovery()`. A lost lease stops the worker and goes back to claiming. Nine tests on the
paused clock, stable across 10 runs.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -40,6 +40,7 @@ mysql_async = { version = "=0.37.1", default-features = false, features = ["defa
 rustls = { version = "0.23", features = ["aws_lc_rs"] }
 serde-saphyr = { version = "=1.3.0", default-features = false, features = ["deserialize"] }
 humantime = "2.4"
+tracing = "0.1"
 tracing-subscriber = { version = "0.3", default-features = false, features = ["std", "env-filter"] }
 rustls-pki-types = { version = "1", features = ["std"] }
 blake3 = { version = "1", default-features = false, features = ["std"] }
```

Change `crates/data-avalanche-cluster/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-cluster/Cargo.toml b/crates/data-avalanche-cluster/Cargo.toml
--- a/crates/data-avalanche-cluster/Cargo.toml
+++ b/crates/data-avalanche-cluster/Cargo.toml
@@ -12,14 +12,18 @@ workspace = true
 [dependencies]
 chrono.workspace = true
 data-avalanche-config.workspace = true
+data-avalanche-core.workspace = true
 data-avalanche-pipeline.workspace = true
 data-avalanche-state.workspace = true
 data-avalanche-storage.workspace = true
 data-avalanche-telemetry.workspace = true
 tokio = { workspace = true, features = ["sync"] }
+tracing.workspace = true
 
 [dev-dependencies]
+arrow-array.workspace = true
 async-trait.workspace = true
 bytes.workspace = true
+data-avalanche-testkit.workspace = true
 object_store.workspace = true
 tokio = { workspace = true, features = ["test-util"] }
```

Create `crates/data-avalanche-cluster/src/controller.rs`:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::{SourceError, TableRef};
    use data_avalanche_testkit::{FakeTxn, Method};

    use super::*;
    use crate::test_support::{
        Fault, Kind, ScriptedStore, TestFactory, eventually, holder, leases, node, pipeline_docs,
        pipeline_docs_on, row, until,
    };

    fn t(name: &str) -> TableRef {
        TableRef::new("public", name)
    }

    fn is(actual: Actual) -> impl Fn(&ControllerStatus) -> bool {
        move |s| s.actual.as_ref() == Some(&actual)
    }

    fn line(text: &str, metric: &str) -> bool {
        text.lines().any(|l| l == metric)
    }

    const P: &str = r#"pipeline="p",source_host="127.0.0.1",source_db="avalanche""#;

    #[tokio::test(start_paused = true)]
    async fn an_owned_pipeline_runs_under_its_lease() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        f.source.load(&t("a"), vec![row(1)]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(2)));
        eventually("rows land", || f.ids("a") == [1, 2]).await;
        assert_eq!(n.status.borrow().owner_epoch, Some(1));
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(lease.doc.is_held_by(&holder("n1")));
        let text = n.metrics.encode();
        assert!(
            line(
                &text,
                &format!(r#"data_avalanche_pipeline_status{{{P},status="running"}} 1"#)
            ),
            "{text}"
        );
        assert!(line(
            &text,
            &format!(r#"data_avalanche_lease_state{{{P},state="held"}} 1"#)
        ));
        assert!(line(&text, "data_avalanche_owned_pipelines 1"), "{text}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_pause_parks_the_worker_and_a_resume_reads_on() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        docs.set_desired_state(DesiredState::Paused).await.unwrap();
        until(&mut n.status, "paused", is(Actual::Paused)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(f.ids("a").is_empty(), "nothing is read while paused");
        assert!(line(
            &n.metrics.encode(),
            &format!(r#"data_avalanche_pipeline_status{{{P},status="paused"}} 1"#)
        ));
        docs.set_desired_state(DesiredState::Running).await.unwrap();
        until(&mut n.status, "running", is(Actual::Running)).await;
        eventually("the row lands", || f.ids("a") == [1]).await;
        assert_eq!(f.builds(), 1, "the same worker: the source stayed warm");
    }

    #[tokio::test(start_paused = true)]
    async fn a_stop_drains_the_worker_and_a_resume_runs_a_new_one() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
        until(&mut n.status, "stopped", is(Actual::Stopped)).await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(f.ids("a").is_empty());
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(
            lease.doc.is_held_by(&holder("n1")),
            "a stopped pipeline keeps its lease"
        );
        docs.set_desired_state(DesiredState::Running).await.unwrap();
        eventually("the row lands", || f.ids("a") == [1]).await;
        assert_eq!(f.builds(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_paused_pipeline_starts_paused_after_a_restart() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        docs.set_desired_state(DesiredState::Paused).await.unwrap();
        until(&mut n.status, "paused", is(Actual::Paused)).await;
        drop(n.target);
        n.task.await.unwrap();
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(lease.doc.released, "a shutdown releases the lease");

        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "paused", is(Actual::Paused)).await;
        assert_eq!(n.status.borrow().owner_epoch, Some(2));
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert!(f.ids("a").is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_node_waits_then_takes_over_when_the_first_dies() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut a = node("n1", &store, &docs, &f);
        until(&mut a.status, "running", is(Actual::Running)).await;
        let mut b = node("n2", &store, &docs, &f);
        until(
            &mut b.status,
            "standby",
            is(Actual::Standby {
                holder: Some("n1".into()),
            }),
        )
        .await;
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(f.builds(), 1, "a live holder keeps it");
        a.task.abort();
        until(&mut b.status, "running on n2", is(Actual::Running)).await;
        assert_eq!(b.status.borrow().owner_epoch, Some(2));
        f.source.commit(FakeTxn::new().insert(&t("a"), row(7)));
        eventually("the row lands", || f.ids("a") == [7]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_worker_error_holds_until_a_stop_and_a_resume() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        f.source.fail(
            Method::Preflight,
            SourceError::Permanent("no privilege".into()),
        );
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "error", |s| {
            matches!(&s.actual, Some(Actual::Error { error }) if error.contains("no privilege"))
        })
        .await;
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(f.builds(), 1, "not rerun");
        assert!(line(
            &n.metrics.encode(),
            &format!(r#"data_avalanche_pipeline_status{{{P},status="error"}} 1"#)
        ));
        docs.open("p", "delta").await.unwrap();
        docs.set_desired_state(DesiredState::Stopped).await.unwrap();
        until(&mut n.status, "stopped", is(Actual::Stopped)).await;
        docs.set_desired_state(DesiredState::Running).await.unwrap();
        until(&mut n.status, "running", is(Actual::Running)).await;
        assert_eq!(f.builds(), 2);
    }

    /// The state store fails Phase 0's first write: the worker ends, and runs again.
    #[tokio::test(start_paused = true)]
    async fn a_transient_worker_error_reruns_it_after_a_backoff() {
        let store = ScriptedStore::new();
        let state = ScriptedStore::new();
        let docs = pipeline_docs_on(&state);
        state.fail_next(Fault {
            kind: Kind::Transient,
            lands: false,
        });
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(
            &mut n.status,
            "retrying",
            |s| matches!(&s.actual, Some(Actual::Retrying { error }) if error.contains("injected")),
        )
        .await;
        until(&mut n.status, "running", is(Actual::Running)).await;
        assert_eq!(f.builds(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_lease_stops_the_worker_and_the_lease_is_claimed_again() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        store.fail_always(Some(Fault {
            kind: Kind::Transient,
            lands: false,
        }));
        until(&mut n.status, "standby", |s| {
            matches!(s.actual, Some(Actual::Standby { .. }))
        })
        .await;
        f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(
            f.ids("a").is_empty(),
            "nothing is committed without the lease"
        );
        store.fail_always(None);
        until(&mut n.status, "running again", is(Actual::Running)).await;
        assert_eq!(n.status.borrow().owner_epoch, Some(2));
        eventually("the row lands", || f.ids("a") == [1]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn a_dont_own_target_drains_releases_and_drops_the_series() {
        let store = ScriptedStore::new();
        let docs = pipeline_docs();
        let f = TestFactory::new(&["a"]);
        let mut n = node("n1", &store, &docs, &f);
        until(&mut n.status, "running", is(Actual::Running)).await;
        n.target.send_replace(Target::DontOwn);
        until(
            &mut n.status,
            "standby",
            is(Actual::Standby { holder: None }),
        )
        .await;
        let lease = leases(&store).observe("p").await.unwrap().unwrap();
        assert!(lease.doc.released);
        let text = n.metrics.encode();
        assert!(!text.contains(P), "{text}");
        assert!(line(&text, "data_avalanche_owned_pipelines 0"), "{text}");
        n.target.send_replace(Target::Own);
        until(&mut n.status, "running", is(Actual::Running)).await;
        assert_eq!(n.status.borrow().owner_epoch, Some(2));
    }
}
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -7,6 +7,7 @@ pub mod claim;
 pub mod lease;
 #[cfg(test)]
 mod test_support;
+pub mod controller;
 
 pub use claim::{Claim, Claimed, release};
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -181,3 +181,218 @@ pub(crate) fn metrics() -> (Metrics, Arc<PipelineMetrics>) {
     );
     (m, Arc::new(p))
 }
+
+/// The test pipeline `p` over `public.*` of a fake PostgreSQL source, flushing at every boundary.
+pub(crate) fn pipeline() -> data_avalanche_config::Pipeline {
+    let yaml = r#"
+config_version: 1
+globals:
+  state: { bucket: app-state }
+pipelines:
+  p:
+    id: p
+    source:
+      host: 127.0.0.1
+      port: 55418
+      db: avalanche
+      user: avalanche
+      password: avalanche
+      ssl_mode: disable
+      postgres: { publication: p_pub, slot: p_slot }
+    tables:
+      include: [public.*]
+      snapshots: { mode: initial }
+    sink: { type: delta, bucket: lake, delta: {}, flush: { seconds: 0 } }
+"#;
+    data_avalanche_config::load_str(
+        yaml,
+        &std::collections::BTreeMap::new(),
+        &data_avalanche_config::FingerprintKey::from_bytes([1; 32]),
+    )
+    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
+    .config
+    .pipelines
+    .into_values()
+    .next()
+    .expect("one pipeline")
+}
+
+/// `public.<name>`: `id bigint` (the key) and `doc text`.
+pub(crate) fn schema(name: &str) -> data_avalanche_core::TableSchema {
+    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, TableRef, TableSchema};
+    TableSchema::new(
+        TableRef::new("public", name),
+        vec![
+            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
+            ColumnSchema::new("doc", V::Text, "text"),
+        ],
+        vec!["id".into()],
+    )
+    .expect("a valid schema")
+}
+
+pub(crate) fn row(id: i64) -> data_avalanche_core::Row {
+    use data_avalanche_core::{ColumnValue, Row};
+    Row(vec![
+        ("id".into(), ColumnValue::Int64(id)),
+        ("doc".into(), ColumnValue::Text("x".into())),
+    ])
+}
+
+/// Builds parts over one fake source and one sink, and counts its builds.
+#[derive(Debug, Clone)]
+pub(crate) struct TestFactory {
+    pub source: data_avalanche_testkit::FakeSource,
+    pub sink: data_avalanche_testkit::MemorySink,
+    pub builds: Arc<std::sync::atomic::AtomicUsize>,
+}
+
+impl TestFactory {
+    pub fn new(tables: &[&str]) -> Self {
+        Self {
+            source: data_avalanche_testkit::FakeSource::new(
+                tables.iter().map(|t| schema(t)).collect(),
+            ),
+            sink: data_avalanche_testkit::MemorySink::new(),
+            builds: Arc::default(),
+        }
+    }
+
+    pub fn builds(&self) -> usize {
+        self.builds.load(std::sync::atomic::Ordering::SeqCst)
+    }
+
+    /// The sink's `id`s of `public.<table>`, in key order.
+    pub fn ids(&self, table: &str) -> Vec<i64> {
+        use arrow_array::{Array, Int64Array};
+        let Some(b) = self
+            .sink
+            .content(&data_avalanche_core::TableRef::new("public", table))
+        else {
+            return vec![];
+        };
+        let ids = b
+            .column_by_name("id")
+            .expect("an id column")
+            .as_any()
+            .downcast_ref::<Int64Array>()
+            .expect("bigint ids");
+        assert_eq!(ids.null_count(), 0);
+        ids.values().to_vec()
+    }
+}
+
+impl crate::WorkerFactory for TestFactory {
+    fn build(&self, _: &data_avalanche_config::Pipeline) -> Result<crate::Parts, String> {
+        self.builds
+            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
+        Ok(crate::Parts {
+            source: Arc::new(self.source.clone()),
+            sink: Arc::new(self.sink.clone()),
+            hooks: Arc::new(data_avalanche_pipeline::NoHooks),
+        })
+    }
+}
+
+/// Fast worker retries; the lease keeps its Balanced timings (the tests run on paused time).
+pub(crate) fn settings() -> crate::ControllerSettings {
+    use data_avalanche_pipeline::{Backoff, Tuning};
+    let ms = |first, max| Backoff {
+        first: Duration::from_millis(first),
+        max: Duration::from_millis(max),
+    };
+    crate::ControllerSettings {
+        timing: timing(),
+        drain_timeout: Duration::from_secs(5),
+        restart: ms(100, 1_000),
+        tuning: Tuning {
+            source_backoff: ms(1, 5),
+            sink_backoff: ms(1, 5),
+            in_flight_backoff: ms(1, 5),
+            contended_window: Duration::from_millis(200),
+            tick: Duration::from_millis(10),
+            augment_attempts: 3,
+        },
+    }
+}
+
+/// The pipeline's state documents, in memory.
+pub(crate) fn pipeline_docs() -> data_avalanche_state::PipelineDocs {
+    let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/p").expect("a root");
+    data_avalanche_state::PipelineDocs::new(DocStore::new(Arc::new(store)))
+}
+
+/// The pipeline's state documents on `store`, whose writes fail on cue.
+pub(crate) fn pipeline_docs_on(store: &Arc<ScriptedStore>) -> data_avalanche_state::PipelineDocs {
+    data_avalanche_state::PipelineDocs::new(DocStore::new(
+        Arc::clone(store) as Arc<dyn VersionedStore>
+    ))
+}
+
+/// A controller running as `node`, its target "own".
+#[derive(Debug)]
+pub(crate) struct Node {
+    pub status: tokio::sync::watch::Receiver<crate::ControllerStatus>,
+    pub target: tokio::sync::watch::Sender<crate::Target>,
+    pub task: tokio::task::JoinHandle<()>,
+    pub metrics: Metrics,
+}
+
+pub(crate) fn deps(
+    name: &str,
+    store: &Arc<ScriptedStore>,
+    factory: &TestFactory,
+) -> crate::ControllerDeps {
+    crate::ControllerDeps {
+        leases: leases(store),
+        holder: holder(name),
+        factory: Arc::new(factory.clone()),
+        metrics: Metrics::new(),
+        settings: settings(),
+        starts: Arc::new(tokio::sync::Semaphore::new(1)),
+    }
+}
+
+pub(crate) fn node(
+    name: &str,
+    store: &Arc<ScriptedStore>,
+    docs: &data_avalanche_state::PipelineDocs,
+    factory: &TestFactory,
+) -> Node {
+    let deps = deps(name, store, factory);
+    let (controller, status) = crate::PipelineController::new(pipeline(), docs.clone(), &deps);
+    let (target, rx) = tokio::sync::watch::channel(crate::Target::Own);
+    Node {
+        status,
+        target,
+        task: tokio::spawn(controller.run(rx)),
+        metrics: deps.metrics,
+    }
+}
+
+/// Waits (on the test clock) until the controller's status satisfies `cond`.
+pub(crate) async fn until(
+    status: &mut tokio::sync::watch::Receiver<crate::ControllerStatus>,
+    what: &str,
+    cond: impl Fn(&crate::ControllerStatus) -> bool,
+) {
+    let waited = tokio::time::timeout(Duration::from_secs(120), status.wait_for(|s| cond(s)))
+        .await
+        .map(|r| r.map(|_| ()));
+    match waited {
+        Ok(Ok(())) => {}
+        Ok(Err(_)) => panic!("the controller ended before {what}"),
+        Err(_) => panic!("timed out waiting until {what}: {:?}", *status.borrow()),
+    }
+}
+
+/// Waits (on the test clock) until `cond` holds, checking every 10 ms.
+pub(crate) async fn eventually(what: &str, cond: impl Fn() -> bool) {
+    for _ in 0..12_000 {
+        if cond() {
+            return;
+        }
+        tokio::time::sleep(Duration::from_millis(10)).await;
+    }
+    panic!("timed out waiting until {what}");
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-cluster
```

Expected:

FAIL to compile (53 errors); the first:

```text
error[E0433]: cannot find `PipelineController` in `crate`
error[E0433]: cannot find `Target` in `crate`
error[E0405]: cannot find trait `WorkerFactory` in the crate root
error[E0425]: cannot find type `Parts` in the crate root
error[E0422]: cannot find struct, variant or union type `Parts` in the crate root
error[E0425]: cannot find type `ControllerSettings` in the crate root
… and 47 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1,3 +1,601 @@
+//! One pipeline's controller (Clustering §20, Pipeline_Lifecycle §5): the only writer of the
+//! pipeline's lease. While its target is "own" it claims the lease, then runs the pipeline's
+//! worker under it and reconciles the worker toward the persisted desired state, polled every
+//! `lease_renewal_interval` (Q-M6b-2). A worker that ends with an error is rerun, stopped for the
+//! lease, or left in `ERROR` (Q-M6b-3).
+
+use std::collections::HashSet;
+use std::fmt::Debug;
+use std::sync::Arc;
+use std::time::Duration;
+
+use data_avalanche_config::Pipeline;
+use data_avalanche_core::{Sink, SourceConnector};
+use data_avalanche_pipeline::{
+    Backoff, Command, Exit, Hooks, Phase, PipelineWorker, Recovery, Tuning, WorkerDeps,
+    WorkerError, WorkerHandle, WorkerStatus,
+};
+use data_avalanche_state::{DesiredState, Holder, Leases, PipelineDocs};
+use data_avalanche_telemetry::{Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, Totals};
+use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
+use tokio::task::JoinHandle;
+use tokio::time::{Instant, MissedTickBehavior};
+
+use crate::{Claim, Claimed, LeaseRun, LeaseTiming, RenewEnd, RenewState, release};
+
+/// A worker's source and sink, built afresh for every run.
+#[derive(Clone)]
+pub struct Parts {
+    pub source: Arc<dyn SourceConnector>,
+    pub sink: Arc<dyn Sink>,
+    pub hooks: Arc<dyn Hooks>,
+}
+
+impl Debug for Parts {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("Parts").finish_non_exhaustive()
+    }
+}
+
+/// Builds a pipeline's [`Parts`]: the binary links the real source and sink.
+pub trait WorkerFactory: Send + Sync + Debug {
+    /// # Errors
+    /// The parts can't be built from the config: the pipeline goes to `ERROR`.
+    fn build(&self, pipeline: &Pipeline) -> Result<Parts, String>;
+}
+
+/// What the reconciler wants of a controller.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum Target {
+    Own,
+    DontOwn,
+}
+
+/// The pipeline's actual state, as this node sees it.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub enum Actual {
+    /// Not owned here: `holder` has the lease, or the target is "don't own".
+    Standby {
+        holder: Option<String>,
+    },
+    /// The worker runs Phase 0.
+    Starting,
+    Running,
+    Paused,
+    Stopped,
+    /// Every table is a `dump_*` table whose snapshot is done: the worker ended.
+    Dumped,
+    /// The worker ended with an error worth another run; it reruns after a backoff.
+    Retrying {
+        error: String,
+    },
+    /// The worker ended with an error an operator must act on (Q-M6b-3): no rerun until the
+    /// process restarts, or a stop then a resume.
+    Error {
+        error: String,
+    },
+}
+
+#[derive(Debug, Clone, PartialEq, Default)]
+pub struct ControllerStatus {
+    pub actual: Option<Actual>,
+    pub owner_epoch: Option<u64>,
+    pub lease: Option<RenewState>,
+    pub worker: Option<WorkerStatus>,
+}
+
+/// A controller's timings.
+#[derive(Debug, Clone)]
+pub struct ControllerSettings {
+    pub timing: LeaseTiming,
+    /// How long a stop or a hand-off waits for the worker to drain before it is cut off.
+    pub drain_timeout: Duration,
+    /// Between reruns of a worker that ended with a transient error.
+    pub restart: Backoff,
+    pub tuning: Tuning,
+}
+
+/// What every controller of a node shares.
+#[derive(Debug, Clone)]
+pub struct ControllerDeps {
+    /// The leases in `globals.state`.
+    pub leases: Leases,
+    /// This node as a lease holder.
+    pub holder: Holder,
+    pub factory: Arc<dyn WorkerFactory>,
+    /// The node's registry.
+    pub metrics: Metrics,
+    pub settings: ControllerSettings,
+    /// Caps the workers in Phase 0 at once (`max_concurrent_takeovers`).
+    pub starts: Arc<Semaphore>,
+}
+
+/// One pipeline's controller.
+#[derive(Debug)]
+pub struct PipelineController {
+    pipeline: Pipeline,
+    docs: PipelineDocs,
+    leases: Leases,
+    holder: Holder,
+    factory: Arc<dyn WorkerFactory>,
+    node: Metrics,
+    metrics: Arc<PipelineMetrics>,
+    settings: ControllerSettings,
+    starts: Arc<Semaphore>,
+    status: watch::Sender<ControllerStatus>,
+}
+
+/// Why the owned phase ended.
+enum Left {
+    /// The lease is gone: claim it again.
+    Lost,
+    /// The target became "don't own": the lease is released.
+    Released,
+    /// The target channel closed: the process is shutting down.
+    Shutdown,
+}
+
+/// What woke the owned loop.
+enum Event {
+    Lost,
+    Target { closed: bool },
+    Ended(Result<Result<Exit, WorkerError>, tokio::task::JoinError>),
+    Status { open: bool },
+    Start(OwnedSemaphorePermit),
+    Due,
+    Poll,
+}
+
+/// A running worker.
+struct Running {
+    handle: WorkerHandle,
+    task: JoinHandle<Result<Exit, WorkerError>>,
+    sink: Arc<dyn Sink>,
+    /// Held while the worker is in Phase 0.
+    permit: Option<OwnedSemaphorePermit>,
+    /// A pause was sent and not yet undone.
+    paused: bool,
+    /// The warnings already logged.
+    logged: HashSet<String>,
+}
+
+impl PipelineController {
+    /// `docs` are the pipeline's state documents.
+    pub fn new(
+        pipeline: Pipeline,
+        docs: PipelineDocs,
+        deps: &ControllerDeps,
+    ) -> (Self, watch::Receiver<ControllerStatus>) {
+        let (status, rx) = watch::channel(ControllerStatus::default());
+        let labels = PipelineLabels {
+            pipeline: pipeline.name.clone(),
+            source_host: pipeline.source.host.clone(),
+            source_db: pipeline.source.db.clone(),
+        };
+        let metrics = Arc::new(
+            deps.metrics
+                .pipeline(labels, pipeline.sink.metrics.per_table),
+        );
+        (
+            Self {
+                pipeline,
+                docs,
+                leases: deps.leases.clone(),
+                holder: deps.holder.clone(),
+                factory: Arc::clone(&deps.factory),
+                node: deps.metrics.clone(),
+                metrics,
+                settings: deps.settings.clone(),
+                starts: Arc::clone(&deps.starts),
+                status,
+            },
+            rx,
+        )
+    }
+
+    /// Runs until the target channel closes; the lease is released on the way out.
+    pub async fn run(self, mut target: watch::Receiver<Target>) {
+        let mut claim = Claim::new(
+            self.leases.clone(),
+            &self.pipeline.id,
+            self.holder.clone(),
+            self.settings.timing,
+        );
+        loop {
+            if *target.borrow_and_update() == Target::DontOwn {
+                self.standby(None);
+                if target.changed().await.is_err() {
+                    return;
+                }
+                continue;
+            }
+            let wait = match claim.step().await {
+                Ok(Claimed::Mine { lease, written }) => {
+                    tracing::info!(
+                        pipeline = %self.pipeline.name,
+                        owner_epoch = lease.doc.owner_epoch,
+                        "lease acquired"
+                    );
+                    let run = LeaseRun::start(
+                        self.leases.clone(),
+                        lease,
+                        self.holder.clone(),
+                        self.settings.timing,
+                        written,
+                        Arc::clone(&self.metrics),
+                    );
+                    match self.owned(run, &mut target).await {
+                        Left::Shutdown => return,
+                        // Not this node's until a step says otherwise, though claiming fails.
+                        Left::Lost => {
+                            self.standby(None);
+                            continue;
+                        }
+                        Left::Released => continue,
+                    }
+                }
+                Ok(Claimed::HeldBy {
+                    node_id,
+                    takeover_at,
+                }) => {
+                    self.standby(Some(node_id));
+                    takeover_at.min(Instant::now() + self.settings.timing.renewal_interval)
+                }
+                Err(e) => {
+                    tracing::warn!(pipeline = %self.pipeline.name, "lease claim failed: {e}");
+                    Instant::now() + self.settings.timing.renewal_interval
+                }
+            };
+            tokio::select! {
+                () = tokio::time::sleep_until(wait) => {}
+                r = target.changed() => if r.is_err() { return },
+            }
+        }
+    }
+
+    /// Holds the lease and reconciles the worker until the lease is lost or let go.
+    async fn owned(&self, run: LeaseRun, target: &mut watch::Receiver<Target>) -> Left {
+        self.node.owned_by(1);
+        let left = self.reconcile(run, target).await;
+        self.node.owned_by(-1);
+        left
+    }
+
+    async fn reconcile(&self, run: LeaseRun, target: &mut watch::Receiver<Target>) -> Left {
+        let mut worker: Option<Running> = None;
+        let mut error: Option<String> = None;
+        let mut dumped = false;
+        let mut retry: Option<(Instant, String)> = None;
+        let mut failures: u32 = 0;
+        let mut desired = self.desired(DesiredState::Running).await;
+        let mut poll = tokio::time::interval(self.settings.timing.renewal_interval);
+        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
+        poll.tick().await;
+        loop {
+            if desired == DesiredState::Stopped {
+                if let Some(w) = worker.take() {
+                    self.drain(w).await;
+                }
+                // A stop clears an error: the resume after it runs the pipeline again.
+                error = None;
+                dumped = false;
+                retry = None;
+                failures = 0;
+            }
+            if let Some(w) = worker.as_mut() {
+                match desired {
+                    DesiredState::Paused if !w.paused => {
+                        let _ = w.handle.control.send(Command::Pause).await;
+                        w.paused = true;
+                    }
+                    DesiredState::Running if w.paused => {
+                        let _ = w.handle.control.send(Command::Resume).await;
+                        w.paused = false;
+                    }
+                    _ => {}
+                }
+            }
+            let due = retry.as_ref().map(|(at, _)| *at);
+            let want_start = worker.is_none()
+                && desired != DesiredState::Stopped
+                && error.is_none()
+                && !dumped
+                && due.is_none_or(|at| Instant::now() >= at);
+            self.publish(
+                &run,
+                worker.as_ref(),
+                desired,
+                error.as_deref(),
+                dumped,
+                retry.as_ref().map(|(_, e)| e.as_str()),
+            );
+            let starts = Arc::clone(&self.starts);
+            let idle = worker.is_none();
+            let event = {
+                let (task, status) = match worker.as_mut() {
+                    Some(Running { task, handle, .. }) => (Some(task), Some(&mut handle.status)),
+                    None => (None, None),
+                };
+                tokio::select! {
+                    biased;
+                    () = run.guard.lost() => Event::Lost,
+                    r = target.changed() => Event::Target { closed: r.is_err() },
+                    ended = async {
+                        match task {
+                            Some(task) => task.await,
+                            None => std::future::pending().await,
+                        }
+                    } => Event::Ended(ended),
+                    changed = async {
+                        match status {
+                            Some(status) => status.changed().await.is_ok(),
+                            None => std::future::pending().await,
+                        }
+                    } => Event::Status { open: changed },
+                    permit = starts.acquire_owned(), if want_start => {
+                        Event::Start(permit.expect("the start semaphore isn't closed"))
+                    }
+                    () = async {
+                        match due {
+                            Some(at) => tokio::time::sleep_until(at).await,
+                            None => std::future::pending().await,
+                        }
+                    }, if idle && !want_start => Event::Due,
+                    _ = poll.tick() => Event::Poll,
+                }
+            };
+            match event {
+                Event::Lost => {
+                    if let Some(w) = worker.take() {
+                        w.task.abort();
+                        self.metrics.worker_ended();
+                    }
+                    let reason = match run.stop().await {
+                        RenewEnd::Lost { reason } => reason,
+                        RenewEnd::Stopped(_) => "the lease's deadline passed".into(),
+                    };
+                    tracing::error!(pipeline = %self.pipeline.name, "lease lost: {reason}");
+                    return Left::Lost;
+                }
+                Event::Target { closed } => {
+                    if closed || *target.borrow() == Target::DontOwn {
+                        if let Some(w) = worker.take() {
+                            self.drain(w).await;
+                        }
+                        self.give_back(run).await;
+                        self.standby(None);
+                        return if closed {
+                            Left::Shutdown
+                        } else {
+                            Left::Released
+                        };
+                    }
+                }
+                Event::Ended(ended) => {
+                    let mut w = worker.take().expect("a worker ended");
+                    self.observe(&mut w);
+                    self.metrics.worker_ended();
+                    let ended = ended.unwrap_or_else(|e| {
+                        Err(WorkerError::Refused(format!(
+                            "the worker task ended abnormally: {e}"
+                        )))
+                    });
+                    match ended {
+                        Ok(Exit::Dumped) => dumped = true,
+                        Ok(Exit::Drained) => {
+                            retry = Some((Instant::now(), "the worker drained unasked".into()));
+                        }
+                        Err(e) => match e.recovery() {
+                            Recovery::Retry => {
+                                let at = Instant::now() + self.settings.restart.delay(failures);
+                                failures = failures.saturating_add(1);
+                                tracing::warn!(
+                                    pipeline = %self.pipeline.name,
+                                    "worker ended, rerun after a backoff: {e}"
+                                );
+                                retry = Some((at, e.to_string()));
+                            }
+                            Recovery::Lease => {
+                                tracing::error!(
+                                    pipeline = %self.pipeline.name,
+                                    "worker stopped for the lease: {e}"
+                                );
+                                let _ = run.stop().await;
+                                return Left::Lost;
+                            }
+                            Recovery::Error => {
+                                tracing::error!(
+                                    pipeline = %self.pipeline.name,
+                                    "pipeline in ERROR: {e}"
+                                );
+                                error = Some(e.to_string());
+                            }
+                        },
+                    }
+                }
+                Event::Status { open } => {
+                    if open && let Some(w) = worker.as_mut() {
+                        if w.handle.status.borrow().phase != Phase::Starting {
+                            // Phase 0 is over: another worker may start, and the run counts
+                            // as a success.
+                            w.permit = None;
+                            failures = 0;
+                            retry = None;
+                        }
+                        self.observe(w);
+                    }
+                }
+                Event::Start(permit) => match self.start(&run, permit) {
+                    Ok(w) => worker = Some(w),
+                    Err(e) => {
+                        tracing::error!(pipeline = %self.pipeline.name, "pipeline in ERROR: {e}");
+                        error = Some(e);
+                    }
+                },
+                Event::Due => {}
+                Event::Poll => {
+                    desired = self.desired(desired).await;
+                    if let Some(w) = worker.as_mut() {
+                        self.observe(w);
+                    }
+                }
+            }
+        }
+    }
+
+    /// The persisted desired state; on a failed read, the last one known. A pipeline never
+    /// started has none yet: it runs.
+    async fn desired(&self, last: DesiredState) -> DesiredState {
+        match self.docs.state().await {
+            Ok(Some(v)) => v.doc.desired_state,
+            Ok(None) => DesiredState::Running,
+            Err(e) => {
+                tracing::warn!(pipeline = %self.pipeline.name, "desired state unread: {e}");
+                last
+            }
+        }
+    }
+
+    fn start(&self, run: &LeaseRun, permit: OwnedSemaphorePermit) -> Result<Running, String> {
+        let parts = self.factory.build(&self.pipeline)?;
+        let deps = WorkerDeps {
+            source: parts.source,
+            sink: Arc::clone(&parts.sink),
+            docs: self.docs.clone(),
+            owner_epoch: run.owner_epoch,
+            guard: run.guard.clone(),
+            hooks: parts.hooks,
+        };
+        let (worker, handle) =
+            PipelineWorker::new(&self.pipeline, deps, self.settings.tuning.clone());
+        tracing::info!(pipeline = %self.pipeline.name, "worker started");
+        Ok(Running {
+            handle,
+            task: tokio::spawn(worker.run()),
+            sink: parts.sink,
+            permit: Some(permit),
+            paused: false,
+            logged: HashSet::new(),
+        })
+    }
+
+    /// A stop or a hand-off: the worker flushes and ends within `drain_timeout`, else it is cut
+    /// off (what it read past the durable checkpoint is read again by the next run).
+    async fn drain(&self, mut w: Running) {
+        let _ = w.handle.control.send(Command::Drain).await;
+        match tokio::time::timeout(self.settings.drain_timeout, &mut w.task).await {
+            Ok(Ok(Ok(_))) => tracing::info!(pipeline = %self.pipeline.name, "worker drained"),
+            Ok(Ok(Err(e))) => {
+                tracing::warn!(pipeline = %self.pipeline.name, "worker ended draining: {e}");
+            }
+            Ok(Err(e)) => tracing::error!(pipeline = %self.pipeline.name, "worker task: {e}"),
+            Err(_) => {
+                tracing::warn!(
+                    pipeline = %self.pipeline.name,
+                    "worker didn't drain within {:?}; cut off",
+                    self.settings.drain_timeout
+                );
+                w.task.abort();
+            }
+        }
+        self.observe(&mut w);
+        self.metrics.worker_ended();
+    }
+
+    async fn give_back(&self, run: LeaseRun) {
+        match run.stop().await {
+            RenewEnd::Stopped(lease) => match release(&self.leases, &lease).await {
+                Ok(()) => tracing::info!(pipeline = %self.pipeline.name, "lease released"),
+                Err(e) => tracing::warn!(
+                    pipeline = %self.pipeline.name,
+                    "lease not released ({e}); it expires instead"
+                ),
+            },
+            RenewEnd::Lost { reason } => {
+                tracing::warn!(pipeline = %self.pipeline.name, "lease lost: {reason}");
+            }
+        }
+    }
+
+    /// The worker's status into the metrics, and its new warnings into the log.
+    fn observe(&self, w: &mut Running) {
+        let st = w.handle.status.borrow().clone();
+        for warning in &st.warnings {
+            if w.logged.insert(warning.clone()) {
+                tracing::warn!(pipeline = %self.pipeline.name, "{warning}");
+            }
+        }
+        let m = &self.metrics;
+        m.wal_lag_bytes(st.retained_bytes);
+        m.halted_tables(st.halted.len());
+        m.reconcile_log(st.reconcile_log.entries, st.reconcile_log.keys);
+        let rows: Vec<(String, u64)> = st
+            .snapshots
+            .iter()
+            .map(|(t, p)| (t.to_string(), p.rows_done))
+            .collect();
+        m.snapshot_rows_done(&rows);
+        let sink = w.sink.stats();
+        m.totals(Totals {
+            source_reconnects: st.reconnects,
+            sink_merge_files_rewritten: sink.files_rewritten,
+            sink_merge_rows_copied: sink.rows_copied,
+        });
+        self.status.send_modify(|s| s.worker = Some(st));
+    }
+
+    fn standby(&self, holder: Option<String>) {
+        self.metrics.remove();
+        self.status.send_replace(ControllerStatus {
+            actual: Some(Actual::Standby { holder }),
+            ..ControllerStatus::default()
+        });
+    }
+
+    fn publish(
+        &self,
+        run: &LeaseRun,
+        worker: Option<&Running>,
+        desired: DesiredState,
+        error: Option<&str>,
+        dumped: bool,
+        retrying: Option<&str>,
+    ) {
+        let phase = worker.map(|w| w.handle.status.borrow().phase.clone());
+        let actual = if let Some(e) = error {
+            Actual::Error {
+                error: e.to_owned(),
+            }
+        } else if dumped {
+            Actual::Dumped
+        } else {
+            match (desired, phase) {
+                (DesiredState::Stopped, _) => Actual::Stopped,
+                (_, Some(Phase::Paused)) => Actual::Paused,
+                (_, Some(Phase::Starting)) => Actual::Starting,
+                (_, Some(_)) => Actual::Running,
+                (_, None) => match retrying {
+                    Some(e) => Actual::Retrying {
+                        error: e.to_owned(),
+                    },
+                    None => Actual::Starting,
+                },
+            }
+        };
+        self.metrics.status(match &actual {
+            Actual::Paused => PipelineStatus::Paused,
+            Actual::Stopped | Actual::Dumped => PipelineStatus::Stopped,
+            Actual::Error { .. } => PipelineStatus::Error,
+            _ => PipelineStatus::Running,
+        });
+        self.status.send_modify(|s| {
+            s.actual = Some(actual);
+            s.owner_epoch = Some(run.owner_epoch);
+            s.lease = Some(run.guard.state());
+        });
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use data_avalanche_core::{SourceError, TableRef};
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -4,10 +4,14 @@
 //! ring, so every pipeline's target is "own".
 
 pub mod claim;
+pub mod controller;
 pub mod lease;
 #[cfg(test)]
 mod test_support;
-pub mod controller;
 
 pub use claim::{Claim, Claimed, release};
+pub use controller::{
+    Actual, ControllerDeps, ControllerSettings, ControllerStatus, Parts, PipelineController,
+    Target, WorkerFactory,
+};
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-cluster
```

Expected:

PASS:

```text
test result: ok. 25 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.19s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(cluster): the pipeline controller: the worker under the lease, reconciled to the desired state"
```


### Task 11: Cluster: the single-node reconciler, the node's identity, capped starts

**Files:**
- Create: `crates/data-avalanche-cluster/src/reconciler.rs`
- Modify: `Cargo.toml`, `crates/data-avalanche-cluster/Cargo.toml`, `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-cluster/src/lib.rs`, `crates/data-avalanche-cluster/src/test_support.rs`

**Interfaces:**
- Consumes: `PipelineController`, `ControllerDeps` (Task 10), `Cluster` (config).
- Produces: `node_identity(&Cluster, started: SystemTime) -> Result<Holder, String>`;
  `ClusterReconciler::start(Vec<(Pipeline, PipelineDocs)>, &ControllerDeps) -> Self`,
  `statuses()`, `async shutdown()`; `ControllerDeps::starts` caps Phase 0 at
  `max_concurrent_takeovers`; `ControllerSettings::start_jitter`.

Roadmap §3.7, one `ClusterReconciler` tick with a self-only ring: every target is `Own`.
D-M6b.7 and D-M6b.8. The cap is a permit held while the worker's phase is `Starting`. A mutation
check, dropping it at the start, fails `one_phase_0_at_a_time_holds_the_other_start`.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -69,6 +69,7 @@ tokio-postgres-rustls = "0.14"
 webpki-roots = "1"
 # The binary's control plane (M6b): metrics (roadmap §2.1).
 prometheus-client = "=0.25.1"
+gethostname = "1.1"
 
 # Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
 # CI runners. Workspace crates keep it.
```

Change `crates/data-avalanche-cluster/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-cluster/Cargo.toml b/crates/data-avalanche-cluster/Cargo.toml
--- a/crates/data-avalanche-cluster/Cargo.toml
+++ b/crates/data-avalanche-cluster/Cargo.toml
@@ -17,6 +17,8 @@ data-avalanche-pipeline.workspace = true
 data-avalanche-state.workspace = true
 data-avalanche-storage.workspace = true
 data-avalanche-telemetry.workspace = true
+gethostname.workspace = true
+getrandom.workspace = true
 tokio = { workspace = true, features = ["sync"] }
 tracing.workspace = true
 
```

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -616,6 +616,13 @@ mod tests {
 
     const P: &str = r#"pipeline="p",source_host="127.0.0.1",source_db="avalanche""#;
 
+    #[test]
+    fn a_start_jitter_stays_below_its_bound() {
+        assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
+        let max = Duration::from_millis(500);
+        assert!((0..200).all(|_| jitter(max) < max));
+    }
+
     #[tokio::test(start_paused = true)]
     async fn an_owned_pipeline_runs_under_its_lease() {
         let store = ScriptedStore::new();
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -8,6 +8,7 @@ pub mod controller;
 pub mod lease;
 #[cfg(test)]
 mod test_support;
+pub mod reconciler;
 
 pub use claim::{Claim, Claimed, release};
 pub use controller::{
```

Create `crates/data-avalanche-cluster/src/reconciler.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use data_avalanche_core::{SourceError, TableRef};
    use data_avalanche_testkit::{FakeTxn, Method};

    use super::*;
    use crate::test_support::{
        ScriptedStore, TestFactory, config, deps, eventually, leases, pipeline_docs, row, until,
    };
    use crate::{Actual, Parts, WorkerFactory};

    /// One [`TestFactory`] per pipeline id.
    #[derive(Debug)]
    struct ByPipeline(BTreeMap<String, TestFactory>);

    impl WorkerFactory for ByPipeline {
        fn build(&self, p: &Pipeline) -> Result<Parts, String> {
            self.0[&p.id].build(p)
        }
    }

    fn running(s: &ControllerStatus) -> bool {
        s.actual == Some(Actual::Running)
    }

    /// Two pipelines `p` and `q` on one node; `starts` workers may be in Phase 0 at once.
    fn two(
        store: &Arc<ScriptedStore>,
        starts: usize,
    ) -> (ClusterReconciler, TestFactory, TestFactory, ControllerDeps) {
        let (fp, fq) = (TestFactory::new(&["a"]), TestFactory::new(&["a"]));
        let mut d = deps("n1", store, &fp);
        d.factory = Arc::new(ByPipeline(BTreeMap::from([
            ("p".to_owned(), fp.clone()),
            ("q".to_owned(), fq.clone()),
        ])));
        d.starts = Arc::new(tokio::sync::Semaphore::new(starts));
        let pipelines = config(&["p", "q"])
            .pipelines
            .into_values()
            .map(|p| (p, pipeline_docs()))
            .collect();
        (ClusterReconciler::start(pipelines, &d), fp, fq, d)
    }

    #[test]
    fn the_node_is_its_node_name_else_its_hostname() {
        let started = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        let mut cluster = config(&["p"]).globals.cluster;
        let h = node_identity(&cluster, started).unwrap();
        assert_eq!(
            (h.node_id.as_str(), h.incarnation),
            ("n1", 1_700_000_000_123)
        );
        cluster.memberlist.node_name = None;
        let h = node_identity(&cluster, started).unwrap();
        assert_eq!(h.node_id, gethostname::gethostname().into_string().unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn every_pipeline_runs_here_and_a_shutdown_releases_every_lease() {
        let store = ScriptedStore::new();
        let (r, fp, fq, d) = two(&store, 2);
        for (_, mut s) in r.statuses() {
            until(&mut s, "running", running).await;
        }
        fp.source
            .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(1)));
        fq.source
            .commit(FakeTxn::new().insert(&TableRef::new("public", "a"), row(2)));
        eventually("both land", || fp.ids("a") == [1] && fq.ids("a") == [2]).await;
        let text = d.metrics.encode();
        assert!(
            text.lines()
                .any(|l| l == "data_avalanche_owned_pipelines 2"),
            "{text}"
        );
        assert!(
            text.lines()
                .any(|l| l == "data_avalanche_cluster_members 1"),
            "{text}"
        );
        r.shutdown().await;
        for id in ["p", "q"] {
            let lease = leases(&store).observe(id).await.unwrap().unwrap();
            assert!(lease.doc.released, "{id}");
        }
        assert!(
            d.metrics
                .encode()
                .lines()
                .any(|l| l == "data_avalanche_owned_pipelines 0")
        );
    }

    /// `max_concurrent_takeovers` 1: while `p`'s Phase 0 retries its preflight, `q` builds no
    /// worker; it starts once `p` streams.
    #[tokio::test(start_paused = true)]
    async fn one_phase_0_at_a_time_holds_the_other_start() {
        let store = ScriptedStore::new();
        let (fp, fq) = (TestFactory::new(&["a"]), TestFactory::new(&["a"]));
        for _ in 0..3_000 {
            fp.source
                .fail(Method::Preflight, SourceError::Transient("slow".into()));
        }
        let mut d = deps("n1", &store, &fp);
        d.factory = Arc::new(ByPipeline(BTreeMap::from([
            ("p".to_owned(), fp.clone()),
            ("q".to_owned(), fq.clone()),
        ])));
        d.starts = Arc::new(tokio::sync::Semaphore::new(1));
        let mut cfg = config(&["p", "q"]).pipelines;
        // `p` first: the controllers start in pipeline-id order, and `p`'s takes the permit.
        let pipelines = vec![
            (cfg.remove("p").unwrap(), pipeline_docs()),
            (cfg.remove("q").unwrap(), pipeline_docs()),
        ];
        let r = ClusterReconciler::start(pipelines, &d);
        let mut st = r.statuses();
        eventually("p is in Phase 0", || fp.builds() == 1).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            st["p"].borrow().actual,
            Some(Actual::Starting),
            "p still retries its preflight"
        );
        assert_eq!(fq.builds(), 0, "q waits for the permit");
        until(st.get_mut("p").unwrap(), "p running", running).await;
        until(st.get_mut("q").unwrap(), "q running", running).await;
        assert_eq!(fq.builds(), 1);
        r.shutdown().await;
    }
}
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -182,15 +182,16 @@ pub(crate) fn metrics() -> (Metrics, Arc<PipelineMetrics>) {
     (m, Arc::new(p))
 }
 
-/// The test pipeline `p` over `public.*` of a fake PostgreSQL source, flushing at every boundary.
-pub(crate) fn pipeline() -> data_avalanche_config::Pipeline {
-    let yaml = r#"
-config_version: 1
-globals:
-  state: { bucket: app-state }
-pipelines:
-  p:
-    id: p
+/// A config with one pipeline per id, each over `public.*` of a fake PostgreSQL source (slot
+/// `<id>_slot`), flushing at every boundary.
+pub(crate) fn config(ids: &[&str]) -> data_avalanche_config::ResolvedConfig {
+    let mut yaml = String::from(
+        "config_version: 1\nglobals:\n  state: { bucket: app-state }\n  cluster: { memberlist: { node_name: n1 } }\npipelines:\n",
+    );
+    for id in ids {
+        yaml.push_str(&format!(
+            r#"  {id}:
+    id: {id}
     source:
       host: 127.0.0.1
       port: 55418
@@ -198,23 +199,26 @@ pipelines:
       user: avalanche
       password: avalanche
       ssl_mode: disable
-      postgres: { publication: p_pub, slot: p_slot }
+      postgres: {{ publication: {id}_pub, slot: {id}_slot }}
     tables:
       include: [public.*]
-      snapshots: { mode: initial }
-    sink: { type: delta, bucket: lake, delta: {}, flush: { seconds: 0 } }
-"#;
+      snapshots: {{ mode: initial }}
+    sink: {{ type: delta, bucket: lake, prefix: {id}, delta: {{}}, flush: {{ seconds: 0 }} }}
+"#
+        ));
+    }
     data_avalanche_config::load_str(
-        yaml,
+        &yaml,
         &std::collections::BTreeMap::new(),
         &data_avalanche_config::FingerprintKey::from_bytes([1; 32]),
     )
-    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
+    .unwrap_or_else(|r| panic!("test config: {r:?}"))
     .config
-    .pipelines
-    .into_values()
-    .next()
-    .expect("one pipeline")
+}
+
+/// The test pipeline `p`.
+pub(crate) fn pipeline() -> data_avalanche_config::Pipeline {
+    config(&["p"]).pipelines.remove("p").expect("pipeline p")
 }
 
 /// `public.<name>`: `id bigint` (the key) and `doc text`.
@@ -305,6 +309,7 @@ pub(crate) fn settings() -> crate::ControllerSettings {
         timing: timing(),
         drain_timeout: Duration::from_secs(5),
         restart: ms(100, 1_000),
+        start_jitter: Duration::from_millis(500),
         tuning: Tuning {
             source_backoff: ms(1, 5),
             sink_backoff: ms(1, 5),
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-cluster
```

Expected:

FAIL to compile (15 errors); the first:

```text
error[E0425]: cannot find type `BTreeMap` in this scope
error[E0425]: cannot find type `Pipeline` in this scope
error[E0425]: cannot find type `ControllerStatus` in this scope
error[E0425]: cannot find type `ClusterReconciler` in this scope
error[E0425]: cannot find type `ControllerDeps` in this scope
error[E0433]: cannot find type `BTreeMap` in this scope
… and 9 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -92,6 +92,9 @@ pub struct ControllerSettings {
     pub drain_timeout: Duration,
     /// Between reruns of a worker that ended with a transient error.
     pub restart: Backoff,
+    /// Each Phase 0 starts after a random wait below this, so starts that the cap lets through
+    /// together don't reconnect together (Clustering §20.2).
+    pub start_jitter: Duration,
     pub tuning: Tuning,
 }
 
@@ -135,6 +138,17 @@ enum Left {
     Shutdown,
 }
 
+/// A random wait in `[0, max)`.
+fn jitter(max: Duration) -> Duration {
+    let nanos = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX);
+    match nanos {
+        0 => Duration::ZERO,
+        n => Duration::from_nanos(
+            getrandom::u64().expect("the OS random number generator is unavailable") % n,
+        ),
+    }
+}
+
 /// What woke the owned loop.
 enum Event {
     Lost,
@@ -463,10 +477,14 @@ impl PipelineController {
         };
         let (worker, handle) =
             PipelineWorker::new(&self.pipeline, deps, self.settings.tuning.clone());
+        let jitter = jitter(self.settings.start_jitter);
         tracing::info!(pipeline = %self.pipeline.name, "worker started");
         Ok(Running {
             handle,
-            task: tokio::spawn(worker.run()),
+            task: tokio::spawn(async move {
+                tokio::time::sleep(jitter).await;
+                worker.run().await
+            }),
             sink: parts.sink,
             permit: Some(permit),
             paused: false,
```

Change `crates/data-avalanche-cluster/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -6,9 +6,9 @@
 pub mod claim;
 pub mod controller;
 pub mod lease;
+pub mod reconciler;
 #[cfg(test)]
 mod test_support;
-pub mod reconciler;
 
 pub use claim::{Claim, Claimed, release};
 pub use controller::{
@@ -16,3 +16,4 @@ pub use controller::{
     Target, WorkerFactory,
 };
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
+pub use reconciler::{ClusterReconciler, node_identity};
```

Change `crates/data-avalanche-cluster/src/reconciler.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/reconciler.rs b/crates/data-avalanche-cluster/src/reconciler.rs
--- a/crates/data-avalanche-cluster/src/reconciler.rs
+++ b/crates/data-avalanche-cluster/src/reconciler.rs
@@ -1,3 +1,108 @@
+//! The node's reconciler (Clustering §8.1, §20). It runs one controller per pipeline and tells
+//! each whether to own its pipeline. M6b's ring holds this node alone, so one tick sets every
+//! target to "own"; M8 derives the targets from membership.
+
+use std::collections::BTreeMap;
+use std::time::{SystemTime, UNIX_EPOCH};
+
+use data_avalanche_config::{Cluster, Pipeline};
+use data_avalanche_state::{Holder, PipelineDocs};
+use tokio::sync::watch;
+use tokio::task::JoinHandle;
+
+use crate::{ControllerDeps, ControllerStatus, PipelineController, Target};
+
+/// This node as a lease holder (Clustering §3.4, D-1): `node_id` is
+/// `globals.cluster.memberlist.node_name`, else the OS hostname; the incarnation is the process's
+/// start in unix milliseconds, so a restarted process is another holder.
+///
+/// # Errors
+/// No `node_name` and no usable hostname.
+pub fn node_identity(cluster: &Cluster, started: SystemTime) -> Result<Holder, String> {
+    let node_id = match &cluster.memberlist.node_name {
+        Some(name) => name.clone(),
+        None => gethostname::gethostname().into_string().map_err(|h| {
+            format!("the hostname {h:?} isn't UTF-8: set globals.cluster.memberlist.node_name")
+        })?,
+    };
+    if node_id.is_empty() {
+        return Err("the hostname is empty: set globals.cluster.memberlist.node_name".into());
+    }
+    let incarnation = started
+        .duration_since(UNIX_EPOCH)
+        .map_err(|e| format!("the clock is before 1970: {e}"))?
+        .as_millis();
+    Ok(Holder {
+        node_id,
+        incarnation: u64::try_from(incarnation).unwrap_or(u64::MAX),
+    })
+}
+
+#[derive(Debug)]
+struct Controlled {
+    target: watch::Sender<Target>,
+    status: watch::Receiver<ControllerStatus>,
+    task: JoinHandle<()>,
+}
+
+/// The controllers of every configured pipeline.
+#[derive(Debug)]
+pub struct ClusterReconciler {
+    controllers: BTreeMap<String, Controlled>,
+}
+
+impl ClusterReconciler {
+    /// Starts a controller for each pipeline (with its state documents) and runs one reconcile
+    /// tick of a self-only ring: every pipeline's target is "own".
+    pub fn start(pipelines: Vec<(Pipeline, PipelineDocs)>, deps: &ControllerDeps) -> Self {
+        deps.metrics.set_cluster_members(1);
+        let controllers = pipelines
+            .into_iter()
+            .map(|(pipeline, docs)| {
+                let id = pipeline.id.clone();
+                let (controller, status) = PipelineController::new(pipeline, docs, deps);
+                let (target, rx) = watch::channel(Target::Own);
+                let task = tokio::spawn(controller.run(rx));
+                (
+                    id,
+                    Controlled {
+                        target,
+                        status,
+                        task,
+                    },
+                )
+            })
+            .collect();
+        Self { controllers }
+    }
+
+    /// Each pipeline's controller status, by pipeline id.
+    pub fn statuses(&self) -> BTreeMap<String, watch::Receiver<ControllerStatus>> {
+        self.controllers
+            .iter()
+            .map(|(id, c)| (id.clone(), c.status.clone()))
+            .collect()
+    }
+
+    /// Lets every pipeline go: each controller drains its worker (within `drain_timeout`) and
+    /// releases its lease, so the next process acquires it without waiting.
+    pub async fn shutdown(self) {
+        let tasks: Vec<JoinHandle<()>> = self
+            .controllers
+            .into_values()
+            .map(|c| {
+                drop(c.target);
+                c.task
+            })
+            .collect();
+        for task in tasks {
+            if let Err(e) = task.await {
+                tracing::error!("a pipeline controller ended abnormally: {e}");
+            }
+        }
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::Arc;
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-cluster
```

Expected:

PASS:

```text
test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.17s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(cluster): the single-node reconciler, this node's identity, Phase 0 starts capped and jittered"
```


### Task 12: The binary: `run --config`, the startup sequence, every pipeline under its lease

**Files:**
- Create: `crates/data-avalanche/Cargo.toml`, `crates/data-avalanche/src/cli.rs`, `crates/data-avalanche/src/lib.rs`, `crates/data-avalanche/src/logging.rs`, `crates/data-avalanche/src/main.rs`, `crates/data-avalanche/src/run.rs`, `crates/data-avalanche/src/startup.rs`, `crates/data-avalanche/src/wiring.rs`, `crates/data-avalanche/tests/cli.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: `load` and the config generation (M1, M2), `Buckets` and state (M2),
  `PgConnector` (M5), `DeltaSink` (M4), `ClusterReconciler` (Task 11), `Metrics` (Task 7).
- Produces (`data-avalanche`):
  - `Cli`, `Command::Run(RunArgs { config, log_level, log_format })`.
  - `run(RunArgs) -> Result<(), Fatal>`, `Fatal { Config(String), Startup(String) }`,
    `exit_code`.
  - `Factory` (the `WorkerFactory` of PG + Delta), `delta_config(&Globals, &Pipeline)`.
  - The binary `data-avalanche`, which installs the aws-lc-rs crypto provider first
    (DA-DEP-5).

D-M6b.9 and D-M6b.12. The binary is the only crate that links `source-pg` and
`sink-delta` (roadmap §2). Startup is M2's sequence against real buckets, with the coordination
timeouts. Three command-line tests run the built binary (exit 2 for a missing file, a V-073 error
and a bad `--log-level`).

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -70,6 +70,7 @@ webpki-roots = "1"
 # The binary's control plane (M6b): metrics (roadmap §2.1).
 prometheus-client = "=0.25.1"
 gethostname = "1.1"
+clap = { version = "4.6.7", features = ["derive"] }
 
 # Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
 # CI runners. Workspace crates keep it.
```

Create `crates/data-avalanche/Cargo.toml`:

```toml
[package]
name = "data-avalanche"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "The data-avalanche process: the CLI, the wiring of sources, sinks and state, and signals."

[lints]
workspace = true

[dependencies]
chrono.workspace = true
clap.workspace = true
data-avalanche-cluster.workspace = true
data-avalanche-config.workspace = true
data-avalanche-pipeline.workspace = true
data-avalanche-sink-delta.workspace = true
data-avalanche-source-pg.workspace = true
data-avalanche-state.workspace = true
data-avalanche-storage.workspace = true
data-avalanche-telemetry.workspace = true
object_store.workspace = true
rustls.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["signal"] }
tracing.workspace = true
tracing-subscriber = { workspace = true, features = ["fmt", "json"] }

[dev-dependencies]
tempfile.workspace = true
```

Create `crates/data-avalanche/src/cli.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_takes_a_config_and_log_overrides() {
        let cli = Cli::try_parse_from([
            "data-avalanche",
            "run",
            "--config",
            "c.yaml",
            "--log-level",
            "debug",
            "--log-format",
            "pretty",
        ])
        .unwrap();
        let Command::Run(args) = cli.command;
        assert_eq!(args.config, PathBuf::from("c.yaml"));
        assert_eq!(args.log_level.as_deref(), Some("debug"));
        assert_eq!(args.log_format, Some(LogFormatArg::Pretty));
    }

    #[test]
    fn run_needs_a_config() {
        let e = Cli::try_parse_from(["data-avalanche", "run"]).unwrap_err();
        assert!(e.to_string().contains("--config"), "{e}");
    }
}
```

Create `crates/data-avalanche/src/lib.rs`:

```rust
mod cli;
mod logging;
mod run;
mod startup;
mod wiring;
```

Create `crates/data-avalanche/src/logging.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use data_avalanche_config::LogFilter;

    use super::*;

    fn args(level: Option<&str>, format: Option<LogFormatArg>) -> RunArgs {
        RunArgs {
            config: PathBuf::from("c.yaml"),
            log_level: level.map(str::to_owned),
            log_format: format,
        }
    }

    #[test]
    fn the_command_line_overrides_the_config() {
        let log = Log {
            level: LogFilter("info".into()),
            format: LogFormat::Json,
        };
        assert_eq!(
            settings(&log, &args(None, None)),
            ("info".to_owned(), LogFormat::Json)
        );
        assert_eq!(
            settings(&log, &args(Some("debug"), Some(LogFormatArg::Compact))),
            ("debug".to_owned(), LogFormat::Compact)
        );
    }
}
```

Create `crates/data-avalanche/src/run.rs`:

```rust
#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;

    use data_avalanche_config::{Origin, Pem, Secret, TlsIdentity};

    use super::*;

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

    pub(crate) fn config() -> ResolvedConfig {
        load_str(
            CONFIG,
            &BTreeMap::new(),
            &FingerprintKey::from_bytes([1; 32]),
        )
        .unwrap_or_else(|r| panic!("{}", render(&r)))
        .config
    }

    #[test]
    fn a_report_prints_a_line_per_diagnostic() {
        let r = load_str(
            "config_version: 1\nglobals: { state: { bucket: s } }\npipelines: {}\nbogus: 1\n",
            &BTreeMap::new(),
            &FingerprintKey::from_bytes([1; 32]),
        )
        .unwrap_err();
        let text = render(&r);
        assert!(text.starts_with("error "), "{text}");
        assert!(text.contains("bogus"), "{text}");
    }

    #[test]
    fn api_tls_is_refused_until_m9() {
        let mut c = config();
        assert_eq!(unsupported(&c), None);
        let key = FingerprintKey::from_bytes([1; 32]);
        let pem = || Pem {
            origin: Origin::Inline,
            text: Secret::new("-----BEGIN-----", &key),
        };
        c.globals.api.tls = Some(TlsIdentity {
            cert: pem(),
            key: pem(),
        });
        assert!(unsupported(&c).unwrap().contains("M9"));
    }

    #[test]
    fn the_controllers_take_the_cluster_timings() {
        let s = settings(&config());
        assert_eq!(s.timing.duration, Duration::from_secs(30));
        assert_eq!(s.timing.renewal_interval, Duration::from_secs(10));
        assert_eq!(s.drain_timeout, Duration::from_secs(45));
    }
}
```

Create `crates/data-avalanche/src/startup.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::tests::config;

    #[test]
    fn the_cloud_names_the_backend_and_the_state_sits_beside_the_tables() {
        let mut c = config();
        assert_eq!(backend(&c.globals), Backend::S3);
        c.globals.cloud = Cloud::Azure;
        c.globals.azure.account_name = Some("acct".into());
        assert_eq!(
            backend(&c.globals),
            Backend::Azure {
                account: Some("acct".into())
            }
        );
        let at = state_location(&c.pipelines["orders"]);
        assert_eq!(at, colocated_state("lake", "shop"));
    }
}
```

Create `crates/data-avalanche/src/wiring.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use data_avalanche_storage::Backend;

    use super::*;
    use crate::run::tests::config;

    #[test]
    fn the_delta_sink_takes_the_pipelines_sink_settings() {
        let c = config();
        let p = &c.pipelines["orders"];
        let d = delta_config(&c.globals, p).unwrap();
        assert_eq!(d.pipeline_id, "orders");
        assert_eq!(
            d.location,
            DeltaLocation::Bucket {
                backend: Backend::S3,
                bucket: "lake".into(),
                prefix: "shop".into(),
                options: vec![],
            }
        );
        assert_eq!(d.table.checkpoint_interval, 20);
        assert_eq!(d.table.log_retention, Duration::from_secs(30 * 24 * 3600));
        assert_eq!(d.table.timestamp_ntz_as, NtzAs::String);
        assert_eq!(d.merge_memory_limit, Some(64 << 20));
    }

    #[test]
    fn an_iceberg_pipeline_gets_no_parts() {
        let c = config();
        let mut p = c.pipelines["orders"].clone();
        p.sink.kind = SinkType::Iceberg;
        let e = Factory::new(&c.globals).build(&p).unwrap_err();
        assert!(e.contains("v2"), "{e}");
    }
}
```

Create `crates/data-avalanche/tests/cli.rs`:

```rust
//! The binary's command line, hermetic: what it refuses before touching storage.

use std::process::Command;

fn run(args: &[&str]) -> (Option<i32>, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_data-avalanche"))
        .args(args)
        .output()
        .expect("the binary runs");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn a_missing_config_file_exits_2_naming_it() {
    let (code, err) = run(&["run", "--config", "/nonexistent/da.yaml"]);
    assert_eq!(code, Some(2), "{err}");
    assert!(err.contains("/nonexistent/da.yaml"), "{err}");
}

#[test]
fn an_invalid_config_exits_2_with_its_rule() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.yaml");
    std::fs::write(
        &path,
        "config_version: 1\nglobals: { state: { bucket: s }, cluster: { lease_duration: 3s, lease_renewal_interval: 2s } }\npipelines: {}\n",
    )
    .unwrap();
    let (code, err) = run(&["run", "--config", path.to_str().unwrap()]);
    assert_eq!(code, Some(2), "{err}");
    assert!(err.contains("error V-"), "{err}");
}

#[test]
fn a_log_level_that_isnt_a_filter_exits_2() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ok.yaml");
    std::fs::write(
        &path,
        "config_version: 1\nglobals: { state: { bucket: s } }\npipelines: {}\n",
    )
    .unwrap();
    let (code, err) = run(&[
        "run",
        "--config",
        path.to_str().unwrap(),
        "--log-level",
        "info,[",
    ]);
    assert_eq!(code, Some(2), "{err}");
    assert!(err.contains("--log-level"), "{err}");
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche
```

Expected:

FAIL to compile (42 errors); the first:

```text
error: environment variable `CARGO_BIN_EXE_data-avalanche` not defined at compile time
error[E0433]: cannot find type `Command` in this scope
error[E0433]: cannot find type `PathBuf` in this scope
error[E0425]: cannot find type `LogFormatArg` in this scope
error[E0425]: cannot find type `RunArgs` in this scope
error[E0422]: cannot find struct, variant or union type `RunArgs` in this scope
… and 36 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche/src/cli.rs`:

```diff
diff --git a/crates/data-avalanche/src/cli.rs b/crates/data-avalanche/src/cli.rs
--- a/crates/data-avalanche/src/cli.rs
+++ b/crates/data-avalanche/src/cli.rs
@@ -1,3 +1,46 @@
+//! The command line.
+
+use std::path::PathBuf;
+
+use clap::{Args, Parser, Subcommand, ValueEnum};
+
+#[derive(Debug, Parser)]
+#[command(
+    name = "data-avalanche",
+    version,
+    about = "Change data capture from PostgreSQL into Delta Lake"
+)]
+pub struct Cli {
+    #[command(subcommand)]
+    pub command: Command,
+}
+
+#[derive(Debug, Subcommand)]
+pub enum Command {
+    /// Runs every pipeline of the config.
+    Run(RunArgs),
+}
+
+#[derive(Debug, Clone, Args)]
+pub struct RunArgs {
+    /// The YAML config file.
+    #[arg(long)]
+    pub config: PathBuf,
+    /// Overrides `globals.log.level` (a tracing filter: `info`, `data_avalanche=debug,warn`).
+    #[arg(long)]
+    pub log_level: Option<String>,
+    /// Overrides `globals.log.format`.
+    #[arg(long, value_enum)]
+    pub log_format: Option<LogFormatArg>,
+}
+
+#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
+pub enum LogFormatArg {
+    Json,
+    Compact,
+    Pretty,
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

Change `crates/data-avalanche/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche/src/lib.rs b/crates/data-avalanche/src/lib.rs
--- a/crates/data-avalanche/src/lib.rs
+++ b/crates/data-avalanche/src/lib.rs
@@ -1,5 +1,14 @@
+//! The data-avalanche process (roadmap §2): the only crate that links the PostgreSQL source and
+//! the Delta sink. `run` loads the config, joins the cluster anchor and the config generation in
+//! `globals.state` (the M2 startup sequence), then runs every pipeline under its lease until
+//! SIGTERM or SIGINT, and lets every lease go on the way out.
+
 mod cli;
 mod logging;
 mod run;
 mod startup;
 mod wiring;
+
+pub use cli::{Cli, Command, LogFormatArg, RunArgs};
+pub use run::{Fatal, run};
+pub use wiring::{Factory, delta_config};
```

Change `crates/data-avalanche/src/logging.rs`:

```diff
diff --git a/crates/data-avalanche/src/logging.rs b/crates/data-avalanche/src/logging.rs
--- a/crates/data-avalanche/src/logging.rs
+++ b/crates/data-avalanche/src/logging.rs
@@ -1,3 +1,45 @@
+//! Logs: `globals.log` (level, format), each overridden by the command line.
+
+use data_avalanche_config::{Log, LogFormat};
+use tracing_subscriber::EnvFilter;
+
+use crate::{LogFormatArg, RunArgs};
+
+/// The filter and the format in force: the command line's, else the config's.
+pub(crate) fn settings(log: &Log, args: &RunArgs) -> (String, LogFormat) {
+    let level = args
+        .log_level
+        .clone()
+        .unwrap_or_else(|| log.level.0.clone());
+    let format = match args.log_format {
+        Some(LogFormatArg::Json) => LogFormat::Json,
+        Some(LogFormatArg::Compact) => LogFormat::Compact,
+        Some(LogFormatArg::Pretty) => LogFormat::Pretty,
+        None => log.format,
+    };
+    (level, format)
+}
+
+/// Installs the process's subscriber; logs go to stderr.
+///
+/// # Errors
+/// A `--log-level` that isn't a filter (the config's was checked by V-017).
+pub(crate) fn init(log: &Log, args: &RunArgs) -> Result<(), String> {
+    let (level, format) = settings(log, args);
+    let filter = EnvFilter::builder()
+        .parse(&level)
+        .map_err(|e| format!("--log-level {level:?} isn't a log filter: {e}"))?;
+    let builder = tracing_subscriber::fmt()
+        .with_env_filter(filter)
+        .with_writer(std::io::stderr);
+    let installed = match format {
+        LogFormat::Json => builder.json().try_init(),
+        LogFormat::Compact => builder.compact().try_init(),
+        LogFormat::Pretty => builder.pretty().try_init(),
+    };
+    installed.map_err(|e| format!("cannot install the logger: {e}"))
+}
+
 #[cfg(test)]
 mod tests {
     use std::path::PathBuf;
```

Create `crates/data-avalanche/src/main.rs`:

```rust
//! `data-avalanche run --config <path>`.

use clap::Parser;
use data_avalanche::{Cli, Command};

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    // One rustls provider before any TLS (DA-DEP-5): two are linked.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("data-avalanche: cannot start the async runtime: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let Command::Run(args) = cli.command;
    match runtime.block_on(data_avalanche::run(args)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("data-avalanche: {e}");
            std::process::ExitCode::from(e.exit_code())
        }
    }
}
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -1,3 +1,166 @@
+//! `data-avalanche run`.
+
+use std::sync::Arc;
+use std::time::{Duration, SystemTime};
+
+use data_avalanche_cluster::{
+    ClusterReconciler, ControllerDeps, ControllerSettings, LeaseTiming, node_identity,
+};
+use data_avalanche_config::{
+    Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
+};
+use data_avalanche_pipeline::{Backoff, Tuning};
+use data_avalanche_state::Leases;
+use data_avalanche_telemetry::Metrics;
+use thiserror::Error;
+use tokio::sync::Semaphore;
+
+use crate::startup::{Buckets, join};
+use crate::{Factory, RunArgs, logging};
+
+/// Why the process stops before running its pipelines.
+#[derive(Debug, Error)]
+pub enum Fatal {
+    /// The config, or the command line, is refused: exit 2.
+    #[error("{0}")]
+    Config(String),
+    /// Storage, the anchor or the generation couldn't be reached or read: exit 1.
+    #[error("{0}")]
+    Startup(String),
+}
+
+impl Fatal {
+    pub fn exit_code(&self) -> u8 {
+        match self {
+            Self::Config(_) => 2,
+            Self::Startup(_) => 1,
+        }
+    }
+}
+
+/// One diagnostic as a line: `error V-nnn at <path> (line:col): <message>`.
+pub(crate) fn line(d: &Diagnostic) -> String {
+    let severity = match d.severity {
+        Severity::Error => "error",
+        Severity::Warning => "warning",
+    };
+    let at = match (&d.path, &d.loc) {
+        (p, Some(loc)) if !p.is_empty() => format!(" at {p} ({loc})"),
+        (p, None) if !p.is_empty() => format!(" at {p}"),
+        (_, Some(loc)) => format!(" ({loc})"),
+        _ => String::new(),
+    };
+    format!("{severity} {}{at}: {}", d.rule, d.message)
+}
+
+/// A config report, a line per diagnostic.
+pub(crate) fn render(report: &Report) -> String {
+    report.iter().map(line).collect::<Vec<_>>().join("\n")
+}
+
+/// What this build doesn't serve yet, refused before anything starts.
+pub(crate) fn unsupported(config: &ResolvedConfig) -> Option<String> {
+    config.globals.api.tls.as_ref().map(|_| {
+        "globals.api.tls: the API listener serves TLS from M9; this build serves plain HTTP only"
+            .to_owned()
+    })
+}
+
+/// The controllers' timings from `globals.cluster`.
+pub(crate) fn settings(config: &ResolvedConfig) -> ControllerSettings {
+    let c = &config.globals.cluster;
+    ControllerSettings {
+        timing: LeaseTiming::new(c),
+        drain_timeout: c.drain_timeout,
+        restart: Backoff::default(),
+        start_jitter: Duration::from_millis(500),
+        tuning: Tuning::default(),
+    }
+}
+
+/// Runs every pipeline until SIGTERM or SIGINT.
+///
+/// # Errors
+/// [`Fatal`]: the config is refused, or `globals.state` can't be joined.
+pub async fn run(args: RunArgs) -> Result<(), Fatal> {
+    let started = SystemTime::now();
+    let text = std::fs::read_to_string(&args.config).map_err(|e| {
+        Fatal::Config(format!(
+            "cannot read config file {}: {e}",
+            args.config.display()
+        ))
+    })?;
+    // 1. A first load with a throwaway key: only globals.state and the cluster settings matter.
+    let first = load_str(&text, &ProcessEnv, &FingerprintKey::random())
+        .map_err(|r| Fatal::Config(render(&r)))?;
+    logging::init(&first.config.globals.log, &args).map_err(Fatal::Config)?;
+    for w in &first.warnings {
+        tracing::warn!("config: {}", line(w));
+    }
+    if let Some(why) = unsupported(&first.config) {
+        return Err(Fatal::Config(why));
+    }
+    let holder = node_identity(&first.config.globals.cluster, started).map_err(Fatal::Config)?;
+    let buckets = Buckets::new(&first.config.globals);
+    let joined = join(&buckets, &holder.node_id, &text, &first.config).await?;
+    let config = joined.config;
+    if let Some(why) = unsupported(&config) {
+        return Err(Fatal::Config(why));
+    }
+    tracing::info!(
+        node = %holder.node_id,
+        incarnation = holder.incarnation,
+        pipelines = config.pipelines.len(),
+        "joined the cluster"
+    );
+
+    let metrics = Metrics::new();
+    let c = &config.globals.cluster;
+    let deps = ControllerDeps {
+        leases: Leases::new(joined.globals),
+        holder,
+        factory: Arc::new(Factory::new(&config.globals)),
+        metrics: metrics.clone(),
+        settings: settings(&config),
+        starts: Arc::new(Semaphore::new(
+            usize::try_from(c.max_concurrent_takeovers).unwrap_or(usize::MAX),
+        )),
+    };
+    let mut pipelines = Vec::new();
+    for p in config.pipelines.values() {
+        pipelines.push((p.clone(), buckets.pipeline_docs(p)?));
+    }
+    let reconciler = ClusterReconciler::start(pipelines, &deps);
+    shutdown_signal().await;
+    tracing::info!("shutting down: draining every pipeline and releasing its lease");
+    reconciler.shutdown().await;
+    tracing::info!("stopped");
+    Ok(())
+}
+
+async fn shutdown_signal() {
+    #[cfg(unix)]
+    {
+        use tokio::signal::unix::{SignalKind, signal};
+        match signal(SignalKind::terminate()) {
+            Ok(mut term) => {
+                tokio::select! {
+                    _ = term.recv() => {}
+                    _ = tokio::signal::ctrl_c() => {}
+                }
+            }
+            Err(e) => {
+                tracing::warn!("SIGTERM can't be watched ({e}); only SIGINT stops the process");
+                let _ = tokio::signal::ctrl_c().await;
+            }
+        }
+    }
+    #[cfg(not(unix))]
+    {
+        let _ = tokio::signal::ctrl_c().await;
+    }
+}
+
 #[cfg(test)]
 pub(crate) mod tests {
     use std::collections::BTreeMap;
```

Change `crates/data-avalanche/src/startup.rs`:

```diff
diff --git a/crates/data-avalanche/src/startup.rs b/crates/data-avalanche/src/startup.rs
--- a/crates/data-avalanche/src/startup.rs
+++ b/crates/data-avalanche/src/startup.rs
@@ -1,3 +1,152 @@
+//! The buckets, and the M2 startup sequence in `globals.state` (config crate docs, "Startup and
+//! reload"): the cluster anchor, this node's settings against it, the config generation.
+
+use std::collections::HashMap;
+use std::sync::{Arc, Mutex, PoisonError};
+use std::time::Duration;
+
+use chrono::Utc;
+use data_avalanche_config::{
+    Cloud, ClusterAnchor, FingerprintKey, Globals, LockBackend, Pipeline, ResolvedConfig,
+    StateLocation, check_cluster_anchor, load_str,
+};
+use data_avalanche_state::{
+    AnchorDoc, ConfigGenerations, DocStore, HashAlgo, PipelineDocs, establish_anchor,
+};
+use data_avalanche_storage::{
+    Backend, BucketSpec, Location, ObjectStoreVersioned, Timeouts, colocated_state, explicit_state,
+    globals_state, open_bucket,
+};
+use object_store::ObjectStore;
+
+use crate::run::Fatal;
+
+/// The store `globals.cloud` names.
+pub(crate) fn backend(globals: &Globals) -> Backend {
+    match globals.cloud {
+        Cloud::Aws => Backend::S3,
+        Cloud::Google => Backend::Gcs,
+        Cloud::Azure => Backend::Azure {
+            account: globals.azure.account_name.clone(),
+        },
+    }
+}
+
+/// Where a pipeline's state lives (config §10).
+pub(crate) fn state_location(p: &Pipeline) -> Location {
+    match &p.state {
+        StateLocation::CoLocated => colocated_state(&p.sink.bucket, &p.sink.prefix),
+        StateLocation::Explicit {
+            bucket,
+            parent_prefix,
+            prefix,
+        } => explicit_state(bucket, parent_prefix.as_deref(), prefix),
+    }
+}
+
+/// The process's buckets, each opened once. Credentials and endpoints come from the cloud's
+/// standard environment variables.
+#[derive(Debug)]
+pub(crate) struct Buckets {
+    backend: Backend,
+    opened: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
+}
+
+impl Buckets {
+    pub fn new(globals: &Globals) -> Self {
+        Self {
+            backend: backend(globals),
+            opened: Mutex::new(HashMap::new()),
+        }
+    }
+
+    /// The documents at `at`, with the coordination timeouts (a hung store surfaces within half
+    /// a default lease).
+    pub fn docs(&self, at: &Location) -> Result<DocStore, Fatal> {
+        let mut opened = self.opened.lock().unwrap_or_else(PoisonError::into_inner);
+        let store = match opened.get(&at.bucket) {
+            Some(s) => Arc::clone(s),
+            None => {
+                let store = open_bucket(&BucketSpec {
+                    backend: self.backend.clone(),
+                    bucket: at.bucket.clone(),
+                    options: vec![],
+                    timeouts: Timeouts::COORDINATION,
+                })
+                .map_err(|e| Fatal::Startup(e.to_string()))?;
+                opened.insert(at.bucket.clone(), Arc::clone(&store));
+                store
+            }
+        };
+        let versioned = ObjectStoreVersioned::new(store, &at.root)
+            .map_err(|e| Fatal::Startup(format!("state at {}/{}: {e}", at.bucket, at.root)))?;
+        Ok(DocStore::new(Arc::new(versioned)))
+    }
+
+    pub fn pipeline_docs(&self, p: &Pipeline) -> Result<PipelineDocs, Fatal> {
+        Ok(PipelineDocs::new(self.docs(&state_location(p))?))
+    }
+}
+
+/// The running config and the `globals.state` documents, once this node has joined.
+#[derive(Debug)]
+pub(crate) struct Joined {
+    pub config: ResolvedConfig,
+    pub globals: DocStore,
+}
+
+/// Steps 2–5 of the startup sequence; `first` is the config loaded with a throwaway key.
+pub(crate) async fn join(
+    buckets: &Buckets,
+    node: &str,
+    text: &str,
+    first: &ResolvedConfig,
+) -> Result<Joined, Fatal> {
+    let g = &first.globals;
+    let globals = buckets.docs(&globals_state(&g.state.bucket, &g.state.prefix))?;
+    let proposal = AnchorDoc {
+        tokens: g.cluster.tokens,
+        lease_duration_ms: u64::try_from(g.cluster.lease_duration.as_millis()).unwrap_or(u64::MAX),
+        lock_backend: g.cluster.lock_backend.to_string(),
+        hash_algo: HashAlgo::Xxh3,
+        fingerprint_key: *FingerprintKey::random().as_bytes(),
+        established_by: node.to_owned(),
+        established_at: Utc::now(),
+    };
+    let anchor = establish_anchor(&globals, &proposal)
+        .await
+        .map_err(|e| Fatal::Startup(format!("the cluster anchor: {e}")))?
+        .doc;
+    let established = ClusterAnchor {
+        tokens: anchor.tokens,
+        lock_backend: anchor
+            .lock_backend
+            .parse::<LockBackend>()
+            .map_err(|e| Fatal::Startup(format!("the cluster anchor: {e}")))?,
+        lease_duration: Duration::from_millis(anchor.lease_duration_ms),
+    };
+    let report = check_cluster_anchor(g, &established);
+    if !report.is_ok() {
+        return Err(Fatal::Config(crate::run::render(&report)));
+    }
+    let (generation, created) = ConfigGenerations::new(globals.clone())
+        .bootstrap_or_adopt(text, node, Utc::now())
+        .await
+        .map_err(|e| Fatal::Startup(format!("the config generation: {e}")))?;
+    if !created && generation.text != text {
+        tracing::warn!(
+            generation = generation.pointer.doc.generation,
+            "the cluster runs config generation {} from globals.state, not this node's file",
+            generation.pointer.doc.generation
+        );
+    }
+    let key = FingerprintKey::from_bytes(anchor.fingerprint_key);
+    let config = load_str(&generation.text, &data_avalanche_config::ProcessEnv, &key)
+        .map_err(|r| Fatal::Config(crate::run::render(&r)))?
+        .config;
+    Ok(Joined { config, globals })
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

Change `crates/data-avalanche/src/wiring.rs`:

```diff
diff --git a/crates/data-avalanche/src/wiring.rs b/crates/data-avalanche/src/wiring.rs
--- a/crates/data-avalanche/src/wiring.rs
+++ b/crates/data-avalanche/src/wiring.rs
@@ -1,3 +1,76 @@
+//! Each pipeline's source and sink, built for every worker run.
+
+use std::sync::Arc;
+
+use data_avalanche_cluster::{Parts, WorkerFactory};
+use data_avalanche_config::{Globals, Pipeline, SinkType, TimestampNtzAs};
+use data_avalanche_pipeline::NoHooks;
+use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig, NtzAs, TableDefaults};
+use data_avalanche_source_pg::PgConnector;
+
+use crate::startup::backend;
+
+/// The Delta sink of `p` (`sink.delta`, config §9.4.3): tables at `<bucket>/<prefix>/…`.
+///
+/// # Errors
+/// The pipeline's sink isn't Delta.
+pub fn delta_config(globals: &Globals, p: &Pipeline) -> Result<DeltaSinkConfig, String> {
+    let Some(d) = &p.sink.delta else {
+        return Err(format!("pipeline {}: the sink isn't Delta", p.name));
+    };
+    Ok(DeltaSinkConfig {
+        pipeline_id: p.id.clone(),
+        location: DeltaLocation::Bucket {
+            backend: backend(globals),
+            bucket: p.sink.bucket.clone(),
+            prefix: p.sink.prefix.clone(),
+            options: vec![],
+        },
+        table: TableDefaults {
+            checkpoint_interval: d.checkpoint_interval,
+            log_retention: d.log_retention,
+            timestamp_ntz_as: match d.timestamp_ntz_as {
+                TimestampNtzAs::TimestampNtz => NtzAs::TimestampNtz,
+                TimestampNtzAs::Timestamp => NtzAs::Timestamp,
+                TimestampNtzAs::String => NtzAs::String,
+            },
+            table_properties: d.table_properties.clone(),
+        },
+        merge_memory_limit: d
+            .merge_memory_limit
+            .map(|b| usize::try_from(b).unwrap_or(usize::MAX)),
+    })
+}
+
+/// Builds the PostgreSQL source and the Delta sink.
+#[derive(Debug)]
+pub struct Factory {
+    globals: Globals,
+}
+
+impl Factory {
+    pub fn new(globals: &Globals) -> Self {
+        Self {
+            globals: globals.clone(),
+        }
+    }
+}
+
+impl WorkerFactory for Factory {
+    fn build(&self, p: &Pipeline) -> Result<Parts, String> {
+        if p.sink.kind == SinkType::Iceberg {
+            return Err("the Iceberg sink ships in v2 (D-V1.4); other pipelines run".into());
+        }
+        let source = PgConnector::new(p).map_err(|e| e.to_string())?;
+        let sink = DeltaSink::new(delta_config(&self.globals, p)?);
+        Ok(Parts {
+            source: Arc::new(source),
+            sink: Arc::new(sink),
+            hooks: Arc::new(NoHooks),
+        })
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use std::time::Duration;
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche
```

Expected:

PASS:

```text
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 7.21s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(data-avalanche): the binary: run --config, log overrides, the startup sequence, every pipeline under its lease until a signal"
```


### Task 13: The binary: the HTTP listener

**Files:**
- Create: `crates/data-avalanche/src/http.rs`
- Modify: `Cargo.toml`, `crates/data-avalanche/Cargo.toml`, `crates/data-avalanche/src/lib.rs`, `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: `Metrics::encode`, `CONTENT_TYPE` (Task 7), `run` (Task 12).
- Produces: `/health/live` (200 once bound), `/health/ready` (200 once started, 503 otherwise),
  `/metrics` (OpenMetrics text), 404 elsewhere; bound before startup, shut down after the
  reconciler.

D-M6b.10. The listener comes up before the slow part of startup, so a liveness probe
answers while the anchor and the config generation are read.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -71,6 +71,7 @@ webpki-roots = "1"
 prometheus-client = "=0.25.1"
 gethostname = "1.1"
 clap = { version = "4.6.7", features = ["derive"] }
+axum = { version = "0.8.9", default-features = false, features = ["http1", "tokio"] }
 
 # Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
 # CI runners. Workspace crates keep it.
```

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -10,6 +10,7 @@ description = "The data-avalanche process: the CLI, the wiring of sources, sinks
 workspace = true
 
 [dependencies]
+axum.workspace = true
 chrono.workspace = true
 clap.workspace = true
 data-avalanche-cluster.workspace = true
@@ -23,7 +24,7 @@ data-avalanche-telemetry.workspace = true
 object_store.workspace = true
 rustls.workspace = true
 thiserror.workspace = true
-tokio = { workspace = true, features = ["signal"] }
+tokio = { workspace = true, features = ["signal", "net"] }
 tracing.workspace = true
 tracing-subscriber = { workspace = true, features = ["fmt", "json"] }
 
```

Create `crates/data-avalanche/src/http.rs`:

```rust
#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// A plain HTTP/1.1 GET: the status code, the headers, the body.
    async fn get(addr: std::net::SocketAddr, path: &str) -> (u16, String, String) {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        let (head, body) = out.split_once("\r\n\r\n").unwrap();
        let code = head[9..12].parse().unwrap();
        (code, head.to_owned(), body.to_owned())
    }

    #[tokio::test]
    async fn the_listener_answers_health_and_metrics() {
        let metrics = Metrics::new();
        metrics.set_cluster_members(1);
        let ready = Arc::new(AtomicBool::new(false));
        let listener = bind(&HostPort {
            host: "127.0.0.1".into(),
            port: 0,
        })
        .await
        .unwrap();
        let addr = listener.local_addr().unwrap();
        let app = router(Http {
            metrics,
            ready: Arc::clone(&ready),
        });
        tokio::spawn(async move { axum::serve(listener, app).await });

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
        assert_eq!(get(addr, "/v1/pipelines").await.0, 404, "the API is M9's");
    }
}
```

Change `crates/data-avalanche/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche/src/lib.rs b/crates/data-avalanche/src/lib.rs
--- a/crates/data-avalanche/src/lib.rs
+++ b/crates/data-avalanche/src/lib.rs
@@ -8,6 +8,7 @@ mod logging;
 mod run;
 mod startup;
 mod wiring;
+mod http;
 
 pub use cli::{Cli, Command, LogFormatArg, RunArgs};
 pub use run::{Fatal, run};
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche
```

Expected:

FAIL to compile (9 errors); the first:

```text
error[E0433]: cannot find type `Arc` in this scope
error[E0433]: cannot find type `AtomicBool` in this scope
error[E0422]: cannot find struct, variant or union type `HostPort` in this scope
error[E0422]: cannot find struct, variant or union type `Http` in this scope
error[E0433]: cannot find type `Ordering` in this scope
error[E0433]: cannot find type `Metrics` in this scope
… and 3 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche/src/http.rs`:

```diff
diff --git a/crates/data-avalanche/src/http.rs b/crates/data-avalanche/src/http.rs
--- a/crates/data-avalanche/src/http.rs
+++ b/crates/data-avalanche/src/http.rs
@@ -1,3 +1,65 @@
+//! The minimal listener on `globals.api.address`: liveness, readiness and the metrics, all
+//! unauthenticated (System-Overview §Observability, API_RBAC §2). The `/v1` API, its auth and TLS
+//! arrive with M9.
+
+use std::sync::Arc;
+use std::sync::atomic::{AtomicBool, Ordering};
+
+use axum::Router;
+use axum::extract::State;
+use axum::http::{StatusCode, header};
+use axum::response::IntoResponse;
+use axum::routing::get;
+use data_avalanche_config::HostPort;
+use data_avalanche_telemetry::{CONTENT_TYPE, Metrics};
+use tokio::net::TcpListener;
+
+use crate::run::Fatal;
+
+/// What the routes read.
+#[derive(Debug, Clone)]
+pub(crate) struct Http {
+    pub metrics: Metrics,
+    /// Startup is done and every pipeline's controller runs (not: every pipeline is healthy).
+    pub ready: Arc<AtomicBool>,
+}
+
+pub(crate) fn router(http: Http) -> Router {
+    Router::new()
+        .route("/health/live", get(|| async { "live\n" }))
+        .route("/health/ready", get(ready))
+        .route("/metrics", get(metrics))
+        .with_state(http)
+}
+
+async fn ready(State(http): State<Http>) -> impl IntoResponse {
+    if http.ready.load(Ordering::SeqCst) {
+        (StatusCode::OK, "ready\n")
+    } else {
+        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
+    }
+}
+
+async fn metrics(State(http): State<Http>) -> impl IntoResponse {
+    (
+        [(header::CONTENT_TYPE, CONTENT_TYPE)],
+        http.metrics.encode(),
+    )
+}
+
+/// # Errors
+/// The address can't be bound.
+pub(crate) async fn bind(address: &HostPort) -> Result<TcpListener, Fatal> {
+    TcpListener::bind((address.host.as_str(), address.port))
+        .await
+        .map_err(|e| {
+            Fatal::Startup(format!(
+                "globals.api.address {}:{} can't be bound: {e}",
+                address.host, address.port
+            ))
+        })
+}
+
 #[cfg(test)]
 mod tests {
     use tokio::io::{AsyncReadExt, AsyncWriteExt};
```

Change `crates/data-avalanche/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche/src/lib.rs b/crates/data-avalanche/src/lib.rs
--- a/crates/data-avalanche/src/lib.rs
+++ b/crates/data-avalanche/src/lib.rs
@@ -4,11 +4,11 @@
 //! SIGTERM or SIGINT, and lets every lease go on the way out.
 
 mod cli;
+mod http;
 mod logging;
 mod run;
 mod startup;
 mod wiring;
-mod http;
 
 pub use cli::{Cli, Command, LogFormatArg, RunArgs};
 pub use run::{Fatal, run};
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -1,6 +1,8 @@
 //! `data-avalanche run`.
 
+use std::future::IntoFuture;
 use std::sync::Arc;
+use std::sync::atomic::{AtomicBool, Ordering};
 use std::time::{Duration, SystemTime};
 
 use data_avalanche_cluster::{
@@ -13,8 +15,9 @@ use data_avalanche_pipeline::{Backoff, Tuning};
 use data_avalanche_state::Leases;
 use data_avalanche_telemetry::Metrics;
 use thiserror::Error;
-use tokio::sync::Semaphore;
+use tokio::sync::{Semaphore, oneshot};
 
+use crate::http::{self, Http};
 use crate::startup::{Buckets, join};
 use crate::{Factory, RunArgs, logging};
 
@@ -100,6 +103,24 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     if let Some(why) = unsupported(&first.config) {
         return Err(Fatal::Config(why));
     }
+    // The listener answers from here: live at once, ready once every controller runs.
+    let metrics = Metrics::new();
+    let ready = Arc::new(AtomicBool::new(false));
+    let listener = http::bind(&first.config.globals.api.address).await?;
+    let (stop_http, http_stopped) = oneshot::channel::<()>();
+    let server = tokio::spawn(
+        axum::serve(
+            listener,
+            http::router(Http {
+                metrics: metrics.clone(),
+                ready: Arc::clone(&ready),
+            }),
+        )
+        .with_graceful_shutdown(async {
+            let _ = http_stopped.await;
+        })
+        .into_future(),
+    );
     let holder = node_identity(&first.config.globals.cluster, started).map_err(Fatal::Config)?;
     let buckets = Buckets::new(&first.config.globals);
     let joined = join(&buckets, &holder.node_id, &text, &first.config).await?;
@@ -114,7 +135,6 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         "joined the cluster"
     );
 
-    let metrics = Metrics::new();
     let c = &config.globals.cluster;
     let deps = ControllerDeps {
         leases: Leases::new(joined.globals),
@@ -131,9 +151,13 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         pipelines.push((p.clone(), buckets.pipeline_docs(p)?));
     }
     let reconciler = ClusterReconciler::start(pipelines, &deps);
+    ready.store(true, Ordering::SeqCst);
     shutdown_signal().await;
     tracing::info!("shutting down: draining every pipeline and releasing its lease");
+    ready.store(false, Ordering::SeqCst);
     reconciler.shutdown().await;
+    let _ = stop_http.send(());
+    let _ = server.await;
     tracing::info!("stopped");
     Ok(())
 }
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche
```

Expected:

PASS:

```text
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 7.36s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(data-avalanche): the minimal HTTP listener: /health/live, /health/ready, /metrics"
```


### Task 14: E2E: the harness, crash points behind `e2e-hooks`, the smoke scenario

**Files:**
- Create: `crates/data-avalanche/src/crash.rs`, `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/main.rs`, `crates/data-avalanche/tests/e2e/smoke.rs`
- Modify: `.github/workflows/ci.yml`, `crates/data-avalanche/Cargo.toml`, `crates/data-avalanche/src/lib.rs`, `crates/data-avalanche/src/run.rs`, `crates/data-avalanche/src/wiring.rs`

**Interfaces:**
- Consumes: the binary (Tasks 12–13), `Hooks`/`HookPoint` (M6a), testkit's RustFS
  endpoints.
- Produces:
  - The feature `e2e-hooks` and `crash::CrashAt` (`DATA_AVALANCHE_E2E_CRASH=<point>[:<n>]`,
    with points `sink_committed`, `checkpoint_persisted`, `chunk_emitted`, `schemas_cached`).
  - `tests/e2e` (`required-features = ["e2e-hooks"]`) and its harness: `E2e::new(test, ddl,
    Options)`, `start`, `Proc::{ready, metric, terminate, exit}`, the lake reader.
  - The `ci.yml` step "Slice-1 E2E".

Q-M6b-5. The harness gives each scenario its own schema, slot, publication and RustFS
prefix, starts the built binary with only the `AWS_*` variables RustFS needs, and prints the
process's log when a test fails. The smoke scenario covers a snapshot, the stream, a SIGTERM
that releases the lease, and a restart at the next epoch.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -28,5 +28,18 @@ tokio = { workspace = true, features = ["signal", "net"] }
 tracing.workspace = true
 tracing-subscriber = { workspace = true, features = ["fmt", "json"] }
 
+[features]
+# E2E crash points (Q-M6b-5): DATA_AVALANCHE_E2E_CRASH aborts the process at a worker hook point.
+e2e-hooks = []
+
 [dev-dependencies]
+arrow-array.workspace = true
+data-avalanche-core.workspace = true
+data-avalanche-testkit.workspace = true
 tempfile.workspace = true
+tokio-postgres.workspace = true
+
+[[test]]
+name = "e2e"
+path = "tests/e2e/main.rs"
+required-features = ["e2e-hooks"]
```

Create `crates/data-avalanche/src/crash.rs`:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::TableRef;

    use super::*;

    #[test]
    fn a_crash_point_is_due_at_its_nth_pass() {
        let c = CrashAt::parse("sink_committed:2").unwrap();
        let commit = HookPoint::SinkCommitted {
            table: TableRef::new("public", "a"),
        };
        assert!(!c.due(&HookPoint::SchemasCached));
        assert!(!c.due(&commit));
        assert!(c.due(&commit));
        assert!(!c.due(&commit), "once");
        assert!(
            CrashAt::parse("schemas_cached")
                .unwrap()
                .due(&HookPoint::SchemasCached)
        );
        assert!(CrashAt::parse("nowhere").is_err());
        assert!(CrashAt::parse("chunk_emitted:0").is_err());
    }
}
```

Change `crates/data-avalanche/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche/src/lib.rs b/crates/data-avalanche/src/lib.rs
--- a/crates/data-avalanche/src/lib.rs
+++ b/crates/data-avalanche/src/lib.rs
@@ -9,6 +9,8 @@ mod logging;
 mod run;
 mod startup;
 mod wiring;
+#[cfg(feature = "e2e-hooks")]
+pub mod crash;
 
 pub use cli::{Cli, Command, LogFormatArg, RunArgs};
 pub use run::{Fatal, run};
```

Change `crates/data-avalanche/src/wiring.rs`:

```diff
diff --git a/crates/data-avalanche/src/wiring.rs b/crates/data-avalanche/src/wiring.rs
--- a/crates/data-avalanche/src/wiring.rs
+++ b/crates/data-avalanche/src/wiring.rs
@@ -106,7 +106,7 @@ mod tests {
         let c = config();
         let mut p = c.pipelines["orders"].clone();
         p.sink.kind = SinkType::Iceberg;
-        let e = Factory::new(&c.globals).build(&p).unwrap_err();
+        let e = Factory::new(&c.globals).unwrap().build(&p).unwrap_err();
         assert!(e.contains("v2"), "{e}");
     }
 }
```

Create `crates/data-avalanche/tests/e2e/harness.rs`:

```rust
//! One E2E test's world: a schema `e2e_<test>` with its slot and publication, a RustFS prefix for
//! `globals.state` and the lake, a config file, and the binary as a child process.

use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use data_avalanche_core::TableRef;
use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig};
use data_avalanche_state::{DocStore, LeaseDoc, Leases, Versioned};
use data_avalanche_storage::{Backend, ObjectStoreVersioned};
use data_avalanche_testkit::endpoints::{
    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
};
use data_avalanche_testkit::{delete_prefix, stores, unique_prefix};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::{Client, NoTls};

/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418).
pub fn pg_port() -> u16 {
    std::env::var("DATA_AVALANCHE_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55418)
}

pub async fn admin() -> Client {
    let dsn = format!(
        "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
        pg_port()
    );
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect to test PostgreSQL");
    tokio::spawn(conn);
    client
}

/// Polls `cond` every 100 ms until it holds, for at most `within`.
pub async fn eventually<F, Fut>(what: &str, within: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let until = Instant::now() + within;
    while !cond().await {
        assert!(Instant::now() < until, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free port")
        .port()
}

/// What a test sets beyond the defaults (YAML flow entries, each starting with `, `).
#[derive(Debug, Clone, Default)]
pub struct Options<'a> {
    /// `tables` entries, e.g. `, snapshots: { mode: never }`.
    pub tables: &'a str,
    /// `sink` entries, e.g. `, flush: { rows: 10 }`.
    pub sink: &'a str,
    /// `source.postgres` entries.
    pub postgres: &'a str,
}

pub struct E2e {
    pub test: String,
    pub schema: String,
    prefix: String,
    port: u16,
    dir: tempfile::TempDir,
    pub admin: Client,
}

impl E2e {
    /// Drops what an earlier run of `test` left in PostgreSQL, creates schema `e2e_<test>` and
    /// runs `ddl` in it, and writes the config.
    pub async fn new(test: &str, ddl: &str, o: Options<'_>) -> Self {
        let admin = admin().await;
        let schema = format!("e2e_{test}");
        drop_slot(&admin, &schema).await;
        admin
            .batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS {schema}_pub;
                 DROP SCHEMA IF EXISTS {schema} CASCADE;
                 CREATE SCHEMA {schema};
                 SET search_path = {schema};
                 {ddl}
                 RESET search_path;"
            ))
            .await
            .expect("test DDL");
        let e = Self {
            test: test.to_owned(),
            schema,
            prefix: unique_prefix(&format!("e2e-{test}")),
            port: free_port(),
            dir: tempfile::tempdir().expect("a temp dir"),
            admin,
        };
        std::fs::write(e.config_path(), e.yaml(&o)).expect("the config file");
        e
    }

    pub fn config_path(&self) -> PathBuf {
        self.dir.path().join("config.yaml")
    }

    fn yaml(&self, o: &Options<'_>) -> String {
        format!(
            r#"config_version: 1
globals:
  cloud: aws
  state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
  api: {{ address: "127.0.0.1:{port}" }}
  log: {{ level: "info,buoyant_kernel=warn", format: compact }}
  cluster: {{ lease_duration: 6s, lease_renewal_interval: 2s, drain_timeout: 30s }}
pipelines:
  {test}:
    id: {test}
    source:
      host: 127.0.0.1
      port: {pg}
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: {{ publication: {schema}_pub, slot: {schema}{postgres} }}
    tables: {{ include: [{schema}.*]{tables} }}
    sink: {{ type: delta, bucket: {bucket}, prefix: {prefix}/lake, delta: {{}}, flush: {{ seconds: 1 }}{sink} }}
"#,
            bucket = TEST_BUCKET,
            prefix = self.prefix,
            port = self.port,
            test = self.test,
            pg = pg_port(),
            schema = self.schema,
            postgres = o.postgres,
            tables = o.tables,
            sink = o.sink,
        )
    }

    /// Starts the binary; `env` adds variables (a crash point).
    pub fn start(&self, env: &[(&str, &str)]) -> Proc {
        let log = self.dir.path().join(format!(
            "run-{}.log",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let out = std::fs::File::create(&log).expect("the log file");
        let err = out.try_clone().expect("the log file");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_data-avalanche"));
        cmd.args(["run", "--config"])
            .arg(self.config_path())
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("AWS_ACCESS_KEY_ID", RUSTFS_ACCESS_KEY)
            .env("AWS_SECRET_ACCESS_KEY", RUSTFS_SECRET_KEY)
            .env("AWS_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL", RUSTFS_URL)
            .env("AWS_ALLOW_HTTP", "true")
            .env("RUST_BACKTRACE", "1")
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err);
        for (k, v) in env {
            cmd.env(k, v);
        }
        Proc {
            child: cmd.spawn().expect("the binary starts"),
            port: self.port,
            log,
        }
    }

    pub fn table(&self, name: &str) -> TableRef {
        TableRef::new(&self.schema, name)
    }

    /// A reader of the pipeline's Delta tables on RustFS.
    pub fn lake(&self) -> DeltaSink {
        let opt = |k: &str, v: &str| (k.to_owned(), v.to_owned());
        DeltaSink::new(DeltaSinkConfig {
            pipeline_id: self.test.clone(),
            location: DeltaLocation::Bucket {
                backend: Backend::S3,
                bucket: TEST_BUCKET.into(),
                prefix: format!("{}/lake", self.prefix),
                options: vec![
                    opt("endpoint", RUSTFS_URL),
                    opt("allow_http", "true"),
                    opt("region", "us-east-1"),
                    opt("access_key_id", RUSTFS_ACCESS_KEY),
                    opt("secret_access_key", RUSTFS_SECRET_KEY),
                ],
            },
            table: Default::default(),
            merge_memory_limit: None,
        })
    }

    /// The table's rows in the lake, `None` before it exists.
    pub async fn content(&self, table: &str) -> Option<RecordBatch> {
        self.lake()
            .content(&self.table(table))
            .await
            .expect("the lake reads")
    }

    /// The table's `id`s in the lake, in key order.
    pub async fn ids(&self, table: &str) -> Vec<i64> {
        match self.content(table).await {
            None => vec![],
            Some(b) => {
                let ids = b
                    .column_by_name("id")
                    .expect("an id column")
                    .as_primitive::<Int64Type>();
                assert_eq!(ids.null_count(), 0);
                ids.values().to_vec()
            }
        }
    }

    /// The source table's `id`s, in key order.
    pub async fn source_ids(&self, table: &str) -> Vec<i64> {
        self.admin
            .query(
                &format!("SELECT id FROM {}.{table} ORDER BY id", self.schema),
                &[],
            )
            .await
            .expect("the source reads")
            .iter()
            .map(|r| r.get::<_, i64>(0))
            .collect()
    }

    /// Waits until the lake's `table` has exactly the source's ids.
    pub async fn converged(&self, table: &str) {
        let want = self.source_ids(table).await;
        let what = format!("{table} has the source's {} rows", want.len());
        eventually(&what, Duration::from_secs(60), async || {
            self.ids(table).await == want
        })
        .await;
    }

    pub async fn exec(&self, sql: &str) {
        self.admin
            .batch_execute(&format!(
                "SET search_path = {}; {sql}; RESET search_path;",
                self.schema
            ))
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// The pipeline's lease in `globals.state`.
    pub async fn lease(&self) -> Option<Versioned<LeaseDoc>> {
        let store =
            ObjectStoreVersioned::new(stores::rustfs(), &format!("{}/globals", self.prefix))
                .expect("a root");
        Leases::new(DocStore::new(std::sync::Arc::new(store)))
            .observe(&self.test)
            .await
            .expect("the lease reads")
    }

    /// Drops the slot, the publication, the schema and the prefix.
    pub async fn cleanup(self) {
        drop_slot(&self.admin, &self.schema).await;
        self.admin
            .batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS {s}_pub; DROP SCHEMA IF EXISTS {s} CASCADE;",
                s = self.schema
            ))
            .await
            .expect("cleanup");
        delete_prefix(stores::rustfs().as_ref(), &self.prefix).await;
    }
}

/// Drops the slot, waiting for a walsender still holding it to go.
async fn drop_slot(admin: &Client, slot: &str) {
    for _ in 0..100 {
        let r = admin
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
                 WHERE slot_name = $1 AND NOT active",
                &[&slot],
            )
            .await;
        let left = admin
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .expect("slots read")
            .get::<_, i64>(0);
        if r.is_ok() && left == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("slot {slot} stays active");
}

/// The binary as a child process; killed when dropped.
pub struct Proc {
    child: Child,
    port: u16,
    log: PathBuf,
}

impl Proc {
    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    async fn get(&self, path: &str) -> Option<(u16, String)> {
        let mut s = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .ok()?;
        s.write_all(
            format!("GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .ok()?;
        let mut out = String::new();
        s.read_to_string(&mut out).await.ok()?;
        let (head, body) = out.split_once("\r\n\r\n")?;
        Some((head.get(9..12)?.parse().ok()?, body.to_owned()))
    }

    /// Waits for `/health/ready`.
    pub async fn ready(&mut self) {
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait().expect("the child") {
                panic!(
                    "the binary exited ({status}) before it was ready:\n{}",
                    self.log()
                );
            }
            if let Some((200, _)) = self.get("/health/ready").await {
                return;
            }
            assert!(
                Instant::now() < until,
                "not ready within 60 s:\n{}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn metrics(&self) -> String {
        self.get("/metrics").await.expect("/metrics answers").1
    }

    /// Waits until the metrics have a line equal to `line`.
    pub async fn metric(&self, line: &str) {
        let what = format!("the metric line {line}");
        eventually(&what, Duration::from_secs(60), async || {
            self.metrics().await.lines().any(|l| l == line)
        })
        .await;
    }

    /// SIGTERM, then the exit (within 60 s).
    pub async fn terminate(mut self) -> ExitStatus {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("kill runs");
        assert!(sent.success());
        self.exit().await
    }

    /// Waits for the process to end by itself (a crash point), within 60 s.
    pub async fn exit(&mut self) -> ExitStatus {
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait().expect("the child") {
                return status;
            }
            assert!(
                Instant::now() < until,
                "still running after 60 s:\n{}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for Proc {
    /// A failing test shows the process's log.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            eprintln!("---- data-avalanche log ----\n{}", self.log());
        }
    }
}
```

Create `crates/data-avalanche/tests/e2e/main.rs`:

```rust
//! The slice-1 E2E suite (roadmap §5.3): the `data-avalanche` binary as a process, PostgreSQL
//! (`DATA_AVALANCHE_PG_PORT`, else 18) and RustFS from `test-infra/compose.yaml`. Needs the
//! `e2e-hooks` feature and `DATA_AVALANCHE_IT=1`:
//!
//! `cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

mod harness;
mod smoke;
```

Create `crates/data-avalanche/tests/e2e/smoke.rs`:

```rust
//! The binary end to end: a snapshot, the stream, a SIGTERM that releases the lease, a restart.

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

#[tokio::test]
async fn a_pipeline_runs_stops_on_sigterm_and_resumes() {
    require_it!();
    let e = E2e::new(
        "smoke",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         INSERT INTO items VALUES (1, 'a'), (2, 'b');",
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    e.exec("INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'A' WHERE id = 1; DELETE FROM items WHERE id = 2").await;
    e.converged("items").await;
    let labels = r#"pipeline="smoke",source_host="127.0.0.1",source_db="avalanche""#;
    p.metric(&format!(
        r#"data_avalanche_pipeline_status{{{labels},status="running"}} 1"#
    ))
    .await;
    p.metric("data_avalanche_owned_pipelines 1").await;
    assert!(e.lease().await.unwrap().doc.owner_epoch == 1);

    let status = p.terminate().await;
    assert!(status.success(), "{status}");
    assert!(
        e.lease().await.unwrap().doc.released,
        "a SIGTERM releases the lease"
    );

    e.exec("INSERT INTO items VALUES (4, 'd')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    assert_eq!(e.ids("items").await, [1, 3, 4]);
    assert_eq!(e.lease().await.unwrap().doc.owner_epoch, 2);
    let status = p.terminate().await;
    assert!(status.success(), "{status}");
    e.cleanup().await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --lib --test e2e -- --test-threads=1
```

Expected:

FAIL to compile (8 errors); the first:

```text
error[E0599]: no method named `unwrap` found for struct `wiring::Factory` in the current scope
error[E0433]: cannot find type `CrashAt` in this scope
error[E0433]: cannot find type `HookPoint` in this scope
… and 5 more
```

- [ ] **Step 3: Implement**

Change `.github/workflows/ci.yml`:

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -78,6 +78,9 @@ jobs:
         run: test-infra/up.sh
       - name: Integration tests
         run: cargo test --locked --workspace -- --test-threads=1
+      # The slice-1 E2E suite (roadmap §5.3): the binary as a process, built with its crash points.
+      - name: Slice-1 E2E
+        run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
       - name: Container logs on failure
         if: failure()
         run: docker compose -f test-infra/compose.yaml logs --no-color
```

Change `crates/data-avalanche/src/crash.rs`:

```diff
diff --git a/crates/data-avalanche/src/crash.rs b/crates/data-avalanche/src/crash.rs
--- a/crates/data-avalanche/src/crash.rs
+++ b/crates/data-avalanche/src/crash.rs
@@ -1,3 +1,75 @@
+//! E2E crash points (feature `e2e-hooks`, Q-M6b-5): `DATA_AVALANCHE_E2E_CRASH=<point>[:<n>]`
+//! aborts the process at the `n`th time (default 1) the worker reaches `point`, as a `kill -9`
+//! there would. Points: `sink_committed`, `checkpoint_persisted`, `chunk_emitted`,
+//! `schemas_cached`. Never built into a release.
+
+use std::sync::atomic::{AtomicUsize, Ordering};
+
+use data_avalanche_pipeline::{HookAction, HookPoint, Hooks};
+
+pub const CRASH_VAR: &str = "DATA_AVALANCHE_E2E_CRASH";
+
+#[derive(Debug)]
+pub struct CrashAt {
+    point: &'static str,
+    nth: usize,
+    seen: AtomicUsize,
+}
+
+fn name(point: &HookPoint) -> &'static str {
+    match point {
+        HookPoint::SinkCommitted { .. } => "sink_committed",
+        HookPoint::CheckpointPersisted { .. } => "checkpoint_persisted",
+        HookPoint::ChunkEmitted { .. } => "chunk_emitted",
+        HookPoint::SchemasCached => "schemas_cached",
+    }
+}
+
+impl CrashAt {
+    /// # Errors
+    /// The value names no point, or its count isn't a positive number.
+    pub fn parse(value: &str) -> Result<Self, String> {
+        let (point, nth) = match value.split_once(':') {
+            Some((p, n)) => (
+                p,
+                n.parse::<usize>()
+                    .ok()
+                    .filter(|n| *n > 0)
+                    .ok_or_else(|| format!("{CRASH_VAR}: {n:?} isn't a positive count"))?,
+            ),
+            None => (value, 1),
+        };
+        let point = [
+            "sink_committed",
+            "checkpoint_persisted",
+            "chunk_emitted",
+            "schemas_cached",
+        ]
+        .into_iter()
+        .find(|p| *p == point)
+        .ok_or_else(|| format!("{CRASH_VAR}: {point:?} isn't a crash point"))?;
+        Ok(Self {
+            point,
+            nth,
+            seen: AtomicUsize::new(0),
+        })
+    }
+
+    fn due(&self, point: &HookPoint) -> bool {
+        name(point) == self.point && self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.nth
+    }
+}
+
+impl Hooks for CrashAt {
+    fn at(&self, point: &HookPoint) -> HookAction {
+        if self.due(point) {
+            eprintln!("data-avalanche: {CRASH_VAR}: aborting at {point:?}");
+            std::process::abort();
+        }
+        HookAction::Continue
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use data_avalanche_core::TableRef;
```

Change `crates/data-avalanche/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche/src/lib.rs b/crates/data-avalanche/src/lib.rs
--- a/crates/data-avalanche/src/lib.rs
+++ b/crates/data-avalanche/src/lib.rs
@@ -4,13 +4,13 @@
 //! SIGTERM or SIGINT, and lets every lease go on the way out.
 
 mod cli;
+#[cfg(feature = "e2e-hooks")]
+pub mod crash;
 mod http;
 mod logging;
 mod run;
 mod startup;
 mod wiring;
-#[cfg(feature = "e2e-hooks")]
-pub mod crash;
 
 pub use cli::{Cli, Command, LogFormatArg, RunArgs};
 pub use run::{Fatal, run};
```

Change `crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -139,7 +139,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     let deps = ControllerDeps {
         leases: Leases::new(joined.globals),
         holder,
-        factory: Arc::new(Factory::new(&config.globals)),
+        factory: Arc::new(Factory::new(&config.globals).map_err(Fatal::Config)?),
         metrics: metrics.clone(),
         settings: settings(&config),
         starts: Arc::new(Semaphore::new(
```

Change `crates/data-avalanche/src/wiring.rs`:

```diff
diff --git a/crates/data-avalanche/src/wiring.rs b/crates/data-avalanche/src/wiring.rs
--- a/crates/data-avalanche/src/wiring.rs
+++ b/crates/data-avalanche/src/wiring.rs
@@ -4,7 +4,7 @@ use std::sync::Arc;
 
 use data_avalanche_cluster::{Parts, WorkerFactory};
 use data_avalanche_config::{Globals, Pipeline, SinkType, TimestampNtzAs};
-use data_avalanche_pipeline::NoHooks;
+use data_avalanche_pipeline::{Hooks, NoHooks};
 use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig, NtzAs, TableDefaults};
 use data_avalanche_source_pg::PgConnector;
 
@@ -46,16 +46,34 @@ pub fn delta_config(globals: &Globals, p: &Pipeline) -> Result<DeltaSinkConfig,
 #[derive(Debug)]
 pub struct Factory {
     globals: Globals,
+    hooks: Arc<dyn Hooks>,
 }
 
 impl Factory {
-    pub fn new(globals: &Globals) -> Self {
-        Self {
+    /// # Errors
+    /// With the `e2e-hooks` feature: a `DATA_AVALANCHE_E2E_CRASH` that names no crash point.
+    pub fn new(globals: &Globals) -> Result<Self, String> {
+        Ok(Self {
             globals: globals.clone(),
-        }
+            hooks: hooks()?,
+        })
     }
 }
 
+#[cfg(feature = "e2e-hooks")]
+fn hooks() -> Result<Arc<dyn Hooks>, String> {
+    match std::env::var(crate::crash::CRASH_VAR) {
+        Ok(v) if !v.is_empty() => Ok(Arc::new(crate::crash::CrashAt::parse(&v)?)),
+        _ => Ok(Arc::new(NoHooks)),
+    }
+}
+
+#[cfg(not(feature = "e2e-hooks"))]
+#[allow(clippy::unnecessary_wraps)]
+fn hooks() -> Result<Arc<dyn Hooks>, String> {
+    Ok(Arc::new(NoHooks))
+}
+
 impl WorkerFactory for Factory {
     fn build(&self, p: &Pipeline) -> Result<Parts, String> {
         if p.sink.kind == SinkType::Iceberg {
@@ -66,7 +84,7 @@ impl WorkerFactory for Factory {
         Ok(Parts {
             source: Arc::new(source),
             sink: Arc::new(sink),
-            hooks: Arc::new(NoHooks),
+            hooks: Arc::clone(&self.hooks),
         })
     }
 }
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --lib --test e2e -- --test-threads=1
```

Expected:

PASS:

```text
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 14.77s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(e2e): the slice-1 harness: the binary against PostgreSQL and RustFS, crash points behind e2e-hooks"
```


### Task 15: E2E scenario 1: the snapshot modes

**Files:**
- Create: `crates/data-avalanche/tests/e2e/snapshots.rs`
- Modify: `crates/data-avalanche/Cargo.toml`, `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the harness (Task 14).
- Produces: `E2e::{docs, snapshot, completed, commits, drop_lake_table, status, streaming}`.

Every `snapshots.mode`: `initial` (an empty sink, then state present), `always`,
`never`, `dump_always`, and `dump_initial` in its three table-state cases. `always`'s restart on
an idle source commits a snapshot chunk at a frontier the marker covers: M6a's carried note
(mutation: skipping snapshot sets in `covers` fails it).

These scenarios exercise what Tasks 1–14 built, so they pass as written. The mutation checks that show they can fail are under "Verified".

- [ ] **Step 1: Write the scenarios**

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -36,6 +36,7 @@ e2e-hooks = []
 arrow-array.workspace = true
 data-avalanche-core.workspace = true
 data-avalanche-testkit.workspace = true
+futures-util.workspace = true
 tempfile.workspace = true
 tokio-postgres.workspace = true
 
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -10,12 +10,13 @@ use arrow_array::types::Int64Type;
 use arrow_array::{Array, RecordBatch};
 use data_avalanche_core::TableRef;
 use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig};
-use data_avalanche_state::{DocStore, LeaseDoc, Leases, Versioned};
-use data_avalanche_storage::{Backend, ObjectStoreVersioned};
+use data_avalanche_state::{DocStore, LeaseDoc, Leases, PipelineDocs, TableSnapshotDoc, Versioned};
+use data_avalanche_storage::{Backend, ObjectStoreVersioned, colocated_state, table_root};
 use data_avalanche_testkit::endpoints::{
     RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
 };
 use data_avalanche_testkit::{delete_prefix, stores, unique_prefix};
+use futures_util::TryStreamExt;
 use tokio::io::{AsyncReadExt, AsyncWriteExt};
 use tokio_postgres::{Client, NoTls};
 
@@ -195,7 +196,7 @@ pipelines:
             location: DeltaLocation::Bucket {
                 backend: Backend::S3,
                 bucket: TEST_BUCKET.into(),
-                prefix: format!("{}/lake", self.prefix),
+                prefix: self.lake_prefix(),
                 options: vec![
                     opt("endpoint", RUSTFS_URL),
                     opt("allow_http", "true"),
@@ -256,6 +257,21 @@ pipelines:
         .await;
     }
 
+    /// Waits until the pipeline's slot streams to a walsender: from here on, a change is read.
+    pub async fn streaming(&self) {
+        eventually("the slot streams", Duration::from_secs(60), async || {
+            self.admin
+                .query_opt(
+                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND active",
+                    &[&self.schema],
+                )
+                .await
+                .expect("slots read")
+                .is_some()
+        })
+        .await;
+    }
+
     pub async fn exec(&self, sql: &str) {
         self.admin
             .batch_execute(&format!(
@@ -266,17 +282,93 @@ pipelines:
             .unwrap_or_else(|e| panic!("{sql}: {e}"));
     }
 
+    fn docs_at(&self, root: &str) -> DocStore {
+        let store = ObjectStoreVersioned::new(stores::rustfs(), root).expect("a root");
+        DocStore::new(std::sync::Arc::new(store))
+    }
+
     /// The pipeline's lease in `globals.state`.
     pub async fn lease(&self) -> Option<Versioned<LeaseDoc>> {
-        let store =
-            ObjectStoreVersioned::new(stores::rustfs(), &format!("{}/globals", self.prefix))
-                .expect("a root");
-        Leases::new(DocStore::new(std::sync::Arc::new(store)))
+        Leases::new(self.docs_at(&format!("{}/globals", self.prefix)))
             .observe(&self.test)
             .await
             .expect("the lease reads")
     }
 
+    /// The pipeline's own documents, co-located with the lake (`<sink.prefix>/.pipeline-state`).
+    pub fn docs(&self) -> PipelineDocs {
+        let at = colocated_state(TEST_BUCKET, &self.lake_prefix());
+        PipelineDocs::new(self.docs_at(&at.root))
+    }
+
+    /// The table's snapshot document.
+    pub async fn snapshot(&self, table: &str) -> Option<TableSnapshotDoc> {
+        self.docs()
+            .table_snapshot(&self.table(table))
+            .await
+            .expect("the snapshot document reads")
+            .map(|v| v.doc)
+    }
+
+    /// Waits until the table's snapshot is completed (`completed_at` set, and not `before`'s),
+    /// and returns its document.
+    pub async fn completed(
+        &self,
+        table: &str,
+        before: Option<&TableSnapshotDoc>,
+    ) -> TableSnapshotDoc {
+        let before = before.and_then(|b| b.completed_at);
+        let until = Instant::now() + Duration::from_secs(60);
+        loop {
+            if let Some(s) = self.snapshot(table).await
+                && s.completed_at.is_some()
+                && s.completed_at != before
+            {
+                return s;
+            }
+            assert!(
+                Instant::now() < until,
+                "timed out waiting for {table}'s snapshot"
+            );
+            tokio::time::sleep(Duration::from_millis(100)).await;
+        }
+    }
+
+    fn lake_prefix(&self) -> String {
+        format!("{}/lake", self.prefix)
+    }
+
+    fn table_root(&self, table: &str) -> String {
+        table_root(TEST_BUCKET, &self.lake_prefix(), &self.schema, table).root
+    }
+
+    /// The Delta table's commits: its `_delta_log/*.json` files.
+    pub async fn commits(&self, table: &str) -> usize {
+        let log = object_store::path::Path::from(format!("{}/_delta_log", self.table_root(table)));
+        let files: Vec<_> = stores::rustfs()
+            .list(Some(&log))
+            .try_collect()
+            .await
+            .expect("the log lists");
+        files
+            .iter()
+            .filter(|m| m.location.extension() == Some("json"))
+            .count()
+    }
+
+    /// Deletes the Delta table from the lake, as an operator would.
+    pub async fn drop_lake_table(&self, table: &str) {
+        delete_prefix(stores::rustfs().as_ref(), &self.table_root(table)).await;
+    }
+
+    /// The `data_avalanche_pipeline_status` line for `status`, set.
+    pub fn status(&self, status: &str) -> String {
+        format!(
+            r#"data_avalanche_pipeline_status{{pipeline="{}",source_host="127.0.0.1",source_db="avalanche",status="{status}"}} 1"#,
+            self.test
+        )
+    }
+
     /// Drops the slot, the publication, the schema and the prefix.
     pub async fn cleanup(self) {
         drop_slot(&self.admin, &self.schema).await;
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -6,3 +6,4 @@
 
 mod harness;
 mod smoke;
+mod snapshots;
```

Create `crates/data-avalanche/tests/e2e/snapshots.rs`:

```rust
//! Scenario 1, the snapshot modes (config §8.2): what each start snapshots, against the state and
//! the lake it finds.

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
     INSERT INTO items VALUES (1, 'a'), (2, 'b');";

async fn world(test: &str, mode: &str) -> E2e {
    let tables = format!(", snapshots: {{ mode: {mode} }}");
    E2e::new(
        test,
        ITEMS,
        Options {
            tables: &tables,
            ..Options::default()
        },
    )
    .await
}

/// `initial` snapshots an empty sink once; a restart with its state streams on, the snapshot
/// document untouched.
#[tokio::test]
async fn initial_snapshots_once() {
    require_it!();
    let e = world("snap_initial", "initial").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    let first = e.completed("items", None).await;
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    p.metric(&e.status("running")).await;
    assert_eq!(e.snapshot("items").await, Some(first), "no second snapshot");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `always` snapshots at every start, also on an idle source whose stream frontier the sink's
/// commit marker covers already: the snapshot's rows are committed, not skipped as a replay.
#[tokio::test]
async fn always_snapshots_at_every_start() {
    require_it!();
    let e = world("snap_always", "always").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("items").await;
    let first = e.completed("items", None).await;
    assert!(p.terminate().await.success());
    let commits = e.commits("items").await;

    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", Some(&first)).await;
    assert!(
        e.commits("items").await > commits,
        "the idle source's snapshot chunk is committed"
    );
    assert_eq!(e.ids("items").await, [1, 2]);
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `never` streams only: the rows already there stay out of the lake.
#[tokio::test]
async fn never_streams_only() {
    require_it!();
    let e = world("snap_never", "never").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'A' WHERE id = 1")
        .await;
    crate::harness::eventually(
        "the streamed rows land",
        std::time::Duration::from_secs(60),
        async || e.ids("items").await == [1, 3],
    )
    .await;
    assert!(
        e.snapshot("items")
            .await
            .is_none_or(|s| s.completed_at.is_none())
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `dump_always` dumps and stops at every start.
#[tokio::test]
async fn dump_always_dumps_at_every_start() {
    require_it!();
    let e = world("snap_dump_always", "dump_always").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(e.ids("items").await, [1, 2]);
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(e.ids("items").await, [1, 2, 3], "dumped again");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `dump_initial` dumps once; with its state and the lake table it stops at once, and with the
/// table gone from the lake it dumps again.
#[tokio::test]
async fn dump_initial_dumps_until_the_lake_has_it() {
    require_it!();
    let e = world("snap_dump_initial", "dump_initial").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(e.ids("items").await, [1, 2]);
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    let commits = e.commits("items").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(e.ids("items").await, [1, 2], "not dumped again");
    assert_eq!(e.commits("items").await, commits);
    assert!(p.terminate().await.success());

    e.drop_lake_table("items").await;
    assert_eq!(e.ids("items").await, Vec::<i64>::new());
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    assert_eq!(
        e.ids("items").await,
        [1, 2, 3],
        "the lost table is dumped again"
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
```

- [ ] **Step 2: Run them**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 snapshots::
```

Expected:

PASS:

```text
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out; finished in 27.86s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`. Also `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(e2e): scenario 1, the snapshot modes: initial once, always at every start (an idle source's chunk committed), never, dump_always, dump_initial until the lake has the table"
```


### Task 16: E2E scenarios 2 and 5: streaming and TOAST

**Files:**
- Create: `crates/data-avalanche/tests/e2e/streaming.rs`, `crates/data-avalanche/tests/e2e/toast.rs`
- Modify: `crates/data-avalanche/Cargo.toml`, `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the harness (Tasks 14–15).
- Produces: `E2e::{rows, source_rows, mirrored}` (cells as text through `arrow-cast`).

Scenario 2:
- inserts, updates and deletes;
- a key-changing UPDATE;
- a transaction over two tables, and one that touches only an excluded table;
- a 50,000-row transaction, never seen in part;
- a split transaction (`max_transaction_batch_rows: 1000`, 5,000 rows), crashed after its
  second slice: exactly ids 1–2000, then the whole after the restart.

Scenario 5: an UPDATE that leaves a TOASTed column alone keeps its value, across batches and
within one. Both of its paths fail under mutation (the mask removed, the in-batch fill removed).

These scenarios exercise what Tasks 1–14 built, so they pass as written. The mutation checks that show they can fail are under "Verified".

- [ ] **Step 1: Write the scenarios**

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -34,6 +34,7 @@ e2e-hooks = []
 
 [dev-dependencies]
 arrow-array.workspace = true
+arrow-cast.workspace = true
 data-avalanche-core.workspace = true
 data-avalanche-testkit.workspace = true
 futures-util.workspace = true
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -8,6 +8,7 @@ use std::time::{Duration, Instant};
 use arrow_array::cast::AsArray;
 use arrow_array::types::Int64Type;
 use arrow_array::{Array, RecordBatch};
+use arrow_cast::display::{ArrayFormatter, FormatOptions};
 use data_avalanche_core::TableRef;
 use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig};
 use data_avalanche_state::{DocStore, LeaseDoc, Leases, PipelineDocs, TableSnapshotDoc, Versioned};
@@ -247,6 +248,55 @@ pipelines:
             .collect()
     }
 
+    /// The lake's `cols` of `table` as text (`NULL` for a null), in key order.
+    pub async fn rows(&self, table: &str, cols: &[&str]) -> Vec<Vec<String>> {
+        let Some(b) = self.content(table).await else {
+            return vec![];
+        };
+        let options = FormatOptions::default().with_null("NULL");
+        let columns: Vec<_> = cols
+            .iter()
+            .map(|c| {
+                let a = b
+                    .column_by_name(c)
+                    .unwrap_or_else(|| panic!("the lake's {table} has no column {c}"));
+                ArrayFormatter::try_new(a.as_ref(), &options).expect("a formatter")
+            })
+            .collect();
+        (0..b.num_rows())
+            .map(|i| columns.iter().map(|f| f.value(i).to_string()).collect())
+            .collect()
+    }
+
+    /// The source's `cols` of `table` as text (`NULL` for a null), in key order.
+    pub async fn source_rows(&self, table: &str, cols: &[&str]) -> Vec<Vec<String>> {
+        let list = cols
+            .iter()
+            .map(|c| format!("coalesce({c}::text, 'NULL')"))
+            .collect::<Vec<_>>()
+            .join(", ");
+        self.admin
+            .query(
+                &format!("SELECT {list} FROM {}.{table} ORDER BY id", self.schema),
+                &[],
+            )
+            .await
+            .expect("the source reads")
+            .iter()
+            .map(|r| (0..cols.len()).map(|i| r.get::<_, String>(i)).collect())
+            .collect()
+    }
+
+    /// Waits until the lake's `table` has the source's rows, compared on `cols`.
+    pub async fn mirrored(&self, table: &str, cols: &[&str]) {
+        let want = self.source_rows(table, cols).await;
+        let what = format!("{table} has the source's {} rows on {cols:?}", want.len());
+        eventually(&what, Duration::from_secs(60), async || {
+            self.rows(table, cols).await == want
+        })
+        .await;
+    }
+
     /// Waits until the lake's `table` has exactly the source's ids.
     pub async fn converged(&self, table: &str) {
         let want = self.source_ids(table).await;
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -7,3 +7,5 @@
 mod harness;
 mod smoke;
 mod snapshots;
+mod streaming;
+mod toast;
```

Create `crates/data-avalanche/tests/e2e/streaming.rs`:

```rust
//! Scenario 2, streaming: row changes, transaction shapes, a large transaction kept atomic, and a
//! split transaction replayed after a crash between its slices.

use std::time::{Duration, Instant};

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

const COLS: &[&str] = &["id", "doc"];

/// Inserts, updates, deletes, a key-changing UPDATE, a transaction over two tables and a
/// transaction that changes no captured table, each mirrored.
#[tokio::test]
async fn row_changes_and_transaction_shapes_are_mirrored() {
    require_it!();
    let e = E2e::new(
        "stream_rows",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         CREATE TABLE other (id bigint PRIMARY KEY, doc text);
         CREATE TABLE ignored (id bigint PRIMARY KEY);",
        Options {
            tables: ", exclude: [e2e_stream_rows.ignored], snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;

    e.exec("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, NULL)")
        .await;
    e.mirrored("items", COLS).await;
    e.exec("UPDATE items SET doc = 'B' WHERE id = 2; UPDATE items SET doc = 'c' WHERE id = 3; DELETE FROM items WHERE id = 1")
        .await;
    e.mirrored("items", COLS).await;

    e.exec("UPDATE items SET id = 20 WHERE id = 2").await;
    e.mirrored("items", COLS).await;
    assert_eq!(e.ids("items").await, [3, 20], "the old key is gone");

    e.exec(
        "BEGIN;
         INSERT INTO other VALUES (1, 'x');
         INSERT INTO items VALUES (4, 'd');
         DELETE FROM items WHERE id = 3;
         UPDATE other SET doc = 'X' WHERE id = 1;
         COMMIT",
    )
    .await;
    e.mirrored("items", COLS).await;
    e.mirrored("other", COLS).await;

    // Changes only an excluded table: an empty transaction to the pipeline.
    e.exec("INSERT INTO ignored VALUES (1)").await;
    e.exec("INSERT INTO items VALUES (5, 'e')").await;
    e.mirrored("items", COLS).await;
    assert_eq!(e.content("ignored").await, None);
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A 50,000-row transaction is never seen in part.
#[tokio::test]
async fn a_large_transaction_lands_at_once() {
    require_it!();
    let e = E2e::new(
        "stream_large",
        "CREATE TABLE bulk (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO bulk SELECT g, 'row ' || g FROM generate_series(1, 50000) g")
        .await;
    let until = Instant::now() + Duration::from_secs(120);
    loop {
        let seen = e.content("bulk").await.map_or(0, |b| b.num_rows());
        assert!(seen == 0 || seen == 50_000, "the lake shows {seen} rows");
        if seen == 50_000 {
            break;
        }
        assert!(Instant::now() < until, "the transaction never landed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    e.mirrored("bulk", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// With `max_transaction_batch_rows`, a larger transaction is committed in slices; a crash after
/// the second slice leaves it in part, and the restart replays it to the whole, once.
#[tokio::test]
async fn a_split_transaction_is_replayed_after_a_crash_between_slices() {
    require_it!();
    let e = E2e::new(
        "stream_split",
        "CREATE TABLE bulk (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            sink: ", max_transaction_batch_rows: 1000",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[("DATA_AVALANCHE_E2E_CRASH", "sink_committed:2")]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO bulk SELECT g, 'row ' || g FROM generate_series(1, 5000) g")
        .await;
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    assert_eq!(
        e.ids("bulk").await,
        (1..=2000).collect::<Vec<i64>>(),
        "two slices of five landed"
    );

    let mut p = e.start(&[]);
    p.ready().await;
    e.mirrored("bulk", COLS).await;
    assert_eq!(e.ids("bulk").await, (1..=5000).collect::<Vec<i64>>());
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
```

Create `crates/data-avalanche/tests/e2e/toast.rs`:

```rust
//! Scenario 5, TOAST (D-IMPL-3): an UPDATE that leaves a TOASTed column alone sends it as
//! unchanged, and the lake keeps the value, whether the row's earlier version is in the lake
//! already or in the same batch.

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

const BIG: usize = 100_000;

/// The lake's `id`, `n` and the length of `big`.
async fn lake(e: &E2e) -> Vec<(String, String, usize)> {
    e.rows("blobs", &["id", "n", "big"])
        .await
        .into_iter()
        .map(|r| (r[0].clone(), r[1].clone(), r[2].len()))
        .collect()
}

fn row(id: u32, n: u32) -> (String, String, usize) {
    (id.to_string(), n.to_string(), BIG)
}

#[tokio::test]
async fn an_update_that_leaves_a_toasted_column_keeps_its_value() {
    require_it!();
    // EXTERNAL: stored out of line, uncompressed, so the value is TOASTed.
    let e = E2e::new(
        "toast",
        "CREATE TABLE blobs (id bigint PRIMARY KEY, n int, big text);
         ALTER TABLE blobs ALTER COLUMN big SET STORAGE EXTERNAL;",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;

    e.exec(&format!(
        "INSERT INTO blobs VALUES (1, 0, repeat('x', {BIG}))"
    ))
    .await;
    e.mirrored("blobs", &["id", "n", "big"]).await;

    // Across batches: the row's value is in the lake already.
    e.exec("UPDATE blobs SET n = 1 WHERE id = 1").await;
    e.mirrored("blobs", &["id", "n"]).await;
    assert_eq!(lake(&e).await, [row(1, 1)]);

    // Within one batch: the insert and the updates collapse into one upsert.
    e.exec(&format!(
        "BEGIN;
         INSERT INTO blobs VALUES (2, 0, repeat('y', {BIG}));
         UPDATE blobs SET n = 2 WHERE id = 2;
         UPDATE blobs SET n = 2 WHERE id = 1;
         UPDATE blobs SET n = 3 WHERE id = 1;
         COMMIT"
    ))
    .await;
    e.mirrored("blobs", &["id", "n"]).await;
    assert_eq!(lake(&e).await, [row(1, 3), row(2, 2)]);
    e.mirrored("blobs", &["id", "n", "big"]).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
```

- [ ] **Step 2: Run them**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 streaming:: toast::
```

Expected:

PASS:

```text
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; finished in 43.87s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`. Also `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(e2e): scenarios 2 and 5: row changes and transaction shapes, a 50k-row transaction seen whole, a split transaction replayed after a crash between slices, unchanged TOAST kept within and across batches"
```


### Task 17: E2E scenario 3: crash and replay

**Files:**
- Create: `crates/data-avalanche/tests/e2e/crashes.rs`
- Modify: `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the harness, `DATA_AVALANCHE_E2E_CRASH` (Task 14).
- Produces: `Proc::kill` (`kill -9`), `E2e::{marker, acknowledged}`.

Five ways to die, each converging with every key once:
- mid-snapshot (the restart resumes from the persisted cursor);
- after a commit, before its checkpoint;
- between the tables of one flush;
- after the checkpoint, before the ack;
- `kill -9` under a write load.

The mid-snapshot case sets `buffer_capacity_rows: 500`, so the reads stay at most one chunk ahead
of the writer and the crash finds chunks committed. The replay after a commit is re-merged, not
skipped: finding F-1.

These scenarios exercise what Tasks 1–14 built, so they pass as written. The mutation checks that show they can fail are under "Verified".

- [ ] **Step 1: Write the scenarios**

Create `crates/data-avalanche/tests/e2e/crashes.rs`:

```rust
//! Scenario 3, crash and replay: the process dies at each phase boundary (an abort at a crash
//! point, or `kill -9`), and the restart brings the lake to the source's rows, each key once.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, admin};

const COLS: &[&str] = &["id", "doc"];
const CRASH: &str = "DATA_AVALANCHE_E2E_CRASH";

/// A crash while a snapshot's chunks are being read: the restart resumes it and the rows changed
/// in between land too.
#[tokio::test]
async fn a_crash_mid_snapshot_resumes_it() {
    require_it!();
    let e = E2e::new(
        "crash_chunk",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, 5000) g;",
        Options {
            tables: ", snapshots: { mode: initial, chunk_size: 500 }",
            // One chunk ahead of the writer at most: the chunks before it are committed.
            sink: ", buffer_capacity_rows: 500",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[(CRASH, "chunk_emitted:6")]);
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    let cursor = e
        .snapshot("items")
        .await
        .and_then(|s| s.cursor)
        .expect("a persisted cursor");
    let part = e.ids("items").await.len();
    eprintln!("after the crash: {part} rows, {cursor:?}");
    assert!(
        cursor.rows_done >= 2000 && part as u64 >= cursor.rows_done && part < 5000,
        "{part} rows, {cursor:?}"
    );

    e.exec(
        "UPDATE items SET doc = 'changed' WHERE id % 7 = 0;
         DELETE FROM items WHERE id % 11 = 0;
         INSERT INTO items VALUES (6000, 'new')",
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", None).await;
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A crash after a commit, before its checkpoint: the restart reads the transaction again and
/// commits it again, which the MERGE makes a no-op on the content. (The commit marker skips a
/// replay only when its flush ends at the marker; the flush ends at the stream's frontier, which
/// a keep-alive moves past the transaction.)
#[tokio::test]
async fn a_crash_between_a_commit_and_its_checkpoint_replays_it_idempotently() {
    require_it!();
    let e = E2e::new(
        "crash_commit",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[(CRASH, "sink_committed:1")]);
    p.ready().await;
    e.streaming().await;
    e.exec(
        "INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, 100) g;
         UPDATE items SET doc = 'changed' WHERE id % 3 = 0",
    )
    .await;
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    e.mirrored("items", COLS).await;
    let marker = e.marker("items").await;

    let mut p = e.start(&[]);
    p.ready().await;
    e.acknowledged(&marker).await;
    e.mirrored("items", COLS).await;
    assert_eq!(e.ids("items").await, (1..=100).collect::<Vec<i64>>());
    e.exec("DELETE FROM items WHERE id > 50").await;
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A crash between the tables of one flush: one table has the transaction, the other doesn't
/// until the restart replays it.
#[tokio::test]
async fn a_crash_mid_multi_table_flush_completes_it_on_restart() {
    require_it!();
    let e = E2e::new(
        "crash_tables",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         CREATE TABLE other (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[(CRASH, "sink_committed:1")]);
    p.ready().await;
    e.streaming().await;
    e.exec(
        "BEGIN;
         INSERT INTO items VALUES (1, 'a'), (2, 'b');
         INSERT INTO other VALUES (1, 'x'), (2, 'y');
         COMMIT",
    )
    .await;
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    let landed = [e.ids("items").await.len(), e.ids("other").await.len()];
    assert!(
        landed == [2, 0] || landed == [0, 2],
        "one table committed: {landed:?}"
    );

    let mut p = e.start(&[]);
    p.ready().await;
    e.mirrored("items", COLS).await;
    e.mirrored("other", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A crash after the checkpoint, before the source is told: the source sends the transaction
/// again and it is skipped.
#[tokio::test]
async fn a_crash_between_the_checkpoint_and_the_ack_skips_the_resent_transaction() {
    require_it!();
    let e = E2e::new(
        "crash_ack",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[(CRASH, "checkpoint_persisted:1")]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a'), (2, 'b')").await;
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    e.mirrored("items", COLS).await;
    let commits = e.commits("items").await;

    let mut p = e.start(&[]);
    p.ready().await;
    e.exec("UPDATE items SET doc = 'B' WHERE id = 2").await;
    e.mirrored("items", COLS).await;
    assert_eq!(
        e.commits("items").await,
        commits + 1,
        "only the new transaction is committed"
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// `kill -9` while a writer keeps changing rows: the restart catches up, each key once.
#[tokio::test]
async fn a_kill_under_load_loses_nothing() {
    require_it!();
    let e = E2e::new(
        "crash_kill",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
         INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, 200) g;",
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn(write_load(e.schema.clone(), Arc::clone(&stop)));
    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", None).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let status = p.kill().await;
    assert!(!status.success(), "{status}");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut p = e.start(&[]);
    p.ready().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::SeqCst);
    let ops = writer.await.expect("the writer");
    assert!(ops > 100, "the writer ran: {ops} statements");
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// Updates, deletes and inserts on keys 1–300 until `stop`, one statement per transaction;
/// returns how many ran.
async fn write_load(schema: String, stop: Arc<AtomicBool>) -> u64 {
    let c = admin().await;
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut ops = 0;
    while !stop.load(Ordering::SeqCst) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let id = (x % 300 + 1) as i64;
        let sql = match x % 3 {
            0 => format!("UPDATE {schema}.items SET doc = 'u{ops}' WHERE id = $1"),
            1 => format!("DELETE FROM {schema}.items WHERE id = $1"),
            _ => format!(
                "INSERT INTO {schema}.items VALUES ($1, 'i{ops}') ON CONFLICT (id) DO UPDATE SET doc = excluded.doc"
            ),
        };
        c.execute(&sql, &[&id]).await.expect("a load statement");
        ops += 1;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    ops
}
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -9,7 +9,7 @@ use arrow_array::cast::AsArray;
 use arrow_array::types::Int64Type;
 use arrow_array::{Array, RecordBatch};
 use arrow_cast::display::{ArrayFormatter, FormatOptions};
-use data_avalanche_core::TableRef;
+use data_avalanche_core::{Sink, TableRef};
 use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig};
 use data_avalanche_state::{DocStore, LeaseDoc, Leases, PipelineDocs, TableSnapshotDoc, Versioned};
 use data_avalanche_storage::{Backend, ObjectStoreVersioned, colocated_state, table_root};
@@ -322,6 +322,36 @@ pipelines:
         .await;
     }
 
+    /// The position the table's latest commit marker names, as PostgreSQL text (`0/16B3748`).
+    pub async fn marker(&self, table: &str) -> String {
+        self.lake()
+            .table_state(&self.table(table))
+            .await
+            .expect("the lake reads")
+            .marker
+            .expect("a commit marker")
+            .committed_position
+            .untagged()
+    }
+
+    /// Waits until the slot's acknowledged position reaches `lsn`: everything before it is
+    /// committed and checkpointed.
+    pub async fn acknowledged(&self, lsn: &str) {
+        let what = format!("the slot acknowledges {lsn}");
+        eventually(&what, Duration::from_secs(60), async || {
+            self.admin
+                .query_opt(
+                    "SELECT 1 FROM pg_replication_slots
+                     WHERE slot_name = $1 AND confirmed_flush_lsn >= $2::text::pg_lsn",
+                    &[&self.schema, &lsn],
+                )
+                .await
+                .expect("slots read")
+                .is_some()
+        })
+        .await;
+    }
+
     pub async fn exec(&self, sql: &str) {
         self.admin
             .batch_execute(&format!(
@@ -532,6 +562,17 @@ impl Proc {
         self.exit().await
     }
 
+    /// `kill -9`, as a host failure would.
+    pub async fn kill(mut self) -> ExitStatus {
+        let pid = self.child.id().to_string();
+        let sent = Command::new("kill")
+            .args(["-KILL", &pid])
+            .status()
+            .expect("kill runs");
+        assert!(sent.success());
+        self.exit().await
+    }
+
     /// Waits for the process to end by itself (a crash point), within 60 s.
     pub async fn exit(&mut self) -> ExitStatus {
         let until = Instant::now() + Duration::from_secs(60);
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -4,6 +4,7 @@
 //!
 //! `cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`
 
+mod crashes;
 mod harness;
 mod smoke;
 mod snapshots;
```

- [ ] **Step 2: Run them**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 crashes::
```

Expected:

PASS:

```text
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 10 filtered out; finished in 104.07s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`. Also `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(e2e): scenario 3, crash and replay: mid-snapshot resume, a commit before its checkpoint, mid multi-table flush, the checkpoint before the ack, kill -9 under a write load"
```


### Task 18: E2E scenario 4: schema changes

**Files:**
- Create: `crates/data-avalanche/tests/e2e/schema.rs`
- Modify: `crates/data-avalanche/Cargo.toml`, `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the harness.
- Produces: `Options::pipeline` (pipeline-level YAML), `E2e::{halts, log}`.

Schema_Change §7.2 on Delta:
- ADD COLUMN is applied.
- DROP COLUMN is soft-dropped: the old rows keep their values, new rows get NULL.
- RENAME adds the new name and keeps the history under the old.
- TRUNCATE is a DELETE commit with no `add` action.
- A type change, a reserved name (`_op`) and a `DROP TABLE` (within `rescan_interval: 2s`) each
  halt their table only, and the halts hold across a restart.
- An ADD COLUMN replayed after a crash adds nothing: the log keeps two `metaData` commits.

These scenarios exercise what Tasks 1–14 built, so they pass as written. The mutation checks that show they can fail are under "Verified".

- [ ] **Step 1: Write the scenarios**

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -38,6 +38,7 @@ arrow-cast.workspace = true
 data-avalanche-core.workspace = true
 data-avalanche-testkit.workspace = true
 futures-util.workspace = true
+serde_json.workspace = true
 tempfile.workspace = true
 tokio-postgres.workspace = true
 
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -11,7 +11,9 @@ use arrow_array::{Array, RecordBatch};
 use arrow_cast::display::{ArrayFormatter, FormatOptions};
 use data_avalanche_core::{Sink, TableRef};
 use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig};
-use data_avalanche_state::{DocStore, LeaseDoc, Leases, PipelineDocs, TableSnapshotDoc, Versioned};
+use data_avalanche_state::{
+    DocStore, HaltsDoc, LeaseDoc, Leases, PipelineDocs, TableSnapshotDoc, Versioned,
+};
 use data_avalanche_storage::{Backend, ObjectStoreVersioned, colocated_state, table_root};
 use data_avalanche_testkit::endpoints::{
     RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
@@ -70,6 +72,9 @@ pub struct Options<'a> {
     pub sink: &'a str,
     /// `source.postgres` entries.
     pub postgres: &'a str,
+    /// Pipeline keys beyond `source`, `tables` and `sink`, one YAML line, e.g.
+    /// `schema_evolution: { rescan_interval: 2s }`.
+    pub pipeline: &'a str,
 }
 
 pub struct E2e {
@@ -137,6 +142,7 @@ pipelines:
       postgres: {{ publication: {schema}_pub, slot: {schema}{postgres} }}
     tables: {{ include: [{schema}.*]{tables} }}
     sink: {{ type: delta, bucket: {bucket}, prefix: {prefix}/lake, delta: {{}}, flush: {{ seconds: 1 }}{sink} }}
+    {pipeline}
 "#,
             bucket = TEST_BUCKET,
             prefix = self.prefix,
@@ -147,6 +153,7 @@ pipelines:
             postgres = o.postgres,
             tables = o.tables,
             sink = o.sink,
+            pipeline = o.pipeline,
         )
     }
 
@@ -414,6 +421,44 @@ pipelines:
         }
     }
 
+    /// The pipeline's halt registry.
+    pub async fn halts(&self) -> HaltsDoc {
+        self.docs().halts().await.expect("the halts read")
+    }
+
+    /// The Delta table's commits in version order, each its actions.
+    pub async fn log(&self, table: &str) -> Vec<Vec<serde_json::Value>> {
+        let store = stores::rustfs();
+        let log = object_store::path::Path::from(format!("{}/_delta_log", self.table_root(table)));
+        let mut files: Vec<_> = store
+            .list(Some(&log))
+            .try_collect::<Vec<_>>()
+            .await
+            .expect("the log lists")
+            .into_iter()
+            .map(|m| m.location)
+            .filter(|l| l.extension() == Some("json"))
+            .collect();
+        files.sort();
+        let mut commits = vec![];
+        for f in files {
+            let bytes = store
+                .get_opts(&f, object_store::GetOptions::default())
+                .await
+                .expect("a commit")
+                .bytes()
+                .await
+                .expect("a commit");
+            let text = String::from_utf8(bytes.to_vec()).expect("UTF-8");
+            commits.push(
+                text.lines()
+                    .map(|l| serde_json::from_str(l).expect("an action"))
+                    .collect(),
+            );
+        }
+        commits
+    }
+
     fn lake_prefix(&self) -> String {
         format!("{}/lake", self.prefix)
     }
@@ -424,16 +469,7 @@ pipelines:
 
     /// The Delta table's commits: its `_delta_log/*.json` files.
     pub async fn commits(&self, table: &str) -> usize {
-        let log = object_store::path::Path::from(format!("{}/_delta_log", self.table_root(table)));
-        let files: Vec<_> = stores::rustfs()
-            .list(Some(&log))
-            .try_collect()
-            .await
-            .expect("the log lists");
-        files
-            .iter()
-            .filter(|m| m.location.extension() == Some("json"))
-            .count()
+        self.log(table).await.len()
     }
 
     /// Deletes the Delta table from the lake, as an operator would.
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -6,6 +6,7 @@
 
 mod crashes;
 mod harness;
+mod schema;
 mod smoke;
 mod snapshots;
 mod streaming;
```

Create `crates/data-avalanche/tests/e2e/schema.rs`:

```rust
//! Scenario 4, schema changes on Delta (Schema_Change §7): what is applied, what degrades, what
//! halts the table, a TRUNCATE, and a DDL replayed after a crash.

use std::time::Duration;

use data_avalanche_core::HaltReason;
use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};

fn row(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| (*v).to_owned()).collect()
}

/// ADD COLUMN is applied; DROP COLUMN soft-drops (the column stays, later rows NULL); RENAME
/// adds the new name beside the old, which keeps the history; TRUNCATE clears the table with a DELETE that rewrites no file.
#[tokio::test]
async fn columns_evolve_and_a_truncate_clears_the_table() {
    require_it!();
    let e = E2e::new(
        "schema_columns",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text, old text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a', 'o')").await;
    e.mirrored("items", &["id", "doc", "old"]).await;

    e.exec("ALTER TABLE items ADD COLUMN extra int; INSERT INTO items VALUES (2, 'b', 'p', 7)")
        .await;
    e.mirrored("items", &["id", "doc", "old", "extra"]).await;

    e.exec("ALTER TABLE items DROP COLUMN old; INSERT INTO items VALUES (3, 'c', 8)")
        .await;
    e.mirrored("items", &["id", "doc", "extra"]).await;
    assert_eq!(
        e.rows("items", &["id", "old"]).await,
        [row(&["1", "o"]), row(&["2", "p"]), row(&["3", "NULL"])],
        "the dropped column stays"
    );

    e.exec("ALTER TABLE items RENAME COLUMN doc TO body; INSERT INTO items VALUES (4, 'd', 9)")
        .await;
    e.converged("items").await;
    assert_eq!(
        e.rows("items", &["id", "doc", "body"]).await,
        [
            row(&["1", "a", "NULL"]),
            row(&["2", "b", "NULL"]),
            row(&["3", "c", "NULL"]),
            row(&["4", "NULL", "d"]),
        ],
        "the history stays under the old name"
    );

    e.exec("TRUNCATE items; INSERT INTO items VALUES (10, 'z', 1)")
        .await;
    e.mirrored("items", &["id", "body", "extra"]).await;
    let log = e.log("items").await;
    let delete = log
        .iter()
        .find(|c| {
            c.iter()
                .any(|a| a["commitInfo"]["operation"].as_str() == Some("DELETE"))
        })
        .expect("a DELETE commit");
    assert!(
        delete.iter().all(|a| a.get("add").is_none()),
        "metadata-only: {delete:?}"
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A type change, a reserved column name and a dropped table each halt their table, and the
/// halts are kept across a restart; the other tables stream on.
#[tokio::test]
async fn unsafe_changes_halt_their_table_only() {
    require_it!();
    let e = E2e::new(
        "schema_halts",
        "CREATE TABLE widen (id bigint PRIMARY KEY, n int);
         CREATE TABLE reserved (id bigint PRIMARY KEY, doc text);
         CREATE TABLE dropped (id bigint PRIMARY KEY);
         CREATE TABLE fine (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            pipeline: "schema_evolution: { rescan_interval: 2s }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec(
        "INSERT INTO widen VALUES (1, 1); INSERT INTO reserved VALUES (1, 'a');
         INSERT INTO dropped VALUES (1); INSERT INTO fine VALUES (1, 'a')",
    )
    .await;
    for t in ["widen", "reserved", "dropped", "fine"] {
        e.converged(t).await;
    }

    e.exec("ALTER TABLE widen ALTER COLUMN n TYPE bigint; INSERT INTO widen VALUES (2, 2)")
        .await;
    e.exec("ALTER TABLE reserved ADD COLUMN _op text; INSERT INTO reserved VALUES (2, 'b', 'x')")
        .await;
    e.exec("DROP TABLE dropped").await;
    e.exec("INSERT INTO fine VALUES (2, 'b')").await;
    e.converged("fine").await;

    eventually("three tables halt", Duration::from_secs(60), async || {
        e.halts().await.tables.len() == 3
    })
    .await;
    let mut halts: Vec<(String, &str)> = e
        .halts()
        .await
        .tables
        .iter()
        .map(|h| (h.table.name.clone(), reason(&h.reason)))
        .collect();
    halts.sort();
    assert_eq!(
        halts,
        [
            ("dropped".into(), "TABLE_DROPPED"),
            ("reserved".into(), "RESERVED_COLUMN_COLLISION"),
            ("widen".into(), "EVOLUTION_UNSUPPORTED"),
        ]
    );
    let labels = r#"pipeline="schema_halts",source_host="127.0.0.1",source_db="avalanche""#;
    p.metric(&format!("data_avalanche_halted_tables{{{labels}}} 3"))
        .await;
    assert_eq!(e.ids("widen").await, [1], "nothing after the halt");
    assert_eq!(e.ids("reserved").await, [1]);
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO widen VALUES (3, 3); INSERT INTO fine VALUES (3, 'c')")
        .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.converged("fine").await;
    p.metric(&format!("data_avalanche_halted_tables{{{labels}}} 3"))
        .await;
    assert_eq!(e.ids("widen").await, [1], "still halted after a restart");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

fn reason(r: &HaltReason) -> &'static str {
    match r {
        HaltReason::ReservedColumnCollision { .. } => "RESERVED_COLUMN_COLLISION",
        HaltReason::TableDropped => "TABLE_DROPPED",
        HaltReason::UnsafeDdl { .. } => "UNSAFE_DDL",
        HaltReason::EvolutionUnsupported { .. } => "EVOLUTION_UNSUPPORTED",
        _ => "OTHER",
    }
}

/// A crash after an ADD COLUMN's rows were committed, before the checkpoint: the restart reads
/// the DDL again and finds the column there.
#[tokio::test]
async fn a_ddl_replayed_after_a_crash_is_applied_once() {
    require_it!();
    let e = E2e::new(
        "schema_replay",
        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
        Options {
            tables: ", snapshots: { mode: never }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[("DATA_AVALANCHE_E2E_CRASH", "sink_committed:1")]);
    p.ready().await;
    e.streaming().await;
    e.exec("ALTER TABLE items ADD COLUMN extra int; INSERT INTO items VALUES (1, 'a', 7)")
        .await;
    let status = p.exit().await;
    assert!(!status.success(), "aborted at the crash point: {status}");
    e.mirrored("items", &["id", "doc", "extra"]).await;

    let mut p = e.start(&[]);
    p.ready().await;
    e.exec("INSERT INTO items VALUES (2, 'b', 8)").await;
    e.mirrored("items", &["id", "doc", "extra"]).await;
    let schema_commits = e
        .log("items")
        .await
        .iter()
        .filter(|c| c.iter().any(|a| a.get("metaData").is_some()))
        .count();
    assert_eq!(schema_commits, 2, "the create and one ADD COLUMN");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
```

- [ ] **Step 2: Run them**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 schema::
```

Expected:

PASS:

```text
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 15 filtered out; finished in 42.40s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`. Also `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(e2e): scenario 4, schema changes: ADD applied, DROP soft-dropped, RENAME beside the old name, TRUNCATE metadata-only, type change / reserved name / DROP TABLE halt their table across a restart, a DDL replayed after a crash"
```


### Task 19: E2E scenario 6: types through DuckDB

**Files:**
- Create: `crates/data-avalanche/tests/e2e/duckdb_read.py`, `crates/data-avalanche/tests/e2e/types.rs`
- Modify: `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the harness, `uv` + DuckDB 1.5.6 (M4's cross-read).
- Produces: `tests/e2e/duckdb_read.py` (S3 secret for RustFS, UTC), `E2e::table_url`.

Each PostgreSQL type family through a snapshot and through the stream, read back by an
independent engine (DuckDB's `delta_scan`, delta-kernel based) with every cell pinned against
`Type_Mapping.md`. The stream and the snapshot give the same cells; the all-NULL row is NULL
everywhere.

These scenarios exercise what Tasks 1–14 built, so they pass as written. The mutation checks that show they can fail are under "Verified".

- [ ] **Step 1: Write the scenarios**

Create `crates/data-avalanche/tests/e2e/duckdb_read.py`:

```python
"""Reads a Delta table on S3 (RustFS) with DuckDB's delta_scan and prints every cell as text, as
JSON rows in `id` order.

Used by tests/e2e/types.rs. Run: uv run --no-project --with duckdb==1.5.6 python duckdb_read.py
<s3 table url>, with DUCKDB_S3_ENDPOINT (host:port), DUCKDB_S3_KEY_ID and DUCKDB_S3_SECRET set.
"""

import json
import os
import sys

import duckdb

con = duckdb.connect()


def quote(s):
    return "'" + s.replace("'", "''") + "'"


con.sql(
    "CREATE SECRET rustfs (TYPE s3, KEY_ID {}, SECRET {}, ENDPOINT {}, REGION 'us-east-1', "
    "URL_STYLE 'path', USE_SSL false)".format(
        quote(os.environ["DUCKDB_S3_KEY_ID"]),
        quote(os.environ["DUCKDB_S3_SECRET"]),
        quote(os.environ["DUCKDB_S3_ENDPOINT"]),
    )
)
# timestamptz cells print in UTC, whatever the host's zone.
con.sql("SET TimeZone = 'UTC'")
scan = f"delta_scan({quote(sys.argv[1])})"
names = con.sql(f"SELECT * FROM {scan}").columns
cols = ", ".join(f'"{c}"::VARCHAR' for c in names)
rows = con.sql(f"SELECT {cols} FROM {scan} ORDER BY id").fetchall()
print(json.dumps({"columns": names, "rows": [list(r) for r in rows]}))
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -467,6 +467,11 @@ pipelines:
         table_root(TEST_BUCKET, &self.lake_prefix(), &self.schema, table).root
     }
 
+    /// The Delta table's root as an `s3://` URL.
+    pub fn table_url(&self, table: &str) -> String {
+        format!("s3://{TEST_BUCKET}/{}", self.table_root(table))
+    }
+
     /// The Delta table's commits: its `_delta_log/*.json` files.
     pub async fn commits(&self, table: &str) -> usize {
         self.log(table).await.len()
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -11,3 +11,4 @@ mod smoke;
 mod snapshots;
 mod streaming;
 mod toast;
+mod types;
```

Create `crates/data-avalanche/tests/e2e/types.rs`:

```rust
//! Scenario 6, types: each PostgreSQL type family through a snapshot and through the stream into
//! Delta, read back by an independent engine, DuckDB's `delta_scan` (`Type_Mapping.md` Hop 3b).

use data_avalanche_testkit::endpoints::{RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL};
use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options};

const DDL: &str = "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
    CREATE TABLE types (
        id bigint PRIMARY KEY,
        b bool, i2 int2, i4 int4, i8 int8, o oid, f4 float4, f8 float8,
        n numeric(12,3), n38 numeric(38,0), nb numeric, m money,
        bp char(5), vc varchar(10), tx text, by bytea,
        d date, tm time, tz timetz, ts timestamp, tt timestamptz, iv interval,
        u uuid, j json, jb jsonb, ip inet, r int4range,
        ai int4[], at text[], e mood
    );";

/// The non-key columns' SQL literals, per row.
const ROW_A: &str = "true, -32768, 2147483647, 9223372036854775807, 4294967295, 1.5, 0.1,
    123456789.123, 99999999999999999999999999999999999999, 12345.6789, 12.34,
    'ab', 'hello', 'it''s', '\\xdeadbeef',
    '2024-02-29', '12:34:56.789012', '12:00:00+05:30', '2024-02-29 12:34:56.789012',
    '2024-02-29 12:34:56.789012+00', '1 year 2 mons 3 days 04:05:06',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"a\": 1}', '{\"b\": [1, 2]}', '192.168.0.1/24',
    '[1,10)', '{1,2,NULL}', '{\"x\",\"y z\"}', 'happy'";
const ROW_B: &str = "false, 32767, -2147483648, -9223372036854775808, 0, '-Infinity', 'NaN',
    -0.001, -99999999999999999999999999999999999999, 0, -1.00,
    'abcde', '', '', '\\x',
    '1970-01-01', '00:00:00', '23:59:59-08', '1969-12-31 23:59:59.999999',
    '1970-01-01 00:00:00+00', '-1 days',
    '00000000-0000-0000-0000-000000000000', 'null', '[]', '::1',
    'empty', '{}', '{}', 'sad'";

/// Per column, DuckDB's text of [`ROW_A`] and [`ROW_B`] (`Type_Mapping.md`: `char(n)` without its
/// padding, `time` and `interval` as text, compact JSON, ranges, `inet` and enums as text).
const EXPECTED: &[[&str; 3]] = &[
    ["b", "true", "false"],
    ["i2", "-32768", "32767"],
    ["i4", "2147483647", "-2147483648"],
    ["i8", "9223372036854775807", "-9223372036854775808"],
    ["o", "4294967295", "0"],
    ["f4", "1.5", "-inf"],
    ["f8", "0.1", "nan"],
    ["n", "123456789.123", "-0.001"],
    [
        "n38",
        "99999999999999999999999999999999999999",
        "-99999999999999999999999999999999999999",
    ],
    ["nb", "12345.6789", "0"],
    ["m", "12.34", "-1.00"],
    ["bp", "ab", "abcde"],
    ["vc", "hello", ""],
    ["tx", "it's", ""],
    ["by", "\\xDE\\xAD\\xBE\\xEF", ""],
    ["d", "2024-02-29", "1970-01-01"],
    ["tm", "12:34:56.789012", "00:00:00.000000"],
    ["tz", "12:00:00+05:30", "23:59:59-08"],
    [
        "ts",
        "2024-02-29 12:34:56.789012",
        "1969-12-31 23:59:59.999999",
    ],
    [
        "tt",
        "2024-02-29 12:34:56.789012+00",
        "1970-01-01 00:00:00+00",
    ],
    ["iv", "P1Y2M3DT4H5M6S", "P-1D"],
    [
        "u",
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        "00000000-0000-0000-0000-000000000000",
    ],
    ["j", "{\"a\":1}", "null"],
    ["jb", "{\"b\":[1,2]}", "[]"],
    ["ip", "192.168.0.1/24", "::1"],
    ["r", "[1,10)", "empty"],
    ["ai", "[1, 2, NULL]", "[]"],
    ["at", "[x, y z]", "[]"],
    ["e", "happy", "sad"],
];

/// DuckDB's `delta_scan` of a lake table: the column names and every cell as text, rows in key
/// order. Needs `uv` (the CI integration job installs it).
fn duckdb_read(url: &str) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/e2e/duckdb_read.py");
    let out = std::process::Command::new("uv")
        .args([
            "run",
            "--no-project",
            "--with",
            "duckdb==1.5.6",
            "python",
            script,
            url,
        ])
        .env(
            "DUCKDB_S3_ENDPOINT",
            RUSTFS_URL.trim_start_matches("http://"),
        )
        .env("DUCKDB_S3_KEY_ID", RUSTFS_ACCESS_KEY)
        .env("DUCKDB_S3_SECRET", RUSTFS_SECRET_KEY)
        .output()
        .expect("uv is installed (https://docs.astral.sh/uv/)");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).expect("JSON");
    (
        serde_json::from_value(v["columns"].clone()).expect("columns"),
        serde_json::from_value(v["rows"].clone()).expect("rows"),
    )
}

#[tokio::test]
async fn every_type_family_reads_back_in_duckdb() {
    require_it!();
    let e = E2e::new(
        "types",
        &format!("{DDL} INSERT INTO types VALUES (1, {ROW_A}), (2, {ROW_B});"),
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("types", None).await;
    e.exec(&format!(
        "INSERT INTO types VALUES (3, {ROW_A}); INSERT INTO types (id) VALUES (4)"
    ))
    .await;
    e.converged("types").await;
    assert!(p.terminate().await.success());

    let (columns, rows) = duckdb_read(&e.table_url("types"));
    assert_eq!(rows.len(), 4);
    let source: Vec<usize> = (0..columns.len())
        .filter(|&i| columns[i] != "id" && !columns[i].starts_with('_'))
        .collect();
    let cells = |row: usize| -> Vec<Option<&str>> {
        source.iter().map(|&i| rows[row][i].as_deref()).collect()
    };
    let named = |row: usize| -> Vec<(&str, Option<&str>)> {
        source
            .iter()
            .map(|&i| (columns[i].as_str(), rows[row][i].as_deref()))
            .collect()
    };
    let want = |i: usize| -> Vec<(&str, Option<&str>)> {
        EXPECTED.iter().map(|c| (c[0], Some(c[i]))).collect()
    };
    assert_eq!(named(0), want(1), "row 1, from the snapshot");
    assert_eq!(named(1), want(2), "row 2, from the snapshot");
    assert_eq!(
        cells(2),
        cells(0),
        "the stream reads what the snapshot reads"
    );
    assert!(cells(3).iter().all(Option::is_none), "the all-NULL row");
    e.cleanup().await;
}
```

- [ ] **Step 2: Run them**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 types::
```

Expected:

PASS:

```text
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 18 filtered out; finished in 12.41s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`. Also `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(e2e): scenario 6, types: every PostgreSQL type family through a snapshot and the stream, read back by DuckDB delta_scan on RustFS"
```


### Task 20: E2E scenarios 7–10: lifecycle, the lease, the Iceberg refusal, the guards

**Files:**
- Create: `crates/data-avalanche/tests/e2e/guards.rs`, `crates/data-avalanche/tests/e2e/lifecycle.rs`
- Modify: `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the harness, `PipelineDocs::set_desired_state` (M2).
- Produces: per-node configs (`E2e::start_node(name)`: own `memberlist.node_name` and API
  port), `Options::pipelines` (more pipelines in the config).

Scenarios 7–10:
- **Lifecycle:** pause holds the source connection; stop releases it; both hold across a
  restart; resume reads on.
- **The lease:** a second process waits while the first holds the lease, and takes it over at
  epoch 2 after a `kill -9`.
- **Iceberg:** the pipeline is refused (ERROR, no slot created) while a Delta pipeline beside it
  runs.
- **Guards:**
  - an open transaction pins the slot while the WAL grows past `max_wal_lag_bytes`;
  - one transaction outgrows `max_transaction_buffer_bytes`;
  - a slot PostgreSQL invalidated while the process was down is re-made and re-snapshotted.

These scenarios exercise what Tasks 1–14 built, so they pass as written. The mutation checks that show they can fail are under "Verified".

- [ ] **Step 1: Write the scenarios**

Create `crates/data-avalanche/tests/e2e/guards.rs`:

```rust
//! Scenario 10, the guards: the WAL a slot retains, the open transaction's buffer, and a slot
//! whose WAL is gone.

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, admin};

const TABLES: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
     CREATE TABLE pad (id bigserial PRIMARY KEY, doc text);";

/// Writes WAL and switches to a new segment, `n` times: each moves the WAL end by up to 16 MiB.
async fn switch_wal(e: &E2e, n: usize) -> Result<(), tokio_postgres::Error> {
    for _ in 0..n {
        e.admin
            .batch_execute(&format!(
                "INSERT INTO {}.pad (doc) VALUES ('x'); SELECT pg_switch_wal()",
                e.schema
            ))
            .await?;
    }
    Ok(())
}

/// An open transaction pins the slot while the WAL grows past `max_wal_lag_bytes`: the pipeline
/// stops with an error, to protect the source's disk. The lag is read once per heartbeat, so the
/// heartbeat is short: at the default 60 s the first read could fall past the wait.
#[tokio::test]
async fn retained_wal_past_the_limit_stops_the_pipeline() {
    require_it!();
    let e = E2e::new(
        "guard_wal",
        TABLES,
        Options {
            tables: ", exclude: [e2e_guard_wal.pad], snapshots: { mode: never }",
            postgres: ", max_wal_lag_bytes: 64MiB, heartbeat: 5s",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    let held = admin().await;
    held.batch_execute(&format!(
        "BEGIN; INSERT INTO {}.items VALUES (1, 'held')",
        e.schema
    ))
    .await
    .expect("an open transaction");
    switch_wal(&e, 6).await.expect("WAL written");
    e.admin
        .batch_execute("CHECKPOINT")
        .await
        .expect("a checkpoint");
    p.metric(&e.status("error")).await;
    held.batch_execute("ROLLBACK").await.expect("rolled back");
    assert!(
        p.log().contains("over max_wal_lag_bytes 67108864"),
        "{}",
        p.log()
    );
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A transaction larger than `max_transaction_buffer_bytes`, splitting off: the pipeline stops
/// with an error instead of growing past the ceiling, and nothing of it lands.
#[tokio::test]
async fn a_transaction_past_the_buffer_ceiling_stops_the_pipeline() {
    require_it!();
    let e = E2e::new(
        "guard_buffer",
        TABLES,
        Options {
            tables: ", snapshots: { mode: never }",
            sink: ", max_transaction_buffer_bytes: 16MiB",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items SELECT g, repeat('x', 300) FROM generate_series(1, 100000) g")
        .await;
    p.metric(&e.status("error")).await;
    assert!(
        p.log()
            .contains("over max_transaction_buffer_bytes 16777216"),
        "{}",
        p.log()
    );
    assert!(e.ids("items").await.is_empty());
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A slot PostgreSQL invalidated while the pipeline was down: the restart makes a new slot and
/// snapshots again, so the change whose WAL is gone lands too.
#[tokio::test]
async fn a_slot_whose_wal_is_gone_is_snapshotted_again() {
    require_it!();
    let e = E2e::new(
        "guard_lost",
        &format!("{TABLES} INSERT INTO items VALUES (1, 'a'), (2, 'b');"),
        Options {
            tables: ", exclude: [e2e_guard_lost.pad], snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    let first = e.completed("items", None).await;
    e.converged("items").await;
    assert!(p.terminate().await.success());
    e.exec("INSERT INTO items VALUES (3, 'c')").await;

    // A 1 MB allowance, WAL past it, a checkpoint: the slot is lost. Every step is fallible, so
    // the server-wide setting is restored whatever happens; the first reset undoes an earlier
    // run that died before its own. ALTER SYSTEM runs outside a transaction: one per call.
    let keep = async |size: Option<&str>| {
        let set = match size {
            None => "ALTER SYSTEM RESET max_slot_wal_keep_size".to_owned(),
            Some(s) => format!("ALTER SYSTEM SET max_slot_wal_keep_size = '{s}'"),
        };
        e.admin.batch_execute(&set).await?;
        e.admin.batch_execute("SELECT pg_reload_conf()").await
    };
    keep(None).await.expect("the default");
    let invalidate = async {
        keep(Some("1MB")).await?;
        switch_wal(&e, 3).await?;
        e.admin.batch_execute("CHECKPOINT").await?;
        e.admin
            .query_one(
                "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
                &[&e.schema],
            )
            .await
            .map(|r| r.get::<_, Option<String>>(0))
    }
    .await;
    keep(None).await.expect("the default restored");
    assert_eq!(
        invalidate.expect("the slot invalidated").as_deref(),
        Some("lost")
    );

    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", Some(&first)).await;
    e.converged("items").await;
    e.exec("INSERT INTO items VALUES (4, 'd')").await;
    e.converged("items").await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -75,14 +75,20 @@ pub struct Options<'a> {
     /// Pipeline keys beyond `source`, `tables` and `sink`, one YAML line, e.g.
     /// `schema_evolution: { rescan_interval: 2s }`.
     pub pipeline: &'a str,
+    /// More pipelines, YAML under `pipelines:` (2-space indent); `@SCHEMA@`, `@PREFIX@`, `@PG@`
+    /// and `@BUCKET@` are replaced.
+    pub pipelines: &'a str,
 }
 
 pub struct E2e {
     pub test: String,
     pub schema: String,
     prefix: String,
+    /// The first node's (`n1`) API port.
     port: u16,
     dir: tempfile::TempDir,
+    /// The config with `@NODE@` and `@PORT@` to fill per node.
+    template: String,
     pub admin: Client,
 }
 
@@ -104,31 +110,53 @@ impl E2e {
             ))
             .await
             .expect("test DDL");
-        let e = Self {
+        let mut e = Self {
             test: test.to_owned(),
             schema,
             prefix: unique_prefix(&format!("e2e-{test}")),
             port: free_port(),
             dir: tempfile::tempdir().expect("a temp dir"),
+            template: String::new(),
             admin,
         };
-        std::fs::write(e.config_path(), e.yaml(&o)).expect("the config file");
+        e.template = e.yaml(&o);
+        e.write_config("n1", e.port);
         e
     }
 
-    pub fn config_path(&self) -> PathBuf {
-        self.dir.path().join("config.yaml")
+    fn config_path(&self, node: &str) -> PathBuf {
+        self.dir.path().join(format!("config-{node}.yaml"))
+    }
+
+    fn write_config(&self, node: &str, port: u16) -> PathBuf {
+        let path = self.config_path(node);
+        let text = self
+            .template
+            .replace("@NODE@", node)
+            .replace("@PORT@", &port.to_string());
+        std::fs::write(&path, text).expect("the config file");
+        path
     }
 
     fn yaml(&self, o: &Options<'_>) -> String {
+        let more = o
+            .pipelines
+            .replace("@SCHEMA@", &self.schema)
+            .replace("@PREFIX@", &self.prefix)
+            .replace("@PG@", &pg_port().to_string())
+            .replace("@BUCKET@", TEST_BUCKET);
         format!(
             r#"config_version: 1
 globals:
   cloud: aws
   state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
-  api: {{ address: "127.0.0.1:{port}" }}
+  api: {{ address: "127.0.0.1:@PORT@" }}
   log: {{ level: "info,buoyant_kernel=warn", format: compact }}
-  cluster: {{ lease_duration: 6s, lease_renewal_interval: 2s, drain_timeout: 30s }}
+  cluster:
+    lease_duration: 6s
+    lease_renewal_interval: 2s
+    drain_timeout: 30s
+    memberlist: {{ node_name: "@NODE@" }}
 pipelines:
   {test}:
     id: {test}
@@ -143,10 +171,10 @@ pipelines:
     tables: {{ include: [{schema}.*]{tables} }}
     sink: {{ type: delta, bucket: {bucket}, prefix: {prefix}/lake, delta: {{}}, flush: {{ seconds: 1 }}{sink} }}
     {pipeline}
+{more}
 "#,
             bucket = TEST_BUCKET,
             prefix = self.prefix,
-            port = self.port,
             test = self.test,
             pg = pg_port(),
             schema = self.schema,
@@ -157,8 +185,19 @@ pipelines:
         )
     }
 
-    /// Starts the binary; `env` adds variables (a crash point).
+    /// Starts the binary as node `n1`; `env` adds variables (a crash point).
     pub fn start(&self, env: &[(&str, &str)]) -> Proc {
+        self.spawn(&self.config_path("n1"), self.port, env)
+    }
+
+    /// Starts the binary as another node, with its own name and API port.
+    pub fn start_node(&self, node: &str) -> Proc {
+        let port = free_port();
+        let path = self.write_config(node, port);
+        self.spawn(&path, port, &[])
+    }
+
+    fn spawn(&self, config: &std::path::Path, port: u16, env: &[(&str, &str)]) -> Proc {
         let log = self.dir.path().join(format!(
             "run-{}.log",
             std::time::SystemTime::now()
@@ -170,7 +209,7 @@ pipelines:
         let err = out.try_clone().expect("the log file");
         let mut cmd = Command::new(env!("CARGO_BIN_EXE_data-avalanche"));
         cmd.args(["run", "--config"])
-            .arg(self.config_path())
+            .arg(config)
             .env_clear()
             .env("PATH", std::env::var("PATH").unwrap_or_default())
             .env("AWS_ACCESS_KEY_ID", RUSTFS_ACCESS_KEY)
@@ -187,7 +226,7 @@ pipelines:
         }
         Proc {
             child: cmd.spawn().expect("the binary starts"),
-            port: self.port,
+            port,
             log,
         }
     }
```

Create `crates/data-avalanche/tests/e2e/lifecycle.rs`:

```rust
//! Scenarios 7–9: the desired state across a restart, one pipeline under one lease with a second
//! process waiting, and an Iceberg pipeline refused beside a running Delta one.

use std::time::Duration;

use data_avalanche_state::DesiredState;
use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";

fn never() -> Options<'static> {
    Options {
        tables: ", snapshots: { mode: never }",
        ..Options::default()
    }
}

/// The slot has a walsender: the source connection is open.
async fn slot_active(e: &E2e) -> bool {
    e.admin
        .query_one(
            "SELECT coalesce(bool_or(active), false) FROM pg_replication_slots WHERE slot_name = $1",
            &[&e.schema],
        )
        .await
        .expect("slots read")
        .get(0)
}

/// Pause holds the stream with the source connection open; stop closes it and is kept across a
/// restart; resume reads on from where each left off.
#[tokio::test]
async fn pause_stop_and_resume_hold_across_a_restart() {
    require_it!();
    let e = E2e::new("life_states", ITEMS, never()).await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.converged("items").await;

    e.docs()
        .set_desired_state(DesiredState::Paused)
        .await
        .expect("paused");
    p.metric(&e.status("paused")).await;
    e.exec("INSERT INTO items VALUES (2, 'b')").await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(e.ids("items").await, [1], "nothing is read while paused");
    assert!(
        slot_active(&e).await,
        "a paused pipeline keeps the source warm"
    );

    e.docs()
        .set_desired_state(DesiredState::Running)
        .await
        .expect("running");
    p.metric(&e.status("running")).await;
    e.converged("items").await;

    e.docs()
        .set_desired_state(DesiredState::Stopped)
        .await
        .expect("stopped");
    p.metric(&e.status("stopped")).await;
    eventually(
        "the slot is released",
        Duration::from_secs(30),
        async || !slot_active(&e).await,
    )
    .await;
    assert!(p.terminate().await.success());

    e.exec("INSERT INTO items VALUES (3, 'c')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("stopped")).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        e.ids("items").await,
        [1, 2],
        "still stopped after a restart"
    );
    assert!(!slot_active(&e).await);

    e.docs()
        .set_desired_state(DesiredState::Running)
        .await
        .expect("running");
    p.metric(&e.status("running")).await;
    e.converged("items").await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A second process waits while the first holds the lease, and takes over after a `kill -9`
/// once the lease runs out, with the next owner epoch.
#[tokio::test]
async fn a_second_process_takes_over_after_the_first_dies() {
    require_it!();
    let e = E2e::new("life_lease", ITEMS, never()).await;
    let mut a = e.start(&[]);
    a.ready().await;
    e.streaming().await;
    let mut b = e.start_node("n2");
    b.ready().await;
    a.metric("data_avalanche_owned_pipelines 1").await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    b.metric("data_avalanche_owned_pipelines 0").await;
    let lease = e.lease().await.expect("a lease").doc;
    assert_eq!(
        (lease.holder_node_id.as_str(), lease.owner_epoch),
        ("n1", 1)
    );
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.converged("items").await;

    let status = a.kill().await;
    assert!(!status.success());
    e.exec("INSERT INTO items VALUES (2, 'b')").await;
    b.metric("data_avalanche_owned_pipelines 1").await;
    e.converged("items").await;
    let lease = e.lease().await.expect("a lease").doc;
    assert_eq!(
        (lease.holder_node_id.as_str(), lease.owner_epoch),
        ("n2", 2)
    );
    assert!(b.terminate().await.success());
    e.cleanup().await;
}

/// An Iceberg pipeline is refused (its status is error) while the Delta pipeline beside it runs.
#[tokio::test]
async fn an_iceberg_pipeline_is_refused_beside_a_running_one() {
    require_it!();
    let e = E2e::new(
        "life_iceberg",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            pipelines: r#"  ice:
    id: ice
    source:
      host: 127.0.0.1
      port: @PG@
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: { publication: @SCHEMA@_ice_pub, slot: @SCHEMA@_ice }
    tables: { include: [@SCHEMA@.*] }
    sink:
      type: iceberg
      bucket: @BUCKET@
      prefix: @PREFIX@/ice
      iceberg: { catalog: { glue: { name: "123" } } }"#,
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(
        r#"data_avalanche_pipeline_status{pipeline="ice",source_host="127.0.0.1",source_db="avalanche",status="error"} 1"#,
    )
    .await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.converged("items").await;
    p.metric(&e.status("running")).await;
    assert!(
        p.log().contains("the Iceberg sink ships in v2"),
        "{}",
        p.log()
    );
    let ice_slot = format!("{}_ice", e.schema);
    let slots: i64 = e
        .admin
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&ice_slot],
        )
        .await
        .expect("slots read")
        .get(0);
    assert_eq!(slots, 0, "the refused pipeline touched no source");
    assert!(p.terminate().await.success());
    e.cleanup().await;
}
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -5,7 +5,9 @@
 //! `cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`
 
 mod crashes;
+mod guards;
 mod harness;
+mod lifecycle;
 mod schema;
 mod smoke;
 mod snapshots;
```

- [ ] **Step 2: Run them**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 lifecycle:: guards::
```

Expected:

PASS:

```text
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 19 filtered out; finished in 65.81s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`. Also `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(e2e): scenarios 7-10: pause/stop/resume across a restart, a second process taking over after kill -9, an Iceberg pipeline refused beside a running one, the WAL-lag and buffer guards, a lost slot snapshotted again"
```


### Task 21: The position guard compares a table's last change, not the flush frontier

**Files:**
- Modify: `crates/data-avalanche-core/src/change_set.rs`, `crates/data-avalanche-core/src/sink.rs`, `crates/data-avalanche-pipeline/src/accumulator.rs`, `crates/data-avalanche-pipeline/src/buffer.rs`, `crates/data-avalanche-sink-delta/src/test_support.rs`, `crates/data-avalanche-sink-delta/tests/sink/common.rs`, `crates/data-avalanche-testkit/src/memory_sink.rs`, `crates/data-avalanche/tests/e2e/crashes.rs`

**Interfaces:**
- Consumes: `PositionRange` and `CommitMarker::covers` (core, M3), the accumulator (M6a).
- Produces:
  - `PositionRange { start, end, last_change: SourcePosition }`: the commit position of the
    range's last transaction that wrote the table. `validate` refuses a `last_change` outside
    `start..=end`.
  - `CommitMarker::covers` compares `last_change` (a split slice keeps its own rule; a
    snapshot set is still never skipped). The marker still records `end`, the frontier.
  - The accumulator records each table's last change at the commit (a slice: the transaction's
    start) and hands it on per change set.

The owner's answer to F-1 (2026-10-08): build the fix in M6b. Data-Flow's checkpoint rule
keeps every table's `committed_position` at the flush frontier (an idle table must not pin the
resume point), so the marker doesn't change: only what the guard compares does. A replay carries
the same transactions with the same commit positions, so its last change is one the marker
covers, whatever keep-alive moved its frontier. Measured: the crash scenario's replay was a third
Delta commit before the fix and is skipped after it. The Data-Flow Phase 4 patch is
under "Spec patches".

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-core/src/change_set.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/change_set.rs b/crates/data-avalanche-core/src/change_set.rs
--- a/crates/data-avalanche-core/src/change_set.rs
+++ b/crates/data-avalanche-core/src/change_set.rs
@@ -255,6 +255,7 @@ mod tests {
             position: PositionRange {
                 start: SourcePosition::Pg(Lsn(10)),
                 end: SourcePosition::Pg(Lsn(20)),
+                last_change: SourcePosition::Pg(Lsn(15)),
             },
             split: None,
             snapshot: false,
@@ -362,6 +363,21 @@ mod tests {
         assert!(detail(c).contains("before it starts"));
     }
 
+    #[test]
+    fn the_last_change_is_inside_the_range() {
+        for (lsn, ok) in [(10, true), (15, true), (20, true), (9, false), (21, false)] {
+            let mut c = cs(vec![], vec![]);
+            c.position.last_change = SourcePosition::Pg(Lsn(lsn));
+            match c.validate() {
+                Ok(()) => assert!(ok, "{lsn} accepted"),
+                Err(e) => {
+                    assert!(!ok, "{lsn}: {}", e.detail);
+                    assert!(e.detail.contains("outside the range"), "{}", e.detail);
+                }
+            }
+        }
+    }
+
     #[test]
     fn a_range_across_dialects_names_both() {
         let mut c = cs(vec![], vec![]);
```

Change `crates/data-avalanche-core/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -351,6 +351,10 @@ mod tests {
     }
 
     fn change_set(end: u64, split: Option<SplitInfo>) -> TableChangeSet {
+        ranged(end, end, split)
+    }
+
+    fn ranged(last_change: u64, end: u64, split: Option<SplitInfo>) -> TableChangeSet {
         let t = table();
         TableChangeSet {
             table: t.table().clone(),
@@ -362,6 +366,7 @@ mod tests {
             position: PositionRange {
                 start: SourcePosition::Pg(Lsn(1)),
                 end: SourcePosition::Pg(Lsn(end)),
+                last_change: SourcePosition::Pg(Lsn(last_change)),
             },
             split,
             snapshot: false,
@@ -455,6 +460,20 @@ mod tests {
         assert!(!stored.covers(&change_set(101, None)).unwrap());
     }
 
+    #[test]
+    fn the_guard_compares_the_last_change_not_the_frontier() {
+        let stored = marker(1, 100);
+        assert!(
+            stored.covers(&ranged(90, 120, None)).unwrap(),
+            "a replay whose flush read past the marker carries only what the table has"
+        );
+        assert!(stored.covers(&ranged(100, 120, None)).unwrap());
+        assert!(
+            !stored.covers(&ranged(110, 120, None)).unwrap(),
+            "a transaction past the marker is owed"
+        );
+    }
+
     #[test]
     fn the_guard_never_skips_a_snapshot_set() {
         let mut chunk = change_set(100, None);
```

Change `crates/data-avalanche-pipeline/src/accumulator.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/accumulator.rs b/crates/data-avalanche-pipeline/src/accumulator.rs
--- a/crates/data-avalanche-pipeline/src/accumulator.rs
+++ b/crates/data-avalanche-pipeline/src/accumulator.rs
@@ -505,6 +505,12 @@ mod tests {
                 (&lsn(100), &lsn(300))
             );
         }
+        let last: Vec<_> = f
+            .change_sets
+            .iter()
+            .map(|cs| (cs.table.name.as_str(), &cs.position.last_change))
+            .collect();
+        assert_eq!(last, [("a", &lsn(300)), ("b", &lsn(200))]);
         assert_eq!(f.change_sets[0].upsert_rows(), 2);
         txn(&mut a, 3, 400, &[("b", 5)], t0);
         let f = a.flush();
@@ -661,6 +667,28 @@ mod tests {
         assert_eq!(f.checkpoint, lsn(200));
     }
 
+    #[test]
+    fn a_keepalive_moves_the_end_but_not_the_last_change() {
+        let t0 = Instant::now();
+        let mut a = acc(FlushPolicy::default());
+        txn(&mut a, 1, 200, &[("a", 1)], t0);
+        a.keepalive(&lsn(250), t0).unwrap();
+        let f = a.flush();
+        let cs = &f.change_sets[0];
+        cs.validate().unwrap();
+        assert_eq!(
+            (&cs.position.end, &cs.position.last_change),
+            (&lsn(250), &lsn(200))
+        );
+        assert_eq!(f.checkpoint, lsn(250));
+        txn(&mut a, 2, 300, &[("a", 2)], t0);
+        assert_eq!(
+            a.flush().change_sets[0].position.last_change,
+            lsn(300),
+            "the next flush starts afresh"
+        );
+    }
+
     #[test]
     fn stream_protocol_violations_are_errors() {
         let t0 = Instant::now();
@@ -822,8 +850,12 @@ mod tests {
                 Some((&lsn(200), 1))
             );
             assert_eq!(
-                (&cs.position.start, &cs.position.end),
-                (&lsn(100), &lsn(200))
+                (
+                    &cs.position.start,
+                    &cs.position.end,
+                    &cs.position.last_change
+                ),
+                (&lsn(100), &lsn(200), &lsn(200))
             );
         }
         assert_eq!(
@@ -843,6 +875,7 @@ mod tests {
         a.commit(&boundary(2, 300), t0).unwrap();
         let rest = a.flush();
         assert_eq!(rest.checkpoint, lsn(300));
+        assert_eq!(rest.change_sets[0].position.last_change, lsn(300));
         assert!(
             rest.change_sets[0].split.is_none(),
             "the rest is a regular flush"
```

Change `crates/data-avalanche-pipeline/src/buffer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/buffer.rs b/crates/data-avalanche-pipeline/src/buffer.rs
--- a/crates/data-avalanche-pipeline/src/buffer.rs
+++ b/crates/data-avalanche-pipeline/src/buffer.rs
@@ -1079,6 +1079,7 @@ mod tests {
         PositionRange {
             start: SourcePosition::Pg(Lsn(0)),
             end: SourcePosition::Pg(Lsn(1000)),
+            last_change: SourcePosition::Pg(Lsn(1000)),
         }
     }
 
```

Change `crates/data-avalanche-sink-delta/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/test_support.rs b/crates/data-avalanche-sink-delta/src/test_support.rs
--- a/crates/data-avalanche-sink-delta/src/test_support.rs
+++ b/crates/data-avalanche-sink-delta/src/test_support.rs
@@ -112,6 +112,7 @@ pub(crate) fn change_set(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>)
         position: PositionRange {
             start: SourcePosition::Pg(Lsn(0)),
             end: SourcePosition::Pg(Lsn(end)),
+            last_change: SourcePosition::Pg(Lsn(end)),
         },
         split: None,
         snapshot: false,
```

Change `crates/data-avalanche-sink-delta/tests/sink/common.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/common.rs b/crates/data-avalanche-sink-delta/tests/sink/common.rs
--- a/crates/data-avalanche-sink-delta/tests/sink/common.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/common.rs
@@ -88,6 +88,7 @@ pub fn cs(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeS
         position: PositionRange {
             start: SourcePosition::Pg(Lsn(0)),
             end: SourcePosition::Pg(Lsn(end)),
+            last_change: SourcePosition::Pg(Lsn(end)),
         },
         split: None,
         snapshot: false,
```

Change `crates/data-avalanche-testkit/src/memory_sink.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -531,6 +531,7 @@ mod tests {
             position: PositionRange {
                 start: SourcePosition::Pg(Lsn(0)),
                 end: SourcePosition::Pg(Lsn(end)),
+                last_change: SourcePosition::Pg(Lsn(end)),
             },
             split: None,
             snapshot: false,
@@ -633,6 +634,7 @@ mod tests {
             c.position = PositionRange {
                 start: end.clone(),
                 end: end.clone(),
+                last_change: end.clone(),
             };
             commit(&sink, c, 1).await;
         }
```

Change `crates/data-avalanche/tests/e2e/crashes.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/crashes.rs b/crates/data-avalanche/tests/e2e/crashes.rs
--- a/crates/data-avalanche/tests/e2e/crashes.rs
+++ b/crates/data-avalanche/tests/e2e/crashes.rs
@@ -58,12 +58,11 @@ async fn a_crash_mid_snapshot_resumes_it() {
     e.cleanup().await;
 }
 
-/// A crash after a commit, before its checkpoint: the restart reads the transaction again and
-/// commits it again, which the MERGE makes a no-op on the content. (The commit marker skips a
-/// replay only when its flush ends at the marker; the flush ends at the stream's frontier, which
-/// a keep-alive moves past the transaction.)
+/// A crash after a commit, before its checkpoint: the restart reads the transaction again, and
+/// the position guard skips it: the table's marker covers the transaction's commit, though the
+/// replay's flush may end past the marker (a keep-alive).
 #[tokio::test]
-async fn a_crash_between_a_commit_and_its_checkpoint_replays_it_idempotently() {
+async fn a_crash_between_a_commit_and_its_checkpoint_skips_the_replay() {
     require_it!();
     let e = E2e::new(
         "crash_commit",
@@ -86,11 +85,17 @@ async fn a_crash_between_a_commit_and_its_checkpoint_replays_it_idempotently() {
     assert!(!status.success(), "aborted at the crash point: {status}");
     e.mirrored("items", COLS).await;
     let marker = e.marker("items").await;
+    let commits = e.commits("items").await;
 
     let mut p = e.start(&[]);
     p.ready().await;
     e.acknowledged(&marker).await;
     e.mirrored("items", COLS).await;
+    assert_eq!(
+        e.commits("items").await,
+        commits,
+        "the replay is skipped, not merged again"
+    );
     assert_eq!(e.ids("items").await, (1..=100).collect::<Vec<i64>>());
     e.exec("DELETE FROM items WHERE id > 50").await;
     e.mirrored("items", COLS).await;
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-core -p data-avalanche-pipeline &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 crashes::a_crash_between_a_commit
```

Expected:

FAIL to compile (3 errors); the first:

```text
error[E0560]: struct `change_set::PositionRange` has no field named `last_change`
error[E0609]: no field `last_change` on type `change_set::PositionRange`
… and 1 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/change_set.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/change_set.rs b/crates/data-avalanche-core/src/change_set.rs
--- a/crates/data-avalanche-core/src/change_set.rs
+++ b/crates/data-avalanche-core/src/change_set.rs
@@ -8,12 +8,17 @@ use thiserror::Error;
 use crate::{SourcePosition, TableRef, UNCHANGED_MASK_PREFIX};
 
 /// The stream range a flush covers: changes after `start` (the previous checkpoint) up to and
-/// including `end`. `end` is what the sink records as the table's `committed_position` and what
-/// the position guard compares (Delivery §3).
+/// including `end`. `end` is what the sink records as the table's `committed_position` (the flush
+/// frontier, Data-Flow §Checkpoint rule); `last_change` is what the position guard compares
+/// (Delivery §3).
 #[derive(Debug, Clone, PartialEq, Eq)]
 pub struct PositionRange {
     pub start: SourcePosition,
     pub end: SourcePosition,
+    /// The commit position of the range's last transaction that wrote this table: at most `end`,
+    /// which a keep-alive or another table's transaction moves past it. A replay of the same
+    /// transactions has the same `last_change` whatever its frontier, so the guard skips it.
+    pub last_change: SourcePosition,
 }
 
 /// Marks a slice of an oversized transaction flushed before the source commit (split commits,
@@ -40,7 +45,8 @@ pub struct SplitInfo {
 /// - every delete batch starts with the identifier columns;
 /// - a key appears at most once across `upserts` and `deletes` (a PK-changing UPDATE is two keys);
 /// - identifier values are never NULL;
-/// - the range doesn't run backwards, and a slice's range ends at its `txn_start`;
+/// - the range doesn't run backwards, `last_change` is inside it, and a slice's range ends at its
+///   `txn_start`;
 /// - a snapshot set only upserts: no deletes, no truncate, not a slice.
 #[derive(Debug, Clone)]
 pub struct TableChangeSet {
@@ -151,6 +157,20 @@ impl TableChangeSet {
                 )));
             }
         }
+        let last = &self.position.last_change;
+        match (last.covers(start), end.covers(last)) {
+            (Ok(true), Ok(true)) => {}
+            (Err(e), _) | (_, Err(e)) => {
+                return Err(err(format!(
+                    "last change {last} doesn't compare with the range from {start} to {end}: {e}"
+                )));
+            }
+            _ => {
+                return Err(err(format!(
+                    "last change {last} is outside the range from {start} to {end}"
+                )));
+            }
+        }
         if self.snapshot && (self.truncate || self.split.is_some() || self.delete_rows() > 0) {
             return Err(err(
                 "a snapshot set carries upserts only: no deletes, truncate or slice".into(),
```

Change `crates/data-avalanche-core/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -176,6 +176,10 @@ impl CommitMarker {
     /// commit is skipped (Delivery §3). Committing again would only repeat an idempotent write,
     /// except for replayed history, which the guard is what keeps out.
     ///
+    /// The marker covers the set's `last_change`, not its `end`: the end is the flush's frontier,
+    /// which a replay's flush may read past the marker (a keep-alive, another table's
+    /// transaction) while carrying only the transactions the table already has.
+    ///
     /// A split slice ends at its pinned `txn_start`, which the marker equals from the transaction's
     /// first slice on, so "covers the end" can't tell a slice still owed from one already applied.
     /// It is skipped only when the marker is strictly past `txn_start`: the transaction's commit (or
@@ -191,7 +195,7 @@ impl CommitMarker {
         let stored = &self.committed_position;
         match &cs.split {
             Some(split) => Ok(stored.covers(&split.txn_start)? && !split.txn_start.covers(stored)?),
-            None => stored.covers(&cs.position.end),
+            None => stored.covers(&cs.position.last_change),
         }
     }
 }
```

Change `crates/data-avalanche-pipeline/src/accumulator.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/accumulator.rs b/crates/data-avalanche-pipeline/src/accumulator.rs
--- a/crates/data-avalanche-pipeline/src/accumulator.rs
+++ b/crates/data-avalanche-pipeline/src/accumulator.rs
@@ -4,7 +4,10 @@
 //! Positions: the **frontier** is the commit position of the last complete transaction (or a
 //! keep-alive past it); the **checkpoint** is the frontier of the last flush. A flush covers
 //! `checkpoint..=frontier` and moves the checkpoint to the frontier, which is where the pipeline
-//! resumes once every change set of the flush is committed.
+//! resumes once every change set of the flush is committed. Each table's change set also carries
+//! its **last change**: the commit position of the last transaction that wrote the table, which
+//! the sink's position guard compares (a replay's frontier may be past the table's marker while
+//! its transactions aren't).
 
 use std::collections::{BTreeMap, BTreeSet};
 use std::time::{Duration, Instant};
@@ -42,6 +45,9 @@ pub struct Accumulator {
     tables: BTreeMap<TableRef, TableBuffer>,
     /// Halted tables: their events are dropped.
     halted: BTreeSet<TableRef>,
+    /// Per table with changes no flush has taken: the commit position of the last transaction
+    /// that wrote it (a slice's: the transaction's start).
+    last_change: BTreeMap<TableRef, SourcePosition>,
     txn: Option<OpenTxn>,
     checkpoint: SourcePosition,
     frontier: SourcePosition,
@@ -58,6 +64,7 @@ impl Accumulator {
             limits: TxnLimits::default(),
             tables: BTreeMap::new(),
             halted: BTreeSet::new(),
+            last_change: BTreeMap::new(),
             txn: None,
             checkpoint: resume_from.clone(),
             frontier: resume_from,
@@ -120,6 +127,7 @@ impl Accumulator {
     /// Calling it again changes nothing.
     pub fn halt_table(&mut self, table: &TableRef) {
         self.tables.remove(table);
+        self.last_change.remove(table);
         self.halted.insert(table.clone());
     }
 
@@ -277,9 +285,7 @@ impl Accumulator {
         }
         self.advance(&boundary.commit_pos)?;
         let txn = self.txn.take().expect("checked above");
-        for buffer in self.tables.values_mut() {
-            buffer.commit_txn();
-        }
+        self.commit_open(&boundary.commit_pos);
         let since = txn.first_event_at.unwrap_or(now);
         self.flushable_since = Some(self.flushable_since.map_or(since, |s| s.min(since)));
         Ok(())
@@ -347,19 +353,41 @@ impl Accumulator {
         None
     }
 
+    /// Moves every buffer's open transaction to its committed layer; a table it wrote has its
+    /// last change at `at`.
+    fn commit_open(&mut self, at: &SourcePosition) {
+        for (table, buffer) in &mut self.tables {
+            if buffer.open_events() > 0 {
+                self.last_change.insert(table.clone(), at.clone());
+            }
+            buffer.commit_txn();
+        }
+    }
+
+    /// Takes the committed layers of the tables that have changes as change sets.
+    fn take(&mut self, split: Option<SplitInfo>) -> Vec<TableChangeSet> {
+        let (start, end) = (&self.checkpoint, &self.frontier);
+        self.tables
+            .iter_mut()
+            .filter(|(_, b)| b.committed_events() > 0)
+            .map(|(table, b)| {
+                let position = PositionRange {
+                    start: start.clone(),
+                    end: end.clone(),
+                    last_change: self
+                        .last_change
+                        .remove(table)
+                        .unwrap_or_else(|| end.clone()),
+                };
+                b.flush(&self.lineage, position, split.clone())
+            })
+            .collect()
+    }
+
     /// Takes every complete transaction as change sets covering `checkpoint..=frontier`; an open
     /// transaction stays buffered.
     pub fn flush(&mut self) -> Flush {
-        let position = PositionRange {
-            start: self.checkpoint.clone(),
-            end: self.frontier.clone(),
-        };
-        let change_sets = self
-            .tables
-            .values_mut()
-            .filter(|b| b.committed_events() > 0)
-            .map(|b| b.flush(&self.lineage, position.clone(), None))
-            .collect();
+        let change_sets = self.take(None);
         self.checkpoint = self.frontier.clone();
         self.flushable_since = None;
         Flush {
@@ -383,17 +411,9 @@ impl Accumulator {
             txn_start: self.frontier.clone(),
             slice: txn.slices,
         };
-        let position = PositionRange {
-            start: self.checkpoint.clone(),
-            end: self.frontier.clone(),
-        };
-        let mut change_sets = Vec::new();
-        for b in self.tables.values_mut() {
-            b.commit_txn();
-            if b.committed_events() > 0 {
-                change_sets.push(b.flush(&self.lineage, position.clone(), Some(split.clone())));
-            }
-        }
+        let start = split.txn_start.clone();
+        self.commit_open(&start);
+        let change_sets = self.take(Some(split));
         self.checkpoint = self.frontier.clone();
         self.flushable_since = None;
         Ok(Flush {
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-core -p data-avalanche-pipeline &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 crashes::a_crash_between_a_commit
```

Expected:

PASS:

```text
test result: ok. 149 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.03s
test result: ok. 186 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.63s
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.14s
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 9.45s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.83s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 8.32s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 24 filtered out; finished in 51.65s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "fix(core): the position guard compares a table's last change, not the flush frontier: a replay whose flush read past the marker is skipped (F-1)"
```


### Task 22: The stream watchdog, and a bounded catalog connect

**Files:**
- Create: `crates/data-avalanche-pipeline/src/watchdog.rs`
- Modify: `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-core/src/lib.rs`, `crates/data-avalanche-core/src/source.rs`, `crates/data-avalanche-pipeline/src/lib.rs`, `crates/data-avalanche-pipeline/src/session.rs`, `crates/data-avalanche-pipeline/src/session/snapshot.rs`, `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-source-pg/src/connect.rs`, `crates/data-avalanche-source-pg/src/connector.rs`, `crates/data-avalanche-source-pg/src/slot.rs`, `crates/data-avalanche-source-pg/tests/pg/slot.rs`, `crates/data-avalanche-testkit/src/fake_source.rs`, `crates/data-avalanche/tests/e2e/guards.rs`, `crates/data-avalanche/tests/e2e/harness.rs`

**Interfaces:**
- Consumes: the session's heartbeat (M6a), `SourceConnector` (core), `Connector` (source-pg).
- Produces:
  - `SourceConnector::retained(&self) -> Result<Option<RetainedLog>, SourceError>`, replacing
    `retained_bytes`; `RetainedLog { bytes: u64, streamed: bool }` (PostgreSQL: the slot's
    `active`).
  - The session runs the heartbeat's read beside its loop (a `JoinSet`, one read at a time).
    A stream silent for `SILENT_HEARTBEATS` (3) heartbeats while 3 reads in a row failed or
    found no reader ends the session with `SourceError::Transient("the source isn't
    answering: …")`, and the worker reconnects. A read still out counts neither way.
  - `Connector::connect` is bounded by `timeouts.connect` as a whole (TCP, TLS, startup,
    authentication).
  - The worker sets `reconnects` and `Phase::Reconnecting { error }` in one status update; the
    controller logs each reconnect with its reason.
  - testkit: `Method::Retained`, `FakeSource::detach_stream`, `FakeSource::stall_retained`.
  - The E2E harness: `Options::source` (keys beyond the connection).

The owner's answer to F-3 (2026-10-08): DA-SRC-16 in M6b.
- **Measured before the fix:** PostgreSQL 14 frozen with `docker pause` for 150 s under a running
  pipeline (heartbeat 5 s). Status stayed `running`, the reconnects counter stayed 0, and
  nothing was logged: a heartbeat read's transient failure is dropped silently. A paused
  container still answers TCP, so a keep-alive wouldn't have told either.
- **The connect:** tokio-postgres's `connect_timeout` bounds only the TCP connect. Against a
  server that accepts and never answers, the startup waits forever, holding the shared
  connection's lock (`a_connect_to_a_server_that_never_answers_times_out` fails without the
  bound).
- **After the fix:** the E2E scenario (heartbeat 1 s, timeouts 2 s) logs the reconnect and its
  reason during the freeze and converges after the unpause, 27 s for the whole scenario.
- The config patch notes the watchdog under `heartbeat` and `timeouts.connect`.
- **Also run at execution:** `cargo test -p data-avalanche-source-pg -- --test-threads=1 tls::`
  (with `DATA_AVALANCHE_IT=1`). The bounded connect wraps the TLS connect too, and the planning
  workspace couldn't run these tests: they read `test-infra/.tls`, which only the repository
  has.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -469,7 +469,7 @@ mod tests {
                 warnings: vec![],
             })
         }
-        async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
+        async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
             Ok(None)
         }
         async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
```

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -23,6 +23,7 @@ pub mod status;
 mod test_support;
 pub mod worker;
 mod writer;
+mod watchdog;
 
 pub use accumulator::{Accumulator, Flush};
 pub use backoff::Backoff;
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -1805,13 +1805,11 @@ mod tests {
                 .collect()
         };
         let driver = async {
-            w.source.fail(
-                Method::RetainedBytes,
-                SourceError::Transient("reset".into()),
-            );
+            w.source
+                .fail(Method::Retained, SourceError::Transient("reset".into()));
             for _ in 0..2 {
                 w.source.fail(
-                    Method::RetainedBytes,
+                    Method::Retained,
                     SourceError::Permanent("permission denied".into()),
                 );
             }
@@ -1826,7 +1824,7 @@ mod tests {
             w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
             durable(&status, w.source.wal_end()).await;
             w.source.fail(
-                Method::RetainedBytes,
+                Method::Retained,
                 SourceError::Permanent("permission denied again".into()),
             );
             wait_until("a new streak warns", || blind().len() == 2).await;
@@ -1840,6 +1838,71 @@ mod tests {
         assert_eq!(w.ids("a"), [1, 2]);
     }
 
+    /// A source that stops answering (a frozen server, DA-SRC-16): the stream is silent and the
+    /// heartbeat's reads time out, so the session ends as a transient source error and the
+    /// worker reconnects.
+    #[tokio::test]
+    async fn a_silent_stream_whose_source_doesn_t_answer_ends_the_session() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let p = pipeline("", "flush: { seconds: 0 }", "");
+        let (mut core, _control, _status) = w.core(&p, &["a"]);
+        core.settings.heartbeat = Duration::from_millis(5);
+        for _ in 0..10 {
+            w.source.fail(
+                Method::Retained,
+                SourceError::Transient("measure the slot's WAL timed out after 30s".into()),
+            );
+        }
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        let end = s.run().await.unwrap();
+        let End::Source(SourceError::Transient(why)) = end else {
+            panic!("{end:?}")
+        };
+        assert!(why.starts_with("the source isn't answering"), "{why}");
+        assert!(why.contains("timed out after 30s"), "{why}");
+    }
+
+    /// The server dropped the stream without the client hearing of it: the slot has no reader.
+    #[tokio::test]
+    async fn a_silent_stream_on_a_slot_no_stream_reads_ends_the_session() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let p = pipeline("", "flush: { seconds: 0 }", "");
+        let (mut core, _control, _status) = w.core(&p, &["a"]);
+        core.settings.heartbeat = Duration::from_millis(5);
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        w.source.set_retained(Some(0));
+        w.source.detach_stream();
+        let end = s.run().await.unwrap();
+        let End::Source(SourceError::Transient(why)) = end else {
+            panic!("{end:?}")
+        };
+        assert!(why.contains("no stream reads the slot"), "{why}");
+    }
+
+    /// A heartbeat read that never comes back holds up neither the stream nor a drain, and
+    /// counts against nothing: it may wait behind a chunk read on a shared connection.
+    #[tokio::test]
+    async fn a_stalled_heartbeat_read_holds_up_nothing() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let p = pipeline("", "flush: { seconds: 0 }", "");
+        let (mut core, control, status) = w.core(&p, &["a"]);
+        core.settings.heartbeat = Duration::from_millis(5);
+        w.source.stall_retained();
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        let driver = async {
+            tokio::time::sleep(Duration::from_millis(100)).await;
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            durable(&status, w.source.wal_end()).await;
+            control.send(Command::Drain).await.unwrap();
+        };
+        let (end, ()) = tokio::join!(s.run(), driver);
+        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
+        assert_eq!(w.ids("a"), [1]);
+    }
+
     /// The blind warning names what is blind: the bound when `max_wal_lag_bytes` is set, else the
     /// reported lag.
     #[tokio::test]
@@ -1856,11 +1919,8 @@ mod tests {
             let p = pipeline("", "flush: { seconds: 0 }", postgres);
             let (mut core, _control, status) = w.core(&p, &["a"]);
             let mut s = Session::open(&mut core).await.unwrap().unwrap();
-            w.source.fail(
-                Method::RetainedBytes,
-                SourceError::Permanent("permission denied".into()),
-            );
-            s.wal_lag().await.unwrap();
+            s.wal_lag(Ok(Err(SourceError::Permanent("permission denied".into()))))
+                .unwrap();
             let warnings = status.borrow().warnings.clone();
             assert!(
                 matches!(&warnings[..], [m] if m.starts_with("the WAL-lag guard is blind") && m.ends_with(tail)),
```

Change `crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -1080,9 +1080,9 @@ mod tests {
         );
     }
 
-    /// Chunk reads share the source's connection with the heartbeat's `retained_bytes`, which the
-    /// session awaits inline: a read hands back its rows only while that call waits, so the reads
-    /// must be polled while the session awaits it (Task 13 review I3).
+    /// Chunk reads share the source's connection with the heartbeat's `retained`: a read hands
+    /// back its rows only while that call waits, so the reads must be polled while the call is
+    /// out (Task 13 review I3).
     #[tokio::test]
     async fn a_chunk_read_goes_on_while_the_session_awaits_the_source() {
         let w = World::new(&["a"]);
```

Create `crates/data-avalanche-pipeline/src/watchdog.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn failed(w: &mut Watchdog) {
        w.answered(Some("timed out".into()));
    }

    #[test]
    fn a_silent_stream_while_the_reads_fail_ends_the_session() {
        let mut w = Watchdog::default();
        for _ in 0..SILENT_HEARTBEATS - 1 {
            failed(&mut w);
            assert_eq!(w.beat(true), None);
        }
        failed(&mut w);
        let why = w.beat(true).unwrap();
        assert!(why.contains("no stream message for 3 heartbeats"), "{why}");
        assert!(why.contains("timed out"), "{why}");
    }

    #[test]
    fn silence_alone_is_an_idle_source() {
        let mut w = Watchdog::default();
        for _ in 0..10 {
            w.answered(None);
            assert_eq!(w.beat(true), None);
        }
    }

    #[test]
    fn failing_reads_beside_a_live_stream_are_the_catalog_s_trouble() {
        let mut w = Watchdog::default();
        for _ in 0..10 {
            failed(&mut w);
            w.heard();
            assert_eq!(w.beat(true), None);
        }
    }

    #[test]
    fn a_session_that_doesn_t_read_isn_t_silent() {
        let mut w = Watchdog::default();
        for _ in 0..10 {
            failed(&mut w);
            assert_eq!(w.beat(false), None, "paused, or waiting for room");
        }
        failed(&mut w);
        assert_eq!(w.beat(true), None, "the count starts again");
    }

    #[test]
    fn an_answer_or_a_message_starts_the_count_again() {
        let mut w = Watchdog::default();
        for _ in 0..SILENT_HEARTBEATS - 1 {
            failed(&mut w);
            w.beat(true);
        }
        w.answered(None);
        failed(&mut w);
        assert_eq!(w.beat(true), None, "one unanswered read since the answer");
        w.heard();
        failed(&mut w);
        failed(&mut w);
        assert_eq!(w.beat(true), None, "the message reset the silence");
    }
}
```

Change `crates/data-avalanche-source-pg/src/connect.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connect.rs b/crates/data-avalanche-source-pg/src/connect.rs
--- a/crates/data-avalanche-source-pg/src/connect.rs
+++ b/crates/data-avalanche-source-pg/src/connect.rs
@@ -267,6 +267,33 @@ mod tests {
         );
     }
 
+    /// A server that accepts the connection and never answers: the startup times out.
+    #[tokio::test]
+    async fn a_connect_to_a_server_that_never_answers_times_out() {
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let port = listener.local_addr().unwrap().port();
+        let held = tokio::spawn(async move {
+            let mut sockets = vec![];
+            while let Ok((s, _)) = listener.accept().await {
+                sockets.push(s);
+            }
+        });
+        let mut source = pipeline("public.t").source;
+        source.port = port;
+        source.timeouts.connect = Duration::from_millis(300);
+        let c = Connector::new(&source).unwrap();
+        let err = tokio::time::timeout(Duration::from_secs(5), c.client())
+            .await
+            .expect("the startup is bounded by timeouts.connect")
+            .unwrap_err();
+        assert!(err.is_transient(), "{err}");
+        assert!(
+            err.to_string().contains("connect to the source timed out"),
+            "{err}"
+        );
+        held.abort();
+    }
+
     #[test]
     fn the_replication_connection_pins_the_same_session() {
         let c = Connector::new(&pipeline("public.t").source).unwrap();
```

Change `crates/data-avalanche-source-pg/tests/pg/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/slot.rs b/crates/data-avalanche-source-pg/tests/pg/slot.rs
--- a/crates/data-avalanche-source-pg/tests/pg/slot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/slot.rs
@@ -8,7 +8,7 @@ use data_avalanche_core::{SnapshotReason, SourceError, SourcePosition, StartPlan
 use data_avalanche_testkit::require_it;
 use tokio_postgres::Client;
 
-use crate::common::{Names, admin, cleanup, connector, fresh, pipeline};
+use crate::common::{Names, admin, cleanup, connector, first_stream, fresh, pipeline};
 
 const DDL: &str = "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY);
                    CREATE TABLE c (id int PRIMARY KEY);";
@@ -423,13 +423,43 @@ async fn a_root_publication_of_tracked_partitions_is_refused() {
     cleanup(&a, &n).await;
 }
 
+/// The heartbeat's read says whether a stream reads the slot: the stream watchdog's evidence
+/// that a silent stream is still there (DA-SRC-16).
+#[tokio::test]
+async fn the_retained_log_says_whether_a_stream_reads_the_slot() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(&a, "slot_streamed", DDL).await;
+    let c = connector(&pipeline(&n, &["*"], "", ""));
+    let streamed = async |want: bool| {
+        let deadline = std::time::Instant::now() + Duration::from_secs(30);
+        loop {
+            let log = c.retained().await.unwrap().expect("the slot");
+            if log.streamed == want {
+                return;
+            }
+            assert!(
+                std::time::Instant::now() < deadline,
+                "streamed stayed {}",
+                log.streamed
+            );
+            tokio::time::sleep(Duration::from_millis(100)).await;
+        }
+    };
+    let (stream, _, _) = first_stream(&c).await;
+    streamed(true).await;
+    drop(stream);
+    streamed(false).await;
+    cleanup(&a, &n).await;
+}
+
 #[tokio::test]
 async fn reset_teardown_and_retained_wal() {
     require_it!();
     let a = admin().await;
     let n = fresh(&a, "slot_reset", DDL).await;
     let c = connector(&pipeline(&n, &["*"], "", ""));
-    assert_eq!(c.retained_bytes().await.unwrap(), None, "no slot yet");
+    assert_eq!(c.retained().await.unwrap(), None, "no slot yet");
     let p = c.prepare(None, &[n.table("a")]).await.unwrap();
     let start = from(&p.plan, SnapshotReason::FirstStart);
     a.execute(
@@ -441,8 +471,9 @@ async fn reset_teardown_and_retained_wal() {
     )
     .await
     .unwrap();
-    let retained = c.retained_bytes().await.unwrap().unwrap();
-    assert!(retained > 0, "WAL written since the slot's position");
+    let retained = c.retained().await.unwrap().unwrap();
+    assert!(retained.bytes > 0, "WAL written since the slot's position");
+    assert!(!retained.streamed, "no stream reads the slot");
 
     let SourcePosition::Pg(fresh_at) = c.reset_position().await.unwrap() else {
         panic!("a PostgreSQL position")
```

Change `crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -32,8 +32,8 @@ use async_trait::async_trait;
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
     CdcEvent, ChangeStream, Chunk, ChunkVisibility, ColumnSchema, ColumnValue, HaltReason,
-    InFlightTxn, KeysetCursor, Op, Preflight, Prepared, RescanReport, ResolvedTables, Row,
-    SchemaChange, SchemaDelta, SchemaHint, SnapshotReason, SourceConnector, SourceError,
+    InFlightTxn, KeysetCursor, Op, Preflight, Prepared, RescanReport, ResolvedTables, RetainedLog,
+    Row, SchemaChange, SchemaDelta, SchemaHint, SnapshotReason, SourceConnector, SourceError,
     SourcePosition, SourceSeq, StartPlan, StreamItem, TableRef, TableSchema, TransactionBoundary,
     XidSnapshot, identifier_values,
 };
@@ -46,7 +46,7 @@ pub enum Method {
     ResolveTables,
     Prepare,
     ResetPosition,
-    RetainedBytes,
+    Retained,
     CurrentPosition,
     InFlight,
     ReadChunk,
@@ -228,6 +228,10 @@ struct Db {
     slot_lost: bool,
     added: Vec<TableRef>,
     retained: Option<Option<u64>>,
+    /// [`FakeSource::detach_stream`].
+    detached: bool,
+    /// [`FakeSource::stall_retained`].
+    retained_stalled: bool,
     faults: Vec<(Method, SourceError)>,
     hold_reads: bool,
     hold_stream: bool,
@@ -280,6 +284,8 @@ impl FakeSource {
             slot_lost: false,
             added: vec![],
             retained: None,
+            detached: false,
+            retained_stalled: false,
             faults: vec![],
             hold_reads: false,
             hold_stream: false,
@@ -425,11 +431,22 @@ impl FakeSource {
         self.db().added = tables.to_vec();
     }
 
-    /// What `retained_bytes` reports from now on (by default, the log from the slot to its end).
+    /// The bytes `retained` reports from now on (by default, the log from the slot to its end).
     pub fn set_retained(&self, bytes: Option<u64>) {
         self.db().retained = Some(bytes);
     }
 
+    /// `retained` reports that no stream reads the slot, whatever the open streams do: a stream
+    /// the source dropped without the client hearing of it (DA-SRC-16).
+    pub fn detach_stream(&self) {
+        self.db().detached = true;
+    }
+
+    /// `retained` calls wait forever from now on: a source that accepts and never answers.
+    pub fn stall_retained(&self) {
+        self.db().retained_stalled = true;
+    }
+
     /// The next call of `method` fails with `error`.
     pub fn fail(&self, method: Method, error: SourceError) {
         self.db().faults.push((method, error));
@@ -441,7 +458,7 @@ impl FakeSource {
         self.db().hold_reads = true;
     }
 
-    /// Chunk reads, `retained_bytes` and `in_flight` share one connection from now on, as they do
+    /// Chunk reads, `retained` and `in_flight` share one connection from now on, as they do
     /// on PostgreSQL's catalog client, whose response channel holds one message: a read that has
     /// its rows hands them back only while one of those calls waits on the connection, and the
     /// call waits until no read is half done. A caller that awaits such a call without polling
@@ -892,14 +909,21 @@ impl SourceConnector for FakeSource {
         Ok(SourcePosition::Pg(db.wal_end))
     }
 
-    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
-        self.take_fault(Method::RetainedBytes)?;
+    async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
+        self.take_fault(Method::Retained)?;
+        if self.db().retained_stalled {
+            std::future::pending::<()>().await;
+        }
         self.connection().await;
         let db = self.db();
-        Ok(match db.retained {
+        let bytes = match db.retained {
             Some(r) => r,
             None => db.slot.map(|s| db.wal_end.0.saturating_sub(s.0)),
-        })
+        };
+        Ok(bytes.map(|bytes| RetainedLog {
+            bytes,
+            streamed: !db.detached,
+        }))
     }
 
     async fn current_position(&self) -> Result<SourcePosition, SourceError> {
```

Change `crates/data-avalanche/tests/e2e/guards.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/guards.rs b/crates/data-avalanche/tests/e2e/guards.rs
--- a/crates/data-avalanche/tests/e2e/guards.rs
+++ b/crates/data-avalanche/tests/e2e/guards.rs
@@ -1,9 +1,12 @@
-//! Scenario 10, the guards: the WAL a slot retains, the open transaction's buffer, and a slot
-//! whose WAL is gone.
+//! Scenario 10, the guards: the WAL a slot retains, the open transaction's buffer, a slot whose
+//! WAL is gone, and a source that stops answering.
+
+use std::process::{Command, Stdio};
+use std::time::Duration;
 
 use data_avalanche_testkit::require_it;
 
-use crate::harness::{E2e, Options, admin};
+use crate::harness::{E2e, Options, admin, eventually, pg_port};
 
 const TABLES: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
      CREATE TABLE pad (id bigserial PRIMARY KEY, doc text);";
@@ -156,3 +159,86 @@ async fn a_slot_whose_wal_is_gone_is_snapshotted_again() {
     assert!(p.terminate().await.success());
     e.cleanup().await;
 }
+
+/// The server under test, frozen (`docker pause`) until dropped, on every path out of the test:
+/// a server that keeps its connections open and never answers.
+struct Frozen(String);
+
+impl Frozen {
+    fn new() -> Self {
+        // test-infra/compose.yaml: project `data-avalanche-test`, `postgres-<major>` on 554<major>.
+        let name = format!("data-avalanche-test-postgres-{}-1", pg_port() - 55400);
+        docker("pause", &name);
+        Self(name)
+    }
+}
+
+impl Drop for Frozen {
+    fn drop(&mut self) {
+        docker("unpause", &self.0);
+    }
+}
+
+fn docker(verb: &str, container: &str) {
+    let status = Command::new("docker")
+        .args([verb, container])
+        .stdout(Stdio::null())
+        .status()
+        .expect("docker runs");
+    assert!(status.success(), "docker {verb} {container}: {status}");
+}
+
+/// The reconnects counter's value.
+fn reconnects(metrics: &str) -> u64 {
+    metrics
+        .lines()
+        .find_map(|l| {
+            l.strip_prefix("data_avalanche_source_reconnects_total{")
+                .and_then(|rest| rest.rsplit_once(' '))
+                .map(|(_, v)| v.parse().expect("a count"))
+        })
+        .expect("the reconnects series")
+}
+
+/// A source that stops answering (DA-SRC-16): the stream goes silent and the heartbeat's reads
+/// time out, so the session ends and the pipeline reconnects, again and again while the server
+/// is frozen; once it answers, the pipeline streams again.
+#[tokio::test]
+async fn a_source_that_stops_answering_is_reconnected_to() {
+    require_it!();
+    let e = E2e::new(
+        "guard_frozen",
+        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
+        Options {
+            tables: ", snapshots: { mode: never }",
+            source: "timeouts: { connect: 2s, command: 2s }",
+            postgres: ", heartbeat: 1s",
+            ..Options::default()
+        },
+    )
+    .await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.streaming().await;
+    e.exec("INSERT INTO items VALUES (1, 'before')").await;
+    e.mirrored("items", &["id", "doc"]).await;
+    assert_eq!(reconnects(&p.metrics().await), 0);
+    {
+        let _frozen = Frozen::new();
+        eventually("a reconnect", Duration::from_secs(60), async || {
+            reconnects(&p.metrics().await) >= 1
+        })
+        .await;
+        p.metric(&e.status("running")).await;
+    }
+    e.exec("INSERT INTO items VALUES (2, 'after')").await;
+    e.mirrored("items", &["id", "doc"]).await;
+    assert!(
+        p.log()
+            .contains("reconnecting to the source: the source isn't answering"),
+        "{}",
+        p.log()
+    );
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -70,6 +70,8 @@ pub struct Options<'a> {
     pub tables: &'a str,
     /// `sink` entries, e.g. `, flush: { rows: 10 }`.
     pub sink: &'a str,
+    /// `source` keys beyond the connection, one YAML line, e.g. `timeouts: { command: 2s }`.
+    pub source: &'a str,
     /// `source.postgres` entries.
     pub postgres: &'a str,
     /// Pipeline keys beyond `source`, `tables` and `sink`, one YAML line, e.g.
@@ -167,6 +169,7 @@ pipelines:
       user: avalanche
       password: avalanche
       ssl_mode: disable
+      {source}
       postgres: {{ publication: {schema}_pub, slot: {schema}{postgres} }}
     tables: {{ include: [{schema}.*]{tables} }}
     sink: {{ type: delta, bucket: {bucket}, prefix: {prefix}/lake, delta: {{}}, flush: {{ seconds: 1 }}{sink} }}
@@ -178,6 +181,7 @@ pipelines:
             test = self.test,
             pg = pg_port(),
             schema = self.schema,
+            source = o.source,
             postgres = o.postgres,
             tables = o.tables,
             sink = o.sink,
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --lib &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-source-pg -- retained a_connect_to &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 guards::a_source_that_stops
```

Expected:

FAIL to compile (3 errors); the first:

```text
error[E0432]: unresolved import `data_avalanche_core::RetainedLog`
error[E0407]: method `retained` is not a member of trait `SourceConnector`
error[E0046]: not all trait items implemented, missing: `retained_bytes`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -171,6 +171,8 @@ struct Running {
     paused: bool,
     /// The warnings already logged.
     logged: HashSet<String>,
+    /// The worker's reconnects already logged.
+    reconnects: u64,
 }
 
 impl PipelineController {
@@ -489,6 +491,7 @@ impl PipelineController {
             permit: Some(permit),
             paused: false,
             logged: HashSet::new(),
+            reconnects: 0,
         })
     }
 
@@ -530,7 +533,7 @@ impl PipelineController {
         }
     }
 
-    /// The worker's status into the metrics, and its new warnings into the log.
+    /// The worker's status into the metrics, and its new warnings and reconnects into the log.
     fn observe(&self, w: &mut Running) {
         let st = w.handle.status.borrow().clone();
         for warning in &st.warnings {
@@ -538,6 +541,12 @@ impl PipelineController {
                 tracing::warn!(pipeline = %self.pipeline.name, "{warning}");
             }
         }
+        if st.reconnects > w.reconnects {
+            w.reconnects = st.reconnects;
+            if let Phase::Reconnecting { error } = &st.phase {
+                tracing::warn!(pipeline = %self.pipeline.name, "reconnecting to the source: {error}");
+            }
+        }
         let m = &self.metrics;
         m.wal_lag_bytes(st.retained_bytes);
         m.halted_tables(st.halted.len());
```

Change `crates/data-avalanche-core/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/lib.rs b/crates/data-avalanche-core/src/lib.rs
--- a/crates/data-avalanche-core/src/lib.rs
+++ b/crates/data-avalanche-core/src/lib.rs
@@ -39,8 +39,8 @@ pub use sink::{
 };
 pub use source::{
     ChangeStream, Chunk, ChunkVisibility, InFlightTxn, KeysetCursor, Preflight, Prepared,
-    RescanReport, ResolvedTables, SchemaHint, SnapshotReason, SourceConnector, SourceError,
-    StartPlan, StreamItem, XidSnapshot, identifier_values,
+    RescanReport, ResolvedTables, RetainedLog, SchemaHint, SnapshotReason, SourceConnector,
+    SourceError, StartPlan, StreamItem, XidSnapshot, identifier_values,
 };
 pub use table::{
     INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
```

Change `crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -230,6 +230,17 @@ pub enum StreamItem {
 }
 
 /// The tables channel D found changed since `known` (`Schema_Change…` §3).
+/// The log the source keeps for the pipeline, read every heartbeat (PostgreSQL: the slot).
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub struct RetainedLog {
+    /// Bytes kept on the source's disk (PostgreSQL: the WAL from the slot's `restart_lsn`, the
+    /// `max_wal_lag_bytes` guard).
+    pub bytes: u64,
+    /// A stream reads it now (PostgreSQL: the slot is `active`). A session whose stream is open
+    /// but which reads `false` has lost its stream without hearing of it (DA-SRC-16).
+    pub streamed: bool,
+}
+
 #[derive(Debug, Clone, PartialEq, Eq, Default)]
 pub struct RescanReport {
     /// Tables gone from the source catalog: each halts with `TABLE_DROPPED`.
@@ -265,10 +276,8 @@ pub trait SourceConnector: Send + Sync {
     /// new consistent position to snapshot from and stream after.
     async fn reset_position(&self) -> Result<SourcePosition, SourceError>;
 
-    /// How many bytes of log the source keeps on disk for the pipeline (PostgreSQL: the WAL from
-    /// the slot's `restart_lsn`, the `max_wal_lag_bytes` guard). `None` when the source keeps
-    /// nothing for it.
-    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError>;
+    /// The log the source keeps for the pipeline. `None` when the source keeps nothing for it.
+    async fn retained(&self) -> Result<Option<RetainedLog>, SourceError>;
 
     /// The source's current position: a snapshot chunk's low and high watermarks, and the point
     /// the stream must pass before a rescan's change is applied.
```

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -21,9 +21,9 @@ mod settings;
 pub mod status;
 #[cfg(test)]
 mod test_support;
+mod watchdog;
 pub mod worker;
 mod writer;
-mod watchdog;
 
 pub use accumulator::{Accumulator, Flush};
 pub use backoff::Backoff;
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -14,19 +14,20 @@ use std::sync::Arc;
 use std::time::Instant;
 
 use data_avalanche_core::{
-    CdcEvent, ChangeStream, ColumnValue, HaltReason, Op, RescanReport, Row, SchemaHint, Sink,
-    SourceConnector, SourceError, SourcePosition, SourceSeq, StreamItem, TableRef, TableSchema,
-    TransactionBoundary, identifier_values,
+    CdcEvent, ChangeStream, ColumnValue, HaltReason, Op, RescanReport, RetainedLog, Row,
+    SchemaHint, Sink, SourceConnector, SourceError, SourcePosition, SourceSeq, StreamItem,
+    TableRef, TableSchema, TransactionBoundary, identifier_values,
 };
 use data_avalanche_state::{PipelineDocs, TableHalt};
 use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
 use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
-use tokio::task::JoinHandle;
+use tokio::task::{JoinError, JoinHandle, JoinSet};
 
 use self::snapshot::Snapshots;
 use crate::schema_change::{Evolver, Planned};
 use crate::settings::Settings;
 use crate::status::Streak;
+use crate::watchdog::Watchdog;
 use crate::writer::{self, Job, WriterCtx, WriterEvent};
 use crate::{
     Accumulator, AccumulatorError, Command, CommitGuard, Exit, FlushReason, Hooks, Phase, Tuning,
@@ -166,6 +167,10 @@ pub(crate) struct Session<'c> {
     draining: bool,
     /// When this session parked for [`Command::Pause`], with everything it read durable.
     paused: Option<Instant>,
+    /// The heartbeat's read of the source's log, one at a time, run beside the loop: a source
+    /// that doesn't answer holds up neither the stream nor a drain (DA-SRC-16).
+    retained_read: JoinSet<Result<Option<RetainedLog>, SourceError>>,
+    watchdog: Watchdog,
 }
 
 impl<'c> Session<'c> {
@@ -281,6 +286,8 @@ impl<'c> Session<'c> {
             snaps,
             draining: false,
             paused: None,
+            retained_read: JoinSet::new(),
+            watchdog: Watchdog::default(),
         }))
     }
 
@@ -410,7 +417,14 @@ impl<'c> Session<'c> {
                         return Ok(end);
                     }
                 }
-                _ = heartbeat.tick() => self.wal_lag().await?,
+                _ = heartbeat.tick() => {
+                    if let Some(end) = self.heartbeat() {
+                        return Ok(end);
+                    }
+                }
+                read = self.retained_read.join_next(), if !self.retained_read.is_empty() => {
+                    self.wal_lag(read.expect("not empty"))?;
+                }
                 _ = async { rescan.as_mut().expect("guarded").tick().await },
                     if rescan.is_some() && self.rescan.is_none() && !parked =>
                 {
@@ -420,6 +434,7 @@ impl<'c> Session<'c> {
                 }
                 item = self.stream.next(), if reading => match item {
                     Ok(item) => {
+                        self.watchdog.heard();
                         if let Some(end) = self.item(item).await? {
                             return Ok(end);
                         }
@@ -869,15 +884,44 @@ impl<'c> Session<'c> {
         Ok(None)
     }
 
-    /// The WAL-retention guard (G-22): the retained log is always reported; past
-    /// `max_wal_lag_bytes` the pipeline stops to protect the source's disk. A failed read waits
-    /// for the next heartbeat, and the pipeline keeps running. The first permanent failure of a
-    /// streak warns that the guard is blind; a read that succeeds ends the streak.
-    async fn wal_lag(&mut self) -> Result<(), WorkerError> {
-        let retained = match self.core.source.retained_bytes().await {
+    /// A heartbeat: the next read of the source's log goes out unless one still is, and the
+    /// watchdog counts. `Some` ends the session: the source isn't answering, so the worker
+    /// reconnects.
+    fn heartbeat(&mut self) -> Option<End> {
+        if self.retained_read.is_empty() {
+            let source = self.core.source.clone();
+            self.retained_read
+                .spawn(async move { source.retained().await });
+        }
+        let reading = self.queued.is_empty() && self.paused.is_none();
+        let why = self.watchdog.beat(reading)?;
+        Some(End::Source(SourceError::Transient(why)))
+    }
+
+    /// A heartbeat's read of the source's log came back. The WAL-retention guard (G-22): the
+    /// retained log is always reported; past `max_wal_lag_bytes` the pipeline stops to protect
+    /// the source's disk. A failed read waits for the next heartbeat, and the pipeline keeps
+    /// running. The first permanent failure of a streak warns that the guard is blind; a read
+    /// that succeeds ends the streak. The watchdog hears whether the source answered: a
+    /// transient failure, or a slot no stream reads, is no answer.
+    fn wal_lag(
+        &mut self,
+        read: Result<Result<Option<RetainedLog>, SourceError>, JoinError>,
+    ) -> Result<(), WorkerError> {
+        let read = read.map_err(|e| {
+            protocol(format!(
+                "the heartbeat's read ended without its answer: {e}"
+            ))
+        })?;
+        let retained = match read {
             Ok(retained) => retained,
-            Err(e) if e.is_transient() => return Ok(()),
+            Err(e) if e.is_transient() => {
+                self.watchdog.answered(Some(e.to_string()));
+                return Ok(());
+            }
             Err(e) => {
+                // The source answered, with a refusal.
+                self.watchdog.answered(None);
                 let blind = if self.core.settings.max_wal_lag_bytes.is_some() {
                     "max_wal_lag_bytes isn't enforced"
                 } else {
@@ -892,6 +936,12 @@ impl<'c> Session<'c> {
             }
         };
         self.core.wal_lag_failing.succeeded();
+        self.watchdog.answered(
+            retained
+                .is_some_and(|r| !r.streamed)
+                .then(|| "no stream reads the slot".to_owned()),
+        );
+        let retained = retained.map(|r| r.bytes);
         self.core.update(|st| st.retained_bytes = retained);
         match (retained, self.core.settings.max_wal_lag_bytes) {
             (Some(lag), Some(limit)) if lag > limit => Err(WorkerError::WalLag { lag, limit }),
```

Change `crates/data-avalanche-pipeline/src/watchdog.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/watchdog.rs b/crates/data-avalanche-pipeline/src/watchdog.rs
--- a/crates/data-avalanche-pipeline/src/watchdog.rs
+++ b/crates/data-avalanche-pipeline/src/watchdog.rs
@@ -1,3 +1,61 @@
+//! The stream watchdog (DA-SRC-16): a stream that stays silent while the source stops answering
+//! ends the session, and the worker reconnects. Silence alone proves nothing (an idle server with
+//! `wal_sender_timeout = 0` sends nothing), so the heartbeat's read of the source's log decides: it
+//! failed (a frozen server times it out), or it found no stream on the slot (the server dropped the
+//! connection without the client hearing of it).
+
+/// Heartbeats the stream may stay silent while the heartbeat's reads go unanswered.
+pub(crate) const SILENT_HEARTBEATS: u32 = 3;
+
+#[derive(Debug, Default)]
+pub(crate) struct Watchdog {
+    /// A stream item since the last heartbeat.
+    heard: bool,
+    /// Consecutive heartbeats without a stream item while the session read the stream.
+    silent: u32,
+    /// Consecutive heartbeat reads that failed or found no stream, and the last one's finding.
+    unanswered: u32,
+    why: String,
+}
+
+impl Watchdog {
+    /// A stream item arrived.
+    pub(crate) fn heard(&mut self) {
+        self.heard = true;
+        self.silent = 0;
+    }
+
+    /// A heartbeat read completed: `failure` is what was wrong with it, `None` when the source
+    /// answered and a stream reads the slot. A read still out counts neither way: it may wait
+    /// behind a chunk read on a shared connection, which `timeouts.command` bounds.
+    pub(crate) fn answered(&mut self, failure: Option<String>) {
+        match failure {
+            None => self.unanswered = 0,
+            Some(why) => {
+                self.unanswered += 1;
+                self.why = why;
+            }
+        }
+    }
+
+    /// A heartbeat. `reading`: the session reads the stream (not paused, no flush waiting for
+    /// room), so a silent stream is the source's doing. `Some(reason)` when the session must end.
+    pub(crate) fn beat(&mut self, reading: bool) -> Option<String> {
+        if reading && !self.heard {
+            self.silent += 1;
+        } else {
+            self.silent = 0;
+        }
+        self.heard = false;
+        (self.silent >= SILENT_HEARTBEATS && self.unanswered >= SILENT_HEARTBEATS).then(|| {
+            format!(
+                "the source isn't answering: no stream message for {} heartbeats, and the last {} reads of its log failed ({})",
+                self.silent, self.unanswered, self.why
+            )
+        })
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -141,12 +141,17 @@ impl PipelineWorker {
             if opened.elapsed() >= core.tuning.source_backoff.max {
                 attempt = 0;
             }
-            core.update(|st| st.reconnects += 1);
-            match failed {
-                SourceError::Transient(error) => {
-                    contended_since = None;
-                    core.update(|st| st.phase = Phase::Reconnecting { error });
+            // One update: an observer that sees the count sees its reason.
+            core.update(|st| {
+                st.reconnects += 1;
+                if let SourceError::Transient(error) = &failed {
+                    st.phase = Phase::Reconnecting {
+                        error: error.clone(),
+                    };
                 }
+            });
+            match failed {
+                SourceError::Transient(_) => contended_since = None,
                 SourceError::Contended(detail) => {
                     let since = *contended_since.get_or_insert_with(Instant::now);
                     let waited = since.elapsed();
```

Change `crates/data-avalanche-source-pg/src/connect.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connect.rs b/crates/data-avalanche-source-pg/src/connect.rs
--- a/crates/data-avalanche-source-pg/src/connect.rs
+++ b/crates/data-avalanche-source-pg/src/connect.rs
@@ -94,8 +94,18 @@ impl Connector {
         c
     }
 
-    /// A new catalog connection, its driver task spawned.
+    /// A new catalog connection, its driver task spawned, within `timeouts.connect`: the
+    /// driver's own `connect_timeout` bounds only the TCP connect, and a server that accepts the
+    /// connection but never answers (frozen, behind a black hole) would hold the startup, and
+    /// every call queued behind the shared connection, forever (DA-SRC-16).
     pub(crate) async fn connect(&self) -> Result<Client, SourceError> {
+        let after = self.timeouts.connect;
+        tokio::time::timeout(after, self.open())
+            .await
+            .map_err(|_| timed_out("connect to the source", after))?
+    }
+
+    async fn open(&self) -> Result<Client, SourceError> {
         let config = self.control_config();
         let client = match &self.tls {
             None => {
```

Change `crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -8,8 +8,8 @@ use data_avalanche_config::{
 };
 use data_avalanche_core::{
     ChangeStream, Chunk, HaltReason, InFlightTxn, KeysetCursor, Preflight, Prepared, RescanReport,
-    ResolvedTables, Row, SchemaDelta, SchemaHint, SourceConnector, SourceError, SourcePosition,
-    TableRef, TableSchema,
+    ResolvedTables, RetainedLog, Row, SchemaDelta, SchemaHint, SourceConnector, SourceError,
+    SourcePosition, TableRef, TableSchema,
 };
 use tokio::sync::Mutex;
 
@@ -274,8 +274,8 @@ impl SourceConnector for PgConnector {
         PgConnector::reset_position(self).await
     }
 
-    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
-        PgConnector::retained_bytes(self).await
+    async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
+        PgConnector::retained(self).await
     }
 
     async fn current_position(&self) -> Result<SourcePosition, SourceError> {
```

Change `crates/data-avalanche-source-pg/src/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -7,7 +7,7 @@ use std::collections::BTreeSet;
 use data_avalanche_config::{SlotManagement, SlotOnOrphan};
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    Prepared, SnapshotReason, SourceError, SourcePosition, StartPlan, TableRef,
+    Prepared, RetainedLog, SnapshotReason, SourceError, SourcePosition, StartPlan, TableRef,
 };
 use tokio_postgres::Client;
 
@@ -517,24 +517,29 @@ impl PgConnector {
     }
 
     /// The WAL the slot keeps on the source's disk: `pg_current_wal_lsn() - restart_lsn`, the
-    /// `max_wal_lag_bytes` guard's measure. `restart_lsn` trails `confirmed_flush_lsn` while a
-    /// transaction older than the confirmed position is still open. `None` without a slot.
-    pub async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
+    /// `max_wal_lag_bytes` guard's measure, and whether a walsender reads the slot (`active`).
+    /// `restart_lsn` trails `confirmed_flush_lsn` while a transaction older than the confirmed
+    /// position is still open. `None` without a slot, or with a slot that lost its WAL.
+    pub async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
         let client = self.conn.client().await?;
         let row = self
             .conn
             .command(
                 "measure the slot's WAL",
                 client.query_opt(
-                    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
+                    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, active
                        FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                     &[&self.slot_name()],
                 ),
             )
             .await?;
-        Ok(row
-            .and_then(|r| r.get::<_, Option<i64>>(0))
-            .map(|b| u64::try_from(b).unwrap_or(0)))
+        Ok(row.and_then(|r| {
+            let bytes = r.get::<_, Option<i64>>(0)?;
+            Some(RetainedLog {
+                bytes: u64::try_from(bytes).unwrap_or(0),
+                streamed: r.get(1),
+            })
+        }))
     }
 }
 
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --lib &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-source-pg -- retained a_connect_to &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 guards::a_source_that_stops
```

Expected:

PASS:

```text
test result: ok. 194 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.64s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 82 filtered out; finished in 0.31s
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 53 filtered out; finished in 0.38s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 25 filtered out; finished in 24.79s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "fix(pipeline): the stream watchdog: a silent stream whose source doesn't answer ends the session and the worker reconnects; the catalog connect is bounded by timeouts.connect (DA-SRC-16)"
```


### Task 23: Quiet Delta library logs at info, and no kernel error for an absent table

**Files:**
- Modify: `crates/data-avalanche-sink-delta/src/table.rs`, `crates/data-avalanche/src/logging.rs`, `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/smoke.rs`

**Interfaces:**
- Consumes: the binary's `logging` (Task 12), `TableHandle::open` (sink-delta, M4).
- Produces:
  - `logging::quieted(filter: &str) -> String`: a filter whose bare level is `info` or quieter
    and that names neither Delta library gains `buoyant_kernel=warn`, `deltalake_core=warn`,
    `deltalake_core::operations::merge=error` (never louder than the level). `init` installs it.
  - `TableHandle::open` builds the table without loading it, and loads it only when
    `_delta_log/` lists a log file.
  - The E2E config logs at plain `info`.

The owner's answer to F-2 (2026-10-08): hide the noise at the global `info` level.
- **Measured before the fix:** of a restart run's 25 log lines, about 20 came from the Delta
  libraries: the kernel's INFO on every snapshot build, the MERGE's INFO, its WARN "Missing
  target skipped-file metric" on every MERGE, and the commit's INFO.
- **The ERROR:** delta-rs's `try_from_url_with_storage_options` loads first and accepts an
  absent table after, and the kernel logs "No files in log segment" for every table a first
  start creates. The LIST before the load (one per uncached table) is what `refresh` does
  already.
- The config patch notes the caps under `globals.log.level`.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche/src/logging.rs`:

```diff
diff --git a/crates/data-avalanche/src/logging.rs b/crates/data-avalanche/src/logging.rs
--- a/crates/data-avalanche/src/logging.rs
+++ b/crates/data-avalanche/src/logging.rs
@@ -56,6 +56,44 @@ mod tests {
         }
     }
 
+    #[test]
+    fn a_bare_level_quiets_the_delta_libraries() {
+        assert_eq!(
+            quieted("info"),
+            "info,buoyant_kernel=warn,deltalake_core=warn,deltalake_core::operations::merge=error"
+        );
+        assert_eq!(
+            quieted("info,data_avalanche=debug"),
+            "info,data_avalanche=debug,buoyant_kernel=warn,deltalake_core=warn,deltalake_core::operations::merge=error"
+        );
+        assert_eq!(
+            quieted("warn"),
+            "warn,deltalake_core::operations::merge=error",
+            "never louder than the level"
+        );
+        assert_eq!(quieted("error"), "error");
+    }
+
+    #[test]
+    fn a_filter_that_names_a_library_or_asks_for_more_is_kept() {
+        for f in [
+            "debug",
+            "trace",
+            "info,deltalake_core=info",
+            "info,buoyant_kernel=debug",
+            "data_avalanche=debug",
+        ] {
+            assert_eq!(quieted(f), f);
+        }
+    }
+
+    #[test]
+    fn the_quieted_filter_parses() {
+        for f in ["info", "warn", "info,data_avalanche_cluster=debug"] {
+            EnvFilter::builder().parse(quieted(f)).unwrap();
+        }
+    }
+
     #[test]
     fn the_command_line_overrides_the_config() {
         let log = Log {
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -153,7 +153,7 @@ globals:
   cloud: aws
   state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
   api: {{ address: "127.0.0.1:@PORT@" }}
-  log: {{ level: "info,buoyant_kernel=warn", format: compact }}
+  log: {{ level: info, format: compact }}
   cluster:
     lease_duration: 6s
     lease_renewal_interval: 2s
```

Change `crates/data-avalanche/tests/e2e/smoke.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/smoke.rs b/crates/data-avalanche/tests/e2e/smoke.rs
--- a/crates/data-avalanche/tests/e2e/smoke.rs
+++ b/crates/data-avalanche/tests/e2e/smoke.rs
@@ -22,6 +22,14 @@ async fn a_pipeline_runs_stops_on_sigterm_and_resumes() {
     e.converged("items").await;
     e.exec("INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'A' WHERE id = 1; DELETE FROM items WHERE id = 2").await;
     e.converged("items").await;
+    // At `info`, a first start that creates its table logs no error, and the Delta libraries
+    // only what they warn of (config §6.4).
+    let log = p.log();
+    assert!(!log.contains("ERROR"), "{log}");
+    assert!(
+        !log.contains("buoyant_kernel") && !log.contains("deltalake_core"),
+        "{log}"
+    );
     let labels = r#"pipeline="smoke",source_host="127.0.0.1",source_db="avalanche""#;
     p.metric(&format!(
         r#"data_avalanche_pipeline_status{{{labels},status="running"}} 1"#
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --lib &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 smoke::
```

Expected:

FAIL to compile (6 errors); the first:

```text
error[E0425]: cannot find function `quieted` in this scope
… and 5 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-sink-delta/src/table.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/table.rs b/crates/data-avalanche-sink-delta/src/table.rs
--- a/crates/data-avalanche-sink-delta/src/table.rs
+++ b/crates/data-avalanche-sink-delta/src/table.rs
@@ -8,11 +8,11 @@ use arrow_array::{ArrayRef, RecordBatch};
 use arrow_row::{RowConverter, SortField};
 use arrow_schema::SchemaRef;
 use data_avalanche_core::{PartitionTransform, SinkError, SourcePosition, TableRef, TableSpec};
-use deltalake::DeltaTable;
 use deltalake::datafusion::prelude::SessionContext;
 use deltalake::logstore::object_store::path::Path;
 use deltalake::logstore::object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
 use deltalake::protocol::SaveMode;
+use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableError};
 use futures_util::TryStreamExt;
 use url::Url;
 
@@ -70,6 +70,10 @@ impl std::fmt::Debug for TableHandle {
 impl TableHandle {
     /// A handle on `url`, where `name` is mirrored; the table is loaded when it exists. A local
     /// root is created first: delta-rs refuses a `file://` location that doesn't exist.
+    ///
+    /// Loaded only when the location holds a log (one LIST): delta-rs's own "load, else accept
+    /// an absent table" makes the kernel log an ERROR ("No files in log segment") for every
+    /// table a first start creates.
     pub async fn open(
         name: TableRef,
         url: Url,
@@ -79,9 +83,21 @@ impl TableHandle {
             std::fs::create_dir_all(&path)
                 .map_err(|e| permanent(format!("{}: {e}", path.display())))?;
         }
-        let table = DeltaTable::try_from_url_with_storage_options(url, options.clone())
-            .await
+        let mut table = DeltaTableBuilder::from_url(url)
+            .and_then(|b| b.with_storage_options(options.clone()).build())
             .map_err(sink_error)?;
+        if table
+            .log_store()
+            .is_delta_table_location()
+            .await
+            .map_err(sink_error)?
+        {
+            match table.load().await {
+                // Dropped since the LIST: absent, as if the LIST had found nothing.
+                Ok(()) | Err(DeltaTableError::NotATable(_)) => {}
+                Err(e) => return Err(sink_error(e)),
+            }
+        }
         let mut handle = Self {
             name,
             table,
```

Change `crates/data-avalanche/src/logging.rs`:

```diff
diff --git a/crates/data-avalanche/src/logging.rs b/crates/data-avalanche/src/logging.rs
--- a/crates/data-avalanche/src/logging.rs
+++ b/crates/data-avalanche/src/logging.rs
@@ -2,6 +2,7 @@
 
 use data_avalanche_config::{Log, LogFormat};
 use tracing_subscriber::EnvFilter;
+use tracing_subscriber::filter::LevelFilter;
 
 use crate::{LogFormatArg, RunArgs};
 
@@ -20,6 +21,43 @@ pub(crate) fn settings(log: &Log, args: &RunArgs) -> (String, LogFormat) {
     (level, format)
 }
 
+/// The Delta libraries' targets, and the most each logs under a level of `info` or quieter:
+/// their INFO traces every snapshot build, MERGE and commit, and the MERGE warns "Missing target
+/// skipped-file metric" on every run (config §6.4).
+const QUIET: &[(&str, LevelFilter)] = &[
+    ("buoyant_kernel", LevelFilter::WARN),
+    ("deltalake_core", LevelFilter::WARN),
+    ("deltalake_core::operations::merge", LevelFilter::ERROR),
+];
+
+/// `filter` with [`QUIET`]'s caps added when its bare level is `info` or quieter and it names
+/// neither library: a filter that names one, or a `debug` or `trace` level, is the operator's.
+pub(crate) fn quieted(filter: &str) -> String {
+    let directives: Vec<&str> = filter.split(',').map(str::trim).collect();
+    if directives
+        .iter()
+        .any(|d| d.starts_with("buoyant_kernel") || d.starts_with("deltalake"))
+    {
+        return filter.to_owned();
+    }
+    let Some(level) = directives
+        .iter()
+        .rev()
+        .find_map(|d| d.parse::<LevelFilter>().ok())
+    else {
+        return filter.to_owned();
+    };
+    let mut out = filter.to_owned();
+    if level <= LevelFilter::INFO {
+        for (target, most) in QUIET {
+            if level > *most {
+                out.push_str(&format!(",{target}={most}"));
+            }
+        }
+    }
+    out
+}
+
 /// Installs the process's subscriber; logs go to stderr.
 ///
 /// # Errors
@@ -27,7 +65,7 @@ pub(crate) fn settings(log: &Log, args: &RunArgs) -> (String, LogFormat) {
 pub(crate) fn init(log: &Log, args: &RunArgs) -> Result<(), String> {
     let (level, format) = settings(log, args);
     let filter = EnvFilter::builder()
-        .parse(&level)
+        .parse(quieted(&level))
         .map_err(|e| format!("--log-level {level:?} isn't a log filter: {e}"))?;
     let builder = tracing_subscriber::fmt()
         .with_env_filter(filter)
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --lib &&
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 smoke::
```

Expected:

PASS:

```text
test result: ok. 13 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 25 filtered out; finished in 15.82s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(data-avalanche): a plain info level quiets the Delta libraries; the Delta sink opens an absent table without the kernel's error (F-2)"
```


### Task 24: CI: the E2E suite weekly on PostgreSQL 14–17

**Files:**
- Modify: `.github/workflows/weekly.yml`

**Interfaces:**
- Consumes: `tests/e2e` (Tasks 14–23), `weekly.yml` (M5).
- Produces: the weekly matrix job starts RustFS (and its bucket) beside `postgres-N`, installs
  `uv`, and runs "Slice-1 E2E" after the worker tests; 60-minute timeout.

The exit's "weekly on PG 14–17". Run locally against `postgres-14` before the change
(25/25, 316 s).

- [ ] **Step 1: Change the workflow**

Change `.github/workflows/weekly.yml`:

```diff
diff --git a/.github/workflows/weekly.yml b/.github/workflows/weekly.yml
--- a/.github/workflows/weekly.yml
+++ b/.github/workflows/weekly.yml
@@ -1,8 +1,9 @@
 name: weekly
 
-# The PostgreSQL source and the pipeline worker against the older supported servers (P-8, Q-M5-7,
-# Q-M5-23): PostgreSQL 14–17, one job each, the `postgres-18` suites of ci.yml re-run through
-# DATA_AVALANCHE_PG_PORT.
+# The PostgreSQL source, the pipeline worker and the slice-1 E2E suite against the older supported
+# servers (P-8, Q-M5-7, Q-M5-23, roadmap §4 M6b exit): PostgreSQL 14–17, one job each, the
+# `postgres-18` suites of ci.yml re-run through DATA_AVALANCHE_PG_PORT. The E2E suite also needs
+# RustFS and uv (its DuckDB read).
 # Weekly (Mondays 03:41 UTC) and on manual dispatch. The TLS tests need `postgres-tls`
 # (PostgreSQL 18) and run in ci.yml only. No secrets.
 
@@ -20,7 +21,7 @@ env:
 jobs:
   postgres:
     runs-on: ubuntu-latest
-    timeout-minutes: 30
+    timeout-minutes: 60
     strategy:
       fail-fast: false
       matrix:
@@ -32,15 +33,20 @@ jobs:
     steps:
       - uses: actions/checkout@v7
       - uses: actions-rust-lang/setup-rust-toolchain@v2
-      - name: Start PostgreSQL ${{ matrix.version }}
-        run: docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "postgres-${{ matrix.version }}"
+      - uses: astral-sh/setup-uv@v10.2.0
+      - name: Start PostgreSQL ${{ matrix.version }} and RustFS
+        run: |
+          docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "postgres-${{ matrix.version }}" rustfs
+          docker compose -f test-infra/compose.yaml --profile init run --rm rustfs-init
       - name: PostgreSQL source tests
         run: 'cargo test --locked -p data-avalanche-source-pg --test pg --test slot_invalidation -- --test-threads=1 --skip tls::'
       - name: Pipeline worker tests
         run: cargo test --locked -p data-avalanche-pipeline --test pg --test sync_commit --test sync_drop -- --test-threads=1
+      - name: Slice-1 E2E
+        run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
       - name: Container logs on failure
         if: failure()
-        run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "postgres-${{ matrix.version }}"
-      - name: Stop PostgreSQL
+        run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "postgres-${{ matrix.version }}" rustfs
+      - name: Stop the containers
         if: always()
         run: docker compose -f test-infra/compose.yaml --profile matrix down -v
```

- [ ] **Step 2: Check that the workflows parse**

```bash
uv run --no-project --with pyyaml python -c "import yaml; [yaml.safe_load(open(f)) for f in ('.github/workflows/weekly.yml', '.github/workflows/ci.yml')]; print('workflows parse')"
```

Expected:

PASS:

```text
workflows parse
```

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "ci: the slice-1 E2E suite weekly on PostgreSQL 14-17, with RustFS and uv"
```


---

## Verified during planning (2026-10-07…08)

All measured on 2026-10-07/08 in a scratch workspace cut from this branch (`eb327d7`), one commit
per task, against the `test-infra` services (PostgreSQL 18 and 14, RustFS, Azurite; GCS skipped).

- **The tasks' code blocks** are the scratch commits' diffs. Each task's Step 1 tree (its tests,
  the previous task's code) was committed and its command run: the "Expected" failures and
  passes above are those runs' output. Tasks 1–14 and 21–23 fail at Step 2 (to compile, or in
  their new tests) and pass at Step 4; Tasks 15–20 and 24 pass as written. Tasks 21–24 were
  added on 2026-10-08 with the owner's answers to F-1…F-3.
- **Workspace at the last task:**
  - `cargo fmt --all --check` and both clippy runs are clean.
  - `cargo test --workspace`: 920 passed.
  - The integration run (`DATA_AVALANCHE_IT=1`, GCS skipped, `--no-fail-fast`): 911 passed. The
    three `source-pg` TLS tests couldn't run there: they need `test-infra/.tls`, which the
    scratch workspace doesn't carry. Task 22 changes the catalog connect they use, so Task 22
    runs them at execution.
- **The E2E suite** (26 scenarios, `--test-threads=1`): green twice in a row on PostgreSQL 18 + RustFS (286 s each), and once on
  PostgreSQL 14 (347 s). PostgreSQL 15–17 weren't run locally; `weekly.yml` runs them.
  - One earlier PostgreSQL 18 run failed in `retained_wal_past_the_limit_stops_the_pipeline`: at
    the default 60 s heartbeat, the lag is first read 60 s into the session, and the test waits
    60 s for the error. Run alone, it took 69 s each time and failed once in three. Task 20's
    scenario now runs at a 5 s heartbeat.
- **Mutation checks** (each mutation made the named test fail, then was reverted):
  - Task 5: the reading gate removed → `a_pause_finishes_the_open_transaction_then_reads_nothing_until_resumed`;
    the snapshot clock shift removed → `a_pause_stops_the_snapshot_and_its_clock`; the permit
    arm's old guard → `a_paused_session_idles` (watchdog after 2 s).
  - Task 2: the refresh's log check removed → `a_live_handle_recovers_from_a_table_recreated_past_its_version`.
  - Task 6: the `ensure_table` gate removed → `a_lease_lost_before_any_phase_0_write_stops_it_there`
    (4 writes, not 3); the `reset_position` gate removed →
    `a_lost_position_is_not_replaced_once_the_lease_lapsed`.
  - Task 11: the Phase 0 permit dropped at start → `one_phase_0_at_a_time_holds_the_other_start`.
  - Task 15: `CommitMarker::covers` skipping snapshot sets → `always_snapshots_at_every_start`
    ("the idle source's snapshot chunk is committed").
  - Task 16: the unchanged-column mask removed (`batch.rs`) → the across-batch TOAST check
    (`"NULL"`, length 4); the in-batch fill removed (`buffer.rs`) → the within-batch check.
  - Task 21: the guard comparing `end` again → `a_crash_between_a_commit_and_its_checkpoint_skips_the_replay`
    (3 Delta commits, not 2).
  - Task 22: the watchdog's verdict ignored → `a_source_that_stops_answering_is_reconnected_to`
    (no reconnect within 60 s); the connect bound lifted → `a_connect_to_a_server_that_never_answers_times_out`;
    a read still out counted as unanswered → `a_stalled_heartbeat_read_holds_up_nothing`.
  - Task 23: `quieted` dropped from `init` → the smoke scenario (Delta library lines); the
    LIST before the load removed → the smoke scenario (the kernel's ERROR).
- **Live facts the plan relies on:**
  - `ALTER ROLE … SET wal_sender_timeout = '2s'` applies to a replication connection on 18 and
    14; a paused worker keeps the same `active_pid` past it (Task 5).
  - RustFS takes `AWS_ENDPOINT_URL` + `AWS_ALLOW_HTTP` through both `object_store` 0.14.2 and
    deltalake's 0.13.2 (Task 14's smoke scenario).
  - `snapshots.mode` defaults to `never`.
  - A split transaction crashed after its second slice leaves exactly the first 2,000 of 5,000
    rows, twice in a row (Task 16).
  - With `buffer_capacity_rows` at the chunk size, a crash at the sixth chunk finds four chunks
    committed and the cursor at 2,000 rows. Without the cap, a crash at the fourth found no
    chunk committed (Task 17).
  - Before Task 21, the commit marker didn't skip a replay whose flush also read a keep-alive
    (F-1). With it, the replay adds no Delta commit.
  - A container frozen with `docker pause` keeps its TCP connections and never answers. Before
    Task 22, a pipeline on a PostgreSQL 14 frozen for 150 s stayed `running`, with no reconnect
    and nothing logged. A local listener that accepts and never answers holds tokio-postgres's
    startup past `connect_timeout`.
  - DuckDB 1.5.6 reads the Delta tables on RustFS through an S3 secret (path style, no SSL),
    printing `timestamptz` in the session's zone (pinned to UTC, Task 19).
  - Leftovers: after every green run, no `e2e%` slot, schema or publication on either server, no
    `data-avalanche-it/e2e-*` prefix on RustFS, `max_slot_wal_keep_size` back to `-1`, and
    `synchronous_standby_names` empty.

## M6b exit checklist

- [ ] Tasks 1–24 committed, each reviewed (one implementer and one reviewer per task, P-9).
- [ ] The whole-branch review and its fixes.
- [ ] Workspace gates green: `cargo fmt --all --check`; `cargo clippy --workspace --all-targets
      -- -D warnings`; the same with `-p data-avalanche --features e2e-hooks`; `cargo test
      --workspace`; the integration run (`DATA_AVALANCHE_IT=1`).
- [ ] The slice-1 E2E suite green on PostgreSQL 18 + RustFS in PR CI.
- [ ] `weekly.yml` green on PostgreSQL 14–17 with the E2E step (a manual dispatch on the branch,
      or the first Monday run after the merge).
- [x] The approved spec patches applied (config §9.4.3 and V-119; the System-Overview
      catalogue), 2026-10-08.
- [x] The Tasks 21–23 spec patches applied (Data-Flow Phase 4; config §6.4, §9.2, §9.2.1),
      2026-10-08.
- [x] The roadmap updates applied (2026-10-08).
- [ ] README status at the exit.
- [x] Gap tracker: the owner's answers recorded (§29, 2026-10-08): DA-X-12 new, DA-SRC-16 🔄,
      DA-SNAP-11 advanced.
- [ ] Gap tracker at the exit: DA-X-12 and DA-SRC-16 closed by Tasks 21 and 22.
- [ ] `.superpowers/sdd/M6a-pipeline-worker/` deleted (kept until M6 is finished).
- [ ] The milestone PR opened against `main`; Oleksii merges.

## Deviations during execution

- **Task 1, the review fix (2026-10-08).** The task review found that `DeltaSink::evolve` built
  its fence `txn` by hand, a copy of `props()`, while the race test built its own properties: a
  `DeltaSink::evolve` without the `txn` left every test green, so nothing pinned the half of
  Q-M6b-7 that holds a lost race. `props` now takes the owner epoch, the evolve uses it, and
  `an_evolve_of_a_table_a_newer_owner_wrote_is_fenced` pins the `txn` the evolve writes: the
  owner evolves at epoch 7, and a commit at 6 is then `Fenced { current_epoch: 7 }`. Verified in
  the scratch workspace (the `evolve` runs 6, 5 and 1 passed; dropping the `txn` from the evolve
  fails the test with `Committed { version: 4 }`), and committed after Task 1's commit as
  `fix(sink-delta): an evolve's commit takes its fence from the shared props builder; a test pins
  the txn it writes`. Task 2's evolve hunk is rebased on it (it calls `self.props`); no other
  task's code changes. The review's three minor notes are parked in the SDD ledger.

- **Task 6, the review fix (2026-10-08).** The task review found that three gated Phase 0
  writes, `reset_position`, `prepare` and `ensure_table`, asked `may_write()` once and were then
  retried on `Transient` without asking again: a lease that lapsed during a transient outage could
  still let the write land, the slot recreated or advanced included (Review Focus 1). The writer
  and the evolver already ask before every try. `write_retry` now asks the guard before every
  try (`retry` is `write_retry` with no gate), and `sink_retry` takes the same gate. The test
  `a_lease_lost_while_a_write_is_retried_stops_the_retry` fails without it (`prepare` landed: 3
  writes, not 2; and, with the sink gate asked only on the first try, `ensure_table` landed: 4,
  not 3). The same review found that a `SourceError::Contended` from Phase 0 (`prepare` meeting
  the slot active for another PID) was classified `Error`, while Q-M6b-3 retries `Contended`: it
  is `Retry` now, with a case in `each_error_names_its_recovery`. Committed after Task 6's
  commit as `fix(pipeline): a Phase 0 write retried while it fails transiently asks the lease
  before every try; a contended slot in Phase 0 is retried`. No later task's code changes; at
  the last task the workspace has 921 tests (920 + 1). The review's other notes are parked in
  the SDD ledger.

- **Task 8, the review fix (2026-10-08).** The task review ran two mutations that every test
  survived: the deadline taken from the renewal's answer instead of from before its write
  (Review Focus 1: a slow write would extend the guard past what the store granted), and a
  lease read back after an unknown outcome adopted whatever holder it names (Review Focus 2: a
  false hold). Two tests now pin them, with a `Kind::Slow` fault in `ScriptedStore`:
  `a_slow_renewal_s_deadline_counts_from_before_the_write` and
  `a_lease_taken_while_a_renewal_s_outcome_is_unknown_is_lost`; each fails under its mutation.
  The code is unchanged. Committed after Task 8's commit as `test(cluster): a slow renewal's
  deadline counts from before the write; a lease taken while a renewal's outcome is unknown is
  lost`. The cluster crate has 10 lease tests at Task 8 and 31 tests at the last task; the
  workspace, 923. A lease read back as absent (`Ok(None)` after an unknown outcome) has no test:
  `ScriptedStore` can't delete, so that one-line arm is checked by reading only. The review's
  other notes are parked in the SDD ledger.

- **Task 9, the review fix in two rounds (2026-10-08).** The task review found that `step`
  read an `Acquired::HeldBy` naming this holder (an earlier write that landed behind its error)
  as another node's lease: the node waited a full `lease_duration`, then took its own lease
  over. It also found that `written` (taken before the write) and the takeover wait (from when
  the read returned) survived mutation. Round 1 renewed a self-held lease at once and added
  three tests, with a `ScriptedStore::slow_reads` knob; every mutation fails one. At the last
  task, Task 10's `a_lost_lease_stops_the_worker_and_the_lease_is_claimed_again` then failed:
  the lease its renewer gave up as lost came back under the same epoch, shared with the worker
  that had run. Round 2: a lease naming this holder is this process's own earlier term, taken
  over at once under the next epoch, never renewed, so a new term never shares an epoch with a
  worker that already ran (a restarted process has another incarnation and waits as before).
  Committed after Task 9's commit as `fix(cluster): a lease naming this holder after a write
  that landed behind an error is renewed at once; written and the takeover wait are pinned by
  tests` and `fix(cluster): a lease naming this holder is taken over at once under the next
  epoch, never renewed: a new term never shares an epoch with a worker that ran`. The
  re-review found the incarnation half of the self check untested (comparing the node id alone
  passed every test); round 3 adds `a_restarted_process_waits_a_full_lease_duration` and states
  the premise, (node id, incarnation) naming one process, in the code: `test(cluster): a
  restarted process waits a full lease duration; the self takeover's premise is documented`.
  The review's other notes are parked in the SDD ledger.
- **Task 10, changed before its execution (2026-10-08).** Round 2 above uncovered a stale
  status: after `Left::Lost`, the controller went back to claiming and, while every claim
  failed, kept reporting `Running` under the lost lease (before, the claim answered `HeldBy`
  naming this node, which set standby). `Left::Lost` now sets `Standby { holder: None }`, which
  also drops the pipeline's series. Task 10's code below carries it. At the last task the
  workspace has 927 tests; the cluster crate's counts from Task 9 on are 6 higher than their
  Step 4 text shows (the Task 8 and Task 9 fixes' tests).

### After Task 1: the review fix

Change `crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
index 2278506..58d91f6 100644
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -164,10 +164,10 @@ impl DeltaSink {
     }
 
     /// One write's commit properties: the fence's `txn` (the owner epoch as the `i64` version
-    /// Delta stores, `txn_version`) and the marker's `commitInfo`.
+    /// Delta stores, `txn_version`) and the `commitInfo`.
     fn props(
         &self,
-        marker: &CommitMarker,
+        owner_epoch: u64,
         txn_version: i64,
         position: Option<&SourcePosition>,
     ) -> CommitProperties {
@@ -178,7 +178,7 @@ impl DeltaSink {
             ))
             .with_metadata(commit_metadata(
                 &self.config.pipeline_id,
-                marker.owner_epoch,
+                owner_epoch,
                 position,
             ))
     }
@@ -229,7 +229,7 @@ impl DeltaSink {
         let mut table = h.table().clone();
         if cs.truncate {
             // The clear carries the fence but no position: a crash before the MERGE replays both.
-            table = match truncate(table, self.props(marker, txn_version, None)).await {
+            table = match truncate(table, self.props(marker.owner_epoch, txn_version, None)).await {
                 Ok((t, metrics)) => {
                     self.count(|s| {
                         s.files_removed += metrics.num_removed_files as u64;
@@ -257,7 +257,7 @@ impl DeltaSink {
         }
         let before = table.version();
         if cs.upsert_rows() + cs.delete_rows() > 0 {
-            let props = self.props(marker, txn_version, Some(&position));
+            let props = self.props(marker.owner_epoch, txn_version, Some(&position));
             match merge(table, cs, &types, props, self.config.merge_memory_limit).await {
                 Ok((t, metrics)) => {
                     self.count(|s| s.add(&metrics));
@@ -271,7 +271,7 @@ impl DeltaSink {
             table.version().expect("a written table is loaded")
         } else {
             // Nothing changed a row: a marker-only commit still moves the position.
-            let props = self.props(marker, txn_version, Some(&position));
+            let props = self.props(marker.owner_epoch, txn_version, Some(&position));
             match marker_only(&table, props, &app_id(pipeline), txn_version).await {
                 Ok(v) => v,
                 Err(e) => return race_or_fail(e),
@@ -357,14 +357,11 @@ impl Sink for DeltaSink {
         // finds the newer epoch.
         let txn_version = i64::try_from(owner_epoch)
             .map_err(|_| permanent(format!("owner epoch {owner_epoch}")))?;
-        let props = CommitProperties::default()
-            .with_application_transaction(Transaction::new(app_id(pipeline), txn_version))
-            .with_metadata(commit_metadata(pipeline, owner_epoch, None));
         let (t, outcome) = evolve(
             h.table().clone(),
             delta,
             self.config.table.timestamp_ntz_as,
-            props,
+            self.props(owner_epoch, txn_version, None),
         )
         .await?;
         h.set_table(t);
@@ -523,9 +520,13 @@ mod tests {
         let out = zombie.evolve(orders().table(), &add_note(), 5).await;
         assert_eq!(out.unwrap(), EvolveOutcome::Fenced { current_epoch: 6 });
         assert_eq!(columns(&zombie).await, before, "nothing added");
-        let out = owner.evolve(orders().table(), &add_note(), 6).await;
+        // The ADD COLUMN carries the fence: a write of an older owner after it is fenced.
+        let out = owner.evolve(orders().table(), &add_note(), 7).await;
         assert_eq!(out.unwrap(), EvolveOutcome::Applied);
         assert_eq!(columns(&owner).await, before + 1);
+        let late = rows(30, &[3]);
+        let out = owner.commit(late.clone(), marker(6, &late)).await;
+        assert_eq!(out.unwrap(), CommitOutcome::Fenced { current_epoch: 7 });
     }
 
     #[tokio::test]
@@ -548,7 +549,7 @@ mod tests {
             .commit(theirs.clone(), marker(6, &theirs))
             .await
             .unwrap();
-        let props = zombie.props(&marker(5, &first), 5, None);
+        let props = zombie.props(5, 5, None);
         let err = evolve(stale, &add_note(), NtzAs::default(), props)
             .await
             .map(|(_, o)| o)
```

### After Task 6: the review fix

Change `crates/data-avalanche-pipeline/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/error.rs b/crates/data-avalanche-pipeline/src/error.rs
index 109e45f..0d06f4d 100644
--- a/crates/data-avalanche-pipeline/src/error.rs
+++ b/crates/data-avalanche-pipeline/src/error.rs
@@ -108,7 +108,9 @@ impl WorkerError {
                 StoreError::Transient { .. } | StoreError::Contention { .. },
             ))
             | Self::Contended { .. } => Recovery::Retry,
-            Self::Source(e) if e.is_transient() => Recovery::Retry,
+            Self::Source(e) if e.is_transient() || matches!(e, SourceError::Contended(_)) => {
+                Recovery::Retry
+            }
             Self::Sink(e) if e.is_transient() => Recovery::Retry,
             Self::Fenced { .. } | Self::LeaseLost => Recovery::Lease,
             Self::Refused(_)
@@ -179,6 +181,11 @@ mod tests {
                 WorkerError::Source(SourceError::Transient("reset".into())),
                 Recovery::Retry,
             ),
+            // Phase 0's `prepare` met the slot active for another PID.
+            (
+                WorkerError::Source(SourceError::Contended("slot active".into())),
+                Recovery::Retry,
+            ),
             (
                 WorkerError::Sink(SinkError::Transient {
                     reason: "503".into(),
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index f6b0afe..f99400a 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -160,8 +160,12 @@ impl PipelineWorker {
                         "the source lost the stream position {position} ({detail}): the stream restarts at a new position and the tables are snapshotted again"
                     );
                     core.update(|st| st.warn(warning));
-                    core.may_write()?;
-                    let from = retry(&core.tuning, || core.source.reset_position()).await?;
+                    let from = write_retry(
+                        &core.tuning,
+                        || core.may_write(),
+                        || core.source.reset_position(),
+                    )
+                    .await?;
                     start(core, Some(from)).await?;
                     attempt = 0;
                     continue;
@@ -206,13 +210,30 @@ async fn drained_while(core: &mut Core, delay: std::time::Duration) -> bool {
 }
 
 /// A source call, retried while it fails transiently.
-async fn retry<T, F, Fut>(tuning: &Tuning, mut call: F) -> Result<T, WorkerError>
+async fn retry<T, F, Fut>(tuning: &Tuning, call: F) -> Result<T, WorkerError>
 where
     F: FnMut() -> Fut,
     Fut: Future<Output = Result<T, SourceError>>,
+{
+    write_retry(tuning, || Ok(()), call).await
+}
+
+/// A source write, retried while it fails transiently: `may_write` (`Core::may_write`) is asked
+/// before every try, as the writer asks before each commit, so a lease that lapses during the
+/// retries stops the write.
+async fn write_retry<T, G, F, Fut>(
+    tuning: &Tuning,
+    may_write: G,
+    mut call: F,
+) -> Result<T, WorkerError>
+where
+    G: Fn() -> Result<(), WorkerError>,
+    F: FnMut() -> Fut,
+    Fut: Future<Output = Result<T, SourceError>>,
 {
     let mut attempt = 0;
     loop {
+        may_write()?;
         match call().await {
             Ok(v) => return Ok(v),
             Err(e) if e.is_transient() => {
@@ -224,20 +245,27 @@ where
     }
 }
 
-/// A sink call, retried while it fails transiently.
-async fn sink_retry<T, F, Fut>(tuning: &Tuning, mut call: F) -> Result<T, SinkError>
+/// A sink call, retried while it fails transiently. A write passes `Core::may_write` as
+/// `may_write`, asked before every try (as in [`write_retry`]); a read passes `|| Ok(())`.
+async fn sink_retry<T, G, F, Fut>(
+    tuning: &Tuning,
+    may_write: G,
+    mut call: F,
+) -> Result<T, WorkerError>
 where
+    G: Fn() -> Result<(), WorkerError>,
     F: FnMut() -> Fut,
     Fut: Future<Output = Result<T, SinkError>>,
 {
     let mut attempt = 0;
     loop {
+        may_write()?;
         match call().await {
             Err(e) if e.is_transient() => {
                 tokio::time::sleep(tuning.sink_backoff.delay(attempt)).await;
                 attempt += 1;
             }
-            r => return r,
+            r => return r.map_err(WorkerError::Sink),
         }
     }
 }
@@ -386,8 +414,12 @@ pub(crate) async fn start(
             }
             let refs: Vec<TableRef> = resolved_tables.keys().cloned().collect();
             // `prepare` may create, recreate or advance the slot.
-            core.may_write()?;
-            let prepared = retry(&tuning, || source.prepare(checkpoint.as_ref(), &refs)).await?;
+            let prepared = write_retry(
+                &tuning,
+                || core.may_write(),
+                || source.prepare(checkpoint.as_ref(), &refs),
+            )
+            .await?;
             warnings.extend(prepared.warnings);
             match prepared.plan {
                 StartPlan::Resume => {
@@ -434,9 +466,7 @@ pub(crate) async fn start(
             tracked.insert(table.clone());
             continue;
         }
-        let state = sink_retry(&tuning, || sink.table_state(table))
-            .await
-            .map_err(WorkerError::Sink)?;
+        let state = sink_retry(&tuning, || Ok(()), || sink.table_state(table)).await?;
         let doc = core.docs.table_snapshot(table).await?.map(|v| v.doc);
         let in_progress = doc
             .as_ref()
@@ -521,15 +551,14 @@ pub(crate) async fn start(
             _ => {}
         }
         if !state.exists {
-            core.may_write()?;
-            match sink_retry(&tuning, || sink.ensure_table(&spec)).await {
+            match sink_retry(&tuning, || core.may_write(), || sink.ensure_table(&spec)).await {
                 Ok(()) => {}
-                Err(SinkError::Permanent { reason }) => {
+                Err(WorkerError::Sink(SinkError::Permanent { reason })) => {
                     halts.push((table.clone(), HaltReason::SinkRejected { detail: reason }));
                     tracked.insert(table.clone());
                     continue;
                 }
-                Err(e) => return Err(WorkerError::Sink(e)),
+                Err(e) => return Err(e),
             }
         }
         if !tracked_before || matches!(plan, Plan::Fresh(_)) {
@@ -622,7 +651,7 @@ pub(crate) async fn start(
 mod tests {
     use data_avalanche_core::position::Lsn;
     use data_avalanche_core::{ColumnSchema, ColumnValue, ColumnValueType as V, Row};
-    use data_avalanche_testkit::{FakeTxn, Method};
+    use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
 
     use super::*;
     use crate::test_support::{Countdown, CrashAt, World, Yaml, fast, row, t, wait_until};
@@ -1245,6 +1274,40 @@ mod tests {
         }
     }
 
+    /// A write retried while it fails transiently asks the guard before every try, as the writer
+    /// does: the lease lapsing between two tries of `prepare` (the third write) or `ensure_table`
+    /// (the fourth) stops it there, and the write never lands.
+    #[tokio::test]
+    async fn a_lease_lost_while_a_write_is_retried_stops_the_retry() {
+        for n in [3, 4] {
+            let w = World::new(&["a"]);
+            w.source.load(&t("a"), rows(1..=3));
+            if n == 3 {
+                for _ in 0..3 {
+                    w.source
+                        .fail(Method::Prepare, SourceError::Transient("down".into()));
+                }
+            } else {
+                w.sink
+                    .fail(SinkOp::EnsureTable, None, 3, || SinkError::Transient {
+                        reason: "down".into(),
+                    });
+            }
+            let mut d = deps(&w);
+            d.guard = Arc::new(Countdown(AtomicUsize::new(n)));
+            let (worker, _handle) = PipelineWorker::new(&mode("mode: initial"), d, fast());
+            let end = tokio::time::timeout(Duration::from_secs(5), worker.run())
+                .await
+                .expect("the lease lapses within Phase 0");
+            assert!(matches!(end, Err(WorkerError::LeaseLost)), "{n}: {end:?}");
+            assert_eq!(
+                phase_0_writes(&w).await,
+                n - 1,
+                "{n}: the retried write never landed"
+            );
+        }
+    }
+
     /// A lost stream position is replaced only under the lease: `reset_position` recreates the
     /// slot, a write like Phase 0's.
     #[tokio::test]
```

### After Task 8: the review fix

Change `crates/data-avalanche-cluster/src/lease.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/lease.rs b/crates/data-avalanche-cluster/src/lease.rs
index 36f1b48..436a7fb 100644
--- a/crates/data-avalanche-cluster/src/lease.rs
+++ b/crates/data-avalanche-cluster/src/lease.rs
@@ -444,6 +444,60 @@ mod tests {
         }
     }
 
+    /// A slow write proves the lease only from when it was sent: the deadline counts from
+    /// before the write, never from its answer.
+    #[tokio::test(start_paused = true)]
+    async fn a_slow_renewal_s_deadline_counts_from_before_the_write() {
+        let store = ScriptedStore::new();
+        let lease = acquired(&leases(&store), "a").await;
+        store.fail_next(Fault {
+            kind: Kind::Slow(Duration::from_secs(3)),
+            lands: true,
+        });
+        store.fail_always(Some(Fault {
+            kind: Kind::Transient,
+            lands: false,
+        }));
+        let (run, _m) = start(&store, lease);
+        advance(8_010).await;
+        assert_eq!(
+            run.guard.state(),
+            RenewState::Held,
+            "sent at 5 s, answered at 8 s"
+        );
+        advance(9_730).await;
+        assert!(run.guard.may_commit());
+        advance(20).await;
+        assert!(!run.guard.may_commit(), "the deadline is 5 s + 12.75 s");
+    }
+
+    /// After a write whose outcome is unknown, the lease read back names another holder: it was
+    /// taken meanwhile, and is lost (never adopted).
+    #[tokio::test(start_paused = true)]
+    async fn a_lease_taken_while_a_renewal_s_outcome_is_unknown_is_lost() {
+        let store = ScriptedStore::new();
+        let ls = leases(&store);
+        let lease = acquired(&ls, "a").await;
+        store.fail_next(Fault {
+            kind: Kind::Contention,
+            lands: false,
+        });
+        let (run, _m) = start(&store, lease);
+        advance(5_010).await;
+        assert_eq!(run.guard.state(), RenewState::Degraded);
+        let seen = ls.observe("p").await.unwrap().unwrap();
+        ls.take_over(&seen, &holder("b"), timing().duration, Utc::now())
+            .await
+            .unwrap();
+        advance(1_000).await;
+        assert_eq!(run.guard.state(), RenewState::Lost);
+        assert!(!run.guard.may_commit());
+        match run.stop().await {
+            RenewEnd::Lost { reason } => assert!(reason.contains("another holder"), "{reason}"),
+            other => panic!("{other:?}"),
+        }
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_renewal_that_landed_behind_an_error_is_still_held() {
         let store = ScriptedStore::new();
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index df03330..18b1981 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -21,6 +21,8 @@ pub(crate) enum Kind {
     Permanent,
     /// The write never answers.
     Hang,
+    /// The write lands after this long, and answers.
+    Slow(Duration),
 }
 
 /// A failed write. `lands`: the write goes through, then the error comes back (an unknown
@@ -40,7 +42,7 @@ impl Fault {
                 reason: "injected".into(),
             },
             Kind::Contention => StoreError::Contention { key },
-            Kind::Permanent | Kind::Hang => StoreError::Permanent {
+            Kind::Permanent | Kind::Hang | Kind::Slow(_) => StoreError::Permanent {
                 key,
                 reason: "injected".into(),
             },
@@ -100,6 +102,13 @@ impl ScriptedStore {
             Some(Fault {
                 kind: Kind::Hang, ..
             }) => std::future::pending().await,
+            Some(Fault {
+                kind: Kind::Slow(after),
+                ..
+            }) => {
+                tokio::time::sleep(after).await;
+                write.await
+            }
             Some(f) if f.lands => {
                 write.await?;
                 Err(f.error(key))
```

### After Task 9: the review fix

Change `crates/data-avalanche-cluster/src/claim.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/claim.rs b/crates/data-avalanche-cluster/src/claim.rs
index 1df67d0..dfe4a99 100644
--- a/crates/data-avalanche-cluster/src/claim.rs
+++ b/crates/data-avalanche-cluster/src/claim.rs
@@ -72,6 +72,18 @@ impl Claim {
                     written: sent,
                 });
             }
+            Acquired::HeldBy(seen) if seen.doc.is_held_by(&self.holder) => {
+                // An earlier step's write landed behind its error: the lease is this node's.
+                // It is renewed, so `written` is a send time this step saw (a later one than
+                // the landed write's would let the renewer's first deadline outlast the lease).
+                self.watched = None;
+                let sent = Instant::now();
+                let lease = self.leases.renew(&seen, Utc::now()).await?;
+                return Ok(Claimed::Mine {
+                    lease,
+                    written: sent,
+                });
+            }
             Acquired::HeldBy(seen) => seen,
         };
         // The wait runs from when the version was read, never from before: a renewal that
@@ -263,6 +275,76 @@ mod tests {
         assert_eq!(epoch(&b.step().await.unwrap()), 2);
     }
 
+    /// An acquire whose write landed behind an error: the next step finds the lease naming this
+    /// holder, and it is this node's at once (renewed, the epoch unchanged, `written` when the
+    /// renewal was sent), never taken over.
+    #[tokio::test(start_paused = true)]
+    async fn an_acquire_that_landed_behind_an_error_is_this_nodes() {
+        let store = ScriptedStore::new();
+        let mut b = claim(&store, "b");
+        store.fail_next(Fault {
+            kind: Kind::Transient,
+            lands: true,
+        });
+        assert!(b.step().await.is_err());
+        // The renewal is slow: `written` is still when it was sent.
+        store.fail_next(Fault {
+            kind: Kind::Slow(Duration::from_secs(2)),
+            lands: true,
+        });
+        let before = Instant::now();
+        let mine = b.step().await.unwrap();
+        assert_eq!(epoch(&mine), 1);
+        let Claimed::Mine { written, .. } = mine else {
+            unreachable!()
+        };
+        assert_eq!(written, before);
+    }
+
+    /// `written` is taken before the write is sent, whether it acquires or takes over: a slow
+    /// write mustn't move the renewer's first deadline past what the lease allows.
+    #[tokio::test(start_paused = true)]
+    async fn written_is_taken_before_the_write() {
+        let store = ScriptedStore::new();
+        let slow = Fault {
+            kind: Kind::Slow(Duration::from_secs(2)),
+            lands: true,
+        };
+        let mut a = claim(&store, "a");
+        let before = Instant::now();
+        store.fail_next(slow);
+        let Claimed::Mine { written, .. } = a.step().await.unwrap() else {
+            panic!("acquired")
+        };
+        assert_eq!(written, before, "the acquire's write was sent at once");
+        let mut b = claim(&store, "b");
+        b.step().await.unwrap();
+        advance(15_000).await;
+        let before = Instant::now();
+        store.fail_next(slow);
+        let Claimed::Mine { written, .. } = b.step().await.unwrap() else {
+            panic!("taken over")
+        };
+        assert_eq!(written, before, "the takeover's write was sent at once");
+    }
+
+    /// The takeover wait runs from when the holder's lease was read, not from when the read was
+    /// sent: a renewal landing while the read is in flight counts.
+    #[tokio::test(start_paused = true)]
+    async fn the_takeover_wait_runs_from_when_the_read_returned() {
+        let store = ScriptedStore::new();
+        acquired(&leases(&store), "a").await;
+        store.slow_reads(Duration::from_secs(2));
+        let sent = Instant::now();
+        let Claimed::HeldBy { takeover_at, .. } = claim(&store, "b").step().await.unwrap() else {
+            panic!("held by a")
+        };
+        assert_eq!(
+            takeover_at,
+            sent + Duration::from_secs(2) + timing().duration
+        );
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_takeover_that_failed_is_tried_again_at_the_next_step() {
         let store = ScriptedStore::new();
```

Change `crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index 18b1981..5e0623b 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -56,6 +56,8 @@ struct Script {
     next: VecDeque<Fault>,
     /// Every write fails so, once `next` is empty.
     always: Option<Fault>,
+    /// How long every read takes.
+    read_delay: Duration,
 }
 
 /// An in-memory store whose writes (`create`, `update`, `put`) fail as scripted.
@@ -82,6 +84,11 @@ impl ScriptedStore {
         self.lock().always = fault;
     }
 
+    /// Every read from now on answers after `delay`.
+    pub fn slow_reads(&self, delay: Duration) {
+        self.lock().read_delay = delay;
+    }
+
     fn lock(&self) -> std::sync::MutexGuard<'_, Script> {
         self.script
             .lock()
@@ -121,6 +128,8 @@ impl ScriptedStore {
 #[async_trait]
 impl VersionedStore for ScriptedStore {
     async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
+        let delay = self.lock().read_delay;
+        tokio::time::sleep(delay).await;
         self.inner.get(key).await
     }
 
```

Change `crates/data-avalanche-cluster/src/claim.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/claim.rs b/crates/data-avalanche-cluster/src/claim.rs
index dfe4a99..6c0ef96 100644
--- a/crates/data-avalanche-cluster/src/claim.rs
+++ b/crates/data-avalanche-cluster/src/claim.rs
@@ -72,36 +72,30 @@ impl Claim {
                     written: sent,
                 });
             }
-            Acquired::HeldBy(seen) if seen.doc.is_held_by(&self.holder) => {
-                // An earlier step's write landed behind its error: the lease is this node's.
-                // It is renewed, so `written` is a send time this step saw (a later one than
-                // the landed write's would let the renewer's first deadline outlast the lease).
-                self.watched = None;
-                let sent = Instant::now();
-                let lease = self.leases.renew(&seen, Utc::now()).await?;
-                return Ok(Claimed::Mine {
-                    lease,
-                    written: sent,
-                });
-            }
             Acquired::HeldBy(seen) => seen,
         };
-        // The wait runs from when the version was read, never from before: a renewal that
-        // landed while the read was in flight must count.
-        let read = Instant::now();
-        let since = match &self.watched {
-            Some((version, since)) if *version == seen.version => *since,
-            _ => {
-                self.watched = Some((seen.version.clone(), read));
-                read
+        // A lease naming this holder is this process's own earlier term: one its renewer gave up
+        // as lost, or an earlier step's write that landed behind its error. No one else holds
+        // it, so it is taken over at once, under the next epoch: a new term never shares an
+        // epoch with a worker that already ran.
+        if !seen.doc.is_held_by(&self.holder) {
+            // The wait runs from when the version was read, never from before: a renewal that
+            // landed while the read was in flight must count.
+            let read = Instant::now();
+            let since = match &self.watched {
+                Some((version, since)) if *version == seen.version => *since,
+                _ => {
+                    self.watched = Some((seen.version.clone(), read));
+                    read
+                }
+            };
+            let takeover_at = since + self.timing.duration;
+            if read < takeover_at {
+                return Ok(Claimed::HeldBy {
+                    node_id: seen.doc.holder_node_id,
+                    takeover_at,
+                });
             }
-        };
-        let takeover_at = since + self.timing.duration;
-        if read < takeover_at {
-            return Ok(Claimed::HeldBy {
-                node_id: seen.doc.holder_node_id,
-                takeover_at,
-            });
         }
         let sent = Instant::now();
         match self
@@ -276,8 +270,8 @@ mod tests {
     }
 
     /// An acquire whose write landed behind an error: the next step finds the lease naming this
-    /// holder, and it is this node's at once (renewed, the epoch unchanged, `written` when the
-    /// renewal was sent), never taken over.
+    /// holder and takes it over at once, under the next epoch (`written` when that write was
+    /// sent), never after a lease duration.
     #[tokio::test(start_paused = true)]
     async fn an_acquire_that_landed_behind_an_error_is_this_nodes() {
         let store = ScriptedStore::new();
@@ -287,14 +281,14 @@ mod tests {
             lands: true,
         });
         assert!(b.step().await.is_err());
-        // The renewal is slow: `written` is still when it was sent.
+        // The takeover is slow: `written` is still when it was sent.
         store.fail_next(Fault {
             kind: Kind::Slow(Duration::from_secs(2)),
             lands: true,
         });
         let before = Instant::now();
         let mine = b.step().await.unwrap();
-        assert_eq!(epoch(&mine), 1);
+        assert_eq!(epoch(&mine), 2);
         let Claimed::Mine { written, .. } = mine else {
             unreachable!()
         };
```

Change `crates/data-avalanche-cluster/src/claim.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/claim.rs b/crates/data-avalanche-cluster/src/claim.rs
index 6c0ef96..12fdddb 100644
--- a/crates/data-avalanche-cluster/src/claim.rs
+++ b/crates/data-avalanche-cluster/src/claim.rs
@@ -1,7 +1,8 @@
 //! Getting a pipeline's lease and giving it back (Clustering §5.3, §8.3). A free lease (absent,
 //! or a released tombstone) is acquired at once. A live holder's lease is taken over only after
 //! it stayed unchanged for a full `lease_duration` on this node's clock: a holder that renews in
-//! the meantime keeps it.
+//! the meantime keeps it. A lease naming this process (its node id and incarnation) is its own
+//! earlier term, taken over at once under the next epoch.
 
 use chrono::Utc;
 use data_avalanche_state::{Acquired, Holder, LeaseDoc, Leases, StateError, Versioned};
@@ -77,7 +78,8 @@ impl Claim {
         // A lease naming this holder is this process's own earlier term: one its renewer gave up
         // as lost, or an earlier step's write that landed behind its error. No one else holds
         // it, so it is taken over at once, under the next epoch: a new term never shares an
-        // epoch with a worker that already ran.
+        // epoch with a worker that already ran. This rests on (node id, incarnation) naming one
+        // process: the incarnation is its start in milliseconds, so a restarted process waits.
         if !seen.doc.is_held_by(&self.holder) {
             // The wait runs from when the version was read, never from before: a renewal that
             // landed while the read was in flight must count.
@@ -339,6 +341,24 @@ mod tests {
         );
     }
 
+    /// A restarted process has the node id of the lease's holder but another incarnation: the
+    /// holder may still be alive, so it waits a full lease duration like any other node.
+    #[tokio::test(start_paused = true)]
+    async fn a_restarted_process_waits_a_full_lease_duration() {
+        let store = ScriptedStore::new();
+        acquired(&leases(&store), "b").await;
+        let reborn = Holder {
+            node_id: "b".into(),
+            incarnation: 2,
+        };
+        let mut b = Claim::new(leases(&store), "p", reborn, timing());
+        assert!(matches!(b.step().await.unwrap(), Claimed::HeldBy { .. }));
+        advance(14_990).await;
+        assert!(matches!(b.step().await.unwrap(), Claimed::HeldBy { .. }));
+        advance(10).await;
+        assert_eq!(epoch(&b.step().await.unwrap()), 2);
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_takeover_that_failed_is_tried_again_at_the_next_step() {
         let store = ScriptedStore::new();
```
