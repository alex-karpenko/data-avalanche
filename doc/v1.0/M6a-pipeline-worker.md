---
updated: 2026-10-07
domain: Data Flow
tags: [v1, implementation-plan, m6a, pipeline-worker, snapshot, checkpoint, schema-evolution, halts, crash-recovery, postgresql]
resolves: [DA-SNAP-9]
sources: []
---

# M6a — Pipeline Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pipeline worker in `data-avalanche-pipeline`: one pipeline's whole data path
from a `SourceConnector` to a `Sink`. It runs Phase 0 (state, resolution, the per-table snapshot
decision), then stream sessions. A session accumulates and flushes, commits through an ordered sink
writer, persists the checkpoint and only then acknowledges it. It runs snapshots inside the
session, reconciled against the stream by what each chunk's read saw (DA-SNAP-9), applies schema
changes (§8), halts tables, reconnects, and guards WAL retention. Every crash point converges on
restart. Tested against a scripted fake source and the in-memory sink, and against PostgreSQL 18
on every change (14–17 weekly).

M6 is split in two (owner decision, 2026-10-02). **M6a** (this plan) is the worker as a library.
**M6b** adds single-node control around it: the lease and renewer behind `CommitGuard`, the
`PipelineController`, the binary with `run --config`, the HTTP health and metrics listener, the
M4-carried Delta items, and the slice-1 E2E suite on RustFS (§5.3), which is M6's exit.

**Architecture:** Small changes in five crates, then the worker in `data-avalanche-pipeline`.

- **`data-avalanche-core`**:
  - `Chunk::visibility` (`ChunkVisibility::Xids(XidSnapshot)`, `XidSnapshot::sees`) and
    `SourceConnector::in_flight` (writers in flight now, by table).
  - `SourceError::Contended` (a busy slot) and two halt codes, `SINK_REJECTED` and
    `SNAPSHOT_TIMEOUT`.
  - `TableChangeSet::snapshot`, which the position guard never skips (Q-M6a-6).
  - serde for `TableSchema`, so the worker can cache it in state.
- **`data-avalanche-source-pg`**: a chunk's read takes `pg_current_snapshot()` in its own
  transaction; `in_flight` lists writing transactions by table (`pg_locks`); `55006` is
  `Contended`.
- **`data-avalanche-state`**: the schema cache (`schemas/<schema>.<table>.json`), the halt
  registry (`halts.json`) and the slot-creation intent (`confirmed_flush_lsn` `0/0`).
- **`data-avalanche-config`**: V-118, `timeouts.snapshot` ≥ 10 s (Q-M6a-7).
- **`data-avalanche-testkit`**: `FakeSource`, a scripted `SourceConnector` with an in-memory
  table store, MVCC-style chunk visibility and stalled commits; `MemorySink` faults, a commit hold
  and a takeover.
- **`data-avalanche-pipeline`** (the worker), one module per concern:
  - `settings`: the worker's view of the resolved `Pipeline`.
  - `decide`: the per-table snapshot decision (pure).
  - `error`, `hooks`, `backoff`, `status`: `WorkerError`; `CommitGuard` and the crash-test hooks;
    the jittered backoff; commands, exits, status and tuning.
  - `writer`: the sink writer task. It commits change sets in order, then persists halts, the
    checkpoint, snapshot progress and schema caches, then reports `Durable`.
  - `session`: one stream session's event loop (flushes, backpressure, barriers, splits, halts,
    schema signals, `re_query`, channel D, the WAL-lag guard).
  - `session/snapshot`: snapshots inside the session (chunk reads, reconciliation, emission,
    cursor docs, the in-flight wait, the timeout).
  - `reconcile`: the DA-SNAP-9 reconciler. `cursor`: keyset cursor ↔ state document.
    `schema_change`: channel C plus the classification and the sink's evolve.
  - `worker`: `PipelineWorker`. It runs Phase 0 and the sessions, the reconnect backoff, the busy
    slot window and the lost-position reset.
- **Tests:**
  - Unit tests in each module run without a server, mostly on `FakeSource` and `MemorySink`.
  - The integration binary `tests/pg` drives the worker against `postgres-18`
    (`DATA_AVALANCHE_IT`). `tests/sync_commit` is DA-SNAP-9's regression in its own binary.
    `weekly.yml` runs `tests/pg` on PostgreSQL 14–17.

**Tech Stack:** Rust 1.98.1 (edition 2024). No new external crate: every dependency below is
already in the workspace.

| Crate | Used for |
|---|---|
| `tokio` (`sync`, `rt`, `time`, `macros`) | The writer task, the session's `select!`, timers, the control and status channels |
| `futures-util` | The chunk reads in flight (`FuturesUnordered`) |
| `getrandom` | Backoff jitter |
| `serde_json`, `uuid`, `chrono` | Cursor documents, test fixtures, timestamps in state documents |
| `data-avalanche-config`, `-state`, `-storage` | The resolved pipeline; the state documents |
| `data-avalanche-testkit`, `proptest`, `async-trait`, `bytes`, `object_store` | dev: fakes, property tests, the flaky state store |
| `data-avalanche-source-pg`, `tokio-postgres` | dev: the live integration binaries |

**Spec:**
- `doc/v1.0/00-roadmap.md`: §2 (layout, dependency direction; Q-M5-24), §3.3, §3.4, §3.5, §3.6,
  §3.9, §4 M6 (scope, "Carried from M4", "Carried from M5"), §5.3, D-IMPL-2, D-IMPL-3.
- `doc/specs/Data-Flow.md`: Phase 0 (preflight, state load, the per-table snapshot decision),
  Phase 1 (snapshot), Phase 2 (stream), Phase 3 (accumulate and flush), Phase 4 (sink commit,
  position guard), Phase 5 (checkpoint and acknowledgment), §Error Paths (Source Disconnect, Sink
  Commit Failure), §State Layout.
- `doc/specs/Configuration_Specification.md`: §9.1 `source.timeouts` (`snapshot`) and
  `toast_strategy`, §9.2.1 `source.postgres` (`slot_on_orphan`, `max_wal_lag_bytes`,
  `heartbeat`), §9.3 `tables` (`snapshots`), §9.4.1 `flush`, `buffer_capacity_rows`,
  `schema_evolution`, V-104.
- `doc/specs/Schema_Change_Detection_and_Evolution.md`: §2.1, §3 (channels A–D, the fail-safe of
  C), §7.2 (classification), §8 (the apply protocol), §8.2 (gap → re-snapshot).
- `doc/research/Snapshot_Resumability_and_Consistency.md`: §1, §2, §6.
- `doc/research/Delivery_Guarantee_and_Position_Guard.md`: §2, §3 (checkpoint and guard).
- `doc/research/Postgres_Slot_and_Publication_Lifecycle.md`: §1–§4, D-15.5 (a busy slot).
- `doc/research/Large_Transaction_Handling.md`: §1 (splits).
- `doc/research/Pipeline_Lifecycle_State_Machine.md`: §4 (drain).

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. `cargo clippy --workspace
  --all-targets -- -D warnings` and `cargo fmt --all --check` must pass before every commit.
- **Dependency direction** (roadmap §2, Q-M5-24): `pipeline → core, config, state, storage`.
  The pipeline never depends on `sink-delta` or on a source crate. `source-pg` and `tokio-postgres`
  are **dev**-dependencies, for the live binaries only. No crate takes `sink-delta` as a
  dev-dependency.
- **Ordering** (Data-Flow Phase 5, D-IMPL-2): the sink commits every table of a flush, then the
  halts recorded since the last flush are persisted. Only then is the checkpoint persisted,
  followed by snapshot progress and schema caches. The source is acknowledged only after that.
  Nothing is acknowledged that isn't durable.
- **Idempotency is correctness:** every replay after a crash re-commits through the guard and
  the fence. A test that crashes at a hook point must converge on restart.
- **Test gates:** plain `cargo test` stays hermetic. The integration binaries gate on
  `DATA_AVALANCHE_IT` and run against `postgres-18` (port 55418), or the server
  `DATA_AVALANCHE_PG_PORT` names. `tests/sync_commit` changes `synchronous_standby_names`
  server-wide and restores it on every path.
- **Credentials:** the tests use the compose services' fixed test credentials only. No key, key
  path or secret goes in code, docs or output.
- **Public-API conventions:** public types are re-exported at the crate root; library errors are
  `thiserror` (`WorkerError`).
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready.
  `Cargo.lock` follows the build and is committed with each task; it isn't shown.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to break a mirror. Each is pinned by a test in the task after
the arrow.

1. **A chunk that missed a change the stream already applied.** A transaction commits after a
   chunk's read began, or it is committed and decoded but not yet visible: a synchronous commit
   waiting for its standby. The chunk's stale row must not undo it. That holds for one chunk, for
   two chunks read in parallel and for a TRUNCATE → Task 9 (`a_change_applied_before_the_issue_but_still_invisible_drops_its_rows`,
   `a_proof_covers_only_the_reads_issued_after_it`, `an_unseen_truncate_drops_the_chunk`). Task 10
   (`a_stalled_commit_applied_before_the_read_keeps_its_change`,
   `a_table_waits_for_the_writers_in_flight_at_the_start`). Task 13
   (`a_snapshot_under_concurrent_writes_ends_equal_to_the_source`, which fails without the
   reconciler, and `tests/sync_commit`, which fails under the window rule).
2. **A crash between any two durable steps.** That means after a sink commit, after a checkpoint,
   after a chunk, or after the last chunk. A restart must converge with no lost and no
   resurrected row → Task 12 (`a_restart_after_a_crash_at_any_point_converges`), Task 13
   (`a_crash_mid_snapshot_resumes_from_the_saved_cursor`), Task 6 (`a_crash_after_the_commit_leaves_the_checkpoint_behind`,
   `a_crash_after_the_checkpoint_leaves_progress_and_schemas_behind`).
3. **Chunk rows at or behind the table's marker.** An idle source, or a replay after a restart,
   emits a chunk at a frontier the marker already covers. The guard must not skip it (Q-M6a-6)
   → Task 1 (`the_guard_never_skips_a_snapshot_set`, `a_snapshot_set_only_upserts`), Task 10
   (`a_snapshot_reads_the_table_in_chunks_while_streaming`), Task 12
   (`a_restart_after_a_crash_at_any_point_converges`).
4. **The catalog moving under the worker.** DDL arrives mid-stream, a table changes under its
   snapshot (idle or not), a table is dropped, or a table is dropped and re-created while the
   worker is down. Each change applies in stream order or halts only its table → Task 7 (`an_added_column_is_evolved_where_the_stream_meets_it`,
   `an_unsafe_change_halts_the_table`, `a_failed_catalog_read_halts_the_table`), Task 8 (`a_rescan_evolves_an_idle_tables_change`,
   `a_dropped_table_halts_once_the_stream_has_its_last_changes`), Task 10
   (`a_chunk_read_under_the_old_schema_is_read_again`), Task 11 (`a_table_dropped_while_down_halts`,
   `a_table_created_again_while_down_halts`), Task 13 (`a_dropped_table_halts_and_the_others_go_on`), Task 14
   (`an_idle_table_changed_while_down_is_snapshotted_under_its_new_schema`), Task 16
   (`a_table_changed_while_down_is_rescanned_before_its_snapshot`,
   `an_idle_table_that_gained_a_column_while_down_is_snapshotted_with_it`).
5. **A sink or a state store that fails for a while, and a slot another reader holds.** Sink
   `Transient` retries forever with DEGRADED status and backpressure (Q-M6a-5). A state write
   retries. A busy slot (`55006`) is retried within a window, and a lost position resets and
   re-snapshots → Task 6 (`a_transient_commit_is_retried_while_degraded`,
   `a_transient_state_write_is_retried_while_degraded`), Task 11
   (`a_busy_slot_is_retried_for_a_while`, `a_position_lost_while_running_is_replaced_and_snapshotted`),
   Task 12 (`a_failing_sink_shows_degraded_until_it_recovers`).

## Decisions and rulings

**Asked and answered (2026-10-02 and 2026-10-03; the plan approved 2026-10-03):**

| # | Question | Answer |
|---|---|---|
| Q-M6a-1 | `snapshots.mode: never` and a recovery case (the position lost, an orphan adopted, a table added) | **No snapshot.** A warning names the gap: the table may miss changes from before the new position |
| Q-M6a-2 | Writers in flight when a session starts (DA-SNAP-9: their commits may predate the stream's start and be invisible to a chunk) | **Wait per table.** `in_flight()` at session start; a table's chunks are read only once none of its writers is still in flight (re-read with backoff 0.5 s → 10 s) |
| Q-M6a-3 | `timeouts.snapshot` exceeded | **Halt the table `SNAPSHOT_TIMEOUT`** and mark its snapshot document `broken`. The clock is per table within one run (from when this run started its snapshot) |
| Q-M6a-4 | When does a channel-D rescan's `changed` schema apply? | **After the stream catches up**: the position read right after the rescan; a change the stream applied meanwhile is skipped |
| Q-M6a-5 | A sink commit that keeps failing `Transient` | **Retry forever**, backoff 1 s → 60 s with jitter, status DEGRADED while retrying; backpressure through `buffer_capacity_rows` stops reading the stream. Data-Flow §Sink Commit Failure changes (S-S below) |
| Q-M6a-6 | Chunk rows emitted at a frontier the table's marker covers (an idle source, or a replay) are skipped by the guard: silent loss while the cursor advances | **A snapshot flag on the change set.** `CommitMarker::covers` never skips a snapshot set; the fence still applies; `validate` refuses a snapshot set with deletes, a truncate or a split. The worker flushes the stream's changes before a chunk, then commits the chunk as its own set (S-T below) |
| Q-M6a-7 | `timeouts.snapshot: 0` halts every snapshot at once (the duration is literal) | **A V-rule: at least 10 s** (V-118, Task 15; spec edit S-U) |
| Q-M6a-8 | A table whose schema changed while the worker was down (an idle table under `always`, or a resumed snapshot): a snapshot under the cached columns leaves an added column NULL | **Rescan before snapshotting** (Task 16, D-M6a.11) |

**Rulings made while building** (each pinned by a test; D-IMPL-13 in the roadmap):

- **D-M6a.1 — Halts.** A halt's `at` is the accumulator checkpoint when the table halted: the sink
  holds the table's changes through it. Halts are persisted before the next checkpoint. A halt
  persisted at an earlier run keeps the table halted at every start until M9's re-snapshot clears
  it.
- **D-M6a.2 — Channel C.** A failed catalog read for a schema signal halts the table
  `UNSAFE_DDL` "the catalog read for a schema change failed: …". That covers three failures in a
  row (`Tuning::augment_attempts`) or any non-transient error. It is §3 C's fail-safe.
- **D-M6a.3 — Evolve.** `Permanent` from `evolve` halts the table `SINK_REJECTED`. `evolve` checks
  the commit guard first. `Transient` retries like a commit (DEGRADED).
- **D-M6a.4 — State writes.** The writer retries `Transient` and `Contention` from the state store
  like a sink `Transient`. Any other `StateError` ends the worker.
- **D-M6a.5 — Channel D.** Dropped and halted tables also wait for the stream to catch up, not
  only `changed`, and the session flushes first, so a dropped table's last streamed changes
  reach the sink. After an out-of-band schema apply the session flushes, so the schema cache is
  written promptly. A failed rescan round waits for the next round.
- **D-M6a.6 — `re_query`.** At the commit, before `acc.commit`, the rows fetched for keys whose
  UPDATE carried unchanged values are applied as Update events with the transaction's last
  sequence. A fetch's `TableHalt` halts the table; other errors end the session.
- **D-M6a.7 — Snapshot runner.**
  - A chunk read under a schema the table has since left (the stream applied a change) is
    discarded and read again.
  - A `Transient` chunk failure retries the same cursor with the source backoff and triggers a
    rescan (Task 14). A `Permanent` failure ends the session; a `TableHalt` halts the table.
  - A cursor that no longer fits the identifier restarts the table's snapshot from the start,
    with a warning.
  - Chunk rows ignore the transaction limits (a chunk is bounded by `chunk_size`).
  - The cursor or completion document rides the same flush as the chunk's rows, never an
    earlier one. An empty last chunk flushes its completion alone.
- **D-M6a.8 — Phase 0.**
  - After a gap (the position lost, an orphan adopted) a snapshot in progress starts over: rows
    before its cursor may have changed in the gap.
  - Only the slot intent maps a fresh plan to `FirstStart`; an adopted orphan keeps its reason.
  - `never` with a stale in-progress document sets the document `none`.
  - A table added by config is untracked until its schema is cached, so a `prepare` whose reply
    was lost still snapshots it on the retry (the M5-carried "record before prepare").
  - Resume with a schema that drifted while down warns and keeps the cached schema: the stream
    replays the change in order. A fresh position baselines the sink through §8 instead.
- **D-M6a.9 — Sessions.**
  - A mid-run `PositionUnreachable` resets the position (`reset_position`) and reruns Phase 0
    with the new position forced: drift is baselined, snapshots requested, the slot document
    saved.
  - A transient `StateError` at session open or run reconnects like a transient source error.
  - A drain during the reconnect backoff exits `Drained` at once.
  - The attempt counter resets after a session that lasted at least `source_backoff.max`.
  - The `Contended` window (120 s) is measured from the first `Contended` in a row.
  - No stream stall watchdog. The spec has none, a server keep-alive may come only every
    `wal_sender_timeout / 2`, and pgwire's idle feedback surfaces a dead connection as an error.
- **D-M6a.10 — WAL lag** (G-22, V-104). `retained_bytes` is read at every heartbeat and always
  reported in the status. Past `max_wal_lag_bytes` the worker ends with `WorkerError::WalLag`. Any
  failed read, transient or permanent, waits for the next heartbeat and the pipeline keeps
  running. A permanent failure adds one warning per failure streak ("the WAL-lag guard is blind",
  with the error; it names `max_wal_lag_bytes` only when a limit is set). A successful read ends the
  streak. (Owner ruling 2026-10-03, built in the post-Task-16 Batch D.)
- **D-M6a.11 — A schema that drifted while down, under a snapshot** (Q-M6a-8). On a resume,
  Phase 0 records each table whose catalog schema moved past the cached one. A snapshot of such a
  table holds its reads until a rescan has brought the schema up to date. A table the rescan
  doesn't name is released once any schema change the stream applied to it has been handed to
  the writer (at once, if none); a table it names is released once the report has applied, after
  the stream catches up. A table the catch-up skips (its re-read failed, or its cache moved) stays
  held for the next round. A pending schema of a held table is flushed at the rescan itself, so a
  quiet table isn't held until the next timed flush (or forever, with `flush.seconds: null`). A
  failing round is retried with `source_backoff` and warns once per failure streak; a halt or a
  drop releases the table. Without the hold, an `ADD COLUMN` on an idle table would be read under
  the cached columns: PostgreSQL's chunk read checks only those, so the new column would stay
  NULL in every snapshotted row. A column dropped or retyped while the worker runs fails the read
  instead, transiently, and the read's rescan (Task 14) catches up. That includes a `DROP COLUMN`
  that takes its lock before the chunk's `SELECT` but becomes visible only after the read's catalog
  check (SQLSTATE 42703, routine with a synchronous standby): source-pg maps it to the same
  transient error (owner ruling 2026-10-07). (The release conditions after the first sentence
  were refined in the post-Task-16 Batches B and F.)

**Facts measured while building** (outrank memory):

- A multi-statement simple query (`batch_execute("A; B; C")`) is **one** transaction in
  PostgreSQL: the live tests issue one statement per call when they need separate transactions.
- The stream reports a server position only at its own commits and at keep-alives. So
  `pg_current_wal_lsn()` can stay ahead of the checkpoint for up to `wal_sender_timeout / 2` on an
  idle slot. The live tests wait for the data to converge, not for a WAL position.
- `snapshots.mode` defaults to `never` (config §9.3): the live tests ask for `initial`.
- A drain mid-snapshot leaves the snapshot in progress; the next run resumes it from its cursor.

**Documents this plan changes** (applied at plan approval, 2026-10-03, in an `infra:` commit and
a `research:` commit; the gap tracker in a `gap:` commit):

- Roadmap:
  - §2: the dependency diagram. `pipeline → core, config, state, storage`; only the binary
    links `source-pg` and `sink-delta` (Q-M5-24).
  - §3.3 (`TableChangeSet::snapshot`), §3.5 (the schema cache, the halt registry, the slot
    intent), §3.6 (`Chunk::visibility`, `in_flight`, `Contended`).
  - A new §3.11, "The pipeline worker": `PipelineWorker`, `WorkerDeps`, `WorkerHandle`,
    `Command`, `Exit`, `WorkerStatus`, `Phase`, `Tuning`, `CommitGuard`, `Hooks`, `WorkerError`.
  - §4 M6 split into **M6a** (this plan; the "Carried from M5" items it closes are marked) and
    **M6b**: the lease and renewer, `PipelineController`, the binary, HTTP, metrics, the
    lifecycle's persisted desired state, "Carried from M4", and the E2E exit.
  - §7: S-R…S-U. §9: D-IMPL-13 for D-M6a.1…11. §9.1: Q-M6a-1…8.
- `doc/v1.0/README.md`: the M6a row; the `M6-…` row becomes `M6b-…`.
- `Snapshot_Resumability_and_Consistency.md` §1: the merge window closed by each chunk's xid
  snapshot, the reconciler, and the wait for the writers in flight at session start (DA-SNAP-9).
- `CDC_Design_Gap_Analysis_v1.md`: DA-SNAP-9 ✅ (this plan, Tasks 9, 10 and 13); a dated section.

**Spec edits** (approved 2026-10-03; applied in a `spec:` commit on the milestone branch):

| # | Spec | Change | From |
|---|---|---|---|
| S-R | `Data-Flow.md` §State Layout | Add `schemas/<schema>.<table>.json` (the cached `TableSchema` as of the checkpoint, with its source id; written after the checkpoint that covers the change) and `halts.json` (the table halt registry: table, code, detail, `at`). Add to `replication/slot.json`: `confirmed_flush_lsn` `0/0` is the intent to create the slot, written before a managed first-start `prepare` | M5 carried items, D-M6a.1 |
| S-S | `Data-Flow.md` §Sink Commit Failure | "Retry with backoff (up to configured max_retries)" → "Retry `Transient` with backoff 1 s → 60 s and jitter, without limit; the status is DEGRADED while retrying, and the full buffer (`buffer_capacity_rows`) stops reading the source". "On persistent failure: pause pipeline; record error" → "A `Permanent` error halts the table (`SINK_REJECTED`); `NoTable` and any other error end the pipeline" | Q-M6a-5 |
| S-T | `Data-Flow.md` Phase 1 step 2 and its consistency paragraph; Phase 4 §Position guard | Step 2's lw/hw watermark reconciliation becomes the one built here: (a) wait for the writers in flight at the session start; (b) read the chunk in a short repeatable-read transaction that records what it sees (`pg_current_snapshot()`); (c) at emission, drop a chunk row whose key a stream transaction wrote that the read didn't see, and the whole chunk on an unseen TRUNCATE (MySQL, without read visibility: the transactions applied after the read was issued); (d) flush the stream's buffered changes, then commit the chunk as its own change set, flagged `snapshot`; (e) persist the cursor with that flush's checkpoint. The consistency paragraph says why `_seq` alone isn't enough across flushes. The guard never skips a snapshot set (its rows are read now, at a frontier the marker may cover); the fence still applies | DA-SNAP-9, Q-M6a-2, Q-M6a-6 |
| S-U | `Configuration_Specification.md` §9.1 `timeouts.snapshot`, §13.3, §15 | `timeouts.snapshot` is at least `10s`: new rule **V-118** (error, pipeline) | Q-M6a-7 |

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-core/src/source.rs` | `ChunkVisibility`, `XidSnapshot`, `InFlightTxn`, `SourceConnector::in_flight`, `SourceError::Contended`; `RescanReport::changed` applies once the stream catches up |
| `…-core/src/change_set.rs`, `…/sink.rs` | `TableChangeSet::snapshot`, its validation; the guard never skips it; a `day(_ts)` partition is allowed |
| `…-core/src/schema.rs`, `…/logical.rs`, `…/value.rs` | `HaltReason::{SinkRejected, SnapshotTimeout}`; serde for `TableSchema` |
| `crates/data-avalanche-source-pg/src/snapshot.rs`, `…/connector.rs`, `…/error.rs` | A chunk's xid snapshot; `in_flight`; `55006` → `Contended` |
| `crates/data-avalanche-state/src/tables.rs`, `…/pipeline.rs`, `…/snapshot.rs` | The schema cache, the halt registry, `SlotDoc::intent` |
| `crates/data-avalanche-config/src/registry.rs`, `…/src/rules/domain.rs` | V-118 |
| `crates/data-avalanche-testkit/src/fake_source.rs`, `…/memory_sink.rs` | `FakeSource`, `FakeTxn`, `Method`; `MemorySink` faults, hold and takeover |
| `crates/data-avalanche-pipeline/src/settings.rs` | `Settings` from the resolved `Pipeline` |
| `…-pipeline/src/decide.rs` | `decide`: the per-table snapshot decision |
| `…-pipeline/src/error.rs`, `…/hooks.rs`, `…/backoff.rs`, `…/status.rs` | `WorkerError`; `CommitGuard`, `Hooks`; `Backoff`; `Command`, `Exit`, `Phase`, `WorkerStatus`, `Tuning` |
| `…-pipeline/src/writer.rs` | The sink writer task |
| `…-pipeline/src/session.rs` | `Core`, `Session`: the stream session |
| `…-pipeline/src/session/snapshot.rs` | Snapshots inside the session |
| `…-pipeline/src/reconcile.rs`, `…/buffer.rs` (`identity`) | The DA-SNAP-9 reconciler |
| `…-pipeline/src/cursor.rs` | Keyset cursor ↔ `SnapshotCursor` |
| `…-pipeline/src/schema_change.rs` | `Evolver`: channel C, classification, evolve |
| `…-pipeline/src/worker.rs` | `PipelineWorker`, `WorkerDeps`, `WorkerHandle`; Phase 0 (`start`) |
| `…-pipeline/src/test_support.rs` | Unit-test fixtures: pipelines, `World`, the flaky state store, crash hooks |
| `…-pipeline/tests/pg/*.rs` | The worker against PostgreSQL: DML, truncate, snapshots, schema, recovery |
| `…-pipeline/tests/sync_commit.rs` | DA-SNAP-9 with a stalled synchronous commit |
| `.github/workflows/weekly.yml` | `tests/pg` on PostgreSQL 14–17 |

---


### Task 1: Core: chunk visibility, writers in flight, the snapshot flag, schema serde

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs`, `…/change_set.rs`, `…/sink.rs`,
  `…/schema.rs`, `…/logical.rs`, `…/value.rs`, `…/lib.rs`
- Modify (one `snapshot: false` each, so the workspace compiles):
  `crates/data-avalanche-pipeline/src/buffer.rs`, `crates/data-avalanche-testkit/src/memory_sink.rs`,
  `crates/data-avalanche-sink-delta/src/test_support.rs`, `crates/data-avalanche-sink-delta/tests/sink/common.rs`
- Modify: `crates/data-avalanche-source-pg/src/snapshot.rs` (`visibility: None` until Task 2)

**Interfaces:**
- Consumes: the M5 source contract (`Chunk`, `SourceConnector`, `SourceError`), `TableChangeSet`,
  `CommitMarker::covers`, `TableSpec::new`, `HaltReason`, `TableSchema` (core, M3/M5).
- Produces (core root):
  - `ChunkVisibility::Xids(XidSnapshot)`; `XidSnapshot { xmin, xmax, xip }` and
    `sees(xid) -> bool` (32-bit xids are placed in the snapshot's epoch); `Chunk::visibility:
    Option<ChunkVisibility>`.
  - `InFlightTxn { xid, tables: Vec<u64> }`; `SourceConnector::in_flight() ->
    Result<Option<Vec<InFlightTxn>>, SourceError>`, default `Ok(None)`.
  - `SourceError::Contended(String)` (not transient: `is_transient()` is false; the worker
    retries it within `contended_window`, D-M6a.9).
  - `HaltReason::SinkRejected { detail }` (`SINK_REJECTED`) and `SnapshotTimeout { limit_secs }`
    (`SNAPSHOT_TIMEOUT`).
  - `TableChangeSet::snapshot: bool`; `validate` refuses a snapshot set with deletes, a truncate
    or a split; `CommitMarker::covers` returns `false` for a snapshot set.
  - `Serialize`/`Deserialize` for `TableSchema` (checked on load, like `TableSchema::new`),
    `ColumnSchema`, `ColumnValueType`, `HaltReason` and `LogicalType` (as its text); `TableSpec::new` accepts a partition on a lineage column
    (`day(_ts)`, config §9.3).

DA-SNAP-9 needs to know what a chunk's read saw: PostgreSQL gives it as the read's
`pg_current_snapshot()` (Task 2). Q-M6a-6 adds the snapshot flag: a chunk's rows are read now,
so a marker that covers the frontier says nothing about them. The worker caches each table's
schema in state (Task 3), hence serde.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-core/src/change_set.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-core/src/change_set.rs b/crates/data-avalanche-core/src/change_set.rs
--- a/crates/data-avalanche-core/src/change_set.rs
+++ b/crates/data-avalanche-core/src/change_set.rs
@@ -247,6 +247,7 @@ mod tests {
                 end: SourcePosition::Pg(Lsn(20)),
             },
             split: None,
+            snapshot: false,
         }
     }
 
@@ -319,6 +320,31 @@ mod tests {
         assert!(detail(c).contains("instead of its transaction start"));
     }
 
+    #[test]
+    fn a_snapshot_set_only_upserts() {
+        let mut c = cs(vec![upserts(&[1], None)], vec![]);
+        c.snapshot = true;
+        c.validate().unwrap();
+        let refused = |f: fn(&mut TableChangeSet)| {
+            let mut c = cs(vec![upserts(&[1], None)], vec![]);
+            c.snapshot = true;
+            f(&mut c);
+            detail(c)
+        };
+        for d in [
+            refused(|c| c.deletes = vec![deletes(&[Some(2)])]),
+            refused(|c| c.truncate = true),
+            refused(|c| {
+                c.split = Some(SplitInfo {
+                    txn_start: SourcePosition::Pg(Lsn(20)),
+                    slice: 1,
+                });
+            }),
+        ] {
+            assert!(d.contains("upserts only"), "{d}");
+        }
+    }
+
     #[test]
     fn the_range_must_not_run_backwards() {
         let mut c = cs(vec![], vec![]);
```

Change `crates/data-avalanche-core/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/lib.rs b/crates/data-avalanche-core/src/lib.rs
--- a/crates/data-avalanche-core/src/lib.rs
+++ b/crates/data-avalanche-core/src/lib.rs
@@ -37,9 +37,9 @@ pub use sink::{
     SinkCapabilities, SinkError, SinkKind, SinkTableState, TableSpec, TransformKind, WriteMode,
 };
 pub use source::{
-    ChangeStream, Chunk, KeysetCursor, Preflight, Prepared, RescanReport, ResolvedTables,
-    SchemaHint, SnapshotReason, SourceConnector, SourceError, StartPlan, StreamItem,
-    identifier_values,
+    ChangeStream, Chunk, ChunkVisibility, InFlightTxn, KeysetCursor, Preflight, Prepared,
+    RescanReport, ResolvedTables, SchemaHint, SnapshotReason, SourceConnector, SourceError,
+    StartPlan, StreamItem, XidSnapshot, identifier_values,
 };
 pub use table::{
     INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
```

Change `crates/data-avalanche-core/src/logical.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-core/src/logical.rs b/crates/data-avalanche-core/src/logical.rs
--- a/crates/data-avalanche-core/src/logical.rs
+++ b/crates/data-avalanche-core/src/logical.rs
@@ -244,4 +244,13 @@ mod tests {
         assert_eq!(err.input, "decimal(99,1)");
         assert!(err.to_string().contains("decimal(99,1)"));
     }
+    #[test]
+    fn serializes_as_its_text() {
+        let t: LogicalType = "list<decimal(10,2)>".parse().unwrap();
+        let json = serde_json::to_string(&t).unwrap();
+        assert_eq!(json, r#""list<decimal(10,2)>""#);
+        assert_eq!(serde_json::from_str::<LogicalType>(&json).unwrap(), t);
+        let e = serde_json::from_str::<LogicalType>(r#""timestamp""#).unwrap_err();
+        assert!(e.to_string().contains("invalid logical type"), "{e}");
+    }
 }
```

Change `crates/data-avalanche-core/src/schema.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-core/src/schema.rs b/crates/data-avalanche-core/src/schema.rs
--- a/crates/data-avalanche-core/src/schema.rs
+++ b/crates/data-avalanche-core/src/schema.rs
@@ -414,9 +414,8 @@ mod tests {
         ));
     }
 
-    #[test]
-    fn every_halt_reason_has_a_distinct_code() {
-        let all = [
+    fn every_halt_reason() -> Vec<HaltReason> {
+        vec![
             HaltReason::ReservedColumnCollision { column: "c".into() },
             HaltReason::TableDropped,
             HaltReason::UnsafeDdl { detail: "d".into() },
@@ -441,7 +440,14 @@ mod tests {
             },
             HaltReason::NullIdentifier { column: "c".into() },
             HaltReason::NotReplicable { reason: "r".into() },
-        ];
+            HaltReason::SinkRejected { detail: "d".into() },
+            HaltReason::SnapshotTimeout { limit_secs: 21_600 },
+        ]
+    }
+
+    #[test]
+    fn every_halt_reason_has_a_distinct_code() {
+        let all = every_halt_reason();
         let codes: std::collections::BTreeSet<_> = all.iter().map(HaltReason::code).collect();
         assert_eq!(codes.len(), all.len());
         assert!(
@@ -459,4 +465,92 @@ mod tests {
         assert_eq!(a.source_id(), Some(7));
         assert_ne!(a, s.with_source_id(8), "another table under the name");
     }
+    #[test]
+    fn halt_reasons_serialize_tagged_by_their_code() {
+        for r in every_halt_reason() {
+            let json = serde_json::to_value(&r).unwrap();
+            assert_eq!(json["code"], r.code(), "{r:?}");
+            assert_eq!(serde_json::from_value::<HaltReason>(json).unwrap(), r);
+        }
+        assert_eq!(
+            serde_json::to_string(&HaltReason::UnsafeDdl { detail: "x".into() }).unwrap(),
+            r#"{"code":"UNSAFE_DDL","detail":"x"}"#
+        );
+        assert_eq!(
+            serde_json::to_string(&HaltReason::TableDropped).unwrap(),
+            r#"{"code":"TABLE_DROPPED"}"#
+        );
+        let unknown =
+            serde_json::from_str::<HaltReason>(r#"{"code":"UNSAFE_DDL","detail":"x","y":1}"#);
+        // A unit variant's extra fields go unchecked: serde's internally tagged enums don't
+        // apply `deny_unknown_fields` to them.
+        assert!(unknown.is_err(), "unknown fields are refused");
+    }
+
+    #[test]
+    fn a_table_schema_round_trips_through_json() {
+        let mut c = cols();
+        c[0].source_ordinal = Some(1);
+        c.push(ColumnSchema::new(
+            "tags",
+            V::Array(Box::new(V::Text)),
+            "text[]",
+        ));
+        c.push(ColumnSchema::new(
+            "at",
+            V::Timestamp { with_tz: true },
+            "timestamp with time zone",
+        ));
+        let s = TableSchema::new(t(), c, vec!["id".into()])
+            .unwrap()
+            .with_source_id(16384);
+        let json = serde_json::to_value(&s).unwrap();
+        assert_eq!(
+            json["table"],
+            serde_json::json!({"schema": "public", "name": "orders"})
+        );
+        assert_eq!(json["identifier"], serde_json::json!(["id"]));
+        assert_eq!(json["source_id"], 16384);
+        assert_eq!(
+            json["columns"][0],
+            serde_json::json!({
+                "name": "id", "value_type": "int64", "logical": "int64", "nullable": false,
+                "source_type": "bigint", "source_ordinal": 1
+            })
+        );
+        assert_eq!(
+            json["columns"][1]["value_type"],
+            serde_json::json!({"numeric": {"precision": 10, "scale": 2}})
+        );
+        assert_eq!(json["columns"][1]["logical"], "decimal(10,2)");
+        assert_eq!(
+            json["columns"][3]["value_type"],
+            serde_json::json!({"array": "text"})
+        );
+        assert_eq!(
+            json["columns"][4]["value_type"],
+            serde_json::json!({"timestamp": {"with_tz": true}})
+        );
+        assert_eq!(serde_json::from_value::<TableSchema>(json).unwrap(), s);
+    }
+
+    #[test]
+    fn a_stored_schema_is_checked_like_a_new_one() {
+        let s = TableSchema::new(t(), cols(), vec!["id".into()]).unwrap();
+        let mut json = serde_json::to_value(&s).unwrap();
+        assert_eq!(json["source_id"], serde_json::Value::Null);
+        json["identifier"] = serde_json::json!(["note"]);
+        let e = serde_json::from_value::<TableSchema>(json.clone()).unwrap_err();
+        assert!(
+            e.to_string()
+                .contains("identifier column \"note\" can't be a key: it is nullable"),
+            "{e}"
+        );
+        json["identifier"] = serde_json::json!(["id"]);
+        json["columns"][0]["extra"] = serde_json::json!(1);
+        assert!(
+            serde_json::from_value::<TableSchema>(json).is_err(),
+            "unknown fields are refused"
+        );
+    }
 }
```

Change `crates/data-avalanche-core/src/sink.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -329,6 +329,7 @@ mod tests {
                 end: SourcePosition::Pg(Lsn(end)),
             },
             split,
+            snapshot: false,
         }
     }
 
@@ -384,6 +385,17 @@ mod tests {
         )
         .unwrap_err();
         assert_eq!(err.code(), "SCHEMA_MISMATCH");
+        let by_commit_time = TableSpec::new(
+            &table(),
+            vec![PartitionField {
+                column: "_ts".into(),
+                transform: PartitionTransform::Day,
+            }],
+        );
+        assert!(
+            by_commit_time.is_ok(),
+            "a lineage column is in the upsert schema (config §9.3: `day(_ts)`)"
+        );
     }
 
     #[test]
@@ -408,6 +420,17 @@ mod tests {
         assert!(!stored.covers(&change_set(101, None)).unwrap());
     }
 
+    #[test]
+    fn the_guard_never_skips_a_snapshot_set() {
+        let mut chunk = change_set(100, None);
+        chunk.snapshot = true;
+        assert!(
+            !marker(1, 100).covers(&chunk).unwrap(),
+            "chunk rows are read now, at a frontier the marker covers"
+        );
+        assert!(!marker(1, 500).covers(&chunk).unwrap());
+    }
+
     #[test]
     fn a_slice_is_skipped_only_once_its_transaction_has_landed() {
         // A slice of a transaction that started at 90 and commits at 110.
```

Change `crates/data-avalanche-core/src/source.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -410,6 +410,7 @@ mod tests {
             Ok(Chunk {
                 rows: vec![],
                 next: None,
+                visibility: None,
             })
         }
         async fn fetch_rows(&self, _: &TableSchema, _: &[Row]) -> Result<Vec<Row>, SourceError> {
@@ -497,6 +498,66 @@ mod tests {
         block_on(stream.acknowledge(&b.commit_pos)).unwrap();
         assert!(block_on(stream.next()).unwrap_err().is_transient());
         block_on(stream.close()).unwrap();
+        assert_eq!(
+            block_on(source.in_flight()).unwrap(),
+            None,
+            "a source without chunk visibility"
+        );
+    }
+
+    #[test]
+    fn a_snapshot_sees_the_transactions_completed_before_it() {
+        // `pg_current_snapshot()` = `100:105:100,103`.
+        let s = XidSnapshot {
+            xmin: 100,
+            xmax: 105,
+            xip: vec![100, 103],
+        };
+        for (xid, seen) in [
+            (99, true),
+            (100, false),
+            (101, true),
+            (103, false),
+            (104, true),
+            (105, false),
+            (106, false),
+        ] {
+            assert_eq!(s.sees(xid), seen, "{xid}");
+        }
+    }
+
+    #[test]
+    fn a_32_bit_xid_is_placed_next_to_the_snapshot() {
+        // Epoch 1: an xid8 is 2^32 + the 32-bit xid the stream sends.
+        let e = 1u64 << 32;
+        let s = XidSnapshot {
+            xmin: e + 10,
+            xmax: e + 20,
+            xip: vec![e + 15],
+        };
+        assert!(s.sees(5), "epoch 1's 5 is below xmin");
+        assert!(!s.sees(15));
+        assert!(!s.sees(25));
+        assert!(!s.sees(e + 15), "a full xid is taken as it is");
+        assert!(s.sees(e + 16));
+        // Just past the wraparound: the stream's 0xFFFF_FFF0 is epoch 0's.
+        let w = XidSnapshot {
+            xmin: e + 1,
+            xmax: e + 3,
+            xip: vec![],
+        };
+        assert!(w.sees(0xFFFF_FFF0));
+        assert!(!w.sees(3));
+    }
+
+    #[test]
+    fn contended_is_retried_by_the_pipeline_not_as_transient() {
+        let e = SourceError::Contended("replication slot \"s\" is active".into());
+        assert!(!e.is_transient());
+        assert_eq!(
+            e.to_string(),
+            "the source's stream position is held by another reader: replication slot \"s\" is active"
+        );
     }
 
     /// Polls a future that never waits (the test connector's methods are all ready at once).
```

Change `crates/data-avalanche-sink-delta/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/test_support.rs b/crates/data-avalanche-sink-delta/src/test_support.rs
--- a/crates/data-avalanche-sink-delta/src/test_support.rs
+++ b/crates/data-avalanche-sink-delta/src/test_support.rs
@@ -110,6 +110,7 @@ pub(crate) fn change_set(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>)
             end: SourcePosition::Pg(Lsn(end)),
         },
         split: None,
+        snapshot: false,
     }
 }
 
```

Change `crates/data-avalanche-sink-delta/tests/sink/common.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/common.rs b/crates/data-avalanche-sink-delta/tests/sink/common.rs
--- a/crates/data-avalanche-sink-delta/tests/sink/common.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/common.rs
@@ -90,6 +90,7 @@ pub fn cs(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeS
             end: SourcePosition::Pg(Lsn(end)),
         },
         split: None,
+        snapshot: false,
     }
 }
 
```

Change `crates/data-avalanche-testkit/src/memory_sink.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -417,6 +417,7 @@ mod tests {
                 end: SourcePosition::Pg(Lsn(end)),
             },
             split: None,
+            snapshot: false,
         }
     }
 
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-core --lib
```

Expected:

FAIL to compile (16 errors); the first:

```text
error[E0432]: unresolved imports `source::ChunkVisibility`, `source::InFlightTxn`, `source::XidSnapshot`
error[E0422]: cannot find struct, variant or union type `XidSnapshot` in this scope
error[E0560]: struct `change_set::TableChangeSet` has no field named `snapshot`
error[E0609]: no field `snapshot` on type `change_set::TableChangeSet`
error[E0277]: the trait bound `logical::LogicalType: serde::Serialize` is not satisfied
error[E0277]: the trait bound `logical::LogicalType: serde::Deserialize<'de>` is not satisfied
… and 10 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/change_set.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/change_set.rs b/crates/data-avalanche-core/src/change_set.rs
--- a/crates/data-avalanche-core/src/change_set.rs
+++ b/crates/data-avalanche-core/src/change_set.rs
@@ -40,7 +40,8 @@ pub struct SplitInfo {
 /// - every delete batch starts with the identifier columns;
 /// - a key appears at most once across `upserts` and `deletes` (a PK-changing UPDATE is two keys);
 /// - identifier values are never NULL;
-/// - the range doesn't run backwards, and a slice's range ends at its `txn_start`.
+/// - the range doesn't run backwards, and a slice's range ends at its `txn_start`;
+/// - a snapshot set only upserts: no deletes, no truncate, not a slice.
 #[derive(Debug, Clone)]
 pub struct TableChangeSet {
     pub table: TableRef,
@@ -53,6 +54,10 @@ pub struct TableChangeSet {
     pub deletes: Vec<RecordBatch>,
     pub position: PositionRange,
     pub split: Option<SplitInfo>,
+    /// Snapshot chunk rows, read from the source now rather than replayed from the stream: the
+    /// position guard never skips them ([`crate::CommitMarker::covers`]). A chunk is emitted at
+    /// the stream's frontier, which the table's marker may already cover (Q-M6a-6).
+    pub snapshot: bool,
 }
 
 #[derive(Debug, Clone, PartialEq, Eq, Error)]
@@ -146,6 +151,11 @@ impl TableChangeSet {
                 )));
             }
         }
+        if self.snapshot && (self.truncate || self.split.is_some() || self.delete_rows() > 0) {
+            return Err(err(
+                "a snapshot set carries upserts only: no deletes, truncate or slice".into(),
+            ));
+        }
         if let Some(split) = &self.split
             && self.position.end != split.txn_start
         {
```

Change `crates/data-avalanche-core/src/logical.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/logical.rs b/crates/data-avalanche-core/src/logical.rs
--- a/crates/data-avalanche-core/src/logical.rs
+++ b/crates/data-avalanche-core/src/logical.rs
@@ -1,6 +1,7 @@
 use std::fmt;
 use std::str::FromStr;
 
+use serde::{Deserialize, Deserializer, Serialize, Serializer};
 use thiserror::Error;
 
 use crate::MAX_DECIMAL_PRECISION;
@@ -139,6 +140,21 @@ impl fmt::Display for LogicalType {
     }
 }
 
+/// Stored as its text (`"decimal(10,2)"`, `"list<int64>"`).
+impl Serialize for LogicalType {
+    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
+        serializer.collect_str(self)
+    }
+}
+
+impl<'de> Deserialize<'de> for LogicalType {
+    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
+        String::deserialize(deserializer)?
+            .parse()
+            .map_err(serde::de::Error::custom)
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

Change `crates/data-avalanche-core/src/schema.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/schema.rs b/crates/data-avalanche-core/src/schema.rs
--- a/crates/data-avalanche-core/src/schema.rs
+++ b/crates/data-avalanche-core/src/schema.rs
@@ -1,10 +1,12 @@
+use serde::{Deserialize, Serialize};
 use thiserror::Error;
 
 use crate::{ColumnValueType, LogicalType, TableRef, first_reserved_collision};
 
 /// One source column (`Schema_Change…` §7): what the decoder produces (`value_type`), what the
 /// target stores (`logical`, Hop 2), and the source's own description.
-#[derive(Debug, Clone, PartialEq, Eq)]
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
 pub struct ColumnSchema {
     pub name: String,
     pub value_type: ColumnValueType,
@@ -84,41 +86,67 @@ pub fn hop2_compatible(value_type: &ColumnValueType, logical: &LogicalType) -> b
 
 /// Why a table stops replicating while the other tables of its pipeline continue (`Schema_Change…`
 /// §7.2, config §5, `Type_Mapping.md`). [`HaltReason::code`] is the stable identifier for status,
-/// logs and metrics.
-#[derive(Debug, Clone, PartialEq, Eq, Error)]
+/// logs and metrics; the pipeline's state stores a halt tagged by it (`{"code": "UNSAFE_DDL",
+/// "detail": …}`).
+#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
+#[serde(tag = "code", deny_unknown_fields)]
 pub enum HaltReason {
     #[error(
         "column {column:?} collides with a reserved lineage or internal column name; rename it at the source"
     )]
+    #[serde(rename = "RESERVED_COLUMN_COLLISION")]
     ReservedColumnCollision { column: String },
     #[error(
         "the table was dropped at the source; the target is kept, and a re-created table needs an operator re-snapshot"
     )]
+    #[serde(rename = "TABLE_DROPPED")]
     TableDropped,
     #[error("unsafe schema change: {detail}")]
+    #[serde(rename = "UNSAFE_DDL")]
     UnsafeDdl { detail: String },
     #[error("the table has no primary key and no tables.primary_key override")]
+    #[serde(rename = "NO_IDENTIFIER")]
     NoIdentifier,
     #[error("identifier column {column:?} can't be a key: {reason}")]
+    #[serde(rename = "IDENTIFIER_NOT_USABLE")]
     IdentifierNotUsable { column: String, reason: String },
     #[error("identifier columns {missing:?} are not in the table's REPLICA IDENTITY")]
+    #[serde(rename = "IDENTIFIER_NOT_IN_REPLICA_IDENTITY")]
     IdentifierNotInReplicaIdentity { missing: Vec<String> },
     #[error("the sink can't apply this schema change: {detail}")]
+    #[serde(rename = "EVOLUTION_UNSUPPORTED")]
     EvolutionUnsupported { detail: String },
     #[error("the schema changed and schema_evolution.enabled is false")]
+    #[serde(rename = "EVOLUTION_DISABLED")]
     EvolutionDisabled,
     #[error("a value in column {column:?} doesn't fit its decimal type")]
+    #[serde(rename = "NUMERIC_OVERFLOW")]
     NumericOverflow { column: String },
     #[error(
         "column {column:?} holds the invalid date {raw:?}; set tables.column_type_overrides: {{{column}: string}} or fix the source rows"
     )]
+    #[serde(rename = "INVALID_DATE_VALUE")]
     InvalidDateValue { column: String, raw: String },
     #[error("column {column:?}: {detail}")]
+    #[serde(rename = "SCHEMA_MISMATCH")]
     SchemaMismatch { column: String, detail: String },
     #[error("identifier column {column:?} is NULL")]
+    #[serde(rename = "NULL_IDENTIFIER")]
     NullIdentifier { column: String },
     #[error("the source can't replicate this table: {reason}")]
+    #[serde(rename = "TABLE_NOT_REPLICABLE")]
     NotReplicable { reason: String },
+    /// The sink refused a change set for good (a `Permanent` commit error): retrying would fail
+    /// the same way (a key outside the Delta range, Q-M5-19).
+    #[error("the sink refused the table's changes: {detail}")]
+    #[serde(rename = "SINK_REJECTED")]
+    SinkRejected { detail: String },
+    /// The table's snapshot ran longer than `source.timeouts.snapshot` in one run.
+    #[error(
+        "the table's snapshot didn't finish within timeouts.snapshot ({limit_secs} s); raise it and re-snapshot the table"
+    )]
+    #[serde(rename = "SNAPSHOT_TIMEOUT")]
+    SnapshotTimeout { limit_secs: u64 },
 }
 
 impl HaltReason {
@@ -137,13 +165,18 @@ impl HaltReason {
             Self::SchemaMismatch { .. } => "SCHEMA_MISMATCH",
             Self::NullIdentifier { .. } => "NULL_IDENTIFIER",
             Self::NotReplicable { .. } => "TABLE_NOT_REPLICABLE",
+            Self::SinkRejected { .. } => "SINK_REJECTED",
+            Self::SnapshotTimeout { .. } => "SNAPSHOT_TIMEOUT",
         }
     }
 }
 
 /// A source table's schema as of one stream position: its columns in source order and the
-/// identifier (inferred PK or `tables.primary_key`) that keys upserts and deletes (G-13).
-#[derive(Debug, Clone, PartialEq, Eq)]
+/// identifier (inferred PK or `tables.primary_key`) that keys upserts and deletes (G-13). The
+/// pipeline's schema cache stores it as JSON; a stored schema is checked like a new one
+/// ([`TableSchema::new`]).
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(try_from = "StoredTableSchema", into = "StoredTableSchema")]
 pub struct TableSchema {
     table: TableRef,
     columns: Vec<ColumnSchema>,
@@ -151,6 +184,39 @@ pub struct TableSchema {
     source_id: Option<u64>,
 }
 
+/// [`TableSchema`]'s stored form.
+#[derive(Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+struct StoredTableSchema {
+    table: TableRef,
+    columns: Vec<ColumnSchema>,
+    identifier: Vec<String>,
+    source_id: Option<u64>,
+}
+
+impl From<TableSchema> for StoredTableSchema {
+    fn from(s: TableSchema) -> Self {
+        Self {
+            table: s.table,
+            columns: s.columns,
+            identifier: s.identifier,
+            source_id: s.source_id,
+        }
+    }
+}
+
+impl TryFrom<StoredTableSchema> for TableSchema {
+    type Error = HaltReason;
+
+    fn try_from(s: StoredTableSchema) -> Result<Self, HaltReason> {
+        let schema = TableSchema::new(s.table, s.columns, s.identifier)?;
+        Ok(match s.source_id {
+            Some(id) => schema.with_source_id(id),
+            None => schema,
+        })
+    }
+}
+
 impl TableSchema {
     /// Checks the table can be replicated, in this order: no reserved column name (config §5), a
     /// non-empty identifier (Ghost_Row §3), identifier columns listed once that exist and are NOT
```

Change `crates/data-avalanche-core/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -10,8 +10,8 @@ use async_trait::async_trait;
 use thiserror::Error;
 
 use crate::{
-    ChangeSetError, HaltReason, PositionError, SchemaDelta, SourcePosition, TableChangeSet,
-    TableRef, TableSchema, upsert_schema,
+    ChangeSetError, HaltReason, LINEAGE_COLUMNS, PositionError, SchemaDelta, SourcePosition,
+    TableChangeSet, TableRef, TableSchema, upsert_schema,
 };
 
 /// The table format a sink writes (`sink.type`). The text is what pipeline state records as
@@ -124,7 +124,8 @@ impl SinkCapabilities {
     }
 }
 
-/// What `ensure_table` creates when the table is absent.
+/// What `ensure_table` creates when the table is absent. A partition column is a source column or
+/// a lineage column (`_ts`, config §9.3).
 #[derive(Debug, Clone, PartialEq)]
 pub struct TableSpec {
     pub table: TableRef,
@@ -139,10 +140,9 @@ impl TableSpec {
         schema: &TableSchema,
         partition_by: Vec<PartitionField>,
     ) -> Result<Self, HaltReason> {
-        if let Some(p) = partition_by
-            .iter()
-            .find(|p| schema.column(&p.column).is_none())
-        {
+        if let Some(p) = partition_by.iter().find(|p| {
+            schema.column(&p.column).is_none() && !LINEAGE_COLUMNS.contains(&p.column.as_str())
+        }) {
             return Err(HaltReason::SchemaMismatch {
                 column: p.column.clone(),
                 detail: "a partition column that isn't in the table".into(),
@@ -181,7 +181,13 @@ impl CommitMarker {
     /// It is skipped only when the marker is strictly past `txn_start`: the transaction's commit (or
     /// a later one) has landed, and re-applying the slice would roll its rows back to their state
     /// inside the transaction.
+    ///
+    /// A snapshot set is never skipped: its rows are read now, not replayed history, and its range
+    /// is the stream's frontier, which the marker may cover already.
     pub fn covers(&self, cs: &TableChangeSet) -> Result<bool, PositionError> {
+        if cs.snapshot {
+            return Ok(false);
+        }
         let stored = &self.committed_position;
         match &cs.split {
             Some(split) => Ok(stored.covers(&split.txn_start)? && !split.txn_start.covers(stored)?),
```

Change `crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -28,6 +28,12 @@ pub enum SourceError {
     /// One table can't be replicated; the pipeline's other tables continue.
     #[error("table {table}: {reason}")]
     TableHalt { table: TableRef, reason: HaltReason },
+    /// Another reader holds the stream position (PostgreSQL: the slot is active for another
+    /// walsender, SQLSTATE 55006). A quick restart meets it until the old session's walsender
+    /// times out (`wal_sender_timeout`): the pipeline retries for a bounded time while it holds
+    /// the lease, then gives up (`Postgres_Slot…` D-15.5).
+    #[error("the source's stream position is held by another reader: {0}")]
+    Contended(String),
 }
 
 impl SourceError {
@@ -117,6 +123,66 @@ pub struct Chunk {
     pub rows: Vec<Row>,
     /// The cursor after the last row; `None` when the chunk is empty (the table is done).
     pub next: Option<KeysetCursor>,
+    /// Which source transactions the rows reflect, so the pipeline can order them against the
+    /// stream (DA-SNAP-9). `None`: the source can't say, and the pipeline falls back to the
+    /// watermark window.
+    pub visibility: Option<ChunkVisibility>,
+}
+
+/// What a chunk's rows reflect.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub enum ChunkVisibility {
+    /// PostgreSQL: the MVCC snapshot the chunk was read in (`pg_current_snapshot()` inside its
+    /// REPEATABLE READ transaction). A transaction whose commit the stream already delivered can
+    /// still be invisible to it: a synchronous commit waiting for its standby is decoded before
+    /// it is visible (measured on PostgreSQL 18).
+    Xids(XidSnapshot),
+}
+
+/// A PostgreSQL snapshot (`pg_snapshot`): full 64-bit transaction ids (`xid8`).
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct XidSnapshot {
+    /// The oldest transaction still in progress; every one before it has completed.
+    pub xmin: u64,
+    /// One past the newest completed transaction; it and every later one hadn't completed.
+    pub xmax: u64,
+    /// The transactions in `xmin..xmax` still in progress.
+    pub xip: Vec<u64>,
+}
+
+impl XidSnapshot {
+    /// True when transaction `xid` had completed (committed or aborted) when the snapshot was
+    /// taken: a committed one's changes are in the rows. `xid` may be a full id or the 32-bit
+    /// one the stream carries, which is placed in the epoch that puts it nearest to `xmax`.
+    pub fn sees(&self, xid: u64) -> bool {
+        let xid = self.full(xid);
+        xid < self.xmin || (xid < self.xmax && !self.xip.contains(&xid))
+    }
+
+    fn full(&self, xid: u64) -> u64 {
+        const EPOCH: u64 = 1 << 32;
+        const HALF: u64 = 1 << 31;
+        if xid >= EPOCH {
+            return xid;
+        }
+        let candidate = (self.xmax & !(EPOCH - 1)) | xid;
+        if candidate > self.xmax.saturating_add(HALF) && candidate >= EPOCH {
+            candidate - EPOCH
+        } else if candidate.saturating_add(HALF) < self.xmax {
+            candidate + EPOCH
+        } else {
+            candidate
+        }
+    }
+}
+
+/// A transaction in progress at the source ([`SourceConnector::in_flight`]).
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct InFlightTxn {
+    /// Its id, as [`StreamItem::Begin`] carries it.
+    pub xid: u64,
+    /// The tables it holds a write lock on, by [`TableSchema::source_id`].
+    pub tables: Vec<u64>,
 }
 
 /// What an in-band schema signal carries (`Schema_Change…` §3 channel B). It says when and where;
@@ -168,8 +234,9 @@ pub enum StreamItem {
 pub struct RescanReport {
     /// Tables gone from the source catalog: each halts with `TABLE_DROPPED`.
     pub dropped: Vec<TableRef>,
-    /// Tables whose catalog schema differs from the known one: the current schemas. Freshness and
-    /// alerting only; the stream's signals apply changes in order.
+    /// Tables whose catalog schema differs from the known one: the current schemas. The stream's
+    /// signals apply changes in order; the pipeline applies one of these only once the stream has
+    /// passed the position after the rescan and no signal did it (an idle table).
     pub changed: Vec<TableSchema>,
     /// Tables that can't be described any more (identifier lost, …).
     pub halted: Vec<(TableRef, HaltReason)>,
@@ -203,9 +270,19 @@ pub trait SourceConnector: Send + Sync {
     /// nothing for it.
     async fn retained_bytes(&self) -> Result<Option<u64>, SourceError>;
 
-    /// The source's current position: a snapshot chunk's low and high watermarks.
+    /// The source's current position: a snapshot chunk's low and high watermarks, and the point
+    /// the stream must pass before a rescan's change is applied.
     async fn current_position(&self) -> Result<SourcePosition, SourceError>;
 
+    /// The transactions in progress now, each with the tables it writes (DA-SNAP-9). Read when a
+    /// stream session opens: one of them may have committed before the session and be applied
+    /// to the sink already while still invisible to a chunk (a synchronous commit waiting for its
+    /// standby), so a table's chunks wait until none of them writes it. `None` for a source whose
+    /// chunks carry no [`ChunkVisibility`].
+    async fn in_flight(&self) -> Result<Option<Vec<InFlightTxn>>, SourceError> {
+        Ok(None)
+    }
+
     /// Up to `limit` rows of `table` after `after`, in identifier order.
     async fn read_chunk(
         &self,
```

Change `crates/data-avalanche-core/src/value.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/value.rs b/crates/data-avalanche-core/src/value.rs
--- a/crates/data-avalanche-core/src/value.rs
+++ b/crates/data-avalanche-core/src/value.rs
@@ -1,9 +1,13 @@
+use serde::{Deserialize, Serialize};
 use uuid::Uuid;
 
 use crate::Decimal;
 
-/// The type of a column as decoded from the source (Hop 1 output, `Type_Mapping.md`).
-#[derive(Debug, Clone, PartialEq, Eq, Hash)]
+/// The type of a column as decoded from the source (Hop 1 output, `Type_Mapping.md`). Stored in
+/// the pipeline's schema cache as `"int64"`, `{"numeric": {"precision": 10, "scale": 2}}`,
+/// `{"array": "text"}`.
+#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
+#[serde(rename_all = "snake_case")]
 pub enum ColumnValueType {
     Bool,
     Int16,
```

Change `crates/data-avalanche-pipeline/src/buffer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/buffer.rs b/crates/data-avalanche-pipeline/src/buffer.rs
--- a/crates/data-avalanche-pipeline/src/buffer.rs
+++ b/crates/data-avalanche-pipeline/src/buffer.rs
@@ -549,6 +549,7 @@ impl TableBuffer {
             deletes,
             position,
             split,
+            snapshot: false,
         }
     }
 }
```

Change `crates/data-avalanche-source-pg/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/snapshot.rs b/crates/data-avalanche-source-pg/src/snapshot.rs
--- a/crates/data-avalanche-source-pg/src/snapshot.rs
+++ b/crates/data-avalanche-source-pg/src/snapshot.rs
@@ -428,7 +428,11 @@ impl PgConnector {
                 collation,
             }),
         };
-        Ok(Chunk { rows, next })
+        Ok(Chunk {
+            rows,
+            next,
+            visibility: None,
+        })
     }
 
     /// The current rows of these keys (`toast_strategy: re_query`, Q-M5-6), [`FETCH_BATCH`] keys
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-core --lib
```

Expected:

PASS:

```text
test result: ok. 147 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(core): chunk visibility, in-flight transactions, schema serde, contended and new halt codes"
```


### Task 2: PostgreSQL: a chunk's snapshot, writers in flight, a busy slot

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/snapshot.rs`, `…/connector.rs`, `…/error.rs`
- Modify: `crates/data-avalanche-source-pg/tests/pg/snapshot.rs`, `…/tests/pg/stream.rs`
- Create: `crates/data-avalanche-source-pg/tests/sync_commit.rs`

**Interfaces:**
- Consumes: Task 1's `ChunkVisibility`, `XidSnapshot`, `InFlightTxn`, `SourceError::Contended`.
- Produces: `PgConnector::read_chunk` fills `visibility` with the read's own
  `pg_current_snapshot()`, taken in the chunk's REPEATABLE READ transaction before the rows;
  `PgConnector::in_flight()` lists every transaction that holds its transaction-id lock, with
  the tables it holds a `RowExclusiveLock` or stronger on (`pg_locks`), and returns `Some`; SQLSTATE `55006` (object in use) maps to
  `Contended`.

S-PG-style regressions on the live server: a chunk's snapshot doesn't see a writer still in
flight, and sees it once it commits. `tests/sync_commit` reproduces DA-SNAP-9 at the source
level. A synchronous commit waiting for a standby that never answers is already in the stream,
while a chunk's snapshot doesn't see it and `in_flight()` lists it. It is its own test binary
because it changes `synchronous_standby_names` server-wide (restored on every path).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-source-pg/src/error.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-source-pg/src/error.rs b/crates/data-avalanche-source-pg/src/error.rs
--- a/crates/data-avalanche-source-pg/src/error.rs
+++ b/crates/data-avalanche-source-pg/src/error.rs
@@ -131,12 +131,12 @@ mod tests {
     }
 
     #[test]
-    fn a_busy_slot_is_fatal() {
+    fn a_busy_slot_is_contended() {
         let e = replication_error(
             server("replication slot \"s\" is active for PID 42 (SQLSTATE 55006)"),
             None,
         );
-        assert!(matches!(e, SourceError::Permanent(m) if m.contains("active for PID")));
+        assert!(matches!(e, SourceError::Contended(m) if m.contains("active for PID")));
     }
 
     #[test]
```

Change `crates/data-avalanche-source-pg/src/snapshot.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-source-pg/src/snapshot.rs b/crates/data-avalanche-source-pg/src/snapshot.rs
--- a/crates/data-avalanche-source-pg/src/snapshot.rs
+++ b/crates/data-avalanche-source-pg/src/snapshot.rs
@@ -499,6 +499,29 @@ mod tests {
         }
     }
 
+    #[test]
+    fn snapshots_parse_from_their_text() {
+        assert_eq!(
+            parse_snapshot("735:740:736,738").unwrap(),
+            XidSnapshot {
+                xmin: 735,
+                xmax: 740,
+                xip: vec![736, 738]
+            }
+        );
+        assert_eq!(
+            parse_snapshot("4294967306:4294967306:").unwrap(),
+            XidSnapshot {
+                xmin: 4_294_967_306,
+                xmax: 4_294_967_306,
+                xip: vec![]
+            }
+        );
+        for bad in ["", "1:2", "a:2:", "1:2:x", "1:2:3,"] {
+            assert!(parse_snapshot(bad).is_err(), "{bad:?}");
+        }
+    }
+
     #[test]
     fn keys_compare_in_one_deterministic_collation() {
         let own = || Some(KeyCollation::Own("en_US.utf8".into()));
```

Change `crates/data-avalanche-source-pg/tests/pg/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/snapshot.rs b/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
--- a/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
@@ -3,13 +3,72 @@
 use std::time::Duration;
 
 use data_avalanche_core::{
-    ColumnValue as V, HaltReason, KeysetCursor, Row, SourceError, TableSchema, identifier_values,
+    ChunkVisibility, ColumnValue as V, HaltReason, KeysetCursor, Row, SourceError, TableSchema,
+    identifier_values,
 };
 use data_avalanche_source_pg::{FETCH_BATCH, PgConnector};
 use data_avalanche_testkit::require_it;
 
 use crate::common::{admin, cleanup, connector, fresh, pipeline, resolved};
 
+/// DA-SNAP-9: a chunk is read in one REPEATABLE READ transaction with its snapshot, which doesn't
+/// see a transaction still open; `in_flight` lists that transaction with the table it writes.
+/// Once it commits, the next chunk sees it and its row.
+#[tokio::test]
+async fn a_chunk_carries_its_snapshot_and_in_flight_lists_open_writers() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(
+        &a,
+        "snapshot_visibility",
+        "CREATE TABLE t (id int PRIMARY KEY, v int NOT NULL);
+         INSERT INTO t VALUES (1, 0), (2, 0);",
+    )
+    .await;
+    let c = connector(&pipeline(&n, &["t"], "", ""));
+    let r = resolved(&c).await;
+    let t = &r.tables[0];
+    let oid = t.source_id().expect("PostgreSQL tables carry their OID");
+    let w = admin().await;
+    w.batch_execute(&format!(
+        "BEGIN; UPDATE {}.t SET v = 1 WHERE id = 1",
+        n.schema
+    ))
+    .await
+    .unwrap();
+    let xid: String = w
+        .query_one("SELECT pg_current_xact_id()::text", &[])
+        .await
+        .unwrap()
+        .get(0);
+    let full: u64 = xid.parse().unwrap();
+    let short = full & 0xFFFF_FFFF;
+
+    let open = c.in_flight().await.unwrap().expect("PostgreSQL lists them");
+    let writer = open
+        .iter()
+        .find(|x| x.xid == short)
+        .unwrap_or_else(|| panic!("{short} in flight: {open:?}"));
+    assert!(writer.tables.contains(&oid), "{writer:?} writes {oid}");
+    let before = c.read_chunk(t, None, 10).await.unwrap();
+    let Some(ChunkVisibility::Xids(s)) = &before.visibility else {
+        panic!("a PostgreSQL chunk carries its snapshot: {before:?}")
+    };
+    assert!(!s.sees(short) && !s.sees(full), "{s:?} doesn't see {full}");
+    assert_eq!(before.rows[0].get("v"), Some(&V::Int32(0)));
+
+    w.batch_execute("COMMIT").await.unwrap();
+    let after = c.read_chunk(t, None, 10).await.unwrap();
+    let Some(ChunkVisibility::Xids(s)) = &after.visibility else {
+        panic!("{after:?}")
+    };
+    assert!(s.sees(short), "{s:?} sees the committed {short}");
+    assert_eq!(after.rows[0].get("v"), Some(&V::Int32(1)));
+    let open = c.in_flight().await.unwrap().unwrap();
+    assert!(open.iter().all(|x| x.xid != short), "{open:?}");
+    cleanup(&a, &n).await;
+}
+
 /// Final review I3: a table dropped under the snapshot halts that table (`TABLE_DROPPED`), and so
 /// does a key column that lost NOT NULL (`IDENTIFIER_NOT_USABLE`): a NULL key sorts after every
 /// cursor, and its rows would be skipped. Neither stops the pipeline.
```

Change `crates/data-avalanche-source-pg/tests/pg/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/stream.rs b/crates/data-avalanche-source-pg/tests/pg/stream.rs
--- a/crates/data-avalanche-source-pg/tests/pg/stream.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/stream.rs
@@ -613,7 +613,7 @@ async fn an_undecodable_value_halts_only_its_table() {
     cleanup(&a, &n).await;
 }
 
-/// Tables the stream wasn't opened for are skipped; a busy slot is a permanent error; an idle
+/// Tables the stream wasn't opened for are skipped; a busy slot is contended; an idle
 /// stream reports the WAL end in keep-alives.
 #[tokio::test(flavor = "multi_thread")]
 async fn untracked_tables_busy_slots_and_keepalives() {
@@ -662,8 +662,8 @@ async fn untracked_tables_busy_slots_and_keepalives() {
 
     let mut busy = c.open_stream(&from, &only_a).await.unwrap();
     match busy.next().await {
-        Err(SourceError::Permanent(m)) => assert!(m.contains("is active for PID"), "{m}"),
-        other => panic!("a busy slot is permanent: {other:?}"),
+        Err(SourceError::Contended(m)) => assert!(m.contains("is active for PID"), "{m}"),
+        other => panic!("a busy slot is contended: {other:?}"),
     }
 
     let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
```

Create `crates/data-avalanche-source-pg/tests/sync_commit.rs`:

```rust
//! DA-SNAP-9 on a live server: a synchronous commit waiting for its standby is already in the
//! stream while a chunk's snapshot doesn't see it. Its own test binary, which cargo never runs
//! beside the `pg` tests: it sets the server-wide `synchronous_standby_names` to a standby that
//! never answers, which stalls every synchronous commit on the server.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "pg/common.rs"]
mod common;

use std::time::Duration;

use data_avalanche_core::{ChangeStream, ChunkVisibility, ColumnValue as V, StreamItem};
use data_avalanche_testkit::require_it;
use tokio_postgres::NoTls;

use crate::common::{admin, cleanup, connector, first_stream, fresh, pipeline, port};

type Fallible<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn a_stalled_synchronous_commit_is_streamed_before_a_chunk_sees_it() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "sync_commit",
        "CREATE TABLE t (id int PRIMARY KEY, v int NOT NULL);
         INSERT INTO t VALUES (1, 0);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let (mut stream, tables, _) = first_stream(&c).await;
    let t = tables[0].clone();
    let oid = t.source_id().unwrap();

    // Every step is fallible, not panicking, so the setting is restored whatever happens; the
    // first reset undoes an earlier run that died before its own. ALTER SYSTEM can't run in a
    // multi-statement batch, hence one statement per call. Clearing the setting also releases a
    // writer still waiting.
    let standby = |names: Option<&'static str>| {
        let a = &a;
        async move {
            let set = match names {
                None => "ALTER SYSTEM RESET synchronous_standby_names".to_string(),
                Some(n) => format!("ALTER SYSTEM SET synchronous_standby_names = '{n}'"),
            };
            a.batch_execute(&set).await?;
            a.batch_execute("SELECT pg_reload_conf()").await
        }
    };
    standby(None).await.unwrap();
    let run: Fallible<_> = async {
        standby(Some("phantom_standby")).await?;
        let dsn = format!(
            "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
            port()
        );
        let (w, conn) = tokio_postgres::connect(&dsn, NoTls).await?;
        tokio::spawn(conn);
        let update = format!(
            "SET synchronous_commit = on; UPDATE {}.t SET v = 1 WHERE id = 1",
            n.schema
        );
        let writer = tokio::spawn(async move { w.batch_execute(&update).await });
        // The writer's commit is in the WAL and waits for the standby.
        let mut xid = None;
        for _ in 0..100 {
            let row = a
                .query_opt(
                    "SELECT backend_xid::text FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
                    &[],
                )
                .await?;
            if let Some(r) = row {
                xid = r.get::<_, Option<String>>(0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let xid: u64 = xid.ok_or("no writer waiting for the standby")?.parse()?;
        // The stream delivers its commit already.
        let mut committed = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while committed.is_none() {
            match tokio::time::timeout_at(deadline, stream.next()).await?? {
                StreamItem::Commit(b) if b.xid == xid => committed = Some(b),
                _ => {}
            }
        }
        let chunk = c.read_chunk(&t, None, 10).await?;
        let in_flight = c.in_flight().await?.ok_or("in_flight is None")?;
        a.batch_execute(
            "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
        )
        .await?;
        writer.await??;
        let after = c.read_chunk(&t, None, 10).await?;
        Ok((xid, chunk, in_flight, after))
    }
    .await;
    standby(None).await.unwrap();
    let (xid, chunk, in_flight, after) = run.unwrap();

    let Some(ChunkVisibility::Xids(s)) = &chunk.visibility else {
        panic!("{chunk:?}")
    };
    assert!(!s.sees(xid), "{s:?} doesn't see the stalled {xid}");
    assert_eq!(chunk.rows[0].get("v"), Some(&V::Int32(0)), "the old row");
    let stalled = in_flight.iter().find(|x| x.xid == xid);
    assert!(
        stalled.is_some_and(|x| x.tables.contains(&oid)),
        "{in_flight:?} lists {xid} writing {oid}"
    );
    let Some(ChunkVisibility::Xids(s)) = &after.visibility else {
        panic!("{after:?}")
    };
    assert!(s.sees(xid));
    assert_eq!(after.rows[0].get("v"), Some(&V::Int32(1)));
    stream.close().await.unwrap();
    cleanup(&a, &n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-source-pg --lib
```

Expected:

FAIL to compile (2 errors); the first:

```text
error[E0422]: cannot find struct, variant or union type `XidSnapshot` in this scope
error[E0425]: cannot find function `parse_snapshot` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -7,7 +7,7 @@ use data_avalanche_config::{
     Pipeline, Postgres, SourceType, check_table_set, check_unbounded_numeric,
 };
 use data_avalanche_core::{
-    ChangeStream, Chunk, HaltReason, KeysetCursor, Preflight, Prepared, RescanReport,
+    ChangeStream, Chunk, HaltReason, InFlightTxn, KeysetCursor, Preflight, Prepared, RescanReport,
     ResolvedTables, Row, SchemaDelta, SchemaHint, SourceConnector, SourceError, SourcePosition,
     TableRef, TableSchema,
 };
@@ -282,6 +282,10 @@ impl SourceConnector for PgConnector {
         PgConnector::current_position(self).await
     }
 
+    async fn in_flight(&self) -> Result<Option<Vec<InFlightTxn>>, SourceError> {
+        PgConnector::in_flight(self).await
+    }
+
     async fn read_chunk(
         &self,
         table: &TableSchema,
```

Change `crates/data-avalanche-source-pg/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/error.rs b/crates/data-avalanche-source-pg/src/error.rs
--- a/crates/data-avalanche-source-pg/src/error.rs
+++ b/crates/data-avalanche-source-pg/src/error.rs
@@ -33,9 +33,9 @@ fn classify(code: Option<&str>, message: String, position: Option<String>) -> So
             position: position.unwrap_or_default(),
             detail: message,
         },
-        // `replication slot "…" is active for PID …` (55006): another holder exists. Fatal, never
-        // retried into a busy loop (Postgres_Slot… D-15.5).
-        Some("55006") => SourceError::Permanent(message),
+        // `replication slot "…" is active for PID …` (55006): another holder exists. The pipeline
+        // retries it for a bounded time, never into a busy loop (Postgres_Slot… D-15.5).
+        Some("55006") => SourceError::Contended(message),
         Some(c) if transient_state(c) => SourceError::Transient(message),
         _ => SourceError::Permanent(message),
     }
```

Change `crates/data-avalanche-source-pg/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/snapshot.rs b/crates/data-avalanche-source-pg/src/snapshot.rs
--- a/crates/data-avalanche-source-pg/src/snapshot.rs
+++ b/crates/data-avalanche-source-pg/src/snapshot.rs
@@ -4,9 +4,15 @@
 //! text, the same text pgoutput sends; they are decoded like stream values, so a snapshot row and
 //! a streamed row of the same data are equal. (A `::text` cast isn't the output function:
 //! `true::text` is `true`, pgoutput sends `t`.)
+//!
+//! A chunk is read in one REPEATABLE READ transaction together with its MVCC snapshot
+//! (`pg_current_snapshot()`), and `in_flight` lists the transactions in progress with the tables
+//! they write: the pipeline orders a chunk against the stream by transaction visibility, not by
+//! WAL position (DA-SNAP-9).
 
 use data_avalanche_core::{
-    Chunk, ColumnValue, HaltReason, KeysetCursor, Row, SourceError, TableSchema, identifier_values,
+    Chunk, ChunkVisibility, ColumnValue, HaltReason, InFlightTxn, KeysetCursor, Row, SourceError,
+    TableSchema, XidSnapshot, identifier_values,
 };
 use tokio_postgres::SimpleQueryMessage;
 
@@ -281,28 +287,33 @@ fn decode_row(
     Ok(Row(out))
 }
 
-/// Runs `sql` and decodes its rows. The server cancels it after `timeouts.command`
-/// (`statement_timeout`, local to the statement's implicit transaction).
-async fn rows(
+/// Runs `sql`, one or more statements in one implicit transaction. The server cancels each after
+/// `timeouts.command` (`statement_timeout`, local to the transaction).
+async fn simple_query(
     c: &PgConnector,
     what: &str,
     schema: &TableSchema,
-    columns: &[ReadColumn],
     sql: &str,
-) -> Result<Vec<Row>, SourceError> {
+) -> Result<Vec<SimpleQueryMessage>, SourceError> {
     let client = c.conn.client().await?;
     let command = c.conn.timeouts.command;
     let sql = format!(
         "SET LOCAL statement_timeout = {}; {sql}",
         server_millis(command)
     );
-    let messages = c
-        .conn
+    c.conn
         .bounded(what, command + SERVER_FIRST, client.simple_query(&sql))
         .await
-        .map_err(|e| dropped(schema, e))?;
+        .map_err(|e| dropped(schema, e))
+}
+
+fn decode_rows<'a>(
+    schema: &TableSchema,
+    columns: &[ReadColumn],
+    messages: impl IntoIterator<Item = &'a SimpleQueryMessage>,
+) -> Result<Vec<Row>, SourceError> {
     messages
-        .iter()
+        .into_iter()
         .filter_map(|m| match m {
             SimpleQueryMessage::Row(r) => Some(decode_row(schema, columns, r)),
             _ => None,
@@ -310,6 +321,52 @@ async fn rows(
         .collect()
 }
 
+/// Runs `sql` and decodes its rows.
+async fn rows(
+    c: &PgConnector,
+    what: &str,
+    schema: &TableSchema,
+    columns: &[ReadColumn],
+    sql: &str,
+) -> Result<Vec<Row>, SourceError> {
+    let messages = simple_query(c, what, schema, sql).await?;
+    decode_rows(schema, columns, &messages)
+}
+
+/// A `pg_snapshot` in its text form, `xmin:xmax:xip,…` (`735:740:736,738`).
+fn parse_snapshot(text: &str) -> Result<XidSnapshot, SourceError> {
+    let bad = || SourceError::Permanent(format!("unexpected pg_current_snapshot() {text:?}"));
+    let mut parts = text.splitn(3, ':');
+    let mut xid =
+        || -> Result<u64, SourceError> { parts.next().ok_or_else(bad)?.parse().map_err(|_| bad()) };
+    let (xmin, xmax) = (xid()?, xid()?);
+    let xip = match parts.next().ok_or_else(bad)? {
+        "" => vec![],
+        list => list
+            .split(',')
+            .map(|x| x.parse().map_err(|_| bad()))
+            .collect::<Result<_, _>>()?,
+    };
+    Ok(XidSnapshot { xmin, xmax, xip })
+}
+
+/// Every transaction id `pg_locks` holds (top-level and subtransactions), with the relations of
+/// this database its virtual transaction holds a write lock on. One scan of the lock table.
+const IN_FLIGHT: &str = "WITH l AS MATERIALIZED (SELECT * FROM pg_catalog.pg_locks)
+    SELECT x.transactionid::text,
+           COALESCE(pg_catalog.array_agg(DISTINCT r.relation::int8)
+                      FILTER (WHERE r.relation IS NOT NULL), '{}')
+      FROM l x
+      LEFT JOIN l r
+        ON r.virtualtransaction = x.virtualtransaction
+       AND r.locktype = 'relation'
+       AND r.database = (SELECT oid FROM pg_catalog.pg_database
+                          WHERE datname = pg_catalog.current_database())
+       AND r.mode IN ('RowExclusiveLock', 'ShareRowExclusiveLock', 'ExclusiveLock',
+                      'AccessExclusiveLock')
+     WHERE x.locktype = 'transactionid' AND x.mode = 'ExclusiveLock'
+     GROUP BY x.transactionid::text";
+
 /// The first identifier column `row` holds NULL in.
 fn null_key(schema: &TableSchema, row: &Row) -> String {
     schema
@@ -373,8 +430,9 @@ fn key_texts(key: &Row, schema: &TableSchema) -> Result<Vec<String>, SourceError
 }
 
 impl PgConnector {
-    /// One chunk: `SELECT … WHERE (key) > (cursor) ORDER BY key LIMIT n`, a single statement, so
-    /// one READ COMMITTED snapshot (Snapshot_Resumability §1).
+    /// One chunk: `SELECT … WHERE (key) > (cursor) ORDER BY key LIMIT n` in a REPEATABLE READ
+    /// transaction, after `pg_current_snapshot()`: the chunk's [`ChunkVisibility`] is the snapshot
+    /// its rows were read in (DA-SNAP-9).
     pub async fn read_chunk(
         &self,
         table: &TableSchema,
@@ -414,7 +472,31 @@ impl PgConnector {
         }
         let order: Vec<String> = keys.iter().map(|c| key_expr(c)).collect();
         sql += &format!(" ORDER BY {} LIMIT {limit}", order.join(", "));
-        let rows = rows(self, "read a snapshot chunk", table, &columns, &sql).await?;
+        // The first query of a REPEATABLE READ transaction takes its snapshot; the chunk reads in
+        // the same one.
+        let sql = format!(
+            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
+             SELECT pg_catalog.pg_current_snapshot()::text; {sql}"
+        );
+        let messages = simple_query(self, "read a snapshot chunk", table, &sql).await?;
+        // Each SELECT's rows follow its row description: the snapshot's, then the chunk's.
+        let mut selects = messages
+            .split_inclusive(|m| matches!(m, SimpleQueryMessage::CommandComplete(_)))
+            .filter(|s| {
+                s.iter()
+                    .any(|m| matches!(m, SimpleQueryMessage::RowDescription(_)))
+            });
+        let snapshot = selects
+            .next()
+            .and_then(|s| {
+                s.iter().find_map(|m| match m {
+                    SimpleQueryMessage::Row(r) => r.get(0),
+                    _ => None,
+                })
+            })
+            .ok_or_else(|| SourceError::Permanent("no pg_current_snapshot() row".into()))?;
+        let visibility = ChunkVisibility::Xids(parse_snapshot(snapshot)?);
+        let rows = decode_rows(table, &columns, selects.next().unwrap_or_default())?;
         let next = match rows.last() {
             None => None,
             Some(last) => Some(KeysetCursor {
@@ -431,10 +513,41 @@ impl PgConnector {
         Ok(Chunk {
             rows,
             next,
-            visibility: None,
+            visibility: Some(visibility),
         })
     }
 
+    /// The transactions in progress now with the tables they write (DA-SNAP-9): every
+    /// transaction id `pg_locks` holds, with the relations its virtual transaction holds a write
+    /// lock on in this database. A synchronous commit waiting for its standby keeps both until it
+    /// is visible (measured on PostgreSQL 18). The ids are 32-bit, like the stream's.
+    pub async fn in_flight(&self) -> Result<Option<Vec<InFlightTxn>>, SourceError> {
+        let client = self.conn.client().await?;
+        let rows = self
+            .conn
+            .command(
+                "read the transactions in progress",
+                client.query(IN_FLIGHT, &[]),
+            )
+            .await?;
+        rows.iter()
+            .map(|r| {
+                let xid: String = r.get(0);
+                Ok(InFlightTxn {
+                    xid: xid.parse().map_err(|_| {
+                        SourceError::Permanent(format!("unexpected transaction id {xid:?}"))
+                    })?,
+                    tables: r
+                        .get::<_, Vec<i64>>(1)
+                        .into_iter()
+                        .map(|t| t as u64)
+                        .collect(),
+                })
+            })
+            .collect::<Result<_, _>>()
+            .map(Some)
+    }
+
     /// The current rows of these keys (`toast_strategy: re_query`, Q-M5-6), [`FETCH_BATCH`] keys
     /// per statement. A key with no row is left out; rows come in no particular order.
     pub async fn fetch_rows(
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-source-pg --lib
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-source-pg --test pg snapshot::
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-source-pg --test sync_commit
```

Expected:

PASS:

```text
test result: ok. 81 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 44 filtered out; finished in 1.42s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.43s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(source-pg): chunks read with their snapshot, in-flight writers, a busy slot is contended"
```


### Task 3: State: the schema cache, the halt registry, the slot intent

**Files:**
- Create: `crates/data-avalanche-state/src/tables.rs`
- Modify: `crates/data-avalanche-state/src/lib.rs`, `…/pipeline.rs`, `…/snapshot.rs`

**Interfaces:**
- Consumes: Task 1's serde for `TableSchema`; `PipelineDocs`, `DocStore`, `Versioned`, `SlotDoc`
  (state, M2).
- Produces (state root):
  - `PipelineDocs::table_schema(&TableRef) -> Result<Option<Versioned<TableSchema>>>`,
    `table_schemas() -> Result<Vec<TableSchema>>`, `save_table_schema(&TableSchema)`; the key
    `schemas/<schema>.<table>.json` (`table_schema_key`, `SCHEMAS_PREFIX`; names escaped like
    the snapshot documents).
  - `TableHalt { table, reason: HaltReason, at: SourcePosition }`, `HaltsDoc { tables }`,
    `PipelineDocs::halts()`, `record_halt(&TableHalt)` (replaces the table's earlier halt),
    `clear_halt(&TableRef)`; the key `halts.json` (`HALTS_KEY`).
  - `SlotDoc::intent(slot_name)` (`confirmed_flush_lsn` `0/0`) and `SlotDoc::is_intent()`.

The M5-carried items need these documents. A cached schema with its source id finds a table
dropped and created again while the pipeline was down. Recording the intent before a managed
first-start `prepare` lets the next start know an orphan slot is its own. The halt registry
keeps a halted table halted across restarts (S-R).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -25,9 +25,10 @@
 //!
 //! Per pipeline (one [`PipelineDocs`] rooted at the pipeline's state location):
 //! [`PipelineDocs::open`] creates or checks `pipeline-state.json` (refusing another pipeline's
-//! state or a changed `sink_type`), the desired state, `replication/slot.json` (PostgreSQL),
-//! `replication/position.json` (MySQL / MariaDB, with the source server identity), per-table
-//! snapshot state and on-demand snapshot jobs.
+//! state or a changed `sink_type`), the desired state, `replication/slot.json` (PostgreSQL, with
+//! the intent recorded before a first start creates the slot), `replication/position.json` (MySQL /
+//! MariaDB, with the source server identity), per-table snapshot state, on-demand snapshot jobs,
+//! the schema cache (`schemas/<schema>.<table>.json`) and the halt registry (`halts.json`).
 
 mod anchor;
 mod doc;
@@ -35,6 +36,7 @@ mod generation;
 mod lease;
 mod pipeline;
 mod snapshot;
+mod tables;
 #[cfg(test)]
 mod test_support;
 
@@ -53,3 +55,4 @@ pub use snapshot::{
     CursorKey, JOBS_PREFIX, JobStatus, JobTable, SnapshotCursor, SnapshotJobDoc, SnapshotStatus,
     TableSnapshotDoc, job_key, table_snapshot_key,
 };
+pub use tables::{HALTS_KEY, HaltsDoc, SCHEMAS_PREFIX, TableHalt, table_schema_key};
```

Change `crates/data-avalanche-state/src/pipeline.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-state/src/pipeline.rs b/crates/data-avalanche-state/src/pipeline.rs
--- a/crates/data-avalanche-state/src/pipeline.rs
+++ b/crates/data-avalanche-state/src/pipeline.rs
@@ -326,6 +326,21 @@ mod tests {
         assert_eq!(p.slot().await.unwrap().unwrap().doc, later);
     }
 
+    #[test]
+    fn a_slot_intent_is_the_zero_position() {
+        let intent = SlotDoc::intent("data_avalanche_orders");
+        assert!(intent.is_intent());
+        assert_eq!(
+            json(&intent),
+            "{\n  \"version\": 1,\n  \"slot_name\": \"data_avalanche_orders\",\n  \"confirmed_flush_lsn\": \"0/0\"\n}"
+        );
+        let started = SlotDoc {
+            confirmed_flush_lsn: "0/16B3748".parse().unwrap(),
+            ..intent
+        };
+        assert!(!started.is_intent());
+    }
+
     #[tokio::test]
     async fn position_round_trips_for_every_mysql_dialect() {
         let p = pipeline();
```

Create `crates/data-avalanche-state/src/tables.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V};
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;
    use crate::doc::DocStore;

    fn pipeline() -> PipelineDocs {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/orders").unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(store)))
    }

    fn schema(table: TableRef) -> TableSchema {
        TableSchema::new(
            table,
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
        .with_source_id(16384)
    }

    fn json<T: Document>(doc: &T) -> String {
        String::from_utf8(crate::doc::encode(doc).to_vec()).unwrap()
    }

    #[test]
    fn schema_keys_escape_like_snapshot_keys() {
        let key = |s: &str, n: &str| table_schema_key(&TableRef::new(s, n));
        assert_eq!(key("public", "orders"), "schemas/public.orders.json");
        assert_eq!(key("a.b", "c"), "schemas/a%2Eb.c.json");
        assert_eq!(key("a", "b.c"), "schemas/a.b%2Ec.json");
    }

    #[test]
    fn schema_json_shape() {
        let s = schema(TableRef::new("public", "orders"));
        assert_eq!(
            json(&s),
            r#"{
  "version": 1,
  "table": {
    "schema": "public",
    "name": "orders"
  },
  "columns": [
    {
      "name": "id",
      "value_type": "int64",
      "logical": "int64",
      "nullable": false,
      "source_type": "bigint",
      "source_ordinal": null
    },
    {
      "name": "note",
      "value_type": "text",
      "logical": "string",
      "nullable": true,
      "source_type": "text",
      "source_ordinal": null
    }
  ],
  "identifier": [
    "id"
  ],
  "source_id": 16384
}"#
        );
    }

    #[tokio::test]
    async fn schemas_round_trip_and_list() {
        let p = pipeline();
        let (orders, items) = (
            TableRef::new("public", "orders"),
            TableRef::new("public", "items"),
        );
        assert!(p.table_schema(&orders).await.unwrap().is_none());
        assert!(p.table_schemas().await.unwrap().is_empty());
        p.save_table_schema(&schema(orders.clone())).await.unwrap();
        p.save_table_schema(&schema(items.clone())).await.unwrap();
        let later = schema(orders.clone()).with_source_id(16999);
        p.save_table_schema(&later).await.unwrap();
        assert_eq!(p.table_schema(&orders).await.unwrap().unwrap().doc, later);
        assert_eq!(
            p.table_schemas().await.unwrap(),
            vec![schema(items), later],
            "in key order"
        );
    }

    #[tokio::test]
    async fn a_schema_under_another_tables_key_is_corrupt() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        let body = crate::doc::encode(&schema(TableRef::new("public", "invoices")));
        p.docs
            .store()
            .put(&table_schema_key(&orders), body)
            .await
            .unwrap();
        let e = p.table_schema(&orders).await.unwrap_err();
        assert!(
            e.to_string()
                .contains("it is for table public.invoices, not public.orders"),
            "{e}"
        );
        assert!(matches!(
            p.table_schemas().await.unwrap_err(),
            StateError::Corrupt { .. }
        ));
    }

    #[tokio::test]
    async fn a_stored_schema_that_isnt_replicable_is_corrupt() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        let text =
            json(&schema(orders.clone())).replace("\"nullable\": false", "\"nullable\": true");
        p.docs
            .store()
            .put(&table_schema_key(&orders), Bytes::from(text))
            .await
            .unwrap();
        let e = p.table_schema(&orders).await.unwrap_err();
        assert!(e.to_string().contains("can't be a key"), "{e}");
    }

    fn halt(name: &str, reason: HaltReason) -> TableHalt {
        TableHalt {
            table: TableRef::new("public", name),
            reason,
            at: "pg:0/16B3748".parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn halts_are_recorded_replaced_and_cleared() {
        let p = pipeline();
        assert_eq!(p.halts().await.unwrap(), HaltsDoc::default());
        let dropped = halt("orders", HaltReason::TableDropped);
        p.record_halt(&dropped).await.unwrap();
        let overflow = halt(
            "items",
            HaltReason::NumericOverflow {
                column: "price".into(),
            },
        );
        p.record_halt(&overflow).await.unwrap();
        let again = halt(
            "orders",
            HaltReason::UnsafeDdl {
                detail: "narrowed".into(),
            },
        );
        p.record_halt(&again).await.unwrap();
        assert_eq!(
            p.halts().await.unwrap().tables,
            vec![overflow.clone(), again],
            "one per table, in table order"
        );
        p.clear_halt(&TableRef::new("public", "orders"))
            .await
            .unwrap();
        assert_eq!(p.halts().await.unwrap().tables, vec![overflow]);
    }

    #[test]
    fn halts_json_shape() {
        let doc = HaltsDoc {
            tables: vec![halt(
                "orders",
                HaltReason::NumericOverflow {
                    column: "price".into(),
                },
            )],
        };
        assert_eq!(
            json(&doc),
            r#"{
  "version": 1,
  "tables": [
    {
      "table": {
        "schema": "public",
        "name": "orders"
      },
      "reason": {
        "code": "NUMERIC_OVERFLOW",
        "column": "price"
      },
      "at": "pg:0/16B3748"
    }
  ]
}"#
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-state --lib
```

Expected:

FAIL to compile (16 errors); the first:

```text
error[E0432]: unresolved imports `tables::HALTS_KEY`, `tables::HaltsDoc`, `tables::SCHEMAS_PREFIX`, `tables::TableHalt`, `tables::table_schema_key`
error[E0425]: cannot find type `PipelineDocs` in this scope
error[E0433]: cannot find type `PipelineDocs` in this scope
error[E0425]: cannot find type `TableRef` in this scope
error[E0425]: cannot find type `TableSchema` in this scope
error[E0405]: cannot find trait `Document` in this scope
… and 10 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-state/src/pipeline.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/pipeline.rs b/crates/data-avalanche-state/src/pipeline.rs
--- a/crates/data-avalanche-state/src/pipeline.rs
+++ b/crates/data-avalanche-state/src/pipeline.rs
@@ -53,6 +53,23 @@ impl Document for SlotDoc {
     const SCHEMA_VERSION: u64 = 1;
 }
 
+impl SlotDoc {
+    /// Recorded before a first start's `prepare` creates the slot: `confirmed_flush_lsn` `0/0`,
+    /// which no slot has. A start that finds it knows a slot of this name is its own (created, then
+    /// the start ended before it recorded the slot's position), so the slot is recreated and the
+    /// start is a first start again, not an orphan's.
+    pub fn intent(slot_name: &str) -> Self {
+        Self {
+            slot_name: slot_name.to_owned(),
+            confirmed_flush_lsn: Lsn(0),
+        }
+    }
+
+    pub fn is_intent(&self) -> bool {
+        self.confirmed_flush_lsn == Lsn(0)
+    }
+}
+
 /// The source server a MySQL / MariaDB position was read from. File/position mode refuses to
 /// resume against a different server (`BINLOG_SOURCE_CHANGED`, Q-M7-2).
 #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
```

Change `crates/data-avalanche-state/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/snapshot.rs b/crates/data-avalanche-state/src/snapshot.rs
--- a/crates/data-avalanche-state/src/snapshot.rs
+++ b/crates/data-avalanche-state/src/snapshot.rs
@@ -9,6 +9,7 @@ use serde::{Deserialize, Serialize};
 
 use crate::doc::{Document, StateError, Versioned};
 use crate::pipeline::PipelineDocs;
+use crate::tables::table_key;
 
 #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
 #[serde(rename_all = "snake_case")]
@@ -101,12 +102,7 @@ impl Document for SnapshotJobDoc {
 /// `snapshots/<schema>.<table>.json`. Each name is escaped as a key segment and its dots become
 /// `%2E`, so the `.` between them is unambiguous (`a.b` + `c` ≠ `a` + `b.c`).
 pub fn table_snapshot_key(table: &TableRef) -> String {
-    let part = |s: &str| escape_segment(s).replace('.', "%2E");
-    format!(
-        "snapshots/{}.{}.json",
-        part(&table.schema),
-        part(&table.name)
-    )
+    table_key("snapshots", table)
 }
 
 pub const JOBS_PREFIX: &str = "snapshots/jobs";
```

Add the implementation to `crates/data-avalanche-state/src/tables.rs`, above the test module:

```rust
//! Per-table documents beside the snapshot state (Data-Flow §State Layout): the schema cache,
//! `schemas/<schema>.<table>.json`, one per tracked table, and the halt registry, `halts.json`.
//!
//! The schema cache holds each table's schema as of the pipeline's checkpoint, with the source's
//! identity of the table (`TableSchema::source_id`): a restart opens the stream with it, and a
//! table dropped and created again while the pipeline was down is told apart by that identity
//! (Q-M5-20). The pipeline writes it after the checkpoint that covers a schema change.
//!
//! A tracked table's halt is recorded before the checkpoint passes it: the halted table's changes
//! after it are dropped, so resuming it from a later checkpoint would skip them. A recorded halt
//! keeps the table halted at every start until a re-snapshot clears it.

use data_avalanche_core::{HaltReason, SourcePosition, TableRef, TableSchema};
use data_avalanche_storage::escape_segment;
use serde::{Deserialize, Serialize};

use crate::doc::{Document, StateError, Versioned};
use crate::pipeline::PipelineDocs;

pub const SCHEMAS_PREFIX: &str = "schemas";
pub const HALTS_KEY: &str = "halts.json";

impl Document for TableSchema {
    const KIND: &'static str = "table schema";
    const SCHEMA_VERSION: u64 = 1;
}

/// `<prefix>/<schema>.<table>.json`. Each name is escaped as a key segment and its dots become
/// `%2E`, so the `.` between them is unambiguous (`a.b` + `c` ≠ `a` + `b.c`).
pub(crate) fn table_key(prefix: &str, table: &TableRef) -> String {
    let part = |s: &str| escape_segment(s).replace('.', "%2E");
    format!(
        "{prefix}/{}.{}.json",
        part(&table.schema),
        part(&table.name)
    )
}

/// `schemas/<schema>.<table>.json`.
pub fn table_schema_key(table: &TableRef) -> String {
    table_key(SCHEMAS_PREFIX, table)
}

/// One halted table: why, and how far its sink content is complete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableHalt {
    pub table: TableRef,
    pub reason: HaltReason,
    /// The sink holds the table's changes through this position; later ones were dropped.
    pub at: SourcePosition,
}

/// `halts.json`: the tracked tables that halted, in table order.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HaltsDoc {
    pub tables: Vec<TableHalt>,
}

impl Document for HaltsDoc {
    const KIND: &'static str = "halt registry";
    const SCHEMA_VERSION: u64 = 1;
}

impl PipelineDocs {
    /// # Errors
    /// A storage or decode failure; `Corrupt` when the stored schema names another table.
    pub async fn table_schema(
        &self,
        table: &TableRef,
    ) -> Result<Option<Versioned<TableSchema>>, StateError> {
        let key = table_schema_key(table);
        let found = self.docs.load::<TableSchema>(&key).await?;
        if let Some(v) = &found
            && v.doc.table() != table
        {
            return Err(StateError::Corrupt {
                key,
                kind: TableSchema::KIND,
                reason: format!("it is for table {}, not {table}", v.doc.table()),
            });
        }
        Ok(found)
    }

    /// Every cached schema, in key order.
    ///
    /// # Errors
    /// A storage or decode failure (one bad document fails the listing).
    pub async fn table_schemas(&self) -> Result<Vec<TableSchema>, StateError> {
        let mut out = vec![];
        for key in self.docs.store().list(SCHEMAS_PREFIX).await? {
            if let Some(v) = self.docs.load::<TableSchema>(&key).await? {
                if table_schema_key(v.doc.table()) != key {
                    return Err(StateError::Corrupt {
                        key,
                        kind: TableSchema::KIND,
                        reason: format!("it is for table {}", v.doc.table()),
                    });
                }
                out.push(v.doc);
            }
        }
        Ok(out)
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn save_table_schema(
        &self,
        schema: &TableSchema,
    ) -> Result<Versioned<TableSchema>, StateError> {
        self.docs
            .modify(
                &table_schema_key(schema.table()),
                |_: Option<TableSchema>| Ok(schema.clone()),
            )
            .await
    }

    /// The halt registry; empty when no table ever halted.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn halts(&self) -> Result<HaltsDoc, StateError> {
        Ok(self
            .docs
            .load::<HaltsDoc>(HALTS_KEY)
            .await?
            .map(|v| v.doc)
            .unwrap_or_default())
    }

    /// Records a table's halt, replacing an earlier one of the same table.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn record_halt(&self, halt: &TableHalt) -> Result<Versioned<HaltsDoc>, StateError> {
        self.docs
            .modify(HALTS_KEY, |cur: Option<HaltsDoc>| {
                let mut doc = cur.unwrap_or_default();
                doc.tables.retain(|h| h.table != halt.table);
                doc.tables.push(halt.clone());
                doc.tables.sort_by(|a, b| a.table.cmp(&b.table));
                Ok(doc)
            })
            .await
    }

    /// Clears a table's halt (the re-snapshot that resumes it).
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn clear_halt(&self, table: &TableRef) -> Result<Versioned<HaltsDoc>, StateError> {
        self.docs
            .modify(HALTS_KEY, |cur: Option<HaltsDoc>| {
                let mut doc = cur.unwrap_or_default();
                doc.tables.retain(|h| &h.table != table);
                Ok(doc)
            })
            .await
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-state --lib
```

Expected:

PASS:

```text
test result: ok. 65 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(state): schema cache, halt registry, the slot-creation intent"
```


### Task 4: Testkit: a scripted fake source, sink faults

**Files:**
- Create: `crates/data-avalanche-testkit/src/fake_source.rs`
- Modify: `crates/data-avalanche-testkit/src/memory_sink.rs`, `…/lib.rs`, `…/Cargo.toml`

**Interfaces:**
- Consumes: the source contract (Task 1), `MemorySink` (testkit, M3).
- Produces (testkit root):
  - `FakeSource::new(Vec<TableSchema>)`, a `SourceConnector` over an in-memory table store
    with a WAL. Commits come from `commit(FakeTxn)` (→ xid), `commit_stalled` / `release`
    (committed and streamed, invisible to reads until released), and `open_writer` /
    `commit_writer` / `abort_writer` (in flight). Schema changes come from `alter`
    and `drop_table`. `load` puts rows in without the WAL; `keepalive`, `wal_end`, `rows`,
    `reads`, `hold_reads` / `release_reads` and `fail(Method, SourceError)` script the rest.
  - `FakeTxn::new().insert / update / update_key / delete / truncate`; `Method` (one per trait
    method, plus `Next`).
  - `MemorySink::fail(SinkOp, Option<&TableRef>, times, error)`, `hold_commits` /
    `release_commits`, `take_over(table, epoch)` (a newer owner's epoch).

The worker's unit tests run on these. A chunk read sees only the transactions committed
and released before it, like an MVCC snapshot, and reports them as an `XidSnapshot`. A schema
change takes effect in the WAL order, so a Relation-style `SchemaSignal` arrives before the next
change.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-testkit/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-testkit/Cargo.toml b/crates/data-avalanche-testkit/Cargo.toml
--- a/crates/data-avalanche-testkit/Cargo.toml
+++ b/crates/data-avalanche-testkit/Cargo.toml
@@ -19,6 +19,7 @@ data-avalanche-core.workspace = true
 futures-util.workspace = true
 object_store.workspace = true
 rustls.workspace = true
+tokio = { workspace = true, features = ["sync"] }
 uuid.workspace = true
 
 [dev-dependencies]
```

Create `crates/data-avalanche-testkit/src/fake_source.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnValue as Val, ColumnValueType as V};

    use super::*;

    fn t() -> TableRef {
        TableRef::new("public", "t")
    }

    fn schema() -> TableSchema {
        TableSchema::new(
            t(),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("v", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn row(id: i64, v: &str) -> Row {
        Row(vec![
            ("id".into(), Val::Int64(id)),
            ("v".into(), Val::Text(v.into())),
        ])
    }

    fn key(id: i64) -> Row {
        Row(vec![("id".into(), Val::Int64(id))])
    }

    async fn items(s: &mut Box<dyn ChangeStream>, n: usize) -> Vec<StreamItem> {
        let mut out = vec![];
        for _ in 0..n {
            out.push(s.next().await.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn a_first_start_snapshots_then_streams_from_the_slot() {
        let src = FakeSource::new(vec![schema()]);
        src.load(&t(), vec![row(1, "a")]);
        let p = src.prepare(None, &[t()]).await.unwrap();
        let StartPlan::Snapshot { from, reason } = p.plan else {
            panic!("{p:?}")
        };
        assert_eq!(reason, SnapshotReason::FirstStart);
        assert_eq!(src.slot(), Some(from.clone()));
        let tables = src.resolve_tables().await.unwrap().tables;
        assert_eq!(tables[0].source_id(), Some(1000));
        let mut s = src.open_stream(&from, &tables).await.unwrap();
        src.commit(FakeTxn::new().insert(&t(), row(2, "b")));
        let got = items(&mut s, 3).await;
        assert!(matches!(got[0], StreamItem::Begin { xid: 700, .. }));
        let StreamItem::Change(e) = &got[1] else {
            panic!("{got:?}")
        };
        assert_eq!(
            (e.op, e.after.clone(), e.xid),
            (Op::Insert, Some(row(2, "b")), 700)
        );
        let StreamItem::Commit(b) = &got[2] else {
            panic!()
        };
        assert_eq!(e.source_pos, b.commit_pos);
        let chunk = src.read_chunk(&tables[0], None, 10).await.unwrap();
        assert_eq!(chunk.rows, vec![row(1, "a"), row(2, "b")]);
        s.acknowledge(&b.commit_pos).await.unwrap();
        assert_eq!(src.slot(), Some(b.commit_pos.clone()));
        let resumed = src.prepare(Some(&b.commit_pos), &[t()]).await.unwrap();
        assert_eq!(resumed.plan, StartPlan::Resume);
    }

    #[tokio::test]
    async fn a_stalled_commit_streams_before_chunks_see_it() {
        let src = FakeSource::new(vec![schema()]);
        src.load(&t(), vec![row(1, "a")]);
        let tables = src.resolve_tables().await.unwrap().tables;
        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
        let xid = src.commit_stalled(FakeTxn::new().update(&t(), row(1, "b")));
        let got = items(&mut s, 3).await;
        assert!(matches!(&got[2], StreamItem::Commit(b) if b.xid == xid));
        let chunk = src.read_chunk(&tables[0], None, 10).await.unwrap();
        assert_eq!(chunk.rows, vec![row(1, "a")], "the old row");
        let Some(ChunkVisibility::Xids(snap)) = &chunk.visibility else {
            panic!()
        };
        assert!(!snap.sees(xid));
        let in_flight = src.in_flight().await.unwrap().unwrap();
        assert_eq!(
            in_flight,
            vec![InFlightTxn {
                xid,
                tables: vec![1000]
            }]
        );
        src.release(xid);
        let chunk = src.read_chunk(&tables[0], None, 10).await.unwrap();
        assert_eq!(chunk.rows, vec![row(1, "b")]);
        let Some(ChunkVisibility::Xids(snap)) = &chunk.visibility else {
            panic!()
        };
        assert!(snap.sees(xid));
    }

    #[tokio::test]
    async fn changes_keys_truncates_and_unchanged_values_stream_as_sent() {
        let src = FakeSource::new(vec![schema()]);
        let tables = src.resolve_tables().await.unwrap().tables;
        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
        src.commit(
            FakeTxn::new()
                .insert(&t(), row(1, "a"))
                .update(
                    &t(),
                    Row(vec![
                        ("id".into(), Val::Int64(1)),
                        ("v".into(), Val::Unchanged),
                    ]),
                )
                .update_key(&t(), key(1), row(2, "c"))
                .delete(&t(), key(2))
                .truncate(&[t()]),
        );
        let got = items(&mut s, 7).await;
        let ops: Vec<_> = got
            .iter()
            .filter_map(|i| match i {
                StreamItem::Change(e) => Some(e.op),
                _ => None,
            })
            .collect();
        assert_eq!(ops, [Op::Insert, Op::Update, Op::Update, Op::Delete]);
        let StreamItem::Change(e) = &got[2] else {
            panic!()
        };
        assert_eq!(e.after.as_ref().unwrap().get("v"), Some(&Val::Unchanged));
        assert_eq!(e.before, Some(key(1)));
        assert!(matches!(&got[5], StreamItem::Truncate { tables, .. } if tables == &[t()]));
        assert!(src.rows(&t()).is_empty());
    }

    #[tokio::test]
    async fn an_alter_signals_before_the_next_change() {
        let src = FakeSource::new(vec![schema()]);
        let tables = src.resolve_tables().await.unwrap().tables;
        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
        let wider = TableSchema::new(
            t(),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("v", V::Text, "text"),
                ColumnSchema::new("n", V::Int32, "integer"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        src.alter(&t(), wider.clone());
        let mut r = row(1, "a");
        r.0.push(("n".into(), Val::Int32(5)));
        src.commit(FakeTxn::new().insert(&t(), r));
        let got = items(&mut s, 4).await;
        let StreamItem::SchemaSignal { table, hint, .. } = &got[1] else {
            panic!("{got:?}")
        };
        assert_eq!(table, &t());
        assert!(matches!(hint, SchemaHint::Columns(c) if c.len() == 3));
        let (delta, now) = src.augment(&tables[0], hint).await.unwrap();
        assert!(
            matches!(&delta.changes[..], [SchemaChange::AddColumn(c)] if c.name == "n"),
            "{delta:?}"
        );
        assert_eq!(now.unwrap().source_id(), Some(1000));
        let report = src.rescan(&tables).await.unwrap();
        assert_eq!(report.changed.len(), 1);
    }

    #[tokio::test]
    async fn faults_fire_once_and_a_lost_slot_fails_the_stream() {
        let src = FakeSource::new(vec![schema()]);
        src.fail(Method::ReadChunk, SourceError::Transient("reset".into()));
        let tables = src.resolve_tables().await.unwrap().tables;
        assert!(
            src.read_chunk(&tables[0], None, 1)
                .await
                .unwrap_err()
                .is_transient()
        );
        assert!(src.read_chunk(&tables[0], None, 1).await.is_ok());
        let p = src.prepare(None, &[t()]).await.unwrap();
        let StartPlan::Snapshot { from, .. } = p.plan else {
            panic!()
        };
        let mut s = src.open_stream(&from, &tables).await.unwrap();
        src.lose_slot();
        assert!(matches!(
            s.next().await,
            Err(SourceError::PositionUnreachable { .. })
        ));
        let again = src.prepare(Some(&from), &[t()]).await.unwrap();
        assert!(matches!(
            again.plan,
            StartPlan::Snapshot {
                reason: SnapshotReason::PositionLost,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn a_held_read_returns_its_earlier_snapshot() {
        let src = FakeSource::new(vec![schema()]);
        src.load(&t(), vec![row(1, "a")]);
        let tables = src.resolve_tables().await.unwrap().tables;
        src.hold_reads();
        let reader = {
            let src = src.clone();
            let t0 = tables[0].clone();
            tokio::spawn(async move { src.read_chunk(&t0, None, 10).await })
        };
        while src.reads(&t()) == 0 {
            tokio::task::yield_now().await;
        }
        let xid = src.commit(FakeTxn::new().update(&t(), row(1, "b")));
        src.release_reads();
        let chunk = reader.await.unwrap().unwrap();
        assert_eq!(chunk.rows, vec![row(1, "a")]);
        let Some(ChunkVisibility::Xids(snap)) = &chunk.visibility else {
            panic!()
        };
        assert!(!snap.sees(xid), "committed after the snapshot");
    }
}
```

Change `crates/data-avalanche-testkit/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/lib.rs b/crates/data-avalanche-testkit/src/lib.rs
--- a/crates/data-avalanche-testkit/src/lib.rs
+++ b/crates/data-avalanche-testkit/src/lib.rs
@@ -4,8 +4,10 @@
 
 use std::sync::Arc;
 
+pub mod fake_source;
 pub mod memory_sink;
-pub use memory_sink::MemorySink;
+pub use fake_source::{FakeSource, FakeTxn, Method};
+pub use memory_sink::{MemorySink, SinkOp};
 
 use futures_util::TryStreamExt;
 use object_store::ObjectStore;
```

Change `crates/data-avalanche-testkit/src/memory_sink.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -542,6 +542,56 @@ mod tests {
         assert_eq!(rows(&sink), owned(&[(1, Some("new"))]));
     }
 
+    #[tokio::test]
+    async fn faults_fail_matching_calls_and_change_nothing() {
+        let sink = sink().await;
+        let t = TableRef::new("public", "t");
+        sink.fail(SinkOp::Commit, Some(&t), 2, || SinkError::Transient {
+            reason: "throttled".into(),
+        });
+        let set = cs(10, vec![upserts(&[(1, Some("a"))], None)], vec![]);
+        let m = marker(1, 10);
+        for _ in 0..2 {
+            let e = sink.commit(set.clone(), m.clone()).await.unwrap_err();
+            assert!(e.is_transient(), "{e}");
+        }
+        assert_eq!(sink.version(&t), Some(0), "nothing written");
+        assert_eq!(
+            sink.commit(set, m).await.unwrap(),
+            CommitOutcome::Committed { version: 1 }
+        );
+        sink.fail(SinkOp::TableState, None, 1, || SinkError::Permanent {
+            reason: "gone".into(),
+        });
+        assert!(sink.table_state(&t).await.is_err());
+        assert!(sink.table_state(&t).await.is_ok());
+        assert_eq!(sink.marker(&t).unwrap().owner_epoch, 1);
+        sink.take_over(&t, 9);
+        let later = cs(20, vec![upserts(&[(1, Some("b"))], None)], vec![]);
+        assert_eq!(
+            commit(&sink, later, 1).await,
+            CommitOutcome::Fenced { current_epoch: 9 }
+        );
+    }
+
+    #[tokio::test]
+    async fn held_commits_wait_for_the_release() {
+        let sink = sink().await;
+        let t = TableRef::new("public", "t");
+        sink.hold_commits();
+        let held = sink.clone();
+        let commit = tokio::spawn(async move {
+            let set = cs(10, vec![upserts(&[(1, Some("a"))], None)], vec![]);
+            held.commit(set, marker(1, 10)).await
+        });
+        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
+        assert!(!commit.is_finished());
+        assert_eq!(sink.version(&t), Some(0));
+        sink.release_commits();
+        assert!(commit.await.unwrap().is_ok());
+        assert_eq!(sink.version(&t), Some(1));
+    }
+
     #[tokio::test]
     async fn unchanged_masks_keep_the_stored_value() {
         let sink = sink().await;
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-testkit --lib
```

Expected:

FAIL to compile (24 errors); the first:

```text
error[E0432]: unresolved imports `fake_source::FakeSource`, `fake_source::FakeTxn`, `fake_source::Method`
error[E0432]: unresolved import `memory_sink::SinkOp`
error[E0425]: cannot find type `TableRef` in this scope
error[E0425]: cannot find type `TableSchema` in this scope
error[E0425]: cannot find type `Row` in this scope
error[E0405]: cannot find trait `ChangeStream` in this scope
… and 18 more
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-testkit/src/fake_source.rs`, above the test module:

```rust
//! A scripted source (roadmap M6a): an in-memory database with a write-ahead log, a replication
//! slot and MVCC visibility, behind the `SourceConnector` contract, so pipeline tests run the
//! whole worker without PostgreSQL. Positions are PostgreSQL LSNs.
//!
//! - Tests commit transactions ([`FakeTxn`]): the rows change and the log grows; an open stream
//!   delivers them. [`FakeSource::commit_stalled`] commits one that the stream delivers but a
//!   chunk doesn't see until [`FakeSource::release`], like a synchronous commit waiting for its
//!   standby (DA-SNAP-9); [`FakeSource::open_writer`] is a transaction still running.
//! - The slot follows `Postgres_Slot…` §1 in short: a first start creates it, a checkpoint the slot
//!   is past (or a lost slot) recreates it, and `acknowledge` moves it.
//! - [`FakeSource::alter`] changes a table's schema: the stream signals it before the table's next
//!   change, `augment` diffs by column name and `rescan` reports it.
//! - [`FakeSource::fail`] makes the next call of one method fail; [`FakeSource::hold_reads`]
//!   holds chunk reads after they took their snapshot, so a test can commit while one is out.
//!
//! Keys are integer or text identifier columns. The fake trusts its tests: a change of a key a
//! stalled transaction holds, or a key that isn't an integer or text, panics.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ChangeStream, Chunk, ChunkVisibility, ColumnSchema, ColumnValue, HaltReason,
    InFlightTxn, KeysetCursor, Op, Preflight, Prepared, RescanReport, ResolvedTables, Row,
    SchemaChange, SchemaDelta, SchemaHint, SnapshotReason, SourceConnector, SourceError,
    SourcePosition, SourceSeq, StartPlan, StreamItem, TableRef, TableSchema, TransactionBoundary,
    XidSnapshot, identifier_values,
};
use tokio::sync::Notify;

/// The method a [`FakeSource::fail`] fault fires in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Preflight,
    ResolveTables,
    Prepare,
    ResetPosition,
    RetainedBytes,
    CurrentPosition,
    InFlight,
    ReadChunk,
    FetchRows,
    OpenStream,
    /// A stream's `next`, after the items it has already queued.
    Next,
    Augment,
    Rescan,
}

/// One change of a [`FakeTxn`].
#[derive(Debug, Clone)]
enum FakeOp {
    Insert(TableRef, Row),
    /// The after-image; a [`ColumnValue::Unchanged`] keeps the stored value (unchanged TOAST).
    Update(TableRef, Row),
    /// A key change: the old key, then the new row.
    Move(TableRef, Row, Row),
    Delete(TableRef, Row),
    Truncate(Vec<TableRef>),
}

/// A source transaction, built change by change.
#[derive(Debug, Clone, Default)]
pub struct FakeTxn {
    ops: Vec<FakeOp>,
}

impl FakeTxn {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(mut self, table: &TableRef, row: Row) -> Self {
        self.ops.push(FakeOp::Insert(table.clone(), row));
        self
    }

    /// Updates the row with `row`'s key; a [`ColumnValue::Unchanged`] in it keeps the stored
    /// value and is streamed as unchanged.
    pub fn update(mut self, table: &TableRef, row: Row) -> Self {
        self.ops.push(FakeOp::Update(table.clone(), row));
        self
    }

    /// Moves the row with key `old` to `row`'s key (a PK-changing UPDATE).
    pub fn update_key(mut self, table: &TableRef, old: Row, row: Row) -> Self {
        self.ops.push(FakeOp::Move(table.clone(), old, row));
        self
    }

    /// Deletes the row with key `key` (its identifier columns).
    pub fn delete(mut self, table: &TableRef, key: Row) -> Self {
        self.ops.push(FakeOp::Delete(table.clone(), key));
        self
    }

    pub fn truncate(mut self, tables: &[TableRef]) -> Self {
        self.ops.push(FakeOp::Truncate(tables.to_vec()));
        self
    }
}

/// A key in identifier order, for sorting chunks.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum KeyPart {
    Int(i64),
    Text(String),
}

type Key = Vec<KeyPart>;

fn key_of(schema: &TableSchema, row: &Row) -> Key {
    let key = identifier_values(schema, row)
        .unwrap_or_else(|| panic!("{}: a row without its key: {row:?}", schema.table()));
    key.0
        .iter()
        .map(|(_, v)| match v {
            ColumnValue::Int16(i) => KeyPart::Int(i64::from(*i)),
            ColumnValue::Int32(i) => KeyPart::Int(i64::from(*i)),
            ColumnValue::Int64(i) => KeyPart::Int(*i),
            ColumnValue::Text(s) => KeyPart::Text(s.clone()),
            other => panic!("the fake source keys on integers and text, not {other:?}"),
        })
        .collect()
}

/// One logged change: what the stream sends, and the table's columns when it was written.
#[derive(Debug, Clone)]
enum WalChange {
    Change {
        event: CdcEvent,
        columns: Vec<ColumnSchema>,
    },
    Truncate(Vec<TableRef>, SourceSeq),
}

#[derive(Debug, Clone)]
enum WalEntry {
    Txn {
        xid: u64,
        ts: i64,
        changes: Vec<WalChange>,
        commit: Lsn,
    },
    KeepAlive(Lsn),
}

impl WalEntry {
    fn end(&self) -> Lsn {
        match self {
            Self::Txn { commit, .. } | Self::KeepAlive(commit) => *commit,
        }
    }
}

#[derive(Debug)]
struct Db {
    catalog: BTreeMap<TableRef, TableSchema>,
    next_source_id: u64,
    rows: BTreeMap<TableRef, BTreeMap<Key, Row>>,
    /// Stalled transactions: the rows they changed, as they were before (`None`: absent).
    stalled: BTreeMap<u64, Vec<(TableRef, Key, Option<Row>)>>,
    /// Transactions still running, with the tables they write.
    open: BTreeMap<u64, Vec<TableRef>>,
    wal: Vec<WalEntry>,
    wal_end: Lsn,
    next_xid: u64,
    next_ts: i64,
    slot: Option<Lsn>,
    slot_lost: bool,
    added: Vec<TableRef>,
    retained: Option<Option<u64>>,
    faults: Vec<(Method, SourceError)>,
    hold_reads: bool,
    acknowledged: Vec<SourcePosition>,
    opened: Vec<SourcePosition>,
    reads: BTreeMap<TableRef, usize>,
    prepares: Vec<Option<SourcePosition>>,
}

/// The scripted source. Cloning shares the database.
#[derive(Debug, Clone)]
pub struct FakeSource {
    db: Arc<Mutex<Db>>,
    /// Wakes open streams when the log grows, and held reads.
    wake: Arc<Notify>,
}

fn lsn(p: &SourcePosition) -> Lsn {
    match p {
        SourcePosition::Pg(l) => *l,
        other => panic!("the fake source speaks PostgreSQL positions, not {other}"),
    }
}

impl FakeSource {
    /// A database holding `tables`, empty, with its log at `0/100`.
    pub fn new(tables: Vec<TableSchema>) -> Self {
        let mut db = Db {
            catalog: BTreeMap::new(),
            next_source_id: 1000,
            rows: BTreeMap::new(),
            stalled: BTreeMap::new(),
            open: BTreeMap::new(),
            wal: vec![],
            wal_end: Lsn(0x100),
            next_xid: 700,
            next_ts: 1_700_000_000_000_000,
            slot: None,
            slot_lost: false,
            added: vec![],
            retained: None,
            faults: vec![],
            hold_reads: false,
            acknowledged: vec![],
            opened: vec![],
            reads: BTreeMap::new(),
            prepares: vec![],
        };
        for t in tables {
            db.create(t);
        }
        Self {
            db: Arc::new(Mutex::new(db)),
            wake: Arc::new(Notify::new()),
        }
    }

    fn db(&self) -> MutexGuard<'_, Db> {
        self.db
            .lock()
            .expect("no test panics while holding the lock")
    }

    /// Rows present before the pipeline starts: in the table, not in the log.
    pub fn load(&self, table: &TableRef, rows: Vec<Row>) {
        let mut db = self.db();
        let schema = db.schema(table);
        for row in rows {
            let key = key_of(&schema, &row);
            db.rows.get_mut(table).expect("a table").insert(key, row);
        }
    }

    /// The schema the catalog has for `table` now, with its source id.
    pub fn schema(&self, table: &TableRef) -> TableSchema {
        self.db().schema(table)
    }

    /// Commits `txn`: the rows change and the stream delivers it. Returns its xid.
    pub fn commit(&self, txn: FakeTxn) -> u64 {
        let xid = self.db().commit(None, txn, false);
        self.wake.notify_waiters();
        xid
    }

    /// Commits `txn` into the log, but chunks don't see it until [`Self::release`]: a synchronous
    /// commit waiting for its standby.
    pub fn commit_stalled(&self, txn: FakeTxn) -> u64 {
        let xid = self.db().commit(None, txn, true);
        self.wake.notify_waiters();
        xid
    }

    /// The stalled transaction becomes visible.
    pub fn release(&self, xid: u64) {
        self.db()
            .stalled
            .remove(&xid)
            .expect("a stalled transaction");
    }

    /// A transaction that is running and writes `tables`: in flight, invisible, not in the log.
    pub fn open_writer(&self, tables: &[TableRef]) -> u64 {
        let mut db = self.db();
        let xid = db.next_xid;
        db.next_xid += 1;
        db.open.insert(xid, tables.to_vec());
        xid
    }

    /// The running transaction `xid` commits `txn`.
    pub fn commit_writer(&self, xid: u64, txn: FakeTxn) {
        let mut db = self.db();
        db.open.remove(&xid).expect("an open writer");
        db.commit(Some(xid), txn, false);
        drop(db);
        self.wake.notify_waiters();
    }

    /// The running transaction `xid` rolls back.
    pub fn abort_writer(&self, xid: u64) {
        self.db().open.remove(&xid).expect("an open writer");
    }

    /// A keep-alive at the log's end.
    pub fn keepalive(&self) {
        let mut db = self.db();
        db.wal_end = Lsn(db.wal_end.0 + 0x10);
        let at = db.wal_end;
        db.wal.push(WalEntry::KeepAlive(at));
        drop(db);
        self.wake.notify_waiters();
    }

    /// Changes `table`'s schema in the catalog. Stored rows lose dropped columns and read NULL in
    /// added ones. Keeps the source id.
    pub fn alter(&self, table: &TableRef, schema: TableSchema) {
        let mut db = self.db();
        let id = db.schema(table).source_id().expect("an id");
        let schema = schema.with_source_id(id);
        for row in db.rows.get_mut(table).expect("a table").values_mut() {
            *row = Row(schema
                .columns()
                .iter()
                .map(|c| {
                    let v = row.get(&c.name).cloned().unwrap_or(ColumnValue::Null);
                    (c.name.clone(), v)
                })
                .collect());
        }
        db.catalog.insert(table.clone(), schema);
    }

    /// Drops `table` from the catalog (it streams nothing more).
    pub fn drop_table(&self, table: &TableRef) {
        let mut db = self.db();
        db.catalog.remove(table);
        db.rows.remove(table);
    }

    /// Creates `schema`'s table (again): a new source id.
    pub fn create_table(&self, schema: TableSchema) {
        self.db().create(schema);
    }

    /// The source loses the slot's position: the next stream fails with `PositionUnreachable`,
    /// and the next `prepare` recreates the slot.
    pub fn lose_slot(&self) {
        self.db().slot_lost = true;
        self.wake.notify_waiters();
    }

    /// The tables the next resuming `prepare` reports in `Prepared::added`.
    pub fn report_added(&self, tables: &[TableRef]) {
        self.db().added = tables.to_vec();
    }

    /// What `retained_bytes` reports from now on (by default, the log from the slot to its end).
    pub fn set_retained(&self, bytes: Option<u64>) {
        self.db().retained = Some(bytes);
    }

    /// The next call of `method` fails with `error`.
    pub fn fail(&self, method: Method, error: SourceError) {
        self.db().faults.push((method, error));
        self.wake.notify_waiters();
    }

    /// Chunk reads wait, after taking their snapshot, until [`Self::release_reads`].
    pub fn hold_reads(&self) {
        self.db().hold_reads = true;
    }

    pub fn release_reads(&self) {
        self.db().hold_reads = false;
        self.wake.notify_waiters();
    }

    /// Every position acknowledged so far, in order.
    pub fn acknowledged(&self) -> Vec<SourcePosition> {
        self.db().acknowledged.clone()
    }

    /// Where each stream was opened, in order.
    pub fn opened(&self) -> Vec<SourcePosition> {
        self.db().opened.clone()
    }

    /// The checkpoint each `prepare` was called with.
    pub fn prepares(&self) -> Vec<Option<SourcePosition>> {
        self.db().prepares.clone()
    }

    /// Chunk reads of `table` so far.
    pub fn reads(&self, table: &TableRef) -> usize {
        self.db().reads.get(table).copied().unwrap_or(0)
    }

    /// The slot's confirmed position; `None` without a slot.
    pub fn slot(&self) -> Option<SourcePosition> {
        self.db().slot.map(SourcePosition::Pg)
    }

    /// The log's end.
    pub fn wal_end(&self) -> SourcePosition {
        SourcePosition::Pg(self.db().wal_end)
    }

    /// `table`'s visible rows in key order.
    pub fn rows(&self, table: &TableRef) -> Vec<Row> {
        self.db().visible(table).into_values().collect()
    }

    fn take_fault(&self, method: Method) -> Result<(), SourceError> {
        let mut db = self.db();
        match db.faults.iter().position(|(m, _)| *m == method) {
            Some(i) => Err(db.faults.remove(i).1),
            None => Ok(()),
        }
    }
}

impl Db {
    fn create(&mut self, schema: TableSchema) {
        let schema = schema.with_source_id(self.next_source_id);
        self.next_source_id += 1;
        self.rows.insert(schema.table().clone(), BTreeMap::new());
        self.catalog.insert(schema.table().clone(), schema);
    }

    fn schema(&self, table: &TableRef) -> TableSchema {
        self.catalog
            .get(table)
            .unwrap_or_else(|| panic!("no table {table}"))
            .clone()
    }

    fn held(&self, table: &TableRef, key: &Key) -> bool {
        self.stalled
            .values()
            .any(|c| c.iter().any(|(t, k, _)| t == table && k == key))
    }

    fn commit(&mut self, xid: Option<u64>, txn: FakeTxn, stalled: bool) -> u64 {
        let xid = xid.unwrap_or_else(|| {
            self.next_xid += 1;
            self.next_xid - 1
        });
        let ts = self.next_ts;
        self.next_ts += 1_000;
        let mut undo = vec![];
        let mut changes = vec![];
        let mut next = self.wal_end.0;
        let mut seq = || {
            next += 0x10;
            SourceSeq::Pg(Lsn(next))
        };
        for op in txn.ops {
            let mut write =
                |db: &mut Self, table: &TableRef, key: Key, row: Option<Row>| -> Option<Row> {
                    assert!(
                        !db.held(table, &key),
                        "{table}: a stalled transaction holds {key:?}; PostgreSQL would block"
                    );
                    let rows = db.rows.get_mut(table).expect("a table");
                    let old = match row {
                        Some(r) => rows.insert(key.clone(), r),
                        None => rows.remove(&key),
                    };
                    undo.push((table.clone(), key, old.clone()));
                    old
                };
            match op {
                FakeOp::Insert(t, row) => {
                    let schema = self.schema(&t);
                    write(self, &t, key_of(&schema, &row), Some(row.clone()));
                    changes.push(self.change(&t, Op::Insert, None, Some(row), seq()));
                }
                FakeOp::Update(t, row) => {
                    let schema = self.schema(&t);
                    let key = key_of(&schema, &row);
                    let stored = self.rows[&t].get(&key).cloned();
                    let merged = Row(row
                        .0
                        .iter()
                        .map(|(n, v)| match v {
                            ColumnValue::Unchanged => (
                                n.clone(),
                                stored
                                    .as_ref()
                                    .and_then(|s| s.get(n).cloned())
                                    .unwrap_or(ColumnValue::Null),
                            ),
                            v => (n.clone(), v.clone()),
                        })
                        .collect());
                    write(self, &t, key, Some(merged));
                    let before = identifier_values(&schema, &row);
                    changes.push(self.change(&t, Op::Update, before, Some(row), seq()));
                }
                FakeOp::Move(t, old, row) => {
                    let schema = self.schema(&t);
                    write(self, &t, key_of(&schema, &old), None);
                    write(self, &t, key_of(&schema, &row), Some(row.clone()));
                    changes.push(self.change(&t, Op::Update, Some(old), Some(row), seq()));
                }
                FakeOp::Delete(t, key) => {
                    let schema = self.schema(&t);
                    write(self, &t, key_of(&schema, &key), None);
                    changes.push(self.change(&t, Op::Delete, Some(key), None, seq()));
                }
                FakeOp::Truncate(tables) => {
                    for t in &tables {
                        let keys: Vec<Key> = self.rows[t].keys().cloned().collect();
                        for k in keys {
                            write(self, t, k, None);
                        }
                    }
                    changes.push(WalChange::Truncate(tables, seq()));
                }
            }
        }
        let commit = Lsn(next + 0x10);
        self.wal_end = commit;
        for c in &mut changes {
            if let WalChange::Change { event, .. } = c {
                event.xid = xid;
                event.commit_ts_micros = ts;
                event.source_pos = SourcePosition::Pg(commit);
            }
        }
        self.wal.push(WalEntry::Txn {
            xid,
            ts,
            changes,
            commit,
        });
        if stalled {
            self.stalled.insert(xid, undo);
        }
        xid
    }

    fn change(
        &self,
        table: &TableRef,
        op: Op,
        before: Option<Row>,
        after: Option<Row>,
        seq: SourceSeq,
    ) -> WalChange {
        WalChange::Change {
            event: CdcEvent {
                op,
                table: table.clone(),
                before,
                after,
                source_pos: SourcePosition::Pg(Lsn(0)),
                change_seq: seq,
                xid: 0,
                commit_ts_micros: 0,
            },
            columns: self.schema(table).columns().to_vec(),
        }
    }

    /// The rows a new snapshot sees: stalled transactions' changes undone.
    fn visible(&self, table: &TableRef) -> BTreeMap<Key, Row> {
        let mut rows = self.rows.get(table).cloned().unwrap_or_default();
        for changes in self.stalled.values() {
            for (t, key, old) in changes.iter().rev() {
                if t == table {
                    match old {
                        Some(r) => rows.insert(key.clone(), r.clone()),
                        None => rows.remove(key),
                    };
                }
            }
        }
        rows
    }

    fn snapshot(&self) -> XidSnapshot {
        let xip: Vec<u64> = self
            .stalled
            .keys()
            .chain(self.open.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        XidSnapshot {
            xmin: xip.first().copied().unwrap_or(self.next_xid),
            xmax: self.next_xid,
            xip,
        }
    }
}

/// What `augment` and `rescan` compare: columns by name, with their types.
fn shape(columns: &[ColumnSchema]) -> Vec<(String, data_avalanche_core::ColumnValueType)> {
    columns
        .iter()
        .map(|c| (c.name.clone(), c.value_type.clone()))
        .collect()
}

fn diff(cached: &TableSchema, now: &TableSchema) -> Vec<SchemaChange> {
    let mut changes = vec![];
    for c in cached.columns() {
        match now.column(&c.name) {
            None => changes.push(SchemaChange::DropColumn(c.name.clone())),
            Some(n) if n.value_type != c.value_type => changes.push(SchemaChange::ChangeType {
                column: c.name.clone(),
                from: c.clone(),
                to: n.clone(),
                narrowed: false,
            }),
            Some(_) => {}
        }
    }
    for c in now.columns() {
        if cached.column(&c.name).is_none() {
            changes.push(SchemaChange::AddColumn(c.clone()));
        }
    }
    if cached.identifier() != now.identifier() {
        changes.push(SchemaChange::IdentifierChanged);
    }
    changes
}

#[async_trait]
impl SourceConnector for FakeSource {
    async fn preflight(&self) -> Result<Preflight, SourceError> {
        self.take_fault(Method::Preflight)?;
        Ok(Preflight {
            server_version: "fake".into(),
            warnings: vec![],
        })
    }

    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
        self.take_fault(Method::ResolveTables)?;
        Ok(ResolvedTables {
            tables: self.db().catalog.values().cloned().collect(),
            ..ResolvedTables::default()
        })
    }

    async fn prepare(
        &self,
        checkpoint: Option<&SourcePosition>,
        _tables: &[TableRef],
    ) -> Result<Prepared, SourceError> {
        self.take_fault(Method::Prepare)?;
        let mut db = self.db();
        db.prepares.push(checkpoint.cloned());
        let checkpoint = checkpoint.map(lsn);
        let reason = match (checkpoint, db.slot) {
            (None, None) => Some(SnapshotReason::FirstStart),
            (None, Some(_)) => {
                return Err(SourceError::Permanent(
                    "replication slot exists but the pipeline has no state (an orphan)".into(),
                ));
            }
            (Some(_), None) => Some(SnapshotReason::PositionLost),
            (Some(_), Some(_)) if db.slot_lost => Some(SnapshotReason::PositionLost),
            (Some(at), Some(s)) if s > at => Some(SnapshotReason::PositionLost),
            (Some(_), Some(_)) => None,
        };
        Ok(match reason {
            None => Prepared {
                plan: StartPlan::Resume,
                added: std::mem::take(&mut db.added),
                warnings: vec![],
            },
            Some(reason) => {
                db.slot = Some(db.wal_end);
                db.slot_lost = false;
                Prepared {
                    plan: StartPlan::Snapshot {
                        from: SourcePosition::Pg(db.wal_end),
                        reason,
                    },
                    added: vec![],
                    warnings: vec![],
                }
            }
        })
    }

    async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
        self.take_fault(Method::ResetPosition)?;
        let mut db = self.db();
        db.slot = Some(db.wal_end);
        db.slot_lost = false;
        Ok(SourcePosition::Pg(db.wal_end))
    }

    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
        self.take_fault(Method::RetainedBytes)?;
        let db = self.db();
        Ok(match db.retained {
            Some(r) => r,
            None => db.slot.map(|s| db.wal_end.0.saturating_sub(s.0)),
        })
    }

    async fn current_position(&self) -> Result<SourcePosition, SourceError> {
        self.take_fault(Method::CurrentPosition)?;
        Ok(SourcePosition::Pg(self.db().wal_end))
    }

    async fn in_flight(
        &self,
    ) -> Result<Option<Vec<data_avalanche_core::InFlightTxn>>, SourceError> {
        self.take_fault(Method::InFlight)?;
        let db = self.db();
        let id = |t: &TableRef| db.catalog.get(t).and_then(TableSchema::source_id);
        let mut out: Vec<InFlightTxn> = db
            .stalled
            .iter()
            .map(|(xid, changes)| InFlightTxn {
                xid: *xid,
                tables: changes
                    .iter()
                    .filter_map(|(t, _, _)| id(t))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            })
            .collect();
        out.extend(db.open.iter().map(|(xid, tables)| InFlightTxn {
            xid: *xid,
            tables: tables.iter().filter_map(id).collect(),
        }));
        Ok(Some(out))
    }

    async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError> {
        self.take_fault(Method::ReadChunk)?;
        let (rows, visibility) = {
            let mut db = self.db();
            *db.reads.entry(table.table().clone()).or_default() += 1;
            let Some(now) = db.catalog.get(table.table()) else {
                return Err(SourceError::TableHalt {
                    table: table.table().clone(),
                    reason: HaltReason::TableDropped,
                });
            };
            if shape(now.columns()) != shape(table.columns()) {
                return Err(SourceError::Transient(format!(
                    "{} changed under the snapshot",
                    table.table()
                )));
            }
            let from = after.map(|c| key_of(table, &c.key));
            let rows: Vec<Row> = db
                .visible(table.table())
                .into_iter()
                .filter(|(k, _)| from.as_ref().is_none_or(|f| k > f))
                .take(limit)
                .map(|(_, r)| r)
                .collect();
            (rows, db.snapshot())
        };
        loop {
            let held = self.wake.notified();
            if !self.db().hold_reads {
                break;
            }
            held.await;
        }
        let next = rows.last().map(|r| KeysetCursor {
            key: identifier_values(table, r).expect("a key"),
            collation: None,
        });
        Ok(Chunk {
            rows,
            next,
            visibility: Some(ChunkVisibility::Xids(visibility)),
        })
    }

    async fn fetch_rows(&self, table: &TableSchema, keys: &[Row]) -> Result<Vec<Row>, SourceError> {
        self.take_fault(Method::FetchRows)?;
        let visible = self.db().visible(table.table());
        Ok(keys
            .iter()
            .filter_map(|k| visible.get(&key_of(table, k)).cloned())
            .collect())
    }

    async fn open_stream(
        &self,
        from: &SourcePosition,
        tables: &[TableSchema],
    ) -> Result<Box<dyn ChangeStream>, SourceError> {
        self.take_fault(Method::OpenStream)?;
        self.db().opened.push(from.clone());
        Ok(Box::new(FakeStream {
            source: self.clone(),
            after: lsn(from),
            next_entry: 0,
            pending: VecDeque::new(),
            known: tables
                .iter()
                .map(|t| (t.table().clone(), shape(t.columns())))
                .collect(),
        }))
    }

    async fn augment(
        &self,
        cached: &TableSchema,
        _hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
        self.take_fault(Method::Augment)?;
        let table = cached.table().clone();
        Ok(match self.db().catalog.get(&table) {
            None => (
                SchemaDelta {
                    table,
                    changes: vec![SchemaChange::TableDropped],
                },
                None,
            ),
            Some(now) => (
                SchemaDelta {
                    table,
                    changes: diff(cached, now),
                },
                Some(now.clone()),
            ),
        })
    }

    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
        self.take_fault(Method::Rescan)?;
        let db = self.db();
        let mut report = RescanReport::default();
        for k in known {
            match db.catalog.get(k.table()) {
                Some(now) if now.source_id() != k.source_id() && k.source_id().is_some() => {
                    report.dropped.push(k.table().clone())
                }
                None => report.dropped.push(k.table().clone()),
                Some(now) if shape(now.columns()) != shape(k.columns()) => {
                    report.changed.push(now.clone())
                }
                Some(_) => {}
            }
        }
        Ok(report)
    }

    async fn teardown(&self) -> Result<(), SourceError> {
        Ok(())
    }
}

/// The stream of one `open_stream`: the log after its start, then whatever is committed later.
struct FakeStream {
    source: FakeSource,
    after: Lsn,
    next_entry: usize,
    pending: VecDeque<StreamItem>,
    known: BTreeMap<TableRef, Vec<(String, data_avalanche_core::ColumnValueType)>>,
}

impl FakeStream {
    /// Queues the next log entry past the start; false when there is none yet.
    fn fill(&mut self, db: &Db) -> bool {
        while let Some(entry) = db.wal.get(self.next_entry) {
            self.next_entry += 1;
            if entry.end() <= self.after {
                continue;
            }
            match entry {
                WalEntry::KeepAlive(at) => {
                    self.pending
                        .push_back(StreamItem::KeepAlive(SourcePosition::Pg(*at)));
                }
                WalEntry::Txn {
                    xid,
                    ts,
                    changes,
                    commit,
                } => {
                    self.pending.push_back(StreamItem::Begin {
                        xid: *xid,
                        commit_ts_micros: *ts,
                    });
                    for c in changes {
                        match c {
                            WalChange::Change { event, columns } => {
                                let Some(known) = self.known.get_mut(&event.table) else {
                                    continue;
                                };
                                let now = shape(columns);
                                if *known != now {
                                    *known = now;
                                    self.pending.push_back(StreamItem::SchemaSignal {
                                        table: event.table.clone(),
                                        seq: event.change_seq,
                                        hint: SchemaHint::Columns(columns.clone()),
                                    });
                                }
                                self.pending.push_back(StreamItem::Change(event.clone()));
                            }
                            WalChange::Truncate(tables, seq) => {
                                let tables: Vec<TableRef> = tables
                                    .iter()
                                    .filter(|t| self.known.contains_key(t))
                                    .cloned()
                                    .collect();
                                if !tables.is_empty() {
                                    self.pending
                                        .push_back(StreamItem::Truncate { tables, seq: *seq });
                                }
                            }
                        }
                    }
                    self.pending
                        .push_back(StreamItem::Commit(TransactionBoundary {
                            xid: *xid,
                            commit_pos: SourcePosition::Pg(*commit),
                            commit_ts_micros: *ts,
                        }));
                }
            }
            return true;
        }
        false
    }
}

#[async_trait]
impl ChangeStream for FakeStream {
    async fn next(&mut self) -> Result<StreamItem, SourceError> {
        let source = self.source.clone();
        loop {
            let wake = source.wake.notified();
            {
                let mut db = source.db();
                if db.slot_lost {
                    return Err(SourceError::PositionUnreachable {
                        position: self.after.to_string(),
                        detail: "the fake slot lost its position".into(),
                    });
                }
                if let Some(item) = self.pending.pop_front() {
                    return Ok(item);
                }
                if let Some(i) = db.faults.iter().position(|(m, _)| *m == Method::Next) {
                    return Err(db.faults.remove(i).1);
                }
                if self.fill(&db) {
                    continue;
                }
            }
            wake.await;
        }
    }

    async fn acknowledge(&mut self, durable: &SourcePosition) -> Result<(), SourceError> {
        let mut db = self.source.db();
        let at = lsn(durable);
        db.slot = Some(db.slot.map_or(at, |s| s.max(at)));
        db.acknowledged.push(durable.clone());
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<(), SourceError> {
        Ok(())
    }
}
```

Change `crates/data-avalanche-testkit/src/memory_sink.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -17,6 +17,7 @@ use data_avalanche_core::{
     SinkCapabilities, SinkError, SinkKind, SinkTableState, TableChangeSet, TableRef, TableSpec,
     TransformKind, UNCHANGED_MASK_PREFIX, WriteMode, added_column_field,
 };
+use tokio::sync::watch;
 
 #[derive(Debug)]
 struct MemTable {
@@ -157,11 +158,42 @@ fn permanent(reason: String) -> SinkError {
     SinkError::Permanent { reason }
 }
 
+/// The sink call a [`MemorySink::fail`] fault fires in.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum SinkOp {
+    TableState,
+    EnsureTable,
+    Evolve,
+    Commit,
+}
+
+/// A failure the next matching calls report, before they change anything.
+struct Fault {
+    op: SinkOp,
+    /// `None`: any table.
+    table: Option<TableRef>,
+    remaining: usize,
+    error: Box<dyn Fn() -> SinkError + Send>,
+}
+
+impl std::fmt::Debug for Fault {
+    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
+        f.debug_struct("Fault")
+            .field("op", &self.op)
+            .field("table", &self.table)
+            .field("remaining", &self.remaining)
+            .finish_non_exhaustive()
+    }
+}
+
 /// The reference sink. Cloning shares the tables.
 #[derive(Debug, Clone)]
 pub struct MemorySink {
     capabilities: SinkCapabilities,
     tables: Arc<Mutex<BTreeMap<TableRef, MemTable>>>,
+    faults: Arc<Mutex<Vec<Fault>>>,
+    /// While set, commits wait before they start.
+    held: Arc<watch::Sender<bool>>,
 }
 
 impl Default for MemorySink {
@@ -188,9 +220,70 @@ impl MemorySink {
                 .into(),
             },
             tables: Arc::default(),
+            faults: Arc::default(),
+            held: Arc::new(watch::Sender::new(false)),
         }
     }
 
+    /// The next `times` calls of `op` (for `table`, or any table) fail with `error()` and change
+    /// nothing.
+    pub fn fail(
+        &self,
+        op: SinkOp,
+        table: Option<&TableRef>,
+        times: usize,
+        error: impl Fn() -> SinkError + Send + 'static,
+    ) {
+        self.faults
+            .lock()
+            .expect("no test panics while holding the lock")
+            .push(Fault {
+                op,
+                table: table.cloned(),
+                remaining: times,
+                error: Box::new(error),
+            });
+    }
+
+    fn fault(&self, op: SinkOp, table: &TableRef) -> Result<(), SinkError> {
+        let mut faults = self
+            .faults
+            .lock()
+            .expect("no test panics while holding the lock");
+        let Some(f) = faults
+            .iter_mut()
+            .find(|f| f.op == op && f.remaining > 0 && f.table.as_ref().is_none_or(|t| t == table))
+        else {
+            return Ok(());
+        };
+        f.remaining -= 1;
+        Err((f.error)())
+    }
+
+    /// Commits wait, before they change anything, until [`Self::release_commits`].
+    pub fn hold_commits(&self) {
+        self.held.send_replace(true);
+    }
+
+    pub fn release_commits(&self) {
+        self.held.send_replace(false);
+    }
+
+    /// A newer owner (`epoch`) has written `table`: this pipeline's next commit with an older
+    /// epoch is `Fenced`.
+    pub fn take_over(&self, table: &TableRef, epoch: u64) {
+        let mut tables = self.tables();
+        let t = tables.get_mut(table).expect("a table");
+        if let Some(m) = &mut t.marker {
+            m.owner_epoch = epoch;
+        }
+    }
+
+    /// The marker of the table's last commit.
+    pub fn marker(&self, table: &TableRef) -> Option<CommitMarker> {
+        self.tables().get(table).and_then(|t| t.marker.clone())
+    }
+
     fn tables(&self) -> MutexGuard<'_, BTreeMap<TableRef, MemTable>> {
         self.tables
             .lock()
@@ -220,6 +313,7 @@ impl Sink for MemorySink {
     }
 
     async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError> {
+        self.fault(SinkOp::TableState, table)?;
         Ok(match self.tables().get(table) {
             Some(t) => SinkTableState {
                 exists: true,
@@ -235,6 +329,7 @@ impl Sink for MemorySink {
     }
 
     async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError> {
+        self.fault(SinkOp::EnsureTable, &spec.table)?;
         self.tables()
             .entry(spec.table.clone())
             .or_insert_with(|| MemTable {
@@ -252,6 +347,7 @@ impl Sink for MemorySink {
         table: &TableRef,
         delta: &SchemaDelta,
     ) -> Result<EvolveOutcome, SinkError> {
+        self.fault(SinkOp::Evolve, table)?;
         let mut tables = self.tables();
         let t = tables.get_mut(table).ok_or_else(|| SinkError::NoTable {
             table: table.clone(),
@@ -306,6 +402,9 @@ impl Sink for MemorySink {
         cs: TableChangeSet,
         marker: CommitMarker,
     ) -> Result<CommitOutcome, SinkError> {
+        let mut held = self.held.subscribe();
+        let _ = held.wait_for(|h| !h).await;
+        self.fault(SinkOp::Commit, &cs.table)?;
         cs.validate()?;
         if marker.committed_position != cs.position.end {
             return Err(permanent(format!(
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-testkit --lib
```

Expected:

PASS:

```text
test result: ok. 17 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(testkit): a scripted fake source and sink faults"
```


### Task 5: Worker settings, the per-table snapshot decision, errors and hooks

**Files:**
- Create: `crates/data-avalanche-pipeline/src/settings.rs`, `…/decide.rs`, `…/hooks.rs`
- Modify: `crates/data-avalanche-pipeline/src/error.rs`, `…/lib.rs`, `…/test_support.rs`, `…/Cargo.toml`

**Interfaces:**
- Consumes: the resolved `Pipeline` (config, M1); `SnapshotMode`; the accumulator's
  `AccumulatorError` (M3).
- Produces:
  - `Settings::new(&Pipeline)` (crate): flush policy, limits, `chunk_size`, `parallelism`,
    `snapshot_timeout`, `re_query`, `evolution`, `rescan_interval`, `max_wal_lag_bytes`,
    `slot_intent`, the sink kind, and `mode(t)`, `is_dump(t)`, `partition_by(t)`,
    `delete_extra(schema)`.
  - `decide(mode, Plan, Facts) -> (Decision, Option<String>)` (crate):
    `Plan::{Resume, Fresh(SnapshotReason)}`, `Facts { tracked_before, in_progress,
    sink_has_table, added }`, `Decision::{Stream, Snapshot { resume, dump }, Stop}`, and a gap
    warning (Q-M6a-1).
  - `WorkerError::{Refused, Source, Contended { waited, detail }, Sink, State, Fenced { table,
    current_epoch }, LeaseLost, TransactionTooLarge { bytes, limit }, WalLag { lag, limit },
    Accumulator, Crashed(HookPoint)}`; `From<AccumulatorError>` maps
    `TransactionTooLarge` to its own variant.
  - `CommitGuard` (`may_commit()`), `Unguarded`; `Hooks` (`at(&HookPoint) -> HookAction`),
    `NoHooks`; `HookPoint::{SinkCommitted { table }, CheckpointPersisted { position },
    ChunkEmitted { table }}`, `HookAction::{Continue, Crash}`.

Pure pieces first. `decide` is the per-table table of Data-Flow Phase 0. Under `initial`,
a table is snapshotted when it wasn't tracked before, when its snapshot is in progress, when it
was added, or when the plan is fresh. A `dump_*` table stops once its snapshot is done.
`never` never snapshots and warns of a gap it doesn't fill. `CommitGuard` is the seam M6b's
lease renewer implements. `Hooks` are the crash points the recovery tests use.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-pipeline/Cargo.toml b/crates/data-avalanche-pipeline/Cargo.toml
--- a/crates/data-avalanche-pipeline/Cargo.toml
+++ b/crates/data-avalanche-pipeline/Cargo.toml
@@ -13,8 +13,13 @@ workspace = true
 arrow-array.workspace = true
 arrow-buffer.workspace = true
 arrow-schema.workspace = true
+data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
+data-avalanche-state.workspace = true
+futures-util.workspace = true
+getrandom.workspace = true
 thiserror.workspace = true
+tokio = { workspace = true, features = ["sync"] }
 
 [dev-dependencies]
 data-avalanche-testkit.workspace = true
```

Create `crates/data-avalanche-pipeline/src/decide.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use SnapshotMode as M;

    const RESUME: Plan = Plan::Resume;
    const FIRST: Plan = Plan::Fresh(SnapshotReason::FirstStart);
    const LOST: Plan = Plan::Fresh(SnapshotReason::PositionLost);
    const ADOPTED: Plan = Plan::Fresh(SnapshotReason::AdoptedOrphan);

    fn facts(tracked_before: bool, in_progress: bool, sink_has_table: bool, added: bool) -> Facts {
        Facts {
            tracked_before,
            in_progress,
            sink_has_table,
            added,
        }
    }

    fn snap(resume: bool, dump: bool) -> Decision {
        Decision::Snapshot { resume, dump }
    }

    /// The Data-Flow Phase 0 table, row by row, with the recovery cases.
    #[test]
    fn the_phase_0_table() {
        let new = facts(false, false, false, false);
        let known = facts(true, false, true, false);
        let cases = [
            // always: snapshot, then stream, either way
            (M::Always, RESUME, known, snap(false, false)),
            (M::Always, FIRST, new, snap(false, false)),
            // never: stream
            (M::Never, FIRST, new, Decision::Stream),
            (M::Never, RESUME, known, Decision::Stream),
            // initial: no confirmed offset → snapshot; confirmed → stream
            (M::Initial, FIRST, new, snap(false, false)),
            (M::Initial, RESUME, new, snap(false, false)),
            (M::Initial, RESUME, known, Decision::Stream),
            (M::Initial, LOST, known, snap(false, false)),
            (M::Initial, ADOPTED, known, snap(false, false)),
            (
                M::Initial,
                RESUME,
                facts(true, false, true, true),
                snap(false, false),
            ),
            // dump_always: snapshot, then stop, either way
            (M::DumpAlways, RESUME, known, snap(false, true)),
            (M::DumpAlways, FIRST, new, snap(false, true)),
            // dump_initial: none → snapshot+stop; confirmed + sink has → stop; sink lacks → snapshot
            (M::DumpInitial, FIRST, new, snap(false, true)),
            (M::DumpInitial, RESUME, known, Decision::Stop),
            (
                M::DumpInitial,
                RESUME,
                facts(true, false, false, false),
                snap(false, true),
            ),
            (M::DumpInitial, LOST, known, Decision::Stop),
        ];
        for (mode, plan, f, want) in cases {
            assert_eq!(decide(mode, plan, f).0, want, "{mode:?} {plan:?} {f:?}");
        }
    }

    #[test]
    fn an_interrupted_snapshot_resumes() {
        let interrupted = facts(true, true, true, false);
        for (mode, dump) in [
            (M::Initial, false),
            (M::Always, false),
            (M::DumpAlways, true),
            (M::DumpInitial, true),
        ] {
            assert_eq!(
                decide(mode, RESUME, interrupted).0,
                snap(true, dump),
                "{mode:?}"
            );
        }
        assert_eq!(
            decide(M::Never, RESUME, interrupted).0,
            Decision::Stream,
            "never doesn't snapshot, not even to finish one"
        );
    }

    #[test]
    fn never_warns_of_a_gap_it_doesnt_fill() {
        let known = facts(true, false, true, false);
        for plan in [LOST, ADOPTED] {
            let (d, w) = decide(M::Never, plan, known);
            assert_eq!(d, Decision::Stream);
            assert!(w.unwrap().contains("position was lost"));
        }
        let (_, w) = decide(M::Never, RESUME, facts(true, false, true, true));
        assert!(w.unwrap().contains("added to the stream"));
        assert_eq!(decide(M::Never, FIRST, Facts::default()).1, None);
        assert_eq!(decide(M::Initial, LOST, known).1, None);
    }
}
```

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -2,17 +2,31 @@
 //! change events by key as they arrive, and the accumulator that tracks transactions and decides
 //! when to flush.
 //!
-//! Nothing here performs I/O or reads a clock: the orchestrator passes events in, reads change
-//! sets out and hands them to a [`data_avalanche_core::Sink`].
+//! The accumulator performs no I/O and reads no clock. The pipeline worker drives it: it reads
+//! the source, runs the snapshots, hands change sets to the sink and persists the checkpoint.
 
 pub mod accumulator;
 pub mod batch;
 pub mod buffer;
+#[allow(
+    dead_code,
+    reason = "the worker reads it (Task 11); the allow goes there"
+)]
+mod decide;
 pub mod error;
+pub mod hooks;
 pub mod policy;
+#[allow(
+    dead_code,
+    reason = "the worker reads it (Task 11); the allow goes there"
+)]
+mod settings;
+#[cfg(test)]
+mod test_support;
 
 pub use accumulator::{Accumulator, Flush};
 pub use batch::{Lineage, MAX_BATCH_BYTES};
 pub use buffer::{CollapsedRow, RowKind, TableBuffer};
-pub use error::AccumulatorError;
+pub use error::{AccumulatorError, WorkerError};
+pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
 pub use policy::{FlushPolicy, FlushReason, TxnLimits};
```

Create `crates/data-avalanche-pipeline/src/settings.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V};

    use super::*;
    use crate::test_support::pipeline;

    #[test]
    fn settings_follow_the_pipeline() {
        let p = pipeline(
            r#"
      snapshots: { mode: initial, parallelism: 3, chunk_size: 500, overrides: { public.big: never } }
      partitioning: { public.orders: [ { column: region, transform: identity }, { column: _ts, transform: identity } ] }"#,
            "max_transaction_batch_rows: 1000, flush: { seconds: 0 }",
            "heartbeat: 5s, max_wal_lag_bytes: 1GiB",
        );
        let s = Settings::new(&p);
        assert_eq!(s.pipeline_id, "p");
        assert_eq!(s.sink_kind, SinkKind::Delta);
        assert_eq!(s.lineage.source_db, "127.0.0.1/avalanche");
        assert_eq!(s.flush.seconds, Some(0));
        assert_eq!(s.limits.batch_rows, Some(1000));
        assert_eq!((s.parallelism, s.chunk_size), (3, 500));
        assert_eq!(s.heartbeat, Duration::from_secs(5));
        assert_eq!(s.max_wal_lag_bytes, Some(1 << 30));
        assert_eq!(s.rescan_interval, Some(Duration::from_secs(3600)));
        assert_eq!(s.slot_intent.as_deref(), Some("p_slot"));
        let orders = TableRef::new("public", "orders");
        assert_eq!(s.mode(&orders), SnapshotMode::Initial);
        assert_eq!(s.mode(&TableRef::new("public", "big")), SnapshotMode::Never);
        assert_eq!(s.partition_by(&orders).len(), 2);
        let schema = TableSchema::new(
            orders,
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("region", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        assert_eq!(
            s.delete_extra(&schema),
            ["region"],
            "_ts isn't a row column"
        );
    }

    #[test]
    fn require_existing_records_no_slot_intent() {
        let p = pipeline(
            "",
            "",
            "slot_management: require_existing, auto_cleanup: false",
        );
        assert_eq!(Settings::new(&p).slot_intent, None);
    }
}
```

Create `crates/data-avalanche-pipeline/src/test_support.rs`:

```rust
//! Fixtures for the crate's unit tests.

use std::collections::BTreeMap;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};

/// A PostgreSQL → Delta pipeline `p` reading `public.*`, with more `tables` keys (YAML lines
/// indented six spaces), more `sink` and `postgres` keys (YAML flow entries).
pub(crate) fn pipeline(tables: &str, sink: &str, postgres: &str) -> Pipeline {
    let sep = |s: &str| {
        if s.is_empty() {
            String::new()
        } else {
            format!(", {s}")
        }
    };
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: p
    source:
      host: 127.0.0.1
      port: 55418
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: {{ publication: p_pub, slot: p_slot{postgres} }}
    tables:
      include: [public.*]{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}}{sink} }}
"#,
        postgres = sep(postgres),
        sink = sep(sink),
    );
    load_str(
        &yaml,
        &BTreeMap::new(),
        &FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
    .config
    .pipelines
    .into_values()
    .next()
    .expect("one pipeline")
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

FAIL to compile (18 errors); the first:

```text
error[E0583]: file not found for module `hooks`
error[E0432]: unresolved import `SnapshotMode`
error[E0432]: unresolved import `error::WorkerError`
error[E0425]: cannot find type `Plan` in this scope
error[E0425]: cannot find type `Facts` in this scope
error[E0422]: cannot find struct, variant or union type `Facts` in this scope
… and 12 more
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-pipeline/src/decide.rs`, above the test module:

```rust
//! Data-Flow Phase 0 step 2: whether a table snapshots at this start, by its `snapshot_mode`
//! (System-Overview §Data snapshotting) and what the start found.

use data_avalanche_config::SnapshotMode;
use data_avalanche_core::SnapshotReason;

/// How the stream starts, as `prepare` decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plan {
    /// From the pipeline's checkpoint.
    Resume,
    /// From a new position (the slot was created or recreated).
    Fresh(SnapshotReason),
}

/// What the start knows about one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Facts {
    /// The pipeline tracked it before (its schema is cached): it "has a confirmed offset".
    pub tracked_before: bool,
    /// Its snapshot document says `in_progress`: a snapshot to resume from its cursor.
    pub in_progress: bool,
    /// The sink has the table (`table_state().exists`, before `ensure_table`).
    pub sink_has_table: bool,
    /// `prepare` began publishing it at this start (`Prepared::added`): its changes before now
    /// never reached the stream.
    pub added: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Stream it, without a snapshot.
    Stream,
    /// Snapshot it while streaming, from its cursor when `resume`. A `dump` stops once its
    /// snapshot is done (it isn't streamed further).
    Snapshot { resume: bool, dump: bool },
    /// Neither: a `dump_initial` table the sink already has.
    Stop,
}

/// The decision, and a warning when `never` leaves a gap: the position was lost or the table was
/// added to the stream at this start, and `never` takes no snapshot (Q-M6a-1).
pub(crate) fn decide(mode: SnapshotMode, plan: Plan, f: Facts) -> (Decision, Option<String>) {
    let snapshot = |dump| Decision::Snapshot {
        resume: f.in_progress,
        dump,
    };
    let recovery = matches!(
        plan,
        Plan::Fresh(SnapshotReason::PositionLost | SnapshotReason::AdoptedOrphan)
    );
    match mode {
        SnapshotMode::Never => {
            let gap = if recovery {
                Some("the stream position was lost and recreated".to_string())
            } else if f.added {
                Some("the table was added to the stream at this start".to_string())
            } else {
                None
            };
            let warning = gap.map(|why| {
                format!(
                    "{why}, and snapshot mode never takes no snapshot: changes before now are missing; snapshot the table on demand"
                )
            });
            (Decision::Stream, warning)
        }
        SnapshotMode::Initial
            if f.in_progress || !f.tracked_before || f.added || matches!(plan, Plan::Fresh(_)) =>
        {
            (snapshot(false), None)
        }
        SnapshotMode::Initial => (Decision::Stream, None),
        SnapshotMode::Always => (snapshot(false), None),
        SnapshotMode::DumpAlways => (snapshot(true), None),
        SnapshotMode::DumpInitial if f.in_progress || !f.tracked_before || !f.sink_has_table => {
            (snapshot(true), None)
        }
        SnapshotMode::DumpInitial => (Decision::Stop, None),
    }
}
```

Change `crates/data-avalanche-pipeline/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/error.rs b/crates/data-avalanche-pipeline/src/error.rs
--- a/crates/data-avalanche-pipeline/src/error.rs
+++ b/crates/data-avalanche-pipeline/src/error.rs
@@ -1,6 +1,13 @@
-use data_avalanche_core::{EventShapeError, HaltReason, PositionError, TableRef};
+use std::time::Duration;
+
+use data_avalanche_core::{
+    EventShapeError, HaltReason, PositionError, SinkError, SourceError, TableRef,
+};
+use data_avalanche_state::StateError;
 use thiserror::Error;
 
+use crate::hooks::HookPoint;
+
 #[derive(Debug, Error)]
 pub enum AccumulatorError {
     /// The table can't continue; the pipeline halts it (Data-Flow §Halt).
@@ -36,3 +43,81 @@ pub enum AccumulatorError {
     )]
     TransactionTooLarge { bytes: usize, limit: u64 },
 }
+
+/// Why a pipeline worker stopped (Data-Flow §Error Paths). A transient source or sink failure
+/// never ends the worker: it retries on its own.
+#[derive(Debug, Error)]
+pub enum WorkerError {
+    /// Phase 0 refused the pipeline (config §13.4, D-V1.4, a capability the sink lacks): an
+    /// operator must change the config or the state.
+    #[error("the pipeline is refused: {0}")]
+    Refused(String),
+    /// A permanent source failure: configuration, privilege, server setting, protocol.
+    #[error(transparent)]
+    Source(SourceError),
+    /// The source's stream position stayed held by another reader for the whole retry window.
+    #[error("the stream position stayed held by another reader for {waited:?}: {detail}")]
+    Contended { waited: Duration, detail: String },
+    /// A sink failure no table halt contains: the sink lost a table it created, or refused a
+    /// change set as malformed (a pipeline bug).
+    #[error(transparent)]
+    Sink(SinkError),
+    #[error(transparent)]
+    State(#[from] StateError),
+    /// A newer owner wrote the table; this worker must stop (Clustering §19).
+    #[error("table {table} has a newer owner (epoch {current_epoch}): this worker stops")]
+    Fenced { table: TableRef, current_epoch: u64 },
+    /// The commit guard refused a commit: the lease is no longer held.
+    #[error("the lease is no longer held: this worker stops before its next commit")]
+    LeaseLost,
+    /// One transaction outgrew `max_transaction_buffer_bytes` with splitting off
+    /// (Large_Transaction §2).
+    #[error(
+        "a transaction buffers {bytes} bytes, over max_transaction_buffer_bytes {limit}; set max_transaction_batch_rows to split it"
+    )]
+    TransactionTooLarge { bytes: usize, limit: u64 },
+    /// The slot retains more WAL than `max_wal_lag_bytes` (`Postgres_Slot…` D-15.4).
+    #[error("the slot retains {lag} bytes of WAL, over max_wal_lag_bytes {limit}")]
+    WalLag { lag: u64, limit: u64 },
+    /// The stream or the accumulator broke an invariant: a bug.
+    #[error(transparent)]
+    Accumulator(AccumulatorError),
+    /// A test hook stopped the worker (`HookAction::Crash`).
+    #[error("crashed at {0:?}")]
+    Crashed(HookPoint),
+}
+
+impl From<AccumulatorError> for WorkerError {
+    fn from(e: AccumulatorError) -> Self {
+        match e {
+            AccumulatorError::TransactionTooLarge { bytes, limit } => {
+                Self::TransactionTooLarge { bytes, limit }
+            }
+            e => Self::Accumulator(e),
+        }
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+
+    #[test]
+    fn a_transaction_too_large_is_its_own_stop() {
+        let e = WorkerError::from(AccumulatorError::TransactionTooLarge {
+            bytes: 2048,
+            limit: 1024,
+        });
+        assert!(matches!(
+            e,
+            WorkerError::TransactionTooLarge {
+                bytes: 2048,
+                limit: 1024
+            }
+        ));
+        let e = WorkerError::from(AccumulatorError::Protocol {
+            detail: "nested".into(),
+        });
+        assert!(matches!(e, WorkerError::Accumulator(_)), "{e}");
+    }
+}
```

Create `crates/data-avalanche-pipeline/src/hooks.rs`:

```rust
//! What the worker asks of its owner: whether it may still commit (the lease), and, in tests, where
//! to stop as if the process died.

use std::fmt::Debug;

use data_avalanche_core::{SourcePosition, TableRef};

/// Asked before every sink commit. M6b's lease renewer answers `false` once the lease's renewal
/// safety deadline passed (Clustering §19): the worker then stops with `LeaseLost` and writes
/// nothing more.
pub trait CommitGuard: Send + Sync + Debug {
    fn may_commit(&self) -> bool;
}

/// Always commits: a worker that no lease guards (tests, and M6a's single worker).
#[derive(Debug, Clone, Copy, Default)]
pub struct Unguarded;

impl CommitGuard for Unguarded {
    fn may_commit(&self) -> bool {
        true
    }
}

/// A point just after the worker made something durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookPoint {
    /// One table's change set is committed to the sink; the checkpoint isn't persisted yet.
    SinkCommitted { table: TableRef },
    /// The checkpoint is persisted; the stream isn't acknowledged yet.
    CheckpointPersisted { position: SourcePosition },
    /// A snapshot chunk of the table is in the accumulator; its cursor isn't persisted yet.
    ChunkEmitted { table: TableRef },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HookAction {
    #[default]
    Continue,
    /// Stop now, as if the process died: the worker returns `WorkerError::Crashed` and writes
    /// nothing more.
    Crash,
}

/// Test hooks; the default does nothing.
pub trait Hooks: Send + Sync + Debug {
    fn at(&self, point: &HookPoint) -> HookAction {
        let _ = point;
        HookAction::Continue
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoHooks;

impl Hooks for NoHooks {}
```

Add the implementation to `crates/data-avalanche-pipeline/src/settings.rs`, above the test module:

```rust
//! The worker's settings, read once from the resolved pipeline (config §9).

use std::collections::BTreeMap;
use std::time::Duration;

use data_avalanche_config::{
    PartitionTransform as ConfigTransform, Pipeline, SinkType, SnapshotMode, SourceType,
    ToastStrategy, WriteMode as ConfigWriteMode,
};
use data_avalanche_core::{
    PartitionField, PartitionTransform, SinkKind, TableRef, TableSchema, WriteMode,
};

use crate::{FlushPolicy, Lineage, TxnLimits};

/// Status interval and stall detection when the source has no heartbeat setting.
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub pipeline_id: String,
    pub sink_kind: SinkKind,
    pub lineage: Lineage,
    pub flush: FlushPolicy,
    pub limits: TxnLimits,
    /// Rows handed to the sink writer and not yet durable, at most (`buffer_capacity_rows`).
    pub buffer_rows: u64,
    pub write_mode: WriteMode,
    pub parallelism: usize,
    pub chunk_size: usize,
    /// `source.timeouts.snapshot`: one table's snapshot within one run.
    pub snapshot_timeout: Duration,
    /// `source.postgres.heartbeat`: the WAL-lag check runs at this interval; a stream with no
    /// item for three of them is stalled.
    pub heartbeat: Duration,
    pub max_wal_lag_bytes: Option<u64>,
    /// Channel D; `None` when `rescan_interval` is 0.
    pub rescan_interval: Option<Duration>,
    pub evolution: bool,
    /// `toast_strategy: re_query`.
    pub re_query: bool,
    /// PostgreSQL `slot_management: managed`: the slot name, whose creation intent a first start
    /// records. `None` when the pipeline never creates the slot.
    pub slot_intent: Option<String>,
    /// PostgreSQL: the slot holding the checkpoint (`replication/slot.json`).
    pub slot: Option<String>,
    mode: SnapshotMode,
    overrides: BTreeMap<String, SnapshotMode>,
    partitioning: BTreeMap<String, Vec<PartitionField>>,
    source: SourceType,
}

fn transform(t: ConfigTransform) -> PartitionTransform {
    match t {
        ConfigTransform::Identity => PartitionTransform::Identity,
        ConfigTransform::Bucket { n } => PartitionTransform::Bucket { n },
        ConfigTransform::Truncate { width } => PartitionTransform::Truncate { width },
        ConfigTransform::Year => PartitionTransform::Year,
        ConfigTransform::Month => PartitionTransform::Month,
        ConfigTransform::Day => PartitionTransform::Day,
        ConfigTransform::Hour => PartitionTransform::Hour,
    }
}

impl Settings {
    pub fn new(p: &Pipeline) -> Self {
        let pg = p.source.postgres.as_ref();
        Self {
            pipeline_id: p.id.clone(),
            sink_kind: match p.sink.kind {
                SinkType::Delta => SinkKind::Delta,
                SinkType::Iceberg => SinkKind::Iceberg,
            },
            lineage: Lineage {
                source_db: format!("{}/{}", p.source.host, p.source.db),
                pipeline: p.name.clone(),
            },
            flush: FlushPolicy {
                seconds: p.sink.flush.seconds,
                rows: p.sink.flush.rows,
                bytes: p.sink.flush.bytes,
            },
            limits: TxnLimits {
                batch_rows: p.sink.max_transaction_batch_rows,
                buffer_bytes: p.sink.max_transaction_buffer_bytes,
            },
            buffer_rows: p.sink.buffer_capacity_rows,
            write_mode: match p.sink.write_mode {
                ConfigWriteMode::Mirror => WriteMode::Mirror,
                ConfigWriteMode::Changelog => WriteMode::Changelog,
            },
            parallelism: p.tables.snapshots.parallelism.max(1) as usize,
            chunk_size: usize::try_from(p.tables.snapshots.chunk_size)
                .unwrap_or(usize::MAX)
                .max(1),
            snapshot_timeout: p.source.timeouts.snapshot,
            heartbeat: pg.map_or(DEFAULT_HEARTBEAT, |pg| pg.heartbeat),
            max_wal_lag_bytes: pg.and_then(|pg| pg.max_wal_lag_bytes),
            rescan_interval: (!p.schema_evolution.rescan_interval.is_zero())
                .then_some(p.schema_evolution.rescan_interval),
            evolution: p.schema_evolution.enabled,
            re_query: p.source.toast_strategy == ToastStrategy::ReQuery,
            slot_intent: pg
                .filter(|pg| pg.slot_management == data_avalanche_config::SlotManagement::Managed)
                .map(|pg| pg.slot.clone()),
            slot: pg.map(|pg| pg.slot.clone()),
            mode: p.tables.snapshots.mode,
            overrides: p.tables.snapshots.overrides.clone(),
            partitioning: p
                .tables
                .partitioning
                .iter()
                .map(|(k, fields)| {
                    let fields = fields
                        .iter()
                        .map(|f| PartitionField {
                            column: f.column.clone(),
                            transform: transform(f.transform),
                        })
                        .collect();
                    (k.clone(), fields)
                })
                .collect(),
            source: p.source.kind,
        }
    }

    /// The table's key in the `tables` maps: `schema.table` for PostgreSQL, the bare name for
    /// MySQL (V-064).
    fn key(&self, t: &TableRef) -> String {
        match self.source {
            SourceType::Postgres => format!("{}.{}", t.schema, t.name),
            SourceType::Mysql => t.name.clone(),
        }
    }

    /// `snapshots.mode`, or the table's override.
    pub fn mode(&self, t: &TableRef) -> SnapshotMode {
        self.overrides
            .get(&self.key(t))
            .copied()
            .unwrap_or(self.mode)
    }

    pub fn partition_by(&self, t: &TableRef) -> Vec<PartitionField> {
        self.partitioning
            .get(&self.key(t))
            .cloned()
            .unwrap_or_default()
    }

    /// The columns a delete carries besides the identifier: the table's partition columns that
    /// are source columns (a lineage column such as `_ts` isn't in the row).
    pub fn delete_extra(&self, schema: &TableSchema) -> Vec<String> {
        self.partition_by(schema.table())
            .into_iter()
            .map(|p| p.column)
            .filter(|c| schema.column(c).is_some())
            .collect()
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

PASS:

```text
test result: ok. 56 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): worker settings, the per-table snapshot decision, worker errors and hooks"
```


### Task 6: The sink writer

**Files:**
- Create: `crates/data-avalanche-pipeline/src/writer.rs`, `…/backoff.rs`
- Modify: `crates/data-avalanche-pipeline/src/lib.rs`, `…/test_support.rs`, `…/Cargo.toml`

**Interfaces:**
- Consumes: `Sink` (core), `PipelineDocs` and Task 3's documents, Task 5's `CommitGuard`,
  `Hooks`, `WorkerError`.
- Produces:
  - `Backoff { first, max }` (pub; default 1 s / 60 s); `delay(attempt)` doubles up to `max`,
    jittered in the upper half.
  - `writer::run(WriterCtx, mpsc::Receiver<Job>, mpsc::Sender<WriterEvent>)` (crate).
    `Job { change_sets, checkpoint, halts, snapshots, schemas, permit }` is one flush.
    `WriterEvent::{Halted(TableHalt), Degraded(Option<String>), Durable(SourcePosition),
    Failed(WorkerError)}` reports back. `WriterCtx { sink, docs, slot, pipeline_id, owner_epoch,
    guard, hooks, backoff }`.

Data-Flow Phase 4 and 5 in one task. The writer commits a job's change sets in order, each
through the guard and the fence. `Transient` retries forever, reporting DEGRADED (Q-M6a-5).
`Permanent` halts the table `SINK_REJECTED` and drops its later sets in the job. `Fenced` and
`NoTable` end the worker, and so does a lost lease, before the commit. Then come the halts, the
slot checkpoint, the snapshot documents and the schema caches, in that order, each write
retried. Only then does it report `Durable`. A job holds its rows' buffer permit until it is
durable, which is the backpressure.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-pipeline/Cargo.toml b/crates/data-avalanche-pipeline/Cargo.toml
--- a/crates/data-avalanche-pipeline/Cargo.toml
+++ b/crates/data-avalanche-pipeline/Cargo.toml
@@ -16,12 +16,16 @@ arrow-schema.workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
 data-avalanche-state.workspace = true
+data-avalanche-storage.workspace = true
 futures-util.workspace = true
 getrandom.workspace = true
 thiserror.workspace = true
-tokio = { workspace = true, features = ["sync"] }
+tokio = { workspace = true, features = ["sync", "rt"] }
 
 [dev-dependencies]
+async-trait.workspace = true
+bytes.workspace = true
+object_store.workspace = true
 data-avalanche-testkit.workspace = true
 tokio.workspace = true
 serde_json.workspace = true
```

Create `crates/data-avalanche-pipeline/src/backoff.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_double_up_to_the_cap_with_jitter_in_the_upper_half() {
        let b = Backoff::default();
        for (attempt, ceiling) in [(0, 1), (1, 2), (2, 4), (5, 32), (6, 60), (40, 60)] {
            let ceiling = Duration::from_secs(ceiling);
            for _ in 0..20 {
                let d = b.delay(attempt);
                assert!(
                    d >= ceiling / 2 && d <= ceiling,
                    "attempt {attempt}: {d:?} outside {:?}..={ceiling:?}",
                    ceiling / 2
                );
            }
        }
        let zero = Backoff {
            first: Duration::ZERO,
            max: Duration::ZERO,
        };
        assert_eq!(zero.delay(3), Duration::ZERO);
    }
}
```

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -6,6 +6,7 @@
 //! the source, runs the snapshots, hands change sets to the sink and persists the checkpoint.
 
 pub mod accumulator;
+pub mod backoff;
 pub mod batch;
 pub mod buffer;
 #[allow(
@@ -23,8 +24,14 @@ pub mod policy;
 mod settings;
 #[cfg(test)]
 mod test_support;
+#[allow(
+    dead_code,
+    reason = "the worker runs it (Task 11); the allow goes there"
+)]
+mod writer;
 
 pub use accumulator::{Accumulator, Flush};
+pub use backoff::Backoff;
 pub use batch::{Lineage, MAX_BATCH_BYTES};
 pub use buffer::{CollapsedRow, RowKind, TableBuffer};
 pub use error::{AccumulatorError, WorkerError};
```

Change `crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -1,8 +1,23 @@
 //! Fixtures for the crate's unit tests.
 
 use std::collections::BTreeMap;
+use std::sync::atomic::{AtomicBool, Ordering};
+use std::sync::{Arc, Mutex};
+use std::time::Instant;
 
+use async_trait::async_trait;
+use bytes::Bytes;
 use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
+use data_avalanche_core::position::Lsn;
+use data_avalanche_core::{
+    CdcEvent, ColumnSchema, ColumnValue, ColumnValueType as V, Op, Row, SourcePosition, SourceSeq,
+    TableRef, TableSchema, TransactionBoundary,
+};
+use data_avalanche_state::{DocStore, PipelineDocs};
+use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
+use object_store::memory::InMemory;
+
+use crate::{Accumulator, CommitGuard, FlushPolicy, HookAction, HookPoint, Hooks, Lineage};
 
 /// A PostgreSQL → Delta pipeline `p` reading `public.*`, with more `tables` keys (YAML lines
 /// indented six spaces), more `sink` and `postgres` keys (YAML flow entries).
@@ -49,3 +64,168 @@ pipelines:
     .next()
     .expect("one pipeline")
 }
+
+/// `public.<name>`: `id bigint` (the key) and `doc text`.
+pub(crate) fn schema(name: &str) -> TableSchema {
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
+pub(crate) fn lsn(n: u64) -> SourcePosition {
+    SourcePosition::Pg(Lsn(n))
+}
+
+pub(crate) fn row(id: i64, doc: &str) -> Row {
+    Row(vec![
+        ("id".into(), ColumnValue::Int64(id)),
+        ("doc".into(), ColumnValue::Text(doc.into())),
+    ])
+}
+
+/// An accumulator over `public.<names>` resuming at `from`, flushing at every boundary.
+pub(crate) fn accumulator(names: &[&str], from: u64) -> Accumulator {
+    let lineage = Lineage {
+        source_db: "db".into(),
+        pipeline: "p".into(),
+    };
+    let policy = FlushPolicy {
+        seconds: Some(0),
+        rows: None,
+        bytes: None,
+    };
+    let mut a = Accumulator::new(lineage, policy, lsn(from));
+    for n in names {
+        a.add_table(schema(n), &[]).expect("a new table");
+    }
+    a
+}
+
+/// One committed transaction of inserts, each `(table, id, doc)`, at `pos`.
+pub(crate) fn txn(a: &mut Accumulator, xid: u64, pos: u64, rows: &[(&str, i64, &str)]) {
+    let now = Instant::now();
+    a.begin(xid).expect("no open transaction");
+    for (i, (t, id, doc)) in rows.iter().enumerate() {
+        let e = CdcEvent {
+            op: Op::Insert,
+            table: TableRef::new("public", *t),
+            before: None,
+            after: Some(row(*id, doc)),
+            source_pos: lsn(pos),
+            change_seq: SourceSeq::Pg(Lsn(pos - 10 + i as u64)),
+            xid,
+            commit_ts_micros: 1,
+        };
+        a.apply(&e, now).expect("a valid event");
+    }
+    let b = TransactionBoundary {
+        xid,
+        commit_pos: lsn(pos),
+        commit_ts_micros: 1,
+    };
+    a.commit(&b, now).expect("the open transaction");
+}
+
+/// Pipeline documents over an in-memory store, and the store for faults.
+pub(crate) fn docs() -> (PipelineDocs, Arc<FlakyStore>) {
+    let inner = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/p").expect("a root");
+    let store = Arc::new(FlakyStore {
+        inner,
+        failing: Mutex::new(vec![]),
+    });
+    (PipelineDocs::new(DocStore::new(store.clone())), store)
+}
+
+/// A store whose next writes to a key fail as `Transient`, before they change anything.
+#[derive(Debug)]
+pub(crate) struct FlakyStore {
+    inner: ObjectStoreVersioned,
+    failing: Mutex<Vec<(String, usize)>>,
+}
+
+impl FlakyStore {
+    pub(crate) fn fail_writes(&self, key: &str, times: usize) {
+        self.failing
+            .lock()
+            .expect("no test panics while holding the lock")
+            .push((key.to_owned(), times));
+    }
+
+    fn fault(&self, key: &str) -> Result<(), StoreError> {
+        let mut failing = self
+            .failing
+            .lock()
+            .expect("no test panics while holding the lock");
+        match failing.iter_mut().find(|(k, n)| k == key && *n > 0) {
+            Some((_, n)) => {
+                *n -= 1;
+                Err(StoreError::Transient {
+                    key: key.to_owned(),
+                    reason: "injected".into(),
+                })
+            }
+            None => Ok(()),
+        }
+    }
+}
+
+#[async_trait]
+impl VersionedStore for FlakyStore {
+    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
+        self.inner.get(key).await
+    }
+
+    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
+        self.fault(key)?;
+        self.inner.create(key, body).await
+    }
+
+    async fn update(
+        &self,
+        key: &str,
+        expected: &ObjectVersion,
+        body: Bytes,
+    ) -> Result<ObjectVersion, StoreError> {
+        self.fault(key)?;
+        self.inner.update(key, expected, body).await
+    }
+
+    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
+        self.fault(key)?;
+        self.inner.put(key, body).await
+    }
+
+    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
+        self.inner.list(prefix).await
+    }
+}
+
+/// Crashes at the first hook point `when` matches.
+#[derive(Debug)]
+pub(crate) struct CrashAt(pub fn(&HookPoint) -> bool);
+
+impl Hooks for CrashAt {
+    fn at(&self, point: &HookPoint) -> HookAction {
+        if (self.0)(point) {
+            HookAction::Crash
+        } else {
+            HookAction::Continue
+        }
+    }
+}
+
+/// A commit guard the test turns off.
+#[derive(Debug, Default)]
+pub(crate) struct Switch(pub AtomicBool);
+
+impl CommitGuard for Switch {
+    fn may_commit(&self) -> bool {
+        !self.0.load(Ordering::SeqCst)
+    }
+}
```

Create `crates/data-avalanche-pipeline/src/writer.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{SinkTableState, TableSpec};
    use data_avalanche_state::{SLOT_KEY, SnapshotStatus};
    use data_avalanche_testkit::{MemorySink, SinkOp};
    use tokio::sync::mpsc;

    use super::*;
    use crate::NoHooks;
    use crate::test_support::{CrashAt, FlakyStore, Switch, accumulator, docs, lsn, schema, txn};

    const FAST: Backoff = Backoff {
        first: Duration::from_millis(1),
        max: Duration::from_millis(4),
    };

    struct Rig {
        sink: MemorySink,
        docs: PipelineDocs,
        store: Arc<FlakyStore>,
        guard: Arc<Switch>,
    }

    async fn rig(tables: &[&str]) -> Rig {
        let sink = MemorySink::new();
        for t in tables {
            sink.ensure_table(&TableSpec::new(&schema(t), vec![]).unwrap())
                .await
                .unwrap();
        }
        let (docs, store) = docs();
        Rig {
            sink,
            docs,
            store,
            guard: Arc::default(),
        }
    }

    impl Rig {
        /// Runs the writer over `jobs` to the end and returns its events.
        async fn run(&self, jobs: Vec<Job>, hooks: Arc<dyn Hooks>) -> Vec<WriterEvent> {
            let ctx = WriterCtx {
                sink: Arc::new(self.sink.clone()),
                docs: self.docs.clone(),
                slot: "p_slot".into(),
                pipeline_id: "p".into(),
                owner_epoch: 1,
                guard: self.guard.clone(),
                hooks,
                backoff: FAST,
            };
            let (jobs_tx, jobs_rx) = mpsc::unbounded_channel();
            let (events_tx, mut events_rx) = mpsc::unbounded_channel();
            for j in jobs {
                jobs_tx.send(j).unwrap();
            }
            drop(jobs_tx);
            run(ctx, jobs_rx, events_tx).await;
            let mut out = vec![];
            while let Some(e) = events_rx.recv().await {
                out.push(e);
            }
            out
        }

        async fn checkpoint(&self) -> Option<Lsn> {
            self.docs
                .slot()
                .await
                .unwrap()
                .map(|v| v.doc.confirmed_flush_lsn)
        }

        async fn state(&self, t: &str) -> SinkTableState {
            self.sink
                .table_state(&TableRef::new("public", t))
                .await
                .unwrap()
        }
    }

    fn job(flush: crate::Flush) -> Job {
        Job {
            change_sets: flush.change_sets,
            checkpoint: flush.checkpoint,
            halts: vec![],
            snapshots: vec![],
            schemas: vec![],
            permit: None,
        }
    }

    fn snapshot_doc(t: &str) -> TableSnapshotDoc {
        TableSnapshotDoc {
            table: TableRef::new("public", t),
            status: SnapshotStatus::Completed,
            start_position: Some(lsn(100)),
            cursor: None,
            row_count: Some(1),
            completed_at: None,
        }
    }

    fn rows(r: &Rig, t: &str) -> usize {
        r.sink
            .content(&TableRef::new("public", t))
            .map_or(0, |b| b.num_rows())
    }

    #[tokio::test]
    async fn a_job_commits_then_records_halts_checkpoint_progress_and_schemas() {
        let r = rig(&["a", "b"]).await;
        let mut acc = accumulator(&["a", "b"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x"), ("b", 1, "y")]);
        let mut j = job(acc.flush());
        let overflow = TableHalt {
            table: TableRef::new("public", "c"),
            reason: HaltReason::NumericOverflow { column: "n".into() },
            at: lsn(100),
        };
        j.halts.push(overflow.clone());
        j.snapshots.push(snapshot_doc("a"));
        j.schemas.push(schema("a"));
        let events = r.run(vec![j], Arc::new(NoHooks)).await;
        assert!(
            matches!(&events[..], [WriterEvent::Durable(p)] if *p == lsn(200)),
            "{events:?}"
        );
        assert_eq!((rows(&r, "a"), rows(&r, "b")), (1, 1));
        let marker = r.state("a").await.marker.unwrap();
        assert_eq!(
            (
                marker.owner_epoch,
                marker.committed_position,
                marker.pipeline_id.as_str()
            ),
            (1, lsn(200), "p")
        );
        assert_eq!(r.checkpoint().await, Some(Lsn(200)));
        assert_eq!(r.docs.halts().await.unwrap().tables, vec![overflow]);
        let a = TableRef::new("public", "a");
        assert!(r.docs.table_snapshot(&a).await.unwrap().is_some());
        assert_eq!(
            r.docs.table_schema(&a).await.unwrap().unwrap().doc,
            schema("a")
        );
    }

    #[tokio::test]
    async fn a_crash_after_the_commit_leaves_the_checkpoint_behind() {
        let r = rig(&["a"]).await;
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let mut j = job(acc.flush());
        j.schemas.push(schema("a"));
        let crash = CrashAt(|p| matches!(p, HookPoint::SinkCommitted { .. }));
        let events = r.run(vec![j], Arc::new(crash)).await;
        assert!(
            matches!(&events[..], [WriterEvent::Failed(WorkerError::Crashed(_))]),
            "{events:?}"
        );
        assert_eq!(rows(&r, "a"), 1);
        assert_eq!(r.checkpoint().await, None, "the replay re-commits it");
    }

    #[tokio::test]
    async fn a_crash_after_the_checkpoint_leaves_progress_and_schemas_behind() {
        let r = rig(&["a"]).await;
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let mut j = job(acc.flush());
        j.snapshots.push(snapshot_doc("a"));
        j.schemas.push(schema("a"));
        let crash = CrashAt(|p| matches!(p, HookPoint::CheckpointPersisted { .. }));
        r.run(vec![j], Arc::new(crash)).await;
        assert_eq!(r.checkpoint().await, Some(Lsn(200)));
        let a = TableRef::new("public", "a");
        assert!(r.docs.table_snapshot(&a).await.unwrap().is_none());
        assert!(r.docs.table_schema(&a).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_transient_commit_is_retried_while_degraded() {
        let r = rig(&["a"]).await;
        r.sink
            .fail(SinkOp::Commit, None, 2, || SinkError::Transient {
                reason: "throttled".into(),
            });
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let events = r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        let shape: Vec<_> = events
            .iter()
            .map(|e| match e {
                WriterEvent::Degraded(Some(why)) => {
                    assert!(why.contains("throttled"), "{why}");
                    "degraded"
                }
                WriterEvent::Degraded(None) => "recovered",
                WriterEvent::Durable(_) => "durable",
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(shape, ["degraded", "degraded", "recovered", "durable"]);
        assert_eq!(rows(&r, "a"), 1);
    }

    #[tokio::test]
    async fn a_transient_state_write_is_retried_while_degraded() {
        let r = rig(&["a"]).await;
        r.store.fail_writes(SLOT_KEY, 2);
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let events = r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        assert_eq!(events.len(), 4, "{events:?}");
        assert!(matches!(events[3], WriterEvent::Durable(_)));
        assert_eq!(r.checkpoint().await, Some(Lsn(200)));
    }

    #[tokio::test]
    async fn a_refused_change_set_halts_its_table_and_its_later_sets_are_dropped() {
        let r = rig(&["a", "b"]).await;
        let a = TableRef::new("public", "a");
        r.sink
            .fail(SinkOp::Commit, Some(&a), 1, || SinkError::Permanent {
                reason: "column mismatch".into(),
            });
        let mut acc = accumulator(&["a", "b"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x"), ("b", 1, "y")]);
        let first = job(acc.flush());
        txn(&mut acc, 2, 300, &[("a", 2, "x"), ("b", 2, "y")]);
        let second = job(acc.flush());
        let events = r.run(vec![first, second], Arc::new(NoHooks)).await;
        let halt = TableHalt {
            table: a.clone(),
            reason: HaltReason::SinkRejected {
                detail: "column mismatch".into(),
            },
            at: lsn(100),
        };
        assert!(
            matches!(&events[..], [WriterEvent::Halted(h), WriterEvent::Durable(_), WriterEvent::Durable(p)] if *h == halt && *p == lsn(300)),
            "{events:?}"
        );
        assert_eq!((rows(&r, "a"), rows(&r, "b")), (0, 2));
        assert_eq!(r.docs.halts().await.unwrap().tables, vec![halt]);
    }

    #[tokio::test]
    async fn a_fenced_commit_stops_the_writer() {
        let r = rig(&["a"]).await;
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let first = job(acc.flush());
        txn(&mut acc, 2, 300, &[("a", 2, "x")]);
        let second = job(acc.flush());
        r.run(vec![first], Arc::new(NoHooks)).await;
        r.sink.take_over(&TableRef::new("public", "a"), 2);
        let events = r.run(vec![second], Arc::new(NoHooks)).await;
        assert!(
            matches!(
                &events[..],
                [WriterEvent::Failed(WorkerError::Fenced {
                    current_epoch: 2,
                    ..
                })]
            ),
            "{events:?}"
        );
        assert_eq!(r.checkpoint().await, Some(Lsn(200)));
    }

    #[tokio::test]
    async fn a_lost_lease_stops_the_writer_before_it_commits() {
        let r = rig(&["a"]).await;
        r.guard.0.store(true, Ordering::SeqCst);
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let events = r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        assert!(
            matches!(&events[..], [WriterEvent::Failed(WorkerError::LeaseLost)]),
            "{events:?}"
        );
        assert_eq!(rows(&r, "a"), 0);
    }

    #[tokio::test]
    async fn a_missing_table_stops_the_writer() {
        let r = rig(&[]).await;
        let mut acc = accumulator(&["a"], 100);
        txn(&mut acc, 1, 200, &[("a", 1, "x")]);
        let events = r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
        assert!(
            matches!(
                &events[..],
                [WriterEvent::Failed(WorkerError::Sink(
                    SinkError::NoTable { .. }
                ))]
            ),
            "{events:?}"
        );
        assert_eq!(r.checkpoint().await, None);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

FAIL to compile (25 errors); the first:

```text
error[E0432]: unresolved import `backoff::Backoff`
error[E0433]: cannot find type `Duration` in this scope
error[E0422]: cannot find struct, variant or union type `Backoff` in this scope
error[E0425]: cannot find type `Backoff` in this scope
error[E0425]: cannot find type `PipelineDocs` in this scope
error[E0425]: cannot find type `Arc` in this scope
… and 19 more
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-pipeline/src/backoff.rs`, above the test module:

```rust
//! Exponential backoff with jitter (Data-Flow §Error Paths: initial 1s, max 60s, jitter).

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    pub first: Duration,
    pub max: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            first: Duration::from_secs(1),
            max: Duration::from_secs(60),
        }
    }
}

impl Backoff {
    /// The wait before retry `attempt` (0 = the first retry): `first · 2^attempt`, capped at `max`,
    /// then a random point in its upper half, so workers that failed together don't retry together.
    pub fn delay(&self, attempt: u32) -> Duration {
        let ceiling = self
            .first
            .saturating_mul(1u32.checked_shl(attempt.min(31)).unwrap_or(u32::MAX))
            .min(self.max);
        let half = ceiling / 2;
        let nanos = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        let jitter = match nanos {
            0 => 0,
            n => getrandom::u64().expect("the OS random number generator is unavailable") % (n + 1),
        };
        half + Duration::from_nanos(jitter)
    }
}
```

Add the implementation to `crates/data-avalanche-pipeline/src/writer.rs`, above the test module:

```rust
//! The sink writer (Data-Flow Phase 4, and Phase 5 up to the acknowledgment): one task that
//! commits each flush's change sets in order, then makes the flush durable and reports it. The
//! event loop acknowledges the stream only on [`WriterEvent::Durable`] (D-IMPL-2).
//!
//! A job becomes durable in this order: every change set committed (or skipped as covered), the
//! halts recorded, the checkpoint, the snapshot progress, the schema cache. A halt is recorded
//! before the checkpoint passes it. Snapshot progress and cached schemas are written once the
//! checkpoint covers what they describe. A crash between two steps leaves a state that a restart
//! replays correctly.

use std::collections::BTreeSet;
use std::sync::Arc;

use data_avalanche_core::{
    CommitMarker, CommitOutcome, HaltReason, Sink, SinkError, SourcePosition, TableChangeSet,
    TableRef, TableSchema,
};
use data_avalanche_state::{PipelineDocs, SlotDoc, StateError, TableHalt, TableSnapshotDoc};
use data_avalanche_storage::StoreError;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::backoff::Backoff;
use crate::{AccumulatorError, CommitGuard, HookAction, HookPoint, Hooks, WorkerError};

/// One flush and what becomes durable with it.
#[derive(Debug)]
pub(crate) struct Job {
    pub change_sets: Vec<TableChangeSet>,
    pub checkpoint: SourcePosition,
    /// Halts the event loop found since the last job, recorded before the checkpoint.
    pub halts: Vec<TableHalt>,
    /// Snapshot documents whose chunks this flush carries.
    pub snapshots: Vec<TableSnapshotDoc>,
    /// Schemas whose change this flush's checkpoint covers.
    pub schemas: Vec<TableSchema>,
    /// The rows this job holds of `buffer_capacity_rows`; released once it is durable.
    pub permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug)]
pub(crate) enum WriterEvent {
    /// The sink refused the table's changes as permanent: the table is halted, and the halt is
    /// recorded before the checkpoint that passes it.
    Halted(TableHalt),
    /// A commit or a state write failed and is being retried (`Some`, the last error), or
    /// succeeded again (`None`).
    Degraded(Option<String>),
    /// Everything up to the checkpoint is durable: acknowledge it to the source.
    Durable(SourcePosition),
    /// The writer stopped; no further event follows.
    Failed(WorkerError),
}

#[derive(Clone)]
pub(crate) struct WriterCtx {
    pub sink: Arc<dyn Sink>,
    pub docs: PipelineDocs,
    /// The slot whose document holds the checkpoint (PostgreSQL).
    pub slot: String,
    pub pipeline_id: String,
    pub owner_epoch: u64,
    pub guard: Arc<dyn CommitGuard>,
    pub hooks: Arc<dyn Hooks>,
    pub backoff: Backoff,
}

impl std::fmt::Debug for WriterCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterCtx")
            .field("slot", &self.slot)
            .field("pipeline_id", &self.pipeline_id)
            .field("owner_epoch", &self.owner_epoch)
            .finish_non_exhaustive()
    }
}

/// Runs jobs until the job channel closes or one fails.
pub(crate) async fn run(
    ctx: WriterCtx,
    mut jobs: UnboundedReceiver<Job>,
    events: UnboundedSender<WriterEvent>,
) {
    let mut w = Writer {
        ctx,
        rejected: BTreeSet::new(),
        events,
        degraded: false,
    };
    while let Some(job) = jobs.recv().await {
        if let Err(e) = w.job(job).await {
            let _ = w.events.send(WriterEvent::Failed(e));
            return;
        }
    }
}

struct Writer {
    ctx: WriterCtx,
    /// Tables the sink refused: their later change sets are dropped.
    rejected: BTreeSet<TableRef>,
    events: UnboundedSender<WriterEvent>,
    degraded: bool,
}

/// A failure worth retrying: the sink's transient class, or a storage failure whose write may not
/// have landed.
fn transient_state(e: &StateError) -> bool {
    matches!(
        e,
        StateError::Store(StoreError::Transient { .. } | StoreError::Contention { .. })
    )
}

impl Writer {
    fn hook(&self, point: HookPoint) -> Result<(), WorkerError> {
        match self.ctx.hooks.at(&point) {
            HookAction::Continue => Ok(()),
            HookAction::Crash => Err(WorkerError::Crashed(point)),
        }
    }

    /// Reports every failed attempt, and the first success after them.
    fn degraded(&mut self, why: Option<String>) {
        if why.is_some() || self.degraded {
            self.degraded = why.is_some();
            let _ = self.events.send(WriterEvent::Degraded(why));
        }
    }

    async fn job(&mut self, job: Job) -> Result<(), WorkerError> {
        let mut halts = job.halts;
        for cs in job.change_sets {
            if self.rejected.contains(&cs.table) {
                continue;
            }
            let table = cs.table.clone();
            let at = cs.position.start.clone();
            match self.commit(cs).await? {
                Some(detail) => {
                    let halt = TableHalt {
                        table: table.clone(),
                        reason: HaltReason::SinkRejected { detail },
                        at,
                    };
                    self.rejected.insert(table);
                    let _ = self.events.send(WriterEvent::Halted(halt.clone()));
                    halts.push(halt);
                }
                None => self.hook(HookPoint::SinkCommitted { table })?,
            }
        }
        for halt in &halts {
            self.state(Write::Halt(halt)).await?;
        }
        self.save_checkpoint(&job.checkpoint).await?;
        self.hook(HookPoint::CheckpointPersisted {
            position: job.checkpoint.clone(),
        })?;
        for doc in &job.snapshots {
            self.state(Write::Snapshot(doc)).await?;
        }
        for schema in &job.schemas {
            self.state(Write::Schema(schema)).await?;
        }
        drop(job.permit);
        let _ = self.events.send(WriterEvent::Durable(job.checkpoint));
        Ok(())
    }

    /// Commits one change set, retrying a transient failure. `Some(detail)` when the sink refused
    /// it as permanent: the table halts.
    async fn commit(&mut self, cs: TableChangeSet) -> Result<Option<String>, WorkerError> {
        let marker = CommitMarker {
            owner_epoch: self.ctx.owner_epoch,
            committed_position: cs.position.end.clone(),
            pipeline_id: self.ctx.pipeline_id.clone(),
        };
        let mut attempt = 0;
        loop {
            if !self.ctx.guard.may_commit() {
                return Err(WorkerError::LeaseLost);
            }
            match self.ctx.sink.commit(cs.clone(), marker.clone()).await {
                Ok(CommitOutcome::Committed { .. } | CommitOutcome::SkippedCovered) => {
                    self.degraded(None);
                    return Ok(None);
                }
                Ok(CommitOutcome::Fenced { current_epoch }) => {
                    return Err(WorkerError::Fenced {
                        table: cs.table,
                        current_epoch,
                    });
                }
                Err(SinkError::Transient { reason }) => {
                    self.degraded(Some(format!("table {}: {reason}", cs.table)));
                    tokio::time::sleep(self.ctx.backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(SinkError::Permanent { reason }) => {
                    self.degraded(None);
                    return Ok(Some(reason));
                }
                Err(e) => return Err(WorkerError::Sink(e)),
            }
        }
    }

    async fn save_checkpoint(&mut self, checkpoint: &SourcePosition) -> Result<(), WorkerError> {
        let SourcePosition::Pg(lsn) = checkpoint else {
            return Err(AccumulatorError::Protocol {
                detail: format!("checkpoint {checkpoint} isn't a PostgreSQL position"),
            }
            .into());
        };
        if !self.ctx.guard.may_commit() {
            return Err(WorkerError::LeaseLost);
        }
        let doc = SlotDoc {
            slot_name: self.ctx.slot.clone(),
            confirmed_flush_lsn: *lsn,
        };
        self.state(Write::Slot(&doc)).await
    }

    /// A state write, retried while it fails transiently.
    async fn state(&mut self, write: Write<'_>) -> Result<(), WorkerError> {
        let mut attempt = 0;
        loop {
            let docs = &self.ctx.docs;
            let r = match write {
                Write::Halt(h) => docs.record_halt(h).await.map(drop),
                Write::Slot(s) => docs.save_slot(s).await.map(drop),
                Write::Snapshot(d) => docs.save_table_snapshot(d).await.map(drop),
                Write::Schema(s) => docs.save_table_schema(s).await.map(drop),
            };
            match r {
                Ok(()) => {
                    self.degraded(None);
                    return Ok(());
                }
                Err(e) if transient_state(&e) => {
                    self.degraded(Some(e.to_string()));
                    tokio::time::sleep(self.ctx.backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

/// One state document a job writes.
#[derive(Clone, Copy)]
enum Write<'a> {
    Halt(&'a TableHalt),
    Slot(&'a SlotDoc),
    Snapshot(&'a TableSnapshotDoc),
    Schema(&'a TableSchema),
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

PASS:

```text
test result: ok. 66 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): the sink writer: ordered commits, then halts, checkpoint, progress and schemas"
```


### Task 7: The stream session

**Files:**
- Create: `crates/data-avalanche-pipeline/src/session.rs`, `…/status.rs`
- Modify: `crates/data-avalanche-pipeline/src/lib.rs`, `…/test_support.rs`, `…/Cargo.toml`

**Interfaces:**
- Consumes: the accumulator (M3), Task 6's writer, `SourceConnector::open_stream` and
  `augment`, `classify` (core), `Sink::evolve`.
- Produces:
  - `Command::{Drain, Flush}`, `Exit::{Drained, Dumped}`, `Phase::{Starting, Streaming,
    Reconnecting { error }, Draining}`, `SnapshotProgress { rows_done }`, `WorkerStatus { phase,
    read, checkpoint, retained_bytes, degraded, halted, snapshots, warnings }`, `Tuning {
    source_backoff, sink_backoff, in_flight_backoff, contended_window, tick, augment_attempts }`
    (pub, re-exported).
  - `Core` (crate): the run-wide state a session borrows (settings, source, sink, docs,
    epoch, guard, hooks, tuning, control, status, the tracked schemas). `Session::open(&mut
    Core) -> Result<Result<Session, SourceError>, WorkerError>` and `Session::run() ->
    Result<End, WorkerError>`, with `End::{Exit(Exit), Source(SourceError)}`.

One session reads the stream from the durable checkpoint. It accumulates, flushes by policy,
splits large transactions and honours the TRUNCATE barrier. It halts a table on a value or
identifier error, applies schema signals by §8, and drains on command. A schema signal reads
channel C against the cached schema, classifies, flushes and waits until durable, evolves the
sink, then sets the accumulator's schema. Everything after a durable flush is acknowledged to
the source. A full buffer stops the reading until the writer frees permits. A source error ends
the session after what is already durable.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-pipeline/Cargo.toml b/crates/data-avalanche-pipeline/Cargo.toml
--- a/crates/data-avalanche-pipeline/Cargo.toml
+++ b/crates/data-avalanche-pipeline/Cargo.toml
@@ -20,7 +20,7 @@ data-avalanche-storage.workspace = true
 futures-util.workspace = true
 getrandom.workspace = true
 thiserror.workspace = true
-tokio = { workspace = true, features = ["sync", "rt"] }
+tokio = { workspace = true, features = ["sync", "rt", "time", "macros"] }
 
 [dev-dependencies]
 async-trait.workspace = true
```

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -17,11 +17,17 @@ mod decide;
 pub mod error;
 pub mod hooks;
 pub mod policy;
+#[allow(
+    dead_code,
+    reason = "the worker runs it (Task 11); the allow goes there"
+)]
+mod session;
 #[allow(
     dead_code,
     reason = "the worker reads it (Task 11); the allow goes there"
 )]
 mod settings;
+pub mod status;
 #[cfg(test)]
 mod test_support;
 #[allow(
@@ -37,3 +43,4 @@ pub use buffer::{CollapsedRow, RowKind, TableBuffer};
 pub use error::{AccumulatorError, WorkerError};
 pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
 pub use policy::{FlushPolicy, FlushReason, TxnLimits};
+pub use status::{Command, Exit, Phase, SnapshotProgress, Tuning, WorkerStatus};
```

Create `crates/data-avalanche-pipeline/src/session.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{ColumnSchema, ColumnValue, ColumnValueType as V, Row};
    use data_avalanche_testkit::{FakeTxn, Method, SinkOp};

    use super::*;
    use crate::test_support::{World, Yaml, pipeline, row, t, wait_until};

    /// Runs a session over `w` until `drive` returns, then drains it.
    async fn session<F: Future<Output = ()>>(
        w: &World,
        p: &data_avalanche_config::Pipeline,
        tables: &[&str],
        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
    ) -> (Result<End, WorkerError>, WorkerStatus) {
        let (mut core, control, status) = w.core(p, tables);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let watching = status.clone();
        let driver = async move {
            drive(watching).await;
            let _ = control.send(Command::Drain).await;
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        let last = status.borrow().clone();
        (end, last)
    }

    fn flush_each() -> data_avalanche_config::Pipeline {
        pipeline("", "flush: { seconds: 0 }", "")
    }

    async fn durable(status: &watch::Receiver<WorkerStatus>, at: SourcePosition) {
        wait_until("the log's end is durable", || {
            status.borrow().checkpoint.as_ref() == Some(&at)
        })
        .await;
    }

    async fn checkpoint(w: &World) -> Lsn {
        w.docs
            .slot()
            .await
            .unwrap()
            .unwrap()
            .doc
            .confirmed_flush_lsn
    }

    fn lsn_of(p: &SourcePosition) -> Lsn {
        match p {
            SourcePosition::Pg(l) => *l,
            other => panic!("{other}"),
        }
    }

    #[tokio::test]
    async fn a_session_streams_flushes_and_acknowledges_what_is_durable() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(1, "x"))
                    .insert(&t("b"), row(1, "y")),
            );
            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!((w.ids("a"), w.ids("b")), (vec![1, 2], vec![1]));
        let end = w.source.wal_end();
        assert_eq!(w.source.slot(), Some(end.clone()), "acknowledged");
        assert_eq!(checkpoint(&w).await, lsn_of(&end));
        assert_eq!((st.read, st.checkpoint), (Some(end.clone()), Some(end)));
    }

    #[tokio::test]
    async fn a_drain_flushes_what_the_policy_held_back() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (end, _) = session(&w, &p, &["a"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            let end = w.source.wal_end();
            wait_until("the commit is read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            assert!(w.ids("a").is_empty(), "the policy holds it");
        })
        .await;
        assert!(matches!(end.unwrap(), End::Exit(Exit::Drained)));
        assert_eq!(w.ids("a"), [1]);
        assert_eq!(checkpoint(&w).await, lsn_of(&w.source.wal_end()));
    }

    #[tokio::test]
    async fn a_full_buffer_stops_the_reading() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 0 }, buffer_capacity_rows: 1", "");
        w.sink.hold_commits();
        let (end, _) = session(&w, &p, &["a"], async |status| {
            let mut ends = vec![];
            for id in 1..=3 {
                w.source
                    .commit(FakeTxn::new().insert(&t("a"), row(id, "x")));
                ends.push(w.source.wal_end());
            }
            wait_until("the second commit is read", || {
                status.borrow().read.as_ref() == Some(&ends[1])
            })
            .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(
                status.borrow().read.as_ref(),
                Some(&ends[1]),
                "one flush is in the writer and one waits: the third isn't read"
            );
            w.sink.release_commits();
            durable(&status, ends[2].clone()).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2, 3]);
    }

    #[tokio::test]
    async fn a_truncate_flushes_the_tables_earlier_changes_first() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline("", "flush: { seconds: 3600 }", "");
        let (end, _) = session(&w, &p, &["a"], async |status| {
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(1, "x"))
                    .insert(&t("a"), row(2, "x")),
            );
            w.source.commit(
                FakeTxn::new()
                    .truncate(&[t("a")])
                    .insert(&t("a"), row(3, "x")),
            );
            let end = w.source.wal_end();
            wait_until("both are read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [3]);
        assert_eq!(
            w.sink.version(&t("a")),
            Some(2),
            "the barrier's flush, then the drain's"
        );
    }

    #[tokio::test]
    async fn a_large_transaction_commits_in_slices() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = pipeline(
            "",
            "flush: { seconds: 0 }, max_transaction_batch_rows: 2",
            "",
        );
        let (end, _) = session(&w, &p, &["a"], async |status| {
            let mut txn = FakeTxn::new();
            for id in 1..=5 {
                txn = txn.insert(&t("a"), row(id, "x"));
            }
            w.source.commit(txn);
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5]);
        assert!(
            w.sink.version(&t("a")).unwrap() >= 3,
            "two slices, then the commit"
        );
    }

    #[tokio::test]
    async fn a_halted_table_is_recorded_and_its_later_changes_dropped() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let start = w.source.wal_end();
        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
            let mut bad = row(1, "x");
            bad.0.push(("zzz".into(), ColumnValue::Int64(1)));
            w.source.commit(FakeTxn::new().insert(&t("a"), bad));
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(2, "x"))
                    .insert(&t("b"), row(1, "y")),
            );
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!((w.ids("a"), w.ids("b")), (vec![], vec![1]));
        let halts = w.docs.halts().await.unwrap().tables;
        assert_eq!(halts.len(), 1);
        assert_eq!((&halts[0].table, &halts[0].at), (&t("a"), &start));
        assert!(matches!(halts[0].reason, HaltReason::SchemaMismatch { .. }));
        assert!(st.halted.contains_key(&t("a")));
    }

    #[tokio::test]
    async fn a_sink_refusal_halts_the_table() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.sink
            .fail(SinkOp::Commit, Some(&t("a")), 1, || SinkError::Permanent {
                reason: "refused".into(),
            });
        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), row(2, "x"))
                    .insert(&t("b"), row(1, "y")),
            );
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!((w.ids("a"), w.ids("b")), (vec![], vec![1]));
        assert!(matches!(
            st.halted.get(&t("a")),
            Some(HaltReason::SinkRejected { .. })
        ));
    }

    #[tokio::test]
    async fn a_source_failure_ends_the_session_after_what_is_durable() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (mut core, _control, status) = w.core(&flush_each(), &["a"]);
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let driver = async {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            durable(&status, w.source.wal_end()).await;
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        assert!(
            matches!(end.unwrap(), End::Source(SourceError::Transient(_))),
            "the worker reopens it"
        );
        assert_eq!(w.ids("a"), [1]);
    }

    fn with_note(name: &str) -> TableSchema {
        TableSchema::new(
            t(name),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    }

    fn noted(id: i64) -> Row {
        let mut r = row(id, "x");
        r.0.push(("note".into(), ColumnValue::Text("n".into())));
        r
    }

    #[tokio::test]
    async fn an_added_column_is_evolved_where_the_stream_meets_it() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (end, _) = session(&w, &flush_each(), &["a"], async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            w.source.alter(&t("a"), with_note("a"));
            w.source.commit(FakeTxn::new().insert(&t("a"), noted(2)));
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2]);
        let content = w.sink.content(&t("a")).unwrap();
        assert!(content.column_by_name("note").is_some());
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert!(cached.column("note").is_some(), "cached once durable");
        assert_eq!(cached.source_id(), w.source.schema(&t("a")).source_id());
    }

    #[tokio::test]
    async fn a_safe_change_halts_when_evolution_is_off() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let p = Yaml {
            pipeline: "\n    schema_evolution: { enabled: false }",
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build();
        let (end, st) = session(&w, &p, &["a"], async |status| {
            w.source.alter(&t("a"), with_note("a"));
            w.source.commit(FakeTxn::new().insert(&t("a"), noted(1)));
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        assert_eq!(st.halted.get(&t("a")), Some(&HaltReason::EvolutionDisabled));
        assert!(w.ids("a").is_empty());
    }

    #[tokio::test]
    async fn an_unsafe_change_halts_the_table() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let int_doc = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Int64, "bigint"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        let (end, st) = session(&w, &flush_each(), &["a"], async |status| {
            w.source.alter(&t("a"), int_doc);
            w.source.commit(FakeTxn::new().insert(
                &t("a"),
                Row(vec![
                    ("id".into(), ColumnValue::Int64(1)),
                    ("doc".into(), ColumnValue::Int64(7)),
                ]),
            ));
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        assert!(matches!(
            st.halted.get(&t("a")),
            Some(HaltReason::UnsafeDdl { .. })
        ));
    }

    #[tokio::test]
    async fn a_widening_the_sink_cant_apply_halts_the_table() {
        let narrow = |v| {
            TableSchema::new(
                t("w"),
                vec![
                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                    ColumnSchema::new("n", v, "int"),
                ],
                vec!["id".into()],
            )
            .unwrap()
        };
        let w = World::with(vec![narrow(V::Int32)]);
        w.started(&["w"]).await;
        let (end, st) = session(&w, &flush_each(), &["w"], async |status| {
            w.source.alter(&t("w"), narrow(V::Int64));
            w.source.commit(FakeTxn::new().insert(
                &t("w"),
                Row(vec![
                    ("id".into(), ColumnValue::Int64(1)),
                    ("n".into(), ColumnValue::Int64(1 << 40)),
                ]),
            ));
            let end = w.source.wal_end();
            wait_until("it is read", || status.borrow().read.as_ref() == Some(&end)).await;
        })
        .await;
        end.unwrap();
        assert!(matches!(
            st.halted.get(&t("w")),
            Some(HaltReason::EvolutionUnsupported { .. })
        ));
    }

    #[tokio::test]
    async fn a_failed_catalog_read_halts_the_table() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
            w.source
                .fail(Method::Augment, SourceError::Permanent("denied".into()));
            w.source.alter(&t("a"), with_note("a"));
            w.source.commit(
                FakeTxn::new()
                    .insert(&t("a"), noted(1))
                    .insert(&t("b"), row(1, "y")),
            );
            durable(&status, w.source.wal_end()).await;
        })
        .await;
        end.unwrap();
        match st.halted.get(&t("a")) {
            Some(HaltReason::UnsafeDdl { detail }) => {
                assert!(detail.contains("denied"), "{detail}")
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(w.ids("b"), [1], "the other table goes on");
    }
}
```

Change `crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -3,34 +3,64 @@
 use std::collections::BTreeMap;
 use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::{Arc, Mutex};
-use std::time::Instant;
+use std::time::{Duration, Instant};
 
+use arrow_array::Int64Array;
 use async_trait::async_trait;
 use bytes::Bytes;
 use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    CdcEvent, ColumnSchema, ColumnValue, ColumnValueType as V, Op, Row, SourcePosition, SourceSeq,
-    TableRef, TableSchema, TransactionBoundary,
+    CdcEvent, ColumnSchema, ColumnValue, ColumnValueType as V, Op, Row, Sink, SourcePosition,
+    SourceSeq, TableRef, TableSchema, TableSpec, TransactionBoundary,
 };
-use data_avalanche_state::{DocStore, PipelineDocs};
+use data_avalanche_state::{DocStore, PipelineDocs, SlotDoc};
 use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
+use data_avalanche_testkit::{FakeSource, MemorySink};
 use object_store::memory::InMemory;
+use tokio::sync::{mpsc, watch};
 
-use crate::{Accumulator, CommitGuard, FlushPolicy, HookAction, HookPoint, Hooks, Lineage};
+use crate::session::Core;
+use crate::settings::Settings;
+use crate::{
+    Accumulator, Backoff, Command, CommitGuard, FlushPolicy, HookAction, HookPoint, Hooks, Lineage,
+    NoHooks, Tuning, Unguarded, WorkerStatus,
+};
 
 /// A PostgreSQL → Delta pipeline `p` reading `public.*`, with more `tables` keys (YAML lines
 /// indented six spaces), more `sink` and `postgres` keys (YAML flow entries).
 pub(crate) fn pipeline(tables: &str, sink: &str, postgres: &str) -> Pipeline {
-    let sep = |s: &str| {
-        if s.is_empty() {
-            String::new()
-        } else {
-            format!(", {s}")
-        }
-    };
-    let yaml = format!(
-        r#"
+    Yaml {
+        tables,
+        sink,
+        postgres,
+        ..Yaml::default()
+    }
+    .build()
+}
+
+/// The test pipeline's extra YAML. `pipeline`, `source` and `tables` are lines (each starting
+/// with a newline, indented four, six and six spaces); `sink` and `postgres` are flow entries.
+#[derive(Debug, Clone, Copy, Default)]
+pub(crate) struct Yaml<'a> {
+    pub pipeline: &'a str,
+    pub source: &'a str,
+    pub postgres: &'a str,
+    pub tables: &'a str,
+    pub sink: &'a str,
+}
+
+impl Yaml<'_> {
+    pub fn build(self) -> Pipeline {
+        let sep = |s: &str| {
+            if s.is_empty() {
+                String::new()
+            } else {
+                format!(", {s}")
+            }
+        };
+        let yaml = format!(
+            r#"
 config_version: 1
 globals:
   state: {{ bucket: app-state }}
@@ -44,25 +74,29 @@ pipelines:
       user: avalanche
       password: avalanche
       ssl_mode: disable
-      postgres: {{ publication: p_pub, slot: p_slot{postgres} }}
+      postgres: {{ publication: p_pub, slot: p_slot{postgres} }}{source}
     tables:
       include: [public.*]{tables}
-    sink: {{ type: delta, bucket: lake, delta: {{}}{sink} }}
+    sink: {{ type: delta, bucket: lake, delta: {{}}{sink} }}{top}
 "#,
-        postgres = sep(postgres),
-        sink = sep(sink),
-    );
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
+            postgres = sep(self.postgres),
+            source = self.source,
+            tables = self.tables,
+            sink = sep(self.sink),
+            top = self.pipeline,
+        );
+        load_str(
+            &yaml,
+            &BTreeMap::new(),
+            &FingerprintKey::from_bytes([1; 32]),
+        )
+        .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
+        .config
+        .pipelines
+        .into_values()
+        .next()
+        .expect("one pipeline")
+    }
 }
 
 /// `public.<name>`: `id bigint` (the key) and `doc text`.
@@ -229,3 +263,117 @@ impl CommitGuard for Switch {
         !self.0.load(Ordering::SeqCst)
     }
 }
+
+/// Fast retries and ticks for tests.
+pub(crate) fn fast() -> Tuning {
+    let ms = |first, max| Backoff {
+        first: Duration::from_millis(first),
+        max: Duration::from_millis(max),
+    };
+    Tuning {
+        source_backoff: ms(1, 4),
+        sink_backoff: ms(1, 4),
+        in_flight_backoff: ms(1, 4),
+        contended_window: Duration::from_millis(200),
+        tick: Duration::from_millis(5),
+        augment_attempts: 3,
+    }
+}
+
+/// Polls `cond` until it holds; panics after five seconds.
+pub(crate) async fn wait_until(what: &str, cond: impl Fn() -> bool) {
+    let deadline = Instant::now() + Duration::from_secs(5);
+    while !cond() {
+        assert!(Instant::now() < deadline, "timed out waiting until {what}");
+        tokio::time::sleep(Duration::from_millis(2)).await;
+    }
+}
+
+/// A source, a sink and the pipeline's state documents.
+#[derive(Debug, Clone)]
+pub(crate) struct World {
+    pub source: FakeSource,
+    pub sink: MemorySink,
+    pub docs: PipelineDocs,
+}
+
+impl World {
+    /// A source holding `tables` (each [`schema`]).
+    pub fn new(tables: &[&str]) -> Self {
+        Self::with(tables.iter().map(|t| schema(t)).collect())
+    }
+
+    pub fn with(tables: Vec<TableSchema>) -> Self {
+        let (docs, _) = docs();
+        Self {
+            source: FakeSource::new(tables),
+            sink: MemorySink::new(),
+            docs,
+        }
+    }
+
+    /// What Phase 0 leaves for a stream session over `tables`: the sink tables, the cached
+    /// schemas and the checkpoint at the log's end.
+    pub async fn started(&self, tables: &[&str]) {
+        for t in tables {
+            let s = self.source.schema(&TableRef::new("public", *t));
+            self.sink
+                .ensure_table(&TableSpec::new(&s, vec![]).expect("a spec"))
+                .await
+                .expect("a table");
+            self.docs.save_table_schema(&s).await.expect("cached");
+        }
+        let SourcePosition::Pg(end) = self.source.wal_end() else {
+            unreachable!("the fake speaks PostgreSQL")
+        };
+        self.docs
+            .save_slot(&SlotDoc {
+                slot_name: "p_slot".into(),
+                confirmed_flush_lsn: end,
+            })
+            .await
+            .expect("the checkpoint");
+    }
+
+    /// A worker core over `tables` with `p`'s settings.
+    pub fn core(
+        &self,
+        p: &Pipeline,
+        tables: &[&str],
+    ) -> (Core, mpsc::Sender<Command>, watch::Receiver<WorkerStatus>) {
+        let (control, control_rx) = mpsc::channel(8);
+        let (status, status_rx) = watch::channel(WorkerStatus::default());
+        let core = Core {
+            settings: Settings::new(p),
+            source: Arc::new(self.source.clone()),
+            sink: Arc::new(self.sink.clone()),
+            docs: self.docs.clone(),
+            owner_epoch: 1,
+            guard: Arc::new(Unguarded),
+            hooks: Arc::new(NoHooks),
+            tuning: fast(),
+            control: control_rx,
+            status,
+            tracked: tables.iter().map(|t| TableRef::new("public", *t)).collect(),
+        };
+        (core, control, status_rx)
+    }
+
+    /// The table's `id` values in the sink, in key order.
+    pub fn ids(&self, table: &str) -> Vec<i64> {
+        let Some(b) = self.sink.content(&TableRef::new("public", table)) else {
+            return vec![];
+        };
+        let ids = b
+            .column_by_name("id")
+            .expect("an id column")
+            .as_any()
+            .downcast_ref::<Int64Array>()
+            .expect("bigint ids");
+        ids.values().to_vec()
+    }
+}
+
+pub(crate) fn t(name: &str) -> TableRef {
+    TableRef::new("public", name)
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

FAIL to compile (17 errors); the first:

```text
error[E0583]: file not found for module `status`
error[E0432]: unresolved import `crate::session::Core`
error[E0425]: cannot find type `WorkerStatus` in this scope
error[E0425]: cannot find type `End` in this scope
error[E0425]: cannot find type `WorkerError` in this scope
error[E0433]: cannot find type `Command` in this scope
… and 11 more
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-pipeline/src/session.rs`, above the test module:

```rust
//! One stream session (Data-Flow Phases 2–5). It opens the stream at the durable checkpoint and
//! reads it into the accumulator. Each flush goes to the sink writer, and the stream is
//! acknowledged once the flush is durable. A session ends on a drain, on a source failure (the
//! worker decides what follows) or with an error.
//!
//! A session starts from durable state only: the checkpoint, the cached schemas and the recorded
//! halts. What it held in memory is lost when it ends, and the next session replays it from the
//! checkpoint.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use data_avalanche_core::{
    ChangeStream, DeltaClass, EvolveOutcome, HaltReason, SchemaDelta, SchemaHint, Sink, SinkError,
    SourceConnector, SourceError, SourcePosition, StreamItem, TableRef, TableSchema, classify,
};
use data_avalanche_state::{PipelineDocs, TableHalt};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinHandle;

use crate::settings::Settings;
use crate::writer::{self, Job, WriterCtx, WriterEvent};
use crate::{
    Accumulator, AccumulatorError, Command, CommitGuard, Exit, FlushReason, Hooks, Phase, Tuning,
    WorkerError, WorkerStatus,
};

/// What every session of one worker shares.
pub(crate) struct Core {
    pub settings: Settings,
    pub source: Arc<dyn SourceConnector>,
    pub sink: Arc<dyn Sink>,
    pub docs: PipelineDocs,
    pub owner_epoch: u64,
    pub guard: Arc<dyn CommitGuard>,
    pub hooks: Arc<dyn Hooks>,
    pub tuning: Tuning,
    pub control: mpsc::Receiver<Command>,
    pub status: watch::Sender<WorkerStatus>,
    /// The tables Phase 0 tracks. A session streams each one unless it is halted.
    pub tracked: BTreeSet<TableRef>,
}

impl Core {
    pub fn update(&self, f: impl FnOnce(&mut WorkerStatus)) {
        self.status.send_modify(f);
    }
}

/// How a session ended without an error.
#[derive(Debug)]
pub(crate) enum End {
    Exit(Exit),
    /// The source failed: a transient failure reopens the stream, a lost position recovers it.
    Source(SourceError),
}

fn protocol(detail: String) -> WorkerError {
    AccumulatorError::Protocol { detail }.into()
}

pub(crate) struct Session<'c> {
    core: &'c mut Core,
    acc: Accumulator,
    /// The streamed tables' schemas as of the frontier.
    schemas: BTreeMap<TableRef, TableSchema>,
    stream: Box<dyn ChangeStream>,
    jobs: UnboundedSender<Job>,
    events: UnboundedReceiver<WriterEvent>,
    writer: JoinHandle<()>,
    /// `buffer_capacity_rows`: rows handed to the writer and not yet durable.
    permits: Arc<Semaphore>,
    /// Flushes waiting for room in the buffer, in order. The stream isn't read meanwhile.
    queued: VecDeque<(Job, u32)>,
    /// The checkpoint of the last flush handed on.
    submitted: SourcePosition,
    durable: SourcePosition,
    /// Halts since the last flush, recorded with the next one.
    halts: Vec<TableHalt>,
    /// The open transaction's schema changes.
    txn_schemas: Vec<TableSchema>,
    /// Schema changes of committed transactions, each cached once a checkpoint covers it.
    pending_schemas: Vec<(SourcePosition, TableSchema)>,
    draining: bool,
}

impl<'c> Session<'c> {
    /// Loads the durable state and opens the stream at the checkpoint; `Ok(Err)` when the source
    /// fails to open it.
    pub async fn open(core: &'c mut Core) -> Result<Result<Self, SourceError>, WorkerError> {
        let Some(slot) = core.settings.slot.clone() else {
            return Err(protocol("a stream session needs a PostgreSQL slot".into()));
        };
        let checkpoint = match core.docs.slot().await? {
            Some(v) if !v.doc.is_intent() => SourcePosition::Pg(v.doc.confirmed_flush_lsn),
            _ => return Err(protocol("a stream session needs a checkpoint".into())),
        };
        let cached: BTreeMap<TableRef, TableSchema> = core
            .docs
            .table_schemas()
            .await?
            .into_iter()
            .map(|s| (s.table().clone(), s))
            .collect();
        let recorded = core.docs.halts().await?.tables;
        let s = &core.settings;
        let mut acc = Accumulator::new(s.lineage.clone(), s.flush, checkpoint.clone());
        acc.set_txn_limits(s.limits);
        let mut halts = vec![];
        let mut schemas = BTreeMap::new();
        for table in &core.tracked {
            if recorded.iter().any(|h| &h.table == table) {
                acc.halt_table(table);
                continue;
            }
            let Some(schema) = cached.get(table) else {
                return Err(protocol(format!(
                    "tracked table {table} has no cached schema"
                )));
            };
            match acc.add_table(schema.clone(), &s.delete_extra(schema)) {
                Ok(()) => {
                    schemas.insert(table.clone(), schema.clone());
                }
                Err(AccumulatorError::Halt { table, reason }) => halts.push(TableHalt {
                    table,
                    reason,
                    at: checkpoint.clone(),
                }),
                Err(e) => return Err(e.into()),
            }
        }
        let live: Vec<TableSchema> = schemas.values().cloned().collect();
        let stream = match core.source.open_stream(&checkpoint, &live).await {
            Ok(stream) => stream,
            Err(e) => return Ok(Err(e)),
        };
        let (jobs, jobs_rx) = mpsc::unbounded_channel();
        let (events_tx, events) = mpsc::unbounded_channel();
        let ctx = WriterCtx {
            sink: core.sink.clone(),
            docs: core.docs.clone(),
            slot,
            pipeline_id: core.settings.pipeline_id.clone(),
            owner_epoch: core.owner_epoch,
            guard: core.guard.clone(),
            hooks: core.hooks.clone(),
            backoff: core.tuning.sink_backoff,
        };
        let writer = tokio::spawn(writer::run(ctx, jobs_rx, events_tx));
        let rows = usize::try_from(core.settings.buffer_rows)
            .unwrap_or(usize::MAX)
            .min(Semaphore::MAX_PERMITS);
        core.update(|st| {
            st.phase = Phase::Streaming;
            st.checkpoint = Some(checkpoint.clone());
            for h in recorded.iter().chain(&halts) {
                st.halted.insert(h.table.clone(), h.reason.clone());
            }
        });
        Ok(Ok(Self {
            core,
            acc,
            schemas,
            stream,
            jobs,
            events,
            writer,
            permits: Arc::new(Semaphore::new(rows)),
            queued: VecDeque::new(),
            submitted: checkpoint.clone(),
            durable: checkpoint,
            halts,
            txn_schemas: vec![],
            pending_schemas: vec![],
            draining: false,
        }))
    }

    /// Runs the session to its end. Flushes already handed to the writer are made durable before
    /// it returns, unless it fails.
    pub async fn run(mut self) -> Result<End, WorkerError> {
        match self.event_loop().await {
            Ok(end) => {
                let end = self.finish(end).await?;
                Ok(end)
            }
            Err(e) => {
                self.writer.abort();
                Err(e)
            }
        }
    }

    async fn finish(mut self, end: End) -> Result<End, WorkerError> {
        while let Some((mut job, _)) = self.queued.pop_front() {
            job.permit = None;
            let _ = self.jobs.send(job);
        }
        drop(self.jobs);
        while let Some(ev) = self.events.recv().await {
            match ev {
                WriterEvent::Failed(e) => return Err(e),
                WriterEvent::Halted(h) => self.core.update(|st| {
                    st.halted.insert(h.table, h.reason);
                }),
                WriterEvent::Degraded(why) => self.core.update(|st| st.degraded = why),
                WriterEvent::Durable(cp) => self.core.update(|st| st.checkpoint = Some(cp)),
            }
        }
        let _ = self.stream.close().await;
        Ok(end)
    }

    async fn event_loop(&mut self) -> Result<End, WorkerError> {
        let mut tick = tokio::time::interval(self.core.tuning.tick);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if self.draining && !self.acc.in_transaction() {
                self.flush()?;
                if let Some(end) = self.until_durable().await? {
                    return Ok(end);
                }
                return Ok(End::Exit(Exit::Drained));
            }
            let reading = self.queued.is_empty();
            let need = self.queued.front().map_or(0, |(_, n)| *n);
            let permits = self.permits.clone();
            tokio::select! {
                biased;
                cmd = self.core.control.recv() => self.command(cmd)?,
                ev = self.events.recv() => {
                    if let Some(end) = self.writer_event(ev).await? {
                        return Ok(end);
                    }
                }
                p = permits.acquire_many_owned(need), if !reading => {
                    self.send_front(p.expect("the semaphore isn't closed"));
                }
                item = self.stream.next(), if reading => match item {
                    Ok(item) => {
                        if let Some(end) = self.item(item).await? {
                            return Ok(end);
                        }
                    }
                    Err(e) => return Ok(End::Source(e)),
                },
                _ = tick.tick() => self.due(Instant::now())?,
            }
        }
    }

    fn command(&mut self, cmd: Option<Command>) -> Result<(), WorkerError> {
        match cmd {
            Some(Command::Flush) => self.flush(),
            Some(Command::Drain) | None => {
                self.draining = true;
                self.core.update(|st| st.phase = Phase::Draining);
                Ok(())
            }
        }
    }

    async fn writer_event(&mut self, ev: Option<WriterEvent>) -> Result<Option<End>, WorkerError> {
        match ev {
            None => Err(protocol("the sink writer ended".into())),
            Some(WriterEvent::Failed(e)) => Err(e),
            Some(WriterEvent::Halted(h)) => {
                self.acc.halt_table(&h.table);
                self.schemas.remove(&h.table);
                self.core.update(|st| {
                    st.halted.insert(h.table, h.reason);
                });
                Ok(None)
            }
            Some(WriterEvent::Degraded(why)) => {
                self.core.update(|st| st.degraded = why);
                Ok(None)
            }
            Some(WriterEvent::Durable(cp)) => {
                self.durable = cp.clone();
                if let Err(e) = self.stream.acknowledge(&cp).await {
                    return Ok(Some(End::Source(e)));
                }
                self.core.update(|st| st.checkpoint = Some(cp));
                Ok(None)
            }
        }
    }

    async fn item(&mut self, item: StreamItem) -> Result<Option<End>, WorkerError> {
        let now = Instant::now();
        match item {
            StreamItem::Begin { xid, .. } => self.acc.begin(xid)?,
            StreamItem::Change(e) => {
                loop {
                    match self.acc.apply(&e, now) {
                        Err(AccumulatorError::SplitRequired) => self.split()?,
                        r => break self.absorb(r)?,
                    }
                }
                if self.acc.due(now) == Some(FlushReason::Split) {
                    self.split()?;
                }
            }
            StreamItem::Truncate { tables, .. } => loop {
                match self.acc.truncate(&tables, now) {
                    Err(AccumulatorError::FlushRequired { .. }) => self.flush()?,
                    Err(AccumulatorError::Halt { table, reason }) => self.halt(table, reason),
                    r => break self.absorb(r)?,
                }
            },
            StreamItem::SchemaSignal { table, hint, .. } => {
                return self.schema_signal(table, hint).await;
            }
            StreamItem::TableHalt { table, reason, .. } => self.halt(table, reason),
            StreamItem::Commit(b) => {
                self.acc.commit(&b, now)?;
                let at = b.commit_pos;
                self.pending_schemas
                    .extend(self.txn_schemas.drain(..).map(|s| (at.clone(), s)));
                self.read();
                self.due(now)?;
            }
            StreamItem::KeepAlive(p) => {
                self.acc.keepalive(&p, now)?;
                self.read();
                self.due(now)?;
            }
        }
        Ok(None)
    }

    fn read(&self) {
        let frontier = self.acc.frontier().clone();
        self.core.update(|st| st.read = Some(frontier));
    }

    /// A table halt is the table's alone; anything else ends the worker.
    fn absorb(&mut self, r: Result<(), AccumulatorError>) -> Result<(), WorkerError> {
        match r {
            Ok(()) => Ok(()),
            Err(AccumulatorError::Halt { table, reason }) => {
                self.halt(table, reason);
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Stops the table: its buffered changes go, its later ones are dropped, and the halt is
    /// recorded before the next checkpoint. The sink holds its changes through the checkpoint.
    fn halt(&mut self, table: TableRef, reason: HaltReason) {
        self.acc.halt_table(&table);
        self.schemas.remove(&table);
        self.halts.push(TableHalt {
            table: table.clone(),
            reason: reason.clone(),
            at: self.acc.checkpoint().clone(),
        });
        self.core.update(|st| {
            st.halted.insert(table, reason);
        });
    }

    fn due(&mut self, now: Instant) -> Result<(), WorkerError> {
        match self.acc.due(now) {
            None => Ok(()),
            Some(FlushReason::Split) => self.split(),
            Some(_) => self.flush(),
        }
    }

    fn flush(&mut self) -> Result<(), WorkerError> {
        let f = self.acc.flush();
        self.submit(f.change_sets, f.checkpoint)
    }

    fn split(&mut self) -> Result<(), WorkerError> {
        let f = self.acc.split()?;
        self.submit(f.change_sets, f.checkpoint)
    }

    /// Hands a flush to the writer, with the halts since the last one and the schema changes its
    /// checkpoint covers; queued while the buffer is full.
    fn submit(
        &mut self,
        change_sets: Vec<data_avalanche_core::TableChangeSet>,
        checkpoint: SourcePosition,
    ) -> Result<(), WorkerError> {
        let mut schemas = vec![];
        let mut rest = vec![];
        for (at, schema) in self.pending_schemas.drain(..) {
            if checkpoint.covers(&at).map_err(AccumulatorError::from)? {
                schemas.push(schema);
            } else {
                rest.push((at, schema));
            }
        }
        self.pending_schemas = rest;
        if change_sets.is_empty()
            && checkpoint == self.submitted
            && self.halts.is_empty()
            && schemas.is_empty()
        {
            return Ok(());
        }
        let rows: usize = change_sets
            .iter()
            .map(|cs| cs.upsert_rows() + cs.delete_rows())
            .sum();
        let cap = self
            .core
            .settings
            .buffer_rows
            .min(Semaphore::MAX_PERMITS as u64);
        let need = u32::try_from((rows as u64).min(cap)).unwrap_or(u32::MAX);
        let job = Job {
            change_sets,
            checkpoint: checkpoint.clone(),
            halts: std::mem::take(&mut self.halts),
            snapshots: vec![],
            schemas,
            permit: None,
        };
        self.submitted = checkpoint;
        self.queued.push_back((job, need));
        if self.queued.len() == 1
            && let Ok(p) = self.permits.clone().try_acquire_many_owned(need)
        {
            self.send_front(p);
        }
        Ok(())
    }

    fn send_front(&mut self, permit: OwnedSemaphorePermit) {
        if let Some((mut job, _)) = self.queued.pop_front() {
            job.permit = Some(permit);
            let _ = self.jobs.send(job);
        }
    }

    /// Waits until every flush handed on is durable.
    async fn until_durable(&mut self) -> Result<Option<End>, WorkerError> {
        while self.durable != self.submitted || !self.queued.is_empty() {
            let need = self.queued.front().map_or(0, |(_, n)| *n);
            let permits = self.permits.clone();
            tokio::select! {
                biased;
                ev = self.events.recv() => {
                    if let Some(end) = self.writer_event(ev).await? {
                        return Ok(Some(end));
                    }
                }
                p = permits.acquire_many_owned(need), if !self.queued.is_empty() => {
                    self.send_front(p.expect("the semaphore isn't closed"));
                }
            }
        }
        Ok(None)
    }

    /// `Schema_Change…` §8.1 at the signal's position: the catalog read (channel C), the
    /// classification, then for a Safe change seal → flush → evolve, and the rebuild.
    async fn schema_signal(
        &mut self,
        table: TableRef,
        hint: SchemaHint,
    ) -> Result<Option<End>, WorkerError> {
        let Some(cached) = self.schemas.get(&table).cloned() else {
            return Ok(None);
        };
        let (delta, now) = match self.augment(&cached, &hint).await {
            Ok(found) => found,
            Err(reason) => {
                self.halt(table, reason);
                return Ok(None);
            }
        };
        let Some(now) = now else {
            self.halt(table, HaltReason::TableDropped);
            return Ok(None);
        };
        let mut safe = vec![];
        for (change, class) in classify(&cached, &delta) {
            match class {
                DeltaClass::Unsafe(reason) => {
                    self.halt(table, reason);
                    return Ok(None);
                }
                DeltaClass::Safe => safe.push(change),
                DeltaClass::Ignorable => {}
            }
        }
        if !safe.is_empty() {
            if !self.core.settings.evolution {
                self.halt(table, HaltReason::EvolutionDisabled);
                return Ok(None);
            }
            self.flush()?;
            if let Some(end) = self.until_durable().await? {
                return Ok(Some(end));
            }
            let delta = SchemaDelta {
                table: table.clone(),
                changes: safe,
            };
            match self.evolve(&delta).await? {
                Ok(None) => {}
                Ok(Some(warning)) => self.core.update(|st| st.warnings.push(warning)),
                Err(reason) => {
                    self.halt(table, reason);
                    return Ok(None);
                }
            }
        }
        let extra = self.core.settings.delete_extra(&now);
        loop {
            match self.acc.set_schema(now.clone(), &extra) {
                Err(AccumulatorError::FlushRequired { .. }) => self.flush()?,
                Err(AccumulatorError::Halt { table, reason }) => {
                    self.halt(table, reason);
                    return Ok(None);
                }
                r => break r?,
            }
        }
        self.schemas.insert(table, now.clone());
        if self.acc.in_transaction() {
            self.txn_schemas.push(now);
        } else {
            self.pending_schemas
                .push((self.acc.frontier().clone(), now));
        }
        Ok(None)
    }

    /// Channel C, retried while it fails transiently; a failure halts the table (fail-safe).
    async fn augment(
        &mut self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), HaltReason> {
        let attempts = self.core.tuning.augment_attempts.max(1);
        let mut attempt = 0;
        loop {
            match self.core.source.augment(cached, hint).await {
                Ok(found) => return Ok(found),
                Err(SourceError::TableHalt { reason, .. }) => return Err(reason),
                Err(e) if e.is_transient() && attempt + 1 < attempts => {
                    tokio::time::sleep(self.core.tuning.source_backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(HaltReason::UnsafeDdl {
                        detail: format!("the catalog read for a schema change failed: {e}"),
                    });
                }
            }
        }
    }

    /// The sink's evolve, retried while it fails transiently. `Ok(Some(warning))` for a degraded
    /// apply; `Err` halts the table.
    async fn evolve(
        &mut self,
        delta: &SchemaDelta,
    ) -> Result<Result<Option<String>, HaltReason>, WorkerError> {
        let mut attempt = 0;
        loop {
            if !self.core.guard.may_commit() {
                return Err(WorkerError::LeaseLost);
            }
            let outcome = self.core.sink.evolve(&delta.table, delta).await;
            if attempt > 0 && outcome.is_ok() {
                self.core.update(|st| st.degraded = None);
            }
            match outcome {
                Ok(EvolveOutcome::Applied) => return Ok(Ok(None)),
                Ok(EvolveOutcome::Degraded { warning }) => {
                    return Ok(Ok(Some(format!("table {}: {warning}", delta.table))));
                }
                Ok(EvolveOutcome::Unsupported { reason }) => {
                    return Ok(Err(HaltReason::EvolutionUnsupported { detail: reason }));
                }
                Err(SinkError::Transient { reason }) => {
                    self.core.update(|st| {
                        st.degraded = Some(format!("table {}: {reason}", delta.table));
                    });
                    tokio::time::sleep(self.core.tuning.sink_backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(SinkError::Permanent { reason }) => {
                    return Ok(Err(HaltReason::SinkRejected { detail: reason }));
                }
                Err(e) => return Err(WorkerError::Sink(e)),
            }
        }
    }
}
```

Create `crates/data-avalanche-pipeline/src/status.rs`:

```rust
//! What a running worker shows and takes: its status, the commands it obeys, why it ended, and
//! the timings its retries use.

use std::collections::BTreeMap;
use std::time::Duration;

use data_avalanche_core::{HaltReason, SourcePosition, TableRef};

use crate::Backoff;

/// A command to a running worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    /// Finish the open transaction, flush, make it durable, close the stream and end with
    /// [`Exit::Drained`] (pause, stop and failover hand-off, Pipeline_Lifecycle §4).
    Drain,
    /// Flush now, whatever the flush policy says.
    Flush,
}

/// How a worker ended without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// A [`Command::Drain`] (or the handle dropped): everything read is durable.
    Drained,
    /// Every table is a `dump_*` table whose snapshot is done.
    Dumped,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Phase {
    #[default]
    Starting,
    Streaming,
    /// The stream failed transiently; the worker reopens it after a backoff.
    Reconnecting {
        error: String,
    },
    Draining,
}

/// A running table snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotProgress {
    /// Rows emitted, this run and the runs before it.
    pub rows_done: u64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct WorkerStatus {
    pub phase: Phase,
    /// The stream's frontier: the last complete transaction (or keep-alive) read.
    pub read: Option<SourcePosition>,
    /// The durable checkpoint: what the source was last told it may release.
    pub checkpoint: Option<SourcePosition>,
    /// Sink commits or state writes are failing and being retried (the last error).
    pub degraded: Option<String>,
    pub halted: BTreeMap<TableRef, HaltReason>,
    pub snapshots: BTreeMap<TableRef, SnapshotProgress>,
    /// Warnings since the start: drift, a `never` gap, a degraded evolve.
    pub warnings: Vec<String>,
}

/// The worker's internal timings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuning {
    /// Between stream sessions after a transient source failure (Data-Flow §Source Disconnect).
    pub source_backoff: Backoff,
    /// Between attempts of a failing sink commit or state write.
    pub sink_backoff: Backoff,
    /// Between checks that a snapshot table's in-flight writers have finished (DA-SNAP-9).
    pub in_flight_backoff: Backoff,
    /// How long a busy stream position is retried (`Postgres_Slot…` D-15.5).
    pub contended_window: Duration,
    /// How often the age flush policy and the snapshot timeout are checked.
    pub tick: Duration,
    /// Attempts of a schema signal's catalog read before the table halts (`Schema_Change…` §3 C).
    pub augment_attempts: u32,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            source_backoff: Backoff::default(),
            sink_backoff: Backoff::default(),
            in_flight_backoff: Backoff {
                first: Duration::from_millis(500),
                max: Duration::from_secs(10),
            },
            contended_window: Duration::from_secs(120),
            tick: Duration::from_secs(1),
            augment_attempts: 3,
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

PASS:

```text
test result: ok. 79 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): the stream session: flushes, backpressure, barriers, splits, halts and schema signals"
```


### Task 8: `re_query`, and channel D once the stream catches up

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/session.rs`, `…/test_support.rs`

**Interfaces:**
- Consumes: `SourceConnector::fetch_rows`, `rescan`, `current_position` (M5); Task 7's session.
- Produces: no new public API. With `toast_strategy: re_query` the session re-reads the keys
  whose UPDATE left values unchanged and applies them at the commit (D-M6a.6). Every
  `schema_evolution.rescan_interval` it rescans; the report applies once the stream has passed
  the position read after the rescan (Q-M6a-4, D-M6a.5).

Two M5-carried items. `fetch_rows` is the explicit alternative to carry-forward (Q-M5-6).
Channel D finds what the stream never says: an idle table's change, and a dropped table (on
PostgreSQL a DROP sends nothing).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/session.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -862,6 +862,99 @@ mod tests {
         assert_eq!(w.ids("a"), [1]);
     }
 
+    #[tokio::test]
+    async fn re_query_reads_the_values_the_stream_left_unchanged() {
+        for (strategy, want) in [("re_query", Some("big")), ("carry_forward", None)] {
+            let w = World::new(&["a"]);
+            w.started(&["a"]).await;
+            w.source.load(&t("a"), vec![row(1, "big")]);
+            let source = format!("\n      toast_strategy: {strategy}");
+            let p = Yaml {
+                source: &source,
+                sink: "flush: { seconds: 0 }",
+                ..Yaml::default()
+            }
+            .build();
+            let (end, _) = session(&w, &p, &["a"], async |status| {
+                let unchanged = Row(vec![
+                    ("id".into(), ColumnValue::Int64(1)),
+                    ("doc".into(), ColumnValue::Unchanged),
+                ]);
+                w.source.commit(FakeTxn::new().update(&t("a"), unchanged));
+                durable(&status, w.source.wal_end()).await;
+            })
+            .await;
+            end.unwrap();
+            assert_eq!(
+                w.text("a", "doc"),
+                [want.map(String::from)],
+                "{strategy}: the sink has no row to keep the value from"
+            );
+        }
+    }
+
+    /// A session over `w` with channel D every few milliseconds.
+    async fn rescanning<F: Future<Output = ()>>(
+        w: &World,
+        p: &data_avalanche_config::Pipeline,
+        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
+    ) -> WorkerStatus {
+        let (mut core, control, status) = w.core(p, &["a"]);
+        core.settings.rescan_interval = Some(Duration::from_millis(5));
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        let watching = status.clone();
+        let driver = async move {
+            drive(watching).await;
+            let _ = control.send(Command::Drain).await;
+        };
+        let (end, ()) = tokio::join!(s.run(), driver);
+        end.unwrap();
+        status.borrow().clone()
+    }
+
+    #[tokio::test]
+    async fn a_rescan_evolves_an_idle_tables_change() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        rescanning(&w, &flush_each(), async |_| {
+            w.source.alter(&t("a"), with_note("a"));
+            wait_until("the sink has the column", || {
+                w.sink
+                    .content(&t("a"))
+                    .is_some_and(|c| c.column_by_name("note").is_some())
+            })
+            .await;
+        })
+        .await;
+        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
+        assert!(cached.column("note").is_some());
+    }
+
+    #[tokio::test]
+    async fn a_dropped_table_halts_once_the_stream_has_its_last_changes() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let p = pipeline("", "flush: { seconds: 0 }, buffer_capacity_rows: 1", "");
+        w.sink.hold_commits();
+        let st = rescanning(&w, &p, async |status| {
+            for id in 1..=3 {
+                w.source
+                    .commit(FakeTxn::new().insert(&t("a"), row(id, "x")));
+            }
+            w.source.drop_table(&t("a"));
+            tokio::time::sleep(Duration::from_millis(50)).await;
+            assert!(
+                status.borrow().halted.is_empty(),
+                "the stream hasn't read the third insert yet"
+            );
+            w.sink.release_commits();
+            wait_until("the table halts", || !status.borrow().halted.is_empty()).await;
+        })
+        .await;
+        assert_eq!(st.halted.get(&t("a")), Some(&HaltReason::TableDropped));
+        assert_eq!(w.ids("a"), [1, 2, 3], "nothing before the drop is lost");
+    }
+
     fn with_note(name: &str) -> TableSchema {
         TableSchema::new(
             t(name),
```

Change `crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -5,7 +5,7 @@ use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::{Arc, Mutex};
 use std::time::{Duration, Instant};
 
-use arrow_array::Int64Array;
+use arrow_array::{Array, Int64Array, StringArray};
 use async_trait::async_trait;
 use bytes::Bytes;
 use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
@@ -359,6 +359,22 @@ impl World {
         (core, control, status_rx)
     }
 
+    /// The table's text `column` in the sink, in key order.
+    pub fn text(&self, table: &str, column: &str) -> Vec<Option<String>> {
+        let Some(b) = self.sink.content(&TableRef::new("public", table)) else {
+            return vec![];
+        };
+        let values = b
+            .column_by_name(column)
+            .expect("the column")
+            .as_any()
+            .downcast_ref::<StringArray>()
+            .expect("a text column");
+        (0..values.len())
+            .map(|i| values.is_valid(i).then(|| values.value(i).to_owned()))
+            .collect()
+    }
+
     /// The table's `id` values in the sink, in key order.
     pub fn ids(&self, table: &str) -> Vec<i64> {
         let Some(b) = self.sink.content(&TableRef::new("public", table)) else {
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

FAIL:

```text
test result: FAILED. 79 passed; 3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.07s

failures:
    session::tests::a_dropped_table_halts_once_the_stream_has_its_last_changes
    session::tests::a_rescan_evolves_an_idle_tables_change
    session::tests::re_query_reads_the_values_the_stream_left_unchanged
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -12,8 +12,9 @@ use std::sync::Arc;
 use std::time::Instant;
 
 use data_avalanche_core::{
-    ChangeStream, DeltaClass, EvolveOutcome, HaltReason, SchemaDelta, SchemaHint, Sink, SinkError,
-    SourceConnector, SourceError, SourcePosition, StreamItem, TableRef, TableSchema, classify,
+    CdcEvent, ChangeStream, ColumnValue, DeltaClass, EvolveOutcome, HaltReason, Op, RescanReport,
+    Row, SchemaDelta, SchemaHint, Sink, SinkError, SourceConnector, SourceError, SourcePosition,
+    SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary, classify, identifier_values,
 };
 use data_avalanche_state::{PipelineDocs, TableHalt};
 use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
@@ -83,6 +84,12 @@ pub(crate) struct Session<'c> {
     txn_schemas: Vec<TableSchema>,
     /// Schema changes of committed transactions, each cached once a checkpoint covers it.
     pending_schemas: Vec<(SourcePosition, TableSchema)>,
+    /// `toast_strategy: re_query`: the keys whose UPDATE in the open transaction carried
+    /// unchanged values, and the transaction's last change.
+    requery: BTreeMap<TableRef, Vec<Row>>,
+    last_seq: Option<SourceSeq>,
+    /// A rescan's findings, applied once the stream passes the position read after it.
+    rescan: Option<(SourcePosition, RescanReport)>,
     draining: bool,
 }
 
@@ -175,6 +182,9 @@ impl<'c> Session<'c> {
             halts,
             txn_schemas: vec![],
             pending_schemas: vec![],
+            requery: BTreeMap::new(),
+            last_seq: None,
+            rescan: None,
             draining: false,
         }))
     }
@@ -217,6 +227,11 @@ impl<'c> Session<'c> {
     async fn event_loop(&mut self) -> Result<End, WorkerError> {
         let mut tick = tokio::time::interval(self.core.tuning.tick);
         tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
+        let mut rescan = self
+            .core
+            .settings
+            .rescan_interval
+            .map(|every| tokio::time::interval_at(tokio::time::Instant::now() + every, every));
         loop {
             if self.draining && !self.acc.in_transaction() {
                 self.flush()?;
@@ -248,6 +263,13 @@ impl<'c> Session<'c> {
                     Err(e) => return Ok(End::Source(e)),
                 },
                 _ = tick.tick() => self.due(Instant::now())?,
+                _ = async { rescan.as_mut().expect("guarded").tick().await },
+                    if rescan.is_some() && self.rescan.is_none() =>
+                {
+                    if let Some(end) = self.rescan().await? {
+                        return Ok(end);
+                    }
+                }
             }
         }
     }
@@ -293,17 +315,17 @@ impl<'c> Session<'c> {
     async fn item(&mut self, item: StreamItem) -> Result<Option<End>, WorkerError> {
         let now = Instant::now();
         match item {
-            StreamItem::Begin { xid, .. } => self.acc.begin(xid)?,
+            StreamItem::Begin { xid, .. } => {
+                self.acc.begin(xid)?;
+                self.requery.clear();
+                self.last_seq = None;
+            }
             StreamItem::Change(e) => {
-                loop {
-                    match self.acc.apply(&e, now) {
-                        Err(AccumulatorError::SplitRequired) => self.split()?,
-                        r => break self.absorb(r)?,
-                    }
-                }
-                if self.acc.due(now) == Some(FlushReason::Split) {
-                    self.split()?;
+                if self.core.settings.re_query {
+                    self.note_unchanged(&e);
                 }
+                self.last_seq = Some(e.change_seq);
+                self.apply(&e, now)?;
             }
             StreamItem::Truncate { tables, .. } => loop {
                 match self.acc.truncate(&tables, now) {
@@ -317,19 +339,163 @@ impl<'c> Session<'c> {
             }
             StreamItem::TableHalt { table, reason, .. } => self.halt(table, reason),
             StreamItem::Commit(b) => {
+                if let Some(end) = self.re_query(&b).await? {
+                    return Ok(Some(end));
+                }
                 self.acc.commit(&b, now)?;
                 let at = b.commit_pos;
                 self.pending_schemas
                     .extend(self.txn_schemas.drain(..).map(|s| (at.clone(), s)));
-                self.read();
-                self.due(now)?;
+                return self.boundary(now).await;
             }
             StreamItem::KeepAlive(p) => {
                 self.acc.keepalive(&p, now)?;
-                self.read();
-                self.due(now)?;
+                return self.boundary(now).await;
+            }
+        }
+        Ok(None)
+    }
+
+    /// Between transactions: what waited for the stream to get here, then the flush policy.
+    async fn boundary(&mut self, now: Instant) -> Result<Option<End>, WorkerError> {
+        self.read();
+        if let Some(end) = self.caught_up().await? {
+            return Ok(Some(end));
+        }
+        self.due(now)?;
+        Ok(None)
+    }
+
+    fn apply(&mut self, e: &CdcEvent, now: Instant) -> Result<(), WorkerError> {
+        loop {
+            match self.acc.apply(e, now) {
+                Err(AccumulatorError::SplitRequired) => self.split()?,
+                r => break self.absorb(r)?,
+            }
+        }
+        if self.acc.due(now) == Some(FlushReason::Split) {
+            self.split()?;
+        }
+        Ok(())
+    }
+
+    /// Notes an UPDATE's key when the source sent some of its values as unchanged.
+    fn note_unchanged(&mut self, e: &CdcEvent) {
+        let Some(after) = &e.after else { return };
+        if e.op != Op::Update || !after.0.iter().any(|(_, v)| *v == ColumnValue::Unchanged) {
+            return;
+        }
+        let Some(key) = self
+            .schemas
+            .get(&e.table)
+            .and_then(|s| identifier_values(s, after))
+        else {
+            return;
+        };
+        let keys = self.requery.entry(e.table.clone()).or_default();
+        if !keys.contains(&key) {
+            keys.push(key);
+        }
+    }
+
+    /// `toast_strategy: re_query` at the commit: reads the current rows of the noted keys and
+    /// applies them as the transaction's last change (Data-Flow §TOAST). A key whose row is gone
+    /// is left to the delete that follows in the stream.
+    async fn re_query(&mut self, b: &TransactionBoundary) -> Result<Option<End>, WorkerError> {
+        let Some(seq) = self.last_seq else {
+            return Ok(None);
+        };
+        for (table, keys) in std::mem::take(&mut self.requery) {
+            let Some(schema) = self.schemas.get(&table).cloned() else {
+                continue;
+            };
+            match self.core.source.fetch_rows(&schema, &keys).await {
+                Ok(rows) => {
+                    for row in rows {
+                        let e = CdcEvent {
+                            op: Op::Update,
+                            table: table.clone(),
+                            before: None,
+                            after: Some(row),
+                            source_pos: b.commit_pos.clone(),
+                            change_seq: seq,
+                            xid: b.xid,
+                            commit_ts_micros: b.commit_ts_micros,
+                        };
+                        self.apply(&e, Instant::now())?;
+                    }
+                }
+                Err(SourceError::TableHalt { table, reason }) => self.halt(table, reason),
+                Err(e) => return Ok(Some(End::Source(e))),
+            }
+        }
+        Ok(None)
+    }
+
+    /// Channel D: one catalog read of the streamed tables, and the source's position after it. A
+    /// failed round waits for the next.
+    async fn rescan(&mut self) -> Result<Option<End>, WorkerError> {
+        let known: Vec<TableSchema> = self.schemas.values().cloned().collect();
+        let Ok(report) = self.core.source.rescan(&known).await else {
+            return Ok(None);
+        };
+        if report == RescanReport::default() {
+            return Ok(None);
+        }
+        let Ok(after) = self.core.source.current_position().await else {
+            return Ok(None);
+        };
+        self.rescan = Some((after, report));
+        if self.acc.in_transaction() {
+            return Ok(None);
+        }
+        self.caught_up().await
+    }
+
+    /// Applies a rescan's findings once the stream has passed the position read after it (Q-M6a-4):
+    /// every change the stream holds for those tables is in by then. A change the stream applied
+    /// on its own meanwhile is skipped.
+    async fn caught_up(&mut self) -> Result<Option<End>, WorkerError> {
+        let Some((after, _)) = &self.rescan else {
+            return Ok(None);
+        };
+        if !self
+            .acc
+            .frontier()
+            .covers(after)
+            .map_err(AccumulatorError::from)?
+        {
+            return Ok(None);
+        }
+        let (_, report) = self.rescan.take().expect("checked above");
+        // The tables' changes up to here are good: flush them before the halts drop what is
+        // buffered.
+        self.flush()?;
+        for table in report.dropped {
+            if self.schemas.contains_key(&table) {
+                self.halt(table, HaltReason::TableDropped);
+            }
+        }
+        for (table, reason) in report.halted {
+            if self.schemas.contains_key(&table) {
+                self.halt(table, reason);
+            }
+        }
+        for now in report.changed {
+            let table = now.table().clone();
+            let Some(cached) = self.schemas.get(&table) else {
+                continue;
+            };
+            if cached.columns() == now.columns() && cached.identifier() == now.identifier() {
+                continue;
+            }
+            let hint = SchemaHint::Columns(now.columns().to_vec());
+            if let Some(end) = self.schema_signal(table, hint).await? {
+                return Ok(Some(end));
             }
         }
+        // Caches the applied schemas without waiting for the tables' next change.
+        self.flush()?;
         Ok(None)
     }
 
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

PASS:

```text
test result: ok. 82 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): re_query at the commit, and channel D applied once the stream catches up"
```


### Task 9: The DA-SNAP-9 reconciler

**Files:**
- Create: `crates/data-avalanche-pipeline/src/reconcile.rs`
- Modify: `crates/data-avalanche-pipeline/src/buffer.rs`, `…/lib.rs`

**Interfaces:**
- Consumes: Task 1's `ChunkVisibility`; the buffer's key comparison (M3).
- Produces (crate):
  - `buffer::Key` and `buffer::identity(&TableSchema, &Row) -> Option<Key>`: a row's identity
    as the buffer compares it.
  - `Touched { keys, truncated }` (what one stream transaction wrote in the tables being
    snapshotted); `Reconciler::applied(xid, Touched)`, `issue() -> Ticket`, `cancel(Ticket)`,
    `reconcile(Ticket, &TableRef, Option<&ChunkVisibility>, Vec<(Key, Row)>) -> Vec<Row>`,
    `clear()`.

A chunk row must not undo a change the pipeline already took from the stream. The reconciler
logs the transactions applied while a snapshot runs, with the keys they wrote. At emission it
drops a chunk's rows for keys a logged transaction wrote that the chunk's read didn't see, and
drops the whole chunk on an unseen TRUNCATE. An entry is kept until a chunk proves it visible,
and until every read still outstanding was issued after that proof. A source without visibility
gets the window rule: the transactions applied after the read was issued.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/buffer.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-pipeline/src/buffer.rs b/crates/data-avalanche-pipeline/src/buffer.rs
--- a/crates/data-avalanche-pipeline/src/buffer.rs
+++ b/crates/data-avalanche-pipeline/src/buffer.rs
@@ -1185,4 +1185,37 @@ mod tests {
         assert_eq!(err.code(), "SCHEMA_MISMATCH");
         assert_eq!(buffer().delete_columns(), [0, 2]);
     }
+
+    #[test]
+    fn a_rows_identity_is_its_key_as_the_buffer_compares_it() {
+        let s = TableSchema::new(
+            TableRef::new("public", "n"),
+            vec![
+                ColumnSchema::new(
+                    "k",
+                    V::Numeric {
+                        precision: 6,
+                        scale: 2,
+                    },
+                    "numeric(6,2)",
+                )
+                .not_null(),
+                ColumnSchema::new("v", V::Text, "text"),
+            ],
+            vec!["k".into()],
+        )
+        .unwrap();
+        let key = |d| Row(vec![("k".into(), ColumnValue::Numeric(d))]);
+        assert_eq!(
+            identity(&s, &key(Decimal::new(15, 1))),
+            identity(&s, &key(Decimal::new(150, 2))),
+            "1.5 and 1.50 are one key"
+        );
+        assert!(identity(&s, &key(Decimal::new(15, 1))).is_some());
+        assert_eq!(identity(&s, &Row(vec![("v".into(), text("x"))])), None);
+        assert_eq!(
+            identity(&s, &Row(vec![("k".into(), ColumnValue::Null)])),
+            None
+        );
+    }
 }
```

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -17,6 +17,11 @@ mod decide;
 pub mod error;
 pub mod hooks;
 pub mod policy;
+#[allow(
+    dead_code,
+    reason = "the snapshot runner uses it (Task 10); the allow goes there"
+)]
+mod reconcile;
 #[allow(
     dead_code,
     reason = "the worker runs it (Task 11); the allow goes there"
```

Create `crates/data-avalanche-pipeline/src/reconcile.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnValue, XidSnapshot};

    use super::*;
    use crate::buffer::identity;
    use crate::test_support::{row, schema, t};

    fn key(id: i64) -> Key {
        identity(&schema("a"), &row(id, "x")).unwrap()
    }

    fn chunk(ids: &[i64]) -> Vec<(Key, Row)> {
        ids.iter().map(|&id| (key(id), row(id, "snap"))).collect()
    }

    fn ids(rows: &[Row]) -> Vec<i64> {
        rows.iter()
            .map(|r| match r.get("id") {
                Some(ColumnValue::Int64(i)) => *i,
                other => panic!("{other:?}"),
            })
            .collect()
    }

    fn wrote(table: &str, ids: &[i64]) -> Touched {
        let mut t = Touched::default();
        for &id in ids {
            t.key(&TableRef::new("public", table), key(id));
        }
        t
    }

    /// A snapshot that sees every transaction below `xmax` except `xip`.
    fn snap(xmax: u64, xip: &[u64]) -> ChunkVisibility {
        ChunkVisibility::Xids(XidSnapshot {
            xmin: xip.iter().copied().min().unwrap_or(xmax),
            xmax,
            xip: xip.to_vec(),
        })
    }

    #[test]
    fn a_change_the_read_didnt_see_drops_its_rows() {
        let mut r = Reconciler::default();
        let c = r.issue();
        r.applied(700, wrote("a", &[2]));
        let kept = r.reconcile(c, &t("a"), Some(&snap(700, &[])), chunk(&[1, 2, 3]));
        assert_eq!(ids(&kept), [1, 3], "700 committed after the read began");
    }

    #[test]
    fn a_change_applied_before_the_issue_but_still_invisible_drops_its_rows() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[2]));
        let c = r.issue();
        let kept = r.reconcile(c, &t("a"), Some(&snap(701, &[700])), chunk(&[1, 2]));
        assert_eq!(
            ids(&kept),
            [1],
            "a synchronous commit decoded before it was visible"
        );
        assert_eq!(r.logged(), [700], "kept until a read sees it");
    }

    #[test]
    fn a_change_the_read_saw_keeps_the_rows_and_goes() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[2]));
        let c = r.issue();
        let kept = r.reconcile(c, &t("a"), Some(&snap(701, &[])), chunk(&[1, 2]));
        assert_eq!(ids(&kept), [1, 2]);
        assert!(r.logged().is_empty(), "seen, and no read is open");
    }

    #[test]
    fn an_unseen_truncate_drops_the_chunk() {
        let mut r = Reconciler::default();
        let c = r.issue();
        let mut tr = Touched::default();
        tr.truncated.insert(t("a"));
        r.applied(700, tr);
        let kept = r.reconcile(c, &t("a"), Some(&snap(700, &[])), chunk(&[1, 2]));
        assert!(kept.is_empty());
    }

    #[test]
    fn another_tables_keys_dont_drop_rows() {
        let mut r = Reconciler::default();
        let c = r.issue();
        r.applied(700, wrote("b", &[1]));
        let kept = r.reconcile(c, &t("a"), Some(&snap(700, &[])), chunk(&[1]));
        assert_eq!(ids(&kept), [1]);
    }

    /// D was issued before C proved 700 visible, so D's read may be older than C's: 700 stays
    /// until D is reconciled.
    #[test]
    fn a_proof_covers_only_the_reads_issued_after_it() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[2]));
        let d = r.issue();
        let c = r.issue();
        let kept = r.reconcile(c, &t("a"), Some(&snap(701, &[])), chunk(&[2]));
        assert_eq!(ids(&kept), [2]);
        assert_eq!(r.logged(), [700], "D is still being read");
        let kept = r.reconcile(d, &t("a"), Some(&snap(701, &[700])), chunk(&[2, 3]));
        assert_eq!(ids(&kept), [3], "D's read didn't see 700");
        assert!(r.logged().is_empty());
    }

    #[test]
    fn a_failed_read_doesnt_hold_the_log() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[2]));
        let d = r.issue();
        let c = r.issue();
        r.reconcile(c, &t("a"), Some(&snap(701, &[])), chunk(&[]));
        r.cancel(d);
        let e = r.issue();
        r.reconcile(e, &t("a"), Some(&snap(701, &[])), chunk(&[]));
        assert!(r.logged().is_empty());
    }

    #[test]
    fn without_visibility_the_changes_applied_after_the_issue_drop_their_rows() {
        let mut r = Reconciler::default();
        r.applied(700, wrote("a", &[1]));
        let c = r.issue();
        r.applied(701, wrote("a", &[2]));
        let kept = r.reconcile(c, &t("a"), None, chunk(&[1, 2, 3]));
        assert_eq!(ids(&kept), [1, 3]);
        assert!(r.logged().is_empty(), "no read is open");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

FAIL to compile (11 errors); the first:

```text
error[E0432]: unresolved import `crate::buffer::identity`
error[E0425]: cannot find function `identity` in this scope
error[E0425]: cannot find type `Key` in this scope
error[E0425]: cannot find type `Row` in this scope
error[E0425]: cannot find type `Touched` in this scope
error[E0425]: cannot find type `ChunkVisibility` in this scope
… and 5 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-pipeline/src/buffer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/buffer.rs b/crates/data-avalanche-pipeline/src/buffer.rs
--- a/crates/data-avalanche-pipeline/src/buffer.rs
+++ b/crates/data-avalanche-pipeline/src/buffer.rs
@@ -37,7 +37,7 @@ pub struct CollapsedRow {
 /// JSON ([`TableSchema::new`]), and decimals are rescaled to the column's scale on the way in, so
 /// equal values have one representation.
 #[derive(Debug, Clone, PartialEq, Eq, Hash)]
-struct Key(Vec<KeyPart>);
+pub(crate) struct Key(Vec<KeyPart>);
 
 #[derive(Debug, Clone, PartialEq, Eq, Hash)]
 enum KeyPart {
@@ -68,6 +68,25 @@ fn key_part(v: &ColumnValue) -> Option<KeyPart> {
     })
 }
 
+/// The key of `row` in `schema`, compared as the buffer compares keys; `None` when an identifier
+/// value is missing, NULL or doesn't fit its column.
+#[allow(
+    dead_code,
+    reason = "the snapshot runner keys chunk rows with it (Task 10); the allow goes there"
+)]
+pub(crate) fn identity(schema: &TableSchema, row: &Row) -> Option<Key> {
+    schema
+        .identifier()
+        .iter()
+        .map(|name| {
+            let c = schema.column(name)?;
+            let v = conform_value(row.get(name)?.clone(), &c.value_type, &c.logical).ok()?;
+            key_part(&v)
+        })
+        .collect::<Option<_>>()
+        .map(Key)
+}
+
 /// Rough in-memory size of a row, for the flush `bytes` trigger and the transaction buffer ceiling.
 pub(crate) fn row_bytes(values: &[ColumnValue]) -> usize {
     fn value_bytes(v: &ColumnValue) -> usize {
```

Add the implementation to `crates/data-avalanche-pipeline/src/reconcile.rs`, above the test module:

```rust
//! A snapshot chunk against the stream (DA-SNAP-9, Data-Flow Phase 1). A chunk row must not undo
//! a change the pipeline already took from the stream. That can happen when the chunk's read
//! didn't see the change's transaction: it committed after the read started, or it was committed
//! and decoded but not yet visible (a synchronous commit waiting for its standby, measured on
//! PostgreSQL 18).
//!
//! The reconciler logs the transactions applied from the stream while a snapshot runs, with the
//! keys they wrote in the tables being snapshotted. Before a chunk is emitted, its rows for keys
//! that a logged transaction wrote, and that the chunk's read didn't see, are dropped: the stream
//! already has those keys. A logged TRUNCATE the read didn't see drops the whole chunk.
//!
//! A logged transaction is kept until a chunk proves it visible, and until every chunk still
//! being read was issued after that proof: a read that starts later sees what an earlier read
//! saw. A source that can't say what a chunk's read saw ([`Chunk::visibility`] `None`) gets the
//! window rule: the transactions applied after the chunk was issued.
//!
//! [`Chunk::visibility`]: data_avalanche_core::Chunk::visibility

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use data_avalanche_core::{ChunkVisibility, Row, TableRef};

use crate::buffer::Key;

/// What one stream transaction wrote in the tables being snapshotted.
#[derive(Debug, Default)]
pub(crate) struct Touched {
    pub keys: HashMap<TableRef, HashSet<Key>>,
    pub truncated: BTreeSet<TableRef>,
}

impl Touched {
    pub fn key(&mut self, table: &TableRef, key: Key) {
        self.keys.entry(table.clone()).or_default().insert(key);
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.truncated.is_empty()
    }
}

/// One chunk read in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ticket {
    id: u64,
    issued: u64,
}

#[derive(Debug)]
struct Entry {
    /// When it was applied, on the reconciler's clock.
    at: u64,
    xid: u64,
    touched: Touched,
    /// When a chunk first proved it visible.
    seen: Option<u64>,
}

#[derive(Debug, Default)]
pub(crate) struct Reconciler {
    /// Orders applies, issues and reconciliations.
    clock: u64,
    next_id: u64,
    log: Vec<Entry>,
    /// The chunks being read: id → when issued.
    outstanding: BTreeMap<u64, u64>,
}

impl Reconciler {
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// A stream transaction the pipeline applied.
    pub fn applied(&mut self, xid: u64, touched: Touched) {
        let at = self.tick();
        if !touched.is_empty() {
            self.log.push(Entry {
                at,
                xid,
                touched,
                seen: None,
            });
        }
    }

    /// A chunk read starts.
    pub fn issue(&mut self) -> Ticket {
        let issued = self.tick();
        self.next_id += 1;
        self.outstanding.insert(self.next_id, issued);
        Ticket {
            id: self.next_id,
            issued,
        }
    }

    /// A chunk read failed: nothing of it is emitted.
    pub fn cancel(&mut self, ticket: Ticket) {
        self.outstanding.remove(&ticket.id);
    }

    /// The rows of a chunk of `table` the stream holds no newer change for. Called when the chunk
    /// is emitted, so it counts every transaction applied until then.
    pub fn reconcile(
        &mut self,
        ticket: Ticket,
        table: &TableRef,
        visibility: Option<&ChunkVisibility>,
        rows: Vec<(Key, Row)>,
    ) -> Vec<Row> {
        self.outstanding.remove(&ticket.id);
        let now = self.tick();
        let mut stale: HashSet<&Key> = HashSet::new();
        let mut truncated = false;
        for e in &mut self.log {
            let hidden = match visibility {
                Some(ChunkVisibility::Xids(s)) => {
                    let sees = s.sees(e.xid);
                    if sees {
                        e.seen.get_or_insert(now);
                    }
                    !sees
                }
                None => e.at > ticket.issued,
            };
            if hidden {
                truncated |= e.touched.truncated.contains(table);
                if let Some(keys) = e.touched.keys.get(table) {
                    stale.extend(keys);
                }
            }
        }
        let kept = if truncated {
            vec![]
        } else {
            rows.into_iter()
                .filter(|(k, _)| !stale.contains(k))
                .map(|(_, r)| r)
                .collect()
        };
        let horizon = self.outstanding.values().min().copied().unwrap_or(u64::MAX);
        self.log.retain(|e| match visibility {
            Some(_) => e.seen.is_none_or(|seen| seen >= horizon),
            None => e.at >= horizon,
        });
        kept
    }

    /// No snapshot runs any more: nothing needs the log.
    pub fn clear(&mut self) {
        self.log.clear();
        self.outstanding.clear();
    }

    #[cfg(test)]
    fn logged(&self) -> Vec<u64> {
        self.log.iter().map(|e| e.xid).collect()
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

PASS:

```text
test result: ok. 91 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): snapshot chunks reconciled against the stream by what their read saw (DA-SNAP-9)"
```


### Task 10: Snapshots inside the stream session

**Files:**
- Create: `crates/data-avalanche-pipeline/src/session/snapshot.rs`, `…/cursor.rs`
- Modify: `crates/data-avalanche-pipeline/src/session.rs`, `…/buffer.rs`, `…/settings.rs`,
  `…/lib.rs`, `…/test_support.rs`, `…/Cargo.toml`

**Interfaces:**
- Consumes: Task 9's reconciler, `SourceConnector::read_chunk` and `in_flight`, Task 3's
  snapshot documents (M2), Task 1's snapshot flag.
- Produces:
  - `cursor::to_doc(&TableSchema, &KeysetCursor, chunk_size, rows_done) ->
    Result<SnapshotCursor, String>` and `from_doc(&TableSchema, &SnapshotCursor) ->
    Result<KeysetCursor, String>` (crate). Integers, dates, times and timestamps are numbers;
    decimals and UUIDs text; bytes hex.
  - `Snapshots` (session-private): the tables being snapshotted, their reads, the reconciler,
    the writers in flight at the start (Q-M6a-2), the documents for the next flush.
  - `WorkerStatus::snapshots` lists each running snapshot's `rows_done`.

Snapshots run in the stream session, up to `parallelism` reads at a time. A chunk's read is
reconciled and emitted between transactions. Emission flushes the stream's buffered changes,
then the chunk goes out as a synthetic transaction (xid 0, `Op::Snapshot`, the first sequence)
flushed as its own snapshot set (Q-M6a-6). The cursor document rides that flush (D-M6a.7). The
completion is final once durable. A `dump_*` table stops there, and the worker ends `Dumped`
once every table is done. A table's reads wait for the writers in flight at the session's start
(Q-M6a-2). `timeouts.snapshot` halts the table `SNAPSHOT_TIMEOUT` (Q-M6a-3).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-pipeline/Cargo.toml b/crates/data-avalanche-pipeline/Cargo.toml
--- a/crates/data-avalanche-pipeline/Cargo.toml
+++ b/crates/data-avalanche-pipeline/Cargo.toml
@@ -13,14 +13,17 @@ workspace = true
 arrow-array.workspace = true
 arrow-buffer.workspace = true
 arrow-schema.workspace = true
+chrono.workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
 data-avalanche-state.workspace = true
 data-avalanche-storage.workspace = true
 futures-util.workspace = true
 getrandom.workspace = true
+serde_json.workspace = true
 thiserror.workspace = true
 tokio = { workspace = true, features = ["sync", "rt", "time", "macros"] }
+uuid.workspace = true
 
 [dev-dependencies]
 async-trait.workspace = true
@@ -28,6 +31,4 @@ bytes.workspace = true
 object_store.workspace = true
 data-avalanche-testkit.workspace = true
 tokio.workspace = true
-serde_json.workspace = true
-uuid.workspace = true
 proptest.workspace = true
```

Create `crates/data-avalanche-pipeline/src/cursor.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnSchema, TableRef};

    use super::*;

    fn schema() -> TableSchema {
        let col = |n: &str, t: T, src: &str| ColumnSchema::new(n, t, src).not_null();
        TableSchema::new(
            TableRef::new("public", "k"),
            vec![
                col("b", T::Bool, "bool"),
                col("i", T::Int16, "int2"),
                col(
                    "n",
                    T::Numeric {
                        precision: 10,
                        scale: 2,
                    },
                    "numeric",
                ),
                col("ts", T::Timestamp { with_tz: true }, "timestamptz"),
                col("d", T::Date, "date"),
                col("s", T::Text, "text"),
                col("u", T::Uuid, "uuid"),
                col("x", T::Bytes, "bytea"),
                col("big", T::UInt64, "bigint unsigned"),
            ],
            ["b", "i", "n", "ts", "d", "s", "u", "x", "big"]
                .map(String::from)
                .to_vec(),
        )
        .unwrap()
    }

    fn cursor() -> KeysetCursor {
        KeysetCursor {
            key: Row(vec![
                ("b".into(), V::Bool(true)),
                ("i".into(), V::Int16(-7)),
                ("n".into(), V::Numeric(Decimal::new(12_345, 2))),
                (
                    "ts".into(),
                    V::Timestamp {
                        micros: 1_700_000_000_000_001,
                        with_tz: true,
                    },
                ),
                ("d".into(), V::Date(-3)),
                ("s".into(), V::Text("é\"x".into())),
                ("u".into(), V::Uuid(Uuid::from_u128(0x0123_4567_89ab_cdef))),
                ("x".into(), V::Bytes(vec![0, 0xab, 0xff])),
                ("big".into(), V::UInt64(u64::MAX)),
            ]),
            collation: Some("C".into()),
        }
    }

    #[test]
    fn a_cursor_round_trips_through_its_document() {
        let doc = to_doc(&schema(), &cursor(), 500, 1500).unwrap();
        assert_eq!((doc.chunk_size, doc.rows_done), (500, 1500));
        assert_eq!(doc.key[2].val, Value::String("123.45".into()));
        assert_eq!(doc.key[7].val, Value::String("00abff".into()));
        assert_eq!(doc.key[3].ty, "timestamptz");
        let json = serde_json::to_value(&doc).unwrap();
        let back: SnapshotCursor = serde_json::from_value(json).unwrap();
        assert_eq!(from_doc(&schema(), &back).unwrap(), cursor());
    }

    #[test]
    fn a_cursor_that_doesnt_fit_the_identifier_is_refused() {
        let mut doc = to_doc(&schema(), &cursor(), 500, 0).unwrap();
        doc.key.pop();
        assert!(
            from_doc(&schema(), &doc)
                .unwrap_err()
                .contains("identifier")
        );
        let mut doc = to_doc(&schema(), &cursor(), 500, 0).unwrap();
        doc.key[1].val = Value::from(70_000);
        assert!(from_doc(&schema(), &doc).is_err(), "out of int2's range");
        doc.key[1].val = Value::from(1);
        doc.key[7].val = Value::String("abc".into());
        assert!(from_doc(&schema(), &doc).unwrap_err().contains("hex"));
    }
}
```

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -9,6 +9,7 @@ pub mod accumulator;
 pub mod backoff;
 pub mod batch;
 pub mod buffer;
+mod cursor;
 #[allow(
     dead_code,
     reason = "the worker reads it (Task 11); the allow goes there"
@@ -17,10 +18,6 @@ mod decide;
 pub mod error;
 pub mod hooks;
 pub mod policy;
-#[allow(
-    dead_code,
-    reason = "the snapshot runner uses it (Task 10); the allow goes there"
-)]
 mod reconcile;
 #[allow(
     dead_code,
```

Create `crates/data-avalanche-pipeline/src/session/snapshot.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use data_avalanche_config::Pipeline;
    use data_avalanche_core::{ColumnSchema, ColumnValue, ColumnValueType as V, Row};
    use data_avalanche_state::{CursorKey, SnapshotCursor};
    use data_avalanche_testkit::FakeTxn;
    use tokio::sync::watch;

    use super::*;
    use crate::session::Core;
    use crate::test_support::{World, Yaml, row, t, wait_until};
    use crate::{Command, Exit, WorkerStatus};

    fn chunks_of(n: u64) -> Pipeline {
        let tables = format!("\n      snapshots: {{ chunk_size: {n} }}");
        Yaml {
            tables: &tables,
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build()
    }

    fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
        ids.map(|id| row(id, "x")).collect()
    }

    /// Runs a session over `core` until `drive` returns, then drains it.
    async fn run<F: Future<Output = ()>>(
        mut core: Core,
        control: tokio::sync::mpsc::Sender<Command>,
        status: watch::Receiver<WorkerStatus>,
        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
    ) -> (End, WorkerStatus) {
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let watching = status.clone();
        let driver = async move {
            drive(watching).await;
            let _ = control.send(Command::Drain).await;
        };
        let (end, ()) = tokio::join!(s.run(), driver);
        let last = status.borrow().clone();
        (end.unwrap(), last)
    }

    async fn snapshot_done(w: &World, status: &watch::Receiver<WorkerStatus>, table: &str) {
        let _ = w;
        wait_until("the snapshot's completion is durable", || {
            let st = status.borrow();
            !st.snapshots.contains_key(&t(table)) && st.checkpoint.is_some()
        })
        .await;
    }

    async fn doc(w: &World, table: &str) -> TableSnapshotDoc {
        w.docs.table_snapshot(&t(table)).await.unwrap().unwrap().doc
    }

    #[tokio::test]
    async fn a_snapshot_reads_the_table_in_chunks_while_streaming() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=5));
        w.request(&["a"]).await;
        let (core, control, status) = w.core(&chunks_of(2), &["a"]);
        let (end, _) = run(core, control, status, async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(9, "s")));
            wait_until("every row is in", || w.ids("a") == [1, 2, 3, 4, 5, 9]).await;
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert!(matches!(end, End::Exit(Exit::Drained)));
        let d = doc(&w, "a").await;
        assert_eq!(
            (d.status, d.row_count, d.cursor),
            (SnapshotStatus::Completed, Some(5), None)
        );
        assert!(d.completed_at.is_some());
        assert_eq!(
            w.source.reads(&t("a")),
            4,
            "two, two, one, then the empty chunk"
        );
    }

    #[tokio::test]
    async fn a_stream_change_the_chunk_didnt_see_wins() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            wait_until("the chunk is being read", || w.source.reads(&t("a")) == 1).await;
            w.source
                .commit(FakeTxn::new().update(&t("a"), row(2, "new")));
            let end = w.source.wal_end();
            wait_until("the update is read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            w.source.release_reads();
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        let docs = w.text("a", "doc");
        assert_eq!(
            docs,
            [Some("x"), Some("new"), Some("x")].map(|d| d.map(String::from))
        );
    }

    /// DA-SNAP-9: a synchronous commit is streamed, and applied, before a chunk read issued after
    /// it can see it.
    #[tokio::test]
    async fn a_stalled_commit_applied_before_the_read_keeps_its_change() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let (core, control, status) = w.core(&chunks_of(1), &["a"]);
        run(core, control, status, async |status| {
            wait_until("the first chunk is being read", || {
                w.source.reads(&t("a")) == 1
            })
            .await;
            let xid = w
                .source
                .commit_stalled(FakeTxn::new().update(&t("a"), row(2, "new")));
            let end = w.source.wal_end();
            wait_until("the update is read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            w.source.release_reads();
            wait_until("the snapshot passed key 2", || w.source.reads(&t("a")) >= 3).await;
            w.source.release(xid);
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        let docs = w.text("a", "doc");
        assert_eq!(
            docs,
            [Some("x"), Some("new"), Some("x")].map(|d| d.map(String::from))
        );
    }

    /// Q-M6a-2: a commit the last session applied, still invisible when this one starts.
    #[tokio::test]
    async fn a_table_waits_for_the_writers_in_flight_at_the_start() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        let xid = w
            .source
            .commit_stalled(FakeTxn::new().update(&t("a"), row(1, "new")));
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            let end = w.source.wal_end();
            wait_until("the stalled commit is durable", || {
                status.borrow().checkpoint.as_ref() == Some(&end)
            })
            .await;
        })
        .await;
        assert_eq!(w.text("a", "doc"), [Some("new".to_owned())]);
        w.request(&["a"]).await;
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert_eq!(w.source.reads(&t("a")), 0, "the writer is still in flight");
            w.source.release(xid);
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(
            w.text("a", "doc"),
            [Some("new"), Some("x")].map(|d| d.map(String::from))
        );
    }

    #[tokio::test]
    async fn a_writer_the_session_applies_frees_the_table() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        let xid = w.source.open_writer(&[t("a")]);
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert_eq!(w.source.reads(&t("a")), 0);
            w.source
                .commit_writer(xid, FakeTxn::new().update(&t("a"), row(2, "new")));
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(
            w.text("a", "doc"),
            [Some("x"), Some("new")].map(|d| d.map(String::from))
        );
    }

    #[tokio::test]
    async fn a_dump_ends_the_worker_once_its_snapshot_is_durable() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=3));
        w.request(&["a"]).await;
        let p = Yaml {
            tables: "\n      snapshots: { mode: dump_always, chunk_size: 2 }",
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build();
        let (mut core, _control, _status) = w.core(&p, &["a"]);
        core.dump_only = true;
        let s = Session::open(&mut core).await.unwrap().unwrap();
        let end = s.run().await.unwrap();
        assert!(matches!(end, End::Exit(Exit::Dumped)));
        assert_eq!(w.ids("a"), [1, 2, 3]);
        assert_eq!(doc(&w, "a").await.status, SnapshotStatus::Completed);
    }

    #[tokio::test]
    async fn a_snapshot_resumes_after_its_cursor() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=6));
        let mut d = TableSnapshotDoc {
            table: t("a"),
            status: SnapshotStatus::InProgress,
            start_position: None,
            cursor: Some(SnapshotCursor {
                key: vec![CursorKey {
                    col: "id".into(),
                    val: 2.into(),
                    ty: "bigint".into(),
                }],
                chunk_size: 2,
                rows_done: 2,
                collation: None,
            }),
            row_count: None,
            completed_at: None,
        };
        w.docs.save_table_snapshot(&d).await.unwrap();
        let (core, control, status) = w.core(&chunks_of(2), &["a"]);
        run(core, control, status, async |status| {
            snapshot_done(&w, &status, "a").await;
        })
        .await;
        assert_eq!(
            w.ids("a"),
            [3, 4, 5, 6],
            "rows 1 and 2 were read by an earlier run"
        );
        assert_eq!(w.source.reads(&t("a")), 3);
        d = doc(&w, "a").await;
        assert_eq!(d.row_count, Some(6));
    }

    #[tokio::test]
    async fn a_snapshot_past_its_timeout_halts_the_table() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let (mut core, control, status) = w.core(&chunks_of(10), &["a"]);
        core.settings.snapshot_timeout = Duration::from_millis(20);
        let (_, st) = run(core, control, status, async |status| {
            wait_until("the table halts", || !status.borrow().halted.is_empty()).await;
        })
        .await;
        w.source.release_reads();
        assert_eq!(
            st.halted.get(&t("a")),
            Some(&HaltReason::SnapshotTimeout { limit_secs: 0 })
        );
        assert_eq!(doc(&w, "a").await.status, SnapshotStatus::Broken);
        assert_eq!(w.docs.halts().await.unwrap().tables[0].table, t("a"));
        assert!(w.ids("a").is_empty());
    }

    #[tokio::test]
    async fn a_chunk_read_under_the_old_schema_is_read_again() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.request(&["a"]).await;
        w.source.hold_reads();
        let noted = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        let reads = AtomicUsize::new(0);
        let (core, control, status) = w.core(&chunks_of(10), &["a"]);
        run(core, control, status, async |status| {
            wait_until("the chunk is being read", || w.source.reads(&t("a")) == 1).await;
            w.source.alter(&t("a"), noted);
            let mut r = row(9, "s");
            r.0.push(("note".into(), ColumnValue::Text("n".into())));
            w.source.commit(FakeTxn::new().insert(&t("a"), r));
            let end = w.source.wal_end();
            wait_until("the change is read", || {
                status.borrow().read.as_ref() == Some(&end)
            })
            .await;
            w.source.release_reads();
            snapshot_done(&w, &status, "a").await;
            reads.store(w.source.reads(&t("a")), Ordering::SeqCst);
        })
        .await;
        assert_eq!(w.ids("a"), [1, 2, 9]);
        assert_eq!(
            w.text("a", "note"),
            [None, None, Some("n".to_owned())],
            "the rows read again under the new schema"
        );
        assert!(
            reads.load(Ordering::SeqCst) >= 3,
            "the stale chunk, again, then the empty one"
        );
    }
}
```

Change `crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -14,7 +14,7 @@ use data_avalanche_core::{
     CdcEvent, ColumnSchema, ColumnValue, ColumnValueType as V, Op, Row, Sink, SourcePosition,
     SourceSeq, TableRef, TableSchema, TableSpec, TransactionBoundary,
 };
-use data_avalanche_state::{DocStore, PipelineDocs, SlotDoc};
+use data_avalanche_state::{DocStore, PipelineDocs, SlotDoc, SnapshotStatus, TableSnapshotDoc};
 use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
 use data_avalanche_testkit::{FakeSource, MemorySink};
 use object_store::memory::InMemory;
@@ -335,6 +335,23 @@ impl World {
             .expect("the checkpoint");
     }
 
+    /// Requests a snapshot of `tables`, as Phase 0 does: `in_progress` with no cursor.
+    pub async fn request(&self, tables: &[&str]) {
+        for t in tables {
+            self.docs
+                .save_table_snapshot(&TableSnapshotDoc {
+                    table: TableRef::new("public", *t),
+                    status: SnapshotStatus::InProgress,
+                    start_position: Some(self.source.wal_end()),
+                    cursor: None,
+                    row_count: None,
+                    completed_at: None,
+                })
+                .await
+                .expect("requested");
+        }
+    }
+
     /// A worker core over `tables` with `p`'s settings.
     pub fn core(
         &self,
@@ -355,6 +372,8 @@ impl World {
             control: control_rx,
             status,
             tracked: tables.iter().map(|t| TableRef::new("public", *t)).collect(),
+            dump_only: false,
+            snapshot_started: BTreeMap::new(),
         };
         (core, control, status_rx)
     }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

FAIL to compile (16 errors); the first:

```text
error[E0425]: cannot find type `TableSchema` in this scope
error[E0425]: cannot find type `T` in this scope
error[E0425]: cannot find type `KeysetCursor` in this scope
error[E0422]: cannot find struct, variant or union type `KeysetCursor` in this scope
error[E0425]: cannot find type `SnapshotCursor` in this scope
error[E0433]: cannot find type `TableSchema` in this scope
… and 10 more
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-pipeline/src/buffer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/buffer.rs b/crates/data-avalanche-pipeline/src/buffer.rs
--- a/crates/data-avalanche-pipeline/src/buffer.rs
+++ b/crates/data-avalanche-pipeline/src/buffer.rs
@@ -70,10 +70,6 @@ fn key_part(v: &ColumnValue) -> Option<KeyPart> {
 
 /// The key of `row` in `schema`, compared as the buffer compares keys; `None` when an identifier
 /// value is missing, NULL or doesn't fit its column.
-#[allow(
-    dead_code,
-    reason = "the snapshot runner keys chunk rows with it (Task 10); the allow goes there"
-)]
 pub(crate) fn identity(schema: &TableSchema, row: &Row) -> Option<Key> {
     schema
         .identifier()
```

Add the implementation to `crates/data-avalanche-pipeline/src/cursor.rs`, above the test module:

```rust
//! A snapshot cursor between the source's typed key ([`KeysetCursor`]) and the state document's
//! JSON ([`SnapshotCursor`], Snapshot_Resumability §2). Each key value is stored by its column's
//! type: integers, dates, times and timestamps (microseconds) as JSON numbers, decimals and UUIDs
//! as their text, bytes as hex.

use data_avalanche_core::{
    ColumnValue as V, ColumnValueType as T, Decimal, KeysetCursor, Row, TableSchema,
};
use data_avalanche_state::{CursorKey, SnapshotCursor};
use serde_json::Value;
use uuid::Uuid;

/// The document form of `cursor`, a cursor of `schema`'s identifier.
pub(crate) fn to_doc(
    schema: &TableSchema,
    cursor: &KeysetCursor,
    chunk_size: u64,
    rows_done: u64,
) -> Result<SnapshotCursor, String> {
    let key = cursor
        .key
        .0
        .iter()
        .map(|(col, v)| {
            let c = schema
                .column(col)
                .ok_or_else(|| format!("{col:?} isn't a column"))?;
            Ok(CursorKey {
                col: col.clone(),
                val: json(v)?,
                ty: c.source_type.clone(),
            })
        })
        .collect::<Result<_, String>>()?;
    Ok(SnapshotCursor {
        key,
        chunk_size,
        rows_done,
        collation: cursor.collation.clone(),
    })
}

/// The cursor a document holds, when it still fits `schema`'s identifier.
pub(crate) fn from_doc(schema: &TableSchema, doc: &SnapshotCursor) -> Result<KeysetCursor, String> {
    let cols: Vec<&str> = doc.key.iter().map(|k| k.col.as_str()).collect();
    if cols != schema.identifier() {
        return Err(format!(
            "the cursor is over {cols:?}, the identifier is {:?}",
            schema.identifier()
        ));
    }
    let key = doc
        .key
        .iter()
        .map(|k| {
            let c = schema.column(&k.col).expect("an identifier column");
            Ok((k.col.clone(), value(&c.value_type, &k.val)?))
        })
        .collect::<Result<_, String>>()?;
    Ok(KeysetCursor {
        key: Row(key),
        collation: doc.collation.clone(),
    })
}

fn json(v: &V) -> Result<Value, String> {
    Ok(match v {
        V::Bool(b) => Value::Bool(*b),
        V::Int16(i) => Value::from(*i),
        V::Int32(i) => Value::from(*i),
        V::Int64(i) | V::Time(i) => Value::from(*i),
        V::UInt64(u) => Value::from(*u),
        V::Timestamp { micros, .. } => Value::from(*micros),
        V::Date(d) => Value::from(*d),
        V::Numeric(d) => Value::String(d.to_string()),
        V::Text(s) => Value::String(s.clone()),
        V::Uuid(u) => Value::String(u.to_string()),
        V::Bytes(b) => Value::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        other => return Err(format!("{other:?} can't be a key value")),
    })
}

fn value(t: &T, v: &Value) -> Result<V, String> {
    let int = || v.as_i64().ok_or_else(|| format!("{v} isn't an integer"));
    let text = || v.as_str().ok_or_else(|| format!("{v} isn't a string"));
    Ok(match t {
        T::Bool => V::Bool(v.as_bool().ok_or_else(|| format!("{v} isn't a boolean"))?),
        T::Int16 => V::Int16(fit(int()?, t)?),
        T::Int32 => V::Int32(fit(int()?, t)?),
        T::Int64 => V::Int64(int()?),
        T::UInt64 => V::UInt64(v.as_u64().ok_or_else(|| format!("{v} isn't unsigned"))?),
        T::Time => V::Time(int()?),
        T::Date => V::Date(fit(int()?, t)?),
        T::Timestamp { with_tz } => V::Timestamp {
            micros: int()?,
            with_tz: *with_tz,
        },
        T::Numeric { .. } => V::Numeric(Decimal::parse(text()?).map_err(|e| e.to_string())?),
        T::Text => V::Text(text()?.to_owned()),
        T::Uuid => V::Uuid(Uuid::parse_str(text()?).map_err(|e| e.to_string())?),
        T::Bytes => V::Bytes(hex(text()?)?),
        other => return Err(format!("{other:?} can't be a key type")),
    })
}

fn fit<N: TryFrom<i64>>(i: i64, t: &T) -> Result<N, String> {
    N::try_from(i).map_err(|_| format!("{i} doesn't fit {t:?}"))
}

fn hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) || !s.is_ascii() {
        return Err(format!("{s:?} isn't hex"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| format!("{s:?} isn't hex")))
        .collect()
}
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -7,6 +7,8 @@
 //! halts. What it held in memory is lost when it ends, and the next session replays it from the
 //! checkpoint.
 
+mod snapshot;
+
 use std::collections::{BTreeMap, BTreeSet, VecDeque};
 use std::sync::Arc;
 use std::time::Instant;
@@ -17,10 +19,12 @@ use data_avalanche_core::{
     SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary, classify, identifier_values,
 };
 use data_avalanche_state::{PipelineDocs, TableHalt};
+use futures_util::StreamExt;
 use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
 use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
 use tokio::task::JoinHandle;
 
+use self::snapshot::Snapshots;
 use crate::settings::Settings;
 use crate::writer::{self, Job, WriterCtx, WriterEvent};
 use crate::{
@@ -40,8 +44,12 @@ pub(crate) struct Core {
     pub tuning: Tuning,
     pub control: mpsc::Receiver<Command>,
     pub status: watch::Sender<WorkerStatus>,
-    /// The tables Phase 0 tracks. A session streams each one unless it is halted.
+    /// The tables Phase 0 tracks. A session streams each one unless it is halted or a done dump.
     pub tracked: BTreeSet<TableRef>,
+    /// Every table of the pipeline is a `dump_*` table: the worker ends once none is left.
+    pub dump_only: bool,
+    /// When each table's snapshot began in this run (`timeouts.snapshot`).
+    pub snapshot_started: BTreeMap<TableRef, Instant>,
 }
 
 impl Core {
@@ -90,6 +98,7 @@ pub(crate) struct Session<'c> {
     last_seq: Option<SourceSeq>,
     /// A rescan's findings, applied once the stream passes the position read after it.
     rescan: Option<(SourcePosition, RescanReport)>,
+    snaps: Snapshots,
     draining: bool,
 }
 
@@ -139,6 +148,18 @@ impl<'c> Session<'c> {
                 Err(e) => return Err(e.into()),
             }
         }
+        let (mut snaps, retired, warnings) = Snapshots::load(core, &schemas).await?;
+        for table in &retired {
+            acc.halt_table(table);
+            schemas.remove(table);
+        }
+        if let Err(e) = snaps.read_in_flight(&*core.source).await {
+            return Ok(Err(e));
+        }
+        let now = Instant::now();
+        for table in snaps.tables() {
+            core.snapshot_started.entry(table.clone()).or_insert(now);
+        }
         let live: Vec<TableSchema> = schemas.values().cloned().collect();
         let stream = match core.source.open_stream(&checkpoint, &live).await {
             Ok(stream) => stream,
@@ -166,6 +187,8 @@ impl<'c> Session<'c> {
             for h in recorded.iter().chain(&halts) {
                 st.halted.insert(h.table.clone(), h.reason.clone());
             }
+            st.warnings.extend(warnings);
+            st.snapshots.extend(snaps.progress());
         });
         Ok(Ok(Self {
             core,
@@ -185,6 +208,7 @@ impl<'c> Session<'c> {
             requery: BTreeMap::new(),
             last_seq: None,
             rescan: None,
+            snaps,
             draining: false,
         }))
     }
@@ -232,7 +256,15 @@ impl<'c> Session<'c> {
             .settings
             .rescan_interval
             .map(|every| tokio::time::interval_at(tokio::time::Instant::now() + every, every));
+        self.issue_reads();
         loop {
+            if self.dumped() {
+                self.flush()?;
+                if let Some(end) = self.until_durable().await? {
+                    return Ok(end);
+                }
+                return Ok(End::Exit(Exit::Dumped));
+            }
             if self.draining && !self.acc.in_transaction() {
                 self.flush()?;
                 if let Some(end) = self.until_durable().await? {
@@ -262,7 +294,18 @@ impl<'c> Session<'c> {
                     }
                     Err(e) => return Ok(End::Source(e)),
                 },
-                _ = tick.tick() => self.due(Instant::now())?,
+                read = self.snaps.reads.next(), if !self.snaps.reads.is_empty() => {
+                    let read = read.expect("not empty");
+                    if let Some(end) = self.chunk_read(read).await? {
+                        return Ok(end);
+                    }
+                }
+                _ = tick.tick() => {
+                    self.due(Instant::now())?;
+                    if let Some(end) = self.snapshot_tick().await? {
+                        return Ok(end);
+                    }
+                }
                 _ = async { rescan.as_mut().expect("guarded").tick().await },
                     if rescan.is_some() && self.rescan.is_none() =>
                 {
@@ -303,6 +346,16 @@ impl<'c> Session<'c> {
             }
             Some(WriterEvent::Durable(cp)) => {
                 self.durable = cp.clone();
+                for (table, dump) in self.snaps.durable(&cp) {
+                    self.core.update(|st| {
+                        st.snapshots.remove(&table);
+                    });
+                    if dump {
+                        // A dump stops once its snapshot is durable.
+                        self.acc.halt_table(&table);
+                        self.schemas.remove(&table);
+                    }
+                }
                 if let Err(e) = self.stream.acknowledge(&cp).await {
                     return Ok(Some(End::Source(e)));
                 }
@@ -317,6 +370,7 @@ impl<'c> Session<'c> {
         match item {
             StreamItem::Begin { xid, .. } => {
                 self.acc.begin(xid)?;
+                self.snaps.begin();
                 self.requery.clear();
                 self.last_seq = None;
             }
@@ -325,9 +379,13 @@ impl<'c> Session<'c> {
                     self.note_unchanged(&e);
                 }
                 self.last_seq = Some(e.change_seq);
+                if let Some(schema) = self.schemas.get(&e.table) {
+                    self.snaps.touch(schema, &e);
+                }
                 self.apply(&e, now)?;
             }
             StreamItem::Truncate { tables, .. } => loop {
+                self.snaps.truncated(&tables);
                 match self.acc.truncate(&tables, now) {
                     Err(AccumulatorError::FlushRequired { .. }) => self.flush()?,
                     Err(AccumulatorError::Halt { table, reason }) => self.halt(table, reason),
@@ -343,6 +401,7 @@ impl<'c> Session<'c> {
                     return Ok(Some(end));
                 }
                 self.acc.commit(&b, now)?;
+                self.snaps.applied(b.xid);
                 let at = b.commit_pos;
                 self.pending_schemas
                     .extend(self.txn_schemas.drain(..).map(|s| (at.clone(), s)));
@@ -362,6 +421,9 @@ impl<'c> Session<'c> {
         if let Some(end) = self.caught_up().await? {
             return Ok(Some(end));
         }
+        if let Some(end) = self.emit_ready().await? {
+            return Ok(Some(end));
+        }
         self.due(now)?;
         Ok(None)
     }
@@ -499,6 +561,11 @@ impl<'c> Session<'c> {
         Ok(None)
     }
 
+    /// Every table is a dump, and every one is done or halted.
+    fn dumped(&self) -> bool {
+        self.core.dump_only && self.schemas.is_empty() && self.snaps.is_empty()
+    }
+
     fn read(&self) {
         let frontier = self.acc.frontier().clone();
         self.core.update(|st| st.read = Some(frontier));
@@ -521,6 +588,7 @@ impl<'c> Session<'c> {
     fn halt(&mut self, table: TableRef, reason: HaltReason) {
         self.acc.halt_table(&table);
         self.schemas.remove(&table);
+        self.snaps.stop(&table);
         self.halts.push(TableHalt {
             table: table.clone(),
             reason: reason.clone(),
@@ -570,6 +638,7 @@ impl<'c> Session<'c> {
             && checkpoint == self.submitted
             && self.halts.is_empty()
             && schemas.is_empty()
+            && !self.snaps.has_docs()
         {
             return Ok(());
         }
@@ -587,7 +656,7 @@ impl<'c> Session<'c> {
             change_sets,
             checkpoint: checkpoint.clone(),
             halts: std::mem::take(&mut self.halts),
-            snapshots: vec![],
+            snapshots: self.snaps.take_docs(&checkpoint),
             schemas,
             permit: None,
         };
```

Add the implementation to `crates/data-avalanche-pipeline/src/session/snapshot.rs`, above the test module:

```rust
//! The snapshot runner inside a stream session (Data-Flow Phase 1). Snapshots run while the
//! stream is read: the tables whose snapshot document says `in_progress` are read chunk by chunk
//! in key order, up to `parallelism` tables at a time.
//!
//! A chunk is emitted between two stream transactions, as a transaction of its own at the
//! frontier: its rows are `Op::Snapshot` events with the lowest `_seq`, so a stream change of the
//! same key buffered with them wins. Before that, the reconciler drops the rows the stream holds
//! newer changes for (DA-SNAP-9). The cursor is persisted once the flush that carries the chunk is
//! durable. A table's completion is persisted the same way, and only then does a `dump_*` table
//! stop streaming.
//!
//! A transaction that was in flight when the session started may have committed before its
//! checkpoint and be in the sink already, while a chunk can't see it yet. A table's chunks wait
//! until no such transaction writes the table (Q-M6a-2).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, Chunk, HaltReason, KeysetCursor, Op, SourceConnector, SourceError, SourcePosition,
    SourceSeq, TableRef, TableSchema, TransactionBoundary,
};
use data_avalanche_state::{SnapshotStatus, TableSnapshotDoc};
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;

use super::{End, Session, protocol};
use crate::buffer::identity;
use crate::cursor;
use crate::reconcile::{Reconciler, Ticket, Touched};
use crate::{HookAction, HookPoint, SnapshotProgress, TxnLimits, WorkerError};

/// One table's snapshot in this session.
#[derive(Debug)]
struct Snap {
    dump: bool,
    doc: TableSnapshotDoc,
    /// Where the next chunk starts; `None` at the table's start.
    cursor: Option<KeysetCursor>,
    reading: bool,
    /// The last chunk has been emitted.
    done: bool,
    /// After a failed read: not before this, and how many failed in a row.
    retry: Option<(Instant, u32)>,
}

/// A finished chunk read.
pub(super) struct Read {
    table: TableRef,
    ticket: Ticket,
    /// The schema the chunk was read under.
    schema: TableSchema,
    result: Result<Chunk, SourceError>,
    read_at_micros: i64,
}

#[derive(Default)]
pub(super) struct Snapshots {
    tables: BTreeMap<TableRef, Snap>,
    pub(super) reads: FuturesUnordered<BoxFuture<'static, Read>>,
    ready: VecDeque<Read>,
    reconciler: Reconciler,
    /// What the open stream transaction wrote in the tables being snapshotted.
    touched: Touched,
    /// Transactions in flight when the session started: xid → the source ids of their tables.
    in_flight: BTreeMap<u64, BTreeSet<u64>>,
    /// When to read the in-flight transactions again, and how many reads found some.
    recheck: Option<(Instant, u32)>,
    /// Snapshot documents for the next flush, the latest per table.
    docs: BTreeMap<TableRef, TableSnapshotDoc>,
    /// Completions handed to the writer, with the checkpoint of their flush.
    completing: Vec<(SourcePosition, TableRef)>,
}

impl std::fmt::Debug for Snapshots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshots")
            .field("tables", &self.tables)
            .field("reads", &self.reads.len())
            .field("ready", &self.ready.len())
            .finish_non_exhaustive()
    }
}

impl Snapshots {
    /// The snapshot state of the streamed tables. Returns it with the `dump_*` tables whose
    /// snapshot is done (they aren't streamed) and warnings.
    pub(super) async fn load(
        core: &super::Core,
        schemas: &BTreeMap<TableRef, TableSchema>,
    ) -> Result<(Self, BTreeSet<TableRef>, Vec<String>), WorkerError> {
        let mut snaps = Self::default();
        let mut retired = BTreeSet::new();
        let mut warnings = vec![];
        for (table, schema) in schemas {
            let Some(v) = core.docs.table_snapshot(table).await? else {
                continue;
            };
            let dump = core.settings.is_dump(table);
            match v.doc.status {
                SnapshotStatus::Completed if dump => {
                    retired.insert(table.clone());
                }
                SnapshotStatus::InProgress => {
                    let cursor = match &v.doc.cursor {
                        None => None,
                        Some(c) => match cursor::from_doc(schema, c) {
                            Ok(c) => Some(c),
                            Err(why) => {
                                warnings.push(format!(
                                    "table {table}: the snapshot starts over, its cursor doesn't fit: {why}"
                                ));
                                None
                            }
                        },
                    };
                    snaps.tables.insert(
                        table.clone(),
                        Snap {
                            dump,
                            doc: v.doc,
                            cursor,
                            reading: false,
                            done: false,
                            retry: None,
                        },
                    );
                }
                _ => {}
            }
        }
        Ok((snaps, retired, warnings))
    }

    /// Reads the transactions in flight now (session start).
    pub(super) async fn read_in_flight(
        &mut self,
        source: &dyn SourceConnector,
    ) -> Result<(), SourceError> {
        if self.tables.is_empty() {
            return Ok(());
        }
        if let Some(txns) = source.in_flight().await? {
            self.in_flight = txns
                .into_iter()
                .map(|t| (t.xid, t.tables.into_iter().collect()))
                .collect();
        }
        Ok(())
    }

    pub(super) fn is_snapshotting(&self, table: &TableRef) -> bool {
        self.tables.contains_key(table)
    }

    pub(super) fn tables(&self) -> impl Iterator<Item = &TableRef> {
        self.tables.keys()
    }

    /// Each running snapshot's progress so far.
    pub(super) fn progress(&self) -> impl Iterator<Item = (TableRef, SnapshotProgress)> + '_ {
        self.tables.iter().map(|(t, s)| {
            let rows_done = s.doc.cursor.as_ref().map_or(0, |c| c.rows_done);
            (t.clone(), SnapshotProgress { rows_done })
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// A stream transaction begins.
    pub(super) fn begin(&mut self) {
        self.touched = Touched::default();
    }

    /// A stream change of the open transaction.
    pub(super) fn touch(&mut self, schema: &TableSchema, e: &CdcEvent) {
        if !self.tables.get(&e.table).is_some_and(|s| !s.done) {
            return;
        }
        for row in e.before.iter().chain(&e.after) {
            if let Some(key) = identity(schema, row) {
                self.touched.key(&e.table, key);
            }
        }
    }

    pub(super) fn truncated(&mut self, tables: &[TableRef]) {
        for t in tables {
            if self.tables.contains_key(t) {
                self.touched.truncated.insert(t.clone());
            }
        }
    }

    /// The stream transaction `xid` was applied.
    pub(super) fn applied(&mut self, xid: u64) {
        self.in_flight.remove(&xid);
        if !self.tables.is_empty() {
            self.reconciler
                .applied(xid, std::mem::take(&mut self.touched));
        }
    }

    /// The table halted: its snapshot stops.
    pub(super) fn stop(&mut self, table: &TableRef) {
        self.tables.remove(table);
        self.docs.remove(table);
        if self.tables.is_empty() {
            self.reconciler.clear();
        }
    }

    /// The documents for the next flush, whose checkpoint is `checkpoint`.
    pub(super) fn take_docs(&mut self, checkpoint: &SourcePosition) -> Vec<TableSnapshotDoc> {
        let docs: Vec<TableSnapshotDoc> = std::mem::take(&mut self.docs).into_values().collect();
        for d in &docs {
            if d.status == SnapshotStatus::Completed {
                self.completing.push((checkpoint.clone(), d.table.clone()));
            }
        }
        docs
    }

    pub(super) fn has_docs(&self) -> bool {
        !self.docs.is_empty()
    }

    /// The tables whose completion is durable at `checkpoint`, each with whether it is a dump.
    pub(super) fn durable(&mut self, checkpoint: &SourcePosition) -> Vec<(TableRef, bool)> {
        let mut done = vec![];
        self.completing.retain(|(at, table)| {
            if checkpoint.covers(at).unwrap_or(false) {
                done.push(table.clone());
                false
            } else {
                true
            }
        });
        let out = done
            .into_iter()
            .filter_map(|t| self.tables.remove(&t).map(|s| (t, s.dump)))
            .collect();
        if self.tables.is_empty() {
            self.reconciler.clear();
        }
        out
    }
}

fn micros_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

/// The lowest change sequence of the source's dialect: any stream change of a key sorts after it.
fn first_seq(at: &SourcePosition) -> SourceSeq {
    match at {
        SourcePosition::Pg(_) => SourceSeq::Pg(Lsn(0)),
        _ => SourceSeq::Binlog {
            file_index: 0,
            end_log_pos: 0,
        },
    }
}

impl Session<'_> {
    /// Starts the chunk reads that may start: one per table, `parallelism` at a time, none while
    /// the buffer is full, none for a table an in-flight transaction writes.
    pub(super) fn issue_reads(&mut self) {
        if !self.queued.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut reading = self.snaps.tables.values().filter(|s| s.reading).count();
        let blocked: BTreeSet<u64> = self.snaps.in_flight.values().flatten().copied().collect();
        let mut wait_for_writers = false;
        for (table, snap) in &mut self.snaps.tables {
            if reading >= self.core.settings.parallelism {
                break;
            }
            if snap.reading || snap.done || snap.retry.is_some_and(|(at, _)| at > now) {
                continue;
            }
            let Some(schema) = self.schemas.get(table).cloned() else {
                continue;
            };
            if schema.source_id().is_some_and(|id| blocked.contains(&id)) {
                wait_for_writers = true;
                continue;
            }
            let ticket = self.snaps.reconciler.issue();
            let source = Arc::clone(&self.core.source);
            let after = snap.cursor.clone();
            let limit = self.core.settings.chunk_size;
            let table = table.clone();
            self.snaps.reads.push(Box::pin(async move {
                let read_at_micros = micros_now();
                let result = source.read_chunk(&schema, after.as_ref(), limit).await;
                Read {
                    table,
                    ticket,
                    schema,
                    result,
                    read_at_micros,
                }
            }));
            snap.reading = true;
            reading += 1;
        }
        if wait_for_writers && self.snaps.recheck.is_none() {
            let delay = self.core.tuning.in_flight_backoff.delay(0);
            self.snaps.recheck = Some((now + delay, 1));
        }
    }

    /// A chunk read finished: it is emitted at the next boundary.
    pub(super) async fn chunk_read(&mut self, read: Read) -> Result<Option<End>, WorkerError> {
        self.snaps.ready.push_back(read);
        if self.acc.in_transaction() {
            return Ok(None);
        }
        self.emit_ready().await
    }

    /// Emits the chunks read so far; called between transactions.
    pub(super) async fn emit_ready(&mut self) -> Result<Option<End>, WorkerError> {
        while let Some(read) = self.snaps.ready.pop_front() {
            if let Some(end) = self.emit(read)? {
                return Ok(Some(end));
            }
        }
        self.issue_reads();
        Ok(None)
    }

    fn emit(&mut self, read: Read) -> Result<Option<End>, WorkerError> {
        let Read {
            table,
            ticket,
            schema,
            result,
            read_at_micros,
        } = read;
        let current = self.schemas.get(&table).cloned();
        let Some(snap) = self.snaps.tables.get_mut(&table) else {
            self.snaps.reconciler.cancel(ticket);
            return Ok(None);
        };
        snap.reading = false;
        let chunk = match result {
            Ok(chunk) if current.as_ref() == Some(&schema) => chunk,
            Ok(_) => {
                // The table's schema changed while the chunk was read: read it again.
                self.snaps.reconciler.cancel(ticket);
                return Ok(None);
            }
            Err(SourceError::TableHalt { reason, .. }) => {
                self.snaps.reconciler.cancel(ticket);
                self.halt(table, reason);
                return Ok(None);
            }
            Err(e) if e.is_transient() => {
                self.snaps.reconciler.cancel(ticket);
                let failed = snap.retry.map_or(0, |(_, n)| n);
                let delay = self.core.tuning.source_backoff.delay(failed);
                snap.retry = Some((Instant::now() + delay, failed + 1));
                return Ok(None);
            }
            Err(e) => {
                self.snaps.reconciler.cancel(ticket);
                return Ok(Some(End::Source(e)));
            }
        };
        snap.retry = None;
        let mut keyed = Vec::with_capacity(chunk.rows.len());
        for row in chunk.rows.iter().cloned() {
            let Some(key) = identity(&schema, &row) else {
                return Err(protocol(format!("a snapshot row of {table} has no key")));
            };
            keyed.push((key, row));
        }
        let read = chunk.rows.len() as u64;
        let kept =
            self.snaps
                .reconciler
                .reconcile(ticket, &table, chunk.visibility.as_ref(), keyed);
        if !kept.is_empty() {
            // The stream's changes go first: the chunk is committed as a set of its own, which
            // the position guard doesn't skip (Q-M6a-6).
            self.flush()?;
        }
        // The cursor, or the completion, rides the flush that carries the chunk.
        let Some(snap) = self.snaps.tables.get_mut(&table) else {
            return Ok(None);
        };
        let rows_done = snap.doc.cursor.as_ref().map_or(0, |c| c.rows_done) + read;
        snap.cursor = chunk.next.clone();
        match &chunk.next {
            Some(next) => {
                let size = self.core.settings.chunk_size as u64;
                let doc = cursor::to_doc(&schema, next, size, rows_done)
                    .map_err(|why| protocol(format!("table {table}: {why}")))?;
                snap.doc.cursor = Some(doc);
            }
            None => {
                snap.done = true;
                snap.doc.status = SnapshotStatus::Completed;
                snap.doc.cursor = None;
                snap.doc.row_count = Some(rows_done);
                snap.doc.completed_at = Some(DateTime::<Utc>::from(SystemTime::now()));
            }
        }
        self.snaps.docs.insert(table.clone(), snap.doc.clone());
        self.core.update(|st| {
            st.snapshots
                .insert(table.clone(), SnapshotProgress { rows_done });
        });
        if kept.is_empty() {
            self.flush()?;
        } else {
            let at = self.acc.frontier().clone();
            // Chunk rows are bounded by `chunk_size`, not by the transaction limits: a slice
            // would be guarded, and the limit isn't meant for them.
            self.acc.set_txn_limits(TxnLimits {
                batch_rows: None,
                buffer_bytes: u64::MAX,
            });
            self.acc.begin(0)?;
            let mut applied = Ok(());
            for row in kept {
                let e = CdcEvent {
                    op: Op::Snapshot,
                    table: table.clone(),
                    before: None,
                    after: Some(row),
                    source_pos: at.clone(),
                    change_seq: first_seq(&at),
                    xid: 0,
                    commit_ts_micros: read_at_micros,
                };
                applied = self.apply(&e, Instant::now());
                if applied.is_err() {
                    break;
                }
            }
            self.acc.set_txn_limits(self.core.settings.limits);
            applied?;
            let b = TransactionBoundary {
                xid: 0,
                commit_pos: at,
                commit_ts_micros: read_at_micros,
            };
            self.acc.commit(&b, Instant::now())?;
            let f = self.acc.flush();
            let sets = f
                .change_sets
                .into_iter()
                .map(|mut cs| {
                    cs.snapshot = true;
                    cs
                })
                .collect();
            self.submit(sets, f.checkpoint)?;
        }
        if self.core.hooks.at(&HookPoint::ChunkEmitted {
            table: table.clone(),
        }) == HookAction::Crash
        {
            return Err(WorkerError::Crashed(HookPoint::ChunkEmitted { table }));
        }
        Ok(None)
    }

    /// On the tick: the snapshot timeout, and the in-flight transactions read again when due.
    pub(super) async fn snapshot_tick(&mut self) -> Result<Option<End>, WorkerError> {
        let now = Instant::now();
        let limit = self.core.settings.snapshot_timeout;
        let late: Vec<TableRef> = self
            .snaps
            .tables
            .iter()
            .filter(|(t, s)| {
                !s.done
                    && self
                        .core
                        .snapshot_started
                        .get(*t)
                        .is_some_and(|at| now.duration_since(*at) >= limit)
            })
            .map(|(t, _)| t.clone())
            .collect();
        for table in late {
            let mut doc = self.snaps.tables[&table].doc.clone();
            doc.status = SnapshotStatus::Broken;
            self.halt(
                table.clone(),
                HaltReason::SnapshotTimeout {
                    limit_secs: limit.as_secs(),
                },
            );
            self.snaps.docs.insert(table, doc);
        }
        if let Some((at, n)) = self.snaps.recheck
            && at <= now
        {
            match self.core.source.in_flight().await {
                Ok(Some(txns)) => {
                    let listed: BTreeSet<u64> = txns.iter().map(|t| t.xid).collect();
                    self.snaps.in_flight.retain(|xid, _| listed.contains(xid));
                }
                Ok(None) => self.snaps.in_flight.clear(),
                Err(e) if e.is_transient() => {}
                Err(e) => return Ok(Some(End::Source(e))),
            }
            self.snaps.recheck = None;
            self.issue_reads();
            if self.snaps.recheck.is_some() {
                let delay = self.core.tuning.in_flight_backoff.delay(n);
                self.snaps.recheck = Some((now + delay, n + 1));
            }
        }
        if !self.acc.in_transaction() {
            return self.emit_ready().await;
        }
        Ok(None)
    }
}
```

Change `crates/data-avalanche-pipeline/src/settings.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/settings.rs b/crates/data-avalanche-pipeline/src/settings.rs
--- a/crates/data-avalanche-pipeline/src/settings.rs
+++ b/crates/data-avalanche-pipeline/src/settings.rs
@@ -142,6 +142,14 @@ impl Settings {
             .unwrap_or(self.mode)
     }
 
+    /// A `dump_*` table: it stops once its snapshot is done.
+    pub fn is_dump(&self, t: &TableRef) -> bool {
+        matches!(
+            self.mode(t),
+            SnapshotMode::DumpAlways | SnapshotMode::DumpInitial
+        )
+    }
+
     pub fn partition_by(&self, t: &TableRef) -> Vec<PartitionField> {
         self.partitioning
             .get(&self.key(t))
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

PASS:

```text
test result: ok. 102 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): snapshots run inside the stream session, chunk by chunk, reconciled and checkpointed"
```


### Task 11: The pipeline worker: Phase 0 and sessions

**Files:**
- Create: `crates/data-avalanche-pipeline/src/worker.rs`, `…/schema_change.rs`
- Modify: `crates/data-avalanche-pipeline/src/session.rs`, `…/session/snapshot.rs`, `…/status.rs`, `…/lib.rs`

**Interfaces:**
- Consumes: everything above; `SourceConnector::{preflight, resolve_tables, prepare,
  reset_position, retained_bytes}`; `Sink::{capabilities, table_state, ensure_table}`;
  `PipelineDocs::open`.
- Produces (pub, re-exported):
  - `WorkerDeps { source, sink, docs, owner_epoch, guard, hooks }`, `WorkerDeps::new(source,
    sink, docs)` (epoch 1, `Unguarded`, `NoHooks`).
  - `PipelineWorker::new(&Pipeline, WorkerDeps, Tuning) -> (PipelineWorker, WorkerHandle)`,
    `PipelineWorker::run() -> Result<Exit, WorkerError>`.
  - `WorkerHandle { control: mpsc::Sender<Command>, status: watch::Receiver<WorkerStatus> }`.
  - `schema_change::Evolver` (crate) with `plan(cached, hint) -> Result<Planned, HaltReason>`
    and `evolve(table, safe)`; the session's schema signal and Phase 0's baseline both use it.

Phase 0 runs once per run. It refuses Iceberg, a pipeline without a slot (MySQL arrives
with M7), and state of another sink type. It opens the state, runs the preflight and resolves
the tables. Each cached table gone from the source halts `TABLE_DROPPED`, and so does one with
a new source id. It saves the slot intent, calls `prepare` and decides per table. It ensures
the sink tables, baselines drift on a fresh position, and persists the halts, the snapshot
requests, the slot document and the schemas (D-M6a.8).

Sessions follow. A transient source or state error reconnects with backoff. `Contended` is
retried for `contended_window`. A mid-run `PositionUnreachable` resets the position and reruns
Phase 0 (D-M6a.9). The heartbeat reads the retained WAL (D-M6a.10).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -2,7 +2,7 @@
 //! change events by key as they arrive, and the accumulator that tracks transactions and decides
 //! when to flush.
 //!
-//! The accumulator performs no I/O and reads no clock. The pipeline worker drives it: it reads
+//! The accumulator performs no I/O and reads no clock. The [`PipelineWorker`] drives it: it reads
 //! the source, runs the snapshots, hands change sets to the sink and persists the checkpoint.
 
 pub mod accumulator;
@@ -10,32 +10,18 @@ pub mod backoff;
 pub mod batch;
 pub mod buffer;
 mod cursor;
-#[allow(
-    dead_code,
-    reason = "the worker reads it (Task 11); the allow goes there"
-)]
 mod decide;
 pub mod error;
 pub mod hooks;
 pub mod policy;
 mod reconcile;
-#[allow(
-    dead_code,
-    reason = "the worker runs it (Task 11); the allow goes there"
-)]
+mod schema_change;
 mod session;
-#[allow(
-    dead_code,
-    reason = "the worker reads it (Task 11); the allow goes there"
-)]
 mod settings;
 pub mod status;
 #[cfg(test)]
 mod test_support;
-#[allow(
-    dead_code,
-    reason = "the worker runs it (Task 11); the allow goes there"
-)]
+pub mod worker;
 mod writer;
 
 pub use accumulator::{Accumulator, Flush};
@@ -46,3 +32,4 @@ pub use error::{AccumulatorError, WorkerError};
 pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
 pub use policy::{FlushPolicy, FlushReason, TxnLimits};
 pub use status::{Command, Exit, Phase, SnapshotProgress, Tuning, WorkerStatus};
+pub use worker::{PipelineWorker, WorkerDeps, WorkerHandle};
```

Change `crates/data-avalanche-pipeline/src/session.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -841,7 +841,7 @@ mod tests {
     use std::time::Duration;
 
     use data_avalanche_core::position::Lsn;
-    use data_avalanche_core::{ColumnSchema, ColumnValue, ColumnValueType as V, Row};
+    use data_avalanche_core::{ColumnSchema, ColumnValue, ColumnValueType as V, Row, SinkError};
     use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
 
     use super::*;
@@ -1190,6 +1190,32 @@ mod tests {
         assert_eq!(w.ids("a"), [1, 2, 3], "nothing before the drop is lost");
     }
 
+    #[tokio::test]
+    async fn the_retained_wal_is_reported_and_bounded() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let p = pipeline("", "flush: { seconds: 0 }", "max_wal_lag_bytes: 64MiB");
+        let (mut core, _control, status) = w.core(&p, &["a"]);
+        core.settings.heartbeat = Duration::from_millis(5);
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        let driver = async {
+            w.source.set_retained(Some(1024));
+            wait_until("the lag is reported", || {
+                status.borrow().retained_bytes == Some(1024)
+            })
+            .await;
+            w.source.set_retained(Some(65 << 20));
+        };
+        let (end, ()) = tokio::join!(s.run(), driver);
+        assert!(matches!(
+            end,
+            Err(WorkerError::WalLag {
+                lag: 68_157_440,
+                limit: 67_108_864
+            })
+        ));
+    }
+
     fn with_note(name: &str) -> TableSchema {
         TableSchema::new(
             t(name),
```

Create `crates/data-avalanche-pipeline/src/worker.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, Row};
    use data_avalanche_testkit::{FakeTxn, Method};

    use super::*;
    use crate::test_support::{World, Yaml, fast, row, t, wait_until};

    fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
        ids.map(|id| row(id, "x")).collect()
    }

    fn mode(tables: &str) -> Pipeline {
        let tables = format!("\n      snapshots: {{ {tables} }}");
        Yaml {
            tables: &tables,
            sink: "flush: { seconds: 0 }",
            ..Yaml::default()
        }
        .build()
    }

    /// Runs a worker over `w` until `drive` returns, then drains it.
    async fn run<F: Future<Output = ()>>(
        w: &World,
        p: &Pipeline,
        drive: impl FnOnce(watch::Receiver<WorkerStatus>) -> F,
    ) -> (Result<Exit, WorkerError>, WorkerStatus) {
        let deps = WorkerDeps::new(
            Arc::new(w.source.clone()),
            Arc::new(w.sink.clone()),
            w.docs.clone(),
        );
        let (worker, handle) = PipelineWorker::new(p, deps, fast());
        let status = handle.status.clone();
        let driver = async move {
            drive(handle.status.clone()).await;
            let _ = handle.control.send(Command::Drain).await;
        };
        let (end, ()) = tokio::join!(worker.run(), driver);
        let last = status.borrow().clone();
        (end, last)
    }

    async fn streamed(status: &watch::Receiver<WorkerStatus>, w: &World) {
        let end = w.source.wal_end();
        wait_until("the log's end is durable", || {
            let st = status.borrow();
            st.checkpoint.as_ref() == Some(&end) && st.snapshots.is_empty()
        })
        .await;
    }

    async fn slot_lsn(w: &World) -> Lsn {
        w.docs
            .slot()
            .await
            .unwrap()
            .unwrap()
            .doc
            .confirmed_flush_lsn
    }

    #[tokio::test]
    async fn a_first_start_snapshots_and_streams() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=3));
        let (end, st) = run(&w, &mode("mode: initial"), async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(9, "s")));
            streamed(&status, &w).await;
        })
        .await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert_eq!(w.ids("a"), [1, 2, 3, 9]);
        assert_eq!(w.source.prepares(), [None]);
        let SourcePosition::Pg(end) = w.source.wal_end() else {
            unreachable!()
        };
        assert_eq!(slot_lsn(&w).await, end);
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(cached, w.source.schema(&t("a")));
        let doc = w.docs.table_snapshot(&t("a")).await.unwrap().unwrap().doc;
        assert_eq!(doc.status, SnapshotStatus::Completed);
        assert!(st.warnings.is_empty(), "{:?}", st.warnings);
    }

    #[tokio::test]
    async fn a_restart_resumes_from_its_checkpoint() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=2));
        let p = mode("mode: initial");
        run(&w, &p, async |status| streamed(&status, &w).await)
            .await
            .0
            .unwrap();
        let reads = w.source.reads(&t("a"));
        let checkpoint = w.source.wal_end();
        w.source
            .commit(FakeTxn::new().insert(&t("a"), row(5, "while down")));
        let (end, _) = run(&w, &p, async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.source.prepares().last(), Some(&Some(checkpoint)));
        assert_eq!(w.source.reads(&t("a")), reads, "no second snapshot");
        assert_eq!(w.ids("a"), [1, 2, 5]);
    }

    #[tokio::test]
    async fn a_first_start_cut_short_after_the_slot_was_made_starts_over() {
        let w = World::new(&["a"]);
        w.docs.save_slot(&SlotDoc::intent("p_slot")).await.unwrap();
        w.source.prepare(None, &[]).await.unwrap();
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(
            w.source.prepares().last(),
            Some(&Some(SourcePosition::Pg(Lsn(0)))),
            "the intent asks for a new slot"
        );
        assert!(
            st.warnings.is_empty(),
            "a first start, not a gap: {:?}",
            st.warnings
        );
        assert!(!w.docs.slot().await.unwrap().unwrap().doc.is_intent());
    }

    #[tokio::test]
    async fn a_lost_slot_snapshots_again_and_never_warns() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.source.load(&t("a"), rows(1..=2));
        w.source.load(&t("b"), rows(1..=2));
        w.source.lose_slot();
        let p = mode("mode: initial, overrides: { public.b: never }");
        let (end, st) = run(&w, &p, async |status| streamed(&status, &w).await).await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2]);
        assert_eq!(w.source.reads(&t("b")), 0);
        assert_eq!(st.warnings.len(), 1, "{:?}", st.warnings);
        assert!(st.warnings[0].contains("public.b") && st.warnings[0].contains("lost"));
    }

    #[tokio::test]
    async fn a_dump_initial_table_the_sink_has_stops_at_once() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.load(&t("a"), rows(1..=2));
        let deps = WorkerDeps::new(
            Arc::new(w.source.clone()),
            Arc::new(w.sink.clone()),
            w.docs.clone(),
        );
        let (worker, _handle) = PipelineWorker::new(&mode("mode: dump_initial"), deps, fast());
        assert_eq!(worker.run().await.unwrap(), Exit::Dumped);
        assert_eq!(w.source.reads(&t("a")), 0);
        assert!(w.source.opened().len() <= 1);
    }

    #[tokio::test]
    async fn a_dump_always_table_dumps_and_ends() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=3));
        let deps = WorkerDeps::new(
            Arc::new(w.source.clone()),
            Arc::new(w.sink.clone()),
            w.docs.clone(),
        );
        let (worker, _handle) = PipelineWorker::new(&mode("mode: dump_always"), deps, fast());
        assert_eq!(worker.run().await.unwrap(), Exit::Dumped);
        assert_eq!(w.ids("a"), [1, 2, 3]);
    }

    #[tokio::test]
    async fn a_state_of_another_sink_type_is_refused() {
        let w = World::new(&["a"]);
        w.docs.open("p", "iceberg").await.unwrap();
        let (end, _) = run(&w, &mode("mode: never"), async |_| {}).await;
        match end {
            Err(WorkerError::Refused(why)) => assert!(why.contains("sink type"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn an_iceberg_pipeline_is_refused_in_v1() {
        let w = World::new(&["a"]);
        let (mut core, _control, _status) = w.core(&mode("mode: never"), &[]);
        core.settings.sink_kind = SinkKind::Iceberg;
        match start(&mut core, None).await {
            Err(WorkerError::Refused(why)) => assert!(why.contains("v2"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_table_dropped_while_down_halts() {
        let w = World::new(&["a", "b"]);
        w.started(&["a", "b"]).await;
        w.source.drop_table(&t("b"));
        let (end, st) = run(&w, &mode("mode: never"), async |status| {
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(st.halted.get(&t("b")), Some(&HaltReason::TableDropped));
        assert_eq!(w.docs.halts().await.unwrap().tables[0].table, t("b"));
    }

    #[tokio::test]
    async fn a_table_created_again_while_down_halts() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        w.source.drop_table(&t("a"));
        w.source.create_table(crate::test_support::schema("a"));
        let (end, st) = run(&w, &mode("mode: never"), async |_| {}).await;
        end.unwrap();
        assert_eq!(st.halted.get(&t("a")), Some(&HaltReason::TableDropped));
    }

    #[tokio::test]
    async fn a_transient_failure_reopens_the_stream() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let (end, _) = run(&w, &mode("mode: never"), async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            streamed(&status, &w).await;
            w.source
                .fail(Method::Next, SourceError::Transient("reset".into()));
            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1, 2]);
        assert_eq!(w.source.opened().len(), 2);
    }

    #[tokio::test]
    async fn a_busy_slot_is_retried_for_a_while() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        for _ in 0..3 {
            w.source
                .fail(Method::OpenStream, SourceError::Contended("active".into()));
        }
        let (end, _) = run(&w, &mode("mode: never"), async |status| {
            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(w.ids("a"), [1]);

        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        for _ in 0..10_000 {
            w.source
                .fail(Method::OpenStream, SourceError::Contended("active".into()));
        }
        let (end, _) = run(&w, &mode("mode: never"), async |_| {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        })
        .await;
        assert!(matches!(end, Err(WorkerError::Contended { .. })), "{end:?}");
    }

    #[tokio::test]
    async fn a_position_lost_while_running_is_replaced_and_snapshotted() {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=2));
        let (end, st) = run(&w, &mode("mode: initial"), async |status| {
            streamed(&status, &w).await;
            let reads = w.source.reads(&t("a"));
            w.source.lose_slot();
            w.source
                .commit(FakeTxn::new().insert(&t("a"), row(7, "lost")));
            wait_until("the table is snapshotted again", || {
                w.source.reads(&t("a")) > reads
            })
            .await;
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        assert_eq!(
            w.ids("a"),
            [1, 2, 7],
            "the snapshot brings back what the stream lost"
        );
        assert!(
            st.warnings
                .iter()
                .any(|w| w.contains("lost the stream position"))
        );
    }

    #[tokio::test]
    async fn a_lost_position_baselines_a_drifted_table() {
        let w = World::new(&["a"]);
        w.started(&["a"]).await;
        let noted = TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("note", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        w.source.alter(&t("a"), noted);
        w.source.lose_slot();
        let (end, _) = run(&w, &mode("mode: initial"), async |status| {
            streamed(&status, &w).await;
        })
        .await;
        end.unwrap();
        let state = w.sink.table_state(&t("a")).await.unwrap();
        assert!(state.schema.unwrap().field_with_name("note").is_ok());
        let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
        assert!(cached.column("note").is_some());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

FAIL to compile (24 errors); the first:

```text
error[E0583]: file not found for module `schema_change`
error[E0432]: unresolved imports `worker::PipelineWorker`, `worker::WorkerDeps`, `worker::WorkerHandle`
error[E0425]: cannot find type `Pipeline` in this scope
error[E0425]: cannot find type `WorkerStatus` in this scope
error[E0425]: cannot find type `Exit` in this scope
error[E0425]: cannot find type `WorkerError` in this scope
… and 18 more
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-pipeline/src/schema_change.rs`:

```rust
//! `Schema_Change…` §8 for one table: what changed (channel C, then the §7.2 classification) and
//! the sink's evolve. A stream session runs them at a signal's position, with a flush in between.
//! Phase 0 runs them to baseline a table whose stream position was lost.

use data_avalanche_core::{
    DeltaClass, EvolveOutcome, HaltReason, SchemaChange, SchemaDelta, SchemaHint, Sink, SinkError,
    SourceConnector, SourceError, TableRef, TableSchema, classify,
};
use tokio::sync::watch;

use crate::{CommitGuard, Tuning, WorkerError, WorkerStatus};

/// A change to apply: the table's new schema, and the changes the sink must evolve.
#[derive(Debug)]
pub(crate) struct Planned {
    pub now: TableSchema,
    pub safe: Vec<SchemaChange>,
}

pub(crate) struct Evolver<'a> {
    pub source: &'a dyn SourceConnector,
    pub sink: &'a dyn Sink,
    pub guard: &'a dyn CommitGuard,
    pub tuning: &'a Tuning,
    pub evolution: bool,
    pub status: &'a watch::Sender<WorkerStatus>,
}

impl Evolver<'_> {
    /// Channel C against the `cached` schema, retried while it fails transiently, then the
    /// classification. `Err` halts the table: an unsafe change, a Safe one with evolution off, a
    /// dropped table, or a failed catalog read (fail-safe, §3 C).
    pub async fn plan(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<Planned, HaltReason> {
        let attempts = self.tuning.augment_attempts.max(1);
        let mut attempt = 0;
        let (delta, now) = loop {
            match self.source.augment(cached, hint).await {
                Ok(found) => break found,
                Err(SourceError::TableHalt { reason, .. }) => return Err(reason),
                Err(e) if e.is_transient() && attempt + 1 < attempts => {
                    tokio::time::sleep(self.tuning.source_backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(HaltReason::UnsafeDdl {
                        detail: format!("the catalog read for a schema change failed: {e}"),
                    });
                }
            }
        };
        let Some(now) = now else {
            return Err(HaltReason::TableDropped);
        };
        let mut safe = vec![];
        for (change, class) in classify(cached, &delta) {
            match class {
                DeltaClass::Unsafe(reason) => return Err(reason),
                DeltaClass::Safe => safe.push(change),
                DeltaClass::Ignorable => {}
            }
        }
        if !safe.is_empty() && !self.evolution {
            return Err(HaltReason::EvolutionDisabled);
        }
        Ok(Planned { now, safe })
    }

    /// The sink's evolve of the Safe changes, retried while it fails transiently. `Ok(Some)` is a
    /// degraded apply's warning; `Ok(Err)` halts the table.
    pub async fn evolve(
        &self,
        table: &TableRef,
        safe: Vec<SchemaChange>,
    ) -> Result<Result<Option<String>, HaltReason>, WorkerError> {
        let delta = SchemaDelta {
            table: table.clone(),
            changes: safe,
        };
        let mut attempt = 0;
        loop {
            if !self.guard.may_commit() {
                return Err(WorkerError::LeaseLost);
            }
            let outcome = self.sink.evolve(table, &delta).await;
            if attempt > 0 && outcome.is_ok() {
                self.status.send_modify(|st| st.degraded = None);
            }
            match outcome {
                Ok(EvolveOutcome::Applied) => return Ok(Ok(None)),
                Ok(EvolveOutcome::Degraded { warning }) => {
                    return Ok(Ok(Some(format!("table {table}: {warning}"))));
                }
                Ok(EvolveOutcome::Unsupported { reason }) => {
                    return Ok(Err(HaltReason::EvolutionUnsupported { detail: reason }));
                }
                Err(SinkError::Transient { reason }) => {
                    self.status
                        .send_modify(|st| st.degraded = Some(format!("table {table}: {reason}")));
                    tokio::time::sleep(self.tuning.sink_backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(SinkError::Permanent { reason }) => {
                    return Ok(Err(HaltReason::SinkRejected { detail: reason }));
                }
                Err(e) => return Err(WorkerError::Sink(e)),
            }
        }
    }
}
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -14,9 +14,9 @@ use std::sync::Arc;
 use std::time::Instant;
 
 use data_avalanche_core::{
-    CdcEvent, ChangeStream, ColumnValue, DeltaClass, EvolveOutcome, HaltReason, Op, RescanReport,
-    Row, SchemaDelta, SchemaHint, Sink, SinkError, SourceConnector, SourceError, SourcePosition,
-    SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary, classify, identifier_values,
+    CdcEvent, ChangeStream, ColumnValue, HaltReason, Op, RescanReport, Row, SchemaHint, Sink,
+    SourceConnector, SourceError, SourcePosition, SourceSeq, StreamItem, TableRef, TableSchema,
+    TransactionBoundary, identifier_values,
 };
 use data_avalanche_state::{PipelineDocs, TableHalt};
 use futures_util::StreamExt;
@@ -25,6 +25,7 @@ use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
 use tokio::task::JoinHandle;
 
 use self::snapshot::Snapshots;
+use crate::schema_change::{Evolver, Planned};
 use crate::settings::Settings;
 use crate::writer::{self, Job, WriterCtx, WriterEvent};
 use crate::{
@@ -56,6 +57,17 @@ impl Core {
     pub fn update(&self, f: impl FnOnce(&mut WorkerStatus)) {
         self.status.send_modify(f);
     }
+
+    pub fn evolver(&self) -> Evolver<'_> {
+        Evolver {
+            source: &*self.source,
+            sink: &*self.sink,
+            guard: &*self.guard,
+            tuning: &self.tuning,
+            evolution: self.settings.evolution,
+            status: &self.status,
+        }
+    }
 }
 
 /// How a session ended without an error.
@@ -251,6 +263,9 @@ impl<'c> Session<'c> {
     async fn event_loop(&mut self) -> Result<End, WorkerError> {
         let mut tick = tokio::time::interval(self.core.tuning.tick);
         tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
+        let every = self.core.settings.heartbeat;
+        let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
+        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
         let mut rescan = self
             .core
             .settings
@@ -306,6 +321,11 @@ impl<'c> Session<'c> {
                         return Ok(end);
                     }
                 }
+                _ = heartbeat.tick() => {
+                    if let Some(end) = self.wal_lag().await? {
+                        return Ok(end);
+                    }
+                }
                 _ = async { rescan.as_mut().expect("guarded").tick().await },
                     if rescan.is_some() && self.rescan.is_none() =>
                 {
@@ -561,6 +581,22 @@ impl<'c> Session<'c> {
         Ok(None)
     }
 
+    /// The WAL-retention guard (G-22): the retained log is always reported; past
+    /// `max_wal_lag_bytes` the pipeline stops to protect the source's disk. A failed read waits
+    /// for the next heartbeat.
+    async fn wal_lag(&mut self) -> Result<Option<End>, WorkerError> {
+        let retained = match self.core.source.retained_bytes().await {
+            Ok(retained) => retained,
+            Err(e) if e.is_transient() => return Ok(None),
+            Err(e) => return Ok(Some(End::Source(e))),
+        };
+        self.core.update(|st| st.retained_bytes = retained);
+        match (retained, self.core.settings.max_wal_lag_bytes) {
+            (Some(lag), Some(limit)) if lag > limit => Err(WorkerError::WalLag { lag, limit }),
+            _ => Ok(None),
+        }
+    }
+
     /// Every table is a dump, and every one is done or halted.
     fn dumped(&self) -> bool {
         self.core.dump_only && self.schemas.is_empty() && self.snaps.is_empty()
@@ -707,42 +743,19 @@ impl<'c> Session<'c> {
         let Some(cached) = self.schemas.get(&table).cloned() else {
             return Ok(None);
         };
-        let (delta, now) = match self.augment(&cached, &hint).await {
-            Ok(found) => found,
+        let Planned { now, safe } = match self.core.evolver().plan(&cached, &hint).await {
+            Ok(planned) => planned,
             Err(reason) => {
                 self.halt(table, reason);
                 return Ok(None);
             }
         };
-        let Some(now) = now else {
-            self.halt(table, HaltReason::TableDropped);
-            return Ok(None);
-        };
-        let mut safe = vec![];
-        for (change, class) in classify(&cached, &delta) {
-            match class {
-                DeltaClass::Unsafe(reason) => {
-                    self.halt(table, reason);
-                    return Ok(None);
-                }
-                DeltaClass::Safe => safe.push(change),
-                DeltaClass::Ignorable => {}
-            }
-        }
         if !safe.is_empty() {
-            if !self.core.settings.evolution {
-                self.halt(table, HaltReason::EvolutionDisabled);
-                return Ok(None);
-            }
             self.flush()?;
             if let Some(end) = self.until_durable().await? {
                 return Ok(Some(end));
             }
-            let delta = SchemaDelta {
-                table: table.clone(),
-                changes: safe,
-            };
-            match self.evolve(&delta).await? {
+            match self.core.evolver().evolve(&table, safe).await? {
                 Ok(None) => {}
                 Ok(Some(warning)) => self.core.update(|st| st.warnings.push(warning)),
                 Err(reason) => {
@@ -771,69 +784,6 @@ impl<'c> Session<'c> {
         }
         Ok(None)
     }
-
-    /// Channel C, retried while it fails transiently; a failure halts the table (fail-safe).
-    async fn augment(
-        &mut self,
-        cached: &TableSchema,
-        hint: &SchemaHint,
-    ) -> Result<(SchemaDelta, Option<TableSchema>), HaltReason> {
-        let attempts = self.core.tuning.augment_attempts.max(1);
-        let mut attempt = 0;
-        loop {
-            match self.core.source.augment(cached, hint).await {
-                Ok(found) => return Ok(found),
-                Err(SourceError::TableHalt { reason, .. }) => return Err(reason),
-                Err(e) if e.is_transient() && attempt + 1 < attempts => {
-                    tokio::time::sleep(self.core.tuning.source_backoff.delay(attempt)).await;
-                    attempt += 1;
-                }
-                Err(e) => {
-                    return Err(HaltReason::UnsafeDdl {
-                        detail: format!("the catalog read for a schema change failed: {e}"),
-                    });
-                }
-            }
-        }
-    }
-
-    /// The sink's evolve, retried while it fails transiently. `Ok(Some(warning))` for a degraded
-    /// apply; `Err` halts the table.
-    async fn evolve(
-        &mut self,
-        delta: &SchemaDelta,
-    ) -> Result<Result<Option<String>, HaltReason>, WorkerError> {
-        let mut attempt = 0;
-        loop {
-            if !self.core.guard.may_commit() {
-                return Err(WorkerError::LeaseLost);
-            }
-            let outcome = self.core.sink.evolve(&delta.table, delta).await;
-            if attempt > 0 && outcome.is_ok() {
-                self.core.update(|st| st.degraded = None);
-            }
-            match outcome {
-                Ok(EvolveOutcome::Applied) => return Ok(Ok(None)),
-                Ok(EvolveOutcome::Degraded { warning }) => {
-                    return Ok(Ok(Some(format!("table {}: {warning}", delta.table))));
-                }
-                Ok(EvolveOutcome::Unsupported { reason }) => {
-                    return Ok(Err(HaltReason::EvolutionUnsupported { detail: reason }));
-                }
-                Err(SinkError::Transient { reason }) => {
-                    self.core.update(|st| {
-                        st.degraded = Some(format!("table {}: {reason}", delta.table));
-                    });
-                    tokio::time::sleep(self.core.tuning.sink_backoff.delay(attempt)).await;
-                    attempt += 1;
-                }
-                Err(SinkError::Permanent { reason }) => {
-                    return Ok(Err(HaltReason::SinkRejected { detail: reason }));
-                }
-                Err(e) => return Err(WorkerError::Sink(e)),
-            }
-        }
-    }
 }
 
 #[cfg(test)]
```

Change `crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -152,10 +152,6 @@ impl Snapshots {
         Ok(())
     }
 
-    pub(super) fn is_snapshotting(&self, table: &TableRef) -> bool {
-        self.tables.contains_key(table)
-    }
-
     pub(super) fn tables(&self) -> impl Iterator<Item = &TableRef> {
         self.tables.keys()
     }
```

Change `crates/data-avalanche-pipeline/src/status.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -53,6 +53,9 @@ pub struct WorkerStatus {
     pub read: Option<SourcePosition>,
     /// The durable checkpoint: what the source was last told it may release.
     pub checkpoint: Option<SourcePosition>,
+    /// The log the source keeps for the pipeline, read every heartbeat (PostgreSQL: the slot's
+    /// retained WAL, `data_avalanche_wal_lag_bytes`).
+    pub retained_bytes: Option<u64>,
     /// Sink commits or state writes are failing and being retried (the last error).
     pub degraded: Option<String>,
     pub halted: BTreeMap<TableRef, HaltReason>,
```

Add the implementation to `crates/data-avalanche-pipeline/src/worker.rs`, above the test module:

```rust
//! The pipeline worker (Data-Flow Phases 0–5): Phase 0 once, then stream sessions until a drain,
//! a finished dump or an error. Between sessions it decides what a source failure means: a
//! transient one reopens the stream after a backoff, a busy slot is retried for a bounded time,
//! and a lost position is replaced and every table snapshotted again.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use data_avalanche_config::{Pipeline, SnapshotMode};
use data_avalanche_core::{
    HaltReason, SchemaHint, Sink, SinkError, SinkKind, SnapshotReason, SourceConnector,
    SourceError, SourcePosition, StartPlan, TableRef, TableSchema, TableSpec,
};
use data_avalanche_state::{
    PipelineDocs, SlotDoc, SnapshotStatus, StateError, TableHalt, TableSnapshotDoc,
};
use data_avalanche_storage::StoreError;
use tokio::sync::{mpsc, watch};

use crate::decide::{Decision, Facts, Plan, decide};
use crate::schema_change::Planned;
use crate::session::{Core, End, Session};
use crate::settings::Settings;
use crate::{
    Command, CommitGuard, Exit, Hooks, NoHooks, Phase, Tuning, Unguarded, WorkerError, WorkerStatus,
};

/// What a worker runs against.
#[derive(Clone)]
pub struct WorkerDeps {
    pub source: Arc<dyn SourceConnector>,
    pub sink: Arc<dyn Sink>,
    /// The pipeline's state documents.
    pub docs: PipelineDocs,
    /// The lease epoch every commit carries (Clustering §19).
    pub owner_epoch: u64,
    pub guard: Arc<dyn CommitGuard>,
    pub hooks: Arc<dyn Hooks>,
}

impl std::fmt::Debug for WorkerDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerDeps")
            .field("owner_epoch", &self.owner_epoch)
            .finish_non_exhaustive()
    }
}

impl WorkerDeps {
    /// A worker no lease guards, with no test hooks.
    pub fn new(source: Arc<dyn SourceConnector>, sink: Arc<dyn Sink>, docs: PipelineDocs) -> Self {
        Self {
            source,
            sink,
            docs,
            owner_epoch: 1,
            guard: Arc::new(Unguarded),
            hooks: Arc::new(NoHooks),
        }
    }
}

/// The owner's side of a running worker.
#[derive(Debug, Clone)]
pub struct WorkerHandle {
    pub control: mpsc::Sender<Command>,
    pub status: watch::Receiver<WorkerStatus>,
}

/// One pipeline's worker.
pub struct PipelineWorker {
    core: Core,
}

impl std::fmt::Debug for PipelineWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineWorker")
            .field("pipeline", &self.core.settings.pipeline_id)
            .finish_non_exhaustive()
    }
}

impl PipelineWorker {
    pub fn new(pipeline: &Pipeline, deps: WorkerDeps, tuning: Tuning) -> (Self, WorkerHandle) {
        let (control, control_rx) = mpsc::channel(16);
        let (status_tx, status) = watch::channel(WorkerStatus::default());
        let core = Core {
            settings: Settings::new(pipeline),
            source: deps.source,
            sink: deps.sink,
            docs: deps.docs,
            owner_epoch: deps.owner_epoch,
            guard: deps.guard,
            hooks: deps.hooks,
            tuning,
            control: control_rx,
            status: status_tx,
            tracked: BTreeSet::new(),
            dump_only: false,
            snapshot_started: BTreeMap::new(),
        };
        (Self { core }, WorkerHandle { control, status })
    }

    /// Runs the pipeline until a [`Command::Drain`] (or the handle drops), until every `dump_*`
    /// table is done, or until an error stops it.
    ///
    /// # Errors
    /// [`WorkerError`]: the pipeline is refused, a permanent source or sink failure, a newer owner,
    /// a lost lease.
    pub async fn run(mut self) -> Result<Exit, WorkerError> {
        let core = &mut self.core;
        start(core, None).await?;
        let mut attempt = 0;
        let mut contended_since: Option<Instant> = None;
        loop {
            let opened = Instant::now();
            let failed = match Session::open(core).await {
                Ok(Ok(session)) => match session.run().await {
                    Ok(End::Exit(exit)) => return Ok(exit),
                    Ok(End::Source(e)) => e,
                    Err(e) if transient_state(&e) => SourceError::Transient(e.to_string()),
                    Err(e) => return Err(e),
                },
                Ok(Err(e)) => e,
                Err(e) if transient_state(&e) => SourceError::Transient(e.to_string()),
                Err(e) => return Err(e),
            };
            if opened.elapsed() >= core.tuning.source_backoff.max {
                attempt = 0;
            }
            match failed {
                SourceError::Transient(error) => {
                    contended_since = None;
                    core.update(|st| st.phase = Phase::Reconnecting { error });
                }
                SourceError::Contended(detail) => {
                    let since = *contended_since.get_or_insert_with(Instant::now);
                    let waited = since.elapsed();
                    if waited >= core.tuning.contended_window {
                        return Err(WorkerError::Contended { waited, detail });
                    }
                }
                SourceError::PositionUnreachable { position, detail } => {
                    contended_since = None;
                    let warning = format!(
                        "the source lost the stream position {position} ({detail}): the stream restarts at a new position and the tables are snapshotted again"
                    );
                    core.update(|st| st.warnings.push(warning));
                    let from = retry(&core.tuning, || core.source.reset_position()).await?;
                    start(core, Some(from)).await?;
                    attempt = 0;
                    continue;
                }
                e => return Err(WorkerError::Source(e)),
            }
            let delay = core.tuning.source_backoff.delay(attempt);
            attempt = attempt.saturating_add(1);
            if drained_while(core, delay).await {
                return Ok(Exit::Drained);
            }
        }
    }
}

/// A storage failure worth another session: the write or read may succeed next time.
fn transient_state(e: &WorkerError) -> bool {
    matches!(
        e,
        WorkerError::State(StateError::Store(
            StoreError::Transient { .. } | StoreError::Contention { .. }
        ))
    )
}

/// Waits `delay` between sessions; true when a drain arrives meanwhile (nothing is buffered).
async fn drained_while(core: &mut Core, delay: std::time::Duration) -> bool {
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            () = &mut sleep => return false,
            cmd = core.control.recv() => match cmd {
                Some(Command::Flush) => {}
                Some(Command::Drain) | None => return true,
            },
        }
    }
}

/// A source call, retried while it fails transiently.
async fn retry<T, F, Fut>(tuning: &Tuning, mut call: F) -> Result<T, WorkerError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SourceError>>,
{
    let mut attempt = 0;
    loop {
        match call().await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_transient() => {
                tokio::time::sleep(tuning.source_backoff.delay(attempt)).await;
                attempt += 1;
            }
            Err(e) => return Err(WorkerError::Source(e)),
        }
    }
}

/// A sink call, retried while it fails transiently.
async fn sink_retry<T, F, Fut>(tuning: &Tuning, mut call: F) -> Result<T, SinkError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, SinkError>>,
{
    let mut attempt = 0;
    loop {
        match call().await {
            Err(e) if e.is_transient() => {
                tokio::time::sleep(tuning.sink_backoff.delay(attempt)).await;
                attempt += 1;
            }
            r => return r,
        }
    }
}

/// Data-Flow Phase 0, or its rerun after the stream position was lost mid-run (`lost` holds the
/// new position `reset_position` returned). Leaves the core tracking the tables to stream, with
/// every decision persisted: halts and snapshot requests first, then the checkpoint, then the
/// cached schemas.
pub(crate) async fn start(
    core: &mut Core,
    lost: Option<SourcePosition>,
) -> Result<(), WorkerError> {
    core.update(|st| st.phase = Phase::Starting);
    let s = &core.settings;
    if s.sink_kind == SinkKind::Iceberg {
        return Err(WorkerError::Refused(
            "the Iceberg sink ships in v2 (D-V1.4); other pipelines run".into(),
        ));
    }
    let Some(slot) = s.slot.clone() else {
        return Err(WorkerError::Refused(
            "this build streams from PostgreSQL only; the MySQL source arrives with M7".into(),
        ));
    };
    if core.sink.kind() != s.sink_kind {
        return Err(WorkerError::Refused(format!(
            "the sink given is {:?}, the config says {:?}",
            core.sink.kind(),
            s.sink_kind
        )));
    }
    let sink_type = match s.sink_kind {
        SinkKind::Delta => "delta",
        SinkKind::Iceberg => "iceberg",
    };
    match core.docs.open(&s.pipeline_id, sink_type).await {
        Ok(_) => {}
        Err(e @ (StateError::SinkTypeMismatch { .. } | StateError::PipelineIdMismatch { .. })) => {
            return Err(WorkerError::Refused(e.to_string()));
        }
        Err(e) => return Err(e.into()),
    }
    let source = Arc::clone(&core.source);
    let sink = Arc::clone(&core.sink);
    let tuning = core.tuning.clone();
    let preflight = retry(&tuning, || source.preflight()).await?;
    let resolved = retry(&tuning, || source.resolve_tables()).await?;
    let mut warnings = preflight.warnings;
    warnings.extend(resolved.warnings);
    warnings.extend(
        resolved
            .skipped
            .iter()
            .map(|(t, why)| format!("table {t} isn't replicated: {why}")),
    );
    for table in &resolved.tables {
        let partition_by = core.settings.partition_by(table.table());
        if let Some(why) = sink
            .capabilities()
            .refusal(core.settings.write_mode, &partition_by)
        {
            return Err(WorkerError::Refused(format!(
                "table {}: {why}",
                table.table()
            )));
        }
    }

    // The state as of the checkpoint.
    let cached: BTreeMap<TableRef, TableSchema> = core
        .docs
        .table_schemas()
        .await?
        .into_iter()
        .map(|t| (t.table().clone(), t))
        .collect();
    let recorded: BTreeSet<TableRef> = core
        .docs
        .halts()
        .await?
        .tables
        .into_iter()
        .map(|h| h.table)
        .collect();
    let slot_doc = core.docs.slot().await?.map(|v| v.doc);

    // New halts, recorded before the checkpoint below.
    let mut halts: Vec<(TableRef, HaltReason)> = resolved.halted.clone();
    let resolved_tables: BTreeMap<TableRef, TableSchema> = resolved
        .tables
        .into_iter()
        .map(|t| (t.table().clone(), t))
        .collect();
    let gone: Vec<TableSchema> = cached
        .values()
        .filter(|c| !resolved_tables.contains_key(c.table()) && !recorded.contains(c.table()))
        .cloned()
        .collect();
    if !gone.is_empty()
        && let Ok(report) = source.rescan(&gone).await
    {
        halts.extend(
            report
                .dropped
                .into_iter()
                .map(|t| (t, HaltReason::TableDropped)),
        );
    }
    for (table, now) in &resolved_tables {
        if let Some(c) = cached.get(table)
            && c.source_id().is_some()
            && c.source_id() != now.source_id()
        {
            halts.push((table.clone(), HaltReason::TableDropped));
        }
    }

    // The stream position: prepare, or the one a mid-run recovery got.
    let intent = slot_doc.as_ref().is_some_and(SlotDoc::is_intent);
    let checkpoint = slot_doc
        .as_ref()
        .map(|d| SourcePosition::Pg(d.confirmed_flush_lsn));
    let (plan, at, added) = match lost {
        Some(from) => (Plan::Fresh(SnapshotReason::PositionLost), from, vec![]),
        None => {
            if slot_doc.is_none()
                && let Some(name) = &core.settings.slot_intent
            {
                core.docs.save_slot(&SlotDoc::intent(name)).await?;
            }
            let refs: Vec<TableRef> = resolved_tables.keys().cloned().collect();
            let prepared = retry(&tuning, || source.prepare(checkpoint.as_ref(), &refs)).await?;
            warnings.extend(prepared.warnings);
            match prepared.plan {
                StartPlan::Resume => {
                    let Some(at) = checkpoint.clone() else {
                        return Err(WorkerError::Refused(
                            "the source resumes a stream the pipeline has no checkpoint for".into(),
                        ));
                    };
                    (Plan::Resume, at, prepared.added)
                }
                StartPlan::Snapshot { from, reason } => {
                    // A start cut short after the slot was made starts over as a first start.
                    let reason = if intent {
                        SnapshotReason::FirstStart
                    } else {
                        reason
                    };
                    (Plan::Fresh(reason), from, vec![])
                }
            }
        }
    };
    let gap = matches!(
        plan,
        Plan::Fresh(SnapshotReason::PositionLost | SnapshotReason::AdoptedOrphan)
    );

    // Each table: what this start does with it.
    let halted: BTreeSet<TableRef> = recorded
        .iter()
        .cloned()
        .chain(halts.iter().map(|(t, _)| t.clone()))
        .collect();
    let mut tracked = BTreeSet::new();
    let mut requests = vec![];
    let mut abandoned = vec![];
    let mut schemas = vec![];
    let mut dump_only = !resolved_tables.is_empty();
    for (table, now) in &resolved_tables {
        let mode = core.settings.mode(table);
        dump_only &= matches!(mode, SnapshotMode::DumpAlways | SnapshotMode::DumpInitial);
        if halted.contains(table) {
            tracked.insert(table.clone());
            continue;
        }
        let state = sink_retry(&tuning, || sink.table_state(table))
            .await
            .map_err(WorkerError::Sink)?;
        let doc = core.docs.table_snapshot(table).await?.map(|v| v.doc);
        let in_progress = doc
            .as_ref()
            .is_some_and(|d| d.status == SnapshotStatus::InProgress);
        let tracked_before = cached.contains_key(table);
        let facts = Facts {
            tracked_before,
            // After a gap, a snapshot starts over: rows before its cursor may have changed.
            in_progress: in_progress && !gap,
            sink_has_table: state.exists,
            added: added.contains(table),
        };
        let (decision, warning) = decide(mode, plan, facts);
        if let Some(w) = warning {
            warnings.push(format!("table {table}: {w}"));
        }
        match decision {
            Decision::Stop => continue,
            Decision::Stream if in_progress => abandoned.push(table.clone()),
            Decision::Snapshot { resume: false, .. } => requests.push(table.clone()),
            _ => {}
        }
        // The sink table: created when absent, baselined to the schema now after a gap in the
        // stream (`Schema_Change…` §8.2), else the cached schema stays until the stream replays.
        let partition_by = core.settings.partition_by(table);
        let spec = match TableSpec::new(now, partition_by) {
            Ok(spec) => spec,
            Err(reason) => {
                halts.push((table.clone(), reason));
                tracked.insert(table.clone());
                continue;
            }
        };
        let mut schema = now.clone();
        match cached.get(table) {
            Some(c)
                if state.exists
                    && matches!(plan, Plan::Fresh(_))
                    && c.columns() != now.columns() =>
            {
                let evolver = core.evolver();
                let hint = SchemaHint::Columns(now.columns().to_vec());
                match evolver.plan(c, &hint).await {
                    Err(reason) => {
                        halts.push((table.clone(), reason));
                        tracked.insert(table.clone());
                        continue;
                    }
                    Ok(Planned { now, safe }) => {
                        if !safe.is_empty() {
                            match evolver.evolve(table, safe).await? {
                                Ok(None) => {}
                                Ok(Some(w)) => warnings.push(w),
                                Err(reason) => {
                                    halts.push((table.clone(), reason));
                                    tracked.insert(table.clone());
                                    continue;
                                }
                            }
                        }
                        schema = now;
                    }
                }
            }
            Some(c) if matches!(plan, Plan::Resume) && c.columns() != now.columns() => {
                warnings.push(format!(
                    "table {table}: its schema changed while the pipeline was down; the stream replays the change in order"
                ));
                schema = c.clone();
            }
            Some(c) if matches!(plan, Plan::Resume) => schema = c.clone(),
            _ => {}
        }
        if !state.exists {
            match sink_retry(&tuning, || sink.ensure_table(&spec)).await {
                Ok(()) => {}
                Err(SinkError::Permanent { reason }) => {
                    halts.push((table.clone(), HaltReason::SinkRejected { detail: reason }));
                    tracked.insert(table.clone());
                    continue;
                }
                Err(e) => return Err(WorkerError::Sink(e)),
            }
        }
        if !tracked_before || matches!(plan, Plan::Fresh(_)) {
            schemas.push(schema);
        }
        tracked.insert(table.clone());
    }

    // Persist, in this order: a restart in between redoes what follows.
    for (table, reason) in &halts {
        core.docs
            .record_halt(&TableHalt {
                table: table.clone(),
                reason: reason.clone(),
                at: at.clone(),
            })
            .await?;
    }
    for table in &requests {
        core.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: table.clone(),
                status: SnapshotStatus::InProgress,
                start_position: Some(at.clone()),
                cursor: None,
                row_count: None,
                completed_at: None,
            })
            .await?;
    }
    for table in &abandoned {
        core.docs
            .save_table_snapshot(&TableSnapshotDoc {
                table: table.clone(),
                status: SnapshotStatus::None,
                start_position: None,
                cursor: None,
                row_count: None,
                completed_at: None,
            })
            .await?;
    }
    if !matches!(plan, Plan::Resume) {
        let SourcePosition::Pg(lsn) = at else {
            return Err(WorkerError::Refused(format!(
                "the source started at {at}, not a PostgreSQL position"
            )));
        };
        core.docs
            .save_slot(&SlotDoc {
                slot_name: slot,
                confirmed_flush_lsn: lsn,
            })
            .await?;
    }
    for schema in &schemas {
        core.docs.save_table_schema(schema).await?;
    }
    core.tracked = tracked;
    core.dump_only = dump_only;
    core.update(|st| st.warnings.extend(warnings));
    Ok(())
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

PASS:

```text
test result: ok. 117 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.55s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): the pipeline worker: Phase 0, sessions, reconnects, a busy slot, a lost position, the WAL-lag guard"
```


### Task 12: Crash recovery and control, through the worker

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/worker.rs` (tests), `…/test_support.rs`

**Interfaces:**
- Consumes: Task 11's `PipelineWorker`, Task 5's `Hooks`, Task 4's `MemorySink` faults.
- Produces: tests only, and the `CrashAfter` hook (crash at the `n`th matching point).

Tests of behaviour Tasks 6–11 built, through `PipelineWorker::run`. The crash case runs
four times, crashing after a commit, after a checkpoint, after a chunk and after the last chunk.
Each run streams changes during the snapshot, deletes a row while the worker is down, restarts,
and asserts the sink equals the source. A newer owner's epoch and a lost lease each stop the
worker before its next commit. `Command::Flush` flushes now, and a failing sink shows DEGRADED
until it recovers.

They pass when written: they pin the behaviour. Removing the snapshot-resume condition from
`decide` (`f.in_progress ||` in the `Initial` arm) makes `a_restart_after_a_crash_at_any_point_converges`
fail; measured while planning.

- [ ] **Step 1: Write the tests**

Change `crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -1,7 +1,7 @@
 //! Fixtures for the crate's unit tests.
 
 use std::collections::BTreeMap;
-use std::sync::atomic::{AtomicBool, Ordering};
+use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
 use std::sync::{Arc, Mutex};
 use std::time::{Duration, Instant};
 
@@ -240,6 +240,34 @@ impl VersionedStore for FlakyStore {
     }
 }
 
+/// Crashes at the `n`th hook point `when` matches (1 = the first).
+#[derive(Debug)]
+pub(crate) struct CrashAfter {
+    pub when: fn(&HookPoint) -> bool,
+    pub n: usize,
+    pub seen: AtomicUsize,
+}
+
+impl CrashAfter {
+    pub fn new(when: fn(&HookPoint) -> bool, n: usize) -> Self {
+        Self {
+            when,
+            n,
+            seen: AtomicUsize::new(0),
+        }
+    }
+}
+
+impl Hooks for CrashAfter {
+    fn at(&self, point: &HookPoint) -> HookAction {
+        if (self.when)(point) && self.seen.fetch_add(1, Ordering::SeqCst) + 1 == self.n {
+            HookAction::Crash
+        } else {
+            HookAction::Continue
+        }
+    }
+}
+
 /// Crashes at the first hook point `when` matches.
 #[derive(Debug)]
 pub(crate) struct CrashAt(pub fn(&HookPoint) -> bool);
```

Change `crates/data-avalanche-pipeline/src/worker.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -555,6 +555,7 @@ mod tests {
     use data_avalanche_testkit::{FakeTxn, Method};
 
     use super::*;
+    use crate::HookPoint;
     use crate::test_support::{World, Yaml, fast, row, t, wait_until};
 
     fn rows(ids: std::ops::RangeInclusive<i64>) -> Vec<Row> {
@@ -874,4 +875,211 @@ mod tests {
         let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
         assert!(cached.column("note").is_some());
     }
+
+    /// Runs a worker with `deps` until `drive` returns, then drains it.
+    async fn run_with<F: Future<Output = ()>>(
+        p: &Pipeline,
+        deps: WorkerDeps,
+        drive: impl FnOnce(WorkerHandle) -> F,
+    ) -> Result<Exit, WorkerError> {
+        let (worker, handle) = PipelineWorker::new(p, deps, fast());
+        let control = handle.control.clone();
+        let driver = async move {
+            drive(handle).await;
+            let _ = control.send(Command::Drain).await;
+        };
+        let (end, ()) = tokio::join!(worker.run(), driver);
+        end
+    }
+
+    fn deps(w: &World) -> WorkerDeps {
+        WorkerDeps::new(
+            Arc::new(w.source.clone()),
+            Arc::new(w.sink.clone()),
+            w.docs.clone(),
+        )
+    }
+
+    /// The source's rows of `table`, as the sink's `id`s and `doc`s.
+    fn source_rows(w: &World, table: &str) -> (Vec<i64>, Vec<Option<String>>) {
+        let rows = w.source.rows(&t(table));
+        let ids = rows
+            .iter()
+            .map(|r| match r.get("id") {
+                Some(data_avalanche_core::ColumnValue::Int64(i)) => *i,
+                other => panic!("{other:?}"),
+            })
+            .collect();
+        let docs = rows
+            .iter()
+            .map(|r| match r.get("doc") {
+                Some(data_avalanche_core::ColumnValue::Text(d)) => Some(d.clone()),
+                _ => None,
+            })
+            .collect();
+        (ids, docs)
+    }
+
+    /// A crash at each point leaves state a restart finishes from: the sink ends equal to the
+    /// source.
+    #[tokio::test]
+    async fn a_restart_after_a_crash_at_any_point_converges() {
+        type At = fn(&HookPoint) -> bool;
+        let points: [(&str, At, usize); 4] = [
+            (
+                "after a commit",
+                |p| matches!(p, HookPoint::SinkCommitted { .. }),
+                2,
+            ),
+            (
+                "after a checkpoint",
+                |p| matches!(p, HookPoint::CheckpointPersisted { .. }),
+                2,
+            ),
+            (
+                "after a chunk",
+                |p| matches!(p, HookPoint::ChunkEmitted { .. }),
+                2,
+            ),
+            (
+                "after the last chunk",
+                |p| matches!(p, HookPoint::ChunkEmitted { .. }),
+                4,
+            ),
+        ];
+        for (name, when, n) in points {
+            let w = World::new(&["a"]);
+            w.source.load(&t("a"), rows(1..=6));
+            let p = mode("mode: initial, chunk_size: 2");
+            let mut crashing = deps(&w);
+            crashing.hooks = Arc::new(crate::test_support::CrashAfter::new(when, n));
+            let end = run_with(&p, crashing, async |h| {
+                w.source.commit(
+                    FakeTxn::new()
+                        .update(&t("a"), row(2, "streamed"))
+                        .insert(&t("a"), row(8, "streamed")),
+                );
+                let _ = tokio::time::timeout(std::time::Duration::from_secs(1), async {
+                    let mut st = h.status.clone();
+                    let _ = st.wait_for(|_| false).await;
+                })
+                .await;
+            })
+            .await;
+            assert!(
+                matches!(end, Err(WorkerError::Crashed(_))),
+                "{name}: {end:?}"
+            );
+            w.source.commit(FakeTxn::new().delete(
+                &t("a"),
+                Row(vec![(
+                    "id".into(),
+                    data_avalanche_core::ColumnValue::Int64(1),
+                )]),
+            ));
+            let end = run_with(&p, deps(&w), async |h| streamed(&h.status, &w).await).await;
+            assert_eq!(end.unwrap(), Exit::Drained, "{name}");
+            let (ids, docs) = source_rows(&w, "a");
+            assert_eq!(w.ids("a"), ids, "{name}");
+            assert_eq!(w.text("a", "doc"), docs, "{name}");
+        }
+    }
+
+    #[tokio::test]
+    async fn a_newer_owner_stops_the_worker() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let end = run_with(&mode("mode: never"), deps(&w), async |h| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            streamed(&h.status, &w).await;
+            w.sink.take_over(&t("a"), 9);
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
+            let mut st = h.status.clone();
+            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), st.wait_for(|_| false))
+                .await;
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
+    }
+
+    #[tokio::test]
+    async fn a_lost_lease_stops_the_worker_before_its_next_commit() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let guard = Arc::new(crate::test_support::Switch::default());
+        let mut d = deps(&w);
+        d.guard = guard.clone();
+        let end = run_with(&mode("mode: never"), d, async |h| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            streamed(&h.status, &w).await;
+            guard.0.store(true, std::sync::atomic::Ordering::SeqCst);
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
+            let mut st = h.status.clone();
+            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), st.wait_for(|_| false))
+                .await;
+        })
+        .await;
+        assert!(matches!(end, Err(WorkerError::LeaseLost)), "{end:?}");
+        assert_eq!(w.ids("a"), [1]);
+    }
+
+    #[tokio::test]
+    async fn a_flush_command_flushes_now() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let p = Yaml {
+            sink: "flush: { seconds: 3600 }",
+            ..Yaml::default()
+        }
+        .build();
+        let end = run_with(&p, deps(&w), async |h| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            let end = w.source.wal_end();
+            wait_until("it is read", || {
+                h.status.borrow().read.as_ref() == Some(&end)
+            })
+            .await;
+            assert!(w.ids("a").is_empty());
+            h.control.send(Command::Flush).await.unwrap();
+            streamed(&h.status, &w).await;
+            assert_eq!(w.ids("a"), [1], "flushed before the drain");
+        })
+        .await;
+        end.unwrap();
+    }
+
+    #[tokio::test]
+    async fn a_failing_sink_shows_degraded_until_it_recovers() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        w.sink
+            .fail(data_avalanche_testkit::SinkOp::Commit, None, 3, || {
+                SinkError::Transient {
+                    reason: "throttled".into(),
+                }
+            });
+        let seen = std::sync::Mutex::new(false);
+        let end = run_with(&mode("mode: never"), deps(&w), async |h| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            let mut st = h.status.clone();
+            let _ = st.wait_for(|s| s.degraded.is_some()).await;
+            *seen.lock().unwrap() = true;
+            streamed(&h.status, &w).await;
+            assert_eq!(h.status.borrow().degraded, None);
+        })
+        .await;
+        end.unwrap();
+        assert!(*seen.lock().unwrap());
+        assert_eq!(w.ids("a"), [1]);
+    }
 }
```

- [ ] **Step 2: Run the tests (they pass: they pin behaviour already built)**

```bash
cargo test -p data-avalanche-pipeline --lib worker::
```

Expected:

PASS:

```text
test result: ok. 19 passed; 0 failed; 0 ignored; 0 measured; 103 filtered out; finished in 0.53s
```

- [ ] **Step 3: Implement**

Nothing to implement: Step 1's tests are the deliverable.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

PASS:

```text
test result: ok. 122 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.55s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(pipeline): crash recovery at every hook point, a newer owner, a lost lease, the flush command, a degraded sink"
```


### Task 13: The worker against a live PostgreSQL

**Files:**
- Create: `crates/data-avalanche-pipeline/tests/pg/{main,common,stream,snapshot,schema,recovery}.rs`,
  `crates/data-avalanche-pipeline/tests/sync_commit.rs`
- Modify: `crates/data-avalanche-pipeline/Cargo.toml` (dev: `data-avalanche-source-pg`,
  `tokio-postgres`), `.github/workflows/weekly.yml`

**Interfaces:**
- Consumes: `PipelineWorker` (Task 11), `PgConnector` (M5, Task 2), `MemorySink`.
- Produces: the `tests/pg` and `tests/sync_commit` binaries. Each test owns a schema, slot
  and publication `pl_<test>`. `common::converge` waits until the sink equals the source.

The live suite: DML, key changes and NULLs; a TRUNCATE in a transaction; a 40,000-row
snapshot under four concurrent writers; a `dump_initial` that ends the worker; an added column
mid-stream; a dropped table (channel D, `rescan_interval: 1s`); a restart that resumes without a
second snapshot; and a crash at the third chunk's commit that resumes from the saved cursor.
`tests/sync_commit` stalls a synchronous commit (a phantom standby) while the snapshot reads the
row's chunk. The stream's change must survive the chunk.

The tests pass when written: they pin Tasks 5–11 on a real server. Measured while planning:
with the reconciler's filtering disabled, the busy-snapshot test fails (3 of 3 runs) and
`tests/sync_commit` fails. Under the window rule (no xid visibility), `tests/sync_commit` fails
too (2 of 2 runs). Without these, a test's waiting can hide the defect.

- [ ] **Step 1: Write the tests**

Change `crates/data-avalanche-pipeline/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-pipeline/Cargo.toml b/crates/data-avalanche-pipeline/Cargo.toml
--- a/crates/data-avalanche-pipeline/Cargo.toml
+++ b/crates/data-avalanche-pipeline/Cargo.toml
@@ -28,7 +28,9 @@ uuid.workspace = true
 [dev-dependencies]
 async-trait.workspace = true
 bytes.workspace = true
-object_store.workspace = true
+data-avalanche-source-pg.workspace = true
 data-avalanche-testkit.workspace = true
-tokio.workspace = true
+object_store.workspace = true
 proptest.workspace = true
+tokio.workspace = true
+tokio-postgres.workspace = true
```

Create `crates/data-avalanche-pipeline/tests/pg/common.rs`:

```rust
//! Shared fixtures: each test owns a schema `pl_<test>`, a slot `pl_<test>` and a publication
//! `pl_<test>_pub`, dropped and re-created at its start, so a failed run leaves nothing that
//! breaks the next one.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Int64Array, StringArray};
use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::TableRef;
use data_avalanche_pipeline::{
    Backoff, Command, Exit, Hooks, Phase, PipelineWorker, Tuning, WorkerDeps, WorkerError,
    WorkerHandle, WorkerStatus,
};
use data_avalanche_source_pg::PgConnector;
use data_avalanche_state::{DocStore, PipelineDocs};
use data_avalanche_storage::ObjectStoreVersioned;
use data_avalanche_testkit::MemorySink;
use object_store::memory::InMemory;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418).
pub fn port() -> u16 {
    std::env::var("DATA_AVALANCHE_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55418)
}

pub fn dsn() -> String {
    format!(
        "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
        port()
    )
}

/// A superuser connection for test setup and writes.
pub async fn admin() -> Client {
    let (client, conn) = tokio_postgres::connect(&dsn(), NoTls)
        .await
        .expect("connect to test PostgreSQL");
    tokio::spawn(conn);
    client
}

/// The test's names: schema and slot `pl_<test>`, publication `pl_<test>_pub`.
pub struct Names {
    pub schema: String,
    pub slot: String,
    pub publication: String,
}

impl Names {
    pub fn table(&self, name: &str) -> TableRef {
        TableRef::new(&self.schema, name)
    }
}

/// Drops what an earlier run of `test` left and creates its schema again, then runs `ddl` in it.
pub async fn fresh(admin: &Client, test: &str, ddl: &str) -> Names {
    let n = Names {
        schema: format!("pl_{test}"),
        slot: format!("pl_{test}"),
        publication: format!("pl_{test}_pub"),
    };
    drop_slot(admin, &n.slot).await;
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {pubn};
             DROP SCHEMA IF EXISTS {s} CASCADE;
             CREATE SCHEMA {s};
             SET search_path = {s};
             {ddl}
             RESET search_path;",
            pubn = n.publication,
            s = n.schema
        ))
        .await
        .expect("test DDL");
    n
}

/// A pipeline on the test server reading `include`, with `snapshots` settings (YAML flow
/// entries; the mode defaults to `never`). It flushes at every transaction.
pub fn pipeline(n: &Names, include: &[&str], snapshots: &str) -> Pipeline {
    pipeline_with(n, include, snapshots, "")
}

/// [`pipeline`] with more pipeline keys (YAML lines indented four spaces, each starting with a
/// newline).
pub fn pipeline_with(n: &Names, include: &[&str], snapshots: &str, more: &str) -> Pipeline {
    let include: Vec<String> = include
        .iter()
        .map(|i| format!("{}.{i}", n.schema))
        .collect();
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: p
    source:
      host: 127.0.0.1
      port: {port}
      db: avalanche
      user: avalanche
      password: avalanche
      ssl_mode: disable
      postgres: {{ publication: {publication}, slot: {slot}, heartbeat: 1s }}
    tables:
      include: [{include}]
      snapshots: {{ {snapshots} }}
    sink: {{ type: delta, bucket: lake, delta: {{}}, flush: {{ seconds: 0 }} }}{more}
"#,
        port = port(),
        publication = n.publication,
        slot = n.slot,
        include = include.join(", "),
    );
    load_str(
        &yaml,
        &BTreeMap::new(),
        &FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"))
    .config
    .pipelines
    .into_values()
    .next()
    .expect("one pipeline")
}

/// The sink and the state documents a pipeline keeps across its runs.
#[derive(Clone)]
pub struct Lake {
    pub sink: MemorySink,
    pub docs: PipelineDocs,
}

impl Lake {
    pub fn new() -> Self {
        let store =
            ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/p").expect("a root");
        Self {
            sink: MemorySink::new(),
            docs: PipelineDocs::new(DocStore::new(Arc::new(store))),
        }
    }

    /// The table's `(id, v)` rows in the sink, in key order.
    pub fn rows(&self, table: &TableRef) -> Vec<(i64, Option<String>)> {
        let Some(b) = self.sink.content(table) else {
            return vec![];
        };
        let ids = b
            .column_by_name("id")
            .expect("an id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("bigint ids");
        let vs = b
            .column_by_name("v")
            .expect("a v column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a text v");
        (0..b.num_rows())
            .map(|i| (ids.value(i), vs.is_valid(i).then(|| vs.value(i).to_owned())))
            .collect()
    }
}

/// The table's `(id, v)` rows in the source, in key order.
pub async fn source_rows(admin: &Client, table: &TableRef) -> Vec<(i64, Option<String>)> {
    admin
        .query(&format!("SELECT id, v FROM {table} ORDER BY id"), &[])
        .await
        .expect("read the source table")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Waits up to 60 s until the sink's `table` holds what the source's does. The stream reports a
/// server's position only at its own commits and at keep-alives, so a test waits for the data
/// itself, not for a WAL position.
pub async fn converge(admin: &Client, lake: &Lake, table: &TableRef) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let want = source_rows(admin, table).await;
        let have = lake.rows(table);
        if have == want {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            assert_eq!(have, want, "{table} doesn't converge");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Fast enough for a test: reconnects and checks within tenths of a second.
pub fn tuning() -> Tuning {
    let short = Backoff {
        first: Duration::from_millis(100),
        max: Duration::from_secs(1),
    };
    Tuning {
        source_backoff: short,
        sink_backoff: short,
        in_flight_backoff: short,
        tick: Duration::from_millis(100),
        ..Tuning::default()
    }
}

/// A running worker.
pub struct Running {
    pub handle: WorkerHandle,
    pub task: JoinHandle<Result<Exit, WorkerError>>,
}

/// Starts a worker of `p` over `lake`, with `hooks` when given.
pub fn start(p: &Pipeline, lake: &Lake, hooks: Option<Arc<dyn Hooks>>) -> Running {
    data_avalanche_testkit::install_crypto_provider();
    let source = PgConnector::new(p).expect("connector");
    let mut deps = WorkerDeps::new(
        Arc::new(source),
        Arc::new(lake.sink.clone()),
        lake.docs.clone(),
    );
    if let Some(h) = hooks {
        deps.hooks = h;
    }
    let (worker, handle) = PipelineWorker::new(p, deps, tuning());
    Running {
        handle,
        task: tokio::spawn(worker.run()),
    }
}

impl Running {
    /// Waits until the worker streams and no snapshot runs (a finished one is durable).
    pub async fn snapshotted(&self) {
        self.wait("the snapshots are done", |st| {
            matches!(st.phase, Phase::Streaming) && st.snapshots.is_empty()
        })
        .await;
    }

    /// Waits up to 60 s for the status to satisfy `f`.
    pub async fn wait(&self, what: &str, f: impl FnMut(&WorkerStatus) -> bool) {
        let mut st = self.handle.status.clone();
        let waited = tokio::time::timeout(Duration::from_secs(60), st.wait_for(f))
            .await
            .map(|r| r.is_ok());
        match waited {
            Ok(true) => {}
            Ok(false) => panic!("{what}: the worker ended: {:?}", *st.borrow()),
            Err(_) => panic!("{what}: timed out: {:?}", *st.borrow()),
        }
    }

    /// Drains the worker and returns how it ended.
    pub async fn drain(self) -> Result<Exit, WorkerError> {
        let _ = self.handle.control.send(Command::Drain).await;
        self.end().await
    }

    /// Waits for the worker to end on its own.
    pub async fn end(self) -> Result<Exit, WorkerError> {
        tokio::time::timeout(Duration::from_secs(60), self.task)
            .await
            .expect("the worker ends")
            .expect("the worker task doesn't panic")
    }
}

/// Drops the replication slot `slot`, once no walsender holds it.
pub async fn drop_slot(admin: &Client, slot: &str) {
    for i in 0..100 {
        let pid: Option<i32> = admin
            .query_one(
                "SELECT (SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot],
            )
            .await
            .expect("read the test slot")
            .get(0);
        let Some(pid) = pid else { break };
        if i == 50 {
            admin
                .execute("SELECT pg_terminate_backend($1)", &[&pid])
                .await
                .expect("terminate the walsender");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .expect("drop the test slot");
}

/// The test's slot, publication and schema, dropped at the end of a passing test.
pub async fn cleanup(admin: &Client, n: &Names) {
    drop_slot(admin, &n.slot).await;
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {}; DROP SCHEMA IF EXISTS {} CASCADE;",
            n.publication, n.schema
        ))
        .await
        .expect("drop the test schema");
}
```

Create `crates/data-avalanche-pipeline/tests/pg/main.rs`:

```rust
//! The pipeline worker against a live PostgreSQL (`test-infra/compose.yaml`): `postgres-18` on
//! every change, PostgreSQL 14–17 weekly (`DATA_AVALANCHE_PG_PORT`). The sink and the state
//! documents are in memory. Each test returns at once without `DATA_AVALANCHE_IT`.

mod common;
mod recovery;
mod schema;
mod snapshot;
mod stream;
```

Create `crates/data-avalanche-pipeline/tests/pg/recovery.rs`:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use data_avalanche_pipeline::{HookAction, HookPoint, Hooks, WorkerError};
use data_avalanche_testkit::require_it;

use crate::common::{Lake, admin, cleanup, converge, fresh, pipeline, start};

/// Counts the chunks emitted, and crashes at the `crash_at`th sink commit (0: never).
#[derive(Debug, Default)]
struct Probe {
    chunks: AtomicUsize,
    commits: AtomicUsize,
    crash_at: usize,
}

impl Hooks for Probe {
    fn at(&self, point: &HookPoint) -> HookAction {
        match point {
            HookPoint::ChunkEmitted { .. } => {
                self.chunks.fetch_add(1, Ordering::SeqCst);
            }
            HookPoint::SinkCommitted { .. } => {
                let n = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
                if n == self.crash_at {
                    return HookAction::Crash;
                }
            }
            _ => {}
        }
        HookAction::Continue
    }
}

#[tokio::test]
async fn a_restart_resumes_where_the_last_run_stopped() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "restart",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a');",
    )
    .await;
    let t = n.table("t");
    let p = pipeline(&n, &["t"], "mode: initial");
    let lake = Lake::new();
    let run = start(&p, &lake, None);
    run.snapshotted().await;
    a.batch_execute(&format!("INSERT INTO {t} VALUES (2, 'b')"))
        .await
        .unwrap();
    converge(&a, &lake, &t).await;
    run.drain().await.unwrap();

    a.batch_execute(&format!(
        "UPDATE {t} SET v = 'A' WHERE id = 1; INSERT INTO {t} VALUES (3, 'c');"
    ))
    .await
    .unwrap();
    let probe = Arc::new(Probe::default());
    let run = start(&p, &lake, Some(probe.clone()));
    converge(&a, &lake, &t).await;
    assert_eq!(probe.chunks.load(Ordering::SeqCst), 0, "no second snapshot");
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}

/// The third chunk's commit crashes the worker before its cursor is saved; the next run reads on
/// from the second chunk's cursor.
#[tokio::test]
async fn a_crash_mid_snapshot_resumes_from_the_saved_cursor() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "crash_snapshot",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t SELECT i, 'v' || i FROM generate_series(1, 1000) i;",
    )
    .await;
    let t = n.table("t");
    let p = pipeline(&n, &["t"], "mode: initial, chunk_size: 100");
    let lake = Lake::new();
    let crash = Arc::new(Probe {
        crash_at: 3,
        ..Probe::default()
    });
    let run = start(&p, &lake, Some(crash));
    let end = run.end().await;
    assert!(matches!(end, Err(WorkerError::Crashed(_))), "{end:?}");

    let probe = Arc::new(Probe::default());
    let run = start(&p, &lake, Some(probe.clone()));
    converge(&a, &lake, &t).await;
    let chunks = probe.chunks.load(Ordering::SeqCst);
    assert!(
        chunks <= 9,
        "{chunks} chunks: at least two of ten were already done"
    );
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
```

Create `crates/data-avalanche-pipeline/tests/pg/schema.rs`:

```rust
use arrow_array::{Array, Int32Array};
use data_avalanche_core::HaltReason;
use data_avalanche_testkit::require_it;

use crate::common::{Lake, admin, cleanup, converge, fresh, pipeline, pipeline_with, start};

#[tokio::test]
async fn an_added_column_reaches_the_sink() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "add_column",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a');",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(&pipeline(&n, &["t"], "mode: initial"), &lake, None);
    run.snapshotted().await;
    converge(&a, &lake, &t).await;
    a.batch_execute(&format!(
        "ALTER TABLE {t} ADD COLUMN w int; INSERT INTO {t} VALUES (2, 'b', 7);"
    ))
    .await
    .unwrap();
    converge(&a, &lake, &t).await;
    let b = lake.sink.content(&t).unwrap();
    let w = b
        .column_by_name("w")
        .expect("the added column")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int");
    assert!(w.is_null(0), "the row from before the change");
    assert_eq!(w.value(1), 7);
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}

/// Channel D finds the table gone; it halts once the stream has passed the rescan, after its
/// last change is in the sink.
#[tokio::test]
async fn a_dropped_table_halts_and_the_others_go_on() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "drop_table",
        "CREATE TABLE gone (id bigint PRIMARY KEY, v text);
         CREATE TABLE kept (id bigint PRIMARY KEY, v text);",
    )
    .await;
    let (gone, kept) = (n.table("gone"), n.table("kept"));
    let lake = Lake::new();
    let p = pipeline_with(
        &n,
        &["gone", "kept"],
        "",
        "\n    schema_evolution: { rescan_interval: 1s }",
    );
    let run = start(&p, &lake, None);
    run.snapshotted().await;
    for sql in [
        format!("INSERT INTO {gone} VALUES (1, 'x')"),
        format!("DROP TABLE {gone}"),
        format!("INSERT INTO {kept} VALUES (1, 'y')"),
    ] {
        a.batch_execute(&sql).await.unwrap();
    }
    run.wait("the dropped table halts", |st| {
        st.halted.get(&gone) == Some(&HaltReason::TableDropped)
    })
    .await;
    assert_eq!(lake.rows(&gone), [(1, Some("x".into()))], "its last change");
    a.batch_execute(&format!("INSERT INTO {kept} VALUES (2, 'z')"))
        .await
        .unwrap();
    converge(&a, &lake, &kept).await;
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
```

Create `crates/data-avalanche-pipeline/tests/pg/snapshot.rs`:

```rust
use data_avalanche_pipeline::Exit;
use data_avalanche_testkit::require_it;
use tokio_postgres::NoTls;

use crate::common::{Lake, admin, cleanup, converge, dsn, fresh, pipeline, source_rows, start};

/// Writers change the table while it is being snapshotted: updates, deletes and inserts in many
/// small transactions, over keys chunks have and haven't reached yet. A chunk's read races the
/// stream: a change it didn't see may already be applied when it is emitted (DA-SNAP-9).
#[tokio::test]
async fn a_snapshot_under_concurrent_writes_ends_equal_to_the_source() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "busy_snapshot",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t SELECT i, 'old' FROM generate_series(1, 40000) i;",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(
        &pipeline(&n, &["t"], "mode: initial, chunk_size: 2000"),
        &lake,
        None,
    );
    let mut writers = vec![];
    for w in 0..4u64 {
        let (c, conn) = tokio_postgres::connect(&dsn(), NoTls).await.unwrap();
        tokio::spawn(conn);
        let t = t.clone();
        writers.push(tokio::spawn(async move {
            // A fixed pseudo-random walk over the keys per writer, so a failure replays.
            let mut x: u64 = 0x2545_f491_4f6c_dd1d ^ (w + 1);
            for i in 0..400 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let id = (x % 42_000) as i64 + 1;
                let sql = match i % 3 {
                    0 => format!("UPDATE {t} SET v = 'new {w}.{i}' WHERE id = {id}"),
                    1 => format!("DELETE FROM {t} WHERE id = {id}"),
                    _ => format!(
                        "INSERT INTO {t} VALUES ({id}, 'ins {w}.{i}') \
                         ON CONFLICT (id) DO UPDATE SET v = 'ups {w}.{i}'"
                    ),
                };
                c.batch_execute(&sql).await.unwrap();
            }
        }));
    }
    for w in writers {
        w.await.unwrap();
    }
    converge(&a, &lake, &t).await;
    assert_eq!(run.drain().await.unwrap(), Exit::Drained);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_dump_initial_table_ends_the_worker_once_dumped() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "dump",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t SELECT i, 'v' || i FROM generate_series(1, 250) i;",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(
        &pipeline(&n, &["t"], "mode: dump_initial, chunk_size: 100"),
        &lake,
        None,
    );
    assert_eq!(run.end().await.unwrap(), Exit::Dumped);
    assert_eq!(lake.rows(&t), source_rows(&a, &t).await);
    cleanup(&a, &n).await;
}
```

Create `crates/data-avalanche-pipeline/tests/pg/stream.rs`:

```rust
use data_avalanche_pipeline::Exit;
use data_avalanche_testkit::require_it;

use crate::common::{Lake, admin, cleanup, converge, fresh, pipeline, start};

#[tokio::test]
async fn inserts_updates_deletes_and_key_changes_reach_the_sink() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "dml",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(&pipeline(&n, &["t"], "mode: initial"), &lake, None);
    converge(&a, &lake, &t).await;
    // One statement per call: a multi-statement simple query is a single transaction.
    for sql in [
        format!("INSERT INTO {t} VALUES (4, 'd')"),
        format!(
            "BEGIN; UPDATE {t} SET v = NULL WHERE id = 1; DELETE FROM {t} WHERE id = 3; COMMIT"
        ),
        format!("UPDATE {t} SET id = 20 WHERE id = 2"),
        format!(
            "BEGIN; INSERT INTO {t} VALUES (5, 'e'); UPDATE {t} SET v = 'E' WHERE id = 5; COMMIT"
        ),
        format!("BEGIN; INSERT INTO {t} VALUES (6, 'f'); DELETE FROM {t} WHERE id = 6; COMMIT"),
    ] {
        a.batch_execute(&sql).await.unwrap();
    }
    converge(&a, &lake, &t).await;
    assert_eq!(
        lake.rows(&t),
        [
            (1, None),
            (4, Some("d".into())),
            (5, Some("E".into())),
            (20, Some("b".into()))
        ]
    );
    assert_eq!(run.drain().await.unwrap(), Exit::Drained);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_truncate_empties_the_sink_table() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "truncate",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a'), (2, 'b');",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(&pipeline(&n, &["t"], "mode: initial"), &lake, None);
    converge(&a, &lake, &t).await;
    a.batch_execute(&format!(
        "BEGIN; TRUNCATE {t}; INSERT INTO {t} VALUES (3, 'c'); COMMIT"
    ))
    .await
    .unwrap();
    converge(&a, &lake, &t).await;
    assert_eq!(lake.rows(&t), [(3, Some("c".into()))]);
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
```

Create `crates/data-avalanche-pipeline/tests/sync_commit.rs`:

```rust
//! DA-SNAP-9 through the worker, on a live server: a synchronous commit waiting for its standby
//! is streamed and applied while the chunk that holds its row is read without it. The chunk's
//! stale row must not undo the change. Its own test binary, which cargo never runs beside the
//! `pg` tests: it sets the server-wide `synchronous_standby_names` to a standby that never
//! answers, which stalls every synchronous commit on the server.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "pg/common.rs"]
mod common;

use std::time::Duration;

use data_avalanche_testkit::require_it;
use tokio_postgres::NoTls;

use crate::common::{Lake, admin, cleanup, converge, dsn, fresh, pipeline, start};

type Fallible<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn a_stalled_synchronous_commit_survives_the_chunk_that_missed_it() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "sync_commit",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t SELECT i, 'old' FROM generate_series(1, 5000) i;",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    // The first chunks wait in the writer, so the snapshot is still reading when the stalled
    // commit is streamed.
    lake.sink.hold_commits();
    let run = start(
        &pipeline(&n, &["t"], "mode: initial, chunk_size: 10"),
        &lake,
        None,
    );
    run.wait("the snapshot starts", |st| !st.snapshots.is_empty())
        .await;

    // Every step is fallible, not panicking, so the setting is restored whatever happens; the
    // first reset undoes an earlier run that died before its own. ALTER SYSTEM can't run in a
    // multi-statement batch, hence one statement per call. Clearing the setting also releases a
    // writer still waiting.
    let standby = |names: Option<&'static str>| {
        let a = &a;
        async move {
            let set = match names {
                None => "ALTER SYSTEM RESET synchronous_standby_names".to_string(),
                Some(n) => format!("ALTER SYSTEM SET synchronous_standby_names = '{n}'"),
            };
            a.batch_execute(&set).await?;
            a.batch_execute("SELECT pg_reload_conf()").await
        }
    };
    standby(None).await.unwrap();
    let run_stalled: Fallible<_> = async {
        standby(Some("phantom_standby")).await?;
        let (w, conn) = tokio_postgres::connect(&dsn(), NoTls).await?;
        tokio::spawn(conn);
        let update =
            format!("SET synchronous_commit = on; UPDATE {t} SET v = 'new' WHERE id = 5000");
        let writer = tokio::spawn(async move { w.batch_execute(&update).await });
        let mut waiting = false;
        for _ in 0..100 {
            let n: i64 = a
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
                    &[],
                )
                .await?
                .get(0);
            if n > 0 {
                waiting = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if !waiting {
            return Err("no writer waiting for the standby".into());
        }
        lake.sink.release_commits();
        // The snapshot finishes while the commit is still invisible to every read.
        run.wait("the snapshot is done", |st| st.snapshots.is_empty())
            .await;
        let rows = lake.rows(&t);
        a.batch_execute(
            "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE wait_event = 'SyncRep'",
        )
        .await?;
        writer.await??;
        Ok(rows)
    }
    .await;
    standby(None).await.unwrap();
    let rows = run_stalled.unwrap();

    assert_eq!(rows.len(), 5000);
    assert_eq!(
        rows[4999],
        (5000, Some("new".into())),
        "the stream's change stays"
    );
    converge(&a, &lake, &t).await;
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
```

- [ ] **Step 2: Run the tests (they pass: they pin behaviour already built)**

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --test pg -- --test-threads=1
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --test sync_commit
```

Expected:

PASS:

```text
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 6.86s
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.10s
```

- [ ] **Step 3: Implement**

Change `.github/workflows/weekly.yml`:

```diff
diff --git a/.github/workflows/weekly.yml b/.github/workflows/weekly.yml
--- a/.github/workflows/weekly.yml
+++ b/.github/workflows/weekly.yml
@@ -1,7 +1,8 @@
 name: weekly
 
-# The PostgreSQL source against the older supported servers (P-8, Q-M5-7, Q-M5-23): PostgreSQL
-# 14–17, one job each, the `postgres-18` suite of ci.yml re-run through DATA_AVALANCHE_PG_PORT.
+# The PostgreSQL source and the pipeline worker against the older supported servers (P-8, Q-M5-7,
+# Q-M5-23): PostgreSQL 14–17, one job each, the `postgres-18` suites of ci.yml re-run through
+# DATA_AVALANCHE_PG_PORT.
 # Weekly (Mondays 03:41 UTC) and on manual dispatch. The TLS tests need `postgres-tls`
 # (PostgreSQL 18) and run in ci.yml only. No secrets.
 
@@ -35,6 +36,8 @@ jobs:
         run: docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "postgres-${{ matrix.version }}"
       - name: PostgreSQL source tests
         run: 'cargo test --locked -p data-avalanche-source-pg --test pg --test slot_invalidation -- --test-threads=1 --skip tls::'
+      - name: Pipeline worker tests
+        run: cargo test --locked -p data-avalanche-pipeline --test pg -- --test-threads=1
       - name: Container logs on failure
         if: failure()
         run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "postgres-${{ matrix.version }}"
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
DATA_AVALANCHE_PG_PORT=55414 DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --test pg -- --test-threads=1
```

Expected:

PASS:

```text
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 6.75s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "test(pipeline): the worker against a live PostgreSQL: DML, truncate, snapshots under writes, a dump, an added column, a dropped table, restarts, a stalled synchronous commit"
```


### Task 14: A chunk read that fails rescans its idle table

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/session/snapshot.rs`, `crates/data-avalanche-pipeline/tests/pg/schema.rs`

**Interfaces:**
- Consumes: Task 10's runner, Task 8's rescan.
- Produces: no new API. A `Transient` chunk failure flags a rescan, which `emit_ready` runs when
  none is pending.

The M5-carried item (M5 Task 10 review). PostgreSQL's chunk read fails `Transient`
"changed under the snapshot" when a cached column is gone or retyped. It is meant to be read
again once the stream's schema catches up, but an idle table's schema only catches up at the
rescan (60 min by default). A failed read now triggers that rescan.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/session/snapshot.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -864,4 +864,39 @@ mod tests {
             "the stale chunk, again, then the empty one"
         );
     }
+
+    /// The table changed before its snapshot read it, and no change of it comes through the
+    /// stream: the failed read rescans, and the snapshot goes on under the new schema.
+    #[tokio::test]
+    async fn a_chunk_read_that_meets_a_changed_idle_table_rescans_it() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        w.source.load(&t("a"), rows(1..=2));
+        w.request(&["a"]).await;
+        w.source.alter(
+            &t("a"),
+            TableSchema::new(
+                t("a"),
+                vec![
+                    ColumnSchema::new("id", V::Int64, "bigint").not_null(),
+                    ColumnSchema::new("doc", V::Text, "text"),
+                    ColumnSchema::new("note", V::Text, "text"),
+                ],
+                vec!["id".into()],
+            )
+            .unwrap(),
+        );
+        let (mut core, control, status) = w.core(&chunks_of(10), &["a"]);
+        core.settings.rescan_interval = None;
+        run(core, control, status, async |status| {
+            snapshot_done(&w, &status, "a").await;
+        })
+        .await;
+        assert_eq!(w.ids("a"), [1, 2]);
+        assert_eq!(
+            w.text("a", "note"),
+            [None, None],
+            "read under the new schema"
+        );
+    }
 }
```

Change `crates/data-avalanche-pipeline/tests/pg/schema.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/tests/pg/schema.rs b/crates/data-avalanche-pipeline/tests/pg/schema.rs
--- a/crates/data-avalanche-pipeline/tests/pg/schema.rs
+++ b/crates/data-avalanche-pipeline/tests/pg/schema.rs
@@ -81,3 +81,35 @@ async fn a_dropped_table_halts_and_the_others_go_on() {
     run.drain().await.unwrap();
     cleanup(&a, &n).await;
 }
+
+/// An `always` table loses a column while the worker is down, and stays idle: the restart's
+/// snapshot reads under the cached schema and fails, the rescan this triggers finds the change,
+/// and the snapshot goes on under the new schema (without it, only the 60-minute rescan would).
+#[tokio::test]
+async fn an_idle_table_changed_while_down_is_snapshotted_under_its_new_schema() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(
+        &a,
+        "idle_alter",
+        "CREATE TABLE t (id bigint PRIMARY KEY, v text, extra text);
+         INSERT INTO t VALUES (1, 'a', 'x');",
+    )
+    .await;
+    let t = n.table("t");
+    let p = pipeline(&n, &["t"], "mode: always");
+    let lake = Lake::new();
+    let run = start(&p, &lake, None);
+    run.snapshotted().await;
+    run.drain().await.unwrap();
+
+    a.batch_execute(&format!("ALTER TABLE {t} DROP COLUMN extra"))
+        .await
+        .unwrap();
+    let run = start(&p, &lake, None);
+    run.snapshotted().await;
+    eprintln!("{:?}", *run.handle.status.borrow());
+    assert_eq!(lake.rows(&t), [(1, Some("a".into()))]);
+    run.drain().await.unwrap();
+    cleanup(&a, &n).await;
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

FAIL:

```text
test result: FAILED. 122 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 5.01s

failures:
    session::snapshot::tests::a_chunk_read_that_meets_a_changed_idle_table_rescans_it
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -73,6 +73,9 @@ pub(super) struct Snapshots {
     docs: BTreeMap<TableRef, TableSnapshotDoc>,
     /// Completions handed to the writer, with the checkpoint of their flush.
     completing: Vec<(SourcePosition, TableRef)>,
+    /// A read failed transiently: the table may have changed under the snapshot with nothing in
+    /// the stream to say so (an idle table), so the session rescans (M5 Task 10 review).
+    rescan: bool,
 }
 
 impl std::fmt::Debug for Snapshots {
@@ -331,6 +334,12 @@ impl Session<'_> {
                 return Ok(Some(end));
             }
         }
+        if std::mem::take(&mut self.snaps.rescan)
+            && self.rescan.is_none()
+            && let Some(end) = self.rescan().await?
+        {
+            return Ok(Some(end));
+        }
         self.issue_reads();
         Ok(None)
     }
@@ -366,6 +375,7 @@ impl Session<'_> {
                 let failed = snap.retry.map_or(0, |(_, n)| n);
                 let delay = self.core.tuning.source_backoff.delay(failed);
                 snap.retry = Some((Instant::now() + delay, failed + 1));
+                self.snaps.rescan = true;
                 return Ok(None);
             }
             Err(e) => {
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --test pg schema::
```

Expected:

PASS:

```text
test result: ok. 123 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.55s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; finished in 1.29s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): a chunk read that fails transiently rescans, so an idle table changed under its snapshot catches up"
```


### Task 15: Config: `timeouts.snapshot` is at least 10s (V-118)

**Files:**
- Modify: `crates/data-avalanche-config/src/registry.rs`, `…/src/rules/domain.rs`,
  `…/tests/rules/domain.rs`

**Interfaces:**
- Consumes: the rule registry and the domain checks (M1).
- Produces: rule **V-118** (error, locus pipeline, input config): `source.timeouts.snapshot` ≥
  `10s`, checked on each pipeline and on `defaults` alone. `RULES` holds 113 rules.

Q-M6a-7 (answered 2026-10-03). A literal `0` would halt every snapshot at once, so the
shortest timeout is 10 s. Spec edit S-U adds the row to Configuration_Specification §9.1 and §13.3.
Like V-112's backoff check, the message hides a templated value.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/registry.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1009,7 +1009,7 @@ mod tests {
 
     #[test]
     fn has_every_spec_rule_once_in_order() {
-        assert_eq!(RULES.len(), 112);
+        assert_eq!(RULES.len(), 113);
         assert!(
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
```

Change `crates/data-avalanche-config/tests/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/domain.rs b/crates/data-avalanche-config/tests/rules/domain.rs
--- a/crates/data-avalanche-config/tests/rules/domain.rs
+++ b/crates/data-avalanche-config/tests/rules/domain.rs
@@ -314,6 +314,15 @@ pub fn cases() -> Vec<Case> {
         case("V-111", Silent, "capacity 1", || {
             pg_delta().set("/pipelines/p/sink/buffer_capacity_rows", json!(1))
         }),
+        case("V-118", Error, "snapshot timeout 5s", || {
+            pg_delta().set("/pipelines/p/source/timeouts/snapshot", json!("5s"))
+        }),
+        case("V-118", Error, "snapshot timeout 0s", || {
+            pg_delta().set("/pipelines/p/source/timeouts/snapshot", json!("0s"))
+        }),
+        case("V-118", Silent, "snapshot timeout 10s", || {
+            pg_delta().set("/pipelines/p/source/timeouts/snapshot", json!("10s"))
+        }),
         case("V-112", Error, "no attempts", || {
             pg_iceberg().set(
                 "/pipelines/p/sink/iceberg/catalog/retry/max_attempts",
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-config --no-fail-fast
```

Expected:

FAIL:

```text
test result: FAILED. 80 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 43 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.36s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

failures:
    registry::tests::has_every_spec_rule_once_in_order
    rule_cases
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -996,6 +996,14 @@ pub const RULES: &[RuleMeta] = &[
         r#"tables.include/exclude globs, when source.type = postgres"#,
         r#"Must be schema.table: one . between a non-empty schema part and a non-empty table part (globs allowed within each part)."#,
     ),
+    rule(
+        "V-118",
+        Error,
+        L::Pipeline,
+        Config,
+        r#"source.timeouts.snapshot"#,
+        r#"≥ 10s."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
```

Change `crates/data-avalanche-config/src/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/domain.rs b/crates/data-avalanche-config/src/rules/domain.rs
--- a/crates/data-avalanche-config/src/rules/domain.rs
+++ b/crates/data-avalanche-config/src/rules/domain.rs
@@ -187,6 +187,27 @@ fn globals(g: &RawGlobals, ctx: &mut Ctx) {
 /// (`cross_field: false`, which skips V-107 — a partition's `n`/`width` against its `transform` —
 /// and V-112's backoff-order half — `base_backoff` against `max_backoff`).
 fn pipeline_fields(at: &At, p: &RawPipeline, ctx: &mut Ctx, cross_field: bool) {
+    // V-118
+    if let Some(t) = p.source.as_ref().and_then(|s| s.timeouts.as_ref()) {
+        let snap_at = at.key("source").key("timeouts").key("snapshot");
+        if let Some((v, loc)) = val(ctx, &t.snapshot, &snap_at)
+            && v < Duration::from_secs(10)
+        {
+            let l = t.snapshot.as_ref().expect("val returned Some");
+            // Duration has no `Display`: redact by hand, like V-112.
+            let shown = if is_templated(l) {
+                format!("{:?} (resolved value hidden)", l.text())
+            } else {
+                crate::units::format_duration(v)
+            };
+            ctx.error(
+                "V-118",
+                &snap_at,
+                loc,
+                format!("timeouts.snapshot must be at least 10s, got {shown}"),
+            );
+        }
+    }
     if let Some(pg) = p.source.as_ref().and_then(|s| s.postgres.as_ref()) {
         // V-104
         let lag_at = at.key("source").key("postgres").key("max_wal_lag_bytes");
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-config
```

Expected:

PASS:

```text
test result: ok. 81 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s
test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
test result: ok. 43 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.37s
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(config): V-118, timeouts.snapshot is at least 10s"
```


### Task 16: A table that drifted while down is rescanned before its snapshot reads

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/session.rs`, `…/session/snapshot.rs`, `…/worker.rs`,
  `…/test_support.rs`, `…/tests/pg/schema.rs`

**Interfaces:**
- Consumes: Task 11's Phase 0 drift check, Task 8's rescan, Task 10's runner.
- Produces (crate): `Core::drifted: BTreeSet<TableRef>` (Phase 0: the tables whose catalog schema
  moved past the cached one on a resume); `Snapshots::hold(&BTreeSet<TableRef>)` and
  `release_unless(&BTreeSet<&TableRef>) -> Vec<TableRef>`.

D-M6a.11 (answered 2026-10-03: rescan before snapshotting). A held table is not read. The
session rescans at the next tick, and again at each tick while a round fails. A held table the
rescan doesn't name is released at once. One it names is released once the report has applied,
after the stream catches up. A halt or drop releases it too. Without this, PostgreSQL reads an
idle table that gained a column under the cached columns, and the column stays NULL in every
snapshotted row.

Measured while planning: with the hold disabled, the unit test reads three times (one under the
old schema), and the live test finds no `w` column.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -1,6 +1,6 @@
 //! Fixtures for the crate's unit tests.
 
-use std::collections::BTreeMap;
+use std::collections::{BTreeMap, BTreeSet};
 use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
 use std::sync::{Arc, Mutex};
 use std::time::{Duration, Instant};
@@ -402,6 +402,7 @@ impl World {
             tracked: tables.iter().map(|t| TableRef::new("public", *t)).collect(),
             dump_only: false,
             snapshot_started: BTreeMap::new(),
+            drifted: BTreeSet::new(),
         };
         (core, control, status_rx)
     }
```

Change `crates/data-avalanche-pipeline/src/worker.rs` (its tests):

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -1082,4 +1082,57 @@ mod tests {
         assert!(*seen.lock().unwrap());
         assert_eq!(w.ids("a"), [1]);
     }
+
+    /// A table that gained a column while the worker was down, snapshotted again under
+    /// `always`: its reads wait for a rescan to bring the schema up to date, so the snapshot
+    /// reads the new column (D-M6a.11), and no read happens under the old schema.
+    #[tokio::test]
+    async fn a_table_changed_while_down_is_rescanned_before_its_snapshot() {
+        let w = World::new(&["a"]);
+        w.source.load(&t("a"), rows(1..=1));
+        let p = mode("mode: always");
+        let (end, _) = run(&w, &p, async |status| streamed(&status, &w).await).await;
+        assert_eq!(end.unwrap(), Exit::Drained);
+        let noted = TableSchema::new(
+            t("a"),
+            vec![
+                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
+                ColumnSchema::new("doc", V::Text, "text"),
+                ColumnSchema::new("note", V::Text, "text"),
+            ],
+            vec!["id".into()],
+        )
+        .unwrap();
+        w.source.alter(&t("a"), noted);
+        let mut r = row(1, "x");
+        r.0.push((
+            "note".into(),
+            data_avalanche_core::ColumnValue::Text("n".into()),
+        ));
+        w.source.load(&t("a"), vec![r]);
+        let before = w.source.reads(&t("a"));
+        let (end, st) = run(&w, &p, async |status| {
+            wait_until("the snapshot is durable", || {
+                let st = status.borrow();
+                st.phase == Phase::Streaming
+                    && st.snapshots.is_empty()
+                    && w.text("a", "note") == [Some("n".to_owned())]
+            })
+            .await;
+        })
+        .await;
+        assert_eq!(end.unwrap(), Exit::Drained);
+        assert!(
+            st.warnings
+                .iter()
+                .any(|m| m.contains("changed while the pipeline was down")),
+            "{:?}",
+            st.warnings
+        );
+        assert_eq!(
+            w.source.reads(&t("a")) - before,
+            2,
+            "the chunk, then the empty one: none under the old schema"
+        );
+    }
 }
```

Change `crates/data-avalanche-pipeline/tests/pg/schema.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/tests/pg/schema.rs b/crates/data-avalanche-pipeline/tests/pg/schema.rs
--- a/crates/data-avalanche-pipeline/tests/pg/schema.rs
+++ b/crates/data-avalanche-pipeline/tests/pg/schema.rs
@@ -113,3 +113,42 @@ async fn an_idle_table_changed_while_down_is_snapshotted_under_its_new_schema()
     run.drain().await.unwrap();
     cleanup(&a, &n).await;
 }
+
+/// An `always` table gains a column while the worker is down, and stays idle. PostgreSQL's chunk
+/// read wouldn't fail (it checks the cached columns only), so without a rescan first the
+/// snapshot would read the old columns and leave the new one NULL. The restart rescans before
+/// it reads (D-M6a.11).
+#[tokio::test]
+async fn an_idle_table_that_gained_a_column_while_down_is_snapshotted_with_it() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(
+        &a,
+        "idle_add",
+        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
+         INSERT INTO t VALUES (1, 'a');",
+    )
+    .await;
+    let t = n.table("t");
+    let p = pipeline(&n, &["t"], "mode: always");
+    let lake = Lake::new();
+    let run = start(&p, &lake, None);
+    run.snapshotted().await;
+    run.drain().await.unwrap();
+
+    a.batch_execute(&format!("ALTER TABLE {t} ADD COLUMN w int DEFAULT 7"))
+        .await
+        .unwrap();
+    let run = start(&p, &lake, None);
+    run.snapshotted().await;
+    let b = lake.sink.content(&t).unwrap();
+    let w = b
+        .column_by_name("w")
+        .expect("the added column")
+        .as_any()
+        .downcast_ref::<Int32Array>()
+        .expect("int");
+    assert_eq!(w.value(0), 7, "the row read under the new schema");
+    run.drain().await.unwrap();
+    cleanup(&a, &n).await;
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p data-avalanche-pipeline --lib
```

Expected:

FAIL to compile (1 error); the first:

```text
error[E0560]: struct `Core` has no field named `drifted`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -51,6 +51,10 @@ pub(crate) struct Core {
     pub dump_only: bool,
     /// When each table's snapshot began in this run (`timeouts.snapshot`).
     pub snapshot_started: BTreeMap<TableRef, Instant>,
+    /// Tables whose catalog schema moved past the cached one while the pipeline was down (Phase
+    /// 0 on a resume). A snapshot of one reads only once a rescan has brought its schema up to
+    /// date (D-M6a.11).
+    pub drifted: BTreeSet<TableRef>,
 }
 
 impl Core {
@@ -161,6 +165,7 @@ impl<'c> Session<'c> {
             }
         }
         let (mut snaps, retired, warnings) = Snapshots::load(core, &schemas).await?;
+        snaps.hold(&core.drifted);
         for table in &retired {
             acc.halt_table(table);
             schemas.remove(table);
@@ -521,6 +526,16 @@ impl<'c> Session<'c> {
         let Ok(report) = self.core.source.rescan(&known).await else {
             return Ok(None);
         };
+        let named: BTreeSet<&TableRef> = report
+            .changed
+            .iter()
+            .map(TableSchema::table)
+            .chain(&report.dropped)
+            .chain(report.halted.iter().map(|(t, _)| t))
+            .collect();
+        for table in self.snaps.release_unless(&named) {
+            self.core.drifted.remove(&table);
+        }
         if report == RescanReport::default() {
             return Ok(None);
         }
@@ -578,6 +593,9 @@ impl<'c> Session<'c> {
         }
         // Caches the applied schemas without waiting for the tables' next change.
         self.flush()?;
+        for table in self.snaps.release_unless(&BTreeSet::new()) {
+            self.core.drifted.remove(&table);
+        }
         Ok(None)
     }
 
```

Change `crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -76,6 +76,9 @@ pub(super) struct Snapshots {
     /// A read failed transiently: the table may have changed under the snapshot with nothing in
     /// the stream to say so (an idle table), so the session rescans (M5 Task 10 review).
     rescan: bool,
+    /// Tables whose schema drifted while the pipeline was down: not read until a rescan has
+    /// brought their schema up to date (D-M6a.11).
+    held: BTreeSet<TableRef>,
 }
 
 impl std::fmt::Debug for Snapshots {
@@ -206,7 +209,26 @@ impl Snapshots {
     }
 
     /// The table halted: its snapshot stops.
+    /// Holds the reads of those of `tables` being snapshotted.
+    pub(super) fn hold(&mut self, tables: &BTreeSet<TableRef>) {
+        self.held = tables
+            .iter()
+            .filter(|t| self.tables.contains_key(*t))
+            .cloned()
+            .collect();
+    }
+
+    /// Releases every held table not in `keep`; returns them.
+    pub(super) fn release_unless(&mut self, keep: &BTreeSet<&TableRef>) -> Vec<TableRef> {
+        let (kept, released) = std::mem::take(&mut self.held)
+            .into_iter()
+            .partition(|t| keep.contains(t));
+        self.held = kept;
+        released.into_iter().collect()
+    }
+
     pub(super) fn stop(&mut self, table: &TableRef) {
+        self.held.remove(table);
         self.tables.remove(table);
         self.docs.remove(table);
         if self.tables.is_empty() {
@@ -283,7 +305,11 @@ impl Session<'_> {
             if reading >= self.core.settings.parallelism {
                 break;
             }
-            if snap.reading || snap.done || snap.retry.is_some_and(|(at, _)| at > now) {
+            if snap.reading
+                || snap.done
+                || snap.retry.is_some_and(|(at, _)| at > now)
+                || self.snaps.held.contains(table)
+            {
                 continue;
             }
             let Some(schema) = self.schemas.get(table).cloned() else {
@@ -485,6 +511,13 @@ impl Session<'_> {
 
     /// On the tick: the snapshot timeout, and the in-flight transactions read again when due.
     pub(super) async fn snapshot_tick(&mut self) -> Result<Option<End>, WorkerError> {
+        // A held table waits for a rescan; a failed one is tried again at the next tick.
+        if !self.snaps.held.is_empty()
+            && self.rescan.is_none()
+            && let Some(end) = self.rescan().await?
+        {
+            return Ok(Some(end));
+        }
         let now = Instant::now();
         let limit = self.core.settings.snapshot_timeout;
         let late: Vec<TableRef> = self
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -99,6 +99,7 @@ impl PipelineWorker {
             tracked: BTreeSet::new(),
             dump_only: false,
             snapshot_started: BTreeMap::new(),
+            drifted: BTreeSet::new(),
         };
         (Self { core }, WorkerHandle { control, status })
     }
@@ -388,6 +389,7 @@ pub(crate) async fn start(
         .chain(halts.iter().map(|(t, _)| t.clone()))
         .collect();
     let mut tracked = BTreeSet::new();
+    let mut drifted = BTreeSet::new();
     let mut requests = vec![];
     let mut abandoned = vec![];
     let mut schemas = vec![];
@@ -467,6 +469,7 @@ pub(crate) async fn start(
                 }
             }
             Some(c) if matches!(plan, Plan::Resume) && c.columns() != now.columns() => {
+                drifted.insert(table.clone());
                 warnings.push(format!(
                     "table {table}: its schema changed while the pipeline was down; the stream replays the change in order"
                 ));
@@ -543,6 +546,7 @@ pub(crate) async fn start(
         core.docs.save_table_schema(schema).await?;
     }
     core.tracked = tracked;
+    core.drifted = drifted;
     core.dump_only = dump_only;
     core.update(|st| st.warnings.extend(warnings));
     Ok(())
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cargo test -p data-avalanche-pipeline --lib
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-pipeline --test pg schema::
```

Expected:

PASS:

```text
test result: ok. 124 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.55s
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out; finished in 1.34s
```

Then the quality gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(pipeline): a table whose schema drifted while the pipeline was down is rescanned before its snapshot reads"
```


## Verified during planning (2026-10-03)

The task blocks above were replayed in order on a fresh clone of the milestone branch
(`origin/main` `7573650`). The replay applied each task's files exactly as shown and ran each step's
command. Every expected failure and every count above is that replay's output. Step 4 of Task 13
ran against PostgreSQL 14. The replayed tree equals the verified workspace, `Cargo.lock` included.
Measured along the way:

- **Each task commit on its own** (Tasks 1–14) passes `cargo fmt --all --check`, `cargo clippy
  --workspace --all-targets -- -D warnings` and `cargo test --workspace` (hermetic); Tasks 15 and
  16 pass the replay's gates. After Task 16, `cargo test --workspace` passes 779 tests (666 after
  M5).
- **The emulator run** (`DATA_AVALANCHE_IT=1`, `DATA_AVALANCHE_SKIP_GCS=1`, `--test-threads=1`, the
  TLS tests skipped: the scratch clone has no `test-infra/.tls`) passes on every binary of the
  workspace, the pipeline's `tests/pg` (8) and `tests/sync_commit` included. Tasks 14 and 16 add
  a ninth and a tenth `tests/pg` test; `tests/pg` (10) then passed three runs in a row on
  PostgreSQL 18.
- **PostgreSQL 14** (`postgres-14`): `tests/pg` and `tests/sync_commit` pass.
- **Stability:** the pipeline's unit tests passed eight runs in a row; `tests/pg` passed eight
  runs in a row on PostgreSQL 18.
- **Mutations** (each reverted): see Tasks 12, 13 and 14. Without the snapshot resume in `decide`,
  the crash test fails. Without the reconciler's filtering, the busy-snapshot test fails (3 of 3)
  and `tests/sync_commit` fails (2 of 2). Under the window rule `tests/sync_commit` fails (2 of 2).
  Without Task 14's rescan flag, its live test times out with the snapshot at `rows_done: 0`.
  Without Task 16's hold, its unit test reads three times and its live test finds no `w`
  column.
  *Superseded during execution* (see Deviations, Batches C and F): after Task 16 the Task 14 live
  test no longer reached Task 14's path (the drifted table is held and rescanned before any read);
  it now drops the column mid-snapshot, and without the flag it times out with `rows_done` stuck
  at 1. `tests/sync_commit` doesn't discriminate the durability counting added in Batch C (unit
  tests do); it still fails under the window rule.
- **Not run while planning:** the CI workflows themselves. `weekly.yml` first runs `tests/pg` on
  PostgreSQL 15–17 after the merge.

## M6a exit checklist

M6a's own exit (M6's exit, the slice-1 E2E suite, is M6b's):

- [x] **The worker as a library** (roadmap §4 M6 scope, the parts that don't need the control
  plane):
  - Phase 0: state load and the recorded `sink_type`, resolution, preflight, the D-V1.4
    Iceberg refusal, the per-table snapshot decision (Tasks 5, 11);
  - snapshots: stream-first chunk loop, reconciliation, cursor per chunk, `parallelism`, the
    per-table timeout and its V-118 floor, dump modes, a rescan before reading a table that
    drifted while down (Tasks 9, 10, 14, 15, 16);
  - streaming, the sink writer, acknowledgment after the durable checkpoint (D-IMPL-2) (Tasks
    6, 7);
  - the schema apply protocol §8, DDL replay included (Tasks 7, 11);
  - the per-table halt registry (Tasks 3, 6, 7);
  - error paths: source reconnect, sink retry → DEGRADED, `Fenced` → stop (Tasks 6, 11, 12).
- [x] **Carried from M5**, each closed:
  - the start position persisted with `prepare`, and the slot intent first (Tasks 3, 11);
  - the checkpoint persisted before it is acknowledged (Tasks 6, 7);
  - `Prepared::added` snapshotted under `Resume`, the requests persisted before the checkpoint,
    and a table added by config untracked until cached (Task 11, D-M6a.8);
  - `55006` retried within `contended_window` (Tasks 2, 11);
  - a stream opened only from a persisted checkpoint or the slot's own start (Tasks 7, 11);
  - a chunk's "changed under the snapshot" on an idle table → a rescan (Task 14);
  - the reconnect backoff (Task 11); the `max_wal_lag_bytes` halt (Tasks 7, 11);
  - keep-alives cost one position update, nothing more (Task 7);
  - `toast_strategy: re_query` (Task 8);
  - each cached `TableSchema` persisted with its source id (Tasks 3, 11);
  - DA-SNAP-9 (Tasks 1, 2, 9, 10, 13; `Snapshot_Resumability` §1 amended);
  - a failing `close` is tolerated (Task 7). `teardown` is M9's (pipeline deletion);
  - the watchdog: ruled out (D-M6a.9);
  - REPLICA IDENTITY FULL signals: a signal whose classification finds no Safe change neither
    flushes nor evolves (Task 7);
  - DataFusion stays out: the pipeline doesn't depend on `sink-delta` (Global Constraints).
- [x] **Moved to M6b:** the lease, renewer and `PipelineController` (the `CommitGuard` seam is
  here); the lifecycle's persisted desired state (here: `Command::Drain`); the binary, HTTP and
  metrics; "Carried from M4"; the slice-1 E2E suite on RustFS.
- [x] `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` and `RUSTDOCFLAGS="-D warnings" cargo doc -p data-avalanche-pipeline
  --no-deps` are clean.
- [ ] The integration job of PR CI is green (`tests/pg`, `tests/sync_commit` and
  `tests/sync_drop` included); `weekly.yml` runs `tests/pg`, `tests/sync_commit` and
  `tests/sync_drop` on PostgreSQL 14–17 after the merge.
- [x] The documents and spec edits listed under Decisions are applied; `doc/v1.0/README.md`
  lists this plan and its status.

## Deviations during execution

Executed 2026-10-03…07 on `feat/claude/m6a-pipeline-worker`. The 16 tasks were applied as written
(each task commit's tree was compared with the verified workspace), except for the fixes below. The
task reviews found defects in the planned code; the owner ruled on each, and most landed as the
post-Task-16 fix batches A–F, one fix commit each plus test follow-ups. Counts in the task blocks
are therefore behind: after the last batch the pipeline's unit tests number 158 and the hermetic
workspace 817 (173 and 832 after the whole-branch review); `tests/pg` has 10 tests, `tests/sync_commit` 1 and the new `tests/sync_drop` 1.

**Task 1: `Contended` isn't transient** (prose only). The Task 1 interface list said
`is_transient()` is true for `SourceError::Contended`; the code and its test say false, and the
worker retries it within `contended_window` (D-M6a.9). The list is corrected in place.

**Task 4: the fake follows pgoutput** (`f67af56`, owner-approved). A change carries its commit
record's start and the `Commit` the record's end, 8 bytes on; a plain UPDATE streams no
before-image (REPLICA IDENTITY DEFAULT).

**Task 5: a snapshot in progress starts over after a gap** (`355c554`, D-M6a.8). `decide`
resumed an in-progress snapshot under a lost position or an adopted orphan; rows before its cursor
may have changed in the gap. Now `resume` is `in_progress && !recovery`.

**Task 6: the writer checks the lease before every state write** (`6a83cb5`). A state write retried
through a storage outage could land after the lease was lost and move the slot document behind
the new owner's checkpoint. Also: Task 6's interface line names `mpsc::Receiver<Job>`; the code
(and the later tasks' blocks) use unbounded channels.

**Task 13: `sync_commit` restores the server after a panic** (`25aa152`). Its reset ran only on
the error path; a panic left `synchronous_standby_names` set, which survives restarts. The restart
test can't pass vacuously any more, and `weekly.yml` runs `sync_commit` too.

**Task 15: V-118 on `defaults` alone** (`2d3cb16`). The planned cases set only the pipeline path;
a case where `defaults` is short and the pipeline overrides it now pins the `defaults` pass, and
`9999ms` pins the sub-second bound.

**Batch A — schema signals** (`03de15f`, `fe98335`; Task 7 review, owner rulings 2026-10-03).
- The control branch is polled only while not draining: with the handle dropped, `recv()` returned
  `Ready(None)` on every poll and starved the stream mid-transaction (a livelock).
- §8.1 order: a signal classifies first. A no-change or Ignorable signal neither flushes nor
  evolves (`TableBuffer::set_schema` keeps a same-shape schema without a flush). A Safe change
  flushes, waits for durability, then evolves. An Unsafe change, or a Safe one with evolution off,
  flushes the table's rows before the change, waits, then halts at that flush's checkpoint (the
  planned code dropped those rows). Spec `Schema_Change_Detection_and_Evolution` §8.1 amended to
  match (`3ec48ab`).
- A crash between the pre-halt flush and the halt's record replays from `at` (tested).

**Batch B — re_query, channel D, rescan failures** (`2a86eb1`, `b4ac9c0`, `06c153a`; Task 8 review,
owner rulings 2026-10-03 and 06).
- A `re_query` fetch that fails (neither `TableHalt` nor `Permanent`) is tried three times with
  backoff, then the columns stay masked (carry_forward's handling) and the status warns once per
  table streak; while the streak lasts each commit tries once, without backoff. The planned code
  ended the session, which looped for the whole pipeline after a `DROP COLUMN`. Data-Flow §TOAST
  amended (`3ec48ab`).
- Channel D keeps each table's cached schema as it was at the rescan; at catch-up it re-reads the
  table and applies the finding only if it is unchanged and the cache hasn't moved, else it skips
  the table for the next round. A failed re-read ends that catch-up.
- A failed rescan round warns once per streak; the rescan timer uses `MissedTickBehavior::Delay`.
- `status::Streak` is the shared once-per-streak helper. The FakeSource scopes a `Columns` hint
  like PostgreSQL's `scope` (types from the hint, nullability from the catalog; an unknown column
  or a hint without the key halts the table).

**Batch C — durability counts jobs** (`97b1331`, `a2f3366`; Task 10 review, owner ruling
2026-10-03; Task 13 review).
- Consecutive chunk jobs can share a checkpoint, so durability by position fired one job early:
  `until_durable` returned with chunk MERGEs still queued (an evolve could land under an old-shape
  commit) and a snapshot was reported done with rows missing (631 of 5000, measured). The session
  now numbers the jobs it hands the writer; the writer emits exactly one `Durable` per job, in
  order; `until_durable` and a snapshot's completion wait by number.
- A writer halt stops the table's snapshot (a dump worker ends `Dumped`, the halt stays
  `SINK_REJECTED`); every halt drops the table from `status.snapshots`.
- `sync_commit` runs with `buffer_capacity_rows: 10` and asserts its stall holds; it doesn't
  discriminate the job counting live (measured: 3 of 3 passes with the counting reverted), so unit
  tests guard that.

**Batch D — Phase 0 and the WAL-lag guard** (`cb63e19`, `6252647`; Task 11 review, owner rulings
2026-10-03 and 07).
- On a Fresh start Phase 0 saves the schema caches before the slot document (a crash between left
  `Resume` with a stale cache).
- Phase 0's rescan of gone tables warns once per failure streak; a start with nothing gone ends
  the streak.
- `retained_bytes`: D-M6a.10 as amended above.
- The writer keeps no snapshot document of a table it refused (a refused dump table ended with a
  `Completed` document).

**Batch E — spawned chunk reads** (`65bb699`, `e015e2e`; Task 13 review I3, Task 14 review).
- Chunk reads shared the catalog client with queries the session awaits inline (the WAL-lag read,
  the in-flight recheck, rescans). tokio-postgres holds one response message per request, so an
  inline query behind a half-read chunk deadlocked until `timeouts.command` (30 s; 2 of 11 busy
  runs). Each read now runs as its own task in a `JoinSet`, aborted on drop.
- The busy test proves its writers overlap the snapshot; the dropped-table test polls the sink for
  durability; Task 14's flag is no longer taken while a rescan is pending; the drift warning holds
  with periodic rescans off.

**Batch F — the drift hold** (`59dc20b`, `b0cffb9`; Task 16 review, owner ruling 2026-10-07).
- Task 16 had voided Task 14's live test (see the verification note above); it now drops the
  column mid-snapshot.
- The release paths are tested with a FakeSource stream-hold knob. A held table the catch-up skips
  stays held; an unnamed one waits until its stream-applied schema change is with the writer; a
  held table's pending schema flushes at the rescan; a failing round backs off and warns once per
  streak, and the streak ends with the hold. D-M6a.11 is amended above.
- SQLSTATE 42703 from a chunk read (a `DROP COLUMN` locked before the `SELECT`, visible only after
  the read's catalog check) ended the whole worker as `Permanent`; source-pg now maps it to the
  transient "changed under the snapshot". `tests/sync_drop` reproduces the window with a phantom
  synchronous standby; it runs in PR CI and weekly. 42883/42804 (a retyped key) stay `Permanent`:
  no test showed them.

**Whole-branch review** (2026-10-07; `4d297d4`, `b078e0d`; owner rulings 2026-10-07). The review
found 1 Critical, 3 Important and 14 Minor issues; its scoped re-review approved the fixes and
found residuals, fixed in a second round.
- **C1:** `Core::snapshot_started` was never reset, so a re-snapshot after a lost position halted
  `SNAPSHOT_TIMEOUT` at the first tick in any run older than `timeouts.snapshot`. It is reset when
  a table's snapshot is requested from scratch.
- **I1:** the biased `select!` put the stream above chunk results, the tick, the WAL-lag heartbeat
  and the rescan; a stream that stays ready starved them. The stream arm is now after them.
- **I2:** `status.snapshots` kept a table whose completion became durable during `finish()`; `open`
  now replaces the snapshot status, and `finish()` handles `Durable` and `Halted` for the status.
- **I3:** a table waiting for in-flight writers, or held across rescan rounds, warns once per wait,
  naming the blocking xids; the status reports the reconciler log's size. Bounding the log is
  DA-SNAP-11.
- **M1 (owner ruling):** every halt flushes the table's committed rows first and rides the next job
  with `at` = that checkpoint (stream and `re_query` `TableHalt`s, the accumulator's data halts);
  the accumulator keeps a halted table's rows until the halt takes them, and the writer keeps its
  own `SINK_REJECTED` over a later halt.
- **Minors fixed:** a failing chunk read warns once per streak; a misfit cursor resets `rows_done`;
  Phase 0 records a snapshot request only for a table that survived its checks; sleep-based test
  assertions replaced; a stream `TableHalt` of a table already halted no longer overwrites its
  halt (a table dropped and re-created moved `at` past rows the sink lacked).
- **M7 (owner ruling):** Data-Flow Phase 0 step 4 amended (`2e33d4e`): a table the sink can't
  represent halts that table.
- **Recorded** (gap tracker §28.2): DA-SNAP-11 (the reconciler log's bound), DA-SNAP-12
  (`parallelism > 1` on one PostgreSQL connection), DA-SNAP-13 (a wiped state baselines nothing),
  DA-SRC-19 (which permanent read errors are table-scoped, M13). Carried to M6b as notes: warnings
  that grow and repeat across reruns and reconnects, `finish()` not acknowledging the durable
  checkpoint, `until_durable` not polling control, and a Delta test of a snapshot set at a covered
  frontier (the slice-1 E2E).

After the review: 832 workspace tests; `tests/pg` (10), `tests/sync_commit` and `tests/sync_drop`
pass on PostgreSQL 18 and 14.
