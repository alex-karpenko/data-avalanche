---
updated: 2026-11-11
domain: Architecture
tags: [v1, implementation-plan, m12, m12d, hardening, source, sink, mysql, postgres, delta, failover, slot-sync, schema-evolution, reposition]
resolves: [DA-SINK-12, DA-SRC-35, DA-SRC-25, DA-SRC-36, DA-SNAP-22, DA-SRC-32, DA-SRC-34, DA-SRC-33, DA-SRC-31]
---

# M12d — Source and Sink Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** the third of M12's four parts (Q-M12-8): the source and sink follow-ups the M12a
whole-branch review deferred (tracker §54.1), closed or ruled out of v1. That covers:
- column names that differ by case alone, which a Delta table can't hold (DA-SINK-12);
- MySQL column names in the config, matched without case as MySQL matches them (DA-SRC-35), and
  a column whose spelling alone changed, kept under its cached name (DA-SRC-25);
- a MySQL table the user can no longer see, told apart from a dropped one where the server allows
  (DA-SRC-36);
- a sink table a new pipeline state meets, baselined; a non-null sink column the source lacks,
  halted; a `publish_via_partition_root` repair that copies only what it hid (DA-SNAP-22);
- a source change that repositions every table the lake holds, and halts the one whose
  reposition the sink refuses for good (DA-SRC-32);
- a host that alternates between servers, halted after three runs in a row (DA-SRC-34);
- a PostgreSQL promotion that the pipeline's failover slot followed, continued without a
  re-snapshot (DA-SRC-33);
- logical decoding on a standby, deferred past v1 with what it needs (DA-SRC-31, Q-M12d-1).

**Architecture:** most fixes are local to one source, the Delta sink or one Phase 0 step. Three
change a contract between crates:

- **The sink lists what it holds** (Task 6). `Sink::held_tables` lists the tables under the
  sink's prefix, and `SinkTableState::identifier` gives the key a table was made with. A source
  change repositions the union of the resolved, the cached and the held tables, each under its
  own recorded key. A table the sink refuses to reposition halts `SINK_REJECTED` instead of
  ending every start.
- **The controller counts server changes** (Task 7). `WorkerStatus::started_from` is the
  checkpoint a worker's first stream session started from. Three runs in a row that end in
  `SourceError::SourceChanged`, none with a durable checkpoint past that, put the pipeline in
  `ERROR` with the change's code.
- **A start tells the source the lake's highest marker** (Task 8). `Preflight::server` names the
  server reached. When it isn't the checkpoint's, the worker reads the highest position the lake's
  markers name and calls `SourceConnector::prepare_marked`. The PostgreSQL source continues the
  stream on a promoted server when its slot is a failover slot synchronized past that marker
  (`continues`, Q-M12d-5). Every other source keeps `prepare`, through the trait's default.

**Tech Stack:** Rust 1.98.1 (pinned), tokio, `tokio-postgres` (catalog), `mysql_async =0.37.1`,
deltalake 1.0 (`buoyant_kernel` 0.28.1), `object_store` (`list_with_delimiter`), PostgreSQL 17+
slot synchronization (`sync_replication_slots`, `synchronized_standby_slots`).

**Spec:** `doc/specs/Data-Flow.md` (Phase 0, Phase 2a's server identity, Phase 2b's dump checks,
Phase 4's reposition), `doc/specs/Schema_Change_Detection_and_Evolution.md` §7.2,
`doc/specs/Configuration_Specification.md` §9.2.1 (`slot_management`, `source_change_action`),
§10 (`column_type_overrides`) and §13 (V-rules). The research the tasks argue from:
`research/Postgres_Slot_and_Publication_Lifecycle.md`, `research/MySQL_DDL_Classifier.md`,
`research/Delivery_Guarantee_and_Position_Guard.md`, `research/Snapshot_Resumability_and_Consistency.md`.
The tracker rows: §54.1 of `analysis/CDC_Design_Gap_Analysis_v1.md`.

**Spec patches:** five places where this plan goes past what the specs say. They are drafted in
§Spec patches below and applied in Task 9. **Oleksii approved the plan and the patches, D-M12d.10
included, on 2026-11-06.**

## Global Constraints

- Toolchain 1.98.1 (`rust-toolchain.toml`); `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` pass before
  every commit. `unsafe_code = "forbid"`; libraries return `thiserror` errors; every new public
  type or function is re-exported at its crate root.
- Plain `cargo test` stays hermetic. A test that needs a server or an emulator starts with
  `require_it!()` (`DATA_AVALANCHE_IT`); the failover-slot scenario also needs
  `DATA_AVALANCHE_PG_SYNC_REPLICA=55422:55423` and skips without it.
- No state document changes shape: v0.1.0 readers must read what M12d writes. `HaltReason` keeps
  its variants (`TableDropped` stays a unit variant; Task 4). `SlotDoc` and `PositionDoc` keep
  their fields; a continued start writes the same `SlotDoc` with another `server`.
- The halt codes are the existing ones: `SINK_REJECTED`, `EVOLUTION_UNSUPPORTED`,
  `SCHEMA_MISMATCH`, `TABLE_DROPPED`, `WAL_SOURCE_CHANGED`, `BINLOG_SOURCE_CHANGED`. No new code.
- Delta compares column names after Unicode lower-casing (`str::to_lowercase`, as
  `buoyant_kernel` 0.28.1's `StructType::try_new` does), not ASCII's.
- MySQL compares column names without case, always (not by `lower_case_table_names`, which
  governs table names only).
- `SOURCE_CHANGES_IN_A_ROW = 3` (Q-M12-8, DA-SRC-34).
- A managed PostgreSQL slot is a failover slot on PostgreSQL 17+ (`server_version_num >= 170000`):
  `pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)`; the two-argument form
  before 17.
- Commit trailer, as every commit on the branch: `Claude-Session:` line from the session.

## Review Focus

1. **A promotion the slot didn't follow.** A standby without `sync_replication_slots`, a slot
   created before 17, a slot invalidated by the promotion, a marker past the synchronized
   position: the start must take the source-change path as before (halt under `error`,
   re-snapshot under `re_snapshot`), never stream from a slot behind the lake. Task 8's
   `a_promotion_the_slot_followed_continues_the_stream` pins each condition; the M12a failover
   E2E on the plain pair pins the whole path.
2. **A reposition the sink refuses.** A held table whose `table_state` reads but whose commit is
   refused for good (a foreign-format table, a key it can't take) must halt that one table and let
   the start go on. Task 6's reposition loop maps `SinkError::Permanent` to a `TableHalt`, and a
   table that records no key is skipped with a warning.
3. **A case change on MySQL that isn't one.** `CHANGE a A`, followed by `DROP b`, `CHANGE c b` in
   the same delta: `keep_names` follows the diff's matching, never a name lookup. Task 3's
   `a_rename_onto_a_dropped_name_keeps_its_new_spelling` pins it.
4. **A pipeline that recovers once.** One failover is one `SourceChanged` rerun followed by a run
   that streams: the count must start again, never halt a pipeline that failed over twice in a
   day. Task 7's `a_durable_checkpoint_starts_the_count_of_server_changes_again`.
5. **The held-table listing on an object store.** The listing must work on S3, GCS and Azure
   prefixes, not only a local root: `list_with_delimiter` on the store `DeltaTableBuilder` builds
   for the sink root. The M12a failover E2E (RustFS) runs Task 6's path; the local test pins the
   escaping.

---

## Decisions and rulings

**Rulings asked of Oleksii (2026-11-06):**

- **Q-M12d-1, DA-SRC-31 (logical decoding on a standby): deferred past v1** (⏭). The source reads a
  primary. Recorded in the tracker with what v2 needs: the standby's `hot_standby_feedback`, a
  slot made on the standby (PostgreSQL 16+), `pg_log_standby_snapshot()` on the primary for a
  slot's first consistent point, and the timeline handling of DA-SRC-33.
- **Q-M12d-2, DA-SRC-33: accept a synchronized slot, and create failover slots.** A managed slot
  is created with `failover` on PostgreSQL 17+. test-infra gains a slot-sync pair. Spec patch P-1.
- **Q-M12d-3, DA-SRC-25: build the source-aware column compare.** `SourceConnector::column_names_fold`
  (MySQL: true) drives the drift compare; MySQL keeps a column's cached spelling across a
  case-only change.
- **Q-M12d-4, DA-SRC-36: keep `TABLE_DROPPED`, and probe.** A table that's gone from the catalog
  is probed with `SELECT 1 FROM t LIMIT 0`. Error 1146 means it was dropped. Error 1142 means it
  was dropped or the user lost `SELECT`, and the server hides which, so the halt says both. The
  halt doesn't change shape: the probe's note is a status warning.
- **Q-M12d-5, DA-SRC-33's switch point: "slot position only".** SQL only, no `TIMELINE_HISTORY`
  client. The stream continues across a new timeline when four things hold:
  - the promoted server has the same system identifier, on a later timeline;
  - its slot is `failover` and `synced`, and not invalidated;
  - its `confirmed_flush` is at least the lake's highest marker;
  - its `confirmed_flush` is at least the checkpoint.

  (Tightened in execution, Task 8: `confirmed_flush` equal to the checkpoint, and no marker past
  it. See "Deviations during execution".)

  Otherwise the source-change path runs as before. It is sound because a standby synchronizes a
  slot only to a position its own WAL holds, which is at or before the switch point (measured,
  PostgreSQL 18).

**Decisions made in planning:**

- **D-M12d.1 — one case-folding rule for Delta.** `core::differ_by_case_alone(a, b)` (Unicode
  `to_lowercase`) is used at create (Permanent → `SINK_REJECTED`), at evolve (`Unsupported` →
  `EVOLUTION_UNSUPPORTED`, naming both spellings) and in the worker's `missing_columns`.
  DA-SINK-12 is wider than the tracker said: a PostgreSQL table with both `note` and `"Note"`
  loops at create too.
- **D-M12d.2 — V-127, a warning.** A `column_type_overrides` key whose column names no column of
  its table warns at schema resolution (both sources). On MySQL the key matches without case,
  and `tables.primary_key` entries map to the catalog's spelling. It is a new rule, not a change
  to V-078/V-087: those check the value.
- **D-M12d.3 — `keep_names` follows the diff.** MySQL's `keep_names(cached, new, renames)`
  rebuilds the new schema under the cached spellings through the diff's own column matching (by
  position and `source_id`), never by looking a name up. A change of case alone is
  `NoTargetEffect`. The later changes in the same delta name the column by its cached name.
- **D-M12d.4 — the hidden-table probe runs on the rescan paths only.** source-mysql has no
  `tracing`, so the probe's note travels as `RescanReport::warnings`. The worker forwards them
  from its gone-tables rescan, the session from its periodic one. The halt itself stays
  `HaltReason::TableDropped`, a unit variant (state compatibility).
- **D-M12d.5 — a sink table is baselined whenever it exists and the table is first tracked.**
  That covers any plan, not only a fresh start (`None if state.exists`). Cached schemas are never
  removed, so "uncached at a resume" means first tracked at a resume, such as the sink table a
  job's copy made. `missing_columns` refuses a non-null sink column the source lacks, checked
  after the source's own columns so that a missing key column is named as one.
- **D-M12d.6 — the via-root repair adds only the hidden leaves.** A `publish_via_partition_root`
  change no longer counts as `rewritten`. The leaves it hid are already in `added`, because
  `pg_publication_tables` lists only the root (measured, PostgreSQL 18), so they are copied, not
  every table.
- **D-M12d.7 — a reposition uses the key the table was made with.** `commit_empty` repositions
  under `SinkTableState::identifier` when the sink records one, else under the source's. It
  returns `Ok(false)` when there is no key at all: a held table no one resolves or caches, made
  without one, is skipped with a warning.
- **D-M12d.8 — progress is a durable checkpoint past `started_from`.** A run that ends
  `SourceChanged` after making a checkpoint durable sets the count back to 1. Any other end sets
  it to 0, and so does a stop. The halt's code is the `SourceChanged` message's prefix.
- **D-M12d.9 — `prepare_marked`, a defaulted trait method.** `prepare`'s signature doesn't
  change (no churn across some 60 PostgreSQL IT call sites). The PostgreSQL connector keeps an
  inherent `prepare` that calls `prepare_marked(…, None)`. The worker reads the markers only when
  `Preflight::server` differs from the checkpoint's: one listing and one `table_state` per held
  table, on that rare start only.
- **D-M12d.10 — a continuation applies under either `source_change_action`.** No position is lost,
  so neither action has anything to decide; the start warns, naming both servers. **For review:**
  an operator who chose `error` to be told of every failover is told by the warning, not a halt.
- **D-M12d.11 — a continued start records the new server.** A resume whose `Prepared::server`
  differs from the checkpoint's saves the checkpoint document with the new server before the
  stream opens. Otherwise the stream's open would see the old server and halt
  `WAL_SOURCE_CHANGED`, which is what the E2E found.

**Measured in planning (2026-11-06):**

- `buoyant_kernel` 0.28.1 `StructType::try_new` folds with `to_lowercase`. delta-rs refuses `ÄRGER`
  beside `ärger` (Task 1's `delta_rs_refuses_a_column_beside_one_that_differs_by_unicode_case`).
- MySQL 8.4 and MariaDB 11.8, for a user with no database-level privilege: a dropped table and an
  ungranted one both answer 1142 to `SELECT 1 … LIMIT 0`. 1146 needs a privilege on the database.
- PostgreSQL 18, a via-root publication naming the root and a leaf: `pg_publication_tables` lists
  only the root, and `pg_publication_rel` lists both.
- PostgreSQL 18 slot synchronization, on the `postgres-sync-*` pair:
  - the standby's first copy of a failover slot waits until the slot's `catalog_xmin` passes the
    standby's own. A slot that no one reads never gets there; the streaming pipeline moves it,
    nudged by `pg_log_standby_snapshot()`;
  - after `pg_promote()` the slot shows `failover`, `synced`, not `temporary`, the primary's
    `confirmed_flush`, and not `conflicting`;
  - `TIMELINE_HISTORY 2` gives the switch point, and `pg_control_checkpoint()` still shows
    timeline 1 right after the promotion;
  - the sync worker refuses a slot whose position needs WAL the standby lacks.
- A node restarted with an edited config file runs the cluster's stored generation, not the file:
  the E2E moves the pipeline with a reload.

## Spec patches (approved 2026-11-06; applied in Task 9)

Applied 2026-11-10 (Task 9). P-2 and P-5 were reworded during execution, with Oleksii's approval
(2026-11-10), to match Task 8's fix round: the slot is confirmed **exactly** at the checkpoint, with
no lake marker past it (it read "confirmed at least at the checkpoint and at the lake's highest
marker"), and P-5's Phase 2a branch says a running pipeline's reconnect reruns the start. P-6 was
added and approved during execution (2026-11-10); its "(or recovers)", which says what the rerun
does under `re_snapshot`, was approved as applied (2026-11-10). With P-6, Oleksii approved
(2026-11-10, Task 9's review, M1) narrowing the line's first PostgreSQL clause to "a reconnect that
reaches another system, or an earlier timeline of the same one (Phase 2a), halts under
source_change_action: error, and reruns Phase 0 under re_snapshot, before a lost slot is dropped or
made again there", so that it no longer contradicts P-6. Phase 2a's identity tree reads
consistently (its "another system or timeline" branch follows the continuation branch) and is
unchanged. P-7 was added and approved at the whole-branch review (2026-11-11, its m-2) and applied
in its fix round. P-8 was added and approved at the second re-review (2026-11-11, its I-B) and
applied in the third fix wave; its amendment (a table first tracked at a resume) was approved at the
third re-review (2026-11-11, its m-4) and applied in the fourth.

**P-1 — `Configuration_Specification.md` §9.2.1, `slot_management` row.** After "`managed`: create
the slot if absent, reuse if present, drop on delete per `auto_cleanup`." insert: "On PostgreSQL
17+ the slot is created as a failover slot (`failover = true`), which a standby with
`sync_replication_slots` synchronizes, so a promotion can continue the stream (DA-SRC-33)."

**P-2 — `Configuration_Specification.md` §9.2.1, `source_change_action` row.** Before "A checkpoint
written before the server was recorded (v0.1.0) …" insert: "Neither applies to a promotion the
slot followed: the same system on a later timeline, whose slot is a failover slot synchronized from
the old primary, still streaming, and confirmed exactly at the checkpoint, with no lake marker past
it. The stream continues there with a warning, and the checkpoint records the new server
(DA-SRC-33)." (Reworded 2026-11-10.)

**P-3 — `Configuration_Specification.md` §10 `column_type_overrides` row, and §13.** Append to the
row: "On MySQL and MariaDB the key's column is matched without case, as the server matches column
names; a key naming no column warns (V-127)." Add to §13, after V-079:

`| V-127 | tables.column_type_overrides keys | The key's column names no column of its table (on MySQL compared without case, as MySQL compares column names): the override changes nothing (DA-SRC-35). (Emitted at schema resolution.) | warning | pipeline |`

and a §15 history item: "M12d planning (2026-11-06): V-127; failover slots (§9.2.1)."

**P-4 — `Schema_Change_Detection_and_Evolution.md` §7.2 table.** Two rows, after `RENAME COLUMN — not
provable`:

`| MySQL RENAME/CHANGE COLUMN that changes only the name's case (MySQL compares column names without case) | **Ignorable** | none | none | the column keeps its cached spelling; a note (DA-SRC-25) |`

`| ADD COLUMN / RENAME COLUMN to a name that differs from a target column's by case alone (Unicode lower-casing) | **Unsafe** for Delta | — | **Unsupported → halt** `EVOLUTION_UNSUPPORTED`, naming both spellings | a table created with two such columns is refused, `SINK_REJECTED` (DA-SINK-12) |`

and in the `DROP TABLE` row's detection text, after "MySQL: channel B …": "On MySQL a table gone from
the catalog is probed (`SELECT 1 … LIMIT 0`): where the server answers 1142, the halt's note says
the user may have lost `SELECT` instead (DA-SRC-36)."

**P-5 — `Data-Flow.md`.**
- Phase 2a's identity tree: a branch before "another system or timeline": "the same system on a
  later timeline, the slot here a failover slot synced from the old primary, streaming, confirmed
  exactly at the checkpoint, with no lake marker past it → continue the stream there (either
  action, a warning); the checkpoint records the new server (DA-SRC-33). A running pipeline's
  reconnect to such a server reruns this start (Source Disconnect), so a promotion is continued
  whether the pipeline was running or stopped." (Reworded 2026-11-10.)
- Phase 4's reposition paragraph: "one per table the pipeline resolves at that start or has a
  cached schema for" becomes "one per table the sink holds, and per table the pipeline resolves or
  has a cached schema for, under the key the table was made with; a table whose reposition the
  sink refuses for good halts `SINK_REJECTED` (DA-SRC-32)".
- Phase 2b, after the dump's reconnect check: "Three Phase 0 reruns in a row for a server change,
  none with a durable checkpoint past its start, halt the pipeline with the change's code, naming
  the alternation: the address reaches more than one server (DA-SRC-34)."

**P-6 — `Data-Flow.md` "Source Disconnect"** (approved during execution, 2026-11-10, Task 8's
ruling). The PostgreSQL reconnect line gains: "a reconnect to the same system on a later timeline
reruns Phase 0 under either action: the start continues the stream there if the slot followed
(DA-SRC-33), else halts (or recovers) as source_change_action says". The "(or recovers)" was
approved as applied (2026-11-10). Its companion, also approved 2026-11-10: the line's first
PostgreSQL clause reads "a reconnect that reaches another system, or an earlier timeline of the same
one (Phase 2a), halts …".

**P-7 — `Data-Flow.md` "Source Disconnect"** (approved 2026-11-11, the whole-branch review's m-2).
After the PostgreSQL line, a line for every source: "─ Any source: three Phase 0 reruns in a row for
a server change, none with a durable checkpoint past its start, halt the pipeline, naming the
alternation: the address reaches more than one server (DA-SRC-34)". P-5 put the sentence in Phase
2b's file/position-mode tree only, while the controller counts every source's `SourceChanged` run,
PostgreSQL's included. Phase 2b's sentence stays.

**P-8 — `Schema_Change_Detection_and_Evolution.md` §3 Channel A and §8.2** (approved 2026-11-11, the
second re-review's I-B). Channel A's "Table absent in target" bullet becomes: "at a first start, a
cleared halt or a non-replayable gap, use the queried schema as the **baseline** to create the
target table via the sink (consistent with the snapshot read point, `Data-Flow.md` Phase 1); at a
replayable resume, create it from the pipeline's cached schema, which the stream's in-order replay
(B+C) then evolves". §8.2's "create absent tables from the queried baseline" gains "(at a
replayable resume, from the cached schema, which in-order replay then evolves)". It records O-1
(D-M12d.3): at a replayable resume the stream still holds the rows before a change, which on MySQL
decode only under the cached schema, and the change replays in order.

*P-8 amendment* (approved 2026-11-11, the third re-review's m-4). The bullet's queried-baseline cases
become "at a first start, a cleared halt, a non-replayable gap, or for a table first tracked at a
resume", and its cached case "for a table tracked before, at a replayable resume". §8.2's
parenthetical becomes "(at a replayable resume, a table tracked before from the cached schema, which
in-order replay then evolves)". A table first tracked at a resume (added to the config, or newly
matched) has no cached schema, and its stream history starts at that start (it gets an initial
snapshot), so the code makes it from the queried schema; "a first start" read as the pipeline's
left it unnamed.

## File Structure

| File | Task | Change |
|---|---|---|
| `crates/data-avalanche-core/src/table.rs`, `lib.rs` | 1 | `differ_by_case_alone` |
| `crates/data-avalanche-sink-delta/src/evolve.rs` | 1 | a case clash → `Unsupported` |
| `crates/data-avalanche-sink-delta/src/table.rs` | 1 | `create` refuses a case clash |
| `crates/data-avalanche-pipeline/src/schema_change.rs` | 1, 5 | `missing_columns`: Unicode case; the non-null check |
| `crates/data-avalanche-config/src/external.rs`, `registry.rs`, `lib.rs` | 2 | `column_override`, `check_column_keys`, V-127 |
| `crates/data-avalanche-source-mysql/src/catalog.rs` | 2, 4 | folded keys and overrides; `hidden`, `hidden_note` |
| `crates/data-avalanche-source-mysql/src/snapshot.rs`, `stream.rs` | 2, 3 | `column_override`; the folded column match |
| `crates/data-avalanche-source-pg/src/connector.rs` | 2, 8 | V-127; the preflight's server; `prepare_marked` |
| `crates/data-avalanche-core/src/source.rs` | 3, 4, 8 | `column_names_fold`; `RescanReport::warnings`; `Preflight::server`, `prepare_marked` |
| `crates/data-avalanche-source-mysql/src/schema.rs`, `lib.rs`, `connector.rs` | 3, 4, 8 | `keep_names`; the rescan's probe; `Preflight::server: None` |
| `crates/data-avalanche-pipeline/src/worker.rs` | 3–8 | `same_columns`; the rescan's warnings; the baseline arm; the reposition loop; `highest_marker`; the continued start |
| `crates/data-avalanche-pipeline/src/session.rs` | 4, 7 | the rescan's warnings; `started_from` |
| `crates/data-avalanche-pipeline/src/status.rs` | 7 | `WorkerStatus::started_from` |
| `crates/data-avalanche-source-pg/src/slot.rs` | 5, 8 | the via-root repair; `SlotState::failover/synced`, `continues`, `prepare_marked`, failover slots |
| `crates/data-avalanche-core/src/sink.rs` | 6 | `SinkTableState::identifier`, `Sink::held_tables` |
| `crates/data-avalanche-storage/src/paths.rs`, `lib.rs` | 6 | `unescape_table_segment` |
| `crates/data-avalanche-sink-delta/src/config.rs`, `sink.rs` | 6 | `root_url`; `held_tables`; the recorded key |
| `crates/data-avalanche-pipeline/src/jobs.rs` | 6 | the copy's reposition test |
| `crates/data-avalanche-cluster/src/controller.rs` | 7 | the alternation count and halt |
| `crates/data-avalanche-testkit/src/fake_source.rs`, `memory_sink.rs` | 3, 6, 8 | `fold_column_names`; `held_tables`; `marks`, `promote_following`, `mark_at` |
| `crates/data-avalanche/tests/e2e/failover.rs` | 8 | the failover-slot scenario |
| `test-infra/compose.yaml`, `postgres-primary.sh`, `postgres-replica.sh`, `README.md`; `.github/workflows/ci.yml` | 8 | the slot-sync pair |
| `docs/src/content/docs/…` (runbooks, sources, generated reference) | 9 | the operator docs |
| `doc/specs/*` (on approval), `doc/research/*`, `doc/analysis/CDC_Design_Gap_Analysis_v1.md`, `doc/v1.0/README.md` | 9 | patches, research, tracker §59, status |

---
## Tasks

### Task 1: Names that differ by case alone (`core`, `sink-delta`, `pipeline`) — DA-SINK-12

**Files:**
- Modify: `crates/data-avalanche-core/src/table.rs`, `crates/data-avalanche-core/src/lib.rs`
- Modify: `crates/data-avalanche-sink-delta/src/evolve.rs`, `crates/data-avalanche-sink-delta/src/table.rs`
- Modify: `crates/data-avalanche-pipeline/src/schema_change.rs`, `crates/data-avalanche-pipeline/src/worker.rs` (test)

**Interfaces:**
- Produces: `data_avalanche_core::differ_by_case_alone(a: &str, b: &str) -> bool` (re-exported at
  the crate root), used by Tasks 1 and 5.

- [ ] **Step 1: Write the failing tests**

`core/src/table.rs` tests:

```rust
#[test]
fn names_that_differ_by_case_alone_clash_unicode_too() {
    assert!(differ_by_case_alone("Note", "note"));
    assert!(differ_by_case_alone("ÄRGER", "ärger"), "Unicode, not ASCII");
    assert!(!differ_by_case_alone("note", "note"), "one name");
    assert!(!differ_by_case_alone("note", "notes"));
}
```

`sink-delta/src/evolve.rs` tests (beside `type_changes_and_structural_changes_are_unsupported`,
whose `created`/`apply` helpers they use; `ref` and `doc` are columns of `created`'s table):

```rust
/// DA-SINK-12: delta-rs refuses a column beside one that differs by case alone, Unicode case
/// too, so the evolver says so instead of retrying a commit that fails every time.
#[tokio::test]
async fn a_column_that_differs_from_one_by_case_alone_is_unsupported() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = created(&dir).await;
    let add = |name: &str| SchemaChange::AddColumn(ColumnSchema::new(name, V::Text, "text"));
    assert_eq!(apply(&mut h, vec![add("ärger")]).await, EvolveOutcome::Applied);
    let version = h.version();
    for change in [
        add("DOC"),
        add("ÄRGER"),
        SchemaChange::RenameColumn { from: "ref".into(), to: "Doc".into(), proven: true },
        // Within one delta: the second spelling clashes with the first.
        add("Tag"),
    ] {
        let changes = match &change {
            SchemaChange::AddColumn(c) if c.name == "Tag" => vec![add("tag"), change],
            _ => vec![change],
        };
        let out = apply(&mut h, changes).await;
        assert!(
            matches!(&out, EvolveOutcome::Unsupported { reason } if reason.contains("by case alone")),
            "{out:?}"
        );
    }
    assert_eq!(h.version(), version, "nothing was written");
    // A rename of a column's case alone clashes with the column it renames, which the sink
    // keeps (a rename adds the new name and keeps the old one).
    let out = apply(
        &mut h,
        vec![SchemaChange::RenameColumn { from: "doc".into(), to: "DOC".into(), proven: true }],
    )
    .await;
    assert!(matches!(out, EvolveOutcome::Unsupported { .. }), "{out:?}");
}

/// What the rule above follows, measured on the delta-rs this workspace links: its schema
/// check folds names with Unicode lower-casing, not ASCII's.
#[tokio::test]
async fn delta_rs_refuses_a_column_beside_one_that_differs_by_unicode_case() {
    let dir = tempfile::tempdir().unwrap();
    let h = created(&dir).await;
    let field = |n: &str| StructField::new(n, deltalake::kernel::DataType::STRING, true);
    let table = h.table().clone().add_columns().with_fields([field("ärger")]).await.unwrap();
    let refused = table.add_columns().with_fields([field("ÄRGER")]).await;
    assert!(refused.is_err(), "delta-rs took a column differing by Unicode case");
}
```

`sink-delta/src/table.rs` tests:

```rust
/// DA-SINK-12: a table whose columns differ by case alone is refused for good, not retried.
#[tokio::test]
async fn a_table_whose_columns_differ_by_case_alone_is_refused() {
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, TableSchema};
    let dir = tempfile::tempdir().unwrap();
    let mut h = handle(&dir, &t()).await;
    let schema = TableSchema::new(
        t(),
        vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("note", V::Text, "text"),
            ColumnSchema::new("Note", V::Text, "text"),
        ],
        vec!["id".into()],
    )
    .unwrap();
    let e = h.create(&spec(&schema), &TableDefaults::default()).await.unwrap_err();
    assert!(
        matches!(&e, SinkError::Permanent { reason } if reason.contains("\"note\" and \"Note\" differ by case alone")),
        "{e}"
    );
}
```

`pipeline/src/worker.rs`, at the end of `a_sink_column_matching_by_case_alone_halts`:

```rust
        // Unicode case too, as delta-rs folds it (DA-SINK-12).
        let sink = upsert_schema(&cased("id", "ärger"));
        assert!(matches!(
            missing_columns(&sink, &cased("id", "ÄRGER")),
            Err(HaltReason::SchemaMismatch { column, .. }) if column == "ÄRGER"
        ));
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-core --lib names_that_differ` → does not compile
(`differ_by_case_alone` undefined). After a stub returning `false`, the sink-delta tests fail:
`a_column_that_differs…` gets `Applied`/a commit error, `a_table_whose_columns…` gets a
transient error; the worker test fails on `ÄRGER` (`eq_ignore_ascii_case` is ASCII only).
`delta_rs_refuses_…` passes already: it records the crate's behaviour.

- [ ] **Step 3: Implement**

`core/src/table.rs` (re-export it from `lib.rs`'s `pub use table::{…}`):

```rust
/// True when `a` and `b` are two names that differ by case alone, which a Delta table can't hold
/// side by side: delta-rs compares column names after Unicode lower-casing (`to_lowercase`, as
/// `StructType::try_new` does), so `Ärger` and `ärger` clash as `Note` and `note` do.
pub fn differ_by_case_alone(a: &str, b: &str) -> bool {
    a != b && a.to_lowercase() == b.to_lowercase()
}
```

`sink-delta/src/evolve.rs` — before the change loop, a lookup over the table's fields and those
this delta added; an `AddColumn` and a `RenameColumn`'s `to` that clash return
`EvolveOutcome::Unsupported` at once (before anything is written). The rename's own `from` is not
exempt: the sink keeps the old column. The module doc's last bullet gains the case.

```rust
    // A name of the table's, or one this delta adds, that `name` matches by case alone.
    let clash = |name: &str, added: &[StructField]| {
        schema
            .fields()
            .chain(added.iter())
            .find(|f| differ_by_case_alone(f.name(), name))
            .map(|f| f.name().clone())
    };
    let clashes = |name: &str, held: String| EvolveOutcome::Unsupported {
        reason: format!(
            "column {name:?} differs from the table's column {held:?} by case alone, and a \
             Delta table can't hold both"
        ),
    };
```

`sink-delta/src/table.rs` `TableHandle::create`, first thing: for each field of `spec.schema`, a
field before it that `differ_by_case_alone` matches → `Err(permanent(format!("columns {:?} and {:?}
differ by case alone, and a Delta table can't hold both", earlier, this)))`.

`pipeline/src/schema_change.rs` `missing_columns`: the case match becomes
`.find(|f| differ_by_case_alone(f.name(), &c.name))`, and the doc says "Names match without case,
Unicode's, as Delta's do".

- [ ] **Step 4: Run them to see them pass**

Run: `cargo test -p data-avalanche-core -p data-avalanche-sink-delta -p data-avalanche-pipeline --lib`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core crates/data-avalanche-sink-delta crates/data-avalanche-pipeline
git commit -m "fix(sink-delta): names that differ by case alone are refused, not retried (DA-SINK-12)"
```

### Task 2: Config keys that name MySQL columns without case (`config`, `source-mysql`, `source-pg`) — DA-SRC-35

**Files:**
- Modify: `crates/data-avalanche-config/src/external.rs`, `registry.rs`, `lib.rs`; Test: `crates/data-avalanche-config/tests/external.rs`
- Modify: `crates/data-avalanche-source-mysql/src/catalog.rs`, `snapshot.rs`, `stream.rs`; Test: `tests/mysql/resolve.rs`
- Modify: `crates/data-avalanche-source-pg/src/connector.rs`; Test: `tests/pg/resolve.rs`

**Interfaces:**
- Produces (re-exported at the config crate root):
  - `column_override<'a>(tables: &'a Tables, table_key: &str, column: &str, fold: bool) -> Option<&'a LogicalType>`
  - `check_column_keys(p: &Pipeline, table_key: &str, columns: &[&str]) -> Report` (V-127, folds when `p.source.kind == SourceType::Mysql`)
- The registry gains V-127; `RULES.len()` becomes 121.

- [ ] **Step 1: Write the failing tests**

`config/tests/external.rs` (import `check_column_keys`, `column_override`; `COVERED` gains
`"V-127"` and becomes `[&str; 8]`):

```rust
/// DA-SRC-35: an override's column part matches as written on PostgreSQL and without case on
/// MySQL, as each compares column names; a key naming no column warns (V-127).
#[test]
fn column_keys_match_as_the_source_compares_names_v127() {
    let pg = load(&MINIMAL.replace(
        "include: [public.orders]",
        "include: [public.orders]\n      column_type_overrides: { public.orders.Amount: string, public.orders.code: string }",
    ));
    let p = &pg.pipelines["orders"];
    assert!(column_override(&p.tables, "public.orders", "code", false).is_some());
    assert!(
        column_override(&p.tables, "public.orders", "amount", false).is_none(),
        "PostgreSQL names are case-sensitive"
    );
    let report = check_column_keys(p, "public.orders", &["amount", "code"]);
    assert_eq!(rules(&report), ["V-127"]);
    assert!(report.warnings[0].message.contains("public.orders.Amount"), "{report:#?}");
    assert!(check_column_keys(p, "public.orders", &["Amount", "code"]).is_ok());
    assert!(check_column_keys(p, "public.other", &["id"]).is_ok(), "another table's keys");

    let my = load(
        &MINIMAL
            .replace("host: db.example.com", "type: mysql\n      host: db.example.com")
            .replace("postgres: { publication: orders_pub, slot: orders_slot }", "mysql: {}")
            .replace(
                "include: [public.orders]",
                "include: [orders]\n      column_type_overrides: { orders.Amount: string, orders.gone: string }",
            ),
    );
    let p = &my.pipelines["orders"];
    assert!(
        column_override(&p.tables, "orders", "amount", true).is_some(),
        "MySQL names compare without case"
    );
    assert!(column_override(&p.tables, "orders", "AMOUNT", true).is_some());
    assert!(column_override(&p.tables, "order", "s.amount", true).is_none());
    let report = check_column_keys(p, "orders", &["id", "amount"]);
    assert_eq!(rules(&report), ["V-127"]);
    assert!(report.warnings[0].message.contains("orders.gone"), "{report:#?}");
}
```

`source-mysql/tests/mysql/resolve.rs`:

```rust
/// DA-SRC-35: MySQL compares column names without case, so an override or a `primary_key` entry
/// spelled in another case applies, the schema keeps the catalog's spelling, and a key that names
/// no column warns (V-127).
#[tokio::test]
async fn column_keys_name_columns_without_case() {
    require_it!();
    let (n, _a) = fresh(
        "column_keys_case",
        "CREATE TABLE t (Id INT NOT NULL, Amount DECIMAL(10,2), v TEXT, UNIQUE KEY (Id));",
    )
    .await;
    let tables = "      column_type_overrides: { t.AMOUNT: string, t.gone: string }\n      primary_key: { t: [ID] }";
    let r = connector(&pipeline(&n, &["t"], tables, "")).resolve_tables().await.unwrap();
    assert!(r.halted.is_empty() && r.skipped.is_empty(), "{r:?}");
    let s = &r.tables[0];
    assert_eq!(s.identifier(), ["Id"], "the catalog's spelling");
    assert_eq!(s.column("Amount").unwrap().value_type, V::Text, "the override applies");
    assert_eq!(
        r.warnings.iter().filter(|w| w.starts_with("V-127") && w.contains("t.gone")).count(),
        1,
        "{:?}",
        r.warnings
    );
    assert!(!r.warnings.iter().any(|w| w.contains("t.AMOUNT")), "{:?}", r.warnings);
    cleanup(&n).await;
}
```

`source-pg/tests/pg/resolve.rs`, at the end of `unconstrained_numerics_under_error_warn_v079`
(before its `cleanup`):

```rust
    // An override whose column part is spelled in another case names no column: PostgreSQL's
    // names are case-sensitive, so it changes nothing and warns (V-127, DA-SRC-35).
    let c = connector(&pipeline(
        &n,
        &["*"],
        &format!(
            "      column_type_overrides: {{ {s}.t.W: \"decimal(38,2)\", {s}.t.n: string }}",
            s = n.schema
        ),
        "",
    ));
    let r = resolved(&c).await;
    assert_eq!(r.tables[0].column("n").unwrap().value_type, V::Text);
    let v127: Vec<&String> = r.warnings.iter().filter(|w| w.starts_with("V-127")).collect();
    assert_eq!(v127.len(), 1, "{:?}", r.warnings);
    assert!(v127[0].contains(".t.W"), "{v127:?}");
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-config --test external column_keys` → does not compile
(`column_override`, `check_column_keys` undefined). With `test-infra/up.sh` running,
`DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-mysql --test mysql column_keys_name` fails
(`ID` isn't a column; `t.AMOUNT` doesn't apply).

- [ ] **Step 3: Implement**

- `config/src/external.rs`:
  - `column_override` looks up `"{table_key}.{column}"` exactly. When `fold` is set, it then finds
    a key of that table whose column part matches after `to_lowercase()`.
  - The private `fn key_column<'k>(key: &'k str, table_key: &str) -> Option<&'k str>` is
    `key.strip_prefix(table_key)?.strip_prefix('.')`.
  - `check_column_keys` warns V-127 at `pipelines.<p>.tables.column_type_overrides.<key>` for each
    key of the table that names none of `columns`. The text is `"{key} names no column of
    {table_key}: it changes nothing"`.
  - The module doc lists V-127. Both functions are re-exported from `lib.rs`.
- `config/src/registry.rs`: after V-126:

  ```rust
  rule(
      "V-127",
      Warning,
      L::Pipeline,
      SourceSchema,
      r#"tables.column_type_overrides keys"#,
      r#"The key's column names no column of its table (on MySQL compared without case, as MySQL compares column names): the override changes nothing (DA-SRC-35). (Emitted at schema resolution.)"#,
  ),
  ```

  and `has_every_spec_rule_once_in_order` asserts `RULES.len() == 121`.
- `source-mysql/src/catalog.rs` `plan_table`: each `tables.primary_key` entry maps to the
  catalog's spelling (the column whose name matches without case, else the entry as written). The
  override is `column_override(tables, key, &c.name, true)`. The doc says the schema keeps the
  catalog's spelling. `resolve_tables`, on a planned table, extends `out.warnings` with
  `check_column_keys(&self.pipeline, table_key(&listed.table), &columns)` formatted
  `"{rule} {path}: {message}"`, as the other config warnings are.
- `source-mysql/src/snapshot.rs` `read_plan` and `src/stream.rs` (the table's decode plans):
  `column_override(tables, key, &col.name, true)`.
- `source-pg/src/connector.rs` `resolve_tables`: `report.extend(check_column_keys(&self.pipeline,
  &table_key(s.table()), &columns))` for each resolved table, before the refusal check (V-127
  warns, so it never refuses).
- `cargo xtask docs` regenerates `docs/src/content/docs/reference/validation-rules.md` with V-127.

- [ ] **Step 4: Run them to see them pass**

Run: `cargo test -p data-avalanche-config` and, with the infra up,
`DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-mysql --test mysql resolve::` and
`DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg resolve::`;
`cargo xtask docs --check`. Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config crates/data-avalanche-source-mysql crates/data-avalanche-source-pg docs/src/content/docs/reference
git commit -m "feat(config): column keys match MySQL columns without case; V-127 (DA-SRC-35)"
```

### Task 3: A MySQL column whose spelling alone changed (`core`, `source-mysql`, `pipeline`, `testkit`) — DA-SRC-25

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs` (`column_names_fold`)
- Modify: `crates/data-avalanche-source-mysql/src/schema.rs`, `lib.rs`, `connector.rs`, `snapshot.rs`; Test: `tests/mysql/schema.rs`
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`

**Interfaces:**
- Consumes: Task 2's `column_override` (MySQL's `snapshot.rs` already uses it).
- Produces:
  - `SourceConnector::column_names_fold(&self) -> bool` (default `false`; MySQL `true`);
  - `data_avalanche_source_mysql::keep_names(cached: &TableSchema, new: &TableSchema, renames: &[(String, String)]) -> Result<TableSchema, HaltReason>`;
  - `FakeSource::fold_column_names(&self)`.

- [ ] **Step 1: Write the failing tests**

`source-mysql/src/schema.rs` tests. In the existing scope test, the `CHANGE a A` case becomes:

```rust
        // `CHANGE a A BIGINT`: a change of case alone is the same column (DA-SRC-25): noted, its
        // type change named as cached, and the schema keeps the cached spelling.
        let now = table(vec![col("price", V::Int32, "int", 2), col("A", V::Int64, "bigint", 3)]);
        let renames = [("a".to_owned(), "A".to_owned())];
        let s = scope(&cached, &cols(&["a", "A"]), &renames, &now).unwrap();
        assert_eq!(names(&s), ["id", "price", "A"]);
        let changes = diff(&cached, &s, &renames);
        assert!(
            matches!(changes.as_slice(), [
                SchemaChange::NoTargetEffect(note),
                SchemaChange::ChangeType { column, .. },
            ] if note.contains("\"A\"") && column == "a"),
            "{changes:?}"
        );
        assert_eq!(names(&keep_names(&cached, &s, &renames).unwrap()), ["id", "price", "a"]);
        // Without the statement (a rescan, a baseline) too.
        let s = scope(&cached, &Scope::Table, &[], &now).unwrap();
        assert!(!diff(&cached, &s, &[]).iter().any(|c| matches!(c, SchemaChange::RenameColumn { .. })));
        assert_eq!(names(&keep_names(&cached, &s, &[]).unwrap()), ["id", "price", "a"]);
```

and two new tests:

```rust
/// `DROP b, CHANGE a B`: `a` is renamed onto the dropped column's name; it keeps its new
/// spelling and doesn't take the dropped column's (M12a's withdrawn `keep_names`).
#[test]
fn a_rename_onto_a_dropped_name_keeps_its_new_spelling() {
    let cached = table(vec![col("a", V::Int32, "int", 2), col("b", V::Int32, "int", 3)]);
    let now = table(vec![col("B", V::Int32, "int", 2)]);
    let renames = [("a".to_owned(), "B".to_owned())];
    let s = scope(&cached, &cols(&["b", "a", "B"]), &renames, &now).unwrap();
    assert_eq!(names(&keep_names(&cached, &s, &renames).unwrap()), ["id", "B"]);
    assert_eq!(
        diff(&cached, &s, &renames),
        [
            SchemaChange::DropColumn("b".into()),
            SchemaChange::RenameColumn { from: "a".into(), to: "B".into(), proven: true }
        ]
    );
}

/// The identifier is renamed with its column.
#[test]
fn a_key_column_keeps_its_cached_spelling() {
    let cached = table(vec![col("v", V::Int32, "int", 2)]);
    let now = TableSchema::new(
        cached.table().clone(),
        vec![col("ID", V::Int32, "int", 1).not_null(), col("v", V::Int32, "int", 2)],
        vec!["ID".into()],
    )
    .unwrap();
    let kept = keep_names(&cached, &now, &[]).unwrap();
    assert_eq!(kept.identifier(), ["id"]);
    assert!(diff(&cached, &now, &[]).iter().all(|c| matches!(c, SchemaChange::NoTargetEffect(_))));
}
```

`source-mysql/tests/mysql/schema.rs`:

```rust
/// DA-SRC-25: `RENAME COLUMN note TO Note` changes the case alone, the same column to MySQL.
/// The statement's change notes it and keeps the cached spelling, which the sink's column has;
/// the rescan reports no change; and the snapshot reads the table under the cached names.
#[tokio::test]
async fn a_change_of_case_alone_keeps_the_cached_name() {
    require_it!();
    let (n, mut a) = fresh(
        "schema_case",
        "CREATE TABLE t (id INT PRIMARY KEY, note TEXT); INSERT INTO t VALUES (1, 'a');",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let cached = c.resolve_tables().await.unwrap().tables.remove(0);
    let sql = "ALTER TABLE t RENAME COLUMN note TO Note";
    a.query_drop(sql).await.unwrap();
    let (delta, now) = c.augment(&cached, &ddl(sql)).await.unwrap();
    assert!(
        matches!(delta.changes.as_slice(), [SchemaChange::NoTargetEffect(_)]),
        "{:?}",
        delta.changes
    );
    assert_eq!(now.unwrap(), cached, "the cached spelling stays");
    let report = c.rescan(std::slice::from_ref(&cached)).await.unwrap();
    assert!(report.changed.is_empty(), "{report:?}");
    let chunk = c.read_chunk(&cached, None, 10).await.unwrap();
    assert_eq!(chunk.rows.len(), 1);
    cleanup(&n).await;
}
```

`pipeline/src/worker.rs` tests:

```rust
/// DA-SRC-25: on a source that compares column names without case (MySQL), a column whose
/// catalog spelling changed case alone while the pipeline was down is the same column: no
/// drift, and the cache keeps the sink's spelling. On PostgreSQL it is a change.
#[tokio::test]
async fn a_change_of_case_alone_is_no_drift_where_names_fold() {
    let recased = || {
        TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("DOC", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap()
    };
    let drift = |st: &WorkerStatus| {
        st.warnings.iter().any(|m| m.contains("changed while the pipeline was down"))
    };
    for fold in [true, false] {
        let w = World::new(&["a"]);
        w.source.load(&t("a"), rows(1..=2));
        let p = mode("mode: initial");
        run(&w, &p, async |status| streamed(&status, &w).await).await.0.unwrap();
        if fold {
            w.source.fold_column_names();
        }
        w.source.alter(&t("a"), recased());
        let (end, st) = run(&w, &p, async |status| streamed(&status, &w).await).await;
        assert_eq!(end.unwrap(), Exit::Drained);
        assert_eq!(drift(&st), !fold, "fold {fold}: {:?}", st.warnings);
        if fold {
            let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
            assert_eq!(cached.columns()[1].name, "doc", "the sink's spelling");
        }
    }
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-source-mysql --lib` → does not compile (`keep_names`); after a
stub returning `new.clone()`, the scope test fails (a `RenameColumn` where a `NoTargetEffect` is
expected). `cargo test -p data-avalanche-pipeline --lib a_change_of_case_alone` → does not compile
(`fold_column_names`).

- [ ] **Step 3: Implement**

- `core/src/source.rs`: `SourceConnector::column_names_fold(&self) -> bool { false }`, documented
  as "whether the source compares column names without case (MySQL: `note` and `Note` are one
  column), so a cached schema and the catalog's may spell a column differently and still agree".
  `MySqlConnector` returns `true`.
- `source-mysql/src/schema.rs` (module doc: "A column keeps the spelling the pipeline first
  cached … a change of case alone … is the same column to MySQL, noted and nothing more. A new
  column takes the catalog's spelling.")
  - `diff`'s local `target` and `taken` move to module functions
    `fn target<'a>(renames: &'a [(String, String)], name: &'a str) -> &'a str` and
    `fn taken(renames: &[(String, String)], n: &str) -> bool`.
  - `fn matched<'a>(cached, new, renames) -> impl Iterator<Item = (&'a ColumnSchema, &'a ColumnSchema)>`
    yields each cached column that isn't `taken`, with the column of `new` that `target` names
    (`column(new, …)`, without case). This is `diff`'s matching, shared.
  - `keep_names` collects, from `matched`, the pairs whose names differ by case alone
    (`c.name != n.name && same(&c.name, &n.name)`) as new name → cached name. With none, it
    returns `new.clone()`. Otherwise it rebuilds `new`'s columns and identifier under the cached
    names through `TableSchema::new`, keeping `new.source_id()`.
  - `diff`: when the matched column's name differs from the cached one by case alone, push
    `SchemaChange::NoTargetEffect(format!("column {:?} is now spelled {:?}: the same column to
    MySQL, kept as {:?}", c.name, n.name, c.name))` and use the cached name as `to` for the
    column's other changes. A proven rename to another name stays a `RenameColumn`.
  - `augment`: after `diff`, `let new = keep_names(cached, &new, &renames).map_err(halt)?;`.
    `rescan`: `Some(Ok(s))` → `keep_names(k, &s, &[])`, then `changed` only if it differs from
    `k`; an `Err(reason)` goes to `halted`.
  - `lib.rs` re-exports `keep_names`.
- `source-mysql/src/snapshot.rs` `read_plan`: the catalog column match is
  `x.name.to_lowercase() == col.name.to_lowercase()`, with a comment that the cached spelling stays.
- `pipeline/src/worker.rs`:

  ```rust
  /// Whether a cached schema's columns are the catalog's now: the same, in order, with the names
  /// compared without case when the source compares them so (`fold`, MySQL): a change of case alone
  /// is the same column, cached under its first spelling (DA-SRC-25).
  fn same_columns(cached: &TableSchema, now: &TableSchema, fold: bool) -> bool {
      let (a, b) = (cached.columns(), now.columns());
      a.len() == b.len()
          && a.iter().zip(b).all(|(x, y)| match fold && x.name != y.name {
              true => {
                  x.name.to_lowercase() == y.name.to_lowercase()
                      && ColumnSchema { name: y.name.clone(), ..x.clone() } == *y
              }
              false => x == y,
          })
  }
  ```

  Phase 0 takes `let fold = source.column_names_fold();` before the table loop, and both drift
  sites (the baseline's `!same_columns(c, now, fold)` and the resume's) use it in place of
  `c.columns() != now.columns()`.
- `testkit/src/fake_source.rs`: a `fold: bool` in `Db` (default false), `pub fn
  fold_column_names(&self)` that sets it (documented beside `alter`, after its doc comment), and
  `column_names_fold` in the trait impl returning it.

- [ ] **Step 4: Run them to see them pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`, `cargo test -p data-avalanche-pipeline --lib`,
and `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-mysql --test mysql schema::`, then the
other MySQL targets (`DATA_AVALANCHE_MYSQL=mysql-84-nogtid`, `mariadb-118`). Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core crates/data-avalanche-source-mysql crates/data-avalanche-pipeline crates/data-avalanche-testkit
git commit -m "fix(source-mysql): a column whose case alone changed keeps its cached name (DA-SRC-25)"
```

### Task 4: A MySQL table the user can no longer see (`core`, `source-mysql`, `pipeline`) — DA-SRC-36

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs` (`RescanReport::warnings`)
- Modify: `crates/data-avalanche-source-mysql/src/catalog.rs`, `schema.rs`; Test: `tests/mysql/schema.rs`
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`, `session.rs`

**Interfaces:**
- Produces: `RescanReport { …, warnings: Vec<String> }` — what the status says beside the halts.
  Every `RescanReport { … }` literal that doesn't use `..Default::default()` gains `warnings: vec![]`.

- [ ] **Step 1: Write the failing test**

`source-mysql/tests/mysql/schema.rs` (import `pipeline_as`, `user` from `common`):

```rust
/// DA-SRC-36: a table gone from `information_schema` halts `TABLE_DROPPED`. When the server
/// refuses to read it (1142) instead of saying it doesn't exist (1146), the rescan says beside
/// the halt that the pipeline's user may only have lost SELECT on it. A user with database-level
/// privileges gets 1146 for a dropped table, and no such word.
#[tokio::test]
async fn a_table_the_user_can_no_longer_see_reads_as_dropped_with_a_warning() {
    require_it!();
    let (n, mut a) = fresh(
        "schema_hidden",
        "CREATE TABLE t (id INT PRIMARY KEY); CREATE TABLE gone (id INT PRIMARY KEY);",
    )
    .await;
    let name = "it_schema_hidden";
    user(
        &mut a,
        name,
        "REPLICATION CLIENT, REPLICATION SLAVE ON *.*",
        &[format!("GRANT SELECT ON {}.t TO '{name}'@'%'", n.db)],
    )
    .await;
    let c = connector(&pipeline_as(&n, name, "      ssl_mode: disable", "", &["t"], ""));
    let t = c.resolve_tables().await.unwrap().tables.remove(0);
    a.query_drop(format!("REVOKE SELECT ON {}.t FROM '{name}'@'%'", n.db)).await.unwrap();
    let report = c.rescan(std::slice::from_ref(&t)).await.unwrap();
    assert_eq!(report.dropped, [t.table().clone()]);
    assert!(
        matches!(report.warnings.as_slice(), [w] if w.contains("1142") && w.contains("lost SELECT")),
        "{report:?}"
    );
    // Dropped, read by a user the server tells the truth to (root).
    let root = connector(&pipeline(&n, &["gone"], "", ""));
    let gone = root.resolve_tables().await.unwrap().tables.remove(0);
    a.query_drop("DROP TABLE gone").await.unwrap();
    let report = root.rescan(std::slice::from_ref(&gone)).await.unwrap();
    assert_eq!(report.dropped, [gone.table().clone()]);
    assert!(report.warnings.is_empty(), "{report:?}");
    a.query_drop(format!("DROP USER '{name}'@'%'")).await.unwrap();
    cleanup(&n).await;
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-mysql --test mysql a_table_the_user_can_no_longer_see`
→ does not compile (`RescanReport::warnings`).

- [ ] **Step 3: Implement**

- `core/src/source.rs`: `RescanReport::warnings: Vec<String>`, documented as "what the status should
  say beside the halts: a dropped table that may only be hidden from the pipeline's user (MySQL,
  DA-SRC-36)".
- `source-mysql/src/catalog.rs`:
  - `const DENIED: u16 = 1142;` (`ER_TABLEACCESS_DENIED_ERROR`) and `const NO_SUCH_TABLE: u16 = 1146;`.
  - `pub(crate) fn hidden_note(table: &TableRef) -> String` returns: "table {table}: it is gone
    from information_schema and the server refuses to read it (1142): it was dropped, or the
    pipeline's user lost SELECT on it, and the server hides which. It halts TABLE_DROPPED; if it
    still exists, grant SELECT on it again and re-snapshot it with a snapshot job".
  - `pub(crate) async fn hidden(&self, table: &TableRef) -> Result<bool, SourceError>` runs
    `SELECT 1 FROM {schema}.{name} LIMIT 0` (quoted with `crate::snapshot::ident`) through
    `self.conn.command("probe a table gone from the catalog", …)`. A server error 1142 gives
    `true`; 1146 or success gives `false` (the catalog was read before a create); any other error
    propagates. The doc records the measurement: for a user without a database-level privilege,
    MySQL 8.4 and MariaDB 11.8 answer 1142 for a dropped table too.
- `source-mysql/src/schema.rs` `rescan`: a table gone from the catalog pushes `hidden_note(t)` to
  `report.warnings` when `self.hidden(t).await?`, then `report.dropped` as before.
- `pipeline/src/worker.rs`, Phase 0's rescan of the tables gone from the source's list:
  `warnings.extend(report.warnings)` on success.
- `pipeline/src/session.rs`, the periodic rescan's apply: before the halts, warn each of
  `report.warnings` (`self.core.update(|st| … st.warn(w.clone()))`).

`HaltReason::TableDropped` doesn't change: the halt is in the state document, which v0.1.0 reads.

- [ ] **Step 4: Run it to see it pass**

Run: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-mysql --test mysql schema::` on
`mysql-84` and with `DATA_AVALANCHE_MYSQL=mariadb-118`; `cargo test -p data-avalanche-pipeline --lib`.
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core crates/data-avalanche-source-mysql crates/data-avalanche-pipeline
git commit -m "feat(source-mysql): a table the user can no longer see is told apart where the server allows (DA-SRC-36)"
```

### Task 5: A sink table a new state meets (`pipeline`, `source-pg`) — DA-SNAP-22

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`, `schema_change.rs`
- Modify: `crates/data-avalanche-source-pg/src/slot.rs`; Test: `tests/pg/slot.rs`

**Interfaces:**
- Consumes: Task 1's `differ_by_case_alone`.
- Produces: nothing new; `missing_columns` refuses one more case.

- [ ] **Step 1: Write the failing tests**

`pipeline/src/worker.rs` tests:

```rust
/// DA-SNAP-22: a table first tracked at a resume (here, one created since the last start)
/// whose sink table exists already, as a job's copy leaves it, is baselined to the source's
/// columns before its copy, like a wiped state's.
#[tokio::test]
async fn a_table_first_tracked_at_a_resume_is_baselined_against_its_sink_table() {
    let w = World::new(&["a"]);
    let p = mode("mode: initial");
    run(&w, &p, async |status| streamed(&status, &w).await).await.0.unwrap();
    w.sink.ensure_table(&TableSpec::new(&schema("b"), vec![]).unwrap()).await.unwrap();
    let noted = TableSchema::new(
        t("b"),
        vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("doc", V::Text, "text"),
            ColumnSchema::new("note", V::Text, "text"),
        ],
        vec!["id".into()],
    )
    .unwrap();
    w.source.create_table(noted);
    let mut r = row(1, "x");
    r.0.push(("note".into(), ColumnValue::Text("n".into())));
    w.source.load(&t("b"), vec![r]);
    let (end, st) = run(&w, &p, async |status| streamed(&status, &w).await).await;
    assert_eq!(end.unwrap(), Exit::Drained);
    assert_eq!(w.source.prepares().len(), 2, "a resume");
    assert!(st.halted.is_empty(), "{:?}", st.halted);
    assert_eq!(w.text("b", "note"), [Some("n".to_owned())]);
}

/// DA-SNAP-22: a sink table holding a non-null column the source no longer has (a key it
/// was made with) can't take the source's rows: it halts at the baseline, naming the column,
/// rather than at the first MERGE.
#[test]
fn a_sink_table_with_a_non_null_column_the_source_lacks_halts() {
    let keyed = TableSchema::new(
        t("a"),
        vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("region", V::Text, "text").not_null(),
            ColumnSchema::new("doc", V::Text, "text"),
        ],
        vec!["id".into(), "region".into()],
    )
    .unwrap();
    let sink = upsert_schema(&keyed);
    let Err(HaltReason::SchemaMismatch { column, detail }) = missing_columns(&sink, &schema("a"))
    else {
        panic!("a non-null column the source lacks halts");
    };
    assert_eq!(column, "region");
    assert!(detail.contains("non-null"), "{detail}");
    // A nullable one is soft-dropped: kept, written NULL.
    let sink = upsert_schema(
        &TableSchema::new(
            t("a"),
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("doc", V::Text, "text"),
                ColumnSchema::new("old", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap(),
    );
    assert!(missing_columns(&sink, &schema("a")).unwrap().is_empty());
}
```

(`a_sink_table_without_the_key_column_halts` must keep naming the missing key `uid`, not the sink's
non-null `id`: the new check runs after the per-column loop.)

`source-pg/tests/pg/slot.rs`:

```rust
/// DA-SNAP-22: a managed publication that publishes via the partition root is set to publish
/// each partition under its own name, and only the tracked partitions whose changes arrived under
/// the root's name are copied again (as added tables), the one the publication names beside its
/// root included; the slot and the other tables' stream stay.
#[tokio::test]
async fn a_via_root_repair_copies_only_the_partitions_it_hid() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "slot_via_root_repair",
        "CREATE TABLE a (id int PRIMARY KEY);
         CREATE TABLE events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
         CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
         CREATE TABLE events_2025 PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let tables = [n.table("a"), n.table("events_2024"), n.table("events_2025")];
    let p = c.prepare(None, &tables).await.unwrap();
    let start = from(&p.plan, SnapshotReason::FirstStart);
    let SourcePosition::Pg(slot) = &start else { unreachable!() };
    a.batch_execute(&format!(
        "ALTER PUBLICATION {p} ADD TABLE {s}.events; ALTER PUBLICATION {p} SET (publish_via_partition_root = true)",
        p = n.publication,
        s = n.schema
    ))
    .await
    .unwrap();
    let p = c.prepare(Some(&Checkpoint::from(start.clone())), &tables).await.unwrap();
    assert_eq!(p.plan, StartPlan::Resume, "the slot stays");
    assert_eq!(confirmed(&a, &n).await, Some(*slot));
    assert_eq!(p.added, [n.table("events_2024"), n.table("events_2025")]);
    let via: bool = a
        .query_one("SELECT pubviaroot FROM pg_publication WHERE pubname = $1", &[&n.publication])
        .await
        .unwrap()
        .get(0);
    assert!(!via, "each partition is published under its own name again");
    cleanup(&a, &n).await;
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-pipeline --lib -- a_table_first_tracked a_sink_table_with_a_non_null`
→ the first fails (the sink table isn't baselined at a resume: the copy's `note` lands nowhere, a
`SINK_REJECTED` halt or a missing column); the second fails (`Ok(vec![])`).
`DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg a_via_root_repair` fails: the
plan is a `PositionLost` snapshot, the slot recreated.

- [ ] **Step 3: Implement**

- `pipeline/src/worker.rs`, Phase 0's per-table match: the arm for a table with no cached schema
  becomes `None if state.exists => {` (was `None if state.exists && baseline`). Its comment names
  both cases: a wiped state, and a table first tracked at a resume whose sink table a job's copy
  made (DA-SNAP-13, DA-SNAP-22).
- `pipeline/src/schema_change.rs` `missing_columns`: after the per-column loop, the first sink
  field that is non-null, not a lineage column (`LINEAGE_COLUMNS`), not internal
  (`INTERNAL_COLUMN_PREFIX`), and that no source column names (exactly or by case alone) →
  `HaltReason::SchemaMismatch { column, detail: "the sink table, made by a run whose state is
  gone, holds this non-null column, which the source doesn't have: no row of the source's could
  fill it; drop the sink table and re-snapshot the table" }`. It is `if let Some(f) = …find(…)`,
  not a loop: clippy's `never_loop`. The doc's "one the source lacks stays, soft-dropped" becomes
  "a nullable one".
- `source-pg/src/slot.rs` `reconcile_publication`: drop `partitioned` from `rewritten`, and with
  it the `partitions` lookup. The reason goes in a comment: `pg_publication_tables` lists only
  the root under `publish_via_partition_root`, even for a partition the publication names too, so
  those partitions are among `missing` and come back as `added`. `Reconciled::rewritten`'s doc
  says so.

- [ ] **Step 4: Run them to see them pass**

Run: `cargo test -p data-avalanche-pipeline --lib`;
`DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg slot::` (on 18, and with
`DATA_AVALANCHE_PG_PORT=55417`/`55414`). Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline crates/data-avalanche-source-pg
git commit -m "fix(pipeline): a sink table a new state meets is baselined; the via-root repair copies what it hid (DA-SNAP-22)"
```

### Task 6: A source change repositions every table the lake holds (`core`, `storage`, `sink-delta`, `pipeline`, `testkit`) — DA-SRC-32

**Files:**
- Modify: `crates/data-avalanche-core/src/sink.rs`
- Modify: `crates/data-avalanche-storage/src/paths.rs`, `lib.rs`
- Modify: `crates/data-avalanche-sink-delta/src/config.rs`, `sink.rs`
- Modify: `crates/data-avalanche-testkit/src/memory_sink.rs`
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`; Test: `crates/data-avalanche-pipeline/src/jobs.rs`

**Interfaces:**
- Produces:
  - `SinkTableState { …, identifier: Vec<String> }` — the key the table was created with; empty
    when the sink records none or the table is absent. Every literal gains it.
  - `Sink::held_tables(&self) -> Result<Vec<TableRef>, SinkError>` — required (no default): both
    sinks implement it.
  - `data_avalanche_storage::unescape_table_segment(segment: &str) -> Option<String>`.
  - `DeltaLocation::root_url(&self) -> Result<Url, SinkError>`.
  - `commit_empty(core, tuning, table: &TableRef, identifier: &[String], at, kind) -> Result<bool, WorkerError>`
    (private; was `(…, schema: &TableSchema, …) -> Result<(), _>`).

- [ ] **Step 1: Write the failing tests**

`storage/src/paths.rs` tests:

```rust
#[test]
fn a_table_segment_unescapes_to_its_name() {
    for name in ["orders", "a/b", "50%", "~x", ".", "..", "naïve", "tab\tbed", ".pipeline-state"] {
        assert_eq!(
            unescape_table_segment(&escape_table_segment(name)).as_deref(),
            Some(name),
            "{name}"
        );
    }
    assert_eq!(unescape_table_segment("~2Epipeline-state").as_deref(), Some(".pipeline-state"));
    assert_eq!(unescape_table_segment("bad~4"), None);
    assert_eq!(unescape_table_segment("bad~ZZ"), None);
    assert_eq!(unescape_table_segment("~FF"), None, "not UTF-8");
}
```

`sink-delta/src/sink.rs` tests:

```rust
/// DA-SRC-32: the sink lists every table under its root, escaped names too, and not the
/// co-located state or a folder without a `_delta_log`; a table's state gives its key.
#[tokio::test]
async fn the_sink_lists_the_tables_it_holds() {
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, TableSchema};
    let dir = tempfile::tempdir().unwrap();
    let s = sink(&dir);
    assert!(s.held_tables().await.unwrap().is_empty(), "nothing yet");
    s.ensure_table(&spec(&orders())).await.unwrap();
    let odd = TableSchema::new(
        TableRef::new("a/b", "50%"),
        vec![ColumnSchema::new("k", V::Int64, "bigint").not_null()],
        vec!["k".into()],
    )
    .unwrap();
    s.ensure_table(&spec(&odd)).await.unwrap();
    std::fs::create_dir_all(dir.path().join(".pipeline-state/tables")).unwrap();
    std::fs::create_dir_all(dir.path().join("public/not_a_table")).unwrap();
    let mut held = s.held_tables().await.unwrap();
    held.sort();
    let mut want = vec![TableRef::new("public", "orders"), TableRef::new("a/b", "50%")];
    want.sort();
    assert_eq!(held, want);
    let state = s.table_state(&TableRef::new("a/b", "50%")).await.unwrap();
    assert_eq!(state.identifier, ["k"]);
}
```

`pipeline/src/jobs.rs` tests (a job's copy is a table the pipeline neither resolves nor caches):

```rust
/// DA-SRC-32: a source change repositions every table the lake holds, a job's copy too,
/// which the pipeline neither resolves nor caches: its marker leaves the old server's
/// positions, so a later copy or stream of it isn't skipped below them.
#[tokio::test]
async fn a_source_change_repositions_a_copy_the_pipeline_doesnt_track() {
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{ServerIdentity, Sink, SourcePosition};
    let pg = |timeline| ServerIdentity::Postgres { system_identifier: 7, timeline };
    let w = World::new(&["a", "x"]);
    w.source.outside_config(&t("x"));
    w.source.set_server(Some(pg(1)));
    established(&w).await;
    w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
    w.source.load(&t("x"), rows(1..=3));
    ask(&w, "p~0", &["x"]).await;
    let (end, _) = run(&w, &never(10), async |status| {
        wait_until("x is copied", || w.ids("x") == [1, 2, 3]).await;
        streamed(&status, &w).await;
        wait_until("the copy is done", || status.borrow().snapshots.is_empty()).await;
    })
    .await;
    end.unwrap();
    let old = w.sink.table_state(&t("x")).await.unwrap().marker.unwrap();
    w.source.fail_over(pg(2), Lsn(0x100));
    let (end, _) = run(&w, &never(10), async |status| streamed(&status, &w).await).await;
    end.unwrap();
    let moved = w.sink.table_state(&t("x")).await.unwrap().marker.unwrap();
    assert_eq!(moved.committed_position, SourcePosition::Pg(Lsn(0x100)));
    assert_ne!(moved, old);
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-storage --lib unescape` and `cargo test -p data-avalanche-sink-delta --lib held`
→ do not compile (`unescape_table_segment`, `held_tables`). After the sink side,
`cargo test -p data-avalanche-pipeline --lib a_source_change_repositions_a_copy` fails at the
`committed_position` assert (x keeps the old server's marker).

- [ ] **Step 3: Implement**

- `core/src/sink.rs`:
  - `SinkTableState::identifier`, documented: "The key the table was created with (the sink
    records it); empty when it records none or the table is absent. A reposition commits under
    it: it carries no row, and the table's key may not be the one the source has now
    (DA-SRC-32)."
  - `Sink::held_tables`, documented: "The tables the sink holds at the pipeline's location
    (Delta: each `<schema>/<table>/` root with a `_delta_log` under the pipeline's
    `sink.prefix`), whether the pipeline resolves or caches them or not: a source change
    repositions every one (DA-SRC-32)."
- `storage/src/paths.rs` (re-export from `lib.rs`):

  ```rust
  /// The identifier [`escape_table_segment`] made `segment` from; `None` for a segment it can't
  /// have made (a `~` not followed by two hex digits, escaped bytes that aren't UTF-8).
  pub fn unescape_table_segment(segment: &str) -> Option<String> {
      let mut bytes = Vec::with_capacity(segment.len());
      let mut rest = segment.as_bytes();
      while let Some((&b, tail)) = rest.split_first() {
          if b == b'~' {
              let hex = std::str::from_utf8(tail.get(..2)?).ok()?;
              bytes.push(u8::from_str_radix(hex, 16).ok()?);
              rest = &tail[2..];
          } else {
              bytes.push(b);
              rest = tail;
          }
      }
      String::from_utf8(bytes).ok()
  }
  ```

- `sink-delta/src/config.rs`: `table_url`'s body moves to `fn url_of(&self, path: impl Fn(&str,
  &str) -> String) -> Result<Url, SinkError>`. `table_url` is
  `self.url_of(|bucket, prefix| table_root(bucket, prefix, &table.schema, &table.name).root)`,
  and the new `root_url` is `self.url_of(|_, prefix| join(&[prefix]))`. The segment loop skips
  empty segments (`root.split('/').filter(|s| !s.is_empty())`), so an empty prefix gives the
  bucket root.
- `sink-delta/src/sink.rs`:
  - `table_state` fills `identifier: h.identifier()?` for a present table, and `vec![]` for an
    absent one.
  - `held_tables`:
    - a local root that doesn't exist holds nothing;
    - otherwise it builds the store with
      `DeltaTableBuilder::from_url(root).with_storage_options(self.options.clone()).build_storage()?.object_store(None)`;
    - it lists two levels with `list_with_delimiter`, skipping a schema segment equal to
      `data_avalanche_storage::COLOCATED_STATE_DIR` and any segment `unescape_table_segment`
      refuses;
    - it keeps each `<schema>/<table>/` whose `_delta_log/` lists at least one object.
- `testkit/src/memory_sink.rs`: `table_state` fills `identifier` from the stored table (`vec![]`
  when absent); `held_tables` returns the keys of its tables.
- `pipeline/src/worker.rs`:
  - `commit_empty` takes `table: &TableRef` and the source's `identifier: &[String]`. It reads
    `table_state` first: an absent table, or a reposition of one without a marker, returns
    `Ok(true)` with nothing committed. A reposition commits under `state.identifier` when that
    isn't empty, else under `identifier`. With no key at all it returns `Ok(false)`. The
    truncate caller passes `schema.table(), schema.identifier()` and treats `Ok(false)` like
    `Ok(true)`.
  - The source-change step (`Plan::Fresh(SnapshotReason::SourceChanged)`) repositions the union
    of `resolved_tables`, `cached` and `sink.held_tables()` (a `BTreeSet<&TableRef>`), each with
    the cached or resolved schema's identifier as the fallback key. For each result:
    - `Ok(false)` warns "table {table}: the lake holds it, but its table records no key and the
      pipeline doesn't track it, so its marker stays on the old server; track it and re-snapshot
      it before its changes stream";
    - `Err(WorkerError::Sink(SinkError::Permanent { reason }))` records a `TableHalt` (`copy:
      false`, `at`) with `HaltReason::SinkRejected { detail: format!("the source changed, and its
      reposition fails: {reason}") }`, after `core.may_write()?`, and shows it in the status. The
      session's start reads the recorded halts, so the table is never streamed;
    - any other error ends the start, as before.

    The comment above the step names DA-SRC-32 (every table the sink holds; a table the sink
    refuses halts instead of ending every start).

- [ ] **Step 4: Run them to see them pass**

Run: `cargo test -p data-avalanche-storage -p data-avalanche-sink-delta -p data-avalanche-pipeline -p data-avalanche-testkit --lib`.
Mutation check: replace `.chain(&held)` with nothing; `a_source_change_repositions_a_copy…` must
fail at the `committed_position` assert. With the infra up, the M12a failover E2E runs the
listing on RustFS (Task 8's run covers it). Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core crates/data-avalanche-storage crates/data-avalanche-sink-delta crates/data-avalanche-testkit crates/data-avalanche-pipeline
git commit -m "fix(pipeline): a source change repositions every table the lake holds (DA-SRC-32)"
```

### Task 7: A host that alternates between servers (`pipeline`, `cluster`) — DA-SRC-34

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/status.rs`, `session.rs`
- Modify: `crates/data-avalanche-cluster/src/controller.rs`, `lib.rs`

**Interfaces:**
- Produces:
  - `WorkerStatus { …, started_from: Option<SourcePosition> }` — the checkpoint the worker's first
    stream session started from;
  - `data_avalanche_cluster::SOURCE_CHANGES_IN_A_ROW: u32 = 3` (re-exported at the crate root).

- [ ] **Step 1: Write the failing tests**

`cluster/src/controller.rs` tests (beside `a_transient_worker_error_reruns_it_after_a_backoff`;
`TestFactory`, `node`, `until`, `eventually` are the module's helpers):

```rust
fn changed() -> SourceError {
    SourceError::SourceChanged(
        "BINLOG_SOURCE_CHANGED: binlog position f:4 was recorded on server 1, and the stream reaches server 2; binlog_purge_action is re_snapshot, so the pipeline starts again to recover from this server's position".into(),
    )
}

/// DA-SRC-34: a host that alternates between servers finds another one at every run, and
/// each run resets every table's marker while nothing streams. The third run in a row
/// without a durable checkpoint halts with the change's code, naming the alternation.
#[tokio::test(start_paused = true)]
async fn runs_that_keep_finding_another_server_halt() {
    let store = ScriptedStore::new();
    let docs = pipeline_docs();
    let f = TestFactory::new(&["a"]);
    for _ in 0..SOURCE_CHANGES_IN_A_ROW {
        f.source.fail(Method::OpenStream, changed());
    }
    let mut n = node("n1", &store, &docs, &f);
    until(&mut n.status, "error", |s| matches!(&s.actual, Some(Actual::Error { .. }))).await;
    let Some(Actual::Error { error, code }) = n.status.borrow().actual.clone() else {
        unreachable!()
    };
    assert_eq!(code.as_deref(), Some("BINLOG_SOURCE_CHANGED"));
    assert!(
        error.starts_with("BINLOG_SOURCE_CHANGED: 3 runs in a row found another source server")
            && error.contains("Point the pipeline at one server")
            && error.contains("The last: binlog position f:4 was recorded on server 1"),
        "{error}"
    );
    assert_eq!(f.builds(), 3);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(f.builds(), 3, "no rerun in ERROR");
}

/// A run that makes a checkpoint durable starts the count again: a failover now and then
/// isn't an alternation.
#[tokio::test(start_paused = true)]
async fn a_durable_checkpoint_starts_the_count_of_server_changes_again() {
    let store = ScriptedStore::new();
    let docs = pipeline_docs();
    let f = TestFactory::new(&["a"]);
    f.source.fail(Method::OpenStream, changed());
    f.source.fail(Method::OpenStream, changed());
    let mut n = node("n1", &store, &docs, &f);
    eventually("a third worker", || f.builds() == 3).await;
    until(&mut n.status, "running", is(Actual::Running)).await;
    f.source.commit(FakeTxn::new().insert(&t("a"), row(1)));
    eventually("the row lands", || f.ids("a") == [1]).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    f.source.fail(Method::Next, changed());
    f.source.fail(Method::OpenStream, changed());
    eventually("a fifth worker", || f.builds() == 5).await;
    until(&mut n.status, "running again", is(Actual::Running)).await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(n.status.borrow().actual, Some(Actual::Running));
    assert_eq!(f.builds(), 5);
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-cluster --lib -- runs_that_keep_finding a_durable_checkpoint_starts`
→ does not compile (`SOURCE_CHANGES_IN_A_ROW`). With the constant alone, the first fails: the
controller reruns forever (`until … "error"` times out).

- [ ] **Step 3: Implement**

- `pipeline/src/status.rs`: `WorkerStatus::started_from`, documented: "The checkpoint the worker's
  first stream session started from. A durable checkpoint past it is the run's progress: the
  controller halts a pipeline whose runs keep finding another server without any (DA-SRC-34)."
- `pipeline/src/session.rs`, where the session sets `Phase::Streaming` and `checkpoint`:
  `st.started_from.get_or_insert_with(|| checkpoint.clone());` (set once: a reconnect's session
  doesn't move it).
- `cluster/src/controller.rs`:
  - `pub const SOURCE_CHANGES_IN_A_ROW: u32 = 3;`, documented as the runs in a row that may find
    another server without a durable checkpoint in between before the pipeline halts.
  - `fn alternating(why: &str, runs: u32) -> Failure`:
    - the code is `SourceError::Permanent(why.to_owned()).halt_code()` (the `SourceChanged`
      message starts with it);
    - `last` is `why` without the `"{code}: "` prefix;
    - the error text is `"{code}: {runs} runs in a row found another source server than the run
      before, and none made a checkpoint durable: the source's address reaches more than one
      server (a load balancer, or a DNS name over several). Point the pipeline at one server,
      then stop and resume it. The last: {last}"`. Without a code, the `"{code}: "` prefix is
      left out.
  - In the term loop:
    - a `changed: u32` counter beside `failures`, reset to 0 by a stop (where `failures` is);
    - on `Event::Ended`, after `observe`, read `progressed` from the worker's last status:
      `st.checkpoint.is_some() && st.checkpoint != st.started_from`;
    - then `changed` becomes 1 for a `SourceChanged` end that progressed, `changed + 1` for one
      that didn't, and 0 for any other end;
    - a first match arm, `Err(WorkerError::Source(SourceError::SourceChanged(why))) if changed >=
      SOURCE_CHANGES_IN_A_ROW`, sets `error = Some(alternating(&why, changed))`, logged as the
      other ERRORs are (`code = …`, `"pipeline in ERROR: …"`). The other arms are unchanged: a
      `SourceChanged` below the limit is still `Recovery::Retry`.
  - `lib.rs` re-exports the constant.

- [ ] **Step 4: Run them to see them pass**

Run: `cargo test -p data-avalanche-cluster --lib` and `cargo test -p data-avalanche-pipeline --lib`.
Two mutation checks:
- `if progressed => changed + 1` must fail `a_durable_checkpoint…` (a halt at the fourth run);
- `changed >= 99` must fail `runs_that_keep…`.

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline crates/data-avalanche-cluster
git commit -m "feat(cluster): runs that keep finding another server halt the pipeline (DA-SRC-34)"
```

### Task 8: A promotion the failover slot followed (`core`, `source-pg`, `source-mysql`, `pipeline`, `testkit`, test-infra, CI) — DA-SRC-33

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs` (`Preflight::server`, `prepare_marked`)
- Modify: `crates/data-avalanche-source-pg/src/slot.rs`, `connector.rs`; Test: `tests/pg/slot.rs`
- Modify: `crates/data-avalanche-source-mysql/src/connector.rs` (`server: None`)
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`, `memory_sink.rs`
- Modify: `test-infra/compose.yaml`, `postgres-primary.sh`, `postgres-replica.sh`, `README.md`; `.github/workflows/ci.yml`
- Test: `crates/data-avalanche/tests/e2e/failover.rs`

**Interfaces:**
- Consumes: Task 6's `Sink::held_tables`, and the reposition loop's union of tables.
- Produces:
  - `Preflight { …, server: Option<ServerIdentity> }` (PostgreSQL: read in the preflight; MySQL: `None`);
  - `SourceConnector::prepare_marked(&self, checkpoint: Option<&Checkpoint>, tables: &[TableRef], marked: Option<&SourcePosition>) -> Result<Prepared, SourceError>`,
    defaulting to `self.prepare(checkpoint, tables)`;
  - `slot::continues(recorded: &Checkpoint, now: &ServerIdentity, slot: Option<&SlotState>, marked: Option<Lsn>) -> bool` (crate-internal, as `source_change` is);
  - `SlotState { …, failover: bool, synced: bool }`;
  - testkit: `FakeSource::marks(&self) -> Vec<Option<SourcePosition>>`, `FakeSource::promote_following(&self, server: ServerIdentity)`,
    `MemorySink::mark_at(&self, table: &TableRef, position: SourcePosition)`.

- [ ] **Step 1: Write the failing tests**

`source-pg/src/slot.rs` tests:

```rust
/// DA-SRC-33: a promotion the slot followed continues only on the same system's later
/// timeline, with the slot failover, synced and streaming, confirmed at the checkpoint and
/// at the lake's highest marker; anything else, or the marker unread, is a source change.
#[test]
fn a_promotion_the_slot_followed_continues_the_stream() {
    let pg = |system_identifier, timeline| ServerIdentity::Postgres { system_identifier, timeline };
    let recorded = Checkpoint { position: SourcePosition::Pg(Lsn(0x20)), server: Some(pg(7, 1)) };
    let synced = SlotState { failover: true, synced: true, ..slot(0x20) };
    let on = |now: ServerIdentity, s: &SlotState, marked: Option<u64>| {
        continues(&recorded, &now, Some(s), marked.map(Lsn))
    };
    assert!(on(pg(7, 2), &synced, Some(0x20)));
    assert!(on(pg(7, 3), &synced, Some(0x18)), "a marker below the checkpoint");
    assert!(!on(pg(7, 2), &synced, Some(0x21)), "a marker past the slot");
    assert!(!on(pg(7, 2), &synced, None), "the markers unread");
    assert!(!on(pg(8, 2), &synced, Some(0x20)), "another system");
    assert!(!on(pg(7, 1), &synced, Some(0x20)), "the same timeline");
    let not = |f: fn(&mut SlotState)| {
        let mut s = synced.clone();
        f(&mut s);
        s
    };
    assert!(!on(pg(7, 2), &not(|s| s.synced = false), Some(0x20)));
    assert!(!on(pg(7, 2), &not(|s| s.failover = false), Some(0x20)));
    assert!(!on(pg(7, 2), &not(|s| s.invalidated = true), Some(0x20)));
    assert!(
        !on(pg(7, 2), &not(|s| s.confirmed_flush = Some(Lsn(0x10))), Some(0x10)),
        "the slot behind the checkpoint"
    );
    assert!(!continues(&recorded, &pg(7, 2), None, Some(Lsn(0x20))), "no slot");
}
```

(the module's `slot(flushed)` helper gains `failover: false, synced: false`.)

`source-pg/tests/pg/slot.rs`:

```rust
/// DA-SRC-33: a managed slot is made a failover slot on PostgreSQL 17+, which a standby with
/// `sync_replication_slots` keeps; before 17 there is no such slot. The preflight names the
/// server, as `prepare` does.
#[tokio::test]
async fn a_managed_slot_is_a_failover_slot_from_postgresql_17() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_failover", DDL).await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let p = c.prepare(None, &[n.table("a")]).await.unwrap();
    let preflight = c.preflight().await.unwrap();
    assert_eq!(preflight.server, p.server);
    let row = a
        .query_one(
            "SELECT current_setting('server_version_num')::int >= 170000,
                    coalesce((to_jsonb(s) ->> 'failover')::bool, false)
               FROM pg_replication_slots s WHERE slot_name = $1",
            &[&n.slot],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, bool>(1), row.get::<_, bool>(0), "a failover slot exactly on 17+");
    cleanup(&a, &n).await;
}
```

`pipeline/src/worker.rs` tests:

```rust
/// DA-SRC-33: a start that reaches another server than the checkpoint's tells the source the
/// lake's highest marker, for it to decide whether its stream continues there; a start on
/// the checkpoint's server reads no marker for it.
#[tokio::test]
async fn a_start_on_another_server_tells_the_source_the_highest_marker() {
    let w = World::new(&["a"]);
    let pg = |timeline| ServerIdentity::Postgres { system_identifier: 7, timeline };
    w.source.set_server(Some(pg(1)));
    w.source.load(&t("a"), rows(1..=2));
    let (end, _) = run(&w, &mode("mode: initial"), async |status| {
        w.source.commit(FakeTxn::new().insert(&t("a"), row(3, "x")));
        streamed(&status, &w).await;
    })
    .await;
    end.unwrap();
    let (end, _) = run(&w, &mode("mode: never"), async |status| streamed(&status, &w).await).await;
    end.unwrap();
    assert_eq!(w.source.marks(), [None, None], "no server change");
    let checkpoint = w.docs.slot().await.unwrap().unwrap().doc.confirmed_flush_lsn;
    let past = SourcePosition::Pg(Lsn(checkpoint.0 + 0x40));
    w.sink.mark_at(&t("a"), past.clone());
    w.source.fail_over(pg(2), Lsn(0x100));
    let (end, _) = run(&w, &mode("mode: never"), async |status| streamed(&status, &w).await).await;
    end.unwrap();
    assert_eq!(w.source.marks()[2], Some(past));
}

/// DA-SRC-33: a promotion the slot followed. The source continues its stream on the new
/// server: the start resumes, records the new server with the checkpoint (which the stream's
/// open checks), and streams on; nothing is re-snapshotted.
#[tokio::test]
async fn a_promotion_the_slot_followed_resumes_on_the_new_server() {
    let w = World::new(&["a"]);
    let pg = |timeline| ServerIdentity::Postgres { system_identifier: 7, timeline };
    w.source.set_server(Some(pg(1)));
    w.source.load(&t("a"), rows(1..=2));
    let (end, _) = run(&w, &mode("mode: initial"), async |status| {
        w.source.commit(FakeTxn::new().insert(&t("a"), row(3, "before")));
        streamed(&status, &w).await;
    })
    .await;
    end.unwrap();
    let read = w.source.reads(&t("a"));
    w.source.promote_following(pg(2));
    let (end, st) = run(&w, &mode("mode: initial"), async |status| {
        streamed(&status, &w).await;
        w.source.commit(FakeTxn::new().insert(&t("a"), row(4, "after")));
        streamed(&status, &w).await;
    })
    .await;
    end.unwrap();
    assert_eq!(w.ids("a"), [1, 2, 3, 4]);
    assert_eq!(w.source.reads(&t("a")), read, "nothing re-snapshotted");
    assert_eq!(w.docs.slot().await.unwrap().unwrap().doc.server, Some(pg(2)));
    assert!(
        !st.warnings.iter().any(|w| w.contains("source server changed")),
        "{:?}",
        st.warnings
    );
}
```

`crates/data-avalanche/tests/e2e/failover.rs`. `pair()` takes the variable's name (`pair(var: &str)`),
and the standby check moves to `async fn standby(port: u16, services: &str) -> Db`, which panics
with the `--force-recreate {services}` hint when the server isn't in recovery. The M12a scenario
calls `pair("DATA_AVALANCHE_PG_REPLICA")` and `standby(replica, "postgres-primary postgres-replica")`.
The module doc names the second pair. The new scenario:

```rust
/// DA-SRC-33 (Q-M12d-5): the pipeline makes its slot a failover slot (PostgreSQL 17+), and the
/// standby synchronizes it. Stopped once the slot holds the lake's markers, the pipeline is moved
/// to the promoted standby: its start finds the slot there, synchronized past every marker, and
/// the stream continues on the new timeline, under `source_change_action`'s default (`error`),
/// without a re-snapshot.
#[tokio::test]
async fn a_promotion_the_failover_slot_followed_continues_the_stream() {
    require_it!();
    crate::postgres_only!();
    let Some((primary, replica)) = pair("DATA_AVALANCHE_PG_SYNC_REPLICA") else {
        eprintln!("skipped: DATA_AVALANCHE_PG_SYNC_REPLICA unset (test-infra's replica profile)");
        return;
    };
    let standby = standby(replica, "postgres-sync-primary postgres-sync-replica").await;
    let e = E2e::on(
        Source::Postgres { port: primary },
        "pg_failover_slot",
        &format!("{ITEMS} INSERT INTO items SELECT g, 'p' FROM generate_series(1, 5) g;"),
        Options { tables: ", snapshots: { mode: initial }", ..Options::default() },
    )
    .await;
    let s = e.schema.clone();
    let mut p = e.start(&[]);
    p.ready().await;
    holds(&e, &[1, 2, 3, 4, 5]).await;
    let first_doc = e.completed("items", None).await;
    e.streaming().await;
    e.exec("INSERT INTO items SELECT g, 'p' FROM generate_series(6, 8) g").await;
    holds(&e, &(1..=8).collect::<Vec<_>>()).await;
    // The standby's first copy of the slot waits until the slot's catalog_xmin passes the
    // standby's own: the streaming pipeline moves it, decoding the standby snapshots logged on
    // the primary (measured on PostgreSQL 18: a slot no one reads is never synchronized).
    eventually("the standby synchronizes the slot", Duration::from_secs(180), async || {
        let _ = e.db.exec("SELECT pg_log_standby_snapshot()").await;
        standby
            .any(&format!("SELECT 1 FROM pg_replication_slots WHERE slot_name = '{s}' AND synced"))
            .await
    })
    .await;
    let marker = e.marker("items").await;
    eventually("the slot confirms the marker", Duration::from_secs(60), async || {
        e.acknowledges(&marker).await
    })
    .await;
    assert!(p.terminate().await.success());
    assert_eq!(
        e.db.text(&format!("SELECT failover FROM pg_replication_slots WHERE slot_name = '{s}'"))
            .await
            .expect("the slot")[0][0]
            .as_deref(),
        Some("t"),
        "a managed slot is a failover slot on PostgreSQL 17+"
    );

    // The standby's copy follows the stopped pipeline's slot up to its checkpoint.
    let checkpoint = e
        .docs()
        .slot()
        .await
        .expect("the slot document reads")
        .expect("a checkpoint")
        .doc
        .confirmed_flush_lsn;
    eventually(
        "the standby's copy of the slot reaches the checkpoint",
        Duration::from_secs(180),
        async || {
            standby
                .any(&format!(
                    "SELECT 1 FROM pg_replication_slots
                      WHERE slot_name = '{s}' AND synced AND failover
                        AND confirmed_flush_lsn >= '{checkpoint}'::pg_lsn
                        AND confirmed_flush_lsn >= '{}'::pg_lsn",
                    marker.untagged()
                ))
                .await
        },
    )
    .await;

    // The failover: the standby, promoted, takes writes on a timeline of its own.
    let promoted = standby.text("SELECT pg_promote()").await.expect("the promotion");
    assert_eq!(promoted[0][0].as_deref(), Some("t"), "pg_promote() didn't finish");
    standby
        .exec(&format!("INSERT INTO {s}.items VALUES (9, 'r')"))
        .await
        .expect("a write on the promoted standby");

    // The pipeline, moved to it, streams on: no halt, no re-snapshot. The node runs the
    // cluster's config generation, the old server's, until the move is reloaded.
    let mut p = e.start(&[]);
    p.ready().await;
    let first = e.config();
    let moved = first.replacen(
        &format!("      port: {primary}\n"),
        &format!("      port: {replica}\n"),
        1,
    );
    assert_ne!(moved, first);
    e.write_config(&moved);
    assert_eq!(p.reload().await.0, 202);
    p.metric(&e.status("running")).await;
    holds(&e, &(1..=9).collect::<Vec<_>>()).await;
    standby
        .exec(&format!("INSERT INTO {s}.items VALUES (10, 'r')"))
        .await
        .expect("a second write");
    holds(&e, &(1..=10).collect::<Vec<_>>()).await;
    assert_eq!(
        e.snapshot("items").await.and_then(|d| d.completed_at),
        first_doc.completed_at,
        "no re-snapshot"
    );
    let log = p.log();
    assert!(log.contains("so the stream continues on the new timeline"), "{log}");
    assert!(!log.contains("WAL_SOURCE_CHANGED"), "{log}");
    assert!(p.terminate().await.success());
    standby
        .exec(&format!(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
              WHERE slot_name = '{s}' AND NOT active;
             DROP PUBLICATION IF EXISTS {s}_pub; DROP SCHEMA IF EXISTS {s} CASCADE;"
        ))
        .await
        .expect("the promoted standby's copy dropped");
    e.cleanup().await;
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-source-pg --lib a_promotion` and `cargo test -p data-avalanche-pipeline --lib a_promotion_the_slot_followed`
→ do not compile (`continues`, `SlotState::failover`, `promote_following`, `marks`, `mark_at`).
Once they compile, `a_promotion_the_slot_followed_resumes…` fails until the checkpoint records
the new server (D-M12d.11): the fake's stream open sees the old server, as PostgreSQL's does. With
the pair up (Step 4's command), the E2E halts `WAL_SOURCE_CHANGED` before the PostgreSQL side is
in.

- [ ] **Step 3: Implement**

- `core/src/source.rs`:
  - `Preflight::server: Option<ServerIdentity>`, documented: "The server the preflight reached,
    as a checkpoint records it: a start compares it with the checkpoint's to tell
    [`SourceConnector::prepare_marked`] the lake's highest marker (DA-SRC-33). `None` for a source
    that doesn't continue across a server change."
  - `prepare_marked`, documented: "[`Self::prepare`], told `marked`: the highest position the
    lake's markers name, at least the checkpoint's, read when the preflight found another server
    than the checkpoint's. A source that can keep its stream across a server change (PostgreSQL's
    synchronized failover slot, DA-SRC-33) does only when the new server's position covers it:
    otherwise a marker could cover a change the new server makes. `None`: not read, so no such
    continuation." The default body is `let _ = marked; self.prepare(checkpoint, tables).await`.
- `source-mysql/src/connector.rs` preflight: `server: None` ("A binlog position means nothing on
  another server: no continuation").
- `source-pg/src/connector.rs`:
  - the preflight reads `crate::slot::server_identity(&client)` through `self.conn.command("read
    the server's identity", …)` and returns `server: Some(parse_identity(&id, &timeline)?)`;
  - the trait impl adds `prepare_marked`, which calls the inherent one; `prepare` still calls
    `PgConnector::prepare`.
- `source-pg/src/slot.rs`:
  - `SlotState::failover` and `SlotState::synced`, documented as PostgreSQL 17+ and false before.
    `slot_state` reads them version-agnostically:
    `coalesce((to_jsonb(s) ->> 'failover')::bool, false)` and the same for `synced`.
  - `pub fn continues(recorded, now, slot, marked) -> bool` (in the private `slot` module, beside
    `source_change`) is true when all of
    these hold:
    - both servers are `ServerIdentity::Postgres`, with the same `system_identifier`, and
      `now.timeline > recorded.timeline`;
    - there is a slot, and it is `failover && synced && !invalidated`;
    - `marked` is `Some`, and the slot's `confirmed_flush` is at least `lsn_of(&recorded.position)`
      and at least `marked`.

    Its doc carries the soundness argument (Q-M12d-5).
  - `PgConnector::prepare` becomes `self.prepare_marked(checkpoint, tables, None).await`.
    `pub async fn prepare_marked(&self, checkpoint, tables, marked: Option<&SourcePosition>)`
    holds the old body, changed in three places:
    - the slot is read before the source-change check;
    - `let continued = recorded.is_some_and(|c| c.server.as_ref().is_some_and(|was| *was !=
      server) && continues(c, &server, slot.as_ref(), marked))`, and
      `let changed = !continued && source_change(recorded, &server, action)?;`;
    - a continued start warns "the checkpoint was read on {was}, and the pipeline now reads
      {server}: replication slot {name} came over synchronized, past everything the lake holds,
      so the stream continues on the new timeline", and `decide` runs as for an unchanged
      server.
  - `create_slot` reads `current_setting('server_version_num')::int >= 170000` and creates with
    `pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)` when it is set, else
    the two-argument form. Its comment names DA-SRC-33.
- `pipeline/src/worker.rs`:
  - `async fn highest_marker<'a>(sink: &dyn Sink, tuning: &Tuning, tracked: impl Iterator<Item =
    &'a TableRef>, checkpoint: &SourcePosition) -> Result<SourcePosition, WorkerError>`. It starts
    from `checkpoint` over `tracked ∪ held_tables()` (a `BTreeSet<TableRef>`). For each table with
    a marker that `highest` doesn't cover, the marker becomes `highest`; a marker that doesn't
    compare (`covers` errs) is left out.
  - Before `prepare`: `marked` is `Some(highest_marker(…, resolved_tables.keys().chain(cached.keys()),
    &c.position).await?)` when the stored checkpoint names a server and `preflight.server` names
    another, else `None`. The call is `source.prepare_marked(checkpoint.as_ref(), &refs,
    marked.as_ref())`.
  - `moved = matches!(plan, Plan::Resume) && server.is_some() && checkpoint.as_ref().is_some_and(|c|
    c.server.is_some() && c.server != server)`. The checkpoint document is saved when the plan is
    fresh or `moved` (`fresh = (… || moved).then_some(doc)`), commented as a resume on another
    server that the source continued (D-M12d.11).
- `testkit/src/fake_source.rs`:
  - `Db` gains `marks: Vec<Option<SourcePosition>>`, `followed: bool` and `continuing: bool`.
  - `preflight` returns `server: self.db().server.clone()`.
  - `pub fn marks(&self)` returns the recorded `marks`.
  - `pub fn promote_following(&self, server)` sets the server and `followed`, keeping the WAL and
    the slot.
  - `prepare_marked` records `marked` and sets `continuing = followed && marked ≤ slot &&
    checkpoint ≤ slot`, then calls `prepare`. `prepare` takes `continuing` (`std::mem::take`) and
    treats a continued server change as no change.
- `testkit/src/memory_sink.rs`: `pub fn mark_at(&self, table, position)` sets the table's marker
  position (a commit whose checkpoint never became durable).
- test-infra (profile `replica`):
  - `compose.yaml` gains `postgres-sync-primary` (postgres:18, port 55422, `-c
    synchronized_standby_slots=standby_sync`, env `STANDBY_SLOT: standby_sync`, tmpfs, the
    primary's init script) and `postgres-sync-replica` (postgres:18, port 55423, `-c
    sync_replication_slots=on -c hot_standby_feedback=on`, env `PRIMARY: postgres-sync-primary`
    and `STANDBY_SLOT: standby_sync`, `depends_on` the primary healthy). Both take the `*pg`
    anchor and the M12a pair's `wal_level`/slots/senders flags.
  - `postgres-primary.sh`: with `STANDBY_SLOT` set, `SELECT
    pg_create_physical_replication_slot('$STANDBY_SLOT')` after the pg_hba line.
  - `postgres-replica.sh`:
    - `primary=${PRIMARY:-postgres-primary}` for `pg_isready` and the base backup;
    - with `STANDBY_SLOT`, the backup is `pg_basebackup -d "host=$primary user=avalanche
      dbname=avalanche" -D "$data" -R -X stream -c fast -S "$STANDBY_SLOT"`, because slot sync
      needs `dbname` in `primary_conninfo`.
  - `README.md`: a row for the pair, and the failover-slot scenario's paragraph and command
    beside M12a's.
- `.github/workflows/ci.yml`: "Start the replica pairs" starts all four services, and the
  slice-1 E2E step sets `DATA_AVALANCHE_PG_SYNC_REPLICA: "55422:55423"`.

- [ ] **Step 4: Run them to see them pass**

Run: `cargo test -p data-avalanche-source-pg -p data-avalanche-pipeline -p data-avalanche-testkit --lib`;
`DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg slot::` on 18, 17 and 14 (the
failover test asserts `false` on 14); then:

```sh
docker compose -f test-infra/compose.yaml --profile replica up -d --wait --force-recreate \
  postgres-primary postgres-replica postgres-sync-primary postgres-sync-replica
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_PG_REPLICA=55420:55421 \
  DATA_AVALANCHE_PG_SYNC_REPLICA=55422:55423 \
  cargo test -p data-avalanche --features e2e-hooks --test e2e -- failover:: --test-threads=1
```

Mutation checks:
- the worker's `highest = m.committed_position` replaced by nothing must fail
  `a_start_on_another_server…` (the floor stays the checkpoint);
- `moved` left out of `fresh` must fail `a_promotion_the_slot_followed_resumes…` (the stream's
  open sees the old server).

Expected: all pass; the failover-slot scenario runs in about 50 s.

- [ ] **Step 5: Commit**

```bash
git add crates test-infra .github/workflows/ci.yml
git commit -m "feat(source-pg): a promotion the failover slot followed continues the stream (DA-SRC-33)"
```

### Task 9: The docs, the specs, the research and the tracker

**Files:**
- Modify: `docs/src/content/docs/runbooks/halted-table.md`, `runbooks/position-lost.mdx`,
  `configuration/sources.md`, `configuration/sink.md`; generated `reference/*` (`cargo xtask docs`)
- Modify (on approval of §Spec patches): `doc/specs/Configuration_Specification.md`,
  `Schema_Change_Detection_and_Evolution.md`, `Data-Flow.md`
- Modify: `doc/research/Postgres_Slot_and_Publication_Lifecycle.md`, `MySQL_DDL_Classifier.md`,
  `Delivery_Guarantee_and_Position_Guard.md`, `Multi_Sink_Architecture_and_Delta_Lake.md`
- Modify: `doc/analysis/CDC_Design_Gap_Analysis_v1.md`, `doc/v1.0/README.md`, this plan
  (deviations)

No code. Each step is checked by `cargo xtask docs --check` (the generated pages, and every config
example in the docs validates) and a read of the changed page.

- [ ] **Step 1: The operator docs**

- `runbooks/halted-table.md`, the code table:
  - `EVOLUTION_UNSUPPORTED`: "… or a column added or renamed to a name that differs from a
    Delta column's by case alone (Delta folds Unicode case), the detail naming both". The fix is
    to rename the source column, or drop the Delta table and copy the table again.
  - `TABLE_DROPPED`: the MySQL text gains the status warning that names `1142` ("the server
    hides whether the table was dropped or the user lost `SELECT`").
  - `SCHEMA_MISMATCH`: "… or a Delta table, left by a state that's gone, holding a non-null
    column the source lacks".
  - `SINK_REJECTED`: "… a table whose two source columns differ by case alone, or whose
    reposition after a source change Delta refused (the detail says `its reposition fails`)".
- `runbooks/position-lost.mdx`:
  - in "A failover (`WAL_SOURCE_CHANGED`)", a first paragraph headed "A failover slot carries the
    stream over". It says that on PostgreSQL 17+ a managed slot is a failover slot. With
    `synchronized_standby_slots` on the primary and `sync_replication_slots` on the standby, a
    promotion the slot followed continues the stream without a halt or a copy, under either
    action, and the log warns `the stream continues on the new timeline`. Otherwise everything
    below applies. A pre-17 slot, or a standby that doesn't sync, is not covered;
  - the binlog section's "The server changed": a paragraph on the alternation halt (three runs in
    a row, `BINLOG_SOURCE_CHANGED` / `WAL_SOURCE_CHANGED`, "the source's address reaches more than
    one server"). The fix is to point the pipeline at one server (no load balancer, no
    round-robin DNS name), then stop and resume it;
  - the "repositions every table" sentence names every Delta table under the prefix, a job's copy
    included.
- `configuration/sources.md`: the `slot_management: managed` paragraph says a managed slot is a
  failover slot on 17+. The source-change paragraph (line ~88) gains the continuation, linking
  the runbook.
- `configuration/sink.md` (`column_type_overrides`): on MySQL and MariaDB the key's column matches
  without case, and a key that names no column warns (V-127).
- `cargo xtask docs` (already run in Task 2 for V-127).

```bash
git add docs
git commit -m "docs: failover slots, the alternation halt, case clashes and hidden tables (M12d)"
```

- [ ] **Step 2: The spec patches (approved 2026-11-06)**

Apply P-1…P-5 as drafted, bump each spec's `updated:`, and add the config §15 history item.

```bash
git add doc/specs
git commit -m "spec: M12d — failover slots, V-127, case-only changes, the reposition set, the alternation halt"
```

- [ ] **Step 3: The research**

- `Postgres_Slot_and_Publication_Lifecycle.md`: a section "Failover slots (DA-SRC-33,
  M12d)". It covers:
  - the creation flag;
  - the four conditions of `continues` and why they're sound (the sync worker's refusal; the
    switch point not read);
  - the first-sync condition on `catalog_xmin`, measured;
  - why the checkpoint is re-recorded;
  - DA-SRC-31's deferral and what v2 needs (Q-M12d-1);
  - the via-root measurement and the narrowed repair (DA-SNAP-22).
- `MySQL_DDL_Classifier.md`: the case-only change as `NoTargetEffect`, `keep_names` following the
  diff's matching, and the 1142/1146 probe and its limit (DA-SRC-25, DA-SRC-36).
- `Delivery_Guarantee_and_Position_Guard.md`: the reposition set (held ∪ resolved ∪ cached), the
  recorded key, the halt on a refused reposition; the alternation count (DA-SRC-32, DA-SRC-34).
- `Multi_Sink_Architecture_and_Delta_Lake.md`: Delta's Unicode case folding, refused at create and
  evolve (DA-SINK-12); `Sink::held_tables`.

```bash
git add doc/research
git commit -m "research: M12d — failover slots, case-only changes, the reposition set"
```

- [ ] **Step 4: The tracker and the index**

- `CDC_Design_Gap_Analysis_v1.md`:
  - a section "M12d source and sink follow-ups — the gaps closed": DA-SINK-12, DA-SRC-35,
    DA-SRC-25, DA-SRC-36, DA-SNAP-22, DA-SRC-32, DA-SRC-34 and DA-SRC-33 ✅, each with its task
    and test, and DA-SRC-31 ⏭ (Q-M12d-1) with what v2 needs;
  - the domain tables' rows updated, the dashboard counts moved, `updated:` bumped.
- `doc/v1.0/README.md`: the M12d row's status, from what ran.
- This plan: a "Deviations during execution" section, if any.

```bash
git add doc/analysis doc/v1.0
git commit -m "gap: v1 §<N> — M12d's gaps closed (DA-SINK-12, DA-SRC-25, 32…36, DA-SNAP-22; DA-SRC-31 ⏭)"
```

---

## Exit

- [ ] Every task's tests pass; `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`
  and `cargo test --workspace` pass; `cargo xtask docs --check` passes.
- [ ] With `test-infra/up.sh` and both replica pairs recreated, the CI integration job's commands
  pass locally: the workspace ITs (`-- --test-threads=1`), the MySQL targets (`mysql-84-nogtid`,
  `mariadb-118`, `mariadb-118-binlog`), PostgreSQL 17 and 14 (`DATA_AVALANCHE_PG_PORT`), and the
  slice-1 E2E with `DATA_AVALANCHE_PG_REPLICA` and `DATA_AVALANCHE_PG_SYNC_REPLICA` set.
- [ ] The tracker closes DA-SINK-12, DA-SRC-25, DA-SRC-32…36 and DA-SNAP-22, and marks DA-SRC-31 ⏭.
  The approved spec patches are applied.
- [ ] The whole-branch review runs when Oleksii asks; then the branch is pushed and its PR opened
  against `main` (not merged).

## Deviations during execution

Each was found by the task's review (or its implementer) and fixed in the task's fix round, unless
it says otherwise. Later tasks' blocks applied on top unchanged.

- **Task 3** (fix round, `5c242af`; Critical in its review, and the planning reference had it too):
  the snapshot's `read_plan` kept the catalog's spelling in its column, so after a case-only change
  the rows, the key columns and the chunk cursor were named otherwise than the cached schema (a
  `SCHEMA_MISMATCH`, or a panic on a recased key). The plan's column is now the cached name over the
  catalog's column (`CatalogColumn { name: col.name.clone(), ..catalog.clone() }`). `scope` spells
  the identifier as the result's column (`column_in`), not `now`'s. A new Phase 0 arm
  (`Some(c) if state.exists && baseline && c.columns() != now.columns()`) caches `c`, the sink's
  spelling, on a fold-only difference (Important in the review: the baseline cached `now`'s
  spelling). `FakeSource::rescan` compares names without case when it folds. Test
  `a_baseline_of_a_case_only_change_caches_the_sink_spelling`; the IT
  `a_change_of_case_alone_keeps_the_cached_name` recases the key and resumes a chunk cursor. The
  code's `keep_names` matches as `diff` does (the proven renames, then the name without case), which
  D-M12d.3's "by position and `source_id`" wording described loosely.
- **Task 4** (fix round, `bebd9ef`): `hidden()` propagated any error other than 1142/1146 (a dropped
  schema's 1049), so the rescan failed and `TABLE_DROPPED` never came. It now returns `bool` and logs
  any other error as not hidden (`tracing::warn!`), so source-mysql gains the `tracing` dependency
  (D-M12d.4 said it had none; the probe's note still travels as `RescanReport::warnings`).
  `FakeSource::set_rescan_warnings` and two pipeline tests pin the forwarding
  (`a_gone_tables_rescan_warnings_reach_the_status`, `a_periodic_rescans_warnings_reach_the_status`);
  IT `a_table_whose_schema_is_gone_is_still_reported_dropped`.
- **Task 5** (`3c6a252`, comments only): the `missing_columns` doc covers a table first tracked at a
  resume. Its halt texts kept the plan's "made by a run whose state is gone", inaccurate at a resume;
  Oleksii: raise at the whole-branch review. Reworded there (m-1, `cb11145`, approved 2026-11-11):
  "the sink table, which the pipeline meets for the first time (left by a state that's gone, or made
  by a job's copy), lacks this key column; …", and the same opening for the non-null column, as the
  halted-table runbook words it.
- **Task 6** (fix round, `d4ed202`): the `_delta_log/` probe listed every log page; it now reads the
  first listing entry (`store.list(..).next()`, one page). `unescape_table_segment` accepts only the
  canonical form (`escape_table_segment` must reproduce the segment). `MemorySink::forget_key`, a test
  hook; tests `a_refused_reposition_halts_that_table_and_the_start_goes_on` and
  `a_held_table_with_no_key_is_warned_about`. Not fixed (M-1): a refused reposition of a table the
  pipeline doesn't track halts one start only and is never retried; tracked later, the table keeps the
  old server's marker (gap tracker DA-SRC-37).
- **Task 8** (fix round, `8002013`; Important in its review): the continuation was evaluated only at a
  start, so a running pipeline whose reconnect reached the promoted server halted
  `WAL_SOURCE_CHANGED` under the default `error`, against D-M12d.10. Oleksii's ruling: a reconnect to
  the same system on a later timeline returns `SourceError::SourceChanged` under either action
  (`slot::reconnect_check`), so the start reruns and continues or halts (spec patch P-6). The message
  carries no `WAL_SOURCE_CHANGED:` prefix, so alerting on the code isn't paged by a continuation.
  The worker test `a_failover_mid_run_halts_under_error` now expects a rerun, then the halt; new
  `a_reconnect_to_the_promoted_server_reruns_the_start_which_continues`. The E2E keeps the pipeline
  running through the promotion, its address moved by a new `Proxy::tcp` (a raw byte copy). Also in
  the round:
  - `continues` requires `confirmed_flush` **equal** to the checkpoint and `marked` at most the
    checkpoint (it accepted a slot past the checkpoint, which `decide` then re-snapshotted silently);
    P-2 and P-5 reworded to match (Oleksii, 2026-11-10). The warning ("… came over synchronized up to
    the checkpoint, with no marker of the lake past it, so the stream continues on the new timeline")
    is logged only when the final action is a resume;
  - the `moved` comment names MySQL GTID's resume on another server too; `FakeSource`'s `followed`
    is cleared by `fail_over` and `set_server`; tests for an earlier timeline and for
    `highest_marker`'s held tables and other dialects
    (`the_highest_marker_counts_held_tables_and_skips_other_dialects`);
  - a permanent sink error in `highest_marker` still ends the start (documented: declining would turn
    a sink fault into a re-snapshot or a misleading halt).
- **Task 9:** `field_docs.rs`'s `source_change_action` text (it said a later timeline halts under
  `error`) updated to the continuation, the reference regenerated. Its review's fix round: P-6's
  "(or recovers)" approved as applied; the Data-Flow Source Disconnect line's first PostgreSQL
  clause narrowed to another system or an earlier timeline (approved, P-6's companion); a 🧪 row
  for the continuation on PostgreSQL 17 folded into DA-SRC-33's rationale; the runbook's standby
  set-up gains `primary_slot_name` and `dbname`; the summaries gain the marker condition; the
  PostgreSQL alternation paragraph says `re_snapshot` and that a promoted-reconnect alternation
  halts without a code; config §15 item 32 notes the 2026-11-10 rewording.
- **Whole-branch review** (two fix rounds, 2026-11-11; `final-review.md`, ready with fixes, then
  `final-rereview.md`):
  - I-1 (`c122059`; Task 3 × DA-SNAP-15): a truncating job took the key from the catalog's schema
    now, so on a MySQL table whose key column changed case alone the sink refused the truncate for
    good (its key keeps the cached spelling) and every start ended in `ERROR`. The truncate now
    takes the schema the session copies the table under: the one this start caches, a copy's, the
    one cached before, then the catalog's. The review proposed the cached schema before a copy's;
    a copy is read under its own described schema (`session.rs`'s `copies`), so a copy's comes
    first. Test `a_truncating_job_keys_a_recased_key_as_the_sink_table_does` (RED: the start
    failed, the change set keyed by `ID` against the sink's `id`). N-1 below supersedes the
    review's optional hardening (a permanent truncate failing that table's job instead of ending
    the start): keyed as the sink table records it, a truncate no longer fails on its key;
  - m-1 (`cb11145`): the two baseline halt texts reworded (see Task 5);
  - m-2 (`7508e67`): spec patch P-7, approved 2026-11-11.
  - The re-review's second fix wave, approved by Oleksii on 2026-11-11:
    - N-1 (`695a490`; Task 3 × DA-SNAP-15): with I-1 alone, a MySQL key that changed case alone,
      together with a sink table the operator dropped and the start made again under the catalog's
      spelling, had the truncate refused for good. Every start then ended in `ERROR`, where before
      M12d one table halted. A truncate is now keyed as the sink table records its key, as the
      reposition already was (`commit_empty`). I-1's lookup is kept only for a sink table that
      records no key: there the session's spelling is the one the sink's columns have. As N-1
      landed, a table truly keyed otherwise than its sink table was emptied, then halted at its
      first data commit; since the third wave (M-A below) it halts `SINK_REJECTED` at the start
      instead, with no truncate, its rows intact and its job table failed, and since the fourth
      (m-1 below) a key that differs by case alone halts too. A truncate then reaches
      `commit_empty` only when the sink table records the session's key or none, so N-1's choice
      of the recorded key is behaviour-neutral for the truncate (kept as defence in depth); it
      stays meaningful for the reposition, which two tests pin
      (`a_source_change_repositions_a_copy_the_pipeline_doesnt_track` and
      `a_refused_reposition_halts_that_table_and_the_start_goes_on` fail when `commit_empty`
      never takes the recorded key). Test
      `a_truncating_job_keys_its_truncate_as_the_sink_table_records_it` (RED at `695a490`: the
      start failed, `identifier column "id" isn't in the schema`; after O-1 it no longer
      discriminated, see I-A below; the fourth wave turned it into the halt's test). N-2: the truncate loop's comment now follows the lookup order
      and says why a copy's schema comes before the cached one.
    - O-1 (`7fed088`; Task 3, D-M12d.3): a sink table absent at a start is made under the schema
      the session writes it with, not the catalog's. At a resume that is the cached schema, so a
      MySQL key recased while the pipeline was down no longer makes the sink table again under the
      new spelling, which halted the table at its first commit, with a clearing job no help.
      - A table whose schema changed while the pipeline was down is made under its cached schema
        too, and the stream applies the change.
      - The partition check still runs on the catalog's schema first.
      - A job's copy was already made under the schema it is read with, and a re-snapshot of a
        configured table goes through the same loop.

      Tests: `a_dropped_sink_table_is_made_again_under_the_cached_spelling` (RED: the commit was
      refused, keyed by `id` against `ID`), and the MySQL E2E
      `a_deleted_table_of_a_recased_key_is_made_again_under_the_cached_spelling` (RED: the Delta
      table was made with `ID`).
  - The second re-review's third fix wave (`final-rereview-2.md`), approved by Oleksii on
    2026-11-11:
    - I-A (`b6c9968`): after O-1, N-1's test passed with N-1 reverted (the start made the dropped
      sink table again under the cached `id`, so the lookup's key matched). The test now makes the
      sink table again by hand under the catalog's spelling (`ID`) after the drop, so the sink
      records a key the cached schema spells otherwise, and asserts the truncate committed under
      `ID` (the table's marker) with nothing halted. RED with N-1 reverted: the start failed,
      `identifier column "id" isn't in the schema`. Since the fourth wave (m-1 below) the same
      scenario halts the table instead, and the test asserts that.
    - M-A (`b6c9968`): the truncate loop compares the session's key (the I-1 lookup) with the key
      the sink table records. Two keys are the same when they name the same columns in the same
      order, compared without case where the source folds names (`same_key`, as `same_columns`
      compares, D-M12d.3). A sink table that records a key other than the session's (other
      columns, or a case difference where names don't fold) would refuse every chunk of the copy
      for good, so the table halts `SINK_REJECTED` at the start ("a truncating job asked to copy it
      again, but the pipeline keys it by [..] and its sink table by [..]: it isn't emptied; …"),
      its job table fails, and the start goes on; a copy's halt is recorded `copy: true`, as the
      session records one. As landed, a case-only difference where names fold still truncated,
      under the sink's key (N-1), then halted at its first chunk; since the fourth wave (m-1 below)
      the compare is exact and a case-only difference halts too. A sink table that records no key
      is left to the commit's checks. Test
      `a_truncating_job_halts_a_table_its_sink_table_keys_otherwise` (RED: the table was emptied,
      `left: []`, `right: [1, 2]`). The runbooks' `SINK_REJECTED` row and the truncate paragraph
      name the new halt.
    - I-B (`035aef4`): spec patch P-8, approved 2026-11-11.
    - M-B: recorded among the known limitations below.
    - M-D: the lookup's comment names its last fallback, the catalog's (`resolved_tables`).
  - The third re-review's fourth fix wave (`final-rereview-3.md`), approved by Oleksii on
    2026-11-11:
    - m-1 (`e67078c`): the truncate loop compares the session's key with the sink table's
      recorded key exactly, byte for byte (`same_key` and its fold are gone). No sink accepts a key
      that differs by case alone (Delta's and `MemorySink`'s checks are exact), and the session
      never takes the sink's spelling (`keep_names` keeps the cached one), so such a table could
      never copy: it now halts `SINK_REJECTED` at the start with its rows intact, its job table
      failed and the start Ok, as a key over other columns does. Reachable for a job's copy on
      MySQL whose key column was recased after its sink table was made, or a sink table made
      outside the pipeline. I-A's test became
      `a_truncating_job_halts_a_table_its_sink_table_keys_spelled_otherwise`: the operator's sink
      table, made under `ID` with a row in it, while the session keys `id` (RED with the fold-aware
      compare: `not emptied`, `left: []`, `right: [7]`; GREEN: the row kept, the marker unchanged,
      one `SINK_REJECTED` naming `["id"]` and `["ID"]`, the job table Failed). The runbooks say
      the Delta table is keyed otherwise than the pipeline's, by other columns or the same
      spelled otherwise.
    - m-3 (`e67078c`): test `a_truncating_job_halts_a_copy_its_sink_table_keys_otherwise` for
      M-A's copy branch: a table outside the config, copied keyed `id`, its source key moved to
      `doc`. The table isn't emptied, the halt is recorded `copy: true`, the status lists no halted
      table, and the job's table fails `SINK_REJECTED`. The snapshot-jobs runbook now points a
      copy's reader to the job's table `error` (`GET /v1/snapshots/{job_id}`), not to the halted
      tables, where a copy doesn't appear.
    - m-4 (`5a51a92`): the P-8 amendment, approved 2026-11-11 (see Spec patches).
    - m-2, m-5: recorded among the known limitations below.
    - The Exit run's flaky PostgreSQL IT (`093168b`):
      `slot::a_reconnect_to_another_server_halts_or_reruns_phase_0` failed once on PostgreSQL 14,
      dropping its slot in `cleanup` (`55006`, the slot active). The replication client's worker connects and sends `START_REPLICATION` in the background after
      `open_stream` returns, whatever its stop flag, and the test dropped each stream it opened:
      the detached worker could take the slot after the test's wait for it to go inactive had
      passed, so the next open or `cleanup` met it active. Measured with a temporary probe on an
      idle PostgreSQL 14: the slot went active 77-94 ms after the drop returned. The test now
      closes each stream (`close` stops the worker, joins it, then waits for the slot to go
      inactive). Then `slot::` passed five runs in a row on PostgreSQL 14 (17 tests each), and the
      whole `--test pg` suite once in parallel (69 tests).

**Known limitations** (recorded in the gap tracker §60.1, and in the operator docs where an operator
acts):
- a refused reposition of an untracked table is never retried (Task 6 M-1; DA-SRC-37);
- on MySQL, the new baseline arm (DA-SNAP-22) halts a column whose name differs only in case, which
  Task 3's cached arm accepts; conservative, per D-M12d.1 (DA-SNAP-23);
- the alternation count never decays: three genuine failovers days apart on an idle source halt; and
  an alternation of promoted-server reconnects halts with no code (Task 7; Task 8 re-review N1;
  DA-SRC-38);
- the failover E2E runs on PostgreSQL 18 only; 17 rests on the same mechanism, per the PostgreSQL
  docs (recorded in DA-SRC-33's rationale);
- after a crash of a busy primary the synced slot usually lags the last acknowledgement, so the start
  halts; a planned switchover continues reliably. On 17+ a down standby named in
  `synchronized_standby_slots` stalls the pipeline; a slot made before an upgrade to 17 stays plain
  (Task 8 review M6, M7; the runbook and `Postgres_Slot…` §8.5);
- when `partition_by` names a column that is in the catalog's schema but not in the cached one
  (added while the pipeline was down, or recased on MySQL with the config following the new
  spelling), a resume whose sink table is absent halts that table `SCHEMA_MISMATCH` at the start,
  since the table is made under the cached schema (O-1); a clearing job recovers it (second
  re-review M-B);
- a table halted at the start by a truncating job's key check (M-A) keeps the `in_progress` snapshot
  document the start wrote for it, against M6a's rule that a table halted in Phase 0 gets none;
  harmless, since `open` reads only tables not halted and the only way out of the halt, a job,
  rewrites the document (third re-review m-2);
- a truncate on a sink table that records no key and whose columns lack the session's key column
  (reachable for a job's copy over a foreign or older Delta table) is refused (`InvalidChangeSet`),
  and the start ends `ERROR` on every start until the job is cancelled. This predates M12d (third
  re-review m-5).

**Deferred to the whole-branch review** (the task reviews' minors): Task 1, the evolve test's `Tag`
case and the quadratic create check; Task 2, the accent-insensitive fold, two keys of one column
differing by case, `plan_table`'s rescan per key entry; Task 3, `FakeSource::fold_column_names`'
limits, `same_columns`' inline fold, `DROP a, ADD A` merged as one column, no unit test of `scope`'s
identifier remap, the fake's snapshot reader not folding; Task 4, no IT for a still-readable table, the 1049 IT reasoned RED but not run RED, and review
minors #2, #3, #4, #6, #7 left as justified in `task-4-report.md`;
Task 5, the halt texts above (reworded at the whole-branch review), no test for a partition the publication names directly, no Phase 0
test recording the non-null halt; Task 6, M-2 (the reposition halt overwrites an earlier halt), M-3
(a job's copy halted with `copy: false`), M-5 (a malformed key property fails `table_state`), M-6
(each held table keeps a loaded handle), M-7 (no RustFS/prefix IT for `held_tables`), the duplicated
`~2Epipeline-state` literal, the `st.halted.insert` line no test exercises, the brief's test filter
`held` matching no test (the test is `the_sink_lists_the_tables_it_holds`); Task 7, no test of the
reset by another end or a stop (the mutants `_ => 0` and `changed = 0` survive), and no comment on
why the final status is authoritative for `progressed`; Task 8, N2 (no
worker test of an immediate halt at a PostgreSQL reconnect) and the E2E's timing note.

## Verification in planning (2026-11-06)

Each task's code was written and run in the working tree (one target directory), saved as
cumulative stage patches in the session scratchpad, and the tree reset to the branch before the
plan was committed. What ran on the final stage:

- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`: clean.
- `cargo test --workspace` (hermetic): green after `cargo xtask docs` regenerated
  `validation-rules.md` for V-127 (`the_generated_pages_are_current` caught it, as it should).
- The mutation checks named in Tasks 5–8 each made their test fail, so those tests are known to
  discriminate. The other tests were written beside their code, not replayed failing-first task by
  task. Each task's Step 2 states the expected failure, and the executor confirms it.
- `a_promotion_the_failover_slot_followed_continues_the_stream` on the `postgres-sync-*` pair:
  passed in 47 s, after the two E2E findings recorded under "Measured in planning" (the first
  sync needs a streaming slot; a restarted node runs the stored generation) and D-M12d.11.
- The full integration run, with `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1`:
  - the workspace ITs (`-- --test-threads=1`): green;
  - `source-mysql` on `mysql-84-nogtid`, `mariadb-118` and `mariadb-118-binlog`: green;
  - `source-pg` on PostgreSQL 14: green;
  - PostgreSQL 17: green, 64 + 1, run serially. A first, parallel run failed M12b's
    `a_waiting_chunk_read_holds_up_no_other_read` at its connection count, which other tests'
    connections on the shared database move. Run alone and serially it passes; the test is
    untouched here;
  - the slice-1 E2E, with both replica pairs recreated and `DATA_AVALANCHE_PG_REPLICA`,
    `DATA_AVALANCHE_PG_SYNC_REPLICA` set: 49 passed in 694 s. That includes M12a's failover
    scenario on RustFS, which runs Task 6's held-table listing under `re_snapshot`.
