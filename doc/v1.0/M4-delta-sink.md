---
updated: 2026-10-01
domain: Sink
tags: [v1, implementation-plan, m4, delta-lake, delta-rs, sink, merge, fence, position-guard, hop-3b, schema-evolution]
sources: []
---

# M4 — Delta Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `data-avalanche-sink-delta`, the Delta Lake realization of the `Sink` trait. It
creates path-based Delta tables. It maps Hop 2 Arrow to Delta types and values (Hop 3b). It
commits each `TableChangeSet` as one MERGE, fenced by a `txn` app-transaction and positioned by
`commitInfo`, and evolves schemas within delta-rs 1.0's limits. For the same change sets it gives
the same table content as the M3 reference sink.

**Architecture:** One new crate, plus one change in `data-avalanche-storage`.

- **`data-avalanche-storage`**: table roots escape names with `~XX` instead of `%XX` (Q-M4-5).
  delta-kernel readers decode a table's path twice, so a table root must never hold a `%`.
- **`data-avalanche-sink-delta`** (new), one module per concern:
  - `config`: `DeltaSinkConfig`, the table location (`DeltaLocation`: local directory or bucket,
    config §10 via `table_root`), and the settings a new table gets (`TableDefaults`,
    `NtzAs`).
  - `types`: Hop 3b for schemas, a Hop 2 field ↔ a Delta column. Each column carries its logical
    type in its metadata (`data-avalanche.type`), so `table_state` reads the schema back in Hop 2
    terms.
  - `convert`: Hop 3b for values. uuid → text, time → text, `timestamp_ntz` three ways, dates and
    timestamps clamped to the Delta range (Q-M4-2), lists element-wise; and back.
  - `marker`: the fence (`txn` app id `data-avalanche/<pipeline_id>/owner`, version =
    `owner_epoch`) and the position (`commitInfo` keys). It also has the walk-back that finds the
    newest own commit past foreign ones.
  - `table`: `TableHandle`, one table: open, refresh, create, the Hop 2 schema, the fence epoch,
    the stored position (walk-back with a cached last-own version), and the content read for tests.
  - `write`: the MERGE of a change set (delete flag, unchanged masks, NULL for missing columns),
    the truncate DELETE, and the marker-only commit when nothing changed.
  - `evolve`: the Delta evolution outcomes (Multi_Sink §7.3, the same as `MemorySink`).
  - `sink`: `DeltaSink`, the `Sink` trait. A per-table async lock, the attempt loop (fence
    pre-check → guard → truncate → MERGE → marker) with a retry after a lost race, and
    `DeltaStats`.
- **Tests:**
  - Unit tests in each module run on a local directory.
  - The integration binary `tests/sink` runs:
    - the delta-capability probes P1–P7 and P10 as scenarios through the `Sink` trait, on a local
      directory, RustFS, Azurite and real GCS;
    - conformance with `MemorySink` through the M3 accumulator;
    - a DuckDB cross-read.

**Tech Stack:** Rust 1.98.1 (edition 2024).

| Crate | Version / features | Used for |
|---|---|---|
| `deltalake` | `=1.0.0`, `default-features = false`, features `datafusion`, `rustls`, `s3`, `gcs`, `azure` | Tables, MERGE, DELETE, commits, stores |
| `deltalake-core` | `=1.0.0`, `default-features = false` (Q-M4-1) | Pins the engine: the `deltalake` facade asks for `deltalake-core = "1.0"`, which resolves to 1.1 |
| `arrow-array`, `arrow-cast`, `arrow-row`, `arrow-schema`, `arrow-select` | `59` (59.3.0, the major deltalake 1.0 uses) | Hop 3b conversions, the content read (cast, concat, sort by key) |
| `url` | `2` | Table URLs |
| `async-trait`, `futures-util`, `serde_json`, `tokio` (`sync`), `uuid` | workspace | The trait, the log listing, the markers, the per-table locks |
| `data-avalanche-pipeline`, `data-avalanche-testkit`, `proptest`, `tempfile` | dev | Conformance through the accumulator, `MemorySink`, emulator helpers |
| `uv` + DuckDB `1.5.6` (Python) | CI tool | The DuckDB cross-read (R-12), run by `uv run --with duckdb==1.5.6` |

`deltalake` 1.0 brings DataFusion 55, `object_store` 0.13.2 (R-13: a second copy next to ours at
0.14.2) and its own delta-kernel fork. Dependencies build without debug info (`[profile.dev.package."*"]
debug = false`): DataFusion's debug build alone is 6.4 GB. With it, a cold workspace build takes
about 8 minutes and `target/` about 14 GB (measured 2026-09-30).

**Spec:**
- `doc/v1.0/00-roadmap.md`: §2 (layout, dependency direction), §2.1 (pins), §3.4 (the `Sink`
  trait), §3.5 (`table_root`), §4 M4 scope and exit, D-IMPL-3, D-IMPL-7, R-5, R-6, R-10, R-12, R-13.
- `doc/specs/Data-Flow.md`: Phase 4 (the sink contract, the position guard, commit failures),
  §Delta Lake realization (items 1–6).
- `doc/specs/Configuration_Specification.md`: §9.4.3 (`sink.delta`: `checkpoint_interval`,
  `log_retention`, `timestamp_ntz_as`, `table_properties`), §10 (table locations), V-092, V-093.
- `doc/specs/Schema_Change_Detection_and_Evolution.md`: §7.1.2 and §7.2 (the Delta column), §8.3
  (idempotent apply).
- `doc/research/Multi_Sink_Architecture_and_Delta_Lake.md`: §6.3–6.5, §7.2–7.3, D-MS.9, D-MS.14–16.
- `doc/research/Type_Mapping.md`: Hop 2, Hop 3b, §Date/time edge values.
- `doc/research/Delivery_Guarantee_and_Position_Guard.md` §3; gap DA-X-10 (walk-back past foreign
  commits).
- `prototypes/delta-capability/README.md`: P1–P10, the evidence this crate re-runs.

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. `cargo clippy --workspace
  --all-targets -- -D warnings` and `cargo fmt --all --check` must pass before every commit.
- **Dependency direction** (roadmap §2): `sink-delta → core, storage`. `pipeline` and `testkit`
  are **dev**-dependencies only. Nothing depends on `sink-delta` in M4.
- **Pins:** `deltalake = "=1.0.0"` and `deltalake-core = "=1.0.0"`. Arrow sub-crates at major `59`,
  never the `arrow` umbrella crate. `Cargo.lock` must keep a single arrow 59.
- **Names** (D-IMPL-7): the app id `data-avalanche/<pipeline_id>/owner`; the `commitInfo` keys
  `data-avalanche.committed_position`, `data-avalanche.owner_epoch`, `data-avalanche.pipeline`; the
  MERGE flag `__data_avalanche_del`; the column metadata key `data-avalanche.type`; the table
  property `data-avalanche.identifier`.
- **Test gates:** plain `cargo test` stays hermetic (local directories only). Object-store tests
  gate on `DATA_AVALANCHE_IT`, GCS also on `DATA_AVALANCHE_SKIP_GCS`. The DuckDB test gates on
  `DATA_AVALANCHE_IT` and needs `uv`.
- **Credentials:** tests take cloud credentials from the environment only. No key, key path or
  secret goes in code, docs or output. The real-GCS run is Oleksii's, started with `!`.
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `SinkError` (core), classified `Permanent` or `Transient`.
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to break a mirror. Each is pinned by a test in the task after
the arrow.

1. **Another writer between two of our commits**: an external OPTIMIZE, a checkpoint, or
   expired-log cleanup. The fence and the guard must still hold. The position is read by walking
   back past foreign commits; the `txn` epoch lives in the checkpoint → Task 9
   (`a_race_with_a_foreign_optimize_is_retried_and_lands`), Task 10
   (`fence_and_guard_survive_a_foreign_optimize`, `checkpoints_and_log_cleanup_keep_the_marker`).
2. **A stale owner (zombie) racing the new owner** on the same table. It must end `Fenced`, never
   overwrite the newer epoch's rows → Task 9
   (`a_zombie_that_loses_the_race_is_fenced_on_the_retry`).
3. **Values at the edges of what Delta can hold.** These are PostgreSQL `±infinity` (clamped to
   the Arrow range in Hop 1), `i64` extremes, `u64::MAX`, 38-digit decimals, times beyond 24 h,
   non-UTF-8 bytes and NULL everywhere. Each must be written, and read back as the reference sink
   holds it (clamped to the Delta range where Delta can't store it) → Task 4
   (`dates_and_timestamps_clamp_to_the_delta_range`, `time_and_ntz_text_round_trip`), Task 13
   (`every_type_round_trips_under_every_timestamp_ntz_option`, `duckdb_reads_the_table`).
4. **Table names that need escaping**: `%`, `/`, `~`, `.`/`..`, a space, non-ASCII text. The
   table must be created, written and found again under the key config §10 names. It must also be
   readable by a delta-kernel reader → Task 1 (`a_table_root_never_holds_a_percent`), Task 2
   (`escaped_names_keep_the_storage_crates_segments`), Task 10
   (`escaped_names_land_under_the_storage_crates_keys`, and the scenario on every store in Task 12),
   Task 13 (`duckdb_reads_the_table`).
5. **A change set that changes no row.** This covers an empty set, deletes of absent keys, and
   rows whose every column is unchanged. The marker must still move (D-IMPL-2: idle tables advance
   too), or the guard stays behind and the checkpoint rule stalls → Task 7
   (`a_merge_that_changes_nothing_commits_nothing_so_the_marker_goes_alone`), Task 10
   (`masks_and_empty_sets`).

## Decisions and rulings

**Asked and answered (2026-09-30):**

| # | Question | Answer |
|---|---|---|
| Q-M4-1 | `deltalake` 1.0.0 pulls `deltalake-core` 1.1.0; pin the probed engine? | **`deltalake =1.0.0` with `deltalake-core =1.0.0` pinned directly** (the engine P1–P10 ran on) |
| Q-M4-2 | delta-rs panics writing `Date32` `i32::MIN` (PG `-infinity` after Q-M5-1's clamp), and readers can't show years beyond 9999 | **Clamp `date`, `timestamp` and `timestamp_ntz` cells to the Delta protocol range**, 0001-01-01 … 9999-12-31 23:59:59.999999, in Hop 3b. PG `±infinity` lands on the bounds. This revises Q-M5-1 for the Delta sink; `Type_Mapping.md` and roadmap §9.1 record it |
| Q-M4-3 | Truncate as a DELETE commit then a MERGE (two commits), or one commit? | **As the spec says: a DELETE commit, then the MERGE.** No spec change |
| Q-M4-4 | Where do the M4 integration tests run? | **Local directory and RustFS (the exit), plus Azurite, the DuckDB cross-read in CI, and real GCS in PR CI** |
| Q-M4-5 | delta-kernel (delta-rs and DuckDB) decodes a table's path twice, so M2's `%XX` escape breaks any table named with `%`, `/`, a control character, `.`/`..`, or the schema `.pipeline-state` | **Table roots escape with `~XX`**: `%`, `/`, `~` and control characters per UTF-8 byte, `.`/`..` as `~2E`, the schema `.pipeline-state` as `~2Epipeline-state`. State keys and snapshot file names keep `%XX` (D-M2.4 stands for them). No released tables exist, so nothing migrates |
| Q-M4-6 | DuckDB 1.5.6 can't open a Delta table whose path has non-ASCII text (it drops that segment when opening data files); delta-rs can | **Keep non-ASCII names verbatim.** The operator docs (M11, R-12) list DuckDB's limit with the engines checked |

**Rulings where the specs are silent** (each is pinned by a test). **Plan approved 2026-09-30**, with these rulings (roadmap D-IMPL-11):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M4.1 | Every column the sink creates carries its logical type in the Delta column metadata (`data-avalanche.type`, the `LogicalType` text), plus the Hop 2 tag (`data-avalanche.logical`) when there is one. `table_state.schema` is read back through it, in Hop 2 terms. A column without the metadata (a foreign one) maps from its Delta type | Delta has no `uuid` or `time`, and `timestamp_ntz` can be stored three ways, so a Delta type alone doesn't give back the Hop 2 field. The core contract says `SinkTableState.schema` is in Hop 2 terms | Task 3 |
| D-M4.2 | Hop 3b types. `uuid` and `time` → `string`. `uint64` → `decimal(20,0)` (as in Hop 2). `timestamp_ntz` → `timestamp_ntz` / `timestamp` (the same µs as UTC) / `string` (`YYYY-MM-DD HH:MM:SS.ffffff`, over all of `i64`), per `timestamp_ntz_as` at creation; an existing column keeps its type. Lists element-wise. A negative decimal scale is refused (`Permanent`): Delta requires scale ≥ 0 | D-MS.16 and config §9.4.3; M5's Hop 1 widens `numeric(p,-s)` to scale 0 (noted for the M5 plan) | Task 3, Task 4 |
| D-M4.3 | Hop 3b values. uuid → canonical lowercase text; time → `time_text`, so intervals beyond a day keep their value. Date and timestamp cells are clamped to the Delta range (Q-M4-2). Reading back accepts view and dictionary arrays (DataFusion scans return them) | The round trip must equal the reference sink after the clamp | Task 4 |
| D-M4.4 | The MERGE is matched **on the identifier only**, with no partition pruning. The change set carries no before-image partition values for upserts, so pruning by the new values would miss a row that moved partition. Delete rows carry the identifier only. A masked column updates with `CASE WHEN mask THEN target.col ELSE source.col END` (D-IMPL-3). A table column the set lacks is a typed NULL. Inserts set every table column | Data-Flow §Delta item 3: "a partition-changing UPDATE is matched by key across partitions". The spec's pruning clause is optional, and pruning without the old values would be wrong | Task 7 |
| D-M4.5 | The fence is a pre-check (the snapshot's `txn` version for our app id; greater than ours → `Fenced`) plus the `txn` on every commit. A commit that loses a race is retried up to `COMMIT_ATTEMPTS` = 3 times. That covers delta-rs `CommitConflict` (including `ConcurrentTransaction`, the same app id committed since our read), `MaxCommitAttempts`, `VersionAlreadyExists` and `VersionMismatch`. Each retry re-reads the table and runs the pre-check again, so a zombie ends `Fenced`. Three lost races in a row → `Transient` | Data-Flow §Delta item 1. A retry must never write over a newer epoch | Task 9 |
| D-M4.6 | The position lives in `commitInfo` (`committed_position` tagged per D-IMPL-4, `owner_epoch`, `pipeline`). The walk-back reads commit files from the latest version down to the version of the last own commit this handle knows. It reads at most `MAX_WALK_BACK` = 1000 commits and stops at a commit file that is gone (log cleanup). Where the walk ends without a newer own commit, the cached position stands. A commit of our pipeline without a position (a truncate's DELETE) is walked past. A marker that doesn't parse is `Permanent` | Gap DA-X-10: foreign commits must not hide the position. An older position is still a safe guard, because it skips less. Without any position the guard skips nothing and the replay is an idempotent re-write | Task 5, Task 6, Task 10 |
| D-M4.7 | The stored position is the union of the stored one and the change set's end (D-M3.16). The sink refuses a marker whose position isn't the change set's end, or whose pipeline isn't the sink's (`Permanent`) | One guard rule, the same as the reference sink; the refusals catch orchestrator bugs | Task 9 |
| D-M4.8 | A truncate is a DELETE commit carrying the `txn` but no position, then the MERGE carrying the position (Q-M4-3). A crash between them replays both | The walk-back never stops at the DELETE, so the guard can't skip the rows that follow it | Task 7, Task 10 |
| D-M4.9 | A change set that changes no row (no rows, deletes of absent keys, a no-op MERGE writes no version) gets a **marker-only commit**: a `StreamingUpdate` commit carrying only the `txn` and the `commitInfo` | D-IMPL-2: every table of a flush records the flush frontier, idle ones included | Task 7, Task 10 |
| D-M4.10 | `ensure_table` creates the table with column mapping off, `delta.enableDeletionVectors=false`, `delta.checkpointInterval`, `delta.logRetentionDuration` (`interval N seconds`), the passthrough `table_properties`, the identifier as the table property `data-avalanche.identifier` (JSON), the name `<schema>.<table>`, and identity partitions only (another transform is `Permanent`; the capabilities declare only `Identity`). A table another writer created first is kept. A root holding Iceberg metadata (`metadata/` or `meta/…metadata.json`) is `Permanent` | Data-Flow §Delta item 5; config §9.4.3; the identifier property lets the content read sort by key without the `TableSchema` | Task 6 |
| D-M4.11 | `evolve` follows Multi_Sink §7.3, the same outcomes as `MemorySink` (D-M3.17). ADD COLUMN → `Applied`: nullable, Hop 3b type, idempotent. DROP → `Degraded`: soft-drop. RENAME → `Degraded`: the new name is added with the old column's type and metadata, and the old column is kept. Nullability, appended enum values and changes with no target effect → `Applied`, nothing written. A type change, an identifier change, a dropped table, or an ADD of an existing column with another type → `Unsupported` | delta-rs 1.0 can only add columns (P7a); a replayed delta must add nothing (§8.3) | Task 8, Task 10 |
| D-M4.12 | Errors. `Permanent`: schema mismatch, invalid data, metadata, commit validation, unsupported column mapping, a missing feature, an invalid location, not a table, and the transaction errors for table features and append-only tables. Everything else is `Transient`, storage and network errors included | Data-Flow §Sink Commit Failure: the pipeline retries `Transient` with backoff and halts the table on `Permanent` | Task 6 |
| D-M4.13 | Locations. The table URL is built from `table_root` segments (percent-encoded for the URL). A local root is created before opening, because delta-rs refuses a missing directory. The cloud handlers are registered once. Azure merges the `AZURE_*` variables into the options (delta-rs doesn't read them; explicit options win), and S3 and GCS read their own. `Debug` hides option values | Config §10; credentials only from the environment | Task 2, Task 6 |
| D-M4.14 | `DeltaStats` counts commits, skips, fences, retries and the MERGE metrics (rows inserted, updated, deleted, copied; files added, removed); M10 exports them. `merge_memory_limit` bounds DataFusion's memory for one MERGE. A MERGE over it fails `Transient` ("Resources exhausted") instead of growing the process. There is no config key for it yet: M6 sets it, and `None` means no ceiling | Data-Flow §Delta item 4 metrics; R-6 | Task 7, Task 9, Task 10 |
| D-M4.15 | Calls on one table run one at a time (a per-table async mutex); different tables run concurrently | The handle caches the last own version; two commits to one table from one sink would race each other for nothing | Task 9 |

**Documents this plan changes** (applied at plan approval, 2026-09-30, in a `research:` commit and
an `infra:` commit; `test-infra/README.md` changes in Task 13):

- `Type_Mapping.md` §Date/time edge values and Hop 3b: the Delta clamp (Q-M4-2); `time`/`uuid` as
  text, the three `timestamp_ntz` forms; a negative decimal scale is refused at Hop 3b, and M5 widens
  it in Hop 1.
- Roadmap:
  - §2.1: the `deltalake-core =1.0.0` pin and why.
  - §3.5: `escape_table_segment` and `table_root`'s `~XX`.
  - §9.1: Q-M4-1…6.
  - R-12: DuckDB's non-ASCII limit and the clamp.
  - R-13: re-checked at M4, since `deltalake` 1.1.0 still needs `object_store ^0.13.2`.
  - At exit, a new §3.10 for the Delta sink's landed interface.
- `M2-storage-and-state.md`: a line under D-M2.4 pointing at Q-M4-5 for table roots.
- `test-infra/README.md` (Task 13): the DuckDB test needs `uv`.

No spec edit is needed. The spec's MERGE pruning clause is optional (D-M4.4), and truncate follows
the spec (Q-M4-3).

## Deviations during execution

The task code blocks below are the plan as approved. Where a review changed the code afterwards,
the landed code differs; `git log` on `feat/claude/m4-delta-sink` is the record.

- **T4, review follow-up (owner's ruling, 2026-09-30; `c579b2b`).** The plan's `parse_ntz_text` and
  `parse_time_text` panicked on malformed but parseable text. A huge year overflowed
  `days_from_civil`, a 30-digit hour overflowed the time arithmetic, and a month of `u32::MAX`
  overflowed `m + 9`. `2024-02-31` was accepted as another date. They are reached from `from_delta`
  when a Delta string column is read back. Now:
  - the time parts must be non-negative, and their arithmetic is checked;
  - the year must be within ±300 000 (`MAX_TEXT_YEAR`), the month 1–12 and the day 1–31;
  - the date must convert back to itself, which rejects days the month doesn't have;
  - `to_delta` returns an error for a uuid column that doesn't hold 16-byte values.

  A new test, `malformed_text_is_an_error_not_a_panic`, covers these. It panicked on the plan's
  code. The unit-test count after T4 is 19, one above the plan's 18. The later tasks' counts are
  one higher too: 23 after T5 and 30 after T6 (T7's deviation below adds one more).
- **T7, review follow-up (owner's ruling, 2026-09-30; `5c46dc0`).** Two defects in the plan's `write.rs`:
  - `merge` wrapped its own deterministic failures (building the MERGE source, a column's physical
    type) as `DeltaTableError::Generic`, which `sink_error` classes `Transient`: such a change set
    would have retried forever instead of halting the table. They are `InvalidData` now (Permanent).
  - The MERGE ran on a bare DataFusion `SessionContext`, not delta-rs's `DeltaSessionContext` (its
    `DeltaPlanner` and tuned settings, including a workaround for a hash-join panic on dictionary
    keys). The probes ran on delta-rs's session. `session()` now builds `DeltaSessionContext`, with
    the memory ceiling through `with_runtime_env`.

  A new test, `a_change_set_the_merge_cant_convert_is_permanent`, came back `Transient` on the plan's
  code. The fix was verified first on the milestone's final code: 44 unit tests and all 15
  integration tests pass, P6 counts unchanged. Unit counts: 39 after T7, 42 after T8, 45 after T9.
- **Final review fix wave (owner's rulings, 2026-10-01; `901ab43`…`4023bf7`).** The whole-branch
  review found no Critical item, two Important ones and eight minor ones. It also confirmed with
  a probe that one parked item was data loss. Eight commits fix them:
  - **A re-created table was skipped (`901ab43`).** `TableHandle` cached the last own version
    and position (`known`) without the table's identity. If the table was deleted and re-created,
    the walk-back stopped at the cached version, and the next commit returned `SkippedCovered`
    onto an empty table. `known` now carries the table id (`metadata().id()`). It is cleared when
    `refresh` finds no table and by `create`, and it is ignored when the id differs or its version
    is past the latest.
    - A handle behind another sink's re-create now recovers in `fence_epoch`. delta-rs 1.0's
      `update_incremental` returns `Ok` on the stale snapshot, so the `txn` read is where it
      fails; the handle re-opens the table once and reads again.
    - One case is left to M6, by the owner's ruling: a table re-created behind a live handle whose
      new log has already passed the handle's version. Every commit then fails `Transient` until
      the handle is re-opened. Nothing is lost.
  - **Failures that always repeat were `Transient` (`9e1576e`).** A MERGE that breaks a Delta rule
    (a NULL in a NOT NULL column, a CHECK constraint) comes back from delta-rs as `Generic`.
    `sink_error` now classes its "validation check" text as `Permanent`, and a test builds the
    real error. `check_columns` refuses, as `Permanent`, a set that lacks a table column that
    can't be NULL (the identifier, the lineage columns). `MemorySink` accepts such a set; the
    Delta table can't store it.
  - **D-M4.5 is narrowed (`9e1576e`).** A retry follows only a real race: `CommitConflict`
    `ConcurrentAppend`, `ConcurrentDeleteRead`, `ConcurrentDeleteDelete`, `MetadataChanged`,
    `ConcurrentTransaction` or `ProtocolChanged`, `MaxCommitAttempts`, and `VersionAlreadyExists`.
    `Predicate` and the other conflict variants are `Permanent`. The `VersionMismatch` arm is gone:
    deltalake-core 1.0 never raises it.
  - **D-M4.8 gains a retry (`4023bf7`).** deltalake-core 1.0's conflict checker skips actions with
    `data_change=false` (`conflict_checker.rs`), which is what OPTIMIZE writes. A truncate's DELETE
    built before a foreign OPTIMIZE therefore committed without a conflict, and the compacted file
    survived with the old rows. After the DELETE, a table that still holds files is now a lost race
    (`Step::Retry`): the retry re-reads, fences, truncates again and lands the MERGE. A truncate on an
    empty table still adds no commit.
  - **Core: a change set needs an identifier (`22a676c`).** `TableChangeSet::validate()` refuses an
    empty `identifier` (a table without a key halts first, Ghost_Row_Dedup §3), and the Delta sink
    returns `Permanent` instead of panicking.
  - **Bad input is an error, not a panic (`6b221aa`).** A delete batch whose key types differ from
    the schema is `Permanent`. `DeltaLocation::table_url` returns `Result<Url, SinkError>`, so a
    relative local root or an invalid bucket host is `Permanent`. Azure options come from
    `std::env::vars_os()`, skipping non-UTF-8 pairs. **D-M4.13 corrected:** delta-rs 1.0 does read
    the Azure credential variables. Only the others (`use_emulator`, the endpoint, …) are missing,
    so merging every `AZURE_*` variable stays harmless.
  - **`convert`, `marker` and `types` are crate-private (`cd76ee5`).** The plan made them public,
    which exposed `Result<_, String>` and deltalake's `LogStore` (CLAUDE.md: libraries return
    `thiserror` errors). The crate root re-exports what tests and later milestones use:
    `DELTA_MIN_DATE`, `DELTA_MAX_DATE`, `DELTA_MIN_MICROS`, `DELTA_MAX_MICROS`, `POSITION_KEY`,
    `EPOCH_KEY`, `PIPELINE_KEY`, `TYPE_KEY` and `app_id`.
  - **Tests and tidies (`66992db`, `9c1ec79`).**
    - New tests: the truncate crash replay (the DELETE landed, the MERGE not), a check that the
      sink's own DELETE carries the fence but no position, three lost races (`Transient`,
      `retries == 2`), and a truncate that loses a race.
    - Literal pins for the Delta bounds.
    - Truncates are counted in `DeltaStats`, and the `txn` version is computed once.
    - Comment and docstring fixes.

  Workspace: 519 tests pass (499 at plan time). The emulator run passes 59 unit and 18
  integration tests on local, RustFS, Azurite and DuckDB.
- **Carried to M6 (final review, 2026-10-01).** These are recorded in the roadmap's M6 scope:
  - a MERGE over `merge_memory_limit` is `Transient` but fails the same way every time (D-M4.14);
  - `table_state`'s `marker: None` doesn't mean "no own commit" (a truncate's DELETE carries none);
  - the stale live handle above, and two cases of the same kind: `evolve` on a stale live handle
    (it doesn't go through `fence_epoch`'s re-open), and a table dropped between `refresh` and the
    re-open (delta-rs's `NotATable` is `Permanent`);
  - `commit` doesn't compare `cs.identifier` with the table's recorded identifier (owner's ruling:
    M6 owns identifier changes, parity with `MemorySink`);
  - `evolve` carries no fence, because `Sink::evolve` takes no marker.

---

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` | Workspace: the new crate; `arrow-cast`, `deltalake`, `deltalake-core`, `url`; no debug info for dependencies |
| `crates/data-avalanche-storage/src/paths.rs`, `…/lib.rs` | `escape_table_segment` (`~XX`); `table_root` uses it (Q-M4-5) |
| `crates/data-avalanche-sink-delta/Cargo.toml` | The crate and its dependencies |
| `…-sink-delta/src/lib.rs` | Modules, root re-exports, crate docs |
| `…-sink-delta/src/config.rs` | `DeltaSinkConfig`, `DeltaLocation` (`table_url`, `storage_options`), `TableDefaults`, `NtzAs` |
| `…-sink-delta/src/types.rs` | Hop 3b schemas: `delta_type`, `delta_field`, `physical_type`, `column_logical`, `hop2_field`, `hop2_schema`, `logical_of`, `TYPE_KEY` |
| `…-sink-delta/src/convert.rs` | Hop 3b values: `to_delta`, `from_delta`, `ntz_text`, `parse_ntz_text`, `parse_time_text`, the Delta range constants |
| `…-sink-delta/src/marker.rs` | `app_id`, `commit_metadata`, `own_position`, `walk_back`, `WalkBack`, the `commitInfo` keys, `MAX_WALK_BACK` |
| `…-sink-delta/src/error.rs` | `permanent`, `sink_error` (delta-rs error → `SinkError`) |
| `…-sink-delta/src/table.rs` | `TableHandle` (open, refresh, create, schema, identifier, fence epoch, stored position, content), `IDENTIFIER_KEY` |
| `…-sink-delta/src/write.rs` | `check_columns`, `merge_source`, `merge`, `truncate`, `marker_only`, `session`, `DELETE_FLAG` |
| `…-sink-delta/src/evolve.rs` | `evolve` |
| `…-sink-delta/src/sink.rs` | `DeltaSink` (the `Sink` impl, `content`, `stats`), `DeltaStats`, `COMMIT_ATTEMPTS` |
| `…-sink-delta/src/test_support.rs` | Unit-test fixtures: the `orders` table, change sets, markers |
| `…-sink-delta/tests/sink/main.rs` | The integration binary |
| `…/tests/sink/common.rs` | Fixtures: the `items` table, change sets, commit and read helpers |
| `…/tests/sink/scenarios.rs` | P1–P5, P7a, masks, empty sets, escaped names: one function per scenario, any location |
| `…/tests/sink/local.rs` | The scenarios on a local directory; P7b canary; checkpoints and log cleanup; P6 cost |
| `…/tests/sink/conformance.rs` | The Delta sink equals `MemorySink` (property test through the accumulator) |
| `…/tests/sink/stores.rs` | The scenarios on RustFS, Azurite and real GCS |
| `…/tests/sink/types.rs` | P10: every Hop 2 type round-trips, per `timestamp_ntz_as`; the DuckDB cross-read |
| `…-sink-delta/tests/duckdb_read.py` | Prints a Delta table as JSON text cells via DuckDB `delta_scan` |
| `.github/workflows/ci.yml` | `astral-sh/setup-uv` in the integration job |
| `test-infra/README.md` | The `uv` requirement |

---

### Task 1: Table roots escape with `~XX` (Q-M4-5)

**Files:**
- Modify: `crates/data-avalanche-storage/src/paths.rs`
- Modify: `crates/data-avalanche-storage/src/lib.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces (storage crate root):
  - `escape_table_segment(&str) -> String`: `%`, `/`, `~` and control characters → `~XX` per
    UTF-8 byte; `.`/`..` → `~2E`/`~2E~2E`; everything else kept.
  - `table_root(bucket, prefix, schema, table)` uses it. A schema named `.pipeline-state` becomes
    `~2Epipeline-state`. The result never contains `%`.
  - `escape_segment` is unchanged (`%XX`): state keys and snapshot file names keep it.

delta-kernel decodes a table's path a second time, so `a%25b` is read as `a%b`. The table
is created under one key and read under another: `Kernel error: File not found: …/a%b/_delta_log/00000000000000000000.json`.
DuckDB's `delta_scan` fails the same way. Both were measured on 2026-09-30, and this plan's Q-M4-5
records the choice. `escape_segment` and `escape_table_segment` share one helper that takes the
escape character.

- [ ] **Step 1: Change the tests to the `~XX` form**

Change `crates/data-avalanche-storage/src/paths.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/paths.rs b/crates/data-avalanche-storage/src/paths.rs
--- a/crates/data-avalanche-storage/src/paths.rs
+++ b/crates/data-avalanche-storage/src/paths.rs
@@ -131,16 +131,42 @@
             table_root("b", "p", "продажи", "Заказы 2026").root,
             "p/продажи/Заказы 2026"
         );
-        assert_eq!(table_root("b", "p", "a/b", "50%").root, "p/a%2Fb/50%25");
-        assert_eq!(table_root("b", "p", "..", ".").root, "p/%2E%2E/%2E");
-        assert_eq!(table_root("b", "p", "s", "t\n").root, "p/s/t%0A");
+        assert_eq!(table_root("b", "p", "a/b", "50%").root, "p/a~2Fb/50~25");
+        assert_eq!(table_root("b", "p", "..", ".").root, "p/~2E~2E/~2E");
+        assert_eq!(table_root("b", "p", "s", "t\n").root, "p/s/t~0A");
+        assert_eq!(
+            table_root("b", "p", "~user", "a~2F").root,
+            "p/~7Euser/a~7E2F"
+        );
+    }
+
+    #[test]
+    fn a_table_root_never_holds_a_percent() {
+        for name in [
+            "50%",
+            "%2F",
+            "a/b",
+            "~",
+            "..",
+            "t\u{7f}",
+            "Заказы 2026",
+            "a b+c#d?e",
+        ] {
+            let root = table_root("b", "p", name, name).root;
+            assert!(!root.contains('%'), "{name:?} → {root:?}");
+        }
+        assert_eq!(
+            escape_segment("a/b 50%"),
+            "a%2Fb 50%25",
+            "state keys keep `%XX`"
+        );
     }
 
     #[test]
     fn a_schema_named_like_the_state_marker_never_lands_in_colocated_state() {
         let state = colocated_state("lake", "p").root;
         let root = table_root("lake", "p", COLOCATED_STATE_DIR, "replication").root;
-        assert_eq!(root, "p/%2Epipeline-state/replication");
+        assert_eq!(root, "p/~2Epipeline-state/replication");
         assert!(!root.starts_with(&format!("{state}/")));
         // Only the schema segment can collide; the rest map as before.
         assert_eq!(
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-storage --lib paths`

Expected: FAIL: `a_schema_named_like_the_state_marker_never_lands_in_colocated_state`, `a_table_root_never_holds_a_percent`, `table_root_escapes_only_what_breaks_a_segment`.

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-storage/src/paths.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/paths.rs b/crates/data-avalanche-storage/src/paths.rs
--- a/crates/data-avalanche-storage/src/paths.rs
+++ b/crates/data-avalanche-storage/src/paths.rs
@@ -27,15 +27,29 @@
 /// `%XX`, and a name of `.` or `..` has its dots escaped. Everything else (including `.` inside a
 /// name and non-ASCII text) is kept, so ordinary names map to themselves.
 pub fn escape_segment(ident: &str) -> String {
+    escape_with(ident, '%')
+}
+
+/// Makes a database identifier safe as one segment of a table root: `%`, `/`, `~` and control
+/// characters become `~XX`, and a name of `.` or `..` has its dots escaped. A table root never
+/// holds a `%`: delta-kernel readers (delta-rs, DuckDB) percent-decode a table's path a second
+/// time, so `a%25b` would be read as `a%b` (Q-M4-5). Ordinary names map to themselves.
+pub fn escape_table_segment(ident: &str) -> String {
+    escape_with(ident, '~')
+}
+
+/// `mark` followed by two upper-case hex digits per UTF-8 byte, for `%`, `/`, `mark` itself and
+/// control characters.
+fn escape_with(ident: &str, mark: char) -> String {
     if ident == "." || ident == ".." {
-        return ident.replace('.', "%2E");
+        return ident.replace('.', &format!("{mark}2E"));
     }
     let mut out = String::with_capacity(ident.len());
     for c in ident.chars() {
-        if c == '%' || c == '/' || c.is_control() {
+        if c == '%' || c == '/' || c == mark || c.is_control() {
             let mut buf = [0u8; 4];
             for byte in c.encode_utf8(&mut buf).bytes() {
-                out.push_str(&format!("%{byte:02X}"));
+                out.push_str(&format!("{mark}{byte:02X}"));
             }
         } else {
             out.push(c);
@@ -44,16 +58,17 @@
     out
 }
 
-/// `<sink.bucket>/<sink.prefix>/<schema>/<table>/`: one table's root, for every sink format. For
-/// MySQL, `schema` is the database (`source.db`). A schema named exactly
-/// [`COLOCATED_STATE_DIR`] has its leading dot escaped (`%2Epipeline-state`), so a table never
-/// lands inside co-located pipeline state under the same `sink.prefix`.
+/// `<sink.bucket>/<sink.prefix>/<schema>/<table>/`: one table's root, for every sink format, with
+/// each name through [`escape_table_segment`]. For MySQL, `schema` is the database (`source.db`).
+/// A schema named exactly [`COLOCATED_STATE_DIR`] has its leading dot escaped
+/// (`~2Epipeline-state`), so a table never lands inside co-located pipeline state under the same
+/// `sink.prefix`.
 pub fn table_root(sink_bucket: &str, sink_prefix: &str, schema: &str, table: &str) -> Location {
-    let schema = match escape_segment(schema) {
-        s if s == COLOCATED_STATE_DIR => format!("%2E{}", &s[1..]),
+    let schema = match escape_table_segment(schema) {
+        s if s == COLOCATED_STATE_DIR => format!("~2E{}", &s[1..]),
         s => s,
     };
-    let table = escape_segment(table);
+    let table = escape_table_segment(table);
     Location {
         bucket: sink_bucket.to_owned(),
         root: join(&[sink_prefix, &schema, &table]),
```

Change `crates/data-avalanche-storage/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/lib.rs b/crates/data-avalanche-storage/src/lib.rs
--- a/crates/data-avalanche-storage/src/lib.rs
+++ b/crates/data-avalanche-storage/src/lib.rs
@@ -7,7 +7,8 @@
 //!   and state: a hung store fails within about 8 s.
 //! - **Where things live:** [`table_root`], [`colocated_state`], [`explicit_state`] and
 //!   [`globals_state`] give a [`Location`] (bucket + key prefix, `""` = bucket root).
-//!   [`escape_segment`] makes a database identifier safe as a key segment.
+//!   [`escape_segment`] makes a database identifier safe as a key segment, and
+//!   [`escape_table_segment`] as a segment of a table root.
 //! - **Compare-and-swap:** [`ObjectStoreVersioned`] roots a [`VersionedStore`] at a
 //!   [`Location`]'s prefix: `get`, `create` (if absent), `update` (if the version still matches),
 //!   `put`, `list`. Failures are a [`StoreError`]: `Conflict` (the condition failed; also an update
@@ -24,7 +25,7 @@
 
 pub use backend::{Backend, BucketSpec, OpenError, Timeouts, open_bucket};
 pub use paths::{
-    COLOCATED_STATE_DIR, Location, colocated_state, escape_segment, explicit_state, globals_state,
-    join, table_root,
+    COLOCATED_STATE_DIR, Location, colocated_state, escape_segment, escape_table_segment,
+    explicit_state, globals_state, join, table_root,
 };
 pub use store::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-storage --lib paths`

Expected: PASS. `data-avalanche-storage` `paths` tests: 9.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-storage/src/paths.rs \
  crates/data-avalanche-storage/src/lib.rs
git commit -m "fix(storage): table roots escape names as ~XX, never %"
```

---

### Task 2: The crate, its dependencies and the sink configuration

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/data-avalanche-sink-delta/Cargo.toml`
- Create: `crates/data-avalanche-sink-delta/src/lib.rs`
- Create: `crates/data-avalanche-sink-delta/src/config.rs`

**Interfaces:**
- Consumes: `table_root`, `Backend` (storage); `TableRef` (core).
- Produces (crate root):
  - `NtzAs::{TimestampNtz (default), Timestamp, String}`.
  - `TableDefaults { checkpoint_interval: u64, log_retention: Duration, timestamp_ntz_as: NtzAs,
    table_properties: BTreeMap<String, String> }`, `Default` = 10, 30 days, `TimestampNtz`, `{}`.
  - `DeltaLocation::{Local { root: PathBuf }, Bucket { backend, bucket, prefix, options:
    Vec<(String, String)> }}` with `table_url(&TableRef) -> Url` (trailing `/`) and
    `storage_options(env) -> HashMap<String, String>`. `Debug` hides option values.
  - `DeltaSinkConfig { pipeline_id, location, table: TableDefaults, merge_memory_limit:
    Option<usize> }`.

The crate's `Cargo.toml` lists every dependency the milestone needs, so later tasks don't touch
it. The first build compiles DataFusion: expect about 8 minutes cold. The workspace profile
drops debug info for dependencies (see Tech Stack), which keeps `target/` near 14 GB instead of
filling a CI runner. `Cargo.lock` gains deltalake 1.0.0, deltalake-core 1.0.0, DataFusion 55 and
a second `object_store` (0.13.2, R-13).

- [ ] **Step 1: Add the workspace dependencies, the crate and the configuration tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -23,6 +23,7 @@
 data-avalanche-storage = { path = "crates/data-avalanche-storage" }
 data-avalanche-state = { path = "crates/data-avalanche-state" }
 data-avalanche-pipeline = { path = "crates/data-avalanche-pipeline" }
+data-avalanche-sink-delta = { path = "crates/data-avalanche-sink-delta" }
 thiserror = "2"
 serde = { version = "1", features = ["derive"] }
 serde_json = "1"
@@ -51,3 +52,14 @@
 arrow-row = "59"
 arrow-schema = "59"
 arrow-select = "59"
+arrow-cast = "59"
+# The Delta sink (roadmap §2.1). `deltalake` asks for `deltalake-core = "1.0"`, which resolves to
+# 1.1; the direct pin keeps the engine the probes ran on (delta P1–P10, 2026-09-26).
+deltalake = { version = "=1.0.0", default-features = false, features = ["datafusion", "rustls", "s3", "gcs", "azure"] }
+deltalake-core = { version = "=1.0.0", default-features = false }
+url = "2"
+
+# Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
+# CI runners. Workspace crates keep it.
+[profile.dev.package."*"]
+debug = false
```

Create `crates/data-avalanche-sink-delta/Cargo.toml`:

```toml
[package]
name = "data-avalanche-sink-delta"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "The Delta Lake realization of the data-avalanche Sink trait (delta-rs)."

[lints]
workspace = true

[dependencies]
arrow-array.workspace = true
arrow-cast.workspace = true
arrow-row.workspace = true
arrow-schema.workspace = true
arrow-select.workspace = true
async-trait.workspace = true
data-avalanche-core.workspace = true
data-avalanche-storage.workspace = true
deltalake.workspace = true
deltalake-core.workspace = true
futures-util.workspace = true
serde_json.workspace = true
tokio = { workspace = true, features = ["sync"] }
url.workspace = true
uuid.workspace = true

[dev-dependencies]
data-avalanche-pipeline.workspace = true
data-avalanche-testkit.workspace = true
proptest.workspace = true
tempfile.workspace = true
```

Create `crates/data-avalanche-sink-delta/src/lib.rs`:

```rust
//! The Delta Lake realization of the `data_avalanche_core::Sink` trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.

mod config;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
```

Create `crates/data-avalanche-sink-delta/src/config.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(backend: Backend, options: Vec<(String, String)>) -> DeltaLocation {
        DeltaLocation::Bucket {
            backend,
            bucket: "lake".into(),
            prefix: "orders/v1".into(),
            options,
        }
    }

    #[test]
    fn table_urls_follow_config_section_10() {
        let t = TableRef::new("public", "orders");
        assert_eq!(
            bucket(Backend::S3, vec![]).table_url(&t).as_str(),
            "s3://lake/orders/v1/public/orders/"
        );
        assert_eq!(
            bucket(Backend::Gcs, vec![]).table_url(&t).as_str(),
            "gs://lake/orders/v1/public/orders/"
        );
        assert_eq!(
            bucket(Backend::Azure { account: None }, vec![])
                .table_url(&t)
                .as_str(),
            "az://lake/orders/v1/public/orders/"
        );
        let local = DeltaLocation::Local {
            root: PathBuf::from("/data/lake"),
        };
        assert_eq!(
            local.table_url(&t).as_str(),
            "file:///data/lake/public/orders/"
        );
    }

    #[test]
    fn escaped_names_keep_the_storage_crates_segments() {
        // `table_root` escapes `%` and `/` as `~XX` (Q-M4-5); the URL encodes a space and
        // non-ASCII text, which delta-rs decodes back.
        let t = TableRef::new("50%", "a/b c.ü");
        let url = bucket(Backend::S3, vec![]).table_url(&t);
        assert_eq!(url.as_str(), "s3://lake/orders/v1/50~25/a~2Fb%20c.%C3%BC/");
        let empty_prefix = DeltaLocation::Bucket {
            backend: Backend::S3,
            bucket: "lake".into(),
            prefix: String::new(),
            options: vec![],
        };
        assert_eq!(
            empty_prefix.table_url(&TableRef::new("s", "t")).as_str(),
            "s3://lake/s/t/"
        );
    }

    #[test]
    fn azure_takes_its_environment_and_explicit_options_win() {
        let loc = bucket(
            Backend::Azure {
                account: Some("acct".into()),
            },
            vec![("azure_client_id".into(), "explicit".into())],
        );
        let env = vec![
            ("AZURE_CLIENT_ID".to_string(), "from-env".to_string()),
            ("AZURE_TENANT_ID".to_string(), "tenant".to_string()),
            ("AWS_REGION".to_string(), "ignored".to_string()),
        ];
        let opts = loc.storage_options(env);
        assert_eq!(opts["azure_client_id"], "explicit");
        assert_eq!(opts["azure_tenant_id"], "tenant");
        assert_eq!(opts["azure_storage_account_name"], "acct");
        assert!(!opts.contains_key("aws_region"));
        let s3 = bucket(Backend::S3, vec![("endpoint".into(), "http://x".into())]);
        let opts = s3.storage_options(vec![("AZURE_CLIENT_ID".into(), "x".into())]);
        assert_eq!(opts.len(), 1, "S3 reads its own environment: {opts:?}");
    }

    #[test]
    fn debug_hides_option_values() {
        let loc = bucket(
            Backend::S3,
            vec![("secret_access_key".into(), "shh-dont-tell".into())],
        );
        let text = format!("{loc:?}");
        assert!(text.contains("secret_access_key"), "{text}");
        assert!(!text.contains("shh-dont-tell"), "{text}");
    }

    #[test]
    fn defaults_are_the_config_defaults() {
        let d = TableDefaults::default();
        assert_eq!(d.checkpoint_interval, 10);
        assert_eq!(d.log_retention, Duration::from_secs(2_592_000));
        assert_eq!(d.timestamp_ntz_as, NtzAs::TimestampNtz);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `config::DeltaLocation`, `config::DeltaSinkConfig`, `config::NtzAs`, `config::TableDefaults`
error[E0425]: cannot find type `Backend` in this scope
error[E0425]: cannot find type `DeltaLocation` in this scope
error[E0433]: cannot find type `PathBuf` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-sink-delta/src/config.rs`, above the test module:

```rust
//! What the Delta sink is built from: the pipeline, where its tables live, and the settings a new
//! table is created with (config §9.4.3). The runtime (M6) fills this from the resolved config.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Duration;

use data_avalanche_core::TableRef;
use data_avalanche_storage::{Backend, table_root};
use url::Url;

/// How a source `timestamp without time zone` column is stored in a **new** Delta table
/// (`sink.delta.timestamp_ntz_as`, D-MS.16). An existing column keeps its type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NtzAs {
    /// Delta `timestamp_ntz`; the table needs reader 3 / writer 7 (`timestampNtz`).
    #[default]
    TimestampNtz,
    /// Delta `timestamp`: the wall-clock value stored as a UTC instant.
    Timestamp,
    /// Delta `string`, `YYYY-MM-DD HH:MM:SS.ffffff`.
    String,
}

/// The settings a table is created with (`sink.delta`, config §9.4.3). Existing tables keep theirs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDefaults {
    /// `delta.checkpointInterval`.
    pub checkpoint_interval: u64,
    /// `delta.logRetentionDuration`, written in whole seconds.
    pub log_retention: Duration,
    pub timestamp_ntz_as: NtzAs,
    /// Passed through as table properties; config V-093 has already checked the `delta.*` keys.
    pub table_properties: BTreeMap<String, String>,
}

impl Default for TableDefaults {
    /// The config defaults: 10 commits, 30 days, `timestamp_ntz`, no extra properties.
    fn default() -> Self {
        Self {
            checkpoint_interval: 10,
            log_retention: Duration::from_secs(30 * 24 * 3600),
            timestamp_ntz_as: NtzAs::TimestampNtz,
            table_properties: BTreeMap::new(),
        }
    }
}

/// Where a pipeline's tables live: `<bucket>/<prefix>/<schema>/<table>/` (config §10), or a local
/// directory (tests and local runs).
#[derive(Clone, PartialEq, Eq)]
pub enum DeltaLocation {
    Local {
        root: PathBuf,
    },
    Bucket {
        backend: Backend,
        bucket: String,
        prefix: String,
        /// `object_store` option keys (`endpoint`, `allow_http`, `use_emulator`, …). Credentials
        /// come from each cloud's standard environment variables, as for the storage crate.
        options: Vec<(String, String)>,
    },
}

impl std::fmt::Debug for DeltaLocation {
    /// Option values can be secrets; only their keys print.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { root } => f.debug_struct("Local").field("root", root).finish(),
            Self::Bucket {
                backend,
                bucket,
                prefix,
                options,
            } => f
                .debug_struct("Bucket")
                .field("backend", backend)
                .field("bucket", bucket)
                .field("prefix", prefix)
                .field(
                    "options",
                    &options
                        .iter()
                        .map(|(k, _)| (k.as_str(), "***"))
                        .collect::<Vec<_>>(),
                )
                .finish(),
        }
    }
}

impl DeltaLocation {
    /// The table's root as a URL with a trailing `/`. The segments are the storage crate's
    /// (`table_root`: config §10, `~XX` escapes, never a `%`), percent-encoded for the URL; the
    /// object keys are exactly those segments.
    pub fn table_url(&self, table: &TableRef) -> Url {
        let (mut url, root) = match self {
            Self::Local { root } => (
                Url::from_directory_path(root).expect("the local root is an absolute path"),
                table_root("", "", &table.schema, &table.name).root,
            ),
            Self::Bucket {
                backend,
                bucket,
                prefix,
                ..
            } => {
                let scheme = match backend {
                    Backend::S3 => "s3",
                    Backend::Gcs => "gs",
                    Backend::Azure { .. } => "az",
                };
                let base = Url::parse(&format!("{scheme}://{bucket}/"))
                    .expect("a bucket name is a valid URL host");
                (
                    base,
                    table_root(bucket, prefix, &table.schema, &table.name).root,
                )
            }
        };
        {
            let mut segments = url.path_segments_mut().expect("a base URL");
            segments.pop_if_empty();
            for s in root.split('/') {
                segments.push(s);
            }
            segments.push("");
        }
        url
    }

    /// The options delta-rs opens a table with. delta-rs reads the `AWS_*` and `GOOGLE_*`
    /// variables itself but not `AZURE_*`, so for Azure they are added here (lower-cased, as
    /// `object_store` spells its keys); an explicit option wins.
    pub fn storage_options(
        &self,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> HashMap<String, String> {
        let Self::Bucket {
            backend, options, ..
        } = self
        else {
            return HashMap::new();
        };
        let mut out: HashMap<String, String> = HashMap::new();
        if let Backend::Azure { account } = backend {
            for (k, v) in env {
                if k.starts_with("AZURE_") {
                    out.insert(k.to_ascii_lowercase(), v);
                }
            }
            if let Some(account) = account {
                out.insert("azure_storage_account_name".into(), account.clone());
            }
        }
        for (k, v) in options {
            out.insert(k.clone(), v.clone());
        }
        out
    }
}

/// Everything a [`crate::DeltaSink`] needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaSinkConfig {
    /// The pipeline `id`: the fence's `app_id` is `data-avalanche/<pipeline_id>/owner`.
    pub pipeline_id: String,
    pub location: DeltaLocation,
    pub table: TableDefaults,
    /// A DataFusion memory ceiling for one MERGE (R-6). A MERGE that needs more fails
    /// (`Transient`) instead of growing the process. `None`: no ceiling.
    pub merge_memory_limit: Option<usize>,
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: PASS. `data-avalanche-sink-delta` unit tests: 5.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml \
  Cargo.lock \
  crates/data-avalanche-sink-delta/Cargo.toml \
  crates/data-avalanche-sink-delta/src/lib.rs \
  crates/data-avalanche-sink-delta/src/config.rs
git commit -m "feat(sink-delta): the crate, its dependencies and the sink configuration"
```

---

### Task 3: Hop 3b schemas

**Files:**
- Modify: `crates/data-avalanche-sink-delta/src/lib.rs`
- Create: `crates/data-avalanche-sink-delta/src/types.rs`

**Interfaces:**
- Consumes: `NtzAs` (Task 2); `LogicalType`, `arrow_type`, `LOGICAL_TAG`, `LIST_ELEMENT` (core).
- Produces (`data_avalanche_sink_delta::types`):
  - `TYPE_KEY = "data-avalanche.type"`.
  - `logical_of(&Field) -> Result<LogicalType, String>`: the logical type a Hop 2 field holds.
  - `delta_type(&LogicalType, NtzAs) -> Result<DeltaType, String>` and
    `delta_field(&Field, NtzAs) -> Result<StructField, String>` (with the metadata of D-M4.1).
  - `physical_type(&DeltaType) -> Result<DataType, String>`: the Arrow type delta-rs reads and
    writes for a Delta type.
  - `column_logical(&StructField) -> Result<LogicalType, String>`, `hop2_field`,
    `hop2_schema(&StructType) -> Result<SchemaRef, String>`: back to Hop 2.

D-M4.1, D-M4.2.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-sink-delta/src/lib.rs` with:

```rust
//! The Delta Lake realization of the `data_avalanche_core::Sink` trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.

mod config;
pub mod types;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
```

Create `crates/data-avalanche-sink-delta/src/types.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::{
        ColumnSchema, ColumnValueType as V, TableRef, TableSchema, upsert_schema,
    };

    use super::*;

    /// One column of every Hop 2 type, the identifier NOT NULL.
    fn every_type() -> TableSchema {
        let cols = vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("flag", V::Bool, "boolean"),
            ColumnSchema::new("small", V::Int16, "smallint"),
            ColumnSchema::new("int", V::Int32, "integer"),
            ColumnSchema::new("u64", V::UInt64, "bigint unsigned"),
            ColumnSchema::new("f32", V::Float32, "real"),
            ColumnSchema::new("f64", V::Float64, "double precision"),
            ColumnSchema::new(
                "dec",
                V::Numeric {
                    precision: 38,
                    scale: 10,
                },
                "numeric(38,10)",
            ),
            ColumnSchema::new("s", V::Text, "text"),
            ColumnSchema::new("js", V::Json, "jsonb"),
            ColumnSchema::new("bin", V::Bytes, "bytea"),
            ColumnSchema::new("d", V::Date, "date"),
            ColumnSchema::new("t", V::Time, "time"),
            ColumnSchema::new("tz", V::Timestamp { with_tz: true }, "timestamptz"),
            ColumnSchema::new("ntz", V::Timestamp { with_tz: false }, "timestamp"),
            ColumnSchema::new("u", V::Uuid, "uuid"),
            ColumnSchema::new("tags", V::Array(Box::new(V::Uuid)), "uuid[]"),
            ColumnSchema::new(
                "grid",
                V::Array(Box::new(V::Array(Box::new(V::Time)))),
                "time[][]",
            ),
        ];
        TableSchema::new(TableRef::new("public", "t"), cols, vec!["id".into()]).unwrap()
    }

    #[test]
    fn hop3b_maps_every_logical_type() {
        let s = upsert_schema(&every_type());
        let ty = |name: &str, ntz| {
            delta_field(s.field_with_name(name).unwrap(), ntz)
                .unwrap()
                .data_type()
                .clone()
        };
        let n = NtzAs::TimestampNtz;
        assert_eq!(ty("flag", n), DeltaType::BOOLEAN);
        assert_eq!(ty("small", n), DeltaType::SHORT);
        assert_eq!(ty("int", n), DeltaType::INTEGER);
        assert_eq!(ty("id", n), DeltaType::LONG);
        assert_eq!(ty("u64", n), DeltaType::decimal(20, 0).unwrap());
        assert_eq!(ty("f32", n), DeltaType::FLOAT);
        assert_eq!(ty("f64", n), DeltaType::DOUBLE);
        assert_eq!(ty("dec", n), DeltaType::decimal(38, 10).unwrap());
        for s in ["s", "js", "u", "t", "_op", "_lsn", "_seq"] {
            assert_eq!(ty(s, n), DeltaType::STRING, "{s}");
        }
        assert_eq!(ty("bin", n), DeltaType::BINARY);
        assert_eq!(ty("d", n), DeltaType::DATE);
        assert_eq!(ty("tz", n), DeltaType::TIMESTAMP);
        assert_eq!(ty("_ts", n), DeltaType::TIMESTAMP);
        assert_eq!(ty("ntz", NtzAs::TimestampNtz), DeltaType::TIMESTAMP_NTZ);
        assert_eq!(ty("ntz", NtzAs::Timestamp), DeltaType::TIMESTAMP);
        assert_eq!(ty("ntz", NtzAs::String), DeltaType::STRING);
        assert_eq!(
            ty("tags", n),
            ArrayType::new(DeltaType::STRING, true).into()
        );
        assert_eq!(
            ty("grid", n),
            ArrayType::new(
                DeltaType::from(ArrayType::new(DeltaType::STRING, true)),
                true
            )
            .into()
        );
    }

    #[test]
    fn every_hop2_field_round_trips_through_its_delta_column() {
        let s = upsert_schema(&every_type());
        for ntz in [NtzAs::TimestampNtz, NtzAs::Timestamp, NtzAs::String] {
            let delta =
                StructType::try_new(s.fields().iter().map(|f| delta_field(f, ntz).unwrap()))
                    .unwrap();
            assert_eq!(hop2_schema(&delta).unwrap(), s, "{ntz:?}");
        }
    }

    #[test]
    fn the_column_keeps_nullability_and_the_tag() {
        let s = upsert_schema(&every_type());
        let id = delta_field(s.field_with_name("id").unwrap(), NtzAs::default()).unwrap();
        assert!(!id.is_nullable());
        let u64 = delta_field(s.field_with_name("u64").unwrap(), NtzAs::default()).unwrap();
        assert!(u64.is_nullable());
        assert_eq!(metadata_text(&u64, LOGICAL_TAG).as_deref(), Some("uint64"));
        assert_eq!(
            metadata_text(&u64, TYPE_KEY).as_deref(),
            Some("decimal(20,0)")
        );
    }

    #[test]
    fn a_column_without_metadata_reads_by_its_delta_type() {
        let foreign = StructType::try_new([
            StructField::new("n", DeltaType::LONG, true),
            StructField::new("ts", DeltaType::TIMESTAMP_NTZ, true),
            StructField::new("s", ArrayType::new(DeltaType::STRING, false), true),
        ])
        .unwrap();
        let s = hop2_schema(&foreign).unwrap();
        assert_eq!(s.field(0).data_type(), &DataType::Int64);
        assert_eq!(
            s.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            s.field(2).data_type(),
            &DataType::List(Arc::new(Field::new(LIST_ELEMENT, DataType::Utf8, true))),
            "the Hop 2 list element is always nullable"
        );
        let map = StructField::new(
            "m",
            deltalake::kernel::MapType::new(DeltaType::STRING, DeltaType::STRING, true),
            true,
        );
        assert!(
            hop2_field(&map)
                .unwrap_err()
                .contains("isn't one this sink writes")
        );
    }

    #[test]
    fn a_negative_decimal_scale_is_refused() {
        let f = Field::new("n", DataType::Decimal128(5, -2), true);
        let err = delta_field(&f, NtzAs::default()).unwrap_err();
        assert!(err.contains("negative scale"), "{err}");
        let f = Field::new("x", DataType::Int8, true);
        assert!(logical_of(&f).unwrap_err().contains("isn't a Hop 2 type"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0433]: cannot find type `NtzAs` in this scope
error[E0425]: cannot find value `LOGICAL_TAG` in this scope
error[E0425]: cannot find value `TYPE_KEY` in this scope
error[E0433]: cannot find type `Arc` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-sink-delta/src/types.rs`, above the test module:

```rust
//! Hop 3b for schemas (`Type_Mapping.md` §Hop 3b): a Hop 2 Arrow field ↔ a Delta column.
//!
//! Delta has no `uuid` or `time`, and `timestamp_ntz` may be stored three ways, so a Delta column
//! alone doesn't say which Hop 2 type it holds. Every column this sink creates carries its logical
//! type in the column metadata (`data-avalanche.type`) and the Hop 2 tag (`data-avalanche.logical`)
//! when there is one; reading them back gives the Hop 2 field exactly.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use data_avalanche_core::{LIST_ELEMENT, LOGICAL_TAG, LogicalType, arrow_type};
use deltalake::kernel::{
    ArrayType, DataType as DeltaType, MetadataValue, PrimitiveType, StructField, StructType,
};

use crate::NtzAs;

/// Column-metadata key holding the column's logical type (`LogicalType`'s text form).
pub const TYPE_KEY: &str = "data-avalanche.type";

/// The logical type a Hop 2 field holds (the inverse of `arrow_type`, with the tag telling `json`
/// from `string`).
pub fn logical_of(field: &Field) -> Result<LogicalType, String> {
    let tag = field.metadata().get(LOGICAL_TAG).map(String::as_str);
    Ok(match field.data_type() {
        DataType::Boolean => LogicalType::Bool,
        DataType::Int16 => LogicalType::Int16,
        DataType::Int32 => LogicalType::Int32,
        DataType::Int64 => LogicalType::Int64,
        DataType::Float32 => LogicalType::Float32,
        DataType::Float64 => LogicalType::Float64,
        DataType::Decimal128(precision, scale) => LogicalType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        DataType::Utf8 if tag == Some("json") => LogicalType::Json,
        DataType::Utf8 => LogicalType::String,
        DataType::Binary => LogicalType::Binary,
        DataType::Date32 => LogicalType::Date,
        DataType::Time64(TimeUnit::Microsecond) => LogicalType::Time,
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC" => {
            LogicalType::TimestampTz
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => LogicalType::TimestampNtz,
        DataType::FixedSizeBinary(16) => LogicalType::Uuid,
        DataType::List(element) => LogicalType::List(Box::new(logical_of(element)?)),
        other => {
            return Err(format!(
                "column {:?}: {other} isn't a Hop 2 type",
                field.name()
            ));
        }
    })
}

/// The Delta type a logical type is stored as (`Type_Mapping.md` §Hop 3b).
pub fn delta_type(logical: &LogicalType, ntz: NtzAs) -> Result<DeltaType, String> {
    use LogicalType as L;
    Ok(match logical {
        L::Bool => DeltaType::BOOLEAN,
        L::Int16 => DeltaType::SHORT,
        L::Int32 => DeltaType::INTEGER,
        L::Int64 => DeltaType::LONG,
        L::Float32 => DeltaType::FLOAT,
        L::Float64 => DeltaType::DOUBLE,
        L::Decimal { precision, scale } => {
            let scale = u8::try_from(*scale)
                .map_err(|_| format!("{logical}: Delta decimals have no negative scale"))?;
            DeltaType::decimal(*precision, scale).map_err(|e| format!("{logical}: {e}"))?
        }
        L::String | L::Json | L::Uuid | L::Time => DeltaType::STRING,
        L::Binary => DeltaType::BINARY,
        L::Date => DeltaType::DATE,
        L::TimestampTz => DeltaType::TIMESTAMP,
        L::TimestampNtz => match ntz {
            NtzAs::TimestampNtz => DeltaType::TIMESTAMP_NTZ,
            NtzAs::Timestamp => DeltaType::TIMESTAMP,
            NtzAs::String => DeltaType::STRING,
        },
        L::List(element) => ArrayType::new(delta_type(element, ntz)?, true).into(),
    })
}

/// The Delta column for a Hop 2 field: same name and nullability, the Hop 3b type, and the
/// metadata that makes it reversible.
pub fn delta_field(field: &Field, ntz: NtzAs) -> Result<StructField, String> {
    let logical = logical_of(field)?;
    let delta = delta_type(&logical, ntz).map_err(|e| format!("column {:?}: {e}", field.name()))?;
    let mut metadata = vec![(TYPE_KEY.to_string(), logical.to_string())];
    if let Some(tag) = field.metadata().get(LOGICAL_TAG) {
        metadata.push((LOGICAL_TAG.to_string(), tag.clone()));
    }
    Ok(StructField::new(field.name(), delta, field.is_nullable()).with_metadata(metadata))
}

/// The Arrow type delta-rs reads and writes a Delta type as (the "physical" type of Hop 3b).
pub fn physical_type(delta: &DeltaType) -> Result<DataType, String> {
    Ok(match delta {
        DeltaType::Primitive(p) => match p {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Byte => DataType::Int8,
            PrimitiveType::Short => DataType::Int16,
            PrimitiveType::Integer => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal(d) => DataType::Decimal128(d.precision(), d.scale() as i8),
            PrimitiveType::String => DataType::Utf8,
            PrimitiveType::Binary => DataType::Binary,
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Timestamp => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            PrimitiveType::TimestampNtz => DataType::Timestamp(TimeUnit::Microsecond, None),
            other => return Err(format!("Delta type {other} isn't one this sink writes")),
        },
        DeltaType::Array(a) => DataType::List(Arc::new(Field::new(
            LIST_ELEMENT,
            physical_type(a.element_type())?,
            a.contains_null(),
        ))),
        other => return Err(format!("Delta type {other} isn't one this sink writes")),
    })
}

fn metadata_text(field: &StructField, key: &str) -> Option<String> {
    match field.metadata().get(key) {
        Some(MetadataValue::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// The logical type a Delta column holds: from its metadata when this sink created it, else read
/// off its Delta type (a column someone else added).
pub fn column_logical(field: &StructField) -> Result<LogicalType, String> {
    match metadata_text(field, TYPE_KEY) {
        Some(text) => text
            .parse()
            .map_err(|e| format!("column {:?}: {e}", field.name())),
        None => {
            let physical = physical_type(field.data_type())
                .map_err(|e| format!("column {:?}: {e}", field.name()))?;
            logical_of(&Field::new(field.name(), physical, true))
        }
    }
}

/// The Hop 2 field of a Delta column (the inverse of [`delta_field`]).
pub fn hop2_field(field: &StructField) -> Result<Field, String> {
    let logical = column_logical(field)?;
    let hop2 = Field::new(field.name(), arrow_type(&logical), field.is_nullable());
    Ok(match metadata_text(field, LOGICAL_TAG) {
        Some(tag) => hop2.with_metadata(HashMap::from([(LOGICAL_TAG.to_string(), tag)])),
        None => hop2,
    })
}

/// The Hop 2 schema of a Delta table, in its column order.
pub fn hop2_schema(schema: &StructType) -> Result<SchemaRef, String> {
    let fields = schema
        .fields()
        .map(hop2_field)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: PASS. `data-avalanche-sink-delta` unit tests: 10.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/src/lib.rs \
  crates/data-avalanche-sink-delta/src/types.rs
git commit -m "feat(sink-delta): Hop 3b schemas"
```

---

### Task 4: Hop 3b values and the Delta date range

**Files:**
- Modify: `crates/data-avalanche-sink-delta/src/lib.rs`
- Create: `crates/data-avalanche-sink-delta/src/convert.rs`

**Interfaces:**
- Consumes: `physical_type` (Task 3); `time_text`, `arrow_type`, `LIST_ELEMENT` (core).
- Produces (`data_avalanche_sink_delta::convert`):
  - `DELTA_MIN_DATE = -719_162`, `DELTA_MAX_DATE = 2_932_896` (days: 0001-01-01, 9999-12-31);
    `DELTA_MIN_MICROS`, `DELTA_MAX_MICROS` (0001-01-01 00:00:00, 9999-12-31 23:59:59.999999).
  - `to_delta(&ArrayRef, &LogicalType, &DeltaType) -> Result<ArrayRef, String>`.
  - `from_delta(&ArrayRef, &LogicalType, &DeltaType) -> Result<ArrayRef, String>`: casts view
    and dictionary arrays to the physical type first.
  - `ntz_text(i64) -> String`, `parse_ntz_text(&str) -> Result<i64, String>`,
    `parse_time_text(&str) -> Result<i64, String>`.

D-M4.3, Q-M4-2. `ntz_text` covers all of `i64` (the civil-from-days algorithm), so the `string` form never fails.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-sink-delta/src/lib.rs` with:

```rust
//! The Delta Lake realization of the `data_avalanche_core::Sink` trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.

mod config;
pub mod convert;
pub mod types;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
```

Create `crates/data-avalanche-sink-delta/src/convert.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use arrow_array::builder::{FixedSizeBinaryBuilder, ListBuilder, StringBuilder};
    use arrow_array::{Date32Array, Int64Array, StringViewArray};
    use arrow_schema::DataType;
    use deltalake::kernel::ArrayType;
    use proptest::prelude::*;

    use super::*;

    fn round_trip(array: ArrayRef, logical: LogicalType, delta: DeltaType) -> ArrayRef {
        let stored = to_delta(&array, &logical, &delta).unwrap();
        assert_eq!(stored.data_type(), &physical_type(&delta).unwrap());
        from_delta(&stored, &logical, &delta).unwrap()
    }

    #[test]
    fn uuid_is_canonical_lowercase_text() {
        let mut b = FixedSizeBinaryBuilder::new(16);
        b.append_value([0xAB; 16]).unwrap();
        b.append_null();
        let a: ArrayRef = Arc::new(b.finish());
        let stored = to_delta(&a, &LogicalType::Uuid, &DeltaType::STRING).unwrap();
        let s = stored.as_string::<i32>();
        assert_eq!(s.value(0), "abababab-abab-abab-abab-abababababab");
        assert!(s.is_null(1));
        assert_eq!(
            round_trip(a.clone(), LogicalType::Uuid, DeltaType::STRING).as_ref(),
            a.as_ref()
        );
    }

    #[test]
    fn time_keeps_intervals_beyond_a_day() {
        let a: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![
            Some(86_400_000_000),
            Some(-5_400_500_000),
            Some(i64::MIN),
            None,
        ]));
        let stored = to_delta(&a, &LogicalType::Time, &DeltaType::STRING).unwrap();
        let s = stored.as_string::<i32>();
        assert_eq!(s.value(0), "24:00:00.000000");
        assert_eq!(s.value(1), "-01:30:00.500000");
        assert_eq!(
            round_trip(a.clone(), LogicalType::Time, DeltaType::STRING).as_ref(),
            a.as_ref()
        );
    }

    #[test]
    fn dates_and_timestamps_clamp_to_the_delta_range() {
        let d: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            Some(0),
            Some(i32::MAX),
            None,
        ]));
        let stored = to_delta(&d, &LogicalType::Date, &DeltaType::DATE).unwrap();
        assert_eq!(
            stored
                .as_primitive::<Date32Type>()
                .iter()
                .collect::<Vec<_>>(),
            [Some(DELTA_MIN_DATE), Some(0), Some(DELTA_MAX_DATE), None]
        );
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            i64::MIN,
            -1,
            i64::MAX,
        ]));
        for (logical, delta, tz) in [
            (LogicalType::TimestampNtz, DeltaType::TIMESTAMP_NTZ, None),
            (LogicalType::TimestampNtz, DeltaType::TIMESTAMP, Some("UTC")),
        ] {
            let stored = to_delta(&ts, &logical, &delta).unwrap();
            let p = stored.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(
                p.values().as_ref(),
                [DELTA_MIN_MICROS, -1, DELTA_MAX_MICROS]
            );
            assert_eq!(p.timezone(), tz);
        }
        let tz: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX]).with_timezone("UTC"));
        let stored = to_delta(&tz, &LogicalType::TimestampTz, &DeltaType::TIMESTAMP).unwrap();
        assert_eq!(
            stored.as_primitive::<TimestampMicrosecondType>().value(0),
            DELTA_MAX_MICROS
        );
        assert!(to_delta(&tz, &LogicalType::TimestampTz, &DeltaType::TIMESTAMP_NTZ).is_err());
    }

    #[test]
    fn ntz_as_string_renders_and_parses_the_whole_range() {
        assert_eq!(ntz_text(0), "1970-01-01 00:00:00.000000");
        assert_eq!(ntz_text(-1), "1969-12-31 23:59:59.999999");
        assert_eq!(ntz_text(DELTA_MIN_MICROS), "0001-01-01 00:00:00.000000");
        assert_eq!(ntz_text(DELTA_MAX_MICROS), "9999-12-31 23:59:59.999999");
        assert_eq!(ntz_text(i64::MIN), "-290308-12-21 19:59:05.224192");
        assert_eq!(ntz_text(i64::MAX), "294247-01-10 04:00:54.775807");
        for v in [0, -1, i64::MIN, i64::MAX, DELTA_MIN_MICROS - 1] {
            assert_eq!(parse_ntz_text(&ntz_text(v)).unwrap(), v, "{v}");
        }
        assert!(parse_ntz_text("2024-01-01").is_err());
    }

    #[test]
    fn lists_convert_element_wise() {
        let mut b = ListBuilder::new(FixedSizeBinaryBuilder::new(16)).with_field(Arc::new(
            Field::new(LIST_ELEMENT, DataType::FixedSizeBinary(16), true),
        ));
        b.values().append_value([1; 16]).unwrap();
        b.values().append_null();
        b.append(true);
        b.append_null();
        let a: ArrayRef = Arc::new(b.finish());
        let logical = LogicalType::List(Box::new(LogicalType::Uuid));
        let delta: DeltaType = ArrayType::new(DeltaType::STRING, true).into();
        let stored = to_delta(&a, &logical, &delta).unwrap();
        assert_eq!(stored.data_type(), &physical_type(&delta).unwrap());
        assert_eq!(round_trip(a.clone(), logical, delta).as_ref(), a.as_ref());
    }

    #[test]
    fn reading_accepts_view_and_dictionary_arrays() {
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![Some("01:00:00.000000"), None]));
        let back = from_delta(&view, &LogicalType::Time, &DeltaType::STRING).unwrap();
        assert_eq!(
            back.as_primitive::<Time64MicrosecondType>().value(0),
            3_600_000_000
        );
        let dict: ArrayRef = Arc::new(
            vec![Some("p1"), Some("p1"), None]
                .into_iter()
                .collect::<arrow_array::DictionaryArray<arrow_array::types::UInt16Type>>(),
        );
        let back = from_delta(&dict, &LogicalType::String, &DeltaType::STRING).unwrap();
        assert_eq!(
            back.as_ref(),
            &StringArray::from(vec![Some("p1"), Some("p1"), None])
        );
    }

    #[test]
    fn a_type_the_column_cant_hold_is_an_error() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let err = to_delta(&a, &LogicalType::Int64, &DeltaType::STRING).unwrap_err();
        assert!(err.contains("can't be stored as Delta"), "{err}");
        let s: ArrayRef = Arc::new(StringArray::from(vec!["not-a-uuid"]));
        assert!(from_delta(&s, &LogicalType::Uuid, &DeltaType::STRING).is_err());
        let mut b = ListBuilder::new(StringBuilder::new());
        b.append(true);
        let l: ArrayRef = Arc::new(b.finish());
        assert!(
            to_delta(
                &l,
                &LogicalType::List(Box::new(LogicalType::String)),
                &DeltaType::STRING
            )
            .is_err()
        );
    }

    proptest! {
        #[test]
        fn time_and_ntz_text_round_trip(v in any::<i64>()) {
            prop_assert_eq!(parse_time_text(&time_text(v)).unwrap(), v);
            prop_assert_eq!(parse_ntz_text(&ntz_text(v)).unwrap(), v);
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0425]: cannot find type `ArrayRef` in this scope
error[E0425]: cannot find type `LogicalType` in this scope
error[E0425]: cannot find type `DeltaType` in this scope
error[E0425]: cannot find function `physical_type` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-sink-delta/src/convert.rs`, above the test module:

```rust
//! Hop 3b for values: a Hop 2 column → the Arrow array of its Delta column, and back.
//!
//! The conversion is chosen by the pair (logical type, the table's Delta type), so a table keeps the
//! representation it was created with (`timestamp_ntz_as` applies to new columns only):
//! - `uuid` → `string`: canonical lowercase text;
//! - `time` → `string`: [`time_text`], which keeps intervals beyond a day;
//! - `timestamp_ntz` → `timestamp`: the same microseconds as a UTC instant;
//! - `timestamp_ntz` → `string`: `YYYY-MM-DD HH:MM:SS.ffffff`;
//! - `date`, `timestamp`, `timestamp_ntz` cells are clamped to the Delta protocol's range,
//!   0001-01-01 … 9999-12-31 23:59:59.999999 (Q-M4-2): delta-rs can't write dates much beyond it
//!   and readers can't read them. PostgreSQL `±infinity` lands on the bounds;
//! - lists convert element-wise; every other pair is the same Arrow type on both sides.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Time64MicrosecondType, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, ListArray, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_schema::Field;
use data_avalanche_core::{LIST_ELEMENT, LogicalType, arrow_type, time_text};
use deltalake::kernel::{DataType as DeltaType, PrimitiveType};

use crate::types::physical_type;

/// 0001-01-01, the first day of the Delta protocol's `date` range.
pub const DELTA_MIN_DATE: i32 = -719_162;
/// 9999-12-31.
pub const DELTA_MAX_DATE: i32 = 2_932_896;
/// 0001-01-01 00:00:00.000000 in microseconds since the Unix epoch.
pub const DELTA_MIN_MICROS: i64 = -62_135_596_800_000_000;
/// 9999-12-31 23:59:59.999999.
pub const DELTA_MAX_MICROS: i64 = 253_402_300_799_999_999;

fn is_string(d: &DeltaType) -> bool {
    matches!(d, DeltaType::Primitive(PrimitiveType::String))
}

fn mismatch(logical: &LogicalType, delta: &DeltaType) -> String {
    format!("a {logical} column can't be stored as Delta {delta}")
}

/// Converts a Hop 2 column of `logical` type to the Arrow array delta-rs writes into a column of
/// type `delta`.
pub fn to_delta(
    array: &ArrayRef,
    logical: &LogicalType,
    delta: &DeltaType,
) -> Result<ArrayRef, String> {
    use LogicalType as L;
    use PrimitiveType as P;
    let DeltaType::Primitive(p) = delta else {
        let (L::List(element), DeltaType::Array(a)) = (logical, delta) else {
            return Err(mismatch(logical, delta));
        };
        let list = array.as_list::<i32>();
        let values = to_delta(list.values(), element, a.element_type())?;
        let field = Arc::new(Field::new(LIST_ELEMENT, values.data_type().clone(), true));
        return Ok(Arc::new(ListArray::new(
            field,
            list.offsets().clone(),
            values,
            list.nulls().cloned(),
        )));
    };
    Ok(match (logical, p) {
        (L::Uuid, P::String) => {
            let a = array.as_fixed_size_binary();
            Arc::new(
                (0..a.len())
                    .map(|i| {
                        a.is_valid(i).then(|| {
                            uuid::Uuid::from_slice(a.value(i))
                                .expect("16 bytes")
                                .hyphenated()
                                .to_string()
                        })
                    })
                    .collect::<StringArray>(),
            )
        }
        (L::Time, P::String) => Arc::new(
            array
                .as_primitive::<Time64MicrosecondType>()
                .iter()
                .map(|v| v.map(time_text))
                .collect::<StringArray>(),
        ),
        (L::Date, P::Date) => Arc::new(
            array
                .as_primitive::<Date32Type>()
                .unary::<_, Date32Type>(|d| d.clamp(DELTA_MIN_DATE, DELTA_MAX_DATE)),
        ),
        (L::TimestampTz | L::TimestampNtz, P::Timestamp | P::TimestampNtz) => {
            if matches!((logical, p), (L::TimestampTz, P::TimestampNtz)) {
                return Err(mismatch(logical, delta));
            }
            let clamped: TimestampMicrosecondArray = array
                .as_primitive::<TimestampMicrosecondType>()
                .unary(|t| t.clamp(DELTA_MIN_MICROS, DELTA_MAX_MICROS));
            match p {
                P::Timestamp => Arc::new(clamped.with_timezone("UTC")),
                _ => Arc::new(clamped),
            }
        }
        (L::TimestampNtz, P::String) => Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .map(|v| v.map(ntz_text))
                .collect::<StringArray>(),
        ),
        _ => {
            let want = physical_type(delta)?;
            if array.data_type() != &want || arrow_type(logical) != want {
                return Err(mismatch(logical, delta));
            }
            array.clone()
        }
    })
}

/// Converts an array read from a Delta column of type `delta` (in whatever Arrow form the reader
/// produced: views, dictionaries) back to the Hop 2 array of `logical` type.
pub fn from_delta(
    array: &ArrayRef,
    logical: &LogicalType,
    delta: &DeltaType,
) -> Result<ArrayRef, String> {
    use LogicalType as L;
    let physical = physical_type(delta)?;
    let array = arrow_cast::cast(array, &physical).map_err(|e| e.to_string())?;
    if let (L::List(element), DeltaType::Array(a)) = (logical, delta) {
        let list = array.as_list::<i32>();
        let values = from_delta(list.values(), element, a.element_type())?;
        let field = Arc::new(Field::new(LIST_ELEMENT, values.data_type().clone(), true));
        return Ok(Arc::new(ListArray::new(
            field,
            list.offsets().clone(),
            values,
            list.nulls().cloned(),
        )));
    }
    Ok(match (logical, is_string(delta)) {
        (L::Uuid, true) => {
            let s = array.as_string::<i32>();
            let bytes = (0..s.len())
                .map(|i| {
                    s.is_valid(i)
                        .then(|| uuid::Uuid::parse_str(s.value(i)).map(|u| u.into_bytes()))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("a uuid cell: {e}"))?;
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(bytes.into_iter(), 16)
                    .map_err(|e| e.to_string())?,
            )
        }
        (L::Time, true) => {
            let s = array.as_string::<i32>();
            Arc::new(
                s.iter()
                    .map(|v| v.map(parse_time_text).transpose())
                    .collect::<Result<Time64MicrosecondArray, _>>()?,
            )
        }
        (L::TimestampNtz, true) => {
            let s = array.as_string::<i32>();
            Arc::new(
                s.iter()
                    .map(|v| v.map(parse_ntz_text).transpose())
                    .collect::<Result<TimestampMicrosecondArray, _>>()?,
            )
        }
        _ => arrow_cast::cast(&array, &arrow_type(logical)).map_err(|e| e.to_string())?,
    })
}

/// Days since the Unix epoch → (year, month, day), proleptic Gregorian, for every `i64` (Howard
/// Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// The inverse of [`civil_from_days`].
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `YYYY-MM-DD HH:MM:SS.ffffff` for a wall-clock timestamp in microseconds. Years outside
/// 0…9999 print their digits with a sign for negative years.
pub fn ntz_text(micros: i64) -> String {
    let (days, rem) = (
        micros.div_euclid(86_400_000_000),
        micros.rem_euclid(86_400_000_000),
    );
    let (y, m, d) = civil_from_days(days);
    let (secs, frac) = (rem / 1_000_000, rem % 1_000_000);
    let year = if y < 0 {
        format!("-{:04}", -y)
    } else {
        format!("{y:04}")
    };
    format!(
        "{year}-{m:02}-{d:02} {:02}:{:02}:{:02}.{frac:06}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

fn bad(text: &str, what: &str) -> String {
    format!("{text:?} isn't a {what}")
}

/// The inverse of [`ntz_text`].
pub fn parse_ntz_text(text: &str) -> Result<i64, String> {
    let err = || bad(text, "timestamp text");
    let (date, time) = text.split_once(' ').ok_or_else(err)?;
    let (negative, date) = match date.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, date),
    };
    let mut parts = date.splitn(3, '-');
    let mut next = || parts.next().ok_or_else(err);
    let (y, m, d) = (next()?, next()?, next()?);
    let y: i64 = y.parse().map_err(|_| err())?;
    let m: u32 = m.parse().map_err(|_| err())?;
    let d: u32 = d.parse().map_err(|_| err())?;
    let y = if negative { -y } else { y };
    let days = i128::from(days_from_civil(y, m, d));
    let micros = days * 86_400_000_000 + i128::from(parse_time_text(time)?);
    i64::try_from(micros).map_err(|_| err())
}

/// The inverse of [`time_text`]: `[-]H…H:MM:SS.ffffff` → signed microseconds.
pub fn parse_time_text(text: &str) -> Result<i64, String> {
    let err = || bad(text, "time text");
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (hms, frac) = body.split_once('.').ok_or_else(err)?;
    let mut parts = hms.splitn(3, ':');
    let mut next =
        || -> Result<i128, String> { parts.next().ok_or_else(err)?.parse().map_err(|_| err()) };
    let (h, m, s) = (next()?, next()?, next()?);
    if frac.len() != 6 || m > 59 || s > 59 {
        return Err(err());
    }
    let frac: i128 = frac.parse().map_err(|_| err())?;
    let magnitude = ((h * 60 + m) * 60 + s) * 1_000_000 + frac;
    let micros = if negative { -magnitude } else { magnitude };
    i64::try_from(micros).map_err(|_| err())
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: PASS. `data-avalanche-sink-delta` unit tests: 18.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/src/lib.rs \
  crates/data-avalanche-sink-delta/src/convert.rs
git commit -m "feat(sink-delta): Hop 3b values and the Delta date range"
```

---

### Task 5: The commit marker and the walk-back

**Files:**
- Modify: `crates/data-avalanche-sink-delta/src/lib.rs`
- Create: `crates/data-avalanche-sink-delta/src/marker.rs`

**Interfaces:**
- Consumes: `SourcePosition` (`to_tagged`, `parse_tagged`) (core); deltalake's `LogStore`.
- Produces (`data_avalanche_sink_delta::marker`):
  - `POSITION_KEY`, `EPOCH_KEY`, `PIPELINE_KEY` (D-IMPL-7), `MAX_WALK_BACK = 1000`.
  - `app_id(pipeline_id) -> String`.
  - `commit_metadata(pipeline_id, owner_epoch, Option<&SourcePosition>) -> HashMap<String, Value>`.
  - `own_position(commit: &[u8], pipeline_id) -> Result<Option<SourcePosition>, String>`.
  - `WalkBack::{Found { version, position }, Reached, NotFound}` and
    `async walk_back(&dyn LogStore, latest: u64, stop: Option<u64>, pipeline_id) -> Result<WalkBack, String>`.

D-M4.6. The walk-back itself is exercised through `TableHandle` in Task 6 and against real logs in Task 10.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-sink-delta/src/lib.rs` with:

```rust
//! The Delta Lake realization of the `data_avalanche_core::Sink` trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.

mod config;
pub mod convert;
pub mod marker;
pub mod types;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
```

Create `crates/data-avalanche-sink-delta/src/marker.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::position::Lsn;

    use super::*;

    fn commit(info: Value) -> Vec<u8> {
        let mut out = br#"{"add":{"path":"x.parquet"}}"#.to_vec();
        out.push(b'\n');
        out.extend(serde_json::to_vec(&serde_json::json!({ "commitInfo": info })).unwrap());
        out.push(b'\n');
        out
    }

    #[test]
    fn the_app_id_names_the_pipeline() {
        assert_eq!(app_id("orders"), "data-avalanche/orders/owner");
    }

    #[test]
    fn own_commits_are_recognized_by_pipeline_and_position() {
        let pos = SourcePosition::Pg(Lsn(0x16B3748));
        let ours = commit(Value::Object(
            commit_metadata("orders", 3, Some(&pos))
                .into_iter()
                .collect(),
        ));
        assert_eq!(own_position(&ours, "orders").unwrap(), Some(pos.clone()));
        assert_eq!(own_position(&ours, "other").unwrap(), None);
        let unpositioned = commit(Value::Object(
            commit_metadata("orders", 3, None).into_iter().collect(),
        ));
        assert_eq!(own_position(&unpositioned, "orders").unwrap(), None);
        let foreign = commit(serde_json::json!({ "operation": "OPTIMIZE" }));
        assert_eq!(own_position(&foreign, "orders").unwrap(), None);
    }

    #[test]
    fn metadata_holds_the_tagged_position_and_the_epoch() {
        let m = commit_metadata("p", 7, Some(&SourcePosition::Pg(Lsn(16))));
        assert_eq!(m[POSITION_KEY], "pg:0/10");
        assert_eq!(m[EPOCH_KEY], 7);
        assert_eq!(m[PIPELINE_KEY], "p");
    }

    #[test]
    fn a_corrupt_marker_is_an_error_not_absent() {
        let bad = commit(serde_json::json!({ PIPELINE_KEY: "p", POSITION_KEY: "pg:nope" }));
        assert!(own_position(&bad, "p").is_err());
        assert!(own_position(b"{not json", "p").is_err());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0425]: cannot find type `Value` in this scope
error[E0425]: cannot find value `POSITION_KEY` in this scope
error[E0425]: cannot find value `EPOCH_KEY` in this scope
error[E0425]: cannot find value `PIPELINE_KEY` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-sink-delta/src/marker.rs`, above the test module:

```rust
//! The commit marker on a Delta table (Data-Flow §Delta Lake realization, D-IMPL-7).
//!
//! - **Fence:** every commit carries the app-transaction `txn { app_id:
//!   "data-avalanche/<pipeline_id>/owner", version: owner_epoch }`. Delta keeps the latest `txn`
//!   version per app in the table state (checkpoints included), so a foreign commit can't hide it.
//! - **Position:** the commit's `commitInfo` carries `data-avalanche.committed_position`,
//!   `.owner_epoch` and `.pipeline`. `commitInfo` is per commit, so the sink walks back from the
//!   latest version to the last commit of its own pipeline, past foreign ones (an external
//!   OPTIMIZE; gap DA-X-10).

use std::collections::HashMap;

use data_avalanche_core::SourcePosition;
use deltalake::logstore::LogStore;
use serde_json::Value;

pub const POSITION_KEY: &str = "data-avalanche.committed_position";
pub const EPOCH_KEY: &str = "data-avalanche.owner_epoch";
pub const PIPELINE_KEY: &str = "data-avalanche.pipeline";

/// How many commits the walk-back reads before giving up. Past it, the position is unknown and
/// the guard skips nothing; a replay is then an idempotent re-write.
pub const MAX_WALK_BACK: u64 = 1000;

/// The fence's app id.
pub fn app_id(pipeline_id: &str) -> String {
    format!("data-avalanche/{pipeline_id}/owner")
}

/// The `commitInfo` entries of one commit. `position` is `None` on a commit that doesn't finish
/// the change set (the truncate before its MERGE), so the walk-back never stops there.
pub fn commit_metadata(
    pipeline_id: &str,
    owner_epoch: u64,
    position: Option<&SourcePosition>,
) -> HashMap<String, Value> {
    let mut m = HashMap::from([
        (PIPELINE_KEY.to_string(), Value::from(pipeline_id)),
        (EPOCH_KEY.to_string(), Value::from(owner_epoch)),
    ]);
    if let Some(p) = position {
        m.insert(POSITION_KEY.to_string(), Value::from(p.to_tagged()));
    }
    m
}

/// The position `pipeline_id` recorded in one commit file (newline-delimited JSON actions), or
/// `None` when the commit isn't one of its positioned commits.
pub fn own_position(commit: &[u8], pipeline_id: &str) -> Result<Option<SourcePosition>, String> {
    for line in commit.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let action: Value =
            serde_json::from_slice(line).map_err(|e| format!("a commit line: {e}"))?;
        let Some(info) = action.get("commitInfo") else {
            continue;
        };
        if info.get(PIPELINE_KEY).and_then(Value::as_str) != Some(pipeline_id) {
            return Ok(None);
        }
        return match info.get(POSITION_KEY).and_then(Value::as_str) {
            Some(tagged) => SourcePosition::parse_tagged(tagged)
                .map(Some)
                .map_err(|e| format!("the committed position {tagged:?}: {e}")),
            None => Ok(None),
        };
    }
    Ok(None)
}

/// What the walk-back found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalkBack {
    Found {
        version: u64,
        position: SourcePosition,
    },
    /// Reached `stop` (the version of the own commit the caller already knows) without a newer one.
    Reached,
    /// No own commit in the log that is left (or within [`MAX_WALK_BACK`]).
    NotFound,
}

/// Reads commits from `latest` down and returns the newest one this pipeline positioned. It stops
/// at `stop` (the version of the own commit the caller already knows) and at a commit file that is
/// gone (expired-log cleanup).
pub async fn walk_back(
    log: &dyn LogStore,
    latest: u64,
    stop: Option<u64>,
    pipeline_id: &str,
) -> Result<WalkBack, String> {
    let mut version = latest;
    for _ in 0..MAX_WALK_BACK {
        if stop.is_some_and(|s| version <= s) {
            return Ok(WalkBack::Reached);
        }
        let Some(bytes) = log
            .read_commit_entry(version)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok(WalkBack::NotFound);
        };
        if let Some(position) = own_position(&bytes, pipeline_id)? {
            return Ok(WalkBack::Found { version, position });
        }
        let Some(previous) = version.checked_sub(1) else {
            return Ok(WalkBack::NotFound);
        };
        version = previous;
    }
    Ok(WalkBack::NotFound)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: PASS. `data-avalanche-sink-delta` unit tests: 22.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/src/lib.rs \
  crates/data-avalanche-sink-delta/src/marker.rs
git commit -m "feat(sink-delta): the commit marker and the walk-back"
```

---

### Task 6: Error classes and the table handle

**Files:**
- Modify: `crates/data-avalanche-sink-delta/src/lib.rs`
- Create: `crates/data-avalanche-sink-delta/src/error.rs`
- Create: `crates/data-avalanche-sink-delta/src/table.rs`
- Create: `crates/data-avalanche-sink-delta/src/test_support.rs`

**Interfaces:**
- Consumes: `TableDefaults`, `DeltaLocation` (Task 2); `delta_field`, `column_logical`,
  `hop2_schema`, `physical_type` (Task 3); `from_delta` (Task 4); `app_id`, `walk_back`,
  `WalkBack`, `commit_metadata` (Task 5).
- Produces:
  - `error::permanent(impl Into<String>) -> SinkError`, `error::sink_error(DeltaTableError) -> SinkError` (D-M4.12).
  - `IDENTIFIER_KEY = "data-avalanche.identifier"` (crate root).
  - `pub(crate) struct TableHandle` with `open(Url, HashMap<String, String>)`, `table()`,
    `set_table(DeltaTable)`, `refresh() -> Result<bool>` (false: no Delta table there),
    `version() -> u64`, `foreign_format() -> Result<Option<String>>`, `create(&TableSpec,
    &TableDefaults)`, `schema() -> Result<SchemaRef>` (Hop 2), `identifier()`,
    `fence_epoch(pipeline) -> Result<Option<u64>>`, `stored_position(pipeline) ->
    Result<Option<SourcePosition>>`, `remember(version, SourcePosition)`, `content() ->
    Result<RecordBatch>` (Hop 2 rows in key order). All errors are `SinkError`.
  - `test_support` (unit tests only): `orders()`, `spec(&TableSchema)`, `local(&TempDir)`,
    `handle(&TempDir, &TableRef)`.

D-M4.6, D-M4.10, D-M4.12, D-M4.13. `table` and its helpers have no caller outside tests until
`DeltaSink` (Task 9), so `lib.rs` marks the module `#[allow(dead_code)]` until then; Task 9 removes
the attribute.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-sink-delta/src/lib.rs` with:

```rust
//! The Delta Lake realization of the `data_avalanche_core::Sink` trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.

mod config;
pub mod convert;
mod error;
pub mod marker;
#[allow(dead_code)] // `DeltaSink` (Task 9) is the caller
mod table;
#[cfg(test)]
mod test_support;
pub mod types;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
pub use table::IDENTIFIER_KEY;
```

Create `crates/data-avalanche-sink-delta/src/test_support.rs`:

```rust
//! Fixtures for the crate's unit tests: a local table of a small schema.

use std::collections::HashMap;

use data_avalanche_core::{ColumnSchema, ColumnValueType as V, TableRef, TableSchema, TableSpec};

use crate::DeltaLocation;
use crate::table::TableHandle;

/// `public.orders (id bigint key, doc text, ref uuid, at timestamp)`.
pub(crate) fn orders() -> TableSchema {
    TableSchema::new(
        TableRef::new("public", "orders"),
        vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("doc", V::Text, "text"),
            ColumnSchema::new("ref", V::Uuid, "uuid"),
            ColumnSchema::new("at", V::Timestamp { with_tz: false }, "timestamp"),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

pub(crate) fn spec(schema: &TableSchema) -> TableSpec {
    TableSpec::new(schema, vec![]).unwrap()
}

pub(crate) fn local(dir: &tempfile::TempDir) -> DeltaLocation {
    DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    }
}

pub(crate) async fn handle(dir: &tempfile::TempDir, table: &TableRef) -> TableHandle {
    TableHandle::open(local(dir).table_url(table), HashMap::new())
        .await
        .unwrap()
}
```

Create `crates/data-avalanche-sink-delta/src/error.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_errors_are_permanent_and_storage_errors_transient() {
        let e = sink_error(DeltaTableError::SchemaMismatch { msg: "x".into() });
        assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
        let e = sink_error(DeltaTableError::Generic("connection reset".into()));
        assert!(e.is_transient(), "{e}");
        let e = sink_error(DeltaTableError::Transaction {
            source: TransactionError::DeltaTableAppendOnly,
        });
        assert!(matches!(e, SinkError::Permanent { .. }), "{e}");
    }
}
```

Create `crates/data-avalanche-sink-delta/src/table.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{PartitionField, TableRef, upsert_schema};
    use deltalake::kernel::Transaction;
    use deltalake::kernel::transaction::CommitProperties;

    use super::*;
    use crate::NtzAs;
    use crate::marker::commit_metadata;
    use crate::test_support::{handle, orders, spec};

    fn t() -> TableRef {
        TableRef::new("public", "orders")
    }

    /// Physical rows of `orders` (uuid as text, `at` as timestamp_ntz), in the order given.
    fn rows(ids: &[i64]) -> RecordBatch {
        let utc = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let text = |n: &str, null| Field::new(n, DataType::Utf8, null);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            text("doc", true),
            text("ref", true),
            Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            text("_op", false),
            Field::new("_ts", utc(), false),
            text("_lsn", false),
            text("_seq", false),
            text("_source_db", false),
            text("_pipeline", false),
        ]));
        let n = ids.len();
        let s = |v: &str| Arc::new(StringArray::from(vec![v; n])) as ArrayRef;
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                s("doc"),
                s("00000000-0000-0000-0000-00000000000a"),
                Arc::new(TimestampMicrosecondArray::from(vec![0; n])),
                s("I"),
                Arc::new(TimestampMicrosecondArray::from(vec![0; n]).with_timezone("UTC")),
                s("0/1"),
                s("00000000/00000001"),
                s("h/db"),
                s("p"),
            ],
        )
        .unwrap()
    }

    /// Appends `ids` as one commit of pipeline `p` at `epoch`, positioned at LSN `lsn`.
    async fn own_append(h: &mut TableHandle, ids: &[i64], epoch: u64, lsn: u64) {
        let props = CommitProperties::default()
            .with_application_transaction(Transaction::new(app_id("p"), epoch as i64))
            .with_metadata(commit_metadata(
                "p",
                epoch,
                Some(&SourcePosition::Pg(Lsn(lsn))),
            ));
        let table = h
            .table()
            .clone()
            .write(vec![rows(ids)])
            .with_commit_properties(props)
            .await
            .unwrap();
        h.set_table(table);
    }

    async fn created(dir: &tempfile::TempDir, defaults: &TableDefaults) -> TableHandle {
        let mut h = handle(dir, &t()).await;
        assert!(!h.refresh().await.unwrap(), "no table yet");
        h.create(&spec(&orders()), defaults).await.unwrap();
        h
    }

    #[tokio::test]
    async fn a_created_table_reads_back_its_hop2_schema_and_settings() {
        let dir = tempfile::tempdir().unwrap();
        let defaults = TableDefaults {
            checkpoint_interval: 7,
            log_retention: Duration::from_secs(3600),
            timestamp_ntz_as: NtzAs::Timestamp,
            table_properties: [("owner".to_string(), "team-a".to_string())].into(),
        };
        let mut h = created(&dir, &defaults).await;
        assert_eq!(h.schema().unwrap(), upsert_schema(&orders()));
        assert_eq!(h.identifier().unwrap(), ["id"]);
        let snapshot = h.table().snapshot().unwrap();
        let config = snapshot.metadata().configuration();
        assert_eq!(config["delta.checkpointInterval"], "7");
        assert_eq!(
            config["delta.logRetentionDuration"],
            "interval 3600 seconds"
        );
        assert_eq!(config["delta.enableDeletionVectors"], "false");
        assert_eq!(config["owner"], "team-a");
        assert!(!config.contains_key("delta.columnMapping.mode"));
        assert_eq!(snapshot.metadata().name(), Some("public.orders"));
        let protocol = snapshot.protocol();
        assert_eq!(
            (protocol.min_reader_version(), protocol.min_writer_version()),
            (1, 2),
            "no timestamp_ntz column: the minimal protocol"
        );
        // A second create keeps the table.
        h.create(&spec(&orders()), &TableDefaults::default())
            .await
            .unwrap();
        assert_eq!(h.version(), 0);
        assert!(h.refresh().await.unwrap());
    }

    #[tokio::test]
    async fn timestamp_ntz_raises_the_protocol() {
        let dir = tempfile::tempdir().unwrap();
        let h = created(&dir, &TableDefaults::default()).await;
        let protocol = h.table().snapshot().unwrap().protocol().clone();
        assert_eq!(
            (protocol.min_reader_version(), protocol.min_writer_version()),
            (3, 7)
        );
    }

    #[tokio::test]
    async fn only_identity_partitions_are_created() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = handle(&dir, &t()).await;
        let mut s = spec(&orders());
        s.partition_by = vec![PartitionField {
            column: "at".into(),
            transform: data_avalanche_core::PartitionTransform::Day,
        }];
        let err = h.create(&s, &TableDefaults::default()).await.unwrap_err();
        assert!(err.to_string().contains("identity only"), "{err}");
        s.partition_by[0].transform = data_avalanche_core::PartitionTransform::Identity;
        s.partition_by[0].column = "doc".into();
        h.create(&s, &TableDefaults::default()).await.unwrap();
        let snapshot = h.table().snapshot().unwrap();
        assert_eq!(
            snapshot.metadata().partition_columns(),
            &["doc".to_string()]
        );
    }

    #[tokio::test]
    async fn iceberg_metadata_under_the_root_is_a_foreign_format() {
        let dir = tempfile::tempdir().unwrap();
        let h = handle(&dir, &t()).await;
        assert_eq!(h.foreign_format().await.unwrap(), None);
        let meta = dir.path().join("public/orders/metadata");
        std::fs::create_dir_all(&meta).unwrap();
        std::fs::write(meta.join("00001-abc.metadata.json"), "{}").unwrap();
        let found = h.foreign_format().await.unwrap().unwrap();
        assert!(found.contains("Iceberg"), "{found}");
    }

    #[tokio::test]
    async fn the_marker_is_the_txn_epoch_and_the_last_own_position() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir, &TableDefaults::default()).await;
        assert_eq!(h.fence_epoch("p").await.unwrap(), None);
        assert_eq!(h.stored_position("p").await.unwrap(), None);
        own_append(&mut h, &[1], 4, 0x10).await;
        own_append(&mut h, &[2], 5, 0x20).await;
        assert_eq!(h.fence_epoch("p").await.unwrap(), Some(5));
        assert_eq!(h.fence_epoch("other").await.unwrap(), None);
        assert_eq!(
            h.stored_position("p").await.unwrap(),
            Some(SourcePosition::Pg(Lsn(0x20)))
        );
        // Foreign commits (an append without a marker, an OPTIMIZE) don't hide the position or
        // the epoch.
        let table = h.table().clone().write(vec![rows(&[3])]).await.unwrap();
        let (table, _) = table.optimize().await.unwrap();
        h.set_table(table);
        let mut fresh = handle(&dir, &t()).await;
        assert!(fresh.refresh().await.unwrap());
        assert_eq!(fresh.version(), 4);
        assert_eq!(fresh.fence_epoch("p").await.unwrap(), Some(5));
        assert_eq!(
            fresh.stored_position("p").await.unwrap(),
            Some(SourcePosition::Pg(Lsn(0x20)))
        );
    }

    #[tokio::test]
    async fn content_is_hop2_rows_in_key_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir, &TableDefaults::default()).await;
        assert_eq!(h.content().await.unwrap().num_rows(), 0);
        own_append(&mut h, &[3, 1], 1, 1).await;
        own_append(&mut h, &[2], 1, 2).await;
        let c = h.content().await.unwrap();
        assert_eq!(c.schema(), upsert_schema(&orders()));
        let ids: Vec<i64> = c.column(0).as_primitive::<Int64Type>().values().to_vec();
        assert_eq!(ids, [1, 2, 3]);
        let uuid = c.column(2).as_fixed_size_binary();
        assert_eq!(uuid.value(0)[15], 0x0a, "uuid text back to bytes");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `crate::table::TableHandle`
error[E0432]: unresolved import `table::IDENTIFIER_KEY`
error[E0425]: cannot find type `RecordBatch` in this scope
error[E0433]: cannot find type `Arc` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-sink-delta/src/error.rs`, above the test module:

```rust
//! delta-rs errors → [`SinkError`]. A failure is `Permanent` only when retrying the same call
//! can't help (the data or the table is wrong, or delta-rs lacks a feature); everything else,
//! storage and network errors included, is `Transient` and the pipeline retries with backoff
//! (Data-Flow §Sink Commit Failure).

use data_avalanche_core::SinkError;
use deltalake::DeltaTableError;
use deltalake::kernel::transaction::TransactionError;

pub(crate) fn permanent(reason: impl Into<String>) -> SinkError {
    SinkError::Permanent {
        reason: reason.into(),
    }
}

pub(crate) fn sink_error(e: DeltaTableError) -> SinkError {
    use DeltaTableError as E;
    let reason = e.to_string();
    match e {
        E::SchemaMismatch { .. }
        | E::InvalidData { .. }
        | E::MetadataError(_)
        | E::CommitValidation { .. }
        | E::UnsupportedColumnMapping { .. }
        | E::MissingFeature { .. }
        | E::InvalidTableLocation(_)
        | E::NotATable(_)
        | E::Transaction {
            source:
                TransactionError::UnsupportedTableFeatures(_)
                | TransactionError::TableFeaturesRequired(_)
                | TransactionError::DeltaTableAppendOnly,
        } => SinkError::Permanent { reason },
        _ => SinkError::Transient { reason },
    }
}
```

Add the implementation to `crates/data-avalanche-sink-delta/src/table.rs`, above the test module:

```rust
//! One Delta table as the sink sees it: open and reload it, create it, and read its Hop 2 schema,
//! its marker and its rows.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_row::{RowConverter, SortField};
use arrow_schema::SchemaRef;
use data_avalanche_core::{PartitionTransform, SinkError, SourcePosition, TableSpec};
use deltalake::DeltaTable;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::logstore::object_store::ObjectStore;
use deltalake::logstore::object_store::path::Path;
use deltalake::protocol::SaveMode;
use futures_util::TryStreamExt;
use url::Url;

use crate::TableDefaults;
use crate::convert::from_delta;
use crate::error::{permanent, sink_error};
use crate::marker::{WalkBack, app_id, walk_back};
use crate::types::{column_logical, delta_field, hop2_schema};

/// Table property holding the identifier columns (a JSON array), so the table can be read in key
/// order without its `TableSchema`.
pub const IDENTIFIER_KEY: &str = "data-avalanche.identifier";

pub(crate) struct TableHandle {
    table: DeltaTable,
    /// The newest commit of this pipeline the handle knows: version and position. The walk-back
    /// stops there, so a steady stream of own commits costs no reads.
    known: Option<(u64, SourcePosition)>,
}

impl std::fmt::Debug for TableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableHandle")
            .field("url", self.table.table_url())
            .field("version", &self.table.version())
            .field("known", &self.known)
            .finish()
    }
}

impl TableHandle {
    /// A handle on `url`; nothing is read yet. A local root is created first: delta-rs refuses a
    /// `file://` location that doesn't exist.
    pub async fn open(url: Url, options: HashMap<String, String>) -> Result<Self, SinkError> {
        if let Ok(path) = url.to_file_path() {
            std::fs::create_dir_all(&path)
                .map_err(|e| permanent(format!("{}: {e}", path.display())))?;
        }
        let table = DeltaTable::try_from_url_with_storage_options(url, options)
            .await
            .map_err(sink_error)?;
        Ok(Self { table, known: None })
    }

    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Replaces the table after a write returned its new state.
    pub fn set_table(&mut self, table: DeltaTable) {
        self.table = table;
    }

    /// Loads the newest version; `false` when the location holds no Delta table.
    pub async fn refresh(&mut self) -> Result<bool, SinkError> {
        let log = self.table.log_store();
        if !log.is_delta_table_location().await.map_err(sink_error)? {
            return Ok(false);
        }
        self.table
            .update_incremental(None)
            .await
            .map_err(sink_error)?;
        Ok(true)
    }

    /// The loaded version (0 before the first load).
    pub fn version(&self) -> u64 {
        self.table.version().unwrap_or(0)
    }

    /// An Iceberg table's metadata under this root (`metadata/…metadata.json`, or `meta/` as
    /// config §10 names it): the root holds another format, which the sink never mixes with.
    pub async fn foreign_format(&self) -> Result<Option<String>, SinkError> {
        let store = self.table.log_store().object_store(None);
        for dir in ["metadata", "meta"] {
            let found = store
                .list(Some(&Path::from(dir)))
                .try_filter(|m| std::future::ready(m.location.as_ref().ends_with(".metadata.json")))
                .try_next()
                .await
                .map_err(|e| sink_error(e.into()))?;
            if let Some(m) = found {
                return Ok(Some(format!(
                    "{} holds Iceberg metadata ({})",
                    self.table.table_url(),
                    m.location
                )));
            }
        }
        Ok(None)
    }

    /// Creates the table (Data-Flow §Delta Lake realization, item 5): column mapping off,
    /// deletion vectors off, the checkpoint interval and log retention from `defaults`, the
    /// passthrough properties, identity partitions. A table another writer created first is kept.
    pub async fn create(
        &mut self,
        spec: &TableSpec,
        defaults: &TableDefaults,
    ) -> Result<(), SinkError> {
        let columns = spec
            .schema
            .fields()
            .iter()
            .map(|f| delta_field(f, defaults.timestamp_ntz_as))
            .collect::<Result<Vec<_>, _>>()
            .map_err(permanent)?;
        let mut partitions = Vec::new();
        for p in &spec.partition_by {
            if p.transform != PartitionTransform::Identity {
                return Err(permanent(format!(
                    "Delta partitions by identity only, not {:?} (column {:?})",
                    p.transform, p.column
                )));
            }
            partitions.push(p.column.clone());
        }
        let mut config: Vec<(String, String)> = defaults
            .table_properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        config.extend([
            (
                "delta.checkpointInterval".to_string(),
                defaults.checkpoint_interval.to_string(),
            ),
            (
                "delta.logRetentionDuration".to_string(),
                format!("interval {} seconds", defaults.log_retention.as_secs()),
            ),
            (
                "delta.enableDeletionVectors".to_string(),
                "false".to_string(),
            ),
            (
                IDENTIFIER_KEY.to_string(),
                serde_json::to_string(&spec.identifier).expect("strings serialize"),
            ),
        ]);
        let created = self
            .table
            .clone()
            .create()
            .with_columns(columns)
            .with_partition_columns(partitions)
            .with_configuration(config.into_iter().map(|(k, v)| (k, Some(v))))
            .with_raise_if_key_not_exists(false)
            .with_table_name(format!("{}.{}", spec.table.schema, spec.table.name))
            .with_save_mode(SaveMode::Ignore)
            .await;
        match created {
            Ok(table) => {
                self.table = table;
                Ok(())
            }
            // Another writer created it between the check and the commit.
            Err(e) => match self.refresh().await {
                Ok(true) => Ok(()),
                _ => Err(sink_error(e)),
            },
        }
    }

    /// The table's columns in Hop 2 terms (soft-dropped columns included).
    pub fn schema(&self) -> Result<SchemaRef, SinkError> {
        let snapshot = self.table.snapshot().map_err(sink_error)?;
        hop2_schema(snapshot.schema().as_ref()).map_err(permanent)
    }

    /// The identifier columns recorded at creation, empty for a table this sink didn't create.
    pub fn identifier(&self) -> Result<Vec<String>, SinkError> {
        let snapshot = self.table.snapshot().map_err(sink_error)?;
        match snapshot.metadata().configuration().get(IDENTIFIER_KEY) {
            Some(text) => serde_json::from_str(text)
                .map_err(|e| permanent(format!("table property {IDENTIFIER_KEY}: {e}"))),
            None => Ok(Vec::new()),
        }
    }

    /// The fence: the owner epoch of the latest commit of `pipeline_id` (its `txn` version).
    pub async fn fence_epoch(&self, pipeline_id: &str) -> Result<Option<u64>, SinkError> {
        let snapshot = self.table.snapshot().map_err(sink_error)?;
        let version = snapshot
            .transaction_version(self.table.log_store().as_ref(), app_id(pipeline_id))
            .await
            .map_err(sink_error)?;
        version
            .map(|v| u64::try_from(v).map_err(|_| permanent(format!("txn version {v}"))))
            .transpose()
    }

    /// The position of the newest commit of `pipeline_id`, walking back past foreign commits.
    /// `None` when none is left in the log.
    pub async fn stored_position(
        &mut self,
        pipeline_id: &str,
    ) -> Result<Option<SourcePosition>, SinkError> {
        let latest = self.version();
        let stop = self.known.as_ref().map(|(v, _)| *v);
        let log = self.table.log_store();
        match walk_back(log.as_ref(), latest, stop, pipeline_id)
            .await
            .map_err(permanent)?
        {
            WalkBack::Found { version, position } => {
                self.known = Some((version, position.clone()));
                Ok(Some(position))
            }
            // A missing commit file ends the walk; an older known position is still a safe
            // guard (it skips less).
            WalkBack::Reached | WalkBack::NotFound => {
                Ok(self.known.as_ref().map(|(_, p)| p.clone()))
            }
        }
    }

    /// Records an own commit this handle just wrote.
    pub fn remember(&mut self, version: u64, position: SourcePosition) {
        self.known = Some((version, position));
    }

    /// Every row in Hop 2 terms, in identifier order: the Delta counterpart of `MemorySink::content`.
    pub async fn content(&self) -> Result<RecordBatch, SinkError> {
        let schema = self.schema()?;
        let ctx = SessionContext::new();
        self.table
            .update_datafusion_session(&ctx.state())
            .map_err(sink_error)?;
        let provider = self
            .table
            .table_provider()
            .await
            .map_err(|e| sink_error(e.into()))?;
        let batches = ctx
            .read_table(provider)
            .map_err(|e| sink_error(e.into()))?
            .collect()
            .await
            .map_err(|e| sink_error(e.into()))?;
        let snapshot = self.table.snapshot().map_err(sink_error)?;
        let delta_schema = snapshot.schema();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for field in schema.fields() {
            let delta = delta_schema
                .field(field.name())
                .expect("the Hop 2 schema comes from this snapshot");
            let parts: Vec<ArrayRef> = batches
                .iter()
                .map(|b| {
                    b.column_by_name(field.name())
                        .cloned()
                        .ok_or_else(|| permanent(format!("the scan lacks {:?}", field.name())))
                })
                .collect::<Result<_, _>>()?;
            let logical = column_logical(delta).map_err(permanent)?;
            let physical = crate::types::physical_type(delta.data_type()).map_err(permanent)?;
            let mut read = Vec::new();
            for p in &parts {
                read.push(arrow_cast::cast(p, &physical).map_err(|e| permanent(e.to_string()))?);
            }
            let joined = if read.is_empty() {
                arrow_array::new_empty_array(&physical)
            } else {
                let refs: Vec<&dyn arrow_array::Array> = read.iter().map(|a| a.as_ref()).collect();
                arrow_select::concat::concat(&refs).map_err(|e| permanent(e.to_string()))?
            };
            columns.push(from_delta(&joined, &logical, delta.data_type()).map_err(permanent)?);
        }
        let batch =
            RecordBatch::try_new(schema.clone(), columns).map_err(|e| permanent(e.to_string()))?;
        let identifier = self.identifier()?;
        if identifier.is_empty() || batch.num_rows() == 0 {
            return Ok(batch);
        }
        let keys: Vec<ArrayRef> = identifier
            .iter()
            .map(|k| {
                batch
                    .column_by_name(k)
                    .cloned()
                    .ok_or_else(|| permanent(format!("identifier column {k:?} is gone")))
            })
            .collect::<Result<_, _>>()?;
        let converter = RowConverter::new(
            keys.iter()
                .map(|k| SortField::new(k.data_type().clone()))
                .collect(),
        )
        .map_err(|e| permanent(e.to_string()))?;
        let rows = converter
            .convert_columns(&keys)
            .map_err(|e| permanent(e.to_string()))?;
        let mut order: Vec<u32> = (0..batch.num_rows() as u32).collect();
        order.sort_by(|a, b| rows.row(*a as usize).cmp(&rows.row(*b as usize)));
        let order = arrow_array::UInt32Array::from(order);
        let sorted = batch
            .columns()
            .iter()
            .map(|c| arrow_select::take::take(c, &order, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| permanent(e.to_string()))?;
        RecordBatch::try_new(Arc::clone(&schema), sorted).map_err(|e| permanent(e.to_string()))
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: PASS. `data-avalanche-sink-delta` unit tests: 29.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/src/lib.rs \
  crates/data-avalanche-sink-delta/src/error.rs \
  crates/data-avalanche-sink-delta/src/table.rs \
  crates/data-avalanche-sink-delta/src/test_support.rs
git commit -m "feat(sink-delta): error classes and the table handle"
```

---

### Task 7: MERGE, truncate and the marker-only commit

**Files:**
- Modify: `crates/data-avalanche-sink-delta/src/lib.rs`
- Modify: `crates/data-avalanche-sink-delta/src/test_support.rs`
- Create: `crates/data-avalanche-sink-delta/src/write.rs`

**Interfaces:**
- Consumes: `to_delta` (Task 4); `logical_of`, `column_logical`, `physical_type` (Task 3);
  `permanent` (Task 6); `TableHandle` and the fixtures (tests); `UNCHANGED_MASK_PREFIX` (core).
- Produces:
  - `DELETE_FLAG = "__data_avalanche_del"` (crate root).
  - `check_columns(&TableChangeSet, &StructType) -> Result<Vec<DeltaType>, SinkError>`: each set
    column's Delta type; a column the table lacks or holds as another type is `Permanent`.
  - `merge_source(&TableChangeSet, &[DeltaType]) -> Result<Vec<RecordBatch>, SinkError>`.
  - `session(Option<usize>) -> Result<SessionContext, SinkError>`.
  - `async merge(DeltaTable, &TableChangeSet, &[DeltaType], CommitProperties, Option<usize>) ->
    Result<(DeltaTable, MergeMetrics), DeltaTableError>`.
  - `async truncate(DeltaTable, CommitProperties) -> Result<DeltaTable, DeltaTableError>`.
  - `async marker_only(&DeltaTable, CommitProperties, app_id: &str, epoch: i64) -> Result<u64, DeltaTableError>`.
  - `test_support` adds `Row`, `upserts(&SchemaRef, &[Row], Option<&[bool]>)`, `deletes(&[i64])`,
    `change_set(end, upserts, deletes)`.

D-M4.4, D-M4.8, D-M4.9, D-M4.14 (the memory limit).

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-sink-delta/src/lib.rs` with:

```rust
//! The Delta Lake realization of the `data_avalanche_core::Sink` trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.

mod config;
pub mod convert;
mod error;
pub mod marker;
#[allow(dead_code)] // `DeltaSink` (Task 9) is the caller
mod table;
#[cfg(test)]
mod test_support;
pub mod types;
#[allow(dead_code)] // `DeltaSink` (Task 9) is the caller
mod write;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
pub use table::IDENTIFIER_KEY;
pub use write::DELETE_FLAG;
```

Replace `crates/data-avalanche-sink-delta/src/test_support.rs` with:

```rust
//! Fixtures for the crate's unit tests: a local table of a small schema.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{Field, Schema, SchemaRef};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    ColumnSchema, ColumnValueType as V, PositionRange, SourcePosition, TableChangeSet, TableRef,
    TableSchema, TableSpec, delete_schema, unchanged_mask_field, upsert_schema,
};

use crate::DeltaLocation;
use crate::table::TableHandle;

/// `public.orders (id bigint key, doc text, ref uuid, at timestamp)`.
pub(crate) fn orders() -> TableSchema {
    TableSchema::new(
        TableRef::new("public", "orders"),
        vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("doc", V::Text, "text"),
            ColumnSchema::new("ref", V::Uuid, "uuid"),
            ColumnSchema::new("at", V::Timestamp { with_tz: false }, "timestamp"),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

pub(crate) fn spec(schema: &TableSchema) -> TableSpec {
    TableSpec::new(schema, vec![]).unwrap()
}

pub(crate) fn local(dir: &tempfile::TempDir) -> DeltaLocation {
    DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    }
}

pub(crate) async fn handle(dir: &tempfile::TempDir, table: &TableRef) -> TableHandle {
    TableHandle::open(local(dir).table_url(table), HashMap::new())
        .await
        .unwrap()
}

/// One `orders` row: `ref` is 16 bytes of `id`, `at` is `id` ms, `doc` as given.
pub(crate) type Row<'a> = (i64, Option<&'a str>);

/// Upsert rows of `schema` (a prefix of `orders`' upsert schema: all of it, or without `doc`),
/// with an unchanged mask on `doc` when `mask` is given.
pub(crate) fn upserts(schema: &SchemaRef, rows: &[Row], mask: Option<&[bool]>) -> RecordBatch {
    let n = rows.len();
    let mut fields: Vec<Arc<Field>> = schema.fields().to_vec();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for f in schema.fields() {
        let s = |v: &str| Arc::new(StringArray::from(vec![v; n])) as ArrayRef;
        columns.push(match f.name().as_str() {
            "id" => Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            "doc" => Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
            "ref" => Arc::new(
                FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| [r.0 as u8; 16])).unwrap(),
            ),
            "at" => Arc::new(TimestampMicrosecondArray::from_iter_values(
                rows.iter().map(|r| r.0 * 1000),
            )),
            "_ts" => Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|_| 0))
                    .with_timezone("UTC"),
            ),
            "_op" => s("I"),
            "_lsn" => s("0/1"),
            "_seq" => s("00000000/00000001"),
            "_source_db" => s("h/db"),
            "_pipeline" => s("p"),
            other => panic!("no fixture for {other}"),
        });
    }
    if let Some(m) = mask {
        fields.push(Arc::new(unchanged_mask_field("doc")));
        columns.push(Arc::new(BooleanArray::from(m.to_vec())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

pub(crate) fn deletes(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        delete_schema(&orders(), &[]).unwrap(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap()
}

/// A change set of `orders` ending at LSN `end`.
pub(crate) fn change_set(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeSet {
    TableChangeSet {
        table: orders().table().clone(),
        schema: upsert_schema(&orders()),
        identifier: vec!["id".into()],
        truncate: false,
        upserts: up,
        deletes: del,
        position: PositionRange {
            start: SourcePosition::Pg(Lsn(0)),
            end: SourcePosition::Pg(Lsn(end)),
        },
        split: None,
    }
}
```

Create `crates/data-avalanche-sink-delta/src/write.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{SourcePosition, TableRef, upsert_schema};
    use deltalake::kernel::Transaction;

    use super::*;
    use crate::TableDefaults;
    use crate::marker::{app_id, commit_metadata};
    use crate::table::TableHandle;
    use crate::test_support::{Row, change_set, deletes, handle, orders, spec, upserts};

    fn props(epoch: u64, lsn: u64) -> CommitProperties {
        CommitProperties::default()
            .with_application_transaction(Transaction::new(app_id("p"), epoch as i64))
            .with_metadata(commit_metadata(
                "p",
                epoch,
                Some(&SourcePosition::Pg(Lsn(lsn))),
            ))
    }

    async fn created(dir: &tempfile::TempDir) -> TableHandle {
        let mut h = handle(dir, &TableRef::new("public", "orders")).await;
        h.create(&spec(&orders()), &TableDefaults::default())
            .await
            .unwrap();
        h
    }

    /// Commits `cs` by MERGE, returning the metrics.
    async fn merged(h: &mut TableHandle, cs: &TableChangeSet, lsn: u64) -> MergeMetrics {
        let snapshot = h.table().snapshot().unwrap();
        let types = check_columns(cs, snapshot.schema().as_ref()).unwrap();
        let (table, metrics) = merge(h.table().clone(), cs, &types, props(1, lsn), None)
            .await
            .unwrap();
        h.set_table(table);
        metrics
    }

    async fn rows(h: &TableHandle) -> Vec<(i64, Option<String>)> {
        let c = h.content().await.unwrap();
        let ids = c.column(0).as_primitive::<Int64Type>();
        let docs = c.column(1).as_string::<i32>();
        (0..c.num_rows())
            .map(|i| {
                (
                    ids.value(i),
                    docs.is_valid(i).then(|| docs.value(i).to_string()),
                )
            })
            .collect()
    }

    fn owned(rows: &[Row]) -> Vec<(i64, Option<String>)> {
        rows.iter()
            .map(|(i, d)| (*i, d.map(String::from)))
            .collect()
    }

    fn full() -> arrow_schema::SchemaRef {
        upsert_schema(&orders())
    }

    #[tokio::test]
    async fn updates_deletes_and_inserts_land_in_one_version() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let seed = change_set(
            10,
            vec![upserts(
                &full(),
                &[(1, Some("a")), (2, Some("b")), (3, Some("c"))],
                None,
            )],
            vec![],
        );
        merged(&mut h, &seed, 10).await;
        let before = h.version();
        let cs = change_set(
            20,
            vec![upserts(&full(), &[(1, Some("A")), (4, Some("d"))], None)],
            vec![deletes(&[2])],
        );
        let m = merged(&mut h, &cs, 20).await;
        assert_eq!(h.version(), before + 1, "one new version");
        assert_eq!(
            (
                m.num_target_rows_updated,
                m.num_target_rows_deleted,
                m.num_target_rows_inserted
            ),
            (1, 1, 1)
        );
        assert_eq!(
            rows(&h).await,
            owned(&[(1, Some("A")), (3, Some("c")), (4, Some("d"))])
        );
        // Replaying the same set gives the same table (probe P2).
        merged(&mut h, &cs, 20).await;
        assert_eq!(
            rows(&h).await,
            owned(&[(1, Some("A")), (3, Some("c")), (4, Some("d"))])
        );
    }

    #[tokio::test]
    async fn a_masked_cell_keeps_the_stored_value_and_a_new_row_gets_null() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        merged(
            &mut h,
            &change_set(
                10,
                vec![upserts(&full(), &[(1, Some("big"))], None)],
                vec![],
            ),
            10,
        )
        .await;
        let masked = upserts(
            &full(),
            &[(1, None), (2, None), (3, Some("x"))],
            Some(&[true, true, false]),
        );
        merged(&mut h, &change_set(20, vec![masked], vec![]), 20).await;
        assert_eq!(
            rows(&h).await,
            owned(&[(1, Some("big")), (2, None), (3, Some("x"))])
        );
    }

    #[tokio::test]
    async fn a_table_column_the_set_lacks_is_written_null() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        merged(
            &mut h,
            &change_set(10, vec![upserts(&full(), &[(1, Some("a"))], None)], vec![]),
            10,
        )
        .await;
        // A set without `doc` (a soft-dropped column): the update and the insert write NULL.
        let fields: Vec<_> = full()
            .fields()
            .iter()
            .filter(|f| f.name() != "doc")
            .cloned()
            .collect();
        let narrow = Arc::new(Schema::new(fields));
        let mut cs = change_set(
            20,
            vec![upserts(&narrow, &[(1, None), (2, None)], None)],
            vec![],
        );
        cs.schema = narrow;
        merged(&mut h, &cs, 20).await;
        assert_eq!(rows(&h).await, owned(&[(1, None), (2, None)]));
    }

    #[tokio::test]
    async fn a_set_column_the_table_lacks_or_types_differently_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let h = created(&dir).await;
        let snapshot = h.table().snapshot().unwrap();
        let mut cs = change_set(10, vec![], vec![]);
        let mut fields = cs.schema.fields().to_vec();
        fields.push(Arc::new(Field::new("extra", DataType::Int32, true)));
        cs.schema = Arc::new(Schema::new(fields));
        let err = check_columns(&cs, snapshot.schema().as_ref()).unwrap_err();
        assert!(err.to_string().contains("evolve first"), "{err}");
        let mut cs = change_set(10, vec![], vec![]);
        let mut fields = cs.schema.fields().to_vec();
        fields[1] = Arc::new(Field::new("doc", DataType::Int32, true));
        cs.schema = Arc::new(Schema::new(fields));
        let err = check_columns(&cs, snapshot.schema().as_ref()).unwrap_err();
        assert!(err.to_string().contains("holds string"), "{err}");
    }

    #[tokio::test]
    async fn a_merge_that_changes_nothing_commits_nothing_so_the_marker_goes_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        merged(
            &mut h,
            &change_set(10, vec![upserts(&full(), &[(1, Some("a"))], None)], vec![]),
            10,
        )
        .await;
        let before = h.version();
        merged(&mut h, &change_set(20, vec![], vec![deletes(&[42])]), 20).await;
        assert_eq!(
            h.version(),
            before,
            "deleting a missing key writes no version"
        );
        let v = marker_only(h.table(), props(2, 20), &app_id("p"), 2)
            .await
            .unwrap();
        assert_eq!(v, before + 1);
        assert!(h.refresh().await.unwrap());
        assert_eq!(h.fence_epoch("p").await.unwrap(), Some(2));
        assert_eq!(
            h.stored_position("p").await.unwrap(),
            Some(SourcePosition::Pg(Lsn(20)))
        );
    }

    #[tokio::test]
    async fn truncate_removes_every_row_without_rewriting() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        merged(
            &mut h,
            &change_set(
                10,
                vec![upserts(&full(), &[(1, Some("a")), (2, Some("b"))], None)],
                vec![],
            ),
            10,
        )
        .await;
        let files_before = h.table().snapshot().unwrap().log_data().num_files();
        assert!(files_before > 0);
        let table = truncate(h.table().clone(), props(1, 20)).await.unwrap();
        h.set_table(table);
        assert_eq!(rows(&h).await, vec![]);
        let commit = h
            .table()
            .log_store()
            .read_commit_entry(h.version())
            .await
            .unwrap()
            .unwrap();
        let text = String::from_utf8(commit.to_vec()).unwrap();
        assert!(text.contains(r#""remove""#), "{text}");
        assert!(!text.contains(r#""add""#), "metadata-only: {text}");
        merged(
            &mut h,
            &change_set(30, vec![upserts(&full(), &[(3, Some("c"))], None)], vec![]),
            30,
        )
        .await;
        assert_eq!(rows(&h).await, owned(&[(3, Some("c"))]));
    }

    #[tokio::test]
    async fn a_row_that_moves_partition_is_matched_by_key_not_duplicated() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = handle(&dir, &TableRef::new("public", "orders")).await;
        let mut s = spec(&orders());
        s.partition_by = vec![data_avalanche_core::PartitionField {
            column: "doc".into(),
            transform: data_avalanche_core::PartitionTransform::Identity,
        }];
        h.create(&s, &TableDefaults::default()).await.unwrap();
        let seed = vec![upserts(&full(), &[(1, Some("a")), (2, Some("a"))], None)];
        merged(&mut h, &change_set(10, seed, vec![]), 10).await;
        // Row 1 moves from partition `a` to `b`; row 2 is deleted by its key alone.
        let cs = change_set(
            20,
            vec![upserts(&full(), &[(1, Some("b"))], None)],
            vec![deletes(&[2])],
        );
        let m = merged(&mut h, &cs, 20).await;
        assert_eq!(
            (m.num_target_rows_updated, m.num_target_rows_deleted),
            (1, 1)
        );
        assert_eq!(rows(&h).await, owned(&[(1, Some("b"))]));
    }

    #[tokio::test]
    async fn a_merge_over_the_memory_limit_fails_instead_of_growing() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let many: Vec<Row> = (0..20_000)
            .map(|i| (i, Some("xxxxxxxxxxxxxxxxxxxxxxxx")))
            .collect();
        let cs = change_set(10, vec![upserts(&full(), &many, None)], vec![]);
        merged(&mut h, &cs, 10).await;
        let snapshot = h.table().snapshot().unwrap();
        let types = check_columns(&cs, snapshot.schema().as_ref()).unwrap();
        let err = merge(h.table().clone(), &cs, &types, props(1, 20), Some(64 << 10))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Resources exhausted"), "{err}");
        merge(h.table().clone(), &cs, &types, props(1, 20), Some(1 << 30))
            .await
            .unwrap();
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `write::DELETE_FLAG`
error[E0425]: cannot find type `CommitProperties` in this scope
error[E0425]: cannot find type `TableChangeSet` in this scope
error[E0425]: cannot find type `MergeMetrics` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-sink-delta/src/write.rs`, above the test module:

```rust
//! The writes of one commit (Data-Flow §Delta Lake realization): the MERGE of a change set, the
//! DELETE of a truncate, and the marker-only commit that moves the position when nothing changed.

use std::ops::Not;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, new_null_array};
use arrow_schema::{DataType, Field, Schema};
use data_avalanche_core::{SinkError, TableChangeSet, UNCHANGED_MASK_PREFIX};
use deltalake::DeltaTable;
use deltalake::datafusion::common::{Column, ScalarValue};
use deltalake::datafusion::execution::SessionStateBuilder;
use deltalake::datafusion::execution::runtime_env::RuntimeEnvBuilder;
use deltalake::datafusion::logical_expr::{Expr, when};
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake::kernel::{DataType as DeltaType, StructType};
use deltalake::operations::merge::MergeMetrics;
use deltalake::protocol::{DeltaOperation, OutputMode};

use crate::convert::to_delta;
use crate::error::permanent;
use crate::types::{column_logical, logical_of, physical_type};

/// The boolean MERGE-source column that marks a delete row.
pub const DELETE_FLAG: &str = "__data_avalanche_del";

fn source(name: &str) -> Expr {
    Expr::Column(Column::new(Some("source"), name))
}

fn target(name: &str) -> Expr {
    Expr::Column(Column::new(Some("target"), name))
}

fn mask_name(column: &str) -> String {
    format!("{UNCHANGED_MASK_PREFIX}{column}")
}

/// Checks the change set against the table: every column of the set is a column of the table
/// holding the same Hop 2 type (the set may lack columns: soft-dropped, or added after it). Returns
/// each set column's Delta type.
pub fn check_columns(cs: &TableChangeSet, table: &StructType) -> Result<Vec<DeltaType>, SinkError> {
    cs.schema
        .fields()
        .iter()
        .map(|f| {
            let column = table.field(f.name()).ok_or_else(|| {
                permanent(format!(
                    "column {:?} isn't in table {} (evolve first)",
                    f.name(),
                    cs.table
                ))
            })?;
            let logical = logical_of(f).map_err(permanent)?;
            let held = column_logical(column).map_err(permanent)?;
            if data_avalanche_core::arrow_type(&held) != *f.data_type() {
                return Err(permanent(format!(
                    "column {:?} of {} holds {held}, the change set has {logical}",
                    f.name(),
                    cs.table
                )));
            }
            Ok(column.data_type().clone())
        })
        .collect()
}

/// The MERGE source: the upserts, then the deletes, in the table's physical types, each row with
/// [`DELETE_FLAG`] and the upserts' unchanged masks (`false` on delete rows). Every field is
/// nullable; a delete row carries only its identifier.
pub fn merge_source(
    cs: &TableChangeSet,
    types: &[DeltaType],
) -> Result<Vec<RecordBatch>, SinkError> {
    let masks: Vec<String> = cs
        .upserts
        .first()
        .map(|b| {
            b.schema().fields()[cs.schema.fields().len()..]
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default();
    let mut fields = Vec::new();
    for (f, t) in cs.schema.fields().iter().zip(types) {
        fields.push(Field::new(
            f.name(),
            physical_type(t).map_err(permanent)?,
            true,
        ));
    }
    fields.extend(
        masks
            .iter()
            .map(|m| Field::new(m, DataType::Boolean, false)),
    );
    fields.push(Field::new(DELETE_FLAG, DataType::Boolean, false));
    let schema = Arc::new(Schema::new(fields));
    let mut batches = Vec::new();
    for b in &cs.upserts {
        let mut columns = Vec::new();
        for (i, (f, t)) in cs.schema.fields().iter().zip(types).enumerate() {
            let logical = logical_of(f).map_err(permanent)?;
            columns.push(to_delta(b.column(i), &logical, t).map_err(permanent)?);
        }
        for m in &masks {
            columns.push(b.column_by_name(m).expect("validated masks").clone());
        }
        columns.push(Arc::new(BooleanArray::from(vec![false; b.num_rows()])));
        batches.push(
            RecordBatch::try_new(schema.clone(), columns).map_err(|e| permanent(e.to_string()))?,
        );
    }
    for b in &cs.deletes {
        let rows = b.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for (f, t) in cs.schema.fields().iter().zip(types) {
            let physical = physical_type(t).map_err(permanent)?;
            columns.push(match cs.identifier.iter().position(|k| k == f.name()) {
                Some(k) => {
                    let logical = logical_of(f).map_err(permanent)?;
                    to_delta(b.column(k), &logical, t).map_err(permanent)?
                }
                None => new_null_array(&physical, rows),
            });
        }
        for _ in &masks {
            columns.push(Arc::new(BooleanArray::from(vec![false; rows])));
        }
        columns.push(Arc::new(BooleanArray::from(vec![true; rows])));
        batches.push(
            RecordBatch::try_new(schema.clone(), columns).map_err(|e| permanent(e.to_string()))?,
        );
    }
    Ok(batches)
}

/// A DataFusion session, with a memory ceiling when one is set (R-6).
pub fn session(memory_limit: Option<usize>) -> Result<SessionContext, SinkError> {
    let Some(limit) = memory_limit else {
        return Ok(SessionContext::new());
    };
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(limit, 1.0)
        .build_arc()
        .map_err(|e| permanent(e.to_string()))?;
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_runtime_env(runtime)
        .with_default_features()
        .build();
    Ok(SessionContext::new_with_state(state))
}

/// One MERGE of the change set's rows (Data-Flow §Delta Lake realization):
/// - matched on the identifier only (a row that moved partitions is still matched);
/// - `WHEN MATCHED AND del THEN DELETE`;
/// - `WHEN MATCHED AND NOT del THEN UPDATE`: a masked column keeps the target's value
///   (`CASE WHEN mask THEN target ELSE source`, D-IMPL-3), a table column the set lacks is NULL;
/// - `WHEN NOT MATCHED AND NOT del THEN INSERT` every table column, NULL where the set has none.
///
/// A MERGE that changes no row writes no commit; the caller checks the version.
pub async fn merge(
    table: DeltaTable,
    cs: &TableChangeSet,
    types: &[DeltaType],
    props: CommitProperties,
    memory_limit: Option<usize>,
) -> Result<(DeltaTable, MergeMetrics), deltalake::DeltaTableError> {
    let batches =
        merge_source(cs, types).map_err(|e| deltalake::DeltaTableError::Generic(e.to_string()))?;
    let ctx =
        session(memory_limit).map_err(|e| deltalake::DeltaTableError::Generic(e.to_string()))?;
    let df = ctx.read_batches(batches)?;
    let columns: Vec<(String, DeltaType)> = table
        .snapshot()?
        .schema()
        .fields()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    let in_set = |name: &str| cs.schema.field_with_name(name).is_ok();
    let masked = |name: &str| {
        cs.upserts
            .first()
            .is_some_and(|b| b.column_by_name(&mask_name(name)).is_some())
    };
    let null = |t: &DeltaType| -> Result<Expr, deltalake::DeltaTableError> {
        let physical = physical_type(t).map_err(deltalake::DeltaTableError::Generic)?;
        Ok(Expr::Literal(ScalarValue::try_from(&physical)?, None))
    };
    let mut on: Option<Expr> = None;
    for k in &cs.identifier {
        let eq = target(k).eq(source(k));
        on = Some(match on {
            Some(prev) => prev.and(eq),
            None => eq,
        });
    }
    let on = on.expect("a change set has an identifier");
    let mut updates = Vec::new();
    let mut inserts = Vec::new();
    for (name, t) in &columns {
        let value = if !in_set(name) {
            null(t)?
        } else {
            source(name)
        };
        if !cs.identifier.contains(name) {
            let update = if in_set(name) && masked(name) {
                when(source(&mask_name(name)), target(name)).otherwise(source(name))?
            } else {
                value.clone()
            };
            updates.push((Column::new_unqualified(name), update));
        }
        inserts.push((Column::new_unqualified(name), value));
    }
    let delete = source(DELETE_FLAG);
    let state = Arc::new(ctx.state());
    table
        .merge(df, on)
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(state)
        .with_commit_properties(props)
        .when_matched_delete(|d| d.predicate(delete.clone()))?
        .when_matched_update(|mut u| {
            u = u.predicate(delete.clone().not());
            for (c, e) in updates {
                u = u.update(c, e);
            }
            u
        })?
        .when_not_matched_insert(|mut i| {
            i = i.predicate(delete.clone().not());
            for (c, e) in inserts {
                i = i.set(c, e);
            }
            i
        })?
        .await
}

/// Removes every row (a TRUNCATE): metadata-only, one commit.
pub async fn truncate(
    table: DeltaTable,
    props: CommitProperties,
) -> Result<DeltaTable, deltalake::DeltaTableError> {
    let (table, _) = table.delete().with_commit_properties(props).await?;
    Ok(table)
}

/// A commit with no data: just the `txn` and the `commitInfo` in `props`. It moves the table's
/// marker when the change set changed no row.
pub async fn marker_only(
    table: &DeltaTable,
    props: CommitProperties,
    app_id: &str,
    epoch: i64,
) -> Result<u64, deltalake::DeltaTableError> {
    let operation = DeltaOperation::StreamingUpdate {
        output_mode: OutputMode::Append,
        query_id: app_id.to_string(),
        epoch_id: epoch,
    };
    let done = CommitBuilder::from(props)
        .build(Some(table.snapshot()?), table.log_store(), operation)
        .await?;
    Ok(done.version())
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: PASS. `data-avalanche-sink-delta` unit tests: 37.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/src/lib.rs \
  crates/data-avalanche-sink-delta/src/test_support.rs \
  crates/data-avalanche-sink-delta/src/write.rs
git commit -m "feat(sink-delta): MERGE, truncate and the marker-only commit"
```

---

### Task 8: Schema evolution

**Files:**
- Modify: `crates/data-avalanche-sink-delta/src/lib.rs`
- Create: `crates/data-avalanche-sink-delta/src/evolve.rs`

**Interfaces:**
- Consumes: `delta_field`, `hop2_field` (Task 3); `permanent`, `sink_error` (Task 6);
  `added_column_field`, `SchemaDelta`, `SchemaChange`, `EvolveOutcome` (core).
- Produces: `async evolve(DeltaTable, &SchemaDelta, NtzAs) -> Result<(DeltaTable, EvolveOutcome), SinkError>`.

D-M4.11.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-sink-delta/src/lib.rs` with:

```rust
//! The Delta Lake realization of the `data_avalanche_core::Sink` trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.

mod config;
pub mod convert;
mod error;
#[allow(dead_code)] // `DeltaSink` (Task 9) is the caller
mod evolve;
pub mod marker;
#[allow(dead_code)] // `DeltaSink` (Task 9) is the caller
mod table;
#[cfg(test)]
mod test_support;
pub mod types;
#[allow(dead_code)] // `DeltaSink` (Task 9) is the caller
mod write;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
pub use table::IDENTIFIER_KEY;
pub use write::DELETE_FLAG;
```

Create `crates/data-avalanche-sink-delta/src/evolve.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V, TableRef};

    use super::*;
    use crate::TableDefaults;
    use crate::table::TableHandle;
    use crate::test_support::{handle, orders, spec};

    async fn created(dir: &tempfile::TempDir) -> TableHandle {
        let mut h = handle(dir, &TableRef::new("public", "orders")).await;
        h.create(&spec(&orders()), &TableDefaults::default())
            .await
            .unwrap();
        h
    }

    fn delta(changes: Vec<SchemaChange>) -> SchemaDelta {
        SchemaDelta {
            table: TableRef::new("public", "orders"),
            changes,
        }
    }

    async fn apply(h: &mut TableHandle, changes: Vec<SchemaChange>) -> EvolveOutcome {
        let (table, outcome) = evolve(h.table().clone(), &delta(changes), NtzAs::default())
            .await
            .unwrap();
        h.set_table(table);
        outcome
    }

    #[tokio::test]
    async fn add_column_is_applied_nullable_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let add = || SchemaChange::AddColumn(ColumnSchema::new("tag", V::Uuid, "uuid").not_null());
        assert_eq!(apply(&mut h, vec![add()]).await, EvolveOutcome::Applied);
        let version = h.version();
        assert_eq!(
            apply(&mut h, vec![add()]).await,
            EvolveOutcome::Applied,
            "replay"
        );
        assert_eq!(h.version(), version, "a replay adds nothing");
        let s = h.schema().unwrap();
        let tag = s.field_with_name("tag").unwrap();
        assert!(tag.is_nullable(), "added columns are nullable (S-O)");
        assert_eq!(
            tag,
            &added_column_field(&ColumnSchema::new("tag", V::Uuid, "uuid"))
        );
    }

    #[tokio::test]
    async fn drop_and_rename_are_degraded_and_keep_the_old_column() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let out = apply(&mut h, vec![SchemaChange::DropColumn("doc".into())]).await;
        assert!(
            matches!(out, EvolveOutcome::Degraded { ref warning } if warning.contains("soft-dropped")),
            "{out:?}"
        );
        assert!(h.schema().unwrap().field_with_name("doc").is_ok());
        let rename = SchemaChange::RenameColumn {
            from: "ref".into(),
            to: "reference".into(),
            proven: true,
        };
        let out = apply(&mut h, vec![rename.clone()]).await;
        assert!(matches!(out, EvolveOutcome::Degraded { .. }), "{out:?}");
        let s = h.schema().unwrap();
        let old = s.field_with_name("ref").unwrap();
        let new = s.field_with_name("reference").unwrap();
        assert_eq!(new.data_type(), old.data_type());
        assert_eq!(new.metadata(), old.metadata(), "the uuid tag moves along");
        let version = h.version();
        apply(&mut h, vec![rename]).await;
        assert_eq!(h.version(), version, "a replayed rename adds nothing");
    }

    #[tokio::test]
    async fn type_changes_and_structural_changes_are_unsupported() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir).await;
        let widen = SchemaChange::ChangeType {
            column: "id".into(),
            from: ColumnSchema::new("id", V::Int32, "integer"),
            to: ColumnSchema::new("id", V::Int64, "bigint"),
            narrowed: false,
        };
        for change in [
            widen,
            SchemaChange::IdentifierChanged,
            SchemaChange::TableDropped,
        ] {
            let out = apply(&mut h, vec![change]).await;
            assert!(matches!(out, EvolveOutcome::Unsupported { .. }), "{out:?}");
        }
        let clash = SchemaChange::AddColumn(ColumnSchema::new("doc", V::Int64, "bigint"));
        assert!(matches!(
            apply(&mut h, vec![clash]).await,
            EvolveOutcome::Unsupported { .. }
        ));
        for nothing in [
            SchemaChange::SetNotNull("doc".into()),
            SchemaChange::DropNotNull("doc".into()),
            SchemaChange::EnumValuesAppended("doc".into()),
            SchemaChange::NoTargetEffect("index".into()),
        ] {
            assert_eq!(apply(&mut h, vec![nothing]).await, EvolveOutcome::Applied);
        }
        assert_eq!(h.version(), 0, "nothing was written");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0425]: cannot find type `SchemaChange` in this scope
error[E0425]: cannot find type `SchemaDelta` in this scope
error[E0422]: cannot find struct, variant or union type `SchemaDelta` in this scope
error[E0425]: cannot find type `EvolveOutcome` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-sink-delta/src/evolve.rs`, above the test module:

```rust
//! Schema evolution on Delta (Schema_Change §7.1.2, §7.2 Delta column; D-MS.9). delta-rs 1.0 adds
//! columns and nothing else, so:
//! - ADD COLUMN → `Applied` (nullable, Hop 3b type);
//! - DROP COLUMN → `Degraded`: a soft-drop, the column stays and later sets write it NULL;
//! - RENAME → `Degraded`: the new name is added with the old column's type, the old one stays;
//! - NOT NULL changes, appended enum values, changes with no target effect → `Applied`, nothing
//!   to do (target columns are nullable, S-O);
//! - everything else (a type change, an identifier change, a dropped table …) → `Unsupported`.
//!
//! Every step is idempotent: a replayed delta finds its columns and adds nothing (§8.3).

use data_avalanche_core::{
    EvolveOutcome, SchemaChange, SchemaDelta, SinkError, added_column_field,
};
use deltalake::DeltaTable;
use deltalake::kernel::StructField;

use crate::NtzAs;
use crate::error::{permanent, sink_error};
use crate::types::{delta_field, hop2_field};

/// Applies `delta` to `table`. The table is returned as it is after the change (unchanged when
/// nothing was added).
pub async fn evolve(
    table: DeltaTable,
    delta: &SchemaDelta,
    ntz: NtzAs,
) -> Result<(DeltaTable, EvolveOutcome), SinkError> {
    let schema = table.snapshot().map_err(sink_error)?.schema();
    let mut added: Vec<StructField> = Vec::new();
    let mut warnings = Vec::new();
    let has = |name: &str, added: &[StructField]| {
        schema
            .field(name)
            .cloned()
            .or_else(|| added.iter().find(|f| f.name() == name).cloned())
    };
    for change in &delta.changes {
        match change {
            SchemaChange::AddColumn(column) => {
                let field = added_column_field(column);
                match has(&column.name, &added) {
                    Some(existing) => {
                        let held = hop2_field(&existing).map_err(permanent)?;
                        if held.data_type() != field.data_type() {
                            return Ok((
                                table,
                                EvolveOutcome::Unsupported {
                                    reason: format!(
                                        "column {:?} exists as {}, the source added it as {}",
                                        column.name,
                                        held.data_type(),
                                        field.data_type()
                                    ),
                                },
                            ));
                        }
                    }
                    None => added.push(delta_field(&field, ntz).map_err(permanent)?),
                }
            }
            SchemaChange::DropColumn(name) => {
                warnings.push(format!("column {name:?} soft-dropped: kept, written NULL"));
            }
            SchemaChange::RenameColumn { from, to, .. } => {
                let old = has(from, &added).ok_or_else(|| {
                    permanent(format!("renamed column {from:?} isn't in the table"))
                })?;
                if has(to, &added).is_none() {
                    added.push(
                        StructField::new(to, old.data_type().clone(), true)
                            .with_metadata(old.metadata().clone()),
                    );
                }
                warnings.push(format!(
                    "column {from:?} renamed to {to:?}: added {to:?}, kept {from:?} (history stays under the old name)"
                ));
            }
            SchemaChange::SetNotNull(_)
            | SchemaChange::DropNotNull(_)
            | SchemaChange::EnumValuesAppended(_)
            | SchemaChange::NoTargetEffect(_) => {}
            other => {
                return Ok((
                    table,
                    EvolveOutcome::Unsupported {
                        reason: format!("{other:?} isn't supported by the Delta sink"),
                    },
                ));
            }
        }
    }
    let table = if added.is_empty() {
        table
    } else {
        table
            .add_columns()
            .with_fields(added)
            .await
            .map_err(sink_error)?
    };
    let outcome = if warnings.is_empty() {
        EvolveOutcome::Applied
    } else {
        EvolveOutcome::Degraded {
            warning: warnings.join("; "),
        }
    };
    Ok((table, outcome))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: PASS. `data-avalanche-sink-delta` unit tests: 40.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/src/lib.rs \
  crates/data-avalanche-sink-delta/src/evolve.rs
git commit -m "feat(sink-delta): schema evolution"
```

---

### Task 9: `DeltaSink`: the `Sink` trait on Delta tables

**Files:**
- Modify: `crates/data-avalanche-sink-delta/src/lib.rs`
- Modify: `crates/data-avalanche-sink-delta/src/test_support.rs`
- Create: `crates/data-avalanche-sink-delta/src/sink.rs`

**Interfaces:**
- Consumes: every module above.
- Produces (crate root):
  - `DeltaSink::new(DeltaSinkConfig)`, `stats() -> DeltaStats`, `async content(&TableRef) ->
    Result<Option<RecordBatch>, SinkError>` (the Delta counterpart of `MemorySink::content`).
  - `impl Sink for DeltaSink`: `kind()` = `Delta`; `capabilities()` = mirror, add column, identity
    partitions; `table_state`, `ensure_table`, `evolve`, `commit`.
  - `DeltaStats { commits, skipped, fenced, retries, rows_inserted, rows_updated, rows_deleted,
    rows_copied, files_added, files_removed }`, `COMMIT_ATTEMPTS = 3`.
  - `test_support` adds `marker(epoch, &TableChangeSet) -> CommitMarker`.

D-M4.5, D-M4.7, D-M4.9, D-M4.14, D-M4.15. `lib.rs` takes its final form: the dead-code
attributes of Tasks 6–8 go, and the crate docs link the public items.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-sink-delta/src/lib.rs` with:

```rust
//! The Delta Lake realization of the [`data_avalanche_core::Sink`] trait (Data-Flow §Delta Lake
//! realization, Multi_Sink §7.2), on delta-rs 1.0.
//!
//! - [`DeltaSink`] commits each `TableChangeSet` as one MERGE, fenced by a `txn`
//!   app-transaction and positioned by `commitInfo` ([`marker`]).
//! - [`types`] and [`convert`] are Hop 3b: Hop 2 Arrow ↔ Delta types and values.
//! - Tables are path-based (no catalog in v1), under the location config §10 names.

mod config;
pub mod convert;
mod error;
mod evolve;
pub mod marker;
mod sink;
mod table;
#[cfg(test)]
mod test_support;
pub mod types;
mod write;

pub use config::{DeltaLocation, DeltaSinkConfig, NtzAs, TableDefaults};
pub use sink::{COMMIT_ATTEMPTS, DeltaSink, DeltaStats};
pub use table::IDENTIFIER_KEY;
pub use write::DELETE_FLAG;
```

Replace `crates/data-avalanche-sink-delta/src/test_support.rs` with:

```rust
//! Fixtures for the crate's unit tests: a local table of a small schema.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{Field, Schema, SchemaRef};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    ColumnSchema, ColumnValueType as V, CommitMarker, PositionRange, SourcePosition,
    TableChangeSet, TableRef, TableSchema, TableSpec, delete_schema, unchanged_mask_field,
    upsert_schema,
};

use crate::DeltaLocation;
use crate::table::TableHandle;

/// `public.orders (id bigint key, doc text, ref uuid, at timestamp)`.
pub(crate) fn orders() -> TableSchema {
    TableSchema::new(
        TableRef::new("public", "orders"),
        vec![
            ColumnSchema::new("id", V::Int64, "bigint").not_null(),
            ColumnSchema::new("doc", V::Text, "text"),
            ColumnSchema::new("ref", V::Uuid, "uuid"),
            ColumnSchema::new("at", V::Timestamp { with_tz: false }, "timestamp"),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

pub(crate) fn spec(schema: &TableSchema) -> TableSpec {
    TableSpec::new(schema, vec![]).unwrap()
}

pub(crate) fn local(dir: &tempfile::TempDir) -> DeltaLocation {
    DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    }
}

pub(crate) async fn handle(dir: &tempfile::TempDir, table: &TableRef) -> TableHandle {
    TableHandle::open(local(dir).table_url(table), HashMap::new())
        .await
        .unwrap()
}

/// One `orders` row: `ref` is 16 bytes of `id`, `at` is `id` ms, `doc` as given.
pub(crate) type Row<'a> = (i64, Option<&'a str>);

/// Upsert rows of `schema` (a prefix of `orders`' upsert schema: all of it, or without `doc`),
/// with an unchanged mask on `doc` when `mask` is given.
pub(crate) fn upserts(schema: &SchemaRef, rows: &[Row], mask: Option<&[bool]>) -> RecordBatch {
    let n = rows.len();
    let mut fields: Vec<Arc<Field>> = schema.fields().to_vec();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for f in schema.fields() {
        let s = |v: &str| Arc::new(StringArray::from(vec![v; n])) as ArrayRef;
        columns.push(match f.name().as_str() {
            "id" => Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            "doc" => Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
            "ref" => Arc::new(
                FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| [r.0 as u8; 16])).unwrap(),
            ),
            "at" => Arc::new(TimestampMicrosecondArray::from_iter_values(
                rows.iter().map(|r| r.0 * 1000),
            )),
            "_ts" => Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|_| 0))
                    .with_timezone("UTC"),
            ),
            "_op" => s("I"),
            "_lsn" => s("0/1"),
            "_seq" => s("00000000/00000001"),
            "_source_db" => s("h/db"),
            "_pipeline" => s("p"),
            other => panic!("no fixture for {other}"),
        });
    }
    if let Some(m) = mask {
        fields.push(Arc::new(unchanged_mask_field("doc")));
        columns.push(Arc::new(BooleanArray::from(m.to_vec())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

pub(crate) fn deletes(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        delete_schema(&orders(), &[]).unwrap(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap()
}

/// A change set of `orders` ending at LSN `end`.
pub(crate) fn change_set(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeSet {
    TableChangeSet {
        table: orders().table().clone(),
        schema: upsert_schema(&orders()),
        identifier: vec!["id".into()],
        truncate: false,
        upserts: up,
        deletes: del,
        position: PositionRange {
            start: SourcePosition::Pg(Lsn(0)),
            end: SourcePosition::Pg(Lsn(end)),
        },
        split: None,
    }
}

pub(crate) fn marker(epoch: u64, cs: &TableChangeSet) -> CommitMarker {
    CommitMarker {
        owner_epoch: epoch,
        committed_position: cs.position.end.clone(),
        pipeline_id: "p".into(),
    }
}
```

Create `crates/data-avalanche-sink-delta/src/sink.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::position::Lsn;

    use super::*;
    use crate::TableDefaults;
    use crate::test_support::{change_set, local, marker, orders, spec, upserts};

    fn sink(dir: &tempfile::TempDir) -> DeltaSink {
        DeltaSink::new(DeltaSinkConfig {
            pipeline_id: "p".into(),
            location: local(dir),
            table: TableDefaults::default(),
            merge_memory_limit: None,
        })
    }

    fn rows(end: u64, ids: &[i64]) -> TableChangeSet {
        let full = data_avalanche_core::upsert_schema(&orders());
        let rows: Vec<_> = ids.iter().map(|i| (*i, Some("x"))).collect();
        change_set(end, vec![upserts(&full, &rows, None)], vec![])
    }

    /// Commits through `attempt` on the sink's current handle without re-reading it first.
    async fn stale_attempt(s: &DeltaSink, cs: &TableChangeSet, epoch: u64) -> Step {
        let mut guard = s.lock(&cs.table).await.unwrap();
        let h = guard.as_mut().unwrap();
        s.attempt(h, cs, &marker(epoch, cs), false).await.unwrap()
    }

    #[tokio::test]
    async fn a_zombie_that_loses_the_race_is_fenced_on_the_retry() {
        let dir = tempfile::tempdir().unwrap();
        let (zombie, owner) = (sink(&dir), sink(&dir));
        zombie.ensure_table(&spec(&orders())).await.unwrap();
        let first = rows(10, &[1]);
        zombie
            .commit(first.clone(), marker(5, &first))
            .await
            .unwrap();
        // The zombie holds the table at epoch 5; the new owner commits at epoch 6.
        let theirs = rows(20, &[2]);
        owner
            .commit(theirs.clone(), marker(6, &theirs))
            .await
            .unwrap();
        // Without re-reading, the fence pre-check passes and delta-rs refuses the commit: the
        // same app id committed since the zombie's snapshot (probe P4, stale case).
        let late = rows(15, &[3]);
        match stale_attempt(&zombie, &late, 5).await {
            Step::Retry(reason) => assert!(reason.contains("Concurrent transaction"), "{reason}"),
            Step::Done(o) => panic!("{o:?}"),
        }
        // The retry re-reads the table and the pre-check fences it.
        let out = zombie.commit(late.clone(), marker(5, &late)).await.unwrap();
        assert_eq!(out, CommitOutcome::Fenced { current_epoch: 6 });
        assert_eq!(zombie.stats().fenced, 1);
    }

    #[tokio::test]
    async fn a_race_with_a_foreign_optimize_is_retried_and_lands() {
        let dir = tempfile::tempdir().unwrap();
        let s = sink(&dir);
        s.ensure_table(&spec(&orders())).await.unwrap();
        for (end, id) in [(10, 1), (20, 2)] {
            let cs = rows(end, &[id]);
            s.commit(cs.clone(), marker(1, &cs)).await.unwrap();
        }
        // Someone compacts the table behind the sink's back.
        let url = local(&dir).table_url(&orders().table().clone());
        let t = deltalake::open_table(url).await.unwrap();
        t.optimize().await.unwrap();
        let update = rows(30, &[1]);
        match stale_attempt(&s, &update, 1).await {
            Step::Retry(reason) => assert!(reason.contains("commit"), "{reason}"),
            Step::Done(o) => panic!("{o:?}"),
        }
        let out = s.commit(update.clone(), marker(1, &update)).await.unwrap();
        assert!(matches!(out, CommitOutcome::Committed { .. }), "{out:?}");
        let state = s.table_state(&update.table).await.unwrap();
        assert_eq!(
            state.marker.unwrap().committed_position,
            SourcePosition::Pg(Lsn(30))
        );
    }

    #[tokio::test]
    async fn a_marker_of_another_pipeline_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let s = sink(&dir);
        s.ensure_table(&spec(&orders())).await.unwrap();
        let cs = rows(10, &[1]);
        let mut m = marker(1, &cs);
        m.pipeline_id = "other".into();
        let err = s.commit(cs, m).await.unwrap_err();
        assert!(err.to_string().contains("\"other\""), "{err}");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `sink::COMMIT_ATTEMPTS`, `sink::DeltaSink`, `sink::DeltaStats`
error[E0425]: cannot find type `DeltaSink` in this scope
error[E0422]: cannot find struct, variant or union type `DeltaSinkConfig` in this scope
error[E0425]: cannot find type `TableChangeSet` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-sink-delta/src/sink.rs`, above the test module:

```rust
//! [`DeltaSink`]: the `Sink` contract (core `sink.rs`, Data-Flow Phase 4) on Delta tables.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use data_avalanche_core::{
    CommitMarker, CommitOutcome, EvolveOutcome, SchemaDelta, SchemaOp, Sink, SinkCapabilities,
    SinkError, SinkKind, SinkTableState, SourcePosition, TableChangeSet, TableRef, TableSpec,
    TransformKind, WriteMode,
};
use deltalake::DeltaTableError;
use deltalake::kernel::Transaction;
use deltalake::kernel::transaction::{CommitProperties, TransactionError};
use deltalake::operations::merge::MergeMetrics;
use tokio::sync::OwnedMutexGuard;

use crate::DeltaSinkConfig;
use crate::error::{permanent, sink_error};
use crate::evolve::evolve;
use crate::marker::{app_id, commit_metadata};
use crate::table::TableHandle;
use crate::write::{check_columns, marker_only, merge, truncate};

/// How many times one `commit` call re-reads the table and tries again after losing a race.
pub const COMMIT_ATTEMPTS: usize = 3;

/// What the sink has done since it was built (the merge metrics of Data-Flow §Delta Lake
/// realization, item 4; M10 exports them).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeltaStats {
    pub commits: u64,
    pub skipped: u64,
    pub fenced: u64,
    /// Attempts repeated after a lost race.
    pub retries: u64,
    pub rows_inserted: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
    /// Rows rewritten unchanged because they share a file with a changed row
    /// (`data_avalanche_sink_merge_rows_copied`).
    pub rows_copied: u64,
    pub files_added: u64,
    /// Files a MERGE replaced (`data_avalanche_sink_merge_files_rewritten`).
    pub files_removed: u64,
}

impl DeltaStats {
    fn add(&mut self, m: &MergeMetrics) {
        self.rows_inserted += m.num_target_rows_inserted as u64;
        self.rows_updated += m.num_target_rows_updated as u64;
        self.rows_deleted += m.num_target_rows_deleted as u64;
        self.rows_copied += m.num_target_rows_copied as u64;
        self.files_added += m.num_target_files_added as u64;
        self.files_removed += m.num_target_files_removed as u64;
    }
}

type Slot = Arc<tokio::sync::Mutex<Option<TableHandle>>>;

/// The Delta Lake sink. One per pipeline; calls on one table run one at a time.
#[derive(Debug)]
pub struct DeltaSink {
    config: DeltaSinkConfig,
    capabilities: SinkCapabilities,
    options: HashMap<String, String>,
    tables: Mutex<HashMap<TableRef, Slot>>,
    stats: Mutex<DeltaStats>,
}

enum Step {
    Done(CommitOutcome),
    /// Lost a race with another commit: re-read the table and try again.
    Retry(String),
}

/// A lost race is retried; any other failure is the call's result.
fn race_or_fail(e: DeltaTableError) -> Result<Step, SinkError> {
    use TransactionError as T;
    match &e {
        DeltaTableError::Transaction {
            source: T::CommitConflict(_) | T::MaxCommitAttempts(_) | T::VersionAlreadyExists(_),
        }
        | DeltaTableError::VersionAlreadyExists(_)
        | DeltaTableError::VersionMismatch(..) => Ok(Step::Retry(e.to_string())),
        _ => Err(sink_error(e)),
    }
}

impl DeltaSink {
    pub fn new(config: DeltaSinkConfig) -> Self {
        static HANDLERS: Once = Once::new();
        HANDLERS.call_once(|| {
            deltalake::aws::register_handlers(None);
            deltalake::gcp::register_handlers(None);
            deltalake::azure::register_handlers(None);
        });
        let options = config.location.storage_options(std::env::vars());
        Self {
            capabilities: SinkCapabilities {
                write_modes: [WriteMode::Mirror].into(),
                schema_ops: [SchemaOp::AddColumn].into(),
                partition_transforms: [TransformKind::Identity].into(),
            },
            options,
            config,
            tables: Mutex::default(),
            stats: Mutex::default(),
        }
    }

    pub fn stats(&self) -> DeltaStats {
        *self.stats.lock().expect("no panic while holding the stats")
    }

    fn count(&self, f: impl FnOnce(&mut DeltaStats)) {
        f(&mut self.stats.lock().expect("no panic while holding the stats"));
    }

    /// The table's handle, opened on first use and held for the call.
    async fn lock(
        &self,
        table: &TableRef,
    ) -> Result<OwnedMutexGuard<Option<TableHandle>>, SinkError> {
        let slot = {
            let mut tables = self.tables.lock().expect("no panic while holding the map");
            Arc::clone(tables.entry(table.clone()).or_default())
        };
        let mut guard = slot.lock_owned().await;
        if guard.is_none() {
            let url = self.config.location.table_url(table);
            *guard = Some(TableHandle::open(url, self.options.clone()).await?);
        }
        Ok(guard)
    }

    /// The table's rows in Hop 2 terms and identifier order (every column, lineage included);
    /// `None` when the table doesn't exist. The Delta counterpart of `MemorySink::content`.
    pub async fn content(&self, table: &TableRef) -> Result<Option<RecordBatch>, SinkError> {
        let mut guard = self.lock(table).await?;
        let h = guard.as_mut().expect("opened by lock");
        if !h.refresh().await? {
            return Ok(None);
        }
        h.content().await.map(Some)
    }

    fn props(
        &self,
        epoch: u64,
        position: Option<&SourcePosition>,
    ) -> Result<CommitProperties, SinkError> {
        let version =
            i64::try_from(epoch).map_err(|_| permanent(format!("owner epoch {epoch}")))?;
        Ok(CommitProperties::default()
            .with_application_transaction(Transaction::new(
                app_id(&self.config.pipeline_id),
                version,
            ))
            .with_metadata(commit_metadata(&self.config.pipeline_id, epoch, position)))
    }

    /// One try at a commit, from the fence to the marker. `refresh` re-reads the table first;
    /// only a test leaves it stale, to lose a race on purpose.
    async fn attempt(
        &self,
        h: &mut TableHandle,
        cs: &TableChangeSet,
        marker: &CommitMarker,
        refresh: bool,
    ) -> Result<Step, SinkError> {
        if refresh && !h.refresh().await? {
            return Err(SinkError::NoTable {
                table: cs.table.clone(),
            });
        }
        let pipeline = &self.config.pipeline_id;
        let epoch = h.fence_epoch(pipeline).await?;
        if let Some(current) = epoch.filter(|e| *e > marker.owner_epoch) {
            return Ok(Step::Done(CommitOutcome::Fenced {
                current_epoch: current,
            }));
        }
        let stored = h.stored_position(pipeline).await?;
        if let Some(position) = &stored {
            let stored_marker = CommitMarker {
                owner_epoch: epoch.unwrap_or(marker.owner_epoch),
                committed_position: position.clone(),
                pipeline_id: pipeline.clone(),
            };
            if stored_marker.covers(cs)? {
                return Ok(Step::Done(CommitOutcome::SkippedCovered));
            }
        }
        let snapshot = h.table().snapshot().map_err(sink_error)?;
        let types = check_columns(cs, snapshot.schema().as_ref())?;
        let position = match &stored {
            Some(p) => p.union(&cs.position.end)?,
            None => cs.position.end.clone(),
        };
        let mut table = h.table().clone();
        if cs.truncate {
            // The clear carries the fence but no position: a crash before the MERGE replays both.
            table = match truncate(table, self.props(marker.owner_epoch, None)?).await {
                Ok(t) => t,
                Err(e) => return race_or_fail(e),
            };
            h.set_table(table.clone());
        }
        let before = table.version();
        if cs.upsert_rows() + cs.delete_rows() > 0 {
            let props = self.props(marker.owner_epoch, Some(&position))?;
            match merge(table, cs, &types, props, self.config.merge_memory_limit).await {
                Ok((t, metrics)) => {
                    self.count(|s| s.add(&metrics));
                    table = t;
                }
                Err(e) => return race_or_fail(e),
            }
            h.set_table(table.clone());
        }
        let version = if table.version() != before {
            table.version().expect("a written table is loaded")
        } else {
            // Nothing changed a row: a marker-only commit still moves the position.
            let props = self.props(marker.owner_epoch, Some(&position))?;
            let epoch = i64::try_from(marker.owner_epoch).unwrap_or(i64::MAX);
            match marker_only(&table, props, &app_id(pipeline), epoch).await {
                Ok(v) => v,
                Err(e) => return race_or_fail(e),
            }
        };
        h.remember(version, position);
        Ok(Step::Done(CommitOutcome::Committed { version }))
    }
}

#[async_trait]
impl Sink for DeltaSink {
    fn kind(&self) -> SinkKind {
        SinkKind::Delta
    }

    fn capabilities(&self) -> &SinkCapabilities {
        &self.capabilities
    }

    async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError> {
        let mut guard = self.lock(table).await?;
        let h = guard.as_mut().expect("opened by lock");
        if !h.refresh().await? {
            if let Some(reason) = h.foreign_format().await? {
                return Err(permanent(reason));
            }
            return Ok(SinkTableState {
                exists: false,
                schema: None,
                marker: None,
            });
        }
        let pipeline = &self.config.pipeline_id;
        let epoch = h.fence_epoch(pipeline).await?;
        let position = h.stored_position(pipeline).await?;
        Ok(SinkTableState {
            exists: true,
            schema: Some(h.schema()?),
            marker: epoch
                .zip(position)
                .map(|(owner_epoch, committed_position)| CommitMarker {
                    owner_epoch,
                    committed_position,
                    pipeline_id: pipeline.clone(),
                }),
        })
    }

    async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError> {
        let mut guard = self.lock(&spec.table).await?;
        let h = guard.as_mut().expect("opened by lock");
        if h.refresh().await? {
            return Ok(());
        }
        if let Some(reason) = h.foreign_format().await? {
            return Err(permanent(reason));
        }
        h.create(spec, &self.config.table).await
    }

    async fn evolve(
        &self,
        table: &TableRef,
        delta: &SchemaDelta,
    ) -> Result<EvolveOutcome, SinkError> {
        let mut guard = self.lock(table).await?;
        let h = guard.as_mut().expect("opened by lock");
        if !h.refresh().await? {
            return Err(SinkError::NoTable {
                table: table.clone(),
            });
        }
        let (t, outcome) =
            evolve(h.table().clone(), delta, self.config.table.timestamp_ntz_as).await?;
        h.set_table(t);
        Ok(outcome)
    }

    async fn commit(
        &self,
        cs: TableChangeSet,
        marker: CommitMarker,
    ) -> Result<CommitOutcome, SinkError> {
        cs.validate()?;
        if marker.committed_position != cs.position.end {
            return Err(permanent(format!(
                "the marker's position {} isn't the change set's end {}",
                marker.committed_position, cs.position.end
            )));
        }
        if marker.pipeline_id != self.config.pipeline_id {
            return Err(permanent(format!(
                "the marker is pipeline {:?}'s, this sink writes for {:?}",
                marker.pipeline_id, self.config.pipeline_id
            )));
        }
        let mut guard = self.lock(&cs.table).await?;
        let h = guard.as_mut().expect("opened by lock");
        let mut lost = String::new();
        for attempt in 0..COMMIT_ATTEMPTS {
            if attempt > 0 {
                self.count(|s| s.retries += 1);
            }
            match self.attempt(h, &cs, &marker, true).await? {
                Step::Done(outcome) => {
                    self.count(|s| match outcome {
                        CommitOutcome::Committed { .. } => s.commits += 1,
                        CommitOutcome::SkippedCovered => s.skipped += 1,
                        CommitOutcome::Fenced { .. } => s.fenced += 1,
                    });
                    return Ok(outcome);
                }
                Step::Retry(reason) => lost = reason,
            }
        }
        Err(SinkError::Transient {
            reason: format!("the commit lost {COMMIT_ATTEMPTS} races in a row, last: {lost}"),
        })
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-sink-delta --lib`

Expected: PASS. `data-avalanche-sink-delta` unit tests: 43.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/src/lib.rs \
  crates/data-avalanche-sink-delta/src/test_support.rs \
  crates/data-avalanche-sink-delta/src/sink.rs
git commit -m "feat(sink-delta): DeltaSink, the Sink trait on Delta tables"
```

---

### Task 10: The delta-capability probes through the `Sink` trait (local)

**Files:**
- Create: `crates/data-avalanche-sink-delta/tests/sink/main.rs`
- Create: `crates/data-avalanche-sink-delta/tests/sink/common.rs`
- Create: `crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`
- Create: `crates/data-avalanche-sink-delta/tests/sink/local.rs`

**Interfaces:**
- Consumes: the crate's public API (Tasks 2–9); `table_root` (storage).
- Produces (inside the test binary):
  - `common`: `items()`, `schema()`, `spec()`, `sink(DeltaLocation, TableDefaults)`,
    `upserts(&[Row], Option<&[bool]>)`, `deletes`, `cs(end, upserts, deletes)`, `commit(&dyn Sink,
    TableChangeSet, epoch)`, `rows(&DeltaSink)`, `owned`, `lsn`.
  - `scenarios`: `mirror_and_replay`, `fence_and_guard_survive_a_foreign_optimize`,
    `truncate_then_rows`, `evolution`, `masks_and_empty_sets`, each `async fn(DeltaLocation)`;
    `odd_names() -> TableRef`; `escaped_names(DeltaLocation, TableRef) -> TableRef`.

The roadmap's M4 exit, first half: probes P1–P5, P7a/P7b and P6, re-run against the crate on
a local directory. Task 12 runs the same scenarios on object stores. The scenario functions take a
`DeltaLocation`, so the stores reuse them.

- P1/P2 (`mirror_and_replay`): updates, deletes and inserts land in one version; a replay is
  guard-skipped; a fresh sink reads the marker back.
- P3/P4 (`fence_and_guard_survive_a_foreign_optimize`, the exit item): an external OPTIMIZE
  between commits disarms neither the fence nor the guard.
- P5 (`truncate_then_rows`), P7a (`evolution`), D-IMPL-3 masks and D-M4.9 (`masks_and_empty_sets`),
  Q-M4-5 (`escaped_names`).
- Local only: the P7b canary (a column-mapped table still refuses ADD COLUMN); checkpoints and log
  cleanup; the P6 cost counters.

- [ ] **Step 1: Write the scenarios and the local runs**

Create `crates/data-avalanche-sink-delta/tests/sink/main.rs`:

```rust
//! The Delta sink through the `Sink` trait: the delta-capability probes re-run against the crate
//! (roadmap M4 exit) on a local directory.

mod common;
mod local;
mod scenarios;
```

Create `crates/data-avalanche-sink-delta/tests/sink/common.rs`:

```rust
//! Fixtures: the table `public.items (id bigint key, doc text)` and change sets of it.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::Schema;
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    ColumnSchema, ColumnValueType, CommitMarker, CommitOutcome, PositionRange, Sink,
    SourcePosition, TableChangeSet, TableRef, TableSchema, TableSpec, delete_schema,
    unchanged_mask_field, upsert_schema,
};
use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig, TableDefaults};

pub fn items() -> TableRef {
    TableRef::new("public", "items")
}

pub fn schema() -> TableSchema {
    TableSchema::new(
        items(),
        vec![
            ColumnSchema::new("id", ColumnValueType::Int64, "bigint").not_null(),
            ColumnSchema::new("doc", ColumnValueType::Text, "text"),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

pub fn spec() -> TableSpec {
    TableSpec::new(&schema(), vec![]).unwrap()
}

pub fn sink(location: DeltaLocation, defaults: TableDefaults) -> DeltaSink {
    DeltaSink::new(DeltaSinkConfig {
        pipeline_id: "p".into(),
        location,
        table: defaults,
        merge_memory_limit: None,
    })
}

pub type Row<'a> = (i64, Option<&'a str>);

/// Upsert rows of `items` (lineage filled in); `masked` marks `doc` unchanged per row.
pub fn upserts(rows: &[Row], masked: Option<&[bool]>) -> RecordBatch {
    let full = upsert_schema(&schema());
    let n = rows.len();
    let text = |v: &str| Arc::new(StringArray::from(vec![v; n])) as ArrayRef;
    let mut fields = full.fields().to_vec();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))),
        text("U"),
        Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![0; n]).with_timezone("UTC")),
        text("0/1"),
        text("00000000/00000001"),
        text("h/db"),
        text("p"),
    ];
    if let Some(m) = masked {
        fields.push(Arc::new(unchanged_mask_field("doc")));
        columns.push(Arc::new(BooleanArray::from(m.to_vec())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

pub fn deletes(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        delete_schema(&schema(), &[]).unwrap(),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .unwrap()
}

/// A change set of `items` ending at LSN `end`.
pub fn cs(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeSet {
    TableChangeSet {
        table: items(),
        schema: upsert_schema(&schema()),
        identifier: vec!["id".into()],
        truncate: false,
        upserts: up,
        deletes: del,
        position: PositionRange {
            start: SourcePosition::Pg(Lsn(0)),
            end: SourcePosition::Pg(Lsn(end)),
        },
        split: None,
    }
}

/// Commits `c` at `epoch` with a marker at its end, as the pipeline does.
pub async fn commit(sink: &dyn Sink, c: TableChangeSet, epoch: u64) -> CommitOutcome {
    let marker = CommitMarker {
        owner_epoch: epoch,
        committed_position: c.position.end.clone(),
        pipeline_id: "p".into(),
    };
    sink.commit(c, marker).await.unwrap()
}

/// `(id, doc)` of every row, in key order.
pub async fn rows(sink: &DeltaSink) -> Vec<(i64, Option<String>)> {
    let b = sink.content(&items()).await.unwrap().unwrap();
    let ids = b.column(0).as_primitive::<Int64Type>();
    let docs = b.column(1).as_string::<i32>();
    (0..b.num_rows())
        .map(|i| {
            (
                ids.value(i),
                docs.is_valid(i).then(|| docs.value(i).to_string()),
            )
        })
        .collect()
}

pub fn owned(rows: &[Row]) -> Vec<(i64, Option<String>)> {
    rows.iter()
        .map(|(i, d)| (*i, d.map(String::from)))
        .collect()
}

pub fn lsn(n: u64) -> SourcePosition {
    SourcePosition::Pg(Lsn(n))
}
```

Create `crates/data-avalanche-sink-delta/tests/sink/scenarios.rs`:

```rust
//! The delta-capability probes (P1–P5, P7a) as scenarios of the sink, run on any location.

use data_avalanche_core::{
    ColumnSchema, ColumnValueType, CommitOutcome, EvolveOutcome, SchemaChange, SchemaDelta, Sink,
    TableRef, TableSchema, TableSpec,
};
use data_avalanche_sink_delta::{DeltaLocation, TableDefaults};
use deltalake::DeltaTable;

use crate::common::{commit, cs, deletes, items, lsn, owned, rows, schema, sink, spec, upserts};

/// A handle on the sink's table as another writer (an external engine) would open it.
async fn foreign(location: &DeltaLocation) -> DeltaTable {
    let url = location.table_url(&items());
    let mut t = DeltaTable::try_from_url_with_storage_options(
        url,
        location.storage_options(std::env::vars()),
    )
    .await
    .unwrap();
    t.load().await.unwrap();
    t
}

/// P1 and P2: updates, deletes and inserts land in one version; a replay is skipped by the guard,
/// and the same sets on a fresh table give the same content.
pub async fn mirror_and_replay(location: DeltaLocation) {
    let s = sink(location.clone(), TableDefaults::default());
    s.ensure_table(&spec()).await.unwrap();
    let first = cs(
        10,
        vec![upserts(
            &[(1, Some("a")), (2, Some("b")), (3, Some("c"))],
            None,
        )],
        vec![],
    );
    let second = cs(
        20,
        vec![upserts(&[(1, Some("A")), (4, Some("d"))], None)],
        vec![deletes(&[2])],
    );
    let CommitOutcome::Committed { version: v1 } = commit(&s, first.clone(), 1).await else {
        panic!("first commit")
    };
    assert_eq!(
        commit(&s, second.clone(), 1).await,
        CommitOutcome::Committed { version: v1 + 1 }
    );
    let want = owned(&[(1, Some("A")), (3, Some("c")), (4, Some("d"))]);
    assert_eq!(rows(&s).await, want);
    assert_eq!(commit(&s, first, 1).await, CommitOutcome::SkippedCovered);
    assert_eq!(rows(&s).await, want);
    let stats = s.stats();
    assert_eq!((stats.commits, stats.skipped), (2, 1));
    assert_eq!(
        (stats.rows_inserted, stats.rows_updated, stats.rows_deleted),
        (4, 1, 1)
    );
    // A new process (a fresh sink) reads the marker back.
    let state = sink(location, TableDefaults::default())
        .table_state(&items())
        .await
        .unwrap();
    let marker = state.marker.unwrap();
    assert_eq!(
        (marker.owner_epoch, marker.committed_position),
        (1, lsn(20))
    );
    assert_eq!(state.schema.unwrap(), spec().schema);
}

/// P3 and P4 (the M4 exit): a foreign OPTIMIZE doesn't disarm the fence or the guard.
pub async fn fence_and_guard_survive_a_foreign_optimize(location: DeltaLocation) {
    let owner = sink(location.clone(), TableDefaults::default());
    owner.ensure_table(&spec()).await.unwrap();
    for (end, id) in [(10, 1), (20, 2), (30, 3)] {
        commit(
            &owner,
            cs(end, vec![upserts(&[(id, Some("v"))], None)], vec![]),
            7,
        )
        .await;
    }
    let (optimized, metrics) = foreign(&location).await.optimize().await.unwrap();
    assert!(metrics.num_files_removed > 1, "{metrics:?}");
    drop(optimized);
    let fresh = sink(location, TableDefaults::default());
    let marker = fresh.table_state(&items()).await.unwrap().marker.unwrap();
    assert_eq!(
        (marker.owner_epoch, marker.committed_position),
        (7, lsn(30))
    );
    // A zombie of epoch 6 is fenced; a replay of an old range is skipped.
    let zombie = cs(40, vec![upserts(&[(9, Some("zombie"))], None)], vec![]);
    assert_eq!(
        commit(&fresh, zombie, 6).await,
        CommitOutcome::Fenced { current_epoch: 7 }
    );
    let replay = cs(20, vec![upserts(&[(2, Some("old"))], None)], vec![]);
    assert_eq!(
        commit(&fresh, replay, 7).await,
        CommitOutcome::SkippedCovered
    );
    assert_eq!(
        rows(&fresh).await,
        owned(&[(1, Some("v")), (2, Some("v")), (3, Some("v"))])
    );
}

/// P5: a truncate clears the table before the set's rows; a replay of it after later commits is
/// skipped.
pub async fn truncate_then_rows(location: DeltaLocation) {
    let s = sink(location, TableDefaults::default());
    s.ensure_table(&spec()).await.unwrap();
    commit(
        &s,
        cs(
            10,
            vec![upserts(&[(1, Some("a")), (2, Some("b"))], None)],
            vec![],
        ),
        1,
    )
    .await;
    let mut t = cs(
        20,
        vec![upserts(&[(3, Some("c"))], None)],
        vec![deletes(&[1])],
    );
    t.truncate = true;
    commit(&s, t.clone(), 1).await;
    assert_eq!(rows(&s).await, owned(&[(3, Some("c"))]));
    let mut clear = cs(30, vec![], vec![]);
    clear.truncate = true;
    commit(&s, clear, 1).await;
    assert_eq!(rows(&s).await, vec![]);
    commit(
        &s,
        cs(40, vec![upserts(&[(4, Some("d"))], None)], vec![]),
        1,
    )
    .await;
    assert_eq!(commit(&s, t, 1).await, CommitOutcome::SkippedCovered);
    assert_eq!(rows(&s).await, owned(&[(4, Some("d"))]));
}

/// P7a and the Delta evolution contract: ADD COLUMN applies and the pre-evolution set shape still
/// commits (the new column is NULL); a drop is a soft-drop, written NULL from then on.
pub async fn evolution(location: DeltaLocation) {
    let s = sink(location, TableDefaults::default());
    s.ensure_table(&spec()).await.unwrap();
    commit(
        &s,
        cs(10, vec![upserts(&[(1, Some("a"))], None)], vec![]),
        1,
    )
    .await;
    let add = SchemaDelta {
        table: items(),
        changes: vec![SchemaChange::AddColumn(ColumnSchema::new(
            "n",
            ColumnValueType::Int32,
            "integer",
        ))],
    };
    assert_eq!(
        s.evolve(&items(), &add).await.unwrap(),
        EvolveOutcome::Applied
    );
    commit(
        &s,
        cs(20, vec![upserts(&[(2, Some("b"))], None)], vec![]),
        1,
    )
    .await;
    let content = s.content(&items()).await.unwrap().unwrap();
    assert_eq!(
        content.schema().field(content.num_columns() - 1).name(),
        "n"
    );
    assert_eq!(content.column_by_name("n").unwrap().null_count(), 2);
    let drop = SchemaDelta {
        table: items(),
        changes: vec![SchemaChange::DropColumn("doc".into())],
    };
    assert!(matches!(
        s.evolve(&items(), &drop).await.unwrap(),
        EvolveOutcome::Degraded { .. }
    ));
    let mut narrow = cs(30, vec![], vec![]);
    let fields: Vec<_> = narrow
        .schema
        .fields()
        .iter()
        .filter(|f| f.name() != "doc")
        .cloned()
        .collect();
    narrow.schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
    let batch = upserts(&[(1, None)], None);
    narrow.upserts = vec![batch.project(&[0, 2, 3, 4, 5, 6, 7]).unwrap()];
    commit(&s, narrow, 1).await;
    assert_eq!(rows(&s).await, owned(&[(1, None), (2, Some("b"))]));
}

/// Unchanged TOAST cells keep the stored value; a set that changes no row still moves the marker.
pub async fn masks_and_empty_sets(location: DeltaLocation) {
    let s = sink(location, TableDefaults::default());
    s.ensure_table(&spec()).await.unwrap();
    commit(
        &s,
        cs(10, vec![upserts(&[(1, Some("big"))], None)], vec![]),
        1,
    )
    .await;
    let masked = upserts(&[(1, None), (2, None)], Some(&[true, true]));
    commit(&s, cs(20, vec![masked], vec![]), 1).await;
    assert_eq!(rows(&s).await, owned(&[(1, Some("big")), (2, None)]));
    for (end, set) in [
        (30, cs(30, vec![], vec![])),
        (40, cs(40, vec![], vec![deletes(&[99])])),
    ] {
        assert!(matches!(
            commit(&s, set, 1).await,
            CommitOutcome::Committed { .. }
        ));
        let marker = s.table_state(&items()).await.unwrap().marker.unwrap();
        assert_eq!(marker.committed_position, lsn(end));
    }
}

/// The table name the escaped-name scenario uses by default: `%` and `/` are escaped (`~XX`,
/// Q-M4-5), a space and non-ASCII text are kept.
pub fn odd_names() -> TableRef {
    TableRef::new("sales 50%", "a/b.ü")
}

/// A table whose names need escaping: it is written and read back, and a fresh sink finds it.
pub async fn escaped_names(location: DeltaLocation, odd: TableRef) -> TableRef {
    let odd_schema =
        TableSchema::new(odd.clone(), schema().columns().to_vec(), vec!["id".into()]).unwrap();
    let s = sink(location.clone(), TableDefaults::default());
    s.ensure_table(&TableSpec::new(&odd_schema, vec![]).unwrap())
        .await
        .unwrap();
    let mut c = cs(10, vec![upserts(&[(1, Some("a"))], None)], vec![]);
    c.table = odd.clone();
    assert!(matches!(
        commit(&s, c, 1).await,
        CommitOutcome::Committed { .. }
    ));
    assert_eq!(s.content(&odd).await.unwrap().unwrap().num_rows(), 1);
    let state = sink(location, TableDefaults::default())
        .table_state(&odd)
        .await
        .unwrap();
    assert!(state.exists);
    assert_eq!(state.marker.unwrap().committed_position, lsn(10));
    odd
}
```

Create `crates/data-avalanche-sink-delta/tests/sink/local.rs`:

```rust
//! The scenarios on a local directory, and what only runs locally: a column-mapped table,
//! checkpoints and log cleanup, the MERGE cost model.

use std::time::Duration;

use data_avalanche_core::{ColumnSchema, ColumnValueType, SchemaChange, SchemaDelta, Sink};
use data_avalanche_sink_delta::{DeltaLocation, TableDefaults};
use deltalake::kernel::{DataType, StructField};
use deltalake::{DeltaTable, TableProperty};

use crate::common::{commit, cs, items, lsn, sink, spec, upserts};
use crate::scenarios;

fn local() -> (tempfile::TempDir, DeltaLocation) {
    let dir = tempfile::tempdir().unwrap();
    let location = DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    };
    (dir, location)
}

#[tokio::test]
async fn mirror_and_replay() {
    let (_dir, location) = local();
    scenarios::mirror_and_replay(location).await;
}

#[tokio::test]
async fn fence_and_guard_survive_a_foreign_optimize() {
    let (_dir, location) = local();
    scenarios::fence_and_guard_survive_a_foreign_optimize(location).await;
}

#[tokio::test]
async fn truncate_then_rows() {
    let (_dir, location) = local();
    scenarios::truncate_then_rows(location).await;
}

#[tokio::test]
async fn evolution() {
    let (_dir, location) = local();
    scenarios::evolution(location).await;
}

#[tokio::test]
async fn masks_and_empty_sets() {
    let (_dir, location) = local();
    scenarios::masks_and_empty_sets(location).await;
}

#[tokio::test]
async fn escaped_names_land_under_the_storage_crates_keys() {
    let (dir, location) = local();
    let odd = scenarios::escaped_names(location, scenarios::odd_names()).await;
    let root = data_avalanche_storage::table_root("", "", &odd.schema, &odd.name).root;
    assert!(dir.path().join(&root).join("_delta_log").is_dir(), "{root}");
}

/// P7b, as a canary: delta-rs 1.0 refuses ADD COLUMN on a column-mapped table, which is why the
/// sink never creates one (V-092). A delta-rs that lifts the limit fails this test.
#[tokio::test]
async fn delta_rs_still_refuses_add_column_under_column_mapping() {
    let (_dir, location) = local();
    let url = location.table_url(&items());
    std::fs::create_dir_all(url.to_file_path().unwrap()).unwrap();
    DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns([StructField::new("id", DataType::LONG, false)])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await
        .unwrap();
    let s = sink(location, TableDefaults::default());
    let add = SchemaDelta {
        table: items(),
        changes: vec![SchemaChange::AddColumn(ColumnSchema::new(
            "n",
            ColumnValueType::Int32,
            "integer",
        ))],
    };
    let err = s.evolve(&items(), &add).await.unwrap_err();
    assert!(err.to_string().contains("Column mapping"), "{err}");
}

/// Checkpoints and expired-log cleanup run as post-commit hooks; the marker survives both (the
/// `txn` lives in the checkpoint, the walk-back stops at the first missing commit).
#[tokio::test]
async fn checkpoints_and_log_cleanup_keep_the_marker() {
    let (dir, location) = local();
    let defaults = TableDefaults {
        checkpoint_interval: 2,
        log_retention: Duration::ZERO,
        ..TableDefaults::default()
    };
    let s = sink(location.clone(), defaults.clone());
    s.ensure_table(&spec()).await.unwrap();
    for end in 1..=6 {
        commit(
            &s,
            cs(
                end * 10,
                vec![upserts(&[(end as i64, Some("v"))], None)],
                vec![],
            ),
            3,
        )
        .await;
    }
    let log: Vec<String> = std::fs::read_dir(dir.path().join("public/items/_delta_log"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        log.iter().any(|f| f.ends_with(".checkpoint.parquet")),
        "{log:?}"
    );
    assert!(
        !log.contains(&"00000000000000000001.json".to_string()),
        "expired commits are cleaned up: {log:?}"
    );
    let fresh = sink(location, defaults);
    let marker = fresh.table_state(&items()).await.unwrap().marker.unwrap();
    assert_eq!(
        (marker.owner_epoch, marker.committed_position),
        (3, lsn(60))
    );
}

/// Probe P6, the Delta cost model (D-MS.15): changes spread over the key space rewrite every file
/// they touch, changes to a key range rewrite only its files. The counters show it.
#[tokio::test]
async fn random_keys_rewrite_more_files_than_a_key_range() {
    const FILES: i64 = 8;
    const PER_FILE: i64 = 5_000;
    const CHANGES: i64 = 400;
    async fn build(location: DeltaLocation) -> data_avalanche_sink_delta::DeltaSink {
        let s = sink(location, TableDefaults::default());
        s.ensure_table(&spec()).await.unwrap();
        for f in 0..FILES {
            let rows: Vec<_> = (f * PER_FILE..(f + 1) * PER_FILE)
                .map(|k| (k, Some("base")))
                .collect();
            commit(&s, cs(f as u64 + 1, vec![upserts(&rows, None)], vec![]), 1).await;
        }
        s
    }
    /// (files rewritten, rows copied) by one MERGE of `keys` over the built table.
    async fn cost(keys: Vec<i64>) -> (u64, u64) {
        let (_dir, location) = local();
        let s = build(location).await;
        let before = s.stats();
        let rows: Vec<_> = keys.into_iter().map(|k| (k, Some("upd"))).collect();
        commit(&s, cs(100, vec![upserts(&rows, None)], vec![]), 1).await;
        let after = s.stats();
        (
            after.files_removed - before.files_removed,
            after.rows_copied - before.rows_copied,
        )
    }
    let total = FILES * PER_FILE;
    let (seq_files, seq_copied) = cost((total - CHANGES..total).collect()).await;
    let (rnd_files, rnd_copied) = cost((0..CHANGES).map(|i| i * (total / CHANGES)).collect()).await;
    // A key range rewrites its one file, copying the rows it didn't change; keys spread over the
    // table rewrite every file (probe P6: 40/40 files for 5k random updates of 1M rows).
    assert_eq!((seq_files, seq_copied), (1, (PER_FILE - CHANGES) as u64));
    assert_eq!(
        (rnd_files, rnd_copied),
        (FILES as u64, (total - CHANGES) as u64)
    );
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-sink-delta --test sink`

Expected: PASS, 9 tests. These tests drive the code of Tasks 2–9, so nothing fails first. A failure here is a defect in that code: stop and report it.

- [ ] **Step 3: Run them against the emulators**

Run: `test-infra/up.sh`, then `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-sink-delta --test sink -- --test-threads=2`

Expected: PASS, 9 tests (the same tests: none of them uses the emulators yet).

- [ ] **Step 4: Run the gates**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/tests/sink/main.rs \
  crates/data-avalanche-sink-delta/tests/sink/common.rs \
  crates/data-avalanche-sink-delta/tests/sink/scenarios.rs \
  crates/data-avalanche-sink-delta/tests/sink/local.rs
git commit -m "test(sink-delta): the delta-capability probes through the Sink trait"
```

---

### Task 11: Conformance with the reference sink

**Files:**
- Modify: `crates/data-avalanche-sink-delta/tests/sink/main.rs`
- Create: `crates/data-avalanche-sink-delta/tests/sink/conformance.rs`

**Interfaces:**
- Consumes: `common` (Task 10); `Accumulator`, `Flush`, `FlushPolicy`, `TxnLimits`, `Lineage`
  (pipeline); `MemorySink` (testkit).
- Produces: nothing for later tasks.

The roadmap's M4 exit, second half: the same change-set sequence gives the same table content.
Random transactions (puts, unchanged-TOAST updates, deletes, key moves with and without TOAST,
truncates) run through the M3 accumulator under random flush and split schedules. The test
follows the orchestrator's protocol (`FlushRequired` → flush → retry, `SplitRequired` → split).
Every change set is committed to both sinks, and the contents are compared at the end.

- [ ] **Step 1: Write the conformance property**

Replace `crates/data-avalanche-sink-delta/tests/sink/main.rs` with:

```rust
//! The Delta sink through the `Sink` trait: the delta-capability probes re-run against the crate
//! (roadmap M4 exit) on a local directory, and conformance with the reference sink.

mod common;
mod conformance;
mod local;
mod scenarios;
```

Create `crates/data-avalanche-sink-delta/tests/sink/conformance.rs`:

```rust
//! M4 exit: the Delta sink and the reference sink (`MemorySink`) give the same table content for
//! the same change sets. Random transactions (puts, unchanged-TOAST updates, deletes, key moves,
//! truncates) run through the accumulator under random flush and split schedules; every change set
//! is committed to both sinks.

use std::collections::BTreeMap;
use std::time::Instant;

use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ColumnValue, CommitMarker, CommitOutcome, Op, Row, Sink, SourcePosition, SourceSeq,
    TransactionBoundary,
};
use data_avalanche_pipeline::{
    Accumulator, AccumulatorError, Flush, FlushPolicy, FlushReason, Lineage, TxnLimits,
};
use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, TableDefaults};
use data_avalanche_testkit::MemorySink;
use proptest::prelude::*;

use crate::common::{items, schema, sink, spec};

#[derive(Debug, Clone)]
enum Action {
    Put {
        key: i64,
        doc: u8,
    },
    /// An UPDATE that doesn't touch `doc` (unchanged TOAST).
    Touch {
        key: i64,
    },
    Delete {
        key: i64,
    },
    /// An UPDATE of the key; the after-image's `doc` is unchanged TOAST when `toast`.
    Move {
        from: i64,
        to: i64,
        toast: bool,
    },
    Truncate,
}

fn action() -> impl Strategy<Value = Action> {
    let key = 0..6i64;
    prop_oneof![
        4 => (key.clone(), any::<u8>()).prop_map(|(key, doc)| Action::Put { key, doc }),
        2 => key.clone().prop_map(|key| Action::Touch { key }),
        2 => key.clone().prop_map(|key| Action::Delete { key }),
        1 => (key.clone(), key, any::<bool>()).prop_map(|(from, to, toast)| Action::Move { from, to, toast }),
        1 => Just(Action::Truncate),
    ]
}

fn row(key: i64, doc: ColumnValue) -> Row {
    Row(vec![
        ("id".into(), ColumnValue::Int64(key)),
        ("doc".into(), doc),
    ])
}

struct Both {
    memory: MemorySink,
    delta: DeltaSink,
}

impl Both {
    async fn commit(&self, flush: Flush) {
        for cs in flush.change_sets {
            let marker = CommitMarker {
                owner_epoch: 1,
                committed_position: cs.position.end.clone(),
                pipeline_id: "p".into(),
            };
            let m = self
                .memory
                .commit(cs.clone(), marker.clone())
                .await
                .unwrap();
            let d = self.delta.commit(cs, marker).await.unwrap();
            let skipped = |o: &CommitOutcome| matches!(o, CommitOutcome::SkippedCovered);
            assert_eq!(skipped(&m), skipped(&d), "{m:?} vs {d:?}");
        }
    }
}

async fn run(txns: Vec<Vec<Action>>, flush_after: Vec<bool>, split_rows: Option<u64>) {
    let dir = tempfile::tempdir().unwrap();
    let location = DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    };
    let both = Both {
        memory: MemorySink::new(),
        delta: sink(location, TableDefaults::default()),
    };
    both.memory.ensure_table(&spec()).await.unwrap();
    both.delta.ensure_table(&spec()).await.unwrap();
    let never = FlushPolicy {
        seconds: None,
        rows: None,
        bytes: None,
    };
    let lineage = Lineage {
        source_db: "db".into(),
        pipeline: "p".into(),
    };
    let mut acc = Accumulator::new(lineage, never, SourcePosition::Pg(Lsn(0)));
    acc.set_txn_limits(TxnLimits {
        batch_rows: split_rows,
        buffer_bytes: 1 << 30,
    });
    acc.add_table(schema(), &[]).unwrap();
    let now = Instant::now();
    let mut db: BTreeMap<i64, String> = BTreeMap::new();
    for (i, actions) in txns.iter().enumerate() {
        let xid = i as u64 + 1;
        let pos = SourcePosition::Pg(Lsn(100 * xid));
        let mut seq = 100 * i as u64;
        acc.begin(xid).unwrap();
        for a in actions {
            let mut event = |op, before, after| {
                seq += 1;
                CdcEvent {
                    op,
                    table: items(),
                    before,
                    after,
                    source_pos: pos.clone(),
                    change_seq: SourceSeq::Pg(Lsn(seq)),
                    xid,
                    commit_ts_micros: i as i64,
                }
            };
            let e = match *a {
                Action::Put { key, doc } => {
                    let doc = format!("d{doc}");
                    let op = if db.insert(key, doc.clone()).is_some() {
                        Op::Update
                    } else {
                        Op::Insert
                    };
                    event(op, None, Some(row(key, ColumnValue::Text(doc))))
                }
                Action::Touch { key } if db.contains_key(&key) => {
                    event(Op::Update, None, Some(row(key, ColumnValue::Unchanged)))
                }
                Action::Delete { key } if db.remove(&key).is_some() => event(
                    Op::Delete,
                    Some(Row(vec![("id".into(), ColumnValue::Int64(key))])),
                    None,
                ),
                Action::Move { from, to, toast }
                    if from != to && !db.contains_key(&to) && db.contains_key(&from) =>
                {
                    let doc = db.remove(&from).unwrap();
                    db.insert(to, doc.clone());
                    let (before, after) = if toast {
                        (
                            row(from, ColumnValue::Text(doc)),
                            row(to, ColumnValue::Unchanged),
                        )
                    } else {
                        (
                            Row(vec![("id".into(), ColumnValue::Int64(from))]),
                            row(to, ColumnValue::Text(doc)),
                        )
                    };
                    event(Op::Update, Some(before), Some(after))
                }
                Action::Truncate => {
                    db.clear();
                    loop {
                        match acc.truncate(&[items()], now) {
                            Ok(()) => break,
                            Err(AccumulatorError::FlushRequired { .. }) => {
                                both.commit(acc.flush()).await
                            }
                            Err(e) => panic!("{e}"),
                        }
                    }
                    continue;
                }
                _ => continue,
            };
            loop {
                match acc.apply(&e, now) {
                    Ok(()) => break,
                    Err(AccumulatorError::SplitRequired) => both.commit(acc.split().unwrap()).await,
                    Err(e) => panic!("{e}"),
                }
            }
            if acc.due(now) == Some(FlushReason::Split) {
                both.commit(acc.split().unwrap()).await;
            }
        }
        acc.commit(
            &TransactionBoundary {
                xid,
                commit_pos: pos,
                commit_ts_micros: i as i64,
            },
            now,
        )
        .unwrap();
        if flush_after[i] {
            both.commit(acc.flush()).await;
        }
    }
    both.commit(acc.flush()).await;
    let memory = both.memory.content(&items()).unwrap();
    let delta = both.delta.content(&items()).await.unwrap().unwrap();
    assert_eq!(delta, memory);
    assert_eq!(delta.num_rows(), db.len());
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    fn the_delta_table_equals_the_reference_sinks(
        txns in prop::collection::vec(prop::collection::vec(action(), 0..6), 1..8),
        flush_after in prop::collection::vec(any::<bool>(), 8),
        split_rows in prop::option::of(1..4u64),
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        rt.block_on(run(txns, flush_after, split_rows));
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-sink-delta --test sink`

Expected: PASS, 10 tests. These tests drive the code of Tasks 2–9, so nothing fails first. A failure here is a defect in that code: stop and report it.

- [ ] **Step 3: Run them against the emulators**

Run: `test-infra/up.sh`, then `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-sink-delta --test sink -- --test-threads=2`

Expected: PASS, 10 tests (the same tests: none of them uses the emulators yet).

- [ ] **Step 4: Run the gates**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/tests/sink/main.rs \
  crates/data-avalanche-sink-delta/tests/sink/conformance.rs
git commit -m "test(sink-delta): conformance with the reference sink"
```

---

### Task 12: The scenarios on RustFS, Azurite and real GCS

**Files:**
- Modify: `crates/data-avalanche-sink-delta/tests/sink/main.rs`
- Create: `crates/data-avalanche-sink-delta/tests/sink/stores.rs`

**Interfaces:**
- Consumes: `scenarios` (Task 10); `require_it!`, `require_gcs!`, `unique_prefix`, `delete_prefix`,
  `stores::{rustfs, azurite, gcs}`, `install_crypto_provider`, the RustFS constants (testkit).
- Produces: nothing for later tasks.

Q-M4-4. Each store runs every scenario under its own unique prefix, deleted afterwards. RustFS
takes S3 options (endpoint, `allow_http`, the emulator's keys); Azurite takes `use_emulator`; GCS
takes nothing but the bucket, with credentials from the environment. Without `DATA_AVALANCHE_IT`
the tests return at once, which keeps plain `cargo test` hermetic.

- [ ] **Step 1: Write the store runs**

Replace `crates/data-avalanche-sink-delta/tests/sink/main.rs` with:

```rust
//! The Delta sink through the `Sink` trait: the delta-capability probes re-run against the crate
//! (roadmap M4 exit) on a local directory and on object stores, and conformance with the
//! reference sink.

mod common;
mod conformance;
mod local;
mod scenarios;
mod stores;
```

Create `crates/data-avalanche-sink-delta/tests/sink/stores.rs`:

```rust
//! The scenarios on object storage (roadmap M4 exit, Q-M4-4): RustFS and Azurite from
//! `test-infra/compose.yaml`, and the real GCS bucket, each under a unique prefix that is deleted
//! afterwards. Gated like every integration test (`DATA_AVALANCHE_IT`, GCS also by
//! `DATA_AVALANCHE_SKIP_GCS`).

use data_avalanche_sink_delta::DeltaLocation;
use data_avalanche_storage::Backend;
use data_avalanche_testkit::endpoints::{
    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
};
use data_avalanche_testkit::{
    delete_prefix, install_crypto_provider, require_gcs, require_it, stores, unique_prefix,
};

use crate::scenarios;

fn options(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Every scenario, each under its own prefix below `prefix`.
async fn run_all(location: impl Fn(String) -> DeltaLocation, prefix: &str) {
    let at = |name: &str| location(format!("{prefix}/{name}"));
    scenarios::mirror_and_replay(at("mirror")).await;
    scenarios::fence_and_guard_survive_a_foreign_optimize(at("fence")).await;
    scenarios::truncate_then_rows(at("truncate")).await;
    scenarios::evolution(at("evolution")).await;
    scenarios::masks_and_empty_sets(at("masks")).await;
    scenarios::escaped_names(at("escaped"), scenarios::odd_names()).await;
}

#[tokio::test]
async fn scenarios_on_rustfs() {
    require_it!();
    install_crypto_provider();
    let prefix = unique_prefix("sink-delta");
    let location = |prefix: String| DeltaLocation::Bucket {
        backend: Backend::S3,
        bucket: TEST_BUCKET.into(),
        prefix,
        options: options(&[
            ("endpoint", RUSTFS_URL),
            ("allow_http", "true"),
            ("region", "us-east-1"),
            ("access_key_id", RUSTFS_ACCESS_KEY),
            ("secret_access_key", RUSTFS_SECRET_KEY),
        ]),
    };
    run_all(location, &prefix).await;
    delete_prefix(stores::rustfs().as_ref(), &prefix).await;
}

#[tokio::test]
async fn scenarios_on_azurite() {
    require_it!();
    install_crypto_provider();
    let prefix = unique_prefix("sink-delta");
    let location = |prefix: String| DeltaLocation::Bucket {
        backend: Backend::Azure { account: None },
        bucket: TEST_BUCKET.into(),
        prefix,
        options: options(&[("use_emulator", "true")]),
    };
    run_all(location, &prefix).await;
    delete_prefix(stores::azurite().as_ref(), &prefix).await;
}

#[tokio::test]
async fn scenarios_on_real_gcs() {
    require_gcs!();
    install_crypto_provider();
    let bucket = data_avalanche_testkit::required_env("DATA_AVALANCHE_GCS_BUCKET");
    let prefix = unique_prefix("sink-delta");
    let location = |prefix: String| DeltaLocation::Bucket {
        backend: Backend::Gcs,
        bucket: bucket.clone(),
        prefix,
        options: vec![],
    };
    run_all(location, &prefix).await;
    delete_prefix(stores::gcs().as_ref(), &prefix).await;
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-sink-delta --test sink`

Expected: PASS, 13 tests. The object-store tests return early without `DATA_AVALANCHE_IT` and count as passed. These tests drive the code of Tasks 2–9, so nothing fails first. A failure here is a defect in that code: stop and report it.

- [ ] **Step 3: Run them against the emulators**

Run: `test-infra/up.sh`, then `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-sink-delta --test sink -- --test-threads=2`

Expected: PASS, 13 tests (RustFS and Azurite included; GCS skipped).

The real-GCS run needs Oleksii's credentials. With `DATA_AVALANCHE_GCS_BUCKET` and the
service-account variable set in his shell, Oleksii runs:

`! DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-sink-delta --test sink scenarios_on_real_gcs`

Expected: PASS. CI runs it on every pull request (the integration job has the bucket and the key).

- [ ] **Step 4: Run the gates**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/tests/sink/main.rs \
  crates/data-avalanche-sink-delta/tests/sink/stores.rs
git commit -m "test(sink-delta): the scenarios on RustFS, Azurite and real GCS"
```

---

### Task 13: P10: the Hop 3b round trip, and the DuckDB cross-read

**Files:**
- Modify: `crates/data-avalanche-sink-delta/tests/sink/main.rs`
- Create: `crates/data-avalanche-sink-delta/tests/sink/types.rs`
- Create: `crates/data-avalanche-sink-delta/tests/duckdb_read.py`
- Modify: `.github/workflows/ci.yml`
- Modify: `test-infra/README.md`

**Interfaces:**
- Consumes: `scenarios::escaped_names` (Task 10); the `convert` range constants (Task 4);
  `Accumulator` (pipeline); `MemorySink` (testkit).
- Produces: nothing for later tasks.

Probe P10 against the crate. A table of every Hop 2 type (scalars, decimals, text, JSON, bytes,
date, time, both timestamps, uuid, lists) gets three rows: ordinary values, all NULL, and the
extremes. It is inserted, then re-imaged, through change sets the accumulator builds. The result
must equal `MemorySink`'s, with date and timestamp cells clamped to the Delta range (Q-M4-2),
under each `timestamp_ntz_as`.

With `DATA_AVALANCHE_IT`, DuckDB reads the table and an escaped-name table. The extreme row
reads as `0001-01-01`, and uuid, time and `u64` read as text (R-12). The escaped name is ASCII
(Q-M4-6). The script runs under `uv`; the CI integration job installs it.

- [ ] **Step 1: Write the round trip, the DuckDB script and the CI step**

Replace `crates/data-avalanche-sink-delta/tests/sink/main.rs` with:

```rust
//! The Delta sink through the `Sink` trait: the delta-capability probes re-run against the crate
//! (roadmap M4 exit) on a local directory and on object stores, conformance with the reference
//! sink, and the Hop 3b type round trip.

mod common;
mod conformance;
mod local;
mod scenarios;
mod stores;
mod types;
```

Create `crates/data-avalanche-sink-delta/tests/sink/types.rs`:

```rust
//! Probe P10 against the crate: every Hop 2 type round-trips through the sink's MERGE (insert, then
//! a full re-image) and reads back as the reference sink holds it, for every `timestamp_ntz_as`.
//! Dates and timestamps outside the Delta range read back clamped (Q-M4-2). With
//! `DATA_AVALANCHE_IT`, DuckDB's `delta_scan` reads the table too (R-12).

use std::time::Instant;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, TimestampMicrosecondType};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::DataType;
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ColumnSchema, ColumnValue as C, ColumnValueType as V, CommitMarker, Decimal, Op, Row,
    Sink, SourcePosition, SourceSeq, TableChangeSet, TableRef, TableSchema, TableSpec,
    TransactionBoundary,
};
use data_avalanche_pipeline::{Accumulator, FlushPolicy, Lineage};
use data_avalanche_sink_delta::convert::{
    DELTA_MAX_DATE, DELTA_MAX_MICROS, DELTA_MIN_DATE, DELTA_MIN_MICROS,
};
use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig, NtzAs, TableDefaults};
use data_avalanche_testkit::{MemorySink, require_it};

fn table() -> TableRef {
    TableRef::new("public", "types")
}

fn schema() -> TableSchema {
    let cols = vec![
        ColumnSchema::new("id", V::Int64, "bigint").not_null(),
        ColumnSchema::new("flag", V::Bool, "boolean"),
        ColumnSchema::new("small", V::Int16, "smallint"),
        ColumnSchema::new("int", V::Int32, "integer"),
        ColumnSchema::new("u64", V::UInt64, "bigint unsigned"),
        ColumnSchema::new("f64", V::Float64, "double precision"),
        ColumnSchema::new(
            "dec",
            V::Numeric {
                precision: 38,
                scale: 10,
            },
            "numeric(38,10)",
        ),
        ColumnSchema::new("s", V::Text, "text"),
        ColumnSchema::new("js", V::Json, "jsonb"),
        ColumnSchema::new("bin", V::Bytes, "bytea"),
        ColumnSchema::new("d", V::Date, "date"),
        ColumnSchema::new("t", V::Time, "time"),
        ColumnSchema::new("tz", V::Timestamp { with_tz: true }, "timestamptz"),
        ColumnSchema::new("ntz", V::Timestamp { with_tz: false }, "timestamp"),
        ColumnSchema::new("u", V::Uuid, "uuid"),
        ColumnSchema::new("tags", V::Array(Box::new(V::Text)), "text[]"),
        ColumnSchema::new("ids", V::Array(Box::new(V::Uuid)), "uuid[]"),
    ];
    TableSchema::new(table(), cols, vec!["id".into()]).unwrap()
}

fn uuid(b: u8) -> C {
    C::Uuid(uuid::Uuid::from_bytes([b; 16]))
}

/// Row 1: ordinary values; row 2: NULL everywhere it may be; row 3: the extremes, including
/// PostgreSQL `±infinity` clamped to the Arrow range (Q-M5-1).
fn images(variant: i64) -> Vec<Row> {
    let text = |s: &str| C::Text(format!("{s}{variant}"));
    let row = |cells: Vec<C>| {
        Row(schema()
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .zip(cells)
            .collect())
    };
    let nulls = |id| {
        let mut cells = vec![C::Int64(id)];
        cells.extend((1..schema().columns().len()).map(|_| C::Null));
        row(cells)
    };
    vec![
        row(vec![
            C::Int64(1),
            C::Bool(true),
            C::Int16(-7),
            C::Int32(42 + variant as i32),
            C::UInt64(18_446_744_073_709_551_615 - variant as u64),
            C::Float64(0.5),
            C::Numeric(Decimal::parse("-1234567890123456789012345678.0123456789").unwrap()),
            text("héllo ✓ "),
            C::Json(serde_json::json!({"a": [1, null], "v": variant})),
            C::Bytes(vec![0, 255, variant as u8]),
            C::Date(20_722),
            C::Time(45_296_789_012 + variant),
            C::Timestamp {
                micros: 1_790_000_000_123_456 + variant,
                with_tz: true,
            },
            C::Timestamp {
                micros: -1,
                with_tz: false,
            },
            uuid(0xab),
            C::Array {
                element_type: V::Text,
                values: vec![text("t"), C::Null],
            },
            C::Array {
                element_type: V::Uuid,
                values: vec![uuid(1), uuid(2)],
            },
        ]),
        nulls(2),
        row(vec![
            C::Int64(3),
            C::Bool(false),
            C::Int16(i16::MIN),
            C::Int32(i32::MAX),
            C::UInt64(0),
            C::Float64(f64::MIN_POSITIVE),
            C::Numeric(Decimal::parse("9999999999999999999999999999.9999999999").unwrap()),
            C::Text(String::new()),
            C::Json(serde_json::json!([])),
            C::Bytes(vec![]),
            C::Date(i32::MIN),
            C::Time(-3_020_399_000_000),
            C::Timestamp {
                micros: i64::MAX,
                with_tz: true,
            },
            C::Timestamp {
                micros: i64::MIN,
                with_tz: false,
            },
            uuid(0),
            C::Array {
                element_type: V::Text,
                values: vec![],
            },
            C::Array {
                element_type: V::Uuid,
                values: vec![C::Null],
            },
        ]),
    ]
}

/// The change set inserting (variant 0) or re-imaging (variant 7) every row, from the accumulator.
fn change_set(variant: i64) -> TableChangeSet {
    let lineage = Lineage {
        source_db: "db".into(),
        pipeline: "p".into(),
    };
    let never = FlushPolicy {
        seconds: None,
        rows: None,
        bytes: None,
    };
    let start = SourcePosition::Pg(Lsn(variant as u64 * 100));
    let pos = SourcePosition::Pg(Lsn(variant as u64 * 100 + 50));
    let mut acc = Accumulator::new(lineage, never, start);
    acc.add_table(schema(), &[]).unwrap();
    let now = Instant::now();
    acc.begin(1).unwrap();
    for (i, after) in images(variant).into_iter().enumerate() {
        let op = if variant == 0 { Op::Insert } else { Op::Update };
        let e = CdcEvent {
            op,
            table: table(),
            before: None,
            after: Some(after),
            source_pos: pos.clone(),
            change_seq: SourceSeq::Pg(Lsn(variant as u64 * 100 + i as u64 + 1)),
            xid: 1,
            commit_ts_micros: 0,
        };
        acc.apply(&e, now).unwrap();
    }
    acc.commit(
        &TransactionBoundary {
            xid: 1,
            commit_pos: pos,
            commit_ts_micros: 0,
        },
        now,
    )
    .unwrap();
    acc.flush().change_sets.pop().unwrap()
}

/// The reference content with the date and timestamp cells Delta stores as such clamped to the
/// Delta range.
fn clamped(expected: RecordBatch, ntz: NtzAs) -> RecordBatch {
    let columns: Vec<ArrayRef> = expected
        .schema()
        .fields()
        .iter()
        .zip(expected.columns())
        .map(|(f, c)| -> ArrayRef {
            match (f.name().as_str(), f.data_type()) {
                ("d", _) => std::sync::Arc::new(
                    c.as_primitive::<Date32Type>()
                        .unary::<_, Date32Type>(|d| d.clamp(DELTA_MIN_DATE, DELTA_MAX_DATE)),
                ),
                ("ntz", _) if ntz == NtzAs::String => c.clone(),
                (_, DataType::Timestamp(_, tz)) => {
                    let p: arrow_array::TimestampMicrosecondArray = c
                        .as_primitive::<TimestampMicrosecondType>()
                        .unary(|t| t.clamp(DELTA_MIN_MICROS, DELTA_MAX_MICROS));
                    std::sync::Arc::new(p.with_timezone_opt(tz.clone()))
                }
                _ => c.clone(),
            }
        })
        .collect();
    RecordBatch::try_new(expected.schema(), columns).unwrap()
}

async fn round_trip(location: DeltaLocation, ntz: NtzAs) -> DeltaSink {
    let sink = DeltaSink::new(DeltaSinkConfig {
        pipeline_id: "p".into(),
        location,
        table: TableDefaults {
            timestamp_ntz_as: ntz,
            ..TableDefaults::default()
        },
        merge_memory_limit: None,
    });
    let reference = MemorySink::new();
    let spec = TableSpec::new(&schema(), vec![]).unwrap();
    sink.ensure_table(&spec).await.unwrap();
    reference.ensure_table(&spec).await.unwrap();
    for variant in [0, 7] {
        let cs = change_set(variant);
        let marker = CommitMarker {
            owner_epoch: 1,
            committed_position: cs.position.end.clone(),
            pipeline_id: "p".into(),
        };
        sink.commit(cs.clone(), marker.clone()).await.unwrap();
        reference.commit(cs, marker).await.unwrap();
    }
    let got = sink.content(&table()).await.unwrap().unwrap();
    assert_eq!(
        got,
        clamped(reference.content(&table()).unwrap(), ntz),
        "{ntz:?}"
    );
    sink
}

#[tokio::test]
async fn every_type_round_trips_under_every_timestamp_ntz_option() {
    for ntz in [NtzAs::TimestampNtz, NtzAs::Timestamp, NtzAs::String] {
        let dir = tempfile::tempdir().unwrap();
        round_trip(
            DeltaLocation::Local {
                root: dir.path().to_path_buf(),
            },
            ntz,
        )
        .await;
    }
}

/// DuckDB's `delta_scan` on a table root: every cell as text, rows in key order. Needs `uv`
/// (the CI integration job installs it).
fn duckdb_read(root: &std::path::Path) -> Vec<Vec<Option<String>>> {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/duckdb_read.py");
    let out = std::process::Command::new("uv")
        .args([
            "run",
            "--no-project",
            "--with",
            "duckdb==1.5.6",
            "python",
            script,
        ])
        .arg(root)
        .output()
        .expect("uv is installed (https://docs.astral.sh/uv/)");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_str(stdout.trim()).unwrap()
}

/// R-12: an independent reader (DuckDB, delta-kernel based) reads the table, clamped bounds
/// included, and a table under an escaped root (Q-M4-5). DuckDB can't open a path with non-ASCII
/// text (Q-M4-6), so the escaped name here is ASCII.
#[tokio::test]
async fn duckdb_reads_the_table() {
    require_it!();
    let dir = tempfile::tempdir().unwrap();
    let location = DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    };
    round_trip(location.clone(), NtzAs::TimestampNtz).await;
    let rows = duckdb_read(&dir.path().join("public/types"));
    let names: Vec<String> = schema().columns().iter().map(|c| c.name.clone()).collect();
    let cell = |row: usize, name: &str| {
        let i = names.iter().position(|n| n == name).unwrap();
        rows[row][i].as_deref()
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(
        cell(0, "u64"),
        Some("18446744073709551608"),
        "decimal(20,0)"
    );
    assert_eq!(cell(0, "u"), Some("abababab-abab-abab-abab-abababababab"));
    assert_eq!(cell(0, "t"), Some("12:34:56.789019"), "time as text");
    assert_eq!(cell(0, "ntz"), Some("1969-12-31 23:59:59.999999"));
    assert_eq!(cell(1, "s"), None, "the all-NULL row");
    assert_eq!(
        cell(2, "d"),
        Some("0001-01-01"),
        "clamped to the Delta minimum"
    );
    assert_eq!(cell(2, "ntz"), Some("0001-01-01 00:00:00"));
    assert_eq!(
        cell(2, "dec"),
        Some("9999999999999999999999999999.9999999999")
    );
    let ascii = TableRef::new("sales 50%", "a/b~c");
    let odd = crate::scenarios::escaped_names(location, ascii).await;
    let root = data_avalanche_storage::table_root("", "", &odd.schema, &odd.name).root;
    let rows = duckdb_read(&dir.path().join(root));
    assert_eq!((rows.len(), rows[0][1].as_deref()), (1, Some("a")));
}
```

Create `crates/data-avalanche-sink-delta/tests/duckdb_read.py`:

```python
"""Reads a Delta table with DuckDB's delta_scan and prints every cell as text, as JSON rows.

Used by tests/sink/types.rs (R-12). Run: uv run --no-project --with duckdb python duckdb_read.py <table dir>
"""

import json
import sys

import duckdb

con = duckdb.connect()
scan = f"delta_scan('{sys.argv[1]}')"
cols = ", ".join(f'"{c}"::VARCHAR' for c in con.sql(f"SELECT * FROM {scan}").columns)
rows = con.sql(f"SELECT {cols} FROM {scan} ORDER BY id").fetchall()
print(json.dumps([list(r) for r in rows]))
```

Change `.github/workflows/ci.yml`:

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -72,6 +72,8 @@
             printf '%s' "$GCP_SA_KEY" > "$RUNNER_TEMP/gcs-key.json"
             echo "GOOGLE_SERVICE_ACCOUNT_PATH=$RUNNER_TEMP/gcs-key.json" >> "$GITHUB_ENV"
           fi
+      # uv runs the DuckDB cross-read of the Delta sink (R-12, crates/data-avalanche-sink-delta).
+      - uses: astral-sh/setup-uv@v10
       - name: Start test infrastructure
         run: test-infra/up.sh
       - name: Integration tests
```

Change `test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -3,7 +3,8 @@
 Containers for integration tests. Start with `test-infra/up.sh` (add `matrix` for older engine
 versions), stop with `test-infra/down.sh`, and run tests with
 `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test`. GCS tests need the variables from the
-GCS section below.
+GCS section below. The Delta sink's DuckDB cross-read also needs [`uv`](https://docs.astral.sh/uv/)
+on `PATH`; it fetches DuckDB itself.
 
 | Service | Image | Host port | Credentials | Profile |
 |---|---|---|---|---|
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-sink-delta --test sink`

Expected: PASS, 15 tests. The object-store and DuckDB tests return early without `DATA_AVALANCHE_IT` and count as passed. These tests drive the code of Tasks 2–9, so nothing fails first. A failure here is a defect in that code: stop and report it.

- [ ] **Step 3: Run them against the emulators**

Run: `test-infra/up.sh`, then `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-sink-delta --test sink -- --test-threads=2`

Expected: PASS, 15 tests (RustFS and Azurite included; GCS skipped).

- [ ] **Step 4: Run the gates**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-sink-delta/tests/sink/main.rs \
  crates/data-avalanche-sink-delta/tests/sink/types.rs \
  crates/data-avalanche-sink-delta/tests/duckdb_read.py \
  .github/workflows/ci.yml \
  test-infra/README.md
git commit -m "test(sink-delta): the Hop 3b round trip and the DuckDB cross-read"
```

---

## Verified during planning (2026-09-30)

The task blocks above were replayed in order on a fresh clone of `origin/main` (`b5e4306`). The
replay applied each task's files exactly as shown and ran each step's command. Every expected
failure and every count above is that replay's output. Measured along the way:

- **Emulators:** every scenario passes on RustFS and Azurite (Task 12), and Task 13's DuckDB
  cross-read passes with DuckDB 1.5.6 under `uv` 0.12.
- **Name probes (Q-M4-5, Q-M4-6):**
  - `%` in a table path breaks delta-rs and DuckDB alike.
  - A space, `+`, `#`, `?` and `~XX` work in both.
  - Non-ASCII works in delta-rs; DuckDB 1.5.5 and 1.5.6 drop the segment.
- **Engine limits:** delta-rs panics writing a `Date32` of `i32::MIN` (the reason for Q-M4-2).
  Under column mapping, ADD COLUMN still fails with "Column mapping" (the P7b canary).
- **P6 cost:** 8 files of 5,000 rows, 400 changed keys.
  - A key range rewrites 1 file and copies 4,600 rows.
  - Keys spread across the table rewrite all 8 files and copy 39,600 rows.
- **Workspace:** 499 tests pass (440 before M4). Clippy, fmt and rustdoc (`-D warnings`)
  are clean.
- **`cargo audit`** reports RUSTSEC-2026-0194 and -0195 (`quick-xml` 0.39.4, through deltalake's
  `object_store` 0.13.2). R-13 foresaw this, and the CI step is non-blocking. `deltalake` 1.1.0
  still requires `object_store ^0.13.2`, so no release fixes it yet. It also warns that
  `proc-macro-error2` (via deltalake-core's `validator`) is unmaintained.
- **Real GCS:** Task 12's GCS case passes on the CI bucket (run by Oleksii, 2026-09-30, 184 s), and
  PR CI runs it on every push.

## M4 exit checklist

The roadmap's M4 exit criteria, and where each is met:

- [x] **The delta-capability probes P1–P7 and P10 are re-run as integration tests against the crate
  on a local filesystem and RustFS**:
  - P1–P5, P7a/P7b and P6 on a local filesystem (Task 10);
  - the scenarios on RustFS, Azurite and real GCS (Task 12);
  - P10 (Task 13).
- [x] **Conformance with the M3 reference sink: the same change-set sequence gives the same table
  content**: Task 11 (`the_delta_table_equals_the_reference_sinks`).
- [x] **A foreign OPTIMIZE doesn't disarm the fence or the guard**, nor a truncate (final review,
  `4023bf7`):
  - Task 10 and Task 12 (`fence_and_guard_survive_a_foreign_optimize`);
  - Task 9 (`a_race_with_a_foreign_optimize_is_retried_and_lands`).
- [x] The M4 scope items not in the exit list, each with its test:
  - capabilities (Task 9);
  - `table_state` with foreign-format detection (Task 6);
  - `ensure_table`'s settings (Task 6);
  - Hop 3b (Tasks 3, 4, 13);
  - the MERGE with masks (Task 7);
  - the fence, the position and the walk-back (Tasks 5, 6, 9);
  - truncate (Tasks 7, 10);
  - `evolve` (Tasks 8, 10);
  - retry and conflicts (Task 9);
  - checkpoints and log cleanup (Task 10);
  - merge metrics (Tasks 9, 10).
- [x] `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` and `RUSTDOCFLAGS="-D warnings" cargo doc -p data-avalanche-sink-delta
  -p data-avalanche-storage --no-deps` are clean.
- [ ] The integration job of PR CI is green: emulators, DuckDB and real GCS. (Real GCS passed
  locally, run by Oleksii on 2026-09-30; the PR run is pending.)
- [x] Roadmap updates:
  - a new §3.10 with the Delta sink's landed interface;
  - §2.1's pin note;
  - §3.5's `escape_table_segment`;
  - §9.1 Q-M4-1…6;
  - R-12 and R-13;
  - D-IMPL-11 for the rulings D-M4.1…15.
- [x] `Type_Mapping.md` records the Delta clamp and the Hop 3b forms (Q-M4-2, D-M4.2).
  `M2-storage-and-state.md` D-M2.4 points at Q-M4-5.
- [x] `doc/v1.0/README.md` lists this plan and its status.
