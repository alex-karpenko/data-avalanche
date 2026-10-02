---
updated: 2026-10-02
domain: Data Flow
tags: [v1, implementation-plan, m5, postgresql, pgoutput, logical-replication, slot, publication, snapshot, type-mapping, tls]
resolves: []
sources: []
---

# M5 — PostgreSQL Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `data-avalanche-source-pg`, the PostgreSQL source, and the source contract it
implements. The crate turns pgoutput into `StreamItem`s and decodes every mapped type to
`ColumnValue` (Hop 1). It runs the slot and publication lifecycle and resolves, describes and
rescans tables (channels A, C, D). It reads keyset snapshot chunks. Every S-PG probe gate gets a
regression test, on PostgreSQL 18 on every change and on PostgreSQL 14–17 nightly.

**Architecture:** One change in core, one new crate.

- **`data-avalanche-core`**: the source contract (`source.rs`), next to `Sink` (Q-M5-2).
  - `SourceConnector` and `ChangeStream`, `StreamItem`, `SourceError` and the result types.
  - Nothing in it does I/O; M6 drives it and M7 implements it again for MySQL.
- **`data-avalanche-source-pg`** (new), one module per concern:
  - `error`: error classes, from SQLSTATEs and client errors.
  - `tls`: the TLS of both connections, and private files for inline PEMs.
  - `connect`: the catalog connection (`tokio-postgres`): cached, bounded by `timeouts.command`,
    session pinned. Also the replication connection's settings (`pgwire-replication`).
  - `pgoutput`: the protocol v1 decoder (Relation, Type, Origin, Insert, Update, Delete, Truncate).
  - `value`: Hop 1, a datum's text → `ColumnValue`.
  - `types`: type planning, OID → value type and decoder, with the fallback chain, numeric rules
    and overrides.
  - `catalog`: the catalog queries; table selection, the baseline schema, the identifier and
    replica identity checks.
  - `position`: LSN conversions.
  - `connector`: `PgConnector`, the preflight, channel A and the `SourceConnector` impl.
  - `slot`: the slot state machine and publication reconciliation.
  - `stream`: `PgStream`, pgoutput → `StreamItem`; acknowledgment.
  - `snapshot`: keyset chunks in the key's own deterministic collation (`COLLATE "C"` otherwise),
    and the keyed re-read.
  - `schema`: channel C (`augment`, the `attnum` diff) and channel D (`rescan`).
- **Tests:**
  - Unit tests in each module run without a server.
  - The integration binary `tests/pg` runs against `postgres-18` (`DATA_AVALANCHE_IT`). It covers
    resolution, the slot table, the stream (S-PG0…6), snapshots, channels C and D, the type round
    trip (S-PG7) and TLS (S-PG8, `postgres-tls`).
  - `nightly.yml` runs the binary on PostgreSQL 14–17.

**Tech Stack:** Rust 1.98.1 (edition 2024).

| Crate | Version / features | Used for |
|---|---|---|
| `pgwire-replication` | `=0.4.1`, `default-features = false`, features `tls-rustls`, `scram`, `md5` | The replication connection: START_REPLICATION, XLogData, status updates (the S-PG probes' transport; the pgoutput decoder is ours) |
| `tokio-postgres` | `0.7` (0.7.18) | The catalog connection: catalog queries, slot and publication commands, snapshot reads |
| `tokio-postgres-rustls` | `0.14` (0.14.0) | rustls for the catalog connection |
| `rustls` | `0.23` (aws-lc-rs provider, already in the workspace) | Both connections' TLS; the `verify-ca` and `require` verifiers |
| `webpki-roots` | `1` (1.0.9) | The default trust roots when `ssl_ca` is unset |
| `bytes`, `serde_json`, `tempfile`, `tokio` (`sync`), `uuid`, `async-trait` | workspace | Zero-copy datums, JSON/hstore values, private PEM files, the type-cache lock, the trait |
| `data-avalanche-testkit`, `proptest`, `rcgen`, `tokio` (`macros`, `rt-multi-thread`) | dev | Test gates, property tests, throwaway certificates |

**Spec:**
- `doc/v1.0/00-roadmap.md`: §2 (layout, dependency direction), §2.1 (pins), §3.1–§3.3, §3.6 (the
  source connector), §4 M5 scope and exit, §5.2–§5.4 (test infra, CI), D-IMPL-2, D-IMPL-3,
  D-IMPL-4, §9.1 Q-M5-1.
- `doc/specs/Data-Flow.md`: Phases 0–2 (preflight, resolution, snapshot, streaming), §Error Paths,
  §CdcEvent.
- `doc/specs/Configuration_Specification.md`: §9.1 `source` (`ssl_*`, `timeouts`,
  `toast_strategy`), §9.2.1 `source.postgres`, §9.3 `tables` (include/exclude, identifier and
  type overrides, `numeric_overflow_action`), §4.4 (PEM material), §11.2 (the PostgreSQL preflight
  rows), V-079, V-104.
- `doc/specs/Schema_Change_Detection_and_Evolution.md`: §0, §2.1, §3 (channels A–D), §6 (rename
  proof).
- `doc/research/Postgres_Slot_and_Publication_Lifecycle.md`: §1–§4, D-15.1–15.5.
- `doc/research/Type_Mapping.md`: the PostgreSQL table (Hop 1), §PostgreSQL-specific notes,
  §Date/time edge values.
- `doc/research/Snapshot_Resumability_and_Consistency.md`: §1, §2, §6.
- `doc/research/Ghost_Row_Dedup_and_Primary_Keys.md` (identifier and replica identity),
  `doc/research/Delivery_Guarantee_and_Position_Guard.md` §2 (positions).
- `prototypes/source-capability/README.md`: S-PG0…S-PG8, the evidence this crate re-runs.

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. `cargo clippy --workspace
  --all-targets -- -D warnings` and `cargo fmt --all --check` must pass before every commit.
- **Dependency direction** (roadmap §2): `source-pg → core, config`. `testkit` is a
  **dev**-dependency only. Nothing depends on `source-pg` in M5.
- **Pins:** `pgwire-replication = "=0.4.1"` (the probed transport). One rustls `CryptoProvider`
  (aws-lc-rs) is named explicitly in every rustls config the crate builds (DA-DEP-5).
- **Servers:** PostgreSQL 14 and later (`MIN_SERVER_VERSION = 140000`, P-8), `wal_level =
  logical`, UTF8 databases.
- **Names** (D-IMPL-7): `application_name` `data-avalanche` on the catalog connections; positions
  `pg:<X/Y>` (D-IMPL-4).
- **Test gates:** plain `cargo test` stays hermetic. Integration tests gate on
  `DATA_AVALANCHE_IT` and run against `postgres-18` (port 55418), or the server
  `DATA_AVALANCHE_PG_PORT` names. The TLS tests use `postgres-tls` (port 55419).
- **Credentials:** the tests use the compose services' fixed test credentials only. Certificates
  and keys are made at run time (by `rcgen` in unit tests, by the container for `postgres-tls`),
  never committed. No key, key path or secret goes in code, docs or output.
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `SourceError` (core), classified `Transient`, `Permanent`, `PositionUnreachable` or
  `TableHalt`.
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to break a mirror. Each is pinned by a test in the task after
the arrow.

1. **The catalog ahead of the stream.** An `ALTER TABLE` lands, then more DDL, before the stream
   reaches the first one. The schema the stream applies must be the one at its own position: the
   stream's types, the catalog's `attnum` for identity. Never the catalog's "now", and a rename
   is only proven by `attnum` → Task 11 (`scope_takes_the_stream_s_types_and_the_catalog_s_identity`,
   `channel_c_proves_a_rename_scoped_to_the_signal`), Task 9
   (`relation_changes_signal_before_the_next_change`).
2. **A slot the pipeline didn't leave the way it expects.** A slot can be missing, invalidated
   (`max_slot_wal_keep_size`), confirmed past the checkpoint, an orphan from an earlier pipeline,
   another plugin's, or busy. None may resume silently from the wrong place → Task 8 (each
   `decide` cell, `first_start_then_resume_then_a_lost_slot`,
   `an_orphan_halts_by_default_and_is_adopted_on_request`, `require_existing_never_creates`), Task 2
   (`a_busy_slot_is_fatal`, `removed_wal_and_invalidated_slots_are_unreachable_positions`).
3. **Keys that sort differently than the server's collation.** These are text with mixed case,
   accents and quotes, `"char"` above 127, numerics, and composite keys. A chunk cursor must
   resume exactly, with no row skipped or read twice → Task 10
   (`text_keys_page_in_one_deterministic_collation`, `composite_keys_of_every_key_type_resume_exactly`,
   `keys_compare_in_one_deterministic_collation`, `a_domain_over_char_key_resumes`).
4. **Values at the edges of what PostgreSQL prints.** These are `±infinity`, BC dates,
   `24:00:00`, numerics of 38+ digits, NaN, negative scales, varbit, unicode in arrays and hstore,
   and invalid UTF-8. Each decodes to the agreed value or halts only its table → Task 4
   (`dates_round_trip`, `timestamps_round_trip`, `numerics_and_money`, `array_text_never_panics`),
   Task 9 (`an_undecodable_value_halts_only_its_table`), Task 12
   (`every_mapped_type_round_trips_through_stream_and_snapshot`).
5. **Server version differences.** PostgreSQL 14 sends empty transactions; slot invalidation
   reads differently by version (`conflicting` from 16, `invalidation_reason` from 17); negative
   numeric scales exist from 15 → Task 8 (`slot_state` via `to_jsonb`), Task 9
   (`untracked_tables_busy_slots_and_keepalives`), Task 12 (the version-gated columns), Task 13
   (`nightly.yml`, PostgreSQL 14–17).

## Decisions and rulings

**Asked and answered (2026-09-26 and 2026-10-01):**

| # | Question | Answer |
|---|---|---|
| Q-M5-1 (2026-09-26) | PostgreSQL `±infinity` for `date` / `timestamp[tz]` | **Clamp to the Arrow min/max**; out-of-range timestamps clamp too (the Delta sink then clamps to its range, Q-M4-2); an identifier value that clamps halts the table instead (Q-M5-18) |
| Q-M5-2 | Where does the source trait live? Roadmap §3.6 puts it in `data-avalanche-pipeline` | **In `data-avalanche-core`**, next to `Sink`; the source crates depend on core only |
| Q-M5-3 | Binary tuples (roadmap: "text and binary tuples") | **Text only.** The stream never asks for binary; a binary datum is a protocol error |
| Q-M5-4 | `pgwire-replication` takes TLS material as file paths; config allows inline PEM | **An inline PEM becomes a private temporary file** (0600 in a 0700 directory, removed when the connector is dropped); a path PEM is used where it is |
| Q-M5-5 | Partitioned tables | **Replicate the leaves as tables.** A partitioned parent (`relkind = 'p'`) is skipped with a warning, named explicitly or by a glob; `publish_via_partition_root = false`. A publication the pipeline may not change that publishes via the root while a tracked table is a partition is refused (D-M5.14): the stream would drop those changes as untracked |
| Q-M5-6 | `toast_strategy: re_query` needs a keyed re-read; who builds it? | **M5 adds `fetch_rows`** to the trait; M6 calls it |
| Q-M5-7 | Where do PostgreSQL 14–17 run? | **A new `nightly.yml`** (scheduled and on dispatch), one job per version |
| Q-M5-8 | A bare `numeric` under `numeric_overflow_action: error` | **`decimal(38,18)`, halting per value**: a value that doesn't fit halts the table (`NUMERIC_OVERFLOW`); V-079 warns at resolution |
| Q-M5-9 | The `varbit` value layout ("length-prefixed") | **PostgreSQL's binary form**: a 4-byte big-endian bit count, then the bits packed MSB-first, the last byte zero-padded |
| Q-M5-10 | libpq treats `sslmode=require` with a root CA file as `verify-ca`. Should `require` with `ssl_ca` set check the chain? | **No: `require` encrypts without checking the chain**, on both connections, whether or not `ssl_ca` is set (D-M5.16). Only `verify-ca` and `verify-full` check it |
| Q-M5-12 (2026-10-01, during execution) | An override other than `string` or a decimal on a numeric column had to equal the mapped type: `int → int64` halted, and `timestamp_ntz_as: timestamp` had no per-column form | **Lossless conversions at Hop 1** (Task 5b): integer widening, an integer to a decimal with room, `float32 → float64`, `timestamp_ntz ↔ timestamp_tz`, `date → timestamp_*`, lists element by element, through one table in core for every source; anything else halts (`SCHEMA_MISMATCH`). Config §9.3 lists them |
| Q-M5-13 (2026-10-01, during execution) | PostgreSQL doesn't publish `UNLOGGED` tables, but channel A selected them: a managed publication refused the pipeline, and under `FOR ALL TABLES` the table was snapshotted once and then never changed | **Like a table without an identifier**: a glob-matched unlogged table is skipped with a warning, a named one halts (`TABLE_NOT_REPLICABLE`, new in core). Config §11.2 lists the precondition |
| Q-M5-14 (2026-10-01, during execution) | Task 8's review: pgoutput reads the publication as of each change's WAL position, so repairing it at start doesn't cover the WAL a `Resume` replays; a mixed-case publication name streamed nothing; `retained_bytes` measured from `confirmed_flush`; two pipelines on one managed publication drop each other's tables; Phase 5 acknowledged before persisting | **A repair that rewrites the publication re-snapshots (slot recreated); added tables are reported (`Prepared::added`) for M6 to snapshot; the publication name is quoted for pgoutput; `retained_bytes` from `restart_lsn`; V-059 is an error when a sharing pipeline is `managed`; Data-Flow Phase 5 persists before acknowledging** |
| Q-M5-15 (2026-10-02, during execution) | Task 9's review: `next` lost a Relation message when `select!` cancelled it at a catalog query; channel B didn't see a primary key or replica identity change; tables were matched by name, so a renamed or swapped table went on silently; PostgreSQL 14 sends a `"char"` byte above 127 raw; `close` returned before the server released the slot | **`next` cancel-safe (the event kept until handled; catalog queries under `timeouts.command`); the replica identity compared in channel B; a name and OID that part halt the table (`TABLE_DROPPED`, `Schema_Change…` §7); the raw byte decodes; `close` waits for the slot's release** |
| Q-M5-16 (2026-10-02, during execution) | Task 10's review: a text key in the database's collation made every chunk a full scan and sort (the `"C"` pin has no index); PostgreSQL 14 prints a `"char"` byte above 127 raw, which panicked the read; a domain over `"char"` key looped; a timed-out chunk kept the shared catalog connection busy | **A text key orders in its own collation when deterministic, `"C"` with a start warning when not (Snapshot_Resumability §2 amended); `"char"` read as its code; domains keyed by the decoder; `SET LOCAL statement_timeout` on every read** |
| Q-M5-17 (2026-10-02, during execution) | Task 11's review: with the catalog ahead of the stream, `scope` placed columns by their current names, so a rename chain or a swap after the signal gave a silent wrong delta; a table set `UNLOGGED` (allowed under `FOR ALL TABLES`) went unnoticed; an add/backfill/drop migration halted; `Schema_Change…` §7.2 halted on any replica identity change, the code only on an identifier change | **A placement that is a guess halts; a column dropped since the signal keeps its `attnum`; an unlogged table doesn't plan (C and D halt it); §7.2 amended: only the identifier, or a replica identity that no longer covers it, halts** |
| Q-M5-18 (2026-10-02, during execution) | Task 12's review: a timestamp key past the microseconds a timestamp holds (294247–294276 AD, or a `date` converted past it) clamps to `i64::MAX`, the same value as `'infinity'`: distinct keys merge into one identifier, and a snapshot cursor there ends the scan early (measured: 2 of 4 rows) | **An identifier value that clamps halts the table (`SCHEMA_MISMATCH`), in the stream and in the snapshot; `±infinity` keys and other columns are unchanged; the type round trip gains the missing edge values and overrides** |
| Q-M5-19 (2026-10-02, during execution) | Task 12's re-review: the Delta sink clamps every `date` / `timestamp` cell to the Delta range in Hop 3b (Q-M4-2), keys included, so distinct keys still merged: BC keys, keys from 10000 AD, and `±infinity` against the bounds | **The Delta sink halts the table (`Permanent`) on an identifier cell outside the Delta range, `±infinity` keys included; other columns still clamp** |

**Rulings where the specs are silent** (each is pinned by a test). **Plan approved 2026-10-01**, with these rulings (roadmap D-IMPL-12):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M5.1 | The trait differs from roadmap §3.6. `resolve_tables()` takes no filter: the connector is built from the pipeline. `prepare(checkpoint, tables) -> Prepared { plan: StartPlan, warnings }` runs the slot state machine. New methods: `reset_position`, `retained_bytes`, `fetch_rows` and `teardown`. `augment(cached, hint)` replaces `describe`, because a signal must be read against the stream's position (§0). `open_stream` takes the tracked `TableSchema`s. `ChangeStream::close` ends a stream cleanly. `StreamItem::Begin` has no `commit_pos`, and `StreamItem::TableHalt` is new. `SchemaHint` is `Columns` (PostgreSQL) or `Ddl` (MySQL) | Each change is something M6 needs from a source and §3.6 lacks. A begin position isn't known to every source (MySQL learns it at the XID event), and the commit carries it anyway | Task 1, Task 11 |
| D-M5.2 | Positions. A change's `source_pos` is its transaction's commit LSN (Begin `final_lsn`, the `_lsn` lineage column). Its `seq` is the XLogData `wal_start` of its message. `Commit`'s `TransactionBoundary.commit_pos` is the Commit `end_lsn`, the position acknowledged and resumed from | `final_lsn` orders transactions as they committed. `end_lsn` is what `START_REPLICATION` resumes after without replaying the transaction | Task 9 (`s_pg0_an_insert_round_trips`, `s_pg4_resume_from_the_acknowledged_position`) |
| D-M5.3 | Relation messages carry `wal_start` 0, so a `SchemaSignal`, or a `TableHalt` for columns that can't be planned, is held back. It is emitted just before the table's next change, with that change's `seq` | A signal must sit between the changes before and after it in `seq` order (measured on PostgreSQL 18) | Task 9 (`relation_changes_signal_before_the_next_change`) |
| D-M5.4 | The stream doesn't reconnect by itself. A stream error is returned (`Transient` for I/O and server restarts), and M6 reopens from its checkpoint with a backoff. The catalog connection reopens on its next call. The roadmap's "reconnect with backoff" moves to M6 | Reopening from the checkpoint is the only resume that can't skip a transaction, and the backoff policy belongs to the pipeline's error paths | Task 2 (`shutdowns_and_io_are_transient`), Task 9 (`s_pg4_resume_from_the_acknowledged_position`) |
| D-M5.5 | Both connections pin the session: `TimeZone=UTC`, `DateStyle=ISO`, `IntervalStyle=iso_8601`, `extra_float_digits=3`, `bytea_output=hex`, `lc_monetary=C`, `standard_conforming_strings=on`. The catalog connection reports `application_name` `data-avalanche`; `pgwire-replication` 0.4.1 hard-codes `pgwire-replication` for the walsender. The status interval and the wakeup interval are `postgres.heartbeat` | The text of every value depends on these settings; the decoders assume them | Task 2 (`the_catalog_connection_pins_the_session`, `the_replication_connection_pins_the_same_session`) |
| D-M5.6 | Errors. SQLSTATE `08*`, `53*`, `57P*`, `57014`, `40001`, `40P01`, `55P03` → `Transient`. `58P01` about WAL and `55000` "can no longer get changes" → `PositionUnreachable`. `55006` (slot active) → `Permanent`. Every other server error → `Permanent`. A TLS failure anywhere in the error chain → `Permanent`. Other client errors (closed connection, I/O, timeouts) → `Transient`. `pgwire-replication` I/O and worker errors → `Transient`; auth, TLS and protocol → `Permanent` | Data-Flow §Error Paths: retry what a reconnect can outlive, halt on the rest. A busy slot must never become a retry loop (D-15.5) | Task 2 |
| D-M5.7 | The stream passes every transaction boundary, empty ones included (PostgreSQL 14 sends a Begin/Commit for each transaction of the database; 15 and later skip empty ones). Changes to untracked tables are dropped. Keep-alives come only between transactions | D-IMPL-2: idle tables advance with the frontier, and WAL is released on an idle pipeline | Task 9 (`untracked_tables_busy_slots_and_keepalives`) |
| D-M5.8 | Preflight requires PostgreSQL ≥ 14, `wal_level = logical`, a UTF8 database and SELECT on the catalog. A role without the REPLICATION attribute gets a warning, not a refusal | Managed services grant replication through role membership (`rds_replication`); the replication connection itself is the real check | Task 7 (`preflight_accepts_the_test_server`, `preflight_refuses_a_user_who_cant_log_in`) |
| D-M5.9 | Numerics. `numeric(P,S)` with P ≤ 38 → `decimal(P,S)`. A negative scale → `decimal(P+|S|, 0)`. S > P → `decimal(S,S)`. P > 38 under `string` → text with a warning, and under `error` → a halt at resolution. A bare `numeric` → text under `string`, `decimal(38,18)` under `error` (Q-M5-8). NaN and `±Infinity` → `SCHEMA_MISMATCH` halt. `money` → `decimal(19,2)` | Type_Mapping's cap, filled in where it is silent | Task 4, Task 5 (`numeric_precision_rules`) |
| D-M5.10 | Overrides. `string` reads the column as its PostgreSQL text. `decimal(P,S)` on a `numeric` or `money` base (through domains) decodes as that decimal. Any other override must be Hop 2-compatible with the column's mapped type or a lossless conversion (Q-M5-12, Task 5b), or the table halts (`SCHEMA_MISMATCH`). Other fallbacks: multi-dimensional arrays halt; array bound decorations are dropped; PostGIS hex EWKB → bytes; hstore → a JSON object; unknown types → their text with a warning | Config §9.3 `column_type_overrides`; the fallback chain of Type_Mapping | Task 5 (`overrides`, `user_and_extension_types`, `arrays_follow_their_element`), Task 5b (`overrides_convert_losslessly`) |
| D-M5.11 | Selection. System schemas (`pg_*`, `information_schema`) are never matched. A glob `*` stays inside one name segment and is case-sensitive. An explicit name that matches nothing warns. Generated columns (`attgenerated <> ''`) are left out | Config §9.3; pgoutput doesn't send generated columns | Task 6 (`globs_stay_inside_a_name`, `selection`) |
| D-M5.12 | Identifier. The override, else the primary key. It must lie within the replica identity (an index, FULL, or the default primary key), else the table halts (`IDENTIFIER_NOT_IN_REPLICA_IDENTITY`). No identifier: a glob-matched table is skipped with a warning, a named one halts. An unlogged table the same way, halting `TABLE_NOT_REPLICABLE` (Q-M5-13) | Ghost_Row_Dedup §PK policy; a delete must carry the identifier | Task 6 (`identifiers_and_replica_identity`), Task 7 (`channel_a_resolves_checks_and_skips`) |
| D-M5.13 | The slot state table (`decide`). Checkpoint and slot → `Resume`, except: another plugin or database → `Refuse`; invalidated, or `confirmed_flush` past the checkpoint → `Recreate(PositionLost)` under `managed`, `Refuse` under `require_existing`. A checkpoint and no slot → `Create(PositionLost)` under `managed`, else `Refuse`. A slot and no checkpoint (an orphan) → `Refuse` under `on_orphan: halt`; `Adopt` (advance to the current WAL, then snapshot) under `adopt_resnapshot`, `require_existing` included; an invalidated orphan → `Recreate(AdoptedOrphan)` under `managed`, else `Refuse`. Neither → `Create(FirstStart)` under `managed`, else `Refuse` | Postgres_Slot… §1–§3. A slot past the checkpoint has released WAL the pipeline still needs | Task 8 (the seven `decide` tests, the integration cells) |
| D-M5.14 | Publication. Under `managed`: create it when absent, `ADD`/`DROP TABLE` to match the tracked set, and set `publish = 'insert, update, delete, truncate'` and `publish_via_partition_root = false`. Under `require_existing`, or for a `FOR ALL TABLES` publication: never change it, warn on each difference, except one. With `publish_via_partition_root = true` and a tracked table that is a partition, `prepare` is refused (`Permanent`), naming the partitions and the `ALTER PUBLICATION` that fixes it. A `managed` repair that changes what is published for every table (created, operations, `publish_via_partition_root`) turns a `Resume` into `Recreate(PositionLost)`; tables it adds come back in `Prepared::added` (Q-M5-14) | D-15.4. A partition's changes would arrive under its untracked parent's name and be dropped: silent loss, so a refusal, not a warning (Q-M5-5) | Task 8 (`the_publication_follows_the_table_set`, `require_existing_never_creates`, `a_root_publication_of_tracked_partitions_is_refused`) |
| D-M5.15 | `prepare` reconciles the publication, then the slot. Creating a slot (it waits for running transactions) is bounded by `timeouts.snapshot`; every other catalog call by `timeouts.command`. `reset_position` drops and recreates the slot (refused under `require_existing`). `teardown` drops the slot and the publication only under `managed` with `auto_cleanup`. `retained_bytes` is `pg_current_wal_lsn() - restart_lsn`, the WAL the slot keeps on disk (Task 8 fix). Advancing an orphan (`Adopt`) is bounded by `timeouts.snapshot` too | The `max_wal_lag_bytes` guard (V-104) compares against it in M6 | Task 8 (`reset_teardown_and_retained_wal`) |
| D-M5.16 | TLS. `allow` connects as `prefer` (the replication crate has no `allow`; both accept either kind). `require` and `prefer` encrypt but don't verify the chain (signatures are still checked), even with `ssl_ca` set (Q-M5-10). `verify-ca` checks the chain and ignores the name. `verify-full` checks both. The trust roots are `ssl_ca`, else the webpki roots. `ssl_cert` and `ssl_key` go together | Config §9.1; one meaning of each mode on both connections | Task 2 (`modes_map_to_both_clients`, `bad_material_is_refused_by_name`), Task 13 |
| D-M5.17 | Snapshot reads. They use the simple-query protocol, so values are the output-function text pgoutput sends (`true::text` is `true`, pgoutput sends `t`). Columns are qualified by the alias `src`, so an output name can't shadow a key. A text key compares in its own collation when that is deterministic (its index serves the chunk), in `COLLATE "C"` otherwise, with a warning at resolution; the cursor records the collations, and a cursor of others halts the table (Q-M5-16). Key values are quoted literals cast to the column's `format_type` text; a `"char"` (or a domain over one) is read as its code and keyed through a signed byte. Each read carries `SET LOCAL statement_timeout` = `timeouts.command`, so the server cancels it first. A column changed under the snapshot (gone, or a different type) → `Transient`: read again once the stream's signal caught up. `fetch_rows` sends `FETCH_BATCH` = 1000 keys per statement | Snapshot_Resumability §2; a snapshot row must equal the streamed row of the same data | Task 10 |
| D-M5.18 | Channel C (`augment`). It takes the hint's columns (names and types, from the stream) and finds each in the catalog by name, else by the `attnum` it had in the cached schema; that `attnum` and the nullability come from the catalog. A column found neither way halts the table. A column dropped since the signal keeps its cached `attnum` (an `attnum` is never reused). The catalog may be ahead of the stream, so a placement that is a guess halts the table: two columns on one `attnum`, `attnum`s out of the stream's order (pgoutput sends `attnum` order), a column of the cached schema the catalog still has with no stream column on it (it existed at the signal), a column placed as dropped since the signal while another cached column is gone too and placed nowhere, a catalog column below the highest placed column the catalog still has with no stream column on it (it existed at the signal), or a column placed as dropped since the signal while a catalog column has no stream column on it (Q-M5-17). A later DDL that breaks planning halts the table at an earlier signal too (fail-safe). `diff` matches by `attnum`, so a new name on the same `attnum` is a proven rename. A type change carries `narrowed` (a shorter length or lower precision of the same logical type). A table gone from the catalog → `[TableDropped]` and no schema | §0 (scope a change to its signal) and §6 (rename proof) | Task 11 |
| D-M5.19 | Channel D (`rescan`). One batched read of the tracked tables. It reports tables gone from `pg_class` as dropped, tables whose catalog schema differs as changed (with that schema, for freshness and alerting only), and tables that now fail planning as halted, an unlogged table among them (`TABLE_NOT_REPLICABLE`, Q-M5-17) | §2.1 `TABLE_DROPPED`; the rescan never applies a change itself | Task 11 (`channel_d_finds_dropped_changed_and_broken_tables`) |

**Measured while planning** (recorded in the tasks and the docs):

- PostgreSQL 15 and later skip empty transactions; 14 sends a Begin/Commit for every transaction
  of the database.
- A Relation message's XLogData `wal_start` is 0 (PostgreSQL 18), hence D-M5.3.
- Partitions (PostgreSQL 14–18, `publish_via_partition_root = false`): a write through the parent
  arrives under its leaf. An update that moves a row to another partition arrives as a delete in
  the old leaf and an insert in the new one. A TRUNCATE of the parent lists the leaves.
- PostgreSQL's first date prints as `4714-11-24 BC` (-2,440,588 days from 1970); the last is
  `5874897-12-31` (2,145,042,905 days).
- `tempfile` creates directories 0755 unless told otherwise; the PEM directory asks for 0700.
- `pgwire-replication` 0.4.1: `connect()` returns before the server answers, so errors surface on
  the first `recv`. A server error reads `"<message> (SQLSTATE xxxxx)"`. The walsender's
  `application_name` is fixed to `pgwire-replication`.

**Documents this plan changes** (applied at plan approval, 2026-10-01, in a `research:` commit and
an `infra:` commit; `test-infra/README.md` changes in Task 13):

- Roadmap:
  - §2: the source contract lives in core (Q-M5-2).
  - §2.1: the `pgwire-replication =0.4.1` pin and `tokio-postgres-rustls`.
  - §3.6: rewritten to the contract of Task 1 (D-M5.1).
  - §4 M5: text tuples only (Q-M5-3); the reconnect backoff and the `max_wal_lag_bytes` halt move
    to M6, with the start-position note below.
  - §4 M6, "Carried from M5":
    - persist the start position right after `prepare` creates the slot (a crash before the
      first checkpoint leaves an orphan);
    - the reconnect backoff (D-M5.4);
    - the `max_wal_lag_bytes` halt on `retained_bytes`;
    - keep-alives can arrive often (each heartbeat, and on every server keep-alive).
  - §5.2: `postgres-tls`. §5.4: `nightly.yml`.
  - §9: D-IMPL-12 for D-M5.1…19. §9.1: Q-M5-2…10.
- `Type_Mapping.md`: the numeric rules of D-M5.9 and Q-M5-8 in §PostgreSQL-specific notes; the
  `varbit` layout (Q-M5-9); PostGIS as hex EWKB decoded to bytes.
- `test-infra/README.md` (Task 13): `postgres-tls`, `DATA_AVALANCHE_PG_PORT`, the nightly run.

No spec edit is needed. Config §9.3's numeric text already allows Q-M5-8 (V-079 warns that a bare
`numeric` may halt under `error`).

---

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` | Workspace: the new crate; `pgwire-replication`, `tokio-postgres-rustls`, `webpki-roots` |
| `crates/data-avalanche-core/src/source.rs`, `…/lib.rs` | The source contract: `SourceConnector`, `ChangeStream`, `StreamItem`, `SourceError`, the result types, `identifier_values` |
| `crates/data-avalanche-core/src/convert.rs` | The lossless override conversions: `conversion`, `ColumnValue::convert` (Q-M5-12) |
| `crates/data-avalanche-source-pg/Cargo.toml` | The crate and its dependencies |
| `…-source-pg/src/lib.rs` | Modules, root re-exports, crate docs |
| `…-source-pg/src/error.rs` | SQLSTATE and client-error classes → `SourceError` |
| `…-source-pg/src/tls.rs` | `PemFiles`, `replication_tls`, `control_tls`, the `verify-ca` and unverified verifiers |
| `…-source-pg/src/connect.rs` | `Connector` (the catalog connection, the replication settings), `SESSION_OPTIONS`, `APPLICATION_NAME` |
| `…-source-pg/src/test_support.rs` | Unit-test fixtures: a resolved pipeline |
| `…-source-pg/src/pgoutput.rs` | The pgoutput v1 decoder |
| `…-source-pg/src/value.rs` | Hop 1: `Decode`, `decode`, the date/time parsers, `text_of` |
| `…-source-pg/src/types.rs` | Type planning: `TypeInfo`, `plan`, `plan_column`, the numeric rules |
| `…-source-pg/src/catalog.rs` | Globs, selection, catalog queries, `table_schema` |
| `…-source-pg/src/position.rs` | LSN conversions |
| `…-source-pg/src/connector.rs` | `PgConnector`: preflight, channel A, the `SourceConnector` impl |
| `…-source-pg/src/slot.rs` | `decide`, slot and publication commands, `prepare`, `reset_position`, `teardown`, `retained_bytes` |
| `…-source-pg/src/stream.rs` | `PgStream`, `open_stream` |
| `…-source-pg/src/snapshot.rs` | `read_chunk`, `fetch_rows` |
| `…-source-pg/src/schema.rs` | `scope`, `diff`, `narrowed`, `augment`, `rescan` |
| `…-source-pg/tests/pg/main.rs` | The integration binary |
| `…/tests/pg/common.rs` | Fixtures: per-test schema, slot and publication; pipelines; stream helpers |
| `…/tests/pg/resolve.rs` | Preflight and channel A |
| `…/tests/pg/slot.rs` | The slot table cell by cell; publication reconciliation |
| `…/tests/pg/stream.rs` | S-PG0…6; signals; value halts; keep-alives |
| `…/tests/pg/snapshot.rs` | Chunks, collations, key types, the keyed re-read |
| `…/tests/pg/schema.rs` | Channels C and D |
| `…/tests/pg/types.rs` | S-PG7: every mapped type through stream and snapshot |
| `…/tests/pg/tls.rs` | S-PG8: TLS on both connections |
| `test-infra/compose.yaml`, `test-infra/postgres-tls.sh` | The `postgres-tls` service and its certificate setup |
| `.gitignore` | `test-infra/.tls/` |
| `test-infra/README.md` | `postgres-tls`, `DATA_AVALANCHE_PG_PORT`, the nightly run |
| `.github/workflows/nightly.yml` | PostgreSQL 14–17 |

---

### Task 1: The source contract in `data-avalanche-core`

**Files:**
- Create: `crates/data-avalanche-core/src/source.rs`
- Modify: `crates/data-avalanche-core/src/lib.rs`

**Interfaces:**
- Consumes: `CdcEvent`, `ColumnSchema`, `ColumnValue`, `HaltReason`, `Row`, `SchemaDelta`,
  `SourcePosition`, `SourceSeq`, `TableRef`, `TableSchema`, `TransactionBoundary` (core, M0/M3).
- Produces (core root; every later task and M6/M7 use these):
  - `SourceError::{Transient(String), Permanent(String), PositionUnreachable { position, detail },
    TableHalt { table, reason }}` and `is_transient()`.
  - `Preflight { server_version, warnings }`; `ResolvedTables { tables, halted, skipped, warnings }`.
  - `SnapshotReason::{FirstStart, PositionLost, AdoptedOrphan}`; `StartPlan::{Resume,
    Snapshot { from, reason }}`; `Prepared { plan, warnings }`.
  - `KeysetCursor { key: Row, collation: Option<String> }`; `Chunk { rows, next: Option<KeysetCursor> }`.
  - `SchemaHint::{Columns(Vec<ColumnSchema>), Ddl(String)}`.
  - `StreamItem::{Begin { xid, commit_ts_micros }, Change(CdcEvent), Truncate { tables, seq },
    SchemaSignal { table, seq, hint }, TableHalt { table, seq, reason },
    Commit(TransactionBoundary), KeepAlive(SourcePosition)}`.
  - `RescanReport { dropped, changed, halted }`.
  - `trait SourceConnector` (`preflight`, `resolve_tables`, `prepare`, `reset_position`,
    `retained_bytes`, `current_position`, `read_chunk`, `fetch_rows`, `open_stream`, `augment`,
    `rescan`, `teardown`) and `trait ChangeStream` (`next`, `acknowledge`, `close`).
  - `identifier_values(&TableSchema, &Row) -> Option<Row>`.

Q-M5-2 puts the trait in core, next to `Sink`, so both source crates and the pipeline depend
only on core. The shape differs from roadmap §3.6 in ways D-M5.1 lists; the roadmap is updated
with the plan. Nothing here performs I/O. The tests use a one-transaction connector as a trait
object, polled by a no-op waker, so core needs no runtime.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-core/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/lib.rs b/crates/data-avalanche-core/src/lib.rs
--- a/crates/data-avalanche-core/src/lib.rs
+++ b/crates/data-avalanche-core/src/lib.rs
@@ -15,6 +15,7 @@
 pub mod position;
 pub mod schema;
 pub mod sink;
+pub mod source;
 pub mod table;
 pub mod value;
 
@@ -33,6 +34,11 @@
     CommitMarker, CommitOutcome, EvolveOutcome, PartitionField, PartitionTransform, SchemaOp, Sink,
     SinkCapabilities, SinkError, SinkKind, SinkTableState, TableSpec, TransformKind, WriteMode,
 };
+pub use source::{
+    ChangeStream, Chunk, KeysetCursor, Preflight, Prepared, RescanReport, ResolvedTables,
+    SchemaHint, SnapshotReason, SourceConnector, SourceError, StartPlan, StreamItem,
+    identifier_values,
+};
 pub use table::{
     INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
 };
```

Create `crates/data-avalanche-core/src/source.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::Lsn;
    use crate::{ColumnValueType, Op};

    fn schema() -> TableSchema {
        TableSchema::new(
            TableRef::new("public", "t"),
            vec![
                ColumnSchema::new("a", ColumnValueType::Int64, "bigint").not_null(),
                ColumnSchema::new("b", ColumnValueType::Text, "text").not_null(),
                ColumnSchema::new("v", ColumnValueType::Text, "text"),
            ],
            vec!["b".into(), "a".into()],
        )
        .unwrap()
    }

    #[test]
    fn identifier_values_follow_the_identifier_order() {
        let row = Row(vec![
            ("a".into(), ColumnValue::Int64(1)),
            ("b".into(), ColumnValue::Text("x".into())),
            ("v".into(), ColumnValue::Null),
        ]);
        assert_eq!(
            identifier_values(&schema(), &row),
            Some(Row(vec![
                ("b".into(), ColumnValue::Text("x".into())),
                ("a".into(), ColumnValue::Int64(1)),
            ]))
        );
    }

    #[test]
    fn identifier_values_need_every_key_column() {
        let missing = Row(vec![("a".into(), ColumnValue::Int64(1))]);
        assert_eq!(identifier_values(&schema(), &missing), None);
        let null = Row(vec![
            ("a".into(), ColumnValue::Int64(1)),
            ("b".into(), ColumnValue::Null),
        ]);
        assert_eq!(identifier_values(&schema(), &null), None);
    }

    #[test]
    fn only_transient_errors_are_retried() {
        assert!(SourceError::Transient("reset".into()).is_transient());
        assert!(!SourceError::Permanent("denied".into()).is_transient());
        assert!(
            !SourceError::PositionUnreachable {
                position: "0/10".into(),
                detail: "removed".into()
            }
            .is_transient()
        );
        let halt = SourceError::TableHalt {
            table: TableRef::new("public", "t"),
            reason: HaltReason::TableDropped,
        };
        assert!(!halt.is_transient());
        assert_eq!(
            halt.to_string(),
            "table public.t: the table was dropped at the source; the target is kept, and a re-created table needs an operator re-snapshot"
        );
    }

    /// A connector that serves one transaction: the traits are object-safe and usable as
    /// `Box<dyn …>`.
    struct OneTxn;

    struct OneTxnStream(Vec<StreamItem>);

    #[async_trait]
    impl ChangeStream for OneTxnStream {
        async fn next(&mut self) -> Result<StreamItem, SourceError> {
            if self.0.is_empty() {
                return Err(SourceError::Transient("end of test stream".into()));
            }
            Ok(self.0.remove(0))
        }

        async fn acknowledge(&mut self, _: &SourcePosition) -> Result<(), SourceError> {
            Ok(())
        }

        async fn close(self: Box<Self>) -> Result<(), SourceError> {
            Ok(())
        }
    }

    #[async_trait]
    impl SourceConnector for OneTxn {
        async fn preflight(&self) -> Result<Preflight, SourceError> {
            Ok(Preflight::default())
        }
        async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
            Ok(ResolvedTables {
                tables: vec![schema()],
                ..ResolvedTables::default()
            })
        }
        async fn prepare(
            &self,
            checkpoint: Option<&SourcePosition>,
            _: &[TableRef],
        ) -> Result<Prepared, SourceError> {
            let plan = match checkpoint {
                Some(_) => StartPlan::Resume,
                None => StartPlan::Snapshot {
                    from: SourcePosition::Pg(Lsn(0x10)),
                    reason: SnapshotReason::FirstStart,
                },
            };
            Ok(Prepared {
                plan,
                warnings: vec![],
            })
        }
        async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
            Ok(None)
        }
        async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
            Ok(SourcePosition::Pg(Lsn(0x20)))
        }
        async fn current_position(&self) -> Result<SourcePosition, SourceError> {
            Ok(SourcePosition::Pg(Lsn(0x30)))
        }
        async fn read_chunk(
            &self,
            _: &TableSchema,
            _: Option<&KeysetCursor>,
            _: usize,
        ) -> Result<Chunk, SourceError> {
            Ok(Chunk {
                rows: vec![],
                next: None,
            })
        }
        async fn fetch_rows(&self, _: &TableSchema, _: &[Row]) -> Result<Vec<Row>, SourceError> {
            Ok(vec![])
        }
        async fn open_stream(
            &self,
            _: &SourcePosition,
            _: &[TableSchema],
        ) -> Result<Box<dyn ChangeStream>, SourceError> {
            let table = TableRef::new("public", "t");
            let commit_pos = SourcePosition::Pg(Lsn(0x48));
            let after = Row(vec![
                ("a".into(), ColumnValue::Int64(1)),
                ("b".into(), ColumnValue::Text("x".into())),
                ("v".into(), ColumnValue::Null),
            ]);
            Ok(Box::new(OneTxnStream(vec![
                StreamItem::Begin {
                    xid: 7,
                    commit_ts_micros: 1,
                },
                StreamItem::Change(CdcEvent {
                    op: Op::Insert,
                    table,
                    before: None,
                    after: Some(after),
                    source_pos: SourcePosition::Pg(Lsn(0x40)),
                    change_seq: SourceSeq::Pg(Lsn(0x38)),
                    xid: 7,
                    commit_ts_micros: 1,
                }),
                StreamItem::Commit(TransactionBoundary {
                    xid: 7,
                    commit_pos,
                    commit_ts_micros: 1,
                }),
            ])))
        }
        async fn augment(
            &self,
            cached: &TableSchema,
            _: &SchemaHint,
        ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
            Ok((
                SchemaDelta {
                    table: cached.table().clone(),
                    changes: vec![],
                },
                Some(cached.clone()),
            ))
        }
        async fn rescan(&self, _: &[TableSchema]) -> Result<RescanReport, SourceError> {
            Ok(RescanReport::default())
        }
        async fn teardown(&self) -> Result<(), SourceError> {
            Ok(())
        }
    }

    #[test]
    fn the_traits_work_as_trait_objects() {
        let source: Box<dyn SourceConnector> = Box::new(OneTxn);
        let prepared = block_on(source.prepare(None, &[])).unwrap();
        assert!(matches!(
            prepared.plan,
            StartPlan::Snapshot {
                reason: SnapshotReason::FirstStart,
                ..
            }
        ));
        let mut stream =
            block_on(source.open_stream(&SourcePosition::Pg(Lsn(0x10)), &[schema()])).unwrap();
        assert!(matches!(
            block_on(stream.next()).unwrap(),
            StreamItem::Begin { xid: 7, .. }
        ));
        let StreamItem::Change(e) = block_on(stream.next()).unwrap() else {
            panic!("a change")
        };
        e.validate().unwrap();
        let StreamItem::Commit(b) = block_on(stream.next()).unwrap() else {
            panic!("a commit")
        };
        block_on(stream.acknowledge(&b.commit_pos)).unwrap();
        assert!(block_on(stream.next()).unwrap_err().is_transient());
        block_on(stream.close()).unwrap();
    }

    /// Polls a future that never waits (the test connector's methods are all ready at once).
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        use std::task::{Context, Poll, Waker};
        let mut f = std::pin::pin!(f);
        match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(v) => v,
            Poll::Pending => panic!("the test connector never waits"),
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core --lib source::`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `source::ChangeStream`, `source::Chunk`, `source::KeysetCursor`, `source::Preflight`, `source::Prepared`, `source::RescanReport`, `source::ResolvedTables`, `source::SchemaHint`, `source::SnapshotReason`, `source::SourceConnector`, `source::SourceError`, `source::StartPlan`, `source::StreamItem`, `source::identifier_values`
error: cannot find attribute `async_trait` in this scope
error[E0425]: cannot find type `TableSchema` in this scope
error[E0433]: cannot find type `TableSchema` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-core/src/source.rs`, above the test module:

```rust
//! The source connector contract (roadmap §3.6, Data-Flow Phases 0–2): what the pipeline asks of a
//! source engine. Each source crate implements it; nothing here performs I/O.

use async_trait::async_trait;
use thiserror::Error;

use crate::{
    CdcEvent, ColumnSchema, ColumnValue, HaltReason, Row, SchemaDelta, SourcePosition, SourceSeq,
    TableRef, TableSchema, TransactionBoundary,
};

/// Why a source call failed (Data-Flow §Error Paths). The class decides what the pipeline does.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SourceError {
    /// Lost connection, timeout, server restart, a busy server: discard the uncommitted buffer and
    /// reopen from the checkpoint after a backoff.
    #[error("transient source error: {0}")]
    Transient(String),
    /// A configuration, privilege, server-setting or protocol problem: the pipeline halts until an
    /// operator acts. Retrying would only repeat it.
    #[error("{0}")]
    Permanent(String),
    /// The source can no longer serve the stream from `position` (PostgreSQL WAL removed or slot
    /// invalidated, MySQL binlog purged). Recovery is a new stream position and a re-snapshot
    /// (Postgres_Slot… D-15.3, `Schema_Change…` §8.2).
    #[error("the source can't serve position {position}: {detail}")]
    PositionUnreachable { position: String, detail: String },
    /// One table can't be replicated; the pipeline's other tables continue.
    #[error("table {table}: {reason}")]
    TableHalt { table: TableRef, reason: HaltReason },
}

impl SourceError {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

/// What a pipeline-level preflight found (config §11.2, the rows that halt a pipeline). A failed
/// check is an `Err(SourceError::Permanent)`; warnings don't stop the pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Preflight {
    /// The server's version string, for status and logs.
    pub server_version: String,
    pub warnings: Vec<String>,
}

/// The tracked table set at start (channel A): the configured include/exclude globs resolved
/// against the live catalog, each table checked (config §11.2, the rows that halt a table).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResolvedTables {
    /// Tables to replicate, with their baseline schemas, in table order.
    pub tables: Vec<TableSchema>,
    /// Tables named without a glob that can't be replicated: each halts at start.
    pub halted: Vec<(TableRef, HaltReason)>,
    /// Tables a glob matched that can't be replicated (no identifier, a partitioned parent): each
    /// is left out with a warning.
    pub skipped: Vec<(TableRef, String)>,
    /// Pipeline-level warnings found while resolving (V-079 unconstrained numerics, unknown types
    /// read as text).
    pub warnings: Vec<String>,
}

/// Why streaming starts with a snapshot of every table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotReason {
    /// No pipeline state and no stream position at the source: the first start.
    FirstStart,
    /// The pipeline has state, but the source lost its stream position (PostgreSQL: the slot is
    /// gone or invalidated). Warn: data was at risk of a retention gap (D-15.3).
    PositionLost,
    /// No pipeline state, but the source kept a stream position: `on_orphan: adopt_resnapshot`
    /// took it over (D-15.1).
    AdoptedOrphan,
}

/// How streaming starts (Postgres_Slot… §1, the slot state machine).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartPlan {
    /// Stream from the pipeline's checkpoint; no snapshot.
    Resume,
    /// Snapshot every table, then stream from `from`, the source's new consistent position.
    Snapshot {
        from: SourcePosition,
        reason: SnapshotReason,
    },
}

/// What [`SourceConnector::prepare`] decided, and what it noticed on the way (publication
/// membership drift under `slot_management: require_existing`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepared {
    pub plan: StartPlan,
    pub warnings: Vec<String>,
}

/// A keyset cursor (Snapshot_Resumability §2): the identifier values of the last row read, in
/// identifier order, and the collation the text columns were ordered by.
#[derive(Debug, Clone, PartialEq)]
pub struct KeysetCursor {
    pub key: Row,
    /// `"C"` for PostgreSQL when an identifier column is text; `None` otherwise.
    pub collation: Option<String>,
}

/// One snapshot chunk: up to `limit` rows in identifier order after the cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub rows: Vec<Row>,
    /// The cursor after the last row; `None` when the chunk is empty (the table is done).
    pub next: Option<KeysetCursor>,
}

/// What an in-band schema signal carries (`Schema_Change…` §3 channel B). It says when and where;
/// [`SourceConnector::augment`] (channel C) says what.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaHint {
    /// The columns the stream now carries for the table, in order, as the source decodes them
    /// (PostgreSQL Relation message). `nullable` is unknown in-band and set to `true`;
    /// `source_ordinal` is `None`.
    Columns(Vec<ColumnSchema>),
    /// A DDL statement on the table (MySQL `QUERY_EVENT`).
    Ddl(String),
}

/// One item of the change stream, in source order. A transaction is `Begin`, its `Change`,
/// `Truncate`, `SchemaSignal` and `TableHalt` items, then `Commit`.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamItem {
    /// A transaction starts. PostgreSQL knows the commit position only at `Commit`.
    Begin {
        xid: u64,
        commit_ts_micros: i64,
    },
    Change(CdcEvent),
    Truncate {
        tables: Vec<TableRef>,
        seq: SourceSeq,
    },
    /// The table's shape changed at `seq`; the changes after it carry the new shape.
    SchemaSignal {
        table: TableRef,
        seq: SourceSeq,
        hint: SchemaHint,
    },
    /// A change of the table at `seq` can't be decoded (a `NaN` in a decimal column, a value
    /// beyond its type): the table halts, the stream continues.
    TableHalt {
        table: TableRef,
        seq: SourceSeq,
        reason: HaltReason,
    },
    Commit(TransactionBoundary),
    /// The source's position while no transaction is open (PostgreSQL keep-alive).
    KeepAlive(SourcePosition),
}

/// The tables channel D found changed since `known` (`Schema_Change…` §3).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RescanReport {
    /// Tables gone from the source catalog: each halts with `TABLE_DROPPED`.
    pub dropped: Vec<TableRef>,
    /// Tables whose catalog schema differs from the known one: the current schemas. Freshness and
    /// alerting only; the stream's signals apply changes in order.
    pub changed: Vec<TableSchema>,
    /// Tables that can't be described any more (identifier lost, …).
    pub halted: Vec<(TableRef, HaltReason)>,
}

/// A source engine (roadmap §3.6). Every method is a short call on a regular connection except
/// [`Self::open_stream`].
#[async_trait]
pub trait SourceConnector: Send + Sync {
    /// The pipeline-level start checks (config §11.2).
    async fn preflight(&self) -> Result<Preflight, SourceError>;

    /// Channel A: the configured table set, resolved and checked.
    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError>;

    /// Prepares the stream position for a start (PostgreSQL: the publication, then the slot state
    /// machine). `checkpoint` is the pipeline's durable position, `None` without state; `tables`
    /// is the tracked set.
    async fn prepare(
        &self,
        checkpoint: Option<&SourcePosition>,
        tables: &[TableRef],
    ) -> Result<Prepared, SourceError>;

    /// Replaces a lost stream position after [`SourceError::PositionUnreachable`] (D-15.3): the
    /// new consistent position to snapshot from and stream after.
    async fn reset_position(&self) -> Result<SourcePosition, SourceError>;

    /// How many bytes of log the source keeps for the pipeline beyond its acknowledged position
    /// (PostgreSQL: WAL the slot retains, the `max_wal_lag_bytes` guard). `None` when the source
    /// keeps nothing for it.
    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError>;

    /// The source's current position: a snapshot chunk's low and high watermarks.
    async fn current_position(&self) -> Result<SourcePosition, SourceError>;

    /// Up to `limit` rows of `table` after `after`, in identifier order.
    async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError>;

    /// The current rows with these identifier values (`toast_strategy: re_query`). A key with no
    /// row is left out.
    async fn fetch_rows(&self, table: &TableSchema, keys: &[Row]) -> Result<Vec<Row>, SourceError>;

    /// Opens the change stream after `from`, for `tables` as of that position.
    async fn open_stream(
        &self,
        from: &SourcePosition,
        tables: &[TableSchema],
    ) -> Result<Box<dyn ChangeStream>, SourceError>;

    /// Channel C: the change a schema signal implicates, against the `cached` as-of-position
    /// schema, scoped to the hint (`Schema_Change…` §0). Returns the delta and the new schema
    /// (`None` when the table was dropped).
    async fn augment(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError>;

    /// Channel D: one batched catalog read of the tracked tables.
    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError>;

    /// Removes what the source created for the pipeline (PostgreSQL `auto_cleanup`: the slot and
    /// the publication). Called when a pipeline is deleted.
    async fn teardown(&self) -> Result<(), SourceError>;
}

/// The change stream of one [`SourceConnector::open_stream`] call.
#[async_trait]
pub trait ChangeStream: Send {
    /// The next item. An error ends the stream; reopen from the checkpoint. Cancel-safe: the
    /// event loop runs it in `tokio::select!` (Data-Flow Phase 2a), and a call dropped before it
    /// returns loses nothing.
    async fn next(&mut self) -> Result<StreamItem, SourceError>;

    /// Tells the source that everything up to `durable` is committed to the sink, so it can free
    /// what it keeps for it (PostgreSQL: the slot's `confirmed_flush_lsn`). MySQL: nothing.
    async fn acknowledge(&mut self, durable: &SourcePosition) -> Result<(), SourceError>;

    /// Ends the stream cleanly.
    async fn close(self: Box<Self>) -> Result<(), SourceError>;
}

/// The identifier values of `row`, in `schema`'s identifier order: the key a cursor or
/// [`SourceConnector::fetch_rows`] carries. `None` when a value is missing or NULL.
pub fn identifier_values(schema: &TableSchema, row: &Row) -> Option<Row> {
    let mut key = Vec::with_capacity(schema.identifier().len());
    for column in schema.identifier() {
        match row.get(column) {
            None | Some(ColumnValue::Null | ColumnValue::Unchanged) => return None,
            Some(v) => key.push((column.clone(), v.clone())),
        }
    }
    Some(Row(key))
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core --lib source::`

Expected: PASS. `data-avalanche-core` `source` tests: 4.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core/src/lib.rs \
  crates/data-avalanche-core/src/source.rs
git commit -m "feat(core): the source connector contract"
```

---

### Task 2: The crate, error classes, TLS and the catalog connection

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/data-avalanche-source-pg/Cargo.toml`
- Create: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/test_support.rs`
- Create: `crates/data-avalanche-source-pg/src/error.rs`
- Create: `crates/data-avalanche-source-pg/src/tls.rs`
- Create: `crates/data-avalanche-source-pg/src/connect.rs`

**Interfaces:**
- Consumes: `Pipeline`, `Source`, `SslMode`, `Pem`, `Origin`, `load_str` (config); `SourceError` (Task 1).
- Produces:
  - `error`: `sqlstate(&str) -> Option<&str>` (the code in a pgwire `"msg (SQLSTATE xxxxx)"`),
    `replication_error(PgWireError, Option<String>) -> SourceError`, `sql_error(tokio_postgres::Error)
    -> SourceError`, `timed_out(&str, Duration) -> SourceError`, `protocol(impl Display) -> SourceError`.
  - `tls`: `PemFiles::new(&Source) -> Result<PemFiles, String>` (fields `ca`, `cert`, `key`:
    `Option<PathBuf>`), `replication_tls(SslMode, &PemFiles) -> TlsConfig`,
    `control_ssl_mode(SslMode)`, `control_tls(&Source) -> Result<rustls::ClientConfig, String>`.
  - `connect`: `APPLICATION_NAME`, `SESSION_OPTIONS` (crate root); `Connector::new(&Source)`,
    `connect()`, `client() -> Arc<Client>` (cached, reopened when closed), `command(what, fut)`
    (bounded by `timeouts.command`), `replication(slot, publication, Lsn, heartbeat) ->
    ReplicationConfig`; `within(what, Duration, fut)`.
  - `test_support` (unit tests only): `pipeline(include)`, `pipeline_with(include, tables_yaml)`.

D-M5.5, D-M5.6, D-M5.16, Q-M5-4. Two connections reach the server. The catalog connection
(`tokio-postgres`, rustls from the PEM text) and the replication connection (`pgwire-replication`,
which takes file paths). An inline PEM is written to a 0600 file in a 0700 directory, removed when
the connector is dropped. A path PEM is used where it is. Both connections pin the session
settings that decide how values print. `connect` and `tls` have no caller outside tests until
`PgConnector` (Task 7), and `error`'s stream half none until Task 9, so `lib.rs` marks those
modules `#[allow(dead_code)]` until then.

The unit tests make throwaway certificates with `rcgen` at run time (Q-M1-6). No key material is
committed.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -24,6 +24,7 @@
 data-avalanche-state = { path = "crates/data-avalanche-state" }
 data-avalanche-pipeline = { path = "crates/data-avalanche-pipeline" }
 data-avalanche-sink-delta = { path = "crates/data-avalanche-sink-delta" }
+data-avalanche-source-pg = { path = "crates/data-avalanche-source-pg" }
 thiserror = "2"
 serde = { version = "1", features = ["derive"] }
 serde_json = "1"
@@ -58,6 +59,11 @@
 deltalake = { version = "=1.0.0", default-features = false, features = ["datafusion", "rustls", "s3", "gcs", "azure"] }
 deltalake-core = { version = "=1.0.0", default-features = false }
 url = "2"
+# The PostgreSQL source (roadmap §2.1): the replication transport the S-PG probes ran on (the
+# pgoutput decoder is ours), and rustls for the catalog connection.
+pgwire-replication = { version = "=0.4.1", default-features = false, features = ["tls-rustls", "scram", "md5"] }
+tokio-postgres-rustls = "0.14"
+webpki-roots = "1"
 
 # Dependencies build without debug info: DataFusion's full debug build is over 6 GB, which fills
 # CI runners. Workspace crates keep it.
```

Create `crates/data-avalanche-source-pg/Cargo.toml`:

```toml
[package]
name = "data-avalanche-source-pg"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "The PostgreSQL source: pgoutput over logical replication, catalog channels, keyset snapshots."

[lints]
workspace = true

[dependencies]
async-trait.workspace = true
bytes.workspace = true
data-avalanche-config.workspace = true
data-avalanche-core.workspace = true
pgwire-replication.workspace = true
rustls.workspace = true
serde_json.workspace = true
tempfile.workspace = true
tokio = { workspace = true, features = ["sync"] }
tokio-postgres.workspace = true
tokio-postgres-rustls.workspace = true
uuid.workspace = true
webpki-roots.workspace = true

[dev-dependencies]
data-avalanche-testkit.workspace = true
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
proptest.workspace = true
rcgen.workspace = true
```

Create `crates/data-avalanche-source-pg/src/lib.rs`:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod connect;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod error;
#[cfg(test)]
mod test_support;
#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod tls;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
```

Create `crates/data-avalanche-source-pg/src/test_support.rs`:

```rust
//! Fixtures for the crate's unit tests: a resolved pipeline for the compose `postgres-18`
//! service.

use std::collections::BTreeMap;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};

/// A pipeline reading `include` from the `postgres-18` service, with `tables` (YAML, the rest of
/// the `tables` block) added.
pub(crate) fn pipeline_with(include: &str, tables: &str) -> Pipeline {
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
      postgres: {{ publication: p_pub, slot: p_slot }}
    tables:
      include: [{include}]
{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#
    );
    let loaded = load_str(
        &yaml,
        &BTreeMap::new(),
        &FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"));
    loaded.config.pipelines.into_values().next().unwrap()
}

pub(crate) fn pipeline(include: &str) -> Pipeline {
    pipeline_with(include, "")
}
```

Create `crates/data-avalanche-source-pg/src/error.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn server(m: &str) -> PgWireError {
        PgWireError::Server(m.into())
    }

    #[test]
    fn sqlstate_comes_from_the_message_tail() {
        assert_eq!(sqlstate("boom (SQLSTATE 55006)"), Some("55006"));
        assert_eq!(sqlstate("no state"), None);
        assert_eq!(sqlstate("cut (SQLSTATE 55"), None);
    }

    #[test]
    fn a_busy_slot_is_fatal() {
        let e = replication_error(
            server("replication slot \"s\" is active for PID 42 (SQLSTATE 55006)"),
            None,
        );
        assert!(matches!(e, SourceError::Permanent(m) if m.contains("active for PID")));
    }

    #[test]
    fn removed_wal_and_invalidated_slots_are_unreachable_positions() {
        let removed = replication_error(
            server(
                "requested WAL segment 000000010000000000000003 has already been removed (SQLSTATE 58P01)",
            ),
            Some("0/3000000".into()),
        );
        assert_eq!(
            removed,
            SourceError::PositionUnreachable {
                position: "0/3000000".into(),
                detail: "requested WAL segment 000000010000000000000003 has already been removed (SQLSTATE 58P01)".into(),
            }
        );
        let invalidated = replication_error(
            server("can no longer get changes from replication slot \"s\" (SQLSTATE 55000)"),
            Some("0/10".into()),
        );
        assert!(matches!(
            invalidated,
            SourceError::PositionUnreachable { .. }
        ));
        // The same SQLSTATE for another reason isn't a lost position.
        assert!(matches!(
            replication_error(
                server("logical decoding requires wal_level >= logical (SQLSTATE 55000)"),
                None
            ),
            SourceError::Permanent(_)
        ));
    }

    #[test]
    fn shutdowns_and_io_are_transient() {
        for m in [
            "terminating connection due to administrator command (SQLSTATE 57P01)",
            "the database system is starting up (SQLSTATE 57P03)",
            "sorry, too many clients already (SQLSTATE 53300)",
            "connection failure (SQLSTATE 08006)",
        ] {
            assert!(replication_error(server(m), None).is_transient(), "{m}");
        }
        let io = PgWireError::Io(Arc::new(std::io::Error::other("reset")));
        assert!(replication_error(io, None).is_transient());
    }

    #[test]
    fn auth_tls_protocol_and_privileges_are_permanent() {
        for e in [
            PgWireError::Auth("password authentication failed".into()),
            PgWireError::Tls("handshake failed".into()),
            PgWireError::Protocol("bad frame".into()),
            server("permission denied to start WAL sender (SQLSTATE 42501)"),
        ] {
            assert!(matches!(
                replication_error(e, None),
                SourceError::Permanent(_)
            ));
        }
    }

    #[test]
    fn a_tls_error_wrapped_in_io_is_found() {
        let io = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
        );
        assert!(tls_failure(&io));
        assert!(!tls_failure(&std::io::Error::other("reset")));
    }
}
```

Create `crates/data-avalanche-source-pg/src/tls.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use data_avalanche_config::{FingerprintKey, Secret};

    fn key() -> FingerprintKey {
        FingerprintKey::from_bytes([7; 32])
    }

    fn inline(text: &str) -> Pem {
        Pem {
            origin: Origin::Inline,
            text: Secret::new(text, &key()),
        }
    }

    /// A throwaway CA and a client certificate it signed, generated for the test run.
    fn material() -> (String, String, String) {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
        let ca_key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(vec![]).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = ca.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca, ca_key);
        let client_key = KeyPair::generate().unwrap();
        let client = CertificateParams::new(vec!["avalanche".into()])
            .unwrap()
            .signed_by(&client_key, &issuer)
            .unwrap();
        (ca_cert.pem(), client.pem(), client_key.serialize_pem())
    }

    fn source(yaml_mode: SslMode, ca: Option<Pem>, cert: Option<Pem>, key: Option<Pem>) -> Source {
        let mut s = crate::test_support::pipeline("public.t").source;
        s.ssl_mode = yaml_mode;
        s.ssl_ca = ca;
        s.ssl_cert = cert;
        s.ssl_key = key;
        s
    }

    #[cfg(unix)]
    #[test]
    fn inline_pems_are_private_files_removed_on_drop() {
        use std::os::unix::fs::PermissionsExt;
        let (ca, cert, client_key) = material();
        let s = source(
            SslMode::VerifyFull,
            Some(inline(&ca)),
            Some(inline(&cert)),
            Some(inline(&client_key)),
        );
        let files = PemFiles::new(&s).unwrap();
        let dir = files.dir().unwrap().to_owned();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        for p in [&files.ca, &files.cert, &files.key] {
            let p = p.as_ref().unwrap();
            assert!(p.starts_with(&dir));
            assert_eq!(mode(p), 0o600);
        }
        assert_eq!(
            std::fs::read_to_string(files.key.as_ref().unwrap()).unwrap(),
            client_key
        );
        drop(files);
        assert!(!dir.exists());
    }

    #[test]
    fn file_pems_are_used_where_they_are() {
        let mut ca = inline("unused");
        ca.origin = Origin::File(PathBuf::from("/etc/ssl/ca.pem"));
        let files = PemFiles::new(&source(SslMode::VerifyCa, Some(ca), None, None)).unwrap();
        assert_eq!(files.ca, Some(PathBuf::from("/etc/ssl/ca.pem")));
        assert!(
            files.dir().is_none(),
            "no temporary directory without inline material"
        );
    }

    #[test]
    fn modes_map_to_both_clients() {
        use pgwire_replication::SslMode as R;
        use tokio_postgres::config::SslMode as T;
        let files = PemFiles {
            ca: Some(PathBuf::from("/ca.pem")),
            ..PemFiles::default()
        };
        for (mode, repl, control) in [
            (SslMode::Disable, R::Disable, T::Disable),
            (SslMode::Allow, R::Prefer, T::Prefer),
            (SslMode::Prefer, R::Prefer, T::Prefer),
            (SslMode::Require, R::Require, T::Require),
            (SslMode::VerifyCa, R::VerifyCa, T::Require),
            (SslMode::VerifyFull, R::VerifyFull, T::Require),
        ] {
            let tls = replication_tls(mode, &files);
            assert_eq!(tls.mode, repl, "{mode}");
            assert_eq!(control_ssl_mode(mode), control, "{mode}");
        }
        assert_eq!(
            replication_tls(SslMode::VerifyFull, &files).ca_pem_path,
            Some(PathBuf::from("/ca.pem"))
        );
    }

    #[test]
    fn the_control_config_takes_the_inline_material() {
        let (ca, cert, client_key) = material();
        for mode in [SslMode::Require, SslMode::VerifyCa, SslMode::VerifyFull] {
            let s = source(
                mode,
                Some(inline(&ca)),
                Some(inline(&cert)),
                Some(inline(&client_key)),
            );
            let config = control_tls(&s).unwrap();
            assert!(config.client_auth_cert_resolver.has_certs());
        }
        let no_roots = source(SslMode::VerifyFull, None, None, None);
        control_tls(&no_roots).unwrap();
    }

    #[test]
    fn bad_material_is_refused_by_name() {
        let (ca, cert, _) = material();
        let e = control_tls(&source(
            SslMode::VerifyFull,
            Some(inline("not a pem")),
            None,
            None,
        ))
        .unwrap_err();
        assert!(e.starts_with("ssl_ca:"), "{e}");
        let e = control_tls(&source(
            SslMode::Require,
            Some(inline(&ca)),
            Some(inline(&cert)),
            None,
        ))
        .unwrap_err();
        assert_eq!(e, "ssl_cert and ssl_key go together");
        let e = control_tls(&source(
            SslMode::Require,
            None,
            Some(inline(&cert)),
            Some(inline("junk")),
        ))
        .unwrap_err();
        assert!(e.starts_with("ssl_key:"), "{e}");
    }
}
```

Create `crates/data-avalanche-source-pg/src/connect.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::pipeline;

    #[test]
    fn the_catalog_connection_pins_the_session() {
        let c = Connector::new(&pipeline("public.t").source).unwrap();
        let config = c.control_config();
        assert_eq!(config.get_options(), Some(SESSION_OPTIONS));
        assert_eq!(config.get_application_name(), Some(APPLICATION_NAME));
        assert_eq!(config.get_dbname(), Some("avalanche"));
        assert_eq!(config.get_user(), Some("avalanche"));
        assert_eq!(config.get_ports(), &[55418]);
        assert_eq!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Disable
        );
    }

    #[test]
    fn the_replication_connection_pins_the_same_session() {
        let c = Connector::new(&pipeline("public.t").source).unwrap();
        let r = c.replication("s", "p", Lsn(0x10), Duration::from_secs(7));
        assert_eq!(r.options.as_deref(), Some(SESSION_OPTIONS));
        assert_eq!(r.slot, "s");
        assert_eq!(r.publication.names(), ["p"]);
        assert_eq!(r.start_lsn, Lsn(0x10));
        assert_eq!(r.status_interval, Duration::from_secs(7));
        assert_eq!(r.idle_wakeup_interval, Duration::from_secs(7));
        assert!(!r.binary, "text tuples only (Q-M5-3)");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `connect::APPLICATION_NAME`, `connect::SESSION_OPTIONS`
error[E0425]: cannot find value `SESSION_OPTIONS` in this scope
error[E0425]: cannot find value `APPLICATION_NAME` in this scope
error[E0433]: cannot find type `Duration` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-source-pg/src/error.rs`, above the test module:

```rust
//! Error classes (Data-Flow §Error Paths): which server and client failures are worth a retry.

use data_avalanche_core::SourceError;
use pgwire_replication::PgWireError;

/// SQLSTATE classes and codes a reconnect can outlive: connection exceptions (`08`), insufficient
/// resources (`53`), operator intervention (`57P01`–`57P05`, a statement timeout `57014`),
/// serialization failures and deadlocks (`40001`, `40P01`), a lock not available (`55P03`).
fn transient_state(code: &str) -> bool {
    code.starts_with("08")
        || code.starts_with("53")
        || code.starts_with("57P")
        || matches!(code, "57014" | "40001" | "40P01" | "55P03")
}

/// A server error on the replication connection, as `pgwire-replication` reports it:
/// `"<message> (SQLSTATE <code>)"`.
pub(crate) fn sqlstate(message: &str) -> Option<&str> {
    let start = message.rfind("(SQLSTATE ")? + "(SQLSTATE ".len();
    message.get(start..start + 5)
}

/// The class of a server error. `position` names the stream position a removed-WAL error is about.
fn classify(code: Option<&str>, message: String, position: Option<String>) -> SourceError {
    match code {
        // `requested WAL segment … has already been removed` (58P01); a slot invalidated by
        // `max_slot_wal_keep_size` or an idle timeout (55000, "can no longer get changes").
        Some("58P01") if message.contains("WAL") => SourceError::PositionUnreachable {
            position: position.unwrap_or_default(),
            detail: message,
        },
        Some("55000") if message.contains("can no longer") => SourceError::PositionUnreachable {
            position: position.unwrap_or_default(),
            detail: message,
        },
        // `replication slot "…" is active for PID …` (55006): another holder exists. Fatal, never
        // retried into a busy loop (Postgres_Slot… D-15.5).
        Some("55006") => SourceError::Permanent(message),
        Some(c) if transient_state(c) => SourceError::Transient(message),
        _ => SourceError::Permanent(message),
    }
}

/// A replication-connection error. I/O and worker failures are transient; authentication, TLS and
/// protocol failures are not.
pub(crate) fn replication_error(e: PgWireError, position: Option<String>) -> SourceError {
    match e {
        PgWireError::Io(_) | PgWireError::Task(_) => SourceError::Transient(e.to_string()),
        PgWireError::Server(m) => classify(sqlstate(&m).map(str::to_owned).as_deref(), m, position),
        other => SourceError::Permanent(other.to_string()),
    }
}

/// True when a TLS failure is somewhere in the error's chain: a certificate the client refuses
/// won't be accepted on a retry.
fn tls_failure(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur = Some(e);
    while let Some(err) = cur {
        if err.is::<rustls::Error>() {
            return true;
        }
        if let Some(io) = err.downcast_ref::<std::io::Error>()
            && io
                .get_ref()
                .is_some_and(|inner| inner.is::<rustls::Error>())
        {
            return true;
        }
        cur = err.source();
    }
    false
}

/// The error and its sources, `: `-separated: `tokio-postgres` keeps the cause (an unknown
/// issuer, a refused connection) out of its own message.
fn chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut cur = e.source();
    while let Some(err) = cur {
        let text = err.to_string();
        if !out.ends_with(&text) {
            out = format!("{out}: {text}");
        }
        cur = err.source();
    }
    out
}

/// A catalog-connection error (`tokio-postgres`).
pub(crate) fn sql_error(e: tokio_postgres::Error) -> SourceError {
    if let Some(db) = e.as_db_error() {
        let message = format!("{} (SQLSTATE {})", db.message(), db.code().code());
        return classify(Some(db.code().code()), message, None);
    }
    if tls_failure(&e) {
        return SourceError::Permanent(format!("TLS: {}", chain(&e)));
    }
    // Closed connections, I/O and connect timeouts.
    SourceError::Transient(chain(&e))
}

/// A client-side timeout of a catalog or snapshot query.
pub(crate) fn timed_out(what: &str, after: std::time::Duration) -> SourceError {
    SourceError::Transient(format!("{what} timed out after {after:?}"))
}

/// A protocol message the decoder can't read: the stream can't continue past it.
pub(crate) fn protocol(detail: impl std::fmt::Display) -> SourceError {
    SourceError::Permanent(format!("pgoutput protocol error: {detail}"))
}
```

Add the implementation to `crates/data-avalanche-source-pg/src/tls.rs`, above the test module:

```rust
//! TLS for both connections (config §4.4, `source.ssl_*`). The catalog connection builds rustls
//! from the PEM text; the replication connection (`pgwire-replication`) takes file paths, so an
//! inline PEM is written to a private temporary file (Q-M5-4).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use data_avalanche_config::{Origin, Pem, Source, SslMode};
use pgwire_replication::TlsConfig;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};

/// PEM files for the replication connection. A file-origin PEM is used where it is; an inline PEM
/// is written to a file of mode 0600 in a directory of mode 0700, both removed when this is
/// dropped.
#[derive(Debug, Default)]
pub(crate) struct PemFiles {
    dir: Option<tempfile::TempDir>,
    pub(crate) ca: Option<PathBuf>,
    pub(crate) cert: Option<PathBuf>,
    pub(crate) key: Option<PathBuf>,
}

impl PemFiles {
    pub(crate) fn new(source: &Source) -> Result<Self, String> {
        let mut files = Self::default();
        files.ca = files.place(source.ssl_ca.as_ref(), "ca.pem")?;
        files.cert = files.place(source.ssl_cert.as_ref(), "cert.pem")?;
        files.key = files.place(source.ssl_key.as_ref(), "key.pem")?;
        Ok(files)
    }

    fn place(&mut self, pem: Option<&Pem>, name: &str) -> Result<Option<PathBuf>, String> {
        let Some(pem) = pem else { return Ok(None) };
        if let Origin::File(path) = &pem.origin {
            return Ok(Some(path.clone()));
        }
        if self.dir.is_none() {
            let mut builder = tempfile::Builder::new();
            builder.prefix("data-avalanche-pg-tls-");
            #[cfg(unix)]
            builder.permissions(std::os::unix::fs::PermissionsExt::from_mode(0o700));
            let dir = builder
                .tempdir()
                .map_err(|e| format!("can't create a directory for the TLS material: {e}"))?;
            self.dir = Some(dir);
        }
        let path = self.dir.as_ref().expect("created above").path().join(name);
        write_private(&path, pem.text.expose().as_bytes())
            .map_err(|e| format!("can't write the TLS material: {e}"))?;
        Ok(Some(path))
    }

    #[cfg(test)]
    pub(crate) fn dir(&self) -> Option<&Path> {
        self.dir.as_ref().map(|d| d.path())
    }
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)?.write_all(bytes)
}

/// The replication connection's TLS (`ssl_mode: allow` connects like `prefer`: the crate has no
/// `allow`, and both accept either kind of connection — D-M5.4).
pub(crate) fn replication_tls(mode: SslMode, files: &PemFiles) -> TlsConfig {
    let mut tls = match mode {
        SslMode::Disable => TlsConfig::disabled(),
        SslMode::Allow | SslMode::Prefer => TlsConfig {
            mode: pgwire_replication::SslMode::Prefer,
            ..TlsConfig::disabled()
        },
        SslMode::Require => TlsConfig::require(),
        SslMode::VerifyCa => TlsConfig::verify_ca(files.ca.clone()),
        SslMode::VerifyFull => TlsConfig::verify_full(files.ca.clone()),
    };
    if let (Some(cert), Some(key)) = (&files.cert, &files.key) {
        tls = tls.with_client_cert(cert.clone(), key.clone());
    }
    tls
}

/// The catalog connection's `sslmode` (tokio-postgres knows disable, prefer and require; the
/// verify modes are require plus a verifying rustls configuration).
pub(crate) fn control_ssl_mode(mode: SslMode) -> tokio_postgres::config::SslMode {
    use tokio_postgres::config::SslMode as T;
    match mode {
        SslMode::Disable => T::Disable,
        SslMode::Allow | SslMode::Prefer => T::Prefer,
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => T::Require,
    }
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

fn certificates(pem: &Pem, what: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_slice_iter(pem.text.expose().as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{what}: not a PEM certificate: {e}"))?;
    if certs.is_empty() {
        return Err(format!("{what}: no certificate in the PEM"));
    }
    Ok(certs)
}

/// The catalog connection's rustls configuration: the same modes as the replication connection
/// (Disable / Prefer / Require: encrypt without verifying; VerifyCa: the chain; VerifyFull: the
/// chain and the host name). Roots are `ssl_ca`, else the Mozilla roots the replication crate uses.
pub(crate) fn control_tls(source: &Source) -> Result<ClientConfig, String> {
    let provider = provider();
    let mut roots = RootCertStore::empty();
    match &source.ssl_ca {
        Some(ca) => {
            let (added, _) = roots.add_parsable_certificates(certificates(ca, "ssl_ca")?);
            if added == 0 {
                return Err("ssl_ca: no usable CA certificate".into());
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let roots = Arc::new(roots);
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots.clone());
    let mut config = match (&source.ssl_cert, &source.ssl_key) {
        (Some(cert), Some(key)) => {
            let key = PrivateKeyDer::from_pem_slice(key.text.expose().as_bytes())
                .map_err(|e| format!("ssl_key: not a PEM private key: {e}"))?;
            builder
                .with_client_auth_cert(certificates(cert, "ssl_cert")?, key)
                .map_err(|e| format!("ssl_cert/ssl_key: {e}"))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => return Err("ssl_cert and ssl_key go together".into()),
    };
    let algorithms = provider.signature_verification_algorithms;
    match source.ssl_mode {
        SslMode::VerifyFull => {}
        SslMode::VerifyCa => {
            let inner = WebPkiServerVerifier::builder_with_provider(roots, provider)
                .build()
                .map_err(|e| e.to_string())?;
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(ChainOnly { inner }));
        }
        _ => {
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(Unverified { algorithms }));
        }
    }
    Ok(config)
}

/// `require`, `prefer`, `allow`: any certificate, as libpq does without a root certificate. The
/// handshake signatures are still checked.
#[derive(Debug)]
struct Unverified {
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Unverified {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// `verify-ca`: the chain must lead to a trusted root; the host name isn't checked.
#[derive(Debug)]
struct ChainOnly {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
```

Add the implementation to `crates/data-avalanche-source-pg/src/connect.rs`, above the test module:

```rust
//! The two connections to the source: the catalog connection (`tokio-postgres`: catalog channels,
//! snapshot chunks, slot and publication DDL) and the replication connection
//! (`pgwire-replication`). Both run with the same pinned session settings, so the text the
//! decoder parses has one form (D-M5.5).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use data_avalanche_config::{Source, SslMode, Timeouts};
use data_avalanche_core::SourceError;
use pgwire_replication::{Lsn, ReplicationConfig};
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::error::{sql_error, timed_out};
use crate::tls::{PemFiles, control_ssl_mode, control_tls, replication_tls};

/// Session settings on both connections: UTC instants, ISO dates, ISO-8601 intervals, round-trip
/// floats, hex `bytea`, `money` without a locale's currency symbol or grouping, and string
/// literals without backslash escapes (snapshot keys travel as literals).
pub const SESSION_OPTIONS: &str = "-c TimeZone=UTC -c DateStyle=ISO -c IntervalStyle=iso_8601 \
     -c extra_float_digits=3 -c bytea_output=hex -c lc_monetary=C \
     -c standard_conforming_strings=on";

/// The `application_name` the catalog connections report (`pg_stat_activity`). The replication
/// connection reports `pgwire-replication`: the crate (0.4.1) hard-codes it in its startup packet.
pub const APPLICATION_NAME: &str = "data-avalanche";

/// What both connections need, built once from `source`. `Debug` leaves out the password.
pub(crate) struct Connector {
    host: String,
    port: u16,
    db: String,
    user: String,
    password: String,
    ssl_mode: SslMode,
    pub(crate) timeouts: Timeouts,
    files: PemFiles,
    tls: Option<MakeRustlsConnect>,
    client: Mutex<Option<Arc<Client>>>,
}

impl Connector {
    pub(crate) fn new(source: &Source) -> Result<Self, SourceError> {
        let files = PemFiles::new(source).map_err(SourceError::Permanent)?;
        let tls = match source.ssl_mode {
            SslMode::Disable => None,
            _ => Some(MakeRustlsConnect::new(
                control_tls(source).map_err(SourceError::Permanent)?,
            )),
        };
        Ok(Self {
            host: source.host.clone(),
            port: source.port,
            db: source.db.clone(),
            user: source.credentials.user().to_owned(),
            password: source.credentials.password().expose().to_owned(),
            ssl_mode: source.ssl_mode,
            timeouts: source.timeouts.clone(),
            files,
            tls,
            client: Mutex::new(None),
        })
    }

    fn control_config(&self) -> tokio_postgres::Config {
        let mut c = tokio_postgres::Config::new();
        c.host(&self.host)
            .port(self.port)
            .dbname(&self.db)
            .user(&self.user)
            .password(&self.password)
            .options(SESSION_OPTIONS)
            .application_name(APPLICATION_NAME)
            .connect_timeout(self.timeouts.connect)
            .ssl_mode(control_ssl_mode(self.ssl_mode));
        c
    }

    /// A new catalog connection, its driver task spawned.
    pub(crate) async fn connect(&self) -> Result<Client, SourceError> {
        let config = self.control_config();
        let client = match &self.tls {
            None => {
                let (client, conn) = config
                    .connect(tokio_postgres::NoTls)
                    .await
                    .map_err(sql_error)?;
                tokio::spawn(conn);
                client
            }
            Some(tls) => {
                let (client, conn) = config.connect(tls.clone()).await.map_err(sql_error)?;
                tokio::spawn(conn);
                client
            }
        };
        Ok(client)
    }

    /// The shared catalog connection, reconnected when it was closed.
    pub(crate) async fn client(&self) -> Result<Arc<Client>, SourceError> {
        let mut slot = self.client.lock().await;
        if let Some(c) = slot.as_ref().filter(|c| !c.is_closed()) {
            return Ok(c.clone());
        }
        let c = Arc::new(self.connect().await?);
        *slot = Some(c.clone());
        Ok(c)
    }

    /// Runs a catalog query under `timeouts.command`.
    pub(crate) async fn command<T>(
        &self,
        what: &str,
        f: impl Future<Output = Result<T, tokio_postgres::Error>>,
    ) -> Result<T, SourceError> {
        within(what, self.timeouts.command, f).await
    }

    /// The replication connection for `slot` and `publication`, starting after `start`. The status
    /// and wake-up intervals are the heartbeat (Data-Flow Phase 2a).
    pub(crate) fn replication(
        &self,
        slot: &str,
        publication: &str,
        start: Lsn,
        heartbeat: Duration,
    ) -> ReplicationConfig {
        ReplicationConfig::new(
            self.host.clone(),
            self.user.clone(),
            self.password.clone(),
            self.db.clone(),
            slot,
            publication,
        )
        .with_port(self.port)
        .with_tls(replication_tls(self.ssl_mode, &self.files))
        .with_start_lsn(start)
        .with_status_interval(heartbeat)
        .with_wakeup_interval(heartbeat)
        .with_options(SESSION_OPTIONS)
    }
}

impl std::fmt::Debug for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("db", &self.db)
            .field("user", &self.user)
            .field("ssl_mode", &self.ssl_mode)
            .finish_non_exhaustive()
    }
}

/// `f` under a client-side timeout; a timeout is transient.
pub(crate) async fn within<T>(
    what: &str,
    after: Duration,
    f: impl Future<Output = Result<T, tokio_postgres::Error>>,
) -> Result<T, SourceError> {
    match tokio::time::timeout(after, f).await {
        Ok(r) => r.map_err(sql_error),
        Err(_) => Err(timed_out(what, after)),
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 13.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml \
  Cargo.lock \
  crates/data-avalanche-source-pg/Cargo.toml \
  crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/test_support.rs \
  crates/data-avalanche-source-pg/src/error.rs \
  crates/data-avalanche-source-pg/src/tls.rs \
  crates/data-avalanche-source-pg/src/connect.rs
git commit -m "feat(source-pg): the crate, error classes, TLS and the catalog connection"
```

---

### Task 3: The pgoutput v1 decoder

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/pgoutput.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces (`pgoutput` is public): `Datum::{Null, Unchanged, Text(Bytes)}`, `RelationColumn { key,
  name, type_oid, typmod }`, `OldKind::{Key, Full}`, `Message::{Relation { oid, namespace, name,
  replica_identity, columns }, Type { oid, namespace, name }, Origin { lsn, name }, Insert { oid,
  new }, Update { oid, old: Option<(OldKind, Vec<Datum>)>, new }, Delete { oid, old }, Truncate {
  options, relations }}`, `decode(&Bytes) -> Result<Message, String>`.

S-PG1, S-PG2, Q-M5-3. `pgwire-replication` hands over each XLogData payload and decodes
Begin and Commit itself, so this decoder covers the other messages of protocol v1. Text datums are
zero-copy slices of the payload. A binary datum (`b`) is refused: the stream never asks for binary
tuples (Q-M5-3). Lengths and counts are checked against the bytes left, so a corrupt message is an
error, not a panic or an allocation of what a length claims. Property tests encode random messages
and decode them back, and feed truncated and random bytes.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-source-pg/src/lib.rs` with:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod connect;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod error;
pub mod pgoutput;
#[cfg(test)]
mod test_support;
#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod tls;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
```

Create `crates/data-avalanche-source-pg/src/pgoutput.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Builds messages byte by byte, as the server writes them.
    #[derive(Default)]
    struct W(Vec<u8>);

    impl W {
        fn u8(mut self, v: u8) -> Self {
            self.0.push(v);
            self
        }
        fn i16(mut self, v: i16) -> Self {
            self.0.extend(v.to_be_bytes());
            self
        }
        fn i32(mut self, v: i32) -> Self {
            self.0.extend(v.to_be_bytes());
            self
        }
        fn u64(mut self, v: u64) -> Self {
            self.0.extend(v.to_be_bytes());
            self
        }
        fn s(mut self, v: &str) -> Self {
            self.0.extend(v.as_bytes());
            self.0.push(0);
            self
        }
        fn text(self, v: &str) -> Self {
            let mut w = self.u8(b't').i32(v.len() as i32);
            w.0.extend(v.as_bytes());
            w
        }
        fn done(self) -> Bytes {
            Bytes::from(self.0)
        }
    }

    fn text(v: &str) -> Datum {
        Datum::Text(Bytes::copy_from_slice(v.as_bytes()))
    }

    #[test]
    fn relation() {
        let m = W::default()
            .u8(b'R')
            .i32(16384)
            .s("public")
            .s("orders")
            .u8(b'd')
            .i16(2)
            .u8(1)
            .s("id")
            .i32(20)
            .i32(-1)
            .u8(0)
            .s("note")
            .i32(1043)
            .i32(104)
            .done();
        assert_eq!(
            decode(&m).unwrap(),
            Message::Relation {
                oid: 16384,
                namespace: "public".into(),
                name: "orders".into(),
                replica_identity: b'd',
                columns: vec![
                    RelationColumn {
                        key: true,
                        name: "id".into(),
                        type_oid: 20,
                        typmod: -1
                    },
                    RelationColumn {
                        key: false,
                        name: "note".into(),
                        type_oid: 1043,
                        typmod: 104
                    },
                ],
            }
        );
    }

    #[test]
    fn type_and_origin() {
        let y = W::default()
            .u8(b'Y')
            .i32(70000)
            .s("public")
            .s("mood")
            .done();
        assert_eq!(
            decode(&y).unwrap(),
            Message::Type {
                oid: 70000,
                namespace: "public".into(),
                name: "mood".into()
            }
        );
        let o = W::default().u8(b'O').u64(0x1_0000_0010).s("node_a").done();
        assert_eq!(
            decode(&o).unwrap(),
            Message::Origin {
                commit_lsn: 0x1_0000_0010,
                name: "node_a".into()
            }
        );
    }

    #[test]
    fn insert_with_every_datum_kind() {
        let m = W::default()
            .u8(b'I')
            .i32(16384)
            .u8(b'N')
            .i16(4)
            .text("42")
            .u8(b'n')
            .u8(b'u')
            .text("")
            .done();
        assert_eq!(
            decode(&m).unwrap(),
            Message::Insert {
                relation: 16384,
                new: vec![text("42"), Datum::Null, Datum::Unchanged, text("")],
            }
        );
    }

    #[test]
    fn updates_with_and_without_an_old_tuple() {
        let plain = W::default()
            .u8(b'U')
            .i32(1)
            .u8(b'N')
            .i16(1)
            .text("a")
            .done();
        assert_eq!(
            decode(&plain).unwrap(),
            Message::Update {
                relation: 1,
                old: None,
                new: vec![text("a")]
            }
        );
        let key = W::default()
            .u8(b'U')
            .i32(1)
            .u8(b'K')
            .i16(2)
            .text("1")
            .u8(b'n')
            .u8(b'N')
            .i16(2)
            .text("2")
            .text("b")
            .done();
        assert_eq!(
            decode(&key).unwrap(),
            Message::Update {
                relation: 1,
                old: Some((OldKind::Key, vec![text("1"), Datum::Null])),
                new: vec![text("2"), text("b")],
            }
        );
        let full = W::default()
            .u8(b'U')
            .i32(1)
            .u8(b'O')
            .i16(1)
            .text("x")
            .u8(b'N')
            .i16(1)
            .text("y")
            .done();
        assert!(matches!(
            decode(&full).unwrap(),
            Message::Update {
                old: Some((OldKind::Full, _)),
                ..
            }
        ));
    }

    #[test]
    fn deletes_and_truncates() {
        let d = W::default()
            .u8(b'D')
            .i32(9)
            .u8(b'K')
            .i16(1)
            .text("7")
            .done();
        assert_eq!(
            decode(&d).unwrap(),
            Message::Delete {
                relation: 9,
                old: (OldKind::Key, vec![text("7")])
            }
        );
        let t = W::default().u8(b'T').i32(2).u8(3).i32(9).i32(10).done();
        assert_eq!(
            decode(&t).unwrap(),
            Message::Truncate {
                options: 3,
                relations: vec![9, 10]
            }
        );
    }

    #[test]
    fn malformed_messages_are_refused() {
        let cases = [
            (W::default().done(), "message tag"),
            (W::default().u8(b'Z').done(), "unexpected message tag 'Z'"),
            (W::default().u8(b'I').i32(1).u8(b'K').done(), "expected 'N'"),
            (
                W::default()
                    .u8(b'I')
                    .i32(1)
                    .u8(b'N')
                    .i16(1)
                    .u8(b'b')
                    .i32(1)
                    .u8(1)
                    .done(),
                "binary",
            ),
            (
                W::default()
                    .u8(b'I')
                    .i32(1)
                    .u8(b'N')
                    .i16(1)
                    .u8(b't')
                    .i32(5)
                    .done(),
                "text value: the message ends",
            ),
            (
                W::default().u8(b'I').i32(1).u8(b'N').i16(1).u8(b'x').done(),
                "unknown tuple column kind 'x'",
            ),
            (W::default().u8(b'Y').i32(1).u8(b'a').done(), "no NUL"),
            (
                W::default().u8(b'D').i32(1).u8(b'N').i16(0).done(),
                "expected 'K' or 'O'",
            ),
            (
                W::default().u8(b'T').i32(1).u8(0).i32(9).u8(0).done(),
                "1 bytes left over",
            ),
            (
                W::default()
                    .u8(b'I')
                    .i32(1)
                    .u8(b'N')
                    .i16(1)
                    .u8(b't')
                    .i32(-1)
                    .done(),
                "text length -1",
            ),
        ];
        for (m, expect) in cases {
            let e = decode(&m).unwrap_err();
            assert!(e.contains(expect), "{e:?} should mention {expect:?}");
        }
    }

    proptest::proptest! {
        /// Arbitrary bytes never panic the decoder.
        #[test]
        fn arbitrary_payloads_never_panic(data in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64)) {
            let _ = decode(&Bytes::from(data));
        }

        /// Counts near `i32::MAX` after a valid tag don't allocate for them.
        #[test]
        fn huge_counts_are_bounded_by_the_payload(tag in proptest::sample::select(vec![b'T', b'R', b'I'])) {
            let m = match tag {
                b'T' => W::default().u8(b'T').i32(i32::MAX).u8(0).done(),
                b'R' => W::default().u8(b'R').i32(1).s("s").s("t").u8(b'd').i16(i16::MAX).done(),
                _ => W::default().u8(b'I').i32(1).u8(b'N').i16(i16::MAX).done(),
            };
            proptest::prop_assert!(decode(&m).is_err());
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0425]: cannot find type `Bytes` in this scope
error[E0433]: cannot find type `Bytes` in this scope
error[E0425]: cannot find type `Datum` in this scope
error[E0422]: cannot find struct, variant or union type `RelationColumn` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-source-pg/src/pgoutput.rs`, above the test module:

```rust
//! The pgoutput protocol v1 decoder (PostgreSQL docs, "Logical Replication Message Formats"): the
//! part `pgwire-replication` leaves to the caller. The crate itself turns Begin (`B`), Commit (`C`)
//! and logical messages (`M`) into events; this decodes the rest of an `XLogData` payload. Tuples
//! are text (Q-M5-3): a binary datum is a protocol error.

use bytes::Bytes;

/// One column value of a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Datum {
    Null,
    /// An unchanged TOASTed value the server didn't send (`u`).
    Unchanged,
    /// The value's text form, in the database encoding.
    Text(Bytes),
}

/// A column of a Relation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    /// Part of the replica identity (flag bit 1).
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
}

/// Which old tuple an Update or Delete carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldKind {
    /// `K`: the replica identity columns; the other columns are NULL.
    Key,
    /// `O`: the whole old row (`REPLICA IDENTITY FULL`).
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Relation {
        oid: u32,
        /// Empty for `pg_catalog`.
        namespace: String,
        name: String,
        /// `d` default, `n` nothing, `f` full, `i` index.
        replica_identity: u8,
        columns: Vec<RelationColumn>,
    },
    Type {
        oid: u32,
        namespace: String,
        name: String,
    },
    Origin {
        commit_lsn: u64,
        name: String,
    },
    Insert {
        relation: u32,
        new: Vec<Datum>,
    },
    Update {
        relation: u32,
        old: Option<(OldKind, Vec<Datum>)>,
        new: Vec<Datum>,
    },
    Delete {
        relation: u32,
        old: (OldKind, Vec<Datum>),
    },
    Truncate {
        /// Bit 1 `CASCADE`, bit 2 `RESTART IDENTITY`.
        options: u8,
        relations: Vec<u32>,
    },
}

struct Reader<'a> {
    data: &'a Bytes,
    at: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize, what: &str) -> Result<&[u8], String> {
        let end = self
            .at
            .checked_add(n)
            .filter(|&e| e <= self.data.len())
            .ok_or_else(|| format!("{what}: the message ends at byte {}", self.data.len()))?;
        let s = &self.data[self.at..end];
        self.at = end;
        Ok(s)
    }

    fn u8(&mut self, what: &str) -> Result<u8, String> {
        Ok(self.take(1, what)?[0])
    }

    fn i16(&mut self, what: &str) -> Result<i16, String> {
        Ok(i16::from_be_bytes(
            self.take(2, what)?.try_into().expect("2 bytes"),
        ))
    }

    fn i32(&mut self, what: &str) -> Result<i32, String> {
        Ok(i32::from_be_bytes(
            self.take(4, what)?.try_into().expect("4 bytes"),
        ))
    }

    fn u32(&mut self, what: &str) -> Result<u32, String> {
        Ok(u32::from_be_bytes(
            self.take(4, what)?.try_into().expect("4 bytes"),
        ))
    }

    fn u64(&mut self, what: &str) -> Result<u64, String> {
        Ok(u64::from_be_bytes(
            self.take(8, what)?.try_into().expect("8 bytes"),
        ))
    }

    fn string(&mut self, what: &str) -> Result<String, String> {
        let rest = &self.data[self.at..];
        let end = rest
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| format!("{what}: no NUL terminator"))?;
        let s = std::str::from_utf8(&rest[..end])
            .map_err(|_| format!("{what}: not UTF-8"))?
            .to_owned();
        self.at += end + 1;
        Ok(s)
    }

    /// A capacity for `n` items of at least `size` bytes each: never more than the bytes left can
    /// hold, so a corrupt count can't allocate.
    fn capacity(&self, n: i64, size: usize) -> usize {
        let left = (self.data.len() - self.at) / size;
        usize::try_from(n).unwrap_or(0).min(left)
    }

    fn tuple(&mut self) -> Result<Vec<Datum>, String> {
        let n = self.i16("tuple column count")?;
        let mut out = Vec::with_capacity(self.capacity(n.into(), 1));
        for i in 0..n.max(0) {
            out.push(match self.u8("tuple column kind")? {
                b'n' => Datum::Null,
                b'u' => Datum::Unchanged,
                b't' => {
                    let len = self.i32("text length")?;
                    let len = usize::try_from(len).map_err(|_| format!("text length {len}"))?;
                    let start = self.at;
                    self.take(len, "text value")?;
                    Datum::Text(self.data.slice(start..start + len))
                }
                b'b' => return Err(format!("column {i} is binary; the stream asks for text")),
                k => return Err(format!("unknown tuple column kind {:?}", k as char)),
            });
        }
        Ok(out)
    }

    fn old_kind(k: u8) -> Option<OldKind> {
        match k {
            b'K' => Some(OldKind::Key),
            b'O' => Some(OldKind::Full),
            _ => None,
        }
    }
}

/// Decodes one `XLogData` payload. Every byte must belong to the message.
pub fn decode(data: &Bytes) -> Result<Message, String> {
    let mut r = Reader { data, at: 0 };
    let tag = r.u8("message tag")?;
    let msg = match tag {
        b'R' => {
            let oid = r.u32("relation oid")?;
            let namespace = r.string("namespace")?;
            let name = r.string("relation name")?;
            let replica_identity = r.u8("replica identity")?;
            let n = r.i16("column count")?;
            let mut columns = Vec::with_capacity(r.capacity(n.into(), 10));
            for _ in 0..n {
                let flags = r.u8("column flags")?;
                columns.push(RelationColumn {
                    key: flags & 1 == 1,
                    name: r.string("column name")?,
                    type_oid: r.u32("column type")?,
                    typmod: r.i32("column typmod")?,
                });
            }
            Message::Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            }
        }
        b'Y' => Message::Type {
            oid: r.u32("type oid")?,
            namespace: r.string("type namespace")?,
            name: r.string("type name")?,
        },
        b'O' => Message::Origin {
            commit_lsn: r.u64("origin commit LSN")?,
            name: r.string("origin name")?,
        },
        b'I' => {
            let relation = r.u32("relation oid")?;
            match r.u8("insert tuple kind")? {
                b'N' => {}
                k => return Err(format!("Insert: tuple kind {:?}, expected 'N'", k as char)),
            }
            Message::Insert {
                relation,
                new: r.tuple()?,
            }
        }
        b'U' => {
            let relation = r.u32("relation oid")?;
            let mut k = r.u8("update tuple kind")?;
            let old = match Reader::old_kind(k) {
                Some(kind) => {
                    let t = r.tuple()?;
                    k = r.u8("update tuple kind")?;
                    Some((kind, t))
                }
                None => None,
            };
            if k != b'N' {
                return Err(format!("Update: tuple kind {:?}, expected 'N'", k as char));
            }
            Message::Update {
                relation,
                old,
                new: r.tuple()?,
            }
        }
        b'D' => {
            let relation = r.u32("relation oid")?;
            let k = r.u8("delete tuple kind")?;
            let kind = Reader::old_kind(k).ok_or_else(|| {
                format!("Delete: tuple kind {:?}, expected 'K' or 'O'", k as char)
            })?;
            Message::Delete {
                relation,
                old: (kind, r.tuple()?),
            }
        }
        b'T' => {
            let n = r.i32("truncate relation count")?;
            let options = r.u8("truncate options")?;
            let mut relations = Vec::with_capacity(r.capacity(n.into(), 4));
            for _ in 0..n {
                relations.push(r.u32("truncated relation oid")?);
            }
            Message::Truncate { options, relations }
        }
        t => return Err(format!("unexpected message tag {:?}", t as char)),
    };
    if r.at != data.len() {
        return Err(format!(
            "{:?} message: {} bytes left over",
            tag as char,
            data.len() - r.at
        ));
    }
    Ok(msg)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 21.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/pgoutput.rs
git commit -m "feat(source-pg): the pgoutput v1 decoder"
```

---

### Task 4: Text values → `ColumnValue`

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/value.rs`

**Interfaces:**
- Consumes: `ColumnValue`, `ColumnValueType`, `Decimal` (core).
- Produces (`value` is public):
  - `Decode::{Bool, Int16, Int32, Int64, Float32, Float64, Numeric, Money, Text, BpChar, Char, Bytea,
    Date, Time, Timestamp { with_tz }, Uuid, Json, Bit, VarBit, Hstore, HexBytes, Array { element,
    element_type, delimiter }}`.
  - `ValueError::{Overflow, Unrepresentable(String)}`.
  - `decode(&Decode, &str) -> Result<ColumnValue, ValueError>`; `date`, `time`, `timestamp(text,
    with_tz)`; `text_of(&ColumnValue) -> Option<String>` (the inverse, for key values).

Type_Mapping Hop 1 for every mapped PostgreSQL type, from the text the session settings pin
(D-M5.5), with the fallbacks of D-M5.10. `±infinity` clamps per Q-M5-1. NaN and infinite numerics are `Unrepresentable` and
halt the table (D-M5.9). A numeric beyond 38 digits is `Overflow`. A varbit is its PostgreSQL
binary form (Q-M5-9). An hstore becomes a JSON object. Multi-dimensional arrays are
`Unrepresentable`. The date and timestamp round trips are property tests over PostgreSQL's whole
range: 4714-11-24 BC to 5874897-12-31 for dates.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-source-pg/src/lib.rs` with:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod connect;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod error;
pub mod pgoutput;
#[cfg(test)]
mod test_support;
#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod tls;
pub mod value;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
```

Create `crates/data-avalanche-source-pg/src/value.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use ColumnValue as V;

    fn ok(d: Decode, text: &str) -> ColumnValue {
        decode(&d, text).unwrap_or_else(|e| panic!("{text:?}: {e:?}"))
    }

    fn unrepresentable(d: Decode, text: &str) {
        assert!(
            matches!(decode(&d, text), Err(ValueError::Unrepresentable(_))),
            "{text:?} should be refused"
        );
    }

    #[test]
    fn scalars() {
        assert_eq!(ok(Decode::Bool, "t"), V::Bool(true));
        assert_eq!(ok(Decode::Bool, "f"), V::Bool(false));
        assert_eq!(ok(Decode::Int16, "-32768"), V::Int16(i16::MIN));
        assert_eq!(ok(Decode::Int32, "2147483647"), V::Int32(i32::MAX));
        assert_eq!(
            ok(Decode::Int64, "-9223372036854775808"),
            V::Int64(i64::MIN)
        );
        assert_eq!(ok(Decode::Float32, "1.5"), V::Float32(1.5));
        assert_eq!(
            ok(Decode::Float64, "-Infinity"),
            V::Float64(f64::NEG_INFINITY)
        );
        assert_eq!(ok(Decode::Float64, "Infinity"), V::Float64(f64::INFINITY));
        assert!(matches!(ok(Decode::Float64, "NaN"), V::Float64(x) if x.is_nan()));
        assert_eq!(ok(Decode::Float64, "0.1"), V::Float64(0.1));
        assert_eq!(ok(Decode::Text, " a "), V::Text(" a ".into()));
        assert_eq!(ok(Decode::BpChar, "ab   "), V::Text("ab".into()));
        assert_eq!(ok(Decode::Char, "a"), V::Int16(97));
        assert_eq!(ok(Decode::Char, "\\377"), V::Int16(255));
        assert_eq!(ok(Decode::Char, ""), V::Int16(0));
        assert_eq!(
            ok(Decode::Bytea, "\\xdeadBEEF"),
            V::Bytes(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(ok(Decode::Bytea, "\\x"), V::Bytes(vec![]));
        assert_eq!(
            ok(Decode::Uuid, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
            V::Uuid(uuid::Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap())
        );
        assert_eq!(
            ok(Decode::Json, r#"{"a": [1, 2.5, null]}"#),
            V::Json(serde_json::json!({"a": [1, 2.5, null]}))
        );
        for (d, t) in [
            (Decode::Bool, "true"),
            (Decode::Int16, "32768"),
            (Decode::Bytea, "deadbeef"),
            (Decode::Bytea, "\\xabc"),
            (Decode::Char, "ab"),
            (Decode::Uuid, "x"),
            (Decode::Json, "{"),
        ] {
            unrepresentable(d, t);
        }
    }

    #[test]
    fn numerics_and_money() {
        assert_eq!(
            ok(Decode::Numeric, "-12.340"),
            V::Numeric(Decimal::new(-12340, 3))
        );
        assert_eq!(
            ok(Decode::Numeric, "12300"),
            V::Numeric(Decimal::new(12300, 0))
        );
        unrepresentable(Decode::Numeric, "NaN");
        unrepresentable(Decode::Numeric, "Infinity");
        unrepresentable(Decode::Numeric, "-Infinity");
        assert_eq!(
            decode(&Decode::Numeric, &"9".repeat(39)),
            Err(ValueError::Overflow)
        );
        assert_eq!(
            ok(Decode::Money, "-$1,234.56"),
            V::Numeric(Decimal::new(-123456, 2))
        );
        assert_eq!(
            ok(Decode::Money, "$92,233,720,368,547,758.07"),
            V::Numeric(Decimal::new(9_223_372_036_854_775_807, 2))
        );
    }

    #[test]
    fn dates() {
        assert_eq!(date("1970-01-01"), Ok(0));
        assert_eq!(date("2000-03-01"), Ok(11_017));
        assert_eq!(date("0001-01-01"), Ok(-719_162));
        assert_eq!(
            date("0001-12-31 BC"),
            Ok(-719_163),
            "1 BC is the year before 1 AD"
        );
        assert_eq!(
            date("4714-11-24 BC"),
            Ok(-2_440_588),
            "PostgreSQL's first date (Julian day 0)"
        );
        assert_eq!(
            date("5874897-12-31"),
            Ok(2_145_042_905),
            "PostgreSQL's last date"
        );
        assert_eq!(date("infinity"), Ok(i32::MAX));
        assert_eq!(date("-infinity"), Ok(i32::MIN));
        for t in ["1970-1-x", "1970-13-01", "1970/01/01", "", "-1970-01-01"] {
            assert!(date(t).is_err(), "{t}");
        }
    }

    #[test]
    fn times() {
        assert_eq!(time("00:00:00"), Ok(0));
        assert_eq!(time("13:45:06.5"), Ok(49_506_500_000));
        assert_eq!(time("23:59:59.999999"), Ok(86_399_999_999));
        assert_eq!(time("24:00:00"), Ok(86_400_000_000));
        for t in [
            "24:00:01",
            "12:60:00",
            "12:00",
            "12:00:00.1234567",
            "1a:00:00",
        ] {
            assert!(time(t).is_err(), "{t}");
        }
    }

    #[test]
    fn timestamps() {
        assert_eq!(timestamp("1970-01-01 00:00:00", false), Ok(0));
        assert_eq!(
            timestamp("2024-02-29 12:34:56.789012", false),
            Ok(1_709_210_096_789_012)
        );
        assert_eq!(
            timestamp("2024-02-29 12:34:56.789012+00", true),
            Ok(1_709_210_096_789_012)
        );
        assert_eq!(timestamp("1970-01-01 05:30:00+05:30", true), Ok(0));
        assert_eq!(timestamp("1969-12-31 23:59:59-00:00:01", true), Ok(0));
        assert_eq!(
            timestamp("0001-01-01 00:00:00 BC", false),
            Ok(-62_167_219_200_000_000)
        );
        assert_eq!(timestamp("infinity", true), Ok(i64::MAX));
        assert_eq!(timestamp("-infinity", false), Ok(i64::MIN));
        assert_eq!(
            timestamp("294276-12-31 23:59:59.999999", false),
            Err(ValueError::Unrepresentable(
                "timestamp out of range: \"294276-12-31 23:59:59.999999\"".into()
            )),
            "PostgreSQL's last instant is past i64 microseconds"
        );
        for (t, tz) in [
            ("1970-01-01", false),
            ("1970-01-01 00:00:00", true),
            ("1970-01-01 00:00:00+1", true),
        ] {
            assert!(timestamp(t, tz).is_err(), "{t}");
        }
    }

    #[test]
    fn key_values_render_as_postgresql_input() {
        assert_eq!(text_of(&V::Bool(true)).as_deref(), Some("t"));
        assert_eq!(
            text_of(&V::Numeric(Decimal::new(-1234, 2))).as_deref(),
            Some("-12.34")
        );
        assert_eq!(
            text_of(&V::Bytes(vec![0xde, 0xad])).as_deref(),
            Some("\\xdead")
        );
        assert_eq!(
            text_of(&V::Date(-719_163)).as_deref(),
            Some("0001-12-31 BC")
        );
        assert_eq!(text_of(&V::Date(i32::MAX)).as_deref(), Some("infinity"));
        assert_eq!(
            text_of(&V::Time(86_400_000_000)).as_deref(),
            Some("24:00:00.000000")
        );
        assert_eq!(
            text_of(&V::Timestamp {
                micros: 1_709_210_096_789_012,
                with_tz: true
            })
            .as_deref(),
            Some("2024-02-29 12:34:56.789012+00")
        );
        assert_eq!(
            text_of(&V::Timestamp {
                micros: -62_167_219_200_000_000,
                with_tz: false
            })
            .as_deref(),
            Some("0001-01-01 00:00:00.000000 BC")
        );
        for v in [V::Null, V::Float64(1.0), V::Json(serde_json::json!(1))] {
            assert_eq!(text_of(&v), None);
        }
    }

    #[test]
    fn bit_strings() {
        assert_eq!(ok(Decode::Bit, "101"), V::Bytes(vec![0b1010_0000]));
        assert_eq!(ok(Decode::Bit, "111111111"), V::Bytes(vec![0xff, 0x80]));
        assert_eq!(ok(Decode::Bit, ""), V::Bytes(vec![]));
        assert_eq!(
            ok(Decode::VarBit, "10"),
            V::Bytes(vec![0, 0, 0, 2, 0b1000_0000])
        );
        assert_eq!(ok(Decode::VarBit, ""), V::Bytes(vec![0, 0, 0, 0]));
        unrepresentable(Decode::Bit, "102");
    }

    #[test]
    fn hstores() {
        assert_eq!(
            ok(Decode::Hstore, r#""a"=>"1", "b c"=>NULL, "q\"k"=>"x\\y""#),
            V::Json(serde_json::json!({"a": "1", "b c": null, "q\"k": "x\\y"}))
        );
        assert_eq!(ok(Decode::Hstore, ""), V::Json(serde_json::json!({})));
        unrepresentable(Decode::Hstore, r#""a"=>1"#);
        unrepresentable(Decode::Hstore, r#""a"=>"1","b"=>"2""#);
    }

    #[test]
    fn postgis_hex() {
        assert_eq!(
            ok(Decode::HexBytes, "0101000020E6100000"),
            V::Bytes(vec![1, 1, 0, 0, 0x20, 0xe6, 0x10, 0, 0])
        );
        unrepresentable(Decode::HexBytes, "0G");
    }

    fn arr(element: Decode, element_type: ColumnValueType, delimiter: u8) -> Decode {
        Decode::Array {
            element: Box::new(element),
            element_type,
            delimiter,
        }
    }

    #[test]
    fn arrays() {
        let ints = arr(Decode::Int32, ColumnValueType::Int32, b',');
        assert_eq!(
            ok(ints.clone(), "{1,NULL,-3}"),
            V::Array {
                element_type: ColumnValueType::Int32,
                values: vec![V::Int32(1), V::Null, V::Int32(-3)]
            }
        );
        assert_eq!(
            ok(ints.clone(), "[0:1]={7,8}"),
            V::Array {
                element_type: ColumnValueType::Int32,
                values: vec![V::Int32(7), V::Int32(8)]
            }
        );
        assert_eq!(
            ok(ints.clone(), "{}"),
            V::Array {
                element_type: ColumnValueType::Int32,
                values: vec![]
            }
        );
        let texts = arr(Decode::Text, ColumnValueType::Text, b',');
        assert_eq!(
            ok(
                texts.clone(),
                r#"{plain,"with, comma","q\"uote","back\\slash","NULL",""}"#
            ),
            V::Array {
                element_type: ColumnValueType::Text,
                values: ["plain", "with, comma", "q\"uote", "back\\slash", "NULL", ""]
                    .map(|s| V::Text(s.into()))
                    .to_vec()
            }
        );
        let boxes = arr(Decode::Text, ColumnValueType::Text, b';');
        assert_eq!(
            ok(boxes, "{(1,1),(0,0);(2,2),(1,1)}"),
            V::Array {
                element_type: ColumnValueType::Text,
                values: vec![V::Text("(1,1),(0,0)".into()), V::Text("(2,2),(1,1)".into())]
            }
        );
        assert_eq!(
            decode(&ints, "{{1,2},{3,4}}"),
            Err(ValueError::Unrepresentable(
                "a multi-dimensional array; override the column to string".into()
            ))
        );
        for t in ["1,2", "{1,,2}", "{\"a}", "{1,2"] {
            unrepresentable(texts.clone(), t);
        }
        let dates = arr(Decode::Date, ColumnValueType::Date, b',');
        assert_eq!(
            ok(dates, "{1970-01-02,infinity}"),
            V::Array {
                element_type: ColumnValueType::Date,
                values: vec![V::Date(1), V::Date(i32::MAX)]
            }
        );
    }

    proptest::proptest! {
        /// Every date PostgreSQL prints round-trips through the civil-day arithmetic.
        #[test]
        fn dates_round_trip(days in -2_440_588i32..=2_145_042_905) {
            let text = text_of(&V::Date(days)).unwrap();
            proptest::prop_assert_eq!(date(&text), Ok(days));
        }

        /// Timestamps round-trip through their text, both kinds, BC included.
        #[test]
        fn timestamps_round_trip(micros in -210_866_803_200_000_000i64..=9_223_371_331_199_999_999, tz: bool) {
            let text = text_of(&V::Timestamp { micros, with_tz: tz }).unwrap();
            proptest::prop_assert_eq!(timestamp(&text, tz), Ok(micros), "{}", text);
        }

        /// Arbitrary array text never panics.
        #[test]
        fn array_text_never_panics(text in "\\PC{0,24}") {
            let _ = array(&text, b',');
            let _ = hstore(&text);
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `ColumnValue`
error[E0425]: cannot find type `Decode` in this scope
error[E0425]: cannot find type `ColumnValue` in this scope
error[E0425]: cannot find function `decode` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-source-pg/src/value.rs`, above the test module:

```rust
//! Text form → `ColumnValue` (Hop 1, `Type_Mapping.md` PostgreSQL tables). The text is what
//! PostgreSQL's output functions print under the pinned session settings
//! ([`crate::SESSION_OPTIONS`]): the same for a pgoutput tuple and a snapshot `::text` column.

use data_avalanche_core::{ColumnValue, ColumnValueType, Decimal, DecimalError};

/// How one column's text becomes a value. Planned once per column from its type
/// ([`crate::types`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decode {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    /// `numeric`, and `numeric(P,S)` overridden to a decimal.
    Numeric,
    /// `money`: the C-locale form `-$1,234.56` (`lc_monetary=C`).
    Money,
    /// The text as is: `text`, `varchar`, and every type kept as its PostgreSQL text.
    Text,
    /// `char(n)`: trailing spaces stripped.
    BpChar,
    /// `"char"`: one byte as `Int16`.
    Char,
    /// `bytea` hex form `\x…`.
    Bytea,
    Date,
    Time,
    Timestamp {
        with_tz: bool,
    },
    Uuid,
    Json,
    /// `bit(n)`: the bits packed MSB-first, the last byte zero-padded.
    Bit,
    /// `varbit(n)`: PostgreSQL's binary form, a 4-byte big-endian bit count then the packed bits
    /// (Q-M5-9).
    VarBit,
    /// `hstore` → a JSON object of strings (or `null`).
    Hstore,
    /// PostGIS `geometry`/`geography`: the hex EWKB text as bytes.
    HexBytes,
    /// An array literal of `element`s separated by `delimiter`.
    Array {
        element: Box<Decode>,
        element_type: ColumnValueType,
        delimiter: u8,
    },
}

/// Why a value can't be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueError {
    /// The value doesn't fit its decimal column (`NUMERIC_OVERFLOW`).
    Overflow,
    /// The value has no representation in the column's type (`SCHEMA_MISMATCH`).
    Unrepresentable(String),
}

fn bad(what: &str, text: &str) -> ValueError {
    ValueError::Unrepresentable(format!("{what} {text:?}"))
}

/// Decodes the text of one value.
pub fn decode(d: &Decode, text: &str) -> Result<ColumnValue, ValueError> {
    use ColumnValue as V;
    Ok(match d {
        Decode::Bool => match text {
            "t" => V::Bool(true),
            "f" => V::Bool(false),
            _ => return Err(bad("not a boolean:", text)),
        },
        Decode::Int16 => V::Int16(text.parse().map_err(|_| bad("not an int2:", text))?),
        Decode::Int32 => V::Int32(text.parse().map_err(|_| bad("not an int4:", text))?),
        Decode::Int64 => V::Int64(text.parse().map_err(|_| bad("not an int8:", text))?),
        Decode::Float32 => V::Float32(text.parse().map_err(|_| bad("not a float4:", text))?),
        Decode::Float64 => V::Float64(text.parse().map_err(|_| bad("not a float8:", text))?),
        Decode::Numeric => V::Numeric(numeric(text)?),
        Decode::Money => V::Numeric(numeric(&money(text))?),
        Decode::Text => V::Text(text.to_owned()),
        Decode::BpChar => V::Text(text.trim_end_matches(' ').to_owned()),
        Decode::Char => V::Int16(char_byte(text)?),
        Decode::Bytea => V::Bytes(bytea(text)?),
        Decode::Date => V::Date(date(text)?),
        Decode::Time => V::Time(time(text)?),
        Decode::Timestamp { with_tz } => V::Timestamp {
            micros: timestamp(text, *with_tz)?,
            with_tz: *with_tz,
        },
        Decode::Uuid => V::Uuid(uuid::Uuid::parse_str(text).map_err(|_| bad("not a uuid:", text))?),
        Decode::Json => V::Json(serde_json::from_str(text).map_err(|e| {
            ValueError::Unrepresentable(format!("JSON the decoder can't read: {e}"))
        })?),
        Decode::Bit => V::Bytes(bits(text)?),
        Decode::VarBit => {
            let n = u32::try_from(text.len()).map_err(|_| bad("bit string too long:", ""))?;
            let mut out = n.to_be_bytes().to_vec();
            out.extend(bits(text)?);
            V::Bytes(out)
        }
        Decode::Hstore => V::Json(hstore(text)?),
        Decode::HexBytes => V::Bytes(hex(text).ok_or_else(|| bad("not hex:", text))?),
        Decode::Array {
            element,
            element_type,
            delimiter,
        } => {
            let values = array(text, *delimiter)?
                .into_iter()
                .map(|e| match e {
                    None => Ok(V::Null),
                    Some(s) => decode(element, &s),
                })
                .collect::<Result<_, _>>()?;
            V::Array {
                element_type: element_type.clone(),
                values,
            }
        }
    })
}

/// `numeric` text. `NaN` and `±Infinity` have no decimal representation; more than 38 digits
/// overflow.
fn numeric(text: &str) -> Result<Decimal, ValueError> {
    Decimal::parse(text).map_err(|e| match e {
        DecimalError::TooManyDigits(_) => ValueError::Overflow,
        DecimalError::NotFinite(_) => {
            ValueError::Unrepresentable(format!("{text} can't be stored in a decimal column"))
        }
        _ => bad("not a numeric:", text),
    })
}

/// `-$1,234.56` → `-1234.56`.
fn money(text: &str) -> String {
    text.chars().filter(|c| !matches!(c, '$' | ',')).collect()
}

/// `"char"` prints a byte as itself, or `\ooo` (octal) when it isn't printable ASCII.
fn char_byte(text: &str) -> Result<i16, ValueError> {
    let b = text.as_bytes();
    match b {
        [] => Ok(0),
        [c] => Ok(i16::from(*c)),
        [b'\\', o @ ..] if o.len() == 3 => u8::from_str_radix(&text[1..], 8)
            .map(i16::from)
            .map_err(|_| bad("not a \"char\":", text)),
        _ => Err(bad("not a \"char\":", text)),
    }
}

fn hex(text: &str) -> Option<Vec<u8>> {
    let b = text.as_bytes();
    if !b.len().is_multiple_of(2) {
        return None;
    }
    let nibble = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    b.chunks(2)
        .map(|p| Some(nibble(p[0])? << 4 | nibble(p[1])?))
        .collect()
}

fn bytea(text: &str) -> Result<Vec<u8>, ValueError> {
    text.strip_prefix("\\x")
        .and_then(hex)
        .ok_or_else(|| bad("not a hex bytea:", text))
}

fn bits(text: &str) -> Result<Vec<u8>, ValueError> {
    let mut out = vec![0u8; text.len().div_ceil(8)];
    for (i, c) in text.bytes().enumerate() {
        match c {
            b'0' => {}
            b'1' => out[i / 8] |= 0x80 >> (i % 8),
            _ => return Err(bad("not a bit string:", text)),
        }
    }
    Ok(out)
}

/// Days from 1970-01-01 to the proleptic Gregorian date (astronomical year numbering).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`] (Howard Hinnant's algorithm): `(year, month, day)`,
/// astronomical year numbering.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// A date as PostgreSQL's ISO input reads it: `YYYY-MM-DD`, ` BC` before year 1.
fn date_text(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    if y <= 0 {
        format!("{:04}-{m:02}-{d:02} BC", 1 - y)
    } else {
        format!("{y:04}-{m:02}-{d:02}")
    }
}

/// The text PostgreSQL reads back as `v`: the inverse of [`decode`] for the types an identifier
/// can have (no floats, JSON or arrays, Ghost_Row §1). `None` for those, and for NULL.
pub fn text_of(v: &ColumnValue) -> Option<String> {
    use ColumnValue as V;
    Some(match v {
        V::Bool(b) => if *b { "t" } else { "f" }.to_owned(),
        V::Int16(i) => i.to_string(),
        V::Int32(i) => i.to_string(),
        V::Int64(i) => i.to_string(),
        V::UInt64(i) => i.to_string(),
        V::Numeric(d) => d.to_string(),
        V::Text(t) => t.clone(),
        V::Bytes(b) => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            format!("\\x{hex}")
        }
        V::Date(i32::MAX) => "infinity".into(),
        V::Date(i32::MIN) => "-infinity".into(),
        V::Date(d) => date_text(i64::from(*d)),
        V::Time(t) => data_avalanche_core::time_text(*t),
        V::Timestamp {
            micros: i64::MAX, ..
        } => "infinity".into(),
        V::Timestamp {
            micros: i64::MIN, ..
        } => "-infinity".into(),
        V::Timestamp { micros, with_tz } => {
            let days = micros.div_euclid(86_400_000_000);
            let clock = data_avalanche_core::time_text(micros.rem_euclid(86_400_000_000));
            let (date, bc) = match date_text(days).strip_suffix(" BC") {
                Some(d) => (d.to_owned(), " BC"),
                None => (date_text(days), ""),
            };
            let tz = if *with_tz { "+00" } else { "" };
            format!("{date} {clock}{tz}{bc}")
        }
        V::Uuid(u) => u.to_string(),
        V::Null | V::Unchanged | V::Float32(_) | V::Float64(_) | V::Json(_) | V::Array { .. } => {
            return None;
        }
    })
}

fn digits(s: &str, what: &str, text: &str) -> Result<i64, ValueError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad(what, text));
    }
    s.parse().map_err(|_| bad(what, text))
}

/// `YYYY-MM-DD` (the year may be longer than four digits) with an optional ` BC`: days since
/// the epoch, before the range checks of the caller.
fn civil_days(date: &str, bc: bool, text: &str) -> Result<i64, ValueError> {
    let mut parts = date.rsplitn(3, '-');
    let (d, m, y) = (parts.next(), parts.next(), parts.next());
    let (Some(d), Some(m), Some(y)) = (d, m, y) else {
        return Err(bad("not an ISO date:", text));
    };
    let (y, m, d) = (
        digits(y, "not an ISO date:", text)?,
        digits(m, "not an ISO date:", text)?,
        digits(d, "not an ISO date:", text)?,
    );
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad("not an ISO date:", text));
    }
    // 1 BC is year 0, 2 BC is year -1.
    let y = if bc { 1 - y } else { y };
    Ok(days_from_civil(y, m, d))
}

fn split_bc(text: &str) -> (&str, bool) {
    match text.strip_suffix(" BC") {
        Some(rest) => (rest, true),
        None => (text, false),
    }
}

/// `date` → days since 1970-01-01; `±infinity` clamps to `i32` MAX/MIN (Q-M5-1).
pub fn date(text: &str) -> Result<i32, ValueError> {
    match text {
        "infinity" => return Ok(i32::MAX),
        "-infinity" => return Ok(i32::MIN),
        _ => {}
    }
    let (date, bc) = split_bc(text);
    i32::try_from(civil_days(date, bc, text)?).map_err(|_| bad("date out of range:", text))
}

/// `HH:MM:SS[.ffffff]` → microseconds; `24:00:00` is 86 400 000 000.
pub fn time(text: &str) -> Result<i64, ValueError> {
    let err = || bad("not a time:", text);
    let (hms, frac) = text.split_once('.').unwrap_or((text, ""));
    let mut it = hms.split(':');
    let (Some(h), Some(m), Some(s), None) = (it.next(), it.next(), it.next(), it.next()) else {
        return Err(err());
    };
    let (h, m, s) = (
        digits(h, "not a time:", text)?,
        digits(m, "not a time:", text)?,
        digits(s, "not a time:", text)?,
    );
    if frac.len() > 6 || (!frac.is_empty() && !frac.bytes().all(|b| b.is_ascii_digit())) {
        return Err(err());
    }
    let micros = if frac.is_empty() {
        0
    } else {
        frac.parse::<i64>().map_err(|_| err())? * 10_i64.pow(6 - frac.len() as u32)
    };
    let total = ((h * 60 + m) * 60 + s) * 1_000_000 + micros;
    if m > 59 || s > 59 || total > 86_400_000_000 {
        return Err(err());
    }
    Ok(total)
}

/// A UTC offset `+HH`, `+HH:MM` or `+HH:MM:SS` → seconds east of UTC.
fn offset(text: &str, full: &str) -> Result<i64, ValueError> {
    let err = || bad("not a timestamp:", full);
    let (sign, rest) = match text.as_bytes().first() {
        Some(b'+') => (1, &text[1..]),
        Some(b'-') => (-1, &text[1..]),
        _ => return Err(err()),
    };
    let mut secs = 0;
    for (i, part) in rest.split(':').enumerate() {
        if i > 2 || part.len() != 2 {
            return Err(err());
        }
        secs += digits(part, "not a timestamp:", full)? * [3600, 60, 1][i];
    }
    Ok(sign * secs)
}

/// `timestamp` / `timestamptz` → microseconds since the epoch (UTC for `timestamptz`, wall clock
/// for `timestamp`); `±infinity` clamps to `i64` MAX/MIN.
pub fn timestamp(text: &str, with_tz: bool) -> Result<i64, ValueError> {
    match text {
        "infinity" => return Ok(i64::MAX),
        "-infinity" => return Ok(i64::MIN),
        _ => {}
    }
    let err = || bad("not a timestamp:", text);
    let (body, bc) = split_bc(text);
    let (date_part, mut clock) = body.split_once(' ').ok_or_else(err)?;
    let mut offset_secs = 0;
    if with_tz {
        let at = clock.rfind(['+', '-']).ok_or_else(err)?;
        offset_secs = offset(&clock[at..], text)?;
        clock = &clock[..at];
    }
    let days = civil_days(date_part, bc, text)?;
    let t = time(clock).map_err(|_| err())?;
    days.checked_mul(86_400_000_000)
        .and_then(|d| d.checked_add(t))
        .and_then(|v| v.checked_sub(offset_secs * 1_000_000))
        .ok_or_else(|| bad("timestamp out of range:", text))
}

/// The `hstore` text form `"k"=>"v", "k2"=>NULL` → a JSON object.
fn hstore(text: &str) -> Result<serde_json::Value, ValueError> {
    let err = || bad("not an hstore:", text);
    let b = text.as_bytes();
    let mut i = 0;
    let mut map = serde_json::Map::new();
    let quoted = |i: &mut usize| -> Result<String, ValueError> {
        if b.get(*i) != Some(&b'"') {
            return Err(err());
        }
        *i += 1;
        let mut out = Vec::new();
        loop {
            match b.get(*i) {
                None => return Err(err()),
                Some(b'"') => {
                    *i += 1;
                    return String::from_utf8(out).map_err(|_| err());
                }
                Some(b'\\') => {
                    out.push(*b.get(*i + 1).ok_or_else(err)?);
                    *i += 2;
                }
                Some(&c) => {
                    out.push(c);
                    *i += 1;
                }
            }
        }
    };
    while i < b.len() {
        let k = quoted(&mut i)?;
        if !text[i..].starts_with("=>") {
            return Err(err());
        }
        i += 2;
        let v = if text[i..].starts_with("NULL") {
            i += 4;
            serde_json::Value::Null
        } else {
            serde_json::Value::String(quoted(&mut i)?)
        };
        map.insert(k, v);
        if i < b.len() {
            if !text[i..].starts_with(", ") {
                return Err(err());
            }
            i += 2;
        }
    }
    Ok(serde_json::Value::Object(map))
}

/// The elements of a one-dimensional array literal (`{a,"b c",NULL}`, optionally after a
/// `[lo:hi]=` bounds decoration); `None` for NULL.
fn array(text: &str, delimiter: u8) -> Result<Vec<Option<String>>, ValueError> {
    let err = || bad("not an array:", text);
    let body = match text.as_bytes().first() {
        Some(b'[') => &text[text.find('=').ok_or_else(err)? + 1..],
        _ => text,
    };
    let inner = body
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(err)?;
    let b = inner.as_bytes();
    let mut out = Vec::new();
    if b.is_empty() {
        return Ok(out);
    }
    let mut i = 0;
    loop {
        match b.get(i) {
            Some(b'{') => {
                return Err(ValueError::Unrepresentable(
                    "a multi-dimensional array; override the column to string".into(),
                ));
            }
            Some(b'"') => {
                i += 1;
                let mut v = Vec::new();
                loop {
                    match b.get(i) {
                        None => return Err(err()),
                        Some(b'"') => {
                            i += 1;
                            break;
                        }
                        Some(b'\\') => {
                            v.push(*b.get(i + 1).ok_or_else(err)?);
                            i += 2;
                        }
                        Some(&c) => {
                            v.push(c);
                            i += 1;
                        }
                    }
                }
                out.push(Some(String::from_utf8(v).map_err(|_| err())?));
            }
            _ => {
                let end = b[i..]
                    .iter()
                    .position(|&c| c == delimiter)
                    .map_or(b.len(), |p| i + p);
                let v = &inner[i..end];
                if v.is_empty() {
                    return Err(err());
                }
                out.push(if v.eq_ignore_ascii_case("NULL") {
                    None
                } else {
                    Some(v.to_owned())
                });
                i = end;
            }
        }
        match b.get(i) {
            None => return Ok(out),
            Some(&c) if c == delimiter => i += 1,
            Some(_) => return Err(err()),
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 34.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/value.rs
git commit -m "feat(source-pg): text values to ColumnValue"
```

---

### Task 5: Type planning: OID → value type and decoder

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/types.rs`

**Interfaces:**
- Consumes: `Decode` (Task 4); `LogicalType`, `NumericOverflowAction` (config);
  `ColumnValueType`, `HaltReason` (core).
- Produces (`types` is public):
  - `TypeInfo { oid, name, schema, kind, category, element, base, base_typmod, delimiter }`,
    `TypeCatalog = HashMap<u32, TypeInfo>`.
  - `Plan { value_type, decode, read_as_text, unconstrained_numeric }`, `PlanError::{NumericOverflow,
    Missing(u32)}`.
  - `numeric_typmod(i32) -> Option<(i32, i32)>`, `UNCONSTRAINED_NUMERIC` (`Numeric { 38, 18 }`).
  - `plan(oid, typmod, &TypeCatalog, NumericOverflowAction) -> Result<Plan, PlanError>`,
    `missing(oid, &TypeCatalog) -> Option<u32>` (the first type the catalog must still load).
  - `ColumnPlan { value_type, logical, decode, read_as_text, unconstrained_numeric }`,
    `plan_column(column, oid, typmod, &TypeCatalog, NumericOverflowAction, Option<&LogicalType>) ->
    Result<ColumnPlan, HaltReason>`.

Type_Mapping's PostgreSQL table and its fallback chain: built-in OIDs first, then domains
(through to their base type), enums, ranges, composites and known extension types (citext,
ltree, hstore, PostGIS), and anything else as its text with a warning. Numeric rules are D-M5.9
and Q-M5-8. Overrides are D-M5.10.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-source-pg/src/lib.rs` with:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod connect;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod error;
pub mod pgoutput;
#[cfg(test)]
mod test_support;
#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod tls;
pub mod types;
pub mod value;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
```

Create `crates/data-avalanche-source-pg/src/types.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const S: NumericOverflowAction = NumericOverflowAction::String;
    const E: NumericOverflowAction = NumericOverflowAction::Error;

    fn info(
        oid: u32,
        name: &str,
        kind: u8,
        category: u8,
        element: u32,
        base: u32,
        base_typmod: i32,
    ) -> TypeInfo {
        TypeInfo {
            oid,
            name: name.into(),
            schema: if oid < 16384 { "pg_catalog" } else { "public" }.into(),
            kind,
            category,
            element,
            base,
            base_typmod,
            delimiter: if name == "box" { b';' } else { b',' },
        }
    }

    /// Catalog rows for the types the tests use beyond the built-ins.
    fn catalog() -> TypeCatalog {
        [
            info(1007, "_int4", b'b', b'A', 23, 0, -1),
            info(1231, "_numeric", b'b', b'A', 1700, 0, -1),
            info(1020, "_box", b'b', b'A', 603, 0, -1),
            info(603, "box", b'b', b'G', 600, 0, -1),
            info(1000, "_bool", b'b', b'A', 16, 0, -1),
            info(70000, "mood", b'e', b'E', 0, 0, -1),
            info(70001, "_mood", b'b', b'A', 70000, 0, -1),
            info(70002, "price", b'd', b'N', 0, 1700, numeric_mod(10, 2)),
            info(70003, "address", b'c', b'C', 0, 0, -1),
            info(70004, "hstore", b'b', b'U', 0, 0, -1),
            info(70005, "geometry", b'b', b'U', 0, 0, -1),
            info(70006, "citext", b'b', b'S', 0, 0, -1),
            info(70007, "vector", b'b', b'U', 0, 0, -1),
            info(70008, "floatrange", b'r', b'R', 0, 0, -1),
            info(70009, "wide", b'd', b'N', 0, 1700, -1),
            info(70010, "cash", b'd', b'N', 0, 790, -1),
            info(70011, "_price", b'b', b'A', 70002, 0, -1),
        ]
        .into_iter()
        .map(|t| (t.oid, t))
        .collect()
    }

    fn numeric_mod(p: i32, s: i32) -> i32 {
        ((p << 16) | (s & 0x7FF)) + 4
    }

    fn vt(oid: u32, typmod: i32) -> V {
        plan(oid, typmod, &catalog(), S).unwrap().value_type
    }

    #[test]
    fn built_in_scalars() {
        let c = catalog();
        for (oid, t) in [
            (16, V::Bool),
            (21, V::Int16),
            (23, V::Int32),
            (20, V::Int64),
            (26, V::Int64),
            (18, V::Int16),
            (700, V::Float32),
            (701, V::Float64),
            (25, V::Text),
            (1043, V::Text),
            (1042, V::Text),
            (19, V::Text),
            (17, V::Bytes),
            (1082, V::Date),
            (1083, V::Time),
            (1114, V::Timestamp { with_tz: false }),
            (1184, V::Timestamp { with_tz: true }),
            (2950, V::Uuid),
            (114, V::Json),
            (3802, V::Json),
            (1560, V::Bytes),
            (1562, V::Bytes),
            (
                790,
                V::Numeric {
                    precision: 19,
                    scale: 2,
                },
            ),
            (1266, V::Text),
            (1186, V::Text),
            (869, V::Text),
            (3904, V::Text),
            (4451, V::Text),
            (3614, V::Text),
            (142, V::Text),
        ] {
            let p = plan(oid, -1, &c, S).unwrap();
            assert_eq!(p.value_type, t, "oid {oid}");
            assert_eq!(p.read_as_text, None, "oid {oid}");
        }
        assert_eq!(plan(1042, -1, &c, S).unwrap().decode, Decode::BpChar);
    }

    #[test]
    fn numeric_precision_rules() {
        assert_eq!(numeric_typmod(-1), None);
        assert_eq!(numeric_typmod(numeric_mod(10, 2)), Some((10, 2)));
        assert_eq!(numeric_typmod(numeric_mod(3, -2)), Some((3, -2)));
        assert_eq!(
            vt(1700, numeric_mod(10, 2)),
            V::Numeric {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            vt(1700, numeric_mod(38, 0)),
            V::Numeric {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(
            vt(1700, numeric_mod(3, -2)),
            V::Numeric {
                precision: 5,
                scale: 0
            },
            "negative scale widens"
        );
        assert_eq!(
            vt(1700, numeric_mod(2, 5)),
            V::Numeric {
                precision: 5,
                scale: 5
            },
            "scale above precision"
        );
        // Beyond 38 digits: text under `string`, a halt under `error`.
        assert_eq!(vt(1700, numeric_mod(39, 2)), V::Text);
        assert_eq!(
            plan(1700, numeric_mod(39, 2), &catalog(), S)
                .unwrap()
                .read_as_text
                .as_deref(),
            Some("numeric(39,2)"),
            "text with a warning (Type_Mapping)"
        );
        assert_eq!(vt(1700, numeric_mod(37, -2)), V::Text);
        assert_eq!(
            plan(1700, numeric_mod(39, 2), &catalog(), E),
            Err(PlanError::NumericOverflow)
        );
        // No declared precision: text under `string`, decimal(38,18) under `error` (Q-M5-8).
        let bare = plan(1700, -1, &catalog(), S).unwrap();
        assert_eq!(
            (bare.value_type, bare.unconstrained_numeric),
            (V::Text, true)
        );
        assert_eq!(bare.read_as_text.as_deref(), Some("numeric"));
        let bare = plan(1700, -1, &catalog(), E).unwrap();
        assert_eq!(
            (bare.value_type, bare.unconstrained_numeric),
            (UNCONSTRAINED_NUMERIC, true)
        );
    }

    #[test]
    fn arrays_follow_their_element() {
        assert_eq!(vt(1007, -1), V::Array(Box::new(V::Int32)));
        assert_eq!(
            vt(1231, numeric_mod(12, 3)),
            V::Array(Box::new(V::Numeric {
                precision: 12,
                scale: 3
            })),
            "the column typmod applies to the element"
        );
        assert_eq!(
            vt(70001, -1),
            V::Array(Box::new(V::Text)),
            "an array of an enum"
        );
        assert_eq!(
            vt(70011, -1),
            V::Array(Box::new(V::Numeric {
                precision: 10,
                scale: 2
            })),
            "an array of a domain"
        );
        let boxes = plan(1020, -1, &catalog(), S).unwrap();
        assert!(
            matches!(
                boxes.decode,
                Decode::Array {
                    delimiter: b';',
                    ..
                }
            ),
            "box arrays use ';'"
        );
    }

    #[test]
    fn user_and_extension_types() {
        assert_eq!(vt(70000, -1), V::Text, "enum");
        assert_eq!(vt(70003, -1), V::Text, "composite");
        assert_eq!(vt(70008, -1), V::Text, "range");
        assert_eq!(
            vt(70002, -1),
            V::Numeric {
                precision: 10,
                scale: 2
            },
            "a domain takes its typmod"
        );
        assert_eq!(vt(70004, -1), V::Json, "hstore");
        assert_eq!(vt(70005, -1), V::Bytes, "PostGIS");
        assert_eq!(vt(70006, -1), V::Text, "citext");
        let unknown = plan(70007, -1, &catalog(), S).unwrap();
        assert_eq!(unknown.value_type, V::Text);
        assert_eq!(unknown.read_as_text.as_deref(), Some("public.vector"));
        assert_eq!(
            plan(99999, -1, &catalog(), S),
            Err(PlanError::Missing(99999))
        );
    }

    #[test]
    fn missing_names_what_to_fetch() {
        let c = catalog();
        assert_eq!(missing(23, &c), None);
        assert_eq!(missing(1007, &c), None);
        assert_eq!(missing(99999, &c), Some(99999));
        let mut partial = c.clone();
        partial.remove(&70000);
        assert_eq!(
            missing(70001, &partial),
            Some(70000),
            "the element of a known array"
        );
    }

    #[test]
    fn overrides() {
        let c = catalog();
        let col = |oid, typmod, action, over: Option<LogicalType>| {
            plan_column("c", oid, typmod, &c, action, over.as_ref())
        };
        let s = col(2950, -1, S, Some(LogicalType::String)).unwrap();
        assert_eq!(
            (s.value_type, s.logical, s.decode),
            (V::Text, LogicalType::String, Decode::Text)
        );
        let d = col(
            1700,
            -1,
            E,
            Some(LogicalType::Decimal {
                precision: 38,
                scale: 10,
            }),
        )
        .unwrap();
        assert_eq!(
            d.value_type,
            V::Numeric {
                precision: 38,
                scale: 10
            }
        );
        assert_eq!(d.decode, Decode::Numeric);
        let wide = col(
            1700,
            numeric_mod(50, 2),
            E,
            Some(LogicalType::Decimal {
                precision: 38,
                scale: 2,
            }),
        )
        .unwrap();
        assert_eq!(
            wide.value_type,
            V::Numeric {
                precision: 38,
                scale: 2
            },
            "an override rescues an over-wide numeric"
        );
        let domain = col(
            70009,
            -1,
            S,
            Some(LogicalType::Decimal {
                precision: 20,
                scale: 4,
            }),
        )
        .unwrap();
        assert_eq!(domain.decode, Decode::Numeric, "through a domain");
        let money = col(
            70010,
            -1,
            S,
            Some(LogicalType::Decimal {
                precision: 20,
                scale: 2,
            }),
        )
        .unwrap();
        assert_eq!(money.decode, Decode::Money);
        let widened = col(
            1700,
            numeric_mod(10, 2),
            S,
            Some(LogicalType::Decimal {
                precision: 20,
                scale: 4,
            }),
        )
        .unwrap();
        assert_eq!(
            widened.logical,
            LogicalType::Decimal {
                precision: 20,
                scale: 4
            }
        );
        let e = col(2950, -1, S, Some(LogicalType::Int64)).unwrap_err();
        assert_eq!(e.code(), "SCHEMA_MISMATCH");
        assert_eq!(
            col(23, -1, S, Some(LogicalType::Int32)).unwrap().logical,
            LogicalType::Int32
        );
        assert_eq!(
            col(1700, numeric_mod(39, 0), E, None),
            Err(HaltReason::NumericOverflow { column: "c".into() })
        );
        let plain = col(1700, -1, S, None).unwrap();
        assert!(plain.unconstrained_numeric);
        assert_eq!(plain.logical, LogicalType::String);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0425]: cannot find type `NumericOverflowAction` in this scope
error[E0425]: cannot find type `TypeInfo` in this scope
error[E0422]: cannot find struct, variant or union type `TypeInfo` in this scope
error[E0425]: cannot find type `TypeCatalog` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-source-pg/src/types.rs`, above the test module:

```rust
//! Type planning (`Type_Mapping.md`, PostgreSQL): a column's type OID and `atttypmod` → its
//! `ColumnValueType` and the [`Decode`] for its text. Built-in scalars are known by OID; every
//! other type (arrays, domains, enums, composites, ranges, extension types) is planned from its
//! `pg_type` row ([`TypeInfo`]).

use std::collections::HashMap;

use data_avalanche_config::NumericOverflowAction;
use data_avalanche_core::{
    ColumnValueType as V, HaltReason, LogicalType, MAX_DECIMAL_PRECISION, canonical_logical,
    hop2_compatible,
};

use crate::value::Decode;

/// The `pg_type` columns the planner reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeInfo {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    /// `typtype`: `b` base, `c` composite, `d` domain, `e` enum, `p` pseudo, `r` range,
    /// `m` multirange.
    pub kind: u8,
    /// `typcategory`: `A` for arrays.
    pub category: u8,
    /// `typelem`: an array's element type.
    pub element: u32,
    /// `typbasetype`: a domain's base type.
    pub base: u32,
    /// `typtypmod`: a domain's typmod.
    pub base_typmod: i32,
    /// `typdelim`: the array delimiter of this element type.
    pub delimiter: u8,
}

/// `pg_type` rows by OID: the planner's view of the catalog.
pub type TypeCatalog = HashMap<u32, TypeInfo>;

/// The planned decoding of one column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub value_type: V,
    pub decode: Decode,
    /// A type kept as its text because it has no closer mapping (`Type_Mapping.md` fallback chain:
    /// text with a warning).
    pub read_as_text: Option<String>,
    /// A `numeric` column without a declared precision (V-079).
    pub unconstrained_numeric: bool,
}

impl Plan {
    fn of(value_type: V, decode: Decode) -> Self {
        Self {
            value_type,
            decode,
            read_as_text: None,
            unconstrained_numeric: false,
        }
    }

    fn text() -> Self {
        Self::of(V::Text, Decode::Text)
    }
}

/// Why a column can't be planned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// `numeric(P,S)` beyond 38 digits under `numeric_overflow_action: error`.
    NumericOverflow,
    /// A type OID with no `pg_type` row in the catalog given: the caller fetches it and plans
    /// again.
    Missing(u32),
}

/// Built-in types kept as their PostgreSQL text (`Type_Mapping.md`: network, geometric, range,
/// text-search, XML, interval, `timetz`, identifier and system types).
const TEXT_OIDS: &[u32] = &[
    // timetz, interval, xml, jsonpath
    1266, 1186, 142, 4072, // inet, cidr, macaddr, macaddr8
    869, 650, 829, 774, // point, lseg, path, box, polygon, line, circle
    600, 601, 602, 603, 604, 628, 718, // tsvector, tsquery
    3614, 3615, // ranges
    3904, 3906, 3908, 3910, 3912, 3926, // multiranges
    4451, 4532, 4533, 4534, 4535,
    4536, // xid, xid8, cid, tid, pg_lsn, txid_snapshot, pg_snapshot
    28, 5069, 29, 27, 3220, 2970, 5038, // reg* types
    24, 2202, 2203, 2204, 2205, 2206, 4096, 4089, 3734, 3769, 4191, // aclitem
    1033,
];

/// Extension types kept as text without a warning (they are text already).
const KNOWN_TEXT_TYPES: &[&str] = &["citext", "ltree", "lquery", "ltxtquery"];

/// `numeric`'s typmod → `(precision, scale)`, `None` without one. The scale is an 11-bit signed
/// field since PostgreSQL 15.
pub fn numeric_typmod(typmod: i32) -> Option<(i32, i32)> {
    if typmod < 4 {
        return None;
    }
    let t = typmod - 4;
    Some(((t >> 16) & 0xFFFF, ((t & 0x7FF) ^ 1024) - 1024))
}

/// Q-M5-8: a `numeric` without a declared precision, under `numeric_overflow_action: error`.
pub const UNCONSTRAINED_NUMERIC: V = V::Numeric {
    precision: 38,
    scale: 18,
};

fn numeric(typmod: i32, action: NumericOverflowAction) -> Result<Plan, PlanError> {
    let as_text = |t: String| Plan {
        read_as_text: Some(t),
        ..Plan::text()
    };
    let Some((p, s)) = numeric_typmod(typmod) else {
        let mut plan = match action {
            NumericOverflowAction::String => as_text("numeric".into()),
            NumericOverflowAction::Error => Plan::of(UNCONSTRAINED_NUMERIC, Decode::Numeric),
        };
        plan.unconstrained_numeric = true;
        return Ok(plan);
    };
    // A negative scale rounds to tens: numeric(3,-2) holds 12300 = decimal(5,0). A scale above
    // the precision holds only fractions: numeric(2,5) holds 0.00012 = decimal(5,5).
    let declared = format!("numeric({p},{s})");
    let (p, s) = if s < 0 { (p - s, 0) } else { (p.max(s), s) };
    if p > i32::from(MAX_DECIMAL_PRECISION) {
        return match action {
            NumericOverflowAction::String => Ok(as_text(declared)),
            NumericOverflowAction::Error => Err(PlanError::NumericOverflow),
        };
    }
    Ok(Plan::of(
        V::Numeric {
            precision: p as u8,
            scale: s as i8,
        },
        Decode::Numeric,
    ))
}

fn builtin(
    oid: u32,
    typmod: i32,
    action: NumericOverflowAction,
) -> Option<Result<Plan, PlanError>> {
    let p = |t: V, d: Decode| Some(Ok(Plan::of(t, d)));
    match oid {
        16 => p(V::Bool, Decode::Bool),
        17 => p(V::Bytes, Decode::Bytea),
        18 => p(V::Int16, Decode::Char),
        19 | 25 | 1043 => p(V::Text, Decode::Text),
        1042 => p(V::Text, Decode::BpChar),
        20 => p(V::Int64, Decode::Int64),
        21 => p(V::Int16, Decode::Int16),
        23 => p(V::Int32, Decode::Int32),
        26 => p(V::Int64, Decode::Int64),
        700 => p(V::Float32, Decode::Float32),
        701 => p(V::Float64, Decode::Float64),
        1700 => Some(numeric(typmod, action)),
        790 => p(
            V::Numeric {
                precision: 19,
                scale: 2,
            },
            Decode::Money,
        ),
        1082 => p(V::Date, Decode::Date),
        1083 => p(V::Time, Decode::Time),
        1114 => p(
            V::Timestamp { with_tz: false },
            Decode::Timestamp { with_tz: false },
        ),
        1184 => p(
            V::Timestamp { with_tz: true },
            Decode::Timestamp { with_tz: true },
        ),
        2950 => p(V::Uuid, Decode::Uuid),
        114 | 3802 => p(V::Json, Decode::Json),
        1560 => p(V::Bytes, Decode::Bit),
        1562 => p(V::Bytes, Decode::VarBit),
        o if TEXT_OIDS.contains(&o) => p(V::Text, Decode::Text),
        _ => None,
    }
}

/// Plans a column of type `oid` with `typmod`. Types not built in come from `types`.
pub fn plan(
    oid: u32,
    typmod: i32,
    types: &TypeCatalog,
    action: NumericOverflowAction,
) -> Result<Plan, PlanError> {
    if let Some(p) = builtin(oid, typmod, action) {
        return p;
    }
    let t = types.get(&oid).ok_or(PlanError::Missing(oid))?;
    Ok(match t.kind {
        b'd' => {
            let typmod = if typmod < 0 { t.base_typmod } else { typmod };
            return plan(t.base, typmod, types, action);
        }
        b'e' | b'c' | b'r' | b'm' => Plan::text(),
        _ if t.category == b'A' && t.element != 0 => {
            let e = types.get(&t.element);
            let element = plan(t.element, typmod, types, action)?;
            let delimiter = match e {
                Some(e) => e.delimiter,
                None if t.element == 603 => b';',
                None => b',',
            };
            Plan {
                value_type: V::Array(Box::new(element.value_type.clone())),
                decode: Decode::Array {
                    element: Box::new(element.decode),
                    element_type: element.value_type,
                    delimiter,
                },
                ..element
            }
        }
        _ if t.name == "hstore" => Plan::of(V::Json, Decode::Hstore),
        _ if matches!(t.name.as_str(), "geometry" | "geography") => {
            Plan::of(V::Bytes, Decode::HexBytes)
        }
        _ if KNOWN_TEXT_TYPES.contains(&t.name.as_str()) => Plan::text(),
        _ => Plan {
            read_as_text: Some(format!("{}.{}", t.schema, t.name)),
            ..Plan::text()
        },
    })
}

/// The OIDs `plan` will look up in the catalog for a column of type `oid`, given what is known.
pub fn missing(oid: u32, types: &TypeCatalog) -> Option<u32> {
    if builtin(oid, -1, NumericOverflowAction::String).is_some() {
        return None;
    }
    let Some(t) = types.get(&oid) else {
        return Some(oid);
    };
    match t.kind {
        b'd' => missing(t.base, types),
        _ if t.category == b'A' && t.element != 0 => missing(t.element, types),
        _ => None,
    }
}

/// A planned column: what the decoder produces and what the target stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPlan {
    pub value_type: V,
    pub logical: LogicalType,
    pub decode: Decode,
    pub read_as_text: Option<String>,
    pub unconstrained_numeric: bool,
}

/// Plans `column` with its `tables.column_type_overrides` entry, if any (config §9.3):
/// - `string` keeps the column's PostgreSQL text (`value_type` Text);
/// - `decimal(P,S)` on a `numeric` or `money` column decodes it as that decimal, whatever its
///   declared precision (the V-079 remedy);
/// - any other override must be one Hop 2 can store the column's values as
///   ([`hop2_compatible`]); otherwise the table halts (`SCHEMA_MISMATCH`).
pub fn plan_column(
    column: &str,
    oid: u32,
    typmod: i32,
    types: &TypeCatalog,
    action: NumericOverflowAction,
    over: Option<&LogicalType>,
) -> Result<ColumnPlan, HaltReason> {
    let plan_or_halt = |action| {
        plan(oid, typmod, types, action).map_err(|e| match e {
            PlanError::NumericOverflow => HaltReason::NumericOverflow {
                column: column.to_owned(),
            },
            PlanError::Missing(o) => HaltReason::SchemaMismatch {
                column: column.to_owned(),
                detail: format!("type {o} isn't in the catalog"),
            },
        })
    };
    let decimal_source = matches!(base_oid(oid, types), 1700 | 790);
    match over {
        Some(LogicalType::String) => Ok(ColumnPlan {
            value_type: V::Text,
            logical: LogicalType::String,
            decode: Decode::Text,
            read_as_text: None,
            unconstrained_numeric: false,
        }),
        Some(LogicalType::Decimal { precision, scale }) if decimal_source => Ok(ColumnPlan {
            value_type: V::Numeric {
                precision: *precision,
                scale: *scale,
            },
            logical: LogicalType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            decode: if base_oid(oid, types) == 790 {
                Decode::Money
            } else {
                Decode::Numeric
            },
            read_as_text: None,
            unconstrained_numeric: false,
        }),
        Some(l) => {
            // The `string` action plans an over-wide numeric as text; the override decides here.
            let p = plan_or_halt(NumericOverflowAction::String)?;
            if !hop2_compatible(&p.value_type, l) {
                return Err(HaltReason::SchemaMismatch {
                    column: column.to_owned(),
                    detail: format!(
                        "column_type_overrides {l} doesn't fit values of {:?}; use string or a compatible type",
                        p.value_type
                    ),
                });
            }
            Ok(ColumnPlan {
                logical: l.clone(),
                value_type: p.value_type,
                decode: p.decode,
                read_as_text: None,
                unconstrained_numeric: false,
            })
        }
        None => {
            let p = plan_or_halt(action)?;
            Ok(ColumnPlan {
                logical: canonical_logical(&p.value_type),
                value_type: p.value_type,
                decode: p.decode,
                read_as_text: p.read_as_text,
                unconstrained_numeric: p.unconstrained_numeric,
            })
        }
    }
}

/// A domain's base type, through any number of domains.
fn base_oid(oid: u32, types: &TypeCatalog) -> u32 {
    match types.get(&oid) {
        Some(t) if t.kind == b'd' => base_oid(t.base, types),
        _ => oid,
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 40.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/types.rs
git commit -m "feat(source-pg): type planning"
```

---

### Task 5b: Lossless override conversions (Q-M5-12)

**Files:**
- Create: `crates/data-avalanche-core/src/convert.rs`
- Modify: `crates/data-avalanche-core/src/lib.rs`
- Modify: `crates/data-avalanche-source-pg/src/value.rs`
- Modify: `crates/data-avalanche-source-pg/src/types.rs`

**Interfaces:**
- Consumes: `ColumnValue`, `ColumnValueType`, `Decimal`, `LogicalType` (core); `Decode`, `decode`
  (Task 4); `plan_column` (Task 5).
- Produces:
  - core: `conversion(&ColumnValueType, &LogicalType) -> Option<ColumnValueType>` (re-exported at
    the root) and `ColumnValue::convert(self, &ColumnValueType) -> ColumnValue`. The MySQL source
    (M7) plans with the same pair.
  - source-pg: `Decode::Convert { from: Box<Decode>, to: ColumnValueType }`. `plan_column` plans a
    lossless conversion where it used to halt.

Added during execution, after Task 5's review (owner decision Q-M5-12, 2026-10-01). Until now an
override other than `string` or a decimal on a numeric column had to equal the column's mapped type,
or the table halted. So `int → int64` halted, and `timestamp_ntz_as: timestamp` had no per-column
form, although config §9.3 says a per-column override takes precedence over it. Hop 2 converts
nothing (D-M3.6), so the conversion happens at Hop 1: the decoder decodes as before, then converts.

The conversions keep every value of the source type:
- `int16` → `int32`, `int64`, `float64`; `int32` → `int64`, `float64`;
- an integer → `decimal(P,S)` with room for every value: `P - S` at least 5 (`int16`), 10 (`int32`)
  or 19 (`int64`);
- `float32` → `float64`, through the shortest decimal text (`0.1` stays `0.1`, as PostgreSQL prints
  it, not `0.10000000149011612`);
- `timestamp_ntz` ↔ `timestamp_tz`: the wall clock read as UTC, as `timestamp_ntz_as: timestamp`
  does;
- `date` → `timestamp_ntz` or `timestamp_tz`: midnight. `±infinity` stays infinite, and a date
  from 294247 AD on (past the `i64` microsecond range) clamps like a timestamp there (Q-M5-1);
- `list<T>` → `list<U>`, element by element.

Anything else still halts at resolution (`SCHEMA_MISMATCH`), and `string` stays the way out. That
covers narrowing, a float to a decimal, a decimal to a float, `int64` → `float64`, `timestamp` →
`date`, and text parsed as `uuid` or `json`. A converted key column still works for snapshot chunks: the cursor
goes back to PostgreSQL as the converted value's text, cast to the column's type:
- `'5'::int4` for an `int64` value;
- a `+00` text read as a `date` or `timestamp`, which ignores the zone;
- a zone-less text read as a `timestamptz`, under the pinned `TimeZone=UTC`.

Task 12's `overrides_convert_through_stream_and_snapshot` checks this on a live server.

- [ ] **Step 1: Write the failing tests**

Create `crates/data-avalanche-core/src/convert.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use ColumnValue as C;
    use ColumnValueType as V;
    use LogicalType as L;

    fn dec(precision: u8, scale: i8) -> L {
        L::Decimal { precision, scale }
    }

    #[test]
    fn integers_widen() {
        assert_eq!(conversion(&V::Int16, &L::Int32), Some(V::Int32));
        assert_eq!(conversion(&V::Int16, &L::Int64), Some(V::Int64));
        assert_eq!(conversion(&V::Int32, &L::Int64), Some(V::Int64));
        assert_eq!(conversion(&V::Int32, &L::Float64), Some(V::Float64));
        assert_eq!(conversion(&V::Float32, &L::Float64), Some(V::Float64));
        assert_eq!(C::Int16(i16::MIN).convert(&V::Int64), C::Int64(-32_768));
        assert_eq!(
            C::Int32(i32::MAX).convert(&V::Int64),
            C::Int64(2_147_483_647)
        );
        assert_eq!(C::Int32(-7).convert(&V::Float64), C::Float64(-7.0));
        assert_eq!(C::Float32(0.1).convert(&V::Float64), C::Float64(0.1));
        assert_eq!(
            C::Float32(f32::NEG_INFINITY).convert(&V::Float64),
            C::Float64(f64::NEG_INFINITY)
        );
        assert!(matches!(
            C::Float32(f32::NAN).convert(&V::Float64),
            C::Float64(f) if f.is_nan()
        ));
    }

    #[test]
    fn integers_to_decimals_with_room() {
        assert_eq!(
            conversion(&V::Int32, &dec(12, 2)),
            Some(V::Numeric {
                precision: 12,
                scale: 2
            })
        );
        assert_eq!(
            conversion(&V::Int32, &dec(11, 2)),
            None,
            "10 digits need P - S ≥ 10"
        );
        assert!(conversion(&V::Int16, &dec(5, 0)).is_some());
        assert!(conversion(&V::Int64, &dec(19, 0)).is_some());
        assert_eq!(conversion(&V::Int64, &dec(38, 20)), None);
        let v = C::Int64(i64::MIN).convert(&V::Numeric {
            precision: 19,
            scale: 0,
        });
        assert_eq!(v, C::Numeric(Decimal::new(i64::MIN.into(), 0)));
        assert!(v.fits(&V::Numeric {
            precision: 19,
            scale: 0
        }));
        assert!(
            C::Int32(5)
                .convert(&V::Numeric {
                    precision: 12,
                    scale: 2
                })
                .fits(&V::Numeric {
                    precision: 12,
                    scale: 2
                })
        );
    }

    #[test]
    fn timestamps_and_dates() {
        let (ntz, tz) = (
            V::Timestamp { with_tz: false },
            V::Timestamp { with_tz: true },
        );
        assert_eq!(conversion(&ntz, &L::TimestampTz), Some(tz.clone()));
        assert_eq!(conversion(&tz, &L::TimestampNtz), Some(ntz.clone()));
        assert_eq!(conversion(&V::Date, &L::TimestampTz), Some(tz.clone()));
        assert_eq!(conversion(&V::Date, &L::TimestampNtz), Some(ntz.clone()));
        let at = C::Timestamp {
            micros: 1_700_000_000_000_000,
            with_tz: false,
        };
        assert_eq!(
            at.convert(&tz),
            C::Timestamp {
                micros: 1_700_000_000_000_000,
                with_tz: true
            },
            "the wall clock read as UTC"
        );
        assert_eq!(
            C::Date(19_723).convert(&ntz),
            C::Timestamp {
                micros: 19_723 * DAY_MICROS,
                with_tz: false
            }
        );
        assert_eq!(
            C::Date(-1).convert(&tz),
            C::Timestamp {
                micros: -DAY_MICROS,
                with_tz: true
            }
        );
        for (d, micros) in [
            (i32::MAX, i64::MAX),
            (i32::MIN, i64::MIN),
            (2_000_000_000, i64::MAX),
            (-2_000_000_000, i64::MIN),
        ] {
            assert_eq!(
                C::Date(d).convert(&tz),
                C::Timestamp {
                    micros,
                    with_tz: true
                },
                "±infinity and dates beyond the range clamp"
            );
        }
    }

    #[test]
    fn lists_convert_their_elements() {
        let to = conversion(&V::Array(Box::new(V::Int32)), &L::List(Box::new(L::Int64)));
        assert_eq!(to, Some(V::Array(Box::new(V::Int64))));
        let v = C::Array {
            element_type: V::Int32,
            values: vec![C::Int32(1), C::Null],
        };
        assert_eq!(
            v.convert(to.as_ref().unwrap()),
            C::Array {
                element_type: V::Int64,
                values: vec![C::Int64(1), C::Null]
            }
        );
        assert_eq!(
            conversion(&V::Array(Box::new(V::Int64)), &L::List(Box::new(L::Int32))),
            None
        );
    }

    #[test]
    fn nothing_else_converts() {
        for (from, to) in [
            (V::Int64, L::Int32),
            (V::Int32, L::Int16),
            (V::Int64, L::Float64),
            (V::Int32, L::Float32),
            (V::Float64, L::Float32),
            (V::Float64, dec(38, 10)),
            (
                V::Numeric {
                    precision: 10,
                    scale: 2,
                },
                L::Float64,
            ),
            (V::Timestamp { with_tz: false }, L::Date),
            (V::Timestamp { with_tz: false }, L::TimestampNtz),
            (V::Text, L::Uuid),
            (V::Text, L::Json),
            (V::Bool, L::Int32),
            (V::UInt64, L::Float64),
            (V::Int32, L::List(Box::new(L::Int32))),
        ] {
            assert_eq!(conversion(&from, &to), None, "{from:?} → {to}");
        }
        assert_eq!(C::Null.convert(&V::Int64), C::Null);
        assert_eq!(C::Unchanged.convert(&V::Int64), C::Unchanged);
    }
}
```

Change `crates/data-avalanche-core/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/lib.rs b/crates/data-avalanche-core/src/lib.rs
--- a/crates/data-avalanche-core/src/lib.rs
+++ b/crates/data-avalanche-core/src/lib.rs
@@ -9,6 +9,7 @@
 pub mod change_set;
 pub mod classify;
 pub mod columnar;
+pub mod convert;
 pub mod decimal;
 pub mod event;
 pub mod logical;
@@ -25,6 +26,7 @@
     LIST_ELEMENT, LOGICAL_TAG, UNCHANGED_MASK_PREFIX, added_column_field, arrow_type, column_field,
     delete_schema, lineage_fields, logical_tag, unchanged_mask_field, upsert_schema,
 };
+pub use convert::conversion;
 pub use decimal::{Decimal, DecimalError, MAX_DECIMAL_PRECISION};
 pub use event::{CdcEvent, EventShapeError, Op, SourceSeq, TransactionBoundary};
 pub use logical::{LogicalType, LogicalTypeError, MAX_LIST_DEPTH};
```

Change `crates/data-avalanche-source-pg/src/types.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/types.rs b/crates/data-avalanche-source-pg/src/types.rs
--- a/crates/data-avalanche-source-pg/src/types.rs
+++ b/crates/data-avalanche-source-pg/src/types.rs
@@ -746,6 +746,74 @@
     }
 
     #[test]
+    fn overrides_convert_losslessly() {
+        let c = catalog();
+        let col = |oid, over: LogicalType| plan_column("c", oid, -1, &c, S, Some(&over));
+        let wide = col(23, LogicalType::Int64).unwrap();
+        assert_eq!(
+            (wide.value_type, wide.logical, wide.decode),
+            (
+                V::Int64,
+                LogicalType::Int64,
+                Decode::Convert {
+                    from: Box::new(Decode::Int32),
+                    to: V::Int64
+                }
+            )
+        );
+        let tz = col(1114, LogicalType::TimestampTz).unwrap();
+        assert_eq!(tz.value_type, V::Timestamp { with_tz: true });
+        let midnight = col(1082, LogicalType::TimestampNtz).unwrap();
+        assert_eq!(midnight.value_type, V::Timestamp { with_tz: false });
+        let money = col(
+            21,
+            LogicalType::Decimal {
+                precision: 7,
+                scale: 2,
+            },
+        )
+        .unwrap();
+        assert_eq!(
+            money.value_type,
+            V::Numeric {
+                precision: 7,
+                scale: 2
+            }
+        );
+        let list = col(1007, LogicalType::List(Box::new(LogicalType::Int64))).unwrap();
+        assert_eq!(list.value_type, V::Array(Box::new(V::Int64)));
+        assert_eq!(
+            crate::value::decode(&list.decode, "{1,NULL,-2147483648}").unwrap(),
+            data_avalanche_core::ColumnValue::Array {
+                element_type: V::Int64,
+                values: vec![
+                    data_avalanche_core::ColumnValue::Int64(1),
+                    data_avalanche_core::ColumnValue::Null,
+                    data_avalanche_core::ColumnValue::Int64(-2_147_483_648),
+                ],
+            }
+        );
+        for (oid, over) in [
+            (20, LogicalType::Int32),
+            (
+                23,
+                LogicalType::Decimal {
+                    precision: 9,
+                    scale: 0,
+                },
+            ),
+            (1114, LogicalType::Date),
+            (25, LogicalType::Uuid),
+        ] {
+            assert_eq!(
+                col(oid, over).unwrap_err().code(),
+                "SCHEMA_MISMATCH",
+                "oid {oid}: lossy or parsing"
+            );
+        }
+    }
+
+    #[test]
     fn a_decimal_list_override_rescues_a_numeric_array() {
         let c = catalog();
         let list = |p, s| {
@@ -786,11 +854,11 @@
             })),
             "an array of a numeric domain"
         );
-        let e = col(1007, -1, S, &list(38, 10)).unwrap_err();
+        let e = col(1000, -1, S, &list(38, 10)).unwrap_err();
         assert_eq!(
             e.code(),
             "SCHEMA_MISMATCH",
-            "an int4[] isn't a numeric array"
-        );
-    }
-}
+            "a bool[] isn't a numeric array"
+        );
+    }
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core --lib convert::`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `convert::conversion`
error[E0432]: unresolved import `ColumnValue`
error[E0432]: unresolved import `ColumnValueType`
error[E0432]: unresolved import `LogicalType`
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-core/src/convert.rs`, above the test module:

```rust
//! The conversions a `tables.column_type_overrides` entry may ask for (config §9.3), applied by
//! the source at Hop 1: Hop 2 converts nothing (D-M3.6). Only conversions that keep every value of
//! the source type are offered; anything else is `string`, a decimal override or a halt.

use crate::{ColumnValue, ColumnValueType, Decimal, LogicalType};

/// Microseconds in a day.
const DAY_MICROS: i64 = 86_400_000_000;

/// The value type a column of `from` is converted to for an override of `to`, or `None` when no
/// conversion applies (the types already match, or the conversion could lose a value):
/// - `int16` → `int32`, `int64`, `float64`; `int32` → `int64`, `float64`;
/// - an integer → `decimal(P,S)` with room for its every value: `P - S` at least 5 (`int16`), 10
///   (`int32`) or 19 (`int64`);
/// - `float32` → `float64`;
/// - `timestamp_ntz` ↔ `timestamp_tz`: the wall clock read as UTC, as `timestamp_ntz_as:
///   timestamp` does;
/// - `date` → `timestamp_ntz` or `timestamp_tz`: midnight;
/// - `list<T>` → `list<U>`: element by element.
pub fn conversion(from: &ColumnValueType, to: &LogicalType) -> Option<ColumnValueType> {
    use ColumnValueType as V;
    use LogicalType as L;
    let digits = |t: &V| match t {
        V::Int16 => Some(5),
        V::Int32 => Some(10),
        V::Int64 => Some(19),
        _ => None,
    };
    Some(match (from, to) {
        (V::Int16, L::Int32) => V::Int32,
        (V::Int16 | V::Int32, L::Int64) => V::Int64,
        (V::Int16 | V::Int32 | V::Float32, L::Float64) => V::Float64,
        (t, L::Decimal { precision, scale })
            if digits(t).is_some_and(|d| i16::from(*precision) - i16::from(*scale) >= d) =>
        {
            V::Numeric {
                precision: *precision,
                scale: *scale,
            }
        }
        (V::Timestamp { with_tz: false }, L::TimestampTz) => V::Timestamp { with_tz: true },
        (V::Timestamp { with_tz: true }, L::TimestampNtz) => V::Timestamp { with_tz: false },
        (V::Date, L::TimestampTz) => V::Timestamp { with_tz: true },
        (V::Date, L::TimestampNtz) => V::Timestamp { with_tz: false },
        (V::Array(e), L::List(l)) => V::Array(Box::new(conversion(e, l)?)),
        _ => return None,
    })
}

impl ColumnValue {
    /// The value as type `to`, which [`conversion`] returned for the value's own type. NULL and
    /// [`ColumnValue::Unchanged`] stay as they are. A `float32` becomes the `float64` of its
    /// shortest decimal form (`0.1` stays `0.1`, as PostgreSQL prints it). A date beyond the
    /// `i64` microsecond range clamps to `i64` MAX/MIN, as a timestamp there does (Q-M5-1).
    pub fn convert(self, to: &ColumnValueType) -> ColumnValue {
        use ColumnValue as C;
        use ColumnValueType as V;
        match (self, to) {
            (C::Int16(i), V::Int32) => C::Int32(i.into()),
            (C::Int16(i), V::Int64) => C::Int64(i.into()),
            (C::Int32(i), V::Int64) => C::Int64(i.into()),
            (C::Int16(i), V::Float64) => C::Float64(i.into()),
            (C::Int32(i), V::Float64) => C::Float64(i.into()),
            (C::Float32(f), V::Float64) => C::Float64(
                f.to_string()
                    .parse()
                    .expect("an f32's text is an f64's text"),
            ),
            (C::Int16(i), V::Numeric { .. }) => C::Numeric(Decimal::new(i.into(), 0)),
            (C::Int32(i), V::Numeric { .. }) => C::Numeric(Decimal::new(i.into(), 0)),
            (C::Int64(i), V::Numeric { .. }) => C::Numeric(Decimal::new(i.into(), 0)),
            (C::Timestamp { micros, .. }, V::Timestamp { with_tz }) => C::Timestamp {
                micros,
                with_tz: *with_tz,
            },
            (C::Date(d), V::Timestamp { with_tz }) => C::Timestamp {
                micros: match d {
                    i32::MAX => i64::MAX,
                    i32::MIN => i64::MIN,
                    d => i64::from(d).checked_mul(DAY_MICROS).unwrap_or(if d < 0 {
                        i64::MIN
                    } else {
                        i64::MAX
                    }),
                },
                with_tz: *with_tz,
            },
            (C::Array { values, .. }, V::Array(e)) => C::Array {
                element_type: e.as_ref().clone(),
                values: values.into_iter().map(|v| v.convert(e)).collect(),
            },
            (v, _) => v,
        }
    }
}
```

Change `crates/data-avalanche-source-pg/src/value.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/value.rs b/crates/data-avalanche-source-pg/src/value.rs
--- a/crates/data-avalanche-source-pg/src/value.rs
+++ b/crates/data-avalanche-source-pg/src/value.rs
@@ -48,6 +48,12 @@
         element_type: ColumnValueType,
         delimiter: u8,
     },
+    /// Decoded as `from`, then converted to `to` for a `column_type_overrides` entry
+    /// ([`data_avalanche_core::conversion`]).
+    Convert {
+        from: Box<Decode>,
+        to: ColumnValueType,
+    },
 }
 
 /// Why a value can't be decoded.
@@ -119,6 +125,7 @@
                 values,
             }
         }
+        Decode::Convert { from, to } => decode(from, text)?.convert(to),
     })
 }
 
```

Change `crates/data-avalanche-source-pg/src/types.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/types.rs b/crates/data-avalanche-source-pg/src/types.rs
--- a/crates/data-avalanche-source-pg/src/types.rs
+++ b/crates/data-avalanche-source-pg/src/types.rs
@@ -8,7 +8,7 @@
 use data_avalanche_config::NumericOverflowAction;
 use data_avalanche_core::{
     ColumnValueType as V, HaltReason, LogicalType, MAX_DECIMAL_PRECISION, canonical_logical,
-    hop2_compatible,
+    conversion, hop2_compatible,
 };
 
 use crate::value::Decode;
@@ -277,8 +277,9 @@
 /// - `decimal(P,S)` on a `numeric` or `money` column decodes it as that decimal, whatever its
 ///   declared precision (the V-079 remedy), and `list<decimal(P,S)>` does the same for an array
 ///   of them;
-/// - any other override must be one Hop 2 can store the column's values as
-///   ([`hop2_compatible`]); otherwise the table halts (`SCHEMA_MISMATCH`).
+/// - any other override is either one Hop 2 can store the column's values as
+///   ([`hop2_compatible`]), or a lossless [`conversion`], which the decoder applies; otherwise the
+///   table halts (`SCHEMA_MISMATCH`).
 pub fn plan_column(
     column: &str,
     oid: u32,
@@ -318,19 +319,27 @@
         Some(l) => {
             // The `string` action plans an over-wide numeric as text; the override decides here.
             let p = plan_or_halt(NumericOverflowAction::String)?;
-            if !hop2_compatible(&p.value_type, l) {
+            let (value_type, decode) = if hop2_compatible(&p.value_type, l) {
+                (p.value_type, p.decode)
+            } else if let Some(to) = conversion(&p.value_type, l) {
+                let decode = Decode::Convert {
+                    from: Box::new(p.decode),
+                    to: to.clone(),
+                };
+                (to, decode)
+            } else {
                 return Err(HaltReason::SchemaMismatch {
                     column: column.to_owned(),
                     detail: format!(
-                        "column_type_overrides {l} doesn't fit values of {:?}; use string or a compatible type",
+                        "column_type_overrides {l} doesn't fit values of {:?}; use string, a compatible type or a lossless conversion",
                         p.value_type
                     ),
                 });
-            }
+            };
             Ok(ColumnPlan {
                 logical: l.clone(),
-                value_type: p.value_type,
-                decode: p.decode,
+                value_type,
+                decode,
                 read_as_text: None,
                 unconstrained_numeric: false,
             })
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core --lib convert::`

Expected: PASS. `data-avalanche-core` `convert` tests: 5.

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 43.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core/src/convert.rs crates/data-avalanche-core/src/lib.rs \
  crates/data-avalanche-source-pg/src/value.rs crates/data-avalanche-source-pg/src/types.rs
git commit -m "feat(core): lossless override conversions, applied by the PostgreSQL decoder"
```

---

### Task 6: Catalog reads and table selection (channel A, pure half)

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/catalog.rs`

**Interfaces:**
- Consumes: `plan_column`, `TypeCatalog`, `TypeInfo` (Task 5); `Tables` (config); `TableSchema`,
  `ColumnSchema` (core); `test_support` (Task 2).
- Produces (`catalog` is public):
  - `glob_match`, `pattern_matches(&str, &TableRef)`.
  - `Relation { oid, table, kind }`, `Selection { tables: Vec<(Relation, bool /* explicit */)>,
    skipped, warnings }`, `select(include, exclude, &[Relation]) -> Selection`.
  - `Attribute { attnum, name, type_oid, typmod, not_null, type_text }`, `Described { oid, table,
    replica_identity, columns, primary_key, identity_index }`.
  - `table_key(&TableRef) -> String`, `Notes { read_as_text, unconstrained }`,
    `table_schema(&Described, &Tables, &TypeCatalog) -> Result<(TableSchema, Notes), HaltReason>`.
  - Queries: `relations(&Client)`, `oids_by_name(&Client, &[TableRef])`, `describe(&Client, &[u32])
    -> BTreeMap<u32, Described>`, `load_types(&Client, oids, &mut TypeCatalog)`.

D-M5.11, D-M5.12, Q-M5-5. The selection and the schema building are pure and unit-tested
here. The queries run against a server from Task 7 on. Partitioned parents (`relkind = 'p'`) are
skipped with a warning; their leaves replicate as tables (Q-M5-5). Generated columns are left out:
pgoutput doesn't send them. The identifier is the override, else the primary key. It must lie
within the replica identity, or the table halts.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-source-pg/src/lib.rs` with:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

pub mod catalog;
#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod connect;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod error;
pub mod pgoutput;
#[cfg(test)]
mod test_support;
#[allow(dead_code)] // `PgConnector` (Task 7) is the caller
mod tls;
pub mod types;
pub mod value;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
```

Create `crates/data-avalanche-source-pg/src/catalog.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::pipeline_with;
    use data_avalanche_core::ColumnValueType as V;

    fn rel(oid: u32, s: &str, t: &str, kind: u8) -> Relation {
        Relation {
            oid,
            table: TableRef::new(s, t),
            kind,
        }
    }

    #[test]
    fn globs_stay_inside_a_name() {
        assert!(glob_match("*", "orders"));
        assert!(glob_match("ord*", "orders"));
        assert!(glob_match("*ers", "orders"));
        assert!(glob_match("o*r*s", "orders"));
        assert!(!glob_match("Orders", "orders"), "case-sensitive");
        assert!(!glob_match("ord", "orders"));
        let t = TableRef::new("public", "orders");
        assert!(pattern_matches("public.*", &t));
        assert!(pattern_matches("*.orders", &t));
        assert!(!pattern_matches("*", &t), "a pattern needs its schema part");
        assert!(
            !pattern_matches("pub*orders", &t),
            "`*` doesn't cross the dot"
        );
    }

    #[test]
    fn selection() {
        let relations = [
            rel(1, "public", "orders", b'r'),
            rel(2, "public", "order_items", b'r'),
            rel(3, "public", "events", b'p'),
            rel(4, "public", "events_2024", b'r'),
            rel(5, "audit", "orders", b'r'),
            rel(6, "public", "tmp_x", b'r'),
        ];
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let sel = select(
            &s(&["public.*", "audit.orders", "public.missing"]),
            &s(&["public.tmp_*"]),
            &relations,
        );
        let got: Vec<(u32, bool)> = sel.tables.iter().map(|(r, e)| (r.oid, *e)).collect();
        assert_eq!(got, vec![(1, false), (2, false), (4, false), (5, true)]);
        assert_eq!(sel.skipped.len(), 1);
        assert_eq!(sel.skipped[0].0, TableRef::new("public", "events"));
        assert_eq!(
            sel.warnings,
            vec!["tables.include public.missing matches no table"]
        );
        let explicit = select(&s(&["public.*", "public.orders"]), &[], &relations[..1]);
        assert!(explicit.tables[0].1, "an exact name wins over a glob");
    }

    fn attr(
        attnum: i16,
        name: &str,
        type_oid: u32,
        typmod: i32,
        not_null: bool,
        text: &str,
    ) -> Attribute {
        Attribute {
            attnum,
            name: name.into(),
            type_oid,
            typmod,
            not_null,
            type_text: text.into(),
        }
    }

    fn orders(replica_identity: u8) -> Described {
        Described {
            oid: 1,
            table: TableRef::new("public", "orders"),
            replica_identity,
            columns: vec![
                attr(1, "id", 20, -1, true, "bigint"),
                attr(2, "code", 25, -1, true, "text"),
                attr(4, "amount", 1700, -1, false, "numeric"),
                attr(5, "at", 1184, -1, false, "timestamp with time zone"),
            ],
            primary_key: vec!["id".into()],
            identity_index: vec!["code".into()],
        }
    }

    fn schema_of(d: &Described, tables: &str) -> Result<(TableSchema, Notes), HaltReason> {
        table_schema(
            d,
            &pipeline_with("public.orders", tables).tables,
            &TypeCatalog::new(),
        )
    }

    #[test]
    fn a_table_schema_from_the_catalog() {
        let (s, notes) = schema_of(&orders(b'd'), "").unwrap();
        assert_eq!(s.identifier(), ["id"]);
        let names: Vec<&str> = s.columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "code", "amount", "at"]);
        let at = s.column("at").unwrap();
        assert_eq!(at.value_type, V::Timestamp { with_tz: true });
        assert_eq!(at.source_ordinal, Some(5), "attnum, gaps and all");
        assert!(at.nullable);
        assert!(!s.column("id").unwrap().nullable);
        assert_eq!(
            s.column("amount").unwrap().value_type,
            V::Text,
            "bare numeric under `string`"
        );
        assert_eq!(notes.unconstrained, ["amount"]);
    }

    #[test]
    fn identifiers_and_replica_identity() {
        // The identifier must be inside the replica identity.
        assert_eq!(
            schema_of(&orders(b'n'), "").unwrap_err(),
            HaltReason::IdentifierNotInReplicaIdentity {
                missing: vec!["id".into()]
            }
        );
        assert!(
            schema_of(&orders(b'f'), "").is_ok(),
            "FULL holds every column"
        );
        // REPLICA IDENTITY USING INDEX on `code` and the override naming it.
        let over = "      primary_key: { public.orders: [code] }";
        assert_eq!(
            schema_of(&orders(b'i'), over).unwrap().0.identifier(),
            ["code"]
        );
        assert_eq!(
            schema_of(&orders(b'i'), "").unwrap_err(),
            HaltReason::IdentifierNotInReplicaIdentity {
                missing: vec!["id".into()]
            }
        );
        // No primary key, no override.
        let mut no_pk = orders(b'f');
        no_pk.primary_key.clear();
        assert_eq!(schema_of(&no_pk, "").unwrap_err(), HaltReason::NoIdentifier);
        // An override on a nullable column.
        let e = schema_of(
            &orders(b'f'),
            "      primary_key: { public.orders: [amount] }",
        )
        .unwrap_err();
        assert_eq!(e.code(), "IDENTIFIER_NOT_USABLE");
    }

    #[test]
    fn overrides_and_numeric_policy() {
        let t = "      column_type_overrides: { public.orders.amount: \"decimal(38,10)\", public.orders.code: string }";
        let (s, notes) = schema_of(&orders(b'd'), t).unwrap();
        assert_eq!(
            s.column("amount").unwrap().value_type,
            V::Numeric {
                precision: 38,
                scale: 10
            }
        );
        assert!(notes.unconstrained.is_empty());
        let e = schema_of(
            &orders(b'd'),
            "      column_type_overrides: { public.orders.at: int64 }",
        )
        .unwrap_err();
        assert_eq!(e.code(), "SCHEMA_MISMATCH");
        let (s, _) = schema_of(&orders(b'd'), "      numeric_overflow_action: error").unwrap();
        assert_eq!(
            s.column("amount").unwrap().value_type,
            V::Numeric {
                precision: 38,
                scale: 18
            }
        );
    }

    #[test]
    fn reserved_names_halt() {
        let mut d = orders(b'd');
        d.columns.push(attr(6, "_OP", 25, -1, false, "text"));
        assert_eq!(
            schema_of(&d, "").unwrap_err().code(),
            "RESERVED_COLUMN_COLLISION"
        );
    }

    #[test]
    fn partition_columns() {
        use data_avalanche_config::PartitionField;
        // Built directly: a Delta pipeline's config refuses temporal transforms (V-086), but the
        // start check is sink-neutral.
        let check = |column: &str, transform| {
            let mut tables = pipeline_with("public.orders", "").tables;
            tables.partitioning.insert(
                "public.orders".into(),
                vec![PartitionField {
                    column: column.into(),
                    transform,
                }],
            );
            table_schema(&orders(b'd'), &tables, &TypeCatalog::new()).map(|_| ())
        };
        use PartitionTransform as P;
        assert!(check("code", P::Identity).is_ok());
        assert!(check("at", P::Day).is_ok());
        assert!(check("_ts", P::Hour).is_ok());
        assert_eq!(
            check("nope", P::Identity).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
        let not_temporal = check("code", P::Month).unwrap_err();
        assert!(
            not_temporal.to_string().contains("date or timestamp"),
            "{not_temporal}"
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `PartitionTransform`
error[E0425]: cannot find type `Relation` in this scope
error[E0422]: cannot find struct, variant or union type `Relation` in this scope
error[E0425]: cannot find type `Attribute` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-source-pg/src/catalog.rs`, above the test module:

```rust
//! Catalog reads (`Schema_Change…` §3 channels A, C, D; config §11.2): which tables the globs
//! select, their columns, identifier and replica identity, and the `pg_type` rows the planner needs.

use std::collections::{BTreeMap, HashMap};

use data_avalanche_config::{PartitionTransform, Tables};
use data_avalanche_core::{ColumnSchema, HaltReason, LogicalType, TableRef, TableSchema};
use tokio_postgres::Client;

use crate::types::{TypeCatalog, TypeInfo, missing, plan_column};

/// `*` matches within one name (it doesn't cross the `.`); matching is case-sensitive
/// (config §9.3 glob grammar).
pub fn glob_match(pattern: &str, name: &str) -> bool {
    fn go(p: &[u8], n: &[u8]) -> bool {
        match p.split_first() {
            None => n.is_empty(),
            Some((b'*', rest)) => (0..=n.len()).any(|i| go(rest, &n[i..])),
            Some((c, rest)) => n.first() == Some(c) && go(rest, &n[1..]),
        }
    }
    go(pattern.as_bytes(), name.as_bytes())
}

/// A PostgreSQL include/exclude pattern (`schema.table`, V-117) against a table.
pub fn pattern_matches(pattern: &str, table: &TableRef) -> bool {
    match pattern.split_once('.') {
        Some((s, t)) => glob_match(s, &table.schema) && glob_match(t, &table.name),
        None => false,
    }
}

/// A `pg_class` row the globs can select.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub oid: u32,
    pub table: TableRef,
    /// `r` table (including a leaf partition), `p` partitioned table.
    pub kind: u8,
}

/// What the globs selected.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selection {
    /// Tables to describe; `true` when a pattern without `*` named the table.
    pub tables: Vec<(Relation, bool)>,
    pub skipped: Vec<(TableRef, String)>,
    pub warnings: Vec<String>,
}

/// Applies `include` minus `exclude` to the catalog's tables. A partitioned parent isn't a table
/// of its own (Q-M5-5): it is skipped with a warning naming its partitions as the way to track
/// it. An include pattern that matches nothing warns.
pub fn select(include: &[String], exclude: &[String], relations: &[Relation]) -> Selection {
    let mut out = Selection::default();
    for p in include {
        if !relations.iter().any(|r| pattern_matches(p, &r.table)) {
            out.warnings
                .push(format!("tables.include {p} matches no table"));
        }
    }
    for r in relations {
        let named = include
            .iter()
            .filter(|p| pattern_matches(p, &r.table))
            .map(|p| !p.contains('*'))
            .reduce(|a, b| a || b);
        let Some(explicit) = named else { continue };
        if exclude.iter().any(|p| pattern_matches(p, &r.table)) {
            continue;
        }
        if r.kind == b'p' {
            out.skipped.push((
                r.table.clone(),
                "a partitioned table: its changes arrive under its partitions; include those"
                    .into(),
            ));
            continue;
        }
        out.tables.push((r.clone(), explicit));
    }
    out
}

/// One live, stored column (`attnum > 0`, not dropped, not generated: generated columns aren't in
/// the stream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    pub attnum: i16,
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    pub not_null: bool,
    /// `format_type(atttypid, atttypmod)`.
    pub type_text: String,
}

/// A table as the catalog describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Described {
    pub oid: u32,
    pub table: TableRef,
    /// `relreplident`: `d` default (the primary key), `n` nothing, `f` full, `i` index.
    pub replica_identity: u8,
    pub columns: Vec<Attribute>,
    pub primary_key: Vec<String>,
    /// The columns of the `REPLICA IDENTITY USING INDEX` index.
    pub identity_index: Vec<String>,
}

/// The table's config keys: `schema.table` (config §9.3).
pub fn table_key(t: &TableRef) -> String {
    format!("{}.{}", t.schema, t.name)
}

/// What building a schema noticed that isn't a halt.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Notes {
    /// Columns of types kept as text with a warning: `(column, type)`.
    pub read_as_text: Vec<(String, String)>,
    /// `numeric` columns without a declared precision (V-079).
    pub unconstrained: Vec<String>,
}

/// The replica identity's columns.
fn identity_columns(d: &Described) -> Vec<&str> {
    match d.replica_identity {
        b'f' => d.columns.iter().map(|c| c.name.as_str()).collect(),
        b'i' => d.identity_index.iter().map(String::as_str).collect(),
        b'd' => d.primary_key.iter().map(String::as_str).collect(),
        _ => vec![],
    }
}

/// A table's schema, checked in this order (config §11.2): every column plans
/// ([`plan_column`]: overrides, numeric caps); the identifier (the `tables.primary_key` entry,
/// else the primary key) passes [`TableSchema::new`] (reserved names, a non-empty identifier of
/// usable columns); the identifier is inside the REPLICA IDENTITY; partition columns exist and
/// temporal transforms name temporal columns.
pub fn table_schema(
    d: &Described,
    tables: &Tables,
    types: &TypeCatalog,
) -> Result<(TableSchema, Notes), HaltReason> {
    let key = table_key(&d.table);
    let mut notes = Notes::default();
    let mut columns = Vec::with_capacity(d.columns.len());
    for a in &d.columns {
        let over = tables
            .column_type_overrides
            .get(&format!("{key}.{}", a.name));
        let p = plan_column(
            &a.name,
            a.type_oid,
            a.typmod,
            types,
            tables.numeric_overflow_action,
            over,
        )?;
        if let Some(t) = p.read_as_text {
            notes.read_as_text.push((a.name.clone(), t));
        }
        if p.unconstrained_numeric {
            notes.unconstrained.push(a.name.clone());
        }
        columns.push(ColumnSchema {
            name: a.name.clone(),
            value_type: p.value_type,
            logical: p.logical,
            nullable: !a.not_null,
            source_type: a.type_text.clone(),
            source_ordinal: Some(i32::from(a.attnum)),
        });
    }
    let identifier = tables
        .primary_key
        .get(&key)
        .cloned()
        .unwrap_or_else(|| d.primary_key.clone());
    let schema = TableSchema::new(d.table.clone(), columns, identifier)?;
    let identity = identity_columns(d);
    let missing: Vec<String> = schema
        .identifier()
        .iter()
        .filter(|c| !identity.contains(&c.as_str()))
        .cloned()
        .collect();
    if !missing.is_empty() {
        return Err(HaltReason::IdentifierNotInReplicaIdentity { missing });
    }
    for f in tables.partitioning.get(&key).into_iter().flatten() {
        let temporal = !matches!(
            f.transform,
            PartitionTransform::Identity
                | PartitionTransform::Bucket { .. }
                | PartitionTransform::Truncate { .. }
        );
        if f.column == "_ts" {
            continue;
        }
        let Some(c) = schema.column(&f.column) else {
            return Err(HaltReason::SchemaMismatch {
                column: f.column.clone(),
                detail: "tables.partitioning names a column the table doesn't have".into(),
            });
        };
        if temporal
            && !matches!(
                c.logical,
                LogicalType::Date | LogicalType::TimestampTz | LogicalType::TimestampNtz
            )
        {
            return Err(HaltReason::SchemaMismatch {
                column: f.column.clone(),
                detail: format!(
                    "a {:?} partition transform needs a date or timestamp column",
                    f.transform
                ),
            });
        }
    }
    Ok((schema, notes))
}

/// The tables the globs can select: plain and partitioned tables outside the system schemas.
pub async fn relations(client: &Client) -> Result<Vec<Relation>, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT c.oid, n.nspname, c.relname, c.relkind::text
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relkind IN ('r', 'p')
                AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
              ORDER BY n.nspname, c.relname",
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| Relation {
            oid: r.get(0),
            table: TableRef::new(r.get::<_, String>(1), r.get::<_, String>(2)),
            kind: r.get::<_, String>(3).as_bytes()[0],
        })
        .collect())
}

/// The OIDs of `tables` that still exist, by name.
pub async fn oids_by_name(
    client: &Client,
    tables: &[TableRef],
) -> Result<HashMap<TableRef, u32>, tokio_postgres::Error> {
    let schemas: Vec<&str> = tables.iter().map(|t| t.schema.as_str()).collect();
    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    let rows = client
        .query(
            "SELECT c.oid, n.nspname, c.relname
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               JOIN unnest($1::text[], $2::text[]) AS w(s, t) ON w.s = n.nspname AND w.t = c.relname
              WHERE c.relkind IN ('r', 'p')",
            &[&schemas, &names],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                TableRef::new(r.get::<_, String>(1), r.get::<_, String>(2)),
                r.get(0),
            )
        })
        .collect())
}

/// Describes the tables with these OIDs in three batched queries. An OID with no table is left
/// out (the table was dropped).
pub async fn describe(
    client: &Client,
    oids: &[u32],
) -> Result<BTreeMap<u32, Described>, tokio_postgres::Error> {
    let mut out: BTreeMap<u32, Described> = BTreeMap::new();
    for r in client
        .query(
            "SELECT c.oid, n.nspname, c.relname, c.relreplident::text
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.oid = ANY($1)",
            &[&oids],
        )
        .await?
    {
        let oid: u32 = r.get(0);
        out.insert(
            oid,
            Described {
                oid,
                table: TableRef::new(r.get::<_, String>(1), r.get::<_, String>(2)),
                replica_identity: r.get::<_, String>(3).as_bytes()[0],
                columns: vec![],
                primary_key: vec![],
                identity_index: vec![],
            },
        );
    }
    for r in client
        .query(
            "SELECT a.attrelid, a.attnum, a.attname, a.atttypid, a.atttypmod, a.attnotnull,
                    pg_catalog.format_type(a.atttypid, a.atttypmod)
               FROM pg_catalog.pg_attribute a
              WHERE a.attrelid = ANY($1) AND a.attnum > 0 AND NOT a.attisdropped
                AND a.attgenerated = ''
              ORDER BY a.attrelid, a.attnum",
            &[&oids],
        )
        .await?
    {
        if let Some(d) = out.get_mut(&r.get::<_, u32>(0)) {
            d.columns.push(Attribute {
                attnum: r.get(1),
                name: r.get(2),
                type_oid: r.get(3),
                typmod: r.get(4),
                not_null: r.get(5),
                type_text: r.get(6),
            });
        }
    }
    for r in client
        .query(
            "SELECT i.indrelid, i.indisprimary, i.indisreplident,
                    ARRAY(SELECT a.attname
                            FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                            JOIN pg_catalog.pg_attribute a
                              ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                           ORDER BY k.ord)
               FROM pg_catalog.pg_index i
              WHERE i.indrelid = ANY($1) AND (i.indisprimary OR i.indisreplident)",
            &[&oids],
        )
        .await?
    {
        if let Some(d) = out.get_mut(&r.get::<_, u32>(0)) {
            let columns: Vec<String> = r.get(3);
            if r.get::<_, bool>(1) {
                d.primary_key = columns.clone();
            }
            if r.get::<_, bool>(2) {
                d.identity_index = columns;
            }
        }
    }
    Ok(out)
}

/// Adds the `pg_type` rows of `oids` and of everything they refer to (array elements, domain
/// bases) to `types`. Only types the planner doesn't know by OID are fetched.
pub async fn load_types(
    client: &Client,
    oids: impl IntoIterator<Item = u32>,
    types: &mut TypeCatalog,
) -> Result<(), tokio_postgres::Error> {
    let mut wanted: Vec<u32> = oids.into_iter().filter_map(|o| missing(o, types)).collect();
    wanted.sort_unstable();
    wanted.dedup();
    if wanted.is_empty() {
        return Ok(());
    }
    for r in client
        .query(
            "WITH RECURSIVE t(oid) AS (
                 SELECT unnest($1::oid[])
                 UNION
                 SELECT v.x FROM pg_catalog.pg_type p JOIN t ON p.oid = t.oid,
                        LATERAL (VALUES (p.typelem), (p.typbasetype)) AS v(x)
                  WHERE v.x <> 0)
             SELECT p.oid, p.typname, n.nspname, p.typtype::text, p.typcategory::text,
                    p.typelem, p.typbasetype, p.typtypmod, p.typdelim::text
               FROM pg_catalog.pg_type p
               JOIN pg_catalog.pg_namespace n ON n.oid = p.typnamespace
              WHERE p.oid IN (SELECT oid FROM t)",
            &[&wanted],
        )
        .await?
    {
        let info = TypeInfo {
            oid: r.get(0),
            name: r.get(1),
            schema: r.get(2),
            kind: r.get::<_, String>(3).as_bytes()[0],
            category: r.get::<_, String>(4).as_bytes()[0],
            element: r.get(5),
            base: r.get(6),
            base_typmod: r.get(7),
            delimiter: r.get::<_, String>(8).as_bytes()[0],
        };
        types.insert(info.oid, info);
    }
    Ok(())
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 47.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/catalog.rs
git commit -m "feat(source-pg): catalog reads and table selection"
```

---

### Task 7: `PgConnector`: preflight and table resolution, against PostgreSQL

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/position.rs`
- Create: `crates/data-avalanche-source-pg/src/connector.rs`
- Create: `crates/data-avalanche-source-pg/tests/pg/main.rs`
- Create: `crates/data-avalanche-source-pg/tests/pg/common.rs`
- Create: `crates/data-avalanche-source-pg/tests/pg/resolve.rs`

**Interfaces:**
- Consumes: Tasks 2, 5 and 6; `check_table_set`, `check_unbounded_numeric` (config).
- Produces:
  - `position`: `parse_lsn(&str) -> Result<Lsn, SourceError>`, `lsn_of(&SourcePosition)`,
    `to_wire(Lsn)`, `from_wire(pgwire_replication::Lsn)`.
  - Crate root: `MIN_SERVER_VERSION = 140_000`; `PgConnector { pipeline, pg, conn, types }` (fields
    `pub(crate)`) with `new(&Pipeline) -> Result<Self, SourceError>`, `preflight()`,
    `resolve_tables() -> ResolvedTables`, `current_position()`, and `pub(crate) schemas(&[u32]) ->
    BTreeMap<u32, (Described, Result<(TableSchema, Notes), HaltReason>)>`.
  - The integration binary `tests/pg`: `common::{port, admin, Names, fresh, pipeline, connector,
    cleanup, resolved}`. `port()` reads `DATA_AVALANCHE_PG_PORT` (default 55418, `postgres-18`).

D-M5.8, D-M5.11, D-M5.12. From here, each task also runs its integration tests against
`postgres-18` (`test-infra/up.sh`). Each test owns a schema, a slot and a publication named after
it and drops them at its start, so a failed run leaves nothing that breaks the next. Without
`DATA_AVALANCHE_IT` the tests return at once, and plain `cargo test` stays hermetic.

Some code has no caller yet, so it carries `#[allow(dead_code)]` until its caller lands:
- `connect`, `error` and `position` until the stream (Task 9);
- `PgConnector.pg` until Task 11 rewrites the file's head (the slot lifecycle reads it from Task 8).

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-source-pg/src/lib.rs` with:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

pub mod catalog;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod connect;
mod connector;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod error;
pub mod pgoutput;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod position;
#[cfg(test)]
mod test_support;
mod tls;
pub mod types;
pub mod value;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
pub use connector::{MIN_SERVER_VERSION, PgConnector};
```

Create `crates/data-avalanche-source-pg/src/position.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsns() {
        assert_eq!(parse_lsn("16/B374D848").unwrap(), Lsn(0x16_B374_D848));
        assert!(parse_lsn("x").is_err());
        assert_eq!(lsn_of(&SourcePosition::Pg(Lsn(7))).unwrap(), Lsn(7));
        let mysql = SourcePosition::parse_tagged("binlog:mysql-bin.000001:4").unwrap();
        assert!(lsn_of(&mysql).is_err());
        assert_eq!(from_wire(to_wire(Lsn(42))), Lsn(42));
    }
}
```

Create `crates/data-avalanche-source-pg/src/connector.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::pipeline;

    #[test]
    fn only_postgres_pipelines() {
        let mut p = pipeline("public.t");
        assert!(PgConnector::new(&p).is_ok());
        p.source.postgres = None;
        let e = PgConnector::new(&p).unwrap_err();
        assert_eq!(
            e,
            SourceError::Permanent("pipeline p: the source isn't PostgreSQL".into())
        );
    }
}
```

Create `crates/data-avalanche-source-pg/tests/pg/main.rs`:

```rust
//! The PostgreSQL source against a live server (`test-infra/compose.yaml`): `postgres-18` on every
//! change, PostgreSQL 14–17 nightly (`DATA_AVALANCHE_PG_PORT`). Each test returns at once without
//! `DATA_AVALANCHE_IT`.

mod common;
mod resolve;
```

Create `crates/data-avalanche-source-pg/tests/pg/common.rs`:

```rust
//! Shared fixtures: each test owns a schema `it_<test>`, a slot `it_<test>` and a publication
//! `it_<test>_pub`, dropped and re-created at its start, so a failed run leaves nothing that
//! breaks the next one.

use std::collections::BTreeMap;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::TableRef;
use data_avalanche_source_pg::PgConnector;
use tokio_postgres::{Client, NoTls};

/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418). The
/// nightly matrix sets it for PostgreSQL 14–17.
pub fn port() -> u16 {
    std::env::var("DATA_AVALANCHE_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55418)
}

/// A superuser connection for test setup.
pub async fn admin() -> Client {
    let dsn = format!(
        "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
        port()
    );
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect to test PostgreSQL");
    tokio::spawn(conn);
    client
}

/// The test's names: schema and slot `it_<test>`, publication `it_<test>_pub`.
pub struct Names {
    pub schema: String,
    pub slot: String,
    pub publication: String,
}

impl Names {
    pub fn new(test: &str) -> Self {
        Self {
            schema: format!("it_{test}"),
            slot: format!("it_{test}"),
            publication: format!("it_{test}_pub"),
        }
    }

    pub fn table(&self, name: &str) -> TableRef {
        TableRef::new(&self.schema, name)
    }
}

/// Drops what an earlier run of `test` left (its slot, publication and schema) and creates the
/// schema again, then runs `ddl` in it.
pub async fn fresh(admin: &Client, test: &str, ddl: &str) -> Names {
    let n = Names::new(test);
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&n.slot],
        )
        .await
        .expect("drop the test slot");
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

/// A pipeline on the test server reading `include`, with more `tables` keys (YAML, indented six
/// spaces) and more `postgres` keys (YAML flow entries).
pub fn pipeline(n: &Names, include: &[&str], tables: &str, postgres: &str) -> Pipeline {
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
      postgres: {{ publication: {publication}, slot: {slot}, heartbeat: 1s{postgres} }}
    tables:
      include: [{include}]
{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#,
        port = port(),
        publication = n.publication,
        slot = n.slot,
        include = include.join(", "),
    );
    let loaded = load_str(
        &yaml,
        &BTreeMap::new(),
        &FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"));
    loaded
        .config
        .pipelines
        .into_values()
        .next()
        .expect("one pipeline")
}

pub fn connector(p: &Pipeline) -> PgConnector {
    data_avalanche_testkit::install_crypto_provider();
    PgConnector::new(p).expect("connector")
}

/// The test's slot, publication and schema, dropped at the end of a passing test.
pub async fn cleanup(admin: &Client, n: &Names) {
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&n.slot],
        )
        .await
        .expect("drop the test slot");
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {}; DROP SCHEMA IF EXISTS {} CASCADE;",
            n.publication, n.schema
        ))
        .await
        .expect("drop the test schema");
}

/// `connector`'s resolved tables, by name.
pub async fn resolved(c: &PgConnector) -> data_avalanche_core::ResolvedTables {
    c.resolve_tables().await.expect("resolve tables")
}
```

Create `crates/data-avalanche-source-pg/tests/pg/resolve.rs`:

```rust
//! Preflight and channel A: the globs against a live catalog (config §11.2).

use data_avalanche_core::{ColumnValueType as V, HaltReason, LogicalType, SourcePosition};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, connector, fresh, pipeline, resolved};

#[tokio::test]
async fn preflight_accepts_the_test_server() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "preflight", "").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let p = c.preflight().await.expect("preflight");
    assert!(!p.server_version.is_empty());
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn preflight_refuses_a_user_who_cant_log_in() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "preflight_auth", "").await;
    let mut p = pipeline(&n, &["*"], "", "");
    p.source.credentials = data_avalanche_config::Credentials::Inline {
        user: "nobody_here".into(),
        password: data_avalanche_config::Secret::new(
            "x",
            &data_avalanche_config::FingerprintKey::from_bytes([1; 32]),
        ),
    };
    let e = connector(&p).preflight().await.unwrap_err();
    assert!(
        matches!(&e, data_avalanche_core::SourceError::Permanent(m) if m.contains("28")),
        "an authentication failure is permanent: {e:?}"
    );
    cleanup(&a, &n).await;
}

const DDL: &str = "
    CREATE TYPE mood AS ENUM ('sad', 'ok');
    CREATE DOMAIN price AS numeric(10,2);
    CREATE TABLE orders (id bigint PRIMARY KEY, gone int, note text, amount numeric(10,2),
                         at timestamptz);
    ALTER TABLE orders DROP COLUMN gone;
    CREATE TABLE typed (id int PRIMARY KEY, m mood, p price, tags text[], ms mood[], wide numeric,
                        doubled int GENERATED ALWAYS AS (id * 2) STORED);
    CREATE TABLE nopk (a int);
    CREATE TABLE nopk_named (a int);
    CREATE TABLE nothing (id int PRIMARY KEY);
    ALTER TABLE nothing REPLICA IDENTITY NOTHING;
    CREATE TABLE by_index (id int PRIMARY KEY, code text NOT NULL UNIQUE);
    ALTER TABLE by_index REPLICA IDENTITY USING INDEX by_index_code_key;
    CREATE TABLE reserved (id int PRIMARY KEY, \"_OP\" text);
    CREATE TABLE events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
    CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
    CREATE TABLE tmp_x (id int PRIMARY KEY);
";

#[tokio::test]
async fn channel_a_resolves_checks_and_skips() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "resolve", DDL).await;
    let s = &n.schema;
    let tables =
        format!("      exclude: [{s}.tmp_*]\n      primary_key: {{ {s}.by_index: [code] }}");
    let c = connector(&pipeline(&n, &["*", "nopk_named", "missing"], &tables, ""));
    let r = resolved(&c).await;

    let names: Vec<&str> = r.tables.iter().map(|t| t.table().name.as_str()).collect();
    assert_eq!(names, ["by_index", "events_2024", "orders", "typed"]);
    let orders = &r.tables[2];
    let columns: Vec<(&str, Option<i32>)> = orders
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), c.source_ordinal))
        .collect();
    assert_eq!(
        columns,
        [
            ("id", Some(1)),
            ("note", Some(3)),
            ("amount", Some(4)),
            ("at", Some(5))
        ],
        "the dropped column is gone, its attnum isn't reused"
    );
    assert_eq!(
        orders.column("amount").unwrap().source_type,
        "numeric(10,2)"
    );
    assert_eq!(
        orders.column("at").unwrap().value_type,
        V::Timestamp { with_tz: true }
    );
    assert_eq!(
        r.tables[0].identifier(),
        ["code"],
        "REPLICA IDENTITY USING INDEX + override"
    );
    assert_eq!(
        r.tables[1].identifier(),
        ["id", "at"],
        "a leaf partition is a table"
    );

    let typed = &r.tables[3];
    let t: Vec<(&str, &V)> = typed
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), &c.value_type))
        .collect();
    assert_eq!(
        t,
        [
            ("id", &V::Int32),
            ("m", &V::Text),
            (
                "p",
                &V::Numeric {
                    precision: 10,
                    scale: 2
                }
            ),
            ("tags", &V::Array(Box::new(V::Text))),
            ("ms", &V::Array(Box::new(V::Text))),
            ("wide", &V::Text),
        ],
        "generated columns aren't in the stream, so they aren't in the schema"
    );
    assert_eq!(typed.column("wide").unwrap().logical, LogicalType::String);
    let wide = format!("{s}.typed.wide: numeric is read as text");
    assert!(r.warnings.contains(&wide), "{:?}", r.warnings);
    let missing = format!("tables.include {s}.missing matches no table");
    assert!(r.warnings.contains(&missing), "{:?}", r.warnings);

    let halted: Vec<(&str, &str)> = r
        .halted
        .iter()
        .map(|(t, h)| (t.name.as_str(), h.code()))
        .collect();
    assert_eq!(
        halted,
        [
            ("nopk_named", "NO_IDENTIFIER"),
            ("nothing", "IDENTIFIER_NOT_IN_REPLICA_IDENTITY"),
            ("reserved", "RESERVED_COLUMN_COLLISION"),
        ]
    );
    assert_eq!(
        r.halted[1].1,
        HaltReason::IdentifierNotInReplicaIdentity {
            missing: vec!["id".into()]
        }
    );
    let skipped: Vec<&str> = r.skipped.iter().map(|(t, _)| t.name.as_str()).collect();
    assert_eq!(
        skipped,
        ["events", "nopk"],
        "a partitioned parent and a glob-matched PK-less table"
    );
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn unconstrained_numerics_under_error_warn_v079() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "v079",
        "CREATE TABLE t (id int PRIMARY KEY, n numeric, w numeric(50,2));",
    )
    .await;
    let c = connector(&pipeline(
        &n,
        &["*"],
        "      numeric_overflow_action: error",
        "",
    ));
    let r = resolved(&c).await;
    assert!(r.tables.is_empty());
    assert_eq!(
        r.halted,
        [(
            n.table("t"),
            HaltReason::NumericOverflow { column: "w".into() }
        )],
        "numeric(50,2) can't be a decimal: the table halts"
    );
    let c = connector(&pipeline(
        &n,
        &["*"],
        &format!(
            "      numeric_overflow_action: error\n      column_type_overrides: {{ {}.t.w: \"decimal(38,2)\" }}",
            n.schema
        ),
        "",
    ));
    let r = resolved(&c).await;
    assert_eq!(
        r.tables[0].column("n").unwrap().value_type,
        V::Numeric {
            precision: 38,
            scale: 18
        }
    );
    assert!(
        r.warnings.iter().any(|w| w.starts_with("V-079")),
        "{:?}",
        r.warnings
    );
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn the_current_position_moves_with_writes() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "position", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let before = c.current_position().await.unwrap();
    a.execute(&format!("INSERT INTO {}.t VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    let after = c.current_position().await.unwrap();
    let (SourcePosition::Pg(b), SourcePosition::Pg(f)) = (before, after) else {
        panic!("PostgreSQL positions")
    };
    assert!(f > b, "{f} after {b}");
    cleanup(&a, &n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `connector::MIN_SERVER_VERSION`, `connector::PgConnector`
error[E0433]: cannot find type `PgConnector` in this scope
error[E0433]: cannot find type `SourceError` in this scope
error[E0425]: cannot find function `parse_lsn` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-source-pg/src/position.rs`, above the test module:

```rust
//! PostgreSQL positions: the core `Lsn` and the replication crate's.

use data_avalanche_core::position::Lsn;
use data_avalanche_core::{SourceError, SourcePosition};

/// A `pg_lsn` as PostgreSQL prints it (`16/B374D848`).
pub(crate) fn parse_lsn(text: &str) -> Result<Lsn, SourceError> {
    text.parse()
        .map_err(|e: data_avalanche_core::position::LsnParseError| {
            SourceError::Permanent(e.to_string())
        })
}

/// The LSN of a PostgreSQL position; another dialect is a caller error.
pub(crate) fn lsn_of(p: &SourcePosition) -> Result<Lsn, SourceError> {
    match p {
        SourcePosition::Pg(l) => Ok(*l),
        other => Err(SourceError::Permanent(format!(
            "a {} position given to the PostgreSQL source",
            other.dialect()
        ))),
    }
}

pub(crate) fn to_wire(l: Lsn) -> pgwire_replication::Lsn {
    pgwire_replication::Lsn(l.0)
}

pub(crate) fn from_wire(l: pgwire_replication::Lsn) -> Lsn {
    Lsn(l.0)
}
```

Add the implementation to `crates/data-avalanche-source-pg/src/connector.rs`, above the test module:

```rust
//! `PgConnector`: the PostgreSQL source of one pipeline.

use std::collections::BTreeMap;

use data_avalanche_config::{
    Pipeline, Postgres, SourceType, check_table_set, check_unbounded_numeric,
};
use data_avalanche_core::{
    HaltReason, Preflight, ResolvedTables, SourceError, SourcePosition, TableRef, TableSchema,
};
use tokio::sync::Mutex;

use crate::catalog::{self, Described, Notes, table_key, table_schema};
use crate::connect::Connector;
use crate::position::parse_lsn;
use crate::types::TypeCatalog;

/// The oldest server the source supports (P-8).
pub const MIN_SERVER_VERSION: i32 = 140_000;

/// The PostgreSQL source of one pipeline (roadmap §3.6). Built from the resolved pipeline
/// configuration; connections open on first use.
#[derive(Debug)]
pub struct PgConnector {
    pub(crate) pipeline: Pipeline,
    #[allow(dead_code)] // the slot lifecycle (Task 8) reads it
    pub(crate) pg: Postgres,
    pub(crate) conn: Connector,
    /// `pg_type` rows fetched so far.
    pub(crate) types: Mutex<TypeCatalog>,
}

impl PgConnector {
    /// Refuses a pipeline whose source isn't PostgreSQL, and TLS material that doesn't parse.
    pub fn new(pipeline: &Pipeline) -> Result<Self, SourceError> {
        let source = &pipeline.source;
        let pg = match (source.kind, &source.postgres) {
            (SourceType::Postgres, Some(pg)) => pg.clone(),
            _ => {
                return Err(SourceError::Permanent(format!(
                    "pipeline {}: the source isn't PostgreSQL",
                    pipeline.name
                )));
            }
        };
        Ok(Self {
            pipeline: pipeline.clone(),
            pg,
            conn: Connector::new(source)?,
            types: Mutex::new(TypeCatalog::new()),
        })
    }

    /// The pipeline-level start checks (config §11.2, `Schema_Change…` §9): the server is
    /// PostgreSQL 14 or newer with `wal_level = logical`, the database encoding is UTF8 (D-M5.8),
    /// and the user can read the catalogs channels A/C/D query. A user without the `REPLICATION`
    /// attribute warns: a managed service may grant replication another way.
    pub async fn preflight(&self) -> Result<Preflight, SourceError> {
        let client = self.conn.client().await?;
        let row = self
            .conn
            .command(
                "preflight",
                client.query_one(
                    "SELECT current_setting('server_version_num')::int,
                            current_setting('server_version'),
                            current_setting('wal_level'),
                            pg_encoding_to_char(d.encoding),
                            r.rolreplication OR r.rolsuper,
                            has_table_privilege('pg_catalog.pg_class', 'SELECT')
                              AND has_table_privilege('pg_catalog.pg_attribute', 'SELECT')
                              AND has_table_privilege('pg_catalog.pg_index', 'SELECT')
                              AND has_table_privilege('pg_catalog.pg_type', 'SELECT')
                              AND has_table_privilege('pg_catalog.pg_namespace', 'SELECT')
                       FROM pg_catalog.pg_database d, pg_catalog.pg_roles r
                      WHERE d.datname = current_database() AND r.rolname = current_user",
                    &[],
                ),
            )
            .await?;
        let (num, version, wal_level, encoding, replication, catalogs): (
            i32,
            String,
            String,
            String,
            bool,
            bool,
        ) = (
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
        );
        let refuse = |m: String| Err(SourceError::Permanent(m));
        if num < MIN_SERVER_VERSION {
            return refuse(format!(
                "PostgreSQL {version}: the source needs 14 or newer"
            ));
        }
        if wal_level != "logical" {
            return refuse(format!(
                "wal_level is {wal_level}; logical replication needs wal_level = logical"
            ));
        }
        if encoding != "UTF8" {
            return refuse(format!(
                "the database encoding is {encoding}; the source reads UTF8 databases only"
            ));
        }
        if !catalogs {
            return refuse(
                "the user can't SELECT from the system catalogs the schema channels read".into(),
            );
        }
        let mut warnings = vec![];
        if !replication {
            warnings.push(
                "the user has neither the REPLICATION attribute nor superuser; the replication connection may be refused".into(),
            );
        }
        Ok(Preflight {
            server_version: version,
            warnings,
        })
    }

    /// Describes `oids` and builds their schemas, fetching the types they use.
    pub(crate) async fn schemas(
        &self,
        oids: &[u32],
    ) -> Result<BTreeMap<u32, (Described, Result<(TableSchema, Notes), HaltReason>)>, SourceError>
    {
        let client = self.conn.client().await?;
        let described = self
            .conn
            .command("describe tables", catalog::describe(&client, oids))
            .await?;
        let mut types = self.types.lock().await;
        let type_oids: Vec<u32> = described
            .values()
            .flat_map(|d| d.columns.iter().map(|c| c.type_oid))
            .collect();
        self.conn
            .command(
                "load types",
                catalog::load_types(&client, type_oids, &mut types),
            )
            .await?;
        Ok(described
            .into_iter()
            .map(|(oid, d)| {
                let s = table_schema(&d, &self.pipeline.tables, &types);
                (oid, (d, s))
            })
            .collect())
    }

    /// Channel A (`Schema_Change…` §3): the include/exclude globs against the live catalog, each
    /// table checked (config §11.2). A table without an identifier is skipped with a warning when
    /// a glob found it, and halts when its name was listed; every other problem halts the table.
    pub async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
        let client = self.conn.client().await?;
        let relations = self
            .conn
            .command("list tables", catalog::relations(&client))
            .await?;
        let t = &self.pipeline.tables;
        let selection = catalog::select(&t.include, &t.exclude, &relations);
        let oids: Vec<u32> = selection.tables.iter().map(|(r, _)| r.oid).collect();
        let mut schemas = self.schemas(&oids).await?;
        let mut out = ResolvedTables {
            skipped: selection.skipped,
            warnings: selection.warnings,
            ..ResolvedTables::default()
        };
        let mut unconstrained = vec![];
        for (relation, explicit) in selection.tables {
            let Some((_, result)) = schemas.remove(&relation.oid) else {
                continue; // dropped since it was listed
            };
            match result {
                Ok((schema, notes)) => {
                    let key = table_key(schema.table());
                    for (column, ty) in notes.read_as_text {
                        out.warnings
                            .push(format!("{key}.{column}: {ty} is read as text"));
                    }
                    unconstrained.extend(
                        notes
                            .unconstrained
                            .into_iter()
                            .map(|c| (schema.table().clone(), c)),
                    );
                    out.tables.push(schema);
                }
                Err(HaltReason::NoIdentifier) if !explicit => out.skipped.push((
                    relation.table,
                    "no primary key and no tables.primary_key entry".into(),
                )),
                Err(reason) => out.halted.push((relation.table, reason)),
            }
        }
        let resolved: Vec<TableRef> = out.tables.iter().map(|s| s.table().clone()).collect();
        let mut report = check_table_set(&self.pipeline, &resolved);
        report.extend(check_unbounded_numeric(&self.pipeline, &unconstrained));
        if let Some(e) = report.errors.first() {
            return Err(SourceError::Permanent(format!(
                "{} {}: {}",
                e.rule, e.path, e.message
            )));
        }
        out.warnings.extend(
            report
                .warnings
                .iter()
                .map(|w| format!("{} {}: {}", w.rule, w.path, w.message)),
        );
        Ok(out)
    }

    /// `pg_current_wal_lsn()`: a snapshot chunk's watermark (Snapshot_Resumability §1).
    pub async fn current_position(&self) -> Result<SourcePosition, SourceError> {
        let client = self.conn.client().await?;
        let row = self
            .conn
            .command(
                "current position",
                client.query_one("SELECT pg_current_wal_lsn()::text", &[]),
            )
            .await?;
        Ok(SourcePosition::Pg(parse_lsn(row.get(0))?))
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 49.

Run, with `test-infra/up.sh` running: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg`

Expected: PASS, 5 tests.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/position.rs \
  crates/data-avalanche-source-pg/src/connector.rs \
  crates/data-avalanche-source-pg/tests/pg/main.rs \
  crates/data-avalanche-source-pg/tests/pg/common.rs \
  crates/data-avalanche-source-pg/tests/pg/resolve.rs
git commit -m "feat(source-pg): PgConnector, preflight and table resolution"
```

---

### Task 8: The slot state machine and publication reconciliation

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/slot.rs`
- Modify: `crates/data-avalanche-source-pg/tests/pg/main.rs`
- Create: `crates/data-avalanche-source-pg/tests/pg/slot.rs`

**Interfaces:**
- Consumes: `PgConnector`, `parse_lsn`, `lsn_of` (Task 7); `Connector` (Task 2); `table_key` (Task 6); `SlotManagement`,
  `SlotOnOrphan` (config).
- Produces:
  - `SlotState { plugin, database, confirmed_flush, invalidated }`, `SlotAction::{Resume,
    Create(SnapshotReason), Recreate(SnapshotReason), Adopt, Refuse(String)}`, `decide(checkpoint,
    slot, db, name, SlotManagement, SlotOnOrphan) -> SlotAction`.
  - `ident(&str)` (a quoted identifier), `qualified(&TableRef)`.
  - `PgConnector::prepare(Option<&SourcePosition>, &[TableRef]) -> Prepared`, `reset_position() ->
    SourcePosition`, `teardown()`, `retained_bytes() -> Option<u64>`, and `pub(crate)
    reconcile_publication(&Client, &[TableRef]) -> Vec<String>` (warnings).

D-M5.13, D-M5.14, D-M5.15. `decide` is the whole state table of Postgres_Slot… §1–§3 as one
pure function, unit-tested cell by cell. The integration tests run every cell against a live
server: a missing slot, an invalidated one (`max_slot_wal_keep_size` and a WAL switch), an orphan
under each policy, a wrong plugin, and `require_existing`. The publication is reconciled before
the slot is created. A publication the pipeline may not change, publishing via the partition root
while a tracked table is a partition, is refused (Q-M5-5).

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-source-pg/src/lib.rs` with:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

pub mod catalog;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod connect;
mod connector;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod error;
pub mod pgoutput;
#[allow(dead_code)] // the stream (Task 9) is the caller
mod position;
mod slot;
#[cfg(test)]
mod test_support;
mod tls;
pub mod types;
pub mod value;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
pub use connector::{MIN_SERVER_VERSION, PgConnector};
```

Create `crates/data-avalanche-source-pg/src/slot.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use SlotAction as A;
    use SnapshotReason as R;

    const M: SlotManagement = SlotManagement::Managed;
    const X: SlotManagement = SlotManagement::RequireExisting;
    const HALT: SlotOnOrphan = SlotOnOrphan::Halt;
    const ADOPT: SlotOnOrphan = SlotOnOrphan::AdoptResnapshot;

    fn slot(flushed: u64) -> SlotState {
        SlotState {
            plugin: Some("pgoutput".into()),
            database: Some("shop".into()),
            confirmed_flush: Some(Lsn(flushed)),
            invalidated: false,
        }
    }

    fn d(
        checkpoint: Option<u64>,
        s: Option<SlotState>,
        m: SlotManagement,
        o: SlotOnOrphan,
    ) -> SlotAction {
        decide(checkpoint.map(Lsn), s.as_ref(), "shop", "s", m, o)
    }

    fn refused(a: SlotAction, needle: &str) {
        match a {
            A::Refuse(m) => assert!(m.contains(needle), "{m:?} should mention {needle:?}"),
            other => panic!("refused expected, got {other:?}"),
        }
    }

    #[test]
    fn state_and_slot_resume() {
        assert_eq!(d(Some(0x20), Some(slot(0x20)), M, HALT), A::Resume);
        assert_eq!(
            d(Some(0x20), Some(slot(0x10)), X, HALT),
            A::Resume,
            "behind the checkpoint is fine"
        );
    }

    #[test]
    fn state_without_slot_recreates_under_managed() {
        assert_eq!(d(Some(0x20), None, M, HALT), A::Create(R::PositionLost));
        refused(d(Some(0x20), None, X, HALT), "doesn't exist");
    }

    #[test]
    fn a_slot_past_the_checkpoint_or_invalidated_lost_the_position() {
        assert_eq!(
            d(Some(0x10), Some(slot(0x20)), M, HALT),
            A::Recreate(R::PositionLost)
        );
        refused(
            d(Some(0x10), Some(slot(0x20)), X, HALT),
            "past the pipeline's checkpoint",
        );
        let mut gone = slot(0x10);
        gone.invalidated = true;
        assert_eq!(
            d(Some(0x20), Some(gone.clone()), M, HALT),
            A::Recreate(R::PositionLost)
        );
        refused(d(Some(0x20), Some(gone), X, HALT), "invalidated");
    }

    #[test]
    fn an_orphan_halts_or_is_adopted() {
        refused(d(None, Some(slot(0x10)), M, HALT), "orphan");
        refused(d(None, Some(slot(0x10)), X, HALT), "orphan");
        assert_eq!(d(None, Some(slot(0x10)), M, ADOPT), A::Adopt);
        assert_eq!(
            d(None, Some(slot(0x10)), X, ADOPT),
            A::Adopt,
            "advancing isn't recreating"
        );
        let mut gone = slot(0x10);
        gone.invalidated = true;
        assert_eq!(
            d(None, Some(gone.clone()), M, ADOPT),
            A::Recreate(R::AdoptedOrphan)
        );
        refused(d(None, Some(gone), X, ADOPT), "invalidated");
    }

    #[test]
    fn a_first_start_creates_under_managed() {
        assert_eq!(d(None, None, M, HALT), A::Create(R::FirstStart));
        refused(d(None, None, X, ADOPT), "doesn't exist");
    }

    #[test]
    fn someone_elses_slot_is_refused() {
        let mut physical = slot(0x10);
        physical.plugin = None;
        refused(
            d(Some(0x10), Some(physical.clone()), M, ADOPT),
            "physical plugin",
        );
        refused(d(None, Some(physical), M, ADOPT), "physical plugin");
        let mut wal2json = slot(0x10);
        wal2json.plugin = Some("wal2json".into());
        refused(d(Some(0x10), Some(wal2json), M, HALT), "wal2json");
        let mut elsewhere = slot(0x10);
        elsewhere.database = Some("other".into());
        refused(d(Some(0x10), Some(elsewhere), M, HALT), "database other");
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(
            qualified(&TableRef::new("My Schema", "t")),
            "\"My Schema\".\"t\""
        );
    }
}
```

Replace `crates/data-avalanche-source-pg/tests/pg/main.rs` with:

```rust
//! The PostgreSQL source against a live server (`test-infra/compose.yaml`): `postgres-18` on every
//! change, PostgreSQL 14–17 nightly (`DATA_AVALANCHE_PG_PORT`). Each test returns at once without
//! `DATA_AVALANCHE_IT`.

mod common;
mod resolve;
mod slot;
```

Create `crates/data-avalanche-source-pg/tests/pg/slot.rs`:

```rust
//! The slot state machine cell by cell (`Postgres_Slot…` §1–§3, the M5 exit) and publication
//! reconciliation (§4) against a live server.

use data_avalanche_core::position::Lsn;
use data_avalanche_core::{SnapshotReason, SourceError, SourcePosition, StartPlan, TableRef};
use data_avalanche_testkit::require_it;
use tokio_postgres::Client;

use crate::common::{Names, admin, cleanup, connector, fresh, pipeline};

const DDL: &str = "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY);
                   CREATE TABLE c (id int PRIMARY KEY);";

async fn confirmed(a: &Client, n: &Names) -> Option<Lsn> {
    a.query_opt(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
        &[&n.slot],
    )
    .await
    .unwrap()
    .map(|r| r.get::<_, String>(0).parse().unwrap())
}

async fn published(a: &Client, n: &Names) -> Vec<String> {
    a.query(
        "SELECT tablename::text FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1",
        &[&n.publication],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| r.get(0))
    .collect()
}

fn from(plan: &StartPlan, expect: SnapshotReason) -> SourcePosition {
    match plan {
        StartPlan::Snapshot { from, reason } if *reason == expect => from.clone(),
        other => panic!("a {expect:?} snapshot expected, got {other:?}"),
    }
}

fn permanent(e: SourceError, needle: &str) {
    match e {
        SourceError::Permanent(m) => assert!(m.contains(needle), "{m:?} should mention {needle:?}"),
        other => panic!("permanent expected, got {other:?}"),
    }
}

#[tokio::test]
async fn first_start_then_resume_then_a_lost_slot() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_cells", DDL).await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let tables = [n.table("a"), n.table("b")];

    // state absent × slot absent: create, snapshot from the slot's consistent point.
    let p = c.prepare(None, &tables).await.unwrap();
    let start = from(&p.plan, SnapshotReason::FirstStart);
    assert!(p.warnings.is_empty());
    assert_eq!(
        confirmed(&a, &n).await.map(SourcePosition::Pg),
        Some(start.clone())
    );
    assert_eq!(published(&a, &n).await, ["a", "b"]);

    // state present × slot present: resume.
    let p = c.prepare(Some(&start), &tables).await.unwrap();
    assert_eq!(p.plan, StartPlan::Resume);

    // state present × slot absent: recreate, snapshot (D-15.3).
    a.execute("SELECT pg_drop_replication_slot($1)", &[&n.slot])
        .await
        .unwrap();
    let p = c.prepare(Some(&start), &tables).await.unwrap();
    let again = from(&p.plan, SnapshotReason::PositionLost);
    assert!(confirmed(&a, &n).await.is_some(), "recreated");

    // A slot confirmed past the checkpoint lost what lies between: recreate, snapshot.
    a.execute(&format!("INSERT INTO {}.a VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    a.execute(
        "SELECT pg_replication_slot_advance($1, pg_current_wal_lsn())",
        &[&n.slot],
    )
    .await
    .unwrap();
    let p = c.prepare(Some(&again), &tables).await.unwrap();
    from(&p.plan, SnapshotReason::PositionLost);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn an_orphan_halts_by_default_and_is_adopted_on_request() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_orphan", DDL).await;
    a.execute(
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
        &[&n.slot],
    )
    .await
    .unwrap();
    let tables = [n.table("a")];
    let halt = connector(&pipeline(&n, &["*"], "", ""));
    permanent(halt.prepare(None, &tables).await.unwrap_err(), "orphan");

    a.execute(&format!("INSERT INTO {}.a VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    let before = confirmed(&a, &n).await.unwrap();
    let adopt = connector(&pipeline(
        &n,
        &["*"],
        "",
        ", slot_on_orphan: adopt_resnapshot",
    ));
    let p = adopt.prepare(None, &tables).await.unwrap();
    let SourcePosition::Pg(at) = from(&p.plan, SnapshotReason::AdoptedOrphan) else {
        panic!("a PostgreSQL position")
    };
    assert!(at > before, "advanced past the orphan's old position");
    assert_eq!(confirmed(&a, &n).await, Some(at));
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn require_existing_never_creates() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_existing", DDL).await;
    let postgres = ", slot_management: require_existing, auto_cleanup: false";
    let c = connector(&pipeline(&n, &["*"], "", postgres));
    let tables = [n.table("a"), n.table("b")];
    permanent(
        c.prepare(None, &tables).await.unwrap_err(),
        &format!("publication {}", n.publication),
    );

    a.batch_execute(&format!(
        "CREATE PUBLICATION {} FOR TABLE {}.a",
        n.publication, n.schema
    ))
    .await
    .unwrap();
    permanent(c.prepare(None, &tables).await.unwrap_err(), "doesn't exist");
    let start: String = a
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&n.slot],
        )
        .await
        .unwrap()
        .get(0);
    let start = SourcePosition::Pg(start.parse().unwrap());
    let p = c.prepare(Some(&start), &tables).await.unwrap();
    assert_eq!(p.plan, StartPlan::Resume);
    assert_eq!(
        p.warnings,
        [format!(
            "publication {} doesn't publish {}.b: its changes won't arrive",
            n.publication, n.schema
        )],
        "the DBA's publication is validated, not altered"
    );
    assert_eq!(published(&a, &n).await, ["a"]);

    // Lost slot under require_existing: refused, not recreated.
    a.execute("SELECT pg_drop_replication_slot($1)", &[&n.slot])
        .await
        .unwrap();
    permanent(
        c.prepare(Some(&start), &tables).await.unwrap_err(),
        "require_existing",
    );
    permanent(c.reset_position().await.unwrap_err(), "require_existing");
    c.teardown().await.unwrap();
    assert_eq!(
        published(&a, &n).await,
        ["a"],
        "teardown leaves what it didn't create"
    );
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn the_publication_follows_the_table_set() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_publication", DDL).await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let p = c
        .prepare(None, &[n.table("a"), n.table("b")])
        .await
        .unwrap();
    let start = from(&p.plan, SnapshotReason::FirstStart);
    a.batch_execute(&format!(
        "ALTER PUBLICATION {} SET (publish = 'insert')",
        n.publication
    ))
    .await
    .unwrap();
    let p = c
        .prepare(Some(&start), &[n.table("b"), n.table("c")])
        .await
        .unwrap();
    assert_eq!(p.plan, StartPlan::Resume);
    assert_eq!(published(&a, &n).await, ["b", "c"]);
    let ops: bool = a
        .query_one(
            "SELECT pubinsert AND pubupdate AND pubdelete AND pubtruncate FROM pg_publication WHERE pubname = $1",
            &[&n.publication],
        )
        .await
        .unwrap()
        .get(0);
    assert!(ops, "every operation published again");
    let empty: [TableRef; 0] = [];
    c.prepare(Some(&start), &empty).await.unwrap();
    assert!(published(&a, &n).await.is_empty());
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_root_publication_of_tracked_partitions_is_refused() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "slot_via_root",
        "CREATE TABLE a (id int PRIMARY KEY);
         CREATE TABLE events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
         CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');",
    )
    .await;
    a.batch_execute(&format!(
        "CREATE PUBLICATION {} FOR TABLE {s}.a, {s}.events WITH (publish_via_partition_root = true)",
        n.publication,
        s = n.schema
    ))
    .await
    .unwrap();
    let start: String = a
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&n.slot],
        )
        .await
        .unwrap()
        .get(0);
    let start = SourcePosition::Pg(start.parse().unwrap());
    let postgres = ", slot_management: require_existing, auto_cleanup: false";
    let c = connector(&pipeline(&n, &["*"], "", postgres));
    let e = c
        .prepare(Some(&start), &[n.table("a"), n.table("events_2024")])
        .await
        .unwrap_err();
    permanent(
        e,
        &format!(
            "the partitions {}.events_2024 would arrive under their parent's name",
            n.schema
        ),
    );
    let p = c.prepare(Some(&start), &[n.table("a")]).await.unwrap();
    assert_eq!(
        p.plan,
        StartPlan::Resume,
        "no partition tracked: the flag is harmless"
    );

    // Under `managed`, a FOR ALL TABLES publication isn't changed either.
    a.batch_execute(&format!(
        "DROP PUBLICATION {p}; CREATE PUBLICATION {p} FOR ALL TABLES WITH (publish_via_partition_root = true)",
        p = n.publication
    ))
    .await
    .unwrap();
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let e = c
        .prepare(Some(&start), &[n.table("events_2024")])
        .await
        .unwrap_err();
    permanent(e, "publish_via_partition_root = false");
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn reset_teardown_and_retained_wal() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "slot_reset", DDL).await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    assert_eq!(c.retained_bytes().await.unwrap(), None, "no slot yet");
    let p = c.prepare(None, &[n.table("a")]).await.unwrap();
    let start = from(&p.plan, SnapshotReason::FirstStart);
    a.execute(
        &format!(
            "INSERT INTO {}.a SELECT g FROM generate_series(1, 1000) g",
            n.schema
        ),
        &[],
    )
    .await
    .unwrap();
    let retained = c.retained_bytes().await.unwrap().unwrap();
    assert!(retained > 0, "WAL written since the slot's position");

    let SourcePosition::Pg(fresh_at) = c.reset_position().await.unwrap() else {
        panic!("a PostgreSQL position")
    };
    let SourcePosition::Pg(old) = start else {
        unreachable!()
    };
    assert!(fresh_at > old, "a new consistent point");

    let kept = connector(&pipeline(&n, &["*"], "", ", auto_cleanup: false"));
    kept.teardown().await.unwrap();
    assert!(
        confirmed(&a, &n).await.is_some(),
        "auto_cleanup: false keeps the slot"
    );
    c.teardown().await.unwrap();
    assert!(
        confirmed(&a, &n).await.is_none(),
        "auto_cleanup drops the slot"
    );
    assert!(
        a.query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&n.publication]
        )
        .await
        .unwrap()
        .is_none(),
        "and the publication"
    );
    cleanup(&a, &n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `SlotAction`
error[E0432]: unresolved import `SnapshotReason`
error[E0425]: cannot find type `SlotManagement` in this scope
error[E0425]: cannot find type `SlotOnOrphan` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-source-pg/src/slot.rs`, above the test module:

```rust
//! The slot and publication lifecycle (`Postgres_Slot_and_Publication_Lifecycle.md`): the slot
//! state machine (D-15.1–15.3), publication reconciliation (D-15.4), teardown and the WAL the
//! slot retains.

use std::collections::BTreeSet;

use data_avalanche_config::{SlotManagement, SlotOnOrphan};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    Prepared, SnapshotReason, SourceError, SourcePosition, StartPlan, TableRef,
};
use tokio_postgres::Client;

use crate::catalog::table_key;
use crate::connect::within;
use crate::connector::PgConnector;
use crate::position::{lsn_of, parse_lsn};

/// The server's row for the pipeline's slot (`pg_replication_slots`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotState {
    pub plugin: Option<String>,
    pub database: Option<String>,
    pub confirmed_flush: Option<Lsn>,
    /// `wal_status = 'lost'`, a PostgreSQL 16 `conflicting` logical slot, or a PostgreSQL 17+
    /// `invalidation_reason`: the slot can't stream any more.
    pub invalidated: bool,
}

/// What [`decide`] says to do with the slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotAction {
    /// Stream from the checkpoint.
    Resume,
    /// Create the slot (none exists), then snapshot.
    Create(SnapshotReason),
    /// Drop the unusable slot and create it again, then snapshot.
    Recreate(SnapshotReason),
    /// Move the orphan slot to the current WAL position (`pg_replication_slot_advance`), then
    /// snapshot: `on_orphan: adopt_resnapshot`. Advancing works under `require_existing` too.
    Adopt,
    /// Refuse to start: the operator decides.
    Refuse(String),
}

/// The slot state machine (`Postgres_Slot…` §1–§3), one cell per combination of pipeline state
/// (`checkpoint`) and server slot (`slot`).
pub fn decide(
    checkpoint: Option<Lsn>,
    slot: Option<&SlotState>,
    db: &str,
    name: &str,
    management: SlotManagement,
    orphan: SlotOnOrphan,
) -> SlotAction {
    let managed = management == SlotManagement::Managed;
    let not_ours = |s: &SlotState| {
        if s.plugin.as_deref() != Some("pgoutput") {
            Some(format!(
                "replication slot {name} uses the {} plugin, not pgoutput",
                s.plugin.as_deref().unwrap_or("physical")
            ))
        } else if s.database.as_deref() != Some(db) {
            Some(format!(
                "replication slot {name} belongs to database {}, not {db}",
                s.database.as_deref().unwrap_or("-")
            ))
        } else {
            None
        }
    };
    let lost = |why: &str| {
        if managed {
            SlotAction::Recreate(SnapshotReason::PositionLost)
        } else {
            SlotAction::Refuse(format!(
                "replication slot {name} {why}; slot_management is require_existing, so recreate it and restart: the pipeline re-snapshots"
            ))
        }
    };
    match (checkpoint, slot) {
        (Some(at), Some(s)) => {
            if let Some(m) = not_ours(s) {
                return SlotAction::Refuse(m);
            }
            if s.invalidated {
                return lost("was invalidated");
            }
            match s.confirmed_flush {
                Some(flushed) if flushed > at => lost(&format!(
                    "confirmed {flushed}, past the pipeline's checkpoint {at}"
                )),
                _ => SlotAction::Resume,
            }
        }
        (Some(_), None) if managed => SlotAction::Create(SnapshotReason::PositionLost),
        (Some(_), None) => SlotAction::Refuse(format!(
            "replication slot {name} doesn't exist and slot_management is require_existing"
        )),
        (None, Some(s)) => match orphan {
            SlotOnOrphan::Halt => SlotAction::Refuse(format!(
                "replication slot {name} exists but the pipeline has no state (an orphan): drop the slot for a first start, restore the pipeline state, or set slot.on_orphan: adopt_resnapshot"
            )),
            SlotOnOrphan::AdoptResnapshot => match not_ours(s) {
                Some(m) => SlotAction::Refuse(m),
                None if s.invalidated && managed => {
                    SlotAction::Recreate(SnapshotReason::AdoptedOrphan)
                }
                None if s.invalidated => lost("was invalidated"),
                None => SlotAction::Adopt,
            },
        },
        (None, None) if managed => SlotAction::Create(SnapshotReason::FirstStart),
        (None, None) => SlotAction::Refuse(format!(
            "replication slot {name} doesn't exist and slot_management is require_existing"
        )),
    }
}

/// A SQL identifier, quoted.
pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `schema.table`, quoted.
pub(crate) fn qualified(t: &TableRef) -> String {
    format!("{}.{}", ident(&t.schema), ident(&t.name))
}

/// The tables among `tables` that are partitions (`relispartition`).
async fn partitions(
    client: &Client,
    tables: &[TableRef],
) -> Result<Vec<TableRef>, tokio_postgres::Error> {
    let schemas: Vec<&str> = tables.iter().map(|t| t.schema.as_str()).collect();
    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    let rows = client
        .query(
            "SELECT n.nspname::text, c.relname::text
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               JOIN unnest($1::text[], $2::text[]) AS w(s, t) ON w.s = n.nspname AND w.t = c.relname
              WHERE c.relispartition
              ORDER BY 1, 2",
            &[&schemas, &names],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| TableRef::new(r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect())
}

pub(crate) async fn slot_state(
    client: &Client,
    name: &str,
) -> Result<Option<SlotState>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "SELECT plugin::text, database::text, confirmed_flush_lsn::text,
                    coalesce(wal_status = 'lost', false)
                      OR coalesce((to_jsonb(s) ->> 'conflicting')::bool, false)
                      OR (to_jsonb(s) ->> 'invalidation_reason') IS NOT NULL
               FROM pg_catalog.pg_replication_slots s
              WHERE slot_name = $1",
            &[&name],
        )
        .await?;
    Ok(row.map(|r| SlotState {
        plugin: r.get(0),
        database: r.get(1),
        confirmed_flush: r.get::<_, Option<String>>(2).and_then(|t| t.parse().ok()),
        invalidated: r.get(3),
    }))
}

impl PgConnector {
    fn slot_name(&self) -> &str {
        &self.pg.slot
    }

    async fn create_slot(&self, client: &Client) -> Result<Lsn, SourceError> {
        // Creating a slot waits for the transactions running at that moment: the snapshot timeout
        // bounds it, not the command timeout.
        let row = within(
            "create the replication slot",
            self.conn.timeouts.snapshot,
            client.query_one(
                "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&self.slot_name()],
            ),
        )
        .await?;
        parse_lsn(row.get(0))
    }

    async fn drop_slot(&self, client: &Client) -> Result<(), SourceError> {
        self.conn
            .command(
                "drop the replication slot",
                client.execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot_name()],
                ),
            )
            .await
            .map(drop)
    }

    /// D-15.4: under `managed`, the publication publishes exactly `tables` and every operation
    /// (created when absent, then `ALTER PUBLICATION … ADD/DROP TABLE` and `SET`); under
    /// `require_existing`, it must exist and a tracked table it doesn't publish warns. A
    /// publication this can't change (`require_existing`, `FOR ALL TABLES`) that publishes via the
    /// partition root while a tracked table is a partition is refused.
    pub(crate) async fn reconcile_publication(
        &self,
        client: &Client,
        tables: &[TableRef],
    ) -> Result<Vec<String>, SourceError> {
        let name = &self.pg.publication;
        let managed = self.pg.slot_management == SlotManagement::Managed;
        let row = self
            .conn
            .command(
                "read the publication",
                client.query_opt(
                    "SELECT puballtables, pubinsert AND pubupdate AND pubdelete AND pubtruncate,
                            pubviaroot
                       FROM pg_catalog.pg_publication WHERE pubname = $1",
                    &[name],
                ),
            )
            .await?;
        let wanted: BTreeSet<&TableRef> = tables.iter().collect();
        let Some(row) = row else {
            if !managed {
                return Err(SourceError::Permanent(format!(
                    "publication {name} doesn't exist and slot_management is require_existing"
                )));
            }
            let list: Vec<String> = tables.iter().map(qualified).collect();
            let sql = if list.is_empty() {
                format!("CREATE PUBLICATION {}", ident(name))
            } else {
                format!(
                    "CREATE PUBLICATION {} FOR TABLE {}",
                    ident(name),
                    list.join(", ")
                )
            };
            self.conn
                .command("create the publication", client.batch_execute(&sql))
                .await?;
            return Ok(vec![]);
        };
        let (all_tables, every_op, via_root): (bool, bool, bool) =
            (row.get(0), row.get(1), row.get(2));
        let published: BTreeSet<TableRef> = self
            .conn
            .command(
                "read the publication's tables",
                client.query(
                    "SELECT schemaname::text, tablename::text FROM pg_catalog.pg_publication_tables WHERE pubname = $1",
                    &[name],
                ),
            )
            .await?
            .iter()
            .map(|r| TableRef::new(r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect();
        let missing: Vec<&TableRef> = wanted
            .iter()
            .copied()
            .filter(|t| !published.contains(*t))
            .collect();
        let mut warnings = vec![];
        if !managed || all_tables {
            // Q-M5-5: a partition's changes would arrive under its parent's name, which isn't
            // tracked, so the stream would drop them. Refused, not warned: it would lose data.
            if via_root {
                let leaves = self
                    .conn
                    .command("find partitions", partitions(client, tables))
                    .await?;
                if !leaves.is_empty() {
                    let list: Vec<String> = leaves.iter().map(table_key).collect();
                    return Err(SourceError::Permanent(format!(
                        "publication {name} publishes via the partition root, so the changes of the partitions {} would arrive under their parent's name and be lost; run ALTER PUBLICATION {} SET (publish_via_partition_root = false)",
                        list.join(", "),
                        ident(name)
                    )));
                }
            }
            for t in missing {
                warnings.push(format!(
                    "publication {name} doesn't publish {t}: its changes won't arrive"
                ));
            }
            if !every_op {
                warnings.push(format!(
                    "publication {name} doesn't publish every operation; set publish = 'insert, update, delete, truncate'"
                ));
            }
            return Ok(warnings);
        }
        let mut sql = vec![];
        if !missing.is_empty() {
            let list: Vec<String> = missing.iter().map(|t| qualified(t)).collect();
            sql.push(format!(
                "ALTER PUBLICATION {} ADD TABLE {}",
                ident(name),
                list.join(", ")
            ));
        }
        let extra: Vec<String> = published
            .iter()
            .filter(|t| !wanted.contains(t))
            .map(qualified)
            .collect();
        if !extra.is_empty() {
            sql.push(format!(
                "ALTER PUBLICATION {} DROP TABLE {}",
                ident(name),
                extra.join(", ")
            ));
        }
        if !every_op || via_root {
            sql.push(format!(
                "ALTER PUBLICATION {} SET (publish = 'insert, update, delete, truncate', publish_via_partition_root = false)",
                ident(name)
            ));
        }
        if !sql.is_empty() {
            self.conn
                .command(
                    "reconcile the publication",
                    client.batch_execute(&sql.join("; ")),
                )
                .await?;
        }
        Ok(warnings)
    }

    /// The publication first (its changes are decoded with the catalog as of their WAL position,
    /// so it must exist before the slot's start), then the slot state machine.
    pub async fn prepare(
        &self,
        checkpoint: Option<&SourcePosition>,
        tables: &[TableRef],
    ) -> Result<Prepared, SourceError> {
        let checkpoint = checkpoint.map(lsn_of).transpose()?;
        let client = self.conn.client().await?;
        let warnings = self.reconcile_publication(&client, tables).await?;
        let slot = self
            .conn
            .command(
                "read the replication slot",
                slot_state(&client, self.slot_name()),
            )
            .await?;
        let action = decide(
            checkpoint,
            slot.as_ref(),
            &self.pipeline.source.db,
            self.slot_name(),
            self.pg.slot_management,
            self.pg.slot_on_orphan,
        );
        let snapshot = |from: Lsn, reason| StartPlan::Snapshot {
            from: SourcePosition::Pg(from),
            reason,
        };
        let plan = match action {
            SlotAction::Resume => StartPlan::Resume,
            SlotAction::Create(reason) => snapshot(self.create_slot(&client).await?, reason),
            SlotAction::Recreate(reason) => {
                self.drop_slot(&client).await?;
                snapshot(self.create_slot(&client).await?, reason)
            }
            SlotAction::Adopt => {
                let row = self
                    .conn
                    .command(
                        "advance the orphan slot",
                        client.query_one(
                            "SELECT end_lsn::text FROM pg_replication_slot_advance($1, pg_current_wal_lsn())",
                            &[&self.slot_name()],
                        ),
                    )
                    .await?;
                snapshot(parse_lsn(row.get(0))?, SnapshotReason::AdoptedOrphan)
            }
            SlotAction::Refuse(m) => return Err(SourceError::Permanent(m)),
        };
        Ok(Prepared { plan, warnings })
    }

    /// D-15.3 after [`SourceError::PositionUnreachable`]: drop the slot and create it again;
    /// stream from the new position after a re-snapshot. Under `require_existing` the slot isn't
    /// ours to recreate.
    pub async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
        if self.pg.slot_management == SlotManagement::RequireExisting {
            return Err(SourceError::Permanent(format!(
                "replication slot {} lost its position and slot_management is require_existing: recreate it and restart",
                self.slot_name()
            )));
        }
        let client = self.conn.client().await?;
        self.drop_slot(&client).await?;
        Ok(SourcePosition::Pg(self.create_slot(&client).await?))
    }

    /// `auto_cleanup`: drops the slot and the publication when the pipeline is deleted. Nothing
    /// otherwise (`require_existing` forces `auto_cleanup: false`, V-103). A slot still streaming
    /// is an error.
    pub async fn teardown(&self) -> Result<(), SourceError> {
        if !self.pg.auto_cleanup || self.pg.slot_management == SlotManagement::RequireExisting {
            return Ok(());
        }
        let client = self.conn.client().await?;
        self.drop_slot(&client).await?;
        self.conn
            .command(
                "drop the publication",
                client.batch_execute(&format!(
                    "DROP PUBLICATION IF EXISTS {}",
                    ident(&self.pg.publication)
                )),
            )
            .await
    }

    /// The WAL the slot retains past its confirmed position:
    /// `pg_current_wal_lsn() - confirmed_flush_lsn`, the `max_wal_lag_bytes` guard's measure.
    /// `None` without a slot.
    pub async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
        let client = self.conn.client().await?;
        let row = self
            .conn
            .command(
                "measure the slot's WAL",
                client.query_opt(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
                       FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot_name()],
                ),
            )
            .await?;
        Ok(row
            .and_then(|r| r.get::<_, Option<i64>>(0))
            .map(|b| u64::try_from(b).unwrap_or(0)))
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 56.

Run, with `test-infra/up.sh` running: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg`

Expected: PASS, 11 tests.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/slot.rs \
  crates/data-avalanche-source-pg/tests/pg/main.rs \
  crates/data-avalanche-source-pg/tests/pg/slot.rs
git commit -m "feat(source-pg): the slot state machine and publication reconciliation"
```

---

### Task 9: The change stream

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs` (the `ChangeStream::next` doc: cancel-safe)
- Modify: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/stream.rs`
- Modify: `crates/data-avalanche-source-pg/src/value.rs` (`Decode::reads_char`, `clamped`)
- Modify: `crates/data-avalanche-source-pg/tests/pg/main.rs`
- Modify: `crates/data-avalanche-source-pg/tests/pg/common.rs`
- Create: `crates/data-avalanche-source-pg/tests/pg/stream.rs`

**Interfaces:**
- Consumes: `decode`, `Message`, `Datum` (Task 3); `value::decode` (Task 4); `plan_column`
  (Task 5); `load_types`, `table_key` (Task 6); `PgConnector`, `to_wire`/`from_wire`/`lsn_of`
  (Task 7); `replication_error`, `protocol` (Task 2).
- Produces:
  - Crate root: `PG_EPOCH_MICROS`; `PgStream`, which implements `ChangeStream`.
  - `PgConnector::open_stream(&SourcePosition, &[TableSchema]) -> Result<Box<PgStream>, SourceError>`.
  - `common` adds `first_stream(&PgConnector)`, `collect(&mut PgStream, commits, secs)` and
    `each(&Client, &[String])`.

D-M5.2, D-M5.3, D-M5.4, D-M5.6, D-M5.7. `a_mixed_case_publication_streams` pins the quoted
publication name (Task 8's fix, C2). The stream turns pgoutput into `StreamItem`s:

- Every change carries the commit LSN as its position and its own `wal_start` as its sequence.
- `Commit` carries `end_lsn`, the position to acknowledge.
- A Relation message whose columns differ from the cached schema becomes a `SchemaSignal`. It is
  emitted with the next change's sequence, because Relation messages carry `wal_start` 0.
- A table whose new columns can't be planned halts with `TableHalt` at the same point.
- A changed replica identity (setting or key columns) signals too: the first Relation message
  of a session against the opening identifier, later ones against the last (Q-M5-15).
- A tracked table whose name and relation OID part within the session (renamed away, or
  another table under its name) halts with `TABLE_DROPPED` (Q-M5-15).
- `next` is cancel-safe: the event it is handling stays in the stream until handled.
- `close` waits, up to `timeouts.command`, until the server releases the slot.
- Tables outside the tracked set are dropped silently.
- `acknowledge` sends the position as written, flushed and applied, and never moves it back.

The integration tests are the regression tests of S-PG0…S-PG6: begin/commit order, unchanged
TOAST, replica identity FULL and key updates, truncate, a Relation change mid-stream, keep-alives
on an idle server and acknowledgment moving `confirmed_flush_lsn`. Partitions are covered too
(Q-M5-5): a write through the parent arrives under its leaf, and a row moved across partitions
arrives as a delete in one leaf and an insert in the other.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-source-pg/src/lib.rs` with:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

pub mod catalog;
mod connect;
mod connector;
mod error;
pub mod pgoutput;
mod position;
mod slot;
mod stream;
#[cfg(test)]
mod test_support;
mod tls;
pub mod types;
pub mod value;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
pub use connector::{MIN_SERVER_VERSION, PgConnector};
pub use stream::{PG_EPOCH_MICROS, PgStream};
```

Create `crates/data-avalanche-source-pg/src/stream.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, key: bool, decode: Decode) -> StreamColumn {
        StreamColumn {
            name: name.into(),
            key,
            identifier: key,
            decode,
        }
    }

    fn text(s: &str) -> Datum {
        Datum::Text(bytes::Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn rows_from_tuples() {
        let columns = [
            col("id", true, Decode::Int64),
            col("doc", false, Decode::Text),
            col("n", false, Decode::Numeric),
        ];
        let row = PgStream::row(
            &columns,
            vec![text("7"), Datum::Unchanged, Datum::Null],
            false,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            row,
            Row(vec![
                ("id".into(), ColumnValue::Int64(7)),
                ("doc".into(), ColumnValue::Unchanged),
                ("n".into(), ColumnValue::Null),
            ])
        );
        let key = PgStream::row(&columns, vec![text("7"), Datum::Null, Datum::Null], true)
            .unwrap()
            .unwrap();
        assert_eq!(
            key,
            Row(vec![("id".into(), ColumnValue::Int64(7))]),
            "a K tuple keeps the key"
        );
    }

    #[test]
    fn undecodable_values_halt_the_table() {
        let columns = [col("n", false, Decode::Numeric)];
        assert_eq!(
            PgStream::row(&columns, vec![text("NaN")], false).unwrap(),
            Err(HaltReason::SchemaMismatch {
                column: "n".into(),
                detail: "NaN can't be stored in a decimal column".into()
            })
        );
        assert_eq!(
            PgStream::row(&columns, vec![text(&"9".repeat(40))], false).unwrap(),
            Err(HaltReason::NumericOverflow { column: "n".into() })
        );
        let latin1 = Datum::Text(bytes::Bytes::from_static(&[0xe9]));
        assert!(matches!(
            PgStream::row(&[col("t", false, Decode::Text)], vec![latin1], false).unwrap(),
            Err(HaltReason::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn a_clamped_identifier_halts_the_table() {
        let ts = || Decode::Timestamp { with_tz: false };
        let far = || vec![text("294250-01-01 00:00:00")];
        assert_eq!(
            PgStream::row(&[col("k", true, ts())], far(), false).unwrap(),
            Err(HaltReason::SchemaMismatch {
                column: "k".into(),
                detail: value::CLAMPED_KEY.into()
            })
        );
        let clamped = ColumnValue::Timestamp {
            micros: i64::MAX,
            with_tz: false,
        };
        assert_eq!(
            PgStream::row(&[col("v", false, ts())], far(), false).unwrap(),
            Ok(Row(vec![("v".into(), clamped.clone())])),
            "another column clamps (Q-M5-1)"
        );
        assert_eq!(
            PgStream::row(&[col("k", true, ts())], vec![text("infinity")], false).unwrap(),
            Ok(Row(vec![("k".into(), clamped)])),
            "infinity keys a row"
        );
    }

    #[test]
    fn a_raw_char_byte_decodes() {
        // PostgreSQL 14 sends "char" 0xC3 as the byte itself (measured).
        let raw = || vec![Datum::Text(bytes::Bytes::from_static(&[0xc3]))];
        assert_eq!(
            PgStream::row(&[col("c", false, Decode::Char)], raw(), false).unwrap(),
            Ok(Row(vec![("c".into(), ColumnValue::Int16(195))]))
        );
        let wider = Decode::Convert {
            from: Box::new(Decode::Char),
            to: ColumnValueType::Int64,
        };
        assert_eq!(
            PgStream::row(&[col("c", false, wider)], raw(), false).unwrap(),
            Ok(Row(vec![("c".into(), ColumnValue::Int64(195))]))
        );
    }

    #[test]
    fn a_tuple_of_the_wrong_width_is_a_protocol_error() {
        let e = PgStream::row(&[col("a", false, Decode::Text)], vec![], false).unwrap_err();
        assert!(matches!(e, SourceError::Permanent(m) if m.contains("a tuple of 0 columns")));
    }

    #[test]
    fn commit_times_move_to_the_unix_epoch() {
        // 2000-01-01 00:00:00 UTC.
        assert_eq!(PG_EPOCH_MICROS, 946_684_800 * 1_000_000);
    }
}
```

Replace `crates/data-avalanche-source-pg/tests/pg/main.rs` with:

```rust
//! The PostgreSQL source against a live server (`test-infra/compose.yaml`): `postgres-18` on every
//! change, PostgreSQL 14–17 nightly (`DATA_AVALANCHE_PG_PORT`). Each test returns at once without
//! `DATA_AVALANCHE_IT`.

mod common;
mod resolve;
mod slot;
mod stream;
```

Replace `crates/data-avalanche-source-pg/tests/pg/common.rs` with:

```rust
//! Shared fixtures: each test owns a schema `it_<test>`, a slot `it_<test>` and a publication
//! `it_<test>_pub`, dropped and re-created at its start, so a failed run leaves nothing that
//! breaks the next one.

use std::collections::BTreeMap;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::TableRef;
use data_avalanche_source_pg::PgConnector;
use tokio_postgres::{Client, NoTls};

/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418). The
/// nightly matrix sets it for PostgreSQL 14–17.
pub fn port() -> u16 {
    std::env::var("DATA_AVALANCHE_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55418)
}

/// A superuser connection for test setup.
pub async fn admin() -> Client {
    let dsn = format!(
        "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
        port()
    );
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("connect to test PostgreSQL");
    tokio::spawn(conn);
    client
}

/// The test's names: schema and slot `it_<test>`, publication `it_<test>_pub`.
pub struct Names {
    pub schema: String,
    pub slot: String,
    pub publication: String,
}

impl Names {
    pub fn new(test: &str) -> Self {
        Self {
            schema: format!("it_{test}"),
            slot: format!("it_{test}"),
            publication: format!("it_{test}_pub"),
        }
    }

    pub fn table(&self, name: &str) -> TableRef {
        TableRef::new(&self.schema, name)
    }
}

/// Drops what an earlier run of `test` left (its slot, publication and schema) and creates the
/// schema again, then runs `ddl` in it.
pub async fn fresh(admin: &Client, test: &str, ddl: &str) -> Names {
    let n = Names::new(test);
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&n.slot],
        )
        .await
        .expect("drop the test slot");
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

/// A pipeline on the test server reading `include`, with more `tables` keys (YAML, indented six
/// spaces) and more `postgres` keys (YAML flow entries).
pub fn pipeline(n: &Names, include: &[&str], tables: &str, postgres: &str) -> Pipeline {
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
      postgres: {{ publication: {publication}, slot: {slot}, heartbeat: 1s{postgres} }}
    tables:
      include: [{include}]
{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#,
        port = port(),
        publication = n.publication,
        slot = n.slot,
        include = include.join(", "),
    );
    let loaded = load_str(
        &yaml,
        &BTreeMap::new(),
        &FingerprintKey::from_bytes([1; 32]),
    )
    .unwrap_or_else(|r| panic!("test pipeline: {r:?}"));
    loaded
        .config
        .pipelines
        .into_values()
        .next()
        .expect("one pipeline")
}

pub fn connector(p: &Pipeline) -> PgConnector {
    data_avalanche_testkit::install_crypto_provider();
    PgConnector::new(p).expect("connector")
}

/// The test's slot, publication and schema, dropped at the end of a passing test.
pub async fn cleanup(admin: &Client, n: &Names) {
    release(admin, &n.slot).await;
    admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
            &[&n.slot],
        )
        .await
        .expect("drop the test slot");
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {}; DROP SCHEMA IF EXISTS {} CASCADE;",
            n.publication, n.schema
        ))
        .await
        .expect("drop the test schema");
}

/// Waits until no walsender holds `slot`, terminating one that still does after 5 s (a stream
/// dropped without `close`).
pub async fn release(admin: &Client, slot: &str) {
    for i in 0..100 {
        let pid: Option<i32> = admin
            .query_one(
                "SELECT (SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1)",
                &[&slot],
            )
            .await
            .expect("read the test slot")
            .get(0);
        let Some(pid) = pid else { return };
        if i == 50 {
            admin
                .execute("SELECT pg_terminate_backend($1)", &[&pid])
                .await
                .expect("terminate the walsender");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("slot {slot} is still active");
}

/// `connector`'s resolved tables, by name.
pub async fn resolved(c: &PgConnector) -> data_avalanche_core::ResolvedTables {
    c.resolve_tables().await.expect("resolve tables")
}

/// A stream on a first start: resolve the tables, prepare (creating the slot and publication),
/// open the stream from the slot's start.
pub async fn first_stream(
    c: &PgConnector,
) -> (
    Box<data_avalanche_source_pg::PgStream>,
    Vec<data_avalanche_core::TableSchema>,
    data_avalanche_core::SourcePosition,
) {
    let r = resolved(c).await;
    let refs: Vec<TableRef> = r.tables.iter().map(|t| t.table().clone()).collect();
    let p = c.prepare(None, &refs).await.expect("prepare");
    let data_avalanche_core::StartPlan::Snapshot { from, .. } = p.plan else {
        panic!("a first start snapshots")
    };
    let s = c
        .open_stream(&from, &r.tables)
        .await
        .expect("open the stream");
    (s, r.tables, from)
}

/// Items until `commits` transactions with items committed, or a panic after `secs` seconds.
/// Keep-alives and empty transactions (PostgreSQL 14 sends them, 15 and later don't) are left out.
pub async fn collect(
    s: &mut data_avalanche_source_pg::PgStream,
    commits: usize,
    secs: u64,
) -> Vec<data_avalanche_core::StreamItem> {
    use data_avalanche_core::{ChangeStream, StreamItem};
    let mut out: Vec<StreamItem> = vec![];
    let mut seen = 0;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
    while seen < commits {
        let item = tokio::time::timeout_at(deadline, s.next())
            .await
            .unwrap_or_else(|_| panic!("{seen}/{commits} commits after {secs}s: {out:?}"))
            .expect("stream item");
        match item {
            StreamItem::KeepAlive(_) => continue,
            StreamItem::Commit(_) if matches!(out.last(), Some(StreamItem::Begin { .. })) => {
                out.pop();
                continue;
            }
            StreamItem::Commit(_) => seen += 1,
            _ => {}
        }
        out.push(item);
    }
    out
}

/// Runs each statement as its own transaction (one `batch_execute` of several statements is a
/// single implicit transaction).
pub async fn each(admin: &Client, statements: &[String]) {
    for s in statements {
        admin
            .batch_execute(s)
            .await
            .unwrap_or_else(|e| panic!("{s}: {e}"));
    }
}
```

Create `crates/data-avalanche-source-pg/tests/pg/stream.rs`:

```rust
//! The change stream against a live server: the S-PG0…S-PG6 probe gates as regressions (the M5
//! exit), channel B signals, per-table halts and acknowledgments.

use std::time::Duration;

use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ChangeStream, ColumnValue as V, Decimal, HaltReason, Op, Row, SchemaHint,
    SourceError, SourcePosition, SourceSeq, StreamItem, TableRef,
};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, collect, connector, each, first_stream, fresh, pipeline};

fn changes(items: &[StreamItem]) -> Vec<&CdcEvent> {
    items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => Some(e),
            _ => None,
        })
        .collect()
}

fn row(values: &[(&str, V)]) -> Row {
    Row(values
        .iter()
        .map(|(n, v)| (n.to_string(), v.clone()))
        .collect())
}

fn lsn(p: &SourcePosition) -> Lsn {
    match p {
        SourcePosition::Pg(l) => *l,
        other => panic!("a PostgreSQL position, got {other}"),
    }
}

/// S-PG0: one insert round-trips; the positions are the commit's.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg0_an_insert_round_trips() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg0",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, from) = first_stream(&c).await;
    a.execute(
        &format!("INSERT INTO {}.t VALUES (1, 'one')", n.schema),
        &[],
    )
    .await
    .unwrap();
    let items = collect(&mut s, 1, 20).await;
    let [
        StreamItem::Begin {
            xid,
            commit_ts_micros,
        },
        StreamItem::Change(e),
        StreamItem::Commit(b),
    ] = &items[..]
    else {
        panic!("Begin, Change, Commit: {items:?}")
    };
    assert_eq!(e.op, Op::Insert);
    assert_eq!(e.table, n.table("t"));
    assert_eq!(
        e.after,
        Some(row(&[("id", V::Int64(1)), ("v", V::Text("one".into()))]))
    );
    assert_eq!(e.before, None);
    e.validate().unwrap();
    assert_eq!((e.xid, b.xid), (*xid, *xid));
    assert_eq!(e.commit_ts_micros, *commit_ts_micros);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    assert!(
        (now - commit_ts_micros).abs() < 60_000_000,
        "a Unix-epoch commit time"
    );
    // `_lsn` is the commit record's LSN; the checkpoint is the commit's end, past it; the
    // change's own LSN is before both.
    let SourceSeq::Pg(seq) = e.change_seq else {
        panic!()
    };
    assert!(
        lsn(&from) <= seq && seq < lsn(&e.source_pos) && lsn(&e.source_pos) < lsn(&b.commit_pos)
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG1: every message the design consumes, with the before-image each replica identity sends.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg1_every_change_kind() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg1",
        "CREATE TABLE t (id int PRIMARY KEY, v text);
         CREATE TABLE f (id int PRIMARY KEY, v text);
         ALTER TABLE f REPLICA IDENTITY FULL;
         INSERT INTO t VALUES (1, 'a'), (2, 'b');
         INSERT INTO f VALUES (1, 'x');",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    a.batch_execute(&format!(
        "BEGIN;
         INSERT INTO {sc}.t VALUES (3, 'c');
         UPDATE {sc}.t SET v = 'a2' WHERE id = 1;
         UPDATE {sc}.t SET id = 20 WHERE id = 2;
         DELETE FROM {sc}.t WHERE id = 3;
         UPDATE {sc}.f SET v = 'y' WHERE id = 1;
         DELETE FROM {sc}.f WHERE id = 1;
         COMMIT;
         TRUNCATE {sc}.t, {sc}.f;"
    ))
    .await
    .unwrap();
    let items = collect(&mut s, 2, 20).await;
    let c = changes(&items);
    let id = |i: i32| ("id", V::Int32(i));
    let v = |s: &str| ("v", V::Text(s.into()));
    let shapes: Vec<(Op, Option<Row>, Option<Row>)> = c
        .iter()
        .map(|e| (e.op, e.before.clone(), e.after.clone()))
        .collect();
    assert_eq!(
        shapes,
        [
            (Op::Insert, None, Some(row(&[id(3), v("c")]))),
            (Op::Update, None, Some(row(&[id(1), v("a2")]))),
            (
                Op::Update,
                Some(row(&[id(2)])),
                Some(row(&[id(20), v("b")]))
            ),
            (Op::Delete, Some(row(&[id(3)])), None),
            (
                Op::Update,
                Some(row(&[id(1), v("x")])),
                Some(row(&[id(1), v("y")]))
            ),
            (Op::Delete, Some(row(&[id(1), v("y")])), None),
        ],
        "default identity: no old image unless the key moves, then the key only; FULL: the whole row"
    );
    let seqs: Vec<SourceSeq> = c.iter().map(|e| e.change_seq).collect();
    assert!(
        seqs.windows(2)
            .all(|w| w[0].partial_cmp(&w[1]) == Some(std::cmp::Ordering::Less))
    );
    let truncates: Vec<&Vec<TableRef>> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Truncate { tables, .. } => Some(tables),
            _ => None,
        })
        .collect();
    assert_eq!(
        truncates,
        [&vec![n.table("t"), n.table("f")]],
        "one TRUNCATE of both tables"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG2: an update that doesn't touch an out-of-line TOASTed column sends it unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg2_unchanged_toast() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg2",
        "CREATE TABLE t (id bigint PRIMARY KEY, big text, n int);
         ALTER TABLE t ALTER COLUMN big SET STORAGE EXTERNAL;",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t SELECT 1, string_agg(md5(g::text), ''), 0 FROM generate_series(1, 400) g"),
            format!("UPDATE {sc}.t SET n = 1 WHERE id = 1"),
        ],
    )
    .await;
    let items = collect(&mut s, 2, 20).await;
    let c = changes(&items);
    let big = |e: &CdcEvent| e.after.as_ref().unwrap().get("big").cloned();
    assert!(matches!(big(c[0]), Some(V::Text(t)) if t.len() == 12_800));
    assert_eq!(big(c[1]), Some(V::Unchanged));
    assert_eq!(c[1].after.as_ref().unwrap().get("n"), Some(&V::Int32(1)));
    c[1].validate().unwrap();
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG3: a transaction far beyond `logical_decoding_work_mem` arrives whole at its commit.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg3_a_large_transaction_arrives_whole() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg3",
        "CREATE TABLE t (id bigint PRIMARY KEY, pad text);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    a.execute(
        &format!(
            "INSERT INTO {}.t SELECT g, repeat('x', 200) FROM generate_series(1, 50000) g",
            n.schema
        ),
        &[],
    )
    .await
    .unwrap();
    let items = collect(&mut s, 1, 120).await;
    let begins = items
        .iter()
        .filter(|i| matches!(i, StreamItem::Begin { .. }))
        .count();
    let c = changes(&items);
    assert_eq!((begins, c.len()), (1, 50_000));
    assert_eq!(
        c[49_999].after.as_ref().unwrap().get("id"),
        Some(&V::Int64(50_000))
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG4: a restart from the acknowledged position replays the unacknowledged transaction only;
/// the acknowledgment reaches the slot.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg4_resume_from_the_acknowledged_position() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "s_pg4", "CREATE TABLE t (id bigint PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, tables, _) = first_stream(&c).await;
    a.execute(&format!("INSERT INTO {}.t VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    let first = collect(&mut s, 1, 20).await;
    let Some(StreamItem::Commit(b)) = first.last() else {
        panic!("{first:?}")
    };
    s.acknowledge(&b.commit_pos).await.unwrap();
    s.acknowledge(&SourcePosition::Pg(Lsn(1))).await.unwrap(); // behind: ignored
    a.execute(&format!("INSERT INTO {}.t VALUES (2)", n.schema), &[])
        .await
        .unwrap();
    collect(&mut s, 1, 20).await; // received, not acknowledged
    let mut confirmed = None;
    for _ in 0..50 {
        let r = a
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&n.slot],
            )
            .await
            .unwrap();
        confirmed = Some(SourcePosition::Pg(r.get::<_, String>(0).parse().unwrap()));
        if confirmed.as_ref() == Some(&b.commit_pos) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        confirmed.as_ref(),
        Some(&b.commit_pos),
        "the status update carried the acknowledgment"
    );
    s.close().await.unwrap();

    let mut s = c.open_stream(&b.commit_pos, &tables).await.unwrap();
    let again = collect(&mut s, 1, 20).await;
    let ids: Vec<&V> = changes(&again)
        .iter()
        .map(|e| e.after.as_ref().unwrap().get("id").unwrap())
        .collect();
    assert_eq!(
        ids,
        [&V::Int64(2)],
        "the acknowledged transaction isn't sent again"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG5: a consumer stalled past `wal_sender_timeout` with the client's buffer full keeps its
/// connection (the replication crate keeps sending status updates).
#[tokio::test(flavor = "multi_thread")]
async fn s_pg5_a_stalled_consumer_survives() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "s_pg5", "CREATE TABLE t (id bigint PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    // Server-wide, so other tests' streams see it too: their status interval is 1 s. The stalled
    // half runs as its own task, so the setting is reset when it fails too.
    a.batch_execute("ALTER SYSTEM SET wal_sender_timeout = '3s'")
        .await
        .unwrap();
    a.batch_execute("SELECT pg_reload_conf()").await.unwrap();
    let schema = n.schema.clone();
    let stalled = tokio::spawn(async move {
        // 10 000 changes overflow the client's 8 192-event buffer.
        admin()
            .await
            .execute(
                &format!("INSERT INTO {schema}.t SELECT g FROM generate_series(1, 10000) g"),
                &[],
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        let items = collect(&mut s, 1, 60).await;
        (s, items)
    })
    .await;
    a.batch_execute("ALTER SYSTEM RESET wal_sender_timeout")
        .await
        .unwrap();
    a.batch_execute("SELECT pg_reload_conf()").await.unwrap();
    let (s, items) = stalled.unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()));
    assert_eq!(changes(&items).len(), 10_000);
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG6: a table added to the publication mid-stream streams from the ALTER on, without a
/// restart.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg6_a_table_added_to_the_publication_mid_stream() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg6",
        "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let r = crate::common::resolved(&c).await;
    let p = c.prepare(None, &[n.table("a")]).await.unwrap();
    let data_avalanche_core::StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.b VALUES (1)"),
            format!("INSERT INTO {sc}.a VALUES (1)"),
            format!("ALTER PUBLICATION {} ADD TABLE {sc}.b", n.publication),
            format!("INSERT INTO {sc}.b VALUES (2)"),
        ],
    )
    .await;
    let items = collect(&mut s, 2, 20).await;
    let got: Vec<(String, &V)> = changes(&items)
        .iter()
        .map(|e| {
            (
                e.table.name.clone(),
                e.after.as_ref().unwrap().get("id").unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [("a".into(), &V::Int32(1)), ("b".into(), &V::Int32(2))]
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Q-M5-5: partitions replicate as tables. A write through the parent arrives under its leaf, a
/// row whose partition key moves it arrives as a delete in one leaf and an insert in the other, and
/// a TRUNCATE of the parent truncates the leaves.
#[tokio::test(flavor = "multi_thread")]
async fn partition_changes_arrive_under_their_leaves() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "stream_partitions",
        "CREATE TABLE events (id int, at date, note text, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
         CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
         CREATE TABLE events_2025 PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, tables, _) = first_stream(&c).await;
    let tracked: Vec<&TableRef> = tables.iter().map(|t| t.table()).collect();
    assert_eq!(
        tracked,
        [&n.table("events_2024"), &n.table("events_2025")],
        "the leaves, not the parent"
    );
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.events VALUES (1, '2024-06-01', 'a')"),
            format!("UPDATE {sc}.events SET at = '2025-06-01' WHERE id = 1"),
            format!("TRUNCATE {sc}.events"),
        ],
    )
    .await;
    let items = collect(&mut s, 3, 20).await;
    let id = ("id", V::Int32(1));
    let at = |days: i32| ("at", V::Date(days));
    let note = ("note", V::Text("a".into()));
    let shapes: Vec<(Op, &TableRef, Option<Row>, Option<Row>)> = changes(&items)
        .iter()
        .map(|e| (e.op, &e.table, e.before.clone(), e.after.clone()))
        .collect();
    assert_eq!(
        shapes,
        [
            (
                Op::Insert,
                &n.table("events_2024"),
                None,
                Some(row(&[id.clone(), at(19875), note.clone()]))
            ),
            (
                Op::Delete,
                &n.table("events_2024"),
                Some(row(&[id.clone(), at(19875)])),
                None
            ),
            (
                Op::Insert,
                &n.table("events_2025"),
                None,
                Some(row(&[id.clone(), at(20240), note.clone()]))
            ),
        ],
        "the moved row leaves one leaf and enters the other"
    );
    let truncates: Vec<&Vec<TableRef>> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Truncate { tables, .. } => Some(tables),
            _ => None,
        })
        .collect();
    assert_eq!(
        truncates,
        [&vec![n.table("events_2024"), n.table("events_2025")]],
        "a TRUNCATE of the parent truncates its leaves"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Channel B: a changed column list signals before the change that carries it; an unchanged one
/// doesn't.
#[tokio::test(flavor = "multi_thread")]
async fn relation_changes_signal_before_the_next_change() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_b",
        "CREATE TABLE t (id int PRIMARY KEY, v int);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t VALUES (1, 1)"),
            format!("ALTER TABLE {sc}.t ADD COLUMN w text"),
            format!("INSERT INTO {sc}.t VALUES (2, 2, 'two')"),
            format!("ALTER TABLE {sc}.t ALTER COLUMN v TYPE bigint"),
            format!("INSERT INTO {sc}.t VALUES (3, 3, 'three')"),
            format!("ALTER TABLE {sc}.t RENAME COLUMN w TO w2"),
            format!("UPDATE {sc}.t SET w2 = 'x' WHERE id = 3"),
        ],
    )
    .await;
    let items = collect(&mut s, 4, 20).await;
    let kinds: Vec<String> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::SchemaSignal {
                hint: SchemaHint::Columns(c),
                ..
            } => {
                let c: Vec<String> = c
                    .iter()
                    .map(|c| format!("{}:{}", c.name, c.source_type))
                    .collect();
                Some(format!("signal {}", c.join(",")))
            }
            StreamItem::Change(_) => Some("change".into()),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "change",
            "signal id:integer,v:integer,w:text",
            "change",
            "signal id:integer,v:bigint,w:text",
            "change",
            "signal id:integer,v:bigint,w2:text",
            "change",
        ],
        "no signal for the first Relation message: the shape is the one the stream opened with"
    );
    // A signal's position is its change's.
    let pos: Vec<SourceSeq> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::SchemaSignal { seq, .. } => Some(*seq),
            StreamItem::Change(e) => Some(e.change_seq),
            _ => None,
        })
        .collect();
    assert_eq!(pos[1], pos[2]);
    let StreamItem::Change(last) = items
        .iter()
        .rev()
        .find(|i| matches!(i, StreamItem::Change(_)))
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(
        last.after.as_ref().unwrap().get("v"),
        Some(&V::Int64(3)),
        "decoded with the new type"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// A value with no representation halts its table once; the stream and the other tables go on.
#[tokio::test(flavor = "multi_thread")]
async fn an_undecodable_value_halts_only_its_table() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "value_halt", "CREATE TABLE t (id int PRIMARY KEY, n numeric(10,2)); CREATE TABLE u (id int PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t VALUES (1, 'NaN')"),
            format!("INSERT INTO {sc}.t VALUES (2, 1.5)"),
            format!("INSERT INTO {sc}.u VALUES (1)"),
        ],
    )
    .await;
    // The second transaction holds only a skipped change: it is empty.
    let items = collect(&mut s, 2, 20).await;
    let halts: Vec<(&TableRef, &HaltReason)> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::TableHalt { table, reason, .. } => Some((table, reason)),
            _ => None,
        })
        .collect();
    assert_eq!(
        halts,
        [(
            &n.table("t"),
            &HaltReason::SchemaMismatch {
                column: "n".into(),
                detail: "NaN can't be stored in a decimal column".into()
            }
        )]
    );
    let tables: Vec<&str> = changes(&items)
        .iter()
        .map(|e| e.table.name.as_str())
        .collect();
    assert_eq!(
        tables,
        ["u"],
        "the halted table's later changes are skipped"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Tables the stream wasn't opened for are skipped; a busy slot is a permanent error; an idle
/// stream reports the WAL end in keep-alives.
#[tokio::test(flavor = "multi_thread")]
async fn untracked_tables_busy_slots_and_keepalives() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "stream_misc",
        "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let r = crate::common::resolved(&c).await;
    let p = c
        .prepare(None, &[n.table("a"), n.table("b")])
        .await
        .unwrap();
    let data_avalanche_core::StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let only_a: Vec<_> = r
        .tables
        .iter()
        .filter(|t| t.table().name == "a")
        .cloned()
        .collect();
    let mut s = c.open_stream(&from, &only_a).await.unwrap();
    each(
        &a,
        &[
            format!("INSERT INTO {}.b VALUES (1)", n.schema),
            format!("INSERT INTO {}.a VALUES (1)", n.schema),
        ],
    )
    .await;
    // The first transaction holds only an untracked change: it is empty.
    let items = collect(&mut s, 1, 20).await;
    let tables: Vec<&str> = changes(&items)
        .iter()
        .map(|e| e.table.name.as_str())
        .collect();
    assert_eq!(tables, ["a"]);
    let Some(StreamItem::Commit(last)) = items.last() else {
        panic!()
    };

    let mut busy = c.open_stream(&from, &only_a).await.unwrap();
    match busy.next().await {
        Err(SourceError::Permanent(m)) => assert!(m.contains("is active for PID"), "{m}"),
        other => panic!("a busy slot is permanent: {other:?}"),
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let keepalive = loop {
        match tokio::time::timeout_at(deadline, s.next())
            .await
            .expect("a keep-alive")
            .unwrap()
        {
            StreamItem::KeepAlive(p) => break p,
            // PostgreSQL 14 sends every transaction of the database, empty ones included.
            StreamItem::Begin { .. } | StreamItem::Commit(_) => {}
            other => panic!("only keep-alives while idle: {other:?}"),
        }
    };
    assert!(
        lsn(&keepalive) >= lsn(&last.commit_pos),
        "{keepalive} vs {}",
        last.commit_pos
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// A publication named with capitals is created quoted (`"It_Mixed_Pub"`), so the stream asks
/// for it quoted: pgoutput would fold `It_Mixed_Pub` to `it_mixed_pub` and stream nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_case_publication_streams() {
    require_it!();
    let a = admin().await;
    let mut n = fresh(&a, "mixed_pub", "CREATE TABLE t (id int PRIMARY KEY);").await;
    n.publication = "It_Mixed_Pub".into();
    a.batch_execute("DROP PUBLICATION IF EXISTS \"It_Mixed_Pub\"")
        .await
        .unwrap();
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    a.execute(&format!("INSERT INTO {}.t VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    let items = collect(&mut s, 1, 20).await;
    assert_eq!(changes(&items).len(), 1, "{items:?}");
    s.close().await.unwrap();
    a.batch_execute("DROP PUBLICATION \"It_Mixed_Pub\"")
        .await
        .unwrap();
    cleanup(&a, &n).await;
}

/// Channel B: a changed primary key or replica identity signals (channel C halts the table on
/// an identifier change). A `"char"` byte above 127 decodes (PostgreSQL 14 sends it raw).
#[tokio::test(flavor = "multi_thread")]
async fn key_and_identity_changes_signal() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "stream_keys",
        "CREATE TABLE t (id int PRIMARY KEY, code int NOT NULL, c \"char\");",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t VALUES (1, 1, chr(195)::\"char\")"),
            format!("ALTER TABLE {sc}.t DROP CONSTRAINT t_pkey, ADD PRIMARY KEY (code)"),
            format!("INSERT INTO {sc}.t VALUES (2, 2)"),
            format!("ALTER TABLE {sc}.t REPLICA IDENTITY FULL"),
            format!("INSERT INTO {sc}.t VALUES (3, 3)"),
            format!("DELETE FROM {sc}.t WHERE code = 3"),
        ],
    )
    .await;
    let items = collect(&mut s, 4, 20).await;
    let kinds: Vec<&str> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::SchemaSignal { .. } => Some("signal"),
            StreamItem::Change(_) => Some("change"),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        ["change", "signal", "change", "signal", "change", "change"],
        "no signal for the first Relation message: its key is the opening identifier"
    );
    let c = changes(&items);
    assert_eq!(c[0].after.as_ref().unwrap().get("c"), Some(&V::Int16(195)));
    assert_eq!(
        c[3].before,
        Some(row(&[
            ("id", V::Int32(3)),
            ("code", V::Int32(3)),
            ("c", V::Null)
        ])),
        "REPLICA IDENTITY FULL: the whole old row"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// A tracked table renamed away, or replaced by another table under its name, halts
/// (`TABLE_DROPPED`): a re-created table doesn't resume (`Schema_Change…` §7).
#[tokio::test(flavor = "multi_thread")]
async fn a_renamed_or_replaced_table_halts() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "stream_renames",
        "CREATE TABLE t (id int PRIMARY KEY);
         CREATE TABLE u (id int PRIMARY KEY);
         CREATE TABLE u_new (id int PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t", "u"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t VALUES (1)"),
            format!("INSERT INTO {sc}.u VALUES (1)"),
            format!("ALTER PUBLICATION {} ADD TABLE {sc}.u_new", n.publication),
            format!("ALTER TABLE {sc}.t RENAME TO t_old"),
            format!("INSERT INTO {sc}.t_old VALUES (2)"),
            format!(
                "BEGIN; ALTER TABLE {sc}.u RENAME TO u_old; ALTER TABLE {sc}.u_new RENAME TO u; COMMIT"
            ),
            format!("INSERT INTO {sc}.u VALUES (2)"),
            format!("INSERT INTO {sc}.u_old VALUES (3)"),
            format!("INSERT INTO {sc}.t_old VALUES (3)"),
            format!("INSERT INTO {sc}.u VALUES (3)"),
        ],
    )
    .await;
    // Four transactions with items: two changes, two halts; the later ones are skipped.
    let items = collect(&mut s, 4, 20).await;
    let got: Vec<String> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => Some(format!("change {}", e.table.name)),
            StreamItem::TableHalt { table, reason, .. } => {
                Some(format!("halt {} {}", table.name, reason.code()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        got,
        [
            "change t",
            "change u",
            "halt t TABLE_DROPPED",
            "halt u TABLE_DROPPED"
        ]
    );
    // Nothing more: the halted tables' later changes are skipped under either OID.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while let Ok(item) = tokio::time::timeout_at(deadline, s.next()).await {
        let item = item.unwrap();
        assert!(
            matches!(
                item,
                StreamItem::KeepAlive(_) | StreamItem::Begin { .. } | StreamItem::Commit(_)
            ),
            "{item:?}"
        );
    }
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// `next` is cancel-safe: calls dropped at every await, the Relation message's catalog queries
/// among them, lose nothing.
#[tokio::test(flavor = "multi_thread")]
async fn next_is_cancel_safe() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "stream_cancel", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            // A type the stream hasn't read yet: its Relation message queries the catalog.
            format!("ALTER TABLE {sc}.t ADD COLUMN w numeric(7,3)"),
            format!("INSERT INTO {sc}.t VALUES (1, 1.5)"),
        ],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await; // the events are in the client's buffer
    let mut items = vec![];
    let mut cancelled = 0;
    let mut wait = Duration::from_micros(20);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !items.iter().any(|i| matches!(i, StreamItem::Change(_))) {
        assert!(tokio::time::Instant::now() < deadline, "{items:?}");
        match tokio::time::timeout(wait, s.next()).await {
            Ok(item) => items.push(item.expect("stream item")),
            Err(_) => {
                cancelled += 1;
                wait = (wait * 2).min(Duration::from_millis(200));
            }
        }
    }
    assert!(cancelled > 0, "no call was cancelled");
    let kinds: Vec<&str> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::SchemaSignal { .. } => Some("signal"),
            StreamItem::Change(_) => Some("change"),
            _ => None,
        })
        .collect();
    assert_eq!(kinds, ["signal", "change"], "{items:?}");
    assert_eq!(
        changes(&items)[0].after,
        Some(row(&[
            ("id", V::Int32(1)),
            ("w", V::Numeric(Decimal::parse("1.500").unwrap()))
        ]))
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `stream::PG_EPOCH_MICROS`, `stream::PgStream`
error[E0425]: cannot find type `Decode` in this scope
error[E0425]: cannot find type `StreamColumn` in this scope
error[E0422]: cannot find struct, variant or union type `StreamColumn` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-source-pg/src/value.rs`: the stream is the first caller of `Decode::reads_char` and of
`clamped`, which tells a key that clamps (Q-M5-18) from `±infinity`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/value.rs b/crates/data-avalanche-source-pg/src/value.rs
--- a/crates/data-avalanche-source-pg/src/value.rs
+++ b/crates/data-avalanche-source-pg/src/value.rs
@@ -56,6 +56,17 @@
     },
 }
 
+impl Decode {
+    /// Whether this reads a `"char"` (directly or before a conversion).
+    pub(crate) fn reads_char(&self) -> bool {
+        match self {
+            Decode::Char => true,
+            Decode::Convert { from, .. } => from.reads_char(),
+            _ => false,
+        }
+    }
+}
+
 /// Why a value can't be decoded.
 #[derive(Debug, Clone, PartialEq, Eq)]
 pub enum ValueError {
@@ -67,6 +78,23 @@
 
 fn bad(what: &str, text: &str) -> ValueError {
     ValueError::Unrepresentable(format!("{what} {text:?}"))
+}
+
+/// Why an identifier value that clamps halts its table (Q-M5-18).
+pub(crate) const CLAMPED_KEY: &str = "an instant at or past the last microsecond a timestamp \
+    holds (294247-01-10 04:00:54.775807 UTC) clamps to the value of infinity, and can't key a \
+    row: distinct keys would merge";
+
+/// Whether `value`, decoded from `text`, is a clamped instant: at or past the last `i64`
+/// microsecond (Q-M5-1), not `±infinity`, which decodes to the same bound.
+pub(crate) fn clamped(value: &ColumnValue, text: &str) -> bool {
+    matches!(
+        value,
+        ColumnValue::Timestamp {
+            micros: i64::MAX | i64::MIN,
+            ..
+        }
+    ) && !matches!(text, "infinity" | "-infinity")
 }
 
 /// Decodes the text of one value.
@@ -662,6 +690,21 @@
             timestamp("0001-01-01 00:00:00 BC", false),
             Ok(-62_167_219_200_000_000)
         );
+        let at = |micros| ColumnValue::Timestamp {
+            micros,
+            with_tz: false,
+        };
+        assert!(clamped(&at(i64::MAX), "294250-01-01 00:00:00"));
+        // The last microsecond itself is the value of infinity.
+        assert_eq!(
+            timestamp("294247-01-10 04:00:54.775807", false),
+            Ok(i64::MAX)
+        );
+        assert!(clamped(&at(i64::MAX), "294247-01-10 04:00:54.775807"));
+        assert!(clamped(&at(i64::MAX), "300000-01-01"), "a date converted");
+        assert!(!clamped(&at(i64::MAX), "infinity"));
+        assert!(!clamped(&at(i64::MIN), "-infinity"));
+        assert!(!clamped(&at(0), "1970-01-01 00:00:00"));
         assert_eq!(timestamp("infinity", true), Ok(i64::MAX));
         assert_eq!(timestamp("-infinity", false), Ok(i64::MIN));
         assert_eq!(
```

Add the implementation to `crates/data-avalanche-source-pg/src/stream.rs`, above the test module:

```rust
//! The change stream (Data-Flow Phase 2a): `pgwire-replication` events and decoded pgoutput
//! messages → `StreamItem`s. Relation messages are channel B (`Schema_Change…` §3): a tracked
//! table whose columns differ from what the stream last saw gets a `SchemaSignal` before its next
//! change, at that change's position. A Relation message carries no position of its own: its
//! `XLogData` start is 0 (measured on PostgreSQL 18, D-M5.3). A changed replica identity signals
//! too, and a tracked table whose name and relation OID part within the session halts
//! (`TABLE_DROPPED`, `Schema_Change…` §7).
//!
//! `next` is cancel-safe (Data-Flow Phase 2a runs it in `tokio::select!`): the event it is
//! handling stays in the stream until handled, so a call dropped at a Relation message's catalog
//! query handles the message again on the next call.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::time::Duration;

use async_trait::async_trait;
use data_avalanche_config::Tables;
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ChangeStream, ColumnSchema, ColumnValue, ColumnValueType, HaltReason, Op, Row,
    SchemaHint, SourceError, SourcePosition, SourceSeq, StreamItem, TableRef, TableSchema,
    TransactionBoundary,
};
use pgwire_replication::{ReplicationClient, ReplicationEvent};
use tokio_postgres::Client;

use crate::catalog::{load_types, table_key};
use crate::connect::within;
use crate::connector::PgConnector;
use crate::error::{protocol, replication_error};
use crate::pgoutput::{Datum, Message, OldKind, RelationColumn, decode};
use crate::position::{from_wire, lsn_of, to_wire};
use crate::types::{TypeCatalog, plan_column};
use crate::value::{self, Decode, ValueError};

/// Microseconds from the Unix epoch to PostgreSQL's (2000-01-01).
pub const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// One column as the stream decodes it.
#[derive(Debug, Clone)]
struct StreamColumn {
    name: String,
    key: bool,
    /// One of the table's identifier columns: a value that clamps halts the table (Q-M5-18).
    identifier: bool,
    decode: Decode,
}

/// What the stream knows about a relation OID from its last Relation message.
#[derive(Debug)]
enum RelationState {
    /// Not a table the stream was opened for.
    Untracked,
    /// A tracked table that halted: its changes are skipped.
    Halted,
    /// A tracked table whose new columns can't be planned: it halts at its next change.
    Halting { table: TableRef, reason: HaltReason },
    Tracked {
        table: TableRef,
        columns: Vec<StreamColumn>,
        /// A shape change to announce before the next change.
        signal: Option<SchemaHint>,
    },
}

/// The open transaction.
#[derive(Debug, Clone, Copy)]
struct Txn {
    xid: u64,
    /// The commit record's LSN (Begin `final_lsn`): the events' `source_pos`, `_lsn` (D-M5.2).
    commit_lsn: Lsn,
    commit_ts_micros: i64,
}

/// The column shape channel B compares: name, decoded type and the catalog's type text.
type Shape = Vec<(String, ColumnValueType, String)>;

fn shape_of(columns: &[ColumnSchema]) -> Shape {
    columns
        .iter()
        .map(|c| (c.name.clone(), c.value_type.clone(), c.source_type.clone()))
        .collect()
}

/// The replica identity of a Relation message: its setting (`d` default, `n` nothing, `f` full,
/// `i` index) and the columns it flags as key.
type Identity = (u8, BTreeSet<String>);

/// What the stream knows about a table it was opened for.
#[derive(Debug)]
struct Known {
    /// Its shape when the stream last saw it.
    shape: Shape,
    /// Its identifier when the stream opened.
    identifier: BTreeSet<String>,
    /// Its replica identity in its last Relation message of this session.
    identity: Option<Identity>,
    /// Its relation OID, bound by its first Relation message of this session.
    oid: Option<u32>,
    /// Halted: its changes are skipped, under any OID.
    halted: bool,
}

/// The stream of one `open_stream` call.
pub struct PgStream {
    client: ReplicationClient,
    /// Its own catalog connection, for the types and type names of Relation messages.
    catalog: Client,
    types: TypeCatalog,
    tables: Tables,
    /// The tables the stream was opened for.
    known: HashMap<TableRef, Known>,
    relations: HashMap<u32, RelationState>,
    type_names: HashMap<(u32, i32), String>,
    txn: Option<Txn>,
    pending: VecDeque<StreamItem>,
    /// The event `next` is handling, until it is handled (cancel safety).
    inflight: Option<ReplicationEvent>,
    slot: String,
    /// `timeouts.command`: the catalog queries, and the wait for the slot's release.
    command: Duration,
    start: Lsn,
    acknowledged: Lsn,
}

impl std::fmt::Debug for PgStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgStream")
            .field("start", &self.start)
            .field("acknowledged", &self.acknowledged)
            .field("tables", &self.known.len())
            .finish_non_exhaustive()
    }
}

impl PgConnector {
    /// Opens the replication stream after `from` for `tables`, their schemas as of `from`. The
    /// stream has its own catalog connection for the types its Relation messages name.
    pub async fn open_stream(
        &self,
        from: &SourcePosition,
        tables: &[TableSchema],
    ) -> Result<Box<PgStream>, SourceError> {
        let start = lsn_of(from)?;
        let catalog = self.conn.connect().await?;
        let types = self.types.lock().await.clone();
        let config = self.conn.replication(
            &self.pg.slot,
            &self.pg.publication,
            to_wire(start),
            self.pg.heartbeat,
        );
        let client = ReplicationClient::connect(config)
            .await
            .map_err(|e| replication_error(e, Some(start.to_string())))?;
        Ok(Box::new(PgStream::new(
            client,
            catalog,
            types,
            self.pipeline.tables.clone(),
            tables,
            (self.pg.slot.clone(), self.conn.timeouts.command),
            start,
        )))
    }
}

impl PgStream {
    fn new(
        client: ReplicationClient,
        catalog: Client,
        types: TypeCatalog,
        tables: Tables,
        schemas: &[TableSchema],
        (slot, command): (String, Duration),
        start: Lsn,
    ) -> Self {
        Self {
            client,
            catalog,
            types,
            tables,
            known: schemas
                .iter()
                .map(|s| {
                    let known = Known {
                        shape: shape_of(s.columns()),
                        identifier: s.identifier().iter().cloned().collect(),
                        identity: None,
                        oid: None,
                        halted: false,
                    };
                    (s.table().clone(), known)
                })
                .collect(),
            relations: HashMap::new(),
            type_names: HashMap::new(),
            txn: None,
            pending: VecDeque::new(),
            inflight: None,
            slot,
            command,
            start,
            acknowledged: start,
        }
    }

    /// `format_type` of each column's type, cached.
    async fn type_names(&mut self, columns: &[RelationColumn]) -> Result<Vec<String>, SourceError> {
        let wanted: Vec<(u32, i32)> = columns
            .iter()
            .map(|c| (c.type_oid, c.typmod))
            .filter(|k| !self.type_names.contains_key(k))
            .collect();
        if !wanted.is_empty() {
            let oids: Vec<u32> = wanted.iter().map(|w| w.0).collect();
            let mods: Vec<i32> = wanted.iter().map(|w| w.1).collect();
            let rows = within(
                "read type names",
                self.command,
                self.catalog.query(
                    "SELECT x.o, x.m, pg_catalog.format_type(x.o, x.m)
                       FROM unnest($1::oid[], $2::int4[]) AS x(o, m)",
                    &[&oids, &mods],
                ),
            )
            .await?;
            for r in rows {
                self.type_names.insert((r.get(0), r.get(1)), r.get(2));
            }
        }
        Ok(columns
            .iter()
            .map(|c| self.type_names[&(c.type_oid, c.typmod)].clone())
            .collect())
    }

    /// Channel B: a Relation message. A tracked table will get a `SchemaSignal` when its shape or
    /// replica identity changed, or a `TableHalt` when a column can't be planned any more or the
    /// table's name and OID parted ([`Self::announce`]). Everything before the catalog queries is
    /// idempotent, so a message handled again after a cancelled `next` ends the same way.
    async fn relation(
        &mut self,
        oid: u32,
        table: TableRef,
        replica_identity: u8,
        columns: Vec<RelationColumn>,
    ) -> Result<(), SourceError> {
        match self.relations.get(&oid) {
            Some(RelationState::Halted) => return Ok(()),
            // A tracked table's OID under another name: the table was renamed away. Channel D
            // finds it dropped too.
            Some(RelationState::Tracked { table: was, .. }) if *was != table => {
                let was = was.clone();
                self.relations.insert(
                    oid,
                    RelationState::Halting {
                        table: was,
                        reason: HaltReason::TableDropped,
                    },
                );
                return Ok(());
            }
            Some(RelationState::Halting { table: was, .. }) if *was != table => return Ok(()),
            _ => {}
        }
        let Some(known) = self.known.get_mut(&table) else {
            self.relations.insert(oid, RelationState::Untracked);
            return Ok(());
        };
        if known.halted {
            self.relations.insert(oid, RelationState::Halted);
            return Ok(());
        }
        match known.oid {
            // A tracked table's name on another OID: the table was dropped or renamed away and
            // another took its name. A re-created table doesn't resume (`Schema_Change…` §7).
            Some(bound) if bound != oid => {
                self.relations.insert(
                    oid,
                    RelationState::Halting {
                        table,
                        reason: HaltReason::TableDropped,
                    },
                );
                return Ok(());
            }
            _ => known.oid = Some(oid),
        }
        within(
            "load types",
            self.command,
            load_types(
                &self.catalog,
                columns.iter().map(|c| c.type_oid),
                &mut self.types,
            ),
        )
        .await?;
        let type_names = self.type_names(&columns).await?;
        let identifier = self.known[&table].identifier.clone();
        let key = table_key(&table);
        let mut schema = Vec::with_capacity(columns.len());
        let mut stream = Vec::with_capacity(columns.len());
        for (c, type_text) in columns.iter().zip(type_names) {
            let over = self
                .tables
                .column_type_overrides
                .get(&format!("{key}.{}", c.name));
            match plan_column(
                &c.name,
                c.type_oid,
                c.typmod,
                &self.types,
                self.tables.numeric_overflow_action,
                over,
            ) {
                Ok(p) => {
                    schema.push(ColumnSchema {
                        name: c.name.clone(),
                        value_type: p.value_type,
                        logical: p.logical,
                        nullable: true,
                        source_type: type_text,
                        source_ordinal: None,
                    });
                    stream.push(StreamColumn {
                        name: c.name.clone(),
                        key: c.key,
                        identifier: identifier.contains(&c.name),
                        decode: p.decode,
                    });
                }
                Err(reason) => {
                    self.relations
                        .insert(oid, RelationState::Halting { table, reason });
                    return Ok(());
                }
            }
        }
        let identity: Identity = (
            replica_identity,
            columns
                .iter()
                .filter(|c| c.key)
                .map(|c| c.name.clone())
                .collect(),
        );
        let known = self.known.get_mut(&table).expect("a tracked table");
        // Before its first Relation message, a table's replica identity is the one its schema
        // implies: every column under REPLICA IDENTITY FULL, else its identifier. Another index
        // as the identity signals once per session; channel C finds nothing changed.
        let before = known.identity.take().unwrap_or_else(|| {
            let keys = if replica_identity == b'f' {
                columns.iter().map(|c| c.name.clone()).collect()
            } else {
                known.identifier.clone()
            };
            (replica_identity, keys)
        });
        let shape = shape_of(&schema);
        let changed = known.shape != shape || before != identity;
        known.shape = shape;
        known.identity = Some(identity);
        let signal = changed.then_some(SchemaHint::Columns(schema));
        self.relations.insert(
            oid,
            RelationState::Tracked {
                table,
                columns: stream,
                signal,
            },
        );
        Ok(())
    }

    /// Halts `table` at `seq`, once: its changes are skipped from here on, under any OID.
    fn halt(&mut self, oid: u32, table: TableRef, seq: SourceSeq, reason: HaltReason) {
        self.relations.insert(oid, RelationState::Halted);
        let Some(known) = self.known.get_mut(&table) else {
            return;
        };
        if !std::mem::replace(&mut known.halted, true) {
            self.pending
                .push_back(StreamItem::TableHalt { table, seq, reason });
        }
    }

    /// Before a change or TRUNCATE of `oid` at `seq`: the signal or halt its last Relation message
    /// left.
    fn announce(&mut self, oid: u32, seq: SourceSeq) {
        if let Some(RelationState::Tracked { table, .. }) = self.relations.get(&oid)
            && self.known.get(table).is_some_and(|k| k.halted)
        {
            // Halted under its other OID.
            self.relations.insert(oid, RelationState::Halted);
            return;
        }
        match self.relations.get_mut(&oid) {
            Some(RelationState::Tracked { table, signal, .. }) => {
                if let Some(hint) = signal.take() {
                    let table = table.clone();
                    self.pending
                        .push_back(StreamItem::SchemaSignal { table, seq, hint });
                }
            }
            Some(RelationState::Halting { .. }) => {
                let Some(RelationState::Halting { table, reason }) =
                    self.relations.insert(oid, RelationState::Halted)
                else {
                    unreachable!("matched above")
                };
                self.halt(oid, table, seq, reason);
            }
            _ => {}
        }
    }

    /// A tuple as a row of the relation's columns; `keys_only` keeps the replica identity columns
    /// (a `K` old tuple, whose other columns are NULL placeholders).
    fn row(
        columns: &[StreamColumn],
        tuple: Vec<Datum>,
        keys_only: bool,
    ) -> Result<Result<Row, HaltReason>, SourceError> {
        if tuple.len() != columns.len() {
            return Err(protocol(format!(
                "a tuple of {} columns for a relation of {}",
                tuple.len(),
                columns.len()
            )));
        }
        let mut row = Vec::with_capacity(columns.len());
        for (c, d) in columns.iter().zip(tuple) {
            if keys_only && !c.key {
                continue;
            }
            let v = match d {
                Datum::Null => ColumnValue::Null,
                Datum::Unchanged => ColumnValue::Unchanged,
                Datum::Text(bytes) => {
                    let text = match std::str::from_utf8(&bytes) {
                        Ok(text) => Cow::Borrowed(text),
                        // PostgreSQL 14 sends a `"char"` byte above 127 as itself; 15 and later
                        // send `\ooo`.
                        Err(_) if bytes.len() == 1 && c.decode.reads_char() => {
                            Cow::Owned(format!("\\{:03o}", bytes[0]))
                        }
                        Err(_) => {
                            return Ok(Err(HaltReason::SchemaMismatch {
                                column: c.name.clone(),
                                detail: "the value isn't valid UTF-8".into(),
                            }));
                        }
                    };
                    match value::decode(&c.decode, &text) {
                        Ok(v) if c.identifier && value::clamped(&v, &text) => {
                            return Ok(Err(HaltReason::SchemaMismatch {
                                column: c.name.clone(),
                                detail: value::CLAMPED_KEY.into(),
                            }));
                        }
                        Ok(v) => v,
                        Err(ValueError::Overflow) => {
                            return Ok(Err(HaltReason::NumericOverflow {
                                column: c.name.clone(),
                            }));
                        }
                        Err(ValueError::Unrepresentable(detail)) => {
                            return Ok(Err(HaltReason::SchemaMismatch {
                                column: c.name.clone(),
                                detail,
                            }));
                        }
                    }
                }
            };
            row.push((c.name.clone(), v));
        }
        Ok(Ok(Row(row)))
    }

    /// A change of `relation`, or nothing for a table the stream doesn't track.
    fn change(
        &mut self,
        relation: u32,
        op: Op,
        old: Option<(OldKind, Vec<Datum>)>,
        new: Option<Vec<Datum>>,
        seq: SourceSeq,
    ) -> Result<(), SourceError> {
        let txn = self
            .txn
            .ok_or_else(|| protocol("a change outside a transaction"))?;
        self.announce(relation, seq);
        let (table, before, after) = match self.relations.get(&relation) {
            Some(RelationState::Tracked { table, columns, .. }) => {
                let before = match old {
                    Some((kind, t)) => Some(Self::row(columns, t, kind == OldKind::Key)?),
                    None => None,
                };
                let after = match new {
                    Some(t) => Some(Self::row(columns, t, false)?),
                    None => None,
                };
                (table.clone(), before, after)
            }
            Some(_) => return Ok(()),
            None => {
                return Err(protocol(format!(
                    "a change of relation {relation} before its Relation message"
                )));
            }
        };
        let halt = |r: &Option<Result<Row, HaltReason>>| match r {
            Some(Err(h)) => Some(h.clone()),
            _ => None,
        };
        if let Some(reason) = halt(&before).or_else(|| halt(&after)) {
            self.halt(relation, table, seq, reason);
            return Ok(());
        }
        self.pending.push_back(StreamItem::Change(CdcEvent {
            op,
            table,
            before: before.map(|r| r.expect("checked")),
            after: after.map(|r| r.expect("checked")),
            source_pos: SourcePosition::Pg(txn.commit_lsn),
            change_seq: seq,
            xid: txn.xid,
            commit_ts_micros: txn.commit_ts_micros,
        }));
        Ok(())
    }

    async fn xlog(&mut self, wal_start: Lsn, data: bytes::Bytes) -> Result<(), SourceError> {
        let seq = SourceSeq::Pg(wal_start);
        match decode(&data).map_err(protocol)? {
            Message::Relation {
                oid,
                namespace,
                name,
                replica_identity,
                columns,
            } => {
                let schema = if namespace.is_empty() {
                    "pg_catalog".into()
                } else {
                    namespace
                };
                self.relation(oid, TableRef::new(schema, name), replica_identity, columns)
                    .await?;
            }
            Message::Insert { relation, new } => {
                self.change(relation, Op::Insert, None, Some(new), seq)?
            }
            Message::Update { relation, old, new } => {
                self.change(relation, Op::Update, old, Some(new), seq)?
            }
            Message::Delete { relation, old } => {
                self.change(relation, Op::Delete, Some(old), None, seq)?
            }
            Message::Truncate { relations, .. } => {
                if self.txn.is_none() {
                    return Err(protocol("a TRUNCATE outside a transaction"));
                }
                for r in &relations {
                    self.announce(*r, seq);
                }
                let tables: Vec<TableRef> = relations
                    .iter()
                    .filter_map(|r| match self.relations.get(r) {
                        Some(RelationState::Tracked { table, .. }) => Some(table.clone()),
                        _ => None,
                    })
                    .collect();
                if !tables.is_empty() {
                    self.pending.push_back(StreamItem::Truncate { tables, seq });
                }
            }
            Message::Type { .. } | Message::Origin { .. } => {}
        }
        Ok(())
    }

    async fn event(&mut self, ev: ReplicationEvent) -> Result<(), SourceError> {
        match ev {
            ReplicationEvent::KeepAlive { wal_end, .. } => {
                if self.txn.is_none() {
                    self.pending
                        .push_back(StreamItem::KeepAlive(SourcePosition::Pg(from_wire(
                            wal_end,
                        ))));
                }
            }
            ReplicationEvent::Begin {
                final_lsn,
                xid,
                commit_time_micros,
            } => {
                if let Some(open) = self.txn {
                    return Err(protocol(format!(
                        "transaction {xid} begins inside {}",
                        open.xid
                    )));
                }
                let t = Txn {
                    xid: u64::from(xid),
                    commit_lsn: from_wire(final_lsn),
                    commit_ts_micros: commit_time_micros + PG_EPOCH_MICROS,
                };
                self.txn = Some(t);
                self.pending.push_back(StreamItem::Begin {
                    xid: t.xid,
                    commit_ts_micros: t.commit_ts_micros,
                });
            }
            ReplicationEvent::Commit { lsn, end_lsn, .. } => {
                let t = self
                    .txn
                    .take()
                    .ok_or_else(|| protocol("a Commit outside a transaction"))?;
                if from_wire(lsn) != t.commit_lsn {
                    return Err(protocol(format!(
                        "transaction {} commits at {} after beginning with {}",
                        t.xid,
                        from_wire(lsn),
                        t.commit_lsn
                    )));
                }
                self.pending
                    .push_back(StreamItem::Commit(TransactionBoundary {
                        xid: t.xid,
                        commit_pos: SourcePosition::Pg(from_wire(end_lsn)),
                        commit_ts_micros: t.commit_ts_micros,
                    }));
            }
            ReplicationEvent::XLogData {
                wal_start, data, ..
            } => self.xlog(from_wire(wal_start), data).await?,
            ReplicationEvent::Message { .. } | ReplicationEvent::StoppedAt { .. } => {}
        }
        Ok(())
    }
}

#[async_trait]
impl ChangeStream for PgStream {
    async fn next(&mut self) -> Result<StreamItem, SourceError> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Ok(item);
            }
            let ev = match &self.inflight {
                Some(ev) => ev.clone(),
                None => match self.client.recv().await {
                    Ok(Some(ev)) => self.inflight.insert(ev).clone(),
                    Ok(None) => {
                        return Err(SourceError::Transient(
                            "the replication stream ended".into(),
                        ));
                    }
                    Err(e) => {
                        return Err(replication_error(e, Some(self.acknowledged.to_string())));
                    }
                },
            };
            self.event(ev).await?;
            self.inflight = None;
        }
    }

    /// Sets the slot's confirmed position (the status update's write, flush and apply LSNs,
    /// Data-Flow Phase 5). A position behind the last acknowledged one changes nothing.
    async fn acknowledge(&mut self, durable: &SourcePosition) -> Result<(), SourceError> {
        let lsn = lsn_of(durable)?;
        if lsn > self.acknowledged {
            self.acknowledged = lsn;
            self.client.update_applied_lsn(to_wire(lsn));
        }
        Ok(())
    }

    /// Ends the stream, then waits up to `timeouts.command` for the server to release the slot:
    /// its walsender lets go after the client has left, and a stream reopened before then fails
    /// with `55006` (measured on PostgreSQL 18).
    async fn close(mut self: Box<Self>) -> Result<(), SourceError> {
        self.client
            .shutdown()
            .await
            .map_err(|e| replication_error(e, None))?;
        let deadline = tokio::time::Instant::now() + self.command;
        loop {
            let row = within(
                "read the slot",
                self.command,
                self.catalog.query_opt(
                    "SELECT active FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot],
                ),
            )
            .await?;
            if !row.is_some_and(|r| r.get::<_, bool>(0)) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(SourceError::Transient(format!(
                    "slot {} is still active {:?} after the stream closed",
                    self.slot, self.command
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 61.

Run, with `test-infra/up.sh` running: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg`

Expected: PASS, 28 tests.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/stream.rs \
  crates/data-avalanche-source-pg/src/value.rs \
  crates/data-avalanche-source-pg/tests/pg/main.rs \
  crates/data-avalanche-source-pg/tests/pg/common.rs \
  crates/data-avalanche-source-pg/tests/pg/stream.rs
git commit -m "feat(source-pg): the change stream"
```

---

### Task 10: Keyset snapshot chunks and the keyed re-read

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/snapshot.rs`
- Modify: `crates/data-avalanche-source-pg/tests/pg/main.rs`
- Create: `crates/data-avalanche-source-pg/tests/pg/snapshot.rs`

**Interfaces:**
- Consumes: `value::decode`, `text_of` (Task 4); `plan_column` (Task 5); `load_types`, `table_key`
  (Task 6); `PgConnector` (Task 7); `ident`, `qualified` (Task 8).
- Produces: crate root `FETCH_BATCH = 1000`; `PgConnector::read_chunk(&TableSchema,
  Option<&KeysetCursor>, limit) -> Chunk` and `fetch_rows(&TableSchema, &[Row]) -> Vec<Row>`.

D-M5.17, Q-M5-6. A chunk is one statement, `WHERE (key) > (cursor) ORDER BY key LIMIT n`.
A text key compares in its own collation when that is deterministic (its index serves the
chunk), in `COLLATE "C"` otherwise; the cursor records the collations (Q-M5-16). A `"char"` is
read as its code. Each read carries `SET LOCAL statement_timeout`. Rows are read over the
simple-query protocol, so values are the output-function text pgoutput also sends, and a snapshot
row equals the streamed row of the same data. The integration tests read a table in chunks of
every size and check the union equals the table. They cover a composite key with text, a `"char"`
key past 127, and a column changed under the snapshot (`Transient`), a key in a
nondeterministic collation, a domain over `"char"`, and a read cancelled by the server.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-source-pg/src/lib.rs` with:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

pub mod catalog;
mod connect;
mod connector;
mod error;
pub mod pgoutput;
mod position;
mod slot;
mod snapshot;
mod stream;
#[cfg(test)]
mod test_support;
mod tls;
pub mod types;
pub mod value;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
pub use connector::{MIN_SERVER_VERSION, PgConnector};
pub use snapshot::FETCH_BATCH;
pub use stream::{PG_EPOCH_MICROS, PgStream};
```

Create `crates/data-avalanche-source-pg/src/snapshot.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn rc(name: &str, type_text: &str, collation: Option<KeyCollation>) -> ReadColumn {
        ReadColumn {
            name: name.into(),
            decode: Decode::Text,
            type_text: type_text.into(),
            collation,
            identifier: false,
        }
    }

    fn char_column(name: &str, type_text: &str) -> ReadColumn {
        ReadColumn {
            decode: Decode::Char,
            ..rc(name, type_text, None)
        }
    }

    #[test]
    fn keys_compare_in_one_deterministic_collation() {
        let own = || Some(KeyCollation::Own("en_US.utf8".into()));
        assert_eq!(
            key_expr(&rc("code", "text", Some(KeyCollation::C))),
            "src.\"code\" COLLATE \"C\""
        );
        assert_eq!(key_expr(&rc("code", "text", own())), "src.\"code\"");
        assert_eq!(key_expr(&rc("id", "bigint", None)), "src.\"id\"");
        assert_eq!(
            key_value(
                &rc("code", "character varying(10)", Some(KeyCollation::C)),
                "it's"
            ),
            "'it''s'::character varying(10) COLLATE \"C\""
        );
        assert_eq!(
            key_value(&rc("code", "character varying(10)", own()), "it's"),
            "'it''s'::character varying(10)",
            "the column's own collation wins over the literal's"
        );
        assert_eq!(
            key_value(&rc("id", "numeric(10,2)", None), "1.50"),
            "'1.50'::numeric(10,2)"
        );
        assert_eq!(
            key_value(&char_column("c", "\"char\""), "255"),
            "(('255'::int4 + 128) % 256 - 128)::\"char\""
        );
        assert_eq!(
            key_value(&char_column("c", "it.flag"), "97"),
            "(('97'::int4 + 128) % 256 - 128)::\"char\"::it.flag",
            "a domain over \"char\""
        );
        assert_eq!(
            select_list(&[rc("a b", "text", own()), char_column("c", "\"char\"")]),
            "src.\"a b\", ((src.\"c\")::\"char\"::int4 + 256) % 256"
        );
        let a = rc("a", "text", own());
        let b = rc("b", "text", Some(KeyCollation::C));
        let id = rc("id", "int", None);
        assert_eq!(
            cursor_collation(&[&a, &id, &b]).as_deref(),
            Some("en_US.utf8,C")
        );
        assert_eq!(cursor_collation(&[&id]), None);
        assert_eq!(
            quote("a'b\\c"),
            "'a''b\\c'",
            "backslashes stay: standard_conforming_strings"
        );
    }
}
```

Replace `crates/data-avalanche-source-pg/tests/pg/main.rs` with:

```rust
//! The PostgreSQL source against a live server (`test-infra/compose.yaml`): `postgres-18` on every
//! change, PostgreSQL 14–17 nightly (`DATA_AVALANCHE_PG_PORT`). Each test returns at once without
//! `DATA_AVALANCHE_IT`.

mod common;
mod resolve;
mod slot;
mod snapshot;
mod stream;
```

Create `crates/data-avalanche-source-pg/tests/pg/snapshot.rs`:

```rust
//! Snapshot chunks and keyed re-reads against a live server (Snapshot_Resumability §1–§2).

use std::time::Duration;

use data_avalanche_core::{
    ColumnValue as V, HaltReason, KeysetCursor, Row, SourceError, TableSchema, identifier_values,
};
use data_avalanche_source_pg::{FETCH_BATCH, PgConnector};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, connector, fresh, pipeline, resolved};

/// Every row of `table`, chunk by chunk.
async fn read_all(c: &PgConnector, table: &TableSchema, limit: usize) -> (Vec<Row>, usize) {
    let mut rows = vec![];
    let mut cursor: Option<KeysetCursor> = None;
    let mut chunks = 0;
    loop {
        let chunk = c
            .read_chunk(table, cursor.as_ref(), limit)
            .await
            .expect("chunk");
        chunks += 1;
        assert!(chunks <= 10_000, "the cursor doesn't advance");
        assert!(chunk.rows.len() <= limit);
        let Some(next) = chunk.next else {
            assert!(chunk.rows.is_empty());
            return (rows, chunks);
        };
        assert_eq!(
            Some(&next.key),
            identifier_values(table, chunk.rows.last().unwrap()).as_ref()
        );
        rows.extend(chunk.rows);
        cursor = Some(next);
    }
}

#[tokio::test]
async fn chunks_walk_the_table_in_key_order() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks",
        "CREATE TABLE t (id int PRIMARY KEY, v text);
         INSERT INTO t SELECT g, 'v' || g FROM generate_series(25, 1, -1) g;",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = &resolved(&c).await.tables[0];
    let (rows, chunks) = read_all(&c, t, 10).await;
    assert_eq!(chunks, 4, "10 + 10 + 5, then an empty chunk");
    let ids: Vec<&V> = rows.iter().map(|r| r.get("id").unwrap()).collect();
    assert_eq!(
        ids,
        (1..=25)
            .map(V::Int32)
            .collect::<Vec<_>>()
            .iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows[0],
        Row(vec![
            ("id".into(), V::Int32(1)),
            ("v".into(), V::Text("v1".into()))
        ])
    );
    let first = c.read_chunk(t, None, 3).await.unwrap();
    assert_eq!(
        first.next.unwrap().collation,
        None,
        "no text key, no collation"
    );
    cleanup(&a, &n).await;
}

fn texts(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|r| match r.get("code") {
            Some(V::Text(s)) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

/// Snapshot_Resumability §2: a text key pages in its own collation when it is deterministic (its
/// index serves the order), in `"C"` when it isn't, with a warning at resolution. The cursor
/// records the collation; a cursor of another one halts the table.
#[tokio::test]
async fn text_keys_page_in_one_deterministic_collation() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks_collation",
        "CREATE COLLATION folding (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
         CREATE TABLE \"Codes\" (code text COLLATE \"en_US.utf8\" PRIMARY KEY);
         CREATE TABLE folded (code text COLLATE folding PRIMARY KEY);
         INSERT INTO \"Codes\" VALUES ('b'), ('B'), ('a'), ('Z'), ('é'), ('a b'), ('ab');
         INSERT INTO folded VALUES ('b'), ('A'), ('é'), ('Z'), ('a b');",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let r = resolved(&c).await;
    let table = |name: &str| {
        r.tables
            .iter()
            .find(|t| t.table().name == name)
            .unwrap()
            .clone()
    };
    let server = |sql: String| {
        let a = &a;
        async move {
            a.query(&sql, &[])
                .await
                .unwrap()
                .iter()
                .map(|r| r.get::<_, String>(0))
                .collect::<Vec<_>>()
        }
    };

    let codes = table("Codes");
    let (rows, _) = read_all(&c, &codes, 2).await;
    assert_eq!(
        texts(&rows),
        server(format!(
            "SELECT code FROM {}.\"Codes\" ORDER BY code",
            n.schema
        ))
        .await,
        "every row once, in the column's own collation"
    );
    let chunk = c.read_chunk(&codes, None, 2).await.unwrap();
    let cursor = chunk.next.unwrap();
    assert_eq!(cursor.collation.as_deref(), Some("en_US.utf8"));
    let stale = KeysetCursor {
        collation: Some("C".into()),
        ..cursor
    };
    match c.read_chunk(&codes, Some(&stale), 2).await {
        Err(SourceError::TableHalt {
            reason: HaltReason::SchemaMismatch { detail, .. },
            ..
        }) => assert!(detail.contains("re-snapshot"), "{detail}"),
        other => panic!("a cursor of another collation halts: {other:?}"),
    }

    let folded = table("folded");
    let (rows, _) = read_all(&c, &folded, 1).await;
    assert_eq!(
        texts(&rows),
        server(format!(
            "SELECT code FROM {}.folded ORDER BY code COLLATE \"C\"",
            n.schema
        ))
        .await,
        "a nondeterministic collation pages in byte order"
    );
    let chunk = c.read_chunk(&folded, None, 1).await.unwrap();
    assert_eq!(chunk.next.unwrap().collation.as_deref(), Some("C"));
    assert!(
        r.warnings
            .iter()
            .any(|w| w.contains(".folded.code") && w.contains("nondeterministic")),
        "{:?}",
        r.warnings
    );
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn composite_keys_of_every_key_type_resume_exactly() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks_keys",
        "CREATE TABLE t (d date, ts timestamptz, n numeric(10,2), u uuid, b bytea, c \"char\",
                         v varchar(5), PRIMARY KEY (d, ts, n, u, b, c, v));
         INSERT INTO t
         SELECT d, ts, n, u, b, c, v
           FROM (VALUES ('-infinity'::date), ('0044-03-15 BC'), ('2024-02-29'), ('infinity')) AS d(d),
                (VALUES ('-infinity'::timestamptz), ('2024-02-29 12:34:56.789012+00'), ('0001-01-01 00:00:00+00 BC')) AS ts(ts),
                (VALUES (-1.5), (0), (12345678.99)) AS n(n),
                (VALUES ('00000000-0000-0000-0000-000000000001'::uuid)) AS u(u),
                (VALUES ('\\x00'::bytea), ('\\xff')) AS b(b),
                (VALUES ('a'::\"char\"), ((-1)::int4::\"char\")) AS c(c),
                (VALUES ('x'), ('x y')) AS v(v);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = &resolved(&c).await.tables[0];
    let expected: i64 = a
        .query_one(&format!("SELECT count(*) FROM {}.t", n.schema), &[])
        .await
        .unwrap()
        .get(0);
    let (one_by_one, _) = read_all(&c, t, 1).await;
    let (all, _) = read_all(&c, t, 1000).await;
    assert_eq!(
        one_by_one.len() as i64,
        expected,
        "the cursor never skips or repeats a row"
    );
    assert_eq!(one_by_one, all);
    let high = one_by_one
        .iter()
        .filter(|r| r.get("c") == Some(&V::Int16(255)))
        .count();
    assert_eq!(
        high as i64 * 2,
        expected,
        "the \"char\" byte 255 (PostgreSQL 14 prints it raw)"
    );
    cleanup(&a, &n).await;
}

/// A key of a domain over `"char"` travels as its byte too.
#[tokio::test]
async fn a_domain_over_char_key_resumes() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks_domain",
        "CREATE DOMAIN flag AS \"char\";
         CREATE TABLE t (k flag PRIMARY KEY);
         INSERT INTO t VALUES ('a'), ('b'), ((-1)::int4::\"char\");",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = &resolved(&c).await.tables[0];
    let (rows, _) = read_all(&c, t, 1).await;
    let keys: Vec<&V> = rows.iter().map(|r| r.get("k").unwrap()).collect();
    assert_eq!(keys, [&V::Int16(97), &V::Int16(98), &V::Int16(255)]);
    let again = c.fetch_rows(t, &rows).await.unwrap();
    assert_eq!(again.len(), 3);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn fetch_rows_rereads_keys_in_batches() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "fetch_rows",
        "CREATE TABLE t (k text, id int, doc text, PRIMARY KEY (k, id));
         INSERT INTO t SELECT 'k''' || (g % 3), g, repeat('d', g) FROM generate_series(1, 2500) g;",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = &resolved(&c).await.tables[0];
    let key = |k: &str, id: i32| {
        Row(vec![
            ("k".into(), V::Text(k.into())),
            ("id".into(), V::Int32(id)),
        ])
    };
    let mut keys: Vec<Row> = (1..=2500)
        .map(|g| key(&format!("k'{}", g % 3), g))
        .collect();
    keys.push(key("k'1", 99_999));
    assert!(keys.len() > 2 * FETCH_BATCH);
    let rows = c.fetch_rows(t, &keys).await.unwrap();
    assert_eq!(rows.len(), 2500, "the missing key is left out");
    let r7 = rows
        .iter()
        .find(|r| r.get("id") == Some(&V::Int32(7)))
        .unwrap();
    assert_eq!(r7.get("doc"), Some(&V::Text("d".repeat(7))));
    assert!(c.fetch_rows(t, &[]).await.unwrap().is_empty());
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_table_changed_under_the_snapshot_is_retried() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks_drift",
        "CREATE TABLE t (id int PRIMARY KEY, v int, w int);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = resolved(&c).await.tables[0].clone();
    a.batch_execute(&format!("ALTER TABLE {}.t DROP COLUMN w", n.schema))
        .await
        .unwrap();
    match c.read_chunk(&t, None, 10).await {
        Err(SourceError::Transient(m)) => assert!(m.contains("column w is gone"), "{m}"),
        other => panic!("transient expected: {other:?}"),
    }
    a.batch_execute(&format!(
        "ALTER TABLE {}.t ALTER COLUMN v TYPE text",
        n.schema
    ))
    .await
    .unwrap();
    let t = resolved(&c).await.tables[0].clone();
    a.batch_execute(&format!(
        "ALTER TABLE {}.t ALTER COLUMN v TYPE bigint USING 0",
        n.schema
    ))
    .await
    .unwrap();
    match c.read_chunk(&t, None, 10).await {
        Err(SourceError::Transient(m)) => assert!(m.contains("column v is now"), "{m}"),
        other => panic!("transient expected: {other:?}"),
    }
    let t = resolved(&c).await.tables[0].clone();
    assert!(
        c.read_chunk(&t, None, 10).await.is_ok(),
        "the current schema reads"
    );
    cleanup(&a, &n).await;
}

/// The server cancels a chunk read past `timeouts.command`, so the catalog connection the reads
/// share is free right after: a client-side timeout alone would leave the statement running.
#[tokio::test]
async fn a_timed_out_chunk_frees_the_connection() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "chunks_timeout", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let mut p = pipeline(&n, &["*"], "", "");
    p.source.timeouts.command = Duration::from_secs(1);
    let c = connector(&p);
    let t = &resolved(&c).await.tables[0];
    let locker = admin().await;
    locker
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {}.t IN ACCESS EXCLUSIVE MODE",
            n.schema
        ))
        .await
        .unwrap();
    match c.read_chunk(t, None, 10).await {
        Err(SourceError::Transient(m)) => assert!(m.contains("57014"), "{m}"),
        other => panic!("a statement timeout is transient: {other:?}"),
    }
    tokio::time::timeout(Duration::from_secs(2), c.current_position())
        .await
        .expect("the connection is free while the lock is still held")
        .unwrap();
    locker.batch_execute("ROLLBACK").await.unwrap();
    cleanup(&a, &n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `snapshot::FETCH_BATCH`
error[E0425]: cannot find type `ReadColumn` in this scope
error[E0422]: cannot find struct, variant or union type `ReadColumn` in this scope
error[E0433]: cannot find type `Decode` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-source-pg/src/snapshot.rs`, above the test module:

```rust
//! Snapshot reads (Snapshot_Resumability §1–§2, §6): keyset chunks in identifier order, a text
//! identifier column ordered in its own collation when that is deterministic (its index serves the
//! order) and in `"C"` otherwise, and the keyed re-read `toast_strategy: re_query` uses. Rows are read over the simple-query protocol, whose values are the types' output-function
//! text, the same text pgoutput sends; they are decoded like stream values, so a snapshot row and
//! a streamed row of the same data are equal. (A `::text` cast isn't the output function:
//! `true::text` is `true`, pgoutput sends `t`.)

use std::time::Duration;

use data_avalanche_core::{
    Chunk, ColumnValue, HaltReason, KeysetCursor, Row, SourceError, TableSchema, identifier_values,
};
use tokio_postgres::SimpleQueryMessage;

use crate::catalog::{load_types, table_key};
use crate::connect::within;
use crate::connector::PgConnector;
use crate::slot::{ident, qualified};
use crate::types::plan_column;
use crate::value::{self, Decode, ValueError, text_of};

/// Keys per `fetch_rows` statement, which bounds its size.
pub const FETCH_BATCH: usize = 1000;

/// How much longer the client waits than the server's `statement_timeout`: the server cancels a
/// slow read first, so the shared catalog connection is free again when the client gives up.
const SERVER_FIRST: Duration = Duration::from_secs(5);

/// How to read one column of a table.
#[derive(Debug, Clone)]
pub(crate) struct ReadColumn {
    name: String,
    decode: Decode,
    /// The catalog's type text, for casting a key value.
    type_text: String,
    /// The collation a key comparison uses; `None` for a type without one.
    collation: Option<KeyCollation>,
    /// One of the table's identifier columns: a value that clamps halts the table (Q-M5-18).
    identifier: bool,
}

/// The collation a text key column is ordered and compared in (Snapshot_Resumability §2): one
/// deterministic collation for both, so the order and the cursor agree.
#[derive(Debug, Clone, PartialEq, Eq)]
enum KeyCollation {
    /// The column's own, deterministic: its index serves the order.
    Own(String),
    /// A nondeterministic collation's column, compared in `"C"`.
    C,
}

impl KeyCollation {
    fn name(&self) -> &str {
        match self {
            KeyCollation::Own(name) => name,
            KeyCollation::C => "C",
        }
    }
}

/// The read plan of a table, checked against `schema`: every column still exists and decodes to
/// the type the schema says. A difference is transient: the stream's schema signal hasn't caught
/// up with the catalog yet.
async fn read_plan(c: &PgConnector, schema: &TableSchema) -> Result<Vec<ReadColumn>, SourceError> {
    let client = c.conn.client().await?;
    let names: Vec<&str> = schema.columns().iter().map(|c| c.name.as_str()).collect();
    let rows = c
        .conn
        .command(
            "read the table's columns",
            client.query(
                "SELECT a.attname, a.atttypid, a.atttypmod,
                        pg_catalog.format_type(a.atttypid, a.atttypmod), co.collname::text,
                        co.collisdeterministic
                   FROM pg_catalog.pg_attribute a
                   LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
                  WHERE a.attrelid = $1::text::regclass AND a.attname = ANY($2)
                    AND a.attnum > 0 AND NOT a.attisdropped",
                &[&qualified(schema.table()), &names],
            ),
        )
        .await?;
    let mut types = c.types.lock().await;
    c.conn
        .command(
            "load types",
            load_types(&client, rows.iter().map(|r| r.get::<_, u32>(1)), &mut types),
        )
        .await?;
    let key = table_key(schema.table());
    let changed = |detail: String| {
        SourceError::Transient(format!(
            "{key} changed under the snapshot: {detail}; read again once the stream's schema caught up"
        ))
    };
    let mut out = Vec::with_capacity(names.len());
    for col in schema.columns() {
        let r = rows
            .iter()
            .find(|r| r.get::<_, &str>(0) == col.name)
            .ok_or_else(|| changed(format!("column {} is gone", col.name)))?;
        let over = c
            .pipeline
            .tables
            .column_type_overrides
            .get(&format!("{key}.{}", col.name));
        let p = plan_column(
            &col.name,
            r.get(1),
            r.get(2),
            &types,
            c.pipeline.tables.numeric_overflow_action,
            over,
        )
        .map_err(|h| SourceError::TableHalt {
            table: schema.table().clone(),
            reason: h,
        })?;
        if p.value_type != col.value_type {
            return Err(changed(format!(
                "column {} is now {:?}, the schema has {:?}",
                col.name, p.value_type, col.value_type
            )));
        }
        let collation = match (r.get::<_, Option<String>>(4), r.get::<_, Option<bool>>(5)) {
            (Some(name), Some(true)) => Some(KeyCollation::Own(name)),
            (Some(_), _) => Some(KeyCollation::C),
            (None, _) => None,
        };
        out.push(ReadColumn {
            name: col.name.clone(),
            decode: p.decode,
            type_text: r.get(3),
            collation,
            identifier: schema.identifier().contains(&col.name),
        });
    }
    Ok(out)
}

/// The table's alias in snapshot queries. Columns are qualified with it.
const ALIAS: &str = "src";

/// The select list: every column, in schema order. A `"char"` is read as its byte, 0–255:
/// PostgreSQL 14 prints a byte above 127 as itself, which isn't UTF-8.
fn select_list(columns: &[ReadColumn]) -> String {
    columns
        .iter()
        .map(|c| {
            if c.decode.reads_char() {
                format!("(({ALIAS}.{})::\"char\"::int4 + 256) % 256", ident(&c.name))
            } else {
                format!("{ALIAS}.{}", ident(&c.name))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A key column in a comparison: `COLLATE "C"` on a column of a nondeterministic collation, its
/// own collation otherwise (Snapshot_Resumability §2).
fn key_expr(c: &ReadColumn) -> String {
    let column = format!("{ALIAS}.{}", ident(&c.name));
    if c.collation == Some(KeyCollation::C) {
        format!("{column} COLLATE \"C\"")
    } else {
        column
    }
}

/// A string literal (`standard_conforming_strings` is pinned on, so only quotes are doubled).
fn quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// A key value of the column's type: its text, cast. A `"char"` (or a domain over one) travels
/// as its byte, 0–255, and the `int4` → `"char"` cast takes a signed byte.
fn cast(c: &ReadColumn, text: &str) -> String {
    if c.decode.reads_char() {
        let byte = format!("(({}::int4 + 128) % 256 - 128)::\"char\"", quote(text));
        if c.type_text == "\"char\"" {
            byte
        } else {
            format!("{byte}::{}", c.type_text)
        }
    } else {
        format!("{}::{}", quote(text), c.type_text)
    }
}

/// A key value in a keyset comparison: [`cast`], collated like the column's [`key_expr`]. A cast
/// literal's collation yields to the column's own.
fn key_value(c: &ReadColumn, text: &str) -> String {
    let cast = cast(c, text);
    if c.collation == Some(KeyCollation::C) {
        format!("{cast} COLLATE \"C\"")
    } else {
        cast
    }
}

/// The key's collations, for the cursor: one name per collatable identifier column, in order.
fn cursor_collation(keys: &[&ReadColumn]) -> Option<String> {
    let names: Vec<&str> = keys
        .iter()
        .filter_map(|c| c.collation.as_ref().map(KeyCollation::name))
        .collect();
    (!names.is_empty()).then(|| names.join(","))
}

fn decode_row(
    schema: &TableSchema,
    columns: &[ReadColumn],
    row: &tokio_postgres::SimpleQueryRow,
) -> Result<Row, SourceError> {
    let mut out = Vec::with_capacity(columns.len());
    for (i, c) in columns.iter().enumerate() {
        let halt = |reason| SourceError::TableHalt {
            table: schema.table().clone(),
            reason,
        };
        let mismatch = |detail: String| {
            halt(HaltReason::SchemaMismatch {
                column: c.name.clone(),
                detail,
            })
        };
        // A `"char"[]` element above 127 on PostgreSQL 14 isn't UTF-8 either: the table halts,
        // as in the stream.
        let text = row
            .try_get(i)
            .map_err(|_| mismatch("the value isn't valid UTF-8".into()))?;
        let decoded = match text {
            None => Ok(ColumnValue::Null),
            Some(code) if c.decode.reads_char() => {
                let byte: u8 = code
                    .parse()
                    .map_err(|_| mismatch(format!("not a \"char\" code: {code:?}")))?;
                value::decode(&c.decode, &format!("\\{byte:03o}"))
            }
            Some(text) => value::decode(&c.decode, text),
        };
        let v = decoded.map_err(|e| match e {
            ValueError::Overflow => halt(HaltReason::NumericOverflow {
                column: c.name.clone(),
            }),
            ValueError::Unrepresentable(detail) => mismatch(detail),
        })?;
        if c.identifier && text.is_some_and(|t| value::clamped(&v, t)) {
            return Err(mismatch(value::CLAMPED_KEY.into()));
        }
        out.push((c.name.clone(), v));
    }
    Ok(Row(out))
}

/// Runs `sql` and decodes its rows. The server cancels it after `timeouts.command`
/// (`statement_timeout`, local to the statement's implicit transaction).
async fn rows(
    c: &PgConnector,
    what: &str,
    schema: &TableSchema,
    columns: &[ReadColumn],
    sql: &str,
) -> Result<Vec<Row>, SourceError> {
    let client = c.conn.client().await?;
    let command = c.conn.timeouts.command;
    let sql = format!(
        "SET LOCAL statement_timeout = {}; {sql}",
        command.as_millis()
    );
    let messages = within(what, command + SERVER_FIRST, client.simple_query(&sql)).await?;
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(decode_row(schema, columns, r)),
            _ => None,
        })
        .collect()
}

/// The identifier's read columns, in identifier order.
fn key_columns<'a>(schema: &TableSchema, columns: &'a [ReadColumn]) -> Vec<&'a ReadColumn> {
    schema
        .identifier()
        .iter()
        .map(|k| {
            columns
                .iter()
                .find(|c| &c.name == k)
                .expect("the identifier is a column")
        })
        .collect()
}

/// A key's values as bind parameters.
fn key_texts(key: &Row, schema: &TableSchema) -> Result<Vec<String>, SourceError> {
    schema
        .identifier()
        .iter()
        .map(|k| {
            key.get(k).and_then(text_of).ok_or_else(|| {
                SourceError::Permanent(format!(
                    "{}: the key has no usable value for {k}",
                    schema.table()
                ))
            })
        })
        .collect()
}

impl PgConnector {
    /// One chunk: `SELECT … WHERE (key) > (cursor) ORDER BY key LIMIT n`, a single statement, so
    /// one READ COMMITTED snapshot (Snapshot_Resumability §1).
    pub async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError> {
        let columns = read_plan(self, table).await?;
        let keys = key_columns(table, &columns);
        let collation = cursor_collation(&keys);
        let mut sql = format!(
            "SELECT {} FROM {} AS {ALIAS}",
            select_list(&columns),
            qualified(table.table())
        );
        if let Some(cursor) = after {
            if cursor.collation != collation {
                return Err(SourceError::TableHalt {
                    table: table.table().clone(),
                    reason: HaltReason::SchemaMismatch {
                        column: table.identifier().join(","),
                        detail: format!(
                            "the key's collation is {collation:?}, the snapshot's cursor was \
                             ordered by {:?}; re-snapshot the table",
                            cursor.collation
                        ),
                    },
                });
            }
            let texts = key_texts(&cursor.key, table)?;
            let left: Vec<String> = keys.iter().map(|c| key_expr(c)).collect();
            let right: Vec<String> = keys
                .iter()
                .zip(&texts)
                .map(|(c, t)| key_value(c, t))
                .collect();
            sql += &format!(" WHERE ({}) > ({})", left.join(", "), right.join(", "));
        }
        let order: Vec<String> = keys.iter().map(|c| key_expr(c)).collect();
        sql += &format!(" ORDER BY {} LIMIT {limit}", order.join(", "));
        let rows = rows(self, "read a snapshot chunk", table, &columns, &sql).await?;
        let next = match rows.last() {
            None => None,
            Some(last) => Some(KeysetCursor {
                key: identifier_values(table, last).ok_or_else(|| {
                    SourceError::Permanent(format!("{}: a row with a NULL key", table.table()))
                })?,
                collation,
            }),
        };
        Ok(Chunk { rows, next })
    }

    /// The current rows of these keys (`toast_strategy: re_query`, Q-M5-6), [`FETCH_BATCH`] keys
    /// per statement. A key with no row is left out; rows come in no particular order.
    pub async fn fetch_rows(
        &self,
        table: &TableSchema,
        keys: &[Row],
    ) -> Result<Vec<Row>, SourceError> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let columns = read_plan(self, table).await?;
        let key_cols = key_columns(table, &columns);
        let left: Vec<String> = key_cols
            .iter()
            .map(|c| format!("{ALIAS}.{}", ident(&c.name)))
            .collect();
        let mut out = vec![];
        for batch in keys.chunks(FETCH_BATCH) {
            let tuples = batch
                .iter()
                .map(|key| {
                    let texts = key_texts(key, table)?;
                    let values: Vec<String> = key_cols
                        .iter()
                        .zip(&texts)
                        .map(|(c, t)| cast(c, t))
                        .collect();
                    Ok(format!("({})", values.join(", ")))
                })
                .collect::<Result<Vec<_>, SourceError>>()?;
            let sql = format!(
                "SELECT {} FROM {} AS {ALIAS} WHERE ({}) IN ({})",
                select_list(&columns),
                qualified(table.table()),
                left.join(", "),
                tuples.join(", ")
            );
            out.extend(rows(self, "re-read rows", table, &columns, &sql).await?);
        }
        Ok(out)
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 62.

Run, with `test-infra/up.sh` running: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg`

Expected: PASS, 35 tests.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/snapshot.rs \
  crates/data-avalanche-source-pg/tests/pg/main.rs \
  crates/data-avalanche-source-pg/tests/pg/snapshot.rs
git commit -m "feat(source-pg): keyset snapshot chunks and the keyed re-read"
```

---

### Task 11: Channels C and D, and the `SourceConnector` impl

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/lib.rs`
- Create: `crates/data-avalanche-source-pg/src/schema.rs`
- Modify: `crates/data-avalanche-source-pg/src/connector.rs`
- Modify: `crates/data-avalanche-source-pg/tests/pg/main.rs`
- Create: `crates/data-avalanche-source-pg/tests/pg/schema.rs`

**Interfaces:**
- Consumes: `oids_by_name`, `table_key` (Task 6); `PgConnector::schemas` (Task 7); every inherent
  method of Tasks 7–10.
- Produces:
  - `narrowed(&ColumnSchema, &ColumnSchema) -> bool`, `scope(cached, hint, now) ->
    Result<TableSchema, HaltReason>`, `diff(cached, new) -> Vec<SchemaChange>`.
  - `PgConnector::augment(&TableSchema, &SchemaHint) -> (SchemaDelta, Option<TableSchema>)` and
    `rescan(&[TableSchema]) -> RescanReport`.
  - `impl SourceConnector for PgConnector`: each method delegates to the inherent one.

D-M5.18, D-M5.19. Channel C turns a stream signal into the change it implies. Names and types
come from the stream's columns, because the catalog may already be past the stream's position.
Identity (`attnum`) and nullability come from the catalog. Matched by `attnum`, a column under a
new name is a proven rename. With the catalog ahead of the stream, a placement that doesn't fit the
stream's columns is a guess, and the table halts; a column dropped since the signal keeps its
`attnum` (Q-M5-17). A table gone from the catalog is `TableDropped`. Channel D reads all
tracked tables in one batch for the idle rescan. `lib.rs` takes its final form.

- [ ] **Step 1: Write the failing tests**

Replace `crates/data-avalanche-source-pg/src/lib.rs` with:

```rust
//! The PostgreSQL realization of `data_avalanche_core::SourceConnector` (roadmap M5): pgoutput
//! protocol v1 over `pgwire-replication`, text-format tuples decoded to `ColumnValue`s
//! (`Type_Mapping.md`), the slot and publication lifecycle (`Postgres_Slot…`), catalog channels
//! A, C and D (`Schema_Change…` §3) and keyset snapshot chunks (`Snapshot_Resumability…`).

pub mod catalog;
mod connect;
mod connector;
mod error;
pub mod pgoutput;
mod position;
mod schema;
mod slot;
mod snapshot;
mod stream;
#[cfg(test)]
mod test_support;
mod tls;
pub mod types;
pub mod value;

pub use connect::{APPLICATION_NAME, SESSION_OPTIONS};
pub use connector::{MIN_SERVER_VERSION, PgConnector};
pub use snapshot::FETCH_BATCH;
pub use stream::{PG_EPOCH_MICROS, PgStream};
```

Create `crates/data-avalanche-source-pg/src/schema.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use data_avalanche_core::ColumnValueType as V;

    fn col(name: &str, v: V, ty: &str, ordinal: i32) -> ColumnSchema {
        ColumnSchema {
            source_ordinal: Some(ordinal),
            ..ColumnSchema::new(name, v, ty)
        }
    }

    fn table(columns: Vec<ColumnSchema>, id: &[&str]) -> TableSchema {
        TableSchema::new(
            TableRef::new("public", "t"),
            columns,
            id.iter().map(|s| s.to_string()).collect(),
        )
        .unwrap()
    }

    fn base() -> TableSchema {
        table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a", V::Text, "character varying(100)", 2),
                col("b", V::Int32, "integer", 3),
            ],
            &["id"],
        )
    }

    #[test]
    fn bounds_and_narrowing() {
        assert_eq!(bound("character varying(100)"), Some(100));
        assert_eq!(bound("numeric(10,2)"), Some(10));
        assert_eq!(bound("timestamp(3) with time zone"), Some(3));
        assert_eq!(bound("text"), None);
        assert_eq!(bound("timestamp without time zone"), Some(6), "the default");
        assert_eq!(bound("interval"), Some(6));
        assert_eq!(bound("time with time zone[]"), Some(6));
        assert_eq!(bound("timeslot"), None, "a user type");
        let ts = |t: &str| col("t", V::Timestamp { with_tz: false }, t, 4);
        assert!(!narrowed(
            &ts("timestamp without time zone"),
            &ts("timestamp(6) without time zone")
        ));
        assert!(narrowed(
            &ts("timestamp without time zone"),
            &ts("timestamp(3) without time zone")
        ));
        let a = |t: &str| col("a", V::Text, t, 2);
        assert!(narrowed(
            &a("character varying(100)"),
            &a("character varying(10)")
        ));
        assert!(narrowed(&a("text"), &a("character varying(10)")));
        assert!(!narrowed(&a("character varying(10)"), &a("text")));
        assert!(!narrowed(
            &a("character varying(10)"),
            &a("character varying(20)")
        ));
        assert!(!narrowed(
            &col("b", V::Int32, "integer", 3),
            &col("b", V::Int64, "bigint", 3)
        ));
    }

    #[test]
    fn diffs_by_attnum() {
        let new = table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a2", V::Text, "character varying(10)", 2).not_null(),
                col("c", V::Bool, "boolean", 4),
            ],
            &["id"],
        );
        let changes = diff(&base(), &new);
        assert_eq!(
            changes,
            vec![
                SchemaChange::RenameColumn {
                    from: "a".into(),
                    to: "a2".into(),
                    proven: true
                },
                SchemaChange::ChangeType {
                    column: "a2".into(),
                    from: base().columns()[1].clone(),
                    to: new.columns()[1].clone(),
                    narrowed: true,
                },
                SchemaChange::SetNotNull("a2".into()),
                SchemaChange::DropColumn("b".into()),
                SchemaChange::AddColumn(new.columns()[2].clone()),
            ]
        );
        assert!(diff(&base(), &base()).is_empty());
    }

    #[test]
    fn an_identifier_on_other_columns_is_a_change() {
        let mut columns = base().columns().to_vec();
        columns[2] = columns[2].clone().not_null();
        let new = table(columns, &["b"]);
        assert!(diff(&base(), &new).contains(&SchemaChange::IdentifierChanged));
    }

    #[test]
    fn scope_takes_the_stream_s_types_and_the_catalog_s_identity() {
        // The catalog is ahead: `a` was renamed to `a2` after the signal, and `z` added.
        let now = table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a2", V::Text, "text", 2).not_null(),
                col("b", V::Int64, "bigint", 3),
                col("z", V::Text, "text", 5),
            ],
            &["id"],
        );
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
        ];
        let scoped = scope(&base(), &hint, &now).unwrap();
        let got: Vec<(&str, &V, Option<i32>, bool)> = scoped
            .columns()
            .iter()
            .map(|c| (c.name.as_str(), &c.value_type, c.source_ordinal, c.nullable))
            .collect();
        assert_eq!(
            got,
            [
                ("id", &V::Int32, Some(1), false),
                ("a", &V::Text, Some(2), false),
                ("b", &V::Int32, Some(3), true),
            ],
            "nothing past the stream's position: not z, not the rename, not bigint"
        );
        assert_eq!(
            diff(&base(), &scoped),
            vec![SchemaChange::SetNotNull("a".into())]
        );
        let unknown = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("q", V::Text, "text"),
        ];
        assert_eq!(
            scope(&base(), &unknown, &now).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
    }

    #[test]
    fn scope_halts_where_the_catalog_ahead_would_be_a_guess() {
        let id = || col("id", V::Int32, "integer", 1).not_null();
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
        ];
        let halts = |now: TableSchema| scope(&base(), &hint, &now).unwrap_err().code();
        // `a` → `x`, then `b` → `a`, after the signal: `a` and `b` land on attnum 3.
        let chain = table(
            vec![
                id(),
                col("x", V::Text, "character varying(100)", 2),
                col("a", V::Int32, "integer", 3),
            ],
            &["id"],
        );
        assert_eq!(halts(chain), "SCHEMA_MISMATCH");
        // `a` and `b` swapped after the signal: attnums out of order.
        let swap = table(
            vec![
                id(),
                col("b", V::Text, "character varying(100)", 2),
                col("a", V::Int32, "integer", 3),
            ],
            &["id"],
        );
        assert_eq!(halts(swap), "SCHEMA_MISMATCH");
        // `b` renamed away and a new `b` added after the signal: in order, but `b` names
        // another column while its own still exists.
        let taken = table(
            vec![
                id(),
                col("a", V::Text, "character varying(100)", 2),
                col("b_old", V::Int32, "integer", 3),
                col("b", V::Int32, "integer", 4),
            ],
            &["id"],
        );
        assert_eq!(halts(taken), "SCHEMA_MISMATCH");
        // Before the signal `a` dropped and `b` renamed to `a` (the stream has `id, a`); after
        // it, `a` renamed to `x`, or dropped. Placing `a` as dropped since the signal would read
        // as `DropColumn b`.
        let replaced = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Int32, "integer"),
        ];
        let renamed = table(vec![id(), col("x", V::Int32, "integer", 3)], &["id"]);
        assert_eq!(
            scope(&base(), &replaced, &renamed).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
        let dropped = table(vec![id()], &["id"]);
        assert_eq!(
            scope(&base(), &replaced, &dropped).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
        // Before the signal, `a` → `x` then `b` → `a`: the stream has the new names, and the
        // placement fits.
        let before = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("x", V::Text, "character varying(100)"),
            ColumnSchema::new("a", V::Int32, "integer"),
        ];
        let now = table(
            vec![
                id(),
                col("x", V::Text, "character varying(100)", 2),
                col("a", V::Int32, "integer", 3),
            ],
            &["id"],
        );
        let scoped = scope(&base(), &before, &now).unwrap();
        assert_eq!(
            diff(&base(), &scoped),
            vec![
                SchemaChange::RenameColumn {
                    from: "a".into(),
                    to: "x".into(),
                    proven: true
                },
                SchemaChange::RenameColumn {
                    from: "b".into(),
                    to: "a".into(),
                    proven: true
                },
            ]
        );
    }

    #[test]
    fn scope_halts_on_a_name_re_added_since_the_signal() {
        // `c` added at the signal; after it, `c` renamed to `x` and a new `c` added: the stream's
        // `c` would land on the new one.
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
            ColumnSchema::new("c", V::Int32, "integer"),
        ];
        let mut columns = base().columns().to_vec();
        columns.push(col("x", V::Int32, "integer", 4));
        columns.push(col("c", V::Int32, "integer", 5));
        let now = table(columns, &["id"]);
        assert_eq!(
            scope(&base(), &hint, &now).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
        // `b` dropped and re-added before the signal, then renamed to `x` after it: the same
        // inputs as `b` dropped and `x` added after the signal.
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
        ];
        let now = table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a", V::Text, "character varying(100)", 2),
                col("x", V::Int32, "integer", 4),
            ],
            &["id"],
        );
        assert_eq!(
            scope(&base(), &hint, &now).unwrap_err().code(),
            "SCHEMA_MISMATCH"
        );
    }

    #[test]
    fn scope_keeps_a_column_dropped_after_the_signal() {
        // Expand and contract: `c` added (the signal), then `b` dropped in the catalog.
        let hint = vec![
            ColumnSchema::new("id", V::Int32, "integer"),
            ColumnSchema::new("a", V::Text, "character varying(100)"),
            ColumnSchema::new("b", V::Int32, "integer"),
            ColumnSchema::new("c", V::Bool, "boolean"),
        ];
        let now = table(
            vec![
                col("id", V::Int32, "integer", 1).not_null(),
                col("a", V::Text, "character varying(100)", 2),
                col("c", V::Bool, "boolean", 4),
            ],
            &["id"],
        );
        let scoped = scope(&base(), &hint, &now).unwrap();
        assert_eq!(scoped.column("b").unwrap().source_ordinal, Some(3));
        assert_eq!(
            diff(&base(), &scoped),
            vec![SchemaChange::AddColumn(scoped.columns()[3].clone())]
        );
    }
}
```

Replace `crates/data-avalanche-source-pg/tests/pg/main.rs` with:

```rust
//! The PostgreSQL source against a live server (`test-infra/compose.yaml`): `postgres-18` on every
//! change, PostgreSQL 14–17 nightly (`DATA_AVALANCHE_PG_PORT`). Each test returns at once without
//! `DATA_AVALANCHE_IT`.

mod common;
mod resolve;
mod schema;
mod slot;
mod snapshot;
mod stream;
```

Create `crates/data-avalanche-source-pg/tests/pg/schema.rs`:

```rust
//! Channels C and D against a live server, through `dyn SourceConnector` (`Schema_Change…` §3,
//! §6).

use std::collections::BTreeMap;

use data_avalanche_core::{
    ChangeStream, ColumnValueType as V, DeltaClass, HaltReason, SchemaChange, SchemaHint,
    SourceConnector, SourceError, StartPlan, StreamItem, TableRef, TableSchema, classify,
};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, connector, each, fresh, pipeline};

#[tokio::test(flavor = "multi_thread")]
async fn channel_c_proves_a_rename_scoped_to_the_signal() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_c",
        "CREATE TABLE t (id int PRIMARY KEY, a varchar(100), b int);",
    )
    .await;
    let c: Box<dyn SourceConnector> = Box::new(connector(&pipeline(&n, &["*"], "", "")));
    let r = c.resolve_tables().await.unwrap();
    let cached = r.tables[0].clone();
    let p = c.prepare(None, &[n.table("t")]).await.unwrap();
    let StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("ALTER TABLE {sc}.t RENAME COLUMN a TO a2"),
            format!("INSERT INTO {sc}.t VALUES (1, 'x', 1)"),
            // After the signal's position: the catalog runs ahead of the stream.
            format!("ALTER TABLE {sc}.t ADD COLUMN z text"),
            format!("ALTER TABLE {sc}.t ALTER COLUMN a2 TYPE varchar(5)"),
        ],
    )
    .await;
    let hint = loop {
        match s.next().await.unwrap() {
            StreamItem::SchemaSignal { hint, .. } => break hint,
            StreamItem::Change(e) => panic!("a change before the signal: {e:?}"),
            _ => {}
        }
    };
    let SchemaHint::Columns(columns) = &hint else {
        panic!()
    };
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "a2", "b"]);
    let (delta, new) = c.augment(&cached, &hint).await.unwrap();
    assert_eq!(
        delta.changes,
        [SchemaChange::RenameColumn {
            from: "a".into(),
            to: "a2".into(),
            proven: true
        }],
        "the rename only: not the later column, not the later narrowing"
    );
    let new = new.unwrap();
    assert_eq!(
        new.column("a2").unwrap().source_type,
        "character varying(100)"
    );
    assert_eq!(new.column("a2").unwrap().source_ordinal, Some(2));
    assert_eq!(classify(&cached, &delta)[0].1, DeltaClass::Safe);
    s.close().await.unwrap();

    a.batch_execute(&format!("DROP TABLE {sc}.t"))
        .await
        .unwrap();
    let (gone, schema) = c.augment(&new, &hint).await.unwrap();
    assert_eq!(gone.changes, [SchemaChange::TableDropped]);
    assert!(schema.is_none());
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn channel_d_finds_dropped_changed_and_broken_tables() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_d",
        "CREATE TABLE t (id int PRIMARY KEY); CREATE TABLE u (id int PRIMARY KEY);
         CREATE TABLE w (id int PRIMARY KEY); CREATE TABLE same (id int PRIMARY KEY);
         CREATE TABLE l (id int PRIMARY KEY);",
    )
    .await;
    let c: Box<dyn SourceConnector> = Box::new(connector(&pipeline(&n, &["*"], "", "")));
    let known = c.resolve_tables().await.unwrap().tables;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("DROP TABLE {sc}.u"),
            format!("ALTER TABLE {sc}.t ADD COLUMN v bigint"),
            format!("ALTER TABLE {sc}.w REPLICA IDENTITY NOTHING"),
            // Allowed outside a FOR TABLE publication (under FOR ALL TABLES too): the table
            // stops publishing.
            format!("ALTER TABLE {sc}.l SET UNLOGGED"),
        ],
    )
    .await;
    let report = c.rescan(&known).await.unwrap();
    assert_eq!(report.dropped, [n.table("u")]);
    let changed: Vec<(&TableRef, Vec<&V>)> = report
        .changed
        .iter()
        .map(|s| {
            (
                s.table(),
                s.columns().iter().map(|c| &c.value_type).collect(),
            )
        })
        .collect();
    assert_eq!(changed, [(&n.table("t"), vec![&V::Int32, &V::Int64])]);
    let mut halted: Vec<(&TableRef, &str)> =
        report.halted.iter().map(|(t, r)| (t, r.code())).collect();
    halted.sort();
    assert_eq!(
        halted,
        [
            (&n.table("l"), "TABLE_NOT_REPLICABLE"),
            (&n.table("w"), "IDENTIFIER_NOT_IN_REPLICA_IDENTITY")
        ]
    );
    cleanup(&a, &n).await;
}

/// The signals of `tables` (one each), in any order.
async fn signals(s: &mut Box<dyn ChangeStream>, tables: usize) -> BTreeMap<String, SchemaHint> {
    let mut out = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while out.len() < tables {
        let item = tokio::time::timeout_at(deadline, s.next())
            .await
            .unwrap_or_else(|_| panic!("signals after 20 s: {:?}", out.keys()))
            .unwrap();
        if let StreamItem::SchemaSignal { table, hint, .. } = item {
            out.insert(table.name, hint);
        }
    }
    out
}

fn cached<'a>(r: &'a [TableSchema], name: &str) -> &'a TableSchema {
    r.iter().find(|t| t.table().name == name).unwrap()
}

/// Q-M5-17: with the catalog ahead of the stream, a column dropped since the signal keeps its
/// place, and renames since the signal that a placement would have to guess halt the table.
#[tokio::test(flavor = "multi_thread")]
async fn channel_c_with_the_catalog_ahead() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_c_ahead",
        "CREATE TABLE t (id int PRIMARY KEY, a int, b int);
         CREATE TABLE u (id int PRIMARY KEY, a int, b int);
         CREATE TABLE v (id int PRIMARY KEY, a int, b int);
         CREATE TABLE w (id int PRIMARY KEY, b int);",
    )
    .await;
    let c: Box<dyn SourceConnector> = Box::new(connector(&pipeline(&n, &["*"], "", "")));
    let r = c.resolve_tables().await.unwrap();
    let p = c
        .prepare(
            None,
            &[n.table("t"), n.table("u"), n.table("v"), n.table("w")],
        )
        .await
        .unwrap();
    let StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
    let sc = &n.schema;
    each(
        &a,
        &[
            // v: a column replaced (dropped, another renamed into its name), then a write.
            format!("ALTER TABLE {sc}.v DROP COLUMN a"),
            format!("ALTER TABLE {sc}.v RENAME COLUMN b TO a"),
            format!("INSERT INTO {sc}.v VALUES (1, 1)"),
            format!("ALTER TABLE {sc}.t ADD COLUMN c int"),
            format!("INSERT INTO {sc}.t VALUES (1, 1, 1, 1)"),
            format!("ALTER TABLE {sc}.u ADD COLUMN c int"),
            format!("INSERT INTO {sc}.u VALUES (1, 1, 1, 1)"),
            format!("ALTER TABLE {sc}.w ADD COLUMN a int"),
            format!("INSERT INTO {sc}.w VALUES (1, 1, 1)"),
            // After the signals: expand and contract on t, a rename chain on u, a rename on v,
            // the added column renamed and its name re-added on w.
            format!("ALTER TABLE {sc}.t DROP COLUMN b"),
            format!("ALTER TABLE {sc}.u RENAME COLUMN a TO x"),
            format!("ALTER TABLE {sc}.u RENAME COLUMN b TO a"),
            format!("ALTER TABLE {sc}.v RENAME COLUMN a TO x"),
            format!("ALTER TABLE {sc}.w RENAME COLUMN a TO x"),
            format!("ALTER TABLE {sc}.w ADD COLUMN a int"),
        ],
    )
    .await;
    let hints = signals(&mut s, 4).await;
    let (delta, new) = c
        .augment(cached(&r.tables, "t"), &hints["t"])
        .await
        .unwrap();
    let new = new.unwrap();
    assert_eq!(
        delta.changes,
        [SchemaChange::AddColumn(new.column("c").unwrap().clone())],
        "the later drop isn't in this change"
    );
    for table in ["u", "v", "w"] {
        match c.augment(cached(&r.tables, table), &hints[table]).await {
            Err(SourceError::TableHalt {
                reason: HaltReason::SchemaMismatch { .. },
                ..
            }) => {}
            other => panic!("{table}: a guess halts the table: {other:?}"),
        }
    }
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// `Schema_Change…` §7.2 (Q-M5-17): a primary key on other columns is an identifier change
/// (Unsafe); REPLICA IDENTITY FULL over the same key changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn channel_c_classifies_identity_changes() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_c_identity",
        "CREATE TABLE k (id int PRIMARY KEY, code int NOT NULL);
         CREATE TABLE f (id int PRIMARY KEY, v int);",
    )
    .await;
    let c: Box<dyn SourceConnector> = Box::new(connector(&pipeline(&n, &["*"], "", "")));
    let r = c.resolve_tables().await.unwrap();
    let p = c
        .prepare(None, &[n.table("k"), n.table("f")])
        .await
        .unwrap();
    let StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("ALTER TABLE {sc}.k DROP CONSTRAINT k_pkey, ADD PRIMARY KEY (code)"),
            format!("INSERT INTO {sc}.k VALUES (1, 1)"),
            // A change first: the session has seen f under its primary key.
            format!("INSERT INTO {sc}.f VALUES (0, 0)"),
            format!("ALTER TABLE {sc}.f REPLICA IDENTITY FULL"),
            format!("INSERT INTO {sc}.f VALUES (1, 1)"),
        ],
    )
    .await;
    let hints = signals(&mut s, 2).await;
    let k = cached(&r.tables, "k");
    let (delta, _) = c.augment(k, &hints["k"]).await.unwrap();
    assert!(
        delta.changes.contains(&SchemaChange::IdentifierChanged),
        "{delta:?}"
    );
    assert!(
        classify(k, &delta)
            .iter()
            .any(|(_, class)| matches!(class, DeltaClass::Unsafe(_)))
    );
    let (delta, _) = c
        .augment(cached(&r.tables, "f"), &hints["f"])
        .await
        .unwrap();
    assert!(delta.changes.is_empty(), "{delta:?}");
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0425]: cannot find type `ColumnSchema` in this scope
error[E0422]: cannot find struct, variant or union type `ColumnSchema` in this scope
error[E0425]: cannot find type `TableSchema` in this scope
error[E0425]: cannot find function `scope` in this scope
```

- [ ] **Step 3: Implement**

Add the implementation to `crates/data-avalanche-source-pg/src/schema.rs`, above the test module:

```rust
//! Channels C and D (`Schema_Change…` §3, §6): the change a schema signal implicates, proven
//! with `pg_attribute.attnum`, and the batched idle rescan.

use data_avalanche_core::{
    ColumnSchema, HaltReason, RescanReport, SchemaChange, SchemaDelta, SchemaHint, SourceError,
    TableRef, TableSchema,
};

use crate::catalog::{self, table_key};
use crate::connector::PgConnector;

/// The first length or precision in a type's text: `character varying(100)` → 100,
/// `timestamp(3) with time zone` → 3. A time type without one has the default precision, 6;
/// another type without one is unbounded (`None`).
fn bound(type_text: &str) -> Option<i64> {
    let explicit = || {
        let open = type_text.find('(')?;
        let close = type_text[open..].find([')', ','])? + open;
        type_text[open + 1..close].trim().parse().ok()
    };
    explicit().or_else(|| {
        let word = type_text.split(['(', ' ', '[']).next().unwrap_or_default();
        matches!(word, "timestamp" | "time" | "interval").then_some(6)
    })
}

/// Whether `to` holds fewer values than `from` when both map to the same logical type: a shorter
/// length, a lower time precision, a bound where there was none (`text` → `varchar(10)`). Only
/// the source can tell (`SchemaChange::ChangeType`).
pub fn narrowed(from: &ColumnSchema, to: &ColumnSchema) -> bool {
    if from.logical != to.logical {
        return false;
    }
    match (bound(&from.source_type), bound(&to.source_type)) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(f), Some(t)) => t < f,
    }
}

/// The schema the stream's columns imply (§0: scoped to the change, never the catalog's "now"):
/// each hint column takes its type from the stream and its identity (`attnum`) and nullability
/// from the catalog. It is found by name; else by the `attnum` it had in `cached` (renamed since
/// the signal), or with that `attnum` when the catalog no longer has it (dropped since the signal:
/// an `attnum` is never reused). The catalog may be ahead of the stream, so a placement that
/// doesn't fit the stream's columns is a guess, and the table halts (Q-M5-17):
/// - two columns on one `attnum`, or `attnum`s out of the stream's order (pgoutput sends them in
///   `attnum` order): renames or a swap since the signal;
/// - a column of `cached` the catalog still has, with no stream column on it: it existed at the
///   signal, so the stream carries it under some name (renamed away, and its name taken);
/// - a column placed as dropped since the signal while another column of `cached` is gone too and
///   placed nowhere: either could be the one the name meant;
/// - a column of the catalog with no stream column on it, below the highest placed column the
///   catalog still has: created before that one, it existed at the signal (a name re-added since);
/// - a column placed as dropped since the signal while a column of the catalog has no stream
///   column on it: a drop and an add since the signal read the same as a column re-added before it
///   and renamed since.
///
/// A column found no way halts too.
pub fn scope(
    cached: &TableSchema,
    hint: &[ColumnSchema],
    now: &TableSchema,
) -> Result<TableSchema, HaltReason> {
    let in_now = |o: Option<i32>| {
        now.columns()
            .iter()
            .find(|c| o.is_some() && c.source_ordinal == o)
    };
    let guess = |column: &str, why: &str| HaltReason::SchemaMismatch {
        column: column.into(),
        detail: format!("{why}; re-snapshot"),
    };
    let mut columns: Vec<ColumnSchema> = Vec::with_capacity(hint.len());
    let mut dropped_since = false;
    for h in hint {
        let (ordinal, nullable) = if let Some(c) = now.column(&h.name) {
            (c.source_ordinal, c.nullable)
        } else if let Some(was) = cached.column(&h.name) {
            match in_now(was.source_ordinal) {
                Some(c) => (c.source_ordinal, c.nullable),
                None => {
                    dropped_since = true;
                    (was.source_ordinal, was.nullable)
                }
            }
        } else {
            return Err(guess(
                &h.name,
                "the stream carries a column the catalog doesn't have by that name or position",
            ));
        };
        let ordinal = ordinal.ok_or_else(|| guess(&h.name, "a column without an attnum"))?;
        if let Some(last) = columns.last()
            && last.source_ordinal >= Some(ordinal)
        {
            return Err(guess(
                &h.name,
                "the catalog, ahead of the stream, doesn't place its columns in their order",
            ));
        }
        columns.push(ColumnSchema {
            nullable,
            source_ordinal: Some(ordinal),
            ..h.clone()
        });
    }
    let placed = |o: Option<i32>| columns.iter().any(|c| c.source_ordinal == o);
    for was in cached.columns() {
        let gone = in_now(was.source_ordinal).is_none();
        if !gone && !placed(was.source_ordinal) {
            return Err(guess(
                &was.name,
                "the catalog, ahead of the stream, has a column no stream column is placed on",
            ));
        }
        if dropped_since && gone && !placed(was.source_ordinal) {
            return Err(guess(
                &was.name,
                "two columns are gone from the catalog, and the stream's names don't say which",
            ));
        }
    }
    let top = columns
        .iter()
        .filter(|c| in_now(c.source_ordinal).is_some())
        .filter_map(|c| c.source_ordinal)
        .max();
    for c in now.columns() {
        if placed(c.source_ordinal) {
            continue;
        }
        if c.source_ordinal < top {
            return Err(guess(
                &c.name,
                "the catalog, ahead of the stream, has a column older than one the stream carries, \
                 and no stream column is placed on it",
            ));
        }
        if dropped_since {
            return Err(guess(
                &c.name,
                "a column dropped and one added since the signal read the same as a column \
                 re-added before it",
            ));
        }
    }
    let identifier = now
        .identifier()
        .iter()
        .map(|k| {
            let ordinal = now.column(k).and_then(|c| c.source_ordinal);
            columns
                .iter()
                .find(|c| c.source_ordinal == ordinal)
                .map(|c| c.name.clone())
                .ok_or_else(|| HaltReason::SchemaMismatch {
                    column: k.clone(),
                    detail: "the identifier column isn't in the stream's columns".into(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    TableSchema::new(cached.table().clone(), columns, identifier)
}

/// `cached` → `new` as changes, columns matched by `attnum`: the same `attnum` under another name
/// is a proven rename (§6).
pub fn diff(cached: &TableSchema, new: &TableSchema) -> Vec<SchemaChange> {
    let find = |o: Option<i32>| {
        new.columns()
            .iter()
            .find(|c| o.is_some() && c.source_ordinal == o)
    };
    let mut changes = vec![];
    for c in cached.columns() {
        let Some(n) = find(c.source_ordinal) else {
            changes.push(SchemaChange::DropColumn(c.name.clone()));
            continue;
        };
        if n.name != c.name {
            changes.push(SchemaChange::RenameColumn {
                from: c.name.clone(),
                to: n.name.clone(),
                proven: true,
            });
        }
        if n.value_type != c.value_type || n.source_type != c.source_type || n.logical != c.logical
        {
            changes.push(SchemaChange::ChangeType {
                column: n.name.clone(),
                from: c.clone(),
                to: n.clone(),
                narrowed: narrowed(c, n),
            });
        }
        match (c.nullable, n.nullable) {
            (true, false) => changes.push(SchemaChange::SetNotNull(n.name.clone())),
            (false, true) => changes.push(SchemaChange::DropNotNull(n.name.clone())),
            _ => {}
        }
    }
    for n in new.columns() {
        let known = cached
            .columns()
            .iter()
            .any(|c| c.source_ordinal.is_some() && c.source_ordinal == n.source_ordinal);
        if !known {
            changes.push(SchemaChange::AddColumn(n.clone()));
        }
    }
    let ordinals = |s: &TableSchema| -> Vec<Option<i32>> {
        s.identifier()
            .iter()
            .map(|k| s.column(k).and_then(|c| c.source_ordinal))
            .collect()
    };
    if ordinals(cached) != ordinals(new) {
        changes.push(SchemaChange::IdentifierChanged);
    }
    changes
}

impl PgConnector {
    /// The table's schema as the catalog has it now; `None` when it was dropped.
    async fn current(&self, table: &TableRef) -> Result<Option<TableSchema>, SourceError> {
        let client = self.conn.client().await?;
        let oids = self
            .conn
            .command(
                "find the table",
                catalog::oids_by_name(&client, std::slice::from_ref(table)),
            )
            .await?;
        let Some(oid) = oids.get(table) else {
            return Ok(None);
        };
        let mut schemas = self.schemas(&[*oid]).await?;
        match schemas.remove(oid) {
            None => Ok(None),
            Some((_, Ok((s, _)))) => Ok(Some(s)),
            Some((_, Err(reason))) => Err(SourceError::TableHalt {
                table: table.clone(),
                reason,
            }),
        }
    }

    /// Channel C: the change `hint` implicates, against `cached` (`Schema_Change…` §0, §3, §6).
    /// A catalog that can't place the stream's columns halts the table (fail-safe).
    pub async fn augment(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
        let table = cached.table().clone();
        let SchemaHint::Columns(columns) = hint else {
            return Err(SourceError::Permanent(format!(
                "{}: a DDL hint on a PostgreSQL source",
                table_key(&table)
            )));
        };
        let Some(now) = self.current(&table).await? else {
            return Ok((
                SchemaDelta {
                    table,
                    changes: vec![SchemaChange::TableDropped],
                },
                None,
            ));
        };
        let new = scope(cached, columns, &now).map_err(|reason| SourceError::TableHalt {
            table: table.clone(),
            reason,
        })?;
        let changes = diff(cached, &new);
        Ok((SchemaDelta { table, changes }, Some(new)))
    }

    /// Channel D: one batched catalog read of the tracked tables. A table gone from `pg_class`
    /// is dropped (§2.1, `TABLE_DROPPED`); a table whose catalog schema differs is reported with
    /// its current schema, for freshness and alerting only.
    pub async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
        let client = self.conn.client().await?;
        let tables: Vec<TableRef> = known.iter().map(|s| s.table().clone()).collect();
        let oids = self
            .conn
            .command("find the tables", catalog::oids_by_name(&client, &tables))
            .await?;
        let present: Vec<u32> = oids.values().copied().collect();
        let mut schemas = self.schemas(&present).await?;
        let mut report = RescanReport::default();
        for k in known {
            let t = k.table();
            match oids.get(t).and_then(|oid| schemas.remove(oid)) {
                None => report.dropped.push(t.clone()),
                Some((_, Err(reason))) => report.halted.push((t.clone(), reason)),
                Some((_, Ok((now, _)))) if *k != now => report.changed.push(now),
                Some(_) => {}
            }
        }
        Ok(report)
    }
}
```

Change `crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -2,11 +2,14 @@
 
 use std::collections::BTreeMap;
 
+use async_trait::async_trait;
 use data_avalanche_config::{
     Pipeline, Postgres, SourceType, check_table_set, check_unbounded_numeric,
 };
 use data_avalanche_core::{
-    HaltReason, Preflight, ResolvedTables, SourceError, SourcePosition, TableRef, TableSchema,
+    ChangeStream, Chunk, HaltReason, KeysetCursor, Preflight, Prepared, RescanReport,
+    ResolvedTables, Row, SchemaDelta, SchemaHint, SourceConnector, SourceError, SourcePosition,
+    TableRef, TableSchema,
 };
 use tokio::sync::Mutex;
 
@@ -23,7 +26,6 @@
 #[derive(Debug)]
 pub struct PgConnector {
     pub(crate) pipeline: Pipeline,
-    #[allow(dead_code)] // the slot lifecycle (Task 8) reads it
     pub(crate) pg: Postgres,
     pub(crate) conn: Connector,
     /// `pg_type` rows fetched so far.
@@ -248,10 +250,87 @@
     }
 }
 
+/// The trait is the inherent methods of the modules: `catalog` and here (channel A), `slot`
+/// (the lifecycle), `snapshot`, `stream`, `schema` (channels C and D).
+#[async_trait]
+impl SourceConnector for PgConnector {
+    async fn preflight(&self) -> Result<Preflight, SourceError> {
+        PgConnector::preflight(self).await
+    }
+
+    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
+        PgConnector::resolve_tables(self).await
+    }
+
+    async fn prepare(
+        &self,
+        checkpoint: Option<&SourcePosition>,
+        tables: &[TableRef],
+    ) -> Result<Prepared, SourceError> {
+        PgConnector::prepare(self, checkpoint, tables).await
+    }
+
+    async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
+        PgConnector::reset_position(self).await
+    }
+
+    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError> {
+        PgConnector::retained_bytes(self).await
+    }
+
+    async fn current_position(&self) -> Result<SourcePosition, SourceError> {
+        PgConnector::current_position(self).await
+    }
+
+    async fn read_chunk(
+        &self,
+        table: &TableSchema,
+        after: Option<&KeysetCursor>,
+        limit: usize,
+    ) -> Result<Chunk, SourceError> {
+        PgConnector::read_chunk(self, table, after, limit).await
+    }
+
+    async fn fetch_rows(&self, table: &TableSchema, keys: &[Row]) -> Result<Vec<Row>, SourceError> {
+        PgConnector::fetch_rows(self, table, keys).await
+    }
+
+    async fn open_stream(
+        &self,
+        from: &SourcePosition,
+        tables: &[TableSchema],
+    ) -> Result<Box<dyn ChangeStream>, SourceError> {
+        Ok(PgConnector::open_stream(self, from, tables).await?)
+    }
+
+    async fn augment(
+        &self,
+        cached: &TableSchema,
+        hint: &SchemaHint,
+    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
+        PgConnector::augment(self, cached, hint).await
+    }
+
+    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
+        PgConnector::rescan(self, known).await
+    }
+
+    async fn teardown(&self) -> Result<(), SourceError> {
+        PgConnector::teardown(self).await
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
     use crate::test_support::pipeline;
+
+    #[test]
+    fn a_connector_is_a_source_connector() {
+        let c: Box<dyn SourceConnector> =
+            Box::new(PgConnector::new(&pipeline("public.t")).unwrap());
+        drop(c);
+    }
 
     #[test]
     fn only_postgres_pipelines() {
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS. `data-avalanche-source-pg` unit tests: 67.

Run, with `test-infra/up.sh` running: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg`

Expected: PASS, 39 tests.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/lib.rs \
  crates/data-avalanche-source-pg/src/schema.rs \
  crates/data-avalanche-source-pg/src/connector.rs \
  crates/data-avalanche-source-pg/tests/pg/main.rs \
  crates/data-avalanche-source-pg/tests/pg/schema.rs
git commit -m "feat(source-pg): channels C and D, and the SourceConnector impl"
```

---

### Task 12: The type round trip

**Files:**
- Modify: `crates/data-avalanche-source-pg/tests/pg/main.rs`
- Create: `crates/data-avalanche-source-pg/tests/pg/types.rs`

**Interfaces:**
- Consumes: the crate's public API (Tasks 7–11).
- Produces: nothing for later tasks.

The roadmap's M5 exit: a round trip over every mapped type. One table holds a column of each
PostgreSQL type of Type_Mapping, extension types (hstore) and overrides included. It is filled with
ordinary, NULL and edge values (`±infinity`, the first and last dates, `24:00:00`, 38-digit
numerics, NaN under `string`, unicode, multi-byte delimiters), then read both ways: streamed and
snapshotted. The two must be equal, and equal to the expected `ColumnValue`s. Negative numeric
scales exist only on PostgreSQL 15 and later, so those columns are added there only.

A second test, `overrides_convert_through_stream_and_snapshot`, runs the lossless override
conversions of Task 5b both ways. Four converted columns form the key, and the snapshot reads
one row per chunk, so every cursor goes back to PostgreSQL as converted values. A third,
`string_and_decimal_overrides_through_stream_and_snapshot`, runs `string` on types that otherwise
decode, `decimal(P,S)` on `numeric` and `money`, and the remaining conversions at their bounds. A
fourth, `a_clamped_timestamp_key_halts_the_table`, pins Q-M5-18: a key at or past the last
microsecond a timestamp holds halts the table in the stream (a new row's key, a delete's old key;
`timestamp`, `timestamptz`, a composite key, a `date` converted) and in the snapshot, and
`±infinity` keys page.

- [ ] **Step 1: Write the round trip**

Replace `crates/data-avalanche-source-pg/tests/pg/main.rs` with:

```rust
//! The PostgreSQL source against a live server (`test-infra/compose.yaml`): `postgres-18` on every
//! change, PostgreSQL 14–17 nightly (`DATA_AVALANCHE_PG_PORT`). Each test returns at once without
//! `DATA_AVALANCHE_IT`.

mod common;
mod resolve;
mod schema;
mod slot;
mod snapshot;
mod stream;
mod types;
```

Create `crates/data-avalanche-source-pg/tests/pg/types.rs`:

```rust
//! S-PG7 as a regression, and the M5 exit's type round trip: every mapped PostgreSQL type
//! (`Type_Mapping.md`) inserted, then read through the stream and through a snapshot chunk. Both
//! give the expected values, and the same ones. The writer's session uses another time zone,
//! date style and interval style than the pinned ones (they change how its literals parse, not
//! how either read prints; Task 2 tests the pins).

use data_avalanche_core::{
    ChangeStream, ColumnValue as V, ColumnValueType as T, Decimal, HaltReason, KeysetCursor, Row,
    SourceError, StreamItem,
};
use data_avalanche_testkit::require_it;
use serde_json::json;

use crate::common::{admin, cleanup, collect, connector, first_stream, fresh, pipeline};

/// A column: its name, its type, and per row a SQL literal and the value it decodes to. A row
/// past the end of the list is NULL.
struct Case {
    column: &'static str,
    ty: &'static str,
    values: Vec<(&'static str, V)>,
}

fn case(column: &'static str, ty: &'static str, values: Vec<(&'static str, V)>) -> Case {
    Case { column, ty, values }
}

fn dec(unscaled: i128, scale: i8) -> V {
    V::Numeric(Decimal::new(unscaled, scale))
}

fn text(s: &str) -> V {
    V::Text(s.into())
}

fn array(element_type: T, values: Vec<V>) -> V {
    V::Array {
        element_type,
        values,
    }
}

fn cases(server_version: i32) -> Vec<Case> {
    let mut c = vec![
        case(
            "b",
            "bool",
            vec![("true", V::Bool(true)), ("false", V::Bool(false))],
        ),
        case(
            "i2",
            "int2",
            vec![
                ("-32768", V::Int16(i16::MIN)),
                ("32767", V::Int16(i16::MAX)),
            ],
        ),
        case(
            "i4",
            "int4",
            vec![
                ("2147483647", V::Int32(i32::MAX)),
                ("-2147483648", V::Int32(i32::MIN)),
            ],
        ),
        case(
            "i8",
            "int8",
            vec![
                ("9223372036854775807", V::Int64(i64::MAX)),
                ("-9223372036854775808", V::Int64(i64::MIN)),
            ],
        ),
        case(
            "o",
            "oid",
            vec![("4294967295", V::Int64(4_294_967_295)), ("0", V::Int64(0))],
        ),
        case(
            "ch",
            "\"char\"",
            vec![
                ("'a'", V::Int16(97)),
                ("'Z'", V::Int16(90)),
                // Not '\377': PostgreSQL 14 reads that as a backslash.
                ("(-1)::int4::\"char\"", V::Int16(255)),
                ("''", V::Int16(0)),
            ],
        ),
        case(
            "f4",
            "float4",
            vec![
                ("1.5", V::Float32(1.5)),
                ("'-Infinity'", V::Float32(f32::NEG_INFINITY)),
                ("'NaN'", V::Float32(f32::NAN)),
            ],
        ),
        case(
            "f8",
            "float8",
            vec![
                ("0.1", V::Float64(0.1)),
                ("'Infinity'", V::Float64(f64::INFINITY)),
                ("'NaN'", V::Float64(f64::NAN)),
            ],
        ),
        case(
            "n",
            "numeric(12,3)",
            vec![
                ("123456789.123", dec(123_456_789_123, 3)),
                ("-0.001", dec(-1, 3)),
            ],
        ),
        case(
            "n38",
            "numeric(38,0)",
            vec![
                (
                    "99999999999999999999999999999999999999",
                    dec(10i128.pow(38) - 1, 0),
                ),
                (
                    "-99999999999999999999999999999999999999",
                    dec(1 - 10i128.pow(38), 0),
                ),
            ],
        ),
        case(
            "n38s",
            "numeric(38,38)",
            vec![(
                "0.99999999999999999999999999999999999999",
                dec(10i128.pow(38) - 1, 38),
            )],
        ),
        case(
            "n40",
            "numeric(40,2)",
            vec![(
                "12345678901234567890123456789012345678.90",
                text("12345678901234567890123456789012345678.90"),
            )],
        ),
        case(
            "nb",
            "numeric",
            vec![
                ("1e-20", text("0.00000000000000000001")),
                ("'NaN'", text("NaN")),
            ],
        ),
        case(
            "m",
            "money",
            vec![
                ("1234.56", dec(123_456, 2)),
                ("'-92233720368547758.08'", dec(i64::MIN.into(), 2)),
                ("'92233720368547758.07'", dec(i64::MAX.into(), 2)),
            ],
        ),
        case(
            "bp",
            "char(5)",
            vec![("'ab'", text("ab")), ("''", text(""))],
        ),
        case(
            "vc",
            "varchar(10)",
            vec![("'héllo ✓'", text("héllo ✓")), ("''", text(""))],
        ),
        case(
            "tx",
            "text",
            vec![("'it''s \"q\" \\ {x}'", text("it's \"q\" \\ {x}"))],
        ),
        case("nm", "name", vec![("'nm'", text("nm"))]),
        case(
            "by",
            "bytea",
            vec![
                ("'\\xdeadbeef'", V::Bytes(vec![0xde, 0xad, 0xbe, 0xef])),
                ("'\\x'", V::Bytes(vec![])),
            ],
        ),
        case(
            "d",
            "date",
            vec![
                ("'2024-02-29'", V::Date(19_782)),
                ("'4714-11-24 BC'", V::Date(-2_440_588)),
                ("'infinity'", V::Date(i32::MAX)),
                ("'5874897-12-31'", V::Date(2_145_042_905)),
                ("'-infinity'", V::Date(i32::MIN)),
            ],
        ),
        case(
            "tm",
            "time",
            vec![
                ("'23:59:59.999999'", V::Time(86_399_999_999)),
                ("'24:00:00'", V::Time(86_400_000_000)),
            ],
        ),
        case(
            "tz",
            "timetz",
            vec![("'12:00:00+05:30'", text("12:00:00+05:30"))],
        ),
        case(
            "ts",
            "timestamp",
            vec![
                (
                    "'2024-02-29 12:34:56.789012'",
                    V::Timestamp {
                        micros: 1_709_210_096_789_012,
                        with_tz: false,
                    },
                ),
                (
                    "'0001-01-01 00:00:00 BC'",
                    V::Timestamp {
                        micros: -62_167_219_200_000_000,
                        with_tz: false,
                    },
                ),
                (
                    "'-infinity'",
                    V::Timestamp {
                        micros: i64::MIN,
                        with_tz: false,
                    },
                ),
                (
                    "'infinity'",
                    V::Timestamp {
                        micros: i64::MAX,
                        with_tz: false,
                    },
                ),
                (
                    // Past i64 microseconds: a column that isn't the key clamps (Q-M5-1).
                    "'294276-12-31 23:59:59.999999'",
                    V::Timestamp {
                        micros: i64::MAX,
                        with_tz: false,
                    },
                ),
            ],
        ),
        case(
            "tt",
            "timestamptz",
            vec![
                (
                    "'2024-02-29 12:34:56.789012+03'",
                    V::Timestamp {
                        micros: 1_709_199_296_789_012,
                        with_tz: true,
                    },
                ),
                (
                    "'infinity'",
                    V::Timestamp {
                        micros: i64::MAX,
                        with_tz: true,
                    },
                ),
                (
                    "'-infinity'",
                    V::Timestamp {
                        micros: i64::MIN,
                        with_tz: true,
                    },
                ),
            ],
        ),
        case(
            "iv",
            "interval",
            vec![
                ("'1 day 02:03:04'", text("P1DT2H3M4S")),
                ("'-1 mon'", text("P-1M")),
            ],
        ),
        case(
            "u",
            "uuid",
            vec![(
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
                V::Uuid(uuid::Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap()),
            )],
        ),
        case(
            "j",
            "json",
            vec![(
                r#"'{"b": 1, "a": [1, 2.5, null]}'"#,
                V::Json(json!({"b": 1, "a": [1, 2.5, null]})),
            )],
        ),
        case(
            "jb",
            "jsonb",
            vec![(
                r#"'{"b": 1, "a": [1, 2.5, null]}'"#,
                V::Json(json!({"b": 1, "a": [1, 2.5, null]})),
            )],
        ),
        case(
            "bt",
            "bit(5)",
            vec![("B'10101'", V::Bytes(vec![0b1010_1000]))],
        ),
        case(
            "vb",
            "varbit(10)",
            vec![
                ("B'101'", V::Bytes(vec![0, 0, 0, 3, 0b1010_0000])),
                ("B''", V::Bytes(vec![0, 0, 0, 0])),
            ],
        ),
        case(
            "ip",
            "inet",
            vec![("'192.168.0.1/24'", text("192.168.0.1/24"))],
        ),
        case("cd", "cidr", vec![("'10.0.0.0/8'", text("10.0.0.0/8"))]),
        case(
            "mc",
            "macaddr",
            vec![("'08:00:2b:01:02:03'", text("08:00:2b:01:02:03"))],
        ),
        case(
            "m8",
            "macaddr8",
            vec![("'08:00:2b:01:02:03:04:05'", text("08:00:2b:01:02:03:04:05"))],
        ),
        case("pt", "point", vec![("'(1.5,2)'", text("(1.5,2)"))]),
        case("bx", "box", vec![("'(1,1),(0,0)'", text("(1,1),(0,0)"))]),
        case("ci", "circle", vec![("'<(0,0),2>'", text("<(0,0),2>"))]),
        case(
            "r4",
            "int4range",
            vec![("'[1,10)'", text("[1,10)")), ("'empty'", text("empty"))],
        ),
        case(
            "tr",
            "tstzrange",
            vec![(
                "'[2024-01-01 00:00+00,2024-01-02 00:00+00)'",
                text("[\"2024-01-01 00:00:00+00\",\"2024-01-02 00:00:00+00\")"),
            )],
        ),
        case(
            "mr",
            "int4multirange",
            vec![("'{[1,3),[5,7)}'", text("{[1,3),[5,7)}"))],
        ),
        case(
            "tv",
            "tsvector",
            vec![("'a fat cat'", text("'a' 'cat' 'fat'"))],
        ),
        case("x", "xml", vec![("'<a>1</a>'", text("<a>1</a>"))]),
        case("e", "mood", vec![("'ok'", text("ok"))]),
        case("cp", "pair", vec![("ROW(1, 'x')", text("(1,x)"))]),
        case("dm", "price", vec![("12.5", dec(1250, 2))]),
        case(
            "hs",
            "public.hstore",
            vec![
                ("'a=>1, b=>NULL'", V::Json(json!({"a": "1", "b": null}))),
                (
                    r#"'"ключ"=>"знач ✓", "a\"b"=>"c\\d", "x y"=>"", "=>"=>","'"#,
                    V::Json(json!({"ключ": "знач ✓", "a\"b": "c\\d", "x y": "", "=>": ","})),
                ),
            ],
        ),
        case(
            "ai",
            "int4[]",
            vec![
                (
                    "'{1,NULL,3}'",
                    array(T::Int32, vec![V::Int32(1), V::Null, V::Int32(3)]),
                ),
                ("'{}'", array(T::Int32, vec![])),
                (
                    "'[0:1]={1,2}'",
                    array(T::Int32, vec![V::Int32(1), V::Int32(2)]),
                ),
            ],
        ),
        case(
            "at",
            "text[]",
            vec![
                (
                    r#"'{a,"b c",NULL,"NULL"}'"#,
                    array(T::Text, vec![text("a"), text("b c"), V::Null, text("NULL")]),
                ),
                (
                    r#"'{"héllo ✓","a\"b","c\\d","{x}","",",;"," sp "}'"#,
                    array(
                        T::Text,
                        vec![
                            text("héllo ✓"),
                            text("a\"b"),
                            text("c\\d"),
                            text("{x}"),
                            text(""),
                            text(",;"),
                            text(" sp "),
                        ],
                    ),
                ),
            ],
        ),
        case(
            "an",
            "numeric(5,2)[]",
            vec![(
                "'{1.5,-2}'",
                array(
                    T::Numeric {
                        precision: 5,
                        scale: 2,
                    },
                    vec![dec(150, 2), dec(-200, 2)],
                ),
            )],
        ),
        case(
            "ad",
            "date[]",
            vec![(
                "'{2024-01-01,infinity}'",
                array(T::Date, vec![V::Date(19_723), V::Date(i32::MAX)]),
            )],
        ),
        case(
            "ab",
            "box[]",
            vec![(
                "'{(1,1),(0,0);(2,2),(1,1)}'",
                array(T::Text, vec![text("(1,1),(0,0)"), text("(2,2),(1,1)")]),
            )],
        ),
        case(
            "ae",
            "mood[]",
            vec![("'{ok,sad}'", array(T::Text, vec![text("ok"), text("sad")]))],
        ),
    ];
    // Negative scales and scales above the precision: PostgreSQL 15 and later.
    if server_version >= 150_000 {
        c.push(case("ns", "numeric(3,-2)", vec![("12345", dec(12_300, 0))]));
        c.push(case("nf", "numeric(2,5)", vec![("0.00012", dec(12, 5))]));
    }
    c
}

fn rows(cases: &[Case]) -> usize {
    cases.iter().map(|c| c.values.len()).max().unwrap() + 1
}

/// Equal, or both a NaN (which `PartialEq` never finds equal).
fn same(a: &(String, V), b: &(String, V)) -> bool {
    a.0 == b.0
        && match (&a.1, &b.1) {
            (V::Float32(x), V::Float32(y)) if x.is_nan() => y.is_nan(),
            (V::Float64(x), V::Float64(y)) if x.is_nan() => y.is_nan(),
            (x, y) => x == y,
        }
}

/// Row `i` as expected: the id, then each column's value, NULL past its list.
fn expected(cases: &[Case], i: usize) -> Row {
    let mut r = vec![("id".to_string(), V::Int32(i as i32))];
    for c in cases {
        r.push((
            c.column.into(),
            c.values.get(i).map_or(V::Null, |v| v.1.clone()),
        ));
    }
    Row(r)
}

#[tokio::test(flavor = "multi_thread")]
async fn every_mapped_type_round_trips_through_stream_and_snapshot() {
    require_it!();
    let a = admin().await;
    a.batch_execute("CREATE EXTENSION IF NOT EXISTS hstore SCHEMA public")
        .await
        .unwrap();
    let version: i32 = a
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .unwrap()
        .get(0);
    let cases = cases(version);
    let columns: Vec<String> = cases
        .iter()
        .map(|c| format!("{} {}", c.column, c.ty))
        .collect();
    let n = fresh(
        &a,
        "types",
        &format!(
            "CREATE TYPE mood AS ENUM ('sad', 'ok');
             CREATE TYPE pair AS (n int, s text);
             CREATE DOMAIN price AS numeric(10,2);
             CREATE TABLE t (id int PRIMARY KEY, {});",
            columns.join(", ")
        ),
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, tables, _) = first_stream(&c).await;
    let t = &tables[0];
    assert!(t.columns().iter().all(|c| c.source_ordinal.is_some()));

    // The writer's session differs from the pinned one in every setting the text depends on.
    let mut inserts = vec![
        "SET TimeZone = 'America/St_Johns'".to_string(),
        "SET DateStyle = 'German, DMY'".into(),
        "SET IntervalStyle = 'postgres_verbose'".into(),
        "SET extra_float_digits = 0".into(),
        format!("SET search_path = {}, public", n.schema),
        "BEGIN".into(),
    ];
    for i in 0..rows(&cases) {
        let values: Vec<&str> = cases
            .iter()
            .map(|c| c.values.get(i).map_or("NULL", |v| v.0))
            .collect();
        let names: Vec<&str> = cases.iter().map(|c| c.column).collect();
        inserts.push(format!(
            "INSERT INTO t (id, {}) VALUES ({i}, {})",
            names.join(", "),
            values.join(", ")
        ));
    }
    inserts.push("COMMIT".into());
    a.batch_execute(&inserts.join(";\n")).await.unwrap();

    let items = collect(&mut s, 1, 30).await;
    let streamed: Vec<&Row> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => e.after.as_ref(),
            StreamItem::TableHalt { reason, .. } => panic!("halted: {reason}"),
            _ => None,
        })
        .collect();
    let mut snapshot = vec![];
    let mut cursor: Option<KeysetCursor> = None;
    loop {
        let chunk = c.read_chunk(t, cursor.as_ref(), 2).await.unwrap();
        let Some(next) = chunk.next else { break };
        snapshot.extend(chunk.rows);
        cursor = Some(next);
    }
    assert_eq!(streamed.len(), rows(&cases));
    assert_eq!(snapshot.len(), rows(&cases));
    for i in 0..rows(&cases) {
        let want = expected(&cases, i);
        for (j, w) in want.0.iter().enumerate() {
            assert!(
                same(&streamed[i].0[j], w),
                "stream row {i}: {:?} vs {w:?}",
                streamed[i].0[j]
            );
            assert!(
                same(&snapshot[i].0[j], w),
                "snapshot row {i}: {:?} vs {w:?}",
                snapshot[i].0[j]
            );
        }
        assert_eq!(streamed[i].0.len(), want.0.len());
        assert_eq!(snapshot[i].0.len(), want.0.len());
    }
    // Every value fits the column type the schema declares.
    for r in &snapshot {
        for (column, value) in &r.0 {
            let col = t.column(column).unwrap();
            assert!(
                value.fits(&col.value_type),
                "{column}: {value:?} vs {:?}",
                col.value_type
            );
        }
    }
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Config §9.3's lossless conversions, through the stream and through snapshot chunks of one row.
/// Four of the converted columns form the key, so each chunk's cursor goes back to PostgreSQL as
/// converted values: an `int8` for an `int4`, a `timestamptz` text for a `date` and a
/// `timestamp`, a zone-less text for a `timestamptz`.
#[tokio::test(flavor = "multi_thread")]
async fn overrides_convert_through_stream_and_snapshot() {
    require_it!();
    const DAY: i64 = 86_400_000_000;
    const HOUR: i64 = 3_600_000_000;
    let a = admin().await;
    let n = fresh(
        &a,
        "convert",
        "CREATE TABLE t (id int, d date, ts timestamp, tstz timestamptz, s smallint, f real,
                         a int[], PRIMARY KEY (id, d, ts, tstz));",
    )
    .await;
    let overrides = format!(
        "      column_type_overrides: {{ {s}.t.id: int64, {s}.t.d: timestamp_tz, \
         {s}.t.ts: timestamp_tz, {s}.t.tstz: timestamp_ntz, {s}.t.s: \"decimal(7,2)\", \
         {s}.t.f: float64, {s}.t.a: \"list<int64>\" }}",
        s = n.schema
    );
    let c = connector(&pipeline(&n, &["*"], &overrides, ""));
    let (mut s, tables, _) = first_stream(&c).await;
    let t = &tables[0];
    let types: Vec<(&str, T)> = t
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), c.value_type.clone()))
        .collect();
    assert_eq!(
        types,
        [
            ("id", T::Int64),
            ("d", T::Timestamp { with_tz: true }),
            ("ts", T::Timestamp { with_tz: true }),
            ("tstz", T::Timestamp { with_tz: false }),
            (
                "s",
                T::Numeric {
                    precision: 7,
                    scale: 2
                }
            ),
            ("f", T::Float64),
            ("a", T::Array(Box::new(T::Int64))),
        ]
    );

    // The key's order is decided by each of its parts in turn.
    let keys = [
        (1, "2024-01-01", "2024-01-01 10:00", "2024-01-01 10:00+00"),
        (1, "2024-01-01", "2024-01-01 10:00", "2024-01-01 11:00+00"),
        (1, "2024-01-01", "2024-01-01 11:00", "2024-01-01 09:00+00"),
        (1, "2024-01-02", "2024-01-01 09:00", "2024-01-01 09:00+00"),
        (2, "2023-12-31", "2023-12-31 23:00", "2023-12-31 23:00+00"),
    ];
    let mut inserts = vec![
        "SET TimeZone = 'America/St_Johns'".to_string(),
        format!("SET search_path = {}", n.schema),
        "BEGIN".into(),
    ];
    for (id, d, ts, tstz) in keys {
        inserts.push(format!(
            "INSERT INTO t VALUES ({id}, '{d}', '{ts}', '{tstz}', -32768, 0.1, '{{1,NULL}}')"
        ));
    }
    inserts.push("COMMIT".into());
    a.batch_execute(&inserts.join(";\n")).await.unwrap();

    let micros = |day: i64, hour: i64| day * DAY + hour * HOUR;
    let expected: Vec<Row> = [
        (1, 19_723, 10, 10),
        (1, 19_723, 10, 11),
        (1, 19_723, 11, 9),
        (1, 19_724, 9, 9),
        (2, 19_722, 23, 23),
    ]
    .into_iter()
    .map(|(id, day, ts, tstz)| {
        let ts_day = if id == 2 { 19_722 } else { 19_723 };
        Row(vec![
            ("id".into(), V::Int64(id)),
            (
                "d".into(),
                V::Timestamp {
                    micros: micros(day, 0),
                    with_tz: true,
                },
            ),
            (
                "ts".into(),
                V::Timestamp {
                    micros: micros(ts_day, ts),
                    with_tz: true,
                },
            ),
            (
                "tstz".into(),
                V::Timestamp {
                    micros: micros(ts_day, tstz),
                    with_tz: false,
                },
            ),
            ("s".into(), dec(-32_768, 0)),
            ("f".into(), V::Float64(0.1)),
            ("a".into(), array(T::Int64, vec![V::Int64(1), V::Null])),
        ])
    })
    .collect();

    let items = collect(&mut s, 1, 30).await;
    let streamed: Vec<Row> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => e.after.clone(),
            StreamItem::TableHalt { reason, .. } => panic!("halted: {reason}"),
            _ => None,
        })
        .collect();
    assert_eq!(streamed, expected, "the stream");
    let mut snapshot = vec![];
    let mut cursor: Option<KeysetCursor> = None;
    loop {
        let chunk = c.read_chunk(t, cursor.as_ref(), 1).await.unwrap();
        let Some(next) = chunk.next else { break };
        snapshot.extend(chunk.rows);
        cursor = Some(next);
    }
    assert_eq!(snapshot, expected, "snapshot chunks of one row");
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Overrides through both reads: `string` (D-M5.10) on types that otherwise decode, a decimal on
/// `numeric` and `money`, and the Q-M5-12 conversions the test above leaves out. A decimal keeps
/// the value's own scale; the declared type bounds it (`ColumnValue::fits`).
#[tokio::test(flavor = "multi_thread")]
async fn string_and_decimal_overrides_through_stream_and_snapshot() {
    require_it!();
    const DAY: i64 = 86_400_000_000;
    let a = admin().await;
    let n = fresh(
        &a,
        "convert_more",
        "CREATE TABLE t (id int PRIMARY KEY, b bool, ch \"char\", tt timestamptz, iv interval,
                         am int4[], at text[], n numeric(8,2), nb numeric, m money, s int2,
                         i int4, dt date, big int8);",
    )
    .await;
    let overrides = format!(
        "      column_type_overrides: {{ {s}.t.b: string, {s}.t.ch: string, {s}.t.tt: string, \
         {s}.t.iv: string, {s}.t.am: string, {s}.t.at: string, {s}.t.n: \"decimal(10,4)\", \
         {s}.t.nb: \"decimal(38,10)\", {s}.t.m: \"decimal(20,3)\", {s}.t.s: int32, \
         {s}.t.i: float64, {s}.t.dt: timestamp_ntz, {s}.t.big: \"decimal(25,2)\" }}",
        s = n.schema
    );
    let c = connector(&pipeline(&n, &["*"], &overrides, ""));
    let (mut s, tables, _) = first_stream(&c).await;
    let t = &tables[0];
    let num = |precision: u8, scale: i8| T::Numeric { precision, scale };
    let types: Vec<(&str, T)> = t
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), c.value_type.clone()))
        .collect();
    assert_eq!(
        types,
        [
            ("id", T::Int32),
            ("b", T::Text),
            ("ch", T::Text),
            ("tt", T::Text),
            ("iv", T::Text),
            ("am", T::Text),
            ("at", T::Text),
            ("n", num(10, 4)),
            ("nb", num(38, 10)),
            ("m", num(20, 3)),
            ("s", T::Int32),
            ("i", T::Float64),
            ("dt", T::Timestamp { with_tz: false }),
            ("big", num(25, 2)),
        ],
        "each override is the declared type"
    );
    a.batch_execute(&format!(
        "SET search_path = {};
         INSERT INTO t VALUES
           (1, true, 'a', '2024-02-29 12:34:56.789012+03', '1 day 02:03:04', '{{{{1,2}},{{3,NULL}}}}',
            '{{a,\"b c\"}}', 1.5, 123.456, 1234.56, -32768, 2147483647, '2024-01-01',
            9223372036854775807),
           (2, false, 'Z', '-infinity', '-1 mon', '{{}}', '{{}}', -999999.99, 0, '-0.01', 32767,
            -2147483648, 'infinity', -9223372036854775808);",
        n.schema
    ))
    .await
    .unwrap();
    let ts = |micros| V::Timestamp {
        micros,
        with_tz: false,
    };
    let expected = vec![
        Row(vec![
            ("id".into(), V::Int32(1)),
            ("b".into(), text("t")),
            ("ch".into(), text("a")),
            ("tt".into(), text("2024-02-29 09:34:56.789012+00")),
            ("iv".into(), text("P1DT2H3M4S")),
            ("am".into(), text("{{1,2},{3,NULL}}")),
            ("at".into(), text("{a,\"b c\"}")),
            ("n".into(), dec(150, 2)),
            ("nb".into(), dec(123_456, 3)),
            ("m".into(), dec(123_456, 2)),
            ("s".into(), V::Int32(-32_768)),
            ("i".into(), V::Float64(2_147_483_647.0)),
            ("dt".into(), ts(19_723 * DAY)),
            ("big".into(), dec(i64::MAX.into(), 0)),
        ]),
        Row(vec![
            ("id".into(), V::Int32(2)),
            ("b".into(), text("f")),
            ("ch".into(), text("Z")),
            ("tt".into(), text("-infinity")),
            ("iv".into(), text("P-1M")),
            ("am".into(), text("{}")),
            ("at".into(), text("{}")),
            ("n".into(), dec(-99_999_999, 2)),
            ("nb".into(), dec(0, 0)),
            ("m".into(), dec(-1, 2)),
            ("s".into(), V::Int32(32_767)),
            ("i".into(), V::Float64(-2_147_483_648.0)),
            ("dt".into(), ts(i64::MAX)),
            ("big".into(), dec(i64::MIN.into(), 0)),
        ]),
    ];
    let items = collect(&mut s, 1, 30).await;
    let streamed: Vec<Row> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => e.after.clone(),
            StreamItem::TableHalt { reason, .. } => panic!("halted: {reason}"),
            _ => None,
        })
        .collect();
    assert_eq!(streamed, expected, "the stream");
    let chunk = c.read_chunk(t, None, 10).await.unwrap();
    assert_eq!(chunk.rows, expected, "the snapshot");
    for row in &expected {
        for ((column, value), (_, ty)) in row.0.iter().zip(&types) {
            assert!(value.fits(ty), "{column}: {value:?} fits {ty:?}");
        }
    }
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Q-M5-18: a key at or past the last microsecond a timestamp holds halts the table, in the stream
/// (a new row's key, a delete's old key) and in the snapshot; `±infinity` keys page.
#[tokio::test(flavor = "multi_thread")]
async fn a_clamped_timestamp_key_halts_the_table() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "clamped_key",
        "CREATE TABLE t (ts timestamp PRIMARY KEY, v int);
         INSERT INTO t VALUES ('2024-01-01', 1), ('-infinity', 0), ('infinity', 2);
         CREATE TABLE z (tz timestamptz PRIMARY KEY);
         CREATE TABLE k (id int, ts timestamp, PRIMARY KEY (id, ts));
         CREATE TABLE d (d date PRIMARY KEY);
         CREATE TABLE o (ts timestamp PRIMARY KEY);
         INSERT INTO o VALUES ('294250-01-01');",
    )
    .await;
    let overrides = format!(
        "      column_type_overrides: {{ {}.d.d: timestamp_ntz }}",
        n.schema
    );
    let c = connector(&pipeline(&n, &["*"], &overrides, ""));
    let (mut s, tables, _) = first_stream(&c).await;
    let table = |name: &str| tables.iter().find(|t| t.table().name == name).unwrap();
    let halt = |r: &HaltReason, col: &str| {
        matches!(r, HaltReason::SchemaMismatch { column, detail }
            if column == col && detail.contains("can't key a row"))
    };
    let mut rows = 0;
    let mut cursor: Option<KeysetCursor> = None;
    loop {
        let chunk = c.read_chunk(table("t"), cursor.as_ref(), 1).await.unwrap();
        let Some(next) = chunk.next else { break };
        rows += chunk.rows.len();
        cursor = Some(next);
    }
    assert_eq!(rows, 3, "infinity keys page");
    match c.read_chunk(table("o"), None, 10).await {
        Err(SourceError::TableHalt { reason, .. }) if halt(&reason, "ts") => {}
        other => panic!("o: the snapshot halts: {other:?}"),
    }
    // One transaction: a key past the range, the last microsecond (with an offset), a composite
    // key, a date converted to a timestamp, and a delete's old key.
    a.batch_execute(&format!(
        "SET search_path = {};
         INSERT INTO t VALUES ('294250-01-01', 3);
         INSERT INTO z VALUES ('294247-01-10 09:30:54.775807+05:30');
         INSERT INTO k VALUES (1, '294276-12-31 23:59:59.999999');
         INSERT INTO d VALUES ('300000-01-01');
         DELETE FROM o;",
        n.schema
    ))
    .await
    .unwrap();
    let items = collect(&mut s, 1, 20).await;
    for (name, column) in [
        ("t", "ts"),
        ("z", "tz"),
        ("k", "ts"),
        ("d", "d"),
        ("o", "ts"),
    ] {
        assert!(
            items
                .iter()
                .any(|i| matches!(i, StreamItem::TableHalt { table, reason, .. }
                if table.name == name && halt(reason, column))),
            "{name}: the stream halts: {items:?}"
        );
    }
    assert!(
        !items.iter().any(|i| matches!(i, StreamItem::Change(_))),
        "no change of a halted table: {items:?}"
    );
    for (name, column) in [("t", "ts"), ("z", "tz"), ("k", "ts"), ("d", "d")] {
        match c.read_chunk(table(name), None, 10).await {
            Err(SourceError::TableHalt { reason, .. }) if halt(&reason, column) => {}
            other => panic!("{name}: the snapshot halts: {other:?}"),
        }
    }
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-source-pg --test pg`

Expected: PASS, 43 tests. Without `DATA_AVALANCHE_IT` every test returns at once. These tests drive the code of Tasks 2–11, so nothing fails first. A failure in Step 3 is a defect in that code: stop and report it.

- [ ] **Step 3: Run them against PostgreSQL**

Run: `test-infra/up.sh`, then `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg`

Expected: PASS, 43 tests.

- [ ] **Step 4: Run the gates**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/tests/pg/main.rs \
  crates/data-avalanche-source-pg/tests/pg/types.rs
git commit -m "test(source-pg): the type round trip"
```

---

### Task 13: TLS end to end, and PostgreSQL 14–17 nightly

**Files:**
- Modify: `crates/data-avalanche-source-pg/tests/pg/main.rs`
- Create: `crates/data-avalanche-source-pg/tests/pg/tls.rs`
- Modify: `test-infra/compose.yaml`
- Create: `test-infra/postgres-tls.sh`
- Modify: `.gitignore`
- Modify: `test-infra/README.md`
- Create: `.github/workflows/nightly.yml`

**Interfaces:**
- Consumes: the crate's public API; `common::{Names, fresh, cleanup, connector, first_stream, collect}`.
- Produces: the `postgres-tls` service (port 55419) and `nightly.yml`.

S-PG8 and Q-M5-7. `postgres-tls` refuses any TCP connection without TLS. At each start it
makes a throwaway CA, a server certificate for `localhost` only, and a client certificate for the
role `avalanche_cert`. It writes the CA and the client pair to the git-ignored
`test-infra/.tls/`; the server key stays in the container. The tests check, in `pg_stat_ssl`, that
the catalog and the replication connection both run over TLS: once under `verify-full` with an
inline CA, and once with a client certificate and an inline key (written to private files for the
replication connection, Q-M5-4). A foreign CA (under `verify-full` and `verify-ca`) and `disable`
are refused as `Permanent`. `verify-ca` by `127.0.0.1`, a name the certificate lacks, passes on
the catalog connection and is refused on the stream: this pins the Task 2 deviation
(pgwire-replication 0.4.1 checks the name under `verify-ca`).

`nightly.yml` runs the suite on PostgreSQL 14–17, one job each, through `DATA_AVALANCHE_PG_PORT`.
The TLS tests need `postgres-tls` and run in `ci.yml` only.

- [ ] **Step 1: Write the TLS service, its tests and the nightly workflow**

Replace `crates/data-avalanche-source-pg/tests/pg/main.rs` with:

```rust
//! The PostgreSQL source against a live server (`test-infra/compose.yaml`): `postgres-18` on every
//! change, PostgreSQL 14–17 nightly (`DATA_AVALANCHE_PG_PORT`). Each test returns at once without
//! `DATA_AVALANCHE_IT`.

mod common;
mod resolve;
mod schema;
mod slot;
mod snapshot;
mod stream;
mod tls;
mod types;
```

Create `crates/data-avalanche-source-pg/tests/pg/tls.rs`:

```rust
//! TLS on both connections (S-PG8, Q-M5-4) against `postgres-tls`, which refuses any TCP
//! connection without TLS. Its throwaway CA and client certificate are made when the container
//! starts and read from the git-ignored `test-infra/.tls`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::{ChangeStream, SourceError};
use data_avalanche_testkit::require_it;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use tokio_postgres::Client;

use crate::common::{Names, cleanup, collect, connector, first_stream, fresh};

const PORT: u16 = 55419;

fn material(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-infra/.tls")
        .join(name)
}

fn read(name: &str) -> String {
    std::fs::read_to_string(material(name))
        .unwrap_or_else(|e| panic!("{name} from test-infra/.tls (is postgres-tls up?): {e}"))
}

/// A superuser connection over TLS, verifying the test CA.
async fn admin() -> Client {
    data_avalanche_testkit::install_crypto_provider();
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(material("ca.crt")).expect("the test CA") {
        roots.add(cert.unwrap()).unwrap();
    }
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls);
    let dsn = format!(
        "host=localhost port={PORT} user=avalanche password=avalanche dbname=avalanche sslmode=require"
    );
    let (client, conn) = tokio_postgres::connect(&dsn, tls)
        .await
        .expect("connect to postgres-tls");
    tokio::spawn(conn);
    client
}

/// A pipeline on `postgres-tls` at `host` as `user`, with `ssl` (YAML flow entries) and `env` for
/// its `${…}` references. The server certificate names `localhost` only.
fn pipeline(n: &Names, host: &str, user: &str, ssl: &str, env: &[(&str, String)]) -> Pipeline {
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: p
    source:
      host: {host}
      port: {PORT}
      db: avalanche
      user: {user}
      password: avalanche
      {ssl}
      postgres: {{ publication: {publication}, slot: {slot}, heartbeat: 1s }}
    tables:
      include: [{schema}.t]
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#,
        publication = n.publication,
        slot = n.slot,
        schema = n.schema,
    );
    let env: BTreeMap<String, String> = env
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    let loaded = load_str(&yaml, &env, &FingerprintKey::from_bytes([1; 32]))
        .unwrap_or_else(|r| panic!("test pipeline: {r:?}"));
    loaded
        .config
        .pipelines
        .into_values()
        .next()
        .expect("one pipeline")
}

/// `(user, backend type, ssl, client_dn)` of `user`'s catalog sessions (`data-avalanche`) and of
/// the walsender holding the test's slot (it reports `pgwire-replication`: the crate hard-codes
/// its `application_name`).
async fn sessions(
    a: &Client,
    user: &str,
    slot: &str,
) -> Vec<(String, String, bool, Option<String>)> {
    a.query(
        "SELECT a.usename::text, a.backend_type, s.ssl, s.client_dn
           FROM pg_stat_activity a JOIN pg_stat_ssl s ON s.pid = a.pid
          WHERE a.usename = $1
            AND (a.application_name = 'data-avalanche'
                 OR a.pid = (SELECT active_pid FROM pg_replication_slots WHERE slot_name = $2))
          ORDER BY 2",
        &[&user, &slot],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
    .collect()
}

/// Opens a stream, streams one insert and returns the sessions while it is open.
async fn stream_one(
    a: &Client,
    p: &Pipeline,
    n: &Names,
) -> Vec<(String, String, bool, Option<String>)> {
    let c = connector(p);
    c.preflight().await.expect("preflight over TLS");
    let (mut s, _, _) = first_stream(&c).await;
    a.batch_execute(&format!("INSERT INTO {}.t VALUES (1)", n.schema))
        .await
        .unwrap();
    let items = collect(&mut s, 1, 20).await;
    assert_eq!(items.len(), 3, "Begin, the insert, Commit: {items:?}");
    let seen = sessions(a, p.source.credentials.user(), &n.slot).await;
    s.close().await.expect("close");
    seen
}

#[tokio::test]
async fn verify_full_with_an_inline_ca_encrypts_both_connections() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "tls_full", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let p = pipeline(
        &n,
        "localhost",
        "avalanche",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
        &[("CA", read("ca.crt"))],
    );
    let seen = stream_one(&a, &p, &n).await;
    let kinds: Vec<&str> = seen.iter().map(|s| s.1.as_str()).collect();
    // The connector's catalog connection, the stream's own, and the replication connection.
    assert_eq!(kinds, ["client backend", "client backend", "walsender"]);
    assert!(seen.iter().all(|s| s.2), "both over TLS: {seen:?}");
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_client_certificate_with_an_inline_key_authenticates_both_connections() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "tls_cert", "CREATE TABLE t (id int PRIMARY KEY);").await;
    // The CA by path (passed through), the certificate and key inline (written to private files
    // for the replication connection).
    let ca = material("ca.crt").to_string_lossy().into_owned();
    let p = pipeline(
        &n,
        "localhost",
        "avalanche_cert",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"\n      ssl_cert: \"${CERT}\"\n      ssl_key: \"${KEY}\"",
        &[
            ("CA", ca),
            ("CERT", read("client.crt")),
            ("KEY", read("client.key")),
        ],
    );
    let seen = stream_one(&a, &p, &n).await;
    assert_eq!(seen.len(), 3, "{seen:?}");
    for (user, _, ssl, dn) in &seen {
        assert_eq!(user, "avalanche_cert");
        assert!(ssl);
        assert_eq!(dn.as_deref(), Some("/CN=avalanche_cert"));
    }
    cleanup(&a, &n).await;
}

/// `verify-ca` ignores the host name on the catalog connection (D-M5.16) but not on the
/// replication connection: pgwire-replication 0.4.1 forgives only rustls's `NotValidForName`, and
/// rustls 0.23 reports `NotValidForNameContext` (the plan's Deviations, Task 2). This pins the
/// difference: once the crate forgives the mismatch, the stream connects and this test fails.
#[tokio::test]
async fn verify_ca_by_an_unnamed_address_passes_the_catalog_but_not_the_stream() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "tls_verify_ca", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let p = pipeline(
        &n,
        "127.0.0.1",
        "avalanche",
        "ssl_mode: verify-ca\n      ssl_ca: \"${CA}\"",
        &[("CA", read("ca.crt"))],
    );
    let c = connector(&p);
    c.preflight()
        .await
        .expect("the catalog connection ignores the name");
    let (mut s, _, _) = first_stream(&c).await;
    match s.next().await {
        Err(SourceError::Permanent(detail)) => {
            assert!(detail.contains("not valid for name"), "{detail}")
        }
        other => panic!("the stream should refuse the name, got {other:?}"),
    }
    drop(s);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_wrong_ca_or_no_tls_is_refused() {
    require_it!();
    let n = Names::new("tls_refused");
    let other_ca = {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(vec![]).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.self_signed(&key).unwrap().pem()
    };
    for (what, ssl, env) in [
        (
            "a foreign CA",
            "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
            vec![("CA", other_ca.clone())],
        ),
        (
            "a foreign CA, chain only",
            "ssl_mode: verify-ca\n      ssl_ca: \"${CA}\"",
            vec![("CA", other_ca)],
        ),
        ("no TLS", "ssl_mode: disable", vec![]),
    ] {
        let c = connector(&pipeline(&n, "localhost", "avalanche", ssl, &env));
        match c.preflight().await {
            Err(SourceError::Permanent(detail)) => eprintln!("{what}: {detail}"),
            other => panic!("{what}: a permanent error expected, got {other:?}"),
        }
    }
}
```

Change `test-infra/compose.yaml`:

```diff
diff --git a/test-infra/compose.yaml b/test-infra/compose.yaml
--- a/test-infra/compose.yaml
+++ b/test-infra/compose.yaml
@@ -22,6 +22,14 @@
   postgres-16: { <<: *pg, image: "postgres:16", ports: ["55416:5432"], profiles: [matrix] }
   postgres-15: { <<: *pg, image: "postgres:15", ports: ["55415:5432"], profiles: [matrix] }
   postgres-14: { <<: *pg, image: "postgres:14", ports: ["55414:5432"], profiles: [matrix] }
+  # TLS required on every TCP connection, throwaway certificates made at start (postgres-tls.sh).
+  postgres-tls:
+    image: "postgres:18"
+    environment: { POSTGRES_USER: avalanche, POSTGRES_PASSWORD: avalanche, POSTGRES_DB: avalanche }
+    entrypoint: ["bash", "/postgres-tls.sh"]
+    volumes: ["./postgres-tls.sh:/postgres-tls.sh:ro", "./.tls:/tls"]
+    ports: ["55419:5432"]
+    healthcheck: { test: ["CMD-SHELL", "pg_isready -U avalanche -d avalanche"], interval: 2s, retries: 30 }
 
   mysql-84:
     image: "mysql:8.4"
```

Create `test-infra/postgres-tls.sh`:

```bash
#!/usr/bin/env bash
# Entrypoint of the `postgres-tls` service: a fresh throwaway CA, server certificate (localhost
# only, so a connection by 127.0.0.1 is a name mismatch) and client certificate (CN
# avalanche_cert) at every start, then PostgreSQL with TLS required for every TCP connection. The
# CA and the client pair are copied to /tls (bind-mounted from the git-ignored test-infra/.tls)
# for the tests; the server key stays in the container.
set -euo pipefail
server=/var/lib/postgresql/tls
mkdir -p "$server" /tls
cd "$server"
rm -f ./*
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 30 \
  -subj "/CN=data-avalanche test CA" -keyout ca.key -out ca.crt 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=localhost" \
  -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=DNS:localhost\n' > server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 \
  -extfile server.ext -out server.crt 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=avalanche_cert" \
  -keyout client.key -out client.csr 2>/dev/null
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 -out client.crt 2>/dev/null
chown postgres:postgres server.key && chmod 600 server.key
cp ca.crt client.crt client.key /tls/ && chmod 644 /tls/*
cat > "$server/pg_hba.conf" <<'HBA'
local     all  all             trust
hostssl   all  avalanche_cert  all  cert
hostssl   all  all             all  scram-sha-256
hostnossl all  all             all  reject
HBA
# The role the client certificate maps to (the data directory is fresh at every start).
mkdir -p /docker-entrypoint-initdb.d
echo "CREATE ROLE avalanche_cert SUPERUSER LOGIN REPLICATION PASSWORD 'unused';" \
  > /docker-entrypoint-initdb.d/cert-role.sql
exec docker-entrypoint.sh postgres -c wal_level=logical -c max_replication_slots=20 \
  -c max_wal_senders=20 -c ssl=on -c ssl_cert_file="$server/server.crt" \
  -c ssl_key_file="$server/server.key" -c ssl_ca_file="$server/ca.crt" \
  -c hba_file="$server/pg_hba.conf"
```

Change `.gitignore`:

```diff
diff --git a/.gitignore b/.gitignore
--- a/.gitignore
+++ b/.gitignore
@@ -1,2 +1,3 @@
 target/
 prototypes/*/target/
+test-infra/.tls/
```

Change `test-infra/README.md`:

````diff
diff --git a/test-infra/README.md b/test-infra/README.md
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -10,6 +10,7 @@
 |---|---|---|---|---|
 | postgres-18 | postgres:18 | 55418 | avalanche / avalanche, db `avalanche` | default |
 | postgres-14…17 | postgres:14…17 | 55414…55417 | same | matrix |
+| postgres-tls | postgres:18, TLS required (`postgres-tls.sh`) | 55419 | avalanche / avalanche; `avalanche_cert` by client certificate | default |
 | mysql-84 | mysql:8.4 (GTID on) | 53384 | root / avalanche, db `avalanche` | default |
 | mariadb-118 | mariadb:11.8 (`gtid_strict_mode=1`) | 53318 | root / avalanche | default |
 | mariadb-114, mariadb-1011 | mariadb:11.4, 10.11 | 53314, 53311 | root / avalanche | matrix |
@@ -19,8 +20,25 @@
 Bucket / container `data-avalanche-test` is created by the `init` profile (`up.sh` runs it). Azurite keeps
 port 10000 because `object_store`'s emulator mode always targets `127.0.0.1:10000`.
 
-Added by later milestones: `postgres-tls` (M5, CA-signed server certificate) and `mysql-84-nogtid`
-(M7, file/position mode).
+Added by a later milestone: `mysql-84-nogtid` (M7, file/position mode).
+
+## PostgreSQL
+
+The PostgreSQL source's tests (`crates/data-avalanche-source-pg/tests/pg`) run against
+`postgres-18`; `DATA_AVALANCHE_PG_PORT` points them at another server, and
+`.github/workflows/nightly.yml` runs them on PostgreSQL 14–17 every night (and on manual dispatch):
+
+```sh
+docker compose -f test-infra/compose.yaml --profile matrix up -d --wait postgres-14
+DATA_AVALANCHE_IT=1 DATA_AVALANCHE_PG_PORT=55414 cargo test -p data-avalanche-source-pg --test pg --test slot_invalidation -- --skip tls::
+```
+
+`postgres-tls` refuses every TCP connection without TLS. At each start it makes a throwaway CA, a
+server certificate for `localhost` only (a connection by `127.0.0.1` is a name mismatch, which
+the `verify-ca` test needs), and a client certificate for the role
+`avalanche_cert`, which authenticates by certificate (`pg_hba` `cert`). It writes the CA and the
+client pair to `test-infra/.tls/` (git-ignored) for the TLS tests; the server key stays in the
+container. Nothing is committed.
 
 ## GCS: a real bucket, not an emulator
 
````

Create `.github/workflows/nightly.yml`:

```yaml
name: nightly

# The PostgreSQL source against the older supported servers (P-8, Q-M5-7): PostgreSQL 14–17, one
# job each, the `postgres-18` suite of ci.yml re-run through DATA_AVALANCHE_PG_PORT. The TLS tests
# need `postgres-tls` (PostgreSQL 18) and run in ci.yml only. No secrets.

on:
  schedule:
    - cron: "41 3 * * *"
  workflow_dispatch:

permissions:
  contents: read

env:
  CARGO_TERM_COLOR: always

jobs:
  postgres:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    strategy:
      fail-fast: false
      matrix:
        version: [14, 15, 16, 17]
    env:
      DATA_AVALANCHE_IT: "1"
      DATA_AVALANCHE_SKIP_GCS: "1"
      DATA_AVALANCHE_PG_PORT: "554${{ matrix.version }}"
    steps:
      - uses: actions/checkout@v7
      - uses: actions-rust-lang/setup-rust-toolchain@v2
      - name: Start PostgreSQL ${{ matrix.version }}
        run: docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "postgres-${{ matrix.version }}"
      - name: PostgreSQL source tests
        run: cargo test --locked -p data-avalanche-source-pg --test pg --test slot_invalidation -- --test-threads=1 --skip tls::
      - name: Container logs on failure
        if: failure()
        run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "postgres-${{ matrix.version }}"
      - name: Stop PostgreSQL
        if: always()
        run: docker compose -f test-infra/compose.yaml --profile matrix down -v
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-source-pg --test pg`

Expected: PASS, 47 tests. Without `DATA_AVALANCHE_IT` every test returns at once. These tests drive the code of Tasks 2–11, so nothing fails first. A failure in Step 3 is a defect in that code: stop and report it.

- [ ] **Step 3: Run them against PostgreSQL**

Run: `test-infra/up.sh` (it now starts `postgres-tls` too), then `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg`

Expected: PASS, 47 tests.

Then the nightly matrix by hand, one server at a time (55414…55417):

```bash
docker compose -f test-infra/compose.yaml --profile matrix up -d --wait postgres-14
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_PG_PORT=55414 cargo test -p data-avalanche-source-pg --test pg --test slot_invalidation -- --skip tls::
```

Expected: PASS on each, 43 tests, and the `slot_invalidation` test.

- [ ] **Step 4: Run the gates**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/tests/pg/main.rs \
  crates/data-avalanche-source-pg/tests/pg/tls.rs \
  test-infra/compose.yaml \
  test-infra/postgres-tls.sh \
  .gitignore \
  test-infra/README.md \
  .github/workflows/nightly.yml
git commit -m "test(source-pg): TLS end to end, and PostgreSQL 14-17 nightly"
```

---

## Verified during planning (2026-10-01)

The task blocks above were replayed in order on a fresh clone of the milestone branch
(`origin/main` `9502c8c` plus the plan commits). The replay applied each task's files exactly as
shown and ran each step's command. Every expected failure and every count above is that replay's
output. Outside `doc/`, the replayed tree equals the verified workspace. Measured along the way:

- **PostgreSQL 18** (`postgres-18`, `postgres-tls`): 34 integration tests pass (Task 13's run).
- **PostgreSQL 14, 15, 16, 17** (the `matrix` profile): 30 integration tests pass on each,
  the TLS tests skipped (the nightly command). PostgreSQL 14 needed the empty-transaction handling
  of D-M5.7 in the tests' idle loop.
- **TLS** (Task 13): `pg_stat_ssl` shows TLS on the catalog connections and the walsender, under
  `verify-full` with an inline CA, and with a client certificate whose key is inline. A foreign CA
  fails with `invalid peer certificate: UnknownIssuer`. `disable` is refused by `pg_hba`
  (SQLSTATE 28000). Both are `Permanent`. `verify-ca` by `127.0.0.1` (a name the certificate
  lacks) passes on the catalog connection; the stream is refused with `certificate not valid for
  name "127.0.0.1"` (the Task 2 deviation, re-measured 2026-10-01).
- **Workspace:** 623 tests pass with plain `cargo test --workspace`. Clippy, fmt and
  rustdoc (`-D warnings`) are clean.
- **Not run while planning:** the CI workflows themselves. GitHub schedules and dispatches a
  workflow only from the default branch, so `nightly.yml` first runs after the merge. Until then
  the matrix is Task 13's by-hand run.

## M5 exit checklist

The roadmap's M5 exit criteria, and where each is met:

- [ ] **Every probe gate S-PG0…8 has a regression test, against PostgreSQL 18 on every change and
  PostgreSQL 14–17 nightly**:
  - S-PG0…S-PG6: Task 9 (`s_pg0_an_insert_round_trips` … `s_pg6_a_table_added_to_the_publication_mid_stream`);
  - S-PG7: Task 12 (`every_mapped_type_round_trips_through_stream_and_snapshot`);
  - S-PG8: Task 13 (`tls.rs`);
  - PostgreSQL 18 in `ci.yml`'s integration job; 14–17 in `nightly.yml` (Task 13).
- [ ] **A type round-trip suite covers every mapped type**: Task 12, with Task 4's property tests
  for the date and timestamp ranges.
- [ ] **The slot state-machine table is exercised cell by cell**: Task 8 (`decide`'s seven unit
  tests, and the integration cells: first start, resume, lost slot, orphan under each policy,
  `require_existing`).
- [ ] The M5 scope items not in the exit list, each with its test:
  - the pgoutput decoder (Task 3; text tuples only, Q-M5-3);
  - Hop 1 and the fallback chain, the numeric cap, array elements (Tasks 4, 5, 12);
  - the replication wrapper: session pinning, status interval, acknowledgment (Tasks 2, 9);
  - publication reconciliation (Task 8);
  - preflight (Task 7);
  - channels A, C and D (Tasks 6, 7, 11);
  - the keyset chunk reader and the watermark (Tasks 7, 10);
  - the `max_wal_lag_bytes` measure, `retained_bytes` (Task 8). The halt is M6's.
- [ ] `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` and `RUSTDOCFLAGS="-D warnings" cargo doc -p data-avalanche-source-pg
  -p data-avalanche-core --no-deps` are clean.
- [ ] The integration job of PR CI is green (`postgres-18` and `postgres-tls` included). The
  by-hand matrix run of Task 13 is green on the final tree. `nightly.yml` is dispatched once on
  `main` after the merge.
- [ ] Roadmap updates:
  - §2 and §3.6 (the contract in core, D-M5.1);
  - §2.1's pins;
  - §4 M5's scope notes and M6's "Carried from M5";
  - §5.2 and §5.4;
  - §9.1 Q-M5-2…10;
  - D-IMPL-12 for the rulings D-M5.1…19.
- [ ] `Type_Mapping.md` records the numeric rules (D-M5.9, Q-M5-8), the `varbit` layout (Q-M5-9)
  and the PostGIS form.
- [ ] `doc/v1.0/README.md` lists this plan and its status.

## Deviations during execution

**Task 2: `verify-ca` on the replication connection also checks the host name** (found by the
task review, confirmed in the crate source). The code is as planned; D-M5.16 doesn't hold for
the replication connection in two respects:

- pgwire-replication 0.4.1's `verify-ca` verifier forgives only `CertificateError::NotValidForName`
  (`src/tls/rustls.rs:453`). rustls 0.23.45 reports a name mismatch as `NotValidForNameContext`,
  so the mismatch is fatal there. On the replication connection `verify-ca` behaves like
  `verify-full`. A pipeline that connects by a name its server certificate doesn't carry works on
  the catalog connection and is refused, `Permanent`, on the stream. It fails closed. Our code
  can't route around it: `TlsConfig` takes only paths and a mode.
- Under `require` and `prefer`, the crate's verifier skips the handshake-signature checks
  (`src/tls/rustls.rs:395-411`). "Signatures are still checked" holds for the catalog connection
  only. With the chain unchecked in those modes, this adds no real exposure.

Also noted for M6 (DA-DEP-5): pgwire-replication installs `ring` as the process-wide rustls
provider when none is installed (`src/tls/rustls.rs:173`) and builds its config from the process
default. The binary must install aws-lc-rs before the first replication connection. The tests do,
through `data_avalanche_testkit::install_crypto_provider`.

Owner ruling (2026-10-01): keep the code and pin the difference with a test. Task 13 gained
`verify_ca_by_an_unnamed_address_passes_the_catalog_but_not_the_stream`, and `postgres-tls`'s
server certificate now names `localhost` only, so connecting by `127.0.0.1` is a name mismatch.
The test fails once the crate forgives the mismatch, which is the signal to drop this deviation.

**Task 4: two fixes to the planned code** (both found by the task review):

- **Out-of-range timestamps clamp** (`aba85ec`). The planned `timestamp()` halted the table when
  an instant didn't fit in `i64` microseconds (PostgreSQL stores 294247–294276 AD). Q-M5-1 and
  Type_Mapping §Date/time edge values say those clamp to the Arrow maximum, so the code now
  computes in `i128` and clamps. The unit test expects `i64::MAX` for `294276-12-31
  23:59:59.999999`.
- **JSON numbers keep their digits** (`fedfc22`, owner decision Q-M5-11, 2026-10-01). Without
  serde_json's `arbitrary_precision`, every JSON number went through `f64`. A 23-digit integer was
  rounded, `0.10000000000000000000001` became `0.1`, and a 401-digit number halted the table,
  though `jsonb` numbers are `numeric`. The workspace now builds `serde_json` with
  `arbitrary_precision`. Measured before deciding: the whole workspace suite, the PostgreSQL type
  round trip, and the Delta type round trip with the DuckDB cross-read all pass with the feature.
  The new test is `json_numbers_keep_their_digits`, so Task 4's unit-test count is 35, and each
  later task's count is one higher than its block shows.

**Task 5: two fixes to the planned code** (`ce1cb9c`; found by the task review, owner decision
2026-10-01):

- **`int2vector` and `oidvector` are read as text.** Their `pg_type` category is `A`, so the plan
  treated them as arrays. Their text has no braces, though (`1 2 3`, measured on PostgreSQL 18), so
  every value failed and the table halted with `SCHEMA_MISMATCH`. OIDs 22 and 30 are now in
  `TEXT_OIDS`, and `built_in_scalars` checks them. `TEXT_OIDS` now has one comment line per group
  (rustfmt had moved each comment to the end of the previous group).
- **A `list<decimal(P,S)>` override rescues a `numeric[]` column** (D-M5.10 extended). The planned
  decimal override looked only at the column's base type, so an array of `numeric` or `money` got
  `SCHEMA_MISMATCH`. That left the V-079 remedy unusable for a bare or over-wide `numeric[]`.
  `decimal_override` now follows an array to its element, through domains, and decodes each
  element as the decimal named. The new test is `a_decimal_list_override_rescues_a_numeric_array`,
  so Task 5's unit-test count is 42, and each later task's count is two higher than its block
  shows.

**Task 5b added: lossless override conversions** (owner decision Q-M5-12, 2026-10-01). The Task 5
review found that an override had to equal the column's mapped type, which config §9.3's
`timestamp_ntz_as` row contradicts. Task 5b (before Task 6) adds the conversions; Task 12 gains
`overrides_convert_through_stream_and_snapshot`. Task 5's `a_decimal_list_override_rescues_a_numeric_array`
changes its negative case from `int4[]` (now a lossless conversion to `list<decimal(38,10)>`) to
`bool[]`. Measured before approval, on the verified workspace: PostgreSQL 14–18 pass 31 integration
tests each without TLS, and the workspace passes 632 tests. Task 5b replayed on a fresh clone of the
branch at `88b7c58`. Counts after it: `data-avalanche-source-pg` unit tests 43, so each later task's
count is three higher than its block shows. Integration tests: Task 12 31, Task 13 35 (31 on each of
PostgreSQL 14–17).

**Task 7: unlogged tables, and two small fixes** (`b25934f`; found by the task review, owner
decision Q-M5-13, 2026-10-01). `catalog::relations` listed every `relkind` `r`/`p` table, including
`UNLOGGED` ones, which PostgreSQL can't publish. Measured: PostgreSQL 18 refuses `CREATE PUBLICATION
… FOR TABLE` on one with SQLSTATE 22023 ("not supported for unlogged tables"), and 14 with "Temporary
and unlogged relations cannot be replicated". Under a managed publication Task 8 would have refused
the whole pipeline. Under `FOR ALL TABLES` or `require_existing` the table would have been snapshotted
once and then never changed. Now:
- `Relation.logged` (`relpersistence = 'p'`). `select` skips a glob-matched unlogged table with a
  warning and halts a named one: `Selection.halted`, `HaltReason::NotReplicable`
  (`TABLE_NOT_REPLICABLE`, new in core). Config §11.2 has the row (`4db7c26`).
- `resolve_tables` counts halted tables as resolved for V-106. A halted table's `primary_key` entry
  used to warn "not in the pipeline's resolved table set".
- The authentication test checks `SQLSTATE 28`, not just `28`.

Tests: `unlogged_tables_are_skipped_or_halted` (unit); `channel_a_resolves_checks_and_skips` gains
`cache` (glob, skipped) and `cache_named` (named, halted), a `primary_key` entry on the halted
`nothing`, and asserts no V-106 warning. Measured on the verified workspace: PostgreSQL 14–18 pass
31 integration tests each without TLS; the workspace passes 633. Task 11's `connector.rs` diff is
regenerated against the fixed file (it applied at an offset of 7 lines). Counts: each later task's
`data-avalanche-source-pg` unit-test count is four higher than its block shows.

**Task 8: the publication repair, the publication's name, the WAL measure** (`83187ec`; found by the
task review, owner decision Q-M5-14, 2026-10-01). Measured on PostgreSQL 18 and 14 before the fix:
- **A repaired publication doesn't cover the WAL a resume replays.** pgoutput reads the publication
  as of each change's position. A change made before the publication existed: PostgreSQL 18 skips
  it with `WARNING: skipped loading publication`; 14 fails with `publication … does not exist`. So
  a `managed` repair that rewrites the publication (created; operations restored;
  `publish_via_partition_root` cleared) turns `Resume` into `Recreate(PositionLost)`, with a
  warning. Tables the repair adds return in the new `Prepared::added` (core), for M6 to snapshot.
  `reconcile_publication` returns `Reconciled { rewritten, added, warnings }`.
- **A mixed-case publication name streamed nothing.** `"MyPub"` was created quoted, but pgoutput got
  `publication_names 'MyPub'`, folded it to `mypub`, and streamed nothing on 18 (an error on 14).
  `Connector::replication` (Task 2's file) now passes the quoted name; its unit test checks
  `"\"MyPub\""`, and Task 9's `a_mixed_case_publication_streams` checks the stream.
- `retained_bytes` measures from `restart_lsn`, the WAL the slot keeps on disk, which trails
  `confirmed_flush_lsn` while an older transaction is open.
- Advancing an adopted orphan is bounded by `timeouts.snapshot`: it decodes the orphan's backlog.
- V-059 (config crate): a publication shared within one source database is an error when a sharing
  pipeline is `managed` (`ErrorAndWarning`); config §13 and Data-Flow Phase 5 were changed with
  the owner's approval.

Tests: `a_repaired_publication_re_snapshots` and `a_slot_of_another_plugin_is_refused` (the
plan's prose promised the plugin cell); `the_publication_follows_the_table_set` checks
`Prepared::added`; the new test binary `slot_invalidation` invalidates a slot through
`max_slot_wal_keep_size` and checks the recreate. It is its own binary because the setting is
server-wide: cargo never runs it beside the `pg` tests, and the nightly runs it too. Measured on
the verified workspace: PostgreSQL 14–18 pass 34 integration tests each without TLS, and
`slot_invalidation` on each; the workspace passes 637. Counts: Task 8's integration tests are 13;
each later integration count is three higher than before (two slot tests, one stream test).

Fix round 2 (`c4be91f`, from the re-review). `prepare` reads and decides before it changes
anything: `reconcile_publication` only returns the statements (`Reconciled::sql`), and
`apply_publication` runs them after the slot decision. A refusal leaves the publication as it was.
A rewriting repair drops the slot first, then changes the publication, then creates the slot, so a
`Transient` failure and a retry find no slot and re-snapshot, instead of resuming over the WAL the
old publication didn't publish. `publish_via_partition_root` counts as rewriting only while a
tracked table is a partition. V-059's uniqueness covers names that need quoting too. The
invalidation test restores `max_slot_wal_keep_size` on every failure path, and resets it first.
`Prepared::added` can't be made durable by the source: a retry after the `ADD TABLE` committed
reports nothing, so M6 records those snapshot requests as soon as `prepare` returns (roadmap M6).
Measured: PostgreSQL 14–18 pass 34 integration tests each and `slot_invalidation`; the workspace
passes 637 on the verified workspace, 602 on the branch.

**Task 9: cancel safety, the replica identity, renamed tables, the slot on close** (`1c117be`;
found by the task review, owner decision Q-M5-15, 2026-10-02):
- **`next` wasn't cancel-safe.** It took an event from the client, then awaited two catalog
  queries for a Relation message. Data-Flow Phase 2a runs `next` in `tokio::select!`; a call dropped
  there lost the message. The next change then failed (`a change of relation N before its Relation
  message`, permanent), or, after a same-width `RENAME COLUMN`, decoded under the old name with no
  signal. Now the event stays in the stream until handled; handling a Relation message again is
  idempotent. The queries run under `timeouts.command`. Core documents `ChangeStream::next` as
  cancel-safe (Task 1's file; the M7 stream owes the same).
- **Channel B missed key changes.** It compared names and types only. After `DROP CONSTRAINT
  t_pkey, ADD PRIMARY KEY (code)` no signal went out, and later `K` images carried `code` only.
  Now the replica identity (setting, key columns) is compared too: a table's first Relation message
  of a session against its opening identifier (all columns under `FULL`), later ones against the
  last. A difference signals; channel C finds `IdentifierChanged` and halts. Another index as the
  identity signals once per session, and channel C finds nothing.
- **Tables were matched by name.** A table renamed away dropped its changes until channel D
  noticed; a table swapped in under the name (pg-osc) streamed as the old one, though §7 says a
  re-created table doesn't resume. Now, within a session, a tracked table's name on another OID, or
  its OID under another name, halts it (`TABLE_DROPPED`); its later changes are skipped under
  either OID. `Schema_Change…` §7 names channel B for it (spec edit, owner-approved).
- PostgreSQL 14 sends a `"char"` byte above 127 as the raw byte (measured: `c3`), which isn't
  UTF-8; it decodes as `\ooo` now. A `"char"[]` with such a byte still halts.
- **`close` returned before the server released the slot.** The walsender lets the slot go after
  the client has left; tests that dropped the slot `AND NOT active` right after `close` left it
  behind, and a reopen right after `close` could meet `55006`. `close` now waits, up to
  `timeouts.command`, until `pg_replication_slots.active` is false. The test `cleanup` waits too
  (terminating a walsender still active after 5 s) and drops the slot unconditionally.
- Tests: `key_and_identity_changes_signal` (with the raw `"char"`), `a_renamed_or_replaced_table_halts`,
  `next_is_cancel_safe` (calls dropped at growing timeouts; without the fix it fails with the
  protocol error above). `s_pg5` resets `wal_sender_timeout` when it fails (the stalled half runs as
  its own task). A misplaced doc comment moved to `relation_changes_signal_before_the_next_change`.

Measured on the verified workspace: PostgreSQL 14–18 pass 37 integration tests each without TLS
and `slot_invalidation` on each, with no test slot left behind; the workspace passes 641. Counts:
Task 9's integration tests are 28, and each later integration count is three higher than before;
each later task's `data-avalanche-source-pg` unit-test count is five higher than its block shows
(Task 7's fix four, this one). Left for the whole-branch review: the client's read-ahead is bounded
in events (8 192), not bytes; a `next` cancelled while the client joins its ended worker reports
`replication worker already joined` (`Internal`, permanent) instead of the worker's error.

**Task 10: the key's collation, `"char"` on PostgreSQL 14, the read timeout** (`b271469`; found by
the task review, owner decision Q-M5-16, 2026-10-02):
- **Every chunk was a full scan for most text keys.** Snapshot_Resumability §2 pinned keyset order
  to `COLLATE "C"`; a primary key in the database's collation has no index in `"C"`, so each chunk
  scanned and sorted the table (the reviewer measured 17.8 ms against 0.28 ms on 200 000 rows; a
  large table's chunk could pass `timeouts.command` and retry for ever). §2's requirement is one
  deterministic collation for the order and the cursor. Now a text key orders in its own collation
  when that is deterministic, and its index serves the chunk. A key in a nondeterministic collation
  orders in `"C"`, and `resolve_tables` warns, naming the index that avoids the scan (`Notes`
  gains `c_ordered_keys`, from the catalog's `pg_collation`; Task 6's and Task 7's files). The
  cursor records the collations, one per text key column; a cursor of others halts the table
  (`SCHEMA_MISMATCH`, re-snapshot). Snapshot_Resumability §2 says so now.
- **PostgreSQL 14 prints a `"char"` byte above 127 raw.** It isn't UTF-8, and
  `SimpleQueryRow::get` panicked. The snapshot reads a `"char"` as its code
  (`((src.c)::"char"::int4 + 256) % 256`) and decodes it as `\ooo`; any value that isn't UTF-8
  (a `"char"[]` element) halts the table, as in the stream. The test's `'\377'::"char"` is a
  backslash on 14, so the old test never had a byte above 127 there; it inserts
  `(-1)::int4::"char"` now.
- **A domain over `"char"` as a key looped:** the cursor cast went by the type text. It goes by the
  decoder now (`Decode::reads_char`, moved from the stream to `value.rs`).
- **A timed-out chunk kept the shared catalog connection busy:** the client stopped waiting, the
  server didn't stop. Each read now runs as `SET LOCAL statement_timeout = <timeouts.command>;
  SELECT …` (one simple query, so the setting ends with it; measured on PostgreSQL 14 and 18), and
  the client waits 5 s longer.
- Tests: `text_keys_page_in_one_deterministic_collation` (a mixed-case table in `en_US.utf8`, an ICU
  nondeterministic key, a stale cursor), `a_domain_over_char_key_resumes`,
  `a_timed_out_chunk_frees_the_connection` (a locked table; the next catalog call answers while the
  lock is held); the composite-key test counts the byte-255 rows; the drift test asserts the
  type-change `Transient`; `read_all` fails a cursor that doesn't advance. With the original
  `snapshot.rs`, the four new checks fail on PostgreSQL 14. Unit: `keys_in_a_nondeterministic_collation_are_noted`.

Measured on the verified workspace: PostgreSQL 14–18 pass 39 integration tests each without TLS
and `slot_invalidation` on each; the workspace passes 644. Task 11's `connector.rs` diff is
regenerated against the fixed file. Counts: Task 10's integration tests are 35, each later
integration count is two higher than before; each later task's unit-test count is six higher than
its block shows. Left for the whole-branch review: `bit`/`varbit` keys (their key text isn't
valid input for the type); a key column that loses `NOT NULL` isn't re-checked; `E''` literals
would make quoting independent of `standard_conforming_strings`; no byte budget per chunk;
`read_chunk` with `limit` 0 reads as the table's end.

**Task 11: the catalog ahead of the stream, unlogged tables, replica identity** (`43e26ef`; found by
the task review, owner decision Q-M5-17, 2026-10-02):
- **A rename after the signal gave a silent wrong delta.** `scope` found each stream column by its
  current catalog name first. The reviewer measured on PostgreSQL 18: `a → x; b → a` after the
  signal gave `[DropColumn a, RenameColumn b → a, …]`, all Safe, with two columns on one `attnum`;
  a swap gave two Safe renames applied at the signal, never corrected. Now the placement is
  checked: two columns on one `attnum`, `attnum`s out of the stream's order (pgoutput sends
  `attnum` order), or a name on another `attnum` while its own still exists and no stream column
  holds it, halt the table (`SCHEMA_MISMATCH`, re-snapshot). Without the catalog ahead the lookup is
  exact and nothing fires.
- **An add/backfill/drop migration halted the table** when the drop was already in the catalog: a
  column dropped since the signal now keeps its cached `attnum`, which PostgreSQL never reuses. Two
  renames of one column across the signal (`a → b`, signal, `b → c`) still halt.
- **A table set `UNLOGGED` went unnoticed.** PostgreSQL refuses it for a table in a `FOR TABLE`
  publication but not under `FOR ALL TABLES`, and the table stops publishing. `Described` carries
  `logged` (`relpersistence`), and `table_schema` refuses an unlogged table
  (`TABLE_NOT_REPLICABLE`), so `rescan` reports it halted and `augment` halts it (Task 6's file).
- `narrowed`: a time type without a precision has the default, 6 (`timestamp` → `timestamp(6)`
  halted as a narrowing).
- **`Schema_Change…` §7.2 amended** (owner decision, option a): a change of the identifier (the
  primary key, or the `tables.primary_key` columns), or a replica identity that no longer covers it,
  halts; another replica identity or unique-key change is ignorable. That is what the code does;
  core's `IdentifierChanged` doc says so (Task 1's file).
- Tests: `channel_c_with_the_catalog_ahead` (a later drop kept, a rename chain halts),
  `channel_c_classifies_identity_changes` (a primary key moved → `IdentifierChanged`, Unsafe;
  `FULL` over the same key → nothing); `channel_d_finds_dropped_changed_and_broken_tables` gains an
  unlogged table; unit tests for the three guesses, a rename chain before the signal (placed), the
  later drop, the default precision and an unlogged table. With the original code, the unlogged and
  later-drop checks fail on PostgreSQL 18.

Measured on the verified workspace: PostgreSQL 14–18 pass 41 integration tests each without TLS
and `slot_invalidation` on each; the workspace passes 649. Counts: Task 11's integration tests are
39, each later integration count is two higher than before; the `data-avalanche-source-pg` unit
count after Task 11 is 75, nine higher than its block shows. Left for the whole-branch review:
narrowing an interval's fields (`interval` → `interval year`) isn't seen; the table schema carries
no OID, so a table replaced while the pipeline is down resumes silently.

**Task 11, round 2: a replaced column** (`cc797c9`; found by the re-review of round 1, within
Q-M5-17). Round 1's "dropped since the signal" rule placed a column at its cached `attnum` whenever
the catalog no longer had it, but "absent now" only proves it was dropped after the cached schema
was read. `DROP a; RENAME b TO a` before the signal, then `RENAME a TO x` (or `DROP a`) after it:
the stream's `a` landed on the old `a`'s `attnum`, and the delta read as a silent, Safe `[DropColumn
b]` (measured on PostgreSQL 18). Two guards replace round 1's name check, which the first one
covers: a column of the cached schema the catalog still has must have a stream column on it, since
it existed at the signal; and a column placed as dropped since the signal halts when another cached
column is gone too and placed nowhere. The add/backfill/drop migration still places. `bound`
matches the time types by their first word (`timeslot` isn't one). Tests:
`channel_c_with_the_catalog_ahead` gains the replaced column (table `v`); unit asserts for both
variants and the type name. Counts unchanged. Measured on the verified workspace: PostgreSQL 14–18
pass 41 integration tests each and `slot_invalidation`; the workspace passes 649. Left for the
whole-branch review: a column dropped and re-added under one name after the signal reads as
`DropColumn` + `AddColumn` (the catalog can't tell it from the same change before the signal);
a glob-matched table set `UNLOGGED` between channel A's list and its describe halts instead of
being skipped.

**Task 11, round 3: a name re-added since the signal** (`cd586bd`; found by the re-review of round
2, within Q-M5-17; present since round 1). A column added at the signal, renamed after it, and its
name re-added: the stream's column landed on the new column, `[AddColumn a]` on the wrong `attnum`,
and the next signal read the rename as an add (measured on PostgreSQL 18). A catalog column below
the highest placed column the catalog still has was created before it, so it existed at the signal:
with no stream column on it, the table halts. And a column dropped and re-added before the signal,
then renamed after it, read as no change; those inputs equal a drop and an add after the signal, so
a placement as dropped since the signal halts when a catalog column has no stream column on it
(that halts the legitimate drop-and-add after the signal too: fail-safe). Tests:
`channel_c_with_the_catalog_ahead` gains table `w`; unit test
`scope_halts_on_a_name_re_added_since_the_signal`. The `data-avalanche-source-pg` unit count after
Task 11 is 76. Measured on the verified workspace: PostgreSQL 14–18 pass 41 integration tests each
and `slot_invalidation`; the workspace passes 650. Known limits, which the catalog can't tell from
legitimate changes (the same changes made before the signal give the same inputs, and halting
them would halt those too): a column dropped after the signal whose name a later column then takes,
by `ADD` or by `RENAME` of a later column, places the stream's columns on the wrong `attnum`s (for
example `DROP a; RENAME b TO a; ADD b` after a signal that added `b`); a column re-added before the
signal and dropped after it reads as a drop after the signal. The re-review argued no other
placement can be silently wrong. Left for the whole-branch review: a `require_existing` publication with a
column list (PostgreSQL 15+) or PostgreSQL 18's `publish_generated_columns` makes channel C halt the
table on every signal (fail-safe); reconcile could warn or refuse.

**Task 12: a clamped key halts the table, and the round trip's gaps** (`412ef25`; found by the task
review, owner decision Q-M5-18, 2026-10-02):
- **Distinct keys merged.** Q-M5-1 clamps a timestamp past the `i64` microseconds (PostgreSQL
  stores up to 294276 AD) to `i64::MAX`, which is also `'infinity'`. In a key column, two rows
  became one identifier, and a snapshot cursor at the clamp read as `'infinity'` and ended the
  scan (the reviewer measured 2 rows of 4 on PostgreSQL 18). An identifier column whose value
  clamps now halts the table, `SCHEMA_MISMATCH`, in the stream (`StreamColumn.identifier`, Task 9)
  and in the snapshot (`ReadColumn.identifier`, Task 10); `value::clamped` tells a clamp from
  `±infinity` by the text (Task 4). Other columns still clamp. Unit test
  `a_clamped_identifier_halts_the_table`; the `data-avalanche-source-pg` unit count after Task 11
  is 77.
- **The round trip's gaps.** `every_mapped_type_round_trips_through_stream_and_snapshot` gains
  38-digit numerics (`numeric(38,0)` at ±(10³⁸−1), `numeric(38,38)`, `numeric(40,2)` as text), the
  last date and `-infinity`, `timestamp` `infinity` and a non-key clamp, `timestamptz`
  `-infinity`, `money`'s maximum, float `NaN` (compared NaN-aware), a `"char"` byte above 127 and
  the empty `"char"`, unicode and escapes in `text[]` and `hstore`, an array with explicit
  bounds, and a row length check on the snapshot. New tests:
  `string_and_decimal_overrides_through_stream_and_snapshot` (a `decimal(P,S)` value keeps its own
  scale; the declared type bounds it, as in Task 5b) and `a_clamped_timestamp_key_halts_the_table`.
  Measured on the verified workspace: PostgreSQL 14–18 pass 43 integration tests each and
  `slot_invalidation`; the workspace passes 653.
- **A plan correction.** Task 10's fix moved `reads_char` from the stream to `Decode::reads_char`
  in `value.rs`, but the plan's Task 5b diff and Task 9 code didn't show the move; they do now.
  Task 4's code with Task 5b's diff now gives the final `value.rs` but for Task 4's own two fixes
  above (the timestamp clamp, `aba85ec`; JSON digits, `fedfc22`, with the workspace `Cargo.toml`
  feature), which stay recorded here only.

**Task 12, round 2: the Delta sink's keys; the plan's helpers** (`537e2f1`; found by the re-review
of round 1, owner decision Q-M5-19, 2026-10-02):
- **Keys merged in the sink too.** Hop 3b clamps every `date`, `timestamp` and `timestamp_ntz` cell
  to 0001-01-01 … 9999-12-31 23:59:59.999999 (Q-M4-2), identifier columns included: two BC keys,
  two keys from 10000 AD, or `infinity` and `9999-12-31 23:59:59.999999` became one key in the
  MERGE. `data-avalanche-sink-delta`'s `merge_source` now checks each identifier column
  (`convert::clamped_key`) before converting it, upserts and deletes alike; a cell outside the range
  is `Permanent`, and the table halts. On Delta, a `date` or `timestamp` key holding `±infinity`
  halts its table. A column that isn't a key still clamps. Tests (outside this plan, in the M4
  crate): `convert::a_key_the_delta_range_would_clamp_is_found`,
  `write::a_key_outside_the_delta_range_is_permanent`.
- **The helpers had no caller until Task 9** (M-1): as round 1 placed them, `clamped`,
  `CLAMPED_KEY` (Task 4) and `Decode::reads_char` (Task 5b) were `pub(crate)` and unused, and
  Tasks 4–8 failed the `-D warnings` gate. Task 9 adds them now, as a diff of `value.rs`; Tasks 4
  and 5b are as before round 1.
- **Wording and tests** (M-2 … M-4): the last microsecond itself, `294247-01-10 04:00:54.775807`,
  equals `infinity` and halts; the halt says "at or past" (unit assertion). The overrides test
  asserts each declared type and that every value fits it. The clamped-key test adds a
  `timestamptz` key at the last microsecond written with an offset, a composite key, a `date` key
  converted to `timestamp_ntz`, a delete's old key, and the halt's detail.
  Measured on the verified workspace: PostgreSQL 14–18 pass 43 integration tests each and
  `slot_invalidation`; the workspace passes 655.
