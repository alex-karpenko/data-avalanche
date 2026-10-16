---
updated: 2026-10-14
domain: Data Flow
tags: [v1, implementation-plan, m7a, mysql, mariadb, binlog, gtid, file-position, snapshot, type-mapping, ddl-classifier, tls]
resolves: []
sources: []
---

# M7a — MySQL and MariaDB Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `data-avalanche-source-mysql`, the MySQL and MariaDB source, against the source
contract of M5. The crate turns the row-based binlog into `StreamItem`s in all three position
dialects (a MySQL GTID set, a MariaDB GTID state, a binlog file and offset). It decodes every type
of Type_Mapping's MySQL and MariaDB tables to `ColumnValue` (Hop 1), from the binlog and from a
chunk through one decoder. It classifies DDL statements, reads a statement's change from the
catalog (channels A, C, D), reads keyset snapshot chunks with a locking read (DA-SNAP-10), and
decides where a start resumes. Its integration tests run on MySQL 8.4 (GTIDs on and off) and
MariaDB 11.8 (GTID and file/position) on every change, and MariaDB 11.4 and 10.11 weekly.

M7 is split (Q-M7a-1): **M7a** is this crate and its tests, driven directly as M5 drove the
PostgreSQL source. **M7b** wires it into the worker (the checkpoint and its server, the binary)
and runs the slice-1 E2E parity suite in the three position modes and the payload
fault-injection E2E. M7a changes the worker only mechanically (Task 1).

**Architecture:** One change each in core and config, one new crate.

- **`data-avalanche-core`**: a `Checkpoint` carries its source server (`ServerIdentity`, moved
  here from the state crate), and `Prepared` reports it (Task 1).
- **`data-avalanche-config`**: `source.mysql.server_id` (V-120) and `source.mysql.heartbeat`
  (V-121) (Task 2); the glob and partition-spec checks shared by both sources (Task 9).
- **`data-avalanche-source-mysql`** (new), one module per concern:
  - `error`: server codes and client errors → `SourceError`.
  - `connect`: the catalog connection (`mysql_async`), its session settings and TLS.
  - `server`: the flavour (MySQL or MariaDB), supported versions, the settings and grants
    preflight reads.
  - `position`: the three dialects (`Mode`), the write position, the server's identity, the
    binlogs it holds, the purge check.
  - `binlog`: order offsets and the payload check, MariaDB `GTID_EVENT`, MariaDB compressed
    events, rotate names.
  - `types`, `value`, `json`: Hop 1 (type plans, one decoder for both inputs, partial JSON).
  - `catalog`: channel A (selection, `information_schema`, schemas and plans).
  - `snapshot`: locking keyset chunks in the key's own collation.
  - `ddl`: the DDL classifier (tokenizer and `sqlparser`).
  - `schema`: channels C (`augment`) and D (`rescan`).
  - `stream`: `MySqlStream`, the binlog dump → `StreamItem`.
  - `start`: the start position (`prepare`), the purge action, the dump watch (`retained`).
  - `connector`: `MySqlConnector`, the preflight and the `SourceConnector` impl.
- **Tests:**
  - Unit tests in each module run without a server, many of them on bytes captured from the
    servers.
  - The integration binary `tests/mysql` runs against the target `DATA_AVALANCHE_MYSQL`
    (`DATA_AVALANCHE_IT`): `mysql-84`, `mysql-84-nogtid`, `mariadb-118`, `mariadb-118-binlog`,
    and weekly `mariadb-114`, `mariadb-1011`. The TLS tests use `mysql-tls`.
  - Two binaries change server-wide state and run alone: `tests/binlog_purge.rs`,
    `tests/mariadb_compress.rs`.

**Tech Stack:** Rust 1.98.1 (edition 2024).

| Crate | Version / features | Used for |
|---|---|---|
| `mysql_async` | `=0.37.1`, `default-features = false`, features `default-rustls`, `binlog` | The catalog connection, chunk reads and the binlog dump (the S-MY probes' client) |
| `mysql_common` | `=0.37.3`, `default-features = false` (dev) | Building binlog events in unit tests (the version `mysql_async` 0.37.1 uses) |
| `sqlparser` | `=0.62.0`, `default-features = false`, features `std` | Tier 1 of the DDL classifier (`MySqlDialect`), ALTER operations |
| `flate2` | `1` (1.1.10, already in the lockfile) | Inflating MariaDB compressed events |
| `rustls` | `0.23` (aws-lc-rs, the workspace's) | TLS errors in the error chain |
| `async-trait`, `chrono`, `futures-util`, `serde_json`, `thiserror`, `tokio` (`sync`, `time`), `uuid` | workspace | The trait, dates, the event stream, JSON values, errors, the shared state, UUIDs |
| `data-avalanche-testkit`, `rcgen`, `tokio` (`macros`, `rt-multi-thread`) | dev | Test gates, throwaway certificates in unit tests |

**Spec:**
- `doc/v1.0/00-roadmap.md`: §2 (layout, dependency direction), §2.1 (pins), §3.6 (the source
  connector), §4 M7 scope and exit, §5.2–§5.4 (test infra, CI), D-IMPL-2, D-IMPL-5, §9.1 Q-M7-1,
  Q-M7-2, and the S-C row of §7.
- `doc/specs/Data-Flow.md`: Phase 1 (resolution, the MySQL snapshot window rule and locking chunk
  reads), Phase 2b (the MySQL stream: flavours, the three dialects, the heartbeat, the payload
  check, file/position mode and `BINLOG_SOURCE_CHANGED`), §Error Paths, the lineage table (`_lsn`,
  `_seq`).
- `doc/specs/Configuration_Specification.md`: §9.1 `source` (`ssl_*`, `timeouts`), §9.2.2
  `source.mysql` (`replication_type`, `tinyint1_as_bool`, `tinyint1_detection`,
  `binlog_purge_action`, `server_id`, `heartbeat`), §9.3 `tables` (bare-name globs V-064,
  identifier and type overrides, `numeric_overflow_action`), §11.2 (the MySQL preflight rows),
  V-030, V-079, V-105, V-120, V-121.
- `doc/specs/Schema_Change_Detection_and_Evolution.md`: §0, §2.2 (channel B on MySQL), §3, §6
  (rename proof by keyword), §7.2 (ENUM/SET, DROP TABLE, unclassifiable DDL).
- `doc/research/Type_Mapping.md`: the MySQL table (Hop 1), §Date/time edge values.
- `doc/research/MySQL_DDL_Classifier.md`: the two tiers and the routing.
- `doc/research/Snapshot_Resumability_and_Consistency.md`: §1.1 (DA-SNAP-10, the locking read),
  §2, §6.
- `doc/research/Delivery_Guarantee_and_Position_Guard.md` §2 (positions);
  `doc/research/V1_Scope_Decisions.md` D-V1.5 (MariaDB in v1), D-V1.6 (the payload check).
- `prototypes/source-capability/README.md`: S-MY0…S-MY11b, the evidence this crate re-runs.

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. `cargo clippy --workspace
  --all-targets -- -D warnings` and `cargo fmt --all --check` must pass before every commit.
- **Dependency direction** (roadmap §2): `source-mysql → core, config`. `testkit` is a
  **dev**-dependency only. Nothing depends on `source-mysql` in M7a (M7b wires it).
- **Pins:** `mysql_async = "=0.37.1"` and `mysql_common = "=0.37.3"` (the probed client),
  `sqlparser = "=0.62.0"`. One rustls `CryptoProvider` (aws-lc-rs) is installed at process start
  (DA-DEP-5); the tests install it through `data_avalanche_testkit::install_crypto_provider`.
- **Servers:** MySQL 8.4 and later, MariaDB 10.11 and later (P-8), the binlog on,
  `binlog_format = ROW`, `binlog_row_image = FULL`.
- **Positions** (D-IMPL-4): `mysql-gtid:<set>`, `mariadb-gtid:<state>`, `binlog:<file>:<pos>`.
  Pipeline-scope halts are `SourceError::Permanent` whose message starts with the code and a
  colon (`BINLOG_PAYLOAD_INVALID: …`, `BINLOG_SOURCE_CHANGED: …`, D-M7a.8).
- **Test gates:** plain `cargo test` stays hermetic. Integration tests gate on
  `DATA_AVALANCHE_IT` and run against `DATA_AVALANCHE_MYSQL` (default `mysql-84`, port 53384).
  The TLS tests use `mysql-tls` (port 53386) with the `mysql-84` target only.
- **Credentials:** the tests use the compose services' fixed test credentials only. Certificates
  and keys are made at run time (by `rcgen` in unit tests, by the container for `mysql-tls`),
  never committed. No key, key path or secret goes in code, docs or output; never print the
  files under `test-infra/.tls/`.
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `SourceError` (core), classified `Transient`, `Permanent`, `PositionUnreachable`, `TableHalt`
  or `Contended`.
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to break a mirror. Each is pinned by a test in the task after
the arrow.

1. **The catalog ahead of the stream.** An `ALTER TABLE`, then more DDL, lands before the stream
   reaches the first one; or an online migration swaps a shadow table in. The schema the stream
   decodes by must be the one at its own position, and a table it can no longer read must halt,
   never decode wrongly → Task 12 (`a_statement_s_change_is_scoped_to_its_columns`,
   `what_the_catalog_cant_place_halts`), Task 13
   (`a_table_map_or_row_image_the_schema_doesnt_read_halts_the_table`,
   `a_cut_over_swap_is_followed`, `statements_route_to_signals_truncates_and_halts`).
2. **A start where the position no longer means what it did.** The server purged the
   checkpoint's binlog (before `prepare`, or between `prepare` and the dump), the server is
   another one (failover in file mode), or `replication_type` changed. None may resume from the
   wrong place → Task 14 (`a_purged_checkpoint_follows_the_purge_action`,
   `a_checkpoint_from_another_server`, `a_checkpoint_in_another_dialect_follows_the_purge_action`),
   Task 5 (`a_live_position_is_reachable_and_a_file_never_written_is_purged`).
3. **Events the client reads as nothing.** A MySQL compressed transaction whose events didn't
   all come out, a MariaDB compressed event, a partial JSON update, a two-phase XA, an unknown
   event type. Each must decode or halt, never be skipped → Task 6
   (`a_payload_whose_events_went_missing_is_invalid`, `mariadb_compressed_rows_read_as_their_plain_event`),
   Task 13 (`mysql_payloads_partial_json_and_tagged_gtids`,
   `a_two_phase_xa_of_tracked_rows_halts_the_stream`), Task 14 (`compressed_events_decode`).
4. **Keys that sort differently from the server's order.** Text in a case- or accent-insensitive
   collation, `latin1` bytes, decimals, `BINARY`, `BIT`, `ENUM`, dates, composite keys. A chunk
   cursor must resume exactly, with no row skipped or read twice → Task 10
   (`every_key_type_walks_in_the_servers_order`, `a_table_changed_under_the_snapshot_halts_or_waits`).
5. **Values at the edges of what the binlog carries.** Signed `MEDIUMINT` minimum, `YEAR` 0000,
   zero and impossible dates, stripped `BINARY` zeros, `latin1`'s cp1252 bytes, MariaDB IPv6 text,
   `TIME` past 24 hours. Each decodes to the agreed value, the same from the binlog and a chunk,
   or halts only its table → Task 7 (`integers_read_with_the_catalogs_signedness`,
   `all_zero_dates_are_null_and_invalid_dates_halt`, `latin1_bytes_decode_as_the_server_converts_them`),
   Task 15 (`every_type_reads_the_same_from_the_stream_and_a_chunk`).

## Decisions and rulings

**Asked and answered:**

| # | Question | Answer |
|---|---|---|
| Q-M7-1 (2026-09-26) | MySQL/MariaDB zero dates under a permissive `sql_mode` | **All-zero date → NULL** (a halt in a key column); **zero-in-part and impossible dates → table halt `INVALID_DATE_VALUE`**, remedy a `string` override (the server's text) |
| Q-M7-2 (2026-09-26) | File/position mode under a source failover | **Halt with a clear error**: pipeline halt `BINLOG_SOURCE_CHANGED` when `@@server_uuid` (MySQL) / `@@server_id` (MariaDB) differs from the recorded one; remedy `gtid` or a re-snapshot |
| Q-M7-3 (2026-10-14) | DA-SNAP-10: a chunk can miss a transaction the stream already delivered (semi-sync `AFTER_SYNC`, S-MY11) | **Locking chunk reads**: a short `READ COMMITTED` transaction with `LOCK IN SHARE MODE`; the window rule stays (Snapshot_Resumability §1.1, applied) |
| Q-M7-4 (2026-10-14) | `_seq` when the header's `log_pos` is 0 (S-MY10f), and `_lsn` | **`_seq` = (file index, computed end offset)**: the last set header position plus event sizes, a payload's inner events at the payload's end. **`_lsn` = the transaction's GTID**, or its start `file:pos` in file mode (Data-Flow, applied) |
| Q-M7-5 (2026-10-14) | Two dumps under one replica id end each other (S-MY10h); no config field | **`source.mysql.server_id`**, derived from the pipeline `id` by default; V-120 unique per source server (config, applied) |
| Q-M7-6 (2026-10-14) | The MySQL heartbeat period | **`source.mysql.heartbeat`** (`60s`): the server's heartbeat and the watchdog's pace; V-121 (config, applied) |
| Q-M7a-1 (2026-10-14) | One milestone or two? | **Split M7a / M7b**: the crate and its ITs (M7a); worker wiring, the parity E2E and fault injection (M7b) |
| Q-M7a-2 | Which charsets? | **The UTF-8 family (`utf8mb4`, `utf8mb3`, `ascii`) and `latin1` (cp1252)**; `binary` → bytes; any other charset halts the table at resolution, naming the column |
| Q-M7a-3 | `binlog_row_value_options = PARTIAL_JSON` | **Apply the diff** to the before image; the after image carries the whole document |
| Q-M7a-4 | MariaDB's own types | **`UUID` → uuid, `INET4`/`INET6` → string (MariaDB's text), `VECTOR` → binary**; Type_Mapping gets a MariaDB table |
| Q-M7a-5 | MariaDB `log_bin_compress` (`mysql_common` reads its events as nothing) | **Decompress them ourselves** |
| Q-M7a-6 | A checkpoint in another dialect than the pipeline reads (a `replication_type` change, another flavour) | **Follow `binlog_purge_action`**: `re_snapshot` → a new position and a snapshot, `error` → halt; the message names the dialect change |
| Q-M7a-7 | `GEOMETRY` | **Bytes as stored: SRID (4 bytes, little-endian) + WKB**; Type_Mapping's row says "MySQL internal form" |
| Q-M7a-8 | An online-migration cut-over (`RENAME TABLE t TO x, shadow TO t`) | **Follow the swap**: the shadow's columns diffed against `t`'s. Needs a §7.2 row (patch below, for approval) |
| Q-M7a-9 | Which swaps are cut-overs? | **Tool names only**: an untracked incoming table named gh-ost `_<t>_gho` or pt-online-schema-change `_<t>_new` (one or more leading `_`), same database. Any other swap halts `TABLE_DROPPED` |

**Rulings where the specs are silent** (each is pinned by a test). **Plan approved 2026-10-14**, with these rulings (roadmap D-IMPL-15) and the two spec patches below:

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M7a.1 | `prepare` takes `Option<&Checkpoint { position, server: Option<ServerIdentity> }>`; `Prepared` gains `server`. `ServerIdentity` moves from the state crate to core; the state crate re-exports it and its JSON is unchanged. The worker passes `Checkpoint::from(position)` (no server) in M7a; M7b records and passes the server | The source must check the server a file position came from (Q-M7-2), and the source crates depend on core only | Task 1 (`a_server_identity_names_its_engine_and_keeps_its_json`, `a_position_alone_is_a_checkpoint_without_a_server`) |
| D-M7a.2 | The derived `server_id`: the first four bytes of BLAKE3(`id`), little-endian, with the top bit set; V-120 counts set and derived ids per `host` + `port` | Stable across restarts, far from the small ids replicas usually take, and checkable at load | Task 2 (`the_derived_server_id_is_stable_and_in_the_high_range`) |
| D-M7a.3 | Errors: 1040, 1053, 1205, 1213, 1317, 1927, 3169, 4031 → `Transient`. 4052, and 1236 "same server_uuid/server_id" → `Contended`. 1236 "Could not find first log file name", "purged required binary logs", "Could not find GTID state" → `PositionUnreachable`. Any other server error → `Permanent`. I/O and a closed connection → `Transient`; TLS anywhere in the chain, auth and protocol → `Permanent` | Data-Flow §Error Paths. 1236 means three things, told apart only by its message (S-MY10e, h, k) | Task 4 (the five `error` tests) |
| D-M7a.4 | Every session runs `SET time_zone = '+00:00'` and `SET NAMES utf8mb4`. One shared catalog connection, each command under `timeouts.command`, dropped after a fatal error or a timeout | A chunk's `TIMESTAMP` is read in the session's zone, the binlog's in UTC; text arrives in the session's charset | Task 7 (`dates_times_and_timestamps`), Task 15 |
| D-M7a.5 | TLS: `prefer` falls back to plain when the server offers none; `require` doesn't check the chain; inline PEM in memory. **`verify-ca` checks the name too** (`mysql_async` 0.37.1 matches `NotValidForName`, rustls 0.23 prints `certificate not valid for name`): it fails closed, pinned | The limitation is the client's; failing closed is safe, and the test notices when an upgrade lifts it | Task 4 (`verify_ca_by_an_unnamed_address_is_refused_like_verify_full`, `prefer_falls_back_to_plain_and_require_is_refused`) |
| D-M7a.6 | Preflight: MySQL ≥ 8.4, MariaDB ≥ 10.11, the binlog on, `ROW`, `FULL` row image, GTIDs on for `gtid` on MySQL → else `Permanent`. `binlog_row_metadata` ≠ `FULL` warns. The status privilege by running the status statement; `REPLICATION SLAVE` by `SHOW GRANTS` (MySQL `USING CURRENT_ROLE()`) | Config §11.2's MySQL rows; a default role's grants count (measured on both) | Task 4 (`supported_servers_with_the_settings_pass`, `replication_granted_through_a_default_role_passes`) |
| D-M7a.7 | The purge check is a pure function of what the server reports (`SHOW BINARY LOGS`, `gtid_purged`, `BINLOG_GTID_POS(<oldest>, 4)`); the dump's 1236 is the fallback | The check runs before a dump can fail late, and it is testable without purging | Task 5 (the three `purged` tests) |
| D-M7a.8 | A pipeline-scope halt is `Permanent` with the code as the message's prefix (`BINLOG_PAYLOAD_INVALID: `, `BINLOG_SOURCE_CHANGED: `) | Core has no pipeline-halt variant; M7b's status reads the prefix | Task 6, Task 14 |
| D-M7a.9 | MariaDB compressed events: re-read under the plain type, inflate, rebuild; the v2 types (169–171), never seen, fail closed | Q-M7a-5; `mysql_common` drops them silently (measured) | Task 6 (`mariadb_compressed_rows_read_as_their_plain_event`), Task 14 (`compressed_events_decode`) |
| D-M7a.10 | One decoder for both inputs; signed `MEDIUMINT` sign-extended; `YEAR` `1900` → 0; `BINARY` and MariaDB fixed types padded back; a `string` override on a byte column halts; a float's text is Rust's shortest | The binlog and a chunk must agree (Task 15); `mysql_common`'s quirks measured on three servers | Task 7, Task 15 |
| D-M7a.11 | Channel A: bare names in `source.db`; system-versioned tables halt (named) or are skipped (glob); `describe` in chunks of 500; the glob and partition checks move to config | Config §9.3; one implementation for both sources | Task 9 (`mariadb_types_and_system_versioned_tables`, `globs_select_bare_names`) |
| D-M7a.12 | Chunk keys compare in their own collation, not `utf8mb4_bin` (the roadmap line is corrected); the predicate is the expanded keyset form; each type has its operand | A binary collation has no index (every chunk a full scan); a row constructor walks the index (`EXPLAIN`, measured) | Task 10 (`every_key_type_walks_in_the_servers_order`, `the_keyset_predicate_is_spelled_out`) |
| D-M7a.13 | A boolean key value other than 0 or 1 halts the table | Two keys would merge into one (Q-M5-18's rule) | Task 10 (`a_boolean_key_other_than_0_or_1_is_unrepresentable`) |
| D-M7a.14 | The classifier reads names and verbs by tokenizer, ALTER operations by `sqlparser`; an executable comment or a statement `sqlparser` can't read scopes the whole table; a statement on a table the tokenizer can't name halts the pipeline | Names must never depend on a grammar's coverage; the fail-safe of §7.2 | Task 11 (`what_sqlparser_cant_read_scopes_the_whole_table`, `a_table_statement_without_a_name_is_unreadable`) |
| D-M7a.15 | Partition and tablespace operations that change rows without row events halt the table (`UNSAFE_DDL`) | The target would diverge silently; §7.2 row proposed below | Task 11 (`partition_operations_that_move_rows_are_unlogged`), Task 13 |
| D-M7a.16 | The stream decodes by the schema as of its position (`source_type` carries the charset; `from_schema` plans it back); at a signal it reads the change itself and keeps it for `augment`; a `TABLE_MAP` or row image the schema doesn't read halts the table | `Schema_Change…` §0; one catalog read per statement; a catalog ahead of the stream halts, never misdecodes | Task 12, Task 13 |
| D-M7a.17 | Fail closed: `INCIDENT_EVENT` and unknown event types halt the pipeline. XA (measured): a two-phase `XA PREPARE` with tracked rows halts; one-phase commits; untracked XA advances | Rows of a prepared XA can still roll back; halting any XA stopped every unrelated stream (first design, measured) | Task 13 (`a_two_phase_xa_of_tracked_rows_halts_the_stream`) |
| D-M7a.18 | Positions: a change's `source_pos` is its transaction's own GTID, or its start `file:pos`; a commit's is the cumulative set, the MariaDB state, or the XID's end | Q-M7-4; the accumulator's frontier must cover each commit | Task 13 (`changes_arrive_as_transactions_and_resume_after_a_commit`) |
| D-M7a.19 | `Begin.xid` for MySQL is a per-session counter (anonymous transactions carry no id); MariaDB's is the GTID sequence | The reconciler logs by xid; it needs only session uniqueness | Task 13 |
| D-M7a.20 | The purge action is one rule (`purge_action`) applied at `prepare` and to the dump's 1236 at open and mid-stream | Otherwise a purge between `prepare` and the dump re-snapshots under `error` | Task 14 (`the_purge_action_decides`, `a_purged_checkpoint_follows_the_purge_action`) |
| D-M7a.21 | `retained`: `bytes` 0; `streamed` = the stream's connection id in `PROCESSLIST` with a `Binlog Dump` command (V-121 amended below) | The server keeps no log for the pipeline; the connection id is visible without `PROCESS` (measured) | Task 14 (`the_watch_finds_the_dump_by_its_connection`) |
| D-M7a.22 | `fetch_rows` → `Permanent`; `teardown` → nothing; `reset_position` and `current_position` → the write position | `re_query` is PostgreSQL's; a MySQL source creates nothing on the server | Task 14 |
| D-M7a.23 | Tests that change server-wide state run in their own binaries (`binlog_purge`, `mariadb_compress`) | cargo runs test binaries one at a time (the PostgreSQL crate's `slot_invalidation.rs`) | Task 14 |

**Measured while planning** (recorded in the tasks and the docs):

- `mysql_common` 0.37.3 reads a signed `MEDIUMINT`'s minimum as positive, `YEAR` 0000 as `1900`,
  and MariaDB's compressed events (165–168) as nothing; `BINARY(n)` and MariaDB `UUID`, `INET4`,
  `INET6` arrive with trailing zeros stripped (MySQL 8.4, MariaDB 11.8 and 10.11).
- The header's `log_pos` is 0 inside MySQL compressed transactions and for MariaDB 11.x table maps
  and rows events; MariaDB 10.11 fills every position (S-MY10f).
- MariaDB's IPv6 text follows MySQL's `ipv6_to_str`, not RFC 5952.
- `(a, b) > (?, ?)` walks the whole index on MySQL 8.4 and MariaDB 10.11 / 11.8; the expanded
  predicate is a range scan; an explicit `COLLATE` of the column's own collation keeps it.
- XA: a two-phase transaction's rows are logged at `XA PREPARE` (event 38 on MySQL, GTID flags on
  MariaDB), its commit later as its own transaction; a one-phase commit is one transaction.
- MariaDB keeps the binlog it just closed until its binlog checkpoint moves on (`PURGE` right
  after `FLUSH` leaves it). `VEC_FromText` refuses components past about 1e16; the binary form
  refuses `FLT_MAX` and accepts denormals (11.8).
- A replication user without `PROCESS` sees its own `Binlog Dump` in
  `information_schema.PROCESSLIST` (MySQL 8.4, MariaDB 11.8 and 10.11).

**Documents this plan changes** (applied at plan approval, in a `research:` commit, a `gap:`
commit and an `infra:` commit; `test-infra/README.md` changes in Tasks 3 and 16):

- Roadmap:
  - §2.1: `mysql_async =0.37.1`, `mysql_common =0.37.3`, `sqlparser =0.62.0` (no default
    features), `flate2 1`.
  - §3.6: `prepare(checkpoint: Option<&Checkpoint>, …)` and `Prepared.server` (D-M7a.1).
  - §4 M7: split into M7a and M7b (Q-M7a-1); "Chunk reader with `utf8mb4_bin` ordering" →
    "in the key's own collation, with locking reads" (D-M7a.12, Q-M7-3); the DDL classifier's tier
    order (D-M7a.14).
  - §5.2: `mysql-84-nogtid`, `mysql-tls`, the `DATA_AVALANCHE_MYSQL` targets. §5.4: the MySQL CI
    step and `weekly.yml`'s `mariadb` job.
  - §9: D-IMPL-15 for D-M7a.1…23. §9.1: Q-M7-3…6 and Q-M7a-1…9.
- `Type_Mapping.md`: a MariaDB table (`UUID`, `INET4`, `INET6`, `VECTOR`, `JSON` as text); the
  `GEOMETRY` row ("MySQL internal form: SRID (4 bytes LE) + WKB", Q-M7a-7); the decode traps of
  D-M7a.10; the charsets of Q-M7a-2.
- `MySQL_DDL_Classifier.md` §2 and §3: the tier order, the cut-over swap, unlogged partition
  operations.
- `CDC_Design_Gap_Analysis_v1.md`: a dated section for M7a planning; DA-SRC-12 (the payload check
  designed and tested), the MariaDB compressed-event loss, the `verify-ca` limitation.
- `test-infra/README.md`: the two services (Task 3), the targets and the global-state binaries
  (Task 16).

**Spec patches** (approved and applied 2026-10-14; nothing in a task depends on them):

- `Schema_Change_Detection_and_Evolution.md` §7.2: a row for the cut-over swap (Q-M7a-8, Q-M7a-9),
  the `DROP TABLE` row's MySQL clause pointing at it, and a row for row-changing partition
  operations (D-M7a.15).
- `Configuration_Specification.md` §9.2.2 `heartbeat`: the watchdog finds the dump by the stream's
  connection id, not by `server_id` (D-M7a.21).

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-core/src/source.rs`, `…/lib.rs` | `ServerIdentity`, `Checkpoint`, `Prepared.server`, `prepare(Option<&Checkpoint>, …)` |
| `crates/data-avalanche-state/src/{lib,pipeline}.rs` | Re-export core's `ServerIdentity` |
| `crates/data-avalanche-source-pg/src/{connector,slot}.rs`, `…/tests/pg/slot.rs`, `…/tests/slot_invalidation.rs` | `prepare` takes a checkpoint; `Prepared.server: None` |
| `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-testkit/src/fake_source.rs` | Pass the position as a checkpoint |
| `crates/data-avalanche-config/src/…` | `server_id`, `heartbeat`, V-060, V-120, V-121; `glob_match`, `check_partitioning` (`external.rs`) |
| `crates/data-avalanche-source-pg/src/catalog.rs` | Uses config's glob and partition checks |
| `crates/data-avalanche-testkit/src/lib.rs` | The new services' endpoints |
| `test-infra/compose.yaml`, `test-infra/mysql-tls.sh`, `test-infra/README.md` | `mysql-84-nogtid`, `mysql-tls`; the MySQL section |
| `Cargo.toml` | The new crate; `mysql_common`, `flate2`, `sqlparser` |
| `crates/data-avalanche-source-mysql/Cargo.toml` | The crate and its dependencies |
| `…-source-mysql/src/lib.rs` | Modules, root re-exports, crate docs |
| `…-source-mysql/src/error.rs` | Server codes and client errors → `SourceError` |
| `…-source-mysql/src/connect.rs` | `Connector` (the catalog connection), `SESSION_SETUP`, TLS options |
| `…-source-mysql/src/server.rs` | `Flavour`, `version_of`, `Settings`, `check_settings`, `grants_replication` |
| `…-source-mysql/src/connector.rs` | `MySqlConnector`, `Shared`, the preflight, the `SourceConnector` impl |
| `…-source-mysql/src/position.rs` | `Mode`, `BinaryLog`, `Retention`, `purged`, `bytes_after`; the server's position, identity, binlogs |
| `…-source-mysql/src/binlog.rs` | `Offsets`, `MariaGtidEvent`, `decompress`, `rotate_name`, event type codes |
| `…-source-mysql/src/types.rs` | `CatalogColumn`, `ColumnPlan`, `plan_column`, `members` |
| `…-source-mysql/src/value.rs` | `Decode`, `Input`, `Charset`, `decode`, `decode_after`, `ValueError` |
| `…-source-mysql/src/json.rs` | Partial JSON: paths and diffs |
| `…-source-mysql/src/catalog.rs` | `select`, `describe`, `plan_table`, `resolve_tables` |
| `…-source-mysql/src/snapshot.rs` | `read_chunk`: the read plan, operands, the keyset predicate |
| `…-source-mysql/src/ddl.rs` | `classify`, `Ddl`, `Scope`, `Altered`, `renamed`, `shadow_of` |
| `…-source-mysql/src/schema.rs` | `scope_of`, `scope`, `diff`, `narrowed`, `augment`, `rescan` |
| `…-source-mysql/src/stream.rs` | `open_stream`, `MySqlStream` |
| `…-source-mysql/src/start.rs` | `prepare`, `purge_action`, `dialect_change`, `same_server`, `retained`, `dump_id` |
| `…-source-mysql/tests/mysql/main.rs`, `common.rs` | The integration binary; targets, per-test databases, pipelines |
| `…/tests/mysql/{preflight,tls,position,values,resolve,snapshot,schema,stream,start,roundtrip}.rs` | One file per area |
| `…-source-mysql/tests/binlog_purge.rs`, `tests/mariadb_compress.rs` | The server-wide tests, each alone |
| `.github/workflows/ci.yml`, `.github/workflows/weekly.yml` | The MySQL targets per change; MariaDB 11.4 and 10.11 weekly |

---

### Task 1: A checkpoint carries its source server (`data-avalanche-core`)

**Files:**
- Modify: `crates/data-avalanche-core/src/lib.rs`
- Modify: `crates/data-avalanche-core/src/source.rs`
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`
- Modify: `crates/data-avalanche-source-pg/src/connector.rs`
- Modify: `crates/data-avalanche-source-pg/src/slot.rs`
- Modify (test): `crates/data-avalanche-source-pg/tests/pg/slot.rs`
- Modify (test): `crates/data-avalanche-source-pg/tests/slot_invalidation.rs`
- Modify: `crates/data-avalanche-state/src/lib.rs`
- Modify: `crates/data-avalanche-state/src/pipeline.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`

**Interfaces:**
- Consumes:
  - `SourcePosition`, `SourceConnector`, `Prepared`, `StartPlan` (core, M5).
  - `data_avalanche_state::{PositionDoc, ServerIdentity}` (M2).
- Produces:
  - `data-avalanche-core/src/source.rs`:
    - `pub enum ServerIdentity`
    - `pub struct Checkpoint`

D-M7a.1. A binlog file and offset mean something only on the server that wrote them, so a
MySQL checkpoint is a position *and* the server it was read from (Q-M7-2). The trait's
`prepare` takes a `Checkpoint { position, server }` instead of a bare position, and
`Prepared` reports the server the stream reads, for the pipeline to record beside the next
checkpoint. `ServerIdentity` moves from `data-avalanche-state` (where M2 put it, unused) to
core, so the source crates can name it; the state crate re-exports it, and its JSON in
`replication/position.json` is unchanged.

The PostgreSQL source and the worker change mechanically: PostgreSQL records no server
(`Checkpoint::from(position)`, `Prepared.server: None`), and the worker passes its position as a
checkpoint without a server. Persisting and checking the server is M7b's (the worker's
checkpoint for MySQL).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-core/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/lib.rs b/crates/data-avalanche-core/src/lib.rs
--- a/crates/data-avalanche-core/src/lib.rs
+++ b/crates/data-avalanche-core/src/lib.rs
@@ -38,9 +38,10 @@ pub use sink::{
     WriteMode,
 };
 pub use source::{
-    ChangeStream, Chunk, ChunkVisibility, InFlightTxn, KeysetCursor, Preflight, Prepared,
-    RescanReport, ResolvedTables, RetainedLog, SchemaHint, SnapshotReason, SourceConnector,
-    SourceError, StartPlan, StreamItem, XidSnapshot, identifier_values,
+    ChangeStream, Checkpoint, Chunk, ChunkVisibility, InFlightTxn, KeysetCursor, Preflight,
+    Prepared, RescanReport, ResolvedTables, RetainedLog, SchemaHint, ServerIdentity,
+    SnapshotReason, SourceConnector, SourceError, StartPlan, StreamItem, XidSnapshot,
+    identifier_values,
 };
 pub use table::{
     INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
```

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -47,9 +47,11 @@ pub use generation::{
     config_snapshot_key,
 };
 pub use lease::{Acquired, Holder, LeaseDoc, Leases, lease_key};
+// The position document's server is core's type (M7a): the source reports it, the state records it.
+pub use data_avalanche_core::ServerIdentity;
 pub use pipeline::{
     DesiredState, PIPELINE_STATE_KEY, POSITION_KEY, PipelineDocs, PipelineStateDoc, PositionDoc,
-    SLOT_KEY, ServerIdentity, SlotDoc,
+    SLOT_KEY, SlotDoc,
 };
 pub use snapshot::{
     CursorKey, JOBS_PREFIX, JobStatus, JobTable, SnapshotCursor, SnapshotJobDoc, SnapshotStatus,
```

Change `crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -462,7 +462,7 @@ mod tests {
         }
         async fn prepare(
             &self,
-            checkpoint: Option<&SourcePosition>,
+            checkpoint: Option<&Checkpoint>,
             _: &[TableRef],
         ) -> Result<Prepared, SourceError> {
             let plan = match checkpoint {
@@ -476,6 +476,7 @@ mod tests {
                 plan,
                 added: vec![],
                 warnings: vec![],
+                server: None,
             })
         }
         async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
@@ -636,6 +637,36 @@ mod tests {
         assert!(!w.sees(3));
     }
 
+    #[test]
+    fn a_server_identity_names_its_engine_and_keeps_its_json() {
+        let mysql = ServerIdentity::Mysql {
+            server_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562".into(),
+        };
+        let mariadb = ServerIdentity::Mariadb { server_id: 7 };
+        assert_eq!(
+            mysql.to_string(),
+            "MySQL server 3e11fa47-71ca-11e1-9e33-c80aa9429562"
+        );
+        assert_eq!(mariadb.to_string(), "MariaDB server_id 7");
+        let json = serde_json::to_string(&mariadb).unwrap();
+        assert_eq!(json, r#"{"engine":"mariadb","server_id":7}"#);
+        assert_eq!(
+            serde_json::from_str::<ServerIdentity>(&json).unwrap(),
+            mariadb
+        );
+        assert!(
+            serde_json::from_str::<ServerIdentity>(r#"{"engine":"mysql","server_id":7}"#).is_err(),
+            "a field of the other engine is refused"
+        );
+    }
+
+    #[test]
+    fn a_position_alone_is_a_checkpoint_without_a_server() {
+        let c = Checkpoint::from(SourcePosition::Pg(Lsn(0x10)));
+        assert_eq!(c.position, SourcePosition::Pg(Lsn(0x10)));
+        assert_eq!(c.server, None);
+    }
+
     #[test]
     fn contended_is_retried_by_the_pipeline_not_as_transient() {
         let e = SourceError::Contended("replication slot \"s\" is active".into());
```

Change `crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -1372,7 +1372,10 @@ mod tests {
         assert_eq!(chunk.rows, vec![row(1, "a"), row(2, "b")]);
         s.acknowledge(&b.commit_pos).await.unwrap();
         assert_eq!(src.slot(), Some(b.commit_pos.clone()));
-        let resumed = src.prepare(Some(&b.commit_pos), &[t()]).await.unwrap();
+        let resumed = src
+            .prepare(Some(&Checkpoint::from(b.commit_pos.clone())), &[t()])
+            .await
+            .unwrap();
         assert_eq!(resumed.plan, StartPlan::Resume);
     }
 
@@ -1507,7 +1510,10 @@ mod tests {
             s.next().await,
             Err(SourceError::PositionUnreachable { .. })
         ));
-        let again = src.prepare(Some(&from), &[t()]).await.unwrap();
+        let again = src
+            .prepare(Some(&Checkpoint::from(from.clone())), &[t()])
+            .await
+            .unwrap();
         assert!(matches!(
             again.plan,
             StartPlan::Snapshot {
```

Change `crates/data-avalanche-source-pg/tests/pg/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/slot.rs b/crates/data-avalanche-source-pg/tests/pg/slot.rs
--- a/crates/data-avalanche-source-pg/tests/pg/slot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/slot.rs
@@ -4,7 +4,9 @@
 use std::time::Duration;
 
 use data_avalanche_core::position::Lsn;
-use data_avalanche_core::{SnapshotReason, SourceError, SourcePosition, StartPlan, TableRef};
+use data_avalanche_core::{
+    Checkpoint, SnapshotReason, SourceError, SourcePosition, StartPlan, TableRef,
+};
 use data_avalanche_testkit::require_it;
 use tokio_postgres::Client;
 
@@ -68,14 +70,20 @@ async fn first_start_then_resume_then_a_lost_slot() {
     assert_eq!(published(&a, &n).await, ["a", "b"]);
 
     // state present × slot present: resume.
-    let p = c.prepare(Some(&start), &tables).await.unwrap();
+    let p = c
+        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
+        .await
+        .unwrap();
     assert_eq!(p.plan, StartPlan::Resume);
 
     // state present × slot absent: recreate, snapshot (D-15.3).
     a.execute("SELECT pg_drop_replication_slot($1)", &[&n.slot])
         .await
         .unwrap();
-    let p = c.prepare(Some(&start), &tables).await.unwrap();
+    let p = c
+        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
+        .await
+        .unwrap();
     let again = from(&p.plan, SnapshotReason::PositionLost);
     assert!(confirmed(&a, &n).await.is_some(), "recreated");
 
@@ -89,7 +97,10 @@ async fn first_start_then_resume_then_a_lost_slot() {
     )
     .await
     .unwrap();
-    let p = c.prepare(Some(&again), &tables).await.unwrap();
+    let p = c
+        .prepare(Some(&Checkpoint::from(again.clone())), &tables)
+        .await
+        .unwrap();
     from(&p.plan, SnapshotReason::PositionLost);
     cleanup(&a, &n).await;
 }
@@ -157,7 +168,10 @@ async fn require_existing_never_creates() {
         .unwrap()
         .get(0);
     let start = SourcePosition::Pg(start.parse().unwrap());
-    let p = c.prepare(Some(&start), &tables).await.unwrap();
+    let p = c
+        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
+        .await
+        .unwrap();
     assert_eq!(p.plan, StartPlan::Resume);
     assert_eq!(
         p.warnings,
@@ -174,7 +188,9 @@ async fn require_existing_never_creates() {
         .await
         .unwrap();
     permanent(
-        c.prepare(Some(&start), &tables).await.unwrap_err(),
+        c.prepare(Some(&Checkpoint::from(start.clone())), &tables)
+            .await
+            .unwrap_err(),
         "require_existing",
     );
     permanent(c.reset_position().await.unwrap_err(), "require_existing");
@@ -202,14 +218,19 @@ async fn the_publication_follows_the_table_set() {
 
     // A table joins and one leaves: resume; the joining table is reported for its snapshot.
     let p = c
-        .prepare(Some(&start), &[n.table("b"), n.table("c")])
+        .prepare(
+            Some(&Checkpoint::from(start.clone())),
+            &[n.table("b"), n.table("c")],
+        )
         .await
         .unwrap();
     assert_eq!(p.plan, StartPlan::Resume);
     assert_eq!(p.added, [n.table("c")]);
     assert_eq!(published(&a, &n).await, ["b", "c"]);
     let empty: [TableRef; 0] = [];
-    c.prepare(Some(&start), &empty).await.unwrap();
+    c.prepare(Some(&Checkpoint::from(start.clone())), &empty)
+        .await
+        .unwrap();
     assert!(published(&a, &n).await.is_empty());
     cleanup(&a, &n).await;
 }
@@ -237,7 +258,10 @@ async fn a_publication_change_blocked_by_a_lock_is_retried_whole() {
         .await
         .unwrap();
     let err = c
-        .prepare(Some(&start), &[n.table("a"), n.table("b")])
+        .prepare(
+            Some(&Checkpoint::from(start.clone())),
+            &[n.table("a"), n.table("b")],
+        )
         .await
         .unwrap_err();
     assert!(err.is_transient(), "{err}");
@@ -245,7 +269,10 @@ async fn a_publication_change_blocked_by_a_lock_is_retried_whole() {
     // Time for a change the server didn't cancel to commit.
     tokio::time::sleep(Duration::from_secs(2)).await;
     let p = c
-        .prepare(Some(&start), &[n.table("a"), n.table("b")])
+        .prepare(
+            Some(&Checkpoint::from(start.clone())),
+            &[n.table("a"), n.table("b")],
+        )
         .await
         .unwrap();
     assert_eq!(p.plan, StartPlan::Resume);
@@ -282,7 +309,10 @@ async fn a_repaired_publication_re_snapshots() {
     ))
     .await
     .unwrap();
-    let p = c.prepare(Some(&start), &tables).await.unwrap();
+    let p = c
+        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
+        .await
+        .unwrap();
     let again = from(&p.plan, SnapshotReason::PositionLost);
     assert!(ops().await, "every operation published again");
     assert!(
@@ -300,7 +330,10 @@ async fn a_repaired_publication_re_snapshots() {
     a.batch_execute(&format!("DROP PUBLICATION {}", n.publication))
         .await
         .unwrap();
-    let p = c.prepare(Some(&again), &tables).await.unwrap();
+    let p = c
+        .prepare(Some(&Checkpoint::from(again.clone())), &tables)
+        .await
+        .unwrap();
     from(&p.plan, SnapshotReason::PositionLost);
     assert_eq!(published(&a, &n).await, ["a"]);
 
@@ -309,7 +342,7 @@ async fn a_repaired_publication_re_snapshots() {
         unreachable!()
     };
     let p = c
-        .prepare(Some(&SourcePosition::Pg(now)), &tables)
+        .prepare(Some(&Checkpoint::from(SourcePosition::Pg(now))), &tables)
         .await
         .unwrap();
     assert_eq!(p.plan, StartPlan::Resume);
@@ -332,7 +365,9 @@ async fn a_slot_of_another_plugin_is_refused() {
     let at = SourcePosition::Pg(at.parse().unwrap());
     let c = connector(&pipeline(&n, &["*"], "", ""));
     permanent(
-        c.prepare(Some(&at), &[n.table("a")]).await.unwrap_err(),
+        c.prepare(Some(&Checkpoint::from(at.clone())), &[n.table("a")])
+            .await
+            .unwrap_err(),
         "uses the test_decoding plugin, not pgoutput",
     );
     let adopt = connector(&pipeline(
@@ -390,7 +425,10 @@ async fn a_root_publication_of_tracked_partitions_is_refused() {
     let postgres = ", slot_management: require_existing, auto_cleanup: false";
     let c = connector(&pipeline(&n, &["*"], "", postgres));
     let e = c
-        .prepare(Some(&start), &[n.table("a"), n.table("events_2024")])
+        .prepare(
+            Some(&Checkpoint::from(start.clone())),
+            &[n.table("a"), n.table("events_2024")],
+        )
         .await
         .unwrap_err();
     permanent(
@@ -400,7 +438,10 @@ async fn a_root_publication_of_tracked_partitions_is_refused() {
             n.schema
         ),
     );
-    let p = c.prepare(Some(&start), &[n.table("a")]).await.unwrap();
+    let p = c
+        .prepare(Some(&Checkpoint::from(start.clone())), &[n.table("a")])
+        .await
+        .unwrap();
     assert_eq!(
         p.plan,
         StartPlan::Resume,
@@ -416,7 +457,10 @@ async fn a_root_publication_of_tracked_partitions_is_refused() {
     .unwrap();
     let c = connector(&pipeline(&n, &["*"], "", ""));
     let e = c
-        .prepare(Some(&start), &[n.table("events_2024")])
+        .prepare(
+            Some(&Checkpoint::from(start.clone())),
+            &[n.table("events_2024")],
+        )
         .await
         .unwrap_err();
     permanent(e, "publish_via_partition_root = false");
```

Change `crates/data-avalanche-source-pg/tests/slot_invalidation.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/slot_invalidation.rs b/crates/data-avalanche-source-pg/tests/slot_invalidation.rs
--- a/crates/data-avalanche-source-pg/tests/slot_invalidation.rs
+++ b/crates/data-avalanche-source-pg/tests/slot_invalidation.rs
@@ -6,7 +6,7 @@
 #[path = "pg/common.rs"]
 mod common;
 
-use data_avalanche_core::{SnapshotReason, StartPlan};
+use data_avalanche_core::{Checkpoint, SnapshotReason, StartPlan};
 use data_avalanche_testkit::require_it;
 
 use crate::common::{admin, cleanup, connector, fresh, pipeline};
@@ -66,7 +66,10 @@ async fn an_invalidated_slot_is_recreated() {
     let status = invalidate.unwrap();
     assert_eq!(status.as_deref(), Some("lost"));
 
-    let p = c.prepare(Some(&from), &tables).await.unwrap();
+    let p = c
+        .prepare(Some(&Checkpoint::from(from.clone())), &tables)
+        .await
+        .unwrap();
     let StartPlan::Snapshot { reason, .. } = p.plan else {
         panic!("a lost slot snapshots: {:?}", p.plan)
     };
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `source::Checkpoint`, `source::ServerIdentity`
error[E0425]: cannot find type `Checkpoint` in this scope
error[E0425]: cannot find type `ServerIdentity` in this scope
error[E0560]: struct `source::Prepared` has no field named `server`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -1,7 +1,10 @@
 //! The source connector contract (roadmap §3.6, Data-Flow Phases 0–2): what the pipeline asks of a
 //! source engine. Each source crate implements it; nothing here performs I/O.
 
+use std::fmt;
+
 use async_trait::async_trait;
+use serde::{Deserialize, Serialize};
 use thiserror::Error;
 
 use crate::{
@@ -93,6 +96,44 @@ pub enum StartPlan {
     },
 }
 
+/// The source server a stream position was read from (MySQL / MariaDB). A binlog file and offset
+/// mean something only on the server that wrote them: in file/position mode a start against
+/// another server halts the pipeline (`BINLOG_SOURCE_CHANGED`, Data-Flow Phase 2b). Its JSON is
+/// the `server` of `replication/position.json`.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
+#[serde(tag = "engine", rename_all = "lowercase", deny_unknown_fields)]
+pub enum ServerIdentity {
+    Mysql { server_uuid: String },
+    Mariadb { server_id: u32 },
+}
+
+impl fmt::Display for ServerIdentity {
+    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
+        match self {
+            Self::Mysql { server_uuid } => write!(f, "MySQL server {server_uuid}"),
+            Self::Mariadb { server_id } => write!(f, "MariaDB server_id {server_id}"),
+        }
+    }
+}
+
+/// The pipeline's durable stream position, as [`SourceConnector::prepare`] gets it: where the
+/// stream resumes, and the server it was read from when the source records one.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct Checkpoint {
+    pub position: SourcePosition,
+    pub server: Option<ServerIdentity>,
+}
+
+impl From<SourcePosition> for Checkpoint {
+    /// A position with no recorded server (PostgreSQL).
+    fn from(position: SourcePosition) -> Self {
+        Self {
+            position,
+            server: None,
+        }
+    }
+}
+
 /// What [`SourceConnector::prepare`] decided, and what it noticed on the way (publication
 /// membership drift under `slot_management: require_existing`).
 #[derive(Debug, Clone, PartialEq, Eq)]
@@ -104,6 +145,9 @@ pub struct Prepared {
     /// publication behind its back. Empty when the plan snapshots every table.
     pub added: Vec<TableRef>,
     pub warnings: Vec<String>,
+    /// The server the stream reads, recorded with the checkpoint so the next start can check it
+    /// (MySQL / MariaDB). `None` for a source that records none (PostgreSQL).
+    pub server: Option<ServerIdentity>,
 }
 
 /// A keyset cursor (Snapshot_Resumability §2): the identifier values of the last row read, in
@@ -264,11 +308,12 @@ pub trait SourceConnector: Send + Sync {
     async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError>;
 
     /// Prepares the stream position for a start (PostgreSQL: the publication, then the slot state
-    /// machine). `checkpoint` is the pipeline's durable position, `None` without state; `tables`
-    /// is the tracked set.
+    /// machine; MySQL: the checkpoint's server and whether its binlog is still there).
+    /// `checkpoint` is the pipeline's durable position, `None` without state; `tables` is the
+    /// tracked set.
     async fn prepare(
         &self,
-        checkpoint: Option<&SourcePosition>,
+        checkpoint: Option<&Checkpoint>,
         tables: &[TableRef],
     ) -> Result<Prepared, SourceError>;
 
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -9,7 +9,7 @@ use std::time::Instant;
 
 use data_avalanche_config::{Pipeline, SnapshotMode};
 use data_avalanche_core::{
-    HaltReason, SchemaHint, Sink, SinkError, SinkKind, SnapshotReason, SourceConnector,
+    Checkpoint, HaltReason, SchemaHint, Sink, SinkError, SinkKind, SnapshotReason, SourceConnector,
     SourceError, SourcePosition, StartPlan, TableRef, TableSchema, TableSpec,
 };
 use data_avalanche_state::{
@@ -425,11 +425,12 @@ pub(crate) async fn start(
                 core.docs.save_slot(&SlotDoc::intent(name)).await?;
             }
             let refs: Vec<TableRef> = resolved_tables.keys().cloned().collect();
+            let durable = checkpoint.clone().map(Checkpoint::from);
             // `prepare` may create, recreate or advance the slot.
             let prepared = write_retry(
                 &tuning,
                 || core.may_write(),
-                || source.prepare(checkpoint.as_ref(), &refs),
+                || source.prepare(durable.as_ref(), &refs),
             )
             .await?;
             warnings.extend(prepared.warnings);
```

Change `crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -7,9 +7,9 @@ use data_avalanche_config::{
     Pipeline, Postgres, SourceType, check_table_set, check_unbounded_numeric,
 };
 use data_avalanche_core::{
-    ChangeStream, Chunk, HaltReason, InFlightTxn, KeysetCursor, Preflight, Prepared, RescanReport,
-    ResolvedTables, RetainedLog, Row, SchemaDelta, SchemaHint, SourceConnector, SourceError,
-    SourcePosition, TableRef, TableSchema,
+    ChangeStream, Checkpoint, Chunk, HaltReason, InFlightTxn, KeysetCursor, Preflight, Prepared,
+    RescanReport, ResolvedTables, RetainedLog, Row, SchemaDelta, SchemaHint, SourceConnector,
+    SourceError, SourcePosition, TableRef, TableSchema,
 };
 use tokio::sync::Mutex;
 
@@ -264,7 +264,7 @@ impl SourceConnector for PgConnector {
 
     async fn prepare(
         &self,
-        checkpoint: Option<&SourcePosition>,
+        checkpoint: Option<&Checkpoint>,
         tables: &[TableRef],
     ) -> Result<Prepared, SourceError> {
         PgConnector::prepare(self, checkpoint, tables).await
```

Change `crates/data-avalanche-source-pg/src/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -7,7 +7,8 @@ use std::collections::BTreeSet;
 use data_avalanche_config::{SlotManagement, SlotOnOrphan};
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    Prepared, RetainedLog, SnapshotReason, SourceError, SourcePosition, StartPlan, TableRef,
+    Checkpoint, Prepared, RetainedLog, SnapshotReason, SourceError, SourcePosition, StartPlan,
+    TableRef,
 };
 use tokio_postgres::Client;
 
@@ -400,10 +401,10 @@ impl PgConnector {
     /// rather than resuming over WAL the old publication didn't publish.
     pub async fn prepare(
         &self,
-        checkpoint: Option<&SourcePosition>,
+        checkpoint: Option<&Checkpoint>,
         tables: &[TableRef],
     ) -> Result<Prepared, SourceError> {
-        let checkpoint = checkpoint.map(lsn_of).transpose()?;
+        let checkpoint = checkpoint.map(|c| lsn_of(&c.position)).transpose()?;
         let client = self.conn.client().await?;
         let Reconciled {
             sql,
@@ -478,6 +479,7 @@ impl PgConnector {
             plan,
             added,
             warnings,
+            server: None,
         })
     }
 
```

Change `crates/data-avalanche-state/src/pipeline.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/pipeline.rs b/crates/data-avalanche-state/src/pipeline.rs
--- a/crates/data-avalanche-state/src/pipeline.rs
+++ b/crates/data-avalanche-state/src/pipeline.rs
@@ -5,8 +5,8 @@
 use std::fmt::Display;
 use std::str::FromStr;
 
-use data_avalanche_core::SourcePosition;
 use data_avalanche_core::position::Lsn;
+use data_avalanche_core::{ServerIdentity, SourcePosition};
 use serde::{Deserialize, Deserializer, Serialize, Serializer};
 
 use crate::doc::{CREATE_ATTEMPTS, DocStore, Document, StateError, Versioned};
@@ -70,15 +70,6 @@ impl SlotDoc {
     }
 }
 
-/// The source server a MySQL / MariaDB position was read from. File/position mode refuses to
-/// resume against a different server (`BINLOG_SOURCE_CHANGED`, Q-M7-2).
-#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
-#[serde(tag = "engine", rename_all = "lowercase", deny_unknown_fields)]
-pub enum ServerIdentity {
-    Mysql { server_uuid: String },
-    Mariadb { server_id: u32 },
-}
-
 /// MySQL / MariaDB: the stream position (`mysql-gtid:…`, `mariadb-gtid:…`, `binlog:<file>:<pos>`).
 #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
 #[serde(deny_unknown_fields)]
```

Change `crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -31,11 +31,11 @@ use std::sync::{Arc, Mutex, MutexGuard};
 use async_trait::async_trait;
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    CdcEvent, ChangeStream, Chunk, ChunkVisibility, ColumnSchema, ColumnValue, HaltReason,
-    InFlightTxn, KeysetCursor, Op, Preflight, Prepared, RescanReport, ResolvedTables, RetainedLog,
-    Row, SchemaChange, SchemaDelta, SchemaHint, SnapshotReason, SourceConnector, SourceError,
-    SourcePosition, SourceSeq, StartPlan, StreamItem, TableRef, TableSchema, TransactionBoundary,
-    XidSnapshot, identifier_values,
+    CdcEvent, ChangeStream, Checkpoint, Chunk, ChunkVisibility, ColumnSchema, ColumnValue,
+    HaltReason, InFlightTxn, KeysetCursor, Op, Preflight, Prepared, RescanReport, ResolvedTables,
+    RetainedLog, Row, SchemaChange, SchemaDelta, SchemaHint, SnapshotReason, SourceConnector,
+    SourceError, SourcePosition, SourceSeq, StartPlan, StreamItem, TableRef, TableSchema,
+    TransactionBoundary, XidSnapshot, identifier_values,
 };
 use tokio::sync::Notify;
 
@@ -861,13 +861,13 @@ impl SourceConnector for FakeSource {
 
     async fn prepare(
         &self,
-        checkpoint: Option<&SourcePosition>,
+        checkpoint: Option<&Checkpoint>,
         _tables: &[TableRef],
     ) -> Result<Prepared, SourceError> {
         self.take_fault(Method::Prepare)?;
         let mut db = self.db();
-        db.prepares.push(checkpoint.cloned());
-        let checkpoint = checkpoint.map(lsn);
+        db.prepares.push(checkpoint.map(|c| c.position.clone()));
+        let checkpoint = checkpoint.map(|c| lsn(&c.position));
         let reason = match (checkpoint, db.slot) {
             (None, None) => Some(SnapshotReason::FirstStart),
             (None, Some(_)) => {
@@ -885,6 +885,7 @@ impl SourceConnector for FakeSource {
                 plan: StartPlan::Resume,
                 added: std::mem::take(&mut db.added),
                 warnings: vec![],
+                server: None,
             },
             Some(reason) => {
                 db.slot = Some(db.wal_end);
@@ -896,6 +897,7 @@ impl SourceConnector for FakeSource {
                     },
                     added: vec![],
                     warnings: vec![],
+                    server: None,
                 }
             }
         })
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core --lib`

Expected: PASS, 151 tests.

Run: `cargo test --workspace`

Expected: PASS, every test binary (938 tests).

Run: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg --test slot_invalidation -- --test-threads=1 --skip tls::`

Expected: PASS, 52 tests on the target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core/src/lib.rs \
  crates/data-avalanche-core/src/source.rs \
  crates/data-avalanche-pipeline/src/worker.rs \
  crates/data-avalanche-source-pg/src/connector.rs \
  crates/data-avalanche-source-pg/src/slot.rs \
  crates/data-avalanche-source-pg/tests/pg/slot.rs \
  crates/data-avalanche-source-pg/tests/slot_invalidation.rs \
  crates/data-avalanche-state/src/lib.rs \
  crates/data-avalanche-state/src/pipeline.rs \
  crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "feat(core): a checkpoint carries its source server (ServerIdentity, Checkpoint, Prepared.server)"
```

---

### Task 2: `source.mysql.server_id` and `source.mysql.heartbeat` (`data-avalanche-config`)

**Files:**
- Modify: `crates/data-avalanche-config/src/build.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`
- Modify: `crates/data-avalanche-config/src/merge.rs`
- Modify: `crates/data-avalanche-config/src/mutability.rs`
- Modify: `crates/data-avalanche-config/src/raw.rs`
- Modify: `crates/data-avalanche-config/src/registry.rs`
- Modify: `crates/data-avalanche-config/src/resolved.rs`
- Modify: `crates/data-avalanche-config/src/rules/domain.rs`
- Modify: `crates/data-avalanche-config/src/rules/identity.rs`
- Modify (test): `crates/data-avalanche-config/tests/load.rs`
- Modify (test): `crates/data-avalanche-config/tests/rules/domain.rs`
- Modify (test): `crates/data-avalanche-config/tests/rules/identity.rs`

**Interfaces:**
- Consumes:
  - The config crate's raw → resolved build, its registry and the rule modules (M1).
- Produces:
  - `data-avalanche-config/src/build.rs`:
    - `pub fn derived_server_id(id: &str) -> u32`

Q-M7-5 and Q-M7-6, as config §9.2.2 states them (applied 2026-10-14, d026ac0).

- `server_id`: the replica id the binlog dump registers with. A source server ends any other
  dump under the same id (S-MY10h), so every pipeline needs its own. Unset, it is derived from
  the pipeline `id`: the first four bytes of its BLAKE3 hash, little-endian, with the top bit set
  (2147483648…4294967295). V-060 lists it among the keys a pipeline may set; V-120 refuses two
  pipelines on one source server (`host` + `port`) whose ids, set or derived, are equal, and an
  id of 0. The derived value of `orders` is 2328456920, cross-checked with Python's `blake3`.
- `heartbeat`: the server's heartbeat period (`@master_heartbeat_period`) and the stream
  watchdog's pace (default `60s`, V-121: `1s` to `4294967s`).

Mutability: `server_id` is pipeline-restart (`PR`), `heartbeat` reload (`R`).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -88,6 +88,7 @@ mod template;
 mod units;
 mod walk;
 
+pub use build::derived_server_id;
 pub use coerce::LogFilter;
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use diff::{ConfigDiff, FieldChange, PipelineChange, diff};
```

Change `crates/data-avalanche-config/src/build.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/build.rs b/crates/data-avalanche-config/src/build.rs
--- a/crates/data-avalanche-config/src/build.rs
+++ b/crates/data-avalanche-config/src/build.rs
@@ -1448,6 +1448,17 @@ mod tests {
     /// V-068 / §11 Phase 2: `hive.tls.ca` is only in effect when `tls.enabled` is `true`, the same
     /// gate `api.tls`/`memberlist.tls` use. Unreachable via `load_str` today (V-115 refuses `hive`
     /// in Phase 1), so exercised directly against `hive()`.
+    #[test]
+    fn the_derived_server_id_is_stable_and_in_the_high_range() {
+        // Pinned: a change would re-register every pipeline under a new replica id.
+        // Checked against the BLAKE3 reference implementation (`blake3` 1.0 for Python).
+        assert_eq!(derived_server_id("orders"), 2_328_456_920);
+        for id in ["a", "b", "orders", "events-mysql"] {
+            assert!(derived_server_id(id) >= 1 << 31, "{id}");
+        }
+        assert_ne!(derived_server_id("a"), derived_server_id("b"));
+    }
+
     #[test]
     fn hive_tls_ca_is_not_read_when_tls_is_disabled() {
         let env: BTreeMap<String, String> = BTreeMap::new();
```

Change `crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1025,7 +1025,7 @@ mod tests {
 
     #[test]
     fn has_every_spec_rule_once_in_order() {
-        assert_eq!(RULES.len(), 114);
+        assert_eq!(RULES.len(), 116);
         assert!(
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
```

Change `crates/data-avalanche-config/tests/load.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/load.rs b/crates/data-avalanche-config/tests/load.rs
--- a/crates/data-avalanche-config/tests/load.rs
+++ b/crates/data-avalanche-config/tests/load.rs
@@ -189,6 +189,37 @@ fn absent_mysql_and_delta_blocks_take_every_default() {
     assert_eq!(delta.merge_memory_limit, None, "no ceiling by default");
 }
 
+#[test]
+fn a_mysql_server_id_and_heartbeat_resolve() {
+    let mysql = |extra: &str| {
+        MINIMAL
+            .replace(
+                "host: db.example.com",
+                "type: mysql\n      host: db.example.com",
+            )
+            .replace(
+                "      postgres: { publication: orders_pub, slot: orders_slot }\n",
+                &format!("      mysql: {{ {extra} }}\n"),
+            )
+            .replace("include: [public.orders]", "include: [orders]")
+    };
+    let resolve = |yaml: String| {
+        let loaded =
+            load_str(&yaml, &env(&[("PW", "pw")]), &fp_key()).unwrap_or_else(|r| panic!("{r:#?}"));
+        loaded.config.pipelines["orders"]
+            .source
+            .mysql
+            .clone()
+            .unwrap()
+    };
+    let derived = resolve(mysql(""));
+    assert_eq!(derived.server_id, derived_server_id("orders"));
+    assert_eq!(derived.heartbeat, Duration::from_secs(60));
+    let set = resolve(mysql("server_id: 42, heartbeat: 5s"));
+    assert_eq!(set.server_id, 42);
+    assert_eq!(set.heartbeat, Duration::from_secs(5));
+}
+
 #[test]
 fn a_merge_memory_limit_resolves_to_bytes() {
     let yaml = MINIMAL.replace("delta: {}", "delta: { merge_memory_limit: 256MiB }");
```

Change `crates/data-avalanche-config/tests/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/domain.rs b/crates/data-avalanche-config/tests/rules/domain.rs
--- a/crates/data-avalanche-config/tests/rules/domain.rs
+++ b/crates/data-avalanche-config/tests/rules/domain.rs
@@ -323,6 +323,24 @@ pub fn cases() -> Vec<Case> {
         case("V-118", Silent, "snapshot timeout 10s", || {
             pg_delta().set("/pipelines/p/source/timeouts/snapshot", json!("10s"))
         }),
+        case("V-121", Error, "mysql heartbeat 0s", || {
+            mysql_delta().set("/pipelines/p/source/mysql/heartbeat", json!("0s"))
+        }),
+        case("V-121", Error, "mysql heartbeat 500ms", || {
+            mysql_delta().set("/pipelines/p/source/mysql/heartbeat", json!("500ms"))
+        }),
+        case("V-121", Error, "mysql heartbeat 4294968s", || {
+            mysql_delta().set("/pipelines/p/source/mysql/heartbeat", json!("4294968s"))
+        }),
+        case("V-121", Silent, "mysql heartbeat 1s", || {
+            mysql_delta().set("/pipelines/p/source/mysql/heartbeat", json!("1s"))
+        }),
+        case("V-121", Silent, "mysql heartbeat 4294967s", || {
+            mysql_delta().set("/pipelines/p/source/mysql/heartbeat", json!("4294967s"))
+        }),
+        case("V-121", Error, "defaults mysql heartbeat 0s", || {
+            mysql_delta().set("/defaults/source/mysql/heartbeat", json!("0s"))
+        }),
         case("V-119", Error, "merge memory 1MiB", || {
             pg_delta().set("/pipelines/p/sink/delta/merge_memory_limit", json!("1MiB"))
         }),
```

Change `crates/data-avalanche-config/tests/rules/identity.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/identity.rs b/crates/data-avalanche-config/tests/rules/identity.rs
--- a/crates/data-avalanche-config/tests/rules/identity.rs
+++ b/crates/data-avalanche-config/tests/rules/identity.rs
@@ -2,7 +2,15 @@
 
 use serde_json::{Value, json};
 
-use crate::cases::{Case, Edit, Expect::*, case, pg_delta};
+use crate::cases::{Case, Edit, Expect::*, case, mysql_delta, pg_delta};
+
+/// Two MySQL pipelines `p` and `q` on one server, each with its own derived `server_id`.
+fn two_mysql() -> Value {
+    let doc = mysql_delta();
+    let q = doc.pointer("/pipelines/p").unwrap().clone();
+    doc.set("/pipelines/q", q)
+        .set("/pipelines/q/id", json!("q"))
+}
 
 /// The base config with a second pipeline `q` copied from `p`, then given its own id, slot and
 /// publication so the two don't collide.
@@ -129,5 +137,33 @@ pub fn cases() -> Vec<Case> {
         case("V-060", Silent, "shared settings in defaults", || {
             pg_delta().set("/defaults/source/postgres/heartbeat", json!("30s"))
         }),
+        case("V-060", Error, "mysql server_id in defaults", || {
+            mysql_delta().set("/defaults/source/mysql/server_id", json!(7))
+        }),
+        case("V-060", Silent, "mysql heartbeat in defaults", || {
+            mysql_delta().set("/defaults/source/mysql/heartbeat", json!("30s"))
+        }),
+        case("V-120", Error, "server_id 0", || {
+            mysql_delta().set("/pipelines/p/source/mysql/server_id", json!(0))
+        }),
+        case("V-120", Error, "server_id shared on one server", || {
+            two_mysql()
+                .set("/pipelines/p/source/mysql/server_id", json!(7))
+                .set("/pipelines/q/source/mysql/server_id", json!(7))
+        }),
+        // A set id collides with another pipeline's derived one just the same.
+        case("V-120", Error, "server_id equal to a derived one", || {
+            two_mysql().set(
+                "/pipelines/q/source/mysql/server_id",
+                json!(data_avalanche_config::derived_server_id("p")),
+            )
+        }),
+        case("V-120", Silent, "same server_id on another port", || {
+            two_mysql()
+                .set("/pipelines/p/source/mysql/server_id", json!(7))
+                .set("/pipelines/q/source/mysql/server_id", json!(7))
+                .set("/pipelines/q/source/port", json!(3307))
+        }),
+        case("V-120", Silent, "derived server_ids", two_mysql),
     ]
 }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `build::derived_server_id`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-config/src/build.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/build.rs b/crates/data-avalanche-config/src/build.rs
--- a/crates/data-avalanche-config/src/build.rs
+++ b/crates/data-avalanche-config/src/build.rs
@@ -39,6 +39,16 @@ pub(crate) const DEFAULT_FLUSH_BYTES: u64 = 512 * ByteSize::MIB;
 pub(crate) const DEFAULT_POSTGRES_PORT: u16 = 5432;
 pub(crate) const DEFAULT_MYSQL_PORT: u16 = 3306;
 
+/// The default `source.mysql.server_id` (§9.2.2): the first four bytes of the BLAKE3 hash of the
+/// pipeline `id`, little-endian, with the top bit set, so 2147483648…4294967295, away from the
+/// small ids replicas usually take. Stable: a changed derivation would move every pipeline to a
+/// new replica id.
+pub fn derived_server_id(id: &str) -> u32 {
+    let hash = blake3::hash(id.as_bytes());
+    let b = hash.as_bytes();
+    u32::from_le_bytes([b[0], b[1], b[2], b[3]]) | 0x8000_0000
+}
+
 /// A field that has a default: absent → `Some(default)`; present → its value (`None` if unknown
 /// or invalid).
 fn or<T: LeafType>(ctx: &mut Ctx, leaf: &L<T>, at: &At, key: &str, default: T) -> Option<T> {
@@ -639,6 +649,7 @@ fn pipeline(
     let source = source(
         &p.source.clone().unwrap_or_default(),
         &at.key("source"),
+        id.as_deref(),
         ctx,
     );
     let tables = tables(
@@ -698,7 +709,7 @@ fn pipeline(
     })
 }
 
-fn source(s: &RawSource, at: &At, ctx: &mut Ctx) -> Option<Source> {
+fn source(s: &RawSource, at: &At, id: Option<&str>, ctx: &mut Ctx) -> Option<Source> {
     let kind = or(ctx, &s.kind, at, "type", SourceType::Postgres);
     let host = req(ctx, &s.host, at, "host");
     let port = opt(ctx, &s.port, at, "port");
@@ -758,6 +769,7 @@ fn source(s: &RawSource, at: &At, ctx: &mut Ctx) -> Option<Source> {
     let mysql = mysql(
         s.mysql.as_ref().unwrap_or(&RawMysql::default()),
         &at.key("mysql"),
+        id,
         ctx,
     );
     let kind = kind?;
@@ -821,7 +833,9 @@ fn postgres(p: &RawPostgres, at: &At, ctx: &mut Ctx) -> Option<Postgres> {
     })
 }
 
-fn mysql(m: &RawMysql, at: &At, ctx: &mut Ctx) -> Option<Mysql> {
+/// `id`: the pipeline's, `None` when it is missing (V-055 reports it): then so is the derived
+/// `server_id`, and the pipeline doesn't resolve either way.
+fn mysql(m: &RawMysql, at: &At, id: Option<&str>, ctx: &mut Ctx) -> Option<Mysql> {
     let replication_type = or(
         ctx,
         &m.replication_type,
@@ -844,11 +858,18 @@ fn mysql(m: &RawMysql, at: &At, ctx: &mut Ctx) -> Option<Mysql> {
         "binlog_purge_action",
         BinlogPurgeAction::Error,
     );
+    let server_id = match &m.server_id {
+        None => id.map(derived_server_id),
+        Some(l) => ctx.value(l, &at.key("server_id")),
+    };
+    let heartbeat = or(ctx, &m.heartbeat, at, "heartbeat", Duration::from_secs(60));
     Some(Mysql {
         replication_type: replication_type?,
         tinyint1_as_bool: tinyint1_as_bool?,
         tinyint1_detection: tinyint1_detection?,
         binlog_purge_action: binlog_purge_action?,
+        server_id: server_id?,
+        heartbeat: heartbeat?,
     })
 }
 
```

Change `crates/data-avalanche-config/src/merge.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/merge.rs b/crates/data-avalanche-config/src/merge.rs
--- a/crates/data-avalanche-config/src/merge.rs
+++ b/crates/data-avalanche-config/src/merge.rs
@@ -101,7 +101,9 @@ merge_struct!(RawMysql {
     replication_type,
     tinyint1_as_bool,
     tinyint1_detection,
-    binlog_purge_action
+    binlog_purge_action,
+    server_id,
+    heartbeat
 });
 merge_struct!(RawTables {
     include,
```

Change `crates/data-avalanche-config/src/mutability.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/mutability.rs b/crates/data-avalanche-config/src/mutability.rs
--- a/crates/data-avalanche-config/src/mutability.rs
+++ b/crates/data-avalanche-config/src/mutability.rs
@@ -51,6 +51,7 @@ pub(crate) const CLASSES: &[(&str, Mutability)] = &[
     ("pipelines.*.source.postgres.slot_on_orphan", R),
     ("pipelines.*.source.postgres.max_wal_lag_bytes", R),
     ("pipelines.*.source.mysql.binlog_purge_action", R),
+    ("pipelines.*.source.mysql.heartbeat", R),
     ("pipelines.*.tables", Pr),
     ("pipelines.*.tables.include", PrIfTableSetChanges),
     ("pipelines.*.tables.exclude", PrIfTableSetChanges),
```

Change `crates/data-avalanche-config/src/raw.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/raw.rs b/crates/data-avalanche-config/src/raw.rs
--- a/crates/data-avalanche-config/src/raw.rs
+++ b/crates/data-avalanche-config/src/raw.rs
@@ -219,6 +219,8 @@ pub(crate) struct RawMysql {
     pub tinyint1_as_bool: L<bool>,
     pub tinyint1_detection: L<Tinyint1Detection>,
     pub binlog_purge_action: L<BinlogPurgeAction>,
+    pub server_id: L<u32>,
+    pub heartbeat: L<Duration>,
 }
 
 #[derive(Debug, Clone, Default, Deserialize)]
```

Change `crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -546,7 +546,7 @@ pub const RULES: &[RuleMeta] = &[
         L::Config,
         Config,
         r#"defaults"#,
-        r#"Must not set per-pipeline-unique resource names (postgres.publication, postgres.slot, id)."#,
+        r#"Must not set per-pipeline-unique resource names (postgres.publication, postgres.slot, mysql.server_id, id)."#,
     ),
     rule(
         "V-061",
@@ -1012,6 +1012,22 @@ pub const RULES: &[RuleMeta] = &[
         r#"sink.delta.merge_memory_limit"#,
         r#"≥ 16 MiB when set."#,
     ),
+    rule(
+        "V-120",
+        Error,
+        L::Config,
+        Config,
+        r#"source.mysql.server_id"#,
+        r#"≥ 1 when set. Unique per source server (host + port) across pipelines, set or derived: two dumps under one id end each other."#,
+    ),
+    rule(
+        "V-121",
+        Error,
+        L::Pipeline,
+        Config,
+        r#"source.mysql.heartbeat"#,
+        r#"Between 1s and 4294967s (the server's heartbeat period range)."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
```

Change `crates/data-avalanche-config/src/resolved.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/resolved.rs b/crates/data-avalanche-config/src/resolved.rs
--- a/crates/data-avalanche-config/src/resolved.rs
+++ b/crates/data-avalanche-config/src/resolved.rs
@@ -261,6 +261,14 @@ pub struct Mysql {
     pub tinyint1_as_bool: bool,
     pub tinyint1_detection: Tinyint1Detection,
     pub binlog_purge_action: BinlogPurgeAction,
+    /// The replica id the binlog dump registers with: set, or [`derived_server_id`] of the
+    /// pipeline `id` (§9.2.2, V-120).
+    ///
+    /// [`derived_server_id`]: crate::derived_server_id
+    pub server_id: u32,
+    /// `@master_heartbeat_period`, and the stream watchdog's pace (§9.2.2, V-121).
+    #[serde(serialize_with = "ser_duration")]
+    pub heartbeat: Duration,
 }
 
 #[derive(Debug, Clone, PartialEq, Serialize)]
```

Change `crates/data-avalanche-config/src/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/domain.rs b/crates/data-avalanche-config/src/rules/domain.rs
--- a/crates/data-avalanche-config/src/rules/domain.rs
+++ b/crates/data-avalanche-config/src/rules/domain.rs
@@ -208,6 +208,26 @@ fn pipeline_fields(at: &At, p: &RawPipeline, ctx: &mut Ctx, cross_field: bool) {
             );
         }
     }
+    // V-121: `@master_heartbeat_period`'s range.
+    if let Some(my) = p.source.as_ref().and_then(|s| s.mysql.as_ref()) {
+        let hb_at = at.key("source").key("mysql").key("heartbeat");
+        if let Some((v, loc)) = val(ctx, &my.heartbeat, &hb_at)
+            && !(Duration::from_secs(1)..=Duration::from_secs(4_294_967)).contains(&v)
+        {
+            let l = my.heartbeat.as_ref().expect("val returned Some");
+            let shown = if is_templated(l) {
+                format!("{:?} (resolved value hidden)", l.text())
+            } else {
+                crate::units::format_duration(v)
+            };
+            ctx.error(
+                "V-121",
+                &hb_at,
+                loc,
+                format!("heartbeat must be between 1s and 4294967s, got {shown}"),
+            );
+        }
+    }
     if let Some(pg) = p.source.as_ref().and_then(|s| s.postgres.as_ref()) {
         // V-104
         let lag_at = at.key("source").key("postgres").key("max_wal_lag_bytes");
```

Change `crates/data-avalanche-config/src/rules/identity.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/identity.rs b/crates/data-avalanche-config/src/rules/identity.rs
--- a/crates/data-avalanche-config/src/rules/identity.rs
+++ b/crates/data-avalanche-config/src/rules/identity.rs
@@ -3,7 +3,7 @@
 use std::collections::BTreeMap;
 
 use super::pipeline_at;
-use crate::build::DEFAULT_POSTGRES_PORT;
+use crate::build::{DEFAULT_MYSQL_PORT, DEFAULT_POSTGRES_PORT, derived_server_id};
 use crate::ctx::{At, Ctx, shown, shown_value};
 use crate::enums::{SlotManagement, SourceType};
 use crate::leaf::Leaf;
@@ -18,6 +18,9 @@ type SlotEntries<'a> = Vec<(&'a str, String, String, String)>;
 /// V-059 dedup entries: the pipeline name, then the host's, port's, db's and publication's redacted
 /// display text, and whether the pipeline manages its publication (`slot_management: managed`).
 type PublicationEntries<'a> = Vec<(&'a str, String, String, String, String, bool)>;
+/// V-120 dedup entries: the pipeline name, then the host's, port's and the server id's redacted
+/// display text (a derived id is shown as such).
+type ServerIdEntries<'a> = Vec<(&'a str, String, String, String)>;
 
 /// `^[a-z0-9][a-z0-9_-]{0,62}$` (V-055).
 fn is_pipeline_id(s: &str) -> bool {
@@ -60,6 +63,7 @@ pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut
     if let Some(d) = &raw.defaults {
         let at = At::config(YamlPath::root().key("defaults"));
         let pg = d.source.as_ref().and_then(|s| s.postgres.as_ref());
+        let my = d.source.as_ref().and_then(|s| s.mysql.as_ref());
         let forbidden = [
             (
                 "id",
@@ -80,6 +84,12 @@ pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut
                 at.key("source").key("postgres").key("slot"),
                 pg.is_some_and(|p| p.slot.is_some()),
             ),
+            (
+                "source.mysql.server_id",
+                my.and_then(|m| m.server_id.as_ref()).and_then(|l| l.loc()),
+                at.key("source").key("mysql").key("server_id"),
+                my.is_some_and(|m| m.server_id.is_some()),
+            ),
             (
                 "tables.snapshots.overrides",
                 None,
@@ -113,6 +123,7 @@ pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut
     let mut slots: BTreeMap<(String, u16, String), SlotEntries<'_>> = BTreeMap::new();
     let mut publications: BTreeMap<(String, u16, String, String), PublicationEntries<'_>> =
         BTreeMap::new();
+    let mut server_ids: BTreeMap<(String, u16, u32), ServerIdEntries<'_>> = BTreeMap::new();
     for (name, p) in merged {
         let at = pipeline_at(name);
         // V-057
@@ -146,8 +157,54 @@ pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut
                 }
             }
         }
-        // V-058, V-059
         let Some(src) = &p.source else { continue };
+        // V-120: the binlog dump's replica id, set or derived from `id`, is unique per source
+        // server (`host` + `port`): a second dump under one id ends the first (S-MY10h).
+        if ctx.peek(&src.kind) == Some(SourceType::Mysql) {
+            let my = src.mysql.clone().unwrap_or_default();
+            let my_at = at.key("source").key("mysql");
+            let id = p.id.as_ref().and_then(|l| ctx.text(l, &at.key("id")));
+            let server_id = match &my.server_id {
+                Some(l) => ctx
+                    .value(l, &my_at.key("server_id"))
+                    .map(|v| (v, shown_value(l, &v), l.loc())),
+                None => id.map(|id| {
+                    let v = derived_server_id(&id);
+                    (v, format!("{v} (derived from the id)"), None)
+                }),
+            };
+            if let Some((0, _, loc)) = &server_id {
+                ctx.error(
+                    "V-120",
+                    &At::config(my_at.key("server_id").path),
+                    *loc,
+                    "server_id must be at least 1",
+                );
+            } else if let Some((v, shown_id, _)) = server_id {
+                let host = src
+                    .host
+                    .as_ref()
+                    .and_then(|l| ctx.text(l, &at.key("source").key("host")).map(|t| (l, t)));
+                let port: Option<(Option<&Leaf<u16>>, u16)> = match &src.port {
+                    Some(l) => ctx
+                        .value(l, &at.key("source").key("port"))
+                        .map(|v| (Some(l), v)),
+                    None => Some((None, DEFAULT_MYSQL_PORT)),
+                };
+                if let (Some((host_leaf, host)), Some((port_leaf, port))) = (host, port) {
+                    let host_shown = shown(host_leaf, &host);
+                    let port_shown = match port_leaf {
+                        Some(pl) => shown_value(pl, &port),
+                        None => port.to_string(),
+                    };
+                    server_ids
+                        .entry((host, port, v))
+                        .or_default()
+                        .push((name, host_shown, port_shown, shown_id));
+                }
+            }
+        }
+        // V-058, V-059
         let is_pg = src.kind.is_none() || ctx.peek(&src.kind) == Some(SourceType::Postgres);
         let Some(pg) = src.postgres.as_ref().filter(|_| is_pg) else {
             continue;
@@ -269,6 +326,27 @@ pub(crate) fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut
             );
         }
     }
+    // V-120 (uniqueness per source server: host + port)
+    for (_, entries) in server_ids.iter().filter(|(_, n)| n.len() > 1) {
+        let names: Vec<&str> = entries.iter().map(|(n, _, _, _)| *n).collect();
+        for (name, host_shown, port_shown, id_shown) in entries {
+            ctx.error(
+                "V-120",
+                &At::config(
+                    pipeline_at(name)
+                        .path
+                        .key("source")
+                        .key("mysql")
+                        .key("server_id"),
+                ),
+                None,
+                format!(
+                    "server_id {id_shown} on server {host_shown}:{port_shown} is used by pipelines {}: a second binlog dump under one id ends the first",
+                    names.join(", ")
+                ),
+            );
+        }
+    }
     // V-059 (uniqueness per source database: host + port + db). An error when a sharing pipeline
     // manages the publication: it reconciles the publication to its own tables, dropping the
     // others', whose changes are then never published (Q-M5-14).
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`

Expected: PASS, every test binary (146 tests).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/build.rs \
  crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/src/merge.rs \
  crates/data-avalanche-config/src/mutability.rs \
  crates/data-avalanche-config/src/raw.rs \
  crates/data-avalanche-config/src/registry.rs \
  crates/data-avalanche-config/src/resolved.rs \
  crates/data-avalanche-config/src/rules/domain.rs \
  crates/data-avalanche-config/src/rules/identity.rs \
  crates/data-avalanche-config/tests/load.rs \
  crates/data-avalanche-config/tests/rules/domain.rs \
  crates/data-avalanche-config/tests/rules/identity.rs
git commit -m "feat(config): source.mysql.server_id (derived from id, V-120) and source.mysql.heartbeat (V-121)"
```

---

### Task 3: `mysql-84-nogtid` and `mysql-tls` (test infrastructure)

**Files:**
- Modify: `crates/data-avalanche-testkit/src/lib.rs`
- Modify: `test-infra/README.md`
- Modify: `test-infra/compose.yaml`
- Create: `test-infra/mysql-tls.sh`

**Interfaces:**
- Consumes:
  - `test-infra/compose.yaml` (M0), `data_avalanche_testkit::endpoints` (M0).
- Produces:
  - `data-avalanche-testkit/src/lib.rs`:
    - `pub const MYSQL_84_NOGTID: &str = "mysql://root:avalanche@127.0.0.1:53385/avalanche";`
    - `pub const MYSQL_TLS: &str = "mysql://root:avalanche@127.0.0.1:53386/avalanche";`

Two MySQL 8.4 services join the default profile (roadmap §5.2):

- `mysql-84-nogtid` (port 53385): GTIDs off, for `replication_type: binlog` on MySQL. TLS off
  too (`--tls-version=`), the one server that offers none, for `ssl_mode: prefer`'s fallback.
  Its health check uses the socket: without TLS, a TCP login's first `caching_sha2_password`
  exchange needs the server's RSA key, which `mysqladmin` doesn't ask for (`mysql_async` does).
- `mysql-tls` (port 53386): TLS required on every TCP connection. Its entrypoint
  (`mysql-tls.sh`) makes a throwaway CA, a server certificate for `localhost` only and a client
  certificate for `avalanche_cert` at every start. It writes the CA and the client pair to the
  git-ignored `test-infra/.tls/mysql/`; the server key stays in the container. Nothing is
  committed.

The test kit gains their endpoints. MariaDB's file/position mode needs no service:
`mariadb-118` serves both modes (the target `mariadb-118-binlog` reads it with
`replication_type: binlog`).

- [ ] **Step 1: Add the test kit's endpoints**

Change `crates/data-avalanche-testkit/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/lib.rs b/crates/data-avalanche-testkit/src/lib.rs
--- a/crates/data-avalanche-testkit/src/lib.rs
+++ b/crates/data-avalanche-testkit/src/lib.rs
@@ -123,6 +123,9 @@ pub mod endpoints {
     pub const POSTGRES_18: &str =
         "host=127.0.0.1 port=55418 user=avalanche password=avalanche dbname=avalanche";
     pub const MYSQL_84: &str = "mysql://root:avalanche@127.0.0.1:53384/avalanche";
+    pub const MYSQL_84_NOGTID: &str = "mysql://root:avalanche@127.0.0.1:53385/avalanche";
+    /// TLS required: connect with TLS (`require_ssl=true`).
+    pub const MYSQL_TLS: &str = "mysql://root:avalanche@127.0.0.1:53386/avalanche";
     pub const MARIADB_118: &str = "mysql://root:avalanche@127.0.0.1:53318/avalanche";
     pub const RUSTFS_URL: &str = "http://127.0.0.1:59000";
     pub const RUSTFS_ACCESS_KEY: &str = "avalanche";
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-testkit`

Expected: PASS, 27 tests. The constants are new and nothing reads them yet: the run passes.

- [ ] **Step 3: Add the services**

Change `test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -12,6 +12,8 @@ on `PATH`; it fetches DuckDB itself.
 | postgres-14…17 | postgres:14…17 | 55414…55417 | same | matrix |
 | postgres-tls | postgres:18, TLS required (`postgres-tls.sh`) | 55419 | avalanche / avalanche; `avalanche_cert` by client certificate | default |
 | mysql-84 | mysql:8.4 (GTID on) | 53384 | root / avalanche, db `avalanche` | default |
+| mysql-84-nogtid | mysql:8.4 (GTID off: `replication_type: binlog`; TLS off) | 53385 | same | default |
+| mysql-tls | mysql:8.4 (GTID on), TLS required (`mysql-tls.sh`) | 53386 | root / avalanche; `avalanche_cert` / `unused` with its client certificate | default |
 | mariadb-118 | mariadb:11.8 (`gtid_strict_mode=1`) | 53318 | root / avalanche | default |
 | mariadb-114, mariadb-1011 | mariadb:11.4, 10.11 | 53314, 53311 | root / avalanche | matrix |
 | rustfs (S3) | rustfs/rustfs:1.0.0 | 59000 | avalanche / avalanche-secret | default |
@@ -20,7 +22,9 @@ on `PATH`; it fetches DuckDB itself.
 Bucket / container `data-avalanche-test` is created by the `init` profile (`up.sh` runs it). Azurite keeps
 port 10000 because `object_store`'s emulator mode always targets `127.0.0.1:10000`.
 
-Added by a later milestone: `mysql-84-nogtid` (M7, file/position mode).
+`mysql-tls` writes its throwaway CA and client pair to `test-infra/.tls/mysql/` at each start; its
+server certificate names `localhost` only, so a `verify-full` connection to `127.0.0.1` fails by
+design.
 
 ## PostgreSQL
 
```

Change `test-infra/compose.yaml`:

```diff
diff --git a/test-infra/compose.yaml b/test-infra/compose.yaml
--- a/test-infra/compose.yaml
+++ b/test-infra/compose.yaml
@@ -39,6 +39,28 @@ services:
     ports: ["53384:3306"]
     healthcheck: { test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-uroot", "-pavalanche"], interval: 2s, retries: 60 }
 
+  # GTID off: the file/position mode (`replication_type: binlog`) on MySQL (M7a). TLS off too
+  # (`--tls-version=`): the one server here that offers none, for `ssl_mode: prefer`'s fallback.
+  mysql-84-nogtid:
+    image: "mysql:8.4"
+    environment: { MYSQL_ROOT_PASSWORD: avalanche, MYSQL_DATABASE: avalanche }
+    command: ["--server-id=1", "--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL",
+              "--binlog-row-metadata=FULL", "--gtid-mode=OFF", "--tls-version="]
+    ports: ["53385:3306"]
+    # Over the socket: without TLS, a TCP login's first caching_sha2_password exchange needs the
+    # server's RSA key, which mysqladmin doesn't ask for.
+    healthcheck: { test: ["CMD", "mysqladmin", "ping", "-uroot", "-pavalanche"], interval: 2s, retries: 60 }
+
+  # TLS required on every TCP connection, throwaway certificates made at start (mysql-tls.sh).
+  mysql-tls:
+    image: "mysql:8.4"
+    environment: { MYSQL_ROOT_PASSWORD: avalanche, MYSQL_DATABASE: avalanche }
+    entrypoint: ["bash", "/mysql-tls.sh"]
+    volumes: ["./mysql-tls.sh:/mysql-tls.sh:ro", "./.tls:/tls"]
+    ports: ["53386:3306"]
+    # mysqladmin connects over the socket, which TLS doesn't apply to.
+    healthcheck: { test: ["CMD", "mysqladmin", "ping", "-uroot", "-pavalanche"], interval: 2s, retries: 60 }
+
   mariadb-118: { <<: *mariadb, image: "mariadb:11.8", ports: ["53318:3306"] }
   mariadb-114: { <<: *mariadb, image: "mariadb:11.4", ports: ["53314:3306"], profiles: [matrix] }
   mariadb-1011: { <<: *mariadb, image: "mariadb:10.11", ports: ["53311:3306"], profiles: [matrix] }
```

Create `test-infra/mysql-tls.sh`:

```bash
#!/usr/bin/env bash
# Entrypoint of the `mysql-tls` service: a fresh throwaway CA, server certificate (localhost
# only, so a connection by 127.0.0.1 is a name mismatch) and client certificate (CN
# avalanche_cert) at every start, then MySQL 8.4 with TLS required for every TCP connection. The
# CA and the client pair are copied to /tls/mysql (bind-mounted from the git-ignored
# test-infra/.tls) for the tests; the server key stays in the container.
set -euo pipefail
server=/etc/mysql-tls
mkdir -p "$server" /tls/mysql
cd "$server"
rm -f ./*
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 30 \
  -subj "/CN=data-avalanche test CA" -keyout ca.key -out ca.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=localhost" \
  -keyout server.key -out server.csr
printf 'subjectAltName=DNS:localhost\n' > server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 \
  -extfile server.ext -out server.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=avalanche_cert" \
  -keyout client.key -out client.csr
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 -out client.crt
chown mysql:mysql server.key && chmod 600 server.key
# 644: on a Linux host the bind-mounted files are root-owned, and the tests read them as another
# user. Throwaway material, granting nothing the test password doesn't.
cp ca.crt client.crt client.key /tls/mysql/ && chmod 644 /tls/mysql/*
# The user the client certificate identifies: its password alone doesn't log in. The init scripts
# run once, on the first start; the user then persists with the data directory.
mkdir -p /docker-entrypoint-initdb.d
cat > /docker-entrypoint-initdb.d/cert-user.sql <<'SQL'
CREATE USER 'avalanche_cert'@'%' IDENTIFIED BY 'unused' REQUIRE SUBJECT '/CN=avalanche_cert';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'avalanche_cert'@'%';
SQL
exec docker-entrypoint.sh mysqld --server-id=1 --log-bin=mysql-bin --binlog-format=ROW \
  --binlog-row-image=FULL --binlog-row-metadata=FULL --gtid-mode=ON --enforce-gtid-consistency=ON \
  --ssl-ca="$server/ca.crt" --ssl-cert="$server/server.crt" --ssl-key="$server/server.key" \
  --require-secure-transport=ON
```

- [ ] **Step 4: Check it**

Run: `docker compose -f test-infra/compose.yaml up -d --wait mysql-84-nogtid mysql-tls`

Run: `docker compose -f test-infra/compose.yaml exec mysql-84-nogtid mysql -uroot -pavalanche -N -e "SELECT @@gtid_mode, CONCAT('[', @@tls_version, ']')"`

Run: `docker compose -f test-infra/compose.yaml exec mysql-tls mysql -uroot -pavalanche -N -e 'SELECT @@require_secure_transport'`

Run: `ls test-infra/.tls/mysql`

Expected: `OFF	[]` (GTIDs off, no TLS version), then `1`, then `ca.crt  client.crt  client.key` (the names only;
never print the files).

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-testkit/src/lib.rs \
  test-infra/README.md \
  test-infra/compose.yaml \
  test-infra/mysql-tls.sh
git commit -m "infra: test-infra — mysql-84-nogtid (file/position mode) and mysql-tls (TLS required, throwaway CA)"
```

---

### Task 4: The crate: connections, TLS, server flavour and the start checks

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/data-avalanche-source-mysql/Cargo.toml`
- Create: `crates/data-avalanche-source-mysql/src/connect.rs`
- Create: `crates/data-avalanche-source-mysql/src/connector.rs`
- Create: `crates/data-avalanche-source-mysql/src/error.rs`
- Create: `crates/data-avalanche-source-mysql/src/lib.rs`
- Create: `crates/data-avalanche-source-mysql/src/server.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/common.rs`
- Create: `crates/data-avalanche-source-mysql/tests/mysql/main.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/preflight.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/tls.rs`

**Interfaces:**
- Consumes:
  - `data_avalanche_config::{Pipeline, Mysql, Source, SslMode}` with `server_id` and `heartbeat`
    (Task 2); `data_avalanche_core::{SourceError, Preflight}`.
  - `data_avalanche_testkit::{require_it, install_crypto_provider}`; the services of Task 3.
- Produces:
  - `data-avalanche-source-mysql/src/connect.rs`:
    - `pub const SESSION_SETUP: [&str; 2] = ["SET time_zone = '+00:00'", "SET NAMES utf8mb4"];`
  - `data-avalanche-source-mysql/src/connector.rs`:
    - `pub struct MySqlConnector`
    - `pub fn new(pipeline: &Pipeline) -> Result<Self, SourceError>`
    - `pub async fn server_version(&self) -> Result<&str, SourceError>`
    - `pub async fn flavour(&self) -> Result<Flavour, SourceError>`
    - `pub async fn preflight(&self) -> Result<Preflight, SourceError>`
  - `data-avalanche-source-mysql/src/lib.rs`:
    - `pub mod server;`
  - `data-avalanche-source-mysql/src/server.rs`:
    - `pub enum Flavour`
    - `pub fn of(version: &str) -> Self`
    - `pub fn minimum(self) -> (u16, u16)`
    - `pub fn status_statement(self) -> &'static str`
    - `pub fn status_privilege(self) -> &'static str`
    - `pub fn version_of(version: &str) -> Option<(u16, u16)>`
    - `pub struct Settings`
    - `pub fn check_settings(`
    - `pub fn grants_replication(grants: &[String]) -> bool`

`data-avalanche-source-mysql`, its error classes, its connections and the pipeline preflight
(config §11.2, the MySQL rows).

- **Errors** (D-M7a.3): server codes a retry can outlive (1040, 1053, 1205, 1213, 1317, 1927,
  3169, 4031) are `Transient`. Error 1236 carries several meanings, told apart by the message:
  another dump under this replica id (MySQL; MariaDB says 4052) is `Contended`, a binlog the
  server no longer has is `PositionUnreachable`, any other is `Permanent`. I/O is `Transient`;
  TLS (a `rustls::Error` inside an I/O error, or a server that offers no TLS when the mode
  requires it) is `Permanent`.
- **Connections** (D-M7a.4): one catalog connection, shared, each command under
  `timeouts.command`, dropped after a fatal error or a timeout. Every session runs
  `SET time_zone = '+00:00'` and `SET NAMES utf8mb4`, which the decoders assume.
- **TLS** (D-M7a.5): `disable`, `prefer` (TLS when the server offers it, else plain: measured on
  `mysql-84-nogtid`), `require` (encrypt, no chain check), `verify-ca`, `verify-full`. Inline
  PEM is passed in memory. `mysql_async` 0.37.1 skips the name check only for an error whose
  text contains `NotValidForName`, and rustls 0.23 prints `certificate not valid for name`: so
  `verify-ca` checks the name too and fails closed on a mismatch. A test pins it, as M5 pinned
  the same limitation of the PostgreSQL replication connection.
- **Flavour and preflight** (D-M7a.6): `VERSION()` names MariaDB or MySQL. The minimums are
  MySQL 8.4 and MariaDB 10.11 (P-8). The binlog must be on, `binlog_format = ROW` and
  `binlog_row_image = FULL`, and GTIDs must be on for `replication_type: gtid` on MySQL; each
  failure halts the pipeline (`Permanent`). `binlog_row_metadata` other than `FULL` warns. The
  user needs the binlog status privilege (MySQL `REPLICATION CLIENT`, MariaDB `BINLOG MONITOR`),
  checked by running the status statement, and `REPLICATION SLAVE`, read from `SHOW GRANTS`
  (MySQL needs `USING CURRENT_ROLE()` to list a default role's grants; MariaDB lists them
  plainly, measured).

The integration binary `tests/mysql` starts here. Its target is `DATA_AVALANCHE_MYSQL`:
`mysql-84` (default), `mysql-84-nogtid`, `mariadb-118`, `mariadb-118-binlog`; the matrix adds
`mariadb-114` and `mariadb-1011`. Each test owns a database `it_<test>`, dropped and created again
at its start, and the pipeline `id` is that name, so the derived replica ids of tests running at
once differ. The TLS tests run with the `mysql-84` target only.

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
+data-avalanche-source-mysql = { path = "crates/data-avalanche-source-mysql" }
 data-avalanche-telemetry = { path = "crates/data-avalanche-telemetry" }
 data-avalanche-cluster = { path = "crates/data-avalanche-cluster" }
 thiserror = "2"
```

Create `crates/data-avalanche-source-mysql/Cargo.toml`:

```toml
[package]
name = "data-avalanche-source-mysql"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "The MySQL and MariaDB source: the binlog over mysql_async, catalog channels, locking keyset snapshots."

[lints]
workspace = true

[dependencies]
data-avalanche-config.workspace = true
data-avalanche-core.workspace = true
futures-util.workspace = true
mysql_async.workspace = true
rustls.workspace = true
tokio = { workspace = true, features = ["sync", "time"] }

[dev-dependencies]
data-avalanche-testkit.workspace = true
rcgen.workspace = true
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

Create `crates/data-avalanche-source-mysql/src/lib.rs`:

```rust
//! The MySQL and MariaDB realization of `data_avalanche_core::SourceConnector` (roadmap M7): the
//! row-based binlog over `mysql_async` in GTID or file/position mode (S-MY10), binlog values
//! decoded to `ColumnValue`s (`Type_Mapping.md`), catalog channels A, C and D over
//! `information_schema` (`Schema_Change…` §3) and keyset snapshot chunks read with a locking read
//! (`Snapshot_Resumability…` §1.1, DA-SNAP-10).

mod connect;
mod connector;
mod error;
pub mod server;

pub use connect::SESSION_SETUP;
pub use connector::MySqlConnector;
pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
```

Create `crates/data-avalanche-source-mysql/tests/mysql/main.rs`:

```rust
//! The MySQL and MariaDB source against a live server (`test-infra/compose.yaml`): the target is
//! `DATA_AVALANCHE_MYSQL` — `mysql-84` (default), `mysql-84-nogtid`, `mariadb-118` and
//! `mariadb-118-binlog` on every change, `mariadb-114` and `mariadb-1011` weekly. Each test
//! returns at once without `DATA_AVALANCHE_IT`.

mod common;
mod preflight;
mod tls;
```

Create `crates/data-avalanche-source-mysql/src/connect.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use data_avalanche_config::{FingerprintKey, load_str};
    use std::collections::BTreeMap;

    /// A source with `ssl` (YAML flow entries) and `env` for its `${…}` references.
    fn source(ssl: &str, env: &[(&str, &str)]) -> Source {
        let yaml = format!(
            r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: p
    source:
      type: mysql
      host: db.example.com
      port: 3306
      db: shop
      user: cdc
      password: hunter2
      {ssl}
      mysql: {{}}
    tables:
      include: [t]
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#
        );
        let env: BTreeMap<String, String> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let loaded = load_str(&yaml, &env, &FingerprintKey::from_bytes([1; 32]))
            .unwrap_or_else(|r| panic!("test pipeline: {r:?}"));
        loaded.config.pipelines.into_values().next().unwrap().source
    }

    fn ca() -> String {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(vec![]).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.self_signed(&key).unwrap().pem()
    }

    #[test]
    fn each_ssl_mode_maps_to_its_checks() {
        assert_eq!(ssl_opts(&source("ssl_mode: disable", &[])).unwrap(), None);
        for mode in ["prefer", "require"] {
            let o = ssl_opts(&source(&format!("ssl_mode: {mode}"), &[]))
                .unwrap()
                .unwrap();
            assert!(o.accept_invalid_certs(), "{mode}");
        }
        let ca = ca();
        let o = ssl_opts(&source(
            "ssl_mode: verify-ca\n      ssl_ca: \"${CA}\"",
            &[("CA", &ca)],
        ))
        .unwrap()
        .unwrap();
        assert!(!o.accept_invalid_certs());
        assert!(o.skip_domain_validation());
        assert!(
            o.disable_built_in_roots(),
            "ssl_ca replaces the Mozilla roots"
        );
        assert_eq!(o.root_certs(), &[ca.as_bytes().to_vec().into()]);
        let o = ssl_opts(&source(
            "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
            &[("CA", &ca)],
        ))
        .unwrap()
        .unwrap();
        assert!(!o.accept_invalid_certs() && !o.skip_domain_validation());
        // Without ssl_ca, the Mozilla roots.
        let o = ssl_opts(&source("ssl_mode: verify-full", &[]))
            .unwrap()
            .unwrap();
        assert!(!o.disable_built_in_roots() && o.root_certs().is_empty());
    }

    #[test]
    fn the_debug_form_leaves_out_the_password() {
        let c = Connector::new(&source("ssl_mode: require", &[])).unwrap();
        let text = format!("{c:?}");
        assert!(
            text.contains("db.example.com") && text.contains("cdc"),
            "{text}"
        );
        assert!(!text.contains("hunter2"), "{text}");
    }
}
```

Create `crates/data-avalanche-source-mysql/src/error.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::ServerError;

    fn server_error(code: u16, message: &str) -> Error {
        Error::Server(ServerError {
            code,
            message: message.into(),
            state: "HY000".into(),
        })
    }

    #[test]
    fn lock_waits_deadlocks_and_shutdowns_are_transient() {
        for code in [1040, 1053, 1205, 1213, 1317, 1927, 3169, 4031] {
            assert!(
                mysql(server_error(code, "x"), None).is_transient(),
                "{code}"
            );
        }
        let io = Error::Io(IoError::Io(std::io::Error::other("reset")));
        assert!(mysql(io, None).is_transient());
        assert!(mysql(Error::Driver(DriverError::ConnectionClosed), None).is_transient());
    }

    #[test]
    fn privileges_and_settings_are_permanent() {
        for (code, m) in [
            (1045, "Access denied for user 'u'@'%' (using password: YES)"),
            (
                1227,
                "Access denied; you need (at least one of) the REPLICATION CLIENT privilege(s)",
            ),
            (3159, "Connections using insecure transport are prohibited"),
        ] {
            assert_eq!(
                mysql(server_error(code, m), None),
                SourceError::Permanent(format!("{m} (error {code})"))
            );
        }
        assert!(matches!(
            mysql(Error::Driver(DriverError::NoClientSslFlagFromServer), None),
            SourceError::Permanent(m) if m.starts_with("TLS:")
        ));
    }

    #[test]
    fn a_tls_error_wrapped_in_io_is_permanent() {
        let io = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
        );
        assert!(matches!(
            mysql(Error::Io(IoError::Io(io)), None),
            SourceError::Permanent(m) if m.starts_with("TLS:") && m.contains("UnknownIssuer")
        ));
    }

    #[test]
    fn another_dump_with_the_replica_id_is_contended() {
        // S-MY10h: the older dump ends when a newer one registers the same replica id.
        let mysql_kick = "A replica with the same server_uuid/server_id as this replica has \
                          connected to the source; the first event 'mysql-bin.000018' at 593";
        let maria_kick = "A slave with the same server_id is already connected";
        assert!(matches!(
            mysql(server_error(1236, mysql_kick), Some("x")),
            SourceError::Contended(_)
        ));
        assert!(matches!(
            mysql(server_error(4052, maria_kick), None),
            SourceError::Contended(_)
        ));
    }

    #[test]
    fn a_purged_binlog_is_an_unreachable_position() {
        for m in [
            "Could not find first log file name in binary log index file",
            "Cannot replicate because the source purged required binary logs. Replicate the \
             missing transactions from elsewhere",
            "Could not find GTID state requested by slave in any binlog files",
        ] {
            assert_eq!(
                mysql(server_error(1236, m), Some("mysql-bin.000017:1193")),
                SourceError::PositionUnreachable {
                    position: "mysql-bin.000017:1193".into(),
                    detail: format!("{m} (error 1236)"),
                }
            );
        }
        // Another 1236 is neither.
        assert!(matches!(
            mysql(
                server_error(1236, "log event entry exceeded max_allowed_packet"),
                None
            ),
            SourceError::Permanent(_)
        ));
    }
}
```

Create `crates/data-avalanche-source-mysql/src/server.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn settings(version: &str) -> Settings {
        Settings {
            version: version.into(),
            log_bin: true,
            binlog_format: "ROW".into(),
            binlog_row_image: "FULL".into(),
            binlog_row_metadata: Some("FULL".into()),
            gtid_mode: Flavour::of(version)
                .eq(&Flavour::Mysql)
                .then(|| "ON".into()),
        }
    }

    #[test]
    fn the_flavour_and_version_come_from_the_version_string() {
        assert_eq!(Flavour::of("8.4.6"), Flavour::Mysql);
        assert_eq!(Flavour::of("11.8.3-MariaDB-ubu2404-log"), Flavour::Mariadb);
        assert_eq!(version_of("8.4.6"), Some((8, 4)));
        assert_eq!(version_of("10.11.14-MariaDB-log"), Some((10, 11)));
        assert_eq!(version_of("8"), None);
    }

    #[test]
    fn supported_servers_with_the_settings_pass() {
        for (v, f) in [
            ("8.4.6", Flavour::Mysql),
            ("9.4.0", Flavour::Mysql),
            ("10.11.14-MariaDB", Flavour::Mariadb),
            ("11.8.3-MariaDB-log", Flavour::Mariadb),
        ] {
            for r in [ReplicationType::Gtid, ReplicationType::Binlog] {
                assert_eq!(check_settings(f, r, &settings(v)), Ok(vec![]), "{v}");
            }
        }
    }

    #[test]
    fn old_servers_and_wrong_settings_refuse_the_pipeline() {
        let refused = |f, r, s: Settings, needle: &str| {
            let e = check_settings(f, r, &s).unwrap_err();
            assert!(e.contains(needle), "{e}");
        };
        let (my, maria, gtid, binlog) = (
            Flavour::Mysql,
            Flavour::Mariadb,
            ReplicationType::Gtid,
            ReplicationType::Binlog,
        );
        refused(my, gtid, settings("8.0.39"), "needs MySQL 8.4 or newer");
        refused(
            maria,
            gtid,
            settings("10.6.21-MariaDB"),
            "needs MariaDB 10.11",
        );
        let mut s = settings("8.4.6");
        s.log_bin = false;
        refused(my, binlog, s, "log_bin");
        let mut s = settings("8.4.6");
        s.binlog_format = "MIXED".into();
        refused(my, binlog, s, "binlog_format = ROW");
        let mut s = settings("11.8.3-MariaDB");
        s.binlog_row_image = "MINIMAL".into();
        refused(maria, gtid, s, "binlog_row_image = FULL");
        let mut s = settings("8.4.6");
        s.gtid_mode = Some("ON_PERMISSIVE".into());
        refused(my, gtid, s.clone(), "gtid_mode = ON");
        // File/position mode reads a server without GTIDs.
        assert_eq!(check_settings(my, binlog, &s), Ok(vec![]));
    }

    #[test]
    fn minimal_row_metadata_warns() {
        let mut s = settings("8.4.6");
        s.binlog_row_metadata = Some("MINIMAL".into());
        let w = check_settings(Flavour::Mysql, ReplicationType::Gtid, &s).unwrap();
        assert!(w[0].contains("binlog_row_metadata is MINIMAL"), "{w:?}");
    }

    #[test]
    fn replication_comes_from_a_global_grant() {
        let g = |lines: &[&str]| {
            grants_replication(&lines.iter().map(|l| l.to_string()).collect::<Vec<_>>())
        };
        assert!(g(&[
            "GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION"
        ]));
        assert!(g(&[
            "GRANT USAGE ON *.* TO `u`@`%`",
            "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `u`@`%`",
        ]));
        assert!(!g(&["GRANT SELECT, REPLICATION CLIENT ON *.* TO `u`@`%`"]));
        // A database grant can't hold it.
        assert!(!g(&["GRANT ALL PRIVILEGES ON `shop`.* TO `u`@`%`"]));
    }
}
```

Create `crates/data-avalanche-source-mysql/tests/mysql/common.rs`:

```rust
//! Shared fixtures. The server under test is `DATA_AVALANCHE_MYSQL` (default `mysql-84`); each
//! test owns a database `it_<test>`, dropped and re-created at its start, so a failed run leaves
//! nothing that breaks the next one. The pipeline id is the database name, so the derived replica
//! ids of tests running at once differ (V-120).

use std::collections::BTreeMap;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_source_mysql::{Flavour, MySqlConnector};
use mysql_async::Conn;
use mysql_async::prelude::Queryable;

/// A server of `test-infra/compose.yaml` and the replication mode the tests use on it.
#[derive(Debug, Clone, Copy)]
pub struct Target {
    pub name: &'static str,
    pub port: u16,
    pub flavour: Flavour,
    /// `gtid` or `binlog`.
    pub replication: &'static str,
}

const TARGETS: [Target; 6] = [
    Target {
        name: "mysql-84",
        port: 53384,
        flavour: Flavour::Mysql,
        replication: "gtid",
    },
    Target {
        name: "mysql-84-nogtid",
        port: 53385,
        flavour: Flavour::Mysql,
        replication: "binlog",
    },
    Target {
        name: "mariadb-118",
        port: 53318,
        flavour: Flavour::Mariadb,
        replication: "gtid",
    },
    Target {
        name: "mariadb-118-binlog",
        port: 53318,
        flavour: Flavour::Mariadb,
        replication: "binlog",
    },
    Target {
        name: "mariadb-114",
        port: 53314,
        flavour: Flavour::Mariadb,
        replication: "gtid",
    },
    Target {
        name: "mariadb-1011",
        port: 53311,
        flavour: Flavour::Mariadb,
        replication: "gtid",
    },
];

/// `DATA_AVALANCHE_MYSQL`, else `mysql-84`.
pub fn target() -> Target {
    let name = std::env::var("DATA_AVALANCHE_MYSQL").unwrap_or_else(|_| "mysql-84".into());
    TARGETS
        .into_iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| panic!("DATA_AVALANCHE_MYSQL={name}: not a test-infra MySQL target"))
}

/// A root connection to the target for test setup, in `db`.
pub async fn admin_in(db: &str) -> Conn {
    let url = format!("mysql://root:avalanche@127.0.0.1:{}/{db}", target().port);
    Conn::from_url(url)
        .await
        .expect("connect to the test server")
}

/// The test's database `it_<test>`.
pub struct Names {
    pub db: String,
}

impl Names {
    pub fn new(test: &str) -> Self {
        Self {
            db: format!("it_{test}"),
        }
    }
}

/// Drops what an earlier run of `test` left and creates its database again, then runs `ddl` in
/// it. Returns the names and a root connection in the database.
pub async fn fresh(test: &str, ddl: &str) -> (Names, Conn) {
    let n = Names::new(test);
    let mut a = admin_in("avalanche").await;
    a.query_drop(format!(
        "DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db}",
        db = n.db
    ))
    .await
    .expect("the test database");
    drop(a);
    let mut a = admin_in(&n.db).await;
    if !ddl.is_empty() {
        a.query_drop(ddl).await.expect("test DDL");
    }
    (n, a)
}

/// A pipeline `n.db` on the target as `user`, reading `include`, with more `source` keys (YAML,
/// indented six spaces), `mysql` keys (YAML flow entries; a `replication_type` there replaces the
/// target's) and `tables` keys (indented six).
pub fn pipeline_as(
    n: &Names,
    user: &str,
    source: &str,
    mysql: &str,
    include: &[&str],
    tables: &str,
) -> Pipeline {
    let t = target();
    let replication = match mysql.contains("replication_type") {
        true => String::new(),
        false => format!("replication_type: {}, ", t.replication),
    };
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: {db}
    source:
      type: mysql
      host: 127.0.0.1
      port: {port}
      db: {db}
      user: {user}
      password: avalanche
{source}
      mysql: {{ {replication}heartbeat: 1s{mysql} }}
    tables:
      include: [{include}]
{tables}
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#,
        db = n.db,
        port = t.port,
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

/// A pipeline as root without TLS.
pub fn pipeline(n: &Names, include: &[&str], tables: &str, mysql: &str) -> Pipeline {
    pipeline_as(n, "root", "      ssl_mode: disable", mysql, include, tables)
}

pub fn connector(p: &Pipeline) -> MySqlConnector {
    data_avalanche_testkit::install_crypto_provider();
    MySqlConnector::new(p).expect("connector")
}

/// The test's database, dropped at the end of a passing test.
pub async fn cleanup(n: &Names) {
    let mut a = admin_in("avalanche").await;
    a.query_drop(format!("DROP DATABASE IF EXISTS {}", n.db))
        .await
        .expect("drop the test database");
}
```

Create `crates/data-avalanche-source-mysql/tests/mysql/preflight.rs`:

```rust
//! The pipeline start checks (config §11.2, the MySQL rows) against the target server: its
//! settings pass, and a user without the replication privileges is refused, naming the one it
//! lacks.

use data_avalanche_core::SourceError;
use data_avalanche_source_mysql::Flavour;
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{Names, cleanup, connector, fresh, pipeline, pipeline_as, target};

#[tokio::test]
async fn the_test_server_passes_preflight() {
    require_it!();
    let (n, _a) = fresh("preflight_ok", "").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let p = c.preflight().await.expect("preflight");
    assert_eq!(c.flavour().await.unwrap(), target().flavour);
    assert_eq!(p.server_version, c.server_version().await.unwrap());
    assert_eq!(p.warnings, Vec::<String>::new());
    cleanup(&n).await;
}

/// MySQL without GTIDs refuses `replication_type: gtid` (file/position mode reads it).
#[tokio::test]
async fn gtid_replication_needs_gtid_mode_on() {
    require_it!();
    if target().name != "mysql-84-nogtid" {
        eprintln!("skipped: needs the mysql-84-nogtid target");
        return;
    }
    let (n, _a) = fresh("preflight_gtid_off", "").await;
    let c = connector(&pipeline(&n, &["t"], "", ", replication_type: gtid"));
    match c.preflight().await {
        Err(SourceError::Permanent(m)) => assert!(m.contains("gtid_mode is OFF"), "{m}"),
        other => panic!("refused expected, got {other:?}"),
    }
    cleanup(&n).await;
}

/// A user `it_<test>` with `grants` (and `extra` statements after them), password `avalanche`.
async fn user(a: &mut mysql_async::Conn, name: &str, grants: &str, extra: &[String]) {
    a.query_drop(format!("DROP USER IF EXISTS '{name}'@'%'"))
        .await
        .unwrap();
    a.query_drop(format!(
        "CREATE USER '{name}'@'%' IDENTIFIED BY 'avalanche'"
    ))
    .await
    .unwrap();
    a.query_drop(format!("GRANT {grants} TO '{name}'@'%'"))
        .await
        .unwrap();
    for s in extra {
        a.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
    }
}

#[tokio::test]
async fn a_user_without_replication_privileges_is_refused() {
    require_it!();
    let (n, mut a) = fresh("preflight_grants", "").await;
    let name = "it_preflight_grants";
    let status_privilege = match target().flavour {
        Flavour::Mysql => "REPLICATION CLIENT",
        Flavour::Mariadb => "BINLOG MONITOR",
    };
    for (grants, refusal) in [
        (format!("SELECT ON {}.* ", n.db), Some(status_privilege)),
        (
            "SELECT, REPLICATION CLIENT ON *.*".to_string(),
            Some("REPLICATION SLAVE"),
        ),
        (
            "SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.*".to_string(),
            None,
        ),
    ] {
        user(&mut a, name, &grants, &[]).await;
        let c = connector(&pipeline_as(
            &n,
            name,
            "      ssl_mode: disable",
            "",
            &["t"],
            "",
        ));
        match (c.preflight().await, refusal) {
            (Ok(_), None) => {}
            (Err(SourceError::Permanent(m)), Some(lacking)) => {
                assert!(m.contains(lacking), "{grants}: {m}")
            }
            (other, _) => panic!("{grants}: {other:?}"),
        }
    }
    a.query_drop(format!("DROP USER '{name}'@'%'"))
        .await
        .unwrap();
    cleanup(&n).await;
}

/// The privileges a role grants count when the role is active at login (a default role).
#[tokio::test]
async fn replication_granted_through_a_default_role_passes() {
    require_it!();
    let (n, mut a) = fresh("preflight_role", "").await;
    let (name, role) = ("it_preflight_role", "it_preflight_role_r");
    a.query_drop(format!("DROP ROLE IF EXISTS {role}"))
        .await
        .unwrap();
    a.query_drop(format!("CREATE ROLE {role}")).await.unwrap();
    a.query_drop(format!(
        "GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO {role}"
    ))
    .await
    .unwrap();
    let default_role = match target().flavour {
        Flavour::Mysql => format!("SET DEFAULT ROLE {role} TO '{name}'@'%'"),
        Flavour::Mariadb => format!("SET DEFAULT ROLE {role} FOR '{name}'@'%'"),
    };
    user(&mut a, name, role, &[default_role]).await;
    let c = connector(&pipeline_as(
        &n,
        name,
        "      ssl_mode: disable",
        "",
        &["t"],
        "",
    ));
    c.preflight().await.expect("the role's privileges count");
    a.query_drop(format!("DROP USER '{name}'@'%'"))
        .await
        .unwrap();
    a.query_drop(format!("DROP ROLE {role}")).await.unwrap();
    cleanup(&n).await;
}

/// `ssl_mode: prefer` connects without TLS to a server that offers none, and `require` is refused
/// there (`mysql-84-nogtid` runs with TLS off).
#[tokio::test]
async fn prefer_falls_back_to_plain_and_require_is_refused() {
    require_it!();
    if target().name != "mysql-84-nogtid" {
        eprintln!("skipped: needs the mysql-84-nogtid target (TLS off)");
        return;
    }
    let (n, _a) = fresh("preflight_prefer", "").await;
    let source = |mode: &str| format!("      ssl_mode: {mode}");
    let c = connector(&pipeline_as(&n, "root", &source("prefer"), "", &["t"], ""));
    c.preflight().await.expect("prefer connects without TLS");
    let c = connector(&pipeline_as(&n, "root", &source("require"), "", &["t"], ""));
    match c.preflight().await {
        Err(SourceError::Permanent(m)) => assert!(m.contains("doesn't offer TLS"), "{m}"),
        other => panic!("refused expected, got {other:?}"),
    }
    cleanup(&n).await;
}

#[test]
fn names_are_per_test() {
    assert_eq!(Names::new("x").db, "it_x");
}
```

Create `crates/data-avalanche-source-mysql/tests/mysql/tls.rs`:

```rust
//! TLS against `mysql-tls`, which refuses any TCP connection without TLS
//! (`require_secure_transport`): every connection that logs in there is encrypted. Its throwaway
//! CA and client certificate are made when the container starts and read from the git-ignored
//! `test-infra/.tls/mysql`. These run with the default `mysql-84` target only.

use std::collections::BTreeMap;
use std::path::PathBuf;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::SourceError;
use data_avalanche_testkit::require_it;

use crate::common::{connector, target};

const PORT: u16 = 53386;

fn material(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-infra/.tls/mysql")
        .join(name)
}

fn read(name: &str) -> String {
    std::fs::read_to_string(material(name))
        .unwrap_or_else(|e| panic!("{name} from test-infra/.tls/mysql (is mysql-tls up?): {e}"))
}

fn default_target() -> bool {
    if target().name == "mysql-84" {
        return true;
    }
    eprintln!("skipped: the TLS tests run with the mysql-84 target");
    false
}

/// A pipeline on `mysql-tls` at `host` as `user`/`password`, with `ssl` (YAML, indented six
/// spaces) and `env` for its `${…}` references. The server certificate names `localhost` only.
fn pipeline(host: &str, user: &str, password: &str, ssl: &str, env: &[(&str, String)]) -> Pipeline {
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: it_tls
    source:
      type: mysql
      host: {host}
      port: {PORT}
      db: avalanche
      user: {user}
      password: {password}
      {ssl}
      mysql: {{ replication_type: gtid }}
    tables:
      include: [t]
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#
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

#[tokio::test]
async fn verify_full_with_an_inline_ca_connects() {
    require_it!();
    if !default_target() {
        return;
    }
    let p = pipeline(
        "localhost",
        "root",
        "avalanche",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
        &[("CA", read("ca.crt"))],
    );
    connector(&p).preflight().await.expect("preflight over TLS");
}

#[tokio::test]
async fn a_client_certificate_with_an_inline_key_authenticates() {
    require_it!();
    if !default_target() {
        return;
    }
    // `avalanche_cert` logs in only with the certificate whose subject it requires.
    let p = pipeline(
        "localhost",
        "avalanche_cert",
        "unused",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"\n      ssl_cert: \"${CERT}\"\n      ssl_key: \"${KEY}\"",
        &[
            ("CA", read("ca.crt")),
            ("CERT", read("client.crt")),
            ("KEY", read("client.key")),
        ],
    );
    connector(&p).preflight().await.expect("certificate login");
    let without = pipeline(
        "localhost",
        "avalanche_cert",
        "unused",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
        &[("CA", read("ca.crt"))],
    );
    match connector(&without).preflight().await {
        Err(SourceError::Permanent(m)) => assert!(m.contains("(error 1045)"), "{m}"),
        other => panic!("refused expected without the certificate, got {other:?}"),
    }
}

/// `verify-ca` should check the chain and not the name, but mysql_async 0.37.1 forgives a name
/// mismatch only when rustls's message contains `NotValidForName`, and rustls 0.23 prints
/// "certificate not valid for name": `verify-ca` behaves like `verify-full` (D-M7a.6, as PG's
/// replication connection, M5 Deviations Task 2). This pins it: once the crate forgives the
/// mismatch, the connection by `127.0.0.1` (not in the certificate) succeeds and this test fails.
#[tokio::test]
async fn verify_ca_by_an_unnamed_address_is_refused_like_verify_full() {
    require_it!();
    if !default_target() {
        return;
    }
    for mode in ["verify-ca", "verify-full"] {
        let p = pipeline(
            "127.0.0.1",
            "root",
            "avalanche",
            &format!("ssl_mode: {mode}\n      ssl_ca: \"${{CA}}\""),
            &[("CA", read("ca.crt"))],
        );
        match connector(&p).preflight().await {
            Err(SourceError::Permanent(m)) => {
                assert!(
                    m.starts_with("TLS:") && m.contains("not valid for name"),
                    "{mode}: {m}"
                )
            }
            other => panic!("{mode}: refused expected, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_wrong_ca_or_no_tls_is_refused() {
    require_it!();
    if !default_target() {
        return;
    }
    let other_ca = {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(vec![]).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.self_signed(&key).unwrap().pem()
    };
    for (what, ssl, env, reason) in [
        (
            "a foreign CA",
            "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
            vec![("CA", other_ca.clone())],
            "UnknownIssuer",
        ),
        (
            "a foreign CA, chain only",
            "ssl_mode: verify-ca\n      ssl_ca: \"${CA}\"",
            vec![("CA", other_ca)],
            "UnknownIssuer",
        ),
        ("no TLS", "ssl_mode: disable", vec![], "(error 3159)"),
    ] {
        let c = connector(&pipeline("localhost", "root", "avalanche", ssl, &env));
        match c.preflight().await {
            Err(SourceError::Permanent(detail)) => {
                assert!(detail.contains(reason), "{what}: {detail}")
            }
            other => panic!("{what}: a permanent error expected, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0583]: file not found for module `connector`
error[E0432]: unresolved import `connect::SESSION_SETUP`
error[E0432]: unresolved imports `server::Flavour`, `server::Settings`, `server::check_settings`, `server::grants_replication`, `server::version_of`
error[E0425]: cannot find type `Source` in this scope
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-source-mysql/src/connect.rs`, add above the test module:

```rust
//! Connections to the source (config §4.4): options from the resolved `source`, TLS from the
//! `ssl_*` PEM text (in memory: `mysql_async` reads a buffer as well as a file), the session
//! settings every connection runs with, and the shared catalog connection.

use std::fmt;
use std::time::Duration;

use data_avalanche_config::{Source, SslMode};
use data_avalanche_core::SourceError;
use futures_util::future::BoxFuture;
use mysql_async::{ClientIdentity, Conn, DriverError, Error, Opts, OptsBuilder, SslOpts};
use tokio::sync::Mutex;

use crate::error::{mysql, timed_out};

/// What every connection runs first: UTC for `TIMESTAMP` values (a snapshot reads them as text in
/// the session time zone, the binlog carries them in UTC), and UTF-8 results whatever the server's
/// handshake default is.
pub const SESSION_SETUP: [&str; 2] = ["SET time_zone = '+00:00'", "SET NAMES utf8mb4"];

/// The `ssl_mode` → `mysql_async` TLS options (`allow` is refused for MySQL by V-030 and connects
/// like `prefer`). `require` and `prefer` encrypt without verifying, `verify-ca` checks the chain
/// and `verify-full` the chain and the host name. Roots are `ssl_ca`, else the Mozilla roots.
pub(crate) fn ssl_opts(source: &Source) -> Result<Option<SslOpts>, String> {
    if source.ssl_mode == SslMode::Disable {
        return Ok(None);
    }
    let mut opts = SslOpts::default();
    if let Some(ca) = &source.ssl_ca {
        opts = opts
            .with_root_certs(vec![ca.text.expose().as_bytes().to_vec().into()])
            .with_disable_built_in_roots(true);
    }
    match (&source.ssl_cert, &source.ssl_key) {
        (Some(cert), Some(key)) => {
            opts = opts.with_client_identity(Some(ClientIdentity::new(
                cert.text.expose().as_bytes().to_vec().into(),
                key.text.expose().as_bytes().to_vec().into(),
            )));
        }
        (None, None) => {}
        _ => return Err("ssl_cert and ssl_key go together".into()),
    }
    Ok(Some(match source.ssl_mode {
        SslMode::VerifyFull => opts,
        SslMode::VerifyCa => opts.with_danger_skip_domain_validation(true),
        _ => opts.with_danger_accept_invalid_certs(true),
    }))
}

/// What the source's connections need, built once from `source`. `Debug` leaves out the password.
pub(crate) struct Connector {
    opts: Opts,
    /// `prefer` falls back to a connection without TLS when the server offers none.
    prefer: bool,
    pub(crate) connect: Duration,
    pub(crate) command: Duration,
    catalog: Mutex<Option<Conn>>,
}

impl fmt::Debug for Connector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connector")
            .field("host", &self.opts.ip_or_hostname())
            .field("port", &self.opts.tcp_port())
            .field("user", &self.opts.user())
            .field("db", &self.opts.db_name())
            .field("tls", &self.opts.ssl_opts().is_some())
            .finish_non_exhaustive()
    }
}

impl Connector {
    pub(crate) fn new(source: &Source) -> Result<Self, SourceError> {
        let ssl = ssl_opts(source).map_err(SourceError::Permanent)?;
        let opts = OptsBuilder::default()
            .ip_or_hostname(source.host.clone())
            .tcp_port(source.port)
            .db_name(Some(source.db.clone()))
            .user(Some(source.credentials.user()))
            .pass(Some(source.credentials.password().expose()))
            .init(SESSION_SETUP.to_vec())
            .ssl_opts(ssl);
        Ok(Self {
            opts: opts.into(),
            prefer: matches!(source.ssl_mode, SslMode::Prefer | SslMode::Allow),
            connect: source.timeouts.connect,
            command: source.timeouts.command,
            catalog: Mutex::new(None),
        })
    }

    /// A new connection within `timeouts.connect`: a server that accepts the TCP connection but
    /// never answers the handshake would otherwise hold it forever (DA-SRC-16).
    pub(crate) async fn connect(&self) -> Result<Conn, SourceError> {
        let after = self.connect;
        tokio::time::timeout(after, self.open())
            .await
            .map_err(|_| timed_out("connect to the source", after))?
    }

    async fn open(&self) -> Result<Conn, SourceError> {
        match Conn::new(self.opts.clone()).await {
            Err(Error::Driver(DriverError::NoClientSslFlagFromServer)) if self.prefer => {
                let plain = OptsBuilder::from_opts(self.opts.clone()).ssl_opts(None);
                Conn::new(plain).await.map_err(|e| mysql(e, None))
            }
            other => other.map_err(|e| mysql(e, None)),
        }
    }

    /// Runs `f` on the shared catalog connection under `timeouts.command`, connecting first when
    /// there is none. A connection that failed or timed out is dropped: the next call opens
    /// another.
    pub(crate) async fn command<T>(
        &self,
        what: &str,
        f: impl for<'c> FnOnce(&'c mut Conn) -> BoxFuture<'c, mysql_async::Result<T>>,
    ) -> Result<T, SourceError> {
        let mut slot = self.catalog.lock().await;
        if slot.is_none() {
            *slot = Some(self.connect().await?);
        }
        let conn = slot.as_mut().expect("connected above");
        match tokio::time::timeout(self.command, f(conn)).await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                if e.is_fatal() {
                    *slot = None;
                }
                Err(mysql(e, None))
            }
            Err(_) => {
                *slot = None;
                Err(timed_out(what, self.command))
            }
        }
    }
}

```

Create `crates/data-avalanche-source-mysql/src/connector.rs`:

```rust
//! `MySqlConnector`: the MySQL or MariaDB source of one pipeline.

use data_avalanche_config::{Mysql, Pipeline, SourceType};
use data_avalanche_core::{Preflight, SourceError};
use mysql_async::prelude::Queryable;
use tokio::sync::OnceCell;

use crate::connect::Connector;
use crate::server::{Flavour, Settings, check_settings, grants_replication};

/// The MySQL or MariaDB source of one pipeline (roadmap §3.6). Built from the resolved pipeline
/// configuration; connections open on first use.
#[derive(Debug)]
pub struct MySqlConnector {
    pub(crate) mysql: Mysql,
    pub(crate) conn: Connector,
    /// `VERSION()`, read once: the dialect every later statement depends on.
    version: OnceCell<String>,
}

impl MySqlConnector {
    /// Refuses a pipeline whose source isn't MySQL, and TLS material that doesn't parse.
    pub fn new(pipeline: &Pipeline) -> Result<Self, SourceError> {
        let source = &pipeline.source;
        let mysql = match (source.kind, &source.mysql) {
            (SourceType::Mysql, Some(m)) => m.clone(),
            _ => {
                return Err(SourceError::Permanent(format!(
                    "pipeline {}: the source isn't MySQL",
                    pipeline.name
                )));
            }
        };
        Ok(Self {
            mysql,
            conn: Connector::new(source)?,
            version: OnceCell::new(),
        })
    }

    /// The server's `VERSION()`.
    pub async fn server_version(&self) -> Result<&str, SourceError> {
        self.version
            .get_or_try_init(|| {
                self.conn.command("read the server version", |c| {
                    Box::pin(async move {
                        let v: Option<String> = c.query_first("SELECT VERSION()").await?;
                        Ok(v.unwrap_or_default())
                    })
                })
            })
            .await
            .map(String::as_str)
    }

    /// MySQL or MariaDB.
    pub async fn flavour(&self) -> Result<Flavour, SourceError> {
        Ok(Flavour::of(self.server_version().await?))
    }

    /// The pipeline-level start checks (config §11.2, the MySQL rows): a supported server
    /// (P-8) with the binary log on, `binlog_format = ROW`, `binlog_row_image = FULL`, GTIDs on for
    /// `replication_type: gtid` on MySQL, and a user that can read binlog positions (`REPLICATION
    /// CLIENT`) and dump the binlog (`REPLICATION SLAVE`). `binlog_row_metadata` other than FULL
    /// warns.
    pub async fn preflight(&self) -> Result<Preflight, SourceError> {
        let flavour = self.flavour().await?;
        let settings = self
            .conn
            .command("preflight", move |c| {
                Box::pin(async move {
                    let gtid_mode = match flavour {
                        Flavour::Mysql => "@@gtid_mode",
                        Flavour::Mariadb => "NULL",
                    };
                    let row: Option<mysql_async::Row> = c
                        .query_first(format!(
                            "SELECT VERSION(), @@log_bin, @@binlog_format, @@binlog_row_image,
                                    @@binlog_row_metadata, {gtid_mode}"
                        ))
                        .await?;
                    Ok(row.map(|r| Settings {
                        version: r.get(0).unwrap_or_default(),
                        log_bin: r.get::<i64, _>(1) == Some(1),
                        binlog_format: r.get(2).unwrap_or_default(),
                        binlog_row_image: r.get(3).unwrap_or_default(),
                        binlog_row_metadata: r.get::<Option<String>, _>(4).flatten(),
                        gtid_mode: r.get::<Option<String>, _>(5).flatten(),
                    }))
                })
            })
            .await?
            .ok_or_else(|| SourceError::Permanent("the server returned no settings".into()))?;
        let warnings = check_settings(flavour, self.mysql.replication_type, &settings)
            .map_err(SourceError::Permanent)?;
        let status = flavour.status_statement();
        let refused = self
            .conn
            .command("preflight", move |c| {
                Box::pin(async move {
                    match c.query_drop(status).await {
                        Ok(()) => Ok(None),
                        Err(mysql_async::Error::Server(e)) => Ok(Some(e.message)),
                        Err(e) => Err(e),
                    }
                })
            })
            .await?;
        if let Some(why) = refused {
            return Err(SourceError::Permanent(format!(
                "the user can't read the binlog position ({status}: {why}); grant {}",
                flavour.status_privilege()
            )));
        }
        if !self.grants_replication(flavour).await? {
            return Err(SourceError::Permanent(
                "the user has no REPLICATION SLAVE privilege on *.*; the binlog dump needs it"
                    .into(),
            ));
        }
        Ok(Preflight {
            server_version: settings.version,
            warnings,
        })
    }

    /// `REPLICATION SLAVE` from the user's grants, or from its active roles' (MySQL `SHOW GRANTS`
    /// lists a role's privileges only with `USING`).
    async fn grants_replication(&self, flavour: Flavour) -> Result<bool, SourceError> {
        self.conn
            .command("read the user's grants", move |c| {
                Box::pin(async move {
                    let grants: Vec<String> = c.query("SHOW GRANTS").await?;
                    if grants_replication(&grants) || flavour == Flavour::Mariadb {
                        return Ok(grants_replication(&grants));
                    }
                    let roles: Option<String> = c.query_first("SELECT CURRENT_ROLE()").await?;
                    match roles.filter(|r| r != "NONE") {
                        None => Ok(false),
                        Some(roles) => {
                            let grants: Vec<String> = c
                                .query(format!("SHOW GRANTS FOR CURRENT_USER() USING {roles}"))
                                .await?;
                            Ok(grants_replication(&grants))
                        }
                    }
                })
            })
            .await
    }
}
```

In `crates/data-avalanche-source-mysql/src/error.rs`, add above the test module:

```rust
//! Error classes (Data-Flow §Error Paths): which server and client failures are worth a retry.

use data_avalanche_core::SourceError;
use mysql_async::{DriverError, Error, IoError};

/// Server error codes a reconnect or a retry can outlive: a lock wait timeout (1205) or deadlock
/// (1213), too many connections (1040), a shutdown in progress (1053), a killed query or
/// connection (1317, 1927 MariaDB, 3169 MySQL), and the server's own inactivity disconnect (4031).
fn transient_code(code: u16) -> bool {
    matches!(code, 1040 | 1053 | 1205 | 1213 | 1317 | 1927 | 3169 | 4031)
}

/// A server error. Error 1236 carries several meanings, told apart by the message: another binlog
/// dump took over this pipeline's replica id (MySQL; MariaDB says it with 4052), or the requested
/// binlog is gone. The purge prechecks in `prepare` catch a gone binlog first; this is the
/// fallback when it goes between the check and the dump. `position` names the stream position a
/// purge is about.
pub(crate) fn server(code: u16, message: &str, position: Option<&str>) -> SourceError {
    let text = format!("{message} (error {code})");
    match code {
        4052 => SourceError::Contended(text),
        1236 if message.contains("same server_uuid/server_id") => SourceError::Contended(text),
        1236 if purged(message) => SourceError::PositionUnreachable {
            position: position.unwrap_or_default().to_owned(),
            detail: text,
        },
        c if transient_code(c) => SourceError::Transient(text),
        _ => SourceError::Permanent(text),
    }
}

/// The 1236 messages of a binlog the server no longer has (S-MY10e, S-MY10k): a file gone from
/// the index, purged GTIDs (MySQL), a GTID state no binlog holds (MariaDB).
fn purged(message: &str) -> bool {
    message.contains("Could not find first log file name")
        || message.contains("purged required binary logs")
        || message.contains("Could not find GTID state")
}

/// A `mysql_async` error. I/O and a closed connection are transient; a server error by its code;
/// TLS, authentication and protocol failures are permanent.
pub(crate) fn mysql(e: Error, position: Option<&str>) -> SourceError {
    match e {
        Error::Server(s) => server(s.code, &s.message, position),
        Error::Io(IoError::Tls(t)) => SourceError::Permanent(format!("TLS: {t}")),
        // A handshake failure (an unknown issuer, a name mismatch) arrives as I/O around the
        // rustls error: a retry meets the same certificate.
        Error::Io(IoError::Io(io)) if io.get_ref().is_some_and(|e| e.is::<rustls::Error>()) => {
            SourceError::Permanent(format!("TLS: {io}"))
        }
        Error::Io(IoError::Io(io)) => SourceError::Transient(io.to_string()),
        Error::Driver(DriverError::ConnectionClosed) => {
            SourceError::Transient("the server closed the connection".into())
        }
        Error::Driver(DriverError::NoClientSslFlagFromServer) => SourceError::Permanent(
            "TLS: the server doesn't offer TLS, and the ssl_mode requires it".into(),
        ),
        other => SourceError::Permanent(other.to_string()),
    }
}

/// A client-side timeout of a connect, a catalog or a snapshot query.
pub(crate) fn timed_out(what: &str, after: std::time::Duration) -> SourceError {
    SourceError::Transient(format!("{what} timed out after {after:?}"))
}

```

In `crates/data-avalanche-source-mysql/src/server.rs`, add above the test module:

```rust
//! The server behind the source: MySQL or MariaDB, its version, and the start checks on its
//! settings and the user's privileges (config §11.2, the MySQL rows; P-8 versions). The checks
//! are pure functions over what the server reported, so every refusal is unit-tested.

use data_avalanche_config::ReplicationType;

/// The binlog dialect: GTIDs, status statements, row events and types differ (S-MY10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavour {
    Mysql,
    Mariadb,
}

impl Flavour {
    /// From `VERSION()`: MariaDB's carries `-MariaDB` (S-MY10a).
    pub fn of(version: &str) -> Self {
        if version.contains("MariaDB") {
            Self::Mariadb
        } else {
            Self::Mysql
        }
    }

    /// The oldest supported release (P-8): MySQL 8.4 LTS, MariaDB 10.11 LTS.
    pub fn minimum(self) -> (u16, u16) {
        match self {
            Self::Mysql => (8, 4),
            Self::Mariadb => (10, 11),
        }
    }

    /// The statement that reads the binlog write position: MySQL 8.4 accepts only `SHOW BINARY
    /// LOG STATUS`, MariaDB 10.11 and 11.4 only `SHOW BINLOG STATUS` (S-MY10a).
    pub fn status_statement(self) -> &'static str {
        match self {
            Self::Mysql => "SHOW BINARY LOG STATUS",
            Self::Mariadb => "SHOW BINLOG STATUS",
        }
    }

    /// The privilege that statement needs, as the server names it.
    pub fn status_privilege(self) -> &'static str {
        match self {
            Self::Mysql => "REPLICATION CLIENT",
            Self::Mariadb => "BINLOG MONITOR (or REPLICATION CLIENT)",
        }
    }
}

impl std::fmt::Display for Flavour {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Mysql => "MySQL",
            Self::Mariadb => "MariaDB",
        })
    }
}

/// `major.minor` from a `VERSION()` string (`8.4.6`, `11.8.3-MariaDB-ubu2404-log`).
pub fn version_of(version: &str) -> Option<(u16, u16)> {
    let mut parts = version.split(['.', '-']);
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// The server settings the start checks read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// `VERSION()`.
    pub version: String,
    pub log_bin: bool,
    pub binlog_format: String,
    pub binlog_row_image: String,
    /// `None` on a server without the variable.
    pub binlog_row_metadata: Option<String>,
    /// MySQL `@@gtid_mode`; `None` on MariaDB, whose GTIDs are always on.
    pub gtid_mode: Option<String>,
}

/// The pipeline-level checks on `settings` (config §11.2): `Err` refuses the pipeline, the `Ok`
/// warnings let it start.
pub fn check_settings(
    flavour: Flavour,
    replication: ReplicationType,
    s: &Settings,
) -> Result<Vec<String>, String> {
    let (major, minor) = version_of(&s.version)
        .ok_or_else(|| format!("can't read the server version from {:?}", s.version))?;
    let (min_major, min_minor) = flavour.minimum();
    if (major, minor) < (min_major, min_minor) {
        return Err(format!(
            "{flavour} {}: the source needs {flavour} {min_major}.{min_minor} or newer",
            s.version
        ));
    }
    if !s.log_bin {
        return Err("the binary log is off (log_bin); the source reads the binlog".into());
    }
    if !s.binlog_format.eq_ignore_ascii_case("ROW") {
        return Err(format!(
            "binlog_format is {}; the source needs binlog_format = ROW",
            s.binlog_format
        ));
    }
    if !s.binlog_row_image.eq_ignore_ascii_case("FULL") {
        return Err(format!(
            "binlog_row_image is {}; the source needs binlog_row_image = FULL",
            s.binlog_row_image
        ));
    }
    if let (ReplicationType::Gtid, Some(mode)) = (replication, &s.gtid_mode)
        && !mode.eq_ignore_ascii_case("ON")
    {
        return Err(format!(
            "gtid_mode is {mode}; replication_type: gtid needs gtid_mode = ON (or use replication_type: binlog)"
        ));
    }
    let mut warnings = vec![];
    match &s.binlog_row_metadata {
        Some(m) if m.eq_ignore_ascii_case("FULL") => {}
        other => warnings.push(format!(
            "binlog_row_metadata is {}; with FULL the binlog names the columns, otherwise the \
             names come from the catalog",
            other.as_deref().unwrap_or("not available")
        )),
    }
    Ok(warnings)
}

/// True when `SHOW GRANTS` lines give `REPLICATION SLAVE` on `*.*` (directly or through `ALL
/// PRIVILEGES`). MariaDB 10.5+ still prints the privilege under that name.
pub fn grants_replication(grants: &[String]) -> bool {
    grants.iter().any(|g| {
        let g = g.to_ascii_uppercase();
        g.starts_with("GRANT ")
            && g.contains(" ON *.* ")
            && (g.contains("ALL PRIVILEGES") || g.contains("REPLICATION SLAVE"))
    })
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 12 tests.

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql preflight:: || break; done`

Expected: PASS, 6 tests on each target (`test-infra/up.sh` first).

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql tls:: || break; done`

Expected: PASS, 4 tests on each target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-source-mysql/Cargo.toml \
  crates/data-avalanche-source-mysql/src/connect.rs \
  crates/data-avalanche-source-mysql/src/connector.rs \
  crates/data-avalanche-source-mysql/src/error.rs \
  crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/server.rs \
  crates/data-avalanche-source-mysql/tests/mysql/common.rs \
  crates/data-avalanche-source-mysql/tests/mysql/main.rs \
  crates/data-avalanche-source-mysql/tests/mysql/preflight.rs \
  crates/data-avalanche-source-mysql/tests/mysql/tls.rs
git commit -m "feat(source-mysql): the crate — connections, TLS, server flavour and the start checks"
```

---

### Task 5: Positions: the write position, the server's identity, the binlogs it holds

**Files:**
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`
- Create: `crates/data-avalanche-source-mysql/src/position.rs`
- Modify: `crates/data-avalanche-source-mysql/tests/mysql/main.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/position.rs`

**Interfaces:**
- Consumes:
  - `MySqlConnector`, its catalog connection's `command` and `flavour()` (Task 4).
  - `data_avalanche_core::position::{GtidSet, MariaGtidState, BinlogFilePos}` and
    `SourcePosition::parse_tagged` (M0).
- Produces:
  - `data-avalanche-source-mysql/src/lib.rs`:
    - `pub mod position;`
  - `data-avalanche-source-mysql/src/position.rs`:
    - `pub enum Mode`
    - `pub fn of(flavour: Flavour, replication: ReplicationType) -> Self`
    - `pub struct BinaryLog`
    - `pub fn bytes_after(logs: &[BinaryLog], at: &BinlogFilePos) -> u64`
    - `pub enum Retention`
    - `pub fn purged(checkpoint: &SourcePosition, retention: &Retention) -> Option<String>`
    - `pub async fn mode(&self) -> Result<Mode, SourceError>`
    - `pub async fn position_now(&self) -> Result<SourcePosition, SourceError>`
    - `pub async fn server_identity(&self) -> Result<ServerIdentity, SourceError>`
    - `pub async fn binary_logs(&self) -> Result<Vec<BinaryLog>, SourceError>`
    - `pub async fn retention(&self, checkpoint: &SourcePosition) -> Result<Retention, SourceError>`

S-MY10 as code (D-M7a.7). A pipeline's positions take one of three dialects (`Mode`): a MySQL
GTID set (`replication_type: gtid` on MySQL), a MariaDB GTID state (`gtid` on MariaDB), or a
binlog file and offset (`binlog`, either server).

- The write position now: MySQL `@@GLOBAL.gtid_executed`, MariaDB `@@GLOBAL.gtid_binlog_pos`,
  or the status statement's file and position (MySQL 8.4 `SHOW BINARY LOG STATUS`, MariaDB
  `SHOW BINLOG STATUS`; S-MY10a).
- The server's identity: MySQL `@@server_uuid`, MariaDB `@@server_id` (MariaDB has no
  `server_uuid`).
- The binlogs it holds (`SHOW BINARY LOGS`), and `purged(checkpoint, retention)`: why the
  server can no longer serve a checkpoint, as a pure function of what it reported. A file gone
  from the list; a MySQL set that lacks GTIDs in `@@GLOBAL.gtid_purged`; a MariaDB state behind
  the state the oldest binlog starts at (`BINLOG_GTID_POS(<oldest>, 4)`). The server's own
  refusal (1236, Task 4) is the fallback for a purge between the check and the dump.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -7,8 +7,10 @@
 mod connect;
 mod connector;
 mod error;
+pub mod position;
 pub mod server;
 
 pub use connect::SESSION_SETUP;
 pub use connector::MySqlConnector;
+pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
 pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
```

Change `crates/data-avalanche-source-mysql/tests/mysql/main.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/main.rs b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/main.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
@@ -4,5 +4,6 @@
 //! returns at once without `DATA_AVALANCHE_IT`.
 
 mod common;
+mod position;
 mod preflight;
 mod tls;
```

Create `crates/data-avalanche-source-mysql/src/position.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn logs(sizes: &[(&str, u64)]) -> Vec<BinaryLog> {
        sizes
            .iter()
            .map(|(n, s)| BinaryLog {
                name: n.to_string(),
                size: *s,
            })
            .collect()
    }

    fn pos(s: &str) -> SourcePosition {
        SourcePosition::parse_tagged(s).unwrap()
    }

    #[test]
    fn the_mode_follows_the_flavour_and_replication_type() {
        use Flavour::*;
        use ReplicationType::*;
        assert_eq!(Mode::of(Mysql, Gtid), Mode::MysqlGtid);
        assert_eq!(Mode::of(Mariadb, Gtid), Mode::MariadbGtid);
        assert_eq!(Mode::of(Mysql, Binlog), Mode::File);
        assert_eq!(Mode::of(Mariadb, Binlog), Mode::File);
    }

    #[test]
    fn server_positions_parse_in_their_dialect() {
        // gtid_executed wraps long sets with a newline after each comma.
        let set =
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,\n3e11fa47-71ca-11e1-9e33-c80aa9429563:1";
        assert!(matches!(
            parse(Mode::MysqlGtid, set),
            Ok(SourcePosition::MySqlGtid(_))
        ));
        assert_eq!(
            parse(Mode::MysqlGtid, "").unwrap(),
            SourcePosition::MySqlGtid(GtidSet::default())
        );
        assert_eq!(
            parse(Mode::MariadbGtid, "0-1-42").unwrap(),
            pos("mariadb-gtid:0-1-42")
        );
        assert_eq!(
            parse(Mode::File, "mysql-bin.000003:157").unwrap(),
            pos("binlog:mysql-bin.000003:157")
        );
        assert!(parse(Mode::File, ":0").is_err());
    }

    #[test]
    fn retained_bytes_count_the_rest_of_the_file_and_later_files() {
        let l = logs(&[("b.000001", 1000), ("b.000002", 500), ("b.000003", 200)]);
        let at = |s: &str| s.parse::<BinlogFilePos>().unwrap();
        assert_eq!(bytes_after(&l, &at("b.000002:100")), 400 + 200);
        assert_eq!(bytes_after(&l, &at("b.000003:200")), 0);
        assert_eq!(bytes_after(&l, &at("b.000001:4")), 996 + 700);
        assert_eq!(bytes_after(&l, &at("b.000009:4")), 0, "not on the server");
    }

    #[test]
    fn a_file_gone_from_the_server_is_purged() {
        let l = Retention::Files(logs(&[("b.000002", 500), ("b.000003", 200)]));
        assert_eq!(purged(&pos("binlog:b.000002:4"), &l), None);
        assert_eq!(
            purged(&pos("binlog:b.000001:1193"), &l).unwrap(),
            "binlog b.000001 is no longer on the server (oldest: b.000002)"
        );
    }

    #[test]
    fn purged_gtids_the_checkpoint_lacks_are_purged() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let purged_set = |s: &str| Retention::MysqlPurged(s.parse().unwrap());
        let cp = pos(&format!("mysql-gtid:{u}:1-10"));
        assert_eq!(purged(&cp, &purged_set("")), None);
        assert_eq!(purged(&cp, &purged_set(&format!("{u}:1-10"))), None);
        let p = purged(&cp, &purged_set(&format!("{u}:1-12"))).unwrap();
        assert!(p.contains(&format!("{u}:1-12")), "{p}");
    }

    #[test]
    fn a_mariadb_checkpoint_before_the_oldest_binlog_is_purged() {
        let oldest = |s: &str| Retention::MariadbOldest(s.parse().unwrap());
        let cp = pos("mariadb-gtid:0-1-42");
        assert_eq!(purged(&cp, &oldest("")), None, "no purge yet");
        assert_eq!(purged(&cp, &oldest("0-1-42")), None);
        assert!(purged(&cp, &oldest("0-1-43")).is_some());
        // A domain the checkpoint never saw whose start was purged.
        assert!(purged(&cp, &oldest("0-1-40,1-1-5")).is_some());
    }
}
```

Create `crates/data-avalanche-source-mysql/tests/mysql/position.rs`:

```rust
//! Positions on the target server (S-MY10): the write position in the pipeline's dialect, the
//! server's identity, the binlogs it holds, and the purge check on a live position.

use data_avalanche_core::{ServerIdentity, SourcePosition};
use data_avalanche_source_mysql::{Flavour, Retention, purged};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, target};

#[tokio::test]
async fn the_write_position_moves_past_a_write_in_the_pipelines_dialect() {
    require_it!();
    let (n, mut a) = fresh("position_now", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let before = c.position_now().await.expect("position");
    let dialect = match (target().replication, target().flavour) {
        ("binlog", _) => "binlog",
        (_, Flavour::Mysql) => "mysql-gtid",
        (_, Flavour::Mariadb) => "mariadb-gtid",
    };
    assert_eq!(before.dialect(), dialect);
    a.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
    let after = c.position_now().await.expect("position");
    assert!(after.covers(&before).unwrap(), "{after} covers {before}");
    assert!(
        !before.covers(&after).unwrap(),
        "{before} is before {after}"
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn the_identity_names_the_server() {
    require_it!();
    let (n, mut a) = fresh("position_identity", "").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let id = c.server_identity().await.expect("identity");
    match target().flavour {
        Flavour::Mysql => {
            let uuid: String = a
                .query_first("SELECT @@server_uuid")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(id, ServerIdentity::Mysql { server_uuid: uuid });
        }
        Flavour::Mariadb => {
            let server_id: u32 = a.query_first("SELECT @@server_id").await.unwrap().unwrap();
            assert_eq!(id, ServerIdentity::Mariadb { server_id });
        }
    }
    cleanup(&n).await;
}

#[tokio::test]
async fn a_live_position_is_reachable_and_a_file_never_written_is_purged() {
    require_it!();
    let (n, _a) = fresh("position_purged", "").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let now = c.position_now().await.unwrap();
    let retention = c.retention(&now).await.expect("retention");
    assert_eq!(purged(&now, &retention), None, "{retention:?}");
    // The binlogs: the current file is listed with its bytes.
    let logs = c.binary_logs().await.expect("binary logs");
    assert!(
        !logs.is_empty() && logs.iter().all(|l| l.size > 0),
        "{logs:?}"
    );
    if let SourcePosition::BinlogFile(at) = &now {
        assert!(logs.iter().any(|l| l.name == at.file()), "{at} in {logs:?}");
        // Index 0 is never written: the series starts at 000001.
        let gone = SourcePosition::parse_tagged(&format!("binlog:{}.000000:4", at.base())).unwrap();
        let r = c.retention(&gone).await.unwrap();
        assert!(matches!(r, Retention::Files(_)));
        let why = purged(&gone, &r).expect("purged");
        assert!(why.contains("is no longer on the server"), "{why}");
    }
    cleanup(&n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `position::BinaryLog`, `position::Mode`, `position::Retention`, `position::bytes_after`, `position::purged`
error[E0432]: unresolved import `Flavour`
error[E0432]: unresolved import `ReplicationType`
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-source-mysql/src/position.rs`, add above the test module:

```rust
//! Stream positions on the server (S-MY10): the dialect a pipeline's positions take, the server's
//! write position now, its identity, the binlogs it still holds, and whether a checkpoint can
//! still be served. The decisions are pure functions over what the server reported.

use data_avalanche_config::ReplicationType;
use data_avalanche_core::position::{BinlogFilePos, GtidSet, MariaGtidState};
use data_avalanche_core::{ServerIdentity, SourceError, SourcePosition};
use mysql_async::prelude::Queryable;

use crate::connector::MySqlConnector;
use crate::server::Flavour;

/// The dialect of a pipeline's positions: a MySQL GTID set, a MariaDB GTID state, or a binlog
/// file/position (either server, `replication_type: binlog`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    MysqlGtid,
    MariadbGtid,
    File,
}

impl Mode {
    pub fn of(flavour: Flavour, replication: ReplicationType) -> Self {
        match (replication, flavour) {
            (ReplicationType::Binlog, _) => Self::File,
            (ReplicationType::Gtid, Flavour::Mysql) => Self::MysqlGtid,
            (ReplicationType::Gtid, Flavour::Mariadb) => Self::MariadbGtid,
        }
    }
}

/// One row of `SHOW BINARY LOGS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryLog {
    pub name: String,
    pub size: u64,
}

/// The binlog bytes the server holds after `at`: the rest of its file and every later file. A
/// file not in the list (purged, or another series) counts nothing.
pub fn bytes_after(logs: &[BinaryLog], at: &BinlogFilePos) -> u64 {
    let Some(i) = logs.iter().position(|l| l.name == at.file()) else {
        return 0;
    };
    let rest = logs[i].size.saturating_sub(at.pos());
    rest + logs[i + 1..].iter().map(|l| l.size).sum::<u64>()
}

/// What the server reports about the binlogs it no longer has, in the checkpoint's dialect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retention {
    /// `SHOW BINARY LOGS`: the files it still has.
    Files(Vec<BinaryLog>),
    /// MySQL `@@GLOBAL.gtid_purged`.
    MysqlPurged(GtidSet),
    /// MariaDB `BINLOG_GTID_POS(<oldest file>, 4)`: the state where the oldest binlog starts.
    MariadbOldest(MariaGtidState),
}

/// Why the server can't serve `checkpoint` any more, or `None` when it can: its file is gone, or
/// it lacks transactions the server purged (MySQL), or the oldest binlog starts after it
/// (MariaDB). The server's own refusal (error 1236) is the fallback for a purge after this check.
pub fn purged(checkpoint: &SourcePosition, retention: &Retention) -> Option<String> {
    match (checkpoint, retention) {
        (SourcePosition::BinlogFile(at), Retention::Files(logs)) => {
            (!logs.iter().any(|l| l.name == at.file())).then(|| {
                let first = logs.first().map_or("none", |l| l.name.as_str());
                format!(
                    "binlog {} is no longer on the server (oldest: {first})",
                    at.file()
                )
            })
        }
        (SourcePosition::MySqlGtid(set), Retention::MysqlPurged(purged)) => (!set.covers(purged))
            .then(|| format!("the server purged GTIDs {purged} the position {set} hasn't read")),
        (SourcePosition::MariaDbGtid(state), Retention::MariadbOldest(oldest)) => (!state
            .covers(oldest))
        .then(|| format!("the oldest binlog starts at {oldest}, after the position {state}")),
        (p, r) => Some(format!("position {p} can't be checked against {r:?}")),
    }
}

impl MySqlConnector {
    /// The dialect this pipeline's positions take on this server.
    pub async fn mode(&self) -> Result<Mode, SourceError> {
        Ok(Mode::of(self.flavour().await?, self.mysql.replication_type))
    }

    /// The server's write position now, in the pipeline's dialect: MySQL `@@GLOBAL.gtid_executed`,
    /// MariaDB `@@GLOBAL.gtid_binlog_pos`, or the binlog status statement's file and position.
    pub async fn position_now(&self) -> Result<SourcePosition, SourceError> {
        let mode = self.mode().await?;
        let status = self.flavour().await?.status_statement();
        let text = self
            .conn
            .command("read the binlog position", move |c| {
                Box::pin(async move {
                    match mode {
                        Mode::MysqlGtid => c.query_first("SELECT @@GLOBAL.gtid_executed").await,
                        Mode::MariadbGtid => c.query_first("SELECT @@GLOBAL.gtid_binlog_pos").await,
                        Mode::File => {
                            let row: Option<mysql_async::Row> = c.query_first(status).await?;
                            Ok(row.map(|r| {
                                let file: String = r.get(0).unwrap_or_default();
                                let pos: u64 = r.get(1).unwrap_or_default();
                                format!("{file}:{pos}")
                            }))
                        }
                    }
                })
            })
            .await?
            .ok_or_else(|| {
                SourceError::Permanent("the server reports no binlog position".into())
            })?;
        parse(mode, &text)
    }

    /// The server a file/position belongs to: MySQL `@@server_uuid`, MariaDB `@@server_id`
    /// (MariaDB has no server UUID, S-MY10a).
    pub async fn server_identity(&self) -> Result<ServerIdentity, SourceError> {
        let flavour = self.flavour().await?;
        self.conn
            .command("read the server identity", move |c| {
                Box::pin(async move {
                    Ok(match flavour {
                        Flavour::Mysql => ServerIdentity::Mysql {
                            server_uuid: c
                                .query_first("SELECT @@server_uuid")
                                .await?
                                .unwrap_or_default(),
                        },
                        Flavour::Mariadb => ServerIdentity::Mariadb {
                            server_id: c
                                .query_first("SELECT @@server_id")
                                .await?
                                .unwrap_or_default(),
                        },
                    })
                })
            })
            .await
    }

    /// `SHOW BINARY LOGS`, oldest first.
    pub async fn binary_logs(&self) -> Result<Vec<BinaryLog>, SourceError> {
        self.conn
            .command("list the binlogs", |c| {
                Box::pin(async move {
                    let rows: Vec<mysql_async::Row> = c.query("SHOW BINARY LOGS").await?;
                    Ok(rows
                        .into_iter()
                        .map(|r| BinaryLog {
                            name: r.get(0).unwrap_or_default(),
                            size: r.get(1).unwrap_or_default(),
                        })
                        .collect())
                })
            })
            .await
    }

    /// What the server reports about purged binlogs, in `checkpoint`'s dialect.
    pub async fn retention(&self, checkpoint: &SourcePosition) -> Result<Retention, SourceError> {
        match checkpoint {
            SourcePosition::BinlogFile(_) => Ok(Retention::Files(self.binary_logs().await?)),
            SourcePosition::MySqlGtid(_) => {
                let text: String = self
                    .conn
                    .command("read gtid_purged", |c| {
                        Box::pin(async move {
                            Ok(c.query_first("SELECT @@GLOBAL.gtid_purged")
                                .await?
                                .unwrap_or_default())
                        })
                    })
                    .await?;
                match parse(Mode::MysqlGtid, &text)? {
                    SourcePosition::MySqlGtid(set) => Ok(Retention::MysqlPurged(set)),
                    _ => unreachable!("parsed in the MySQL GTID dialect"),
                }
            }
            SourcePosition::MariaDbGtid(_) => {
                let logs = self.binary_logs().await?;
                let Some(oldest) = logs.first().map(|l| l.name.clone()) else {
                    return Ok(Retention::MariadbOldest(MariaGtidState::default()));
                };
                let text: Option<String> = self
                    .conn
                    .command("read the oldest binlog's GTID state", move |c| {
                        Box::pin(async move {
                            c.exec_first("SELECT BINLOG_GTID_POS(?, 4)", (oldest,))
                                .await
                        })
                    })
                    .await?;
                match parse(Mode::MariadbGtid, text.as_deref().unwrap_or_default())? {
                    SourcePosition::MariaDbGtid(s) => Ok(Retention::MariadbOldest(s)),
                    _ => unreachable!("parsed in the MariaDB GTID dialect"),
                }
            }
            SourcePosition::Pg(_) => Err(SourceError::Permanent(format!(
                "position {checkpoint} isn't a MySQL position"
            ))),
        }
    }
}

/// A position the server printed, in `mode`'s dialect.
pub(crate) fn parse(mode: Mode, text: &str) -> Result<SourcePosition, SourceError> {
    let tag = match mode {
        Mode::MysqlGtid => "mysql-gtid",
        Mode::MariadbGtid => "mariadb-gtid",
        Mode::File => "binlog",
    };
    SourcePosition::parse_tagged(&format!("{tag}:{text}"))
        .map_err(|e| SourceError::Permanent(format!("the server's position: {e}")))
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 18 tests.

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql position:: || break; done`

Expected: PASS, 3 tests on each target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/position.rs \
  crates/data-avalanche-source-mysql/tests/mysql/main.rs \
  crates/data-avalanche-source-mysql/tests/mysql/position.rs
git commit -m "feat(source-mysql): positions — the write position per dialect, server identity, binlogs held and the purge check"
```

---

### Task 6: Binlog primitives: order offsets, the payload check, MariaDB events

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/data-avalanche-source-mysql/Cargo.toml`
- Create: `crates/data-avalanche-source-mysql/src/binlog.rs`
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`

**Interfaces:**
- Consumes:
  - `mysql_async::binlog` event types (`=0.37.1`).
- Produces:
  - `data-avalanche-source-mysql/src/binlog.rs`:
    - `pub mod types`
    - `pub const QUERY: u8 = 2;`
    - `pub const STOP: u8 = 3;`
    - `pub const ROTATE: u8 = 4;`
    - `pub const INTVAR: u8 = 5;`
    - `pub const RAND: u8 = 13;`
    - `pub const USER_VAR: u8 = 14;`
    - `pub const FORMAT_DESCRIPTION: u8 = 15;`
    - `pub const XID: u8 = 16;`
    - `pub const TABLE_MAP: u8 = 19;`
    - `pub const WRITE_ROWS_V1: u8 = 23;`
    - `pub const UPDATE_ROWS_V1: u8 = 24;`
    - `pub const DELETE_ROWS_V1: u8 = 25;`
    - `pub const INCIDENT: u8 = 26;`
    - `pub const HEARTBEAT: u8 = 27;`
    - `pub const IGNORABLE: u8 = 28;`
    - `pub const ROWS_QUERY: u8 = 29;`
    - `pub const WRITE_ROWS: u8 = 30;`
    - `pub const UPDATE_ROWS: u8 = 31;`
    - `pub const DELETE_ROWS: u8 = 32;`
    - `pub const GTID: u8 = 33;`
    - `pub const ANONYMOUS_GTID: u8 = 34;`
    - `pub const PREVIOUS_GTIDS: u8 = 35;`
    - `pub const TRANSACTION_CONTEXT: u8 = 36;`
    - `pub const VIEW_CHANGE: u8 = 37;`
    - `pub const XA_PREPARE: u8 = 38;`
    - `pub const PARTIAL_UPDATE_ROWS: u8 = 39;`
    - `pub const TRANSACTION_PAYLOAD: u8 = 40;`
    - `pub const HEARTBEAT_V2: u8 = 41;`
    - `pub const GTID_TAGGED: u8 = 42;`
    - `pub const MARIADB_ANNOTATE_ROWS: u8 = 160;`
    - `pub const MARIADB_BINLOG_CHECKPOINT: u8 = 161;`
    - `pub const MARIADB_GTID: u8 = 162;`
    - `pub const MARIADB_GTID_LIST: u8 = 163;`
    - `pub const MARIADB_START_ENCRYPTION: u8 = 164;`
    - `pub const MARIADB_QUERY_COMPRESSED: u8 = 165;`
    - `pub const MARIADB_WRITE_ROWS_COMPRESSED_V1: u8 = 166;`
    - `pub const MARIADB_UPDATE_ROWS_COMPRESSED_V1: u8 = 167;`
    - `pub const MARIADB_DELETE_ROWS_COMPRESSED_V1: u8 = 168;`
    - `pub fn rotate_name(raw: &[u8]) -> &[u8]`
    - `pub struct MariaGtidEvent`
    - `pub const STANDALONE: u8 = 0x01;`
    - `pub const PREPARED_XA: u8 = 0x40;`
    - `pub const COMPLETED_XA: u8 = 0x80;`
    - `pub fn parse(server_id: u32, data: &[u8]) -> Result<Self, String>`
    - `pub fn standalone(&self) -> bool`
    - `pub fn xa(&self) -> bool`
    - `pub struct Offsets`
    - `pub fn new(file: &str, pos: u64) -> Self`
    - `pub fn file(&self) -> &str`
    - `pub fn end(&self) -> u64`
    - `pub fn in_payload(&self) -> bool`
    - `pub fn rotate(&mut self, file: &str, pos: u64) -> Result<(), String>`
    - `pub fn heartbeat(&self) -> Result<(), String>`
    - `pub fn event(&mut self, log_pos: u32, size: u32) -> Result<u64, String>`
    - `pub fn payload(&mut self, uncompressed_size: u64) -> Result<(), String>`
    - `pub fn plain_type(raw: u8) -> Option<u8>`
    - `pub fn decompress(e: &Event) -> Result<Option<Event>, String>`
  - `data-avalanche-source-mysql/src/lib.rs`:
    - `pub mod binlog;`

The parts of the binlog the stream reads that `mysql_async` doesn't (S-MY10, D-V1.6, Q-M7-4,
Q-M7a-5). Pure functions, tested on bytes captured from the servers.

- **Order offsets** (`Offsets`, Q-M7-4; D-M7a.8). A change's `_seq` is (file index, end offset).
  The header's `log_pos` is 0 for the events inside a MySQL compressed transaction and for
  MariaDB 11.x table maps and rows events (S-MY10f), so the offset is computed: the last header
  `log_pos` that is set, plus the sizes of the events since. The events inside a
  `TRANSACTION_PAYLOAD` take the payload's end.
- **The payload check** (D-V1.6, DA-SRC-12). `mysql_async` decompresses a payload and yields its
  events, and yields none when decompression fails: a silent loss. `Offsets` counts the
  payload's `uncompressed_size` down by the events it yields. An event that overruns it, or a
  payload that ends short of it, is a pipeline halt whose message starts `BINLOG_PAYLOAD_INVALID:`
  (D-M7a.8: the pipeline-halt code as the message's prefix, the convention M7b's status reads).
- **MariaDB events**: `GTID_EVENT` (sequence, domain, flags: standalone, XA prepared, XA
  completed), which `mysql_common` doesn't decode; the fake `ROTATE` name, which carries four
  trailing checksum bytes before the format description (S-MY10b).
- **MariaDB compressed events** (`log_bin_compress`, Q-M7a-5; D-M7a.9). `mysql_common` reads
  `QUERY_COMPRESSED` (165) and `WRITE/UPDATE/DELETE_ROWS_COMPRESSED_V1` (166–168) as nothing: a
  silent loss (measured on 11.8 and 10.11). `decompress` reads the event again under its plain
  type, inflates the compressed tail (a byte `0x80 | n`, an n-byte big-endian length, then
  zlib), and rebuilds the plain event. The v2 compressed rows events (169–171) were never seen
  and fail closed in Task 13.

Workspace dependencies: `flate2 = "1"` (already in the lockfile through `mysql_common`) and
`mysql_common = "=0.37.3"` (dev, to build events in the tests).

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -38,6 +38,10 @@ tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
 object_store = { version = "0.14.2", features = ["aws", "gcp", "azure"] }
 tokio-postgres = "0.7"
 mysql_async = { version = "=0.37.1", default-features = false, features = ["default-rustls", "binlog"] }
+# The MySQL source (roadmap §2.1): the binlog events mysql_async reads, and zlib for MariaDB's
+# compressed events (Q-M7a-5).
+mysql_common = { version = "=0.37.3", default-features = false }
+flate2 = "1"
 rustls = { version = "0.23", features = ["aws_lc_rs"] }
 serde-saphyr = { version = "=1.3.0", default-features = false, features = ["deserialize"] }
 humantime = "2.4"
```

Change `crates/data-avalanche-source-mysql/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-source-mysql/Cargo.toml b/crates/data-avalanche-source-mysql/Cargo.toml
--- a/crates/data-avalanche-source-mysql/Cargo.toml
+++ b/crates/data-avalanche-source-mysql/Cargo.toml
@@ -12,6 +12,7 @@ workspace = true
 [dependencies]
 data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
+flate2.workspace = true
 futures-util.workspace = true
 mysql_async.workspace = true
 rustls.workspace = true
@@ -19,5 +20,6 @@ tokio = { workspace = true, features = ["sync", "time"] }
 
 [dev-dependencies]
 data-avalanche-testkit.workspace = true
+mysql_common.workspace = true
 rcgen.workspace = true
 tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -4,12 +4,14 @@
 //! `information_schema` (`Schema_Change…` §3) and keyset snapshot chunks read with a locking read
 //! (`Snapshot_Resumability…` §1.1, DA-SNAP-10).
 
+pub mod binlog;
 mod connect;
 mod connector;
 mod error;
 pub mod position;
 pub mod server;
 
+pub use binlog::{MariaGtidEvent, Offsets, decompress, rotate_name};
 pub use connect::SESSION_SETUP;
 pub use connector::MySqlConnector;
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
```

Create `crates/data-avalanche-source-mysql/src/binlog.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::binlog::events::RowsEventData;
    use mysql_async::binlog::value::BinlogValue;
    use mysql_common::binlog::EventStreamReader;
    use mysql_common::binlog::consts::BinlogVersion;

    /// Events captured from MariaDB 11.8 with `log_bin_compress = ON` (`m7/x` capture, 2026-10-14):
    /// the format description, a table map of `m7v (id INT PRIMARY KEY, v VARCHAR(40))`, the
    /// compressed insert of `(7, 'compressed rows, decompressed')`, a GTID event and the
    /// compressed `ALTER TABLE m7v COMMENT 'a compressed statement'`.
    const FDE: &str = "8223cf6a0f01000000fc000000000000000000040031312e382e392d4d6172696144422d756275323430342d6c6f6700\
        00000000000000000000000000000000000000000000000000000013380d000800120004040404120000e400041a0800\
        0000080808020000000a0a0a0000000000000a0a0a000000000000000000000000000000000000000000000000000000\
        000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\
        000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004130400\
        0d0808080a0a0a0110dbbc0e";
    const GTID_ROWS: &str =
        "f567cf6aa2010000002a0000001820000008006801000000000000000000000c109441000000916d1eee";
    const TABLE_MAP: &str = "f567cf6a1301000000440000000000000000002d000000000001000570726f626500036d37760002030f02a000020101\
        000203fc000904050269640176080100d9f8cd79";
    const WRITE_COMPRESSED: &str = "f567cf6aa601000000460000000000000000002d0000000000010002038123789cfbc3cec0c0209b9c9f5b50945a5c9c\
        9aa250945f5eaca390928a100200d2430c8b8642f8f5";
    const GTID_DDL: &str =
        "f567cf6aa2010000002a0000003d210000080069010000000000000000000029109441000000fc0a44cb";
    const QUERY_COMPRESSED: &str = "f567cf6aa50100000087000000c421000000009441000000000000050000230000000000010100002054000000000603\
        7374640400090009000981c28300000000000070726f6265008130789c73f409710d52087174f27155c8352f5370f6f7\
        f575f50b51504f5448cecf2d284a2d2e4e4d51282e492c49cd4dcd2b5107005cf70f879bee9f57";

    fn bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    fn reader() -> EventStreamReader {
        let mut r = EventStreamReader::new(BinlogVersion::Version4);
        r.read(&bytes(FDE)[..])
            .unwrap()
            .expect("the format description");
        r
    }

    fn event(r: &mut EventStreamReader, hex: &str) -> Event {
        r.read(&bytes(hex)[..]).unwrap().expect("an event")
    }

    #[test]
    fn a_rotate_name_loses_the_crc_mariadb_leaves_on_it() {
        assert_eq!(
            rotate_name(b"mariadb-bin.000037x\x1b\xfe\x01"),
            b"mariadb-bin.000037"
        );
        assert_eq!(rotate_name(b"mysql-bin.000003"), b"mysql-bin.000003");
        // A name that reads either way is left alone.
        assert_eq!(rotate_name(b"b.000001.000002"), b"b.000001.000002");
        assert_eq!(rotate_name(b"abc"), b"abc");
    }

    #[test]
    fn a_mariadb_gtid_event_reads_its_gtid_and_flags() {
        let mut r = reader();
        let e = event(&mut r, GTID_ROWS);
        let g = MariaGtidEvent::parse(e.header().server_id(), e.data()).unwrap();
        assert_eq!(
            g.gtid,
            MariaGtid {
                domain: 0,
                server_id: 1,
                seq: 360
            }
        );
        assert!(!g.standalone() && !g.xa());
        let e = event(&mut r, GTID_DDL);
        let g = MariaGtidEvent::parse(1, e.data()).unwrap();
        assert_eq!(g.gtid.seq, 361);
        assert!(g.standalone(), "DDL stands alone: {:#x}", g.flags);
        let xa = MariaGtidEvent {
            flags: MariaGtidEvent::PREPARED_XA,
            ..g
        };
        assert!(xa.xa());
        assert!(MariaGtidEvent::parse(1, &[0; 12]).is_err());
    }

    #[test]
    fn offsets_follow_the_header_or_add_up_sizes() {
        let mut o = Offsets::new("b.000001", 4);
        o.rotate("b.000001", 4).unwrap();
        assert_eq!(o.event(0, 120).unwrap(), 124, "no position set: the sum");
        assert_eq!(o.event(300, 50).unwrap(), 300, "a set position wins");
        assert_eq!(o.event(0, 40).unwrap(), 340);
        o.heartbeat().unwrap();
        o.rotate("b.000002", 4).unwrap();
        assert_eq!((o.file(), o.end()), ("b.000002", 4));
    }

    #[test]
    fn a_payloads_events_take_its_end_and_count_down() {
        let mut o = Offsets::new("b.000001", 4);
        assert_eq!(o.event(1000, 400).unwrap(), 1000);
        o.payload(250).unwrap();
        assert!(o.in_payload());
        for size in [100, 100, 50] {
            assert_eq!(o.event(0, size).unwrap(), 1000, "inside: the payload's end");
        }
        assert!(!o.in_payload());
        assert_eq!(o.event(1100, 100).unwrap(), 1100);
    }

    #[test]
    fn a_payload_whose_events_went_missing_is_invalid() {
        let lost = |o: &mut Offsets| o.payload(250).map(|_| ());
        // Nothing came out: the next transaction's event has its position set.
        let mut o = Offsets::new("b.000001", 4);
        o.event(1000, 400).unwrap();
        lost(&mut o).unwrap();
        let e = o.event(1100, 100).unwrap_err();
        assert!(e.contains("yielded 0 of its 250 bytes"), "{e}");
        // Part came out, then a heartbeat, or a rotate.
        let mut o = Offsets::new("b.000001", 4);
        o.event(1000, 400).unwrap();
        lost(&mut o).unwrap();
        o.event(0, 100).unwrap();
        assert!(
            o.heartbeat()
                .unwrap_err()
                .contains("yielded 100 of its 250")
        );
        assert!(o.rotate("b.000002", 4).is_err());
        // An event larger than what is left, and a payload without events.
        assert!(o.event(0, 200).unwrap_err().contains("overruns"));
        assert!(Offsets::new("b.000001", 4).payload(0).is_err());
    }

    #[test]
    fn mariadb_compressed_rows_read_as_their_plain_event() {
        let mut r = reader();
        let _ = event(&mut r, GTID_ROWS);
        let map = event(&mut r, TABLE_MAP);
        let compressed = event(&mut r, WRITE_COMPRESSED);
        assert!(
            compressed.read_data().unwrap().is_none(),
            "mysql_common reads nothing"
        );
        let plain = decompress(&compressed)
            .unwrap()
            .expect("a compressed event");
        assert_eq!(plain.header().event_type_raw(), types::WRITE_ROWS_V1);
        assert_eq!(plain.header().log_pos(), compressed.header().log_pos());
        let Some(EventData::TableMapEvent(tme)) = map.read_data().unwrap() else {
            panic!("a table map")
        };
        let Some(EventData::RowsEvent(RowsEventData::WriteRowsEventV1(w))) =
            plain.read_data().unwrap()
        else {
            panic!("a write rows event")
        };
        let rows: Vec<_> = RowsEventData::WriteRowsEventV1(w)
            .rows(&tme)
            .map(|r| r.unwrap().1.unwrap().unwrap())
            .collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![
                BinlogValue::Value(mysql_async::Value::Int(7)),
                BinlogValue::Value(mysql_async::Value::Bytes(
                    b"compressed rows, decompressed".to_vec()
                )),
            ]
        );
    }

    #[test]
    fn a_mariadb_compressed_statement_reads_as_its_query() {
        let mut r = reader();
        let compressed = event(&mut r, QUERY_COMPRESSED);
        let plain = decompress(&compressed).unwrap().unwrap();
        let Some(EventData::QueryEvent(q)) = plain.read_data().unwrap() else {
            panic!("a query event")
        };
        assert_eq!(
            q.query(),
            "ALTER TABLE m7v COMMENT 'a compressed statement'"
        );
        assert_eq!(q.schema(), "probe");
        // Not a compressed event: nothing to do.
        assert!(decompress(&event(&mut r, GTID_DDL)).unwrap().is_none());
    }

    #[test]
    fn a_corrupt_compressed_block_is_an_error() {
        assert!(inflate(&[]).is_err());
        assert!(inflate(&[0x01, 5, 0]).unwrap_err().contains("header"));
        assert!(
            inflate(&[0x81, 5, 0x78, 0x9c, 0])
                .unwrap_err()
                .contains("zlib")
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `binlog::MariaGtidEvent`, `binlog::Offsets`, `binlog::decompress`, `binlog::rotate_name`
error[E0425]: cannot find type `Event` in this scope
error[E0422]: cannot find struct, variant or union type `MariaGtid` in this scope
error[E0422]: cannot find struct, variant or union type `MariaGtidEvent` in this scope
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-source-mysql/src/binlog.rs`, add above the test module:

```rust
//! Binlog primitives the stream builds on, each pure and unit-tested (S-MY10, D-V1.6):
//!
//! - [`rotate_name`]: MariaDB's fake ROTATE keeps its CRC on the file name;
//! - [`MariaGtidEvent`]: MariaDB's `GTID_EVENT` (type 162), which `mysql_common` doesn't read;
//! - [`Offsets`]: the end offset of each event in its file (the `_seq` order key, Q-M7-4) and the
//!   check that a compressed transaction yields all of its events (`BINLOG_PAYLOAD_INVALID`);
//! - [`decompress`]: MariaDB's compressed events (`log_bin_compress`, Q-M7a-5), which
//!   `mysql_common` reads as nothing.

use std::io::Read;

use data_avalanche_core::position::MariaGtid;
use mysql_async::binlog::events::{Event, EventData};

/// Raw event types the stream tells apart (MySQL `binlog_event.h`, MariaDB `log_event.h`).
pub mod types {
    pub const QUERY: u8 = 2;
    pub const STOP: u8 = 3;
    pub const ROTATE: u8 = 4;
    pub const INTVAR: u8 = 5;
    pub const RAND: u8 = 13;
    pub const USER_VAR: u8 = 14;
    pub const FORMAT_DESCRIPTION: u8 = 15;
    pub const XID: u8 = 16;
    pub const TABLE_MAP: u8 = 19;
    pub const WRITE_ROWS_V1: u8 = 23;
    pub const UPDATE_ROWS_V1: u8 = 24;
    pub const DELETE_ROWS_V1: u8 = 25;
    pub const INCIDENT: u8 = 26;
    pub const HEARTBEAT: u8 = 27;
    pub const IGNORABLE: u8 = 28;
    pub const ROWS_QUERY: u8 = 29;
    pub const WRITE_ROWS: u8 = 30;
    pub const UPDATE_ROWS: u8 = 31;
    pub const DELETE_ROWS: u8 = 32;
    pub const GTID: u8 = 33;
    pub const ANONYMOUS_GTID: u8 = 34;
    pub const PREVIOUS_GTIDS: u8 = 35;
    pub const TRANSACTION_CONTEXT: u8 = 36;
    pub const VIEW_CHANGE: u8 = 37;
    pub const XA_PREPARE: u8 = 38;
    pub const PARTIAL_UPDATE_ROWS: u8 = 39;
    pub const TRANSACTION_PAYLOAD: u8 = 40;
    pub const HEARTBEAT_V2: u8 = 41;
    pub const GTID_TAGGED: u8 = 42;
    pub const MARIADB_ANNOTATE_ROWS: u8 = 160;
    pub const MARIADB_BINLOG_CHECKPOINT: u8 = 161;
    pub const MARIADB_GTID: u8 = 162;
    pub const MARIADB_GTID_LIST: u8 = 163;
    pub const MARIADB_START_ENCRYPTION: u8 = 164;
    pub const MARIADB_QUERY_COMPRESSED: u8 = 165;
    pub const MARIADB_WRITE_ROWS_COMPRESSED_V1: u8 = 166;
    pub const MARIADB_UPDATE_ROWS_COMPRESSED_V1: u8 = 167;
    pub const MARIADB_DELETE_ROWS_COMPRESSED_V1: u8 = 168;
}

/// A ROTATE event's file name. MariaDB's fake ROTATE at the start of a dump comes before the
/// format description, so `mysql_common` doesn't know the checksum is on and leaves the 4 CRC32
/// bytes on the name (S-MY10b): strip them when the name only reads as `<base>.<6 digits>`
/// without them.
pub fn rotate_name(raw: &[u8]) -> &[u8] {
    fn file_name(n: &[u8]) -> bool {
        n.len() > 7 && n[n.len() - 7] == b'.' && n[n.len() - 6..].iter().all(u8::is_ascii_digit)
    }
    match raw.len().checked_sub(4) {
        Some(cut) if !file_name(raw) && file_name(&raw[..cut]) => &raw[..cut],
        _ => raw,
    }
}

/// MariaDB's `GTID_EVENT` (type 162): `seq_no` u64 LE, `domain_id` u32 LE, `flags2` u8; the
/// server id is the event header's. It opens a transaction, or a standalone statement (DDL).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MariaGtidEvent {
    pub gtid: MariaGtid,
    pub flags: u8,
}

impl MariaGtidEvent {
    /// The statement stands alone: no `BEGIN`, no `XID` (DDL).
    pub const STANDALONE: u8 = 0x01;
    /// The XA transaction's prepare, and its later commit or rollback.
    pub const PREPARED_XA: u8 = 0x40;
    pub const COMPLETED_XA: u8 = 0x80;

    /// From the event body (checksum already off) and the header's server id.
    pub fn parse(server_id: u32, data: &[u8]) -> Result<Self, String> {
        if data.len() < 13 {
            return Err(format!("a MariaDB GTID event of {} bytes", data.len()));
        }
        let seq = u64::from_le_bytes(data[0..8].try_into().expect("8 bytes"));
        let domain = u32::from_le_bytes(data[8..12].try_into().expect("4 bytes"));
        Ok(Self {
            gtid: MariaGtid {
                domain,
                server_id,
                seq,
            },
            flags: data[12],
        })
    }

    pub fn standalone(&self) -> bool {
        self.flags & Self::STANDALONE != 0
    }

    /// An XA transaction's prepare or completion: its rows can still roll back after the event.
    pub fn xa(&self) -> bool {
        self.flags & (Self::PREPARED_XA | Self::COMPLETED_XA) != 0
    }
}

/// The end offset of each event in its binlog file (Q-M7-4): the header's `log_pos` when the
/// server set it, else the last end plus the event's size. MySQL leaves `log_pos` at 0 inside a
/// compressed transaction and MariaDB 11.x on table maps and rows events (S-MY10f): the computed
/// offsets still grow, and equal the header's wherever it is set. Events inside a
/// `TRANSACTION_PAYLOAD` take the payload's end, and count its `uncompressed_size` down: an event
/// with a set `log_pos` (the next transaction) or a heartbeat before the count reaches zero means
/// the payload's events were lost (D-V1.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Offsets {
    file: String,
    end: u64,
    /// Bytes of the current payload's events still to come.
    inner: u64,
    /// The payload's `uncompressed_size`, for the message.
    payload: u64,
}

impl Offsets {
    /// At `pos` in `file`: where a dump starts, before its fake ROTATE.
    pub fn new(file: &str, pos: u64) -> Self {
        Self {
            file: file.to_owned(),
            end: pos,
            inner: 0,
            payload: 0,
        }
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    /// The end offset of the last event.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// True between a payload event and its last inner event.
    pub fn in_payload(&self) -> bool {
        self.inner > 0
    }

    fn lost(&self) -> String {
        format!(
            "the compressed transaction ending at {}:{} yielded {} of its {} bytes of events",
            self.file,
            self.end,
            self.payload - self.inner,
            self.payload
        )
    }

    /// A ROTATE (real or fake): the next events are in `file` from `pos`.
    pub fn rotate(&mut self, file: &str, pos: u64) -> Result<(), String> {
        if self.in_payload() {
            return Err(self.lost());
        }
        self.file = file.to_owned();
        self.end = pos;
        Ok(())
    }

    /// A heartbeat: it carries no event of the file, and can't come inside a payload.
    pub fn heartbeat(&self) -> Result<(), String> {
        match self.in_payload() {
            true => Err(self.lost()),
            false => Ok(()),
        }
    }

    /// An event of the file with header `log_pos` and `size`: its end offset.
    pub fn event(&mut self, log_pos: u32, size: u32) -> Result<u64, String> {
        let size = u64::from(size);
        if self.in_payload() {
            if log_pos != 0 {
                return Err(self.lost());
            }
            if size > self.inner {
                return Err(format!(
                    "an event of {size} bytes overruns the compressed transaction ending at {}:{} ({} bytes left)",
                    self.file, self.end, self.inner
                ));
            }
            self.inner -= size;
            return Ok(self.end);
        }
        self.end = match log_pos {
            0 => self.end + size,
            set => u64::from(set),
        };
        Ok(self.end)
    }

    /// After [`Offsets::event`] for a `TRANSACTION_PAYLOAD`: its events, `uncompressed_size`
    /// bytes, follow. A payload without events is invalid.
    pub fn payload(&mut self, uncompressed_size: u64) -> Result<(), String> {
        if uncompressed_size == 0 {
            return Err(format!(
                "the compressed transaction ending at {}:{} holds no events",
                self.file, self.end
            ));
        }
        self.inner = uncompressed_size;
        self.payload = uncompressed_size;
        Ok(())
    }
}

/// The uncompressed type of a MariaDB compressed event the source reads (measured on 11.8 and
/// 10.11: rows events v1, and statements). The v2 rows forms (169–171) were never seen; they
/// halt the pipeline instead.
pub fn plain_type(raw: u8) -> Option<u8> {
    use types::*;
    Some(match raw {
        MARIADB_QUERY_COMPRESSED => QUERY,
        MARIADB_WRITE_ROWS_COMPRESSED_V1 => WRITE_ROWS_V1,
        MARIADB_UPDATE_ROWS_COMPRESSED_V1 => UPDATE_ROWS_V1,
        MARIADB_DELETE_ROWS_COMPRESSED_V1 => DELETE_ROWS_V1,
        _ => return None,
    })
}

/// MariaDB's compressed block: a header byte (`0x80` | the length of the length), the
/// uncompressed length big-endian, then zlib.
fn inflate(block: &[u8]) -> Result<Vec<u8>, String> {
    let header = *block.first().ok_or("an empty compressed block")?;
    let lenlen = usize::from(header & 0x07);
    if header & 0x80 == 0 || !(1..=4).contains(&lenlen) || block.len() <= 1 + lenlen {
        return Err(format!("a compressed block with header {header:#04x}"));
    }
    let len = block[1..1 + lenlen]
        .iter()
        .fold(0usize, |a, b| (a << 8) | usize::from(*b));
    let mut out = Vec::with_capacity(len);
    flate2::read::ZlibDecoder::new(&block[1 + lenlen..])
        .read_to_end(&mut out)
        .map_err(|e| format!("zlib: {e}"))?;
    if out.len() != len {
        return Err(format!(
            "inflated {} bytes, the header says {len}",
            out.len()
        ));
    }
    Ok(out)
}

/// `e`'s header with type `ty` and body `data`, read under `e`'s format description (with a
/// placeholder checksum when the format has one: reading doesn't verify it).
fn rebuild(e: &Event, ty: u8, data: &[u8]) -> Result<Event, String> {
    let h = e.header();
    let footer = e.fde().footer();
    let checksum = footer.get_checksum_enabled()
        && footer
            .get_checksum_alg()
            .ok()
            .flatten()
            .is_some_and(|a| a as u8 != 0);
    let size = 19 + data.len() + if checksum { 4 } else { 0 };
    let mut buf = Vec::with_capacity(size);
    buf.extend_from_slice(&h.timestamp().to_le_bytes());
    buf.push(ty);
    buf.extend_from_slice(&h.server_id().to_le_bytes());
    buf.extend_from_slice(
        &u32::try_from(size)
            .map_err(|e| e.to_string())?
            .to_le_bytes(),
    );
    buf.extend_from_slice(&h.log_pos().to_le_bytes());
    buf.extend_from_slice(&h.flags_raw().to_le_bytes());
    buf.extend_from_slice(data);
    if checksum {
        buf.extend_from_slice(&[0; 4]);
    }
    Event::read(e.fde(), &buf[..]).map_err(|e| e.to_string())
}

/// A MariaDB compressed event as its uncompressed form: read under the plain type, the event
/// names where its compressed tail starts (the rows, or the statement text); inflated, it is
/// read again. `None` for an event that isn't one.
pub fn decompress(e: &Event) -> Result<Option<Event>, String> {
    let Some(ty) = plain_type(e.header().event_type_raw()) else {
        return Ok(None);
    };
    let shell = rebuild(e, ty, e.data())?;
    let tail = match shell.read_data().map_err(|e| e.to_string())? {
        Some(EventData::QueryEvent(q)) => q.query_raw().len(),
        Some(EventData::RowsEvent(r)) => r.rows_data().len(),
        other => return Err(format!("a compressed event reads as {other:?}")),
    };
    let data = e.data();
    let (head, compressed) = data.split_at(data.len() - tail);
    let mut body = head.to_vec();
    body.extend(inflate(compressed)?);
    rebuild(e, ty, &body).map(Some)
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 26 tests.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-source-mysql/Cargo.toml \
  crates/data-avalanche-source-mysql/src/binlog.rs \
  crates/data-avalanche-source-mysql/src/lib.rs
git commit -m "feat(source-mysql): binlog primitives — order offsets and the payload check, MariaDB GTID events, compressed events, rotate names"
```

---

### Task 7: Hop 1: type plans and one decoder for binlog and chunk values

**Files:**
- Modify: `crates/data-avalanche-source-mysql/Cargo.toml`
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`
- Create: `crates/data-avalanche-source-mysql/src/types.rs`
- Create: `crates/data-avalanche-source-mysql/src/value.rs`
- Modify: `crates/data-avalanche-source-mysql/tests/mysql/main.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/values.rs`

**Interfaces:**
- Consumes:
  - `data_avalanche_core::{ColumnValue, ColumnValueType, LogicalType, HaltReason}`, the lossless
    conversions of `convert` (M5, Q-M5-12).
  - `data_avalanche_config::{NumericOverflowAction, Tinyint1AsBool}` (M1).
- Produces:
  - `data-avalanche-source-mysql/src/lib.rs`:
    - `pub mod types;`
    - `pub mod value;`
  - `data-avalanche-source-mysql/src/types.rs`:
    - `pub struct CatalogColumn`
    - `pub type Tinyint1AsBool = bool;`
    - `pub struct ColumnPlan`
    - `pub fn members(column_type: &str) -> Vec<String>`
    - `pub fn plan_column(`
  - `data-avalanche-source-mysql/src/value.rs`:
    - `pub enum Charset`
    - `pub fn of(name: &str) -> Option<Self>`
    - `pub enum Decode`
    - `pub fn int_type(bits: u32, unsigned: bool) -> ColumnValueType`
    - `pub enum Input<'a>`
    - `pub enum ValueError`
    - `pub fn decode(d: &Decode, input: Input<'_>, identifier: bool) -> Result<ColumnValue, ValueError>`

Type_Mapping's MySQL table as code (D-M7a.10). `plan_column` maps a catalog column
(`CatalogColumn`: `DATA_TYPE`, `COLUMN_TYPE`, charset, precision, scale, `fsp`, length) to its
value type and decoder (`ColumnPlan`), with its `column_type_overrides` entry. `decode` reads a
value from either input (`Input::Binlog` from a rows event, `Input::Row` from a chunk's
prepared statement) through one plan, so the stream and a snapshot agree by construction (Task 15
checks it).

Rulings and measurements (the type probe, MySQL 8.4 and MariaDB 11.8 / 10.11):

- `mysql_common` reads a signed `MEDIUMINT` without its sign (−8388608 arrives as 8388608):
  the decoder sign-extends it.
- `YEAR` 0000 arrives from the binlog as the text `1900` (`mysql_common` adds 1900): read as 0.
- `BINARY(n)`, and MariaDB `UUID`, `INET4`, `INET6`, arrive with trailing `0x00` stripped:
  padded back to the declared length.
- Charsets (Q-M7a-2): the UTF-8 family (`utf8mb4`, `utf8mb3`, `ascii`) and `latin1`, which is
  cp1252 (its table equals the server's `CONVERT` for bytes 1…255, measured); `binary` reads as
  bytes; any other charset halts the table at resolution, naming the column.
- MariaDB (Q-M7a-4): `UUID` → uuid, `INET4`/`INET6` → their MariaDB text, `VECTOR` → bytes.
  MariaDB prints IPv6 as MySQL's `ipv6_to_str` does, not as RFC 5952 (Rust's `Ipv6Addr`): the
  port matches 11.8 on 21 fixed and 300 random sparse addresses.
- `GEOMETRY` (Q-M7a-7): bytes, as stored (SRID, 4 bytes little-endian, then WKB).
- Dates (Q-M7-1): an all-zero date → NULL (a halt in a key column); zero-in-part and impossible
  dates halt the table (`INVALID_DATE_VALUE`), and a `string` override keeps the server's text.
- A `string` override on a byte column halts (`SCHEMA_MISMATCH`): bytes have no text. A float
  under `string` is Rust's shortest text, the same from both inputs; JSON is compact.
- MariaDB's `JSON` is `LONGTEXT`: it plans as text.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-source-mysql/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-source-mysql/Cargo.toml b/crates/data-avalanche-source-mysql/Cargo.toml
--- a/crates/data-avalanche-source-mysql/Cargo.toml
+++ b/crates/data-avalanche-source-mysql/Cargo.toml
@@ -10,13 +10,17 @@ description = "The MySQL and MariaDB source: the binlog over mysql_async, catalo
 workspace = true
 
 [dependencies]
+chrono.workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
 flate2.workspace = true
 futures-util.workspace = true
 mysql_async.workspace = true
 rustls.workspace = true
+serde_json.workspace = true
+thiserror.workspace = true
 tokio = { workspace = true, features = ["sync", "time"] }
+uuid.workspace = true
 
 [dev-dependencies]
 data-avalanche-testkit.workspace = true
```

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -10,9 +10,13 @@ mod connector;
 mod error;
 pub mod position;
 pub mod server;
+pub mod types;
+pub mod value;
 
 pub use binlog::{MariaGtidEvent, Offsets, decompress, rotate_name};
 pub use connect::SESSION_SETUP;
 pub use connector::MySqlConnector;
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
 pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
+pub use types::{CatalogColumn, ColumnPlan, members, plan_column};
+pub use value::{Charset, Decode, Input, ValueError, decode, int_type};
```

Change `crates/data-avalanche-source-mysql/tests/mysql/main.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/main.rs b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/main.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
@@ -7,3 +7,4 @@ mod common;
 mod position;
 mod preflight;
 mod tls;
+mod values;
```

Create `crates/data-avalanche-source-mysql/src/types.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn col(data_type: &str, column_type: &str) -> CatalogColumn {
        CatalogColumn {
            name: "c".into(),
            data_type: data_type.into(),
            column_type: column_type.into(),
            charset: Some("utf8mb4".into()),
            ..Default::default()
        }
    }

    fn planned(c: &CatalogColumn) -> (V, Decode) {
        let p = plan_column(c, true, NumericOverflowAction::Error, None, false).unwrap();
        (p.value_type, p.decode)
    }

    #[test]
    fn integers_widen_unsigned_and_tinyint1_is_bool() {
        let t = |d: &str, ct: &str| planned(&col(d, ct)).0;
        assert_eq!(t("tinyint", "tinyint"), V::Int16);
        assert_eq!(t("tinyint", "tinyint unsigned"), V::Int16);
        assert_eq!(t("tinyint", "tinyint(1)"), V::Bool);
        assert_eq!(t("tinyint", "tinyint(1) unsigned"), V::Int16);
        assert_eq!(t("smallint", "smallint unsigned"), V::Int32);
        assert_eq!(t("mediumint", "mediumint"), V::Int32);
        assert_eq!(t("int", "int unsigned"), V::Int64);
        assert_eq!(t("bigint", "bigint"), V::Int64);
        assert_eq!(t("bigint", "bigint unsigned"), V::UInt64);
        let p = plan_column(
            &col("tinyint", "tinyint(1)"),
            false,
            NumericOverflowAction::Error,
            None,
            false,
        )
        .unwrap();
        assert_eq!(p.value_type, V::Int16, "tinyint1_as_bool off");
    }

    #[test]
    fn decimals_wider_than_38_digits_follow_the_overflow_action() {
        let mut c = col("decimal", "decimal(65,30)");
        (c.precision, c.scale) = (Some(65), Some(30));
        assert_eq!(
            plan_column(&c, true, NumericOverflowAction::Error, None, false).unwrap_err(),
            HaltReason::NumericOverflow { column: "c".into() }
        );
        let p = plan_column(&c, true, NumericOverflowAction::String, None, false).unwrap();
        assert_eq!(p.value_type, V::Text);
        assert_eq!(p.read_as_text.as_deref(), Some("decimal(65,30)"));
        // The V-079 remedy: an override to a decimal that fits.
        let d = LogicalType::Decimal {
            precision: 38,
            scale: 10,
        };
        let p = plan_column(&c, true, NumericOverflowAction::Error, Some(&d), false).unwrap();
        assert_eq!(
            p.value_type,
            V::Numeric {
                precision: 38,
                scale: 10
            }
        );
        (c.precision, c.scale) = (Some(10), Some(2));
        assert_eq!(
            planned(&c).0,
            V::Numeric {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn characters_read_by_charset() {
        let mut c = col("varchar", "varchar(20)");
        for cs in ["utf8mb4", "utf8mb3", "utf8", "ascii"] {
            c.charset = Some(cs.into());
            assert_eq!(
                planned(&c).1,
                Decode::Text {
                    charset: Charset::Utf8
                },
                "{cs}"
            );
        }
        c.charset = Some("latin1".into());
        assert_eq!(
            planned(&c).1,
            Decode::Text {
                charset: Charset::Latin1
            }
        );
        c.charset = Some("binary".into());
        assert_eq!(planned(&c), (V::Bytes, Decode::Bytes { pad: None }));
        c.charset = Some("sjis".into());
        let e = plan_column(&c, true, NumericOverflowAction::Error, None, false).unwrap_err();
        assert!(
            matches!(&e, HaltReason::SchemaMismatch { detail, .. } if detail.contains("sjis")),
            "{e:?}"
        );
    }

    #[test]
    fn the_other_types_follow_the_table() {
        let mut bin = col("binary", "binary(4)");
        bin.max_length = Some(4);
        assert_eq!(planned(&bin), (V::Bytes, Decode::Bytes { pad: Some(4) }));
        let mut dt = col("datetime", "datetime(3)");
        dt.fsp = Some(3);
        assert_eq!(
            planned(&dt),
            (V::Timestamp { with_tz: false }, Decode::DateTime { fsp: 3 })
        );
        assert_eq!(
            planned(&col("timestamp", "timestamp")).0,
            V::Timestamp { with_tz: true }
        );
        assert_eq!(planned(&col("year", "year")), (V::Int32, Decode::Year));
        assert_eq!(planned(&col("json", "json")).0, V::Json);
        assert_eq!(
            planned(&col("enum", "enum('a','b')")),
            (
                V::Text,
                Decode::Enum {
                    members: vec!["a".into(), "b".into()]
                }
            )
        );
        assert_eq!(
            planned(&col("set", "set('x','y')")).0,
            V::Array(Box::new(V::Text))
        );
        assert_eq!(planned(&col("geometry", "point")).0, V::Bytes);
        assert_eq!(planned(&col("uuid", "uuid")).0, V::Uuid);
        assert_eq!(planned(&col("inet6", "inet6")), (V::Text, Decode::Inet6));
        assert_eq!(planned(&col("vector", "vector(3)")).0, V::Bytes);
        assert!(
            plan_column(
                &col("hyperloglog", "x"),
                true,
                NumericOverflowAction::Error,
                None,
                false
            )
            .is_err()
        );
    }

    #[test]
    fn enum_and_set_members_undo_their_quoting() {
        assert_eq!(members("enum('a','b','c')"), ["a", "b", "c"]);
        assert_eq!(members("set('it''s','a,b')"), ["it's", "a,b"]);
        assert_eq!(members("enum('it''s','x\\\\y','')"), ["it's", "x\\y", ""]);
        assert_eq!(members("enum()"), Vec::<String>::new());
    }

    #[test]
    fn overrides_keep_text_convert_losslessly_or_halt() {
        let mut d = col("date", "date");
        let s = plan_column(
            &d,
            true,
            NumericOverflowAction::Error,
            Some(&LogicalType::String),
            false,
        )
        .unwrap();
        assert_eq!(s.value_type, V::Text);
        assert_eq!(s.decode, Decode::String(Box::new(Decode::Date)));
        let ts = plan_column(
            &d,
            true,
            NumericOverflowAction::Error,
            Some(&LogicalType::TimestampNtz),
            false,
        )
        .unwrap();
        assert_eq!(ts.value_type, V::Timestamp { with_tz: false });
        assert!(matches!(ts.decode, Decode::Convert { .. }));
        let i = plan_column(
            &col("int", "int"),
            true,
            NumericOverflowAction::Error,
            Some(&LogicalType::Int64),
            false,
        )
        .unwrap();
        assert_eq!(i.value_type, V::Int64);
        d.data_type = "blob".into();
        assert!(
            plan_column(
                &d,
                true,
                NumericOverflowAction::Error,
                Some(&LogicalType::String),
                false
            )
            .is_err()
        );
        assert!(
            plan_column(
                &col("bigint", "bigint"),
                true,
                NumericOverflowAction::Error,
                Some(&LogicalType::Int32),
                false
            )
            .is_err()
        );
    }
}
```

Create `crates/data-avalanche-source-mysql/src/value.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn row(d: &Decode, v: Value) -> Result<ColumnValue, ValueError> {
        decode(d, Input::Row(&v), false)
    }

    fn binlog(d: &Decode, v: Value) -> Result<ColumnValue, ValueError> {
        decode(d, Input::Binlog(&BinlogValue::Value(v)), false)
    }

    /// Both inputs as measured (S-MY type probe): the binlog's form and the prepared statement's
    /// decode to `want`.
    fn both(d: &Decode, from_binlog: Value, from_row: Value, want: ColumnValue) {
        assert_eq!(binlog(d, from_binlog).unwrap(), want, "binlog, {d:?}");
        assert_eq!(row(d, from_row).unwrap(), want, "row, {d:?}");
    }

    fn b(s: &[u8]) -> Value {
        Value::Bytes(s.to_vec())
    }

    #[test]
    fn integers_read_with_the_catalogs_signedness() {
        let mi = Decode::Int {
            bits: 24,
            unsigned: false,
        };
        // mysql_common reads a signed MEDIUMINT's -8388608 as 8388608.
        both(
            &mi,
            Value::Int(8_388_608),
            Value::Int(-8_388_608),
            ColumnValue::Int32(-8_388_608),
        );
        let tiu = Decode::Int {
            bits: 8,
            unsigned: true,
        };
        // Without FULL row metadata, an unsigned 255 reads as -1.
        both(
            &tiu,
            Value::Int(-1),
            Value::Int(255),
            ColumnValue::Int16(255),
        );
        let iu = Decode::Int {
            bits: 32,
            unsigned: true,
        };
        both(
            &iu,
            Value::Int(4_294_967_295),
            Value::Int(4_294_967_295),
            ColumnValue::Int64(4_294_967_295),
        );
        let biu = Decode::Int {
            bits: 64,
            unsigned: true,
        };
        both(
            &biu,
            Value::UInt(u64::MAX),
            Value::UInt(u64::MAX),
            ColumnValue::UInt64(u64::MAX),
        );
        assert_eq!(
            binlog(&biu, Value::Int(-1)).unwrap(),
            ColumnValue::UInt64(u64::MAX)
        );
        let bi = Decode::Int {
            bits: 64,
            unsigned: false,
        };
        both(
            &bi,
            Value::Int(i64::MIN),
            Value::Int(i64::MIN),
            ColumnValue::Int64(i64::MIN),
        );
        assert!(
            binlog(
                &Decode::Int {
                    bits: 16,
                    unsigned: false
                },
                Value::Int(70_000)
            )
            .is_err()
        );
        both(
            &Decode::Bool,
            Value::Int(2),
            Value::Int(2),
            ColumnValue::Bool(true),
        );
        both(
            &Decode::Bool,
            Value::Int(0),
            Value::Int(0),
            ColumnValue::Bool(false),
        );
    }

    #[test]
    fn floats_decimals_and_nulls() {
        both(
            &Decode::Float32,
            Value::Float(1.5),
            Value::Float(1.5),
            ColumnValue::Float32(1.5),
        );
        both(
            &Decode::Float64,
            Value::Double(2.25),
            Value::Double(2.25),
            ColumnValue::Float64(2.25),
        );
        both(
            &Decode::Decimal,
            b(b"12345678.90"),
            b(b"12345678.90"),
            ColumnValue::Numeric(Decimal::parse("12345678.90").unwrap()),
        );
        both(
            &Decode::Decimal,
            Value::NULL,
            Value::NULL,
            ColumnValue::Null,
        );
        let wide = "12345678901234567890123456789012345.123456789012345678901234567891";
        assert_eq!(
            binlog(&Decode::DecimalText, b(wide.as_bytes())).unwrap(),
            ColumnValue::Text(wide.into())
        );
        assert_eq!(
            binlog(&Decode::Decimal, b(wide.as_bytes())),
            Err(ValueError::Overflow)
        );
    }

    #[test]
    fn text_reads_its_charset_from_the_binlog_and_utf8_from_a_chunk() {
        let latin = Decode::Text {
            charset: Charset::Latin1,
        };
        both(
            &latin,
            b(&[0x64, 0xe9, 0x6a, 0xe0]),
            b("déjà".as_bytes()),
            ColumnValue::Text("déjà".into()),
        );
        assert_eq!(latin1(&[0x80, 0x81, 0x9f, 0xff]), "€\u{81}Ÿÿ");
        let u = Decode::Text {
            charset: Charset::Utf8,
        };
        both(
            &u,
            b("héllo 🎉".as_bytes()),
            b("héllo 🎉".as_bytes()),
            ColumnValue::Text("héllo 🎉".into()),
        );
        assert!(binlog(&u, b(&[0xff])).is_err());
    }

    #[test]
    fn binary_n_and_mariadb_types_get_their_stripped_zeros_back() {
        both(
            &Decode::Bytes { pad: Some(4) },
            b(b"ab"),
            b(b"ab\0\0"),
            ColumnValue::Bytes(b"ab\0\0".to_vec()),
        );
        both(
            &Decode::Bytes { pad: None },
            b(b"a\0b"),
            b(b"a\0b"),
            ColumnValue::Bytes(b"a\0b".to_vec()),
        );
        let u = uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        both(
            &Decode::Uuid,
            b(&u.as_bytes()[..15]),
            b(b"123e4567-e89b-12d3-a456-426614174000"),
            ColumnValue::Uuid(u),
        );
        both(
            &Decode::Uuid,
            b(&[]),
            b(b"00000000-0000-0000-0000-000000000000"),
            ColumnValue::Uuid(uuid::Uuid::nil()),
        );
        both(
            &Decode::Inet4,
            b(&[192, 168, 0, 1]),
            b(b"192.168.0.1"),
            ColumnValue::Text("192.168.0.1".into()),
        );
        both(
            &Decode::Inet4,
            b(&[]),
            b(b"0.0.0.0"),
            ColumnValue::Text("0.0.0.0".into()),
        );
        let v6 = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        both(
            &Decode::Inet6,
            b(&v6),
            b(b"2001:db8::1"),
            ColumnValue::Text("2001:db8::1".into()),
        );
        both(
            &Decode::Inet6,
            b(&[]),
            b(b"::"),
            ColumnValue::Text("::".into()),
        );
        let compat = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 192, 168, 0, 1];
        both(
            &Decode::Inet6,
            b(&compat),
            b(b"::192.168.0.1"),
            ColumnValue::Text("::192.168.0.1".into()),
        );
        assert_eq!(inet6([0; 16]), "::");
        let mut two = [0; 16];
        two[15] = 2;
        assert_eq!(inet6(two), "::2");
        assert_eq!(inet6(std::net::Ipv6Addr::LOCALHOST.octets()), "::1");
        let w = |s: &str| inet6(s.parse::<std::net::Ipv6Addr>().unwrap().octets());
        assert_eq!(w("1:0:2:0:3:0:4:0"), "1::2:0:3:0:4:0");
        assert_eq!(w("1:0:0:2:0:0:0:3"), "1:0:0:2::3");
        assert_eq!(w("1::"), "1::");
        assert_eq!(w("::ffff:1.2.3.4"), "::ffff:1.2.3.4");
        assert_eq!(w("1:2:3:4:5:6:7:8"), "1:2:3:4:5:6:7:8");
    }

    #[test]
    fn dates_times_and_timestamps() {
        let ntz = |micros| ColumnValue::Timestamp {
            micros,
            with_tz: false,
        };
        both(
            &Decode::Date,
            Value::Date(2024, 2, 29, 0, 0, 0, 0),
            Value::Date(2024, 2, 29, 0, 0, 0, 0),
            ColumnValue::Date(19_782),
        );
        both(
            &Decode::DateTime { fsp: 6 },
            Value::Date(2024, 2, 29, 23, 59, 59, 999_999),
            Value::Date(2024, 2, 29, 23, 59, 59, 999_999),
            ntz(1_709_251_199_999_999),
        );
        // The binlog's TIMESTAMP is epoch text in UTC; a chunk's is the session's (UTC) clock.
        both(
            &Decode::Timestamp { fsp: 6 },
            b(b"1709251199.123456"),
            Value::Date(2024, 2, 29, 23, 59, 59, 123_456),
            ColumnValue::Timestamp {
                micros: 1_709_251_199_123_456,
                with_tz: true,
            },
        );
        both(
            &Decode::Timestamp { fsp: 3 },
            b(b"1.5"),
            Value::Date(1970, 1, 1, 0, 0, 1, 500_000),
            ColumnValue::Timestamp {
                micros: 1_500_000,
                with_tz: true,
            },
        );
        both(
            &Decode::Time { fsp: 6 },
            Value::Time(true, 34, 22, 59, 59, 0),
            Value::Time(true, 34, 22, 59, 59, 0),
            ColumnValue::Time(-(838 * 3600 + 59 * 60 + 59) * 1_000_000),
        );
        // YEAR: mysql_common reads 0000 as 1900 from the binlog.
        both(
            &Decode::Year,
            b(b"2024"),
            Value::Int(2024),
            ColumnValue::Int32(2024),
        );
        both(
            &Decode::Year,
            b(b"1900"),
            Value::Int(0),
            ColumnValue::Int32(0),
        );
    }

    #[test]
    fn all_zero_dates_are_null_and_invalid_dates_halt() {
        let zero_dt = Value::Date(0, 0, 0, 0, 0, 0, 0);
        both(
            &Decode::Date,
            zero_dt.clone(),
            zero_dt.clone(),
            ColumnValue::Null,
        );
        both(
            &Decode::Timestamp { fsp: 0 },
            b(b"0"),
            zero_dt.clone(),
            ColumnValue::Null,
        );
        // In an identifier, all-zero has no NULL to become.
        assert_eq!(
            decode(&Decode::DateTime { fsp: 0 }, Input::Row(&zero_dt), true),
            Err(ValueError::InvalidDate {
                raw: "0000-00-00 00:00:00".into()
            })
        );
        for (d, v, raw) in [
            (
                Decode::Date,
                Value::Date(2024, 0, 15, 0, 0, 0, 0),
                "2024-00-15",
            ),
            (
                Decode::Date,
                Value::Date(2024, 2, 31, 0, 0, 0, 0),
                "2024-02-31",
            ),
            (
                Decode::DateTime { fsp: 2 },
                Value::Date(2024, 5, 0, 10, 0, 0, 0),
                "2024-05-00 10:00:00.00",
            ),
            (
                Decode::DateTime { fsp: 0 },
                Value::Date(0, 0, 0, 10, 0, 0, 0),
                "0000-00-00 10:00:00",
            ),
        ] {
            assert_eq!(
                binlog(&d, v),
                Err(ValueError::InvalidDate { raw: raw.into() })
            );
        }
    }

    #[test]
    fn json_enum_set_from_both_forms() {
        both(
            &Decode::Json,
            b(br#"{"a": [1, 2.5]}"#),
            b(br#"{"a": [1, 2.5]}"#),
            ColumnValue::Json(serde_json::json!({"a": [1, 2.5]})),
        );
        let e = Decode::Enum {
            members: vec!["a".into(), "b".into(), "c".into()],
        };
        both(&e, Value::Int(2), b(b"b"), ColumnValue::Text("b".into()));
        both(&e, Value::Int(0), b(b""), ColumnValue::Text("".into()));
        assert!(binlog(&e, Value::Int(4)).is_err());
        let s = Decode::Set {
            members: vec!["x".into(), "y".into(), "z".into()],
        };
        let labels = |l: &[&str]| ColumnValue::Array {
            element_type: ColumnValueType::Text,
            values: l.iter().map(|x| ColumnValue::Text(x.to_string())).collect(),
        };
        both(&s, b(&[0b101]), b(b"x,z"), labels(&["x", "z"]));
        both(&s, b(&[0]), b(b""), labels(&[]));
        let partial = BinlogValue::JsonDiff(vec![]);
        assert!(decode(&Decode::Json, Input::Binlog(&partial), false).is_err());
    }

    #[test]
    fn string_overrides_keep_the_servers_text() {
        let s = |d: Decode| Decode::String(Box::new(d));
        let t = |x: &str| ColumnValue::Text(x.into());
        both(
            &s(Decode::Date),
            Value::Date(2024, 0, 15, 0, 0, 0, 0),
            Value::Date(2024, 0, 15, 0, 0, 0, 0),
            t("2024-00-15"),
        );
        both(
            &s(Decode::DateTime { fsp: 3 }),
            Value::Date(2024, 2, 29, 23, 59, 59, 999_000),
            Value::Date(2024, 2, 29, 23, 59, 59, 999_000),
            t("2024-02-29 23:59:59.999"),
        );
        both(
            &s(Decode::Timestamp { fsp: 0 }),
            b(b"0"),
            Value::Date(0, 0, 0, 0, 0, 0, 0),
            t("0000-00-00 00:00:00"),
        );
        both(
            &s(Decode::Time { fsp: 0 }),
            Value::Time(false, 0, 12, 0, 0, 0),
            Value::Time(false, 0, 12, 0, 0, 0),
            t("12:00:00"),
        );
        both(
            &s(Decode::Time { fsp: 6 }),
            Value::Time(true, 34, 22, 59, 59, 1),
            Value::Time(true, 34, 22, 59, 59, 1),
            t("-838:59:59.000001"),
        );
        both(&s(Decode::Year), b(b"1900"), Value::Int(0), t("0000"));
        both(
            &s(Decode::Int {
                bits: 24,
                unsigned: false,
            }),
            Value::Int(8_388_608),
            Value::Int(-8_388_608),
            t("-8388608"),
        );
        let set = Decode::Set {
            members: vec!["x".into(), "y".into(), "z".into()],
        };
        both(&s(set), b(&[0b110]), b(b"y,z"), t("y,z"));
        both(
            &s(Decode::Json),
            b(br#"{"a": 1}"#),
            b(br#"{"a": 1}"#),
            t(r#"{"a":1}"#),
        );
    }

    #[test]
    fn conversions_apply_after_decoding() {
        let d = Decode::Convert {
            from: Box::new(Decode::Int {
                bits: 16,
                unsigned: false,
            }),
            to: ColumnValueType::Int64,
        };
        both(&d, Value::Int(-5), Value::Int(-5), ColumnValue::Int64(-5));
    }
}
```

Create `crates/data-avalanche-source-mysql/tests/mysql/values.rs`:

```rust
//! The decoder against the server's own conversions, value by value: what the binlog carries
//! (raw bytes) decodes to what a chunk reads (the server's text).

use data_avalanche_core::ColumnValue;
use data_avalanche_source_mysql::{Charset, Decode, Flavour, Input, decode};
use data_avalanche_testkit::require_it;
use mysql_async::Value;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::prelude::Queryable;

use crate::common::{admin_in, target};

fn from_binlog(d: &Decode, bytes: Vec<u8>) -> ColumnValue {
    decode(
        d,
        Input::Binlog(&BinlogValue::Value(Value::Bytes(bytes))),
        false,
    )
    .unwrap()
}

fn from_row(d: &Decode, v: &Value) -> ColumnValue {
    decode(d, Input::Row(v), false).unwrap()
}

/// Every latin1 byte: the binlog's raw byte decodes to the character the server converts it to.
#[tokio::test]
async fn latin1_bytes_decode_as_the_server_converts_them() {
    require_it!();
    let mut a = admin_in("avalanche").await;
    let d = Decode::Text {
        charset: Charset::Latin1,
    };
    for byte in 1..=255u8 {
        let v: Value = a
            .exec_first(
                "SELECT CONVERT(UNHEX(?) USING latin1)",
                (format!("{byte:02X}"),),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            from_binlog(&d, vec![byte]),
            from_row(&d, &v),
            "byte {byte:#04x}"
        );
    }
}

/// MariaDB `INET6` and `INET4`: the stored bytes, trailing zeros stripped as the binlog strips
/// them, print as MariaDB prints the value.
#[tokio::test]
async fn mariadb_addresses_print_as_mariadb_prints_them() {
    require_it!();
    if target().flavour != Flavour::Mariadb {
        eprintln!("skipped: MariaDB only");
        return;
    }
    let mut a = admin_in("avalanche").await;
    for (d, kind, text) in [
        (Decode::Inet6, "INET6", "2001:db8::1"),
        (Decode::Inet6, "INET6", "::"),
        (Decode::Inet6, "INET6", "::1"),
        (Decode::Inet6, "INET6", "1::"),
        (Decode::Inet6, "INET6", "1:0:0:2::3"),
        (Decode::Inet6, "INET6", "1:0:0:2:0:0:0:3"),
        (Decode::Inet6, "INET6", "::ffff:192.168.0.1"),
        (Decode::Inet6, "INET6", "::192.168.0.1"),
        (Decode::Inet6, "INET6", "::2"),
        (Decode::Inet6, "INET6", "::0.0.1.0"),
        (Decode::Inet6, "INET6", "::1:0:0"),
        (Decode::Inet6, "INET6", "::1:2"),
        (Decode::Inet6, "INET6", "::ffff:0.0.0.1"),
        (Decode::Inet6, "INET6", "0:0:0:0:0:1:0:0"),
        (Decode::Inet6, "INET6", "1:0:2:0:3:0:4:0"),
        (Decode::Inet6, "INET6", "fe80::1:2:3:4"),
        (
            Decode::Inet6,
            "INET6",
            "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        ),
        (Decode::Inet4, "INET4", "192.168.0.1"),
        (Decode::Inet4, "INET4", "0.0.0.0"),
        (Decode::Inet4, "INET4", "10.0.0.0"),
    ] {
        let (hex, printed): (String, Value) = a
            .exec_first(
                format!("SELECT HEX(CAST(? AS {kind})), CAST(? AS {kind})"),
                (text, text),
            )
            .await
            .unwrap()
            .unwrap();
        let mut bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        while bytes.last() == Some(&0) {
            bytes.pop();
        }
        assert_eq!(
            from_binlog(&d, bytes),
            from_row(&d, &printed),
            "{kind} {text}"
        );
    }
}

/// 300 sparse addresses (each group zero with probability ½, so runs of every length and place)
/// print as MariaDB prints them.
#[tokio::test]
async fn sparse_mariadb_inet6_addresses_print_as_mariadb_prints_them() {
    require_it!();
    if target().flavour != Flavour::Mariadb {
        eprintln!("skipped: MariaDB only");
        return;
    }
    let mut a = admin_in("avalanche").await;
    let mut seed: u64 = 0x5eed;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as u16
    };
    for _ in 0..300 {
        let words: Vec<u16> = (0..8)
            .map(|_| if next() % 2 == 0 { 0 } else { next() })
            .collect();
        let hex: String = words.iter().map(|w| format!("{w:04X}")).collect();
        let printed: Value = a
            .exec_first("SELECT CAST(UNHEX(?) AS INET6)", (hex.clone(),))
            .await
            .unwrap()
            .unwrap();
        let mut bytes: Vec<u8> = words.iter().flat_map(|w| w.to_be_bytes()).collect();
        while bytes.last() == Some(&0) {
            bytes.pop();
        }
        assert_eq!(
            from_binlog(&Decode::Inet6, bytes),
            from_row(&Decode::Inet6, &printed),
            "{hex}"
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `types::CatalogColumn`, `types::ColumnPlan`, `types::members`, `types::plan_column`
error[E0432]: unresolved imports `value::Charset`, `value::Decode`, `value::Input`, `value::ValueError`, `value::decode`, `value::int_type`
error[E0425]: cannot find type `CatalogColumn` in this scope
error[E0422]: cannot find struct, variant or union type `CatalogColumn` in this scope
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-source-mysql/src/types.rs`, add above the test module:

```rust
//! Type planning (`Type_Mapping.md`, the MySQL and MariaDB tables): a column as the catalog
//! describes it → its `ColumnValueType` and the [`Decode`] for its values. One plan serves both
//! inputs, the binlog and a snapshot chunk (S-MY type probe, 2026-10-14).

use data_avalanche_config::NumericOverflowAction;
use data_avalanche_core::{
    ColumnValueType as V, HaltReason, LogicalType, MAX_DECIMAL_PRECISION, canonical_logical,
    conversion, hop2_compatible,
};

use crate::value::{Charset, Decode, int_type};

/// A column as `information_schema.COLUMNS` describes it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CatalogColumn {
    pub name: String,
    /// `DATA_TYPE`, lower case: `int`, `varchar`, `enum`, MariaDB `uuid`, `inet6`.
    pub data_type: String,
    /// `COLUMN_TYPE`: `int unsigned`, `tinyint(1)`, `enum('a','b')`, `binary(4)`.
    pub column_type: String,
    /// `CHARACTER_SET_NAME`, for character columns.
    pub charset: Option<String>,
    /// `NUMERIC_PRECISION` / `NUMERIC_SCALE`, for decimals.
    pub precision: Option<u32>,
    pub scale: Option<u32>,
    /// `DATETIME_PRECISION`: the fractional digits of a `TIME`, `DATETIME` or `TIMESTAMP`.
    pub fsp: Option<u32>,
    /// `CHARACTER_MAXIMUM_LENGTH`: `n` of a `BINARY(n)`.
    pub max_length: Option<u64>,
}

impl CatalogColumn {
    fn unsigned(&self) -> bool {
        self.column_type.contains("unsigned")
    }

    fn fsp(&self) -> u8 {
        self.fsp.map_or(0, |f| f.min(6) as u8)
    }
}

/// The `tinyint1` settings (`source.mysql.tinyint1_as_bool` and `tinyint1_detection`): `true`
/// maps a `tinyint(1)` column to `bool`.
pub type Tinyint1AsBool = bool;

/// A planned column: what the decoder produces and what the target stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPlan {
    pub value_type: V,
    pub logical: LogicalType,
    pub decode: Decode,
    /// `Some(type)` when the column is kept as its text because its decimal is wider than 38
    /// digits (`numeric_overflow_action: string`).
    pub read_as_text: Option<String>,
    /// An identifier column: an all-zero date there has no NULL to become (Q-M7-1).
    pub identifier: bool,
}

/// The members of an `enum(…)` or `set(…)` `COLUMN_TYPE`, quotes undoubled.
pub fn members(column_type: &str) -> Vec<String> {
    let Some(start) = column_type.find('(') else {
        return vec![];
    };
    let inner = &column_type[start + 1..column_type.rfind(')').unwrap_or(column_type.len())];
    let mut out = vec![];
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut member = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    member.push('\'');
                }
                '\'' => break,
                '\\' => member.extend(chars.next()),
                other => member.push(other),
            }
        }
        out.push(member);
    }
    out
}

/// The decoding of a character column in `charset` (Q-M7a-2): the UTF-8 family and latin1 read as
/// text, `binary` as bytes; any other charset can't be read.
fn character(c: &CatalogColumn) -> Result<(V, Decode), HaltReason> {
    let name = c.charset.as_deref().unwrap_or("binary");
    match Charset::of(name) {
        Some(Charset::Binary) => Ok((V::Bytes, Decode::Bytes { pad: None })),
        Some(charset) => Ok((V::Text, Decode::Text { charset })),
        None => Err(HaltReason::SchemaMismatch {
            column: c.name.clone(),
            detail: format!(
                "character set {name} isn't read by the source (UTF-8 family, latin1, binary); \
                 convert the column, or override it to binary"
            ),
        }),
    }
}

fn int(bits: u32, unsigned: bool) -> (V, Decode) {
    (int_type(bits, unsigned), Decode::Int { bits, unsigned })
}

/// Plans `c` without an override. `Ok(None)` keeps an over-wide decimal as text.
fn plan(
    c: &CatalogColumn,
    tinyint1_as_bool: Tinyint1AsBool,
    action: NumericOverflowAction,
) -> Result<(V, Decode, Option<String>), HaltReason> {
    let unsigned = c.unsigned();
    let fsp = c.fsp();
    let (t, d) = match c.data_type.as_str() {
        "tinyint" if tinyint1_as_bool && !unsigned && c.column_type.starts_with("tinyint(1)") => {
            (V::Bool, Decode::Bool)
        }
        "tinyint" => int(8, unsigned),
        "smallint" => int(16, unsigned),
        "mediumint" => int(24, unsigned),
        "int" | "integer" => int(32, unsigned),
        "bigint" => int(64, unsigned),
        "float" => (V::Float32, Decode::Float32),
        "double" | "real" => (V::Float64, Decode::Float64),
        "decimal" | "numeric" => {
            let (p, s) = (c.precision.unwrap_or(10), c.scale.unwrap_or(0));
            if p > u32::from(MAX_DECIMAL_PRECISION) {
                return match action {
                    NumericOverflowAction::String => Ok((
                        V::Text,
                        Decode::DecimalText,
                        Some(format!("decimal({p},{s})")),
                    )),
                    NumericOverflowAction::Error => Err(HaltReason::NumericOverflow {
                        column: c.name.clone(),
                    }),
                };
            }
            (
                V::Numeric {
                    precision: p as u8,
                    scale: s as i8,
                },
                Decode::Decimal,
            )
        }
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => character(c)?,
        "binary" => (
            V::Bytes,
            Decode::Bytes {
                pad: c.max_length.map(|n| n as usize),
            },
        ),
        "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" | "vector" => {
            (V::Bytes, Decode::Bytes { pad: None })
        }
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" | "geomcollection" => {
            (V::Bytes, Decode::Bytes { pad: None })
        }
        "date" => (V::Date, Decode::Date),
        "time" => (V::Time, Decode::Time { fsp }),
        "datetime" => (V::Timestamp { with_tz: false }, Decode::DateTime { fsp }),
        "timestamp" => (V::Timestamp { with_tz: true }, Decode::Timestamp { fsp }),
        "year" => (V::Int32, Decode::Year),
        "json" => (V::Json, Decode::Json),
        "enum" => (
            V::Text,
            Decode::Enum {
                members: members(&c.column_type),
            },
        ),
        "set" => (
            V::Array(Box::new(V::Text)),
            Decode::Set {
                members: members(&c.column_type),
            },
        ),
        "uuid" => (V::Uuid, Decode::Uuid),
        "inet4" => (V::Text, Decode::Inet4),
        "inet6" => (V::Text, Decode::Inet6),
        other => {
            return Err(HaltReason::SchemaMismatch {
                column: c.name.clone(),
                detail: format!("type {other} ({}) isn't supported", c.column_type),
            });
        }
    };
    Ok((t, d, None))
}

/// Plans column `c` with its `tables.column_type_overrides` entry, if any (config §9.3):
/// - `string` keeps the server's text of the value (an invalid date's raw text, Q-M7-1); a byte
///   column has no text and halts;
/// - `decimal(P,S)` on a decimal column decodes it as that decimal, whatever its declared
///   precision (the V-079 remedy);
/// - any other override is one Hop 2 can store the values as ([`hop2_compatible`]) or a lossless
///   [`conversion`]; otherwise the table halts (`SCHEMA_MISMATCH`).
pub fn plan_column(
    c: &CatalogColumn,
    tinyint1_as_bool: Tinyint1AsBool,
    action: NumericOverflowAction,
    over: Option<&LogicalType>,
    identifier: bool,
) -> Result<ColumnPlan, HaltReason> {
    let column = |value_type: V, logical: LogicalType, decode: Decode, read_as_text| ColumnPlan {
        value_type,
        logical,
        decode,
        read_as_text,
        identifier,
    };
    let mismatch = |detail: String| HaltReason::SchemaMismatch {
        column: c.name.clone(),
        detail,
    };
    match over {
        None => {
            let (t, d, text) = plan(c, tinyint1_as_bool, action)?;
            Ok(column(t.clone(), canonical_logical(&t), d, text))
        }
        Some(LogicalType::String) => {
            let (t, d, _) = plan(c, tinyint1_as_bool, NumericOverflowAction::String)?;
            if t == V::Bytes {
                return Err(mismatch(
                    "column_type_overrides string: a byte column has no text form".into(),
                ));
            }
            Ok(column(
                V::Text,
                LogicalType::String,
                Decode::String(Box::new(d)),
                None,
            ))
        }
        Some(LogicalType::Decimal { precision, scale })
            if matches!(c.data_type.as_str(), "decimal" | "numeric") =>
        {
            let t = V::Numeric {
                precision: *precision,
                scale: *scale,
            };
            Ok(column(
                t.clone(),
                canonical_logical(&t),
                Decode::Decimal,
                None,
            ))
        }
        Some(l) => {
            let (t, d, _) = plan(c, tinyint1_as_bool, NumericOverflowAction::String)?;
            if hop2_compatible(&t, l) {
                Ok(column(t, l.clone(), d, None))
            } else if let Some(to) = conversion(&t, l) {
                let d = Decode::Convert {
                    from: Box::new(d),
                    to: to.clone(),
                };
                Ok(column(to, l.clone(), d, None))
            } else {
                Err(mismatch(format!(
                    "column_type_overrides {l} doesn't fit values of {t:?}; use string, a \
                     compatible type or a lossless conversion"
                )))
            }
        }
    }
}

```

In `crates/data-avalanche-source-mysql/src/value.rs`, add above the test module:

```rust
//! Values → `ColumnValue` (Hop 1, `Type_Mapping.md` MySQL and MariaDB tables). A value comes from
//! the binlog (`mysql_common`'s row decoding) or from a snapshot chunk (a prepared statement, the
//! binary protocol). The two differ per type (S-MY type probe, 2026-10-14): `mysql_common` drops
//! a signed `MEDIUMINT`'s sign and reads `YEAR 0000` as `1900`; the binlog carries a `TIMESTAMP` as
//! epoch text, an `ENUM` as its index, a `SET` as its bitmap, latin1 as its own bytes, and strips
//! the trailing zero bytes of `BINARY(n)` and MariaDB's `UUID`/`INET4`/`INET6`. Both inputs become
//! the same [`Raw`] value first, so a row reads the same from either.

use std::net::Ipv4Addr;

use chrono::{DateTime, NaiveDate};
use data_avalanche_core::{ColumnValue, ColumnValueType, Decimal, DecimalError};
use mysql_async::Value;
use mysql_async::binlog::value::BinlogValue;

/// The character sets the source reads (Q-M7a-2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Charset {
    /// `utf8mb4`, `utf8mb3` (`utf8`) and `ascii`.
    Utf8,
    /// MySQL's `latin1`, which is Windows-1252.
    Latin1,
    Binary,
}

impl Charset {
    pub fn of(name: &str) -> Option<Self> {
        match name {
            "utf8mb4" | "utf8mb3" | "utf8" | "ascii" => Some(Self::Utf8),
            "latin1" => Some(Self::Latin1),
            "binary" => Some(Self::Binary),
            _ => None,
        }
    }
}

/// How one column's values become `ColumnValue`s. Planned once per column ([`crate::types`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decode {
    /// `TINYINT(1)`: nonzero is true.
    Bool,
    /// An integer of `bits` bits, read with the catalog's signedness.
    Int {
        bits: u32,
        unsigned: bool,
    },
    Float32,
    Float64,
    Decimal,
    /// A decimal wider than 38 digits kept as its text (`numeric_overflow_action: string`).
    DecimalText,
    Text {
        charset: Charset,
    },
    /// Bytes; `BINARY(n)` pads to `n` the trailing zero bytes the binlog strips.
    Bytes {
        pad: Option<usize>,
    },
    Date,
    Time {
        fsp: u8,
    },
    DateTime {
        fsp: u8,
    },
    Timestamp {
        fsp: u8,
    },
    Year,
    Json,
    Enum {
        members: Vec<String>,
    },
    Set {
        members: Vec<String>,
    },
    /// MariaDB `UUID`.
    Uuid,
    /// MariaDB `INET4` / `INET6`, as MariaDB prints them.
    Inet4,
    Inet6,
    /// The server's text of the value (`column_type_overrides: string`).
    String(Box<Decode>),
    /// Decoded as `from`, then converted for a `column_type_overrides` entry.
    Convert {
        from: Box<Decode>,
        to: ColumnValueType,
    },
}

/// The value type an integer of `bits` bits widens to (`Type_Mapping.md` Integer Types).
pub fn int_type(bits: u32, unsigned: bool) -> ColumnValueType {
    use ColumnValueType as V;
    match (bits, unsigned) {
        (8, _) | (16, false) => V::Int16,
        (16, true) | (24, _) | (32, false) => V::Int32,
        (32, true) | (64, false) => V::Int64,
        _ => V::UInt64,
    }
}

/// Where a value comes from.
#[derive(Debug, Clone, Copy)]
pub enum Input<'a> {
    Binlog(&'a BinlogValue<'a>),
    /// A snapshot chunk's prepared statement.
    Row(&'a Value),
}

/// Why a value can't be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueError {
    /// The value doesn't fit its decimal column (`NUMERIC_OVERFLOW`).
    #[error("the value doesn't fit its decimal column")]
    Overflow,
    /// A zero-in-part or impossible date, or an all-zero date in an identifier column
    /// (`INVALID_DATE_VALUE`, Q-M7-1). `raw` is the server's text of it.
    #[error("invalid date {raw}")]
    InvalidDate { raw: String },
    /// The value has no representation in the column's type (`SCHEMA_MISMATCH`).
    #[error("{0}")]
    Unrepresentable(String),
}

fn bad(what: impl std::fmt::Display) -> ValueError {
    ValueError::Unrepresentable(what.to_string())
}

/// A value read from either input, before it becomes a `ColumnValue` or text.
#[derive(Debug, Clone, PartialEq)]
enum Raw {
    Null,
    Int(i128),
    F32(f32),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// `DATE`, `DATETIME` and `TIMESTAMP` (in UTC) as their parts, zero parts kept.
    Clock {
        y: u16,
        mo: u8,
        d: u8,
        h: u8,
        mi: u8,
        s: u8,
        us: u32,
    },
    Time {
        neg: bool,
        hours: u32,
        mi: u8,
        s: u8,
        us: u32,
    },
    Json(serde_json::Value),
    Labels(Vec<String>),
}

/// MySQL `latin1` (Windows-1252, its five unassigned bytes kept as the C1 controls) → UTF-8.
fn latin1(bytes: &[u8]) -> String {
    const HIGH: [u16; 32] = [
        0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160,
        0x2039, 0x0152, 0x008D, 0x017D, 0x008F, 0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022,
        0x2013, 0x2014, 0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
    ];
    bytes
        .iter()
        .map(|&b| match b {
            0x80..=0x9F => char::from_u32(u32::from(HIGH[usize::from(b - 0x80)])).expect("BMP"),
            _ => char::from(b),
        })
        .collect()
}

fn utf8(bytes: &[u8]) -> Result<String, ValueError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| bad("text that isn't valid UTF-8"))
}

/// An `INET6` as MariaDB prints it (`ipv6_to_str`, which MariaDB inherited from MySQL's
/// `INET6_NTOA`; measured on 11.8). Not RFC 5952: the first longest run of zero groups is
/// compressed even when it is a single group (`1::2:0:3:0:4:0`), and the last 32 bits are dotted
/// after a leading run of exactly six zero groups (`::192.168.0.1`, but `::0.0.0.2` is `::2`) or
/// of five and `ffff` (`::ffff:192.168.0.1`).
fn inet6(b: [u8; 16]) -> String {
    let words: Vec<u16> = b
        .chunks(2)
        .map(|w| u16::from_be_bytes([w[0], w[1]]))
        .collect();
    // The gap: the first longest run of zero words, as (start, length).
    type Run = Option<(usize, usize)>;
    let (mut gap, mut run): (Run, Run) = (None, None);
    for (i, w) in words.iter().enumerate() {
        run = match (*w, run) {
            (0, Some((start, len))) => Some((start, len + 1)),
            (0, None) => Some((i, 1)),
            (_, r) => {
                if let Some(r) = r
                    && gap.is_none_or(|g| r.1 > g.1)
                {
                    gap = Some(r);
                }
                None
            }
        };
    }
    if let Some(r) = run
        && gap.is_none_or(|g| r.1 > g.1)
    {
        gap = Some(r);
    }
    let mut out = String::new();
    let mut i = 0;
    while i < 8 {
        match gap {
            Some((start, len)) if start == i => {
                out.push_str(if i == 0 { "::" } else { ":" });
                i += len;
                continue;
            }
            Some((0, len)) if i == 6 && (len == 6 || (len == 5 && words[5] == 0xffff)) => {
                out.push_str(&Ipv4Addr::new(b[12], b[13], b[14], b[15]).to_string());
                return out;
            }
            _ => {}
        }
        out.push_str(&format!("{:x}", words[i]));
        if i != 7 {
            out.push(':');
        }
        i += 1;
    }
    out
}

fn padded(mut bytes: Vec<u8>, n: usize) -> Vec<u8> {
    if bytes.len() < n {
        bytes.resize(n, 0);
    }
    bytes
}

/// The integer of `bits` bits in `v`, read with `unsigned`: `mysql_common` reads a signed
/// `MEDIUMINT` as unsigned, and without `binlog_row_metadata = FULL` it doesn't know a column's
/// signedness at all.
fn integer(v: &Value, bits: u32, unsigned: bool) -> Result<i128, ValueError> {
    let n = match v {
        Value::Int(i) => i128::from(*i),
        Value::UInt(u) => i128::from(*u),
        other => return Err(bad(format!("an integer arrived as {other:?}"))),
    };
    // Reinterpret only the other reading of the same `bits` bits.
    let m = 1i128 << bits;
    let n = match unsigned {
        true if (-m / 2..0).contains(&n) => n + m,
        false if (m / 2..m).contains(&n) => n - m,
        _ => n,
    };
    let (lo, hi) = match unsigned {
        true => (0, m - 1),
        false => (-m / 2, m / 2 - 1),
    };
    match (lo..=hi).contains(&n) {
        true => Ok(n),
        false => Err(bad(format!("{n} is out of range for a {bits}-bit integer"))),
    }
}

fn clock(v: &Value) -> Option<Raw> {
    match *v {
        Value::Date(y, mo, d, h, mi, s, us) => Some(Raw::Clock {
            y,
            mo,
            d,
            h,
            mi,
            s,
            us,
        }),
        _ => None,
    }
}

/// The binlog's `TIMESTAMP`: seconds since the epoch, a fraction, in UTC; `0` is the zero value.
fn epoch(text: &str) -> Result<Raw, ValueError> {
    let invalid = || bad(format!("a timestamp arrived as {text:?}"));
    let (secs, frac) = text.split_once('.').unwrap_or((text, ""));
    let secs: i64 = secs.parse().map_err(|_| invalid())?;
    if secs == 0 && frac.trim_matches('0').is_empty() {
        return Ok(Raw::Clock {
            y: 0,
            mo: 0,
            d: 0,
            h: 0,
            mi: 0,
            s: 0,
            us: 0,
        });
    }
    if frac.len() > 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    let us: u32 = format!("{frac:0<6}").parse().map_err(|_| invalid())?;
    let t = DateTime::from_timestamp(secs, us * 1000).ok_or_else(invalid)?;
    use chrono::{Datelike, Timelike};
    Ok(Raw::Clock {
        y: t.year() as u16,
        mo: t.month() as u8,
        d: t.day() as u8,
        h: t.hour() as u8,
        mi: t.minute() as u8,
        s: t.second() as u8,
        us,
    })
}

/// The value of `input` as `d` reads it, before it becomes a `ColumnValue` or text.
fn raw(d: &Decode, input: Input<'_>) -> Result<Raw, ValueError> {
    let v = match input {
        Input::Binlog(BinlogValue::Value(v)) | Input::Row(v) => v,
        Input::Binlog(BinlogValue::Jsonb(j)) => {
            let json = serde_json::Value::try_from(j.clone())
                .map_err(|e| bad(format!("binary JSON the decoder can't read: {e}")))?;
            return Ok(Raw::Json(json));
        }
        Input::Binlog(BinlogValue::JsonDiff(_)) => {
            return Err(bad(
                "a partial JSON update (binlog_row_value_options) needs its before image",
            ));
        }
    };
    if matches!(v, Value::NULL) {
        return Ok(Raw::Null);
    }
    let binlog = matches!(input, Input::Binlog(_));
    let bytes = || match v {
        Value::Bytes(b) => Ok(b.clone()),
        other => Err(bad(format!("{d:?} arrived as {other:?}"))),
    };
    let unexpected = || bad(format!("{d:?} arrived as {v:?}"));
    Ok(match d {
        Decode::Bool => Raw::Int(integer(v, 8, false)?),
        Decode::Int { bits, unsigned } => Raw::Int(integer(v, *bits, *unsigned)?),
        Decode::Float32 => match v {
            Value::Float(f) => Raw::F32(*f),
            _ => return Err(unexpected()),
        },
        Decode::Float64 => match v {
            Value::Double(f) => Raw::F64(*f),
            Value::Float(f) => Raw::F64(f64::from(*f)),
            _ => return Err(unexpected()),
        },
        Decode::Decimal | Decode::DecimalText => Raw::Text(utf8(&bytes()?)?),
        Decode::Text { charset } => {
            let b = bytes()?;
            match (binlog, charset) {
                (true, Charset::Latin1) => Raw::Text(latin1(&b)),
                (_, Charset::Binary) => Raw::Bytes(b),
                _ => Raw::Text(utf8(&b)?),
            }
        }
        Decode::Bytes { pad } => Raw::Bytes(padded(bytes()?, pad.unwrap_or(0))),
        Decode::Date | Decode::DateTime { .. } => clock(v).ok_or_else(unexpected)?,
        Decode::Timestamp { .. } => match v {
            Value::Bytes(b) if binlog => epoch(&utf8(b)?)?,
            _ => clock(v).ok_or_else(unexpected)?,
        },
        Decode::Time { .. } => match *v {
            Value::Time(neg, days, h, mi, s, us) => Raw::Time {
                neg,
                hours: days * 24 + u32::from(h),
                mi,
                s,
                us,
            },
            _ => return Err(unexpected()),
        },
        // `mysql_common` reads the binlog's YEAR as text, 1900 + the stored byte: 0 is 1900, a
        // year YEAR can't hold.
        Decode::Year => match v {
            Value::Bytes(b) => match utf8(b)?.parse::<i128>() {
                Ok(1900) => Raw::Int(0),
                Ok(y) => Raw::Int(y),
                Err(_) => return Err(unexpected()),
            },
            Value::Int(_) | Value::UInt(_) => Raw::Int(integer(v, 16, true)?),
            _ => return Err(unexpected()),
        },
        Decode::Json => Raw::Json(
            serde_json::from_slice(&bytes()?)
                .map_err(|e| bad(format!("JSON the decoder can't read: {e}")))?,
        ),
        Decode::Enum { members } => match v {
            Value::Int(_) | Value::UInt(_) if binlog => {
                let i = integer(v, 16, true)? as usize;
                match i {
                    0 => Raw::Text(String::new()),
                    i => Raw::Text(members.get(i - 1).cloned().ok_or_else(|| {
                        bad(format!("ENUM index {i} of {} members", members.len()))
                    })?),
                }
            }
            _ => Raw::Text(utf8(&bytes()?)?),
        },
        Decode::Set { members } => {
            let b = bytes()?;
            if binlog {
                let labels = (0..members.len())
                    .filter(|i| b.get(i / 8).is_some_and(|byte| byte & (1 << (i % 8)) != 0))
                    .map(|i| members[i].clone())
                    .collect();
                Raw::Labels(labels)
            } else {
                let text = utf8(&b)?;
                Raw::Labels(match text.is_empty() {
                    true => vec![],
                    false => text.split(',').map(str::to_owned).collect(),
                })
            }
        }
        Decode::Uuid => {
            let b = bytes()?;
            match binlog {
                true => Raw::Bytes(padded(b, 16)),
                false => {
                    let text = utf8(&b)?;
                    let u = uuid::Uuid::parse_str(&text)
                        .map_err(|_| bad(format!("not a UUID: {text}")))?;
                    Raw::Bytes(u.as_bytes().to_vec())
                }
            }
        }
        Decode::Inet4 | Decode::Inet6 => {
            let b = bytes()?;
            match (binlog, d) {
                (true, Decode::Inet4) => {
                    let b: [u8; 4] = padded(b, 4).try_into().map_err(|_| unexpected())?;
                    Raw::Text(Ipv4Addr::from(b).to_string())
                }
                (true, _) => {
                    let b: [u8; 16] = padded(b, 16).try_into().map_err(|_| unexpected())?;
                    Raw::Text(inet6(b))
                }
                (false, _) => Raw::Text(utf8(&b)?),
            }
        }
        Decode::String(inner) | Decode::Convert { from: inner, .. } => raw(inner, input)?,
    })
}

/// `us` with `fsp` fractional digits, MySQL's way: none for `fsp` 0.
fn fraction(us: u32, fsp: u8) -> String {
    match fsp {
        0 => String::new(),
        n => format!(".{:06}", us)[..usize::from(n) + 1].to_owned(),
    }
}

/// The server's text of `raw` read by `d` (`column_type_overrides: string`).
fn text(d: &Decode, raw: &Raw) -> String {
    match (d, raw) {
        (_, Raw::Null) => String::new(),
        (Decode::Year, Raw::Int(y)) => format!("{y:04}"),
        (_, Raw::Int(n)) => n.to_string(),
        (_, Raw::F32(f)) => f.to_string(),
        (_, Raw::F64(f)) => f.to_string(),
        (_, Raw::Text(t)) => t.clone(),
        (Decode::Uuid, Raw::Bytes(b)) => uuid::Uuid::from_slice(b)
            .map(|u| u.hyphenated().to_string())
            .unwrap_or_default(),
        (_, Raw::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        (
            _,
            &Raw::Clock {
                y,
                mo,
                d: day,
                h,
                mi,
                s,
                us,
            },
        ) => match d {
            Decode::Date => format!("{y:04}-{mo:02}-{day:02}"),
            Decode::DateTime { fsp } | Decode::Timestamp { fsp } => format!(
                "{y:04}-{mo:02}-{day:02} {h:02}:{mi:02}:{s:02}{}",
                fraction(us, *fsp)
            ),
            _ => String::new(),
        },
        (
            _,
            &Raw::Time {
                neg,
                hours,
                mi,
                s,
                us,
            },
        ) => {
            let fsp = match d {
                Decode::Time { fsp } => *fsp,
                _ => 6,
            };
            let sign = if neg { "-" } else { "" };
            format!("{sign}{hours:02}:{mi:02}:{s:02}{}", fraction(us, fsp))
        }
        (_, Raw::Json(j)) => j.to_string(),
        (_, Raw::Labels(l)) => l.join(","),
    }
}

/// The days and microseconds of a `Clock`: `None` for the all-zero "no date"; an error for a
/// zero-in-part or impossible date (Q-M7-1).
fn instant(d: &Decode, raw: &Raw) -> Result<Option<(i32, i64)>, ValueError> {
    let &Raw::Clock {
        y,
        mo,
        d: day,
        h,
        mi,
        s,
        us,
    } = raw
    else {
        return Err(bad(format!("{d:?} read {raw:?}")));
    };
    if y == 0 && mo == 0 && day == 0 && h == 0 && mi == 0 && s == 0 && us == 0 {
        return Ok(None);
    }
    let invalid = || ValueError::InvalidDate { raw: text(d, raw) };
    let date =
        NaiveDate::from_ymd_opt(i32::from(y), u32::from(mo), u32::from(day)).ok_or_else(invalid)?;
    let days = date
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch"))
        .num_days() as i32;
    let micros = i64::from(days) * 86_400_000_000
        + (i64::from(h) * 3600 + i64::from(mi) * 60 + i64::from(s)) * 1_000_000
        + i64::from(us);
    Ok(Some((days, micros)))
}

/// The `ColumnValue` of `raw` read by `d`.
fn value(d: &Decode, raw: Raw, identifier: bool) -> Result<ColumnValue, ValueError> {
    use ColumnValue as V;
    if raw == Raw::Null {
        return Ok(V::Null);
    }
    Ok(match d {
        Decode::Date | Decode::DateTime { .. } | Decode::Timestamp { .. } => {
            match (instant(d, &raw)?, identifier) {
                (None, true) => return Err(ValueError::InvalidDate { raw: text(d, &raw) }),
                (None, false) => V::Null,
                (Some((days, _)), _) if *d == Decode::Date => V::Date(days),
                (Some((_, micros)), _) => V::Timestamp {
                    micros,
                    with_tz: matches!(d, Decode::Timestamp { .. }),
                },
            }
        }
        Decode::Bool => V::Bool(raw != Raw::Int(0)),
        Decode::Int { bits, unsigned } => {
            let Raw::Int(n) = raw else {
                return Err(bad(format!("an integer read {raw:?}")));
            };
            match int_type(*bits, *unsigned) {
                ColumnValueType::Int16 => V::Int16(n as i16),
                ColumnValueType::Int32 => V::Int32(n as i32),
                ColumnValueType::Int64 => V::Int64(n as i64),
                _ => V::UInt64(n as u64),
            }
        }
        Decode::Year => match raw {
            Raw::Int(y) => V::Int32(y as i32),
            other => return Err(bad(format!("a year read {other:?}"))),
        },
        Decode::Float32 => match raw {
            Raw::F32(f) => V::Float32(f),
            other => return Err(bad(format!("a float read {other:?}"))),
        },
        Decode::Float64 => match raw {
            Raw::F64(f) => V::Float64(f),
            other => return Err(bad(format!("a double read {other:?}"))),
        },
        Decode::Decimal => match raw {
            Raw::Text(t) => V::Numeric(Decimal::parse(&t).map_err(|e| match e {
                DecimalError::TooManyDigits(_) => ValueError::Overflow,
                _ => bad(format!("not a decimal: {t}")),
            })?),
            other => return Err(bad(format!("a decimal read {other:?}"))),
        },
        Decode::Time { .. } => match raw {
            Raw::Time {
                neg,
                hours,
                mi,
                s,
                us,
            } => {
                let micros = (i64::from(hours) * 3600 + i64::from(mi) * 60 + i64::from(s))
                    * 1_000_000
                    + i64::from(us);
                V::Time(if neg { -micros } else { micros })
            }
            other => return Err(bad(format!("a time read {other:?}"))),
        },
        Decode::Json => match raw {
            Raw::Json(j) => V::Json(j),
            other => return Err(bad(format!("JSON read {other:?}"))),
        },
        Decode::Set { .. } => match raw {
            Raw::Labels(l) => V::Array {
                element_type: ColumnValueType::Text,
                values: l.into_iter().map(V::Text).collect(),
            },
            other => return Err(bad(format!("a SET read {other:?}"))),
        },
        Decode::Uuid => match raw {
            Raw::Bytes(b) => {
                V::Uuid(uuid::Uuid::from_slice(&b).map_err(|_| bad("a UUID of the wrong length"))?)
            }
            other => return Err(bad(format!("a UUID read {other:?}"))),
        },
        Decode::String(inner) => V::Text(text(inner, &raw)),
        Decode::Convert { from, to } => value(from, raw, identifier)?.convert(to),
        _ => match raw {
            Raw::Text(t) => V::Text(t),
            Raw::Bytes(b) => V::Bytes(b),
            other => return Err(bad(format!("{d:?} read {other:?}"))),
        },
    })
}

/// Decodes one value of a column planned as `d`; `identifier` marks a column of the table's
/// identifier.
pub fn decode(d: &Decode, input: Input<'_>, identifier: bool) -> Result<ColumnValue, ValueError> {
    let r = raw(d, input)?;
    value(d, r, identifier)
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 41 tests.

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql values:: || break; done`

Expected: PASS, 3 tests on each target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche-source-mysql/Cargo.toml \
  crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/types.rs \
  crates/data-avalanche-source-mysql/src/value.rs \
  crates/data-avalanche-source-mysql/tests/mysql/main.rs \
  crates/data-avalanche-source-mysql/tests/mysql/values.rs
git commit -m "feat(source-mysql): Hop 1 values — type plans from the catalog, one decoder for binlog and chunk values, overrides"
```

---

### Task 8: Partial JSON updates

**Files:**
- Create: `crates/data-avalanche-source-mysql/src/json.rs`
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`
- Modify: `crates/data-avalanche-source-mysql/src/value.rs`

**Interfaces:**
- Consumes:
  - `Decode`, `Input`, `decode` (Task 7); `mysql_common`'s `JsonDiff`.
- Produces:
  - `data-avalanche-source-mysql/src/json.rs`:
    - `pub fn apply(`
    - `pub fn apply_all(mut doc: Value, diffs: &[JsonDiff<'_>]) -> Result<Value, String>`
  - `data-avalanche-source-mysql/src/lib.rs`:
    - `pub mod json;`
  - `data-avalanche-source-mysql/src/value.rs`:
    - `pub fn decode_after(`

Q-M7a-3. With `binlog_row_value_options = PARTIAL_JSON`, MySQL logs an update of a JSON column
as a diff against the before image (a type-39 `PARTIAL_UPDATE_ROWS` event), even under a `FULL`
row image (measured). The diff is applied to the before image (`REPLACE`, `INSERT`, `REMOVE`
at a path), so the after image carries the whole new document.

Measured on MySQL 8.4: paths arrive normalized (`$`, `.key`, `."quoted key"`, `[n]`); `INSERT
$.arr[10]` on a three-element array appends; an auto-wrap is logged as a `REPLACE` of the
wrapped value; a multi-path update is several diffs; a no-op `REMOVE` is an empty list;
`JSON_SET('$', …)` and `JSON_ARRAY_APPEND` are logged as whole documents.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -8,6 +8,7 @@ pub mod binlog;
 mod connect;
 mod connector;
 mod error;
+pub mod json;
 pub mod position;
 pub mod server;
 pub mod types;
@@ -19,4 +20,4 @@ pub use connector::MySqlConnector;
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
 pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
 pub use types::{CatalogColumn, ColumnPlan, members, plan_column};
-pub use value::{Charset, Decode, Input, ValueError, decode, int_type};
+pub use value::{Charset, Decode, Input, ValueError, decode, decode_after, int_type};
```

Create `crates/data-avalanche-source-mysql/src/json.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use JsonDiffOperation::*;
    use serde_json::json;

    fn start() -> Value {
        json!({"a": 1, "b": "long", "arr": [10, 20, 30], "o": {"p": {"q": 0}},
               "k e y": 1, "qu\"ote": 2, "ключ": 3, "s": 5})
    }

    /// Each measured statement's diffs give the server's document.
    #[test]
    fn measured_diffs_give_the_servers_document() {
        type Diff<'a> = (JsonDiffOperation, &'a str, Option<Value>);
        let cases: Vec<(Vec<Diff>, Value)> = vec![
            (vec![(REPLACE, "$.a", Some(json!(2)))], json!({"a": 2})),
            (
                vec![(INSERT, "$.new", Some(json!("x")))],
                json!({"new": "x"}),
            ),
            (vec![(REMOVE, "$.b", None)], json!({"b": null})),
            (
                vec![(REPLACE, "$.arr[1]", Some(json!(9)))],
                json!({"arr": [10, 9, 30]}),
            ),
            (
                vec![(INSERT, "$.arr[10]", Some(json!(7)))],
                json!({"arr": [10, 20, 30, 7]}),
            ),
            (vec![(REMOVE, "$.arr[0]", None)], json!({"arr": [20, 30]})),
            (
                vec![(REPLACE, r#"$."k e y""#, Some(json!(11)))],
                json!({"k e y": 11}),
            ),
            (
                vec![(REPLACE, r#"$."qu\"ote""#, Some(json!(12)))],
                json!({"qu\"ote": 12}),
            ),
            (
                vec![(REPLACE, r#"$."ключ""#, Some(json!(13)))],
                json!({"ключ": 13}),
            ),
            (
                vec![
                    (REPLACE, "$.o.p.q", Some(json!(1))),
                    (INSERT, "$.o.p.r", Some(json!(2))),
                ],
                json!({"o": {"p": {"q": 1, "r": 2}}}),
            ),
            (
                vec![(REPLACE, "$.s", Some(json!([5, 6])))],
                json!({"s": [5, 6]}),
            ),
            (
                vec![(REPLACE, "$.arr[0]", Some(json!({"x": 1})))],
                json!({"arr": [{"x": 1}, 20, 30]}),
            ),
            (vec![], json!({})),
        ];
        for (diffs, changes) in cases {
            let mut want = start();
            for (k, v) in changes.as_object().unwrap() {
                match v {
                    Value::Null => want.as_object_mut().unwrap().remove(k),
                    v => want.as_object_mut().unwrap().insert(k.clone(), v.clone()),
                };
            }
            let mut doc = start();
            for (op, path, value) in diffs.clone() {
                apply(&mut doc, op, path, value).unwrap();
            }
            assert_eq!(doc, want, "{diffs:?}");
        }
    }

    #[test]
    fn the_whole_document_and_array_inserts() {
        let mut doc = start();
        apply(&mut doc, REPLACE, "$", Some(json!({"whole": 1}))).unwrap();
        assert_eq!(doc, json!({"whole": 1}));
        let mut a = json!([1, 2]);
        apply(&mut a, INSERT, "$[0]", Some(json!(0))).unwrap();
        assert_eq!(a, json!([0, 1, 2]));
    }

    #[test]
    fn a_diff_the_before_image_cant_take_is_an_error() {
        for (op, path) in [
            (REPLACE, "$.nope.x"),
            (REPLACE, "$.arr[9]"),
            (REMOVE, "$"),
            (REPLACE, "$.a.b"),
            (REPLACE, "a"),
            (REPLACE, "$.\"open"),
            (REPLACE, "$[x]"),
            (REPLACE, "$.arr[*]"),
        ] {
            let mut doc = start();
            assert!(
                apply(&mut doc, op, path, Some(json!(1))).is_err(),
                "{op:?} {path}"
            );
        }
        assert!(
            apply(&mut start(), INSERT, "$.n", None).is_err(),
            "no value"
        );
    }

    #[test]
    fn paths_parse_their_steps() {
        assert_eq!(
            parse(r#"$.a[2]."b.c"[0].d"#).unwrap(),
            vec![
                Leg::Key("a".into()),
                Leg::Index(2),
                Leg::Key("b.c".into()),
                Leg::Index(0),
                Leg::Key("d".into())
            ]
        );
        assert_eq!(parse("$").unwrap(), vec![]);
    }
}
```

Change `crates/data-avalanche-source-mysql/src/value.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/value.rs b/crates/data-avalanche-source-mysql/src/value.rs
--- a/crates/data-avalanche-source-mysql/src/value.rs
+++ b/crates/data-avalanche-source-mysql/src/value.rs
@@ -1009,6 +1009,25 @@ mod tests {
         both(&s, b(&[0]), b(b""), labels(&[]));
         let partial = BinlogValue::JsonDiff(vec![]);
         assert!(decode(&Decode::Json, Input::Binlog(&partial), false).is_err());
+        // With its before image, an empty diff list (a statement that changed nothing) is the
+        // before image; a `string` override renders the document.
+        let before = b(br#"{"a": 1}"#);
+        for (d, want) in [
+            (Decode::Json, ColumnValue::Json(serde_json::json!({"a": 1}))),
+            (
+                Decode::String(Box::new(Decode::Json)),
+                ColumnValue::Text(r#"{"a":1}"#.into()),
+            ),
+        ] {
+            let got = decode_after(
+                &d,
+                Some(Input::Row(&before)),
+                Input::Binlog(&partial),
+                false,
+            );
+            assert_eq!(got.unwrap(), want);
+        }
+        assert!(decode_after(&Decode::Json, None, Input::Binlog(&partial), false).is_err());
     }
 
     #[test]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `JsonDiffOperation`
error[E0432]: unresolved import `value::decode_after`
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-source-mysql/src/json.rs`, add above the test module:

```rust
//! MySQL partial JSON updates (`binlog_row_value_options = PARTIAL_JSON`, Q-M7a-3): an UPDATE by
//! `JSON_SET`, `JSON_REPLACE` or `JSON_REMOVE` logs the column's after image as a list of diffs
//! against its before image, which `binlog_row_image = FULL` carries whole. The source applies them
//! and emits the full new document. Measured on MySQL 8.4 (2026-10-14): paths are normalized
//! (`$`, `.key`, `."quoted key"` as a JSON string, `[n]`); an INSERT at an array index past the end
//! appends; auto-wrapping a scalar logs a REPLACE of the wrapped array; a statement that changes
//! nothing logs no diff. A whole-document set or `JSON_ARRAY_APPEND` logs the full value instead.

use mysql_async::binlog::jsondiff::{JsonDiff, JsonDiffOperation};
use serde_json::Value;

/// One step of a path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Leg {
    Key(String),
    Index(usize),
}

/// A normalized MySQL JSON path, `$` first.
fn parse(path: &str) -> Result<Vec<Leg>, String> {
    let bad = |why: &str| format!("JSON path {path:?}: {why}");
    let rest = path.strip_prefix('$').ok_or_else(|| bad("no leading $"))?;
    let mut legs = vec![];
    let mut s = rest;
    while !s.is_empty() {
        if let Some(r) = s.strip_prefix('.') {
            if r.starts_with('"') {
                // A JSON string: find its closing quote past escapes.
                let mut end = None;
                let mut escaped = false;
                for (i, c) in r.char_indices().skip(1) {
                    match (c, escaped) {
                        ('\\', false) => escaped = true,
                        ('"', false) => {
                            end = Some(i);
                            break;
                        }
                        _ => escaped = false,
                    }
                }
                let end = end.ok_or_else(|| bad("an unterminated quoted key"))?;
                let key: String =
                    serde_json::from_str(&r[..=end]).map_err(|_| bad("a quoted key"))?;
                legs.push(Leg::Key(key));
                s = &r[end + 1..];
            } else {
                let end = r.find(['.', '[']).unwrap_or(r.len());
                if end == 0 {
                    return Err(bad("an empty key"));
                }
                legs.push(Leg::Key(r[..end].to_owned()));
                s = &r[end..];
            }
        } else if let Some(r) = s.strip_prefix('[') {
            let end = r.find(']').ok_or_else(|| bad("an unterminated index"))?;
            let i = r[..end]
                .parse()
                .map_err(|_| bad("an index that isn't a number"))?;
            legs.push(Leg::Index(i));
            s = &r[end + 1..];
        } else {
            return Err(bad("a step that is neither .key nor [n]"));
        }
    }
    Ok(legs)
}

/// Applies one diff to `doc`.
pub fn apply(
    doc: &mut Value,
    op: JsonDiffOperation,
    path: &str,
    value: Option<Value>,
) -> Result<(), String> {
    let legs = parse(path)?;
    let missing = || format!("JSON path {path:?} isn't in the before image");
    let Some((last, parents)) = legs.split_last() else {
        // `$`: the whole document.
        return match (op, value) {
            (JsonDiffOperation::REMOVE, _) => Err("a REMOVE of the whole document".into()),
            (_, Some(v)) => {
                *doc = v;
                Ok(())
            }
            (_, None) => Err(format!("a {op:?} of {path:?} without a value")),
        };
    };
    let mut at = doc;
    for leg in parents {
        at = match (leg, at) {
            (Leg::Key(k), Value::Object(m)) => m.get_mut(k).ok_or_else(missing)?,
            (Leg::Index(i), Value::Array(a)) => a.get_mut(*i).ok_or_else(missing)?,
            _ => return Err(missing()),
        };
    }
    let need = |v: Option<Value>| v.ok_or_else(|| format!("a {op:?} of {path:?} without a value"));
    match (op, last, at) {
        (JsonDiffOperation::REMOVE, Leg::Key(k), Value::Object(m)) => {
            m.remove(k);
        }
        (JsonDiffOperation::REMOVE, Leg::Index(i), Value::Array(a)) if *i < a.len() => {
            a.remove(*i);
        }
        (_, Leg::Key(k), Value::Object(m)) if op != JsonDiffOperation::REMOVE => {
            m.insert(k.clone(), need(value)?);
        }
        (JsonDiffOperation::REPLACE, Leg::Index(i), Value::Array(a)) if *i < a.len() => {
            a[*i] = need(value)?;
        }
        (JsonDiffOperation::INSERT, Leg::Index(i), Value::Array(a)) => {
            let v = need(value)?;
            match *i < a.len() {
                true => a.insert(*i, v),
                false => a.push(v),
            }
        }
        _ => return Err(missing()),
    }
    Ok(())
}

/// Applies `diffs` in order to the before image `doc`.
pub fn apply_all(mut doc: Value, diffs: &[JsonDiff<'_>]) -> Result<Value, String> {
    for d in diffs {
        let value = d
            .value()
            .map(|v| Value::try_from(v.clone()))
            .transpose()
            .map_err(|e| format!("a diff value the decoder can't read: {e}"))?;
        apply(&mut doc, d.operation(), &d.path_str(), value)?;
    }
    Ok(doc)
}

```

Change `crates/data-avalanche-source-mysql/src/value.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/value.rs b/crates/data-avalanche-source-mysql/src/value.rs
--- a/crates/data-avalanche-source-mysql/src/value.rs
+++ b/crates/data-avalanche-source-mysql/src/value.rs
@@ -637,6 +637,27 @@ pub fn decode(d: &Decode, input: Input<'_>, identifier: bool) -> Result<ColumnVa
     value(d, r, identifier)
 }
 
+/// Decodes an after image's value, which a partial JSON update logs as diffs against the before
+/// image's (Q-M7a-3): the diffs are applied to `before`, and the full document is the value.
+pub fn decode_after(
+    d: &Decode,
+    before: Option<Input<'_>>,
+    after: Input<'_>,
+    identifier: bool,
+) -> Result<ColumnValue, ValueError> {
+    let Input::Binlog(BinlogValue::JsonDiff(diffs)) = after else {
+        return decode(d, after, identifier);
+    };
+    let before = before.ok_or_else(|| bad("a partial JSON update without a before image"))?;
+    let Raw::Json(doc) = raw(d, before)? else {
+        return Err(bad(
+            "a partial JSON update of a value that isn't a JSON document",
+        ));
+    };
+    let doc = crate::json::apply_all(doc, diffs).map_err(bad)?;
+    value(d, Raw::Json(doc), identifier)
+}
+
 #[cfg(test)]
 mod tests {
     use super::*;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 45 tests.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/src/json.rs \
  crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/value.rs
git commit -m "feat(source-mysql): partial JSON updates applied to the before image"
```

---

### Task 9: Channel A: table selection and the catalog over `information_schema`

**Files:**
- Modify: `crates/data-avalanche-config/src/external.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`
- Modify (test): `crates/data-avalanche-config/tests/external.rs`
- Create: `crates/data-avalanche-source-mysql/src/catalog.rs`
- Modify: `crates/data-avalanche-source-mysql/src/connector.rs`
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`
- Modify: `crates/data-avalanche-source-mysql/src/types.rs`
- Modify: `crates/data-avalanche-source-mysql/tests/mysql/main.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/resolve.rs`
- Modify: `crates/data-avalanche-source-pg/src/catalog.rs`

**Interfaces:**
- Consumes:
  - `plan_column`, `CatalogColumn` (Task 7); the catalog connection (Task 4).
  - `data_avalanche_core::{ResolvedTables, TableSchema, TableRef, HaltReason}`;
    `data_avalanche_config::Tables`.
- Produces:
  - `data-avalanche-config/src/external.rs`:
    - `pub fn glob_match(pattern: &str, name: &str) -> bool`
    - `pub fn check_partitioning(`
  - `data-avalanche-source-mysql/src/catalog.rs`:
    - `pub struct Listed`
    - `pub struct Selection`
    - `pub fn table_key(t: &TableRef) -> &str`
    - `pub fn select(include: &[String], exclude: &[String], listed: &[Listed]) -> Selection`
    - `pub struct Described`
    - `pub struct Notes`
    - `pub struct Planned`
    - `pub fn plan_table(`
    - `pub async fn list_tables(&self) -> Result<Vec<Listed>, SourceError>`
    - `pub async fn describe(`
    - `pub async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError>`
  - `data-avalanche-source-mysql/src/lib.rs`:
    - `pub mod catalog;`

The tracked table set at start (Data-Flow Phase 1, config §9.3 and §11.2; D-M7a.12).

- Include and exclude globs match bare table names in `source.db` (V-064). `glob_match` and the
  partition-spec check move from the PostgreSQL crate to `data-avalanche-config`, which both
  sources share; the PostgreSQL crate re-exports the glob and calls the shared check (its tests
  move with them).
- A system-versioned MariaDB table (its history isn't in the binlog's rows) halts when named,
  and is skipped with a warning when a glob matched it.
- `describe` reads `information_schema.COLUMNS` and the primary key from `KEY_COLUMN_USAGE`, in
  chunks of 500 tables. `plan_table` builds the `TableSchema` and each column's `ColumnPlan`
  (Task 7), the identifier from `tables.primary_key` or the primary key. A table without one is
  skipped with a warning (glob) or halts (named).
- MariaDB keeps the display width in `COLUMN_TYPE` (`smallint(5) unsigned`), MySQL drops it
  (measured): the plan reads `DATA_TYPE` and the `unsigned` suffix, never the width.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -92,7 +92,10 @@ pub use build::derived_server_id;
 pub use coerce::LogFilter;
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use diff::{ConfigDiff, FieldChange, PipelineChange, diff};
-pub use external::{ClusterAnchor, check_cluster_anchor, check_table_set, check_unbounded_numeric};
+pub use external::{
+    ClusterAnchor, check_cluster_anchor, check_partitioning, check_table_set,
+    check_unbounded_numeric, glob_match,
+};
 pub use leaf::Loc;
 pub use load::{Loaded, load_file, load_str, validate_str};
 pub use material::{FileRef, Origin, Pem};
```

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -5,6 +5,7 @@
 //! (`Snapshot_Resumability…` §1.1, DA-SNAP-10).
 
 pub mod binlog;
+pub mod catalog;
 mod connect;
 mod connector;
 mod error;
@@ -15,6 +16,7 @@ pub mod types;
 pub mod value;
 
 pub use binlog::{MariaGtidEvent, Offsets, decompress, rotate_name};
+pub use catalog::{Described, Listed, Notes, Planned, Selection, plan_table, select, table_key};
 pub use connect::SESSION_SETUP;
 pub use connector::MySqlConnector;
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
```

Change `crates/data-avalanche-source-mysql/tests/mysql/main.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/main.rs b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/main.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
@@ -6,5 +6,6 @@
 mod common;
 mod position;
 mod preflight;
+mod resolve;
 mod tls;
 mod values;
```

Create `crates/data-avalanche-source-mysql/src/catalog.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn listed(name: &str, versioned: bool) -> Listed {
        Listed {
            table: TableRef::new("shop", name),
            versioned,
        }
    }

    fn strings(s: &[&str]) -> Vec<String> {
        s.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn globs_select_bare_names() {
        let all = [
            listed("orders", false),
            listed("order_items", false),
            listed("audit", false),
            listed("history", true),
        ];
        let s = select(
            &strings(&["order*", "audit", "nope"]),
            &strings(&["*_items"]),
            &all,
        );
        assert_eq!(
            s.tables,
            vec![
                (listed("orders", false), false),
                (listed("audit", false), true)
            ]
        );
        assert_eq!(s.warnings, ["tables.include nope matches no table"]);
        // A system-versioned table: halted when named, skipped when a glob found it.
        let named = select(&strings(&["history"]), &[], &all);
        assert!(matches!(
            named.halted[0].1,
            HaltReason::NotReplicable { .. }
        ));
        let globbed = select(&strings(&["*"]), &[], &all);
        assert_eq!(globbed.skipped.len(), 1);
        assert_eq!(globbed.tables.len(), 3);
    }
}
```

Change `crates/data-avalanche-source-pg/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/catalog.rs b/crates/data-avalanche-source-pg/src/catalog.rs
--- a/crates/data-avalanche-source-pg/src/catalog.rs
+++ b/crates/data-avalanche-source-pg/src/catalog.rs
@@ -465,6 +465,7 @@ pub async fn load_types(
 mod tests {
     use super::*;
     use crate::test_support::pipeline_with;
+    use data_avalanche_config::PartitionTransform;
     use data_avalanche_core::ColumnValueType as V;
 
     fn rel(oid: u32, s: &str, t: &str, kind: u8) -> Relation {
@@ -477,13 +478,7 @@ mod tests {
     }
 
     #[test]
-    fn globs_stay_inside_a_name() {
-        assert!(glob_match("*", "orders"));
-        assert!(glob_match("ord*", "orders"));
-        assert!(glob_match("*ers", "orders"));
-        assert!(glob_match("o*r*s", "orders"));
-        assert!(!glob_match("Orders", "orders"), "case-sensitive");
-        assert!(!glob_match("ord", "orders"));
+    fn patterns_match_schema_and_table() {
         let t = TableRef::new("public", "orders");
         assert!(pattern_matches("public.*", &t));
         assert!(pattern_matches("*.orders", &t));
```

Change `crates/data-avalanche-config/tests/external.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/external.rs b/crates/data-avalanche-config/tests/external.rs
--- a/crates/data-avalanche-config/tests/external.rs
+++ b/crates/data-avalanche-config/tests/external.rs
@@ -6,10 +6,11 @@ mod support;
 use std::time::Duration;
 
 use data_avalanche_config::{
-    ClusterAnchor, LockBackend, RULES, ResolvedConfig, RuleInput, check_cluster_anchor,
-    check_table_set, check_unbounded_numeric, load_str,
+    ClusterAnchor, LockBackend, PartitionField, PartitionTransform, RULES, ResolvedConfig,
+    RuleInput, check_cluster_anchor, check_partitioning, check_table_set, check_unbounded_numeric,
+    glob_match, load_str,
 };
-use data_avalanche_core::TableRef;
+use data_avalanche_core::{ColumnSchema, ColumnValueType, TableRef, TableSchema};
 use support::{MINIMAL, env, fp_key};
 
 fn load(yaml: &str) -> ResolvedConfig {
@@ -150,6 +151,51 @@ fn unbounded_numeric_v079() {
     );
 }
 
+#[test]
+fn globs_stay_inside_a_name() {
+    assert!(glob_match("*", "orders"));
+    assert!(glob_match("ord*", "orders"));
+    assert!(glob_match("*ers", "orders"));
+    assert!(glob_match("o*r*s", "orders"));
+    assert!(!glob_match("Orders", "orders"), "case-sensitive");
+    assert!(!glob_match("ord", "orders"));
+}
+
+#[test]
+fn partition_columns_exist_and_temporal_transforms_name_dates() {
+    let schema = TableSchema::new(
+        TableRef::new("shop", "orders"),
+        vec![
+            ColumnSchema::new("id", ColumnValueType::Int64, "bigint").not_null(),
+            ColumnSchema::new("at", ColumnValueType::Date, "date"),
+        ],
+        vec!["id".into()],
+    )
+    .unwrap();
+    let check = |column: &str, transform| {
+        check_partitioning(
+            &schema,
+            &[PartitionField {
+                column: column.into(),
+                transform,
+            }],
+        )
+    };
+    assert!(check("id", PartitionTransform::Identity).is_ok());
+    assert!(check("at", PartitionTransform::Day).is_ok());
+    assert!(check("_ts", PartitionTransform::Hour).is_ok());
+    assert_eq!(
+        check("nope", PartitionTransform::Identity)
+            .unwrap_err()
+            .code(),
+        "SCHEMA_MISMATCH"
+    );
+    assert_eq!(
+        check("id", PartitionTransform::Month).unwrap_err().code(),
+        "SCHEMA_MISMATCH"
+    );
+}
+
 /// Every rule that needs input besides the file has a test in this file or in `tests/diff.rs`.
 #[test]
 fn every_external_rule_is_covered() {
```

Create `crates/data-avalanche-source-mysql/tests/mysql/resolve.rs`:

```rust
//! Channel A against the target server: the globs select the table set, identifiers decide what
//! halts and what is skipped, and each column plans per `Type_Mapping.md` (config §11.2).

use data_avalanche_core::{ColumnValueType as V, HaltReason, TableRef};
use data_avalanche_source_mysql::Flavour;
use data_avalanche_testkit::require_it;

use crate::common::{cleanup, connector, fresh, pipeline, target};

#[tokio::test]
async fn globs_and_identifiers_decide_the_table_set() {
    require_it!();
    let (n, _a) = fresh(
        "resolve_set",
        "CREATE TABLE orders (id INT PRIMARY KEY);
         CREATE TABLE order_items (id INT PRIMARY KEY);
         CREATE TABLE order_log (msg TEXT);
         CREATE TABLE audit (msg TEXT);
         CREATE TABLE keyed (k INT NOT NULL, v INT);
         CREATE VIEW order_view AS SELECT id FROM orders;",
    )
    .await;
    let p = pipeline(
        &n,
        &["order*", "audit", "keyed", "nothing"],
        "      exclude: [order_items]\n      primary_key: { keyed: [k], gone: [id] }",
        "",
    );
    let r = connector(&p).resolve_tables().await.expect("resolve");
    let t = |name: &str| TableRef::new(&n.db, name);
    let names: Vec<_> = r.tables.iter().map(|s| s.table().clone()).collect();
    assert_eq!(names, [t("keyed"), t("orders")], "a view isn't a table");
    assert_eq!(r.halted, [(t("audit"), HaltReason::NoIdentifier)]);
    assert_eq!(r.skipped.len(), 1);
    assert_eq!(r.skipped[0].0, t("order_log"));
    let keyed = r.tables.iter().find(|s| s.table() == &t("keyed")).unwrap();
    assert_eq!(keyed.identifier(), ["k"], "the tables.primary_key entry");
    assert!(
        r.warnings
            .iter()
            .any(|w| w.contains("nothing matches no table")),
        "{:?}",
        r.warnings
    );
    assert!(
        r.warnings
            .iter()
            .any(|w| w.starts_with("V-106") && w.contains("gone")),
        "{:?}",
        r.warnings
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn columns_plan_per_the_type_table() {
    require_it!();
    let (n, _a) = fresh(
        "resolve_types",
        "CREATE TABLE t (
            id BIGINT UNSIGNED NOT NULL PRIMARY KEY, b TINYINT(1), ti TINYINT, siu SMALLINT UNSIGNED,
            mi MEDIUMINT, iu INT UNSIGNED, f FLOAT, d DOUBLE, dec1 DECIMAL(10,2),
            c CHAR(3) CHARACTER SET latin1, vc VARCHAR(10), vb VARBINARY(4), bn BINARY(4),
            dt DATE, tm TIME(3), dtm DATETIME(6), ts TIMESTAMP NULL, yr YEAR,
            en ENUM('a','b'), st SET('x','y'), bt BIT(3), g POINT)",
    )
    .await;
    let r = connector(&pipeline(&n, &["t"], "", ""))
        .resolve_tables()
        .await
        .unwrap();
    let s = &r.tables[0];
    let types: Vec<(String, V, bool)> = s
        .columns()
        .iter()
        .map(|c| (c.name.clone(), c.value_type.clone(), c.nullable))
        .collect();
    let want = [
        ("id", V::UInt64, false),
        ("b", V::Bool, true),
        ("ti", V::Int16, true),
        ("siu", V::Int32, true),
        ("mi", V::Int32, true),
        ("iu", V::Int64, true),
        ("f", V::Float32, true),
        ("d", V::Float64, true),
        (
            "dec1",
            V::Numeric {
                precision: 10,
                scale: 2,
            },
            true,
        ),
        ("c", V::Text, true),
        ("vc", V::Text, true),
        ("vb", V::Bytes, true),
        ("bn", V::Bytes, true),
        ("dt", V::Date, true),
        ("tm", V::Time, true),
        ("dtm", V::Timestamp { with_tz: false }, true),
        ("ts", V::Timestamp { with_tz: true }, true),
        ("yr", V::Int32, true),
        ("en", V::Text, true),
        ("st", V::Array(Box::new(V::Text)), true),
        ("bt", V::Bytes, true),
        ("g", V::Bytes, true),
    ];
    let want: Vec<(String, V, bool)> = want.into_iter().map(|(a, b, c)| (a.into(), b, c)).collect();
    assert_eq!(types, want);
    // MySQL drops the display width, MariaDB keeps it: `smallint(5) unsigned`.
    let siu = &s.column("siu").unwrap().source_type;
    assert!(
        siu.starts_with("smallint") && siu.ends_with(" unsigned"),
        "{siu}"
    );
    assert_eq!(s.column("tm").unwrap().source_ordinal, Some(15));
    // tinyint1_as_bool off: TINYINT(1) is an integer.
    let r = connector(&pipeline(&n, &["t"], "", ", tinyint1_as_bool: false"))
        .resolve_tables()
        .await
        .unwrap();
    assert_eq!(r.tables[0].column("b").unwrap().value_type, V::Int16);
    cleanup(&n).await;
}

#[tokio::test]
async fn overrides_charsets_wide_decimals_and_reserved_names() {
    require_it!();
    let (n, _a) = fresh(
        "resolve_halts",
        "CREATE TABLE ok (id INT PRIMARY KEY, d DATE, i INT);
         CREATE TABLE sjis (id INT PRIMARY KEY, s VARCHAR(5) CHARACTER SET sjis);
         CREATE TABLE wide (id INT PRIMARY KEY, w DECIMAL(65,30));
         CREATE TABLE reserved (id INT PRIMARY KEY, _op INT);
         CREATE TABLE fkey (id INT PRIMARY KEY, f DOUBLE NOT NULL);",
    )
    .await;
    let tables = "      column_type_overrides: { ok.d: string, ok.i: int64 }\n      primary_key: { fkey: [f] }\n      numeric_overflow_action: error";
    let r = connector(&pipeline(
        &n,
        &["ok", "sjis", "wide", "reserved", "fkey"],
        tables,
        "",
    ))
    .resolve_tables()
    .await
    .unwrap();
    let ok = r.tables.iter().find(|s| s.table().name == "ok").unwrap();
    assert_eq!(ok.column("d").unwrap().value_type, V::Text);
    assert_eq!(ok.column("i").unwrap().value_type, V::Int64);
    let halted = |name: &str| {
        r.halted
            .iter()
            .find(|(t, _)| t.name == name)
            .map(|(_, h)| h.code())
    };
    assert_eq!(halted("sjis"), Some("SCHEMA_MISMATCH"));
    assert_eq!(
        halted("wide"),
        Some("NUMERIC_OVERFLOW"),
        "numeric_overflow_action: error"
    );
    assert_eq!(halted("reserved"), Some("RESERVED_COLUMN_COLLISION"));
    assert_eq!(
        halted("fkey"),
        Some("IDENTIFIER_NOT_USABLE"),
        "a floating key"
    );
    // The string action keeps the wide decimal as text, with a warning.
    let r = connector(&pipeline(
        &n,
        &["wide"],
        "      numeric_overflow_action: string",
        "",
    ))
    .resolve_tables()
    .await
    .unwrap();
    assert_eq!(r.tables[0].column("w").unwrap().value_type, V::Text);
    assert!(
        r.warnings.iter().any(|w| w.contains("decimal(65,30)")),
        "{:?}",
        r.warnings
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn mariadb_types_and_system_versioned_tables() {
    require_it!();
    if target().flavour != Flavour::Mariadb {
        eprintln!("skipped: MariaDB only");
        return;
    }
    let (n, _a) = fresh(
        "resolve_maria",
        "CREATE TABLE m (id INT PRIMARY KEY, u UUID, i4 INET4, i6 INET6, j JSON);
         CREATE TABLE hist (id INT PRIMARY KEY, v INT) WITH SYSTEM VERSIONING;",
    )
    .await;
    let r = connector(&pipeline(&n, &["m", "hist"], "", ""))
        .resolve_tables()
        .await
        .unwrap();
    let m = &r.tables[0];
    assert_eq!(m.column("u").unwrap().value_type, V::Uuid);
    assert_eq!(m.column("i4").unwrap().value_type, V::Text);
    assert_eq!(m.column("i6").unwrap().value_type, V::Text);
    assert_eq!(
        m.column("j").unwrap().value_type,
        V::Text,
        "MariaDB JSON is LONGTEXT"
    );
    assert_eq!(r.halted.len(), 1);
    assert_eq!(r.halted[0].1.code(), "TABLE_NOT_REPLICABLE");
    let r = connector(&pipeline(&n, &["'*'"], "", ""))
        .resolve_tables()
        .await
        .unwrap();
    assert_eq!(r.skipped.len(), 1, "a glob skips it");
    cleanup(&n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `external::check_partitioning`, `external::glob_match`
```

Run: `cargo test -p data-avalanche-config --test external`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `external::check_partitioning`, `external::glob_match`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-config/src/external.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/external.rs b/crates/data-avalanche-config/src/external.rs
--- a/crates/data-avalanche-config/src/external.rs
+++ b/crates/data-avalanche-config/src/external.rs
@@ -2,20 +2,21 @@
 //! - the cluster anchor from `globals.state` at startup (V-062, V-076, V-094; storage in M2);
 //! - a pipeline's resolved table set at start or on a table-set change (V-034, the warning half of
 //!   V-106, V-113; sources in M5/M7);
-//! - source column types at schema resolution (V-079).
+//! - source column types at schema resolution (V-079);
+//! - a table's schema, for its `tables.partitioning` entry (config §11.2).
 //!
 //! V-061 (a PROC field changed on reload) is part of [`crate::diff()`].
 
 use std::collections::{BTreeMap, BTreeSet};
 use std::time::Duration;
 
-use data_avalanche_core::TableRef;
+use data_avalanche_core::{HaltReason, LogicalType, TableRef, TableSchema};
 
 use crate::ctx::{At, Ctx};
 use crate::diag::Report;
 use crate::enums::{LockBackend, NumericOverflowAction, SourceType};
 use crate::path::YamlPath;
-use crate::resolved::{Globals, Pipeline};
+use crate::resolved::{Globals, PartitionField, PartitionTransform, Pipeline};
 use crate::units::format_duration;
 
 /// The values the first node wrote to the `globals.state` cluster anchor (Clustering §5.6). The
@@ -69,6 +70,56 @@ pub fn check_cluster_anchor(globals: &Globals, anchor: &ClusterAnchor) -> Report
     ctx.into_report()
 }
 
+/// A `tables.include`/`exclude` glob against one name (config §9.3): `*` matches within the name
+/// (it doesn't cross a `.`), and matching is case-sensitive.
+pub fn glob_match(pattern: &str, name: &str) -> bool {
+    fn go(p: &[u8], n: &[u8]) -> bool {
+        match p.split_first() {
+            None => n.is_empty(),
+            Some((b'*', rest)) => (0..=n.len()).any(|i| go(rest, &n[i..])),
+            Some((c, rest)) => n.first() == Some(c) && go(rest, &n[1..]),
+        }
+    }
+    go(pattern.as_bytes(), name.as_bytes())
+}
+
+/// A table's `tables.partitioning` entry against its schema (config §11.2): each column exists
+/// (`_ts` always does), and a temporal transform names a date or timestamp column.
+pub fn check_partitioning(
+    schema: &TableSchema,
+    fields: &[PartitionField],
+) -> Result<(), HaltReason> {
+    for f in fields.iter().filter(|f| f.column != "_ts") {
+        let temporal = !matches!(
+            f.transform,
+            PartitionTransform::Identity
+                | PartitionTransform::Bucket { .. }
+                | PartitionTransform::Truncate { .. }
+        );
+        let Some(c) = schema.column(&f.column) else {
+            return Err(HaltReason::SchemaMismatch {
+                column: f.column.clone(),
+                detail: "tables.partitioning names a column the table doesn't have".into(),
+            });
+        };
+        if temporal
+            && !matches!(
+                c.logical,
+                LogicalType::Date | LogicalType::TimestampTz | LogicalType::TimestampNtz
+            )
+        {
+            return Err(HaltReason::SchemaMismatch {
+                column: f.column.clone(),
+                detail: format!(
+                    "a {:?} partition transform needs a date or timestamp column",
+                    f.transform
+                ),
+            });
+        }
+    }
+    Ok(())
+}
+
 /// A per-table config key as a table: PG `schema.table`; MySQL `name` in `source.db` (§10).
 fn key_table(p: &Pipeline, key: &str) -> Option<TableRef> {
     match p.source.kind {
```

In `crates/data-avalanche-source-mysql/src/catalog.rs`, add above the test module:

```rust
//! Catalog reads over `information_schema` (`Schema_Change…` §3 channels A, C, D; config §11.2):
//! which tables of `source.db` the globs select, their columns and primary key, and each table's
//! schema and decode plans. MySQL table patterns and per-table keys are bare names (V-064).

use std::collections::BTreeMap;

use data_avalanche_config::{
    Tables, Tinyint1Detection, check_partitioning, check_table_set, glob_match,
};
use data_avalanche_core::{
    ColumnSchema, HaltReason, ResolvedTables, SourceError, TableRef, TableSchema,
};
use mysql_async::prelude::Queryable;

use crate::connector::MySqlConnector;
use crate::types::{CatalogColumn, ColumnPlan, plan_column};

/// A table the globs can select: `BASE TABLE`, or MariaDB's `SYSTEM VERSIONED`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub table: TableRef,
    pub versioned: bool,
}

/// What the globs selected.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selection {
    /// Each table with `true` when a pattern without `*` named it.
    pub tables: Vec<(Listed, bool)>,
    pub skipped: Vec<(TableRef, String)>,
    pub halted: Vec<(TableRef, HaltReason)>,
    pub warnings: Vec<String>,
}

/// Why a system-versioned table isn't replicated.
const VERSIONED: &str = "a MariaDB system-versioned table: its history rows and period columns \
    aren't replicated (DROP SYSTEM VERSIONING, or exclude it)";

/// The table's config key: its bare name (V-064).
pub fn table_key(t: &TableRef) -> &str {
    &t.name
}

/// Applies `include` minus `exclude` to the database's tables. A system-versioned table halts
/// (`TABLE_NOT_REPLICABLE`) when its name was listed and is skipped with a warning when a glob
/// found it. An include pattern that matches nothing warns.
pub fn select(include: &[String], exclude: &[String], listed: &[Listed]) -> Selection {
    let mut out = Selection::default();
    for p in include {
        if !listed.iter().any(|l| glob_match(p, &l.table.name)) {
            out.warnings
                .push(format!("tables.include {p} matches no table"));
        }
    }
    for l in listed {
        let named = include
            .iter()
            .filter(|p| glob_match(p, &l.table.name))
            .map(|p| !p.contains('*'))
            .reduce(|a, b| a || b);
        let Some(explicit) = named else { continue };
        if exclude.iter().any(|p| glob_match(p, &l.table.name)) {
            continue;
        }
        if l.versioned {
            match explicit {
                true => out.halted.push((
                    l.table.clone(),
                    HaltReason::NotReplicable {
                        reason: VERSIONED.into(),
                    },
                )),
                false => out.skipped.push((l.table.clone(), VERSIONED.into())),
            }
            continue;
        }
        out.tables.push((l.clone(), explicit));
    }
    out
}

/// A table as the catalog describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Described {
    pub table: TableRef,
    /// In `ORDINAL_POSITION` order.
    pub columns: Vec<CatalogColumn>,
    pub primary_key: Vec<String>,
}

/// What planning a table noticed that isn't a halt.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Notes {
    /// Decimals wider than 38 digits kept as text: `(column, type)`.
    pub read_as_text: Vec<(String, String)>,
}

/// A planned table: its schema, and each column's decode plan in schema order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub schema: TableSchema,
    pub plans: Vec<ColumnPlan>,
    pub notes: Notes,
}

/// A table's schema and plans, checked in this order (config §11.2): every column plans
/// ([`plan_column`]: overrides, charsets, decimal caps); the identifier (the
/// `tables.primary_key` entry, else the primary key) passes [`TableSchema::new`] (reserved names,
/// a non-empty identifier of usable columns); `tables.partitioning` fits the columns.
pub fn plan_table(
    d: &Described,
    tables: &Tables,
    tinyint1_as_bool: bool,
) -> Result<Planned, HaltReason> {
    let key = table_key(&d.table);
    let identifier = tables
        .primary_key
        .get(key)
        .cloned()
        .unwrap_or_else(|| d.primary_key.clone());
    let mut notes = Notes::default();
    let mut columns = Vec::with_capacity(d.columns.len());
    let mut plans = Vec::with_capacity(d.columns.len());
    for c in &d.columns {
        let over = tables
            .column_type_overrides
            .get(&format!("{key}.{}", c.name));
        let p = plan_column(
            c,
            tinyint1_as_bool,
            tables.numeric_overflow_action,
            over,
            identifier.contains(&c.name),
        )?;
        if let Some(t) = &p.read_as_text {
            notes.read_as_text.push((c.name.clone(), t.clone()));
        }
        columns.push(ColumnSchema {
            name: c.name.clone(),
            value_type: p.value_type.clone(),
            logical: p.logical.clone(),
            nullable: c.nullable,
            source_type: c.column_type.clone(),
            source_ordinal: Some(c.ordinal as i32),
        });
        plans.push(p);
    }
    let schema = TableSchema::new(d.table.clone(), columns, identifier)?;
    if let Some(fields) = tables.partitioning.get(key) {
        check_partitioning(&schema, fields)?;
    }
    Ok(Planned {
        schema,
        plans,
        notes,
    })
}

/// `information_schema` rows of one table name per query: an `IN` list this long at most.
const NAMES_PER_QUERY: usize = 500;

impl MySqlConnector {
    /// `tinyint1_as_bool` with `tinyint1_detection: information_schema` (config §9.2.2).
    pub(crate) fn tinyint1_as_bool(&self) -> bool {
        self.mysql.tinyint1_as_bool
            && self.mysql.tinyint1_detection == Tinyint1Detection::InformationSchema
    }

    /// The tables of `source.db` the globs can select.
    pub async fn list_tables(&self) -> Result<Vec<Listed>, SourceError> {
        let db = self.pipeline.source.db.clone();
        let rows: Vec<(String, String)> = self
            .conn
            .command("list tables", move |c| {
                Box::pin(async move {
                    c.exec(
                        "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES
                          WHERE TABLE_SCHEMA = ? AND TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED')
                          ORDER BY TABLE_NAME",
                        (db,),
                    )
                    .await
                })
            })
            .await?;
        Ok(rows
            .into_iter()
            .map(|(name, kind)| Listed {
                table: TableRef::new(self.pipeline.source.db.clone(), name),
                versioned: kind == "SYSTEM VERSIONED",
            })
            .collect())
    }

    /// The columns and primary keys of the tables named `names` in `source.db`; a table that
    /// isn't there (dropped since it was listed) is left out.
    pub async fn describe(
        &self,
        names: &[String],
    ) -> Result<BTreeMap<String, Described>, SourceError> {
        let mut out: BTreeMap<String, Described> = BTreeMap::new();
        for chunk in names.chunks(NAMES_PER_QUERY) {
            let db = self.pipeline.source.db.clone();
            let list = vec!["?"; chunk.len()].join(", ");
            let mut params: Vec<mysql_async::Value> = vec![db.clone().into()];
            params.extend(chunk.iter().map(|n| n.clone().into()));
            let keys_params = params.clone();
            let (columns, keys): (Vec<mysql_async::Row>, Vec<(String, String)>) = self
                .conn
                .command("describe tables", move |c| {
                    Box::pin(async move {
                        let columns = c
                            .exec(
                                format!(
                                    "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE,
                                            COLUMN_TYPE, CHARACTER_SET_NAME, NUMERIC_PRECISION,
                                            NUMERIC_SCALE, DATETIME_PRECISION,
                                            CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                                       FROM information_schema.COLUMNS
                                      WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ({list})
                                      ORDER BY TABLE_NAME, ORDINAL_POSITION"
                                ),
                                params,
                            )
                            .await?;
                        let keys = c
                            .exec(
                                format!(
                                    "SELECT TABLE_NAME, COLUMN_NAME
                                       FROM information_schema.KEY_COLUMN_USAGE
                                      WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ({list})
                                        AND CONSTRAINT_NAME = 'PRIMARY'
                                      ORDER BY TABLE_NAME, ORDINAL_POSITION"
                                ),
                                keys_params,
                            )
                            .await?;
                        Ok((columns, keys))
                    })
                })
                .await?;
            for r in columns {
                let table: String = r.get(0).unwrap_or_default();
                let d = out.entry(table.clone()).or_insert_with(|| Described {
                    table: TableRef::new(db.clone(), table),
                    columns: vec![],
                    primary_key: vec![],
                });
                d.columns.push(CatalogColumn {
                    name: r.get(1).unwrap_or_default(),
                    ordinal: r.get(2).unwrap_or_default(),
                    data_type: r
                        .get::<String, _>(3)
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                    column_type: r
                        .get::<String, _>(4)
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                    charset: r.get(5).flatten(),
                    precision: r.get(6).flatten(),
                    scale: r.get(7).flatten(),
                    fsp: r.get(8).flatten(),
                    max_length: r.get(9).flatten(),
                    nullable: r.get::<String, _>(10).is_some_and(|n| n == "YES"),
                });
            }
            for (table, column) in keys {
                if let Some(d) = out.get_mut(&table) {
                    d.primary_key.push(column);
                }
            }
        }
        Ok(out)
    }

    /// Channel A: the configured table set, resolved against the catalog and checked (config
    /// §11.2): a table a glob found without a primary key (and no `tables.primary_key` entry) is
    /// skipped with a warning; one named without a glob halts.
    pub async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
        let listed = self.list_tables().await?;
        let t = &self.pipeline.tables;
        let selection = select(&t.include, &t.exclude, &listed);
        let names: Vec<String> = selection
            .tables
            .iter()
            .map(|(l, _)| l.table.name.clone())
            .collect();
        let mut described = self.describe(&names).await?;
        let mut out = ResolvedTables {
            skipped: selection.skipped,
            halted: selection.halted,
            warnings: selection.warnings,
            ..ResolvedTables::default()
        };
        for (listed, explicit) in selection.tables {
            let Some(d) = described.remove(&listed.table.name) else {
                continue; // dropped since it was listed
            };
            match plan_table(&d, t, self.tinyint1_as_bool()) {
                Ok(p) => {
                    for (column, ty) in p.notes.read_as_text {
                        out.warnings.push(format!(
                            "{}.{column}: {ty} is wider than 38 digits and is read as text",
                            listed.table.name
                        ));
                    }
                    out.tables.push(p.schema);
                }
                Err(HaltReason::NoIdentifier) if !explicit => out.skipped.push((
                    listed.table,
                    "no primary key and no tables.primary_key entry".into(),
                )),
                Err(reason) => out.halted.push((listed.table, reason)),
            }
        }
        // Halted tables are in the set: their per-table config keys aren't stale (V-106).
        let resolved: Vec<TableRef> = out
            .tables
            .iter()
            .map(|s| s.table().clone())
            .chain(out.halted.iter().map(|(t, _)| t.clone()))
            .collect();
        let report = check_table_set(&self.pipeline, &resolved);
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
}

```

Change `crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -12,6 +12,7 @@ use crate::server::{Flavour, Settings, check_settings, grants_replication};
 /// configuration; connections open on first use.
 #[derive(Debug)]
 pub struct MySqlConnector {
+    pub(crate) pipeline: Pipeline,
     pub(crate) mysql: Mysql,
     pub(crate) conn: Connector,
     /// `VERSION()`, read once: the dialect every later statement depends on.
@@ -32,6 +33,7 @@ impl MySqlConnector {
             }
         };
         Ok(Self {
+            pipeline: pipeline.clone(),
             mysql,
             conn: Connector::new(source)?,
             version: OnceCell::new(),
```

Change `crates/data-avalanche-source-mysql/src/types.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/types.rs b/crates/data-avalanche-source-mysql/src/types.rs
--- a/crates/data-avalanche-source-mysql/src/types.rs
+++ b/crates/data-avalanche-source-mysql/src/types.rs
@@ -27,6 +27,10 @@ pub struct CatalogColumn {
     pub fsp: Option<u32>,
     /// `CHARACTER_MAXIMUM_LENGTH`: `n` of a `BINARY(n)`.
     pub max_length: Option<u64>,
+    /// `ORDINAL_POSITION`, from 1.
+    pub ordinal: u32,
+    /// `IS_NULLABLE`.
+    pub nullable: bool,
 }
 
 impl CatalogColumn {
```

Change `crates/data-avalanche-source-pg/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/catalog.rs b/crates/data-avalanche-source-pg/src/catalog.rs
--- a/crates/data-avalanche-source-pg/src/catalog.rs
+++ b/crates/data-avalanche-source-pg/src/catalog.rs
@@ -3,24 +3,13 @@
 
 use std::collections::{BTreeMap, HashMap};
 
-use data_avalanche_config::{PartitionTransform, Tables};
-use data_avalanche_core::{ColumnSchema, HaltReason, LogicalType, TableRef, TableSchema};
+use data_avalanche_config::{Tables, check_partitioning};
+use data_avalanche_core::{ColumnSchema, HaltReason, TableRef, TableSchema};
 use tokio_postgres::Client;
 
 use crate::types::{TypeCatalog, TypeInfo, missing, plan_column};
 
-/// `*` matches within one name (it doesn't cross the `.`); matching is case-sensitive
-/// (config §9.3 glob grammar).
-pub fn glob_match(pattern: &str, name: &str) -> bool {
-    fn go(p: &[u8], n: &[u8]) -> bool {
-        match p.split_first() {
-            None => n.is_empty(),
-            Some((b'*', rest)) => (0..=n.len()).any(|i| go(rest, &n[i..])),
-            Some((c, rest)) => n.first() == Some(c) && go(rest, &n[1..]),
-        }
-    }
-    go(pattern.as_bytes(), name.as_bytes())
-}
+pub use data_avalanche_config::glob_match;
 
 /// A PostgreSQL include/exclude pattern (`schema.table`, V-117) against a table.
 pub fn pattern_matches(pattern: &str, table: &TableRef) -> bool {
@@ -234,36 +223,8 @@ pub fn table_schema(
             notes.c_ordered_keys.push((k.clone(), collation));
         }
     }
-    for f in tables.partitioning.get(&key).into_iter().flatten() {
-        let temporal = !matches!(
-            f.transform,
-            PartitionTransform::Identity
-                | PartitionTransform::Bucket { .. }
-                | PartitionTransform::Truncate { .. }
-        );
-        if f.column == "_ts" {
-            continue;
-        }
-        let Some(c) = schema.column(&f.column) else {
-            return Err(HaltReason::SchemaMismatch {
-                column: f.column.clone(),
-                detail: "tables.partitioning names a column the table doesn't have".into(),
-            });
-        };
-        if temporal
-            && !matches!(
-                c.logical,
-                LogicalType::Date | LogicalType::TimestampTz | LogicalType::TimestampNtz
-            )
-        {
-            return Err(HaltReason::SchemaMismatch {
-                column: f.column.clone(),
-                detail: format!(
-                    "a {:?} partition transform needs a date or timestamp column",
-                    f.transform
-                ),
-            });
-        }
+    if let Some(fields) = tables.partitioning.get(&key) {
+        check_partitioning(&schema, fields)?;
     }
     Ok((schema, notes))
 }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 46 tests.

Run: `cargo test -p data-avalanche-config`

Expected: PASS, every test binary (148 tests).

Run: `cargo test -p data-avalanche-source-pg --lib`

Expected: PASS, 83 tests.

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql resolve:: || break; done`

Expected: PASS, 4 tests on each target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/external.rs \
  crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/tests/external.rs \
  crates/data-avalanche-source-mysql/src/catalog.rs \
  crates/data-avalanche-source-mysql/src/connector.rs \
  crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/types.rs \
  crates/data-avalanche-source-mysql/tests/mysql/main.rs \
  crates/data-avalanche-source-mysql/tests/mysql/resolve.rs \
  crates/data-avalanche-source-pg/src/catalog.rs
git commit -m "feat(source-mysql): channel A — table selection, the catalog over information_schema, schemas and decode plans"
```

---

### Task 10: Snapshot chunks: locking keyset reads in the key's own order

**Files:**
- Modify: `crates/data-avalanche-source-mysql/src/catalog.rs`
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`
- Create: `crates/data-avalanche-source-mysql/src/snapshot.rs`
- Modify: `crates/data-avalanche-source-mysql/src/types.rs`
- Modify: `crates/data-avalanche-source-mysql/src/value.rs`
- Modify: `crates/data-avalanche-source-mysql/tests/mysql/main.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs`

**Interfaces:**
- Consumes:
  - `describe`, `plan_table` (Task 9); `decode` with `Input::Row` (Task 7).
  - `data_avalanche_core::{Chunk, KeysetCursor, TableSchema}`.
- Produces:
  - `data-avalanche-source-mysql/src/snapshot.rs`:
    - `pub async fn read_chunk(`

`read_chunk` (Snapshot_Resumability §1.1 and §2, DA-SNAP-10, Q-M7-3; D-M7a.13, D-M7a.14).

- **Locking read**: each chunk is a short `READ COMMITTED` transaction whose `SELECT` ends `LOCK
  IN SHARE MODE` (MariaDB has no `FOR SHARE`). A transaction the binlog already holds but InnoDB
  hasn't committed (semi-sync `AFTER_SYNC`) blocks the read until it commits, so no chunk misses
  a row the stream delivered before it (S-MY11, measured). A lock wait timeout (1205) is
  `Transient`: the chunk is read again.
- **Order**: a text key is compared in its own collation (`CONVERT(? USING cs) COLLATE coll`),
  so the server's index serves the chunk; the cursor records the collations, and a cursor of
  others halts the table. This replaces the roadmap's `utf8mb4_bin` pin: a binary collation
  has no index and makes every chunk a full scan and sort.
- **Predicate**: the expanded keyset form `a > ? OR (a = ? AND b > ?)`. A row constructor
  `(a, b) > (?, ?)` walks the whole index on both servers (`EXPLAIN`, measured); the expanded
  form is a range.
- **Operands** per type: integers, `YEAR` and booleans as integers; decimals as `CAST(? AS
  DECIMAL(p,s))`; bytes as `UNHEX(?)`; `BIT` as an unsigned integer; dates and times as date
  and time values; `ENUM` by its member index; a `SET` or float key only under a `string`
  override (core refuses them as identifiers otherwise).
- **Changes under the snapshot**: a table gone (1146) halts `TABLE_DROPPED`; a column gone or
  of another type (1054) is `Transient` until the stream's signal catches up; a key column
  nullable now halts (`IDENTIFIER_NOT_USABLE`). A boolean key value other than 0 or 1 halts
  (`SCHEMA_MISMATCH`): two keys would merge (Q-M5-18's rule). Chunks carry no visibility: the
  pipeline's watermark window orders them.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -12,6 +12,7 @@ mod error;
 pub mod json;
 pub mod position;
 pub mod server;
+mod snapshot;
 pub mod types;
 pub mod value;
 
```

Change `crates/data-avalanche-source-mysql/tests/mysql/main.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/main.rs b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/main.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
@@ -7,5 +7,6 @@ mod common;
 mod position;
 mod preflight;
 mod resolve;
+mod snapshot;
 mod tls;
 mod values;
```

Create `crates/data-avalanche-source-mysql/src/snapshot.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::Decimal;

    use super::*;
    use crate::value::Charset;

    fn column(data_type: &str, decode: Decode) -> ReadColumn {
        ReadColumn {
            catalog: CatalogColumn {
                name: "k".into(),
                data_type: data_type.into(),
                ..Default::default()
            },
            decode,
            identifier: true,
        }
    }

    fn operand(c: &ReadColumn, v: ColumnValue) -> (String, Value) {
        key_operand(c, &v).unwrap()
    }

    #[test]
    fn integers_and_their_overrides_travel_as_integers() {
        let int = |unsigned| column("bigint", Decode::Int { bits: 64, unsigned });
        assert_eq!(
            operand(&int(true), ColumnValue::UInt64(u64::MAX)),
            ("?".into(), Value::UInt(u64::MAX))
        );
        assert_eq!(
            operand(&int(false), ColumnValue::Int64(-5)),
            ("?".into(), Value::Int(-5))
        );
        let as_string = column(
            "int",
            Decode::String(Box::new(Decode::Int {
                bits: 32,
                unsigned: false,
            })),
        );
        assert_eq!(
            operand(&as_string, ColumnValue::Text("-7".into())).1,
            Value::Int(-7)
        );
        let as_decimal = column(
            "int",
            Decode::Convert {
                from: Box::new(Decode::Int {
                    bits: 32,
                    unsigned: false,
                }),
                to: data_avalanche_core::ColumnValueType::Numeric {
                    precision: 12,
                    scale: 2,
                },
            },
        );
        assert_eq!(
            operand(
                &as_decimal,
                ColumnValue::Numeric(Decimal::parse("42.00").unwrap())
            )
            .1,
            Value::UInt(42)
        );
        assert_eq!(
            operand(&column("tinyint", Decode::Bool), ColumnValue::Bool(true)).1,
            Value::UInt(1)
        );
    }

    #[test]
    fn decimals_cast_and_text_keys_use_their_collation() {
        let mut d = column("decimal", Decode::Decimal);
        (d.catalog.precision, d.catalog.scale) = (Some(30), Some(10));
        assert_eq!(
            operand(
                &d,
                ColumnValue::Numeric(Decimal::parse("12345678901234567890.0000000001").unwrap())
            ),
            (
                "CAST(? AS DECIMAL(30,10))".into(),
                text_param("12345678901234567890.0000000001")
            )
        );
        let mut t = column(
            "varchar",
            Decode::Text {
                charset: Charset::Latin1,
            },
        );
        t.catalog.charset = Some("latin1".into());
        t.catalog.collation = Some("latin1_swedish_ci".into());
        assert_eq!(
            operand(&t, ColumnValue::Text("é".into())),
            (
                "CONVERT(? USING latin1) COLLATE latin1_swedish_ci".into(),
                text_param("é")
            )
        );
        assert_eq!(cursor_collation(&[&t]), Some("latin1_swedish_ci".into()));
        assert_eq!(
            cursor_collation(&[&column(
                "int",
                Decode::Int {
                    bits: 32,
                    unsigned: false
                }
            )]),
            None
        );
    }

    #[test]
    fn bytes_bits_enums_and_sets_take_their_index_order() {
        let bytes = column("varbinary", Decode::Bytes { pad: None });
        assert_eq!(
            operand(&bytes, ColumnValue::Bytes(vec![0, 0xff, b'a'])),
            ("UNHEX(?)".into(), text_param("00FF61"))
        );
        let bit = column("bit", Decode::Bytes { pad: None });
        assert_eq!(
            operand(&bit, ColumnValue::Bytes(vec![0x02, 0x01])).1,
            Value::UInt(513)
        );
        let members = vec!["z".to_owned(), "a".to_owned(), "m".to_owned()];
        let e = column(
            "enum",
            Decode::Enum {
                members: members.clone(),
            },
        );
        assert_eq!(operand(&e, ColumnValue::Text("a".into())).1, Value::UInt(2));
        assert_eq!(operand(&e, ColumnValue::Text("".into())).1, Value::UInt(0));
        assert!(key_operand(&e, &ColumnValue::Text("q".into())).is_err());
        let s_text = column("set", Decode::String(Box::new(Decode::Set { members })));
        assert_eq!(
            operand(&s_text, ColumnValue::Text("z,m".into())).1,
            Value::UInt(0b101)
        );
    }

    #[test]
    fn clocks_travel_as_dates_and_times() {
        let date = column("date", Decode::Date);
        assert_eq!(
            operand(&date, ColumnValue::Date(-1)).1,
            Value::Date(1969, 12, 31, 0, 0, 0, 0)
        );
        let dt = column("datetime", Decode::DateTime { fsp: 6 });
        assert_eq!(
            operand(
                &dt,
                ColumnValue::Timestamp {
                    micros: 1_700_000_000_123_456,
                    with_tz: false
                }
            )
            .1,
            Value::Date(2023, 11, 14, 22, 13, 20, 123_456)
        );
        let tm = column("time", Decode::Time { fsp: 6 });
        assert_eq!(
            operand(
                &tm,
                ColumnValue::Time(-(838 * 3600 + 59 * 60 + 59) * 1_000_000 - 1)
            )
            .1,
            Value::Time(true, 34, 22, 59, 59, 1)
        );
    }

    #[test]
    fn the_keyset_predicate_is_spelled_out() {
        let keys = vec![
            ("`a`".to_owned(), "?".to_owned(), Value::Int(1)),
            ("`b`".to_owned(), "UNHEX(?)".to_owned(), text_param("00")),
        ];
        let (sql, params) = keyset_predicate(&keys);
        assert_eq!(sql, "(`a` > ?) OR (`a` = ? AND `b` > UNHEX(?))");
        assert_eq!(params, [Value::Int(1), Value::Int(1), text_param("00")]);
        assert_eq!(ident("we`ird"), "`we``ird`");
    }
}
```

Change `crates/data-avalanche-source-mysql/src/value.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/value.rs b/crates/data-avalanche-source-mysql/src/value.rs
--- a/crates/data-avalanche-source-mysql/src/value.rs
+++ b/crates/data-avalanche-source-mysql/src/value.rs
@@ -681,6 +681,18 @@ mod tests {
         Value::Bytes(s.to_vec())
     }
 
+    #[test]
+    fn a_boolean_key_other_than_0_or_1_is_unrepresentable() {
+        let key = |v| decode(&Decode::Bool, Input::Row(&Value::Int(v)), true);
+        assert_eq!(key(1), Ok(ColumnValue::Bool(true)));
+        assert_eq!(key(0), Ok(ColumnValue::Bool(false)));
+        assert!(matches!(key(2), Err(ValueError::Unrepresentable(_))));
+        assert_eq!(
+            row(&Decode::Bool, Value::Int(2)),
+            Ok(ColumnValue::Bool(true))
+        );
+    }
+
     #[test]
     fn integers_read_with_the_catalogs_signedness() {
         let mi = Decode::Int {
```

Create `crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs`:

```rust
//! Snapshot chunks against the target server (Snapshot_Resumability §1.1, §2; DA-SNAP-10): every
//! key type walks in chunks without a skip or a repeat, in the server's order; the locking read
//! waits for a writer of its rows; a table gone or changed under the snapshot halts or retries.

use std::time::Duration;

use data_avalanche_core::{ColumnValue, HaltReason, KeysetCursor, Row, SourceError, TableSchema};
use data_avalanche_source_mysql::{Flavour, MySqlConnector};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, target};

/// Every row of `t`, in chunks of `limit`; the cursors along the way.
async fn walk(c: &MySqlConnector, t: &TableSchema, limit: usize) -> (Vec<Row>, Vec<KeysetCursor>) {
    let (mut rows, mut cursors) = (vec![], vec![]);
    let mut after: Option<KeysetCursor> = None;
    loop {
        let chunk = c
            .read_chunk(t, after.as_ref(), limit)
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", t.table()));
        assert!(chunk.visibility.is_none());
        assert!(chunk.rows.len() <= limit);
        rows.extend(chunk.rows);
        match chunk.next {
            None => return (rows, cursors),
            Some(next) => {
                cursors.push(next.clone());
                after = Some(next);
            }
        }
    }
}

/// Key types and values chosen where a cursor parameter could go wrong: past `i64`, digits a
/// double loses, case and accents under the column's collation, latin1 above ASCII, trailing zero
/// bytes, the clock ranges' ends, negative times, ENUM and SET in member order, BIT as a number.
const KEYS: &[(&str, &str, &str)] = &[
    (
        "bu",
        "BIGINT UNSIGNED",
        "0, 1, 9223372036854775808, 18446744073709551615",
    ),
    ("i", "INT", "-5, 0, 3, 2147483647"),
    (
        "dec",
        "DECIMAL(30,10)",
        "12345678901234567890.0000000001, 12345678901234567890.0000000002, \
         12345678901234567890.0000000003, -1",
    ),
    ("ci", "VARCHAR(10)", "'a', 'B', 'c', 'ö', 'Z', 'aa'"),
    (
        "l1",
        "VARCHAR(10) CHARACTER SET latin1",
        "'a', 'B', 'é', 'ÿ', '€', 'z'",
    ),
    (
        "bin",
        "VARCHAR(10) COLLATE utf8mb4_bin",
        "'a', 'B', 'b', 'A'",
    ),
    ("vb", "VARBINARY(8)", "x'00', x'0000', x'FF', x'61', x''"),
    ("bn", "BINARY(3)", "x'00', x'0001', x'FF', x'000001'"),
    (
        "dt",
        "DATETIME(6)",
        "'1000-01-01 00:00:00', '2024-02-29 12:00:00.000001', '2024-02-29 12:00:00', \
         '9999-12-31 23:59:59.999999'",
    ),
    (
        "ts",
        "TIMESTAMP(3)",
        "'1970-01-01 00:00:01', '2038-01-19 03:14:07.999', '2000-01-01 00:00:00.5'",
    ),
    ("d", "DATE", "'1000-01-01', '1969-12-31', '9999-12-31'"),
    (
        "tm",
        "TIME(6)",
        "'-838:59:59', '-00:00:00.000001', '00:00:00', '838:59:59', '-01:00:00'",
    ),
    ("y", "YEAR", "1901, 2155, '0000', 1999"),
    ("e", "ENUM('z','a','m')", "'z', 'a', 'm'"),
    ("st", "SET('x','y','z')", "'', 'x', 'y,z', 'x,z', 'z'"),
    ("bt", "BIT(10)", "0, 1, 512, 1023, 2"),
    ("f", "FLOAT", "-1.5, 0.1, 3.4e38, 0"),
    ("db", "DOUBLE", "0.1, 0.2, 1e300, -0.30000000000000004"),
];

/// MariaDB's own types.
const MARIADB_KEYS: &[(&str, &str, &str)] = &[
    (
        "uu",
        "UUID",
        "'00000000-0000-1000-8000-000000000002', '00000001-0000-1000-8000-000000000001', \
         'ffffffff-ffff-4fff-bfff-ffffffffffff', '00000000-0000-0000-0000-000000000000'",
    ),
    (
        "i4",
        "INET4",
        "'0.0.0.0', '10.0.0.1', '255.255.255.255', '9.255.0.1'",
    ),
    (
        "i6",
        "INET6",
        "'::', '::1', 'fe80::1', '2001:db8::ff00:42:8329', '::ffff:1.2.3.4'",
    ),
];

#[tokio::test]
async fn every_key_type_walks_in_the_servers_order() {
    require_it!();
    let mut keys = KEYS.to_vec();
    if target().flavour == Flavour::Mariadb {
        keys.extend_from_slice(MARIADB_KEYS);
    }
    let mut ddl = String::new();
    for (name, ty, values) in &keys {
        let rows: Vec<String> = values
            .split(", ")
            .enumerate()
            .map(|(i, v)| format!("({v}, {i})"))
            .collect();
        ddl += &format!(
            "CREATE TABLE k_{name} (k {ty} NOT NULL PRIMARY KEY, n INT);
             INSERT INTO k_{name} VALUES {};",
            rows.join(", ")
        );
    }
    // A composite key, the second column breaking ties.
    ddl += "CREATE TABLE k_pair (a INT, b VARCHAR(5), n INT, PRIMARY KEY (a, b));
            INSERT INTO k_pair VALUES (1, 'b', 0), (1, 'a', 1), (2, 'a', 2), (0, 'z', 3),
                                      (1, 'C', 4);";
    let (n, _a) = fresh("snapshot_keys", &ddl).await;
    // Float and SET keys are identifiers only as text.
    let p = pipeline(
        &n,
        &["'k_*'"],
        "      column_type_overrides: { k_f.k: string, k_db.k: string, k_st.k: string }",
        "",
    );
    let c = connector(&p);
    let r = c.resolve_tables().await.expect("resolve");
    assert!(r.halted.is_empty(), "{:?}", r.halted);
    assert_eq!(r.tables.len(), keys.len() + 1);
    for t in &r.tables {
        let (whole, _) = walk(&c, t, 1000).await;
        let (chunked, cursors) = walk(&c, t, 2).await;
        assert_eq!(
            chunked,
            whole,
            "{}: chunks of 2 read every row once, in the server's order",
            t.table()
        );
        assert_eq!(
            whole.len(),
            match t.table().name.as_str() {
                "k_pair" => 5,
                name => keys
                    .iter()
                    .find(|k| format!("k_{}", k.0) == name)
                    .unwrap()
                    .2
                    .split(", ")
                    .count(),
            },
            "{}",
            t.table()
        );
        let text_key = ["k_ci", "k_l1", "k_bin", "k_pair"].contains(&t.table().name.as_str());
        assert_eq!(
            cursors[0].collation.is_some(),
            text_key,
            "{}: {:?}",
            t.table(),
            cursors[0].collation
        );
    }
    cleanup(&n).await;
}

#[tokio::test]
async fn a_locking_read_waits_for_a_writer_of_its_rows() {
    require_it!();
    let (n, mut a) = fresh(
        "snapshot_lock",
        "CREATE TABLE t (id INT PRIMARY KEY, v INT);
         INSERT INTO t VALUES (1, 1), (2, 1);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let t = c.resolve_tables().await.unwrap().tables.remove(0);
    a.query_drop("START TRANSACTION; UPDATE t SET v = 2 WHERE id = 1")
        .await
        .unwrap();
    let read = tokio::spawn({
        let t = t.clone();
        async move { c.read_chunk(&t, None, 10).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!read.is_finished(), "the read waits for the open writer");
    a.query_drop("COMMIT").await.unwrap();
    let chunk = read.await.unwrap().expect("the chunk");
    assert_eq!(
        chunk.rows[0].get("v"),
        Some(&ColumnValue::Int32(2)),
        "the committed value"
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn a_table_changed_under_the_snapshot_halts_or_waits() {
    require_it!();
    let (n, mut a) = fresh(
        "snapshot_changed",
        "CREATE TABLE gone (id INT PRIMARY KEY);
         CREATE TABLE typed (id INT PRIMARY KEY, v INT);
         CREATE TABLE named (k VARCHAR(5) PRIMARY KEY);
         INSERT INTO named VALUES ('a'), ('b');
         CREATE TABLE keyed (k INT NOT NULL, UNIQUE (k));
         CREATE TABLE flag (k TINYINT(1) PRIMARY KEY);
         INSERT INTO flag VALUES (0), (1), (2);",
    )
    .await;
    let c = connector(&pipeline(
        &n,
        &["gone", "typed", "named", "keyed", "flag"],
        "      primary_key: { keyed: [k] }",
        "",
    ));
    let r = c.resolve_tables().await.unwrap();
    let schema = |name: &str| r.tables.iter().find(|s| s.table().name == name).unwrap();
    let halted = |e: SourceError| match e {
        SourceError::TableHalt { reason, .. } => reason,
        other => panic!("not a table halt: {other}"),
    };
    let first = c.read_chunk(schema("named"), None, 1).await.unwrap();
    a.query_drop(
        "DROP TABLE gone;
         ALTER TABLE typed MODIFY v VARCHAR(5);
         ALTER TABLE named MODIFY k VARCHAR(5) COLLATE utf8mb4_bin;
         ALTER TABLE keyed MODIFY k INT NULL;",
    )
    .await
    .unwrap();
    assert_eq!(
        halted(c.read_chunk(schema("gone"), None, 10).await.unwrap_err()),
        HaltReason::TableDropped
    );
    assert!(matches!(
        c.read_chunk(schema("typed"), None, 10).await.unwrap_err(),
        SourceError::Transient(m) if m.contains("column v is now")
    ));
    assert!(matches!(
        halted(c.read_chunk(schema("named"), first.next.as_ref(), 10).await.unwrap_err()),
        HaltReason::SchemaMismatch { detail, .. } if detail.contains("re-snapshot")
    ));
    assert!(matches!(
        halted(c.read_chunk(schema("keyed"), None, 10).await.unwrap_err()),
        HaltReason::IdentifierNotUsable { reason, .. } if reason.contains("nullable")
    ));
    assert!(matches!(
        halted(c.read_chunk(schema("flag"), None, 10).await.unwrap_err()),
        HaltReason::SchemaMismatch { detail, .. } if detail.contains("TINYINT(1) key value 2")
    ));
    cleanup(&n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0425]: cannot find type `Decode` in this scope
error[E0425]: cannot find type `ReadColumn` in this scope
error[E0422]: cannot find struct, variant or union type `ReadColumn` in this scope
error[E0422]: cannot find struct, variant or union type `CatalogColumn` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-source-mysql/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/catalog.rs b/crates/data-avalanche-source-mysql/src/catalog.rs
--- a/crates/data-avalanche-source-mysql/src/catalog.rs
+++ b/crates/data-avalanche-source-mysql/src/catalog.rs
@@ -215,7 +215,7 @@ impl MySqlConnector {
                                     "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE,
                                             COLUMN_TYPE, CHARACTER_SET_NAME, NUMERIC_PRECISION,
                                             NUMERIC_SCALE, DATETIME_PRECISION,
-                                            CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
+                                            CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLLATION_NAME
                                        FROM information_schema.COLUMNS
                                       WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ({list})
                                       ORDER BY TABLE_NAME, ORDINAL_POSITION"
@@ -263,6 +263,7 @@ impl MySqlConnector {
                     fsp: r.get(8).flatten(),
                     max_length: r.get(9).flatten(),
                     nullable: r.get::<String, _>(10).is_some_and(|n| n == "YES"),
+                    collation: r.get(11).flatten(),
                 });
             }
             for (table, column) in keys {
```

In `crates/data-avalanche-source-mysql/src/snapshot.rs`, add above the test module:

```rust
//! Snapshot chunks (Snapshot_Resumability §1.1, §2; DA-SNAP-10): keyset chunks in identifier
//! order, each read in a short READ COMMITTED transaction with `LOCK IN SHARE MODE`. The shared
//! row locks wait for a writer of the chunk's rows still committing (a semi-sync commit waiting at
//! AFTER_SYNC is in the binlog before a plain read sees it, S-MY11), then read its committed value.
//! A lock wait past `innodb_lock_wait_timeout` fails with error 1205, which is transient: the chunk
//! is read again after a backoff.
//!
//! The key is ordered and compared the way its index orders it: a text column in its own
//! collation (a primary key is unique under it, so the order is total), an `ENUM` by member index,
//! a `SET` by bitmap, a `BIT` as a number. The cursor's values travel as parameters in that form.
//! The keyset predicate is spelled out (`a > ? OR (a = ? AND b > ?)`): a row-constructor
//! comparison `(a, b) > (?, ?)` walks the index from its start on MySQL 8.4 and MariaDB 10.11 and
//! 11.8, where the expanded form is a range scan (measured with EXPLAIN, 2026-10-14).
//!
//! Rows come from a prepared statement and decode like binlog values (one plan, two inputs), so a
//! snapshot row and a streamed row of the same data are equal. A chunk carries no
//! [`data_avalanche_core::ChunkVisibility`]: the pipeline orders it against the stream by the
//! window rule (Data-Flow, snapshot step c).

use chrono::{Datelike, Timelike};
use data_avalanche_core::{
    Chunk, ColumnValue, HaltReason, KeysetCursor, Row, SourceError, TableRef, TableSchema,
    identifier_values,
};
use mysql_async::prelude::Queryable;
use mysql_async::{IsolationLevel, TxOpts, Value};

use crate::catalog::table_key;
use crate::connector::MySqlConnector;
use crate::types::{CatalogColumn, plan_column};
use crate::value::{self, Decode, Input, ValueError};

/// How to read one column of a table.
#[derive(Debug, Clone)]
pub(crate) struct ReadColumn {
    catalog: CatalogColumn,
    decode: Decode,
    /// One of the table's identifier columns.
    identifier: bool,
}

/// A name as a MySQL identifier.
pub(crate) fn ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `db`.`table`.
pub(crate) fn qualified(t: &TableRef) -> String {
    format!("{}.{}", ident(&t.schema), ident(&t.name))
}

fn halt(schema: &TableSchema, reason: HaltReason) -> SourceError {
    SourceError::TableHalt {
        table: schema.table().clone(),
        reason,
    }
}

/// The table's columns differ from `schema`'s: transient, since the stream's schema signal hasn't
/// caught up with the catalog yet.
fn changed(schema: &TableSchema, detail: String) -> SourceError {
    SourceError::Transient(format!(
        "{} changed under the snapshot: {detail}; read again once the stream's schema caught up",
        table_key(schema.table())
    ))
}

/// The read plan of a table, checked against `schema`: the table is still there, and every column
/// still exists and decodes to the type the schema says. A table gone halts it; a key column that
/// became nullable halts it (a NULL key sorts before every cursor, so its rows would be skipped).
/// Another difference is transient: the stream's schema signal hasn't caught up yet.
async fn read_plan(
    c: &MySqlConnector,
    schema: &TableSchema,
) -> Result<Vec<ReadColumn>, SourceError> {
    let name = schema.table().name.clone();
    let Some(d) = c.describe(std::slice::from_ref(&name)).await?.remove(&name) else {
        return Err(halt(schema, HaltReason::TableDropped));
    };
    let tables = &c.pipeline.tables;
    let key = table_key(schema.table());
    let mut out = Vec::with_capacity(schema.columns().len());
    for col in schema.columns() {
        let catalog = d
            .columns
            .iter()
            .find(|x| x.name == col.name)
            .ok_or_else(|| changed(schema, format!("column {} is gone", col.name)))?;
        let identifier = schema.is_identifier(&col.name);
        if identifier && catalog.nullable {
            return Err(halt(
                schema,
                HaltReason::IdentifierNotUsable {
                    column: col.name.clone(),
                    reason: "it is nullable now".into(),
                },
            ));
        }
        let over = tables
            .column_type_overrides
            .get(&format!("{key}.{}", col.name));
        let p = plan_column(
            catalog,
            c.tinyint1_as_bool(),
            tables.numeric_overflow_action,
            over,
            identifier,
        )
        .map_err(|h| halt(schema, h))?;
        if p.value_type != col.value_type {
            return Err(changed(
                schema,
                format!(
                    "column {} is now {:?}, the schema has {:?}",
                    col.name, p.value_type, col.value_type
                ),
            ));
        }
        out.push(ReadColumn {
            catalog: catalog.clone(),
            decode: p.decode,
            identifier,
        });
    }
    Ok(out)
}

/// The decode an override wraps: the column's own type, which orders the key.
fn base(d: &Decode) -> &Decode {
    match d {
        Decode::String(inner) | Decode::Convert { from: inner, .. } => base(inner),
        d => d,
    }
}

/// An integer key value: a decoded integer, a decimal an override widened it to, or its text.
fn integer_of(v: &ColumnValue) -> Option<i128> {
    match v {
        ColumnValue::Bool(b) => Some(i128::from(*b)),
        ColumnValue::Int16(n) => Some(i128::from(*n)),
        ColumnValue::Int32(n) => Some(i128::from(*n)),
        ColumnValue::Int64(n) => Some(i128::from(*n)),
        ColumnValue::UInt64(n) => Some(i128::from(*n)),
        ColumnValue::Numeric(d) => {
            let text = d.to_string();
            let (whole, fraction) = text.split_once('.').unwrap_or((&text, ""));
            fraction
                .bytes()
                .all(|b| b == b'0')
                .then(|| whole.parse().ok())
                .flatten()
        }
        ColumnValue::Text(t) => t.parse().ok(),
        _ => None,
    }
}

fn integer_param(n: i128) -> Value {
    match u64::try_from(n) {
        Ok(u) => Value::UInt(u),
        Err(_) => Value::Int(n as i64),
    }
}

/// A date and time as a parameter. `micros` counts from 1970-01-01 00:00.
fn clock_param(micros: i64) -> Option<Value> {
    let t = chrono::DateTime::from_timestamp_micros(micros)?.naive_utc();
    Some(Value::Date(
        u16::try_from(t.year()).ok()?,
        t.month() as u8,
        t.day() as u8,
        t.hour() as u8,
        t.minute() as u8,
        t.second() as u8,
        t.nanosecond() / 1000,
    ))
}

fn time_param(micros: i64) -> Value {
    let neg = micros < 0;
    let m = micros.unsigned_abs();
    let (secs, us) = (m / 1_000_000, (m % 1_000_000) as u32);
    let hours = secs / 3600;
    Value::Time(
        neg,
        (hours / 24) as u32,
        (hours % 24) as u8,
        (secs / 60 % 60) as u8,
        (secs % 60) as u8,
        us,
    )
}

fn text_param(t: &str) -> Value {
    Value::Bytes(t.as_bytes().to_vec())
}

/// A key column's comparison operand: the SQL that reads the parameter in the column's type and
/// order, and the parameter, from the cursor's value `v`.
fn key_operand(c: &ReadColumn, v: &ColumnValue) -> Result<(String, Value), String> {
    let cat = &c.catalog;
    let unusable = || format!("{}: no key parameter from {v:?}", cat.name);
    let plain = |p: Value| Ok(("?".to_owned(), p));
    match (base(&c.decode), v) {
        (Decode::Bool | Decode::Int { .. } | Decode::Year, v) => {
            plain(integer_param(integer_of(v).ok_or_else(unusable)?))
        }
        // A float or SET key is an identifier only under a `string` override (core refuses
        // float equality and arrays); its text is Rust's shortest form, which parses back exactly.
        (Decode::Float32, ColumnValue::Text(t)) => {
            plain(Value::Float(t.parse().map_err(|_| unusable())?))
        }
        (Decode::Float64, ColumnValue::Text(t)) => {
            plain(Value::Double(t.parse().map_err(|_| unusable())?))
        }
        // The text parameter, cast to the column's decimal: MySQL 8.4 and MariaDB 11.8 compare
        // it exactly uncast too (measured), but a string against a decimal compares as a double
        // by the documented rules.
        (Decode::Decimal | Decode::DecimalText, ColumnValue::Numeric(_) | ColumnValue::Text(_)) => {
            let text = match v {
                ColumnValue::Numeric(d) => d.to_string(),
                ColumnValue::Text(t) => t.clone(),
                _ => unreachable!("matched above"),
            };
            let (p, s) = (cat.precision.unwrap_or(65), cat.scale.unwrap_or(30));
            Ok((format!("CAST(? AS DECIMAL({p},{s}))"), text_param(&text)))
        }
        (Decode::Text { .. }, ColumnValue::Text(t)) => {
            let sql = match (&cat.charset, &cat.collation) {
                (Some(cs), Some(co)) => format!("CONVERT(? USING {cs}) COLLATE {co}"),
                _ => "?".to_owned(),
            };
            Ok((sql, text_param(t)))
        }
        (Decode::Bytes { .. }, ColumnValue::Bytes(b)) if cat.data_type == "bit" => plain(
            Value::UInt(b.iter().fold(0u64, |n, byte| n << 8 | u64::from(*byte))),
        ),
        // Bytes travel as hex: a parameter is a string in the connection's character set.
        (Decode::Bytes { .. }, ColumnValue::Bytes(b)) => {
            let hex: String = b.iter().map(|x| format!("{x:02X}")).collect();
            Ok(("UNHEX(?)".to_owned(), text_param(&hex)))
        }
        (Decode::Date, ColumnValue::Date(days)) => {
            plain(clock_param(i64::from(*days) * 86_400_000_000).ok_or_else(unusable)?)
        }
        (
            Decode::Date | Decode::DateTime { .. } | Decode::Timestamp { .. },
            ColumnValue::Timestamp { micros, .. },
        ) => plain(clock_param(*micros).ok_or_else(unusable)?),
        (Decode::Time { .. }, ColumnValue::Time(micros)) => plain(time_param(*micros)),
        (
            Decode::Date
            | Decode::DateTime { .. }
            | Decode::Timestamp { .. }
            | Decode::Time { .. }
            | Decode::Uuid
            | Decode::Inet4
            | Decode::Inet6,
            ColumnValue::Text(t),
        ) => plain(text_param(t)),
        (Decode::Uuid, ColumnValue::Uuid(u)) => plain(text_param(&u.hyphenated().to_string())),
        // ENUM and SET order by member index and bitmap, and compare that way with a number.
        (Decode::Enum { members }, ColumnValue::Text(label)) => {
            let index = members
                .iter()
                .position(|m| m == label)
                .map_or(0, |i| i as u64 + 1);
            if index == 0 && !label.is_empty() {
                return Err(unusable());
            }
            plain(Value::UInt(index))
        }
        (Decode::Set { members }, ColumnValue::Text(t)) => {
            let mut bits = 0u64;
            for l in t.split(',').filter(|l| !l.is_empty()) {
                let i = members.iter().position(|m| m == l).ok_or_else(unusable)?;
                bits |= 1 << i;
            }
            plain(Value::UInt(bits))
        }
        _ => Err(unusable()),
    }
}

/// The keyset predicate after the cursor, spelled out: `k1 > ?1 OR (k1 = ?1 AND k2 > ?2) …`, with
/// its parameters in order.
fn keyset_predicate(keys: &[(String, String, Value)]) -> (String, Vec<Value>) {
    let mut terms = vec![];
    let mut params = vec![];
    for i in 0..keys.len() {
        let mut parts = vec![];
        for (column, operand, param) in &keys[..i] {
            parts.push(format!("{column} = {operand}"));
            params.push(param.clone());
        }
        let (column, operand, param) = &keys[i];
        parts.push(format!("{column} > {operand}"));
        params.push(param.clone());
        terms.push(format!("({})", parts.join(" AND ")));
    }
    (terms.join(" OR "), params)
}

/// The key's collations, for the cursor: one name per text identifier column, in order.
fn cursor_collation(keys: &[&ReadColumn]) -> Option<String> {
    let names: Vec<&str> = keys
        .iter()
        .filter(|c| matches!(base(&c.decode), Decode::Text { .. }))
        .filter_map(|c| c.catalog.collation.as_deref())
        .collect();
    (!names.is_empty()).then(|| names.join(","))
}

/// The identifier's read columns, in identifier order.
fn key_columns<'a>(schema: &TableSchema, columns: &'a [ReadColumn]) -> Vec<&'a ReadColumn> {
    schema
        .identifier()
        .iter()
        .map(|k| {
            columns
                .iter()
                .find(|c| &c.catalog.name == k)
                .expect("the identifier is a column")
        })
        .collect()
}

fn decode_row(
    schema: &TableSchema,
    columns: &[ReadColumn],
    row: mysql_async::Row,
) -> Result<Row, SourceError> {
    let values = row.unwrap();
    let mut out = Vec::with_capacity(columns.len());
    for (c, v) in columns.iter().zip(&values) {
        let column = c.catalog.name.clone();
        let decoded = value::decode(&c.decode, Input::Row(v), c.identifier).map_err(|e| {
            halt(
                schema,
                match e {
                    ValueError::Overflow => HaltReason::NumericOverflow { column },
                    ValueError::InvalidDate { raw } => HaltReason::InvalidDateValue { column, raw },
                    ValueError::Unrepresentable(detail) => {
                        HaltReason::SchemaMismatch { column, detail }
                    }
                },
            )
        })?;
        out.push((c.catalog.name.clone(), decoded));
    }
    Ok(Row(out))
}

/// The first identifier column `row` holds NULL in.
fn null_key(schema: &TableSchema, row: &Row) -> String {
    schema
        .identifier()
        .iter()
        .find(|k| matches!(row.get(k), None | Some(ColumnValue::Null)))
        .cloned()
        .unwrap_or_default()
}

/// Server errors of a table or column gone between the plan's catalog read and the query.
const NO_TABLE: u16 = 1146;
const NO_COLUMN: u16 = 1054;

impl MySqlConnector {
    /// One chunk: `SELECT … WHERE <after the cursor> ORDER BY key LIMIT n LOCK IN SHARE MODE` in
    /// a READ COMMITTED transaction that commits at once (DA-SNAP-10).
    pub async fn read_chunk(
        &self,
        table: &TableSchema,
        after: Option<&KeysetCursor>,
        limit: usize,
    ) -> Result<Chunk, SourceError> {
        let columns = read_plan(self, table).await?;
        let keys = key_columns(table, &columns);
        let collation = cursor_collation(&keys);
        let list: Vec<String> = columns.iter().map(|c| ident(&c.catalog.name)).collect();
        let mut sql = format!(
            "SELECT {} FROM {}",
            list.join(", "),
            qualified(table.table())
        );
        let mut params = vec![];
        if let Some(cursor) = after {
            if cursor.collation != collation {
                return Err(halt(
                    table,
                    HaltReason::SchemaMismatch {
                        column: table.identifier().join(","),
                        detail: format!(
                            "the key's collation is {collation:?}, the snapshot's cursor was \
                             ordered by {:?}; re-snapshot the table",
                            cursor.collation
                        ),
                    },
                ));
            }
            let operands = keys
                .iter()
                .map(|c| {
                    let v = cursor.key.get(&c.catalog.name).ok_or_else(|| {
                        format!("{}: the cursor has no {}", table.table(), c.catalog.name)
                    })?;
                    let (operand, param) = key_operand(c, v)?;
                    Ok((ident(&c.catalog.name), operand, param))
                })
                .collect::<Result<Vec<_>, String>>()
                .map_err(SourceError::Permanent)?;
            let (predicate, p) = keyset_predicate(&operands);
            sql += &format!(" WHERE {predicate}");
            params = p;
        }
        let order: Vec<String> = keys.iter().map(|c| ident(&c.catalog.name)).collect();
        sql += &format!(
            " ORDER BY {} LIMIT {limit} LOCK IN SHARE MODE",
            order.join(", ")
        );
        let read = self
            .conn
            .command("read a snapshot chunk", move |c| {
                Box::pin(async move {
                    let mut opts = TxOpts::new();
                    opts.with_isolation_level(IsolationLevel::ReadCommitted);
                    let mut tx = c.start_transaction(opts).await?;
                    match tx.exec::<mysql_async::Row, _, _>(sql, params).await {
                        Ok(rows) => {
                            tx.commit().await?;
                            Ok(Ok(rows))
                        }
                        Err(mysql_async::Error::Server(e))
                            if matches!(e.code, NO_TABLE | NO_COLUMN) =>
                        {
                            tx.rollback().await?;
                            Ok(Err(e))
                        }
                        Err(e) => Err(e),
                    }
                })
            })
            .await?;
        let rows = match read {
            Ok(rows) => rows,
            Err(e) if e.code == NO_TABLE => return Err(halt(table, HaltReason::TableDropped)),
            Err(e) => return Err(changed(table, e.message)),
        };
        let rows = rows
            .into_iter()
            .map(|r| decode_row(table, &columns, r))
            .collect::<Result<Vec<_>, _>>()?;
        let next = match rows.last() {
            None => None,
            Some(last) => Some(KeysetCursor {
                // A key column nullable now halts in `read_plan`; this is the backstop.
                key: identifier_values(table, last).ok_or_else(|| {
                    halt(
                        table,
                        HaltReason::NullIdentifier {
                            column: null_key(table, last),
                        },
                    )
                })?,
                collation,
            }),
        };
        Ok(Chunk {
            rows,
            next,
            visibility: None,
        })
    }
}

```

Change `crates/data-avalanche-source-mysql/src/types.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/types.rs b/crates/data-avalanche-source-mysql/src/types.rs
--- a/crates/data-avalanche-source-mysql/src/types.rs
+++ b/crates/data-avalanche-source-mysql/src/types.rs
@@ -20,6 +20,8 @@ pub struct CatalogColumn {
     pub column_type: String,
     /// `CHARACTER_SET_NAME`, for character columns.
     pub charset: Option<String>,
+    /// `COLLATION_NAME`, for character columns: the order a snapshot reads a text key in.
+    pub collation: Option<String>,
     /// `NUMERIC_PRECISION` / `NUMERIC_SCALE`, for decimals.
     pub precision: Option<u32>,
     pub scale: Option<u32>,
```

Change `crates/data-avalanche-source-mysql/src/value.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/value.rs b/crates/data-avalanche-source-mysql/src/value.rs
--- a/crates/data-avalanche-source-mysql/src/value.rs
+++ b/crates/data-avalanche-source-mysql/src/value.rs
@@ -557,7 +557,17 @@ fn value(d: &Decode, raw: Raw, identifier: bool) -> Result<ColumnValue, ValueErr
                 },
             }
         }
-        Decode::Bool => V::Bool(raw != Raw::Int(0)),
+        // A key value other than 0 or 1 halts the table: as a boolean, two keys would be one
+        // (as a key value that clamps, Q-M5-18).
+        Decode::Bool => match raw {
+            Raw::Int(n) if identifier && n != 0 && n != 1 => {
+                return Err(bad(format!(
+                    "a TINYINT(1) key value {n} isn't 0 or 1, and two keys would be one boolean; \
+                     turn tinyint1_as_bool off or override the column"
+                )));
+            }
+            raw => V::Bool(raw != Raw::Int(0)),
+        },
         Decode::Int { bits, unsigned } => {
             let Raw::Int(n) = raw else {
                 return Err(bad(format!("an integer read {raw:?}")));
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 52 tests.

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql snapshot:: || break; done`

Expected: PASS, 3 tests on each target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/src/catalog.rs \
  crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/snapshot.rs \
  crates/data-avalanche-source-mysql/src/types.rs \
  crates/data-avalanche-source-mysql/src/value.rs \
  crates/data-avalanche-source-mysql/tests/mysql/main.rs \
  crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs
git commit -m "feat(source-mysql): snapshot chunks — locking keyset reads in the key's own order"
```

---

### Task 11: The DDL classifier

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/data-avalanche-source-mysql/Cargo.toml`
- Create: `crates/data-avalanche-source-mysql/src/ddl.rs`
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`

**Interfaces:**
- Consumes:
  - Nothing from earlier tasks: pure functions over the statement text.
- Produces:
  - `data-avalanche-source-mysql/src/ddl.rs`:
    - `pub enum Scope`
    - `pub struct Altered`
    - `pub struct Ddl`
    - `pub fn classify(sql: &str, db: &str) -> Ddl`
    - `pub enum Renamed`
    - `pub fn shadow_of(shadow: &TableRef, t: &TableRef) -> bool`
    - `pub fn renamed(renames: &[(TableRef, TableRef)], tracked: &[TableRef]) -> Vec<(TableRef, Renamed)>`
  - `data-avalanche-source-mysql/src/lib.rs`:
    - `pub mod ddl;`

Channel B for MySQL (`MySQL_DDL_Classifier`, `Schema_Change…` §2.2 and §7.2; D-M7a.15). A
statement in a `QUERY_EVENT` → the tracked tables it touches and how.

- **Tier 2, the tokenizer, always**: it reads every verb and table name, skips comments, and
  keeps the text of executable comments (`/*! … */`, `/*M! … */`). ALTER (with `ONLINE`,
  `IGNORE`, `IF EXISTS`), CREATE (`OR REPLACE` drops a tracked table), CREATE and DROP INDEX,
  DROP TABLE lists, DROP DATABASE, RENAME TABLE pairs, TRUNCATE, `ALTER … RENAME TO`.
- **Tier 1, `sqlparser` 0.62 `MySqlDialect`, for ALTER**: the columns an ALTER touches
  (`Scope::Columns`) and the renames it proves (`CHANGE a b`, `RENAME COLUMN a TO b`). A
  statement it can't read, or one with an executable comment, scopes the whole table
  (`Scope::Table`). The tiers run in the other order from the research doc's diagram (names
  always by the tokenizer); the outcomes are the same, and the doc is updated.
- **Unlogged row changes halt** (`UNSAFE_DDL`): `DROP`, `TRUNCATE`, `EXCHANGE`, `IMPORT`,
  `DISCARD` or `CONVERT` of a `PARTITION`, `TABLESPACE` or `TABLE` change rows the binlog
  carries no events for.
- **The cut-over swap** (Q-M7a-8, Q-M7a-9): one `RENAME TABLE t TO x, s TO t` statement where
  `s` is an untracked table named as gh-ost (`_t_gho`) or pt-online-schema-change (`_t_new`)
  names it is followed (`Renamed::Swapped`). Any other rename of a tracked table away is `Gone`
  (`TABLE_DROPPED`).

`sqlparser = "=0.62.0"` without default features: its `recursive-protection` feature builds
`stacker` (C). The parser's recursion limit (50) holds without it (a test parses 10,000 nested
parentheses). The servers' own binlog texts were measured on MySQL 8.4, MariaDB 11.8 and 10.11:
DROP TABLE is rewritten as `` DROP TABLE `a`,`b` /* generated by server */ ``, temporary tables
aren't logged, `CREATE … SELECT` logs the CREATE and then rows.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -38,10 +38,12 @@ tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
 object_store = { version = "0.14.2", features = ["aws", "gcp", "azure"] }
 tokio-postgres = "0.7"
 mysql_async = { version = "=0.37.1", default-features = false, features = ["default-rustls", "binlog"] }
-# The MySQL source (roadmap §2.1): the binlog events mysql_async reads, and zlib for MariaDB's
-# compressed events (Q-M7a-5).
+# The MySQL source (roadmap §2.1): the binlog events mysql_async reads, zlib for MariaDB's
+# compressed events (Q-M7a-5), and the DDL classifier's parser (MySQL_DDL_Classifier §5; without
+# `recursive-protection`, whose stack growth builds C code: the parser's recursion limit bounds it).
 mysql_common = { version = "=0.37.3", default-features = false }
 flate2 = "1"
+sqlparser = { version = "=0.62.0", default-features = false, features = ["std"] }
 rustls = { version = "0.23", features = ["aws_lc_rs"] }
 serde-saphyr = { version = "=1.3.0", default-features = false, features = ["deserialize"] }
 humantime = "2.4"
```

Change `crates/data-avalanche-source-mysql/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-source-mysql/Cargo.toml b/crates/data-avalanche-source-mysql/Cargo.toml
--- a/crates/data-avalanche-source-mysql/Cargo.toml
+++ b/crates/data-avalanche-source-mysql/Cargo.toml
@@ -18,6 +18,7 @@ futures-util.workspace = true
 mysql_async.workspace = true
 rustls.workspace = true
 serde_json.workspace = true
+sqlparser.workspace = true
 thiserror.workspace = true
 tokio = { workspace = true, features = ["sync", "time"] }
 uuid.workspace = true
```

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -8,6 +8,7 @@ pub mod binlog;
 pub mod catalog;
 mod connect;
 mod connector;
+pub mod ddl;
 mod error;
 pub mod json;
 pub mod position;
@@ -20,6 +21,7 @@ pub use binlog::{MariaGtidEvent, Offsets, decompress, rotate_name};
 pub use catalog::{Described, Listed, Notes, Planned, Selection, plan_table, select, table_key};
 pub use connect::SESSION_SETUP;
 pub use connector::MySqlConnector;
+pub use ddl::{Altered, Ddl, Renamed, Scope, classify, renamed, shadow_of};
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
 pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
 pub use types::{CatalogColumn, ColumnPlan, members, plan_column};
```

Create `crates/data-avalanche-source-mysql/src/ddl.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn t(name: &str) -> TableRef {
        TableRef::new("shop", name)
    }

    fn c(sql: &str) -> Ddl {
        classify(sql, "shop")
    }

    fn altered(sql: &str) -> Altered {
        let d = c(sql);
        assert_eq!(d.altered.len(), 1, "{sql}: {d:?}");
        d.altered[0].clone()
    }

    fn columns(names: &[&str]) -> Scope {
        Scope::Columns(names.iter().map(|n| n.to_string()).collect())
    }

    #[test]
    fn alter_operations_scope_the_columns_and_prove_renames() {
        let a = altered(
            "ALTER TABLE `shop`.`orders` ADD COLUMN `x` INT NOT NULL DEFAULT 0 AFTER id, DROP COLUMN y",
        );
        assert_eq!(a.table, t("orders"));
        assert_eq!(a.scope, columns(&["x", "y"]));
        assert_eq!(
            altered("ALTER TABLE orders CHANGE COLUMN a b BIGINT").renames,
            [("a".into(), "b".into())]
        );
        assert_eq!(
            altered("alter table orders rename column a to b").renames,
            [("a".into(), "b".into())]
        );
        let modify = altered("ALTER TABLE orders CHANGE a a VARCHAR(10)");
        assert_eq!(
            (modify.scope, modify.renames),
            (columns(&["a", "a"]), vec![])
        );
        assert_eq!(
            altered("ALTER TABLE orders MODIFY COLUMN a BIGINT UNSIGNED").scope,
            columns(&["a"])
        );
        assert_eq!(
            altered("ALTER TABLE orders ADD INDEX i (a), ALGORITHM=INSTANT").scope,
            columns(&[])
        );
        assert_eq!(
            altered("ALTER TABLE orders DROP PRIMARY KEY, ADD PRIMARY KEY (a, b)").scope,
            columns(&[])
        );
        assert_eq!(
            altered("ALTER TABLE orders ADD COLUMN `weird name` INT, ADD COLUMN `ba``ck` INT")
                .scope,
            columns(&["weird name", "ba`ck"])
        );
    }

    #[test]
    fn what_sqlparser_cant_read_scopes_the_whole_table() {
        for sql in [
            "ALTER TABLE orders ENGINE=InnoDB",
            "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4",
            "ALTER TABLE orders ADD COLUMN (p INT, q INT)",
            "ALTER ONLINE TABLE orders ADD COLUMN x INT",
            "ALTER TABLE IF EXISTS orders ADD COLUMN IF NOT EXISTS x INT",
            "ALTER TABLE orders ADD COLUMN x INT /*!80023 INVISIBLE */",
        ] {
            let a = altered(sql);
            assert_eq!(
                (a.table, a.scope, a.renames),
                (t("orders"), Scope::Table, vec![]),
                "{sql}"
            );
        }
    }

    #[test]
    fn a_deeply_nested_statement_stops_at_the_recursion_limit() {
        let deep = format!(
            "ALTER TABLE orders ADD COLUMN x INT DEFAULT ({}1{})",
            "(".repeat(10_000),
            ")".repeat(10_000)
        );
        assert_eq!(altered(&deep).scope, Scope::Table);
    }

    #[test]
    fn table_statements_name_their_tables() {
        assert_eq!(
            c("DROP TABLE `orders` /* generated by server */").dropped,
            [t("orders")]
        );
        assert_eq!(
            c("DROP TABLE IF EXISTS `a`,`other`.`b` /* generated by server */").dropped,
            [t("a"), TableRef::new("other", "b")]
        );
        assert_eq!(c("DROP TEMPORARY TABLE IF EXISTS orders"), Ddl::default());
        assert_eq!(c("TRUNCATE TABLE orders").truncated, [t("orders")]);
        assert_eq!(c("truncate orders").truncated, [t("orders")]);
        assert_eq!(
            c("RENAME TABLE a TO b, other.c TO d").renamed,
            [(t("a"), t("b")), (TableRef::new("other", "c"), t("d"))]
        );
        assert_eq!(
            c("ALTER TABLE orders RENAME TO archive").renamed,
            [(t("orders"), t("archive"))]
        );
        assert_eq!(c("ALTER TABLE orders RENAME COLUMN a TO b").renamed, []);
        assert_eq!(
            c("CREATE INDEX i ON orders (a)").altered[0].table,
            t("orders")
        );
        assert_eq!(
            c("DROP INDEX `PRIMARY` ON orders").altered[0].scope,
            columns(&[])
        );
        assert_eq!(
            c("CREATE OR REPLACE TABLE orders (id INT PRIMARY KEY)").dropped,
            [t("orders")]
        );
        assert_eq!(
            c("CREATE TABLE IF NOT EXISTS orders LIKE x"),
            Ddl::default()
        );
        assert_eq!(
            c("DROP DATABASE IF EXISTS `shop`")
                .dropped_database
                .as_deref(),
            Some("shop")
        );
        assert_eq!(
            c("CREATE OR REPLACE DATABASE shop")
                .dropped_database
                .as_deref(),
            Some("shop")
        );
    }

    #[test]
    fn statements_on_no_table_route_nowhere() {
        for sql in [
            "BEGIN",
            "GRANT SELECT ON *.* TO 'u'@'%'",
            "CREATE DATABASE x",
            "CREATE DEFINER=`root`@`%` TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.a = 1",
            "CREATE VIEW v AS SELECT 1",
            "OPTIMIZE TABLE orders",
            "ANALYZE TABLE orders",
            "ALTER DATABASE shop CHARACTER SET utf8mb4",
            "DROP VIEW v",
            "FLUSH PRIVILEGES",
        ] {
            assert_eq!(c(sql), Ddl::default(), "{sql}");
        }
    }

    #[test]
    fn a_table_statement_without_a_name_is_unreadable() {
        for sql in [
            "ALTER TABLE",
            "TRUNCATE TABLE",
            "RENAME TABLE a TO",
            "DROP TABLE ,",
        ] {
            assert!(c(sql).unreadable, "{sql}");
        }
    }

    #[test]
    fn partition_operations_that_move_rows_are_unlogged() {
        for (sql, op) in [
            ("ALTER TABLE orders DROP PARTITION p0", "DROP PARTITION"),
            (
                "ALTER TABLE orders TRUNCATE PARTITION p0, p1",
                "TRUNCATE PARTITION",
            ),
            (
                "ALTER TABLE orders EXCHANGE PARTITION p0 WITH TABLE x",
                "EXCHANGE PARTITION",
            ),
            (
                "ALTER TABLE orders DISCARD TABLESPACE",
                "DISCARD TABLESPACE",
            ),
            (
                "ALTER TABLE orders IMPORT PARTITION p0 TABLESPACE",
                "IMPORT PARTITION",
            ),
            (
                "ALTER TABLE orders CONVERT PARTITION p0 TO TABLE x",
                "CONVERT PARTITION",
            ),
            (
                "ALTER TABLE orders CONVERT TABLE x TO PARTITION p1 VALUES LESS THAN (10)",
                "CONVERT TABLE",
            ),
        ] {
            assert_eq!(c(sql).unlogged, [(t("orders"), op.to_owned())], "{sql}");
        }
        for sql in [
            "ALTER TABLE orders ADD PARTITION (PARTITION p9 VALUES LESS THAN (99))",
            "ALTER TABLE orders PARTITION BY HASH(id) PARTITIONS 4",
            "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4",
            "ALTER TABLE orders ADD COLUMN `drop` INT, ADD COLUMN `partition` INT",
            "ALTER TABLE orders COMMENT 'drop partition'",
        ] {
            assert_eq!(c(sql).unlogged, [], "{sql}");
        }
    }

    #[test]
    fn comments_and_strings_hide_their_words() {
        assert_eq!(
            tokens("/* DROP TABLE x */ -- RENAME\n# ALTER\nTRUNCATE 'a''b' \"c\\\"d\" `e`"),
            [
                Token::Word("TRUNCATE".into()),
                Token::Str,
                Token::Str,
                Token::Quoted("e".into())
            ]
        );
        assert_eq!(
            tokens("ALTER /*!50100 TABLE */ t"),
            [
                Token::Word("ALTER".into()),
                Token::Word("TABLE".into()),
                Token::Word("t".into())
            ]
        );
        assert_eq!(c("/*M!100100 DROP TABLE */ orders").dropped, [t("orders")]);
    }

    #[test]
    fn a_cut_over_swap_follows_the_shadow_table() {
        let tracked = [t("orders"), t("items")];
        let swap = |sql: &str| renamed(&c(sql).renamed, &tracked);
        assert_eq!(
            swap("RENAME TABLE orders TO _orders_del, _orders_gho TO orders"),
            [(
                t("orders"),
                Renamed::Swapped {
                    from: t("_orders_gho")
                }
            )]
        );
        assert_eq!(
            swap("RENAME TABLE `orders` TO `_orders_old`, `__orders_new` TO `orders`"),
            [(
                t("orders"),
                Renamed::Swapped {
                    from: t("__orders_new")
                }
            )]
        );
        // Another table into the name, a table away, two tracked tables swapped.
        assert_eq!(
            swap("RENAME TABLE orders TO orders_old, archive TO orders"),
            [(t("orders"), Renamed::Gone)]
        );
        assert_eq!(
            swap("RENAME TABLE orders TO o2"),
            [(t("orders"), Renamed::Gone)]
        );
        assert_eq!(
            swap("RENAME TABLE orders TO tmp, items TO orders, tmp TO items"),
            [(t("orders"), Renamed::Gone), (t("items"), Renamed::Gone)]
        );
        // Away and back is the same table; an untracked rename touches nothing.
        assert_eq!(
            swap("RENAME TABLE orders TO tmp, tmp TO orders"),
            [(t("orders"), Renamed::Kept)]
        );
        assert_eq!(swap("RENAME TABLE a TO b"), []);
        // The shadow of another table, or in another database, isn't followed.
        assert_eq!(
            swap("RENAME TABLE orders TO x, _items_gho TO orders"),
            [(t("orders"), Renamed::Gone)]
        );
        assert!(!shadow_of(
            &TableRef::new("other", "_orders_gho"),
            &t("orders")
        ));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `ddl::Altered`, `ddl::Ddl`, `ddl::Renamed`, `ddl::Scope`, `ddl::classify`, `ddl::renamed`, `ddl::shadow_of`
error[E0425]: cannot find type `TableRef` in this scope
error[E0425]: cannot find type `Ddl` in this scope
error[E0425]: cannot find type `Altered` in this scope
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-source-mysql/src/ddl.rs`, add above the test module:

```rust
//! The DDL classifier (channel B, `MySQL_DDL_Classifier.md`): what a `QUERY_EVENT` statement does
//! to which tables. It routes; channel C reads what changed from the catalog. Two tiers, fail-soft
//! (D-19a): a tokenizer, aware of quotes and comments, reads every statement's verb and tables;
//! `sqlparser` (`MySqlDialect`) reads an `ALTER TABLE`'s operations, which scope channel C to the
//! columns the statement names (`Schema_Change…` §0) and prove its column renames (§6). When
//! `sqlparser` can't read a statement (`ENGINE=`, `CONVERT TO CHARACTER SET`, MariaDB's
//! `IF EXISTS`, measured on 0.62), the scope is the whole table and no rename is proven.
//!
//! A partition or tablespace operation that removes or replaces rows (`DROP`, `TRUNCATE`,
//! `EXCHANGE`, `IMPORT`, `DISCARD` or MariaDB `CONVERT` of a partition) changes rows without row
//! events: the table halts (`UNSAFE_DDL`), never diverges silently.

use data_avalanche_core::TableRef;
use sqlparser::ast::{AlterTableOperation, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// The columns a table's change may touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// The columns the statement names; empty for an index, key or option change.
    Columns(Vec<String>),
    /// Any column: the statement's operations couldn't be read.
    Table,
}

/// A table whose shape the statement may change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Altered {
    pub table: TableRef,
    pub scope: Scope,
    /// Column renames the statement proves (`RENAME COLUMN`, `CHANGE old new`): `(old, new)`.
    pub renames: Vec<(String, String)>,
}

/// What one statement does to tables. Names are qualified with the event's default database.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Ddl {
    /// `ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`.
    pub altered: Vec<Altered>,
    /// `RENAME TABLE` pairs and `ALTER TABLE … RENAME TO`, in statement order.
    pub renamed: Vec<(TableRef, TableRef)>,
    /// `DROP TABLE`; MariaDB `CREATE OR REPLACE TABLE`, which drops the table it replaces.
    pub dropped: Vec<TableRef>,
    /// `DROP DATABASE`, MariaDB `CREATE OR REPLACE DATABASE`.
    pub dropped_database: Option<String>,
    pub truncated: Vec<TableRef>,
    /// Tables whose rows the statement changes without row events, with the operation.
    pub unlogged: Vec<(TableRef, String)>,
    /// A table statement neither tier read a table name from.
    pub unreadable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// A bare word: a keyword or an unquoted name.
    Word(String),
    /// A backtick-quoted name, unquoted.
    Quoted(String),
    /// A string literal.
    Str,
    Punct(char),
}

/// The statement's tokens. Comments are skipped, except executable ones (`/*!80023 … */`,
/// MariaDB `/*M!… */`), whose text the server runs.
fn tokens(sql: &str) -> Vec<Token> {
    let b: Vec<char> = sql.chars().collect();
    let mut out = vec![];
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let next = b.get(i + 1).copied();
        match c {
            _ if c.is_whitespace() => i += 1,
            '#' => {
                while i < b.len() && b[i] != '\n' {
                    i += 1;
                }
            }
            '-' if next == Some('-') && b.get(i + 2).is_none_or(|c| c.is_whitespace()) => {
                while i < b.len() && b[i] != '\n' {
                    i += 1;
                }
            }
            '/' if next == Some('*') => {
                let executable = b.get(i + 2) == Some(&'!')
                    || (b.get(i + 2) == Some(&'M') && b.get(i + 3) == Some(&'!'));
                if executable {
                    // Skip the marker and its version digits; the closing `*/` is skipped below.
                    i += if b[i + 2] == '!' { 3 } else { 4 };
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                } else {
                    i += 2;
                    while i < b.len() && !(b[i] == '*' && b.get(i + 1) == Some(&'/')) {
                        i += 1;
                    }
                    i += 2;
                }
            }
            '*' if next == Some('/') => i += 2,
            '`' => {
                let mut name = String::new();
                i += 1;
                while i < b.len() {
                    if b[i] == '`' {
                        if b.get(i + 1) == Some(&'`') {
                            name.push('`');
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    name.push(b[i]);
                    i += 1;
                }
                i += 1;
                out.push(Token::Quoted(name));
            }
            '\'' | '"' => {
                i += 1;
                while i < b.len() {
                    match b[i] {
                        '\\' => i += 2,
                        q if q == c && b.get(i + 1) == Some(&c) => i += 2,
                        q if q == c => break,
                        _ => i += 1,
                    }
                }
                i += 1;
                out.push(Token::Str);
            }
            _ if c.is_alphanumeric() || c == '_' || c == '$' => {
                let start = i;
                while i < b.len() && (b[i].is_alphanumeric() || b[i] == '_' || b[i] == '$') {
                    i += 1;
                }
                out.push(Token::Word(b[start..i].iter().collect()));
            }
            _ => {
                out.push(Token::Punct(c));
                i += 1;
            }
        }
    }
    out
}

/// A cursor over the tokens.
struct Words<'a> {
    t: &'a [Token],
    i: usize,
    db: &'a str,
}

impl Words<'_> {
    /// The next token is the keyword `k` (case-insensitive): consumes it.
    fn eat(&mut self, k: &str) -> bool {
        match self.t.get(self.i) {
            Some(Token::Word(w)) if w.eq_ignore_ascii_case(k) => {
                self.i += 1;
                true
            }
            _ => false,
        }
    }

    fn eat_all(&mut self, ks: &[&str]) -> bool {
        let at = self.i;
        if ks.iter().all(|k| self.eat(k)) {
            return true;
        }
        self.i = at;
        false
    }

    fn punct(&mut self, p: char) -> bool {
        if self.t.get(self.i) == Some(&Token::Punct(p)) {
            self.i += 1;
            return true;
        }
        false
    }

    fn ident(&mut self) -> Option<String> {
        let name = match self.t.get(self.i)? {
            Token::Word(w) | Token::Quoted(w) => w.clone(),
            _ => return None,
        };
        self.i += 1;
        Some(name)
    }

    /// `name` or `db.name`, qualified with the default database.
    fn table(&mut self) -> Option<TableRef> {
        let first = self.ident()?;
        if self.punct('.') {
            let name = self.ident()?;
            return Some(TableRef::new(first, name));
        }
        Some(TableRef::new(self.db, first))
    }

    /// `name {, name}`.
    fn tables(&mut self) -> Option<Vec<TableRef>> {
        let mut out = vec![self.table()?];
        while self.punct(',') {
            out.push(self.table()?);
        }
        Some(out)
    }

    /// Skips to the keyword `k` at any depth; consumes it.
    fn find(&mut self, k: &str) -> bool {
        while self.i < self.t.len() {
            if self.eat(k) {
                return true;
            }
            self.i += 1;
        }
        false
    }

    fn rest(&self) -> &[Token] {
        &self.t[self.i..]
    }
}

fn is_word(t: &Token, k: &str) -> bool {
    matches!(t, Token::Word(w) if w.eq_ignore_ascii_case(k))
}

/// Partition and tablespace operations that remove or replace rows without row events, as the
/// adjacent words that start them.
const UNLOGGED: [(&str, &str); 9] = [
    ("DROP", "PARTITION"),
    ("TRUNCATE", "PARTITION"),
    ("EXCHANGE", "PARTITION"),
    ("IMPORT", "PARTITION"),
    ("IMPORT", "TABLESPACE"),
    ("DISCARD", "PARTITION"),
    ("DISCARD", "TABLESPACE"),
    ("CONVERT", "PARTITION"),
    ("CONVERT", "TABLE"),
];

fn unlogged(rest: &[Token]) -> Option<String> {
    rest.windows(2).find_map(|w| {
        UNLOGGED
            .iter()
            .find(|(a, b)| is_word(&w[0], a) && is_word(&w[1], b))
            .map(|(a, b)| format!("{a} {b}"))
    })
}

/// `RENAME [TO | AS] name` among an `ALTER TABLE`'s operations (not `RENAME COLUMN`, `INDEX`
/// or `KEY`).
fn renamed_to(w: &mut Words<'_>) -> Option<TableRef> {
    while w.find("RENAME") {
        if w.eat("COLUMN") || w.eat("INDEX") || w.eat("KEY") {
            continue;
        }
        let _ = w.eat("TO") || w.eat("AS");
        return w.table();
    }
    None
}

/// Tier 2: the verb and the tables.
fn read(sql: &str, db: &str) -> Ddl {
    let t = tokens(sql);
    let mut w = Words { t: &t, i: 0, db };
    let mut d = Ddl::default();
    let unreadable = |mut d: Ddl| {
        d.unreadable = true;
        d
    };
    if w.eat("ALTER") {
        let _ = w.eat("ONLINE") || w.eat("OFFLINE");
        let _ = w.eat("IGNORE");
        if !w.eat("TABLE") {
            return d;
        }
        let _ = w.eat_all(&["IF", "EXISTS"]);
        let Some(table) = w.table() else {
            return unreadable(d);
        };
        if let Some(op) = unlogged(w.rest()) {
            d.unlogged.push((table.clone(), op));
        }
        let mut r = Words {
            t: &t,
            i: w.i,
            db: &table.schema,
        };
        if let Some(to) = renamed_to(&mut r) {
            d.renamed.push((table.clone(), to));
        }
        d.altered.push(Altered {
            table,
            scope: Scope::Table,
            renames: vec![],
        });
    } else if w.eat("CREATE") {
        let replace = w.eat_all(&["OR", "REPLACE"]);
        let temporary = w.eat("TEMPORARY");
        if w.eat("TABLE") {
            let _ = w.eat_all(&["IF", "NOT", "EXISTS"]);
            match w.table() {
                Some(t) if replace && !temporary => d.dropped.push(t),
                Some(_) => {}
                None => return unreadable(d),
            }
        } else if w.eat("DATABASE") || w.eat("SCHEMA") {
            let _ = w.eat_all(&["IF", "NOT", "EXISTS"]);
            if replace {
                d.dropped_database = w.ident();
            }
        } else {
            let _ = w.eat("ONLINE") || w.eat("OFFLINE");
            let _ = w.eat("UNIQUE") || w.eat("FULLTEXT") || w.eat("SPATIAL");
            if w.eat("INDEX") {
                if !w.find("ON") {
                    return unreadable(d);
                }
                let Some(table) = w.table() else {
                    return unreadable(d);
                };
                d.altered.push(Altered {
                    table,
                    scope: Scope::Columns(vec![]),
                    renames: vec![],
                });
            }
        }
    } else if w.eat("DROP") {
        let temporary = w.eat("TEMPORARY");
        if w.eat("TABLE") || w.eat("TABLES") {
            let _ = w.eat_all(&["IF", "EXISTS"]);
            match w.tables() {
                Some(tables) if !temporary => d.dropped = tables,
                Some(_) => {}
                None => return unreadable(d),
            }
        } else if w.eat("DATABASE") || w.eat("SCHEMA") {
            let _ = w.eat_all(&["IF", "EXISTS"]);
            d.dropped_database = w.ident();
        } else {
            let _ = w.eat("ONLINE") || w.eat("OFFLINE");
            if w.eat("INDEX") {
                if !w.find("ON") {
                    return unreadable(d);
                }
                let Some(table) = w.table() else {
                    return unreadable(d);
                };
                d.altered.push(Altered {
                    table,
                    scope: Scope::Columns(vec![]),
                    renames: vec![],
                });
            }
        }
    } else if w.eat("RENAME") {
        if !(w.eat("TABLE") || w.eat("TABLES")) {
            return d;
        }
        loop {
            let (Some(from), true, Some(to)) = (w.table(), w.eat("TO"), w.table()) else {
                return unreadable(d);
            };
            d.renamed.push((from, to));
            if !w.punct(',') {
                break;
            }
        }
    } else if w.eat("TRUNCATE") {
        let _ = w.eat("TABLE");
        match w.table() {
            Some(t) => d.truncated.push(t),
            None => return unreadable(d),
        }
    }
    d
}

/// Tier 1: `table`'s change as an `ALTER TABLE`'s operations say it; `None` when `sqlparser`
/// can't read them all.
fn operations(sql: &str, table: &TableRef) -> Option<Altered> {
    // `sqlparser` skips an executable comment, whose text the server runs.
    if sql.contains("/*!") || sql.contains("/*M!") {
        return None;
    }
    let stmts = Parser::parse_sql(&MySqlDialect {}, sql).ok()?;
    let [Statement::AlterTable(a)] = stmts.as_slice() else {
        return None;
    };
    let mut columns: Vec<String> = vec![];
    let mut renames = vec![];
    for op in &a.operations {
        use AlterTableOperation as O;
        match op {
            O::AddColumn { column_def, .. } => columns.push(column_def.name.value.clone()),
            O::DropColumn { column_names, .. } => {
                columns.extend(column_names.iter().map(|c| c.value.clone()))
            }
            O::ModifyColumn { col_name, .. } => columns.push(col_name.value.clone()),
            O::AlterColumn { column_name, .. } => columns.push(column_name.value.clone()),
            O::ChangeColumn {
                old_name, new_name, ..
            }
            | O::RenameColumn {
                old_column_name: old_name,
                new_column_name: new_name,
            } => {
                columns.extend([old_name.value.clone(), new_name.value.clone()]);
                if old_name.value != new_name.value {
                    renames.push((old_name.value.clone(), new_name.value.clone()));
                }
            }
            // Keys, indexes, constraints and options change no column; channel C reads the
            // identifier on every signal.
            O::AddConstraint { .. }
            | O::DropConstraint { .. }
            | O::DropPrimaryKey { .. }
            | O::DropForeignKey { .. }
            | O::DropIndex { .. }
            | O::RenameTable { .. }
            | O::Algorithm { .. }
            | O::Lock { .. }
            | O::AutoIncrement { .. } => {}
            _ => return None,
        }
    }
    Some(Altered {
        table: table.clone(),
        scope: Scope::Columns(columns),
        renames,
    })
}

/// Classifies one statement; `db` is the event's default database (`QUERY_EVENT.db`).
pub fn classify(sql: &str, db: &str) -> Ddl {
    let mut d = read(sql, db);
    if let [a] = d.altered.as_mut_slice()
        && a.scope == Scope::Table
        && let Some(read) = operations(sql, &a.table)
    {
        *a = read;
    }
    d
}

/// What became of a tracked table after a statement's renames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Renamed {
    /// The name holds the table it held.
    Kept,
    /// An online schema change cut-over moved the shadow table `from` into the name (gh-ost
    /// `_<t>_gho`, pt-online-schema-change `_<t>_new`): the stream follows it.
    Swapped { from: TableRef },
    /// The table left the name, or another table took it: the table halts (`TABLE_DROPPED`).
    Gone,
}

/// Whether `shadow` is an online schema change tool's shadow table of `t`: gh-ost `_t_gho`,
/// pt-online-schema-change `_t_new` (with more leading underscores when that name is taken).
pub fn shadow_of(shadow: &TableRef, t: &TableRef) -> bool {
    if shadow.schema != t.schema {
        return false;
    }
    let Some(rest) = shadow.name.strip_prefix('_') else {
        return false;
    };
    let rest = rest.trim_start_matches('_');
    [format!("{}_gho", t.name), format!("{}_new", t.name)].contains(&rest.to_owned())
}

/// Applies `renames` in order and says what each `tracked` table's name holds afterwards.
pub fn renamed(renames: &[(TableRef, TableRef)], tracked: &[TableRef]) -> Vec<(TableRef, Renamed)> {
    // Each name's occupant: the table that held the name before the statement.
    let mut at: Vec<(TableRef, TableRef)> = vec![];
    let occupant = |at: &[(TableRef, TableRef)], n: &TableRef| {
        at.iter()
            .find(|(name, _)| name == n)
            .map(|(_, o)| o.clone())
    };
    for (from, to) in renames {
        let moving = occupant(&at, from).unwrap_or_else(|| from.clone());
        at.retain(|(name, _)| name != from && name != to);
        // `from` is empty now unless something moves into it later.
        at.push((from.clone(), TableRef::new("", "")));
        at.push((to.clone(), moving));
    }
    tracked
        .iter()
        .filter_map(|t| {
            let o = occupant(&at, t)?;
            let outcome = if &o == t {
                Renamed::Kept
            } else if !o.name.is_empty() && !tracked.contains(&o) && shadow_of(&o, t) {
                Renamed::Swapped { from: o }
            } else {
                Renamed::Gone
            };
            Some((t.clone(), outcome))
        })
        .collect()
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 61 tests.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  Cargo.toml \
  crates/data-avalanche-source-mysql/Cargo.toml \
  crates/data-avalanche-source-mysql/src/ddl.rs \
  crates/data-avalanche-source-mysql/src/lib.rs
git commit -m "feat(source-mysql): the DDL classifier — tables and verbs by tokenizer, ALTER scope and renames by sqlparser, cut-over swaps"
```

---

### Task 12: Channels C and D: a statement's change, the rescan

**Files:**
- Modify: `crates/data-avalanche-source-mysql/src/catalog.rs`
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`
- Create: `crates/data-avalanche-source-mysql/src/schema.rs`
- Modify: `crates/data-avalanche-source-mysql/src/types.rs`
- Modify: `crates/data-avalanche-source-mysql/tests/mysql/main.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/mysql/resolve.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/schema.rs`

**Interfaces:**
- Consumes:
  - `classify`, `Scope`, `renamed` (Task 11); `describe`, `plan_table`, `CatalogColumn` (Task 9).
  - `data_avalanche_core::{SchemaDelta, SchemaChange, SchemaHint, RescanReport}`.
- Produces:
  - `data-avalanche-source-mysql/src/lib.rs`:
    - `pub mod schema;`
  - `data-avalanche-source-mysql/src/schema.rs`:
    - `pub fn scope_of(sql: &str, table: &TableRef) -> (Scope, Vec<(String, String)>)`
    - `pub fn narrowed(from: &ColumnSchema, to: &ColumnSchema) -> bool`
    - `pub fn scope(`
    - `pub fn diff(`
    - `pub async fn augment(`
    - `pub async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError>`
  - `data-avalanche-source-mysql/src/types.rs`:
    - `pub fn source_type(&self) -> String`
    - `pub fn from_schema(c: &ColumnSchema) -> Self`

`augment` and `rescan` (`Schema_Change…` §0, §3, §6; D-M7a.16). The catalog is the schema
*now*; the stream may be behind it by several statements. A change is read for one statement
and scoped to what it touched.

- **The stream decodes by the schema as of its position**, not the live catalog: a column's
  `source_type` is its `COLUMN_TYPE` plus ` character set <cs>` for character columns, and
  `CatalogColumn::from_schema` parses it back, so the cached schema alone plans every column as
  the catalog did (a test in `resolve` checks every type).
- `scope(cached, scope, renames, now)`: renames first, in place; then each scoped column from the
  catalog (modified, dropped, or added after its nearest preceding column). A column that can't
  be placed halts the table (`SCHEMA_MISMATCH`, "re-snapshot the table"). `Scope::Table` takes
  the catalog's table whole.
- `diff(cached, new, renames)`: columns matched by name and proven renames. `ENUM`/`SET` members
  appended → `EnumValuesAppended`, changed otherwise; a charset-only change → no target effect;
  nullability; an identifier change.
- `rescan`: one batched read of the tracked tables; dropped, changed and halted tables.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -12,6 +12,7 @@ pub mod ddl;
 mod error;
 pub mod json;
 pub mod position;
+pub mod schema;
 pub mod server;
 mod snapshot;
 pub mod types;
@@ -23,6 +24,7 @@ pub use connect::SESSION_SETUP;
 pub use connector::MySqlConnector;
 pub use ddl::{Altered, Ddl, Renamed, Scope, classify, renamed, shadow_of};
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
+pub use schema::{diff, narrowed, scope, scope_of};
 pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
 pub use types::{CatalogColumn, ColumnPlan, members, plan_column};
 pub use value::{Charset, Decode, Input, ValueError, decode, decode_after, int_type};
```

Change `crates/data-avalanche-source-mysql/tests/mysql/main.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/main.rs b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/main.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
@@ -7,6 +7,7 @@ mod common;
 mod position;
 mod preflight;
 mod resolve;
+mod schema;
 mod snapshot;
 mod tls;
 mod values;
```

Create `crates/data-avalanche-source-mysql/src/schema.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::ColumnValueType as V;

    use super::*;

    fn col(name: &str, t: V, source: &str, ordinal: i32) -> ColumnSchema {
        ColumnSchema {
            source_ordinal: Some(ordinal),
            ..ColumnSchema::new(name, t, source)
        }
    }

    fn table(columns: Vec<ColumnSchema>) -> TableSchema {
        let id = ColumnSchema {
            nullable: false,
            ..col("id", V::Int32, "int", 1)
        };
        let mut all = vec![id];
        all.extend(columns);
        TableSchema::new(TableRef::new("shop", "t"), all, vec!["id".into()]).unwrap()
    }

    fn names(s: &TableSchema) -> Vec<&str> {
        s.columns().iter().map(|c| c.name.as_str()).collect()
    }

    fn cols(n: &[&str]) -> Scope {
        Scope::Columns(n.iter().map(|x| x.to_string()).collect())
    }

    #[test]
    fn a_scoped_change_takes_only_its_columns_from_the_catalog() {
        let cached = table(vec![
            col("a", V::Int32, "int", 2),
            col("b", V::Text, "text", 3),
        ]);
        // The catalog is ahead: `ADD c AFTER id` is the statement, `DROP b` comes later.
        let now = table(vec![
            col("c", V::Int64, "bigint", 2),
            col("a", V::Int32, "int", 3),
        ]);
        let s = scope(&cached, &cols(&["c"]), &[], &now).unwrap();
        assert_eq!(names(&s), ["id", "c", "a", "b"]);
        assert_eq!(
            diff(&cached, &s, &[]),
            [SchemaChange::AddColumn(col("c", V::Int64, "bigint", 2))]
        );
        // The whole table, when the statement wasn't read.
        assert_eq!(
            names(&scope(&cached, &Scope::Table, &[], &now).unwrap()),
            ["id", "c", "a"]
        );
    }

    #[test]
    fn a_proven_rename_keeps_its_place() {
        let cached = table(vec![
            col("a", V::Int32, "int", 2),
            col("b", V::Text, "text", 3),
        ]);
        let now = table(vec![
            col("aa", V::Int64, "bigint", 2),
            col("b", V::Text, "text", 3),
        ]);
        let renames = [("a".to_owned(), "aa".to_owned())];
        let s = scope(&cached, &cols(&["a", "aa"]), &renames, &now).unwrap();
        assert_eq!(names(&s), ["id", "aa", "b"]);
        let d = diff(&cached, &s, &renames);
        assert_eq!(
            d[0],
            SchemaChange::RenameColumn {
                from: "a".into(),
                to: "aa".into(),
                proven: true
            }
        );
        assert!(
            matches!(&d[1], SchemaChange::ChangeType { column, narrowed: false, .. } if column == "aa")
        );
        // Unproven (the whole table): a drop and an add.
        let d = diff(&cached, &now, &[]);
        assert!(d.contains(&SchemaChange::DropColumn("a".into())));
        assert!(matches!(d.last(), Some(SchemaChange::AddColumn(c)) if c.name == "aa"));
    }

    #[test]
    fn what_the_catalog_cant_place_halts() {
        let cached = table(vec![col("a", V::Int32, "int", 2)]);
        let now = table(vec![col("b", V::Int32, "int", 2)]);
        // Renamed again since: the catalog doesn't show the rename's target.
        assert!(
            scope(
                &cached,
                &cols(&["a", "x"]),
                &[("a".into(), "x".into())],
                &now
            )
            .is_err()
        );
        // Added and dropped again since.
        assert!(scope(&cached, &cols(&["q"]), &[], &now).is_err());
    }

    #[test]
    fn types_members_charsets_and_keys_classify() {
        let cached = table(vec![
            col("e", V::Text, "enum('a','b') character set utf8mb4", 2),
            col("v", V::Text, "varchar(100) character set latin1", 3),
            col(
                "d",
                V::Numeric {
                    precision: 10,
                    scale: 2,
                },
                "decimal(10,2)",
                4,
            ),
        ]);
        let changed = |e: &str, v: &str, d: &str| {
            table(vec![
                col("e", V::Text, e, 2),
                col("v", V::Text, v, 3),
                col(
                    "d",
                    V::Numeric {
                        precision: 10,
                        scale: 2,
                    },
                    d,
                    4,
                ),
            ])
        };
        let d = diff(
            &cached,
            &changed(
                "enum('a','b','c') character set utf8mb4",
                "varchar(100) character set utf8mb4",
                "decimal(10,2)",
            ),
            &[],
        );
        assert_eq!(d[0], SchemaChange::EnumValuesAppended("e".into()));
        assert!(matches!(&d[1], SchemaChange::NoTargetEffect(m) if m.contains("latin1")));
        let d = diff(
            &cached,
            &changed(
                "enum('b','a') character set utf8mb4",
                "varchar(10) character set latin1",
                "decimal(10,2)",
            ),
            &[],
        );
        assert_eq!(d[0], SchemaChange::EnumValuesChanged("e".into()));
        assert!(matches!(
            &d[1],
            SchemaChange::ChangeType { narrowed: true, .. }
        ));
        let mut keyed = cached.columns().to_vec();
        keyed[1].nullable = false;
        let rekeyed = TableSchema::new(
            TableRef::new("shop", "t"),
            keyed,
            vec!["id".into(), "e".into()],
        )
        .unwrap();
        assert_eq!(
            diff(&cached, &rekeyed, &[]),
            [
                SchemaChange::SetNotNull("e".into()),
                SchemaChange::IdentifierChanged
            ]
        );
    }

    #[test]
    fn narrowing_reads_lengths_sizes_digits_and_fractions() {
        let c = |t: &str, v: V| ColumnSchema::new("c", v, t);
        let text = |a: &str, b: &str| narrowed(&c(a, V::Text), &c(b, V::Text));
        assert!(text("varchar(100)", "varchar(10)"));
        assert!(!text("varchar(10)", "varchar(100)"));
        assert!(text("text", "varchar(100)"));
        assert!(!text("varchar(100)", "mediumtext"));
        assert!(text(
            "longtext character set utf8mb4",
            "text character set utf8mb4"
        ));
        let ts = |a: &str, b: &str| {
            narrowed(
                &c(a, V::Timestamp { with_tz: false }),
                &c(b, V::Timestamp { with_tz: false }),
            )
        };
        assert!(ts("datetime(6)", "datetime(3)"));
        assert!(!ts("datetime", "datetime(6)"));
        let n = V::Numeric {
            precision: 10,
            scale: 2,
        };
        assert!(narrowed(
            &c("decimal(10,4)", n.clone()),
            &c("decimal(10,2)", n.clone())
        ));
        // MariaDB keeps integer display widths: they bound nothing.
        assert!(!narrowed(&c("int(11)", V::Int32), &c("int(5)", V::Int32)));
    }

    #[test]
    fn a_statement_scopes_its_table() {
        let t = TableRef::new("shop", "orders");
        assert_eq!(
            scope_of("ALTER TABLE orders ADD COLUMN x INT", &t),
            (cols(&["x"]), vec![])
        );
        assert_eq!(
            scope_of("ALTER TABLE orders ENGINE=InnoDB", &t).0,
            Scope::Table
        );
        assert_eq!(
            scope_of(
                "RENAME TABLE orders TO _orders_del, _orders_gho TO orders",
                &t
            ),
            (Scope::Table, vec![])
        );
    }
}
```

Change `crates/data-avalanche-source-mysql/tests/mysql/resolve.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs b/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/resolve.rs
@@ -1,8 +1,9 @@
 //! Channel A against the target server: the globs select the table set, identifiers decide what
 //! halts and what is skipped, and each column plans per `Type_Mapping.md` (config §11.2).
 
+use data_avalanche_config::NumericOverflowAction;
 use data_avalanche_core::{ColumnValueType as V, HaltReason, TableRef};
-use data_avalanche_source_mysql::Flavour;
+use data_avalanche_source_mysql::{CatalogColumn, Flavour, plan_column, plan_table};
 use data_avalanche_testkit::require_it;
 
 use crate::common::{cleanup, connector, fresh, pipeline, target};
@@ -115,6 +116,34 @@ async fn columns_plan_per_the_type_table() {
         "{siu}"
     );
     assert_eq!(s.column("tm").unwrap().source_ordinal, Some(15));
+    assert!(
+        s.column("c")
+            .unwrap()
+            .source_type
+            .ends_with(" character set latin1"),
+        "{:?}",
+        s.column("c").unwrap().source_type
+    );
+    // The schema alone plans every column as the catalog does: the stream decodes by it.
+    let c = connector(&pipeline(&n, &["t"], "", ""));
+    let described = c.describe(&["t".to_owned()]).await.unwrap();
+    let planned = plan_table(&described["t"], &pipeline(&n, &["t"], "", "").tables, true).unwrap();
+    for (col, plan) in planned.schema.columns().iter().zip(&planned.plans) {
+        let again = plan_column(
+            &CatalogColumn::from_schema(col),
+            true,
+            NumericOverflowAction::String,
+            None,
+            plan.identifier,
+        )
+        .unwrap();
+        assert_eq!(
+            (&again.value_type, &again.decode),
+            (&col.value_type, &plan.decode),
+            "{}",
+            col.name
+        );
+    }
     // tinyint1_as_bool off: TINYINT(1) is an integer.
     let r = connector(&pipeline(&n, &["t"], "", ", tinyint1_as_bool: false"))
         .resolve_tables()
```

Create `crates/data-avalanche-source-mysql/tests/mysql/schema.rs`:

```rust
//! Channels C and D against the target server: a statement's change read from the catalog and
//! scoped to its columns, with the catalog ahead of the stream; the idle rescan.

use data_avalanche_core::{ColumnValueType as V, SchemaChange, SchemaHint, TableRef};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline};

fn ddl(sql: &str) -> SchemaHint {
    SchemaHint::Ddl(sql.into())
}

#[tokio::test]
async fn a_statement_s_change_is_scoped_to_its_columns() {
    require_it!();
    let (n, mut a) = fresh(
        "schema_augment",
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b VARCHAR(20), e ENUM('x','y'));",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let cached = c.resolve_tables().await.unwrap().tables.remove(0);
    // Two statements ran; the stream is at the first: the catalog is ahead.
    let first = "ALTER TABLE t ADD COLUMN c BIGINT AFTER id, CHANGE COLUMN a aa BIGINT";
    a.query_drop(first).await.unwrap();
    a.query_drop("ALTER TABLE t DROP COLUMN b, MODIFY e ENUM('x','y','z')")
        .await
        .unwrap();
    let (delta, now) = c.augment(&cached, &ddl(first)).await.unwrap();
    let now = now.unwrap();
    let names: Vec<&str> = now.columns().iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        ["id", "c", "aa", "b", "e"],
        "b is still in the stream"
    );
    assert!(
        matches!(&delta.changes[0], SchemaChange::RenameColumn { from, to, proven: true } if from == "a" && to == "aa")
    );
    assert!(matches!(&delta.changes[1], SchemaChange::ChangeType { column, .. } if column == "aa"));
    assert!(
        matches!(&delta.changes[2], SchemaChange::AddColumn(col) if col.name == "c" && col.value_type == V::Int64)
    );
    assert_eq!(delta.changes.len(), 3, "{:?}", delta.changes);
    // The second statement, from there.
    let (delta, _) = c
        .augment(
            &now,
            &ddl("ALTER TABLE t DROP COLUMN b, MODIFY e ENUM('x','y','z')"),
        )
        .await
        .unwrap();
    assert_eq!(
        delta.changes,
        [
            SchemaChange::DropColumn("b".into()),
            SchemaChange::EnumValuesAppended("e".into())
        ]
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn a_dropped_or_swapped_table() {
    require_it!();
    let (n, mut a) = fresh(
        "schema_swap",
        "CREATE TABLE t (id INT PRIMARY KEY, v INT);
         CREATE TABLE gone (id INT PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t", "gone"], "", ""));
    let r = c.resolve_tables().await.unwrap();
    let schema = |name: &str| {
        r.tables
            .iter()
            .find(|s| s.table().name == name)
            .unwrap()
            .clone()
    };
    // A gh-ost cut-over: the shadow table, migrated, swaps into the name.
    let swap = "RENAME TABLE t TO _t_del, _t_gho TO t";
    a.query_drop(
        "CREATE TABLE _t_gho (id INT PRIMARY KEY, v BIGINT, w INT);
         RENAME TABLE t TO _t_del, _t_gho TO t;
         DROP TABLE gone;",
    )
    .await
    .unwrap();
    let (delta, now) = c.augment(&schema("t"), &ddl(swap)).await.unwrap();
    assert_eq!(now.unwrap().columns().len(), 3);
    assert!(matches!(&delta.changes[0], SchemaChange::ChangeType { column, .. } if column == "v"));
    assert!(matches!(&delta.changes[1], SchemaChange::AddColumn(c) if c.name == "w"));
    let (delta, now) = c
        .augment(
            &schema("gone"),
            &ddl("DROP TABLE `gone` /* generated by server */"),
        )
        .await
        .unwrap();
    assert_eq!(
        (delta.changes, now),
        (vec![SchemaChange::TableDropped], None)
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn the_rescan_reports_dropped_and_changed_tables() {
    require_it!();
    let (n, mut a) = fresh(
        "schema_rescan",
        "CREATE TABLE same (id INT PRIMARY KEY);
         CREATE TABLE altered (id INT PRIMARY KEY);
         CREATE TABLE dropped (id INT PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["same", "altered", "dropped"], "", ""));
    let known = c.resolve_tables().await.unwrap().tables;
    a.query_drop("ALTER TABLE altered ADD COLUMN v INT; DROP TABLE dropped;")
        .await
        .unwrap();
    let report = c.rescan(&known).await.unwrap();
    assert_eq!(report.dropped, [TableRef::new(&n.db, "dropped")]);
    assert_eq!(report.changed.len(), 1);
    assert_eq!(report.changed[0].table().name, "altered");
    assert!(report.halted.is_empty());
    cleanup(&n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `schema::diff`, `schema::narrowed`, `schema::scope`, `schema::scope_of`
error[E0425]: cannot find type `ColumnSchema` in this scope
error[E0422]: cannot find struct, variant or union type `ColumnSchema` in this scope
error[E0425]: cannot find type `TableSchema` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-source-mysql/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/catalog.rs b/crates/data-avalanche-source-mysql/src/catalog.rs
--- a/crates/data-avalanche-source-mysql/src/catalog.rs
+++ b/crates/data-avalanche-source-mysql/src/catalog.rs
@@ -140,7 +140,7 @@ pub fn plan_table(
             value_type: p.value_type.clone(),
             logical: p.logical.clone(),
             nullable: c.nullable,
-            source_type: c.column_type.clone(),
+            source_type: c.source_type(),
             source_ordinal: Some(c.ordinal as i32),
         });
         plans.push(p);
```

In `crates/data-avalanche-source-mysql/src/schema.rs`, add above the test module:

```rust
//! Channels C and D (`Schema_Change…` §0, §3, §6): the change a DDL statement implicates, read from
//! `information_schema` and scoped to the columns the statement names, and the batched idle
//! rescan. MySQL has no column identity that survives a rename: a column is its name, and a
//! rename is proven only by the statement (`RENAME COLUMN`, `CHANGE old new`). Where the
//! classifier couldn't read the statement, the whole catalog table is the change, and a rename
//! reads as a drop and an add (§6, degraded with a warning by the evolver).
//!
//! The catalog may be ahead of the stream (§0): a scoped change takes only its columns from it.
//! The stream checks every table map against the schema, so a change taken early (a later DDL's
//! column) halts the table instead of misreading rows.

use std::collections::BTreeMap;

use data_avalanche_core::{
    ColumnSchema, HaltReason, RescanReport, SchemaChange, SchemaDelta, SchemaHint, SourceError,
    TableRef, TableSchema,
};

use crate::catalog::{plan_table, table_key};
use crate::connector::MySqlConnector;
use crate::ddl::{Renamed, Scope, classify, renamed};
use crate::types::{CatalogColumn, members};

/// The change a statement makes to `table`: its scope and proven renames. A cut-over swap into the
/// table's name scopes the whole table, with nothing proven.
pub fn scope_of(sql: &str, table: &TableRef) -> (Scope, Vec<(String, String)>) {
    let d = classify(sql, &table.schema);
    let swapped = renamed(&d.renamed, std::slice::from_ref(table))
        .iter()
        .any(|(_, r)| matches!(r, Renamed::Swapped { .. }));
    match d.altered.iter().find(|a| &a.table == table) {
        Some(a) if !swapped => (a.scope.clone(), a.renames.clone()),
        _ => (Scope::Table, vec![]),
    }
}

/// The bound a type's text puts on its values, where one type holds fewer than another of the
/// same logical type: a length, a precision and scale, fractional digits, a text or blob size.
fn bound(c: &CatalogColumn) -> Option<(u64, u64)> {
    let n = |v: Option<u32>| v.map(u64::from);
    let length = || {
        let open = c.column_type.find('(')?;
        let close = c.column_type[open..].find(')')? + open;
        c.column_type[open + 1..close].trim().parse().ok()
    };
    Some(match c.data_type.as_str() {
        "char" | "varchar" | "binary" | "varbinary" | "bit" => (length()?, 0),
        "tinytext" | "tinyblob" => (255, 0),
        "text" | "blob" => (65_535, 0),
        "mediumtext" | "mediumblob" => (16_777_215, 0),
        "longtext" | "longblob" => (4_294_967_295, 0),
        "decimal" | "numeric" => (n(c.precision)? - n(c.scale)?, n(c.scale)?),
        "time" | "datetime" | "timestamp" => (n(c.fsp)?, 0),
        _ => return None,
    })
}

/// Whether `to` holds fewer values than `from` of the same logical type: a shorter length or text
/// size, fewer integer or fraction digits, fewer fractional seconds.
pub fn narrowed(from: &ColumnSchema, to: &ColumnSchema) -> bool {
    if from.logical != to.logical {
        return false;
    }
    let (f, t) = (
        CatalogColumn::from_schema(from),
        CatalogColumn::from_schema(to),
    );
    match (bound(&f), bound(&t)) {
        (Some((fa, fb)), Some((ta, tb))) => ta < fa || tb < fb,
        _ => false,
    }
}

fn guess(column: &str, why: &str) -> HaltReason {
    HaltReason::SchemaMismatch {
        column: column.into(),
        detail: format!("{why}; re-snapshot the table"),
    }
}

/// The schema after the statement (§0: scoped, never the catalog's "now" beyond the statement's
/// columns): `cached` with each scoped column taken from `now` — changed, added at its place among
/// the columns `now` has before it, or dropped when `now` hasn't it — and each proven rename's
/// column renamed in place. A scoped column neither schema has, or a rename `now` doesn't show,
/// can't be placed: the table halts. The identifier is `now`'s.
pub fn scope(
    cached: &TableSchema,
    scope: &Scope,
    renames: &[(String, String)],
    now: &TableSchema,
) -> Result<TableSchema, HaltReason> {
    let Scope::Columns(names) = scope else {
        return Ok(now.clone());
    };
    let mut columns: Vec<ColumnSchema> = cached.columns().to_vec();
    for (old, new) in renames {
        let Some(at) = columns.iter().position(|c| &c.name == old) else {
            return Err(guess(old, "a renamed column the schema doesn't have"));
        };
        let Some(c) = now.column(new) else {
            return Err(guess(
                new,
                "the catalog, ahead of the stream, doesn't have the renamed column",
            ));
        };
        columns[at] = c.clone();
    }
    let renamed = |n: &String| renames.iter().any(|(o, r)| o == n || r == n);
    for name in names.iter().filter(|n| !renamed(n)) {
        let at = columns.iter().position(|c| &c.name == name);
        match (at, now.column(name)) {
            (Some(at), Some(c)) => columns[at] = c.clone(),
            (Some(at), None) => {
                columns.remove(at);
            }
            (None, Some(c)) => {
                // After the nearest column before it in the catalog that the schema has.
                let before = now
                    .columns()
                    .iter()
                    .take_while(|x| &x.name != name)
                    .filter_map(|x| columns.iter().position(|y| y.name == x.name))
                    .last();
                columns.insert(before.map_or(0, |b| b + 1), c.clone());
            }
            (None, None) => {
                return Err(guess(
                    name,
                    "a column the statement names that neither the schema nor the catalog has",
                ));
            }
        }
    }
    // Positions follow the catalog's where it has the column.
    for c in &mut columns {
        if let Some(n) = now.column(&c.name) {
            c.source_ordinal = n.source_ordinal;
        }
    }
    TableSchema::new(cached.table().clone(), columns, now.identifier().to_vec())
}

/// The ENUM or SET change between two column types of the same column, if both are one.
fn members_change(column: &str, from: &CatalogColumn, to: &CatalogColumn) -> Option<SchemaChange> {
    let kind = |c: &CatalogColumn| matches!(c.data_type.as_str(), "enum" | "set");
    if !kind(from) || from.data_type != to.data_type {
        return None;
    }
    let (f, t) = (members(&from.column_type), members(&to.column_type));
    Some(match t.starts_with(&f) {
        true => SchemaChange::EnumValuesAppended(column.into()),
        false => SchemaChange::EnumValuesChanged(column.into()),
    })
}

/// `cached` → `new` as changes, columns matched by name and by the proven `renames`.
pub fn diff(
    cached: &TableSchema,
    new: &TableSchema,
    renames: &[(String, String)],
) -> Vec<SchemaChange> {
    let mut changes = vec![];
    fn target<'a>(renames: &'a [(String, String)], name: &'a str) -> &'a str {
        renames
            .iter()
            .find(|(o, _)| o == name)
            .map_or(name, |(_, n)| n.as_str())
    }
    for c in cached.columns() {
        let to = target(renames, &c.name);
        let Some(n) = new.column(to) else {
            changes.push(SchemaChange::DropColumn(c.name.clone()));
            continue;
        };
        if to != c.name {
            changes.push(SchemaChange::RenameColumn {
                from: c.name.clone(),
                to: to.to_owned(),
                proven: true,
            });
        }
        let (f, t) = (CatalogColumn::from_schema(c), CatalogColumn::from_schema(n));
        if n.value_type != c.value_type || n.logical != c.logical || f.column_type != t.column_type
        {
            match members_change(to, &f, &t) {
                Some(m) if n.value_type == c.value_type => changes.push(m),
                _ => changes.push(SchemaChange::ChangeType {
                    column: to.to_owned(),
                    from: c.clone(),
                    to: n.clone(),
                    narrowed: narrowed(c, n),
                }),
            }
        } else if f.charset != t.charset {
            changes.push(SchemaChange::NoTargetEffect(format!(
                "column {to:?}: character set {:?} → {:?}",
                f.charset, t.charset
            )));
        }
        match (c.nullable, n.nullable) {
            (true, false) => changes.push(SchemaChange::SetNotNull(to.to_owned())),
            (false, true) => changes.push(SchemaChange::DropNotNull(to.to_owned())),
            _ => {}
        }
    }
    let renamed_to = |n: &str| renames.iter().any(|(_, r)| r == n);
    for n in new.columns() {
        if cached.column(&n.name).is_none() && !renamed_to(&n.name) {
            changes.push(SchemaChange::AddColumn(n.clone()));
        }
    }
    let identifier = |s: &TableSchema| -> Vec<String> {
        s.identifier()
            .iter()
            .map(|k| target(renames, k).to_owned())
            .collect()
    };
    if identifier(cached) != new.identifier() {
        changes.push(SchemaChange::IdentifierChanged);
    }
    changes
}

impl MySqlConnector {
    /// The tables' schemas as the catalog has them now, by name; a table not there is left out.
    async fn current(
        &self,
        names: &[String],
    ) -> Result<BTreeMap<String, Result<TableSchema, HaltReason>>, SourceError> {
        let described = self.describe(names).await?;
        Ok(described
            .into_iter()
            .map(|(name, d)| {
                let planned = plan_table(&d, &self.pipeline.tables, self.tinyint1_as_bool());
                (name, planned.map(|p| p.schema))
            })
            .collect())
    }

    /// Channel C: the change the statement `hint` makes to `cached` (`Schema_Change…` §0, §3,
    /// §6). A table gone from the catalog is dropped.
    pub async fn augment(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
        let table = cached.table().clone();
        let SchemaHint::Ddl(sql) = hint else {
            return Err(SourceError::Permanent(format!(
                "{}: a column hint on a MySQL source",
                table_key(&table)
            )));
        };
        let halt = |reason| SourceError::TableHalt {
            table: table.clone(),
            reason,
        };
        let Some(now) = self
            .current(std::slice::from_ref(&table.name))
            .await?
            .remove(&table.name)
        else {
            return Ok((
                SchemaDelta {
                    table: table.clone(),
                    changes: vec![SchemaChange::TableDropped],
                },
                None,
            ));
        };
        let now = now.map_err(halt)?;
        let (columns, renames) = scope_of(sql, &table);
        let new = scope(cached, &columns, &renames, &now).map_err(halt)?;
        let changes = diff(cached, &new, &renames);
        Ok((SchemaDelta { table, changes }, Some(new)))
    }

    /// Channel D: one batched catalog read of the tracked tables. A table gone from the catalog is
    /// dropped; one whose catalog schema differs is reported with it, for freshness and alerting
    /// only.
    pub async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
        let names: Vec<String> = known.iter().map(|s| s.table().name.clone()).collect();
        let mut now = self.current(&names).await?;
        let mut report = RescanReport::default();
        for k in known {
            let t = k.table();
            match now.remove(&t.name) {
                None => report.dropped.push(t.clone()),
                Some(Err(reason)) => report.halted.push((t.clone(), reason)),
                Some(Ok(s)) if s != *k => report.changed.push(s),
                Some(Ok(_)) => {}
            }
        }
        Ok(report)
    }
}

```

Change `crates/data-avalanche-source-mysql/src/types.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/types.rs b/crates/data-avalanche-source-mysql/src/types.rs
--- a/crates/data-avalanche-source-mysql/src/types.rs
+++ b/crates/data-avalanche-source-mysql/src/types.rs
@@ -4,8 +4,8 @@
 
 use data_avalanche_config::NumericOverflowAction;
 use data_avalanche_core::{
-    ColumnValueType as V, HaltReason, LogicalType, MAX_DECIMAL_PRECISION, canonical_logical,
-    conversion, hop2_compatible,
+    ColumnSchema, ColumnValueType as V, HaltReason, LogicalType, MAX_DECIMAL_PRECISION,
+    canonical_logical, conversion, hop2_compatible,
 };
 
 use crate::value::{Charset, Decode, int_type};
@@ -35,7 +35,73 @@ pub struct CatalogColumn {
     pub nullable: bool,
 }
 
+/// The text between a type's parentheses, split at commas: `decimal(10,2)` → `["10", "2"]`.
+fn arguments(column_type: &str) -> Vec<&str> {
+    match (column_type.find('('), column_type.find(')')) {
+        (Some(open), Some(close)) if open < close => column_type[open + 1..close]
+            .split(',')
+            .map(str::trim)
+            .collect(),
+        _ => vec![],
+    }
+}
+
+/// Where a `source_type` names its character set.
+const CHARSET: &str = " character set ";
+
 impl CatalogColumn {
+    /// The schema's `source_type`: `COLUMN_TYPE`, and the character set of a character column
+    /// (`varchar(10) character set latin1`), so that the as-of-position schema is enough to decode
+    /// the stream's values ([`CatalogColumn::from_schema`]).
+    pub fn source_type(&self) -> String {
+        match &self.charset {
+            Some(cs) => format!("{}{CHARSET}{cs}", self.column_type),
+            None => self.column_type.clone(),
+        }
+    }
+
+    /// The column a schema column was planned from, as far as decoding needs it: the type,
+    /// signedness, members, precision, fractional digits, `BINARY` length and character set
+    /// (`source_type`), its position and nullability. Not the collation, which only a snapshot
+    /// read uses (it reads the catalog).
+    pub fn from_schema(c: &ColumnSchema) -> Self {
+        let (column_type, charset) = match c.source_type.rsplit_once(CHARSET) {
+            Some((t, cs))
+                if !cs.is_empty() && cs.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') =>
+            {
+                (t.to_owned(), Some(cs.to_owned()))
+            }
+            _ => (c.source_type.clone(), None),
+        };
+        let data_type = column_type
+            .split(['(', ' '])
+            .next()
+            .unwrap_or_default()
+            .to_owned();
+        let args = arguments(&column_type);
+        let number = |i: usize| args.get(i).and_then(|a| a.parse::<u32>().ok());
+        let (mut precision, mut scale, mut fsp, mut max_length) = (None, None, None, None);
+        match data_type.as_str() {
+            "decimal" | "numeric" => (precision, scale) = (number(0), number(1).or(Some(0))),
+            "time" | "datetime" | "timestamp" => fsp = Some(number(0).unwrap_or(0)),
+            "binary" => max_length = number(0).map(u64::from),
+            _ => {}
+        }
+        Self {
+            name: c.name.clone(),
+            data_type,
+            column_type,
+            charset,
+            collation: None,
+            precision,
+            scale,
+            fsp,
+            max_length,
+            ordinal: c.source_ordinal.unwrap_or_default() as u32,
+            nullable: c.nullable,
+        }
+    }
+
     fn unsigned(&self) -> bool {
         self.column_type.contains("unsigned")
     }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 67 tests.

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql schema:: || break; done`

Expected: PASS, 3 tests on each target (`test-infra/up.sh` first).

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql resolve:: || break; done`

Expected: PASS, 4 tests on each target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/src/catalog.rs \
  crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/schema.rs \
  crates/data-avalanche-source-mysql/src/types.rs \
  crates/data-avalanche-source-mysql/tests/mysql/main.rs \
  crates/data-avalanche-source-mysql/tests/mysql/resolve.rs \
  crates/data-avalanche-source-mysql/tests/mysql/schema.rs
git commit -m "feat(source-mysql): channels C and D — a statement's change scoped to its columns, proven renames, the rescan"
```

---

### Task 13: The binlog stream

**Files:**
- Modify: `crates/data-avalanche-source-mysql/Cargo.toml`
- Modify: `crates/data-avalanche-source-mysql/src/connector.rs`
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`
- Modify: `crates/data-avalanche-source-mysql/src/schema.rs`
- Modify: `crates/data-avalanche-source-mysql/src/snapshot.rs`
- Create: `crates/data-avalanche-source-mysql/src/stream.rs`
- Modify: `crates/data-avalanche-source-mysql/src/value.rs`
- Modify: `crates/data-avalanche-source-mysql/tests/mysql/main.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/stream.rs`

**Interfaces:**
- Consumes:
  - `Offsets`, `MariaGtidEvent`, `decompress`, `rotate_name` (Task 6); `decode`, `decode_after`
    (Tasks 7, 8); `classify`, `renamed` (Task 11); `read_change`, `scope` (Task 12); positions
    (Task 5).
  - `data_avalanche_core::{ChangeStream, StreamItem, CdcEvent, TransactionBoundary}`.
- Produces:
  - `data-avalanche-source-mysql/src/stream.rs`:
    - `pub struct MySqlStream`
    - `pub async fn open_stream(`

`open_stream` and `MySqlStream` (Data-Flow Phase 2b; D-M7a.16 to D-M7a.19).

- **The dump**: the replica id is `source.mysql.server_id`, the heartbeat period
  `source.mysql.heartbeat`. MySQL GTID mode sends the GTID set (tags included); MariaDB GTID
  mode sets `@slave_connect_state` and `@slave_gtid_strict_mode = 1`; file mode sends the file
  and offset. MariaDB always gets `@mariadb_slave_capability = 4`, which keeps GTID and annotate
  events real (S-MY10i).
- **Decoding by the schema**: the stream keeps the tracked tables' schemas as of its position
  and plans their columns from them (Task 12). A `TABLE_MAP` whose column count, type families
  or names (under `FULL` metadata) differ from the schema halts the table ("re-snapshot the
  table"), and so does a rows event whose image isn't `FULL`: a catalog read that got ahead of
  the stream can halt a table, never decode it wrongly.
- **Statements**: each `QUERY_EVENT` goes through the classifier. A dropped database, an
  unlogged partition operation, a dropped table or one renamed away halt (pipeline or table);
  TRUNCATE becomes a `Truncate` item; an ALTER or a cut-over swap becomes a `SchemaSignal`. At a
  signal the stream reads the change itself (a sibling connector with its own catalog
  connection), plans the table again, and keeps the answer for the pipeline's `augment` of the
  same signal: one catalog read per statement, and one answer for both.
- **Transactions and positions** (Q-M7-4): a change's `source_pos` (the `_lsn` lineage column)
  is its transaction's own GTID (a set or state of one), or the transaction's start `file:pos`
  in file mode. A commit's position is the cumulative GTID set, the MariaDB state, or the file
  and the XID's end. `Begin` carries a session counter for MySQL's anonymous transactions
  (D-M7a.19), the MariaDB sequence otherwise.
- **Fail closed** (D-M7a.17): `INCIDENT_EVENT` and event types the stream doesn't know halt
  the pipeline. XA (measured on the three servers): a two-phase `XA PREPARE` holding tracked
  rows halts the stream, since its rows could still roll back; a one-phase XA commits as a
  transaction; XA on untracked tables advances the position.
- Heartbeats become `KeepAlive` between transactions. `next` is cancel-safe: an event is kept
  until handled.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-source-mysql/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-source-mysql/Cargo.toml b/crates/data-avalanche-source-mysql/Cargo.toml
--- a/crates/data-avalanche-source-mysql/Cargo.toml
+++ b/crates/data-avalanche-source-mysql/Cargo.toml
@@ -10,6 +10,7 @@ description = "The MySQL and MariaDB source: the binlog over mysql_async, catalo
 workspace = true
 
 [dependencies]
+async-trait.workspace = true
 chrono.workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-core.workspace = true
```

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -15,6 +15,7 @@ pub mod position;
 pub mod schema;
 pub mod server;
 mod snapshot;
+mod stream;
 pub mod types;
 pub mod value;
 
@@ -26,5 +27,6 @@ pub use ddl::{Altered, Ddl, Renamed, Scope, classify, renamed, shadow_of};
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
 pub use schema::{diff, narrowed, scope, scope_of};
 pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
+pub use stream::MySqlStream;
 pub use types::{CatalogColumn, ColumnPlan, members, plan_column};
 pub use value::{Charset, Decode, Input, ValueError, decode, decode_after, int_type};
```

Change `crates/data-avalanche-source-mysql/tests/mysql/main.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/main.rs b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/main.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
@@ -9,5 +9,6 @@ mod preflight;
 mod resolve;
 mod schema;
 mod snapshot;
+mod stream;
 mod tls;
 mod values;
```

Create `crates/data-avalanche-source-mysql/src/stream.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gtid_set_becomes_sid_blocks_per_uuid_and_tag() {
        let set: GtidSet = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7:alpha:1-3,4e11fa47-71ca-11e1-9e33-c80aa9429562:9"
            .parse()
            .unwrap();
        let s = sids(&set).unwrap();
        assert_eq!(s.len(), 3);
        assert_eq!(s[0].intervals().len(), 2);
        assert_eq!(s[1].tag().map(|t| t.as_str()), Some("alpha"));
        assert_eq!(
            (s[2].intervals()[0].start(), s[2].intervals()[0].end()),
            (9, 10)
        );
        assert!(sids(&GtidSet::default()).unwrap().is_empty());
    }

    #[test]
    fn statements_lead_with_their_verb() {
        assert_eq!(verb("  begin"), "BEGIN");
        assert_eq!(verb("XA START 'x'"), "XA");
        assert_eq!(verb("SAVEPOINT `s`"), "SAVEPOINT");
        assert_eq!(verb("/* c */ ALTER"), "");
    }

    #[test]
    fn column_types_arrive_as_their_binlog_types() {
        use ColumnType::*;
        assert!(arrives_as("bigint").contains(&MYSQL_TYPE_LONGLONG));
        assert!(arrives_as("enum").contains(&MYSQL_TYPE_STRING));
        assert!(arrives_as("longtext").contains(&MYSQL_TYPE_BLOB));
        assert!(arrives_as("datetime").contains(&MYSQL_TYPE_DATETIME2));
        assert!(!arrives_as("int").contains(&MYSQL_TYPE_LONGLONG));
        assert!(arrives_as("nonsense").is_empty());
    }
}
```

Create `crates/data-avalanche-source-mysql/tests/mysql/stream.rs`:

```rust
//! The binlog stream against the target server (Data-Flow Phase 2b): transactions, positions and
//! resume; statements routed to signals, truncates and halts; table maps and row images checked;
//! heartbeats; and on MySQL compressed payloads, partial JSON, tagged GTIDs and XA.

use std::time::Duration;

use data_avalanche_core::{
    ChangeStream, ColumnValue, HaltReason, Op, SourceError, SourcePosition, StreamItem, TableSchema,
};
use data_avalanche_source_mysql::{Flavour, MySqlConnector, MySqlStream};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, target};

/// Items until `commits` transactions committed (keep-alives left out).
async fn items(s: &mut MySqlStream, commits: usize) -> Vec<StreamItem> {
    let mut out = vec![];
    let mut seen = 0;
    while seen < commits {
        let item = tokio::time::timeout(Duration::from_secs(20), s.next())
            .await
            .expect("an item within 20 s")
            .expect("the stream");
        match item {
            StreamItem::KeepAlive(_) => continue,
            StreamItem::Commit(_) => seen += 1,
            _ => {}
        }
        out.push(item);
    }
    out
}

/// The first stream error.
async fn error(s: &mut MySqlStream) -> SourceError {
    loop {
        match tokio::time::timeout(Duration::from_secs(20), s.next())
            .await
            .expect("an item within 20 s")
        {
            Ok(_) => continue,
            Err(e) => return e,
        }
    }
}

async fn shut(s: MySqlStream) {
    Box::new(s).close().await.expect("close");
}

async fn open(c: &MySqlConnector, tables: &[TableSchema]) -> MySqlStream {
    let from = c.position_now().await.expect("position");
    c.open_stream(&from, tables).await.expect("stream")
}

fn changes(items: &[StreamItem]) -> Vec<(Op, Option<ColumnValue>)> {
    items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => {
                let image = e.after.as_ref().or(e.before.as_ref()).unwrap();
                Some((e.op, image.get("id").cloned()))
            }
            _ => None,
        })
        .collect()
}

fn commits(items: &[StreamItem]) -> Vec<SourcePosition> {
    items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Commit(b) => Some(b.commit_pos.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn changes_arrive_as_transactions_and_resume_after_a_commit() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_txn",
        "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10)); CREATE TABLE other (id INT PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    a.query_drop(
        "BEGIN; INSERT INTO t VALUES (1, 'a'), (2, 'b'); COMMIT;
         INSERT INTO other VALUES (1);
         UPDATE t SET v = 'c' WHERE id = 1;
         DELETE FROM t WHERE id = 2;",
    )
    .await
    .unwrap();
    let got = items(&mut s, 3).await;
    let one = Some(ColumnValue::Int32(1));
    let two = Some(ColumnValue::Int32(2));
    assert_eq!(
        changes(&got),
        [
            (Op::Insert, one.clone()),
            (Op::Insert, two.clone()),
            (Op::Update, one.clone()),
            (Op::Delete, two)
        ]
    );
    assert!(matches!(got[0], StreamItem::Begin { .. }));
    // The update carries both images; the order key grows, equal only within a transaction.
    let events: Vec<_> = got
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => Some(e.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        events[2].before.as_ref().unwrap().get("v"),
        Some(&ColumnValue::Text("a".into()))
    );
    assert!(events[0].change_seq <= events[1].change_seq);
    assert!(events[1].change_seq < events[2].change_seq);
    assert!(events[2].change_seq < events[3].change_seq);
    // Each commit covers the one before it.
    let pos = commits(&got);
    assert!(pos[1].covers(&pos[0]).unwrap() && pos[2].covers(&pos[1]).unwrap());
    // A row's position is its transaction's own; the commit's covers it.
    assert!(pos[0].covers(&events[0].source_pos).unwrap());
    assert_eq!(events[0].source_pos, events[1].source_pos);
    shut(s).await;
    // Resume after the second commit: the third transaction, once.
    let mut s = c.open_stream(&pos[1], &tables).await.unwrap();
    let again = items(&mut s, 1).await;
    assert_eq!(changes(&again), [(Op::Delete, Some(ColumnValue::Int32(2)))]);
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn statements_route_to_signals_truncates_and_halts() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_ddl",
        "CREATE TABLE t (id INT PRIMARY KEY, v INT);
         CREATE TABLE gone (id INT PRIMARY KEY);
         CREATE TABLE parts (id INT PRIMARY KEY) PARTITION BY RANGE (id)
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE);
         CREATE TABLE emptied (id INT PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t", "gone", "parts", "emptied"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let cached_t = tables
        .iter()
        .find(|s| s.table().name == "t")
        .unwrap()
        .clone();
    let mut s = open(&c, &tables).await;
    a.query_drop(
        "ALTER TABLE t ADD COLUMN w VARCHAR(5) AFTER id;
         INSERT INTO t VALUES (1, 'x', 2);
         TRUNCATE emptied;
         DROP TABLE gone;
         ALTER TABLE parts DROP PARTITION p0;",
    )
    .await
    .unwrap();
    let got = items(&mut s, 5).await;
    let signal = got.iter().find_map(|i| match i {
        StreamItem::SchemaSignal { table, hint, .. } => Some((table.name.clone(), hint.clone())),
        _ => None,
    });
    let (table, hint) = signal.expect("a schema signal");
    assert_eq!(table, "t");
    // The pipeline's augment gets the answer the stream read.
    let (delta, now) = c.augment(&cached_t, &hint).await.unwrap();
    assert_eq!(delta.changes.len(), 1, "{:?}", delta.changes);
    assert_eq!(now.unwrap().columns()[1].name, "w");
    let row = got
        .iter()
        .find_map(|i| match i {
            StreamItem::Change(e) => e.after.clone(),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        row.get("w"),
        Some(&ColumnValue::Text("x".into())),
        "decoded by the new schema"
    );
    assert_eq!(row.get("v"), Some(&ColumnValue::Int32(2)));
    let truncated = got
        .iter()
        .any(|i| matches!(i, StreamItem::Truncate { tables, .. } if tables[0].name == "emptied"));
    assert!(truncated);
    let halts: Vec<(String, &str)> = got
        .iter()
        .filter_map(|i| match i {
            StreamItem::TableHalt { table, reason, .. } => {
                Some((table.name.clone(), reason.code()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        halts,
        [
            ("gone".to_owned(), "TABLE_DROPPED"),
            ("parts".to_owned(), "UNSAFE_DDL")
        ]
    );
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn a_cut_over_swap_is_followed() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_swap",
        "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 1);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    // gh-ost: the shadow table gets the rows and the new column, then swaps in.
    a.query_drop(
        "CREATE TABLE _t_gho (id INT PRIMARY KEY, v INT, w INT);
         INSERT INTO _t_gho SELECT id, v, 7 FROM t;
         RENAME TABLE t TO _t_del, _t_gho TO t;
         INSERT INTO t VALUES (2, 2, 8);",
    )
    .await
    .unwrap();
    let got = items(&mut s, 2).await;
    assert!(
        got.iter()
            .any(|i| matches!(i, StreamItem::SchemaSignal { table, .. } if table.name == "t"))
    );
    assert_eq!(
        changes(&got),
        [(Op::Insert, Some(ColumnValue::Int32(2)))],
        "the shadow's own rows aren't tracked"
    );
    let row = got.iter().find_map(|i| match i {
        StreamItem::Change(e) => e.after.clone(),
        _ => None,
    });
    assert_eq!(row.unwrap().get("w"), Some(&ColumnValue::Int32(8)));
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn a_table_map_or_row_image_the_schema_doesnt_read_halts_the_table() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_map",
        "CREATE TABLE stale (id INT PRIMARY KEY, v INT); CREATE TABLE minimal (id INT PRIMARY KEY, v INT);
         INSERT INTO minimal VALUES (1, 1);",
    )
    .await;
    let c = connector(&pipeline(&n, &["stale", "minimal"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    // The schema is behind the server: its statement is before the stream's start.
    a.query_drop("ALTER TABLE stale ADD COLUMN w INT")
        .await
        .unwrap();
    let mut s = open(&c, &tables).await;
    a.query_drop(
        "INSERT INTO stale VALUES (1, 1, 1);
         SET SESSION binlog_row_image = MINIMAL; UPDATE minimal SET v = 2 WHERE id = 1;
         SET SESSION binlog_row_image = FULL;",
    )
    .await
    .unwrap();
    let got = items(&mut s, 2).await;
    let halts: Vec<(String, String)> = got
        .iter()
        .filter_map(|i| match i {
            StreamItem::TableHalt {
                table,
                reason: HaltReason::SchemaMismatch { detail, .. },
                ..
            } => Some((table.name.clone(), detail.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(halts.len(), 2, "{halts:?}");
    assert!(halts[0].1.contains("3 columns, the schema 2"), "{halts:?}");
    assert!(halts[1].1.contains("isn't FULL"), "{halts:?}");
    assert!(changes(&got).is_empty());
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn an_idle_stream_keeps_alive() {
    require_it!();
    let (n, _a) = fresh("stream_idle", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    let from = c.position_now().await.unwrap();
    // Other tests write meanwhile: a keep-alive comes from a heartbeat or a skipped write.
    let item = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let StreamItem::KeepAlive(p) = s.next().await.unwrap() {
                return p;
            }
        }
    })
    .await
    .expect("a keep-alive within 5 s at a 1 s heartbeat");
    assert!(item.covers(&from).unwrap() || from.covers(&item).unwrap());
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn mysql_payloads_partial_json_and_tagged_gtids() {
    require_it!();
    if target().flavour != Flavour::Mysql {
        return;
    }
    let (n, mut a) = fresh(
        "stream_mysql",
        "CREATE TABLE t (id INT PRIMARY KEY, j JSON); INSERT INTO t VALUES (1, '{\"a\": 1, \"b\": [1, 2]}');",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    let tagged = target().replication == "gtid";
    a.query_drop(
        "SET SESSION binlog_transaction_compression = ON;
         INSERT INTO t VALUES (2, '[]'), (3, '{}');
         SET SESSION binlog_transaction_compression = OFF;
         SET SESSION binlog_row_value_options = 'PARTIAL_JSON';
         UPDATE t SET j = JSON_SET(j, '$.a', 5, '$.b[1]', 9) WHERE id = 1;
         SET SESSION binlog_row_value_options = '';",
    )
    .await
    .unwrap();
    if tagged {
        a.query_drop(
            "SET gtid_next = 'AUTOMATIC:m7a'; INSERT INTO t VALUES (4, 'null'); SET gtid_next = 'AUTOMATIC';",
        )
        .await
        .unwrap();
    }
    let got = items(&mut s, if tagged { 3 } else { 2 }).await;
    let ids: Vec<_> = changes(&got).into_iter().map(|(_, id)| id).collect();
    assert_eq!(
        ids[..3],
        [
            Some(ColumnValue::Int32(2)),
            Some(ColumnValue::Int32(3)),
            Some(ColumnValue::Int32(1))
        ]
    );
    let updated = got
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) if e.op == Op::Update => e.after.clone(),
            _ => None,
        })
        .next()
        .unwrap();
    assert_eq!(
        updated.get("j"),
        Some(&ColumnValue::Json(serde_json::json!({"a": 5, "b": [1, 9]})))
    );
    if tagged {
        let last = commits(&got).pop().unwrap();
        assert!(last.untagged().contains(":m7a:"), "{last}");
        shut(s).await;
        // A tagged set resumes.
        let mut s = c.open_stream(&last, &tables).await.unwrap();
        a.query_drop("INSERT INTO t VALUES (5, 'null')")
            .await
            .unwrap();
        assert_eq!(
            changes(&items(&mut s, 1).await),
            [(Op::Insert, Some(ColumnValue::Int32(5)))]
        );
        shut(s).await;
    } else {
        shut(s).await;
    }
    cleanup(&n).await;
}

#[tokio::test]
async fn a_two_phase_xa_of_tracked_rows_halts_the_stream() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_xa",
        "CREATE TABLE t (id INT PRIMARY KEY); CREATE TABLE other (id INT PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    // Untracked two-phase XA, and a one-phase commit of tracked rows: both stream on.
    a.query_drop(
        "XA START 'u'; INSERT INTO other VALUES (1); XA END 'u'; XA PREPARE 'u'; XA COMMIT 'u';
         XA START 'one'; INSERT INTO t VALUES (1); XA END 'one'; XA COMMIT 'one' ONE PHASE;",
    )
    .await
    .unwrap();
    assert_eq!(
        changes(&items(&mut s, 1).await),
        [(Op::Insert, Some(ColumnValue::Int32(1)))]
    );
    a.query_drop(
        "XA START 'two'; INSERT INTO t VALUES (2); XA END 'two'; XA PREPARE 'two'; XA COMMIT 'two';",
    )
    .await
    .unwrap();
    let e = error(&mut s).await;
    assert!(
        matches!(&e, SourceError::Permanent(m) if m.contains("XA PREPARE")),
        "{e}"
    );
    cleanup(&n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved import `stream::MySqlStream`
error[E0432]: unresolved import `ColumnType`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -1,13 +1,56 @@
 //! `MySqlConnector`: the MySQL or MariaDB source of one pipeline.
 
 use data_avalanche_config::{Mysql, Pipeline, SourceType};
-use data_avalanche_core::{Preflight, SourceError};
+use std::sync::atomic::AtomicU32;
+use std::sync::{Arc, Mutex};
+
+use data_avalanche_core::{Preflight, SchemaDelta, SourceError, TableSchema};
 use mysql_async::prelude::Queryable;
 use tokio::sync::OnceCell;
 
 use crate::connect::Connector;
 use crate::server::{Flavour, Settings, check_settings, grants_replication};
 
+/// What a connector and its stream share.
+#[derive(Debug, Default)]
+pub(crate) struct Shared {
+    /// The change the stream read for a schema signal, kept for the pipeline's `augment` of the
+    /// same signal: one catalog read per statement, and one answer for both.
+    remembered: Mutex<Vec<Remembered>>,
+    /// The connection id of the open binlog dump, 0 without one: the watchdog finds the dump by it.
+    pub(crate) dump: AtomicU32,
+}
+
+#[derive(Debug)]
+struct Remembered {
+    cached: TableSchema,
+    sql: String,
+    change: Change,
+}
+
+/// A channel C answer: the delta, and the schema after it (`None`: the table was dropped).
+pub(crate) type Change = (SchemaDelta, Option<TableSchema>);
+
+impl Shared {
+    /// Keeps the stream's answer for `cached` and `sql`, in place of an older one for the table.
+    pub(crate) fn remember(&self, cached: &TableSchema, sql: &str, change: Change) {
+        let mut r = self.remembered.lock().expect("not poisoned");
+        r.retain(|x| x.cached.table() != cached.table());
+        r.push(Remembered {
+            cached: cached.clone(),
+            sql: sql.to_owned(),
+            change,
+        });
+    }
+
+    /// The stream's answer for this signal, if it read one from the same schema.
+    pub(crate) fn take(&self, cached: &TableSchema, sql: &str) -> Option<Change> {
+        let mut r = self.remembered.lock().expect("not poisoned");
+        let i = r.iter().position(|x| &x.cached == cached && x.sql == sql)?;
+        Some(r.remove(i).change)
+    }
+}
+
 /// The MySQL or MariaDB source of one pipeline (roadmap §3.6). Built from the resolved pipeline
 /// configuration; connections open on first use.
 #[derive(Debug)]
@@ -17,6 +60,7 @@ pub struct MySqlConnector {
     pub(crate) conn: Connector,
     /// `VERSION()`, read once: the dialect every later statement depends on.
     version: OnceCell<String>,
+    pub(crate) shared: Arc<Shared>,
 }
 
 impl MySqlConnector {
@@ -37,6 +81,19 @@ impl MySqlConnector {
             mysql,
             conn: Connector::new(source)?,
             version: OnceCell::new(),
+            shared: Arc::default(),
+        })
+    }
+
+    /// A connector for the stream's catalog reads: its own catalog connection, the same
+    /// configuration, version and shared state.
+    pub(crate) fn sibling(&self) -> Result<Self, SourceError> {
+        Ok(Self {
+            pipeline: self.pipeline.clone(),
+            mysql: self.mysql.clone(),
+            conn: Connector::new(&self.pipeline.source)?,
+            version: self.version.clone(),
+            shared: self.shared.clone(),
         })
     }
 
```

Change `crates/data-avalanche-source-mysql/src/schema.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/schema.rs b/crates/data-avalanche-source-mysql/src/schema.rs
--- a/crates/data-avalanche-source-mysql/src/schema.rs
+++ b/crates/data-avalanche-source-mysql/src/schema.rs
@@ -238,11 +238,27 @@ impl MySqlConnector {
     }
 
     /// Channel C: the change the statement `hint` makes to `cached` (`Schema_Change…` §0, §3,
-    /// §6). A table gone from the catalog is dropped.
+    /// §6). The stream read it already to decode the rows after the statement: that answer, when
+    /// it was read from the same schema; else a fresh read.
     pub async fn augment(
         &self,
         cached: &TableSchema,
         hint: &SchemaHint,
+    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
+        if let SchemaHint::Ddl(sql) = hint
+            && let Some(change) = self.shared.take(cached, sql)
+        {
+            return Ok(change);
+        }
+        self.read_change(cached, hint).await
+    }
+
+    /// The change the statement `hint` makes to `cached`, read from the catalog now. A table gone
+    /// from the catalog is dropped.
+    pub(crate) async fn read_change(
+        &self,
+        cached: &TableSchema,
+        hint: &SchemaHint,
     ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
         let table = cached.table().clone();
         let SchemaHint::Ddl(sql) = hint else {
```

Change `crates/data-avalanche-source-mysql/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/snapshot.rs b/crates/data-avalanche-source-mysql/src/snapshot.rs
--- a/crates/data-avalanche-source-mysql/src/snapshot.rs
+++ b/crates/data-avalanche-source-mysql/src/snapshot.rs
@@ -28,7 +28,7 @@ use mysql_async::{IsolationLevel, TxOpts, Value};
 use crate::catalog::table_key;
 use crate::connector::MySqlConnector;
 use crate::types::{CatalogColumn, plan_column};
-use crate::value::{self, Decode, Input, ValueError};
+use crate::value::{self, Decode, Input};
 
 /// How to read one column of a table.
 #[derive(Debug, Clone)]
@@ -334,18 +334,8 @@ fn decode_row(
     let mut out = Vec::with_capacity(columns.len());
     for (c, v) in columns.iter().zip(&values) {
         let column = c.catalog.name.clone();
-        let decoded = value::decode(&c.decode, Input::Row(v), c.identifier).map_err(|e| {
-            halt(
-                schema,
-                match e {
-                    ValueError::Overflow => HaltReason::NumericOverflow { column },
-                    ValueError::InvalidDate { raw } => HaltReason::InvalidDateValue { column, raw },
-                    ValueError::Unrepresentable(detail) => {
-                        HaltReason::SchemaMismatch { column, detail }
-                    }
-                },
-            )
-        })?;
+        let decoded = value::decode(&c.decode, Input::Row(v), c.identifier)
+            .map_err(|e| halt(schema, value::halt_reason(column, e)))?;
         out.push((c.catalog.name.clone(), decoded));
     }
     Ok(Row(out))
```

In `crates/data-avalanche-source-mysql/src/stream.rs`, add above the test module:

```rust
//! The binlog stream (Data-Flow Phase 2b): a binlog dump read as transactions of row changes,
//! schema signals, truncates and table halts.
//!
//! - **Positions.** A MySQL GTID dump sends the GTID set, a MariaDB one its GTID state through
//!   `@slave_connect_state`; a file/position dump its file and offset. A transaction's commit
//!   advances the position: the set gains its GTID, the state its domain's GTID, the file
//!   position becomes the commit event's end. Every transaction advances it, tracked or not. A
//!   row's `_lsn` is its transaction's own GTID, or in file/position mode the transaction's start.
//! - **Order.** Each event's end offset in its file ([`Offsets`], Q-M7-4) is the `_seq` of the
//!   rows it holds.
//! - **Decoding.** Each tracked table decodes by the as-of-position schema: the pipeline's at
//!   open, then each statement's channel C answer, read here and remembered for the pipeline's
//!   `augment` of the same signal. A table map that doesn't match the schema (columns, types,
//!   names under `binlog_row_metadata = FULL`) halts the table, never misreads a row; so does a
//!   row image that isn't FULL.
//! - **Fail closed.** A compressed transaction that yields less than its size halts the pipeline
//!   (`BINLOG_PAYLOAD_INVALID`, D-V1.6); so does a two-phase XA prepare of tracked rows (they can
//!   roll back after they are logged), an incident event (the server lost events), and any event
//!   type the source doesn't read.
//!
//! [`ChangeStream::next`] is cancel-safe: an event is processed without an await, and the one
//! await inside a transaction (channel C after a statement) keeps its work until it completes.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use data_avalanche_core::position::{
    BinlogFilePos, GtidSet, GtidSource, MariaGtid, MariaGtidState,
};
use data_avalanche_core::{
    CdcEvent, ChangeStream, HaltReason, Op, Row, SchemaHint, SourceError, SourcePosition,
    SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary,
};
use futures_util::StreamExt;
use mysql_async::binlog::events::{
    Event, EventData, OptionalMetadataField, RowsEventData, TableMapEvent, TransactionPayloadEvent,
};
use mysql_async::binlog::value::BinlogValue;
use mysql_async::consts::ColumnType;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, Sid};

use crate::binlog::{MariaGtidEvent, Offsets, decompress, rotate_name, types};
use crate::catalog::table_key;
use crate::connector::MySqlConnector;
use crate::ddl::{Renamed, classify, renamed};
use crate::error::mysql;
use crate::position::Mode;
use crate::server::Flavour;
use crate::types::{CatalogColumn, plan_column};
use crate::value::{Decode, Input, decode_after, halt_reason};

/// One column of a tracked table, as the stream decodes it.
#[derive(Debug, Clone)]
struct Column {
    name: String,
    data_type: String,
    decode: Decode,
    identifier: bool,
}

/// A tracked table: its as-of-position schema and how its columns decode.
#[derive(Debug, Clone)]
struct Tracked {
    schema: TableSchema,
    columns: Vec<Column>,
}

/// What a transaction's commit adds to the position.
#[derive(Debug, Clone)]
enum Gtid {
    Mysql(GtidSource, u64),
    Maria(MariaGtid),
    /// An anonymous transaction (MySQL with GTIDs off).
    None,
}

#[derive(Debug, Clone)]
struct Txn {
    xid: u64,
    commit_ts_micros: i64,
    /// Its own position: the `_lsn` of its rows.
    identity: SourcePosition,
    gtid: Gtid,
    /// Opened by `BEGIN`: it ends at `XID` (or `COMMIT`), not after its first statement.
    with_begin: bool,
    /// An XA transaction's branch (`XA START`, MariaDB's prepared-XA GTID): it ends at its
    /// prepare.
    xa: bool,
    /// Its `Begin` item went out.
    begun: bool,
}

/// A statement's schema signals still to read from the catalog, then what follows them.
#[derive(Debug)]
struct Signals {
    sql: String,
    seq: SourceSeq,
    tables: VecDeque<TableRef>,
    after: Vec<StreamItem>,
}

/// The binlog stream of one [`MySqlConnector::open_stream`] call.
pub struct MySqlStream {
    events: BinlogStream,
    /// Catalog reads for channel C, on a connection of their own.
    catalog: MySqlConnector,
    mode: Mode,
    offsets: Offsets,
    position: SourcePosition,
    tables: HashMap<TableRef, Tracked>,
    txn: Option<Txn>,
    ready: VecDeque<StreamItem>,
    signals: Option<Signals>,
    /// Ids for anonymous transactions.
    anonymous: u64,
}

/// A GTID set as the dump request's sid blocks: one per server UUID and tag.
fn sids(set: &GtidSet) -> Result<Vec<Sid<'static>>, String> {
    let text = set.to_string();
    let mut out = vec![];
    for entry in text.split(',').filter(|e| !e.is_empty()) {
        let mut parts = entry.split(':');
        let uuid = parts.next().unwrap_or_default();
        let mut groups: Vec<(Option<&str>, Vec<&str>)> = vec![(None, vec![])];
        for p in parts {
            if p.bytes().all(|b| b.is_ascii_digit() || b == b'-') {
                groups.last_mut().expect("one group").1.push(p);
            } else {
                groups.push((Some(p), vec![]));
            }
        }
        for (tag, intervals) in groups.into_iter().filter(|(_, i)| !i.is_empty()) {
            let sid = match tag {
                Some(t) => format!("{uuid}:{t}:{}", intervals.join(":")),
                None => format!("{uuid}:{}", intervals.join(":")),
            };
            out.push(sid.parse().map_err(|e| format!("GTID {sid}: {e}"))?);
        }
    }
    Ok(out)
}

/// The binlog column types a column of `data_type` arrives as.
fn arrives_as(data_type: &str) -> &'static [ColumnType] {
    use ColumnType::*;
    match data_type {
        "tinyint" => &[MYSQL_TYPE_TINY],
        "smallint" => &[MYSQL_TYPE_SHORT],
        "mediumint" => &[MYSQL_TYPE_INT24],
        "int" | "integer" => &[MYSQL_TYPE_LONG],
        "bigint" => &[MYSQL_TYPE_LONGLONG],
        "float" => &[MYSQL_TYPE_FLOAT],
        "double" | "real" => &[MYSQL_TYPE_DOUBLE],
        "decimal" | "numeric" => &[MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_DECIMAL],
        "char" | "varchar" | "binary" | "varbinary" | "uuid" | "inet4" | "inet6" => {
            &[MYSQL_TYPE_STRING, MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING]
        }
        "enum" => &[MYSQL_TYPE_ENUM, MYSQL_TYPE_STRING],
        "set" => &[MYSQL_TYPE_SET, MYSQL_TYPE_STRING],
        "tinytext" | "text" | "mediumtext" | "longtext" | "tinyblob" | "blob" | "mediumblob"
        | "longblob" => &[
            MYSQL_TYPE_BLOB,
            MYSQL_TYPE_TINY_BLOB,
            MYSQL_TYPE_MEDIUM_BLOB,
            MYSQL_TYPE_LONG_BLOB,
        ],
        "json" => &[MYSQL_TYPE_JSON, MYSQL_TYPE_BLOB],
        "date" => &[MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE],
        "time" => &[MYSQL_TYPE_TIME2, MYSQL_TYPE_TIME],
        "datetime" => &[MYSQL_TYPE_DATETIME2, MYSQL_TYPE_DATETIME],
        "timestamp" => &[MYSQL_TYPE_TIMESTAMP2, MYSQL_TYPE_TIMESTAMP],
        "year" => &[MYSQL_TYPE_YEAR],
        "bit" => &[MYSQL_TYPE_BIT],
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" | "geomcollection" => &[MYSQL_TYPE_GEOMETRY],
        "vector" => &[
            MYSQL_TYPE_VECTOR,
            MYSQL_TYPE_STRING,
            MYSQL_TYPE_VARCHAR,
            MYSQL_TYPE_VAR_STRING,
            MYSQL_TYPE_BLOB,
        ],
        _ => &[],
    }
}

/// Whether the table map describes the rows the schema decodes: as many columns, each of a
/// binlog type its type arrives as, and under `binlog_row_metadata = FULL` the same names.
fn check_map(tme: &TableMapEvent<'_>, t: &Tracked) -> Result<(), String> {
    let n = tme.columns_count() as usize;
    if n != t.columns.len() {
        return Err(format!(
            "the binlog's rows have {n} columns, the schema {}",
            t.columns.len()
        ));
    }
    for (i, c) in t.columns.iter().enumerate() {
        let ty = tme
            .get_column_type(i)
            .map_err(|e| format!("column {}: {e}", c.name))?
            .ok_or_else(|| format!("column {}: no binlog type", c.name))?;
        if !arrives_as(&c.data_type).contains(&ty) {
            return Err(format!(
                "column {}: the binlog's rows have {ty:?}, the schema {}",
                c.name, c.data_type
            ));
        }
    }
    for field in tme.iter_optional_meta() {
        if let Ok(OptionalMetadataField::ColumnName(names)) = field {
            for (i, name) in names.iter_names().enumerate() {
                let name = name.map_err(|e| format!("column names: {e}"))?;
                let want = t.columns.get(i).map(|c| c.name.as_str());
                if Some(name.name().as_ref()) != want {
                    return Err(format!(
                        "column {}: the binlog names it {:?}",
                        want.unwrap_or("?"),
                        name.name()
                    ));
                }
            }
        }
    }
    Ok(())
}

/// A pipeline halt for a compressed transaction that didn't yield its events (D-V1.6).
fn payload_invalid(detail: String) -> SourceError {
    SourceError::Permanent(format!("BINLOG_PAYLOAD_INVALID: {detail}"))
}

fn permanent(detail: impl std::fmt::Display) -> SourceError {
    SourceError::Permanent(detail.to_string())
}

/// The statement's leading word, upper case.
fn verb(sql: &str) -> String {
    sql.trim_start()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase()
}

impl MySqlConnector {
    /// How the stream decodes `schema`: each column planned from the schema alone
    /// ([`CatalogColumn::from_schema`]) with the pipeline's settings; a plan that doesn't give the
    /// schema's type halts the table.
    fn track(&self, schema: &TableSchema) -> Result<Tracked, HaltReason> {
        let tables = &self.pipeline.tables;
        let key = table_key(schema.table());
        let mut columns = Vec::with_capacity(schema.columns().len());
        for col in schema.columns() {
            let c = CatalogColumn::from_schema(col);
            let over = tables
                .column_type_overrides
                .get(&format!("{key}.{}", col.name));
            let identifier = schema.is_identifier(&col.name);
            let p = plan_column(
                &c,
                self.tinyint1_as_bool(),
                tables.numeric_overflow_action,
                over,
                identifier,
            )?;
            if p.value_type != col.value_type {
                return Err(HaltReason::SchemaMismatch {
                    column: col.name.clone(),
                    detail: format!(
                        "the schema's {} plans as {:?}, not {:?}",
                        col.source_type, p.value_type, col.value_type
                    ),
                });
            }
            columns.push(Column {
                name: col.name.clone(),
                data_type: c.data_type,
                decode: p.decode,
                identifier,
            });
        }
        Ok(Tracked {
            schema: schema.clone(),
            columns,
        })
    }

    /// Opens the binlog dump after `from` for `tables` as of that position (Data-Flow Phase 2b):
    /// the heartbeat period, the replica id `source.mysql.server_id`, and the position in its
    /// dialect.
    pub async fn open_stream(
        &self,
        from: &SourcePosition,
        tables: &[TableSchema],
    ) -> Result<MySqlStream, SourceError> {
        let flavour = self.flavour().await?;
        let mode = Mode::of(flavour, self.mysql.replication_type);
        let catalog = self.sibling()?;
        let mut tracked = HashMap::new();
        for s in tables {
            let t = catalog.track(s).map_err(|reason| SourceError::TableHalt {
                table: s.table().clone(),
                reason,
            })?;
            tracked.insert(s.table().clone(), t);
        }
        let at = from.to_tagged();
        let err = |e| mysql(e, Some(&at));
        let mut conn = self.conn.connect().await?;
        let id = conn.id();
        conn.query_drop(format!(
            "SET @master_heartbeat_period = {}",
            self.mysql.heartbeat.as_nanos()
        ))
        .await
        .map_err(err)?;
        if flavour == Flavour::Mariadb {
            conn.query_drop("SET @mariadb_slave_capability = 4")
                .await
                .map_err(err)?;
        }
        let file;
        let mut req = BinlogStreamRequest::new(self.mysql.server_id);
        let offsets = match (mode, from) {
            (Mode::MysqlGtid, SourcePosition::MySqlGtid(set)) => {
                req = req.with_gtid().with_gtid_set(sids(set).map_err(permanent)?);
                Offsets::new("", 4)
            }
            (Mode::MariadbGtid, SourcePosition::MariaDbGtid(state)) => {
                conn.query_drop(format!("SET @slave_connect_state = '{state}'"))
                    .await
                    .map_err(err)?;
                conn.query_drop("SET @slave_gtid_strict_mode = 1")
                    .await
                    .map_err(err)?;
                Offsets::new("", 4)
            }
            (Mode::File, SourcePosition::BinlogFile(p)) => {
                file = p.file().as_bytes().to_vec();
                req = req.with_filename(&file[..]).with_pos(p.pos());
                Offsets::new(p.file(), p.pos())
            }
            _ => {
                return Err(permanent(format!(
                    "the stream position {at} isn't a {mode:?} position"
                )));
            }
        };
        let events = conn.get_binlog_stream(req).await.map_err(err)?;
        self.shared.dump.store(id, Ordering::Relaxed);
        Ok(MySqlStream {
            events,
            catalog,
            mode,
            offsets,
            position: from.clone(),
            tables: tracked,
            txn: None,
            ready: VecDeque::new(),
            signals: None,
            anonymous: 0,
        })
    }
}

impl MySqlStream {
    fn seq(&self, end: u64) -> Result<SourceSeq, SourceError> {
        SourceSeq::binlog(self.offsets.file(), end).map_err(permanent)
    }

    /// Where items of the open transaction go: after a statement's pending signals, if any.
    fn emit(&mut self, item: StreamItem) {
        match &mut self.signals {
            Some(s) => s.after.push(item),
            None => self.ready.push_back(item),
        }
    }

    /// The open transaction's `Begin`, once.
    fn begin(&mut self) -> Result<(), SourceError> {
        let txn = self
            .txn
            .as_mut()
            .ok_or_else(|| permanent("a change outside a transaction"))?;
        if !txn.begun {
            txn.begun = true;
            let item = StreamItem::Begin {
                xid: txn.xid,
                commit_ts_micros: txn.commit_ts_micros,
            };
            self.emit(item);
        }
        Ok(())
    }

    fn halt(
        &mut self,
        table: &TableRef,
        seq: SourceSeq,
        reason: HaltReason,
    ) -> Result<(), SourceError> {
        self.begin()?;
        self.tables.remove(table);
        self.emit(StreamItem::TableHalt {
            table: table.clone(),
            seq,
            reason,
        });
        Ok(())
    }

    /// A transaction starts at `start`, the end of the event before its GTID event.
    fn start(&mut self, xid: u64, ts: i64, gtid: Gtid, start: u64) -> Result<(), SourceError> {
        if self.txn.is_some() {
            return Err(permanent("a GTID event inside a transaction"));
        }
        let identity = match (&gtid, self.mode) {
            (_, Mode::File) | (Gtid::None, _) => SourcePosition::BinlogFile(
                BinlogFilePos::new(self.offsets.file(), start).map_err(permanent)?,
            ),
            (Gtid::Mysql(source, gno), _) => {
                let mut set = GtidSet::default();
                set.insert(source, *gno);
                SourcePosition::MySqlGtid(set)
            }
            (Gtid::Maria(g), _) => {
                let mut state = MariaGtidState::default();
                state.advance(*g);
                SourcePosition::MariaDbGtid(state)
            }
        };
        self.txn = Some(Txn {
            xid,
            commit_ts_micros: ts,
            identity,
            gtid,
            with_begin: false,
            xa: false,
            begun: false,
        });
        Ok(())
    }

    /// The open transaction commits with the event ending at `end`: the position advances, and a
    /// transaction with items ends with its `Commit`.
    fn commit(&mut self, end: u64) -> Result<(), SourceError> {
        let txn = self
            .txn
            .take()
            .ok_or_else(|| permanent("a commit outside a transaction"))?;
        self.position = match (&self.position, &txn.gtid, self.mode) {
            (_, _, Mode::File) => SourcePosition::BinlogFile(
                BinlogFilePos::new(self.offsets.file(), end).map_err(permanent)?,
            ),
            (SourcePosition::MySqlGtid(set), Gtid::Mysql(source, gno), _) => {
                let mut set = set.clone();
                set.insert(source, *gno);
                SourcePosition::MySqlGtid(set)
            }
            (SourcePosition::MariaDbGtid(state), Gtid::Maria(g), _) => {
                let mut state = state.clone();
                state.advance(*g);
                SourcePosition::MariaDbGtid(state)
            }
            (p, g, m) => {
                return Err(permanent(format!(
                    "a {g:?} transaction in a {m:?} stream at {}",
                    p.to_tagged()
                )));
            }
        };
        if txn.begun {
            let item = StreamItem::Commit(TransactionBoundary {
                xid: txn.xid,
                commit_pos: self.position.clone(),
                commit_ts_micros: txn.commit_ts_micros,
            });
            self.emit(item);
        }
        Ok(())
    }

    fn on_event(&mut self, ev: Event) -> Result<(), SourceError> {
        let h = ev.header();
        let (raw, log_pos, size) = (h.event_type_raw(), h.log_pos(), h.event_size());
        match raw {
            types::HEARTBEAT | types::HEARTBEAT_V2 => {
                self.offsets.heartbeat().map_err(payload_invalid)?;
                if self.txn.is_none() && self.signals.is_none() {
                    self.ready
                        .push_back(StreamItem::KeepAlive(self.position.clone()));
                }
                return Ok(());
            }
            types::ROTATE => {
                let Some(EventData::RotateEvent(r)) = ev.read_data().map_err(permanent)? else {
                    return Err(permanent("an unreadable ROTATE event"));
                };
                let name = String::from_utf8_lossy(rotate_name(r.name_raw())).into_owned();
                return self
                    .offsets
                    .rotate(&name, r.position())
                    .map_err(payload_invalid);
            }
            // The format description a dump resends at its start isn't in the file there.
            types::FORMAT_DESCRIPTION if log_pos == 0 => return Ok(()),
            _ => {}
        }
        let start = self.offsets.end();
        let end = self.offsets.event(log_pos, size).map_err(payload_invalid)?;
        match raw {
            types::STOP
            | types::INTVAR
            | types::RAND
            | types::USER_VAR
            | types::FORMAT_DESCRIPTION
            | types::IGNORABLE
            | types::ROWS_QUERY
            | types::PREVIOUS_GTIDS
            | types::TRANSACTION_CONTEXT
            | types::VIEW_CHANGE
            | types::TABLE_MAP
            | types::MARIADB_ANNOTATE_ROWS
            | types::MARIADB_BINLOG_CHECKPOINT
            | types::MARIADB_GTID_LIST
            | types::MARIADB_START_ENCRYPTION => Ok(()),
            types::GTID | types::GTID_TAGGED | types::ANONYMOUS_GTID => {
                let (g, anonymous) = match ev.read_data().map_err(permanent)? {
                    Some(EventData::GtidEvent(g)) => (g, false),
                    Some(EventData::AnonymousGtidEvent(g)) => (g.0, true),
                    other => return Err(permanent(format!("a GTID event reads as {other:?}"))),
                };
                let ts = match g.immediate_commit_timestamp() {
                    0 => i64::from(h.timestamp()) * 1_000_000,
                    us => us as i64,
                };
                if anonymous {
                    self.anonymous += 1;
                    return self.start(self.anonymous, ts, Gtid::None, start);
                }
                if g.gno() == 0 {
                    return Err(permanent("a GTID event with transaction number 0"));
                }
                let source = GtidSource {
                    uuid: uuid::Uuid::from_bytes(g.sid()),
                    tag: g.tag().map(|t| t.as_str().to_owned()),
                };
                self.start(g.gno(), ts, Gtid::Mysql(source, g.gno()), start)
            }
            types::MARIADB_GTID => {
                let g = MariaGtidEvent::parse(h.server_id(), ev.data()).map_err(permanent)?;
                let ts = i64::from(h.timestamp()) * 1_000_000;
                self.start(g.gtid.seq, ts, Gtid::Maria(g.gtid), start)?;
                if g.flags & MariaGtidEvent::PREPARED_XA != 0 {
                    self.txn.as_mut().expect("started").xa = true;
                }
                Ok(())
            }
            types::QUERY => self.query(&ev, end),
            types::MARIADB_QUERY_COMPRESSED => {
                let plain = decompress(&ev)
                    .map_err(permanent)?
                    .expect("a compressed type");
                self.query(&plain, end)
            }
            types::XID => self.commit(end),
            types::WRITE_ROWS_V1
            | types::UPDATE_ROWS_V1
            | types::DELETE_ROWS_V1
            | types::WRITE_ROWS
            | types::UPDATE_ROWS
            | types::DELETE_ROWS
            | types::PARTIAL_UPDATE_ROWS => self.rows(&ev, end),
            types::MARIADB_WRITE_ROWS_COMPRESSED_V1
            | types::MARIADB_UPDATE_ROWS_COMPRESSED_V1
            | types::MARIADB_DELETE_ROWS_COMPRESSED_V1 => {
                let plain = decompress(&ev)
                    .map_err(permanent)?
                    .expect("a compressed type");
                self.rows(&plain, end)
            }
            types::TRANSACTION_PAYLOAD => {
                let p: TransactionPayloadEvent<'_> = ev.read_event().map_err(permanent)?;
                self.offsets
                    .payload(p.uncompressed_size())
                    .map_err(payload_invalid)
            }
            types::XA_PREPARE => self.xa_prepare(&ev, end),
            types::INCIDENT => Err(permanent(format!(
                "an incident event at {}:{end}: the server lost events it didn't log",
                self.offsets.file()
            ))),
            other => Err(permanent(format!(
                "a binlog event of type {other} at {}:{end}, which the source doesn't read",
                self.offsets.file()
            ))),
        }
    }

    /// An XA branch's prepare ends its transaction. A one-phase commit (MySQL) commits it; a
    /// two-phase prepare with rows of a tracked table halts the pipeline: they can still roll back,
    /// and the source doesn't hold prepared rows for their outcome (D-M7a). Without them, it ends
    /// like any untracked transaction.
    fn xa_prepare(&mut self, ev: &Event, end: u64) -> Result<(), SourceError> {
        let one_phase = ev.data().first().is_some_and(|b| *b != 0);
        let tracked = self.txn.as_ref().is_some_and(|t| t.begun);
        if tracked && !one_phase {
            return Err(permanent(format!(
                "an XA PREPARE at {}:{end} holds rows of tracked tables: they can roll back \
                 after they are logged, and the source doesn't replicate two-phase XA",
                self.offsets.file()
            )));
        }
        self.commit(end)
    }

    fn query(&mut self, ev: &Event, end: u64) -> Result<(), SourceError> {
        let Some(EventData::QueryEvent(q)) = ev.read_data().map_err(permanent)? else {
            return Err(permanent("an unreadable QUERY event"));
        };
        let sql = q.query().into_owned();
        let db = q.schema().into_owned();
        if self.txn.is_none() {
            // No GTID event before it: an anonymous transaction.
            self.anonymous += 1;
            let ts = i64::from(ev.header().timestamp()) * 1_000_000;
            self.start(
                self.anonymous,
                ts,
                Gtid::None,
                end - u64::from(ev.header().event_size()),
            )?;
        }
        match sql.trim().to_ascii_uppercase().as_str() {
            "BEGIN" => {
                if let Some(t) = &mut self.txn {
                    t.with_begin = true;
                }
                return Ok(());
            }
            // MariaDB ends a transaction on a non-transactional engine with COMMIT; a ROLLBACK is
            // logged when the transaction's non-transactional changes stay.
            "COMMIT" | "ROLLBACK" => return self.commit(end),
            _ => {}
        }
        match verb(&sql).as_str() {
            // A branch's rows come between its START and END, and its prepare ends it; its
            // COMMIT or ROLLBACK is a transaction of its own, with no rows.
            "XA" => {
                let word = sql.split_whitespace().nth(1).unwrap_or_default();
                return match word.to_ascii_uppercase().as_str() {
                    "START" | "BEGIN" => {
                        if let Some(t) = &mut self.txn {
                            t.xa = true;
                        }
                        Ok(())
                    }
                    "COMMIT" | "ROLLBACK" => self.commit(end),
                    _ => Ok(()),
                };
            }
            "SAVEPOINT" | "ROLLBACK" | "RELEASE" => return Ok(()),
            _ => {}
        }
        let seq = self.seq(end)?;
        self.statement(&sql, &db, seq)?;
        if !self.txn.as_ref().is_some_and(|t| t.with_begin) {
            self.commit(end)?;
        }
        Ok(())
    }

    /// Routes a statement (channel B): halts, truncates and schema signals for the tracked tables
    /// it touches.
    fn statement(&mut self, sql: &str, db: &str, seq: SourceSeq) -> Result<(), SourceError> {
        let d = classify(sql, db);
        if d.unreadable {
            return Err(permanent(format!(
                "a table statement the source can't read the tables of: {sql}"
            )));
        }
        let tracked: Vec<TableRef> = self.tables.keys().cloned().collect();
        let source_db = self.catalog.pipeline.source.db.clone();
        let mut halts: Vec<(TableRef, HaltReason)> = vec![];
        if d.dropped_database.as_deref() == Some(source_db.as_str()) {
            halts.extend(
                tracked
                    .iter()
                    .map(|t| (t.clone(), HaltReason::TableDropped)),
            );
        }
        for (t, op) in &d.unlogged {
            halts.push((
                t.clone(),
                HaltReason::UnsafeDdl {
                    detail: format!("{op} changed rows without row events; re-snapshot the table"),
                },
            ));
        }
        halts.extend(
            d.dropped
                .iter()
                .map(|t| (t.clone(), HaltReason::TableDropped)),
        );
        let mut signals: Vec<TableRef> = vec![];
        for (t, r) in renamed(&d.renamed, &tracked) {
            match r {
                Renamed::Gone => halts.push((t, HaltReason::TableDropped)),
                Renamed::Swapped { .. } => signals.push(t),
                Renamed::Kept => {}
            }
        }
        halts.retain(|(t, _)| self.tables.contains_key(t));
        let halted = |t: &TableRef| halts.iter().any(|(h, _)| h == t);
        for a in &d.altered {
            if self.tables.contains_key(&a.table)
                && !halted(&a.table)
                && !signals.contains(&a.table)
            {
                signals.push(a.table.clone());
            }
        }
        signals.retain(|t| !halted(t));
        let truncated: Vec<TableRef> = d
            .truncated
            .iter()
            .filter(|t| self.tables.contains_key(t) && !halted(t))
            .cloned()
            .collect();
        for (t, reason) in halts {
            if self.tables.contains_key(&t) {
                self.halt(&t, seq, reason)?;
            }
        }
        if !truncated.is_empty() {
            self.begin()?;
            self.emit(StreamItem::Truncate {
                tables: truncated,
                seq,
            });
        }
        if !signals.is_empty() {
            self.begin()?;
            self.signals = Some(Signals {
                sql: sql.to_owned(),
                seq,
                tables: signals.into(),
                after: vec![],
            });
        }
        Ok(())
    }

    /// Reads the pending signals' changes from the catalog, one table at a time: each answer
    /// re-plans the table, is remembered for the pipeline, and goes out as a signal.
    async fn read_signals(&mut self) -> Result<(), SourceError> {
        loop {
            let Some(s) = &self.signals else {
                return Ok(());
            };
            let Some(table) = s.tables.front().cloned() else {
                let s = self.signals.take().expect("pending");
                self.ready.extend(s.after);
                return Ok(());
            };
            let (sql, seq) = (s.sql.clone(), s.seq);
            let Some(cached) = self.tables.get(&table).map(|t| t.schema.clone()) else {
                self.signals.as_mut().expect("pending").tables.pop_front();
                continue;
            };
            let hint = SchemaHint::Ddl(sql.clone());
            let read = self.catalog.read_change(&cached, &hint).await;
            self.signals.as_mut().expect("pending").tables.pop_front();
            let item = match read {
                Ok((delta, now)) => {
                    self.catalog
                        .shared
                        .remember(&cached, &sql, (delta, now.clone()));
                    match now.map(|n| self.catalog.track(&n)) {
                        None => {
                            self.tables.remove(&table);
                        }
                        Some(Ok(t)) => {
                            self.tables.insert(table.clone(), t);
                        }
                        Some(Err(reason)) => {
                            self.tables.remove(&table);
                            self.ready
                                .push_back(StreamItem::TableHalt { table, seq, reason });
                            continue;
                        }
                    }
                    StreamItem::SchemaSignal { table, seq, hint }
                }
                Err(SourceError::TableHalt { reason, .. }) => {
                    self.tables.remove(&table);
                    StreamItem::TableHalt { table, seq, reason }
                }
                Err(e) => return Err(e),
            };
            self.ready.push_back(item);
        }
    }

    fn rows(&mut self, ev: &Event, end: u64) -> Result<(), SourceError> {
        let Some(EventData::RowsEvent(re)) = ev.read_data().map_err(permanent)? else {
            return Err(permanent("an unreadable rows event"));
        };
        let tme = self
            .events
            .get_tme(re.table_id())
            .ok_or_else(|| permanent("a rows event without its table map"))?
            .clone();
        let table = TableRef::new(tme.database_name(), tme.table_name());
        let Some(tracked) = self.tables.get(&table).cloned() else {
            return Ok(());
        };
        let seq = self.seq(end)?;
        let mismatch = |detail: String| HaltReason::SchemaMismatch {
            column: String::new(),
            detail,
        };
        if let Err(detail) = check_map(&tme, &tracked) {
            return self.halt(
                &table,
                seq,
                mismatch(format!("{detail}; re-snapshot the table")),
            );
        }
        let n = tracked.columns.len();
        let full = re
            .columns_before_image()
            .is_none_or(|b| b.iter().take(n).all(|x| *x))
            && re
                .columns_after_image()
                .is_none_or(|b| b.iter().take(n).all(|x| *x));
        if !full {
            return self.halt(
                &table,
                seq,
                mismatch("a row image that isn't FULL (a session set binlog_row_image)".into()),
            );
        }
        let op = match &re {
            RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => Op::Insert,
            RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => Op::Delete,
            _ => Op::Update,
        };
        self.begin()?;
        let txn = self.txn.clone().expect("begun");
        for row in re.rows(&tme) {
            let (before, after) = row.map_err(permanent)?;
            let before: Option<Vec<BinlogValue<'static>>> = before.map(|r| r.unwrap());
            let after: Option<Vec<BinlogValue<'static>>> = after.map(|r| r.unwrap());
            let decode = |values: &[BinlogValue<'static>],
                          prior: Option<&[BinlogValue<'static>]>| {
                tracked
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let v = values
                            .get(i)
                            .ok_or_else(|| mismatch(format!("a row without column {}", c.name)))?;
                        let b = prior.and_then(|p| p.get(i)).map(Input::Binlog);
                        decode_after(&c.decode, b, Input::Binlog(v), c.identifier)
                            .map(|value| (c.name.clone(), value))
                            .map_err(|e| halt_reason(c.name.clone(), e))
                    })
                    .collect::<Result<Vec<_>, HaltReason>>()
                    .map(Row)
            };
            let images = (|| {
                let b = before.as_deref().map(|v| decode(v, None)).transpose()?;
                let a = after
                    .as_deref()
                    .map(|v| decode(v, before.as_deref()))
                    .transpose()?;
                Ok::<_, HaltReason>((b, a))
            })();
            match images {
                Ok((before, after)) => self.emit(StreamItem::Change(CdcEvent {
                    op,
                    table: table.clone(),
                    before,
                    after,
                    source_pos: txn.identity.clone(),
                    change_seq: seq,
                    xid: txn.xid,
                    commit_ts_micros: txn.commit_ts_micros,
                })),
                Err(reason) => return self.halt(&table, seq, reason),
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for MySqlStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlStream")
            .field("mode", &self.mode)
            .field("offsets", &self.offsets)
            .field("position", &self.position)
            .field("txn", &self.txn)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ChangeStream for MySqlStream {
    async fn next(&mut self) -> Result<StreamItem, SourceError> {
        loop {
            if let Some(item) = self.ready.pop_front() {
                return Ok(item);
            }
            if self.signals.is_some() {
                self.read_signals().await?;
                continue;
            }
            let at = self.position.to_tagged();
            let ev = match self.events.next().await {
                Some(Ok(ev)) => ev,
                Some(Err(e)) => return Err(mysql(e, Some(&at))),
                None => {
                    return Err(SourceError::Transient(
                        "the server ended the binlog stream".into(),
                    ));
                }
            };
            self.on_event(ev)?;
        }
    }

    async fn acknowledge(&mut self, _: &SourcePosition) -> Result<(), SourceError> {
        Ok(())
    }

    async fn close(self: Box<Self>) -> Result<(), SourceError> {
        self.catalog.shared.dump.store(0, Ordering::Relaxed);
        self.events.close().await.map_err(|e| mysql(e, None))
    }
}

```

Change `crates/data-avalanche-source-mysql/src/value.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/value.rs b/crates/data-avalanche-source-mysql/src/value.rs
--- a/crates/data-avalanche-source-mysql/src/value.rs
+++ b/crates/data-avalanche-source-mysql/src/value.rs
@@ -9,7 +9,7 @@
 use std::net::Ipv4Addr;
 
 use chrono::{DateTime, NaiveDate};
-use data_avalanche_core::{ColumnValue, ColumnValueType, Decimal, DecimalError};
+use data_avalanche_core::{ColumnValue, ColumnValueType, Decimal, DecimalError, HaltReason};
 use mysql_async::Value;
 use mysql_async::binlog::value::BinlogValue;
 
@@ -640,6 +640,15 @@ fn value(d: &Decode, raw: Raw, identifier: bool) -> Result<ColumnValue, ValueErr
     })
 }
 
+/// The table halt a value that can't be decoded in `column` causes.
+pub(crate) fn halt_reason(column: String, e: ValueError) -> HaltReason {
+    match e {
+        ValueError::Overflow => HaltReason::NumericOverflow { column },
+        ValueError::InvalidDate { raw } => HaltReason::InvalidDateValue { column, raw },
+        ValueError::Unrepresentable(detail) => HaltReason::SchemaMismatch { column, detail },
+    }
+}
+
 /// Decodes one value of a column planned as `d`; `identifier` marks a column of the table's
 /// identifier.
 pub fn decode(d: &Decode, input: Input<'_>, identifier: bool) -> Result<ColumnValue, ValueError> {
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 70 tests.

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql stream:: || break; done`

Expected: PASS, 7 tests on each target (`test-infra/up.sh` first).

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql || break; done`

Expected: PASS, 33 tests on each target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche-source-mysql/Cargo.toml \
  crates/data-avalanche-source-mysql/src/connector.rs \
  crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/schema.rs \
  crates/data-avalanche-source-mysql/src/snapshot.rs \
  crates/data-avalanche-source-mysql/src/stream.rs \
  crates/data-avalanche-source-mysql/src/value.rs \
  crates/data-avalanche-source-mysql/tests/mysql/main.rs \
  crates/data-avalanche-source-mysql/tests/mysql/stream.rs
git commit -m "feat(source-mysql): the binlog stream — transactions and positions in three dialects, statements routed, table maps checked, payloads guarded, fail-closed events"
```

---

### Task 14: The start position, the dump watch, and the `SourceConnector` impl

**Files:**
- Modify: `crates/data-avalanche-source-mysql/src/connector.rs`
- Modify: `crates/data-avalanche-source-mysql/src/lib.rs`
- Modify: `crates/data-avalanche-source-mysql/src/position.rs`
- Create: `crates/data-avalanche-source-mysql/src/start.rs`
- Modify: `crates/data-avalanche-source-mysql/src/stream.rs`
- Test: `crates/data-avalanche-source-mysql/tests/binlog_purge.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mariadb_compress.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/mysql/common.rs`
- Modify: `crates/data-avalanche-source-mysql/tests/mysql/main.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/mysql/preflight.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/start.rs`

**Interfaces:**
- Consumes:
  - `position_now`, `server_identity`, `retention`, `purged`, `Mode` (Task 5); `open_stream`,
    `Shared::dump` (Task 13); every inherent method of Tasks 4–13 for the trait.
  - `data_avalanche_core::{Checkpoint, Prepared, RetainedLog, SourceConnector}` (Task 1);
    `data_avalanche_config::BinlogPurgeAction`.
- Produces:
  - `data-avalanche-source-mysql/src/lib.rs`:
    - `pub mod start;`
  - `data-avalanche-source-mysql/src/position.rs`:
    - `pub fn dialect(self) -> &'static str`
  - `data-avalanche-source-mysql/src/start.rs`:
    - `pub fn dialect_change(mode: Mode, checkpoint: &SourcePosition) -> Option<String>`
    - `pub fn same_server(checkpoint: &Checkpoint, now: &ServerIdentity) -> Result<(), SourceError>`
    - `pub fn purge_action(action: BinlogPurgeAction, e: SourceError) -> SourceError`
    - `pub async fn prepare(&self, checkpoint: Option<&Checkpoint>) -> Result<Prepared, SourceError>`
    - `pub fn dump_id(&self) -> Option<u32>`
    - `pub async fn retained(&self) -> Result<Option<RetainedLog>, SourceError>`

`prepare`, `retained`, the rest of the trait (Data-Flow Phase 2b, G-26, Q-M7-2, Q-M7a-6;
D-M7a.20 to D-M7a.22).

- **`prepare`**: a first start snapshots from the server's write position and records the
  server. A checkpoint in another dialect than the pipeline reads now (`replication_type`
  changed, or another flavour of server) follows `binlog_purge_action` (Q-M7a-6), and so does a
  checkpoint the server purged. In file mode a checkpoint recorded on another server, or on none,
  halts the pipeline: `BINLOG_SOURCE_CHANGED: …` (Q-M7-2). GTID positions carry across servers.
  Otherwise the stream resumes.
- **The purge action is one rule** (`purge_action`), applied at `prepare` and to the dump's own
  refusal (1236), at open and mid-stream: `re_snapshot` keeps `PositionUnreachable` (the worker
  re-snapshots from a new position), `error` turns it into `Permanent`. Without the second
  place, a purge between `prepare` and the dump would re-snapshot under `error`.
- **`retained`** (DA-SRC-16, V-121): the server keeps no log for the pipeline (its binlogs expire
  by its own policy), so `bytes` is 0 and `max_wal_lag_bytes` stays PostgreSQL's. `streamed`
  reads whether the server still runs this stream's dump: its connection id in
  `information_schema.PROCESSLIST` with a `Binlog Dump` command. A user without `PROCESS` sees
  its own connections (measured); after a `KILL` the watch reads `false` before the stream
  hears of it. The spec's wording ("no dump of this pipeline's `server_id`") is amended to the
  connection id (patch below, for approval).
- `reset_position` and `current_position` are the write position; `teardown` has nothing to
  remove; `fetch_rows` is `Permanent` (`toast_strategy: re_query` is PostgreSQL's; a `FULL`
  image carries every column).

Two test binaries change server-wide state, so cargo runs each alone, never beside `tests/mysql`
(the PostgreSQL crate's convention, `slot_invalidation.rs`): `binlog_purge` (`PURGE BINARY LOGS`;
MariaDB keeps the binlog it just closed until its binlog checkpoint moves, so the purge is
retried for up to 5 s) and `mariadb_compress` (`log_bin_compress` on, which compresses every
session's events, restored to the server default before it asserts; `SHOW BINLOG EVENTS` proves
the four compressed types were written).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -15,6 +15,7 @@ pub mod position;
 pub mod schema;
 pub mod server;
 mod snapshot;
+pub mod start;
 mod stream;
 pub mod types;
 pub mod value;
@@ -27,6 +28,7 @@ pub use ddl::{Altered, Ddl, Renamed, Scope, classify, renamed, shadow_of};
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
 pub use schema::{diff, narrowed, scope, scope_of};
 pub use server::{Flavour, Settings, check_settings, grants_replication, version_of};
+pub use start::{dialect_change, purge_action, same_server};
 pub use stream::MySqlStream;
 pub use types::{CatalogColumn, ColumnPlan, members, plan_column};
 pub use value::{Charset, Decode, Input, ValueError, decode, decode_after, int_type};
```

Change `crates/data-avalanche-source-mysql/tests/mysql/main.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/main.rs b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/main.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
@@ -9,6 +9,7 @@ mod preflight;
 mod resolve;
 mod schema;
 mod snapshot;
+mod start;
 mod stream;
 mod tls;
 mod values;
```

Create `crates/data-avalanche-source-mysql/src/start.rs` with only its test module; Step 3 adds the implementation above it:

```rust
#[cfg(test)]
mod tests {
    use data_avalanche_core::position::BinlogFilePos;

    use super::*;

    fn file() -> SourcePosition {
        SourcePosition::BinlogFile(BinlogFilePos::new("binlog.000003", 154).unwrap())
    }

    #[test]
    fn a_checkpoint_in_another_dialect_is_named() {
        assert_eq!(dialect_change(Mode::File, &file()), None);
        let why = dialect_change(Mode::MysqlGtid, &file()).unwrap();
        assert!(
            why.contains("binlog position") && why.contains("mysql-gtid"),
            "{why}"
        );
    }

    #[test]
    fn a_file_position_from_another_server_halts() {
        let a = ServerIdentity::Mariadb { server_id: 1 };
        let b = ServerIdentity::Mariadb { server_id: 2 };
        let cp = |server| Checkpoint {
            position: file(),
            server,
        };
        assert!(same_server(&cp(Some(a.clone())), &a).is_ok());
        for c in [cp(Some(b)), cp(None)] {
            let e = same_server(&c, &a).unwrap_err();
            assert!(
                matches!(&e, SourceError::Permanent(m) if m.starts_with("BINLOG_SOURCE_CHANGED: binlog position binlog.000003:154")),
                "{e}"
            );
        }
    }

    #[test]
    fn the_purge_action_decides() {
        let lost = || SourceError::PositionUnreachable {
            position: "binlog:binlog.000003:154".into(),
            detail: "gone".into(),
        };
        assert_eq!(
            purge_action(BinlogPurgeAction::ReSnapshot, lost()).to_string(),
            lost().to_string()
        );
        assert!(matches!(
            purge_action(BinlogPurgeAction::Error, lost()),
            SourceError::Permanent(m) if m.starts_with("the source can't serve position binlog:binlog.000003:154: gone; binlog_purge_action is error")
        ));
        // Other errors pass through.
        assert!(matches!(
            purge_action(BinlogPurgeAction::Error, SourceError::Transient("x".into())),
            SourceError::Transient(_)
        ));
    }
}
```

Create `crates/data-avalanche-source-mysql/tests/binlog_purge.rs`:

```rust
//! A checkpoint the server purged after it was taken follows `binlog_purge_action` (G-26), at
//! `prepare` and, past `prepare`, on the server's own refusal (error 1236). Its own test binary,
//! which cargo never runs beside the `mysql` tests: `PURGE BINARY LOGS` removes the binlogs their
//! streams read.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "mysql/common.rs"]
mod common;

use std::time::Duration;

use data_avalanche_core::{ChangeStream, Checkpoint, SourceConnector, SourceError, StartPlan};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline};

/// The error `result` ends in: the open's, else the stream's first.
async fn ended(result: Result<Box<dyn ChangeStream>, SourceError>) -> SourceError {
    let mut s = match result {
        Err(e) => return e,
        Ok(s) => s,
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Err(e) = s.next().await {
                return e;
            }
        }
    })
    .await
    .expect("the stream ends")
}

#[tokio::test]
async fn a_purged_checkpoint_follows_the_purge_action() {
    require_it!();
    let (n, mut a) = fresh("binlog_purge", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let halts = connector(&pipeline(&n, &["t"], "", ""));
    let recovers = connector(&pipeline(
        &n,
        &["t"],
        "",
        ", binlog_purge_action: re_snapshot",
    ));
    let tables = halts.resolve_tables().await.unwrap().tables;
    let cp = Checkpoint {
        position: halts.position_now().await.unwrap(),
        server: Some(halts.server_identity().await.unwrap()),
    };
    let resume = SourceConnector::prepare(&halts, Some(&cp), &[])
        .await
        .unwrap();
    assert_eq!(resume.plan, StartPlan::Resume);
    // A transaction after the checkpoint, in a binlog the server then purges.
    a.query_drop("INSERT INTO t VALUES (1); FLUSH BINARY LOGS")
        .await
        .unwrap();
    let newest = halts.binary_logs().await.unwrap().pop().unwrap().name;
    // MariaDB keeps the file it just closed until its binlog checkpoint moves on.
    let mut left = vec![];
    for _ in 0..50 {
        a.query_drop(format!("PURGE BINARY LOGS TO '{newest}'"))
            .await
            .unwrap();
        left = halts.binary_logs().await.unwrap();
        if left.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(left.len(), 1, "the purge left {left:?}");
    for (c, re_snapshot) in [(&halts, false), (&recovers, true)] {
        let e = SourceConnector::prepare(c, Some(&cp), &[])
            .await
            .unwrap_err();
        check(re_snapshot, "prepare", e);
        // The server's refusal, for a stream opened without the check.
        let e = ended(SourceConnector::open_stream(c, &cp.position, &tables).await).await;
        check(re_snapshot, "the dump", e);
    }
    cleanup(&n).await;
}

/// `re_snapshot` keeps the position unreachable; `error` halts the pipeline.
fn check(re_snapshot: bool, at: &str, e: SourceError) {
    match (re_snapshot, &e) {
        (true, SourceError::PositionUnreachable { .. }) => {}
        (false, SourceError::Permanent(m)) if m.contains("binlog_purge_action is error") => {}
        _ => panic!("{at}, re_snapshot {re_snapshot}: {e:?}"),
    }
}
```

Create `crates/data-avalanche-source-mysql/tests/mariadb_compress.rs`:

```rust
//! MariaDB's compressed binlog events (`log_bin_compress`, Q-M7a-5) decode as the plain ones do.
//! Its own test binary, which cargo never runs beside the `mysql` tests: the setting is global,
//! and would compress their events too. A MySQL target returns at once.

#[allow(dead_code)] // the helpers this test doesn't use
#[path = "mysql/common.rs"]
mod common;

use std::time::Duration;

use data_avalanche_core::{ChangeStream, ColumnValue, Op, StreamItem};
use data_avalanche_source_mysql::Flavour;
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{admin_in, cleanup, connector, fresh, pipeline, target};

#[tokio::test]
async fn compressed_events_decode() {
    require_it!();
    if target().flavour != Flavour::Mariadb {
        return;
    }
    let (n, mut a) = fresh(
        "mariadb_compress",
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let from = c.position_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");
    let long = "x".repeat(300);
    // Compress every event of 10 bytes or more (the least the server takes), from a session opened
    // after the change. Every step until the reset is fallible, so the setting is restored
    // whatever happens; the server's defaults also undo an earlier run that died before its own.
    let wrote = async {
        a.query_drop("SET GLOBAL log_bin_compress = ON, GLOBAL log_bin_compress_min_len = 10")
            .await?;
        let mut w = admin_in(&n.db).await;
        w.query_drop(format!(
            "INSERT INTO t VALUES (1, '{long}'), (2, 'short');
             UPDATE t SET v = 'changed' WHERE id = 1;
             DELETE FROM t WHERE id = 2;
             ALTER TABLE t ADD COLUMN w INT"
        ))
        .await?;
        let file = c.binary_logs().await.unwrap().pop().unwrap().name;
        let kinds: Vec<String> = w
            .query_map(
                format!("SHOW BINLOG EVENTS IN '{file}'"),
                |r: mysql_async::Row| r.get::<String, _>(2).unwrap_or_default(),
            )
            .await?;
        Ok::<_, mysql_async::Error>(kinds)
    }
    .await;
    a.query_drop(
        "SET GLOBAL log_bin_compress = DEFAULT, GLOBAL log_bin_compress_min_len = DEFAULT",
    )
    .await
    .expect("restore log_bin_compress");
    let kinds = wrote.expect("the compressed writes");
    // The server did compress them.
    for kind in [
        "Query_compressed",
        "Write_rows_compressed_v1",
        "Update_rows_compressed_v1",
        "Delete_rows_compressed_v1",
    ] {
        assert!(kinds.iter().any(|k| k == kind), "{kind} in {kinds:?}");
    }
    let mut changes = vec![];
    let mut signalled = false;
    while !signalled {
        let item = tokio::time::timeout(Duration::from_secs(20), s.next())
            .await
            .expect("an item within 20 s")
            .expect("the stream");
        match item {
            StreamItem::Change(e) => {
                let image = e.after.as_ref().or(e.before.as_ref()).unwrap();
                changes.push((e.op, image.get("id").cloned(), image.get("v").cloned()));
            }
            StreamItem::SchemaSignal { table, .. } => {
                assert_eq!(table.name, "t");
                signalled = true;
            }
            _ => {}
        }
    }
    let text = |v: &str| Some(ColumnValue::Text(v.into()));
    assert_eq!(
        changes,
        [
            (Op::Insert, Some(ColumnValue::Int32(1)), text(&long)),
            (Op::Insert, Some(ColumnValue::Int32(2)), text("short")),
            (Op::Update, Some(ColumnValue::Int32(1)), text("changed")),
            (Op::Delete, Some(ColumnValue::Int32(2)), text("short")),
        ]
    );
    Box::new(s).close().await.expect("close");
    cleanup(&n).await;
}
```

Change `crates/data-avalanche-source-mysql/tests/mysql/common.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/common.rs b/crates/data-avalanche-source-mysql/tests/mysql/common.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/common.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/common.rs
@@ -174,6 +174,24 @@ pub fn connector(p: &Pipeline) -> MySqlConnector {
     MySqlConnector::new(p).expect("connector")
 }
 
+/// A user `it_<test>` with `grants` (and `extra` statements after them), password `avalanche`.
+pub async fn user(a: &mut mysql_async::Conn, name: &str, grants: &str, extra: &[String]) {
+    a.query_drop(format!("DROP USER IF EXISTS '{name}'@'%'"))
+        .await
+        .unwrap();
+    a.query_drop(format!(
+        "CREATE USER '{name}'@'%' IDENTIFIED BY 'avalanche'"
+    ))
+    .await
+    .unwrap();
+    a.query_drop(format!("GRANT {grants} TO '{name}'@'%'"))
+        .await
+        .unwrap();
+    for s in extra {
+        a.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
+    }
+}
+
 /// The test's database, dropped at the end of a passing test.
 pub async fn cleanup(n: &Names) {
     let mut a = admin_in("avalanche").await;
```

Change `crates/data-avalanche-source-mysql/tests/mysql/preflight.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/preflight.rs b/crates/data-avalanche-source-mysql/tests/mysql/preflight.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/preflight.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/preflight.rs
@@ -7,7 +7,7 @@ use data_avalanche_source_mysql::Flavour;
 use data_avalanche_testkit::require_it;
 use mysql_async::prelude::Queryable;
 
-use crate::common::{Names, cleanup, connector, fresh, pipeline, pipeline_as, target};
+use crate::common::{Names, cleanup, connector, fresh, pipeline, pipeline_as, target, user};
 
 #[tokio::test]
 async fn the_test_server_passes_preflight() {
@@ -38,24 +38,6 @@ async fn gtid_replication_needs_gtid_mode_on() {
     cleanup(&n).await;
 }
 
-/// A user `it_<test>` with `grants` (and `extra` statements after them), password `avalanche`.
-async fn user(a: &mut mysql_async::Conn, name: &str, grants: &str, extra: &[String]) {
-    a.query_drop(format!("DROP USER IF EXISTS '{name}'@'%'"))
-        .await
-        .unwrap();
-    a.query_drop(format!(
-        "CREATE USER '{name}'@'%' IDENTIFIED BY 'avalanche'"
-    ))
-    .await
-    .unwrap();
-    a.query_drop(format!("GRANT {grants} TO '{name}'@'%'"))
-        .await
-        .unwrap();
-    for s in extra {
-        a.query_drop(s).await.unwrap_or_else(|e| panic!("{s}: {e}"));
-    }
-}
-
 #[tokio::test]
 async fn a_user_without_replication_privileges_is_refused() {
     require_it!();
```

Create `crates/data-avalanche-source-mysql/tests/mysql/start.rs`:

```rust
//! A start's stream position against the target server: the first start, a resume, a checkpoint
//! in another dialect, from another server or purged, and the heartbeat's watch on the dump.

use std::time::Duration;

use data_avalanche_core::{
    ChangeStream, Checkpoint, RetainedLog, ServerIdentity, SnapshotReason, SourceConnector,
    SourceError, SourcePosition, StartPlan,
};
use data_avalanche_source_mysql::{Flavour, Mode, MySqlConnector};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, pipeline_as, target, user};

const RE_SNAPSHOT: &str = ", binlog_purge_action: re_snapshot";

/// Another server of the target's flavour.
fn elsewhere() -> ServerIdentity {
    match target().flavour {
        Flavour::Mysql => ServerIdentity::Mysql {
            server_uuid: "00000000-0000-0000-0000-000000000001".into(),
        },
        Flavour::Mariadb => ServerIdentity::Mariadb {
            server_id: u32::MAX,
        },
    }
}

/// A checkpoint `c` can't read: a GTID set where it reads files, a file where it reads GTIDs.
async fn foreign(c: &MySqlConnector) -> SourcePosition {
    let tagged = match c.mode().await.unwrap() {
        Mode::File => "mysql-gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
        _ => "binlog:binlog.000001:4",
    };
    SourcePosition::parse_tagged(tagged).unwrap()
}

/// `prepare` for `checkpoint` through the trait.
async fn prepare(c: &MySqlConnector, checkpoint: &Checkpoint) -> Result<StartPlan, SourceError> {
    SourceConnector::prepare(c, Some(checkpoint), &[])
        .await
        .map(|p| p.plan)
}

#[tokio::test]
async fn a_first_start_snapshots_and_records_the_server_then_resumes() {
    require_it!();
    let (n, _a) = fresh("start_first", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let identity = c.server_identity().await.unwrap();
    let first = SourceConnector::prepare(&c, None, &[])
        .await
        .expect("prepare");
    let StartPlan::Snapshot { from, reason } = first.plan else {
        panic!("a snapshot, got {:?}", first.plan)
    };
    assert_eq!(reason, SnapshotReason::FirstStart);
    assert_eq!(from.dialect(), c.mode().await.unwrap().dialect());
    assert_eq!(first.server.as_ref(), Some(&identity));
    let again = SourceConnector::prepare(
        &c,
        Some(&Checkpoint {
            position: from.clone(),
            server: first.server,
        }),
        &[],
    )
    .await
    .expect("resume");
    assert_eq!(again.plan, StartPlan::Resume);
    assert!(again.added.is_empty() && again.warnings.is_empty());
    assert_eq!(again.server, Some(identity));
    // A lost position is replaced by the write position now.
    let reset = c.reset_position().await.unwrap();
    assert!(reset.covers(&from).unwrap(), "{reset} covers {from}");
    cleanup(&n).await;
}

#[tokio::test]
async fn a_checkpoint_in_another_dialect_follows_the_purge_action() {
    require_it!();
    let (n, _a) = fresh("start_dialect", "").await;
    let halts = connector(&pipeline(&n, &["t"], "", ""));
    let cp = Checkpoint {
        position: foreign(&halts).await,
        server: Some(halts.server_identity().await.unwrap()),
    };
    match prepare(&halts, &cp).await {
        Err(SourceError::Permanent(m)) => {
            assert!(
                m.contains("now reads") && m.contains("binlog_purge_action is error"),
                "{m}"
            )
        }
        other => panic!("a halt, got {other:?}"),
    }
    let recovers = connector(&pipeline(&n, &["t"], "", RE_SNAPSHOT));
    match prepare(&recovers, &cp).await {
        Err(SourceError::PositionUnreachable { position, .. }) => {
            assert_eq!(position, cp.position.to_tagged())
        }
        other => panic!("unreachable, got {other:?}"),
    }
    cleanup(&n).await;
}

/// File/position mode halts on a checkpoint from another server; GTIDs carry across servers.
#[tokio::test]
async fn a_checkpoint_from_another_server() {
    require_it!();
    let (n, _a) = fresh("start_server", "").await;
    let c = connector(&pipeline(&n, &["t"], "", RE_SNAPSHOT));
    let position = c.position_now().await.unwrap();
    for server in [Some(elsewhere()), None] {
        let cp = Checkpoint {
            position: position.clone(),
            server,
        };
        match (c.mode().await.unwrap(), prepare(&c, &cp).await) {
            (Mode::File, Err(SourceError::Permanent(m))) => {
                assert!(m.starts_with("BINLOG_SOURCE_CHANGED: "), "{m}")
            }
            (Mode::MysqlGtid | Mode::MariadbGtid, Ok(StartPlan::Resume)) => {}
            other => panic!("{cp:?}: {other:?}"),
        }
    }
    cleanup(&n).await;
}

/// A binlog file the server doesn't hold (GTID purges need a purge on the server:
/// `tests/mysql_global.rs`).
#[tokio::test]
async fn a_purged_file_follows_the_purge_action() {
    require_it!();
    let (n, _a) = fresh("start_purged", "").await;
    let halts = connector(&pipeline(&n, &["t"], "", ""));
    let SourcePosition::BinlogFile(now) = halts.position_now().await.unwrap() else {
        return;
    };
    let cp = Checkpoint {
        position: SourcePosition::parse_tagged(&format!("binlog:{}.000000:4", now.base())).unwrap(),
        server: Some(halts.server_identity().await.unwrap()),
    };
    match prepare(&halts, &cp).await {
        Err(SourceError::Permanent(m)) => assert!(
            m.contains("is no longer on the server") && m.contains("binlog_purge_action is error"),
            "{m}"
        ),
        other => panic!("a halt, got {other:?}"),
    }
    let recovers = connector(&pipeline(&n, &["t"], "", RE_SNAPSHOT));
    assert!(matches!(
        prepare(&recovers, &cp).await,
        Err(SourceError::PositionUnreachable { .. })
    ));
    cleanup(&n).await;
}

/// Polls `retained` until it reads `streamed`.
async fn watch(c: &MySqlConnector, streamed: bool) {
    for _ in 0..50 {
        let r = c.retained().await.expect("retained");
        if r == Some(RetainedLog { bytes: 0, streamed }) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("retained never read streamed: {streamed}");
}

/// The watch finds the dump by its connection id, as a user without `PROCESS`; a dump the server
/// ended reads as not streamed before the stream hears of it (DA-SRC-16).
#[tokio::test]
async fn the_watch_finds_the_dump_by_its_connection() {
    require_it!();
    let (n, mut a) = fresh("start_watch", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let name = "it_start_watch";
    user(
        &mut a,
        name,
        "SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.*",
        &[],
    )
    .await;
    let c = connector(&pipeline_as(
        &n,
        name,
        "      ssl_mode: disable",
        "",
        &["t"],
        "",
    ));
    watch(&c, false).await;
    let tables = c.resolve_tables().await.unwrap().tables;
    let from = c.position_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");
    watch(&c, true).await;
    let id = c.dump_id().expect("the dump's id");
    a.query_drop(format!("KILL {id}")).await.unwrap();
    watch(&c, false).await;
    // What the server sent before the kill comes first.
    let e = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Err(e) = s.next().await {
                return e;
            }
        }
    })
    .await
    .expect("the stream ends");
    assert!(e.is_transient(), "{e}");
    let _ = Box::new(s).close().await;
    assert_eq!(c.dump_id(), None);
    a.query_drop(format!("DROP USER '{name}'@'%'"))
        .await
        .unwrap();
    cleanup(&n).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: FAIL to compile. The first errors:

```text
error[E0432]: unresolved imports `start::dialect_change`, `start::purge_action`, `start::same_server`
error[E0425]: cannot find type `SourcePosition` in this scope
error[E0433]: cannot find type `Mode` in this scope
error[E0422]: cannot find struct, variant or union type `Checkpoint` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -4,7 +4,12 @@ use data_avalanche_config::{Mysql, Pipeline, SourceType};
 use std::sync::atomic::AtomicU32;
 use std::sync::{Arc, Mutex};
 
-use data_avalanche_core::{Preflight, SchemaDelta, SourceError, TableSchema};
+use async_trait::async_trait;
+use data_avalanche_core::{
+    ChangeStream, Checkpoint, Chunk, KeysetCursor, Preflight, Prepared, RescanReport,
+    ResolvedTables, RetainedLog, Row, SchemaDelta, SchemaHint, SourceConnector, SourceError,
+    SourcePosition, TableRef, TableSchema,
+};
 use mysql_async::prelude::Queryable;
 use tokio::sync::OnceCell;
 
@@ -208,3 +213,85 @@ impl MySqlConnector {
             .await
     }
 }
+
+/// The trait is the inherent methods of the modules: `catalog` (channel A), `start` (the start
+/// position and the dump watch), `position`, `snapshot`, `stream`, `schema` (channels C and D).
+#[async_trait]
+impl SourceConnector for MySqlConnector {
+    async fn preflight(&self) -> Result<Preflight, SourceError> {
+        MySqlConnector::preflight(self).await
+    }
+
+    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError> {
+        MySqlConnector::resolve_tables(self).await
+    }
+
+    async fn prepare(
+        &self,
+        checkpoint: Option<&Checkpoint>,
+        _tables: &[TableRef],
+    ) -> Result<Prepared, SourceError> {
+        MySqlConnector::prepare(self, checkpoint).await
+    }
+
+    /// The server's write position now: the binlog keeps no position for the pipeline to lose.
+    async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
+        self.position_now().await
+    }
+
+    async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
+        MySqlConnector::retained(self).await
+    }
+
+    async fn current_position(&self) -> Result<SourcePosition, SourceError> {
+        self.position_now().await
+    }
+
+    async fn read_chunk(
+        &self,
+        table: &TableSchema,
+        after: Option<&KeysetCursor>,
+        limit: usize,
+    ) -> Result<Chunk, SourceError> {
+        MySqlConnector::read_chunk(self, table, after, limit).await
+    }
+
+    /// `toast_strategy: re_query` is PostgreSQL's: a FULL row image carries every column.
+    async fn fetch_rows(
+        &self,
+        table: &TableSchema,
+        _keys: &[Row],
+    ) -> Result<Vec<Row>, SourceError> {
+        Err(SourceError::Permanent(format!(
+            "table {}: a MySQL source doesn't re-query rows (toast_strategy is PostgreSQL's)",
+            table.table()
+        )))
+    }
+
+    async fn open_stream(
+        &self,
+        from: &SourcePosition,
+        tables: &[TableSchema],
+    ) -> Result<Box<dyn ChangeStream>, SourceError> {
+        Ok(Box::new(
+            MySqlConnector::open_stream(self, from, tables).await?,
+        ))
+    }
+
+    async fn augment(
+        &self,
+        cached: &TableSchema,
+        hint: &SchemaHint,
+    ) -> Result<(SchemaDelta, Option<TableSchema>), SourceError> {
+        MySqlConnector::augment(self, cached, hint).await
+    }
+
+    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError> {
+        MySqlConnector::rescan(self, known).await
+    }
+
+    /// The source creates nothing for a pipeline: no slot, no publication.
+    async fn teardown(&self) -> Result<(), SourceError> {
+        Ok(())
+    }
+}
```

Change `crates/data-avalanche-source-mysql/src/position.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/position.rs b/crates/data-avalanche-source-mysql/src/position.rs
--- a/crates/data-avalanche-source-mysql/src/position.rs
+++ b/crates/data-avalanche-source-mysql/src/position.rs
@@ -27,6 +27,15 @@ impl Mode {
             (ReplicationType::Gtid, Flavour::Mariadb) => Self::MariadbGtid,
         }
     }
+
+    /// The tag of this mode's positions (`SourcePosition::dialect`).
+    pub fn dialect(self) -> &'static str {
+        match self {
+            Self::MysqlGtid => "mysql-gtid",
+            Self::MariadbGtid => "mariadb-gtid",
+            Self::File => "binlog",
+        }
+    }
 }
 
 /// One row of `SHOW BINARY LOGS`.
@@ -208,12 +217,7 @@ impl MySqlConnector {
 
 /// A position the server printed, in `mode`'s dialect.
 pub(crate) fn parse(mode: Mode, text: &str) -> Result<SourcePosition, SourceError> {
-    let tag = match mode {
-        Mode::MysqlGtid => "mysql-gtid",
-        Mode::MariadbGtid => "mariadb-gtid",
-        Mode::File => "binlog",
-    };
-    SourcePosition::parse_tagged(&format!("{tag}:{text}"))
+    SourcePosition::parse_tagged(&format!("{}:{text}", mode.dialect()))
         .map_err(|e| SourceError::Permanent(format!("the server's position: {e}")))
 }
 
```

In `crates/data-avalanche-source-mysql/src/start.rs`, add above the test module:

```rust
//! A start's stream position (Data-Flow Phase 2b, G-26, Q-M7-2, Q-M7a-6), the heartbeat's watch on
//! the dump.
//!
//! A first start snapshots from the server's write position and records the server. A checkpoint
//! in another dialect than the pipeline reads now (`replication_type` changed, or the server is
//! another flavour) and a checkpoint the server can no longer serve both follow
//! `binlog_purge_action`: `re_snapshot` re-snapshots from a new position, `error` halts. In
//! file/position mode a checkpoint recorded on another server halts the pipeline
//! (`BINLOG_SOURCE_CHANGED`): a position in one server's files means nothing in another's.

use std::sync::atomic::Ordering;

use data_avalanche_config::BinlogPurgeAction;
use data_avalanche_core::{
    Checkpoint, Prepared, RetainedLog, ServerIdentity, SnapshotReason, SourceError, SourcePosition,
    StartPlan,
};
use mysql_async::prelude::Queryable;

use crate::connector::MySqlConnector;
use crate::position::{Mode, purged};

/// Why `checkpoint` can't be read in `mode`, if it can't.
pub fn dialect_change(mode: Mode, checkpoint: &SourcePosition) -> Option<String> {
    (checkpoint.dialect() != mode.dialect()).then(|| {
        format!(
            "the checkpoint {} is a {} position, and the pipeline now reads {} positions \
             (replication_type changed, or the server is another flavour)",
            checkpoint.to_tagged(),
            checkpoint.dialect(),
            mode.dialect()
        )
    })
}

/// File/position mode: the checkpoint must be from the connected server (Q-M7-2).
pub fn same_server(checkpoint: &Checkpoint, now: &ServerIdentity) -> Result<(), SourceError> {
    let at = checkpoint.position.untagged();
    let recorded = match &checkpoint.server {
        Some(s) if s == now => return Ok(()),
        Some(s) => s.to_string(),
        None => "an unrecorded server".to_owned(),
    };
    Err(SourceError::Permanent(format!(
        "BINLOG_SOURCE_CHANGED: binlog position {at} was recorded on {recorded}, now connected \
         to {now}; file/position cannot be carried across servers — switch to replication_type: \
         gtid or re-snapshot"
    )))
}

/// A position the server can't serve, as `binlog_purge_action` says (G-26): `re_snapshot` keeps
/// it [`SourceError::PositionUnreachable`], and the pipeline re-snapshots from a new position;
/// `error` halts the pipeline for the operator. `prepare` and the stream (error 1236, a purge
/// after `prepare`) both decide by it.
pub fn purge_action(action: BinlogPurgeAction, e: SourceError) -> SourceError {
    match (action, e) {
        (BinlogPurgeAction::Error, SourceError::PositionUnreachable { position, detail }) => {
            SourceError::Permanent(format!(
                "the source can't serve position {position}: {detail}; binlog_purge_action is \
                 error, so the pipeline halts: set binlog_purge_action: re_snapshot to re-snapshot \
                 from the server's position now"
            ))
        }
        (_, e) => e,
    }
}

impl MySqlConnector {
    /// The stream position for a start. The binlog carries every table, so a resume adds none.
    pub async fn prepare(&self, checkpoint: Option<&Checkpoint>) -> Result<Prepared, SourceError> {
        let mode = self.mode().await?;
        let server = self.server_identity().await?;
        let Some(cp) = checkpoint else {
            return Ok(Prepared {
                plan: StartPlan::Snapshot {
                    from: self.position_now().await?,
                    reason: SnapshotReason::FirstStart,
                },
                added: vec![],
                warnings: vec![],
                server: Some(server),
            });
        };
        let unreachable = |detail| {
            purge_action(
                self.mysql.binlog_purge_action,
                SourceError::PositionUnreachable {
                    position: cp.position.to_tagged(),
                    detail,
                },
            )
        };
        if let Some(why) = dialect_change(mode, &cp.position) {
            return Err(unreachable(why));
        }
        if mode == Mode::File {
            same_server(cp, &server)?;
        }
        let retention = self.retention(&cp.position).await?;
        if let Some(why) = purged(&cp.position, &retention) {
            return Err(unreachable(why));
        }
        Ok(Prepared {
            plan: StartPlan::Resume,
            added: vec![],
            warnings: vec![],
            server: Some(server),
        })
    }

    /// The connection id of the open binlog dump, `None` without one: the server's
    /// `information_schema.PROCESSLIST` id of the stream's connection, for status and logs.
    pub fn dump_id(&self) -> Option<u32> {
        match self.shared.dump.load(Ordering::Relaxed) {
            0 => None,
            id => Some(id),
        }
    }

    /// The heartbeat's watch on the dump (DA-SRC-16, V-121): whether the server still runs the
    /// stream's binlog dump, found by its connection id. The server keeps no log for the pipeline
    /// (its binlogs expire by its own policy), so `bytes` is 0.
    pub async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
        let id = self.shared.dump.load(Ordering::Relaxed);
        let streamed = match id {
            0 => false,
            id => {
                self.conn
                    .command("find the binlog dump", move |c| {
                        Box::pin(async move {
                            let n: Option<i64> = c
                                .exec_first(
                                    "SELECT COUNT(*) FROM information_schema.PROCESSLIST
                                  WHERE ID = ? AND COMMAND LIKE 'Binlog Dump%'",
                                    (id,),
                                )
                                .await?;
                            Ok(n.unwrap_or(0) > 0)
                        })
                    })
                    .await?
            }
        };
        Ok(Some(RetainedLog { bytes: 0, streamed }))
    }
}

```

Change `crates/data-avalanche-source-mysql/src/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/stream.rs b/crates/data-avalanche-source-mysql/src/stream.rs
--- a/crates/data-avalanche-source-mysql/src/stream.rs
+++ b/crates/data-avalanche-source-mysql/src/stream.rs
@@ -48,6 +48,7 @@ use crate::ddl::{Renamed, classify, renamed};
 use crate::error::mysql;
 use crate::position::Mode;
 use crate::server::Flavour;
+use crate::start::purge_action;
 use crate::types::{CatalogColumn, plan_column};
 use crate::value::{Decode, Input, decode_after, halt_reason};
 
@@ -308,7 +309,8 @@ impl MySqlConnector {
             tracked.insert(s.table().clone(), t);
         }
         let at = from.to_tagged();
-        let err = |e| mysql(e, Some(&at));
+        let action = self.mysql.binlog_purge_action;
+        let err = |e| purge_action(action, mysql(e, Some(&at)));
         let mut conn = self.conn.connect().await?;
         let id = conn.id();
         conn.query_drop(format!(
@@ -924,7 +926,10 @@ impl ChangeStream for MySqlStream {
             let at = self.position.to_tagged();
             let ev = match self.events.next().await {
                 Some(Ok(ev)) => ev,
-                Some(Err(e)) => return Err(mysql(e, Some(&at))),
+                Some(Err(e)) => {
+                    let action = self.catalog.mysql.binlog_purge_action;
+                    return Err(purge_action(action, mysql(e, Some(&at))));
+                }
                 None => {
                     return Err(SourceError::Transient(
                         "the server ended the binlog stream".into(),
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib`

Expected: PASS, 73 tests.

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql start:: || break; done`

Expected: PASS, 5 tests on each target (`test-infra/up.sh` first).

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test binlog_purge || break; done`

Expected: PASS, 1 test on each target (`test-infra/up.sh` first).

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mariadb_compress || break; done`

Expected: PASS, 1 test on each target (`test-infra/up.sh` first).

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql || break; done`

Expected: PASS, 38 tests on each target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/src/connector.rs \
  crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/position.rs \
  crates/data-avalanche-source-mysql/src/start.rs \
  crates/data-avalanche-source-mysql/src/stream.rs \
  crates/data-avalanche-source-mysql/tests/binlog_purge.rs \
  crates/data-avalanche-source-mysql/tests/mariadb_compress.rs \
  crates/data-avalanche-source-mysql/tests/mysql/common.rs \
  crates/data-avalanche-source-mysql/tests/mysql/main.rs \
  crates/data-avalanche-source-mysql/tests/mysql/preflight.rs \
  crates/data-avalanche-source-mysql/tests/mysql/start.rs
git commit -m "feat(source-mysql): the start position and the dump watch — first start, resume, dialect and server checks, binlog_purge_action at prepare and on 1236; SourceConnector"
```

---

### Task 15: The type round trip

**Files:**
- Modify: `crates/data-avalanche-source-mysql/tests/mysql/main.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs`

**Interfaces:**
- Consumes:
  - The crate's whole public API (Tasks 4–14).
- Produces:
  - Nothing public: the change is inside the crate.

Every type of Type_Mapping's MySQL and MariaDB tables, written once and read both ways. The
test inserts a row at each type's low end, one at its high end, one of NULLs and an ordinary
one, then updates every column of the ordinary row. The stream's insert images must equal a
chunk's rows, the update's before image the insert image, and its after image the chunk row
after it; four values are pinned so both paths can't be wrong alike.

The table covers integers signed and unsigned at their limits, `FLOAT`/`DOUBLE` extremes,
decimals up to `DECIMAL(65,30)` (text under the default `numeric_overflow_action: string`),
`utf8mb4` and `latin1` text (cp1252's `€` and `‰`), every text and blob size up to 100,000
four-byte characters, `BINARY` padding, `DATE`, `TIME` at ±838 hours, `DATETIME` and `TIMESTAMP`
at fsp 0, 3 and 6 over their ranges, `YEAR`, `ENUM`, `SET`, `BIT(1)` and `BIT(64)`, nested JSON
(u64 max, i64 min, 1.5e300, unicode), `GEOMETRY` and `POINT` with SRID 4326, and MariaDB's
`UUID`, `INET4`, `INET6` and, from 11.7, `VECTOR`. MariaDB refuses a `VECTOR` component past
about 1e16 in text and in binary (measured on 11.8), so its ends are float32 bytes: ±1e16 with
the smallest normal and denormal floats.

A mutation (the binlog's `YEAR` read one too high) fails the test with three differences, so
the comparison catches a decoder of one input going wrong.

- [ ] **Step 1: Write the round trip**

Change `crates/data-avalanche-source-mysql/tests/mysql/main.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/main.rs b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
--- a/crates/data-avalanche-source-mysql/tests/mysql/main.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/main.rs
@@ -7,6 +7,7 @@ mod common;
 mod position;
 mod preflight;
 mod resolve;
+mod roundtrip;
 mod schema;
 mod snapshot;
 mod start;
```

Create `crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs`:

```rust
//! Every type, both ways (`Type_Mapping.md`): the binlog's row images decode to the values a
//! chunk reads, at each type's extremes, NULL and ordinary values, through an insert and an
//! update.

use std::time::Duration;

use data_avalanche_core::{ChangeStream, ColumnValue, Op, Row, StreamItem};
use data_avalanche_source_mysql::{Flavour, version_of};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, target};

/// A column, its type, and its value as SQL at the low end, the high end, and an ordinary one.
type Column = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
);

const COLUMNS: &[Column] = &[
    ("b", "TINYINT(1)", "0", "1", "1"),
    ("ti", "TINYINT", "-128", "127", "-5"),
    ("tiu", "TINYINT UNSIGNED", "0", "255", "200"),
    ("si", "SMALLINT", "-32768", "32767", "-300"),
    ("siu", "SMALLINT UNSIGNED", "0", "65535", "40000"),
    ("mi", "MEDIUMINT", "-8388608", "8388607", "-70000"),
    ("miu", "MEDIUMINT UNSIGNED", "0", "16777215", "9000000"),
    ("i", "INT", "-2147483648", "2147483647", "-7"),
    ("iu", "INT UNSIGNED", "0", "4294967295", "3000000000"),
    (
        "bi",
        "BIGINT",
        "-9223372036854775808",
        "9223372036854775807",
        "-1",
    ),
    (
        "biu",
        "BIGINT UNSIGNED",
        "0",
        "18446744073709551615",
        "10000000000000000000",
    ),
    ("f", "FLOAT", "-3.4e38", "3.4e38", "1.5"),
    (
        "d",
        "DOUBLE",
        "-1.7976931348623157e308",
        "1.7976931348623157e308",
        "0.1",
    ),
    ("dec0", "DECIMAL(10,0)", "-9999999999", "9999999999", "42"),
    (
        "dec2",
        "DECIMAL(12,2)",
        "-9999999999.99",
        "9999999999.99",
        "-0.01",
    ),
    (
        "dec38",
        "DECIMAL(38,10)",
        "-9999999999999999999999999999.9999999999",
        "9999999999999999999999999999.9999999999",
        "0.0000000001",
    ),
    (
        "dec65",
        "DECIMAL(65,30)",
        "-99999999999999999999999999999999999.999999999999999999999999999999",
        "99999999999999999999999999999999999.999999999999999999999999999999",
        "1.5",
    ),
    ("c", "CHAR(4)", "''", "'abcd'", "'ab'"),
    ("cl", "CHAR(4) CHARACTER SET latin1", "''", "'Ææ¿ÿ'", "'€'"),
    ("vc", "VARCHAR(40)", "''", "'😀 ünïcødé 中文'", "'x'"),
    (
        "vcl",
        "VARCHAR(40) CHARACTER SET latin1",
        "''",
        "'Ææ¿ÿ€‰'",
        "'é'",
    ),
    ("tt", "TINYTEXT", "''", "REPEAT('ü', 127)", "'t'"),
    ("tx", "TEXT", "''", "REPEAT('ü', 30000)", "'t'"),
    ("mt", "MEDIUMTEXT", "''", "REPEAT('ü', 100000)", "'t'"),
    ("lt", "LONGTEXT", "''", "REPEAT('😀', 100000)", "'t'"),
    ("bn", "BINARY(4)", "''", "x'01020304'", "x'ff'"),
    ("vb", "VARBINARY(8)", "''", "x'00ff00ff00ff00ff'", "x'00'"),
    ("tb", "TINYBLOB", "''", "REPEAT(x'ff', 255)", "x'01'"),
    ("bl", "BLOB", "''", "REPEAT(x'00ff', 30000)", "x'01'"),
    ("mb", "MEDIUMBLOB", "''", "REPEAT(x'00ff', 100000)", "x'01'"),
    ("lb", "LONGBLOB", "''", "REPEAT(x'abcd', 100000)", "x'01'"),
    ("dt", "DATE", "'1000-01-01'", "'9999-12-31'", "'2026-10-14'"),
    ("tm0", "TIME", "'-838:59:59'", "'838:59:59'", "'12:34:56'"),
    (
        "tm6",
        "TIME(6)",
        "'-838:59:58.999999'",
        "'838:59:58.999999'",
        "'-00:00:00.000001'",
    ),
    (
        "dtm0",
        "DATETIME",
        "'1000-01-01 00:00:00'",
        "'9999-12-31 23:59:59'",
        "'2026-10-14 12:00:00'",
    ),
    (
        "dtm3",
        "DATETIME(3)",
        "'1000-01-01 00:00:00.000'",
        "'9999-12-31 23:59:59.999'",
        "'2026-10-14 12:00:00.5'",
    ),
    (
        "dtm6",
        "DATETIME(6)",
        "'1000-01-01 00:00:00.000000'",
        "'9999-12-31 23:59:59.999999'",
        "'2026-10-14 12:00:00.000001'",
    ),
    (
        "ts0",
        "TIMESTAMP NULL",
        "'1970-01-01 00:00:01'",
        "'2038-01-19 03:14:07'",
        "'2026-10-14 12:00:00'",
    ),
    (
        "ts6",
        "TIMESTAMP(6) NULL",
        "'1970-01-01 00:00:01.000000'",
        "'2038-01-19 03:14:07.999999'",
        "'2026-10-14 12:00:00.123456'",
    ),
    ("yr", "YEAR", "1901", "2155", "2026"),
    ("en", "ENUM('a','b','ç')", "'a'", "'ç'", "'b'"),
    ("st", "SET('x','y','z')", "''", "'x,y,z'", "'y'"),
    ("bt1", "BIT(1)", "b'0'", "b'1'", "b'1'"),
    ("bt64", "BIT(64)", "0", "18446744073709551615", "5"),
    (
        "js",
        "JSON",
        "'null'",
        r#"'{"a": [1, 2.5, "x", null, true, false], "b": {"c": "ü😀"}, "big": 18446744073709551615, "neg": -9223372036854775808, "f": 1.5e300, "e": "", "o": {}}'"#,
        "'[]'",
    ),
    (
        "g",
        "GEOMETRY",
        "ST_GeomFromText('POINT(0 0)')",
        "ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 0))')",
        "ST_GeomFromText('LINESTRING(1 1,2 2)')",
    ),
    (
        "pt",
        "POINT",
        "ST_GeomFromText('POINT(-1.5 -2.5)')",
        "ST_GeomFromText('POINT(10 20)', 4326)",
        "ST_GeomFromText('POINT(1 2)')",
    ),
];

/// MariaDB's own types; `VECTOR` from 11.7. Its text form refuses magnitudes past about 1e19 and
/// its binary form past about the same (measured on 11.8): the ends are float32 bytes, ±1e16 with
/// the smallest normal and denormal floats.
const MARIADB: &[Column] = &[
    (
        "u",
        "UUID",
        "'00000000-0000-0000-0000-000000000000'",
        "'ffffffff-ffff-ffff-ffff-ffffffffffff'",
        "'123e4567-e89b-12d3-a456-426614174000'",
    ),
    (
        "i4",
        "INET4",
        "'0.0.0.0'",
        "'255.255.255.255'",
        "'192.168.0.1'",
    ),
    (
        "i6",
        "INET6",
        "'::'",
        "'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'",
        "'2001:db8::1'",
    ),
];
const VECTOR: Column = (
    "vec",
    "VECTOR(3)",
    "x'ca1b0eda0000000001000080'",
    "x'ca1b0e5a0000800001000000'",
    "VEC_FromText('[0.5,0.25,0.125]')",
);

/// The values of `columns` at `pick` (1 low, 2 high, 3 ordinary).
fn values(columns: &[Column], pick: usize) -> Vec<&'static str> {
    columns
        .iter()
        .map(|c| match pick {
            1 => c.2,
            2 => c.3,
            _ => c.4,
        })
        .collect()
}

/// The columns where `a` and `b` differ.
fn differ(a: &Row, b: &Row) -> Vec<String> {
    let names: Vec<&String> = a.0.iter().map(|(n, _)| n).collect();
    let mut out = vec![];
    if names != b.0.iter().map(|(n, _)| n).collect::<Vec<_>>() {
        out.push(format!("columns {names:?} vs {:?}", b.0));
    }
    for ((n, x), (_, y)) in a.0.iter().zip(&b.0) {
        if x != y {
            let show = |v: &ColumnValue| {
                let s = format!("{v:?}");
                s.chars().take(200).collect::<String>()
            };
            out.push(format!("{n}: {} vs {}", show(x), show(y)));
        }
    }
    out
}

#[tokio::test]
async fn every_type_reads_the_same_from_the_stream_and_a_chunk() {
    require_it!();
    let mut columns = COLUMNS.to_vec();
    if target().flavour == Flavour::Mariadb {
        columns.extend_from_slice(MARIADB);
    }
    let (n, mut a) = fresh("roundtrip", "").await;
    let version: String = a.query_first("SELECT VERSION()").await.unwrap().unwrap();
    if target().flavour == Flavour::Mariadb && version_of(&version) >= Some((11, 7)) {
        columns.push(VECTOR);
    }
    let ddl: Vec<String> = columns.iter().map(|c| format!("{} {}", c.0, c.1)).collect();
    a.query_drop(format!(
        "CREATE TABLE t (id INT PRIMARY KEY, {}); SET time_zone = '+00:00'",
        ddl.join(", ")
    ))
    .await
    .unwrap();
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let resolved = c.resolve_tables().await.unwrap();
    assert!(resolved.halted.is_empty(), "{:?}", resolved.halted);
    let tables = resolved.tables;
    let from = c.position_now().await.unwrap();
    let mut s = c.open_stream(&from, &tables).await.expect("stream");

    let names: Vec<&str> = columns.iter().map(|c| c.0).collect();
    let row = |id: u32, v: Vec<&str>| format!("({id}, {})", v.join(", "));
    let nulls = vec!["NULL"; columns.len()];
    a.query_drop(format!(
        "INSERT INTO t (id, {}) VALUES {}, {}, {}, {}",
        names.join(", "),
        row(1, values(&columns, 1)),
        row(2, values(&columns, 2)),
        row(3, nulls),
        row(4, values(&columns, 3)),
    ))
    .await
    .unwrap();
    // Row 4, every column changed: to the low ends.
    let set: Vec<String> = columns
        .iter()
        .map(|c| format!("{} = {}", c.0, c.2))
        .collect();
    a.query_drop(format!("UPDATE t SET {} WHERE id = 4", set.join(", ")))
        .await
        .unwrap();

    let (mut inserted, mut updated) = (vec![], None);
    let mut commits = 0;
    while commits < 2 {
        let item = tokio::time::timeout(Duration::from_secs(20), s.next())
            .await
            .expect("an item within 20 s")
            .expect("the stream");
        match item {
            StreamItem::Change(e) if e.op == Op::Insert => inserted.push(e.after.unwrap()),
            StreamItem::Change(e) if e.op == Op::Update => {
                updated = Some((e.before.unwrap(), e.after.unwrap()))
            }
            StreamItem::Commit(_) => commits += 1,
            StreamItem::TableHalt { reason, .. } => panic!("halted: {reason:?}"),
            _ => {}
        }
    }
    Box::new(s).close().await.expect("close");
    let chunk = c.read_chunk(&tables[0], None, 10).await.expect("chunk");
    assert_eq!(chunk.rows.len(), 4);
    let (before, after) = updated.expect("the update");
    let pairs = [
        ("insert 1", &inserted[0], &chunk.rows[0]),
        ("insert 2", &inserted[1], &chunk.rows[1]),
        ("insert 3", &inserted[2], &chunk.rows[2]),
        ("update before", &before, &inserted[3]),
        ("update after", &after, &chunk.rows[3]),
    ];
    let mut failures = vec![];
    for (what, stream, read) in pairs {
        for d in differ(stream, read) {
            failures.push(format!("{what}: {d}"));
        }
    }
    assert!(
        failures.is_empty(),
        "stream vs chunk:\n{}",
        failures.join("\n")
    );
    // A few values pinned, so both paths can't be wrong alike.
    let high = &chunk.rows[1];
    assert_eq!(high.get("biu"), Some(&ColumnValue::UInt64(u64::MAX)));
    assert_eq!(high.get("b"), Some(&ColumnValue::Bool(true)));
    assert_eq!(high.get("bt64"), Some(&ColumnValue::Bytes(vec![0xff; 8])));
    assert_eq!(chunk.rows[0].get("bi"), Some(&ColumnValue::Int64(i64::MIN)));
    assert!(
        chunk.rows[2]
            .0
            .iter()
            .skip(1)
            .all(|(_, v)| *v == ColumnValue::Null)
    );
    cleanup(&n).await;
}
```

- [ ] **Step 2: Run the tests**

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql roundtrip:: || break; done`

Expected: PASS, 1 test on each target. These tests drive the code of Tasks 7–14, so nothing fails first. A failure is a defect in that code: stop and report it.

- [ ] **Step 3: (nothing to implement)**

Nothing: Step 1 wrote the whole task.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `for t in mysql-84 mysql-84-nogtid mariadb-118 mariadb-118-binlog; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --test mysql || break; done`

Expected: PASS, 39 tests on each target (`test-infra/up.sh` first).

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`

Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/tests/mysql/main.rs \
  crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs
git commit -m "test(source-mysql): every type read the same from the binlog and a chunk — extremes, NULL, ordinary values, an update"
```

---

### Task 16: CI: four targets per change, MariaDB 11.4 and 10.11 weekly

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/weekly.yml`
- Modify: `test-infra/README.md`

**Interfaces:**
- Consumes:
  - `test-infra/compose.yaml` (Task 3, profile `matrix`); the crate's test binaries.
- Produces:
  - Nothing public: the change is inside the crate.

P-8 for the MySQL source. `ci.yml` runs the workspace's integration tests against `mysql-84` (the
default target), then the crate's tests against `mysql-84-nogtid`, `mariadb-118` and
`mariadb-118-binlog`. `weekly.yml` gains a `mariadb` job: `mariadb-114` and `mariadb-1011`, one
job each, through `DATA_AVALANCHE_MYSQL` (the TLS tests skip themselves off `mysql-84`). The
whole crate suite was run on both servers while planning (73 unit, 39 + 1 + 1 integration
tests). `test-infra/README.md` gains a "MySQL and MariaDB" section. The slice-1 E2E parity suite
on the three position modes is M7b's.

- [ ] **Step 1: (nothing to test first)**

Nothing: the change has no behaviour to test before it exists.

- [ ] **Step 2: Run the tests**

Nothing to run.

- [ ] **Step 3: Change the workflows and the README**

Change `.github/workflows/ci.yml`:

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -92,6 +92,13 @@ jobs:
         run: test-infra/up.sh
       - name: Integration tests
         run: cargo test --locked --workspace -- --test-threads=1
+      # The MySQL source on its other per-change targets (P-8; the run above reads mysql-84):
+      # file/position mode on MySQL 8.4, and MariaDB 11.8 in GTID and file/position mode.
+      - name: MySQL source, other targets
+        run: |
+          for target in mysql-84-nogtid mariadb-118 mariadb-118-binlog; do
+            DATA_AVALANCHE_MYSQL=$target cargo test --locked -p data-avalanche-source-mysql --tests
+          done
       # The slice-1 E2E suite (roadmap §5.3): the binary as a process, built with its crash points.
       - name: Slice-1 E2E
         run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
```

Change `.github/workflows/weekly.yml`:

```diff
diff --git a/.github/workflows/weekly.yml b/.github/workflows/weekly.yml
--- a/.github/workflows/weekly.yml
+++ b/.github/workflows/weekly.yml
@@ -4,8 +4,10 @@ name: weekly
 # servers (P-8, Q-M5-7, Q-M5-23, roadmap §4 M6b exit): PostgreSQL 14–17, one job each, the
 # `postgres-18` suites of ci.yml re-run through DATA_AVALANCHE_PG_PORT. The E2E suite also needs
 # RustFS and uv (its DuckDB read).
+# The MySQL source against the older MariaDB LTS servers (P-8): 11.4 and 10.11, GTID mode, through
+# DATA_AVALANCHE_MYSQL.
 # Weekly (Mondays 03:41 UTC) and on manual dispatch. The TLS tests need `postgres-tls`
-# (PostgreSQL 18) and run in ci.yml only. No secrets.
+# (PostgreSQL 18) or `mysql-tls` (MySQL 8.4) and run in ci.yml only. No secrets.
 
 on:
   schedule:
@@ -50,3 +52,29 @@ jobs:
       - name: Stop the containers
         if: always()
         run: docker compose -f test-infra/compose.yaml --profile matrix down -v
+
+  mariadb:
+    runs-on: ubuntu-latest
+    timeout-minutes: 60
+    strategy:
+      fail-fast: false
+      matrix:
+        target: [mariadb-114, mariadb-1011]
+    env:
+      DATA_AVALANCHE_IT: "1"
+      DATA_AVALANCHE_SKIP_GCS: "1"
+      DATA_AVALANCHE_MYSQL: ${{ matrix.target }}
+    steps:
+      - uses: actions/checkout@v7
+      - uses: actions-rust-lang/setup-rust-toolchain@v2
+      - name: Start ${{ matrix.target }}
+        run: docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "${{ matrix.target }}"
+      # The TLS tests skip themselves off the mysql-84 target.
+      - name: MySQL source tests
+        run: cargo test --locked -p data-avalanche-source-mysql --tests
+      - name: Container logs on failure
+        if: failure()
+        run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "${{ matrix.target }}"
+      - name: Stop the containers
+        if: always()
+        run: docker compose -f test-infra/compose.yaml --profile matrix down -v
```

Change `test-infra/README.md`:

````diff
diff --git a/test-infra/README.md b/test-infra/README.md
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -44,6 +44,25 @@ the `verify-ca` test needs), and a client certificate for the role
 client pair to `test-infra/.tls/` (git-ignored) for the TLS tests; the server key stays in the
 container. Nothing is committed.
 
+## MySQL and MariaDB
+
+The MySQL source's tests (`crates/data-avalanche-source-mysql/tests`) read the target
+`DATA_AVALANCHE_MYSQL`: a server of the table above and the replication mode the tests use on it.
+`ci.yml` runs them on every change against `mysql-84` (the default), `mysql-84-nogtid`
+(file/position), `mariadb-118` (GTID) and `mariadb-118-binlog` (`mariadb-118` in file/position
+mode); `.github/workflows/weekly.yml` runs `mariadb-114` and `mariadb-1011` (GTID) every Monday:
+
+```sh
+docker compose -f test-infra/compose.yaml --profile matrix up -d --wait mariadb-114
+DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=mariadb-114 cargo test -p data-avalanche-source-mysql --tests
+```
+
+The TLS tests run with the `mysql-84` target only, against `mysql-tls`. Two test binaries change
+server-wide state, and cargo runs each alone, never beside the `mysql` tests: `binlog_purge`
+(`PURGE BINARY LOGS` removes the older binlogs of the target) and `mariadb_compress` (turns
+`log_bin_compress` on, and back to the server default before it asserts). MariaDB keeps a binlog
+it just closed until its binlog checkpoint moves on, so the purge test retries for up to 5 s.
+
 ## GCS: a real bucket, not an emulator
 
 `fake-gcs-server` 1.56.1 rejects `object_store`'s path-style PUT (`400 invalid uploadType`) and
````

- [ ] **Step 4: Check it**

Run: `python3 -c "import yaml; [yaml.safe_load(open(f)) for f in ['.github/workflows/ci.yml', '.github/workflows/weekly.yml']]"`

Run: `docker compose -f test-infra/compose.yaml --profile matrix up -d --wait mariadb-114 mariadb-1011`

Run: `for t in mariadb-114 mariadb-1011; do DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=$t cargo test -p data-avalanche-source-mysql --tests || break; done`

Expected: the workflows parse (no output); then every test passes on both servers.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml \
  .github/workflows/weekly.yml \
  test-infra/README.md
git commit -m "ci: the MySQL source on four targets per change, MariaDB 11.4 and 10.11 weekly"
```

---

## Verified during planning (2026-10-14)

- Every task was built and run in a scratch workspace, one commit per task, before this plan was
  written from those commits. The integration tests passed on `mysql-84`, `mysql-84-nogtid`,
  `mariadb-118` and `mariadb-118-binlog` at each task, and the whole crate suite on
  `mariadb-114` and `mariadb-1011` at the end: 73 unit tests, 39 in `tests/mysql`, 1 in
  `tests/binlog_purge.rs`, 1 in `tests/mariadb_compress.rs` (a no-op on MySQL).
- The plan was then replayed from its own code blocks at the base commit (`0b439ba`): each task's
  Step 1 applied, Step 2 run (the failures above are its output), Step 3 applied, and the tree
  compared with the verified commit: identical for all 16 tasks. Integration counts are the
  replay's `--list` of each run; their passes are the scratch runs.
- On the final tree, with the PostgreSQL 18 and RustFS services: the PostgreSQL source's
  integration tests (78 + 51 + 1 + 1, the TLS tests skipped: they read the repository's
  `test-infra/.tls`), the pipeline worker's (198 + 2 + 11 + 1 + 1) and the slice-1 E2E suite
  (26 tests) pass: Task 1's and Task 9's changes to those crates break nothing. One E2E run
  beside the replay's builds failed once in `streaming::a_large_transaction_lands_at_once` (the
  50,000-row transaction counted more than one Delta commit; its rows were never seen in part).
  The test alone and the whole suite again, without the load, passed: a load-dependent flake
  of an M6b test, not reproduced, to watch in CI.
- Mutation checks: the type round trip fails when the binlog's `YEAR` is read one too high; the
  snapshot test fails under a `utf8mb4_bin` cursor collation. Removing the decimal key's `CAST`
  still passes (both servers compare a plain parameter exactly, measured): the `CAST` stays as the
  explicit form, and its comment says so.

## M7a exit checklist

- [ ] `data-avalanche-source-mysql` implements every `SourceConnector` method; the worker builds
      with `Checkpoint` (Task 1).
- [ ] The integration suite passes on `mysql-84`, `mysql-84-nogtid`, `mariadb-118` and
      `mariadb-118-binlog` in `ci.yml`, and on `mariadb-114` and `mariadb-1011` in `weekly.yml`.
- [ ] Every Review Focus line has its passing test.
- [ ] The documents of "Documents this plan changes" are updated; the spec patches are applied or
      recorded as declined.
- [ ] The workspace gates pass: `cargo fmt --all --check`, `cargo clippy --workspace
      --all-targets -- -D warnings`, `cargo test --workspace`.
- [ ] Carried to M7b: the worker's MySQL checkpoint (`PositionDoc` with its server), the binary's
      wiring, the slice-1 parity E2E in the three position modes (MySQL 8.4 and MariaDB 11.8 per
      change, 10.11 / 11.4 weekly), the payload fault-injection E2E, and the status of the
      `BINLOG_*` pipeline halts. From the whole-branch review: an error, not an empty identity,
      when `server_uuid` reads NULL; a file/position resume IT; the pipeline dropping a halted
      table's buffered rows (a halt mid-rows-event); `re_snapshot` at start end to end through the
      worker (the source's half is built).

## Deviations during execution

- **Task 2, placement (2026-10-14).** The V-120 loop in `rules/identity.rs` landed after the
  V-059 loop instead of before it, and the test helper `two_mysql` after `two_pipelines`. A
  report sorts its errors and warnings, so the output is the same; no later task touches either
  file. Kept as committed.
- **Task 4, the review fixes (2026-10-14, approved by Oleksii).** The task review found two
  edge cases in the preflight:
  - Any server error from the binlog status statement was read as a missing privilege, so a
    server shutting down (1053) or a lock wait halted the pipeline with a "grant …" refusal
    instead of a retry. Now only an access-denied error (1044, 1142, 1227, `server::access_denied`)
    is the refusal; any other goes through `error::mysql` like every statement.
  - `grants_replication` matched `REPLICATION SLAVE` as a substring, so MariaDB's separate
    `REPLICATION SLAVE ADMIN` (10.5+), which can't dump the binlog, passed. Privilege names are
    now compared whole.

  Each has a unit test (`only_a_privilege_error_refuses_the_status_statement`, two new
  assertions in `replication_comes_from_a_global_grant`); the preflight ITs pass on the four
  targets (the refusal case proves the servers answer 1227). Folded into Task 4's commit. No
  later task's code changes: they touch only these items' `use` lines. The review's minor notes
  are parked in the SDD ledger.
- **Task 6, the review fix (2026-10-15, approved by Oleksii).** `binlog::inflate` sized its
  buffer by the uncompressed length in the block's header (up to 4 GiB) before checking anything,
  and inflated without a limit, so a corrupt block or a zlib bomb could exhaust memory instead of
  returning the error that halts the pipeline. Now the preallocation is capped at 1 MiB and the
  inflate reads at most one byte past the header's length, so the existing length check refuses
  the excess. Two unit tests (`a_header_claiming_more_than_the_block_inflates_is_refused`,
  `a_block_inflating_past_its_header_is_refused`). Folded into Task 6's commit; no later task
  touches `binlog.rs`. With Task 4's test, the crate's lib test count from Task 6 on is three
  above the plan's figures (Task 6: 29, not 26).
- **Task 7, the review fix (2026-10-15, approved by Oleksii).** A `SET` value from the binlog
  became labels only for the bits that have members, so a set bit beyond them (a member list
  older than the row) was dropped without a word, while an `ENUM` index beyond its members is an
  error. Now such a bit is an error too (`SET bit N of M members`), and the pipeline halts. One
  assertion in the existing test of ENUM, SET and JSON values; the lib test count is unchanged
  (Task 7: 44, the plan's 41 plus the three earlier tests). Folded into Task 7's commit; later
  tasks' hunks in `value.rs` are far from it and apply unchanged.
- **Task 11, the review fixes (2026-10-15, approved by Oleksii).** Two statements that drop or
  move a tracked table's rows could pass the classifier as harmless:
  - `DROP DATABASE` and MariaDB's `CREATE OR REPLACE DATABASE` with a name the tokenizer can't
    read as an identifier (`"shop"` under `ANSI_QUOTES`, or none) left `dropped_database` empty.
    Such a statement is now `unreadable`, which halts the pipeline.
  - `ALTER TABLE p EXCHANGE PARTITION … WITH TABLE x` and MariaDB's `ALTER TABLE p CONVERT TABLE
    x TO PARTITION …` recorded only `p` as changed without row events. `x` is now in `unlogged`
    too (in its own schema when qualified), so a tracked `x` halts (`UNSAFE_DDL`); an `x` that
    can't be read makes the statement `unreadable`.

  Assertions in the existing tests (`partition_operations_that_move_rows_are_unlogged` and the
  test of table statements); the lib test count is unchanged (Task 11: 64). Folded into Task
  11's commit; no later task touches `ddl.rs`. The review's other notes are parked in the SDD
  ledger.
- **Task 12, the review fixes (2026-10-15, approved by Oleksii).** `schema::scope` builds the
  schema as of a statement, which the stream decodes rows by, position for position. Three gaps:
  - A moved column (`MODIFY x … AFTER y` or `FIRST`, `CHANGE a b … AFTER y`) kept its old place,
    so rows after it could be read into the wrong columns without an error wherever the types
    agree: the table map is checked by name only under `binlog_row_metadata = FULL`, which is
    a warning, not a requirement. Now every scoped column the catalog has goes after its
    nearest preceding catalog column, in the catalog's order (as an added column already did).
  - Renames were applied one at a time, so a swap (`CHANGE a b, CHANGE b a`), a chain, or `DROP
    b, CHANGE a b` left a duplicate name and halted the table. They are applied at once; a
    column whose name only a rename takes is gone, and `diff` reports its drop before the
    rename.
  - `source_ordinal` was copied from the catalog, which, when ahead of the stream, gave
    duplicates and gaps. Positions are numbered in the result's order.

  One new unit test (`moves_swaps_chains_and_taken_names`), so the lib test count from Task 12
  on is four above the plan's figures (Task 12: 71, not 67). Folded into Task 12's commit; Task
  13's hunk in `schema.rs` (`augment`) is below `scope` and applies unchanged. Known limit, as
  before: when the catalog is ahead by a later statement that moves a column, a scoped column
  is placed by the later order; the table map catches that only by type or (under FULL) name.
- **Task 13, the review fix (2026-10-15, approved by Oleksii).** Preflight checks
  `binlog_format = ROW` once, globally, but a session can still set STATEMENT or MIXED. Its
  `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, `LOAD DATA` (or `WITH … UPDATE/DELETE`) is then
  logged as a QUERY event with no row events. The classifier read it as harmless, and the stream
  committed past rows it never saw.
  - The classifier marks such a statement (`Ddl::logged_as_statement`, with the names it
    qualifies others with), and the stream halts the pipeline when the statement runs in the
    source's database or names it. A trigger or a multi-table statement can reach any table
    there, so the halt is for the pipeline, not one table. A statement in another database is
    not the source's, so an unrelated application's traffic doesn't stop the pipeline.
  - Not seen: a statement in another database whose trigger writes into the source's; a
    qualifier the tokenizer reads as a string (`"shop".t` under `ANSI_QUOTES`) or in another
    case (`lower_case_table_names`). A MEMORY table the server empties at restart is logged as
    `DELETE FROM`, and in the source's database it halts (its rows did go without row events).
  - The new integration test exposed a bug in the plan's stream: MariaDB writes no `BEGIN` event
    (a transaction's GTID event stands for it), and the stream never took that from the GTID
    event's standalone flag. A QUERY inside a MariaDB transaction (as statement format writes)
    committed it early, and its XID then failed the stream ("a commit outside a transaction").
    The stream now opens the transaction from the GTID event. Under ROW format alone no QUERY
    comes inside a MariaDB transaction, which is why no earlier test met it.

  Unit assertions in the classifier's tests; one new stream integration test
  (`a_row_change_logged_as_its_statement_halts_the_stream`: a statement-format transaction in
  another database streams on, one in the source's database halts). From Task 13 on, the
  `--test mysql` count is one above the plan's figures (Task 13: 34 per target, stream 8). Folded
  into Task 13's commit (`ddl.rs` included); Task 14's hunks in `stream.rs` apply unchanged, and
  no later task touches `ddl.rs` or `tests/mysql/stream.rs`.
- **The whole-branch review's fixes (2026-10-15/16, approved by Oleksii).** The review found one
  Critical and five Important issues across the tasks; all were fixed in four commits after Task
  16, with the spec patches `acecb41` and `3b516e2`:
  - **ENUM and SET labels kept their case only in snapshot chunks.** The catalog read lowercased
    `COLUMN_TYPE`, so the stream wrote `'active'` where the source held `'Active'`, and an ENUM key
    with an upper-case member broke the chunk reads. `COLUMN_TYPE` is kept as the server returns
    it; keyword tests read a lowercased copy. Column names now compare without case, as MySQL's do.
  - **`re_snapshot` at start.** `prepare` returned `PositionUnreachable` for a purged or
    other-dialect checkpoint, which the worker handles only once streaming. It now returns
    `StartPlan::Snapshot { reason: PositionLost }` with a warning, as the PostgreSQL source does;
    the dump's own 1236 is unchanged.
  - **Keep-alives past untracked commits.** The server sends a heartbeat only while the dump is
    idle, so on a busy server whose tracked tables are rarely written the position never moved.
    A transaction without tracked rows now sends a keep-alive (at most one a second).
  - **`binlog_row_metadata = FULL` is required** (DT-R6 resolved; config §11.2, `Schema_Change…`
    §2.2). A table map without names halts its table, and each table map is checked against the
    decode schema for names, signedness (YEAR excepted: the server flags it unsigned), character
    sets (collation ids mapped through `information_schema.COLLATIONS` on MySQL and
    `COLLATION_CHARACTER_SET_APPLICABILITY` on MariaDB, whose `COLLATIONS` gives the uca1400
    collations no id; measured) and ENUM/SET labels (only counted in a character set the source
    doesn't read). The last matching table map is cached.
  - **The binlog must carry the source's changes** (config §11.2). Any `binlog_do_db` /
    `binlog_ignore_db` filter refuses the pipeline: the server filters DDL and `TRUNCATE` by the
    session's database, so even a filter that admits `source.db` can drop them. With
    `log_replica_updates` (MariaDB `log_slave_updates`, OFF by default) off, a server that
    replicates from another is refused; a user that can't read the replication status (MariaDB
    asks for `SLAVE MONITOR`, measured) gets a warning.
  - **Names fold under `lower_case_table_names` ≠ 0**: table and database names in statements
    compare without case, so `TRUNCATE TABLE Orders` reaches the tracked `orders`.
  - **Rollbacks after tracked rows halt.** MariaDB logs the rows a rollback undid in a transaction
    with non-transactional changes, followed by `ROLLBACK TO` or `ROLLBACK` (measured on 10.11,
    11.4 and 11.8); the stream had committed them, rows the source doesn't hold. Such a
    transaction with tracked rows now halts the pipeline. MySQL 8.4 logs none of them, and with
    GTIDs refuses the transaction (1785).
  - **Smaller fixes:** file mode follows a rotate between transactions; MySQL's `Begin.xid` is the
    session counter (D-M7a.19); a statement from another database that doesn't contain the
    source's name is routed nowhere unread (one the classifier can't read there no longer
    halts); crate-private functions where nothing outside needs them, and the remaining public
    type re-exported; doc fixes.

  New tests: unit tests for each check; ITs for the mixed-case labels, `re_snapshot` at start,
  keep-alives, rollbacks; two new test binaries run alone: `row_metadata` (MINIMAL refused at
  preflight and halting the stream, FULL restored however the test ends) and `binlog_rotate`
  (`FLUSH BINARY LOGS`, then the next file's start). From here the lib test count is 85 and
  `--test mysql` 42 per target. Known limits, parked: a Galera node with `log_slave_updates` off
  isn't detected as a replica (its replication status is empty); a case-only column rename reads
  as a rename, which the Delta sink doesn't apply.
