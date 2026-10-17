---
updated: 2026-10-17
domain: Data Flow
tags: [v1, implementation-plan, m7b, mysql, mariadb, checkpoint, server-identity, e2e, fault-injection, binlog]
resolves: []
sources: []
---

# M7b — The MySQL Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run a MySQL or MariaDB pipeline end to end, the way M6 runs a PostgreSQL one. The worker
keeps a MySQL checkpoint: the position document, with the server it was read from. The binary
builds the MySQL source for a `type: mysql` pipeline. A pipeline-scope halt
(`BINLOG_PAYLOAD_INVALID`, `BINLOG_SOURCE_CHANGED`) puts the pipeline in `ERROR` with its code. The
slice-1 E2E suite runs on either source, across the three position modes. On every change it runs
on MySQL 8.4 with GTIDs on and off and on MariaDB 11.8 in GTID mode. Weekly it runs on MariaDB
11.8 in file/position mode and on 11.4 and 10.11. A corrupted compressed
transaction, injected on the wire, halts the pipeline with nothing lost.

M7 is split (Q-M7a-1): M7a built `data-avalanche-source-mysql` and drove it directly. **M7b** (this
plan) wires it into the worker and the binary and runs the parity suites. M7a's whole-branch review
carried four items here, each in a task below:
- an error, not an empty identity, when `server_uuid` reads NULL (Task 4);
- a file/position resume IT (Task 4);
- the pipeline dropping a halted table's buffered rows, a halt mid-rows-event (Task 2);
- `re_snapshot` at start, end to end through the worker (Task 7).

**Architecture:** The source contract changes once, in core. The rest follows it.

- **`data-avalanche-core`** (Task 1): `open_stream` takes a `Checkpoint` (the position and the
  server it was recorded on), and `reset_position` returns one. A reconnect opens the stream
  without `prepare`, so the stream's open must see the server to check it. A lost position's
  replacement must name the server it was read on, for the next checkpoint.
  `SourceError::halt_code` reads a pipeline-scope halt's code from its message (D-M7a.8).
- **`data-avalanche-pipeline`** (Task 2): a checkpoint store, `checkpoint::Store`, either
  PostgreSQL's slot document or MySQL's position document. Phase 0, the session and the writer
  load and save through it. The core keeps the server the last start read (`Prepared.server`, or
  the reset position's), and every checkpoint is written with it. The PostgreSQL-only refusal
  goes.
- **`data-avalanche-cluster`** (Task 3): `Actual::Error` gains `code`, and the `ERROR` log line
  carries it as a field (Q-M7b-4).
- **`data-avalanche-source-mysql`** (Task 4): in file/position mode the dump checks the server on
  its own connection (`BINLOG_SOURCE_CHANGED` at a reconnect too). A server that reports no
  identity is an error. A file/position checkpoint resumes after itself (IT).
- **`data-avalanche`** (Task 5): the factory builds the source by `source.type`.
- **The E2E suite** (Tasks 6, 7): one suite, two sources (Q-M7b-1).
  - Task 6: the harness drives PostgreSQL or MySQL by `DATA_AVALANCHE_E2E_SOURCE`. Shared
    scenarios use dialect helpers. A scenario of one source skips itself on the other.
  - Task 7: the MySQL scenarios. A test-only wire proxy (Q-M7b-3) corrupts a compressed
    transaction and fails a connection over to another server. A purged checkpoint follows
    `binlog_purge_action`. The types read back in DuckDB.
- **CI** (Task 8): a parallel `e2e-mysql` job per change (`mysql-84`, `mysql-84-nogtid`,
  `mariadb-118`). `weekly.yml` runs `mariadb-118-binlog`, `mariadb-114` and `mariadb-1011`
  (Q-M7b-2).

**Tech Stack:** Rust 1.98.1 (edition 2024). No new external crate: the binary's dev-dependencies
gain `mysql_async` (`=0.37.1`, the workspace's) and `flate2` (`1`, the workspace's) for the E2E
suite's admin connection and the proxy's CRC32.

**Spec:**
- `doc/v1.0/00-roadmap.md`: §3.6 (the source connector), §3.7–§3.11 (the worker, its status, the
  controller), §4 M7 (M7b scope and exit), §5.3 (the slice-1 E2E suite), §5.4 (CI), §9.1 Q-M7-2.
- `doc/v1.0/M7a-mysql-source.md`: D-M7a.1 (the checkpoint and its server), D-M7a.8 (a
  pipeline-scope halt's code), D-M7a.20 (the purge action), D-M7a.21 (`retained`), the carry
  list of its exit.
- `doc/specs/Data-Flow.md`: Phase 0 (the start), Phase 2b (the MySQL stream, file/position mode
  and `BINLOG_SOURCE_CHANGED`, the payload check), Phase 5 (the MySQL checkpoint in
  `replication/position.json`), §Error Paths, §State Layout.
- `doc/specs/Configuration_Specification.md`: §9.2.2 `source.mysql` (`replication_type`,
  `binlog_purge_action`, `heartbeat`), §9.3 (bare-name patterns, V-064).
- `doc/research/V1_Scope_Decisions.md` D-V1.5 (MariaDB in v1), D-V1.6 (the payload check).
- `doc/research/Type_Mapping.md`: the MySQL and MariaDB tables, Hop 3b (the E2E type scenario).

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` pass before
  every commit. The binary's feature is linted too:
  `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`.
- **Dependency direction** (roadmap §2): `pipeline → core, config, state`; `cluster → pipeline`;
  the binary → every crate, `source-mysql` included (new in Task 5). `testkit` is a
  dev-dependency only.
- **Positions and servers:** a MySQL checkpoint is `replication/position.json`,
  `PositionDoc { position, server }` (M2's document, unchanged); a PostgreSQL one stays
  `replication/slot.json`. A pipeline-scope halt is `SourceError::Permanent` whose message starts
  with the code and `": "` (D-M7a.8).
- **Test gates:** plain `cargo test` stays hermetic. Integration tests gate on
  `DATA_AVALANCHE_IT`. The MySQL ITs read `DATA_AVALANCHE_MYSQL` (default `mysql-84`). The E2E
  suite needs the `e2e-hooks` feature and reads `DATA_AVALANCHE_E2E_SOURCE` (`postgres`, the
  default, or `mysql`) and `DATA_AVALANCHE_MYSQL` or `DATA_AVALANCHE_PG_PORT`. It runs with
  `--test-threads=1`.
- **Credentials:** the tests use the compose services' fixed test credentials only; nothing else
  goes in code, docs or output. Never print the files under `test-infra/.tls/`.
- **Public-API conventions:** public types are re-exported at the crate root; errors are the
  crates' existing `thiserror` types.
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready;
  a step's diffs apply in the order given, after the earlier steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to break a MySQL mirror. Each is pinned by a test in the task
after the arrow.

1. **A failover behind the pipeline's address in file/position mode, met at a reconnect.** A
   reconnect opens the stream without `prepare`. The open must still find that the server
   changed, and halt `BINLOG_SOURCE_CHANGED`, never stream another server's files from a
   position that means nothing there → Task 4 (`a_file_position_stream_opens_only_on_its_server`),
   Task 7 (`a_failover_in_file_position_mode_halts_the_pipeline`).
2. **A position lost while running, on MySQL.** The replacement checkpoint must name the server
   the reset read, not the one before. Else the next start in file mode halts for nothing, or
   accepts the wrong server → Task 2 (`a_lost_mysql_position_records_the_resets_server`,
   `a_server_is_reported_and_the_checkpoints_kept`).
3. **A halt in the middle of a transaction** (MySQL: a rows event whose later row can't be
   decoded). The table's rows of that transaction must go, before and after the halt; its earlier
   committed rows and the other tables' rows must land → Task 2
   (`a_halt_mid_transaction_drops_the_tables_rows_of_it`).
4. **A compressed transaction whose events don't all come out.** The pipeline halts with the
   code, and nothing of the transaction lands; a restart without the fault reads it whole, so
   nothing is lost → Task 7 (`a_corrupted_compressed_transaction_halts_the_pipeline`,
   `only_a_transaction_payload_is_corrupted_and_its_checksum_still_holds`).
5. **A server that reports no identity** (`@@server_uuid` NULL or empty). A checkpoint recorded
   with an empty identity would match nothing later, or everything. It must be an error at once
   → Task 4 (`a_server_that_reports_no_identity_is_an_error`). And a checkpoint the store can't
   hold (a MySQL position with no server) is a protocol error, never a silent write → Task 2
   (`a_checkpoint_the_store_cant_hold_is_a_protocol_error`).

## Decisions and rulings

**Asked and answered** (2026-10-17, all four "Recommended"):

| # | Question | Answer |
|---|---|---|
| Q-M7b-1 | How does the E2E suite run on MySQL? | **One suite, two sources**: a harness source picked by `DATA_AVALANCHE_E2E_SOURCE` (and `DATA_AVALANCHE_MYSQL`); shared scenarios use dialect helpers; source-only scenarios skip themselves (PostgreSQL: TOAST, the WAL guard, a lost slot, its types; MySQL: a corrupted payload, a failover, a purged checkpoint, its types) |
| Q-M7b-2 | Which targets, where in CI? | **The three position modes per change, in their own job**: a parallel `e2e-mysql` matrix job in `ci.yml` (`mysql-84`, `mysql-84-nogtid`, `mariadb-118`); `weekly.yml` runs `mariadb-118-binlog`, `mariadb-114`, `mariadb-1011` |
| Q-M7b-3 | How is a corrupted payload injected? | **A test-only TCP wire proxy** in the E2E suite: it passes packets through and, on request, corrupts the next `TRANSACTION_PAYLOAD` a dump sends (one byte flipped, the CRC32 recomputed) |
| Q-M7b-4 | How does the status show a pipeline-scope halt? | **A structured code**: `Actual::Error` gains `code: Option<String>`, read from the message's prefix; the `ERROR` log line carries it as a field; no new metric. The E2E asserts `status="error"` and the code |

**Rulings where the plan decides** (each pinned by a test). **Plan approved 2026-10-17**, with these rulings (roadmap D-IMPL-16):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M7b.1 | `SourceConnector::open_stream(from: &Checkpoint, …)` and `reset_position() -> Checkpoint`. PostgreSQL's impl passes `from.position` on and its reset has no server; MySQL's reset reads the write position and the server (`checkpoint_now`) | A reconnect (`Session::open`) opens the stream without `prepare`: only the open can check the server. The lost-position path skips `prepare` too, and its new checkpoint needs a server | Task 2 (`a_server_is_reported_and_the_checkpoints_kept`, `a_lost_mysql_position_records_the_resets_server`), Task 4 (`a_file_position_stream_opens_only_on_its_server`) |
| D-M7b.2 | `SourceError::halt_code`: a `Permanent` whose message starts with `[A-Z][A-Z0-9_]*` holding a `_`, then `": "`; `WorkerError::code` reads it from a source error only | D-M7a.8 put the code in the message; the status must not mistake prose for a code | Task 1 (`a_pipeline_halt_names_its_code`), Task 3 (`a_source_halt_names_its_code`) |
| D-M7b.3 | The worker's checkpoint goes through `checkpoint::Store`: `Slot { name, intent }` (PostgreSQL) or `Position` (MySQL), from `Settings::new`. It replaces `Settings.slot` and `slot_intent`. `Store::doc` refuses a checkpoint it can't hold (a non-LSN in a slot, a MySQL position without a server) as a protocol error | One code path for Phase 0, the session and the writer; a wrong write must fail loudly | Task 2 (`a_slot_holds_a_postgresql_checkpoint`, `a_position_document_holds_the_server_with_the_position`, `a_checkpoint_the_store_cant_hold_is_a_protocol_error`) |
| D-M7b.4 | `Core.server` is the server the last start read: `Prepared.server` (resume or snapshot), or the reset position's. A reconnect keeps it; the writer records it with every checkpoint. A resume rewrites nothing: the next checkpoint records the server read now | In file mode a resume implies the same server (`prepare` checks it). In GTID mode a failover between runs is legal, and the first checkpoint after it records the new server | Task 2 (`a_mysql_restart_hands_prepare_the_recorded_server`, `a_lost_mysql_position_records_the_resets_server`) |
| D-M7b.5 | A GTID failover met at a reconnect (no `prepare`) keeps the start's server in the checkpoints until the next start | GTID positions carry across servers; the recorded server matters only in file mode, where the dump checks it (D-M7b.7) | Task 2 (ruling only; no behaviour depends on it) |
| D-M7b.6 | `Settings.heartbeat` is `source.postgres.heartbeat` or `source.mysql.heartbeat`. The watchdog's text for a stream the source doesn't run comes from the store: "no stream reads the slot", or "the server runs no binlog dump for the stream" | V-121 made the MySQL heartbeat the watchdog's pace; a MySQL operator has no slot | Task 2 (`a_mysql_pipeline_keeps_a_position_document`, `a_stream_the_source_doesnt_run_is_named_for_the_source`) |
| D-M7b.7 | In file/position mode `open_stream` reads the identity on the dump's own connection, after connecting and before the dump command, and halts `BINLOG_SOURCE_CHANGED` (D-M7a's `same_server`) on another server or none recorded. GTID modes don't check | The control connection could reach another server than the dump (DNS moves between two connects); only the dump's own connection proves its server | Task 4 (`a_file_position_stream_opens_only_on_its_server`) |
| D-M7b.8 | `identity()`: no row, a NULL or an empty value is `Permanent` ("the server reports no @@server_uuid"); a MariaDB `server_id` that isn't a number is `Permanent` | An empty identity recorded with a checkpoint could never be checked against a server (M7a's review) | Task 4 (`a_server_that_reports_no_identity_is_an_error`) |
| D-M7b.9 | A halted table's buffered rows: no code change. `Session::halt` flushes the table's committed changes first, and `Accumulator::halt_table` drops its buffer, open transaction included | Read in M7b planning (`session.rs` `halt`, `accumulator.rs` `halt_table`); the carry item needs a test, not a change | Task 2 (`a_halt_mid_transaction_drops_the_tables_rows_of_it`) |
| D-M7b.10 | The binary builds `PgConnector` or `MySqlConnector` by `source.type`; the binary depends on `data-avalanche-core` (the trait object) and `data-avalanche-source-mysql` | A `type: mysql` pipeline got the PostgreSQL connector, which refuses it | Task 5 (`a_mysql_pipeline_gets_its_parts`) |
| D-M7b.11 | The E2E harness on MySQL: per test, a database and a user named `e2e_<test>` (`SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.*`). "Streaming" is a `Binlog Dump%` command of that user in `PROCESSLIST`. "Acknowledged" is the position document covering the position (the binlog keeps no position for the pipeline), so `crash_ack`'s "the crash came before the ack" is PostgreSQL's alone | The user tells the pipeline's dump from any other; MySQL's checkpoint is its acknowledgment | Task 6 (the shared scenarios on MySQL) |
| D-M7b.12 | Table patterns in a scenario write `@SCHEMA@.` for the qualifier: `e2e_<test>.` on PostgreSQL, nothing on MySQL (V-064). Large series are one statement: PostgreSQL's `generate_series`, MySQL's recursive CTE with the session ceiling raised (MySQL `cte_max_recursion_depth`, MariaDB `max_recursive_iterations`, both 1000 by default, measured). A two-statement transaction on MySQL is wrapped in `BEGIN … COMMIT` (a MySQL batch commits per statement; a PostgreSQL batch is one implicit transaction) | The scenarios assert the same outcomes on both | Task 6 |
| D-M7b.13 | The stop scenario on MySQL sets `mysql.heartbeat: 1s` | A MySQL dump thread sees a closed connection only at its next write, a heartbeat at the latest (measured: past 30 s at the default 60 s) | Task 6 (`pause_stop_and_resume_hold_across_a_restart`) |
| D-M7b.14 | The proxy: it reads protocol packets (the suite's pipelines use `ssl_mode: disable`). After a client's `COM_BINLOG_DUMP` (0x12) or `COM_BINLOG_DUMP_GTID` (0x1e) it corrupts the next server packet holding a `TRANSACTION_PAYLOAD` event (type 40): one byte of the compressed body flipped, the event's CRC32 recomputed. `switch(port)` sends new connections to another server and cuts the open ones | The CRC is recomputed so that only the decompression can find the fault, the case D-V1.6 guards (`mysql_common` checks no dump checksum, measured) | Task 7 (`only_a_transaction_payload_is_corrupted_and_its_checksum_still_holds`, the payload and failover scenarios) |
| D-M7b.15 | The payload scenario runs on MySQL only; the failover scenario on `mysql-84-nogtid` only, failing over to `mysql-84` | MariaDB has no `TRANSACTION_PAYLOAD` (its compressed events are M7a's `mariadb_compress` IT). MariaDB tells servers apart by `server_id`, and every test-infra server has `--server-id=1` | Task 7 |
| D-M7b.16 | A purged checkpoint under `binlog_purge_action: error` shows `status="error"` and its message, with no code | The spec names two pipeline-scope codes (`BINLOG_PAYLOAD_INVALID`, `BINLOG_SOURCE_CHANGED`); a purge has none | Task 7 (`a_purged_checkpoint_halts_under_error`) |

**Measured while planning** (2026-10-17, recorded in the tasks):

- The proxy passes the protocol through untouched (a control run streamed two transactions and
  three rows). A corrupted payload, with its CRC fixed or not, yields no inner events, and the
  stream ends `Permanent("BINLOG_PAYLOAD_INVALID: the compressed transaction ending at
  mysql-bin.000015:5501 yielded 0 of its 622 bytes of events")`. `mysql_common` 0.37.3 checks no
  dump event's checksum. MySQL 8.4.
- A file/position dump opened with a checkpoint from another server streams that server's binlog
  unless the open checks it. With the check disabled, Task 4's IT failed: the open succeeded
  (MySQL 8.4).
- A MySQL dump thread stays in `PROCESSLIST` after the client's `COM_QUIT` until it next writes;
  at the default 60 s heartbeat the stop scenario's 30 s wait timed out (MySQL 8.4).
- MariaDB 11.8 refuses a recursive CTE past `max_recursive_iterations = 1000` (error 1931), as
  MySQL refuses past `cte_max_recursion_depth = 1000`.
- MariaDB's `JSON` keeps the text as written (`{"a": 1}`, `LONGTEXT`); MySQL's prints compact
  (`{"a":1}`), through the snapshot and the stream alike (DuckDB).
- The binary's log carries ANSI colour sequences into a file (`tracing-subscriber`'s default when
  stdout isn't a terminal is still colour): the suite strips them to find the `code` field. Not
  changed here; recorded in the gap tracker.
- The E2E suite, Tasks 6 and 7 applied: PostgreSQL 18, 33 of 33; `mysql-84`, 33 of 33;
  `mysql-84-nogtid`, 32 of 32; `mariadb-114` and `mariadb-1011`, 33 of 33 each; `mariadb-118`
  and `mariadb-118-binlog`, 28 of 33 at first, then the 5 that failed (the CTE ceiling and the
  MariaDB JSON text, both fixed in the blocks below) green on rerun.

**Documents this plan changes** (applied 2026-10-17 at plan approval, in an `infra:` and a `gap:` commit;
`test-infra/README.md` changes in Task 8):

- Roadmap:
  - §3.6: `reset_position() -> Checkpoint`, `open_stream(from: &Checkpoint, …)`,
    `SourceError::halt_code` (D-M7b.1, D-M7b.2); the MySQL dump's server check (D-M7b.7).
  - §3.7–§3.11: the worker's checkpoint store and `Core.server` (D-M7b.3, D-M7b.4);
    `Actual::Error { error, code }` (Q-M7b-4).
  - §4 M7: M7b's scope as built.
  - §5.3: the suite's two sources and the MySQL scenarios.
  - §5.4: the `e2e-mysql` jobs.
  - §9: D-IMPL-16 for D-M7b.1…16. §9.1: Q-M7b-1…4.
- `doc/v1.0/README.md`: the M7b row; `DATA_AVALANCHE_E2E_SOURCE` in the test gates.
- `CDC_Design_Gap_Analysis_v1.md`: a dated section for M7b planning; the ANSI log finding.

No spec changes: Data-Flow Phase 5 and §State Layout already describe `position.json` as the
tagged position with the server identity.

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-core/src/source.rs` | `open_stream(&Checkpoint, …)`, `reset_position() -> Checkpoint`, `SourceError::halt_code` |
| `crates/data-avalanche-source-pg/src/connector.rs`, `…/tests/pg/schema.rs` | The PostgreSQL impl of the new signatures |
| `crates/data-avalanche-testkit/src/fake_source.rs` | The fake's server (`set_server`), the checkpoints it was handed (`opened_at`, `prepared_at`) |
| `crates/data-avalanche-pipeline/src/checkpoint.rs` (new) | `Store`, `Stored`, `Doc`: the slot or position document |
| `crates/data-avalanche-pipeline/src/{settings,worker,session,writer}.rs` | The store in the settings; Phase 0, the session and the writer through it; `Core.server` |
| `crates/data-avalanche-pipeline/src/test_support.rs` | A MySQL pipeline (`Yaml::mysql`) |
| `crates/data-avalanche-pipeline/src/error.rs` | `WorkerError::code` |
| `crates/data-avalanche-cluster/src/controller.rs` | `Actual::Error { error, code }`, the log field |
| `crates/data-avalanche-source-mysql/src/{position,stream,connector}.rs` | `identity`, `checkpoint_now`, the dump's server check |
| `crates/data-avalanche-source-mysql/tests/…` | Checkpoints with their server; the file/position ITs |
| `crates/data-avalanche/{Cargo.toml,src/wiring.rs}` | The source by `source.type` |
| `crates/data-avalanche/tests/e2e/source.rs` (new) | `Source`, `Target`, `Db`: the source under test and its admin connection |
| `crates/data-avalanche/tests/e2e/harness.rs` | The world on either source; dialect helpers |
| `crates/data-avalanche/tests/e2e/{smoke,snapshots,streaming,crashes,schema,toast,types,guards,lifecycle}.rs` | The shared scenarios on the helpers; the PostgreSQL-only ones gated |
| `crates/data-avalanche/tests/e2e/proxy.rs` (new) | The wire proxy |
| `crates/data-avalanche/tests/e2e/mysql.rs` (new) | The MySQL scenarios |
| `.github/workflows/{ci,weekly}.yml`, `test-infra/README.md` | The `e2e-mysql` jobs; how to run the suite on either source |

---

### Task 1: The source contract passes the server with the position (`data-avalanche-core`)

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs`
- Modify: `crates/data-avalanche-pipeline/src/session.rs`
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`
- Modify: `crates/data-avalanche-source-mysql/src/connector.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/binlog_purge.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/mysql/start.rs`
- Modify: `crates/data-avalanche-source-pg/src/connector.rs`
- Modify (test): `crates/data-avalanche-source-pg/tests/pg/schema.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`

**Interfaces:**
- Consumes:
  - `Checkpoint { position, server: Option<ServerIdentity> }`, `From<SourcePosition> for Checkpoint` (core, M7a Task 1).
  - `MySqlConnector::{position_now, server_identity}` (M7a).
- Produces:
  - `data-avalanche-core/src/source.rs`:
    - `SourceConnector::reset_position(&self) -> Result<Checkpoint, SourceError>`
    - `SourceConnector::open_stream(&self, from: &Checkpoint, tables: &[TableSchema]) -> Result<Box<dyn ChangeStream>, SourceError>`
    - `SourceError::halt_code(&self) -> Option<&str>`
  - `data-avalanche-testkit`: `FakeSource::open_stream` takes `&Checkpoint` (the fake's server comes in Task 2).

D-M7b.1, D-M7b.2. A reconnect opens the stream without `prepare` (`Session::open`), and a position lost mid-run is replaced without `prepare` too (`reset_position`, then Phase 0 again). In file/position mode both must know the server: the reconnect to check it (Task 4), the replacement to record it (Task 2). So the stream's open takes the whole checkpoint, and the reset returns one.

The PostgreSQL connector passes `from.position` to its inherent `open_stream`, and its reset has no server. The MySQL connector's reset reads the write position and the server; its open passes the position on for now (the check is Task 4's). `halt_code` reads the code D-M7a.8 put at the front of a pipeline-scope halt's message, for the status (Task 3). The worker and the session change mechanically: they hand the stream a checkpoint, and the lost path hands Phase 0 the reset's position (Task 2 keeps its server).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index bec1f9c..6d52f00 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -448,6 +465,37 @@ mod tests {
         assert_eq!(identifier_values(&schema(), &null), None);
     }
 
+    #[test]
+    fn a_pipeline_halt_names_its_code() {
+        let code = |m: &str| {
+            SourceError::Permanent(m.into())
+                .halt_code()
+                .map(str::to_owned)
+        };
+        assert_eq!(
+            code("BINLOG_PAYLOAD_INVALID: the compressed transaction at f:4 yielded 0 bytes"),
+            Some("BINLOG_PAYLOAD_INVALID".into())
+        );
+        assert_eq!(
+            code("BINLOG_SOURCE_CHANGED: binlog position f:4 was recorded on …"),
+            Some("BINLOG_SOURCE_CHANGED".into())
+        );
+        for plain in [
+            "MySQL 8.0.36: the source needs MySQL 8.4 or newer",
+            "the binary log is off (log_bin); the source reads the binlog",
+            "ERROR: no colon space",
+            "binlog_format is MIXED: lower case",
+            "",
+        ] {
+            assert_eq!(code(plain), None, "{plain}");
+        }
+        assert_eq!(
+            SourceError::Transient("BINLOG_PAYLOAD_INVALID: x".into()).halt_code(),
+            None,
+            "only a permanent error halts the pipeline"
+        );
+    }
+
     #[test]
     fn only_transient_errors_are_retried() {
         assert!(SourceError::Transient("reset".into()).is_transient());
@@ -527,8 +575,8 @@ mod tests {
         async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
             Ok(None)
         }
-        async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
-            Ok(SourcePosition::Pg(Lsn(0x20)))
+        async fn reset_position(&self) -> Result<Checkpoint, SourceError> {
+            Ok(SourcePosition::Pg(Lsn(0x20)).into())
         }
         async fn current_position(&self) -> Result<SourcePosition, SourceError> {
             Ok(SourcePosition::Pg(Lsn(0x30)))
@@ -550,7 +598,7 @@ mod tests {
         }
         async fn open_stream(
             &self,
-            _: &SourcePosition,
+            _: &Checkpoint,
             _: &[TableSchema],
         ) -> Result<Box<dyn ChangeStream>, SourceError> {
             let table = TableRef::new("public", "t");
@@ -614,8 +662,8 @@ mod tests {
                 ..
             }
         ));
-        let mut stream =
-            block_on(source.open_stream(&SourcePosition::Pg(Lsn(0x10)), &[schema()])).unwrap();
+        let from = Checkpoint::from(SourcePosition::Pg(Lsn(0x10)));
+        let mut stream = block_on(source.open_stream(&from, &[schema()])).unwrap();
         assert!(matches!(
             block_on(stream.next()).unwrap(),
             StreamItem::Begin { xid: 7, .. }
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 72b23e9..1eaa335 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -1668,7 +1668,7 @@ mod tests {
         core.snapshot_started.insert(t("a"), long_ago);
         w.source.lose_slot();
         let from = core.source.reset_position().await.unwrap();
-        start(&mut core, Some(from)).await.unwrap();
+        start(&mut core, Some(from.position)).await.unwrap();
         let xid = w.source.open_writer(&[t("b")]);
         w.source.hold_reads();
         let before = w.source.in_flights();
```

Change `crates/data-avalanche-source-mysql/tests/binlog_purge.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/binlog_purge.rs b/crates/data-avalanche-source-mysql/tests/binlog_purge.rs
index 096b644..fbedc34 100644
--- a/crates/data-avalanche-source-mysql/tests/binlog_purge.rs
+++ b/crates/data-avalanche-source-mysql/tests/binlog_purge.rs
@@ -102,7 +102,7 @@ async fn a_purged_checkpoint_follows_the_purge_action() {
     );
     for (c, re_snapshot) in [(&halts, false), (&recovers, true)] {
         // The server's refusal, for a stream opened without the check.
-        let e = ended(SourceConnector::open_stream(c, &cp.position, &tables).await).await;
+        let e = ended(SourceConnector::open_stream(c, &cp, &tables).await).await;
         check(re_snapshot, "the dump", e);
     }
     cleanup(&n).await;
```

Change `crates/data-avalanche-source-mysql/tests/mysql/start.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/start.rs b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
index 94a00fd..8cb0886 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/start.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
@@ -70,10 +70,15 @@ async fn a_first_start_snapshots_and_records_the_server_then_resumes() {
     .expect("resume");
     assert_eq!(again.plan, StartPlan::Resume);
     assert!(again.added.is_empty() && again.warnings.is_empty());
-    assert_eq!(again.server, Some(identity));
+    assert_eq!(again.server, Some(identity.clone()));
     // A lost position is replaced by the write position now.
     let reset = c.reset_position().await.unwrap();
-    assert!(reset.covers(&from).unwrap(), "{reset} covers {from}");
+    assert!(
+        reset.position.covers(&from).unwrap(),
+        "{} covers {from}",
+        reset.position
+    );
+    assert_eq!(reset.server, Some(identity));
     cleanup(&n).await;
 }
 
```

Change `crates/data-avalanche-source-pg/tests/pg/schema.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/schema.rs b/crates/data-avalanche-source-pg/tests/pg/schema.rs
index f8bf9de..42382a7 100644
--- a/crates/data-avalanche-source-pg/tests/pg/schema.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/schema.rs
@@ -28,7 +28,10 @@ async fn channel_c_proves_a_rename_scoped_to_the_signal() {
     let StartPlan::Snapshot { from, .. } = p.plan else {
         panic!()
     };
-    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
+    let mut s = c
+        .open_stream(&from.clone().into(), &r.tables)
+        .await
+        .unwrap();
     let sc = &n.schema;
     each(
         &a,
@@ -181,7 +184,10 @@ async fn channel_c_with_the_catalog_ahead() {
     let StartPlan::Snapshot { from, .. } = p.plan else {
         panic!()
     };
-    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
+    let mut s = c
+        .open_stream(&from.clone().into(), &r.tables)
+        .await
+        .unwrap();
     let sc = &n.schema;
     each(
         &a,
@@ -255,7 +261,10 @@ async fn channel_c_classifies_identity_changes() {
     let StartPlan::Snapshot { from, .. } = p.plan else {
         panic!()
     };
-    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
+    let mut s = c
+        .open_stream(&from.clone().into(), &r.tables)
+        .await
+        .unwrap();
     let sc = &n.schema;
     each(
         &a,
```

Change `crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index de61d7e..d81579d 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -1350,7 +1350,7 @@ mod tests {
         assert_eq!(src.slot(), Some(from.clone()));
         let tables = src.resolve_tables().await.unwrap().tables;
         assert_eq!(tables[0].source_id(), Some(1000));
-        let mut s = src.open_stream(&from, &tables).await.unwrap();
+        let mut s = src.open_stream(&from.into(), &tables).await.unwrap();
         src.commit(FakeTxn::new().insert(&t(), row(2, "b")));
         let got = items(&mut s, 3).await;
         assert!(matches!(got[0], StreamItem::Begin { xid: 700, .. }));
@@ -1386,7 +1386,10 @@ mod tests {
         let src = FakeSource::new(vec![schema()]);
         src.load(&t(), vec![row(1, "a")]);
         let tables = src.resolve_tables().await.unwrap().tables;
-        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
+        let mut s = src
+            .open_stream(&src.wal_end().into(), &tables)
+            .await
+            .unwrap();
         let xid = src.commit_stalled(FakeTxn::new().update(&t(), row(1, "b")));
         let got = items(&mut s, 3).await;
         assert!(matches!(&got[2], StreamItem::Commit(b) if b.xid == xid));
@@ -1417,7 +1420,10 @@ mod tests {
     async fn changes_keys_truncates_and_unchanged_values_stream_as_sent() {
         let src = FakeSource::new(vec![schema()]);
         let tables = src.resolve_tables().await.unwrap().tables;
-        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
+        let mut s = src
+            .open_stream(&src.wal_end().into(), &tables)
+            .await
+            .unwrap();
         src.commit(
             FakeTxn::new()
                 .insert(&t(), row(1, "a"))
@@ -1459,7 +1465,10 @@ mod tests {
     async fn an_alter_signals_before_the_next_change() {
         let src = FakeSource::new(vec![schema()]);
         let tables = src.resolve_tables().await.unwrap().tables;
-        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
+        let mut s = src
+            .open_stream(&src.wal_end().into(), &tables)
+            .await
+            .unwrap();
         let wider = TableSchema::new(
             t(),
             vec![
@@ -1506,7 +1515,10 @@ mod tests {
         let StartPlan::Snapshot { from, .. } = p.plan else {
             panic!()
         };
-        let mut s = src.open_stream(&from, &tables).await.unwrap();
+        let mut s = src
+            .open_stream(&from.clone().into(), &tables)
+            .await
+            .unwrap();
         src.lose_slot();
         assert!(matches!(
             s.next().await,
@@ -1554,7 +1566,10 @@ mod tests {
     async fn a_held_stream_falls_behind_the_position() {
         let src = FakeSource::new(vec![schema()]);
         let tables = src.resolve_tables().await.unwrap().tables;
-        let mut s = src.open_stream(&src.wal_end(), &tables).await.unwrap();
+        let mut s = src
+            .open_stream(&src.wal_end().into(), &tables)
+            .await
+            .unwrap();
         src.hold_stream();
         src.keepalive();
         let at = src.current_position().await.unwrap();
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core --lib source::`

Expected: FAIL. The core tests no longer compile: the test connector implements the new signatures, which the trait doesn't have yet, and `halt_code` doesn't exist. The first errors:

```text
error[E0053]: method `reset_position` has an incompatible type for trait  --> crates/data-avalanche-core/src/source.rs:528:5
error[E0053]: method `open_stream` has an incompatible type for trait  --> crates/data-avalanche-core/src/source.rs:584:16
error[E0599]: no method named `halt_code` found for enum `source::SourceError` in the current scope  --> crates/data-avalanche-core/src/source.rs:455:18
error[E0308]: mismatched types  --> crates/data-avalanche-core/src/source.rs:649:54
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index bec1f9c..6d52f00 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -43,6 +43,21 @@ impl SourceError {
     pub fn is_transient(&self) -> bool {
         matches!(self, Self::Transient(_))
     }
+
+    /// The code of a pipeline-scope halt: a [`Self::Permanent`] whose message starts with an
+    /// upper-case code and a colon (`BINLOG_PAYLOAD_INVALID: …`, `BINLOG_SOURCE_CHANGED: …`).
+    pub fn halt_code(&self) -> Option<&str> {
+        let Self::Permanent(m) = self else {
+            return None;
+        };
+        let (code, _) = m.split_once(": ")?;
+        let valid = code.contains('_')
+            && code.starts_with(|c: char| c.is_ascii_uppercase())
+            && code
+                .chars()
+                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
+        valid.then_some(code)
+    }
 }
 
 /// What a pipeline-level preflight found (config §11.2, the rows that halt a pipeline). A failed
@@ -318,8 +333,9 @@ pub trait SourceConnector: Send + Sync {
     ) -> Result<Prepared, SourceError>;
 
     /// Replaces a lost stream position after [`SourceError::PositionUnreachable`] (D-15.3): the
-    /// new consistent position to snapshot from and stream after.
-    async fn reset_position(&self) -> Result<SourcePosition, SourceError>;
+    /// new consistent position to snapshot from and stream after, with the server it was read
+    /// from when the source records one.
+    async fn reset_position(&self) -> Result<Checkpoint, SourceError>;
 
     /// The log the source keeps for the pipeline. `None` when the source keeps nothing for it.
     async fn retained(&self) -> Result<Option<RetainedLog>, SourceError>;
@@ -349,10 +365,11 @@ pub trait SourceConnector: Send + Sync {
     /// row is left out.
     async fn fetch_rows(&self, table: &TableSchema, keys: &[Row]) -> Result<Vec<Row>, SourceError>;
 
-    /// Opens the change stream after `from`, for `tables` as of that position.
+    /// Opens the change stream after `from`'s position, for `tables` as of that position. A
+    /// source that records the server (MySQL file/position mode) refuses another one.
     async fn open_stream(
         &self,
-        from: &SourcePosition,
+        from: &Checkpoint,
         tables: &[TableSchema],
     ) -> Result<Box<dyn ChangeStream>, SourceError>;
 
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 462fbda..44da560 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -243,7 +243,11 @@ impl<'c> Session<'c> {
             core.snapshot_started.entry(table.clone()).or_insert(now);
         }
         let live: Vec<TableSchema> = schemas.values().cloned().collect();
-        let stream = match core.source.open_stream(&checkpoint, &live).await {
+        let stream = match core
+            .source
+            .open_stream(&checkpoint.clone().into(), &live)
+            .await
+        {
             Ok(stream) => stream,
             Err(e) => return Ok(Err(e)),
         };
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 72b23e9..1eaa335 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -178,7 +178,7 @@ impl PipelineWorker {
                         || core.source.reset_position(),
                     )
                     .await?;
-                    start(core, Some(from)).await?;
+                    start(core, Some(from.position)).await?;
                     attempt = 0;
                     continue;
                 }
```

Change `crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
index cfd222e..f4d1f2c 100644
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -328,8 +328,11 @@ impl SourceConnector for MySqlConnector {
     }
 
     /// The server's write position now: the binlog keeps no position for the pipeline to lose.
-    async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
-        self.position_now().await
+    async fn reset_position(&self) -> Result<Checkpoint, SourceError> {
+        Ok(Checkpoint {
+            position: self.position_now().await?,
+            server: Some(self.server_identity().await?),
+        })
     }
 
     async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
@@ -363,11 +366,11 @@ impl SourceConnector for MySqlConnector {
 
     async fn open_stream(
         &self,
-        from: &SourcePosition,
+        from: &Checkpoint,
         tables: &[TableSchema],
     ) -> Result<Box<dyn ChangeStream>, SourceError> {
         Ok(Box::new(
-            MySqlConnector::open_stream(self, from, tables).await?,
+            MySqlConnector::open_stream(self, &from.position, tables).await?,
         ))
     }
 
```

Change `crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index 34580b8..e66643a 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -270,8 +270,10 @@ impl SourceConnector for PgConnector {
         PgConnector::prepare(self, checkpoint, tables).await
     }
 
-    async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
-        PgConnector::reset_position(self).await
+    async fn reset_position(&self) -> Result<Checkpoint, SourceError> {
+        PgConnector::reset_position(self)
+            .await
+            .map(Checkpoint::from)
     }
 
     async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
@@ -301,10 +303,10 @@ impl SourceConnector for PgConnector {
 
     async fn open_stream(
         &self,
-        from: &SourcePosition,
+        from: &Checkpoint,
         tables: &[TableSchema],
     ) -> Result<Box<dyn ChangeStream>, SourceError> {
-        Ok(PgConnector::open_stream(self, from, tables).await?)
+        Ok(PgConnector::open_stream(self, &from.position, tables).await?)
     }
 
     async fn augment(
```

Change `crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index de61d7e..d81579d 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -903,12 +903,12 @@ impl SourceConnector for FakeSource {
         })
     }
 
-    async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
+    async fn reset_position(&self) -> Result<Checkpoint, SourceError> {
         self.take_fault(Method::ResetPosition)?;
         let mut db = self.db();
         db.slot = Some(db.wal_end);
         db.slot_lost = false;
-        Ok(SourcePosition::Pg(db.wal_end))
+        Ok(SourcePosition::Pg(db.wal_end).into())
     }
 
     async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
@@ -1033,14 +1033,14 @@ impl SourceConnector for FakeSource {
 
     async fn open_stream(
         &self,
-        from: &SourcePosition,
+        from: &Checkpoint,
         tables: &[TableSchema],
     ) -> Result<Box<dyn ChangeStream>, SourceError> {
         self.take_fault(Method::OpenStream)?;
-        self.db().opened.push(from.clone());
+        self.db().opened.push(from.position.clone());
         Ok(Box::new(FakeStream {
             source: self.clone(),
-            after: lsn(from),
+            after: lsn(&from.position),
             next_entry: 0,
             pending: VecDeque::new(),
             known: tables
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core --lib source::`

Expected: PASS, 10 tests.

Run: `cargo build -q --workspace --all-targets`

Expected: it builds, with no errors.

Run: `cargo test -p data-avalanche-testkit --lib fake_source::`

Expected: PASS, 8 tests.

Run: `cargo test --workspace`

Expected: PASS, every test binary.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core/src/source.rs \
  crates/data-avalanche-pipeline/src/session.rs \
  crates/data-avalanche-pipeline/src/worker.rs \
  crates/data-avalanche-source-mysql/src/connector.rs \
  crates/data-avalanche-source-mysql/tests/binlog_purge.rs \
  crates/data-avalanche-source-mysql/tests/mysql/start.rs \
  crates/data-avalanche-source-pg/src/connector.rs \
  crates/data-avalanche-source-pg/tests/pg/schema.rs \
  crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "feat(core): the source contract — open_stream takes a Checkpoint, reset_position returns one, SourceError::halt_code"
```

---

### Task 2: The worker's MySQL checkpoint (`data-avalanche-pipeline`)

**Files:**
- Create: `crates/data-avalanche-pipeline/src/checkpoint.rs`
- Modify: `crates/data-avalanche-pipeline/src/lib.rs`
- Modify: `crates/data-avalanche-pipeline/src/session.rs`
- Modify: `crates/data-avalanche-pipeline/src/settings.rs`
- Modify (test): `crates/data-avalanche-pipeline/src/test_support.rs`
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`
- Modify: `crates/data-avalanche-pipeline/src/writer.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`

**Interfaces:**
- Consumes:
  - Task 1's `open_stream(&Checkpoint)`, `reset_position() -> Checkpoint`.
  - `PipelineDocs::{slot, save_slot, position, save_position}`, `PositionDoc { position, server }`, `SlotDoc::{intent, is_intent}` (state, M2).
  - `Prepared.server` (core, M7a).
- Produces:
  - `crates/data-avalanche-pipeline/src/checkpoint.rs` (crate-private):
    - `enum Store { Slot { name: String, intent: Option<String> }, Position }`
    - `struct Stored { checkpoint: Checkpoint, intent: bool }`, `enum Doc { Slot(SlotDoc), Position(PositionDoc) }`
    - `Store::{load, intent, unread, doc}`, `Doc::save`
  - `Settings.checkpoint: Store` (replaces `slot`, `slot_intent`); `Core.server: Option<ServerIdentity>`; `WriterCtx { store, server, … }`.
  - `start(core, lost: Option<Checkpoint>)`.
  - `data-avalanche-testkit`: `FakeSource::{set_server, opened_at, prepared_at}`.
  - `test_support::Yaml.mysql: Option<&str>` (a MySQL pipeline).

D-M7b.3…6, D-M7b.9. Phase 0, the session and the writer each knew only the slot document. They now load and save through one `checkpoint::Store`, read from the source's config block: PostgreSQL's slot (its name, and the slot a managed first start records its intent for), or MySQL's position document. `Store::doc` builds the document a checkpoint becomes and refuses one it can't hold.

The server a MySQL checkpoint records is the one the last start read. That is `Prepared.server` after `prepare`, or the reset checkpoint's server after a lost position. The core keeps it, and the writer writes it with every checkpoint. A reconnect keeps it. The PostgreSQL-only refusal in `start` goes. `Settings.heartbeat` takes `source.mysql.heartbeat` for a MySQL pipeline, and the watchdog names a stream the source doesn't run in the source's own terms.

The fake source learns to answer like a MySQL source: with a server set, `prepare` and `reset_position` name it. It also keeps every checkpoint it was handed, with its server. The fake still speaks LSNs; the worker is position-blind, and a MySQL pipeline over it keeps them in the position document.

M7a's carry item "the pipeline drops a halted table's buffered rows" needs no code (D-M7b.9). `Session::halt` flushes the table's committed changes, then `Accumulator::halt_table` removes its buffer, open transaction included. The session test `a_halt_mid_transaction_drops_the_tables_rows_of_it` pins it. It passes on the code as it stands, and it is the only test of this task that does.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 44da560..198db39 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -1555,6 +1555,36 @@ mod tests {
         assert!(st.halted.contains_key(&t("a")));
     }
 
+    /// A halt mid-transaction (MySQL: a rows event whose later row can't be decoded) drops the
+    /// table's rows of that transaction, before the halt and after it; its committed rows stay
+    /// and the transaction's other tables land.
+    #[tokio::test]
+    async fn a_halt_mid_transaction_drops_the_tables_rows_of_it() {
+        let w = World::new(&["a", "b"]);
+        w.started(&["a", "b"]).await;
+        let (end, st) = session(&w, &flush_each(), &["a", "b"], async |status| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            let mut bad = row(3, "x");
+            bad.0.push(("zzz".into(), ColumnValue::Int64(1)));
+            w.source.commit(
+                FakeTxn::new()
+                    .insert(&t("a"), row(2, "before"))
+                    .insert(&t("b"), row(1, "y"))
+                    .insert(&t("a"), bad)
+                    .insert(&t("a"), row(4, "after"))
+                    .insert(&t("b"), row(2, "y")),
+            );
+            durable(&status, w.source.wal_end()).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!((w.ids("a"), w.ids("b")), (vec![1], vec![1, 2]));
+        let halts = w.docs.halts().await.unwrap().tables;
+        assert_eq!(halts.len(), 1);
+        assert_eq!(halts[0].table, t("a"));
+        assert!(st.halted.contains_key(&t("a")));
+    }
+
     #[tokio::test]
     async fn a_sink_refusal_halts_the_table() {
         let w = World::new(&["a", "b"]);
```

Change `crates/data-avalanche-pipeline/src/settings.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/settings.rs b/crates/data-avalanche-pipeline/src/settings.rs
index 229e713..1960ce0 100644
--- a/crates/data-avalanche-pipeline/src/settings.rs
+++ b/crates/data-avalanche-pipeline/src/settings.rs
@@ -173,7 +179,7 @@ mod tests {
     use data_avalanche_core::{ColumnSchema, ColumnValueType as V};
 
     use super::*;
-    use crate::test_support::pipeline;
+    use crate::test_support::{Yaml, pipeline};
 
     #[test]
     fn settings_follow_the_pipeline() {
@@ -194,7 +200,13 @@ mod tests {
         assert_eq!(s.heartbeat, Duration::from_secs(5));
         assert_eq!(s.max_wal_lag_bytes, Some(1 << 30));
         assert_eq!(s.rescan_interval, Some(Duration::from_secs(3600)));
-        assert_eq!(s.slot_intent.as_deref(), Some("p_slot"));
+        assert_eq!(
+            s.checkpoint,
+            Store::Slot {
+                name: "p_slot".into(),
+                intent: Some("p_slot".into())
+            }
+        );
         let orders = TableRef::new("public", "orders");
         assert_eq!(s.mode(&orders), SnapshotMode::Initial);
         assert_eq!(s.mode(&TableRef::new("public", "big")), SnapshotMode::Never);
@@ -222,6 +234,32 @@ mod tests {
             "",
             "slot_management: require_existing, auto_cleanup: false",
         );
-        assert_eq!(Settings::new(&p).slot_intent, None);
+        assert_eq!(
+            Settings::new(&p).checkpoint,
+            Store::Slot {
+                name: "p_slot".into(),
+                intent: None
+            }
+        );
+    }
+
+    /// A MySQL pipeline keeps its checkpoint in the position document, reads the dump at
+    /// `mysql.heartbeat`, has no WAL to guard, and keys its tables by bare name (V-064).
+    #[test]
+    fn a_mysql_pipeline_keeps_a_position_document() {
+        let p = Yaml {
+            mysql: Some("heartbeat: 7s"),
+            tables: "\n      snapshots: { overrides: { orders: initial } }",
+            ..Yaml::default()
+        }
+        .build();
+        let s = Settings::new(&p);
+        assert_eq!(s.checkpoint, Store::Position);
+        assert_eq!(s.heartbeat, Duration::from_secs(7));
+        assert_eq!(s.max_wal_lag_bytes, None);
+        assert_eq!(
+            s.mode(&TableRef::new("public", "orders")),
+            SnapshotMode::Initial
+        );
     }
 }
```

Change `crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
index a740977..dd5355a 100644
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -41,12 +41,15 @@ pub(crate) fn pipeline(tables: &str, sink: &str, postgres: &str) -> Pipeline {
 }
 
 /// The test pipeline's extra YAML. `pipeline`, `source` and `tables` are lines (each starting
-/// with a newline, indented four, six and six spaces); `sink` and `postgres` are flow entries.
+/// with a newline, indented four, six and six spaces); `sink`, `postgres` and `mysql` are flow
+/// entries. With `mysql` set the source is MySQL (`db: public`, so its tables are the fake's
+/// `public.*`), and `postgres` is ignored.
 #[derive(Debug, Clone, Copy, Default)]
 pub(crate) struct Yaml<'a> {
     pub pipeline: &'a str,
     pub source: &'a str,
     pub postgres: &'a str,
+    pub mysql: Option<&'a str>,
     pub tables: &'a str,
     pub sink: &'a str,
 }
@@ -60,6 +63,24 @@ impl Yaml<'_> {
                 format!(", {s}")
             }
         };
+        let (db, origin, include) = match self.mysql {
+            None => (
+                "avalanche",
+                format!(
+                    "ssl_mode: disable\n      postgres: {{ publication: p_pub, slot: p_slot{} }}",
+                    sep(self.postgres)
+                ),
+                "public.*",
+            ),
+            Some(mysql) => (
+                "public",
+                format!(
+                    "type: mysql\n      mysql: {{ replication_type: gtid{} }}",
+                    sep(mysql)
+                ),
+                "'*'",
+            ),
+        };
         let yaml = format!(
             r#"
 config_version: 1
@@ -71,16 +92,14 @@ pipelines:
     source:
       host: 127.0.0.1
       port: 55418
-      db: avalanche
+      db: {db}
       user: avalanche
       password: avalanche
-      ssl_mode: disable
-      postgres: {{ publication: p_pub, slot: p_slot{postgres} }}{source}
+      {origin}{source}
     tables:
-      include: [public.*]{tables}
+      include: [{include}]{tables}
     sink: {{ type: delta, bucket: lake, delta: {{}}{sink} }}{top}
 "#,
-            postgres = sep(self.postgres),
             source = self.source,
             tables = self.tables,
             sink = sep(self.sink),
@@ -423,6 +442,7 @@ impl World {
             wal_lag_failing: Streak::default(),
             writers_waited: BTreeSet::new(),
             read_failing: BTreeMap::new(),
+            server: None,
             pause: false,
         };
         (core, control, status_rx)
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 1eaa335..19f5f9f 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -663,7 +650,10 @@ pub(crate) async fn start(
 #[cfg(test)]
 mod tests {
     use data_avalanche_core::position::Lsn;
-    use data_avalanche_core::{ColumnSchema, ColumnValue, ColumnValueType as V, Row};
+    use data_avalanche_core::{
+        ColumnSchema, ColumnValue, ColumnValueType as V, Row, ServerIdentity, SourcePosition,
+    };
+    use data_avalanche_state::{PositionDoc, SlotDoc};
     use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
 
     use super::*;
@@ -1066,6 +1056,121 @@ mod tests {
         );
     }
 
+    /// A MySQL → Delta pipeline `p` over the fake's `public.*`, its `snapshots` set to `tables`.
+    fn mysql(tables: &str) -> Pipeline {
+        let tables = format!("\n      snapshots: {{ {tables} }}");
+        Yaml {
+            mysql: Some(""),
+            tables: &tables,
+            sink: "flush: { seconds: 0 }",
+            ..Yaml::default()
+        }
+        .build()
+    }
+
+    fn server(id: u32) -> ServerIdentity {
+        ServerIdentity::Mariadb { server_id: id }
+    }
+
+    async fn position(w: &World) -> Option<PositionDoc> {
+        w.docs.position().await.unwrap().map(|v| v.doc)
+    }
+
+    /// A MySQL pipeline's first start records its position in the position document, with the
+    /// server `prepare` named, and every stream opens from it.
+    #[tokio::test]
+    async fn a_mysql_first_start_records_the_server_with_the_position() {
+        let w = World::new(&["a"]);
+        w.source.set_server(Some(server(1)));
+        w.source.load(&t("a"), rows(1..=2));
+        let (end, st) = run(&w, &mysql("mode: initial"), async |status| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(9, "s")));
+            streamed(&status, &w).await;
+        })
+        .await;
+        assert_eq!(end.unwrap(), Exit::Drained);
+        assert_eq!(w.ids("a"), [1, 2, 9]);
+        assert_eq!(
+            position(&w).await,
+            Some(PositionDoc {
+                position: w.source.wal_end(),
+                server: server(1)
+            })
+        );
+        assert!(w.docs.slot().await.unwrap().is_none(), "no slot document");
+        let opened = w.source.opened_at();
+        assert!(
+            !opened.is_empty() && opened.iter().all(|c| c.server == Some(server(1))),
+            "{opened:?}"
+        );
+        assert!(st.warnings.is_empty(), "{:?}", st.warnings);
+    }
+
+    /// A restart hands `prepare` the recorded server; the checkpoints after it record the server
+    /// the start reads now (a GTID failover between two runs).
+    #[tokio::test]
+    async fn a_mysql_restart_hands_prepare_the_recorded_server() {
+        let w = World::new(&["a"]);
+        w.source.set_server(Some(server(1)));
+        let p = mysql("mode: initial");
+        run(&w, &p, async |status| streamed(&status, &w).await)
+            .await
+            .0
+            .unwrap();
+        let recorded = position(&w).await.unwrap();
+        w.source.set_server(Some(server(2)));
+        w.source
+            .commit(FakeTxn::new().insert(&t("a"), row(5, "while down")));
+        run(&w, &p, async |status| streamed(&status, &w).await)
+            .await
+            .0
+            .unwrap();
+        assert_eq!(
+            w.source.prepared_at().last(),
+            Some(&Some(Checkpoint {
+                position: recorded.position,
+                server: Some(server(1))
+            }))
+        );
+        assert_eq!(w.ids("a"), [5]);
+        assert_eq!(position(&w).await.unwrap().server, server(2));
+    }
+
+    /// A position lost mid-run restarts at the reset position, recorded with the server the reset
+    /// read, and the stream reopens from it.
+    #[tokio::test]
+    async fn a_lost_mysql_position_records_the_resets_server() {
+        let w = World::new(&["a"]);
+        w.source.set_server(Some(server(1)));
+        w.source.load(&t("a"), rows(1..=2));
+        let (end, st) = run(&w, &mysql("mode: initial"), async |status| {
+            streamed(&status, &w).await;
+            let reads = w.source.reads(&t("a"));
+            w.source.set_server(Some(server(2)));
+            w.source.lose_slot();
+            w.source
+                .commit(FakeTxn::new().insert(&t("a"), row(7, "lost")));
+            wait_until("the table is snapshotted again", || {
+                w.source.reads(&t("a")) > reads
+            })
+            .await;
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(w.ids("a"), [1, 2, 7]);
+        assert_eq!(position(&w).await.unwrap().server, server(2));
+        assert_eq!(
+            w.source.opened_at().last().and_then(|c| c.server.clone()),
+            Some(server(2))
+        );
+        assert!(
+            st.warnings
+                .iter()
+                .any(|w| w.contains("lost the stream position"))
+        );
+    }
+
     #[tokio::test]
     async fn a_lost_position_baselines_a_drifted_table() {
         let w = World::new(&["a"]);
@@ -1668,7 +1773,7 @@ mod tests {
         core.snapshot_started.insert(t("a"), long_ago);
         w.source.lose_slot();
         let from = core.source.reset_position().await.unwrap();
-        start(&mut core, Some(from.position)).await.unwrap();
+        start(&mut core, Some(from)).await.unwrap();
         let xid = w.source.open_writer(&[t("b")]);
         w.source.hold_reads();
         let before = w.source.in_flights();
```

Change `crates/data-avalanche-pipeline/src/writer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index 160ac86..c5408cc 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -286,7 +284,7 @@ mod tests {
 
     use data_avalanche_core::position::Lsn;
     use data_avalanche_core::{SinkTableState, TableSpec};
-    use data_avalanche_state::{SLOT_KEY, SnapshotCursor, SnapshotStatus};
+    use data_avalanche_state::{PositionDoc, SLOT_KEY, SnapshotCursor, SnapshotStatus};
     use data_avalanche_testkit::{MemorySink, SinkOp};
     use tokio::sync::mpsc;
 
@@ -304,6 +302,9 @@ mod tests {
         docs: PipelineDocs,
         store: Arc<FlakyStore>,
         guard: Arc<Switch>,
+        /// The checkpoint's document, and the server it is recorded with.
+        checkpoints: Store,
+        server: Option<ServerIdentity>,
     }
 
     async fn rig(tables: &[&str]) -> Rig {
@@ -319,6 +320,11 @@ mod tests {
             docs,
             store,
             guard: Arc::default(),
+            checkpoints: Store::Slot {
+                name: "p_slot".into(),
+                intent: None,
+            },
+            server: None,
         }
     }
 
@@ -349,7 +355,8 @@ mod tests {
             let ctx = WriterCtx {
                 sink: Arc::new(self.sink.clone()),
                 docs: self.docs.clone(),
-                slot: "p_slot".into(),
+                store: self.checkpoints.clone(),
+                server: self.server.clone(),
                 pipeline_id: "p".into(),
                 owner_epoch: 1,
                 guard,
@@ -453,6 +460,38 @@ mod tests {
         );
     }
 
+    /// A MySQL pipeline's checkpoint is the position document, with the server the stream reads.
+    #[tokio::test]
+    async fn a_mysql_checkpoint_is_recorded_with_its_server() {
+        let mut r = rig(&[]).await;
+        let server = ServerIdentity::Mariadb { server_id: 7 };
+        r.checkpoints = Store::Position;
+        r.server = Some(server.clone());
+        let at = SourcePosition::parse_tagged("mariadb-gtid:0-7-42").unwrap();
+        let j = Job {
+            change_sets: vec![],
+            checkpoint: at.clone(),
+            halts: vec![],
+            snapshots: vec![],
+            schemas: vec![],
+            permit: None,
+        };
+        let events = r.run(vec![j], Arc::new(NoHooks)).await;
+        assert!(
+            matches!(&events[..], [WriterEvent::Durable(p)] if *p == at),
+            "{events:?}"
+        );
+        let doc = r.docs.position().await.unwrap().unwrap().doc;
+        assert_eq!(
+            doc,
+            PositionDoc {
+                position: at,
+                server
+            }
+        );
+        assert!(r.docs.slot().await.unwrap().is_none(), "no slot document");
+    }
+
     #[tokio::test]
     async fn a_crash_after_the_commit_leaves_the_checkpoint_behind() {
         let r = rig(&["a"]).await;
```

Change `crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index d81579d..6857c9c 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -1338,6 +1370,30 @@ mod tests {
         }
     }
 
+    /// With a server set, the fake answers as a MySQL source: `prepare` and `reset_position` name
+    /// it, and the checkpoints it was handed are kept with theirs.
+    #[tokio::test]
+    async fn a_server_is_reported_and_the_checkpoints_kept() {
+        let src = FakeSource::new(vec![schema()]);
+        assert_eq!(src.prepare(None, &[t()]).await.unwrap().server, None);
+        let a = ServerIdentity::Mariadb { server_id: 1 };
+        src.set_server(Some(a.clone()));
+        let at = Checkpoint {
+            position: src.wal_end(),
+            server: Some(a.clone()),
+        };
+        let p = src.prepare(Some(&at), &[t()]).await.unwrap();
+        assert_eq!((p.plan, p.server), (StartPlan::Resume, Some(a.clone())));
+        assert_eq!(src.prepared_at().last(), Some(&Some(at.clone())));
+        assert_eq!(src.prepares().last(), Some(&Some(at.position.clone())));
+        src.open_stream(&at, &[]).await.unwrap();
+        assert_eq!(src.opened_at(), std::slice::from_ref(&at));
+        assert_eq!(src.opened(), [at.position]);
+        let b = ServerIdentity::Mariadb { server_id: 2 };
+        src.set_server(Some(b.clone()));
+        assert_eq!(src.reset_position().await.unwrap().server, Some(b));
+    }
+
     #[tokio::test]
     async fn a_first_start_snapshots_then_streams_from_the_slot() {
         let src = FakeSource::new(vec![schema()]);
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-pipeline --lib`

Expected: FAIL. The pipeline's tests don't compile: `checkpoint::Store`, `Settings.checkpoint`, `Yaml.mysql`, `FakeSource::set_server` and the writer's `store` don't exist yet. The first errors:

```text
error[E0425]: cannot find type `Store` in this scope  --> crates/data-avalanche-pipeline/src/writer.rs:308:22
error[E0425]: cannot find type `ServerIdentity` in this scope  --> crates/data-avalanche-pipeline/src/writer.rs:309:24
error[E0560]: struct `writer::WriterCtx` has no field named `store`  --> crates/data-avalanche-pipeline/src/writer.rs:360:17
error[E0560]: struct `writer::WriterCtx` has no field named `server`  --> crates/data-avalanche-pipeline/src/writer.rs:361:17
error[E0609]: no field `checkpoint` on type `settings::Settings`  --> crates/data-avalanche-pipeline/src/settings.rs:198:15
error[E0433]: cannot find type `Store` in this scope  --> crates/data-avalanche-pipeline/src/settings.rs:251:34
error[E0560]: struct `Core` has no field named `server`  --> crates/data-avalanche-pipeline/src/test_support.rs:445:13
error[E0599]: no method named `set_server` found for struct `FakeSource` in the current scope  --> crates/data-avalanche-pipeline/src/worker.rs:1097:18
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-pipeline/src/checkpoint.rs`:

```rust
//! Where the pipeline's checkpoint lives (Data-Flow Phase 5): PostgreSQL's slot document
//! (`replication/slot.json`), or MySQL's position document (`replication/position.json`), which
//! records the server the position was read from with it.

use data_avalanche_core::{Checkpoint, SourcePosition};
use data_avalanche_state::{PipelineDocs, PositionDoc, SlotDoc, StateError};

use crate::{AccumulatorError, WorkerError};

/// The document that holds the checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Store {
    /// PostgreSQL: the slot's document. `intent` is the slot a first start records its creation
    /// intent for (`slot_management: managed`), `None` when the pipeline never creates it.
    Slot {
        name: String,
        intent: Option<String>,
    },
    /// MySQL / MariaDB: the position document.
    Position,
}

/// A checkpoint as the state holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stored {
    pub checkpoint: Checkpoint,
    /// A slot intent: a first start recorded it and ended before the slot's position was
    /// recorded. Its checkpoint (`0/0`) is no stream position.
    pub intent: bool,
}

/// One checkpoint write, as the store's document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Doc {
    Slot(SlotDoc),
    Position(PositionDoc),
}

impl Store {
    /// The checkpoint the state holds; `None` before a first start records one.
    pub async fn load(&self, docs: &PipelineDocs) -> Result<Option<Stored>, StateError> {
        Ok(match self {
            Self::Slot { .. } => docs.slot().await?.map(|v| Stored {
                checkpoint: SourcePosition::Pg(v.doc.confirmed_flush_lsn).into(),
                intent: v.doc.is_intent(),
            }),
            Self::Position => docs.position().await?.map(|v| Stored {
                checkpoint: Checkpoint {
                    position: v.doc.position,
                    server: Some(v.doc.server),
                },
                intent: false,
            }),
        })
    }

    /// The slot intent a first start records before `prepare` creates the slot; `None` for a
    /// store that records none.
    pub fn intent(&self) -> Option<SlotDoc> {
        match self {
            Self::Slot {
                intent: Some(name), ..
            } => Some(SlotDoc::intent(name)),
            _ => None,
        }
    }

    /// What the heartbeat found when the source runs no stream for the pipeline: PostgreSQL's
    /// slot has no reader, MySQL's server runs no binlog dump for it.
    pub fn unread(&self) -> &'static str {
        match self {
            Self::Slot { .. } => "no stream reads the slot",
            Self::Position => "the server runs no binlog dump for the stream",
        }
    }

    /// `checkpoint` as this store's document.
    ///
    /// # Errors
    /// A protocol error: a PostgreSQL checkpoint that isn't an LSN, or a MySQL one with no server.
    pub fn doc(&self, checkpoint: &Checkpoint) -> Result<Doc, WorkerError> {
        let protocol = |detail: String| WorkerError::from(AccumulatorError::Protocol { detail });
        match (self, checkpoint) {
            (
                Self::Slot { name, .. },
                Checkpoint {
                    position: SourcePosition::Pg(lsn),
                    ..
                },
            ) => Ok(Doc::Slot(SlotDoc {
                slot_name: name.clone(),
                confirmed_flush_lsn: *lsn,
            })),
            (Self::Slot { .. }, c) => Err(protocol(format!(
                "checkpoint {} isn't a PostgreSQL position",
                c.position
            ))),
            (
                Self::Position,
                Checkpoint {
                    position,
                    server: Some(server),
                },
            ) => Ok(Doc::Position(PositionDoc {
                position: position.clone(),
                server: server.clone(),
            })),
            (Self::Position, c) => Err(protocol(format!(
                "checkpoint {} names no server to record with it",
                c.position
            ))),
        }
    }
}

impl Doc {
    pub async fn save(&self, docs: &PipelineDocs) -> Result<(), StateError> {
        match self {
            Self::Slot(d) => docs.save_slot(d).await.map(drop),
            Self::Position(d) => docs.save_position(d).await.map(drop),
        }
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::ServerIdentity;
    use data_avalanche_core::position::Lsn;

    use super::*;
    use crate::test_support::docs;

    fn mysql() -> ServerIdentity {
        ServerIdentity::Mysql {
            server_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562".into(),
        }
    }

    fn pg(n: u64) -> Checkpoint {
        SourcePosition::Pg(Lsn(n)).into()
    }

    #[tokio::test]
    async fn a_slot_holds_a_postgresql_checkpoint() {
        let (d, _) = docs();
        let store = Store::Slot {
            name: "p_slot".into(),
            intent: Some("p_slot".into()),
        };
        assert_eq!(store.load(&d).await.unwrap(), None);
        d.save_slot(&store.intent().unwrap()).await.unwrap();
        let intent = store.load(&d).await.unwrap().unwrap();
        assert!(intent.intent && intent.checkpoint == pg(0));
        store.doc(&pg(0x200)).unwrap().save(&d).await.unwrap();
        assert_eq!(
            store.load(&d).await.unwrap(),
            Some(Stored {
                checkpoint: pg(0x200),
                intent: false
            })
        );
        let unmanaged = Store::Slot {
            name: "p_slot".into(),
            intent: None,
        };
        assert_eq!(unmanaged.intent(), None);
    }

    #[tokio::test]
    async fn a_position_document_holds_the_server_with_the_position() {
        let (d, _) = docs();
        let store = Store::Position;
        assert_eq!(store.load(&d).await.unwrap(), None);
        assert_eq!(store.intent(), None);
        let at = Checkpoint {
            position: SourcePosition::parse_tagged("binlog:mysql-bin.000003:154").unwrap(),
            server: Some(mysql()),
        };
        store.doc(&at).unwrap().save(&d).await.unwrap();
        assert_eq!(
            store.load(&d).await.unwrap(),
            Some(Stored {
                checkpoint: at,
                intent: false
            })
        );
        assert!(d.slot().await.unwrap().is_none(), "no slot document");
    }

    #[test]
    fn a_stream_the_source_doesnt_run_is_named_for_the_source() {
        let slot = Store::Slot {
            name: "s".into(),
            intent: None,
        };
        assert_eq!(slot.unread(), "no stream reads the slot");
        assert!(Store::Position.unread().contains("binlog dump"));
    }

    #[test]
    fn a_checkpoint_the_store_cant_hold_is_a_protocol_error() {
        let slot = Store::Slot {
            name: "s".into(),
            intent: None,
        };
        let binlog = SourcePosition::parse_tagged("binlog:mysql-bin.000003:154").unwrap();
        let e = slot.doc(&binlog.clone().into()).unwrap_err();
        assert!(e.to_string().contains("isn't a PostgreSQL position"), "{e}");
        let e = Store::Position.doc(&binlog.into()).unwrap_err();
        assert!(e.to_string().contains("names no server"), "{e}");
    }
}
```

Change `crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
index 01809bc..f89a0c1 100644
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -9,6 +9,7 @@ pub mod accumulator;
 pub mod backoff;
 pub mod batch;
 pub mod buffer;
+mod checkpoint;
 mod cursor;
 mod decide;
 pub mod error;
```

Change `crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 44da560..198db39 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -15,8 +15,8 @@ use std::time::Instant;
 
 use data_avalanche_core::{
     CdcEvent, ChangeStream, ColumnValue, HaltReason, Op, RescanReport, RetainedLog, Row,
-    SchemaHint, Sink, SourceConnector, SourceError, SourcePosition, SourceSeq, StreamItem,
-    TableRef, TableSchema, TransactionBoundary, identifier_values,
+    SchemaHint, ServerIdentity, Sink, SourceConnector, SourceError, SourcePosition, SourceSeq,
+    StreamItem, TableRef, TableSchema, TransactionBoundary, identifier_values,
 };
 use data_avalanche_state::{PipelineDocs, TableHalt};
 use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
@@ -79,6 +79,9 @@ pub(crate) struct Core {
     /// The tables whose chunk reads are failing, a streak each. Like `requery_failing` it outlives
     /// a session.
     pub read_failing: BTreeMap<TableRef, Streak>,
+    /// The server the checkpoint is recorded with (MySQL; `None` for PostgreSQL): the one the
+    /// last start read, from `prepare` or the reset position. A reconnect keeps it.
+    pub server: Option<ServerIdentity>,
     /// [`Command::Pause`] is in force: a session finishes its open transaction, makes it durable
     /// and reads nothing more until [`Command::Resume`]. It outlives a session, so a reconnect
     /// while paused stays paused.
@@ -187,13 +190,11 @@ impl<'c> Session<'c> {
     /// Loads the durable state and opens the stream at the checkpoint; `Ok(Err)` when the source
     /// fails to open it.
     pub async fn open(core: &'c mut Core) -> Result<Result<Self, SourceError>, WorkerError> {
-        let Some(slot) = core.settings.slot.clone() else {
-            return Err(protocol("a stream session needs a PostgreSQL slot".into()));
-        };
-        let checkpoint = match core.docs.slot().await? {
-            Some(v) if !v.doc.is_intent() => SourcePosition::Pg(v.doc.confirmed_flush_lsn),
+        let from = match core.settings.checkpoint.load(&core.docs).await? {
+            Some(s) if !s.intent => s.checkpoint,
             _ => return Err(protocol("a stream session needs a checkpoint".into())),
         };
+        let checkpoint = from.position.clone();
         let cached: BTreeMap<TableRef, TableSchema> = core
             .docs
             .table_schemas()
@@ -243,11 +244,7 @@ impl<'c> Session<'c> {
             core.snapshot_started.entry(table.clone()).or_insert(now);
         }
         let live: Vec<TableSchema> = schemas.values().cloned().collect();
-        let stream = match core
-            .source
-            .open_stream(&checkpoint.clone().into(), &live)
-            .await
-        {
+        let stream = match core.source.open_stream(&from, &live).await {
             Ok(stream) => stream,
             Err(e) => return Ok(Err(e)),
         };
@@ -256,7 +253,8 @@ impl<'c> Session<'c> {
         let ctx = WriterCtx {
             sink: core.sink.clone(),
             docs: core.docs.clone(),
-            slot,
+            store: core.settings.checkpoint.clone(),
+            server: core.server.clone(),
             pipeline_id: core.settings.pipeline_id.clone(),
             owner_epoch: core.owner_epoch,
             guard: core.guard.clone(),
@@ -923,7 +921,8 @@ impl<'c> Session<'c> {
     /// the source's disk. A failed read waits for the next heartbeat, and the pipeline keeps
     /// running. The first permanent failure of a streak warns that the guard is blind; a read
     /// that succeeds ends the streak. The watchdog hears whether the source answered: a
-    /// transient failure, or a slot no stream reads, is no answer.
+    /// transient failure, or a stream the source doesn't run (a slot no stream reads, no binlog
+    /// dump), is no answer.
     fn wal_lag(
         &mut self,
         read: Result<Result<Option<RetainedLog>, SourceError>, JoinError>,
@@ -956,10 +955,11 @@ impl<'c> Session<'c> {
             }
         };
         self.core.wal_lag_failing.succeeded();
+        let unread = self.core.settings.checkpoint.unread();
         self.watchdog.answered(
             retained
                 .is_some_and(|r| !r.streamed)
-                .then(|| "no stream reads the slot".to_owned()),
+                .then(|| unread.to_owned()),
         );
         let retained = retained.map(|r| r.bytes);
         self.core.update(|st| st.retained_bytes = retained);
```

Change `crates/data-avalanche-pipeline/src/settings.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/settings.rs b/crates/data-avalanche-pipeline/src/settings.rs
index 229e713..1960ce0 100644
--- a/crates/data-avalanche-pipeline/src/settings.rs
+++ b/crates/data-avalanche-pipeline/src/settings.rs
@@ -11,6 +11,7 @@ use data_avalanche_core::{
     PartitionField, PartitionTransform, SinkKind, TableRef, TableSchema, WriteMode,
 };
 
+use crate::checkpoint::Store;
 use crate::{FlushPolicy, Lineage, TxnLimits};
 
 /// Status interval and stall detection when the source has no heartbeat setting.
@@ -30,8 +31,9 @@ pub(crate) struct Settings {
     pub chunk_size: usize,
     /// `source.timeouts.snapshot`: one table's snapshot within one run.
     pub snapshot_timeout: Duration,
-    /// `source.postgres.heartbeat`: the WAL-lag check runs at this interval; a stream with no
-    /// item for three of them is stalled.
+    /// `source.postgres.heartbeat` or `source.mysql.heartbeat`: the source's log is read at this
+    /// interval (PostgreSQL: the WAL-lag check; MySQL: whether the dump still runs); a stream
+    /// with no item for three of them is stalled.
     pub heartbeat: Duration,
     pub max_wal_lag_bytes: Option<u64>,
     /// Channel D; `None` when `rescan_interval` is 0.
@@ -39,11 +41,8 @@ pub(crate) struct Settings {
     pub evolution: bool,
     /// `toast_strategy: re_query`.
     pub re_query: bool,
-    /// PostgreSQL `slot_management: managed`: the slot name, whose creation intent a first start
-    /// records. `None` when the pipeline never creates the slot.
-    pub slot_intent: Option<String>,
-    /// PostgreSQL: the slot holding the checkpoint (`replication/slot.json`).
-    pub slot: Option<String>,
+    /// The document holding the checkpoint.
+    pub checkpoint: Store,
     mode: SnapshotMode,
     overrides: BTreeMap<String, SnapshotMode>,
     partitioning: BTreeMap<String, Vec<PartitionField>>,
@@ -94,16 +93,23 @@ impl Settings {
                 .unwrap_or(usize::MAX)
                 .max(1),
             snapshot_timeout: p.source.timeouts.snapshot,
-            heartbeat: pg.map_or(DEFAULT_HEARTBEAT, |pg| pg.heartbeat),
+            heartbeat: pg
+                .map(|pg| pg.heartbeat)
+                .or(p.source.mysql.as_ref().map(|m| m.heartbeat))
+                .unwrap_or(DEFAULT_HEARTBEAT),
             max_wal_lag_bytes: pg.and_then(|pg| pg.max_wal_lag_bytes),
             rescan_interval: (!p.schema_evolution.rescan_interval.is_zero())
                 .then_some(p.schema_evolution.rescan_interval),
             evolution: p.schema_evolution.enabled,
             re_query: p.source.toast_strategy == ToastStrategy::ReQuery,
-            slot_intent: pg
-                .filter(|pg| pg.slot_management == data_avalanche_config::SlotManagement::Managed)
-                .map(|pg| pg.slot.clone()),
-            slot: pg.map(|pg| pg.slot.clone()),
+            checkpoint: match pg {
+                Some(pg) => Store::Slot {
+                    name: pg.slot.clone(),
+                    intent: (pg.slot_management == data_avalanche_config::SlotManagement::Managed)
+                        .then(|| pg.slot.clone()),
+                },
+                None => Store::Position,
+            },
             mode: p.tables.snapshots.mode,
             overrides: p.tables.snapshots.overrides.clone(),
             partitioning: p
```

Change `crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 1eaa335..19f5f9f 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -10,11 +10,9 @@ use std::time::Instant;
 use data_avalanche_config::{Pipeline, SnapshotMode};
 use data_avalanche_core::{
     Checkpoint, HaltReason, SchemaHint, Sink, SinkError, SinkKind, SnapshotReason, SourceConnector,
-    SourceError, SourcePosition, StartPlan, TableRef, TableSchema, TableSpec,
-};
-use data_avalanche_state::{
-    PipelineDocs, SlotDoc, SnapshotStatus, StateError, TableHalt, TableSnapshotDoc,
+    SourceError, StartPlan, TableRef, TableSchema, TableSpec,
 };
+use data_avalanche_state::{PipelineDocs, SnapshotStatus, StateError, TableHalt, TableSnapshotDoc};
 use data_avalanche_storage::StoreError;
 use tokio::sync::{mpsc, watch};
 
@@ -109,6 +107,7 @@ impl PipelineWorker {
             wal_lag_failing: Streak::default(),
             writers_waited: BTreeSet::new(),
             read_failing: BTreeMap::new(),
+            server: None,
             pause: false,
         };
         (Self { core }, WorkerHandle { control, status })
@@ -178,7 +177,7 @@ impl PipelineWorker {
                         || core.source.reset_position(),
                     )
                     .await?;
-                    start(core, Some(from.position)).await?;
+                    start(core, Some(from)).await?;
                     attempt = 0;
                     continue;
                 }
@@ -283,13 +282,10 @@ where
 }
 
 /// Data-Flow Phase 0, or its rerun after the stream position was lost mid-run (`lost` holds the
-/// new position `reset_position` returned). Leaves the core tracking the tables to stream, with
+/// new checkpoint `reset_position` returned). Leaves the core tracking the tables to stream, with
 /// every decision persisted: halts and snapshot requests first, then the cached schemas, then a
-/// fresh start's checkpoint.
-pub(crate) async fn start(
-    core: &mut Core,
-    lost: Option<SourcePosition>,
-) -> Result<(), WorkerError> {
+/// fresh start's checkpoint, recorded with the server the start read (MySQL).
+pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(), WorkerError> {
     core.update(|st| st.phase = Phase::Starting);
     let s = &core.settings;
     if s.sink_kind == SinkKind::Iceberg {
@@ -297,11 +293,6 @@ pub(crate) async fn start(
             "the Iceberg sink ships in v2 (D-V1.4); other pipelines run".into(),
         ));
     }
-    let Some(slot) = s.slot.clone() else {
-        return Err(WorkerError::Refused(
-            "this build streams from PostgreSQL only; the MySQL source arrives with M7".into(),
-        ));
-    };
     if core.sink.kind() != s.sink_kind {
         return Err(WorkerError::Refused(format!(
             "the sink given is {:?}, the config says {:?}",
@@ -363,7 +354,7 @@ pub(crate) async fn start(
         .into_iter()
         .map(|h| h.table)
         .collect();
-    let slot_doc = core.docs.slot().await?.map(|v| v.doc);
+    let stored = core.settings.checkpoint.load(&core.docs).await?;
 
     // New halts, recorded before the checkpoint below.
     let mut halts: Vec<(TableRef, HaltReason)> = resolved.halted.clone();
@@ -411,37 +402,39 @@ pub(crate) async fn start(
     }
 
     // The stream position: prepare, or the one a mid-run recovery got.
-    let intent = slot_doc.as_ref().is_some_and(SlotDoc::is_intent);
-    let checkpoint = slot_doc
-        .as_ref()
-        .map(|d| SourcePosition::Pg(d.confirmed_flush_lsn));
-    let (plan, at, added) = match lost {
-        Some(from) => (Plan::Fresh(SnapshotReason::PositionLost), from, vec![]),
+    let intent = stored.as_ref().is_some_and(|s| s.intent);
+    let checkpoint = stored.map(|s| s.checkpoint);
+    let (plan, at, added, server) = match lost {
+        Some(from) => (
+            Plan::Fresh(SnapshotReason::PositionLost),
+            from.position,
+            vec![],
+            from.server,
+        ),
         None => {
-            if slot_doc.is_none()
-                && let Some(name) = &core.settings.slot_intent
+            if checkpoint.is_none()
+                && let Some(doc) = core.settings.checkpoint.intent()
             {
                 core.may_write()?;
-                core.docs.save_slot(&SlotDoc::intent(name)).await?;
+                core.docs.save_slot(&doc).await?;
             }
             let refs: Vec<TableRef> = resolved_tables.keys().cloned().collect();
-            let durable = checkpoint.clone().map(Checkpoint::from);
             // `prepare` may create, recreate or advance the slot.
             let prepared = write_retry(
                 &tuning,
                 || core.may_write(),
-                || source.prepare(durable.as_ref(), &refs),
+                || source.prepare(checkpoint.as_ref(), &refs),
             )
             .await?;
             warnings.extend(prepared.warnings);
             match prepared.plan {
                 StartPlan::Resume => {
-                    let Some(at) = checkpoint.clone() else {
+                    let Some(at) = checkpoint.as_ref().map(|c| c.position.clone()) else {
                         return Err(WorkerError::Refused(
                             "the source resumes a stream the pipeline has no checkpoint for".into(),
                         ));
                     };
-                    (Plan::Resume, at, prepared.added)
+                    (Plan::Resume, at, prepared.added, prepared.server)
                 }
                 StartPlan::Snapshot { from, reason } => {
                     // A start cut short after the slot was made starts over as a first start.
@@ -450,11 +443,17 @@ pub(crate) async fn start(
                     } else {
                         reason
                     };
-                    (Plan::Fresh(reason), from, vec![])
+                    (Plan::Fresh(reason), from, vec![], prepared.server)
                 }
             }
         }
     };
+    // The checkpoint document now: written at a fresh start, and a check at a resume that the
+    // source gave what the store records (a MySQL source names the server).
+    let doc = core.settings.checkpoint.doc(&Checkpoint {
+        position: at.clone(),
+        server: server.clone(),
+    })?;
     let gap = matches!(
         plan,
         Plan::Fresh(SnapshotReason::PositionLost | SnapshotReason::AdoptedOrphan)
@@ -624,20 +623,7 @@ pub(crate) async fn start(
             })
             .await?;
     }
-    let fresh = match plan {
-        Plan::Resume => None,
-        Plan::Fresh(_) => {
-            let SourcePosition::Pg(lsn) = at else {
-                return Err(WorkerError::Refused(format!(
-                    "the source started at {at}, not a PostgreSQL position"
-                )));
-            };
-            Some(SlotDoc {
-                slot_name: slot,
-                confirmed_flush_lsn: lsn,
-            })
-        }
-    };
+    let fresh = matches!(plan, Plan::Fresh(_)).then_some(doc);
     // The schemas before a fresh start's checkpoint: a restart in between still finds the
     // checkpoint before it and starts fresh again. The other way round, it would resume from the
     // new checkpoint under the cache of before the baseline, as if the stream still held the
@@ -651,8 +637,9 @@ pub(crate) async fn start(
             return Err(WorkerError::Crashed(HookPoint::SchemasCached));
         }
         core.may_write()?;
-        core.docs.save_slot(&doc).await?;
+        doc.save(&core.docs).await?;
     }
+    core.server = server;
     core.tracked = tracked;
     core.drifted = drifted;
     core.dump_only = dump_only;
```

Change `crates/data-avalanche-pipeline/src/writer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index 160ac86..c5408cc 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -12,16 +12,17 @@ use std::collections::BTreeSet;
 use std::sync::Arc;
 
 use data_avalanche_core::{
-    CommitMarker, CommitOutcome, HaltReason, Sink, SinkError, SourcePosition, TableChangeSet,
-    TableRef, TableSchema,
+    Checkpoint, CommitMarker, CommitOutcome, HaltReason, ServerIdentity, Sink, SinkError,
+    SourcePosition, TableChangeSet, TableRef, TableSchema,
 };
-use data_avalanche_state::{PipelineDocs, SlotDoc, StateError, TableHalt, TableSnapshotDoc};
+use data_avalanche_state::{PipelineDocs, StateError, TableHalt, TableSnapshotDoc};
 use data_avalanche_storage::StoreError;
 use tokio::sync::OwnedSemaphorePermit;
 use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
 
 use crate::backoff::Backoff;
-use crate::{AccumulatorError, CommitGuard, HookAction, HookPoint, Hooks, WorkerError};
+use crate::checkpoint::{Doc, Store};
+use crate::{CommitGuard, HookAction, HookPoint, Hooks, WorkerError};
 
 /// One flush and what becomes durable with it.
 #[derive(Debug)]
@@ -58,8 +59,11 @@ pub(crate) enum WriterEvent {
 pub(crate) struct WriterCtx {
     pub sink: Arc<dyn Sink>,
     pub docs: PipelineDocs,
-    /// The slot whose document holds the checkpoint (PostgreSQL).
-    pub slot: String,
+    /// The document that holds the checkpoint.
+    pub store: Store,
+    /// The server each checkpoint is recorded with (MySQL; `None` for PostgreSQL): the one the
+    /// last start read the stream from.
+    pub server: Option<ServerIdentity>,
     pub pipeline_id: String,
     pub owner_epoch: u64,
     pub guard: Arc<dyn CommitGuard>,
@@ -70,7 +74,7 @@ pub(crate) struct WriterCtx {
 impl std::fmt::Debug for WriterCtx {
     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
         f.debug_struct("WriterCtx")
-            .field("slot", &self.slot)
+            .field("store", &self.store)
             .field("pipeline_id", &self.pipeline_id)
             .field("owner_epoch", &self.owner_epoch)
             .finish_non_exhaustive()
@@ -221,20 +225,14 @@ impl Writer {
     }
 
     async fn save_checkpoint(&mut self, checkpoint: &SourcePosition) -> Result<(), WorkerError> {
-        let SourcePosition::Pg(lsn) = checkpoint else {
-            return Err(AccumulatorError::Protocol {
-                detail: format!("checkpoint {checkpoint} isn't a PostgreSQL position"),
-            }
-            .into());
-        };
+        let doc = self.ctx.store.doc(&Checkpoint {
+            position: checkpoint.clone(),
+            server: self.ctx.server.clone(),
+        })?;
         if !self.ctx.guard.may_commit() {
             return Err(WorkerError::LeaseLost);
         }
-        let doc = SlotDoc {
-            slot_name: self.ctx.slot.clone(),
-            confirmed_flush_lsn: *lsn,
-        };
-        self.state(Write::Slot(&doc)).await
+        self.state(Write::Checkpoint(&doc)).await
     }
 
     /// A state write, retried while it fails transiently.
@@ -242,15 +240,15 @@ impl Writer {
         let mut attempt = 0;
         loop {
             // Checked before every attempt, like a commit's: a writer that lost its lease while a
-            // write kept failing must not land it later, behind the new owner's (`save_slot`
-            // overwrites).
+            // write kept failing must not land it later, behind the new owner's (a checkpoint
+            // write overwrites).
             if !self.ctx.guard.may_commit() {
                 return Err(WorkerError::LeaseLost);
             }
             let docs = &self.ctx.docs;
             let r = match write {
                 Write::Halt(h) => docs.record_halt(h).await.map(drop),
-                Write::Slot(s) => docs.save_slot(s).await.map(drop),
+                Write::Checkpoint(d) => d.save(docs).await,
                 Write::Snapshot(d) => docs.save_table_snapshot(d).await.map(drop),
                 Write::Schema(s) => docs.save_table_schema(s).await.map(drop),
             };
@@ -274,7 +272,7 @@ impl Writer {
 #[derive(Clone, Copy)]
 enum Write<'a> {
     Halt(&'a TableHalt),
-    Slot(&'a SlotDoc),
+    Checkpoint(&'a Doc),
     Snapshot(&'a TableSnapshotDoc),
     Schema(&'a TableSchema),
 }
```

Change `crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index d81579d..6857c9c 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -33,9 +33,9 @@ use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
     CdcEvent, ChangeStream, Checkpoint, Chunk, ChunkVisibility, ColumnSchema, ColumnValue,
     HaltReason, InFlightTxn, KeysetCursor, Op, Preflight, Prepared, RescanReport, ResolvedTables,
-    RetainedLog, Row, SchemaChange, SchemaDelta, SchemaHint, SnapshotReason, SourceConnector,
-    SourceError, SourcePosition, SourceSeq, StartPlan, StreamItem, TableRef, TableSchema,
-    TransactionBoundary, XidSnapshot, identifier_values,
+    RetainedLog, Row, SchemaChange, SchemaDelta, SchemaHint, ServerIdentity, SnapshotReason,
+    SourceConnector, SourceError, SourcePosition, SourceSeq, StartPlan, StreamItem, TableRef,
+    TableSchema, TransactionBoundary, XidSnapshot, identifier_values,
 };
 use tokio::sync::Notify;
 
@@ -244,12 +244,14 @@ struct Db {
     waiting: usize,
     after_rescan: Option<Action>,
     acknowledged: Vec<SourcePosition>,
-    opened: Vec<SourcePosition>,
+    opened: Vec<Checkpoint>,
+    /// [`FakeSource::set_server`].
+    server: Option<ServerIdentity>,
     reads: BTreeMap<TableRef, usize>,
     rescans: usize,
     in_flights: usize,
     fetches: BTreeMap<TableRef, usize>,
-    prepares: Vec<Option<SourcePosition>>,
+    prepares: Vec<Option<Checkpoint>>,
 }
 
 /// The scripted source. Cloning shares the database.
@@ -296,6 +298,7 @@ impl FakeSource {
             after_rescan: None,
             acknowledged: vec![],
             opened: vec![],
+            server: None,
             reads: BTreeMap::new(),
             rescans: 0,
             in_flights: 0,
@@ -522,14 +525,39 @@ impl FakeSource {
 
     /// Where each stream was opened, in order.
     pub fn opened(&self) -> Vec<SourcePosition> {
+        self.db()
+            .opened
+            .iter()
+            .map(|c| c.position.clone())
+            .collect()
+    }
+
+    /// The checkpoint each stream was opened at, with its server, in order.
+    pub fn opened_at(&self) -> Vec<Checkpoint> {
         self.db().opened.clone()
     }
 
-    /// The checkpoint each `prepare` was called with.
+    /// The checkpoint's position each `prepare` was called with.
     pub fn prepares(&self) -> Vec<Option<SourcePosition>> {
+        self.db()
+            .prepares
+            .iter()
+            .map(|c| c.as_ref().map(|c| c.position.clone()))
+            .collect()
+    }
+
+    /// The checkpoint each `prepare` was called with, with its server.
+    pub fn prepared_at(&self) -> Vec<Option<Checkpoint>> {
         self.db().prepares.clone()
     }
 
+    /// The server `prepare` and `reset_position` report from now on, as a MySQL source does
+    /// (`None`, the default: none, as PostgreSQL). Changing it is a failover: the source answers
+    /// from another server.
+    pub fn set_server(&self, server: Option<ServerIdentity>) {
+        self.db().server = server;
+    }
+
     /// Chunk reads of `table` so far.
     pub fn reads(&self, table: &TableRef) -> usize {
         self.db().reads.get(table).copied().unwrap_or(0)
@@ -866,7 +894,8 @@ impl SourceConnector for FakeSource {
     ) -> Result<Prepared, SourceError> {
         self.take_fault(Method::Prepare)?;
         let mut db = self.db();
-        db.prepares.push(checkpoint.map(|c| c.position.clone()));
+        db.prepares.push(checkpoint.cloned());
+        let server = db.server.clone();
         let checkpoint = checkpoint.map(|c| lsn(&c.position));
         let reason = match (checkpoint, db.slot) {
             (None, None) => Some(SnapshotReason::FirstStart),
@@ -885,7 +914,7 @@ impl SourceConnector for FakeSource {
                 plan: StartPlan::Resume,
                 added: std::mem::take(&mut db.added),
                 warnings: vec![],
-                server: None,
+                server,
             },
             Some(reason) => {
                 db.slot = Some(db.wal_end);
@@ -897,7 +926,7 @@ impl SourceConnector for FakeSource {
                     },
                     added: vec![],
                     warnings: vec![],
-                    server: None,
+                    server,
                 }
             }
         })
@@ -908,7 +937,10 @@ impl SourceConnector for FakeSource {
         let mut db = self.db();
         db.slot = Some(db.wal_end);
         db.slot_lost = false;
-        Ok(SourcePosition::Pg(db.wal_end).into())
+        Ok(Checkpoint {
+            position: SourcePosition::Pg(db.wal_end),
+            server: db.server.clone(),
+        })
     }
 
     async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
@@ -1037,7 +1069,7 @@ impl SourceConnector for FakeSource {
         tables: &[TableSchema],
     ) -> Result<Box<dyn ChangeStream>, SourceError> {
         self.take_fault(Method::OpenStream)?;
-        self.db().opened.push(from.position.clone());
+        self.db().opened.push(from.clone());
         Ok(Box::new(FakeStream {
             source: self.clone(),
             after: lsn(&from.position),
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-pipeline --lib`

Expected: PASS, 208 tests.

Run: `cargo test -p data-avalanche-testkit --lib fake_source::`

Expected: PASS, 9 tests.

Run: `cargo test --workspace`

Expected: PASS, every test binary.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline/src/checkpoint.rs \
  crates/data-avalanche-pipeline/src/lib.rs \
  crates/data-avalanche-pipeline/src/session.rs \
  crates/data-avalanche-pipeline/src/settings.rs \
  crates/data-avalanche-pipeline/src/test_support.rs \
  crates/data-avalanche-pipeline/src/worker.rs \
  crates/data-avalanche-pipeline/src/writer.rs \
  crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "feat(pipeline): the MySQL checkpoint — the position document with its server; the worker streams MySQL"
```

---

### Task 3: A pipeline halt's code in the status (`data-avalanche-cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/controller.rs`
- Modify: `crates/data-avalanche-pipeline/src/error.rs`

**Interfaces:**
- Consumes:
  - Task 1's `SourceError::halt_code`.
  - `WorkerError::recovery`, `Recovery::Error` (pipeline, M6b).
- Produces:
  - `WorkerError::code(&self) -> Option<&str>` (pipeline).
  - `Actual::Error { error: String, code: Option<String> }` (cluster).
  - The `ERROR` log line's `code` field: `pipeline in ERROR: <error>` with `code="BINLOG_…"`.

Q-M7b-4, D-M7b.2. A worker that ends with a pipeline-scope halt already lands in `ERROR`: a permanent source error is `Recovery::Error`, and the metric reads `status="error"`. What the operator couldn't see was which halt. `WorkerError::code` reads the code from a source error's message. The controller keeps it beside the error in a `Failure` and publishes it in `Actual::Error`, and the `ERROR` line logs it as a field (`tracing`'s `Option` field: absent when `None`). No new metric. A factory error (the parts can't be built) has no code.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 0697571..5cd04fb 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -822,6 +840,32 @@ mod tests {
         eventually("the row lands", || f.ids("a") == [7]).await;
     }
 
+    /// A pipeline-scope halt's code rides the `ERROR` state (Q-M7b-4).
+    #[tokio::test(start_paused = true)]
+    async fn a_pipeline_halt_publishes_its_code() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        f.source.fail(
+            Method::Prepare,
+            SourceError::Permanent(
+                "BINLOG_SOURCE_CHANGED: binlog position f:4 was recorded on another server".into(),
+            ),
+        );
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "error", |s| {
+            matches!(
+                &s.actual,
+                Some(Actual::Error { code: Some(c), .. }) if c == "BINLOG_SOURCE_CHANGED"
+            )
+        })
+        .await;
+        assert!(line(
+            &n.metrics.encode(),
+            &format!(r#"data_avalanche_pipeline_status{{{P},status="error"}} 1"#)
+        ));
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_permanent_worker_error_holds_until_a_stop_and_a_resume() {
         let store = ScriptedStore::new();
@@ -833,7 +877,7 @@ mod tests {
         );
         let mut n = node("n1", &store, &docs, &f);
         until(&mut n.status, "error", |s| {
-            matches!(&s.actual, Some(Actual::Error { error }) if error.contains("no privilege"))
+            matches!(&s.actual, Some(Actual::Error { error, .. }) if error.contains("no privilege"))
         })
         .await;
         tokio::time::sleep(Duration::from_secs(60)).await;
@@ -1007,7 +1051,7 @@ mod tests {
         until(
             &mut status,
             "error",
-            |s| matches!(&s.actual, Some(Actual::Error { error }) if error.contains("boom")),
+            |s| matches!(&s.actual, Some(Actual::Error { error, .. }) if error.contains("boom")),
         )
         .await;
         idles_through(Duration::from_secs(10)).await;
```

Change `crates/data-avalanche-pipeline/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/error.rs b/crates/data-avalanche-pipeline/src/error.rs
index 0d06f4d..1d981c8 100644
--- a/crates/data-avalanche-pipeline/src/error.rs
+++ b/crates/data-avalanche-pipeline/src/error.rs
@@ -140,6 +149,18 @@ impl From<AccumulatorError> for WorkerError {
 mod tests {
     use super::*;
 
+    #[test]
+    fn a_source_halt_names_its_code() {
+        let halt = WorkerError::Source(SourceError::Permanent(
+            "BINLOG_SOURCE_CHANGED: binlog position f:4 was recorded on another server".into(),
+        ));
+        assert_eq!(halt.code(), Some("BINLOG_SOURCE_CHANGED"));
+        assert_eq!(halt.recovery(), Recovery::Error);
+        let plain = WorkerError::Source(SourceError::Permanent("no privilege".into()));
+        assert_eq!(plain.code(), None);
+        assert_eq!(WorkerError::Refused("BINLOG_X: y".into()).code(), None);
+    }
+
     #[test]
     fn a_transaction_too_large_is_its_own_stop() {
         let e = WorkerError::from(AccumulatorError::TransactionTooLarge {
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-pipeline --lib error:: && cargo test -p data-avalanche-cluster --lib controller::`

Expected: FAIL. The tests don't compile: `WorkerError::code` and the `code` field of `Actual::Error` don't exist. The first errors:

```text
error[E0599]: no method named `code` found for enum `error::WorkerError` in the current scope  --> crates/data-avalanche-pipeline/src/error.rs:148:25
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 0697571..5cd04fb 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -70,12 +70,21 @@ pub enum Actual {
         error: String,
     },
     /// The worker ended with an error an operator must act on (Q-M6b-3): no rerun until the
-    /// process restarts, or a stop then a resume.
+    /// process restarts, or a stop then a resume. `code` names a pipeline-scope halt
+    /// (`BINLOG_PAYLOAD_INVALID`, `BINLOG_SOURCE_CHANGED`; Q-M7b-4).
     Error {
         error: String,
+        code: Option<String>,
     },
 }
 
+/// Why the pipeline is in `ERROR`: the error, and the halt's code when it names one.
+#[derive(Debug, Clone)]
+struct Failure {
+    error: String,
+    code: Option<String>,
+}
+
 #[derive(Debug, Clone, PartialEq, Default)]
 pub struct ControllerStatus {
     pub actual: Option<Actual>,
@@ -279,7 +288,7 @@ impl PipelineController {
 
     async fn reconcile(&self, run: LeaseRun, target: &mut watch::Receiver<Target>) -> Left {
         let mut worker: Option<Running> = None;
-        let mut error: Option<String> = None;
+        let mut error: Option<Failure> = None;
         let mut dumped = false;
         let mut retry: Option<(Instant, String)> = None;
         let mut failures: u32 = 0;
@@ -327,7 +336,7 @@ impl PipelineController {
                 &run,
                 worker.as_ref(),
                 desired,
-                error.as_deref(),
+                error.as_ref(),
                 dumped,
                 retry.as_ref().map(|(_, e)| e.as_str()),
             );
@@ -438,11 +447,16 @@ impl PipelineController {
                                 return Left::Lost;
                             }
                             Recovery::Error => {
+                                let code = e.code().map(str::to_owned);
                                 tracing::error!(
                                     pipeline = %self.pipeline.name,
+                                    code = code.as_deref(),
                                     "pipeline in ERROR: {e}"
                                 );
-                                error = Some(e.to_string());
+                                error = Some(Failure {
+                                    error: e.to_string(),
+                                    code,
+                                });
                             }
                         },
                     }
@@ -463,7 +477,10 @@ impl PipelineController {
                     Ok(w) => worker = Some(w),
                     Err(e) => {
                         tracing::error!(pipeline = %self.pipeline.name, "pipeline in ERROR: {e}");
-                        error = Some(e);
+                        error = Some(Failure {
+                            error: e,
+                            code: None,
+                        });
                     }
                 },
                 Event::Due => {}
@@ -618,14 +635,15 @@ impl PipelineController {
         run: &LeaseRun,
         worker: Option<&Running>,
         desired: Option<DesiredState>,
-        error: Option<&str>,
+        error: Option<&Failure>,
         dumped: bool,
         retrying: Option<&str>,
     ) {
         let phase = worker.map(|w| w.handle.status.borrow().phase.clone());
-        let actual = if let Some(e) = error {
+        let actual = if let Some(f) = error {
             Actual::Error {
-                error: e.to_owned(),
+                error: f.error.clone(),
+                code: f.code.clone(),
             }
         } else if dumped {
             Actual::Dumped
```

Change `crates/data-avalanche-pipeline/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/error.rs b/crates/data-avalanche-pipeline/src/error.rs
index 0d06f4d..1d981c8 100644
--- a/crates/data-avalanche-pipeline/src/error.rs
+++ b/crates/data-avalanche-pipeline/src/error.rs
@@ -102,6 +102,15 @@ pub enum Recovery {
 }
 
 impl WorkerError {
+    /// The code of a pipeline-scope halt (`BINLOG_PAYLOAD_INVALID`, `BINLOG_SOURCE_CHANGED`): a
+    /// permanent source error whose message names one ([`SourceError::halt_code`]).
+    pub fn code(&self) -> Option<&str> {
+        match self {
+            Self::Source(e) => e.halt_code(),
+            _ => None,
+        }
+    }
+
     pub fn recovery(&self) -> Recovery {
         match self {
             Self::State(StateError::Store(
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-pipeline --lib error::`

Expected: PASS, 3 tests.

Run: `cargo test -p data-avalanche-cluster --lib controller::`

Expected: PASS, 16 tests.

Run: `cargo test --workspace`

Expected: PASS, every test binary.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster/src/controller.rs \
  crates/data-avalanche-pipeline/src/error.rs
git commit -m "feat(cluster): a pipeline halt's code in the ERROR state and its log line (Q-M7b-4)"
```

---

### Task 4: The MySQL source checks a file/position dump's server (`data-avalanche-source-mysql`)

**Files:**
- Modify: `crates/data-avalanche-source-mysql/src/connector.rs`
- Modify: `crates/data-avalanche-source-mysql/src/position.rs`
- Modify: `crates/data-avalanche-source-mysql/src/stream.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/binlog_rotate.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/mariadb_compress.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/mysql/start.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/mysql/stream.rs`
- Modify (test): `crates/data-avalanche-source-mysql/tests/row_metadata.rs`

**Interfaces:**
- Consumes:
  - Task 1's `open_stream(&Checkpoint)`.
  - `same_server(&Checkpoint, &ServerIdentity)` (`start.rs`, M7a), `Flavour`, `Mode`.
- Produces:
  - `MySqlConnector::open_stream(&self, from: &Checkpoint, tables) -> Result<MySqlStream, SourceError>` (inherent; the trait's impl passes `from` on).
  - `MySqlConnector::checkpoint_now(&self) -> Result<Checkpoint, SourceError>` (the write position and the server; `reset_position` returns it).
  - `position.rs`: `identity_query(Flavour) -> &'static str`, `identity(Flavour, Option<Option<String>>) -> Result<ServerIdentity, SourceError>` (crate-private).

D-M7b.7, D-M7b.8; M7a's carry items "an error when `server_uuid` reads NULL" and "a file/position resume IT".

In file/position mode `prepare` checks the checkpoint's server (M7a). A reconnect doesn't run `prepare`, so after a failover behind the pipeline's address it would stream another server's binlog from a file and offset that mean nothing there. The dump now reads the identity on its own connection, after the connect and before the dump command, and refuses with `same_server`'s `BINLOG_SOURCE_CHANGED` (D-M7a's message). GTID modes don't check: GTIDs carry across servers.

The identity read becomes a pure function of the server's answer. No row, a NULL or an empty value is an error, where M7a's `unwrap_or_default` made an empty identity. `checkpoint_now` is the write position with its server, for `reset_position` and for the tests. The inherent `open_stream` takes the checkpoint too, so every test that opens a stream hands it one with its server (`checkpoint_now`, or a helper `at`).

The ITs run on every target. `a_file_position_checkpoint_resumes_after_itself` forces `replication_type: binlog`, which works on a GTID server too. `a_file_position_stream_opens_only_on_its_server` opens a file/position stream with a foreign server, then with none: each is refused and no dump starts. On a GTID target, a GTID stream with the foreign server opens.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-source-mysql/src/position.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/position.rs b/crates/data-avalanche-source-mysql/src/position.rs
index 396a78a..c83bb88 100644
--- a/crates/data-avalanche-source-mysql/src/position.rs
+++ b/crates/data-avalanche-source-mysql/src/position.rs
@@ -225,6 +259,36 @@ pub(crate) fn parse(mode: Mode, text: &str) -> Result<SourcePosition, SourceErro
 mod tests {
     use super::*;
 
+    /// A NULL, missing or empty identity is an error, not an identity that matches nothing.
+    #[test]
+    fn a_server_that_reports_no_identity_is_an_error() {
+        let uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
+        assert_eq!(
+            identity(Flavour::Mysql, Some(Some(uuid.into()))).unwrap(),
+            ServerIdentity::Mysql {
+                server_uuid: uuid.into()
+            }
+        );
+        assert_eq!(
+            identity(Flavour::Mariadb, Some(Some("7".into()))).unwrap(),
+            ServerIdentity::Mariadb { server_id: 7 }
+        );
+        for (flavour, answer) in [
+            (Flavour::Mysql, Some(None)),
+            (Flavour::Mysql, None),
+            (Flavour::Mysql, Some(Some(String::new()))),
+            (Flavour::Mariadb, Some(None)),
+        ] {
+            let e = identity(flavour, answer.clone()).unwrap_err();
+            assert!(
+                matches!(&e, SourceError::Permanent(m) if m.starts_with("the server reports no @@")),
+                "{answer:?}: {e}"
+            );
+        }
+        let e = identity(Flavour::Mariadb, Some(Some("x".into()))).unwrap_err();
+        assert!(e.to_string().contains("not a number"), "{e}");
+    }
+
     fn logs(sizes: &[(&str, u64)]) -> Vec<BinaryLog> {
         sizes
             .iter()
```

Change `crates/data-avalanche-source-mysql/tests/binlog_rotate.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/binlog_rotate.rs b/crates/data-avalanche-source-mysql/tests/binlog_rotate.rs
index 9404ccf..afc4316 100644
--- a/crates/data-avalanche-source-mysql/tests/binlog_rotate.rs
+++ b/crates/data-avalanche-source-mysql/tests/binlog_rotate.rs
@@ -25,7 +25,7 @@ async fn a_rotate_moves_a_file_position_to_the_next_file() {
     let (n, mut a) = fresh("rotate", "CREATE TABLE t (id INT PRIMARY KEY)").await;
     let c = connector(&pipeline(&n, &["t"], "", ""));
     let tables = c.resolve_tables().await.unwrap().tables;
-    let from = c.position_now().await.unwrap();
+    let from = c.checkpoint_now().await.unwrap();
     let mut s = c.open_stream(&from, &tables).await.unwrap();
     a.query_drop("FLUSH BINARY LOGS").await.unwrap();
     let newest = c.binary_logs().await.unwrap().pop().unwrap().name;
```

Change `crates/data-avalanche-source-mysql/tests/mariadb_compress.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mariadb_compress.rs b/crates/data-avalanche-source-mysql/tests/mariadb_compress.rs
index 0e9d3e8..96f43ab 100644
--- a/crates/data-avalanche-source-mysql/tests/mariadb_compress.rs
+++ b/crates/data-avalanche-source-mysql/tests/mariadb_compress.rs
@@ -28,7 +28,7 @@ async fn compressed_events_decode() {
     .await;
     let c = connector(&pipeline(&n, &["t"], "", ""));
     let tables = c.resolve_tables().await.unwrap().tables;
-    let from = c.position_now().await.unwrap();
+    let from = c.checkpoint_now().await.unwrap();
     let mut s = c.open_stream(&from, &tables).await.expect("stream");
     let long = "x".repeat(300);
     // Compress every event of 10 bytes or more (the least the server takes), from a session opened
```

Change `crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs b/crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs
index 9b3f381..ac802f1 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs
@@ -257,7 +257,7 @@ async fn every_type_reads_the_same_from_the_stream_and_a_chunk() {
     let resolved = c.resolve_tables().await.unwrap();
     assert!(resolved.halted.is_empty(), "{:?}", resolved.halted);
     let tables = resolved.tables;
-    let from = c.position_now().await.unwrap();
+    let from = c.checkpoint_now().await.unwrap();
     let mut s = c.open_stream(&from, &tables).await.expect("stream");
 
     let names: Vec<&str> = columns.iter().map(|c| c.0).collect();
```

Change `crates/data-avalanche-source-mysql/tests/mysql/start.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/start.rs b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
index 8cb0886..8100eb0 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/start.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
@@ -4,10 +4,10 @@
 use std::time::Duration;
 
 use data_avalanche_core::{
-    ChangeStream, Checkpoint, RetainedLog, ServerIdentity, SnapshotReason, SourceConnector,
-    SourceError, SourcePosition, StartPlan,
+    CdcEvent, ChangeStream, Checkpoint, ColumnValue, RetainedLog, ServerIdentity, SnapshotReason,
+    SourceConnector, SourceError, SourcePosition, StartPlan, StreamItem,
 };
-use data_avalanche_source_mysql::{Flavour, Mode, MySqlConnector};
+use data_avalanche_source_mysql::{Flavour, Mode, MySqlConnector, MySqlStream};
 use data_avalanche_testkit::require_it;
 use mysql_async::prelude::Queryable;
 
@@ -211,7 +211,7 @@ async fn the_watch_finds_the_dump_by_its_connection() {
     ));
     watch(&c, false).await;
     let tables = c.resolve_tables().await.unwrap().tables;
-    let from = c.position_now().await.unwrap();
+    let from = c.checkpoint_now().await.unwrap();
     let mut s = c.open_stream(&from, &tables).await.expect("stream");
     watch(&c, true).await;
     let id = c.dump_id().expect("the dump's id");
@@ -235,3 +235,92 @@ async fn the_watch_finds_the_dump_by_its_connection() {
         .unwrap();
     cleanup(&n).await;
 }
+
+const FILE: &str = ", replication_type: binlog";
+
+/// The changes up to the stream's next commit, and the commit's position, within 20 s.
+async fn to_commit(s: &mut MySqlStream) -> (Vec<CdcEvent>, SourcePosition) {
+    tokio::time::timeout(Duration::from_secs(20), async {
+        let mut changes = vec![];
+        loop {
+            match s.next().await.expect("the stream reads") {
+                StreamItem::Change(e) => changes.push(e),
+                StreamItem::Commit(b) => return (changes, b.commit_pos),
+                _ => {}
+            }
+        }
+    })
+    .await
+    .expect("a commit within 20 s")
+}
+
+/// File/position mode resumes after its checkpoint: a restart's `prepare` resumes, and the
+/// stream opened at the checkpoint reads only what came after it.
+#[tokio::test]
+async fn a_file_position_checkpoint_resumes_after_itself() {
+    require_it!();
+    let (n, mut a) = fresh("start_file_resume", "CREATE TABLE t (id INT PRIMARY KEY)").await;
+    let c = connector(&pipeline(&n, &["t"], "", FILE));
+    assert_eq!(c.mode().await.unwrap(), Mode::File);
+    let tables = c.resolve_tables().await.unwrap().tables;
+    let from = c.checkpoint_now().await.unwrap();
+    let mut s = c.open_stream(&from, &tables).await.expect("stream");
+    a.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
+    let (_, end) = to_commit(&mut s).await;
+    Box::new(s).close().await.expect("close");
+    a.query_drop("INSERT INTO t VALUES (2)").await.unwrap();
+    let checkpoint = Checkpoint {
+        position: end,
+        server: from.server.clone(),
+    };
+    assert!(matches!(checkpoint.position, SourcePosition::BinlogFile(_)));
+    assert_eq!(prepare(&c, &checkpoint).await.unwrap(), StartPlan::Resume);
+    let mut s = c.open_stream(&checkpoint, &tables).await.expect("resumed");
+    let (changes, _) = to_commit(&mut s).await;
+    let ids: Vec<_> = changes
+        .iter()
+        .map(|e| e.after.as_ref().and_then(|r| r.get("id").cloned()))
+        .collect();
+    assert_eq!(
+        ids,
+        [Some(ColumnValue::Int32(2))],
+        "the row after the checkpoint, once"
+    );
+    Box::new(s).close().await.expect("close");
+    cleanup(&n).await;
+}
+
+/// In file/position mode the dump checks the server on its own connection: a checkpoint
+/// recorded on another server (a failover behind the same address), or with none, halts the
+/// pipeline before a dump starts. A GTID position isn't checked: GTIDs carry across servers.
+#[tokio::test]
+async fn a_file_position_stream_opens_only_on_its_server() {
+    require_it!();
+    let (n, _a) = fresh("start_file_server", "CREATE TABLE t (id INT PRIMARY KEY)").await;
+    let c = connector(&pipeline(&n, &["t"], "", FILE));
+    let tables = c.resolve_tables().await.unwrap().tables;
+    let now = c.position_now().await.unwrap();
+    for server in [Some(elsewhere()), None] {
+        let cp = Checkpoint {
+            position: now.clone(),
+            server,
+        };
+        match c.open_stream(&cp, &tables).await.err() {
+            Some(SourceError::Permanent(m)) => {
+                assert!(m.starts_with("BINLOG_SOURCE_CHANGED: "), "{m}")
+            }
+            other => panic!("{cp:?}: {other:?}"),
+        }
+    }
+    assert_eq!(c.dump_id(), None, "no dump started");
+    if target().replication == "gtid" {
+        let gtid = connector(&pipeline(&n, &["t"], "", ""));
+        let cp = Checkpoint {
+            position: gtid.position_now().await.unwrap(),
+            server: Some(elsewhere()),
+        };
+        let s = gtid.open_stream(&cp, &tables).await.expect("a GTID stream");
+        Box::new(s).close().await.expect("close");
+    }
+    cleanup(&n).await;
+}
```

Change `crates/data-avalanche-source-mysql/tests/mysql/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/stream.rs b/crates/data-avalanche-source-mysql/tests/mysql/stream.rs
index e558141..cd12023 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/stream.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/stream.rs
@@ -5,7 +5,8 @@
 use std::time::Duration;
 
 use data_avalanche_core::{
-    ChangeStream, ColumnValue, HaltReason, Op, SourceError, SourcePosition, StreamItem, TableSchema,
+    ChangeStream, Checkpoint, ColumnValue, HaltReason, Op, SourceError, SourcePosition, StreamItem,
+    TableSchema,
 };
 use data_avalanche_source_mysql::{Flavour, MySqlConnector, MySqlStream};
 use data_avalanche_testkit::require_it;
@@ -53,8 +54,16 @@ async fn shut(s: MySqlStream) {
     Box::new(s).close().await.expect("close");
 }
 
+/// `position` on `c`'s server, as a checkpoint records it.
+async fn at(c: &MySqlConnector, position: SourcePosition) -> Checkpoint {
+    Checkpoint {
+        position,
+        server: Some(c.server_identity().await.expect("the server's identity")),
+    }
+}
+
 async fn open(c: &MySqlConnector, tables: &[TableSchema]) -> MySqlStream {
-    let from = c.position_now().await.expect("position");
+    let from = c.checkpoint_now().await.expect("position");
     c.open_stream(&from, tables).await.expect("stream")
 }
 
@@ -136,7 +145,10 @@ async fn changes_arrive_as_transactions_and_resume_after_a_commit() {
     assert_eq!(events[0].source_pos, events[1].source_pos);
     shut(s).await;
     // Resume after the second commit: the third transaction, once.
-    let mut s = c.open_stream(&pos[1], &tables).await.unwrap();
+    let mut s = c
+        .open_stream(&at(&c, pos[1].clone()).await, &tables)
+        .await
+        .unwrap();
     let again = items(&mut s, 1).await;
     assert_eq!(changes(&again), [(Op::Delete, Some(ColumnValue::Int32(2)))]);
     shut(s).await;
@@ -501,7 +513,7 @@ async fn mysql_payloads_partial_json_and_tagged_gtids() {
         assert!(last.untagged().contains(":m7a:"), "{last}");
         shut(s).await;
         // A tagged set resumes.
-        let mut s = c.open_stream(&last, &tables).await.unwrap();
+        let mut s = c.open_stream(&at(&c, last).await, &tables).await.unwrap();
         a.query_drop("INSERT INTO t VALUES (5, 'null')")
             .await
             .unwrap();
```

Change `crates/data-avalanche-source-mysql/tests/row_metadata.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/row_metadata.rs b/crates/data-avalanche-source-mysql/tests/row_metadata.rs
index 0267072..af8f08a 100644
--- a/crates/data-avalanche-source-mysql/tests/row_metadata.rs
+++ b/crates/data-avalanche-source-mysql/tests/row_metadata.rs
@@ -47,7 +47,7 @@ async fn row_metadata_other_than_full_refuses_and_halts() {
     let c = connector(&pipeline(&n, &["t"], "", ""));
     c.preflight().await.expect("preflight under FULL");
     let tables = c.resolve_tables().await.unwrap().tables;
-    let from = c.position_now().await.unwrap();
+    let from = c.checkpoint_now().await.unwrap();
     let mut s = c.open_stream(&from, &tables).await.expect("stream");
     if let Err(e) = a
         .query_drop("SET GLOBAL binlog_row_metadata = MINIMAL")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib position::`

Expected: FAIL. The unit test doesn't compile: `identity` doesn't exist. The first errors:

```text
error[E0425]: cannot find function `identity` in this scope  --> crates/data-avalanche-source-mysql/src/position.rs:233:13
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
index f4d1f2c..f2ddee6 100644
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -329,10 +329,7 @@ impl SourceConnector for MySqlConnector {
 
     /// The server's write position now: the binlog keeps no position for the pipeline to lose.
     async fn reset_position(&self) -> Result<Checkpoint, SourceError> {
-        Ok(Checkpoint {
-            position: self.position_now().await?,
-            server: Some(self.server_identity().await?),
-        })
+        self.checkpoint_now().await
     }
 
     async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
@@ -370,7 +367,7 @@ impl SourceConnector for MySqlConnector {
         tables: &[TableSchema],
     ) -> Result<Box<dyn ChangeStream>, SourceError> {
         Ok(Box::new(
-            MySqlConnector::open_stream(self, &from.position, tables).await?,
+            MySqlConnector::open_stream(self, from, tables).await?,
         ))
     }
 
```

Change `crates/data-avalanche-source-mysql/src/position.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/position.rs b/crates/data-avalanche-source-mysql/src/position.rs
index 396a78a..c83bb88 100644
--- a/crates/data-avalanche-source-mysql/src/position.rs
+++ b/crates/data-avalanche-source-mysql/src/position.rs
@@ -4,12 +4,50 @@
 
 use data_avalanche_config::ReplicationType;
 use data_avalanche_core::position::{BinlogFilePos, GtidSet, MariaGtidState};
-use data_avalanche_core::{ServerIdentity, SourceError, SourcePosition};
+use data_avalanche_core::{Checkpoint, ServerIdentity, SourceError, SourcePosition};
 use mysql_async::prelude::Queryable;
 
 use crate::connector::MySqlConnector;
 use crate::server::Flavour;
 
+/// The statement that reads the server's identity (as text: the query protocol sends every
+/// value as text).
+pub(crate) fn identity_query(flavour: Flavour) -> &'static str {
+    match flavour {
+        Flavour::Mysql => "SELECT @@server_uuid",
+        Flavour::Mariadb => "SELECT @@server_id",
+    }
+}
+
+/// The server's identity from its answer to [`identity_query`]. No row, a NULL or an empty value
+/// is an error, not an empty identity: a checkpoint recorded with one could never be checked
+/// against a server.
+pub(crate) fn identity(
+    flavour: Flavour,
+    answer: Option<Option<String>>,
+) -> Result<ServerIdentity, SourceError> {
+    let var = match flavour {
+        Flavour::Mysql => "server_uuid",
+        Flavour::Mariadb => "server_id",
+    };
+    let value = answer.flatten().filter(|v| !v.is_empty()).ok_or_else(|| {
+        SourceError::Permanent(format!(
+            "the server reports no @@{var}: the stream position can't be recorded with the \
+             server it came from"
+        ))
+    })?;
+    Ok(match flavour {
+        Flavour::Mysql => ServerIdentity::Mysql { server_uuid: value },
+        Flavour::Mariadb => ServerIdentity::Mariadb {
+            server_id: value.parse().map_err(|_| {
+                SourceError::Permanent(format!(
+                    "the server's @@server_id is {value:?}, not a number"
+                ))
+            })?,
+        },
+    })
+}
+
 /// The dialect of a pipeline's positions: a MySQL GTID set, a MariaDB GTID state, or a binlog
 /// file/position (either server, `replication_type: binlog`).
 #[derive(Debug, Clone, Copy, PartialEq, Eq)]
@@ -129,26 +167,22 @@ impl MySqlConnector {
     /// (MariaDB has no server UUID, S-MY10a).
     pub async fn server_identity(&self) -> Result<ServerIdentity, SourceError> {
         let flavour = self.flavour().await?;
-        self.conn
+        let answer = self
+            .conn
             .command("read the server identity", move |c| {
-                Box::pin(async move {
-                    Ok(match flavour {
-                        Flavour::Mysql => ServerIdentity::Mysql {
-                            server_uuid: c
-                                .query_first("SELECT @@server_uuid")
-                                .await?
-                                .unwrap_or_default(),
-                        },
-                        Flavour::Mariadb => ServerIdentity::Mariadb {
-                            server_id: c
-                                .query_first("SELECT @@server_id")
-                                .await?
-                                .unwrap_or_default(),
-                        },
-                    })
-                })
+                Box::pin(async move { c.query_first(identity_query(flavour)).await })
             })
-            .await
+            .await?;
+        identity(flavour, answer)
+    }
+
+    /// The server's write position now, with the server: the checkpoint a start from now
+    /// records.
+    pub async fn checkpoint_now(&self) -> Result<Checkpoint, SourceError> {
+        Ok(Checkpoint {
+            position: self.position_now().await?,
+            server: Some(self.server_identity().await?),
+        })
     }
 
     /// `SHOW BINARY LOGS`, oldest first.
```

Change `crates/data-avalanche-source-mysql/src/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/stream.rs b/crates/data-avalanche-source-mysql/src/stream.rs
index 9a0965f..15ce895 100644
--- a/crates/data-avalanche-source-mysql/src/stream.rs
+++ b/crates/data-avalanche-source-mysql/src/stream.rs
@@ -37,8 +37,8 @@ use data_avalanche_core::position::{
     BinlogFilePos, GtidSet, GtidSource, MariaGtid, MariaGtidState,
 };
 use data_avalanche_core::{
-    CdcEvent, ChangeStream, HaltReason, Op, Row, SchemaHint, SourceError, SourcePosition,
-    SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary,
+    CdcEvent, ChangeStream, Checkpoint, HaltReason, Op, Row, SchemaHint, SourceError,
+    SourcePosition, SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary,
 };
 use futures_util::StreamExt;
 use mysql_async::binlog::events::{
@@ -55,9 +55,9 @@ use crate::catalog::table_key;
 use crate::connector::MySqlConnector;
 use crate::ddl::{Renamed, classify, renamed};
 use crate::error::mysql;
-use crate::position::Mode;
+use crate::position::{Mode, identity, identity_query};
 use crate::server::{Flavour, Fold};
-use crate::start::purge_action;
+use crate::start::{purge_action, same_server};
 use crate::types::{CatalogColumn, members, plan_column};
 use crate::value::{Charset, Decode, Input, decode_after, halt_reason, text_of};
 
@@ -679,12 +679,15 @@ impl MySqlConnector {
     /// Opens the binlog dump after `from` for `tables` as of that position (Data-Flow Phase 2b):
     /// the heartbeat period, the replica id `source.mysql.server_id`, and the position in its
     /// dialect. The server's name comparison and collations are read once, for the stream's
-    /// routing and table map checks.
+    /// routing and table map checks. In file/position mode the dump's own connection must reach
+    /// the server `from` was recorded on (`BINLOG_SOURCE_CHANGED`, Q-M7-2): a reconnect after a
+    /// failover reaches another server, whose files the position means nothing in.
     pub async fn open_stream(
         &self,
-        from: &SourcePosition,
+        from: &Checkpoint,
         tables: &[TableSchema],
     ) -> Result<MySqlStream, SourceError> {
+        let (recorded, from) = (from, &from.position);
         let flavour = self.flavour().await?;
         let mode = Mode::of(flavour, self.mysql.replication_type);
         let catalog = self.sibling()?;
@@ -731,6 +734,11 @@ impl MySqlConnector {
                 Offsets::new("", 4)
             }
             (Mode::File, SourcePosition::BinlogFile(p)) => {
+                let answer = conn
+                    .query_first(identity_query(flavour))
+                    .await
+                    .map_err(err)?;
+                same_server(recorded, &identity(flavour, answer)?)?;
                 file = p.file().as_bytes().to_vec();
                 req = req.with_filename(&file[..]).with_pos(p.pos());
                 Offsets::new(p.file(), p.pos())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib position::`

Expected: PASS, 7 tests.

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_MYSQL=mysql-84 cargo test -p data-avalanche-source-mysql --test mysql start::`

Expected: PASS, 7 tests.

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_MYSQL=mysql-84-nogtid cargo test -p data-avalanche-source-mysql --test mysql start::`

Expected: PASS, 7 tests.

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_MYSQL=mariadb-118-binlog cargo test -p data-avalanche-source-mysql --test mysql start::`

Expected: PASS, 7 tests.

Run: `cargo test --workspace`

Expected: PASS, every test binary.

Measured: with the server check disabled (the `same_server` line replaced), `a_file_position_stream_opens_only_on_its_server` fails on `mysql-84`. The open succeeds: `Checkpoint { position: BinlogFile(… mysql-bin.000016 …), server: Some(Mysql { server_uuid: "00000000-0000-0000-0000-000000000001" }) }: None`. The whole `tests/mysql` binary and the four solo binaries pass on `mysql-84-nogtid`, `mariadb-118`, `mariadb-114` and `mariadb-1011`. On `mysql-84` the three TLS tests need `mysql-tls`'s material in `test-infra/.tls/` (git-ignored, made by the container); the rest pass.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/src/connector.rs \
  crates/data-avalanche-source-mysql/src/position.rs \
  crates/data-avalanche-source-mysql/src/stream.rs \
  crates/data-avalanche-source-mysql/tests/binlog_rotate.rs \
  crates/data-avalanche-source-mysql/tests/mariadb_compress.rs \
  crates/data-avalanche-source-mysql/tests/mysql/roundtrip.rs \
  crates/data-avalanche-source-mysql/tests/mysql/start.rs \
  crates/data-avalanche-source-mysql/tests/mysql/stream.rs \
  crates/data-avalanche-source-mysql/tests/row_metadata.rs
git commit -m "fix(source-mysql): a file/position dump checks its server; no identity is an error; the file/position resume IT"
```

---

### Task 5: The binary builds the MySQL source (`data-avalanche`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `crates/data-avalanche/Cargo.toml`
- Modify: `crates/data-avalanche/src/wiring.rs`

**Interfaces:**
- Consumes:
  - `MySqlConnector::new(&Pipeline) -> Result<MySqlConnector, SourceError>` (M7a).
  - `WorkerFactory::build`, `Parts { source: Arc<dyn SourceConnector>, … }` (cluster, M6b).
- Produces:
  - `Factory::build` picks the source by `source.type`.

D-M7b.10. The factory built `PgConnector` for every pipeline, and a `type: mysql` pipeline went to `ERROR` with "the source isn't PostgreSQL". It now matches `source.type`. The binary gains the MySQL crate and core (for the trait object) as dependencies; core moves out of the dev-dependencies. One rustls provider is already installed at start (`main.rs`, DA-DEP-5).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche/src/wiring.rs`:

```diff
diff --git a/crates/data-avalanche/src/wiring.rs b/crates/data-avalanche/src/wiring.rs
index fd0940e..3399af8 100644
--- a/crates/data-avalanche/src/wiring.rs
+++ b/crates/data-avalanche/src/wiring.rs
@@ -119,6 +125,42 @@ mod tests {
         assert_eq!(d.merge_memory_limit, Some(64 << 20));
     }
 
+    /// A `type: mysql` pipeline gets the MySQL source (the PostgreSQL connector refuses it).
+    #[test]
+    fn a_mysql_pipeline_gets_its_parts() {
+        let yaml = r#"
+config_version: 1
+globals: { state: { bucket: app-state } }
+pipelines:
+  shop:
+    id: shop
+    source:
+      type: mysql
+      host: db.example.com
+      db: shop
+      user: cdc
+      password: pw
+      mysql: { replication_type: gtid }
+    tables:
+      include: [orders]
+    sink: { type: delta, bucket: lake, delta: {} }
+"#;
+        let c = data_avalanche_config::load_str(
+            yaml,
+            &std::collections::BTreeMap::new(),
+            &data_avalanche_config::FingerprintKey::from_bytes([1; 32]),
+        )
+        .unwrap_or_else(|r| panic!("{r:?}"))
+        .config;
+        let factory = Factory::new(&c.globals).unwrap();
+        factory
+            .build(&c.pipelines["shop"])
+            .expect("the MySQL parts");
+        factory
+            .build(&config().pipelines["orders"])
+            .expect("the PostgreSQL parts");
+    }
+
     #[test]
     fn an_iceberg_pipeline_gets_no_parts() {
         let c = config();
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche --lib wiring::`

Expected: FAIL. The new test fails: the factory hands a MySQL pipeline to the PostgreSQL connector, which refuses it. The failing tests:

```text
test wiring::tests::a_mysql_pipeline_gets_its_parts ... FAILED
```

- [ ] **Step 3: Implement**

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 2ec9526..a8362f1 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1688,6 +1688,7 @@ dependencies = [
  "data-avalanche-core",
  "data-avalanche-pipeline",
  "data-avalanche-sink-delta",
+ "data-avalanche-source-mysql",
  "data-avalanche-source-pg",
  "data-avalanche-state",
  "data-avalanche-storage",
```

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
index bf08abf..f5edb57 100644
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -15,8 +15,10 @@ chrono.workspace = true
 clap.workspace = true
 data-avalanche-cluster.workspace = true
 data-avalanche-config.workspace = true
+data-avalanche-core.workspace = true
 data-avalanche-pipeline.workspace = true
 data-avalanche-sink-delta.workspace = true
+data-avalanche-source-mysql.workspace = true
 data-avalanche-source-pg.workspace = true
 data-avalanche-state.workspace = true
 data-avalanche-storage.workspace = true
@@ -35,7 +37,6 @@ e2e-hooks = []
 [dev-dependencies]
 arrow-array.workspace = true
 arrow-cast.workspace = true
-data-avalanche-core.workspace = true
 data-avalanche-testkit.workspace = true
 futures-util.workspace = true
 serde_json.workspace = true
```

Change `crates/data-avalanche/src/wiring.rs`:

```diff
diff --git a/crates/data-avalanche/src/wiring.rs b/crates/data-avalanche/src/wiring.rs
index fd0940e..3399af8 100644
--- a/crates/data-avalanche/src/wiring.rs
+++ b/crates/data-avalanche/src/wiring.rs
@@ -3,9 +3,11 @@
 use std::sync::Arc;
 
 use data_avalanche_cluster::{Parts, WorkerFactory};
-use data_avalanche_config::{Globals, Pipeline, SinkType, TimestampNtzAs};
+use data_avalanche_config::{Globals, Pipeline, SinkType, SourceType, TimestampNtzAs};
+use data_avalanche_core::SourceConnector;
 use data_avalanche_pipeline::{Hooks, NoHooks};
 use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig, NtzAs, TableDefaults};
+use data_avalanche_source_mysql::MySqlConnector;
 use data_avalanche_source_pg::PgConnector;
 
 use crate::startup::backend;
@@ -42,7 +44,8 @@ pub fn delta_config(globals: &Globals, p: &Pipeline) -> Result<DeltaSinkConfig,
     })
 }
 
-/// Builds the PostgreSQL source and the Delta sink.
+/// Builds the pipeline's source (`source.type`: PostgreSQL, or MySQL / MariaDB) and the Delta
+/// sink.
 #[derive(Debug)]
 pub struct Factory {
     globals: Globals,
@@ -79,10 +82,13 @@ impl WorkerFactory for Factory {
         if p.sink.kind == SinkType::Iceberg {
             return Err("the Iceberg sink ships in v2 (D-V1.4); other pipelines run".into());
         }
-        let source = PgConnector::new(p).map_err(|e| e.to_string())?;
+        let source: Arc<dyn SourceConnector> = match p.source.kind {
+            SourceType::Postgres => Arc::new(PgConnector::new(p).map_err(|e| e.to_string())?),
+            SourceType::Mysql => Arc::new(MySqlConnector::new(p).map_err(|e| e.to_string())?),
+        };
         let sink = DeltaSink::new(delta_config(&self.globals, p)?);
         Ok(Parts {
-            source: Arc::new(source),
+            source,
             sink: Arc::new(sink),
             hooks: Arc::clone(&self.hooks),
         })
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche --lib wiring::`

Expected: PASS, 3 tests.

Run: `cargo test --workspace`

Expected: PASS, every test binary.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche/Cargo.toml \
  crates/data-avalanche/src/wiring.rs
git commit -m "feat(data-avalanche): the binary builds the MySQL source for a type: mysql pipeline"
```

---

### Task 6: One E2E suite, two sources (`tests/e2e`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `crates/data-avalanche/Cargo.toml`
- Modify (test): `crates/data-avalanche/tests/e2e/crashes.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/guards.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/harness.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/lifecycle.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/main.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/schema.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/smoke.rs`
- Create (test): `crates/data-avalanche/tests/e2e/source.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/streaming.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/toast.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/types.rs`

**Interfaces:**
- Consumes:
  - Tasks 1–5 (the binary streams a MySQL pipeline).
  - `mysql_async` (workspace pin) for the admin connection.
- Produces:
  - `tests/e2e/source.rs`: `Source { Postgres { port }, Mysql(Target) }` with `from_env`, `is_postgres`, `container`; `Target { name, port, mariadb, replication, service }`, `target(name)`; `Db { Pg, Mysql }` with `connect`, `exec`, `text`, `any`, `pg`; `mysql(port)`; the macros `postgres_only!()` and `mysql_only!()` (the latter returns the target).
  - `harness.rs`: `E2e { test, schema, source, db, … }`; `Options.mysql`, `Options.port`; `Doc { Row, Pad(u32) }`; `E2e::{source_block, insert_series, series, stream_open, streaming, marker -> SourcePosition, acknowledges(&SourcePosition), acknowledged, source_db, status_of, status, labels}`; `@SCHEMA@.`, `@INCLUDE@` and `@SOURCE(<suffix>)@` in the options' YAML.

Q-M7b-1, D-M7b.11…13. The harness was PostgreSQL's: its admin client, its schema, slot and publication, its slot queries. It now builds the world on the source `DATA_AVALANCHE_E2E_SOURCE` names.

- **PostgreSQL**: as before.
- **MySQL**: a database and a user `e2e_<test>`, the pipeline reading the database as that user (`replication_type` from the target). "Streaming" is that user's binlog dump in `PROCESSLIST`. "Acknowledged" is the position document covering the position.

The scenarios change only where the dialects differ:
- `@SCHEMA@.` in a table pattern.
- `insert_series` for `generate_series`.
- The upsert of `crash_kill`'s load (`ON DUPLICATE KEY UPDATE`).
- `MODIFY` for `ALTER COLUMN … TYPE`.
- `BEGIN … COMMIT` where a PostgreSQL batch was one implicit transaction.
- The stop scenario's 1 s MySQL heartbeat.
- The frozen server's container.
- The Iceberg scenario's second pipeline from `@SOURCE(_ice)@`.

The PostgreSQL-only scenarios skip themselves on MySQL: TOAST, the WAL guard, a lost slot, the PostgreSQL types. The buffer-ceiling scenario gets its own DDL, since `bigserial` and the `pad` table are the WAL guards'.

- [ ] **Step 1: Write the change**

Change `crates/data-avalanche/tests/e2e/crashes.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/crashes.rs b/crates/data-avalanche/tests/e2e/crashes.rs
index aaa2e86..e35736d 100644
--- a/crates/data-avalanche/tests/e2e/crashes.rs
+++ b/crates/data-avalanche/tests/e2e/crashes.rs
@@ -7,7 +7,7 @@ use std::time::Duration;
 
 use data_avalanche_testkit::require_it;
 
-use crate::harness::{E2e, Options, admin};
+use crate::harness::{Db, Doc, E2e, Options, Source};
 
 const COLS: &[&str] = &["id", "doc"];
 const CRASH: &str = "DATA_AVALANCHE_E2E_CRASH";
@@ -19,8 +19,7 @@ async fn a_crash_mid_snapshot_resumes_it() {
     require_it!();
     let e = E2e::new(
         "crash_chunk",
-        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
-         INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, 5000) g;",
+        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
         Options {
             tables: ", snapshots: { mode: initial, chunk_size: 500 }",
             // One chunk ahead of the writer at most: the chunks before it are committed.
@@ -29,6 +28,7 @@ async fn a_crash_mid_snapshot_resumes_it() {
         },
     )
     .await;
+    e.insert_series("items", 5000, Doc::Row).await;
     let mut p = e.start(&[(CRASH, "chunk_emitted:6")]);
     let status = p.exit().await;
     assert!(!status.success(), "aborted at the crash point: {status}");
@@ -76,10 +76,11 @@ async fn a_crash_between_a_commit_and_its_checkpoint_skips_the_replay() {
     let mut p = e.start(&[(CRASH, "sink_committed:1")]);
     p.ready().await;
     e.streaming().await;
-    e.exec(
-        "INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, 100) g;
-         UPDATE items SET doc = 'changed' WHERE id % 3 = 0",
-    )
+    // One transaction: MySQL commits each statement of a batch on its own.
+    e.exec(&format!(
+        "BEGIN; {}; UPDATE items SET doc = 'changed' WHERE id % 3 = 0; COMMIT",
+        e.series("items", 100, Doc::Row)
+    ))
     .await;
     let status = p.exit().await;
     assert!(!status.success(), "aborted at the crash point: {status}");
@@ -149,7 +150,8 @@ async fn a_crash_mid_multi_table_flush_completes_it_on_restart() {
 }
 
 /// A crash after the checkpoint, before the source is told: the source sends the transaction
-/// again and it is skipped.
+/// again and it is skipped. MySQL's binlog keeps no position for the pipeline: its checkpoint is
+/// the acknowledgment, so the restart reads on after the transaction and nothing is resent.
 #[tokio::test]
 async fn a_crash_between_the_checkpoint_and_the_ack_skips_the_resent_transaction() {
     require_it!();
@@ -175,10 +177,12 @@ async fn a_crash_between_the_checkpoint_and_the_ack_skips_the_resent_transaction
     assert!(!status.success(), "aborted at the crash point: {status}");
     e.mirrored("items", COLS).await;
     let marker = e.marker("items").await;
-    assert!(
-        !e.acknowledges(&marker).await,
-        "the crash came before the ack"
-    );
+    if e.source.is_postgres() {
+        assert!(
+            !e.acknowledges(&marker).await,
+            "the crash came before the ack"
+        );
+    }
     let commits = e.commits("items").await;
 
     let mut p = e.start(&[]);
@@ -200,16 +204,16 @@ async fn a_kill_under_load_loses_nothing() {
     require_it!();
     let e = E2e::new(
         "crash_kill",
-        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
-         INSERT INTO items SELECT g, 'row ' || g FROM generate_series(1, 200) g;",
+        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
         Options {
             tables: ", snapshots: { mode: initial }",
             ..Options::default()
         },
     )
     .await;
+    e.insert_series("items", 200, Doc::Row).await;
     let stop = Arc::new(AtomicBool::new(false));
-    let writer = tokio::spawn(write_load(e.schema.clone(), Arc::clone(&stop)));
+    let writer = tokio::spawn(write_load(e.source, e.schema.clone(), Arc::clone(&stop)));
     let mut p = e.start(&[]);
     p.ready().await;
     e.completed("items", None).await;
@@ -229,25 +233,27 @@ async fn a_kill_under_load_loses_nothing() {
     e.cleanup().await;
 }
 
-/// Updates, deletes and inserts on keys 1–300 until `stop`, one statement per transaction;
-/// returns how many ran.
-async fn write_load(schema: String, stop: Arc<AtomicBool>) -> u64 {
-    let c = admin().await;
+/// Updates, deletes and inserts on keys 1–300 until `stop`, one statement per transaction, on a
+/// connection of its own; returns how many ran.
+async fn write_load(source: Source, schema: String, stop: Arc<AtomicBool>) -> u64 {
+    let c = Db::connect(source).await;
     let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
     let mut ops = 0;
     while !stop.load(Ordering::SeqCst) {
         x ^= x << 13;
         x ^= x >> 7;
         x ^= x << 17;
-        let id = (x % 300 + 1) as i64;
+        let id = x % 300 + 1;
+        let upsert = match source {
+            Source::Postgres { .. } => "ON CONFLICT (id) DO UPDATE SET doc = excluded.doc",
+            Source::Mysql(_) => "ON DUPLICATE KEY UPDATE doc = VALUES(doc)",
+        };
         let sql = match x % 3 {
-            0 => format!("UPDATE {schema}.items SET doc = 'u{ops}' WHERE id = $1"),
-            1 => format!("DELETE FROM {schema}.items WHERE id = $1"),
-            _ => format!(
-                "INSERT INTO {schema}.items VALUES ($1, 'i{ops}') ON CONFLICT (id) DO UPDATE SET doc = excluded.doc"
-            ),
+            0 => format!("UPDATE {schema}.items SET doc = 'u{ops}' WHERE id = {id}"),
+            1 => format!("DELETE FROM {schema}.items WHERE id = {id}"),
+            _ => format!("INSERT INTO {schema}.items VALUES ({id}, 'i{ops}') {upsert}"),
         };
-        c.execute(&sql, &[&id]).await.expect("a load statement");
+        c.exec(&sql).await.expect("a load statement");
         ops += 1;
         tokio::time::sleep(Duration::from_millis(5)).await;
     }
```

Change `crates/data-avalanche/tests/e2e/guards.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/guards.rs b/crates/data-avalanche/tests/e2e/guards.rs
index 5fce3c5..945298c 100644
--- a/crates/data-avalanche/tests/e2e/guards.rs
+++ b/crates/data-avalanche/tests/e2e/guards.rs
@@ -1,12 +1,12 @@
 //! Scenario 10, the guards: the WAL a slot retains, the open transaction's buffer, a slot whose
-//! WAL is gone, and a source that stops answering.
+//! WAL is gone (PostgreSQL), and a source that stops answering.
 
 use std::process::{Command, Stdio};
 use std::time::Duration;
 
 use data_avalanche_testkit::require_it;
 
-use crate::harness::{E2e, Options, admin, eventually, pg_port};
+use crate::harness::{Db, Doc, E2e, Options, eventually};
 
 const TABLES: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
      CREATE TABLE pad (id bigserial PRIMARY KEY, doc text);";
@@ -14,7 +14,7 @@ const TABLES: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
 /// Writes WAL and switches to a new segment, `n` times: each moves the WAL end by up to 16 MiB.
 async fn switch_wal(e: &E2e, n: usize) -> Result<(), tokio_postgres::Error> {
     for _ in 0..n {
-        e.admin
+        e.db.pg()
             .batch_execute(&format!(
                 "INSERT INTO {}.pad (doc) VALUES ('x'); SELECT pg_switch_wal()",
                 e.schema
@@ -30,11 +30,12 @@ async fn switch_wal(e: &E2e, n: usize) -> Result<(), tokio_postgres::Error> {
 #[tokio::test]
 async fn retained_wal_past_the_limit_stops_the_pipeline() {
     require_it!();
+    crate::postgres_only!();
     let e = E2e::new(
         "guard_wal",
         TABLES,
         Options {
-            tables: ", exclude: [e2e_guard_wal.pad], snapshots: { mode: never }",
+            tables: ", exclude: ['@SCHEMA@.pad'], snapshots: { mode: never }",
             postgres: ", max_wal_lag_bytes: 64MiB, heartbeat: 5s",
             ..Options::default()
         },
@@ -43,20 +44,17 @@ async fn retained_wal_past_the_limit_stops_the_pipeline() {
     let mut p = e.start(&[]);
     p.ready().await;
     e.streaming().await;
-    let held = admin().await;
-    held.batch_execute(&format!(
+    let held = Db::connect(e.source).await;
+    held.exec(&format!(
         "BEGIN; INSERT INTO {}.items VALUES (1, 'held')",
         e.schema
     ))
     .await
     .expect("an open transaction");
     switch_wal(&e, 6).await.expect("WAL written");
-    e.admin
-        .batch_execute("CHECKPOINT")
-        .await
-        .expect("a checkpoint");
+    e.db.exec("CHECKPOINT").await.expect("a checkpoint");
     p.metric(&e.status("error")).await;
-    held.batch_execute("ROLLBACK").await.expect("rolled back");
+    held.exec("ROLLBACK").await.expect("rolled back");
     assert!(
         p.log().contains("over max_wal_lag_bytes 67108864"),
         "{}",
@@ -73,7 +71,7 @@ async fn a_transaction_past_the_buffer_ceiling_stops_the_pipeline() {
     require_it!();
     let e = E2e::new(
         "guard_buffer",
-        TABLES,
+        "CREATE TABLE items (id bigint PRIMARY KEY, doc text);",
         Options {
             tables: ", snapshots: { mode: never }",
             sink: ", max_transaction_buffer_bytes: 16MiB",
@@ -84,8 +82,7 @@ async fn a_transaction_past_the_buffer_ceiling_stops_the_pipeline() {
     let mut p = e.start(&[]);
     p.ready().await;
     e.streaming().await;
-    e.exec("INSERT INTO items SELECT g, repeat('x', 300) FROM generate_series(1, 100000) g")
-        .await;
+    e.insert_series("items", 100_000, Doc::Pad(300)).await;
     p.metric(&e.status("error")).await;
     assert!(
         p.log()
@@ -103,11 +100,12 @@ async fn a_transaction_past_the_buffer_ceiling_stops_the_pipeline() {
 #[tokio::test]
 async fn a_slot_whose_wal_is_gone_is_snapshotted_again() {
     require_it!();
+    crate::postgres_only!();
     let e = E2e::new(
         "guard_lost",
         &format!("{TABLES} INSERT INTO items VALUES (1, 'a'), (2, 'b');"),
         Options {
-            tables: ", exclude: [e2e_guard_lost.pad], snapshots: { mode: initial }",
+            tables: ", exclude: ['@SCHEMA@.pad'], snapshots: { mode: initial }",
             ..Options::default()
         },
     )
@@ -127,15 +125,15 @@ async fn a_slot_whose_wal_is_gone_is_snapshotted_again() {
             None => "ALTER SYSTEM RESET max_slot_wal_keep_size".to_owned(),
             Some(s) => format!("ALTER SYSTEM SET max_slot_wal_keep_size = '{s}'"),
         };
-        e.admin.batch_execute(&set).await?;
-        e.admin.batch_execute("SELECT pg_reload_conf()").await
+        e.db.pg().batch_execute(&set).await?;
+        e.db.pg().batch_execute("SELECT pg_reload_conf()").await
     };
     keep(None).await.expect("the default");
     let invalidate = async {
         keep(Some("1MB")).await?;
         switch_wal(&e, 3).await?;
-        e.admin.batch_execute("CHECKPOINT").await?;
-        e.admin
+        e.db.pg().batch_execute("CHECKPOINT").await?;
+        e.db.pg()
             .query_one(
                 "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
                 &[&e.schema],
@@ -165,9 +163,8 @@ async fn a_slot_whose_wal_is_gone_is_snapshotted_again() {
 struct Frozen(String);
 
 impl Frozen {
-    fn new() -> Self {
-        // test-infra/compose.yaml: project `data-avalanche-test`, `postgres-<major>` on 554<major>.
-        let name = format!("data-avalanche-test-postgres-{}-1", pg_port() - 55400);
+    fn new(e: &E2e) -> Self {
+        let name = e.source.container();
         docker("pause", &name);
         Self(name)
     }
@@ -213,6 +210,7 @@ async fn a_source_that_stops_answering_is_reconnected_to() {
             tables: ", snapshots: { mode: never }",
             source: "timeouts: { connect: 2s, command: 2s }",
             postgres: ", heartbeat: 1s",
+            mysql: ", heartbeat: 1s",
             ..Options::default()
         },
     )
@@ -224,7 +222,7 @@ async fn a_source_that_stops_answering_is_reconnected_to() {
     e.mirrored("items", &["id", "doc"]).await;
     assert_eq!(reconnects(&p.metrics().await), 0);
     {
-        let _frozen = Frozen::new();
+        let _frozen = Frozen::new(&e);
         eventually("a reconnect", Duration::from_secs(60), async || {
             reconnects(&p.metrics().await) >= 1
         })
```

Change `crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index 7a5bd50..8b85030 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -1,5 +1,7 @@
-//! One E2E test's world: a schema `e2e_<test>` with its slot and publication, a RustFS prefix for
-//! `globals.state` and the lake, a config file, and the binary as a child process.
+//! One E2E test's world on the source under test ([`Source::from_env`]): PostgreSQL's schema
+//! `e2e_<test>` with its slot and publication, or MySQL's database `e2e_<test>` with a user of
+//! that name; a RustFS prefix for `globals.state` and the lake, a config file, and the binary as
+//! a child process.
 
 use std::path::PathBuf;
 use std::process::{Child, Command, ExitStatus, Stdio};
@@ -9,7 +11,7 @@ use arrow_array::cast::AsArray;
 use arrow_array::types::Int64Type;
 use arrow_array::{Array, RecordBatch};
 use arrow_cast::display::{ArrayFormatter, FormatOptions};
-use data_avalanche_core::{Sink, TableRef};
+use data_avalanche_core::{Sink, SourcePosition, TableRef};
 use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig};
 use data_avalanche_state::{
     DocStore, HaltsDoc, LeaseDoc, Leases, PipelineDocs, TableSnapshotDoc, Versioned,
@@ -21,27 +23,8 @@ use data_avalanche_testkit::endpoints::{
 use data_avalanche_testkit::{delete_prefix, stores, unique_prefix};
 use futures_util::TryStreamExt;
 use tokio::io::{AsyncReadExt, AsyncWriteExt};
-use tokio_postgres::{Client, NoTls};
-
-/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418).
-pub fn pg_port() -> u16 {
-    std::env::var("DATA_AVALANCHE_PG_PORT")
-        .ok()
-        .and_then(|p| p.parse().ok())
-        .unwrap_or(55418)
-}
 
-pub async fn admin() -> Client {
-    let dsn = format!(
-        "host=127.0.0.1 port={} user=avalanche password=avalanche dbname=avalanche",
-        pg_port()
-    );
-    let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
-        .await
-        .expect("connect to test PostgreSQL");
-    tokio::spawn(conn);
-    client
-}
+pub use crate::source::{Db, Source};
 
 /// Polls `cond` every 100 ms until it holds, for at most `within`.
 pub async fn eventually<F, Fut>(what: &str, within: Duration, mut cond: F)
@@ -66,70 +49,110 @@ fn free_port() -> u16 {
 /// What a test sets beyond the defaults (YAML flow entries, each starting with `, `).
 #[derive(Debug, Clone, Default)]
 pub struct Options<'a> {
-    /// `tables` entries, e.g. `, snapshots: { mode: never }`.
+    /// `tables` entries, e.g. `, snapshots: { mode: never }`. `@SCHEMA@.` qualifies a table
+    /// name as the source's patterns do: `e2e_<test>.` on PostgreSQL, nothing on MySQL (V-064).
     pub tables: &'a str,
     /// `sink` entries, e.g. `, flush: { rows: 10 }`.
     pub sink: &'a str,
     /// `source` keys beyond the connection, one YAML line, e.g. `timeouts: { command: 2s }`.
     pub source: &'a str,
-    /// `source.postgres` entries.
+    /// `source.postgres` entries, on PostgreSQL.
     pub postgres: &'a str,
+    /// `source.mysql` entries, on MySQL.
+    pub mysql: &'a str,
+    /// The port the pipeline connects to, when not the server's (a proxy in front of it).
+    pub port: Option<u16>,
     /// Pipeline keys beyond `source`, `tables` and `sink`, one YAML line, e.g.
     /// `schema_evolution: { rescan_interval: 2s }`.
     pub pipeline: &'a str,
-    /// More pipelines, YAML under `pipelines:` (2-space indent); `@SCHEMA@`, `@PREFIX@`, `@PG@`
-    /// and `@BUCKET@` are replaced.
+    /// More pipelines, YAML under `pipelines:` (2-space indent); `@SCHEMA@`, `@PREFIX@` and
+    /// `@BUCKET@` are replaced, `@INCLUDE@` by the pattern of every table of the test, and a line
+    /// `@SOURCE(<suffix>)@` by the source block ([`E2e::source_block`]).
     pub pipelines: &'a str,
 }
 
+/// The rows [`E2e::insert_series`] gives a key `g`.
+#[derive(Debug, Clone, Copy)]
+pub enum Doc {
+    /// `'row <g>'`.
+    Row,
+    /// `n` times `x`.
+    Pad(u32),
+}
+
 pub struct E2e {
     pub test: String,
+    /// PostgreSQL's schema, MySQL's database: `e2e_<test>`.
     pub schema: String,
+    pub source: Source,
     prefix: String,
     /// The first node's (`n1`) API port.
     port: u16,
     dir: tempfile::TempDir,
     /// The config with `@NODE@` and `@PORT@` to fill per node.
     template: String,
-    pub admin: Client,
+    pub db: Db,
 }
 
 impl E2e {
-    /// Resets the server-wide `max_slot_wal_keep_size`, drops what an earlier run of `test` left
-    /// in PostgreSQL, creates schema `e2e_<test>` and runs `ddl` in it, and writes the config.
+    /// Drops what an earlier run of `test` left in the source, creates its schema (PostgreSQL)
+    /// or database and user (MySQL), runs `ddl` in it, and writes the config. On PostgreSQL it
+    /// resets the server-wide `max_slot_wal_keep_size` first.
     pub async fn new(test: &str, ddl: &str, o: Options<'_>) -> Self {
-        let admin = admin().await;
-        // A scenario sets it (guards.rs) and restores it, but a run killed in between leaves it
-        // set for the next. ALTER SYSTEM runs outside a transaction: one per call.
-        admin
-            .batch_execute("ALTER SYSTEM RESET max_slot_wal_keep_size")
-            .await
-            .expect("max_slot_wal_keep_size reset");
-        admin
-            .batch_execute("SELECT pg_reload_conf()")
-            .await
-            .expect("the configuration reloaded");
+        let source = Source::from_env();
+        let db = Db::connect(source).await;
         let schema = format!("e2e_{test}");
-        drop_slot(&admin, &schema).await;
-        admin
-            .batch_execute(&format!(
-                "DROP PUBLICATION IF EXISTS {schema}_pub;
-                 DROP SCHEMA IF EXISTS {schema} CASCADE;
-                 CREATE SCHEMA {schema};
-                 SET search_path = {schema};
-                 {ddl}
-                 RESET search_path;"
-            ))
-            .await
-            .expect("test DDL");
+        match source {
+            Source::Postgres { .. } => {
+                // A scenario sets it (guards.rs) and restores it, but a run killed in between
+                // leaves it set for the next. ALTER SYSTEM runs outside a transaction: one per
+                // call.
+                for sql in [
+                    "ALTER SYSTEM RESET max_slot_wal_keep_size",
+                    "SELECT pg_reload_conf()",
+                ] {
+                    db.exec(sql).await.expect(sql);
+                }
+                drop_slot(&db, &schema).await;
+                db.exec(&format!(
+                    "DROP PUBLICATION IF EXISTS {schema}_pub;
+                     DROP SCHEMA IF EXISTS {schema} CASCADE;
+                     CREATE SCHEMA {schema};
+                     SET search_path = {schema};
+                     {ddl}
+                     RESET search_path;"
+                ))
+                .await
+                .expect("test DDL");
+            }
+            Source::Mysql(_) => {
+                // The pipeline's own user: its binlog dump is the one in the process list with
+                // that user ([`Self::stream_open`]).
+                db.exec(&format!(
+                    "DROP DATABASE IF EXISTS {schema};
+                     CREATE DATABASE {schema};
+                     DROP USER IF EXISTS '{schema}'@'%';
+                     CREATE USER '{schema}'@'%' IDENTIFIED BY 'avalanche';
+                     GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '{schema}'@'%';"
+                ))
+                .await
+                .expect("the test database");
+                if !ddl.is_empty() {
+                    db.exec(&format!("USE {schema}; {ddl}"))
+                        .await
+                        .expect("test DDL");
+                }
+            }
+        }
         let mut e = Self {
             test: test.to_owned(),
             schema,
+            source,
             prefix: unique_prefix(&format!("e2e-{test}")),
             port: free_port(),
             dir: tempfile::tempdir().expect("a temp dir"),
             template: String::new(),
-            admin,
+            db,
         };
         e.template = e.yaml(&o);
         e.write_config("n1", e.port);
@@ -150,12 +173,69 @@ impl E2e {
         path
     }
 
+    /// The `source` block of a pipeline named `test` (the slot and publication `e2e_<test>`
+    /// takes `suffix`), indented for a pipeline under `pipelines:`; `extra` is one more line of
+    /// `source` keys and `postgres` / `mysql` their flow entries.
+    pub fn source_block(
+        &self,
+        suffix: &str,
+        port: Option<u16>,
+        extra: &str,
+        postgres: &str,
+        mysql: &str,
+    ) -> String {
+        let s = &self.schema;
+        match self.source {
+            Source::Postgres { port: pg } => format!(
+                "    source:
+      host: 127.0.0.1
+      port: {port}
+      db: avalanche
+      user: avalanche
+      password: avalanche
+      ssl_mode: disable
+      {extra}
+      postgres: {{ publication: {s}{suffix}_pub, slot: {s}{suffix}{postgres} }}",
+                port = port.unwrap_or(pg),
+            ),
+            Source::Mysql(t) => format!(
+                "    source:
+      type: mysql
+      host: 127.0.0.1
+      port: {port}
+      db: {s}
+      user: {s}
+      password: avalanche
+      ssl_mode: disable
+      {extra}
+      mysql: {{ replication_type: {repl}{mysql} }}",
+                port = port.unwrap_or(t.port),
+                repl = t.replication,
+            ),
+        }
+    }
+
+    /// `@SCHEMA@.` as the source's table patterns qualify a name.
+    fn qualify(&self, text: &str) -> String {
+        let q = match self.source {
+            Source::Postgres { .. } => format!("{}.", self.schema),
+            Source::Mysql(_) => String::new(),
+        };
+        text.replace("@SCHEMA@.", &q)
+    }
+
     fn yaml(&self, o: &Options<'_>) -> String {
-        let more = o
-            .pipelines
+        let include = self.qualify("@SCHEMA@.*");
+        let mut more = o.pipelines.to_owned();
+        while let Some(at) = more.find("@SOURCE(") {
+            let end = at + more[at..].find(")@").expect("@SOURCE(<suffix>)@");
+            let block = self.source_block(&more[at + 8..end], None, "", "", "");
+            more.replace_range(at..end + 2, &block);
+        }
+        let more = more
+            .replace("@INCLUDE@", &include)
             .replace("@SCHEMA@", &self.schema)
             .replace("@PREFIX@", &self.prefix)
-            .replace("@PG@", &pg_port().to_string())
             .replace("@BUCKET@", TEST_BUCKET);
         format!(
             r#"config_version: 1
@@ -172,16 +252,8 @@ globals:
 pipelines:
   {test}:
     id: {test}
-    source:
-      host: 127.0.0.1
-      port: {pg}
-      db: avalanche
-      user: avalanche
-      password: avalanche
-      ssl_mode: disable
-      {source}
-      postgres: {{ publication: {schema}_pub, slot: {schema}{postgres} }}
-    tables: {{ include: [{schema}.*]{tables} }}
+{source}
+    tables: {{ include: ['{include}']{tables} }}
     sink: {{ type: delta, bucket: {bucket}, prefix: {prefix}/lake, delta: {{}}, flush: {{ seconds: 1 }}{sink} }}
     {pipeline}
 {more}
@@ -189,11 +261,8 @@ pipelines:
             bucket = TEST_BUCKET,
             prefix = self.prefix,
             test = self.test,
-            pg = pg_port(),
-            schema = self.schema,
-            source = o.source,
-            postgres = o.postgres,
-            tables = o.tables,
+            source = self.source_block("", o.port, o.source, o.postgres, o.mysql),
+            tables = self.qualify(o.tables),
             sink = o.sink,
             pipeline = o.pipeline,
         )
@@ -296,15 +365,10 @@ pipelines:
 
     /// The source table's `id`s, in key order.
     pub async fn source_ids(&self, table: &str) -> Vec<i64> {
-        self.admin
-            .query(
-                &format!("SELECT id FROM {}.{table} ORDER BY id", self.schema),
-                &[],
-            )
+        self.source_rows(table, &["id"])
             .await
-            .expect("the source reads")
             .iter()
-            .map(|r| r.get::<_, i64>(0))
+            .map(|r| r[0].parse().expect("an id"))
             .collect()
     }
 
@@ -328,22 +392,24 @@ pipelines:
             .collect()
     }
 
-    /// The source's `cols` of `table` as text (`NULL` for a null), in key order.
+    /// The source's `cols` of `table` as the server prints them (`NULL` for a null), in key
+    /// order.
     pub async fn source_rows(&self, table: &str, cols: &[&str]) -> Vec<Vec<String>> {
-        let list = cols
-            .iter()
-            .map(|c| format!("coalesce({c}::text, 'NULL')"))
-            .collect::<Vec<_>>()
-            .join(", ");
-        self.admin
-            .query(
-                &format!("SELECT {list} FROM {}.{table} ORDER BY id", self.schema),
-                &[],
-            )
+        let sql = format!(
+            "SELECT {} FROM {}.{table} ORDER BY id",
+            cols.join(", "),
+            self.schema
+        );
+        self.db
+            .text(&sql)
             .await
             .expect("the source reads")
-            .iter()
-            .map(|r| (0..cols.len()).map(|i| r.get::<_, String>(i)).collect())
+            .into_iter()
+            .map(|r| {
+                r.into_iter()
+                    .map(|c| c.unwrap_or_else(|| "NULL".into()))
+                    .collect()
+            })
             .collect()
     }
 
@@ -367,23 +433,33 @@ pipelines:
         .await;
     }
 
-    /// Waits until the pipeline's slot streams to a walsender: from here on, a change is read.
+    /// Whether the source streams to the pipeline now: PostgreSQL's slot has a walsender,
+    /// MySQL runs a binlog dump for the pipeline's user.
+    pub async fn stream_open(&self) -> bool {
+        let sql = match self.source {
+            Source::Postgres { .. } => format!(
+                "SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}' AND active",
+                self.schema
+            ),
+            Source::Mysql(_) => format!(
+                "SELECT 1 FROM information_schema.PROCESSLIST
+                 WHERE USER = '{}' AND COMMAND LIKE 'Binlog Dump%'",
+                self.schema
+            ),
+        };
+        self.db.any(&sql).await
+    }
+
+    /// Waits until the source streams to the pipeline: from here on, a change is read.
     pub async fn streaming(&self) {
-        eventually("the slot streams", Duration::from_secs(60), async || {
-            self.admin
-                .query_opt(
-                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND active",
-                    &[&self.schema],
-                )
-                .await
-                .expect("slots read")
-                .is_some()
+        eventually("the source streams", Duration::from_secs(60), async || {
+            self.stream_open().await
         })
         .await;
     }
 
-    /// The position the table's latest commit marker names, as PostgreSQL text (`0/16B3748`).
-    pub async fn marker(&self, table: &str) -> String {
+    /// The position the table's latest commit marker names.
+    pub async fn marker(&self, table: &str) -> SourcePosition {
         self.lake()
             .table_state(&self.table(table))
             .await
@@ -391,42 +467,93 @@ pipelines:
             .marker
             .expect("a commit marker")
             .committed_position
-            .untagged()
     }
 
-    /// Whether the slot's acknowledged position has reached `lsn`.
-    pub async fn acknowledges(&self, lsn: &str) -> bool {
-        self.admin
-            .query_opt(
-                "SELECT 1 FROM pg_replication_slots
-                 WHERE slot_name = $1 AND confirmed_flush_lsn >= $2::text::pg_lsn",
-                &[&self.schema, &lsn],
-            )
-            .await
-            .expect("slots read")
-            .is_some()
+    /// Whether the source's acknowledged position has reached `at`: PostgreSQL's slot, MySQL's
+    /// checkpoint (the binlog keeps no position for the pipeline: the checkpoint is its ack).
+    pub async fn acknowledges(&self, at: &SourcePosition) -> bool {
+        match self.source {
+            Source::Postgres { .. } => {
+                self.db
+                    .any(&format!(
+                        "SELECT 1 FROM pg_replication_slots
+                         WHERE slot_name = '{}' AND confirmed_flush_lsn >= '{}'::pg_lsn",
+                        self.schema,
+                        at.untagged()
+                    ))
+                    .await
+            }
+            Source::Mysql(_) => self
+                .docs()
+                .position()
+                .await
+                .expect("the position reads")
+                .is_some_and(|v| v.doc.position.covers(at).expect("one dialect")),
+        }
     }
 
-    /// Waits until the slot's acknowledged position reaches `lsn`: everything before it is
+    /// Waits until the source's acknowledged position reaches `at`: everything before it is
     /// committed and checkpointed.
-    pub async fn acknowledged(&self, lsn: &str) {
-        let what = format!("the slot acknowledges {lsn}");
+    pub async fn acknowledged(&self, at: &SourcePosition) {
+        let what = format!("the source acknowledges {at}");
         eventually(&what, Duration::from_secs(60), async || {
-            self.acknowledges(lsn).await
+            self.acknowledges(at).await
         })
         .await;
     }
 
+    /// Runs `sql` in the test's schema or database.
     pub async fn exec(&self, sql: &str) {
-        self.admin
-            .batch_execute(&format!(
-                "SET search_path = {}; {sql}; RESET search_path;",
-                self.schema
-            ))
+        let wrapped = match self.source {
+            Source::Postgres { .. } => {
+                format!(
+                    "SET search_path = {}; {sql}; RESET search_path;",
+                    self.schema
+                )
+            }
+            Source::Mysql(_) => format!("USE {}; {sql}", self.schema),
+        };
+        self.db
+            .exec(&wrapped)
             .await
             .unwrap_or_else(|e| panic!("{sql}: {e}"));
     }
 
+    /// Inserts keys 1..=`n` into `table` (`id`, `doc`) in one statement: one transaction.
+    pub async fn insert_series(&self, table: &str, n: u64, doc: Doc) {
+        self.exec(&self.series(table, n, doc)).await;
+    }
+
+    /// The statement of [`Self::insert_series`] (MySQL: with the session setting before it).
+    pub fn series(&self, table: &str, n: u64, doc: Doc) -> String {
+        match (self.source, doc) {
+            (Source::Postgres { .. }, Doc::Row) => {
+                format!("INSERT INTO {table} SELECT g, 'row ' || g FROM generate_series(1, {n}) g")
+            }
+            (Source::Postgres { .. }, Doc::Pad(w)) => format!(
+                "INSERT INTO {table} SELECT g, repeat('x', {w}) FROM generate_series(1, {n}) g"
+            ),
+            (Source::Mysql(t), doc) => {
+                let doc = match doc {
+                    Doc::Row => "CONCAT('row ', g)".to_owned(),
+                    Doc::Pad(w) => format!("REPEAT('x', {w})"),
+                };
+                // The recursion's ceiling, 1000 by default on both: MySQL's
+                // `cte_max_recursion_depth`, MariaDB's `max_recursive_iterations`.
+                let var = if t.mariadb {
+                    "max_recursive_iterations"
+                } else {
+                    "cte_max_recursion_depth"
+                };
+                let depth = format!("SET SESSION {var} = {n}; ");
+                format!(
+                    "{depth}INSERT INTO {table} (id, doc) WITH RECURSIVE s (g) AS
+                     (SELECT 1 UNION ALL SELECT g + 1 FROM s WHERE g < {n}) SELECT g, {doc} FROM s"
+                )
+            }
+        }
+    }
+
     fn docs_at(&self, root: &str) -> DocStore {
         let store = ObjectStoreVersioned::new(stores::rustfs(), root).expect("a root");
         DocStore::new(std::sync::Arc::new(store))
@@ -540,30 +667,64 @@ pipelines:
         delete_prefix(stores::rustfs().as_ref(), &self.table_root(table)).await;
     }
 
+    /// The metrics' `source_db` label: PostgreSQL's database, MySQL's (the test's).
+    pub fn source_db(&self) -> &str {
+        match self.source {
+            Source::Postgres { .. } => "avalanche",
+            Source::Mysql(_) => &self.schema,
+        }
+    }
+
+    /// The `data_avalanche_pipeline_status` line of pipeline `pipeline` for `status`, set.
+    pub fn status_of(&self, pipeline: &str, status: &str) -> String {
+        format!(
+            r#"data_avalanche_pipeline_status{{pipeline="{pipeline}",source_host="127.0.0.1",source_db="{}",status="{status}"}} 1"#,
+            self.source_db()
+        )
+    }
+
     /// The `data_avalanche_pipeline_status` line for `status`, set.
     pub fn status(&self, status: &str) -> String {
+        self.status_of(&self.test, status)
+    }
+
+    /// The labels of the pipeline's series.
+    pub fn labels(&self) -> String {
         format!(
-            r#"data_avalanche_pipeline_status{{pipeline="{}",source_host="127.0.0.1",source_db="avalanche",status="{status}"}} 1"#,
-            self.test
+            r#"pipeline="{}",source_host="127.0.0.1",source_db="{}""#,
+            self.test,
+            self.source_db()
         )
     }
 
-    /// Drops the slot, the publication, the schema and the prefix.
+    /// Drops what the test made in the source, and the prefix.
     pub async fn cleanup(self) {
-        drop_slot(&self.admin, &self.schema).await;
-        self.admin
-            .batch_execute(&format!(
-                "DROP PUBLICATION IF EXISTS {s}_pub; DROP SCHEMA IF EXISTS {s} CASCADE;",
-                s = self.schema
-            ))
-            .await
-            .expect("cleanup");
+        let s = &self.schema;
+        match self.source {
+            Source::Postgres { .. } => {
+                drop_slot(&self.db, s).await;
+                self.db
+                    .exec(&format!(
+                        "DROP PUBLICATION IF EXISTS {s}_pub; DROP SCHEMA IF EXISTS {s} CASCADE;"
+                    ))
+                    .await
+                    .expect("cleanup");
+            }
+            Source::Mysql(_) => self
+                .db
+                .exec(&format!(
+                    "DROP DATABASE IF EXISTS {s}; DROP USER IF EXISTS '{s}'@'%';"
+                ))
+                .await
+                .expect("cleanup"),
+        }
         delete_prefix(stores::rustfs().as_ref(), &self.prefix).await;
     }
 }
 
 /// Drops the slot, waiting for a walsender still holding it to go.
-async fn drop_slot(admin: &Client, slot: &str) {
+async fn drop_slot(db: &Db, slot: &str) {
+    let admin = db.pg();
     for _ in 0..100 {
         let r = admin
             .execute(
```

Change `crates/data-avalanche/tests/e2e/lifecycle.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/lifecycle.rs b/crates/data-avalanche/tests/e2e/lifecycle.rs
index 90c379c..d89f316 100644
--- a/crates/data-avalanche/tests/e2e/lifecycle.rs
+++ b/crates/data-avalanche/tests/e2e/lifecycle.rs
@@ -17,24 +17,22 @@ fn never() -> Options<'static> {
     }
 }
 
-/// The slot has a walsender: the source connection is open.
-async fn slot_active(e: &E2e) -> bool {
-    e.admin
-        .query_one(
-            "SELECT coalesce(bool_or(active), false) FROM pg_replication_slots WHERE slot_name = $1",
-            &[&e.schema],
-        )
-        .await
-        .expect("slots read")
-        .get(0)
-}
-
 /// Pause holds the stream with the source connection open; stop closes it and is kept across a
 /// restart; resume reads on from where each left off.
 #[tokio::test]
 async fn pause_stop_and_resume_hold_across_a_restart() {
     require_it!();
-    let e = E2e::new("life_states", ITEMS, never()).await;
+    // MySQL's dump thread sees the stop's closed connection at its next write, a heartbeat at
+    // the latest: 1 s, not the default minute.
+    let e = E2e::new(
+        "life_states",
+        ITEMS,
+        Options {
+            mysql: ", heartbeat: 1s",
+            ..never()
+        },
+    )
+    .await;
     let mut p = e.start(&[]);
     p.ready().await;
     e.streaming().await;
@@ -50,7 +48,7 @@ async fn pause_stop_and_resume_hold_across_a_restart() {
     tokio::time::sleep(Duration::from_secs(3)).await;
     assert_eq!(e.ids("items").await, [1], "nothing is read while paused");
     assert!(
-        slot_active(&e).await,
+        e.stream_open().await,
         "a paused pipeline keeps the source warm"
     );
 
@@ -67,9 +65,9 @@ async fn pause_stop_and_resume_hold_across_a_restart() {
         .expect("stopped");
     p.metric(&e.status("stopped")).await;
     eventually(
-        "the slot is released",
+        "the stream is closed",
         Duration::from_secs(30),
-        async || !slot_active(&e).await,
+        async || !e.stream_open().await,
     )
     .await;
     assert!(p.terminate().await.success());
@@ -84,7 +82,7 @@ async fn pause_stop_and_resume_hold_across_a_restart() {
         [1, 2],
         "still stopped after a restart"
     );
-    assert!(!slot_active(&e).await);
+    assert!(!e.stream_open().await);
 
     e.docs()
         .set_desired_state(DesiredState::Running)
@@ -145,15 +143,8 @@ async fn an_iceberg_pipeline_is_refused_beside_a_running_one() {
             tables: ", snapshots: { mode: never }",
             pipelines: r#"  ice:
     id: ice
-    source:
-      host: 127.0.0.1
-      port: @PG@
-      db: avalanche
-      user: avalanche
-      password: avalanche
-      ssl_mode: disable
-      postgres: { publication: @SCHEMA@_ice_pub, slot: @SCHEMA@_ice }
-    tables: { include: [@SCHEMA@.*] }
+@SOURCE(_ice)@
+    tables: { include: ['@INCLUDE@'] }
     sink:
       type: iceberg
       bucket: @BUCKET@
@@ -165,10 +156,7 @@ async fn an_iceberg_pipeline_is_refused_beside_a_running_one() {
     .await;
     let mut p = e.start(&[]);
     p.ready().await;
-    p.metric(
-        r#"data_avalanche_pipeline_status{pipeline="ice",source_host="127.0.0.1",source_db="avalanche",status="error"} 1"#,
-    )
-    .await;
+    p.metric(&e.status_of("ice", "error")).await;
     e.streaming().await;
     e.exec("INSERT INTO items VALUES (1, 'a')").await;
     e.converged("items").await;
@@ -178,17 +166,17 @@ async fn an_iceberg_pipeline_is_refused_beside_a_running_one() {
         "{}",
         p.log()
     );
-    let ice_slot = format!("{}_ice", e.schema);
-    let slots: i64 = e
-        .admin
-        .query_one(
-            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
-            &[&ice_slot],
-        )
-        .await
-        .expect("slots read")
-        .get(0);
-    assert_eq!(slots, 0, "the refused pipeline touched no source");
+    if e.source.is_postgres() {
+        let ice_slot = format!("{}_ice", e.schema);
+        assert!(
+            !e.db
+                .any(&format!(
+                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = '{ice_slot}'"
+                ))
+                .await,
+            "the refused pipeline touched no source"
+        );
+    }
     assert!(p.terminate().await.success());
     e.cleanup().await;
 }
```

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index f879792..ee1452e 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -1,6 +1,8 @@
-//! The slice-1 E2E suite (roadmap §5.3): the `data-avalanche` binary as a process, PostgreSQL
-//! (`DATA_AVALANCHE_PG_PORT`, else 18) and RustFS from `test-infra/compose.yaml`. Needs the
-//! `e2e-hooks` feature and `DATA_AVALANCHE_IT=1`:
+//! The slice-1 E2E suite (roadmap §5.3): the `data-avalanche` binary as a process, a source and
+//! RustFS from `test-infra/compose.yaml`. The source is PostgreSQL (`DATA_AVALANCHE_PG_PORT`,
+//! else 18), or with `DATA_AVALANCHE_E2E_SOURCE=mysql` the MySQL or MariaDB server
+//! `DATA_AVALANCHE_MYSQL` names (else `mysql-84`); a scenario of one source only skips itself on
+//! the other (Q-M7b-1). Needs the `e2e-hooks` feature and `DATA_AVALANCHE_IT=1`:
 //!
 //! `cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`
 
@@ -11,6 +13,7 @@ mod lifecycle;
 mod schema;
 mod smoke;
 mod snapshots;
+mod source;
 mod streaming;
 mod toast;
 mod types;
```

Change `crates/data-avalanche/tests/e2e/schema.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/schema.rs b/crates/data-avalanche/tests/e2e/schema.rs
index 97225ea..e9604f2 100644
--- a/crates/data-avalanche/tests/e2e/schema.rs
+++ b/crates/data-avalanche/tests/e2e/schema.rs
@@ -108,7 +108,12 @@ async fn unsafe_changes_halt_their_table_only() {
         e.converged(t).await;
     }
 
-    e.exec("ALTER TABLE widen ALTER COLUMN n TYPE bigint; INSERT INTO widen VALUES (2, 2)")
+    let widen = if e.source.is_postgres() {
+        "ALTER TABLE widen ALTER COLUMN n TYPE bigint"
+    } else {
+        "ALTER TABLE widen MODIFY n bigint"
+    };
+    e.exec(&format!("{widen}; INSERT INTO widen VALUES (2, 2)"))
         .await;
     e.exec("ALTER TABLE reserved ADD COLUMN _op text; INSERT INTO reserved VALUES (2, 'b', 'x')")
         .await;
@@ -136,7 +141,7 @@ async fn unsafe_changes_halt_their_table_only() {
             ("widen".into(), "EVOLUTION_UNSUPPORTED"),
         ]
     );
-    let labels = r#"pipeline="schema_halts",source_host="127.0.0.1",source_db="avalanche""#;
+    let labels = e.labels();
     p.metric(&format!("data_avalanche_halted_tables{{{labels}}} 3"))
         .await;
     assert_eq!(e.ids("widen").await, [1], "nothing after the halt");
```

Change `crates/data-avalanche/tests/e2e/smoke.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/smoke.rs b/crates/data-avalanche/tests/e2e/smoke.rs
index ebdb4f3..b0958f7 100644
--- a/crates/data-avalanche/tests/e2e/smoke.rs
+++ b/crates/data-avalanche/tests/e2e/smoke.rs
@@ -30,11 +30,7 @@ async fn a_pipeline_runs_stops_on_sigterm_and_resumes() {
         !log.contains("buoyant_kernel") && !log.contains("deltalake_core"),
         "{log}"
     );
-    let labels = r#"pipeline="smoke",source_host="127.0.0.1",source_db="avalanche""#;
-    p.metric(&format!(
-        r#"data_avalanche_pipeline_status{{{labels},status="running"}} 1"#
-    ))
-    .await;
+    p.metric(&e.status("running")).await;
     p.metric("data_avalanche_owned_pipelines 1").await;
     assert!(e.lease().await.unwrap().doc.owner_epoch == 1);
 
```

Create `crates/data-avalanche/tests/e2e/source.rs`:

```rust
//! The source under test (Q-M7b-1): PostgreSQL by default, MySQL or MariaDB with
//! `DATA_AVALANCHE_E2E_SOURCE=mysql` (the server `DATA_AVALANCHE_MYSQL` names, else `mysql-84`),
//! and an admin connection that speaks either.

use mysql_async::prelude::Queryable;
use tokio::sync::Mutex;
use tokio_postgres::{NoTls, SimpleQueryMessage};

/// The PostgreSQL port under test: `DATA_AVALANCHE_PG_PORT`, else `postgres-18` (55418).
pub fn pg_port() -> u16 {
    std::env::var("DATA_AVALANCHE_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55418)
}

/// A MySQL or MariaDB server of `test-infra/compose.yaml`, and the position mode a pipeline
/// reads it in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Target {
    pub name: &'static str,
    pub port: u16,
    pub mariadb: bool,
    /// `replication_type`: `gtid` or `binlog`.
    pub replication: &'static str,
    /// The compose service (its container is `data-avalanche-test-<service>-1`).
    pub service: &'static str,
}

const TARGETS: [Target; 6] = [
    Target {
        name: "mysql-84",
        port: 53384,
        mariadb: false,
        replication: "gtid",
        service: "mysql-84",
    },
    Target {
        name: "mysql-84-nogtid",
        port: 53385,
        mariadb: false,
        replication: "binlog",
        service: "mysql-84-nogtid",
    },
    Target {
        name: "mariadb-118",
        port: 53318,
        mariadb: true,
        replication: "gtid",
        service: "mariadb-118",
    },
    Target {
        name: "mariadb-118-binlog",
        port: 53318,
        mariadb: true,
        replication: "binlog",
        service: "mariadb-118",
    },
    Target {
        name: "mariadb-114",
        port: 53314,
        mariadb: true,
        replication: "gtid",
        service: "mariadb-114",
    },
    Target {
        name: "mariadb-1011",
        port: 53311,
        mariadb: true,
        replication: "gtid",
        service: "mariadb-1011",
    },
];

/// A MySQL target by name.
///
/// # Panics
/// When `name` isn't one of [`TARGETS`].
pub fn target(name: &str) -> Target {
    TARGETS
        .into_iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| panic!("{name}: not a test-infra MySQL target"))
}

/// The source the suite runs against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Postgres { port: u16 },
    Mysql(Target),
}

impl Source {
    /// From `DATA_AVALANCHE_E2E_SOURCE` (`postgres`, the default, or `mysql`).
    ///
    /// # Panics
    /// On another value, or a `DATA_AVALANCHE_MYSQL` that names no target.
    pub fn from_env() -> Self {
        match std::env::var("DATA_AVALANCHE_E2E_SOURCE").as_deref() {
            Err(_) | Ok("" | "postgres") => Self::Postgres { port: pg_port() },
            Ok("mysql") => Self::Mysql(target(
                &std::env::var("DATA_AVALANCHE_MYSQL").unwrap_or_else(|_| "mysql-84".into()),
            )),
            Ok(other) => panic!("DATA_AVALANCHE_E2E_SOURCE={other}: postgres or mysql"),
        }
    }

    pub fn is_postgres(self) -> bool {
        matches!(self, Self::Postgres { .. })
    }

    /// The server's container: `postgres-<major>` on `554<major>`, a MySQL target's service.
    pub fn container(self) -> String {
        match self {
            Self::Postgres { port } => format!("data-avalanche-test-postgres-{}-1", port - 55400),
            Self::Mysql(t) => format!("data-avalanche-test-{}-1", t.service),
        }
    }
}

/// Skips the test, with a line saying so, unless the suite runs against PostgreSQL.
#[macro_export]
macro_rules! postgres_only {
    () => {
        if !$crate::source::Source::from_env().is_postgres() {
            eprintln!("skipped: a PostgreSQL scenario");
            return;
        }
    };
}

/// Skips the test, with a line saying so, unless the suite runs against MySQL or MariaDB; else
/// the target.
#[macro_export]
macro_rules! mysql_only {
    () => {
        match $crate::source::Source::from_env() {
            $crate::source::Source::Mysql(t) => t,
            $crate::source::Source::Postgres { .. } => {
                eprintln!("skipped: a MySQL scenario");
                return;
            }
        }
    };
}

/// An admin connection to the source: PostgreSQL's `avalanche` database, or MySQL's server as
/// `root`.
pub enum Db {
    Pg(tokio_postgres::Client),
    Mysql(Mutex<mysql_async::Conn>),
}

impl Db {
    pub async fn connect(source: Source) -> Self {
        match source {
            Source::Postgres { port } => {
                let dsn = format!(
                    "host=127.0.0.1 port={port} user=avalanche password=avalanche dbname=avalanche"
                );
                let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
                    .await
                    .expect("connect to test PostgreSQL");
                tokio::spawn(conn);
                Self::Pg(client)
            }
            Source::Mysql(t) => Self::Mysql(Mutex::new(mysql(t.port).await)),
        }
    }

    /// Runs `sql`, one or more statements.
    pub async fn exec(&self, sql: &str) -> Result<(), String> {
        match self {
            Self::Pg(c) => c.batch_execute(sql).await.map_err(|e| e.to_string()),
            Self::Mysql(c) => c
                .lock()
                .await
                .query_drop(sql)
                .await
                .map_err(|e| e.to_string()),
        }
    }

    /// The rows of one query, each cell as the server prints it (`None` for a NULL).
    pub async fn text(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>, String> {
        match self {
            Self::Pg(c) => Ok(c
                .simple_query(sql)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .filter_map(|m| match m {
                    SimpleQueryMessage::Row(r) => {
                        Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect())
                    }
                    _ => None,
                })
                .collect()),
            Self::Mysql(c) => {
                let rows: Vec<mysql_async::Row> =
                    c.lock().await.query(sql).await.map_err(|e| e.to_string())?;
                Ok(rows
                    .into_iter()
                    .map(|r| {
                        r.unwrap()
                            .into_iter()
                            .map(|v| match v {
                                mysql_async::Value::NULL => None,
                                mysql_async::Value::Bytes(b) => {
                                    Some(String::from_utf8_lossy(&b).into_owned())
                                }
                                // The text protocol sends every value as bytes.
                                other => Some(other.as_sql(true)),
                            })
                            .collect()
                    })
                    .collect())
            }
        }
    }

    /// Whether `sql` returns a row.
    pub async fn any(&self, sql: &str) -> bool {
        !self.text(sql).await.expect("the source reads").is_empty()
    }

    /// The PostgreSQL client, for a PostgreSQL scenario.
    ///
    /// # Panics
    /// On MySQL.
    pub fn pg(&self) -> &tokio_postgres::Client {
        match self {
            Self::Pg(c) => c,
            Self::Mysql(_) => panic!("a PostgreSQL scenario on MySQL"),
        }
    }
}

/// A `root` connection to the MySQL server on `port`, in no database.
pub async fn mysql(port: u16) -> mysql_async::Conn {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("avalanche"))
        .prefer_socket(false);
    mysql_async::Conn::new(opts)
        .await
        .expect("connect to the test MySQL server")
}
```

Change `crates/data-avalanche/tests/e2e/streaming.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/streaming.rs b/crates/data-avalanche/tests/e2e/streaming.rs
index 079d253..9e4c3f8 100644
--- a/crates/data-avalanche/tests/e2e/streaming.rs
+++ b/crates/data-avalanche/tests/e2e/streaming.rs
@@ -5,7 +5,7 @@ use std::time::{Duration, Instant};
 
 use data_avalanche_testkit::require_it;
 
-use crate::harness::{E2e, Options};
+use crate::harness::{Doc, E2e, Options};
 
 const COLS: &[&str] = &["id", "doc"];
 
@@ -20,7 +20,7 @@ async fn row_changes_and_transaction_shapes_are_mirrored() {
          CREATE TABLE other (id bigint PRIMARY KEY, doc text);
          CREATE TABLE ignored (id bigint PRIMARY KEY);",
         Options {
-            tables: ", exclude: [e2e_stream_rows.ignored], snapshots: { mode: never }",
+            tables: ", exclude: ['@SCHEMA@.ignored'], snapshots: { mode: never }",
             ..Options::default()
         },
     )
@@ -78,8 +78,7 @@ async fn a_large_transaction_lands_at_once() {
     p.ready().await;
     e.streaming().await;
     let before = e.commits("bulk").await;
-    e.exec("INSERT INTO bulk SELECT g, 'row ' || g FROM generate_series(1, 50000) g")
-        .await;
+    e.insert_series("bulk", 50_000, Doc::Row).await;
     let until = Instant::now() + Duration::from_secs(120);
     loop {
         let seen = e.content("bulk").await.map_or(0, |b| b.num_rows());
@@ -118,8 +117,7 @@ async fn a_split_transaction_is_replayed_after_a_crash_between_slices() {
     let mut p = e.start(&[("DATA_AVALANCHE_E2E_CRASH", "sink_committed:2")]);
     p.ready().await;
     e.streaming().await;
-    e.exec("INSERT INTO bulk SELECT g, 'row ' || g FROM generate_series(1, 5000) g")
-        .await;
+    e.insert_series("bulk", 5000, Doc::Row).await;
     let status = p.exit().await;
     assert!(!status.success(), "aborted at the crash point: {status}");
     assert_eq!(
```

Change `crates/data-avalanche/tests/e2e/toast.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/toast.rs b/crates/data-avalanche/tests/e2e/toast.rs
index 2144f2b..332d5c1 100644
--- a/crates/data-avalanche/tests/e2e/toast.rs
+++ b/crates/data-avalanche/tests/e2e/toast.rs
@@ -24,6 +24,7 @@ fn row(id: u32, n: u32) -> (String, String, usize) {
 #[tokio::test]
 async fn an_update_that_leaves_a_toasted_column_keeps_its_value() {
     require_it!();
+    crate::postgres_only!();
     // EXTERNAL: stored out of line, uncompressed, so the value is TOASTed.
     let e = E2e::new(
         "toast",
```

Change `crates/data-avalanche/tests/e2e/types.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/types.rs b/crates/data-avalanche/tests/e2e/types.rs
index 6810087..a80813c 100644
--- a/crates/data-avalanche/tests/e2e/types.rs
+++ b/crates/data-avalanche/tests/e2e/types.rs
@@ -133,6 +133,7 @@ fn duckdb_read(url: &str) -> (Vec<String>, Vec<String>, Vec<Vec<Option<String>>>
 #[tokio::test]
 async fn every_type_family_reads_back_in_duckdb() {
     require_it!();
+    crate::postgres_only!();
     let e = E2e::new(
         "types",
         &format!("{DDL} INSERT INTO types VALUES (1, {ROW_A}), (2, {ROW_B});"),
```

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index a8362f1..6392a78 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1695,6 +1695,7 @@ dependencies = [
  "data-avalanche-telemetry",
  "data-avalanche-testkit",
  "futures-util",
+ "mysql_async",
  "object_store 0.14.2",
  "rustls",
  "serde_json",
```

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
index f5edb57..a6225a2 100644
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -39,6 +39,7 @@ arrow-array.workspace = true
 arrow-cast.workspace = true
 data-avalanche-testkit.workspace = true
 futures-util.workspace = true
+mysql_async.workspace = true
 serde_json.workspace = true
 tempfile.workspace = true
 tokio-postgres.workspace = true
```

- [ ] **Step 2: Run it**

Run: `cargo clippy -q -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no warnings.

With `test-infra/up.sh` run (PostgreSQL 18, the MySQL targets, RustFS):

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=mysql-84 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=mysql-84-nogtid cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=mariadb-118 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Expected: PASS on each target.

Measured (2026-10-17, Tasks 1–6 applied; Task 7's scenarios added later run in the same suite): PostgreSQL 18, 26 of 26 (the regression run); `mysql-84`, 24 of 26 at first. The two failures were fixed into the blocks above: the buffer scenario's `bigserial`, and the stop scenario's wait for the dump to go (D-M7b.13). With Task 7 applied: PostgreSQL 18, `mysql-84`, `mariadb-114` and `mariadb-1011`, 33 of 33 each; `mysql-84-nogtid`, 32 of 32; `mariadb-118` and `mariadb-118-binlog`, 28 of 33, then the five failures green on rerun once fixed (the recursion ceiling on MariaDB, D-M7b.12).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 3: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche/Cargo.toml \
  crates/data-avalanche/tests/e2e/crashes.rs \
  crates/data-avalanche/tests/e2e/guards.rs \
  crates/data-avalanche/tests/e2e/harness.rs \
  crates/data-avalanche/tests/e2e/lifecycle.rs \
  crates/data-avalanche/tests/e2e/main.rs \
  crates/data-avalanche/tests/e2e/schema.rs \
  crates/data-avalanche/tests/e2e/smoke.rs \
  crates/data-avalanche/tests/e2e/source.rs \
  crates/data-avalanche/tests/e2e/streaming.rs \
  crates/data-avalanche/tests/e2e/toast.rs \
  crates/data-avalanche/tests/e2e/types.rs
git commit -m "test(e2e): one suite, two sources — the harness drives PostgreSQL or MySQL/MariaDB (Q-M7b-1)"
```

---

### Task 7: The MySQL scenarios and the wire proxy (`tests/e2e`)

**Files:**
- Modify: `Cargo.lock`
- Modify: `crates/data-avalanche/Cargo.toml`
- Modify (test): `crates/data-avalanche/tests/e2e/main.rs`
- Create (test): `crates/data-avalanche/tests/e2e/mysql.rs`
- Create (test): `crates/data-avalanche/tests/e2e/proxy.rs`
- Modify (test): `crates/data-avalanche/tests/e2e/types.rs`

**Interfaces:**
- Consumes:
  - Task 6's harness (`E2e`, `Options.port`, `Options.mysql`, `mysql_only!`, `Db`, `Source`, `target`).
  - Task 3's `code` log field; Task 4's server check.
  - `flate2::Crc` (workspace pin).
- Produces:
  - `tests/e2e/proxy.rs`: `Proxy::{start(upstream: u16), port, corrupt_next_payload, corrupted, switch(port)}`.
  - `tests/e2e/mysql.rs`: the scenarios; `types::duckdb_read` becomes `pub`.

Q-M7b-3, D-M7b.14…16; the roadmap's exit "payload-check fault injection halts with no silent loss", and M7a's carry item "`re_snapshot` at start, end to end through the worker".

- **A corrupted compressed transaction** (MySQL): the proxy corrupts the next `TRANSACTION_PAYLOAD` a dump reads. The pipeline goes to `ERROR` with `code="BINLOG_PAYLOAD_INVALID"`, and nothing of the transaction lands. A restart without the fault reads the transaction again, whole: nothing is lost.
- **A purged checkpoint**: the binlog after the checkpoint is purged while the pipeline is down. Under `re_snapshot` the start re-snapshots, with the warning, and streams on. Under `error` the pipeline goes to `ERROR` with the lost checkpoint in its message.
- **A failover in file/position mode** (`mysql-84-nogtid`): the proxy sends the pipeline's connections to `mysql-84`. The reconnect's dump finds another server and halts with `code="BINLOG_SOURCE_CHANGED"`. Back on its server, a restart streams on.
- **The types**: one row per MySQL family (MariaDB's own types are M7a's ITs), through the snapshot and the stream, read back with DuckDB. MariaDB's `JSON` keeps its text as written.

The log's `code` field is matched through the ANSI colour sequences the binary writes even to a file (D-M7b's measured list).

- [ ] **Step 1: Write the change**

Change `crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index ee1452e..1035625 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -10,6 +10,8 @@ mod crashes;
 mod guards;
 mod harness;
 mod lifecycle;
+mod mysql;
+mod proxy;
 mod schema;
 mod smoke;
 mod snapshots;
```

Create `crates/data-avalanche/tests/e2e/mysql.rs`:

```rust
//! The MySQL and MariaDB scenarios (Q-M7b-1): a corrupted compressed transaction and a failover
//! behind the pipeline's address each halt the pipeline with their code; a checkpoint the server
//! purged follows `binlog_purge_action`.

use std::time::Duration;

use data_avalanche_testkit::require_it;

use crate::harness::{E2e, Options, eventually};
use crate::proxy::Proxy;
use crate::source::{Db, Source, target};

const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";
const COLS: &[&str] = &["id", "doc"];

/// `text` without its ANSI colour sequences (`ESC [ … m`): the binary colours its log.
fn plain(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            chars.by_ref().find(|&c| c == 'm');
        } else {
            out.push(c);
        }
    }
    out
}

/// Whether the log has the line of a pipeline in `ERROR` with `code` (Q-M7b-4).
fn halted_with(log: &str, code: &str) -> bool {
    plain(log)
        .lines()
        .any(|l| l.contains("pipeline in ERROR") && l.contains(&format!("code=\"{code}\"")))
}

#[test]
fn a_halt_line_is_found_through_its_colours() {
    let line = "\u{1b}[31mERROR\u{1b}[0m pipeline in ERROR: X_Y: z \u{1b}[3mcode\u{1b}[0m\u{1b}[2m=\u{1b}[0m\"X_Y\"";
    assert!(halted_with(line, "X_Y"));
    assert!(!halted_with(line, "X_Z"));
}

/// A compressed transaction (MySQL's `binlog_transaction_compression`) whose payload the wire
/// corrupted halts the pipeline with `BINLOG_PAYLOAD_INVALID`: status `error`, the code in the
/// log, and nothing of the transaction in the lake. A restart without the fault reads the
/// transaction again, whole.
#[tokio::test]
async fn a_corrupted_compressed_transaction_halts_the_pipeline() {
    require_it!();
    let t = crate::mysql_only!();
    if t.mariadb {
        eprintln!("skipped: MySQL's compressed transactions");
        return;
    }
    let proxy = Proxy::start(t.port).await;
    let e = E2e::new(
        "my_payload",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            port: Some(proxy.port()),
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'plain')").await;
    e.mirrored("items", COLS).await;

    proxy.corrupt_next_payload();
    e.exec(
        "SET SESSION binlog_transaction_compression = ON;
         INSERT INTO items VALUES (2, REPEAT('x', 200)), (3, REPEAT('y', 200));
         SET SESSION binlog_transaction_compression = OFF",
    )
    .await;
    p.metric(&e.status("error")).await;
    assert!(proxy.corrupted(), "the proxy corrupted the payload");
    let log = p.log();
    assert!(halted_with(&log, "BINLOG_PAYLOAD_INVALID"), "{log}");
    assert_eq!(
        e.ids("items").await,
        [1],
        "nothing of the transaction lands"
    );
    assert!(p.terminate().await.success());

    let mut p = e.start(&[]);
    p.ready().await;
    e.mirrored("items", COLS).await;
    assert_eq!(e.ids("items").await, [1, 2, 3]);
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// Purges every binlog but the newest, after closing the current one: the transactions in them
/// are gone from the server.
async fn purge(db: &Db) {
    db.exec("FLUSH BINARY LOGS").await.expect("flushed");
    let logs = db.text("SHOW BINARY LOGS").await.expect("the binlogs");
    let newest = logs.last().and_then(|r| r[0].clone()).expect("a binlog");
    // MariaDB keeps the file it just closed until its binlog checkpoint moves on.
    eventually("the purge", Duration::from_secs(10), async || {
        db.exec(&format!("PURGE BINARY LOGS TO '{newest}'"))
            .await
            .expect("purged");
        db.text("SHOW BINARY LOGS")
            .await
            .expect("the binlogs")
            .len()
            == 1
    })
    .await;
}

/// A world whose checkpoint the server purged while the pipeline was down: a first run
/// snapshots `items`, then a row is inserted and its binlog purged. Returns the first snapshot.
async fn purged(e: &E2e) -> data_avalanche_state::TableSnapshotDoc {
    let mut p = e.start(&[]);
    p.ready().await;
    let first = e.completed("items", None).await;
    e.converged("items").await;
    assert!(p.terminate().await.success());
    e.exec("INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'B' WHERE id = 2")
        .await;
    purge(&e.db).await;
    first
}

/// Under `binlog_purge_action: re_snapshot` the start re-snapshots from the server's position now,
/// with a warning, and the lake gets the rows the purged binlog held; then it streams on.
#[tokio::test]
async fn a_purged_checkpoint_re_snapshots_at_the_start() {
    require_it!();
    crate::mysql_only!();
    let e = E2e::new(
        "my_purge_snap",
        &format!("{ITEMS} INSERT INTO items VALUES (1, 'a'), (2, 'b');"),
        Options {
            tables: ", snapshots: { mode: initial }",
            mysql: ", binlog_purge_action: re_snapshot",
            ..Options::default()
        },
    )
    .await;
    let first = purged(&e).await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.completed("items", Some(&first)).await;
    e.mirrored("items", COLS).await;
    let log = p.log();
    assert!(
        log.contains("binlog_purge_action is re_snapshot, so the pipeline re-snapshots"),
        "{log}"
    );
    p.metric(&e.status("running")).await;
    e.exec("INSERT INTO items VALUES (4, 'd')").await;
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// Under `binlog_purge_action: error` (the default) the same start halts the pipeline: status
/// `error`, the lost checkpoint named in the log, the lake as the first run left it.
#[tokio::test]
async fn a_purged_checkpoint_halts_under_error() {
    require_it!();
    crate::mysql_only!();
    let e = E2e::new(
        "my_purge_error",
        &format!("{ITEMS} INSERT INTO items VALUES (1, 'a'), (2, 'b');"),
        Options {
            tables: ", snapshots: { mode: initial }",
            ..Options::default()
        },
    )
    .await;
    purged(&e).await;
    let mut p = e.start(&[]);
    p.ready().await;
    p.metric(&e.status("error")).await;
    let log = p.log();
    assert!(
        log.contains("binlog_purge_action is error, so the pipeline halts"),
        "{log}"
    );
    assert_eq!(e.ids("items").await, [1, 2]);
    assert!(p.terminate().await.success());
    e.cleanup().await;
}

/// A failover behind the pipeline's address in file/position mode: the reconnect reaches another
/// server, whose binlog files the checkpoint means nothing in. The dump's open checks the server
/// on its own connection and halts the pipeline with `BINLOG_SOURCE_CHANGED`. Back on its server,
/// a restart streams on from the checkpoint. MySQL only: MariaDB tells servers apart by
/// `server_id`, which every test-infra server shares.
#[tokio::test]
async fn a_failover_in_file_position_mode_halts_the_pipeline() {
    require_it!();
    let t = crate::mysql_only!();
    if t.name != "mysql-84-nogtid" {
        eprintln!("skipped: file/position mode on MySQL (mysql-84-nogtid)");
        return;
    }
    // The server behind the address after the failover: the test's database, table and user
    // are there too, as on a replica.
    let other = Source::Mysql(target("mysql-84"));
    let replica = Db::connect(other).await;
    let s = "e2e_my_failover";
    replica
        .exec(&format!(
            "DROP DATABASE IF EXISTS {s}; CREATE DATABASE {s}; USE {s}; {ITEMS}
             DROP USER IF EXISTS '{s}'@'%';
             CREATE USER '{s}'@'%' IDENTIFIED BY 'avalanche';
             GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO '{s}'@'%';"
        ))
        .await
        .expect("the replica's copy");
    let proxy = Proxy::start(t.port).await;
    let e = E2e::new(
        "my_failover",
        ITEMS,
        Options {
            tables: ", snapshots: { mode: never }",
            port: Some(proxy.port()),
            ..Options::default()
        },
    )
    .await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.streaming().await;
    e.exec("INSERT INTO items VALUES (1, 'a')").await;
    e.mirrored("items", COLS).await;

    proxy.switch(target("mysql-84").port);
    p.metric(&e.status("error")).await;
    let log = p.log();
    assert!(halted_with(&log, "BINLOG_SOURCE_CHANGED"), "{log}");
    assert!(p.terminate().await.success());

    proxy.switch(t.port);
    e.exec("INSERT INTO items VALUES (2, 'b')").await;
    let mut p = e.start(&[]);
    p.ready().await;
    e.mirrored("items", COLS).await;
    assert!(p.terminate().await.success());
    replica
        .exec(&format!(
            "DROP DATABASE IF EXISTS {s}; DROP USER IF EXISTS '{s}'@'%';"
        ))
        .await
        .expect("the replica's copy dropped");
    e.cleanup().await;
}

const TYPES_DDL: &str = "CREATE TABLE types (
        id bigint PRIMARY KEY,
        b tinyint(1), ti tinyint, tu tinyint unsigned, si smallint, su smallint unsigned,
        mi mediumint, iu int unsigned, i int, bi bigint, bu bigint unsigned,
        f float, d double, n decimal(12,3),
        ch char(5), vc varchar(10), tx text, bn binary(4), vb varbinary(8), bl blob,
        dt date, tm time(6), dtt datetime(6), ts timestamp(6) NULL, y year,
        j json, e enum('sad','ok','happy'), st set('a','b','c'), bt bit(10)
    )";

/// The non-key columns' SQL literals, per row.
const TYPES_A: &str = "1, -128, 255, -32768, 65535, -8388608, 4294967295, 2147483647,
    9223372036854775807, 18446744073709551615, 1.5, 0.1, 123456789.123,
    'ab', 'hello', 'it''s', X'0102', X'DEADBEEF', X'00FF',
    '2024-02-29', '-838:59:59.000000', '2024-02-29 12:34:56.789012',
    '2024-02-29 12:34:56.789012', 2155, '{\"a\": 1}', 'happy', 'a,c', b'1000000001'";
const TYPES_B: &str = "0, 127, 0, 32767, 0, 8388607, 0, -2147483648,
    -9223372036854775808, 0, -1.5, -0.001, -0.001,
    'abcde', '', '', X'', X'', X'',
    '1970-01-01', '00:00:00', '1000-01-01 00:00:00', '1970-01-01 00:00:01', 1901, '[]', 'sad',
    '', b'0'";

/// Per column, the Delta column's type as DuckDB reads it, and DuckDB's text of [`TYPES_A`] and
/// [`TYPES_B`] (`Type_Mapping.md`, MySQL and Hop 3b).
const TYPES_EXPECTED: &[[&str; 4]] = &[
    ["b", "BOOLEAN", "true", "false"],
    ["ti", "SMALLINT", "-128", "127"],
    ["tu", "SMALLINT", "255", "0"],
    ["si", "SMALLINT", "-32768", "32767"],
    ["su", "INTEGER", "65535", "0"],
    ["mi", "INTEGER", "-8388608", "8388607"],
    ["iu", "BIGINT", "4294967295", "0"],
    ["i", "INTEGER", "2147483647", "-2147483648"],
    [
        "bi",
        "BIGINT",
        "9223372036854775807",
        "-9223372036854775808",
    ],
    ["bu", "DECIMAL(20,0)", "18446744073709551615", "0"],
    ["f", "FLOAT", "1.5", "-1.5"],
    ["d", "DOUBLE", "0.1", "-0.001"],
    ["n", "DECIMAL(12,3)", "123456789.123", "-0.001"],
    ["ch", "VARCHAR", "ab", "abcde"],
    ["vc", "VARCHAR", "hello", ""],
    ["tx", "VARCHAR", "it's", ""],
    ["bn", "BLOB", "\\x01\\x02\\x00\\x00", "\\x00\\x00\\x00\\x00"],
    ["vb", "BLOB", "\\xDE\\xAD\\xBE\\xEF", ""],
    ["bl", "BLOB", "\\x00\\xFF", ""],
    ["dt", "DATE", "2024-02-29", "1970-01-01"],
    ["tm", "VARCHAR", "-838:59:59.000000", "00:00:00.000000"],
    [
        "dtt",
        "TIMESTAMP",
        "2024-02-29 12:34:56.789012",
        "1000-01-01 00:00:00",
    ],
    [
        "ts",
        "TIMESTAMP WITH TIME ZONE",
        "2024-02-29 12:34:56.789012+00",
        "1970-01-01 00:00:01+00",
    ],
    ["y", "INTEGER", "2155", "1901"],
    ["j", "VARCHAR", "{\"a\":1}", "[]"],
    ["e", "VARCHAR", "happy", "sad"],
    ["st", "VARCHAR[]", "[a, c]", "[]"],
    ["bt", "BLOB", "\\x02\\x01", "\\x00\\x00"],
];

/// Each MySQL type family through a snapshot and through the stream into Delta, read back by
/// DuckDB's `delta_scan` (`Type_Mapping.md`, MySQL and Hop 3b); the session's zone is UTC, so a
/// `TIMESTAMP` literal is its UTC value. MariaDB's `JSON` is `LONGTEXT`: its text stays as
/// written.
#[tokio::test]
async fn every_mysql_type_family_reads_back_in_duckdb() {
    require_it!();
    let t = crate::mysql_only!();
    let e = E2e::new(
        "my_types",
        &format!(
            "SET time_zone = '+00:00'; {TYPES_DDL};
             INSERT INTO types VALUES (1, {TYPES_A}), (2, {TYPES_B});"
        ),
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
        "SET time_zone = '+00:00'; INSERT INTO types VALUES (3, {TYPES_A});
         INSERT INTO types (id) VALUES (4)"
    ))
    .await;
    e.converged("types").await;
    assert!(p.terminate().await.success());

    let (columns, types, rows) = crate::types::duckdb_read(&e.table_url("types"));
    assert_eq!(rows.len(), 4);
    let source: Vec<usize> = (0..columns.len())
        .filter(|&i| columns[i] != "id" && !columns[i].starts_with('_'))
        .collect();
    let named = |row: usize| -> Vec<(&str, Option<&str>)> {
        source
            .iter()
            .map(|&i| (columns[i].as_str(), rows[row][i].as_deref()))
            .collect()
    };
    let want = |i: usize| -> Vec<(&str, Option<&str>)> {
        TYPES_EXPECTED
            .iter()
            .map(|c| match (c[0], i, t.mariadb) {
                ("j", 2, true) => ("j", Some("{\"a\": 1}")),
                _ => (c[0], Some(c[i])),
            })
            .collect()
    };
    let typed: Vec<(&str, &str)> = source
        .iter()
        .map(|&i| (columns[i].as_str(), types[i].as_str()))
        .collect();
    let want_types: Vec<(&str, &str)> = TYPES_EXPECTED.iter().map(|c| (c[0], c[1])).collect();
    assert_eq!(typed, want_types, "the Delta column types");
    assert_eq!(named(0), want(2), "row 1, from the snapshot");
    assert_eq!(named(1), want(3), "row 2, from the snapshot");
    assert_eq!(
        named(2),
        named(0),
        "the stream reads what the snapshot reads"
    );
    assert!(
        named(3).iter().all(|(_, v)| v.is_none()),
        "the all-NULL row"
    );
    e.cleanup().await;
}
```

Create `crates/data-avalanche/tests/e2e/proxy.rs`:

```rust
//! A TCP proxy in front of a MySQL server (Q-M7b-3), for the faults no server can be asked for.
//! It passes the protocol's packets through untouched. On request it corrupts the next
//! compressed transaction a binlog dump sends (`BINLOG_PAYLOAD_INVALID`), or sends new
//! connections to another server and cuts the open ones (a failover behind one address,
//! `BINLOG_SOURCE_CHANGED`). Without TLS only (`ssl_mode: disable`): it reads the packets.

use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::AbortHandle;

/// `COM_BINLOG_DUMP`, `COM_BINLOG_DUMP_GTID`: the commands that start a dump.
const DUMP_COMMANDS: [u8; 2] = [0x12, 0x1e];
/// MySQL's `TRANSACTION_PAYLOAD_EVENT` (a transaction compressed by
/// `binlog_transaction_compression`).
const TRANSACTION_PAYLOAD: u8 = 40;

#[derive(Debug, Default)]
struct Faults {
    /// Corrupt the next payload a dump sends.
    corrupt: AtomicBool,
    /// A payload was corrupted.
    corrupted: AtomicBool,
}

/// The proxy, listening until dropped.
#[derive(Debug)]
pub struct Proxy {
    port: u16,
    upstream: Arc<AtomicU16>,
    faults: Arc<Faults>,
    /// The listener's task, then each connection's pumps.
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
}

/// One protocol packet: a 3-byte length, a sequence id, the payload.
async fn packet(r: &mut (impl AsyncRead + Unpin)) -> std::io::Result<Vec<u8>> {
    let mut head = [0u8; 4];
    r.read_exact(&mut head).await?;
    let len = u32::from_le_bytes([head[0], head[1], head[2], 0]) as usize;
    let mut p = head.to_vec();
    p.resize(4 + len, 0);
    r.read_exact(&mut p[4..]).await?;
    Ok(p)
}

/// Corrupts `p` when it is a dump's `TRANSACTION_PAYLOAD` event (an OK byte, then the event's
/// 19-byte header, type at offset 4): one byte of the compressed body flipped and the event's
/// CRC32 recomputed, so only the decompression can find it.
fn corrupt(p: &mut [u8]) -> bool {
    if p.len() < 4 + 1 + 19 + 4 || p[4] != 0 || p[4 + 1 + 4] != TRANSACTION_PAYLOAD {
        return false;
    }
    let ev = &mut p[5..];
    let n = ev.len();
    ev[n - 4 - 2] ^= 0xff;
    let mut crc = flate2::Crc::new();
    crc.update(&ev[..n - 4]);
    ev[n - 4..].copy_from_slice(&crc.sum().to_le_bytes());
    true
}

impl Proxy {
    /// A proxy to the MySQL server on `upstream`.
    pub async fn start(upstream: u16) -> Self {
        let l = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a proxy port");
        let port = l.local_addr().expect("the proxy's address").port();
        let upstream = Arc::new(AtomicU16::new(upstream));
        let faults = Arc::new(Faults::default());
        let tasks = Arc::new(Mutex::new(vec![]));
        let accept = {
            let (upstream, faults, tasks) = (upstream.clone(), faults.clone(), tasks.clone());
            tokio::spawn(async move {
                while let Ok((client, _)) = l.accept().await {
                    let to = upstream.load(Ordering::SeqCst);
                    let Ok(server) = TcpStream::connect(("127.0.0.1", to)).await else {
                        continue;
                    };
                    let (mut cr, mut cw) = client.into_split();
                    let (mut sr, mut sw) = server.into_split();
                    let dump = Arc::new(AtomicBool::new(false));
                    let up = {
                        let dump = dump.clone();
                        tokio::spawn(async move {
                            while let Ok(p) = packet(&mut cr).await {
                                if p[3] == 0 && p.len() > 4 && DUMP_COMMANDS.contains(&p[4]) {
                                    dump.store(true, Ordering::SeqCst);
                                }
                                if sw.write_all(&p).await.is_err() {
                                    break;
                                }
                            }
                        })
                    };
                    let down = {
                        let faults = faults.clone();
                        tokio::spawn(async move {
                            while let Ok(mut p) = packet(&mut sr).await {
                                if dump.load(Ordering::SeqCst)
                                    && faults.corrupt.load(Ordering::SeqCst)
                                    && corrupt(&mut p)
                                {
                                    faults.corrupt.store(false, Ordering::SeqCst);
                                    faults.corrupted.store(true, Ordering::SeqCst);
                                }
                                if cw.write_all(&p).await.is_err() {
                                    break;
                                }
                            }
                        })
                    };
                    let mut t = tasks.lock().expect("no panic holds the lock");
                    t.push(up.abort_handle());
                    t.push(down.abort_handle());
                }
            })
        };
        tasks
            .lock()
            .expect("no panic holds the lock")
            .push(accept.abort_handle());
        Self {
            port,
            upstream,
            faults,
            tasks,
        }
    }

    /// The port a pipeline connects to.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Corrupts the next compressed transaction a dump sends, once.
    pub fn corrupt_next_payload(&self) {
        self.faults.corrupt.store(true, Ordering::SeqCst);
    }

    /// Whether a payload was corrupted.
    pub fn corrupted(&self) -> bool {
        self.faults.corrupted.load(Ordering::SeqCst)
    }

    /// Sends new connections to the server on `port` and cuts the open ones, as a failover
    /// behind one address does.
    pub fn switch(&self, port: u16) {
        self.upstream.store(port, Ordering::SeqCst);
        let mut tasks = self.tasks.lock().expect("no panic holds the lock");
        // The listener's task stays; every connection's pumps go.
        for t in tasks.drain(1..) {
            t.abort();
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        for t in self
            .tasks
            .lock()
            .expect("no panic holds the lock")
            .drain(..)
        {
            t.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A dump's event packet: header, OK byte, a 19-byte event header and `body`, then a CRC.
    fn event(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut ev = vec![0u8; 19];
        ev[4] = kind;
        ev.extend_from_slice(body);
        let mut crc = flate2::Crc::new();
        crc.update(&ev);
        ev.extend_from_slice(&crc.sum().to_le_bytes());
        let len = (ev.len() + 1) as u32;
        let mut p = len.to_le_bytes()[..3].to_vec();
        p.push(1);
        p.push(0);
        p.extend(ev);
        p
    }

    #[test]
    fn only_a_transaction_payload_is_corrupted_and_its_checksum_still_holds() {
        let mut p = event(TRANSACTION_PAYLOAD, b"compressed body");
        let before = p.clone();
        assert!(corrupt(&mut p));
        assert_ne!(p, before);
        let ev = &p[5..];
        let n = ev.len();
        let mut crc = flate2::Crc::new();
        crc.update(&ev[..n - 4]);
        assert_eq!(crc.sum().to_le_bytes(), ev[n - 4..], "the CRC matches");
        let mut rows = event(30, b"rows");
        assert!(!corrupt(&mut rows), "a WRITE_ROWS event passes");
    }
}
```

Change `crates/data-avalanche/tests/e2e/types.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/types.rs b/crates/data-avalanche/tests/e2e/types.rs
index a80813c..609476c 100644
--- a/crates/data-avalanche/tests/e2e/types.rs
+++ b/crates/data-avalanche/tests/e2e/types.rs
@@ -96,7 +96,7 @@ const EXPECTED: &[[&str; 4]] = &[
 
 /// DuckDB's `delta_scan` of a lake table: the column names, their DuckDB types and every cell as
 /// text, rows in key order. Needs `uv` (the CI integration job installs it).
-fn duckdb_read(url: &str) -> (Vec<String>, Vec<String>, Vec<Vec<Option<String>>>) {
+pub fn duckdb_read(url: &str) -> (Vec<String>, Vec<String>, Vec<Vec<Option<String>>>) {
     let script = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/e2e/duckdb_read.py");
     let out = std::process::Command::new("uv")
         .args([
```

Change `Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 6392a78..9f13ec0 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1694,6 +1694,7 @@ dependencies = [
  "data-avalanche-storage",
  "data-avalanche-telemetry",
  "data-avalanche-testkit",
+ "flate2",
  "futures-util",
  "mysql_async",
  "object_store 0.14.2",
```

Change `crates/data-avalanche/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
index a6225a2..de83cf2 100644
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -38,6 +38,7 @@ e2e-hooks = []
 arrow-array.workspace = true
 arrow-cast.workspace = true
 data-avalanche-testkit.workspace = true
+flate2.workspace = true
 futures-util.workspace = true
 mysql_async.workspace = true
 serde_json.workspace = true
```

- [ ] **Step 2: Run it**

Run: `cargo clippy -q -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no warnings.

Run: `cargo test -p data-avalanche --features e2e-hooks --test e2e -- proxy:: mysql::a_halt_line`

Expected: PASS, 2 tests.

With `test-infra/up.sh` run (PostgreSQL 18, the MySQL targets, RustFS):

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=mysql-84 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=mysql-84-nogtid cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=mariadb-118 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Expected: PASS on each target.

Measured (2026-10-17): the five scenarios pass on `mysql-84` (the failover skips itself), on `mysql-84-nogtid` (the failover runs), and on `mariadb-118`, `mariadb-118-binlog`, `mariadb-114` and `mariadb-1011` (the payload and failover scenarios skip themselves). The proxy forwarded unchanged packets in every run of the suite through it. The corrupted payload's halt line: `pipeline in ERROR: BINLOG_PAYLOAD_INVALID: the compressed transaction ending at mysql-bin.000018:31242774 yielded 0 of its 640 bytes of events pipeline=my_payload code="BINLOG_PAYLOAD_INVALID"`.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 3: Commit**

```bash
git add Cargo.lock \
  crates/data-avalanche/Cargo.toml \
  crates/data-avalanche/tests/e2e/main.rs \
  crates/data-avalanche/tests/e2e/mysql.rs \
  crates/data-avalanche/tests/e2e/proxy.rs \
  crates/data-avalanche/tests/e2e/types.rs
git commit -m "test(e2e): the MySQL scenarios — a corrupted payload and a failover halt with their codes, a purged checkpoint, the types in DuckDB (Q-M7b-3)"
```

---

### Task 8: The MySQL E2E in CI (`.github/workflows`, `test-infra/README.md`)

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/weekly.yml`
- Modify: `test-infra/README.md`

**Interfaces:**
- Consumes:
  - Tasks 6 and 7 (the suite, `DATA_AVALANCHE_E2E_SOURCE`, the targets).
  - `test-infra/compose.yaml`'s services and its `matrix` and `init` profiles.
- Produces:
  - `ci.yml` job `e2e-mysql` (matrix `mysql-84`, `mysql-84-nogtid`, `mariadb-118`); `weekly.yml` job `e2e-mysql` (`mariadb-118-binlog`, `mariadb-114`, `mariadb-1011`).

Q-M7b-2. The PostgreSQL suite stays where it is (the integration job, and `weekly.yml`'s PostgreSQL 14–17 jobs, where the MySQL scenarios skip themselves). Each MySQL target gets a job of its own, beside the integration job and in parallel with it. A job needs its server, `mysql-84` (the failover scenario's other server), RustFS and uv (the DuckDB read). No secrets. `test-infra/README.md` says how to run the suite on either source, and which scenarios change server-wide state.

- [ ] **Step 1: Write the change**

Change `.github/workflows/ci.yml`:

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index 53c4ec6..9c162ca 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -111,3 +111,40 @@ jobs:
       - name: Remove GCS key
         if: always()
         run: rm -f "$RUNNER_TEMP/gcs-key.json"
+
+  # The slice-1 E2E suite on MySQL and MariaDB (Q-M7b-1, Q-M7b-2), one job per target beside the
+  # integration job: MySQL 8.4 in GTID and file/position mode, MariaDB 11.8 in GTID mode (its
+  # file/position mode, and MariaDB 11.4 and 10.11, run in weekly.yml). The failover scenario on
+  # mysql-84-nogtid fails over to mysql-84, so mysql-84 runs in every job. No secrets.
+  e2e-mysql:
+    runs-on: ubuntu-latest
+    timeout-minutes: 60
+    needs: check
+    strategy:
+      fail-fast: false
+      matrix:
+        target: [mysql-84, mysql-84-nogtid, mariadb-118]
+    env:
+      DATA_AVALANCHE_IT: "1"
+      DATA_AVALANCHE_SKIP_GCS: "1"
+      DATA_AVALANCHE_E2E_SOURCE: mysql
+      DATA_AVALANCHE_MYSQL: ${{ matrix.target }}
+    steps:
+      - name: Free runner disk
+        run: sudo rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc /opt/hostedtoolcache/CodeQL
+      - uses: actions/checkout@v7
+      - uses: actions-rust-lang/setup-rust-toolchain@v2
+      # uv runs the type scenario's DuckDB read.
+      - uses: astral-sh/setup-uv@v10.2.0
+      - name: Start ${{ matrix.target }}, mysql-84 and RustFS
+        run: |
+          docker compose -f test-infra/compose.yaml up -d --wait "${{ matrix.target }}" mysql-84 rustfs
+          docker compose -f test-infra/compose.yaml --profile init run --rm rustfs-init
+      - name: Slice-1 E2E
+        run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
+      - name: Container logs on failure
+        if: failure()
+        run: docker compose -f test-infra/compose.yaml logs --no-color "${{ matrix.target }}" mysql-84 rustfs
+      - name: Stop the containers
+        if: always()
+        run: docker compose -f test-infra/compose.yaml down -v
```

Change `.github/workflows/weekly.yml`:

```diff
diff --git a/.github/workflows/weekly.yml b/.github/workflows/weekly.yml
index 1fec70e..92b5ba7 100644
--- a/.github/workflows/weekly.yml
+++ b/.github/workflows/weekly.yml
@@ -5,7 +5,8 @@ name: weekly
 # `postgres-18` suites of ci.yml re-run through DATA_AVALANCHE_PG_PORT. The E2E suite also needs
 # RustFS and uv (its DuckDB read).
 # The MySQL source against the older MariaDB LTS servers (P-8): 11.4 and 10.11, GTID mode, through
-# DATA_AVALANCHE_MYSQL.
+# DATA_AVALANCHE_MYSQL. The slice-1 E2E suite on MariaDB 11.8 in file/position mode and on 11.4
+# and 10.11 (Q-M7b-2; ci.yml runs it on MySQL 8.4 and MariaDB 11.8 per change).
 # Weekly (Mondays 03:41 UTC) and on manual dispatch. The TLS tests need `postgres-tls`
 # (PostgreSQL 18) or `mysql-tls` (MySQL 8.4) and run in ci.yml only. No secrets.
 
@@ -78,3 +79,35 @@ jobs:
       - name: Stop the containers
         if: always()
         run: docker compose -f test-infra/compose.yaml --profile matrix down -v
+
+  e2e-mysql:
+    runs-on: ubuntu-latest
+    timeout-minutes: 60
+    strategy:
+      fail-fast: false
+      matrix:
+        include:
+          - { target: mariadb-118-binlog, service: mariadb-118 }
+          - { target: mariadb-114, service: mariadb-114 }
+          - { target: mariadb-1011, service: mariadb-1011 }
+    env:
+      DATA_AVALANCHE_IT: "1"
+      DATA_AVALANCHE_SKIP_GCS: "1"
+      DATA_AVALANCHE_E2E_SOURCE: mysql
+      DATA_AVALANCHE_MYSQL: ${{ matrix.target }}
+    steps:
+      - uses: actions/checkout@v7
+      - uses: actions-rust-lang/setup-rust-toolchain@v2
+      - uses: astral-sh/setup-uv@v10.2.0
+      - name: Start ${{ matrix.service }} and RustFS
+        run: |
+          docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "${{ matrix.service }}" rustfs
+          docker compose -f test-infra/compose.yaml --profile init run --rm rustfs-init
+      - name: Slice-1 E2E
+        run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
+      - name: Container logs on failure
+        if: failure()
+        run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "${{ matrix.service }}" rustfs
+      - name: Stop the containers
+        if: always()
+        run: docker compose -f test-infra/compose.yaml --profile matrix down -v
```

Change `test-infra/README.md`:

````diff
diff --git a/test-infra/README.md b/test-infra/README.md
index 1904fcd..6ed2096 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -66,6 +66,31 @@ test ends) and `binlog_rotate` (`FLUSH BINARY LOGS`, then a position no other wr
 MariaDB keeps a binlog it just closed until its binlog checkpoint moves on, so the purge test
 retries for up to 5 s.
 
+## The slice-1 E2E suite
+
+`crates/data-avalanche/tests/e2e` runs the binary as a process against one source and RustFS.
+The source is PostgreSQL (`postgres-18`, or `DATA_AVALANCHE_PG_PORT`), or with
+`DATA_AVALANCHE_E2E_SOURCE=mysql` the target `DATA_AVALANCHE_MYSQL` names (the table above, default
+`mysql-84`). A scenario of one source only skips itself on the other. `ci.yml` runs the suite on
+PostgreSQL 18 in the integration job and on `mysql-84`, `mysql-84-nogtid` and `mariadb-118` in
+its `e2e-mysql` job; `weekly.yml` runs it on PostgreSQL 14–17 and on `mariadb-118-binlog`,
+`mariadb-114` and `mariadb-1011`:
+
+```sh
+docker compose -f test-infra/compose.yaml up -d --wait mysql-84 rustfs
+docker compose -f test-infra/compose.yaml --profile init run --rm rustfs-init
+DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=mysql-84 \
+  cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
+```
+
+On MySQL each test makes a database and a user named `e2e_<test>` (the user's binlog dump shows
+the stream is open). Two scenarios change server-wide state: the purge scenarios purge the
+target's older binlogs, and the failover scenario (on `mysql-84-nogtid`) makes its database and
+user on `mysql-84` too, which must be up. The scenarios that pause a server (`docker pause`) need
+the Docker CLI. The faults no server can be asked for (a corrupted compressed transaction, a
+failover behind one address) come from a test-only TCP proxy in the suite (`proxy.rs`), which
+reads the protocol in the clear: the suite's pipelines use `ssl_mode: disable`.
+
 ## GCS: a real bucket, not an emulator
 
 `fake-gcs-server` 1.56.1 rejects `object_store`'s path-style PUT (`400 invalid uploadType`) and
````

- [ ] **Step 2: Run it**

Run: `python3 -c "import yaml; [yaml.safe_load(open(f)) for f in ['.github/workflows/ci.yml', '.github/workflows/weekly.yml']]; print('both parse')"`

Expected: `both parse`.

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml \
  .github/workflows/weekly.yml \
  test-infra/README.md
git commit -m "ci: the slice-1 E2E on MySQL and MariaDB — per change on three targets, weekly on three (Q-M7b-2)"
```

---

## Verified during planning (2026-10-17)

- Every task was built and run in a scratch workspace, one commit per task, before this plan was
  written from those commits. The plan was then replayed from its own code blocks at the base
  commit (`2375c33`), task by task: Step 1 applied, Step 2 run (the failures above are its
  output), Step 3 applied, Step 4 run, the format and lint gates run. For every task the tree
  then matched the verified commit exactly. The test counts above are the replay's.
- `cargo test --workspace` on the replay's final tree: 1091 tests, none failing (M7a ended at
  1073). The per-task workspace runs were not repeated in the replay; there each task's format
  and `clippy --all-targets` gates passed, so every test target built.
- The payload fault was measured before the proxy was written (a probe through the same proxy,
  `mysql-84`). Corrupting one byte of a `TRANSACTION_PAYLOAD`'s compressed body gives no inner
  events, then `BINLOG_PAYLOAD_INVALID: the compressed transaction ending at … yielded 0 of its
  622 bytes of events`, with the event's CRC32 recomputed and with it left wrong alike:
  `mysql_common` 0.37.3 doesn't verify a dump's event checksums, so only the payload guard
  (DA-SRC-12) catches it. Without the fault the proxy is transparent: the control run streamed
  its 2 transactions and 3 rows.
- The source-mysql integration tests pass on `mysql-84`, `mysql-84-nogtid`, `mariadb-118`,
  `mariadb-118-binlog`, `mariadb-114` and `mariadb-1011`. The exception is the three TLS tests on
  `mysql-84`, which read the git-ignored `test-infra/.tls/` that the scratch clone doesn't have.
  Mutation check: with the dump's server check disabled, the file/position IT fails (Task 4).
- The E2E suite on the final tree passes on these targets: PostgreSQL 18, `mysql-84`, `mariadb-114`
  and `mariadb-1011` (33 tests each), and `mysql-84-nogtid` (32, before the types scenario's last
  change; that scenario passed there). On `mariadb-118` and `mariadb-118-binlog`, the five failing
  scenarios passed on rerun once fixed. Their fixes are in the blocks above: MariaDB's
  `max_recursive_iterations` and MariaDB's `JSON` text.
- Task 2's `a_halt_mid_transaction_drops_the_tables_rows_of_it` passes on the code as it stands
  (D-M7b.9): it pins the behaviour M7a's carry item asked about.

## M7b exit checklist

- [ ] A `type: mysql` pipeline runs in the binary. Its first start, restart and lost position
      record the server with the position in `replication/position.json` (Tasks 2, 5).
- [ ] A file/position reconnect to another server halts `BINLOG_SOURCE_CHANGED` (Tasks 4, 7).
- [ ] A pipeline halt shows its code in `Actual::Error` and in the `ERROR` log line (Task 3).
- [ ] The slice-1 E2E suite passes on PostgreSQL 18 and, through `ci.yml`'s `e2e-mysql` job, on
      `mysql-84`, `mysql-84-nogtid` and `mariadb-118`. Through `weekly.yml` it passes on
      `mariadb-118-binlog`, `mariadb-114` and `mariadb-1011`.
- [ ] The payload fault-injection E2E halts with `BINLOG_PAYLOAD_INVALID`, and nothing is lost
      (Task 7).
- [ ] M7a's carry items are done: the identity error, the file/position resume IT, the halted
      table's rows (D-M7b.9), and `re_snapshot` at start through the worker.
- [ ] Every Review Focus line has its passing test.
- [ ] The documents of "Documents this plan changes" are updated.
- [ ] The workspace gates pass: `cargo fmt --all --check`, `cargo clippy --workspace
      --all-targets -- -D warnings`, `cargo clippy -p data-avalanche --features e2e-hooks
      --all-targets -- -D warnings`, `cargo test --workspace`.

## Deviations during execution

None yet.
