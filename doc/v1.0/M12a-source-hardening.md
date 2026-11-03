---
updated: 2026-10-30
domain: Architecture
tags: [v1, implementation-plan, m12, m12a, hardening, source, postgres, mysql, mariadb, failover, position-guard, publication, snapshot, tls, e2e]
resolves: [DA-SRC-13, DA-SRC-14, DA-SRC-15, DA-SRC-17, DA-SRC-18, DA-SRC-19, DA-SRC-20, DA-SRC-21, DA-SRC-22, DA-SRC-24, DA-SRC-25, DA-SRC-26, DA-SRC-29, DA-SRC-30, DA-SNAP-13, DA-SNAP-14, DA-OPS-13, DA-SRC-9, DA-X-15]
---

# M12a — Source Correctness and Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** the first of M12's three parts (Q-M12-4): every open source gap closed (Q-M12-1;
tracker §52). That covers:
- a PostgreSQL failover, detected, and recovered on request without losing the new server's
  changes to the position guard;
- a changed MySQL or MariaDB server in file/position mode, recovered the same way;
- a slot a DBA makes again under `require_existing`, adopted on request;
- MariaDB rollbacks over tracked rows, which no longer halt;
- the publication's blind spots, refused, warned or repaired;
- the snapshot's key, collation and error edges;
- the PostgreSQL edges left from M5;
- the M5 test gaps, with a primary/standby pair in test-infra and a failover E2E on it.

**Architecture:** the work is in the two source crates, plus three primitives below them that the
recoveries share:

- **The source's identity** (Tasks 1–3). `ServerIdentity::Postgres { system_identifier, timeline
  }` joins MySQL's and MariaDB's. The PostgreSQL checkpoint (`replication/slot.json`) records it
  as MySQL's position document does. A start that finds another server either halts the pipeline
  with a code (`WAL_SOURCE_CHANGED`, `BINLOG_SOURCE_CHANGED`) or, when asked
  (`source_change_action` / `binlog_purge_action: re_snapshot`), plans a start from the new
  server's position: `SnapshotReason::SourceChanged`.
- **The marker reset** (Task 1). The position guard keeps the union of the stored marker and each
  commit's position. A promoted replica's LSNs, or another server's file positions, sort below the
  lake's markers, so the guard would skip every change the new server sends. A `TableChangeSet`
  with `reposition` set carries no rows, and its commit stores its own position instead of the
  union. At a `SourceChanged` start the worker commits one per table the lake holds a marker for,
  before it saves the checkpoint that names the new server. A crash in between finds the old
  server recorded again and repeats the reset.
- **A held transaction** (Task 5). A MariaDB transaction whose GTID event lacks
  `FL_TRANSACTIONAL` can log rows that a later `ROLLBACK` or `ROLLBACK TO` undoes. The stream
  holds its items until its end, drops what a rollback undid, and lets the rest out, in order.
  The hold is bounded by `sink.max_transaction_buffer_bytes`.

The rest are local fixes, each pinned by a test.

**Tech Stack:** Rust 1.98.1; no new crate. The test-infra pair is two `postgres:18` services on
tmpfs (profile `replica`); the planning's probes ran on test-infra's PostgreSQL 18, MySQL 8.4 and
MariaDB 11.8.

**Spec:**
- `doc/specs/Configuration_Specification.md`:
  - §9.1 `ssl_mode`;
  - §9.2.1, the PostgreSQL keys: `slot_management`, `slot_on_orphan`, and the new
    `source_change_action`;
  - §9.2.2, the MySQL keys: `replication_type`, `binlog_purge_action`;
  - §11.2, binlog coverage;
  - V-102, V-105, V-108, and the new V-123 and V-124.
- `doc/specs/Data-Flow.md`: Phase 2a (PostgreSQL) and 2b (MySQL) start sequences, Phase 4 (the
  guard), the `TableChangeSet` contract, §Worker Failure.
- `doc/research/Delivery_Guarantee_and_Position_Guard.md` §2 (positions, `union`), §6 (a source
  failover).
- `doc/research/Postgres_Slot_and_Publication_Lifecycle.md` §1–§4 (the slot state machine, the
  publication).
- `doc/research/MySQL_DDL_Classifier.md` (the rename rule), `Operational_and_Scope_Gaps_Batch.md`
  G-26 (`binlog_purge_action`), G-38 (MariaDB).
- `doc/v1.0/00-roadmap.md`: §3.2 (positions), §3.4 (the sink trait), §3.6 (the source
  connector), §4 M12.

**Spec edits (approved with this plan, applied before Task 1):** the patch below, drafted on
scratch copies of the two specs.

- **Config §9.1:** `ssl_mode` says MySQL's `verify-ca` checks the name too.
- **Config §9.2.1:**
  - a new row, `source_change_action: error | re_snapshot` (default `error`, applied, V-123);
  - `slot_management` and `slot_on_orphan` say how a slot a DBA made again is adopted.
- **Config §9.2.2:** `replication_type` and `binlog_purge_action` say a changed server follows
  `binlog_purge_action`.
- **Config:** the rule rows V-123 and V-124, and changelog item 30.
- **Data-Flow:**
  - Phase 2a: the server's identity;
  - Phase 2b: a changed server;
  - the `reposition` field and its guard paragraph;
  - §Worker Failure's line.

`doc/specs/Configuration_Specification.md`:

```diff
diff --git a/doc/specs/Configuration_Specification.md b/doc/specs/Configuration_Specification.md
--- a/doc/specs/Configuration_Specification.md
+++ b/doc/specs/Configuration_Specification.md
@@ -664,7 +664,7 @@ source:
 | `user` | string | yes (resolvable) | — | PR | Required unless `credentials_file` is given (V-010); mutually exclusive with it (V-065). |
 | `password` | string | yes (resolvable) | — | PR | Required unless `credentials_file` is given (V-010); mutually exclusive with it (V-065). Use `${ENV}`. Re-read on reconnect to support rotation. |
 | `credentials_file` | path | yes (resolvable) | — | PR | Alternative to `user`/`password` (§4.6): path to JSON `{user, password}`. Mutually exclusive with them (V-065). Rotating the file's contents + reload rotates the DB credentials without a process restart. |
-| `ssl_mode` | enum | no | `require` | PR | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full`. `allow` is **invalid for MySQL** (V-030). |
+| `ssl_mode` | enum | no | `require` | PR | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full`. `allow` is **invalid for MySQL** (V-030). With MySQL and MariaDB, `verify-ca` also checks the host name, as `verify-full` does: the pinned client can't skip the name check alone (DA-SRC-22), so it fails closed (V-124). |
 | `ssl_cert` / `ssl_key` / `ssl_ca` | PEM/path | no | — | PR | §4.4. |
 | `timeouts.connect` | Duration | no | `10s` | PR | The whole connection setup: TCP, TLS, startup and authentication. |
 | `timeouts.command` | Duration | no | `30s` | PR | |
@@ -682,18 +682,19 @@ source:
 | `slot` | string | yes | — | PR | Logical replication slot name. Charset `^[a-z_][a-z0-9_]{0,62}$` (PG constraint); **unique** per source host+db across pipelines (V-058). |
 | `auto_cleanup` | bool | no | `true` | R | Drop the slot & publication when the pipeline is deleted. Teardown-time policy flag only — consulted at deletion/`id`-change, not on the live path, so a change applies in place with no restart. |
 | `heartbeat` | Duration | no | `60s` | R | StandbyStatusUpdate interval; keeps the connection alive and prevents WAL-lag-as-inactivity. Also paces the stream watchdog: a stream silent for 3 heartbeats while the heartbeat's reads of the slot fail or find no reader is reconnected (DA-SRC-16). |
-| `slot_management` | enum `managed`\|`require_existing` | no | `managed` | PR | `managed`: create the slot if absent, reuse if present, drop on delete per `auto_cleanup`. `require_existing`: never create (DBA-owned slot; error if absent) and **requires `auto_cleanup: false`** (V-103). `research/Postgres_Slot_and_Publication_Lifecycle.md` §2. |
-| `slot_on_orphan` | enum `halt`\|`adopt_resnapshot` | no | `halt` | R | Slot exists but the pipeline has no state for it. `halt`: refuse to start, naming the slot. `adopt_resnapshot`: take ownership and restart from a fresh consistent point, a recovery: the tables are snapshotted as `snapshots.mode` says for one (Data-Flow Phase 0; `initial`/`always` incrementally re-snapshot all tables), and under `never` none is, a warning naming the gap (Q-M6a-1). Resuming from the orphan as if continuous is never offered. (Slot-lifecycle doc §1.1; named `slot_on_orphan` because `slot` is the slot-name string.) |
+| `slot_management` | enum `managed`\|`require_existing` | no | `managed` | PR | `managed`: create the slot if absent, reuse if present, drop on delete per `auto_cleanup`. `require_existing`: never create (DBA-owned slot; error if absent) and **requires `auto_cleanup: false`** (V-103). A slot the DBA creates again after the pipeline's was lost starts past the checkpoint: it is an orphan, adopted or refused as `slot_on_orphan` says. `research/Postgres_Slot_and_Publication_Lifecycle.md` §2. |
+| `slot_on_orphan` | enum `halt`\|`adopt_resnapshot` | no | `halt` | R | Slot exists but the pipeline has no state for it, or (under `require_existing`) a slot made again starts past the pipeline's checkpoint. `halt`: refuse to start, naming the slot and the recovery. `adopt_resnapshot`: take ownership and restart from a fresh consistent point, a recovery: the tables are snapshotted as `snapshots.mode` says for one (Data-Flow Phase 0; `initial`/`always` incrementally re-snapshot all tables), and under `never` none is, a warning naming the gap (Q-M6a-1). Resuming from the orphan as if continuous is never offered. (Slot-lifecycle doc §1.1; named `slot_on_orphan` because `slot` is the slot-name string.) |
+| `source_change_action` | enum `error`\|`re_snapshot` | no | `error` | R | The checkpoint was read on another server than the one the pipeline now reads: another system identifier (another cluster, a restore) or another timeline (a promoted standby). An LSN means something only on its server and timeline (Data-Flow Phase 2a). `error`: halt with `WAL_SOURCE_CHANGED` (operator decides). `re_snapshot`: a recovery from the new server's position: the slot is made there (`managed`) or the DBA's slot there is adopted (`require_existing`), every table's position marker is reset to it, and the tables are snapshotted as `snapshots.mode` says for a recovery (under `never` none is, a warning naming the gap). A checkpoint written before the server was recorded (v0.1.0) is taken as the connected server's (V-123). |
 | `max_wal_lag_bytes` | ByteSize \| null | no | `null` | R | WAL-retention guard (G-22). `data_avalanche_wal_lag_bytes` is always emitted; when set and exceeded the pipeline **halts** to protect the source disk. `null` = alert only. ≥ 64 MiB when set (V-104). |
 
 #### 9.2.2 `source.mysql` (iff `type = mysql`)
 
 | Field | Type | Required | Default | Mutability | Notes |
 |---|---|---|---|---|---|
-| `replication_type` | enum `binlog`\|`gtid` | no | `gtid` | PR | `gtid`: GTID-positioned stream (MySQL `gtid_executed`, MariaDB `gtid_binlog_pos`); position-independent, gap-free, survives a source failover. Requires GTID on the server (MySQL `gtid_mode=ON`; MariaDB `gtid_strict_mode=1`). `binlog`: file/position stream (`COM_BINLOG_DUMP` at `<file>:<pos>`) for servers without GTID. A file/position is only meaningful on the server that wrote it: when the source server changes (failover, restore), the pipeline **halts** with `BINLOG_SOURCE_CHANGED` (`Data-Flow.md` Phase 2b); recover by switching to `gtid` or re-snapshotting. |
+| `replication_type` | enum `binlog`\|`gtid` | no | `gtid` | PR | `gtid`: GTID-positioned stream (MySQL `gtid_executed`, MariaDB `gtid_binlog_pos`); position-independent, gap-free, survives a source failover. Requires GTID on the server (MySQL `gtid_mode=ON`; MariaDB `gtid_strict_mode=1`). `binlog`: file/position stream (`COM_BINLOG_DUMP` at `<file>:<pos>`) for servers without GTID. A file/position is only meaningful on the server that wrote it: when the source server changes (failover, restore), the position is lost and `binlog_purge_action` decides: `error` halts with `BINLOG_SOURCE_CHANGED` (`Data-Flow.md` Phase 2b), `re_snapshot` recovers from the new server's position. MariaDB names a server by `@@server_id` alone, which can repeat, so there a position that is not an event boundary on the connected server (`BINLOG_GTID_POS` is NULL) is a changed server too. |
 | `tinyint1_as_bool` | bool | no | `true` | PR | Map `TINYINT(1)` columns to logical `bool`. When `true`, requires the display-width from the catalog query (channel C); when `false`, `TINYINT(1)` maps as `TINYINT SIGNED` → `Int16`. See `research/Type_Mapping.md`. |
 | `tinyint1_detection` | enum `information_schema`\|`none` | no | `information_schema` | PR | Source of the `TINYINT(1)` display-width signal. `none` disables the detection (all `TINYINT` treated as integer); `information_schema` uses the catalog query (always available via channel C). |
-| `binlog_purge_action` | enum `error`\|`re_snapshot` | no | `error` | R | Resume position purged on the server (G-26). `error`: halt with an error (operator decides). `re_snapshot`: automatic recovery from the server's current position, the tables snapshotted as `snapshots.mode` says for a recovery (`initial`/`always` incrementally re-snapshot; under `never` none is, a warning naming the gap — Q-M6a-1). GTID `replication_type` remains recommended. |
+| `binlog_purge_action` | enum `error`\|`re_snapshot` | no | `error` | R | Resume position purged on the server (G-26), or, with `replication_type: binlog`, recorded on another server. `error`: halt with an error (operator decides). `re_snapshot`: automatic recovery from the server's current position (after a server change, every table's position marker is reset to it), the tables snapshotted as `snapshots.mode` says for a recovery (`initial`/`always` incrementally re-snapshot; under `never` none is, a warning naming the gap — Q-M6a-1). GTID `replication_type` remains recommended. |
 | `server_id` | integer | no | derived from `id` | PR | The replica server id the binlog dump registers with. The source server ends any other dump registered under the same id (measured, S-MY10), so each pipeline needs its own (V-120). Default: derived from the pipeline `id` — the first four bytes of its BLAKE3 hash, little-endian, with the top bit set (2147483648…4294967295, away from the small ids replicas usually take). Set it when that range collides with the server's own replicas. |
 | `heartbeat` | Duration | no | `60s` | R | The binlog heartbeat period (`@master_heartbeat_period`): an idle server sends a heartbeat this often (V-121). Also paces the stream watchdog, as `postgres.heartbeat` does: a stream silent for 3 heartbeats while the heartbeat's checks fail or find no binlog dump of the stream's connection on the server (its connection id in `information_schema.PROCESSLIST`, which a user without `PROCESS` sees for its own connections) is reconnected (DA-SRC-16). |
 
@@ -1277,6 +1278,7 @@ errors.
 | V-119 | `sink.delta.merge_memory_limit` | ≥ 16 MiB when set. | error | pipeline |
 | V-121 | `source.mysql.heartbeat` | Between `1s` and `4294967s` (the server's heartbeat period range). | error | pipeline |
 | V-122 | `globals.cluster.memberlist` gossip timers | `gossip_interval`, `probe_interval`, `probe_timeout` and `push_pull_interval` above zero; `gossip_nodes` and `retransmit_factor` at least 1; `probe_timeout` below `probe_interval`. | error | config |
+| V-123 | `source.postgres.source_change_action` | ∈ {error, re_snapshot}. | error | pipeline |
 
 ### 13.4 Cross-field / conditional
 
@@ -1285,6 +1287,7 @@ errors.
 | V-028 | `sink.iceberg.catalog.glue` | Requires `globals.cloud = aws`. | error | pipeline |
 | V-029 | `sink.iceberg.catalog.rest.auth = sigv4` | Requires `globals.cloud = aws`. | error | pipeline |
 | V-030 | `source.ssl_mode = allow` | Invalid for MySQL. | error | pipeline |
+| V-124 | `source.ssl_mode = verify-ca`, when `source.type = mysql` | The host name is checked too, as with `verify-full` (DA-SRC-22). | warning | pipeline |
 | V-073 | `globals.cluster.lease_renewal_interval` | Must be ≤ `lease_duration / 3` (the holder must get ≥2 renew attempts inside the renewal safety deadline). **Hard error** — promoted from the previously-advisory guidance (Clustering doc §18.2/§19.6, D-8). | error | config |
 | V-033 | `tables.snapshots.overrides` keys | Must be full `schema.table`; globs forbidden. | error | pipeline |
 | V-034 | `tables.snapshots.overrides` keys | Key not in the resolved include/exclude set. | warning | pipeline |
@@ -1622,6 +1625,14 @@ Every deviation, so nothing is silent:
     `binlog_purge_action: re_snapshot`: the recovery's snapshot follows `snapshots.mode`, as
     Data-Flow Phase 0 and Q-M6a-1 decided and the code does; under `never` (the default) there is
     none, and a warning names the gap. The rows said every table is re-snapshotted.
+30. **M12a planning (2026-10-30, Q-M12a-1…4)** — §9.2.1 new `source.postgres.source_change_action`
+    (`error` | `re_snapshot`, applied, **V-123**): a start on another PostgreSQL system or
+    timeline halts with `WAL_SOURCE_CHANGED` or recovers; `slot_management: require_existing`
+    and `slot_on_orphan`: a slot the DBA made again past the checkpoint is an orphan. §9.2.2
+    `replication_type: binlog` and `binlog_purge_action`: a changed server is a lost position,
+    recovered by `re_snapshot` (it halted with no recovery); MariaDB's check by
+    `BINLOG_GTID_POS`. §9.1 `ssl_mode`: MySQL `verify-ca` checks the host name too, warning
+    **V-124**.
 
 None of these change the **decided** catalog-auth schema (hive Thrift `none/ldap/kerberos`;
 rest HTTP `none/bearer/oauth2/sigv4`) — those are reproduced verbatim from
```

`doc/specs/Data-Flow.md`:

```diff
diff --git a/doc/specs/Data-Flow.md b/doc/specs/Data-Flow.md
--- a/doc/specs/Data-Flow.md
+++ b/doc/specs/Data-Flow.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-10-27
+updated: 2026-10-30
 domain: Architecture
 tags: [architecture, data-flow, pipeline, cdc, iceberg, delta-lake, multi-sink, arrow, snapshot, replication]
 sources: []
@@ -191,6 +191,24 @@ Replication connection (replication protocol, not SQL)
 - `FULL`: UPDATE and DELETE carry the complete old row image. Required for tables without a primary key.
 - `NOTHING`: DELETE events are unusable — the sink cannot identify which row to delete. Pipelines should reject tables with `REPLICA IDENTITY NOTHING`.
 
+**The server's identity (DA-SRC-20).** An LSN means something only on the server and the timeline
+that wrote it. A start reads the server's system identifier (`pg_control_system()`) and its
+timeline (the current WAL file's, `pg_walfile_name(pg_current_wal_lsn())`), callable by the
+replication role, and the checkpoint records them. A physical standby has its primary's system
+identifier, so a failover to it shows as a new timeline; its slots stay behind unless synced.
+
+```
+Start with a checkpoint
+├─► no identity recorded (a checkpoint from before it was) → take the connected server's
+├─► the same system and timeline → the slot checks below, as before
+└─► another system or timeline → source.postgres.source_change_action:
+    ├─► error (default): halt the PIPELINE, WAL_SOURCE_CHANGED: "the checkpoint <lsn> was
+    │     read on <old>, and the pipeline now reads <new>"
+    └─► re_snapshot: a recovery from the new server's position — the slot is made there
+          (managed) or the DBA's adopted (require_existing; refused if absent), every table's
+          marker is reset (Phase 4, reposition), the tables snapshotted as snapshots.mode says
+```
+
 **Protocol and client (probe-verified, `prototypes/source-capability/` S-PG0…8):**
 - The client crate (`pgwire-replication`) is transport only; **the pgoutput decoder is ours**.
 - **`proto_version '1'`**, which the pinned crate sends, with no `streaming` option. A large
@@ -301,11 +319,14 @@ Connect
 │     record the identity and SHOW BINARY LOG STATUS (MySQL 8.4) / SHOW BINLOG STATUS (MariaDB)
 │     as the start position; Phase 1 watermarks use the same statement
 ├─► stored position present:
-│   ├─► identity differs from the recorded one → halt the PIPELINE (BINLOG_SOURCE_CHANGED):
-│   │     "binlog position <file>:<pos> was recorded on server <old>, now connected to <new>;
-│   │      file/position cannot be carried across servers — switch to replication_type: gtid
-│   │      or re-snapshot". No automatic recovery (a failover under file/position mode
-│   │      would otherwise silently skip or repeat transactions).
+│   ├─► identity differs from the recorded one, or (MariaDB, whose @@server_id can repeat)
+│   │   BINLOG_GTID_POS(<file>, <pos>) is NULL there → the position is lost; never resumed (a
+│   │   failover under file/position mode would silently skip or repeat transactions), and
+│   │   binlog_purge_action decides:
+│   │   ├─► error: halt the PIPELINE (BINLOG_SOURCE_CHANGED): "binlog position <file>:<pos>
+│   │   │     was recorded on server <old>, now connected to <new>; …"
+│   │   └─► re_snapshot: a recovery from the new server's position; every table's marker is
+│   │         reset (Phase 4, reposition), the tables snapshotted as snapshots.mode says
 │   ├─► file no longer in SHOW BINARY LOGS → binlog_purge_action (error | re_snapshot, G-26)
 │   └─► COM_BINLOG_DUMP at <file>:<pos>
 └─► Event loop as above (GTID events ignored); the position after each XID_EVENT is
@@ -436,6 +457,7 @@ TableChangeSet {
     deletes:    Vec<RecordBatch>,   // ≤1 row per key: identifier (+ before-image partition columns)
     position:   PositionRange,      // per-table source position covered (guard / HWM)
     split:      Option<SplitInfo>,  // oversized-transaction slice marker (checkpoint pinning)
+    reposition: bool,               // a source-change recovery: no rows; the marker becomes position.end
 }
 ```
 
@@ -504,7 +526,12 @@ fence still applies. A split slice (`split` set) ends at its transaction's
 pinned start, which the stored position equals from the first slice on; a slice is skipped only
 when the stored position is strictly past that start (the transaction's commit, or a later one, has
 landed). On commit, the new value is the `union` of the prior value
-and the flush's per-table position. The pipeline **resume checkpoint** persisted to state storage is the
+and the flush's per-table position. A **reposition** change set (`reposition` set, no rows, no truncate, no split) is never skipped
+either, and its commit stores its own position instead of the union: after a source change
+(another PostgreSQL system or timeline, another MySQL server in file/position mode) the new
+server's positions may sort below the stored markers, and the union would skip its changes.
+A recovery from a changed source commits one per table the sink holds a marker for, fenced like
+any commit, before the checkpoint naming the new server is saved and the stream starts. The pipeline **resume checkpoint** persisted to state storage is the
 **laggard** across all tracked tables (PG `min` / MySQL set-intersection); tables ahead skip via the
 guard or are absorbed by idempotency. The epoch rejects a zombie writer; the position skips done work.
 See `research/Delivery_Guarantee_and_Position_Guard.md` (G-14).
@@ -784,7 +811,7 @@ Node dies (SWIM detects via failed probes → marks dead)
 └─► New owner starts each inherited pipeline (Phase 0):
     ├─► Reads state from cloud storage
     └─► Resumes from last confirmed_flush_lsn / persisted MySQL position
-        (file/position mode: a changed source server halts, BINLOG_SOURCE_CHANGED)
+        (a changed source server: source_change_action / binlog_purge_action, Phase 2a/2b)
         (worst-case replay = time since last checkpoint write)
 ```
 
```


## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`, which includes the staleness test of the generated reference
    (`xtask/tests/docs.rs`): a task that changes `FIELD_DOCS` or the rule registry runs
    `cargo xtask docs` and commits the pages it rewrites.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`; the source crates depend on `core` and `config`, never on the pipeline.
  `testkit` is a dev-dependency only. No new crate.
- **A recovery never guesses.** Every source change and every lost position halts by default,
  with a code or a message naming the key that recovers it. A recovery's snapshot follows
  `snapshots.mode` (under `never`, none, with a warning), as config §9.2.1 says for a lost slot.
- **State compatibility:** a `replication/slot.json` written by v0.1.0 (no `server`) still
  reads. Its checkpoint is adopted, and the next save records the server
  (`#[serde(default, skip_serializing_if = "Option::is_none")]`). No `SCHEMA_VERSION` bump.
- **Halt codes are the log's contract** (Q-M7b-4): `WAL_SOURCE_CHANGED` is new, and
  `BINLOG_SOURCE_CHANGED` keeps its prefix. The E2E suite finds a halt by
  `pipeline in ERROR … code="<CODE>"`.
- **Test gates:**
  - Plain `cargo test` stays hermetic.
  - The source ITs need `DATA_AVALANCHE_IT=1` (`test-infra/up.sh`). The MySQL ones run on the
    server `DATA_AVALANCHE_MYSQL` names: a task that touches the MySQL source (3, 5, 6, 7, 8, 10)
    runs them on `mysql-84`, `mariadb-118` and `mariadb-118-binlog`, and the Exit adds
    `mysql-84-nogtid`.
  - The TLS ITs read test-infra's `.tls`, so they run from the repository's checkout only.
  - The failover E2E needs `DATA_AVALANCHE_PG_REPLICA=55420:55421` and the `replica` profile
    (Task 11). CI's integration job starts the pair fresh.
- **Public-API conventions:** public types and functions are re-exported at the crate root;
  errors are `thiserror` types (none new).
- Code blocks are the verified content, already `rustfmt`-formatted. Every block is a `git
  apply`-ready diff, one per file; a step's diffs apply in the order given, after the earlier
  steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt a person running a source through a failover or a DBA's
change. Each is pinned by a test in the task after the arrow.

1. **A failover to a replica that lagged behind the lake.** The new server's LSNs repeat ones
   the lake has marked. A recovery must stream its changes, never skip them, under every
   `snapshots.mode`, `never` included → Task 1
   (`a_source_change_repositions_every_table_before_streaming`), and Task 11
   (`a_promoted_replica_halts_the_pipeline_then_re_snapshots_on_request`, the replica's LSN
   asserted below the marker).
2. **A v0.1.0 state upgraded in place.** The slot document has no `server`. The first start must
   resume as before and record the server, not halt as a source change → Task 1
   (`a_slot_document_records_its_server`), Task 2 (`a_checkpoint_from_another_server`, "a
   checkpoint with no server").
3. **A MariaDB transaction that mixes InnoDB and MyISAM rows, with savepoints, rolled back in
   part.** The rows that stand must land exactly once, in order, and the undone ones never. A
   MyISAM row stands whatever the rollback → Task 5 (`a_rollback_drops_the_rows_it_undid`, the
   nested case; `a_rollback_takes_out_what_it_undid`).
4. **A DBA who recreates a lost slot under `require_existing` and restarts.** The pipeline must
   not resume from it as if nothing was lost. It refuses, naming `slot_on_orphan`, or adopts the
   slot and re-snapshots → Task 4 (`a_slot_made_again_under_require_existing_is_adopted_on_request`,
   `require_existing_never_creates`).
5. **A DBA's publication that filters rows or columns.** Rows would be missing silently: it
   must be refused under `require_existing` and repaired under `managed` → Task 9
   (`a_publication_s_blind_spots_are_refused_warned_or_repaired`).

## Decisions and rulings

The questions Oleksii answered while planning (roadmap §9.1):

| ID | Decision |
|---|---|
| Q-M12-1…7 | The milestone's: every gap processed; the clouds; where load runs; the three parts; the weekly matrix met; a PostgreSQL source change halts with a code (Q-M12-6); the snapshot job's `truncate` (M12b) |
| Q-M12a-1 | DA-SRC-20's recovery is a key, `source.postgres.source_change_action: error \| re_snapshot`: default `error`, applied without a restart (V-123), as `binlog_purge_action` is |
| Q-M12a-2 | DA-SRC-29: a changed MySQL server in file/position mode follows `binlog_purge_action` |
| Q-M12a-3 | DA-SRC-30: under `require_existing`, a DBA's slot made again past the checkpoint is adopted under `slot_on_orphan: adopt_resnapshot` |
| Q-M12a-4 | DA-SRC-22: no `mysql_async` patch. `verify-ca` stays fail-closed on MySQL, V-124 warns, and the code names the re-check at each `mysql_async` upgrade |

The rulings this plan makes (roadmap D-IMPL-24):

| ID | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M12a.1 | **A PostgreSQL server is its system identifier and its timeline.** They come from `pg_control_system().system_identifier` and the first eight hex digits of `pg_walfile_name(pg_current_wal_lsn())`; on a standby, which writes no WAL, `pg_control_checkpoint().timeline_id`. All three are open to a plain `LOGIN REPLICATION` role. They are recorded in `replication/slot.json`'s `server`, and a checkpoint without one is adopted | A promotion keeps the identifier and moves to a new timeline at once (measured on PostgreSQL 18); the identifier alone misses a failover, the timeline alone misses a restore elsewhere | Task 1 (`a_slot_document_records_its_server`), Task 2 (`the_identity_is_the_system_and_the_timeline`, `a_checkpoint_from_another_server`) |
| D-M12a.2 | **The marker reset is a change set, not a sink method.** `TableChangeSet.reposition`: no rows, no truncate, no split (`validate`); the guard never skips it (`CommitMarker::covers`); a sink stores `position.end` instead of the union; it is fenced like any commit. The worker sends one per table the lake holds a marker for, before it saves the checkpoint naming the new server | The `Sink` trait keeps one commit path, so the fence, the retries and the stats need nothing new. Saving the checkpoint last makes a crash between the two repeat the reset, which is idempotent | Task 1 (`the_guard_never_skips_a_reposition`, `a_reposition_replaces_the_marker` in both sinks, `a_source_change_repositions_every_table_before_streaming`) |
| D-M12a.3 | **`WAL_SOURCE_CHANGED` names both servers and the two ways out.** Under `re_snapshot`, the slot is made on the new server (`managed`; one already there is dropped first). Under `require_existing` the DBA's slot is adopted, and an absent or invalidated one is refused with the same code. A warning names both servers | The logical slot doesn't come over with a physical failover (no slot sync); a DBA may have made one | Task 2 (`a_source_change_starts_from_the_new_server`, `a_checkpoint_from_another_server`) |
| D-M12a.4 | **A changed MySQL server is a lost position** (Q-M12a-2): `error` halts with `BINLOG_SOURCE_CHANGED`, naming `binlog_purge_action: re_snapshot`; `re_snapshot` plans `Snapshot { SourceChanged }` with a warning. On MariaDB, whose `server_id` repeats across servers, a file position is also checked by `BINLOG_GTID_POS(file, pos)`: NULL means another server while the server still holds the file (`SHOW BINARY LOGS`); for a purged file it is the purge's to report. The check runs at the start and at the dump's open | Another server's file positions are another series, so the union would error (`DifferentBinlogSeries`); the reset applies. `BINLOG_GTID_POS` is NULL for an offset off an event boundary, a missing file or an offset past the end (measured on 11.8). What stays: a coincidental boundary on a server with the same `server_id` and file names, which the docs name; GTID mode is exact | Task 3 (`a_position_off_an_event_boundary_is_another_server_s`, its case "a file purged since is the purge's to report", `a_checkpoint_from_another_server`, `a_mariadb_position_off_an_event_boundary_is_another_server_s`) |
| D-M12a.5 | **A slot made again under `require_existing`** (Q-M12a-3): with `confirmed_flush_lsn` past the checkpoint it is `Adopt(PositionLost)` under `adopt_resnapshot`, and refused under `halt`, naming `slot_on_orphan`. `SlotAction::Adopt` carries its reason. The refusals say `slot_on_orphan`, not `slot.on_orphan` (DA-OPS-13) | The changes between the checkpoint and the new slot are gone: only a recovery is correct | Task 4 (`a_slot_made_again_under_require_existing_is_adopted_on_request`, `require_existing_never_creates`, `the_help_names_every_source`) |
| D-M12a.6 | **A MariaDB transaction that can log undone rows is held.** That is one whose GTID event is neither standalone nor `FL_TRANSACTIONAL` (0x04). `ROLLBACK` drops its held rows; ``ROLLBACK TO `s` `` drops those after the savepoint; a non-InnoDB tracked table's row among the undone ones halts, as before. The engines are read from `information_schema.TABLES` when the stream opens. Past `sink.max_transaction_buffer_bytes` the hold lets go: a later rollback halts as before | Measured on MariaDB 11.8 (ROW): a non-transactional table's rows go in a group of their own, first; the transactional group then logs the undone rows and the rollback. A group of InnoDB tables alone never logs undone rows. The bound keeps memory as the large-transaction split keeps it | Task 5 (`a_mariadb_transaction_says_whether_it_can_roll_back_logged_rows`, `a_rollback_takes_out_what_it_undid`, `a_rollback_drops_the_rows_it_undid`) |
| D-M12a.7 | **The MySQL detection edges:** a Galera node (`wsrep_on = ON`, read only while `log_slave_updates` is off) is refused as a replica that doesn't log its peers' writes. A column renamed by case alone keeps its cached name (`keep_names`): to MySQL it is the same column, and the diff notes it (`NoTargetEffect`) | A Galera node has no replica connection for `SHOW ALL REPLICAS STATUS` to show. The Delta sink applies no rename, so a case-only one would halt the table | Task 6 (`the_binlog_must_carry_the_source_database`, `column_names_compare_without_case`, `a_change_of_case_alone_keeps_the_column_s_name`) |
| D-M12a.8 | **A read error of one table halts that table.** PostgreSQL: SQLSTATE class 22, 42883, 42804, 42P22, 42704, 42501. MySQL: 1142, 1143, 1267, 1271, 1253, 1273, 1292, 1366. Each gives `TableHalt NotReplicable` "its snapshot read fails: …". A lost `SELECT` (42501) is the table's | A restart reads the table the same way, so ending the pipeline loops it; the other tables are fine | Task 7 (`a_read_only_this_table_fails_halts_the_table`, both sources) |
| D-M12a.9 | **A key that can't resume a cursor is refused:** a `bit`, `varbit` or hex-bytes (PostGIS) identifier column gives `IDENTIFIER_NOT_USABLE`, unless `column_type_overrides` reads it as `string` | Its text isn't valid input for its type, so the second chunk failed | Task 7 (`a_bit_key_is_refused_unless_read_as_text`) |
| D-M12a.10 | **A wiped state baselines against the sink table:** the columns the sink table lacks are added (evolved, or `EvolutionDisabled`); a key the sink table lacks halts the table (`SchemaMismatch`). Types aren't compared | `hop2_schema` is lossy under overrides, so a type compare would halt good tables; a real mismatch still fails the commit (`SINK_REJECTED`) | Task 8 (`a_wiped_state_baselines_against_the_sink_table`, `a_sink_table_without_the_key_column_halts`) |
| D-M12a.11 | **The snapshot's collation edges:** a key literal is collated in its column's own collation, named with its schema (`KeyCollation::Own { name, qualified }`). A key whose index sorts it in another collation is warned. A chunk of 0 rows is refused. An ENUM or SET key whose members moved (not appended) is a change under the snapshot. What stays: no byte budget per chunk (`chunk_size` per table instead, documented); `E''` literals can't occur (the session pins `standard_conforming_strings`) | A domain with its own collation gave 42P22 from the second chunk; a chunk of no rows asked ends a table early; an ENUM key sorts by member index | Task 8 (`a_domain_key_declared_with_another_collation_resumes`, `text_keys_page_in_one_deterministic_collation`, `an_enum_key_s_moved_members_are_seen`) |
| D-M12a.12 | **The publication's shape** is read at each start. Under `require_existing` or `FOR ALL TABLES`, a refusal names the `ALTER PUBLICATION` that fixes it for a row filter or column list on a tracked table (PG 15+) or for stored generated columns (PG 18). Untracked tables are warned. Under `managed` the pipeline repairs it (`SET TABLE`, `DROP TABLES IN SCHEMA`, `publish_generated_columns = none`) and re-snapshots. `publish_via_partition_root` is refused only for a tracked partition the publication doesn't name itself | Each of these drops changes silently. The managed publication is the pipeline's to set | Task 9 (`a_publication_s_blind_spots_are_refused_warned_or_repaired`) |
| D-M12a.13 | **DA-SRC-17's items:** fixed are every table-set error reported (not the first), the server's detail and hint in an error, a standby refused before PostgreSQL 16 and warned from 16, a bounded call's wait for the connection inside its deadline, the cache's guard dropped before the call (a timeout's `abandon` takes it again), a year past PostgreSQL's range. Recorded as not reachable, with the reason in the tracker: loose parsing, types matched by name, cyclic domains, `convert`'s scale, `"char"` above 127 on PG 14, `char(n)` padding, interval narrowing, the dropped-orphan race | Each fixed item is reachable from a real server; the others are not, or are by design | Task 10 (`a_refusal_names_every_error`, `a_server_error_keeps_its_detail_and_hint`, `a_standby_is_refused_before_16`, `a_bounded_call_waits_for_the_cache_within_its_budget`, `a_call_on_a_frozen_connection_times_out_and_frees_the_cache`, `a_year_past_postgresql_s_range_is_refused`) |
| D-M12a.14 | **DA-SRC-9:** one catalog connection per pipeline serves channel C, the chunks and the re-queries (batched per commit). V-077 warns; the throughput is measured in M12c's load runs | A pool would serve a rare path (`re_query` under heavy TOAST churn) at the cost of the connection budget every pipeline pays | The tracker (§53) |
| D-M12a.15 | **The failover E2E's pair is single-use** (`test-infra` profile `replica`, on tmpfs). The test fails with a hint to recreate it when the replica isn't in recovery. CI starts it fresh | A promotion can't be undone in place; a skip would hide a broken pair in CI | Task 11 (`a_promoted_replica_halts_the_pipeline_then_re_snapshots_on_request`) |

**Measured while planning** (2026-10-30, recorded in the tasks):

- Every task was built in a scratch workspace on this branch's base, test first. Each task's test
  stage was committed on its own and run (Step 2's output), then its implementation (Step 4's).
- **The replay.** The plan's diff blocks, applied in order with `git apply` to the plan commit
  (this branch's base, the roadmap, README and tracker §52 edits on it) and the spec patch, give a
  tree identical to the last stage, and the spec blocks give the patched specs: 126 blocks, no
  fuzz. Each stage ran the suites of every crate its task touches, with `DATA_AVALANCHE_IT=1`
  (the MySQL source on `mysql-84`, `mariadb-118` and `mariadb-118-binlog`).
- **The last stage's full run:** `cargo fmt`, both clippy runs and 1386 workspace tests pass; with
  `DATA_AVALANCHE_IT=1`, 1370 (the `tls::` ITs skipped: no `.tls` in the scratch workspace); the
  MySQL source's 148 on `mysql-84-nogtid`, `mariadb-118` and `mariadb-118-binlog`; the E2E suite,
  48 scenarios, on PostgreSQL 18 (with a fresh primary/standby pair), `mysql-84`,
  `mysql-84-nogtid` and `mariadb-118`.
- **A PostgreSQL 18 promotion** (a `pg_basebackup` standby): it kept the primary's system
  identifier (`7692239610332024875`), and `pg_control_checkpoint().timeline_id` and the WAL
  file's prefix were 2 at once. The logical slot made on the primary was absent on the promoted
  server.
- **MariaDB 11.8 rollbacks** (`binlog_format=ROW`): the MyISAM rows came first, in a group of
  their own (`COMMIT`). The InnoDB group, its GTID event without `FL_TRANSACTIONAL`, logged the
  undone rows, then ``ROLLBACK TO `s` `` (the savepoint logged) or `ROLLBACK` (the savepoint
  before its first logged row).
- **`BINLOG_GTID_POS`** on MariaDB 11.8: `(f, 344)` (the file's end) and `(f, 4)` answer
  `0-1-20505`; `(f, 300)` (inside an event), a missing file and `(f, 99999)` answer NULL; a
  rotated file's end answers.
- **Mutation checks**, each run against its test:
  - Task 1: the worker without the reset loses the new server's row (the guard skips it).
  - Task 3: the boundary check off fails both MariaDB ITs.
  - Task 5: no hold gives the old halt.
  - Task 8: the baseline off fails `a_wiped_state_baselines_against_the_sink_table`.
  - Task 11: with the reset disabled, the failover E2E times out at the streamed row: the
    snapshot's rows 11–12 land, and row 13 is skipped.
- **Found by the full integration runs, fixed before presenting** (each in its task, with a test):
  - MariaDB in file mode: `BINLOG_GTID_POS` is NULL for a purged file too, which read as another
    server (`binlog_purge.rs` on `mariadb-118-binlog`). The check now asks `SHOW BINARY LOGS`
    first (Task 3).
  - `bounded` kept the connection cache's guard across the call, so a timeout's `abandon`
    deadlocked and a frozen server was never reconnected to (the E2E
    `a_source_that_stops_answering_is_reconnected_to`). The guard is dropped before the call;
    `a_call_on_a_frozen_connection_times_out_and_frees_the_cache` hung 5 s before the fix (Task 10).
  - DA-X-15, a race in the jobs test that predates M12a (2 of 30 runs failed on the base, 7 of 30
    under load): it drained before the copy's last, empty, chunk marked the table done. The test
    waits for that now: 30 of 30 (Task 11).

## File Structure

| File | Task | Change |
|---|---|---|
| `.github/workflows/ci.yml` | 11 | modify |
| `crates/data-avalanche-api/src/pipelines.rs` | 1 | modify |
| `crates/data-avalanche-config/src/build.rs` | 2 | modify |
| `crates/data-avalanche-config/src/diag.rs` | 10 | modify |
| `crates/data-avalanche-config/src/enums.rs` | 2 | modify |
| `crates/data-avalanche-config/src/field_docs.rs` | 2, 3, 4, 6 | modify |
| `crates/data-avalanche-config/src/merge.rs` | 2 | modify |
| `crates/data-avalanche-config/src/mutability.rs` | 2 | modify |
| `crates/data-avalanche-config/src/raw.rs` | 2 | modify |
| `crates/data-avalanche-config/src/registry.rs` | 2, 6 | modify |
| `crates/data-avalanche-config/src/resolved.rs` | 2 | modify |
| `crates/data-avalanche-config/src/rules/cross.rs` | 6 | modify |
| `crates/data-avalanche-config/tests/rules/cross.rs` | 6 | modify |
| `crates/data-avalanche-config/tests/rules/domain.rs` | 2 | modify |
| `crates/data-avalanche-core/src/change_set.rs` | 1 | modify |
| `crates/data-avalanche-core/src/sink.rs` | 1 | modify |
| `crates/data-avalanche-core/src/source.rs` | 1, 2 | modify |
| `crates/data-avalanche-pipeline/src/buffer.rs` | 1 | modify |
| `crates/data-avalanche-pipeline/src/checkpoint.rs` | 1 | modify |
| `crates/data-avalanche-pipeline/src/decide.rs` | 1 | modify |
| `crates/data-avalanche-pipeline/src/jobs.rs` | 11 | modify |
| `crates/data-avalanche-pipeline/src/schema_change.rs` | 8 | modify |
| `crates/data-avalanche-pipeline/src/test_support.rs` | 1 | modify |
| `crates/data-avalanche-pipeline/src/worker.rs` | 1, 8 | modify |
| `crates/data-avalanche-sink-delta/src/sink.rs` | 1 | modify |
| `crates/data-avalanche-sink-delta/src/test_support.rs` | 1 | modify |
| `crates/data-avalanche-sink-delta/tests/sink/common.rs` | 1 | modify |
| `crates/data-avalanche-sink-delta/tests/sink/keys.rs` | 11 | create |
| `crates/data-avalanche-sink-delta/tests/sink/local.rs` | 1 | modify |
| `crates/data-avalanche-sink-delta/tests/sink/main.rs` | 11 | modify |
| `crates/data-avalanche-source-mysql/src/binlog.rs` | 5 | modify |
| `crates/data-avalanche-source-mysql/src/catalog.rs` | 10 | modify |
| `crates/data-avalanche-source-mysql/src/connect.rs` | 6 | modify |
| `crates/data-avalanche-source-mysql/src/connector.rs` | 6 | modify |
| `crates/data-avalanche-source-mysql/src/lib.rs` | 3, 6 | modify |
| `crates/data-avalanche-source-mysql/src/schema.rs` | 6 | modify |
| `crates/data-avalanche-source-mysql/src/server.rs` | 6 | modify |
| `crates/data-avalanche-source-mysql/src/snapshot.rs` | 7, 8 | modify |
| `crates/data-avalanche-source-mysql/src/start.rs` | 3 | modify |
| `crates/data-avalanche-source-mysql/src/stream.rs` | 3, 5 | modify |
| `crates/data-avalanche-source-mysql/tests/mysql/schema.rs` | 6 | modify |
| `crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs` | 8 | modify |
| `crates/data-avalanche-source-mysql/tests/mysql/start.rs` | 3 | modify |
| `crates/data-avalanche-source-mysql/tests/mysql/stream.rs` | 5 | modify |
| `crates/data-avalanche-source-pg/src/catalog.rs` | 7, 8 | modify |
| `crates/data-avalanche-source-pg/src/connect.rs` | 10 | modify |
| `crates/data-avalanche-source-pg/src/connector.rs` | 2, 8, 10 | modify |
| `crates/data-avalanche-source-pg/src/error.rs` | 10 | modify |
| `crates/data-avalanche-source-pg/src/slot.rs` | 2, 4, 9 | modify |
| `crates/data-avalanche-source-pg/src/snapshot.rs` | 7, 8 | modify |
| `crates/data-avalanche-source-pg/src/tls.rs` | 11 | modify |
| `crates/data-avalanche-source-pg/src/value.rs` | 10 | modify |
| `crates/data-avalanche-source-pg/tests/pg/slot.rs` | 2, 4, 9 | modify |
| `crates/data-avalanche-source-pg/tests/pg/snapshot.rs` | 8 | modify |
| `crates/data-avalanche-source-pg/tests/pg/tls.rs` | 11 | modify |
| `crates/data-avalanche-state/src/pipeline.rs` | 1 | modify |
| `crates/data-avalanche-testkit/src/fake_source.rs` | 1 | modify |
| `crates/data-avalanche-testkit/src/memory_sink.rs` | 1 | modify |
| `crates/data-avalanche/src/cli.rs` | 4 | modify |
| `crates/data-avalanche/tests/e2e/failover.rs` | 11 | create |
| `crates/data-avalanche/tests/e2e/harness.rs` | 11 | modify |
| `crates/data-avalanche/tests/e2e/main.rs` | 11 | modify |
| `doc/analysis/CDC_Design_Gap_Analysis_v1.md` | 12 | modify |
| `doc/v1.0/README.md` | 11 | modify |
| `docs/src/content/docs/configuration/sources.md` | 6, 12 | modify |
| `docs/src/content/docs/reference/configuration.md` | 2, 3, 4, 6 | modify |
| `docs/src/content/docs/reference/validation-rules.md` | 2, 6 | modify |
| `docs/src/content/docs/runbooks/position-lost.mdx` | 12 | modify |
| `docs/src/content/docs/runbooks/source-privileges.mdx` | 12 | modify |
| `test-infra/README.md` | 11 | modify |
| `test-infra/compose.yaml` | 11 | modify |
| `test-infra/down.sh` | 11 | modify |
| `test-infra/postgres-primary.sh` | 11 | create |
| `test-infra/postgres-replica.sh` | 11 | create |
| `test-infra/postgres-tls.sh` | 11 | modify |

---

### Task 1: The source change's vocabulary and the marker reset (`core`, `state`, the sinks, `pipeline`, `testkit`)

**Files:**
- Modify: `crates/data-avalanche-api/src/pipelines.rs`, `crates/data-avalanche-core/src/change_set.rs`, `crates/data-avalanche-core/src/sink.rs`, `crates/data-avalanche-pipeline/src/checkpoint.rs`, `crates/data-avalanche-pipeline/src/decide.rs`, `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-state/src/pipeline.rs`, `crates/data-avalanche-testkit/src/memory_sink.rs`, `crates/data-avalanche-core/src/source.rs`, `crates/data-avalanche-pipeline/src/buffer.rs`, `crates/data-avalanche-pipeline/src/test_support.rs`, `crates/data-avalanche-sink-delta/src/sink.rs`, `crates/data-avalanche-sink-delta/src/test_support.rs`, `crates/data-avalanche-testkit/src/fake_source.rs`
- Test: `crates/data-avalanche-sink-delta/tests/sink/common.rs`, `crates/data-avalanche-sink-delta/tests/sink/local.rs`

**Interfaces:**
- Consumes: `TableChangeSet`, `CommitMarker::covers`, `Sink::commit` (M3, M4); `SlotDoc`,
  `Checkpoint` (M2, M6a); `FakeSource::set_server` (M7b).
- Produces:
  - `ServerIdentity::Postgres { system_identifier: u64, timeline: u32 }`, its `Display`
    (`PostgreSQL system 7692239610332024875 on timeline 2`);
  - `SnapshotReason::SourceChanged`;
  - `TableChangeSet.reposition: bool`, refused by `validate` with rows, a truncate or a split;
  - `SlotDoc.server: Option<ServerIdentity>`, carried by `checkpoint::Store` both ways;
  - in the worker: `async fn reposition(core, tuning, schema, at: &SourcePosition)` and the
    `SourceChanged` start step;
  - `FakeSource::fail_over(server: ServerIdentity, wal_end: Lsn)`.

`Plan::Fresh(SourceChanged)` is a recovery, as `PositionLost` and `AdoptedOrphan` are: a
snapshot in progress starts over, and `never` warns, naming the server change. A sink stores a
reposition's `position.end` as the marker. The memory sink and the Delta sink do the same, and
an empty reposition commits as a marker-only commit (M4's path).

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-api/src/pipelines.rs`:

```diff
diff --git a/crates/data-avalanche-api/src/pipelines.rs b/crates/data-avalanche-api/src/pipelines.rs
index 5705307..39e844c 100644
--- a/crates/data-avalanche-api/src/pipelines.rs
+++ b/crates/data-avalanche-api/src/pipelines.rs
@@ -434,6 +434,7 @@ mod tests {
         docs.save_slot(&SlotDoc {
             slot_name: "orders_slot".into(),
             confirmed_flush_lsn: lsn,
+            server: None,
         })
         .await
         .unwrap();
```

`crates/data-avalanche-core/src/change_set.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/change_set.rs b/crates/data-avalanche-core/src/change_set.rs
index 66a0185..8e44a49 100644
--- a/crates/data-avalanche-core/src/change_set.rs
+++ b/crates/data-avalanche-core/src/change_set.rs
@@ -279,6 +279,7 @@ mod tests {
             },
             split: None,
             snapshot: false,
+            reposition: false,
         }
     }
 
@@ -376,6 +377,32 @@ mod tests {
         }
     }
 
+    #[test]
+    fn a_reposition_carries_nothing() {
+        let mut c = cs(vec![], vec![]);
+        c.reposition = true;
+        c.validate().unwrap();
+        let refused = |f: fn(&mut TableChangeSet)| {
+            let mut c = cs(vec![], vec![]);
+            c.reposition = true;
+            f(&mut c);
+            detail(c)
+        };
+        for d in [
+            refused(|c| c.upserts = vec![upserts(&[1], None)]),
+            refused(|c| c.deletes = vec![deletes(&[Some(2)])]),
+            refused(|c| c.truncate = true),
+            refused(|c| {
+                c.split = Some(SplitInfo {
+                    txn_start: SourcePosition::Pg(Lsn(20)),
+                    slice: 1,
+                });
+            }),
+        ] {
+            assert!(d.contains("a reposition carries no rows"), "{d}");
+        }
+    }
+
     #[test]
     fn the_range_must_not_run_backwards() {
         let mut c = cs(vec![], vec![]);
```

`crates/data-avalanche-core/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
index 12ddb93..3f4382d 100644
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -374,6 +374,7 @@ mod tests {
             },
             split,
             snapshot: false,
+            reposition: false,
         }
     }
 
@@ -489,6 +490,14 @@ mod tests {
         assert!(!marker(1, 500).covers(&chunk).unwrap());
     }
 
+    /// A reposition replaces the marker: the marker never covers it, however far past it is.
+    #[test]
+    fn the_guard_never_skips_a_reposition() {
+        let mut moved = change_set(20, None);
+        moved.reposition = true;
+        assert!(!marker(1, 500).covers(&moved).unwrap());
+    }
+
     #[test]
     fn a_slice_is_skipped_only_once_its_transaction_has_landed() {
         // A slice of a transaction that started at 90 and commits at 110.
```

`crates/data-avalanche-pipeline/src/checkpoint.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/checkpoint.rs b/crates/data-avalanche-pipeline/src/checkpoint.rs
index dc224a1..28ddd1d 100644
--- a/crates/data-avalanche-pipeline/src/checkpoint.rs
+++ b/crates/data-avalanche-pipeline/src/checkpoint.rs
@@ -159,6 +159,15 @@ mod tests {
                 intent: false
             })
         );
+        let on = Checkpoint {
+            position: SourcePosition::Pg(Lsn(0x300)),
+            server: Some(ServerIdentity::Postgres {
+                system_identifier: 7692239610332024875,
+                timeline: 2,
+            }),
+        };
+        store.doc(&on).unwrap().save(&d).await.unwrap();
+        assert_eq!(store.load(&d).await.unwrap().unwrap().checkpoint, on);
         let unmanaged = Store::Slot {
             name: "p_slot".into(),
             intent: None,
```

`crates/data-avalanche-pipeline/src/decide.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/decide.rs b/crates/data-avalanche-pipeline/src/decide.rs
index f53221d..06cbef0 100644
--- a/crates/data-avalanche-pipeline/src/decide.rs
+++ b/crates/data-avalanche-pipeline/src/decide.rs
@@ -92,6 +92,7 @@ mod tests {
     const FIRST: Plan = Plan::Fresh(SnapshotReason::FirstStart);
     const LOST: Plan = Plan::Fresh(SnapshotReason::PositionLost);
     const ADOPTED: Plan = Plan::Fresh(SnapshotReason::AdoptedOrphan);
+    const CHANGED: Plan = Plan::Fresh(SnapshotReason::SourceChanged);
 
     fn facts(tracked_before: bool, in_progress: bool, sink_has_table: bool, added: bool) -> Facts {
         Facts {
@@ -124,6 +125,7 @@ mod tests {
             (M::Initial, RESUME, known, Decision::Stream),
             (M::Initial, LOST, known, snap(false, false)),
             (M::Initial, ADOPTED, known, snap(false, false)),
+            (M::Initial, CHANGED, known, snap(false, false)),
             (
                 M::Initial,
                 RESUME,
@@ -174,7 +176,7 @@ mod tests {
     #[test]
     fn a_snapshot_interrupted_before_a_gap_starts_over() {
         let interrupted = facts(true, true, true, false);
-        for plan in [LOST, ADOPTED] {
+        for plan in [LOST, ADOPTED, CHANGED] {
             for (mode, dump) in [
                 (M::Initial, false),
                 (M::Always, false),
@@ -198,6 +200,9 @@ mod tests {
             assert_eq!(d, Decision::Stream);
             assert!(w.unwrap().contains("position was lost"));
         }
+        let (d, w) = decide(M::Never, CHANGED, known);
+        assert_eq!(d, Decision::Stream);
+        assert!(w.unwrap().contains("source server changed"));
         let (_, w) = decide(M::Never, RESUME, facts(true, false, true, true));
         assert!(w.unwrap().contains("added to the stream"));
         assert_eq!(decide(M::Never, FIRST, Facts::default()).1, None);
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 7aa72f3..2a97acb 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -1022,6 +1022,67 @@ mod tests {
         assert!(st.warnings[0].contains("public.b") && st.warnings[0].contains("lost"));
     }
 
+    /// A failover to a replica behind the lake (DA-SRC-20): the new server's LSNs sit below the
+    /// table's marker, and the guard would skip its changes. The source-change start repositions
+    /// the table first, so a change on the new server lands even under `never` (nothing copied).
+    #[tokio::test]
+    async fn a_source_change_repositions_every_table_before_streaming() {
+        let w = World::new(&["a"]);
+        let pg = |timeline| ServerIdentity::Postgres {
+            system_identifier: 7,
+            timeline,
+        };
+        w.source.set_server(Some(pg(1)));
+        w.source.load(&t("a"), rows(1..=2));
+        run(&w, &mode("mode: initial"), async |status| {
+            for id in 3..=6 {
+                w.source
+                    .commit(FakeTxn::new().insert(&t("a"), row(id, "before")));
+            }
+            streamed(&status, &w).await;
+        })
+        .await
+        .0
+        .unwrap();
+        let marked = w.sink.table_state(&t("a")).await.unwrap().marker.unwrap();
+        assert!(
+            marked
+                .committed_position
+                .covers(&SourcePosition::Pg(Lsn(0x110)))
+                .unwrap(),
+            "{marked:?}"
+        );
+        w.source.fail_over(pg(2), Lsn(0x100));
+        let (end, st) = run(&w, &mode("mode: never"), async |status| {
+            streamed(&status, &w).await;
+            w.source
+                .commit(FakeTxn::new().insert(&t("a"), row(9, "on the new server")));
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(w.ids("a"), [1, 2, 3, 4, 5, 6, 9]);
+        let moved = w.sink.table_state(&t("a")).await.unwrap().marker.unwrap();
+        assert!(
+            !moved
+                .committed_position
+                .covers(&marked.committed_position)
+                .unwrap(),
+            "{moved:?}"
+        );
+        assert_eq!(
+            w.docs.slot().await.unwrap().unwrap().doc.server,
+            Some(pg(2))
+        );
+        assert!(
+            st.warnings
+                .iter()
+                .any(|w| w.contains("source server changed")),
+            "{:?}",
+            st.warnings
+        );
+    }
+
     #[tokio::test]
     async fn a_dump_initial_table_the_sink_has_stops_at_once() {
         let w = World::new(&["a"]);
```

`crates/data-avalanche-sink-delta/tests/sink/common.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/common.rs b/crates/data-avalanche-sink-delta/tests/sink/common.rs
index a06b538..d671a9f 100644
--- a/crates/data-avalanche-sink-delta/tests/sink/common.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/common.rs
@@ -92,6 +92,7 @@ pub fn cs(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeS
         },
         split: None,
         snapshot: false,
+        reposition: false,
     }
 }
 
```

`crates/data-avalanche-sink-delta/tests/sink/local.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/local.rs b/crates/data-avalanche-sink-delta/tests/sink/local.rs
index a857e7e..bb32e63 100644
--- a/crates/data-avalanche-sink-delta/tests/sink/local.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/local.rs
@@ -217,6 +217,36 @@ async fn a_recreated_table_is_not_skipped_by_the_old_position() {
     assert_eq!(marker.committed_position, lsn(25));
 }
 
+/// A source-change start repositions the table onto the new server's positions, which sit below
+/// the marker (a promoted replica's LSNs, DA-SRC-20): before it, a change at 30 is skipped as
+/// covered by 100; after it, the change lands.
+#[tokio::test]
+async fn a_reposition_replaces_the_marker() {
+    let (_dir, location) = local();
+    let s = sink(location, TableDefaults::default());
+    s.ensure_table(&spec()).await.unwrap();
+    commit(
+        &s,
+        cs(100, vec![upserts(&[(1, Some("a"))], None)], vec![]),
+        1,
+    )
+    .await;
+    let late = cs(30, vec![upserts(&[(2, Some("b"))], None)], vec![]);
+    assert_eq!(
+        commit(&s, late.clone(), 1).await,
+        CommitOutcome::SkippedCovered
+    );
+    let mut reposition = cs(20, vec![], vec![]);
+    reposition.reposition = true;
+    let out = commit(&s, reposition, 1).await;
+    assert!(matches!(out, CommitOutcome::Committed { .. }), "{out:?}");
+    let marker = s.table_state(&items()).await.unwrap().marker.unwrap();
+    assert_eq!(marker.committed_position, lsn(20));
+    let out = commit(&s, late, 1).await;
+    assert!(matches!(out, CommitOutcome::Committed { .. }), "{out:?}");
+    assert_eq!(rows(&s).await, owned(&[(1, Some("a")), (2, Some("b"))]));
+}
+
 /// Another writer re-creates the table between two calls of this sink: the live handle's stale
 /// snapshot can't be updated incrementally, so the sink re-opens the table and commits.
 #[tokio::test]
```

`crates/data-avalanche-state/src/pipeline.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/pipeline.rs b/crates/data-avalanche-state/src/pipeline.rs
index da8f07a..ed30719 100644
--- a/crates/data-avalanche-state/src/pipeline.rs
+++ b/crates/data-avalanche-state/src/pipeline.rs
@@ -331,6 +331,7 @@ mod tests {
         let doc = SlotDoc {
             slot_name: "data_avalanche_orders".into(),
             confirmed_flush_lsn: "0/16B3748".parse().unwrap(),
+            server: None,
         };
         assert_eq!(
             json(&doc),
@@ -345,6 +346,28 @@ mod tests {
         assert_eq!(p.slot().await.unwrap().unwrap().doc, later);
     }
 
+    /// A slot document records the server its position was read on; one written before that
+    /// (v0.1.0) still reads, with no server.
+    #[test]
+    fn a_slot_document_records_its_server() {
+        let doc = SlotDoc {
+            slot_name: "s".into(),
+            confirmed_flush_lsn: "0/16B3748".parse().unwrap(),
+            server: Some(ServerIdentity::Postgres {
+                system_identifier: 7692239610332024875,
+                timeline: 2,
+            }),
+        };
+        assert_eq!(
+            json(&doc),
+            "{\n  \"version\": 1,\n  \"slot_name\": \"s\",\n  \"confirmed_flush_lsn\": \"0/16B3748\",\n  \"server\": {\n    \"engine\": \"postgres\",\n    \"system_identifier\": 7692239610332024875,\n    \"timeline\": 2\n  }\n}"
+        );
+        let old: SlotDoc =
+            serde_json::from_str(r#"{"slot_name": "s", "confirmed_flush_lsn": "0/16B3748"}"#)
+                .unwrap();
+        assert_eq!(old.server, None);
+    }
+
     #[test]
     fn a_slot_intent_is_the_zero_position() {
         let intent = SlotDoc::intent("data_avalanche_orders");
```

`crates/data-avalanche-testkit/src/memory_sink.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
index 986f64a..494681e 100644
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -535,6 +535,7 @@ mod tests {
             },
             split: None,
             snapshot: false,
+            reposition: false,
         }
     }
 
@@ -643,6 +644,35 @@ mod tests {
         assert!(p.covers(&a).unwrap() && p.covers(&b).unwrap(), "{p}");
     }
 
+    /// A reposition replaces the marker instead of joining it (DA-SRC-20): a change below the old
+    /// marker lands after it.
+    #[tokio::test]
+    async fn a_reposition_replaces_the_marker() {
+        let sink = sink().await;
+        commit(
+            &sink,
+            cs(100, vec![upserts(&[(1, Some("a"))], None)], vec![]),
+            1,
+        )
+        .await;
+        let late = cs(30, vec![upserts(&[(2, Some("b"))], None)], vec![]);
+        assert_eq!(
+            commit(&sink, late.clone(), 1).await,
+            CommitOutcome::SkippedCovered
+        );
+        let mut reposition = cs(20, vec![], vec![]);
+        reposition.reposition = true;
+        commit(&sink, reposition.clone(), 1).await;
+        let t = reposition.table.clone();
+        let marker = sink.table_state(&t).await.unwrap().marker.unwrap();
+        assert_eq!(marker.committed_position, SourcePosition::Pg(Lsn(20)));
+        assert!(matches!(
+            commit(&sink, late, 1).await,
+            CommitOutcome::Committed { .. }
+        ));
+        assert_eq!(rows(&sink), owned(&[(1, Some("a")), (2, Some("b"))]));
+    }
+
     #[tokio::test]
     async fn a_newer_owner_fences_an_older_one() {
         let sink = sink().await;
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-<crate>` for each of `core`, `state`, `sink-delta`, `testkit`, `pipeline` and `api`

Expected: FAIL to compile, in every package:
- `TableChangeSet` has no field `reposition` (E0560, E0609: core, sink-delta, testkit);
- `SlotDoc` has no field `server` (E0560, E0609: state, pipeline, api);
- no variant `ServerIdentity::Postgres`, no `SnapshotReason::SourceChanged`, no method
  `FakeSource::fail_over` (E0599: state, pipeline).

- [ ] **Step 3: Implement**

`crates/data-avalanche-core/src/change_set.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/change_set.rs b/crates/data-avalanche-core/src/change_set.rs
index 8e44a49..f657807 100644
--- a/crates/data-avalanche-core/src/change_set.rs
+++ b/crates/data-avalanche-core/src/change_set.rs
@@ -64,6 +64,11 @@ pub struct TableChangeSet {
     /// position guard never skips them ([`crate::CommitMarker::covers`]). A chunk is emitted at
     /// the stream's frontier, which the table's marker may already cover (Q-M6a-6).
     pub snapshot: bool,
+    /// The table's marker is replaced by this set's position instead of joined with it: a
+    /// source-change start (`SnapshotReason::SourceChanged`) moves every table onto the new
+    /// server's positions, which may sit below the ones the lake has marked (DA-SRC-20). The
+    /// guard never skips it; it carries no rows, no truncate and no split.
+    pub reposition: bool,
 }
 
 #[derive(Debug, Clone, PartialEq, Eq, Error)]
@@ -96,6 +101,11 @@ impl TableChangeSet {
         if self.identifier.is_empty() {
             return Err(err("a change set with no identifier columns".into()));
         }
+        if self.reposition && (!self.is_empty() || self.split.is_some()) {
+            return Err(err(
+                "a reposition carries no rows, no truncate and no split".into(),
+            ));
+        }
         let canonical = self.schema.fields();
         let mut upsert_extra: Option<Vec<String>> = None;
         for b in &self.upserts {
```

`crates/data-avalanche-core/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/sink.rs b/crates/data-avalanche-core/src/sink.rs
index 3f4382d..7eade52 100644
--- a/crates/data-avalanche-core/src/sink.rs
+++ b/crates/data-avalanche-core/src/sink.rs
@@ -187,9 +187,10 @@ impl CommitMarker {
     /// inside the transaction.
     ///
     /// A snapshot set is never skipped: its rows are read now, not replayed history, and its range
-    /// is the stream's frontier, which the marker may cover already.
+    /// is the stream's frontier, which the marker may cover already. Nor is a reposition, whose
+    /// point is to replace the marker.
     pub fn covers(&self, cs: &TableChangeSet) -> Result<bool, PositionError> {
-        if cs.snapshot {
+        if cs.snapshot || cs.reposition {
             return Ok(false);
         }
         let stored = &self.committed_position;
@@ -320,7 +321,8 @@ pub trait Sink: Send + Sync {
     /// 5. the set's identifier must be the one the table was created with, and every column of
     ///    the set must be in the table with the same type;
     /// 6. apply, as one table version: the truncate clear, then the deletes, then the upserts;
-    /// 7. store the marker with the **union** of the prior stored position and the new one.
+    /// 7. store the marker with the **union** of the prior stored position and the new one; a
+    ///    [`TableChangeSet::reposition`] stores its own position instead.
     async fn commit(
         &self,
         cs: TableChangeSet,
```

`crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index b911590..6fd1712 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -101,6 +101,12 @@ pub enum SnapshotReason {
     /// No pipeline state, but the source kept a stream position: `on_orphan: adopt_resnapshot`
     /// took it over (D-15.1).
     AdoptedOrphan,
+    /// The checkpoint was read on another server (PostgreSQL: another system identifier or
+    /// timeline; MySQL / MariaDB in file/position mode: another server), and the pipeline's
+    /// `source_change_action` / `binlog_purge_action` is `re_snapshot`. The stream starts from the
+    /// new server's position, and every table's marker is replaced by it first: the new server's
+    /// positions may sit below ones the lake has marked, which the guard would skip (DA-SRC-20).
+    SourceChanged,
 }
 
 /// How streaming starts (Postgres_Slot… §1, the slot state machine).
@@ -115,15 +121,25 @@ pub enum StartPlan {
     },
 }
 
-/// The source server a stream position was read from (MySQL / MariaDB). A binlog file and offset
-/// mean something only on the server that wrote them: in file/position mode a start against
-/// another server halts the pipeline (`BINLOG_SOURCE_CHANGED`, Data-Flow Phase 2b). Its JSON is
-/// the `server` of `replication/position.json`.
+/// The source server a stream position was read from. A binlog file and offset mean something only
+/// on the server that wrote them: in file/position mode a start against another server halts the
+/// pipeline (`BINLOG_SOURCE_CHANGED`, Data-Flow Phase 2b). A PostgreSQL LSN means something only
+/// on its cluster's timeline: a promoted replica keeps the system identifier and starts a new
+/// timeline, whose LSNs may repeat ones the lake has already marked (`WAL_SOURCE_CHANGED`,
+/// DA-SRC-20). Its JSON is the `server` of `replication/position.json` or `replication/slot.json`.
 #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
 #[serde(tag = "engine", rename_all = "lowercase", deny_unknown_fields)]
 pub enum ServerIdentity {
-    Mysql { server_uuid: String },
-    Mariadb { server_id: u32 },
+    Mysql {
+        server_uuid: String,
+    },
+    Mariadb {
+        server_id: u32,
+    },
+    Postgres {
+        system_identifier: u64,
+        timeline: u32,
+    },
 }
 
 impl fmt::Display for ServerIdentity {
@@ -131,6 +147,13 @@ impl fmt::Display for ServerIdentity {
         match self {
             Self::Mysql { server_uuid } => write!(f, "MySQL server {server_uuid}"),
             Self::Mariadb { server_id } => write!(f, "MariaDB server_id {server_id}"),
+            Self::Postgres {
+                system_identifier,
+                timeline,
+            } => write!(
+                f,
+                "PostgreSQL system {system_identifier} on timeline {timeline}"
+            ),
         }
     }
 }
```

`crates/data-avalanche-pipeline/src/buffer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/buffer.rs b/crates/data-avalanche-pipeline/src/buffer.rs
index de0f590..7f5bc45 100644
--- a/crates/data-avalanche-pipeline/src/buffer.rs
+++ b/crates/data-avalanche-pipeline/src/buffer.rs
@@ -594,6 +594,7 @@ impl TableBuffer {
             position,
             split,
             snapshot: false,
+            reposition: false,
         }
     }
 }
```

`crates/data-avalanche-pipeline/src/checkpoint.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/checkpoint.rs b/crates/data-avalanche-pipeline/src/checkpoint.rs
index 28ddd1d..b498e05 100644
--- a/crates/data-avalanche-pipeline/src/checkpoint.rs
+++ b/crates/data-avalanche-pipeline/src/checkpoint.rs
@@ -1,6 +1,7 @@
 //! Where the pipeline's checkpoint lives (Data-Flow Phase 5): PostgreSQL's slot document
-//! (`replication/slot.json`), or MySQL's position document (`replication/position.json`), which
-//! records the server the position was read from with it.
+//! (`replication/slot.json`), or MySQL's position document (`replication/position.json`). Each
+//! records the server the position was read from with it (an older slot document has none: the
+//! next write records it).
 
 use data_avalanche_core::{Checkpoint, SourcePosition};
 use data_avalanche_state::{PipelineDocs, PositionDoc, SlotDoc, StateError};
@@ -41,8 +42,11 @@ impl Store {
     pub async fn load(&self, docs: &PipelineDocs) -> Result<Option<Stored>, StateError> {
         Ok(match self {
             Self::Slot { .. } => docs.slot().await?.map(|v| Stored {
-                checkpoint: SourcePosition::Pg(v.doc.confirmed_flush_lsn).into(),
                 intent: v.doc.is_intent(),
+                checkpoint: Checkpoint {
+                    position: SourcePosition::Pg(v.doc.confirmed_flush_lsn),
+                    server: v.doc.server,
+                },
             }),
             Self::Position => docs.position().await?.map(|v| Stored {
                 checkpoint: Checkpoint {
@@ -85,11 +89,12 @@ impl Store {
                 Self::Slot { name, .. },
                 Checkpoint {
                     position: SourcePosition::Pg(lsn),
-                    ..
+                    server,
                 },
             ) => Ok(Doc::Slot(SlotDoc {
                 slot_name: name.clone(),
                 confirmed_flush_lsn: *lsn,
+                server: server.clone(),
             })),
             (Self::Slot { .. }, c) => Err(protocol(format!(
                 "checkpoint {} isn't a PostgreSQL position",
```

`crates/data-avalanche-pipeline/src/decide.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/decide.rs b/crates/data-avalanche-pipeline/src/decide.rs
index 06cbef0..abe5268 100644
--- a/crates/data-avalanche-pipeline/src/decide.rs
+++ b/crates/data-avalanche-pipeline/src/decide.rs
@@ -46,7 +46,11 @@ pub(crate) enum Decision {
 pub(crate) fn decide(mode: SnapshotMode, plan: Plan, f: Facts) -> (Decision, Option<String>) {
     let recovery = matches!(
         plan,
-        Plan::Fresh(SnapshotReason::PositionLost | SnapshotReason::AdoptedOrphan)
+        Plan::Fresh(
+            SnapshotReason::PositionLost
+                | SnapshotReason::AdoptedOrphan
+                | SnapshotReason::SourceChanged
+        )
     );
     let snapshot = |dump| Decision::Snapshot {
         resume: f.in_progress && !recovery,
@@ -54,7 +58,9 @@ pub(crate) fn decide(mode: SnapshotMode, plan: Plan, f: Facts) -> (Decision, Opt
     };
     match mode {
         SnapshotMode::Never => {
-            let gap = if recovery {
+            let gap = if plan == Plan::Fresh(SnapshotReason::SourceChanged) {
+                Some("the source server changed and the stream starts on the new one".to_string())
+            } else if recovery {
                 Some("the stream position was lost and recreated".to_string())
             } else if f.added {
                 Some("the table was added to the stream at this start".to_string())
```

`crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
index 89b4230..5fca0b2 100644
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -551,6 +551,7 @@ impl World {
             .save_slot(&SlotDoc {
                 slot_name: "p_slot".into(),
                 confirmed_flush_lsn: end,
+                server: None,
             })
             .await
             .expect("the checkpoint");
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 2a97acb..e275197 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -11,8 +11,9 @@ use std::time::Instant;
 
 use data_avalanche_config::{Pipeline, SnapshotMode};
 use data_avalanche_core::{
-    Checkpoint, HaltReason, SchemaHint, Sink, SinkError, SinkKind, SnapshotReason, SourceConnector,
-    SourceError, StartPlan, TableRef, TableSchema, TableSpec,
+    Checkpoint, CommitMarker, CommitOutcome, HaltReason, PositionRange, SchemaHint, Sink,
+    SinkError, SinkKind, SnapshotReason, SourceConnector, SourceError, SourcePosition, StartPlan,
+    TableChangeSet, TableRef, TableSchema, TableSpec,
 };
 use data_avalanche_state::{PipelineDocs, SnapshotStatus, StateError, TableHalt, TableSnapshotDoc};
 use data_avalanche_storage::StoreError;
@@ -331,6 +332,56 @@ where
 /// new checkpoint `reset_position` returned). Leaves the core tracking the tables to stream, with
 /// every decision persisted: halts and snapshot requests first, then the cached schemas, then a
 /// fresh start's checkpoint, recorded with the server the start read (MySQL).
+/// Moves the table's marker onto `at`, the new server's position (DA-SRC-20): an empty change set
+/// that replaces the marker instead of joining it. A table the lake doesn't hold, or holds no
+/// marker for, has nothing to move.
+async fn reposition(
+    core: &Core,
+    tuning: &Tuning,
+    schema: &TableSchema,
+    at: &SourcePosition,
+) -> Result<(), WorkerError> {
+    let table = schema.table();
+    let state = sink_retry(tuning, || core.may_write(), || core.sink.table_state(table)).await?;
+    let (true, Some(_), Some(columns)) = (state.exists, state.marker, state.schema) else {
+        return Ok(());
+    };
+    let cs = TableChangeSet {
+        table: table.clone(),
+        schema: columns,
+        identifier: schema.identifier().to_vec(),
+        truncate: false,
+        upserts: vec![],
+        deletes: vec![],
+        position: PositionRange {
+            start: at.clone(),
+            end: at.clone(),
+            last_change: at.clone(),
+        },
+        split: None,
+        snapshot: false,
+        reposition: true,
+    };
+    let marker = CommitMarker {
+        owner_epoch: core.owner_epoch,
+        committed_position: at.clone(),
+        pipeline_id: core.settings.pipeline_id.clone(),
+    };
+    let committed = sink_retry(
+        tuning,
+        || core.may_write(),
+        || core.sink.commit(cs.clone(), marker.clone()),
+    )
+    .await?;
+    match committed {
+        CommitOutcome::Fenced { current_epoch } => Err(WorkerError::Fenced {
+            table: table.clone(),
+            current_epoch,
+        }),
+        CommitOutcome::Committed { .. } | CommitOutcome::SkippedCovered => Ok(()),
+    }
+}
+
 pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(), WorkerError> {
     core.update(|st| st.phase = Phase::Starting);
     let s = &core.settings;
@@ -516,7 +567,11 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
     })?;
     let gap = matches!(
         plan,
-        Plan::Fresh(SnapshotReason::PositionLost | SnapshotReason::AdoptedOrphan)
+        Plan::Fresh(
+            SnapshotReason::PositionLost
+                | SnapshotReason::AdoptedOrphan
+                | SnapshotReason::SourceChanged
+        )
     );
 
     // Each table: what this start does with it.
@@ -830,6 +885,17 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
             core.docs.clear_halt(table).await?;
         }
     }
+    // A source change moves every table the lake holds onto the new server's positions before
+    // the checkpoint that names the server: a restart in between still finds the old one
+    // recorded, and repositions them again (DA-SRC-20).
+    if matches!(plan, Plan::Fresh(SnapshotReason::SourceChanged)) {
+        let tables: BTreeSet<&TableRef> = resolved_tables.keys().chain(cached.keys()).collect();
+        for table in tables {
+            if let Some(schema) = cached.get(table).or_else(|| resolved_tables.get(table)) {
+                reposition(core, &tuning, schema, &at).await?;
+            }
+        }
+    }
     if let Some(doc) = fresh {
         if core.hooks.at(&HookPoint::SchemasCached) == HookAction::Crash {
             return Err(WorkerError::Crashed(HookPoint::SchemasCached));
```

`crates/data-avalanche-sink-delta/src/sink.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/sink.rs b/crates/data-avalanche-sink-delta/src/sink.rs
index eb16def..2fdfc89 100644
--- a/crates/data-avalanche-sink-delta/src/sink.rs
+++ b/crates/data-avalanche-sink-delta/src/sink.rs
@@ -243,9 +243,10 @@ impl DeltaSink {
         // Before the truncate's DELETE: a key that halts the MERGE must not leave the table
         // cleared.
         check_keys(cs, &types)?;
+        // The union of the stored position and this set's end, or a reposition's own (DA-SRC-20).
         let position = match &stored {
-            Some(p) => p.union(&cs.position.end)?,
-            None => cs.position.end.clone(),
+            Some(p) if !cs.reposition => p.union(&cs.position.end)?,
+            _ => cs.position.end.clone(),
         };
         let txn_version = i64::try_from(marker.owner_epoch)
             .map_err(|_| permanent(format!("owner epoch {}", marker.owner_epoch)))?;
```

`crates/data-avalanche-sink-delta/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/src/test_support.rs b/crates/data-avalanche-sink-delta/src/test_support.rs
index 3433256..b9ffe38 100644
--- a/crates/data-avalanche-sink-delta/src/test_support.rs
+++ b/crates/data-avalanche-sink-delta/src/test_support.rs
@@ -116,6 +116,7 @@ pub(crate) fn change_set(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>)
         },
         split: None,
         snapshot: false,
+        reposition: false,
     }
 }
 
```

`crates/data-avalanche-state/src/pipeline.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/pipeline.rs b/crates/data-avalanche-state/src/pipeline.rs
index ed30719..cedee16 100644
--- a/crates/data-avalanche-state/src/pipeline.rs
+++ b/crates/data-avalanche-state/src/pipeline.rs
@@ -46,6 +46,11 @@ pub struct SlotDoc {
     /// As PostgreSQL prints it (`0/16B3748`).
     #[serde(serialize_with = "display", deserialize_with = "from_str")]
     pub confirmed_flush_lsn: Lsn,
+    /// The server the position was read on (its system identifier and timeline): a start on
+    /// another one is a source change (DA-SRC-20). `None` in a document written before it was
+    /// recorded (v0.1.0) or for an intent: the next checkpoint write records it.
+    #[serde(default, skip_serializing_if = "Option::is_none")]
+    pub server: Option<ServerIdentity>,
 }
 
 impl Document for SlotDoc {
@@ -62,6 +67,7 @@ impl SlotDoc {
         Self {
             slot_name: slot_name.to_owned(),
             confirmed_flush_lsn: Lsn(0),
+            server: None,
         }
     }
 
```

`crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index 50a435e..8d88311 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -258,6 +258,8 @@ struct Db {
     opened: Vec<Checkpoint>,
     /// [`FakeSource::set_server`].
     server: Option<ServerIdentity>,
+    /// [`FakeSource::fail_over`]: the next `prepare` from a checkpoint is a source change.
+    failed_over: bool,
     reads: BTreeMap<TableRef, usize>,
     rescans: usize,
     in_flights: usize,
@@ -315,6 +317,7 @@ impl FakeSource {
             acknowledged: vec![],
             opened: vec![],
             server: None,
+            failed_over: false,
             reads: BTreeMap::new(),
             rescans: 0,
             in_flights: 0,
@@ -454,6 +457,19 @@ impl FakeSource {
         self.wake.notify_waiters();
     }
 
+    /// A failover to a replica whose WAL ends at `wal_end`, behind the old server's: the WAL past
+    /// it is gone, the slot didn't come over, and `server` answers from now on. The next
+    /// `prepare` from a checkpoint starts from the new server's position, as
+    /// `source_change_action: re_snapshot` does ([`SnapshotReason::SourceChanged`]).
+    pub fn fail_over(&self, server: ServerIdentity, wal_end: Lsn) {
+        let mut db = self.db();
+        db.wal.retain(|e| e.end() <= wal_end);
+        db.wal_end = wal_end;
+        db.slot = None;
+        db.server = Some(server);
+        db.failed_over = true;
+    }
+
     /// The tables the next resuming `prepare` reports in `Prepared::added`.
     pub fn report_added(&self, tables: &[TableRef]) {
         self.db().added = tables.to_vec();
@@ -962,7 +978,9 @@ impl SourceConnector for FakeSource {
         db.prepares.push(checkpoint.cloned());
         let server = db.server.clone();
         let checkpoint = checkpoint.map(|c| lsn(&c.position));
+        let failed_over = std::mem::take(&mut db.failed_over);
         let reason = match (checkpoint, db.slot) {
+            (Some(_), _) if failed_over => Some(SnapshotReason::SourceChanged),
             (None, None) => Some(SnapshotReason::FirstStart),
             (None, Some(_)) => {
                 return Err(SourceError::Permanent(
```

`crates/data-avalanche-testkit/src/memory_sink.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/memory_sink.rs b/crates/data-avalanche-testkit/src/memory_sink.rs
index 494681e..353725d 100644
--- a/crates/data-avalanche-testkit/src/memory_sink.rs
+++ b/crates/data-avalanche-testkit/src/memory_sink.rs
@@ -448,12 +448,13 @@ impl Sink for MemorySink {
         t.check_schema(&cs)?;
         t.apply(&cs)?;
         t.version += 1;
-        // The stored position is the union of the prior one and this commit's (Data-Flow Phase 4).
+        // The stored position is the union of the prior one and this commit's (Data-Flow Phase 4),
+        // or a reposition's own.
         let committed_position = match &t.marker {
-            Some(stored) => stored
+            Some(stored) if !cs.reposition => stored
                 .committed_position
                 .union(&marker.committed_position)?,
-            None => marker.committed_position,
+            _ => marker.committed_position,
         };
         t.marker = Some(CommitMarker {
             committed_position,
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same six packages.

Expected: PASS: core 154, state 89, sink-delta 89, testkit 30, pipeline 254, api 47 tests.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/pipelines.rs \
  crates/data-avalanche-core/src/change_set.rs \
  crates/data-avalanche-core/src/sink.rs \
  crates/data-avalanche-pipeline/src/checkpoint.rs \
  crates/data-avalanche-pipeline/src/decide.rs \
  crates/data-avalanche-pipeline/src/worker.rs \
  crates/data-avalanche-sink-delta/tests/sink/common.rs \
  crates/data-avalanche-sink-delta/tests/sink/local.rs \
  crates/data-avalanche-state/src/pipeline.rs \
  crates/data-avalanche-testkit/src/memory_sink.rs \
  crates/data-avalanche-core/src/source.rs \
  crates/data-avalanche-pipeline/src/buffer.rs \
  crates/data-avalanche-pipeline/src/test_support.rs \
  crates/data-avalanche-sink-delta/src/sink.rs \
  crates/data-avalanche-sink-delta/src/test_support.rs \
  crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "feat(core): a source change and the marker reset — ServerIdentity::Postgres, SnapshotReason::SourceChanged, TableChangeSet.reposition (the guard never skips it, the sink stores its position); the slot document records its server; the worker moves every table onto the new server before the checkpoint names it"
```

---

### Task 2: The PostgreSQL source's identity (`config`, `source-pg`)

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/connector.rs`, `crates/data-avalanche-source-pg/src/slot.rs`, `crates/data-avalanche-config/src/build.rs`, `crates/data-avalanche-config/src/enums.rs`, `crates/data-avalanche-config/src/field_docs.rs`, `crates/data-avalanche-config/src/merge.rs`, `crates/data-avalanche-config/src/mutability.rs`, `crates/data-avalanche-config/src/raw.rs`, `crates/data-avalanche-config/src/registry.rs`, `crates/data-avalanche-config/src/resolved.rs`, `crates/data-avalanche-core/src/source.rs`, `docs/src/content/docs/reference/configuration.md`, `docs/src/content/docs/reference/validation-rules.md`
- Test: `crates/data-avalanche-config/tests/rules/domain.rs`, `crates/data-avalanche-source-pg/tests/pg/slot.rs`

**Interfaces:**
- Consumes: Task 1's `ServerIdentity::Postgres`, `SnapshotReason::SourceChanged`,
  `Prepared.server`.
- Produces:
  - `SourceChangeAction { Error, ReSnapshot }` (config, re-exported), `Postgres.source_change_action`,
    V-123, class R, its `FIELD_DOCS` row;
  - in `source-pg`'s `slot.rs` (a private module):
    - `pub fn parse_identity(id: &str, walfile: &str) -> Result<ServerIdentity, SourceError>`;
    - `pub fn source_change(checkpoint: Option<&Checkpoint>, now: &ServerIdentity, action) -> Result<bool, SourceError>`;
    - `pub fn on_source_change(slot: Option<&SlotState>, name: &str, management) -> SlotAction`;
    - `SlotAction::Adopt(SnapshotReason)`;
    - `Live.source_change_action`.

`prepare` reads the identity first, and `Prepared.server` is always `Some`. On a source change
it takes `on_source_change`'s action instead of `decide`'s, and warns, naming both servers.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-config/tests/rules/domain.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/domain.rs b/crates/data-avalanche-config/tests/rules/domain.rs
index cd98df0..1dbdf20 100644
--- a/crates/data-avalanche-config/tests/rules/domain.rs
+++ b/crates/data-avalanche-config/tests/rules/domain.rs
@@ -172,6 +172,18 @@ pub fn cases() -> Vec<Case> {
                 json!("re_snapshot"),
             )
         }),
+        case("V-123", Error, "source change ignore", || {
+            pg_delta().set(
+                "/pipelines/p/source/postgres/source_change_action",
+                json!("ignore"),
+            )
+        }),
+        case("V-123", Silent, "source change re_snapshot", || {
+            pg_delta().set(
+                "/pipelines/p/source/postgres/source_change_action",
+                json!("re_snapshot"),
+            )
+        }),
         case("V-114", Error, "ntz as date", || {
             pg_delta().set("/pipelines/p/sink/delta/timestamp_ntz_as", json!("date"))
         }),
```

`crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index d86b546..9c348fd 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -445,6 +445,7 @@ mod tests {
         let pg = next.source.postgres.as_mut().unwrap();
         pg.heartbeat = Duration::from_secs(7);
         pg.slot_on_orphan = SlotOnOrphan::AdoptResnapshot;
+        pg.source_change_action = SourceChangeAction::ReSnapshot;
         pg.auto_cleanup = true;
         pg.slot = "another_slot".into();
         next.tables.include = vec!["public.t".into(), "public.u".into()];
@@ -453,6 +454,7 @@ mod tests {
         let live = Live {
             heartbeat: Duration::from_secs(7),
             slot_on_orphan: SlotOnOrphan::AdoptResnapshot,
+            source_change_action: SourceChangeAction::ReSnapshot,
             auto_cleanup: true,
             include: vec!["public.t".into(), "public.u".into()],
             exclude: vec!["public.v".into()],
```

`crates/data-avalanche-source-pg/src/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
index b61630c..a1aefd0 100644
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -604,6 +604,78 @@ mod tests {
         }
     }
 
+    fn read_on(lsn: u64, system_identifier: u64, timeline: u32) -> Checkpoint {
+        Checkpoint {
+            position: SourcePosition::Pg(Lsn(lsn)),
+            server: Some(ServerIdentity::Postgres {
+                system_identifier,
+                timeline,
+            }),
+        }
+    }
+
+    #[test]
+    fn the_identity_is_the_system_and_the_timeline() {
+        assert_eq!(
+            parse_identity("7692239610332024875", "00000002").unwrap(),
+            ServerIdentity::Postgres {
+                system_identifier: 7692239610332024875,
+                timeline: 2
+            }
+        );
+        assert!(parse_identity("x", "00000002").is_err());
+        assert!(parse_identity("1", "zz").is_err());
+    }
+
+    /// A promoted replica keeps the system identifier and starts a new timeline (DA-SRC-20).
+    #[test]
+    fn another_timeline_or_system_is_a_source_change() {
+        let now = ServerIdentity::Postgres {
+            system_identifier: 7,
+            timeline: 2,
+        };
+        let promoted = read_on(0x20, 7, 1);
+        let e = source_change(Some(&promoted), &now, SourceChangeAction::Error).unwrap_err();
+        assert!(
+            e.to_string().starts_with(
+                "WAL_SOURCE_CHANGED: the checkpoint 0/20 was read on PostgreSQL system 7 on \
+                 timeline 1, and the pipeline now reads PostgreSQL system 7 on timeline 2"
+            ),
+            "{e}"
+        );
+        assert!(source_change(Some(&promoted), &now, SourceChangeAction::ReSnapshot).unwrap());
+        let another = read_on(0x20, 8, 2);
+        assert!(source_change(Some(&another), &now, SourceChangeAction::ReSnapshot).unwrap());
+        let same = read_on(0x20, 7, 2);
+        assert!(!source_change(Some(&same), &now, SourceChangeAction::Error).unwrap());
+        let unrecorded: Checkpoint = SourcePosition::Pg(Lsn(0x20)).into();
+        assert!(
+            !source_change(Some(&unrecorded), &now, SourceChangeAction::Error).unwrap(),
+            "a checkpoint written before the identity was recorded is taken as this server's"
+        );
+        assert!(!source_change(None, &now, SourceChangeAction::Error).unwrap());
+    }
+
+    #[test]
+    fn a_source_change_starts_from_the_new_server() {
+        assert_eq!(on_source_change(None, "s", M), A::Create(R::SourceChanged));
+        assert_eq!(
+            on_source_change(Some(&slot(0x10)), "s", M),
+            A::Recreate(R::SourceChanged),
+            "a slot that came over with the failover is dropped"
+        );
+        assert_eq!(
+            on_source_change(Some(&slot(0x10)), "s", X),
+            A::Adopt(R::SourceChanged)
+        );
+        refused(on_source_change(None, "s", X), "must exist there");
+        let lost = SlotState {
+            invalidated: true,
+            ..slot(0x10)
+        };
+        refused(on_source_change(Some(&lost), "s", X), "must exist there");
+    }
+
     #[test]
     fn state_and_slot_resume() {
         assert_eq!(d(Some(0x20), Some(slot(0x20)), M, HALT), A::Resume);
```

`crates/data-avalanche-source-pg/tests/pg/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/slot.rs b/crates/data-avalanche-source-pg/tests/pg/slot.rs
index 0cdab2f..e0cd298 100644
--- a/crates/data-avalanche-source-pg/tests/pg/slot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/slot.rs
@@ -5,7 +5,8 @@ use std::time::Duration;
 
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    Checkpoint, SnapshotReason, SourceConnector, SourceError, SourcePosition, StartPlan, TableRef,
+    Checkpoint, ServerIdentity, SnapshotReason, SourceConnector, SourceError, SourcePosition,
+    StartPlan, TableRef,
 };
 use data_avalanche_testkit::require_it;
 use tokio_postgres::Client;
@@ -105,6 +106,91 @@ async fn first_start_then_resume_then_a_lost_slot() {
     cleanup(&a, &n).await;
 }
 
+/// A checkpoint read on another server, another system or another timeline (a promoted
+/// standby), halts with `WAL_SOURCE_CHANGED`; under `source_change_action: re_snapshot` the slot
+/// is made again on the server read now, and the pipeline snapshots from it. A checkpoint with no
+/// server (v0.1.0's) resumes, and records the server (DA-SRC-20).
+#[tokio::test]
+async fn a_checkpoint_from_another_server() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(&a, "slot_server", DDL).await;
+    let tables = [n.table("a")];
+    let halt = connector(&pipeline(&n, &["*"], "", ""));
+    let first = halt.prepare(None, &tables).await.unwrap();
+    let start = from(&first.plan, SnapshotReason::FirstStart);
+    let Some(ServerIdentity::Postgres {
+        system_identifier,
+        timeline,
+    }) = first.server.clone()
+    else {
+        panic!("a PostgreSQL server, got {:?}", first.server)
+    };
+    let p = halt
+        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
+        .await
+        .unwrap();
+    assert_eq!(
+        (p.plan, p.server),
+        (StartPlan::Resume, first.server.clone())
+    );
+
+    let elsewhere = [
+        ServerIdentity::Postgres {
+            system_identifier: system_identifier ^ 1,
+            timeline,
+        },
+        ServerIdentity::Postgres {
+            system_identifier,
+            timeline: timeline + 1,
+        },
+    ];
+    for server in &elsewhere {
+        let cp = Checkpoint {
+            position: start.clone(),
+            server: Some(server.clone()),
+        };
+        permanent(
+            halt.prepare(Some(&cp), &tables).await.unwrap_err(),
+            "WAL_SOURCE_CHANGED",
+        );
+    }
+    a.execute(&format!("INSERT INTO {}.a VALUES (1)", n.schema), &[])
+        .await
+        .unwrap();
+    let recover = connector(&pipeline(
+        &n,
+        &["*"],
+        "",
+        ", source_change_action: re_snapshot",
+    ));
+    let cp = Checkpoint {
+        position: start.clone(),
+        server: Some(elsewhere[1].clone()),
+    };
+    let p = recover.prepare(Some(&cp), &tables).await.unwrap();
+    let SourcePosition::Pg(at) = from(&p.plan, SnapshotReason::SourceChanged) else {
+        panic!("a PostgreSQL position")
+    };
+    assert!(
+        SourcePosition::Pg(at) != start,
+        "made again, at the server's position now"
+    );
+    assert_eq!(confirmed(&a, &n).await, Some(at));
+    assert_eq!(
+        p.server, first.server,
+        "the checkpoint now names this server"
+    );
+    assert!(
+        p.warnings.iter().any(
+            |w| w.contains("source_change_action is re_snapshot, so the pipeline re-snapshots")
+        ),
+        "{:?}",
+        p.warnings
+    );
+    cleanup(&a, &n).await;
+}
+
 #[tokio::test]
 async fn an_orphan_halts_by_default_and_is_adopted_on_request() {
     require_it!();
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config` and `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg -- --test-threads=1 --skip tls::`

Expected:
- config: FAIL, `rule_cases`: `1 failing case(s): V-123 Error (source change ignore)`.
- source-pg: FAIL to compile: `SourceChangeAction` (E0433), `source_change`, `on_source_change`
  (E0425) and `ServerIdentity` (E0433) not found.

- [ ] **Step 3: Implement, and regenerate the reference**

`crates/data-avalanche-config/src/build.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/build.rs b/crates/data-avalanche-config/src/build.rs
index 15d284a..aa79e04 100644
--- a/crates/data-avalanche-config/src/build.rs
+++ b/crates/data-avalanche-config/src/build.rs
@@ -755,6 +755,13 @@ fn postgres(p: &RawPostgres, at: &At, ctx: &mut Ctx) -> Option<Postgres> {
         "slot_on_orphan",
         SlotOnOrphan::Halt,
     );
+    let source_change_action = or(
+        ctx,
+        &p.source_change_action,
+        at,
+        "source_change_action",
+        SourceChangeAction::Error,
+    );
     let max_wal_lag_bytes = tri(ctx, &p.max_wal_lag_bytes, at, "max_wal_lag_bytes", None);
     Some(Postgres {
         publication: publication?,
@@ -763,6 +770,7 @@ fn postgres(p: &RawPostgres, at: &At, ctx: &mut Ctx) -> Option<Postgres> {
         heartbeat: heartbeat?,
         slot_management: slot_management?,
         slot_on_orphan: slot_on_orphan?,
+        source_change_action: source_change_action?,
         max_wal_lag_bytes: max_wal_lag_bytes?.map(ByteSize::bytes),
     })
 }
```

`crates/data-avalanche-config/src/enums.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/enums.rs b/crates/data-avalanche-config/src/enums.rs
index b31daa3..03bc136 100644
--- a/crates/data-avalanche-config/src/enums.rs
+++ b/crates/data-avalanche-config/src/enums.rs
@@ -101,6 +101,7 @@ config_enum!(SslMode, rule = "V-020", {
 config_enum!(ToastStrategy, rule = "V-080", { CarryForward => "carry_forward", ReQuery => "re_query" });
 config_enum!(SlotManagement, rule = "V-102", { Managed => "managed", RequireExisting => "require_existing" });
 config_enum!(SlotOnOrphan, rule = "V-102", { Halt => "halt", AdoptResnapshot => "adopt_resnapshot" });
+config_enum!(SourceChangeAction, rule = "V-123", { Error => "error", ReSnapshot => "re_snapshot" });
 config_enum!(ReplicationType, rule = "V-021", { Binlog => "binlog", Gtid => "gtid" });
 config_enum!(Tinyint1Detection, rule = "V-081", { InformationSchema => "information_schema", None => "none" });
 config_enum!(BinlogPurgeAction, rule = "V-105", { Error => "error", ReSnapshot => "re_snapshot" });
```

`crates/data-avalanche-config/src/field_docs.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
index 212cdd1..b5d141a 100644
--- a/crates/data-avalanche-config/src/field_docs.rs
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -399,6 +399,11 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
         Value("halt"),
         "A slot that exists with no state of the pipeline's: `halt` refuses to start, naming it; `adopt_resnapshot` takes it over and starts from the current position, snapshotting as `snapshots.mode` says (none under `never`).",
     ),
+    f(
+        "pipelines.*.source.postgres.source_change_action",
+        Value("error"),
+        "A start on a server other than the checkpoint's (another system identifier, or another timeline after a failover): `error` halts `WAL_SOURCE_CHANGED`; `re_snapshot` starts from the new server's position, snapshotting as `snapshots.mode` says (none under `never`).",
+    ),
     f(
         "pipelines.*.source.postgres.max_wal_lag_bytes",
         Value("null"),
```

`crates/data-avalanche-config/src/merge.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/merge.rs b/crates/data-avalanche-config/src/merge.rs
index 322b98c..9d3699b 100644
--- a/crates/data-avalanche-config/src/merge.rs
+++ b/crates/data-avalanche-config/src/merge.rs
@@ -95,6 +95,7 @@ merge_struct!(RawPostgres {
     heartbeat,
     slot_management,
     slot_on_orphan,
+    source_change_action,
     max_wal_lag_bytes,
 });
 merge_struct!(RawMysql {
```

`crates/data-avalanche-config/src/mutability.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/mutability.rs b/crates/data-avalanche-config/src/mutability.rs
index 7a7ce23..23d0b6a 100644
--- a/crates/data-avalanche-config/src/mutability.rs
+++ b/crates/data-avalanche-config/src/mutability.rs
@@ -49,6 +49,7 @@ pub(crate) const CLASSES: &[(&str, Mutability)] = &[
     ("pipelines.*.source.postgres.auto_cleanup", R),
     ("pipelines.*.source.postgres.heartbeat", R),
     ("pipelines.*.source.postgres.slot_on_orphan", R),
+    ("pipelines.*.source.postgres.source_change_action", R),
     ("pipelines.*.source.postgres.max_wal_lag_bytes", R),
     ("pipelines.*.source.mysql.binlog_purge_action", R),
     ("pipelines.*.source.mysql.heartbeat", R),
```

`crates/data-avalanche-config/src/raw.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/raw.rs b/crates/data-avalanche-config/src/raw.rs
index 52184a6..b171528 100644
--- a/crates/data-avalanche-config/src/raw.rs
+++ b/crates/data-avalanche-config/src/raw.rs
@@ -193,6 +193,7 @@ pub(crate) struct RawPostgres {
     pub heartbeat: L<Duration>,
     pub slot_management: L<SlotManagement>,
     pub slot_on_orphan: L<SlotOnOrphan>,
+    pub source_change_action: L<SourceChangeAction>,
     #[serde(default, deserialize_with = "tri")]
     pub max_wal_lag_bytes: Tri<ByteSize>,
 }
```

`crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index 44760f0..9f0dc61 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1028,6 +1028,14 @@ pub const RULES: &[RuleMeta] = &[
         r#"globals.cluster.memberlist timers"#,
         r#"gossip_interval, probe_interval, probe_timeout and push_pull_interval above zero; gossip_nodes and retransmit_factor at least 1; probe_timeout below probe_interval."#,
     ),
+    rule(
+        "V-123",
+        Error,
+        L::Pipeline,
+        Config,
+        r#"source.postgres.source_change_action"#,
+        r#"∈ {error, re_snapshot}."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
@@ -1041,7 +1049,7 @@ mod tests {
 
     #[test]
     fn has_every_spec_rule_once_in_order() {
-        assert_eq!(RULES.len(), 116);
+        assert_eq!(RULES.len(), 117);
         assert!(
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
```

`crates/data-avalanche-config/src/resolved.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/resolved.rs b/crates/data-avalanche-config/src/resolved.rs
index cbb369c..c43bf8b 100644
--- a/crates/data-avalanche-config/src/resolved.rs
+++ b/crates/data-avalanche-config/src/resolved.rs
@@ -233,6 +233,9 @@ pub struct Postgres {
     pub heartbeat: Duration,
     pub slot_management: SlotManagement,
     pub slot_on_orphan: SlotOnOrphan,
+    /// What a start does when the server isn't the one the checkpoint was read on (another system
+    /// identifier, or another timeline after a promotion): `error` halts `WAL_SOURCE_CHANGED`.
+    pub source_change_action: SourceChangeAction,
     /// `None` = alert only (§9.2.1).
     pub max_wal_lag_bytes: Option<u64>,
 }
```

`crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index 6fd1712..e482ad9 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -187,8 +187,8 @@ pub struct Prepared {
     /// publication behind its back. Empty when the plan snapshots every table.
     pub added: Vec<TableRef>,
     pub warnings: Vec<String>,
-    /// The server the stream reads, recorded with the checkpoint so the next start can check it
-    /// (MySQL / MariaDB). `None` for a source that records none (PostgreSQL).
+    /// The server the stream reads, recorded with the checkpoint so the next start can check it.
+    /// `None` for a source that records none.
     pub server: Option<ServerIdentity>,
 }
 
```

`crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index 9c348fd..a34d29b 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -7,7 +7,8 @@ use std::time::Duration;
 
 use async_trait::async_trait;
 use data_avalanche_config::{
-    Pipeline, Postgres, SlotOnOrphan, SourceType, Tables, check_table_set, check_unbounded_numeric,
+    Pipeline, Postgres, SlotOnOrphan, SourceChangeAction, SourceType, Tables, check_table_set,
+    check_unbounded_numeric,
 };
 use data_avalanche_core::{
     ChangeStream, Checkpoint, Chunk, HaltReason, InFlightTxn, KeysetCursor, Preflight, Prepared,
@@ -31,6 +32,8 @@ pub(crate) struct Live {
     pub heartbeat: Duration,
     /// From the next prepare.
     pub slot_on_orphan: SlotOnOrphan,
+    /// From the next prepare.
+    pub source_change_action: SourceChangeAction,
     /// At the teardown.
     pub auto_cleanup: bool,
     /// `tables.include` and `tables.exclude`, from the next resolve: in place only while the
@@ -44,6 +47,7 @@ impl Live {
         Self {
             heartbeat: pg.heartbeat,
             slot_on_orphan: pg.slot_on_orphan,
+            source_change_action: pg.source_change_action,
             auto_cleanup: pg.auto_cleanup,
             include: tables.include.clone(),
             exclude: tables.exclude.clone(),
```

`crates/data-avalanche-source-pg/src/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
index a1aefd0..473c8f9 100644
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -4,11 +4,11 @@
 
 use std::collections::BTreeSet;
 
-use data_avalanche_config::{SlotManagement, SlotOnOrphan};
+use data_avalanche_config::{SlotManagement, SlotOnOrphan, SourceChangeAction};
 use data_avalanche_core::position::Lsn;
 use data_avalanche_core::{
-    Checkpoint, Prepared, RetainedLog, SnapshotReason, SourceError, SourcePosition, StartPlan,
-    TableRef,
+    Checkpoint, Prepared, RetainedLog, ServerIdentity, SnapshotReason, SourceError, SourcePosition,
+    StartPlan, TableRef,
 };
 use tokio_postgres::Client;
 
@@ -52,9 +52,10 @@ pub enum SlotAction {
     Create(SnapshotReason),
     /// Drop the unusable slot and create it again, then snapshot.
     Recreate(SnapshotReason),
-    /// Move the orphan slot to the current WAL position (`pg_replication_slot_advance`), then
-    /// snapshot: `on_orphan: adopt_resnapshot`. Advancing works under `require_existing` too.
-    Adopt,
+    /// Move the slot to the current WAL position (`pg_replication_slot_advance`), then snapshot:
+    /// an orphan under `on_orphan: adopt_resnapshot`, or the slot of a source change. Advancing
+    /// works under `require_existing` too.
+    Adopt(SnapshotReason),
     /// Refuse to start: the operator decides.
     Refuse(String),
 }
@@ -123,7 +124,7 @@ pub fn decide(
                     SlotAction::Recreate(SnapshotReason::AdoptedOrphan)
                 }
                 None if s.invalidated => lost("was invalidated"),
-                None => SlotAction::Adopt,
+                None => SlotAction::Adopt(SnapshotReason::AdoptedOrphan),
             },
         },
         (None, None) if managed => SlotAction::Create(SnapshotReason::FirstStart),
@@ -167,6 +168,64 @@ async fn partitions(
         .collect())
 }
 
+/// The server the stream reads: its system identifier, and the timeline WAL is written on now
+/// (the first eight hex digits of the current WAL file's name). A promotion keeps the identifier
+/// and starts a new timeline at once (measured on PostgreSQL 18, `pg_ctl promote -w`); a standby,
+/// which writes no WAL, reports its last checkpoint's timeline. Every function is open to any
+/// role.
+pub(crate) async fn server_identity(
+    client: &Client,
+) -> Result<(String, String), tokio_postgres::Error> {
+    let row = client
+        .query_one(
+            "SELECT system_identifier::text,
+                    CASE WHEN pg_is_in_recovery()
+                         THEN lpad(to_hex((SELECT timeline_id FROM pg_control_checkpoint())), 8, '0')
+                         ELSE substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8) END
+               FROM pg_control_system()",
+            &[],
+        )
+        .await?;
+    Ok((row.get(0), row.get(1)))
+}
+
+/// [`server_identity`]'s row: the identifier in decimal, the timeline in hex.
+pub fn parse_identity(id: &str, timeline: &str) -> Result<ServerIdentity, SourceError> {
+    let bad = || SourceError::Permanent(format!("the server's identity reads {id}, {timeline}"));
+    Ok(ServerIdentity::Postgres {
+        system_identifier: id.parse().map_err(|_| bad())?,
+        timeline: u32::from_str_radix(timeline, 16).map_err(|_| bad())?,
+    })
+}
+
+/// Whether the checkpoint was read on another server than `now` (DA-SRC-20): `Ok(true)` asks a
+/// source-change start (`source_change_action: re_snapshot`), `error` halts. A checkpoint that
+/// records no server (written before it was recorded) is taken as this server's: the start
+/// records it.
+pub fn source_change(
+    checkpoint: Option<&Checkpoint>,
+    now: &ServerIdentity,
+    action: SourceChangeAction,
+) -> Result<bool, SourceError> {
+    let Some((at, was)) = checkpoint.and_then(|c| c.server.as_ref().map(|s| (&c.position, s)))
+    else {
+        return Ok(false);
+    };
+    if was == now {
+        return Ok(false);
+    }
+    match action {
+        SourceChangeAction::ReSnapshot => Ok(true),
+        SourceChangeAction::Error => Err(SourceError::Permanent(format!(
+            "WAL_SOURCE_CHANGED: the checkpoint {} was read on {was}, and the pipeline now \
+             reads {now}: a promoted replica's LSNs may repeat ones the lake has marked. Set \
+             source_change_action: re_snapshot to start from this server's position, \
+             snapshotting as snapshots.mode says, or point the pipeline back at the old server",
+            at.untagged()
+        ))),
+    }
+}
+
 pub(crate) async fn slot_state(
     client: &Client,
     name: &str,
@@ -190,6 +249,28 @@ pub(crate) async fn slot_state(
     }))
 }
 
+/// The slot at a source-change start (`source_change_action: re_snapshot`): the stream starts
+/// from the new server's position. `managed` creates the slot there (dropping one that came
+/// over with the failover); `require_existing` takes the DBA's slot, which must exist.
+pub fn on_source_change(
+    slot: Option<&SlotState>,
+    name: &str,
+    management: SlotManagement,
+) -> SlotAction {
+    match (management, slot) {
+        (SlotManagement::Managed, None) => SlotAction::Create(SnapshotReason::SourceChanged),
+        (SlotManagement::Managed, Some(_)) => SlotAction::Recreate(SnapshotReason::SourceChanged),
+        (SlotManagement::RequireExisting, Some(s)) if !s.invalidated => {
+            SlotAction::Adopt(SnapshotReason::SourceChanged)
+        }
+        (SlotManagement::RequireExisting, _) => SlotAction::Refuse(format!(
+            "WAL_SOURCE_CHANGED: the pipeline starts from the new server's position, and \
+             slot_management is require_existing, so replication slot {name} must exist there \
+             and stream: have it created, then restart the pipeline"
+        )),
+    }
+}
+
 impl PgConnector {
     fn slot_name(&self) -> &str {
         &self.pg.slot
@@ -404,8 +485,15 @@ impl PgConnector {
         checkpoint: Option<&Checkpoint>,
         tables: &[TableRef],
     ) -> Result<Prepared, SourceError> {
+        let recorded = checkpoint;
         let checkpoint = checkpoint.map(|c| lsn_of(&c.position)).transpose()?;
         let client = self.conn.client().await?;
+        let (id, timeline) = self
+            .conn
+            .command("read the server's identity", server_identity(&client))
+            .await?;
+        let server = parse_identity(&id, &timeline)?;
+        let changed = source_change(recorded, &server, self.live().source_change_action)?;
         let Reconciled {
             sql,
             rewritten,
@@ -419,14 +507,26 @@ impl PgConnector {
                 slot_state(&client, self.slot_name()),
             )
             .await?;
-        let action = match decide(
-            checkpoint,
-            slot.as_ref(),
-            &self.pipeline.source.db,
-            self.slot_name(),
-            self.pg.slot_management,
-            self.live().slot_on_orphan,
-        ) {
+        let decided = if changed {
+            if let Some(was) = recorded.and_then(|c| c.server.as_ref()) {
+                warnings.push(format!(
+                    "the checkpoint was read on {was}, and the pipeline now reads {server}: \
+                     source_change_action is re_snapshot, so the pipeline re-snapshots from this \
+                     server's position"
+                ));
+            }
+            on_source_change(slot.as_ref(), self.slot_name(), self.pg.slot_management)
+        } else {
+            decide(
+                checkpoint,
+                slot.as_ref(),
+                &self.pipeline.source.db,
+                self.slot_name(),
+                self.pg.slot_management,
+                self.live().slot_on_orphan,
+            )
+        };
+        let action = match decided {
             // pgoutput reads the publication as of each change's WAL position: the WAL from the
             // checkpoint up to now was decoded under the old publication, and what it didn't
             // publish never reaches the stream. Only `managed` rewrites the publication, so the
@@ -456,7 +556,7 @@ impl PgConnector {
             SlotAction::Create(reason) | SlotAction::Recreate(reason) => {
                 snapshot(self.create_slot(&client).await?, reason)
             }
-            SlotAction::Adopt => {
+            SlotAction::Adopt(reason) => {
                 // Advancing decodes the orphan's whole backlog: the snapshot timeout bounds it.
                 let row = self.conn.bounded(
                     "advance the orphan slot",
@@ -467,7 +567,7 @@ impl PgConnector {
                     ),
                 )
                 .await?;
-                snapshot(parse_lsn(row.get(0))?, SnapshotReason::AdoptedOrphan)
+                snapshot(parse_lsn(row.get(0))?, reason)
             }
             SlotAction::Refuse(_) => unreachable!("refused above"),
         };
@@ -479,7 +579,7 @@ impl PgConnector {
             plan,
             added,
             warnings,
-            server: None,
+            server: Some(server),
         })
     }
 
@@ -715,10 +815,13 @@ mod tests {
     fn an_orphan_halts_or_is_adopted() {
         refused(d(None, Some(slot(0x10)), M, HALT), "orphan");
         refused(d(None, Some(slot(0x10)), X, HALT), "orphan");
-        assert_eq!(d(None, Some(slot(0x10)), M, ADOPT), A::Adopt);
+        assert_eq!(
+            d(None, Some(slot(0x10)), M, ADOPT),
+            A::Adopt(R::AdoptedOrphan)
+        );
         assert_eq!(
             d(None, Some(slot(0x10)), X, ADOPT),
-            A::Adopt,
+            A::Adopt(R::AdoptedOrphan),
             "advancing isn't recreating"
         );
         let mut gone = slot(0x10);
```


The reference pages are `cargo xtask docs`'s output, committed with the change:

`docs/src/content/docs/reference/configuration.md`:

```diff
diff --git a/docs/src/content/docs/reference/configuration.md b/docs/src/content/docs/reference/configuration.md
index cdd060e..24b47d4 100644
--- a/docs/src/content/docs/reference/configuration.md
+++ b/docs/src/content/docs/reference/configuration.md
@@ -199,6 +199,7 @@ Rules for the whole block: [V-003](/data-avalanche/reference/validation-rules/#v
 | `heartbeat` | duration | `60s` | applied |  | How often the pipeline confirms its position to the server; a stream silent for three heartbeats, while the heartbeat's reads of the slot fail or find no reader, is reconnected. |
 | `slot_management` | `managed` \| `require_existing` | `managed` | pipeline restart | [V-102](/data-avalanche/reference/validation-rules/#v-102) [V-103](/data-avalanche/reference/validation-rules/#v-103) | `managed` creates the slot when absent; `require_existing` uses a slot a DBA created and needs `auto_cleanup: false`. |
 | `slot_on_orphan` | `halt` \| `adopt_resnapshot` | `halt` | applied | [V-102](/data-avalanche/reference/validation-rules/#v-102) | A slot that exists with no state of the pipeline's: `halt` refuses to start, naming it; `adopt_resnapshot` takes it over and starts from the current position, snapshotting as `snapshots.mode` says (none under `never`). |
+| `source_change_action` | `error` \| `re_snapshot` | `error` | applied | [V-123](/data-avalanche/reference/validation-rules/#v-123) | A start on a server other than the checkpoint's (another system identifier, or another timeline after a failover): `error` halts `WAL_SOURCE_CHANGED`; `re_snapshot` starts from the new server's position, snapshotting as `snapshots.mode` says (none under `never`). |
 | `max_wal_lag_bytes` | byte size or `null` | `null` | applied | [V-104](/data-avalanche/reference/validation-rules/#v-104) | Halt the pipeline when its slot holds more WAL than this, to protect the server's disk; at least `64MiB`. `null` only reports `wal_lag_bytes`. |
 
 ## `pipelines.<name>.source.mysql`
```

`docs/src/content/docs/reference/validation-rules.md`:

```diff
diff --git a/docs/src/content/docs/reference/validation-rules.md b/docs/src/content/docs/reference/validation-rules.md
index ce628d9..a173a3a 100644
--- a/docs/src/content/docs/reference/validation-rules.md
+++ b/docs/src/content/docs/reference/validation-rules.md
@@ -128,3 +128,4 @@ A config with any **error** is rejected whole: at startup the node refuses to st
 | <span id="v-120"></span>V-120 | error | `source.mysql.server_id` | ≥ 1 when set. Unique per source server (host + port) across pipelines, set or derived: two dumps under one id end each other. |
 | <span id="v-121"></span>V-121 | error | `source.mysql.heartbeat` | Between 1s and 4294967s (the server's heartbeat period range). |
 | <span id="v-122"></span>V-122 | error | `globals.cluster.memberlist timers` | gossip_interval, probe_interval, probe_timeout and push_pull_interval above zero; gossip_nodes and retransmit_factor at least 1; probe_timeout below probe_interval. |
+| <span id="v-123"></span>V-123 | error | `source.postgres.source_change_action` | ∈ {error, re_snapshot}. |
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same, then `cargo test -p xtask` (the reference regenerated).

Expected: PASS: config 156, source-pg 142 (the ITs on PostgreSQL 18), xtask 4.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/tests/rules/domain.rs \
  crates/data-avalanche-source-pg/src/connector.rs \
  crates/data-avalanche-source-pg/src/slot.rs \
  crates/data-avalanche-source-pg/tests/pg/slot.rs \
  crates/data-avalanche-config/src/build.rs \
  crates/data-avalanche-config/src/enums.rs \
  crates/data-avalanche-config/src/field_docs.rs \
  crates/data-avalanche-config/src/merge.rs \
  crates/data-avalanche-config/src/mutability.rs \
  crates/data-avalanche-config/src/raw.rs \
  crates/data-avalanche-config/src/registry.rs \
  crates/data-avalanche-config/src/resolved.rs \
  crates/data-avalanche-core/src/source.rs \
  docs/src/content/docs/reference/configuration.md \
  docs/src/content/docs/reference/validation-rules.md
git commit -m "feat(source-pg): the server's identity — the checkpoint records the system identifier and the timeline; another server halts WAL_SOURCE_CHANGED, or under source.postgres.source_change_action: re_snapshot (V-123) starts from its position, the slot made there or the DBA's adopted"
```

---

### Task 3: A changed MySQL server, and MariaDB's position check (`source-mysql`)

**Files:**
- Modify: `crates/data-avalanche-source-mysql/src/start.rs`, `crates/data-avalanche-config/src/field_docs.rs`, `crates/data-avalanche-source-mysql/src/lib.rs`, `crates/data-avalanche-source-mysql/src/stream.rs`, `docs/src/content/docs/reference/configuration.md`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/start.rs`

**Interfaces:**
- Consumes: Task 1's `SnapshotReason::SourceChanged`; `same_server`, `purged`, `purge_action`
  (M7a).
- Produces (re-exported): `pub fn at_boundary(at: &SourcePosition, now: &ServerIdentity, answer:
  Option<Option<String>>, held: bool) -> Result<(), SourceError>`. Crate-internal:
  - `BOUNDARY_QUERY = "SELECT BINLOG_GTID_POS(?, ?)"`;
  - `async fn holds(c: &mut Conn, file: &str) -> Result<bool, mysql_async::Error>`;
  - `MySqlConnector::changed(changed: SourceError, server) -> Result<Prepared, SourceError>`.

`prepare` checks, in this order: the dialect, the server (`same_server`), the purge, then on
MariaDB in file mode the boundary. A NULL for a file the server no longer holds is the purge's to
report. The dump's open checks the server and the boundary on its own connection.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-source-mysql/src/start.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/start.rs b/crates/data-avalanche-source-mysql/src/start.rs
index 1aea8a9..a62a72b 100644
--- a/crates/data-avalanche-source-mysql/src/start.rs
+++ b/crates/data-avalanche-source-mysql/src/start.rs
@@ -380,7 +380,29 @@ mod tests {
         for c in [cp(Some(b)), cp(None)] {
             let e = same_server(&c, &a).unwrap_err();
             assert!(
-                matches!(&e, SourceError::Permanent(m) if m.starts_with("BINLOG_SOURCE_CHANGED: binlog position binlog.000003:154")),
+                matches!(&e, SourceError::Permanent(m) if m.starts_with("BINLOG_SOURCE_CHANGED: binlog position binlog.000003:154")
+                    && m.contains("set binlog_purge_action: re_snapshot")),
+                "{e}"
+            );
+        }
+    }
+
+    #[test]
+    fn a_position_off_an_event_boundary_is_another_server_s() {
+        let a = ServerIdentity::Mariadb { server_id: 1 };
+        assert!(at_boundary(&file(), &a, Some(Some("0-1-20505".into())), true).is_ok());
+        assert!(
+            at_boundary(&file(), &a, Some(Some(String::new())), true).is_ok(),
+            "no GTIDs yet"
+        );
+        assert!(
+            at_boundary(&file(), &a, Some(None), false).is_ok(),
+            "a file purged since is the purge's to report"
+        );
+        for answer in [Some(None), None] {
+            let e = at_boundary(&file(), &a, answer, true).unwrap_err();
+            assert!(
+                matches!(&e, SourceError::Permanent(m) if m.starts_with("BINLOG_SOURCE_CHANGED: binlog position binlog.000003:154 isn't an event's boundary on MariaDB server_id 1")),
                 "{e}"
             );
         }
```

`crates/data-avalanche-source-mysql/tests/mysql/start.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/start.rs b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
index 640247c..03d9916 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/start.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
@@ -126,21 +126,24 @@ async fn re_snapshots(c: &MySqlConnector, cp: &Checkpoint) {
     assert_eq!(p.server, Some(c.server_identity().await.unwrap()));
 }
 
-/// File/position mode halts on a checkpoint from another server; GTIDs carry across servers.
+/// File/position mode: a checkpoint from another server follows `binlog_purge_action` (halts
+/// under `error`, re-snapshots under `re_snapshot`, DA-SRC-29); GTIDs carry across servers.
 #[tokio::test]
 async fn a_checkpoint_from_another_server() {
     require_it!();
     let (n, _a) = fresh("start_server", "").await;
-    let c = connector(&pipeline(&n, &["t"], "", RE_SNAPSHOT));
-    let position = c.position_now().await.unwrap();
+    let halts = connector(&pipeline(&n, &["t"], "", ""));
+    let recovers = connector(&pipeline(&n, &["t"], "", RE_SNAPSHOT));
+    let position = halts.position_now().await.unwrap();
     for server in [Some(elsewhere()), None] {
         let cp = Checkpoint {
             position: position.clone(),
             server,
         };
-        match (c.mode().await.unwrap(), prepare(&c, &cp).await) {
+        match (halts.mode().await.unwrap(), prepare(&halts, &cp).await) {
             (Mode::File, Err(SourceError::Permanent(m))) => {
-                assert!(m.starts_with("BINLOG_SOURCE_CHANGED: "), "{m}")
+                assert!(m.starts_with("BINLOG_SOURCE_CHANGED: "), "{m}");
+                changes_server(&recovers, &cp, "was recorded on").await;
             }
             (Mode::MysqlGtid | Mode::MariadbGtid, Ok(StartPlan::Resume)) => {}
             other => panic!("{cp:?}: {other:?}"),
@@ -149,6 +152,68 @@ async fn a_checkpoint_from_another_server() {
     cleanup(&n).await;
 }
 
+/// Under `re_snapshot`, a checkpoint from another server plans a snapshot from this server's
+/// position now (`SourceChanged`), with a warning naming why.
+async fn changes_server(c: &MySqlConnector, cp: &Checkpoint, why: &str) {
+    let p = SourceConnector::prepare(c, Some(cp), &[])
+        .await
+        .expect("a re-snapshot");
+    let StartPlan::Snapshot { from, reason } = p.plan else {
+        panic!("a snapshot, got {:?}", p.plan)
+    };
+    assert_eq!(reason, SnapshotReason::SourceChanged);
+    assert!(
+        from.covers(&cp.position).unwrap(),
+        "{from} covers {}",
+        cp.position
+    );
+    assert!(
+        p.warnings.len() == 1
+            && p.warnings[0].starts_with("BINLOG_SOURCE_CHANGED: ")
+            && p.warnings[0].contains(why)
+            && p.warnings[0].contains("binlog_purge_action is re_snapshot"),
+        "{:?}",
+        p.warnings
+    );
+    assert_eq!(p.server, Some(c.server_identity().await.unwrap()));
+}
+
+/// MariaDB names a server by `@@server_id` alone: in file/position mode a checkpoint that isn't
+/// an event's boundary on the server is another server's, whatever it records (DA-SRC-21).
+#[tokio::test]
+async fn a_mariadb_position_off_an_event_boundary_is_another_server_s() {
+    require_it!();
+    if target().flavour != Flavour::Mariadb {
+        return;
+    }
+    let (n, _a) = fresh("start_boundary", "").await;
+    let halts = connector(&pipeline(&n, &["t"], "", FILE));
+    let SourcePosition::BinlogFile(now) = halts.position_now().await.unwrap() else {
+        panic!("a file position")
+    };
+    // Offset 5 lies inside the file's format description, which starts at 4.
+    let cp = Checkpoint {
+        position: SourcePosition::parse_tagged(&format!("binlog:{}:5", now.file())).unwrap(),
+        server: Some(halts.server_identity().await.unwrap()),
+    };
+    match prepare(&halts, &cp).await {
+        Err(SourceError::Permanent(m)) => assert!(
+            m.starts_with("BINLOG_SOURCE_CHANGED: ") && m.contains("isn't an event's boundary"),
+            "{m}"
+        ),
+        other => panic!("a halt, got {other:?}"),
+    }
+    let recovers = connector(&pipeline(&n, &["t"], "", &format!("{FILE}{RE_SNAPSHOT}")));
+    changes_server(&recovers, &cp, "isn't an event's boundary").await;
+    // The server's own position resumes.
+    let cp = Checkpoint {
+        position: SourcePosition::BinlogFile(now),
+        server: cp.server,
+    };
+    assert_eq!(prepare(&halts, &cp).await.unwrap(), StartPlan::Resume);
+    cleanup(&n).await;
+}
+
 /// A binlog file the server doesn't hold (GTID purges need a purge on the server:
 /// `tests/mysql_global.rs`).
 #[tokio::test]
@@ -384,6 +449,21 @@ async fn a_file_position_stream_opens_only_on_its_server() {
             other => panic!("{cp:?}: {other:?}"),
         }
     }
+    if target().flavour == Flavour::Mariadb {
+        let SourcePosition::BinlogFile(p) = &now else {
+            panic!("a file position")
+        };
+        let cp = Checkpoint {
+            position: SourcePosition::parse_tagged(&format!("binlog:{}:5", p.file())).unwrap(),
+            server: Some(c.server_identity().await.unwrap()),
+        };
+        match c.open_stream(&cp, &tables).await.err() {
+            Some(SourceError::Permanent(m)) => {
+                assert!(m.contains("isn't an event's boundary"), "{m}")
+            }
+            other => panic!("{cp:?}: {other:?}"),
+        }
+    }
     assert_eq!(c.dump_id(), None, "no dump started");
     if target().replication == "gtid" {
         let gtid = connector(&pipeline(&n, &["t"], "", ""));
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=<target> cargo test -p data-avalanche-source-mysql -- --skip tls::` for `mysql-84`, `mariadb-118` and `mariadb-118-binlog`

Expected: FAIL to compile on each: cannot find function `at_boundary` (E0425).

- [ ] **Step 3: Implement, and regenerate the reference**

`crates/data-avalanche-config/src/field_docs.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
index b5d141a..2defaef 100644
--- a/crates/data-avalanche-config/src/field_docs.rs
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -412,7 +412,7 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
     f(
         "pipelines.*.source.mysql.replication_type",
         Value("gtid"),
-        "`gtid` follows GTIDs and survives a failover (MySQL `gtid_mode=ON`, MariaDB `gtid_strict_mode=1`); `binlog` follows file and position, and halts `BINLOG_SOURCE_CHANGED` when the server changes.",
+        "`gtid` follows GTIDs and survives a failover (MySQL `gtid_mode=ON`, MariaDB `gtid_strict_mode=1`); `binlog` follows file and position, which mean something on one server only: on another, the position is lost, as `binlog_purge_action` says.",
     ),
     f(
         "pipelines.*.source.mysql.tinyint1_as_bool",
@@ -427,7 +427,7 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
     f(
         "pipelines.*.source.mysql.binlog_purge_action",
         Value("error"),
-        "When the server purged the binlog the pipeline must resume from: `error` halts; `re_snapshot` starts from the current position, snapshotting as `snapshots.mode` says (none under `never`).",
+        "When the server can't serve the position the pipeline resumes from (its binlog purged, or under `binlog` another server's): `error` halts; `re_snapshot` starts from the current position, snapshotting as `snapshots.mode` says (none under `never`).",
     ),
     f(
         "pipelines.*.source.mysql.server_id",
```

`crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
index 726d2f6..eaaf954 100644
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -28,7 +28,7 @@ pub use ddl::{Altered, Ddl, Renamed, Scope, classify, renamed, shadow_of};
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
 pub use schema::{diff, narrowed, scope, scope_of};
 pub use server::{Flavour, Fold, Settings, grants_replication, version_of};
-pub use start::{dialect_change, purge_action, same_server};
+pub use start::{at_boundary, dialect_change, purge_action, same_server};
 pub use stream::MySqlStream;
 pub use types::{CatalogColumn, ColumnPlan, Tinyint1AsBool, members, plan_column};
 pub use value::{Charset, Decode, Input, ValueError, decode, decode_after, int_type};
```

`crates/data-avalanche-source-mysql/src/start.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/start.rs b/crates/data-avalanche-source-mysql/src/start.rs
index a62a72b..e8e9e40 100644
--- a/crates/data-avalanche-source-mysql/src/start.rs
+++ b/crates/data-avalanche-source-mysql/src/start.rs
@@ -7,8 +7,12 @@
 //! `binlog_purge_action`: at a start, `re_snapshot` plans a snapshot from the server's position
 //! now (`PositionLost`, with a warning) and `error` halts. A purge the stream meets later (the
 //! dump's error 1236) follows it through [`purge_action`]. In file/position mode a checkpoint
-//! recorded on another server halts the pipeline (`BINLOG_SOURCE_CHANGED`): a position in one
-//! server's files means nothing in another's.
+//! recorded on another server is lost too (DA-SRC-29): a position in one server's files means
+//! nothing in another's, so it is never resumed. `error` halts the pipeline
+//! (`BINLOG_SOURCE_CHANGED`); `re_snapshot` plans a snapshot from the server's position now
+//! (`SourceChanged`: the pipeline resets every table's marker to it). MariaDB names a server by
+//! `@@server_id` alone, which can repeat, so there a position that isn't an event's boundary on
+//! the server (`BINLOG_GTID_POS` NULL, DA-SRC-21) is another server's too.
 
 use std::sync::atomic::Ordering;
 use std::time::{Duration, SystemTime, UNIX_EPOCH};
@@ -105,6 +109,10 @@ pub fn dialect_change(mode: Mode, checkpoint: &SourcePosition) -> Option<String>
     })
 }
 
+/// MariaDB's GTID state at a binlog position: NULL unless the position is an event's boundary
+/// there ([`at_boundary`]).
+pub(crate) const BOUNDARY_QUERY: &str = "SELECT BINLOG_GTID_POS(?, ?)";
+
 /// File/position mode: the checkpoint must be from the connected server (Q-M7-2).
 pub fn same_server(checkpoint: &Checkpoint, now: &ServerIdentity) -> Result<(), SourceError> {
     let at = checkpoint.position.untagged();
@@ -113,13 +121,53 @@ pub fn same_server(checkpoint: &Checkpoint, now: &ServerIdentity) -> Result<(),
         Some(s) => s.to_string(),
         None => "an unrecorded server".to_owned(),
     };
-    Err(SourceError::Permanent(format!(
-        "BINLOG_SOURCE_CHANGED: binlog position {at} was recorded on {recorded}, now connected \
-         to {now}; file/position cannot be carried across servers — switch to replication_type: \
-         gtid or re-snapshot"
+    Err(source_changed(format!(
+        "binlog position {at} was recorded on {recorded}, now connected to {now}"
     )))
 }
 
+/// MariaDB file/position mode: whether `at` is an event's boundary in a binlog of the connected
+/// server, from its answer to `SELECT BINLOG_GTID_POS(<file>, <pos>)`: NULL for a file it doesn't
+/// hold, an offset inside an event or past the end (measured on MariaDB 11.8). A checkpoint is
+/// always one: a transaction's end, a file's start. A NULL for a file the server no longer holds
+/// (`held` false, [`holds`]) is a purge since, which the purge's own checks report.
+pub fn at_boundary(
+    at: &SourcePosition,
+    now: &ServerIdentity,
+    answer: Option<Option<String>>,
+    held: bool,
+) -> Result<(), SourceError> {
+    match answer {
+        Some(Some(_)) => Ok(()),
+        _ if !held => Ok(()),
+        _ => Err(source_changed(format!(
+            "binlog position {} isn't an event's boundary on {now}, so it was recorded on another \
+             server with the same @@server_id",
+            at.untagged()
+        ))),
+    }
+}
+
+/// Whether the server still holds the binlog `file` (`SHOW BINARY LOGS`).
+pub(crate) async fn holds(
+    c: &mut mysql_async::Conn,
+    file: &str,
+) -> Result<bool, mysql_async::Error> {
+    let rows: Vec<mysql_async::Row> = c.query("SHOW BINARY LOGS").await?;
+    Ok(rows
+        .iter()
+        .any(|r| r.get::<String, _>(0).as_deref() == Some(file)))
+}
+
+/// A file/position checkpoint the connected server didn't write, as the pipeline's halt.
+fn source_changed(why: String) -> SourceError {
+    SourceError::Permanent(format!(
+        "BINLOG_SOURCE_CHANGED: {why}; file/position cannot be carried across servers — set \
+         binlog_purge_action: re_snapshot to re-snapshot from this server, or switch to \
+         replication_type: gtid"
+    ))
+}
+
 /// A position the stream can't read on, as `binlog_purge_action` says (G-26): `re_snapshot` keeps
 /// it [`SourceError::PositionUnreachable`], which the pipeline's session answers with a
 /// re-snapshot from a new position; `error` halts the pipeline for the operator. The dump's open
@@ -157,11 +205,21 @@ impl MySqlConnector {
         let lost = match dialect_change(mode, &cp.position) {
             Some(why) => Some(why),
             None => {
-                if mode == Mode::File {
-                    same_server(cp, &server)?;
+                if mode == Mode::File
+                    && let Err(changed) = same_server(cp, &server)
+                {
+                    return self.changed(changed, server).await;
                 }
                 let retention = self.retention(&cp.position).await?;
-                purged(&cp.position, &retention)
+                let lost = purged(&cp.position, &retention);
+                if lost.is_none()
+                    && mode == Mode::File
+                    && matches!(server, ServerIdentity::Mariadb { .. })
+                    && let Err(changed) = self.at_boundary(&cp.position, &server).await
+                {
+                    return self.changed(changed, server).await;
+                }
+                lost
             }
         };
         if let Some(why) = lost {
@@ -197,6 +255,54 @@ impl MySqlConnector {
         })
     }
 
+    /// A file/position checkpoint from another server, as `binlog_purge_action` says: `error`
+    /// halts with `changed`; `re_snapshot` snapshots from the server's position now.
+    async fn changed(
+        &self,
+        changed: SourceError,
+        server: ServerIdentity,
+    ) -> Result<Prepared, SourceError> {
+        if self.live().binlog_purge_action != BinlogPurgeAction::ReSnapshot {
+            return Err(changed);
+        }
+        Ok(Prepared {
+            plan: StartPlan::Snapshot {
+                from: self.position_now().await?,
+                reason: SnapshotReason::SourceChanged,
+            },
+            added: vec![],
+            warnings: vec![format!(
+                "{changed}; binlog_purge_action is re_snapshot, so the pipeline re-snapshots from \
+                 the server's position now"
+            )],
+            server: Some(server),
+        })
+    }
+
+    /// [`at_boundary`] on the connected server.
+    async fn at_boundary(
+        &self,
+        at: &SourcePosition,
+        server: &ServerIdentity,
+    ) -> Result<(), SourceError> {
+        let SourcePosition::BinlogFile(p) = at else {
+            return Ok(());
+        };
+        let (file, pos) = (p.file().to_owned(), p.pos());
+        let (answer, held) = self
+            .conn
+            .command("check the position on the server", move |c| {
+                Box::pin(async move {
+                    let answer: Option<Option<String>> =
+                        c.exec_first(BOUNDARY_QUERY, (file.as_str(), pos)).await?;
+                    let held = matches!(answer, Some(Some(_))) || holds(c, &file).await?;
+                    Ok((answer, held))
+                })
+            })
+            .await?;
+        at_boundary(at, server, answer, held)
+    }
+
     /// The connection id of the open binlog dump, `None` without one: the server's
     /// `information_schema.PROCESSLIST` id of the stream's connection, for status and logs.
     pub fn dump_id(&self) -> Option<u32> {
```

`crates/data-avalanche-source-mysql/src/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/stream.rs b/crates/data-avalanche-source-mysql/src/stream.rs
index 37859ef..c2004b9 100644
--- a/crates/data-avalanche-source-mysql/src/stream.rs
+++ b/crates/data-avalanche-source-mysql/src/stream.rs
@@ -57,7 +57,7 @@ use crate::ddl::{Renamed, classify, renamed};
 use crate::error::mysql;
 use crate::position::{Mode, identity, identity_query};
 use crate::server::{Flavour, Fold};
-use crate::start::{purge_action, same_server};
+use crate::start::{BOUNDARY_QUERY, at_boundary, holds, purge_action, same_server};
 use crate::types::{CatalogColumn, members, plan_column};
 use crate::value::{Charset, Decode, Input, decode_after, halt_reason, text_of};
 
@@ -739,7 +739,18 @@ impl MySqlConnector {
                     .query_first(identity_query(flavour))
                     .await
                     .map_err(err)?;
-                same_server(recorded, &identity(flavour, answer)?)?;
+                let server = identity(flavour, answer)?;
+                same_server(recorded, &server)?;
+                if flavour == Flavour::Mariadb {
+                    let answer: Option<Option<String>> = conn
+                        .exec_first(BOUNDARY_QUERY, (p.file(), p.pos()))
+                        .await
+                        .map_err(err)?;
+                    // A file purged since answers NULL too: the dump's refusal is the purge's.
+                    let held = matches!(answer, Some(Some(_)))
+                        || holds(&mut conn, p.file()).await.map_err(err)?;
+                    at_boundary(from, &server, answer, held)?;
+                }
                 file = p.file().as_bytes().to_vec();
                 req = req.with_filename(&file[..]).with_pos(p.pos());
                 Offsets::new(p.file(), p.pos())
```


`docs/src/content/docs/reference/configuration.md`:

```diff
diff --git a/docs/src/content/docs/reference/configuration.md b/docs/src/content/docs/reference/configuration.md
index 24b47d4..4871f0f 100644
--- a/docs/src/content/docs/reference/configuration.md
+++ b/docs/src/content/docs/reference/configuration.md
@@ -210,10 +210,10 @@ Rules for the whole block: [V-003](/data-avalanche/reference/validation-rules/#v
 
 | Key | Type | Default | On reload | Rules | Description |
 |---|---|---|---|---|---|
-| `replication_type` | `binlog` \| `gtid` | `gtid` | pipeline restart | [V-021](/data-avalanche/reference/validation-rules/#v-021) | `gtid` follows GTIDs and survives a failover (MySQL `gtid_mode=ON`, MariaDB `gtid_strict_mode=1`); `binlog` follows file and position, and halts `BINLOG_SOURCE_CHANGED` when the server changes. |
+| `replication_type` | `binlog` \| `gtid` | `gtid` | pipeline restart | [V-021](/data-avalanche/reference/validation-rules/#v-021) | `gtid` follows GTIDs and survives a failover (MySQL `gtid_mode=ON`, MariaDB `gtid_strict_mode=1`); `binlog` follows file and position, which mean something on one server only: on another, the position is lost, as `binlog_purge_action` says. |
 | `tinyint1_as_bool` | boolean | `true` | pipeline restart |  | Read `TINYINT(1)` columns as booleans. |
 | `tinyint1_detection` | `information_schema` \| `none` | `information_schema` | pipeline restart | [V-081](/data-avalanche/reference/validation-rules/#v-081) | Where `TINYINT(1)` is recognised: the catalog (`information_schema`), or `none` to read every `TINYINT` as an integer. |
-| `binlog_purge_action` | `error` \| `re_snapshot` | `error` | applied | [V-105](/data-avalanche/reference/validation-rules/#v-105) | When the server purged the binlog the pipeline must resume from: `error` halts; `re_snapshot` starts from the current position, snapshotting as `snapshots.mode` says (none under `never`). |
+| `binlog_purge_action` | `error` \| `re_snapshot` | `error` | applied | [V-105](/data-avalanche/reference/validation-rules/#v-105) | When the server can't serve the position the pipeline resumes from (its binlog purged, or under `binlog` another server's): `error` halts; `re_snapshot` starts from the current position, snapshotting as `snapshots.mode` says (none under `never`). |
 | `server_id` | integer | derived from the pipeline's id, in 2147483648…4294967295 | pipeline restart | [V-120](/data-avalanche/reference/validation-rules/#v-120) | The replica id the binlog stream registers with; each pipeline needs its own, or the server ends one of the streams. |
 | `heartbeat` | duration | `60s` | applied | [V-121](/data-avalanche/reference/validation-rules/#v-121) | How often an idle server sends a binlog heartbeat; a stream silent for three heartbeats, while the heartbeat's checks fail or find no binlog dump of its connection on the server, is reconnected. |
 
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same three, then `cargo test -p data-avalanche-config -p xtask`.

Expected: PASS: 142 tests on each target; config 156, xtask 4.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/src/start.rs \
  crates/data-avalanche-source-mysql/tests/mysql/start.rs \
  crates/data-avalanche-config/src/field_docs.rs \
  crates/data-avalanche-source-mysql/src/lib.rs \
  crates/data-avalanche-source-mysql/src/stream.rs \
  docs/src/content/docs/reference/configuration.md
git commit -m "feat(source-mysql): a changed server follows binlog_purge_action — re_snapshot starts from its position, every table repositioned; MariaDB's file position checked by BINLOG_GTID_POS at the start and the dump's open"
```

---

### Task 4: A DBA's slot made again, and two texts (`source-pg`, `config`, the binary)

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/slot.rs`, `crates/data-avalanche/src/cli.rs`, `crates/data-avalanche-config/src/field_docs.rs`, `docs/src/content/docs/reference/configuration.md`
- Test: `crates/data-avalanche-source-pg/tests/pg/slot.rs`

**Interfaces:**
- Consumes: Task 2's `SlotAction::Adopt(SnapshotReason)`.
- Produces: `decide`'s `require_existing` arms. A slot whose `confirmed_flush_lsn` is past the
  checkpoint is `Adopt(PositionLost)` under `adopt_resnapshot`, and is refused under `halt`. An
  absent or invalidated slot is refused with the `remake` text. The CLI's `about` is
  `"Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake"`.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-source-pg/src/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
index 473c8f9..961418f 100644
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -789,7 +789,10 @@ mod tests {
     #[test]
     fn state_without_slot_recreates_under_managed() {
         assert_eq!(d(Some(0x20), None, M, HALT), A::Create(R::PositionLost));
-        refused(d(Some(0x20), None, X, HALT), "doesn't exist");
+        refused(
+            d(Some(0x20), None, X, HALT),
+            "doesn't exist; slot_management is require_existing, so create it again",
+        );
     }
 
     #[test]
@@ -798,22 +801,43 @@ mod tests {
             d(Some(0x10), Some(slot(0x20)), M, HALT),
             A::Recreate(R::PositionLost)
         );
-        refused(
-            d(Some(0x10), Some(slot(0x20)), X, HALT),
-            "past the pipeline's checkpoint",
-        );
         let mut gone = slot(0x10);
         gone.invalidated = true;
         assert_eq!(
             d(Some(0x20), Some(gone.clone()), M, HALT),
             A::Recreate(R::PositionLost)
         );
-        refused(d(Some(0x20), Some(gone), X, HALT), "invalidated");
+        refused(
+            d(Some(0x20), Some(gone), X, HALT),
+            "was invalidated; drop it: slot_management is require_existing, so create it again, with slot_on_orphan: adopt_resnapshot",
+        );
+    }
+
+    /// Under `require_existing` the DBA makes a lost slot again, past the checkpoint: adopted on
+    /// request, like an orphan (DA-SRC-30).
+    #[test]
+    fn a_slot_made_again_under_require_existing_is_adopted_on_request() {
+        refused(
+            d(Some(0x10), Some(slot(0x20)), X, HALT),
+            "confirmed 0/20, past the pipeline's checkpoint 0/10: the changes between are lost (a slot made again); slot_management is require_existing, so set slot_on_orphan: adopt_resnapshot",
+        );
+        assert_eq!(
+            d(Some(0x10), Some(slot(0x20)), X, ADOPT),
+            A::Adopt(R::PositionLost)
+        );
+        assert_eq!(
+            d(Some(0x20), Some(slot(0x20)), X, ADOPT),
+            A::Resume,
+            "at the checkpoint it is the pipeline's own"
+        );
     }
 
     #[test]
     fn an_orphan_halts_or_is_adopted() {
-        refused(d(None, Some(slot(0x10)), M, HALT), "orphan");
+        refused(
+            d(None, Some(slot(0x10)), M, HALT),
+            "(an orphan): drop the slot for a first start, restore the pipeline state, or set slot_on_orphan: adopt_resnapshot",
+        );
         refused(d(None, Some(slot(0x10)), X, HALT), "orphan");
         assert_eq!(
             d(None, Some(slot(0x10)), M, ADOPT),
```

`crates/data-avalanche-source-pg/tests/pg/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/slot.rs b/crates/data-avalanche-source-pg/tests/pg/slot.rs
index e0cd298..a15d2ce 100644
--- a/crates/data-avalanche-source-pg/tests/pg/slot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/slot.rs
@@ -303,6 +303,37 @@ async fn require_existing_never_creates() {
         "require_existing",
     );
     permanent(c.reset_position().await.unwrap_err(), "require_existing");
+
+    // The DBA makes it again, past the checkpoint: refused, then adopted on request (DA-SRC-30).
+    a.execute(&format!("INSERT INTO {}.a VALUES (1)", n.schema), &[])
+        .await
+        .unwrap();
+    a.execute(
+        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
+        &[&n.slot],
+    )
+    .await
+    .unwrap();
+    permanent(
+        c.prepare(Some(&Checkpoint::from(start.clone())), &tables)
+            .await
+            .unwrap_err(),
+        "a slot made again",
+    );
+    c.reconfigure(&pipeline(
+        &n,
+        &["*"],
+        "",
+        &format!("{postgres}, slot_on_orphan: adopt_resnapshot"),
+    ));
+    let p = c
+        .prepare(Some(&Checkpoint::from(start.clone())), &tables)
+        .await
+        .unwrap();
+    let SourcePosition::Pg(at) = from(&p.plan, SnapshotReason::PositionLost) else {
+        panic!("a PostgreSQL position")
+    };
+    assert_eq!(confirmed(&a, &n).await, Some(at), "advanced, not recreated");
     c.teardown().await.unwrap();
     assert_eq!(
         published(&a, &n).await,
```

`crates/data-avalanche/src/cli.rs`:

```diff
diff --git a/crates/data-avalanche/src/cli.rs b/crates/data-avalanche/src/cli.rs
index dbf0de2..0d7b01f 100644
--- a/crates/data-avalanche/src/cli.rs
+++ b/crates/data-avalanche/src/cli.rs
@@ -64,6 +64,16 @@ mod tests {
         assert_eq!(args.log_format, Some(LogFormatArg::Pretty));
     }
 
+    #[test]
+    fn the_help_names_every_source() {
+        use clap::CommandFactory;
+        let about = Cli::command().get_about().map(|a| a.to_string());
+        assert_eq!(
+            about.as_deref(),
+            Some("Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake")
+        );
+    }
+
     #[test]
     fn run_needs_a_config() {
         let e = Cli::try_parse_from(["data-avalanche", "run"]).unwrap_err();
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg -- --test-threads=1 --skip tls::` and `cargo test -p data-avalanche --lib the_help_names_every_source`

Expected:
- source-pg: FAIL, 4 of 85 unit tests (`a_slot_made_again_under_require_existing_is_adopted_on_request`,
  `a_slot_past_the_checkpoint_or_invalidated_lost_the_position`, `an_orphan_halts_or_is_adopted`,
  `state_without_slot_recreates_under_managed`): the refusals don't name `slot_on_orphan` or say
  "create it again", e.g. `"replication slot s doesn't exist and slot_management is
  require_existing" should mention "doesn't exist; slot_management is require_existing, so create
  it again"`.
- the binary: FAIL, `left: Some("Change data capture from PostgreSQL into Delta Lake")`.

- [ ] **Step 3: Implement, and regenerate the reference**

`crates/data-avalanche-config/src/field_docs.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
index 2defaef..0a62b76 100644
--- a/crates/data-avalanche-config/src/field_docs.rs
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -392,12 +392,12 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
     f(
         "pipelines.*.source.postgres.slot_management",
         Value("managed"),
-        "`managed` creates the slot when absent; `require_existing` uses a slot a DBA created and needs `auto_cleanup: false`.",
+        "`managed` creates the slot when absent; `require_existing` uses a slot a DBA created and needs `auto_cleanup: false` (a slot the DBA makes again is adopted as `slot_on_orphan` says).",
     ),
     f(
         "pipelines.*.source.postgres.slot_on_orphan",
         Value("halt"),
-        "A slot that exists with no state of the pipeline's: `halt` refuses to start, naming it; `adopt_resnapshot` takes it over and starts from the current position, snapshotting as `snapshots.mode` says (none under `never`).",
+        "A slot that exists with no state of the pipeline's, or under `require_existing` one made again past the pipeline's position: `halt` refuses to start, naming it; `adopt_resnapshot` takes it over and starts from the current position, snapshotting as `snapshots.mode` says (none under `never`).",
     ),
     f(
         "pipelines.*.source.postgres.source_change_action",
```

`crates/data-avalanche-source-pg/src/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
index 961418f..7d9aa61 100644
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -53,15 +53,18 @@ pub enum SlotAction {
     /// Drop the unusable slot and create it again, then snapshot.
     Recreate(SnapshotReason),
     /// Move the slot to the current WAL position (`pg_replication_slot_advance`), then snapshot:
-    /// an orphan under `on_orphan: adopt_resnapshot`, or the slot of a source change. Advancing
-    /// works under `require_existing` too.
+    /// an orphan under `on_orphan: adopt_resnapshot`, under `require_existing` also a slot made
+    /// again past the checkpoint, or the slot of a source change. Advancing works under
+    /// `require_existing` too.
     Adopt(SnapshotReason),
     /// Refuse to start: the operator decides.
     Refuse(String),
 }
 
 /// The slot state machine (`Postgres_Slot…` §1–§3), one cell per combination of pipeline state
-/// (`checkpoint`) and server slot (`slot`).
+/// (`checkpoint`) and server slot (`slot`). Under `require_existing` the pipeline never drops or
+/// creates a slot: one the DBA makes again after the pipeline's was lost starts past the
+/// checkpoint, and is an orphan, adopted under `on_orphan: adopt_resnapshot` (DA-SRC-30).
 pub fn decide(
     checkpoint: Option<Lsn>,
     slot: Option<&SlotState>,
@@ -86,13 +89,14 @@ pub fn decide(
             None
         }
     };
+    // What a DBA does under `require_existing`: make the slot (again), and let the pipeline
+    // adopt it.
+    let remake = "slot_management is require_existing, so create it again, with slot_on_orphan: adopt_resnapshot for the pipeline to adopt it and re-snapshot";
     let lost = |why: &str| {
         if managed {
             SlotAction::Recreate(SnapshotReason::PositionLost)
         } else {
-            SlotAction::Refuse(format!(
-                "replication slot {name} {why}; slot_management is require_existing, so recreate it and restart: the pipeline re-snapshots"
-            ))
+            SlotAction::Refuse(format!("replication slot {name} {why}; drop it: {remake}"))
         }
     };
     match (checkpoint, slot) {
@@ -104,19 +108,25 @@ pub fn decide(
                 return lost("was invalidated");
             }
             match s.confirmed_flush {
-                Some(flushed) if flushed > at => lost(&format!(
-                    "confirmed {flushed}, past the pipeline's checkpoint {at}"
-                )),
+                Some(flushed) if flushed > at => match (managed, orphan) {
+                    (true, _) => SlotAction::Recreate(SnapshotReason::PositionLost),
+                    (false, SlotOnOrphan::AdoptResnapshot) => {
+                        SlotAction::Adopt(SnapshotReason::PositionLost)
+                    }
+                    (false, SlotOnOrphan::Halt) => SlotAction::Refuse(format!(
+                        "replication slot {name} confirmed {flushed}, past the pipeline's checkpoint {at}: the changes between are lost (a slot made again); slot_management is require_existing, so set slot_on_orphan: adopt_resnapshot for the pipeline to adopt it and re-snapshot"
+                    )),
+                },
                 _ => SlotAction::Resume,
             }
         }
         (Some(_), None) if managed => SlotAction::Create(SnapshotReason::PositionLost),
-        (Some(_), None) => SlotAction::Refuse(format!(
-            "replication slot {name} doesn't exist and slot_management is require_existing"
-        )),
+        (Some(_), None) => {
+            SlotAction::Refuse(format!("replication slot {name} doesn't exist; {remake}"))
+        }
         (None, Some(s)) => match orphan {
             SlotOnOrphan::Halt => SlotAction::Refuse(format!(
-                "replication slot {name} exists but the pipeline has no state (an orphan): drop the slot for a first start, restore the pipeline state, or set slot.on_orphan: adopt_resnapshot"
+                "replication slot {name} exists but the pipeline has no state (an orphan): drop the slot for a first start, restore the pipeline state, or set slot_on_orphan: adopt_resnapshot"
             )),
             SlotOnOrphan::AdoptResnapshot => match not_ours(s) {
                 Some(m) => SlotAction::Refuse(m),
```

`crates/data-avalanche/src/cli.rs`:

```diff
diff --git a/crates/data-avalanche/src/cli.rs b/crates/data-avalanche/src/cli.rs
index 0d7b01f..c836f62 100644
--- a/crates/data-avalanche/src/cli.rs
+++ b/crates/data-avalanche/src/cli.rs
@@ -8,7 +8,7 @@ use clap::{Args, Parser, Subcommand, ValueEnum};
 #[command(
     name = "data-avalanche",
     version,
-    about = "Change data capture from PostgreSQL into Delta Lake"
+    about = "Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake"
 )]
 pub struct Cli {
     #[command(subcommand)]
```


`docs/src/content/docs/reference/configuration.md`:

```diff
diff --git a/docs/src/content/docs/reference/configuration.md b/docs/src/content/docs/reference/configuration.md
index 4871f0f..5b1ecc8 100644
--- a/docs/src/content/docs/reference/configuration.md
+++ b/docs/src/content/docs/reference/configuration.md
@@ -197,8 +197,8 @@ Rules for the whole block: [V-003](/data-avalanche/reference/validation-rules/#v
 | `slot` | string | — | pipeline restart | [V-011](/data-avalanche/reference/validation-rules/#v-011) [V-058](/data-avalanche/reference/validation-rules/#v-058) | The logical replication slot. Lowercase letters, digits and `_`; unique per server (host and port) across pipelines. |
 | `auto_cleanup` | boolean | `true` | applied |  | Drop the slot and the publication when the pipeline is removed or its `id` changes. |
 | `heartbeat` | duration | `60s` | applied |  | How often the pipeline confirms its position to the server; a stream silent for three heartbeats, while the heartbeat's reads of the slot fail or find no reader, is reconnected. |
-| `slot_management` | `managed` \| `require_existing` | `managed` | pipeline restart | [V-102](/data-avalanche/reference/validation-rules/#v-102) [V-103](/data-avalanche/reference/validation-rules/#v-103) | `managed` creates the slot when absent; `require_existing` uses a slot a DBA created and needs `auto_cleanup: false`. |
-| `slot_on_orphan` | `halt` \| `adopt_resnapshot` | `halt` | applied | [V-102](/data-avalanche/reference/validation-rules/#v-102) | A slot that exists with no state of the pipeline's: `halt` refuses to start, naming it; `adopt_resnapshot` takes it over and starts from the current position, snapshotting as `snapshots.mode` says (none under `never`). |
+| `slot_management` | `managed` \| `require_existing` | `managed` | pipeline restart | [V-102](/data-avalanche/reference/validation-rules/#v-102) [V-103](/data-avalanche/reference/validation-rules/#v-103) | `managed` creates the slot when absent; `require_existing` uses a slot a DBA created and needs `auto_cleanup: false` (a slot the DBA makes again is adopted as `slot_on_orphan` says). |
+| `slot_on_orphan` | `halt` \| `adopt_resnapshot` | `halt` | applied | [V-102](/data-avalanche/reference/validation-rules/#v-102) | A slot that exists with no state of the pipeline's, or under `require_existing` one made again past the pipeline's position: `halt` refuses to start, naming it; `adopt_resnapshot` takes it over and starts from the current position, snapshotting as `snapshots.mode` says (none under `never`). |
 | `source_change_action` | `error` \| `re_snapshot` | `error` | applied | [V-123](/data-avalanche/reference/validation-rules/#v-123) | A start on a server other than the checkpoint's (another system identifier, or another timeline after a failover): `error` halts `WAL_SOURCE_CHANGED`; `re_snapshot` starts from the new server's position, snapshotting as `snapshots.mode` says (none under `never`). |
 | `max_wal_lag_bytes` | byte size or `null` | `null` | applied | [V-104](/data-avalanche/reference/validation-rules/#v-104) | Halt the pipeline when its slot holds more WAL than this, to protect the server's disk; at least `64MiB`. `null` only reports `wal_lag_bytes`. |
 
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same, then `cargo test -p data-avalanche-config -p xtask`.

Expected: PASS: source-pg 143, the binary's test, config 156, xtask 4.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/src/slot.rs \
  crates/data-avalanche-source-pg/tests/pg/slot.rs \
  crates/data-avalanche/src/cli.rs \
  crates/data-avalanche-config/src/field_docs.rs \
  docs/src/content/docs/reference/configuration.md
git commit -m "feat(source-pg): a slot a DBA made again under require_existing is adopted under slot_on_orphan: adopt_resnapshot, refused naming it otherwise; the refusals say slot_on_orphan, the CLI names its three sources"
```

---

### Task 5: MariaDB rollbacks (`source-mysql`)

**Files:**
- Modify: `crates/data-avalanche-source-mysql/src/stream.rs`, `crates/data-avalanche-source-mysql/src/binlog.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/stream.rs`

**Interfaces:**
- Consumes: `MariaGtidEvent` (M7a), the stream's `emit`/`commit`/`statement` (M7a), the
  large-transaction split (M7b).
- Produces:
  - `MariaGtidEvent::TRANSACTIONAL: u8 = 0x04` and `fn transactional(&self) -> bool`;
  - in `stream.rs`:
    - `struct Held { items, savepoints: Vec<(String, usize)>, bytes }`, with `savepoint`,
      `release` and `roll_back(name) -> Vec<StreamItem>`;
    - `fn savepoint_name(sql) -> Option<String>`;
    - `MySqlStream { transactional: HashSet<TableRef>, held: Option<Held>, hold_limit: u64 }`;
    - `MySqlConnector::transactional(tables, fold) -> Result<HashSet<TableRef>, SourceError>`.

The rows a held transaction emits go to `held`. A `COMMIT`, an XID or a statement lets them out,
in order, first. Past `hold_limit` bytes they go out at once and the hold ends.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-source-mysql/src/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/stream.rs b/crates/data-avalanche-source-mysql/src/stream.rs
index c2004b9..5096593 100644
--- a/crates/data-avalanche-source-mysql/src/stream.rs
+++ b/crates/data-avalanche-source-mysql/src/stream.rs
@@ -1396,6 +1396,89 @@ impl ChangeStream for MySqlStream {
 mod tests {
     use super::*;
 
+    fn held(ids: &[u64]) -> Vec<StreamItem> {
+        ids.iter()
+            .map(|&xid| StreamItem::Begin {
+                xid,
+                commit_ts_micros: 0,
+            })
+            .collect()
+    }
+
+    fn xids(items: &[StreamItem]) -> Vec<u64> {
+        items
+            .iter()
+            .map(|i| match i {
+                StreamItem::Begin { xid, .. } => *xid,
+                other => panic!("{other:?}"),
+            })
+            .collect()
+    }
+
+    /// A rollback to a savepoint takes out what came after it, and the savepoints after it; a
+    /// whole rollback, or one to a savepoint the binlog doesn't hold, takes out everything.
+    #[test]
+    fn a_rollback_takes_out_what_it_undid() {
+        let mut h = Held {
+            items: held(&[1]),
+            ..Held::default()
+        };
+        h.savepoint("a".into());
+        h.items.extend(held(&[2]));
+        h.savepoint("b".into());
+        h.items.extend(held(&[3]));
+        assert_eq!(xids(&h.roll_back(Some("b"))), [3]);
+        h.items.extend(held(&[4]));
+        assert_eq!(xids(&h.roll_back(Some("a"))), [2, 4]);
+        assert_eq!(
+            h.savepoints,
+            [("a".to_owned(), 1)],
+            "b went with the rollback to a"
+        );
+        assert_eq!(xids(&h.roll_back(Some("a"))), [] as [u64; 0], "a stays");
+        h.items.extend(held(&[5]));
+        h.release("a");
+        assert_eq!(
+            xids(&h.roll_back(Some("a"))),
+            [1, 5],
+            "released: as if never logged"
+        );
+        h.items = held(&[6]);
+        h.savepoint("c".into());
+        h.items.extend(held(&[7]));
+        h.savepoint("c".into());
+        h.items.extend(held(&[8]));
+        assert_eq!(
+            xids(&h.roll_back(Some("c"))),
+            [8],
+            "the later c replaced the first"
+        );
+        assert_eq!(xids(&h.roll_back(None)), [6, 7]);
+        assert!(h.savepoints.is_empty());
+    }
+
+    #[test]
+    fn a_savepoint_is_named_without_case_or_quotes() {
+        assert_eq!(savepoint_name("SAVEPOINT `Sp1`"), "sp1");
+        assert_eq!(savepoint_name("ROLLBACK TO `a``b`"), "a`b");
+        assert_eq!(savepoint_name("ROLLBACK TO SAVEPOINT s2"), "s2");
+        assert_eq!(savepoint_name("RELEASE SAVEPOINT `x y`"), "x y");
+    }
+
+    #[test]
+    fn a_mariadb_transaction_says_whether_it_can_roll_back_logged_rows() {
+        let flags = |f: u8| MariaGtidEvent {
+            gtid: MariaGtid {
+                domain: 0,
+                server_id: 1,
+                seq: 1,
+            },
+            flags: f,
+        };
+        assert!(flags(0x0c).transactional(), "trans, allow parallel");
+        assert!(!flags(0x08).transactional());
+    }
+
     #[test]
     fn a_gtid_set_becomes_sid_blocks_per_uuid_and_tag() {
         let set: GtidSet = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7:alpha:1-3,4e11fa47-71ca-11e1-9e33-c80aa9429562:9"
```

`crates/data-avalanche-source-mysql/tests/mysql/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/stream.rs b/crates/data-avalanche-source-mysql/tests/mysql/stream.rs
index cd12023..4f6f3e5 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/stream.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/stream.rs
@@ -392,11 +392,13 @@ async fn untracked_writes_keep_alive_before_a_heartbeat() {
 }
 
 /// MariaDB logs the rows a rollback undid in a transaction with non-transactional changes, then
-/// `ROLLBACK TO` (a savepoint) or `ROLLBACK` (the whole transaction): with tracked rows among them
-/// the pipeline halts. MySQL without GTIDs logs none of them; with GTIDs it refuses the
-/// transaction (`enforce_gtid_consistency`). Measured on MySQL 8.4 and MariaDB 10.11–11.8.
+/// `ROLLBACK TO` (a savepoint) or `ROLLBACK` (the savepoint came before its first logged row):
+/// the stream drops them, and only the rows that stand arrive (DA-SRC-26). The MyISAM table's
+/// rows are logged in a transaction of their own, first, and stand. MySQL without GTIDs logs none
+/// of the undone rows; with GTIDs it refuses the transaction (`enforce_gtid_consistency`).
+/// Measured on MySQL 8.4 and MariaDB 10.11–11.8.
 #[tokio::test]
-async fn a_rollback_after_tracked_rows_halts_the_stream() {
+async fn a_rollback_drops_the_rows_it_undid() {
     require_it!();
     let (n, mut a) = fresh(
         "stream_rollback",
@@ -404,12 +406,15 @@ async fn a_rollback_after_tracked_rows_halts_the_stream() {
          CREATE TABLE m (id INT PRIMARY KEY) ENGINE = MyISAM;",
     )
     .await;
-    let c = connector(&pipeline(&n, &["t"], "", ""));
+    let c = connector(&pipeline(&n, &["t", "m"], "", ""));
     let tables = c.resolve_tables().await.unwrap().tables;
     let to_savepoint = "BEGIN; INSERT INTO t VALUES (1); SAVEPOINT s; INSERT INTO t VALUES (2);
          INSERT INTO m VALUES (2); ROLLBACK TO s; COMMIT;";
     let whole = "BEGIN; SAVEPOINT s; INSERT INTO t VALUES (3); INSERT INTO m VALUES (3);
          ROLLBACK TO s; COMMIT;";
+    let nested = "BEGIN; INSERT INTO t VALUES (4); SAVEPOINT a; INSERT INTO t VALUES (5);
+         SAVEPOINT b; INSERT INTO t VALUES (6); INSERT INTO m VALUES (6); ROLLBACK TO a;
+         INSERT INTO t VALUES (7); COMMIT;";
     if target().flavour == Flavour::Mysql && target().replication == "gtid" {
         match a.query_drop(whole).await {
             Err(mysql_async::Error::Server(e)) => assert_eq!(e.code, 1785, "{e}"),
@@ -419,37 +424,22 @@ async fn a_rollback_after_tracked_rows_halts_the_stream() {
         cleanup(&n).await;
         return;
     }
-    for (sql, logged) in [
-        (to_savepoint, "a savepoint rollback"),
-        (whole, "a ROLLBACK"),
+    // The ids that stand, and their transactions: the MyISAM row's own, then the rest.
+    for (sql, want, commits) in [
+        (to_savepoint, &[2, 1, 9, 9][..], 4),
+        (whole, &[3, 9, 9][..], 3),
+        (nested, &[6, 4, 7, 9, 9][..], 4),
     ] {
         let mut s = open(&c, &tables).await;
         a.query_drop(sql).await.unwrap();
         a.query_drop("INSERT INTO t VALUES (9); DELETE FROM t WHERE id = 9")
             .await
             .unwrap();
-        match target().flavour {
-            Flavour::Mariadb => {
-                let e = error(&mut s).await;
-                assert!(
-                    matches!(&e, SourceError::Permanent(m) if m.starts_with(logged) && m.contains("can't tell which rows stand")),
-                    "{e}"
-                );
-            }
-            Flavour::Mysql => {
-                // Only the rows that stand: the savepoint's own transaction keeps row 1.
-                let want: &[i32] = if sql == to_savepoint {
-                    &[1, 9, 9]
-                } else {
-                    &[9, 9]
-                };
-                let got = changes(&items(&mut s, want.len()).await);
-                let ids: Vec<_> = got.iter().map(|(_, id)| id.clone()).collect();
-                let want: Vec<_> = want.iter().map(|i| Some(ColumnValue::Int32(*i))).collect();
-                assert_eq!(ids, want);
-                shut(s).await;
-            }
-        }
+        let got = changes(&items(&mut s, commits).await);
+        let ids: Vec<_> = got.iter().map(|(_, id)| id.clone()).collect();
+        let want: Vec<_> = want.iter().map(|i| Some(ColumnValue::Int32(*i))).collect();
+        assert_eq!(ids, want, "{sql}");
+        shut(s).await;
     }
     cleanup(&n).await;
 }
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=<target> cargo test -p data-avalanche-source-mysql -- --skip tls::` for the three targets

Expected: FAIL to compile on each: cannot find function `savepoint_name` (E0425), type `Held`
(E0433, E0422), no method `MariaGtidEvent::transactional` (E0599).

- [ ] **Step 3: Implement**

`crates/data-avalanche-source-mysql/src/binlog.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/binlog.rs b/crates/data-avalanche-source-mysql/src/binlog.rs
index d82e94b..1cd275a 100644
--- a/crates/data-avalanche-source-mysql/src/binlog.rs
+++ b/crates/data-avalanche-source-mysql/src/binlog.rs
@@ -79,6 +79,8 @@ pub struct MariaGtidEvent {
 impl MariaGtidEvent {
     /// The statement stands alone: no `BEGIN`, no `XID` (DDL).
     pub const STANDALONE: u8 = 0x01;
+    /// Every change in the transaction can roll back: none is to a non-transactional table.
+    pub const TRANSACTIONAL: u8 = 0x04;
     /// The XA transaction's prepare, and its later commit or rollback.
     pub const PREPARED_XA: u8 = 0x40;
     pub const COMPLETED_XA: u8 = 0x80;
@@ -104,6 +106,12 @@ impl MariaGtidEvent {
         self.flags & Self::STANDALONE != 0
     }
 
+    /// No change in the transaction is to a non-transactional table (`FL_TRANSACTIONAL`): the
+    /// binlog holds no row a rollback undid.
+    pub fn transactional(&self) -> bool {
+        self.flags & Self::TRANSACTIONAL != 0
+    }
+
     /// An XA transaction's prepare or completion: its rows can still roll back after the event.
     pub fn xa(&self) -> bool {
         self.flags & (Self::PREPARED_XA | Self::COMPLETED_XA) != 0
```

`crates/data-avalanche-source-mysql/src/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/stream.rs b/crates/data-avalanche-source-mysql/src/stream.rs
index 5096593..63be306 100644
--- a/crates/data-avalanche-source-mysql/src/stream.rs
+++ b/crates/data-avalanche-source-mysql/src/stream.rs
@@ -21,14 +21,20 @@
 //!   without case.
 //! - **Fail closed.** A compressed transaction that yields less than its size halts the pipeline
 //!   (`BINLOG_PAYLOAD_INVALID`, D-V1.6); so does a two-phase XA prepare of tracked rows (they can
-//!   roll back after they are logged), a rollback logged after tracked rows (MariaDB keeps the
-//!   rows a savepoint or a whole rollback undid, measured on 10.11–11.8), an incident event (the
-//!   server lost events), and any event type the source doesn't read.
+//!   roll back after they are logged), a rollback over tracked rows the stream already gave out,
+//!   an incident event (the server lost events), and any event type the source doesn't read.
+//! - **Rollbacks (DA-SRC-26).** MariaDB logs the rows a `ROLLBACK TO` a savepoint undid, or a
+//!   whole `ROLLBACK`'s, in a transaction with non-transactional changes (measured on 10.11–11.8),
+//!   whose GTID event lacks `FL_TRANSACTIONAL`. Such a transaction's items are [`Held`] to its
+//!   end, and a rollback drops those it undid: the ones after the logged `SAVEPOINT`, or all. A
+//!   non-transactional table's rows are logged in a transaction of their own and stand; one among
+//!   the undone rows halts the pipeline, as do held rows past `sink.max_transaction_buffer_bytes`
+//!   (they go out unheld, and a rollback over them halts).
 //!
 //! [`ChangeStream::next`] is cancel-safe: an event is processed without an await, and the one
 //! await inside a transaction (channel C after a statement) keeps its work until it completes.
 
-use std::collections::{HashMap, VecDeque};
+use std::collections::{HashMap, HashSet, VecDeque};
 use std::sync::atomic::Ordering;
 use std::time::{Duration, Instant};
 
@@ -107,6 +113,61 @@ struct Txn {
     begun: bool,
 }
 
+/// The open MariaDB transaction's items, held to its end because it may log rows a rollback
+/// undid (DA-SRC-26).
+#[derive(Debug, Default)]
+struct Held {
+    items: Vec<StreamItem>,
+    /// Each logged savepoint, lower-case, with the number of items held when it was set.
+    savepoints: Vec<(String, usize)>,
+    /// The binlog bytes of the rows events held.
+    bytes: u64,
+}
+
+impl Held {
+    /// `SAVEPOINT name`: one of the same name moves here.
+    fn savepoint(&mut self, name: String) {
+        self.savepoints.retain(|(s, _)| *s != name);
+        self.savepoints.push((name, self.items.len()));
+    }
+
+    /// `RELEASE SAVEPOINT name`: it and the savepoints after it go.
+    fn release(&mut self, name: &str) {
+        if let Some(i) = self.savepoints.iter().position(|(s, _)| s == name) {
+            self.savepoints.truncate(i);
+        }
+    }
+
+    /// `ROLLBACK TO name` (`Some`) or `ROLLBACK` (`None`): the items it undid, taken out. A
+    /// savepoint the binlog doesn't hold was set before the transaction's first logged event, so
+    /// the rollback undoes them all; the savepoints after the named one go.
+    fn roll_back(&mut self, name: Option<&str>) -> Vec<StreamItem> {
+        let found = name.and_then(|n| self.savepoints.iter().position(|(s, _)| s == n));
+        let mark = match found {
+            Some(i) => {
+                self.savepoints.truncate(i + 1);
+                self.savepoints[i].1
+            }
+            None => {
+                self.savepoints.clear();
+                0
+            }
+        };
+        self.items.split_off(mark)
+    }
+}
+
+/// The savepoint a `SAVEPOINT`, `ROLLBACK TO` or `RELEASE SAVEPOINT` names, lower-case (names
+/// compare without case). MariaDB logs it quoted.
+fn savepoint_name(sql: &str) -> String {
+    let sql = sql.trim();
+    match sql.find('`') {
+        Some(i) => sql[i + 1..].trim_end_matches('`').replace("``", "`"),
+        None => sql.split_whitespace().last().unwrap_or_default().to_owned(),
+    }
+    .to_lowercase()
+}
+
 /// A statement's schema signals still to read from the catalog, then what follows them.
 #[derive(Debug)]
 struct Signals {
@@ -139,6 +200,13 @@ pub struct MySqlStream {
     checked: HashMap<TableRef, TableMapEvent<'static>>,
     /// When the last keep-alive went out.
     kept_alive: Option<Instant>,
+    /// The tracked tables whose engine rolls back (InnoDB), read at open: a rollback drops their
+    /// held rows, and halts on another's.
+    transactional: HashSet<TableRef>,
+    /// The open transaction's items, while they are held.
+    held: Option<Held>,
+    /// The most rows-event bytes a transaction holds (`sink.max_transaction_buffer_bytes`).
+    hold_limit: u64,
 }
 
 /// The least time between two keep-alives after transactions without tracked rows.
@@ -676,6 +744,40 @@ impl MySqlConnector {
             .collect())
     }
 
+    /// Which of `tables` roll back a transaction's changes: their engine is InnoDB.
+    async fn transactional(
+        &self,
+        tables: &[TableRef],
+        fold: Fold,
+    ) -> Result<HashSet<TableRef>, SourceError> {
+        let db = self.pipeline.source.db.clone();
+        let rows: Vec<(String, Option<String>)> = self
+            .conn
+            .command("read the tables' engines", move |c| {
+                Box::pin(async move {
+                    c.exec(
+                        "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES
+                          WHERE TABLE_SCHEMA = ?",
+                        (db,),
+                    )
+                    .await
+                })
+            })
+            .await?;
+        Ok(tables
+            .iter()
+            .filter(|t| {
+                rows.iter().any(|(name, engine)| {
+                    fold.eq(name, &t.name)
+                        && engine
+                            .as_deref()
+                            .is_some_and(|e| e.eq_ignore_ascii_case("InnoDB"))
+                })
+            })
+            .cloned()
+            .collect())
+    }
+
     /// Opens the binlog dump after `from` for `tables` as of that position (Data-Flow Phase 2b):
     /// the heartbeat period, the replica id `source.mysql.server_id`, and the position in its
     /// dialect. The server's name comparison and collations are read once, for the stream's
@@ -701,6 +803,8 @@ impl MySqlConnector {
             })?;
             tracked.insert(s.table().clone(), t);
         }
+        let keys: Vec<TableRef> = tracked.keys().cloned().collect();
+        let transactional = catalog.transactional(&keys, fold).await?;
         let at = from.to_tagged();
         let live = self.live();
         let action = live.binlog_purge_action;
@@ -779,6 +883,9 @@ impl MySqlConnector {
             charsets,
             checked: HashMap::new(),
             kept_alive: None,
+            transactional,
+            held: None,
+            hold_limit: self.pipeline.sink.max_transaction_buffer_bytes,
         })
     }
 }
@@ -788,8 +895,13 @@ impl MySqlStream {
         SourceSeq::binlog(self.offsets.file(), end).map_err(permanent)
     }
 
-    /// Where items of the open transaction go: after a statement's pending signals, if any.
+    /// Where items of the open transaction go: held, or after a statement's pending signals, if
+    /// any.
     fn emit(&mut self, item: StreamItem) {
+        if let Some(h) = &mut self.held {
+            h.items.push(item);
+            return;
+        }
         match &mut self.signals {
             Some(s) => s.after.push(item),
             None => self.ready.push_back(item),
@@ -870,6 +982,7 @@ impl MySqlStream {
     /// The open transaction commits with the event ending at `end`: the position advances, and a
     /// transaction with items ends with its `Commit`.
     fn commit(&mut self, end: u64) -> Result<(), SourceError> {
+        self.unhold();
         let txn = self
             .txn
             .take()
@@ -922,6 +1035,42 @@ impl MySqlStream {
         }
     }
 
+    /// The open transaction's held items go out, in order, and it is no longer held.
+    fn unhold(&mut self) {
+        if let Some(h) = self.held.take() {
+            for item in h.items {
+                self.emit(item);
+            }
+        }
+    }
+
+    /// A rollback (`ROLLBACK TO name`, or `ROLLBACK`) at `end`: a held transaction drops the items
+    /// it undid; one not held halts if it gave out tracked rows ([`Self::rolled_back`]).
+    fn roll_back(&mut self, name: Option<&str>, what: &str, end: u64) -> Result<(), SourceError> {
+        let Some(h) = &mut self.held else {
+            return self.rolled_back(what, end);
+        };
+        let undone = h.roll_back(name);
+        let begun = h
+            .items
+            .iter()
+            .any(|i| matches!(i, StreamItem::Begin { .. }));
+        if let Some(table) = undone.iter().find_map(|i| match i {
+            StreamItem::Change(c) if !self.transactional.contains(&c.table) => Some(&c.table),
+            _ => None,
+        }) {
+            return Err(permanent(format!(
+                "{what} at {}:{end} undid rows of {table}, whose engine doesn't roll back: the \
+                 source can't tell which rows stand",
+                self.offsets.file()
+            )));
+        }
+        if let Some(t) = &mut self.txn {
+            t.begun = begun;
+        }
+        Ok(())
+    }
+
     /// A rollback logged inside a transaction with tracked rows halts the pipeline: the binlog
     /// kept rows the rollback undid, so the source can't tell which of them stand.
     fn rolled_back(&self, what: &str, end: u64) -> Result<(), SourceError> {
@@ -1021,6 +1170,9 @@ impl MySqlStream {
                 if g.flags & MariaGtidEvent::PREPARED_XA != 0 {
                     t.xa = true;
                 }
+                if !g.standalone() && !g.transactional() {
+                    self.held = Some(Held::default());
+                }
                 Ok(())
             }
             types::QUERY => self.query(&ev, end),
@@ -1112,7 +1264,7 @@ impl MySqlStream {
             return self.commit(end);
         }
         if whole.eq_ignore_ascii_case("ROLLBACK") {
-            self.rolled_back("a ROLLBACK", end)?;
+            self.roll_back(None, "a ROLLBACK", end)?;
             return self.commit(end);
         }
         match verb(&sql).as_str() {
@@ -1132,10 +1284,26 @@ impl MySqlStream {
                 };
             }
             // MariaDB logs the rows a `ROLLBACK TO` a savepoint undid, then the statement.
-            "ROLLBACK" => return self.rolled_back("a savepoint rollback", end),
-            "SAVEPOINT" | "RELEASE" => return Ok(()),
+            "ROLLBACK" => {
+                let name = savepoint_name(&sql);
+                return self.roll_back(Some(&name), "a savepoint rollback", end);
+            }
+            "SAVEPOINT" => {
+                if let Some(h) = &mut self.held {
+                    h.savepoint(savepoint_name(&sql));
+                }
+                return Ok(());
+            }
+            "RELEASE" => {
+                if let Some(h) = &mut self.held {
+                    h.release(&savepoint_name(&sql));
+                }
+                return Ok(());
+            }
             _ => {}
         }
+        // A statement's signals keep their order with the rows before them.
+        self.unhold();
         let seq = self.seq(end)?;
         self.statement(&sql, &db, seq)?;
         if !self.txn.as_ref().is_some_and(|t| t.with_begin) {
@@ -1243,6 +1411,12 @@ impl MySqlStream {
     }
 
     fn rows(&mut self, ev: &Event, end: u64) -> Result<(), SourceError> {
+        if let Some(h) = &mut self.held {
+            h.bytes += u64::from(ev.header().event_size());
+            if h.bytes > self.hold_limit {
+                self.unhold();
+            }
+        }
         let Some(EventData::RowsEvent(re)) = ev.read_data().map_err(permanent)? else {
             return Err(permanent("an unreadable rows event"));
         };
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same three.

Expected: PASS: 145 tests on each target (`a_rollback_drops_the_rows_it_undid` runs on MariaDB).

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/src/stream.rs \
  crates/data-avalanche-source-mysql/tests/mysql/stream.rs \
  crates/data-avalanche-source-mysql/src/binlog.rs
git commit -m "feat(source-mysql): a MariaDB rollback drops the rows it undid — a transaction that can log them (no FL_TRANSACTIONAL) is held to its end, within sink.max_transaction_buffer_bytes; ROLLBACK and ROLLBACK TO a savepoint take out what they undid"
```

---

### Task 6: MySQL detection edges (`source-mysql`, `config`)

**Files:**
- Modify: `crates/data-avalanche-source-mysql/src/schema.rs`, `crates/data-avalanche-source-mysql/src/server.rs`, `crates/data-avalanche-config/src/field_docs.rs`, `crates/data-avalanche-config/src/registry.rs`, `crates/data-avalanche-config/src/rules/cross.rs`, `crates/data-avalanche-source-mysql/src/connect.rs`, `crates/data-avalanche-source-mysql/src/connector.rs`, `crates/data-avalanche-source-mysql/src/lib.rs`, `docs/src/content/docs/configuration/sources.md`, `docs/src/content/docs/reference/configuration.md`, `docs/src/content/docs/reference/validation-rules.md`
- Test: `crates/data-avalanche-config/tests/rules/cross.rs`, `crates/data-avalanche-source-mysql/tests/mysql/schema.rs`

**Interfaces:**
- Consumes: `check_coverage` (M7a), the schema diff (M7a), the cross-field rules (M1).
- Produces:
  - `Coverage.galera: bool`, read only while `log_*_updates` is off;
  - `pub fn keep_names(cached: &TableSchema, new: &TableSchema) -> Result<TableSchema, HaltReason>`,
    re-exported;
  - V-124 (`cross.rs`, a warning);
  - the `ssl_mode` `FIELD_DOCS` text;
  - the re-check note in `connect.rs`.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-config/tests/rules/cross.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/cross.rs b/crates/data-avalanche-config/tests/rules/cross.rs
index c5a51a1..6f8f8da 100644
--- a/crates/data-avalanche-config/tests/rules/cross.rs
+++ b/crates/data-avalanche-config/tests/rules/cross.rs
@@ -50,6 +50,15 @@ pub fn cases() -> Vec<Case> {
         case("V-030", Silent, "ssl allow on postgres", || {
             pg_delta().set("/pipelines/p/source/ssl_mode", json!("allow"))
         }),
+        case("V-124", Warning, "verify-ca on mysql", || {
+            mysql_delta().set("/pipelines/p/source/ssl_mode", json!("verify-ca"))
+        }),
+        case("V-124", Silent, "verify-ca on postgres", || {
+            pg_delta().set("/pipelines/p/source/ssl_mode", json!("verify-ca"))
+        }),
+        case("V-124", Silent, "verify-full on mysql", || {
+            mysql_delta().set("/pipelines/p/source/ssl_mode", json!("verify-full"))
+        }),
         case("V-073", Error, "renewal 6s of 15s", || {
             pg_delta().set("/globals/cluster/lease_renewal_interval", json!("6s"))
         }),
```

`crates/data-avalanche-source-mysql/src/schema.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/schema.rs b/crates/data-avalanche-source-mysql/src/schema.rs
index 748eaa0..2aa8749 100644
--- a/crates/data-avalanche-source-mysql/src/schema.rs
+++ b/crates/data-avalanche-source-mysql/src/schema.rs
@@ -625,22 +625,23 @@ mod tests {
             diff(&cached, &s, &[]).as_slice(),
             [SchemaChange::ChangeType { column, .. }] if column == "price"
         ));
-        // `CHANGE a A INT`: a change of case alone stays a rename.
+        // `CHANGE a A BIGINT`: a change of case alone is the same column, which keeps its name
+        // (DA-SRC-25); its other changes name it so.
         let now = table(vec![
             col("price", V::Int32, "int", 2),
-            col("A", V::Int32, "int", 3),
+            col("A", V::Int64, "bigint", 3),
         ]);
         let renames = [("a".to_owned(), "A".to_owned())];
         let s = scope(&cached, &cols(&["a", "A"]), &renames, &now).unwrap();
         assert_eq!(names(&s), ["id", "price", "A"]);
-        assert_eq!(
-            diff(&cached, &s, &renames),
-            [SchemaChange::RenameColumn {
-                from: "a".into(),
-                to: "A".into(),
-                proven: true
-            }]
-        );
+        assert!(matches!(
+            diff(&cached, &s, &renames).as_slice(),
+            [SchemaChange::NoTargetEffect(note), SchemaChange::ChangeType { column, .. }]
+                if note.starts_with("column \"a\" is now \"A\" in the source") && column == "a"
+        ));
+        let kept = keep_names(&cached, &s).unwrap();
+        assert_eq!(names(&kept), ["id", "price", "a"]);
+        assert_eq!(kept.columns()[2].value_type, V::Int64);
         // A statement's spelling of a renamed column finds it too.
         let renames = [("PRICE".to_owned(), "cost".to_owned())];
         let now = table(vec![
```

`crates/data-avalanche-source-mysql/src/server.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/server.rs b/crates/data-avalanche-source-mysql/src/server.rs
index 374560d..1f52c07 100644
--- a/crates/data-avalanche-source-mysql/src/server.rs
+++ b/crates/data-avalanche-source-mysql/src/server.rs
@@ -368,6 +368,7 @@ mod tests {
             ignore_db: ignore_db.into(),
             replica_updates: updates,
             replica,
+            galera: false,
         };
         let ok = cover("", "", true, None);
         assert_eq!(check_coverage(my, &ok), Ok(None));
@@ -395,6 +396,20 @@ mod tests {
             check_coverage(my, &cover("", "", true, Some(true))),
             Ok(None)
         );
+        // A Galera node is a replica of every other node, with no replica connection.
+        let galera = |updates| Coverage {
+            replica: Some(false),
+            replica_updates: updates,
+            galera: true,
+            ..Coverage::default()
+        };
+        let e = check_coverage(maria, &galera(false)).unwrap_err();
+        assert!(
+            e.starts_with("this server is a Galera node (wsrep_on = ON)")
+                && e.contains("set log_slave_updates = ON"),
+            "{e}"
+        );
+        assert_eq!(check_coverage(maria, &galera(true)), Ok(None));
         // Unreadable: a warning naming the grant.
         let w = check_coverage(maria, &cover("", "", false, None))
             .unwrap()
```

`crates/data-avalanche-source-mysql/tests/mysql/schema.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/schema.rs b/crates/data-avalanche-source-mysql/tests/mysql/schema.rs
index 4921991..5c3a3a8 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/schema.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/schema.rs
@@ -161,3 +161,25 @@ async fn the_rescan_reports_dropped_and_changed_tables() {
     assert!(report.halted.is_empty());
     cleanup(&n).await;
 }
+
+/// A column renamed by its case alone is, to MySQL, the same column: it keeps its name, with a
+/// note and no evolution, from a statement and from the rescan alike (DA-SRC-25).
+#[tokio::test]
+async fn a_change_of_case_alone_keeps_the_column_s_name() {
+    require_it!();
+    let (n, mut a) = fresh("schema_case", "CREATE TABLE t (id INT PRIMARY KEY, a INT);").await;
+    let c = connector(&pipeline(&n, &["t"], "", ""));
+    let cached = c.resolve_tables().await.unwrap().tables.remove(0);
+    let sql = "ALTER TABLE t CHANGE a A INT";
+    a.query_drop(sql).await.unwrap();
+    let (delta, now) = c.augment(&cached, &ddl(sql)).await.unwrap();
+    assert!(
+        matches!(delta.changes.as_slice(), [SchemaChange::NoTargetEffect(note)] if note.contains("keeps its name")),
+        "{:?}",
+        delta.changes
+    );
+    assert_eq!(now.as_ref(), Some(&cached));
+    let report = c.rescan(std::slice::from_ref(&cached)).await.unwrap();
+    assert!(report.changed.is_empty(), "{report:?}");
+    cleanup(&n).await;
+}
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config` and the source on the three targets

Expected:
- config: FAIL, `rule_cases`: `1 failing case(s): V-124 Warning (verify-ca on mysql): got []`.
- source-mysql: FAIL to compile: `Coverage` has no field `galera` (E0560), cannot find function
  `keep_names` (E0425).

- [ ] **Step 3: Implement, and regenerate the reference**

`crates/data-avalanche-config/src/field_docs.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
index 0a62b76..93e5a1f 100644
--- a/crates/data-avalanche-config/src/field_docs.rs
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -332,7 +332,7 @@ pub const FIELD_DOCS: &[FieldDoc] = &[
     f(
         "pipelines.*.source.ssl_mode",
         Value("require"),
-        "TLS to the source: `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full`. `allow` isn't valid for MySQL.",
+        "TLS to the source: `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full`. `allow` isn't valid for MySQL, and there `verify-ca` checks the host name too, as `verify-full` does (V-124).",
     ),
     f(
         "pipelines.*.source.ssl_cert",
```

`crates/data-avalanche-config/src/registry.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/registry.rs b/crates/data-avalanche-config/src/registry.rs
index 9f0dc61..f044561 100644
--- a/crates/data-avalanche-config/src/registry.rs
+++ b/crates/data-avalanche-config/src/registry.rs
@@ -1036,6 +1036,14 @@ pub const RULES: &[RuleMeta] = &[
         r#"source.postgres.source_change_action"#,
         r#"∈ {error, re_snapshot}."#,
     ),
+    rule(
+        "V-124",
+        Warning,
+        L::Pipeline,
+        Config,
+        r#"source.ssl_mode = verify-ca, when source.type = mysql"#,
+        r#"The host name is checked too, as with verify-full (DA-SRC-22)."#,
+    ),
 ];
 
 /// The registry entry for `id` (`"V-007"`).
@@ -1049,7 +1057,7 @@ mod tests {
 
     #[test]
     fn has_every_spec_rule_once_in_order() {
-        assert_eq!(RULES.len(), 117);
+        assert_eq!(RULES.len(), 118);
         assert!(
             RULES.windows(2).all(|w| w[0].id < w[1].id),
             "sorted, no duplicates"
```

`crates/data-avalanche-config/src/rules/cross.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/cross.rs b/crates/data-avalanche-config/src/rules/cross.rs
index db440ec..92235f6 100644
--- a/crates/data-avalanche-config/src/rules/cross.rs
+++ b/crates/data-avalanche-config/src/rules/cross.rs
@@ -188,6 +188,15 @@ fn pipeline(name: &str, p: &RawPipeline, cloud: Option<Cloud>, ctx: &mut Ctx) {
             "ssl_mode allow is not valid for MySQL",
         );
     }
+    // V-124: the pinned client can't skip the host name check alone (DA-SRC-22).
+    if kind == Some(SourceType::Mysql) && ctx.peek(&src.ssl_mode) == Some(SslMode::VerifyCa) {
+        ctx.warning(
+            "V-124",
+            &s_at.key("ssl_mode"),
+            src.ssl_mode.as_ref().and_then(|l| l.loc()),
+            "ssl_mode verify-ca on MySQL also checks the server's host name, as verify-full does: the connection fails unless the certificate names the host",
+        );
+    }
     // V-103
     if let Some(pg) = &src.postgres
         && ctx.peek(&pg.slot_management) == Some(SlotManagement::RequireExisting)
```

`crates/data-avalanche-source-mysql/src/connect.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connect.rs b/crates/data-avalanche-source-mysql/src/connect.rs
index 164a092..f073270 100644
--- a/crates/data-avalanche-source-mysql/src/connect.rs
+++ b/crates/data-avalanche-source-mysql/src/connect.rs
@@ -19,8 +19,17 @@ use crate::error::{mysql, timed_out};
 pub const SESSION_SETUP: [&str; 2] = ["SET time_zone = '+00:00'", "SET NAMES utf8mb4"];
 
 /// The `ssl_mode` → `mysql_async` TLS options (`allow` is refused for MySQL by V-030 and connects
-/// like `prefer`). `require` and `prefer` encrypt without verifying, `verify-ca` checks the chain
-/// and `verify-full` the chain and the host name. Roots are `ssl_ca`, else the Mozilla roots.
+/// like `prefer`). `require` and `prefer` encrypt without verifying, `verify-ca` is meant to check
+/// the chain alone and `verify-full` the chain and the host name. Roots are `ssl_ca`, else the
+/// Mozilla roots.
+///
+/// `verify-ca` checks the host name too, failing closed (DA-SRC-22, warned by V-124): with rustls,
+/// `mysql_async` 0.37.1 skips the name check by matching `NotValidForName` in the verifier
+/// error's text (`rustls_io.rs`), and rustls 0.23 prints that error as "certificate not valid for
+/// name", so the match never hits; the crate takes no verifier of ours. Re-check this on every
+/// `mysql_async` upgrade: once its skip works, `verify-ca` connects to a certificate that doesn't
+/// name the host (the `tls` IT `verify_ca_by_an_unnamed_address_is_refused_like_verify_full`
+/// fails then), and V-124 and this note go.
 pub(crate) fn ssl_opts(source: &Source) -> Result<Option<SslOpts>, String> {
     if source.ssl_mode == SslMode::Disable {
         return Ok(None);
```

`crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
index f035f36..0544e55 100644
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -358,6 +358,19 @@ impl MySqlConnector {
             })
             .await?;
         let replica_updates = replica_updates == Some(1);
+        // MySQL has no such variable; MariaDB has it off unless the node is in a Galera cluster.
+        let galera = !replica_updates
+            && self
+                .conn
+                .command("read wsrep_on", |c| {
+                    Box::pin(async move {
+                        let row: Option<(String, String)> = c
+                            .query_first("SHOW GLOBAL VARIABLES LIKE 'wsrep_on'")
+                            .await?;
+                        Ok(row.is_some_and(|(_, v)| v.eq_ignore_ascii_case("ON")))
+                    })
+                })
+                .await?;
         let replica = match replica_updates {
             true => Some(false),
             false => {
@@ -382,6 +395,7 @@ impl MySqlConnector {
             ignore_db,
             replica_updates,
             replica,
+            galera,
         };
         check_coverage(flavour, &c).map_err(SourceError::Permanent)
     }
```

`crates/data-avalanche-source-mysql/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/lib.rs b/crates/data-avalanche-source-mysql/src/lib.rs
index eaaf954..1eaf06a 100644
--- a/crates/data-avalanche-source-mysql/src/lib.rs
+++ b/crates/data-avalanche-source-mysql/src/lib.rs
@@ -26,7 +26,7 @@ pub use connect::SESSION_SETUP;
 pub use connector::MySqlConnector;
 pub use ddl::{Altered, Ddl, Renamed, Scope, classify, renamed, shadow_of};
 pub use position::{BinaryLog, Mode, Retention, bytes_after, purged};
-pub use schema::{diff, narrowed, scope, scope_of};
+pub use schema::{diff, keep_names, narrowed, scope, scope_of};
 pub use server::{Flavour, Fold, Settings, grants_replication, version_of};
 pub use start::{at_boundary, dialect_change, never_held, purge_action, same_server};
 pub use stream::MySqlStream;
```

`crates/data-avalanche-source-mysql/src/schema.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/schema.rs b/crates/data-avalanche-source-mysql/src/schema.rs
index 2aa8749..8760569 100644
--- a/crates/data-avalanche-source-mysql/src/schema.rs
+++ b/crates/data-avalanche-source-mysql/src/schema.rs
@@ -5,8 +5,9 @@
 //! classifier couldn't read the statement, the whole catalog table is the change, and a rename
 //! reads as a drop and an add (§6, degraded with a warning by the evolver).
 //!
-//! Column names compare without case, as MySQL's do; a result keeps the catalog's spelling, and
-//! a change of case alone (`CHANGE a A`) is a rename.
+//! Column names compare without case, as MySQL's do; a result keeps the catalog's spelling,
+//! except a column whose name changed its case alone (`CHANGE a A`): to MySQL that is the same
+//! column, so it keeps the name it has in the sink, with a note and no evolution (DA-SRC-25).
 //!
 //! The catalog may be ahead of the stream (§0): a scoped change takes only its columns from it.
 //! The stream checks every table map against the schema, so a change taken early (a later DDL's
@@ -190,7 +191,8 @@ fn members_change(column: &str, from: &CatalogColumn, to: &CatalogColumn) -> Opt
 }
 
 /// `cached` → `new` as changes, columns matched by name (without case) and by the proven
-/// `renames`. A column whose name changed only its case is renamed.
+/// `renames`. A column whose name changed only its case is noted, not renamed: its changes name
+/// it as `cached` does.
 pub fn diff(
     cached: &TableSchema,
     new: &TableSchema,
@@ -215,8 +217,17 @@ pub fn diff(
             changes.push(SchemaChange::DropColumn(c.name.clone()));
             continue;
         };
-        let to = n.name.as_str();
-        if to != c.name {
+        let to = match same(&n.name, &c.name) {
+            true => c.name.as_str(),
+            false => n.name.as_str(),
+        };
+        if n.name != c.name && to == c.name {
+            changes.push(SchemaChange::NoTargetEffect(format!(
+                "column {:?} is now {:?} in the source: MySQL names compare without case, so it \
+                 keeps its name",
+                c.name, n.name
+            )));
+        } else if to != c.name {
             changes.push(SchemaChange::RenameColumn {
                 from: c.name.clone(),
                 to: to.to_owned(),
@@ -268,6 +279,25 @@ pub fn diff(
     changes
 }
 
+/// `new` with each column whose name differs from `cached`'s by case alone named as `cached`
+/// names it, in the identifier too ([`diff`] notes the change).
+pub fn keep_names(cached: &TableSchema, new: &TableSchema) -> Result<TableSchema, HaltReason> {
+    let kept = |name: &str| match column(cached, name) {
+        Some(c) => c.name.clone(),
+        None => name.to_owned(),
+    };
+    let columns = new
+        .columns()
+        .iter()
+        .map(|c| ColumnSchema {
+            name: kept(&c.name),
+            ..c.clone()
+        })
+        .collect();
+    let identifier = new.identifier().iter().map(|k| kept(k)).collect();
+    TableSchema::new(new.table().clone(), columns, identifier)
+}
+
 impl MySqlConnector {
     /// The tables' schemas as the catalog has them now, by name; a table not there is left out.
     async fn current(
@@ -333,6 +363,7 @@ impl MySqlConnector {
         };
         let new = scope(cached, &columns, &renames, &now).map_err(halt)?;
         let changes = diff(cached, &new, &renames);
+        let new = keep_names(cached, &new).map_err(halt)?;
         Ok((SchemaDelta { table, changes }, Some(new)))
     }
 
@@ -348,8 +379,11 @@ impl MySqlConnector {
             match now.remove(&t.name) {
                 None => report.dropped.push(t.clone()),
                 Some(Err(reason)) => report.halted.push((t.clone(), reason)),
-                Some(Ok(s)) if s != *k => report.changed.push(s),
-                Some(Ok(_)) => {}
+                Some(Ok(s)) => match keep_names(k, &s) {
+                    Ok(s) if s != *k => report.changed.push(s),
+                    Ok(_) => {}
+                    Err(reason) => report.halted.push((t.clone(), reason)),
+                },
             }
         }
         Ok(report)
```

`crates/data-avalanche-source-mysql/src/server.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/server.rs b/crates/data-avalanche-source-mysql/src/server.rs
index 1f52c07..3499dcc 100644
--- a/crates/data-avalanche-source-mysql/src/server.rs
+++ b/crates/data-avalanche-source-mysql/src/server.rs
@@ -201,6 +201,9 @@ pub(crate) struct Coverage {
     pub(crate) replica_updates: bool,
     /// Whether the server replicates from another; `None` when the user can't read it.
     pub(crate) replica: Option<bool>,
+    /// A Galera node (`wsrep_on`): it applies the other nodes' writesets without a replica
+    /// connection, so `replica` doesn't show it (DA-SRC-24).
+    pub(crate) galera: bool,
 }
 
 /// Whether the source database's changes reach the binlog (config §11.2): `Err` refuses the
@@ -223,6 +226,13 @@ pub(crate) fn check_coverage(flavour: Flavour, c: &Coverage) -> Result<Option<St
         }
     }
     let setting = flavour.replica_updates();
+    if c.galera && !c.replica_updates {
+        return Err(format!(
+            "this server is a Galera node (wsrep_on = ON) and doesn't log the writesets it \
+             applies from the other nodes ({setting} = OFF): the binlog lacks their changes; set \
+             {setting} = ON"
+        ));
+    }
     match (c.replica_updates, c.replica) {
         (true, _) | (false, Some(false)) => Ok(None),
         (false, Some(true)) => Err(format!(
```

`docs/src/content/docs/configuration/sources.md`:

```diff
diff --git a/docs/src/content/docs/configuration/sources.md b/docs/src/content/docs/configuration/sources.md
index a2b8aac..e92c221 100644
--- a/docs/src/content/docs/configuration/sources.md
+++ b/docs/src/content/docs/configuration/sources.md
@@ -8,7 +8,8 @@ sidebar:
 A pipeline reads one database: a PostgreSQL database, or one MySQL or MariaDB database (schema).
 `source.type` picks the engine; the common keys (`host`, `port`, `db`, `user`, `password` or
 `credentials_file`, `ssl_mode`, `timeouts`) are the same for both. `ssl_mode` defaults to
-`require`.
+`require`. On MySQL and MariaDB, `verify-ca` checks the server's host name as well, as
+`verify-full` does (a warning, V-124): use a certificate that names the host you connect to.
 
 The server settings and privileges below are checked when the pipeline starts. A missing one stops
 that pipeline (or that table) with a message naming it; the other pipelines run.
```


`docs/src/content/docs/reference/configuration.md`:

```diff
diff --git a/docs/src/content/docs/reference/configuration.md b/docs/src/content/docs/reference/configuration.md
index 5b1ecc8..e01a5fb 100644
--- a/docs/src/content/docs/reference/configuration.md
+++ b/docs/src/content/docs/reference/configuration.md
@@ -169,7 +169,7 @@ Rules for the whole block: [V-001](/data-avalanche/reference/validation-rules/#v
 | `user` | string | — | pipeline restart |  | The replication user, or give `credentials_file`. |
 | `password` | string | — | pipeline restart |  | The user's password; write it as `${VAR}`. Read again at every reconnect. |
 | `credentials_file` | string | — | pipeline restart |  | A JSON file `{"user": …, "password": …}` in place of `user` and `password`; replace its contents and reload to rotate them. |
-| `ssl_mode` | `disable` \| `allow` \| `prefer` \| `require` \| `verify-ca` \| `verify-full` | `require` | pipeline restart | [V-020](/data-avalanche/reference/validation-rules/#v-020) [V-030](/data-avalanche/reference/validation-rules/#v-030) | TLS to the source: `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full`. `allow` isn't valid for MySQL. |
+| `ssl_mode` | `disable` \| `allow` \| `prefer` \| `require` \| `verify-ca` \| `verify-full` | `require` | pipeline restart | [V-020](/data-avalanche/reference/validation-rules/#v-020) [V-030](/data-avalanche/reference/validation-rules/#v-030) [V-124](/data-avalanche/reference/validation-rules/#v-124) | TLS to the source: `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full`. `allow` isn't valid for MySQL, and there `verify-ca` checks the host name too, as `verify-full` does (V-124). |
 | `ssl_cert` | string | — | pipeline restart |  | A client certificate for the source: a path, inline PEM, or `${VAR}`. |
 | `ssl_key` | string | — | pipeline restart |  | The client certificate's key: a path, inline PEM, or `${VAR}`. |
 | `ssl_ca` | string | — | pipeline restart |  | The CA that signed the source's certificate, for `verify-ca` and `verify-full`. |
```

`docs/src/content/docs/reference/validation-rules.md`:

```diff
diff --git a/docs/src/content/docs/reference/validation-rules.md b/docs/src/content/docs/reference/validation-rules.md
index a173a3a..d4fdafd 100644
--- a/docs/src/content/docs/reference/validation-rules.md
+++ b/docs/src/content/docs/reference/validation-rules.md
@@ -129,3 +129,4 @@ A config with any **error** is rejected whole: at startup the node refuses to st
 | <span id="v-121"></span>V-121 | error | `source.mysql.heartbeat` | Between 1s and 4294967s (the server's heartbeat period range). |
 | <span id="v-122"></span>V-122 | error | `globals.cluster.memberlist timers` | gossip_interval, probe_interval, probe_timeout and push_pull_interval above zero; gossip_nodes and retransmit_factor at least 1; probe_timeout below probe_interval. |
 | <span id="v-123"></span>V-123 | error | `source.postgres.source_change_action` | ∈ {error, re_snapshot}. |
+| <span id="v-124"></span>V-124 | warning | `source.ssl_mode = verify-ca, when source.type = mysql` | The host name is checked too, as with verify-full (DA-SRC-22). |
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same, then `cargo test -p xtask`.

Expected: PASS: config 156, 146 tests on each target, xtask 4.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/tests/rules/cross.rs \
  crates/data-avalanche-source-mysql/src/schema.rs \
  crates/data-avalanche-source-mysql/src/server.rs \
  crates/data-avalanche-source-mysql/tests/mysql/schema.rs \
  crates/data-avalanche-config/src/field_docs.rs \
  crates/data-avalanche-config/src/registry.rs \
  crates/data-avalanche-config/src/rules/cross.rs \
  crates/data-avalanche-source-mysql/src/connect.rs \
  crates/data-avalanche-source-mysql/src/connector.rs \
  crates/data-avalanche-source-mysql/src/lib.rs \
  docs/src/content/docs/configuration/sources.md \
  docs/src/content/docs/reference/configuration.md \
  docs/src/content/docs/reference/validation-rules.md
git commit -m "feat(source-mysql): a Galera node without log_slave_updates is refused; a column renamed by case alone keeps its name; verify-ca on MySQL warns (V-124) that it checks the name too, the mysql_async re-check named"
```

---

### Task 7: Keys a cursor can't resume, and a table's own read errors (`source-pg`, `source-mysql`)

**Files:**
- Modify: `crates/data-avalanche-source-mysql/src/snapshot.rs`, `crates/data-avalanche-source-pg/src/catalog.rs`, `crates/data-avalanche-source-pg/src/snapshot.rs`

**Interfaces:**
- Consumes: `table_schema` (M5), `dropped` / the chunk reader's error mapping (M6a, M7a).
- Produces:
  - `table_schema` refuses a `bit`, `varbit` or hex-bytes identifier column with
    `IDENTIFIER_NOT_USABLE`;
  - PostgreSQL's `fn table_scoped(code: &str) -> bool` and MySQL's
    `fn table_scoped(code: u16) -> bool`, each mapping a read error of the table alone to
    `HaltReason::NotReplicable { detail: "its snapshot read fails: …" }` for the table.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-source-mysql/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/snapshot.rs b/crates/data-avalanche-source-mysql/src/snapshot.rs
index 7e52345..3806cc6 100644
--- a/crates/data-avalanche-source-mysql/src/snapshot.rs
+++ b/crates/data-avalanche-source-mysql/src/snapshot.rs
@@ -470,6 +470,18 @@ mod tests {
     use super::*;
     use crate::value::Charset;
 
+    /// The errors of this table's read alone halt the table; a table or column gone keeps its own
+    /// path; the rest end the pipeline (DA-SRC-19).
+    #[test]
+    fn a_read_only_this_table_fails_halts_the_table() {
+        for code in [1142, 1143, 1267, 1271, 1253, 1273, 1292, 1366] {
+            assert!(table_scoped(code), "{code}");
+        }
+        for code in [NO_TABLE, NO_COLUMN, 1064, 1205, 2013] {
+            assert!(!table_scoped(code), "{code}");
+        }
+    }
+
     fn column(data_type: &str, decode: Decode) -> ReadColumn {
         ReadColumn {
             catalog: CatalogColumn {
```

`crates/data-avalanche-source-pg/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/catalog.rs b/crates/data-avalanche-source-pg/src/catalog.rs
index 93cf452..d9cfea1 100644
--- a/crates/data-avalanche-source-pg/src/catalog.rs
+++ b/crates/data-avalanche-source-pg/src/catalog.rs
@@ -590,6 +590,30 @@ mod tests {
         assert!(notes.c_ordered_keys.is_empty());
     }
 
+    #[test]
+    fn a_bit_key_is_refused_unless_read_as_text() {
+        let mut d = orders(b'd');
+        d.columns[0] = attr(1, "id", 1560, 8, true, "bit(8)");
+        let e = schema_of(&d, "").unwrap_err();
+        assert!(
+            matches!(&e, HaltReason::IdentifierNotUsable { column, reason }
+                if column == "id" && reason.starts_with("a bit(8) key can't resume a snapshot")),
+            "{e:?}"
+        );
+        d.columns[0] = attr(1, "id", 1562, -1, true, "bit varying");
+        assert_eq!(
+            schema_of(&d, "").unwrap_err().code(),
+            "IDENTIFIER_NOT_USABLE"
+        );
+        let over = "      column_type_overrides: { public.orders.id: string }";
+        assert!(schema_of(&d, over).is_ok(), "read as text, it reads back");
+        let mut other = orders(b'f');
+        other
+            .columns
+            .push(attr(6, "flags", 1560, 8, false, "bit(8)"));
+        assert!(schema_of(&other, "").is_ok(), "not a key");
+    }
+
     #[test]
     fn an_unlogged_table_doesnt_plan() {
         let mut d = orders(b'd');
```

`crates/data-avalanche-source-pg/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/snapshot.rs b/crates/data-avalanche-source-pg/src/snapshot.rs
index f20fddd..8dcfad2 100644
--- a/crates/data-avalanche-source-pg/src/snapshot.rs
+++ b/crates/data-avalanche-source-pg/src/snapshot.rs
@@ -644,13 +644,44 @@ mod tests {
                 ..
             }
         ));
-        let other = SourceError::Permanent("permission denied (SQLSTATE 42501)".into());
+        // An error of our own SQL, or the server's, ends the pipeline.
+        for code in ["42601", "XX000", "28000"] {
+            let other = SourceError::Permanent(format!("boom (SQLSTATE {code})"));
+            assert!(
+                matches!(dropped(&schema(), other), SourceError::Permanent(_)),
+                "{code}"
+            );
+        }
         assert!(matches!(
-            dropped(&schema(), other),
-            SourceError::Permanent(_)
+            dropped(&schema(), SourceError::Transient("reset".into())),
+            SourceError::Transient(_)
         ));
     }
 
+    /// An error of this table's read alone halts the table, not the pipeline: a restart would read
+    /// it the same way (DA-SRC-19).
+    #[test]
+    fn a_read_only_this_table_fails_halts_the_table() {
+        for code in [
+            "42883", "42804", "42P22", "42704", "42501", "22P02", "22003", "22021",
+        ] {
+            let e = SourceError::Permanent(format!("it failed (SQLSTATE {code})"));
+            match dropped(&schema(), e) {
+                SourceError::TableHalt {
+                    table,
+                    reason: HaltReason::NotReplicable { reason },
+                } => {
+                    assert_eq!(table, TableRef::new("s", "t"));
+                    assert_eq!(
+                        reason,
+                        format!("its snapshot read fails: it failed (SQLSTATE {code})")
+                    );
+                }
+                other => panic!("{code}: {other:?}"),
+            }
+        }
+    }
+
     fn rc(name: &str, type_text: &str, collation: Option<KeyCollation>) -> ReadColumn {
         ReadColumn {
             name: name.into(),
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg -- --test-threads=1 --skip tls::` and the MySQL source on the three targets

Expected:
- source-pg: FAIL, 2 of 87 unit tests: `a_bit_key_is_refused_unless_read_as_text` (`unwrap_err()` on
  an `Ok` value: a `bit(8)` key accepted) and `a_read_only_this_table_fails_halts_the_table`
  (`42883: Permanent("it failed (SQLSTATE 42883)")`).
- source-mysql: FAIL to compile: cannot find function `table_scoped` (E0425).

- [ ] **Step 3: Implement**

`crates/data-avalanche-source-mysql/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/snapshot.rs b/crates/data-avalanche-source-mysql/src/snapshot.rs
index 3806cc6..b087de7 100644
--- a/crates/data-avalanche-source-mysql/src/snapshot.rs
+++ b/crates/data-avalanche-source-mysql/src/snapshot.rs
@@ -355,6 +355,15 @@ fn null_key(schema: &TableSchema, row: &Row) -> String {
 const NO_TABLE: u16 = 1146;
 const NO_COLUMN: u16 = 1054;
 
+/// Server errors of this table's read alone, which halt the table, not the pipeline: a restart
+/// reads it the same way (DA-SRC-19, PostgreSQL's beside it): the user lost `SELECT` on the
+/// table or a column (1142, 1143), the key no longer compares with the cursor (1267, 1271
+/// collations mixed; 1253, 1273 the cursor's collation unknown or another character set's), or
+/// a cursor value its type refuses (1292, 1366). Any other permanent error ends the pipeline.
+pub(crate) fn table_scoped(code: u16) -> bool {
+    matches!(code, 1142 | 1143 | 1267 | 1271 | 1253 | 1273 | 1292 | 1366)
+}
+
 impl MySqlConnector {
     /// One chunk: `SELECT … WHERE <after the cursor> ORDER BY key LIMIT n LOCK IN SHARE MODE` in
     /// a READ COMMITTED transaction that commits at once (DA-SNAP-10).
@@ -421,7 +430,7 @@ impl MySqlConnector {
                             Ok(Ok(rows))
                         }
                         Err(mysql_async::Error::Server(e))
-                            if matches!(e.code, NO_TABLE | NO_COLUMN) =>
+                            if matches!(e.code, NO_TABLE | NO_COLUMN) || table_scoped(e.code) =>
                         {
                             tx.rollback().await?;
                             Ok(Err(e))
@@ -434,7 +443,15 @@ impl MySqlConnector {
         let rows = match read {
             Ok(rows) => rows,
             Err(e) if e.code == NO_TABLE => return Err(halt(table, HaltReason::TableDropped)),
-            Err(e) => return Err(changed(table, e.message)),
+            Err(e) if e.code == NO_COLUMN => return Err(changed(table, e.message)),
+            Err(e) => {
+                return Err(halt(
+                    table,
+                    HaltReason::NotReplicable {
+                        reason: format!("its snapshot read fails: {} ({})", e.message, e.code),
+                    },
+                ));
+            }
         };
         let rows = rows
             .into_iter()
```

`crates/data-avalanche-source-pg/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/catalog.rs b/crates/data-avalanche-source-pg/src/catalog.rs
index d9cfea1..0af854c 100644
--- a/crates/data-avalanche-source-pg/src/catalog.rs
+++ b/crates/data-avalanche-source-pg/src/catalog.rs
@@ -8,6 +8,7 @@ use data_avalanche_core::{ColumnSchema, HaltReason, TableRef, TableSchema};
 use tokio_postgres::Client;
 
 use crate::types::{TypeCatalog, TypeInfo, missing, plan_column};
+use crate::value::Decode;
 
 pub use data_avalanche_config::glob_match;
 
@@ -180,8 +181,10 @@ fn identity_columns(d: &Described) -> Vec<&str> {
 /// A table's schema, checked in this order (config §11.2): every column plans
 /// ([`plan_column`]: overrides, numeric caps); the identifier (the `tables.primary_key` entry,
 /// else the primary key) passes [`TableSchema::new`] (reserved names, a non-empty identifier of
-/// usable columns); the identifier is inside the REPLICA IDENTITY; partition columns exist and
-/// temporal transforms name temporal columns.
+/// usable columns); no identifier column is a `bit`, `varbit` or PostGIS one, whose value doesn't
+/// read back as its type's text for a snapshot's cursor (DA-SRC-14; a `string` override reads it
+/// as text, which does); the identifier is inside the REPLICA IDENTITY; partition columns exist
+/// and temporal transforms name temporal columns.
 pub fn table_schema(
     d: &Described,
     tables: &Tables,
@@ -195,6 +198,7 @@ pub fn table_schema(
     let key = table_key(&d.table);
     let mut notes = Notes::default();
     let mut columns = Vec::with_capacity(d.columns.len());
+    let mut uncursored = vec![];
     for a in &d.columns {
         let over = tables
             .column_type_overrides
@@ -213,6 +217,9 @@ pub fn table_schema(
         if p.unconstrained_numeric {
             notes.unconstrained.push(a.name.clone());
         }
+        if matches!(p.decode, Decode::Bit | Decode::VarBit | Decode::HexBytes) {
+            uncursored.push((a.name.as_str(), a.type_text.as_str()));
+        }
         columns.push(ColumnSchema {
             name: a.name.clone(),
             value_type: p.value_type,
@@ -227,6 +234,19 @@ pub fn table_schema(
         .get(&key)
         .cloned()
         .unwrap_or_else(|| d.primary_key.clone());
+    if let Some((column, kind)) = uncursored
+        .iter()
+        .find(|(c, _)| identifier.iter().any(|k| k == c))
+    {
+        return Err(HaltReason::IdentifierNotUsable {
+            column: (*column).to_owned(),
+            reason: format!(
+                "a {kind} key can't resume a snapshot: its value doesn't read back as {kind}; key \
+                 the table by other columns (tables.primary_key), or read the column as text \
+                 (column_type_overrides: string)"
+            ),
+        });
+    }
     let schema =
         TableSchema::new(d.table.clone(), columns, identifier)?.with_source_id(d.oid.into());
     let identity = identity_columns(d);
```

`crates/data-avalanche-source-pg/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/snapshot.rs b/crates/data-avalanche-source-pg/src/snapshot.rs
index 8dcfad2..f487816 100644
--- a/crates/data-avalanche-source-pg/src/snapshot.rs
+++ b/crates/data-avalanche-source-pg/src/snapshot.rs
@@ -395,20 +395,37 @@ fn changed(schema: &TableSchema, detail: String) -> SourceError {
     ))
 }
 
-/// A table, or its schema, gone from the catalog (SQLSTATE 42P01, 3F000) halts that table, not
-/// the pipeline (§2.1). A column gone (42703) is `read_plan`'s own finding come late: dropped
-/// after the plan's catalog read and before the query took its lock, it is transient the same
-/// way.
+/// A table read's error, by SQLSTATE (§2.1, DA-SRC-19). The table, or its schema, gone from the
+/// catalog (42P01, 3F000) halts that table, not the pipeline. A column gone (42703) is
+/// `read_plan`'s own finding come late: dropped after the plan's catalog read and before the
+/// query took its lock, it is transient the same way. An error of this table's read alone halts
+/// the table too (`TABLE_NOT_REPLICABLE`), since a restart reads it the same way: its key no
+/// longer compares with the cursor (42883 no such operator, 42804 a datatype mismatch, 42P22 an
+/// indeterminate collation, 42704 the cursor's collation gone), a value or a cursor literal its
+/// type refuses (class 22), or the role lost `SELECT` on it (42501). Any other permanent error
+/// ends the pipeline.
 fn dropped(schema: &TableSchema, e: SourceError) -> SourceError {
-    match &e {
-        SourceError::Permanent(m) if matches!(sqlstate(m), Some("42P01" | "3F000")) => {
-            halt_dropped(schema)
-        }
-        SourceError::Permanent(m) if sqlstate(m) == Some("42703") => changed(schema, m.clone()),
+    let SourceError::Permanent(m) = &e else {
+        return e;
+    };
+    match sqlstate(m) {
+        Some("42P01" | "3F000") => halt_dropped(schema),
+        Some("42703") => changed(schema, m.clone()),
+        Some(code) if table_scoped(code) => SourceError::TableHalt {
+            table: schema.table().clone(),
+            reason: HaltReason::NotReplicable {
+                reason: format!("its snapshot read fails: {m}"),
+            },
+        },
         _ => e,
     }
 }
 
+/// The SQLSTATEs [`dropped`] halts the table for.
+fn table_scoped(code: &str) -> bool {
+    code.starts_with("22") || matches!(code, "42883" | "42804" | "42P22" | "42704" | "42501")
+}
+
 /// The identifier's read columns, in identifier order.
 fn key_columns<'a>(schema: &TableSchema, columns: &'a [ReadColumn]) -> Vec<&'a ReadColumn> {
     schema
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same.

Expected: PASS: source-pg 145, 147 tests on each MySQL target.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-mysql/src/snapshot.rs \
  crates/data-avalanche-source-pg/src/catalog.rs \
  crates/data-avalanche-source-pg/src/snapshot.rs
git commit -m "feat(source-pg): a bit, varbit or PostGIS key is refused unless read as text; a read error of one table (a lost SELECT, a type or collation the cursor can't compare) halts that table, on PostgreSQL and MySQL"
```

---

### Task 8: Snapshot edges (`pipeline`, `source-pg`, `source-mysql`)

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`, `crates/data-avalanche-source-mysql/src/snapshot.rs`, `crates/data-avalanche-source-pg/src/catalog.rs`, `crates/data-avalanche-source-pg/src/snapshot.rs`, `crates/data-avalanche-pipeline/src/schema_change.rs`, `crates/data-avalanche-source-pg/src/connector.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs`, `crates/data-avalanche-source-pg/tests/pg/snapshot.rs`

**Interfaces:**
- Consumes: the worker's start baseline (M6a), `hop2_schema` (M3), the PostgreSQL read plan and
  key literals (M6a), MySQL's read plan (M7a), `members` (M7a).
- Produces:
  - `pub(crate) fn missing_columns(sink: &Schema, source: &TableSchema) -> Result<Vec<SchemaChange>, HaltReason>`;
  - the worker's arm for a table the sink holds with no schema cached;
  - `KeyCollation::Own { name: String, qualified: String }`;
  - `Described.index_collations: Vec<(String, String)>` and
    `Notes.index_ordered_otherwise: Vec<(String, String)>`;
  - MySQL's `fn moved(schema: &CatalogColumn, catalog: &CatalogColumn) -> bool`;
  - both readers refuse `limit == 0`.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index e275197..c871ade 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -917,13 +917,15 @@ mod tests {
     use data_avalanche_core::position::Lsn;
     use data_avalanche_core::{
         ColumnSchema, ColumnValue, ColumnValueType as V, Row, SchemaChange, SchemaDelta,
-        ServerIdentity, SourcePosition,
+        ServerIdentity, SourcePosition, upsert_schema,
     };
     use data_avalanche_state::{PositionDoc, SlotDoc};
     use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
 
     use super::*;
-    use crate::test_support::{Countdown, CrashAt, World, Yaml, fast, row, t, wait_until};
+    use crate::test_support::{
+        Countdown, CrashAt, World, Yaml, docs, fast, row, schema, t, wait_until,
+    };
     use crate::{Backoff, HookPoint};
     use std::sync::atomic::AtomicUsize;
     use std::time::Duration;
@@ -1616,6 +1618,58 @@ mod tests {
         );
     }
 
+    /// The pipeline's state is gone, and the sink still holds its table: the start adds the
+    /// columns the source gained, before the copy writes them (DA-SNAP-13).
+    #[tokio::test]
+    async fn a_wiped_state_baselines_against_the_sink_table() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
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
+        let wiped = World {
+            source: w.source.clone(),
+            sink: w.sink.clone(),
+            docs: docs().0,
+        };
+        let (end, st) = run(&wiped, &mode("mode: initial"), async |status| {
+            streamed(&status, &wiped).await;
+        })
+        .await;
+        end.unwrap();
+        assert!(st.halted.is_empty(), "{:?}", st.halted);
+        let state = w.sink.table_state(&t("a")).await.unwrap();
+        assert!(state.schema.unwrap().field_with_name("note").is_ok());
+    }
+
+    /// A sink table without the source's key column is keyed otherwise: it halts.
+    #[test]
+    fn a_sink_table_without_the_key_column_halts() {
+        let sink = upsert_schema(&schema("a"));
+        let rekeyed = TableSchema::new(
+            t("a"),
+            vec![
+                ColumnSchema::new("uid", V::Int64, "bigint").not_null(),
+                ColumnSchema::new("doc", V::Text, "text"),
+            ],
+            vec!["uid".into()],
+        )
+        .unwrap();
+        assert!(matches!(
+            missing_columns(&sink, &rekeyed),
+            Err(HaltReason::SchemaMismatch { column, .. }) if column == "uid"
+        ));
+        assert!(missing_columns(&sink, &schema("a")).unwrap().is_empty());
+    }
+
     #[tokio::test]
     async fn a_lost_position_baselines_a_drifted_table() {
         let w = World::new(&["a"]);
```

`crates/data-avalanche-source-mysql/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/snapshot.rs b/crates/data-avalanche-source-mysql/src/snapshot.rs
index b087de7..fe1a456 100644
--- a/crates/data-avalanche-source-mysql/src/snapshot.rs
+++ b/crates/data-avalanche-source-mysql/src/snapshot.rs
@@ -487,6 +487,24 @@ mod tests {
     use super::*;
     use crate::value::Charset;
 
+    /// An ENUM or SET key's members moved, not appended, are a change under the snapshot
+    /// (DA-SNAP-14).
+    #[test]
+    fn an_enum_key_s_moved_members_are_seen() {
+        let c = |t: &str| CatalogColumn {
+            data_type: t.split('(').next().unwrap().into(),
+            column_type: t.into(),
+            ..CatalogColumn::default()
+        };
+        let was = c("enum('a','b')");
+        assert!(!moved(&was, &c("enum('a','b')")));
+        assert!(!moved(&was, &c("enum('a','b','c')")), "appended");
+        assert!(moved(&was, &c("enum('b','a')")), "reordered");
+        assert!(moved(&was, &c("enum('a','c','b')")), "inserted");
+        assert!(moved(&c("set('x','y')"), &c("set('y','x')")));
+        assert!(!moved(&c("int"), &c("int")));
+    }
+
     /// The errors of this table's read alone halt the table; a table or column gone keeps its own
     /// path; the rest end the pipeline (DA-SRC-19).
     #[test]
```

`crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs b/crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs
index 5ee9cae..09d159f 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs
@@ -177,6 +177,10 @@ async fn every_key_type_walks_in_the_servers_order() {
             t.table(),
             cursors[0].collation
         );
+        assert!(
+            matches!(c.read_chunk(t, None, 0).await, Err(SourceError::Permanent(m)) if m.contains("0 rows")),
+            "a chunk of no rows isn't the table's end"
+        );
     }
     cleanup(&n).await;
 }
```

`crates/data-avalanche-source-pg/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/catalog.rs b/crates/data-avalanche-source-pg/src/catalog.rs
index 0af854c..25045a4 100644
--- a/crates/data-avalanche-source-pg/src/catalog.rs
+++ b/crates/data-avalanche-source-pg/src/catalog.rs
@@ -657,6 +657,23 @@ mod tests {
         assert!(notes.c_ordered_keys.is_empty(), "only identifier columns");
     }
 
+    #[test]
+    fn a_key_index_in_another_collation_is_noted() {
+        let mut d = orders(b'i');
+        d.index_collations = vec![("code".into(), "C".into())];
+        let over = "      primary_key: { public.orders: [code] }";
+        let (_, notes) = schema_of(&d, over).unwrap();
+        assert_eq!(notes.index_ordered_otherwise, [("code".into(), "C".into())]);
+        assert!(notes.c_ordered_keys.is_empty());
+        let mut other = orders(b'd');
+        other.index_collations = vec![("code".into(), "C".into())];
+        let (_, notes) = schema_of(&other, "").unwrap();
+        assert!(
+            notes.index_ordered_otherwise.is_empty(),
+            "only identifier columns"
+        );
+    }
+
     #[test]
     fn identifiers_and_replica_identity() {
         // The identifier must be inside the replica identity.
```

`crates/data-avalanche-source-pg/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/snapshot.rs b/crates/data-avalanche-source-pg/src/snapshot.rs
index f487816..d14763a 100644
--- a/crates/data-avalanche-source-pg/src/snapshot.rs
+++ b/crates/data-avalanche-source-pg/src/snapshot.rs
@@ -741,7 +741,12 @@ mod tests {
 
     #[test]
     fn keys_compare_in_one_deterministic_collation() {
-        let own = || Some(KeyCollation::Own("en_US.utf8".into()));
+        let own = || {
+            Some(KeyCollation::Own {
+                name: "en_US.utf8".into(),
+                qualified: "\"pg_catalog\".\"en_US.utf8\"".into(),
+            })
+        };
         assert_eq!(
             key_expr(&rc("code", "text", Some(KeyCollation::C))),
             "src.\"code\" COLLATE \"C\""
@@ -757,8 +762,8 @@ mod tests {
         );
         assert_eq!(
             key_value(&rc("code", "character varying(10)", own()), "it's"),
-            "'it''s'::character varying(10)",
-            "the column's own collation wins over the literal's"
+            "'it''s'::character varying(10) COLLATE \"pg_catalog\".\"en_US.utf8\"",
+            "the column's own collation, named with its schema, over a domain's"
         );
         assert_eq!(
             key_value(&rc("id", "numeric(10,2)", None), "1.50"),
```

`crates/data-avalanche-source-pg/tests/pg/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/snapshot.rs b/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
index 438d6a6..ec23a13 100644
--- a/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/snapshot.rs
@@ -250,6 +250,10 @@ async fn chunks_walk_the_table_in_key_order() {
         None,
         "no text key, no collation"
     );
+    assert!(
+        matches!(c.read_chunk(t, None, 0).await, Err(SourceError::Permanent(m)) if m.contains("0 rows")),
+        "a chunk of no rows isn't the table's end"
+    );
     cleanup(&a, &n).await;
 }
 
@@ -276,10 +280,14 @@ async fn text_keys_page_in_one_deterministic_collation() {
          CREATE TABLE \"Codes\" (code text COLLATE \"en_US.utf8\" PRIMARY KEY);
          CREATE TABLE folded (code text COLLATE folding PRIMARY KEY);
          INSERT INTO \"Codes\" VALUES ('b'), ('B'), ('a'), ('Z'), ('é'), ('a b'), ('ab');
-         INSERT INTO folded VALUES ('b'), ('A'), ('é'), ('Z'), ('a b');",
+         INSERT INTO folded VALUES ('b'), ('A'), ('é'), ('Z'), ('a b');
+         CREATE TABLE indexed (id int PRIMARY KEY, code text COLLATE \"en_US.utf8\" NOT NULL);
+         CREATE UNIQUE INDEX indexed_code ON indexed (code COLLATE \"C\");
+         ALTER TABLE indexed REPLICA IDENTITY USING INDEX indexed_code;",
     )
     .await;
-    let c = connector(&pipeline(&n, &["*"], "", ""));
+    let keyed = format!("      primary_key: {{ {}.indexed: [code] }}", n.schema);
+    let c = connector(&pipeline(&n, &["*"], &keyed, ""));
     let r = resolved(&c).await;
     let table = |name: &str| {
         r.tables
@@ -346,6 +354,14 @@ async fn text_keys_page_in_one_deterministic_collation() {
         "{:?}",
         r.warnings
     );
+    // A key whose index sorts it in another collation than its own (DA-SRC-15).
+    assert!(
+        r.warnings
+            .iter()
+            .any(|w| w.contains(".indexed.code: the key's index sorts it in C")),
+        "{:?}",
+        r.warnings
+    );
     cleanup(&a, &n).await;
 }
 
@@ -419,6 +435,28 @@ async fn a_domain_over_char_key_resumes() {
     cleanup(&a, &n).await;
 }
 
+/// A key column of a domain with its own collation, declared with another: the cursor carries
+/// the column's collation, named with its schema, so the comparison has one (DA-SRC-15; 42P22
+/// from the second chunk before).
+#[tokio::test]
+async fn a_domain_key_declared_with_another_collation_resumes() {
+    require_it!();
+    let a = admin().await;
+    let n = fresh(
+        &a,
+        "chunks_domain_collation",
+        "CREATE DOMAIN code AS text COLLATE \"POSIX\";
+         CREATE TABLE t (code code COLLATE \"C\" PRIMARY KEY);
+         INSERT INTO t VALUES ('b'), ('a'), ('B'), ('c');",
+    )
+    .await;
+    let c = connector(&pipeline(&n, &["*"], "", ""));
+    let t = &resolved(&c).await.tables[0];
+    let (rows, _) = read_all(&c, t, 1).await;
+    assert_eq!(texts(&rows), ["B", "a", "b", "c"], "in C order");
+    cleanup(&a, &n).await;
+}
+
 #[tokio::test]
 async fn fetch_rows_rereads_keys_in_batches() {
     require_it!();
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-pipeline`, the PostgreSQL source and the MySQL source on the three targets

Expected: FAIL to compile:
- pipeline: cannot find function `missing_columns` (E0425);
- source-pg: `Notes` has no field `index_ordered_otherwise`, `Described` none `index_collations`
  (E0609), `KeyCollation::Own` none `qualified` (E0559);
- source-mysql: cannot find function `moved` (E0425).

- [ ] **Step 3: Implement**

`crates/data-avalanche-pipeline/src/schema_change.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/schema_change.rs b/crates/data-avalanche-pipeline/src/schema_change.rs
index 8fdd297..769b5c8 100644
--- a/crates/data-avalanche-pipeline/src/schema_change.rs
+++ b/crates/data-avalanche-pipeline/src/schema_change.rs
@@ -28,6 +28,34 @@ pub(crate) fn soft_dropped(sink: &Schema, source: &TableSchema) -> usize {
         .count()
 }
 
+/// A table the sink holds while the pipeline has no schema cached for it (a wiped state,
+/// DA-SNAP-13): the source's columns the sink table lacks, by name, as the `AddColumn`s that
+/// baseline it before the copy. A column it holds as another type still surfaces at the first
+/// commit (`SINK_REJECTED`); one the source lacks stays, soft-dropped. A key column it lacks can't
+/// be added: the table is keyed otherwise, and halts.
+pub(crate) fn missing_columns(
+    sink: &Schema,
+    source: &TableSchema,
+) -> Result<Vec<SchemaChange>, HaltReason> {
+    let mut adds = vec![];
+    for c in source.columns() {
+        if sink.field_with_name(&c.name).is_ok() {
+            continue;
+        }
+        if source.is_identifier(&c.name) {
+            return Err(HaltReason::SchemaMismatch {
+                column: c.name.clone(),
+                detail:
+                    "the sink table, made by a run whose state is gone, lacks this key column; \
+                         drop the sink table for a new copy"
+                        .into(),
+            });
+        }
+        adds.push(SchemaChange::AddColumn(c.clone()));
+    }
+    Ok(adds)
+}
+
 /// A change to apply: the table's new schema, and the changes the sink must evolve.
 #[derive(Debug)]
 pub(crate) struct Planned {
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index c871ade..9d9caa9 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -21,7 +21,7 @@ use tokio::sync::{mpsc, watch};
 
 use crate::decide::{Decision, Facts, Plan, decide};
 use crate::jobs::Jobs;
-use crate::schema_change::{Planned, soft_dropped};
+use crate::schema_change::{Planned, missing_columns, soft_dropped};
 use crate::session::{Core, End, Session};
 use crate::settings::Settings;
 use crate::status::Streak;
@@ -678,6 +678,36 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
                     }
                 }
             }
+            // No schema cached for a table the sink holds (a wiped state): baselined against the
+            // sink table's columns (DA-SNAP-13).
+            None if state.exists && baseline => {
+                let adds = match state.schema.as_deref().map(|s| missing_columns(s, now)) {
+                    Some(Ok(adds)) => adds,
+                    Some(Err(reason)) => {
+                        halts.push((table.clone(), reason));
+                        tracked.insert(table.clone());
+                        continue;
+                    }
+                    None => vec![],
+                };
+                if !adds.is_empty() {
+                    let evolver = core.evolver();
+                    let applied = match evolver.evolution {
+                        true => evolver.evolve(table, adds).await?,
+                        false => Err(HaltReason::EvolutionDisabled),
+                    };
+                    match applied {
+                        Ok(None) => {}
+                        Ok(Some(w)) => warnings.push(w),
+                        Err(reason) => {
+                            halts.push((table.clone(), reason));
+                            tracked.insert(table.clone());
+                            continue;
+                        }
+                    }
+                    evolved = true;
+                }
+            }
             Some(_) if cleared.contains(table) => {}
             Some(c) if matches!(plan, Plan::Resume) && c.columns() != now.columns() => {
                 drifted.insert(table.clone());
```

`crates/data-avalanche-source-mysql/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/snapshot.rs b/crates/data-avalanche-source-mysql/src/snapshot.rs
index fe1a456..5e027e1 100644
--- a/crates/data-avalanche-source-mysql/src/snapshot.rs
+++ b/crates/data-avalanche-source-mysql/src/snapshot.rs
@@ -27,7 +27,7 @@ use mysql_async::{IsolationLevel, TxOpts, Value};
 
 use crate::catalog::table_key;
 use crate::connector::MySqlConnector;
-use crate::types::{CatalogColumn, plan_column};
+use crate::types::{CatalogColumn, members, plan_column};
 use crate::value::{self, Decode, Input};
 
 /// How to read one column of a table.
@@ -116,6 +116,18 @@ async fn read_plan(
                 ),
             ));
         }
+        // An ENUM or SET key sorts by member index, and a chunk resumes at its cursor's label's
+        // index: members moved since the schema would skip or repeat rows (DA-SNAP-14). The
+        // stream halts the table at the change; until it reaches it, the chunk waits.
+        if identifier && moved(&CatalogColumn::from_schema(col), catalog) {
+            return Err(changed(
+                schema,
+                format!(
+                    "key column {} is now {}, the schema has {}",
+                    col.name, catalog.column_type, col.source_type
+                ),
+            ));
+        }
         out.push(ReadColumn {
             catalog: catalog.clone(),
             decode: p.decode,
@@ -125,6 +137,13 @@ async fn read_plan(
     Ok(out)
 }
 
+/// An ENUM or SET column whose members the catalog no longer lists in the schema's order:
+/// appending keeps every member's index, anything else may not.
+fn moved(schema: &CatalogColumn, catalog: &CatalogColumn) -> bool {
+    matches!(catalog.data_type.as_str(), "enum" | "set")
+        && !members(&catalog.column_type).starts_with(&members(&schema.column_type))
+}
+
 /// The decode an override wraps: the column's own type, which orders the key.
 fn base(d: &Decode) -> &Decode {
     match d {
@@ -373,6 +392,12 @@ impl MySqlConnector {
         after: Option<&KeysetCursor>,
         limit: usize,
     ) -> Result<Chunk, SourceError> {
+        // An empty chunk ends the table: one of no rows asked would end it early (DA-SRC-15).
+        if limit == 0 {
+            return Err(SourceError::Permanent(
+                "a snapshot chunk of 0 rows: chunk_size is at least 1 (V-108)".into(),
+            ));
+        }
         let columns = read_plan(self, table).await?;
         let keys = key_columns(table, &columns);
         let collation = cursor_collation(&keys);
```

`crates/data-avalanche-source-pg/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/catalog.rs b/crates/data-avalanche-source-pg/src/catalog.rs
index 25045a4..461f312 100644
--- a/crates/data-avalanche-source-pg/src/catalog.rs
+++ b/crates/data-avalanche-source-pg/src/catalog.rs
@@ -149,6 +149,9 @@ pub struct Described {
     pub primary_key: Vec<String>,
     /// The columns of the `REPLICA IDENTITY USING INDEX` index.
     pub identity_index: Vec<String>,
+    /// The key columns the primary key's or the replica identity's index sorts in another
+    /// collation than the column's own: `(column, the index's collation)`.
+    pub index_collations: Vec<(String, String)>,
 }
 
 /// The table's config keys: `schema.table` (config §9.3).
@@ -166,6 +169,10 @@ pub struct Notes {
     /// Identifier columns in a nondeterministic collation: `(column, collation)`. Snapshot chunks
     /// order them by `"C"`, which their index doesn't serve.
     pub c_ordered_keys: Vec<(String, String)>,
+    /// Identifier columns in a deterministic collation that their key index sorts in another
+    /// (`COLLATE "C"`): `(column, the index's collation)`. Snapshot chunks order them by
+    /// their own, which that index doesn't serve (DA-SRC-15).
+    pub index_ordered_otherwise: Vec<(String, String)>,
 }
 
 /// The replica identity's columns.
@@ -260,13 +267,18 @@ pub fn table_schema(
         return Err(HaltReason::IdentifierNotInReplicaIdentity { missing });
     }
     for k in schema.identifier() {
-        if let Some(collation) = d
-            .columns
-            .iter()
-            .find(|a| &a.name == k)
-            .and_then(|a| a.nondeterministic.clone())
-        {
-            notes.c_ordered_keys.push((k.clone(), collation));
+        let Some(a) = d.columns.iter().find(|a| &a.name == k) else {
+            continue;
+        };
+        match &a.nondeterministic {
+            Some(collation) => notes.c_ordered_keys.push((k.clone(), collation.clone())),
+            None => {
+                if let Some((_, other)) = d.index_collations.iter().find(|(c, _)| c == k) {
+                    notes
+                        .index_ordered_otherwise
+                        .push((k.clone(), other.clone()));
+                }
+            }
         }
     }
     if let Some(fields) = tables.partitioning.get(&key) {
@@ -364,6 +376,7 @@ pub async fn describe(
                 columns: vec![],
                 primary_key: vec![],
                 identity_index: vec![],
+                index_collations: vec![],
             },
         );
     }
@@ -402,8 +415,17 @@ pub async fn describe(
                             FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
                             JOIN pg_catalog.pg_attribute a
                               ON a.attrelid = i.indrelid AND a.attnum = k.attnum
-                           ORDER BY k.ord)
-               FROM pg_catalog.pg_index i
+                           ORDER BY k.ord),
+                    o.columns, o.collations
+               FROM pg_catalog.pg_index i,
+                    LATERAL (SELECT array_agg(a.attname::text ORDER BY k.ord) AS columns,
+                                    array_agg(co.collname::text ORDER BY k.ord) AS collations
+                               FROM unnest(i.indkey::int2[], i.indcollation::oid[])
+                                    WITH ORDINALITY AS k(attnum, coll, ord)
+                               JOIN pg_catalog.pg_attribute a
+                                 ON a.attrelid = i.indrelid AND a.attnum = k.attnum
+                               JOIN pg_catalog.pg_collation co ON co.oid = k.coll
+                              WHERE k.coll <> a.attcollation) AS o
               WHERE i.indrelid = ANY($1) AND (i.indisprimary OR i.indisreplident)",
             &[&oids],
         )
@@ -411,6 +433,17 @@ pub async fn describe(
     {
         if let Some(d) = out.get_mut(&r.get::<_, u32>(0)) {
             let columns: Vec<String> = r.get(3);
+            let other: Option<Vec<String>> = r.get(4);
+            let collations: Option<Vec<String>> = r.get(5);
+            for (column, collation) in other
+                .unwrap_or_default()
+                .into_iter()
+                .zip(collations.unwrap_or_default())
+            {
+                if !d.index_collations.iter().any(|(c, _)| *c == column) {
+                    d.index_collations.push((column, collation));
+                }
+            }
             if r.get::<_, bool>(1) {
                 d.primary_key = columns.clone();
             }
@@ -579,6 +612,7 @@ mod tests {
             ],
             primary_key: vec!["id".into()],
             identity_index: vec!["code".into()],
+            index_collations: vec![],
         }
     }
 
```

`crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index a34d29b..6665a7d 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -270,6 +270,14 @@ impl PgConnector {
                              unless an index has ({column} COLLATE \"C\")"
                         ));
                     }
+                    for (column, collation) in notes.index_ordered_otherwise {
+                        out.warnings.push(format!(
+                            "{key}.{column}: the key's index sorts it in {collation}, not in the \
+                             column's own collation, which snapshot chunks order by: \
+                             each chunk scans the table unless an index has ({column}) in its \
+                             own collation"
+                        ));
+                    }
                     unconstrained.extend(
                         notes
                             .unconstrained
```

`crates/data-avalanche-source-pg/src/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/snapshot.rs b/crates/data-avalanche-source-pg/src/snapshot.rs
index d14763a..2977f4c 100644
--- a/crates/data-avalanche-source-pg/src/snapshot.rs
+++ b/crates/data-avalanche-source-pg/src/snapshot.rs
@@ -44,8 +44,9 @@ pub(crate) struct ReadColumn {
 /// deterministic collation for both, so the order and the cursor agree.
 #[derive(Debug, Clone, PartialEq, Eq)]
 enum KeyCollation {
-    /// The column's own, deterministic: its index serves the order.
-    Own(String),
+    /// The column's own, deterministic: its index serves the order. `name` is what the cursor
+    /// records; `qualified`, with its schema, what a cursor literal is collated in.
+    Own { name: String, qualified: String },
     /// A nondeterministic collation's column, compared in `"C"`.
     C,
 }
@@ -53,7 +54,7 @@ enum KeyCollation {
 impl KeyCollation {
     fn name(&self) -> &str {
         match self {
-            KeyCollation::Own(name) => name,
+            KeyCollation::Own { name, .. } => name,
             KeyCollation::C => "C",
         }
     }
@@ -93,9 +94,10 @@ async fn read_plan(c: &PgConnector, schema: &TableSchema) -> Result<Vec<ReadColu
                 &format!(
                     "SELECT a.attname, a.atttypid, a.atttypmod,
                             pg_catalog.format_type(a.atttypid, a.atttypmod), co.collname::text,
-                            co.collisdeterministic, {NOT_NULL}
+                            co.collisdeterministic, {NOT_NULL}, cn.nspname::text
                        FROM pg_catalog.pg_attribute a
                        LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
+                       LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = co.collnamespace
                       WHERE a.attrelid = $1 AND a.attname = ANY($2)
                         AND a.attnum > 0 AND NOT a.attisdropped"
                 ),
@@ -154,7 +156,10 @@ async fn read_plan(c: &PgConnector, schema: &TableSchema) -> Result<Vec<ReadColu
             ));
         }
         let collation = match (r.get::<_, Option<String>>(4), r.get::<_, Option<bool>>(5)) {
-            (Some(name), Some(true)) => Some(KeyCollation::Own(name)),
+            (Some(name), Some(true)) => Some(KeyCollation::Own {
+                qualified: format!("{}.{}", ident(r.get(7)), ident(&name)),
+                name,
+            }),
             (Some(_), _) => Some(KeyCollation::C),
             (None, _) => None,
         };
@@ -219,14 +224,15 @@ fn cast(c: &ReadColumn, text: &str) -> String {
     }
 }
 
-/// A key value in a keyset comparison: [`cast`], collated like the column's [`key_expr`]. A cast
-/// literal's collation yields to the column's own.
+/// A key value in a keyset comparison: [`cast`], collated like the column's [`key_expr`], named
+/// with its schema. Explicitly: a cast to a domain with a collation of its own carries that one,
+/// which conflicts with the column's (42P22, DA-SRC-15).
 fn key_value(c: &ReadColumn, text: &str) -> String {
     let cast = cast(c, text);
-    if c.collation == Some(KeyCollation::C) {
-        format!("{cast} COLLATE \"C\"")
-    } else {
-        cast
+    match &c.collation {
+        Some(KeyCollation::C) => format!("{cast} COLLATE \"C\""),
+        Some(KeyCollation::Own { qualified, .. }) => format!("{cast} COLLATE {qualified}"),
+        None => cast,
     }
 }
 
@@ -466,6 +472,12 @@ impl PgConnector {
         after: Option<&KeysetCursor>,
         limit: usize,
     ) -> Result<Chunk, SourceError> {
+        // An empty chunk ends the table: one of no rows asked would end it early (DA-SRC-15).
+        if limit == 0 {
+            return Err(SourceError::Permanent(
+                "a snapshot chunk of 0 rows: chunk_size is at least 1 (V-108)".into(),
+            ));
+        }
         let columns = read_plan(self, table).await?;
         let keys = key_columns(table, &columns);
         let collation = cursor_collation(&keys);
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same.

Expected: PASS: pipeline 256, source-pg 147, 148 tests on each MySQL target.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline/src/worker.rs \
  crates/data-avalanche-source-mysql/src/snapshot.rs \
  crates/data-avalanche-source-mysql/tests/mysql/snapshot.rs \
  crates/data-avalanche-source-pg/src/catalog.rs \
  crates/data-avalanche-source-pg/src/snapshot.rs \
  crates/data-avalanche-source-pg/tests/pg/snapshot.rs \
  crates/data-avalanche-pipeline/src/schema_change.rs \
  crates/data-avalanche-source-pg/src/connector.rs
git commit -m "feat(pipeline): a wiped state baselines against the sink table (DA-SNAP-13); a reordered ENUM key waits for the stream (DA-SNAP-14); a domain's collation named with its schema, an index in another collation warned, a chunk of 0 rows refused (DA-SRC-15)"
```

---

### Task 9: The publication's blind spots (`source-pg`)

**Files:**
- Modify: `crates/data-avalanche-source-pg/src/slot.rs`
- Test: `crates/data-avalanche-source-pg/tests/pg/slot.rs`

**Interfaces:**
- Consumes: `reconcile_publication` (M5), `partitions` (M5).
- Produces: `struct Shape { members, filtered, schemas, generated_columns, generated }` and
  `async fn publication_shape(client, name, version, tables) -> Result<Shape, tokio_postgres::Error>`.
  Each part is read only on the server version that has it: row filters, column lists and
  schemas from 15, `publish_generated_columns` from 18.

- [ ] **Step 1: Write the failing test**

`crates/data-avalanche-source-pg/tests/pg/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/slot.rs b/crates/data-avalanche-source-pg/tests/pg/slot.rs
index a15d2ce..055b01e 100644
--- a/crates/data-avalanche-source-pg/tests/pg/slot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/slot.rs
@@ -534,6 +534,142 @@ async fn a_slot_of_another_plugin_is_refused() {
     cleanup(&a, &n).await;
 }
 
+/// What a publication publishes besides its table list (DA-SRC-13): a tracked table's row filter
+/// or column list, and stored generated columns (PostgreSQL 18), are refused under
+/// `require_existing` and repaired under `managed`; tables the pipeline doesn't track are warned;
+/// a partition the publication names itself isn't refused for `publish_via_partition_root`; a
+/// managed publication of a schema is set to the tracked tables.
+#[tokio::test]
+async fn a_publication_s_blind_spots_are_refused_warned_or_repaired() {
+    require_it!();
+    let a = admin().await;
+    let version: i32 = a
+        .query_one("SELECT current_setting('server_version_num')::int", &[])
+        .await
+        .unwrap()
+        .get(0);
+    if version < 150_000 {
+        eprintln!("skipped: row filters, column lists and schemas need PostgreSQL 15");
+        return;
+    }
+    let n = fresh(
+        &a,
+        "slot_pub_shape",
+        "CREATE TABLE a (id int PRIMARY KEY, v int);
+         CREATE TABLE b (id int PRIMARY KEY, v int);
+         CREATE TABLE c (id int PRIMARY KEY);
+         CREATE TABLE events (id int, at date, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
+         CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');",
+    )
+    .await;
+    let s = &n.schema;
+    let start: String = a
+        .query_one(
+            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
+            &[&n.slot],
+        )
+        .await
+        .unwrap()
+        .get(0);
+    let at = Checkpoint::from(SourcePosition::Pg(start.parse().unwrap()));
+    let tracked = [n.table("a"), n.table("b")];
+    let existing = connector(&pipeline(
+        &n,
+        &["*"],
+        "",
+        ", slot_management: require_existing, auto_cleanup: false",
+    ));
+    let publish = |spec: String| {
+        let a = &a;
+        let p = n.publication.clone();
+        async move {
+            a.batch_execute(&format!(
+                "DROP PUBLICATION IF EXISTS {p}; CREATE PUBLICATION {p} {spec}"
+            ))
+            .await
+            .unwrap();
+        }
+    };
+    for spec in [
+        format!("FOR TABLE {s}.a WHERE (id > 0), {s}.b"),
+        format!("FOR TABLE {s}.a, {s}.b (id)"),
+    ] {
+        publish(spec.clone()).await;
+        permanent(
+            existing.prepare(Some(&at), &tracked).await.unwrap_err(),
+            "with a row filter or a column list",
+        );
+    }
+    // A filter on a table the pipeline doesn't track, and the table itself, are only warned.
+    publish(format!("FOR TABLE {s}.a, {s}.b, {s}.c WHERE (id > 0)")).await;
+    let p = existing.prepare(Some(&at), &tracked).await.unwrap();
+    assert_eq!(p.plan, StartPlan::Resume);
+    assert!(
+        p.warnings.iter().any(|w| w.contains(&format!(
+            "also publishes tables the pipeline doesn't replicate ({s}.c)"
+        ))),
+        "{:?}",
+        p.warnings
+    );
+    // A partition named itself arrives under its own name.
+    publish(format!(
+        "FOR TABLE {s}.events_2024 WITH (publish_via_partition_root = true)"
+    ))
+    .await;
+    let p = existing
+        .prepare(Some(&at), &[n.table("events_2024")])
+        .await
+        .unwrap();
+    assert_eq!(p.plan, StartPlan::Resume);
+
+    // Managed: the filter and the schema go, the tracked tables are published whole.
+    publish(format!(
+        "FOR TABLE {s}.a WHERE (id > 0), TABLES IN SCHEMA {s}"
+    ))
+    .await;
+    let managed = connector(&pipeline(&n, &["*"], "", ""));
+    let p = managed.prepare(Some(&at), &tracked).await.unwrap();
+    from(&p.plan, SnapshotReason::PositionLost);
+    assert_eq!(published(&a, &n).await, ["a", "b"]);
+    let filters: i64 = a
+        .query_one(
+            "SELECT count(*) FROM pg_publication_rel r JOIN pg_publication p ON p.oid = r.prpubid
+              WHERE p.pubname = $1 AND (r.prqual IS NOT NULL OR r.prattrs IS NOT NULL)",
+            &[&n.publication],
+        )
+        .await
+        .unwrap()
+        .get(0);
+    assert_eq!(filters, 0);
+
+    if version >= 180_000 {
+        a.batch_execute(&format!(
+            "ALTER TABLE {s}.b ADD COLUMN twice int GENERATED ALWAYS AS (v * 2) STORED"
+        ))
+        .await
+        .unwrap();
+        publish(format!(
+            "FOR TABLE {s}.a, {s}.b WITH (publish_generated_columns = stored)"
+        ))
+        .await;
+        permanent(
+            existing.prepare(Some(&at), &tracked).await.unwrap_err(),
+            "publishes stored generated columns",
+        );
+        managed.prepare(Some(&at), &tracked).await.unwrap();
+        let stored: bool = a
+            .query_one(
+                "SELECT pubgencols = 's' FROM pg_publication WHERE pubname = $1",
+                &[&n.publication],
+            )
+            .await
+            .unwrap()
+            .get(0);
+        assert!(!stored, "set to none");
+    }
+    cleanup(&a, &n).await;
+}
+
 #[tokio::test]
 async fn a_root_publication_of_tracked_partitions_is_refused() {
     require_it!();
```


- [ ] **Step 2: Run the test to verify it fails**

Run: `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg -- --test-threads=1 --skip tls::`

Expected: FAIL, 1 of 58 in `tests/pg`: `a_publication_s_blind_spots_are_refused_warned_or_repaired`
(`unwrap_err()` on an `Ok` value: `Prepared { plan: Resume, … }`, a row filter on a tracked table
accepted).

- [ ] **Step 3: Implement**

`crates/data-avalanche-source-pg/src/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
index 7d9aa61..43640a3 100644
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -154,6 +154,107 @@ pub(crate) fn qualified(t: &TableRef) -> String {
     format!("{}.{}", ident(&t.schema), ident(&t.name))
 }
 
+/// What a publication publishes beyond its table list (DA-SRC-13), by server version.
+#[derive(Debug, Default)]
+struct Shape {
+    /// Its tables by name (`pg_publication_rel`): what `DROP TABLE` can take out.
+    members: Vec<TableRef>,
+    /// PostgreSQL 15+: its member tables with a row filter or a column list.
+    filtered: Vec<TableRef>,
+    /// PostgreSQL 15+: the schemas it publishes (`FOR TABLES IN SCHEMA`).
+    schemas: Vec<String>,
+    /// PostgreSQL 18+: `publish_generated_columns = stored`.
+    generated_columns: bool,
+    /// Of `tables`, those with a stored generated column (read when it publishes them).
+    generated: Vec<TableRef>,
+}
+
+async fn publication_shape(
+    client: &Client,
+    name: &str,
+    version: i32,
+    tables: &[TableRef],
+) -> Result<Shape, tokio_postgres::Error> {
+    let table =
+        |r: &tokio_postgres::Row| TableRef::new(r.get::<_, String>(0), r.get::<_, String>(1));
+    let rows = if version >= 150_000 {
+        client
+            .query(
+                "SELECT n.nspname::text, c.relname::text,
+                        r.prqual IS NOT NULL OR r.prattrs IS NOT NULL
+                   FROM pg_catalog.pg_publication_rel r
+                   JOIN pg_catalog.pg_publication p ON p.oid = r.prpubid
+                   JOIN pg_catalog.pg_class c ON c.oid = r.prrelid
+                   JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
+                  WHERE p.pubname = $1",
+                &[&name],
+            )
+            .await?
+    } else {
+        client
+            .query(
+                "SELECT n.nspname::text, c.relname::text, false
+                   FROM pg_catalog.pg_publication_rel r
+                   JOIN pg_catalog.pg_publication p ON p.oid = r.prpubid
+                   JOIN pg_catalog.pg_class c ON c.oid = r.prrelid
+                   JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
+                  WHERE p.pubname = $1",
+                &[&name],
+            )
+            .await?
+    };
+    let mut shape = Shape {
+        members: rows.iter().map(table).collect(),
+        filtered: rows.iter().filter(|r| r.get(2)).map(table).collect(),
+        ..Shape::default()
+    };
+    if version >= 150_000 {
+        shape.schemas = client
+            .query(
+                "SELECT n.nspname::text
+                   FROM pg_catalog.pg_publication_namespace pn
+                   JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
+                   JOIN pg_catalog.pg_namespace n ON n.oid = pn.pnnspid
+                  WHERE p.pubname = $1 ORDER BY 1",
+                &[&name],
+            )
+            .await?
+            .iter()
+            .map(|r| r.get(0))
+            .collect();
+    }
+    if version >= 180_000 {
+        shape.generated_columns = client
+            .query_one(
+                "SELECT pubgencols = 's' FROM pg_catalog.pg_publication WHERE pubname = $1",
+                &[&name],
+            )
+            .await?
+            .get(0);
+    }
+    if shape.generated_columns {
+        let schemas: Vec<&str> = tables.iter().map(|t| t.schema.as_str()).collect();
+        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
+        shape.generated = client
+            .query(
+                "SELECT DISTINCT n.nspname::text, c.relname::text
+                   FROM pg_catalog.pg_class c
+                   JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
+                   JOIN unnest($1::text[], $2::text[]) AS w(s, t)
+                     ON w.s = n.nspname AND w.t = c.relname
+                   JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
+                  WHERE a.attgenerated = 's' AND NOT a.attisdropped
+                  ORDER BY 1, 2",
+                &[&schemas, &names],
+            )
+            .await?
+            .iter()
+            .map(table)
+            .collect();
+    }
+    Ok(shape)
+}
+
 /// The tables among `tables` that are partitions (`relispartition`).
 async fn partitions(
     client: &Client,
@@ -335,7 +436,7 @@ impl PgConnector {
                 "read the publication",
                 client.query_opt(
                     "SELECT puballtables, pubinsert AND pubupdate AND pubdelete AND pubtruncate,
-                            pubviaroot
+                            pubviaroot, current_setting('server_version_num')::int
                        FROM pg_catalog.pg_publication WHERE pubname = $1",
                     &[name],
                 ),
@@ -364,8 +465,8 @@ impl PgConnector {
                 ..Reconciled::default()
             });
         };
-        let (all_tables, every_op, via_root): (bool, bool, bool) =
-            (row.get(0), row.get(1), row.get(2));
+        let (all_tables, every_op, via_root, version): (bool, bool, bool, i32) =
+            (row.get(0), row.get(1), row.get(2), row.get(3));
         let published: BTreeSet<TableRef> = self
             .conn
             .command(
@@ -384,15 +485,55 @@ impl PgConnector {
             .copied()
             .filter(|t| !published.contains(*t))
             .collect();
+        let shape = self
+            .conn
+            .command(
+                "read what the publication publishes",
+                publication_shape(client, name, version, tables),
+            )
+            .await?;
+        let filtered: Vec<String> = shape
+            .filtered
+            .iter()
+            .filter(|t| wanted.contains(t))
+            .map(table_key)
+            .collect();
         let mut warnings = vec![];
         if !managed || all_tables {
+            // A tracked table's row filter or column list (PostgreSQL 15+) keeps some of its rows
+            // or columns from the stream (DA-SRC-13).
+            if !filtered.is_empty() {
+                return Err(SourceError::Permanent(format!(
+                    "publication {name} publishes {} with a row filter or a column list, so some \
+                     of the changes the pipeline replicates wouldn't arrive; publish the tables \
+                     whole (ALTER PUBLICATION … DROP TABLE, then ADD TABLE without WHERE or a \
+                     column list)",
+                    filtered.join(", ")
+                )));
+            }
+            // PostgreSQL 18 publishes stored generated columns when asked, which the pipeline
+            // doesn't replicate.
+            if shape.generated_columns && !shape.generated.is_empty() {
+                let list: Vec<String> = shape.generated.iter().map(table_key).collect();
+                return Err(SourceError::Permanent(format!(
+                    "publication {name} publishes stored generated columns (of {}), which the \
+                     pipeline doesn't replicate; run ALTER PUBLICATION {} SET \
+                     (publish_generated_columns = none)",
+                    list.join(", "),
+                    ident(name)
+                )));
+            }
             // Q-M5-5: a partition's changes would arrive under its parent's name, which isn't
-            // tracked, so the stream would drop them. Refused, not warned: it would lose data.
+            // tracked, so the stream would drop them. Refused, not warned: it would lose data. A
+            // partition the publication names itself arrives under its own.
             if via_root {
-                let leaves = self
+                let leaves: Vec<TableRef> = self
                     .conn
                     .command("find partitions", partitions(client, tables))
-                    .await?;
+                    .await?
+                    .into_iter()
+                    .filter(|l| !published.contains(l))
+                    .collect();
                 if !leaves.is_empty() {
                     let list: Vec<String> = leaves.iter().map(table_key).collect();
                     return Err(SourceError::Permanent(format!(
@@ -412,35 +553,85 @@ impl PgConnector {
                     "publication {name} doesn't publish every operation; set publish = 'insert, update, delete, truncate'"
                 ));
             }
+            // The server decodes the other tables' changes for the slot, and the stream drops
+            // them (D-M5.14).
+            let others: Vec<String> = published
+                .iter()
+                .filter(|t| !wanted.contains(t))
+                .map(table_key)
+                .collect();
+            if !others.is_empty() {
+                let shown = others
+                    .iter()
+                    .take(5)
+                    .cloned()
+                    .collect::<Vec<_>>()
+                    .join(", ");
+                let more = match others.len() {
+                    n if n > 5 => format!(" and {} more", n - 5),
+                    _ => String::new(),
+                };
+                warnings.push(format!(
+                    "publication {name} also publishes tables the pipeline doesn't replicate \
+                     ({shown}{more}): the server decodes their changes for the slot, and the \
+                     pipeline drops them"
+                ));
+            }
             return Ok(Reconciled {
                 warnings,
                 ..Reconciled::default()
             });
         }
         let mut sql = vec![];
-        if !missing.is_empty() {
-            let list: Vec<String> = missing.iter().map(|t| qualified(t)).collect();
+        // A row filter, a column list or a schema the publication publishes can't be taken out
+        // table by table (a schema's table isn't a member to drop, 42704): the managed
+        // publication is set to the tracked tables whole.
+        let reset = !filtered.is_empty() || !shape.schemas.is_empty();
+        let list: Vec<String> = tables.iter().map(qualified).collect();
+        if reset && !list.is_empty() {
             sql.push(format!(
-                "ALTER PUBLICATION {} ADD TABLE {}",
+                "ALTER PUBLICATION {} SET TABLE {}",
                 ident(name),
                 list.join(", ")
             ));
+        } else {
+            if !shape.schemas.is_empty() {
+                let schemas: Vec<String> = shape.schemas.iter().map(|s| ident(s)).collect();
+                sql.push(format!(
+                    "ALTER PUBLICATION {} DROP TABLES IN SCHEMA {}",
+                    ident(name),
+                    schemas.join(", ")
+                ));
+            }
+            if !missing.is_empty() {
+                let list: Vec<String> = missing.iter().map(|t| qualified(t)).collect();
+                sql.push(format!(
+                    "ALTER PUBLICATION {} ADD TABLE {}",
+                    ident(name),
+                    list.join(", ")
+                ));
+            }
+            let extra: Vec<String> = shape
+                .members
+                .iter()
+                .filter(|t| !wanted.contains(t))
+                .map(qualified)
+                .collect();
+            if !extra.is_empty() {
+                sql.push(format!(
+                    "ALTER PUBLICATION {} DROP TABLE {}",
+                    ident(name),
+                    extra.join(", ")
+                ));
+            }
         }
-        let extra: Vec<String> = published
-            .iter()
-            .filter(|t| !wanted.contains(t))
-            .map(qualified)
-            .collect();
-        if !extra.is_empty() {
-            sql.push(format!(
-                "ALTER PUBLICATION {} DROP TABLE {}",
-                ident(name),
-                extra.join(", ")
-            ));
-        }
-        if !every_op || via_root {
+        if !every_op || via_root || shape.generated_columns {
+            let generated = match shape.generated_columns {
+                true => ", publish_generated_columns = none",
+                false => "",
+            };
             sql.push(format!(
-                "ALTER PUBLICATION {} SET (publish = 'insert, update, delete, truncate', publish_via_partition_root = false)",
+                "ALTER PUBLICATION {} SET (publish = 'insert, update, delete, truncate', publish_via_partition_root = false{generated})",
                 ident(name)
             ));
         }
@@ -454,7 +645,10 @@ impl PgConnector {
                 .is_empty();
         Ok(Reconciled {
             sql,
-            rewritten: !every_op || partitioned,
+            rewritten: !every_op
+                || partitioned
+                || !filtered.is_empty()
+                || (shape.generated_columns && !shape.generated.is_empty()),
             added: missing.into_iter().cloned().collect(),
             warnings,
         })
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same.

Expected: PASS: 148 tests.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-source-pg/tests/pg/slot.rs \
  crates/data-avalanche-source-pg/src/slot.rs
git commit -m "feat(source-pg): the publication's blind spots — a tracked table's row filter or column list and stored generated columns refused under require_existing and repaired under managed, the tables it publishes beyond the pipeline's warned, publish_via_partition_root refused only for a partition it doesn't name"
```

---

### Task 10: PostgreSQL's small edges (`config`, `source-pg`, `source-mysql`)

**Files:**
- Modify: `crates/data-avalanche-config/src/diag.rs`, `crates/data-avalanche-source-pg/src/connect.rs`, `crates/data-avalanche-source-pg/src/connector.rs`, `crates/data-avalanche-source-pg/src/error.rs`, `crates/data-avalanche-source-pg/src/value.rs`, `crates/data-avalanche-source-mysql/src/catalog.rs`

**Interfaces:**
- Consumes: `Report` (M1), the connectors' table-set checks (M5, M7a), `bounded` (M6b),
  `civil_days` (M5).
- Produces:
  - `Report::refusal(&self) -> Option<String>`: every error, `rule path: message`, `; `-separated;
  - `fn db_message(message, detail, hint, code) -> String`;
  - `fn standby(version_num: i32, in_recovery: bool) -> Result<Option<String>, String>`;
  - `bounded` waits for the connection cache within its own deadline, and drops the cache's
    guard before the call: `abandon` takes the cache again on a timeout;
  - `civil_days` refuses a year past 5874897.

DA-SRC-17's other items are recorded, not coded (D-M12a.13). DA-SRC-9 is decided (D-M12a.14).

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-config/src/diag.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/diag.rs b/crates/data-avalanche-config/src/diag.rs
index e7aab19..3b939e3 100644
--- a/crates/data-avalanche-config/src/diag.rs
+++ b/crates/data-avalanche-config/src/diag.rs
@@ -127,6 +127,20 @@ mod tests {
         assert!(r.has("V-037"));
     }
 
+    #[test]
+    fn a_refusal_names_every_error() {
+        let mut r = Report::default();
+        assert_eq!(r.refusal(), None);
+        r.push(d("V-037", Severity::Warning, "w"));
+        assert_eq!(r.refusal(), None, "warnings don't refuse");
+        r.push(d("V-113", Severity::Error, "b"));
+        r.push(d("V-113", Severity::Error, "a"));
+        assert_eq!(
+            r.refusal().as_deref(),
+            Some("V-113 a: V-113 at a; V-113 b: V-113 at b")
+        );
+    }
+
     #[test]
     fn json_shape_is_stable() {
         let mut r = Report::default();
```

`crates/data-avalanche-source-pg/src/connect.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connect.rs b/crates/data-avalanche-source-pg/src/connect.rs
index bf20c6a..fe00dc1 100644
--- a/crates/data-avalanche-source-pg/src/connect.rs
+++ b/crates/data-avalanche-source-pg/src/connect.rs
@@ -277,6 +277,73 @@ mod tests {
         );
     }
 
+    /// A call queued behind a reconnect that holds the connection cache gives up within its own
+    /// budget, not the reconnect's.
+    #[tokio::test]
+    async fn a_bounded_call_waits_for_the_cache_within_its_budget() {
+        let c = Connector::new(&pipeline("public.t").source).unwrap();
+        let held = c.client.lock().await;
+        let started = tokio::time::Instant::now();
+        let r = c
+            .bounded("a query", Duration::from_millis(100), async {
+                Ok::<_, tokio_postgres::Error>(())
+            })
+            .await;
+        assert!(matches!(r, Err(SourceError::Transient(m)) if m.contains("timed out")));
+        assert!(started.elapsed() < Duration::from_secs(1));
+        drop(held);
+    }
+
+    /// A server that completes the startup, then answers no query (frozen mid-session): a call
+    /// on the cached connection times out within its budget, and abandons the connection.
+    #[tokio::test]
+    async fn a_call_on_a_frozen_connection_times_out_and_frees_the_cache() {
+        use tokio::io::{AsyncReadExt, AsyncWriteExt};
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let port = listener.local_addr().unwrap().port();
+        let server = tokio::spawn(async move {
+            let mut sockets = vec![];
+            while let Ok((mut s, _)) = listener.accept().await {
+                // A startup packet or a cancel request: its length, then the rest.
+                let mut len = [0u8; 4];
+                if s.read_exact(&mut len).await.is_err() {
+                    continue;
+                }
+                let mut rest = vec![0u8; u32::from_be_bytes(len) as usize - 4];
+                let _ = s.read_exact(&mut rest).await;
+                // AuthenticationOk, BackendKeyData, ReadyForQuery (idle), then silence.
+                let mut reply = vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0];
+                reply.extend([b'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2]);
+                reply.extend([b'Z', 0, 0, 0, 5, b'I']);
+                let _ = s.write_all(&reply).await;
+                sockets.push(s);
+            }
+        });
+        let mut source = pipeline("public.t").source;
+        source.port = port;
+        source.timeouts.connect = Duration::from_millis(300);
+        let c = Connector::new(&source).unwrap();
+        let client = c.client().await.expect("the startup completes");
+        let started = tokio::time::Instant::now();
+        let r = tokio::time::timeout(
+            Duration::from_secs(5),
+            c.bounded(
+                "a query",
+                Duration::from_millis(200),
+                client.simple_query("SELECT 1"),
+            ),
+        )
+        .await
+        .expect("the call returns within its budget");
+        assert!(matches!(r, Err(SourceError::Transient(m)) if m.contains("timed out")));
+        assert!(started.elapsed() < Duration::from_secs(2));
+        assert!(
+            c.client.lock().await.is_none(),
+            "the frozen connection is abandoned"
+        );
+        server.abort();
+    }
+
     /// A server that accepts the connection and never answers: the startup times out.
     #[tokio::test]
     async fn a_connect_to_a_server_that_never_answers_times_out() {
```

`crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index 6665a7d..812b407 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -428,6 +428,18 @@ mod tests {
     use super::*;
     use crate::test_support::pipeline;
 
+    #[test]
+    fn a_standby_is_refused_before_16() {
+        assert_eq!(standby(150_004, false), Ok(None));
+        let e = standby(150_004, true).unwrap_err();
+        assert!(
+            e.starts_with("the server is a standby (in recovery)"),
+            "{e}"
+        );
+        let w = standby(160_000, true).unwrap().unwrap();
+        assert!(w.contains("hot_standby_feedback"), "{w}");
+    }
+
     #[test]
     fn a_connector_is_a_source_connector() {
         let c: Box<dyn SourceConnector> =
```

`crates/data-avalanche-source-pg/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/error.rs b/crates/data-avalanche-source-pg/src/error.rs
index c288858..5260270 100644
--- a/crates/data-avalanche-source-pg/src/error.rs
+++ b/crates/data-avalanche-source-pg/src/error.rs
@@ -119,6 +119,25 @@ mod tests {
     use super::*;
     use std::sync::Arc;
 
+    #[test]
+    fn a_server_error_keeps_its_detail_and_hint() {
+        assert_eq!(
+            db_message("boom", None, None, "XX000"),
+            "boom (SQLSTATE XX000)"
+        );
+        let m = db_message(
+            "permission denied for table t",
+            Some("the role lost SELECT"),
+            Some("GRANT SELECT ON t"),
+            "42501",
+        );
+        assert_eq!(
+            m,
+            "permission denied for table t: the role lost SELECT (hint: GRANT SELECT ON t) (SQLSTATE 42501)"
+        );
+        assert_eq!(sqlstate(&m), Some("42501"));
+    }
+
     fn server(m: &str) -> PgWireError {
         PgWireError::Server(m.into())
     }
```

`crates/data-avalanche-source-pg/src/value.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/value.rs b/crates/data-avalanche-source-pg/src/value.rs
index f616e2d..0dda47b 100644
--- a/crates/data-avalanche-source-pg/src/value.rs
+++ b/crates/data-avalanche-source-pg/src/value.rs
@@ -555,6 +555,28 @@ mod tests {
         );
     }
 
+    /// Crafted text PostgreSQL never prints: a year past its range is refused, not overflowed
+    /// (DA-SRC-17).
+    #[test]
+    fn a_year_past_postgresql_s_range_is_refused() {
+        for text in [
+            "9223372036854775807-01-01",
+            "5874898-01-01",
+            "9000000000000-12-31 23:59:59+00",
+        ] {
+            let decoded = if text.contains(' ') {
+                decode(&Decode::Timestamp { with_tz: true }, text)
+            } else {
+                decode(&Decode::Date, text)
+            };
+            assert!(
+                matches!(decoded, Err(ValueError::Unrepresentable(_))),
+                "{text}: {decoded:?}"
+            );
+        }
+        assert!(decode(&Decode::Date, "5874897-12-31").is_ok());
+    }
+
     #[test]
     fn scalars() {
         assert_eq!(ok(Decode::Bool, "t"), V::Bool(true));
```


- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`, the PostgreSQL source, and the MySQL source on the three targets

Expected:
- config: FAIL to compile: no method `Report::refusal` (E0599).
- source-pg: FAIL to compile: cannot find function `standby`, `db_message` (E0425).
- source-mysql: PASS, 148 on each target (its refusal's test is config's).

- [ ] **Step 3: Implement**

`crates/data-avalanche-config/src/diag.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/diag.rs b/crates/data-avalanche-config/src/diag.rs
index 3b939e3..9a149de 100644
--- a/crates/data-avalanche-config/src/diag.rs
+++ b/crates/data-avalanche-config/src/diag.rs
@@ -87,6 +87,16 @@ impl Report {
         self.iter().any(|d| d.rule == rule)
     }
 
+    /// The errors as one refusal, `rule path: message` each, `; `-separated; `None` without any.
+    pub fn refusal(&self) -> Option<String> {
+        let all: Vec<String> = self
+            .errors
+            .iter()
+            .map(|e| format!("{} {}: {}", e.rule, e.path, e.message))
+            .collect();
+        (!all.is_empty()).then(|| all.join("; "))
+    }
+
     pub fn to_json(&self) -> serde_json::Value {
         serde_json::json!({
             "valid": self.is_ok(),
```

`crates/data-avalanche-source-mysql/src/catalog.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/catalog.rs b/crates/data-avalanche-source-mysql/src/catalog.rs
index efc89b9..ec15ab9 100644
--- a/crates/data-avalanche-source-mysql/src/catalog.rs
+++ b/crates/data-avalanche-source-mysql/src/catalog.rs
@@ -319,11 +319,9 @@ impl MySqlConnector {
             .chain(out.halted.iter().map(|(t, _)| t.clone()))
             .collect();
         let report = check_table_set(&self.pipeline, &resolved);
-        if let Some(e) = report.errors.first() {
-            return Err(SourceError::Permanent(format!(
-                "{} {}: {}",
-                e.rule, e.path, e.message
-            )));
+        // Every error, not the first (DA-SRC-17).
+        if let Some(refusal) = report.refusal() {
+            return Err(SourceError::Permanent(refusal));
         }
         out.warnings.extend(
             report
```

`crates/data-avalanche-source-pg/src/connect.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connect.rs b/crates/data-avalanche-source-pg/src/connect.rs
index fe00dc1..c5c3680 100644
--- a/crates/data-avalanche-source-pg/src/connect.rs
+++ b/crates/data-avalanche-source-pg/src/connect.rs
@@ -155,9 +155,15 @@ impl Connector {
         f: impl Future<Output = Result<T, tokio_postgres::Error>>,
     ) -> Result<T, SourceError> {
         // The connection `f` runs on: the caller has just taken it from [`Self::client`]. Had
-        // another call replaced it in between, a timeout abandons the newer one.
-        let at_start = self.client.lock().await.clone();
-        match tokio::time::timeout(after, f).await {
+        // another call replaced it in between, a timeout abandons the newer one. The wait for the
+        // cache, which a reconnect holds, is inside the budget (DA-SRC-17).
+        // The guard goes at the end of the arm: `abandon` takes the cache again.
+        let deadline = tokio::time::Instant::now() + after;
+        let at_start = match tokio::time::timeout_at(deadline, self.client.lock()).await {
+            Ok(cache) => cache.clone(),
+            Err(_) => return Err(timed_out(what, after)),
+        };
+        match tokio::time::timeout_at(deadline, f).await {
             Ok(r) => r.map_err(sql_error),
             Err(_) => {
                 if let Some(client) = at_start {
```

`crates/data-avalanche-source-pg/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/connector.rs b/crates/data-avalanche-source-pg/src/connector.rs
index 812b407..6dc60f4 100644
--- a/crates/data-avalanche-source-pg/src/connector.rs
+++ b/crates/data-avalanche-source-pg/src/connector.rs
@@ -118,20 +118,22 @@ impl PgConnector {
                               AND has_table_privilege('pg_catalog.pg_attribute', 'SELECT')
                               AND has_table_privilege('pg_catalog.pg_index', 'SELECT')
                               AND has_table_privilege('pg_catalog.pg_type', 'SELECT')
-                              AND has_table_privilege('pg_catalog.pg_namespace', 'SELECT')
+                              AND has_table_privilege('pg_catalog.pg_namespace', 'SELECT'),
+                            pg_is_in_recovery()
                        FROM pg_catalog.pg_database d, pg_catalog.pg_roles r
                       WHERE d.datname = current_database() AND r.rolname = current_user",
                     &[],
                 ),
             )
             .await?;
-        let (num, version, wal_level, encoding, replication, catalogs): (
+        let (num, version, wal_level, encoding, replication, catalogs, recovery): (
             i32,
             String,
             String,
             String,
             bool,
             bool,
+            bool,
         ) = (
             row.get(0),
             row.get(1),
@@ -139,6 +141,7 @@ impl PgConnector {
             row.get(3),
             row.get(4),
             row.get(5),
+            row.get(6),
         );
         let refuse = |m: String| Err(SourceError::Permanent(m));
         if num < MIN_SERVER_VERSION {
@@ -162,6 +165,11 @@ impl PgConnector {
             );
         }
         let mut warnings = vec![];
+        match standby(num, recovery) {
+            Err(m) => return refuse(m),
+            Ok(Some(w)) => warnings.push(w),
+            Ok(None) => {}
+        }
         if !replication {
             warnings.push(
                 "the user has neither the REPLICATION attribute nor superuser; the replication connection may be refused".into(),
@@ -221,11 +229,9 @@ impl PgConnector {
             .collect();
         let mut report = check_table_set(&self.pipeline, &resolved);
         report.extend(check_unbounded_numeric(&self.pipeline, &unconstrained));
-        if let Some(e) = report.errors.first() {
-            return Err(SourceError::Permanent(format!(
-                "{} {}: {}",
-                e.rule, e.path, e.message
-            )));
+        // Every error, not the first (DA-SRC-17).
+        if let Some(refusal) = report.refusal() {
+            return Err(SourceError::Permanent(refusal));
         }
         out.warnings.extend(
             report
@@ -327,6 +333,25 @@ impl PgConnector {
     }
 }
 
+/// A standby (in recovery) as a source (DA-SRC-17): PostgreSQL decodes logical changes on one
+/// from 16 on; before, it is refused by name, not by the server's error at the slot's creation.
+fn standby(version_num: i32, in_recovery: bool) -> Result<Option<String>, String> {
+    match (in_recovery, version_num >= 160_000) {
+        (false, _) => Ok(None),
+        (true, false) => Err(
+            "the server is a standby (in recovery), and PostgreSQL before 16 can't decode \
+             logical changes on one; point the pipeline at the primary"
+                .into(),
+        ),
+        (true, true) => Ok(Some(
+            "the server is a standby: its slot is invalidated when the primary removes rows the \
+             slot still needs, unless hot_standby_feedback is on with a physical slot on the \
+             primary"
+                .into(),
+        )),
+    }
+}
+
 /// The trait is the inherent methods of the modules: `catalog` and here (channel A), `slot`
 /// (the lifecycle), `snapshot`, `stream`, `schema` (channels C and D).
 #[async_trait]
```

`crates/data-avalanche-source-pg/src/error.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/error.rs b/crates/data-avalanche-source-pg/src/error.rs
index 5260270..1880f9a 100644
--- a/crates/data-avalanche-source-pg/src/error.rs
+++ b/crates/data-avalanche-source-pg/src/error.rs
@@ -94,7 +94,7 @@ fn chain(e: &(dyn std::error::Error + 'static)) -> String {
 /// A catalog-connection error (`tokio-postgres`).
 pub(crate) fn sql_error(e: tokio_postgres::Error) -> SourceError {
     if let Some(db) = e.as_db_error() {
-        let message = format!("{} (SQLSTATE {})", db.message(), db.code().code());
+        let message = db_message(db.message(), db.detail(), db.hint(), db.code().code());
         return classify(Some(db.code().code()), message, None);
     }
     if tls_failure(&e) {
@@ -104,6 +104,19 @@ pub(crate) fn sql_error(e: tokio_postgres::Error) -> SourceError {
     SourceError::Transient(chain(&e))
 }
 
+/// A server error as the pipeline reports it: the message, the server's detail and hint when it
+/// gave them (DA-SRC-17), then the SQLSTATE, last ([`sqlstate`] reads it there).
+fn db_message(message: &str, detail: Option<&str>, hint: Option<&str>, code: &str) -> String {
+    let mut out = message.to_owned();
+    if let Some(d) = detail {
+        out = format!("{out}: {d}");
+    }
+    if let Some(h) = hint {
+        out = format!("{out} (hint: {h})");
+    }
+    format!("{out} (SQLSTATE {code})")
+}
+
 /// A client-side timeout of a catalog or snapshot query.
 pub(crate) fn timed_out(what: &str, after: std::time::Duration) -> SourceError {
     SourceError::Transient(format!("{what} timed out after {after:?}"))
```

`crates/data-avalanche-source-pg/src/value.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/value.rs b/crates/data-avalanche-source-pg/src/value.rs
index 0dda47b..f8f68a8 100644
--- a/crates/data-avalanche-source-pg/src/value.rs
+++ b/crates/data-avalanche-source-pg/src/value.rs
@@ -320,6 +320,11 @@ fn civil_days(date: &str, bc: bool, text: &str) -> Result<i64, ValueError> {
     if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
         return Err(bad("not an ISO date:", text));
     }
+    // PostgreSQL's years end at 5874897 (`date`) and 4713 BC; a longer one is crafted text, and
+    // would overflow the day count (DA-SRC-17).
+    if y > 5_874_897 {
+        return Err(bad("date out of range:", text));
+    }
     // 1 BC is year 0, 2 BC is year -1.
     let y = if bc { 1 - y } else { y };
     Ok(days_from_civil(y, m, d))
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: the same.

Expected: PASS: config 157, source-pg 153, 148 tests on each MySQL target.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/diag.rs \
  crates/data-avalanche-source-pg/src/connect.rs \
  crates/data-avalanche-source-pg/src/connector.rs \
  crates/data-avalanche-source-pg/src/error.rs \
  crates/data-avalanche-source-pg/src/value.rs \
  crates/data-avalanche-source-mysql/src/catalog.rs
git commit -m "fix(source-pg): every table-set error reported, the server's detail and hint kept, a standby refused before 16 and warned from 16, a bounded call's wait for the connection inside its budget, a year past PostgreSQL's range refused"
```

---

### Task 11: The test gaps, and the failover E2E (`source-pg`, `sink-delta`, `pipeline`, the binary's E2E, `test-infra`, CI)

**Files:**
- Create: `test-infra/postgres-primary.sh`, `test-infra/postgres-replica.sh`
- Modify: `crates/data-avalanche-pipeline/src/jobs.rs`, `crates/data-avalanche-source-pg/src/tls.rs`, `.github/workflows/ci.yml`, `doc/v1.0/README.md`, `test-infra/README.md`, `test-infra/compose.yaml`, `test-infra/down.sh`, `test-infra/postgres-tls.sh`
- Test: `crates/data-avalanche-sink-delta/tests/sink/keys.rs` (new), `crates/data-avalanche-sink-delta/tests/sink/main.rs`, `crates/data-avalanche-source-pg/tests/pg/tls.rs`, `crates/data-avalanche/tests/e2e/failover.rs` (new), `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: everything above; the E2E harness (M6b–M10).
- Produces:
  - `E2e::on(source: Source, test, ddl, Options) -> E2e`, which `E2e::new` delegates to;
  - the `replica` compose profile (`postgres-primary` on 55420, `postgres-replica` on 55421);
  - `DATA_AVALANCHE_PG_REPLICA`, the test gate (`doc/v1.0/README.md`).

The tests of this task pin behaviour the code already has, so they pass at Step 2: the
verifiers' unit tests, the composite-key test and the jobs test's wait (DA-X-15, a race in the
test, not the code). The failover E2E needs the pair Step 3 adds, and skips until
`DATA_AVALANCHE_PG_REPLICA` names one. The `require` IT reads test-infra's `.tls`, which the
planning's scratch workspace doesn't have: it was not run while planning. It is written to what
`pgwire-replication` 0.4.1's `Require` does (no verification, read in its `src/tls/rustls.rs`)
and runs first at Step 2, from the checkout. A failure there is a finding for the task.

- [ ] **Step 1: Write the tests**

`crates/data-avalanche-pipeline/src/jobs.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/jobs.rs b/crates/data-avalanche-pipeline/src/jobs.rs
index d476fee..9e0f675 100644
--- a/crates/data-avalanche-pipeline/src/jobs.rs
+++ b/crates/data-avalanche-pipeline/src/jobs.rs
@@ -410,6 +410,8 @@ mod tests {
             wait_until("the copy lands", || w.ids("x") == [1, 2, 3]).await;
             wait_until("the stream moves on", || w.ids("a") == [1]).await;
             streamed(&status, &w).await;
+            // Its rows land before its last chunk read (an empty one) marks it done.
+            wait_until("the copy is done", || status.borrow().snapshots.is_empty()).await;
         })
         .await;
         end.unwrap();
```

`crates/data-avalanche-sink-delta/tests/sink/keys.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/keys.rs b/crates/data-avalanche-sink-delta/tests/sink/keys.rs
new file mode 100644
index 0000000..e6bd7cd
--- /dev/null
+++ b/crates/data-avalanche-sink-delta/tests/sink/keys.rs
@@ -0,0 +1,143 @@
+//! A composite key with a `date` part, end to end (DA-SRC-18): the MERGE matches on every key
+//! column, so an upsert updates only its own `(region, day)` and a delete removes only its own.
+
+use std::sync::Arc;
+
+use arrow_array::cast::AsArray;
+use arrow_array::types::Date32Type;
+use arrow_array::{Array, ArrayRef, Date32Array, RecordBatch, StringArray};
+use data_avalanche_core::{
+    ColumnSchema, ColumnValueType, PositionRange, Sink, TableChangeSet, TableRef, TableSchema,
+    TableSpec, delete_schema, upsert_schema,
+};
+use data_avalanche_sink_delta::{DeltaLocation, TableDefaults};
+
+use crate::common::{commit, lsn, sink};
+
+fn orders() -> TableRef {
+    TableRef::new("public", "orders")
+}
+
+fn schema() -> TableSchema {
+    TableSchema::new(
+        orders(),
+        vec![
+            ColumnSchema::new("region", ColumnValueType::Text, "text").not_null(),
+            ColumnSchema::new("day", ColumnValueType::Date, "date").not_null(),
+            ColumnSchema::new("doc", ColumnValueType::Text, "text"),
+        ],
+        vec!["region".into(), "day".into()],
+    )
+    .unwrap()
+}
+
+type Row<'a> = (&'a str, i32, &'a str);
+
+/// Upsert rows of `orders`, lineage filled in.
+fn upserts(rows: &[Row]) -> RecordBatch {
+    let n = rows.len();
+    let text = |v: &str| Arc::new(StringArray::from(vec![v; n])) as ArrayRef;
+    let columns: Vec<ArrayRef> = vec![
+        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
+        Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.1))),
+        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
+        text("U"),
+        Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![0; n]).with_timezone("UTC")),
+        text("0/1"),
+        text("00000000/00000001"),
+        text("h/db"),
+        text("p"),
+    ];
+    RecordBatch::try_new(upsert_schema(&schema()), columns).unwrap()
+}
+
+fn deletes(keys: &[(&str, i32)]) -> RecordBatch {
+    RecordBatch::try_new(
+        delete_schema(&schema(), &[]).unwrap(),
+        vec![
+            Arc::new(StringArray::from_iter_values(keys.iter().map(|k| k.0))),
+            Arc::new(Date32Array::from_iter_values(keys.iter().map(|k| k.1))),
+        ],
+    )
+    .unwrap()
+}
+
+fn cs(end: u64, up: Vec<RecordBatch>, del: Vec<RecordBatch>) -> TableChangeSet {
+    TableChangeSet {
+        table: orders(),
+        schema: upsert_schema(&schema()),
+        identifier: vec!["region".into(), "day".into()],
+        truncate: false,
+        upserts: up,
+        deletes: del,
+        position: PositionRange {
+            start: lsn(0),
+            end: lsn(end),
+            last_change: lsn(end),
+        },
+        split: None,
+        snapshot: false,
+        reposition: false,
+    }
+}
+
+/// `(region, day, doc)` of every row, sorted.
+async fn rows(s: &data_avalanche_sink_delta::DeltaSink) -> Vec<(String, i32, String)> {
+    let b = s.content(&orders()).await.unwrap().unwrap();
+    let (r, d, doc) = (
+        b.column(0).as_string::<i32>(),
+        b.column(1).as_primitive::<Date32Type>(),
+        b.column(2).as_string::<i32>(),
+    );
+    let mut out: Vec<_> = (0..b.num_rows())
+        .map(|i| {
+            assert!(doc.is_valid(i));
+            (r.value(i).to_owned(), d.value(i), doc.value(i).to_owned())
+        })
+        .collect();
+    out.sort();
+    out
+}
+
+#[tokio::test]
+async fn a_composite_date_key_matches_on_every_part() {
+    let dir = tempfile::tempdir().unwrap();
+    let s = sink(
+        DeltaLocation::Local {
+            root: dir.path().to_path_buf(),
+        },
+        TableDefaults::default(),
+    );
+    s.ensure_table(&TableSpec::new(&schema(), vec![]).unwrap())
+        .await
+        .unwrap();
+    // 2026-10-30 is day 20_756 of the epoch.
+    let first = upserts(&[
+        ("eu", 20_756, "a"),
+        ("eu", 20_757, "b"),
+        ("us", 20_756, "c"),
+    ]);
+    commit(&s, cs(10, vec![first], vec![]), 1).await;
+    // Each key shares one part with another row: only the full match counts.
+    let up = upserts(&[("eu", 20_757, "B"), ("us", 20_757, "d")]);
+    commit(&s, cs(20, vec![up], vec![deletes(&[("us", 20_756)])]), 1).await;
+    let owned = |r: &[Row]| -> Vec<(String, i32, String)> {
+        r.iter()
+            .map(|(a, b, c)| ((*a).to_owned(), *b, (*c).to_owned()))
+            .collect()
+    };
+    assert_eq!(
+        rows(&s).await,
+        owned(&[
+            ("eu", 20_756, "a"),
+            ("eu", 20_757, "B"),
+            ("us", 20_757, "d"),
+        ])
+    );
+    // The key survives the round trip as a Delta `date`, not a string.
+    let b = s.content(&orders()).await.unwrap().unwrap();
+    assert_eq!(
+        b.schema().field(1).data_type(),
+        &arrow_schema::DataType::Date32
+    );
+}
```

`crates/data-avalanche-sink-delta/tests/sink/main.rs`:

```diff
diff --git a/crates/data-avalanche-sink-delta/tests/sink/main.rs b/crates/data-avalanche-sink-delta/tests/sink/main.rs
index da63833..1de4c36 100644
--- a/crates/data-avalanche-sink-delta/tests/sink/main.rs
+++ b/crates/data-avalanche-sink-delta/tests/sink/main.rs
@@ -4,6 +4,7 @@
 
 mod common;
 mod conformance;
+mod keys;
 mod local;
 mod scenarios;
 mod stores;
```

`crates/data-avalanche-source-pg/src/tls.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/tls.rs b/crates/data-avalanche-source-pg/src/tls.rs
index 2c2e124..b330e03 100644
--- a/crates/data-avalanche-source-pg/src/tls.rs
+++ b/crates/data-avalanche-source-pg/src/tls.rs
@@ -382,6 +382,90 @@ mod tests {
         control_tls(&no_roots).unwrap();
     }
 
+    /// A throwaway CA named `cn`, self-signed.
+    fn ca(cn: &str) -> (rcgen::Certificate, rcgen::Issuer<'static, rcgen::KeyPair>) {
+        use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
+        let key = KeyPair::generate().unwrap();
+        let mut params = CertificateParams::new(vec![]).unwrap();
+        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
+        params.distinguished_name.push(DnType::CommonName, cn);
+        let cert = params.self_signed(&key).unwrap();
+        (cert, Issuer::new(params, key))
+    }
+
+    /// A certificate for `localhost` that `issuer` signed.
+    fn localhost(issuer: &rcgen::Issuer<'_, rcgen::KeyPair>) -> CertificateDer<'static> {
+        let key = rcgen::KeyPair::generate().unwrap();
+        rcgen::CertificateParams::new(vec!["localhost".into()])
+            .unwrap()
+            .signed_by(&key, issuer)
+            .unwrap()
+            .der()
+            .clone()
+    }
+
+    /// A server certificate for `localhost` signed by a throwaway CA, the CA's roots, and a
+    /// certificate another CA signed.
+    fn server_material() -> (
+        CertificateDer<'static>,
+        Arc<RootCertStore>,
+        CertificateDer<'static>,
+    ) {
+        let (ca_cert, issuer) = ca("data-avalanche test CA");
+        let (_, other) = ca("another CA");
+        let mut roots = RootCertStore::empty();
+        roots.add(ca_cert.der().clone()).unwrap();
+        (localhost(&issuer), Arc::new(roots), localhost(&other))
+    }
+
+    fn verify(
+        v: &dyn ServerCertVerifier,
+        cert: &CertificateDer<'_>,
+        name: &str,
+    ) -> Result<ServerCertVerified, rustls::Error> {
+        let name = ServerName::try_from(name.to_owned()).unwrap();
+        v.verify_server_cert(cert, &[], &name, &[], UnixTime::now())
+    }
+
+    /// `verify-ca` takes a certificate the CA signed whatever the name it is reached by, and
+    /// refuses one no root signed (DA-SRC-18).
+    #[test]
+    fn chain_only_forgives_the_name_but_not_the_chain() {
+        let (server, roots, stranger) = server_material();
+        let inner = WebPkiServerVerifier::builder_with_provider(roots, provider())
+            .build()
+            .unwrap();
+        let v = ChainOnly { inner };
+        for name in ["localhost", "127.0.0.1", "db.example.com"] {
+            verify(&v, &server, name).unwrap_or_else(|e| panic!("{name}: {e}"));
+        }
+        let e = verify(&v, &stranger, "localhost").unwrap_err();
+        assert_eq!(
+            e,
+            rustls::Error::InvalidCertificate(CertificateError::UnknownIssuer)
+        );
+        assert!(!v.supported_verify_schemes().is_empty());
+    }
+
+    /// `require`, `prefer` and `allow` take any certificate by any name, as libpq does without
+    /// a root certificate, and still check the handshake's signature schemes (DA-SRC-18).
+    #[test]
+    fn unverified_takes_any_certificate() {
+        let (server, _, stranger) = server_material();
+        let v = Unverified {
+            algorithms: provider().signature_verification_algorithms,
+        };
+        for cert in [&server, &stranger] {
+            verify(&v, cert, "127.0.0.1").unwrap();
+        }
+        assert_eq!(
+            v.supported_verify_schemes(),
+            provider()
+                .signature_verification_algorithms
+                .supported_schemes()
+        );
+    }
+
     #[test]
     fn bad_material_is_refused_by_name() {
         let (ca, cert, _) = material();
```

`crates/data-avalanche-source-pg/tests/pg/tls.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/tls.rs b/crates/data-avalanche-source-pg/tests/pg/tls.rs
index 46f2718..8ae15eb 100644
--- a/crates/data-avalanche-source-pg/tests/pg/tls.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/tls.rs
@@ -158,6 +158,22 @@ async fn verify_full_with_an_inline_ca_encrypts_both_connections() {
     cleanup(&a, &n).await;
 }
 
+/// `require` encrypts both connections without checking the certificate, as libpq does: no CA
+/// given, and an address the certificate doesn't name (DA-SRC-18).
+#[tokio::test]
+async fn require_encrypts_both_connections_without_verifying() {
+    require_it!();
+    let _serial = SERIAL.lock().await;
+    let a = admin().await;
+    let n = fresh(&a, "tls_require", "CREATE TABLE t (id int PRIMARY KEY);").await;
+    let p = pipeline(&n, "127.0.0.1", "avalanche", "ssl_mode: require", &[]);
+    let seen = stream_one(&a, &p, &n).await;
+    let kinds: Vec<&str> = seen.iter().map(|s| s.1.as_str()).collect();
+    assert_eq!(kinds, ["client backend", "client backend", "walsender"]);
+    assert!(seen.iter().all(|s| s.2), "both over TLS: {seen:?}");
+    cleanup(&a, &n).await;
+}
+
 #[tokio::test]
 async fn a_client_certificate_with_an_inline_key_authenticates_both_connections() {
     require_it!();
```

`crates/data-avalanche/tests/e2e/failover.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/failover.rs b/crates/data-avalanche/tests/e2e/failover.rs
new file mode 100644
index 0000000..2f61a15
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/failover.rs
@@ -0,0 +1,199 @@
+//! A PostgreSQL failover (DA-SRC-20) on test-infra's `replica` pair, named by
+//! `DATA_AVALANCHE_PG_REPLICA=<primary port>:<replica port>`: the pipeline reads the primary, the
+//! replica is cut off and promoted behind it, and the pipeline is moved to it. The promoted
+//! server's LSNs repeat ones the lake has marked, so the start halts with `WAL_SOURCE_CHANGED`;
+//! under `source_change_action: re_snapshot` it re-snapshots from the new server and streams on
+//! past the old markers.
+
+use std::time::Duration;
+
+use data_avalanche_core::SourcePosition;
+use data_avalanche_core::position::Lsn;
+use data_avalanche_testkit::require_it;
+
+use crate::harness::{E2e, Options, eventually};
+use crate::source::{Db, Source};
+
+const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";
+
+/// The primary's and the replica's ports, or `None` (the scenario skipped) without the variable.
+fn pair() -> Option<(u16, u16)> {
+    let v = std::env::var("DATA_AVALANCHE_PG_REPLICA").ok()?;
+    let (primary, replica) = v
+        .split_once(':')
+        .and_then(|(a, b)| Some((a.parse().ok()?, b.parse().ok()?)))
+        .unwrap_or_else(|| panic!("DATA_AVALANCHE_PG_REPLICA={v}: <primary port>:<replica port>"));
+    Some((primary, replica))
+}
+
+/// Whether the log has the line of a pipeline in `ERROR` with `code` (Q-M7b-4).
+fn halted_with(log: &str, code: &str) -> bool {
+    log.lines()
+        .any(|l| l.contains("pipeline in ERROR") && l.contains(&format!("code=\"{code}\"")))
+}
+
+/// The keys of `items` on `db`, in order.
+async fn ids(db: &Db, schema: &str) -> Vec<i64> {
+    db.text(&format!("SELECT id FROM {schema}.items ORDER BY id"))
+        .await
+        .expect("the ids")
+        .into_iter()
+        .map(|r| r[0].as_deref().expect("an id").parse().expect("a number"))
+        .collect()
+}
+
+/// The lake holds `want`, within a minute.
+async fn holds(e: &E2e, want: &[i64]) {
+    let what = format!("the lake holds {want:?}");
+    eventually(&what, Duration::from_secs(60), async || {
+        e.ids("items").await == want
+    })
+    .await;
+}
+
+#[tokio::test]
+async fn a_promoted_replica_halts_the_pipeline_then_re_snapshots_on_request() {
+    require_it!();
+    crate::postgres_only!();
+    let Some((primary, replica)) = pair() else {
+        eprintln!("skipped: DATA_AVALANCHE_PG_REPLICA unset (test-infra's replica profile)");
+        return;
+    };
+    let standby = Db::connect(Source::Postgres { port: replica }).await;
+    if standby
+        .text("SELECT pg_is_in_recovery()")
+        .await
+        .expect("recovery")[0][0]
+        .as_deref()
+        != Some("t")
+    {
+        panic!(
+            "postgres-replica isn't a standby (an earlier run promoted it): recreate the pair with \
+             docker compose -f test-infra/compose.yaml --profile replica up -d --wait \
+             --force-recreate postgres-primary postgres-replica"
+        );
+    }
+    let e = E2e::on(
+        Source::Postgres { port: primary },
+        "pg_failover",
+        &format!("{ITEMS} INSERT INTO items SELECT g, 'p' FROM generate_series(1, 5) g;"),
+        Options {
+            tables: ", snapshots: { mode: initial }",
+            ..Options::default()
+        },
+    )
+    .await;
+    let s = e.schema.clone();
+    let mut p = e.start(&[]);
+    p.ready().await;
+    holds(&e, &[1, 2, 3, 4, 5]).await;
+    e.streaming().await;
+    eventually(
+        "the replica has 1..5",
+        Duration::from_secs(30),
+        async || ids(&standby, &s).await.len() == 5,
+    )
+    .await;
+
+    // The replica stops receiving WAL. The primary goes on, a WAL segment further (the lake's
+    // markers well past anything the promoted replica writes next).
+    for sql in [
+        "ALTER SYSTEM SET primary_conninfo = ''",
+        "SELECT pg_reload_conf()",
+    ] {
+        standby.exec(sql).await.expect(sql);
+    }
+    eventually(
+        "the replica's WAL receiver stops",
+        Duration::from_secs(30),
+        async || !standby.any("SELECT 1 FROM pg_stat_wal_receiver").await,
+    )
+    .await;
+    e.exec("INSERT INTO items SELECT g, 'p' FROM generate_series(6, 9) g")
+        .await;
+    e.db.exec("SELECT pg_switch_wal()")
+        .await
+        .expect("a WAL switch");
+    e.exec("INSERT INTO items VALUES (10, 'p')").await;
+    holds(&e, &(1..=10).collect::<Vec<_>>()).await;
+    let marker = e.marker("items").await;
+
+    // The failover: the replica, promoted, takes writes on a timeline of its own.
+    standby
+        .exec("SELECT pg_promote()")
+        .await
+        .expect("the promotion");
+    standby
+        .exec(&format!(
+            "INSERT INTO {s}.items VALUES (11, 'r'), (12, 'r')"
+        ))
+        .await
+        .expect("writes on the promoted replica");
+    let lsn = standby
+        .text("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint")
+        .await
+        .expect("the LSN")[0][0]
+        .as_deref()
+        .expect("an LSN")
+        .parse::<u64>()
+        .expect("a number");
+    let SourcePosition::Pg(Lsn(marked)) = marker else {
+        panic!("a PostgreSQL marker: {marker}")
+    };
+    assert!(
+        lsn < marked,
+        "the promoted replica's LSN {lsn} is below the lake's marker {marked}: the guard would skip its changes"
+    );
+
+    // The pipeline moved to the promoted replica (a source change restarts it): its start
+    // finds another server than the checkpoint's and halts.
+    let first = e.config();
+    let moved = first.replacen(
+        &format!("      port: {primary}\n"),
+        &format!("      port: {replica}\n"),
+        1,
+    );
+    assert_ne!(moved, first);
+    e.write_config(&moved);
+    assert_eq!(p.reload().await.0, 202);
+    p.metric(&e.status("error")).await;
+    let log = p.log();
+    assert!(halted_with(&log, "WAL_SOURCE_CHANGED"), "{log}");
+    assert_eq!(e.ids("items").await, (1..=10).collect::<Vec<_>>());
+
+    // Asked to re-snapshot: the table is copied again from the promoted replica, and a change
+    // written there after it, at an LSN below the old marker, streams in.
+    let snap = moved.replacen(
+        &format!("slot: {s}"),
+        &format!("slot: {s}, source_change_action: re_snapshot"),
+        1,
+    );
+    assert_ne!(snap, moved);
+    e.write_config(&snap);
+    assert_eq!(p.reload().await.0, 202);
+    assert_eq!(p.ask(&e.test, "restart").await.0, 202);
+    p.metric(&e.status("running")).await;
+    // Rows 6–10 are ghosts: the promoted server never had them, and a snapshot adds and
+    // updates rows but deletes none.
+    holds(&e, &(1..=12).collect::<Vec<_>>()).await;
+    standby
+        .exec(&format!("INSERT INTO {s}.items VALUES (13, 'r')"))
+        .await
+        .expect("a write after the recovery");
+    holds(&e, &(1..=13).collect::<Vec<_>>()).await;
+    let log = p.log();
+    assert!(
+        log.contains("source_change_action is re_snapshot, so the pipeline re-snapshots"),
+        "{log}"
+    );
+    assert!(p.terminate().await.success());
+    standby
+        .exec(&format!(
+            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
+              WHERE slot_name = '{s}' AND NOT active;
+             DROP PUBLICATION IF EXISTS {s}_pub; DROP SCHEMA IF EXISTS {s} CASCADE;"
+        ))
+        .await
+        .expect("the promoted replica's copy dropped");
+    e.cleanup().await;
+}
```

`crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index b19a742..0bc64ce 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -129,7 +129,11 @@ impl E2e {
     /// or database and user (MySQL), runs `ddl` in it, and writes the config. On PostgreSQL it
     /// resets the server-wide `max_slot_wal_keep_size` first.
     pub async fn new(test: &str, ddl: &str, o: Options<'_>) -> Self {
-        let source = Source::from_env();
+        Self::on(Source::from_env(), test, ddl, o).await
+    }
+
+    /// [`Self::new`] on `source` rather than the suite's.
+    pub async fn on(source: Source, test: &str, ddl: &str, o: Options<'_>) -> Self {
         let db = Db::connect(source).await;
         let schema = format!("e2e_{test}");
         match source {
```

`crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index 85a1371..fbd890c 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -9,6 +9,7 @@
 mod api;
 mod cluster;
 mod crashes;
+mod failover;
 mod forwarder;
 mod guards;
 mod harness;
```


- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-sink-delta -p data-avalanche-pipeline`, `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg -- --test-threads=1` (from the checkout, for `tls::`) and `DATA_AVALANCHE_IT=1 cargo test -p data-avalanche --features e2e-hooks --test e2e a_promoted_replica`

Expected: PASS: sink-delta 90, pipeline 256, source-pg 153 besides `tls::` (measured with
`--skip tls::`; the `tls::` ITs, `require_encrypts_both_connections_without_verifying` among
them, run here first). The failover E2E prints `skipped: DATA_AVALANCHE_PG_REPLICA unset` and
passes.

- [ ] **Step 3: The replica pair, its wiring in CI, and the `postgres-tls` alignment**

`.github/workflows/ci.yml`:

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index 15bf416..3533fcd 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -122,12 +122,17 @@ jobs:
           for target in mysql-84-nogtid mariadb-118 mariadb-118-binlog; do
             DATA_AVALANCHE_MYSQL=$target cargo test --locked -p data-avalanche-source-mysql --tests
           done
+      # The PostgreSQL failover scenario's primary and standby (profile replica, M12a).
+      - name: Start the replica pair
+        run: docker compose -f test-infra/compose.yaml --profile replica up -d --wait postgres-primary postgres-replica
       # The slice-1 E2E suite (roadmap §5.3): the binary as a process, built with its crash points.
       - name: Slice-1 E2E
+        env:
+          DATA_AVALANCHE_PG_REPLICA: "55420:55421"
         run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
       - name: Container logs on failure
         if: failure()
-        run: docker compose -f test-infra/compose.yaml logs --no-color
+        run: docker compose -f test-infra/compose.yaml --profile replica logs --no-color
       - name: Stop test infrastructure
         if: always()
         run: test-infra/down.sh
```

`doc/v1.0/README.md`:

```diff
diff --git a/doc/v1.0/README.md b/doc/v1.0/README.md
index 9f7f887..8017485 100644
--- a/doc/v1.0/README.md
+++ b/doc/v1.0/README.md
@@ -60,6 +60,7 @@ This folder holds the plan for **building** v1. The design it implements is in `
 | `DATA_AVALANCHE_CLOUD=s3\|azure` (from M2) | Real S3 / Azure conformance runs (`cloud.yml`) |
 | `DATA_AVALANCHE_MYSQL=<target>` (from M7a) | The MySQL or MariaDB server and position mode of the MySQL source's integration tests, and of the E2E suite on MySQL (`test-infra/README.md`; default `mysql-84`) |
 | `DATA_AVALANCHE_E2E_SOURCE=postgres\|mysql` (from M7b) | The source the slice-1 E2E suite runs on (default `postgres`; `mysql` reads `DATA_AVALANCHE_MYSQL`) |
+| `DATA_AVALANCHE_PG_REPLICA=<primary port>:<replica port>` (from M12a) | The PostgreSQL failover E2E scenario on `test-infra`'s `replica` profile (`55420:55421`); unset, it is skipped |
 
 CI secrets and variables are listed in `test-infra/README.md` ("CI requirements") and in the comments
 of `.github/workflows/ci.yml`.
```

`test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
index 06a9e8e..7fe4a76 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -11,6 +11,7 @@ on `PATH`; it fetches DuckDB itself.
 | postgres-18 | postgres:18 | 55418 | avalanche / avalanche, db `avalanche` | default |
 | postgres-14…17 | postgres:14…17 | 55414…55417 | same | matrix |
 | postgres-tls | postgres:18, TLS required (`postgres-tls.sh`) | 55419 | avalanche / avalanche; `avalanche_cert` by client certificate | default |
+| postgres-primary, postgres-replica | postgres:18, a primary and its streaming standby (`postgres-primary.sh`, `postgres-replica.sh`), on tmpfs | 55420, 55421 | avalanche / avalanche | replica |
 | mysql-84 | mysql:8.4 (GTID on) | 53384 | root / avalanche, db `avalanche` | default |
 | mysql-84-nogtid | mysql:8.4 (GTID off: `replication_type: binlog`; TLS off) | 53385 | same | default |
 | mysql-tls | mysql:8.4 (GTID on), TLS required (`mysql-tls.sh`) | 53386 | root / avalanche; `avalanche_cert` / `unused` with its client certificate | default |
@@ -110,6 +111,22 @@ through a node that doesn't hold it, and leaves and shuts a node down through th
 snapshot-job scenarios (M9b, `jobs.rs`) need nothing more either: they ask for jobs through the
 API, drop and create a table again to halt it, and `kill -9` the process while a job reads.
 
+## The PostgreSQL failover (replica)
+
+The failover scenario (`crates/data-avalanche/tests/e2e/failover.rs`, M12a) runs a pipeline on
+`postgres-primary`, cuts `postgres-replica` off from it, writes on, promotes the replica and moves
+the pipeline to it: the start halts with `WAL_SOURCE_CHANGED`, and under
+`source_change_action: re_snapshot` it re-snapshots from the promoted server. The replica is made
+from the primary's base backup at its first start. The scenario promotes it, so the pair serves
+one run: recreate both before the next (the scenario fails with this hint when the replica isn't
+in recovery).
+
+```sh
+docker compose -f test-infra/compose.yaml --profile replica up -d --wait --force-recreate postgres-primary postgres-replica
+DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_PG_REPLICA=55420:55421 \
+  cargo test -p data-avalanche --features e2e-hooks --test e2e -- failover:: --test-threads=1
+```
+
 ## The smoke deploy (k3s)
 
 The Helm chart's scenario (`crates/data-avalanche/tests/e2e/k3s.rs`, M11a) installs the chart
@@ -128,7 +145,7 @@ DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_K3S=1 \
 
 The kubelet evicts only under 1% free disk (`compose.yaml`): a laptop's Docker disk is often
 fuller than the default thresholds (10–15%) allow, which taints the node and schedules nothing.
-`test-infra/down.sh` stops k3s with the rest.
+`test-infra/down.sh` stops k3s and the replica pair with the rest.
 
 ## GCS: a real bucket, not an emulator
 
```

`test-infra/compose.yaml`:

```diff
diff --git a/test-infra/compose.yaml b/test-infra/compose.yaml
index 8a97cfa..df68f03 100644
--- a/test-infra/compose.yaml
+++ b/test-infra/compose.yaml
@@ -1,6 +1,7 @@
 # Integration-test infrastructure. Default profile = newest of each engine + S3/Azure emulators.
 # GCS has no usable emulator for object_store; GCS tests use a real bucket (see README.md).
 # `matrix` profile adds the older supported versions (P-8). `init` profile = one-shot bucket creation.
+# `replica` profile = a PostgreSQL primary and its standby (the failover E2E).
 # Host ports are fixed and non-default; see README.md.
 name: data-avalanche-test
 
@@ -22,14 +23,34 @@ services:
   postgres-16: { <<: *pg, image: "postgres:16", ports: ["55416:5432"], profiles: [matrix] }
   postgres-15: { <<: *pg, image: "postgres:15", ports: ["55415:5432"], profiles: [matrix] }
   postgres-14: { <<: *pg, image: "postgres:14", ports: ["55414:5432"], profiles: [matrix] }
-  # TLS required on every TCP connection, throwaway certificates made at start (postgres-tls.sh).
+  # TLS required on every TCP connection, throwaway certificates made at start (postgres-tls.sh,
+  # which runs the anchor's command with the TLS settings added).
   postgres-tls:
+    <<: *pg
     image: "postgres:18"
-    environment: { POSTGRES_USER: avalanche, POSTGRES_PASSWORD: avalanche, POSTGRES_DB: avalanche }
     entrypoint: ["bash", "/postgres-tls.sh"]
     volumes: ["./postgres-tls.sh:/postgres-tls.sh:ro", "./.tls:/tls"]
     ports: ["55419:5432"]
-    healthcheck: { test: ["CMD-SHELL", "pg_isready -U avalanche -d avalanche"], interval: 2s, retries: 30 }
+  # A primary and its streaming standby (profile replica), for the failover E2E
+  # (crates/data-avalanche/tests/e2e/failover.rs): the test promotes the standby, so the pair is
+  # good for one run. Both on tmpfs; recreate both for the next run:
+  #   docker compose --profile replica up -d --force-recreate postgres-primary postgres-replica
+  postgres-primary:
+    <<: *pg
+    image: "postgres:18"
+    profiles: [replica]
+    volumes: ["./postgres-primary.sh:/docker-entrypoint-initdb.d/replication.sh:ro"]
+    tmpfs: [/var/lib/postgresql]
+    ports: ["55420:5432"]
+  postgres-replica:
+    <<: *pg
+    image: "postgres:18"
+    profiles: [replica]
+    entrypoint: ["bash", "/postgres-replica.sh"]
+    volumes: ["./postgres-replica.sh:/postgres-replica.sh:ro"]
+    tmpfs: [/var/lib/postgresql]
+    ports: ["55421:5432"]
+    depends_on: { postgres-primary: { condition: service_healthy } }
 
   mysql-84:
     image: "mysql:8.4"
```

`test-infra/down.sh`:

```diff
diff --git a/test-infra/down.sh b/test-infra/down.sh
index 018f784..fb65c09 100755
--- a/test-infra/down.sh
+++ b/test-infra/down.sh
@@ -2,4 +2,4 @@
 # Stop everything and drop volumes (all profiles).
 set -euo pipefail
 cd "$(dirname "$0")"
-docker compose --profile matrix --profile init --profile k3s down -v --remove-orphans
+docker compose --profile matrix --profile init --profile k3s --profile replica down -v --remove-orphans
```

`test-infra/postgres-primary.sh`:

```diff
diff --git a/test-infra/postgres-primary.sh b/test-infra/postgres-primary.sh
new file mode 100755
index 0000000..403256f
--- /dev/null
+++ b/test-infra/postgres-primary.sh
@@ -0,0 +1,5 @@
+#!/usr/bin/env bash
+# An init script of `postgres-primary` (run once, by initdb): the image's pg_hba admits no
+# replication connection over TCP, which `postgres-replica`'s base backup and WAL stream are.
+set -euo pipefail
+echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
```

`test-infra/postgres-replica.sh`:

```diff
diff --git a/test-infra/postgres-replica.sh b/test-infra/postgres-replica.sh
new file mode 100644
index 0000000..7850a0c
--- /dev/null
+++ b/test-infra/postgres-replica.sh
@@ -0,0 +1,16 @@
+#!/usr/bin/env bash
+# Entrypoint of `postgres-replica`: a streaming standby of `postgres-primary`, made from its base
+# backup at the first start (-R: standby.signal and primary_conninfo), then PostgreSQL with the
+# compose anchor's command ("$@"). Its configuration and pg_hba come with the backup.
+set -euo pipefail
+export PGPASSWORD=avalanche
+data=/var/lib/postgresql/standby
+until pg_isready -q -h postgres-primary -U avalanche -d avalanche; do sleep 1; done
+mkdir -p "$data"
+chown postgres:postgres "$data"
+chmod 700 "$data"
+if [ ! -s "$data/PG_VERSION" ]; then
+  gosu postgres pg_basebackup -h postgres-primary -U avalanche -D "$data" -R -X stream -c fast
+fi
+shift
+exec gosu postgres postgres -D "$data" "$@"
```

`test-infra/postgres-tls.sh`:

```diff
diff --git a/test-infra/postgres-tls.sh b/test-infra/postgres-tls.sh
index cb5f143..55e69d6 100644
--- a/test-infra/postgres-tls.sh
+++ b/test-infra/postgres-tls.sh
@@ -35,7 +35,7 @@ HBA
 mkdir -p /docker-entrypoint-initdb.d
 echo "CREATE ROLE avalanche_cert SUPERUSER LOGIN REPLICATION PASSWORD 'unused';" \
   > /docker-entrypoint-initdb.d/cert-role.sql
-exec docker-entrypoint.sh postgres -c wal_level=logical -c max_replication_slots=20 \
-  -c max_wal_senders=20 -c ssl=on -c ssl_cert_file="$server/server.crt" \
+# "$@": the compose anchor's command (`postgres` and the settings every PostgreSQL service shares).
+exec docker-entrypoint.sh "$@" -c ssl=on -c ssl_cert_file="$server/server.crt" \
   -c ssl_key_file="$server/server.key" -c ssl_ca_file="$server/ca.crt" \
   -c hba_file="$server/pg_hba.conf"
```


- [ ] **Step 4: Run the tests to verify they pass**

Run: `docker compose -f test-infra/compose.yaml --profile replica up -d --wait postgres-primary postgres-replica`, then `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_PG_REPLICA=55420:55421 cargo test -p data-avalanche --features e2e-hooks --test e2e a_promoted_replica`, and the three packages again

Expected: PASS: `a_promoted_replica_halts_the_pipeline_then_re_snapshots_on_request` (20.7 s on a
fresh pair); sink-delta 90, pipeline 256, source-pg 153. A second run fails on the promoted pair,
with the hint to recreate it.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline/src/jobs.rs \
  crates/data-avalanche-sink-delta/tests/sink/keys.rs \
  crates/data-avalanche-sink-delta/tests/sink/main.rs \
  crates/data-avalanche-source-pg/src/tls.rs \
  crates/data-avalanche-source-pg/tests/pg/tls.rs \
  crates/data-avalanche/tests/e2e/failover.rs \
  crates/data-avalanche/tests/e2e/harness.rs \
  crates/data-avalanche/tests/e2e/main.rs \
  .github/workflows/ci.yml \
  doc/v1.0/README.md \
  test-infra/README.md \
  test-infra/compose.yaml \
  test-infra/down.sh \
  test-infra/postgres-primary.sh \
  test-infra/postgres-replica.sh \
  test-infra/postgres-tls.sh
git commit -m "test(source-pg): DA-SRC-18's gaps — the verifiers' unit tests, a require IT, a composite date key end to end, postgres-tls on the x-pg anchor; a primary/standby pair in test-infra (profile replica) and the failover E2E on it, in CI; the jobs test waits for its copy's completion (DA-X-15)"
```

---

### Task 12: The docs and the tracker

**Files:**
- Modify: `doc/analysis/CDC_Design_Gap_Analysis_v1.md`, `docs/src/content/docs/configuration/sources.md`, `docs/src/content/docs/runbooks/position-lost.mdx`, `docs/src/content/docs/runbooks/source-privileges.mdx`

- [ ] **Step 1: The operator pages**

The sources guide says how a failover, a standby and the publication's checks behave. The
lost-position runbook gives the recoveries: a failover (`WAL_SOURCE_CHANGED`), a DBA's slot made
again, a renamed publication, a changed MySQL server. The privileges runbook says what a DBA's
publication must publish, what a lost privilege halts, and what a Galera node needs.

`docs/src/content/docs/configuration/sources.md`:

```diff
diff --git a/docs/src/content/docs/configuration/sources.md b/docs/src/content/docs/configuration/sources.md
index e92c221..427feab 100644
--- a/docs/src/content/docs/configuration/sources.md
+++ b/docs/src/content/docs/configuration/sources.md
@@ -44,10 +44,20 @@ refuses to start without them (and needs `auto_cleanup: false`). The managed pub
 the pipeline's role to own the tables;
 [Source privileges](/data-avalanche/runbooks/source-privileges/) has both set-ups.
 
+**What the publication publishes.** At each start the pipeline checks what the publication
+publishes of its tables. Under `require_existing`, a publication that drops part of a table's
+changes is refused, naming the `ALTER PUBLICATION` that fixes it: a row filter or a column list on
+a replicated table (PostgreSQL 15 and later), or stored generated columns
+(`publish_generated_columns = stored`, PostgreSQL 18). The managed publication is put right
+instead: the pipeline sets its tables (dropping a row filter, a column list or a
+`TABLES IN SCHEMA` member) and `publish_generated_columns = none`, and re-snapshots as it does
+after any change to the publication. Tables the publication publishes but the pipeline doesn't
+replicate are a warning: their changes are decoded and dropped.
+
 **Partitioned tables.** Name the partitions, or a pattern that matches them: each partition
-replicates as its own table. A publication that publishes through the partition root
-(`publish_via_partition_root = true`) is refused, since the partitions' changes would arrive under
-the parent's name.
+replicates as its own table. A publication that publishes a replicated partition only through its
+root (`publish_via_partition_root = true`) is refused, since the partition's changes would arrive
+under the parent's name.
 
 **Large values.** PostgreSQL leaves an unchanged TOASTed value out of an update. With
 `toast_strategy: carry_forward` (the default) the pipeline keeps the value it wrote last;
@@ -61,6 +71,19 @@ it is invalidated, and the pipeline then starts over with a new slot and a snaps
 `snapshots.mode` says, none under `never` (see
 [A lost position](/data-avalanche/runbooks/position-lost/)).
 
+**A failover.** The checkpoint records the server it was read on: its system identifier and its
+timeline. A promoted replica keeps the identifier but starts a timeline of its own, and its LSNs
+can repeat ones the lake has already marked. So a start on another server halts the pipeline with
+`WAL_SOURCE_CHANGED` (`source.postgres.source_change_action: error`, the default). With
+`re_snapshot` the pipeline moves its tables onto the new server's position and copies them as
+`snapshots.mode` says; see
+[A lost position](/data-avalanche/runbooks/position-lost/#a-failover-wal_source_changed).
+
+**A standby.** Point the pipeline at the primary. PostgreSQL before 16 can't decode on a standby,
+so one is refused. From 16 a standby works, with a warning: the primary may remove rows the slot
+still needs, which invalidates it, unless `hot_standby_feedback` is on with a physical slot on the
+primary.
+
 ## MySQL
 
 Tested on MySQL 8.4.
```

`docs/src/content/docs/runbooks/position-lost.mdx`:

```diff
diff --git a/docs/src/content/docs/runbooks/position-lost.mdx b/docs/src/content/docs/runbooks/position-lost.mdx
index 1c864ee..d144b20 100644
--- a/docs/src/content/docs/runbooks/position-lost.mdx
+++ b/docs/src/content/docs/runbooks/position-lost.mdx
@@ -1,6 +1,6 @@
 ---
 title: A lost position
-description: A PostgreSQL slot lost, invalidated or orphaned, a WAL backlog, a purged binlog, a changed binlog server.
+description: A PostgreSQL slot lost, invalidated, made again or orphaned, a failover, a renamed publication, a WAL backlog, a purged binlog, a changed binlog server.
 sidebar:
   order: 3
 ---
@@ -50,16 +50,44 @@ hand, or advanced past the pipeline's checkpoint.
 - With `slot_management: managed` (the default) the pipeline creates the slot again at its next
   start and streams from the new slot. It copies the tables as `snapshots.mode` says: under
   `never` it takes none, and warns. The log says why.
-- With `require_existing` the pipeline stops in `error`, and v1 has no in-place recovery
-  (DA-SRC-30). A slot the DBA creates again is past the pipeline's checkpoint, so it counts as
-  lost and is refused again; the refusal's advice to recreate it and restart is wrong. The path
-  that works:
-  1. Have the DBA drop the slot, if it still exists.
-  2. Set `slot_management: managed` and reload. That restarts the pipeline, even one in `error`.
-     The pipeline creates the slot and recovers as under `managed`, above.
-  3. Once the pipeline runs, set `require_existing` back (keep `auto_cleanup: false`).
-
-  The role already has `REPLICATION`, which streaming needs, and that is enough to create a slot.
+- With `require_existing` the pipeline stops in `error`: it creates no slot. Have the DBA create
+  the slot again. A slot made again starts at the server's position now, past the pipeline's
+  checkpoint, so the changes in between are lost and the pipeline refuses it with
+  `slot_on_orphan: halt` (the default). Set `slot_on_orphan: adopt_resnapshot` (applied without a
+  restart) and `POST /v1/pipelines/<id>/restart`: the pipeline adopts the slot and copies the
+  tables as `snapshots.mode` says. Once it runs, set `slot_on_orphan` back to `halt`.
+
+## A failover (`WAL_SOURCE_CHANGED`)
+
+**Symptom:** after a failover, or after the host was pointed at another server, the pipeline stops
+in `error` with `WAL_SOURCE_CHANGED`, naming the server its checkpoint was read on and the one it
+reads now.
+
+The checkpoint records the server: its system identifier and its timeline. A promoted replica
+keeps the identifier and moves to a new timeline. Its WAL ends where its replication did, which can
+be behind what the lake already holds, so it writes again LSNs the lake has marked. Resuming would
+skip those changes. The logical slot isn't on the promoted server either: PostgreSQL doesn't copy
+it to a physical replica.
+
+1. If the old server is the one you want, point the pipeline back at it (a reload restarts the
+   pipeline) and it resumes.
+2. Otherwise set `source.postgres.source_change_action: re_snapshot` (a reload applies it, without
+   a restart) and `POST /v1/pipelines/<id>/restart`. The pipeline moves every table onto the new
+   server's position, makes its slot there (under `require_existing`, have the DBA create it
+   first), and copies the tables as `snapshots.mode` says: under `never`, none, with a warning, and
+   you start a snapshot job for each table.
+3. Rows the old server had but the promoted one never received stay in the Delta tables (the
+   caution above). They are the changes the failover lost.
+
+Leave `source_change_action` at `re_snapshot` only where a failover should recover by itself.
+
+## A renamed or dropped publication
+
+The pipeline finds its publication by name at each start. Under `managed` a publication it can't
+find is created again, with the pipeline's tables, and the pipeline re-snapshots: the WAL since its
+checkpoint was decoded without it. The renamed publication stays behind; drop it. Under
+`require_existing` the pipeline stops in `error` ("publication … doesn't exist"): rename it back,
+or set `source.postgres.publication` to the new name and reload.
 
 ## An orphan slot
 
@@ -95,18 +123,21 @@ needs. A pipeline stopped for longer than the server keeps its binlog
 ## The server changed (`BINLOG_SOURCE_CHANGED`)
 
 With `replication_type: binlog`, a position is a file and an offset of one server. When the host
-now leads to another server (a failover, a restore), the pipeline stops in `error` with
-`BINLOG_SOURCE_CHANGED`.
-
-1. Prefer GTIDs: set `replication_type: gtid` (the server needs them on) and
-   `binlog_purge_action: re_snapshot`, and reload. The pipeline can't carry a file position over
-   to a GTID one, so it starts from the server's position now (and copies the tables as
-   `snapshots.mode` says), then follows GTIDs, which survive later failovers.
-2. Without GTIDs, the position can't be repaired in place: replace the pipeline. Give it a new
-   `id` and a new `sink.prefix` (or none, so the prefix is the new `id`), with
-   `snapshots.mode: initial`, in one reload. The new pipeline copies every table into fresh Delta
-   tables and streams from the server's position now; drop the old tables once nothing reads
-   them. The old pipeline's state stays behind, unused.
+now leads to another server (a failover, a restore), the checkpoint means nothing there. The
+pipeline tells servers apart by `server_uuid` on MySQL and by `server_id` on MariaDB, and on
+MariaDB it also checks that the position is an event boundary the server's binlog knows
+(`BINLOG_GTID_POS`).
+
+- With `binlog_purge_action: error` (the default) the pipeline stops in `error` with
+  `BINLOG_SOURCE_CHANGED`. Point it back at its server, or recover as below.
+- With `binlog_purge_action: re_snapshot` the pipeline starts from the new server's position, with
+  a warning, and copies the tables as `snapshots.mode` says. To recover a stopped pipeline, set it
+  (a reload applies it, without a restart) and `POST /v1/pipelines/<id>/restart`.
+
+Prefer GTIDs: with `replication_type: gtid` (the server needs them on) a position survives a
+failover to a replica. Switching is a reload: the pipeline can't carry a file position over to a
+GTID one, so it starts from the server's position now (with `binlog_purge_action: re_snapshot`) and
+copies the tables as `snapshots.mode` says.
 
 </TabItem>
 </Tabs>
```

`docs/src/content/docs/runbooks/source-privileges.mdx`:

```diff
diff --git a/docs/src/content/docs/runbooks/source-privileges.mdx b/docs/src/content/docs/runbooks/source-privileges.mdx
index b2f2758..73678a0 100644
--- a/docs/src/content/docs/runbooks/source-privileges.mdx
+++ b/docs/src/content/docs/runbooks/source-privileges.mdx
@@ -65,7 +65,17 @@ pipeline runs, set `slot_on_orphan` back to `halt` (a reload applies it), so tha
 state is lost later stops rather than starting over by itself.
 
 With `require_existing`, a table the pipeline replicates but the publication doesn't publish is a
-warning: its changes never arrive. Keep the publication in step with `tables.include`.
+warning: its changes never arrive. Keep the publication in step with `tables.include`, and publish
+the tables whole: a row filter, a column list or `publish_generated_columns = stored` on a
+replicated table is refused (see
+[Sources](/data-avalanche/configuration/sources/#postgresql)).
+
+A privilege lost later, such as `SELECT` revoked on one table, halts that table when a snapshot
+reads it; the other tables run on. Grant it again, then copy the table afresh (see
+[A halted table](/data-avalanche/runbooks/halted-table/)).
+
+Point the pipeline at the primary: a standby before PostgreSQL 16 is refused, and from 16 one
+works with a warning (see Sources).
 
 On a managed service, follow its guide to logical replication: on Amazon RDS and Aurora the role
 is granted `rds_replication` and `rds.logical_replication` is set; on Cloud SQL the
@@ -126,6 +136,10 @@ GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, SLAVE MONITOR ON *.* TO 'cd
 `REPLICATION CLIENT` is MariaDB's `BINLOG MONITOR` since 10.5. `SLAVE MONITOR` lets the pipeline
 read a replica's status to check `log_slave_updates`; without it the pipeline warns.
 
+A Galera node (`wsrep_on = ON`) applies the other nodes' writes without a replica connection: it
+needs `log_slave_updates = ON`, or the pipeline refuses it, since its binlog would lack those
+writes.
+
 </TabItem>
 </Tabs>
 
```


- [ ] **Step 2: The tracker**

§53 closes every row §52 named, with a pointer, and the counts follow (Open 36 → 18). Set the
section's date and the rows' dates to the day it is written.

`doc/analysis/CDC_Design_Gap_Analysis_v1.md`:

```diff
diff --git a/doc/analysis/CDC_Design_Gap_Analysis_v1.md b/doc/analysis/CDC_Design_Gap_Analysis_v1.md
index a7f3403..1fda885 100644
--- a/doc/analysis/CDC_Design_Gap_Analysis_v1.md
+++ b/doc/analysis/CDC_Design_Gap_Analysis_v1.md
@@ -506,6 +506,35 @@ read 37. With DA-X-15 (§52.1) it is 36.
 |---|---|---|---|---|
 | DA-X-15 | `jobs::tests::a_table_outside_the_config_is_copied_and_not_streamed` fails now and then (2 runs in 30 on the base, 7 in 30 beside a build): it drains the worker once the copied rows land, but the copy is marked done only at its last chunk read, an empty one, so a drain in between leaves its snapshot `InProgress` | 🟢 | M12a planning (the gates, under load) | The test waits until the worker reports no snapshot running (30 runs in 30 then); M12a Task 11 |
 
+## 53. M12a source hardening — the gaps closed (2026-10-30)
+
+M12a's tasks (`doc/v1.0/M12a-source-hardening.md`) closed the source gaps §52 named. The
+operator pages say what changed: `configuration/sources.md` (a failover, a standby, what the
+publication publishes, `verify-ca` on MySQL), `runbooks/position-lost.mdx` (a failover, a DBA's
+slot made again, a renamed publication, a changed MySQL server) and `runbooks/source-privileges.mdx`.
+
+| ID | Change | Evidence |
+|---|---|---|
+| DA-SRC-20 | 🟥 → ✅ | `slot.rs` `source_change` (`WAL_SOURCE_CHANGED`, `source_change_action`), `worker.rs` `reposition`; E2E `failover.rs` |
+| DA-SRC-29 | 🟥 → ✅ | `start.rs` `MySqlConnector::changed` follows `binlog_purge_action`; IT `a_checkpoint_from_another_server` |
+| DA-SRC-21 | 🟥 → ✅ | `start.rs` `at_boundary` (`BINLOG_GTID_POS`), at the start and at the dump's open; IT `a_mariadb_position_off_an_event_boundary_is_another_server_s` |
+| DA-SRC-30 | 🟥 → ✅ | `slot.rs` `decide`: adopted under `slot_on_orphan: adopt_resnapshot`, else refused with the key named; IT `require_existing_never_creates` |
+| DA-OPS-13 | 🟥 → ✅ | `slot.rs` names `slot_on_orphan`; `cli.rs` `about` names the three sources (`the_help_names_every_source`) |
+| DA-SRC-26 | 🟥 → ✅ | `stream.rs` `Held` (savepoints), `roll_back`; IT `a_rollback_drops_the_rows_it_undid` |
+| DA-SRC-24 | 🟥 → ✅ | `server.rs` `check_coverage` refuses a Galera node without `log_slave_updates` |
+| DA-SRC-25 | 🟥 → ✅ | `schema.rs` `keep_names`; IT `a_change_of_case_alone_keeps_the_column_s_name` |
+| DA-SRC-22 | 🟥 → ✅ | documented, fails closed: V-124 warns on `verify-ca` for MySQL/MariaDB; `connect.rs` names the re-check at each `mysql_async` upgrade (pinned by `verify_ca_by_an_unnamed_address_is_refused_like_verify_full`) |
+| DA-SRC-14 | 🟥 → ✅ | `catalog.rs` refuses a `bit`/`varbit` key (`IDENTIFIER_NOT_USABLE`) unless read as text |
+| DA-SRC-19 | 🟥 → ✅ | `snapshot.rs` (both sources) `table_scoped`: a table's SQLSTATE / MySQL error halts the table |
+| DA-SNAP-13 | 🟥 → ✅ | `schema_change.rs` `missing_columns`, the worker's baseline against the sink table; `a_wiped_state_baselines_against_the_sink_table` |
+| DA-SNAP-14 | 🟥 → ✅ | MySQL `snapshot.rs` `moved`: a reordered `ENUM`/`SET` key is a change under the snapshot |
+| DA-SRC-15 | 🟥 → ✅ | `KeyCollation::Own` (a domain's collation, schema-qualified), the index-collation warning, `limit 0` refused; the byte budget documented (`chunk_size` per table), `E''` literals unreachable (`standard_conforming_strings` pinned) |
+| DA-SRC-13 | 🟥 → ✅ | `slot.rs` `publication_shape`: refused under `require_existing`, repaired under `managed`, extra tables warned; IT `a_publication_s_blind_spots_are_refused_warned_or_repaired` |
+| DA-SRC-17 | 🟥 → ✅ | fixed: the table-set errors all reported, the server's detail and hint, a standby refused/warned, the bounded call's one deadline, a year past PostgreSQL's range; the rest recorded as not reachable in the plan (Task 10) |
+| DA-SRC-18 | 🟥 → ✅ | `tls.rs` verifier tests, `require_encrypts_both_connections_without_verifying`, `keys.rs`, `postgres-tls` on `x-pg`, the `replica` profile and `failover.rs` |
+| DA-X-15 | 🆕 → ✅ | `jobs.rs` `a_table_outside_the_config_is_copied_and_not_streamed` waits for the copy's completion |
+| DA-SRC-9 | 🔄 → ✅ | decided: one catalog connection per pipeline serves channel C, the chunks and the re-queries (batched per commit); V-077 warns; throughput measured in M12c |
+
 ---
 
 ### Counts (by status)
@@ -517,13 +546,13 @@ read 37. With DA-X-15 (§52.1) it is 36.
 > M7a's plan (§32), 2026-10-16 for M7a's exit (§33), 2026-10-17 for M7b's plan (§34),
 > 2026-10-20 for M7b's exit (§35), 2026-10-21 for M8a's plan (§36), 2026-10-22 for M8a's exit (§37),
 > 2026-10-22 for M8b's plan (§38), 2026-10-23 for M8b's exit (§39), 2026-10-23 for M9a's plan
-> (§40), 2026-10-23 for M9a's exit (§41), 2026-10-27 for M9b's plan (§42), 2026-10-28 for M9b's exit (§43), 2026-10-28 for M10's plan (§44), and 2026-10-29 for M10's exit (§45), and 2026-10-29 for M11a's plan (§46), and 2026-10-29 for M11a's whole-branch review (§47), and 2026-10-29 for M11a's exit (§48), and 2026-10-29 for M11b's plan (§49), and 2026-10-30 for M11b's pages (§50), and 2026-10-30 for M11b's exit and v0.1.0 (§51), and 2026-10-30 for M12a's plan (§52).
+> (§40), 2026-10-23 for M9a's exit (§41), 2026-10-27 for M9b's plan (§42), 2026-10-28 for M9b's exit (§43), 2026-10-28 for M10's plan (§44), and 2026-10-29 for M10's exit (§45), and 2026-10-29 for M11a's plan (§46), and 2026-10-29 for M11a's whole-branch review (§47), and 2026-10-29 for M11a's exit (§48), and 2026-10-29 for M11b's plan (§49), and 2026-10-30 for M11b's pages (§50), and 2026-10-30 for M11b's exit and v0.1.0 (§51), and 2026-10-30 for M12a's plan (§52), and 2026-10-30 for M12a's gaps closed (§53).
 
 | Status | Count (v1 scope) | Notes |
 |---|---|---|
 | ✅ Resolved | the large majority | Design closed by a research/spec doc or a probe. |
-| 🔄 Partial | 4 | DA-OPS-10 and DA-SNAP-15 🔄 2026-10-30 (§50); DA-SCHEMA-10 ✅ 2026-10-28 (M9b merged, §43); DA-SNAP-7, DA-SRC-9 (neither blocks the plan); DA-OPS-8 ✅ 2026-10-30 (§50; 🔄 2026-10-22, §38); DA-SNAP-10 ✅ 2026-10-16 (M7a merged, §33). DA-MS-3 is a decided caveat. DA-X-8 ✅ 2026-09-26; DA-SRC-16 and DA-X-12 ✅ 2026-10-13 (§30). |
-| 🟥 Open (v1) | **36** | DA-X-15 🆕 2026-10-30 (§52.1). Recounted 2026-10-30 (§52): the 34 domain rows and DA-X-13. DA-OPS-11 ✅ 2026-10-30 (§51: `v0.1.0` released, the packages public); DA-SRC-30 🆕 2026-10-30 (§50.1); DA-OPS-10, DA-API-14, DA-API-15, DA-SNAP-15, DA-SNAP-17 and DA-SNAP-18 leave 2026-10-30 (§50; DA-OPS-10 and DA-SNAP-15 as 🔄); DA-CLUS-15 stays; DA-OPS-12, DA-OPS-13 and DA-SRC-29 🆕 2026-10-29 (§49.1); DA-CLUS-15 🆕 2026-10-29 (§47.1); DA-OPS-11 🆕 2026-10-29 (§46.1), extended (§47), the manual run done (§48); DA-OBS-8 and DA-OBS-9 🆕 2026-10-29 (§45.1); DA-OBS-7 🆕 2026-10-28 (§44.1); DA-SNAP-18…21 🆕 2026-10-28 (§43.1); DA-SNAP-15…17 🆕 2026-10-27 (§42.1); DA-API-13…18 🆕 2026-10-23 and DA-API-20 🆕 2026-10-24 (§41.1); DA-API-19 🆕 2026-10-23, ✅ 2026-10-24 (§41); DA-SRC-13…15, 17, 18, carried from M5 (§27); DA-SNAP-11…13 and DA-SRC-19 🆕 2026-10-07 (§28.2); DA-SRC-20 🆕 2026-10-13 (§30.1); DA-SRC-21 🆕 2026-10-14 (§31.2); DA-SRC-22, DA-SNAP-14 and DA-X-13 🆕 2026-10-14 (§32.1); DA-SRC-24…26 🆕 2026-10-16 (§33.1); DA-OBS-6 🆕 2026-10-17 (§34.1), ✅ 2026-10-29 (M10 merged, §45); DA-OPS-9 🆕 2026-10-22 (§38.1); DA-OPS-10 🆕 2026-10-23 (§39.1). DA-OPS-8 🔄 2026-10-22 (§38). DA-API-11 and DA-API-12 ✅ 2026-10-23 (§40). DA-CLUS-14, DA-OPS-7 and DA-SRC-28 ✅ 2026-10-22 (§37). DA-SRC-27 and DA-X-14 ✅ 2026-10-20 (§35). DA-SNAP-9 ✅ 2026-10-03 (§28). The design decisions were all made 2026-09-26 (§26.5, `V1_Scope_Decisions.md`), and the deferrals confirmed. |
+| 🔄 Partial | 3 | DA-SRC-9 ✅ 2026-10-30 (§53); DA-OPS-10 and DA-SNAP-15 🔄 2026-10-30 (§50); DA-SCHEMA-10 ✅ 2026-10-28 (M9b merged, §43); DA-SNAP-7, DA-SRC-9 (neither blocks the plan); DA-OPS-8 ✅ 2026-10-30 (§50; 🔄 2026-10-22, §38); DA-SNAP-10 ✅ 2026-10-16 (M7a merged, §33). DA-MS-3 is a decided caveat. DA-X-8 ✅ 2026-09-26; DA-SRC-16 and DA-X-12 ✅ 2026-10-13 (§30). |
+| 🟥 Open (v1) | **18** | DA-SRC-13…15, 17…22, 24…26, 29, 30, DA-SNAP-13, DA-SNAP-14, DA-OPS-13 and DA-X-15 ✅ 2026-10-30 (§53). DA-X-15 🆕 2026-10-30 (§52.1). Recounted 2026-10-30 (§52): the 34 domain rows and DA-X-13. DA-OPS-11 ✅ 2026-10-30 (§51: `v0.1.0` released, the packages public); DA-SRC-30 🆕 2026-10-30 (§50.1); DA-OPS-10, DA-API-14, DA-API-15, DA-SNAP-15, DA-SNAP-17 and DA-SNAP-18 leave 2026-10-30 (§50; DA-OPS-10 and DA-SNAP-15 as 🔄); DA-CLUS-15 stays; DA-OPS-12, DA-OPS-13 and DA-SRC-29 🆕 2026-10-29 (§49.1); DA-CLUS-15 🆕 2026-10-29 (§47.1); DA-OPS-11 🆕 2026-10-29 (§46.1), extended (§47), the manual run done (§48); DA-OBS-8 and DA-OBS-9 🆕 2026-10-29 (§45.1); DA-OBS-7 🆕 2026-10-28 (§44.1); DA-SNAP-18…21 🆕 2026-10-28 (§43.1); DA-SNAP-15…17 🆕 2026-10-27 (§42.1); DA-API-13…18 🆕 2026-10-23 and DA-API-20 🆕 2026-10-24 (§41.1); DA-API-19 🆕 2026-10-23, ✅ 2026-10-24 (§41); DA-SRC-13…15, 17, 18, carried from M5 (§27); DA-SNAP-11…13 and DA-SRC-19 🆕 2026-10-07 (§28.2); DA-SRC-20 🆕 2026-10-13 (§30.1); DA-SRC-21 🆕 2026-10-14 (§31.2); DA-SRC-22, DA-SNAP-14 and DA-X-13 🆕 2026-10-14 (§32.1); DA-SRC-24…26 🆕 2026-10-16 (§33.1); DA-OBS-6 🆕 2026-10-17 (§34.1), ✅ 2026-10-29 (M10 merged, §45); DA-OPS-9 🆕 2026-10-22 (§38.1); DA-OPS-10 🆕 2026-10-23 (§39.1). DA-OPS-8 🔄 2026-10-22 (§38). DA-API-11 and DA-API-12 ✅ 2026-10-23 (§40). DA-CLUS-14, DA-OPS-7 and DA-SRC-28 ✅ 2026-10-22 (§37). DA-SRC-27 and DA-X-14 ✅ 2026-10-20 (§35). DA-SNAP-9 ✅ 2026-10-03 (§28). The design decisions were all made 2026-09-26 (§26.5, `V1_Scope_Decisions.md`), and the deferrals confirmed. |
 | 🧪 Verify (v1) | **0** | DA-SRC-10/11 proven 2026-09-26 (§21). |
 | ⏭ v2 | 13 | Iceberg-sink items (§26.2). |
 | 🟢 Spec-merge debt | **0** | All applied 2026-09-26 (§26.6): §19.3 S-1…S-6, the DA-X-8 config residual, the D-V1 patches and the Data-Flow Phase 2a probe details. |
@@ -688,8 +717,8 @@ read-point hazard. This is a *change* to `Data-Flow.md` Phase 1, so it carries s
 | DA-SNAP-10 | MySQL's window rule may miss a transaction the binlog delivered before InnoDB made it visible | 🟡 | ✅ | §28.1. MySQL/MariaDB chunks carry no read visibility, so the reconciler uses the window rule: only transactions applied after a read was issued are filtered. Group commit makes the binlog readable (sync stage) before the InnoDB commit (commit stage); semi-sync `AFTER_SYNC` waits for the replica ack between them, up to `rpl_semi_sync_source_timeout`. A transaction applied from the binlog before a chunk is issued can then still be invisible to that chunk's read, and a stale chunk row would undo it, the MySQL analogue of DA-SNAP-9's stalled synchronous commit. Reasoned (M6a Task 9 review), not measured. Close before M7: an S-MY probe that stalls a semi-sync ack, then the fix (e.g. a per-read visibility signal, or waiting for in-flight commits as Q-M6a-2 does on PostgreSQL). **🔄 2026-10-14 (§31): measured and decided.** S-MY11 measured it on MySQL 8.4 and MariaDB 10.11 / 11.8 (semi-sync AFTER_SYNC; MariaDB's default AFTER_COMMIT has no gap). Fix (Q-M7-3): a MySQL chunk reads with `LOCK IN SHARE MODE` in its short READ COMMITTED transaction, which waits for the stalled commit and sees it (S-MY11; its cost, S-MY11b); `Data-Flow.md` Phase 1 step 2b/2c, `Snapshot_Resumability…` §1.1. Closes when M7 builds it with its test. **2026-10-14 (§32):** built and tested in M7a's plan (Task 10, `a_locking_read_waits_for_a_writer_of_its_rows`, on MySQL 8.4 and MariaDB 11.8); ✅ when M7a merges. **✅ 2026-10-16:** M7a merged (PR #9) with the locking chunk read and its test. | — |
 | DA-SNAP-11 | 🆕 The reconciler's log is unbounded while a snapshotting table's reads wait | 🟢 | 🟥 | §28.2. Every stream transaction that writes a snapshotting table is logged with its keys until a chunk proves it visible. While the table's reads wait (an in-flight writer at session start, a drift hold), no chunk is reconciled and the log grows with every key written, for up to `timeouts.snapshot`. M6a warns once per wait and reports the log's size in the status (M6b's gauge); bounding it (coalesce per table, or skip logging until a read can be issued) is open. | — |
 | DA-SNAP-12 | 🆕 `parallelism > 1` reads chunks one after another on PostgreSQL | 🟢 | 🟥 | §28.2. Chunk reads share the one catalog connection, so they pipeline rather than run in parallel; each read's client-side bound starts at spawn, so a read queued behind slow chunks can time out and the abandoned connection fails its neighbours. Fix: a small read pool or a connection per concurrent read. | — |
-| DA-SNAP-14 | 🆕 An `ENUM` key whose members are reordered during a snapshot | 🟢 | 🟥 | §32.1. An `ENUM` key sorts by member index, and a chunk resumes after its cursor's label at that label's index under the members read for the chunk. A reorder mid-snapshot changes the order, so the next chunk can skip or repeat rows. The stream halts the table at the reorder (§7.2), but a chunk read between the DDL and the signal doesn't notice: the read plan compares value types, text for an `ENUM` either way. | — |
-| DA-SNAP-13 | 🆕 A wiped state with an existing sink table baselines nothing | 🟢 | 🟥 | §28.2. With no cached schema, Phase 0 doesn't baseline (§8.2) against the sink table's schema; a shape mismatch surfaces later as `SINK_REJECTED` (visible, not silent). Only the adopted-orphan case with a cache is covered. | — |
+| DA-SNAP-14 | 🆕 An `ENUM` key whose members are reordered during a snapshot | 🟢 | ✅ | §32.1. An `ENUM` key sorts by member index, and a chunk resumes after its cursor's label at that label's index under the members read for the chunk. A reorder mid-snapshot changes the order, so the next chunk can skip or repeat rows. The stream halts the table at the reorder (§7.2), but a chunk read between the DDL and the signal doesn't notice: the read plan compares value types, text for an `ENUM` either way. **✅ 2026-10-30 (§53):** MySQL `snapshot.rs` `moved`: a reordered `ENUM`/`SET` key is a change under the snapshot. | — |
+| DA-SNAP-13 | 🆕 A wiped state with an existing sink table baselines nothing | 🟢 | ✅ | §28.2. With no cached schema, Phase 0 doesn't baseline (§8.2) against the sink table's schema; a shape mismatch surfaces later as `SINK_REJECTED` (visible, not silent). Only the adopted-orphan case with a cache is covered. **✅ 2026-10-30 (§53):** `schema_change.rs` `missing_columns`, the worker's baseline against the sink table; `a_wiped_state_baselines_against_the_sink_table`. | — |
 | DA-SNAP-15 | 🆕 A job upserts: a row deleted at the source while its table wasn't streamed stays | 🟡 | 🔄 | §42.1. Q-M9b-2: no truncate. M11's docs (drop or move the Delta table first); a truncate option later. **🔄 2026-10-30 (§50):** documented in `architecture/snapshots.md`, `runbooks/snapshot-jobs.md`, `runbooks/halted-table.md`; the truncate option stays open. | — |
 | DA-SNAP-16 | 🆕 Job documents accumulate, and the list and the ask's check read every one | 🟢 | 🟥 | §42.1. A retention, or an index of the unfinished jobs. | — |
 | DA-SNAP-17 | 🆕 A job table's halt fails the job only with the session's next flush | 🟢 | ✅ | §42.1. Write a job table's halt at once, or M11's docs. **✅ 2026-10-30 (§50, documented):** `runbooks/snapshot-jobs.md`. | — |
@@ -777,28 +806,28 @@ the MySQL path with one genuine correctness dependency.
 | DA-SRC-6 | MySQL purged-binlog handling | 🟡 | ✅ | `Operational…` G-26 — `binlog_purge_action: error(default)\|re_snapshot`. | G-26 |
 | DA-SRC-7 | External same-name slot collisions undetectable | 🟢 | ✅(limitation) | §5 — PG slots carry no ownership tag; mitigated by V-058 uniqueness + operator discipline; documented, not papered over. | G-15 |
 | DA-SRC-8 | MariaDB GTID dialect (`domain-server-sequence`) | 🟡 | ✅ | `Operational…` G-38 — MariaDB accepted via the MySQL path, but the `SourcePosition` MySQL impl **must support both GTID dialects** (MariaDB domains vs MySQL UUIDs); flagged as a **new sub-research item** and the one genuine correctness dependency. **2026-09-26: feasible — proven** (S-MY9, `mysql_async`): MariaDB GTID-positioned resume via `@mariadb_slave_capability`/`@slave_connect_state`/`@slave_gtid_strict_mode`; per-txn GTIDs decode exactly (last = `gtid_binlog_pos`) with a ~10-line parser. Remaining: confirm MariaDB in v1 scope (recommended). **✅ 2026-09-26 — D-V1.5 (`V1_Scope_Decisions.md`):** MariaDB in v1; dialect selected at connect. | G-38 |
-| DA-SRC-9 | TOAST re-query control-connection pool sizing | 🟢 | 🔄 | V-077 warns on `re_query`+high-volume snapshot; concrete pool sizing is an implementation detail, not yet fixed. | — |
+| DA-SRC-9 | TOAST re-query control-connection pool sizing | 🟢 | ✅ | V-077 warns on `re_query`+high-volume snapshot; concrete pool sizing is an implementation detail, not yet fixed. **✅ 2026-10-30 (§53):** decided: one catalog connection per pipeline serves channel C, the chunks and the re-queries (batched per commit); V-077 warns; throughput measured in M12c. | — |
 | DA-SRC-10 | **PG replication client crate capability** (`pgwire-replication` named in Sources_Index; never pinned, compiled or probed) | 🔴 | ✅ | Needed: pgoutput proto v1–v4 incl. streamed in-progress txns (`Stream Start/Stop/Commit/Abort`), `Relation`/`Type`/`Truncate` messages, unchanged-TOAST marker, standby status feedback (flush-LSN ack), slot `START_REPLICATION` from an LSN, TLS, keepalives. Probe: `prototypes/source-capability/` (S-gates) against local PostgreSQL. A negative result means writing/forking the protocol layer — a plan-shaping item. **Scaffolded 2026-09-26, source-read:** the crate is **transport-only** — pgoutput decoding (Relation/Insert/Update/Delete/Truncate/Type/Origin) is a stated non-goal, so **the decoder is ours** (bounded v1 work item); `proto_version '1'` is hard-coded with no `streaming` (large txns arrive whole at commit — fine for the design, but `Data-Flow.md` Phase 2a says `'2'`). Gates S-PG0…S-PG8 in `prototypes/source-capability/README.md`. **Proven 2026-09-26** (`prototypes/source-capability/`, PG 17): S-PG0…S-PG8 9/9 — transport, our pgoutput decoder (Relation/Type/Origin/Insert/Update none·K·O/Delete K·O/Truncate), unchanged-TOAST `u`, 50k-row txn whole at commit under proto v1, restart/resume at-least-once, slow consumer survives `wal_sender_timeout`, live `ALTER PUBLICATION ADD TABLE`, 18-type text decode, TLS 1.3 verify-full. **Pin `pgwire-replication =0.4.1`; the pgoutput decoder is a v1 work item.** Implementation rules: `with_wakeup_interval` ≈ status interval (idle acks otherwise lag up to 10 s); pin the walsender session (`TimeZone=UTC`, `DateStyle=ISO`, `IntervalStyle=iso_8601`, `extra_float_digits=3`). | — |
 | DA-SRC-11 | **MySQL binlog client crate capability** (`mysql-binlog-connector-rust` named in Sources_Index; never pinned, compiled or probed) | 🔴 | ✅ | Needed: GTID-set positioning (`COM_BINLOG_DUMP_GTID`), `ROW` events with FULL image, `TABLE_MAP` optional metadata (column names/signedness, `binlog_row_metadata=FULL`), `QUERY_EVENT` (DDL classifier input), CRC32 checksums, heartbeats, MySQL 8.4 `caching_sha2_password` auth + TLS; MariaDB GTID dialect if in v1 scope (DA-SRC-8). Probe: `prototypes/source-capability/` against local MySQL (and MariaDB). **Scaffolded 2026-09-26, source-read:** two candidates probed side by side — (a) `mysql-binlog-connector-rust` 0.3.3 (TLS looks absent, claims ≤ MySQL 8.0.31, has MariaDB GTID events) vs (b) `mysql_async` 0.37.1 binlog stream (TLS ✅, active; MariaDB GTID events not found in `mysql_common`). Neither complete on paper. Gates S-MY0…S-MY9. **Proven 2026-09-26** (MySQL 8.4 + MariaDB 11.4): **pin `mysql_async =0.37.1`** — S-MY0…S-MY9 10/10 (GTID resume, FULL images, optional metadata incl. UNSIGNED, DDL `QUERY_EVENT`, heartbeats via `@master_heartbeat_period`, zstd payloads, type decode, TLS 1.3, MariaDB GTID). **Reject** `mysql-binlog-connector-rust` 0.3.3: no TLS, no MariaDB GTID positioning, `is_signed` carries the *unsigned* bit, `BIGINT UNSIGNED` as `-1`. Own code: MariaDB `GTID_EVENT` (type 162) decode; payload guard (DA-SRC-12). | — |
 | DA-SRC-12 | **`mysql_async` silently drops a compressed transaction it cannot decompress** | 🟡 | ✅ (guard designed) | Found 2026-09-26 (source-read, `mysql_common` 0.37.3 / `mysql_async` 0.37.1): on a zstd error `TransactionPayloadEvent::danger_decompress` returns an **empty buffer**, and `mysql_async` ignores a payload-header parse error (`/* TODO: Log the error */`) — the transaction's rows vanish with no stream error. Rare (corrupt payload / unsupported algorithm) but silent data loss. Guard (v1): validate every `TRANSACTION_PAYLOAD` ourselves — decompressed length == `uncompressed_size`, ≥ 1 inner event — and halt the pipeline on mismatch; offer the fix upstream. **✅ 2026-09-26 — D-V1.6 (`V1_Scope_Decisions.md`):** validate each payload; pipeline halt `BINLOG_PAYLOAD_INVALID`; report upstream. **2026-10-14 (§32):** M7a's plan builds it as a count-down of the payload's `uncompressed_size` by the events it yields (Task 6, measured equal to the decompressed length on MySQL 8.4), halting with a `BINLOG_PAYLOAD_INVALID: ` message; the fault-injection E2E is M7b's. | — |
-| DA-SRC-13 | Publication blind spots | 🟢 | 🆕 🟥 | Carried from M5; detail in §27. | — |
-| DA-SRC-14 | `bit` / `varbit` / PostGIS keys | 🟢 | 🆕 🟥 | Carried from M5; detail in §27. | — |
-| DA-SRC-15 | Snapshot collation and chunk edges | 🟢 | 🆕 🟥 | Carried from M5; detail in §27. | — |
+| DA-SRC-13 | Publication blind spots | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-10-30 (§53):** `slot.rs` `publication_shape`: refused under `require_existing`, repaired under `managed`, extra tables warned; IT `a_publication_s_blind_spots_are_refused_warned_or_repaired`. | — |
+| DA-SRC-14 | `bit` / `varbit` / PostGIS keys | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-10-30 (§53):** `catalog.rs` refuses a `bit`/`varbit` key (`IDENTIFIER_NOT_USABLE`) unless read as text. | — |
+| DA-SRC-15 | Snapshot collation and chunk edges | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-10-30 (§53):** `KeyCollation::Own` (a domain's collation, schema-qualified), the index-collation warning, `limit 0` refused; the byte budget documented (`chunk_size` per table), `E''` literals unreachable (`standard_conforming_strings` pinned). | — |
 | DA-SRC-16 | The replication connection's timeouts | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-10-13** — M6b Task 22: the stream watchdog ends a session whose stream is silent while the slot finds no reader, and the catalog connect is bounded by `timeouts.connect` (§30). | — |
-| DA-SRC-17 | Source edges PostgreSQL doesn't produce, and small gaps | 🟢 | 🆕 🟥 | Carried from M5; detail in §27. | — |
-| DA-SRC-18 | M5 test gaps | 🟢 | 🆕 🟥 | Carried from M5; detail in §27. | — |
-| DA-SRC-19 | 🆕 Which permanent chunk-read errors are table-scoped | 🟢 | 🟥 | §28.2. A permanent chunk-read error ends the whole pipeline (D-M6a.7); a retyped key's 42883/42804 would loop it through restarts though only that table's read is broken. A non-drop 42703 (e.g. a quoting bug) now retries as "changed under the snapshot" until `SNAPSHOT_TIMEOUT`, with a warning per streak. Rule per SQLSTATE which errors halt the table and which end the pipeline, with MySQL's in view (M7/M9). | — |
-| DA-SRC-20 | 🆕 A PostgreSQL source failover moves positions backwards | 🟡 | 🟥 | §30.1. The position guard assumes an LSN only grows. After a failover to a replica whose WAL ends behind the table's marker, the new primary reuses LSNs the lake has already marked, and the guard skips its changes, so data is lost. `Delivery_Guarantee_and_Position_Guard.md` §6 designs a source failover for MySQL only (a new `server_uuid`). Design the PostgreSQL case: detect the timeline or system identifier change, then halt or re-snapshot. | — |
-| DA-SRC-22 | 🆕 MySQL `ssl_mode: verify-ca` checks the host name too | 🟢 | 🟥 | §32.1. `mysql_async` 0.37.1 skips the name check only for an error whose text contains `NotValidForName`; rustls 0.23 prints `certificate not valid for name`. So `verify-ca` acts as `verify-full` and refuses a connection by an address the certificate doesn't name. Fails closed (pinned by a test in M7a); M5 met the same in the PostgreSQL replication connection. | — |
+| DA-SRC-17 | Source edges PostgreSQL doesn't produce, and small gaps | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-10-30 (§53):** fixed: the table-set errors all reported, the server's detail and hint, a standby refused/warned, the bounded call's one deadline, a year past PostgreSQL's range; the rest recorded as not reachable in the plan (Task 10). | — |
+| DA-SRC-18 | M5 test gaps | 🟢 | ✅ | Carried from M5; detail in §27. **✅ 2026-10-30 (§53):** `tls.rs` verifier tests, `require_encrypts_both_connections_without_verifying`, `keys.rs`, `postgres-tls` on `x-pg`, the `replica` profile and `failover.rs`. | — |
+| DA-SRC-19 | 🆕 Which permanent chunk-read errors are table-scoped | 🟢 | ✅ | §28.2. A permanent chunk-read error ends the whole pipeline (D-M6a.7); a retyped key's 42883/42804 would loop it through restarts though only that table's read is broken. A non-drop 42703 (e.g. a quoting bug) now retries as "changed under the snapshot" until `SNAPSHOT_TIMEOUT`, with a warning per streak. Rule per SQLSTATE which errors halt the table and which end the pipeline, with MySQL's in view (M7/M9). **✅ 2026-10-30 (§53):** `snapshot.rs` (both sources) `table_scoped`: a table's SQLSTATE / MySQL error halts the table. | — |
+| DA-SRC-20 | 🆕 A PostgreSQL source failover moves positions backwards | 🟡 | ✅ | §30.1. The position guard assumes an LSN only grows. After a failover to a replica whose WAL ends behind the table's marker, the new primary reuses LSNs the lake has already marked, and the guard skips its changes, so data is lost. `Delivery_Guarantee_and_Position_Guard.md` §6 designs a source failover for MySQL only (a new `server_uuid`). Design the PostgreSQL case: detect the timeline or system identifier change, then halt or re-snapshot. **✅ 2026-10-30 (§53):** `slot.rs` `source_change` (`WAL_SOURCE_CHANGED`, `source_change_action`), `worker.rs` `reposition`; E2E `failover.rs`. | — |
+| DA-SRC-22 | 🆕 MySQL `ssl_mode: verify-ca` checks the host name too | 🟢 | ✅ | §32.1. `mysql_async` 0.37.1 skips the name check only for an error whose text contains `NotValidForName`; rustls 0.23 prints `certificate not valid for name`. So `verify-ca` acts as `verify-full` and refuses a connection by an address the certificate doesn't name. Fails closed (pinned by a test in M7a); M5 met the same in the PostgreSQL replication connection. **✅ 2026-10-30 (§53):** documented, fails closed: V-124 warns on `verify-ca` for MySQL/MariaDB; `connect.rs` names the re-check at each `mysql_async` upgrade (pinned by `verify_ca_by_an_unnamed_address_is_refused_like_verify_full`). | — |
 | DA-SRC-23 | 🆕 `mysql_common` reads MariaDB's compressed events as nothing | 🟡 | ✅ | §32. With `log_bin_compress` on, MariaDB writes `QUERY_COMPRESSED` and `*_ROWS_COMPRESSED_V1` events (165–168); `mysql_common` 0.37.3 returns no data for them, a silent loss (measured on 11.8 and 10.11). Closed by design (Q-M7a-5): the source decompresses them itself (M7a Task 6); the v2 types (169–171), never seen, halt. | — |
-| DA-SRC-21 | 🆕 MariaDB's file/position identity is `@@server_id` alone, and ids repeat across servers | 🟢 | 🟥 | §31.2. A pipeline in `binlog` mode pointed at another MariaDB with the same `server_id` resumes at a file and offset that mean nothing there, and BINLOG_SOURCE_CHANGED doesn't fire. Candidate check: compare `BINLOG_GTID_POS(file, pos)` with the GTID state recorded at the position. GTID mode (the default) doesn't depend on it. | — |
-| DA-SRC-24 | 🆕 A Galera node with `log_slave_updates` off isn't detected as a replica | 🟢 | 🟥 | §33.1. The binlog-coverage precondition (config §11.2) reads `SHOW ALL REPLICAS STATUS` when `log_slave_updates` is off; a Galera node applies its peers' writesets without a replica connection, so the status is empty and the check passes, while its binlog lacks the other nodes' changes. Check `wsrep_on` too. | — |
-| DA-SRC-25 | 🆕 A case-only column rename reads as a rename | 🟢 | 🟥 | §33.1. Column names compare without case (as MySQL's do), so a column whose name changed in case alone diffs as a proven `RenameColumn`; the Delta sink doesn't apply renames, so the table halts or degrades for what is, to MySQL, the same column. Candidate: a case-only rename updates the cached name without an evolution. | — |
-| DA-SRC-26 | 🆕 A MariaDB rollback after tracked rows halts the pipeline | 🟢 | 🟥 | §33.1. MariaDB logs the rows a rollback undid in a transaction with non-transactional changes, then `ROLLBACK TO` or `ROLLBACK` (measured, 10.11–11.8); M7a halts on one with tracked rows, since the stream already emitted them. Fail-closed, but a false halt for a tracked MyISAM table (whose rows stand) and an outage for an application using savepoints across engines. Candidate: hold a transaction's rows until its end, drop those after the savepoint, keep a non-transactional table's. | — |
+| DA-SRC-21 | 🆕 MariaDB's file/position identity is `@@server_id` alone, and ids repeat across servers | 🟢 | ✅ | §31.2. A pipeline in `binlog` mode pointed at another MariaDB with the same `server_id` resumes at a file and offset that mean nothing there, and BINLOG_SOURCE_CHANGED doesn't fire. Candidate check: compare `BINLOG_GTID_POS(file, pos)` with the GTID state recorded at the position. GTID mode (the default) doesn't depend on it. **✅ 2026-10-30 (§53):** `start.rs` `at_boundary` (`BINLOG_GTID_POS`), at the start and at the dump's open; IT `a_mariadb_position_off_an_event_boundary_is_another_server_s`. | — |
+| DA-SRC-24 | 🆕 A Galera node with `log_slave_updates` off isn't detected as a replica | 🟢 | ✅ | §33.1. The binlog-coverage precondition (config §11.2) reads `SHOW ALL REPLICAS STATUS` when `log_slave_updates` is off; a Galera node applies its peers' writesets without a replica connection, so the status is empty and the check passes, while its binlog lacks the other nodes' changes. Check `wsrep_on` too. **✅ 2026-10-30 (§53):** `server.rs` `check_coverage` refuses a Galera node without `log_slave_updates`. | — |
+| DA-SRC-25 | 🆕 A case-only column rename reads as a rename | 🟢 | ✅ | §33.1. Column names compare without case (as MySQL's do), so a column whose name changed in case alone diffs as a proven `RenameColumn`; the Delta sink doesn't apply renames, so the table halts or degrades for what is, to MySQL, the same column. Candidate: a case-only rename updates the cached name without an evolution. **✅ 2026-10-30 (§53):** `schema.rs` `keep_names`; IT `a_change_of_case_alone_keeps_the_column_s_name`. | — |
+| DA-SRC-26 | 🆕 A MariaDB rollback after tracked rows halts the pipeline | 🟢 | ✅ | §33.1. MariaDB logs the rows a rollback undid in a transaction with non-transactional changes, then `ROLLBACK TO` or `ROLLBACK` (measured, 10.11–11.8); M7a halts on one with tracked rows, since the stream already emitted them. Fail-closed, but a false halt for a tracked MyISAM table (whose rows stand) and an outage for an application using savepoints across engines. Candidate: hold a transaction's rows until its end, drop those after the savepoint, keep a non-transactional table's. **✅ 2026-10-30 (§53):** `stream.rs` `Held` (savepoints), `roll_back`; IT `a_rollback_drops_the_rows_it_undid`. | — |
 | DA-SRC-27 | A MySQL stream ended once with "Packets out of sync" | 🟢 | ✅ | ✅ 2026-10-20 (§35): the candidate cause was real and is fixed. `Connector::command` now takes the catalog connection out of its slot for a call and puts it back only when the call completes, so a call dropped mid-query drops its connection; a transient error names the call. The likeliest trigger was the session's `select!` dropping `MySqlStream::next()` during a DDL's catalog read. An IT drops a call mid-`SLEEP(3)`: on the old code the next query waited 2.8 s for the abandoned reply, now it answers at once (`connect.rs` `a_call_dropped_mid_query_does_not_poison_the_next`). That this was the observed occurrence is inferred, not reproduced; reopen if CI logs it again. The original record: in M7b Task 7's E2E run on `mysql-84` the worker logged `reconnecting to the source: Packets out of sync` about 3 s after the pipeline started (`mysql_async`'s `PacketOutOfSync`: a connection read a reply it didn't expect). The reconnect recovered and nothing was lost. The log doesn't name the failing call, and the E2E harness prints a test's log only when it fails, so the frequency is unknown. A candidate cause, unproven: `Connector::command` keeps the shared catalog connection in its slot while a query runs, so a caller dropped mid-query (the session's heartbeat read, aborted when a session ends; an aborted chunk read) leaves a connection with an unread reply for the next query. It doesn't obviously fit this case (no earlier session had ended in the process). Next: name the call in the reconnect's log; take the connection out of the slot for a call and put it back only when the call completes | — |
 | DA-SRC-28 | A takeover after a pause can't stream on PostgreSQL until the old walsender times out | 🟢 | ✅ | ✅ 2026-10-22 (§37): on the first busy attempt of a spell, a worker that holds its lease ends the slot's walsender of the pipeline's own role (`pg_terminate_backend(pid, 5000)`), then retries from the first backoff step. At most once a lease term, so two holders that each believe they hold the lease (two lease stores on one slot) can't end each other in turn: the later spell gives up with `Contended`. Another role's reader is never ended. The E2E pause scenario's wait went from 150 s to the suite's 60 s (4 scenarios: 189 s → 141 s). M8a Task 13 (D-M8a.22) | — |
-| DA-SRC-29 | 🆕 `BINLOG_SOURCE_CHANGED` without GTIDs has no in-place recovery | 🟢 | 🟥 | §49.1. A pipeline in `error` keeps its lease and its desired state `running`, so a snapshot job is accepted (`snapshots.rs:150-157`) and its restart clears the error; `same_server` then halts it again with `BINLOG_SOURCE_CHANGED`, and doesn't follow `binlog_purge_action`. The docs: `gtid` with `re_snapshot`, or a new pipeline. | — |
-| DA-SRC-30 | 🆕 Under `slot_management: require_existing` a lost PostgreSQL slot has no in-place recovery | 🟢 | 🟥 | §50.1. A slot the DBA creates again is past the pipeline's checkpoint and refused (`slot.rs`); the refusal's text says otherwise. The docs: drop the slot, switch to `managed` in one reload, and set `require_existing` back (`runbooks/position-lost.mdx`). | — |
+| DA-SRC-29 | 🆕 `BINLOG_SOURCE_CHANGED` without GTIDs has no in-place recovery | 🟢 | ✅ | §49.1. A pipeline in `error` keeps its lease and its desired state `running`, so a snapshot job is accepted (`snapshots.rs:150-157`) and its restart clears the error; `same_server` then halts it again with `BINLOG_SOURCE_CHANGED`, and doesn't follow `binlog_purge_action`. The docs: `gtid` with `re_snapshot`, or a new pipeline. **✅ 2026-10-30 (§53):** `start.rs` `MySqlConnector::changed` follows `binlog_purge_action`; IT `a_checkpoint_from_another_server`. | — |
+| DA-SRC-30 | 🆕 Under `slot_management: require_existing` a lost PostgreSQL slot has no in-place recovery | 🟢 | ✅ | §50.1. A slot the DBA creates again is past the pipeline's checkpoint and refused (`slot.rs`); the refusal's text says otherwise. The docs: drop the slot, switch to `managed` in one reload, and set `require_existing` back (`runbooks/position-lost.mdx`). **✅ 2026-10-30 (§53):** `slot.rs` `decide`: adopted under `slot_on_orphan: adopt_resnapshot`, else refused with the key named; IT `require_existing_never_creates`. | — |
 
 ---
 
@@ -876,7 +905,7 @@ pinning is the remaining cross-cutting concern.
 | DA-OPS-10 | 🆕 An `id` change that keeps its resources is sequenced only within one reload, on the nodes that applied it | 🟢 | 🔄 | §39.1. M8b's whole-branch review: an added pipeline sharing a removed one's slot, publication, sink or state waits for the removed one's lease. A node that starts during the wait, a removal and an add in two reloads, a cut-off drain, or the old id reused while the removal drains let it start early. **🔄 2026-10-30 (§50):** `configuration/overview.md` says to wait for the old lease's release, or to do both in one reload; the tombstone stays. | — |
 | DA-OPS-11 | 🆕 The release workflow's first full run is the first tag's | 🟢 | ✅ | §46.1. `release.yml` (M11a) runs on a `v*` tag or by hand, and only from `main`: before M12's `v1.0.0` only its builds are proven. The first push creates the GHCR packages private. Next: a manual run on `main` after M11a merges; the packages made public after the first tag §47: the repository is private, so the attestation is skipped (BuildKit's in-registry provenance and the SBOM stay) and the `ubuntu-24.04-arm` runner is unproven until `weekly.yml` is dispatched. Oleksii's ruling 2026-10-29: the repository goes public (done, after PR #17's README). §48: the manual run on `main` green (run 40918381277). Next: after the first tag, make the image and chart packages public. **✅ 2026-10-30 (§51):** `v0.1.0` released by `release.yml` (run 41034298202), its image attested, the image and chart packages public | — |
 | DA-OPS-12 | 🆕 A process-restart key written as a literal can't change once a generation exists | 🟢 | 🟥 | §49.1. A reload refuses it (V-061) and a restarted node adopts the stored generation; only `${VAR}` values and file contents change, by a rolling restart. The docs give the workaround. | — |
-| DA-OPS-13 | 🆕 Two operator-facing texts are wrong (the orphan-slot refusal's key, the CLI's `about`) | 🟢 | 🟥 | §49.1. `slot.on_orphan` for `slot_on_orphan`; MySQL and MariaDB missing from the description. | — |
+| DA-OPS-13 | 🆕 Two operator-facing texts are wrong (the orphan-slot refusal's key, the CLI's `about`) | 🟢 | ✅ | §49.1. `slot.on_orphan` for `slot_on_orphan`; MySQL and MariaDB missing from the description. **✅ 2026-10-30 (§53):** `slot.rs` names `slot_on_orphan`; `cli.rs` `about` names the three sources (`the_help_names_every_source`). | — |
 | DA-OPS-9 | 🆕 A pipeline removed while no node holds its lease isn't torn down | 🟢 | 🟥 | §38.1. Q-M8b-3: the owner drains, tears down (`auto_cleanup`) and releases. With no owner at the removal (it is down, or a hand-off is between release and claim), every node's controller is on standby and just ends, so the PostgreSQL slot and publication stay and the slot keeps WAL (bounded by `max_slot_wal_keep_size`). Config §12.3's "orphan-cleanup … whose owner was offline" design item. | — |
 | DA-OPS-8 | Per-node literals in a node's own file are ignored once a generation exists | 🟢 | ✅ | §36.1, §38. A node runs the cluster's generation resolved with its own environment (D-3); per-node values go through `${VAR}` (config §6.5.1). M8b warns at startup naming each per-node key that differs, and which value runs; the operator docs stay with M11. **✅ 2026-10-30 (§50):** the operator docs say it, in `configuration/cluster.md` and `runbooks/config-changes.md`: the gossip addresses (`bind_addr`, `bind_port`, `advertise_addr`, `advertise_port`) are the generation's, so a literal there is ignored with a warning; `node_name` and `api.address` are the node's file's. | — |
 | DA-OPS-7 | A controller task that panics goes unnoticed | 🟢 | ✅ | ✅ 2026-10-22 (§37): the reconciler supervises its controllers. One that ended unasked (a panic, a return) is logged and started again on its status channel, and a dropped reconciler counts as a crash; the owned-pipelines gauge is held by a guard, so a panic doesn't over-count it. Readiness after a controller panic is not in M8a (DA-OPS-7's other half: M10). M8a Task 6 (D-M8a.11, `a_controller_that_panics_is_started_again`) | — |
@@ -1933,12 +1962,12 @@ items it carried. They are mirrored in §10 and §14.
 | ID | Title | Sev | Status | Note |
 |---|---|---|---|---|
 | DA-SNAP-9 | The snapshot watermark can't order a chunk's MVCC snapshot against the stream | 🟡 | ✅ (§28) | `pg_current_wal_lsn()` watermarks miss a commit whose WAL precedes `lw` but which isn't visible to the chunk yet (synchronous replication widens it): a stale chunk row can overwrite a newer streamed change. Fix in M6: watermarks from `pg_current_snapshot()` in `Chunk`, the window by xid; amend Snapshot_Resumability §1. Reasoned, not measured. |
-| DA-SRC-13 | Publication blind spots | 🟢 | 🆕 🟥 | Unchecked at reconcile: PG15+ row filters and column lists (a column list makes channel C halt at every signal), PG18 `publish_generated_columns`, extra tables under `require_existing` / `FOR ALL TABLES` (D-M5.14 says warn), a managed `DROP TABLE` of a `TABLES IN SCHEMA` member (42704), `via_root` refused when only the leaf is published, the old publication after a rename. |
-| DA-SRC-14 | `bit` / `varbit` / PostGIS keys | 🟢 | 🆕 🟥 | `text_of` isn't valid input for those types, so a snapshot's second chunk fails `Permanent`. Refuse such keys at resolution, or render them. |
-| DA-SRC-15 | Snapshot collation and chunk edges | 🟢 | 🆕 🟥 | A domain with its own collation in a column declared with another (42P22 from the second chunk); the cursor's collation name isn't schema-qualified; a `COLLATE "C"` identity index on a non-C column isn't warned; no byte budget per chunk; `limit` 0 reads as the end; no `E''` literals. |
+| DA-SRC-13 | Publication blind spots | 🟢 | ✅ | Unchecked at reconcile: PG15+ row filters and column lists (a column list makes channel C halt at every signal), PG18 `publish_generated_columns`, extra tables under `require_existing` / `FOR ALL TABLES` (D-M5.14 says warn), a managed `DROP TABLE` of a `TABLES IN SCHEMA` member (42704), `via_root` refused when only the leaf is published, the old publication after a rename. **✅ 2026-10-30** (§53). |
+| DA-SRC-14 | `bit` / `varbit` / PostGIS keys | 🟢 | ✅ | `text_of` isn't valid input for those types, so a snapshot's second chunk fails `Permanent`. Refuse such keys at resolution, or render them. **✅ 2026-10-30** (§53). |
+| DA-SRC-15 | Snapshot collation and chunk edges | 🟢 | ✅ | A domain with its own collation in a column declared with another (42P22 from the second chunk); the cursor's collation name isn't schema-qualified; a `COLLATE "C"` identity index on a non-C column isn't warned; no byte budget per chunk; `limit` 0 reads as the end; no `E''` literals. **✅ 2026-10-30** (§53). |
 | DA-SRC-16 | The replication connection's timeouts | 🟢 | ✅ | `pgwire-replication` sets no TCP keep-alive, connect timeout or idle deadline: a black-holed peer stalls `next` until the kernel gives up. M6: a watchdog of a few heartbeats; read-ahead is 8192 events, not bytes (`with_buffer_size`). | **✅ 2026-10-13** (§30, §14; this row updated 2026-10-30, §52): the stream watchdog and the bounded catalog connect (M6b Task 22). |
-| DA-SRC-17 | Source edges PostgreSQL doesn't produce, and small gaps | 🟢 | 🆕 🟥 | Crafted date/time text can overflow `i64`; loose parsing; hstore/PostGIS matched by name; no cycle guard in type planning; `convert` without a scale guard; a `string` override on a `"char"` above 127 halts on PG14 only; `char(n)` under `string` keeps its spaces; an interval's fields narrowed aren't seen; the catalog driver's errors and the server's detail aren't reported (no logging in the crate yet); a retry after dropping an invalidated orphan reports `FirstStart`; only the first V-113 error; a standby isn't refused by name; a glob table set `UNLOGGED` mid-resolve halts; `bounded` takes the connection-cache lock before its timer starts, so a call queued behind a reconnect waits longer than its budget (latency only). |
-| DA-SRC-18 | M5 test gaps | 🟢 | 🆕 🟥 | No unit test of the `ChainOnly` / `Unverified` slot verifiers; no `require`-mode integration test; no Delta sink test of a composite or `Date` key end to end; the `postgres-tls` compose service drifts from the `x-pg` anchor (`test-infra/compose.yaml`). |
+| DA-SRC-17 | Source edges PostgreSQL doesn't produce, and small gaps | 🟢 | ✅ | Crafted date/time text can overflow `i64`; loose parsing; hstore/PostGIS matched by name; no cycle guard in type planning; `convert` without a scale guard; a `string` override on a `"char"` above 127 halts on PG14 only; `char(n)` under `string` keeps its spaces; an interval's fields narrowed aren't seen; the catalog driver's errors and the server's detail aren't reported (no logging in the crate yet); a retry after dropping an invalidated orphan reports `FirstStart`; only the first V-113 error; a standby isn't refused by name; a glob table set `UNLOGGED` mid-resolve halts; `bounded` takes the connection-cache lock before its timer starts, so a call queued behind a reconnect waits longer than its budget (latency only). **✅ 2026-10-30** (§53). |
+| DA-SRC-18 | M5 test gaps | 🟢 | ✅ | No unit test of the `ChainOnly` / `Unverified` slot verifiers; no `require`-mode integration test; no Delta sink test of a composite or `Date` key end to end; the `postgres-tls` compose service drifts from the `x-pg` anchor (`test-infra/compose.yaml`). **✅ 2026-10-30** (§53). |
 
 ---
 
```


- [ ] **Step 3: Check the site and the gates**

Run: `cd docs && npm ci && npm run build`

Expected: `All internal links are valid.`, the build complete.

Run: `cargo test --workspace`

Expected: PASS, `xtask`'s docs test included.

- [ ] **Step 4: Commit**

```bash
git add doc/analysis/CDC_Design_Gap_Analysis_v1.md \
  docs/src/content/docs/configuration/sources.md \
  docs/src/content/docs/runbooks/position-lost.mdx \
  docs/src/content/docs/runbooks/source-privileges.mdx
git commit -m "docs: the M12a recoveries — a PostgreSQL failover, a DBA's slot made again, a renamed publication, a changed MySQL server, the publication's checks, a standby; gap: v1 §53 — the M12a source gaps closed"
```

---

## Exit

- [ ] Every task's Step 4 passes, and the whole E2E suite on PostgreSQL 18 (with the `replica`
  pair), `mysql-84`, `mysql-84-nogtid` and `mariadb-118`.
- [ ] The source ITs on all four default MySQL targets, and the TLS ITs from the repository's
  checkout.
- [ ] The whole-branch review's findings fixed or recorded; the PR's CI green.
- [ ] The tracker's §53 rows closed with pointers when M12a merges.

## Deviations during execution

- **Task 1** (review fix, `775163c`): Step 3's `worker.rs` block put `reposition` between `start`'s
  doc comment and `start`, so the doc went to `reposition`. The four doc lines are back above
  `pub(crate) async fn start`; nothing else moved. Task 8's `worker.rs` hunk is inside `start`,
  below both, and applies unchanged.
- **Task 2** (review fix, `2c031fe`): `reset_position` returned its new checkpoint without a
  server (`Checkpoint::from`), so after a mid-run `PositionUnreachable` the slot document lost
  its server until the next restart, and a failover in between would have been adopted.
  `PgConnector::reset_position` now returns a `Checkpoint` naming the server it read after
  recreating the slot, as MySQL's does; the lost-slot IT asserts it equals `prepare`'s.
  Later blocks in `slot.rs`, `tests/pg/slot.rs` and `connector.rs` don't overlap it.
- **Task 3** (review fix, `5d2c639`, `d5bb2b6`; approved by Oleksii 2026-11-03): two holes in
  D-M12a.4. (1) A file-mode checkpoint whose file wasn't in `SHOW BINARY LOGS` was always the
  purge's, so a rebuilt MariaDB (same `server_id`, same base name, numbering restarted) recovered
  as `PositionLost` without the marker reset, and the sink skipped its changes. Now a missing file
  with another base name, or after the server's newest binlog, is another server's
  (`never_held`, both flavours), and only one before the oldest is a purge. (2) The dump's open
  halted `BINLOG_SOURCE_CHANGED` whatever `binlog_purge_action` said, and a reconnect doesn't run
  `prepare`, so a failover found at a reconnect halted under `re_snapshot`. Now under
  `re_snapshot` the dump's open returns the new `SourceError::SourceChanged`, which
  `WorkerError::recovery` maps to `Retry`: the worker reruns with Phase 0, whose `prepare` plans
  the recovery. The re-snapshot warning no longer carries the halt's advice, and the list and
  boundary probe is one helper (`foreign_position`). New tests:
  `a_file_the_server_could_not_have_purged_is_another_server_s` and the dump's-open
  `re_snapshot` cases; source-mysql has 145 tests per target (the plan said 142). Task 6's
  `lib.rs` block now has `never_held` in its context line. Task 12's pages describe file mode
  with these two rules. The spec patch for Data-Flow Phase 2b and Source Disconnect, and for the
  Configuration `replication_type` row, is applied separately on approval.
- **Task 4** (review fix, `4f43600`): `reset_position`'s `require_existing` refusal, reached
  after a mid-run `PositionUnreachable`, still said "recreate it and restart" and didn't name
  `slot_on_orphan`. Under the default `halt`, following it met a second refusal. It now reads as
  `decide`'s `remake` text does. The made-again refusal says "(a slot made again, or moved by
  something else)". `slot_management`'s field doc says "adopted or refused", as the spec does.
  Doc comments use `slot_on_orphan`. The unit test pins four more cells: an invalidated slot
  under `adopt_resnapshot` is refused, a slot behind the checkpoint resumes, `managed` ignores
  `slot_on_orphan`, and a foreign plugin is refused. The IT writes once more after reading the
  slot, and asserts that the adoption advanced it. Tasks 6 and 9's blocks still apply.
- **Task 5** (review fixes `38e7a51`, `c3776e0`): three holes in D-M12a.6's hold.
  (1) A `TableHalt` among the undone items went with them. The source had already untracked
  the table, so its halt never reached the pipeline. A rollback now takes out only the
  `Change`s, and the `Begin` and every `TableHalt` stay held, in order.
  (2) A `ROLLBACK TO` a savepoint the hold doesn't know took out everything, including rows
  that stand. Planning measured that a savepoint before the first logged row is logged as a
  whole `ROLLBACK`. So an unknown name now halts (permanent, naming it), but only when the hold
  holds a change.
  (3) The hold counted binlog event bytes against `sink.max_transaction_buffer_bytes`, while it
  holds decoded items several times larger. `row_bytes` moved to core. The hold now counts it
  per held change, adding on hold and taking back on a rollback, as the accumulator counts the
  same key.
  Also: a savepoint name ending in an escaped backtick parses right, the engines query runs on
  MariaDB only, and a new IT, `a_rollback_past_the_hold_limit_halts`, covers the limit.
  source-mysql has 152 tests per target (the plan said 145).
