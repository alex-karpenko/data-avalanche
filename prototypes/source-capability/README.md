# source-capability — replication client crates for the CDC sources

Companion to `lease-conformance/`, `iceberg-capability/` and `delta-capability/`. It answers, for pinned
crates, whether the **source side** can be built on them: gates **DA-SRC-10** (PostgreSQL pgoutput
client) and **DA-SRC-11** (MySQL / MariaDB binlog client) in
`doc/analysis/CDC_Design_Gap_Analysis_v1.md` §26. A negative result reshapes the v1 plan (fork or
write a protocol layer), so these run **before** implementation planning.

Status: **run 2026-09-26 — all gates green on the chosen crates.** PostgreSQL 17: S-PG0…S-PG8 9/9.
MySQL 8.4 + MariaDB 11.4: candidate (b) `mysql_async` 10/10; candidate (a) fails TLS and MariaDB GTID.
**M7 planning, 2026-10-14:** positions and the DA-SNAP-10 commit gap (S-MY10, S-MY11) on MySQL 8.4
(GTID on and off) and MariaDB 10.11 / 11.4 / 11.8: 11/11.

## Verdict

| Source | Pin | Our own code on top | Gate |
|---|---|---|---|
| PostgreSQL | `pgwire-replication =0.4.1` (+ `tokio-postgres` control plane) | **pgoutput decoder** (probe version ~150 lines: `tests/pgoutput/mod.rs`) | DA-SRC-10 ✅ |
| MySQL / MariaDB | `mysql_async =0.37.1` binlog stream (`mysql_common` 0.37.3) | MariaDB GTID event decode (13-byte prefix) + payload-decompression guard | DA-SRC-11 ✅ |
| — | reject `mysql-binlog-connector-rust` 0.3.3 | — | no TLS, no MariaDB GTID positioning, mislabeled signedness |

**MariaDB is feasible in v1** (DA-SRC-8): GTID-positioned resume works through `mysql_async` with the
MariaDB session variables, and per-transaction GTIDs decode exactly (last = `gtid_binlog_pos`).

## Candidates (crates.io, 2026-09-26)

| Source | Crate | Version | Notes |
|---|---|---|---|
| PostgreSQL | `pgwire-replication` | 0.4.1 (2026-09-21) | Active. **Transport-level only** — see findings. Control plane via `tokio-postgres` 0.7. No maintained alternative on crates.io (upstream `tokio-postgres` has no replication stream). |
| MySQL (a) | `mysql-binlog-connector-rust` | 0.3.3 (2025-10-31) | Low activity; claims tests up to MySQL 8.0.31. |
| MySQL (b) | `mysql_async` binlog stream (`mysql_common` parser) | 0.37.1 (2026-09-01) | Very active general client with a binlog API. |

## Findings (source-read, then confirmed by the runs)

1. **pgwire-replication does not decode pgoutput.** "Full pgoutput decoding into rows or events" is a
   stated non-goal; only Begin/Commit are parsed, every other message (Relation, Insert, Update,
   Delete, Truncate, Type, Origin) arrives raw in `XLogData.data`. **The pgoutput decoder is ours** —
   a bounded, well-specified work item (the protocol is documented message by message), but it is a
   v1 work item the plan must carry. What the crate does give us — explicit start LSN, standby status
   / flush-LSN ack, keepalives that survive a slow consumer, SCRAM, rustls TLS — is exactly the
   transport half.
2. **`proto_version '1'` is hard-coded** in `START_REPLICATION`, with no `streaming` option. Large
   transactions therefore arrive whole at commit (the server spills past `logical_decoding_work_mem`);
   nothing is streamed in progress. Functionally fine for the design (split-commit happens on our
   side), but `Data-Flow.md` Phase 2a says `proto_version '2'` — spec/crate mismatch to reconcile (a
   one-line upstream PR/fork makes it configurable; without `streaming` v1 and v2 carry the same
   messages).
3. **MySQL — neither candidate is complete on paper:**

| Need | (a) mysql-binlog-connector-rust | (b) mysql_async binlog |
|---|---|---|
| GTID-set positioning | ✅ `StartPosition::Gtid` | ✅ `with_gtid_set` |
| `caching_sha2_password` (MySQL 8.4 default) | ✅ | ✅ |
| TLS | ⚠️ looks absent (only an `ssl-mode=disabled` URL example) | ✅ rustls |
| `TABLE_MAP` optional metadata (names, signedness) | ✅ `table_metadata` | ✅ |
| Transaction payload (binlog compression) | ✅ | ✅ |
| MariaDB GTID events | ✅ event types present | ⚠️ not found in `mysql_common` |
| Tested against MySQL 8.4 | ❌ claims ≤ 8.0.31 | to probe |

### Findings from the runs

- **F-PG-ack — idle acks are late by default.** The worker checks `status_interval` only when a
  message arrives; while idle it sends feedback every `idle_wakeup_interval` (default **10 s**). Set
  `with_wakeup_interval` ≈ `status_interval`, or an ack made in a quiet period reaches the slot up
  to 10 s late (slot lag, not a correctness issue).
- **F-PG-session — text rendering follows the walsender session.** `timestamptz` / dates / intervals /
  floats are rendered with the *replication connection's* settings, not the writer's. Pin them via
  `with_options("-c TimeZone=UTC -c DateStyle=ISO -c IntervalStyle=iso_8601 -c extra_float_digits=3")`
  (S-PG7 proves the writer's `Europe/Kyiv` does not leak).
- **F-PG-toast** — confirmed: an UPDATE that leaves a TOASTed column untouched sends `u` (unchanged);
  the TOAST re-query path (DA-SRC-9) is needed.
- **F-PG-bigtxn** — proto v1: a 50k-row / ~10 MB transaction with `logical_decoding_work_mem=64kB`
  spilled server-side (`spill_txns=1`) and arrived whole in ~1.2 s; nothing streams in progress.
- **F-PG-pub** — `ALTER PUBLICATION … ADD TABLE` applies to a live stream (no restart), and rows
  written before the ADD are not sent — matches the publication-reconciliation design (DA-SRC-4).
- **F-MY-metadata** — both candidates expose names + UNSIGNED from `TABLE_MAP` optional metadata,
  but (a)'s field `is_signed` actually carries the **unsigned** bit (reading it as named inverts every
  numeric column), and (a) returns `BIGINT UNSIGNED` max as `-1`; (b) returns `18446744073709551615`.
- **F-MY-values (b)** — DECIMAL(38,10), DATETIME(6), TIME(6), JSON, utf8mb4, BLOB exact. ENUM → index,
  SET → bitmap bytes, BIT → bytes, TIMESTAMP → `epoch.micros` text: map ENUM/SET via the optional
  metadata string values (`binlog_row_metadata=FULL`).
- **F-MY-heartbeat (b)** — heartbeats need `SET @master_heartbeat_period = <ns>` on the connection
  before `get_binlog_stream` (no request option).
- **F-MY-mariadb (b)** — MariaDB GTID positioning via `@mariadb_slave_capability=4`,
  `@slave_connect_state`, `@slave_gtid_strict_mode=1`; `mysql_common` does not model the MariaDB
  `GTID_EVENT` (type 162) — decode `seq_no u64 LE, domain u32 LE` from `Event::data()` + header
  `server_id` ourselves.
- **F-MY-payload (b) — silent data-loss risk.** On a zstd error `danger_decompress` returns an
  **empty buffer**, and `mysql_async` ignores a payload-header parse error (`/* TODO: Log the error */`):
  that transaction's rows vanish without an error. Guard: validate every `TRANSACTION_PAYLOAD`
  ourselves (decompressed length == `uncompressed_size`, ≥ 1 inner event) and halt on mismatch; also
  worth an upstream fix. Tracked as DA-SRC-12.
- **F-TLS — two rustls crypto providers.** `pgwire-replication` enables `ring`, `mysql_async`
  enables `aws-lc-rs`; with both linked rustls cannot choose and **panics at the first TLS handshake**.
  The process must call `CryptoProvider::install_default()` once at startup (deltalake / object_store
  use rustls too). Found only when both features were built together.

### Findings from the M7 position probes (2026-10-14, S-MY10, S-MY11)

- **F-MY-status — the watermark statement differs.** MySQL 8.4 accepts only `SHOW BINARY LOG
  STATUS`. MariaDB 10.11 and 11.4 accept only `SHOW BINLOG STATUS` (11.8 accepts both;
  `SHOW MASTER STATUS` works on all three). Identity: MySQL `@@server_uuid`; MariaDB has no
  `server_uuid` (error 1193) and `server_uid` changes on restart, so its identity is `@@server_id`
  alone (every probe server has id 1, so that identity is weak). Flavour: `version()` contains
  `MariaDB`.
- **F-MY-dumpstart — what a file/position dump sends first.** A fake Rotate (`log_pos` 0,
  `next_pos` = the start) and the resent format description (`log_pos` 0); neither is a change. On
  MariaDB the fake Rotate's name carries the 4 CRC bytes (`mysql_common` parses it before it knows
  the checksum): strip them. Resuming at (file, end position of an XID) is exact on all five
  servers, across rotation and restart. After a restart MySQL's old file has no STOP event,
  MariaDB's has one (type 3).
- **F-MY-logpos — the header position is not always set.** `log_pos` is 0 for the events inside a
  MySQL compressed transaction payload (its XID included; the payload event carries the position),
  and for the Map/Rows events inside a MariaDB 11.4+ transaction (10.11 sets every event). A
  multi-row event has one position for all its rows. So the order key is computed: the header
  position where set, otherwise the last set one plus the sizes of the events since; a payload's
  inner events take the payload's. Measured: it equals the header wherever the header is set, and
  it is monotonic (MariaDB with `@mariadb_slave_capability=4` omits Annotate_rows events, so the
  computed key falls short of the next set one, still monotonic). Taking the header as is would put
  a compressed transaction's rows at offset 0, before earlier changes.
- **F-MY-purged — a purged start position.** File/position: error 1236 "Could not find first log
  file name in binary log index file", on the first read (the dump opens lazily). GTID: MySQL 1236
  "Cannot replicate because the source purged required binary logs…" (`@@gtid_purged` shows it);
  MariaDB 1236 "Could not find GTID state requested by slave in any binlog files…"
  (`BINLOG_GTID_POS(<first file>, 4)` shows it). MySQL also uses 1236 for the same-replica-id kick
  below, so a purge is told by a pre-check (`SHOW BINARY LOGS`, `@@gtid_purged`,
  `BINLOG_GTID_POS`) plus the message, never by the code alone. MariaDB defers a `PURGE` until its
  binlog checkpoint ("may be needed for crash recovery"): the probe retries for up to 10 s.
- **F-MY-heartbeat-pos** — heartbeats are type 27 (v1) on every server: the header `log_pos` is the
  current position and the body the file name, a keep-alive position in file/position mode.
  `mysql_common` has no heartbeat variant (raw type 27) and no `HEARTBEAT_V2` (type 41).
- **F-MY-replica-id — one dump per replica id.** A second dump under the same replica `server_id`
  ends the first (MySQL 1236 "A replica with the same server_uuid/server_id…", MariaDB 4052), so
  each pipeline registers its own id (`source.mysql.server_id`, V-120).
- **F-MY-snap10 — a commit the binlog delivers before it is visible (DA-SNAP-10).** With semi-sync
  AFTER_SYNC and the replica ack stalled 6 s (MySQL 8.4, MariaDB 10.11 and 11.8): the stream
  delivers the transaction in ~5 ms, a plain read misses it for the 6 s, and the status position
  moves past it. MySQL's `gtid_executed` excludes it, MariaDB's `gtid_binlog_pos` includes it. A
  `LOCK IN SHARE MODE` read in a READ COMMITTED transaction waits the ~6 s, then sees it (MariaDB
  11.8 rejects `FOR SHARE`). MariaDB's default wait point is AFTER_COMMIT, which has no gap. This is
  why MySQL snapshot chunks read with `LOCK IN SHARE MODE` (`Data-Flow.md` Phase 1).
- **F-MY-payload-order** — `mysql_async` yields a payload event, then its inner events (decompressed
  by `danger_decompress`, empty on error: F-MY-payload). The guard checks the payload before its
  inner events are used.

## Gates

Pass criteria are what the CDC design needs (`Data-Flow.md` Phase 2a/2b, `Type_Mapping.md`,
`MySQL_DDL_Classifier.md`, `Large_Transaction_Handling.md`, `Postgres_Slot_and_Publication_Lifecycle.md`).

### PostgreSQL (DA-SRC-10) — pgwire-replication + own pgoutput decoder

| Gate | What | Pass | Status |
|---|---|---|---|
| S-PG0 | Smoke: slot + publication, stream from the slot LSN, one INSERT | `R` then `I` inside Begin/Commit; ack advances `confirmed_flush_lsn` | ✅ |
| S-PG1 | pgoutput decoder coverage | Relation, Insert, Update (`K`/`O` old tuple per REPLICA IDENTITY DEFAULT/FULL), Delete, Truncate, Type, Origin all decode | ✅ |
| S-PG2 | Unchanged-TOAST marker | UPDATE not touching a TOASTed column yields `u` for it (drives the TOAST re-query path) | ✅ |
| S-PG3 | Large transaction under proto v1 | txn ≫ `logical_decoding_work_mem` (64 kB in compose) arrives complete at commit; boundaries intact; client memory bounded by our buffer | ✅ |
| S-PG4 | Restart / resume | ack, disconnect mid-stream, reconnect at `confirmed_flush_lsn` → nothing lost; un-acked txn replays whole (at-least-once) | ✅ |
| S-PG5 | Slow consumer | consumer stalls > `wal_sender_timeout`; connection survives (keepalives) | ✅ |
| S-PG6 | Publication change mid-stream | `ALTER PUBLICATION … ADD TABLE` takes effect without restarting the stream | ✅ |
| S-PG7 | Type decode (text format) | every `Type_Mapping.md` PG row decodes to the expected `ColumnValue` | ✅ |
| S-PG8 | TLS | `sslmode=require` against a TLS-enabled server | ✅ TLS 1.3, verify-full (throwaway CA in compose) |

### MySQL / MariaDB (DA-SRC-11) — both candidates

| Gate | What | Pass | Status |
|---|---|---|---|
| S-MY0 | Smoke (a) and (b): GTID-positioned dump, one INSERT | TableMap + one WriteRows row for the table; MySQL 8.4 default auth works | (a) ✅ (b) ✅ |
| S-MY1 | Optional metadata | column names + UNSIGNED flags from `TABLE_MAP` with `binlog_row_metadata=FULL` | (a) ⚠️ data present, `is_signed` inverted · (b) ✅ |
| S-MY2 | Full row images | UPDATE carries before + after, DELETE carries the full before image | (a) ✅ (b) ✅ |
| S-MY3 | DDL delivery | `QUERY_EVENT` carries the ALTER/CREATE/DROP/TRUNCATE text (classifier input) | (a) ✅ (b) ✅ (DROP is server-rewritten: ``DROP TABLE `t` /* generated by server */``) |
| S-MY4 | GTID resume | stop after N txns, reconnect with the executed set → no loss, no dup; XID boundaries | (a) ✅ (b) ✅ |
| S-MY5 | Heartbeat + checksum | heartbeats while idle; events parse with `binlog_checksum=CRC32` (8.4 default) — detection of a *corrupted* checksum not tested | (a) ✅ (b) ✅ |
| S-MY6 | Binlog transaction compression | `binlog_transaction_compression=ON` payload decodes | (a) ✅ (b) ✅ |
| S-MY7 | Type decode | every `Type_Mapping.md` MySQL row (DECIMAL, DATETIME(6), JSON, BIT, ENUM/SET, BLOB, TINYINT(1), BIGINT UNSIGNED) | (a) ⚠️ BIGINT UNSIGNED as `-1` · (b) ✅ |
| S-MY8 | TLS | a `REQUIRE SSL` account streams | (a) ❌ no TLS (access denied) · (b) ✅ TLS 1.3 |
| S-MY9 | MariaDB 11.4 | MariaDB GTID (`domain-server-seq`) positioning + row events — **decides DA-SRC-8** | (a) ❌ GTID (file/pos only) · (b) ✅ GTID resume + per-txn GTIDs |

### MySQL / MariaDB positions (M7 planning, 2026-10-14) — `mysql_async` only

`tests/probe_mysql_position.rs`, run on MySQL 8.4 with GTID on and off, and MariaDB 10.11, 11.4 and
11.8: 11/11 green. The findings are under "Findings from the M7 position probes" below.

| Gate | What | Pass | Status |
|---|---|---|---|
| S-MY10a | Watermark, identity, flavour | the status statement each server accepts; a stable server identity; flavour from `version()` | ✅ (F-MY-status) |
| S-MY10b | File/position resume | resume at (file, end position of an XID) → no loss, no duplicate | ✅ all five |
| S-MY10c | Rotation | resume across `FLUSH BINARY LOGS`; the Rotate events say where | ✅ |
| S-MY10d | Server restart | identity unchanged; resume at the old position reaches the new file | ✅ |
| S-MY10e | Purged file | a position in a purged file fails, and how | ✅ error 1236 on the first read (F-MY-purged) |
| S-MY10f | Positions inside a transaction | an order key per row that is strictly monotonic, compressed payloads included | ✅ computed (F-MY-logpos) |
| S-MY10g | Heartbeats | the heartbeat carries the current file and position | ✅ |
| S-MY10h | Two dumps under one replica id | what happens to the first | ✅ the second ends the first (F-MY-replica-id) |
| S-MY10i | Order key in GTID dumps | the computed key stays monotonic when the dump skips transactions | ✅ |
| S-MY10k | GTID position purged | the GTID dump fails, and how | ✅ error 1236 (F-MY-purged) |
| S-MY11 | A commit delivered before it is visible (DA-SNAP-10) | semi-sync AFTER_SYNC stalled 6 s: what the stream, a plain read and a locking read see | ✅ measured (F-MY-snap10) |

## Running

```bash
docker compose up -d --wait            # postgres:17 :55432, mysql:8.4 :53306, mariadb:11.4 :53307,
                                       # mysql:8.4 GTID off :53308, mariadb:11.8 :53309, mariadb:10.11 :53310
cargo test                                                                   # default: no deps, instant
SRC_PROBE=1 cargo test --features pg    --test probe_pg    -- --nocapture --test-threads=1
SRC_PROBE=1 cargo test --features mysql --test probe_mysql -- --nocapture --test-threads=1
SRC_PROBE=1 cargo test --features mysql --test probe_mysql_position -- --nocapture --test-threads=1
docker compose down -v
```

Without `SRC_PROBE` set (non-empty) every probe prints "skipped" and passes, so the crate stays green
in environments without Docker.
