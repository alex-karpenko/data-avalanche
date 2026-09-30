# source-capability — replication client crates for the CDC sources

Companion to `lease-conformance/`, `iceberg-capability/` and `delta-capability/`. It answers, for pinned
crates, whether the **source side** can be built on them: gates **DA-SRC-10** (PostgreSQL pgoutput
client) and **DA-SRC-11** (MySQL / MariaDB binlog client) in
`doc/analysis/CDC_Design_Gap_Analysis_v1.md` §26. A negative result reshapes the v1 plan (fork or
write a protocol layer), so these run **before** implementation planning.

Status: **run 2026-09-26 — all gates green on the chosen crates.** PostgreSQL 17: S-PG0…S-PG8 9/9.
MySQL 8.4 + MariaDB 11.4: candidate (b) `mysql_async` 10/10; candidate (a) fails TLS and MariaDB GTID.

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

## Running

```bash
docker compose up -d --wait            # postgres:17 :55432, mysql:8.4 :53306, mariadb:11.4 :53307
cargo test                                                                   # default: no deps, instant
SRC_PROBE=1 cargo test --features pg    --test probe_pg    -- --nocapture --test-threads=1
SRC_PROBE=1 cargo test --features mysql --test probe_mysql -- --nocapture --test-threads=1
docker compose down -v
```

Without `SRC_PROBE` set (non-empty) every probe prints "skipped" and passes, so the crate stays green
in environments without Docker.
