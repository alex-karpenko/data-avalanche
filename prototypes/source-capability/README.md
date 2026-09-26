# source-capability — replication client crates for the CDC sources

Companion to `lease-conformance/`, `iceberg-capability/` and `delta-capability/`. It answers, for pinned
crates, whether the **source side** can be built on them: gates **DA-SRC-10** (PostgreSQL pgoutput
client) and **DA-SRC-11** (MySQL / MariaDB binlog client) in
`doc/analysis/CDC_Design_Gap_Analysis_v1.md` §26. A negative result reshapes the v1 plan (fork or
write a protocol layer), so these run **before** implementation planning.

Status: **scaffolded 2026-09-26** — crate, containers and one smoke probe per candidate compile and
skip cleanly; nothing has run against a server yet (Docker was not running).

## Candidates (crates.io, 2026-09-26)

| Source | Crate | Version | Notes |
|---|---|---|---|
| PostgreSQL | `pgwire-replication` | 0.4.1 (2026-09-21) | Active. **Transport-level only** — see findings. Control plane via `tokio-postgres` 0.7. No maintained alternative on crates.io (upstream `tokio-postgres` has no replication stream). |
| MySQL (a) | `mysql-binlog-connector-rust` | 0.3.3 (2025-10-31) | Low activity; claims tests up to MySQL 8.0.31. |
| MySQL (b) | `mysql_async` binlog stream (`mysql_common` parser) | 0.37.1 (2026-09-01) | Very active general client with a binlog API. |

## Findings so far (source-read, not yet run)

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

## Gates

Pass criteria are what the CDC design needs (`Data-Flow.md` Phase 2a/2b, `Type_Mapping.md`,
`MySQL_DDL_Classifier.md`, `Large_Transaction_Handling.md`, `Postgres_Slot_and_Publication_Lifecycle.md`).

### PostgreSQL (DA-SRC-10) — pgwire-replication + own pgoutput decoder

| Gate | What | Pass | Status |
|---|---|---|---|
| S-PG0 | Smoke: slot + publication, stream from the slot LSN, one INSERT | `R` then `I` inside Begin/Commit; ack advances `confirmed_flush_lsn` | 🛠 written |
| S-PG1 | pgoutput decoder coverage | Relation, Insert, Update (`K`/`O` old tuple per REPLICA IDENTITY DEFAULT/FULL), Delete, Truncate, Type, Origin all decode | ⏳ |
| S-PG2 | Unchanged-TOAST marker | UPDATE not touching a TOASTed column yields `u` for it (drives the TOAST re-query path) | ⏳ |
| S-PG3 | Large transaction under proto v1 | txn ≫ `logical_decoding_work_mem` (64 kB in compose) arrives complete at commit; boundaries intact; client memory bounded by our buffer | ⏳ |
| S-PG4 | Restart / resume | ack, disconnect mid-stream, reconnect at `confirmed_flush_lsn` → nothing lost; un-acked txn replays whole (at-least-once) | ⏳ |
| S-PG5 | Slow consumer | consumer stalls > `wal_sender_timeout`; connection survives (keepalives) | ⏳ |
| S-PG6 | Publication change mid-stream | `ALTER PUBLICATION … ADD TABLE` takes effect without restarting the stream | ⏳ |
| S-PG7 | Type decode (text format) | every `Type_Mapping.md` PG row decodes to the expected `ColumnValue` | ⏳ |
| S-PG8 | TLS | `sslmode=require` against a TLS-enabled server | ⏳ (needs certs in compose) |

### MySQL / MariaDB (DA-SRC-11) — both candidates

| Gate | What | Pass | Status |
|---|---|---|---|
| S-MY0 | Smoke (a) and (b): GTID-positioned dump, one INSERT | TableMap + one WriteRows row for the table; MySQL 8.4 default auth works | 🛠 written |
| S-MY1 | Optional metadata | column names + UNSIGNED flags from `TABLE_MAP` with `binlog_row_metadata=FULL` | ⏳ |
| S-MY2 | Full row images | UPDATE carries before + after, DELETE carries the full before image | ⏳ |
| S-MY3 | DDL delivery | `QUERY_EVENT` carries the ALTER/CREATE/DROP/TRUNCATE text (classifier input) | ⏳ |
| S-MY4 | GTID resume | stop after N txns, reconnect with the executed set → no loss, no dup; XID boundaries | ⏳ |
| S-MY5 | Heartbeat + checksum | heartbeats while idle; CRC32 verified | ⏳ |
| S-MY6 | Binlog transaction compression | `binlog_transaction_compression=ON` payload decodes | ⏳ |
| S-MY7 | Type decode | every `Type_Mapping.md` MySQL row (DECIMAL, DATETIME(6), JSON, BIT, ENUM/SET, BLOB, TINYINT(1), BIGINT UNSIGNED) | ⏳ |
| S-MY8 | TLS | `require_secure_transport=ON` | ⏳ |
| S-MY9 | MariaDB 11.4 | MariaDB GTID (`domain-server-seq`) positioning + row events — **decides DA-SRC-8** | ⏳ |

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
