---
updated: 2026-06-20
domain: Data Flow
tags: [snapshot, resumability, incremental-snapshot, watermark, keyset, consistency, on-demand, cdc]
resolves: [G-6, G-7, G-20]
---

# Snapshot Resumability and Consistency (Incremental Chunked Model)

## Scope

Closes **G-6** (resumability cursor format), **G-7** (multi-table consistency toggle/fallback) and
**G-20** (on-demand snapshot read point) together, because a single change — moving from a *blocking
consistent snapshot* to an *incremental chunked snapshot* — resolves all three.

Builds on `Ghost_Row_Dedup_and_Primary_Keys.md` (G-13, identifier + upsert idempotency) and
`Delivery_Guarantee_and_Position_Guard.md` (G-14, `SourcePosition`). It **revises Data-Flow.md
Phase 1**.

---

## §0 — The contradiction in the current design

Data-Flow Phase 1 is a **blocking consistent snapshot**: one `REPEATABLE READ` transaction,
`SET TRANSACTION SNAPSHOT` (PG) / `START TRANSACTION WITH CONSISTENT SNAPSHOT` (MySQL),
`COPY ... TO STDOUT`, every row read at `start_lsn`. But it *also* claims a per-page cursor that
"enables resume on crash without full re-scan." **These are mutually exclusive:**

- The snapshot transaction dies with the worker; you cannot re-attach to an exported snapshot. A
  resumed read opens a **new** transaction at a **later** point, so resumed rows are read at a
  different consistent point than pre-crash rows — the "no gap, no overlap" guarantee breaks.
- `COPY` is a single streaming statement with no `WHERE`/cursor — it cannot resume mid-table at all.

So resumability and a single frozen consistent point cannot coexist in the blocking model. We choose
resumability and obtain consistency a different way.

---

## §1 — The model: incremental chunked snapshot with position watermarks

The snapshot proceeds in **chunks** keyed by the table's G-13 identifier, **interleaved with live
streaming**, and reconciled by **position watermarks + PK-upsert idempotency** rather than a lock.
This is the DBLog / Debezium-incremental-snapshot technique, adapted to our read-only replication user
and our upsert sink.

Order of operations flips from "snapshot then stream" to **"stream first, snapshot concurrently":**

1. **Establish the stream.** Create the PG logical slot (capturing `start_lsn` and beginning WAL
   retention) / position the MySQL binlog stream at `gtid_executed`. Streaming begins immediately and
   runs throughout the snapshot.
2. **Chunk loop**, per table, ordered by the identifier:
   a. **Low watermark** `lw` = current stream position (`pg_current_wal_lsn()` /
      `@@gtid_executed`) — read, no write to the source.
   b. **Read the chunk** — `SELECT ... WHERE (key) > (cursor) ORDER BY (key) LIMIT chunk_size` in a
      short `READ COMMITTED` transaction. Buffer the rows.
   c. **High watermark** `hw` = current stream position.
   d. **Reconcile.** As the stream is consumed from `lw` to `hw`, any streamed change whose identifier
      falls in the buffered chunk **removes that key from the buffer** (the live change supersedes the
      snapshot read). After `hw`, emit the surviving buffered rows as snapshot inserts (`_op = r`).
   e. **Checkpoint** the cursor (last row's identifier tuple) to state storage.
3. When the last chunk completes, the table is fully captured; streaming simply continues.

Because every table is PK-keyed and written as an **upsert with keep-last-by-`_seq`** (G-13), the
watermark step is belt-and-suspenders: even a snapshot row that races a concurrent change converges,
since the streamed change carries a higher `_seq` and wins. **No global read lock, no long-held
transaction.**

---

## §2 — Cursor format (D-6.2, resolves G-6)

The resume cursor is an **ordered, typed tuple** of the identifier columns of the last row of the last
committed chunk:

```json
{
  "key":        [ {"col": "tenant_id", "val": 42,       "type": "int8"},
                  {"col": "id",        "val": "9f1c...", "type": "uuid"} ],
  "chunk_size": 10000,
  "rows_done":  4530000
}
```

Pagination is a **row-value comparison**, supported by both engines:
`WHERE (c1, c2, …) > (v1, v2, …) ORDER BY c1, c2, … LIMIT chunk_size`.

**The "no suitable cursor key" fallback is moot.** Per G-13 every replicated table has an identifier
that is top-level, primitive, NOT NULL, and non-floating — i.e. always a valid, totally-ordered
keyset key. PK-less tables already halt and never snapshot.

### Collation pin (subtle, load-bearing)

For text/`varchar` identifier columns, the `ORDER BY` and the cursor `>` comparison **must use the
same deterministic collation**, or the server's ordering and our resume comparison disagree and rows
are silently skipped or duplicated. Keyset ordering therefore pins to a **binary collation**
(`COLLATE "C"` in PostgreSQL, `_bin` / `utf8mb4_bin` in MySQL) for the pagination predicate,
independent of the column's display collation. The pinned collation is recorded in the cursor.

---

## §3 — Consistency without a lock (resolves G-7)

The blocking model needed `FLUSH TABLES WITH READ LOCK` (MySQL `RELOAD`/`LOCK TABLES` privilege) or
`EXPORT_SNAPSHOT` to freeze all tables at one point. The incremental model **removes that
requirement entirely**:

- **No global lock, no `RELOAD`/`LOCK TABLES` privilege** — only `SELECT` + the existing replication
  privileges. This deletes the G-7 "MySQL privilege requirement" and "fallback when consistency can't
  be achieved" concerns: there is no global-lock failure mode to fall back from.
- **Multi-table consistency** is no longer a frozen instant; it is **convergent** — each table's
  chunks reconcile against the stream, and the stream delivers all post-`start` changes in `_seq`
  order. The system reaches a consistent state once snapshots complete and the stream catches up.
- The **`consistent_snapshot` toggle is not introduced** — incremental is the single model; there is
  no weaker mode to opt out to. (A strict point-in-time export is explicitly out of scope; if ever
  required it would be a separate, lock-based mode.)

Trade-off stated honestly: a reader querying Iceberg **mid-snapshot** sees a partially-populated table
and no cross-table atomic instant. This is the same per-table-not-cross-table boundary as
`Delivery_Guarantee_and_Position_Guard.md` §5, and is acceptable for a CDC mirror.

---

## §4 — On-demand snapshot (resolves G-20)

The current on-demand flow (Data-Flow §"On-Demand Snapshot") pauses the stream, captures `pause_lsn`,
runs a blocking snapshot, and resumes from `pause_lsn + 1` — which **assumes** the snapshot's read
point equals `pause_lsn`, with a gap window between pausing and opening the snapshot transaction.

Under the incremental model this hazard **disappears**: an on-demand snapshot is just the §1 chunk
loop run **without pausing the stream**. Watermarks reconcile the re-snapshotted rows against
concurrent changes; the stream position never has to be "assumed." This also makes
**re-snapshot-after-unsafe-DDL** (schema-evolution §7.2) and ad-hoc snapshots use the identical
mechanism. Idempotent upserts mean re-reading existing keys simply re-writes the same rows.

---

## §5 — Chunking, checkpointing, parallelism (D-6.3)

- **`snapshot.chunk_size`** — rows per chunk / keyset window. Default **10000**. One small state-store
  PUT per chunk checkpoint. Smaller = more frequent checkpoints, less crash re-work, more overhead.
- **Checkpoint granularity = per chunk** (the keyset boundary), not per row.
- **Parallelism** reuses the existing `parallelism` field (snapshot connection budget already
  documented, ≥3 connections per PG job). Each parallel worker owns disjoint tables; per-table chunk
  ordering is serial within a table (keyset is inherently sequential).
- **Crash resume** = reopen from the checkpointed cursor; re-read of the in-flight chunk is harmless
  (upsert idempotency).

---

## §6 — Engine specifics

| Concern | PostgreSQL | MySQL |
|---|---|---|
| Stream start | logical slot `start_lsn` | binlog at `gtid_executed` |
| Low/high watermark | `pg_current_wal_lsn()` | `@@gtid_executed` (or `SHOW MASTER STATUS`) |
| Chunk read isolation | `READ COMMITTED` short txn | `READ COMMITTED` short txn |
| Privileges removed | `EXPORT_SNAPSHOT` no longer load-bearing for snapshot consistency | **`RELOAD` / `LOCK TABLES` no longer needed** |
| Long-txn / bloat risk | eliminated (short per-chunk txns) | eliminated |

No signal/watermark table is written on the source — watermarks are read-only stream positions, so the
replication user needs no write access.

---

## §7 — Config & doc impact

**Config**

- Add **`snapshot.chunk_size`** (R, default 10000; validate ≥ 1, warn if very large).
- No `consistent_snapshot` toggle (§3).

**Data-Flow.md edits owed** (spec, propose-only)

- Phase 1 rewritten from blocking COPY/EXPORT_SNAPSHOT to the §1 chunk loop; the order becomes
  stream-first, snapshot-concurrent.
- On-Demand Snapshot section: drop "pause / `pause_lsn + 1`"; replace with the watermark chunk loop.
- Privilege/preconditions: remove MySQL `RELOAD`/`LOCK TABLES` from snapshot requirements.

**Cross-refs:** G-13 (identifier/upsert), G-14 (`SourcePosition`, per-table HWM), G-26/G-22 (binlog/WAL
retained long enough to cover a long snapshot — a slow snapshot lengthens the stream backlog; note the
WAL-lag guard interaction).

---

## §8 — Resolved decision summary

| Decision | Resolution |
|---|---|
| D-6.1 model | **Incremental chunked** snapshot, stream-first, watermark + upsert reconciliation |
| D-6.2 cursor | Typed identifier tuple; row-value keyset; **binary-collation** ordering pin; no-key fallback moot (G-13) |
| D-6.3 chunking | `snapshot.chunk_size` default 10000; checkpoint per chunk; resume is idempotent |
| G-7 consistency | Lock-free, convergent; no `RELOAD`/`LOCK TABLES`; no `consistent_snapshot` toggle |
| G-20 on-demand | Watermark chunk loop, **no stream pause**, no read-point assumption |
