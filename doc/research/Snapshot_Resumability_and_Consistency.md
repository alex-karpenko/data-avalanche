---
updated: 2026-10-14
domain: Data Flow
tags: [snapshot, resumability, incremental-snapshot, watermark, keyset, consistency, on-demand, cdc]
resolves: [G-6, G-7, G-20, DA-SNAP-9, DA-SNAP-10]
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

Because every table is PK-keyed and written as an **upsert with keep-last-by-`_seq`** (G-13), a
snapshot row that races a concurrent change **in the same flush** converges: the streamed change
carries a higher `_seq` and wins. Across flushes it doesn't: a chunk row committed after the
stream's change overwrites it. Steps 2a–2d above don't close that window; §1.1 replaces them.
**No global read lock, no long-held transaction.**

### §1.1 — The merge window, as built (DA-SNAP-9; M6a, 2026-10-03)

A WAL position doesn't say what a chunk's read saw. Two cases break the `lw`/`hw` reconciliation:

- A transaction commits while the chunk is being read. The stream may decode and apply it before
  the chunk is emitted, while the read didn't see it.
- A **synchronous commit** waiting for its standby is in the WAL, so the stream already delivers
  it. It is not yet visible to any snapshot, so a read issued later still doesn't see it. Measured
  on PostgreSQL 18 (`data-avalanche-source-pg`'s `tests/sync_commit`, M6a Task 2).

The chunk loop the pipeline runs instead (Data-Flow Phase 1 step 2, spec edit S-T):

1. **Writers in flight at the session's start.** Their commits may precede the stream's start yet
   be invisible to a read. A table's chunks are read only once none of its writers listed by
   `in_flight()` (PG: `pg_locks`) is still in flight, re-checked with backoff 0.5 s → 10 s (Q-M6a-2).
2. **The read records what it saw.** Each chunk is read in a short REPEATABLE READ transaction that
   first takes `pg_current_snapshot()` (`xmin`, `xmax`, `xip`).
3. **Reconcile at emission**, between stream transactions. Every stream transaction applied while
   a table is being snapshotted is logged with the keys it wrote in those tables, and its
   TRUNCATEs. A chunk row whose key a logged transaction wrote, where the chunk's snapshot doesn't
   see that transaction, is dropped: the stream already has the newer change. An unseen TRUNCATE
   drops the chunk. A logged entry is kept until a chunk proves it visible, and until every read
   still outstanding was issued after that proof: a read issued earlier may hold an older snapshot.
   A source that can't say what a read saw (MySQL, `visibility: None`) uses the window rule: the
   transactions applied after the read was issued.
4. **Emit.** The stream's buffered changes are flushed first. Then the chunk's rows are committed
   as their own change set, flagged `snapshot`. The position guard never skips such a set: its
   rows are read now, at a frontier the table's marker may already cover (Q-M6a-6). The cursor
   document rides the same flush.
5. **A schema that drifted while the pipeline was down** holds the table's reads until a rescan has
   brought its schema up to date (Q-M6a-8). A read that fails because the table changed under it
   triggers a rescan too.

Evidence (`crates/data-avalanche-pipeline`, M6a): the reconciler's unit tests; the busy-snapshot
test (40,000 rows, four concurrent writers) fails without the reconciler's filtering; the
stalled-synchronous-commit test fails without it and under the window rule.

**MySQL: the read locks the rows it reads (DA-SNAP-10; M7 planning, 2026-10-14, Q-M7-3).** The
window rule alone misses the second case above. A semi-sync commit waiting at AFTER_SYNC for its
replica's ack is in the binlog, so the stream delivers it, while a read doesn't see it yet: the
stream applies it first, the window rule finds no newer change, and the chunk's older row
overwrites it. Measured on MySQL 8.4 and MariaDB 10.11 / 11.8 (`prototypes/source-capability`
S-MY11): the stream delivered the stalled transaction in ~5 ms and a plain read missed it for the
6 s stall. MariaDB's default wait point, AFTER_COMMIT, has no such gap; AFTER_SYNC is MySQL's
default. So a MySQL chunk reads in a short READ COMMITTED transaction **with `LOCK IN SHARE
MODE`**: the shared row locks wait for any writer of the chunk's rows still committing, the stalled
commit included, and then read its committed value (S-MY11: the locking read waited the 6 s, then
saw it). The window rule then holds: a change the stream applies after the read was issued is newer
than the read, and one the read waited for is in what it read.

What the locking read costs (S-MY11b): `SELECT` alone is enough (no write privilege); it waits for
*any* open writer of its rows, so a long user transaction delays a chunk, up to
`innodb_lock_wait_timeout` (default 50 s), when it fails with error 1205 and the chunk is read again
after a backoff; while the chunk's transaction is open, writers of its rows wait, while inserts and
writers of other rows don't (READ COMMITTED takes no gap locks). The transaction holds one
statement and commits at once. (`FOR SHARE` is MySQL-only: MariaDB 11.8 rejects it.)

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
are silently skipped or duplicated. A column's **own** collation meets that when it is deterministic,
and its index then serves the chunk (`ORDER BY … LIMIT` walks the index instead of sorting the
table). So a text key orders and compares in its own collation when that is deterministic, and in a
**binary collation** (`COLLATE "C"` in PostgreSQL, `_bin` / `utf8mb4_bin` in MySQL) when it isn't,
with a warning at start: no index serves that order, so each chunk scans the table unless one is
added (`CREATE INDEX … (k COLLATE "C")`). The collations are recorded in the cursor; a cursor of
other collations (the key's collation changed mid-snapshot) halts the table for an operator
re-snapshot (`SCHEMA_MISMATCH`).
(Revised 2026-10-02, Q-M5-16: the earlier unconditional binary pin made every chunk of a key in
the database's default collation a full scan and sort, measured on PostgreSQL 18. A collation
library upgraded mid-snapshot without a reindex can reorder the keys; that breaks the server's own
indexes too.)

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
| What a chunk's read saw | `pg_current_snapshot()` in the chunk's transaction (§1.1) | none: the window rule, with a locking read (§1.1) |
| Chunk read isolation | `REPEATABLE READ` short txn | `READ COMMITTED` short txn, `LOCK IN SHARE MODE` (§1.1, DA-SNAP-10) |
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
| D-6.1 model | **Incremental chunked** snapshot, stream-first; reconciled by what each read saw (§1.1, DA-SNAP-9) + upsert; MySQL reads lock their rows (DA-SNAP-10) |
| D-6.2 cursor | Typed identifier tuple; row-value keyset; one **deterministic collation** for order and cursor (the key's own when deterministic, else binary; Q-M5-16); no-key fallback moot (G-13) |
| D-6.3 chunking | `snapshot.chunk_size` default 10000; checkpoint per chunk; resume is idempotent |
| G-7 consistency | Lock-free, convergent; no `RELOAD`/`LOCK TABLES`; no `consistent_snapshot` toggle |
| G-20 on-demand | Watermark chunk loop, **no stream pause**, no read-point assumption |
