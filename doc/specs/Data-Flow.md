---
updated: 2026-06-20
domain: Architecture
tags: [architecture, data-flow, pipeline, cdc, iceberg, arrow, snapshot, replication]
sources: []
---

# Pipeline Data Flow

## Overview

Each pipeline moves data through four sequential stages:

1. **Source** — connect to the database and receive change events (WAL / binlog)
2. **Decode** — translate raw protocol messages into typed `CdcEvent` structs
3. **Accumulate** — batch row-oriented events into columnar Arrow `RecordBatch` objects
4. **Sink** — encode batches as Parquet, write to cloud storage, commit an Iceberg snapshot

Two paths feed the accumulate stage: **streaming replication** (steady-state) and **snapshot** (initial load or on-demand). Both produce the same `CdcEvent` type so the rest of the pipeline is source-agnostic.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PIPELINE WORKER                                                        │
│                                                                         │
│  ┌──────────────┐     CdcEvent     ┌──────────────────┐                 │
│  │ Replication  │ ───mpsc──────►   │                  │  RecordBatch    │
│  │ Reader       │                  │  Batch           │ ───mpsc──────►  │
│  └──────────────┘                  │  Accumulator     │                 │
│  ┌──────────────┐     CdcEvent     │  (per-table      │                 │
│  │ Snapshot     │ ───mpsc──────►   │   builders)      │                 │
│  │ Reader       │                  │                  │                 │
│  └──────────────┘                  └──────────────────┘                 │
│                                                                         │
│   flush_lsn ◄──── flush_confirmation ◄──── Iceberg Writer ◄───────────  │
└─────────────────────────────────────────────────────────────────────────┘
```

All tasks communicate via `tokio::sync::mpsc` channels. Backpressure is automatic: a slow Iceberg writer fills the channel and stalls the accumulator, which stalls the replication reader, which stops advancing the source offset — no data is lost.

---

## Phase 0 — Pipeline Startup

On worker start (or when a pipeline is assigned via the consistent hash ring), the pipeline worker executes startup in this order:

```
1. Load pipeline state from cloud storage
   └── confirmed LSN / binlog name+position / GTID per table
   └── snapshot status per table (none | in-progress | completed | broken)
   └── replication slot name (if PG)

2. Evaluate snapshot_mode:
   ┌────────────────────────────────────────────────────────────────────────────┐
   │  snapshot_mode  │  table has confirmed offset?   │  action                 │
   ├────────────────────────────────────────────────────────────────────────────┤
   │  always         │  either                        │  snapshot, then stream  │
   │  never          │  either                        │  stream                 │
   │  initial        │  no                            │  snapshot, then stream  │
   │  initial        │  yes                           │  stream                 │
   │  dump_always    │  either                        │  snapshot, then stop    │
   |  dump_initial   |  no                            │  snapshot, then stop    │
   |  dump_initial   |  yes, and catalog has table    │  stop                   │
   |  dump_initial   |  yes, and not table in catalog |  snapshot, then stop    │
   └────────────────────────────────────────────────────────────────────────────┘

3. Apply table include/exclude glob filters → resolved table set

4. Start Snapshot Reader (if required) and/or Replication Reader
```

The resolved table set is the result of resolution of configured include filter, minus the exclude filter. Source of the full actual list of tables in the source database is the schema's data at the pipeline start moment. Glob patterns are evaluated against `schema.table` (e.g. `public.*`, `*.orders`).
Each time table set changes against current publication set, publication should be updated to reflect actual set.

---

## Phase 1 — Snapshot (incremental chunked, stream-first)

Snapshot does **not** block the stream or hold a long transaction. Streaming is established first and
runs throughout; each table is captured in keyset **chunks** interleaved with the live stream and
reconciled by **position watermarks + PK-upsert idempotency**. The order is **stream-first,
snapshot-concurrent**. This supersedes the earlier blocking `EXPORT_SNAPSHOT` / `FLUSH TABLES WITH
READ LOCK` model (which could not resume mid-table — an exported snapshot dies with the worker). See
`research/Snapshot_Resumability_and_Consistency.md` (closes G-6/G-7/G-20).

```
1. Establish the stream:
   PG    — CREATE_REPLICATION_SLOT slot LOGICAL pgoutput; capture start_lsn (WAL retention begins);
           persist slot_name + start_lsn; stream from start_lsn.
   MySQL — position the binlog stream at gtid_executed (COM_BINLOG_DUMP_GTID); stream immediately.

2. Per table, ordered by the G-13 identifier — chunk loop:
   a. lw  = current stream position            (pg_current_wal_lsn() / @@gtid_executed; read-only)
   b. read chunk: SELECT ... WHERE (key) > (cursor) ORDER BY (key) LIMIT chunk_size
                  in a SHORT read-committed txn; buffer the rows
                  rows → CdcEvent { op: Snapshot (_op = r), after: Row, ... }
   c. hw  = current stream position
   d. reconcile: as the stream is consumed lw→hw, any streamed change whose identifier is in the
                 buffered chunk REMOVES that key from the buffer (the live change supersedes the
                 snapshot read). After hw, emit the surviving buffered rows as snapshot inserts.
   e. checkpoint the cursor (last row's identifier tuple) to state storage.

3. Last chunk done ⇒ table fully captured (snapshot_status = completed); streaming simply continues.
```

**Consistency is convergent, not a frozen instant.** Every table is PK-keyed and written as an upsert
with keep-last-by-`_seq` (G-13), so a snapshot row that races a concurrent change converges (the
streamed change carries the higher `_seq` and wins). **No global read lock, no long-held transaction,
no `EXPORT_SNAPSHOT`/`FLUSH TABLES`** — the snapshot needs only `SELECT` plus the existing replication
privileges (MySQL `RELOAD`/`LOCK TABLES` are no longer required).

**Cursor & resume.** The resume cursor is a typed identifier tuple; pagination is a row-value
comparison `WHERE (c1,…) > (v1,…) ORDER BY c1,… LIMIT chunk_size`, with ordering pinned to a **binary
collation** (`COLLATE "C"` / `utf8mb4_bin`) recorded in the cursor. Crash resume reopens from the
checkpointed cursor; re-reading the in-flight chunk is harmless (upsert idempotency).
`snapshot.chunk_size` defaults to 10000 (checkpoint per chunk). `parallelism` assigns disjoint tables
to parallel workers; keyset order is serial within a table.

---

## Phase 2a — Streaming Replication: PostgreSQL

```
Replication connection (replication protocol, not SQL)
│
├─► START_REPLICATION SLOT <slot> LOGICAL <confirmed_flush_lsn>
│       (proto_version '2', publication_names '<pub>')
│
├─► Keepalive timer (tokio::time::interval, default 60s)
│   └─► send StandbyStatusUpdate(write_lsn, flush_lsn, apply_lsn)
│       advancing confirmed_flush_lsn after each committed + flushed batch
│
└─► Event loop (tokio::select! over stream.next() and keepalive tick):
    │
    ├─► R (Relation message)
    │   └─► update local schema cache: relation_oid → (schema, table, columns)
    │
    ├─► B (Begin message)
    │   └─► record xid, commit_lsn, commit_ts for current transaction
    │
    ├─► I (Insert) / U (Update) / D (Delete) / T (Truncate)
    │   └─► decode using schema cache
    │   └─► emit CdcEvent to mpsc channel
    │
    └─► C (Commit message)
        └─► emit TransactionBoundary(commit_lsn, xid) to accumulator
            (accumulator uses this to decide if a batch can be safely flushed)
```

**REPLICA IDENTITY handling:**
- `DEFAULT` (PK only): UPDATE messages carry the new tuple + PK of the old row; DELETE carries only the PK. The full old image is not available.
- `FULL`: UPDATE and DELETE carry the complete old row image. Required for tables without a primary key.
- `NOTHING`: DELETE events are unusable — the Iceberg writer cannot identify which row to mark deleted. Pipelines should reject tables with `REPLICA IDENTITY NOTHING`.

**TOAST columns:** Unchanged large columns in UPDATE events are represented by a "unchanged TOAST" marker, not the actual value. The pipeline must either re-fetch the value via a separate query or treat the column as unchanged and carry forward the last known value.

---

## Phase 2b — Streaming Replication: MySQL

```
Replication connection (COM_BINLOG_DUMP_GTID)
│
├─► Send COM_BINLOG_DUMP_GTID with gtid_executed set from state storage
│   └─► MySQL streams only transactions not in the set (gap-free, position-independent)
│
└─► Event loop:
    │
    ├─► GTID_LOG_EVENT
    │   └─► record current_gtid for this transaction
    │
    ├─► QUERY_EVENT (BEGIN)
    │   └─► record transaction start
    │
    ├─► TABLE_MAP_EVENT
    │   └─► update table_id → (schema, table, column_types) mapping
    │       (mapping resets on ROTATE_EVENT; cache is file-scoped)
    │
    ├─► WRITE_ROWS_EVENT
    │   └─► decode after-image → CdcEvent { op: Insert, after: Row, lsn: current_gtid }
    │
    ├─► UPDATE_ROWS_EVENT
    │   └─► decode before + after images → CdcEvent { op: Update, before: Row, after: Row }
    │
    ├─► DELETE_ROWS_EVENT
    │   └─► decode before-image → CdcEvent { op: Delete, before: Row }
    │
    ├─► XID_EVENT
    │   └─► emit TransactionBoundary(current_gtid) to accumulator
    │   └─► update local gtid_executed set
    │
    └─► ROTATE_EVENT
        └─► update current binlog filename; clear table_id cache
```

---

## CdcEvent Structure

Both sources produce the same intermediate type. This is the boundary between source-specific and source-agnostic code:

```
CdcEvent {
    op:        Op,              // Insert | Update | Delete | Snapshot
    table:     TableRef,        // { schema: String, name: String }
    before:    Option<Row>,     // old image: present for Update (if FULL/PK) and Delete
    after:     Option<Row>,     // new image: present for Insert, Update, Snapshot
    source_pos: SourcePosition, // COMMIT position: LSN (PG) or GtidSet (MySQL)
    change_seq: SourceSeq,      // PER-CHANGE order: PG change-LSN / MySQL (binlog_file,end_log_pos).
                                // Finer than source_pos; the keep-last key for dedup (Ghost_Row §4).
    xid:       u64,             // transaction ID (xid in PG, seq-no in GTID for MySQL)
    commit_ts: DateTime<Utc>,   // server commit timestamp
}

Row = Vec<(column_name: String, value: ColumnValue)>

ColumnValue = Null
            | Bool(bool)
            | Int16(i16) | Int32(i32) | Int64(i64)
            | UInt64(u64)                                            // MySQL/MariaDB BIGINT UNSIGNED (Type_Mapping Gap C)
            | Float32(f32) | Float64(f64)
            | Numeric(Decimal)
            | Text(String)
            | Bytes(Vec<u8>)
            | Timestamp { value: DateTime<Utc>, with_tz: bool }     // with_tz distinguishes timestamp vs timestamptz (Gap A)
            | Date(NaiveDate)
            | Time(NaiveTime)                                       // PG time / MySQL TIME, no date component (Gap D)
            | Uuid(Uuid)
            | Json(serde_json::Value)
            | Array { element_type: ColumnValueType, values: Vec<ColumnValue> }  // typed element for ListBuilder<T> (Gap B)

// element-type tag carried by Array so the accumulator can build a typed Arrow ListBuilder<T>
ColumnValueType = BoolType
                | Int16Type | Int32Type | Int64Type | UInt64Type
                | Float32Type | Float64Type | NumericType(u8, i8)
                | TextType | BytesType | TimestampType | DateType | TimeType
                | UuidType | JsonType | ArrayType(Box<ColumnValueType>)
```

The four extensions over the original enum — `UInt64`, the `with_tz` flag on `Timestamp`, `Time`, and
the typed `Array` — are required by the Hop-2 accumulator; see `research/Type_Mapping.md` §"ColumnValue
enum extensions" for the per-extension correctness rationale.

---

## Phase 3 — Batch Accumulation

The batch accumulator groups `CdcEvent` objects by table into per-table Arrow `RecordBatch` builders. It is source-agnostic — all source-specific logic has been resolved by Phase 2.

```
Accumulator state (per-table):
│
├─► SchemaBuilder         — builds Arrow schema from first event's column set
├─► Vec<ArrayBuilder>     — one Arrow array builder per column
├─► metadata columns:     _op, _ts, _lsn, _source_db, _pipeline
├─► row_count: usize
├─► byte_estimate: usize
└─► oldest_event_ts: Instant

For each incoming CdcEvent:
│
├─► Row conversion (row-oriented → columnar):
│   For each (column_name, value) in after (or before for deletes):
│   └─► append value to the corresponding ArrayBuilder
│
├─► Append metadata:
│   ├─► _op:        'I' | 'U' | 'D' | 'r'  (r = snapshot/read)
│   ├─► _ts:        commit_ts
│   ├─► _lsn:       source_pos.to_string()
│   └─► _source_db: pipeline.source.host + '/' + pipeline.source.database
│
├─► Update row_count += 1
└─► Update byte_estimate += estimated_row_size

On TransactionBoundary(lsn):
└─► record last_complete_lsn = lsn  (only complete transactions can be safely flushed)
```

**Flush decision** — evaluated after each transaction boundary:

```
Flush trigger (OR semantics — first condition that fires):
│
├─► time:      now - oldest_event_ts  ≥  flush_interval      (default 60s)
├─► rows:      row_count              ≥  max_rows            (default 100,000)
└─► size:      byte_estimate          ≥  max_bytes           (default 512 MiB)

Note: flush only occurs on a transaction boundary — never mid-transaction.
This guarantees atomic transaction visibility in the Iceberg snapshot.
If schema change was detected during batch accumulation, it forces flush of all data accumulated before change event.
```

**Oversized transactions (opt-in split commits).** When `sink.max_transaction_batch_rows` is set and a
single source transaction exceeds it, the writer flushes **partial-transaction** Iceberg commits
instead of waiting for the source `COMMIT`, but **pins the durable resume checkpoint at the
transaction's START** — `cdc.committed_position` does not advance past the txn until its `XID`/commit is
seen. A crash mid-split replays the whole transaction; already-flushed slices re-write as idempotent
upserts (no loss, no duplicate). Default `null` = unlimited = strict atomic visibility (a sub-threshold
txn flushes as one commit). An absolute `sink.max_transaction_buffer_bytes` ceiling (default ~1 GiB)
halts the pipeline (never OOMs the worker) if an unsplittable txn exceeds it. See
`research/Large_Transaction_Handling.md` (G-29).

---

## Phase 4 — Iceberg Write

When the accumulator triggers a flush, it finalizes the Arrow `RecordBatch` for each table and sends it to the Iceberg writer.

```
For each (TableRef, RecordBatch) in the flush batch:
│
├─► Schema reconciliation:
│   ├─► Load current Iceberg table schema from catalog
│   ├─► Compare with RecordBatch schema
│   └─► If new columns present AND schema_evolution = add_columns:
│           → open SchemaUpdate transaction, add columns, commit schema
│       Else if incompatible AND schema_evolution = reject:
│           → return error, pause pipeline
│
├─► Parquet encoding (spawn_blocking — CPU-bound):
│   ├─► iceberg-rust encodes RecordBatch → Parquet byte stream
│   ├─► compression: zstd (default)
│   └─► row group size: aligned to RecordBatch size
│
├─► Object storage write (OpenDAL):
│   └─► PUT data/<uuid>.parquet to configured bucket/prefix
│
├─► Manifest entry creation:
│   ├─► record file path, file size, row count
│   └─► record per-column statistics (min/max for partition pruning)
│
└─► Iceberg transaction commit:
    ├─► TableTransaction::fast_append()  →  appends new data files
    ├─► Atomic metadata.json update via catalog
    │   (REST catalog: conditional HTTP PUT;
    │    Glue: UpdateTable with expected version;
    │    file-based: atomic rename or conditional PUT)
    └─► On success: send flush_confirmation(lsn) back to accumulator
```

**Position guard (optimization, not correctness):** correctness comes from row-level PK-upsert idempotency (G-13); the guard only skips already-done work. Before committing a table, the writer reads that table's `cdc.committed_position` from its Iceberg snapshot summary — an **engine-tagged** value (PG LSN, or **canonical GTID-set** string). If `committed_position.covers(flush.position_for_table)` (PG: `≥`; MySQL: GTID **subset**) the table is already committed and is skipped. On commit, the new value is the `union` of the prior value and the flush's per-table position. The pipeline **resume checkpoint** persisted to state storage is the **laggard** across all tracked tables (PG `min` / MySQL set-intersection); tables ahead skip via the guard or are absorbed by idempotency. `cdc.committed_position` coexists with `cdc.owner_epoch` (clustering fence) — orthogonal: the epoch rejects a zombie writer, the position skips done work. See `research/Delivery_Guarantee_and_Position_Guard.md` (G-14).

### Sink-writer contract (consolidated)

The Iceberg writer must, for every flush batch:

1. **Key** each table by its **identifier** (inferred source PK via channel C, or `tables.primary_key`
   override) as Iceberg `identifier-field-ids`; identifier columns are top-level, primitive, NOT NULL,
   non-floating (`research/Ghost_Row_Dedup_and_Primary_Keys.md`, G-13).
2. **Collapse** to ≤1 equality-delete + ≤1 data record per identifier tuple, keep-last by `change_seq`
   (`_seq`). DELETE → equality-delete only (mirror; no `D` data row). INSERT/UPDATE/snapshot →
   equality-delete(old key) + one data record (new image). A PK-changing UPDATE is two tuples:
   delete(old key) + data(new key).
3. **Sequence** the equality-delete file with the **enclosing commit's** data-sequence-number so the
   strictly-less-than rule purges prior-commit versions but spares the same-commit insert
   (**verification gate DA-SINK-1**; fallback: position-deletes / COW).
4. **Route partition-scoped deletes by the BEFORE-image:** when a partition-source column changes, write
   the equality-delete in the row's **old** partition (computed from the before-image), data in the new
   partition — no cross-partition ghost (`research/Iceberg_Partitioning_and_Layout_Evolution.md`, G-24).
5. **Stamp the snapshot summary** with `cdc.committed_position` (per-table HWM) and `cdc.owner_epoch`
   (clustering fence); re-assert both on every commit (**gate DA-SINK-2**).
6. **TRUNCATE** of a tracked table is a data-clear (overwrite-to-empty / delete-all), not a schema
   change; same idempotent treatment for PG (`T` message) and MySQL (`QUERY_EVENT` classifier). *(Open:
   confirm the exact Iceberg op — gap-analysis DA-X-5.)*
7. **Tolerate unmaintained tables:** never assume compaction/expiry has run; planning and writes work
   (slower) under deep delete-file stacks and many small files — v1 maintenance is external
   (`research/Iceberg_Maintenance.md`, G-25).

---

## Phase 5 — Offset Acknowledgment

After receiving `flush_confirmation(lsn)` from the Iceberg writer:

**PostgreSQL:**
```
1. Update local confirmed_flush_lsn = lsn
2. Send StandbyStatusUpdate to PostgreSQL:
   write_lsn  = lsn  (data written to local buffer)
   flush_lsn  = lsn  (data durably flushed to Iceberg)
   apply_lsn  = lsn
3. PostgreSQL advances the slot's confirmed_flush_lsn
   → WAL before this point can now be reclaimed
4. Persist confirmed_flush_lsn to state storage
```

**MySQL:**
```
1. Update local gtid_executed set to include the flushed transactions
2. Persist updated gtid_executed to state storage
   (MySQL binlog has no server-side acknowledgment — the executed set is the resume point)
```

The state storage write (step 4 / step 2) is the crash-safe checkpoint. On restart, the pipeline resumes streaming from the last persisted position, replaying any events not yet flushed. The exactly-once guard in Phase 4 handles the resulting duplicates.

---

## State Layout in Cloud Storage

Per-pipeline state is rooted at the **per-pipeline state location** — `state.prefix` (or `sink.prefix`),
which defaults to the pipeline `id` (see `Configuration_Specification.md` §10). The state is already
per-pipeline-scoped by that prefix, so there is **no** redundant `pipelines/<id>/` parent segment; the
files sit directly under the per-pipeline root. Cross-pipeline data (config generation, cluster anchor,
membership, the lease registry) lives separately under `globals.state` per
`research/Clustering_Architecture_Research.md` §12.

```
<per-pipeline state location>/                   (state.prefix | sink.prefix, defaults to pipeline id)
├── pipeline-state.json                          — overall pipeline state version
├── replication/
│   ├── slot.json                                — PG: slot name, confirmed_flush_lsn
│   └── gtid.json                                — MySQL: current gtid_executed set
└── snapshots/
    ├── <schema>.<table>.json                    — per-table snapshot state
    │   {
    │     "status": "completed",                  // none | in_progress | completed | broken
    │     "start_lsn": "0/1A2B3C4",
    │     "cursor": null,                         // keyset/offset for resume on crash
    │     "row_count": 1234567,
    │     "completed_at": "2026-06-06T..."
    │   }
    └── ...
```

---

## On-Demand Snapshot Flow

Triggered via the management REST API (`POST /pipelines/{name}/snapshot`):

```
Request payload: { tables: ["schema.orders", "schema.products"] }
                  (exact table names, no globs; may include tables not in pipeline config)

1. Validate: tables list non-empty, no globs.
2. For each table: run the Phase-1 chunk loop (watermark lw/hw + keyset chunks) WITHOUT pausing the
   stream. Tables already in the streaming set keep streaming; the snapshot reconciles against the
   live stream exactly as at startup.
3. Tables not in the pipeline config: snapshot is written to Iceberg but the table is not added to
   the pipeline's streaming set.
4. Update snapshot_status = completed in state storage.
5. Return snapshot job ID; status queryable via GET /v1/snapshots/{id}.
```

There is **no stream pause and no `pause_lsn + 1` assumption**: watermarks reconcile re-snapshotted
rows against concurrent changes, and idempotent upserts mean re-reading existing keys re-writes the
same rows. This is also the mechanism for **re-snapshot-after-unsafe-DDL** (`Schema_Change_Detection_
and_Evolution.md` §8.2).

---

## Error Paths & Recovery

### Source Disconnect

```
Replication stream error / TCP timeout
│
├─► Close connection
├─► Exponential backoff (initial 1s, max 60s, jitter)
├─► Reconnect and re-authenticate
└─► Resume from last confirmed_flush_lsn (PG) or gtid_executed (MySQL)
    ─ any events since last confirmed position are replayed
    ─ exactly-once guard in Phase 4 deduplicates previously flushed events
```

### Iceberg Write Failure

```
Iceberg commit error (network, catalog conflict, storage error)
│
├─► Retry with backoff (up to configured max_retries)
├─► On catalog conflict (concurrent writer):
│   └─► Re-load current table metadata, retry fast_append
├─► On persistent failure:
│   └─► Pause pipeline; record error in state storage
│   └─► Alert via metrics (pipeline_status gauge = error)
└─► On restart: replay from last confirmed_flush_lsn
```

### Worker Failure

```
Node dies (SWIM detects via failed probes → marks dead)
│
├─► Gossip propagates dead state to all peers
├─► All peers independently recompute consistent hash ring
│   └─► dead node's pipelines are distributed to next-clockwise live nodes
│
└─► New owner starts each inherited pipeline (Phase 0):
    ├─► Reads state from cloud storage
    └─► Resumes from last confirmed_flush_lsn / gtid_executed
        (worst-case replay = time since last checkpoint write)
```

**Desired-state survives failover.** Each pipeline's `desired_state` (`RUNNING`/`PAUSED`/`STOPPED`) is
persisted in per-pipeline state. The new owner reads it and reconciles — a paused pipeline stays paused,
a stopped one stays stopped. `PAUSE` and `STOP` **flush to a clean transaction boundary and commit
before settling**, so there is **no in-memory buffer at risk** during failover or restart; a config
reload does **not** resurrect a paused/stopped pipeline (runtime desired-state wins). See
`research/Pipeline_Lifecycle_State_Machine.md` (G-NEW-15).

### Mid-Snapshot Crash

```
Worker killed during Phase 1 (snapshot)
│
└─► On restart:
    ├─► Load state: snapshot_status = in_progress for this table
    ├─► Restore cursor from state storage
    └─► Resume table scan from cursor (not from row 0)
        ─ completed tables skipped (snapshot_status = completed)
        ─ in-progress table resumes from last checkpointed page
```

---

## Concurrency Model: Task Structure Per Pipeline

```
tokio runtime
│
└─► PipelineWorker task
    │
    ├─► SnapshotReader task (temporary, during snapshot phase)
    │   └─► one sub-task per table (parallel table snapshot)
    │
    ├─► ReplicationReader task (persistent, during streaming phase)
    │   └─► single task per pipeline source connection
    │
    ├─► BatchAccumulator task
    │   ├─► per-table RecordBatch builders (in-task state, not spawned)
    │   └─► flush timer (tokio::time::interval)
    │
    ├─► IcebergWriter task
    │   └─► per-table Parquet encode + S3 write (spawn_blocking for CPU work)
    │
    └─► StateCheckpointer task
        └─► periodic write of confirmed_lsn to cloud storage
            (decoupled from the hot path to avoid S3 write amplification)
```

Shutdown sequence (on SIGTERM or pipeline removal from config):
1. Send shutdown signal to `ReplicationReader` → it stops reading, closes connection
2. `BatchAccumulator` receives channel close → flushes any buffered data
3. `IcebergWriter` processes remaining flush, sends final confirmation
4. `StateCheckpointer` writes final confirmed position

---

## Data Lineage Per Row

Every row written to Iceberg carries these metadata columns in addition to the source table columns:

| Column | Type | Content |
|---|---|---|
| `_op` | `CHAR(1)` | `I` insert, `U` update, `D` delete, `r` snapshot |
| `_ts` | `TIMESTAMP WITH TIME ZONE` | source database commit timestamp |
| `_lsn` | `VARCHAR` | commit LSN string (PG) or GTID string (MySQL) |
| `_seq` | `VARCHAR` | per-change source order (PG change-LSN / MySQL binlog file+pos); keep-last dedup key (G-13) |
| `_source_db` | `VARCHAR` | `host/database` from pipeline source config |
| `_pipeline` | `VARCHAR` | pipeline name from config |

These columns enable downstream consumers to reconstruct point-in-time state, detect duplicates (via `_lsn`), and audit change provenance.

---

## Related Pages

- [[Architecture/System-Overview]] — high-level requirements and design decisions
- [[CDC/PostgreSQL-Replication]] — pgoutput protocol internals, slot management
- [[CDC/MySQL-Binlog]] — binlog event types, GTID positioning
- [[CDC/CDC-Overview]] — delivery guarantees, ghost-row hazards
- [[Iceberg/iceberg-rust]] — RecordBatch → Parquet → Iceberg write API
- [[Iceberg/Iceberg-Overview]] — snapshot model, catalog types, maintenance
- [[Arrow/Arrow-Overview]] — RecordBatch structure and columnar format
- [[Distributed-Systems/Gossip-SWIM]] — failure detection and pipeline reassignment
- [[Rust-Infra/Tokio-Async]] — task structure and channel backpressure patterns

## Sources
