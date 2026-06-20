---
updated: 2026-06-06
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

## Phase 1a — Snapshot: PostgreSQL

```
Worker connection (regular)
│
├─► CREATE_REPLICATION_SLOT slot LOGICAL pgoutput (snapshot='EXPORT_SNAPSHOT')
│   Returns: (slot_name, snapshot_name, start_lsn)
│   ─ persist slot_name and start_lsn to state storage
│
└─► For each table in resolved table set:
    │
    ├─► Open new connection
    ├─► BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ
    ├─► SET TRANSACTION SNAPSHOT '<snapshot_name>'
    │
    ├─► COPY schema.table TO STDOUT (BINARY)
    │   │
    │   └─► rows → CdcEvent { op: Snapshot, after: Row, lsn: start_lsn, ... }
    │        │
    │        └─► mpsc ──► Batch Accumulator
    │
    ├─► Checkpoint per-table cursor to state storage after each page
    │   (enables resume on crash without full re-scan)
    │
    └─► Checkpoint snapshot_status = completed for this table
```

The `start_lsn` captured with the slot is the safe resume point: after all tables are snapshotted, streaming begins from `start_lsn`. The exported snapshot ensures the SELECT sees exactly the data that existed at `start_lsn` — no gap, no overlap.

---

## Phase 1b — Snapshot: MySQL

```
Worker connection (regular, SUPER privilege or REPLICATION CLIENT)
│
├─► FLUSH TABLES WITH READ LOCK  (MySQL < 8.0)
│   or: START TRANSACTION WITH CONSISTENT SNAPSHOT (MySQL 8.0+)
│
├─► SELECT @@GLOBAL.gtid_executed → snapshot_gtid_set
│   ─ persist snapshot_gtid_set to state storage
│
├─► UNLOCK TABLES
│
└─► For each table in resolved table set:
    │
    ├─► Open new connection
    ├─► START TRANSACTION WITH CONSISTENT SNAPSHOT
    │
    ├─► SELECT * FROM schema.table  (with LIMIT/OFFSET cursor or keyset pagination)
    │   │
    │   └─► rows → CdcEvent { op: Snapshot, after: Row, lsn: snapshot_gtid_set, ... }
    │        │
    │        └─► mpsc ──► Batch Accumulator
    │
    ├─► Checkpoint per-table cursor after each page
    │
    └─► Checkpoint snapshot_status = completed for this table
```

After all tables are snapshotted, streaming starts with `COM_BINLOG_DUMP_GTID` passing `snapshot_gtid_set` as the already-executed set — MySQL streams only transactions not in that set.

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
    source_pos: SourcePosition, // LSN (PG) or GtidSet (MySQL)
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

**Exactly-once guard (if enabled):** Before committing, the writer checks the Iceberg table's snapshot summary for a `committed_lsn` property. If `committed_lsn ≥ flush_lsn`, the batch was already committed (duplicate from a crash recovery replay) and is silently dropped without re-writing.

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

1. Validate: tables list non-empty, no globs
2. For each table:
   a. Pause streaming events for that table in the accumulator
      (events continue to be received but are not forwarded to the Iceberg writer)
   b. Record pause_lsn = last_complete_lsn at the moment of pause
3. Run snapshot for each table (Phase 1a / 1b)
   ─ uses the same snapshot flow as startup
   ─ tables not in the pipeline config: snapshot is written to Iceberg but
     table is not added to the pipeline's streaming set
4. After snapshot completes:
   a. Resume streaming events from pause_lsn + 1
      (events buffered during the pause are now forwarded)
   b. Update snapshot_status = completed in state storage
5. Return snapshot job ID; status queryable via GET /snapshots/{id}
```

The "resume from pause_lsn + 1" ensures the snapshot and the subsequent stream are contiguous with no gap and no overlap.

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
| `_lsn` | `VARCHAR` | LSN string (PG) or GTID string (MySQL) |
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
