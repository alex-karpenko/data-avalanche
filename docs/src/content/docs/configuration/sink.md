---
title: Sink and flushing
description: The Delta sink's settings, when changes are committed, what a commit costs, and how source types land in Delta.
sidebar:
  order: 5
---

Every pipeline names its table format with `sink.type`, which has no default. v1 writes Delta Lake
(`delta`); `iceberg` is accepted by the config but refused at start ("the Iceberg sink ships in
v2"), while the other pipelines run.

## Flushing

A pipeline buffers changes in memory and commits them in a **flush**: one Delta MERGE per table that
changed, each table committed atomically, then the pipeline's checkpoint. A flush fires when the
first of its triggers is met, always at a source transaction boundary:

| `sink.flush` | Default | |
|---|---|---|
| `seconds` | `30` | the oldest buffered change is this old; `0` flushes at every transaction |
| `rows` | `100000` | this many rows are buffered |
| `bytes` | `512MiB` | the buffer holds this much |

Each trigger can be turned off with `null`, but not all three (V-035). `POST
/v1/pipelines/<id>/flush` flushes at once.

**What a flush costs.** A Delta MERGE rewrites every data file that holds a key the flush changed.
Changes to recent rows (appends, updates to new orders) touch a few files; updates spread over a
large table's whole key range touch most of its files, so each flush rewrites most of the table.
Watch `data_avalanche_sink_merge_files_rewritten_total` and
`data_avalanche_sink_merge_rows_copied_total`: when they grow much faster than the rows changed,
flush less often (raise `seconds` and `rows`), so each MERGE carries more changes for the same
rewrite. The cost of a lower bound is lag: the
[replication lag](/data-avalanche/deployment/observability/) is up to about the flush interval
at steady state.

**Large transactions.** A transaction is committed whole, so it is buffered whole:
`max_transaction_buffer_bytes` (1 GiB) caps it, and a bigger one halts the pipeline rather than
the process running out of memory. Set `max_transaction_batch_rows` to commit a large transaction
in slices instead: readers then see it in parts, and a crash replays it from its start.

**Back-pressure.** The buffer between reading and writing holds `buffer_capacity_rows` (200000)
rows. While the storage fails, the pipeline retries its commit with backoff and, once the buffer
is full, stops reading the source until the storage answers.

## Delta settings

`sink.delta` applies when the sink creates a table; an existing table keeps its properties.

- `timestamp_ntz_as`: how a `timestamp without time zone` column is written. The default,
  `timestamp_ntz`, keeps its meaning but creates the table at reader version 3 and writer version 7
  (the `timestampNtz` table feature), which older readers can't open. `timestamp` writes the wall
  clock as a UTC instant at protocol 1/2; `string` writes its text.
- `checkpoint_interval` (10) and `log_retention` (30 days): Delta's own `delta.checkpointInterval`
  and `delta.logRetentionDuration`.
- `merge_memory_limit` caps one MERGE's memory. A MERGE that needs more fails on every replay, so
  the table halts `SINK_REJECTED` naming the key: raise it or lower the flush bounds, then
  re-snapshot the table.
- `table_properties` passes table properties through; `delta.*` keys must be on the allow-list
  (V-093).
- `column_mapping: name` is refused in v1: delta-rs can't add a column to a column-mapped table.

Partitioning (`tables.partitioning`) takes `identity` fields only on Delta.

## What lands in the table

Each table gets the source's columns, then six lineage columns:

| Column | |
|---|---|
| `_op` | `I` insert, `U` update, `r` a snapshot row (a delete removes the row) |
| `_ts` | the source transaction's commit time |
| `_lsn` | the source transaction: its commit LSN, or its GTID, or its binlog file and offset |
| `_seq` | the change's place in the source's order, as text that sorts the same way |
| `_source_db` | `host/database` of the source |
| `_pipeline` | the pipeline's name |

A source column with one of these names (in any case), or starting `__data_avalanche_`, halts its
table with `RESERVED_COLUMN_COLLISION`: rename it at the source.

Identifier columns are `NOT NULL`; every other column is nullable, whatever the source says, so
that a column added later can be added.

Types Delta lacks are converted, and every column records its source's logical type in its Delta
metadata:

| Source | Delta |
|---|---|
| `uuid` | `string`, the canonical lower-case form |
| `time` | `string`, `HH:MM:SS.ffffff` |
| MySQL `BIGINT UNSIGNED` | `decimal(20,0)` |
| PostgreSQL `numeric` wider than 38 digits | `string` (or a halt, `numeric_overflow_action: error`) |
| `json`, `jsonb`, `enum`, and most types with no native mapping | `string`, the source's text |
| PostgreSQL `±infinity` and dates outside 0001-01-01 … 9999-12-31 | clamped to that range |

`tables.column_type_overrides` sets a column's type, in the sink-neutral grammar (`int64`,
`decimal(38,10)`, `string`, `timestamp_tz`, …). An override must be the column's own type, `string`,
or a lossless conversion (`int32` to `int64`, `timestamp_ntz` to `timestamp_tz`, …); any other
halts the table at start with `SCHEMA_MISMATCH`.
