---
title: A halted table
description: A table that stopped replicating while its pipeline goes on, and a pipeline in error.
sidebar:
  order: 2
---

**Symptom:** `DataAvalancheTablesHalted` fires, `data_avalanche_halted_tables` is above 0, or a table
stops changing in the lake.

A table halts when a change can't be applied correctly. The pipeline records the halt with its
reason, keeps the table's Delta rows as they were, and goes on with its other tables. It drops the
halted table's later changes, so the table can't simply resume: a **snapshot job** copies it afresh
once the cause is fixed. The halt stays across restarts and failovers until then.

## 1. Find the reason

```sh
curl -s https://cdc.example.internal/v1/pipelines/shop
```

Its `halted_tables` lists each halted table with its code and a detail.

## 2. Fix the cause

| Code | Cause | Fix |
|---|---|---|
| `UNSAFE_DDL` | a schema change the table can't follow: a type change, a key change, `ENUM` values reordered, a partition operation, an unparsable DDL | decide whether the Delta table can keep its history; if not, drop or move it |
| `EVOLUTION_UNSUPPORTED` | a change Delta can't apply, such as widening a column | as for `UNSAFE_DDL` |
| `EVOLUTION_DISABLED` | any schema change, with `schema_evolution.enabled: false` | enable it, or accept the change by hand |
| `TABLE_DROPPED` | the table was dropped, or renamed away, at the source | remove it from the config (reload), or, if it was created again, go on to step 3 |
| `RESERVED_COLUMN_COLLISION` | a column named `_op`, `_ts`, `_lsn`, `_seq`, `_source_db`, `_pipeline` or `__data_avalanche_…` | rename the column at the source |
| `NO_IDENTIFIER` | no primary key and no `tables.primary_key` | add a primary key, or a `tables.primary_key` entry (reload) |
| `IDENTIFIER_NOT_USABLE` | a key column that is nullable, floating-point, or not a plain column | choose other columns in `tables.primary_key` |
| `IDENTIFIER_NOT_IN_REPLICA_IDENTITY` | PostgreSQL: the key isn't in the table's `REPLICA IDENTITY` | `ALTER TABLE … REPLICA IDENTITY …` to cover it |
| `NULL_IDENTIFIER` | a row arrived with a NULL key column | fix the source rows, or the key |
| `TABLE_NOT_REPLICABLE` | an `UNLOGGED` PostgreSQL table, or a table the source can't stream | make it logged, or drop it from the config |
| `NUMERIC_OVERFLOW` | a value that doesn't fit its decimal type | widen the type with `tables.column_type_overrides`, or `numeric_overflow_action: string` |
| `INVALID_DATE_VALUE` | MySQL: a zero or invalid date such as `2024-00-15` | override the column to `string`, or fix the rows |
| `SCHEMA_MISMATCH` | a column whose type the stream and the table disagree on, or an override that can't apply | correct the `column_type_overrides` entry |
| `SINK_REJECTED` | Delta refused the table's changes for good, for example a MERGE over `merge_memory_limit` | the detail names it: raise the limit, or lower the `flush` bounds |
| `SNAPSHOT_TIMEOUT` | the table's snapshot ran past `source.timeouts.snapshot` | raise the timeout |

A config fix goes through `POST /v1/config/reload`; most of these keys restart the pipeline.

## 3. Copy the table again

```sh
curl -s -X POST https://cdc.example.internal/v1/pipelines/shop/snapshots \
  -H 'content-type: application/json' -d '{"tables": ["public.orders"]}'
```

The job clears the halt, takes the table's schema from the source's catalog, copies the table, and
streams it again. Follow it with `GET /v1/snapshots/<job_id>` (see
[Snapshot jobs](/data-avalanche/runbooks/snapshot-jobs/)).

:::caution[Rows deleted while the table was halted]
A job upserts every row it reads and deletes nothing. A row deleted at the source while the table
was halted stays in the Delta table. To start clean, drop or move the Delta table before the job.
:::

:::note[A table created again while the pipeline was down]
If the table was dropped and created again while the pipeline was stopped, its first job halts it
`TABLE_DROPPED` at the job's start and fails. Start a second job: it clears the halt and copies the
new table.
:::

## A pipeline in error

**Symptom:** `DataAvalanchePipelineError`, or `actual_state: error`. Unlike a halted table, the
whole pipeline has stopped and waits for an operator. `GET /v1/pipelines/<id>` gives the reason;
the common ones:

- the binlog server changed under a file-and-position pipeline (`BINLOG_SOURCE_CHANGED`), or the
  position was purged (see [A lost position](/data-avalanche/runbooks/position-lost/));
- the slot is missing, invalidated or orphaned, and the settings say not to recover by themselves;
- the WAL behind the slot passed `max_wal_lag_bytes`;
- a transaction passed `max_transaction_buffer_bytes`;
- the sink's table is missing, or a commit failed in a way a retry won't fix.

Fix the cause, then `POST /v1/pipelines/<id>/restart` (or a `stop` and a `resume`). A pipeline in
`retrying` needs nothing: it hit an error worth another try, and tries again by itself.
