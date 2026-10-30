---
title: Snapshot jobs
description: Copying tables on demand through the API, following a job, and what a job does and doesn't do.
sidebar:
  order: 6
---

A snapshot job copies tables into the lake while the pipeline keeps streaming: to re-copy a halted
table, to backfill a table after a gap, or to copy once a table the config doesn't replicate. How
a snapshot reads is in [Snapshots](/data-avalanche/architecture/snapshots/).

## Start a job

```sh
curl -s -X POST https://cdc.example.internal/v1/pipelines/shop/snapshots \
  -H 'content-type: application/json' \
  -d '{"tables": ["public.orders", "public.order_lines"]}'
```

- Name each table exactly, as the config's per-table keys do: `schema.table` on PostgreSQL, the
  bare name on MySQL. No patterns.
- `202` answers with the job's id, `<pipeline id>~<suffix>`.
- `409`: the pipeline isn't running (paused, stopped), no node holds its lease, or an unfinished
  job already holds one of the tables.
- `422`: no table, a pattern, or a name not in the source's form.

The call needs the `operator` role. Any node accepts it; the pipeline's owner restarts the
pipeline to take the job, which replays from the last checkpoint and is harmless.

## Follow it

```sh
curl -s https://cdc.example.internal/v1/snapshots/shop~0192f0c4-7b9e-7c3a-9d5e-2f0a1b3c4d5e
curl -s https://cdc.example.internal/v1/pipelines/shop/snapshots
```

A job's status, and each of its tables', is kept in the pipeline's state: it survives restarts and
failover, and a job interrupted by a crash goes on from its last chunk.

## What a job does

- Copies each table from its first row, whatever the table's `snapshots.mode`.
- For a halted table: clears the halt, takes the schema from the source's catalog, and streams the
  table again once it's copied.
- For a table the config doesn't replicate: copies it once, and doesn't stream it.

## What it doesn't do

- **Delete.** A job upserts. Rows deleted at the source while the table wasn't streamed stay in
  the lake: drop or move the Delta table first when you need an exact copy.
- **Stop the stream.** The pipeline keeps streaming every table; a change to a row the job is
  copying wins over the job's copy.

:::note
A job's table that halts while it is being copied fails in the job only at the pipeline's next
flush, up to `sink.flush.seconds` later.
:::
