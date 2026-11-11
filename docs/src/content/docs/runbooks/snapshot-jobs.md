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
  job already holds one of the tables. When that job is still queued, the call also asks the
  pipeline's restart for it again, in case the first ask was lost: the job starts at the owner's
  next poll.
- `422`: no table, a pattern, or a name not in the source's form.

The call needs the `operator` role. Any node accepts it; the pipeline's owner restarts the
pipeline to take the job, which replays from the last checkpoint and is harmless.

To make the lake an exact copy, add `"truncate": true`: each of the job's tables is emptied in the
lake before its copy starts, so rows deleted at the source while the table wasn't streamed go.
Until the job completes, those tables are empty or partly copied: readers see that. A job
interrupted by a crash after the truncate goes on copying; it doesn't empty the table again.
A table whose Delta table is keyed by other columns than the pipeline's isn't emptied: it halts
`SINK_REJECTED` and fails in the job (see [A halted table](/data-avalanche/runbooks/halted-table/)).

## Follow it

```sh
curl -s https://cdc.example.internal/v1/snapshots/shop~0192f0c4-7b9e-7c3a-9d5e-2f0a1b3c4d5e
curl -s https://cdc.example.internal/v1/pipelines/shop/snapshots
```

A job's status, and each of its tables', is kept in the pipeline's state: it survives restarts and
failover, and a job interrupted by a crash goes on from its last chunk. A pipeline keeps its 100
newest ended jobs (completed, failed or cancelled); older ones are deleted as each job ends. A
job document the node can't read is left out of the list, with a warning in the log.

## What a job does

- Copies each table from its first row, whatever the table's `snapshots.mode`.
- For a halted table: clears the halt, takes the schema from the source's catalog, and streams the
  table again once it's copied.
- For a table the config doesn't replicate: copies it once, and doesn't stream it. If that copy
  fails, the job fails with the reason; the table isn't counted as a halted table of the pipeline.

## What it doesn't do

- **Delete**, unless asked to. A job upserts. Rows deleted at the source while the table wasn't
  streamed stay in the lake, unless the job is asked with `truncate`.
- **Stop the stream.** The pipeline keeps streaming every table; a change to a row the job is
  copying wins over the job's copy.

:::note
A job's table that halts while it is being copied fails in the job only at the pipeline's next
flush, up to `sink.flush.seconds` later.
:::
