---
title: Snapshot jobs
description: Copying tables on demand through the API, following, cancelling and retrying a job, and what a job does and doesn't do.
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
A table whose Delta table is keyed otherwise than the pipeline's (other columns, or the same
spelled otherwise) isn't emptied: it fails in the job with `SINK_REJECTED`, its rows kept. A table
the config replicates also halts (see [A halted table](/data-avalanche/runbooks/halted-table/)); for
a table the config doesn't replicate, the reason shows only as that table's `error` in the job
(`GET /v1/snapshots/{job_id}`, below), not in the pipeline's halted tables. Either way, drop or
move the Delta table, then ask the job again.

## Follow it

```sh
curl -s https://cdc.example.internal/v1/snapshots/shop~0192f0c47b9e7c3a9d5e2f0a1b3c4d5e
curl -s https://cdc.example.internal/v1/pipelines/shop/snapshots
curl -s 'https://cdc.example.internal/v1/snapshots?status=running,queued'
```

A job's status, and each of its tables', is kept in the pipeline's state: it survives restarts and
failover, and a job interrupted by a crash goes on from its last chunk. A pipeline keeps its 100
newest ended jobs (completed, failed or cancelled); older ones are deleted as each job ends. A
job document the node can't read is left out of the list, with a warning in the log.

`GET /v1/snapshots` lists every pipeline's jobs, newest first, filtered by `status` (`queued`,
`running`, `cancelling`, `completed`, `failed`, `cancelled`; several, comma-separated), `pipeline`
and `by` (who asked). A pipeline whose jobs can't be read is named under `unreadable`.

For each table a job shows its rows copied, the source's estimate of its rows when the copy began
(PostgreSQL's `pg_class.reltuples`, which is `-1`, so absent, until the table was analyzed;
MySQL's `information_schema.TABLES.TABLE_ROWS`, approximate on InnoDB), when the copy began and
ended, the halt the job cleared for it (`from_halt`), and what lake readers see of it:

| `lake` | Lake readers see |
|---|---|
| `complete` | the table, copied |
| `as_before` | the rows it held before the job: the copy writes over them, or hasn't begun |
| `empty` | nothing: a truncating job emptied it, and its copy hasn't written a row yet |
| `partial` | part of the rows: a truncating job emptied it, and its copy didn't complete |

After a minute, a running job shows its `rate` (rows a second since its first table began) and
`eta_seconds`, from the rows its unended tables' estimates leave: an estimate moves, and so does
the ETA. Without an estimate for each of them, there's no ETA.

## Cancel a job

```sh
curl -s -X POST https://cdc.example.internal/v1/snapshots/shop~0192f0c47b9e7c3a9d5e2f0a1b3c4d5e/cancel \
  -H 'content-type: application/json' -d '{"note": "the wrong table"}'
```

- `202`: the job reads `cancelling`. The pipeline's owner restarts its worker, which ends the job's
  tables that haven't ended: their copy stops, and they read `cancelled`. A table whose copy had
  completed stays copied. A paused pipeline restarts too, and stays paused.
- A stopped pipeline, or one whose lease no node holds, runs no worker: the job reads `cancelling`
  until the pipeline's next start ends it (`"restart": false` in the answer).
- `200`: it was cancelled already. `409`: it has ended.

What the cancelled tables hold:

- A table the config replicates streams on. A copy that doesn't truncate leaves its rows as they
  were, partly refreshed; one that truncates leaves the table empty or partly copied until a new
  job copies it.
- A table the job was bringing back from a halt halts again, `COPY_CANCELLED`, naming the job: its
  Delta table holds part of its rows. Copy it again, or retry the job.
- A table the config doesn't replicate isn't read again: it keeps the rows the copy wrote.

The cancel is recorded in the events (`action.snapshot.cancel`, with the note), and the job's end
too (`job.cancelled`).

## Retry a job

```sh
curl -s -X POST https://cdc.example.internal/v1/snapshots/shop~0192f0c47b9e7c3a9d5e2f0a1b3c4d5e/retry \
  -H 'content-type: application/json' -d '{"tables": "not_copied"}'
```

A retry starts a new job of an ended job's tables, as `POST /v1/pipelines/{id}/snapshots` does:

- `tables`: `not_copied` (the default) for the tables that didn't complete, `all` for every one.
- `truncate`: the old job's unless given.
- `note`: why, kept with the new job and its event.

The new job records the one it retries (`retry_of`). `409`: the job hasn't ended, it left no table
to copy, or, as for any job, the pipeline doesn't run, no node holds its lease, or an unfinished
job holds one of the tables.

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
