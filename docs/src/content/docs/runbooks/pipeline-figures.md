---
title: A pipeline's tables and figures
description: What a pipeline's worker shows now, a table's page, and every pipeline's tables, asked of any node.
sidebar:
  order: 9
---

`GET /v1/pipelines/<id>` reads a pipeline's state from storage. The routes here show what its
worker holds in memory: they are answered by the node that holds the pipeline's lease. Any node
takes them, with the `viewer` role, and asks the holder with your own credentials.

## The worker, now

```sh
curl -s https://cdc.example.internal/v1/pipelines/shop/live
```

The answer names the `node` that runs the worker, its `actual` state and `owner_epoch`, and:

- `worker`: its `phase`, the positions it has `read` and made durable (`checkpoint`),
  `lag_seconds` and `fresh_as_of` (what lake readers see: every change older than the oldest one
  not yet durable), the rows read since it started, and its warnings.
- `source`: what the source keeps for the pipeline, read every heartbeat, with `read_at`.
  PostgreSQL: the server's write position (`head`), the slot (`active`, `confirmed_flush`,
  `restart_lsn`, `wal_held_bytes`, `wal_status`), `max_slot_wal_keep_size` (`null`: unlimited),
  and the publication with its table count. MySQL and MariaDB: the server's `head` (its GTID set,
  or binlog file and offset) and `binlog_expire_seconds` (`null`: never expired), and how far the
  stream reads behind (`behind_seconds`).
- `tables`: each table the worker tracks, halts or copies, with its `state` (`streaming`,
  `halted`, `snapshot`, or `copied`: a `dump_*` table whose copy ended, or a `dump_initial` one
  the lake had at the start, which isn't streamed), the changes committed since the worker
  started, the changes of the last minute (`changes_per_minute`), its `last_commit` and Delta
  `version`, the last MERGE's `files_rewritten` against the table's `files`, and its
  `fresh_as_of`. A halted table is as fresh as its halt; a `copied` one has none. A streaming
  table is **stale** when that is over 60 s old (the lag target) while the worker streams; a
  paused pipeline is never stale. A snapshot job's table outside the config is listed while it
  copies; afterwards its job tells ([Snapshot jobs](/data-avalanche/runbooks/snapshot-jobs/)).

The figures start again with each worker: a restart or a failover sets the counts to zero.

| Answer | Why |
|---|---|
| `409` | no node holds the lease: nothing runs the pipeline |
| `503` | the holder can't be asked: it isn't a member this node sees, gossips no API address, or is this node before its worker starts (`node` names it) |
| `502`, `504` | the holder didn't answer, or not within 3 s |
| `421` | a request forwarded to a node that no longer runs the worker: the lease moved; ask again |

## A table's page

```sh
curl -s https://cdc.example.internal/v1/pipelines/shop/tables/public.orders
```

Name the table as the pipeline's config does: `schema.table` for PostgreSQL, the table alone for
MySQL. The page brings together:

- `live`: the table's row of the worker's figures above, or `live_error` with why there is none;
- `halt`: its code, detail and `since`, when it halted (none for a halt recorded before 1.1.0);
- `delta`: the Delta table's `location`, `version`, `files`, `bytes` and `rows` (from the files'
  statistics), read by the node you ask when you ask, and kept 30 s; `delta_error` when it
  can't be read;
- `columns`: each column at the source (as the pipeline last cached it) against Delta's. `state`
  is `kept`, `soft_dropped` (gone at the source, kept in Delta and written NULL), `not_in_delta`
  (not added yet), or `lineage`;
- `jobs`: the snapshot jobs that copied it, newest first, with what lake readers saw of it.

A table the pipeline has no trace of is `404`; a name not in the source's form is `400`.

## Every pipeline's tables

```sh
curl -s 'https://cdc.example.internal/v1/tables?state=halted,stale'
```

Every pipeline's tables, worst first, each with its `pipeline`, `node` and `listed_as`:
`halted`, `stale`, `snapshot`, `costly` (a streaming table whose last MERGE rewrote more than half
its files: random-key churn, the cost driver), `streaming`, or `copied`. `counts` gives how many
there are of each. Filter with `state` (comma-separated), `pipeline`, and `q` (text in the table's name, any
case).

A pipeline whose holder can't be asked is named under `unreachable` with why; its halted tables
are listed from storage, with `live: false`.
