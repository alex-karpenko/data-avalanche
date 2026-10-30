---
title: Pausing, stopping and restarting
description: The pipeline commands, what each one does to the source and the lease, and their answers.
sidebar:
  order: 5
---

Every command is a `POST` to `/v1/pipelines/<id>/…` with the `operator` role, accepted by any node.
It records what you asked in the pipeline's state, and the pipeline's owner carries it out within
a poll. The command you gave last is the pipeline's **desired state**: it survives restarts,
failovers and config reloads, so a reload never resumes a paused or stopped pipeline.

| Command | From | Does | The source | The lease |
|---|---|---|---|---|
| `pause` | running | flushes to a transaction boundary and commits, then reads nothing | connection kept | kept |
| `stop` | running, paused | flushes and commits, then closes the source connection | closed; a PostgreSQL slot stays and holds WAL | kept |
| `resume` | paused, stopped | streams again from the checkpoint | reopened | kept |
| `restart` | running (`error` included), with a lease holder | drops what isn't committed and replays from the last checkpoint | reconnected | kept |
| `flush` | running, with a lease holder | commits what is buffered now, and restarts the flush timer; a paused pipeline has nothing buffered | — | — |

- `202`: recorded; the owner acts at its next poll.
- `200`: the pipeline is in that state already (or, for `flush`, paused with nothing to flush);
  nothing changes.
- `409`: the state doesn't allow it (`pause` of a stopped pipeline; `restart` of a paused or
  stopped one; `flush` of a stopped one; `restart` or `flush` of one no node holds).

:::caution[A stopped PostgreSQL pipeline grows the server's WAL]
`stop` keeps the replication slot, which keeps the WAL from the checkpoint on. Watch
`data_avalanche_wal_lag_bytes`, and remove the pipeline from the config if it won't come back.
:::

## Retrying a command

`restart` and `flush` are recorded as requests, one per HTTP call. A client that retries after
losing the answer asks twice, and the pipeline may restart twice. That costs a second replay from
the checkpoint, never data. Check `GET /v1/pipelines/<id>` before retrying.

## Restart and reload

`restart` reconnects with the config the pipeline runs. A changed config reaches it only through
`POST /v1/config/reload`: see [Config changes](/data-avalanche/runbooks/config-changes/).
