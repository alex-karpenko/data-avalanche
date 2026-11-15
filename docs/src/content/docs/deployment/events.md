---
title: Events and audit trail
description: The cluster's timeline of what happened and who did it, how to read it, and how long it is kept.
sidebar:
  order: 4
---

Every cluster keeps one timeline of what happened to it: the actions people took through the API,
the attempts it refused, and what the cluster did on its own. Any node reads it, from the
cluster's state location (`globals.state`), and `GET /v1/events` returns it.

## Who did it

Every call that changes something records who made it and, if they gave one, why:

```sh
curl -X POST https://avalanche.example.com:8080/v1/pipelines/orders-pg/pause \
  -u oleksii:… -H 'Content-Type: application/json' \
  -d '{"note": "the warehouse migration, back by 15:00"}'
```

The event names the caller and their role as the server verified them (`anonymous` when the API
has no authentication), the note, and how the call ended: `accepted`, `unchanged` (the pipeline
was paused already) or `refused` (a `409` or `422`, with the reason). A note is optional, at most
500 characters; any other body field is the endpoint's own, and a request without a body works
as before.

A caller whose role is too low for a call (a `403`) is recorded too, as `action.denied`, so the
trail shows attempts as well as changes. A node records at most 30 of those at once and then one
a second; the rest are logged only.

## What is recorded

| Kind | Severity | When |
|---|---|---|
| `action.<object>.<verb>` | info, warning when refused | an API action: `pipeline.pause`, `.stop`, `.resume`, `.restart`, `.flush`, `snapshot.start`, `config.reload`, `node.leave`, `node.shutdown` |
| `action.denied` | warning | a call refused for the caller's role |
| `lease.acquired`, `lease.takeover`, `lease.released`, `lease.lost` | info, warning, info, error | a pipeline starts on a node; is taken over from a node that stopped renewing (a failover); is handed off; is lost |
| `pipeline.error`, `pipeline.retrying`, `pipeline.running` | error, warning, info | a pipeline goes into `ERROR`; fails and is retried; runs again after either, or after a pause or a stop |
| `pipeline.paused`, `pipeline.stopped`, `pipeline.dumped` | info | the owner carried out a pause or a stop; a dump finished |
| `pipeline.degraded`, `pipeline.recovered` | warning, info | sink commits or state writes start failing and being retried; succeed again |
| `pipeline.removed` | info, warning | a pipeline gone from the config is drained and its source's slot and publication torn down; a warning when that waits for its lease to expire |
| `table.halted`, `table.resumed` | error, info | a table halts, with its reason; runs again after a snapshot job brought it back |
| `schema.applied`, `schema.degraded` | info, warning | a source schema change is evolved into the sink, fully or in part (a change the sink refuses halts the table) |
| `job.completed`, `job.failed` | info, error | a snapshot job ends |
| `member.joined`, `member.restarted`, `member.draining`, `member.left` | info | a node joins, restarts, starts handing its pipelines off, leaves |
| `member.suspect`, `member.alive`, `member.down` | warning, info, warning | a node stops answering probes, answers again, is declared failed |
| `config.published` | info | a config generation is published |
| `config.refused` | error | a node can't apply a generation, and runs the one before |

Each event has its time, the node that recorded it, its pipeline when it has one, its object (a
table, a job, a node, a generation), and its text. A change the whole cluster sees, such as a
node going suspect, is recorded by one node.

## Reading it

`GET /v1/events` (any role) returns the newest first, 100 at a time:

```sh
curl -s 'https://avalanche.example.com:8080/v1/events?kind=action&actor=oleksii&since=2026-11-14T00:00:00Z'
```

| Parameter | Filters on |
|---|---|
| `since`, `until` | the time, RFC 3339: from `since`, before `until` |
| `kind` | a kind or a family: `action`, `lease.takeover`; repeat it, or separate with commas, for several |
| `object` | a pipeline's id, or an object's name: `public.orders`, a job's id, a node |
| `actor` | who caused it: the name as recorded, exactly |
| `node` | the node that recorded it |
| `q` | text in the event or its note, ignoring case |
| `limit` | 1 to 1000 |
| `after` | the `next` of the page before |

A page that stopped before the end of what matches says where to go on: its `next`, passed back as
`after`. A query looks at no more than 5000 events; past that, `next` takes it further.

## How long it is kept

`globals.console.events.retention`, 30 days by default (1 day to 400 days). Every node deletes
the days past it once an hour, and a reload changes it. The events of one day are one folder,
`events/<YYYY-MM-DD>/`, in the state location, so a day expires whole.

## Keep the state location private

Events name people and describe your pipelines, their tables and their failures. Keep
`globals.state` where only the cluster and its operators can read it, not in a bucket or prefix
your lake's readers have access to.

## When the timeline has gaps

Recording an event never waits on the storage, and never fails or slows what it records. A node
retries a write that fails three times over about 6 seconds, then drops the event; and when events
come faster than it writes them (more than 1024 waiting), it drops the new ones. Each drop is
logged and counted in `data_avalanche_events_dropped_total`, and the
[`DataAvalancheEventsDropped`](/data-avalanche/deployment/observability/#alerts) alert fires on
it. A node that crashes loses the events it hadn't written yet.
