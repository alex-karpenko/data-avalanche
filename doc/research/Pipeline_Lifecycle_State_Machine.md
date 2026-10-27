---
updated: 2026-10-27
domain: Data Flow
tags: [lifecycle, state-machine, pause, stop, resume, restart, flush, failover, management-api, cdc]
resolves: [G-NEW-15]
---

# Pipeline Lifecycle State Machine

## Scope

Closes **G-NEW-15** (lifecycle API state machine) and the §9.6 flush/restart-overlap gap. Defines the
desired/actual state model for `pause` / `stop` / `resume` / `restart` / `flush`, how state survives
failover and restart, and where it is persisted.

Builds on the clustering controller model (`Clustering_Architecture_Research.md` §20:
`ClusterReconciler` + per-pipeline `PipelineController`, level-triggered) and the restart policy
(config §12.4). The lifecycle endpoints inherit **operator** role from `API_RBAC_and_Versioning.md`.

---

## §1 — The principle: persisted desired-state, reconciled by the owner

Each pipeline has a **desired state** set by the operator API and an **actual state** the owning
`PipelineController` reconciles toward. The desired state is **persisted** (per-pipeline) and
**level-triggered**, so it survives both worker failover and process restart: a new owner reads the
desired state and reconciles to it. A paused pipeline stays paused; a stopped pipeline stays stopped.

This is the single idea that answers every §2.5 question.

---

## §2 — States

**Desired (operator-set):** `RUNNING`, `PAUSED`, `STOPPED`.
**Transient (controller-internal):** `STARTING`, `DRAINING`.
`restart` and `flush` are **actions**, not states.

| State | Source reading | Buffered data | Source connection | Lease / ownership |
|---|---|---|---|---|
| `RUNNING` | active | accumulating | open | held, active |
| `PAUSED` | suspended | **flushed to a clean txn boundary, then none held** | kept warm | **held**, idle |
| `STOPPED` | suspended | flushed then none | **closed** (PG slot persists on server) | **held, owner idle** |

- **PAUSE flushes to a clean transaction boundary and commits**, so a paused pipeline has **no
  in-memory data at risk** during failover/restart. Resume is fast (warm resources, no buffer to
  reconcile).
- **STOP** additionally closes the source replication connection. The PG slot is **not** dropped (it
  retains `confirmed_flush_lsn`); `auto_cleanup` applies only to *deletion*, not stop.
- **STOPPED keeps its lease** with the owner idle (actual=STOPPED). This avoids ring churn, makes
  resume immediate, and preserves the single-owner-reconciles invariant; the cost is one idle lease
  renewal per stopped pipeline (negligible). (Alternative — release-and-exclude-from-ring — was
  considered and rejected for the extra re-assignment path.)

---

## §3 — Transitions

```
            pause                       stop
  RUNNING ─────────► PAUSED      RUNNING ─────────► STOPPED
  PAUSED  ◄───────── RUNNING     PAUSED  ─────────► STOPPED   (stop)
            resume                      stop
  STOPPED ─────────► RUNNING     (resume / start)

  restart:  RUNNING ─(discard and replay, §12.4)─► STARTING ─► RUNNING   (valid only on RUNNING)
  flush:    action in RUNNING (and PAUSED is already flushed) — no state change

  out of ERROR:  ERROR ─stop─► STOPPED ─resume─► RUNNING   (reruns from Phase 0, as a restart)
```

- **`resume` targets both `PAUSED` and `STOPPED`** → `RUNNING` (answers §2.5).
- **`restart`** is valid only on a `RUNNING` pipeline; it follows the §12.4 **discard-and-replay**
  policy (drop unflushed batches, replay from last confirmed offset, dedup via G-13/G-14 idempotency)
  — identical to a config-driven restart. On `PAUSED`/`STOPPED` it is rejected (`409`).
- **Out of `ERROR`** (M6b, owner ruling Q-M6b-12, 2026-10-08). `ERROR` is the controller's
  actual state, held in memory; the desired state stays what the operator set. A `stop` clears it
  (actual `STOPPED`), and a `resume` then runs the worker again from Phase 0, as a process restart
  would. If the cause persists (the WAL still over `max_wal_lag_bytes`, the same oversized
  transaction), the rerun fails the same way: only an operator triggers it. Until M9's `restart`,
  this and a process restart are the ways out of `ERROR`.
- **As built in M9a** (D-M9a.3, D-M9a.4, confirmed 2026-10-23):
  - "`RUNNING` only" for `restart` is the **desired** state. A pipeline whose desired state is
    running but whose actual state is `ERROR` (or retrying) can be restarted, and the restart runs
    it again; desired `PAUSED`/`STOPPED`, or no lease holder, is `409`.
  - Asking for the state the pipeline is in already (`pause` on `PAUSED`) changes nothing and
    answers `200` with `unchanged`: a retried request must not read as refused. Only `pause` on
    `STOPPED` is illegal.
  - `flush` on `PAUSED` answers `200` unchanged (the pause flushed); on `STOPPED`, or with no lease
    holder, `409`.
  - A `restart` or `flush` is taken by the holder at its next poll, so its `202` says the request
    is recorded, not done. A flush taken while no worker runs (`ERROR`, or between retries) is a
    logged no-op. Each request gets a new id: a client that retries the call after losing the
    answer asks twice (DA-API-15).
- **`flush`** forces an immediate flush and **resets the time-based flush timer and the row/byte
  counters** (closes §9.6). A no-op warning if the buffer is empty.

Illegal transitions (e.g. `pause` on a `STOPPED` pipeline) return `409 Conflict` with the current
state.

---

## §4 — Persistence and failover (the §2.5 answers)

- **Where:** `desired_state` is a field in the **per-pipeline runtime state** (`pipeline-state.json`),
  **not** config and **not** `globals.state`. It is operator intent, not topology, so it stays out of
  the cross-pipeline `globals.state` boundary (§6.2) and out of the config file.
- **Config-reload independence:** a reload must **not** resurrect a paused/stopped pipeline. The
  reconcile compares config against the running set but **desired_state (runtime) wins** over config's
  implicit "should run." A reload of an unrelated field on a paused pipeline leaves it paused.
- **Failover:** the new owner's `PipelineController` reads `desired_state` from per-pipeline state and
  reconciles — paused stays paused, stopped stays stopped. Because pause/stop already flushed, there is
  no in-flight buffer to hand off.
- **Process restart:** identical — `desired_state` is durable.

---

## §5 — Controller integration

The `PipelineController` (clustering §20) gains `Paused` and `Stopped` actual-states alongside
`Owned(Running)`:

- `Owned` holds the lease and runs the §19 renewer in all three of Running/Paused/Stopped (the lease is
  retained so ownership doesn't bounce).
- Reconcile loop: `actual → desired` on every event/resync tick (idempotent). A desired change arrives
  via the API on whichever node holds the lease (or is routed by the same generation-pointer mechanism
  as config reload, §9.2 — every node can observe the per-pipeline desired-state change and the owner
  acts).
- **As built in M9a** (Q-M9-B, D-M9a.2, D-M9a.5): through storage, never by proxy. `pause`, `stop`
  and `resume` compare-and-swap `desired_state`. `restart` and `flush` are recorded in
  `requests.json` (the last of each, `{id, at}`, a fresh id per request so a retried write sets
  the same value). The owner reads the requests when its lease term starts (the baseline) and
  takes a new id at a poll. It writes `status.json` (actual state, error, halt code, degraded,
  under its epoch) when its status changes. Any node answers a status from these documents and
  the lease; a `status.json` of another holder or an earlier epoch is not shown as current.
- **As built in M9b** (Q-M9b-1, Q-M9b-4, D-M9b.3, D-M9b.4): an on-demand snapshot job is asked as
  a restart. The API writes the job (`snapshots/jobs/<job_id>.json`, queued), then a restart
  request whose id is the job's; the owner lists the unfinished jobs before it discards the
  worker, and the new worker's Phase 0 takes them. So a job is refused (`409`) exactly where a
  restart is: unless the desired state is `RUNNING` and a node holds the lease. A pipeline in
  `ERROR` takes one, and the job's restart runs it again. The owner also lists the jobs when its
  term starts, so a job survives the loss of its restart request to a takeover.

---

## §6 — Config & API impact

- **No new config field** — pause/stop are runtime API actions, not config.
- **API** (from `API_RBAC_and_Versioning.md` §2, all **operator** role): `POST /v1/pipelines/{id}/
  {pause,resume,restart,stop,flush}`; `GET /v1/pipelines/{id}` returns `{desired_state, actual_state,
  ...}`.
- **State schema:** add `desired_state: running | paused | stopped` to `pipeline-state.json`
  (default `running`).

**Doc edits owed (spec, propose-only):** System-Overview lifecycle endpoints annotated with this state
machine; Data-Flow.md shutdown/handoff to note pause/stop flush-to-boundary and desired-state
reconciliation.

---

## §7 — Resolved decision summary

| Decision | Resolution |
|---|---|
| D-NEW15.1 stopped lease | **Keep lease, owner idle** — no ring churn, immediate resume |
| D-NEW15.2 transitions | resume targets PAUSED *and* STOPPED; restart only on RUNNING; flush is an action |
| D-NEW15.3 pause flush | **Flush to a clean boundary then hold** — no in-memory data at failover risk |
| D-NEW15.4 persistence | `desired_state` in per-pipeline state; reconciled on failover/restart; survives config reload |
| §9.6 flush/restart | manual flush resets timer+counters; manual restart = §12.4 discard-and-replay |
