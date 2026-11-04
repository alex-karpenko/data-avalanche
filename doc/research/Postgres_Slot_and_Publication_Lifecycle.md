---
updated: 2026-11-04
domain: Data Flow
tags: [postgres, replication-slot, publication, lifecycle, wal, re-snapshot, cdc]
resolves: [G-15]
---

# PostgreSQL Replication Slot & Publication Lifecycle

## Scope

Closes **G-15**. The config layer already fixes the *static* side — `slot`/`publication` charset and
**uniqueness** (V-058 per source host+db, V-059), `auto_cleanup` teardown (config §6.x). This document
specifies the **runtime lifecycle state machine**: slot creation, reconnect, orphan handling, WAL-loss
recovery, and publication reconciliation.

Builds on G-6 (incremental re-snapshot is now cheap & resumable) and the clustering lease (per-pipeline
single-runner).

---

## §1 — Slot existence state machine (D-15.1)

A PG logical slot **persists on the server across disconnects** and retains WAL — so "slot present" is
the *normal* steady state, not an error. Behavior is a function of {our persisted state} × {server
slot}:

| | **slot present** | **slot absent** |
|---|---|---|
| **state present** | **reconnect & resume** from `confirmed_flush_lsn` (normal restart) | slot lost / WAL gone → **recreate + incremental re-snapshot** (§3) |
| **state absent** | **orphan** → policy (§1.1) | **first start** → create slot, capture `start_lsn`, incremental snapshot |

The first, second (top-left), and bottom-right cells are unambiguous. The contested cell is **state
absent + slot present**.

### §1.1 — Orphan slot policy

`source.postgres.slot.on_orphan: halt | adopt_resnapshot` — **default `halt`.**

- **`halt` (default)** — refuse to start the pipeline with a clear error naming the orphan slot. The
  operator decides: drop the slot for a clean first-start, or restore the lost state. No surprise data
  movement.
- **`adopt_resnapshot`** — treat as fresh: take ownership of the slot, advance/recreate it to a new
  consistent start, run an incremental snapshot of all tables, then stream. Self-healing for
  environments that prefer automatic recovery.

**Rejected:** *adopt + resume from slot position without snapshotting* — the pipeline never captured
pre-existing rows, so it would silently miss all data before the slot's position. Never offered.

---

## §2 — Slot creation responsibility (D-15.2)

`source.postgres.slot_management: managed | require_existing` — **default `managed`.**

- **`managed` (default)** — create the slot if absent, reconnect if present. The app owns the slot
  lifecycle (and drops it on delete per `auto_cleanup`).
- **`require_existing`** — the app **never creates** a slot; if absent at start it errors. For
  locked-down environments where slot creation is a DBA-only operation. `auto_cleanup` is forced off
  (the app did not create it, so it must not drop it) — validate this combination.

`require_existing` + `on_orphan: halt` with no local state still halts (the slot exists but we have no
state) — correct: the operator/DBA coordinates the initial state.

---

## §3 — Slot loss & WAL-gone recovery (D-15.3)

Two failure shapes converge on the same recovery:

1. State present but the **slot is gone** (dropped externally, server restored from backup).
2. Slot present but `START_REPLICATION` fails with **"requested WAL has already been removed"** — the
   persisted `confirmed_flush_lsn` references WAL the server has already recycled.

In both, the persisted resume position is **unreachable**. Recovery: **recreate the slot (if needed),
capture a fresh `start_lsn`, and run an incremental re-snapshot** (G-6) — which is resumable and
reconciles via upsert idempotency, so the cost is bounded. This is the same recovery the MySQL side
uses for a purged binlog (G-26) and is the failure mode the WAL-lag guard (G-22) exists to *prevent*.

Emit a prominent warning/metric on this path — it means data was at risk of a retention gap.

---

## §4 — Publication reconciliation (D-15.4)

The publication is **app-maintained** with an **explicit table list** (not `FOR ALL TABLES`, which
would capture untracked tables). On every (re)start and on any table-set change (a PR-class restart,
config §):

- `CREATE PUBLICATION` if absent (under `managed`), else `ALTER PUBLICATION ADD/DROP TABLE` to
  reconcile membership to the resolved include−exclude set.
- A table dropped from the set is `DROP`ped from the publication; its Iceberg artifacts are retained
  (never deleted).
- Under `require_existing`, the publication is likewise expected to pre-exist; the app validates its
  membership covers the tracked set and warns on drift rather than altering it.
- **A repair covers only the WAL that follows it** (Q-M5-14, measured on PostgreSQL 14 and 18).
  pgoutput reads the publication as of each change's WAL position: a change made before the
  publication existed is skipped on 18 (`WARNING: skipped loading publication`) and fails the
  stream on 14 (`publication … does not exist`). So when a restart from a checkpoint finds the
  publication missing, or publishing fewer operations, or publishing via the partition root, the
  repair is followed by a slot recreate and a full re-snapshot, not a resume. A table added to the
  publication at start is snapshotted on its own (`Prepared::added`).
- The publication name is a case-sensitive identifier: the app quotes it in SQL and in pgoutput's
  `publication_names` (an unquoted `MyPub` folds to `mypub`). Two pipelines must not share a
  publication they manage (V-059): each would reconcile it to its own tables.

**What the publication publishes, not only which tables (DA-SRC-13, M12a, 2026-11-04).** The table
list is not the whole of a publication: a row filter or a column list on a member (PostgreSQL 15+),
`FOR TABLES IN SCHEMA` (15+), `publish_via_partition_root`, the operations, and
`publish_generated_columns = stored` (18+) each keep changes from the stream or change their
shape. `slot.rs` `publication_shape` reads them from `pg_publication_rel` (`prqual`, `prattrs`),
`pg_publication_namespace` and `pg_publication` (`pubgencols`), and the reconciliation answers each:

- **`managed`, named tables:** the publication's direct members become exactly the tracked set.
  `ADD TABLE`/`DROP TABLE` where a member can be named; `ALTER PUBLICATION … SET TABLE <tracked>`
  where a filter, a column list or a published schema can't be taken out by name (a schema's table
  isn't a member to drop, 42704). A tracked table it published before through a partitioned root
  or a schema is added by name and not copied again. The operations and the partition-root flag
  are set right. All of it runs as one implicit transaction bounded by `lock_timeout` and
  `statement_timeout`, after the slot decision.
- **A rewrite re-snapshots.** A repair that changes what a tracked table's changes carried (a
  filter or a column list on a tracked table, the operations, the partition-root flag while a
  tracked table is a partition, stored generated columns while a tracked table has one) drops and
  recreates the slot before it changes the publication, and the pipeline re-snapshots its tables as
  `snapshots.mode` says (`Reconciled::rewritten`): the WAL before the repair was decoded under the
  old definition. A via-root repair re-snapshots every table even for a leaf the publication names
  itself, conservatively: `pg_publication_tables` lists only the root under via-root, so narrowing
  it needs a measurement first (§54 of the gap tracker).
- **`require_existing` or `FOR ALL TABLES`** (a publication the app doesn't change): a tracked table
  published with a row filter or a column list **halts by itself** at resolve (`halt_filtered_tables`,
  `TABLE_NOT_REPLICABLE`, naming the one-transaction `DROP TABLE` + `ADD TABLE` that publishes it
  whole), and the other tables go on; once fixed it re-snapshots rather than resuming over WAL the
  filter decoded. Stored generated columns of a tracked table refuse the pipeline (naming
  `SET (publish_generated_columns = none)`); via-root while a tracked partition isn't named itself
  refuses (its changes would arrive under the untracked parent's name); a tracked table not
  published, or an operation not published, warns.
- **Untracked tables** the publication also publishes warn (up to five named): the server decodes
  their changes for the slot and the stream drops them (D-M5.14). A managed publication of named
  tables drops them.

---

## §5 — Mutual exclusion & external collisions (D-15.5)

- **Two workers cannot fight over one slot.** The per-pipeline **lease** (clustering) guarantees a
  single runner, and PostgreSQL itself locks an active slot to one connection
  (`replication slot "…" is active for PID …`). If that error is ever seen, it is treated as fatal —
  another holder exists — and surfaced, not retried into a busy-loop. Amended 2026-10-01 (Task 8
  review, M6): on a quick restart the old walsender can hold the slot until `wal_sender_timeout`,
  so the owner of the lease retries it a bounded number of times before treating it as fatal.
  Amended 2026-10-21 (M8a Task 13, DA-SRC-28, Q-M8-5): the owner of the lease first ends the walsender
  holding the slot when it runs as the pipeline's own role (`pg_terminate_backend`, waiting up to
  5 s). The lease proves that reader is stale: a holder that stopped without closing its connection
  (a stop-the-world pause) or this node's own dropped session. Without that, a takeover after a
  pause waited for `wal_sender_timeout` plus a backoff step, about two minutes. A reader of another
  role is never ended, so the bounded retry still covers it, and so is the external collision
  below. Amended 2026-10-22 (M8a's whole-branch review): a holder ends such a reader at most once a
  lease term. Two holders that each believe they hold the lease (two lease stores on one slot) would
  otherwise end each other in turn, splitting the slot's changes between two lakes; with the limit,
  the later busy spell gives up loudly with the bounded retry's error.
- **External (non-app) collisions cannot be detected.** PG replication slots carry **no ownership
  tag/comment**, so a slot created by an unrelated system under the same name is indistinguishable.
  Mitigation is V-058 naming uniqueness + operator discipline; documented as a known limitation, not
  papered over with a fragile marker.

---

## §6 — Config & validation impact

**Added fields**

- `source.postgres.slot.on_orphan: halt | adopt_resnapshot` (R, default `halt`).
- `source.postgres.slot_management: managed | require_existing` (PR, default `managed`).

**New validation rules**

1. `slot_management = require_existing` ⇒ `auto_cleanup` must be `false` (else error — can't drop what
   we didn't create).
2. `on_orphan` ∈ {halt, adopt_resnapshot}; `slot_management` ∈ {managed, require_existing} (domain
   checks).

**Doc edits owed (spec, propose-only):** Data-Flow.md Phase-0/Phase-2 to reference the §1 state
machine and the §3 WAL-gone → re-snapshot path; the snapshot is now the incremental model (G-6).

---

## §7 — Resolved decision summary

| Decision | Resolution |
|---|---|
| D-15.1 orphan slot | `on_orphan: halt` (default) \| `adopt_resnapshot`; resume-without-snapshot rejected |
| D-15.2 creation | `slot_management: managed` (default) \| `require_existing` (DBA-precreated) |
| D-15.3 WAL-gone | unreachable position ⇒ recreate slot + incremental re-snapshot (G-6); warn/metric |
| D-15.4 publication | app-maintained explicit list; `ALTER PUBLICATION` reconciles on table-set change, its shape too (filters, column lists, schemas, via-root, operations, generated columns: `SET TABLE` repair under `managed`, a filtered table halted otherwise, a rewrite re-snapshots) |
| D-15.5 mutual exclusion | lease + PG active-slot lock; external same-name collisions undetectable (documented) |
