---
updated: 2026-11-10
domain: Data Flow
tags: [postgres, replication-slot, publication, lifecycle, wal, re-snapshot, failover-slot, slot-sync, cdc]
resolves: [G-15, DA-SRC-33, DA-SNAP-22]
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
  publication missing, or publishing fewer operations, the repair is followed by a slot recreate
  and a full re-snapshot, not a resume. (A via-root repair no longer is: see "A rewrite
  re-snapshots" below, DA-SNAP-22.) A table added to the
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
  filter or a column list on a tracked table, the operations, stored generated columns while a
  tracked table has one) drops and recreates the slot before it changes the publication, and the
  pipeline re-snapshots its tables as `snapshots.mode` says (`Reconciled::rewritten`): the WAL
  before the repair was decoded under the old definition.
- **A via-root repair copies only what the root hid (DA-SNAP-22, M12d, 2026-11-10).** Measured on
  PostgreSQL 18, for a via-root publication that names the root and a leaf: `pg_publication_tables`
  lists only the root, and `pg_publication_rel` lists both. So every tracked partition the
  publication published through its root, the leaf it also names included, is missing from what
  the server says it publishes, and is in `Reconciled::added`: it is copied alone, as
  `snapshots.mode` says, while the slot and the other tables' stream stay. The partition-root flag
  is no longer part of `rewritten` (D-M12d.6). Before M12d a via-root repair re-snapshotted every
  table.
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
| DA-SRC-33 failover slots (M12d) | a managed slot is a failover slot on PostgreSQL 17+; a promotion the slot followed, confirmed exactly at the checkpoint with no lake marker past it, continues the stream under either `source_change_action` (§8) |
| DA-SRC-31 decoding on a standby | deferred past v1 (Q-M12d-1, ⏭); what v2 needs in §8.6 |

---

## §8 — Failover slots (DA-SRC-33, M12d)

Added 2026-11-10 (M12d Tasks 5 and 8, rulings Q-M12d-1, Q-M12d-2, Q-M12d-5). Before M12d a
promotion always took the source-change path (DA-SRC-20): a halt `WAL_SOURCE_CHANGED` under
`source_change_action: error`, a recovery and a snapshot under `re_snapshot`. PostgreSQL 17's slot
synchronization keeps a copy of a logical slot on a physical standby, so a promotion can carry the
pipeline's slot over.

### §8.1 The creation flag

`create_slot` reads `current_setting('server_version_num')::int >= 170000` and, on 17+, creates
the slot with `pg_create_logical_replication_slot($1, 'pgoutput', false, false, true)` (the fifth
argument is `failover`); before 17 it keeps the two-argument form. Only a slot the pipeline creates
gets the flag: under `require_existing` the DBA creates it (with `failover` or not), and a slot made
before an upgrade to 17 stays a plain slot. The pipeline doesn't alter an existing slot (that takes
`ALTER_REPLICATION_SLOT` on a replication connection), so such a pipeline never continues. The IT
`a_managed_slot_is_a_failover_slot_from_postgresql_17` passes on 14 (no flag), 17 and 18.

The standby needs `sync_replication_slots = on`, `hot_standby_feedback = on` and `dbname` in its
`primary_conninfo`; the primary names the standby's physical slot in `synchronized_standby_slots`.
test-infra's `postgres-sync-primary` / `postgres-sync-replica` pair (55422/55423) is set up so.

### §8.2 When the stream continues: `slot::continues`

`SlotState` gains `failover` and `synced`, read version-independently
(`coalesce((to_jsonb(s) ->> 'failover')::bool, false)`). `continues(recorded, now, slot, marked)`
is true only when all of these hold:

1. **The same system on a later timeline:** the checkpoint's server and the server now share the
   `system_identifier`, and the timeline now is strictly later.
2. **A followed slot:** the slot here is `failover` and `synced`, and not invalidated (`wal_status
   = 'lost'`, `conflicting`, or an `invalidation_reason`).
3. **No lake marker past the checkpoint:** `marked`, the highest position any Delta table's marker
   names (every table the sink holds, and every table resolved or cached; a marker another source
   kind wrote is left out; floored at the checkpoint), is read and is at most the checkpoint.
4. **Confirmed exactly at the checkpoint:** the slot's `confirmed_flush` equals the checkpoint.

Otherwise the start takes the source-change path as before, which honours `source_change_action`.

**Why it is sound.** A standby synchronizes a slot only to a position its own WAL holds: the sync
worker refuses a slot whose position needs WAL the standby lacks (measured, PostgreSQL 18). That
position is at or before the timeline's switch point, since a promotion replays everything the
standby received. Every lake commit is at or below its table's marker, which (3) bounds by the
checkpoint, which (4) equals the slot's position. So all of the lake is shared history, and the
new timeline's changes all come after every marker: nothing is skipped as covered. The switch
point itself is never read (Q-M12d-5, "slot position only": SQL only, no `TIMELINE_HISTORY`
client); the slot's position bounds it. A marker past the checkpoint (a crash between a sink commit
and the checkpoint's save) refuses the continuation: it may cover new-timeline LSNs the lake never
saw.

**Why "exactly", not "at least".** The plan's first rule (and the approved P-2/P-5 text) accepted a
slot confirmed at or past the checkpoint. The pipeline acknowledges only a durable checkpoint
(the writer saves it, then the stream acknowledges it), so its own slot never confirms past it. A
slot that did was moved by something else (a `pg_replication_slot_advance`, an older restored state,
another reader), the changes between are lost, and `decide` would never have resumed it anyway.
Task 8's fix round made the rule `confirmed_flush == checkpoint` so that such a start takes the
source-change path and honours the action instead of re-snapshotting silently; Oleksii approved
rewording P-2 and P-5 to match (2026-11-10).

### §8.3 Where it is decided

- **At a start.** `Preflight::server` names the server reached. When the checkpoint names another,
  the worker reads `marked` (one `held_tables` listing and one `table_state` per table, on that
  start only, D-M12d.9) and calls `SourceConnector::prepare_marked`. The PostgreSQL connector reads
  the slot before the source-change check; a continued start runs `decide` as for an unchanged
  server. The warning — "the checkpoint was read on <old>, and the pipeline now reads <new>:
  replication slot <slot> came over synchronized up to the checkpoint, with no marker of the lake
  past it, so the stream continues on the new timeline" — is logged only when the final action is
  a resume (a continued slot can still be refused, or re-snapshot for a publication the repair
  rewrote). Every other source keeps `prepare` through the trait's default; MySQL's preflight
  returns `server: None`.
- **At a reconnect (P-6).** `slot::reconnect_check` returns `SourceError::SourceChanged` for the
  same system on a later timeline under either action, so the pipeline reruns its start, which
  continues or halts as above. A running pipeline's failover is therefore continued as a stopped
  one's is. The message ("… a promoted server of the same system: the pipeline starts again, and
  its start decides whether the stream continues there …") carries no `WAL_SOURCE_CHANGED:` prefix,
  so alerting on the code isn't paged by each continuation. Another system, or an earlier timeline,
  still halts at once under `error`.
- **The checkpoint is re-recorded (D-M12d.11).** A resume whose server differs from the
  checkpoint's saves the checkpoint document with the new server before the stream opens.
  Otherwise the stream's open would see the old server recorded and refuse; the E2E found it.
- The continuation applies under either `source_change_action` (D-M12d.10): no position is lost,
  so neither action has anything to decide. An operator who chose `error` to hear of every
  failover hears of it through the warning.

### §8.4 Measured (PostgreSQL 18, the `postgres-sync-*` pair, 2026-11-06)

- **The first sync waits on `catalog_xmin`.** The standby's first copy of a failover slot waits
  until the slot's `catalog_xmin` passes the standby's own. A slot no one reads never gets there;
  the streaming pipeline moves it, nudged by `pg_log_standby_snapshot()` on the primary.
- After `pg_promote()` the slot shows `failover`, `synced`, not `temporary`, the primary's
  `confirmed_flush`, and not `conflicting`.
- `TIMELINE_HISTORY 2` gives the switch point, and `pg_control_checkpoint()` still shows timeline 1
  right after the promotion (the timeline is read from `pg_walfile_name(pg_current_wal_lsn())`).
- The sync worker refuses a slot whose position needs WAL the standby lacks.
- The E2E `a_promotion_the_failover_slot_followed_continues_the_stream` keeps the pipeline running
  through the promotion (its address moved by a TCP proxy) and checks the lake, no re-snapshot, the
  reconnect's and the continuation's log lines, and no `WAL_SOURCE_CHANGED`. It runs on PostgreSQL
  18 only; that `synced` stays true after a promotion on 17 is documented upstream, not measured
  here.

### §8.5 Operational limits

- **A crash under load usually halts.** The sync worker copies the slot periodically (its nap
  grows while nothing changes), not at each acknowledgement, so the standby's copy lags the
  pipeline's last acknowledgement. After a crash of a busy primary the copy is usually behind the
  checkpoint and the start takes the source-change path: safe, a halt or a recovery as configured.
  A planned switchover (writes stopped, the copy caught up, then the promotion) continues
  reliably; that is what the E2E proves.
- **A down standby stalls the pipeline.** On 17+ a failover slot's walsender waits for every
  physical slot `synchronized_standby_slots` names: a standby that is down or far behind stalls
  the pipeline, with only a server-side warning. A plain slot never waited.
- **Pre-17 slots** stay plain after an upgrade (§8.1).
- **An alternation can carry no code.** The alternation halt (DA-SRC-34) takes its code from the
  last `SourceChanged` text; the promoted-reconnect message has none. Three promoted-timeline
  reconnects in a row with no durable checkpoint between would halt without `WAL_SOURCE_CHANGED`
  in the status (a rare case: each start on the old server streams, each reconnect reaches the
  promoted one).

### §8.6 Decoding on a standby: deferred (DA-SRC-31, Q-M12d-1)

The source reads a primary; a standby is refused at the start on every version. Logical decoding
on a standby (PostgreSQL 16+) is deferred past v1 (⏭). v2 needs: the standby's
`hot_standby_feedback`; a slot made on the standby; `pg_log_standby_snapshot()` on the primary for
the slot's first consistent point; and the timeline handling of DA-SRC-33.
