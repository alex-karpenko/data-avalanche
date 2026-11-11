---
updated: 2026-11-11
domain: Data Flow
tags: [delivery-guarantee, exactly-once, gtid, lsn, idempotency, checkpoint, multi-table, reposition, source-change, cdc]
resolves: [G-14, DA-X-12, DA-SRC-32, DA-SRC-34]
sources: []
---

# Delivery Guarantee and the Position Guard

## Scope

Closes **G-14**. Defines the committed-position comparison for both engines (numeric LSN vs GTID-set
subset), the scope of the exactly-once guard (per-table vs per-commit), the safe pipeline resume point
under non-atomic multi-table commits, and the guard's true role given the G-13 dedup design.

Builds on `Ghost_Row_Dedup_and_Primary_Keys.md` (G-13) and the Phase-4 `committed_lsn` guard in
Data-Flow.md §"Exactly-once guard".

---

## §0 — Why the existing guard is incomplete

Data-Flow.md's guard is `committed_lsn ≥ flush_lsn` read from the Iceberg snapshot summary. Two
problems:

1. **GTID is a set, not a scalar.** A MySQL position is `server-uuid:1-100,uuid2:1-50`. `≥` is
   undefined on it; the correct test is **subset**.
2. **Multi-table flushes are not atomic.** Each tracked table is its own Iceberg table and thus its
   own commit. A flush touching tables A and B is *two* commits; a crash between them commits A and
   not B. There is no single pipeline-wide committed position — each table has its own, and the safe
   resume point is the **laggard's**.

A third, subtler issue: flush boundaries are time/size-driven, so a post-crash replay regroups
transactions into **different batches**. A whole-batch subset check can then be partially true, which
a naive guard mishandles.

---

## §1 — The insight: idempotency is the correctness mechanism (D-14.3)

With G-13 decided, every tracked table has a PK and is written with **PK-keyed upsert + mirror
deletes**:

- Re-inserting a key re-writes the identical row (equality-delete old + insert new) → no duplicate.
- Replaying a delete is a harmless no-op on an already-deleted key.
- The only non-idempotent shape — PK-less / append-only tables — is **rejected** (G-13 §3).

Therefore the pipeline is **row-level idempotent regardless of how batches re-align on replay.**
Correctness does **not** depend on the position guard.

**Consequence:** the committed-position guard is demoted to:

- a **performance optimization** — skip re-writing a flush that is already durably committed; and
- the **checkpoint-advance** mechanism — record how far each table has durably progressed.

This removes any need for deterministic batching or row-level position filtering. The batch
re-alignment problem in §0 simply dissolves: a re-aligned replayed batch either re-writes idempotent
rows (correct, just wasted work) or is skipped by the guard (optimized).

---

## §2 — Position model (D-14.1)

A `SourcePosition` abstraction with one implementation per engine:

| Op | PostgreSQL (LSN, `u64`) | MySQL (GTID-set) |
|---|---|---|
| `covers(other)` (is `other` already included?) | `self >= other` | `other ⊆ self` |
| `union(other)` (advance on commit) | `max(self, other)` | set union, merge intervals |
| `intersect(other)` (resume frontier) | `min(self, other)` | set intersection |
| serialize | LSN string (e.g. `0/1A2B3C4`) | **canonical** GTID-set string |

**Canonical GTID-set serialization** — UUIDs sorted ascending, intervals per UUID merged and sorted
(`uuid:1-100:150-200`). A canonical form makes the stored string stable to parse and compare and lets
two positions be equality-checked without re-parsing ambiguity.

PG 64-bit LSN has no practical wraparound. A source-primary failover introduces a second `server_uuid`
into the GTID set; `union`/`covers` handle multiple UUIDs naturally.

---

## §3 — Guard scope and the safe resume point (D-14.2)

- **Per-table high-water mark.** Each Iceberg table's snapshot summary carries its own
  `cdc.committed_position`. The guard compares a flush's per-table last change against that
  table's **latest own** marker (below): if `table.committed_position.covers(flush.last_change_for_table)`
  → already committed, skip writing that table. The last change is the commit position of the
  flush's last transaction that wrote the table, not the flush frontier the marker records: a
  replay's flush may end past the marker (a keep-alive, another table's transaction) while
  carrying only transactions the table has (DA-X-12, M6b 2026-10-08). A split-commit slice is the exception: its range
  ends at its transaction's pinned start, so it is skipped only when the stored position is
  **strictly past** that start (`Large_Transaction_Handling.md` §1; spec edit S-P).
- **"Latest own", not "latest" (DA-X-10, 2026-09-26).** Foreign commits (external compaction,
  `expire_snapshots`, other engines) produce snapshots without `cdc.*` properties. The guard walks the
  snapshot log back to the newest snapshot whose summary carries `cdc.pipeline = <pipeline_id>` and
  reads its `cdc.committed_position`. If no own snapshot is retained (expired by external
  maintenance), the table is treated as **not covered**: the flush is written and idempotency (§1)
  absorbs any re-delivery. The guard never blocks progress and never skips on missing evidence.
- **Pipeline resume checkpoint = the laggard.** The single position persisted to state storage
  (`slot.json` / `position.json`) is the **`intersect` (MySQL) / `min` (PG) across all tracked tables** —
  the position every table has durably passed. On restart the single replication stream resumes from
  there; tables already ahead skip via the guard (or, failing that, idempotency absorbs the
  re-delivery).

Worked example (a transaction `T` at LSN 90 touches tables A and B; A committed it, B did not):
intersection excludes `T`, so resume re-streams from ≤90; A's guard sees `T` already covered and skips
A's rows; B writes `T`'s rows. Consistent, no loss, no duplicate.

---

## §4 — Snapshot-summary property (D-14.4)

Generalize the Data-Flow.md `committed_lsn` scalar to:

- **`cdc.committed_position`** — engine-tagged string: PG LSN, or canonical GTID-set. Written into
  each table's Iceberg snapshot summary on every successful commit, as `union` of the prior value and
  the flush's per-table position. The "prior value" is the latest **own** snapshot's value (§3).
- **`cdc.pipeline`** — the pipeline `id`, written alongside, so own snapshots are identifiable during
  the walk-back. (Delta sink equivalent: `commitInfo` `data-avalanche.committed_position` /
  `data-avalanche.pipeline`, same walk-back rule — probed in `prototypes/delta-capability/` P3.)

It coexists with **`cdc.owner_epoch`** (clustering D-7, sink-side fencing). The two are orthogonal:
`owner_epoch` rejects a zombie writer; `committed_position` skips already-done work.

The `_lsn` metadata **column** (per-row commit position) and the per-change **`_seq`** (G-13 dedup
ordering) are distinct from `committed_position` (per-commit, per-table durable HWM) and remain as
specified.

**Capability gate + fallback (DA-SINK-2 — ✅ verified on the append path: probes G2 and G2b,
`prototypes/iceberg-capability/`, 2026-09-26; the table-property carrier commits atomically with the
data files).** Both
`cdc.committed_position` and `cdc.owner_epoch` rely on iceberg-rust being able to read the latest
snapshot summary and set custom string properties on the snapshot it commits, atomically with the data
commit, with the values surviving across commits (summaries are per-snapshot, so the writer re-asserts
them each commit). This must be verified against the pinned crate. **If it fails, correctness is
unaffected** — per §1, idempotency (G-13) is the correctness mechanism and this guard is only an
optimization + checkpoint-advance. Fallbacks, in order: (1) use Iceberg **table properties** if they
commit atomically with the data files; (2) move `committed_position` to the per-pipeline **state
storage** HWM (a fully adequate home, since the guard is an optimization, not a guarantee); (3) drop
the in-summary `owner_epoch` fence to advisory — it is explicitly defense-in-depth (`Clustering…`
§5.5), backstopped by the metadata-pointer CAS and `_lsn`/PK-upsert idempotency.

---

## §5 — Multi-table atomicity limitation (D-14.5)

Because each table commits independently, **cross-table atomic visibility is not guaranteed**: a
reader may briefly see transaction `T` in table A but not yet in table B. What *is* guaranteed is
per-table exactly-once-effect (via idempotency) and a consistent per-table resume.

This is the same tension that G-29 (large transactions vs the txn-atomic-flush rule) must reconcile;
if true cross-table atomic visibility is ever required it would need a multi-table Iceberg transaction
(e.g. a catalog supporting multi-table commit such as Nessie) — out of scope here and noted for G-25.

---

## §6 — Edge cases

- **Binlog/WAL purged below the resume frontier** → the frontier is unreachable; fall back to
  re-snapshot (cross-ref G-26 `binlog_purge_action`, and PG slot-retained WAL via G-22).
- **Source-primary failover (MySQL)** → new `server_uuid` appears; handled by set ops in §2.
- **Empty flush for a table** (a multi-table flush where this table got no rows) → its
  `committed_position` simply does not advance; correct.
- **Guard disabled** (config opt-out) → still correct, just re-writes idempotent rows after a crash.

---

## §7 — Resolved decision summary

| Decision | Resolution |
|---|---|
| D-14.1 comparison | `SourcePosition` trait: PG numeric LSN; MySQL GTID-set subset/union/intersect; canonical GTID serialization |
| D-14.2 scope | Per-table HWM in snapshot summary; pipeline checkpoint = min/intersection across tables |
| D-14.3 correctness basis | **Idempotency (G-13) is correctness**; position guard is optimization + checkpoint-advance |
| D-14.4 property | Generalize `committed_lsn` → `cdc.committed_position` (engine-tagged); coexists with `cdc.owner_epoch` |
| D-14.5 limitation | Cross-table atomic visibility not guaranteed (separate commits); documented, cross-ref G-29/G-25 |
| DA-SRC-32 reposition set (M12d) | a source change repositions every table the sink holds ∪ resolved ∪ cached, under the key the table was made with; a refused one halts `SINK_REJECTED` (§8.1) |
| DA-SRC-34 alternation (M12d) | three runs in a row ending in a server change, none with a durable checkpoint past its start, halt the pipeline with the change's code (§8.2) |

---

## §8 — A changed source server (M12d)

Added 2026-11-10. A source change (another PostgreSQL system or timeline, another MySQL server in
file/position mode, DA-SRC-20) makes the new server's positions incomparable with the stored
markers: they may sort below them, and the guard (§3) would skip the new server's changes as
covered. The recovery (`source_change_action` / `binlog_purge_action: re_snapshot`) therefore
commits a **reposition** change set to each table before the checkpoint naming the new server is
saved: no rows, and a commit that stores its own position instead of the union (Data-Flow Phase 4).

### §8.1 The reposition set (DA-SRC-32, Task 6)

**Every table the lake holds, not only those resolved or cached now.** Before M12d the set was
the tables the pipeline resolved at that start or had a cached schema for. A job's copy, or a table
taken out of the config whose schema was never cached, kept the old server's marker and, once
tracked, skipped the new server's changes up to it. The set is now
`held_tables() ∪ resolved ∪ cached`, a `BTreeSet`:

- **`Sink::held_tables`** lists the tables under the sink's prefix. The Delta sink lists
  `<schema>/<table>/` two levels deep (`list_with_delimiter` on the store `DeltaTableBuilder`
  builds for the sink root, so S3, GCS, Azure and a local root alike), skips the co-located state
  directory, unescapes each segment (`unescape_table_segment`, which accepts only the canonical
  form the escape makes: a segment is a table's only if `escape_table_segment` reproduces it), and
  keeps a table root whose `_delta_log/` lists at least one entry (one page, first entry only).
- **The recorded key.** `SinkTableState::identifier` is the key the table was created with (the
  Delta sink records it). `commit_empty` repositions under it, else under the key the pipeline has
  for the table (cached, else resolved), since a reposition carries no row and a wiped state's
  table may be keyed otherwise than the source is now (D-M12d.7). A held table that records no
  key and that the pipeline neither resolves nor caches has no key to commit under: it is skipped,
  `Ok(false)`, with a warning ("its marker stays on the old server; track it and re-snapshot it
  before its changes stream").
- **A refused reposition halts that table.** A `SinkError::Permanent` on one table's reposition (a
  foreign-format table, a key it can't take) records a `TableHalt` `SINK_REJECTED` ("the source
  changed, and its reposition fails: …") and the start goes on. Before, it ended every start. A
  transient error is retried; any other error ends the start.
- **The halt stays while the lake holds the table (DA-SRC-37, M12c).** The reposition is never
  retried: the checkpoint already names the new server, and holding it back until every reposition
  succeeds would make an always-refusing table fail every start, the regression the halt closes.
  So a later start keeps a refused reposition's halt (its detail begins `the source changed, and its reposition fails`, the code's `REPOSITION_REFUSED`) of a
  table the pipeline doesn't track while the sink still holds it (`table_state(..).exists`), where
  it used to clear the halt of every table neither configured, cached, requested nor copied. Tracked
  later, the table stays halted rather than skipping the new server's changes up to its old marker;
  a snapshot job's copy ends the halt, and so does the table leaving the lake (a dropped or moved
  Delta table). Test: `a_refused_repositions_halt_stays_while_the_lake_holds_the_table`.
- The same listing feeds `highest_marker` for DA-SRC-33's continuation
  (`Postgres_Slot_and_Publication_Lifecycle.md` §8).
- Tests: `a_source_change_repositions_a_copy_the_pipeline_doesnt_track`,
  `a_refused_reposition_halts_that_table_and_the_start_goes_on`,
  `a_held_table_with_no_key_is_warned_about`; `the_sink_lists_the_tables_it_holds` (Delta); the M12a failover
  E2E on RustFS runs the listing under `re_snapshot`.

### §8.2 A host that alternates between servers (DA-SRC-34, Task 7)

A host behind a load balancer or a DNS name over several servers reaches another server at each
connection. Under `re_snapshot` each run would recover, and the next would find the first server
again: a loop of recoveries. The controller counts them:

- `WorkerStatus::started_from` is the checkpoint the worker's first stream session started from. A
  run **progressed** when its checkpoint at the end is present and differs from it.
- A run that ends `SourceError::SourceChanged` after progress sets the count to 1; one without
  progress adds 1, unless it ends more than `SOURCE_CHANGE_WINDOW` (10 minutes) after the change
  counted before, which sets it to 1 too: an alternation reruns within the restart backoff, a
  minute at most, and failovers further apart are failovers (DA-SRC-38, M12c). Any other end sets
  it to 0, and so does a stop (D-M12d.8).
- At `SOURCE_CHANGES_IN_A_ROW = 3` the pipeline goes to `ERROR` with the change's code (the prefix
  of the last `SourceChanged` message, `WAL_SOURCE_CHANGED` or `BINLOG_SOURCE_CHANGED`) and the
  message "<code>: 3 runs in a row found another source server than the run before, and none made
  a checkpoint durable: the source's address reaches more than one server (a load balancer, or a
  DNS name over several). Point the pipeline at one server, then stop and resume it. The last: …".
- A single failover can't reach it: its reconnect makes the count at most 1, and the rerun's start
  either streams (the next end resets it), or refuses with a `Permanent` (0, a halt).
- A PostgreSQL reconnect to a promoted server of the same system asks the rerun with a message
  prefixed `WAL_SOURCE_CHANGED` too (M12c; until then it carried no code, DA-SRC-38), so an
  alternation of such reconnects halts with the code.
- Tests: `runs_that_keep_finding_another_server_halt`,
  `a_durable_checkpoint_starts_the_count_of_server_changes_again`,
  `server_changes_far_apart_dont_halt`, `an_alternation_halts_with_the_code_its_change_names`.
