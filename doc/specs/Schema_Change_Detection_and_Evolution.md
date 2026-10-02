---
updated: 2026-10-02
domain: Architecture
tags: [architecture, schema-evolution, ddl, cdc, postgresql, mysql, iceberg, delta-lake, multi-sink, arrow]
status: design reviewed & approved — unified trigger→augment→diff model
sources:
  - https://www.postgresql.org/docs/18/logical-replication.html
  - https://www.postgresql.org/docs/18/protocol-logical-replication.html
  - https://www.postgresql.org/docs/18/logical-replication-restrictions.html
  - https://dev.mysql.com/doc/refman/8.4/en/replication.html
  - https://dev.mysql.com/doc/refman/8.0/en/mysqlbinlog-row-events.html
  - https://iceberg.apache.org/docs/latest/evolution/
  - https://iceberg.apache.org/spec/
related-gaps: [G-18, G-19, G-19a, G-19b, OQ-DT2, OQ-DT3]
---

# Schema Change Detection & Schema Evolution

Design for handling source-side DDL in the CDC pipeline, for **PostgreSQL logical
replication (pgoutput)** and **MySQL binlog (ROW format, GTID)**. Feeds the design work
tracked as **G-19** (DDL safe/unsafe classification), **G-19a** (MySQL DDL handling) and the
open questions **OQ-DT2 / OQ-DT3**. It assumes the Phase 2–4 flow in `Data-Flow.md` and
updates the `schema_evolution` contract in `Configuration_Specification.md` §9.7 (formerly `catalog.schema_evolution`, §9.5.1)
(see §10–§11: the `method` field is removed).

---

## 0. The correctness invariant

Everything in this document depends on one rule. The source-catalog query is the authority
for **completeness** (what the columns are); in-band signals are the authority for **timing
and ordering** (when and where a change takes effect in the stream).

> **A source-catalog query reflects the schema *now*, which may be *ahead* of the stream
> position currently being processed.** The query therefore supplies the **complete
> attributes of the change the in-band signal implicates** — applied **in stream order** and
> **scoped to that change**. It must **never** fast-forward the target schema past the
> stream position by adopting the whole "now" schema, **except** when the change cannot be
> delivered by the stream at all (a non-replayable gap), which instead triggers a
> re-snapshot (§8.2).

Violating this rule is silent data loss, not an error: applying a column *drop* or *narrow*
early discards the values still arriving in the stream for the rows that predate the real
DDL position. Additive changes are self-correcting (a column added "early" simply reads NULL
for older rows); destructive ones are not.

---

## 1. The problem, decomposed

"Schema evolution" is two separate problems with different owners and correctness properties:

1. **Detection** — *noticing* that a source table's structure changed. **Source-specific**:
   PostgreSQL and MySQL expose changes through different channels.
2. **Evolution** — *reacting* by mutating the target table schema (through the pipeline's sink,
   `sink.type`) and the in-flight Arrow builders. **Source-agnostic** and **sink-mediated**: the
   classification (§7.2) states the intent; each sink applies it within its declared capabilities
   (§7.1), exactly like the per-sink Hop 3 of the type mapping (G-18).

Detection answers three sub-questions that must be handled independently:

| Sub-question | Why it matters | Handled by |
|---|---|---|
| **(a) Decode correctness** — the *physical layout* of the bytes being decoded right now. | Decoding against the wrong layout is silent corruption. | The row decoder (Phase 2), on **every** event, from the in-band layout signal. Non-negotiable. |
| **(b) Idle-table changes** — a schema changed though no rows have flowed. | An in-band signal can't fire without DML; only a poll catches it. | The idle rescan (§3, channel D). |
| **(c) Semantic intent** — *rename* (preserve data + field ID) vs *drop + add* (lose lineage); *widen* vs *narrow*. | Determines the right target-schema op and whether history survives. | Catalog augmentation (§3, channel C). |

Consequence: **(a) is mandatory and in-band regardless of any configuration** — the decoder
self-heals its layout cache from the in-band signal that precedes the first row after a
(re)connect. The detection design below governs **(b)** and **(c)** and the evolution
decision, not whether the decoder tracks layout.

---

## 2. What each source exposes

### 2.1 PostgreSQL (pgoutput)

- **DDL is not replicated** (unchanged through PostgreSQL 18). `ALTER TABLE` never appears as
  a DDL event; the docs are explicit that schema must be kept in sync out-of-band and that
  replication simply *errors* when arriving data no longer fits.
- **The Relation message (`R`) is the in-band trigger.** Before the first `I`/`U`/`D` for a
  relation — and again whenever its tuple descriptor changed since last sent on this
  connection — pgoutput emits a Relation carrying `relation_oid`, names, replica-identity,
  and a column list of `(flags, name, type_oid, atttypmod)`. The decoder already caches this.
- **The Relation message does *not* carry** nullability, defaults, constraints, or any
  **stable column identity** — a rename looks identical to drop+add.
- **`pg_attribute` carries a stable identity.** `(attrelid, attnum)` survives a rename;
  dropped columns become `attisdropped = true` and their attnum is never reused. This is what
  makes rename positively provable on PostgreSQL (§6).
- **Trigger blind spot:** changes that don't alter the tuple descriptor pgoutput sees (a bare
  nullability tighten, a `CHECK` addition) may not emit a Relation message. Acceptable here —
  those are Ignorable for the target (§7) — with the idle rescan as backstop.
- **`DROP TABLE` is not replicated either.** A dropped table just stops emitting. It is detected
  out-of-band: its relation OID is gone from `pg_class`. Channel D finds it in steady state and
  channel A at the next start; the table then halts (§7.2, `TABLE_DROPPED`). MySQL delivers the
  DROP in-band on channel B.

### 2.2 MySQL (binlog, ROW format)

- **DDL is in-band, as statement text.** Even under `binlog_format = ROW`, DDL is written as
  a statement-based `QUERY_EVENT` containing the literal SQL. This is the in-band trigger.
- **`TABLE_MAP_EVENT` is the physical-layout channel** (column types + nullability), parsed
  by the decoder for (a); file-scoped, resets on `ROTATE_EVENT`.
- **Column names are conditional.** `TABLE_MAP_EVENT` carries names, PK info, and ENUM/SET
  strings **only when `binlog_row_metadata = FULL`** (server default `MINIMAL` → positional
  `@1, @2`). Under `MINIMAL`, names come from the catalog query — which the augment model
  performs anyway, so this degrades gracefully.
- **`binlog_row_image = FULL`** is required for complete before-images (delete/update keying).

---

## 3. The detection model: four channels, one pipeline

Detection is **not** a choice between competing methods. The system runs four channels that
feed the same evolution pipeline; each has a distinct role. (This replaces the earlier
`method: query|events` framing — see §10.)

```
                                          ┌─────────────────────────────┐
  A. Startup reconciliation ──────────────►                             │
  B. In-band trigger ─────────► C. Catalog ►   normalized SchemaDelta   ►  Evolver (§7,§8)
     (PG Relation diff /          augment    │   (full attrs, in order,  │
      MySQL QUERY_EVENT DDL)      (query)     │    scoped to the change)  │
  D. Idle rescan (periodic) ──────────────────►                          │
                                          └─────────────────────────────┘
```

### Channel A — Startup reconciliation (every pipeline start/restart)

Query the source catalog for each tracked table; then:

- **Table absent in target** → use the queried schema as the **baseline** to create the
  target table via the sink (consistent with the snapshot read point, `Data-Flow.md` Phase 1).
- **Replayable existing table** → **do not** evolve the target from this query. The decoder
  re-acquires its layout from the in-band signal at stream resume, and any DDL between the
  confirmed offset and now is replayed **in order** by channels B+C. The startup query here is
  a **drift check / alert** only.
- **Non-replayable gap** (PG slot invalidated / WAL recycled; MySQL binlog purged — G-26) →
  the DDL events and intervening rows are lost; baseline the target to the queried schema and
  trigger a **re-snapshot** (§8.2).
- **Tracked table gone from the source** (PG: relation OID absent from `pg_class`) → halt that
  table with `TABLE_DROPPED`; the target is kept (§7.2).
- **Source column named like a reserved lineage column** → halt that table with
  `RESERVED_COLUMN_COLLISION` (`Configuration_Specification.md` §5).

Channel A is *not* redundant with in-band replay: it is the only thing that covers table
creation and the non-replayable-gap recovery, which the stream structurally cannot deliver.

### Channel B — In-band trigger (real-time, steady state)

The trigger establishes **when/where** (exact stream position) and **which table** is
implicated. It does not need to extract the full delta.

- **PostgreSQL:** diff the incoming Relation message against the cached descriptor; any
  non-trivial delta is a trigger at that LSN.
- **MySQL:** classify the `QUERY_EVENT` as DDL (vs `BEGIN`/`COMMIT`) and extract the affected
  table name(s) and the default schema (`QUERY_EVENT.db`). This is the **reduced G-19a
  scope** — a DDL classifier + table-name extractor (plus optional `RENAME`/`CHANGE COLUMN`
  keyword detection for rename intent), **not** a full ALTER-delta parser. Much lower risk
  than full DDL parsing; an unparseable DDL on a tracked table fails safe (halt that table).

### Channel C — Catalog augmentation (on every B trigger)

On a trigger for table T, query the source catalog (`pg_attribute` / `information_schema`)
for T and produce the complete, unambiguous delta by diffing against the **cached
as-of-position schema** — supplying full attributes (types, nullability, defaults; PG
`attnum`; MySQL rename intent from the channel-B keyword). Scoped and ordered per §0.

- Runs on a regular (non-replication) connection; briefly stalls the evolve step (§8) — fine,
  DDL is rare. **Query failure → halt the implicated table (fail-safe)**, never guess.

### Channel D — Idle rescan (periodic, low frequency)

A single **batched** catalog query across all tracked tables, every `rescan_interval`
(default 60m, `0` disables). Catches DDL on tables with no DML — its value is
**downstream-schema freshness** and **early alerting**, not data correctness (a truly idle
table has no rows to write under the new schema, and the first write would trigger B+C
anyway). Lower-criticality than A–C. On PostgreSQL it is also the steady-state detector for a
**dropped tracked table** (relation OID gone from `pg_class` → `TABLE_DROPPED`, §7.2). Latency is
bounded by `rescan_interval` (with `0`, until the next start). Nothing is lost while detection waits,
because a dropped table emits nothing further.

### Channel comparison

| Channel | Trigger | Role | Timing | Engine mechanism |
|---|---|---|---|---|
| A Startup | pipeline (re)start | create baseline / drift check / gap→re-snapshot | once per start | catalog query |
| B In-band | DML / DDL in stream | when + where + which table | real-time, exact order | PG Relation diff · MySQL QUERY_EVENT DDL-classify |
| C Augment | a B trigger | complete attributes of the implicated change | per change (brief stall) | catalog query + PG `attnum` / MySQL rename keyword |
| D Idle | timer | idle-table freshness + alerting | every `rescan_interval` | batched catalog query |

---

## 4. Why this beats single-method detection

- **Completeness without full DDL parsing.** B is only a trigger; C supplies attributes from
  the catalog. MySQL no longer needs `sqlparser-rs` to extract the column delta — only to
  classify DDL and pull the table name (G-19a shrinks accordingly).
- **No ambiguity.** C resolves rename, nullability and type details that the in-band payloads
  omit (§6).
- **No lag window for active tables.** B fires at the exact stream position, so the evolve
  boundary is exact (§8.1); the old poll-window hazard (rows decoded against a stale cached
  schema between a real DDL and a delayed poll) is gone for tables that receive DML.
- **Idle tables and unobservable gaps are still covered**, by D and A respectively.

The one cost is the §0 ordering discipline, which C must implement precisely.

---

## 5. (reserved)

*The per-method comparison matrices from the research draft are superseded by §3's channel
model and have been removed.*

---

## 6. Rename resolution

A rename is the highest-stakes case: the *correct* op and the *fallback* diverge sharply. (Iceberg
terms below; the Delta sink has no rename op in delta-rs 1.0 and always takes the degraded path —
§7.1.2.)

- **True rename** → Iceberg `rename_column`: metadata-only, **preserves the field ID and all
  historical data** under the new name.
- **Misread as drop+add** → `drop_column` + `add_column`: old history projected out, new
  column NULL for every pre-change row. **Silent lineage loss.**

Under the augment model (channel C) rename is **positively provable in the normal path**:

| Engine | Resolution mechanism | Provable? |
|---|---|---|
| PostgreSQL | catalog augment via `pg_attribute.attnum` — same attnum, new name = rename | **Yes** |
| MySQL | channel-B DDL keyword (`RENAME COLUMN` / `CHANGE COLUMN old new …`) | **Yes** |
| MySQL, DDL text unparseable | catalog diff only — no stable identity | No → degrade |

**Policy:** emit `rename_column` whenever C/B prove it. Only when proof is unavailable
(MySQL + unparseable DDL) degrade to drop+add **with a logged warning** naming the table and
columns, so the lineage loss is visible rather than silent. Never infer a rename from
"same position, same type" heuristics.

---

## 7. Schema evolution (source-agnostic action, sink-mediated)

Evolution is applied through the sink's `evolve(delta)` (`Data-Flow.md` Phase 4, sink contract
item 5), which returns one of:

- **Applied** — the target schema now reflects the delta;
- **Degraded** — the delta was absorbed in a weaker form that keeps the mirror flowing and loses no
  data, with a **warning + metric** naming table and columns (e.g. Delta soft-drop, §7.1.2);
- **Unsupported** — the sink cannot perform it → **halt the table** exactly like an Unsafe delta.

A delta that is **Safe** in §7.2 is therefore *auto-evolved if the pipeline's sink can*; the per-sink
column of §7.2 is the contract.

### 7.1 Sink capability reference

#### 7.1.1 Iceberg

Iceberg tracks columns by **unique field ID** (not name/position), so name reuse is safe and
reordering is free. **Safe** operations: add column, drop column, rename column, reorder, and
**type promotion** limited to `int → long`, `float → double`, and `decimal(P,s) →
decimal(P',s)` with `P' > P`. Everything else is **breaking**: any narrowing, cross-family
type change, or tightening `optional → required`.

> **Implementation note.** Mutations go through the `iceberg` crate's table transaction
> (apache `iceberg-rust`, MSRV 1.92.0 per `Sources_Index_v3.md`). Add-column is the most
> mature path; **verify drop / rename / type-promotion support against the pinned crate
> version** and route any unsupported op into the "halt table" branch until the crate catches
> up.

#### 7.1.2 Delta Lake (delta-rs 1.0)

Delta tracks columns by **name** unless column mapping is enabled; the Delta sink creates tables
**without** column mapping (delta-rs 1.0 refuses ADD COLUMN on column-mapped tables — probe P7b,
config V-092). delta-rs 1.0 exposes **add column** and **drop NOT NULL**; it has **no** drop-column,
rename-column or type-widening operation. Hence:

- **Add column** → Applied (nullable).
- **Drop column (non-identity)** → **Degraded: soft-drop** — the column stays in the Delta schema and
  is written NULL from the change onward; history is intact. Physical drop is reserved until delta-rs
  exposes it.
- **Rename** → **Degraded**: add the new column + soft-drop the old one; history remains under the
  old name (the same visible outcome as §6's unprovable-rename fallback).
- **Type widening** → **Unsupported → halt** (the operator re-snapshots into a fresh table if needed).

### 7.2 DDL classification table (the G-19 contract)

Each normalized delta is **Safe** (auto-evolve, within the sink's capabilities), **Unsafe** (halt
the table per the `enabled`/halt contract, G-19b), or **Ignorable** (no target action). Applied at a
transaction boundary via §8.

| Source DDL | Class | Iceberg action | Delta action | Arrow / data handling |
|---|---|---|---|---|
| `ADD COLUMN` (any nullability) | **Safe** | `add_column` as **optional** (§7.3) | `add_columns`, nullable | new builder; historical rows read NULL |
| `DROP COLUMN` (non-identity) | **Safe** | `drop_column` (metadata-only) | **Degraded**: soft-drop (column kept, NULL onward; warn) | stop producing the column |
| `DROP COLUMN` part of PK / replica identity | **Unsafe** | — | — | halt table (breaks keying) |
| `RENAME COLUMN` — proven (§6) | **Safe** | `rename_column` (keeps field ID + data) | **Degraded**: add new + soft-drop old (warn; history under old name) | re-point builder to new name |
| `RENAME COLUMN` — not provable | **Safe (degraded)** | `drop_column` + `add_column` | **Degraded**: add new + soft-drop old (warn) | **warn**: history NULL under new name |
| Type **widen** within Iceberg set (§7.1.1) | **Safe** | `update_column` promotion | **Unsupported → halt** | builder adopts wider Arrow type |
| Type **widen** outside the set (e.g. `int→decimal`) | **Unsafe** | — | — | halt table |
| Type **narrow** (`bigint→int`, `varchar(100)→varchar(10)`, decimal shrink) | **Unsafe** | — | — | halt table |
| Cross-family change (`int→varchar`, `timestamp→date`) | **Unsafe** | — | — | halt table |
| `nullable → NOT NULL` | **Ignorable** | none | none | none: target columns are always nullable (§7.3) |
| `NOT NULL → nullable` | **Ignorable** | none | none | none |
| Change PK / `REPLICA IDENTITY` / unique key | **Unsafe** | — | — | halt table; operator re-snapshot |
| `ADD COLUMN` / `RENAME COLUMN` to a **reserved lineage name** (`_op`, `_ts`, `_lsn`, `_seq`, `_source_db`, `_pipeline`, `__data_avalanche_*`; case-insensitive) | **Unsafe** | — | — | halt table `RESERVED_COLUMN_COLLISION` (config §5) |
| `DROP TABLE` (tracked) | **Unsafe** | — (target kept) | — (target kept) | halt table `TABLE_DROPPED`; the target is never dropped. A re-created table of the same name (new OID) does **not** auto-resume; it needs an operator re-snapshot. Detection — PG: channel B within a stream session (the table's name on another OID, or its OID under another name; a table renamed away halts the same way), channel D / A (§2.1); MySQL: channel B |
| MySQL `ENUM`/`SET` **add** values (append-only) | Ignorable\*\* | none | none | decoded value set widens; verify mapping (G-18) |
| MySQL `ENUM`/`SET` reorder/remove values | **Unsafe** | — | — | halt (changes decoded string meaning) |
| Index/comment/storage-params/`AUTO_INCREMENT`/charset w/ same logical type | **Ignorable** | none | none | none |
| **Parse failure / unclassifiable DDL on a tracked table** | **Unsafe** | — | — | halt table (fail-safe) |

\*\* Only if the Arrow/Iceberg representation is a string, not an ordinal; pin in the
type-mapping doc (G-18).

### 7.3 Always add columns as nullable

A column added mid-stream has no value for any historical row already in the target. Iceberg can
only add a *required* column with both initial- and write-defaults (immature in the crate), and a
Delta column added to an existing table must be nullable, so add as **optional** in every sink — both
spec-safe and semantically honest. Enforce source `NOT NULL` downstream if needed; never push it into
the target schema. This also makes a source
nullability change effectively Ignorable for the target.

At table creation every non-identifier column is nullable, whatever the source declares; identifier
columns are NOT NULL.

---

## 8. Apply protocol (resolves OQ-DT2)

### 8.1 Steady-state: detect-at-boundary → flush → evolve → rebuild → resume

Schema changes apply **only at a transaction boundary**, never mid-transaction — the same
invariant used for flushing.

```
On a normalized delta D for table T at transaction boundary B:

1. SEAL    finalize T's current RecordBatch under the OLD schema.
2. FLUSH   commit it to the sink under the pre-change target schema (Phase 4);
           obtain flush_confirmation.
3. CLASSIFY D → Safe | Unsafe | Ignorable (§7.2).
              ├─ Ignorable → goto 6.
              └─ Unsafe (or evolution disabled) → halt table T only,
                 log error, emit metric; other tables keep running (G-19b).
4. EVOLVE  sink.evolve(D), idempotently (§8.3), scoped to D's columns only — never the
           whole "now" catalog schema (§0).
           ├─ Applied / Degraded (warn + metric) → continue.
           └─ Unsupported → halt table T only (as Unsafe).
5. REBUILD recreate T's Arrow ArrayBuilders from the NEW schema.
6. RESUME  continue accumulation under the new schema from B onward.
```

Because B is exact (channel B fires at the precise stream position), each data file stays
homogeneous — written under exactly one schema — which is what keeps evolution a metadata operation
rather than a file rewrite (in both Iceberg and Delta).

### 8.2 Startup & non-replayable gaps

At pipeline start (channel A): create absent tables from the queried baseline; for an
existing replayable table, defer to in-order replay (B+C) and treat the startup query as a
drift check only (§0, §3-A). If the resume point is **non-replayable** (PG slot invalidated /
WAL recycled; MySQL binlog purged), the intervening DDL and rows are unrecoverable from the
stream: baseline the target to the queried schema and run a **re-snapshot** of T (Phase 1),
resuming streaming from the new snapshot point. This is also the recovery when channel D
detects a change on a table whose stream position cannot be reconciled.

### 8.3 Idempotency & checkpoint ordering

DDL carries an LSN/GTID and is **replayed on restart**. On replay, B re-fires and C
re-queries (returning a possibly further-evolved schema — the §0 hazard applies on replay
too), so:

1. **Idempotent apply** — before mutating, re-read the live target schema and skip the op if
   the target already reflects it (mirrors the Phase 4 position guard). A soft-dropped Delta column
   counts as "already reflected" for a replayed drop. Scoping to D's
   columns (§0) prevents replay from importing later changes early.
2. **Checkpoint ordering** — advance the confirmed offset past the DDL **only after** the
   sink's schema change commits. A crash between EVOLVE and the offset write replays the
   DDL, and the idempotent guard absorbs it.

---

## 9. Source preconditions

| Engine | Setting | Requirement | Rationale |
|---|---|---|---|
| Both | **The replication user holds SELECT on `information_schema` / catalogs**, used over a regular (non-replication) connection | **Required** (NEW) | Channels A/C/D query the source catalog on every pipeline, including MySQL. Single credential set — no separate read role. Validate at start. |
| PostgreSQL | `REPLICA IDENTITY` ≠ `NOTHING` | required (already enforced) | delete keying |
| MySQL | `binlog_format = ROW` | required | granular change capture |
| MySQL | `binlog_row_image = FULL` | required | complete before-images / equality-delete keys |
| MySQL | `binlog_row_metadata = FULL` | **recommended** (warn if absent) | names/PK/ENUM strings in `TABLE_MAP`; absent → names come from channel C catalog query |
| MySQL | `REPLICATION CLIENT` / `REPLICATION SLAVE` | required | binlog dump |

Validate these at pipeline start and surface a clear error (or warning for the
recommended-only ones), per `Configuration_Specification.md` §13.

---

## 10. Configuration impact

The `catalog.schema_evolution.method` field is **removed** — detection is the fixed
four-channel model (§3), not an operator choice. The hybrid is strictly better than either
former mode, and `method` leaked engine internals (operators should not need to know pgoutput
carries no DDL). The escape hatch stays **internal** (query failure → halt table fail-safe),
not an operator toggle.

```yaml
catalog:
  schema_evolution:
    enabled: true            # auto-apply Safe; halt table on Unsafe (G-19b)
    rescan_interval: 60m     # channel D (idle tables) only; 0 = disabled
```

Ripple into `Configuration_Specification.md` (deliberate spec edit, touches G-1f):

- Remove `method`; delete validation rules **V-031** (events-on-PG), **V-024**
  (method enum), **V-032** (interval-with-events warning).
- Rename `interval` → `rescan_interval`; it now **always applies** (idle rescan), default
  `60m`, `0` disables. (Was "valid only when `method = query`.")
- Add the source-query-access precondition (§9) to the validation table.
- §9.5.1 reduces to two fields: `enabled`, `rescan_interval`. *(Now §9.7, pipeline-level `schema_evolution` — multi-sink, 2026-09-26.)*

---

## 11. Open decision points for spec development

| ID | Decision | Recommendation / status |
|---|---|---|
| G-19 | Ratify the §7.2 classification table | as tabled |
| G-19a | MySQL `QUERY_EVENT` handling scope | **reduced** to DDL-classify + table-name extract (+ rename-keyword); full-delta parsing not required |
| DT-R2 | Rename when intent unprovable (MySQL + unparseable DDL) | drop+add + warning |
| DT-R5 | ENUM/SET representation (string vs ordinal) | string (makes value-add Ignorable) — coordinate with G-18 |
| DT-R6 | `binlog_row_metadata = FULL` — required or recommended? | recommended; warn + catalog-name fallback |
| DT-R7 | `rescan_interval` default and disable semantics | `60m`; `0` disables (NEW) |
| DT-R8 | Source query connection — reuse replication creds or separate role? | **Resolved** — reuse replication credentials; the replication user must hold SELECT (validated at start, §9) |

Resolved by the approved model (closed): **DT-R1** (PG detection framing — removed with
`method`); **DT-R3** (poll-window recovery — now the non-replayable-gap → re-snapshot path,
§8.2); **DT-R4** (PG event-trigger high-fidelity mode — unnecessary, since channel C delivers
completeness without source instrumentation).

---

## 12. Downstream document actions

These edits belong to other documents, maintained in their own chats. They are specified
here explicitly so they can be applied directly; the rationale is §0–§11 above. This
document is the source of truth for schema detection/evolution — the config and gap docs
should reference it rather than restate it.

### 12.1 `Configuration_Specification.md`

Replace §9.5.1 `catalog.schema_evolution` with two fields *(now §9.7, pipeline-level `schema_evolution` — multi-sink, 2026-09-26)*:

| Field | Type | Required | Default | Mutability | Notes |
|---|---|---|---|---|---|
| `enabled` | bool | no | `true` | R | When disabled, or on an unsafe change, the changed table halts (log error); other tables continue (G-19b). |
| `rescan_interval` | Duration | no | `60m` | R | Idle-table rescan cadence (detection channel D only). `0` disables the idle rescan. Always applicable. |

- **Remove** the `method` field entirely.
- **Rename** `interval` → `rescan_interval`; default `60s` → `60m`; drop the
  "valid only when `method = query`" constraint (it always applies now).
- **Delete** validation rules **V-024** (method enum), **V-031** (events-on-PG),
  **V-032** (interval-with-events). Repoint any other rule referencing
  `schema_evolution.interval` to `rescan_interval`.
- **Add** a precondition rule (assign next free V-ID): *the configured source credentials
  must grant SELECT on `information_schema` / system catalogs* — error at start if absent.
  Required for every pipeline including MySQL (channels A/C/D); the replication user is
  reused, no separate role.
- **Update example configs** from `schema_evolution: { … method: …, interval: … }` to
  `schema_evolution: { enabled: true, rescan_interval: 60m }`, removing the per-engine
  `method: query|events` lines.
- §16 deferred-fields list: the "DDL classification → G-19" entry is satisfied by §7.2 here;
  G-19a's reserved scope narrows (see 12.2).

### 12.2 `CDC_Gap_Analysis_Report.md`

| Gap / item | Action |
|---|---|
| **G-19** (DDL safe/unsafe classification) | ✅ Resolve by adopting the §7.2 classification table as the contract. |
| **G-19a** (MySQL DDL handling) | 🔽 Reduce scope — from full ALTER-delta parser to a DDL **classifier + table-name extractor** (+ optional `RENAME`/`CHANGE` keyword for rename intent); the full delta now comes from catalog augmentation (§3 channel C). Still required, materially de-risked. |
| **G-19b** (evolution disabled / unsafe behavior) | ✅ Reaffirm — halt the changed table only, continue others (§8.1). |
| **OQ-DT2** (DDL during live replication) | ✅ Resolve — apply protocol §8 (detect-at-boundary → flush → evolve → rebuild → resume; startup & non-replayable-gap §8.2). |
| **OQ-DT3** (supported evolution operations) | ✅ Resolve — Iceberg safe-set + classification (§7.1–§7.2). |
| Source-SELECT precondition | 🆕 New item — ties to the config rule in 12.1; replication user must hold SELECT. |
| **DT-R1** (PG detection framing) | ✅ Close — `method` removed; four-channel model (§3). |
| **DT-R3** (poll-window recovery) | ✅ Close — non-replayable-gap → re-snapshot (§8.2). |
| **DT-R4** (PG event-trigger high-fidelity mode) | ✅ Close — unnecessary; channel C gives completeness without source instrumentation. |
| **DT-R8** (source query credentials) | ✅ Resolve — reuse replication creds; SELECT required. |
| **DT-R2** / **DT-R5** / **DT-R6** / **DT-R7** | 🟡 Keep open — recommendations in §11 (DT-R5 coordinates with G-18). |

---

## Related pages

- [[Architecture/System-Overview]] — OQ-DT2 / OQ-DT3, minimal source-footprint principle
- [[Architecture/Data-Flow]] — Phase 2 decode (`R` / `TABLE_MAP`), Phase 3 force-flush, Phase 4 schema reconciliation
- [[Configuration_Specification]] — §9.7 `schema_evolution` (`enabled` + `rescan_interval`), validation rules
- [[CDC/Type_Mapping]] — G-18 three-hop type mapping (type promotion + ENUM/SET representation)
- [[CDC_Gap_Analysis_Report]] — G-19 / G-19a / G-19b tracking

## Sources
