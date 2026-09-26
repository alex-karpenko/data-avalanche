---
updated: 2026-09-26
domain: Architecture
tags: [sink, multi-sink, delta-lake, delta-rs, iceberg, merge, upsert, capabilities, configuration, arrow]
resolves: [DA-SINK-1, DA-X-5, DA-X-3]
---

# Multi-Sink Architecture and Delta Lake as the Second Sink

> **Status: DECIDED 2026-09-26.** All decisions in the register (§12) are signed off: D-MS.1–D-MS.14
> (D-MS.4 with an explicit correction), plus D-MS.15 and revision R-1 surfaced by the capability probe. Spec edits it implies (§8, §9) are
> **propose-only** per project rules. Delta design details remain subject to the capability probe (§10).
>
> `resolves:` lists gaps this doc *advances*: DA-SINK-1 stops being a **system** blocker (it narrows to
> "Iceberg sink, mirror mode"), DA-X-5 (TRUNCATE) gets a per-sink realization, DA-X-3 (scattered
> sink-writer contract) is replaced by a sink-neutral contract + per-sink realizations.

---

## 1. Scope & motivation

Two goals, one change:

1. **Unblock.** The sink is the only 🔴 left in the design (`CDC_Design_Gap_Analysis_v1.md` DA-SINK-1):
   iceberg-rust's public `Transaction` API is append-only on every published version (0.9.1 → 0.10.1)
   and the v0.11.0 RCs. Rechecked **2026-09-26**: upstream PRs #3046 (MergingSnapshotProducer /
   RewriteFilesAction), #2203 (RowDelta), #2185 (OverwriteAction), #2752 (COW rewrite primitive) are
   **all still open/unmerged**. There is no date for a delete-capable Iceberg writer in Rust.
2. **Make the sink pluggable by nature.** A pipeline selects its target table format (`sink.type`),
   so the system is not hostage to one crate's roadmap and can serve users who standardize on a
   different lakehouse format. The first additional sink is **Delta Lake via delta-rs**.

Out of scope for this doc: writing the sink code; per-pipeline **fan-out** to several sinks at once
(discussed as a decision, §12 D-MS.2, recommended deferred).

---

## 2. Key findings (delta-rs, verified against source)

All rows marked **✅src** were verified by reading the published **`deltalake-core` 1.0.0** crate source
(crates.io, 2026-09-21). Rows marked **🧪** are plausible but need the capability probe (§10).

### 2.1 Release & dependency facts

| Fact | Value | Evidence |
|---|---|---|
| Latest `deltalake` | **1.0.0**, published 2026-09-21 (previous 0.32.4, 2026-06-07). First 1.x = API-stability signal. | crates.io ✅ |
| MSRV | 1.94.1 | crates.io ✅ |
| Arrow / Parquet | **arrow 59**, parquet 59 | `deltalake-core` deps ✅ |
| Object storage | `object_store` 0.13.2 (optional `deltalake-opendal` backend feature exists) | deps / features ✅ |
| Kernel | `buoyant_kernel` 0.28 — a *distribution* of `delta-kernel-rs` (same upstream repo) | crates.io ✅ |
| DML engine | `merge` / `delete` / `update` / `write` require the **`datafusion` feature** (DataFusion 55) | `operations/mod.rs` `cfg(feature="datafusion")` ✅src |
| iceberg 0.10.1 arrow | **arrow 58** | crates.io ✅ |
| iceberg-rust `main` (→ 0.11) arrow | **arrow 59.2**, opendal 0.58, MSRV 1.95 | repo `Cargo.toml` ✅ |

**Consequence:** today, iceberg 0.10.1 (arrow 58) and deltalake 1.0 (arrow 59) cannot share one
`RecordBatch` type in one binary. iceberg 0.11 lands on arrow 59 → alignment is imminent. See §6.6.

### 2.2 Capability matrix for the CDC write path

| Need (from our design) | delta-rs 1.0 | Status |
|---|---|---|
| Commit **deletes + upserts atomically** in one table version | `MergeBuilder`: `when_matched_update` / `when_matched_delete` / `when_not_matched_insert` in **one** commit | ✅src |
| Attach commit-time **custom metadata** | `CommitProperties::with_metadata` → `commitInfo` | ✅src |
| **Application transactions** (idempotent streaming marker) | `CommitProperties::with_application_transaction(Transaction{app_id, version})` → Delta `txn` action | ✅src |
| Concurrent commits touching the **same `app_id` conflict** | conflict checker tracks `read_app_ids` from `Action::Txn` | ✅src (semantics 🧪) |
| Low-level commit of arbitrary actions (escape hatch) | `CommitBuilder::with_actions(Vec<Action>)` public | ✅src |
| **Truncate** (delete all rows) | `DeleteBuilder` without predicate → `Remove` for all files (metadata-only) | ✅src |
| Atomic commit on **S3** without an external lock | conditional put (`If-None-Match`) is the default; **DynamoDB log store removed in 1.0** (discussion #4482) | ✅src |
| Atomic commit on **GCS / Azure** | object_store put-if-absent (native) | ✅ (🧪 real-service run) |
| Add column | `add_columns()` op; `with_merge_schema(true)` on MERGE; `SchemaMode::Merge` on write | ✅ probe P7a — **but refused on column-mapping tables** (P7b) |
| Relax NOT NULL | `drop_column_not_null()` | ✅src |
| **Drop / rename column** | **No operation.** Column-mapping writer feature is supported, but no drop/rename op is exposed | ❌ (✅src: absent) |
| **Type widening** (int→long, etc.) | `TypeWidening` **not** in the writer feature set | ❌ ✅src |
| Deletion vectors | Readable; tables with DVs are writable; **MERGE/UPDATE do not write DVs** (they rewrite files) | ✅src (no DV refs in merge/update) |
| Compaction / Z-order | `optimize()` | ✅src |
| Checkpoints / log cleanup | post-commit hook (`create_checkpoint`, `cleanup_expired_logs`) | ✅src |
| Vacuum (delete unreferenced files) | `vacuum()` | ✅src |
| Catalog **registration** | **None.** `deltalake-catalog-glue` / `unity` implement only `get_table_storage_location` (resolve, not create/register) | ✅src |
| Iceberg interop (UniForm) | Not written by delta-rs (no IcebergCompat writer feature) | ✅src (absent) |

### 2.3 The one semantic difference that matters

**Delta Lake has no equality deletes.** A Delta delete is always *positional*: either the containing
data file is rewritten (copy-on-write) or a deletion vector (a bitmap of row positions) is attached.
Both require **finding the rows first** — i.e., a read of every data file whose statistics may contain
an affected key. delta-rs's MERGE does exactly that (a DataFusion join of the flush batch against the
candidate files), then rewrites the touched files.

This inverts Iceberg-MOR's cost profile:

| | Iceberg MOR (equality deletes) — *designed, not buildable today* | Delta MERGE (delta-rs 1.0) — *buildable today* |
|---|---|---|
| Write cost per flush | O(batch) — no read of existing data | O(files touched) — read + rewrite each candidate file |
| Read cost | grows with delete-file stack until compaction | clean (no delete reconciliation; files are rewritten) |
| Maintenance urgency | high (delete-file compaction is MOR-critical) | low for correctness; OPTIMIZE for small files |
| Ghost-row hazard (strictly-less-than seq rule, `Ghost_Row…` §0) | real, must be engineered around | **does not exist** — a MERGE commit is a snapshot replacement of touched files |
| Cost sensitivity to key distribution | none | **high**: sequential/time-correlated keys touch few files; random keys (UUID PKs, hot-spread updates) touch many |

The last row is the main Delta risk and is measurable (§10, probe P6).

---

## 3. Delta vs Iceberg as a CDC mirror target — full comparison

| Dimension | Iceberg (iceberg-rust) | Delta (delta-rs 1.0) | Implication for us |
|---|---|---|---|
| Upsert/delete commit | ❌ append-only API (DA-SINK-1) | ✅ MERGE, DELETE, UPDATE | Delta ships the mirror today. |
| Idempotent replay | PK-upsert + equality-delete keying | PK-upsert via MERGE (same semantics) | `Delivery…` §1 "idempotency is correctness" holds unchanged. |
| Per-table atomicity only | yes | yes | DA-DEDUP-7 unchanged. |
| Optimistic commit primitive | catalog metadata-pointer CAS | put-if-absent of `_delta_log/N.json` | Both are a CAS backstop for the lease (`Clustering…` §5.5 layer 2). |
| Sink-side epoch fence carrier | snapshot summary (per-snapshot; **not inherited**) | `txn` action (**persisted in table state**, survives foreign commits) | Delta carrier is *stronger*. See §7.2 and new gap DA-X-10. |
| Position carrier | snapshot summary `cdc.committed_position` | `commitInfo` metadata (per-commit) or `txn.version` (i64, PG only) | Both need "walk back to last own commit". |
| Schema: add column | ✅ | ✅ | same |
| Schema: drop column | ✅ metadata-only (field IDs) | ❌ no op in delta-rs | Delta: **soft-drop** (keep column, write NULL) — §7.3 |
| Schema: rename | ✅ keeps field ID | ❌ no op | Delta: degraded path (§7.3) |
| Schema: type promotion | ✅ int→long, float→double, decimal widen | ❌ | Delta: halt (existing fail-safe) |
| Types: `uuid` | ✅ (`fixed(16)` annotated) | ❌ no type | Delta: `string` (canonical) |
| Types: `time` | ✅ | ❌ no type | Delta: `string` or `long` µs (D-MS.8) |
| Types: tz vs ntz timestamp | `timestamptz` / `timestamp` | `timestamp` (**= with tz**) / `timestamp_ntz` | ⚠️ same word, opposite meaning — config type grammar must be sink-neutral (D-MS.7) |
| Types: JSON | string | string (Variant available, preview) | same default |
| Partitioning | transforms (identity/bucket/truncate/day…); spec evolution | **identity only** (generated columns can emulate); **no evolution** | Neutral partition spec + per-sink validation |
| Catalog | required (Glue/HMS/REST) | **optional** (path-based); delta-rs cannot register | Catalog becomes a sink-specific block |
| Maintenance in Rust | `expire_snapshots` only | optimize, vacuum, checkpoint, log cleanup | Delta can self-maintain metadata (checkpoints) from day 1 |
| Readers | Spark, Trino, Flink, Athena, Snowflake, DuckDB, … | Spark/Databricks, Trino, Athena, Snowflake, DuckDB, Polars, … | both broad |
| Storage stack | OpenDAL | object_store (OpenDAL optional) | two stacks; one credential model must feed both (§6.7) |
| Binary weight | moderate | **heavy** (DataFusion) | Cargo feature per sink (§6.6) |

---

## 4. Pros & cons

### 4.1 Adding Delta Lake as a sink

**Pros**
- **Unblocks the mirror today** on a 1.0 crate: MERGE commits deletes + upserts atomically, with custom
  commit metadata and application-transaction markers in the same commit.
- **Eliminates the ghost-row class** (`Ghost_Row…` §0) for this sink: no sequence-number reasoning.
- **Truncate is trivial** (metadata-only remove-all) — DA-X-5 solved for Delta.
- **Stronger fence carrier** (`txn` survives foreign commits, participates in conflict detection).
- **S3 commits need no DynamoDB** since 1.0 — only the same conditional-PUT capability our lease
  already requires (and has proven on S3, `prototypes/lease-conformance/`).
- **Self-maintained metadata**: checkpoints and log cleanup run as post-commit hooks.
- Broad reader ecosystem; path-based tables work without any catalog.

**Cons / risks**
- **Write amplification under random-key churn** (read+rewrite of touched files per flush). Mitigations:
  larger/less frequent flushes, clustering, OPTIMIZE; hard numbers needed (probe P6).
- **DataFusion dependency**: compile time, binary size, a query engine in the hot path (memory to bound).
- **Weaker schema evolution in delta-rs**: no drop/rename ops, no type widening → more degraded/halt paths.
- **Identity-only partitioning**, no partition evolution.
- **No catalog registration** in delta-rs: Glue/HMS/Unity registration would be our code (or deferred).
- **Type gaps**: no `uuid`, `time`, unsigned — lossless mappings exist but change the downstream schema.
- **Crate coupling**: a second fast-moving crate pinned to arrow/parquet majors; delta-rs depends on a
  third-party kernel distribution (`buoyant_kernel`).

### 4.2 Going multi-sink at all

**Pros:** no single-crate hostage (DA-SINK-1 becomes a per-sink capability, not a system blocker);
forces a clean sink-neutral contract (DA-X-3 fixed by construction); opens later sinks (Iceberg
changelog, Paimon, Lance, Kafka) without touching capture/decode/accumulate.

**Cons:** every sink-touching design (types, schema evolution, partitioning, fencing, maintenance,
observability, config) now has a per-sink column; the test matrix multiplies; arrow-version alignment
becomes a release-engineering constraint; two storage clients.

**Net:** the abstraction cost is mostly *documentation and validation*, because the core pipeline was
already sink-agnostic up to the accumulator. The sink-specific surface is well bounded (§6).

---

## 5. Alternatives considered

| Alternative | Verdict |
|---|---|
| **Keep waiting for iceberg-rust RowDelta** (current decision since 2026-06) | Keeps the system blocked with no date; 3 months elapsed, foundation PR #3046 still open. Remains the path for the *Iceberg mirror* — no longer the path for the system. |
| **Implement the Iceberg delete commit ourselves** (low-level manifest assembly, or upstream contribution) | Valid in parallel (§12 D-MS.10); high effort, spec-heavy (sequence numbers, manifest lists, conflict detection). Multi-sink does not preclude it. |
| **Iceberg changelog (append-only) mode** | Buildable **today** on 0.10.1 (`fast_append` + `cdc.*` summary — G2 probe passes). Different product (consumers dedupe) — offered as D-MS.5. |
| **JVM bridge** for Iceberg commits | Rejected (contradicts Rust-native tenet; operational weight). |
| **Apache Hudi** (`hudi-rs`) | Read-focused Rust impl; no mature writer. Not viable now. |
| **Apache Paimon** | Designed for CDC (primary-key tables, LSM), but no mature Rust writer. Watch. |
| **Lance** | Rust-native, has merge-insert/delete; weak lakehouse-engine interop. Niche. |
| **Plain Parquet changelog** | No ACID table; pushes all correctness to readers. Rejected as a mirror. |

---

## 6. Proposed architecture

### 6.1 Layering

```
 Source decode ──► Accumulator ──► Collapse ──► TableChangeSet ──► Sink (trait) ──► table format
 (PG / MySQL)      (per-table       (keep-last    (sink-neutral     ├─ IcebergSink  (iceberg-rust)
                   Arrow builders)  by _seq,      Arrow batches +   ├─ DeltaSink    (delta-rs)
                                    ≤1 del + ≤1   positions +       └─ …future
                                    data per key) epoch)
                    ▲                                                      │
                    └─────────── flush confirmation (per table) ◄──────────┘
```

Changes vs today's `Data-Flow.md`:

1. **Collapse moves out of the sink into the core.** Today it is step 2 of the Iceberg sink-writer
   contract, but it is format-independent (keep-last by `_seq`, mirror-delete semantics). Every sink
   receives an already-collapsed `TableChangeSet`.
2. **Phase 4 becomes "Sink commit"**; the Iceberg-specific steps (equality-delete sequencing, partition
   routing of deletes, snapshot summary) become the *Iceberg realization* of a neutral contract.
3. **Hop 3 of type mapping becomes per-sink** (Arrow → sink physical type). Hops 1–2 are unchanged.

### 6.2 The sink-neutral change set

```rust
struct TableChangeSet {
    table:        TableRef,
    schema:       SchemaRef,          // canonical Arrow schema + logical-type field metadata (§6.5)
    identifier:   Vec<FieldName>,     // PK or tables.primary_key override
    truncate:     bool,               // a TRUNCATE barrier precedes this set (see §6.4)
    upserts:      Vec<RecordBatch>,   // one row per key: final image (I/U/r), incl. lineage cols
    deletes:      Vec<RecordBatch>,   // one row per key: identifier cols (+ before-image partition cols)
    position:     PositionRange,      // per-table source position covered (for the guard / HWM)
    split:        Option<SplitInfo>,  // Large_Transaction split-commit marker (checkpoint pinning)
}
```

Invariant from `Ghost_Row…` §4 moves here: **a key appears in at most one of `upserts` / `deletes`**,
except a PK-changing UPDATE, which is two keys (delete(old) + upsert(new)).

### 6.3 The `Sink` trait (shape, not final code)

```rust
trait Sink: Send + Sync {
    fn kind(&self) -> SinkKind;                               // Iceberg | Delta | …
    fn capabilities(&self) -> &SinkCapabilities;             // static per build + backend (§6.4)

    async fn table_state(&self, t: &TableRef) -> Result<SinkTableState>;
        // { exists_data, exists_catalog, schema, marker: Option<CommitMarker> }
        // feeds Phase-0 snapshot-mode decisions (initial / dump_initial) and the position guard

    async fn ensure_table(&self, t: &TableRef, spec: &TableSpec) -> Result<()>;
        // create-if-absent: schema, identifier, partitioning, format properties, registration

    async fn evolve(&self, t: &TableRef, d: &SchemaDelta) -> Result<EvolveOutcome>;
        // Applied | Degraded{warning} | Unsupported  (Unsupported ⇒ halt table, existing contract)

    async fn commit(&self, cs: TableChangeSet, m: CommitMarker) -> Result<CommitOutcome>;
        // atomic per table; idempotent under replay; MUST reject if the table's
        // latest marker epoch > m.owner_epoch (fence); re-asserts marker on success.
        // CommitOutcome = Committed{version} | SkippedCovered | Fenced
}

struct CommitMarker { owner_epoch: u64, committed_position: SourcePositionString, pipeline_id: String }
```

Everything above the trait (lease, reconciler, accumulator, snapshot loop, schema detection, position
logic, back-pressure) is untouched by the sink choice.

### 6.4 Capabilities and fail-fast

```rust
struct SinkCapabilities {
    write_modes:       Set<WriteMode>,        // Mirror, Changelog
    schema_ops:        Set<SchemaOp>,         // AddColumn, DropColumn, RenameColumn, Widen(set), RelaxNullability
    partition_xforms:  Set<Transform>,        // Identity, Bucket, Truncate, Year/Month/Day/Hour
    types:             Set<LogicalType>,      // which logical types map natively
    marker_carrier:    MarkerCarrier,         // how epoch/position are persisted atomically
    registration:      Set<CatalogKind>,      // catalogs this sink can register tables in
    self_maintenance:  Set<MaintenanceOp>,    // Checkpoint, LogCleanup, Compact, Expire, …
}
```

- **Config validation (Phase 2)** checks config against the *declared* capabilities of the compiled-in
  sink (unsupported transform / override type / write mode → error).
- **Startup assertion** (generalizes §24.7 of the gap report): each sink probes the runtime (crate
  version features, storage conditional-put support) and **refuses to start a pipeline** whose config
  needs a capability it lacks. Example: `sink.type: iceberg, write_mode: mirror` on iceberg-rust 0.10.1
  → pipeline halts at start with "Iceberg mirror requires a delete-capable commit action (DA-SINK-1)".
  This turns DA-SINK-1 from a design blocker into a **clearly-reported runtime capability**.
- **Evolution routing**: `evolve()` returning `Unsupported` goes to the existing halt-table branch
  (`Schema_Change…` §7.1 implementation note) — the fallback design already exists.

**TRUNCATE barrier.** TRUNCATE is sequenced like DDL (`Schema_Change…` §8 seal→flush→apply): the
accumulator seals and flushes the pre-truncate batch, then emits a change set with `truncate = true`
followed by the post-truncate rows. Iceberg: overwrite-to-empty (blocked with the mirror). Delta:
delete-all (metadata-only). Idempotent under replay in both (deleting an empty table is a no-op).

### 6.5 Logical types (sink-neutral Hop 2½)

The accumulator's Arrow schema carries a **logical type tag** in field metadata where Arrow alone is
ambiguous: `avalanche.logical = uuid | time | json | uint64 | enum | set | timestamp_ntz`. Each sink's
Hop-3 maps logical → physical:

| Logical (from `Type_Mapping.md`) | Arrow (accumulator) | Iceberg | Delta |
|---|---|---|---|
| int16/int32/int64 | Int16/Int32/Int64 | int/int/long | short/integer/long |
| uint64 (MySQL BIGINT UNSIGNED) | Decimal128(20,0) | decimal(20,0) | decimal(20,0) |
| decimal(P≤38,S) | Decimal128 | decimal | decimal |
| decimal P>38 | Utf8 (`numeric_overflow_action: string`) | string | string |
| timestamp **with** tz | Timestamp(µs, "UTC") | timestamptz | **timestamp** |
| timestamp **without** tz | Timestamp(µs, None) | **timestamp** | timestamp_ntz |
| time | Time64(µs) | time | **string** (`HH:MM:SS.ffffff`) or long µs — D-MS.8 |
| uuid | FixedSizeBinary(16) | uuid | **string** (canonical 36-char) |
| json | Utf8 | string | string (Variant later) |
| enum / set | Utf8 / List<Utf8> | string / list<string> | string / array<string> |
| array<T> | List<T> | list<T> | array<T> |
| bytes | Binary | binary | binary |

A sink converts arrays where the physical type differs (uuid → string, time → string) inside its own
Hop 3 — the accumulator never knows the target format.

### 6.6 Crates, features, and the Arrow-version rule

- Workspace crates: `avalanche-core` (types, change set, `Sink` trait), `avalanche-sink-iceberg`,
  `avalanche-sink-delta`, plus the existing source/cluster crates.
- **Cargo features** `sink-iceberg`, `sink-delta` (both default-on). A build without a sink rejects a
  config that selects it (validation V-083). Lets slim images drop DataFusion.
- **Single-Arrow rule:** the workspace pins **one** arrow/parquet major; every sink crate must compile
  against it. Today that means arrow 59 (deltalake 1.0; iceberg-rust `main`/0.11). **Escape hatch** if a
  sink lags a major: bridge through the Arrow **C Data Interface** (`arrow::ffi`, zero-copy, ABI-stable
  across majors) inside that sink crate — never in the core.
- Practical consequence for sequencing: the Iceberg sink moves to iceberg 0.11 (arrow 59) when it
  ships; until then the capability probe keeps 0.10.1 in its own crate (it already is isolated).

### 6.7 Storage and credentials

One pipeline-level storage description (`sink.bucket/prefix` + `globals.cloud` credential chain) is
translated by each sink into its client config: OpenDAL operator (Iceberg), `object_store` options
(Delta). delta-rs's optional OpenDAL backend is a later convergence option, not a v1 requirement.
The **lease** keeps its own per-backend `LeaseStore` (unchanged).

Table location layout (config §10) becomes sink-specific *below* the table root:

```
<bucket>/<prefix>/<schema>/<table>/          ← table root (common)
    Iceberg:  data/ , metadata/
    Delta:    _delta_log/ , part-*.parquet (partition dirs if partitioned)
<bucket>/<prefix>/.pipeline-state/           ← pipeline state (common, unchanged)
```

Startup check: a table root that already holds the **other** format's metadata is a hard error
(no silent mixing).

---

## 7. Per-sink realizations of the contract

### 7.1 Iceberg sink

Unchanged design (`Ghost_Row…`, `Iceberg_Partitioning…`, `Delivery…`), now labelled as the Iceberg
realization: equality-delete keyed on `identifier-field-ids`, delete-file sequencing, before-image
partition routing, `cdc.*` snapshot summary. **Mirror mode is capability-gated on DA-SINK-1.** If
D-MS.5 is accepted, **changelog mode** ships on the current crate (append `I/U/D/r` rows with `_op`,
`_seq`; no deletes; `cdc.*` summary proven by the G2 probe).

### 7.2 Delta sink (mirror mode)

**Per flush, per table — one Delta commit:**

1. If `truncate`: `DeleteBuilder` (no predicate) commit first — metadata-only — then continue.
2. Build the MERGE source = `upserts` ∪ `deletes` with a boolean `__avalanche_del` column
   (deletes carry only identifier + before-image partition columns; other columns NULL).
3. `MergeBuilder` with predicate `t.<id cols> = s.<id cols>` (+ partition-pruning disjunct
   `t.<part> IN (s.<old part>, s.<new part>)` when partitioned):
   - `when_matched_delete(s.__avalanche_del)`
   - `when_matched_update(NOT s.__avalanche_del)` → all columns from `s`
   - `when_not_matched_insert(NOT s.__avalanche_del)`
4. `CommitProperties`:
   - `with_application_transaction(txn{ app_id: "avalanche/<pipeline_id>/owner", version: owner_epoch })`
   - `with_metadata({ "avalanche.committed_position": "<engine-tagged>", "avalanche.pipeline": id,
     "avalanche.owner_epoch": n })`
5. **Fence (before step 3):** read the snapshot's app-transaction version for
   `avalanche/<pipeline_id>/owner`; if `> owner_epoch` → `Fenced` (abort, stop pipeline per §19 of
   clustering). A concurrent commit carrying the same `app_id` makes the loser's commit conflict and
   re-check on retry (🧪 P4).
6. **Position guard:** read `avalanche.committed_position` from the latest **own** `commitInfo`
   (walk back past foreign commits such as an external OPTIMIZE); if it covers `position` → `SkippedCovered`.
   Guard is an optimization; correctness is the idempotent MERGE.

**Properties that fall out:**
- Same-commit delete+insert of one key is impossible after collapse (one row per key), and MERGE has no
  sequence-number semantics → **no ghost rows by construction**.
- PK-changing UPDATE = delete(old key) row + upsert(new key) row → both handled by the one MERGE.
- Partition-changing UPDATE: MERGE matches by key across partitions; before-image partition values are
  used only for pruning, not routing.
- Snapshot chunks: upserts like any other change set. Keyset chunks are key-ordered, so file stats
  prune well — chunk MERGEs mostly touch zero or few existing files (🧪 P6).
- Split commits (`Large_Transaction…`): unchanged — each slice is an idempotent MERGE; the checkpoint
  pin lives in pipeline state, not in the sink.

**Table creation defaults (proposed; revised 2026-09-26 by probe P7b):** writer features
`appendOnly=false`, column mapping **off** (`none`) — *originally proposed `name` to future-proof
drop/rename, but delta-rs 1.0 **refuses ADD COLUMN on column-mapped tables** (P7b), which would disable
the one evolution op the Delta sink supports*,
`delta.checkpointInterval` default (10), `delta.logRetentionDuration` default. Deletion vectors **off**
(delta-rs MERGE does not produce them; keeps readers simple).

### 7.3 Delta schema-evolution mapping (vs `Schema_Change…` §7.2)

| Source DDL (class today) | Iceberg action | Delta action (proposed) |
|---|---|---|
| ADD COLUMN (Safe) | add_column optional | `add_columns` nullable ✅ |
| DROP COLUMN non-identity (Safe) | drop_column | **Soft-drop** (Degraded): keep column, write NULL from here; warn + metric. Physical drop reserved for when delta-rs exposes it. |
| RENAME proven (Safe) | rename_column (field ID kept) | **Degraded**: add new column; soft-drop old; warn (history under old name) |
| RENAME unprovable (Safe-degraded) | drop + add | add new + soft-drop old; warn |
| Type widen in Iceberg set (Safe) | update_column | **Unsupported → halt** (no type widening in delta-rs) |
| nullable → NOT NULL (Unsafe) | halt | halt |
| NOT NULL → nullable | (n/a today: Ignorable) | `drop_column_not_null` if the Delta column was NOT NULL (we create all non-identifier columns nullable, so usually no-op) |
| PK / replica-identity change (Unsafe) | halt | halt |
| Everything else Unsafe/Ignorable | as today | as today |

Soft-drop is a new outcome class (**Degraded**) the `evolve()` contract already allows. Whether
soft-drop is acceptable vs halting is D-MS.9.

### 7.4 Maintenance per sink

| Op | Iceberg (v1) | Delta (v1, proposed) |
|---|---|---|
| Checkpoint / metadata compaction | n/a | **built-in** (post-commit hook) — required for log read performance |
| Log / metadata expiry | external | **built-in** log cleanup per `delta.logRetentionDuration` (metadata only) |
| Data compaction | external | external (OPTIMIZE by Spark/Databricks) — `optimize()` reserved under `sink.maintenance` |
| Delete unreferenced data files | external | external (VACUUM) — keeps the "app never deletes data artifacts" tenet |

---

## 8. Impact on existing designs (propose-only follow-ups)

| Document | Change |
|---|---|
| `System-Overview.md` | Flow diagram "Iceberg writer" → "Sink (Iceberg / Delta)"; Key Design Decisions: table format = **pluggable**, v1 sinks Iceberg + Delta; `catalog` bullet becomes sink-specific. |
| `Data-Flow.md` | Phase 3 gains **Collapse** (moved from sink contract step 2) and the `TableChangeSet`; Phase 4 → **Sink commit** with the neutral contract + "Iceberg realization" + "Delta realization"; TRUNCATE barrier; state layout note on table-root formats. |
| `Configuration_Specification.md` | §9.4 restructured (§9 below); `catalog` moves under `sink.iceberg`; `schema_evolution` moves to pipeline level; neutral type grammar for `column_type_overrides`; neutral `tables.partitioning`; new rules V-083…V-090; §10 path layout per format. |
| `Schema_Change_Detection_and_Evolution.md` | §7.1 becomes per-sink capability reference; §7.2 table gains the Delta column (§7.3 here); new **Degraded (soft-drop)** outcome. |
| `Type_Mapping.md` | Hop 3 split into 3a Iceberg / 3b Delta; logical-type field metadata (§6.5); `column_type_overrides` grammar. |
| `Ghost_Row_Dedup_and_Primary_Keys.md` | §4 collapse re-homed to core; §0/§8 labelled Iceberg-specific. PK rules unchanged (Delta MERGE also needs a key). |
| `Delivery_Guarantee_and_Position_Guard.md` | Carrier becomes a sink responsibility (`CommitMarker`); add the walk-back-to-own-commit rule (both sinks). |
| `Clustering_Architecture_Research.md` §5.5 | Fence = "sink marker epoch" — Iceberg: snapshot summary; Delta: `txn` app-transaction version. |
| `Iceberg_Partitioning…` | Partition spec becomes neutral; Delta = identity-only in v1. |
| `Iceberg_Maintenance.md` | Add Delta column (§7.4). |
| `Observability_Metrics_and_Tracing.md` | `cdc_iceberg_*` → `cdc_sink_*{sink_type}`; Delta-specific: `cdc_sink_merge_files_rewritten`, `cdc_sink_merge_bytes_rewritten`, `cdc_sink_merge_duration_seconds`, `cdc_sink_soft_dropped_columns`. |
| `CDC_Design_Gap_Analysis_v1.md` | DA-SINK-1 re-scoped to "Iceberg mirror capability"; new gaps DA-MS-* and DA-X-10 (below). |

**New cross-cutting gap surfaced — DA-X-10 (🟡):** the sink marker (`owner_epoch`,
`committed_position`) is written per commit, but **foreign commits** (external compaction/OPTIMIZE,
expire) create snapshots/versions without it. The current Iceberg design reads "the latest snapshot's
summary" — after an external compaction that read returns nothing and the fence silently disarms until
our next commit. Rule for both sinks: **read the marker from the latest commit authored by this
application** (walk back), or use a carrier that persists in table state (Delta `txn`). Independent of
Delta; applies to the Iceberg design as-is. **Resolved in the Iceberg research docs (2026-09-26):**
epoch mirrored into a `cdc.owner_epoch` table property in the same transaction (authoritative read),
position via own-snapshot walk-back by `cdc.pipeline`, "none retained" = not covered —
`Clustering…` §5.5, `Delivery…` §3–§4, `Iceberg_Maintenance.md` §2.

---

## 9. Configuration changes (proposed)

### 9.1 Shape

```yaml
pipelines:
  orders:
    id: orders
    source: { ... }                          # unchanged
    tables:
      include: [ public.* ]
      primary_key:      { public.events: [tenant_id, event_id] }   # unchanged semantics
      partitioning:     { public.orders: [ { column: created_at, transform: day } ] }  # neutral
      column_type_overrides: { public.orders.amount: "decimal(38,10)" }            # neutral grammar
    schema_evolution: { enabled: true, rescan_interval: 60m }       # MOVED from catalog.schema_evolution

    sink:
      type: delta                            # iceberg | delta   (discriminator; REQUIRED, no default)
      write_mode: mirror                     # mirror | changelog (changelog only if D-MS.5 accepted)
      bucket: lake-bucket
      prefix: orders
      flush: { seconds: 60, rows: 100000, bytes: 512MiB }
      max_transaction_batch_rows: null
      max_transaction_buffer_bytes: 1GiB
      buffer_capacity_rows: 200000

      iceberg:                               # iff type = iceberg
        strategy: mor                        # mor | cow   (mirror only)
        catalog:                             # MOVED from pipeline-level `catalog`
          type: rest
          rest: { url: ..., auth: oauth2, ... }

      delta:                                 # iff type = delta
        column_mapping: none                 # none (v1); `name` reserved — delta-rs 1.0 refuses ADD COLUMN under column mapping (P7b)
        checkpoint_interval: 10              # → delta.checkpointInterval (new tables)
        log_retention: 30d                   # → delta.logRetentionDuration (new tables)
        table_properties: {}                 # passthrough delta.* for new tables (validated allow-list)
        catalog: { type: none }              # none (v1) | glue | unity | hive  (registration; reserved)
```

### 9.2 Field tables (new/changed)

| Field | Type | Default | Mutability | Notes |
|---|---|---|---|---|
| `sink.type` | enum `iceberg`\|`delta` | **— (required, no default)** | **PR, re-seed** | Must be specified explicitly (V-091). No default, so a pipeline's target format never changes implicitly when the runtime binary is rebuilt with a wider/different sink set (D-MS.4). Changing it is equivalent to `id` change for the sink: new tables, fresh positions. State records the sink type; resume refuses a mismatched type. |
| `sink.write_mode` | enum `mirror`\|`changelog` | `mirror` | PR, re-seed | Must be in the sink's capabilities (startup assertion). |
| `sink.iceberg.strategy` | enum `mor`\|`cow` | `mor` | PR | Moved from `sink.strategy`. Mirror only. |
| `sink.iceberg.catalog` | union glue/hive/rest | — | PR | Moved from top-level `catalog` (all existing sub-rules V-040…V-048 carry over). |
| `sink.delta.column_mapping` | enum `none`\|`name` | `none` | PR (new tables) | Applies at table creation. `name` is rejected in v1 (V-092): delta-rs 1.0 refuses ADD COLUMN on column-mapped tables (probe P7b). *Revised 2026-09-26 from `name`.* |
| `sink.delta.checkpoint_interval` | integer ≥1 | `10` | PR (new tables) | |
| `sink.delta.log_retention` | Duration | `30d` | PR (new tables) | |
| `sink.delta.table_properties` | map<string,string> | `{}` | PR (new tables) | Allow-listed `delta.*` keys only. |
| `sink.delta.catalog.type` | enum `none`\|`glue`\|`unity`\|`hive` | `none` | PR | v1 implements `none`; others reserved (V-088). |
| `schema_evolution` | object | as today | R | Moved from `catalog.schema_evolution`; semantics unchanged. |
| `tables.column_type_overrides` | map<col, logical-type> | `{}` | PR | Values in the **neutral logical grammar** (`int16`, `int32`, `int64`, `decimal(p,s)`, `string`, `binary`, `date`, `time`, `timestamp_tz`, `timestamp_ntz`, `uuid`, `json`, `list<T>`) — avoids the Iceberg/Delta `timestamp` ambiguity. |

### 9.3 New validation rules

| Rule | Field | Check |
|---|---|---|
| V-091 | `sink.type` | **Required** — absent (after `defaults` merge) is an error; there is no implicit default sink type. |
| V-083 | `sink.type` | Sink compiled into this binary (Cargo feature) — else error. |
| V-084 | `sink.iceberg` / `sink.delta` | Exactly the block matching `type` present (mirror of V-002). |
| V-085 | `sink.iceberg.strategy` | Only valid with `write_mode: mirror`. |
| V-086 | `tables.partitioning` | Every transform ∈ sink capabilities (Delta v1: `identity` only) — else error. |
| V-087 | `tables.column_type_overrides` | Logical type mappable by the sink — else error. |
| V-088 | `sink.delta.catalog.type` | ≠ `none` → error in v1 (reserved). |
| V-089 | `sink.write_mode` | `changelog` → identifier rules relaxed (PK-less allowed) — *only if D-MS.5 and D-MS.6 accepted*. |
| V-090 | (runtime) | Startup capability assertion per §6.4 — pipeline start refused with a named capability. |
| V-092 | `sink.delta.column_mapping` | `name` → error in v1 (reserved until delta-rs supports ADD COLUMN under column mapping; P7b). |

---

## 10. Verification gates — `prototypes/delta-capability/` (proposed)

Mirror of `prototypes/iceberg-capability/`, pinned to `deltalake = "=1.0.0"`:

| # | Probe | Pass criterion |
|---|---|---|
| P1 | MERGE upsert+delete in **one** commit (flag column pattern, §7.2) | one new table version; reads show updated/inserted rows, deleted keys gone |
| P2 | Idempotent replay: apply the same change set twice | table content identical after 1× and 2× |
| P3 | `txn` + `commitInfo` metadata written atomically with the MERGE; readable after reload and **after a foreign OPTIMIZE commit** | `txn.version` survives; own-commit walk-back finds `committed_position` |
| P4 | Fence: two writers, epochs 5 and 6, racing | epoch-5 commit rejected (pre-check or conflict); never both land |
| P5 | Truncate (delete-all) then MERGE in consecutive commits | empty then post-truncate rows only |
| P6 | **Cost**: 10M-row table, flush of 50k changes with (a) sequential keys, (b) random keys | record files/bytes rewritten + latency; decides guidance & whether D-MS.1 needs a caveat |
| P7 | Add column via `merge_schema`; column mapping `name` table readable by Spark + DuckDB | schema updated; external readers OK |
| P8 | Real S3 / GCS / Azure commit (reuse lease-conformance buckets) | concurrent commit to same version → exactly one wins, on each backend |
| P9 | Arrow 59 alignment: one workspace compiling deltalake 1.0 + iceberg `main` | single arrow in `cargo tree -d` |
| P10 | Type round-trip: uuid→string, time→string, decimal(20,0), timestamp vs timestamp_ntz | Spark/DuckDB read expected types/values |

P1, P2, P4, P6, P8 are the gates whose failure would change the Delta design; the rest are confirmatory.

### 10.1 Results (2026-09-26, `prototypes/delta-capability/`, local `file://`, deltalake 1.0.0) — 10/10 green

| Probe | Result |
|---|---|
| P1 atomic delete+update+insert | ✅ one MERGE → one table version, content correct |
| P2 idempotent replay | ✅ identical content |
| P3 marker survives reload + foreign OPTIMIZE | ✅ `txn` persists in table state; latest commit (`OPTIMIZE`) has no marker → own-commit walk-back required and works (**DA-X-10 confirmed real**) |
| P4 zombie fence | ✅ stale zombie → `Concurrent transaction failed` (same-`app_id` conflict); reloaded zombie → pre-check `FENCED` |
| P5 truncate | ✅ delete-all metadata-only (0 rows copied, 0 files added) |
| P6 cost (1M rows / 40 files / 5k updates) | ⚠️ sequential keys: 1 file, 20k rows copied, ~55 ms · **random keys: 40/40 files, 995k rows copied, ~550 ms** |
| P7a add column (default table) | ✅ |
| P7b add column on column-mapping `name` table | ❌ refused by delta-rs 1.0 → **default revised to `none`** (§7.2, §9.2, V-092) |
| P9 single arrow with iceberg `main` | ✅ at resolution (arrow/parquet 59.3), joint compile not run |
| P10 types + independent cross-read | ✅ all Hop-3b types exact through append **and** MERGE; DuckDB 1.5.5 `delta_scan` (delta-kernel) reads identical types/values; tz vs ntz semantics preserved. **Finding:** a `timestamp_ntz` column puts the table at reader v3 / writer v7 (`timestampNtz` feature); without it the table stays at reader 1 / writer 2 (P10b) — gap DA-MS-8 |
| P8 real clouds | ⏳ not run (needs credentials) |

**P6 interpretation.** Delta MERGE cost per flush is ∝ *(number of files whose key range contains a
changed key)* × file size. For time/sequence-correlated updates (append-heavy OLTP, recent-row
updates) this is ∝ change volume — cheap. For uniformly-spread updates (random UUID PKs, bulk
backfills touching old rows) it approaches **a full-table rewrite per flush** (≈200× here). This is not
a correctness risk but it is a real scope caveat — see **D-MS.15**.

---

## 11. Rollout / sequencing (proposed)

1. **Decide** (§12) — this discussion.
2. **Probe** `prototypes/delta-capability/` (P1–P10); in parallel keep the iceberg canary.
3. **Spec edits** (propose → approve): Data-Flow Phase 3/4, Config §9.4/§9.5/§10/§13, Type_Mapping Hop 3,
   Schema §7, System-Overview; gap report: DA-SINK-1 re-scope, DA-MS-*, DA-X-10.
4. **Implementation order:** core (config, sources, accumulator, collapse, `Sink` trait, cluster/lease,
   state) → **Delta sink (mirror)** → Iceberg sink (changelog if D-MS.5; mirror when capability lands).

---

## 12. Decision register (signed off 2026-09-26)

| ID | Decision | Options | Decision |
|---|---|---|---|
| **D-MS.1** | Adopt a pluggable sink with Delta Lake as a first-class v1 sink? | (a) yes, Delta v1 · (b) yes, abstraction only, Delta later · (c) no, keep waiting on iceberg-rust | ✅ **DECIDED 2026-09-26:** **(a)** — the only path that ships a mirror in v1; P6 may add a "high-churn random-key tables" caveat. |
| **D-MS.2** | Sinks per pipeline | (a) exactly one · (b) fan-out list | ✅ **DECIDED 2026-09-26:** **(a) for v1.** Keep positions per (sink, table) internally so fan-out is additive later. Fan-out needs laggard-across-sinks resume + failure isolation — not free. |
| **D-MS.3** | Where does collapse live? | core vs sink | ✅ **DECIDED 2026-09-26:** **Core** — format-independent; each sink receives a collapsed `TableChangeSet`. |
| **D-MS.4** | Default `sink.type` | iceberg · delta · none (required) | ✅ **DECIDED 2026-09-26: required, no default** (V-091). Must be explicit so a pipeline's target format can never shift implicitly when the runtime binary changes its compiled-in sink set; also avoids silently defaulting to a format whose mirror isn't buildable. |
| **D-MS.5** | Add `write_mode: changelog` (append-only `_op` log) | yes / no / reserve | ✅ **DECIDED 2026-09-26:** **Reserve in config, implement after Delta mirror.** Gives Iceberg a shippable mode on today's crate; different consumer contract. |
| **D-MS.6** | If changelog adopted: allow PK-less tables in changelog mode (reopens G-30 / DA-X-6) | yes / no | ✅ **DECIDED 2026-09-26:** **Yes, changelog only** — the halt rationale is equality-delete keying, which changelog doesn't use. |
| **D-MS.7** | `column_type_overrides` grammar | Iceberg strings · neutral logical grammar | ✅ **DECIDED 2026-09-26:** **Neutral grammar** — `timestamp` means opposite things in Iceberg and Delta. |
| **D-MS.8** | Delta mapping of `time` | string `HH:MM:SS.ffffff` · long µs | ✅ **DECIDED 2026-09-26:** **string** — self-describing for readers; long µs available via override. |
| **D-MS.9** | Delta DROP/RENAME column (no delta-rs op) | soft-drop/degrade · halt | ✅ **DECIDED 2026-09-26:** **Soft-drop + warn + metric** — keeps the mirror flowing; data never lost; physical drop deferred until delta-rs exposes it. |
| **D-MS.10** | Iceberg mirror path | keep tracking upstream · also build/contribute RowDelta | ✅ **DECIDED 2026-09-26:** **Keep tracking** (capability-gated); revisit contributing after Delta ships. Multi-sink removes the schedule pressure. |
| **D-MS.11** | Move `catalog` under `sink.iceberg` and `schema_evolution` to pipeline level | yes / keep top-level `catalog` with sink-dependent validity | ✅ **DECIDED 2026-09-26:** **Yes** — catalog kinds and meaning differ per format; schema-evolution policy is sink-neutral. Pre-implementation, so no compatibility cost. |
| **D-MS.12** | Delta catalog registration in v1 | none · glue · unity | ✅ **DECIDED 2026-09-26:** **none** (path-based) in v1; reserve `glue`/`unity`/`hive`. |
| **D-MS.13** | Delta built-in maintenance | checkpoint+log cleanup only · + optimize · none | ✅ **DECIDED 2026-09-26:** **checkpoint + log cleanup** (metadata; required for health); OPTIMIZE/VACUUM external, consistent with Iceberg_Maintenance and the "never delete data artifacts" tenet. |
| **D-MS.14** | Arrow alignment policy | single major workspace-wide (+FFI escape hatch) · per-sink majors via FFI always | ✅ **DECIDED 2026-09-26:** **Single major** (arrow 59 now); FFI bridge only as a temporary per-sink escape hatch. |
| **D-MS.15** | *(surfaced by probe P6)* Delta write amplification for large tables with spread-out updates | (a) accept + document + metrics, guidance: larger flush intervals · (b) (a) in v1 + post-v1 **deletion-vector writer** via low-level `CommitBuilder` · (c) route such tables to `write_mode: changelog` | ✅ **DECIDED 2026-09-26: (b)** — v1 ships (a): documented caveat, `cdc_sink_merge_rows_copied` / `cdc_sink_merge_files_rewritten` metrics, flush-interval guidance. Post-v1: DV writer (read key columns → row positions → write deletion vectors, no data rewrite), gated on its own probe. |
| **R-1** | *(revision of the signed-off `column_mapping: name`, from probe P7b)* `sink.delta.column_mapping` default | `name` · `none` | ✅ **DECIDED 2026-09-26: `none`; `name` rejected in v1 (V-092)** — delta-rs 1.0 refuses ADD COLUMN on column-mapped tables. |
| **D-MS.16** | *(surfaced by probe P10/P10b)* Delta mapping of source `timestamp without time zone` — `timestamp_ntz` raises the table to reader v3 / writer v7 | (a) accept + document · (b) map to `timestamp` (UTC instant, protocol stays 1/2) · (c) string · (d) `sink.delta` option choosing (a)/(b)/(c) | ✅ **DECIDED 2026-09-26: (d), default (a)** — `sink.delta.timestamp_ntz_as: timestamp_ntz` (default) \| `timestamp` \| `string` (config §9.4.3, V-114); per-column `column_type_overrides` still take precedence. Correct semantics by default, explicit escape hatch for legacy-reader estates. |
