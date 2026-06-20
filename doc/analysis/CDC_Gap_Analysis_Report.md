# CDC System — Requirements Gap Analysis & Research Agenda

**Document type:** Pre-implementation analysis  
**Based on:** System-Overview.md · Data-Flow.md · Configuration_Specification.md · Catalog_Auth_Research.md · Type_Mapping.md · Schema_Change_Detection_and_Evolution.md · Sources Index v3 (50 sources)  
**Revision:** v13.13 — doc-reconciliation spec edits applied (G-NEW-20/21/22 ✅)  
**Purpose:** Identify every gap, edge case, and research deficit that must be resolved before detailed requirements can be written

---

## How to read this report

Each section names a **domain**, lists **confirmed gaps**, and closes with a **research agenda**.

Change markers (against v2):
- ✅ **Resolved** — the latest drafts close this gap
- 🔄 **Partially resolved** — progress made, sub-questions remain
- 🆕 **New gap** — surfaced by the latest revisions

Severity codes:
- 🔴 **Blocker** — implementation cannot start without resolving this
- 🟡 **High** — wrong answer causes correctness bugs or data loss
- 🟢 **Medium** — wrong answer degrades performance or operability

---

## v13.13 update — doc-reconciliation spec edits applied (G-NEW-20/21/22 ✅)

The three staged reconciliation edits were applied to the Oleksii-owned spec files (on explicit
instruction):

- **G-NEW-20 ✅** — `Data-Flow.md` state-layout diagram re-rooted: the redundant `pipelines/<id>/`
  segment removed; per-pipeline files now sit directly under the per-pipeline state location
  (`state.prefix`/`sink.prefix`, default `id`), with a note that cross-pipeline data lives under
  `globals.state`.
- **G-NEW-21 ✅** — `Configuration_Specification.md` gained the four Type_Mapping fields
  (`source.toast_strategy`, `source.mysql.tinyint1_as_bool`/`tinyint1_detection`,
  `tables.column_type_overrides`, `tables.numeric_overflow_action`) in their field tables, plus
  validation rules **V-077…V-082**. The stale `tinyint1_detection ↔ schema_evolution.method:events`
  warning was **dropped** (the `method` field was removed; channel C always supplies the catalog query)
  — recorded as a reconciliation note in §13.9.
- **G-NEW-22 ✅** — `Data-Flow.md` `ColumnValue` enum extended to match `Type_Mapping.md`: `UInt64(u64)`,
  `Timestamp { value, with_tz }`, `Time(NaiveTime)`, typed `Array { element_type, values }`, plus the
  `ColumnValueType` tag enum.

**With this, every gap in the report — 🔴/🟡/🟢 — is resolved or staged-as-executed (D-5 conformance run
+ the two flagged §14 research items: MariaDB GTID dialect, iceberg-rust compaction API).**

---

## v13.12 update — operational & scope gaps batched (G-22/26/27/34/35/37/38/11.3 ✅)

`Operational_and_Scope_Gaps_Batch.md` closes a batch of smaller gaps, one decision each, plus a
reconciliation appendix.

- **G-22** — `max_wal_lag_bytes` (default null); `cdc_wal_lag_bytes` always emitted; **alert-only
  default, opt-in halt**.
- **G-26** — `binlog_purge_action: error (default) | re_snapshot`.
- **G-27** — bounded inter-task channels; `sink.buffer_capacity_rows`; **DEGRADED back-pressure** on
  sink/catalog failure (closes 13.3).
- **G-34** — Azure default credential chain + optional `azure.account_name`; container = bucket.
- **G-35** — catalog-specific retry + circuit breaker + DEGRADED (separate from storage).
- **G-37** — per-provider minimum IAM permissions documented (S3/GCS/Azure).
- **G-38 — MariaDB IN scope** (MySQL-compatible path). **Genuine dependency flagged:** MariaDB GTID is
  `domain-server-sequence`, not MySQL `uuid:tx`, so the **G-14 `SourcePosition` MySQL impl must handle
  both GTID dialects** — new §14 sub-research item. Channel-C + DDL classifier validated against MariaDB
  `information_schema`.
- **G-11.3** — `strip_schema` collision = config error; non-stripped transform = `schema__table`.

**Reconciliation appendix (propose-only spec edits, ready to apply):** G-NEW-20 (re-root Data-Flow state
diagram), G-NEW-21 (merge 4 Type_Mapping config fields + rules into config §13), G-NEW-22 (reflect 4
`ColumnValue` enum extensions in Data-Flow). Schema residuals **DT-R2/R5/R6/R7 ratified** (all already
had recommendations; none reopen a gap).

---

## v13.11 update — observability designed (G-33 ✅ / G-32 ✅ / §10.3 ✅)

`Observability_Metrics_and_Tracing.md` was added, closing the metrics catalogue, tracing, and
log/trace correlation.

- **G-33 catalogue** — mandatory Prometheus metrics across replication/throughput, source health,
  snapshot, Iceberg+maintenance, clustering (lease metrics from §19), and pipeline status.
- **G-33 cardinality** — **pipeline-level labels by default; per-table opt-in** via
  `sink.metrics.per_table` (default false), bounding series for large fleets.
- **G-32 tracing** — `globals.tracing.otlp_endpoint` (disabled when absent) + `sample_ratio`;
  batch/commit-granularity spans, never per-source-event.
- **§10.3 correlation** — inject `trace_id`/`span_id`/`pipeline` into the JSON logs.

**Config:** `globals.tracing.otlp_endpoint`+`sample_ratio` (promoted from §16), `sink.metrics.per_table`
+ validation rules.

---

## v13.10 update — Iceberg maintenance designed (G-25 ✅)

`Iceberg_Maintenance.md` was added, closing G-25. **v1 = external maintenance** (Spark/Trino/Iceberg
procedures) because iceberg-rust's rewrite/compaction APIs are immature; the app instead **exposes
when-needed signals** (`cdc_iceberg_delete_files` — the MOR-critical one — plus data-file count,
small-file ratio, snapshot count, manifest depth) and **reserves `sink.maintenance`** for a future
built-in scheduler. Promotion gated on iceberg-rust compaction support (§14 research item); hybrid
(built-in compaction only) is the first step when it lands. Signals fold into G-33.

---

## v13.9 update — MySQL DDL classifier designed (G-19a ✅)

`MySQL_DDL_Classifier.md` was added, closing G-19a — and definitively reversing its v7 blocker
elevation. The classifier is a **router, not a delta parser**: channel C owns the authoritative delta,
so channel B only locates the table + detects rename intent.

- **Two-tier fail-soft (D-19a):** Tier 1 `sqlparser-rs` (MySqlDialect) for structured classify + rename
  intent; Tier 2 a verb+table-name regex on parse failure. A recovered table name **still triggers
  channel C** (catalog delta classifies via §7.2), so parser coverage gaps **degrade, not halt**. Only
  a total failure to identify any table halts (fail-safe).
- **Table-level ops:** DROP→halt table, TRUNCATE→data-clear, RENAME→identity change (§7.2); untracked
  DDL ignored; multi-table DDL routes each.
- **Rename:** provable via Tier-1 structure / channel-C column position → keeps Iceberg field ID;
  otherwise drop+add+warn.

`sqlparser-rs` now sits on a fail-soft path, removing the risk that elevated G-19a in v7.

---

## v13.8 update — Iceberg partitioning & layout evolution designed (G-24 ✅ / G-12a ✅)

`Iceberg_Partitioning_and_Layout_Evolution.md` was added, closing G-24 and G-12a under one principle:
**layout changes apply to new data only; history is never auto-rewritten; full migration is an explicit
re-snapshot/compaction.**

- **G-24 partitioning** — `tables.partitioning` list of Iceberg transform fields (identity/bucket/
  truncate/year/month/day/hour); default **unpartitioned**; `_ts` allowed for time-pruning.
- **Correctness wrinkle** — equality deletes are **partition-scoped**, so a partition-value change
  would orphan the old version. Resolved by treating partition-key mutation like PK mutation: emit the
  equality-delete in the **OLD** partition (routed by the **before-image**, already required by G-13) +
  data in the NEW. Any partition column allowed; sink-writer contract gains "delete partition = before-
  image."
- **G-24 evolution** — partition-spec change = Iceberg spec evolution (new data only, no rewrite, PR).
- **G-12a** — MOR↔COW `sink.strategy` change = new data only; explicit compaction or re-snapshot to
  migrate history (PR). Closes the "applies to new tables only + migration path" gap.

**Config:** `tables.partitioning` promoted from §16 + four validation rules; `sink.strategy` gains
new-data-only semantics.

---

## v13.7 update — large-transaction handling designed (G-29 ✅)

`Large_Transaction_Handling.md` was added, closing G-29 — the tension between "flush only on a
transaction boundary" and bounded memory.

- **Trilemma framed:** bounded memory / atomic visibility / no spill — pick two.
- **Mechanism (D-29.1): bounded split commits.** Above `sink.max_transaction_batch_rows`, flush
  partial-txn Iceberg commits but **pin the resume checkpoint at the txn start** until the source
  commit. A mid-txn crash replays the whole txn; already-flushed slices are **idempotent upserts**
  (G-13) → crash-safe. Only cost: brief partial-visibility for oversized txns.
- **Default (D-29.2): `null` (atomic by default).** No splitting unless the operator opts in. A separate
  **`sink.max_transaction_buffer_bytes`** ceiling **halts the pipeline** (never OOMs the worker) when a
  txn is unsplittable; when splitting is enabled it is just an early-flush trigger.
- **Spill** (preserve atomicity + bound memory) deferred to a future `large_txn_mode: spill`.

**Config added:** `sink.max_transaction_batch_rows` (promoted from §16), `sink.max_transaction_buffer_bytes`
+ validation rules. Cross-ref G-27 (back-pressure buffers) and G-14 (checkpoint pinning).

---

## v13.6 update — pipeline lifecycle state machine designed (G-NEW-15 ✅ / §9.6 ✅)

`Pipeline_Lifecycle_State_Machine.md` was added, closing G-NEW-15 and the §9.6 flush/restart overlap.

- **Principle** — three operator-set **desired** states (`RUNNING`/`PAUSED`/`STOPPED`) reconciled by
  the owning `PipelineController` (level-triggered, clustering §20). `desired_state` is **persisted
  per-pipeline** so pause/stop survive failover and restart — the §2.5 answer.
- **PAUSE** flushes to a clean txn boundary then holds (no in-memory data at failover risk), keeps
  resources warm + lease held. **STOP** also closes the source connection (PG slot persists). **STOPPED
  keeps its lease, owner idle** (no ring churn; immediate resume).
- **Transitions** — `resume` targets **both** PAUSED and STOPPED; `restart` valid only on RUNNING
  (§12.4 discard-and-replay); illegal transitions → `409`.
- **§9.6** — manual `flush` resets the time-based flush timer + counters; manual `restart` follows the
  same in-flight policy as config-driven.
- **Persistence** — `desired_state` in `pipeline-state.json` (not config, not `globals.state`); a
  config reload does **not** override it (paused stays paused).

**State schema:** add `desired_state: running|paused|stopped` (default `running`). Lifecycle endpoints
are **operator** role (from the RBAC matrix).

---

## v13.5 update — clustering D-5/D-6 ratified; G-11 reconciled

The last clustering follow-ons are now signed off (one with an empirical tail).

- **D-6 ✅ ratified** — **Balanced** lease timing is the default (`lease_duration = 15s`,
  `lease_renewal_interval = 5s`), ~20–22s worst-case unowned window, fitting a ~60s max-lag SLA. Fast
  (5s/1.5s) and Conservative (30s/10s) documented as opt-ins. OQ-OB2 numeric-SLA + p99-stall load
  tests are *tuning inputs*, not blockers. (Clustering doc §18.6.)
- **D-5 ✅ approved + scaffolded, open-until-executed** — Route A (OpenDAL) approved as default, with
  startup capability assertion + native-SDK escape hatch. The `LeaseStore` trait and the **T1–T10
  conformance suite** are now scaffolded at **`prototypes/lease-conformance/`** (runnable via
  `cargo test` against MinIO / fake-gcs-server / Azurite). D-5 cannot be closed on paper: the suite
  must pass against **real S3, GCS *and* Azure** once — **T10 (GCS generation-vs-ETag) is the gating
  assertion**. This is the single remaining clustering item awaiting an empirical run.
- **G-11 ✅ reconciled** — "atomic S3 state updates" is the **same** question as D-5; §5.2 already
  established the CAS primitive exists on all three backends (S3 `If-None-Match`/`If-Match`, GCS
  generation, Azure ETag). G-11's matrix row lagged at "Open"; it is now design-resolved, with the
  same empirical conformance gate as D-5.

**With this, every clustering decision (D-1…D-11) is design-closed.** The only open clustering work is
*executing* the D-5 conformance suite against the three real clouds.

---

## v13.4 update — API RBAC matrix & versioning designed (G-16 ✅ / G-9.1 ✅)

`API_RBAC_and_Versioning.md` was added, closing G-16 (the last pure-design blocker) and G-9.1 in
passing. The auth *mechanism* was already done (§6.3.1); this fixes the **roles + endpoint matrix**.

- **Roles** — linear hierarchy `admin ⊇ operator ⊇ viewer`; authorization is one comparison
  `principal.max_role ≥ endpoint.min_role`.
- **Matrix** — viewer = status/observability; operator = pipeline lifecycle + config
  reload/validate (validate is operator, not viewer, because it echoes *resolved* config with
  secrets); admin = `leave cluster` + `shutdown`. Full table in the doc §2.
- **G-9.1 versioning** — functional/authenticated endpoints under **`/v1`**; `/health/{live,ready}`
  and `/metrics` **unversioned + unauthenticated**.
- **Binding per mode** — `none` = all open (dev/trusted-net, V-052 warning); `basic` = single
  principal = **admin**; `jwt/oauth2` = roles from `roles_claim`, mapped, max wins; 401 (no/invalid
  token) vs 403 (valid token, insufficient role) distinguished.
- **IdP role mapping** — configurable **`globals.api.role_mapping`** (default identity on the three
  names) so real IdP role strings (`cdc-admin`, …) map cleanly.
- **TLS-and-auth** — V-052 stays a **warning** (k8s upstream TLS termination is common), not promoted
  to error.

**Config added:** `globals.api.role_mapping` (R, optional) + two validation rules. The G-NEW-15
lifecycle endpoints inherit **operator** from this matrix.

---

## v13.3 update — PostgreSQL slot/publication lifecycle designed (G-15 ✅)

`Postgres_Slot_and_Publication_Lifecycle.md` was added, closing G-15. The config layer already fixed
the *static* side (V-058/059 uniqueness, `auto_cleanup`); this doc specifies the **runtime lifecycle
state machine**.

- **Slot existence matrix** — "slot present" is the *normal* steady state (slots persist + retain WAL),
  so the matrix is {state present/absent} × {slot present/absent}: reconnect-&-resume (normal
  restart); first-start create+snapshot; slot-lost → recreate + re-snapshot; and the one contested
  cell, **state-absent + slot-present (orphan)**.
- **D-15.1 orphan policy** — `source.postgres.slot.on_orphan: halt | adopt_resnapshot`, **default
  `halt`** (operator decides; no surprise data movement). *Adopt-and-resume-without-snapshot* is
  rejected (would silently miss pre-existing data).
- **D-15.2 creation** — `source.postgres.slot_management: managed | require_existing`, **default
  `managed`**. `require_existing` (DBA-precreated, locked-down envs) forces `auto_cleanup = false`.
- **D-15.3 WAL-gone recovery** — unreachable persisted position (slot dropped, or "requested WAL has
  already been removed") ⇒ recreate slot + **incremental re-snapshot** (cheap now via G-6); warn/metric.
  Same recovery family as MySQL purged binlog (G-26); the failure G-22's WAL-lag guard prevents.
- **D-15.4 publication** — app-maintained **explicit table list**; `ALTER PUBLICATION ADD/DROP`
  reconciles on table-set change.
- **D-15.5 mutual exclusion** — the per-pipeline **lease** + PG's active-slot lock already prevent two
  workers fighting over a slot; external same-name collisions are undetectable (PG slots carry no
  ownership tag) — documented, not papered over.

**Config added:** `slot.on_orphan` (R), `slot_management` (PR), + two validation rules (require_existing
⇒ auto_cleanup false; domain checks).

---

## v13.2 update — incremental snapshot model designed (G-6 ✅ / G-7 ✅ / G-20 ✅)

`Snapshot_Resumability_and_Consistency.md` was added, resolving three snapshot gaps at once by
switching from a **blocking consistent snapshot** to an **incremental chunked snapshot** (DBLog /
Debezium-incremental technique, adapted to our read-only user and upsert sink).

**The trigger:** the current Data-Flow Phase 1 is internally contradictory — it claims both a
single-transaction consistent snapshot (`EXPORT_SNAPSHOT` / `COPY`) *and* per-page crash resume, which
cannot coexist (the snapshot transaction dies with the worker; `COPY` has no cursor). Resolving G-6
forced a model choice; incremental was selected.

- **G-6 ✅ — resumability.** Snapshot proceeds in identifier-keyed **chunks**, each in a short
  `READ COMMITTED` transaction, checkpointed by a **typed identifier-tuple cursor** with a
  **binary-collation ordering pin** (the subtle correctness point for text keys). Row-value keyset
  predicate `WHERE (k) > (cursor)`. The old "no suitable cursor key" fallback is **moot** — G-13
  guarantees every replicated table has an orderable identifier (PK-less tables already halt).
  `snapshot.chunk_size` (default 10000), checkpoint per chunk.
- **G-7 ✅ — consistency.** Lock-free and **convergent** instead of frozen-instant: per-chunk position
  **watermarks** + G-13 upsert idempotency reconcile snapshot rows against concurrent stream changes.
  Eliminates the MySQL **`RELOAD`/`LOCK TABLES` privilege** requirement and the "fallback when
  consistency can't be achieved" concern. **No `consistent_snapshot` toggle** — incremental is the
  single model. Order flips to **stream-first, snapshot-concurrent**.
- **G-20 ✅ — on-demand read point.** An on-demand snapshot is the same chunk loop run **without
  pausing the stream**; the `pause_lsn + 1` read-point assumption disappears. Re-snapshot-after-unsafe-
  DDL and ad-hoc snapshots use the identical mechanism.

**Doc impact owed (spec, propose-only):** Data-Flow.md Phase 1 + On-Demand Snapshot rewritten;
snapshot privilege preconditions drop MySQL `RELOAD`/`LOCK TABLES`. **Config:** add
`snapshot.chunk_size`; no `consistent_snapshot`. Cross-ref G-22/G-26 (a slow snapshot lengthens WAL/
binlog retention — the lag guard interaction).

---

## v13.1 update — delivery-guarantee position guard designed (G-14 ✅)

`Delivery_Guarantee_and_Position_Guard.md` was added, closing G-14. The key move is recognizing that
**G-13's PK-keyed upsert + mirror deletes make the pipeline row-level idempotent**, so the
committed-position guard is **not** the correctness mechanism — it is a write-skip optimization and the
checkpoint-advance carrier. This dissolves the batch-re-alignment-on-replay hazard entirely (a
re-aligned replay either re-writes idempotent rows or is skipped).

Decisions (D-14.1…D-14.5):

- **D-14.1 — comparison.** A `SourcePosition` abstraction: PG = numeric LSN (`≥`/`max`/`min`); MySQL =
  GTID-set with **subset** (covers), **union** (commit), **intersection** (resume frontier), plus a
  **canonical GTID-set serialization** (sorted UUIDs, merged intervals) for stable storage. The old
  `committed_lsn ≥ flush_lsn` test was only ever valid for PG.
- **D-14.2 — scope: per-table HWM, laggard checkpoint.** Multi-table flushes are **not atomic** (each
  table is a separate Iceberg commit), so each table carries its own committed position; the pipeline
  resume point persisted to state storage is the **min (PG) / intersection (MySQL) across all tracked
  tables** — the laggard — so no table loses data on restart.
- **D-14.3 — idempotency is correctness.** The position guard is demoted to optimization +
  checkpoint-advance. Valid because PK-less/append-only tables are rejected (G-13 §3).
- **D-14.4 — property generalized.** Snapshot-summary `committed_lsn` → **`cdc.committed_position`**
  (engine-tagged LSN or canonical GTID-set), coexisting with `cdc.owner_epoch` (clustering D-7).
- **D-14.5 — limitation stated.** Cross-table **atomic visibility is not guaranteed** (separate
  commits); per-table exactly-once-effect is. Cross-ref G-29 (large-txn tension) and G-25 (a
  multi-table-commit catalog like Nessie would be required for true cross-table atomicity).

Edge cases folded in: WAL/binlog purged below the resume frontier → re-snapshot (G-26/G-22);
source-primary failover adds a second GTID UUID (set ops absorb it); empty per-table flush just
doesn't advance that table's HWM.

---

## v13 update — ghost-row dedup & PK keying designed (G-13 ✅ / G-30 ✅-rejected)

`Ghost_Row_Dedup_and_Primary_Keys.md` was added, closing the highest remaining correctness blocker.
It specifies the full equality-delete keying and intra-batch dedup contract for the MOR sink.

The spine is **§0's one rule**: an Iceberg equality delete applies only to data files with a
*strictly smaller* sequence number, which is both what makes in-batch upserts correct and what
produces ghost rows when violated. Every decision flows from staying on the right side of it.

Decisions signed off (D-13.1…D-13.7):

- **D-13.1 / D-13.7 — identifier source.** Default identifier = inferred source PK (channel-C catalog
  query), overridable via **`tables.primary_key`** (kept — the only way to support PG `REPLICA
  IDENTITY USING INDEX` on a non-PK unique index). Materialized as Iceberg `identifier-field-ids`.
  Identifier columns must be top-level, primitive, NOT NULL, non-floating.
- **D-13.2 — PK-less tables: HALT.** System-Overview line 50 kept verbatim. **G-30 is resolved by
  rejection** — the reserved `tables.surrogate_key` field is **removed** from config §16 and no
  append-only mode is added. Accepted limitation: genuinely key-less tables are un-replicable.
- **D-13.3 — intra-batch collapse.** Each flush collapses to ≤1 delete + ≤1 data record per identifier
  tuple, **keep-last by a per-change `_seq`** (PG: change WAL LSN; MySQL: binlog file+pos / GTID+event
  index). The `_lsn` *commit* column is insufficient (many rows share one commit position).
- **D-13.4 — before-image contract.** Identifier columns ⊆ source replica-identity columns — **new
  validation**, halt with the missing-column list otherwise (PG `DEFAULT` covers PK; a non-PK override
  needs `FULL`/`USING INDEX`). MySQL `binlog_row_image = FULL` already required.
- **D-13.6 — delete representation: MIRROR.** A DELETE emits an equality-delete only, **no `D` data
  row**; `_op` in data files is only ever `I`/`U`/`r`. Appending a `D` row alongside its own
  equality-delete in one commit is exactly the ghost-row bug. Key mutation (UPDATE changing the PK) =
  delete(old key) + insert(new key).
- **D-13.5 — strategy.** MOR/equality-delete is primary; COW = new-tables-only, same identifier rules.

**Consistency with schema evolution (no new decision):** Iceberg `identifier-field-ids` reference
field IDs not names, so a *provable rename* preserves the identifier automatically; and §7.2 already
classifies PK / REPLICA IDENTITY change as Unsafe → halt + re-snapshot — the one change that
invalidates the identifier. `tables.primary_key` is therefore mutability class **PR** (change ⇒
re-snapshot).

**New implementation gate (added to §14):** verify iceberg-rust 0.9.0 exposes an equality-delete
writer that assigns delete files the enclosing commit's sequence number — the §0 guarantee depends on
it. If not, the writer needs a workaround. This is the one item that could reopen G-13 at
implementation time.

**Config-spec follow-ups (pending application to `Configuration_Specification.md`):** add
`tables.primary_key` (PR) with its four validation rules; **remove** `tables.surrogate_key` from §16.

---

## v12 update — clustering decisions D-1–D-4, D-7 signed off

The five outstanding clustering decision points are now resolved (recommendations approved, with
sharpenings). **All clustering design decisions D-1…D-11 are closed**; what remains is
implementation-time verification (D-5 conditional-write conformance, D-6 timing under load) and the
proposed config-spec edits below.

- **D-1 — `node_id` source.** `node_id` *is* the existing `globals.cluster.memberlist.node_name` (not
  a new field). k8s → set it to the **StatefulSet pod name** via the Downward API, which **mandates a
  StatefulSet (not a Deployment)** for workers — a Deployment's changing pod names would reshuffle the
  derived ring on every restart. Standalone → persisted-once UUID, `${HOSTNAME}` fallback. §3.4.
- **D-2 — lock backend.** Cloud-storage CAS lease is primary/default. Reserve
  `cluster.lock_backend: storage | kubernetes` but **build `storage` only for v1**. `lock_backend` is
  **pinned in the `globals.state` anchor** (cluster-wide-consistent) — a mixed-backend cluster is
  split-brain, so a mismatched node refuses to start. §5.6 / §12.
- **D-3 — cold-start precedence.** Cloud-generation-wins-at-runtime, file-is-input, reload-promotes.
  Simplified: no "compare file to generation number" — a starting node adopts the latest cloud
  generation if one exists; the file matters only at first bootstrap (CAS-create gen 1) or explicit
  reload. **Documented footgun: a pod restart does not apply config-file changes — only
  `POST /v1/config/reload` does.** §9.3.
- **D-4 — discovery.** Add `cluster.discovery: static | dns` (+ `discovery_dns_name`, default
  `static`); `dns` resolves **and periodically re-resolves** a headless Service so an isolated node can
  re-bootstrap. Already reserved in config spec §16 (G-9 row). §10.2.
- **D-7 — `owner_epoch` fencing (assert it).** `owner_epoch` is embedded in the Iceberg snapshot
  summary (`cdc.owner_epoch`); the commit reload-retry loop rejects a commit when
  `latest_committed_epoch > my_epoch` — a **sink-side fencing token** that stops a zombie committing
  *after* a new owner takes over. Defense-in-depth atop the metadata-pointer CAS + `_lsn` dedup, not a
  correctness prerequisite. §5.5.

**Config-spec follow-ups — D-1 / D-2 APPLIED:**
(1) **D-1** — `globals.cluster.memberlist.node_name` (§6.5.1) now documents the sourcing rule:
it *is* the ring `node_id`, must be stable / non-IP, k8s → StatefulSet pod name via Downward API
(StatefulSet **not** Deployment), standalone → persisted UUID / `${HOSTNAME}`.
(2) **D-2** — `globals.cluster.lock_backend` promoted into the §6.5 field table (enum, default
`storage`, `kubernetes` reserved/not-yet-implemented), pinned cluster-wide via the `globals.state`
anchor; validation rules **V-075** (domain ∈ {storage, kubernetes}; `kubernetes` → error in v1) and
**V-076** (differs from anchor → refuse to start, parallels V-062 for `tokens`) added.
(3) `discovery`/`discovery_dns_name` remain reserved in §16 (G-9 row). The lease-timing fields stay
§16-reserved pending D-6.

---

## v11 update — clustering reconciler robustness (D-9/D-10/D-11)

`Clustering_Architecture_Research.md` gained **§20**, closing the three remaining clustering
correctness sub-gaps surfaced while reading §8/§13 as the implementation contract. All three are
resolved at the design level; none required reopening a 🔴 blocker (G-8/G-9 stay resolved).

- **D-9 — reconciler ↔ renewer orchestration.** §8.1's single reconcile loop would block every
  pipeline behind any one pipeline's takeover-evaluation wait (up to a full `lease_duration`), and the
  "who writes the lease" boundary between the loop and the §13 renewer was undrawn. Resolved by
  splitting into a **`ClusterReconciler`** (owns only the ring; pushes per-pipeline `Own`/`DontOwn`
  targets; never touches a lease) + a **per-pipeline `PipelineController`** (sole lease-writer; runs
  acquire/takeover/renew/handoff in its own task, with the §19 renewer living in its `Owned` state).
  Triggering is **event-driven + periodic-resync**, controllers are **idempotent / level-triggered**
  (Kubernetes-controller pattern), so at-most-one / at-least-one holds under dropped or reordered
  events. §8.1 and §13 refined to point at §20.1.
- **D-10 — unplanned-takeover storm.** `max_concurrent_handoffs` capped only *planned* drains; an
  unplanned death of a multi-pipeline node let survivors stampede source-DB reconnects + state reads.
  Resolved by `cluster.max_concurrent_takeovers` (default `4`) gating concurrent **Phase-0 starts**
  (not the cheap takeover-evaluation waits) on the unplanned path, plus jitter. Per-node-global scope;
  per-source-endpoint bulkhead noted as future. It is a tuning knob, not a safety boundary — the lease
  still guarantees at-most-one.
- **D-11 — flap immunity.** Stabilization (§8.5) covered only first-time joins. Resolved by keeping
  **suspect nodes in the ring** (only `dead`/`left` removes a node — safe because safety is the lease,
  not the ring, §8.4) and applying `rebalance_stabilization` to **all ring additions including
  rejoins**, while ring **removals act immediately** (dead-node failover is lease-expiry-driven and
  must not be delayed). A transient suspicion now causes zero ring churn.

New config field: `cluster.max_concurrent_takeovers` (R, default 4). **Config-spec follow-up —
APPLIED:** `Configuration_Specification.md` §16 now reserves the full **G-9 rebalancing/discovery
surface** (`rebalance_stabilization`, `max_concurrent_handoffs`, `max_concurrent_takeovers`,
`drain_timeout`, `discovery`/`discovery_dns_name`, `memberlist.lifeguard_enabled`) marked
design-landed, and validation rule **V-074** (`max_concurrent_handoffs` / `max_concurrent_takeovers`
≥ 1) was added. These fields stay §16-reserved pending promotion into the §6.5 `globals.cluster` field
table when the clustering design is ratified.

Remaining clustering decision points: D-1 (node_id source), D-2 (lock_backend switch), D-3 (cold-start
precedence), D-4 (DNS discovery), D-7 (owner_epoch into Iceberg metadata) — recommendations exist,
sign-off pending. D-5/D-6 remain elaborated-and-recommended pending implementation verification.

---

## v10 update — clustering lease renewal hardened (D-8)

`Clustering_Architecture_Research.md` gained **§19 (D-8 elaborated)**, closing a correctness/
availability defect in the original §5.3 renew rule. The old text treated *every* renewal failure as
"I lost the lease → hard stop," conflating two different classes:

- **`412` (definitive loss)** — a challenger CAS-advanced the lease object → stop immediately. Correct.
- **Transient storage error (timeout / `5xx` / throttle / conn-reset)** — the arbiter was unreachable;
  lease status is *unknown*, not lost. Stopping here needlessly drops a healthy holder's pipelines on
  every storage blip, and in **single-node mode** stops the only worker for a 2-second S3 hiccup.

§19 replaces the rule with a **renew-outcome taxonomy** + a **renewer state machine**
(`Held → Renewing → {Held | Degraded | Lost}`) bounded by a **safety deadline**:
`deadline = last_renew_local + lease_duration − guard`, with `guard = lease_duration ×
lease_safety_margin` (default `0.15`), measured on the holder's **own** clock. Transient failures move
to *Degraded* and retry until either a renew succeeds or the deadline is hit; only `412`/`404`/`403`
stop immediately. The deadline preserves §5.3's clock-skew-safe property (both holder-stop and
challenger-take are durations anchored to the same ETag-write event; only bounded clock-*rate* drift
matters, which `guard` absorbs). This also gives the §18.2 invariant
`lease_renewal_interval ≤ lease_duration / 3` its purpose — it guarantees two retry attempts inside
the deadline — so that rule is **promoted from advisory to an enforced config-validation rule**.

Commits gate at commit-start on `state ∈ {Held, Degraded} ∧ now() < deadline`, with the Iceberg
atomic-commit fence (§5.5) as the slip backstop. New config field: `cluster.lease_safety_margin`
(PROC, default `0.15`). New clustering metrics: `cdc_lease_state`, `cdc_lease_renew_failures_total`,
`cdc_lease_renew_latency_seconds` (fold into G-33). D-8 is **resolved (design)**; two small follow-ups
fall out (see §4.1 and §9.2 below):

- **Config-spec follow-up — APPLIED:** `Configuration_Specification.md` now carries
  `cluster.lease_safety_margin` (§16 G-8 row, defaults noted), validation rule **V-072**
  (`lease_safety_margin ∈ [0, 0.5]`, warn outside 0.05–0.30) and **V-073**
  (`lease_renewal_interval ≤ lease_duration / 3`, hard error). The three lease fields remain in §16
  reserved-for-design pending promotion into the §6.5 `globals.cluster` field table when the
  clustering design is ratified.
- The three lease metrics extend the still-open metrics catalogue (G-33).

Remaining clustering decision points: D-1 (node_id source), D-2 (lock_backend switch), D-3 (cold-start
precedence), D-4 (DNS discovery), D-7 (owner_epoch into Iceberg metadata) — recommendations exist,
sign-off pending. D-5/D-6 remain elaborated-and-recommended pending implementation verification.

---

## v9 update — clustering architecture designed

`Clustering_Architecture_Research.md` was added: a full design for the six clustering
responsibilities. It resolves the cluster-coordination blockers that a config schema could not
address on its own, and is the contract for the membership/ring layer, the lease layer, the
rebalancing/handoff protocol, and config propagation.

The spine of the design is a **two-plane separation by consistency requirement**:

- **Coordination plane (AP):** `memberlist`/SWIM maintains membership + `alive/suspect/dead/left`
  status + a tiny per-node metadata blob (phase, incarnation, applied config generation). The hash
  ring is **derived** from membership (every node computes a byte-identical ring), not gossiped as
  mutable state. This makes the cluster-wide `tokens` rule (G-36) a *safety invariant*, not a tuning
  knob, and resolves the SWIM-vs-Scuttlebutt question.
- **Mutual-exclusion plane (CP):** a per-pipeline **lease object in `globals.state`**, acquired and
  renewed by **conditional writes (compare-and-swap)**, with a **clock-skew-safe** acquire/renew/
  takeover protocol. The ring says who *should* own a pipeline; the lease says who *may run* it.

Key consequences folded into the sections below: **G-8 (lease) and G-9 (drain) are resolved at the
design level**, **G-2 / G-17 (config propagation + reload proxy) are resolved** (generation pointer
in storage + gossip nudge; no proxy — every node reconciles its own owned pipelines), **G-23 is
decided** (keep SWIM + Lifeguard; borrow generation-tagged identity), and **gaps 5.1 / 5.2 are
closed** (concrete `globals.state` object layout + conditional-write mechanism, with a `version`
field on every state file). Single-node mode (4.4) becomes a first-class, same-code-path case with
the lease left active.

Two follow-on items remain in flight and are being elaborated first (gate everything downstream):
**D-5** — verify the conditional-write path is genuinely atomic on S3 *and* GCS *and* Azure (SDK vs
`opendal`; GCS is the trap — it uses *generation* preconditions, not ETag-on-write); and **D-6** —
pick concrete `lease_duration` / `lease_renewal_interval` defaults against the failover-time and
replication-lag SLA (ties to OQ-OB2). See `Clustering_Architecture_Research.md` §17–§18.

The new clustering config surface (`cluster.lease_duration`, `cluster.lease_renewal_interval`,
`cluster.lock_backend`, `cluster.rebalance_stabilization`, `cluster.max_concurrent_handoffs`,
`cluster.drain_timeout`, `cluster.discovery[_dns_name]`, `cluster.memberlist.lifeguard_enabled`) is
specified with mutability classes in that doc §11 and slots into the config spec §16 reservations
for G-8.

---

## v8 update — type mapping and schema evolution designed

Two substantial design documents were added, each targeting one of the two highest-leverage blockers, and the config spec was updated in lockstep. This is the most significant correctness-mechanics advance since the analysis began.

### `Type_Mapping.md` — resolves G-18 ✅ (was the #1 leverage blocker)

A complete three-hop mapping specification: DB wire format → `ColumnValue` (source-specific decode) → Arrow `DataType` (source-agnostic accumulate) → Iceberg type (sink, via iceberg-rust). What it delivers:

- **Full PostgreSQL type table** — core, date/time, the large set of text-serialized types with no Iceberg analogue (inet/cidr/macaddr/geometric/range/tsvector/xml → `string`), and structured types (uuid → `FixedSizeBinary(16)`→`uuid`, json/jsonb → `string`, arrays → `List<T>`, PostGIS → WKB `binary`).
- **Full MySQL type table** — including the unsigned-integer widening ladder, with the key correctness call that `BIGINT UNSIGNED` maps to `decimal(20,0)` because no signed 64-bit type holds its range; `TINYINT(1)` → `boolean` (with the information_schema detection caveat); `SET` → `list<string>`; `ENUM` → `string`; `BIT(N)` → `fixed`.
- **Four `ColumnValue` enum extensions** identified as prerequisites: a tz-awareness flag on `Timestamp` (distinguishes PG `timestamp` from `timestamptz`), an element-type annotation on `Array` (so the accumulator can build a typed `ListBuilder<T>`), a new `Time(NaiveTime)` variant, and a new `UInt64` variant for `BIGINT UNSIGNED`. Each has a documented correctness rationale.
- **The Hop-3 Arrow→Iceberg annotation table** — what iceberg-rust handles automatically vs. what needs an explicit hint (notably `FixedSizeBinary(16)`→`uuid`, and the rule never to pass unsigned Arrow types to iceberg-rust).
- **An unsupported-type fallback chain** (override → text → bytes → halt, all logged) so no data is silently lost.
- **A partial DDL classification table** (later superseded/ratified by the schema-evolution doc, see below).

It also surfaces **new config fields** with their own validation rules: `source.mysql.tinyint1_as_bool` / `tinyint1_detection`, `source.toast_strategy` (carry_forward vs re_query), `tables.column_type_overrides`, and `tables.numeric_overflow_action`. These are tracked below as G-NEW-21.

### `Schema_Change_Detection_and_Evolution.md` — resolves G-19, G-19b, OQ-DT2, OQ-DT3 ✅; reduces G-19a 🔽

A design-reviewed-and-approved model that replaces the earlier `method: query|events` framing with a unified **four-channel detection** pipeline feeding one evolution path:

- **Channel A (startup reconciliation)** — baseline new tables, drift-check existing ones, trigger re-snapshot on a non-replayable gap.
- **Channel B (in-band trigger)** — PG Relation-message diff / MySQL QUERY_EVENT DDL-classify; establishes *when/where/which table* at the exact stream position.
- **Channel C (catalog augmentation)** — on each B trigger, query the source catalog to produce the complete, unambiguous delta (types, nullability, PG `attnum` for provable renames).
- **Channel D (idle rescan)** — periodic batched catalog query for tables with no DML; freshness/alerting only.

The standout design decisions:

- **G-19a scope is materially reduced 🔽.** Because channel C supplies the column delta from the catalog, MySQL no longer needs a full ALTER-delta SQL parser — only a **DDL classifier + table-name extractor** (plus optional `RENAME`/`CHANGE` keyword detection for rename intent). This de-risks what was an elevated blocker in v7: `sqlparser-rs` is still needed but for a much narrower, lower-risk job. Unparseable DDL on a tracked table fails safe (halt that table).
- **The §0 correctness invariant** — the catalog query supplies the *attributes* of the change the in-band signal implicates, applied *in stream order* and *scoped to that change*, never fast-forwarding the target schema to "now." This correctly prevents the silent-data-loss hazard of applying a drop/narrow early.
- **The §7.2 DDL classification table (the G-19 contract)** — every delta is Safe / Unsafe / Ignorable, tied to Iceberg's actual safe-operation set (add, drop, rename, reorder, and the limited promotions int→long, float→double, decimal precision-widen). Rename is positively provable (PG attnum / MySQL keyword) and degrades to drop+add-with-warning only when unprovable.
- **The §8 apply protocol (resolves OQ-DT2)** — seal → flush → classify → evolve → rebuild → resume, at a transaction boundary, with an idempotent guard mirroring the Phase-4 `committed_lsn` check so DDL replay on restart is absorbed.

**Config contract change (already applied to `Configuration_Specification.md`):** the `schema_evolution.method` field is removed (the four-channel model is fixed, not an operator choice); `interval` is renamed `rescan_interval` (default `60m`, `0` disables) and now always applies; validation rules V-024/V-031/V-032 are deleted; and a new **source-SELECT precondition** is added (the replication user must hold SELECT on information_schema / system catalogs — required for every pipeline including MySQL, since channels A/C/D query the catalog). I verified the config spec already reflects all of these (§9.5.1, the new precondition at §13, the changelog item 18).

**New gaps surfaced:**
- **G-NEW-21 🟡** — the four new Type_Mapping config fields and the source-SELECT precondition need their validation rules folded into the master table (§13). The config spec added the SELECT precondition but the four type-mapping fields (`tinyint1_*`, `toast_strategy`, `column_type_overrides`, `numeric_overflow_action`) and their four validation rules are specified in Type_Mapping.md and not yet merged into the spec's §13.
- **The `ColumnValue` enum extensions** are a prerequisite for the Hop-2 accumulator and should be reflected in Data-Flow.md (which currently shows the un-extended enum). Tracked as G-NEW-22 🟢 (doc reconciliation).

With these two documents, **the two top correctness blockers (G-18, G-19) are resolved**, G-19a is de-risked, and four OQ/DT items close. The remaining blockers are clustering coordination (split-brain lease, rebalancing drain, cross-worker config propagation), the ghost-row/delivery-guarantee mechanics, and the RBAC matrix.

---

## v7 update — full Configuration Specification supersedes the draft

The `Configuration_file_model.md` draft has been replaced by `Configuration_Specification.md` — a 1,264-line authoritative spec that is the contract for the config loader, the validation API endpoint, and the reload/reconcile engine. It is a large step up in rigor and closes or advances a substantial set of gaps.

**Newly resolved by the specification:**

- **G-1f (master validation table) ✅** — §13 consolidates 70+ rules (V-001…V-071) across structure, requiredness, value domains, cross-field constraints, the 9 catalog-auth rules, API security, identity/naming, reload-time, and resolution. This was the last open config-side item. The validation API endpoint now has a complete spec.
- **G-1a (merge semantics) ✅✅** — §8 is now fully normative: scalars/maps deep-merge, lists replace wholesale (§8.3, previously unspecified), and both `catalog` and the `source` engine block are discriminated unions whose active branch drops the sibling (§8.4). A worked example (§8.5) is included.
- **G-2 partial / config side — reload reconcile engine ✅** — §12 defines diff granularity (per-pipeline by `id`, per global field), add/remove/rename/`id`-change/modify handling, and the restart policy. The mutability-class system (R / PR / PROC, §1.2) assigns every field a reload behavior. The cross-worker *propagation* of a reload remains a design item (still G-2/G-17), but the single-worker reconcile is now fully specified.
- **G-5 (in-flight batches on restart) ✅** — §12.4 sets the default to discard-and-replay from the last confirmed offset, with the Phase-4 exactly-once guard deduplicating and a graceful-flush flag noted as planned. The behavior is now decided.
- **API JWT/OAuth2 verification schema ✅** — §6.3.1 defines `issuer`, `audience`, `jwks_url`, `discovery_url`, `roles_claim`. (The RBAC *matrix* is still deferred — see G-16.)
- **Secret rotation without restart ✅ (was 1.3)** — §4.6 introduces `credentials_file` / `token_file` (mutually exclusive with inline credentials), read during resolution, whose file *contents* can be rotated and picked up on reload because the reconcile diff runs on resolved values (§11.1). Env-var secrets remain PROC; file-based secrets are R/PR.
- **Failure blast radius ✅ (new, well-reasoned)** — §1.4 establishes all-or-nothing config-time validation (any error rejects the entire config; on reload the previous config stays in effect) vs. per-pipeline/per-table isolation at runtime. This is a clean, correct boundary that resolves ambiguity about partial application.
- **Byte-size units & env-var grammar ✅** — §4.1–4.3 define duration/size unit handling (fixing the `512000000` "=512Mb" inconsistency → `512MiB`) and a complete env-var grammar (`${VAR}`, `${VAR:-default}`, `$$` escape, undefined-is-error, post-parse coercion).
- **`config_version` ✅ (new)** — a mandatory top-level structural-compatibility gate (§3).
- **Strict parsing ✅ (new)** — `deny_unknown_fields` makes any unknown key an error, the primary typo-protection mechanism.
- **Identity charset/uniqueness ✅** — `pipeline.id` (V-055/056), name (V-057), PG slot/publication (V-058/059), and the `defaults`-may-not-set-unique-names rule (V-060) are explicit.

**Materially advanced (still design items, but the config surface is now reserved):**

§16 of the spec lists 12 future fields deliberately deferred to design, each tied to a gap ID (lease tuning → G-8; `primary_key`/`surrogate_key` → G-13/G-30; `partitioning` → G-24; `max_transaction_batch_rows` → G-29; `max_wal_lag_bytes` → G-22; `binlog_purge_action` → G-26; `consistent_snapshot` → G-7; `buffer_capacity_rows` → G-27; `otlp_endpoint` → G-32; RBAC matrix → G-16; DDL classification → G-19; `glue.strip_schema` transform). Reserving these means the schema can grow without churn — the design work is unchanged but its landing spot is now defined.

**Two new findings from the specification:**

- **G-NEW-20 🟢 — Data-Flow.md state-root nesting is now flagged as redundant by the spec.** §10 of the spec observes that Data-Flow.md still roots per-pipeline state at `<prefix>/pipelines/<pipeline-id>/` under a "global" prefix, but under the spec's per-pipeline `state`/`sink` location model (and the `globals.state` boundary reserving the global prefix for cross-pipeline data only), the `pipelines/<id>/` segment is redundant. The spec recommends Data-Flow.md re-root at the per-pipeline state location and drop the segment. This is a documentation-reconciliation item between the spec and Data-Flow.md — note the irony that v5 "fixed" Data-Flow.md to *add* `<pipeline-id>` keying, and the spec now wants that nesting removed because state is already per-pipeline-scoped by `sink.prefix`/`state.prefix` defaulting to `id`. The two are reconcilable (the *id* is still the key; it's the redundant `pipelines/` parent segment that goes), but Data-Flow.md needs one more edit to match the spec.
- **G-19a elevated 🔴 — MySQL `schema_evolution.method: events` is now the hard default for MySQL.** §9.5.1 makes `events` the conditional default for `source.type = mysql` (and V-031 forbids `events` on PostgreSQL). Since `events` parses DDL from MySQL binlog QUERY_EVENT, the MySQL DDL-SQL parser (G-19a, e.g. `sqlparser-rs`) is now on the default path for every MySQL pipeline, not an opt-in. This raises its priority: a MySQL deployment cannot do schema evolution at all without it.

With the specification in place, **the entire config-modeling phase is complete and consistent.** Every remaining open item is engineering design (correctness mechanics, cross-cluster coordination) — the spec itself enumerates them in §16.

---

## v6 update — defaults.catalog schema aligned

- **G-NEW-19 ✅** — the `defaults.catalog.hive` and `defaults.catalog.rest` blocks now carry the full, protocol-correct auth schema (Thrift `none`/`ldap`/`kerberos` + TLS for hive; HTTP `none`/`bearer`/`oauth2`/`sigv4` + warehouse/prefix/TLS for rest), matching the pipeline-level blocks. The obsolete `auth: basic` + `user`/`password` form is gone from defaults. The DRY pattern (catalog auth set once in defaults) now produces a valid configuration.

With this fix, **every config-schema gap and every document-consistency gap is closed.** The config model and the three architecture documents are mutually consistent. All remaining open items are engineering-design problems (correctness mechanics, cross-cluster coordination) that no config schema can resolve — see the priority matrix.

---

## v5 update — catalog auth resolved, state keying fixed

Two significant items closed since v4, driven by the new `Catalog_Auth_Research.md` document and config edits:

- **G-1g ✅ (catalog JWT/OAuth2 auth — was the last schema-level 🔴 Blocker)** — fully resolved. The pipeline-level `catalog.hive` and `catalog.rest` blocks now carry complete, protocol-accurate auth schemas. Hive uses Thrift/SASL modes (`none` / `ldap` / `kerberos`) with a `kerberos` sub-block and optional Thrift TLS. REST uses HTTP modes (`none` / `bearer` / `oauth2` / `sigv4`) with proper `oauth2` (client_id/client_secret/token_url/scope) and `sigv4` (region/service) sub-blocks, plus `warehouse`, `prefix`, and `tls.ca`. The research correctly removed the inapplicable `basic`/`jwt`/`aws` placeholder values and documented the deprecated `/v1/oauth/tokens` endpoint trap. The 9 implied validation rules are captured for folding into G-1f.

- **G-10 ✅ (state path keying on `id` not name)** — fully resolved. Data-Flow.md's state-layout diagram now shows `pipelines/<pipeline-id>/`, and all three config prefix comments (`defaults.state`, `sink`, pipeline `state`) now say "use pipeline ID if omitted." The intent and the documentation are now consistent.

**One new inconsistency was introduced by the catalog-auth edit (now resolved in v6):**
- **G-NEW-19 ✅** — `defaults.catalog.hive`/`rest` initially lagged on the old `basic`/`user`/`password` schema while the pipeline blocks were upgraded. Fixed in v6: the defaults block now mirrors the new schema.

---

## v4 update — document contradictions fixed

The four parallel-editing contradictions flagged in v3 are now resolved:
- **G-NEW-16 ✅** — default snapshot mode is now `never` in both the config draft and System Overview line 46.
- **G-NEW-18 ✅** — System Overview line 46 no longer lists the orphaned `only` mode; it now reads `always, never (default), initial, dump_always, dump_initial`, matching the detailed section and the config.
- **G-NEW-17 ✅** — `source.postgres.auto_cleanup` is now present in both the defaults and pipeline source blocks, matching the reference in Data-Flow.md's shutdown sequence.
- **G-31 typo ✅** — the memberlist TLS CA field is corrected from `cat:` to `ca:`.

**One residual remains (now resolved in v5):**
- **G-10 (state path keying) ✅** — flagged in v3 as 🔴, downgraded to 🟡 in v4, now fully resolved in v5: Data-Flow.md's diagram uses `pipelines/<pipeline-id>/` and all config prefix comments say "use pipeline ID if omitted."

---

## Summary of this revision (carried from v3)

The config and overview updates **closed 11 gaps** and **materially advanced 6 more**. Highlights:

- `globals.state` section added → G-1c resolved
- Immutable `id` field added to pipelines → G-1b resolved
- `globals.api.tls` and `memberlist.tls` sections added → most of G-31 resolved
- `globals.api.user` / `password` added → G-16a resolved
- Env-var substitution syntax (`${VAR}`) documented → G-39 resolved
- `defaults` merge semantics documented in prose → G-1a resolved
- `dump_always` / `dump_initial` modes defined in System Overview → G-7b resolved
- `flush.seconds: 0` redefined to "flush at every transaction boundary" → G-3b resolved
- `flush` all-null = validation error documented → G-3a resolved
- `snapshots.overrides` glob/format rules documented → G-7a resolved
- `parallelism > 4` warning documented → G-21a resolved
- New pipeline lifecycle API endpoints added (pause/stop/resume/restart/flush) → new API surface to specify

The remaining blockers are now concentrated in **correctness mechanics** (split-brain lease, ghost rows, delivery guarantee, type mapping, DDL classification) and **cross-cluster coordination** (config propagation, reload proxy, RBAC) — areas a config schema cannot resolve on its own.

---

## 1. Configuration System

### 1.1 Config data model — PARTIALLY RESOLVED 🔄

**Resolved since v2:**
- **G-1a (merge semantics) ✅** — the draft now states the rule in prose: defaults merge with priority to pipeline values, and a discriminated-union boundary like `catalog` is replaced wholesale (specifying `catalog.rest` when the default is `catalog.glue` replaces the entire subtree). This is exactly the recommendation from v2.
- **G-1b (pipeline ID) ✅** — each pipeline now has an explicit `id` field, documented as the stable identity for state preservation: renaming the dict key without changing `id` is "just renaming," and changing `id` re-creates the pipeline. This resolves the hash-stability blocker.
- **G-1c (globals.state) ✅** — `globals.state` with `bucket` and `prefix` is now present for application-level state.
- **G-1d (typo `clutster`) ✅** — corrected to `cluster`.
- **G-1e (`replication_factor`) ✅** — the field has been removed from the draft, eliminating the ambiguity.
- **G-39 (env-var syntax) ✅** — the `${VARIABLE_NAME}` syntax is now documented, including partial substitution and the rationale (hide credentials, DRY).

**Remaining gaps:**

**1.1f — Config validation rules — RESOLVED ✅ (was G-1f)**
`Configuration_Specification.md` §13 consolidates every constraint into a master validation table (V-001…V-071) spanning structure & mutual exclusion (§13.1), requiredness (§13.2), value domains (§13.3), cross-field/conditional (§13.4), the 9 catalog-auth rules (§13.5), management-API security (§13.6), identity/naming (§13.7), reload-time & cluster (§13.8), and resolution/materialization (§13.9). Each rule carries an ID, severity (error/warning), and locus (config/pipeline/reload). This is the complete spec for the config-validation API endpoint. All the previously-scattered constraints (glue-requires-aws, mutual exclusions, flush all-null, parallelism>4, ssl_mode=allow on MySQL, the catalog rules) now have V-numbers.

**1.1g — Catalog JWT auth parameters — RESOLVED ✅**
Previously the last schema-level blocker. The `Catalog_Auth_Research.md` document and the updated config now define complete, protocol-correct auth schemas for both catalog types:
- **`catalog.hive`** (Thrift/SASL): `auth: none | ldap | kerberos`, with `user`/`password` for ldap, a `kerberos` sub-block (`principal`, `keytab`, `service_name`), and an optional Thrift `tls` block. The inapplicable `jwt` and `aws` values were correctly removed.
- **`catalog.rest`** (HTTP): `auth: none | bearer | oauth2 | sigv4`, with `token` for bearer, an `oauth2` sub-block (`client_id`, `client_secret`, `token_url`, `scope`), a `sigv4` sub-block (`region`, `service`), plus `warehouse`, `prefix`, and `tls.ca`.

**Residual:** none — the 9 catalog validation rules are now in the master table (§13.5, V-040…V-048) and the `defaults.catalog` block carries the correct schema.

---

### 1.2 Config propagation across workers — PARTIALLY RESOLVED 🔄

The **single-worker** side is now fully specified: §12 defines the reload flow (re-read → parse → validate → resolve → diff → reconcile), diff granularity (per-pipeline by `id`, per global field), mutability-class-driven actions (R/PR/PROC), and add/remove/rename/`id`-change handling. The reconcile engine is design-complete for one worker.

**Remaining gap 🔴:** the spec explicitly scopes out **cross-worker propagation** (§12.1: "Cross-worker propagation of the reload … is a design item (G-2 / G-17) and is out of scope for this schema"). How a reload on worker A causes peers B and C to re-read is still undefined. `globals.state` provides the location for a shared config-generation pointer; the gossip-broadcast or poll mechanism and the version-reconciliation tiebreak remain to be designed.

**Research agenda:** Write config generation + hash to `globals.state`. Workers poll the generation pointer (or receive a gossip signal); on mismatch they re-read and run the §11/§12 pipeline. Define precedence when the local mounted file and the cloud generation disagree, with a monotonic generation counter as the tiebreak.

---

### 1.3 Secret management & rotation — RESOLVED ✅

Env-var substitution has a complete grammar (§4.3). The spec adds `credentials_file` / `token_file` (§4.6): a path to a JSON `{user,password}` or `{client_id,client_secret}` pair, or a raw-token file, offered beside every inline-credential site and mutually exclusive with it (V-065/066/070). Because the reconcile diff is computed on **resolved** values (§11.1), replacing a credential file's *contents* and triggering a reload rotates the secret per the field's mutability class (R or PR) — **no process restart**, which env-var secrets cannot offer (they are PROC). Source DB credentials are documented as re-read on reconnect (§9.2). Dedicated secret-manager integration (Vault, cloud secret managers) is not built in, but the `credentials_file` pattern composes with any secret manager that can project a secret to a file (e.g., CSI driver, Vault Agent), which is the idiomatic Kubernetes approach.

---

### 1.4 `globals.state` vs `defaults.state` boundary — RESOLVED ✅

The specification §6.2 sets the boundary as a normative rule: `globals.state` holds **cross-pipeline** data only — config generation pointer, cluster membership cache, token-count anchor, lease registry. **Per-pipeline** offsets/cursors/snapshot status live under the per-pipeline `state` location (§9.6). "Implementations must not write per-pipeline offsets into `globals.state`." The overlap ambiguity is closed.

---

### 1.5 `defaults.catalog` block auth schema — RESOLVED ✅

The pipeline-level `catalog.hive`/`catalog.rest` blocks were upgraded to the protocol-correct auth schemas (G-1g), and the `defaults.catalog` block has now been brought into alignment as well. Both `defaults.catalog.hive` (Thrift `none`/`ldap`/`kerberos` + TLS) and `defaults.catalog.rest` (HTTP `none`/`bearer`/`oauth2`/`sigv4` + warehouse/prefix/TLS) match the pipeline-level form. The DRY pattern of setting catalog auth once in `defaults` now yields a valid configuration, and `auth: basic` is no longer present anywhere in the config.

---

## 2. Pipeline Lifecycle & Flush Semantics

### 2.1 Flush semantics — RESOLVED ✅

- **G-3 ✅** — OR semantics confirmed.
- **G-3a ✅** — all-null is now an explicit validation error; `seconds: null` with other criteria set is valid (time-based flushing disabled) with a warning.
- **G-3b ✅** — `seconds: 0` is redefined to "don't wait and flush at every transaction boundary" (not per-row), eliminating the per-row write-amplification risk. The `< 5s` warning threshold is documented.

This domain's flush configuration is now fully specified at the config level.

---

### 2.2 In-flight RecordBatches during pipeline restart — RESOLVED ✅

Specification §12.4 sets the policy: a restart (PR change, explicit `restart`/`stop`, or `id` change) defaults to **discard-and-replay** — drop unflushed in-memory batches and replay from the last confirmed offset, with the Phase-4 exactly-once guard deduplicating. The replay window is bounded by the checkpoint interval (flush-driven). A graceful-flush option (flush before restart) is noted as a planned flag (tracked under gap 2.2's original research note). The core behavior is decided.

---

### 2.3 Pipeline identity — RESOLVED ✅

Superseded by G-1b, now resolved with the `id` field.

---

### 2.4 Pipeline deletion semantics — PARTIALLY RESOLVED 🔄

The `id` field clarifies what constitutes deletion (an `id` disappearing from config, or its `id` changing). Still undefined:
- Whether deletion is detected via config diff or only via explicit API.
- Who cleans up DB resources (slot, publication) if the owning worker is offline when the pipeline is removed.
- Fate of in-flight snapshots for a deleted pipeline.

**Research agenda (unchanged):** Tombstone deleted pipeline IDs in `globals.state`; any worker discovering an orphaned slot for a tombstoned ID cleans it up. Cancel in-flight snapshots gracefully.

---

### 2.5 Pipeline lifecycle API state machine — RESOLVED (design) ✅

Designed in `Pipeline_Lifecycle_State_Machine.md` (resolves G-NEW-15). Three operator-set **desired**
states (`RUNNING`/`PAUSED`/`STOPPED`) reconciled by the owning `PipelineController`; `desired_state`
persisted per-pipeline so pause/stop survive failover and restart. PAUSE flushes-then-holds (lease
kept); STOP closes the source connection (slot persists); STOPPED keeps its lease idle. `resume`
targets both PAUSED and STOPPED; `restart` only on RUNNING (§12.4). §9.6 closed: manual flush resets
timer+counters; manual restart = discard-and-replay. Config reload does not override `desired_state`.
Original open questions below are retained for the audit trail.

#### (original) Pipeline lifecycle API state machine undefined 🆕 🟡

The System Overview adds `pause`, `stop`, `resume`, `restart`, and `flush` endpoints. The state machine they operate on is not defined:
- What is the difference between `pause` and `stop`? (Presumably pause = stop reading but keep resources; stop = stop and release connection but keep state.)
- Can you `resume` a `stopped` pipeline, or only a `paused` one?
- Does `pause` flush buffered data or hold it in memory?
- What happens to a paused pipeline during a worker failover — does the new owner inherit the paused state?
- Is pause/stop state persisted to `globals.state` (so it survives restarts) or in-memory only?

**Research agenda:** Define a pipeline state machine: `RUNNING → PAUSED → RUNNING`, `RUNNING → STOPPED`, `STOPPED → (restart) → RUNNING`. Specify which transitions flush, which release DB connections, which release leases, and which persist to cloud state so they survive failover and worker restart.

---

## 3. Snapshotting

### 3.1 Snapshot resumability on crash — RESOLVED (design) ✅

Designed in `Snapshot_Resumability_and_Consistency.md` (resolves G-6). The blocking-snapshot model is
replaced by an **incremental chunked snapshot**: identifier-keyed chunks, each a short
`READ COMMITTED` transaction, checkpointed by a **typed identifier-tuple cursor** with a
**binary-collation ordering pin**. Row-value keyset predicate `WHERE (k) > (cursor) ORDER BY k LIMIT
chunk_size`. `snapshot.chunk_size` default 10000; checkpoint per chunk; crash-resume re-reads the
in-flight chunk idempotently (G-13). The "no suitable cursor key" fallback is **moot** — every
replicated table has an orderable identifier (PK-less tables halt).

---

### 3.2 Multi-table snapshot consistency — RESOLVED (design) ✅

Resolved in `Snapshot_Resumability_and_Consistency.md` §3 (resolves G-7) by the incremental model.
Consistency is **lock-free and convergent** instead of a frozen instant: per-chunk **position
watermarks** + G-13 upsert idempotency reconcile snapshot rows against concurrent stream changes.

This **eliminates** the prior concerns rather than configuring them: no global read lock, so the
MySQL **`RELOAD`/`LOCK TABLES`** privilege requirement is gone and there is no global-lock failure
mode to fall back from. **No `consistent_snapshot` toggle** is introduced — incremental is the single
model. Honest limitation: a reader querying mid-snapshot sees a partially-populated table and no
cross-table atomic instant (same per-table boundary as G-14 §5).

---

### 3.3 On-demand snapshot read point — RESOLVED (design) ✅

Resolved in `Snapshot_Resumability_and_Consistency.md` §4 (resolves G-20). The `pause_lsn + 1`
read-point *assumption* disappears: an on-demand snapshot is the same incremental chunk loop run
**without pausing the stream**, with watermarks + upsert idempotency reconciling re-snapshotted rows
against concurrent changes. Re-snapshot-after-unsafe-DDL and ad-hoc snapshots reuse the identical
mechanism. No read point is ever "assumed."

---

### 3.4 Snapshot parallelism — RESOLVED ✅

- **G-21 ✅** — `parallelism` field present.
- **G-21a ✅** — the connection-budget concern is now documented in the draft: "each postgres snapshot job consumes at least 3 connections" and "emit warning if parallelism is greater than 4."

**Residual (folded into 1.1f):** the warning threshold is a validation rule to be captured in the validation spec.

---

### 3.5 Ad-hoc snapshot for tables not in pipeline config — UNCHANGED 🟡

Data-Flow.md confirms the behavior (snapshot written to Iceberg, table not added to streaming set) but the sink/catalog resolution and ad-hoc table ownership/TTL are still not pinned down.

**Research agenda (unchanged):** Pipeline name must resolve to an existing pipeline; sink and catalog inherited; table created if absent; document whether ad-hoc snapshot tables have any lifecycle ownership.

---

### 3.6 Snapshot override semantics — RESOLVED ✅

**G-7a ✅** — the draft now documents: globs forbidden, full table name including schema, any allowed mode permitted at table level, not present in defaults. Behavior for unmatched keys should still be confirmed (recommend: warning at load time, not error) — fold into the validation spec (1.1f).

---

### 3.7 Snapshot mode naming — RESOLVED ✅

**G-7b ✅** — `dump_always` and `dump_initial` are now fully defined in the System Overview:
- `dump_always` — snapshot every start, don't stream.
- `dump_initial` — snapshot once if absent at sink and catalog, don't stream; with detailed handling for the catalog/storage mismatch cases.

**G-NEW-18 ✅** — the System Overview's pipeline-entity summary (line 46) is corrected: it now reads `always, never (default), initial, dump_always, dump_initial`. The orphaned `only` is removed and `dump_*` is added, so the summary line, the detailed snapshotting section, and the config draft all agree.

---

### 3.8 Default snapshot mode — RESOLVED ✅

**G-NEW-16 ✅** — both documents now agree the default is `never`: the config draft sets `defaults.tables.snapshots.mode: never`, and System Overview line 46 now says "never (default)." The safer default (no surprise full-table reads on first run) was chosen.

---

## 4. Clustering and Fault Tolerance

### 4.1 Split-brain / lease mechanism — RESOLVED (design) ✅

Was the single most critical safety gap; now designed in `Clustering_Architecture_Research.md` §5.
The mechanism is a **per-pipeline lease object in `globals.state`** (`leases/<pipeline_id>.json`)
taken and renewed via **conditional writes (compare-and-swap)** on the object's ETag/generation.
The acquire/renew/takeover protocol is **clock-skew-safe**: a challenger judges holder liveness by
whether the object's version changes within a *locally-measured* `lease_duration` interval, never by
comparing clocks across nodes. `dead_node_reclaim_time` is no longer load-bearing for safety — the
**lease TTL** is the real boundary.

Why gossip alone cannot do this (and why the lease is necessary) is argued from CAP in the research
doc §1/§5.1: an AP membership protocol cannot provide linearizable mutual exclusion. The ring
decides who *should* own a pipeline; the lease decides who *may run* it.

**Honest residual (design-acknowledged, not open):** object storage offers **no fencing token**, so
a GC-paused "zombie" holder could attempt one stale commit. Closed by defense-in-depth — pre-commit
lease re-check + **Iceberg's atomic commit as the final fence** (exactly the "lease primary, Iceberg
last line of defense" stance below) + `_lsn` dedup. Iceberg atomic commit is the backstop, not the
mechanism.

**Config landed:** `cluster.lease_duration` (default 15s, PROC, cluster-wide-consistent anchor) and
`cluster.lease_renewal_interval` (default 5s) populate the config spec §16 G-8 reservation. Final
default values are D-6 (research doc §18), being tuned against the replication-lag SLA (OQ-OB2).

**Renewal hardened (D-8, research doc §19 — v10):** the renew rule no longer treats all failures as
loss. A `412` is a definitive loss (stop now); a transient storage error moves the renewer to a
*Degraded* state that retries until a **safety deadline** (`last_renew_local + lease_duration − guard`,
`guard = lease_duration × lease_safety_margin`, default margin `0.15`), then stops — preserving the
clock-skew-safe property and fixing the single-node "stop on a 2-second S3 blip" regression. Adds
`cluster.lease_safety_margin` and promotes `lease_renewal_interval ≤ lease_duration / 3` to an
enforced validation rule (now applied to the config spec as V-072/V-073). Lease-state metrics fold
into G-33.

---

### 4.2 Offset handover staleness — UNCHANGED 🟡

Data-Flow.md's worker-failure path says the new owner resumes "from last confirmed_flush_lsn / gtid_executed" with "worst-case replay = time since last checkpoint write," and the checkpoint is written after each Iceberg commit. This actually resolves the *checkpoint frequency* question (it's flush-driven).

**Resolved 🟡 (G-22):** `Operational_and_Scope_Gaps_Batch.md` adds `source.postgres.max_wal_lag_bytes`
(default null), always emits `cdc_wal_lag_bytes`, and makes the behavior **alert-only by default with
opt-in halt** when the threshold is set and exceeded (protects the source disk). Pairs with the G-15
abandoned-slot lifecycle.

---

### 4.3 Rebalancing handoff protocol — RESOLVED (design) ✅

Designed in `Clustering_Architecture_Research.md` §8. The **unplanned-failure** path (dead node →
lease expiry → new owner takes over and replays from the last checkpoint, at-least-once) and the
**planned-departure** path are now separated:

- **Planned drain (the missing piece):** `phase → Draining` → stop accepting source events → flush +
  Iceberg commit → write final offsets/cursors → **release the lease** (CAS delete/tombstone) →
  broadcast departure. Because the lease is *released*, the new owner acquires **immediately** with
  no `lease_duration` wait — planned handoff is fast and clean; only unplanned death pays the full
  lease wait.
- **Anti-flap / rate-limiting:** `cluster.rebalance_stabilization` (default 15s) delays *proactive*
  rebalancing on joins until membership is stable (dead-node failover is **not** delayed);
  `cluster.max_concurrent_handoffs` (default 2) caps simultaneous drains (Akka "rebalance N at a
  time"); a joiner passes through `Joining` before `Active` so it never grabs-and-drops.
- **Bound:** `cluster.drain_timeout` (default 60s) — exceed it and fall back to the unplanned replay
  path. This extends the existing `Data-Flow.md` shutdown sequence with an explicit lease release.

**Hardened in v11 (research doc §20):** the *unplanned* path now also has a burst cap —
`cluster.max_concurrent_takeovers` (default 4) limits concurrent Phase-0 starts (not the cheap
takeover-evaluation waits) with jitter, so a dead multi-pipeline node doesn't stampede source-DB
reconnects (D-10). Rebalancing is also flap-immune: **suspect** nodes stay in the ring (only
`dead`/`left` removes a node), and `rebalance_stabilization` now applies to **all ring additions
including rejoins** while removals act immediately (D-11). The execution model is a `ClusterReconciler`
(ring only) + per-pipeline `PipelineController` (sole lease-writer), so a long takeover-evaluation
wait never blocks other pipelines' reconciliation (D-9).

---

### 4.4 Single-node degraded mode — RESOLVED (design) ✅

Specified in `Clustering_Architecture_Research.md` §10. Empty `join_members` with no discovered
peers = **single-node mode on the identical code path**: the ring contains only self's vnodes (self
owns everything), gossip runs but is idle (no peers to probe), and **the lease stays active** — its
cost is one periodic conditional PUT per pipeline, and it guards the real k8s hazard of two
instances briefly coexisting against the same `globals.state` bucket during a rolling deploy. There
is no `cluster.enabled` toggle; single vs multi is purely a function of whether peers exist.
Scale-out is seamless: a second pod joins (via headless-Service DNS discovery, D-4), membership
grows, the ring recomputes, and some pipelines move via the normal graceful-handoff protocol (4.3).
The N=1 ↔ N>1 symmetry is the whole point — nothing special happens at the boundary.

---

### 4.5 Virtual node count — RESOLVED ✅

`tokens: 128` (changed from 512 in the prior draft). 128 is close to the common 150 default and more reasonable than 512.

**Cluster-wide consistency is now established (config spec V-026/V-062 + G-36) and explained:**
`Clustering_Architecture_Research.md` §3.1/§3.3 makes clear *why* this is a safety invariant, not a
tuning knob. Because the ring is **derived** from membership (each node positions its vnodes by
`hash(node_id ‖ vnode_index)`), every node computes a byte-identical ring **only if** every node
uses the same `tokens`. A mismatch produces divergent rings → permanent disagreement on ownership →
split-brain. The first node writes the established value to the `globals.state` cluster anchor; a
node whose `tokens` differ refuses to start. The research doc extends this anchor to also pin
`lease_duration` (same reasoning — see 4.1 / §7.3).

---

## 5. State and Durability

### 5.1 State file layout — RESOLVED ✅

Data-Flow.md provides the pipeline-level state hierarchy; `Clustering_Architecture_Research.md` §12
now details the **`globals.state` object set** (closing the remaining 🔴):

```
<globals.state.prefix>/
├── cluster/
│   ├── anchor.json          # first-writer-wins (If-None-Match:*); pins tokens + lease_duration + hash_algo;
│   │                        # a node whose config disagrees refuses to start (extends V-026/V-062)
│   └── membership.json      # advisory last-known membership snapshot (gossip is authoritative)
├── config/
│   ├── generation.json      # CAS pointer { version, generation, hash, written_by, written_at }
│   └── snapshots/<NNNNNN>.yaml   # immutable resolved config per generation
└── leases/
    └── <pipeline_id>.json   # per-pipeline lease (CAS via ETag/generation)
```

Every state file (global **and** per-pipeline) carries a top-level **`version` integer** for forward
compatibility — the second half of this gap. The §6.2 boundary rule is respected: only cross-pipeline
data lives here; per-pipeline offsets/cursors stay under the pipeline `state`/`sink` location.

**Resolved earlier:** Data-Flow.md's state-layout diagram keys on `pipelines/<pipeline-id>/` (G-10 closed).

---

### 5.2 Atomic state updates on S3/GCS — RESOLVED (design) ✅

The premise (S3 has no conditional overwrite) is now **outdated**: S3 added create-if-absent
(`If-None-Match: *`, Aug 2024) and ETag compare-and-swap (`If-Match: <etag>`, Nov 2024); GCS
(generation-match) and Azure (ETag/If-Match) have had CAS for years.
`Clustering_Architecture_Research.md` §5.2/§12 builds the lease and the `config/generation.json`
pointer on exactly this primitive (failed precondition → 412).

**Two-pronged answer to multi-writer races:** (1) the **lease guarantees single-writer** for
per-pipeline offset/cursor files, so those files need no CAS in the common path; (2) the
**cross-pipeline** files (`generation.json`, `anchor.json`, `leases/*`) use **CAS directly** since
multiple workers legitimately contend for them.

**Caveat carried into D-5 (research doc §17):** the three backends are *not* uniform. S3 and Azure are
ETag-based; **GCS uses *generation* preconditions and its ETag is not usable as a write precondition**.
The implementation must abstract the lease behind a `LeaseStore` trait with an opaque per-backend
version token (S3/Azure ETag, GCS generation) and ship a per-backend conformance test. `opendal`
exposes `write_with().if_none_match("*")` / `.if_match(etag)` (errors as `ErrorKind::ConditionNotMatch`)
but gates them behind per-backend `Capability` flags — so the GCS mapping must be verified, not assumed.
The verification harness is now scaffolded at **`prototypes/lease-conformance/`** (`LeaseStore` trait +
T1–T10); D-5/G-11 close once it passes against real S3, GCS and Azure (T10 = the GCS trap).

---

### 5.3 Config state storage — RESOLVED ✅

`globals.state` now exists and is documented as the application-level state location. The remaining clarity question (what goes in global vs pipeline state) is captured in 1.4.

---

## 6. Apache Iceberg Write Semantics

### 6.1 COW vs MOR — RESOLVED ✅ (unchanged)

`sink.strategy: mor` default. Note 6.6 (migration on strategy change) remains.

### 6.2 Ghost rows / deduplication — RESOLVED (design) ✅

Designed in `Ghost_Row_Dedup_and_Primary_Keys.md` (resolves G-13; resolves G-30 by rejection). The
core defense is now fully specified:

- **Identifier** = inferred source PK (channel-C catalog query), overridable via `tables.primary_key`,
  materialized as Iceberg `identifier-field-ids`. Constraints: top-level, primitive, NOT NULL,
  non-floating.
- **Intra-batch dedup** collapses each flush to ≤1 equality-delete + ≤1 data record per identifier
  tuple, **keep-last by a per-change `_seq`** (PG change WAL LSN; MySQL binlog file+pos / GTID+event
  index) — the `_lsn` *commit* column is too coarse.
- **Delete = mirror semantics** — equality-delete only, no `D` data row; the §0 strictly-less-than
  rule is what makes in-batch "delete-old + insert-new" correct and what the mirror rule avoids
  ghost-ing.
- **Before-image contract** — identifier ⊆ source replica-identity columns (new validation, halt
  otherwise); MySQL FULL image already required.
- **PK-less tables halt** (line 50 kept); `tables.surrogate_key` removed from config §16.

**Residual (implementation gate, §14):** verify iceberg-rust 0.9.0's equality-delete writer assigns
delete files the commit sequence number — the §0 guarantee depends on it.

### 6.3 Delivery guarantee — RESOLVED (design) ✅

Designed in `Delivery_Guarantee_and_Position_Guard.md` (resolves G-14). At-least-once delivery with
**per-table** exactly-once-*effect*. The previously-assumed `committed_lsn ≥ flush_lsn` test was only
valid for PG; it is replaced by:

- a **`SourcePosition`** abstraction — PG numeric LSN vs MySQL GTID-set (**subset**/union/intersection,
  canonical serialization);
- a **per-table** committed high-water mark (`cdc.committed_position` in each table's snapshot summary)
  with the **pipeline resume checkpoint = min/intersection across tables** (the laggard), since
  multi-table flushes are separate, non-atomic Iceberg commits;
- the crucial reframing that **G-13 PK-upsert idempotency is the correctness mechanism**, so the
  position guard is an optimization + checkpoint-advance, not the exactly-once boundary — which
  dissolves the replay batch-re-alignment hazard.

**Known limitation (documented):** cross-table *atomic visibility* is not guaranteed (separate
commits). Cross-ref G-29 / G-25.

### 6.4 Iceberg partitioning — RESOLVED (design) ✅
Resolved in `Iceberg_Partitioning_and_Layout_Evolution.md` (resolves G-24). `tables.partitioning` list
of transform fields; default unpartitioned; `_ts` allowed. Partition-key mutation handled via the
before-image (delete old partition + insert new) so equality deletes don't orphan rows across
partitions. Partition-spec changes evolve (new data only, no rewrite).

### 6.5 Iceberg maintenance — RESOLVED (design) ✅
Resolved in `Iceberg_Maintenance.md` (resolves G-25). v1 = **external maintenance** + app-exposed
when-needed signals (delete-file count, small-file ratio, snapshot count, manifest depth); `sink.maintenance`
reserved for a future built-in scheduler, gated on iceberg-rust rewrite support.

### 6.6 Strategy change does not migrate existing tables — RESOLVED (design) ✅
Resolved in `Iceberg_Partitioning_and_Layout_Evolution.md` §4 (resolves G-12a). `sink.strategy` change
applies to **new data only** (Iceberg tolerates mixed COW/MOR); migration is an explicit operator
action — compaction (fold MOR deletes) or re-snapshot into a fresh table (G-6). Mutability PR.

---

## 7. Source Database Connectors

### 7.1 PostgreSQL replication slot lifecycle — RESOLVED (design) ✅

Designed in `Postgres_Slot_and_Publication_Lifecycle.md` (resolves G-15). The static side was already
done in the config spec (V-058/059 uniqueness, `auto_cleanup`); the runtime **state machine** is now
specified:

- **Existence matrix** {state present/absent} × {slot present/absent}: reconnect-&-resume (normal
  restart); first-start create+snapshot; slot-lost → recreate + re-snapshot; orphan (state-absent +
  slot-present) → policy.
- **Orphan policy** `slot.on_orphan: halt (default) | adopt_resnapshot`; resume-without-snapshot
  rejected.
- **Creation** `slot_management: managed (default) | require_existing` (DBA-precreated; forces
  `auto_cleanup=false`).
- **WAL-gone** → recreate + incremental re-snapshot (G-6); warn/metric.
- **Publication** app-maintained explicit list; `ALTER PUBLICATION` reconciles on table-set change.
- **Mutual exclusion** via the lease + PG active-slot lock; external same-name collisions documented
  as undetectable.

### 7.2 MySQL GTID vs binlog — RESOLVED (design) ✅
`replication_type: binlog | gtid` present. Binlog-purge handling resolved in
`Operational_and_Scope_Gaps_Batch.md` (resolves G-26): `source.mysql.binlog_purge_action: error
(default) | re_snapshot`; GTID documented as preferred. Recovery via incremental re-snapshot (G-6).

### 7.3 MariaDB support — RESOLVED (decision) ✅
Resolved in `Operational_and_Scope_Gaps_Batch.md` (resolves G-38): **MariaDB is in scope via the
MySQL-compatible path** for v1. The one genuine correctness dependency — MariaDB GTID is
`domain-server-sequence`, not MySQL `uuid:tx` — extends the G-14 `SourcePosition` MySQL impl (both
dialects); flagged as a §14 sub-research item, plus channel-C/DDL-classifier validation against MariaDB.

### 7.4 Connection pool management — PARTIALLY RESOLVED 🔄
Snapshot connection budget is now documented inline ("≥ 3 connections per postgres snapshot job"). Reconnect backoff strategy still not in config.
**Research agenda:** Define reconnect backoff (initial 1s, max 60s, jitter); expose as config or document as fixed.

### 7.5 `heartbeat` — RESOLVED ✅
Now documented inline: "interval to send StandbyStatusUpdate messages to keep the connection alive and to prevent WAL slot lag from being interpreted as inactivity."

### 7.6 `snapshot` timeout scope — RESOLVED ✅
Now annotated "per-table snapshot timeout" in both defaults and pipeline sections.

---

## 8. Clustering — Gossip Protocol

### 8.1 SWIM vs Scuttlebutt — DECIDED ✅
`Clustering_Architecture_Research.md` §3.6 records the decision: **keep SWIM via `memberlist`, enable
Lifeguard, and borrow the good ideas from the Scuttlebutt camp without switching libraries.** The two
reasons people prefer Scuttlebutt are neutralized: (i) anti-entropy of arbitrary KV state is moot
because we put **no** mutable KV in gossip (the ring is *derived*; durable truth is in object
storage); (ii) Phi-accrual's "dead or slow?" scoring is addressed by **Lifeguard** and, more
fundamentally, by making **SWIM accuracy a latency concern, not a safety concern** (§8.4 of the
research doc — a false "dead" verdict only triggers a lease-takeover *attempt* that the lease
rejects). The generation-tagged-identity idea is adopted regardless (per-node `incarnation`, §3.4).
**Config:** `cluster.memberlist.lifeguard_enabled` (default `true`).

### 8.2 Virtual node count — see 4.5 (RESOLVED; cluster-wide token consistency is a safety invariant, anchored in `globals.state`).

---

## 9. Management REST API

### 9.1 API versioning — RESOLVED (design) ✅
Resolved in `API_RBAC_and_Versioning.md` §2 (resolves G-9.1): functional/authenticated endpoints under
**`/v1`**; `/health/live`, `/health/ready`, `/metrics` unversioned and unauthenticated.

### 9.2 RBAC role matrix — RESOLVED (design) ✅
Resolved in `API_RBAC_and_Versioning.md` (resolves G-16). Linear hierarchy `admin ⊇ operator ⊇
viewer`; full endpoint→min-role matrix (§2): viewer = status, operator = pipeline lifecycle + config
reload/validate, admin = leave/shutdown. Binding per auth mode (none = open; basic = admin; jwt/oauth2
= mapped `roles_claim`, max wins, 401 vs 403). Configurable `globals.api.role_mapping` for IdP role
names. V-052 TLS-without-auth stays a warning.

### 9.3 Config reload proxy behavior — RESOLVED (design) ✅
`Clustering_Architecture_Research.md` §9.2 resolves this: **no proxy and no per-pipeline routing of
the reload.** Worker A re-reads/validates/resolves, writes an immutable `config/snapshots/<N>.yaml`
and CAS-advances `config/generation.json`, then bumps its gossiped `config_generation`. Every other
worker observes the new target generation (via the gossiped field or a short poll of the pointer),
fetches the snapshot, and **reconciles its own owned pipelines** independently. Because every node
already holds the full config (it must, to compute the ring and be ready to take over any pipeline),
there is nothing to forward to "the owner of B." Worker A returns **`202 Accepted`**; `GET
/v1/config/status` reports per-node applied generation, and the reload is complete when all live
nodes report `N`. Cold-start precedence (cloud-wins-at-runtime, file-is-input) is D-3 (§9.3 of the
research doc) — the safe choice under k8s ConfigMap propagation lag.

### 9.4 Snapshot job API atomicity — UNCHANGED 🟡
Partial-failure behavior (some tables valid, some not) undefined.
**Research agenda:** Validate atomically; reject the whole request with 422 listing failures; no partial execution.

### 9.5 Basic auth credentials — RESOLVED ✅
`globals.api.user` and `password` (env-var substitutable) are now present.

### 9.6 `flush` and `restart` endpoint semantics — RESOLVED (design) ✅
Resolved in `Pipeline_Lifecycle_State_Machine.md` §3: a manual `flush` forces an immediate flush and
**resets the time-based flush timer + row/byte counters** (no-op warning if empty); a manual `restart`
follows the same §12.4 discard-and-replay policy as a config-driven restart.

---

## 10. Observability

### 10.1 Distributed tracing — RESOLVED (design) ✅
Resolved in `Observability_Metrics_and_Tracing.md` §3 (resolves G-32). `globals.tracing.otlp_endpoint`
(disabled when absent) + `sample_ratio`; batch/commit-granularity spans, not per-source-event.

### 10.2 Metrics catalogue — RESOLVED (design) ✅
Resolved in `Observability_Metrics_and_Tracing.md` §2 (resolves G-33). Mandatory catalogue across
replication, source, snapshot, Iceberg+maintenance, clustering, status; **pipeline-level labels by
default, per-table opt-in** (`sink.metrics.per_table`) to bound cardinality.

### 10.3 Log/trace correlation — RESOLVED (design) ✅
Resolved in `Observability_Metrics_and_Tracing.md` §4: inject `trace_id`/`span_id`/`pipeline` into the
JSON logs (fields absent when tracing disabled).

---

## 11. Data Type Mapping

### 11.1 Type mapping table — RESOLVED ✅

`Type_Mapping.md` delivers the full three-hop mapping (DB wire → `ColumnValue` → Arrow → Iceberg) for both engines: complete PostgreSQL and MySQL type tables (core, date/time, text-serialized, structured, binary), the unsigned-integer widening ladder (with `BIGINT UNSIGNED` → `decimal(20,0)`), the Hop-3 Arrow→Iceberg annotation table, and an unsupported-type fallback chain (override → text → bytes → halt, all logged). It identifies the four required `ColumnValue` enum extensions (tz-awareness flag, array element type, `Time`, `UInt64`), each with a correctness rationale.

**Residual (tracked separately):** the four new config fields it introduces (`tinyint1_*`, `toast_strategy`, `column_type_overrides`, `numeric_overflow_action`) and their validation rules need merging into the config spec §13 (G-NEW-21), and the enum extensions need reflecting in Data-Flow.md (G-NEW-22). Both are documentation reconciliation, not design gaps.

### 11.2 DDL changes during live replication — RESOLVED ✅

`Schema_Change_Detection_and_Evolution.md` supersedes the earlier `method: query|events` framing with a four-channel detection model (startup reconciliation, in-band trigger, catalog augmentation, idle rescan) feeding one evolution path. It resolves:
- **G-19 ✅** — the §7.2 Safe/Unsafe/Ignorable classification table, tied to Iceberg's actual safe-operation set, is the ratified contract.
- **G-19b ✅** — disabled/unsafe → halt the changed table only, continue others, reaffirmed (§8.1).
- **OQ-DT2 ✅** — the §8 apply protocol (seal → flush → classify → evolve → rebuild → resume at a transaction boundary, with an idempotent replay guard).
- **OQ-DT3 ✅** — supported evolution operations = Iceberg safe-set + classification (§7.1–7.2).
- **G-19a 🔽 reduced** — because channel C augments from the source catalog, the MySQL DDL parser only classifies DDL and extracts the table name (plus optional rename keyword), not the full ALTER delta. Still required (`sqlparser-rs`), but materially de-risked; this reverses the v7 elevation.

The §0 correctness invariant (catalog query supplies attributes of the implicated change, applied in stream order, scoped to that change, never fast-forwarding to "now") correctly prevents the silent-data-loss hazard. Rename is positively provable (PG `attnum` / MySQL keyword), degrading to drop+add-with-warning only when unprovable.

**Config impact (already applied):** `schema_evolution.method` removed; `interval` → `rescan_interval` (default 60m, always applies); V-024/V-031/V-032 deleted; new source-SELECT precondition added (replication user must hold SELECT on information_schema — required for all engines, since channels A/C/D query the catalog). Notably this removes the v7 "query-method lag window" concern for active tables entirely: channel B fires at the exact stream position, so there is no stale-schema decode window for any table receiving DML; idle tables are covered by channel D.

**Remaining open (kept by the schema doc §11):** DT-R2/R5/R6/R7 — minor recommendations (rename-when-unprovable policy, ENUM/SET representation coordination with G-18, `binlog_row_metadata` required-vs-recommended, `rescan_interval` default). All have recommendations; none are blockers.

### 11.3 `glue.strip_schema` collisions — RESOLVED (design) ✅
Resolved in `Operational_and_Scope_Gaps_Batch.md` (resolves G-11.3): collision under `strip_schema:
true` = config-time validation error naming the colliding tables; non-stripped transform = `schema__table`.

---

## 12. Security and Network

### 12.1 TLS for management API and gossip — RESOLVED ✅ (was G-31, partially)
Both `globals.api.tls` and `globals.cluster.memberlist.tls` (cert/key/ca, env-var substitutable) are now present, and the `cat:` → `ca:` typo on the memberlist CA field is fixed. The `auth: jwt | oauth2` without TLS = error rule should be added to the validation spec (1.1f).

**Research agenda:** Add the "non-anonymous auth requires TLS" validation rule. Confirm whether memberlist TLS implies mTLS (client cert verification) or server-only TLS.

### 12.2 Source DB SSL/TLS — RESOLVED ✅ (unchanged)
`ssl-mode` with full value set defined.

### 12.3 Cloud storage credential scope — PARTIALLY RESOLVED 🔄
The draft now states "no explicit credentials config, the cloud SDK's default credential chain is used."
**Resolved (G-37):** `Operational_and_Scope_Gaps_Batch.md` documents the minimum IAM permissions per
provider (S3 Get/Put/Delete/List/Head; GCS objects create/get/delete/list; Azure Storage Blob Data
Contributor) — CAS preconditions need no extra IAM action (request-header level).

---

## 13. Operational Edge Cases

### 13.1 Back-pressure — RESOLVED (design) ✅
Resolved in `Operational_and_Scope_Gaps_Batch.md` (resolves G-27): all inter-task channels are bounded;
capacity exposed as `sink.buffer_capacity_rows` (default ~200000); sits below the G-29 large-txn byte
backstop. Sink/catalog failure drives a DEGRADED state that halts source reading via back-pressure.

### 13.2 Source DB unavailability — PARTIALLY RESOLVED 🔄
Data-Flow.md defines reconnect with backoff (initial 1s, max 60s, jitter) and "in-progress snapshots paused, cursor preserved, resume after reconnect." This largely resolves the behavior. Backoff is documented in the data-flow doc but not exposed as config — acceptable if fixed.
**Status:** Effectively resolved at the behavior level; only config-exposure is optional.

### 13.3 Cloud storage unavailability — RESOLVED (design) ✅
Resolved in `Operational_and_Scope_Gaps_Batch.md` (G-27 §): persistent sink failure → DEGRADED (retry
with backoff, stop consuming source once buffers fill via back-pressure, protecting the PG slot/WAL via
the G-22 guard); recovers on next successful commit.

### 13.4 Iceberg catalog unavailability — RESOLVED (design) ✅
Resolved in `Operational_and_Scope_Gaps_Batch.md` (resolves G-35): catalog-specific bounded retry +
backoff + circuit breaker → DEGRADED (pause + alert), separate from object-storage retry. Optional
`catalog.retry` config; else fixed defaults (5 attempts, 1s→30s).

### 13.5 Large transactions — RESOLVED (design) ✅
Resolved in `Large_Transaction_Handling.md` (resolves G-29). **Bounded split commits** above
`sink.max_transaction_batch_rows`, with the resume checkpoint **pinned at the txn start** until commit
so a crash replays the whole txn (idempotent re-write, G-13). Default `null` = atomic visibility
preserved; `sink.max_transaction_buffer_bytes` **halts** an unsplittable runaway rather than OOMing the
worker. Atomicity is relaxed only for oversized txns, documented; spill-to-disk deferred.

### 13.6 Tables without primary keys — UNCHANGED 🟡
System Overview line 50 states tables without a PK have replication halted and PK-less tables found via glob are skipped with a warning — this is good and resolves the *detection* and *default behavior*. Still missing: the `surrogate_key` config option to allow PK-less tables to be replicated with a user-designated identity.
**Research agenda:** Add `tables.surrogate_key` override for tables lacking a natural PK.

### 13.7 Azure Blob Storage — RESOLVED (design) ✅
Resolved in `Operational_and_Scope_Gaps_Batch.md` (resolves G-34): default SDK credential chain
(managed/workload identity), consistent with S3/GCS; optional `globals.azure.account_name` for
disambiguation; container = bucket. No explicit secret fields.

---

## 14. Missing Research Topics (unchanged + 1)

| Topic | Why It's Needed | Recommended Source |
|---|---|---|
| OpenDAL integration with iceberg-rust | Storage extension not indexed | OpenDAL docs + iceberg-rust source |
| Iceberg REST Catalog spec | Needed for catalog auth + commit design | Apache Iceberg REST Catalog OpenAPI spec |
| Parquet file format tuning | Row group size, compression affect cost/perf | Apache Parquet documentation |
| Arrow IPC / RecordBatch memory mgmt | Bounded channel capacity, buffer lifecycle | arrow-ipc crate docs |
| Iceberg equality delete file format | Equality deletes recommended for CDC | Iceberg spec §3.3 + iceberg-rust writer API |
| **iceberg-rust 0.9.0 equality-delete writer sequencing** (G-13 §8 gate) | The §0 strictly-less-than guarantee requires delete files to carry the enclosing commit's sequence number; if the crate mis-sequences or lacks equality deletes, the sink writer needs a workaround | iceberg-rust 0.9.0 writer API + source #43 |
| Kubernetes operator patterns | SIGTERM drain, ConfigMap hot-reload, probes | K8s operator SDK patterns |
| Rate limiting / throttling on source DB | High-throughput snapshot can overwhelm source | PG pg_stat_replication + MySQL throttling |
| Iceberg commit.retry / conflict resolution | Concurrent commit handling before writer design | iceberg-rust commit API source |
| `sqlparser-rs` for MySQL DDL parsing | Required for `schema_evolution.method: events` on MySQL | sqlparser-rs crate docs |
| **MariaDB GTID dialect in `SourcePosition`** (G-38) | MariaDB GTID is `domain-server-sequence`, not MySQL `uuid:tx`; the G-14 position abstraction must parse/compare both; channel-C + DDL classifier validated against MariaDB | MariaDB GTID docs + `mysql_async`/binlog crate |
| **Iceberg maintenance/compaction API in iceberg-rust** (G-25) | Gates any built-in maintenance scheduler; v1 delegates to external tooling until this lands | iceberg-rust rewrite/compaction API source |

---

## 15. Updated Summary Priority Matrix

| ID | Domain | Gap | Severity | Status |
|---|---|---|---|---|
| G-1a | Config | `defaults` merge semantics | — | ✅ Resolved (§8 fully normative incl. list-replace + union) |
| G-1b | Config | Pipeline ID vs YAML key | — | ✅ Resolved (`id` field added) |
| G-1c | Config | Missing `globals.state` | — | ✅ Resolved (section added) |
| G-1d | Config | Typo `clutster` | — | ✅ Resolved |
| G-1e | Config | `replication_factor` undefined | — | ✅ Resolved (field removed) |
| G-1f | Config | Validation rules not enumerated | — | ✅ Resolved (spec §13: V-001…V-071) |
| G-1g | Config | Catalog JWT/OAuth2 auth fields | — | ✅ Resolved (full hive/rest auth schemas + research doc) |
| G-2 | Config | Config propagation across workers | 🔴 Blocker | ✅ Resolved (design) — generation pointer in `globals.state` + gossiped `config_generation`; every node reconciles its own pipelines (Clustering doc §9) |
| G-3 / 3a / 3b | Pipeline | Flush semantics (OR / null / seconds:0) | — | ✅ All resolved |
| G-5 | Pipeline | In-flight batches during restart | — | ✅ Resolved (spec §12.4: discard-and-replay default) |
| G-NEW-15 | Pipeline | Lifecycle API state machine (pause/stop/resume) | 🟡 High | ✅ Resolved (design) — persisted desired-state RUNNING/PAUSED/STOPPED reconciled by owner (`Pipeline_Lifecycle…md`); §9.6 closed |
| G-6 | Snapshot | Resumability cursor format | 🔴 Blocker | ✅ Resolved (design) — incremental chunked snapshot, typed identifier-tuple cursor + binary-collation pin (`Snapshot_Resumability…md`) |
| G-7 | Snapshot | Cross-table consistency | 🔴 Blocker | ✅ Resolved (design) — lock-free convergent (watermark + upsert); no `RELOAD`/`LOCK TABLES`, no toggle (`Snapshot_Resumability…md` §3) |
| G-7a | Snapshot | Override semantics | — | ✅ Resolved |
| G-7b | Snapshot | `dump_*` mode definitions | — | ✅ Resolved (defined in System Overview) |
| G-NEW-16 | Snapshot | Default mode disagreement (`never` vs `initial`) | — | ✅ Resolved (both say `never`) |
| G-8 | Cluster | Split-brain / lease | 🔴 Blocker | ✅ Resolved (design) — per-pipeline CAS lease in `globals.state`, clock-skew-safe protocol, Iceberg-commit fence (Clustering doc §5); defaults = D-6 |
| G-9 | Cluster | Rebalancing handoff (planned drain) | 🔴 Blocker | ✅ Resolved (design) — drain→flush→commit→checkpoint→release-lease; rate-limited; planned vs unplanned paths split (Clustering doc §8) |
| G-9a | Cluster | Single-node mode behavior | 🟡 High | 🔄 Partially (intent stated, behavior not) |
| G-10 | State | State layout keyed on name not id | — | ✅ Resolved (Data-Flow + config now use `id`) |
| G-11 | State | Atomic updates on S3 | 🔴 Blocker | ✅ Resolved (design) — same as D-5; CAS primitive on all 3 backends (§5.2); empirical conformance gate = `prototypes/lease-conformance` T1–T10 |
| G-12 | Iceberg | COW vs MOR | — | ✅ Resolved |
| G-12a | Iceberg | Strategy change migration | 🟡 High | ✅ Resolved (design) — new-data-only + explicit compaction/re-snapshot migration (`Iceberg_Partitioning…md` §4) |
| G-13 | Iceberg | Ghost row / `primary_key` dedup | 🔴 Blocker | ✅ Resolved (design) — equality-delete keying + per-`_seq` keep-last dedup + mirror deletes (`Ghost_Row_Dedup…md`); residual = iceberg-rust writer verification (§14) |
| G-14 | Iceberg | Delivery guarantee | 🔴 Blocker | ✅ Resolved (design) — `SourcePosition` (LSN numeric / GTID subset), per-table HWM + laggard checkpoint, idempotency-is-correctness (`Delivery_Guarantee…md`) |
| G-15 | Postgres | Slot lifecycle | 🔴 Blocker | ✅ Resolved (design) — runtime state machine: orphan policy (`on_orphan`), `slot_management`, WAL-gone re-snapshot, publication reconcile (`Postgres_Slot…md`) |
| G-16 | API | RBAC role matrix | 🔴 Blocker | ✅ Resolved (design) — `admin⊇operator⊇viewer`, full endpoint matrix, per-mode binding, configurable IdP `role_mapping` (`API_RBAC_and_Versioning.md`) |
| G-9.1 | API | API versioning | 🟡 High | ✅ Resolved (design) — `/v1` for functional endpoints; health/metrics unversioned+unauth (`API_RBAC…md` §2) |
| G-16a | API | Basic auth + JWT/OAuth2 verification fields | — | ✅ Resolved (basic creds + §6.3.1 jwt/oauth2 schema) |
| G-17 | API | Config reload proxy | 🔴 Blocker | ✅ Resolved (design) — no proxy; 202 + `GET /v1/config/status` poll; every node reconciles its own owned pipelines (Clustering doc §9.2) |
| G-18 | Data Types | Type mapping table | — | ✅ Resolved (`Type_Mapping.md` — full 3-hop PG+MySQL→ColumnValue→Arrow→Iceberg) |
| G-19 | Data Types | DDL classification | — | ✅ Resolved (`Schema_Change…md` §7.2 classification table; four-channel detection) |
| G-19a | Data Types | MySQL DDL parser | 🟡 High | ✅ Resolved (design) — two-tier fail-soft classifier (sqlparser-rs → regex → channel C); router not delta-parser (`MySQL_DDL_Classifier.md`) |
| G-19b | Data Types | Behavior when evolution disabled | — | ✅ Resolved (halt table, continue others) |
| G-20 | Snapshot | On-demand resume read point | 🟡 High | ✅ Resolved (design) — on-demand = incremental chunk loop, no stream pause, no read-point assumption (`Snapshot_Resumability…md` §4) |
| G-21 / 21a | Snapshot | Parallelism + connection budget | — | ✅ Resolved |
| G-22 | Cluster | Checkpoint staleness / WAL lag | 🟡 High | ✅ Resolved (design) — `max_wal_lag_bytes` (alert-only default, opt-in halt) + `cdc_wal_lag_bytes` (`Operational…Batch.md`) |
| G-23 | Cluster | SWIM Lifeguard / Scuttlebutt decision | 🟡 High | ✅ Decided — keep SWIM + Lifeguard; no mutable KV in gossip; SWIM accuracy is latency-not-safety (Clustering doc §3.6) |
| G-24 | Iceberg | Partitioning strategy | 🟡 High | ✅ Resolved (design) — `tables.partitioning` transforms; partition-key mutation via before-image; spec-evolution (`Iceberg_Partitioning…md`) |
| G-25 | Iceberg | Maintenance responsibilities | 🟡 High | ✅ Resolved (design) — external maintenance v1 + when-needed signals + reserved `sink.maintenance` (`Iceberg_Maintenance.md`) |
| G-26 | MySQL | Binlog purge handling | 🟡 High | ✅ Resolved (design) — `binlog_purge_action: error (default) | re_snapshot` (`Operational…Batch.md`) |
| G-27 | Ops | Back-pressure buffer capacity config | 🟡 High | ✅ Resolved (design) — bounded channels + `sink.buffer_capacity_rows` + DEGRADED back-pressure (`Operational…Batch.md`) |
| G-28 | Ops | Source DB unavailability | 🟡 High | ✅ Resolved (behavior in Data-Flow) |
| G-29 | Ops | Large transaction splitting | 🟡 High | ✅ Resolved (design) — bounded split commits + checkpoint-pinning + memory backstop halt (`Large_Transaction_Handling.md`) |
| G-30 | Ops | Tables without PK / surrogate key | 🟡 High | ✅ Resolved by rejection — PK-less tables **halt** (line 50 kept); `surrogate_key` removed from config §16, no append-only mode (`Ghost_Row_Dedup…md` §3) |
| G-31 | Security | TLS for API + gossip | — | ✅ Resolved (tls sections added; `cat:` typo fixed) |
| G-32 | Observability | Distributed tracing | 🟡 High | ✅ Resolved (design) — `globals.tracing.otlp_endpoint`, commit-granularity spans (`Observability_Metrics…md`) |
| G-33 | Observability | Metrics catalogue | 🟡 High | ✅ Resolved (design) — mandatory catalogue + pipeline-default/per-table-opt-in cardinality (`Observability_Metrics…md`) |
| G-34 | Ops | Azure-specific config fields | 🟡 High | ✅ Resolved (design) — default credential chain + optional `azure.account_name` (`Operational…Batch.md`) |
| G-35 | Ops | Catalog unavailability | 🟢 Medium | ✅ Resolved (design) — catalog-specific retry + circuit breaker + DEGRADED (`Operational…Batch.md`) |
| G-36 | Cluster | Token count consistency rule | — | ✅ Resolved (spec V-026/V-062: cluster-wide anchor in globals.state, mismatch rejected) |
| G-37 | Security | IAM permission documentation | 🟢 Medium | ✅ Resolved (design) — per-provider minimum IAM permissions documented (`Operational…Batch.md`) |
| G-38 | MySQL | MariaDB in-scope decision | 🟢 Medium | ✅ Resolved — **in scope** (MySQL path); MariaDB GTID-dialect handling in `SourcePosition` flagged as §14 sub-item (`Operational…Batch.md`) |
| G-39 | Config | Env-var substitution syntax | — | ✅ Resolved (full grammar, spec §4.3) |
| G-NEW-20 | State | Data-Flow `pipelines/<id>/` segment redundant under spec §10 | 🟢 Medium | ✅ Resolved — Data-Flow.md state diagram re-rooted (v13.13) |
| G-NEW-21 | Config | Type_Mapping config fields + rules not yet in spec §13 | 🟡 High | ✅ Resolved — 4 fields + V-077…V-082 merged into Configuration_Specification.md (v13.13) |
| G-NEW-22 | Data Flow | `ColumnValue` enum extensions not reflected in Data-Flow.md | 🟢 Medium | ✅ Resolved — enum extended (`UInt64`, `with_tz`, `Time`, typed `Array`) + `ColumnValueType` (v13.13) |
| G-NEW-17 | Postgres | `auto_cleanup` referenced but missing from config | — | ✅ Resolved (field added) |
| G-NEW-18 | Snapshot | Line-46 summary still lists orphaned `only` | — | ✅ Resolved (line 46 corrected) |
| G-NEW-19 | Config | `defaults.catalog` still uses old `basic`/`user`/`password` schema | — | ✅ Resolved (defaults block aligned) |

**Tally vs v2:** 11 gaps fully resolved · 6 materially advanced to partial · 4 new gaps surfaced.

**v4 delta:** 4 of the 4 new gaps surfaced in v3 are now resolved (G-NEW-16, -17, -18, and the `cat:` typo). G-10 downgraded 🔴 → 🟡 (intent unambiguous, documentation alignment remaining).

**v5 delta:** G-1g resolved (catalog auth — the last schema-level blocker), G-10 resolved (state keying). 1 new minor inconsistency surfaced (G-NEW-19, defaults catalog block). **No schema-level blockers remain** — all remaining 🔴 blockers are correctness mechanics or cross-cluster coordination that a config schema cannot express.

**v6 delta:** G-NEW-19 resolved (defaults catalog block aligned). **All config-schema and document-consistency gaps are now closed.** The remaining backlog is exclusively engineering-design work.

**v7 delta:** The full `Configuration_Specification.md` resolves G-1f (validation table), G-1a (merge semantics, now fully normative), G-5 (in-flight batch restart policy), the API JWT/OAuth2 verification schema, secret rotation (credentials_file/token_file), and adds config_version + strict parsing + blast-radius model. G-2 and G-16 advance (single-worker reconcile and auth schema done; cross-worker propagation and RBAC matrix still design items). 1 new doc-reconciliation item (G-NEW-20). The config-modeling phase is complete; §16 of the spec enumerates the deferred design fields.

**v8 delta:** `Type_Mapping.md` resolves G-18 (the #1 leverage blocker). `Schema_Change_Detection_and_Evolution.md` resolves G-19, G-19b, OQ-DT2, OQ-DT3 and reduces G-19a from blocker to high (channel-C augmentation removes the need for a full DDL parser). G-26 and G-36 advance/resolve. The config spec was updated in lockstep (method removed, source-SELECT precondition added). 2 new doc-reconciliation items (G-NEW-21 config-rule merge, G-NEW-22 enum extensions). **No 🔴 blockers remain in the data-types domain.** The remaining blockers are all in clustering coordination and the ghost-row/delivery-guarantee mechanics.

---

## 16. Recommended Next Steps

1. **Two phases are now complete: config modeling and data-types/schema-evolution.** `Configuration_Specification.md` is the loader/validation/reconcile contract; `Type_Mapping.md` (G-18) and `Schema_Change_Detection_and_Evolution.md` (G-19/G-19b/OQ-DT2/OQ-DT3) close the data-types domain. Three small documentation reconciliations remain from these: re-root the Data-Flow.md state diagram (G-NEW-20), merge the four Type_Mapping config fields + rules into spec §13 (G-NEW-21), and reflect the four `ColumnValue` enum extensions in Data-Flow.md (G-NEW-22). All mechanical.

2. **The critical path is now the clustering-coordination and correctness-mechanics cluster.** Recommended sequencing by leverage:
   - **Ghost-row dedup + PK config (G-13/G-30)** — now the highest correctness blocker; unlocks `tables.primary_key` / `surrogate_key` and is required for correct equality-delete keying. Coordinate with the schema doc's PK-change-is-unsafe rule (§7.2).
   - **Delivery-guarantee GTID-set comparison (G-14)** — define subset-vs-numeric comparison for the exactly-once guard (the same idempotent-guard pattern the schema doc reuses for DDL replay).
   - **Clustering coordination (G-8/G-9/G-2/G-17/G-23) — now designed** (`Clustering_Architecture_Research.md`). Remaining clustering work is the two follow-on items being elaborated first because they gate implementation: **D-5** (verify atomic conditional-write on S3/GCS/Azure — SDK vs `opendal`; GCS uses *generation*, not ETag-on-write) and **D-6** (final `lease_duration`/`renewal_interval` defaults vs the failover + replication-lag SLA, OQ-OB2). Then the smaller D-1…D-4, D-7 (node-id source, lock-backend switch, cold-start precedence, DNS discovery, owner-epoch assertion).
   - **RBAC matrix (G-16)** — verification schema is in place (§6.3.1); define roles and per-endpoint permissions, gating the lifecycle endpoints.

3. **Finish the reduced MySQL DDL work (G-19a).** Scope is now a DDL classifier + table-name extractor (+ rename keyword), not a full ALTER-delta parser — `sqlparser-rs` for a narrow, lower-risk job. Channel C supplies the actual column delta from the catalog.

4. **Specify the pipeline lifecycle state machine** (G-NEW-15) — pause/stop/resume/restart/flush transitions. The restart in-flight-batch policy is decided (spec §12.4); the broader state machine is still open.

5. **Reconcile the large-transaction tension** (G-29): the "flush only on transaction boundary" rule (Data-Flow.md, and now reused by the schema-evolution apply protocol §8) and bounded memory are in conflict; `sink.max_transaction_batch_rows` is reserved (spec §16) but the trade-off decision is still owed.

6. **Research the remaining missing topics** in Section 14 — `sqlparser-rs` (now reduced scope), Iceberg equality-delete format (tied to G-13), OpenDAL, REST catalog spec, Parquet tuning, Arrow IPC, K8s operator patterns, source-DB throttling, and iceberg-rust commit-retry semantics. Note that the schema-evolution doc's §7.1 implementation note flags one new research item: verify drop/rename/type-promotion support against the pinned iceberg-rust version, routing unsupported ops to the halt-table branch.
