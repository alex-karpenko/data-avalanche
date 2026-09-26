---
updated: 2026-09-26
domain: Architecture
tags: [gap-analysis, design, re-derivation, spec-readiness, blockers, open-decisions]
resolves: []
sources: [Sources_Index_v3.md]
---

# CDC System — Design Gap Analysis (fresh re-derivation, v1)

> **This document is a from-scratch re-evaluation.** It deliberately ignores the content of
> `CDC_Gap_Analysis_Report.md` (v9) and re-derives the gap set directly from the four spec
> documents and the thirteen research documents as they stand on 2026-06-20. The legacy report is
> retained unchanged as the historical audit trail. Where a re-derived gap maps onto a legacy ID,
> the legacy ID is shown in *(parentheses)* so the two trackers can be reconciled.
>
> Purpose, per the brief: **prepare a strong foundation for application-specification elaboration.**
> The lens is comprehensive — every domain is mapped, including what is already resolved (with the
> resolving document cited) and everything still open, risky, or contradictory.

---

## 1. Method & inputs

This analysis was produced by reading, in full:

**Specs (Oleksii owns; source of truth):**
`System-Overview.md`, `Data-Flow.md`, `Configuration_Specification.md`, `Schema_Change_Detection_and_Evolution.md`.

**Research (Claude authors; Oleksii reviews):**
`Type_Mapping.md`, `Catalog_Auth_Research.md`, `Clustering_Architecture_Research.md`,
`Snapshot_Resumability_and_Consistency.md`, `Ghost_Row_Dedup_and_Primary_Keys.md`,
`Delivery_Guarantee_and_Position_Guard.md`, `Pipeline_Lifecycle_State_Machine.md`,
`Postgres_Slot_and_Publication_Lifecycle.md`, `MySQL_DDL_Classifier.md`,
`Iceberg_Maintenance.md`, `Iceberg_Partitioning_and_Layout_Evolution.md`,
`Large_Transaction_Handling.md`, `Observability_Metrics_and_Tracing.md`,
`Operational_and_Scope_Gaps_Batch.md`, `API_RBAC_and_Versioning.md`.

**Reference (read-only):** `Sources_Index_v3.md` (50-source index; crate versions & external protocols).

Each domain section below states the **design state**, then a **gap table**. A "gap" here is any
question whose answer the application specification will need: an open decision, an unverified
external dependency, a contradiction between two documents, an unhandled edge case, or spec text
that no longer matches the agreed design.

---

## 2. Legend

### Severity
- 🔴 **Blocker** — implementation/spec of the affected component cannot responsibly start until this is closed.
- 🟡 **High** — a wrong or missing answer causes correctness bugs or data loss in some path.
- 🟢 **Medium** — degrades performance, operability, or document coherence; not a correctness risk.

### Status
- ✅ **Resolved** — a spec or research document fully closes it; only execution/ratification remains.
- 🔄 **Partial** — design decided, but a sub-question, verification, or spec-merge is still open.
- 🟥 **Open** — no settled answer yet; genuine work for the spec-elaboration phase.
- 🧪 **Verify** — design is settled *on paper* but rests on an unproven external capability (a pinned
  crate version or a cloud-storage guarantee) that must be empirically confirmed before it can be trusted.

A 🧪 item is *not* a design gap — it is an execution gate. They are tracked separately (§21) because
they are the items most likely to invalidate a "design complete" claim if they fail.

### ID scheme
Fresh, domain-prefixed, sequential: `DA-<DOMAIN>-<n>`. Domains: `SYS`, `CFG`, `FLOW`, `TYPE`,
`SCHEMA`, `CLUS`, `SNAP`, `DEDUP`, `CAT`, `SINK`, `SRC`, `OBS`, `API`, `OPS`, `DEP`.

---

## 3. Executive dashboard

The design corpus is **substantially complete**. Of the original blocker set, the clustering,
type-mapping, schema-evolution, snapshot, dedup/delivery, lifecycle, slot-lifecycle, catalog-auth,
RBAC, observability, and operational domains are all closed *on paper*. What remains is concentrated
in three buckets:

1. **Empirical capability gates (🧪)** — a handful of external guarantees that must be proven before
   the code that depends on them can be trusted. These are the single biggest risk to a
   "design-complete" claim. See §21.
2. **Spec-merge debt (🟢, mostly)** — a large volume of agreed decisions that live only in research
   docs and have not yet been folded into the authoritative specs (especially `Data-Flow.md`, which
   still describes a superseded snapshot model, and `Configuration_Specification.md` §16's reserved
   field list). See §19.
3. **A short list of genuinely open design questions (🟥)** — mostly scoping/SLA decisions and a few
   newly-surfaced edge cases. See §20 and §18.

### Remaining blockers (🔴)

| ID | Title | Why it blocks |
|---|---|---|
| ~~DA-SINK-1~~ → re-scoped 2026-09-26 | *No longer a system blocker — see the multi-sink note below.* **iceberg-rust cannot commit deletes/overwrites — the MOR-equality-delete sink (and its COW fallback) is not buildable on any published version** | Confirmed 2026-06 on 0.9.1, rechecked 2026-07-22 on 0.10.0, rechecked 2026-08-23 on 0.10.1, **rechecked 2026-09-19 on compiled 0.10.1 and the v0.11.0-rc.1/rc.2 surfaces — still true**: `EqualityDeleteFileWriter` exists but the `Transaction` API is **append-only** (`fast_append` data files + metadata actions; no row-delta/overwrite/rewrite/merge-append). A CDC sink that applies UPDATE/DELETE cannot be built on the current public API. Upstream epic apache/iceberg-rust#2186 is **open**; the §24.2.3 "`OverwriteAction` merged" reading is **retracted** (PR #2185 is open/unmerged — §24.2.4). **The sink spec must gate on a future iceberg-rust version or adopt §24.2.1 option 2–4.** *(G-13 §8; details §24.2.1, rechecks §24.2.2/§24.2.3, latest §24.2.4)* |

> **MULTI-SINK RE-SCOPE (2026-09-26) — no system-level 🔴 remains on paper.**
> `research/Multi_Sink_Architecture_and_Delta_Lake.md` (decisions D-MS.1–D-MS.14 signed off) makes the
> sink pluggable (`sink.type`, required, no default) and adopts **Delta Lake via delta-rs 1.0** as a
> first-class v1 sink: MERGE commits deletes + upserts atomically, which iceberg-rust cannot. DA-SINK-1
> is re-scoped to **DA-SINK-1i — "Iceberg sink, mirror mode" capability**, enforced at pipeline start
> by the per-sink capability assertion instead of blocking the design. The Delta sink carries its own
> 🧪 gates (DA-MS-1…DA-MS-4, §25) — P6 (MERGE cost under random-key churn) is the one that could add a
> scope caveat. See §25.

> **DA-CLUS-1 (storage-CAS conformance) — CLOSED 2026-06.** Proven against **real S3, GCS, and Azure**:
> S3 T1–T9 (versioned + non-versioned), GCS T1–T10 (incl. the generation-CAS gate T10), Azure T1–T9.
> The design changed under contact with real services — `lock_backend: storage` is a **per-backend
> `LeaseStore`**: OpenDAL for S3, native `google-cloud-storage` for GCS, native Blob-REST+SAS for Azure
> (OpenDAL 0.57 advertises `write_with_if_match` only for S3). One trait, §5.3 protocol unchanged. The
> conformance harness `prototypes/lease-conformance/` is the durable proof. **DA-SINK-1 is now the only
> remaining 🔴** (itself re-scoped 2026-09-26 — see the note above). See §24.1.

Everything else is 🧪 (other capability gates), 🟡/🟢 with a decided design, or 🟥 scoping work that
does not block starting the specification.

### Counts (by status)

| Status | Count (approx.) | Notes |
|---|---|---|
| ✅ Resolved | ~70% of items | Design closed by a research/spec doc. |
| 🔄 Partial | small | Decided but with an open sub-item. |
| 🟥 Open | ~12 items | §20 register. |
| 🧪 Verify | 6 gates | §21. |
| 🟢 Spec-merge debt | ~22 owed (16 config + 6 overview); **9 data-flow applied 2026-06-20** | §19. |

---

## 4. Architecture & system scope (SYS)

**State.** `System-Overview.md` fixes the top-level shape: Rust + Arrow + Iceberg + memberlist +
tokio; pipeline as the unit of replication; per-table halt philosophy; "no local state, everything
durable to cloud storage"; app creates/destroys its own DB resources but never deletes Iceberg
artifacts. Most of the original "Open Questions" block in that doc has since been answered by the
research corpus.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SYS-1 | Two-plane architecture (AP coordination ring + CP lease) | 🔴→✅ | ✅ | `Clustering_Architecture_Research.md` §1 — ring decides *should-own*, lease decides *may-run*. | OQ-CL1 |
| DA-SYS-2 | Single-node = one-member cluster, same code path | 🟡 | ✅ | Clustering §10; lease stays on to guard rolling-deploy double-singleton. | OQ-CL4 |
| DA-SYS-3 | "App never deletes Iceberg artifacts" vs maintenance/orphan cleanup ownership | 🟢 | ✅ | `Iceberg_Maintenance.md` — v1 maintenance is external (operator/Spark/Trino); app exposes signals + reserves `sink.maintenance`. | OQ-M2, G-25 |
| DA-SYS-4 | Reserved metadata column collision (`_op/_ts/_lsn/_source_db/_pipeline` vs a source column of the same name) | 🟡 | 🟥 | Config spec §5 only says operators "should be aware." **No defined runtime behavior** if a source table actually has an `_op` column — rename? prefix-escape? halt? Needs a rule. See DA-FLOW-5. | — |
| DA-SYS-5 | Lineage column `_seq` (per-change order) is required by dedup but absent from the documented lineage column set | 🟡 | 🟥 | `Ghost_Row_Dedup_and_Primary_Keys.md` §4 introduces `_seq`; `Data-Flow.md`'s lineage table lists only five `_`-columns and `CdcEvent` carries only commit `source_pos`. The per-change position must be added to both. See DA-FLOW-2. | — |
| DA-SYS-6 | `snapshot_mode: only` referenced but not in the enum | 🟢 | ✅(moot) | The enum is `never\|initial\|always\|dump_always\|dump_initial` (config §9.3). The stray "only" in System-Overview's OQ-M2 is superseded by `dump_*`. Drop the dangling reference. | OQ-M2 |

---

## 5. Configuration model (CFG)

**State.** `Configuration_Specification.md` is the most mature spec: full type vocabulary, env-var
grammar, two-phase validation (structural Phase 1 / resolution Phase 2), the R/PR/PROC mutability
model, merge/union semantics, the 80+ rule master validation table (§13), and a complete annotated
example. The catalog-auth and schema-evolution surfaces are already merged. The principal CFG gaps
are **not** in the model itself but in **reserved fields awaiting promotion** (§16 of that spec) and
in keeping the table synchronized with research outputs.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-CFG-1 | Config data model fully specified | 🔴→✅ | ✅ | The whole `Configuration_Specification.md`. | OQ-C2, G-1f |
| DA-CFG-2 | Env-var substitution grammar (`${VAR}`, `:-default`, `$$` escape, coercion order) | 🟡 | ✅ | Config §4.3. | — |
| DA-CFG-3 | Secret **rotation** without process restart | 🟡 | ✅ | `credentials_file`/`token_file` (§4.6) + resolved-config diffing (§11.1). | OQ-C3 (partial) |
| DA-CFG-4 | External secret-manager integration (Vault / AWS SM / GCP SM) | 🟢 | 🟥 | Only env-var and file-content rotation are designed. Direct secret-manager integration is **undecided** — likely out-of-scope-for-v1, but say so explicitly (file-on-tmpfs by a sidecar is the implied pattern). | OQ-C3 (remainder) |
| DA-CFG-5 | Flush-trigger multi-criteria semantics (OR, first-wins, tri-state null) | 🟡 | ✅ | Config §9.4.1; `Data-Flow.md` §Flush decision. | OQ-P1 |
| DA-CFG-6 | Reserved cluster fields not yet promoted into `globals.cluster` table | 🟢 | ✅ | `lease_duration`, `lease_renewal_interval`, `lease_safety_margin`, `rebalance_stabilization`, `max_concurrent_handoffs`, `max_concurrent_takeovers`, `drain_timeout`, `discovery`, `discovery_dns_name`, `memberlist.lifeguard_enabled` are designed (Clustering §11/§19/§20) but still sit in config §16. Promotion owed. See §19. **Promoted 2026-09-26** — config §6.5 / §6.5.1 (V-094–V-096). | G-8, G-9 |
| DA-CFG-7 | Reserved data-plane fields not yet promoted | 🟢 | ✅ | `tables.primary_key`, `tables.partitioning`, `snapshot.chunk_size`, `sink.max_transaction_batch_rows`, `sink.max_transaction_buffer_bytes`, `sink.buffer_capacity_rows`, `source.postgres.max_wal_lag_bytes`, `source.mysql.binlog_purge_action`, slot-lifecycle fields, `globals.tracing.*`, `sink.metrics.per_table`, `globals.api.role_mapping`, `globals.azure.account_name`, `catalog.retry`, `sink.maintenance`, `glue.strip_schema`. Designed in research; promotion owed. See §19. **Promoted 2026-09-26** — config §6.3/§6.6/§6.7/§9.2/§9.3/§9.4/§9.5 (V-097–V-113); `sink.maintenance` stays reserved per sink in §16; `primary_key`/`partitioning` specified as per-table maps; PG orphan policy named `slot_on_orphan` (`slot` is the name string). | many |
| DA-CFG-8 | Reserved fields now **rejected/moot** and to be struck from §16 | 🟢 | ✅ | `tables.surrogate_key` (G-30 rejected, `Ghost_Row…` §3) and `source.consistent_snapshot` (G-7 moot — incremental snapshot has no weaker mode, `Snapshot_Resumability…` §3). Remove from the deferred list. **Struck from §16 2026-09-26.** | G-30, G-7 |
| DA-CFG-9 | `lease_renewal_interval ≤ lease_duration/3` enforcement | 🟡 | ✅ | Promoted to hard error V-073 (config §13.4); rationale in Clustering §19.3/§19.6. | D-8 |
| DA-CFG-10 | Config reload cross-worker propagation contract | 🔴→✅ | ✅ | Clustering §9 — generation pointer in `globals.state` + gossip nudge; no proxy. | OQ-C1, G-2, G-17, OQ-API3 |

---

## 6. Data flow & pipeline execution (FLOW)

**State.** `Data-Flow.md` defines the four stages, the `CdcEvent` boundary type, per-source decode,
the accumulator, the Iceberg write path, offset acknowledgment, and the task/concurrency model. It is
also the spec that has **drifted most** from the agreed design: it still documents the *blocking
consistent snapshot* (superseded by the incremental chunked model), the scalar `committed_lsn` guard
(generalized to `committed_position`), and a redundant `pipelines/<id>/` state path. These are
documentation gaps, not design gaps, but they are load-bearing for anyone reading the spec as truth.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-FLOW-1 | In-flight buffer handling on pipeline restart | 🟡 | ✅ | Config §12.4 discard-and-replay default; `Pipeline_Lifecycle_State_Machine.md`. | OQ-P2 |
| DA-FLOW-2 | `CdcEvent` lacks a **per-change** position distinct from commit `source_pos` | 🟡 | 🟥 | Dedup keep-last needs `_seq` = PG change LSN / MySQL `(binlog_file,end_log_pos)` (`Ghost_Row…` §4); `CdcEvent` currently carries only commit `source_pos` + `xid`. The struct and the accumulator contract must add the per-change sequence. Surfaced by reading FLOW+DEDUP together. | — |
| DA-FLOW-3 | Backpressure / bounded channels | 🟡 | ✅ | `Operational…` G-27 — all inter-task channels bounded; `sink.buffer_capacity_rows`. | G-27 |
| DA-FLOW-4 | Phase-1 snapshot model in spec is superseded | 🟢 | 🔄 | `Snapshot_Resumability…` replaces blocking COPY/EXPORT_SNAPSHOT with the stream-first incremental chunk loop; `Data-Flow.md` Phase 1 + On-Demand section must be rewritten. Spec-merge debt (§19). | G-6 |
| DA-FLOW-5 | `TRUNCATE` manifestation in the Iceberg mirror (both engines) | 🟡 | 🟥 | `MySQL_DDL_Classifier.md` §3 says TRUNCATE → "table-clear (Iceberg DELETE/overwrite all rows or fresh snapshot boundary), flagged for the sink-writer contract." `Data-Flow.md` Phase 2a lists the PG `T` message but no sink action is specified. The concrete sink behavior (and PG/MySQL parity) is undefined. | — |
| DA-FLOW-6 | PG `DROP TABLE` detection (no in-band DDL on pgoutput) | 🟡 | 🔄 | MySQL DROP is caught by channel B (`QUERY_EVENT`). PG drops are **not** replicated as DDL; the table simply stops emitting and is caught by channel D idle-rescan / channel A on restart (`Schema_Change…` §2.1). The *latency and exact reaction* (halt vs retain) for a PG drop of a tracked table is not spelled out as crisply as MySQL's. Worth an explicit line. | — |
| DA-FLOW-7 | `committed_lsn` scalar generalized to engine-tagged `committed_position` | 🟡 | ✅(design) | `Delivery_Guarantee…` §4 — per-table HWM `cdc.committed_position`; `Data-Flow.md` wording owed (§19). | G-14 |
| DA-FLOW-8 | Shutdown / drain ordering | 🟡 | ✅ | `Data-Flow.md` §Concurrency + Clustering §8.3 drain protocol (adds explicit lease release). | OQ-CL3 |

---

## 7. Type mapping (TYPE)

**State.** `Type_Mapping.md` closes the three-hop chain (DB wire → `ColumnValue` → Arrow → Iceberg)
with full PG and MySQL tables, four `ColumnValue` enum extensions, an unsupported-type fallback chain,
and per-column overrides. Essentially complete; residual items are spec-merge and one crate annotation.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-TYPE-1 | Full DB→Arrow→Iceberg mapping table | 🔴→✅ | ✅ | `Type_Mapping.md`. | G-18, OQ-DT1 |
| DA-TYPE-2 | `ColumnValue` enum extensions (tz flag, typed `Array`, `Time`, `UInt64`) | 🟡 | ✅ | `Type_Mapping.md` §extensions; reflected in `Data-Flow.md` enum. | G-NEW-22 |
| DA-TYPE-3 | `BIGINT UNSIGNED` / `numeric(P>38)` overflow handling | 🟡 | ✅ | `decimal(20,0)` for the former; `numeric_overflow_action: string\|error` for the latter. | — |
| DA-TYPE-4 | `TINYINT(1)`→bool detection requiring `information_schema` | 🟢 | ✅ | `tinyint1_as_bool` + `tinyint1_detection`; G-NEW-21 dropped the stale events-mode warning. | G-NEW-21 |
| DA-TYPE-5 | `FixedSizeBinary(16)` → Iceberg `uuid` needs explicit iceberg-rust annotation | 🟢 | 🧪 | `Type_Mapping.md` Hop-3 table flags this as the one Arrow type needing a field-level `{"iceberg.type":"uuid"}` hint — verify the mechanism exists in the pinned crate. See §21. | — |
| DA-TYPE-6 | Merge the four TYPE config fields + 4 validation rules into the master table | 🟢 | ✅ | Already largely done (V-078..V-082 present); confirm `column_type_overrides`, `numeric_overflow_action`, `toast_strategy`, `tinyint1_*` all merged (G-NEW-21). Verified 2026-09-26: V-078…V-082 present; V-078 now uses the sink-neutral logical type grammar. | G-NEW-21 |
| DA-TYPE-7 | TOAST unchanged-column strategy (`carry_forward` vs `re_query`) | 🟡 | ✅ | `source.toast_strategy` (PG only); V-077/V-080. Re-query pool-saturation is a warning, not a halt. | — |

---

## 8. Schema change detection & evolution (SCHEMA)

**State.** `Schema_Change_Detection_and_Evolution.md` is a complete, reviewed design: the
four-channel detection model (A startup / B in-band trigger / C catalog augment / D idle rescan), the
§0 ordering invariant (apply in stream order, scoped to the change, never fast-forward to "now"), the
§7.2 safe/unsafe/ignorable DDL classification table, the apply protocol (seal→flush→evolve→rebuild→
resume), idempotent replay, and rename resolution. The MySQL classifier is separately de-risked.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SCHEMA-1 | DDL during live replication — apply protocol | 🔴→✅ | ✅ | §8 detect-at-boundary protocol; startup & non-replayable-gap → re-snapshot (§8.2). | OQ-DT2 |
| DA-SCHEMA-2 | Supported evolution operations (safe/unsafe set) | 🟡 | ✅ | §7.1–§7.2 classification table = the G-19 contract. | OQ-DT3, G-19 |
| DA-SCHEMA-3 | Rename vs drop+add disambiguation | 🟡 | ✅ | §6 — PG `pg_attribute.attnum`; MySQL `RENAME`/`CHANGE` keyword; degrade to drop+add+warn only when unprovable. | DT-R2 |
| DA-SCHEMA-4 | MySQL DDL classifier scope & failure mode | 🟡 | ✅ | `MySQL_DDL_Classifier.md` — two-tier fail-soft (sqlparser-rs → regex → channel C); halt only if no table name recoverable. De-risked from its v7 blocker elevation. | G-19a |
| DA-SCHEMA-5 | Channel-C/A/D require SELECT on `information_schema`/catalogs | 🟡 | ✅ | §9 precondition; checked at pipeline start (runtime, not config). Reuse replication creds. | DT-R8 |
| DA-SCHEMA-6 | `method` field removed; detection is fixed four-channel model | 🟢 | ✅ | §10; config V-024/V-031/V-032 deleted, `interval`→`rescan_interval`. | DT-R1 |
| DA-SCHEMA-7 | ENUM/SET representation (string vs ordinal) | 🟢 | ✅ | `ENUM→string`, `SET→list<string>`; append-only value adds Ignorable, reorder/remove Unsafe. | DT-R5 |
| DA-SCHEMA-8 | iceberg-rust drop/rename/type-promotion support | 🟡 | 🧪 | §7.1 implementation note — only `add_column` is mature; **verify** drop/rename/promotion against the pinned crate, route unsupported ops to halt. See §21. | — |
| DA-SCHEMA-9 | Idempotent DDL replay + checkpoint-after-evolve ordering | 🟡 | ✅ | §8.3 — re-read live Iceberg schema, skip if already applied; advance offset only after schema commit. | — |

---

## 9. Clustering & coordination (CLUS)

**State.** `Clustering_Architecture_Research.md` is the deepest doc: two-plane model, derived ring,
NodeMeta schema, SWIM-vs-Scuttlebutt decision (keep SWIM+Lifeguard), the storage CAS lease with a
clock-skew-safe acquire/renew/takeover protocol, the renewal-failure taxonomy + safety deadline
(§19), the `ClusterReconciler`+`PipelineController` split (§20.1), takeover rate-limiting (§20.2),
flap immunity (§20.3), config propagation, single-node mode, and the `globals.state` object layout.
All design decisions D-1..D-11 are resolved on paper; **D-5 is the one item that cannot be closed on
paper** and is this report's top blocker.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-CLUS-1 | **Conditional-write CAS conformance on S3+GCS+Azure** | 🔴→✅ | ✅ | **CLOSED 2026-06 — all three backends proven against real services (§24.1); the notes below are the historical trail.** | Clustering §12/§17 — lease rests entirely on atomic CAS on one object; **GCS uses *generation*, not ETag, for write preconditions** (the trap). Trait + T1–T10 suite at `prototypes/lease-conformance/`. **S3 proven (Route A) and GCS proven (Route B native `google-cloud-storage` SDK, T1–T10 incl. the generation-CAS gate), 2026-06** — findings folded into §24.1. **Only Azure still owed.** The last clustering item not closeable on paper. | D-5 |
| DA-CLUS-2 | Split-brain exclusive ownership (the lease) | 🔴→✅ | ✅ | §5 storage CAS lease; ring is scheduling, lease is safety. | G-8, OQ-CL1 |
| DA-CLUS-3 | Sink-side fencing for a zombie holder | 🟡 | ✅(design) | §5.5 — `owner_epoch` in Iceberg snapshot summary + Iceberg atomic-commit fence + `_lsn` dedup (defense in depth). Depends on DA-SINK-2 (snapshot-summary persistence). | D-7 |
| DA-CLUS-4 | Lease renewal failure taxonomy (412 vs transient) + safety deadline | 🟡 | ✅ | §19 — `deadline = last_renew_local + lease_duration − guard`; renewer state machine; commit gating. Prevents a transient S3 blip from stopping a healthy (esp. single-node) worker. | D-8 |
| DA-CLUS-5 | Rebalancing / graceful drain / handoff | 🔴→✅ | ✅ | §8.3 drain (flush→commit→checkpoint→release lease) + §8.5 rate-limiting; planned vs unplanned paths separated. | G-9, OQ-CL3 |
| DA-CLUS-6 | Reconciler ↔ renewer orchestration (head-of-line blocking) | 🟡 | ✅ | §20.1 — `ClusterReconciler` (ring only) + per-pipeline `PipelineController` (sole lease-writer); level-triggered + periodic resync. | D-9 |
| DA-CLUS-7 | Unplanned-takeover storm rate-limiting | 🟢 | ✅ | §20.2 — `max_concurrent_takeovers` (default 4) caps concurrent Phase-0 starts, with jitter. | D-10 |
| DA-CLUS-8 | Flap immunity (suspect stays in ring; symmetric stabilization) | 🟢 | ✅ | §20.3 — only `dead`/`left` removes from ring; `rebalance_stabilization` on all additions incl. rejoins. | D-11 |
| DA-CLUS-9 | `node_id` stability / StatefulSet mandate | 🟡 | ✅ | §3.4 D-1 — `node_id = memberlist.node_name`; k8s StatefulSet pod name; standalone persisted UUID; `incarnation` distinguishes a fresh process. | D-1, OQ-P3 |
| DA-CLUS-10 | Cluster-wide-consistent anchors (`tokens`, `lease_duration`, `lock_backend`, `hash_algo`) | 🟡 | ✅ | §12 anchor; refuse-to-start on mismatch (V-026/V-062/V-075/V-076). | G-36, D-2 |
| DA-CLUS-11 | Lease timing defaults vs failover SLA | 🟡 | ✅ | §18 D-6 — **Balanced** ratified (`lease_duration=15s`, `renewal=5s`, ~20–22s unowned window). Tunable. | D-6 |
| DA-CLUS-12 | `kubernetes` lock_backend (K8s Lease API) | 🟢 | 🟥 | Reserved as a future `LeaseStore` impl; **only `storage` is built for v1** (V-075). A genuine but deliberately deferred scope item. | D-2 |
| DA-CLUS-13 | Per-source-endpoint takeover bulkhead | 🟢 | 🟥 | §20.2 notes the sharper risk (many pipelines on one source DB reconnecting together) as a **future** per-endpoint concurrency group; v1 cap is per-node-global. | D-10 |

---

## 10. Snapshotting (SNAP)

**State.** `Snapshot_Resumability_and_Consistency.md` replaces the blocking snapshot with an
**incremental chunked, stream-first** model (DBLog/Debezium-style watermarks + PK-upsert
reconciliation), which simultaneously closes resumability, multi-table consistency, and the on-demand
read-point hazard. This is a *change* to `Data-Flow.md` Phase 1, so it carries spec-merge debt.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SNAP-1 | Mid-snapshot crash resumability (cursor format) | 🔴→✅ | ✅ | §2 typed identifier-tuple keyset cursor; checkpoint per chunk; resume idempotent. | G-6, OQ-S1 |
| DA-SNAP-2 | Multi-table consistency without a global lock | 🟡 | ✅ | §3 — convergent, not frozen-instant; no `RELOAD`/`LOCK TABLES`/`EXPORT_SNAPSHOT` needed; no `consistent_snapshot` toggle. | G-7, OQ-S2 |
| DA-SNAP-3 | On-demand snapshot read-point (the "pause_lsn+1" hazard) | 🟡 | ✅ | §4 — on-demand is the §1 chunk loop with **no stream pause**; watermarks reconcile. | G-20, OQ-S3 |
| DA-SNAP-4 | Snapshot parallelism within a pipeline | 🟢 | ✅ | §5 — reuses `parallelism`; disjoint tables per worker; serial keyset within a table. | OQ-S4 |
| DA-SNAP-5 | Keyset collation correctness (text identifiers) | 🟡 | ✅ | §2 collation pin — binary collation (`COLLATE "C"` / `utf8mb4_bin`) for the pagination predicate, recorded in the cursor. Subtle but load-bearing. | — |
| DA-SNAP-6 | `snapshot.chunk_size` config field | 🟢 | ✅ | Default 10000; promotion into config owed (§19). Promoted 2026-09-26 as `tables.snapshots.chunk_size` (V-108). | G-6 |
| DA-SNAP-7 | Snapshot vs WAL/binlog retention coupling | 🟡 | 🔄 | §7 cross-ref — a slow snapshot lengthens the stream backlog; interacts with the WAL-lag guard (G-22) and binlog purge (G-26). Designed but the interaction tuning is an operational note, not a hard rule. | — |
| DA-SNAP-8 | PK-less table under a **dump-only** mode (snapshot, never stream) | 🟡 | 🟥 | The PK-less halt rule (DA-DEDUP-3) is justified by equality-delete keying — but a `dump_*` table is insert-only and never deletes, so the keying argument doesn't apply. Whether a dump-only PK-less table may be snapshotted (or still halts) is **unspecified**. Newly surfaced edge case. | — |

---

## 11. Delivery guarantee, dedup & primary keys (DEDUP)

**State.** `Ghost_Row_Dedup_and_Primary_Keys.md` (G-13/G-30) and
`Delivery_Guarantee_and_Position_Guard.md` (G-14) together make the pipeline **row-level idempotent**:
MOR equality-delete keyed on the table identifier, intra-batch collapse to ≤1 delete + ≤1 data record
per key ordered by `_seq`, mirror-delete semantics (no `D` data row), and a `SourcePosition`
abstraction (numeric LSN vs GTID-set subset/union/intersect). Idempotency is the correctness basis;
the position guard is demoted to optimization + checkpoint-advance.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-DEDUP-1 | Ghost-row / equality-delete keying correctness | 🔴→✅ | ✅ | §0 strictly-less-than rule; in-batch collapse §4. | G-13 |
| DA-DEDUP-2 | Delivery-guarantee position comparison (LSN vs GTID-set) | 🔴→✅ | ✅ | `SourcePosition` trait; canonical GTID-set serialization; per-table HWM; resume = laggard (min/intersect). | G-14 |
| DA-DEDUP-3 | PK-less tables | 🟡 | ✅(by rejection) | §3 — halt; `surrogate_key` removed; accepted limitation (genuinely key-less tables can't replicate). | G-30 |
| DA-DEDUP-4 | Identifier source & override | 🟡 | ✅ | Inferred PK via channel C; `tables.primary_key` override (not a surrogate); Iceberg identifier constraints (top-level, primitive, NOT NULL, non-floating). | D-13.1/7 |
| DA-DEDUP-5 | Before-image / REPLICA IDENTITY contract | 🟡 | ✅ | §2 — identifier ⊆ replica-identity (PG); MySQL FULL image already required; halt with named columns if unmet. | D-13.4 |
| DA-DEDUP-6 | Key-mutating UPDATE (PK value changes) | 🟡 | ✅ | §4 — delete(old key) + data(new key); reuses before-image. | D-13.3 |
| DA-DEDUP-7 | Cross-table atomic visibility **not** guaranteed | 🟡 | 🟥(scoped-out) | `Delivery…` §5 / `Large_Transaction…` — each table is its own Iceberg commit; cross-table atomicity is explicitly out of scope; would need a multi-table-commit catalog (Nessie). Documented limitation, reopen path noted. | D-14.5, G-25-noted |

---

## 12. Catalog & authentication (CAT)

**State.** `Catalog_Auth_Research.md` closed G-1g and the schema is fully merged into config §9.5 and
the §13 validation rules (V-040..V-048). Hive Thrift `none/ldap/kerberos`; REST HTTP
`none/bearer/oauth2/sigv4`; the deprecated built-in `/v1/oauth/tokens` warning.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-CAT-1 | Catalog auth model (Hive SASL / REST OAuth2/bearer/sigv4) | 🔴→✅ | ✅ | `Catalog_Auth_Research.md` §2–§3; config §9.5. | G-1g |
| DA-CAT-2 | Primary catalog vs multi-catalog at launch | 🟢 | ✅ | All three backends (glue/hive/rest) are first-class via the union; choice is per-pipeline. | OQ-IC2 |
| DA-CAT-3 | OAuth2 must point at an external IdP | 🟡 | ✅ | `token_url` external; V-048 warns on the deprecated built-in endpoint. | — |
| DA-CAT-4 | Credential vending interaction with the sink credential model | 🟢 | 🟥 | §3.5 is informational only — if the catalog vends short-lived table-scoped storage creds, the `globals.cloud` chain becomes optional, but **how the writer consumes vended creds is not specified**. Minor; flag for the sink-writer contract. | — |
| DA-CAT-5 | Catalog unavailability retry policy (separate from object storage) | 🟡 | ✅ | `Operational…` G-35 — own retry + circuit breaker + DEGRADED; optional `catalog.retry`. | G-35 |

---

## 13. Sink / Iceberg writer (SINK)

**State.** The writer contract is spread across `Ghost_Row_Dedup…` (equality-delete keying),
`Iceberg_Partitioning…` (partition spec + before-image routing), `Iceberg_Maintenance…` (external
maintenance + signals), `Large_Transaction…` (split commits), and `Delivery…` (snapshot-summary
properties). The design is coherent; the open items are the **two crate-capability gates** that the
writer's correctness literally depends on.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SINK-1 | **Equality-delete writer assigns delete files the enclosing commit's sequence number** | 🔴→🟡 | 🔄 | **Superseded:** the real finding is "no delete-commit action at all" (§24.2.1–§24.2.4). **Re-scoped 2026-09-26 to DA-SINK-1i (Iceberg mirror-mode capability)** by the multi-sink design (§25); Delta sink is the v1 mirror. Original note: | `Ghost_Row…` §8 — the entire strictly-less-than ghost-row guarantee assumes this iceberg-rust 0.9.0 behavior; **verify before building the writer**. If unsupported/mis-sequenced, need position-deletes-after-read or a different strategy. | G-13 §8 |
| DA-SINK-2 | Custom snapshot-summary properties (`cdc.owner_epoch`, `cdc.committed_position`) preserved across commits | 🟡 | ✅ (append path) | Required by CLUS fencing (D-7) and DEDUP guard (D-14.4); `Clustering…` §5.5 flags "verify iceberg-rust preserves custom snapshot-summary properties." See §21. **2026-09-26 (probe G2b):** the `cdc.owner_epoch` table-property carrier commits in the same metadata commit as the append, all-or-nothing, and survives foreign commits + expiry. Delete-commit path still waits on DA-SINK-1i. | D-7 |
| DA-SINK-3 | Write strategy COW vs MOR | 🟡 | ✅ | MOR + equality deletes is the default (`Ghost_Row…` §5); COW new-tables-only. | OQ-IC1 |
| DA-SINK-4 | MOR↔COW strategy change migration | 🟢 | ✅ | `Iceberg_Partitioning…` §4 (G-12a) — new-data-only; explicit compaction/re-snapshot to migrate; PR. | G-12a |
| DA-SINK-5 | Partition spec config + evolution | 🟢 | ✅ | `Iceberg_Partitioning…` §1/§3 (G-24) — `tables.partitioning`; default unpartitioned; spec evolution = new-data-only. | G-24 |
| DA-SINK-6 | Partition-key-changing UPDATE (cross-partition ghost) | 🟡 | ✅ | §2 (D-24) — equality-delete routed to the **old** partition via before-image; same machinery as a PK mutation. **Sink-writer contract addition.** | G-24 |
| DA-SINK-7 | Large-transaction vs txn-atomic-flush tension | 🟡 | ✅ | `Large_Transaction…` (G-29) — bounded split commits (default off → atomic), `max_transaction_buffer_bytes` halt-backstop; crash-safe via idempotency. | G-29 |
| DA-SINK-8 | Spill-to-disk for oversized txns | 🟢 | 🟥(deferred) | `Large_Transaction…` §5 — future `sink.large_txn_mode: split\|spill`; out of scope for v1. | G-29 §5 |
| DA-SINK-9 | Iceberg maintenance ownership (compaction/expiry/orphans) | 🟢 | ✅ | `Iceberg_Maintenance…` (G-25) — external for v1; app exposes signals + reserves `sink.maintenance`; built-in gated on iceberg-rust rewrite support. | G-25 |
| DA-SINK-10 | iceberg-rust compaction/rewrite API availability | 🟢 | 🧪 | Gates any **future** built-in maintenance (deferred anyway). Verify against pinned crate. | G-25 §14 |
| DA-SINK-11 | Exactly-once vs at-least-once into Iceberg | 🟡 | ✅ | Effectively exactly-once-effect via idempotency (DEDUP) + position guard; at-least-once delivery, dedup absorbs replays. | OQ-IC3 |

---

## 14. Source connection lifecycle (SRC)

**State.** `Postgres_Slot_and_Publication_Lifecycle.md` (G-15) closes the PG runtime lifecycle;
`Operational…` covers MySQL binlog purge (G-26) and the WAL-lag guard (G-22). MariaDB is in scope via
the MySQL path with one genuine correctness dependency.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-SRC-1 | PG slot existence state machine + orphan policy | 🟡 | ✅ | §1 — `slot.on_orphan: halt(default)\|adopt_resnapshot`; resume-without-snapshot rejected. | G-15 |
| DA-SRC-2 | Slot creation responsibility | 🟡 | ✅ | §2 — `slot_management: managed(default)\|require_existing`; the latter forces `auto_cleanup=false`. | G-15 |
| DA-SRC-3 | Slot loss / WAL-gone recovery | 🟡 | ✅ | §3 — recreate slot + incremental re-snapshot (G-6); warn/metric. | G-15 |
| DA-SRC-4 | Publication reconciliation | 🟢 | ✅ | §4 — app-maintained explicit table list; `ALTER PUBLICATION` on table-set change; never `FOR ALL TABLES`. | G-15 |
| DA-SRC-5 | Abandoned-slot WAL-lag guard | 🟡 | ✅ | `Operational…` G-22 — `cdc_wal_lag_bytes` always emitted; `max_wal_lag_bytes` opt-in halt. | G-22 |
| DA-SRC-6 | MySQL purged-binlog handling | 🟡 | ✅ | `Operational…` G-26 — `binlog_purge_action: error(default)\|re_snapshot`. | G-26 |
| DA-SRC-7 | External same-name slot collisions undetectable | 🟢 | ✅(limitation) | §5 — PG slots carry no ownership tag; mitigated by V-058 uniqueness + operator discipline; documented, not papered over. | G-15 |
| DA-SRC-8 | MariaDB GTID dialect (`domain-server-sequence`) | 🟡 | 🟥 | `Operational…` G-38 — MariaDB accepted via the MySQL path, but the `SourcePosition` MySQL impl **must support both GTID dialects** (MariaDB domains vs MySQL UUIDs); flagged as a **new sub-research item** and the one genuine correctness dependency. | G-38 |
| DA-SRC-9 | TOAST re-query control-connection pool sizing | 🟢 | 🔄 | V-077 warns on `re_query`+high-volume snapshot; concrete pool sizing is an implementation detail, not yet fixed. | — |

---

## 15. Observability (OBS)

**State.** `Observability_Metrics_and_Tracing.md` (G-33/G-32) defines the mandatory metric catalogue,
the pipeline-level-default / per-table-opt-in cardinality policy, OTLP tracing (disabled when no
endpoint), and trace-id/pipeline injection into JSON logs.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-OBS-1 | Mandatory Prometheus metric catalogue | 🟡 | ✅ | §2 — replication/source/snapshot/Iceberg+maintenance/clustering/status families. | G-33, OQ-OB3 |
| DA-OBS-2 | Metric cardinality policy | 🟢 | ✅ | §1 — `{pipeline,source_host,source_db}` default; `table` label only when `sink.metrics.per_table=true`. | G-33 |
| DA-OBS-3 | Distributed tracing | 🟢 | ✅ | §3 (G-32) — `globals.tracing.otlp_endpoint` (absent⇒off) + `sample_ratio`; batch/commit-granularity spans, no per-event spans. | G-32, OQ-OB1 |
| DA-OBS-4 | Log/trace correlation | 🟢 | ✅ | §4 — inject `trace_id`/`span_id`/`pipeline` into JSON logs. | §10.3 |
| DA-OBS-5 | **Numeric end-to-end replication-lag SLA** | 🟡 | 🟥 | `Clustering…` §18.6 explicitly leaves OQ-OB2 open as a *tuning input*: a concrete max-lag number is not fixed, which is what grounds the lease-timing profile and flush/checkpoint cadence. Decide a number (and whether failover-time carries its own SLA distinct from steady-state). | OQ-OB2 |

---

## 16. Management API & security (API)

**State.** `API_RBAC_and_Versioning.md` (G-16/G-9.1) closes the RBAC matrix: three nested roles
(`admin ⊇ operator ⊇ viewer`), an endpoint→min-role table under `/v1`, per-auth-mode role binding,
configurable IdP role mapping, and the versioning scheme. Health/metrics are unversioned and
unauthenticated. The auth *mechanism* (jwt/oauth2 verification params) was already in config §6.3.1.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-API-1 | RBAC role→permission matrix | 🟡 | ✅ | `API_RBAC…` §2 — endpoint table; reload/validate = operator; leave/shutdown = admin. | G-16, OQ-API2 |
| DA-API-2 | API versioning | 🟢 | ✅ | `/v1` for functional endpoints; health/metrics unversioned. | G-9.1 |
| DA-API-3 | Role binding per auth mode + IdP role mapping | 🟡 | ✅ | §3/§4 — none=open, basic=admin, jwt/oauth2=mapped claim (max wins); 401 vs 403 distinguished; configurable `role_mapping`. | D-16.3/4 |
| DA-API-4 | `config/validate` may echo resolved secrets → operator-only | 🟡 | ✅ | §2 note — validate is operator, not viewer, because the resolved config can contain secret material. | — |
| DA-API-5 | TLS / mTLS for API and cluster-internal transport | 🟡 | ✅ | API TLS config §6.3.2; memberlist TLS §6.5.2 (CA presence ⇒ mTLS). V-052 "auth without TLS" stays a **warning** (upstream termination in k8s is common). | OQ-API1, D-16.5 |
| DA-API-6 | Lifecycle endpoints inherit operator role | 🟢 | ✅ | `Pipeline_Lifecycle…` §6 — pause/resume/restart/stop/flush all operator. | G-NEW-15 |

### Pipeline lifecycle state machine (sub-domain of API + FLOW)

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-API-7 | Pause/stop/resume/restart/flush state machine | 🟡 | ✅ | `Pipeline_Lifecycle_State_Machine.md` — persisted desired-state (`RUNNING/PAUSED/STOPPED`), owner reconciles; survives failover & reload. | G-NEW-15 |
| DA-API-8 | STOPPED keeps its lease (owner idle) | 🟢 | ✅ | §2 — avoids ring churn, immediate resume; cost = one idle renewal. | D-NEW15.1 |
| DA-API-9 | PAUSE flushes to a clean boundary (no at-risk buffer at failover) | 🟡 | ✅ | §2/§4 — resume is fast (warm, no buffer reconcile). | D-NEW15.3 |
| DA-API-10 | Reload must not resurrect a paused/stopped pipeline | 🟡 | ✅ | §4 — runtime `desired_state` wins over config's implicit "should run." | §9.6 |

---

## 17. Operational, deployment & dependencies (OPS / DEP)

**State.** `Operational_and_Scope_Gaps_Batch.md` resolves a batch of smaller gaps (Azure creds,
catalog retry, IAM matrix, MariaDB scope, strip_schema collisions, backpressure-on-sink-failure).
Deployment topology (StatefulSet, headless Service) is fixed by the clustering doc. Dependency
pinning is the remaining cross-cutting concern.

| ID | Title | Sev | Status | Resolution / note | Old ID |
|---|---|---|---|---|---|
| DA-OPS-1 | Azure credentials & container model | 🟢 | ✅ | `Operational…` G-34 — SDK default chain; optional `azure.account_name`; bucket=container. | G-34 |
| DA-OPS-2 | Minimum IAM permissions per provider | 🟢 | ✅ | G-37 — documented S3/GCS/Azure permission sets (runbook prerequisite, no config). | G-37 |
| DA-OPS-3 | `glue.strip_schema` collision handling | 🟢 | ✅ | G-11.3 — collision = config error; non-stripped = `schema__table`. | G-11.3 |
| DA-OPS-4 | Sink/catalog unavailability → DEGRADED back-pressure | 🟡 | ✅ | G-27/G-35 — stop consuming once buffers fill, protect slot/WAL via G-22; recover on next commit; catalog circuit breaker. | G-27, G-35 |
| DA-OPS-5 | k8s deployment as StatefulSet (not Deployment) | 🟡 | ✅ | Clustering §3.4/§10.2 — stable `node_name` via Downward API; headless Service discovery. | D-1, D-4 |
| DA-OPS-6 | "Restart ≠ apply config" operator footgun | 🟢 | ✅(documented) | Clustering §9.3 D-3 — cloud-generation-wins; a pod restart does **not** apply ConfigMap edits, only `POST /reload` does. Document loudly. | D-3 |
| DA-DEP-1 | iceberg-rust pinned version & capabilities | 🟡 | 🧪 | `iceberg` 0.10.1, MSRV 1.94 (main has moved to 1.95, #3083; Sources_Index needs refreshing from 1.92). Multiple capability gates ride on the exact version: DA-SINK-1, DA-SINK-2, DA-SCHEMA-8, DA-TYPE-5, DA-SINK-10. Pin and assert at startup. | — |
| DA-DEP-2 | OpenDAL version exposing conditional writes | 🔴 | 🧪 | Clustering §17.2 — `if_match` on writes landed late-2024/early-2025; the `iceberg-storage-opendal` transitive pin must be ≥ that, and capabilities asserted at startup (refuse to start if absent). Part of DA-CLUS-1. | D-5 |
| DA-DEP-3 | `sqlparser-rs` (MySqlDialect) pin | 🟢 | ✅ | `MySQL_DDL_Classifier.md` §5 — now on a fail-soft path, so a coverage gap degrades rather than halts; de-risked. | G-19a |
| DA-DEP-4 | `memberlist` crate (Lifeguard, TLS/QUIC transport) | 🟢 | ✅ | Sources_Index; Clustering §2 — features confirmed available. | G-23 |

---

## 18. Cross-cutting & newly-surfaced gaps

These are not contained in any single domain — they emerge from reading two or more documents
together and finding a seam. They are the items most worth attention in spec elaboration because no
single research doc "owns" them.

### DA-X-1 — `CdcEvent` is missing the per-change sequence the dedup design requires 🟡 🟥
`Ghost_Row_Dedup_and_Primary_Keys.md` §4 keeps-last by a **per-change** `_seq` (PG: the change's own
WAL LSN; MySQL: `(binlog_file, end_log_pos)` or GTID + intra-txn event index), explicitly *finer* than
the commit position. But `Data-Flow.md`'s `CdcEvent` carries only `source_pos` (commit LSN/GTID) and
`xid`. **The boundary type and the accumulator contract must add a per-change position**, and the
lineage columns (`_op/_ts/_lsn/_source_db/_pipeline`) must decide whether `_seq` is also persisted as a
column (it is referenced as one in the dedup doc but absent from the lineage table). Resolving this is a
prerequisite to specifying both the decoder output and the sink collapse step. *(Links DA-FLOW-2,
DA-SYS-5.)*

### DA-X-2 — The Iceberg snapshot-summary carries three independent CDC properties; their coexistence is assumed, not specified 🟡 🧪
Three separate designs write into the Iceberg snapshot summary: `cdc.owner_epoch` (clustering fence,
D-7), `cdc.committed_position` (delivery guard, D-14.4), and the implicit per-table HWM. Each doc
individually says "verify iceberg-rust preserves custom snapshot-summary properties across commits."
**No doc owns the combined property set or its schema.** Spec elaboration should define one
`cdc.*` snapshot-summary schema and a single verification that all of it survives commits. *(Links
DA-SINK-2, DA-CLUS-3, DA-FLOW-7.)*

### DA-X-3 — Equality-delete partition routing by before-image is a writer-contract obligation referenced from two docs but defined in neither's spec 🟡 🔄
`Iceberg_Partitioning…` §2 (D-24) requires the writer to route an equality-delete to the **old**
partition (from the before-image) when an UPDATE changes a partition value; `Ghost_Row…` §4 requires
the analogous old-key delete for PK mutation. Both are "sink-writer contract additions" with no single
authoritative sink-writer contract document yet. Spec elaboration needs a **consolidated sink-writer
contract** (keying, collapse, partition routing, delete-file sequencing, snapshot-summary properties,
truncate handling) — today it is scattered across five research docs.

### DA-X-4 — Reserved metadata-column collision has no runtime rule 🟡 🟥
*(= DA-SYS-4, restated as cross-cutting.)* If a source table legitimately has a column named `_op`,
`_ts`, `_lsn`, `_source_db`, `_pipeline`, or a future `_seq`, the accumulator injects a colliding
column. Config §5 only says operators "should be aware." Decide the rule: reject/halt the table, or
escape source columns (e.g. double-underscore prefix as in `glue.strip_schema` collisions). Until
decided, the writer behavior is undefined.

### DA-X-5 — `TRUNCATE` sink semantics undefined and possibly engine-asymmetric 🟡 🟥
*(= DA-FLOW-5.)* MySQL's classifier maps TRUNCATE to a "table-clear" but defers the actual Iceberg
operation to "the sink-writer contract"; PG's `T` truncate message is decoded but no sink action is
written down. Define one cross-engine truncate→Iceberg mapping (overwrite-to-empty vs delete-all vs a
snapshot boundary marker) and confirm it is idempotent under replay like everything else.

### DA-X-6 — PK-less semantics interact with dump-only modes 🟡 🟥
*(= DA-SNAP-8.)* "PK-less halts" is justified by equality-delete keying, but `dump_always` /
`dump_initial` tables are insert-only and never delete — so the keying argument does not bind. The
spec should state explicitly whether a dump-only table may be PK-less (snapshot as plain inserts,
`identifier-field-ids` unset) or still halts for uniformity.

### DA-X-7 — MariaDB GTID dialect is the one open correctness dependency in an otherwise "in-scope" claim 🟡 🟥
*(= DA-SRC-8.)* G-38 accepts MariaDB "best-effort via the MySQL path," but the `SourcePosition` MySQL
implementation must handle MariaDB's `domain-server-sequence` GTID, which is a *different* set algebra
from MySQL's `server-uuid:tx`. This is flagged as a new sub-research item and is the single place where
"in scope" hides real work. Either schedule the sub-research or narrow the v1 scope statement.

### DA-X-8 — Three specs disagree on the per-pipeline state path; one canonical layout is needed 🟢 🔄
`Data-Flow.md` roots per-pipeline state at `…/pipelines/<id>/`; `Configuration_Specification.md` §10
and `Operational…` G-NEW-20 remove that segment (state already roots at `sink.prefix`/`state.prefix` =
`id`). Until `Data-Flow.md` is re-rooted, implementers reading the two specs will build different key
layouts. Low severity, high confusion potential. *(Spec-merge debt, §19.)*

### DA-X-9 — "Idempotency is correctness; the position guard is an optimization" is a load-bearing systemic claim worth a single statement 🟢 ✅(but distributed)
The whole exactly-once story (Delivery §1, Large-Txn §1, Snapshot §1, Lifecycle replay) rests on
row-level PK-upsert idempotency being the correctness mechanism, with positions/guards/leases as
optimizations and fences layered on top. This is stated correctly in each doc but never in one place as
*the* system invariant. A one-paragraph "correctness model" at the top of the application spec would
prevent a future contributor from re-introducing a correctness dependency on batch alignment or
position arithmetic.

---

## 19. Spec-merge debt (decided in research, not yet in the authoritative specs)

These are **not open design questions** — every item is decided. They are the gap between the research
corpus and the Oleksii-owned spec docs. Each is a "propose-only" edit the research docs already
flagged. Carrying them across keeps the specs usable as the single source of truth and is the
lowest-risk, highest-leverage work before elaboration. Per project rules, these are proposed, not
applied without instruction.

### 19.1 `Configuration_Specification.md`

> **APPLIED 2026-09-26** — C-1…C-16 promoted into `Configuration_Specification.md` (§15 item 20, rules
> V-094…V-113). Adjusted in the merge: C-3 partitioning is sink-neutral (V-086 per-sink check);
> C-13 `catalog.retry` lands at `sink.iceberg.catalog.retry`; C-14 `sink.maintenance` stays reserved as
> `sink.iceberg.maintenance` / `sink.delta.maintenance`; C-15 at `sink.iceberg.catalog.glue.strip_schema`;
> C-16 verified. Identifier/partition column checks are start-time preconditions (§11.2), not config rules.

| # | Edit | Source |
|---|---|---|
| C-1 | Promote the 10 reserved `globals.cluster.*` fields from §16 into the §6.5 field tables (lease/rebalance/discovery/lifeguard). | Clustering §11/§19.6/§20.4 |
| C-2 | Promote `tables.primary_key` (PR); **remove** `tables.surrogate_key` from §16. | Ghost_Row §7 |
| C-3 | Promote `tables.partitioning` (PR, default none) + its 4 validation rules. | Partitioning §5 |
| C-4 | Promote `snapshot.chunk_size` (R, default 10000, ≥1). | Snapshot §7 |
| C-5 | Promote `sink.max_transaction_batch_rows` (R, null) + `sink.max_transaction_buffer_bytes` (R, ~1GiB) + rules. | Large_Txn §4 |
| C-6 | Promote `sink.buffer_capacity_rows` (R, ~200000). | Operational G-27 |
| C-7 | Promote `source.postgres.max_wal_lag_bytes` (R, null) + floor rule; `source.postgres.slot.on_orphan` and `slot_management` (+ the `require_existing`⇒`auto_cleanup=false` rule). | Operational G-22, Slot_Lifecycle §6 |
| C-8 | Promote `source.mysql.binlog_purge_action` (R, `error`). | Operational G-26 |
| C-9 | **Remove** `source.consistent_snapshot` from §16 (moot — no weaker snapshot mode exists). | Snapshot §3 |
| C-10 | Promote `globals.tracing.otlp_endpoint` + `sample_ratio` (R) + rules; `sink.metrics.per_table` (R, false). | Observability §5 |
| C-11 | Promote `globals.api.role_mapping` + 2 rules; reference the RBAC matrix. | API_RBAC §6 |
| C-12 | Promote `globals.azure.account_name` (R) + rule. | Operational G-34 |
| C-13 | Add optional `catalog.retry { max_attempts, base_backoff, max_backoff }` (R). | Operational G-35 |
| C-14 | Reserve `sink.maintenance` block (disabled in v1). | Maintenance §4 |
| C-15 | Add `glue.strip_schema` + collision-error validation rule. | Operational G-11.3 |
| C-16 | Merge the four Type_Mapping config fields & rules if any remain unmerged (verify V-078..V-082 cover all). | Type_Mapping §Validation |

### 19.2 `Data-Flow.md` (the most-drifted spec)

> **APPLIED 2026-06-20.** All nine landed: DF-1/DF-2/DF-4/DF-5/DF-7/DF-8/DF-9 as edits, DF-3 subsumed by
> DF-1, DF-6 was already correct in `Data-Flow.md` (its residual is a stale note in config §10). Patch
> record: git history (commit on 2026-06-20). The DA-FLOW-2/4/7 and DA-X-1/3 items above are now
> reflected in the spec.

| # | Edit | Source |
|---|---|---|
| DF-1 | Rewrite **Phase 1** from blocking COPY/EXPORT_SNAPSHOT to the stream-first incremental chunk loop (watermarks + PK-upsert reconciliation). | Snapshot §1 |
| DF-2 | Rewrite the **On-Demand Snapshot** section: drop "pause / `pause_lsn + 1`"; use the watermark chunk loop with no stream pause. | Snapshot §4 |
| DF-3 | Remove MySQL `RELOAD`/`LOCK TABLES` from snapshot preconditions. | Snapshot §3 |
| DF-4 | Generalize the `committed_lsn` scalar guard to engine-tagged `cdc.committed_position` (per-table HWM; resume = min/intersect across tables). | Delivery §3/§4 |
| DF-5 | Add the per-change `_seq` to `CdcEvent` and the dedup collapse to Phase 3/4 (mirror-delete semantics, ≤1 delete + ≤1 data per key). | Ghost_Row §4 (see DA-X-1) |
| DF-6 | Re-root per-pipeline state directly under the per-pipeline location; drop the `pipelines/<id>/` segment. | Config §10, Operational G-NEW-20 |
| DF-7 | Add the split-commit rule + checkpoint-pinned-at-txn-start for oversized transactions. | Large_Txn §1 |
| DF-8 | Note pause/stop flush-to-boundary and desired-state reconciliation in the shutdown/handoff section. | Lifecycle §6 |
| DF-9 | State the consolidated **sink-writer contract** (keying, collapse, partition-routing-by-before-image, delete-file sequencing, snapshot-summary properties, truncate). | DA-X-3 |

### 19.3 `System-Overview.md`

| # | Edit | Source |
|---|---|---|
| S-1 | Replace the resolved "Open Questions" block with pointers to the closing docs (most OQ-* are now closed). | this report |
| S-2 | Add the `/v1` API path list + min-role column; annotate lifecycle endpoints with the state machine. | API_RBAC §6, Lifecycle |
| S-3 | Add the observability catalogue + cardinality policy summary. | Observability |
| S-4 | State external-maintenance-for-v1 + the exposed when-needed signals. | Maintenance §5 |
| S-5 | Document the `node_name` sourcing rule + StatefulSet mandate; the "restart ≠ apply config" footgun. | Clustering §3.4/§9.3 |
| S-6 | Fix the typo `worked` → `worker that` (line ~241 region) and drop the dangling `snapshot_mode: only`. | OQ-M1/M2 |

> `Schema_Change_Detection_and_Evolution.md` is already aligned with the config spec (the `method`
> removal and `rescan_interval` rename are reflected in §13). No further schema-doc merge owed.

---

## 20. Open-decisions register (genuinely undecided, 🟥)

The short list of questions spec elaboration must actually *decide* (everything else is resolved or is
merge/verify work). Ordered roughly by leverage.

| ID | Decision | Recommendation / starting position |
|---|---|---|
| DA-OBS-5 | Fix a **numeric end-to-end replication-lag SLA**, and whether failover-time has its own SLA. | Pick a steady-state max-lag (e.g. 60s fits Balanced lease defaults) + a separate, looser failover-time target (~20–30s); load-test to confirm. Grounds lease/flush/checkpoint tuning. |
| DA-X-1 / DA-FLOW-2 | `CdcEvent` per-change sequence field + whether `_seq` is a persisted column. | Add `change_seq: SourceSeq` to `CdcEvent`; persist `_seq` as a 6th lineage column (downstream dedup auditability). |
| DA-X-4 / DA-SYS-4 | Runtime rule for source columns colliding with reserved `_`-names. | Halt the table with a clear error by default; offer a configurable escape-prefix if demand appears. |
| DA-X-5 / DA-FLOW-5 | Cross-engine `TRUNCATE` → Iceberg semantics. | Map to an overwrite-to-empty (or delete-all-by-no-key snapshot boundary); define once, same for PG + MySQL; keep idempotent. |
| DA-X-6 / DA-SNAP-8 | PK-less table under dump-only modes. | Allow dump-only PK-less tables as plain inserts (no `identifier-field-ids`); they never stream/delete so the keying rule doesn't bind. (Or halt for uniformity — decide.) |
| DA-SRC-8 / DA-X-7 | MariaDB GTID dialect support in `SourcePosition`. | Schedule the sub-research; detect MariaDB at connect and select the GTID parser; otherwise narrow v1 scope to MySQL-only. |
| DA-CFG-4 | External secret-manager integration. | Declare out of scope for v1; the file-content-rotation path (sidecar writes the file) is the supported pattern. |
| DA-DEDUP-7 | Cross-table atomic visibility. | Keep out of scope; document the per-table-only guarantee; reopen only if a multi-table-commit catalog (Nessie) becomes a requirement. |
| DA-CLUS-12 | `kubernetes` lock_backend. | Defer; `storage` CAS lease is the v1 backend (already enforced by V-075). |
| DA-CLUS-13 | Per-source-endpoint takeover bulkhead. | Defer; per-node-global cap ships in v1; add per-endpoint group if reconnect storms are observed. |
| DA-SINK-8 | Spill-to-disk for oversized transactions. | Defer to a future `sink.large_txn_mode: spill`; v1 uses split-commits or sized memory. |
| DA-CAT-4 | How the writer consumes catalog-vended credentials. | Specify in the sink-writer contract if credential vending is targeted for v1; otherwise note as future. |

---

## 21. Capability & verification gates (🧪 — must be proven, cannot be closed on paper)

These are the items most likely to invalidate a "design complete" claim. Each depends on a real
behavior of a pinned crate or a cloud service. **Recommendation: stand up the prototypes/gates below
before (or in parallel with) writing the implementation spec, because a negative result changes the
design, not just the code.**

| ID | Gate | Depends on | If it fails |
|---|---|---|---|
| DA-CLUS-1 / DA-DEP-2 ✅ closed 2026-06 | Atomic CAS (create-if-absent, swap-if-version, conditional-delete) on **S3, GCS, Azure** — especially the **GCS generation (not ETag)** mapping. | OpenDAL ≥ late-2024 + per-backend behavior. | The storage lease is unsafe; either drop to native SDKs per backend, or reconsider the lock backend. **Top blocker.** Suite scaffolded at `prototypes/lease-conformance/` (T1–T10); T4/T5/T10 must run against real services. |
| DA-SINK-1 | Equality-delete writer assigns delete files the **enclosing commit's sequence number** | iceberg-rust 0.9.0. **Confirmed ABSENT on 0.9.1/0.10.0/0.10.1 and the v0.11.0-rc.1/rc.2 surfaces (2026-09-19, compiled crate).** | The ghost-row strictly-less-than guarantee breaks; need position-deletes-after-read or a different write strategy. **Blocks the Iceberg mirror only** since the 2026-09-26 multi-sink re-scope (§25). |
| DA-SINK-2 / DA-X-2 | Custom `cdc.*` **snapshot-summary properties preserved across commits** | iceberg-rust 0.9.0. **Append-path mechanism flash-verified on 0.10.1 (2026-09-19): `fast_append().set_snapshot_properties(...)` + catalog reload round-trip in `prototypes/iceberg-capability`.** Full gate (carrier across delete commits) deferred to the delete-commit capability. | `owner_epoch` fence + `committed_position` guard lose their carrier; need a side-channel (e.g. table properties) or an alternate fence. |
| DA-SCHEMA-8 | `drop_column` / `rename_column` / type-promotion transactions supported. | iceberg-rust 0.9.0. **`update_schema` confirmed present on 0.10.1 (2026-09-19); the open question narrowed to drop/rename/promotion extent.** | Route the unsupported op to "halt table" until the crate catches up (graceful, already the fallback). |
| DA-TYPE-5 | `FixedSizeBinary(16)` → Iceberg `uuid` annotation mechanism. | iceberg-rust 0.9.0. | UUID columns fall back to `fixed(16)`/`binary`; cosmetic, low risk. |
| DA-SINK-10 | Compaction / rewrite API availability. | iceberg-rust (future). | Built-in maintenance stays deferred (already external for v1); no v1 impact. |

A single **iceberg-rust capability probe** (one integration test exercising equality-delete
sequencing, snapshot-summary persistence, schema-evolution ops, and the uuid annotation against the
pinned version) would close five of these six at once. The storage-CAS conformance suite closes the
sixth (and the top blocker).

> **Each gate is elaborated in §24 (Appendix A)** — mechanism, exact claim to prove, verification
> plan with pass/fail criteria, design fallback, and redesign-risk impact. §24.0 triages which gates
> can force a redesign (only DA-CLUS-1 and DA-SINK-1) versus which degrade gracefully.

---

## 22. Spec-readiness assessment

**Verdict: the foundation is strong; design is essentially complete and the application specification
can begin now**, provided the work is sequenced so the two 🔴 verification gates are run early (a
negative result there is the only thing that would force a design change rather than a spec edit).

Readiness by area:

- **Ready to specify immediately:** configuration model, catalog auth, RBAC/API, schema-evolution
  detection/apply, type mapping, snapshot model, dedup/keying, clustering protocol, lifecycle state
  machine, observability, source-lifecycle. All have a closing research doc.
- **Specify against a verification gate:** the sink writer (DA-SINK-1, DA-SINK-2) and the storage
  lease (DA-CLUS-1). Write the contract, but treat the crate/cloud behavior as an assumption to be
  proven, with the documented fallback paths in the spec.
- **Decide first (small, bounded):** the ~12 items in §20 — most have a clear recommended default
  already; a half-day of decisions unblocks them.
- **Mechanical, do anytime:** the §19 spec-merge debt — best done before elaboration so the specs are
  internally consistent when the spec author reads them.

Suggested sequence: (1) run the two verification gates / probes; (2) make the §20 decisions; (3) apply
the §19 merges to the specs; (4) write the consolidated sink-writer contract (DA-X-3); (5) proceed to
full application specification.

---

## 23. Document inventory & authority

| Document | Role | Authoritative for | Edit policy |
|---|---|---|---|
| `specs/System-Overview.md` | spec | top-level requirements, scope | Oleksii owns; propose-only |
| `specs/Data-Flow.md` | spec | pipeline stages, CdcEvent, state layout | Oleksii owns; propose-only (most merge debt, §19.2) |
| `specs/Configuration_Specification.md` | spec | config model, validation, mutability | Oleksii owns; propose-only |
| `specs/Schema_Change_Detection_and_Evolution.md` | spec | detection channels, DDL classification, apply | reviewed & approved |
| `research/Type_Mapping.md` | research | three-hop type mapping | Claude authors |
| `research/Catalog_Auth_Research.md` | research | catalog auth schema | Claude authors |
| `research/Clustering_Architecture_Research.md` | research | two-plane clustering, lease, propagation | Claude authors |
| `research/Snapshot_Resumability_and_Consistency.md` | research | incremental chunked snapshot | Claude authors |
| `research/Ghost_Row_Dedup_and_Primary_Keys.md` | research | equality-delete keying, PK policy | Claude authors |
| `research/Delivery_Guarantee_and_Position_Guard.md` | research | SourcePosition, committed_position | Claude authors |
| `research/Pipeline_Lifecycle_State_Machine.md` | research | pause/stop/resume/restart/flush | Claude authors |
| `research/Postgres_Slot_and_Publication_Lifecycle.md` | research | PG slot/publication runtime lifecycle | Claude authors |
| `research/MySQL_DDL_Classifier.md` | research | channel-B classifier | Claude authors |
| `research/Iceberg_Maintenance.md` | research | maintenance ownership + signals | Claude authors |
| `research/Iceberg_Partitioning_and_Layout_Evolution.md` | research | partitioning, MOR↔COW change | Claude authors |
| `research/Large_Transaction_Handling.md` | research | split commits, memory backstop | Claude authors |
| `research/Observability_Metrics_and_Tracing.md` | research | metrics catalogue, tracing | Claude authors |
| `research/Operational_and_Scope_Gaps_Batch.md` | research | WAL/binlog guards, Azure, MariaDB, IAM | Claude authors |
| `research/API_RBAC_and_Versioning.md` | research | RBAC matrix, versioning | Claude authors |
| `sources/Sources_Index_v3.md` | reference | 50-source index, crate versions | read-only |
| `analysis/CDC_Gap_Analysis_Report.md` | analysis | legacy v9 tracker | retained as audit trail |
| `analysis/CDC_Design_Gap_Analysis_v1.md` | analysis | **this fresh re-derivation** | Claude maintains |

### Cross-reference: this report's IDs ↔ legacy IDs

The *(parenthetical)* legacy IDs in every gap table above are the bridge back to the v9 tracker. Notable
legacy blockers and their current status here: `G-8`→DA-CLUS-2 ✅, `G-9`→DA-CLUS-5 ✅, `G-13`→DA-DEDUP-1
✅ (+ DA-SINK-1 🧪), `G-14`→DA-DEDUP-2 ✅, `G-6`→DA-SNAP-1 ✅, `G-15`→DA-SRC-1..4 ✅, `G-16`→DA-API-1 ✅,
`G-18`→DA-TYPE-1 ✅, `G-19/19a/19b`→DA-SCHEMA-* ✅, `D-5`→DA-CLUS-1 🧪 (still the top blocker),
`D-6`→DA-CLUS-11 ✅. The legacy "remaining blockers" list (D-5, D-6, G-13/G-30, G-14, G-6, G-11, G-15,
G-16, G-NEW-20/21/22) is now **all resolved on paper except D-5 (DA-CLUS-1)** — plus the newly-explicit
crate gate DA-SINK-1, which the legacy report folded into G-13's implementation note.

---

## 24. Appendix A — Blocker & capability-gate deep-dive (verification + fallback)

This appendix elaborates the six items in §21 that cannot be closed on paper. For each: the
**mechanism** it underpins, the **exact claim** that must be true, a concrete **verification plan**
(harness + pass/fail), the **design fallback** if it proves false, and the **redesign-risk impact**.

### 24.0 Triage — only two of the six can force a redesign

The six gates are not equal. Two of them, if they fail, invalidate a core design choice and force a
different architecture (🔴). The other four have a benign fallback that is *already part of the
agreed design*, so a negative result degrades a feature or a defense layer without reopening a
decision (🧪).

| Gate | DA-ID | If it fails… | Redesign? |
|---|---|---|---|
| Storage CAS conformance | DA-CLUS-1 | the storage lease is unsafe (esp. silent on GCS) | **Yes → native-SDK lease path** 🔴 |
| Equality-delete sequencing | DA-SINK-1 | the MOR ghost-row guarantee breaks | **Yes → position-delete or COW writer** 🔴 |
| Snapshot-summary persistence | DA-SINK-2 | lose the sink-side fence + dedup optimization | No — idempotency is correctness; fence is defense-in-depth 🧪 |
| Schema-evolution ops | DA-SCHEMA-8 | unsupported op can't auto-apply | No — halt-the-table is already the safety branch 🧪 |
| uuid annotation | DA-TYPE-5 | uuid stored as `fixed(16)`/`string` | No — cosmetic logical-type loss 🧪 |
| Compaction API | DA-SINK-10 | no built-in maintenance | No — external maintenance is the v1 plan 🧪 |

The practical consequence: **focus verification effort on DA-CLUS-1 and DA-SINK-1.** The other four
are worth a single confirmatory test each, but their failure paths are already designed.

---

### 24.1 DA-CLUS-1 — Atomic compare-and-swap conformance on S3 / GCS / Azure 🔴

**Mechanism.** The entire single-writer safety guarantee (`Clustering…` §5) is a per-pipeline lease
object mutated only by atomic compare-and-swap. Everything reduces to three operations on one object:
**acquire** = create-if-absent, **renew/takeover** = swap-if-version-matches, **release** =
conditional delete.

**Exact claim (per backend).**

| Backend | Create-if-absent | CAS update | Version token | Conflict code |
|---|---|---|---|---|
| S3 | `If-None-Match: *` | `If-Match: <etag>` | **ETag** | 412 (sometimes 409 `ConditionalRequestConflict` → retry) |
| GCS | `x-goog-if-generation-match: 0` | `x-goog-if-generation-match: <generation>` | **generation (int64), NOT ETag** | 412 |
| Azure Blob | `If-None-Match: *` | `If-Match: <etag>` | **ETag** | 412 |

The load-bearing risk is the **GCS trap**: GCS honors ETag-match only on *reads*; object-modifying
writes must use the **generation** precondition. An abstraction that feeds the GCS read-ETag into a
"write if-match" silently performs **no CAS** and breaks single-writer on GCS with no error.

**Verification plan.** The `LeaseStore` trait + the T1–T10 conformance suite already scaffolded at
`prototypes/lease-conformance/`:

- T1–T3, T6–T8: functional acquire/swap/release happy paths (emulators OK: MinIO / fake-gcs-server / Azurite).
- **T4** swap-with-stale-version → Conflict, and **T5** N concurrent swaps on the same version →
  exactly one Ok — these two *prove* "at most one writer."
- **T9** S3 `409 ConditionalRequestConflict` surfaces as a retryable Conflict (not a hard error).
- **T10** the token fed back into `swap` on GCS is the **generation**, not the read ETag — the gating
  assertion for the trap.
- Startup guard: assert `op.info().full_capability()` advertises `write_with_if_not_exists` and
  `write_with_if_match`; **refuse to start** if a backend build lacks them. (Real-S3 finding: S3 reports
  `write_with_if_none_match = false` — create-if-absent is the dedicated `if_not_exists` capability; and
  OpenDAL has no conditional delete, so release tombstones via `write_with(...).if_match(...)`.)

**Pass/fail.** T4, T5, T10 **must pass against the real services** (not just emulators — emulators
implement preconditions loosely). T10 failing means the OpenDAL→GCS mapping is wrong and the storage
lease is unsafe on GCS. Pin OpenDAL ≥ the release where `if_match`-on-writes landed (late-2024 /
early-2025).

**Fallback.** The `LeaseStore` trait already isolates this. If OpenDAL lacks a capability or mis-maps a
backend, **drop that backend to its native SDK** behind the same trait (Route B, `Clustering…` §17.3):
`aws-sdk-s3` (`if_none_match`/`if_match`), the GCS SDK (`if_generation_match`), `azure_storage_blob`
(`If-None-Match`/`If-Match`). This is a per-backend escape hatch, not a redesign of the lease protocol.
The protocol itself (acquire/renew/takeover, §5.3) is unchanged. The optional `kubernetes` lock_backend
(D-2) remains a separate future `LeaseStore` impl for k8s.

**Impact.** 🔴 because a *silent* failure on GCS would corrupt single-owner safety in production. But
the cost of the fallback is bounded (native SDK for one backend), so it is a "must verify," not a
"design may collapse."

**S3 run outcome (2026-06) — PROVEN.** T1–T9 pass against real AWS S3, on both versioning-enabled and
non-versioned buckets. Two findings were folded back into the harness and the clustering doc §17.1/§17.2:
(1) create-if-absent is OpenDAL's **`if_not_exists`** capability — S3 reports `write_with_if_none_match =
false`; (2) the `if_match` CAS token must be the **ETag** on S3/Azure (the **generation** on GCS) — a
versioning-enabled bucket populates `version()` with the version-id, which `If-Match` rejects, so the
token kind is fixed by backend. Bucket versioning is **not required** (best kept off for the lease prefix).

**GCS run outcome (2026-06) — Route A does NOT work on GCS (the trap, confirmed early).** OpenDAL's GCS
backend reports **`write_with_if_match = false`** — the conformance suite fails at the *static* capability
assertion, before any network/auth call (so it is **not** a credentials or bucket-access problem). Root
cause: GCS write preconditions are **generation-based** (`ifGenerationMatch`), not ETag/`If-Match`; OpenDAL
exposes the generation only as `if_not_exists` (=0) for *create* and has no arbitrary generation-match
write option, so it cannot express the lease's **CAS update** (renew/takeover) on GCS. **Resolution
(the documented escape hatch fires): implement the GCS `LeaseStore` over the native GCS SDK**
(`google-cloud-storage`, `ifGenerationMatch` for both create=0 and swap=N; token = generation), behind the
same trait. Route A (OpenDAL) covers **S3 + Azure**; **GCS = Route B**. Net design change: `lock_backend:
storage` uses OpenDAL for S3/Azure and the native GCS SDK for GCS — a per-backend `LeaseStore` impl, the
protocol (§5.3) unchanged.

**GCS run outcome (2026-06) — PROVEN (Route B).** The native-SDK `GcsLeaseStore`
(`prototypes/lease-conformance/` `--features gcs`, `ifGenerationMatch`) passes **T1–T10 against real GCS,
including T10 — the generation-CAS gate** (a stale generation is correctly rejected). This was the single
riskiest assumption in the lease design; it now holds with a native SDK.

> **SDK-port note (2026-06) — re-verified.** GCS was first proven on yoshidan's `google-cloud-storage`
> 0.24, then migrated to the **official `google-cloud-storage` 1.x** (`googleapis/google-cloud-rust`:
> `Storage::builder()` / `write_object().set_if_generation_match()` / `read_object()`), with `reqwest`
> bumped to 0.13. **T1–T10 re-run green against real GCS on 1.15**, so the port is confirmed (incl. the
> t10 generation-CAS gate). 1.x quirk recorded: a failed `ifGenerationMatch` surfaces as `code UNKNOWN`
> ("…pre-conditions you specified did not hold"), not a clean 412, so the conflict check matches that text.

**Azure run outcome (2026-06) — OpenDAL 0.57 azblob ALSO lacks `write_with_if_match`.** The printed
matrix is `if_not_exists=true if_match=false if_none_match=true`, so OpenDAL can't do the lease's CAS
update on Azure either — the same gap class as GCS, but here it is an **OpenDAL limitation, not an Azure
one** (Azure Blob natively supports `If-Match` conditional PUT; OpenDAL 0.57 just hasn't wired it into
the azblob write path). The project is pinned to OpenDAL 0.57 (shared with the iceberg sink), so bumping
is not an option. Net: **on 0.57, Route A (OpenDAL) covers S3 only; both GCS and Azure use native Route
B.** Azure is now implemented as a native `AzureLeaseStore` (`--features azure`): **raw Blob REST over
`reqwest` + SAS**, `If-None-Match: *` (create) / `If-Match: <etag>` (CAS), ETag token — same trait, same
T1–T10 suite. (Raw REST + SAS was chosen over the Azure SDK to avoid SDK-API churn and request signing.)

**Status: CLOSED 2026-06 — all three backends proven against real services.** Azure native Route B
(`--features azure`, Blob REST + SAS) passes T1–T9 against a real Azure container; S3 (T1–T9, versioned +
non-versioned) and GCS (T1–T10, incl. the generation-CAS gate) already proven. DA-CLUS-1 is closed; the
`prototypes/lease-conformance/` suite is the durable regression proof for all three backends.

> **Architecture note (lock_backend: storage).** The portable storage lease is now a **per-backend
> `LeaseStore`**: OpenDAL for S3, native `google-cloud-storage` for GCS, native Blob-REST for Azure —
> all behind one trait, the §5.3 protocol unchanged. "OpenDAL as the single abstraction for all three"
> did not survive contact with real services; the trait-with-per-backend-impls did.

---

### 24.2 DA-SINK-1 — Equality-delete files carry the enclosing commit's sequence number 🔴

**Mechanism.** An Iceberg equality delete applies **only to data files whose data-sequence-number is
strictly less than the delete file's** (`Ghost_Row…` §0). This one property makes the in-batch upsert
correct: a "delete old key + insert new key" written in the *same* commit shares one sequence number,
so the delete purges every **prior-commit** version of the key while sparing the **same-commit**
insert. If the writer mis-sequences — gives the same-commit insert a lower sequence than its delete —
the row that must survive is purged, or (the documented ghost-row bug) a row that must disappear
survives forever.

**Exact claim.** iceberg-rust 0.9.0's equality-delete writer assigns delete files **the enclosing
commit's data-sequence-number** (equal to the data files committed in the same transaction), and the
strictly-less-than rule holds on read.

**Verification plan.** A focused integration test against the *pinned* crate:

1. Create a MOR table with `identifier-field-ids` set.
2. Commit C1: insert key `K=1` (data seq `s1`).
3. Commit C2 (single transaction): equality-delete `K=1` **and** insert `K=1` with a new image.
4. **Assert (read):** the table shows exactly the C2 image of `K=1` — the same-commit delete did **not**
   remove the same-commit insert, and it **did** remove C1's version.
5. **Assert (metadata):** the C2 delete file's sequence number equals C2's data sequence number.
6. **Cross-read with a mature engine** (Spark or Trino Iceberg) to confirm the on-disk result, not just
   iceberg-rust's own reader — the two must agree.
7. Ordering test: insert/delete of the same key spread across three commits resolves to the last op.

Suggested home: a new `prototypes/iceberg-capability/` test crate, pinned to the exact iceberg-rust
version, parallel to `prototypes/lease-conformance/`.

**Pass/fail.** Step 4 + step 6 are the gate. If the same-commit insert is purged, or a Spark read
disagrees with iceberg-rust's, the equality-delete path is unusable as designed.

**Fallback (in priority order).**

1. **Position deletes after a read.** For each affected key, read the data file(s), compute row
   positions, write position-delete files. Correct, but adds a read per affected file and is
   partition-scoped — materially costlier on write, and changes the §4 collapse contract.
2. **COW for affected tables.** Read-modify-rewrite whole data files (`Ghost_Row…` §5 already notes COW
   carries the same identifier rules). Highest write cost; reserved as the heavy fallback.
3. **Manual sequence assignment** if the crate exposes per-file sequence control — keep equality
   deletes but set the sequence explicitly.
4. **Pin/upgrade or contribute upstream** to a crate version that sequences correctly.

**Impact.** 🔴 and the single most consequential sink gate: a failure cascades into `Ghost_Row…`
(keying/collapse), `Iceberg_Partitioning…` (before-image partition routing of deletes),
`Iceberg_Maintenance…` (delete-file compaction), and `Delivery…` (the per-table guard rides the same
writer). All of those assume equality deletes work. The position-delete fallback keeps correctness but
is a real writer redesign — hence verify **before** the sink writer is specified in detail.

---

#### 24.2.1 RESULT (2026-06) — 🔴 **iceberg-rust 0.9.1 cannot commit deletes at all. This is worse than a sequencing bug.**

The probe was answered by reading the pinned crate's public API (`iceberg` 0.9.1, the latest 0.9). The
question is no longer "does the equality-delete writer sequence correctly" — it is "**can deletes be
committed at all?**", and the answer is **no**:

- **The writer exists, the commit path does not.** `iceberg::writer::base_writer` does expose
  `EqualityDeleteFileWriter`, `PositionDeleteFileWriter`, and `DataFileWriter` — so delete files can be
  *written*. But the **`Transaction` commit API exposes only**: `fast_append()` (→ `add_data_files(...)`,
  **data files only**), `update_table_properties`, `replace_sort_order`, `update_location`,
  `update_statistics`, `upgrade_table_version`, `commit`. There is **no row-delta / overwrite / rewrite
  / delete-commit action, and no `update_schema`.** The transaction module contains only
  `Transaction`, `ActionCommit`, and the `ApplyTransactionAction` trait. The doc's own example states
  fast-append "will not rewrite or append to existing metadata."
- **Consequence.** iceberg-rust 0.9.1's public write API is **append-only**. You can ingest data files;
  you **cannot commit equality-delete files, position-delete files, overwrites, or file rewrites.**
  The MOR-equality-delete sink (the system default) is **not implementable** on 0.9.1 — and the **COW
  fallback is equally blocked**, because COW also needs an overwrite/replace-files commit, which is
  absent. A CDC sink whose entire purpose is to apply UPDATEs and DELETEs cannot be built on this crate
  version through the public API. This is the position-delete/COW fallback from the list above being
  *unavailable*, not just costly.

**Options (all require a decision before the sink is specced):**

1. **Track iceberg-rust and pin a version that adds delete/overwrite commits.** The `fast_append`
   naming and the active write-path work upstream imply row-delta/MOR commit support is coming; this is
   the lowest-friction path but is **schedule-dependent on an external project**. Make the sink spec
   explicitly gated on "iceberg-rust ≥ the release that lands delete commits," and re-run this probe then.
2. **Drop to a lower-level commit** — assemble the snapshot/manifest with delete files via iceberg-rust's
   spec types directly (below the `Transaction` convenience API), if those internals are public enough.
   High effort and fragile across versions; essentially re-implementing the delete-commit action.
3. **Bridge the commit to another Iceberg implementation** (PyIceberg / Java) for the delete/upsert
   commit while keeping the Rust capture/decode/accumulate path. Contradicts the "Rust-native, no JVM"
   design tenet; heavy operationally.
4. **Contribute the delete-commit (row-delta) action upstream** to iceberg-rust. Most aligned with the
   design, real engineering + review-cycle cost, benefits the ecosystem.
5. **Restrict v1 to append-only** (snapshot/insert-only tables) — **not viable** for a CDC system that
   must mirror updates and deletes; listed only to reject it.

**This finding outranks the original DA-SINK-1 framing and should headline the sink section of the
application spec:** the sink's core write strategy (MOR equality deletes, and its COW fallback) is **not
buildable on the pinned iceberg-rust**, so the spec must either gate on a future crate version (option 1)
or commit to options 2–4. The capture→decode→accumulate pipeline is unaffected; the gap is entirely at
the Iceberg *commit* boundary. The probe (`prototypes/iceberg-capability/`) records this; re-run it
against any newer iceberg-rust to detect when delete commits land.

> **DECISION (2026-06) — Option 1: track iceberg-rust; gate the sink spec on a delete-commit-capable
> release.** The sink writer's detailed spec is **deferred** until iceberg-rust ships a delete-aware
> commit action (row-delta / overwrite / merge-append). Capture/decode/accumulate spec work may
> proceed; the *commit* layer waits. **Revisit ~2026-07-21**: re-check the iceberg-rust `Transaction`
> API (and the `iceberg-capability` canary) for a delete-commit action. A scheduled reminder is set.
> If, by the revisit, no delete-commit support has landed and the timeline matters, escalate to
> option 4 (contribute upstream) or 2 (low-level commit).

> **Knock-on:** the same "no `update_schema` / no rewrite" gap means **DA-SCHEMA-8** (schema evolution
> via transaction) and **DA-SINK-10** (compaction/rewrite) are *also* absent from 0.9.1's public API —
> see §24.4 and §24.6. DA-SINK-2 (snapshot-summary properties) is moot until a delete-commit path exists.

---

#### 24.2.2 RECHECK (2026-07-22) — still 🔴 **no delete-commit action; `iceberg` 0.10.0 checked**

Scheduled recheck per the 2026-06 decision's "revisit ~2026-07-21" trigger. Latest published crate at
recheck time: **`iceberg` 0.10.0** (crates.io `max_stable_version`; published 2026-07-21, one day before
this recheck — so this is a fresh release, not a stale read).

**`Transaction` API in 0.10.0** (`docs.rs/iceberg/0.10.0/iceberg/transaction/struct.Transaction.html`):
`new`, `upgrade_table_version`, `update_table_properties`, `update_schema`, `fast_append`,
`replace_sort_order`, `update_location`, `update_statistics`, **`expire_snapshots`** (new in this
release), `commit`. `fast_append` still only exposes `add_data_files(...)` — no `row_delta`,
`overwrite`, `rewrite_files`, or `merge_append` action exists anywhere on `Transaction`.

**Result: NO — delete-commit support has not landed.** The MOR-equality-delete sink (and its COW
fallback) remain un-buildable on the public `Transaction` API for the same reason recorded in §24.2.1.
The one addition since the June check is `expire_snapshots` (a maintenance action — relevant to
DA-SINK-10/`Iceberg_Maintenance…`, not to DA-SINK-1).

**Correction to §24.2.1:** `update_schema` is *not* missing — it was present on `Transaction` in 0.9.1
at recheck time (and remains in 0.10.0). The June finding that "no `update_schema`" existed appears to
have been an error in that pass (possibly checking against `rust.iceberg.apache.org`'s
then-lagging docs mirror rather than the published crate); DA-SCHEMA-8's knock-on framing in §24.2.1
and §24.4 should be read as "unverified pending the DA-SCHEMA-8 probe," not "absent from the API."

**Action taken:** `prototypes/iceberg-capability/Cargo.toml` bumped `iceberg` from `"0.9"` to `"0.10"`.
Could not run `cargo build -p iceberg-capability --features probe` as a canary in this session — no
Rust toolchain available in the execution sandbox. **Recommend running that build locally** before
relying on the bump; the probe bodies are still `todo!()` scaffolds either way (§24.2's verification
plan / G1 probe), so a green build only confirms the dependency bump resolves, not that the delete
sequencing behavior holds.

**Decision unchanged:** still tracking iceberg-rust (option 1). No new revisit date is fixed by this
recheck; re-run when the next iceberg-rust minor ships, or sooner if upstream issue/PR activity signals
a row-delta or overwrite commit action landing.

---

#### 24.2.3 RECHECK (2026-08-23) — still 🔴 **no delete-commit action; `iceberg` 0.10.1 checked; upstream CoW work spotted**

Automated recheck (scheduled task `iceberg-delete-commit-recheck`). Latest published crate at recheck
time: **`iceberg` 0.10.1** (crates.io `max_stable_version`; published 2026-08-01, three weeks before
this recheck).

**`Transaction` API in 0.10.1** (`docs.rs/iceberg/0.10.1/iceberg/transaction/struct.Transaction.html`,
cross-checked against the crate's own `src/transaction/mod.rs` source on docs.rs): `new`,
`upgrade_table_version`, `update_table_properties`, `update_schema`, `fast_append`,
`replace_sort_order`, `update_location`, `update_statistics`, `expire_snapshots`, `commit` — unchanged
from the 0.10.0 set in §24.2.2. `fast_append` still only exposes `add_data_files(...)`. No `row_delta`,
`overwrite`, `rewrite_files`, or `merge_append` method exists on `Transaction`, and the `transaction`
module's public struct list (`ActionCommit`, `AddColumn`, `Transaction`) has no `OverwriteAction` or
`RowDeltaAction` type exported.

**Result: NO — delete-commit support has still not landed in a published release.** The
MOR-equality-delete sink (and its COW fallback) remain un-buildable on the public `Transaction` API,
same root cause as §24.2.1/§24.2.2.

**New signal worth tracking closely.** ⚠️ *Superseded 2026-09-19 — the merge claims below were
upstream-reported and are RETRACTED by direct verification (§24.2.4): PR #2185 is open/unmerged,
and no delete-aware action exists on `main` or in the v0.11.0-rc.* releases. Kept as the historical
record.* `apache/iceberg-rust` issue **#2186** ("Copy-on-Write and Merge-on-Read support", opened
2026-02-27) is an epic explicitly tracking this gap upstream:

- Copy-on-Write: **`OverwriteAction` (PR #2185, "add OverwriteAction with CoW delete support" —
  overwrite semantics with manifest rewriting for deleted entries) is checked off as merged** in the
  epic's task list. Conflict detection, filter-based overwrite, and rewrite-based compaction are still
  open (unchecked).
- Merge-on-Read: `RowDeltaAction` (position/equality delete file write path), scan-side delete
  reconciliation, and V3 Deletion Vectors are **all unchecked / not started.**

Despite `OverwriteAction` being marked merged in the epic, it does **not** appear anywhere in the
0.10.1 public API checked above — no `overwrite()` method on `Transaction`, no `OverwriteAction` struct
in the `transaction` module docs, and no such module in the `src/transaction/mod.rs` source. Most
likely explanation: merged to `main` after the 0.10.1 release cut (2026-08-01) and not yet published to
crates.io, or not yet wired into the public `Transaction` surface even on `main`. This could not be
confirmed further without a GitHub API/commit-date check beyond what was available this session — treat
the merge status as **upstream-reported, not independently confirmed against a tagged release.**

**Practical read:** if `OverwriteAction` does ship in the next `iceberg` release, it would very plausibly
unblock **option 2 from §24.2.1 — the COW fallback** (whole-file overwrite/replace), since "overwrite
semantics with manifest rewriting for deleted entries" is exactly the commit primitive COW needs. It
would **not** by itself unblock the MOR-equality-delete default path, which needs `RowDeltaAction`
(still unstarted per the epic). This raises the value of re-checking on a **shorter cadence** than "next
minor" — the next `iceberg` patch/minor release is worth checking specifically for `OverwriteAction`
landing, independent of `RowDeltaAction` progress.

**Action taken:** none to `prototypes/iceberg-capability/Cargo.toml` — its existing dependency pin
(`version = "0.10"`, i.e. `^0.10`) already resolves to 0.10.1, so no bump was needed for the version
requirement itself; only the comment's "latest checked" note would go stale, not the resolution. Did
**not** attempt `cargo build -p iceberg-capability --features probe` — no Rust toolchain available in
this session's execution sandbox (same limitation as §24.2.2). **Recommend running that build locally**
before relying on the pin, and recommend implementing the `iceberg-capability` G1 probe (equality-delete
same-commit sequencing, §24.2's verification plan) once a delete-aware commit action is confirmed
available, per the scheduled task's standing instruction.

**Decision unchanged:** still tracking iceberg-rust (option 1 in §24.2.1). Recommend the next recheck
trigger on **either** the next `iceberg` crate release **or** direct confirmation (via the GitHub repo)
that `OverwriteAction`/`RowDeltaAction` landed in a tagged version — whichever comes first — rather than
waiting a full minor-release cycle as before.

---

#### 24.2.4 RECHECK (2026-09-19) — still 🔴 **no delete-commit action in any release, incl. v0.11.0 RCs; §24.2.3's "OverwriteAction merged" lead retracted; G2 flash-verified on the append path**

This session is the **first recheck with a Rust toolchain available**, so the finding is now backed by
the **compiled crate** (plus the repo tags), not docs.rs alone. `prototypes/iceberg-capability`
`Cargo.lock` was bumped 0.9.1 → 0.10.1, `cargo build --features probe` **compiles green**, and the
append-path probe **passes**.

**The delete-commit question, checked three independent ways — still NO:**

1. **Crates.io latest remains `iceberg` 0.10.1** (published 2026-08-01; rust-version **1.94**). No
   0.11.0 published; RCs are GitHub tags only.
2. **Compiled 0.10.1 source** (`~/.cargo/registry/…/iceberg-0.10.1/src/transaction/`): `Transaction`
   exposes `new`, `upgrade_table_version`, `update_table_properties`, `update_schema`, `fast_append`,
   `replace_sort_order`, `update_location`, `update_statistics`, `expire_snapshots`, `commit` — same
   set as §24.2.2/§24.2.3. `FastAppendAction` accepts **data files only** and produces an
   `Operation::Append` snapshot with **no delete entries** (`delete_entries` → empty; the §24.2.3
   append.md source confirms a delete-only-manifest-carry-forward fix #2148, but that is a preservation
   fix, not a delete-commit action). Optional **snapshot-summary properties are supported**
   (`set_snapshot_properties`, §24.2.4 G2 below).
3. **v0.11.0-rc.1 / v0.11.0-rc.2** (git tags, cut after the "Bump version to 0.11.0" commit of
   2026-09-04; MSRV raised to 1.95 per #3083): their own `src/transaction/mod.rs` reads the **same
   append-only method set**. **Conclusion: the imminent 0.11.0 will NOT unblock the sink.**
   The next trigger is a PR merge, not a release.

**Retraction — §24.2.3's "OverwriteAction merged" was wrong.** apache/iceberg-rust **PR #2185**
("feat(transaction): add OverwriteAction with CoW delete support") is **open, unmerged** (GitHub API,
2026-09-19: `merged: False`). The epic #2186's task-list checkbox was inaccurate/optimistic. Lesson
recorded: **an epic checkbox is never merge proof; the PR's own `merged` state is the source of truth.**
The §24.2.3 "upstream-reported, not independently confirmed" caveat was therefore right to keep — and
the independent confirmation now refutes the report.

**RowDelta work exists; none merged.**
`#2203` (RowDelta, the original MoR action), **open** since 2026-03, last active 2026-08-31 (594 insertions, 5 files).
`#2785` (RowDeltaAction: commit data + delete files in one snapshot) and `#2678` (RowDelta deletion
vectors) both **closed unmerged** (2026-09-02) — superseded in favour of the new base work below.
**`#3046` "MergingSnapshotProducer and RewriteFilesAction"** (open, 861 insertions, updated
2026-09-18) — the author states it is *the first in a series* to add compaction/rewrite support
(issue #1607); **follow-ups will add OverwriteFiles, RowDelta, and conflict detection**. This is the
foundation any delete-aware `Transaction` action must sit on, so it is the single most important PR
to watch. Related: `#2752` (core COW rewrite primitive, open) and RFC **`#2620`** "Stateful
Transaction Action" (opened 2026-09-19 — may re-shape the whole action API). MoR *read*-side
groundwork landed for the 0.11.0 window (V3 deletion-vector scan PRs [2/N]/[3/N], `_row_id`
synthesis on read, #3058) — read-side only, not a write/commit capability.

**G2 (DA-SINK-2) flash-verified on the append path — de-risked.**
`prototypes/iceberg-capability` now has its first **real passing probe**
(`tests/probe_append_and_summary.rs`): MemoryCatalog + two `fast_append` commits. Findings:
(a) data files **and** arbitrary `cdc.*` snapshot-summary properties commit **atomically** in one
`Transaction`; (b) they read back from the **catalog-reloaded** table, i.e. they survive the metadata
commit + reload; (c) a second commit re-asserting a monotonic `cdc.owner_epoch` lands on the new
snapshot while the first snapshot keeps its old value. **This is exactly the carrier + monotonic-read
semantics the clustering fence (D-7) requires** — on the one commit path that exists today. The full
DA-SINK-2 gate ("does the carrier survive *delete* commits too") remains deferred to the
delete-commit capability, but the mechanism exists and §24.3's fallbacks are unchanged.

**Other gates touched by 0.10.1:** `update_schema` **confirmed present** (G3's concern narrows to the
drop/rename/type-promotion *extent* — DA-SCHEMA-8 stays 🧪 until probed); `expire_snapshots` exists
(maintenance — positive signal for DA-SINK-10's future built-in path, still external for v1).

**Decision unchanged: option 1 (track iceberg-rust; gate the sink spec on a delete-commit-capable
release).** Revised re-check trigger: watch **#3046 merge on `main`** and then its promised
OverwriteFiles/RowDelta follow-ups appearing on the `Transaction` surface; validate in a **tagged
release** before trusting it (0.11.0-RCs prove a tag can lag a merge, and a merge can lag a release —
both directions of lag observed). The scheduled-recheck cadence stays in effect.

---

### 24.3 DA-SINK-2 — Custom `cdc.*` snapshot-summary properties persist across commits 🧪

**Mechanism.** Two orthogonal sink-side mechanisms live in the Iceberg snapshot summary string map:
`cdc.owner_epoch` (the clustering zombie-fence, D-7) and `cdc.committed_position` (the delivery
guard / per-table durable HWM, D-14.4). On each commit the writer reads the latest snapshot's summary,
checks `latest_committed_epoch`/`committed_position`, and writes the updated values into the new
snapshot's summary. Iceberg snapshot summaries are **per-snapshot**, so the writer must *re-assert*
these on every commit (they are not auto-inherited).

**Exact claim.** iceberg-rust lets the writer (a) read the latest snapshot's summary before committing
and (b) set arbitrary string properties on the snapshot it creates, atomically with the data commit.

**Verification plan.** In `prototypes/iceberg-capability/`:

1. Commit with `cdc.owner_epoch="5"`, `cdc.committed_position="0/1A2B3C4"`; read them back from the
   snapshot summary.
2. Commit again with `owner_epoch="6"`; assert the monotonic read (`latest > mine → abort`) is
   answerable from the summary.
3. Confirm a commit that does **not** set the property does not silently strand the running value —
   i.e. the writer can read the prior summary and carry it forward (since summaries don't inherit).

**Pass/fail.** If custom summary properties are dropped or unreadable across commits, the in-summary
fence/guard has no carrier.

**Fallback.**

1. **Iceberg table properties** instead of the snapshot summary — but confirm the property update
   commits **atomically with the data files** (it must be the same metadata commit to be a real fence,
   not a separate write).
2. **Drop `owner_epoch` to advisory.** The design already states it is **defense-in-depth, not a
   correctness prerequisite** (`Clustering…` §5.5): the metadata-pointer CAS (layer 2) + `_lsn`/PK-upsert
   idempotency (layer 4) preserve correctness without it. Losing the sink-side fence only re-widens the
   rare "zombie commits in the gap before the new owner's first commit" window, which idempotency
   absorbs.
3. **Move `committed_position` to per-table state storage** (the per-pipeline `state` location) instead
   of the snapshot summary. Since `Delivery…` §1 demotes the guard to an *optimization* (idempotency is
   the correctness basis, D-14.3), a state-storage HWM is a fully adequate home; the only loss is reading
   the durable position straight from the table.

**Impact.** 🧪 not 🔴 — correctness is anchored in idempotency, so every fallback degrades a defense
layer or an optimization, never a guarantee.

---

### 24.4 DA-SCHEMA-8 — iceberg-rust supports drop / rename / type-promotion transactions 🧪

**Mechanism.** The safe-evolution set (`Schema_Change…` §7.1) is `add_column` (mature), plus
`drop_column`, `rename_column`, and type-promotion (`int→long`, `float→double`, `decimal` precision
widen). Rename correctness rests on Iceberg's **field-ID stability** (a rename keeps the field ID and
all history).

**Exact claim.** The pinned crate's table-transaction API exposes `drop_column`, `rename_column`, and
`update_column` (promotion), committing them as metadata-only ops with field IDs preserved on rename.

**Verification plan.** In `prototypes/iceberg-capability/`: apply each op to a table; assert (a) rename
preserves the field ID (write under the old name, rename, read history under the new name), and (b)
promotion is metadata-only (no data rewrite). 

**Fallback.** Already the agreed safety branch (`Schema_Change…` §7.1 implementation note): **route any
unsupported op into the "halt table" branch** until the crate catches up. A halted table surfaces an
error + metric; the operator re-snapshots into a fresh table if needed. Rename specifically degrades to
the documented `drop+add` + warning (history NULL under the new name) when `rename_column` is
unavailable.

**Impact.** 🧪 — the fallback is the existing design's fail-safe; no redesign.

---

### 24.5 DA-TYPE-5 — `FixedSizeBinary(16)` → Iceberg `uuid` annotation 🧪

**Mechanism.** iceberg-rust auto-maps most Arrow types, but `FixedSizeBinary(16)` is ambiguous between
`uuid` and `fixed(16)` and needs an explicit field-level hint (`{"iceberg.type":"uuid"}` or
`IcebergType::Uuid` in the schema builder) — `Type_Mapping.md` Hop-3.

**Exact claim.** The pinned crate honors a field-level annotation that forces `FixedSizeBinary(16)` →
Iceberg `uuid`.

**Verification plan.** Build a schema with a uuid column using the hint; assert the resulting Iceberg
field type is `uuid` (not `fixed(16)`); round-trip a value through write + read.

**Fallback.** Store uuid as `fixed(16)` (raw 16 bytes, data fully intact, downstream loses only the
`uuid` logical type) or as canonical `string` (text uuid, human-readable, slightly larger). Both are
lossless of *data*; only the logical type annotation is lost.

**Impact.** 🧪, cosmetic — purely a downstream logical-type nicety.

---

### 24.6 DA-SINK-10 — Compaction / rewrite API availability 🧪 (deferred)

**Mechanism.** Built-in maintenance (delete-file compaction, data-file compaction, expire snapshots,
orphan removal) needs iceberg-rust rewrite APIs. v1 maintenance is **external** (Spark/Trino), so this
gate only affects a *future* built-in scheduler (`sink.maintenance`, reserved & disabled in v1).

**Exact claim.** A future crate version exposes `rewrite_data_files` / `rewrite_position_delete_files` /
`expire_snapshots` equivalents.

**Verification plan.** Periodically re-check the crate's API surface; only build `sink.maintenance` once
available, starting with the MOR-critical delete-file + data compaction (the hybrid first step,
`Iceberg_Maintenance…` §4).

**Fallback.** External maintenance — already the v1 plan. The app's only obligation is to never assume
maintenance has run (planning/writes tolerate deep delete stacks and many small files) and to emit the
when-needed signals (`cdc_iceberg_delete_files`, etc.).

**Impact.** 🧪 and deferred — **no v1 impact.**

---

### 24.7 Consolidated execution plan

All six gates close with **two prototypes**, both pinned to the exact dependency versions:

1. **`prototypes/lease-conformance/`** (runs against real services) — the T1–T10 storage-CAS suite →
   closes **DA-CLUS-1**. **S3 done** (Route A, T1–T9, versioned + non-versioned) **and GCS done**
   (Route B native SDK, T1–T10 incl. the generation-CAS gate), 2026-06. **Only a real Azure run remains.**
2. **`prototypes/iceberg-capability/`** (recommended new) — one integration test binary pinned to
   iceberg-rust 0.9.0, exercising: equality-delete same-commit sequencing + Spark/Trino cross-read
   (**DA-SINK-1**), custom snapshot-summary round-trip (**DA-SINK-2**), drop/rename/promote with
   field-ID preservation (**DA-SCHEMA-8**), and the uuid annotation (**DA-TYPE-5**). DA-SINK-10 is a
   passive API-surface check in the same crate, deferred.

**Recommended sequencing for spec elaboration.** Run both prototypes early. DA-CLUS-1 and DA-SINK-1 are
the only two whose failure changes the architecture (native-SDK lease; position-delete/COW writer), so
they gate the **lease** and **sink-writer** portions of the application spec respectively. The other
four can be written against the assumed capability with the documented fallback noted inline, because
their failure paths (halt, cosmetic, defer, advisory) are already part of the design.

**Config/spec follow-ups owed (propose-only):** add a startup assertion requirement —
*the worker asserts the storage-CAS capabilities (DA-CLUS-1) and the iceberg-rust capability set
(DA-SINK-1/2, DA-SCHEMA-8) at boot and refuses to start if a required capability is absent* — to
`System-Overview.md` (operational preconditions) and reference it from `Configuration_Specification.md`
§11.2. This makes a missing capability a fail-fast at startup rather than a first-write surprise.

---

## 25. Multi-sink re-scope & Delta Lake sink (2026-09-26)

Source: `research/Multi_Sink_Architecture_and_Delta_Lake.md` — all decisions **D-MS.1–D-MS.14 signed off
2026-09-26** (D-MS.4 corrected by Oleksii: `sink.type` is **required, no default**, V-091).

### 25.1 What changed

- **Sink is pluggable per pipeline** (`sink.type: iceberg | delta`, required). One sink per pipeline in
  v1 (D-MS.2); positions kept per (sink, table) so fan-out is additive later.
- **Collapse moves to the core** (D-MS.3): sinks receive a sink-neutral `TableChangeSet`; the old
  Iceberg "sink-writer contract" becomes the Iceberg *realization* of a neutral `Sink` trait contract
  (DA-X-3 ✅ by construction, pending spec merge).
- **Per-sink capabilities + startup assertion**: a config needing a capability the compiled-in sink
  lacks is refused at pipeline start with a named capability. This is what turns DA-SINK-1 from a
  design blocker into a runtime capability.
- **Delta Lake (delta-rs 1.0.0) is a first-class v1 sink** (D-MS.1): one MERGE per table per flush
  (delete + upsert + insert), `txn` app-transaction as the epoch fence carrier, `commitInfo` metadata as
  the position carrier, delete-all TRUNCATE, checkpoints + log cleanup built-in, OPTIMIZE/VACUUM external.

### 25.2 Gap status changes

| ID | Change |
|---|---|
| DA-SINK-1 | 🔴 → re-scoped as **DA-SINK-1i** 🟡: Iceberg **mirror** mode capability-gated on an iceberg-rust delete-commit action; tracking upstream continues (D-MS.10). No longer blocks the system. |
| DA-X-3 | 🔄 → ✅: neutral sink contract + per-sink realizations — merged into `Data-Flow.md` Phase 4 (2026-09-26). |
| DA-X-5 / DA-FLOW-5 | 🟥 → ✅(design): TRUNCATE is a barrier (seal→flush→clear); Delta = delete-all (metadata-only), Iceberg = overwrite-to-empty (with the mirror capability). |
| DA-X-6 / DA-SNAP-8 | 🟥 → 🔄: PK-less tables allowed **only in `write_mode: changelog`** (D-MS.6); changelog reserved in config, implemented after the Delta mirror (D-MS.5). Dump-only + mirror still halts. |
| DA-SINK-2 | scope widened: marker carrier is now a per-sink responsibility (`CommitMarker`). |
| DA-DEP-1 | + **single-Arrow rule** (D-MS.14): arrow 59 workspace-wide; iceberg sink moves to iceberg 0.11 (arrow 59); FFI bridge only as a temporary escape hatch. |

### 25.3 New gaps

| ID | Title | Sev | Status | Note |
|---|---|---|---|---|
| DA-X-10 | **Sink marker disarmed by foreign commits** *(confirmed empirically 2026-09-26, delta probe P3: latest commit after OPTIMIZE carries no marker)* — an external compaction/OPTIMIZE/expire commit carries no `owner_epoch`/`committed_position`; "read the latest snapshot's marker" then returns nothing and the fence silently disarms | 🟡 | ✅(design) | Applies to the Iceberg design as-is. Rule: read the marker from the **latest commit authored by this application** (walk back), or use a carrier persisted in table state (Delta `txn`). Research §8. **Resolved in research 2026-09-26:** Iceberg epoch mirrored to a `cdc.owner_epoch` table property in the same transaction (authoritative; atomicity ✅ probed 2026-09-26, `prototypes/iceberg-capability` G2b), position via own-snapshot walk-back by `cdc.pipeline`, none retained = not covered (`Clustering…` §5.5, `Delivery…` §3–§4, `Iceberg_Maintenance.md` §2). Delta: `txn` + `commitInfo` walk-back (probed). Spec follow-up applied 2026-09-26: `Data-Flow.md` Phase 4 Iceberg item 5 names the table-property carrier. |
| DA-MS-1 | Delta MERGE atomic delete+upsert, idempotent replay (probes P1/P2/P5) | 🔴→✅ | ✅ | **Proven 2026-09-26** on local fs (`prototypes/delta-capability/`): one MERGE = one version; replay identical; truncate metadata-only. Real-cloud atomicity is DA-MS-4. |
| DA-MS-2 | Delta epoch fence via `txn` (conflict on same `app_id`, survives foreign OPTIMIZE) (P3/P4) | 🟡→✅ | ✅ | **Proven 2026-09-26**: stale zombie rejected (`Concurrent transaction failed`), reloaded zombie fenced by pre-check; `txn` survives a foreign OPTIMIZE; commitInfo walk-back recovers the position. |
| DA-MS-3 | Delta MERGE write amplification under random-key churn (P6) | 🟡 | 🔄 | **Measured 2026-09-26**: 1M rows/40 files, 5k updates — sequential keys rewrite 1 file (20k rows); random keys rewrite **all 40 files (995k rows, ≈200×)**. Not a correctness risk; a scope caveat for large tables with spread-out updates. **D-MS.15 decided (b)**: v1 = documented caveat + rows-copied/files-rewritten metrics + flush guidance; post-v1 = deletion-vector writer (own probe). |
| DA-MS-4 | Delta commit atomicity on real S3/GCS/Azure via conditional put (P8) | 🔴 | 🔄 (S3 ✅, GCS ✅; Azure 🧪) | Same capability class as DA-CLUS-1 (proven for the lease). **S3 proven 2026-09-26** (`prototypes/delta-capability/tests/probe_cloud_commit.rs`, us-east-1): delta-rs 1.0 commits with one `If-None-Match: *` PUT (no DynamoDB); raw put-if-absent 160 contenders → 10/10 single winners; same-version MERGE race → exactly one version with the winner's content only; concurrent owners → exactly one lands (fence alone, P8e: `txn` app-id conflict); concurrent foreign appends → all land, none lost. **GCS proven 2026-09-26** (`gs://leasetest`, same probe, `--features gcs`): commit = one `x-goog-if-generation-match: 0` PUT; identical results P8a–e; no 429 surfaced to the caller in 160 racing creates. Azure: credentials pending. |
| DA-MS-5 | Delta schema evolution gaps (no drop/rename/widen in delta-rs) | 🟢 | ✅(design) | Soft-drop + warn (D-MS.9); widen → halt. |
| DA-MS-7 | delta-rs 1.0 refuses ADD COLUMN on column-mapping (`name`) tables (probe P7b) | 🟡 | ✅ | Would disable the only Delta evolution op. **R-1 decided 2026-09-26**: `sink.delta.column_mapping` default `none`, `name` rejected in v1 (V-092). |
| DA-MS-8 | **`timestamp_ntz` raises the Delta table protocol to reader v3 / writer v7** (probe P10) | 🟡 | ✅ | Any source `timestamp without time zone` column (common in PG/MySQL) makes the Delta table require `timestampNtz` table-feature support; engines without it cannot read the table at all. Without such a column the table stays at reader 1 / writer 2 (P10b). Options: (a) accept + document reader requirements; (b) map `timestamp_ntz` → Delta `timestamp` (UTC-interpreted wall clock; keeps the protocol minimal, loses the no-tz marker); (c) → string; (d) a `sink.delta` option choosing (a)/(b). **Recommendation: (d) with default (a)** — correct semantics by default, an explicit escape hatch for legacy-reader estates. **Decided 2026-09-26 (D-MS.16): (d) with default (a)** — `sink.delta.timestamp_ntz_as: timestamp_ntz | timestamp | string` (config §9.4.3, V-114). |
| DA-MS-9 | Hop-3b type mapping verified end-to-end (probe P10) | 🟢 | ✅ | All types exact through append + MERGE; independent DuckDB (delta-kernel) cross-read agrees; tz vs ntz semantics preserved. |
| DA-DEP-1b | Single arrow major across sinks (P9) | 🟢 | ✅(resolution) | deltalake 1.0 + iceberg `main` resolve to one arrow/parquet 59.3; joint compile not yet run. |
| DA-MS-6 | Spec-merge debt from the multi-sink design | 🟢 | ✅ | **Applied 2026-09-26**: System-Overview (Sinks block, pluggable format), Data-Flow (Phase 0 sink preflight, Phase 3 collapse + `TableChangeSet` + TRUNCATE barrier, Phase 4 sink contract + Iceberg/Delta realizations), Config (`sink.type` required, `sink.iceberg`/`sink.delta`, catalog → `sink.iceberg.catalog`, pipeline-level `schema_evolution`, sink preconditions §11.2, V-083…V-093), Schema §7 (Applied/Degraded/Unsupported, Delta column), Type_Mapping (logical tags, Hop 3b). |

| DA-X-11 | **iceberg-rust `Transaction::commit` rebases a stale zombie instead of rejecting it** *(probe G2b-3, iceberg 0.10.1, 2026-09-26)* — `do_commit` reloads the table and re-applies the actions on **every** attempt (not only on retry), so requirements are derived from the fresh metadata: a zombie holding a stale `Table` lands on top of the new owner's snapshot and regresses `cdc.owner_epoch` 6 → 5. Invalidates `Clustering…` §5.5 layer 2 ("zombie's commit conflicts and fails") and the TOCTOU-free reading of layer 3 | 🟡 | ✅(research) | **Fix, public API only (G2b-4):** commit through a per-commit catalog view whose `load_table` returns the base the fence was checked against → `fast_append`'s `assert-ref-snapshot-id(main)` makes it a real CAS; conflict surfaced non-retryable; the sink owns reload → re-check → re-pin. Proven: stale zombie rejected with neither property nor snapshot applied; foreign append under the pin → conflict → re-pin succeeds; foreign metadata-only edit not clobbered. Residual: a new owner's metadata-only commit doesn't move `main`, so one pinned zombie commit can still land — same window as "before the new owner's first data commit", absorbed by idempotency. `Clustering…` §5.5 / D-7 updated. Spec applied 2026-09-26: `Data-Flow.md` "Sink Commit Failure" names the pin. **Real catalog (probe G2c, AWS Glue + S3, 2026-09-26):** the pinned commit is a CAS — 6 rounds × 8 concurrent pinned writers → exactly 1 winner per round, **all 42 losers stopped by Glue's `VersionId` lock** (`ConcurrentModificationException`; the client-side requirement check alone stopped none), nothing of theirs applied; a stale pinned zombie rejected; the plain-`Transaction::commit` zombie rebase reproduced on Glue (property 603 → 1). S3 Tables uses the same pattern (`versionToken`; source-read, not run). REST / HMS: see DA-CAT-7 / DA-CAT-6. |
| DA-CAT-6 | **iceberg-rust HMS catalog cannot commit** — `iceberg-catalog-hms` `update_table` returns `FeatureUnsupported` (0.10.1 and `main`, source-read 2026-09-26) | 🟡 | 🆕 🔄 | **Gated 2026-09-26: config V-115 rejects `catalog.hive`** until a pinned release can commit (probe to lift). Every Iceberg sink commit is an `update_table`, so `sink.iceberg.catalog.type: hive` is unusable on the pinned crate (create/load work; commit does not). Moot for v1 while the Iceberg sink is gated (DA-SINK-1i), but it must join the Iceberg sink's **capability assertion** (per catalog backend). Upstream: #3104 (HMS config refactor) is the only open HMS PR; no commit path in flight. Options: capability-gate `hive` · contribute `update_table` (HMS `alter_table` with an env-context `metadata_location` expectation, as Java's `HiveTableOperations` does — needs a lock or `expected_parameter_key`) · route HMS estates via a REST catalog in front of HMS. |
| DA-CAT-7 | **iceberg-rust REST client has no SigV4** (0.10.1, source-read 2026-09-26) | 🟡 | 🆕 🔄 | **Gated 2026-09-26: config V-116 rejects `rest.auth: sigv4`** until a pinned release carries it (probe to lift). `sink.iceberg.catalog.rest.auth: sigv4` (config §9.5.3, `Catalog_Auth_Research.md` §3.2) is not realizable on the pinned crate, so the AWS Glue Iceberg REST and S3 Tables REST endpoints are unreachable through `iceberg-catalog-rest`. Upstream: open PRs **#3082** (SigV4 request signer) and **#3092** (SigV4 auth manager). Meanwhile the native `glue` catalog (probed, G2c) and the native `iceberg-catalog-s3tables` crate cover AWS. Capability-gate `rest`+`sigv4` until a release carries it. REST commit atomicity itself is server-side (409 on failed requirements) — not probed. |
| DA-CAT-8 | Losing / rejected commits leave orphan files | 🟢 | 🆕 ✅(measured) | G2c: every attempt writes its manifest + manifest list before the catalog decides, and every attempt that passes the client check also writes a `metadata.json`; iceberg-rust does not delete them on failure. 48 racing attempts → 42 orphan `metadata.json` + 86 orphan manifests. Bounded by conflict rate (the pipeline lease makes conflicts rare in practice); reclaimed by external `remove_orphan_files` (`Iceberg_Maintenance.md` §1). No design change. |

### 25.4 Next

1. ~~`prototypes/delta-capability/` — probes P1–P7, P10~~ ✅ 2026-09-26; remaining: **P8** Azure (S3 ✅, GCS ✅ 2026-09-26, DA-MS-4). P10 finding DA-MS-8 decided (D-MS.16: `sink.delta.timestamp_ntz_as`, default `timestamp_ntz`).
2. ~~Spec patches (DA-MS-6)~~ ✅ applied 2026-09-26.
3. Implementation order: core → Delta sink (mirror) → Iceberg sink (changelog; mirror when capable).
   **v1 consequence:** with Iceberg mirror capability-gated (DA-SINK-1i) and changelog deferred
   (D-MS.5), an Iceberg-sink pipeline is refused at start in v1 until either lands.
4. ~~Iceberg probe: `cdc.owner_epoch` property + append atomicity~~ ✅ 2026-09-26 (G2b); surfaced
   DA-X-11 (zombie rebase) — fixed in research; `Data-Flow.md` wording applied.
5. ~~Real-catalog requirement atomicity~~ ✅ 2026-09-26 on **AWS Glue** (G2c, `VersionId` CAS). Surfaced
   DA-CAT-6 (HMS cannot commit) and DA-CAT-7 (REST has no SigV4) from the crate sources — both
   join the Iceberg sink's per-catalog capability assertion. Spec applied 2026-09-26: V-115
   (`catalog.hive` rejected) and V-116 (`rest.auth: sigv4` rejected) — both lifted by a passing probe.

