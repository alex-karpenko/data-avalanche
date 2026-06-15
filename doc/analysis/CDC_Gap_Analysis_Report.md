# CDC System — Requirements Gap Analysis & Research Agenda

**Document type:** Pre-implementation analysis  
**Based on:** System-Overview.md · Data-Flow.md · Configuration_Specification.md · Catalog_Auth_Research.md · Type_Mapping.md · Schema_Change_Detection_and_Evolution.md · Sources Index v3 (50 sources)  
**Revision:** v9 — updated after Clustering_Architecture_Research.md was added  
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

### 2.5 NEW — Pipeline lifecycle API state machine undefined 🆕 🟡

The System Overview adds `pause`, `stop`, `resume`, `restart`, and `flush` endpoints. The state machine they operate on is not defined:
- What is the difference between `pause` and `stop`? (Presumably pause = stop reading but keep resources; stop = stop and release connection but keep state.)
- Can you `resume` a `stopped` pipeline, or only a `paused` one?
- Does `pause` flush buffered data or hold it in memory?
- What happens to a paused pipeline during a worker failover — does the new owner inherit the paused state?
- Is pause/stop state persisted to `globals.state` (so it survives restarts) or in-memory only?

**Research agenda:** Define a pipeline state machine: `RUNNING → PAUSED → RUNNING`, `RUNNING → STOPPED`, `STOPPED → (restart) → RUNNING`. Specify which transitions flush, which release DB connections, which release leases, and which persist to cloud state so they survive failover and worker restart.

---

## 3. Snapshotting

### 3.1 Snapshot resumability on crash — UNCHANGED 🔴

Data-Flow.md documents a per-table `cursor` in the state layout (`snapshots/<schema>.<table>.json` with a `cursor` field), which is real progress on the *layout*, but the cursor *format* for composite/non-integer primary keys and the checkpoint frequency are still unspecified.

**Research agenda:** Define cursor format for keyset pagination across composite PKs. Define checkpoint frequency (every N rows, default 10,000). Define fallback for tables with no suitable cursor key.

---

### 3.2 Multi-table snapshot consistency — PARTIALLY RESOLVED 🔄

Data-Flow.md now specifies the mechanism per engine: PostgreSQL uses `EXPORT_SNAPSHOT` + `SET TRANSACTION SNAPSHOT`; MySQL uses `FLUSH TABLES WITH READ LOCK` (< 8.0) or `START TRANSACTION WITH CONSISTENT SNAPSHOT` (8.0+). This is a substantial advance.

**Remaining gap 🟡:** No `consistent_snapshot` config toggle, no handling of the MySQL privilege requirement (`RELOAD`, `LOCK TABLES`), and no fallback when consistency cannot be achieved (e.g., GTID disabled). The mechanism is described but not made configurable or fault-tolerant.

**Research agenda:** Expose `source.consistent_snapshot` (default true). Document required MySQL privileges as a prerequisite. Define degraded behavior with a prominent warning when consistency cannot be guaranteed.

---

### 3.3 "Next to last snapshotted position" semantics — PARTIALLY RESOLVED 🔄

Data-Flow.md resolves the *startup* snapshot case: the slot's `start_lsn` is the resume point, and `EXPORT_SNAPSHOT` guarantees the SELECT sees exactly the data at `start_lsn` — no gap, no overlap. For the *on-demand* API snapshot, Data-Flow.md adds "resume from pause_lsn + 1," which is clearer but introduces a subtle question.

**Remaining gap 🟡:** "Resume from pause_lsn + 1" assumes the snapshot's consistent read point equals `pause_lsn`. If the snapshot transaction's actual read view is established slightly after `pause_lsn` (between pausing the stream and opening the snapshot transaction), there is a gap window. The exact ordering — pause stream, capture `pause_lsn`, open snapshot transaction — must be atomic or the snapshot read point must be captured, not assumed.

**Research agenda:** Specify that the on-demand snapshot captures its own consistent read point (LSN/GTID at the moment its snapshot transaction opens) and resumes streaming from *that* position, not from `pause_lsn`. Accept duplicates (handled by writer-side dedup) over gaps.

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

---

### 4.2 Offset handover staleness — UNCHANGED 🟡

Data-Flow.md's worker-failure path says the new owner resumes "from last confirmed_flush_lsn / gtid_executed" with "worst-case replay = time since last checkpoint write," and the checkpoint is written after each Iceberg commit. This actually resolves the *checkpoint frequency* question (it's flush-driven).

**Remaining gap 🟡:** WAL-lag handling for an abandoned PostgreSQL slot (a crashed worker leaves the slot unconsumed, accumulating WAL on the source) is still not addressed. No `max_wal_lag_bytes` parameter, no alerting threshold.

**Research agenda:** Add `source.postgres.max_wal_lag_bytes`; emit a `cdc_wal_lag_bytes` metric; optionally halt the pipeline if lag exceeds the threshold to protect the source DB's disk.

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

---

### 5.3 Config state storage — RESOLVED ✅

`globals.state` now exists and is documented as the application-level state location. The remaining clarity question (what goes in global vs pipeline state) is captured in 1.4.

---

## 6. Apache Iceberg Write Semantics

### 6.1 COW vs MOR — RESOLVED ✅ (unchanged)

`sink.strategy: mor` default. Note 6.6 (migration on strategy change) remains.

### 6.2 Ghost rows / deduplication — UNCHANGED 🔴

Data-Flow.md adds an `_lsn` metadata column and a per-batch flush-on-transaction-boundary rule, and mentions an "exactly-once guard" checking `committed_lsn` in the snapshot summary. This is meaningful, but the core ghost-row defense — a declared `primary_key` per table for equality deletes and intra-batch dedup ordering — is still not in the config or specified as a requirement.

**Research agenda (unchanged):** Add `tables.primary_key` (inferred from source PK, manual override). Intra-batch dedup: sort by (PK, sequence) and keep last. Halt tables with no PK and no surrogate key.

### 6.3 Delivery guarantee — PARTIALLY RESOLVED 🔄

Data-Flow.md commits to a concrete model: at-least-once with an exactly-once guard via a `committed_lsn` property in the Iceberg snapshot summary, and the critical invariant "write the offset checkpoint only after a successful Iceberg commit." This is essentially the v1/v2 recommendation, now adopted in the data-flow doc.

**Remaining gap 🟡:** The `committed_lsn` guard assumes a single monotonic LSN per table per commit. For a multi-table flush, or for MySQL GTID sets (which are sets, not scalars), the comparison `committed_lsn ≥ flush_lsn` needs a defined ordering. GTID set comparison is subset-based, not numeric.

**Research agenda:** Define the dedup-guard comparison for GTID sets (subset check) vs PostgreSQL LSN (numeric). Confirm whether the guard is per-table or per-commit.

### 6.4 Iceberg partitioning — UNCHANGED 🟡
No `partitioning` field in `tables`.

### 6.5 Iceberg maintenance — UNCHANGED 🟡
No maintenance schedule config.

### 6.6 Strategy change does not migrate existing tables — UNCHANGED 🟡
`sink.strategy` change via reload does not migrate existing Iceberg tables. Still needs the "applies to new tables only" rule plus a migration path.

---

## 7. Source Database Connectors

### 7.1 PostgreSQL replication slot lifecycle — PARTIALLY RESOLVED 🔄

`source.postgres.slot` and `publication` are user-specified, and Data-Flow.md's shutdown sequence adds "drop replication slot if `auto_cleanup = true` in config" — but `auto_cleanup` does not appear in the config draft. There's a missing config field referenced by the data-flow doc.

**Remaining gaps 🔴:**
- Create-vs-reconnect-vs-reject behavior when the user-named slot already exists (G-15a) still undefined.
- No slot-name uniqueness validation across pipelines sharing a source DB.

**Resolved:** `source.postgres.auto_cleanup` (bool, default true) is now present in the config at both the defaults and pipeline level, matching the Data-Flow.md shutdown-sequence reference (G-NEW-17 ✅).

**Research agenda:** Specify slot create-if-absent / reconnect-if-LSN-matches / warn-otherwise. Validate slot-name uniqueness at config-validation time.

### 7.2 MySQL GTID vs binlog — PARTIALLY RESOLVED 🔄
`replication_type: binlog | gtid` present. Binlog-purge handling (stored position purged from server) still undefined.
**Research agenda:** Add `mysql.binlog_purge_action: error | re_snapshot` (default error). Document GTID as preferred.

### 7.3 MariaDB support — UNCHANGED 🟡
`source.type` still only `postgres` / `mysql`.

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

### 9.1 API versioning — UNCHANGED 🟡
`globals.api` defines `address`, `auth`, `user`, `password`, `tls` but no version path. With the expanded endpoint set (pause/stop/resume/restart/flush now added), versioning matters more.
**Research agenda:** Adopt `/v1/...`; keep `/health/live`, `/health/ready`, `/metrics` unversioned and unauthenticated.

### 9.2 RBAC role matrix — UNCHANGED 🔴
`auth` modes present and basic-auth credentials now configurable, but no role definitions or per-endpoint permission matrix. The new lifecycle endpoints (stop, shutdown, leave cluster) make this more urgent — these are destructive operations needing role gating.
**Research agenda:** Define `viewer` / `operator` / `admin`; map every endpoint (including the new pause/stop/resume/restart/flush/leave/shutdown) to a minimum role. JWT/OAuth2 still need JWKS URL, issuer, audience fields — not present.

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

### 9.6 NEW — `flush` and `restart` endpoint semantics overlap with config-driven behavior 🆕 🟢
The `pipeline flush data to storage` endpoint forces an immediate flush, and `pipeline restart` forces a restart. Their interaction with the automatic `flush_mode` and config-reload-driven restart is undefined (e.g., does a manual flush reset the time-based flush timer?).
**Research agenda:** Define that a manual flush resets the flush timer and counters; define that a manual restart follows the same in-flight-batch policy as a config-driven restart (see 2.2).

---

## 10. Observability

### 10.1 Distributed tracing — UNCHANGED 🟡
No `globals.tracing` / OTLP endpoint field.
**Research agenda:** Add `globals.tracing.otlp_endpoint` (optional; disabled when absent).

### 10.2 Metrics catalogue — UNCHANGED 🟡
`/metrics` endpoint exists; mandatory metric catalogue still undefined.
**Research agenda:** Adopt the metric catalogue from prior revisions (replication lag, rows read, flush duration, snapshot progress, cluster members, WAL lag, Iceberg commit duration/failures, delete file count).

### 10.3 Log/trace correlation — UNCHANGED 🟡
`log.format: json` confirmed; `trace_id`/`span_id` injection not yet required.

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

### 11.3 `glue.strip_schema` collisions — UNCHANGED 🟢
`strip_schema` present; name-collision behavior (two schemas, same table name) undefined.
**Research agenda:** Collision under `strip_schema: true` = validation error; define the non-stripped naming transform (`schema__table`).

---

## 12. Security and Network

### 12.1 TLS for management API and gossip — RESOLVED ✅ (was G-31, partially)
Both `globals.api.tls` and `globals.cluster.memberlist.tls` (cert/key/ca, env-var substitutable) are now present, and the `cat:` → `ca:` typo on the memberlist CA field is fixed. The `auth: jwt | oauth2` without TLS = error rule should be added to the validation spec (1.1f).

**Research agenda:** Add the "non-anonymous auth requires TLS" validation rule. Confirm whether memberlist TLS implies mTLS (client cert verification) or server-only TLS.

### 12.2 Source DB SSL/TLS — RESOLVED ✅ (unchanged)
`ssl-mode` with full value set defined.

### 12.3 Cloud storage credential scope — PARTIALLY RESOLVED 🔄
The draft now states "no explicit credentials config, the cloud SDK's default credential chain is used." This resolves the *mechanism* (default chain) but the minimum IAM permission documentation is still absent.
**Research agenda:** Document minimum IAM permissions per provider (S3 Get/Put/Delete/List/Head; GCS objects create/get/delete/list; Azure Storage Blob Data Contributor).

---

## 13. Operational Edge Cases

### 13.1 Back-pressure — PARTIALLY RESOLVED 🔄
Data-Flow.md confirms automatic channel back-pressure. Channel/buffer capacity is still not a configurable parameter, and an unbounded channel would defeat the mechanism.
**Research agenda:** Confirm all inter-task channels are bounded; expose capacity (e.g., `sink.buffer_capacity_rows`).

### 13.2 Source DB unavailability — PARTIALLY RESOLVED 🔄
Data-Flow.md defines reconnect with backoff (initial 1s, max 60s, jitter) and "in-progress snapshots paused, cursor preserved, resume after reconnect." This largely resolves the behavior. Backoff is documented in the data-flow doc but not exposed as config — acceptable if fixed.
**Status:** Effectively resolved at the behavior level; only config-exposure is optional.

### 13.3 Cloud storage unavailability — UNCHANGED 🟡
Data-Flow.md's Iceberg-write-failure path covers retry + pause, but the DEGRADED-state buffering ceiling and the source-halt-on-full-buffer rule are not specified.
**Research agenda:** Define retry count, DEGRADED state, buffer ceiling, source-halt to protect WAL.

### 13.4 Iceberg catalog unavailability — UNCHANGED 🟢
Catalog retry semantics (separate from storage) and circuit breaker undefined.

### 13.5 Large transactions — UNCHANGED 🟡
No `max_transaction_batch_rows`. Data-Flow.md's "flush only on transaction boundary, never mid-transaction" rule actually *increases* this risk: a 10M-row transaction cannot be flushed until it commits, guaranteeing a giant in-memory batch.
**Research agenda (now more urgent):** Add `max_transaction_batch_rows` with a documented trade-off: splitting a transaction across Iceberg commits breaks the "atomic transaction visibility" guarantee that Data-Flow.md currently promises. These two requirements are in direct tension and must be reconciled.

### 13.6 Tables without primary keys — UNCHANGED 🟡
System Overview line 50 states tables without a PK have replication halted and PK-less tables found via glob are skipped with a warning — this is good and resolves the *detection* and *default behavior*. Still missing: the `surrogate_key` config option to allow PK-less tables to be replicated with a user-designated identity.
**Research agenda:** Add `tables.surrogate_key` override for tables lacking a natural PK.

### 13.7 Azure Blob Storage — PARTIALLY RESOLVED 🔄
`globals.cloud: azure` confirmed and the draft now states bucket = container for Azure. But Azure-specific fields (account name, credential mode) beyond the default credential chain are not enumerated.
**Research agenda:** Confirm whether the default credential chain (managed identity) is sufficient for Azure, or whether `account_name` / explicit credential fields are needed.

---

## 14. Missing Research Topics (unchanged + 1)

| Topic | Why It's Needed | Recommended Source |
|---|---|---|
| OpenDAL integration with iceberg-rust | Storage extension not indexed | OpenDAL docs + iceberg-rust source |
| Iceberg REST Catalog spec | Needed for catalog auth + commit design | Apache Iceberg REST Catalog OpenAPI spec |
| Parquet file format tuning | Row group size, compression affect cost/perf | Apache Parquet documentation |
| Arrow IPC / RecordBatch memory mgmt | Bounded channel capacity, buffer lifecycle | arrow-ipc crate docs |
| Iceberg equality delete file format | Equality deletes recommended for CDC | Iceberg spec §3.3 + iceberg-rust writer API |
| Kubernetes operator patterns | SIGTERM drain, ConfigMap hot-reload, probes | K8s operator SDK patterns |
| Rate limiting / throttling on source DB | High-throughput snapshot can overwhelm source | PG pg_stat_replication + MySQL throttling |
| Iceberg commit.retry / conflict resolution | Concurrent commit handling before writer design | iceberg-rust commit API source |
| `sqlparser-rs` for MySQL DDL parsing | Required for `schema_evolution.method: events` on MySQL | sqlparser-rs crate docs |

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
| G-NEW-15 | Pipeline | Lifecycle API state machine (pause/stop/resume) | 🟡 High | 🆕 New |
| G-6 | Snapshot | Resumability cursor format | 🔴 Blocker | 🔄 Open (layout defined, format not) |
| G-7 | Snapshot | Cross-table consistency | 🔴 Blocker | 🔄 Partially (mechanism in Data-Flow; no toggle/fallback) |
| G-7a | Snapshot | Override semantics | — | ✅ Resolved |
| G-7b | Snapshot | `dump_*` mode definitions | — | ✅ Resolved (defined in System Overview) |
| G-NEW-16 | Snapshot | Default mode disagreement (`never` vs `initial`) | — | ✅ Resolved (both say `never`) |
| G-8 | Cluster | Split-brain / lease | 🔴 Blocker | ✅ Resolved (design) — per-pipeline CAS lease in `globals.state`, clock-skew-safe protocol, Iceberg-commit fence (Clustering doc §5); defaults = D-6 |
| G-9 | Cluster | Rebalancing handoff (planned drain) | 🔴 Blocker | ✅ Resolved (design) — drain→flush→commit→checkpoint→release-lease; rate-limited; planned vs unplanned paths split (Clustering doc §8) |
| G-9a | Cluster | Single-node mode behavior | 🟡 High | 🔄 Partially (intent stated, behavior not) |
| G-10 | State | State layout keyed on name not id | — | ✅ Resolved (Data-Flow + config now use `id`) |
| G-11 | State | Atomic updates on S3 | 🔴 Blocker | 🔄 Open |
| G-12 | Iceberg | COW vs MOR | — | ✅ Resolved |
| G-12a | Iceberg | Strategy change migration | 🟡 High | 🔄 Open |
| G-13 | Iceberg | Ghost row / `primary_key` dedup | 🔴 Blocker | 🔄 Open (metadata cols added; PK config missing) |
| G-14 | Iceberg | Delivery guarantee | 🔴 Blocker | 🔄 Partially (model chosen; GTID-set guard undefined) |
| G-15 | Postgres | Slot lifecycle | 🔴 Blocker | 🔄 Open (+ `auto_cleanup` field missing from config) |
| G-16 | API | RBAC role matrix | 🔴 Blocker | 🔄 Auth verification schema done (spec §6.3.1); role→permission matrix still deferred to API design |
| G-16a | API | Basic auth + JWT/OAuth2 verification fields | — | ✅ Resolved (basic creds + §6.3.1 jwt/oauth2 schema) |
| G-17 | API | Config reload proxy | 🔴 Blocker | ✅ Resolved (design) — no proxy; 202 + `GET /v1/config/status` poll; every node reconciles its own owned pipelines (Clustering doc §9.2) |
| G-18 | Data Types | Type mapping table | — | ✅ Resolved (`Type_Mapping.md` — full 3-hop PG+MySQL→ColumnValue→Arrow→Iceberg) |
| G-19 | Data Types | DDL classification | — | ✅ Resolved (`Schema_Change…md` §7.2 classification table; four-channel detection) |
| G-19a | Data Types | MySQL DDL parser | 🟡 High | 🔽 Scope reduced — channel C supplies the delta; parser only classifies DDL + extracts table name (+rename keyword), not full ALTER-delta. De-risked but still required. |
| G-19b | Data Types | Behavior when evolution disabled | — | ✅ Resolved (halt table, continue others) |
| G-20 | Snapshot | On-demand resume read point | 🟡 High | 🔄 Partially (pause_lsn+1 defined; read-point assumption risky) |
| G-21 / 21a | Snapshot | Parallelism + connection budget | — | ✅ Resolved |
| G-22 | Cluster | Checkpoint staleness / WAL lag | 🟡 High | 🔄 Partially (checkpoint freq resolved; WAL-lag guard not) |
| G-23 | Cluster | SWIM Lifeguard / Scuttlebutt decision | 🟡 High | ✅ Decided — keep SWIM + Lifeguard; no mutable KV in gossip; SWIM accuracy is latency-not-safety (Clustering doc §3.6) |
| G-24 | Iceberg | Partitioning strategy | 🟡 High | 🔄 Open |
| G-25 | Iceberg | Maintenance responsibilities | 🟡 High | 🔄 Open |
| G-26 | MySQL | Binlog purge handling | 🟡 High | 🔄 Recovery path now defined (`Schema_Change…md` §8.2: non-replayable gap → re-snapshot); `binlog_purge_action` config field still reserved (spec §16) |
| G-27 | Ops | Back-pressure buffer capacity config | 🟡 High | 🔄 Partially |
| G-28 | Ops | Source DB unavailability | 🟡 High | ✅ Resolved (behavior in Data-Flow) |
| G-29 | Ops | Large transaction splitting | 🟡 High | 🔄 Open (now in tension with txn-atomic flush rule) |
| G-30 | Ops | Tables without PK / surrogate key | 🟡 High | 🔄 Partially (detection resolved; surrogate_key missing) |
| G-31 | Security | TLS for API + gossip | — | ✅ Resolved (tls sections added; `cat:` typo fixed) |
| G-32 | Observability | Distributed tracing | 🟡 High | 🔄 Open |
| G-33 | Observability | Metrics catalogue | 🟡 High | 🔄 Open |
| G-34 | Ops | Azure-specific config fields | 🟡 High | 🔄 Partially (cloud:azure + container mapping defined) |
| G-35 | Ops | Catalog unavailability | 🟢 Medium | 🔄 Open |
| G-36 | Cluster | Token count consistency rule | — | ✅ Resolved (spec V-026/V-062: cluster-wide anchor in globals.state, mismatch rejected) |
| G-37 | Security | IAM permission documentation | 🟢 Medium | 🔄 Open |
| G-38 | MySQL | MariaDB in-scope decision | 🟢 Medium | 🔄 Open |
| G-39 | Config | Env-var substitution syntax | — | ✅ Resolved (full grammar, spec §4.3) |
| G-NEW-20 | State | Data-Flow `pipelines/<id>/` segment redundant under spec §10 | 🟢 Medium | 🆕 New (doc reconciliation: re-root Data-Flow state diagram) |
| G-NEW-21 | Config | Type_Mapping config fields + rules not yet in spec §13 | 🟡 High | 🆕 New (4 fields + 4 validation rules to merge: tinyint1_*, toast_strategy, column_type_overrides, numeric_overflow_action) |
| G-NEW-22 | Data Flow | `ColumnValue` enum extensions not reflected in Data-Flow.md | 🟢 Medium | 🆕 New (doc reconciliation: 4 enum additions — tz flag, array element type, Time, UInt64) |
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
