---
updated: 2026-10-16
domain: Configuration
tags: [wal-lag, binlog-purge, backpressure, azure, mariadb, catalog, iam, strip-schema, batch, cdc]
resolves: [G-22, G-26, G-27, G-34, G-35, G-37, G-38, G-11.3]
---

# Operational & Scope Gaps — Consolidated Batch

## Scope

Closes a batch of smaller operability/scope gaps with a single decision each, plus a reconciliation
appendix for the documentation-consistency items (G-NEW-20/21/22) and the schema-evolution residuals
(DT-R2/R5/R6/R7).

---

## G-22 — PostgreSQL abandoned-slot WAL-lag guard

- **`source.postgres.max_wal_lag_bytes`** (R, default `null`). Metric **`data_avalanche_wal_lag_bytes`** is always
  emitted; default behavior on lag is **alert only** (warning). When the field is set and exceeded, the
  pipeline **halts** (opt-in) to protect the source disk from WAL accumulation behind an abandoned/slow
  slot.
- Pairs with G-15's slot lifecycle (a crashed owner's slot is the classic cause) and the WAL-gone →
  re-snapshot recovery.
- Validation: `max_wal_lag_bytes ≥` a floor (e.g. 64 MiB) when set.

## G-26 — MySQL binlog-purge action

- **`source.mysql.binlog_purge_action: error | re_snapshot`** (R, default **`error`**). When the resume
  position is purged from the server, default is to **halt with an error** (operator decides);
  `re_snapshot` opts into automatic recovery via incremental re-snapshot (G-6).
- Same recovery family as PG WAL-gone (G-15 §3). GTID remains the recommended `replication_type`.

## G-27 — Back-pressure buffer capacity

- **All inter-task channels are bounded** (confirmed as a requirement) — an unbounded channel would
  defeat back-pressure. Capacity exposed as **`sink.buffer_capacity_rows`** (R, default e.g. `200000`).
- Sits below the large-txn accumulator backstop (`sink.max_transaction_buffer_bytes`, G-29): the channel
  bounds steady-state flow; the byte ceiling bounds a single oversized transaction. Both are the
  `sink.buffer_*` family.
- **Cloud-storage / catalog unavailability (13.3):** on persistent sink failure the pipeline enters a
  **DEGRADED** state — retry with backoff, stop consuming source events once buffers fill
  (back-pressure halts reading), protecting the PG slot/WAL via the G-22 guard. Recovers on the next
  successful commit.

## G-34 — Azure credentials

- Rely on the **SDK default credential chain** (managed identity / workload identity), consistent with
  the S3/GCS no-explicit-credentials model. Add an optional **`globals.azure.account_name`** (R) for
  disambiguation. Every `bucket` is a **container** (already in config §10). No explicit secret fields.
- Validation: `account_name` valid storage-account name when set.

## G-35 — Iceberg catalog unavailability

- Catalog operations get their **own** retry policy (separate from object storage): bounded retries with
  exponential backoff + jitter, then **DEGRADED** (pause + alert via `data_avalanche_pipeline_status = error` and
  `data_avalanche_iceberg_commit_failures_total`). A **circuit breaker** avoids hammering a down catalog. Mirrors
  the Iceberg-write-failure path (Data-Flow) but keyed on catalog (commit/metadata) vs data-file write.
- Optional `catalog.retry: { max_attempts, base_backoff, max_backoff }` (R) — else fixed defaults
  (5 attempts, 1s→30s).

## G-37 — Minimum IAM permissions (documentation)

Document per provider (no config; an operator runbook prerequisite):

- **S3:** `GetObject`, `PutObject`, `DeleteObject`, `ListBucket`, `HeadObject` (+ `s3:PutObject` with
  `If-None-Match`/`If-Match` conditional support for the lease — i.e. standard object perms; CAS is
  request-header-level, no extra IAM action).
- **GCS:** `storage.objects.{create,get,delete,list}` (generation preconditions need no extra role).
- **Azure:** **Storage Blob Data Contributor** on the container.

## G-38 — MariaDB scope (IN scope, MySQL-compatible path)

MariaDB is **accepted via the MySQL path** (`source.type = mysql` covers it; a `mariadb` alias may be
added for clarity). **Real divergence to handle — flagged, not hand-waved:**

- **GTID format differs.** MariaDB GTID is `domain-server-sequence` (e.g. `0-1-100`), **not** MySQL's
  `server-uuid:tx`. The G-14 **`SourcePosition` MySQL implementation must support both** GTID
  dialects (subset/union/intersection over MariaDB domains vs MySQL UUIDs). **New sub-research item.**
- Binlog event layout and some system tables differ; the channel-C catalog query and the DDL classifier
  (G-19a) must be validated against MariaDB's `information_schema`.
- **Validation:** detect MariaDB at connect (server version banner) and select the correct GTID parser;
  warn if GTID mode/dialect is ambiguous.
- This is accepted as **best-effort MySQL-compatible** for v1 with the GTID-dialect work called out as
  the one genuine correctness dependency (it extends, not breaks, the G-14 design).

## G-11.3 — `glue.strip_schema` collisions

- Under **`strip_schema: true`**, two source schemas with the same table name collide → **config-time
  validation error** (V-rule) naming the colliding tables.
- Under `strip_schema: false`, the naming transform is **`<schema>__<table>`** (double underscore),
  documented and collision-free.

---

## Appendix A — Documentation reconciliation (propose-only spec edits)

These are mechanical edits to Oleksii-owned spec docs; staged here as ready-to-apply proposals.

- **G-NEW-20** — `Data-Flow.md` state diagram: drop the redundant `pipelines/<id>/` segment; per-pipeline
  state already roots at `sink.prefix`/`state.prefix` defaulting to `id` (config spec §10). Edit: change
  the state-layout tree's `pipelines/<pipeline-id>/...` to root the per-pipeline files directly under the
  per-pipeline state location.
- **G-NEW-21** — `Configuration_Specification.md` §13: merge the four Type_Mapping config fields
  (`source.mysql.tinyint1_as_bool`/`tinyint1_detection`, `source.toast_strategy`,
  `tables.column_type_overrides`, `tables.numeric_overflow_action`) and their four validation rules into
  the master table. (The source-SELECT precondition is already merged.)
- **G-NEW-22** — `Data-Flow.md`: reflect the four `ColumnValue` enum extensions (tz-aware `Timestamp`
  flag, `Array` element type, new `Time(NaiveTime)`, new `UInt64(u64)`) in the enum shown in the
  accumulator section.

## Appendix B — Schema-evolution residuals (DT-R2/R5/R6/R7) — confirmed/closed

- **DT-R2 (rename-when-unprovable):** confirmed — **drop+add with warning** (already in §7.2 and G-13 §6).
- **DT-R5 (ENUM/SET representation):** confirmed against G-18 — `ENUM → string`, `SET → list<string>`;
  append-only value additions Ignorable, reorder/remove Unsafe.
- **DT-R6 (`binlog_row_metadata`):** confirmed **recommended (warn if absent)**, names fall back to
  channel C — already in config §13. **Superseded (M7a, 2026-10-15): required.** A row is decoded by
  position against a schema channel C builds partly from a later catalog read, so catalog names
  can't stand in for the table map's: same-typed columns would be read into each other. The stream
  checks each table map's names, signedness, charsets and ENUM/SET strings (config §11.2,
  `Schema_Change…` §2.2).
- **DT-R7 (`rescan_interval` default):** confirmed **60m**, `0` disables — already in config §9.7 (formerly §9.5.1).

These residuals had recommendations already; this batch ratifies them — none reopen a gap.

---

## Resolved decision summary

| Gap | Resolution |
|---|---|
| G-22 | `max_wal_lag_bytes` (default null); metric always on; alert-only default, opt-in halt |
| G-26 | `binlog_purge_action: error (default) | re_snapshot` |
| G-27 | bounded channels; `sink.buffer_capacity_rows`; DEGRADED back-pressure on sink failure |
| G-34 | default credential chain + optional `azure.account_name`; container = bucket |
| G-35 | catalog-specific retry + circuit breaker + DEGRADED |
| G-37 | per-provider minimum IAM permissions documented |
| G-38 | MariaDB **in scope** (MySQL path); `SourcePosition` must handle MariaDB GTID dialect (sub-item) |
| G-11.3 | `strip_schema` collision = config error; non-stripped = `schema__table` |
