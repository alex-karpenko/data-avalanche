---
updated: 2026-09-26
domain: Architecture
tags: [v1-scope, decisions, reserved-columns, drop-table, replication-lag, sla, mariadb, binlog-compression, deferrals]
resolves: [DA-SYS-4, DA-X-4, DA-FLOW-6, DA-OBS-5, DA-SYS-7, DA-SRC-8, DA-X-7, DA-SRC-12, DA-CFG-4, DA-CLUS-12, DA-CLUS-13, DA-SINK-8, DA-DEDUP-7]
---

# v1 Scope Decisions

The last open v1 decisions from the gap tracker (§20 / §26.4 of `CDC_Design_Gap_Analysis_v1.md`), signed
off 2026-09-26. v1 is the **Delta Lake sink only**; the Iceberg sink is v2 (tracker §26). Each decision
names the spec change it implies; those patches are listed in §3 and are **not yet applied** (spec edits
need approval).

## 1. Decisions

### D-V1.1 — Reserved lineage-column collision halts the table (DA-SYS-4 / DA-X-4)

**Rule.** If a source table has a column whose name equals a lineage column the accumulator injects
(`_op`, `_ts`, `_lsn`, `_seq`, `_source_db`, `_pipeline`) or starts with the sink-internal prefix
`__avalanche_`, that table **halts** with `RESERVED_COLUMN_COLLISION`, naming the column. Other tables
continue (the per-table halt of G-19b). Nothing is renamed or escaped.

- **Comparison is case-insensitive.** Delta column names are case-insensitive, so `_OP` collides with
  `_op`.
- **Checked at two points:** (1) schema resolution, when a table is first discovered, before
  `ensure_table`; (2) schema evolution. An `ADD COLUMN` or rename that introduces a colliding name is
  classified **Unsupported** and halts the table, like any other Unsupported delta
  (`Schema_Change…` §7).
- **The halt is sticky.** It needs an operator action (drop or rename the source column, then resume or
  re-snapshot the table).
- **Why not escape.** Silently renaming a source column makes the target schema differ from the source
  without anyone deciding it, and a later source rename can create a new collision on the escaped
  name. The case is rare enough that an explicit halt costs little.
- **Reopen path (v2).** A per-pipeline setting that renames *our* lineage columns (e.g. prefix `_cdc`)
  if real demand appears. The source schema is still never touched.

### D-V1.2 — A dropped PostgreSQL table halts that table; the Delta table is kept (DA-FLOW-6)

pgoutput carries no DDL, so a dropped table simply stops emitting.

- **Detection.** The table's relation OID is gone from `pg_class`. This is noticed by:
  - channel D (idle rescan, every `rescan_interval`, default `60m`);
  - channel A (schema resolution on every pipeline start or restart).
- **Reaction.** Halt that table with `TABLE_DROPPED`; other tables continue. The Delta table and its data
  are **never dropped automatically**. This is identical to the MySQL rule (`MySQL_DDL_Classifier.md`,
  "`DROP TABLE` (tracked) → halt that table; artifacts retained"), where the DROP arrives in-band on
  channel B.
- **Latency bound.** Detection takes at most `rescan_interval`, or until the next restart if
  `rescan_interval: 0`. No data is lost in that window: a dropped table has no further changes, and
  every change it made before the drop has already streamed.
- **Re-created table.** A same-named table has a new OID. It does **not** auto-resume into the halted
  target, because the schema and data may have nothing in common. It stays halted until an operator
  re-snapshots it.
- **Publication membership drift is out of scope here.** A table that still exists but is missing from
  the publication is reconciled or warned on by D-15.4 (`Postgres_Slot_and_Publication_Lifecycle.md`
  §4).

### D-V1.3 — Replication-lag targets: 60s steady state, 2 min failover (DA-OBS-5, OQ-OB2)

| Target | Value | Measured by | Budget |
|---|---|---|---|
| Steady-state lag | **p99 ≤ 60s** | `cdc_replication_lag_seconds{pipeline}` (source commit timestamp → sink commit) | default flush 30s + MERGE commit + decode/accumulate; about 2× headroom |
| Failover lag | **≤ 2 min** from owner loss to lag back under the steady-state target | the same metric, over a takeover | unowned window ~20–22s (Balanced lease, D-6) + replay since the last checkpoint (≤ one 30s flush interval) + Phase-0 resume |

- These are **design targets at default config**, not a contractual SLA. They ground the defaults
  already ratified: Balanced lease 15s/5s (D-6, `Clustering…` §18.6) and `flush_mode` 30s / 100k rows /
  512 MB. A tighter target means a shorter flush interval, so more small Delta commits and files.
- **Exclusions** (lag may exceed the target, and the metric shows it):
  - initial or re-snapshot backfill;
  - a single transaction larger than the flush bounds (`Large_Transaction_Handling.md`);
  - MERGE cost under random-key churn on large tables (D-MS.15);
  - source-side stalls.
- To be confirmed by a v1 load test. The recommended alert rule is lag > 60s for 5 min.

### D-V1.4 — `sink.type: iceberg` stays valid and is refused at start, naming v2 (DA-SYS-7)

- **Config.** The config accepts `iceberg` (the enum stays stable across v1 → v2, and V-115/V-116 still
  apply). The pipeline is refused at start by the per-sink capability assertion with an explicit message:
  *"sink.type 'iceberg' is not available in this version — the Iceberg sink ships in v2; use 'delta'."*
  It is a pipeline-level refusal, not a node failure.
- **Scope statement.** `System-Overview.md` gains a v1 scope statement: Delta Lake sink only; Iceberg
  in v2.

### D-V1.5 — MariaDB is in v1 (DA-SRC-8 / DA-X-7)

- **Evidence.** Proven by `prototypes/source-capability/` S-MY9 on MariaDB 11.4 with
  `mysql_async =0.37.1`.
- **Connect-time detection.** Detect MariaDB at connect from `version()`, then select the GTID dialect.
- **MariaDB resume.**
  - Set `@mariadb_slave_capability=4`, `@slave_connect_state=<gtid_pos>` and
    `@slave_gtid_strict_mode=1`.
  - Decode `GTID_EVENT` (type 162) ourselves: `seq u64 LE`, `domain u32 LE`, plus `server_id` from the
    event header.
- **Position.** `SourcePosition` for MySQL carries either dialect:
  - a UUID-based GTID set (MySQL);
  - a `domain-server-seq` list (MariaDB).
- The v1 test matrix covers MySQL 8.4 and MariaDB 11.4.

### D-V1.6 — Validate every compressed binlog transaction; halt the pipeline on a mismatch (DA-SRC-12)

- **The problem.** `mysql_common` 0.37.3 returns an empty buffer when zstd decompression fails, and
  `mysql_async` 0.37.1 ignores a payload-header parse error. A compressed transaction can therefore
  vanish without a stream error.
- **Guard.** Our source handles every `TRANSACTION_PAYLOAD_EVENT` itself and checks two things:
  - the payload header parses, and the decompressed length equals the header's `uncompressed_size`;
  - the payload yields **≥ 1** inner event.
- **On failure.** **Halt the pipeline**, not the table: the binlog position cannot advance past a
  transaction we could not read. Emit `BINLOG_PAYLOAD_INVALID` with the binlog file/position and the
  GTID.
- **Cost.** The check runs once per compressed transaction and costs nothing extra: we already hold
  the decompressed bytes.
- **Upstream.** Report the silent drop and offer a fix. Remove the guard only once a pinned release
  surfaces the error.

## 2. Deferrals confirmed (v1 out of scope; rows stay as the reopen path)

| Gap | Decision | v1 behaviour |
|---|---|---|
| DA-CFG-4 | External secret-manager integration → out of v1 | Env-var and file-content secrets with rotation. A sidecar (Vault agent, CSI driver) writing the file is the supported pattern. |
| DA-CLUS-12 | `kubernetes` lock backend → deferred | `storage` CAS lease only (V-075). |
| DA-CLUS-13 | Per-source-endpoint takeover bulkhead → deferred | Per-node-global takeover cap. Add per-endpoint groups if reconnect storms are observed. |
| DA-SINK-8 | Spill-to-disk for oversized transactions → deferred | Split-commits within the memory bounds (`Large_Transaction_Handling.md`). |
| DA-DEDUP-7 | Cross-table atomic visibility → out of scope | Each table commits atomically on its own. This is documented as a guarantee boundary; reopen only for a multi-table-commit requirement. |

## 3. Spec patches implied (pending approval)

| Spec | Change | From |
|---|---|---|
| `Configuration_Specification.md` §5 | Replace "operators should be aware" with the D-V1.1 rule: case-insensitive match, halt with `RESERVED_COLUMN_COLLISION`, checked at resolution and evolution. | D-V1.1 |
| `Schema_Change_Detection_and_Evolution.md` §2.1 / §7 | PG `DROP TABLE`: detection via channels D/A, `TABLE_DROPPED` halt, latency bound = `rescan_interval`, no auto-resume of a re-created table. Add a colliding `ADD COLUMN` to the Unsupported classes. | D-V1.2, D-V1.1 |
| `System-Overview.md` | v1 scope statement (Delta only; Iceberg v2); lag targets (60s p99 / 2 min failover); MariaDB in scope. Folds into the §19.3 S-1…S-6 merge. | D-V1.3/4/5 |
| `Configuration_Specification.md` `sink.type` | Note that `iceberg` is accepted but refused at start in v1, with the message text. | D-V1.4 |
| `Data-Flow.md` Phase 2a (MySQL) | MariaDB GTID dialect and connect-time detection; `TRANSACTION_PAYLOAD` validation → pipeline halt `BINLOG_PAYLOAD_INVALID`. | D-V1.5/6 |
