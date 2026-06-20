---
updated: 2026-06-20
domain: Architecture
tags: [iceberg, maintenance, compaction, expire-snapshots, delete-files, orphan-files, mor, cdc]
resolves: [G-25]
---

# Iceberg Table Maintenance

## Scope

Closes **G-25**. Defines the maintenance operations a CDC-written Iceberg table needs, **who runs them
in v1**, the signals the app exposes so maintenance can be triggered, and the reserved config surface
for a future built-in scheduler.

Builds on the MOR equality-delete writer (G-13) — the reason delete-file compaction is load-bearing.

---

## §1 — What MOR-CDC tables need

| Operation | Why it matters for CDC | Urgency |
|---|---|---|
| **Delete-file compaction** (fold equality deletes into data files) | Every flush writes delete files (G-13); read amplification grows linearly without it | **High (MOR-critical)** |
| **Data-file compaction** (merge small files) | Frequent small flushes → many small Parquet files → slow scans, metadata bloat | High |
| **Expire snapshots** | CDC commits frequently → snapshot history explodes → large metadata.json, slow planning | Medium |
| **Remove orphan files** | Failed commits / aborted writes leave unreferenced files → storage cost | Low |
| **Rewrite manifests** | Deep manifest lists slow query planning | Low |

Unmaintained, a MOR-CDC table stays **correct** but degrades in read latency and cost — hence 🟡, not a
blocker.

---

## §2 — v1 ownership: external + hooks (G-25 decision)

For v1 maintenance is the **operator's responsibility**, run with mature external tooling (Spark/Trino
Iceberg `rewrite_data_files` / `rewrite_position_delete_files` / `expire_snapshots` /
`remove_orphan_files`, or a managed-catalog equivalent). The CDC app does **not** run maintenance
itself in v1.

Rationale: **iceberg-rust's rewrite/compaction APIs are immature** in the pinned version; shipping a
half-built compactor is worse than delegating to battle-tested engines. The app instead makes
maintenance *triggerable* by exposing the signals (§3) and reserves the config (§4) so a built-in
scheduler can land later without a config break.

**The app's obligation:** never assume maintenance has run — planning and writes must tolerate deep
delete-file stacks and many small files (just slower).

---

## §3 — When-needed signals (metrics the app must expose)

So operators / automation know *when* to run maintenance, the app emits per-table:

- `cdc_iceberg_delete_files` (gauge) — equality-delete files since last compaction; the primary MOR
  health signal.
- `cdc_iceberg_data_file_count` and `cdc_iceberg_small_file_ratio` (files below target size).
- `cdc_iceberg_snapshot_count` and `cdc_iceberg_manifest_depth`.
- `cdc_iceberg_metadata_json_bytes`.

These fold into the metrics catalogue (G-33) and are the trigger inputs for an external maintenance
cron or an alerting rule (e.g. "delete_files > N → run compaction").

---

## §4 — Reserved config (future built-in scheduler)

`sink.maintenance` is **reserved** (design-landed, not built in v1):

```yaml
sink:
  maintenance:                 # reserved — disabled in v1
    enabled: false
    expire_snapshots: { retain_last: 100, older_than: 7d }
    compaction:       { target_file_size: 256MiB, rewrite_delete_files: true }
    remove_orphans:   { older_than: 3d }
    schedule:         "0 3 * * *"
```

Promotion to a real implementation is **gated on iceberg-rust gaining rewrite/compaction support**
(tracked as a §14 research item). When that lands, the natural first step is the **hybrid** — build in
delete-file + data compaction (the MOR-critical, writer-coupled part), keep expiry/orphans optional.

---

## §5 — Config & doc impact

- **Reserved:** `sink.maintenance` block (§16 list) — not active in v1.
- **Metrics:** the §3 signals added to the G-33 catalogue.
- **Research item (§14):** verify iceberg-rust compaction/rewrite API availability against the pinned
  version; it gates any built-in maintenance.
- **Doc edits owed (spec, propose-only):** System-Overview to state external-maintenance-for-v1 + the
  exposed signals; operational runbook to include the external maintenance procedures.

---

## §6 — Resolved decision summary

| Decision | Resolution |
|---|---|
| G-25 ownership (v1) | **External maintenance**; app exposes when-needed signals + reserves `sink.maintenance` |
| MOR criticality | delete-file compaction is the key signal (`cdc_iceberg_delete_files`) |
| built-in path | deferred, gated on iceberg-rust rewrite support; hybrid (compaction-only) is the first step |
