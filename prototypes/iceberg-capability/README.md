# iceberg-capability — iceberg-rust 0.9.1 capability assessment

Companion to `prototypes/lease-conformance/`. It answers, for the **pinned** crate, whether
iceberg-rust can do the writes the CDC **sink** design needs. The headline result is a 🔴 finding,
so this crate is now primarily a **recorded capability assessment** (with a re-runnable smoke test
for when the crate gains the missing actions), not a passing test suite.

## TL;DR — iceberg-rust 0.9.1 public write API is APPEND-ONLY

Determined by reading the pinned API (`iceberg` **0.9.1**, the latest 0.9; verified on crates.io /
docs.rs, 2026-06):

- ✅ **Writers exist:** `iceberg::writer::base_writer` has `DataFileWriter`, **`EqualityDeleteFileWriter`**,
  and `PositionDeleteFileWriter`. Delete files *can be written*.
- 🔴 **But there is no way to COMMIT deletes.** The whole `Transaction` commit surface is:
  `fast_append()` → `add_data_files(...)` (**data files only**), `update_table_properties`,
  `replace_sort_order`, `update_location`, `update_statistics`, `upgrade_table_version`, `commit`.
  **No row-delta / overwrite / rewrite / delete-commit action, and no `update_schema`.** The module
  contains only `Transaction`, `ActionCommit`, and the `ApplyTransactionAction` trait.

**Consequence:** the MOR-equality-delete sink (the system default) is **not buildable on 0.9.1**, and
the **COW fallback is equally blocked** (it needs an overwrite/replace-files commit, also absent). A
CDC sink that mirrors UPDATEs/DELETEs cannot be implemented on this crate version's public API. Full
analysis + options (track upstream / low-level commit / JVM bridge / contribute upstream) in
`doc/analysis/CDC_Design_Gap_Analysis_v1.md` **§24.2.1**.

## Gate verdicts (against 0.9.1)

| Gate | DA-ID | Verdict | Notes |
|---|---|---|---|
| G1 | DA-SINK-1 | 🔴 **Unsupported** | equality-delete *writer* exists; **no commit path** for delete files. Blocks the sink. |
| G2 | DA-SINK-2 | ⏸ Moot | snapshot-summary props are irrelevant until deletes can be committed. |
| G3 | DA-SCHEMA-8 | 🔴 **Unsupported** | no `update_schema` transaction action in 0.9.1's public API. |
| G4 | DA-TYPE-5 | 🟡 Likely OK | uuid is a schema-definition concern, independent of the commit path (not yet exercised here). |
| G5 | DA-SINK-10 | 🔴 (expected) | no rewrite/compaction API — confirms "external maintenance for v1." |

What *does* work on 0.9.1: catalog + namespace + table creation, **append-only** data ingestion
(`fast_append`), and reads. That is the slice a re-runnable smoke test would cover.

## Re-running when the crate updates

The single thing to re-check on any iceberg-rust bump: **does `Transaction` gain a delete-aware commit
action** (row-delta / overwrite / merge-append, or a `fast_append` that accepts delete-content files)?

```bash
cargo build -p iceberg-capability                 # default: no iceberg deps, green
cargo build -p iceberg-capability --features probe # pulls iceberg; compile the probe
```

When a delete-commit action lands, implement G1 for real:
1. MemoryCatalog (`iceberg::memory::MemoryCatalogBuilder`) over a temp `file://` warehouse.
2. MOR table with `identifier-field-ids`.
3. C1: write a data file (`DataFileWriterBuilder` → `ParquetWriterBuilder` → `RollingFileWriterBuilder`),
   commit via `fast_append().add_data_files(...).apply(tx)` then `tx.commit(&catalog)`.
4. C2: write `EqualityDeleteFileWriter` (delete K) + a data file (insert K'), commit **both in one
   transaction via the new delete-aware action**, then assert the read shows only K' and that the
   delete-file sequence number equals the data-file sequence number. Cross-read with Spark/Trino.

## Dependencies

`iceberg = "0.9"` (→ 0.9.1) only, behind the `probe` feature. Notes:
- `iceberg-catalog-memory` is **yanked at every version** — the memory catalog is in the core `iceberg`
  crate now (`iceberg::memory::MemoryCatalogBuilder`).
- Do **not** pin `arrow-*` independently — iceberg 0.9.1 builds against **arrow 57.1**; use iceberg's
  re-exported arrow (or pin `arrow-array = "57"`) to avoid a version clash.
