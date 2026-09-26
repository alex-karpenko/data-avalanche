# iceberg-capability — iceberg-rust capability assessment

Companion to `prototypes/lease-conformance/`. It answers, for the **pinned** crate, whether
iceberg-rust can do the writes the CDC **sink** design needs. The headline result is a 🔴 finding,
so this crate is now primarily a **recorded capability assessment**, with a real append-path probe
and a re-runnable canary, not a passing delete-commit test suite.

## TL;DR — iceberg-rust's public write API is APPEND-ONLY (all published versions)

Determined by reading the compiled crate (`iceberg` **0.10.1** on disk) and the repo's
`v0.11.0-rc.1`/`v0.11.0-rc.2` sources (recheck **2026-09-19**; first recheck with a working
toolchain — `cargo test --features probe` is green here):

- ✅ **Writers exist:** `iceberg::writer` has `DataFileWriter`, **`EqualityDeleteFileWriter`**,
  and `PositionDeleteFileWriter`. Delete files *can be written*.
- 🔴 **But there is no way to COMMIT deletes.** The whole `Transaction` commit surface is:
  `fast_append()` → `add_data_files(...)` (**data files only**), `update_table_properties`,
  `replace_sort_order`, `update_location`, `update_statistics`, `upgrade_table_version`,
  `update_schema`, `expire_snapshots`, `commit`. **No row-delta / overwrite / rewrite /
  delete-commit action.** Confirmed: 0.9.1 (2026-06), 0.10.0/0.10.1 (2026-07/08), and the
  **v0.11.0-rc.1/rc.2** surfaces (git tags after 0.10.1) — same set.

**Consequence:** the MOR-equality-delete sink (the system default) is **not buildable on any
published iceberg-rust**, and the **COW fallback is equally blocked** (it needs an
overwrite/replace-files commit, also absent). A CDC sink that mirrors UPDATEs/DELETEs cannot be
implemented on the crate's public API. Full analysis + options (track upstream / low-level commit /
JVM bridge / contribute upstream) in `doc/analysis/CDC_Design_Gap_Analysis_v1.md` **§24.2.1**, with
the ongoing rechecks in **§24.2.2–§24.2.4**.

## Gate verdicts (recheck 2026-09-19, against compiled 0.10.1)

| Gate | DA-ID | Verdict | Notes |
|---|---|---|---|
| G1 | DA-SINK-1 | 🔴 **Unsupported** | equality-delete *writer* exists; **no commit path** for delete files. Blocks the sink. |
| G2 | DA-SINK-2 | 🟢 **flash-verified (append path)** | `tests/probe_append_and_summary.rs` commits `cdc.*` snapshot-summary properties atomically with data, reads them back from the catalog after reload, and re-asserts a monotonic fence value across two commits. Full gate re-check when a delete-commit path exists. |
| G2b | DA-X-10 / DA-X-11 | 🟢 **verified** + ⚠️ finding | `tests/probe_epoch_property.rs` (2026-09-26): `cdc.owner_epoch` table property + append = one metadata commit; property survives foreign commits and expiry. **Finding:** `Transaction::commit` rebases a stale zombie — see below. |
| G3 | DA-SCHEMA-8 | 🟡 Partial | `update_schema` exists; drop/rename/promotion extent still unverified. |
| G4 | DA-TYPE-5 | 🟡 Likely OK | uuid is a schema-definition concern, independent of the commit path (not yet exercised here). |
| G5 | DA-SINK-10 | 🟡 Partial | `expire_snapshots` landed; rewrite/compaction still absent (external maintenance for v1). |

What *does* work on 0.10.1: catalog + namespace + table creation, **append-only** data ingestion
(`fast_append`) with custom snapshot-summary properties, and reads. That is the slice the
append-path probe covers.

## G2b — the owner-epoch table property (DA-X-10), 2026-09-26

The Iceberg fence carrier (`Clustering_Architecture_Research.md` §5.5): every sink data commit writes
`cdc.owner_epoch` both into the snapshot summary and as a **table property**, in one transaction; the
fence reads the property. Four probes (MemoryCatalog, iceberg 0.10.1):

| Probe | What | Result |
|---|---|---|
| G2b-1 | `update_table_properties` + `fast_append` in one `Transaction` | ✅ **one** metadata commit (either action order); property + summary both present after reload |
| G2b-2 | foreign append, foreign property edit, `expire_snapshots` of our snapshot | ✅ property survives all three; the summary walk-back finds nothing after expiry — why the property is authoritative |
| G2b-3 | zombie (epoch 5, stale `Table`) commits after the new owner (epoch 6) via plain `Transaction::commit` | ⚠️ **lands**: re-parented onto the new owner's snapshot, property **regressed 6 → 5** |
| G2b-4 | same, committed through a pinned-base catalog view | ✅ rejected (`assert-ref-snapshot-id(main)` failed); **neither** property nor snapshot applied; reloaded zombie refused by the fence pre-check; foreign append under the pin → conflict → re-pin commits; foreign metadata-only edit not clobbered |

**Finding (DA-X-11).** `Transaction::do_commit` calls `catalog.load_table` and re-applies every action
on **each** attempt, including the first (`iceberg-0.10.1/src/transaction/mod.rs`, `do_commit`). The
requirements (`assert-ref-snapshot-id`) are therefore derived from the *fresh* metadata, and the
metadata-pointer CAS never sees a stale base. A fence check done by the caller before `commit()` has a
TOCTOU gap that this rebase fills.

**Fix on the public API (no fork).** `PinnedBase` — a per-commit `Catalog` wrapper whose `load_table`
returns the base the fence was checked against and whose `update_table` delegates, turning
`CatalogCommitConflicts` into a non-retryable error. `Transaction` then does not rebase, and the
catalog's requirement check makes the commit a compare-and-swap on `main`. The sink owns the retry:
reload → re-check fence → re-pin → recommit. `TableCommit::build` and `TransactionAction` are
`pub(crate)`, so a wrapper is the only public way to control the base.

Residual: the pin fences on `main`, so a new owner's metadata-only commit (schema-only `evolve`)
does not stop a pinned zombie; one stale commit can land before the new owner's next data commit
(same window the design already accepts; idempotency absorbs it). Not covered here: whether the
Glue / HMS / REST catalogs evaluate requirements atomically with the pointer swap (MemoryCatalog does,
under its lock) — a real-catalog check, same class as delta P8.

```bash
cargo test --features probe --test probe_epoch_property -- --nocapture
```

## Upstream status (2026-09-19)

- The epic `apache/iceberg-rust#2186` ("Copy-on-Write and Merge-on-Read support") is **open**.
- **Correction:** §24.2.3's "`OverwriteAction` merged" lead was wrong — PR **#2185** is **open,
  unmerged**. Treat epic task-list checkboxes as unconfirmed until the PR's own state says merged.
- Real upstream work exists but nothing is merged or released: **#2203** (RowDelta, open),
  **#2785/#2678** (RowDeltaAction follow-ups, **closed unmerged**), **#2752** (core COW rewrite
  primitive, open), and **#3046** (MergingSnapshotProducer + RewriteFilesAction, open — stated as
  the *first* PR of a series that will add OverwriteFiles, RowDelta, and conflict detection).
  An RFC **#2620** "Stateful Transaction Action" was opened 2026-09-19.
- **v0.11.0-rc.1/rc.2** (git tags; MSRV 1.95; not on crates.io) ship **without** any delete-aware
  action — so the next release will not unblock the sink either.

## Re-running when the crate updates

The single thing to re-check on any iceberg-rust bump: **does `Transaction` gain a delete-aware commit
action** (row-delta / overwrite / merge-append, or a `fast_append` that accepts delete-content files)?

```bash
cargo test  -p iceberg-capability                            # default: no iceberg deps, green
cargo build -p iceberg-capability --features probe           # pulls iceberg; canary + probe compile
cargo test  -p iceberg-capability --features probe           # runs the append-path (G2, G2b) probes
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

`iceberg = "0.10"` (→ 0.10.1) only, behind the `probe` feature. Notes:
- `iceberg-catalog-memory` is **yanked at every version** — the memory catalog is in the core `iceberg`
  crate now (`iceberg::memory`).
- MSRV: iceberg 0.10.1 is `rust-version 1.94` (main: 1.95). This probe compiles on 1.98.1.
- Do **not** pin `arrow-*` independently — iceberg 0.10.x builds against **arrow 58**; use iceberg's
  re-exported arrow to avoid a version clash.
