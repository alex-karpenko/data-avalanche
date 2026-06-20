---
updated: 2026-06-20
domain: Architecture
tags: [iceberg, partitioning, partition-spec, equality-delete, mor, cow, strategy, layout-evolution, cdc]
resolves: [G-24, G-12a]
---

# Iceberg Partitioning & Layout Evolution

## Scope

Closes **G-24** (partitioning strategy) and **G-12a** (MOR↔COW strategy-change migration). Both are
"layout" decisions that share one principle: **a layout change applies to new data only; history is
never auto-rewritten; full migration is an explicit re-snapshot.**

Builds on G-13 (equality-delete identifier + before-image) and G-6 (incremental re-snapshot).

---

## §1 — Partition spec config (G-24)

`tables.partitioning` — an ordered list of partition fields, each an Iceberg transform over a column:

```yaml
tables:
  partitioning:
    - { column: _ts,       transform: day }            # CDC commit-time pruning
    - { column: tenant_id, transform: bucket, n: 16 }
```

- **Transforms:** `identity`, `bucket` (`n`), `truncate` (`width`), `year` / `month` / `day` / `hour`.
- **Metadata columns allowed:** `_ts` (commit timestamp) is a valid partition source and the natural
  time-pruning choice.
- **Default:** **unpartitioned.** No layout is imposed unless configured; `day(_ts)` is documented as
  the common opt-in.

Validation: column exists; transform ∈ the set; temporal transforms require a date/time column;
`bucket.n ≥ 1`, `truncate.width ≥ 1`.

---

## §2 — Partition-key mutation vs equality deletes (the correctness wrinkle)

Iceberg equality deletes are **partition-scoped** — a delete file applies within its partition. If an
UPDATE moves a row to a **different** partition (e.g. `day(updated_at)` changes), an equality delete
written in the new partition will **not** remove the old version in the old partition → a cross-
partition ghost row.

**Resolution (D-24, chosen): handle via the before-image.** A partition-value change is treated exactly
like a **PK mutation** (G-13 §4): emit the **equality-delete in the OLD partition** (computed from the
before-image the source already provides — PG replica identity / MySQL FULL image) and the **data
record in the NEW partition**. Any column may be a partition source; no ghost rows. This reuses
machinery already mandated by G-13, so it costs nothing extra.

> Implication for the writer: the partition value of a delete is derived from the **before-image**, not
> the after-image — the writer must route the equality-delete to the old partition. Add this to the
> sink-writer contract.

---

## §3 — Partition-spec evolution (reload)

Changing `tables.partitioning` on a live table applies as **Iceberg partition-spec evolution**: the new
spec governs **new data files**; existing data keeps the spec it was written under (Iceberg natively
supports mixed specs and prunes each correctly). **No rewrite of history.** Mutability class **PR**
(the writer must re-initialize its partitioner). Operators wanting a uniform physical layout run a
compaction/rewrite as a separate maintenance action (G-25) or a re-snapshot into a fresh table.

---

## §4 — MOR ↔ COW strategy change (G-12a)

Same principle. `sink.strategy` (default `mor`) change via reload:

- **Applies to new writes only.** Existing data files and delete files keep their form; Iceberg readers
  handle a table that contains both COW-rewritten and MOR delete-augmented data.
- **No automatic migration.** Switching `mor → cow` does **not** retroactively materialize existing
  delete files into rewritten data; switching `cow → mor` does not split existing files.
- **Migration path** (explicit, operator-initiated): either (a) run Iceberg compaction to fold MOR
  delete files into data files (effectively COW for history), or (b) re-snapshot the table (incremental,
  G-6) into a fresh table under the new strategy. Documented; not automatic.
- Mutability class **PR**.

This closes the G-12a "applies to new tables only + migration path" gap.

---

## §5 — Config & validation impact

**Added / promoted**

- `tables.partitioning: [ {column, transform, ...} ]` (PR, default none) — promoted from §16 (G-24).
- `sink.strategy` already exists (V-023 mor/cow); add the **new-data-only** semantics + the documented
  migration note (G-12a).

**New validation rules**

1. Partition column exists; transform ∈ {identity, bucket, truncate, year, month, day, hour}.
2. Temporal transforms (year/month/day/hour) require a date/time-typed column.
3. `bucket.n ≥ 1`; `truncate.width ≥ 1`.
4. (Warning) partitioning on a high-cardinality `identity` column (small-files risk).

**Doc edits owed (spec, propose-only):** sink-writer contract — equality-delete partition routed by
the **before-image**; `sink.strategy` change documented as new-data-only with the migration path.

---

## §6 — Resolved decision summary

| Decision | Resolution |
|---|---|
| G-24 spec | `tables.partitioning` list of transform fields; default unpartitioned; `_ts` allowed |
| G-24 mutation | partition-key change handled via before-image (delete old partition + insert new) |
| G-24 evolution | partition-spec evolution = new data only, no rewrite (PR) |
| G-12a strategy | MOR↔COW change = new data only; explicit compaction/re-snapshot to migrate (PR) |
