---
updated: 2026-06-20
domain: Data Flow
tags: [iceberg, equality-delete, dedup, primary-key, mor, ghost-row, replica-identity, cdc]
resolves: [G-13, G-30]
sources: [Apache Iceberg Rust 0.9.0 Release]
---

# Ghost-Row Deduplication and Primary-Key Keying

## Scope

This document closes **G-13** (ghost rows / equality-delete keying) and **G-30** (PK-less tables).
It specifies how the sink writer derives a table's *identifier*, how a flush batch is collapsed
before it reaches the Iceberg writer, how source DELETEs and key mutations are manifested, and what
before-image the source must emit for any of this to be correct.

It assumes the existing sink defaults: **MOR with equality deletes** (System-Overview §sink, sink
note in Data-Flow), the `_op/_ts/_lsn/_source_db/_pipeline` metadata columns, and the Phase-4
`committed_lsn` exactly-once guard.

---

## §0 — The one rule everything rests on

An Iceberg **equality delete** is keyed on a table's *identifier columns* and Iceberg applies it
**only to data files whose data-sequence-number is strictly less than the delete file's**. This single
property is simultaneously:

- what makes an in-batch upsert correct — a "delete old key + insert new key" written in the *same*
  commit shares one sequence number, so the delete purges every **prior-commit** version of the key
  while sparing the **same-commit** insert; and
- the source of the **ghost-row** bug — if a row that must disappear is written as a data record in
  the same commit as its own equality delete, the strictly-less-than rule spares it and it survives
  forever.

Every decision below exists to keep the writer on the correct side of this rule.

---

## §1 — Identifier source and precedence (D-13.1, D-13.7)

The **identifier** is the column set used as Iceberg `identifier-field-ids` and as the equality-delete
key.

1. **Default — inferred source PK.** Discovered via the channel-C catalog query
   (PG: the primary-key / REPLICA IDENTITY index from `pg_index`; MySQL: the PK from
   `information_schema`). No configuration required for the common case.
2. **Override — `tables.primary_key: [cols]`.** Lets the operator choose a different identifier than
   the declared PK — required to support PostgreSQL `REPLICA IDENTITY USING INDEX` on a *non-PK*
   unique index, and useful when the natural PK is unsuitable. The override is **not** a surrogate
   key (see §3): it names columns that genuinely identify a row and that the source *already* emits in
   its before-image.

`tables.primary_key` is **not** the rejected `surrogate_key`. It selects an existing identity; it does
not invent one.

### Identifier constraints (Iceberg-imposed, validated at config/startup)

An identifier column must be:

- **top-level** (not nested inside a struct/list/map),
- **primitive**,
- **required / NOT NULL** (Iceberg forbids null identifier values; equality on NULL is undefined),
- **non-floating** (`float`/`double` are rejected — equality on floats is unsafe).

A source PK always satisfies NOT-NULL. An override that names a nullable or floating column is rejected
with a specific error.

---

## §2 — Before-image / REPLICA IDENTITY contract (D-13.4)

Equality deletes for UPDATE and DELETE need the **old** identifier values. The source must emit them:

| Engine | Requirement | Action if unmet |
|---|---|---|
| PostgreSQL | Identifier columns ⊆ the table's REPLICA IDENTITY columns. `DEFAULT` covers the PK (sufficient when identifier = PK); a `tables.primary_key` override onto non-PK columns needs `FULL` or `USING INDEX` covering them. | **New validation** — halt the table with a message naming the missing columns. |
| MySQL | `binlog_row_image = FULL` (already a required precondition, config §13) → full before-image always present. | No new constraint. |

**Operational caveat:** forcing PostgreSQL `REPLICA IDENTITY FULL` to cover a non-PK identifier makes
the source log the *entire* old row in WAL on every UPDATE/DELETE — a real bandwidth cost. Document it;
prefer `USING INDEX` over `FULL` when only a unique index is needed.

---

## §3 — PK-less tables: halt (D-13.2, resolves G-30)

A table with **no PK and no `tables.primary_key` override halts** — the existing System-Overview line 50
contract is kept verbatim. A PK-less table found via a glob is skipped with a warning, not halted.

This **resolves G-30 by rejection**: the previously-reserved `tables.surrogate_key` field is **removed**
from the config §16 deferred list, and no append-only/changelog capture mode is added. Rationale: a
surrogate key's correctness rests on an operator's uniqueness claim the system cannot cheaply verify, and
a wrong claim silently corrupts dedup. Halting is safe and unambiguous.

**Accepted limitation:** genuinely key-less tables (event logs, some junction tables) cannot be
replicated and have no escape hatch. If a future requirement forces this, the re-opening path is an
explicit append-only changelog mode — *not* a surrogate equality-delete key.

---

## §4 — Intra-batch collapse and ordering (D-13.3, D-13.6)

Before a flush batch reaches the Iceberg writer it is collapsed to **at most one delete and at most
one data record per identifier tuple**.

### Sequence key

Keep-last ordering uses a **per-change** sequence `_seq`, **not** the commit position:

- **PostgreSQL** — the change's own WAL LSN (unique and monotonic within and across transactions).
- **MySQL** — `(binlog_file, end_log_pos)`, or GTID + intra-transaction event index.

`_lsn` (the *commit* LSN/GTID, the metadata column) is **insufficient** for keep-last because many
rows share one commit position; `_seq` is finer. (This finer ordering is also what G-14's dedup guard
will build on.)

### Collapse rules (per identifier tuple, after sorting by `_seq`)

| Last op for the key in the batch | Emit |
|---|---|
| INSERT / UPDATE (`I`/`U`/`r`) | equality-delete(old key) **+** one data record (new image) |
| DELETE (`D`) | equality-delete only, **no data record** |

- **Delete representation is mirror semantics:** a DELETE never appears as a data row; `_op` in data
  files only ever holds `I`, `U`, or `r`. Appending a `D` row *and* an equality-delete in the same
  commit would leave a ghost (§0).
- **The equality-delete is always safe to emit** even for a key with no prior data file — it is a
  harmless no-op against earlier commits.
- **Equality-delete files carry only the identifier columns** (the writer projects them).

### Key mutation (UPDATE changes the PK value)

An UPDATE that changes an identifier value is **two distinct identifier tuples**: it collapses to an
equality-delete on the **old** key plus a data record for the **new** key. This is why §2's
before-image contract is load-bearing — without the old key the delete cannot be formed.

---

## §5 — Write strategy interaction (D-13.5)

- **MOR (default)** — the equality-delete path above. No read of existing data required.
- **COW** — would require read-modify-rewrite of whole data files (a read per affected file, far
  costlier) and carries the **same** identifier requirements. Keep MOR the default; a `sink.strategy`
  change applies to **new tables only** (see G-12a) and does not retro-convert.

---

## §6 — Schema-evolution consistency (no new decision)

Two properties already align G-13 with `Schema_Change_Detection_and_Evolution.md` §6–§7.2:

- **Provable rename preserves the identifier automatically.** Iceberg `identifier-field-ids` reference
  **field IDs, not names**, and a proven rename (§6) keeps the field ID and data. No re-keying needed.
- **PK / REPLICA IDENTITY change is Unsafe → halt + re-snapshot** (§7.2). This is exactly correct: it
  is the one change that invalidates `identifier-field-ids`. Likewise `DROP COLUMN` of an identifier
  column is Unsafe. No conflict to resolve.

Because changing `tables.primary_key` on a live table is equivalent to a PK change, the field's
mutability class is **PR**, and changing it forces the same re-snapshot path as an Unsafe DDL.

---

## §7 — Config surface and validation rules

**Added**

- `tables.primary_key: [col, ...]` — optional identifier override. Mutability **PR**; change ⇒
  re-snapshot.

**Removed**

- `tables.surrogate_key` — struck from the config §16 deferred list (G-30 rejected).

**New validation rules (for the §13 master table)**

1. Each identifier column is top-level, primitive, NOT NULL, non-floating — else error/halt.
2. (PostgreSQL) identifier columns ⊆ REPLICA IDENTITY columns — else halt with the missing-column list.
3. A table with no inferred PK and no `tables.primary_key` halts (config-time error if explicitly in
   `tables`; load-time warning + skip if matched via glob).
4. `tables.primary_key` referencing an unknown column — error.

---

## §8 — Implementation gate (research item)

The whole mechanism assumes **iceberg-rust 0.9.0 exposes an equality-delete writer that assigns delete
files the enclosing commit's sequence number**, so the §0 strictly-less-than guarantee holds. This
must be verified against the pinned crate version before the sink writer is implemented; if equality
deletes are unsupported or mis-sequenced, the writer needs a workaround (e.g. position deletes after a
read, or a higher write strategy). Tracked as a new entry in the gap report §14 "missing research
topics."

---

## §9 — Resolved decision summary

| Decision | Resolution |
|---|---|
| D-13.1 identifier source | Infer source PK (channel C); `tables.primary_key` override |
| D-13.2 PK-less tables | **Halt** (line 50 kept); G-30 rejected, `surrogate_key` removed |
| D-13.3 dedup ordering | Collapse ≤1 delete + ≤1 data per identifier; keep-last by per-change `_seq` |
| D-13.4 before-image | identifier ⊆ replica-identity (PG); MySQL FULL image already required |
| D-13.5 strategy | MOR/equality-delete primary; COW new-tables-only, same identifier rules |
| D-13.6 delete representation | **Mirror** — equality-delete only, no `D` data row |
| D-13.7 `primary_key` override | **Kept** (supports `USING INDEX` / non-PK identifier) |
