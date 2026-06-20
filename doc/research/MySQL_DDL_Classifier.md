---
updated: 2026-06-20
domain: Schema Evolution
tags: [mysql, ddl, sqlparser, classifier, channel-c, schema-evolution, rename, cdc]
resolves: [G-19a]
---

# MySQL DDL Classifier (in-band, channel B)

## Scope

Closes **G-19a**. The schema-evolution spec already *reduced* this from "a full MySQL ALTER-delta
parser" to "a DDL classifier + table-name extractor (+ optional rename intent)" because **channel C**
(catalog augmentation) supplies the actual column delta. This document fixes the parser strategy and —
the only real open decision — its **failure mode**.

Builds on `Schema_Change_Detection_and_Evolution.md` (channels A–D, §7.2 classification table).

---

## §1 — The classifier is a router, not a delta parser

Channel B parses the MySQL binlog `QUERY_EVENT` DDL text. Its job is **not** to compute what changed —
that is channel C's job (a catalog query at the exact stream position, producing the authoritative
delta classified by §7.2). Channel B only needs to:

1. **Determine the affected table name(s)** — to fire channel C at the right stream position.
2. **Detect rename intent** (`RENAME` / `CHANGE COLUMN`) — the provable-rename signal (§6 of the schema
   doc), so a rename keeps its Iceberg field ID instead of degrading to drop+add.
3. **Optionally pre-classify** obvious table-level ops (DROP/TRUNCATE/RENAME TABLE) for fast pathing.

Because the catalog delta is the source of truth, **the parser's coverage gaps are not correctness
risks** — a missed parse degrades to "let channel C decide," not to silent data loss.

---

## §2 — Two-tier, fail-soft parsing (D-19a)

```
QUERY_EVENT DDL text
   │
   ├─ Tier 1: sqlparser-rs (MySqlDialect)
   │     └─ on success → structured: {verb, affected_tables[], rename_intent}
   │
   └─ Tier 1 fails (unsupported syntax) →
         Tier 2: lightweight verb + table-name extractor (regex/tokenizer)
            ├─ leading verb ∈ {ALTER, CREATE, DROP, RENAME, TRUNCATE}
            ├─ backtick-/quote-aware table-name extraction (incl. db.table)
            ├─ rename_intent = contains(RENAME | CHANGE) heuristically
            └─ table name recovered?
                 ├─ yes → TRIGGER CHANNEL C (catalog delta classifies via §7.2)
                 └─ no  → HALT the implicated table(s) conservatively (cannot locate the change)
```

- **Tier 1 success** is the common path — clean classification + reliable rename intent.
- **Tier 1 failure** does not halt: Tier 2 recovers the table name and lets **channel C** drive
  evolution. Only a total failure to identify *any* table halts (conservative, rare).
- A **provable rename** still requires Tier-1 structure (or MySQL's `attnum`-equivalent column position
  from channel C); if only Tier 2 fired, rename degrades to **drop+add with a warning** (§7.2's "not
  provable" branch) — correct, just lossy-of-history under the new name.

---

## §3 — Routing and table-level ops

- **DDL on an untracked table** → ignored (no channel-C trigger). Determined by matching the extracted
  name against the resolved include−exclude set.
- **Multi-table DDL** (`RENAME TABLE a TO b, c TO d`) → extract all names; route each.
- **`DROP TABLE` (tracked)** → halt that table (stop replicating); Iceberg artifacts retained.
- **`TRUNCATE TABLE` (tracked)** → a data operation, not schema: emit as a table-clear. For the MOR
  mirror this is an Iceberg `DELETE`/overwrite of all rows (or a fresh snapshot boundary), per §7.2's
  data-vs-schema split — flagged for the sink-writer contract.
- **`RENAME TABLE` (tracked)** → table-identity change; classified per §7.2 (table-level).

---

## §4 — Failure modes & fail-safe

| Situation | Behavior |
|---|---|
| Tier-1 parse ok | structured classify + rename intent |
| Tier-1 fails, table recovered | trigger channel C; rename → drop+add+warn if not provable |
| Neither tier recovers a table name | **halt** implicated table(s) (fail-safe) |
| DDL on untracked table | ignore |
| `binlog_row_metadata = MINIMAL` | names come from channel C anyway (schema doc §) — unaffected |

The §0 schema-evolution invariant (apply in stream order, scoped to the implicated change, never
fast-forward to "now") is preserved: channel B only *locates* the change; channel C *describes* it; the
apply protocol (§8) sequences it.

---

## §5 — Library & dependency

- **`sqlparser-rs`** with `MySqlDialect` for Tier 1. Pin and track; it is the only new parse dependency
  and now sits on a **fail-soft** path (a coverage gap degrades, not halts), which removes the v7 risk
  that elevated G-19a.
- Tier 2 is a small in-crate tokenizer (no extra dependency).
- This reverses the v7 "blocker" elevation definitively: the parser is a routing optimization over the
  authoritative channel-C delta.

---

## §6 — Resolved decision summary

| Decision | Resolution |
|---|---|
| D-19a parse failure | **Two-tier fail-soft**: sqlparser-rs → regex table-name fallback → channel C; halt only if no table recoverable |
| classifier scope | router (table name + rename intent), not a delta parser — channel C owns the delta |
| table-level ops | DROP→halt, TRUNCATE→data-clear, RENAME→identity change (§7.2); untracked→ignore |
| library | `sqlparser-rs` (MySqlDialect) on a fail-soft path; G-19a de-risked from its v7 elevation |
