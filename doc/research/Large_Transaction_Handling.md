---
updated: 2026-09-30
domain: Data Flow
tags: [large-transaction, flush, atomicity, memory, backpressure, split-commit, idempotency, cdc]
resolves: [G-29]
---

# Large Transaction Handling

## Scope

Closes **G-29**. Reconciles the **"flush only on a transaction boundary, never mid-transaction"** rule
(Data-Flow.md, reused by the schema-evolution apply protocol §8) with **bounded memory**. The rule
guarantees that a single large source transaction (e.g. a 10M-row backfill `UPDATE`) accumulates
entirely in memory before it can flush.

Builds on G-13 (PK-upsert idempotency) and G-14 (per-table committed position, `SourcePosition`).

---

## §0 — The trilemma

For an oversized transaction you cannot have all three of **bounded memory**, **atomic transaction
visibility**, and **no spill-to-disk**. The decision picks two:

- **Bounded memory + atomic visibility** ⇒ requires **spill** (deferred, §5).
- **Atomic visibility + no spill** ⇒ unbounded memory ⇒ OOM or halt.
- **Bounded memory + no spill** ⇒ relax atomic visibility (**the chosen default-capable mechanism**).

This system already abandoned *cross-table* atomicity (G-14 §5) and consumers already live with the
`_lsn`/`_seq` dedup model, so relaxing *intra-table* atomicity **for oversized transactions only** is
consistent and cheap.

---

## §1 — Mechanism: bounded split commits (D-29.1)

When a transaction's buffered rows exceed **`sink.max_transaction_batch_rows`** (when set), the writer
flushes **partial-transaction Iceberg commits** instead of waiting for the source commit:

1. Accumulate until the threshold; flush the partial batch as a normal Iceberg commit (data visible).
2. **Pin the durable resume checkpoint at the transaction's START** — do **not** advance
   `cdc.committed_position` past the txn until the source `COMMIT`/`XID` is seen.
3. Continue accumulating the next slice; repeat.
4. On the source commit, flush the final slice and **advance the checkpoint** to the txn's end
   position.

### Why this is crash-safe (the G-13/G-14 payoff)

A crash mid-split-transaction loses the checkpoint advance (step 2 pinned it at txn start), so on
restart the stream **replays the entire transaction**. The already-flushed slices are re-written as
**idempotent upserts** (keep-last by `_seq`, equality-delete keyed) — re-writing the same rows
produces the identical table state. No duplicates, no loss. The only externally observable effect is
that a concurrent reader may briefly see a **partially-applied** large transaction.

**The position guard for a slice (D-M3.14, `doc/v1.0/M3-core-data-path.md`; spec edit S-P,
2026-09-30).** A slice's range ends at the pinned transaction start, and from the first slice on the
table's stored `committed_position` equals that start. The ordinary guard ("the stored position
covers the end → skip") would therefore skip every slice after the first: **data loss**. Never
guarding slices is wrong too: on a replay after the transaction and later commits have landed, a
slice would roll its rows back to their state inside the transaction, and the later commits would
then be guard-skipped. The rule is: **skip a slice only when the stored position is strictly past
its transaction's start** (the commit, or a later one, has landed). Every skip decision is then
monotone in stream order, and a replay from any checkpoint converges; the M3 property tests prove it,
and fail with either of the other two rules.

**After the last slice.** What the transaction writes after its last slice needs no "last slice"
marker: it goes out with the next regular flush, whose range ends at the commit, under the ordinary
guard. That flush may also carry later transactions.

### What is given up, stated honestly

Strict "all-or-nothing visibility at the instant of source commit" no longer holds **for transactions
that exceed the threshold**. Transactions under the threshold keep full atomic visibility (single
commit). The relaxation is bounded to the pathological case and is documented as a known semantic.

---

## §2 — Default and the memory backstop (D-29.2)

- **`sink.max_transaction_batch_rows` default = `null` (unlimited)** → strict atomic visibility is the
  **default**: no splitting, a transaction flushes as one commit. Operators with large transactions opt
  in by setting a value.
- **Absolute memory backstop** — independent of the above, **`sink.max_transaction_buffer_bytes`**
  (default a sane ceiling, e.g. 1 GiB) bounds a single transaction's in-memory accumulation. If a
  transaction reaches the ceiling **and splitting is disabled** (`max_transaction_batch_rows = null`),
  the **pipeline halts** with a clear error (`pipeline_status = error`, metric + log) — it never
  OOM-kills the worker and its sibling pipelines. If splitting **is** enabled, the ceiling is just a
  flush trigger (split early), not a halt.

This gives a safe default (atomic semantics preserved; a runaway txn halts one pipeline, not the box)
while letting operators trade atomicity for memory deliberately.

---

## §3 — Interaction with other mechanisms

- **Back-pressure (G-27)** — splitting and the byte backstop sit *above* the bounded inter-task
  channels; G-27 bounds the channels, this bounds a single transaction's accumulator. Both feed the
  same `sink.buffer_capacity_*` family; tracked together.
- **Schema-evolution apply protocol (§8)** — its seal→flush→evolve sequence is a transaction-boundary
  operation; a DDL is its own boundary and is never split. No conflict.
- **Exactly-once guard (G-14)** — the checkpoint-pinned-at-txn-start rule is what keeps the per-table
  `committed_position` correct across a split; intermediate commits advance the table's data but not
  the pipeline resume frontier.

---

## §4 — Config & validation impact

**Added fields**

- `sink.max_transaction_batch_rows` (R, default `null` = unlimited). When set, enables split commits at
  that row count.
- `sink.max_transaction_buffer_bytes` (R, default e.g. `1GiB`). Absolute per-transaction memory
  ceiling; halt-if-unsplittable, flush-trigger-if-splittable.

**New validation rules**

1. `max_transaction_batch_rows` ≥ 1 when set; warn if very small (commit amplification).
2. `max_transaction_buffer_bytes` ≥ a floor (e.g. 16 MiB).
3. (Doc) when `max_transaction_batch_rows` is set, surface the relaxed-atomic-visibility note.

**Doc edits owed (spec, propose-only):** Data-Flow.md flush-decision section to state the split rule
and the checkpoint-pinning; the §16 reservation for `sink.max_transaction_batch_rows` is promoted.

---

## §5 — Deferred: spill-to-disk

A future `sink.large_txn_mode: split | spill` could add **spill** — buffer the oversized txn to local
disk / object storage and commit atomically — preserving atomic visibility with bounded memory. It is
out of scope for v1 (adds a spill store, lifecycle, and cleanup) and is noted so the config surface can
grow without churn. Until then, atomicity-critical deployments keep `max_transaction_batch_rows = null`
and size memory for their largest transaction.

---

## §6 — Resolved decision summary

| Decision | Resolution |
|---|---|
| D-29.1 mechanism | **Bounded split commits**; checkpoint pinned at txn start until commit; crash-safe via idempotency |
| D-29.2 default | `max_transaction_batch_rows = null` (atomic by default); `max_transaction_buffer_bytes` backstop **halts** an unsplittable runaway, never OOMs |
| atomicity trade | strict visibility kept for sub-threshold txns; relaxed only for oversized txns, documented |
| spill | deferred to a future `large_txn_mode: spill` |
