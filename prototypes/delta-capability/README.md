# delta-capability — delta-rs capability assessment for the Delta sink

Companion to `prototypes/iceberg-capability/` and `prototypes/lease-conformance/`. It proves, for the
**pinned** `deltalake = "=1.0.0"` (arrow 59, DataFusion 55), the behaviors the Delta sink realization
depends on (`doc/research/Multi_Sink_Architecture_and_Delta_Lake.md` §7.2, §10; gaps DA-MS-1…4).

The sink commit under test is **one MERGE per table per flush**: source = collapsed change set
(upserts ∪ deletes) with a boolean `__avalanche_del` flag; `txn{app_id: avalanche/<pipeline>/owner,
version: owner_epoch}` as the fence carrier; `commitInfo["avalanche.committed_position"]` as the
position carrier.

## Results (2026-09-26, local `file://` tables, release build) — 8/8 green

| Probe | What | Result |
|---|---|---|
| P1 | update + delete + insert in **one** MERGE | ✅ exactly one new table version; content correct |
| P2 | replay the same change set | ✅ content identical (idempotent) |
| P3 | `txn` + `commitInfo` atomic with MERGE; survive reload **and a foreign OPTIMIZE** | ✅ `txn` version survives in table state; latest commit is `OPTIMIZE` without our metadata → walk-back finds our position (confirms DA-X-10 is real) |
| P4 | zombie writer (epoch 5) vs new owner (epoch 6) | ✅ stale-snapshot zombie: `Concurrent transaction failed` (same-`app_id` conflict); reloaded zombie: pre-check `FENCED` |
| P5 | TRUNCATE = delete-all, then MERGE | ✅ metadata-only (0 rows copied, 0 files added) |
| P6 | MERGE cost, 1M rows / 40 files, 5,000 updates | ⚠️ see below |
| P7a | add column on a default table | ✅ `add_columns()`; pre-evolution MERGE shape still commits |
| P7b | add column on a **column-mapping `name`** table | ❌ **refused**: `UnsupportedColumnMapping { mode: Write, operation: "ADD COLUMN" }` (asserted, so a fixed delta-rs flips the test) |

### P6 — write amplification (the Delta cost model, measured)

| Key pattern | Files rewritten | Rows copied | Wall time |
|---|---|---|---|
| (a) sequential — updates to the most recent key range | 1 / 40 | 20,000 | ~55 ms |
| (b) random — keys uniform over the table | **40 / 40** | **995,000** | ~550 ms |

A random-key flush rewrites **every file whose key range contains a changed key** — here the whole
table (≈200× amplification for 5k changes). Cost per flush ∝ table size for uniformly-spread updates;
∝ change volume for time/sequence-correlated updates. (delta-rs reports `files scanned=0 skipped=40`
for (b) while removing 40 files — the scan/skip metrics are unreliable; use `files_removed` /
`rows_copied`.)

### Also checked (outside this crate)

- **P9 arrow alignment** — a scratch project depending on `deltalake =1.0.0` + iceberg-rust `main`
  (`9a2111a`, 2026-09-26) resolves to a **single arrow 59.3 / parquet 59.3**. Resolution only, not a
  joint compile.

### Not covered yet

- **P8** — commit atomicity on real S3 / GCS / Azure (needs the lease-conformance buckets/credentials).
- **P10** — type round-trip (uuid/time as string, decimal(20,0), timestamp vs timestamp_ntz) and
  cross-read by Spark/DuckDB.

## Running

```bash
cargo test                                                     # default: no delta deps, instant
cargo test --release --features probe -- --nocapture --test-threads=1   # ~18 min cold build (DataFusion)
```
