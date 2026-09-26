# delta-capability — delta-rs capability assessment for the Delta sink

Companion to `prototypes/iceberg-capability/` and `prototypes/lease-conformance/`. It proves, for the
**pinned** `deltalake = "=1.0.0"` (arrow 59, DataFusion 55), the behaviors the Delta sink realization
depends on (`doc/research/Multi_Sink_Architecture_and_Delta_Lake.md` §7.2, §10; gaps DA-MS-1…4).

The sink commit under test is **one MERGE per table per flush**: source = collapsed change set
(upserts ∪ deletes) with a boolean `__avalanche_del` flag; `txn{app_id: avalanche/<pipeline>/owner,
version: owner_epoch}` as the fence carrier; `commitInfo["avalanche.committed_position"]` as the
position carrier.

## Results (2026-09-26, local `file://` tables unless noted, release build) — 10/10 green + P8 on S3

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
| P10 | Hop-3b type round-trip (append + MERGE), read back by delta-rs **and cross-read by DuckDB** | ✅ all 15 columns, exact types and values — see below |
| P10b | protocol of the same table without `timestamp_ntz` | ✅ reader 1 / writer 2 (so `timestamp_ntz` alone raises it to 3 / 7) |
| P8 | commit atomicity on **real S3** (us-east-1) — see below | ✅ P8a–P8e; GCS / Azure pending credentials |

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

### P10 — type round-trip (`tests/probe_types.rs` + `scripts/p10_duckdb_crossread.py`)

One table with every Hop-3b row (`doc/research/Type_Mapping.md`), loaded by **append** then fully
re-imaged by the sink's **MERGE**; uuid (FixedSizeBinary(16)) and time (Time64 µs) are converted in the
sink exactly as specified (canonical lowercase uuid string; `HH:MM:SS.ffffff`).

| Column (Delta type) | delta-rs read-back | DuckDB 1.5.5 `delta_scan` |
|---|---|---|
| byte / short / long / boolean | Int8 / Int16 / Int64 / Boolean, extremes exact | TINYINT / SMALLINT / BIGINT / BOOLEAN ✅ |
| `decimal(20,0)` (BIGINT UNSIGNED) | Decimal128(20,0), `u64::MAX−7` exact | DECIMAL(20,0) ✅ |
| `decimal(38,10)` | Decimal128(38,10), 38-digit negative exact | DECIMAL(38,10) ✅ |
| string (text, json, uuid, time) | Utf8View; Unicode / empty / NULL exact | VARCHAR ✅ |
| binary | BinaryView; non-UTF-8 bytes exact (hex-compared) | BLOB ✅ |
| date | Date32 incl. 1969-12-31 | DATE ✅ |
| `timestamp` (with tz) | Timestamp(µs, "UTC") | TIMESTAMP WITH TIME ZONE — an instant ✅ |
| `timestamp_ntz` | Timestamp(µs, None) | TIMESTAMP — wall-clock ✅ (incl. −1 µs pre-epoch) |
| `array<string>` | List<Utf8View>, NULL list exact | VARCHAR[] ✅ |

**Finding — `timestamp_ntz` raises the table protocol.** A table with a `timestamp_ntz` column is
created at **reader v3 / writer v7** with the `timestampNtz` table feature. Engines without Delta
table-feature / `timestampNtz` support cannot read such a table at all. **P10b** confirms the cause:
the same columns without `timestamp_ntz` stay at **reader 1 / writer 2**. Gap DA-MS-8 → decided D-MS.16: native `timestamp_ntz` by default,
`sink.delta.timestamp_ntz_as: timestamp | string` as the opt-out for legacy readers (config V-114).

The cross-read needs no global install: `uv` runs DuckDB in a throwaway environment.

```bash
P10_OUT=/tmp/p10 cargo test --release --features probe --test probe_types -- --nocapture
uv run --no-project --with duckdb --with pytz python scripts/p10_duckdb_crossread.py /tmp/p10
```

### P8 — commit atomicity on real S3 (`tests/probe_s3_commit.rs`, `--features s3`)

delta-rs 1.0 on S3 uses the `DefaultLogStore`: a commit is **one** `PUT _delta_log/<N>.json` with
`If-None-Match: *` (object_store 0.13 `S3ConditionalPut::ETagMatch`, the default). No DynamoDB
locking provider, no temp-file + rename (none found in the bucket after the run).

| Probe | What | Result (2026-09-26) |
|---|---|---|
| P8a | 16 concurrent put-if-absent of one key, 10 rounds | ✅ exactly 1 winner each round (15 `AlreadyExists`); stored body = winner's |
| P8b | 8 sink MERGEs on one snapshot, **retries off**, 5 rounds | ✅ exactly one new version per round, no stray v+2; v+1 carries the winner's `commitInfo` position; content = base + winner's change set only (~4.4 s per round) |
| P8c | 8 owners MERGE the **same row**, retries on | ✅ exactly one lands; losers stopped by the **data** conflict check (`a concurrent transactions added new data`) — fires before the fence |
| P8e | 8 owners append **disjoint** rows, retries on | ✅ exactly one lands; losers stopped by the **`txn` fence alone** (`Concurrent transaction failed`); `txn` version = winner's epoch |
| P8d | 8 foreign appends (no `txn`), retries on | ✅ all land on contiguous versions 1..=8; 800/800 rows |

Losing writers leave their Parquet files behind (≈7 per contested round) — reclaimed by VACUUM
(external maintenance). P8b cannot tell whether a loser was stopped by S3's `If-None-Match` or by
delta-rs's pre-commit version check (both surface as `MaxCommitAttempts(0)`); P8a proves the S3
primitive directly.

```bash
eval "$(aws configure export-credentials --profile <profile> --format env)"
AWS_REGION=us-east-1 P8_S3_URL=s3://<bucket>/delta-probe \
cargo test --release --features s3 --test probe_s3_commit -- --nocapture --test-threads=1
aws s3 rm s3://<bucket>/delta-probe/ --recursive   # cleanup
```

Run 2026-09-26 on account `418759522081` (profile `kav`); prefix deleted afterwards.

### Not covered yet

- **P8 on GCS / Azure** — credentials pending (same probe shape; needs `deltalake/gcs`, `deltalake/azure`).
- Spark cross-read (no JVM on the probe host; DuckDB covers the independent-reader check).

## Running

```bash
cargo test                                                     # default: no delta deps, instant
cargo test --release --features probe -- --nocapture --test-threads=1   # ~18 min cold build (DataFusion)
```
