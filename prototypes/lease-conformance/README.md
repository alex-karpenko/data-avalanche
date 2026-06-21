# lease-conformance — D-5 CAS conformance harness

Proves the one safety property the clustering lease depends on
(`doc/research/Clustering_Architecture_Research.md` §5, §17): a compare-and-swap on the
lease object succeeds for **exactly one** writer per version; every staler writer gets a
clean, retryable `Conflict`.

This is a **prototype / verification harness**, not the production implementation. It pins
the `LeaseStore` trait (the contract a real `cluster.lock_backend: storage` will implement)
and the T1–T10 suite that gates it.

## What it checks (T1–T10, from §17.4)

| Test | Property | Needs real service? |
|---|---|---|
| T1 | acquire on empty key → Ok | emulator ok |
| T2 | acquire on held key → Conflict (create-if-absent) | emulator ok |
| T3 | swap with correct version advances the token | emulator ok |
| **T4** | **swap with stale version → Conflict (core safety)** | **yes** |
| **T5** | **N concurrent swaps → exactly 1 wins (single-writer)** | **yes** |
| T6 | release with correct version frees the lease (tombstone); re-acquire works | emulator ok |
| T7 | release with stale version → Conflict | emulator ok |
| T8 | re-acquire immediately after release (planned-handoff fast path) | emulator ok |
| T9 | S3 409 ConditionalRequestConflict surfaces as retryable Conflict | real S3 |
| **T10** | **GCS: the round-tripped token behaves as the *generation*** | **real GCS** |

Emulators (MinIO / fake-gcs-server / Azurite) sometimes implement preconditions loosely, so
**T4, T5, T10 must run against the real backend at least once** before the lease is trusted.

### Release is a tombstone, not a delete

OpenDAL exposes a conditional *write* (`if_match`) but **no conditional delete** (its
`delete_with(...)` builder has no `if_match`). So `release` does a CAS *write of a tombstone*
marker — the design's "PUT a tombstone … If-Match" path (`Clustering…` §8.3) — and `acquire`
recognizes the tombstone and CAS-takes it (planned-handoff fast path, no wait). This preserves
the stale-version → `Conflict` guarantee (T7) without needing a conditional-delete capability,
and keeps the safety proof on the two write preconditions (`write_with_if_not_exists`,
`write_with_if_match`) the startup assertion checks. One object per `pipeline_id` is reused
across acquire→release→re-acquire, so tombstones don't accumulate.

## Running

Select a backend via `LEASE_TEST_SCHEME` (`s3` | `gcs` | `azblob`) and pass OpenDAL service
config through `OPENDAL_<SCHEME>_*` env vars. With no `LEASE_TEST_SCHEME`, the suite skips
(stays green).

Config keys are the OpenDAL service keys, upper-cased and prefixed `OPENDAL_<SCHEME>_`. Run
single-threaded so the concurrency tests don't contend across each other:
`cargo test -p lease-conformance -- --test-threads=1`.

```bash
# --- S3 / MinIO (LEASE_TEST_SCHEME=s3) ---
export LEASE_TEST_SCHEME=s3
export OPENDAL_S3_BUCKET=leasetest
export OPENDAL_S3_REGION=us-east-1
export OPENDAL_S3_ROOT=/lease
# MinIO: point at the local endpoint + static creds
export OPENDAL_S3_ENDPOINT=http://localhost:9000
export OPENDAL_S3_ACCESS_KEY_ID=minioadmin
export OPENDAL_S3_SECRET_ACCESS_KEY=minioadmin
# Real AWS S3 instead: drop ENDPOINT and either set
#   OPENDAL_S3_ACCESS_KEY_ID / OPENDAL_S3_SECRET_ACCESS_KEY (+ optional OPENDAL_S3_SESSION_TOKEN)
# or rely on the ambient chain (instance profile / SSO) — leave the key vars unset.
cargo test -p lease-conformance -- --test-threads=1
```

```bash
# --- GCS (LEASE_TEST_SCHEME=gcs) — Route B, the NATIVE GCS SDK (not OpenDAL) ---
# OpenDAL can't run the lease on GCS (write_with_if_match=false; GCS preconditions
# are generation-based), so GCS uses google-cloud-storage behind the `gcs` feature.
export LEASE_TEST_SCHEME=gcs
export GCS_BUCKET=leasetest
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json   # ADC; SA needs storage.objectAdmin on the bucket
# NOTE the extra flag — the gcs backend only compiles with --features gcs:
cargo test -p lease-conformance --features gcs -- --test-threads=1
```

```bash
# --- Azure Blob (LEASE_TEST_SCHEME=azblob) — Route B, native Blob REST + SAS ---
# OpenDAL 0.57 azblob can't do the CAS update (write_with_if_match=false), so Azure
# uses the native backend behind the `azure` feature. Auth is a SAS token (no signing).
export LEASE_TEST_SCHEME=azblob
export AZURE_STORAGE_ACCOUNT=<account>
export AZURE_CONTAINER=leasetest
# Generate a container SAS with read/add/create/write/delete/list:
#   az storage container generate-sas --name leasetest --account-name <account> \
#     --permissions racwdl --expiry 2026-12-31T00:00Z --account-key <key> -o tsv
export AZURE_STORAGE_SAS_TOKEN="sv=...&ss=b&srt=co&sp=racwdl&sig=..."
# NOTE the extra flag — the azblob backend only compiles with --features azure:
cargo test -p lease-conformance --features azure -- --test-threads=1
```

> **Keep the test bucket/container's lease prefix versioning-off** (or lifecycle-expire noncurrent
> versions). Versioning isn't required, and the high-frequency renew writes would otherwise pile up
> noncurrent versions. S3 with versioning on is exercised and passes (ETag token), but off is the
> recommended operational default.

Run the matrix (emulators in CI for T1–T8; a scheduled job against real S3/GCS/Azure for
T4/T5/T9/T10) as the **merge gate** for the lease implementation. **T10 only does real work on
`gcs`** — the generation-CAS assertion, and the one that most needs a real run.

## D-5 outcome encoded here

On **OpenDAL 0.57**, Route A (OpenDAL) covers **S3 only**; GCS and Azure each report
`write_with_if_match = false` and use a native Route B behind a feature flag. Same `LeaseStore`
trait, same T1–T10 suite, for all three.

- **S3 — Route A (OpenDAL)** — `write_with().if_not_exists(true)` (create) / `.if_match(etag)` (CAS);
  token = ETag.
- **GCS — Route B (`--features gcs`, `google-cloud-storage`)** — `ifGenerationMatch` (create = 0,
  swap/release = the holder's generation); token = generation.
- **Azure — Route B (`--features azure`, raw Blob REST + SAS)** — `If-None-Match: *` (create) /
  `If-Match: <etag>` (CAS); token = ETag. OpenDAL azblob can't do the CAS update even though Azure
  Blob natively supports `If-Match`.
- **Startup capability assertion** (`OpendalLeaseStore::assert_capabilities`) fails fast on an
  OpenDAL backend lacking `write_with_if_not_exists` + `write_with_if_match`, and prints the
  conditional-write matrix.

## Real-service findings (2026-06, AWS S3 + GCS + Azure)

1. **Create-if-absent is `if_not_exists`, not `if_none_match`.** S3 supports `If-None-Match` only
   with the wildcard `*`, so OpenDAL models it as the dedicated **`write_with_if_not_exists`**
   capability and reports **`write_with_if_none_match = false`** on S3. Acquire uses
   `.if_not_exists(true)`; the startup assertion checks `write_with_if_not_exists`.
2. **The CAS token is the ETag on S3/Azure, the generation on GCS — fix it by backend.** See
   [`TokenKind`]. On a **versioning-enabled** S3 bucket `meta.version()` is the object *version-id*,
   which `If-Match` does **not** accept — a naive "prefer version() over etag()" makes every swap
   fail with `Conflict`. The OpenDAL store pins `TokenKind::Etag` for S3/Azure.
3. **OpenDAL (0.57) cannot run the lease on GCS *or* Azure → native SDKs (Route B).** OpenDAL reports
   `write_with_if_match = false` for both. GCS is fundamental (its preconditions are generation-based,
   not ETag/`If-Match`). **Azure is an OpenDAL limitation** — Azure Blob natively supports `If-Match`
   conditional PUT (matrix: `if_not_exists=true if_match=false if_none_match=true`), OpenDAL 0.57 just
   hasn't wired it into the azblob write path. Both are *static capability gaps* — they fail before any
   network/auth call, so credentials and bucket access are irrelevant. GCS uses `google-cloud-storage`;
   Azure uses raw Blob REST + SAS.
4. **Bucket versioning is NOT required and NOT recommended for the lease prefix.** The lease works
   on a non-versioned bucket. With versioning on it still works (ETag token on S3), but every renew
   (~every few seconds per pipeline) leaves a noncurrent version, and tombstone releases pile up too.
   Keep the `…/leases/` prefix **versioning-off**, or lifecycle-expire noncurrent versions.

## Status

The CAS single-writer guarantee is **proven on all three real services** — S3 (T1–T9, versioned +
non-versioned), GCS (T1–T10 incl. the generation-CAS gate T10), Azure (T1–T9). That closes the D-5
design gate.

Dependency note (2026-06): `reqwest` bumped to **0.13**; the GCS module was **migrated from yoshidan's
`google-cloud-storage` 0.24 to the official `google-cloud-storage` 1.x** (`googleapis/google-cloud-rust`:
`Storage::builder()` / `write_object().set_if_generation_match()` / `read_object()`). The 1.x port is
**re-verified against real GCS — T1–T10 pass on 1.15**, so the earlier `// VERIFY:` accessors are
confirmed. One 1.x quirk worth noting: a failed `ifGenerationMatch` surfaces as `code UNKNOWN` with the
text *"…the pre-conditions you specified did not hold."* (no clean 412), so `GcsLeaseStore::is_conflict`
matches that wording.

Per-backend wiring: **S3** = OpenDAL (Route A); **GCS** = official `google-cloud-storage` 1.x
(`--features gcs`); **Azure** = raw Blob REST + SAS over `reqwest` (`--features azure`).
