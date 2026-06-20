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
| T6 | release with correct version removes the object | emulator ok |
| T7 | release with stale version → Conflict | emulator ok |
| T8 | re-acquire immediately after release (planned-handoff fast path) | emulator ok |
| T9 | S3 409 ConditionalRequestConflict surfaces as retryable Conflict | real S3 |
| **T10** | **GCS: the round-tripped token behaves as the *generation*** | **real GCS** |

Emulators (MinIO / fake-gcs-server / Azurite) sometimes implement preconditions loosely, so
**T4, T5, T10 must run against the real backend at least once** before the lease is trusted.

## Running

Select a backend via `LEASE_TEST_SCHEME` (`s3` | `gcs` | `azblob`) and pass OpenDAL service
config through `OPENDAL_<SCHEME>_*` env vars. With no `LEASE_TEST_SCHEME`, the suite skips
(stays green).

```bash
# Example: MinIO
export LEASE_TEST_SCHEME=s3
export OPENDAL_S3_BUCKET=leasetest
export OPENDAL_S3_ENDPOINT=http://localhost:9000
export OPENDAL_S3_REGION=us-east-1
export OPENDAL_S3_ACCESS_KEY_ID=minioadmin
export OPENDAL_S3_SECRET_ACCESS_KEY=minioadmin
export OPENDAL_S3_ROOT=/lease
cargo test -p lease-conformance -- --test-threads=1
```

Run the matrix (emulators in CI for T1–T8; a scheduled job against real S3/GCS/Azure for
T4/T5/T9/T10) as the **merge gate** for the lease implementation.

## D-5 outcome encoded here

- **Route A (OpenDAL) is the default** — one operator shared with the sink; conditional writes
  via `write_with().if_none_match("*")` / `.if_match(version)`.
- **Startup capability assertion** (`OpendalLeaseStore::assert_capabilities`) fails fast on a
  backend that doesn't advertise the conditional-write flags.
- **Native-SDK escape hatch** — if T10 shows OpenDAL isn't mapping GCS `if_match` to the
  generation precondition, implement `LeaseStore` over `google-cloud-storage` for GCS only,
  behind the same trait. Same hatch applies to any backend that fails T4/T5.

## Status

Scaffold — compiles against the pinned deps once fetched; **not yet executed against any real
backend** (that run is the actual D-5 gate and needs cloud credentials / buckets).
