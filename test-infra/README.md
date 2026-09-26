# test-infra

Containers for integration tests. Start with `test-infra/up.sh` (add `matrix` for older engine
versions), stop with `test-infra/down.sh`, and run tests with
`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test`. GCS tests need the variables from the
GCS section below.

| Service | Image | Host port | Credentials | Profile |
|---|---|---|---|---|
| postgres-18 | postgres:18 | 55418 | avalanche / avalanche, db `avalanche` | default |
| postgres-14…17 | postgres:14…17 | 55414…55417 | same | matrix |
| mysql-84 | mysql:8.4 (GTID on) | 53384 | root / avalanche, db `avalanche` | default |
| mariadb-118 | mariadb:11.8 (`gtid_strict_mode=1`) | 53318 | root / avalanche | default |
| mariadb-114, mariadb-1011 | mariadb:11.4, 10.11 | 53314, 53311 | root / avalanche | matrix |
| rustfs (S3) | rustfs/rustfs:1.0.0 | 59000 | avalanche / avalanche-secret | default |
| azurite (Azure Blob) | mcr.microsoft.com/azure-storage/azurite:3.37.0 | 10000 | Azurite's public dev account | default |

Bucket / container `data-avalanche-test` is created by the `init` profile (`up.sh` runs it). Azurite keeps
port 10000 because `object_store`'s emulator mode always targets `127.0.0.1:10000`.

Added by later milestones: `postgres-tls` (M5, CA-signed server certificate) and `mysql-84-nogtid`
(M7, file/position mode).

## GCS: a real bucket, not an emulator

`fake-gcs-server` 1.56.1 rejects `object_store`'s path-style PUT (`400 invalid uploadType`) and
doesn't honour preconditions (fake-gcs-server issue #994), so GCS tests use a real bucket.
Re-checked on 2026-09-26 after the bump to `object_store` 0.14.2: fake-gcs 1.56.1 (the current
release; the Docker `latest` tag is the same image) still answers every PUT with
`400 invalid uploadType`, and #994 is still open.

| Variable | Meaning |
|---|---|
| `DATA_AVALANCHE_GCS_BUCKET` | Bucket for integration tests. Tests write only under `data-avalanche-it/<test>/<uuid>/` and delete what they wrote. |
| `GOOGLE_SERVICE_ACCOUNT_PATH` | Path to a service-account key JSON (read by `object_store`). `GOOGLE_SERVICE_ACCOUNT_KEY` (inline JSON) or `GOOGLE_APPLICATION_CREDENTIALS` also work. |
| `DATA_AVALANCHE_SKIP_GCS` | Any non-empty value skips GCS tests. With `DATA_AVALANCHE_IT` set and this unset, a missing bucket or credentials **fails** the test instead of skipping it. |

Bucket requirements:
- The service account needs `roles/storage.objectAdmin` on the bucket, and nothing project-wide.
- **Object versioning off** (conditional writes use generations; see the lease-conformance README).
- A lifecycle rule deleting objects under `data-avalanche-it/` after 1 day, to clean up after crashed runs:
  `gcloud storage buckets update gs://<bucket> --lifecycle-file=lifecycle.json` with
  `{"rule":[{"action":{"type":"Delete"},"condition":{"age":1,"matchesPrefix":["data-avalanche-it/"]}}]}`.

## CI requirements (GitHub Actions)

| Name | Kind | Value |
|---|---|---|
| `GCP_SA_KEY` | repository **secret** | The service-account key JSON, verbatim. The workflow writes it to `$RUNNER_TEMP` and exports `GOOGLE_SERVICE_ACCOUNT_PATH`; it is never echoed. |
| `DATA_AVALANCHE_GCS_BUCKET` | repository **variable** | The CI bucket name (not secret). Prefer a bucket separate from local development. |

Pull requests from forks receive no secrets. The workflow detects the empty secret, sets
`DATA_AVALANCHE_SKIP_GCS=1` and emits a notice, so fork PRs test S3/Azure only; a maintainer re-runs
GCS on merge. On pushes and same-repository pull requests, a missing `GCP_SA_KEY` secret or
`DATA_AVALANCHE_GCS_BUCKET` variable is a misconfiguration, not a fork limitation: the workflow
prints an `::error::` and fails the integration job. S3 and Azure need no CI secrets because they
run on the emulators above.

## Storage conditional-write results (M0 Task 12)

| Backend | Create-if-absent | Update-if-version | Measured | Consequence for M2 |
|---|---|---|---|---|
| RustFS 1.0.0 (S3 API) | PASS | PASS | 2026-09-26 | Carries the S3 lease/CAS suite in PR CI |
| Azurite 3.37.0 | PASS | PASS | 2026-09-26 | Carries the Azure lease/CAS suite in PR CI |
| fake-gcs-server 1.56.1 | FAIL (plain PUT → 400 `invalid uploadType`) | not reachable | 2026-09-26 (`object_store` 0.13.2 and 0.14.2) | Dropped |
| Real GCS (`DATA_AVALANCHE_GCS_BUCKET`) | PASS | PASS | 2026-09-26 (bucket `leasetest`) | Carries the GCS lease/CAS suite in PR CI |

RustFS and Azurite rows above were re-measured against this implementation's test code on 2026-09-26
(`test-infra/up.sh`, then `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p
data-avalanche-testkit -- --test-threads=1 --nocapture`): both `rustfs_honours_conditional_writes`
and `azurite_honours_conditional_writes` PASS. The real-GCS row is carried over from the plan
(no GCS credentials available in this environment; `real_gcs_honours_conditional_writes` correctly
prints `skipped: GCS tests need DATA_AVALANCHE_IT=1 and no DATA_AVALANCHE_SKIP_GCS` and reports PASS
as a no-op when `DATA_AVALANCHE_SKIP_GCS=1`) — re-run Step 8 with real credentials to reconfirm before
M2.

Re-run on `object_store` 0.14.2 on 2026-09-26: RustFS and Azurite PASS (same command), and real GCS
PASS (`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_GCS_BUCKET=leasetest GOOGLE_SERVICE_ACCOUNT_PATH=<key.json>
cargo test -p data-avalanche-testkit --test storage_cas real_gcs`), which also reconfirms the real-GCS
row above against this implementation's test code.

## Dependency audit

CI runs `cargo audit` (RustSec) as the last step of the `check` job. It is **non-blocking**: an
advisory fails that step and adds a warning annotation, but the job stays green. To run it locally:

```sh
cargo install cargo-audit --locked   # once; cargo-audit isn't a rustup component, so rust-toolchain.toml can't pin it
cargo audit
```
