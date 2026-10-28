# test-infra

Containers for integration tests. Start with `test-infra/up.sh` (add `matrix` for older engine
versions), stop with `test-infra/down.sh`, and run tests with
`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test`. GCS tests need the variables from the
GCS section below. The Delta sink's DuckDB cross-read also needs [`uv`](https://docs.astral.sh/uv/)
on `PATH`; it fetches DuckDB itself.

| Service | Image | Host port | Credentials | Profile |
|---|---|---|---|---|
| postgres-18 | postgres:18 | 55418 | avalanche / avalanche, db `avalanche` | default |
| postgres-14…17 | postgres:14…17 | 55414…55417 | same | matrix |
| postgres-tls | postgres:18, TLS required (`postgres-tls.sh`) | 55419 | avalanche / avalanche; `avalanche_cert` by client certificate | default |
| mysql-84 | mysql:8.4 (GTID on) | 53384 | root / avalanche, db `avalanche` | default |
| mysql-84-nogtid | mysql:8.4 (GTID off: `replication_type: binlog`; TLS off) | 53385 | same | default |
| mysql-tls | mysql:8.4 (GTID on), TLS required (`mysql-tls.sh`) | 53386 | root / avalanche; `avalanche_cert` / `unused` with its client certificate | default |
| mariadb-118 | mariadb:11.8 (`gtid_strict_mode=1`) | 53318 | root / avalanche | default |
| mariadb-114, mariadb-1011 | mariadb:11.4, 10.11 | 53314, 53311 | root / avalanche | matrix |
| rustfs (S3) | rustfs/rustfs:1.0.0 | 59000 | avalanche / avalanche-secret | default |
| azurite (Azure Blob) | mcr.microsoft.com/azure-storage/azurite:3.37.0 | 10000 | Azurite's public dev account | default |

Bucket / container `data-avalanche-test` is created by the `init` profile (`up.sh` runs it). Azurite keeps
port 10000 because `object_store`'s emulator mode always targets `127.0.0.1:10000`.

`mysql-tls` writes its throwaway CA and client pair to `test-infra/.tls/mysql/` at each start; its
server certificate names `localhost` only, so a `verify-full` connection to `127.0.0.1` fails by
design.

## PostgreSQL

The PostgreSQL source's tests (`crates/data-avalanche-source-pg/tests/pg`) run against
`postgres-18`; `DATA_AVALANCHE_PG_PORT` points them at another server, and
`.github/workflows/weekly.yml` runs them on PostgreSQL 14–17 every Monday (and on manual dispatch):

```sh
docker compose -f test-infra/compose.yaml --profile matrix up -d --wait postgres-14
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_PG_PORT=55414 cargo test -p data-avalanche-source-pg --test pg --test slot_invalidation -- --skip tls::
```

`postgres-tls` refuses every TCP connection without TLS. At each start it makes a throwaway CA, a
server certificate for `localhost` only (a connection by `127.0.0.1` is a name mismatch, which
the `verify-ca` test needs), and a client certificate for the role
`avalanche_cert`, which authenticates by certificate (`pg_hba` `cert`). It writes the CA and the
client pair to `test-infra/.tls/` (git-ignored) for the TLS tests; the server key stays in the
container. Nothing is committed.

## MySQL and MariaDB

The MySQL source's tests (`crates/data-avalanche-source-mysql/tests`) read the target
`DATA_AVALANCHE_MYSQL`: a server of the table above and the replication mode the tests use on it.
`ci.yml` runs them on every change against `mysql-84` (the default), `mysql-84-nogtid`
(file/position), `mariadb-118` (GTID) and `mariadb-118-binlog` (`mariadb-118` in file/position
mode); `.github/workflows/weekly.yml` runs `mariadb-114` and `mariadb-1011` (GTID) every Monday:

```sh
docker compose -f test-infra/compose.yaml --profile matrix up -d --wait mariadb-114
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=mariadb-114 cargo test -p data-avalanche-source-mysql --tests
```

The TLS tests run with the `mysql-84` target only, against `mysql-tls`. Four test binaries
change server-wide state or need the server to themselves, and cargo runs each alone, never
beside the `mysql` tests: `binlog_purge` (`PURGE BINARY LOGS` removes the older binlogs of the
target), `mariadb_compress` (turns `log_bin_compress` on, and back to the server default before
it asserts), `row_metadata` (sets `binlog_row_metadata = MINIMAL`, and back to FULL however the
test ends) and `binlog_rotate` (`FLUSH BINARY LOGS`, then a position no other write moved).
MariaDB keeps a binlog it just closed until its binlog checkpoint moves on, so the purge test
retries for up to 5 s.

## The slice-1 E2E suite

`crates/data-avalanche/tests/e2e` runs the binary as a process against one source and RustFS.
The source is PostgreSQL (`postgres-18`, or `DATA_AVALANCHE_PG_PORT`), or with
`DATA_AVALANCHE_E2E_SOURCE=mysql` the target `DATA_AVALANCHE_MYSQL` names (the table above, default
`mysql-84`). A scenario of one source only skips itself on the other. `ci.yml` runs the suite on
PostgreSQL 18 in the integration job and on `mysql-84`, `mysql-84-nogtid` and `mariadb-118` in
its `e2e-mysql` job; `weekly.yml` runs it on PostgreSQL 14–17 and on `mariadb-118-binlog`,
`mariadb-114` and `mariadb-1011`:

```sh
docker compose -f test-infra/compose.yaml up -d --wait mysql-84 rustfs
docker compose -f test-infra/compose.yaml --profile init run --rm rustfs-init
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=mysql-84 \
  cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
```

On MySQL each test makes a database and a user named `e2e_<test>` (the user's binlog dump shows
the stream is open). Two scenarios change server-wide state: the purge scenarios purge the
target's older binlogs, and the failover scenario (on `mysql-84-nogtid`) makes its database and
user on `mysql-84` too, which must be up. The scenarios that pause a server (`docker pause`) need
the Docker CLI. The faults no server can be asked for (a corrupted compressed transaction, a
failover behind one address) come from a test-only TCP proxy in the suite (`proxy.rs`), which
reads the protocol in the clear: the suite's pipelines use `ssl_mode: disable`.

Each binary the suite starts is a node of its own: its name, API port and gossip port come from
`E2E_*` variables, as a cluster's one config takes them from each pod's environment (a node runs
the cluster's config generation, not its own file). The cluster scenarios (`cluster.rs`) run
three nodes as three processes on loopback, each behind a test-only UDP forwarder
(`forwarder.rs`) that drops a chosen side's gossip (a partition); they also `kill -9`, SIGTERM
and SIGSTOP the nodes. They need no service beyond the source and RustFS.

The reload scenarios (`cluster.rs`'s `a_reload_reaches_every_node`, and `reload.rs`) rewrite that
one config file and call `POST /v1/config/reload` on one node; the others adopt the new config
generation from `globals.state`, and `GET /v1/config/status` reports each node's. Those
scenarios run the API with `auth: none`. The management API scenarios (M9a) need nothing more:
`api.rs` serves the API over TLS with a certificate generated at run time and checks bearer
tokens against a JWKS the test serves on loopback (`idp.rs`); `cluster.rs` drives a pipeline
through a node that doesn't hold it, and leaves and shuts a node down through the API. The
snapshot-job scenarios (M9b, `jobs.rs`) need nothing more either: they ask for jobs through the
API, drop and create a table again to halt it, and `kill -9` the process while a job reads.

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

`ci.yml` runs on pull requests only: opening a PR and every push to its head branch run it; a push
to a branch with no open PR, and a merge to `main`, run nothing. Pull requests from forks receive no
secrets. The workflow detects the empty secret, sets `DATA_AVALANCHE_SKIP_GCS=1` and emits a notice,
so fork PRs test S3/Azure only; a maintainer re-runs GCS from a same-repository branch before
merging. On a same-repository pull request, a missing `GCP_SA_KEY` secret or
`DATA_AVALANCHE_GCS_BUCKET` variable is a misconfiguration, not a fork limitation: the workflow
prints an `::error::` and fails the integration job. S3 and Azure need no CI secrets because they
run on the emulators above.

## Real S3 and Azure (`cloud.yml`)

Real S3 and Azure Blob run only in `.github/workflows/cloud.yml` (weekly and on manual dispatch) or by
hand, never in `ci.yml`. A test runs when `DATA_AVALANCHE_CLOUD` names its cloud (`s3`, `azure`, or
`s3,azure`); it opens the bucket through `open_bucket`, so credentials come from the standard
chains. Tests write only under `data-avalanche-it/<test>/<uuid>/` and delete what they wrote.

| Name | Kind | Value |
|---|---|---|
| `DATA_AVALANCHE_S3_BUCKET` | repository **variable** | S3 bucket for the tests (versioning off; lifecycle rule on `data-avalanche-it/`, 1 day). |
| `DATA_AVALANCHE_S3_REGION` | repository **variable** | Its region, exported as `AWS_REGION`. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | repository **secrets** | An IAM user limited to `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on the bucket and `s3:ListBucket` on it. |
| `DATA_AVALANCHE_AZURE_CONTAINER` | repository **variable** | Blob container for the tests (lifecycle rule on `data-avalanche-it/`, 1 day). |
| `DATA_AVALANCHE_AZURE_ACCOUNT` | repository **variable** | The storage account name, exported as `AZURE_STORAGE_ACCOUNT_NAME`. |
| `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` | repository **secrets** | A service principal with `Storage Blob Data Contributor` on the container. |

By hand: `DATA_AVALANCHE_CLOUD=s3 DATA_AVALANCHE_S3_BUCKET=<bucket> AWS_REGION=<region> cargo test -p
data-avalanche-state --test lease_conformance real_s3 -- --nocapture` with AWS credentials in the
environment or profile (`azure` / `real_azure` likewise, with `AZURE_*`).

## Lease conformance T1–T10 (M2)

`crates/data-avalanche-state/tests/lease_conformance.rs` runs T1–T10 (Clustering §17.4) through
`object_store` and `Leases` on each backend. It replaces `prototypes/lease-conformance` as the
evidence the lease rests on (roadmap R-2).

| Backend | Result | Measured | Command |
|---|---|---|---|
| In memory | PASS | 2026-09-29 | `cargo test -p data-avalanche-state --test lease_conformance in_memory` |
| RustFS 1.0.0 | PASS | 2026-09-29 | `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-state --test lease_conformance rustfs` |
| Azurite 3.37.0 | PASS | 2026-09-29 | same, `azurite` |
| Real GCS | PASS (bucket `leasetest`, 24 s; owner's run at 3b215ef, before the final-review fixes) | 2026-09-29 | `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_GCS_BUCKET=<bucket> GOOGLE_SERVICE_ACCOUNT_PATH=<key.json> cargo test -p data-avalanche-state --test lease_conformance real_gcs` |
| Real S3 (us-east-1) | PASS (`cloud.yml` run 36604762684, 2.7 s) | 2026-09-29 | `cloud.yml`, job `s3` |
| Real Azure Blob | PASS (`cloud.yml` run 36604762684, 4.4 s) | 2026-09-29 | `cloud.yml`, job `azure` |

In memory, RustFS and Azurite were re-run on the final M2 tree (`feat/claude/impl-v1` after the final-review fix wave). Real S3 and Azure ran on `feat/claude/impl-v1` at 3a287dd, the final M2 tree.

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
