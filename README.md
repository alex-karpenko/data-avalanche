# data-avalanche

Change data capture from PostgreSQL and MySQL/MariaDB into Delta Lake tables on object storage
(S3, GCS, Azure Blob), written in Rust. A cluster of nodes shares the pipelines: membership by
gossip, ownership by a lease in the object store, so a node that leaves or dies hands its
pipelines to the others.

> [!WARNING]
> **Not ready for production.** data-avalanche is under active development toward its first
> release (v1.0.0) and has not been run against production databases or workloads. Until it has
> been, treat every version as experimental:
>
> - **Breaking changes without migration.** The config format, the state kept in object storage
>   and the management API can change between versions; an upgrade may mean starting the
>   pipelines afresh.
> - **Tested on containers and emulators, not at scale.** The test suites run against local
>   database containers and storage emulators. Large tables, long outages, real network faults
>   and every cloud's quirks haven't been exercised, and a bug can leave the lake out of step
>   with the source.
> - **It holds resources on your source.** On PostgreSQL it creates a replication slot and a
>   publication; a stopped or stuck pipeline keeps the slot, and the slot keeps WAL on the
>   primary's disk until the pipeline resumes or the slot is dropped.
>
> To try it, point it at a replica or a throwaway copy of the database and at a bucket or prefix
> of its own, and watch the source's disk. Bug reports and feedback are welcome in the issues.

## Status

v1 is built milestone by milestone. Which milestones have landed, and what each one covers, is in
the index of [`doc/v1.0/README.md`](doc/v1.0/README.md); the roadmap is
[`doc/v1.0/00-roadmap.md`](doc/v1.0/00-roadmap.md). The Apache Iceberg sink is designed but
deferred to v2: v1 writes Delta Lake only.

## Layout

- `crates/`: the application, one crate per subsystem (config, storage and state, sources, the
  Delta sink, the pipeline worker, the cluster, the management API, telemetry, the binary).
- `deploy/`: deployment artifacts (example Prometheus alert rules; the container image and the
  Helm chart land with milestone M11a).
- `test-infra/`: the containers the integration and end-to-end tests run against
  (`test-infra/README.md`).
- `doc/`: the specifications (`doc/specs/`), the research and design decisions (`doc/research/`),
  the build plan (`doc/v1.0/`) and the design gap tracker (`doc/analysis/`).
- `prototypes/`: the capability probes the design rests on.

## Building and testing

```sh
cargo build --release -p data-avalanche
cargo test --workspace             # hermetic: no containers needed
test-infra/up.sh                   # then the integration tests, gated by DATA_AVALANCHE_IT=1
```

The test gates are described in [`doc/v1.0/README.md`](doc/v1.0/README.md).
