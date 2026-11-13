---
title: data-avalanche
description: Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake tables on S3, Google Cloud Storage and Azure Blob Storage.
sidebar:
  label: Overview
---

data-avalanche replicates row-level changes from **PostgreSQL**, **MySQL** and **MariaDB** into
**Delta Lake** tables on object storage (Amazon S3, Google Cloud Storage, Azure Blob Storage, or an
S3-compatible store). Each source table becomes a Delta table that mirrors it: inserts and updates
are upserts by primary key, deletes remove the row, and every flush commits all of a table's changes
in one Delta transaction.

:::caution[Not ready for production]
data-avalanche is under active development and has not been run against production databases or
workloads. Until it has been, treat every version as experimental:

- **Breaking changes without migration.** The config format, the state kept in object storage and
  the management API can change between versions; an upgrade may mean starting the pipelines
  afresh.
- **Tested on containers and emulators, not at scale.** The test suites run against local database
  containers and storage emulators. Large tables, long outages, real network faults and every
  cloud's quirks haven't been exercised, and a bug can leave the lake out of step with the source.
- **It holds resources on your source.** On PostgreSQL it creates a replication slot and a
  publication; a stopped or stuck pipeline keeps the slot, and the slot keeps WAL on the primary's
  disk until the pipeline resumes or the slot is dropped.

To try it, point it at a replica or a throwaway copy of the database and at a bucket or prefix of
its own, and watch the source's disk. Bug reports and feedback are welcome in the
[issue tracker](https://github.com/alex-karpenko/data-avalanche/issues).
:::

## What it does

- **Reads the source's own change stream.** PostgreSQL logical replication (`pgoutput`, a
  replication slot and a publication the pipeline manages); the MySQL and MariaDB binlog in row
  format, positioned by GTID or by file and position.
- **Copies existing rows first, without stopping the stream.** A snapshot reads each table in
  keyset chunks beside the live stream and resumes from its last chunk after a crash.
- **Follows schema changes.** An added column appears in the Delta table; a dropped or renamed
  column is kept and filled with nulls from then on; a change it can't apply safely stops that
  table only, with a reason.
- **Runs as a cluster, or as one process.** Nodes find each other by gossip, share the pipelines
  out by consistent hashing, and hold each pipeline by a lease in object storage, so one pipeline
  never has two writers. One process is a cluster of one.
- **Keeps no local state.** Positions, snapshot progress, leases and the config the cluster runs
  are all in object storage.
- **Is managed over a REST API**: pause, stop, resume, restart and flush pipelines, start
  snapshots, reload the config, with Prometheus metrics and optional OpenTelemetry traces.

## What v1 doesn't do

- **Apache Iceberg** is designed but ships in v2: a pipeline with `sink.type: iceberg` is refused at
  start while the others run.
- **Table maintenance** (OPTIMIZE, VACUUM, file compaction) is left to your engine (Spark,
  Databricks, Trino, delta-rs tooling). data-avalanche writes Delta checkpoints and cleans up the
  expired transaction log, nothing more.
- **Transactions across tables** aren't atomic in the lake: each table commits on its own.
- **Tables without a primary key** replicate only with a `tables.primary_key` override.

## Where to go next

- [Getting started](/data-avalanche/getting-started/): one pipeline from PostgreSQL to Delta.
- [Configuration](/data-avalanche/configuration/overview/): the config file, its sources and its
  storage.
- [Architecture](/data-avalanche/architecture/overview/): how a change travels, and what holds
  when a node fails.
- [Deployment](/data-avalanche/deployment/kubernetes/): the container image and the Helm chart.
- [Runbooks](/data-avalanche/runbooks/source-privileges/): what to do when something stops.
- [Reference](/data-avalanche/reference/configuration/): every config key, every validation rule,
  every API route.

The design behind these pages is in the repository's
[specifications](https://github.com/alex-karpenko/data-avalanche/tree/main/doc/specs) and
[research notes](https://github.com/alex-karpenko/data-avalanche/tree/main/doc/research).
