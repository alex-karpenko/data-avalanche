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

:::caution[Not for production yet]
data-avalanche is under development. v1 is being built and hasn't been tested at scale or in
production. Expect missing features, breaking changes to the config and the state layout, and bugs.
Try it on data you can rebuild, and report what you find in the
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
