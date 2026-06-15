---
updated: 2026-06-06
domain: Architecture
tags: [architecture, design, system]
sources: []
---

# System Overview

## Overview
A high-load Change Data Capture system written in Rust that streams row-level changes from PostgreSQL and MySQL source databases into Apache Iceberg tables stored in cloud object storage. Built for correctness, throughput, and operational simplicity.

## High-Level Flow

```
PostgreSQL (logical replication / pgoutput)  ─┐
                                               ├─► CDC Worker (Rust)
MySQL (binlog / GTID)                         ─┘        │
                                                         ▼
                                               Arrow RecordBatch buffer
                                                         │
                                                         ▼
                                               Apache Iceberg writer
                                               (iceberg-rust + OpenDAL)
                                                         │
                                                         ▼
                                               Cloud Object Storage (S3/GCS)
```

## High-level system requirements

- System Configuration:
	- file in `YAML` format
	- path to the file is passed via command line parameter
	- configuration can by changed on the fly by triggering file reload via management REST API call
	- system makes diff between old and new config and reconciles state
	- many of config parameters can by substituted by environment variables, line credentials, host names, etc.
	- has mandatory `pipelines` dictionary with pipelines configuration (see below). may be empty.
	- has optional `defaults` top-level section which provides (overrides) pipelines defaults
	- has mandatory `globals` top-level section which provides parameters not related to pipelines: config state storage, etc.
	- config data model will be provided later
- Data replication entity is `pipeline`, it defines:
	- `source`: database attributes to source data from (type, hostname, credentials, publication and replication slot name, ssl mode, etc.), without tables definition (see below)
	- `sink`:  cloud storage attributes where to store data to (type, bucket name, prefix, etc.)
	- `catalog`:  where to store schema to, catalog type, how to handle schema evolution, etc.
	- `snapshot_mode`:  defines startup snapshotting mode (when app restarts or pipeline restarts due to config change) like - always, never (default), initial, dump_always, dump_initial
	- `tables`:  include/exclude filters lists with glob pattern allowed, table level snapshot mode overrides
	- `flush_mode`:  when to flush source data to sink: immediately or when one or several criteria satisfied: time interval elapsed (default mode, value is 30s), max number of sourced rows collected (default is 100,000 rows), max collected data size (default is 512Mb)
	- `state`: optional cloud storage attributes (bucket name, prefix, etc.) where to store pipeline state to (snapshot jobs state, confirmed source offsets, etc.). default is to store to sink with prefix like `.pipeline-state`. sometimes it's useful to store state to other place.
	- postgres tables should have REPLICATION IDENTITY set to any value except NONE, we explicitly forbid replication of the tables without primary key - halt the table replication. in case when table without replica identity is discovered via glob pattern - don't add it to tables list and emit warning.
- Data snapshotting:
	- on startup depending on `snapshot_mode`:
		- always - do snapshot each time pipeline starts (on app startup or on pipeline restart due to pipeline config change) and continue data streaming from the point of snapshot finished
		- never - just start/continue data streaming without snapshot, this is default
		- initial - if table doesn't exist at the sink storage (there is no confirmed replication point), then do snapshot once and start replication
		- dump_always - do snapshot each time pipeline starts, but don't stream further data
		- dump_initial - if table doesn't exist at the sink storage and in the catalog, then do snapshot once but don't stream further data. if data is present at the storage but no table in the catalog - don't snapshot, emit warning. if catalog entry is present but data storage is empty for the table - do snapshot like data is absent.
	- on request via management REST API:
		- request payload should contain pipeline name and non-empty tables list without globs. 
		- replication of the specified tables should be paused, and resumed after snapshot completed form the point "next to last snapshotted position"
		- may contain table(s) which isn't present in the pipeline config - do snapshot but don't stream data
- Clustering:
	- app (Kubernetes pod(s), standalone container(s), standalone Linux service(s)) may be running in one or more instances
	- single instance (worker) process its unique non-overlapped set of pipelines
	- workers are sharded by pipelines using consistent hashing
	- Gossip/SWIM detects node failures and updates membership and consistent hash ring with virtual nodes
	- number of virtual nodes is a global config parameter
	- config reloading request should be processed by worked got the request
	- `memberlist` state contains current configuration
- Management REST API:
	- listening address:port is a command line parameter
	- authentication/authorization:
		- anonymous
		- basic
		- JWT (with RBAC)
		- OAuth2/OpenID (with RBAC)
	- liveness probe (w/o auth)
	- readiness probe (w/o auth)
	- config validation
	- config reloading
	- get cluster status
	- get pipelines status
	- request snapshot
	- get snapshot job status
	- pipeline pause
	- pipeline stop
	- pipeline resume
	- pipeline restart
	- pipeline flush data to storage
	- leave cluster
	- shutdown
	- get metrics (w/o auth)
- Observability
	- write logs on different levels to stderr
	- log level and format (text, json, pretty) configured via command line parameters
	- detailed metrics in Prometheus format
- State and reconciliation:
	- no data or state on local filesystem, everything we need to be stored should be preserved to cloud storage
	- app automatically creates pipeline's database resources it needs, like publications, replication slots, etc. and automatically destroys created resources when pipeline is gone
	- to satisfy previous requirement app preserves current config/reconciliation state to cloud storage (defined in globals config section)
	- app doesn't delete any other created data artifacts like iceberg tables, tables metadata, catalog entries, etc.

## Key Design Decisions

| Decision         | Choice         | Rationale                                     |
| ---------------- | -------------- | --------------------------------------------- |
| Language         | Rust           | Memory safety + performance without GC pauses |
| In-memory format | Apache Arrow   | Zero-copy, columnar, interoperable            |
| Table format     | Apache Iceberg | ACID, time travel, cloud-native               |
| Cluster state    | memberlist     | Proven, well-understood failure model         |
| Async runtime    | tokio          | Ecosystem standard for Rust async             |

## Open Questions

### Configuration

**OQ-C1 — Config propagation across cluster nodes**
When a config reload request hits one worker via REST API, how does the new configuration reach the other workers? Options: the receiving worker broadcasts via gossip; the client must call every node; a shared config version is stored in cloud storage and polled. The current spec says "config reloading request should be processed by worker that got the request" but is silent on peer propagation.

**OQ-C2 — Config data model**
The spec explicitly defers the config data model. It needs to be defined before implementation can start: field names, types, required vs optional, validation rules, defaults hierarchy (`defaults` section vs pipeline-level overrides).

**OQ-C3 — Secret management**
Environment variable substitution is mentioned for credentials. Is integration with a secret manager (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager) planned or out of scope? Is credential rotation without restart a requirement?

---

### Pipeline Behaviour

**OQ-P1 — Flush mode: multiple criteria semantics**
`flush_mode` can have several criteria (time interval, row count, data size). If more than one is configured, does the first satisfied criterion trigger a flush (OR), or must all be satisfied (AND)? The default behaviour should be explicit.

**OQ-P2 — In-flight data during pipeline restart**
When a pipeline restart is triggered by a config change, what happens to buffered Arrow RecordBatches that haven't been flushed yet? Are they flushed before restart, discarded and replayed from the last confirmed offset, or is partial flushing acceptable?

**OQ-P3 — Pipeline hash key**
Consistent hashing assigns pipelines to workers. What is the hash key — the pipeline name string? An opaque pipeline ID? This matters for stability when pipelines are renamed.

---

### Snapshotting

**OQ-S1 — Snapshot resumability on crash**
If the application crashes or the worker is killed mid-snapshot, does the snapshot job resume from the last checkpointed row or restart from scratch? If resumable, how is the per-table cursor persisted?

**OQ-S2 — Snapshot isolation and consistent read point**
For multi-table snapshots, is a consistent point-in-time guaranteed? PostgreSQL offers `pg_export_snapshot()` / `SET TRANSACTION SNAPSHOT` for this; MySQL requires `FLUSH TABLES WITH READ LOCK` or GTID-based coordination. The mechanism must be specified to avoid cross-table inconsistencies.

**OQ-S3 — "Next to last snapshotted position" semantics**
The spec says an on-demand snapshot should resume streaming from "next to last snapshotted position". Clarify: is this the LSN/GTID captured at the moment the snapshot lock was acquired, or the position after the last snapshotted row was written? The difference matters for duplicate-vs-gap correctness.

**OQ-S4 — Snapshot parallelism within a pipeline**
Can multiple tables within a single pipeline be snapshotted concurrently (multiple DB connections, parallel Arrow writers)? Or are they serialised? This has a large impact on initial load time.

---

### Clustering & Fault Tolerance

**OQ-CL1 — Exclusive pipeline ownership (split-brain prevention)**
What prevents two workers from simultaneously replicating the same pipeline after a network partition or a slow gossip convergence? Without a distributed lock or fencing token, duplicate writes to Iceberg are possible. Is optimistic concurrency via Iceberg's atomic metadata swap sufficient, or is an explicit lease mechanism required?

**OQ-CL2 — Offset handover on node failure**
When a worker fails and its pipelines are reassigned via the hash ring, from which offset does the new owner start replication? It reads the last confirmed offset from cloud storage, but what is the maximum staleness of that checkpoint? This defines the worst-case replay window.

**OQ-CL3 — Rebalancing during normal operation**
When a new worker joins or leaves the ring, some pipelines are reassigned. What happens to in-flight RecordBatches on the previous owner? Is there a handoff protocol (drain then reassign), or is reassignment immediate with the new owner starting from the last checkpoint?

**OQ-CL4 — Minimum viable cluster size**
Can the system run as a single-node deployment with no gossip peers? What is the degraded behaviour if the memberlist has only one member?

---

### State & Durability

**OQ-ST1 — State checkpoint frequency**
How often are confirmed source offsets and snapshot cursors written to cloud storage? A configurable interval? After every flush? The answer directly sets the worst-case data replay window on crash.

**OQ-ST2 — Object storage consistency model**
S3 provides strong read-after-write consistency for new objects (since 2020), but conditional PUT (PUT-if-absent via `If-None-Match: *`) is required for safe atomic state updates. Is conditional PUT used for offset checkpointing, or is a sequence number / generation counter approach taken instead?

**OQ-ST3 — State file layout**
What is the physical layout of state in cloud storage? One file per pipeline? One file per pipeline+table? A single global state object? Granularity affects concurrent access patterns and partial failure recovery.

---

### Schema & Data Types

**OQ-DT1 — Type mapping table**
Database-specific types (PostgreSQL: `jsonb`, `uuid`, `hstore`, `arrays`, `enum`, `geometry`; MySQL: `SET`, `ENUM`, `JSON`, `BIT`) must map to Arrow and then Iceberg/Parquet types. The mapping table needs to be defined: what is the target representation for each, and how are unsupported types handled (error, skip column, serialize as bytes)?

**OQ-DT2 — DDL changes during live replication**
What happens when a source table undergoes an `ALTER TABLE` (add column, drop column, rename column, change type) while replication is active? Options: halt the pipeline with an error; auto-evolve the Iceberg schema; buffer and replay. Each has different complexity and consistency implications.

**OQ-DT3 — Schema evolution supported operations**
The `catalog` section mentions schema evolution handling but doesn't enumerate which operations are supported. At minimum: add nullable column, drop column, widen numeric type. Breaking changes (type narrowing, rename) — are they rejected or handled with a compatibility mode?

---

### Apache Iceberg

**OQ-IC1 — Write mode: COW vs MOR**
Will Iceberg tables use Copy-on-Write or Merge-on-Read? COW simplifies reads but amplifies write cost for CDC workloads with high-frequency small updates. MOR is better for write throughput but requires compaction to control read amplification. Decision needed before Iceberg writer implementation.

**OQ-IC2 — Catalog implementation**
Which catalog will be the primary supported implementation: REST Catalog, AWS Glue, Apache Nessie, or Hive Metastore? Each has different dependency and deployment complexity. Is multi-catalog support required at launch?

**OQ-IC3 — Delivery guarantee**
Is the system designed for at-least-once or exactly-once delivery into Iceberg? At-least-once (replay on crash) is simpler but requires consumers to handle duplicates. Exactly-once requires idempotent writes using Iceberg's row-level deletes or a deduplication key per commit. This must be decided before the writer design is finalised.

---

### Management API & Security

**OQ-API1 — REST API TLS**
Is TLS for the management REST API a requirement? Is mutual TLS (mTLS) required for cluster-internal communication? Without TLS, basic-auth and JWT tokens are transmitted in cleartext.

**OQ-API2 — RBAC role definitions**
For JWT and OAuth2/OpenID auth modes, what roles are defined? Which operations (shutdown, config reload, snapshot trigger) require elevated roles vs read-only? A role matrix should be specified.

**OQ-API3 — Config reload proxy**
When a config reload request is received by worker A but the pipeline belongs to worker B (based on consistent hashing), does worker A proxy the request to worker B, reject it with a redirect, or accept and broadcast? Needs a defined contract.

---

### Observability

**OQ-OB1 — Distributed tracing**
Are OpenTelemetry traces planned? For diagnosing replication lag or snapshot stalls across workers, trace propagation across gossip-triggered operations is valuable. If not in scope, document explicitly.

**OQ-OB2 — Replication lag SLA**
What is the target maximum end-to-end replication lag (source commit → Iceberg file visible) under normal load? This drives flush interval defaults, buffer sizing, and cluster sizing guidelines.

**OQ-OB3 — Key metrics catalogue**
Which Prometheus metrics are mandatory? At minimum: replication lag per pipeline, rows/sec, flush latency, snapshot progress (rows remaining), cluster membership size, last confirmed offset age. A catalogue should be defined to avoid ad-hoc metric proliferation.

---

### Miscellaneous

**OQ-M1 — Typo / ambiguity in clustering spec**
"config reloading request should be processed by **worked** got the request" — "worked" should be "worker that". Fix before this doc is used as a reference.

**OQ-M2 — App does not delete Iceberg artifacts — clarification needed**
The spec says the app never deletes Iceberg tables or catalog entries. Does this include snapshot-only tables created for pipelines with `snapshot_mode: only`? And does it include orphaned Iceberg snapshots that result from aborted flush operations? Iceberg maintenance (snapshot expiration, orphan file cleanup) responsibilities need to be assigned.

## Sources
