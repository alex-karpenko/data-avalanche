---
updated: 2026-10-28
domain: Architecture
tags: [architecture, design, system, multi-sink, iceberg, delta-lake, v1-scope]
sources: []
---

# System Overview

## Overview
A high-load Change Data Capture system written in Rust that streams row-level changes from PostgreSQL and MySQL (incl. MariaDB) source databases into lakehouse tables — **Apache Iceberg or Delta Lake, selected per pipeline** — stored in cloud object storage. Built for correctness, throughput, and operational simplicity. **v1 ships the Delta Lake sink; the Iceberg sink ships in v2** (see [Scope (v1)](#scope-v1)).

## High-Level Flow

```
PostgreSQL (logical replication / pgoutput)  ─┐
                                               ├─► CDC Worker (Rust)
MySQL / MariaDB (binlog / GTID)               ─┘        │
                                                         ▼
                                               Arrow RecordBatch buffer
                                               + per-key collapse (TableChangeSet)
                                                         │
                                                         ▼
                                               Sink (per pipeline, `sink.type`)
                                               ├─ Delta Lake  (delta-rs + object_store)   — v1
                                               └─ Iceberg     (iceberg-rust + OpenDAL)    — v2
                                                         │
                                                         ▼
                                               Cloud Object Storage (S3/GCS/Azure)
```

## Scope (v1)

- **Sources:** PostgreSQL (logical replication, `pgoutput`), MySQL 8.x, and MariaDB (binlog, row-based, GTID-positioned — both GTID dialects).
- **Sink:** Delta Lake only (mirror mode: upserts + deletes via one MERGE per table per flush). `sink.type: iceberg` stays a valid config value, but such a pipeline is **refused at start** ("the Iceberg sink ships in v2") while other pipelines keep running — so v1 → v2 needs no config migration. The Iceberg design and probes carry over as the v2 backlog.
- **Replication-lag targets** (at default config; measured by `data_avalanche_replication_lag_seconds`): steady-state **p99 ≤ 60s**; after an owner failure, back under that target within **2 min**. Initial/re-snapshot backfill, a single transaction larger than the flush bounds, and heavy random-key MERGE churn on large tables are excluded.
- **Out of v1 scope** (deferred, with a reopen path): external secret-manager integration (a sidecar writing the credential file is the supported pattern), the `kubernetes` lock backend, per-source-endpoint takeover limits, spill-to-disk for oversized transactions, and atomic visibility **across** tables (each table commits atomically on its own).

Decisions: `research/V1_Scope_Decisions.md`.

## High-level system requirements

- System Configuration:
	- file in `YAML` format
	- path to the file is passed via command line parameter
	- configuration can be changed on the fly by triggering file reload via management REST API call
	- system makes diff between old and new config and reconciles state
	- many of config parameters can be substituted by environment variables, like credentials, host names, etc.
	- has mandatory `pipelines` dictionary with pipelines configuration (see below). may be empty.
	- has optional `defaults` top-level section which provides (overrides) pipelines defaults
	- has mandatory `globals` top-level section which provides parameters not related to pipelines: config state storage, etc.
	- config data model: `Configuration_Specification.md`
- Data replication entity is `pipeline`, it defines:
	- `source`: database attributes to source data from (type, hostname, credentials, publication and replication slot name, ssl mode, etc.), without tables definition (see below)
	- `sink`:  target table format and cloud storage attributes where to store data to: `type` (`iceberg` | `delta` — **mandatory, no default**, so a pipeline's target format never changes implicitly when the binary's compiled-in sink set changes), write mode, bucket name, prefix, flush criteria, and one format-specific block:
		- `iceberg`: write strategy (MOR/COW) and `catalog` — where table schemas/metadata are registered (Glue, Hive Metastore, REST)
		- `delta`: table-creation properties; Delta tables are path-based (no catalog registration in v1)
	- `schema_evolution`:  how to handle source schema changes (sink-neutral policy; the sink applies it within its declared capabilities)
	- `snapshot_mode`:  defines startup snapshotting mode (when app restarts or pipeline restarts due to config change) like - always, never (default), initial, dump_always, dump_initial
	- `tables`:  include/exclude filters lists with glob pattern allowed, table level snapshot mode overrides
	- `flush_mode`:  when to flush source data to sink: immediately or when one or several criteria satisfied: time interval elapsed (default mode, value is 30s), max number of sourced rows collected (default is 100,000 rows), max collected data size (default is 512Mb)
	- `state`: optional cloud storage attributes (bucket name, prefix, etc.) where to store pipeline state to (snapshot jobs state, confirmed source offsets, etc.). default is to store to sink with prefix like `.pipeline-state`. sometimes it's useful to store state to other place.
	- postgres tables should have REPLICATION IDENTITY set to any value except NONE, we explicitly forbid replication of the tables without primary key - halt the table replication. in case when table without replica identity is discovered via glob pattern - don't add it to tables list and emit warning.
	- a source column named like one of the lineage columns the pipeline adds (`_op`, `_ts`, `_lsn`, `_seq`, `_source_db`, `_pipeline`; case-insensitive) halts that table with a clear error — source columns are never renamed.
	- a tracked table dropped at the source halts that table; the sink table is kept. On PostgreSQL (no DDL in the replication stream) the drop is detected by the periodic idle rescan (`rescan_interval`) or at the next pipeline start.
- Data snapshotting:
	- on startup depending on `snapshot_mode`:
		- always - do snapshot each time pipeline starts (on app startup or on pipeline restart due to pipeline config change) and continue data streaming from the point of snapshot finished
		- never - just start/continue data streaming without snapshot, this is default
		- initial - if table doesn't exist at the sink storage (there is no confirmed replication point), then do snapshot once and start replication
		- dump_always - do snapshot each time pipeline starts, but don't stream further data
		- dump_initial - if table doesn't exist at the sink storage and in the catalog, then do snapshot once but don't stream further data. if data is present at the storage but no table in the catalog - don't snapshot, emit warning. if catalog entry is present but data storage is empty for the table - do snapshot like data is absent. for sinks without a catalog (Delta Lake in v1) the catalog condition is the table's own metadata (`_delta_log`) existence.
	- on request via management REST API:
		- request payload should contain pipeline name and non-empty tables list without globs. 
		- the snapshot runs as incremental chunks alongside the live stream — replication of the specified tables is **not** paused; position watermarks reconcile snapshot rows with concurrent changes (`research/Snapshot_Resumability_and_Consistency.md` §4)
		- may contain table(s) which isn't present in the pipeline config - do snapshot but don't stream data
- Sinks:
	- each pipeline writes to exactly one sink (v1); the sink type is selected explicitly per pipeline
	- capture, decode, accumulation and per-key collapse are sink-independent; only the commit to the target table format is sink-specific
	- every sink declares its capabilities (write modes, schema-evolution operations, partition transforms, logical types); a pipeline whose configuration needs a capability its sink lacks is **refused at pipeline start** with the named capability (e.g. Iceberg mirror mode until iceberg-rust gains a delete-capable commit)
	- a table location holding another format's metadata, or pipeline state recorded under a different sink type, is a hard error (no silent format mixing); changing `sink.type` re-seeds the pipeline's tables
	- **v1:** only `delta` is implemented; `sink.type: iceberg` is refused at pipeline start, naming v2 (see [Scope (v1)](#scope-v1))
	- table maintenance is **external** in v1: compaction / OPTIMIZE, snapshot or version expiry, and data-file removal (VACUUM, orphan cleanup) are run by the operator's engine (Spark, Trino, Databricks, delta-rs tooling). The app does only the format-internal housekeeping the format itself requires (Delta checkpoints and transaction-log cleanup) and exposes the when-needed signals as metrics (see [Observability](#observability-catalogue)). A built-in scheduler is reserved (`sink.delta.maintenance` / `sink.iceberg.maintenance`, disabled in v1).
- Clustering:
	- app (Kubernetes pod(s), standalone container(s), standalone Linux service(s)) may be running in one or more instances
	- single instance (worker) process its unique non-overlapped set of pipelines
	- workers are sharded by pipelines using consistent hashing on the immutable pipeline `id`
	- Gossip/SWIM detects node failures and updates membership and consistent hash ring with virtual nodes
	- number of virtual nodes is a global config parameter
	- the ring only schedules; **exclusive ownership** of a pipeline is enforced by a per-pipeline lease object in cloud storage (compare-and-swap acquire / renew / takeover, epoch-fenced commits)
	- a single instance is a one-member cluster — same code path, lease active
	- a config reload request is processed by the worker that got it: it validates the file and publishes a new config generation to cloud storage; peers are notified via gossip and adopt it (no proxying)
	- the current configuration lives in cloud storage (a generation pointer under `globals.state`); gossip only carries the notification
	- **a restart does not apply config-file changes**: a starting node adopts the latest config generation from cloud storage; the local file matters only at first bootstrap or on an explicit reload
	- node identity is `globals.cluster.memberlist.node_name`: on Kubernetes set it to the pod name via the Downward API and run the workers as a **StatefulSet** (not a Deployment — changing pod names would reshuffle the ring on every restart); standalone uses a persisted-once UUID
- Management REST API:
	- listening address:port is a command line parameter
	- authentication/authorization:
		- anonymous
		- basic
		- JWT (with RBAC)
		- OAuth2/OpenID (with RBAC)
	- roles are nested `viewer` ⊆ `operator` ⊆ `admin`; endpoints and minimum roles are in the [endpoint table](#management-api-endpoints)
	- TLS is optional (upstream termination is common); authentication without TLS is a config warning
- Observability
	- write logs on different levels to stderr
	- log level and format (text, json, pretty) configured via command line parameters
	- detailed metrics in Prometheus format (catalogue below)
	- optional OpenTelemetry tracing (OTLP), off unless `globals.tracing.otlp_endpoint` is set
- State and reconciliation:
	- no data or state on local filesystem, everything we need to be stored should be preserved to cloud storage
	- app automatically creates pipeline's database resources it needs, like publications, replication slots, etc. and automatically destroys created resources when pipeline is gone
	- to satisfy previous requirement app preserves current config/reconciliation state to cloud storage (defined in globals config section)
	- app doesn't delete any other created data artifacts like sink tables (Iceberg or Delta), tables metadata, catalog entries, etc. format-internal metadata housekeeping that the table format itself requires (Delta checkpoints and expired transaction-log cleanup) is not artifact deletion; data-file removal (Iceberg orphan cleanup, Delta VACUUM) stays external.

## Management API endpoints

Functional endpoints are versioned under `/v1`; probes and metrics are unversioned and unauthenticated. Authorization is `principal role ≥ min role`. Source: `research/API_RBAC_and_Versioning.md` §2; lifecycle semantics: `research/Pipeline_Lifecycle_State_Machine.md`.

| Endpoint | Method · Path | Min role | Semantics |
|---|---|---|---|
| Liveness / readiness | `GET /health/live`, `GET /health/ready` | none | |
| Metrics | `GET /metrics` | none | Prometheus exposition |
| Config validate | `POST /v1/config/validate` | operator | dry-run of the full load pipeline (parse → validate → resolve) of the body, or of the node's file when the body is empty: the report and what a reload would change; the resolved config is not returned; nothing is published |
| Config reload | `POST /v1/config/reload` | operator | publishes a new config generation cluster-wide |
| Config status | `GET /v1/config/status` | viewer | |
| Cluster status | `GET /v1/cluster/status` | viewer | |
| Pipelines status | `GET /v1/pipelines`, `GET /v1/pipelines/{id}` | viewer | `desired_state`, `actual_state`, positions, per-table status (halted tables with their reason) |
| Request snapshot | `POST /v1/pipelines/{id}/snapshots` | operator | tables list (the per-table config keys), no globs; refused (`409`) unless the desired state is `RUNNING` and a node holds the lease, as a restart (`ERROR` included), or while an unfinished job holds one of the tables; `202` with the job's id. The job runs in a restarted worker. It upserts: a row deleted at the source before the job stays (drop or move the sink table first to start clean) |
| Snapshot job status | `GET /v1/snapshots/{job_id}` | viewer | |
| List snapshot jobs | `GET /v1/pipelines/{id}/snapshots` | viewer | persisted in the pipeline state location; survive restart and failover |
| Pipeline pause | `POST /v1/pipelines/{id}/pause` | operator | `RUNNING → PAUSED`: flush to a transaction boundary and commit; source connection kept warm; lease kept |
| Pipeline stop | `POST /v1/pipelines/{id}/stop` | operator | `RUNNING/PAUSED → STOPPED`: flush, close the source connection (PG slot kept); lease kept |
| Pipeline resume | `POST /v1/pipelines/{id}/resume` | operator | `PAUSED/STOPPED → RUNNING` |
| Pipeline restart | `POST /v1/pipelines/{id}/restart` | operator | desired state `RUNNING` only (a pipeline in `ERROR` included: a restart runs it again): drain, discard unflushed data, replay from the last confirmed position; otherwise `409` |
| Pipeline flush | `POST /v1/pipelines/{id}/flush` | operator | immediate flush; resets the flush timer and counters; no state change |
| Leave cluster | `POST /v1/cluster/leave` | admin | planned drain and hand-off of owned pipelines; the process stays up, not ready, and still answers the API; a process restart joins again |
| Shutdown | `POST /v1/admin/shutdown` | admin | as SIGTERM: the leave, then the process exits |
| API description | `GET /v1/openapi.json` | viewer | the OpenAPI 3.1 document of the `/v1` endpoints, each with its min role |

Pause/stop/resume set the pipeline's persisted **desired state**; it survives restarts, failover and config reloads (a reload never resumes a paused or stopped pipeline). Illegal transitions return `409 Conflict` with the current state; asking for the state the pipeline is in already changes nothing and is not an error. Any node answers any endpoint: a lifecycle action is recorded in the pipeline's state location and the pipeline's owner carries it out at its next poll (restart and flush answer `202`), and a status is read from the same documents.

## Observability catalogue

Names are the exposed sample names (OpenMetrics): a counter's name ends in `_total`.

**Cardinality:** every metric carries `{pipeline, source_host, source_db}`; a `table` label is added only when `sink.metrics.per_table = true`, and then only to a few per-table gauges (snapshot progress and snapshot state). Series count scales with pipelines, not tables, by default. Source: `research/Observability_Metrics_and_Tracing.md`.

| Family | Metrics (mandatory) |
|---|---|
| Replication | `data_avalanche_replication_lag_seconds` (headline — see the lag targets in Scope), `data_avalanche_rows_read_total{op}`, `data_avalanche_rows_written_total`, `data_avalanche_flush_duration_seconds`, `data_avalanche_flush_rows`, `data_avalanche_transaction_buffer_bytes` |
| Source health | `data_avalanche_wal_lag_bytes` (PG) / `data_avalanche_binlog_lag_seconds` (MySQL/MariaDB), `data_avalanche_source_reconnects_total` |
| Snapshot | `data_avalanche_snapshot_rows_done`, `data_avalanche_snapshot_state`, `data_avalanche_snapshot_chunk_duration_seconds`, `data_avalanche_reconcile_log_entries`, `data_avalanche_reconcile_log_keys` (the snapshot reconciler's log, DA-SNAP-11) |
| Sink | `data_avalanche_sink_commit_duration_seconds{sink_type}`, `data_avalanche_sink_commit_failures_total`; Delta MERGE cost and maintenance signals: `data_avalanche_sink_merge_files_rewritten_total`, `data_avalanche_sink_merge_rows_copied_total`, `data_avalanche_sink_soft_dropped_columns` |
| Clustering | `data_avalanche_lease_state`, `data_avalanche_lease_renew_failures_total`, `data_avalanche_lease_renew_latency_seconds`, `data_avalanche_cluster_members`, `data_avalanche_owned_pipelines`, `data_avalanche_ring_generation` |
| Status | `data_avalanche_pipeline_status` (running / paused / stopped / error), `data_avalanche_halted_tables` |

**Definitions:** `replication_lag_seconds` is the age of the oldest change read but not yet durable in the sink: now minus the source commit time of the oldest transaction read since the last checkpoint, `0` when nothing is pending; snapshot rows don't count. `binlog_lag_seconds` is the age of the last transaction the stream read, `0` at the server's head; once a server heartbeat has shown the stream caught up, it counts from that heartbeat as soon as the server writes past the head the stream was at; a MySQL pipeline has no `wal_lag_bytes`, a PostgreSQL one no `binlog_lag_seconds`. `rows_read_total{op}` counts row changes read (`insert`, `update`, `delete`) and snapshot rows (`snapshot`). `snapshot_state{state}` is `running` or `waiting` (held by the source, or behind the previous term's writers). `sink_commit_failures_total` counts every failed attempt, retried ones included.

**Tracing:** a span per flush/commit cycle, per snapshot chunk and per DDL apply (no per-event spans), sampled by `globals.tracing.sample_ratio`, exported over OTLP/HTTP (protobuf); a JSON log line logged in a sampled span carries its `trace_id` and `span_id`.

## Key Design Decisions

| Decision         | Choice         | Rationale                                     |
| ---------------- | -------------- | --------------------------------------------- |
| Language         | Rust           | Memory safety + performance without GC pauses |
| In-memory format | Apache Arrow   | Zero-copy, columnar, interoperable            |
| Table format     | Pluggable per pipeline: Delta Lake (v1), Apache Iceberg (v2) | ACID, time travel, cloud-native; no single-crate hostage (see `research/Multi_Sink_Architecture_and_Delta_Lake.md`) |
| Sink abstraction | `Sink` trait over a sink-neutral `TableChangeSet` + declared capabilities | Format-independent core; missing capability fails fast at pipeline start |
| Cluster state    | SWIM gossip (`foca`, membership) + storage CAS lease (ownership) | Proven, well-understood failure model; the lease is the single-runner guarantee the ring cannot give |
| Delivery         | At-least-once delivery, exactly-once effect | Idempotent primary-key upserts absorb replays; the position guard and epoch fence are defense-in-depth |
| Async runtime    | tokio          | Ecosystem standard for Rust async             |

## Resolved questions

The open questions from the first draft of this document are all closed. Each row gives the answer and where it is specified.

| # | Question | Answer | Where |
|---|---|---|---|
| OQ-C1 | Config propagation across nodes | Receiving node publishes a new config generation to cloud storage; gossip notifies peers; no proxying | `research/Clustering_Architecture_Research.md` §9 |
| OQ-C2 | Config data model | Fully specified | `Configuration_Specification.md` |
| OQ-C3 | Secret management / rotation | Env-var and file-content secrets; a changed credential file applies on reload without restart; secret managers out of v1 (sidecar-written file) | Config §4.6, §11.1; `research/V1_Scope_Decisions.md` §2 |
| OQ-P1 | Multiple flush criteria | OR — the first satisfied criterion flushes, evaluated at transaction boundaries | Config §9.4.1; `Data-Flow.md` §Phase 3 |
| OQ-P2 | In-flight data on restart | Discard unflushed batches and replay from the last confirmed position (idempotent commits absorb duplicates); pause/stop flush first | Config §12.4; `research/Pipeline_Lifecycle_State_Machine.md` |
| OQ-P3 | Pipeline hash key | The immutable pipeline `id` (a rename of the map key does not move the pipeline) | Clustering §4.1 |
| OQ-S1 | Snapshot resumability | Resumable: typed keyset cursor checkpointed per chunk; resume is idempotent | `research/Snapshot_Resumability_and_Consistency.md` §2 |
| OQ-S2 | Consistent read point | Convergent, not a frozen instant: stream-first chunk loop with position watermarks; no global lock or exported snapshot | Snapshot §3 |
| OQ-S3 | "Next to last snapshotted position" | Dissolved: on-demand snapshots don't pause the stream; watermarks reconcile | Snapshot §4 |
| OQ-S4 | Snapshot parallelism | Across tables (`parallelism`); serial keyset within a table | Snapshot §5 |
| OQ-CL1 | Split-brain prevention | Per-pipeline storage CAS lease + epoch-fenced commits; proven on S3, GCS and Azure | Clustering §5; `prototypes/lease-conformance/` |
| OQ-CL2 | Offset hand-over staleness | New owner resumes from the last persisted position, checkpointed after every committed flush; replay ≤ about one flush interval | `Data-Flow.md` Phase 5; Clustering §18.3 |
| OQ-CL3 | Rebalancing hand-off | Planned: drain (flush → commit → checkpoint → release lease); unplanned: lease expiry + replay | Clustering §8.3 |
| OQ-CL4 | Single-node mode | One-member cluster, same code path, lease active | Clustering §10 |
| OQ-ST1 | Checkpoint frequency | After every committed flush | `Data-Flow.md` Phase 5 |
| OQ-ST2 | Object-storage consistency | Conditional writes throughout: versioned state files and the lease use compare-and-swap; Delta commits are create-only PUTs (no DynamoDB) — proven on all three clouds | Clustering §5; `research/Multi_Sink_Architecture_and_Delta_Lake.md` §10.1 (P8) |
| OQ-ST3 | State file layout | Small files per concern under the per-pipeline root (pipeline state, replication position, per-table snapshot state); cross-pipeline data under `globals.state` | `Data-Flow.md` §State Layout |
| OQ-DT1 | Type mapping | Full DB → Arrow → sink mapping, per sink | `research/Type_Mapping.md` |
| OQ-DT2 | DDL during replication | At the change's stream position: classify → flush → evolve → resume; a non-replayable gap → re-snapshot | `Schema_Change_Detection_and_Evolution.md` §8 |
| OQ-DT3 | Supported evolution operations | Safe / Unsafe / Ignorable classification with a per-sink action; Unsafe halts the table | Schema_Change §7.2 |
| OQ-IC1 | Iceberg COW vs MOR | MOR with equality deletes (COW for new tables only) — v2, with the Iceberg sink; the v1 Delta sink uses MERGE | `research/Ghost_Row_Dedup_and_Primary_Keys.md` §5 |
| OQ-IC2 | Iceberg catalog | Glue, Hive Metastore and REST, chosen per pipeline (v2); HMS and REST-SigV4 are gated on the pinned crate (V-115/V-116) | `research/Catalog_Auth_Research.md`; Config §13 |
| OQ-IC3 | Delivery guarantee | At-least-once delivery, exactly-once effect (idempotent upserts + position guard + epoch fence) | `research/Delivery_Guarantee_and_Position_Guard.md` |
| OQ-API1 | API TLS / mTLS | API TLS optional (auth without TLS warns, V-052); gossip packets carry a MAC keyed from the cluster anchor, not TLS | Config §6.3.2, §6.5.1 |
| OQ-API2 | RBAC roles | `viewer` ⊆ `operator` ⊆ `admin`; see the endpoint table | `research/API_RBAC_and_Versioning.md` |
| OQ-API3 | Config reload proxy | No proxy — any node accepts a reload; generation pointer + gossip | Clustering §9.2 |
| OQ-OB1 | Distributed tracing | Optional OTLP, off by default | `research/Observability_Metrics_and_Tracing.md` §3 |
| OQ-OB2 | Replication-lag SLA | p99 ≤ 60s steady state; ≤ 2 min recovery after failover | `research/V1_Scope_Decisions.md` D-V1.3 |
| OQ-OB3 | Metric catalogue | See the catalogue above | Observability §2 |
| OQ-M1 | Typo in the clustering requirement | Fixed | — |
| OQ-M2 | Artifact deletion / maintenance | The app never deletes sink tables or catalog entries (there is no `snapshot_mode: only`; the `dump_*` modes cover it); maintenance is external in v1 | `research/Iceberg_Maintenance.md`; Sinks above |

## Sources
