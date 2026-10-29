---
updated: 2026-10-29
domain: Architecture
tags: [observability, metrics, prometheus, tracing, otlp, logs, correlation, cardinality, cdc]
resolves: [G-33, G-32]
---

# Observability: Metrics Catalogue, Tracing & Log Correlation

## Scope

Closes **G-33** (metrics catalogue), **G-32** (distributed tracing) and the §10.3 log/trace-correlation
gap. Defines the mandatory metric set, label-cardinality policy, the OTLP tracing config, and trace-id
injection into logs.

Folds in metrics already defined elsewhere: the clustering lease metrics (Clustering doc §19), the
WAL-lag guard (G-22), and the Iceberg maintenance signals (G-25).

---

## §1 — Cardinality policy (G-33 decision)

**Pipeline-level labels by default; per-table opt-in.** Every metric carries `{pipeline, source_host,
source_db}`; a **`table` label is added only when `sink.metrics.per_table = true`**, and even then only
to a small set of per-table gauges (snapshot progress, lag, delete-file count). **As built in M10
(Q-M10-2):** `table` goes on `snapshot_rows_done` and `snapshot_state` only; the lag is the
pipeline's, one series (delete-file count is Iceberg's, ⏭ v2). This bounds cardinality
for large fleets (thousands of tables) while letting operators opt into per-table detail where they
need it.

Total series ≈ `pipelines × constant` by default, vs `pipelines × tables × constant` if per-table were
always on.

---

## §2 — Mandatory metric catalogue

All Prometheus, exposed at `/metrics` (unauthenticated, per System-Overview / RBAC §2).

### Replication / throughput
- `data_avalanche_replication_lag_seconds{pipeline}` — source commit-ts to sink-commit lag (the headline SLA).
- `data_avalanche_rows_read_total{pipeline,op}` / `data_avalanche_rows_written_total{pipeline}` — counters.
- `data_avalanche_flush_duration_seconds{pipeline}` (histogram), `data_avalanche_flush_rows{pipeline}` (histogram).
- `data_avalanche_transaction_buffer_bytes{pipeline}` — large-txn backstop visibility (G-29).

### Source health
- `data_avalanche_wal_lag_bytes{pipeline}` (PG abandoned-slot guard, G-22) / `data_avalanche_binlog_lag_seconds{pipeline}`.
- `data_avalanche_source_reconnects_total{pipeline}`.

### Snapshot (per-table opt-in)
- `data_avalanche_snapshot_rows_done{pipeline[,table]}` / `data_avalanche_snapshot_state{pipeline[,table]}`.
- `data_avalanche_snapshot_chunk_duration_seconds{pipeline}` (G-6).

### Sink + maintenance (G-25; multi-sink rename 2026-09-26)
- `data_avalanche_sink_commit_duration_seconds{pipeline,sink_type}` (histogram),
  `data_avalanche_sink_commit_failures_total{pipeline,sink_type}` — sink-neutral (were `data_avalanche_iceberg_*`; the rename
  follows `Multi_Sink_Architecture_and_Delta_Lake.md` §8).
- **Delta (v1):** `data_avalanche_sink_merge_files_rewritten`, `data_avalanche_sink_merge_rows_copied`,
  `data_avalanche_sink_merge_bytes_rewritten`, `data_avalanche_sink_merge_duration_seconds` (MERGE cost — D-MS.15), and
  `data_avalanche_sink_soft_dropped_columns` (D-MS.9).
- **Iceberg (⏭ v2):** `data_avalanche_iceberg_delete_files{pipeline[,table]}` — **MOR health signal**;
  `data_avalanche_iceberg_data_file_count`, `data_avalanche_iceberg_small_file_ratio`, `data_avalanche_iceberg_snapshot_count`,
  `data_avalanche_iceberg_manifest_depth`, `data_avalanche_iceberg_metadata_json_bytes` (format-specific; keep their names).

### Clustering (from Clustering doc §19)
- `data_avalanche_lease_state{pipeline}`, `data_avalanche_lease_renew_failures_total{pipeline}`,
  `data_avalanche_lease_renew_latency_seconds{pipeline}`.
- `data_avalanche_cluster_members`, `data_avalanche_owned_pipelines`, `data_avalanche_ring_generation`.

### Pipeline status
- `data_avalanche_pipeline_status{pipeline}` — gauge enum (running/paused/stopped/error), aligned with the
  lifecycle state machine (G-NEW-15).

### As built (M6b, M10)

`doc/v1.0/M10-observability.md` D-M10.1…8 holds the rulings, and the catalogue test
(`crates/data-avalanche-telemetry/tests/catalogue.rs`) holds every sample with its labels.
- **The lag** (Q-M10-3): now minus the source commit time of the oldest transaction read but not
  yet durable in the sink, `0` when nothing is pending; snapshot rows don't count. The worker's
  status carries the commit time; the controller computes the age at each status change and each
  renewal poll, clamped at 0 when the node's clock is behind the source's. It sees only what the
  pipeline read: an unread backlog is the source series' to show.
  - It carries across a worker restart: what an ended worker read and didn't make durable still
    counts, so the lag grows while no worker runs (a restart's backoff, `ERROR`, a stop with a
    change pending; the example lag alert fires for a pipeline stopped that way). The next worker
    takes over once it reads a change again, or has read as far with nothing pending.
  - Known edges: it reads `0` early while a restarted worker has made its first re-read change
    durable before re-reading the next, and after a source reconnect inside one worker, until
    the change is read again (about a heartbeat on MySQL; on PostgreSQL, the server's re-decode
    of a large transaction).
- **The binlog lag** (MySQL, MariaDB): the age of the last transaction the stream read, `0` once
  the stream is at the server's head, with no series without a stream.
  - A server heartbeat says the stream caught up. The lag stays `0` while the server's head
    doesn't move past the head the first check after the heartbeat saw.
  - Once it moves past, the lag counts from the heartbeat, so a stream that stopped reading
    after an idle stretch shows its stall.
  - The series is absent while the server's head can't be read, and for a stream reopened behind
    the head until its first transaction ends.
  - A MySQL pipeline has no `wal_lag_bytes`; a PostgreSQL one has no binlog lag.
- **`rows_read_total{op}`**: `insert`, `update`, `delete` for the row changes the stream reads,
  before the collapse; `snapshot` for chunk rows.
- **`snapshot_state{state}`**: `running`, or `waiting` (held by the source, or behind the previous
  term's writers in flight); a count per state, or one-hot per table under `per_table`.
- **The sink series**: `rows_written_total` counts committed rows (a commit skipped as covered
  wrote nothing); `sink_commit_duration_seconds{sink_type}` times one table's commit, retries
  included; `sink_commit_failures_total{sink_type}` counts every failed attempt. `sink_type` is
  on these two only. `merge_bytes_rewritten` and `merge_duration_seconds` aren't in the mandatory
  catalogue and aren't exported.
- **`sink_soft_dropped_columns`**: the sink columns the source lacks, lineage and internal
  columns left out, counted at the start and after each evolve.
- Example alert rules: `deploy/prometheus/alerts.yaml` (lag > 60 s for 5 min, a degraded lease,
  halted tables, `ERROR`, a slot's WAL > 10 GiB), tested by promtool.

---

## §3 — Distributed tracing (G-32)

- **`globals.tracing.otlp_endpoint`** (optional). **Absent → tracing disabled** (zero overhead). Present
  → export OTLP spans to that collector.
- Optional `globals.tracing.sample_ratio` (default `0.0` when endpoint absent; e.g. `0.05` when set) to
  bound overhead on the hot path.
- Span model: a span per flush/commit cycle (`pipeline.flush` → `iceberg.commit`), a span per snapshot
  chunk, a span per DDL apply. Source-event-level spans are **not** emitted (far too high volume) —
  tracing is at the batch/commit granularity, with counts as span attributes.

**As built (M10, D-M10.9…12):**
- **Transport** (Q-M10-1): OTLP/HTTP with protobuf (`opentelemetry-otlp` 0.33.1, the blocking
  `reqwest` client); `otlp_endpoint` is the collector's base URL (port 4318), with `/v1/traces`
  appended. No gRPC.
- **One tracer provider per process**: the resource (`service.name` `data-avalanche`,
  `service.instance.id` the node, `service.version`), a batch processor over an exporter that a
  reload swaps, and `ParentBased(TraceIdRatioBased)` whose ratio a reload sets. The layer itself
  can't be reloaded: `tracing-opentelemetry`'s `get_otel_context` finds it by downcasting through
  the subscriber, which `reload::Layer` doesn't forward. An endpoint the exporter can't use keeps
  the one in force, with a warning. An export that fails is warned of once, then every 5 min
  while it fails, and once at INFO when it works again. At exit the queue is exported, within 5 s.
- **Spans** (`sink.commit` is the sink-neutral name of `iceberg.commit`):
  - `pipeline.flush`: a root per flush; fields `pipeline`, `change_sets`, `rows`, `checkpoint`.
  - `sink.commit`: a child per table; fields `table`, `rows`, `attempts`, `outcome` (`committed`,
    `covered`, `fenced`, `refused`, `failed`, `lease lost`).
  - `snapshot.chunk`: a root; fields `pipeline`, `table`, `rows`.
  - `schema.apply`: a root; fields `pipeline`, `table`, `changes`, `outcome` (`applied`,
    `degraded`, `halted`, `failed`).
- **The traces' filter is their own**: the pipeline's spans (`data_avalanche*` at INFO) while an
  endpoint is set, nothing otherwise, whatever the log level; the Delta libraries' spans never
  reach the collector.
- Spans the batch queue (2048) can't hold are dropped, and nothing counts the drops. The export
  sends no headers, so a collector that wants a token can't take one (DA-OBS-7).

---

## §4 — Log / trace correlation (§10.3)

`log.format: json` already exists. Add **`trace_id` / `span_id` injection** into log records emitted
within a traced scope, so a log line can be pivoted to its trace. When tracing is disabled the fields
are simply absent. Also inject `pipeline` on every pipeline-scoped log line for grep-ability without a
trace backend.

**As built (M10, D-M10.13, D-M10.14):**
- A JSON line logged in a sampled span carries `trace_id` (32 hex digits) and `span_id` (16) at the
  top level. A span that wasn't sampled gives no ids.
- A line logged in a span with a `pipeline` field carries it as `fields.pipeline`, unless the event
  names its own. The controller's lines name the pipeline already; a library's line inside a
  commit gets it from the span.
- The formatter reads the ids through the process's dispatch, held weakly, because inside an event
  a scoped dispatch isn't the current one.
- The text formats are in colour only when stderr is a terminal and `NO_COLOR` is unset or empty
  (DA-OBS-6): a file or a collector gets no escape sequences.

---

## §5 — Config & validation impact

**Added**

- `globals.tracing.otlp_endpoint` (R, optional; promoted from §16 G-32) + `tracing.sample_ratio`
  (R, default 0.0).
- `sink.metrics.per_table` (R, default `false`).

**Validation**

1. `sample_ratio ∈ [0, 1]`.
2. `otlp_endpoint` absent ⇒ tracing disabled (no error); `sample_ratio > 0` with no endpoint → warning.
3. `metrics.per_table = true` → info note about cardinality on large fleets.

**Doc edits owed (spec, propose-only):** System-Overview observability section to list the catalogue +
the cardinality policy; the §16 `otlp_endpoint` reservation promoted.

---

## §6 — Resolved decision summary

| Decision | Resolution |
|---|---|
| G-33 catalogue | mandatory metric set (§2) spanning replication, source, snapshot, Iceberg+maintenance, clustering, status |
| G-33 cardinality | **pipeline-level default, per-table opt-in** (`sink.metrics.per_table`) |
| G-32 tracing | `globals.tracing.otlp_endpoint` (disabled when absent) + sampling; batch/commit-granularity spans |
| §10.3 correlation | inject `trace_id`/`span_id`/`pipeline` into JSON logs |
