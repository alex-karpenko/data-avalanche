---
updated: 2026-06-20
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
to a small set of per-table gauges (snapshot progress, lag, delete-file count). This bounds cardinality
for large fleets (thousands of tables) while letting operators opt into per-table detail where they
need it.

Total series ≈ `pipelines × constant` by default, vs `pipelines × tables × constant` if per-table were
always on.

---

## §2 — Mandatory metric catalogue

All Prometheus, exposed at `/metrics` (unauthenticated, per System-Overview / RBAC §2).

### Replication / throughput
- `cdc_replication_lag_seconds{pipeline}` — source commit-ts to sink-commit lag (the headline SLA).
- `cdc_rows_read_total{pipeline,op}` / `cdc_rows_written_total{pipeline}` — counters.
- `cdc_flush_duration_seconds{pipeline}` (histogram), `cdc_flush_rows{pipeline}` (histogram).
- `cdc_transaction_buffer_bytes{pipeline}` — large-txn backstop visibility (G-29).

### Source health
- `cdc_wal_lag_bytes{pipeline}` (PG abandoned-slot guard, G-22) / `cdc_binlog_lag_seconds{pipeline}`.
- `cdc_source_reconnects_total{pipeline}`.

### Snapshot (per-table opt-in)
- `cdc_snapshot_rows_done{pipeline[,table]}` / `cdc_snapshot_state{pipeline[,table]}`.
- `cdc_snapshot_chunk_duration_seconds{pipeline}` (G-6).

### Iceberg sink + maintenance (G-25)
- `cdc_iceberg_commit_duration_seconds{pipeline}` (histogram), `cdc_iceberg_commit_failures_total{pipeline}`.
- `cdc_iceberg_delete_files{pipeline[,table]}` — **MOR health signal**.
- `cdc_iceberg_data_file_count`, `cdc_iceberg_small_file_ratio`, `cdc_iceberg_snapshot_count`,
  `cdc_iceberg_manifest_depth`, `cdc_iceberg_metadata_json_bytes`.

### Clustering (from Clustering doc §19)
- `cdc_lease_state{pipeline}`, `cdc_lease_renew_failures_total{pipeline}`,
  `cdc_lease_renew_latency_seconds{pipeline}`.
- `cdc_cluster_members`, `cdc_owned_pipelines`, `cdc_ring_generation`.

### Pipeline status
- `cdc_pipeline_status{pipeline}` — gauge enum (running/paused/stopped/error), aligned with the
  lifecycle state machine (G-NEW-15).

---

## §3 — Distributed tracing (G-32)

- **`globals.tracing.otlp_endpoint`** (optional). **Absent → tracing disabled** (zero overhead). Present
  → export OTLP spans to that collector.
- Optional `globals.tracing.sample_ratio` (default `0.0` when endpoint absent; e.g. `0.05` when set) to
  bound overhead on the hot path.
- Span model: a span per flush/commit cycle (`pipeline.flush` → `iceberg.commit`), a span per snapshot
  chunk, a span per DDL apply. Source-event-level spans are **not** emitted (far too high volume) —
  tracing is at the batch/commit granularity, with counts as span attributes.

---

## §4 — Log / trace correlation (§10.3)

`log.format: json` already exists. Add **`trace_id` / `span_id` injection** into log records emitted
within a traced scope, so a log line can be pivoted to its trace. When tracing is disabled the fields
are simply absent. Also inject `pipeline` on every pipeline-scoped log line for grep-ability without a
trace backend.

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
