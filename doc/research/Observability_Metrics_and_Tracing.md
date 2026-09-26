---
updated: 2026-09-26
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
