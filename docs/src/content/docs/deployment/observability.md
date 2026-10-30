---
title: Observability
description: The metrics a node exposes, the alerts that come with it, the log, and the traces.
sidebar:
  order: 3
---

## Metrics

`GET /metrics` on every node, with no credentials, in the Prometheus text format. Each node
exposes the pipelines it owns. Every series has the labels `pipeline`, `source_host` and
`source_db`; `sink.metrics.per_table: true` adds a `table` label to the snapshot gauges.

| Family | Metrics |
|---|---|
| Replication | `data_avalanche_replication_lag_seconds`, `data_avalanche_rows_read_total{op}`, `data_avalanche_rows_written_total`, `data_avalanche_flush_duration_seconds`, `data_avalanche_flush_rows`, `data_avalanche_transaction_buffer_bytes` |
| Source | `data_avalanche_wal_lag_bytes` (PostgreSQL), `data_avalanche_binlog_lag_seconds` (MySQL, MariaDB), `data_avalanche_source_reconnects_total` |
| Snapshots | `data_avalanche_snapshot_rows_done`, `data_avalanche_snapshot_state`, `data_avalanche_snapshot_chunk_duration_seconds`, `data_avalanche_reconcile_log_entries`, `data_avalanche_reconcile_log_keys` |
| Sink | `data_avalanche_sink_commit_duration_seconds{sink_type}`, `data_avalanche_sink_commit_failures_total`, `data_avalanche_sink_merge_files_rewritten_total`, `data_avalanche_sink_merge_rows_copied_total`, `data_avalanche_sink_soft_dropped_columns` |
| Cluster | `data_avalanche_lease_state`, `data_avalanche_lease_renew_failures_total`, `data_avalanche_lease_renew_latency_seconds`, `data_avalanche_cluster_members`, `data_avalanche_owned_pipelines`, `data_avalanche_ring_generation` |
| Status | `data_avalanche_pipeline_status` (running, paused, stopped, error), `data_avalanche_halted_tables` |

The ones to watch first:

- **`data_avalanche_replication_lag_seconds`**: the age of the oldest change read but not yet
  committed; `0` when nothing waits. v1's target is a p99 under 60 s at steady state, and back
  under it within 2 minutes of a node failure. A snapshot, a transaction larger than the flush
  bounds, and heavy random-key MERGEs on large tables are outside the target.
- **`data_avalanche_wal_lag_bytes`**: the WAL a PostgreSQL slot holds on the server's disk.
- **`data_avalanche_halted_tables`** and **`data_avalanche_pipeline_status`**: something waits for
  an operator.
- **`data_avalanche_sink_merge_files_rewritten_total`**: what the flushes cost (see
  [Sink and flushing](/data-avalanche/configuration/sink/#flushing)).

## Alerts

[`deploy/prometheus/alerts.yaml`](https://github.com/alex-karpenko/data-avalanche/blob/main/deploy/prometheus/alerts.yaml)
is a starting set of Prometheus rules, with `promtool` tests beside it:

| Alert | When | Runbook |
|---|---|---|
| `DataAvalancheReplicationLag` | lag above 60 s for 5 minutes | [Failover](/data-avalanche/runbooks/failover/), [Sink and flushing](/data-avalanche/configuration/sink/) |
| `DataAvalancheLeaseDegraded` | a lease `degraded` (a renewal failed, it runs on its safety deadline) for 2 minutes | [Failover](/data-avalanche/runbooks/failover/) |
| `DataAvalancheTablesHalted` | a halted table | [A halted table](/data-avalanche/runbooks/halted-table/) |
| `DataAvalanchePipelineError` | a pipeline in `error` | [A halted table](/data-avalanche/runbooks/halted-table/#a-pipeline-in-error) |
| `DataAvalancheWalLag` | a slot holds more than 10 GiB of WAL for 10 minutes | [A lost position](/data-avalanche/runbooks/position-lost/) |

## The log

To stderr, as `globals.log.format`: `json` (one object a line), `compact` or `pretty`; at
`globals.log.level`, a level or a filter such as `info,data_avalanche_pipeline=debug`. Both apply
on reload, and `--log-level` / `--log-format` override them. A log line never carries a secret.

## Traces

With `globals.tracing.otlp_endpoint` set, a node sends spans over OTLP/HTTP (protobuf) to
`<endpoint>/v1/traces`: one per flush and its commits, per snapshot chunk, per schema change
applied, never one per row. `sample_ratio` (0.05 with an endpoint) samples them. A JSON log line
written inside a sampled span carries its `trace_id` and `span_id`.
