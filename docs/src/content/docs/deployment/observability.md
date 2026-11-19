---
title: Observability
description: The metrics a node exposes, the alerts that come with it, the log, and the traces.
sidebar:
  order: 3
---

## Metrics

`GET /metrics` on every node, with no credentials, in the Prometheus text format. Each node
exposes the pipelines it owns. Every pipeline series has the labels `pipeline`, `source_host` and
`source_db`; the node's own gauges (`data_avalanche_cluster_members`,
`data_avalanche_owned_pipelines`, `data_avalanche_ring_generation`,
`data_avalanche_config_generation`) and its event counters have none. `sink.metrics.per_table: true` adds a `table` label
to the snapshot gauges.

| Family | Metrics |
|---|---|
| Replication | `data_avalanche_replication_lag_seconds`, `data_avalanche_rows_read_total{op}`, `data_avalanche_rows_written_total`, `data_avalanche_flush_duration_seconds`, `data_avalanche_flush_rows`, `data_avalanche_transaction_buffer_bytes` |
| Source | `data_avalanche_wal_lag_bytes` (PostgreSQL), `data_avalanche_binlog_lag_seconds` (MySQL, MariaDB), `data_avalanche_source_reconnects_total` |
| Snapshots | `data_avalanche_snapshot_rows_done`, `data_avalanche_snapshot_state`, `data_avalanche_snapshot_chunk_duration_seconds`, `data_avalanche_reconcile_log_entries`, `data_avalanche_reconcile_log_keys` |
| Sink | `data_avalanche_sink_commit_duration_seconds{sink_type}`, `data_avalanche_sink_commit_failures_total`, `data_avalanche_sink_merge_files_rewritten_total`, `data_avalanche_sink_merge_rows_copied_total`, `data_avalanche_sink_soft_dropped_columns` |
| Cluster | `data_avalanche_lease_state`, `data_avalanche_lease_renew_failures_total`, `data_avalanche_lease_renew_latency_seconds`, `data_avalanche_cluster_members`, `data_avalanche_owned_pipelines`, `data_avalanche_ring_generation` |
| Status | `data_avalanche_pipeline_status` (running, paused, stopped, error), `data_avalanche_halted_tables` |
| Events | `data_avalanche_events_written_total`, `data_avalanche_events_dropped_total`: the node's writes to the cluster's [timeline](/data-avalanche/deployment/events/) |
| Process | `process_resident_memory_bytes`, `process_cpu_seconds_total` (Linux, the usual names), `data_avalanche_process_memory_limit_bytes`, `data_avalanche_process_cpu_limit_cores`: the container's limits, absent when there is none (a limit set on the pod alone, above its containers', isn't read: set it on the container) |
| Gossip | `data_avalanche_gossip_probe_rtt_seconds`: a probe's round trip; `data_avalanche_gossip_suspicions_total{direction}`: suspicions this node `raised` of another, and `received` of itself |

The ones to watch first:

- **`data_avalanche_replication_lag_seconds`**: the age of the oldest change read but not yet
  committed; `0` when nothing waits. After a failover, a restart or a source reconnect, it keeps
  counting from the oldest change the earlier session left uncommitted until the new one has read
  that far again. v1's target is a p99 under 60 s at steady state, and back
  under it within 2 minutes of a node failure. A snapshot, a transaction larger than the flush
  bounds, and heavy random-key MERGEs on large tables are outside the target. Measured at the
  defaults (a 30 s flush) on one machine: a p99 of 31 s (p50 16 s) for one node at 1000 changes
  a second over four tables, on both PostgreSQL 18 and MySQL 8.4; and in a three-node cluster,
  the lag back under 60 s 52 to 54 s after the lease holder was killed. At steady state the lag follows the flush
  interval.
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
| `DataAvalancheEventsDropped` | a node dropped events from the cluster's timeline in the last 15 minutes | [Events](/data-avalanche/deployment/events/#when-the-timeline-has-gaps) |

## Chart history

Every node keeps a short history of the figures its charts need, without Prometheus, in the
cluster's state location (`globals.state`, under `history/`). Each node samples what it owns:

| Of | Series | Every |
|---|---|---|
| a pipeline, by the node that holds its lease | `lag_seconds`, `changes_per_minute`, `wal_held_bytes` (PostgreSQL), `binlog_lag_seconds` (MySQL, MariaDB), `snapshot_rows_per_second` (while a snapshot copies) | 15 s |
| each of its tables | `changes_per_minute`, `files_rewritten` (the costliest MERGE of the minute), `commit_seconds` (the slowest commit of the minute) | 1 min; a table the pipeline starts tracking at once |
| the node | `memory_bytes` and `cpu_cores` (Linux), `lease_renew_seconds` (its slowest lease renewal since the sample before) | 15 s |

`lag_seconds` is what the pipeline's live figures show: how far behind lake readers are. The
samples of the current hour are written when the hour begins or a new series starts (a table the
pipeline starts tracking shows within a sample), then every minute, so a node that
crashes loses at most a minute of them, and a pipeline's history follows it to the node that
takes it over. A storage outage holds the samples in memory and writes them when it ends; it
never slows a pipeline. History older than `globals.console.history.retention` (24 h by default)
is deleted a day at a time. A pipeline's history costs about 60 writes an hour, to one object
an hour.

`GET /v1/history` reads it, from any node:

```sh
curl -u alice:… 'https://avalanche.example.com:8080/v1/history?pipeline=orders-pg&series=lag_seconds&since=2026-11-18T06:00:00Z'
```

It answers each series' points, `[unix seconds, value]`, and `kept_since`, where the history kept
begins. The step keeps a series to 1440 points by default; a `step` asked, in seconds, is never
finer than the samples nor than 1440 points over the range. A longer step keeps each step's peak
of a lag or a size, and the mean of a rate.

### Older ranges from Prometheus

With `globals.console.prometheus` set, a pipeline's range older than the retention comes from
that Prometheus, which scrapes the nodes' `/metrics`: each series is one range query over
`data_avalanche_replication_lag_seconds`, `data_avalanche_rows_read_total`,
`data_avalanche_wal_lag_bytes` or `data_avalanche_binlog_lag_seconds`, grouped by `pipeline`,
with `extra_labels` in every selector so another cluster's series never mix in:

```yaml
globals:
  console:
    prometheus:
      url: https://prometheus.observability:9090
      bearer_token: ${PROM_TOKEN}
      extra_labels: { cluster: prod-eu }
```

Each series then says where its older range came from (`older`): `prometheus` (with no points
for a pipeline younger than the retention); `failing`, when Prometheus didn't serve it:
unreachable or past `timeout`, a refusing or invalid answer, one over 8 MiB, or no series for the
labels anywhere in the range asked (the history kept is shown, and the answer's `prometheus` says
what failed); or `not_in_prometheus` for a table's or a node's series, whose metrics carry no such
label. A series without `older` asked nothing older than the history kept. Without the setting,
the history kept is the whole answer, and nothing is marked.

## The live stream

`GET /v1/stream` sends the cluster's changes as they happen, as server-sent events, from any
node: the node asked follows every other member's stream with the caller's own credentials and
merges them into one. It sends no replay: read the current state (`GET /v1/pipelines`,
`/v1/cluster/status`, `/v1/history`), then apply what follows.

```sh
curl -N -u alice:… 'https://avalanche.example.com:8080/v1/stream?pipeline=orders-pg'
```

Each message's data is a JSON object; `node` names the node that sent it.

| Message | When | Data |
|---|---|---|
| `hello` | first | `at`, and `follows`: the other members followed |
| `pipeline` | a pipeline's state, error or lease term changes on the node that runs it (looked at every second); `standby` once when it stops running it | `pipeline`, `actual`, `error`, `owner_epoch` |
| `figures` | every 5 s, for each pipeline a node runs | what `GET /v1/pipelines/{id}/live` answers, without the halts' start times |
| `node` | every 5 s, from each node | `phase`, `process`, `gossip`, and the `pipelines` it runs |
| `samples` | each chart sample, as it is taken | `scope` (`pipeline` or `node`), `pipeline`, `at` (unix seconds), `series`, and a pipeline's `tables` |
| `event` | each event, once written | the event as `GET /v1/events` lists it |
| `members` | the members or their phases change | `members`: each one's `node` and `phase` |
| `peer` | another node's stream is followed, or can't be (it is followed again after 1, 2, 5, then every 10 s) | `peer`, `state` (`following` or `unreachable`), `error` |
| `reset` | the stream fell more than 1024 messages behind and missed some (`lagged`), or the caller's credentials no longer verify (`credentials`) | `reason`; with `lagged`, `missed`: read the current state again; with `credentials` the stream ends: open it again with fresh credentials |

`pipeline` takes a pipeline's id, or several comma-separated, and sends only the messages about
them. `kind` takes the kinds wanted, comma-separated, of `pipeline`, `figures`, `node`,
`samples` and `event`. `hello`, `members`, `peer` and `reset` are always sent. A `pipeline`
filter drops the messages that name no pipeline: `node`, a node's `samples`, and the events
without one. A pipeline the generation doesn't have is `404`; a kind or a parameter that isn't
one, or a parameter given twice or empty, `400`.

A node killed sends no `standby`: when two nodes name the same pipeline as theirs, the one with the
higher `owner_epoch` runs it. When a `peer` comes back to `following`, that node's states are
sent again, but not its events from the gap: read `GET /v1/events` for the gap.

A node holds at most 64 streams at once, those it serves its peers included; past that it answers
`503` with `Retry-After`. Each page holds a place on every node, since its node follows the
others: a node full of streams shows on other pages as `unreachable`, "it answered 503". A stream
with nothing to send sends a comment every 15 s, so a proxy in front of the nodes must wait longer
than that before it closes one, and must not buffer the answer (the node sends
`X-Accel-Buffering: no` for nginx). A node builds no message while no stream is open on it, and
ends its streams when it stops. The caller's credentials are checked when the stream opens, and
again by the node asked before it follows another node again: once they no longer verify (a
token expired, a password changed), the stream is sent `reset` (`credentials`) and ends.

## The log

To stderr, as `globals.log.format`: `json` (one object a line), `compact` or `pretty`; at
`globals.log.level`, a level or a filter such as `info,data_avalanche_pipeline=debug`. Both apply
on reload, and `--log-level` / `--log-format` override them. A log line never carries a secret.

## Traces

With `globals.tracing.otlp_endpoint` set, a node sends spans over OTLP/HTTP (protobuf) to
`<endpoint>/v1/traces`: one per flush and its commits, per snapshot chunk, per schema change
applied, never one per row. `sample_ratio` (0.05 with an endpoint) samples them. A JSON log line
written inside a sampled span carries its `trace_id` and `span_id`, so a flush's log lines (at
`debug` for `data_avalanche_pipeline::writer`) lead to its trace.

A hosted collector that needs a token takes it in `globals.tracing.headers`, sent with each
export. The values are secrets: they never reach the log or the API's answers. Write them as
`${VAR}`, so that the token stays out of the config text:

```yaml
globals:
  tracing:
    otlp_endpoint: https://otlp.example.com
    headers:
      authorization: "Bearer ${OTLP_TOKEN}"
```

`OTEL_EXPORTER_OTLP_TRACES_HEADERS` in a node's environment (`key=value,…`), read first, or else
`OTEL_EXPORTER_OTLP_HEADERS`, is sent too, and wins over a configured header of the same name. Use
an `https` endpoint: a token sent over `http` can be read on the way.
