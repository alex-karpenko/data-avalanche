---
updated: 2026-10-28
domain: Architecture
tags: [v1, implementation-plan, m10, observability, metrics, prometheus, tracing, otlp, opentelemetry, logs, alerts, jaeger, e2e]
resolves: [DA-OBS-6]
---

# M10 — Observability Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** System-Overview's observability as v1 ships it (roadmap M10):
- every series of the mandatory metric catalogue, with the cardinality policy;
- OTLP tracing: a span per flush and per table commit, per snapshot chunk and per schema
  change applied; sampled by `globals.tracing.sample_ratio`, and changed by a reload;
- `trace_id` and `span_id` in JSON logs, and the `pipeline` of the span a line is logged in;
- text logs in colour only on a terminal (DA-OBS-6);
- example Prometheus alert rules, tested by promtool;
- the exit: a catalogue test, and a tracing smoke test against an OTLP collector container.

**Architecture:** Two channels carry what the worker knows to the metrics, as they do since M6b:

- **Events** (Task 2): a new `Observer` trait in `data-avalanche-pipeline` hears each flush, each
  commit, each failed commit attempt and each snapshot chunk read, with its rows and its time.
  The histograms and the counters of each need every occurrence, which a status sampled at
  intervals would miss.
- **State** (Tasks 3–4): `WorkerStatus` gains what a gauge shows:
  - the commit time of the oldest change read but not yet durable (the lag's start, Q-M10-3);
  - the rows read per operation;
  - the transaction buffer's bytes;
  - a snapshot waiting;
  - the soft-dropped columns per table;
  - for MySQL, the age of the last transaction the stream read (the binlog lag). It comes
    through `RetainedLog`, whose `bytes` becomes optional: MySQL has no retained WAL.
- **The metrics** (Task 1, `data-avalanche-telemetry`): the missing families and
  `PipelineMetrics` methods for them. Task 5 has the controller implement the `Observer` over
  the pipeline's metrics, and set the gauges from each status it observes. The lag is computed
  there, at each status change and at every renewal poll, so it grows while nothing changes.
- **Traces** (Task 6): one OpenTelemetry tracer provider for the process. Its exporter
  (OTLP/HTTP, protobuf, Q-M10-1) is swapped when `globals.tracing` changes, and its sampler's
  ratio set. The pipeline's code opens `tracing` spans; `tracing-opentelemetry` turns them into
  OTLP spans.
- **The binary** (Task 7): the logger composes a format layer, filtered by the log level, with
  the traces' layer, filtered on its own. The traces' filter passes the pipeline's spans while an
  endpoint is set, and nothing otherwise. The JSON format adds the ids and the pipeline.
- **Deploy** (Task 8): `deploy/prometheus/alerts.yaml` and its promtool tests; CI runs them.
- **E2E** (Task 9): a Jaeger container in `test-infra`; the binary's `/metrics` scraped live,
  and its traces read back through Jaeger's query API, on either source.

**Tech Stack:** Rust 1.98.1; `prometheus-client` (M6b); new: `opentelemetry`, `opentelemetry_sdk`
and `opentelemetry-otlp` `=0.33.1` (`trace`, `http-proto`, `reqwest-blocking-client`) and
`tracing-opentelemetry` `=0.34.0`; `tracing-subscriber` 0.3.23 (`registry`, `json`); promtool from
`prom/prometheus:v3.15.0` (2026-09-25); `jaegertracing/jaeger:2.21.0` (2026-09-15).

**Spec:**
- `doc/specs/System-Overview.md`: the Observability bullets and §Observability catalogue (the
  families, the cardinality line, the tracing line).
- `doc/specs/Configuration_Specification.md`: §6.4 `globals.log`, §6.6 `globals.tracing`
  (V-097, V-098), §9.4 `sink.metrics.per_table`, §1.2 (R fields).
- `doc/research/Observability_Metrics_and_Tracing.md`: §2 the catalogue and cardinality, §3
  tracing, §4 log/trace correlation.
- `doc/v1.0/00-roadmap.md`: §3.12 (telemetry), §3.11 (the worker), §4 M10, §5 (test gates).
- `doc/v1.0/M6b-single-node-control.md`: the telemetry crate's design (D-M6b on the metrics: the
  handle per pipeline, totals per worker, series removed when the pipeline leaves the node).

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`; `telemetry` is used by `cluster` and the binary, never by the
  pipeline: the pipeline reports to an `Observer` it is handed. `testkit` is a dev-dependency only.
- **Names** are the catalogue's, verbatim (System-Overview §Observability catalogue): every
  sample starts `data_avalanche_`, a counter's ends in `_total`, a histogram's samples are
  `_bucket`, `_sum`, `_count`.
- **Cardinality** (System-Overview, Q-M10-2): every pipeline series carries `{pipeline,
  source_host, source_db}`; `sink.metrics.per_table` adds `table` to `snapshot_rows_done` and
  `snapshot_state` only. `op` (`insert`, `update`, `delete`, `snapshot`), `state`, `status`,
  `class` and `sink_type` are bounded enums.
- **Tracing is off unless `globals.tracing.otlp_endpoint` is set** (config §6.6: "Absent ⇒
  tracing disabled (zero overhead, no error)"); spans are at batch/commit granularity, "never per
  source event".
- **Dependencies:** exact pins (`=`) for the four OpenTelemetry crates, as every other workspace
  dependency of a 0.x crate that moves fast. They add 7 packages to `Cargo.lock`, among them
  `reqwest` 0.13 (the OTLP client's; rustls, as the workspace's other TLS).
- **Time in tests:** the pipeline's and the controller's tests run on tokio's paused clock
  (`start_paused = true`); the telemetry, logging, promtool and E2E tests run in real time.
- **Test gates:**
  - Plain `cargo test` stays hermetic: the OTLP tests export to a collector on a thread of the
    test (the testkit's `otlp_collector`), never to a container.
  - The source ITs need `DATA_AVALANCHE_IT=1` (`test-infra/up.sh`); the MySQL ones run on the
    server `DATA_AVALANCHE_MYSQL` names.
  - The E2E suite needs `DATA_AVALANCHE_IT=1` and the `e2e-hooks` feature, runs with
    `--test-threads=1`, and runs on either source (`DATA_AVALANCHE_E2E_SOURCE`). Its tracing
    scenario needs the `jaeger` service (Task 9).
  - The promtool tests need Docker (`prom/prometheus:v3.15.0`); CI's `check` job runs them.
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `thiserror` types (one new: `TracesError`).
- Code blocks are the verified content, already `rustfmt`-formatted. Every block is a `git
  apply`-ready diff, one per file; a step's diffs apply in the order given, after the earlier
  steps'.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt a person running the binary in production with
monitoring on. Each is pinned by a test in the task after the arrow.

1. **The source's clock ahead of the node's** (a skewed NTP, a replica in another zone). The lag
   must never go negative or wrap to a huge number: it reads 0 → Task 5
   (`the_lag_is_the_oldest_unsynced_change_s_age`, "a clock behind the source's").
2. **A collector that is down, or takes the connection and never answers.** The pipeline must
   not wait for it: spans queue in the batch processor and are dropped when it is full, and the
   process's exit waits a bounded time → Task 6
   (`a_collector_that_never_answers_doesn_t_hold_the_spans_up`).
3. **A reload that changes or removes the endpoint, or names one that isn't a URL.** The spans
   after it go to the new endpoint, or nowhere, and a bad one keeps the one in force with a
   warning; a typo never stops a running pipeline → Task 6
   (`a_new_endpoint_takes_the_next_spans_and_none_takes_none`,
   `an_endpoint_that_isn_t_a_url_is_refused_and_the_one_in_force_stays`).
4. **A source that stops changing.** The lag must fall to 0 once the last change is durable, not
   stay at the last value or grow; on MySQL the binlog lag is 0 at the server's head → Task 3
   (`a_durable_flush_clears_the_lag_and_the_buffer`), Task 4
   (`the_binlog_lag_is_zero_at_the_head_else_the_last_transaction_s_age`).
5. **A log pipeline that greps or parses the binary's stderr.** Text logs written to a file or a
   collector carry no escape sequences, and a JSON line inside a commit names its pipeline even
   when a library logged it → Task 7 (`a_text_log_is_in_colour_only_when_asked`,
   `a_json_line_in_a_sampled_span_carries_its_ids_and_its_pipeline`), and the MySQL E2E halt
   scenarios, which search the log as is.

## Decisions and rulings

**Asked and answered** (2026-10-28, all four "Recommended"). Oleksii approved the plan and its
spec patches 2026-10-28.

| # | Question | Answer |
|---|---|---|
| Q-M10-1 | Which OTLP transport? | **OTLP/HTTP with protobuf only**, the collector's port 4318. `otlp_endpoint` is the collector's base URL; spans go to `<endpoint>/v1/traces`. No gRPC (it would add `tonic` and a second HTTP/2 stack). Spec edit: config §6.6's row, and its example's `:4317` becomes `:4318` |
| Q-M10-2 | Which series take the `table` label under `per_table`? | **Snapshot progress and snapshot state only**: the lag is the pipeline's, a single series. Spec edit: System-Overview's cardinality line ("snapshot progress, lag" → "snapshot progress and snapshot state") |
| Q-M10-3 | What is `replication_lag_seconds`? | **The age of the oldest undelivered change**: now minus the source commit time of the oldest transaction read but not yet durable in the sink (its flush committed and checkpointed); `0` while nothing is pending. Snapshot rows don't count. Spec edit: the definition under the catalogue |
| Q-M10-4 | One milestone, or two? | **One milestone, about nine tasks** |

**Rulings where the plan decides** (each pinned by a test):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M10.1 | **Events through an `Observer`, gauges through `WorkerStatus`.** `pipeline::observe::Observer` has four methods with no-op defaults: `flushed(rows, took)` (a job's commits and its checkpoint, timed together), `committed(rows, took)` (one table's commit), `commit_failed()` (one failed attempt), `chunk_read(took)` (one snapshot chunk read). `WorkerDeps.observer: Arc<dyn Observer>`; `NoObserver` for the tests that don't listen. The gauges' state is in `WorkerStatus`, which the controller already samples | A histogram or a counter needs each occurrence; a status sampled at a change and at each renewal poll gives the latest value only. The worker can't name the telemetry crate (the dependency direction), so it reports to a trait the cluster implements | Task 2 (all three tests), Task 5 (`the_worker_s_reads_flushes_and_lag_reach_the_metrics`) |
| D-M10.2 | **What the sink series count.** `rows_written_total` adds a committed change set's upsert and delete rows; a commit the sink skips as covered (`SkippedCovered`) wrote nothing and isn't counted, nor timed. `sink_commit_duration_seconds{sink_type}` times one table's commit, retries included. `sink_commit_failures_total{sink_type}` counts every failed attempt, the retried ones too. `flush_rows` is a job's rows, `flush_duration_seconds` its commits and its checkpoint | A retried failure is still a failure an operator wants to see rising (a throttled store, a conflict storm); the job's time is what holds the next flush up | Task 2 (`a_flush_reports_each_commit_then_itself`, `every_failed_commit_attempt_is_reported`) |
| D-M10.3 | **The lag** (Q-M10-3). The session keeps the commit time of the open transaction (`Begin`'s, which both sources carry), the oldest one read since the last job was handed to the writer, and the jobs not durable yet, each with its oldest commit time. `WorkerStatus::unsynced_since_micros` is the oldest of them, `None` when nothing is pending. A slice of a large transaction leaves the rest of it pending. The controller sets `replication_lag_seconds` to now − since at each status change and at every renewal poll, clamped at 0 | The lag must grow while a flush is stuck, which only the controller's clock sees; a status changed at every read would flood the watch | Task 3 (`the_status_shows_the_oldest_unsynced_change_the_rows_read_and_the_buffer`, `a_durable_flush_clears_the_lag_and_the_buffer`), Task 5 (`the_lag_is_the_oldest_unsynced_change_s_age`) |
| D-M10.4 | **The binlog lag** (MySQL, MariaDB). `RetainedLog { bytes: Option<u64>, streamed, behind: Option<Duration> }`. At each heartbeat's read, MySQL compares the stream's last read position with the server's head: at the head, or after a server heartbeat (an idle server), `behind` is 0; behind it, the age of the last transaction read; without a stream, `None` and the series is removed. MySQL's `bytes` is `None`, so a MySQL pipeline no longer exports `wal_lag_bytes`; PostgreSQL's `behind` is `None` | The catalogue names `binlog_lag_seconds` for MySQL and `wal_lag_bytes` for PostgreSQL. A binlog's bytes behind the head say nothing of time, and the 0 they gave MySQL before M10 was misleading | Task 4 (all three tests), Task 9 (`a_running_pipeline_exports_the_catalogue`: neither source has the other's series) |
| D-M10.5 | **The rows read** (`rows_read_total{op}`): each row change the stream reads, before the collapse, by its operation (`insert`, `update`, `delete`); each row of a snapshot chunk as `snapshot`. A truncate isn't a row. The counts are the worker's totals; the metrics add each worker's difference, so a restart or a reconnect goes on counting | The catalogue's `{op}`; M6b's totals per worker | Task 1 (`counters_keep_counting_across_workers`), Task 3, Task 9 (the four counts on either source) |
| D-M10.6 | **`snapshot_state{state}`**: `running` while a table's snapshot reads; `waiting` while it can't: the source holds it, or a table's writers from a previous term are still in flight at the start (Data-Flow Phase 0). Without `per_table`, a count per state; with it, one series per table, one-hot | The two states a table in a snapshot can be in; `completed` is no series (the table left the snapshot) | Task 1 (`snapshot_states_are_counted_unless_per_table`), Task 3 (the waiting assertion in `a_table_waits_for_the_writers_in_flight_at_the_start`) |
| D-M10.7 | **`sink_soft_dropped_columns`**: the sink columns the source no longer has, the lineage columns and the internal ones (`INTERNAL_COLUMN_PREFIX`) left out. Counted per table at the start (from the sink's table state; re-read after a baseline evolved it) and after each evolve in the stream; the pipeline's series is the sum | A soft drop keeps the column (Schema_Change §7.2, Delta has no drop): the gauge tells the operator what a manual cleanup would remove | Task 3 (`a_dropped_column_is_counted_soft_dropped`, `the_start_counts_the_sink_columns_the_source_lacks`) |
| D-M10.8 | **`sink_type`** is a label of the two commit series only, as the catalogue writes them; the pipeline's three labels stay on every other series | The catalogue's `{sink_type}`; a label on every series would change every existing dashboard's selectors for one value per pipeline | Task 1 (`every_mandatory_series_is_exported_with_its_labels`) |
| D-M10.9 | **One tracer provider, its exporter swapped.** `Traces::new(node)` builds the provider once: the resource (`service.name` `data-avalanche`, `service.instance.id` the node, `service.version`), a batch processor over a `Switch` exporter, and a sampler `ParentBased(ratio)` whose ratio is an atomic. `Traces::set(endpoint, ratio)` swaps the exporter (an OTLP/HTTP exporter to `<endpoint>/v1/traces`, or none) and sets the ratio; an endpoint that can't be exported to (not a URL) is `TracesError::Endpoint` and the exporter in force stays. The old exporter is dropped on a thread of its own (the blocking client panics when dropped inside the runtime). `shutdown()` exports what is queued, waiting at most 5 s | `get_otel_context`, which the log correlation needs, finds the OTel layer by downcasting through the subscriber, and `reload::Layer` doesn't forward that downcast: the layer must stay, so what a reload changes is behind it | Task 6 (the five telemetry tests) |
| D-M10.10 | **A bad endpoint is a warning at run time, not a V-rule.** `globals.tracing.otlp_endpoint` keeps its type check (a URL, V-097/V-098 as they are); an endpoint the exporter refuses at start or at a reload logs `the traces stay as they were: …`, and the pipelines run on | Config §6.6: tracing is optional and off by default; a collector's address must never stop replication | Task 6 (`an_endpoint_that_isn_t_a_url_is_refused_and_the_one_in_force_stays`) |
| D-M10.11 | **The spans** (target `data_avalanche_pipeline::…`, level INFO): `pipeline.flush`, a root per job, with `pipeline`, `change_sets`, `rows`, `checkpoint`; `sink.commit`, its child per table, with `table`, `rows`, `attempts`, `outcome` (`committed`, `covered`, `fenced`, `refused`, `failed`, `lease lost`); `snapshot.chunk`, a root, with `pipeline`, `table`, `rows`; `schema.apply`, a root, with `pipeline`, `table`, `changes`, `outcome` (`applied`, `degraded`, `halted`, `failed`). The worker has no span of its own, so each flush is a trace of its own | Config §6.6 ("flush → sink commit, snapshot chunk, DDL apply"); a worker's span would make a trace that lasts the worker's life | Task 6 (`a_flush_is_a_span_with_a_span_per_commit`, the span assertions of `each_chunk_read_is_reported` and `an_added_column_is_evolved_where_the_stream_meets_it`), Task 9 |
| D-M10.12 | **The traces' filter is their own.** The logger's layers are the format layer, filtered by the log level, and the traces' layer, filtered by a `Targets` of its own: `data_avalanche` at INFO while an endpoint is set, nothing otherwise. A log level of `warn` doesn't stop the spans; the Delta libraries' spans never reach the collector | A log level is about log volume; a trace of a pipeline must not depend on it, and the libraries' spans (per file, per scan) would swamp a trace | Task 7 (`the_pipeline_s_spans_are_exported_once_an_endpoint_is_set`) |
| D-M10.13 | **The log correlation** (config §6.6, Observability research §4). In the JSON format, a line logged in a sampled span carries `trace_id` (32 hex digits) and `span_id` (16) at the top level; one logged in a span with a `pipeline` field carries it as `fields.pipeline` unless the event names its own. A span not sampled gives no ids. The formatter reads the ids through the process's dispatch, held weakly: inside an event, a scoped dispatch isn't the current one any more | "When tracing is active, `trace_id` / `span_id` are injected into JSON log records"; the research doc's `pipeline` "on every pipeline-scoped log line" (the controller's lines name it already; a library's line inside a commit gets it from the span) | Task 7 (`a_json_line_in_a_sampled_span_carries_its_ids_and_its_pipeline`, `without_traces_a_json_line_carries_its_pipeline_but_no_ids`) |
| D-M10.14 | **Colour only on a terminal** (DA-OBS-6): the text formats colour when stderr is a terminal and `NO_COLOR` is unset or empty. The E2E suite's colour stripping goes | A file or a log collector got escape sequences around the fields, so a search for `code="…"` missed them | Task 7 (`a_text_log_is_in_colour_only_when_asked`, `colour_needs_a_terminal_and_no_no_color`), the MySQL E2E halt scenarios |
| D-M10.15 | **The example alert rules** (`deploy/prometheus/alerts.yaml`): replication lag > 60 s for 5 min (warning); lease degraded for 2 min (warning); halted tables > 0 (warning); pipeline in `ERROR` (critical); a slot's WAL > 10 GiB for 10 min (warning). `alerts.test.yaml` is their promtool test, run in CI's `check` job with `prom/prometheus:v3.15.0`; a hermetic test checks that the rules name only series and labels of the catalogue | The roadmap's four rules, and the one state an operator must act on at once; thresholds are examples, the file says so | Task 8 (`the_alert_rules_name_only_catalogued_series_and_labels`, promtool's test) |
| D-M10.16 | **The E2E collector is Jaeger** (`jaegertracing/jaeger:2.21.0`, in-memory, the default profile; OTLP/HTTP on host port 54318, the query API on 56686). Its v3 query API (`/api/v3/traces`; the 1.x `/api/traces` is gone in 2.x) returns OTLP JSON. The scenarios set the sources' heartbeat to 2 s, so the source's lag series appears within the test | A real collector proves the protobuf, the path and the resource; the v1.x API is 404 on 2.21 (measured) | Task 9 (both scenarios) |

**Measured while planning** (2026-10-28, recorded in the tasks):

- Every task was built in a scratch workspace on this branch's base, test first. Each task's test
  stage was committed on its own and run (Step 2's output), then its implementation (Step 4's).
- The plan's code blocks were replayed onto the base with `git apply` and compared with the
  scratch workspace: the 75 blocks apply in order and give a tree identical to the
  workspace's last commit.
- Mutation checks, each run against its test:
  - Task 1: an unknown binlog lag set to 0 instead of removed fails
    `the_lag_gauges_are_seconds_and_a_binlog_lag_unknown_has_no_series`.
  - Task 2: a failed attempt reported only when it is the last one fails
    `every_failed_commit_attempt_is_reported`.
  - Task 3: the last durable job left in the queue (`<` for `<=`) fails
    `a_durable_flush_clears_the_lag_and_the_buffer`; the lineage columns counted as soft-dropped
    fail `a_dropped_column_is_counted_soft_dropped` and `the_start_counts_the_sink_columns_the_source_lacks`.
  - Task 4: an idle stream not read as at the head fails
    `the_binlog_lag_is_zero_at_the_head_else_the_last_transaction_s_age`.
  - Task 5: the lag's age taken as an absolute difference fails
    `the_lag_is_the_oldest_unsynced_change_s_age` ("a clock behind the source's").
  - Task 6: a sampler that ignores the ratio fails `a_flush_not_sampled_is_neither_exported_nor_named` and
    `a_new_endpoint_takes_the_next_spans_and_none_takes_none`. While planning, the span test
    `each_chunk_read_is_reported` failed in one run of two of the crate's tests, because its subscriber
    was the thread's own. Task 6's `Spans` now records through one global subscriber, and six runs
    of the crate's 235 tests passed.
  - Task 7: a traces filter that passes every target fails
    `the_pipeline_s_spans_are_exported_once_an_endpoint_is_set` (the library's span is exported);
    `dispatcher::get_default` in place of the held dispatch fails
    `a_json_line_in_a_sampled_span_carries_its_ids_and_its_pipeline` (no ids: inside the event the
    scoped dispatch is the no-op one); `insert` in place of `or_insert` for the pipeline fails it
    too (the event's own `pipeline` overwritten).
  - Task 8: a lag rule's `for: 4m` fails promtool's test (the alert fires at 5 m); a selector on a
    label the series doesn't have (`lease_state{class=…}`) and a series not in the catalogue
    (`halted_table_count`) each fail `the_alert_rules_name_only_catalogued_series_and_labels`.
  - Task 9: without the start's `logger.trace(…)` the tracing scenario fails (`timed out waiting
    until a traced flush and its commit`); without the controller's `replication_lag` the
    catalogue scenario fails (`data_avalanche_replication_lag_seconds{…} isn't scraped`).
- Crates and services:
  - `get_otel_context` returns `None` through a `reload::Layer` (it doesn't forward
    `downcast_raw`); `Vec<Box<dyn Layer>>`, `Box` and `Filtered` do forward it.
  - `opentelemetry-otlp`'s `with_endpoint` takes the URL as given: `/v1/traces` is ours to append.
  - The blocking HTTP client builds its runtime on a thread of its own, and panics when dropped
    inside a tokio runtime: the replaced exporter is dropped on a spawned thread.
  - Jaeger 2.21.0: `/api/services` and `/api/traces` answer 404; `/api/v3/services`,
    `/api/v3/traces` answer, with OTLP JSON (`result.resourceSpans`).
  - The live scrape of a pipeline on PostgreSQL 18 and on `mysql-84` (Task 9): every series of
    `LIVE` present; `wal_lag_bytes` only after the first heartbeat's read (60 s by default).
- The integration tests: `slot::` (11) on PostgreSQL 18; `start::` (8, the binlog lag's IT among them) on `mysql-84`,
  `mysql-84-nogtid` and `mariadb-118`.
- The E2E scenarios: the two observability scenarios on PostgreSQL 18, `mysql-84`, `mysql-84-nogtid` and
  `mariadb-118` (17–19 s each); the MySQL halt scenarios (5) on `mysql-84`; the whole suite on
  PostgreSQL 18, 46 of 46 (605 s).
- The workspace: 1289 tests at the base, 1317 after Task 9. At Task 4's stage one run of three
  had one failing test, which didn't recur in two more runs; the measurement kept only the counts,
  so the test isn't known. The executor runs `cargo test --workspace` twice at Task 4.

**Known edges, accepted:**

- The lag sees only what the pipeline read (Q-M10-3): a stream that reads nothing, paused or
  stuck before its first change, shows 0. The source's own series (`wal_lag_bytes`,
  `binlog_lag_seconds`) show the backlog; the example rules alert on the WAL.
- An `https` endpoint goes through `reqwest` 0.13 with rustls and the platform's verifier; no test
  covers it. The export sends no headers, so a collector that wants a token can't take it (new
  gap, DA-OBS-7).
- A span is dropped when the batch processor's queue (2048) is full; nothing counts the drops.
- `sink_commit_failures_total` counts failed attempts, so a retried conflict counts once per try
  (D-M10.2).

**Documents this plan changes:**

- Applied at plan approval, in an `infra:`, a `research:` and a `gap:` commit:
  - Roadmap:
    - §3.6: `RetainedLog { bytes: Option<u64>, streamed, behind: Option<Duration> }` (D-M10.4).
    - §3.11: `pipeline::observe::{Observer, NoObserver}`, `WorkerDeps.observer`; `WorkerStatus`'s
      `unsynced_since_micros`, `rows_read: RowsRead`, `buffer_bytes`, `soft_dropped`,
      `source_behind`, `SnapshotProgress.waiting` (D-M10.1, D-M10.3, D-M10.5–7).
    - §3.12: the telemetry crate's new methods and types (`RowOp`, `SnapshotState`, `RowsRead`,
      `PipelineLabels.sink_type`, `Traces`, `TracesError`, `trace_ids`); the binary's logger
      (D-M10.9–14).
    - §4 M10: the scope as built, the exit.
    - §5: the `jaeger` service; the promtool step.
    - §9: D-IMPL-21 for D-M10.1…16; §9.1 Q-M10-1…4.
  - `Observability_Metrics_and_Tracing.md` §2–§4, as built: the lag's definition, the binlog lag,
    the snapshot states, the transport, the spans and their fields, the correlation.
  - `doc/v1.0/README.md`: the M10 row.
  - `CDC_Design_Gap_Analysis_v1.md`, §44 for M10's plan: DA-OBS-6 fixed by Task 7 (✅ when M10
    merges); DA-OBS-7 🆕 (the OTLP export takes no headers).
- **Spec edits, approved and applied 2026-10-28** (`spec:` commit):
  - `System-Overview.md` §Observability catalogue:
    - the cardinality line: "(snapshot progress, lag)" → "(snapshot progress and snapshot state)"
      (Q-M10-2);
    - after the table, the definitions: "`replication_lag_seconds` is the age of the oldest
      change read but not yet durable in the sink: now minus the source commit time of the
      oldest transaction read since the last checkpoint, `0` when nothing is pending; snapshot
      rows don't count. `binlog_lag_seconds` is the age of the last transaction the stream read,
      `0` at the server's head; a MySQL pipeline has no `wal_lag_bytes`, a PostgreSQL one no
      `binlog_lag_seconds`. `rows_read_total{op}` counts row changes read (`insert`, `update`,
      `delete`) and snapshot rows (`snapshot`). `snapshot_state{state}` is `running` or
      `waiting` (held by the source, or behind the previous term's writers).
      `sink_commit_failures_total` counts every failed attempt, retried ones included" (Q-M10-3,
      D-M10.2, D-M10.4–6);
    - the tracing line: "… sampled by `globals.tracing.sample_ratio`, exported over OTLP/HTTP
      (protobuf); a JSON log line logged in a sampled span carries its `trace_id` and `span_id`".
  - `Configuration_Specification.md` §6.6: the `otlp_endpoint` row's notes: "OTLP/HTTP
    (protobuf) collector base URL, e.g. `http://otel-collector:4318`; spans are sent to
    `<endpoint>/v1/traces`. **Absent ⇒ tracing disabled** (zero overhead, no error). An endpoint
    the exporter can't use is a warning at run time, and the one in force stays"; the example's
    `otlp_endpoint: http://otel-collector.observability:4317` → `:4318` (Q-M10-1, D-M10.10).
- `test-infra/README.md`: the `jaeger` service (Task 9).

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-telemetry/src/lib.rs` | The new families; `RowOp`, `SnapshotState`, `RowsRead`; `PipelineLabels.sink_type`; the `PipelineMetrics` methods |
| `crates/data-avalanche-telemetry/src/traces.rs` (new) | `Traces` (the provider, the swappable exporter, the ratio sampler), `TracesError`, `trace_ids` |
| `crates/data-avalanche-telemetry/tests/{catalogue,metrics,traces}.rs` | The catalogue test (new), the new series, the OTLP export (new) |
| `crates/data-avalanche-pipeline/src/observe.rs` (new), `lib.rs` | `Observer`, `NoObserver` |
| `crates/data-avalanche-pipeline/src/writer.rs` | The flush and commit reports; the `pipeline.flush` and `sink.commit` spans |
| `crates/data-avalanche-pipeline/src/session.rs`, `session/snapshot.rs` | The lag's start, the rows read, the buffer, the soft drops; the chunk reports and the `snapshot.chunk` span; a snapshot waiting |
| `crates/data-avalanche-pipeline/src/{status,accumulator,schema_change,worker}.rs` | The status fields; `buffered_bytes`; `soft_dropped` and the `schema.apply` span; the start's soft drops |
| `crates/data-avalanche-pipeline/src/test_support.rs` | `Ears` (an observer that records), `Spans` (a layer that records spans) |
| `crates/data-avalanche-core/src/source.rs` | `RetainedLog`'s optional bytes and `behind` |
| `crates/data-avalanche-source-mysql/src/{connector,start,stream}.rs` | What the stream read last; `behind` |
| `crates/data-avalanche-source-pg/src/slot.rs` | `RetainedLog`'s new shape |
| `crates/data-avalanche-testkit/src/{fake_source,otlp,lib}.rs` | `FakeSource::set_behind`; `otlp_collector` (new) |
| `crates/data-avalanche-cluster/src/controller.rs`, `test_support.rs` | `Observed` (the `Observer` over the metrics), the lag, the gauges from the status |
| `crates/data-avalanche/src/{logging,run}.rs` | The logger with the traces; the JSON correlation; colour on a terminal; `globals.tracing` at start and reload; the traces' shutdown |
| `crates/data-avalanche/tests/e2e/{observability,harness,main,mysql}.rs` | The two scenarios (new); `Options.globals`; the colour stripping gone |
| `Cargo.toml`, `Cargo.lock`, `crates/{telemetry,pipeline}/Cargo.toml` | The OpenTelemetry pins; `tracing` in the pipeline |
| `deploy/prometheus/{alerts,alerts.test}.yaml` (new) | The example rules and their promtool test |
| `.github/workflows/{ci,weekly}.yml`, `test-infra/{compose.yaml,README.md}` | The promtool step; the `jaeger` service where the E2E suite runs |

---

### Task 1: The catalogue's missing series (`data-avalanche-telemetry`)

**Files:**
- Modify: `crates/data-avalanche-telemetry/src/lib.rs`, `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-cluster/src/test_support.rs`
- Test: `crates/data-avalanche-telemetry/tests/metrics.rs`, `crates/data-avalanche-telemetry/tests/catalogue.rs` (new)

**Interfaces:**
- Consumes: `Metrics`, `PipelineMetrics` and its M6b methods, `PipelineLabels`, `Totals` (M6b).
- Produces (re-exported at the crate root):
  - `RowOp { Insert, Update, Delete, Snapshot }` and `SnapshotState { Running, Waiting }`;
  - `RowsRead { insert, update, delete, snapshot: u64 }`; `Totals.rows_read: RowsRead`;
  - `PipelineLabels.sink_type: String` (on the two commit series only, D-M10.8);
  - on `PipelineMetrics`: `replication_lag(Duration)`, `transaction_buffer_bytes(u64)`,
    `binlog_lag(Option<Duration>)` (`None` removes the series), `snapshot_state(&[(String,
    SnapshotState)])`, `soft_dropped_columns(usize)`, `flushed(rows: u64, took: Duration)`,
    `committed(rows: u64, took: Duration)`, `commit_failed()`, `chunk_read(took: Duration)`;
    `totals` adds each `rows_read` difference; `remove` drops every new series.

The histograms of durations take `exponential_buckets(0.005, 2, 15)` (5 ms to 82 s), `flush_rows`
`exponential_buckets(1, 4, 11)` (1 to about a million rows). `snapshot_state` follows
`snapshot_rows_done`: a count per state, or with `per_table` a one-hot per table, the series of a
table no longer named removed. The controller fills `sink_type` from `sink.type`.

- [ ] **Step 1: Write the failing tests**

`metrics.rs` gains the new series' tests; `catalogue.rs` lists every mandatory sample with its
labels (a histogram by its `_count`) and checks a pipeline that set everything exports exactly
those, and that `per_table` adds `table` to the two snapshot series only.

`crates/data-avalanche-cluster/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/test_support.rs b/crates/data-avalanche-cluster/src/test_support.rs
index 11a2231..2e16c5a 100644
--- a/crates/data-avalanche-cluster/src/test_support.rs
+++ b/crates/data-avalanche-cluster/src/test_support.rs
@@ -214,6 +214,7 @@ pub(crate) fn metrics() -> (Metrics, Arc<PipelineMetrics>) {
             pipeline: "p".into(),
             source_host: "h".into(),
             source_db: "d".into(),
+            sink_type: "delta".into(),
         },
         false,
     );
```

`crates/data-avalanche-telemetry/tests/catalogue.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/catalogue.rs b/crates/data-avalanche-telemetry/tests/catalogue.rs
new file mode 100644
index 0000000..1df28c7
--- /dev/null
+++ b/crates/data-avalanche-telemetry/tests/catalogue.rs
@@ -0,0 +1,181 @@
+//! The mandatory catalogue (System-Overview §Observability catalogue): once a pipeline's handle
+//! has set each of its series, every mandatory sample is in `/metrics` with exactly its labels.
+
+use std::collections::BTreeSet;
+use std::time::Duration;
+
+use data_avalanche_telemetry::{
+    LeaseState, Metrics, PipelineLabels, PipelineStatus, RenewFailure, RowsRead, SnapshotState,
+    Totals,
+};
+
+const PIPELINE: &[&str] = &["pipeline", "source_host", "source_db"];
+
+/// Every mandatory sample, and the labels it carries beyond the pipeline's three (`None`: a node
+/// gauge, no labels at all). A histogram is named by its `_count` sample.
+const CATALOGUE: &[(&str, Option<&[&str]>)] = &[
+    // Replication
+    ("data_avalanche_replication_lag_seconds", Some(&[])),
+    ("data_avalanche_rows_read_total", Some(&["op"])),
+    ("data_avalanche_rows_written_total", Some(&[])),
+    ("data_avalanche_flush_duration_seconds_count", Some(&[])),
+    ("data_avalanche_flush_rows_count", Some(&[])),
+    ("data_avalanche_transaction_buffer_bytes", Some(&[])),
+    // Source health
+    ("data_avalanche_wal_lag_bytes", Some(&[])),
+    ("data_avalanche_binlog_lag_seconds", Some(&[])),
+    ("data_avalanche_source_reconnects_total", Some(&[])),
+    // Snapshot
+    ("data_avalanche_snapshot_rows_done", Some(&[])),
+    ("data_avalanche_snapshot_state", Some(&["state"])),
+    (
+        "data_avalanche_snapshot_chunk_duration_seconds_count",
+        Some(&[]),
+    ),
+    ("data_avalanche_reconcile_log_entries", Some(&[])),
+    ("data_avalanche_reconcile_log_keys", Some(&[])),
+    // Sink
+    (
+        "data_avalanche_sink_commit_duration_seconds_count",
+        Some(&["sink_type"]),
+    ),
+    (
+        "data_avalanche_sink_commit_failures_total",
+        Some(&["sink_type"]),
+    ),
+    ("data_avalanche_sink_merge_files_rewritten_total", Some(&[])),
+    ("data_avalanche_sink_merge_rows_copied_total", Some(&[])),
+    ("data_avalanche_sink_soft_dropped_columns", Some(&[])),
+    // Clustering
+    ("data_avalanche_lease_state", Some(&["state"])),
+    (
+        "data_avalanche_lease_renew_failures_total",
+        Some(&["class"]),
+    ),
+    (
+        "data_avalanche_lease_renew_latency_seconds_count",
+        Some(&[]),
+    ),
+    ("data_avalanche_cluster_members", None),
+    ("data_avalanche_owned_pipelines", None),
+    ("data_avalanche_ring_generation", None),
+    // Status
+    ("data_avalanche_pipeline_status", Some(&["status"])),
+    ("data_avalanche_halted_tables", Some(&[])),
+];
+
+/// The per-table series `sink.metrics.per_table` adds `table` to, with their other labels.
+const PER_TABLE: &[(&str, &[&str])] = &[
+    ("data_avalanche_snapshot_rows_done", &["table"]),
+    ("data_avalanche_snapshot_state", &["table", "state"]),
+];
+
+fn labels() -> PipelineLabels {
+    PipelineLabels {
+        pipeline: "orders".into(),
+        source_host: "db1".into(),
+        source_db: "shop".into(),
+        sink_type: "delta".into(),
+    }
+}
+
+/// Each sample name in the text, with the label names of each of its lines.
+fn samples(text: &str) -> Vec<(String, BTreeSet<String>)> {
+    text.lines()
+        .filter(|l| !l.starts_with('#'))
+        .map(|l| {
+            let name_end = l.find(['{', ' ']).unwrap_or(l.len());
+            let names = match l[name_end..].strip_prefix('{') {
+                Some(rest) => rest[..rest.find('}').expect("a closed label set")]
+                    .split(',')
+                    .map(|kv| kv.split('=').next().unwrap().to_owned())
+                    .collect(),
+                None => BTreeSet::new(),
+            };
+            (l[..name_end].to_owned(), names)
+        })
+        .collect()
+}
+
+/// Sets every series a pipeline and the node have.
+fn everything(per_table: bool) -> Metrics {
+    let m = Metrics::new();
+    let p = m.pipeline(labels(), per_table);
+    p.status(PipelineStatus::Running);
+    p.lease(LeaseState::Held);
+    p.renew_failed(RenewFailure::Transient);
+    p.renewed(Duration::from_millis(5));
+    p.wal_lag_bytes(Some(1));
+    p.binlog_lag(Some(Duration::from_secs(1)));
+    p.snapshot_rows_done(&[("public.a".to_owned(), 5)]);
+    p.snapshot_state(&[("public.a".to_owned(), SnapshotState::Running)]);
+    p.halted_tables(0);
+    p.reconcile_log(0, 0);
+    p.replication_lag(Duration::ZERO);
+    p.transaction_buffer_bytes(0);
+    p.soft_dropped_columns(0);
+    p.flushed(1, Duration::from_millis(1));
+    p.committed(1, Duration::from_millis(1));
+    p.commit_failed();
+    p.chunk_read(Duration::from_millis(1));
+    p.totals(Totals {
+        source_reconnects: 1,
+        sink_merge_files_rewritten: 1,
+        sink_merge_rows_copied: 1,
+        rows_read: RowsRead {
+            insert: 1,
+            ..RowsRead::default()
+        },
+    });
+    m.owned_by(1);
+    m.set_cluster_members(1);
+    m.set_ring_generation(1);
+    m
+}
+
+#[test]
+fn every_mandatory_series_is_exported_with_its_labels() {
+    let found = samples(&everything(false).encode());
+    for (name, extra) in CATALOGUE {
+        let want: BTreeSet<String> = match extra {
+            Some(extra) => PIPELINE
+                .iter()
+                .chain(*extra)
+                .map(|s| (*s).to_owned())
+                .collect(),
+            None => BTreeSet::new(),
+        };
+        let lines: Vec<_> = found.iter().filter(|(n, _)| n == name).collect();
+        assert!(!lines.is_empty(), "{name} isn't exported");
+        for (_, labels) in lines {
+            assert_eq!(labels, &want, "{name}");
+        }
+    }
+}
+
+#[test]
+fn per_table_adds_the_table_label_to_the_snapshot_series_only() {
+    let found = samples(&everything(true).encode());
+    for (name, extra) in PER_TABLE {
+        let want: BTreeSet<String> = PIPELINE
+            .iter()
+            .chain(*extra)
+            .map(|s| (*s).to_owned())
+            .collect();
+        let lines: Vec<_> = found.iter().filter(|(n, _)| n == name).collect();
+        assert!(!lines.is_empty(), "{name} isn't exported");
+        for (_, labels) in lines {
+            assert_eq!(labels, &want, "{name}");
+        }
+    }
+    let tabled: BTreeSet<&str> = found
+        .iter()
+        .filter(|(_, l)| l.contains("table"))
+        .map(|(n, _)| n.as_str())
+        .collect();
+    assert_eq!(
+        tabled,
+        PER_TABLE.iter().map(|(n, _)| *n).collect::<BTreeSet<_>>(),
+        "no other series takes the table label"
+    );
+}
```

`crates/data-avalanche-telemetry/tests/metrics.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/metrics.rs b/crates/data-avalanche-telemetry/tests/metrics.rs
index 724408f..b1dfa3c 100644
--- a/crates/data-avalanche-telemetry/tests/metrics.rs
+++ b/crates/data-avalanche-telemetry/tests/metrics.rs
@@ -1,7 +1,8 @@
 use std::time::Duration;
 
 use data_avalanche_telemetry::{
-    LeaseState, Metrics, PipelineLabels, PipelineStatus, RenewFailure, Totals,
+    LeaseState, Metrics, PipelineLabels, PipelineStatus, RenewFailure, RowsRead, SnapshotState,
+    Totals,
 };
 
 const L: &str = r#"pipeline="orders",source_host="db1",source_db="shop""#;
@@ -11,6 +12,7 @@ fn labels() -> PipelineLabels {
         pipeline: "orders".into(),
         source_host: "db1".into(),
         source_db: "shop".into(),
+        sink_type: "delta".into(),
     }
 }
 
@@ -83,6 +85,12 @@ fn counters_keep_counting_across_workers() {
         source_reconnects: n,
         sink_merge_files_rewritten: 10 * n,
         sink_merge_rows_copied: 100 * n,
+        rows_read: RowsRead {
+            insert: n,
+            update: 2 * n,
+            delete: 3 * n,
+            snapshot: 5 * n,
+        },
     };
     p.totals(totals(2));
     p.totals(totals(3));
@@ -105,6 +113,15 @@ fn counters_keep_counting_across_workers() {
             "data_avalanche_sink_merge_rows_copied_total{{{L}}} 400"
         )]
     );
+    assert_eq!(
+        samples(&text, "data_avalanche_rows_read_total"),
+        [
+            format!(r#"data_avalanche_rows_read_total{{{L},op="delete"}} 12"#),
+            format!(r#"data_avalanche_rows_read_total{{{L},op="insert"}} 4"#),
+            format!(r#"data_avalanche_rows_read_total{{{L},op="snapshot"}} 20"#),
+            format!(r#"data_avalanche_rows_read_total{{{L},op="update"}} 8"#),
+        ]
+    );
 }
 
 #[test]
@@ -145,8 +162,21 @@ fn a_pipeline_the_node_no_longer_owns_loses_its_series_but_its_renew_failures()
     p.reconcile_log(3, 9);
     p.totals(Totals {
         source_reconnects: 1,
+        rows_read: RowsRead {
+            insert: 1,
+            ..RowsRead::default()
+        },
         ..Totals::default()
     });
+    p.replication_lag(Duration::from_secs(1));
+    p.binlog_lag(Some(Duration::from_secs(1)));
+    p.transaction_buffer_bytes(1);
+    p.soft_dropped_columns(1);
+    p.snapshot_state(&[("public.a".to_owned(), SnapshotState::Running)]);
+    p.flushed(1, Duration::from_millis(1));
+    p.committed(1, Duration::from_millis(1));
+    p.commit_failed();
+    p.chunk_read(Duration::from_millis(1));
     m.owned_by(1);
     m.set_cluster_members(1);
     m.set_ring_generation(3);
@@ -199,6 +229,85 @@ fn the_gauges_read_what_was_set_last() {
     assert!(!m.encode().contains("data_avalanche_wal_lag_bytes{"));
 }
 
+#[test]
+fn the_lag_gauges_are_seconds_and_a_binlog_lag_unknown_has_no_series() {
+    let m = Metrics::new();
+    let p = m.pipeline(labels(), false);
+    p.replication_lag(Duration::from_millis(1500));
+    p.binlog_lag(Some(Duration::from_secs(7)));
+    p.transaction_buffer_bytes(2048);
+    p.soft_dropped_columns(2);
+    let text = m.encode();
+    for line in [
+        format!("data_avalanche_replication_lag_seconds{{{L}}} 1.5"),
+        format!("data_avalanche_binlog_lag_seconds{{{L}}} 7.0"),
+        format!("data_avalanche_transaction_buffer_bytes{{{L}}} 2048"),
+        format!("data_avalanche_sink_soft_dropped_columns{{{L}}} 2"),
+    ] {
+        assert!(text.lines().any(|l| l == line), "{line}\n{text}");
+    }
+    p.binlog_lag(None);
+    assert!(!m.encode().contains("data_avalanche_binlog_lag_seconds{"));
+}
+
+/// A flush and a commit are observed when they happen: the histograms count each one, the
+/// commit's series carry the sink type, and a failed attempt counts once.
+#[test]
+fn flushes_and_commits_are_timed_and_counted() {
+    let m = Metrics::new();
+    let p = m.pipeline(labels(), false);
+    p.flushed(40, Duration::from_millis(30));
+    p.flushed(2, Duration::from_millis(10));
+    p.committed(25, Duration::from_millis(20));
+    p.committed(15, Duration::from_millis(20));
+    p.commit_failed();
+    p.chunk_read(Duration::from_millis(8));
+    let text = m.encode();
+    let s = r#"sink_type="delta""#;
+    for line in [
+        format!("data_avalanche_flush_duration_seconds_count{{{L}}} 2"),
+        format!("data_avalanche_flush_rows_count{{{L}}} 2"),
+        format!("data_avalanche_flush_rows_sum{{{L}}} 42.0"),
+        format!("data_avalanche_rows_written_total{{{L}}} 40"),
+        format!("data_avalanche_sink_commit_duration_seconds_count{{{L},{s}}} 2"),
+        format!("data_avalanche_sink_commit_failures_total{{{L},{s}}} 1"),
+        format!("data_avalanche_snapshot_chunk_duration_seconds_count{{{L}}} 1"),
+    ] {
+        assert!(text.lines().any(|l| l == line), "{line}\n{text}");
+    }
+}
+
+#[test]
+fn snapshot_states_are_counted_unless_per_table() {
+    let m = Metrics::new();
+    let tables = [
+        ("public.a".to_owned(), SnapshotState::Running),
+        ("public.b".to_owned(), SnapshotState::Waiting),
+        ("public.c".to_owned(), SnapshotState::Running),
+    ];
+    m.pipeline(labels(), false).snapshot_state(&tables);
+    assert_eq!(
+        samples(&m.encode(), "data_avalanche_snapshot_state"),
+        [
+            format!(r#"data_avalanche_snapshot_state{{{L},state="running"}} 2"#),
+            format!(r#"data_avalanche_snapshot_state{{{L},state="waiting"}} 1"#),
+        ]
+    );
+
+    let m = Metrics::new();
+    let p = m.pipeline(labels(), true);
+    p.snapshot_state(&tables);
+    p.snapshot_state(&tables[1..2]);
+    assert_eq!(
+        samples(&m.encode(), "data_avalanche_snapshot_state"),
+        [
+            format!(r#"data_avalanche_snapshot_state{{{L},table="public.b",state="running"}} 0"#),
+            format!(r#"data_avalanche_snapshot_state{{{L},table="public.b",state="waiting"}} 1"#),
+        ],
+        "a finished snapshot loses its series"
+    );
+}
+
 /// A renamed pipeline (config §12.3) loses every series under its old name, the renew failures
 /// too; what is set next carries the new name. The same labels again change nothing.
 #[test]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-telemetry --test metrics --test catalogue`

Expected: FAIL to compile: `PipelineMetrics` has no method `replication_lag`, `transaction_buffer_bytes`,
`binlog_lag`, `snapshot_state`, `soft_dropped_columns`, `flushed`, `committed`, `commit_failed` or
`chunk_read` (E0599); `Totals` has no field `rows_read`, `PipelineLabels` none `sink_type` (E0560);
`RowsRead` and `SnapshotState` are unresolved imports (E0432).

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 725153c..39d1ac7 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -305,6 +305,7 @@ fn labels(p: &Pipeline) -> PipelineLabels {
         pipeline: p.name.clone(),
         source_host: p.source.host.clone(),
         source_db: p.source.db.clone(),
+        sink_type: p.sink.kind.as_str().to_owned(),
     }
 }
 
@@ -1184,6 +1185,7 @@ impl PipelineController {
             source_reconnects: st.reconnects,
             sink_merge_files_rewritten: sink.files_rewritten,
             sink_merge_rows_copied: sink.rows_copied,
+            ..Totals::default()
         });
         self.status.send_modify(|s| s.worker = Some(st));
     }
```

`crates/data-avalanche-telemetry/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/src/lib.rs b/crates/data-avalanche-telemetry/src/lib.rs
index 68c6f4f..53f71d7 100644
--- a/crates/data-avalanche-telemetry/src/lib.rs
+++ b/crates/data-avalanche-telemetry/src/lib.rs
@@ -1,12 +1,13 @@
 //! The process's metrics (System-Overview §Observability catalogue): one Prometheus registry, the
-//! node's gauges, and a handle per pipeline that sets that pipeline's series. M6b exports what the
-//! control plane and the worker know so far; M10 completes the catalogue.
+//! node's gauges, and a handle per pipeline that sets that pipeline's series.
 //!
 //! Every pipeline series carries `{pipeline, source_host, source_db}`; a `table` label is added
-//! only with `sink.metrics.per_table`, and only to `snapshot_rows_done`. Counter families get the
-//! OpenMetrics `_total` suffix on their samples (`data_avalanche_source_reconnects_total`).
+//! only with `sink.metrics.per_table`, and only to `snapshot_rows_done` and `snapshot_state`. The
+//! sink's commit series add `sink_type`. Counter families get the OpenMetrics `_total` suffix on
+//! their samples (`data_avalanche_source_reconnects_total`).
 
 use std::collections::BTreeSet;
+use std::sync::atomic::AtomicU64;
 use std::sync::{Arc, Mutex, PoisonError};
 use std::time::Duration;
 
@@ -22,6 +23,9 @@ type Labels = Vec<(&'static str, String)>;
 
 type HistogramFamily = Family<Labels, Histogram, fn() -> Histogram>;
 
+/// A gauge of seconds.
+type Seconds = Gauge<f64, AtomicU64>;
+
 /// The OpenMetrics text content type `/metrics` answers with.
 pub const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";
 
@@ -94,12 +98,57 @@ impl RenewFailure {
     }
 }
 
-/// The labels every series of a pipeline carries.
+/// `data_avalanche_rows_read_total{op}`: a change's operation, a snapshot's row included.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum RowOp {
+    Insert,
+    Update,
+    Delete,
+    Snapshot,
+}
+
+impl RowOp {
+    const ALL: [Self; 4] = [Self::Insert, Self::Update, Self::Delete, Self::Snapshot];
+
+    fn label(self) -> &'static str {
+        match self {
+            Self::Insert => "insert",
+            Self::Update => "update",
+            Self::Delete => "delete",
+            Self::Snapshot => "snapshot",
+        }
+    }
+}
+
+/// `data_avalanche_snapshot_state{state}`: where a running table snapshot is.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum SnapshotState {
+    /// It reads its chunks, or waits for its turn (`snapshot_parallelism`).
+    Running,
+    /// It reads nothing until something else ends: transactions in flight at the session's start
+    /// that write it, or a schema change a rescan found.
+    Waiting,
+}
+
+impl SnapshotState {
+    const ALL: [Self; 2] = [Self::Running, Self::Waiting];
+
+    fn label(self) -> &'static str {
+        match self {
+            Self::Running => "running",
+            Self::Waiting => "waiting",
+        }
+    }
+}
+
+/// The labels every series of a pipeline carries, and the sink type its commit series add.
 #[derive(Debug, Clone, PartialEq, Eq)]
 pub struct PipelineLabels {
     pub pipeline: String,
     pub source_host: String,
     pub source_db: String,
+    /// `sink.type` (`delta`): the `sink_type` label of the commit series.
+    pub sink_type: String,
 }
 
 impl PipelineLabels {
@@ -112,6 +161,30 @@ impl PipelineLabels {
         labels.extend(extra.iter().map(|(k, v)| (*k, (*v).to_owned())));
         labels
     }
+
+    fn sink(&self) -> Labels {
+        self.with(&[("sink_type", &self.sink_type)])
+    }
+}
+
+/// Rows read by operation.
+#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
+pub struct RowsRead {
+    pub insert: u64,
+    pub update: u64,
+    pub delete: u64,
+    pub snapshot: u64,
+}
+
+impl RowsRead {
+    fn get(&self, op: RowOp) -> u64 {
+        match op {
+            RowOp::Insert => self.insert,
+            RowOp::Update => self.update,
+            RowOp::Delete => self.delete,
+            RowOp::Snapshot => self.snapshot,
+        }
+    }
 }
 
 /// Monotonic totals a worker reports since it started. A new worker starts them at zero again;
@@ -121,6 +194,7 @@ pub struct Totals {
     pub source_reconnects: u64,
     pub sink_merge_files_rewritten: u64,
     pub sink_merge_rows_copied: u64,
+    pub rows_read: RowsRead,
 }
 
 #[derive(Debug)]
@@ -141,6 +215,18 @@ struct Families {
     reconcile_log_keys: Family<Labels, Gauge>,
     sink_merge_files_rewritten: Family<Labels, Counter>,
     sink_merge_rows_copied: Family<Labels, Counter>,
+    replication_lag: Family<Labels, Seconds>,
+    rows_read: Family<Labels, Counter>,
+    rows_written: Family<Labels, Counter>,
+    flush_duration: HistogramFamily,
+    flush_rows: HistogramFamily,
+    transaction_buffer_bytes: Family<Labels, Gauge>,
+    binlog_lag: Family<Labels, Seconds>,
+    snapshot_state: Family<Labels, Gauge>,
+    snapshot_chunk_duration: HistogramFamily,
+    sink_commit_duration: HistogramFamily,
+    sink_commit_failures: Family<Labels, Counter>,
+    sink_soft_dropped_columns: Family<Labels, Gauge>,
 }
 
 fn renew_latency() -> Histogram {
@@ -148,6 +234,16 @@ fn renew_latency() -> Histogram {
     Histogram::new(exponential_buckets(0.005, 2.0, 12))
 }
 
+/// A flush, a commit, a chunk read: 5 ms … ~82 s.
+fn duration() -> Histogram {
+    Histogram::new(exponential_buckets(0.005, 2.0, 15))
+}
+
+/// A flush's rows: 1 … ~1 M.
+fn rows() -> Histogram {
+    Histogram::new(exponential_buckets(1.0, 4.0, 11))
+}
+
 /// The process's registry; cheap to clone.
 #[derive(Debug, Clone)]
 pub struct Metrics {
@@ -180,6 +276,18 @@ impl Metrics {
             reconcile_log_keys: Family::default(),
             sink_merge_files_rewritten: Family::default(),
             sink_merge_rows_copied: Family::default(),
+            replication_lag: Family::default(),
+            rows_read: Family::default(),
+            rows_written: Family::default(),
+            flush_duration: Family::new_with_constructor(duration),
+            flush_rows: Family::new_with_constructor(rows),
+            transaction_buffer_bytes: Family::default(),
+            binlog_lag: Family::default(),
+            snapshot_state: Family::default(),
+            snapshot_chunk_duration: Family::new_with_constructor(duration),
+            sink_commit_duration: Family::new_with_constructor(duration),
+            sink_commit_failures: Family::default(),
+            sink_soft_dropped_columns: Family::default(),
         };
         let f = &families;
         let mut r = Registry::with_prefix("data_avalanche");
@@ -263,6 +371,62 @@ impl Metrics {
             "Rows a MERGE rewrote unchanged because they share a file with a changed row",
             f.sink_merge_rows_copied.clone(),
         );
+        r.register(
+            "replication_lag_seconds",
+            "Age of the oldest change read and not yet in the sink; 0 when none waits",
+            f.replication_lag.clone(),
+        );
+        r.register(
+            "rows_read",
+            "Rows read from the source: the stream's changes by operation, and snapshot rows",
+            f.rows_read.clone(),
+        );
+        r.register(
+            "rows_written",
+            "Rows the sink's commits wrote (upserts and deletes)",
+            f.rows_written.clone(),
+        );
+        r.register(
+            "flush_duration_seconds",
+            "How long a flush took, from its hand-off to the writer until it was durable",
+            f.flush_duration.clone(),
+        );
+        r.register("flush_rows", "Rows a flush carried", f.flush_rows.clone());
+        r.register(
+            "transaction_buffer_bytes",
+            "Bytes the accumulator buffers and hasn't handed to the writer yet",
+            f.transaction_buffer_bytes.clone(),
+        );
+        r.register(
+            "binlog_lag_seconds",
+            "How far the binlog stream reads behind the server's newest event; 0 once at it",
+            f.binlog_lag.clone(),
+        );
+        r.register(
+            "snapshot_state",
+            "Running table snapshots, by state",
+            f.snapshot_state.clone(),
+        );
+        r.register(
+            "snapshot_chunk_duration_seconds",
+            "How long a snapshot chunk's read took",
+            f.snapshot_chunk_duration.clone(),
+        );
+        r.register(
+            "sink_commit_duration_seconds",
+            "How long a table's commit to the sink took",
+            f.sink_commit_duration.clone(),
+        );
+        r.register(
+            "sink_commit_failures",
+            "Commits to the sink that failed: each retried attempt, and each refusal",
+            f.sink_commit_failures.clone(),
+        );
+        r.register(
+            "sink_soft_dropped_columns",
+            "Sink columns the source no longer has: kept, written NULL",
+            f.sink_soft_dropped_columns.clone(),
+        );
         Self {
             registry: Arc::new(r),
             families: Arc::new(families),
@@ -310,7 +474,10 @@ fn gauge(v: u64) -> i64 {
 #[derive(Debug, Default)]
 struct Seen {
     totals: Totals,
+    /// The tables `snapshot_rows_done` has a series of (with `per_table`).
     tables: BTreeSet<String>,
+    /// The tables `snapshot_state` has series of (with `per_table`).
+    state_tables: BTreeSet<String>,
 }
 
 /// One pipeline's series. The controller that owns the pipeline sets them; [`Self::remove`]
@@ -454,6 +621,111 @@ impl PipelineMetrics {
             .set(gauge(keys as u64));
     }
 
+    /// The age of the oldest change read and not yet durable in the sink (zero when none waits).
+    pub fn replication_lag(&self, lag: Duration) {
+        self.families
+            .replication_lag
+            .get_or_create(&self.base())
+            .set(lag.as_secs_f64());
+    }
+
+    pub fn transaction_buffer_bytes(&self, bytes: u64) {
+        self.families
+            .transaction_buffer_bytes
+            .get_or_create(&self.base())
+            .set(gauge(bytes));
+    }
+
+    /// `None` drops the series: the source isn't a binlog, or its read failed.
+    pub fn binlog_lag(&self, lag: Option<Duration>) {
+        match lag {
+            Some(l) => {
+                self.families
+                    .binlog_lag
+                    .get_or_create(&self.base())
+                    .set(l.as_secs_f64());
+            }
+            None => {
+                self.families.binlog_lag.remove(&self.base());
+            }
+        }
+    }
+
+    /// The running snapshots' states: with `per_table`, each table's series is 1 on its state; else
+    /// each state counts its tables. A table whose snapshot ended loses its series.
+    pub fn snapshot_state(&self, tables: &[(String, SnapshotState)]) {
+        let family = &self.families.snapshot_state;
+        if !self.per_table() {
+            for s in SnapshotState::ALL {
+                let n = tables.iter().filter(|(_, t)| *t == s).count();
+                family
+                    .get_or_create(&self.labels().with(&[("state", s.label())]))
+                    .set(gauge(n as u64));
+            }
+            return;
+        }
+        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
+        let now: BTreeSet<String> = tables.iter().map(|(t, _)| t.clone()).collect();
+        for gone in seen.state_tables.difference(&now) {
+            for s in SnapshotState::ALL {
+                family.remove(&self.labels().with(&[("table", gone), ("state", s.label())]));
+            }
+        }
+        for (table, state) in tables {
+            for s in SnapshotState::ALL {
+                family
+                    .get_or_create(
+                        &self
+                            .labels()
+                            .with(&[("table", table), ("state", s.label())]),
+                    )
+                    .set(i64::from(s == *state));
+            }
+        }
+        seen.state_tables = now;
+    }
+
+    pub fn soft_dropped_columns(&self, n: usize) {
+        self.families
+            .sink_soft_dropped_columns
+            .get_or_create(&self.base())
+            .set(gauge(n as u64));
+    }
+
+    /// A flush became durable: its rows, and how long it took.
+    pub fn flushed(&self, rows: u64, took: Duration) {
+        let f = &self.families;
+        let base = self.base();
+        f.flush_rows.get_or_create(&base).observe(rows as f64);
+        f.flush_duration
+            .get_or_create(&base)
+            .observe(took.as_secs_f64());
+    }
+
+    /// A table's commit landed: the rows it wrote, and how long it took.
+    pub fn committed(&self, rows: u64, took: Duration) {
+        let f = &self.families;
+        f.rows_written.get_or_create(&self.base()).inc_by(rows);
+        f.sink_commit_duration
+            .get_or_create(&self.labels().sink())
+            .observe(took.as_secs_f64());
+    }
+
+    /// A commit attempt failed: retried, or refused.
+    pub fn commit_failed(&self) {
+        self.families
+            .sink_commit_failures
+            .get_or_create(&self.labels().sink())
+            .inc();
+    }
+
+    pub fn chunk_read(&self, took: Duration) {
+        self.families
+            .snapshot_chunk_duration
+            .get_or_create(&self.base())
+            .observe(took.as_secs_f64());
+    }
+
     /// The running worker's totals. A total below the last one seen is a new worker's: it counts
     /// from zero.
     pub fn totals(&self, now: Totals) {
@@ -480,6 +752,13 @@ impl PipelineMetrics {
             now.sink_merge_rows_copied,
             last.sink_merge_rows_copied,
         );
+        for op in RowOp::ALL {
+            let (now, last) = (now.rows_read.get(op), last.rows_read.get(op));
+            let by = if now >= last { now - last } else { now };
+            f.rows_read
+                .get_or_create(&self.labels().with(&[("op", op.label())]))
+                .inc_by(by);
+        }
         seen.totals = now;
     }
 
@@ -505,28 +784,60 @@ impl PipelineMetrics {
             f.lease_state
                 .remove(&self.labels().with(&[("state", s.label())]));
         }
-        f.lease_renew_latency.remove(&base);
+        for family in [
+            &f.lease_renew_latency,
+            &f.flush_duration,
+            &f.flush_rows,
+            &f.snapshot_chunk_duration,
+        ] {
+            family.remove(&base);
+        }
+        f.sink_commit_duration.remove(&self.labels().sink());
         for family in [
             &f.wal_lag_bytes,
             &f.halted_tables,
             &f.reconcile_log_entries,
             &f.reconcile_log_keys,
             &f.snapshot_rows_done,
+            &f.transaction_buffer_bytes,
+            &f.sink_soft_dropped_columns,
         ] {
             family.remove(&base);
         }
+        for family in [&f.replication_lag, &f.binlog_lag] {
+            family.remove(&base);
+        }
         for family in [
             &f.source_reconnects,
             &f.sink_merge_files_rewritten,
             &f.sink_merge_rows_copied,
+            &f.rows_written,
         ] {
             family.remove(&base);
         }
+        f.sink_commit_failures.remove(&self.labels().sink());
+        for op in RowOp::ALL {
+            f.rows_read
+                .remove(&self.labels().with(&[("op", op.label())]));
+        }
+        for s in SnapshotState::ALL {
+            f.snapshot_state
+                .remove(&self.labels().with(&[("state", s.label())]));
+        }
         let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
         for table in std::mem::take(&mut seen.tables) {
             f.snapshot_rows_done
                 .remove(&self.labels().with(&[("table", &table)]));
         }
+        for table in std::mem::take(&mut seen.state_tables) {
+            for s in SnapshotState::ALL {
+                f.snapshot_state.remove(
+                    &self
+                        .labels()
+                        .with(&[("table", &table), ("state", s.label())]),
+                );
+            }
+        }
         seen.totals = Totals::default();
     }
 }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-telemetry --test metrics --test catalogue`

Expected: PASS: `metrics` 12 tests, `catalogue` 2.

Run: `cargo test --workspace`

Expected: PASS (1294 tests in all).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`

Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-telemetry crates/data-avalanche-cluster
git commit -m "feat(telemetry): the catalogue's missing series — lag, rows read and written, flushes, the buffer, binlog lag, snapshot state and chunk time, commits and their failures, soft drops"
```

---

### Task 2: The worker's events (`data-avalanche-pipeline`)

**Files:**
- Create: `crates/data-avalanche-pipeline/src/observe.rs`
- Modify: `crates/data-avalanche-pipeline/src/{lib,writer,worker,session}.rs`, `crates/data-avalanche-pipeline/src/session/snapshot.rs`, `crates/data-avalanche-cluster/src/controller.rs`
- Test: `crates/data-avalanche-pipeline/src/test_support.rs`, the tests of `writer.rs` and `session/snapshot.rs`

**Interfaces:**
- Consumes: the writer's commit loop and `SinkCommitResult` (M6a), the snapshot's spawned
  chunk reads (M6a).
- Produces:
  - `pipeline::observe::Observer: Send + Sync + Debug` with `flushed(&self, rows: u64, took:
    Duration)`, `committed(&self, rows: u64, took: Duration)`, `commit_failed(&self)`,
    `chunk_read(&self, took: Duration)`, each a no-op by default; `NoObserver`; both re-exported;
  - `WorkerDeps.observer: Arc<dyn Observer>` (the controller passes `Arc::new(NoObserver)` until
    Task 5).

D-M10.1, D-M10.2. A job's rows are its change sets' `upsert_rows + delete_rows`. `committed` is
called on `Committed` only; `commit_failed` on every `Err` of an attempt, retried or not;
`flushed` once the job's checkpoint is written. `chunk_read` is called when a chunk read
succeeds, with the read's time.

- [ ] **Step 1: Write the failing tests**

`test_support.rs` gains `Ears`, an `Observer` that records what it hears (`Heard::{Flushed,
Committed, CommitFailed, ChunkRead}`), and the worlds take it.

`crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
index 9a5bab6..d325855 100644
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -671,7 +671,7 @@ mod tests {
 
     use super::*;
     use crate::session::Core;
-    use crate::test_support::{World, Yaml, row, t, wait_until};
+    use crate::test_support::{Ears, Heard, World, Yaml, row, t, wait_until};
     use crate::{Backoff, Command, Exit, Hooks, Phase, ReconcileLog, WorkerStatus};
 
     fn chunks_of(n: u64) -> Pipeline {
@@ -803,6 +803,28 @@ mod tests {
         );
     }
 
+    /// Each chunk read is reported, the last, empty one included.
+    #[tokio::test]
+    async fn each_chunk_read_is_reported() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        w.source.load(&t("a"), rows(1..=5));
+        w.request(&["a"]).await;
+        let (mut core, control, status) = w.core(&chunks_of(2), &["a"]);
+        let ears = Arc::new(Ears::default());
+        core.observer = ears.clone();
+        run(core, control, status, async |status| {
+            snapshot_done(&w, &status, "a").await;
+        })
+        .await;
+        let chunks = ears
+            .heard()
+            .into_iter()
+            .filter(|h| *h == Heard::ChunkRead)
+            .count();
+        assert_eq!(chunks, 4, "two, two, one, then the empty chunk");
+    }
+
     #[tokio::test]
     async fn a_stream_change_the_chunk_didnt_see_wins() {
         let w = World::new(&["a"]);
```

`crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
index 3cc80ef..1c7a6ef 100644
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -25,9 +25,46 @@ use crate::settings::Settings;
 use crate::status::Streak;
 use crate::{
     Accumulator, Backoff, Command, CommitGuard, FlushPolicy, HookAction, HookPoint, Hooks, Lineage,
-    NoHooks, Tuning, Unguarded, WorkerStatus,
+    NoHooks, NoObserver, Observer, Tuning, Unguarded, WorkerStatus,
 };
 
+/// What an [`Ears`] observer heard.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub(crate) enum Heard {
+    Flushed { rows: u64 },
+    Committed { rows: u64 },
+    CommitFailed,
+    ChunkRead,
+}
+
+/// An observer that keeps what it hears, in order.
+#[derive(Debug, Default)]
+pub(crate) struct Ears(Mutex<Vec<Heard>>);
+
+impl Ears {
+    pub fn heard(&self) -> Vec<Heard> {
+        self.0.lock().unwrap().clone()
+    }
+}
+
+impl Observer for Ears {
+    fn flushed(&self, rows: u64, _: Duration) {
+        self.0.lock().unwrap().push(Heard::Flushed { rows });
+    }
+
+    fn committed(&self, rows: u64, _: Duration) {
+        self.0.lock().unwrap().push(Heard::Committed { rows });
+    }
+
+    fn commit_failed(&self) {
+        self.0.lock().unwrap().push(Heard::CommitFailed);
+    }
+
+    fn chunk_read(&self, _: Duration) {
+        self.0.lock().unwrap().push(Heard::ChunkRead);
+    }
+}
+
 /// A PostgreSQL → Delta pipeline `p` reading `public.*`, with more `tables` keys (YAML lines
 /// indented six spaces), more `sink` and `postgres` keys (YAML flow entries).
 pub(crate) fn pipeline(tables: &str, sink: &str, postgres: &str) -> Pipeline {
@@ -428,6 +465,7 @@ impl World {
             owner_epoch: 1,
             guard: Arc::new(Unguarded),
             hooks: Arc::new(NoHooks),
+            observer: Arc::new(NoObserver),
             tuning: fast(),
             control: control_rx,
             status,
```

`crates/data-avalanche-pipeline/src/writer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index c5408cc..9ef59f9 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -290,7 +290,9 @@ mod tests {
 
     use super::*;
     use crate::NoHooks;
-    use crate::test_support::{CrashAt, FlakyStore, Switch, accumulator, docs, lsn, schema, txn};
+    use crate::test_support::{
+        CrashAt, Ears, FlakyStore, Heard, Switch, accumulator, docs, lsn, schema, txn,
+    };
 
     const FAST: Backoff = Backoff {
         first: Duration::from_millis(1),
@@ -305,6 +307,7 @@ mod tests {
         /// The checkpoint's document, and the server it is recorded with.
         checkpoints: Store,
         server: Option<ServerIdentity>,
+        ears: Arc<Ears>,
     }
 
     async fn rig(tables: &[&str]) -> Rig {
@@ -325,6 +328,7 @@ mod tests {
                 intent: None,
             },
             server: None,
+            ears: Arc::default(),
         }
     }
 
@@ -361,6 +365,7 @@ mod tests {
                 owner_epoch: 1,
                 guard,
                 hooks,
+                observer: self.ears.clone(),
                 backoff: FAST,
             };
             let (jobs_tx, jobs_rx) = mpsc::unbounded_channel();
@@ -551,6 +556,70 @@ mod tests {
         assert_eq!(rows(&r, "a"), 1);
     }
 
+    /// Each commit that landed reports its rows, then the flush reports them all once it is
+    /// durable. A set the sink already holds writes nothing: no commit is reported, the flush is.
+    #[tokio::test]
+    async fn a_flush_reports_each_commit_then_itself() {
+        let r = rig(&["a", "b"]).await;
+        let mut acc = accumulator(&["a", "b"], 100);
+        txn(
+            &mut acc,
+            1,
+            200,
+            &[("a", 1, "x"), ("a", 2, "x"), ("b", 1, "y")],
+        );
+        let f = acc.flush();
+        let again = job(crate::Flush {
+            change_sets: f.change_sets.clone(),
+            checkpoint: f.checkpoint.clone(),
+        });
+        r.run(vec![job(f), again], Arc::new(NoHooks)).await;
+        assert_eq!(
+            r.ears.heard(),
+            [
+                Heard::Committed { rows: 2 },
+                Heard::Committed { rows: 1 },
+                Heard::Flushed { rows: 3 },
+                Heard::Flushed { rows: 3 },
+            ]
+        );
+    }
+
+    /// Every failed attempt counts, the retried ones and the refusal.
+    #[tokio::test]
+    async fn every_failed_commit_attempt_is_reported() {
+        let r = rig(&["a", "b"]).await;
+        r.sink.fail(
+            SinkOp::Commit,
+            Some(&TableRef::new("public", "a")),
+            2,
+            || SinkError::Transient {
+                reason: "throttled".into(),
+            },
+        );
+        r.sink.fail(
+            SinkOp::Commit,
+            Some(&TableRef::new("public", "b")),
+            1,
+            || SinkError::Permanent {
+                reason: "column mismatch".into(),
+            },
+        );
+        let mut acc = accumulator(&["a", "b"], 100);
+        txn(&mut acc, 1, 200, &[("a", 1, "x"), ("b", 1, "y")]);
+        r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
+        assert_eq!(
+            r.ears.heard(),
+            [
+                Heard::CommitFailed,
+                Heard::CommitFailed,
+                Heard::Committed { rows: 1 },
+                Heard::CommitFailed,
+                Heard::Flushed { rows: 2 },
+            ]
+        );
+    }
+
     #[tokio::test]
     async fn a_transient_state_write_is_retried_while_degraded() {
         let r = rig(&["a"]).await;
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-pipeline --lib -- a_flush_reports_each_commit_then_itself every_failed_commit_attempt_is_reported each_chunk_read_is_reported`

Expected: FAIL to compile: unresolved imports `crate::NoObserver`, `crate::Observer` (E0432).

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 39d1ac7..7764f2b 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -15,8 +15,8 @@ use std::time::Duration;
 use data_avalanche_config::Pipeline;
 use data_avalanche_core::{Sink, SourceConnector, SourceError};
 use data_avalanche_pipeline::{
-    Backoff, Command, CommitGuard, Exit, Hooks, Phase, PipelineWorker, Recovery, Tuning,
-    WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
+    Backoff, Command, CommitGuard, Exit, Hooks, NoObserver, Phase, PipelineWorker, Recovery,
+    Tuning, WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
 };
 use data_avalanche_state::{
     ActualState, DesiredState, Holder, Leases, PipelineDocs, RequestsDoc, StatusDoc,
@@ -997,6 +997,7 @@ impl PipelineController {
             owner_epoch: run.owner_epoch,
             guard: run.guard.clone(),
             hooks: parts.hooks,
+            observer: Arc::new(NoObserver),
             reader_ended: Arc::clone(reader_ended),
         };
         let (worker, handle) =
```

`crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
index a1bca90..7fc708c 100644
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -15,6 +15,7 @@ mod decide;
 pub mod error;
 pub mod hooks;
 mod jobs;
+pub mod observe;
 pub mod policy;
 mod reconcile;
 mod schema_change;
@@ -33,6 +34,7 @@ pub use batch::{Lineage, MAX_BATCH_BYTES};
 pub use buffer::{CollapsedRow, RowKind, TableBuffer};
 pub use error::{AccumulatorError, Recovery, WorkerError};
 pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
+pub use observe::{NoObserver, Observer};
 pub use policy::{FlushPolicy, FlushReason, TxnLimits};
 pub use status::{Command, Exit, Phase, ReconcileLog, SnapshotProgress, Tuning, WorkerStatus};
 pub use worker::{PipelineWorker, WorkerDeps, WorkerHandle};
```

`crates/data-avalanche-pipeline/src/observe.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/observe.rs b/crates/data-avalanche-pipeline/src/observe.rs
new file mode 100644
index 0000000..dbd1355
--- /dev/null
+++ b/crates/data-avalanche-pipeline/src/observe.rs
@@ -0,0 +1,35 @@
+//! What the worker reports as it happens: the timings and the counts the metrics' histograms and
+//! event counters take (System-Overview §Observability catalogue). Its state (the lag, the
+//! buffer, the snapshots) is in [`crate::WorkerStatus`] instead, which the owner samples.
+
+use std::fmt::Debug;
+use std::time::Duration;
+
+/// The owner's ears; every method defaults to nothing, and none may block.
+pub trait Observer: Send + Sync + Debug {
+    /// A flush is durable: the rows of its change sets, and the time from when the writer took
+    /// it up until then.
+    fn flushed(&self, rows: u64, took: Duration) {
+        let _ = (rows, took);
+    }
+
+    /// A table's change set was committed: the rows it wrote, and how long the attempt that
+    /// landed took. A set the sink skipped as already covered wrote nothing and isn't reported.
+    fn committed(&self, rows: u64, took: Duration) {
+        let _ = (rows, took);
+    }
+
+    /// A commit attempt failed: a transient failure that is retried, or the sink's refusal.
+    fn commit_failed(&self) {}
+
+    /// A snapshot chunk was read from the source.
+    fn chunk_read(&self, took: Duration) {
+        let _ = took;
+    }
+}
+
+/// Hears nothing: a worker without an owner's metrics (tests).
+#[derive(Debug, Clone, Copy, Default)]
+pub struct NoObserver;
+
+impl Observer for NoObserver {}
```

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index fe7ac9f..e0dbd13 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -31,8 +31,8 @@ use crate::status::Streak;
 use crate::watchdog::Watchdog;
 use crate::writer::{self, Job, WriterCtx, WriterEvent};
 use crate::{
-    Accumulator, AccumulatorError, Command, CommitGuard, Exit, FlushReason, Hooks, Phase, Tuning,
-    WorkerError, WorkerStatus,
+    Accumulator, AccumulatorError, Command, CommitGuard, Exit, FlushReason, Hooks, Observer, Phase,
+    Tuning, WorkerError, WorkerStatus,
 };
 
 /// What every session of one worker shares.
@@ -44,6 +44,7 @@ pub(crate) struct Core {
     pub owner_epoch: u64,
     pub guard: Arc<dyn CommitGuard>,
     pub hooks: Arc<dyn Hooks>,
+    pub observer: Arc<dyn Observer>,
     pub tuning: Tuning,
     pub control: mpsc::Receiver<Command>,
     pub status: watch::Sender<WorkerStatus>,
@@ -310,6 +311,7 @@ impl<'c> Session<'c> {
             owner_epoch: core.owner_epoch,
             guard: core.guard.clone(),
             hooks: core.hooks.clone(),
+            observer: core.observer.clone(),
             backoff: core.tuning.sink_backoff,
         };
         let writer = tokio::spawn(writer::run(ctx, jobs_rx, events_tx));
```

`crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
index d325855..a614eec 100644
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -383,12 +383,17 @@ impl Session<'_> {
             self.core.writers_waited.remove(table);
             let ticket = self.snaps.reconciler.issue();
             let source = Arc::clone(&self.core.source);
+            let observer = Arc::clone(&self.core.observer);
             let after = snap.cursor.clone();
             let limit = self.core.settings.chunk_size;
             let table = table.clone();
             self.snaps.reads.spawn(async move {
                 let read_at_micros = micros_now();
+                let started = Instant::now();
                 let result = source.read_chunk(&schema, after.as_ref(), limit).await;
+                if result.is_ok() {
+                    observer.chunk_read(started.elapsed());
+                }
                 Read {
                     table,
                     ticket,
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index d76970c..d6dd823 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -25,8 +25,8 @@ use crate::session::{Core, End, Session};
 use crate::settings::Settings;
 use crate::status::Streak;
 use crate::{
-    Command, CommitGuard, Exit, HookAction, HookPoint, Hooks, NoHooks, Phase, Tuning, Unguarded,
-    WorkerError, WorkerStatus,
+    Command, CommitGuard, Exit, HookAction, HookPoint, Hooks, NoHooks, NoObserver, Observer, Phase,
+    Tuning, Unguarded, WorkerError, WorkerStatus,
 };
 
 /// What a worker runs against.
@@ -40,6 +40,8 @@ pub struct WorkerDeps {
     pub owner_epoch: u64,
     pub guard: Arc<dyn CommitGuard>,
     pub hooks: Arc<dyn Hooks>,
+    /// Hears the flushes, the commits and the chunk reads (the owner's metrics).
+    pub observer: Arc<dyn Observer>,
     /// Whether this lease term already ended the stream position's other reader (DA-SRC-28):
     /// every worker run of the term shares it.
     pub reader_ended: Arc<AtomicBool>,
@@ -63,6 +65,7 @@ impl WorkerDeps {
             owner_epoch: 1,
             guard: Arc::new(Unguarded),
             hooks: Arc::new(NoHooks),
+            observer: Arc::new(NoObserver),
             reader_ended: Arc::new(AtomicBool::new(false)),
         }
     }
@@ -102,6 +105,7 @@ impl PipelineWorker {
             owner_epoch: deps.owner_epoch,
             guard: deps.guard,
             hooks: deps.hooks,
+            observer: deps.observer,
             tuning,
             control: control_rx,
             status: status_tx,
```

`crates/data-avalanche-pipeline/src/writer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index 9ef59f9..69b9373 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -10,6 +10,7 @@
 
 use std::collections::BTreeSet;
 use std::sync::Arc;
+use std::time::Instant;
 
 use data_avalanche_core::{
     Checkpoint, CommitMarker, CommitOutcome, HaltReason, ServerIdentity, Sink, SinkError,
@@ -22,7 +23,7 @@ use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
 
 use crate::backoff::Backoff;
 use crate::checkpoint::{Doc, Store};
-use crate::{CommitGuard, HookAction, HookPoint, Hooks, WorkerError};
+use crate::{CommitGuard, HookAction, HookPoint, Hooks, Observer, WorkerError};
 
 /// One flush and what becomes durable with it.
 #[derive(Debug)]
@@ -68,6 +69,7 @@ pub(crate) struct WriterCtx {
     pub owner_epoch: u64,
     pub guard: Arc<dyn CommitGuard>,
     pub hooks: Arc<dyn Hooks>,
+    pub observer: Arc<dyn Observer>,
     pub backoff: Backoff,
 }
 
@@ -135,6 +137,8 @@ impl Writer {
     }
 
     async fn job(&mut self, job: Job) -> Result<(), WorkerError> {
+        let started = Instant::now();
+        let rows = job.change_sets.iter().map(rows_of).sum();
         // A table the sink refused keeps that halt, its reason and its `at`: a halt the session
         // sends after it (one whose flush the sink refused, final review M1) would claim the
         // sink holds the refused rows.
@@ -182,6 +186,7 @@ impl Writer {
             self.state(Write::Schema(schema)).await?;
         }
         drop(job.permit);
+        self.ctx.observer.flushed(rows, started.elapsed());
         let _ = self.events.send(WriterEvent::Durable(job.checkpoint));
         Ok(())
     }
@@ -194,13 +199,25 @@ impl Writer {
             committed_position: cs.position.end.clone(),
             pipeline_id: self.ctx.pipeline_id.clone(),
         };
+        let rows = rows_of(&cs);
+        let observer = Arc::clone(&self.ctx.observer);
         let mut attempt = 0;
         loop {
             if !self.ctx.guard.may_commit() {
                 return Err(WorkerError::LeaseLost);
             }
-            match self.ctx.sink.commit(cs.clone(), marker.clone()).await {
-                Ok(CommitOutcome::Committed { .. } | CommitOutcome::SkippedCovered) => {
+            let at = Instant::now();
+            let outcome = self.ctx.sink.commit(cs.clone(), marker.clone()).await;
+            if outcome.is_err() {
+                observer.commit_failed();
+            }
+            match outcome {
+                Ok(CommitOutcome::Committed { .. }) => {
+                    observer.committed(rows, at.elapsed());
+                    self.degraded(None);
+                    return Ok(None);
+                }
+                Ok(CommitOutcome::SkippedCovered) => {
                     self.degraded(None);
                     return Ok(None);
                 }
@@ -268,6 +285,11 @@ impl Writer {
     }
 }
 
+/// A change set's rows: its upserts and its deletes.
+fn rows_of(cs: &TableChangeSet) -> u64 {
+    (cs.upsert_rows() + cs.delete_rows()) as u64
+}
+
 /// One state document a job writes.
 #[derive(Clone, Copy)]
 enum Write<'a> {
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-pipeline --lib -- a_flush_reports_each_commit_then_itself every_failed_commit_attempt_is_reported each_chunk_read_is_reported`

Expected: PASS, 3 tests.

Run: `cargo test --workspace`

Expected: PASS (1297 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline crates/data-avalanche-cluster
git commit -m "feat(pipeline): the worker reports its flushes, commits, failed commit attempts and chunk reads to an observer"
```

---

### Task 3: What the status shows (`data-avalanche-pipeline`)

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/{status,accumulator,schema_change,session,worker,lib}.rs`, `crates/data-avalanche-pipeline/src/session/snapshot.rs`
- Test: the tests of `session.rs`, `session/snapshot.rs` and `worker.rs`; `test_support.rs`

**Interfaces:**
- Consumes: `StreamItem::Begin { commit_ts_micros }` (M5, M7a), the writer's `Durable` events
  (M6a), `Evolver::evolve` (M6a), `SinkTableState` (M3).
- Produces:
  - `WorkerStatus.unsynced_since_micros: Option<i64>` (D-M10.3), `.rows_read: RowsRead`,
    `.buffer_bytes: u64`, `.soft_dropped: BTreeMap<TableRef, usize>`;
    `SnapshotProgress.waiting: bool` (D-M10.6);
  - `pipeline::RowsRead { insert, update, delete, snapshot: u64 }` with `count(op: Op, n: u64)`;
  - `Accumulator::buffered_bytes(&self) -> usize`;
  - `schema_change::soft_dropped(sink: &Schema, source: &TableSchema) -> usize` (crate-private).

The status changes only when one of its values does (`send_if_modified`), at each read, each
durable flush and each heartbeat. Phase 0 counts the soft drops from each table's sink state, or
re-reads it after a baseline evolved the table.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index e0dbd13..76140be 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -1283,7 +1283,7 @@ mod tests {
     use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
 
     use super::*;
-    use crate::test_support::{World, Yaml, pipeline, row, t, wait_until};
+    use crate::test_support::{World, Yaml, pipeline, row, schema, t, wait_until};
 
     /// Runs a session over `w` until `drive` returns, then drains it.
     async fn session<F: Future<Output = ()>>(
@@ -1354,6 +1354,84 @@ mod tests {
         assert_eq!((st.read, st.checkpoint), (Some(end.clone()), Some(end)));
     }
 
+    /// The fake source's first commit time.
+    const FIRST_COMMIT: i64 = 1_700_000_000_000_000;
+
+    /// The lag starts at the oldest transaction read and not yet durable: it stays at the first
+    /// one while the policy holds the flush back, and clears once the flush is durable. The rows
+    /// read count by operation, and the buffer holds the unflushed changes' bytes until the
+    /// flush.
+    #[tokio::test]
+    async fn the_status_shows_the_oldest_unsynced_change_the_rows_read_and_the_buffer() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let held = pipeline("", "flush: { seconds: 600 }", "");
+        let (end, _) = session(&w, &held, &["a"], async |status| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            w.source.commit(
+                FakeTxn::new()
+                    .update(&t("a"), row(1, "y"))
+                    .insert(&t("a"), row(2, "x")),
+            );
+            w.source.commit(
+                FakeTxn::new().delete(&t("a"), Row(vec![("id".into(), ColumnValue::Int64(2))])),
+            );
+            let end = w.source.wal_end();
+            wait_until("everything is read", || {
+                status.borrow().read.as_ref() == Some(&end)
+            })
+            .await;
+            let st = status.borrow().clone();
+            assert_eq!(st.unsynced_since_micros, Some(FIRST_COMMIT));
+            assert_eq!(
+                st.rows_read,
+                RowsRead {
+                    insert: 2,
+                    update: 1,
+                    delete: 1,
+                    snapshot: 0
+                }
+            );
+            assert!(st.buffer_bytes > 0, "{st:?}");
+        })
+        .await;
+        end.unwrap();
+    }
+
+    /// Once the flush is durable, nothing waits: no lag, no buffer.
+    #[tokio::test]
+    async fn a_durable_flush_clears_the_lag_and_the_buffer() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let (end, st) = session(&w, &flush_each(), &["a"], async |status| {
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+            durable(&status, w.source.wal_end()).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!((st.unsynced_since_micros, st.buffer_bytes), (None, 0));
+        assert_eq!(st.rows_read.insert, 1);
+    }
+
+    /// A column the source drops stays in the sink: the evolve's soft-drop is counted.
+    #[tokio::test]
+    async fn a_dropped_column_is_counted_soft_dropped() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let (end, st) = session(&w, &flush_each(), &["a"], async |status| {
+            w.source.alter(&t("a"), with_note("a"));
+            w.source.commit(FakeTxn::new().insert(&t("a"), noted(1)));
+            durable(&status, w.source.wal_end()).await;
+            assert_eq!(status.borrow().soft_dropped.get(&t("a")), Some(&0));
+            w.source.alter(&t("a"), schema("a"));
+            w.source.commit(FakeTxn::new().insert(&t("a"), row(2, "x")));
+            durable(&status, w.source.wal_end()).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(st.soft_dropped.get(&t("a")), Some(&1), "{st:?}");
+    }
+
     #[tokio::test]
     async fn a_drain_flushes_what_the_policy_held_back() {
         let w = World::new(&["a"]);
```

`crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
index a614eec..a89cf2d 100644
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -818,7 +818,7 @@ mod tests {
         let (mut core, control, status) = w.core(&chunks_of(2), &["a"]);
         let ears = Arc::new(Ears::default());
         core.observer = ears.clone();
-        run(core, control, status, async |status| {
+        let (_, st) = run(core, control, status, async |status| {
             snapshot_done(&w, &status, "a").await;
         })
         .await;
@@ -828,6 +828,7 @@ mod tests {
             .filter(|h| *h == Heard::ChunkRead)
             .count();
         assert_eq!(chunks, 4, "two, two, one, then the empty chunk");
+        assert_eq!(st.rows_read.snapshot, 5);
     }
 
     #[tokio::test]
@@ -923,7 +924,16 @@ mod tests {
             .await;
             rechecked(&w, before).await;
             assert_eq!(w.source.reads(&t("a")), 0, "the writer is still in flight");
+            assert!(status.borrow().snapshots[&t("a")].waiting, "it waits");
             w.source.release(xid);
+            wait_until("it reads", || {
+                status
+                    .borrow()
+                    .snapshots
+                    .get(&t("a"))
+                    .is_none_or(|p| !p.waiting)
+            })
+            .await;
             snapshot_done(&w, &status, "a").await;
         })
         .await;
@@ -1737,8 +1747,13 @@ mod tests {
             status.borrow().snapshots
         );
         core.update(|st| {
-            st.snapshots
-                .insert(t("gone"), SnapshotProgress { rows_done: 7 });
+            st.snapshots.insert(
+                t("gone"),
+                SnapshotProgress {
+                    rows_done: 7,
+                    ..SnapshotProgress::default()
+                },
+            );
         });
         let _next = Session::open(&mut core).await.unwrap().unwrap();
         assert!(
```

`crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
index 1c7a6ef..b925d7b 100644
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -25,7 +25,7 @@ use crate::settings::Settings;
 use crate::status::Streak;
 use crate::{
     Accumulator, Backoff, Command, CommitGuard, FlushPolicy, HookAction, HookPoint, Hooks, Lineage,
-    NoHooks, NoObserver, Observer, Tuning, Unguarded, WorkerStatus,
+    NoHooks, NoObserver, Observer, RowsRead, Tuning, Unguarded, WorkerStatus,
 };
 
 /// What an [`Ears`] observer heard.
@@ -483,6 +483,7 @@ impl World {
             read_failing: BTreeMap::new(),
             server: None,
             pause: false,
+            rows_read: RowsRead::default(),
         };
         (core, control, status_rx)
     }
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index d6dd823..63c85e6 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -836,7 +836,8 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
 mod tests {
     use data_avalanche_core::position::Lsn;
     use data_avalanche_core::{
-        ColumnSchema, ColumnValue, ColumnValueType as V, Row, ServerIdentity, SourcePosition,
+        ColumnSchema, ColumnValue, ColumnValueType as V, Row, SchemaChange, SchemaDelta,
+        ServerIdentity, SourcePosition,
     };
     use data_avalanche_state::{PositionDoc, SlotDoc};
     use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
@@ -925,6 +926,29 @@ mod tests {
         assert!(st.warnings.is_empty(), "{:?}", st.warnings);
     }
 
+    /// The start counts the sink's columns the source table doesn't have: soft-dropped, kept and
+    /// written NULL.
+    #[tokio::test]
+    async fn the_start_counts_the_sink_columns_the_source_lacks() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        let gone = SchemaDelta {
+            table: t("a"),
+            changes: vec![SchemaChange::AddColumn(ColumnSchema::new(
+                "gone",
+                V::Text,
+                "text",
+            ))],
+        };
+        w.sink.evolve(&t("a"), &gone, 1).await.unwrap();
+        let (end, st) = run(&w, &mode("mode: initial"), async |status| {
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert_eq!(st.soft_dropped, BTreeMap::from([(t("a"), 1)]));
+    }
+
     #[tokio::test]
     async fn a_restart_resumes_from_its_checkpoint() {
         let w = World::new(&["a"]);
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-pipeline --lib -- the_status_shows_the_oldest_unsynced_change a_durable_flush_clears_the_lag a_dropped_column_is_counted_soft_dropped the_start_counts_the_sink_columns a_table_waits_for_the_writers_in_flight_at_the_start`

Expected: FAIL to compile: `WorkerStatus` has no field `rows_read`, `unsynced_since_micros`,
`soft_dropped` or `buffer_bytes`, `SnapshotProgress` none `waiting` (E0609); `Core` has no field
`rows_read` (E0560); `crate::RowsRead` is unresolved (E0432, E0422).

- [ ] **Step 3: Implement**

`crates/data-avalanche-pipeline/src/accumulator.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/accumulator.rs b/crates/data-avalanche-pipeline/src/accumulator.rs
index 979ad3b..1e38c9b 100644
--- a/crates/data-avalanche-pipeline/src/accumulator.rs
+++ b/crates/data-avalanche-pipeline/src/accumulator.rs
@@ -95,6 +95,14 @@ impl Accumulator {
         self.tables.values().map(TableBuffer::open_bytes).sum()
     }
 
+    /// Bytes buffered and not flushed yet: the committed transactions' and the open one's.
+    pub fn buffered_bytes(&self) -> usize {
+        self.tables
+            .values()
+            .map(|b| b.committed_bytes() + b.open_bytes())
+            .sum()
+    }
+
     pub fn checkpoint(&self) -> &SourcePosition {
         &self.checkpoint
     }
```

`crates/data-avalanche-pipeline/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/lib.rs b/crates/data-avalanche-pipeline/src/lib.rs
index 7fc708c..a162864 100644
--- a/crates/data-avalanche-pipeline/src/lib.rs
+++ b/crates/data-avalanche-pipeline/src/lib.rs
@@ -36,5 +36,7 @@ pub use error::{AccumulatorError, Recovery, WorkerError};
 pub use hooks::{CommitGuard, HookAction, HookPoint, Hooks, NoHooks, Unguarded};
 pub use observe::{NoObserver, Observer};
 pub use policy::{FlushPolicy, FlushReason, TxnLimits};
-pub use status::{Command, Exit, Phase, ReconcileLog, SnapshotProgress, Tuning, WorkerStatus};
+pub use status::{
+    Command, Exit, Phase, ReconcileLog, RowsRead, SnapshotProgress, Tuning, WorkerStatus,
+};
 pub use worker::{PipelineWorker, WorkerDeps, WorkerHandle};
```

`crates/data-avalanche-pipeline/src/schema_change.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/schema_change.rs b/crates/data-avalanche-pipeline/src/schema_change.rs
index 0ddc44b..f0cce6d 100644
--- a/crates/data-avalanche-pipeline/src/schema_change.rs
+++ b/crates/data-avalanche-pipeline/src/schema_change.rs
@@ -2,14 +2,30 @@
 //! the sink's evolve. A stream session runs them at a signal's position, with a flush in between.
 //! Phase 0 runs them to baseline a table whose stream position was lost.
 
+use arrow_schema::Schema;
 use data_avalanche_core::{
-    DeltaClass, EvolveOutcome, HaltReason, SchemaChange, SchemaDelta, SchemaHint, Sink, SinkError,
-    SourceConnector, SourceError, TableRef, TableSchema, classify,
+    DeltaClass, EvolveOutcome, HaltReason, INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, SchemaChange,
+    SchemaDelta, SchemaHint, Sink, SinkError, SourceConnector, SourceError, TableRef, TableSchema,
+    classify,
 };
 use tokio::sync::watch;
 
 use crate::{CommitGuard, Tuning, WorkerError, WorkerStatus};
 
+/// The sink table's columns `source` doesn't have, the lineage and internal columns aside:
+/// soft-dropped, kept and written NULL (`data_avalanche_sink_soft_dropped_columns`).
+pub(crate) fn soft_dropped(sink: &Schema, source: &TableSchema) -> usize {
+    sink.fields()
+        .iter()
+        .map(|f| f.name().as_str())
+        .filter(|n| {
+            !LINEAGE_COLUMNS.contains(n)
+                && !n.starts_with(INTERNAL_COLUMN_PREFIX)
+                && source.column(n).is_none()
+        })
+        .count()
+}
+
 /// A change to apply: the table's new schema, and the changes the sink must evolve.
 #[derive(Debug)]
 pub(crate) struct Planned {
```

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 76140be..6e22d65 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -16,8 +16,8 @@ use std::time::Instant;
 use data_avalanche_config::Pipeline;
 use data_avalanche_core::{
     CdcEvent, ChangeStream, ColumnValue, HaltReason, Op, RescanReport, RetainedLog, Row,
-    SchemaHint, ServerIdentity, Sink, SourceConnector, SourceError, SourcePosition, SourceSeq,
-    StreamItem, TableRef, TableSchema, TransactionBoundary, identifier_values,
+    SchemaHint, ServerIdentity, Sink, SinkTableState, SourceConnector, SourceError, SourcePosition,
+    SourceSeq, StreamItem, TableRef, TableSchema, TransactionBoundary, identifier_values,
 };
 use data_avalanche_state::{PipelineDocs, TableHalt};
 use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
@@ -25,14 +25,14 @@ use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
 use tokio::task::{JoinError, JoinHandle, JoinSet};
 
 use self::snapshot::Snapshots;
-use crate::schema_change::{Evolver, Planned};
+use crate::schema_change::{Evolver, Planned, soft_dropped};
 use crate::settings::Settings;
 use crate::status::Streak;
 use crate::watchdog::Watchdog;
 use crate::writer::{self, Job, WriterCtx, WriterEvent};
 use crate::{
     Accumulator, AccumulatorError, Command, CommitGuard, Exit, FlushReason, Hooks, Observer, Phase,
-    Tuning, WorkerError, WorkerStatus,
+    RowsRead, Tuning, WorkerError, WorkerStatus,
 };
 
 /// What every session of one worker shares.
@@ -91,6 +91,8 @@ pub(crate) struct Core {
     /// and reads nothing more until [`Command::Resume`]. It outlives a session, so a reconnect
     /// while paused stays paused.
     pub pause: bool,
+    /// [`WorkerStatus::rows_read`], counted by every session of the worker.
+    pub rows_read: RowsRead,
 }
 
 impl Core {
@@ -114,6 +116,20 @@ impl Core {
         self.source.reconfigure(p);
     }
 
+    /// The sink's soft-dropped columns of `now`'s table into the status, after an evolve; a
+    /// failed read leaves the count as it was.
+    pub async fn count_soft_dropped(&self, now: &TableSchema) {
+        if let Ok(SinkTableState {
+            schema: Some(sink), ..
+        }) = self.sink.table_state(now.table()).await
+        {
+            let n = soft_dropped(&sink, now);
+            self.update(|st| {
+                st.soft_dropped.insert(now.table().clone(), n);
+            });
+        }
+    }
+
     pub fn evolver(&self) -> Evolver<'_> {
         Evolver {
             source: &*self.source,
@@ -217,6 +233,13 @@ pub(crate) struct Session<'c> {
     watchdog: Watchdog,
     /// A reload changed the heartbeat or the rescan interval: the loop re-arms its timers.
     retimed: bool,
+    /// The open transaction's commit time (microseconds since the Unix epoch).
+    open_since: Option<i64>,
+    /// The commit time of the oldest transaction read since the last flush handed on.
+    unflushed_since: Option<i64>,
+    /// The jobs handed on and not durable yet, each with the commit time of its oldest
+    /// transaction, in job order.
+    unsynced: VecDeque<(u64, i64)>,
 }
 
 impl<'c> Session<'c> {
@@ -353,6 +376,9 @@ impl<'c> Session<'c> {
             retained_read: JoinSet::new(),
             watchdog: Watchdog::default(),
             retimed: false,
+            open_since: None,
+            unflushed_since: None,
+            unsynced: VecDeque::new(),
         }))
     }
 
@@ -580,6 +606,13 @@ impl<'c> Session<'c> {
             Some(WriterEvent::Durable(cp)) => {
                 self.durable += 1;
                 debug_assert!(self.durable <= self.handed, "one Durable per job");
+                while self
+                    .unsynced
+                    .front()
+                    .is_some_and(|(job, _)| *job <= self.durable)
+                {
+                    self.unsynced.pop_front();
+                }
                 for (table, dump) in self.snaps.durable(self.durable) {
                     self.core.update(|st| {
                         st.snapshots.remove(&table);
@@ -595,6 +628,7 @@ impl<'c> Session<'c> {
                     return Ok(Some(End::Source(e)));
                 }
                 self.core.update(|st| st.checkpoint = Some(cp));
+                self.report_progress();
                 Ok(None)
             }
         }
@@ -603,7 +637,12 @@ impl<'c> Session<'c> {
     async fn item(&mut self, item: StreamItem) -> Result<Option<End>, WorkerError> {
         let now = Instant::now();
         match item {
-            StreamItem::Begin { xid, .. } => {
+            StreamItem::Begin {
+                xid,
+                commit_ts_micros,
+            } => {
+                self.open_since = Some(commit_ts_micros);
+                self.unflushed_since.get_or_insert(commit_ts_micros);
                 self.acc.begin(xid)?;
                 self.snaps.begin();
                 self.requery.clear();
@@ -614,6 +653,7 @@ impl<'c> Session<'c> {
                     self.note_unchanged(&e);
                 }
                 self.last_seq = Some(e.change_seq);
+                self.core.rows_read.count(e.op, 1);
                 if let Some(schema) = self.schemas.get(&e.table) {
                     self.snaps.touch(schema, &e);
                 }
@@ -643,6 +683,7 @@ impl<'c> Session<'c> {
                     return Ok(Some(end));
                 }
                 self.acc.commit(&b, now)?;
+                self.open_since = None;
                 self.snaps.applied(b.xid);
                 let at = b.commit_pos;
                 self.pending_schemas
@@ -979,6 +1020,7 @@ impl<'c> Session<'c> {
             self.retained_read
                 .spawn(async move { source.retained().await });
         }
+        self.report_progress();
         let why = self.watchdog.beat(self.reads_stream())?;
         Some(End::Source(SourceError::Transient(why)))
     }
@@ -1056,6 +1098,26 @@ impl<'c> Session<'c> {
     fn read(&self) {
         let frontier = self.acc.frontier().clone();
         self.core.update(|st| st.read = Some(frontier));
+        self.report_progress();
+    }
+
+    /// The lag's start, the rows read and the buffered bytes into the status, when they changed.
+    fn report_progress(&self) {
+        let since = self.unsynced.front().map(|(_, ts)| *ts);
+        let since = match (since, self.unflushed_since) {
+            (Some(a), Some(b)) => Some(a.min(b)),
+            (a, b) => a.or(b),
+        };
+        let rows_read = self.core.rows_read;
+        let bytes = self.acc.buffered_bytes() as u64;
+        self.core.status.send_if_modified(|st| {
+            let changed = (st.unsynced_since_micros, st.rows_read, st.buffer_bytes)
+                != (since, rows_read, bytes);
+            st.unsynced_since_micros = since;
+            st.rows_read = rows_read;
+            st.buffer_bytes = bytes;
+            changed
+        });
     }
 
     /// The reconciler log's size into the status, when it changed (I3).
@@ -1158,6 +1220,11 @@ impl<'c> Session<'c> {
             .min(Semaphore::MAX_PERMITS as u64);
         let need = u32::try_from((rows as u64).min(cap)).unwrap_or(u32::MAX);
         self.handed += 1;
+        if let Some(since) = self.unflushed_since.take() {
+            self.unsynced.push_back((self.handed, since));
+        }
+        // A slice of a large transaction leaves the rest of it to wait.
+        self.unflushed_since = self.open_since;
         let job = Job {
             change_sets,
             checkpoint: checkpoint.clone(),
@@ -1251,6 +1318,7 @@ impl<'c> Session<'c> {
                     return Ok(None);
                 }
             }
+            self.core.count_soft_dropped(&now).await;
         }
         let extra = self.core.settings.delete_extra(&now);
         loop {
```

`crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
index a89cf2d..9b2445f 100644
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -176,7 +176,13 @@ impl Snapshots {
     pub(super) fn progress(&self) -> impl Iterator<Item = (TableRef, SnapshotProgress)> + '_ {
         self.tables.iter().map(|(t, s)| {
             let rows_done = s.doc.cursor.as_ref().map_or(0, |c| c.rows_done);
-            (t.clone(), SnapshotProgress { rows_done })
+            (
+                t.clone(),
+                SnapshotProgress {
+                    rows_done,
+                    waiting: false,
+                },
+            )
         })
     }
 
@@ -339,6 +345,24 @@ impl Session<'_> {
             return;
         }
         let now = Instant::now();
+        // Each table's writers in flight at the session's start, by xid.
+        let writers: BTreeMap<TableRef, Vec<String>> = self
+            .snaps
+            .tables
+            .keys()
+            .filter_map(|table| {
+                let id = self.read_schema(table)?.source_id()?;
+                let xids: Vec<String> = self
+                    .snaps
+                    .in_flight
+                    .iter()
+                    .filter(|(_, tables)| tables.contains(&id))
+                    .map(|(xid, _)| xid.to_string())
+                    .collect();
+                (!xids.is_empty()).then(|| (table.clone(), xids))
+            })
+            .collect();
+        self.report_waiting(&writers);
         let mut reading = self.snaps.tables.values().filter(|s| s.reading).count();
         let mut wait_for_writers = false;
         for (table, snap) in &mut self.snaps.tables {
@@ -361,15 +385,7 @@ impl Session<'_> {
             else {
                 continue;
             };
-            let writers: Vec<String> = schema.source_id().map_or_else(Vec::new, |id| {
-                self.snaps
-                    .in_flight
-                    .iter()
-                    .filter(|(_, tables)| tables.contains(&id))
-                    .map(|(xid, _)| xid.to_string())
-                    .collect()
-            });
-            if !writers.is_empty() {
+            if let Some(writers) = writers.get(table) {
                 wait_for_writers = true;
                 if self.core.writers_waited.insert(table.clone()) {
                     let warning = format!(
@@ -411,6 +427,21 @@ impl Session<'_> {
         }
     }
 
+    /// Marks the running snapshots that wait in the status: a held table, and one with `writers`
+    /// in flight. Changes the status only when a mark changes.
+    fn report_waiting(&self, writers: &BTreeMap<TableRef, Vec<String>>) {
+        let held = &self.snaps.held;
+        self.core.status.send_if_modified(|st| {
+            let mut changed = false;
+            for (table, p) in &mut st.snapshots {
+                let waiting = held.contains(table) || writers.contains_key(table);
+                changed |= p.waiting != waiting;
+                p.waiting = waiting;
+            }
+            changed
+        });
+    }
+
     /// A chunk read finished: it is emitted at the next boundary.
     pub(super) async fn chunk_read(&mut self, read: Read) -> Result<Option<End>, WorkerError> {
         self.snaps.ready.push_back(read);
@@ -506,6 +537,7 @@ impl Session<'_> {
             keyed.push((key, row));
         }
         let read = chunk.rows.len() as u64;
+        self.core.rows_read.count(Op::Snapshot, read);
         let kept =
             self.snaps
                 .reconciler
@@ -538,8 +570,7 @@ impl Session<'_> {
         }
         self.snaps.docs.insert(table.clone(), snap.doc.clone());
         self.core.update(|st| {
-            st.snapshots
-                .insert(table.clone(), SnapshotProgress { rows_done });
+            st.snapshots.entry(table.clone()).or_default().rows_done = rows_done;
         });
         if kept.is_empty() {
             self.flush()?;
```

`crates/data-avalanche-pipeline/src/status.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
index 6c02a3c..e1a0d27 100644
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -5,7 +5,7 @@ use std::collections::BTreeMap;
 use std::time::Duration;
 
 use data_avalanche_config::Pipeline;
-use data_avalanche_core::{HaltReason, SourcePosition, TableRef};
+use data_avalanche_core::{HaltReason, Op, SourcePosition, TableRef};
 use tokio::sync::watch;
 
 use crate::Backoff;
@@ -63,6 +63,32 @@ pub enum Phase {
 pub struct SnapshotProgress {
     /// Rows emitted, this run and the runs before it.
     pub rows_done: u64,
+    /// It reads nothing until something else ends: a transaction in flight at the session's
+    /// start that writes the table, or a schema change a rescan found
+    /// (`data_avalanche_snapshot_state{state="waiting"}`).
+    pub waiting: bool,
+}
+
+/// Rows read since the worker started (`data_avalanche_rows_read_total{op}`): the stream's
+/// changes by operation, and the snapshot chunks' rows.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
+pub struct RowsRead {
+    pub insert: u64,
+    pub update: u64,
+    pub delete: u64,
+    pub snapshot: u64,
+}
+
+impl RowsRead {
+    pub fn count(&mut self, op: Op, n: u64) {
+        let total = match op {
+            Op::Insert => &mut self.insert,
+            Op::Update => &mut self.update,
+            Op::Delete => &mut self.delete,
+            Op::Snapshot => &mut self.snapshot,
+        };
+        *total += n;
+    }
 }
 
 /// The snapshot reconciler's log (DA-SNAP-9): the stream transactions kept to check chunks
@@ -100,6 +126,20 @@ pub struct WorkerStatus {
     pub warnings_dropped: u64,
     /// Stream sessions reopened after a source failure (`data_avalanche_source_reconnects_total`).
     pub reconnects: u64,
+    /// The source commit time, in microseconds since the Unix epoch, of the oldest transaction
+    /// read and not yet durable in the sink; `None` when none waits. Its age is
+    /// `data_avalanche_replication_lag_seconds`. Snapshot rows have no commit time and don't
+    /// count. As of the last stream boundary, durable flush or heartbeat.
+    pub unsynced_since_micros: Option<i64>,
+    pub rows_read: RowsRead,
+    /// Bytes the accumulator buffers and hasn't handed to the writer yet, the open transaction's
+    /// included (`data_avalanche_transaction_buffer_bytes`). As of the last stream boundary or
+    /// heartbeat.
+    pub buffer_bytes: u64,
+    /// Per table, the sink's columns the source table no longer has: soft-dropped, kept and
+    /// written NULL (`data_avalanche_sink_soft_dropped_columns`). Counted at the start and after
+    /// each evolve.
+    pub soft_dropped: BTreeMap<TableRef, usize>,
 }
 
 /// The warnings a status keeps.
```

`crates/data-avalanche-pipeline/src/worker.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 63c85e6..7aa72f3 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -20,13 +20,13 @@ use tokio::sync::{mpsc, watch};
 
 use crate::decide::{Decision, Facts, Plan, decide};
 use crate::jobs::Jobs;
-use crate::schema_change::Planned;
+use crate::schema_change::{Planned, soft_dropped};
 use crate::session::{Core, End, Session};
 use crate::settings::Settings;
 use crate::status::Streak;
 use crate::{
     Command, CommitGuard, Exit, HookAction, HookPoint, Hooks, NoHooks, NoObserver, Observer, Phase,
-    Tuning, Unguarded, WorkerError, WorkerStatus,
+    RowsRead, Tuning, Unguarded, WorkerError, WorkerStatus,
 };
 
 /// What a worker runs against.
@@ -123,6 +123,7 @@ impl PipelineWorker {
             read_failing: BTreeMap::new(),
             server: None,
             pause: false,
+            rows_read: RowsRead::default(),
         };
         let reader_ended = deps.reader_ended;
         (
@@ -587,6 +588,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
             }
         };
         let mut schema = now.clone();
+        let mut evolved = false;
         let baseline = matches!(plan, Plan::Fresh(_)) || cleared.contains(table);
         // A cleared table's change is planned under its source id now: one created again is
         // the table the catalog has.
@@ -615,6 +617,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
                                     continue;
                                 }
                             }
+                            evolved = true;
                         }
                         schema = now;
                     }
@@ -649,6 +652,17 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
                 Err(e) => return Err(e),
             }
         }
+        // The sink's soft-dropped columns: the table as read, or as the baseline's evolve left it.
+        match (&state.schema, evolved) {
+            (_, true) => core.count_soft_dropped(&schema).await,
+            (Some(sink_schema), false) => {
+                let n = soft_dropped(sink_schema, &schema);
+                core.update(|st| {
+                    st.soft_dropped.insert(table.clone(), n);
+                });
+            }
+            (None, false) => {}
+        }
         if !tracked_before || baseline {
             schemas.push(schema);
         }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the command of Step 2.

Expected: PASS, 5 tests.

Run: `cargo test --workspace`

Expected: PASS (1301 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline
git commit -m "feat(pipeline): the status carries the oldest unsynced change, the rows read, the buffer, a snapshot that waits and the soft-dropped columns"
```

---

### Task 4: The binlog lag (`core`, `source-mysql`, `source-pg`, `testkit`, `pipeline`)

**Files:**
- Modify: `crates/data-avalanche-core/src/source.rs`, `crates/data-avalanche-source-mysql/src/{connector,start,stream}.rs`, `crates/data-avalanche-source-pg/src/slot.rs`, `crates/data-avalanche-pipeline/src/{session,status}.rs`
- Test: `crates/data-avalanche-source-mysql/tests/mysql/start.rs`, `crates/data-avalanche-source-pg/tests/pg/slot.rs`, `crates/data-avalanche-testkit/src/fake_source.rs`, the tests of `start.rs`

**Interfaces:**
- Consumes: `SourceConnector::retained` (M6a), MySQL's `position_now` and the stream's
  heartbeat events (M7a).
- Produces:
  - `RetainedLog { bytes: Option<u64>, streamed: bool, behind: Option<Duration> }` (D-M10.4):
    PostgreSQL `bytes: Some(..)`, `behind: None`; MySQL `bytes: None`, `behind` as below;
  - `WorkerStatus.source_behind: Option<Duration>`; `WorkerStatus.retained_bytes` stays, and is
    `None` on MySQL;
  - `FakeSource::set_behind(Option<Duration>)`.

MySQL's stream records what it read last (`ReadAt { position, commit_micros, idle }`): at the
open, at each commit, at each server heartbeat (idle), and clears it at the close. `retained()`
asks the server's head (`position_now`) and computes `behind(read, head, now_micros)`: `None`
without a read, `0` when idle or at the head, else now minus the last commit, at least 0.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 6e22d65..b0fc767 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -2094,8 +2094,10 @@ mod tests {
         let s = Session::open(&mut core).await.unwrap().unwrap();
         let driver = async {
             w.source.set_retained(Some(1024));
+            w.source.set_behind(Some(Duration::from_secs(3)));
             wait_until("the lag is reported", || {
-                status.borrow().retained_bytes == Some(1024)
+                let st = status.borrow();
+                st.retained_bytes == Some(1024) && st.source_behind == Some(Duration::from_secs(3))
             })
             .await;
             w.source.set_retained(Some(65 << 20));
```

`crates/data-avalanche-source-mysql/src/start.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/start.rs b/crates/data-avalanche-source-mysql/src/start.rs
index 0cbfdd8..14ec5aa 100644
--- a/crates/data-avalanche-source-mysql/src/start.rs
+++ b/crates/data-avalanche-source-mysql/src/start.rs
@@ -174,6 +174,43 @@ mod tests {
         SourcePosition::BinlogFile(BinlogFilePos::new("binlog.000003", 154).unwrap())
     }
 
+    fn at(pos: u64, commit_micros: Option<i64>, idle: bool) -> ReadAt {
+        ReadAt {
+            position: SourcePosition::BinlogFile(BinlogFilePos::new("binlog.000003", pos).unwrap()),
+            commit_micros,
+            idle,
+        }
+    }
+
+    /// At the server's write position, or idle by a heartbeat, the stream is 0 behind; short of
+    /// it, the last transaction it read is as old as the lag. Behind with nothing read, or
+    /// without the server's position, the lag is unknown.
+    #[test]
+    fn the_binlog_lag_is_zero_at_the_head_else_the_last_transaction_s_age() {
+        let head = SourcePosition::BinlogFile(BinlogFilePos::new("binlog.000003", 900).unwrap());
+        let now = 10_000_000;
+        assert_eq!(
+            behind(&at(900, Some(1_000_000), false), Some(&head), now),
+            Some(Duration::ZERO)
+        );
+        assert_eq!(
+            behind(&at(500, Some(1_000_000), false), Some(&head), now),
+            Some(Duration::from_secs(9))
+        );
+        assert_eq!(
+            behind(&at(500, Some(1_000_000), true), None, now),
+            Some(Duration::ZERO),
+            "a heartbeat came after it"
+        );
+        assert_eq!(behind(&at(500, None, false), Some(&head), now), None);
+        assert_eq!(behind(&at(500, Some(1_000_000), false), None, now), None);
+        assert_eq!(
+            behind(&at(500, Some(now + 5), false), Some(&head), now),
+            Some(Duration::ZERO),
+            "a clock behind the server's reads no lag"
+        );
+    }
+
     #[test]
     fn a_checkpoint_in_another_dialect_is_named() {
         assert_eq!(dialect_change(Mode::File, &file()), None);
```

`crates/data-avalanche-source-mysql/tests/mysql/start.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/tests/mysql/start.rs b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
index 8100eb0..48b93bf 100644
--- a/crates/data-avalanche-source-mysql/tests/mysql/start.rs
+++ b/crates/data-avalanche-source-mysql/tests/mysql/start.rs
@@ -4,7 +4,7 @@
 use std::time::Duration;
 
 use data_avalanche_core::{
-    CdcEvent, ChangeStream, Checkpoint, ColumnValue, RetainedLog, ServerIdentity, SnapshotReason,
+    CdcEvent, ChangeStream, Checkpoint, ColumnValue, ServerIdentity, SnapshotReason,
     SourceConnector, SourceError, SourcePosition, StartPlan, StreamItem,
 };
 use data_avalanche_source_mysql::{Flavour, Mode, MySqlConnector, MySqlStream};
@@ -179,7 +179,7 @@ async fn a_purged_file_follows_the_purge_action() {
 async fn watch(c: &MySqlConnector, streamed: bool) {
     for _ in 0..50 {
         let r = c.retained().await.expect("retained");
-        if r == Some(RetainedLog { bytes: 0, streamed }) {
+        if r.is_some_and(|r| r.streamed == streamed && r.bytes.is_none()) {
             return;
         }
         tokio::time::sleep(Duration::from_millis(100)).await;
@@ -238,6 +238,41 @@ async fn the_watch_finds_the_dump_by_its_connection() {
 
 const FILE: &str = ", replication_type: binlog";
 
+/// The heartbeat's lag: behind the server's write position, the age of the last transaction the
+/// stream read; none without a stream. A MySQL source keeps no log for the pipeline: no bytes.
+/// Other tests write the same server meanwhile, so the stream is never sure to be at its head:
+/// the zero at the head is the unit test's.
+#[tokio::test]
+async fn the_binlog_lag_is_the_age_of_the_last_commit_read() {
+    require_it!();
+    let (n, mut a) = fresh("start_lag", "CREATE TABLE t (id INT PRIMARY KEY)").await;
+    let c = connector(&pipeline(&n, &["t"], "", ""));
+    let lag = async |c: &MySqlConnector| c.retained().await.expect("retained").expect("a log");
+    assert_eq!(lag(&c).await.behind, None, "no stream");
+    let tables = c.resolve_tables().await.unwrap().tables;
+    let from = c.checkpoint_now().await.unwrap();
+    let mut s = c.open_stream(&from, &tables).await.expect("stream");
+    a.query_drop("INSERT INTO t VALUES (1)").await.unwrap();
+    to_commit(&mut s).await;
+    tokio::time::sleep(Duration::from_secs(2)).await;
+    a.query_drop("INSERT INTO t VALUES (2)").await.unwrap();
+    let r = lag(&c).await;
+    assert!(
+        r.behind.is_some_and(|b| b >= Duration::from_secs(1)),
+        "the commit read 2 s ago is the lag: {r:?}"
+    );
+    assert_eq!(r.bytes, None);
+    to_commit(&mut s).await;
+    let r = lag(&c).await;
+    assert!(
+        r.behind.is_some_and(|b| b < Duration::from_secs(2)),
+        "the commit just read is the lag: {r:?}"
+    );
+    let _ = Box::new(s).close().await;
+    assert_eq!(lag(&c).await.behind, None, "closed");
+    cleanup(&n).await;
+}
+
 /// The changes up to the stream's next commit, and the commit's position, within 20 s.
 async fn to_commit(s: &mut MySqlStream) -> (Vec<CdcEvent>, SourcePosition) {
     tokio::time::timeout(Duration::from_secs(20), async {
```

`crates/data-avalanche-source-pg/tests/pg/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/tests/pg/slot.rs b/crates/data-avalanche-source-pg/tests/pg/slot.rs
index 0cd0834..0cdab2f 100644
--- a/crates/data-avalanche-source-pg/tests/pg/slot.rs
+++ b/crates/data-avalanche-source-pg/tests/pg/slot.rs
@@ -539,8 +539,12 @@ async fn reset_teardown_and_retained_wal() {
     .await
     .unwrap();
     let retained = c.retained().await.unwrap().unwrap();
-    assert!(retained.bytes > 0, "WAL written since the slot's position");
+    assert!(
+        retained.bytes.is_some_and(|b| b > 0),
+        "WAL written since the slot's position"
+    );
     assert!(!retained.streamed, "no stream reads the slot");
+    assert_eq!(retained.behind, None, "the binlog lag is MySQL's");
 
     let SourcePosition::Pg(fresh_at) = c.reset_position().await.unwrap() else {
         panic!("a PostgreSQL position")
```

`crates/data-avalanche-testkit/src/fake_source.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index 1c8dbce..50a435e 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -28,6 +28,7 @@
 
 use std::collections::{BTreeMap, BTreeSet, VecDeque};
 use std::sync::{Arc, Mutex, MutexGuard};
+use std::time::Duration;
 
 use async_trait::async_trait;
 use data_avalanche_core::position::Lsn;
@@ -236,6 +237,8 @@ struct Db {
     slot_lost: bool,
     added: Vec<TableRef>,
     retained: Option<Option<u64>>,
+    /// [`FakeSource::set_behind`].
+    behind: Option<Duration>,
     /// [`FakeSource::detach_stream`].
     detached: bool,
     /// [`FakeSource::stall_retained`].
@@ -298,6 +301,7 @@ impl FakeSource {
             slot_lost: false,
             added: vec![],
             retained: None,
+            behind: None,
             detached: false,
             retained_stalled: false,
             faults: vec![],
@@ -460,6 +464,11 @@ impl FakeSource {
         self.db().retained = Some(bytes);
     }
 
+    /// How far behind `retained` says the stream reads from now on (by default, it doesn't say).
+    pub fn set_behind(&self, behind: Option<Duration>) {
+        self.db().behind = behind;
+    }
+
     /// `retained` reports that no stream reads the slot, whatever the open streams do: a stream
     /// the source dropped without the client hearing of it (DA-SRC-16).
     pub fn detach_stream(&self) {
@@ -1011,8 +1020,9 @@ impl SourceConnector for FakeSource {
             None => db.slot.map(|s| db.wal_end.0.saturating_sub(s.0)),
         };
         Ok(bytes.map(|bytes| RetainedLog {
-            bytes,
+            bytes: Some(bytes),
             streamed: !db.detached,
+            behind: db.behind,
         }))
     }
 
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-source-mysql --lib start::`

Expected: FAIL to compile: `RetainedLog` has no field `behind` (E0560), and `bytes` isn't an `Option`
yet (E0308).

- [ ] **Step 3: Implement**

`crates/data-avalanche-core/src/source.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/source.rs b/crates/data-avalanche-core/src/source.rs
index bc97b48..b911590 100644
--- a/crates/data-avalanche-core/src/source.rs
+++ b/crates/data-avalanche-core/src/source.rs
@@ -3,6 +3,7 @@
 
 use std::any::Any;
 use std::fmt;
+use std::time::Duration;
 
 use async_trait::async_trait;
 use serde::{Deserialize, Serialize};
@@ -296,11 +297,17 @@ pub enum StreamItem {
 #[derive(Debug, Clone, Copy, PartialEq, Eq)]
 pub struct RetainedLog {
     /// Bytes kept on the source's disk (PostgreSQL: the WAL from the slot's `restart_lsn`, the
-    /// `max_wal_lag_bytes` guard).
-    pub bytes: u64,
+    /// `max_wal_lag_bytes` guard); `None` when the source keeps no log for the pipeline (MySQL
+    /// and MariaDB: the binlogs expire by the server's own policy).
+    pub bytes: Option<u64>,
     /// A stream reads it now (PostgreSQL: the slot is `active`). A session whose stream is open
     /// but which reads `false` has lost its stream without hearing of it (DA-SRC-16).
     pub streamed: bool,
+    /// How far the stream reads behind the source's newest change (MySQL and MariaDB:
+    /// `data_avalanche_binlog_lag_seconds`): zero once it has read everything there is, else the
+    /// age of the last transaction it read. `None` when the source doesn't say (PostgreSQL), or
+    /// can't yet: no stream, or one behind that hasn't read a transaction.
+    pub behind: Option<Duration>,
 }
 
 #[derive(Debug, Clone, PartialEq, Eq, Default)]
```

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index b0fc767..9399f1c 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -1077,8 +1077,12 @@ impl<'c> Session<'c> {
                 .is_some_and(|r| !r.streamed)
                 .then(|| unread.to_owned()),
         );
-        let retained = retained.map(|r| r.bytes);
-        self.core.update(|st| st.retained_bytes = retained);
+        let behind = retained.and_then(|r| r.behind);
+        let retained = retained.and_then(|r| r.bytes);
+        self.core.update(|st| {
+            st.retained_bytes = retained;
+            st.source_behind = behind;
+        });
         match (retained, self.core.settings.max_wal_lag_bytes) {
             (Some(lag), Some(limit)) if lag > limit => Err(WorkerError::WalLag { lag, limit }),
             _ => Ok(()),
```

`crates/data-avalanche-pipeline/src/status.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
index e1a0d27..4dc67b0 100644
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -113,6 +113,9 @@ pub struct WorkerStatus {
     /// The log the source keeps for the pipeline, read every heartbeat (PostgreSQL: the slot's
     /// retained WAL, `data_avalanche_wal_lag_bytes`).
     pub retained_bytes: Option<u64>,
+    /// How far the stream reads behind the source's newest change, read every heartbeat (MySQL
+    /// and MariaDB, `data_avalanche_binlog_lag_seconds`).
+    pub source_behind: Option<Duration>,
     /// Sink commits or state writes are failing and being retried (the last error).
     pub degraded: Option<String>,
     pub halted: BTreeMap<TableRef, HaltReason>,
```

`crates/data-avalanche-source-mysql/src/connector.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/connector.rs b/crates/data-avalanche-source-mysql/src/connector.rs
index ca329d9..d19be9b 100644
--- a/crates/data-avalanche-source-mysql/src/connector.rs
+++ b/crates/data-avalanche-source-mysql/src/connector.rs
@@ -20,6 +20,7 @@ use crate::server::{
     Coverage, Flavour, Fold, Settings, access_denied, check_coverage, check_settings,
     grants_replication,
 };
+use crate::start::ReadAt;
 
 /// The settings a reload changes while the connector runs (config §1.2, class R).
 #[derive(Debug, Clone, PartialEq, Eq)]
@@ -57,6 +58,8 @@ pub(crate) struct Shared {
     fold: AtomicBool,
     /// The reloadable settings in force.
     live: Mutex<Live>,
+    /// Where the open stream has read to, `None` without one: the heartbeat's lag.
+    read: Mutex<Option<ReadAt>>,
 }
 
 impl Shared {
@@ -66,8 +69,46 @@ impl Shared {
             dump: AtomicU32::default(),
             fold: AtomicBool::default(),
             live: Mutex::new(Live::of(m, tables)),
+            read: Mutex::default(),
         }
     }
+
+    fn read_at(&self) -> std::sync::MutexGuard<'_, Option<ReadAt>> {
+        self.read.lock().unwrap_or_else(PoisonError::into_inner)
+    }
+
+    /// A stream opened at `from`.
+    pub(crate) fn opened(&self, from: &SourcePosition) {
+        *self.read_at() = Some(ReadAt {
+            position: from.clone(),
+            commit_micros: None,
+            idle: false,
+        });
+    }
+
+    /// The stream read a transaction that committed at `commit_micros`, up to `position`.
+    pub(crate) fn read_to(&self, position: &SourcePosition, commit_micros: i64) {
+        *self.read_at() = Some(ReadAt {
+            position: position.clone(),
+            commit_micros: Some(commit_micros),
+            idle: false,
+        });
+    }
+
+    /// A heartbeat: the dump has nothing more to send.
+    pub(crate) fn idle(&self) {
+        if let Some(r) = self.read_at().as_mut() {
+            r.idle = true;
+        }
+    }
+
+    pub(crate) fn closed(&self) {
+        *self.read_at() = None;
+    }
+
+    pub(crate) fn read(&self) -> Option<ReadAt> {
+        self.read_at().clone()
+    }
 }
 
 #[derive(Debug)]
```

`crates/data-avalanche-source-mysql/src/start.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/start.rs b/crates/data-avalanche-source-mysql/src/start.rs
index 14ec5aa..1f69598 100644
--- a/crates/data-avalanche-source-mysql/src/start.rs
+++ b/crates/data-avalanche-source-mysql/src/start.rs
@@ -11,6 +11,7 @@
 //! server's files means nothing in another's.
 
 use std::sync::atomic::Ordering;
+use std::time::{Duration, SystemTime, UNIX_EPOCH};
 
 use data_avalanche_config::BinlogPurgeAction;
 use data_avalanche_core::{
@@ -22,6 +23,42 @@ use mysql_async::prelude::Queryable;
 use crate::connector::MySqlConnector;
 use crate::position::{Mode, purged};
 
+/// Where the binlog stream has read to, for the heartbeat's lag
+/// (`data_avalanche_binlog_lag_seconds`).
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) struct ReadAt {
+    /// Where the stream opened, then each transaction's end.
+    pub position: SourcePosition,
+    /// The last transaction's commit time (microseconds since the Unix epoch); `None` before the
+    /// stream read one.
+    pub commit_micros: Option<i64>,
+    /// A heartbeat came after it: the dump had nothing more to send.
+    pub idle: bool,
+}
+
+/// How far the stream that read to `read` is behind the server's write position `head` at
+/// `now_micros`: zero once a heartbeat said the dump is idle, or the stream's position covers
+/// `head`; else the age of the last transaction it read. `None` when `head` is unknown, or the
+/// stream is behind without having read a transaction.
+pub(crate) fn behind(
+    read: &ReadAt,
+    head: Option<&SourcePosition>,
+    now_micros: i64,
+) -> Option<Duration> {
+    if read.idle || read.position.covers(head?).unwrap_or(false) {
+        return Some(Duration::ZERO);
+    }
+    let age = now_micros.saturating_sub(read.commit_micros?);
+    Some(Duration::from_micros(u64::try_from(age).unwrap_or(0)))
+}
+
+fn now_micros() -> i64 {
+    let since = SystemTime::now()
+        .duration_since(UNIX_EPOCH)
+        .unwrap_or_default();
+    i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
+}
+
 /// Why `checkpoint` can't be read in `mode`, if it can't.
 pub fn dialect_change(mode: Mode, checkpoint: &SourcePosition) -> Option<String> {
     (checkpoint.dialect() != mode.dialect()).then(|| {
@@ -137,8 +174,9 @@ impl MySqlConnector {
     }
 
     /// The heartbeat's watch on the dump (DA-SRC-16, V-121): whether the server still runs the
-    /// stream's binlog dump, found by its connection id. The server keeps no log for the pipeline
-    /// (its binlogs expire by its own policy), so `bytes` is 0.
+    /// stream's binlog dump, found by its connection id, and how far the stream reads behind the
+    /// server's write position ([`behind`]). The server keeps no log for the pipeline (its
+    /// binlogs expire by its own policy), so `bytes` is `None`.
     pub async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
         let id = self.shared.dump.load(Ordering::Relaxed);
         let streamed = match id {
@@ -160,7 +198,19 @@ impl MySqlConnector {
                     .await?
             }
         };
-        Ok(Some(RetainedLog { bytes: 0, streamed }))
+        let behind = match self.shared.read() {
+            None => None,
+            Some(read) if read.idle => Some(Duration::ZERO),
+            Some(read) => {
+                let head = self.position_now().await.ok();
+                behind(&read, head.as_ref(), now_micros())
+            }
+        };
+        Ok(Some(RetainedLog {
+            bytes: None,
+            streamed,
+            behind,
+        }))
     }
 }
 
```

`crates/data-avalanche-source-mysql/src/stream.rs`:

```diff
diff --git a/crates/data-avalanche-source-mysql/src/stream.rs b/crates/data-avalanche-source-mysql/src/stream.rs
index 62673ff..e7db529 100644
--- a/crates/data-avalanche-source-mysql/src/stream.rs
+++ b/crates/data-avalanche-source-mysql/src/stream.rs
@@ -752,6 +752,7 @@ impl MySqlConnector {
         };
         let events = conn.get_binlog_stream(req).await.map_err(err)?;
         self.shared.dump.store(id, Ordering::Relaxed);
+        self.shared.opened(from);
         Ok(MySqlStream {
             events,
             catalog,
@@ -883,6 +884,9 @@ impl MySqlStream {
                 )));
             }
         };
+        self.catalog
+            .shared
+            .read_to(&self.position, txn.commit_ts_micros);
         if txn.begun {
             let item = StreamItem::Commit(TransactionBoundary {
                 xid: txn.xid,
@@ -926,6 +930,7 @@ impl MySqlStream {
         match raw {
             types::HEARTBEAT | types::HEARTBEAT_V2 => {
                 self.offsets.heartbeat().map_err(payload_invalid)?;
+                self.catalog.shared.idle();
                 if self.txn.is_none() && self.signals.is_none() {
                     self.keep_alive(true);
                 }
@@ -1371,6 +1376,7 @@ impl ChangeStream for MySqlStream {
 
     async fn close(self: Box<Self>) -> Result<(), SourceError> {
         self.catalog.shared.dump.store(0, Ordering::Relaxed);
+        self.catalog.shared.closed();
         self.events.close().await.map_err(|e| mysql(e, None))
     }
 }
```

`crates/data-avalanche-source-pg/src/slot.rs`:

```diff
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
index 5d65c2e..b61630c 100644
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -560,8 +560,9 @@ impl PgConnector {
         Ok(row.and_then(|r| {
             let bytes = r.get::<_, Option<i64>>(0)?;
             Some(RetainedLog {
-                bytes: u64::try_from(bytes).unwrap_or(0),
+                bytes: Some(u64::try_from(bytes).unwrap_or(0)),
                 streamed: r.get(1),
+                behind: None,
             })
         }))
     }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-source-mysql --lib start::`

Expected: PASS, 4 tests.

Run, with `test-infra/up.sh` running, on each of `mysql-84`, `mysql-84-nogtid` and `mariadb-118`:
`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_MYSQL=<target> cargo test -p data-avalanche-source-mysql --test mysql -- start:: --test-threads=1`, and
`DATA_AVALANCHE_IT=1 cargo test -p data-avalanche-source-pg --test pg -- slot:: --test-threads=1`

Expected: PASS: `slot::` 11 tests on PostgreSQL 18; `start::` 8 tests on each MySQL target, the binlog lag's
IT among them.

Run: `cargo test --workspace`

Expected: PASS (1303 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core crates/data-avalanche-source-mysql crates/data-avalanche-source-pg crates/data-avalanche-testkit crates/data-avalanche-pipeline
git commit -m "feat(source-mysql): the binlog lag, the age of the last transaction the stream read; a MySQL source retains no WAL"
```

---

### Task 5: The controller fills the metrics (`data-avalanche-cluster`)

**Files:**
- Modify: `crates/data-avalanche-cluster/src/controller.rs`

**Interfaces:**
- Consumes: Task 1's `PipelineMetrics` methods, Task 2's `Observer`, Tasks 3–4's status fields.
- Produces: `Observed(Arc<PipelineMetrics>)`, the controller's `Observer` (private), passed as
  `WorkerDeps.observer`; `fn lag(unsynced_since_micros: Option<i64>, now_micros: i64) ->
  Duration` (private, clamped at 0).

`observe()`, at each status change and each renewal-interval poll (M6b), sets `wal_lag_bytes`,
`binlog_lag`, `replication_lag` (D-M10.3), `transaction_buffer_bytes`,
`soft_dropped_columns` (the sum over the tables), `snapshot_state` (`waiting` for a snapshot
that waits, else `running`), and the totals with `rows_read`.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 7764f2b..3325b7d 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -1286,6 +1286,62 @@ mod tests {
 
     const P: &str = r#"pipeline="p",source_host="127.0.0.1",source_db="avalanche""#;
 
+    #[test]
+    fn the_lag_is_the_oldest_unsynced_change_s_age() {
+        assert_eq!(lag(None, 5_000_000), Duration::ZERO);
+        assert_eq!(lag(Some(2_000_000), 5_000_000), Duration::from_secs(3));
+        assert_eq!(
+            lag(Some(6_000_000), 5_000_000),
+            Duration::ZERO,
+            "a clock behind the source's"
+        );
+    }
+
+    /// The worker's reads, flushes, commits and lag reach the pipeline's series.
+    #[tokio::test(start_paused = true)]
+    async fn the_worker_s_reads_flushes_and_lag_reach_the_metrics() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        f.source.load(&t("a"), vec![row(1)]);
+        let mut n = node("n1", &store, &docs, &f);
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        f.source.commit(FakeTxn::new().insert(&t("a"), row(2)));
+        eventually("rows land", || f.ids("a") == [1, 2]).await;
+        let rows = format!("data_avalanche_rows_written_total{{{P}}} 2");
+        eventually("the commits are counted", || {
+            line(&n.metrics.encode(), &rows)
+        })
+        .await;
+        let text = n.metrics.encode();
+        let sink = r#"sink_type="delta""#;
+        for metric in [
+            format!(r#"data_avalanche_rows_read_total{{{P},op="insert"}} 1"#),
+            format!(r#"data_avalanche_rows_read_total{{{P},op="snapshot"}} 1"#),
+            format!("data_avalanche_replication_lag_seconds{{{P}}} 0.0"),
+            format!("data_avalanche_transaction_buffer_bytes{{{P}}} 0"),
+            format!("data_avalanche_sink_soft_dropped_columns{{{P}}} 0"),
+            format!(r#"data_avalanche_snapshot_state{{{P},state="running"}} 0"#),
+        ] {
+            assert!(line(&text, &metric), "{metric}\n{text}");
+        }
+        for family in [
+            format!("data_avalanche_flush_rows_count{{{P}}} "),
+            format!("data_avalanche_flush_duration_seconds_count{{{P}}} "),
+            format!("data_avalanche_sink_commit_duration_seconds_count{{{P},{sink}}} "),
+            format!("data_avalanche_snapshot_chunk_duration_seconds_count{{{P}}} "),
+        ] {
+            assert!(
+                text.lines().any(|l| l.starts_with(&family)),
+                "{family}\n{text}"
+            );
+        }
+        assert!(
+            !text.contains("data_avalanche_binlog_lag_seconds{"),
+            "a PostgreSQL source has no binlog: {text}"
+        );
+    }
+
     #[test]
     fn a_start_jitter_stays_below_its_bound() {
         assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-cluster --lib -- the_lag_is_the_oldest_unsynced_change_s_age the_worker_s_reads_flushes_and_lag_reach_the_metrics`

Expected: FAIL to compile: cannot find function `lag` (E0425).

- [ ] **Step 3: Implement**

`crates/data-avalanche-cluster/src/controller.rs`:

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 3325b7d..2b49e7a 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -10,19 +10,21 @@ use std::collections::{BTreeMap, BTreeSet, HashSet};
 use std::fmt::Debug;
 use std::sync::atomic::AtomicBool;
 use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
-use std::time::Duration;
+use std::time::{Duration, SystemTime, UNIX_EPOCH};
 
 use data_avalanche_config::Pipeline;
 use data_avalanche_core::{Sink, SourceConnector, SourceError};
 use data_avalanche_pipeline::{
-    Backoff, Command, CommitGuard, Exit, Hooks, NoObserver, Phase, PipelineWorker, Recovery,
-    Tuning, WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
+    Backoff, Command, CommitGuard, Exit, Hooks, Observer, Phase, PipelineWorker, Recovery, Tuning,
+    WorkerDeps, WorkerError, WorkerHandle, WorkerStatus,
 };
 use data_avalanche_state::{
     ActualState, DesiredState, Holder, Leases, PipelineDocs, RequestsDoc, StatusDoc,
 };
 use data_avalanche_storage::ObjectVersion;
-use data_avalanche_telemetry::{Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, Totals};
+use data_avalanche_telemetry::{
+    Metrics, PipelineLabels, PipelineMetrics, PipelineStatus, RowsRead, SnapshotState, Totals,
+};
 use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
 use tokio::task::JoinHandle;
 use tokio::time::{Instant, MissedTickBehavior};
@@ -309,6 +311,43 @@ fn labels(p: &Pipeline) -> PipelineLabels {
     }
 }
 
+/// The worker's flushes, commits and chunk reads, into the pipeline's series.
+#[derive(Debug)]
+struct Observed(Arc<PipelineMetrics>);
+
+impl Observer for Observed {
+    fn flushed(&self, rows: u64, took: Duration) {
+        self.0.flushed(rows, took);
+    }
+
+    fn committed(&self, rows: u64, took: Duration) {
+        self.0.committed(rows, took);
+    }
+
+    fn commit_failed(&self) {
+        self.0.commit_failed();
+    }
+
+    fn chunk_read(&self, took: Duration) {
+        self.0.chunk_read(took);
+    }
+}
+
+/// `data_avalanche_replication_lag_seconds` at `now_micros`: the age of the oldest change the
+/// worker read and the sink doesn't hold yet, zero when none waits (or the clock is behind the
+/// source's).
+fn lag(unsynced_since_micros: Option<i64>, now_micros: i64) -> Duration {
+    let age = unsynced_since_micros.map_or(0, |since| now_micros.saturating_sub(since));
+    Duration::from_micros(u64::try_from(age).unwrap_or(0))
+}
+
+fn now_micros() -> i64 {
+    let since = SystemTime::now()
+        .duration_since(UNIX_EPOCH)
+        .unwrap_or_default();
+    i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
+}
+
 /// A running worker.
 struct Running {
     handle: WorkerHandle,
@@ -997,7 +1036,7 @@ impl PipelineController {
             owner_epoch: run.owner_epoch,
             guard: run.guard.clone(),
             hooks: parts.hooks,
-            observer: Arc::new(NoObserver),
+            observer: Arc::new(Observed(Arc::clone(&self.metrics))),
             reader_ended: Arc::clone(reader_ended),
         };
         let (worker, handle) =
@@ -1173,6 +1212,10 @@ impl PipelineController {
         }
         let m = &self.metrics;
         m.wal_lag_bytes(st.retained_bytes);
+        m.binlog_lag(st.source_behind);
+        m.replication_lag(lag(st.unsynced_since_micros, now_micros()));
+        m.transaction_buffer_bytes(st.buffer_bytes);
+        m.soft_dropped_columns(st.soft_dropped.values().sum());
         m.halted_tables(st.halted.len());
         m.reconcile_log(st.reconcile_log.entries, st.reconcile_log.keys);
         let rows: Vec<(String, u64)> = st
@@ -1181,12 +1224,31 @@ impl PipelineController {
             .map(|(t, p)| (t.to_string(), p.rows_done))
             .collect();
         m.snapshot_rows_done(&rows);
+        let states: Vec<(String, SnapshotState)> = st
+            .snapshots
+            .iter()
+            .map(|(t, p)| {
+                let state = if p.waiting {
+                    SnapshotState::Waiting
+                } else {
+                    SnapshotState::Running
+                };
+                (t.to_string(), state)
+            })
+            .collect();
+        m.snapshot_state(&states);
         let sink = w.sink.stats();
+        let read = st.rows_read;
         m.totals(Totals {
             source_reconnects: st.reconnects,
             sink_merge_files_rewritten: sink.files_rewritten,
             sink_merge_rows_copied: sink.rows_copied,
-            ..Totals::default()
+            rows_read: RowsRead {
+                insert: read.insert,
+                update: read.update,
+                delete: read.delete,
+                snapshot: read.snapshot,
+            },
         });
         self.status.send_modify(|s| s.worker = Some(st));
     }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-cluster --lib`

Expected: PASS, 141 tests.

Run: `cargo test --workspace`

Expected: PASS (1305 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster
git commit -m "feat(cluster): the controller puts the worker's events and status into the metrics; the lag grows between status changes"
```

---

### Task 6: Traces (`data-avalanche-telemetry`, `data-avalanche-pipeline`)

**Files:**
- Create: `crates/data-avalanche-telemetry/src/traces.rs`
- Modify: `Cargo.toml`, `Cargo.lock`, `crates/data-avalanche-telemetry/{Cargo.toml,src/lib.rs}`, `crates/data-avalanche-pipeline/Cargo.toml`, `crates/data-avalanche-pipeline/src/{writer,session,schema_change}.rs`, `crates/data-avalanche-pipeline/src/session/snapshot.rs`
- Test: `crates/data-avalanche-telemetry/tests/traces.rs` (new), `crates/data-avalanche-pipeline/src/test_support.rs`, the tests of `writer.rs`, `session/snapshot.rs` and `session.rs`

**Interfaces:**
- Consumes: Task 2's writer and chunk reads, `Evolver::evolve` (M6a).
- Produces (re-exported at the telemetry crate's root):
  - `Traces::new(instance: &str) -> Traces`; `set(&self, endpoint: Option<&str>, sample_ratio:
    f64) -> Result<bool, TracesError>` (true when an endpoint is in force); `layer<S>(&self) ->
    impl Layer<S> + Send + Sync` for `S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync`;
    `flush(&self)`; `shutdown(&self)` (each logs a failure as a warning);
  - `TracesError::Endpoint { endpoint: String, reason: String }`;
  - `trace_ids(span: &span::Id, dispatch: &Dispatch) -> Option<(String, String)>`: the trace and
    span ids in hex when a `Traces::layer` in `dispatch` sampled the span.
  - In the pipeline: `Job.pipeline: String` (from `settings.lineage.pipeline`), `Evolver.pipeline:
    &str`; the spans of D-M10.11.

D-M10.9–11. The workspace pins `opentelemetry`, `opentelemetry_sdk` and `opentelemetry-otlp`
`=0.33.1` (`default-features = false`; `trace`; the exporter with `http-proto` and
`reqwest-blocking-client`) and `tracing-opentelemetry` `=0.34.0` (no default features). The
pipeline gains `tracing`, and `tracing-subscriber` (`registry`) for its tests.

- [ ] **Step 1: Write the failing tests**

The telemetry tests run a collector on a thread of the test. `test_support.rs` gains `Spans`: it
records each span the test's thread makes (name, parent, fields) through one global subscriber,
installed at the first `install()`. A subscriber of the test's own would race tracing's callsite
cache, which the whole test binary shares (measured: one run in two lost every span).

`Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index dd725c8..9bb7212 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1842,6 +1842,8 @@ dependencies = [
  "thiserror 2.0.21",
  "tokio",
  "tokio-postgres",
+ "tracing",
+ "tracing-subscriber",
  "uuid",
 ]
 
@@ -1952,7 +1954,15 @@ dependencies = [
 name = "data-avalanche-telemetry"
 version = "0.1.0"
 dependencies = [
+ "opentelemetry",
+ "opentelemetry-otlp",
+ "opentelemetry_sdk",
  "prometheus-client",
+ "thiserror 2.0.21",
+ "tokio",
+ "tracing",
+ "tracing-opentelemetry",
+ "tracing-subscriber",
 ]
 
 [[package]]
@@ -4379,6 +4389,76 @@ version = "0.2.1"
 source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "7c87def4c32ab89d880effc9e097653c8da5d6ef28e6b539d313baaacfbafcbe"
 
+[[package]]
+name = "opentelemetry"
+version = "0.33.1"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "8c1f1a6ac9c875a4941e4c7c085b61e8f4daa326de03b4701768c3491a6ad9ee"
+dependencies = [
+ "futures-core",
+ "futures-sink",
+ "js-sys",
+ "pin-project-lite",
+ "thiserror 2.0.21",
+]
+
+[[package]]
+name = "opentelemetry-http"
+version = "0.33.1"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "302249bd8ee804789faaa7b5b2cbf79bc61cb864714079c644e3828af5b7e811"
+dependencies = [
+ "async-trait",
+ "bytes",
+ "http",
+ "opentelemetry",
+ "reqwest 0.13.5",
+]
+
+[[package]]
+name = "opentelemetry-otlp"
+version = "0.33.1"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "0756af24841e367128ef31c831999937b8b5c5197d47914590d8f4e2e88c2e1a"
+dependencies = [
+ "http",
+ "httpdate",
+ "opentelemetry",
+ "opentelemetry-http",
+ "opentelemetry-proto",
+ "opentelemetry_sdk",
+ "prost",
+ "reqwest 0.13.5",
+ "thiserror 2.0.21",
+]
+
+[[package]]
+name = "opentelemetry-proto"
+version = "0.33.1"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "d576f5dddc3299341bcd85a04be17b2564844c0c9563214f02d85f2524d1afae"
+dependencies = [
+ "opentelemetry",
+ "opentelemetry_sdk",
+ "prost",
+]
+
+[[package]]
+name = "opentelemetry_sdk"
+version = "0.33.1"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "372a912db2974d1990207006fd0e3aa0ef3fd4ad3be43c65cfaadf2206866061"
+dependencies = [
+ "futures-channel",
+ "futures-executor",
+ "futures-util",
+ "opentelemetry",
+ "percent-encoding",
+ "portable-atomic",
+ "rand 0.9.5",
+ "thiserror 2.0.21",
+]
+
 [[package]]
 name = "option-ext"
 version = "0.2.0"
@@ -4579,6 +4659,12 @@ version = "0.3.34"
 source = "registry+https://github.com/rust-lang/crates.io-index"
 checksum = "f6b464fbc74e149a392436b17d523f769e057cb6877f6a5c4618bc6f11800548"
 
+[[package]]
+name = "portable-atomic"
+version = "1.15.0"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "05c8b63e8d9609db387f0324918f81d68fe27748f084ef092fb35954d0539a85"
+
 [[package]]
 name = "postcard"
 version = "1.1.3"
@@ -5090,6 +5176,7 @@ dependencies = [
  "base64 0.23.1",
  "bytes",
  "encoding_rs",
+ "futures-channel",
  "futures-core",
  "futures-util",
  "h2",
@@ -6120,6 +6207,20 @@ dependencies = [
  "tracing-core",
 ]
 
+[[package]]
+name = "tracing-opentelemetry"
+version = "0.34.0"
+source = "registry+https://github.com/rust-lang/crates.io-index"
+checksum = "0a904802a1b902f43638b677ff2a650847e3b4404101b6c586d648e8c1e3e8fe"
+dependencies = [
+ "js-sys",
+ "opentelemetry",
+ "tracing",
+ "tracing-core",
+ "tracing-subscriber",
+ "web-time",
+]
+
 [[package]]
 name = "tracing-serde"
 version = "0.2.0"
```

`Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 7890a87..74df10b 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -91,6 +91,12 @@ subtle = "2.6.1"
 # Its OpenAPI document (roadmap §4 M9), built from the route table with the builders. 6.0.0
 # doesn't build without `macros`: its schema module names a type only that feature defines.
 utoipa = { version = "=6.0.0", default-features = false, features = ["macros"] }
+# Traces (M10, Q-M10-1): OTLP over HTTP/protobuf with the workspace's reqwest and prost; the
+# batch exporter's thread exports with the blocking client.
+opentelemetry = { version = "=0.33.1", default-features = false, features = ["trace"] }
+opentelemetry_sdk = { version = "=0.33.1", default-features = false, features = ["trace"] }
+opentelemetry-otlp = { version = "=0.33.1", default-features = false, features = ["trace", "http-proto", "reqwest-blocking-client"] }
+tracing-opentelemetry = { version = "=0.34.0", default-features = false }
 # The cluster (M8a, roadmap §2.1): the ring's hash (the anchor's `hash_algo: xxh3`); the SWIM
 # core the gossip drives (Q-M8-1, probe `prototypes/cluster-capability`) and its random source;
 # the host's addresses, for an unset `advertise_addr`.
```

`crates/data-avalanche-pipeline/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-pipeline/Cargo.toml b/crates/data-avalanche-pipeline/Cargo.toml
index 5d3aa49..dba7f62 100644
--- a/crates/data-avalanche-pipeline/Cargo.toml
+++ b/crates/data-avalanche-pipeline/Cargo.toml
@@ -22,6 +22,7 @@ getrandom.workspace = true
 serde_json.workspace = true
 thiserror.workspace = true
 tokio = { workspace = true, features = ["sync", "rt", "time", "macros"] }
+tracing.workspace = true
 uuid.workspace = true
 
 [dev-dependencies]
@@ -34,3 +35,4 @@ object_store.workspace = true
 proptest.workspace = true
 tokio = { workspace = true, features = ["test-util"] }
 tokio-postgres.workspace = true
+tracing-subscriber = { workspace = true, features = ["registry"] }
```

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index 9399f1c..dae7fe2 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -1355,7 +1355,7 @@ mod tests {
     use data_avalanche_testkit::{FakeTxn, Method, SinkOp};
 
     use super::*;
-    use crate::test_support::{World, Yaml, pipeline, row, schema, t, wait_until};
+    use crate::test_support::{Spans, World, Yaml, pipeline, row, schema, t, wait_until};
 
     /// Runs a session over `w` until `drive` returns, then drains it.
     async fn session<F: Future<Output = ()>>(
@@ -2540,6 +2540,8 @@ mod tests {
 
     #[tokio::test]
     async fn an_added_column_is_evolved_where_the_stream_meets_it() {
+        let spans = Spans::default();
+        let _subscriber = spans.install();
         let w = World::new(&["a"]);
         w.started(&["a"]).await;
         let (end, _) = session(&w, &flush_each(), &["a"], async |status| {
@@ -2556,6 +2558,18 @@ mod tests {
         let cached = w.docs.table_schema(&t("a")).await.unwrap().unwrap().doc;
         assert!(cached.column("note").is_some(), "cached once durable");
         assert_eq!(cached.source_id(), w.source.schema(&t("a")).source_id());
+        let [apply] = &spans.named("schema.apply")[..] else {
+            panic!("one apply")
+        };
+        assert_eq!(apply.parent, None, "a trace of its own");
+        for (field, value) in [
+            ("pipeline", "p"),
+            ("table", "public.a"),
+            ("changes", "1"),
+            ("outcome", "applied"),
+        ] {
+            assert_eq!(apply.field(field), Some(value), "{field}");
+        }
     }
 
     /// The table's next evolve fails `Permanent`: an evolve would halt it `SINK_REJECTED`.
```

`crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
index 9b2445f..0c5d508 100644
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -707,7 +707,7 @@ mod tests {
 
     use super::*;
     use crate::session::Core;
-    use crate::test_support::{Ears, Heard, World, Yaml, row, t, wait_until};
+    use crate::test_support::{Ears, Heard, Spans, World, Yaml, row, t, wait_until};
     use crate::{Backoff, Command, Exit, Hooks, Phase, ReconcileLog, WorkerStatus};
 
     fn chunks_of(n: u64) -> Pipeline {
@@ -839,9 +839,11 @@ mod tests {
         );
     }
 
-    /// Each chunk read is reported, the last, empty one included.
+    /// Each chunk read is reported, the last, empty one included, and is a trace of its own.
     #[tokio::test]
     async fn each_chunk_read_is_reported() {
+        let spans = Spans::default();
+        let _subscriber = spans.install();
         let w = World::new(&["a"]);
         w.started(&["a"]).await;
         w.source.load(&t("a"), rows(1..=5));
@@ -860,6 +862,13 @@ mod tests {
             .count();
         assert_eq!(chunks, 4, "two, two, one, then the empty chunk");
         assert_eq!(st.rows_read.snapshot, 5);
+        let chunk_spans = spans.named("snapshot.chunk");
+        let reads: Vec<_> = chunk_spans
+            .iter()
+            .map(|s| (s.parent, s.field("table"), s.field("rows")))
+            .collect();
+        let read = |rows| (None, Some("public.a"), Some(rows));
+        assert_eq!(reads, [read("2"), read("2"), read("1"), read("0")]);
     }
 
     #[tokio::test]
```

`crates/data-avalanche-pipeline/src/test_support.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/test_support.rs b/crates/data-avalanche-pipeline/src/test_support.rs
index b925d7b..c2e96d0 100644
--- a/crates/data-avalanche-pipeline/src/test_support.rs
+++ b/crates/data-avalanche-pipeline/src/test_support.rs
@@ -28,6 +28,124 @@ use crate::{
     NoHooks, NoObserver, Observer, RowsRead, Tuning, Unguarded, WorkerStatus,
 };
 
+/// A span a test saw: its name, its parent's, and its fields as last recorded.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub(crate) struct SeenSpan {
+    pub name: &'static str,
+    pub parent: Option<&'static str>,
+    pub fields: BTreeMap<String, String>,
+}
+
+impl SeenSpan {
+    pub fn field(&self, name: &str) -> Option<&str> {
+        self.fields.get(name).map(String::as_str)
+    }
+}
+
+/// The spans a test's thread made while [`Spans::install`]'s guard lived, in order.
+#[derive(Debug, Clone, Default)]
+pub(crate) struct Spans(Arc<Mutex<Vec<(tracing::span::Id, SeenSpan)>>>);
+
+thread_local! {
+    /// The [`Spans`] this thread's spans go to, while a test records them.
+    static RECORDING: std::cell::RefCell<Option<Spans>> = const { std::cell::RefCell::new(None) };
+}
+
+/// [`Spans::install`]'s guard: once it drops, the thread's spans go nowhere.
+pub(crate) struct Recording;
+
+impl Drop for Recording {
+    fn drop(&mut self) {
+        RECORDING.with(|r| r.borrow_mut().take());
+    }
+}
+
+impl Spans {
+    /// Records this thread's spans until the guard drops, through the process's subscriber: one
+    /// [`Recorder`] for the test binary, installed at the first call. A subscriber of the test's
+    /// own would race tracing's callsite cache, which is the process's: a span first met on
+    /// another test's thread, with no subscriber, can be cached as wanted by nobody, and is then
+    /// never seen (one run in two of the crate's tests, measured).
+    pub fn install(&self) -> Recording {
+        static GLOBAL: std::sync::Once = std::sync::Once::new();
+        GLOBAL.call_once(|| {
+            use tracing_subscriber::layer::SubscriberExt;
+            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Recorder))
+                .expect("the crate's tests install no other global subscriber");
+        });
+        RECORDING.with(|r| *r.borrow_mut() = Some(self.clone()));
+        Recording
+    }
+
+    pub fn named(&self, name: &str) -> Vec<SeenSpan> {
+        self.0
+            .lock()
+            .unwrap()
+            .iter()
+            .filter(|(_, s)| s.name == name)
+            .map(|(_, s)| s.clone())
+            .collect()
+    }
+}
+
+struct Fields<'a>(&'a mut BTreeMap<String, String>);
+
+impl tracing::field::Visit for Fields<'_> {
+    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
+        self.0.insert(field.name().to_owned(), value.to_owned());
+    }
+
+    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
+        self.0.insert(field.name().to_owned(), format!("{value:?}"));
+    }
+}
+
+/// The test binary's layer: each span goes to its thread's [`Spans`], if a test records them.
+struct Recorder;
+
+impl<S> tracing_subscriber::Layer<S> for Recorder
+where
+    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
+{
+    fn on_new_span(
+        &self,
+        attrs: &tracing::span::Attributes<'_>,
+        id: &tracing::span::Id,
+        ctx: tracing_subscriber::layer::Context<'_, S>,
+    ) {
+        let parent = ctx.span(id).and_then(|s| s.parent()).map(|p| p.name());
+        let mut fields = BTreeMap::new();
+        attrs.record(&mut Fields(&mut fields));
+        let seen = SeenSpan {
+            name: attrs.metadata().name(),
+            parent,
+            fields,
+        };
+        RECORDING.with(|r| {
+            if let Some(spans) = &*r.borrow() {
+                spans.0.lock().unwrap().push((id.clone(), seen));
+            }
+        });
+    }
+
+    fn on_record(
+        &self,
+        id: &tracing::span::Id,
+        values: &tracing::span::Record<'_>,
+        _: tracing_subscriber::layer::Context<'_, S>,
+    ) {
+        RECORDING.with(|r| {
+            let Some(spans) = &*r.borrow() else {
+                return;
+            };
+            let mut spans = spans.0.lock().unwrap();
+            if let Some((_, s)) = spans.iter_mut().rev().find(|(i, _)| i == id) {
+                values.record(&mut Fields(&mut s.fields));
+            }
+        });
+    }
+}
+
 /// What an [`Ears`] observer heard.
 #[derive(Debug, Clone, Copy, PartialEq, Eq)]
 pub(crate) enum Heard {
```

`crates/data-avalanche-pipeline/src/writer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index 69b9373..ef1172e 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -313,7 +313,7 @@ mod tests {
     use super::*;
     use crate::NoHooks;
     use crate::test_support::{
-        CrashAt, Ears, FlakyStore, Heard, Switch, accumulator, docs, lsn, schema, txn,
+        CrashAt, Ears, FlakyStore, Heard, Spans, Switch, accumulator, docs, lsn, schema, txn,
     };
 
     const FAST: Backoff = Backoff {
@@ -422,6 +422,7 @@ mod tests {
 
     fn job(flush: crate::Flush) -> Job {
         Job {
+            pipeline: "p".into(),
             change_sets: flush.change_sets,
             checkpoint: flush.checkpoint,
             halts: vec![],
@@ -496,6 +497,7 @@ mod tests {
         r.server = Some(server.clone());
         let at = SourcePosition::parse_tagged("mariadb-gtid:0-7-42").unwrap();
         let j = Job {
+            pipeline: "p".into(),
             change_sets: vec![],
             checkpoint: at.clone(),
             halts: vec![],
@@ -607,6 +609,62 @@ mod tests {
         );
     }
 
+    /// A flush is a trace of its own: its commits inside it, each with its attempts and how it
+    /// ended.
+    #[tokio::test]
+    async fn a_flush_is_a_span_with_a_span_per_commit() {
+        let spans = Spans::default();
+        let _subscriber = spans.install();
+        let r = rig(&["a", "b"]).await;
+        r.sink.fail(
+            SinkOp::Commit,
+            Some(&TableRef::new("public", "a")),
+            1,
+            || SinkError::Transient {
+                reason: "throttled".into(),
+            },
+        );
+        let mut acc = accumulator(&["a", "b"], 100);
+        txn(&mut acc, 1, 200, &[("a", 1, "x"), ("b", 1, "y")]);
+        r.run(vec![job(acc.flush())], Arc::new(NoHooks)).await;
+        let [flush] = &spans.named("pipeline.flush")[..] else {
+            panic!("one flush")
+        };
+        assert_eq!(flush.parent, None, "a root");
+        for (field, value) in [("pipeline", "p"), ("change_sets", "2"), ("rows", "2")] {
+            assert_eq!(flush.field(field), Some(value), "{field}");
+        }
+        let commits = spans.named("sink.commit");
+        let seen: Vec<_> = commits
+            .iter()
+            .map(|c| {
+                (
+                    c.parent,
+                    c.field("table"),
+                    c.field("attempts"),
+                    c.field("outcome"),
+                )
+            })
+            .collect();
+        assert_eq!(
+            seen,
+            [
+                (
+                    Some("pipeline.flush"),
+                    Some("public.a"),
+                    Some("2"),
+                    Some("committed")
+                ),
+                (
+                    Some("pipeline.flush"),
+                    Some("public.b"),
+                    Some("1"),
+                    Some("committed")
+                ),
+            ]
+        );
+    }
+
     /// Every failed attempt counts, the retried ones and the refusal.
     #[tokio::test]
     async fn every_failed_commit_attempt_is_reported() {
```

`crates/data-avalanche-telemetry/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-telemetry/Cargo.toml b/crates/data-avalanche-telemetry/Cargo.toml
index 888b48b..748c544 100644
--- a/crates/data-avalanche-telemetry/Cargo.toml
+++ b/crates/data-avalanche-telemetry/Cargo.toml
@@ -10,4 +10,14 @@ description = "Metrics for data-avalanche: the Prometheus registry and the catal
 workspace = true
 
 [dependencies]
+opentelemetry.workspace = true
+opentelemetry-otlp.workspace = true
+opentelemetry_sdk.workspace = true
 prometheus-client.workspace = true
+thiserror.workspace = true
+tracing.workspace = true
+tracing-opentelemetry.workspace = true
+tracing-subscriber = { workspace = true, features = ["registry"] }
+
+[dev-dependencies]
+tokio = { workspace = true, features = ["rt", "macros", "net", "io-util", "time"] }
```

`crates/data-avalanche-telemetry/tests/traces.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/traces.rs b/crates/data-avalanche-telemetry/tests/traces.rs
new file mode 100644
index 0000000..04d355d
--- /dev/null
+++ b/crates/data-avalanche-telemetry/tests/traces.rs
@@ -0,0 +1,154 @@
+//! The OTLP/HTTP export (config §6.6): a sampled span reaches the collector's `/v1/traces`; one
+//! not sampled doesn't; a span's ids are known only when it is sampled.
+
+use std::io::{BufRead, BufReader, Read, Write};
+use std::net::TcpListener;
+use std::sync::mpsc;
+use std::time::Duration;
+
+use data_avalanche_telemetry::{Traces, trace_ids};
+use tracing_subscriber::layer::SubscriberExt;
+
+/// A collector on a thread of its own: each request's path and body, answered 200.
+fn collector() -> (String, mpsc::Receiver<(String, Vec<u8>)>) {
+    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
+    let url = format!("http://{}", listener.local_addr().unwrap());
+    let (tx, rx) = mpsc::channel();
+    std::thread::spawn(move || {
+        for conn in listener.incoming() {
+            let mut conn = conn.unwrap();
+            let mut reader = BufReader::new(conn.try_clone().unwrap());
+            let mut line = String::new();
+            reader.read_line(&mut line).unwrap();
+            let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
+            let mut length = 0;
+            loop {
+                let mut header = String::new();
+                reader.read_line(&mut header).unwrap();
+                if header == "\r\n" {
+                    break;
+                }
+                if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
+                    length = v.trim().parse().unwrap();
+                }
+            }
+            let mut body = vec![0; length];
+            reader.read_exact(&mut body).unwrap();
+            conn.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
+                .unwrap();
+            let _ = tx.send((path, body));
+        }
+    });
+    (url, rx)
+}
+
+fn contains(haystack: &[u8], needle: &str) -> bool {
+    haystack
+        .windows(needle.len())
+        .any(|w| w == needle.as_bytes())
+}
+
+/// Runs a `pipeline.flush` span with a `sink.commit` inside it under `traces`, and returns the
+/// flush's ids as the layer saw them.
+fn flush(traces: &Traces) -> Option<(String, String)> {
+    let subscriber = tracing_subscriber::registry().with(traces.layer());
+    let dispatch = tracing::Dispatch::new(subscriber);
+    tracing::dispatcher::with_default(&dispatch, || {
+        let span = tracing::info_span!("pipeline.flush", pipeline = "orders");
+        let ids = span.id().and_then(|id| trace_ids(&id, &dispatch));
+        span.in_scope(|| {
+            tracing::info_span!("sink.commit", table = "public.a").in_scope(|| {});
+        });
+        ids
+    })
+}
+
+#[test]
+fn a_sampled_flush_is_exported_to_the_collector() {
+    let (url, requests) = collector();
+    let traces = Traces::new("node-1");
+    assert!(traces.set(Some(&format!("{url}/")), 1.0).unwrap());
+    let ids = flush(&traces);
+    traces.shutdown();
+    let (path, body) = requests.recv_timeout(Duration::from_secs(10)).unwrap();
+    assert_eq!(path, "/v1/traces");
+    for text in [
+        "pipeline.flush",
+        "sink.commit",
+        "orders",
+        "data-avalanche",
+        "node-1",
+    ] {
+        assert!(contains(&body, text), "{text}");
+    }
+    let (trace, span) = ids.expect("a sampled span has ids");
+    assert_eq!((trace.len(), span.len()), (32, 16));
+}
+
+#[test]
+fn a_flush_not_sampled_is_neither_exported_nor_named() {
+    let (url, requests) = collector();
+    let traces = Traces::new("node-1");
+    traces.set(Some(&url), 0.0).unwrap();
+    assert_eq!(flush(&traces), None);
+    traces.shutdown();
+    assert!(requests.recv_timeout(Duration::from_millis(500)).is_err());
+}
+
+/// A reload swaps the exporter: the spans after it go to the new endpoint; without one, nowhere.
+#[test]
+fn a_new_endpoint_takes_the_next_spans_and_none_takes_none() {
+    let (first, to_first) = collector();
+    let (second, to_second) = collector();
+    let traces = Traces::new("node-1");
+    traces.set(Some(&first), 1.0).unwrap();
+    flush(&traces);
+    traces.flush();
+    assert!(to_first.recv_timeout(Duration::from_secs(10)).is_ok());
+    traces.set(Some(&second), 1.0).unwrap();
+    flush(&traces);
+    traces.flush();
+    assert!(to_second.recv_timeout(Duration::from_secs(10)).is_ok());
+    assert!(to_first.try_recv().is_err());
+    assert!(!traces.set(None, 1.0).unwrap());
+    assert_eq!(flush(&traces), None, "off: nothing is sampled");
+    traces.shutdown();
+    assert!(to_second.recv_timeout(Duration::from_millis(500)).is_err());
+}
+
+#[test]
+fn an_endpoint_that_isn_t_a_url_is_refused_and_the_one_in_force_stays() {
+    let (url, requests) = collector();
+    let traces = Traces::new("node-1");
+    traces.set(Some(&url), 1.0).unwrap();
+    let e = traces.set(Some("not a url"), 1.0).unwrap_err();
+    assert!(e.to_string().contains("not a url"), "{e}");
+    flush(&traces);
+    traces.shutdown();
+    assert!(requests.recv_timeout(Duration::from_secs(10)).is_ok());
+}
+
+/// A collector that takes the connection and never answers holds nothing up (Review Focus 2): the
+/// spans queue in the batch processor, which drops them once it is full, and the shutdown waits a
+/// bounded time.
+#[test]
+fn a_collector_that_never_answers_doesn_t_hold_the_spans_up() {
+    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
+    let url = format!("http://{}", silent.local_addr().unwrap());
+    let traces = Traces::new("node-1");
+    traces.set(Some(&url), 1.0).unwrap();
+    let started = std::time::Instant::now();
+    for _ in 0..5_000 {
+        flush(&traces);
+    }
+    let traced = started.elapsed();
+    assert!(
+        traced < Duration::from_secs(5),
+        "5000 flushes traced in {traced:?}"
+    );
+    let started = std::time::Instant::now();
+    traces.shutdown();
+    let shut = started.elapsed();
+    assert!(shut < Duration::from_secs(15), "shut down in {shut:?}");
+    drop(silent);
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-telemetry --test traces`, then
`cargo test -p data-avalanche-pipeline --lib -- a_flush_is_a_span_with_a_span_per_commit each_chunk_read_is_reported an_added_column_is_evolved_where_the_stream_meets_it`

Expected: FAIL to compile: unresolved imports `data_avalanche_telemetry::{Traces, trace_ids}` (E0432);
in the pipeline, `writer::Job` has no field `pipeline` (E0560).

- [ ] **Step 3: Implement**

`crates/data-avalanche-pipeline/src/schema_change.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/schema_change.rs b/crates/data-avalanche-pipeline/src/schema_change.rs
index f0cce6d..8fdd297 100644
--- a/crates/data-avalanche-pipeline/src/schema_change.rs
+++ b/crates/data-avalanche-pipeline/src/schema_change.rs
@@ -9,6 +9,8 @@ use data_avalanche_core::{
     classify,
 };
 use tokio::sync::watch;
+use tracing::Instrument;
+use tracing::field::Empty;
 
 use crate::{CommitGuard, Tuning, WorkerError, WorkerStatus};
 
@@ -34,6 +36,8 @@ pub(crate) struct Planned {
 }
 
 pub(crate) struct Evolver<'a> {
+    /// The pipeline's name, as the evolve's span names it.
+    pub pipeline: &'a str,
     pub source: &'a dyn SourceConnector,
     pub sink: &'a dyn Sink,
     pub guard: &'a dyn CommitGuard,
@@ -88,11 +92,36 @@ impl Evolver<'_> {
     }
 
     /// The sink's evolve of the Safe changes, retried while it fails transiently. `Ok(Some)` is a
-    /// degraded apply's warning; `Ok(Err)` halts the table.
+    /// degraded apply's warning; `Ok(Err)` halts the table. A trace of its own, which records how
+    /// it ended.
     pub async fn evolve(
         &self,
         table: &TableRef,
         safe: Vec<SchemaChange>,
+    ) -> Result<Result<Option<String>, HaltReason>, WorkerError> {
+        let span = tracing::info_span!(
+            parent: None,
+            "schema.apply",
+            pipeline = %self.pipeline,
+            table = %table,
+            changes = safe.len(),
+            outcome = Empty,
+        );
+        let applied = self.apply(table, safe).instrument(span.clone()).await;
+        let outcome = match &applied {
+            Ok(Ok(None)) => "applied",
+            Ok(Ok(Some(_))) => "degraded",
+            Ok(Err(_)) => "halted",
+            Err(_) => "failed",
+        };
+        span.record("outcome", outcome);
+        applied
+    }
+
+    async fn apply(
+        &self,
+        table: &TableRef,
+        safe: Vec<SchemaChange>,
     ) -> Result<Result<Option<String>, HaltReason>, WorkerError> {
         let delta = SchemaDelta {
             table: table.clone(),
```

`crates/data-avalanche-pipeline/src/session.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index dae7fe2..127c0d6 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -132,6 +132,7 @@ impl Core {
 
     pub fn evolver(&self) -> Evolver<'_> {
         Evolver {
+            pipeline: &self.settings.lineage.pipeline,
             source: &*self.source,
             sink: &*self.sink,
             guard: &*self.guard,
@@ -1230,6 +1231,7 @@ impl<'c> Session<'c> {
         // A slice of a large transaction leaves the rest of it to wait.
         self.unflushed_since = self.open_since;
         let job = Job {
+            pipeline: self.core.settings.lineage.pipeline.clone(),
             change_sets,
             checkpoint: checkpoint.clone(),
             halts: std::mem::take(&mut self.halts),
```

`crates/data-avalanche-pipeline/src/session/snapshot.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/session/snapshot.rs b/crates/data-avalanche-pipeline/src/session/snapshot.rs
index 0c5d508..6598134 100644
--- a/crates/data-avalanche-pipeline/src/session/snapshot.rs
+++ b/crates/data-avalanche-pipeline/src/session/snapshot.rs
@@ -25,6 +25,8 @@ use data_avalanche_core::{
 };
 use data_avalanche_state::{SnapshotStatus, TableSnapshotDoc};
 use tokio::task::JoinSet;
+use tracing::Instrument;
+use tracing::field::Empty;
 
 use super::{End, Session, protocol};
 use crate::buffer::identity;
@@ -403,12 +405,21 @@ impl Session<'_> {
             let after = snap.cursor.clone();
             let limit = self.core.settings.chunk_size;
             let table = table.clone();
-            self.snaps.reads.spawn(async move {
+            // A trace per chunk read.
+            let span = tracing::info_span!(
+                parent: None,
+                "snapshot.chunk",
+                pipeline = %self.core.settings.lineage.pipeline,
+                table = %table,
+                rows = Empty,
+            );
+            let read = async move {
                 let read_at_micros = micros_now();
                 let started = Instant::now();
                 let result = source.read_chunk(&schema, after.as_ref(), limit).await;
-                if result.is_ok() {
+                if let Ok(chunk) = &result {
                     observer.chunk_read(started.elapsed());
+                    tracing::Span::current().record("rows", chunk.rows.len());
                 }
                 Read {
                     table,
@@ -417,7 +428,8 @@ impl Session<'_> {
                     result,
                     read_at_micros,
                 }
-            });
+            };
+            self.snaps.reads.spawn(read.instrument(span));
             snap.reading = true;
             reading += 1;
         }
```

`crates/data-avalanche-pipeline/src/writer.rs`:

```diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index ef1172e..132841a 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -20,6 +20,8 @@ use data_avalanche_state::{PipelineDocs, StateError, TableHalt, TableSnapshotDoc
 use data_avalanche_storage::StoreError;
 use tokio::sync::OwnedSemaphorePermit;
 use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
+use tracing::Instrument;
+use tracing::field::Empty;
 
 use crate::backoff::Backoff;
 use crate::checkpoint::{Doc, Store};
@@ -28,6 +30,8 @@ use crate::{CommitGuard, HookAction, HookPoint, Hooks, Observer, WorkerError};
 /// One flush and what becomes durable with it.
 #[derive(Debug)]
 pub(crate) struct Job {
+    /// The pipeline's name, as the flush's span names it.
+    pub pipeline: String,
     pub change_sets: Vec<TableChangeSet>,
     pub checkpoint: SourcePosition,
     /// Halts the event loop found since the last job, recorded before the checkpoint.
@@ -96,7 +100,16 @@ pub(crate) async fn run(
         degraded: false,
     };
     while let Some(job) = jobs.recv().await {
-        if let Err(e) = w.job(job).await {
+        // A trace per flush: its own root, the commits inside it.
+        let span = tracing::info_span!(
+            parent: None,
+            "pipeline.flush",
+            pipeline = %job.pipeline,
+            change_sets = job.change_sets.len(),
+            rows = job.change_sets.iter().map(rows_of).sum::<u64>(),
+            checkpoint = %job.checkpoint,
+        );
+        if let Err(e) = w.job(job).instrument(span).await {
             let _ = w.events.send(WriterEvent::Failed(e));
             return;
         }
@@ -192,20 +205,38 @@ impl Writer {
     }
 
     /// Commits one change set, retrying a transient failure. `Some(detail)` when the sink refused
-    /// it as permanent: the table halts.
+    /// it as permanent: the table halts. Its span records the attempts and how it ended.
     async fn commit(&mut self, cs: TableChangeSet) -> Result<Option<String>, WorkerError> {
+        let rows = rows_of(&cs);
+        let span = tracing::info_span!(
+            "sink.commit",
+            table = %cs.table,
+            rows,
+            attempts = Empty,
+            outcome = Empty,
+        );
+        self.attempts(cs, rows).instrument(span).await
+    }
+
+    async fn attempts(
+        &mut self,
+        cs: TableChangeSet,
+        rows: u64,
+    ) -> Result<Option<String>, WorkerError> {
         let marker = CommitMarker {
             owner_epoch: self.ctx.owner_epoch,
             committed_position: cs.position.end.clone(),
             pipeline_id: self.ctx.pipeline_id.clone(),
         };
-        let rows = rows_of(&cs);
         let observer = Arc::clone(&self.ctx.observer);
+        let span = tracing::Span::current();
         let mut attempt = 0;
         loop {
             if !self.ctx.guard.may_commit() {
+                span.record("outcome", "lease lost");
                 return Err(WorkerError::LeaseLost);
             }
+            span.record("attempts", attempt + 1);
             let at = Instant::now();
             let outcome = self.ctx.sink.commit(cs.clone(), marker.clone()).await;
             if outcome.is_err() {
@@ -214,14 +245,17 @@ impl Writer {
             match outcome {
                 Ok(CommitOutcome::Committed { .. }) => {
                     observer.committed(rows, at.elapsed());
+                    span.record("outcome", "committed");
                     self.degraded(None);
                     return Ok(None);
                 }
                 Ok(CommitOutcome::SkippedCovered) => {
+                    span.record("outcome", "covered");
                     self.degraded(None);
                     return Ok(None);
                 }
                 Ok(CommitOutcome::Fenced { current_epoch }) => {
+                    span.record("outcome", "fenced");
                     return Err(WorkerError::Fenced {
                         table: cs.table,
                         current_epoch,
@@ -233,10 +267,14 @@ impl Writer {
                     attempt += 1;
                 }
                 Err(SinkError::Permanent { reason }) => {
+                    span.record("outcome", "refused");
                     self.degraded(None);
                     return Ok(Some(reason));
                 }
-                Err(e) => return Err(WorkerError::Sink(e)),
+                Err(e) => {
+                    span.record("outcome", "failed");
+                    return Err(WorkerError::Sink(e));
+                }
             }
         }
     }
```

`crates/data-avalanche-telemetry/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/src/lib.rs b/crates/data-avalanche-telemetry/src/lib.rs
index 53f71d7..1f414db 100644
--- a/crates/data-avalanche-telemetry/src/lib.rs
+++ b/crates/data-avalanche-telemetry/src/lib.rs
@@ -11,6 +11,10 @@ use std::sync::atomic::AtomicU64;
 use std::sync::{Arc, Mutex, PoisonError};
 use std::time::Duration;
 
+mod traces;
+
+pub use traces::{Traces, TracesError, trace_ids};
+
 use prometheus_client::encoding::text::encode;
 use prometheus_client::metrics::counter::Counter;
 use prometheus_client::metrics::family::Family;
```

`crates/data-avalanche-telemetry/src/traces.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/src/traces.rs b/crates/data-avalanche-telemetry/src/traces.rs
new file mode 100644
index 0000000..95955e8
--- /dev/null
+++ b/crates/data-avalanche-telemetry/src/traces.rs
@@ -0,0 +1,238 @@
+//! Traces (config §6.6, `Observability_Metrics_and_Tracing` §3): the `tracing` spans of a flush,
+//! a snapshot chunk and a schema change, exported over OTLP/HTTP (protobuf) to
+//! `<otlp_endpoint>/v1/traces` (Q-M10-1), head-sampled by `sample_ratio`.
+//!
+//! One provider and one `tracing` layer serve the whole run: a reload of `globals.tracing` swaps
+//! the exporter behind them and the sampling ratio (D-M10.6). A layer can't be reloaded: the
+//! logs find a span's ids by downcasting to it, which a reload layer doesn't pass through. The
+//! batch exporter runs on a thread of its own and exports with a blocking client.
+
+use std::fmt;
+use std::sync::atomic::{AtomicU64, Ordering};
+use std::sync::{Arc, PoisonError, RwLock};
+use std::time::Duration;
+
+use opentelemetry::trace::{Link, SpanKind, TraceContextExt, TraceId, TracerProvider};
+use opentelemetry::{Context, KeyValue};
+use opentelemetry_otlp::WithExportConfig;
+use opentelemetry_sdk::Resource;
+use opentelemetry_sdk::error::OTelSdkResult;
+use opentelemetry_sdk::trace::{
+    Sampler, SamplingResult, SdkTracerProvider, ShouldSample, SpanData, SpanExporter,
+};
+use tracing::{Dispatch, Subscriber, span};
+use tracing_subscriber::Layer;
+use tracing_subscriber::registry::LookupSpan;
+
+/// How long a shutdown waits for the spans still queued.
+const SHUTDOWN: Duration = Duration::from_secs(5);
+
+#[derive(Debug, thiserror::Error)]
+pub enum TracesError {
+    #[error("otlp_endpoint {endpoint:?} can't be exported to: {reason}")]
+    Endpoint { endpoint: String, reason: String },
+}
+
+/// The OTLP exporter in force, `None` while tracing is off, and the resource every one carries.
+#[derive(Default)]
+struct Current {
+    exporter: Option<Arc<opentelemetry_otlp::SpanExporter>>,
+    resource: Option<Resource>,
+}
+
+/// The provider's exporter: whichever [`Traces::set`] put in force.
+#[derive(Clone, Default)]
+struct Switch(Arc<RwLock<Current>>);
+
+impl fmt::Debug for Switch {
+    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
+        f.debug_struct("Switch").finish_non_exhaustive()
+    }
+}
+
+impl Switch {
+    fn current(&self) -> Option<Arc<opentelemetry_otlp::SpanExporter>> {
+        self.0
+            .read()
+            .unwrap_or_else(PoisonError::into_inner)
+            .exporter
+            .clone()
+    }
+}
+
+impl SpanExporter for Switch {
+    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
+        match self.current() {
+            Some(exporter) => exporter.export(batch).await,
+            None => Ok(()),
+        }
+    }
+
+    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
+        self.current()
+            .map_or(Ok(()), |e| e.shutdown_with_timeout(timeout))
+    }
+
+    fn set_resource(&mut self, resource: &Resource) {
+        self.0
+            .write()
+            .unwrap_or_else(PoisonError::into_inner)
+            .resource = Some(resource.clone());
+    }
+}
+
+/// `sample_ratio`, as it is now: a span follows its parent's decision, a root is sampled at the
+/// ratio by its trace id.
+#[derive(Debug, Clone, Default)]
+struct Ratio(Arc<AtomicU64>);
+
+impl Ratio {
+    fn get(&self) -> f64 {
+        f64::from_bits(self.0.load(Ordering::Relaxed))
+    }
+}
+
+impl ShouldSample for Ratio {
+    fn should_sample(
+        &self,
+        parent: Option<&Context>,
+        trace_id: TraceId,
+        name: &str,
+        kind: &SpanKind,
+        attributes: &[KeyValue],
+        links: &[Link],
+    ) -> SamplingResult {
+        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(self.get())))
+            .should_sample(parent, trace_id, name, kind, attributes, links)
+    }
+}
+
+/// The process's traces: exporting nothing until [`Traces::set`] gives them an endpoint.
+#[derive(Debug)]
+pub struct Traces {
+    provider: SdkTracerProvider,
+    switch: Switch,
+    ratio: Ratio,
+    /// The endpoint in force.
+    endpoint: RwLock<Option<String>>,
+}
+
+impl Traces {
+    /// `instance` is the node (`service.instance.id`).
+    pub fn new(instance: &str) -> Self {
+        let switch = Switch::default();
+        let ratio = Ratio::default();
+        let resource = Resource::builder()
+            .with_service_name("data-avalanche")
+            .with_attributes([
+                KeyValue::new("service.instance.id", instance.to_owned()),
+                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
+            ])
+            .build();
+        let provider = SdkTracerProvider::builder()
+            .with_batch_exporter(switch.clone())
+            .with_sampler(ratio.clone())
+            .with_resource(resource)
+            .build();
+        Self {
+            provider,
+            switch,
+            ratio,
+            endpoint: RwLock::default(),
+        }
+    }
+
+    /// Puts `globals.tracing` in force: spans go to `endpoint`'s `/v1/traces`, `sample_ratio` of
+    /// the traces; `None` exports nothing. The same endpoint again keeps its exporter. Returns
+    /// whether traces are exported now.
+    ///
+    /// # Errors
+    /// [`TracesError::Endpoint`]: `endpoint` isn't a URL the exporter takes; what was in force
+    /// stays.
+    pub fn set(&self, endpoint: Option<&str>, sample_ratio: f64) -> Result<bool, TracesError> {
+        let mut current = self
+            .endpoint
+            .write()
+            .unwrap_or_else(PoisonError::into_inner);
+        if current.as_deref() != endpoint {
+            let exporter = endpoint.map(|e| self.exporter(e)).transpose()?;
+            let old = std::mem::replace(
+                &mut self
+                    .switch
+                    .0
+                    .write()
+                    .unwrap_or_else(PoisonError::into_inner)
+                    .exporter,
+                exporter.map(Arc::new),
+            );
+            // Its blocking client can't be dropped on an async runtime's thread.
+            if let Some(old) = old {
+                std::thread::spawn(move || drop(old));
+            }
+            *current = endpoint.map(str::to_owned);
+        }
+        let ratio = if endpoint.is_some() {
+            sample_ratio
+        } else {
+            0.0
+        };
+        self.ratio.0.store(ratio.to_bits(), Ordering::Relaxed);
+        Ok(endpoint.is_some())
+    }
+
+    fn exporter(&self, endpoint: &str) -> Result<opentelemetry_otlp::SpanExporter, TracesError> {
+        let url = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
+        let mut exporter = opentelemetry_otlp::SpanExporter::builder()
+            .with_http()
+            .with_endpoint(url)
+            .build()
+            .map_err(|e| TracesError::Endpoint {
+                endpoint: endpoint.to_owned(),
+                reason: e.to_string(),
+            })?;
+        let resource = self
+            .switch
+            .0
+            .read()
+            .unwrap_or_else(PoisonError::into_inner)
+            .resource
+            .clone();
+        if let Some(resource) = resource {
+            exporter.set_resource(&resource);
+        }
+        Ok(exporter)
+    }
+
+    /// The `tracing` layer that turns spans into this provider's.
+    pub fn layer<S>(&self) -> impl Layer<S> + Send + Sync + use<S>
+    where
+        S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
+    {
+        tracing_opentelemetry::layer().with_tracer(self.provider.tracer("data-avalanche"))
+    }
+
+    /// Exports the spans queued now; it blocks until they are, so it runs off an async runtime's
+    /// threads.
+    pub fn flush(&self) {
+        if let Err(e) = self.provider.force_flush() {
+            tracing::warn!("traces weren't exported: {e}");
+        }
+    }
+
+    /// Exports the spans still queued, then stops; it blocks up to a few seconds, so it runs
+    /// off an async runtime's threads (`spawn_blocking`). A failed last export is only logged.
+    pub fn shutdown(&self) {
+        if let Err(e) = self.provider.shutdown_with_timeout(SHUTDOWN) {
+            tracing::warn!("the last traces weren't exported: {e}");
+        }
+    }
+}
+
+/// The trace and span ids of `span`, in hex, when a [`Traces::layer`] in `dispatch` sampled it.
+pub fn trace_ids(span: &span::Id, dispatch: &Dispatch) -> Option<(String, String)> {
+    let cx = tracing_opentelemetry::get_otel_context(span, dispatch)?;
+    let span = cx.span();
+    let sc = span.span_context();
+    (sc.is_valid() && sc.is_sampled())
+        .then(|| (sc.trace_id().to_string(), sc.span_id().to_string()))
+}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the two commands of Step 2.

Expected: PASS: `traces` 5 tests; the pipeline's 3.

Run: `cargo test --workspace`

Expected: PASS (1311 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/data-avalanche-telemetry crates/data-avalanche-pipeline
git commit -m "feat(telemetry): OTLP/HTTP traces — one provider, its exporter swapped and its ratio set at run time; the pipeline's flush, commit, chunk and schema spans"
```

---

### Task 7: The binary's logger (`data-avalanche`)

**Files:**
- Modify: `crates/data-avalanche/src/{logging,run}.rs`
- Test: the tests of `logging.rs`; `crates/data-avalanche-testkit/src/{otlp,lib}.rs` (`otlp` new), `crates/data-avalanche-telemetry/{Cargo.toml,tests/traces.rs}`, `crates/data-avalanche/tests/e2e/mysql.rs`, `Cargo.lock`

**Interfaces:**
- Consumes: Task 6's `Traces`, `trace_ids`; `Tracing { otlp_endpoint, sample_ratio }` (M1).
- Produces:
  - `logging::build(log, node, args, writer, ansi: bool) -> Result<(Dispatch, Logger), String>`
    and `init(log, node, args) -> Result<Logger, String>` (crate-private);
  - `Logger::trace(&self, tracing: &Tracing)`: at start and at each generation;
    `Logger::finish(&self)` (async): the traces' shutdown at exit;
  - in the testkit: `otlp_collector() -> (String, mpsc::Receiver<OtlpRequest>)`,
    `OtlpRequest { path, body }` with `has(&self, text: &str) -> bool`.

D-M10.12–14. The node's identity is resolved before the logger is built (the traces carry it).
`run` calls `trace` after the join and from the coordinator's globals callback, and `finish`
before it returns. The format layers are `Box<dyn Layer<Registry>>`, each filtered per layer, in a
`Vec`.

- [ ] **Step 1: Write the failing tests**

The fake collector moves from the telemetry tests to the testkit, which both crates' tests use.
The E2E suite's `plain()` goes: its log isn't in colour (D-M10.14).

`Cargo.lock`:

```diff
diff --git a/Cargo.lock b/Cargo.lock
index 9bb7212..c301659 100644
--- a/Cargo.lock
+++ b/Cargo.lock
@@ -1954,6 +1954,7 @@ dependencies = [
 name = "data-avalanche-telemetry"
 version = "0.1.0"
 dependencies = [
+ "data-avalanche-testkit",
  "opentelemetry",
  "opentelemetry-otlp",
  "opentelemetry_sdk",
```

`crates/data-avalanche-telemetry/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-telemetry/Cargo.toml b/crates/data-avalanche-telemetry/Cargo.toml
index 748c544..12100b0 100644
--- a/crates/data-avalanche-telemetry/Cargo.toml
+++ b/crates/data-avalanche-telemetry/Cargo.toml
@@ -20,4 +20,5 @@ tracing-opentelemetry.workspace = true
 tracing-subscriber = { workspace = true, features = ["registry"] }
 
 [dev-dependencies]
+data-avalanche-testkit.workspace = true
 tokio = { workspace = true, features = ["rt", "macros", "net", "io-util", "time"] }
```

`crates/data-avalanche-telemetry/tests/traces.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/traces.rs b/crates/data-avalanche-telemetry/tests/traces.rs
index 04d355d..df8d992 100644
--- a/crates/data-avalanche-telemetry/tests/traces.rs
+++ b/crates/data-avalanche-telemetry/tests/traces.rs
@@ -1,53 +1,12 @@
 //! The OTLP/HTTP export (config §6.6): a sampled span reaches the collector's `/v1/traces`; one
 //! not sampled doesn't; a span's ids are known only when it is sampled.
 
-use std::io::{BufRead, BufReader, Read, Write};
-use std::net::TcpListener;
-use std::sync::mpsc;
 use std::time::Duration;
 
 use data_avalanche_telemetry::{Traces, trace_ids};
+use data_avalanche_testkit::otlp_collector as collector;
 use tracing_subscriber::layer::SubscriberExt;
 
-/// A collector on a thread of its own: each request's path and body, answered 200.
-fn collector() -> (String, mpsc::Receiver<(String, Vec<u8>)>) {
-    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
-    let url = format!("http://{}", listener.local_addr().unwrap());
-    let (tx, rx) = mpsc::channel();
-    std::thread::spawn(move || {
-        for conn in listener.incoming() {
-            let mut conn = conn.unwrap();
-            let mut reader = BufReader::new(conn.try_clone().unwrap());
-            let mut line = String::new();
-            reader.read_line(&mut line).unwrap();
-            let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
-            let mut length = 0;
-            loop {
-                let mut header = String::new();
-                reader.read_line(&mut header).unwrap();
-                if header == "\r\n" {
-                    break;
-                }
-                if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
-                    length = v.trim().parse().unwrap();
-                }
-            }
-            let mut body = vec![0; length];
-            reader.read_exact(&mut body).unwrap();
-            conn.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
-                .unwrap();
-            let _ = tx.send((path, body));
-        }
-    });
-    (url, rx)
-}
-
-fn contains(haystack: &[u8], needle: &str) -> bool {
-    haystack
-        .windows(needle.len())
-        .any(|w| w == needle.as_bytes())
-}
-
 /// Runs a `pipeline.flush` span with a `sink.commit` inside it under `traces`, and returns the
 /// flush's ids as the layer saw them.
 fn flush(traces: &Traces) -> Option<(String, String)> {
@@ -70,8 +29,8 @@ fn a_sampled_flush_is_exported_to_the_collector() {
     assert!(traces.set(Some(&format!("{url}/")), 1.0).unwrap());
     let ids = flush(&traces);
     traces.shutdown();
-    let (path, body) = requests.recv_timeout(Duration::from_secs(10)).unwrap();
-    assert_eq!(path, "/v1/traces");
+    let request = requests.recv_timeout(Duration::from_secs(10)).unwrap();
+    assert_eq!(request.path, "/v1/traces");
     for text in [
         "pipeline.flush",
         "sink.commit",
@@ -79,7 +38,7 @@ fn a_sampled_flush_is_exported_to_the_collector() {
         "data-avalanche",
         "node-1",
     ] {
-        assert!(contains(&body, text), "{text}");
+        assert!(request.has(text), "{text}");
     }
     let (trace, span) = ids.expect("a sampled span has ids");
     assert_eq!((trace.len(), span.len()), (32, 16));
```

`crates/data-avalanche-testkit/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/lib.rs b/crates/data-avalanche-testkit/src/lib.rs
index 88ac16a..8fdbd10 100644
--- a/crates/data-avalanche-testkit/src/lib.rs
+++ b/crates/data-avalanche-testkit/src/lib.rs
@@ -6,8 +6,10 @@ use std::sync::Arc;
 
 pub mod fake_source;
 pub mod memory_sink;
+pub mod otlp;
 pub use fake_source::{FakeSource, FakeTxn, Method};
 pub use memory_sink::{MemorySink, SinkOp};
+pub use otlp::{OtlpRequest, otlp_collector};
 
 use futures_util::TryStreamExt;
 use object_store::ObjectStore;
```

`crates/data-avalanche-testkit/src/otlp.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/otlp.rs b/crates/data-avalanche-testkit/src/otlp.rs
new file mode 100644
index 0000000..a1c1d2e
--- /dev/null
+++ b/crates/data-avalanche-testkit/src/otlp.rs
@@ -0,0 +1,61 @@
+//! A fake OTLP/HTTP collector (config §6.6), on a thread of its own: it answers every request
+//! 200 and hands over its path and body.
+
+use std::io::{BufRead, BufReader, Read, Write};
+use std::net::TcpListener;
+use std::sync::mpsc;
+
+/// One export the collector took.
+#[derive(Debug, Clone)]
+pub struct OtlpRequest {
+    pub path: String,
+    /// The protobuf body: span names and attribute values are in it verbatim.
+    pub body: Vec<u8>,
+}
+
+impl OtlpRequest {
+    /// Whether `text` is in the body.
+    #[must_use]
+    pub fn has(&self, text: &str) -> bool {
+        self.body.windows(text.len()).any(|w| w == text.as_bytes())
+    }
+}
+
+/// A collector on `127.0.0.1`: its base URL and the requests it takes.
+///
+/// # Panics
+/// When it can't bind a port, or a request isn't HTTP/1.1 with a `content-length`.
+#[must_use]
+pub fn otlp_collector() -> (String, mpsc::Receiver<OtlpRequest>) {
+    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
+    let url = format!("http://{}", listener.local_addr().unwrap());
+    let (tx, rx) = mpsc::channel();
+    std::thread::spawn(move || {
+        for conn in listener.incoming() {
+            let mut conn = conn.unwrap();
+            let mut reader = BufReader::new(conn.try_clone().unwrap());
+            let mut line = String::new();
+            reader.read_line(&mut line).unwrap();
+            let path = line.split(' ').nth(1).unwrap_or_default().to_owned();
+            let mut length = 0;
+            loop {
+                let mut header = String::new();
+                reader.read_line(&mut header).unwrap();
+                if header == "\r\n" {
+                    break;
+                }
+                if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
+                    length = v.trim().parse().unwrap();
+                }
+            }
+            let mut body = vec![0; length];
+            reader.read_exact(&mut body).unwrap();
+            conn.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
+                .unwrap();
+            if tx.send(OtlpRequest { path, body }).is_err() {
+                return;
+            }
+        }
+    });
+    (url, rx)
+}
```

`crates/data-avalanche/src/logging.rs`:

```diff
diff --git a/crates/data-avalanche/src/logging.rs b/crates/data-avalanche/src/logging.rs
index 36b34f8..a94978a 100644
--- a/crates/data-avalanche/src/logging.rs
+++ b/crates/data-avalanche/src/logging.rs
@@ -174,6 +174,7 @@ impl Logger {
 #[cfg(test)]
 mod tests {
     use std::path::PathBuf;
+    use std::time::Duration;
 
     use data_avalanche_config::LogFilter;
 
@@ -284,13 +285,15 @@ mod tests {
     #[test]
     fn a_generation_changes_the_level_and_the_format() {
         let buf = Buf::default();
-        let (subscriber, logger) = build(
+        let (dispatch, logger) = build(
             &log("info", LogFormat::Compact),
+            "n1",
             &args(None, None),
             buf.clone(),
+            false,
         )
         .unwrap();
-        tracing::subscriber::with_default(subscriber, || {
+        tracing::dispatcher::with_default(&dispatch, || {
             tracing::debug!("before");
             logger.apply(&log("debug", LogFormat::Json));
             tracing::debug!("after");
@@ -306,9 +309,15 @@ mod tests {
     fn the_command_line_wins_over_a_generation() {
         let buf = Buf::default();
         let cli = args(Some("info"), Some(LogFormatArg::Compact));
-        let (subscriber, logger) =
-            build(&log("info", LogFormat::Compact), &cli, buf.clone()).unwrap();
-        tracing::subscriber::with_default(subscriber, || {
+        let (dispatch, logger) = build(
+            &log("info", LogFormat::Compact),
+            "n1",
+            &cli,
+            buf.clone(),
+            false,
+        )
+        .unwrap();
+        tracing::dispatcher::with_default(&dispatch, || {
             logger.apply(&log("debug", LogFormat::Json));
             tracing::debug!("quiet");
             tracing::info!("loud");
@@ -321,4 +330,137 @@ mod tests {
             "{loud:?}"
         );
     }
+
+    /// The traces take the pipeline's spans once an endpoint is set, whatever the log level:
+    /// none before it, and none of the libraries'.
+    #[test]
+    fn the_pipeline_s_spans_are_exported_once_an_endpoint_is_set() {
+        let (url, requests) = data_avalanche_testkit::otlp_collector();
+        let (dispatch, logger) = build(
+            &log("warn", LogFormat::Compact),
+            "n1",
+            &args(None, None),
+            Buf::default(),
+            false,
+        )
+        .unwrap();
+        tracing::dispatcher::with_default(&dispatch, || {
+            tracing::info_span!("before.endpoint").in_scope(|| {});
+            logger.trace(&Tracing {
+                otlp_endpoint: Some(url.clone()),
+                sample_ratio: 1.0,
+            });
+            tracing::info_span!(target: "deltalake_core", "a.library.span").in_scope(|| {});
+            tracing::info_span!("after.endpoint").in_scope(|| {});
+        });
+        logger.traces.shutdown();
+        let request = requests.recv_timeout(Duration::from_secs(10)).unwrap();
+        assert_eq!(request.path, "/v1/traces");
+        assert!(request.has("after.endpoint"));
+        assert!(request.has("n1"), "service.instance.id");
+        assert!(!request.has("before.endpoint"));
+        assert!(!request.has("a.library.span"));
+    }
+
+    /// The JSON lines a sampled `pipeline.flush` logs in, and one logged outside any span.
+    fn correlated_lines(endpoint: Option<String>) -> (Buf, Logger) {
+        let buf = Buf::default();
+        let (dispatch, logger) = build(
+            &log("info", LogFormat::Json),
+            "n1",
+            &args(None, None),
+            buf.clone(),
+            false,
+        )
+        .unwrap();
+        tracing::dispatcher::with_default(&dispatch, || {
+            logger.trace(&Tracing {
+                otlp_endpoint: endpoint,
+                sample_ratio: 1.0,
+            });
+            tracing::info_span!("pipeline.flush", pipeline = "orders").in_scope(|| {
+                tracing::info_span!("sink.commit", table = "public.a").in_scope(|| {
+                    tracing::warn!("in the commit");
+                });
+                tracing::warn!(pipeline = "named", "naming its own");
+            });
+            tracing::warn!("outside");
+        });
+        (buf, logger)
+    }
+
+    fn line(buf: &Buf, what: &str) -> serde_json::Value {
+        let lines = buf.lines(what);
+        assert_eq!(lines.len(), 1, "{lines:?}");
+        serde_json::from_str(&lines[0]).unwrap()
+    }
+
+    /// Config §6.6 and Observability research §4: a JSON line logged in a sampled span carries
+    /// the span's `trace_id` and `span_id`, and the `pipeline` of the spans it is in unless it
+    /// names its own; a line outside every span carries neither.
+    #[test]
+    fn a_json_line_in_a_sampled_span_carries_its_ids_and_its_pipeline() {
+        let (url, _requests) = data_avalanche_testkit::otlp_collector();
+        let (buf, logger) = correlated_lines(Some(url));
+        logger.traces.shutdown();
+        let inside = line(&buf, "in the commit");
+        let (trace, span) = (inside["trace_id"].as_str(), inside["span_id"].as_str());
+        assert_eq!(trace.map(str::len), Some(32), "{inside}");
+        assert_eq!(span.map(str::len), Some(16), "{inside}");
+        assert_eq!(inside["fields"]["pipeline"], "orders");
+        let named = line(&buf, "naming its own");
+        assert_eq!(named["fields"]["pipeline"], "named");
+        assert_eq!(named["trace_id"].as_str(), trace, "the same trace");
+        assert_ne!(named["span_id"].as_str(), span, "the flush, not the commit");
+        let outside = line(&buf, "outside");
+        assert!(outside.get("trace_id").is_none(), "{outside}");
+        assert!(outside["fields"].get("pipeline").is_none(), "{outside}");
+    }
+
+    /// Without an endpoint nothing is sampled: no ids, but the `pipeline` still.
+    #[test]
+    fn without_traces_a_json_line_carries_its_pipeline_but_no_ids() {
+        let (buf, _logger) = correlated_lines(None);
+        let inside = line(&buf, "in the commit");
+        assert!(inside.get("trace_id").is_none(), "{inside}");
+        assert!(inside.get("span_id").is_none(), "{inside}");
+        assert_eq!(inside["fields"]["pipeline"], "orders");
+    }
+
+    /// DA-OBS-6: the text logs carry no escape sequences unless they are in colour.
+    #[test]
+    fn a_text_log_is_in_colour_only_when_asked() {
+        for (ansi, format) in [
+            (false, LogFormat::Compact),
+            (false, LogFormat::Pretty),
+            (true, LogFormat::Compact),
+        ] {
+            let buf = Buf::default();
+            let (dispatch, _logger) = build(
+                &log("info", format),
+                "n1",
+                &args(None, None),
+                buf.clone(),
+                ansi,
+            )
+            .unwrap();
+            tracing::dispatcher::with_default(&dispatch, || {
+                tracing::warn!(code = "X", "coloured?")
+            });
+            let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
+            assert_eq!(text.contains("\x1b["), ansi, "{format}: {text:?}");
+        }
+    }
+
+    #[test]
+    fn colour_needs_a_terminal_and_no_no_color() {
+        use std::ffi::OsStr;
+        assert!(coloured(true, None));
+        assert!(
+            coloured(true, Some(OsStr::new(""))),
+            "an empty NO_COLOR is unset"
+        );
+        assert!(!coloured(true, Some(OsStr::new("1"))));
+        assert!(!coloured(false, None));
+    }
 }
```

`crates/data-avalanche/tests/e2e/mysql.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/mysql.rs b/crates/data-avalanche/tests/e2e/mysql.rs
index df4bc82..0b6e7c5 100644
--- a/crates/data-avalanche/tests/e2e/mysql.rs
+++ b/crates/data-avalanche/tests/e2e/mysql.rs
@@ -13,34 +13,13 @@ use crate::source::{Db, Source, target};
 const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";
 const COLS: &[&str] = &["id", "doc"];
 
-/// `text` without its ANSI colour sequences (`ESC [ … m`): the binary colours its log.
-fn plain(text: &str) -> String {
-    let mut out = String::with_capacity(text.len());
-    let mut chars = text.chars();
-    while let Some(c) = chars.next() {
-        if c == '\u{1b}' {
-            chars.by_ref().find(|&c| c == 'm');
-        } else {
-            out.push(c);
-        }
-    }
-    out
-}
-
-/// Whether the log has the line of a pipeline in `ERROR` with `code` (Q-M7b-4).
+/// Whether the log has the line of a pipeline in `ERROR` with `code` (Q-M7b-4); the log isn't
+/// in colour, stderr being no terminal (DA-OBS-6).
 fn halted_with(log: &str, code: &str) -> bool {
-    plain(log)
-        .lines()
+    log.lines()
         .any(|l| l.contains("pipeline in ERROR") && l.contains(&format!("code=\"{code}\"")))
 }
 
-#[test]
-fn a_halt_line_is_found_through_its_colours() {
-    let line = "\u{1b}[31mERROR\u{1b}[0m pipeline in ERROR: X_Y: z \u{1b}[3mcode\u{1b}[0m\u{1b}[2m=\u{1b}[0m\"X_Y\"";
-    assert!(halted_with(line, "X_Y"));
-    assert!(!halted_with(line, "X_Z"));
-}
-
 /// A compressed transaction (MySQL's `binlog_transaction_compression`) whose payload the wire
 /// corrupted halts the pipeline with `BINLOG_PAYLOAD_INVALID`: status `error`, the code in the
 /// log, and nothing of the transaction in the lake. A restart without the fault reads the
@@ -191,8 +170,7 @@ async fn a_purged_checkpoint_halts_under_error() {
         log.contains("binlog_purge_action is error, so the pipeline halts"),
         "{log}"
     );
-    let text = plain(&log);
-    let halt = text.lines().find(|l| l.contains("pipeline in ERROR"));
+    let halt = log.lines().find(|l| l.contains("pipeline in ERROR"));
     assert!(halt.is_some_and(|l| !l.contains("code=")), "{log}");
     assert_eq!(e.ids("items").await, [1, 2]);
     assert!(p.terminate().await.success());
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche --lib logging::`

Expected: FAIL to compile: `build` takes 3 arguments but 5 are given (E0061, with the E0277 and E0308
that follow from it); `coloured` isn't found (E0425); `Logger` has no field `traces` (E0609) and no
method `trace` (E0599); `Tracing` isn't in scope (E0422).

- [ ] **Step 3: Implement**

`crates/data-avalanche/src/logging.rs`:

```diff
diff --git a/crates/data-avalanche/src/logging.rs b/crates/data-avalanche/src/logging.rs
index a94978a..550146d 100644
--- a/crates/data-avalanche/src/logging.rs
+++ b/crates/data-avalanche/src/logging.rs
@@ -1,13 +1,20 @@
 //! Logs: `globals.log` (level, format), each overridden by the command line, and changed by a
-//! config generation (config §1.2: R).
-
-use std::sync::{Mutex, PoisonError};
-
-use data_avalanche_config::{Log, LogFormat};
-use tracing::Subscriber;
-use tracing_subscriber::filter::LevelFilter;
-use tracing_subscriber::fmt::MakeWriter;
-use tracing_subscriber::layer::{Layered, SubscriberExt};
+//! config generation (config §1.2: R). Traces: `globals.tracing`, changed the same way
+//! (D-M10.6). The log level filters the log alone: the traces' layer has a filter of its own,
+//! which passes the pipeline's spans while an endpoint is set and nothing otherwise.
+
+use std::sync::{Arc, Mutex, OnceLock, PoisonError};
+
+use data_avalanche_config::{Log, LogFormat, Tracing};
+use data_avalanche_telemetry::{Traces, trace_ids};
+use serde_json::Value;
+use tracing::dispatcher::WeakDispatch;
+use tracing::{Dispatch, Event, Level, Subscriber};
+use tracing_subscriber::filter::{LevelFilter, Targets};
+use tracing_subscriber::fmt::format::{Format as EventFormat, Json, Writer};
+use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, MakeWriter};
+use tracing_subscriber::layer::SubscriberExt;
+use tracing_subscriber::registry::LookupSpan;
 use tracing_subscriber::{EnvFilter, Layer, Registry, reload};
 
 use crate::{LogFormatArg, RunArgs};
@@ -64,18 +71,29 @@ pub(crate) fn quieted(filter: &str) -> String {
     out
 }
 
-type Filtered = Layered<reload::Layer<EnvFilter, Registry>, Registry>;
-type Format = Box<dyn Layer<Filtered> + Send + Sync>;
+type Format = Box<dyn Layer<Registry> + Send + Sync>;
+
+/// The spans the traces take while an endpoint is set: the pipeline's flushes, commits, chunk
+/// reads and schema changes, whatever the log level.
+fn traced() -> Targets {
+    Targets::new().with_target("data_avalanche", Level::INFO)
+}
 
 /// The installed logger: its filter and its format change with each config generation's
-/// `globals.log`, except where the command line set them.
+/// `globals.log`, except where the command line set them; its traces follow
+/// `globals.tracing`.
 pub(crate) struct Logger {
     filter: reload::Handle<EnvFilter, Registry>,
-    format: reload::Handle<Format, Filtered>,
+    format: reload::Handle<Format, Registry>,
     layer: Box<dyn Fn(LogFormat) -> Format + Send + Sync>,
     args: RunArgs,
     /// The level and the format in force.
     current: Mutex<(String, LogFormat)>,
+    traces: Arc<Traces>,
+    /// The traces' filter: [`traced`] with an endpoint, nothing without.
+    spans: reload::Handle<Targets, Registry>,
+    /// `globals.tracing` in force.
+    tracing: Mutex<Tracing>,
 }
 
 impl std::fmt::Debug for Logger {
@@ -92,54 +110,145 @@ fn filter(level: &str) -> Result<EnvFilter, String> {
         .map_err(|e| format!("{level:?} isn't a log filter: {e}"))
 }
 
-fn layer<W>(format: LogFormat, writer: W) -> Format
+/// The process's dispatch, once built: the JSON format reads a span's trace ids through it.
+/// Inside an event a scoped dispatch isn't the current one any more, so it is held, weakly.
+type Process = Arc<OnceLock<WeakDispatch>>;
+
+fn layer<W>(format: LogFormat, writer: W, ansi: bool, process: &Process) -> Format
 where
     W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
 {
-    let builder = tracing_subscriber::fmt::layer().with_writer(writer);
+    let builder = tracing_subscriber::fmt::layer()
+        .with_writer(writer)
+        .with_ansi(ansi);
     match format {
-        LogFormat::Json => builder.json().boxed(),
+        LogFormat::Json => builder
+            .json()
+            .event_format(Correlated {
+                json: tracing_subscriber::fmt::format().json(),
+                process: Arc::clone(process),
+            })
+            .boxed(),
         LogFormat::Compact => builder.compact().boxed(),
         LogFormat::Pretty => builder.pretty().boxed(),
     }
 }
 
-/// The process's subscriber, writing to `writer`, and its [`Logger`].
+/// The process's dispatch, writing to `writer` (in colour when `ansi`), and its [`Logger`]; the
+/// traces carry `node` (`service.instance.id`) and export nothing until [`Logger::trace`].
 ///
 /// # Errors
 /// A `--log-level` that isn't a filter (the config's was checked by V-017).
 pub(crate) fn build<W>(
     log: &Log,
+    node: &str,
     args: &RunArgs,
     writer: W,
-) -> Result<(impl Subscriber + Send + Sync + use<W>, Logger), String>
+    ansi: bool,
+) -> Result<(Dispatch, Logger), String>
 where
     W: for<'w> MakeWriter<'w> + Clone + Send + Sync + 'static,
 {
     let (level, format) = settings(log, args);
+    let process = Process::default();
     let (filter_layer, filter) =
         reload::Layer::new(filter(&level).map_err(|e| format!("--log-level {e}"))?);
-    let (format_layer, format_handle) = reload::Layer::new(layer(format, writer.clone()));
-    let subscriber = tracing_subscriber::registry()
-        .with(filter_layer)
-        .with(format_layer);
+    let (format_layer, format_handle) =
+        reload::Layer::new(layer(format, writer.clone(), ansi, &process));
+    let traces = Arc::new(Traces::new(node));
+    let (spans_filter, spans) = reload::Layer::new(Targets::new());
+    let layers: Vec<Format> = vec![
+        Box::new(format_layer.with_filter(filter_layer)),
+        Box::new(traces.layer().with_filter(spans_filter)),
+    ];
+    let dispatch = Dispatch::new(tracing_subscriber::registry().with(layers));
+    let _ = process.set(dispatch.downgrade());
     let logger = Logger {
         filter,
         format: format_handle,
-        layer: Box::new(move |f| layer(f, writer.clone())),
+        layer: Box::new(move |f| layer(f, writer.clone(), ansi, &process)),
         args: args.clone(),
         current: Mutex::new((level, format)),
+        traces,
+        spans,
+        tracing: Mutex::new(Tracing {
+            otlp_endpoint: None,
+            sample_ratio: 0.0,
+        }),
+    };
+    Ok((dispatch, logger))
+}
+
+/// Whether the text logs are in colour: only on a terminal, and not under `NO_COLOR` set to
+/// anything but empty (DA-OBS-6).
+fn coloured(terminal: bool, no_color: Option<&std::ffi::OsStr>) -> bool {
+    terminal && no_color.is_none_or(std::ffi::OsStr::is_empty)
+}
+
+/// The JSON format, correlated (config §6.6, Observability research §4): a line logged in a
+/// sampled span carries the span's `trace_id` and `span_id`, and one logged in a span with a
+/// `pipeline` field carries it as `fields.pipeline` unless it names its own.
+struct Correlated {
+    json: EventFormat<Json>,
+    process: Process,
+}
+
+impl<S, N> FormatEvent<S, N> for Correlated
+where
+    S: Subscriber + for<'a> LookupSpan<'a>,
+    N: for<'w> FormatFields<'w> + 'static,
+{
+    fn format_event(
+        &self,
+        ctx: &FmtContext<'_, S, N>,
+        mut writer: Writer<'_>,
+        event: &Event<'_>,
+    ) -> std::fmt::Result {
+        let mut line = String::new();
+        self.json.format_event(ctx, Writer::new(&mut line), event)?;
+        let ids = ctx.parent_span().map(|s| s.id()).and_then(|id| {
+            let process = self.process.get()?.upgrade()?;
+            trace_ids(&id, &process)
+        });
+        match correlated(&line, ids) {
+            Some(out) => writeln!(writer, "{out}"),
+            None => writer.write_str(&line),
+        }
+    }
+}
+
+/// `line`, a JSON log line, with the ids and the spans' `pipeline` added; `None` when it isn't
+/// an object.
+fn correlated(line: &str, ids: Option<(String, String)>) -> Option<String> {
+    let Ok(Value::Object(mut map)) = serde_json::from_str::<Value>(line) else {
+        return None;
     };
-    Ok((subscriber, logger))
+    let pipeline = map
+        .get("spans")
+        .and_then(Value::as_array)
+        .and_then(|spans| spans.iter().rev().find_map(|s| s.get("pipeline")).cloned());
+    if let (Some(pipeline), Some(Value::Object(fields))) = (pipeline, map.get_mut("fields")) {
+        fields.entry("pipeline").or_insert(pipeline);
+    }
+    if let Some((trace, span)) = ids {
+        map.insert("trace_id".to_owned(), trace.into());
+        map.insert("span_id".to_owned(), span.into());
+    }
+    Some(Value::Object(map).to_string())
 }
 
-/// Installs the process's subscriber; logs go to stderr.
+/// Installs the process's dispatch; logs go to stderr, in colour on a terminal.
 ///
 /// # Errors
 /// A `--log-level` that isn't a filter, or a subscriber is installed already.
-pub(crate) fn init(log: &Log, args: &RunArgs) -> Result<Logger, String> {
-    let (subscriber, logger) = build(log, args, std::io::stderr)?;
-    tracing::subscriber::set_global_default(subscriber)
+pub(crate) fn init(log: &Log, node: &str, args: &RunArgs) -> Result<Logger, String> {
+    use std::io::IsTerminal as _;
+    let ansi = coloured(
+        std::io::stderr().is_terminal(),
+        std::env::var_os("NO_COLOR").as_deref(),
+    );
+    let (dispatch, logger) = build(log, node, args, std::io::stderr, ansi)?;
+    tracing::dispatcher::set_global_default(dispatch)
         .map_err(|e| format!("cannot install the logger: {e}"))?;
     Ok(logger)
 }
@@ -169,6 +278,40 @@ impl Logger {
             }
         }
     }
+
+    /// Takes `tracing`, a config generation's (config §6.6: R). An endpoint that can't be
+    /// exported to keeps what was in force, with a warning.
+    pub fn trace(&self, tracing: &Tracing) {
+        let mut current = self.tracing.lock().unwrap_or_else(PoisonError::into_inner);
+        if *current == *tracing {
+            return;
+        }
+        let endpoint = tracing.otlp_endpoint.as_deref();
+        match self.traces.set(endpoint, tracing.sample_ratio) {
+            Ok(on) => {
+                let spans = if on { traced() } else { Targets::new() };
+                if let Err(e) = self.spans.reload(spans) {
+                    tracing::warn!("the traces' filter stays as it was: {e}");
+                }
+                match endpoint {
+                    Some(e) => tracing::info!(
+                        "traces go to {e} (OTLP/HTTP), {} of them sampled",
+                        tracing.sample_ratio
+                    ),
+                    None if current.otlp_endpoint.is_some() => tracing::info!("traces are off"),
+                    None => {}
+                }
+                *current = tracing.clone();
+            }
+            Err(e) => tracing::warn!("the traces stay as they were: {e}"),
+        }
+    }
+
+    /// Exports the traces still queued; at the process's end.
+    pub async fn finish(&self) {
+        let traces = Arc::clone(&self.traces);
+        let _ = tokio::task::spawn_blocking(move || traces.shutdown()).await;
+    }
 }
 
 #[cfg(test)]
```

`crates/data-avalanche/src/run.rs`:

```diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index afee88a..53d3e55 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -104,7 +104,11 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     // 1. A first load with a throwaway key: only globals.state and the cluster settings matter.
     let first = load_str(&text, &ProcessEnv, &FingerprintKey::random())
         .map_err(|r| Fatal::Config(render(&r)))?;
-    let logger = Arc::new(logging::init(&first.config.globals.log, &args).map_err(Fatal::Config)?);
+    let holder = node_identity(&first.config.globals.cluster, started)
+        .map_err(|e| Fatal::Config(e.to_string()))?;
+    let logger = Arc::new(
+        logging::init(&first.config.globals.log, &holder.node_id, &args).map_err(Fatal::Config)?,
+    );
     for w in &first.warnings {
         tracing::warn!("config: {}", line(w));
     }
@@ -132,12 +136,11 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             let _ = http_stopped.await;
         },
     ));
-    let holder = node_identity(&first.config.globals.cluster, started)
-        .map_err(|e| Fatal::Config(e.to_string()))?;
     let buckets = Arc::new(Buckets::new(&first.config.globals));
     let joined = join(&buckets, &holder.node_id, &text, &first.config).await?;
     let config = joined.config;
     logger.apply(&config.globals.log);
+    logger.trace(&config.globals.tracing);
     auth.set(&config.globals.api);
     tracing::info!(
         node = %holder.node_id,
@@ -222,6 +225,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
             metrics: metrics.clone(),
             globals: Arc::new(move |g| {
                 log.apply(&g.log);
+                log.trace(&g.tracing);
                 reloaded_auth.set(&g.api);
             }),
         },
@@ -266,6 +270,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
     let _ = stop_http.send(());
     let _ = server.await;
     tracing::info!("stopped");
+    logger.finish().await;
     Ok(())
 }
 
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche --lib logging:: && cargo test -p data-avalanche-telemetry --test traces`

Expected: PASS: `logging::` 11 tests, `traces` 5.

Run, with `test-infra/up.sh` running: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql cargo test -p data-avalanche --features e2e-hooks --test e2e -- mysql:: --test-threads=1`

Expected: PASS, 5 tests on `mysql-84` (`halted_with` finds the `code` field in the log as written).

Run: `cargo test --workspace`

Expected: PASS (1316 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock crates/data-avalanche crates/data-avalanche-testkit crates/data-avalanche-telemetry
git commit -m "feat(data-avalanche): traces follow globals.tracing; JSON logs carry trace_id, span_id and the pipeline; colour only on a terminal (DA-OBS-6)"
```

---

### Task 8: The example alert rules (`deploy/prometheus`)

**Files:**
- Create: `deploy/prometheus/alerts.yaml`
- Modify: `.github/workflows/ci.yml`
- Test: `deploy/prometheus/alerts.test.yaml` (new), `crates/data-avalanche-telemetry/tests/catalogue.rs`

**Interfaces:**
- Consumes: Task 1's `CATALOGUE` (in `catalogue.rs`).
- Produces: the five rules of D-M10.15 in group `data-avalanche`; CI's `Alert rules` step.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche-telemetry/tests/catalogue.rs`:

```diff
diff --git a/crates/data-avalanche-telemetry/tests/catalogue.rs b/crates/data-avalanche-telemetry/tests/catalogue.rs
index 1df28c7..bd56122 100644
--- a/crates/data-avalanche-telemetry/tests/catalogue.rs
+++ b/crates/data-avalanche-telemetry/tests/catalogue.rs
@@ -179,3 +179,53 @@ fn per_table_adds_the_table_label_to_the_snapshot_series_only() {
         "no other series takes the table label"
     );
 }
+
+/// Each series the text names (`data_avalanche_…`), with the labels its selector matches on.
+fn selectors(text: &str) -> Vec<(String, Vec<String>)> {
+    let mut found = Vec::new();
+    let mut rest = text;
+    while let Some(at) = rest.find("data_avalanche_") {
+        rest = &rest[at..];
+        let end = rest
+            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
+            .unwrap_or(rest.len());
+        let name = rest[..end].to_owned();
+        rest = &rest[end..];
+        let mut labels = Vec::new();
+        if let Some(selector) = rest.strip_prefix('{') {
+            let close = selector.find('}').expect("a closed selector");
+            labels = selector[..close]
+                .split(',')
+                .filter_map(|m| m.split_once('=').map(|(l, _)| l.trim().to_owned()))
+                .collect();
+        }
+        found.push((name, labels));
+    }
+    found
+}
+
+/// The example alert rules (`deploy/prometheus/alerts.yaml`, roadmap M10) name only series of
+/// the catalogue, and match only on the labels those carry; promtool runs them in CI.
+#[test]
+fn the_alert_rules_name_only_catalogued_series_and_labels() {
+    let rules = include_str!(concat!(
+        env!("CARGO_MANIFEST_DIR"),
+        "/../../deploy/prometheus/alerts.yaml"
+    ));
+    let named = selectors(rules);
+    assert_eq!(named.len(), 5, "{named:?}");
+    for (name, labels) in named {
+        let (_, extra) = CATALOGUE
+            .iter()
+            .find(|(n, _)| *n == name)
+            .unwrap_or_else(|| panic!("{name} isn't in the catalogue"));
+        let carried: Vec<&str> = PIPELINE
+            .iter()
+            .chain(extra.unwrap_or_default())
+            .copied()
+            .collect();
+        for label in labels {
+            assert!(carried.contains(&label.as_str()), "{name} has no {label}");
+        }
+    }
+}
```

`deploy/prometheus/alerts.test.yaml`:

```diff
diff --git a/deploy/prometheus/alerts.test.yaml b/deploy/prometheus/alerts.test.yaml
new file mode 100644
index 0000000..4ba24b8
--- /dev/null
+++ b/deploy/prometheus/alerts.test.yaml
@@ -0,0 +1,82 @@
+# promtool test rules deploy/prometheus/alerts.test.yaml (CI: the check job).
+rule_files:
+  - alerts.yaml
+evaluation_interval: 1m
+tests:
+  - interval: 1m
+    input_series:
+      - series: 'data_avalanche_replication_lag_seconds{pipeline="orders",source_host="db1",source_db="shop"}'
+        values: "30 90x10"
+      - series: 'data_avalanche_lease_state{pipeline="orders",source_host="db1",source_db="shop",state="degraded"}'
+        values: "0 1x5"
+      - series: 'data_avalanche_halted_tables{pipeline="orders",source_host="db1",source_db="shop"}'
+        values: "0 2"
+      - series: 'data_avalanche_pipeline_status{pipeline="orders",source_host="db1",source_db="shop",status="error"}'
+        values: "0 1"
+      - series: 'data_avalanche_wal_lag_bytes{pipeline="orders",source_host="db1",source_db="shop"}'
+        values: "1073741824 21474836480x15"
+    alert_rule_test:
+      - eval_time: 5m
+        alertname: DataAvalancheReplicationLag
+        exp_alerts: []
+      - eval_time: 6m
+        alertname: DataAvalancheReplicationLag
+        exp_alerts:
+          - exp_labels:
+              severity: warning
+              pipeline: orders
+              source_host: db1
+              source_db: shop
+            exp_annotations:
+              summary: "orders is 1m 30s behind its source"
+      - eval_time: 2m
+        alertname: DataAvalancheLeaseDegraded
+        exp_alerts: []
+      - eval_time: 3m
+        alertname: DataAvalancheLeaseDegraded
+        exp_alerts:
+          - exp_labels:
+              severity: warning
+              pipeline: orders
+              source_host: db1
+              source_db: shop
+              state: degraded
+            exp_annotations:
+              summary: "orders's lease isn't renewed"
+      - eval_time: 0m
+        alertname: DataAvalancheTablesHalted
+        exp_alerts: []
+      - eval_time: 1m
+        alertname: DataAvalancheTablesHalted
+        exp_alerts:
+          - exp_labels:
+              severity: warning
+              pipeline: orders
+              source_host: db1
+              source_db: shop
+            exp_annotations:
+              summary: "orders has 2 halted tables"
+      - eval_time: 1m
+        alertname: DataAvalanchePipelineError
+        exp_alerts:
+          - exp_labels:
+              severity: critical
+              pipeline: orders
+              source_host: db1
+              source_db: shop
+              status: error
+            exp_annotations:
+              summary: "orders is in ERROR"
+      - eval_time: 10m
+        alertname: DataAvalancheWalLag
+        exp_alerts: []
+      - eval_time: 11m
+        alertname: DataAvalancheWalLag
+        exp_alerts:
+          - exp_labels:
+              severity: warning
+              pipeline: orders
+              source_host: db1
+              source_db: shop
+            exp_annotations:
+              summary: "orders's slot retains 20GiB of WAL"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-telemetry --test catalogue`, then
`docker run --rm -v "$PWD/deploy/prometheus:/rules:ro" --entrypoint promtool prom/prometheus:v3.15.0 test rules /rules/alerts.test.yaml`

Expected: the catalogue test FAILS to compile: ``couldn't read `…/deploy/prometheus/alerts.yaml`: No such
file or directory``; promtool prints `WARNING: no file match pattern /rules/alerts.yaml`, then
`FAILED:` for each of the five alerts (`got:[]`), and exits 1.

- [ ] **Step 3: Implement**

`.github/workflows/ci.yml`:

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index 9c162ca..794fe60 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -34,6 +34,12 @@ jobs:
         run: cargo clippy --locked -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
       - name: Unit tests (e2e-hooks)
         run: cargo test --locked -p data-avalanche --features e2e-hooks --lib
+      # The example alert rules against their promtool tests (M10); the catalogue test checks
+      # the series they name.
+      - name: Alert rules
+        run: >-
+          docker run --rm -v "$PWD/deploy/prometheus:/rules:ro" --entrypoint promtool
+          prom/prometheus:v3.15.0 test rules /rules/alerts.test.yaml
       # RustSec advisory audit of Cargo.lock. Non-blocking by design: a vulnerable dependency is
       # reported (warning annotation + failed step) but never fails the job.
       - name: Install cargo-audit
```

`deploy/prometheus/alerts.yaml`:

```diff
diff --git a/deploy/prometheus/alerts.yaml b/deploy/prometheus/alerts.yaml
new file mode 100644
index 0000000..34b016a
--- /dev/null
+++ b/deploy/prometheus/alerts.yaml
@@ -0,0 +1,49 @@
+# Example Prometheus alert rules for data-avalanche (roadmap M10): a starting point, to be tuned
+# to each deployment's thresholds. Every series is in the mandatory catalogue (System-Overview
+# §Observability catalogue); alerts.test.yaml exercises each rule:
+#   promtool test rules deploy/prometheus/alerts.test.yaml
+groups:
+  - name: data-avalanche
+    rules:
+      # The oldest change read but not yet durable in the sink (D-M10.3).
+      - alert: DataAvalancheReplicationLag
+        expr: data_avalanche_replication_lag_seconds > 60
+        for: 5m
+        labels:
+          severity: warning
+        annotations:
+          summary: "{{ $labels.pipeline }} is {{ $value | humanizeDuration }} behind its source"
+
+      # A renewal failed and the lease runs on its safety deadline (Clustering §6).
+      - alert: DataAvalancheLeaseDegraded
+        expr: data_avalanche_lease_state{state="degraded"} == 1
+        for: 2m
+        labels:
+          severity: warning
+        annotations:
+          summary: "{{ $labels.pipeline }}'s lease isn't renewed"
+
+      # A table stopped by an unsupported schema change or an unknown type; the pipeline goes on
+      # without it (Data-Flow §halt).
+      - alert: DataAvalancheTablesHalted
+        expr: data_avalanche_halted_tables > 0
+        labels:
+          severity: warning
+        annotations:
+          summary: "{{ $labels.pipeline }} has {{ $value }} halted tables"
+
+      - alert: DataAvalanchePipelineError
+        expr: data_avalanche_pipeline_status{status="error"} == 1
+        labels:
+          severity: critical
+        annotations:
+          summary: "{{ $labels.pipeline }} is in ERROR"
+
+      # The WAL a PostgreSQL slot retains for the pipeline: it fills the server's disk.
+      - alert: DataAvalancheWalLag
+        expr: data_avalanche_wal_lag_bytes > 10 * 1024 * 1024 * 1024
+        for: 10m
+        labels:
+          severity: warning
+        annotations:
+          summary: "{{ $labels.pipeline }}'s slot retains {{ $value | humanize1024 }}B of WAL"
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: the two commands of Step 2.

Expected: PASS: `catalogue` 3 tests; promtool prints `SUCCESS`.

Run: `cargo test --workspace`

Expected: PASS (1317 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add deploy crates/data-avalanche-telemetry .github/workflows/ci.yml
git commit -m "feat(deploy): example Prometheus alert rules — lag, a degraded lease, halted tables, ERROR, retained WAL — tested by promtool in CI"
```

---

### Task 9: The E2E scenarios (`data-avalanche` tests, `test-infra`)

**Files:**
- Create: `crates/data-avalanche/tests/e2e/observability.rs`
- Modify: `test-infra/compose.yaml`, `test-infra/README.md`, `.github/workflows/{ci,weekly}.yml`
- Test: `crates/data-avalanche/tests/e2e/{harness,main}.rs`

**Interfaces:**
- Consumes: everything above; the harness's `E2e`, `Proc::metrics`, `eventually` (M6b).
- Produces: `Options.globals: &str` (more `globals` keys, one YAML line); the `jaeger` service
  (D-M10.16); the E2E jobs start it.

Two scenarios, each on the source the suite runs on:

- `a_running_pipeline_exports_the_catalogue`: after a snapshot of two rows and an insert, an update
  and a delete, the scrape has every series of `LIVE` with the pipeline's labels, the node's
  three, the source's lag and not the other source's; `rows_read_total` is 2, 1, 1, 1 for
  `snapshot`, `insert`, `update`, `delete`; `rows_written_total` is 5; the lag is under 60 s.
- `the_flushes_commits_and_chunks_are_traced_to_the_collector`: with every trace sampled, Jaeger
  holds a `pipeline.flush` root of the pipeline with a `sink.commit` child for `items`,
  `committed`; the resource is `data-avalanche` on `n1`; a `snapshot.chunk` root for `items`.

- [ ] **Step 1: Write the failing tests**

`crates/data-avalanche/tests/e2e/harness.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index 4e2ba53..9db0b76 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -87,6 +87,8 @@ pub struct Options<'a> {
     pub pipelines: &'a str,
     /// `globals.api` entries beyond the address, e.g. `, auth: jwt, jwt: { … }`.
     pub api: &'a str,
+    /// More `globals` keys, one YAML line, e.g. `tracing: { otlp_endpoint: … }`.
+    pub globals: &'a str,
 }
 
 /// The rows [`E2e::insert_series`] gives a key `g`.
@@ -279,6 +281,7 @@ globals:
     lease_renewal_interval: 2s
     drain_timeout: 30s{cluster}
     memberlist: {{ node_name: "${{E2E_NODE}}", bind_addr: [127.0.0.1], bind_port: "${{E2E_GOSSIP_PORT}}", advertise_addr: 127.0.0.1, advertise_port: "${{E2E_ADVERTISE_PORT}}"{memberlist} }}
+  {globals}
 pipelines:
   {test}:
     id: {test}
@@ -298,6 +301,7 @@ pipelines:
             cluster = o.cluster.replace(", ", "\n    "),
             memberlist = o.memberlist,
             api = o.api,
+            globals = o.globals,
         )
     }
 
```

`crates/data-avalanche/tests/e2e/main.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index d3dd0f9..cecd134 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -16,6 +16,7 @@ mod idp;
 mod jobs;
 mod lifecycle;
 mod mysql;
+mod observability;
 mod proxy;
 mod reload;
 mod schema;
```

`crates/data-avalanche/tests/e2e/observability.rs`:

```diff
diff --git a/crates/data-avalanche/tests/e2e/observability.rs b/crates/data-avalanche/tests/e2e/observability.rs
new file mode 100644
index 0000000..0873735
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/observability.rs
@@ -0,0 +1,317 @@
+//! M10's exit (roadmap M10): a running pipeline exports the mandatory catalogue with its labels
+//! (System-Overview §Observability catalogue), and its flushes, commits and snapshot chunks
+//! reach an OTLP/HTTP collector as traces (config §6.6): Jaeger, from `test-infra/compose.yaml`.
+
+use std::collections::BTreeMap;
+use std::time::Duration;
+
+use chrono::{DateTime, SecondsFormat, Utc};
+use data_avalanche_testkit::require_it;
+use serde_json::Value;
+use tokio::io::{AsyncReadExt, AsyncWriteExt};
+
+use crate::harness::{E2e, Options, eventually};
+
+const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);
+                     INSERT INTO items VALUES (1, 'a'), (2, 'b');";
+
+const CHANGES: &str = "INSERT INTO items VALUES (3, 'c'); UPDATE items SET doc = 'A' WHERE id = 1; DELETE FROM items WHERE id = 2";
+
+/// A heartbeat every 2 s: the retained log, and with it the source's lag series, is read at
+/// each.
+fn options() -> Options<'static> {
+    Options {
+        tables: ", snapshots: { mode: initial }",
+        postgres: ", heartbeat: 2s",
+        mysql: ", heartbeat: 2s",
+        ..Options::default()
+    }
+}
+
+/// The series of the catalogue a pipeline shows once its snapshot is done and a change of each
+/// kind went through, with the labels beyond the pipeline's three; a histogram by its `_count`.
+/// Not here: the commit and renewal failures, which only a failure makes, and the source's lag,
+/// which depends on the source.
+const LIVE: &[(&str, &str)] = &[
+    ("data_avalanche_replication_lag_seconds", ""),
+    ("data_avalanche_rows_written_total", ""),
+    ("data_avalanche_flush_duration_seconds_count", ""),
+    ("data_avalanche_flush_rows_count", ""),
+    ("data_avalanche_transaction_buffer_bytes", ""),
+    ("data_avalanche_source_reconnects_total", ""),
+    ("data_avalanche_snapshot_rows_done", ""),
+    ("data_avalanche_snapshot_state", r#",state="running""#),
+    ("data_avalanche_snapshot_state", r#",state="waiting""#),
+    ("data_avalanche_snapshot_chunk_duration_seconds_count", ""),
+    ("data_avalanche_reconcile_log_entries", ""),
+    ("data_avalanche_reconcile_log_keys", ""),
+    (
+        "data_avalanche_sink_commit_duration_seconds_count",
+        r#",sink_type="delta""#,
+    ),
+    ("data_avalanche_sink_merge_files_rewritten_total", ""),
+    ("data_avalanche_sink_merge_rows_copied_total", ""),
+    ("data_avalanche_sink_soft_dropped_columns", ""),
+    ("data_avalanche_lease_state", r#",state="held""#),
+    ("data_avalanche_lease_renew_latency_seconds_count", ""),
+    ("data_avalanche_pipeline_status", r#",status="running""#),
+    ("data_avalanche_halted_tables", ""),
+];
+
+/// The value of the sample `name{<labels><extra>}`, or of the node's `name` when `labels` is
+/// empty.
+fn sample(text: &str, name: &str, labels: &str, extra: &str) -> Option<f64> {
+    let head = if labels.is_empty() {
+        format!("{name} ")
+    } else {
+        format!("{name}{{{labels}{extra}}} ")
+    };
+    text.lines()
+        .find_map(|l| l.strip_prefix(&head))
+        .map(|v| v.parse().expect("a sample's value"))
+}
+
+/// The source's lag: the WAL a PostgreSQL slot retains, the age of the last transaction a MySQL
+/// stream read (D-M10.4); and the other one, which the pipeline doesn't have.
+fn lags(e: &E2e) -> (&'static str, &'static str) {
+    if e.source.is_postgres() {
+        (
+            "data_avalanche_wal_lag_bytes",
+            "data_avalanche_binlog_lag_seconds",
+        )
+    } else {
+        (
+            "data_avalanche_binlog_lag_seconds",
+            "data_avalanche_wal_lag_bytes",
+        )
+    }
+}
+
+#[tokio::test]
+async fn a_running_pipeline_exports_the_catalogue() {
+    require_it!();
+    let e = E2e::new("observed", ITEMS, options()).await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.converged("items").await;
+    e.exec(CHANGES).await;
+    e.converged("items").await;
+    let labels = e.labels();
+    let (lag, other) = lags(&e);
+    eventually("the source's lag", Duration::from_secs(60), async || {
+        sample(&p.metrics().await, lag, &labels, "").is_some()
+    })
+    .await;
+    let text = p.metrics().await;
+    for (name, extra) in LIVE {
+        assert!(
+            sample(&text, name, &labels, extra).is_some(),
+            "{name}{{{labels}{extra}}} isn't scraped:\n{text}"
+        );
+    }
+    for node in [
+        "data_avalanche_cluster_members",
+        "data_avalanche_owned_pipelines",
+        "data_avalanche_ring_generation",
+    ] {
+        assert!(sample(&text, node, "", "").is_some(), "{node}:\n{text}");
+    }
+    assert!(!text.contains(&format!("{other}{{")), "{text}");
+    let read = |op: &str| {
+        sample(
+            &text,
+            "data_avalanche_rows_read_total",
+            &labels,
+            &format!(r#",op="{op}""#),
+        )
+    };
+    assert_eq!(
+        ["snapshot", "insert", "update", "delete"].map(read),
+        [Some(2.0), Some(1.0), Some(1.0), Some(1.0)],
+        "{text}"
+    );
+    assert_eq!(
+        sample(&text, "data_avalanche_rows_written_total", &labels, ""),
+        Some(5.0),
+        "the two rows of the snapshot and the three changed"
+    );
+    let lagged = sample(&text, "data_avalanche_replication_lag_seconds", &labels, "");
+    assert!(lagged.is_some_and(|s| s < 60.0), "{text}");
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+const JAEGER_OTLP: &str = "http://127.0.0.1:54318";
+const JAEGER_QUERY: u16 = 56686;
+
+/// A span as Jaeger's query API returns it (OTLP JSON), its attributes and its resource's as
+/// text.
+#[derive(Debug)]
+struct Span {
+    trace: String,
+    id: String,
+    parent: String,
+    name: String,
+    attributes: BTreeMap<String, String>,
+    resource: BTreeMap<String, String>,
+}
+
+fn attributes(v: &Value) -> BTreeMap<String, String> {
+    v.as_array()
+        .into_iter()
+        .flatten()
+        .filter_map(|kv| {
+            let value = kv["value"].as_object()?.values().next()?;
+            let text = value
+                .as_str()
+                .map_or_else(|| value.to_string(), str::to_owned);
+            Some((kv["key"].as_str()?.to_owned(), text))
+        })
+        .collect()
+}
+
+/// `GET path` of Jaeger's query API: the whole answer. HTTP/1.0, so the body runs to the close,
+/// never chunked.
+async fn get(path: &str) -> Option<String> {
+    let mut tcp = tokio::net::TcpStream::connect(("127.0.0.1", JAEGER_QUERY))
+        .await
+        .ok()?;
+    let request = format!("GET {path} HTTP/1.0\r\nHost: jaeger\r\n\r\n");
+    tcp.write_all(request.as_bytes()).await.ok()?;
+    let mut answer = String::new();
+    tcp.read_to_string(&mut answer).await.ok()?;
+    Some(answer)
+}
+
+/// The spans of the traces of `operation` that started since `since` (`GET /api/v3/traces`);
+/// none when Jaeger has none, or doesn't answer within 5 s.
+async fn traced(operation: &str, since: DateTime<Utc>) -> Vec<Span> {
+    let at = |t: DateTime<Utc>| t.to_rfc3339_opts(SecondsFormat::Secs, true);
+    let path = format!(
+        "/api/v3/traces?query.service_name=data-avalanche&query.operation_name={operation}&query.start_time_min={}&query.start_time_max={}&query.num_traces=100",
+        at(since - chrono::Duration::seconds(5)),
+        at(Utc::now() + chrono::Duration::seconds(5)),
+    );
+    let Ok(Some(answer)) = tokio::time::timeout(Duration::from_secs(5), get(&path)).await else {
+        return Vec::new();
+    };
+    let Some((_, body)) = answer.split_once("\r\n\r\n") else {
+        return Vec::new();
+    };
+    let Ok(v) = serde_json::from_str::<Value>(body) else {
+        return Vec::new();
+    };
+    let mut spans = Vec::new();
+    for rs in v["result"]["resourceSpans"]
+        .as_array()
+        .into_iter()
+        .flatten()
+    {
+        let resource = attributes(&rs["resource"]["attributes"]);
+        for ss in rs["scopeSpans"].as_array().into_iter().flatten() {
+            for s in ss["spans"].as_array().into_iter().flatten() {
+                spans.push(Span {
+                    trace: s["traceId"].as_str().unwrap_or_default().to_owned(),
+                    id: s["spanId"].as_str().unwrap_or_default().to_owned(),
+                    parent: s["parentSpanId"].as_str().unwrap_or_default().to_owned(),
+                    name: s["name"].as_str().unwrap_or_default().to_owned(),
+                    attributes: attributes(&s["attributes"]),
+                    resource: resource.clone(),
+                });
+            }
+        }
+    }
+    spans
+}
+
+/// The pipeline's root spans named `name`, each with its children.
+fn roots<'a>(spans: &'a [Span], name: &str, pipeline: &str) -> Vec<(&'a Span, Vec<&'a Span>)> {
+    spans
+        .iter()
+        .filter(|s| {
+            s.name == name
+                && s.parent.is_empty()
+                && s.attributes.get("pipeline").map(String::as_str) == Some(pipeline)
+        })
+        .map(|root| {
+            let children = spans
+                .iter()
+                .filter(|s| s.trace == root.trace && s.parent == root.id)
+                .collect();
+            (root, children)
+        })
+        .collect()
+}
+
+/// Config §6.6: with an endpoint and every trace sampled, a flush is a trace whose root carries
+/// the pipeline and whose children are its tables' commits; a snapshot chunk is a trace of its
+/// own; the node is the resource's `service.instance.id`.
+#[tokio::test]
+async fn the_flushes_commits_and_chunks_are_traced_to_the_collector() {
+    require_it!();
+    let tracing = format!("tracing: {{ otlp_endpoint: \"{JAEGER_OTLP}\", sample_ratio: 1.0 }}");
+    let e = E2e::new(
+        "traced",
+        ITEMS,
+        Options {
+            globals: &tracing,
+            ..options()
+        },
+    )
+    .await;
+    let since = Utc::now();
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.converged("items").await;
+    e.exec(CHANGES).await;
+    e.converged("items").await;
+    let committed = |spans: &[Span]| {
+        roots(spans, "pipeline.flush", &e.test)
+            .iter()
+            .any(|(_, commits)| {
+                commits.iter().any(|c| {
+                    c.name == "sink.commit"
+                        && c.attributes.get("outcome").map(String::as_str) == Some("committed")
+                        && c.attributes
+                            .get("table")
+                            .is_some_and(|t| t.ends_with("items"))
+                })
+            })
+    };
+    eventually(
+        "a traced flush and its commit",
+        Duration::from_secs(60),
+        async || committed(&traced("pipeline.flush", since).await),
+    )
+    .await;
+    let flushes = traced("pipeline.flush", since).await;
+    let (flush, _) = roots(&flushes, "pipeline.flush", &e.test)[0];
+    assert_eq!(
+        flush.resource.get("service.name").map(String::as_str),
+        Some("data-avalanche")
+    );
+    assert_eq!(
+        flush
+            .resource
+            .get("service.instance.id")
+            .map(String::as_str),
+        Some("n1")
+    );
+    eventually(
+        "a traced snapshot chunk",
+        Duration::from_secs(60),
+        async || {
+            let chunks = traced("snapshot.chunk", since).await;
+            roots(&chunks, "snapshot.chunk", &e.test)
+                .iter()
+                .any(|(c, _)| {
+                    c.attributes
+                        .get("table")
+                        .is_some_and(|t| t.ends_with("items"))
+                })
+        },
+    )
+    .await;
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run, with a `test-infra` started before this task (no `jaeger`):
`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- observability:: --test-threads=1`

Expected: the catalogue scenario passes, since Tasks 1–5 built what it reads. The tracing scenario
FAILS: `timed out waiting until a traced flush and its commit`, as nothing listens on 54318 and
56686. That failure was measured with the start's `logger.trace(…)` removed, which leaves the
collector empty the same way, because the planning machine's `test-infra` already ran Jaeger.

- [ ] **Step 3: Implement**

`.github/workflows/ci.yml`:

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index 794fe60..f6c8ae4 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -142,15 +142,15 @@ jobs:
       - uses: actions-rust-lang/setup-rust-toolchain@v2
       # uv runs the type scenario's DuckDB read.
       - uses: astral-sh/setup-uv@v10.2.0
-      - name: Start ${{ matrix.target }}, mysql-84 and RustFS
+      - name: Start ${{ matrix.target }}, mysql-84, RustFS and Jaeger
         run: |
-          docker compose -f test-infra/compose.yaml up -d --wait "${{ matrix.target }}" mysql-84 rustfs
+          docker compose -f test-infra/compose.yaml up -d --wait "${{ matrix.target }}" mysql-84 rustfs jaeger
           docker compose -f test-infra/compose.yaml --profile init run --rm rustfs-init
       - name: Slice-1 E2E
         run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
       - name: Container logs on failure
         if: failure()
-        run: docker compose -f test-infra/compose.yaml logs --no-color "${{ matrix.target }}" mysql-84 rustfs
+        run: docker compose -f test-infra/compose.yaml logs --no-color "${{ matrix.target }}" mysql-84 rustfs jaeger
       - name: Stop the containers
         if: always()
         run: docker compose -f test-infra/compose.yaml down -v
```

`.github/workflows/weekly.yml`:

```diff
diff --git a/.github/workflows/weekly.yml b/.github/workflows/weekly.yml
index 92b5ba7..0fa7eab 100644
--- a/.github/workflows/weekly.yml
+++ b/.github/workflows/weekly.yml
@@ -37,9 +37,9 @@ jobs:
       - uses: actions/checkout@v7
       - uses: actions-rust-lang/setup-rust-toolchain@v2
       - uses: astral-sh/setup-uv@v10.2.0
-      - name: Start PostgreSQL ${{ matrix.version }} and RustFS
+      - name: Start PostgreSQL ${{ matrix.version }}, RustFS and Jaeger
         run: |
-          docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "postgres-${{ matrix.version }}" rustfs
+          docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "postgres-${{ matrix.version }}" rustfs jaeger
           docker compose -f test-infra/compose.yaml --profile init run --rm rustfs-init
       - name: PostgreSQL source tests
         run: 'cargo test --locked -p data-avalanche-source-pg --test pg --test slot_invalidation -- --test-threads=1 --skip tls::'
@@ -49,7 +49,7 @@ jobs:
         run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
       - name: Container logs on failure
         if: failure()
-        run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "postgres-${{ matrix.version }}" rustfs
+        run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "postgres-${{ matrix.version }}" rustfs jaeger
       - name: Stop the containers
         if: always()
         run: docker compose -f test-infra/compose.yaml --profile matrix down -v
@@ -99,15 +99,15 @@ jobs:
       - uses: actions/checkout@v7
       - uses: actions-rust-lang/setup-rust-toolchain@v2
       - uses: astral-sh/setup-uv@v10.2.0
-      - name: Start ${{ matrix.service }} and RustFS
+      - name: Start ${{ matrix.service }}, RustFS and Jaeger
         run: |
-          docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "${{ matrix.service }}" rustfs
+          docker compose -f test-infra/compose.yaml --profile matrix up -d --wait "${{ matrix.service }}" rustfs jaeger
           docker compose -f test-infra/compose.yaml --profile init run --rm rustfs-init
       - name: Slice-1 E2E
         run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1
       - name: Container logs on failure
         if: failure()
-        run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "${{ matrix.service }}" rustfs
+        run: docker compose -f test-infra/compose.yaml --profile matrix logs --no-color "${{ matrix.service }}" rustfs jaeger
       - name: Stop the containers
         if: always()
         run: docker compose -f test-infra/compose.yaml --profile matrix down -v
```

`test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
index c9b78fa..992f6ff 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -18,6 +18,7 @@ on `PATH`; it fetches DuckDB itself.
 | mariadb-114, mariadb-1011 | mariadb:11.4, 10.11 | 53314, 53311 | root / avalanche | matrix |
 | rustfs (S3) | rustfs/rustfs:1.0.0 | 59000 | avalanche / avalanche-secret | default |
 | azurite (Azure Blob) | mcr.microsoft.com/azure-storage/azurite:3.37.0 | 10000 | Azurite's public dev account | default |
+| jaeger (OTLP/HTTP collector, query API) | jaegertracing/jaeger:2.21.0 | 54318 (OTLP), 56686 (query) | — | default |
 
 Bucket / container `data-avalanche-test` is created by the `init` profile (`up.sh` runs it). Azurite keeps
 port 10000 because `object_store`'s emulator mode always targets `127.0.0.1:10000`.
```

`test-infra/compose.yaml`:

```diff
diff --git a/test-infra/compose.yaml b/test-infra/compose.yaml
index a8ae498..b4e9871 100644
--- a/test-infra/compose.yaml
+++ b/test-infra/compose.yaml
@@ -82,6 +82,12 @@ services:
     # object_store's emulator mode targets 127.0.0.1:10000, so this port is not remapped.
     ports: ["10000:10000"]
 
+  # An OTLP/HTTP collector the E2E tracing scenario exports to (config §6.6), its traces read back
+  # through Jaeger's query API; in-memory storage.
+  jaeger:
+    image: "jaegertracing/jaeger:2.21.0"
+    ports: ["54318:4318", "56686:16686"]
+
   rustfs-init:
     image: "amazon/aws-cli:2.37.4"
     profiles: [init]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `docker compose -f test-infra/compose.yaml up -d --wait jaeger`, then the command of Step 2
on PostgreSQL, and with `DATA_AVALANCHE_E2E_SOURCE=mysql` on each of `mysql-84`,
`mysql-84-nogtid` and `mariadb-118` (`DATA_AVALANCHE_MYSQL`).

Expected: PASS, 2 tests on each of PostgreSQL 18 (17 s), `mysql-84` (19 s), `mysql-84-nogtid` (19 s) and
`mariadb-118` (19 s).

Run: the whole E2E suite on PostgreSQL 18:
`DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1`

Expected: PASS, 46 scenarios (605 s), on the tree before the Jaeger query's 5 s timeout. The timeout
changes only `observability.rs`, whose two scenarios were run again on every source after it.

Run: `cargo test --workspace`

Expected: PASS (1317 tests in all).

Run: the fmt and clippy line of Task 1. Expected: no output, no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests test-infra .github/workflows
git commit -m "test(e2e): the catalogue scraped live, and the traces read back from Jaeger"
```

---

## Exit

- [ ] Every task's Step 4 passes, the whole E2E suite on PostgreSQL 18 and the observability
  scenarios on `mysql-84`, `mysql-84-nogtid` and `mariadb-118`.
- [ ] The catalogue test asserts every mandatory series with its labels (Task 1, roadmap M10's
  exit).
- [ ] The tracing smoke test passes against the Jaeger container (Task 9, roadmap M10's exit).
- [ ] The promtool test passes in CI's `check` job (Task 8).
- [ ] The whole-branch review's findings fixed or recorded; the PR's CI green.
- [ ] DA-OBS-6 ✅ in the gap tracker when M10 merges.

## Deviations during execution

- **Task 2, a table's commit time** (fix commit after the task's commit). The plan's code reset
  the timer at each attempt (`let at = Instant::now()` inside the retry loop of
  `Writer::commit`), so `committed` timed only the attempt that landed. D-M10.2 and the research
  doc say the time covers the table's commit, retries included. The timer now starts once,
  before the loop, so the time covers every attempt and the backoff between them;
  `Observer::committed`'s doc says so. `Ears` records each commit's time apart
  (`commit_times()`), and `a_commit_is_timed_with_its_retries` pins it: three transient
  failures under the test backoff take at least 3.5 ms, and the old placement measured about
  0.2 ms. The writer's tests run on the real clock, so the test asserts a lower bound only. The
  workspace has 1298 tests after Task 2, one more than Step 4 expects; the later tasks' totals
  are one higher too.
- **Task 3, two more lag tests** (a test commit after the task's commit). The task's review found
  that the two cases D-M10.3 names had no test, though the code was right:
  - `a_slice_of_a_large_transaction_leaves_the_rest_pending`: a slice handed while its
    transaction is still open, then durable, leaves the lag at that transaction's commit time.
    It fails without the reset of the lag's start after a slice.
  - `a_durable_job_leaves_the_next_job_s_lag`: of two queued jobs, the first one durable moves
    the lag to the second's commit time. It fails when a durable job clears the whole queue.

  Both drive the session by hand, because the fake sink's `release_commits` can't release a
  single job. The workspace has 1304 tests after Task 3; the later tasks' totals are three
  higher than their Step 4s say.
- **Task 4, an idle stream's binlog lag** (two commits after the task's commit; Oleksii decided
  the behaviour 2026-10-28). The task's review found that D-M10.4's "after a server heartbeat,
  `behind` is 0" stuck: the idle flag cleared only at the next commit read, and `retained()`
  returned 0 for an idle read without asking the head. A MySQL pipeline that stopped reading
  after a heartbeat (backpressure, a pause) showed 0 while the server wrote on; so did a long
  first transaction read after an idle stretch. The spec says "`0` at the server's head", which
  such a stream isn't.

  What is built:
  - `ReadAt.idle` became `caught_up: Option<CaughtUp { at_micros, head }>`. A heartbeat stamps
    the time; a commit read, an open and a close clear it.
  - `retained()` always asks the head. The first check after a heartbeat records the head it
    sees as the baseline, before the "covers the head" test: the other order never records one
    for a stream at the head, and hid the stall in the IT.
  - The lag is 0 while the head equals the baseline or the stream covers it. Once the head moves
    past, it is now − the heartbeat's time, clamped at 0. The baseline is written only onto the
    caught-up period it was read for (matched by `at_micros`).
  - A write between the heartbeat and the first check becomes the baseline; a stall then shows
    from the server's next write (documented on `CaughtUp`). A repeated heartbeat resets the time
    and the baseline.

  Pinned by a unit test (with the old idle short-circuit restored it reads 0 for 6 s), and by an
  IT, `an_idle_stream_that_stops_reading_lags_once_the_server_writes_on`. The IT waits for a
  check at the head, then writes a row it doesn't read, and expects a lag above 0. It passed in
  the MySQL test binary's parallel run: 144 tests, three times on `mysql-84` and once each on
  `mysql-84-nogtid` and `mariadb-118`. The workspace has 1308 tests after Task 4.

  The spec's definition of `binlog_lag_seconds` gains the idle case (approved and applied 2026-10-28). The
  research doc's §2 matches.
- **Task 6, the writer's diff over Task 2's fix.** Step 3's diff of `writer.rs` no longer applied:
  its hunk on `Writer::commit` expects the timer inside the retry loop, where Task 2's fix took it
  out. The task applied the plan's diff with that hunk re-applied by hand: the timer stays before
  the loop, now in `attempts`, and the span's `attempts` and `outcome` are recorded as planned.
  The other 14 diffs applied as given.
- **Task 6, the resource before the batch thread** (a fix commit after the task's). The task's
  review found a race in the plan's code. The provider hands the resource to the batch thread as
  a message, and `Traces::exporter` read it from the `Switch`. A `set()` before the batch
  thread had applied it built an exporter without `service.name`, `service.instance.id` or
  `service.version`, which stayed until the endpoint changed. `Traces::new` now seeds the
  `Switch` with the resource; the SDK applies the same value later.
  `the_switch_carries_the_resource_before_the_batch_thread_applies_it` pins it: without the seed
  it failed 5 runs of 5. The workspace has 1317 tests after Task 6.
