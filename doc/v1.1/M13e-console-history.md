---
updated: 2026-11-19
domain: Architecture
tags: [v1.1, implementation-plan, m13e, console, chart-history, prometheus, api, cluster, state, telemetry, pipeline]
---

# M13e — The Console's Chart History Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The fifth server milestone of the 1.1.0 admin console (Admin_Console §14). It builds
the history the console's charts read (Admin_Console §7):
- each node samples the series it owns: the pipelines whose lease it holds, their tables, and
  itself;
- the samples are kept in `globals.state`, so a pipeline's history follows it across a failover
  and survives a restart, for `globals.console.history.retention`;
- `GET /v1/history` reads them from any node, over a range and at a step;
- with `globals.console.prometheus` set, a pipeline's range older than the retention comes from
  that Prometheus;
- the site and the E2E suite follow.

The live stream (SSE) is M13f's (D-M13e.1).

**Architecture:**

- **The store** (Task 1, `data-avalanche-state`): `Charts`, the chart history under
  `history/<YYYY-MM-DD>/<scope>/<HH>-<writer>.json`. One object per scope (a pipeline or a node),
  hour and writer (a lease term or a process), written only by its writer. The series catalogue
  (`SERIES`) lives here, for the sampler and the API alike.
- **Two figures the sampler needs** (Task 2, `data-avalanche-pipeline`, `data-avalanche-telemetry`):
  a table's slowest commit and costliest MERGE of the last minute; the node's slowest lease
  renewal since the last sample.
- **The sampler** (Task 3, `data-avalanche-cluster`, the binary): every 15 s, from the
  controllers' statuses and the process's figures; writes each chunk with its first sample, then
  every minute, and once more when its hour or term ends; expires whole days. The pipeline's
  freshness moves onto `ControllerStatus`, so the live figures and the chart compute one lag.
- **The API** (Tasks 4–5, `data-avalanche-api`): `GET /v1/history` folds the samples into points
  at a step; Task 5 adds the Prometheus reader, one range query per series grouped by pipeline.
- **The site** (Task 6) and **the E2E suite** (Task 7). On three nodes, a node that doesn't run
  the pipeline reads its lag, its table's series and the holder's own; the holder killed, the
  next term's samples join what it wrote. The scenario runs on PostgreSQL and on all three MySQL
  targets.

**Tech Stack:** Rust 1.100.0 (edition 2024). No new crate, and no new feature of one (the Prometheus
query's form is encoded with `url`, as reqwest's `form` feature is off). `data-avalanche-cluster` gains
`futures-util`, already in the workspace.

**Spec:**
- `doc/research/Admin_Console.md`: §4.2 (charts), §7 (history for charts), §8 (freshness), §11
  (`history.retention`, `prometheus`), §12 (the history sampler and store, the Prometheus
  reader), §13 (layout, resolution and endpoint paths are the builder's), §14.
- The approved mockups `doc/v1.1/mockups/04-node.html` (the node's 6 h charts),
  `05-pipelines.html` (lag, 6 h), `06-pipeline.html` (the lag chart), `07-tables.html` (a
  table's 24 h charts), `08-snapshots.html` (rows/s copied, lag during a job).
- `doc/specs/Configuration_Specification.md` §6.8 (`globals.console.history`,
  `globals.console.prometheus`; V-130, V-131, V-132, V-137).
- `doc/specs/System-Overview.md` §Management API endpoints, §Observability catalogue. Amended by
  S-M13e.1, below.
- `doc/research/Observability_Metrics_and_Tracing.md` §2 (the metrics Prometheus holds).
- `doc/v1.1/M13a-console-events.md` (the events' layout and expiry, which the history follows),
  `doc/v1.1/M13d-console-pipelines.md` (the live figures the history samples).

## Global Constraints

- Toolchain `1.100.0`, edition `2024`. `unsafe_code = "forbid"`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`.
- **Dependency direction** (roadmap §2): `data-avalanche (bin) → api → cluster → pipeline → core,
  config, state, storage`, with the sources and the sinks beside the pipeline. `testkit` is only
  ever a dev-dependency.
- **Recorded whatever the console's switch says** (Admin_Console §3): the sampler runs and
  `GET /v1/history` answers with `globals.console.enabled: false`.
- **Replication first** (Admin_Console §8): sampling reads the controllers' statuses and never
  waits on storage while a pipeline does; a failing store holds samples in memory, bounded.
- **Without `prometheus`, nothing is marked** (Admin_Console §7, Oleksii): no `older`, no
  `prometheus` in an answer, no warning.
- **Prometheus never fails an answer** (Config §6.8): unreachable, slow or refusing, the answer is
  `200` with the history kept.
- **The history is new state.** A 1.0 node never reads `history/`; rolling back leaves it, and the
  1.0 node never expires it. It goes into the 1.1.0 release notes (D-M13e.7).
- **Time in tests:** the sampler's tests pass `now` (no clock); the API's tests and the E2E suite
  run in real time.
- **Test gates:** plain `cargo test` stays hermetic. The E2E suite needs `DATA_AVALANCHE_IT=1` and
  the `e2e-hooks` feature, and runs with `--test-threads=1`, on PostgreSQL and on
  `DATA_AVALANCHE_E2E_SOURCE=mysql` (CI's e2e-mysql runs `mysql-84`, `mysql-84-nogtid` and
  `mariadb-118`).
- **The site follows the behaviour in the same milestone** (`doc/v1.0/README.md`).
- **Public-API conventions:** public types are re-exported at the crate root; errors are
  `thiserror` types.
- **Code blocks:**
  - They are the verified content, already `rustfmt`-formatted.
  - Every block is a `git apply`-ready diff, one per file.
  - A step's diffs apply in the order given, after the earlier steps'.
  - The OpenAPI document, the site's API reference and `Cargo.lock` are regenerated, not written
    (Tasks 3, 4, 6).
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt someone running 1.1.0 in production. Each is pinned by a
test in the task after the arrow.

1. **A failover in the middle of an hour.** The next term must not overwrite what the last one
   wrote, and a chart across the failover shows both.
   - Task 1 (`a_read_returns_every_writer_of_the_hours_it_meets`).
   - Task 3 (`a_new_term_or_hour_ends_the_chunk_before_it`).
   - Task 7 (the holder killed: its samples kept, the next term's added).
2. **A storage outage.** Sampling goes on and no pipeline waits; the samples land when the store
   is back, and memory stays bounded when it never is → Task 3
   (`samples_wait_through_a_storage_outage`).
3. **A Prometheus that is down, slow or refusing.** The chart still draws the history kept and
   says the older range is missing, never an error → Task 5
   (`a_failing_prometheus_leaves_the_history_kept`).
4. **A long range on a page.** A month at 15 s samples would be 178 560 points a series: the
   default step keeps 1440, never finer than the samples → Task 4
   (`a_query_names_what_it_asks_and_its_step`).
5. **Names that are PromQL syntax.** A pipeline named `bill.ing` or an `extra_labels` value with a
   quote must select only its own series → Task 5 (`each_pipeline_series_has_its_query`).

## Decisions and rulings

Settled with Oleksii before this plan (2026-11-18):
- M13e is split: the history and the Prometheus reader here; the SSE stream in a new M13f;
- each chunk is written every minute (a crash loses at most a minute);
- Prometheus serves the older range of a pipeline's series only; a table's and a node's stop at
  the retention, marked.

The rest are Claude's calls under Admin_Console §13.

- **D-M13e.1 What this milestone serves, and what waits.**
  - Served here: every time series of the mockups' charts and sparklines, read through
    `GET /v1/history`.
  - M13f: the live stream. A page then draws its chart's tail from the stream's samples; until
    then the stored history ends at most a minute back, and a page shows the live figures
    (`GET /v1/pipelines/{id}/live`) for now.
  - A job's "effect on lag" reads the pipeline's `lag_seconds` over the job's time; its "rows/s
    copied" the pipeline's `snapshot_rows_per_second`. A chart's event lines read
    `GET /v1/events`.
  - M13d parked nothing for these pages, and M13c's parked items belong to the jobs list.
- **D-M13e.2 The series** (`SERIES`, in `data-avalanche-state`):

  | Scope | Series | Unit | Fold | Sampled |
  |---|---|---|---|---|
  | pipeline | `lag_seconds`: `now − fresh_as_of`, the live figures' lag | seconds | max | 15 s, while a worker runs |
  | pipeline | `changes_per_minute`: its tables' sum | changes/min | mean | 15 s |
  | pipeline | `wal_held_bytes` (PostgreSQL: the slot's retained WAL) | bytes | max | 15 s |
  | pipeline | `binlog_lag_seconds` (MySQL, MariaDB: `source_behind`) | seconds | max | 15 s |
  | pipeline | `snapshot_rows_per_second`: rows done between two samples of one term | rows/s | mean | 15 s, while a snapshot copies |
  | table | `changes_per_minute` (0 for a tracked table without commits) | changes/min | mean | 1 min; a table new to the term's chunk at once |
  | table | `files_rewritten`: the minute's costliest stream MERGE | files | max | 1 min, when one wrote rows |
  | table | `commit_seconds`: the minute's slowest commit, retries included | seconds | max | 1 min, when one committed |
  | node | `memory_bytes`, `cpu_cores` (Linux; none elsewhere) | bytes, cores | max, mean | 15 s |
  | node | `lease_renew_seconds`: the slowest renewal of any pipeline since the last sample | seconds | max | 15 s, when one renewed |

  - A page's "p99" over a range (lease renewal, commit duration) is the page's to compute from the
    points; the history keeps each window's slowest, which bounds it from above.
  - **New figures:** `TableCommit::took` (the commit with its retries); `TableFigures` keeps every
    commit of the last minute (`slowest_commit`, `most_rewritten`); `Metrics::renewed` also
    keeps the node's slowest renewal, which `Metrics::take_slowest_renewal` takes.
- **D-M13e.3 The sampler** (`Sampler`, `sample_history`, in `data-avalanche-cluster`).
  - **What it owns:** a pipeline whose controller has an owner epoch and isn't a standby; its
    writer is `e<epoch>`. The node; its writer is `i<incarnation>`.
  - **Cadence:** a sample every `SAMPLE_EVERY` (15 s), the tables at every fourth: every table
    the worker tracks or committed to, and a table the term's chunk hasn't sampled yet at once. A
    chunk is written with its first sample or a new series (a new term's samples, or a new
    table's, land within 15 s), then every `WRITE_EVERY` (60 s) while it changes, and once more
    when its hour or its term ends (the lease lost or moved, or another epoch). Writes go 8 at a
    time.
  - **A failing store:** a write that fails is tried again at the next sample. An ended chunk
    still unwritten `UNWRITTEN_KEPT` (an hour) after its hour ended is dropped with a warning:
    memory holds at most two hours of a scope.
  - **Lag:** `ControllerStatus::fresh_as_of`, moved from the API's live figures, so the chart and
    the live answer are one number.
- **D-M13e.4 Layout and store** (`Charts`, in `data-avalanche-state`).
  - `history/<YYYY-MM-DD>/pipelines/<id>/<HH>-e<epoch>.json` and
    `history/<YYYY-MM-DD>/nodes/<name>/<HH>-i<incarnation>.json`, each segment escaped
    (`escape_segment`). Day first, so a day expires as one folder, as the events do (M13a).
  - `ChartChunk { scope, hour, writer, series, tables }` (schema version 1); a series' samples are
    `Points { t: seconds into the hour, v }`. A table is keyed by its `schema.table`.
  - Written with an unconditional `put` (`DocStore::put`, new): one writer per object.
  - **Reading:** a day's folder listed per scope; the hours that meet the range loaded 16 at a
    time; a chunk that can't be decoded is left out with a warning. An hour that ended over
    `CLOSED_AFTER` (5 min) ago is final and kept in memory, `CLOSED_KEPT` (4096) at most, the
    oldest let go first.
  - **Expiry:** `expire_history` on every node, hourly, the first a minute after the start: the
    days that ended past `globals.console.history.retention`, as `Events::prune` does. A reload
    of the retention reaches it.
- **D-M13e.5 `GET /v1/history`** (viewer).
  - **What:** `pipeline` (an id, several comma-separated, or, without it or `node`, every
    pipeline of the generation the node runs); `table` (a config key, with one pipeline); `node`
    (alone; any name: a node that left keeps its history). `series` (comma-separated; every one of
    the scope by default). `404` for a pipeline not in the generation.
  - **Range:** `since` and `until` (RFC 3339), an hour back from now by default; `until` is never
    past now; 31 days at most (`MAX_SPAN`).
  - **Step:** by default the samples' own (15 s; 60 s with a table), coarsened in multiples of it
    to at most `MAX_POINTS` (1440) points; a `step` asked is never finer than the samples. A
    point stands at a multiple of the step, so two pages line up; it folds its samples as the
    series says.
  - **Retention:** the history shown starts at `kept_since` (now − retention), whatever storage
    still holds.
  - **Answer:** `{since, until, step, kept_since, series: [{pipeline | node, table?, name, unit,
    points: [[unix seconds, value]]}]}`; `503` when storage can't be read.
- **D-M13e.6 Prometheus** (`PrometheusReader`, `PrometheusClient`, in `data-avalanche-api`).
  - Asked only when `prometheus` is set and the range starts before `kept_since`, for a pipeline's
    series: one `query_range` (POST, form-encoded) per series, for every pipeline asked at once,
    `by (pipeline)` (the metrics' `pipeline` label is the pipeline's name):
    - `lag_seconds`: `max by (pipeline) (data_avalanche_replication_lag_seconds{…})`;
    - `wal_held_bytes`, `binlog_lag_seconds`: the same over `data_avalanche_wal_lag_bytes`,
      `data_avalanche_binlog_lag_seconds`;
    - `changes_per_minute`: `60 * sum by (pipeline) (rate(data_avalanche_rows_read_total{…,op!="snapshot"}[W]))`;
    - `snapshot_rows_per_second`: the same over `op="snapshot"`, without the 60;
    - `W` is the step, at least `MIN_RATE_WINDOW` (300 s); every selector carries
      `pipeline=~"<names, regex-escaped>"` and `extra_labels`.
  - From the step before `since` to the last step that ends by `kept_since`, so the step
    `kept_since` falls in is the history's; at the answer's step.
  - The config's `basic` or `bearer_token`, `headers`, `tls.ca` (besides the platform's roots) and
    `timeout`; no redirects or proxies; answers over 8 MiB are refused. The client is made again
    only when the config changes.
  - Each series says where its older range came from (`older`): `prometheus`, `failing`, or
    `not_in_prometheus` (a table's or a node's). With `prometheus` set, the answer's
    `prometheus` is the node's last read: `unasked`, or `ok` / `failing` with its time and error.
- **D-M13e.7 Rollback and the release notes.** Nothing a 1.0 node reads changes. After a rollback
  the `history/` folder stays in `globals.state` and nothing expires it: the 1.1.0 release notes
  say so, and that it can be deleted.
- **D-M13e.8 The E2E scenario** waits for each series' first write (within 15 s of its first
  sample: a new chunk or a new series is written at once), never for the minute's; one scenario
  on each source, about 42 s.

### Spec patch S-M13e.1 (approved 2026-11-19, applied)

`doc/specs/System-Overview.md` §Management API endpoints, a row after **Events**:

> | Chart history | `GET /v1/history` | viewer | the series the nodes sampled for the console's charts, read by any node from `globals.state`: a pipeline's (`lag_seconds`, `changes_per_minute`, `wal_held_bytes`, `binlog_lag_seconds`, `snapshot_rows_per_second`), a table's (`changes_per_minute`, `files_rewritten`, `commit_seconds`) or a node's (`memory_bytes`, `cpu_cores`, `lease_renew_seconds`); filters `pipeline` (several, or every pipeline), `table`, `node`, `series`, `since` and `until` (an hour by default, 31 days at most), `step` (at most 1440 points by default; a coarser step keeps a gauge's peak and a rate's mean). Kept for `globals.console.history.retention`; with `globals.console.prometheus`, a pipeline's older range comes from it, and each series says where (`older`: `prometheus`, `failing`, `not_in_prometheus`) |

## File Structure

| File | Responsibility |
|---|---|
| `crates/data-avalanche-state/src/{charts,doc,lib}.rs` | `Charts`, `ChartChunk`, `ChartScope`, `Points`, the series catalogue `SERIES`; `DocStore::put` (Task 1) |
| `crates/data-avalanche-pipeline/src/{status,writer}.rs` | `TableCommit::took`; a table's last minute: `slowest_commit`, `most_rewritten` (Task 2) |
| `crates/data-avalanche-telemetry/src/lib.rs`, `tests/metrics.rs` | The node's slowest renewal, `Metrics::take_slowest_renewal` (Task 2) |
| `crates/data-avalanche-cluster/src/{history,controller,lib}.rs`, `Cargo.toml` | `Sampler`, `sample_history`, `expire_history`; `ControllerStatus::fresh_as_of` (Task 3) |
| `crates/data-avalanche/src/run.rs` | The sampler and the expiry on every node; `Node`'s `charts` and `prometheus` (Tasks 3–5) |
| `crates/data-avalanche-api/src/{history,prometheus}.rs` | `GET /v1/history`; the Prometheus reader (Tasks 4, 5) |
| `crates/data-avalanche-api/src/{lib,live,tables,events,openapi,test_support}.rs`, `openapi.json` | The route, its parameters and role; `Node`'s fields; the live figures read `fresh_as_of` (Tasks 2–5) |
| `xtask/src/api.rs` | The API page's `history` section (Task 6) |
| `docs/src/content/docs/deployment/observability.md`, `docs/src/content/docs/reference/api.md`, `docs/public/openapi.json`, `doc/research/Observability_Metrics_and_Tracing.md` | The pages and the research doc (Task 6) |
| `crates/data-avalanche/tests/e2e/{console_history,main}.rs` | The scenario on three nodes (Task 7) |

The plan's commit also carries S-M13e.1 (once approved) and `doc/v1.1/README.md`: M13d merged
(#30), this plan's row, and M13f's.

---

### Task 1: The chart history's store (`data-avalanche-state`)

**Files:**
- Create: `crates/data-avalanche-state/src/charts.rs`
- Modify: `crates/data-avalanche-state/src/doc.rs`
- Modify: `crates/data-avalanche-state/src/lib.rs`

**Interfaces:**
- Consumes: `DocStore`, `Document`, `StateError`; `VersionedStore::{put, list, folders, delete_many}`; `escape_segment` (storage).
- Produces (state crate root):
  - `Charts::new(DocStore)`; `Charts::write(&self, &ChartChunk) -> Result<(), StateError>`;
    `Charts::read(&self, &ChartScope, since, until, now: DateTime<Utc>) -> Result<Vec<Arc<ChartChunk>>, StateError>`
    (each writer's chunk apart, by hour); `Charts::prune(&self, retention: Duration, now) -> Result<usize, StateError>`.
  - `ChartChunk { scope: ChartScope, hour: DateTime<Utc>, writer: String, series: BTreeMap<String, Points>, tables: BTreeMap<String, BTreeMap<String, Points>> }`,
    `ChartChunk::new(scope, hour, writer: &str)`, `key()`, `is_empty()`; `Points { t: Vec<u16>, v: Vec<f64> }`, `push(t, v)`, `iter()`, `is_empty()`.
  - `ChartScope::{Pipeline(id), Node(name)}`; `chart_key(&ChartScope, hour, writer) -> String`; `hour_of(DateTime<Utc>) -> DateTime<Utc>`.
  - `SERIES: &[Series]`, `Series { name, scope: SeriesScope, unit, fold: Fold, every: Duration }`, `SeriesScope::{Pipeline, Table, Node}`, `Fold::{Max, Mean}`, `series_of(SeriesScope, &str) -> Option<&'static Series>`.
  - `CHARTS_PREFIX` (`"history"`), `SAMPLE_EVERY` (15 s), `TABLE_SAMPLE_EVERY` (60 s), `CLOSED_AFTER` (5 min), `CLOSED_KEPT` (4096).
  - `DocStore::put<T: Document>(&self, key, &T) -> Result<ObjectVersion, StateError>`.

- [ ] **Step 1: Write the failing tests**

The tests: `a_chunk_is_keyed_by_day_scope_hour_and_writer`, `a_read_returns_every_writer_of_the_hours_it_meets`, `an_ended_hour_is_read_once_and_the_current_one_every_time`, `whole_days_past_the_retention_are_deleted`, `the_series_are_named_once_per_scope`. The new module holds only its tests for now; the code goes above them in Step 3.

`crates/data-avalanche-state/src/charts.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/charts.rs b/crates/data-avalanche-state/src/charts.rs
new file mode 100644
index 0000000..7f4856d
--- /dev/null
+++ b/crates/data-avalanche-state/src/charts.rs
@@ -0,0 +1,174 @@
+//! The chart history (Admin_Console §7): what each node samples of the series it owns, kept in
+//! `globals.state` as `history/<YYYY-MM-DD>/<scope>/<HH>-<writer>.json`. One object holds one
+//! scope's hour as one writer saw it: a pipeline's holder over one lease term (`e<epoch>`), or a
+//! node's process (`i<incarnation>`). Its writer rewrites it while the hour lasts, so no two
+//! writers share an object and a failover's two terms are both kept. A day lists its scope's hours
+//! in one listing, and a whole day expires by deleting its folder.
+
+#[cfg(test)]
+mod tests {
+    use chrono::TimeZone;
+    use data_avalanche_storage::{ObjectStoreVersioned, VersionedStore};
+    use object_store::memory::InMemory;
+
+    use super::*;
+
+    fn charts() -> (Charts, Arc<dyn VersionedStore>) {
+        let store: Arc<dyn VersionedStore> =
+            Arc::new(ObjectStoreVersioned::new(Arc::new(InMemory::new()), "g").unwrap());
+        (Charts::new(DocStore::new(Arc::clone(&store))), store)
+    }
+
+    fn at(d: u32, h: u32, m: u32) -> DateTime<Utc> {
+        Utc.with_ymd_and_hms(2026, 11, d, h, m, 0).unwrap()
+    }
+
+    fn chunk(scope: &ChartScope, hour: DateTime<Utc>, writer: &str, v: f64) -> ChartChunk {
+        let mut c = ChartChunk::new(scope.clone(), hour, writer);
+        c.series
+            .entry("lag_seconds".into())
+            .or_default()
+            .push(15, v);
+        c
+    }
+
+    #[test]
+    fn a_chunk_is_keyed_by_day_scope_hour_and_writer() {
+        let p = ChartScope::Pipeline("orders/eu".into());
+        assert_eq!(
+            chart_key(&p, at(18, 9, 0), "e7"),
+            "history/2026-11-18/pipelines/orders%2Feu/09-e7.json"
+        );
+        let n = ChartScope::Node("node-a".into());
+        assert_eq!(
+            chart_key(&n, at(18, 23, 0), "i3"),
+            "history/2026-11-18/nodes/node-a/23-i3.json"
+        );
+        assert_eq!(hour_of(at(18, 9, 41)), at(18, 9, 0));
+    }
+
+    #[tokio::test]
+    async fn a_read_returns_every_writer_of_the_hours_it_meets() {
+        let (charts, _) = charts();
+        let p = ChartScope::Pipeline("p".into());
+        let other = ChartScope::Pipeline("q".into());
+        for c in [
+            chunk(&p, at(17, 23, 0), "e1", 1.0),
+            chunk(&p, at(18, 0, 0), "e1", 2.0),
+            // A failover in the hour: the next term writes its own chunk.
+            chunk(&p, at(18, 0, 0), "e2", 3.0),
+            chunk(&p, at(18, 2, 0), "e2", 4.0),
+            chunk(&other, at(18, 0, 0), "e1", 9.0),
+        ] {
+            charts.write(&c).await.unwrap();
+        }
+        let read = charts
+            .read(&p, at(17, 23, 30), at(18, 1, 0), at(18, 3, 0))
+            .await
+            .unwrap();
+        let got: Vec<(DateTime<Utc>, &str)> =
+            read.iter().map(|c| (c.hour, c.writer.as_str())).collect();
+        assert_eq!(
+            got,
+            [
+                (at(17, 23, 0), "e1"),
+                (at(18, 0, 0), "e1"),
+                (at(18, 0, 0), "e2")
+            ]
+        );
+        // An hour that only touches the range's end is left out.
+        assert!(
+            charts
+                .read(&p, at(18, 1, 0), at(18, 2, 0), at(18, 3, 0))
+                .await
+                .unwrap()
+                .is_empty()
+        );
+    }
+
+    #[tokio::test]
+    async fn an_ended_hour_is_read_once_and_the_current_one_every_time() {
+        let (charts, store) = charts();
+        let p = ChartScope::Pipeline("p".into());
+        charts
+            .write(&chunk(&p, at(18, 0, 0), "e1", 1.0))
+            .await
+            .unwrap();
+        charts
+            .write(&chunk(&p, at(18, 1, 0), "e1", 1.0))
+            .await
+            .unwrap();
+        let now = at(18, 1, 30);
+        charts.read(&p, at(18, 0, 0), now, now).await.unwrap();
+        // Both rewritten behind the reader's back: only the current hour is read again.
+        charts
+            .write(&chunk(&p, at(18, 0, 0), "e1", 5.0))
+            .await
+            .unwrap();
+        charts
+            .write(&chunk(&p, at(18, 1, 0), "e1", 6.0))
+            .await
+            .unwrap();
+        let read = charts.read(&p, at(18, 0, 0), now, now).await.unwrap();
+        let values: Vec<f64> = read.iter().map(|c| c.series["lag_seconds"].v[0]).collect();
+        assert_eq!(values, [1.0, 6.0]);
+        // A chunk that can't be read is left out.
+        store
+            .put(
+                &chart_key(&p, at(18, 1, 0), "e9"),
+                bytes::Bytes::from_static(b"{not json"),
+            )
+            .await
+            .unwrap();
+        assert_eq!(
+            charts.read(&p, at(18, 0, 0), now, now).await.unwrap().len(),
+            2
+        );
+    }
+
+    #[tokio::test]
+    async fn whole_days_past_the_retention_are_deleted() {
+        let (charts, store) = charts();
+        let p = ChartScope::Pipeline("p".into());
+        let n = ChartScope::Node("node-a".into());
+        for c in [
+            chunk(&p, at(16, 22, 0), "e1", 1.0),
+            chunk(&n, at(16, 23, 0), "i1", 1.0),
+            chunk(&p, at(17, 9, 0), "e1", 1.0),
+            chunk(&p, at(18, 9, 0), "e1", 1.0),
+        ] {
+            charts.write(&c).await.unwrap();
+        }
+        // 24 h before 2026-11-18 10:00 is the 17th: the 16th ended past it.
+        let deleted = charts
+            .prune(Duration::from_secs(24 * 3600), at(18, 10, 0))
+            .await
+            .unwrap();
+        assert_eq!(deleted, 2);
+        assert_eq!(
+            store.folders(CHARTS_PREFIX).await.unwrap(),
+            ["2026-11-17", "2026-11-18"]
+        );
+        assert_eq!(charts.prune(Duration::MAX, at(18, 10, 0)).await.unwrap(), 0);
+    }
+
+    #[test]
+    fn the_series_are_named_once_per_scope() {
+        for s in SERIES {
+            assert_eq!(
+                SERIES
+                    .iter()
+                    .filter(|o| o.scope == s.scope && o.name == s.name)
+                    .count(),
+                1,
+                "{}",
+                s.name
+            );
+        }
+        assert_eq!(
+            series_of(SeriesScope::Table, "commit_seconds").map(|s| s.every),
+            Some(TABLE_SAMPLE_EVERY)
+        );
+        assert!(series_of(SeriesScope::Node, "lag_seconds").is_none());
+    }
+}
````

`crates/data-avalanche-state/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 3ebb94d..6474445 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -34,6 +34,7 @@
 //! management API's `status.json` and `requests.json` with the desired state's transitions.
 
 mod anchor;
+mod charts;
 mod doc;
 mod events;
 mod generation;
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-state --lib charts::`
Expected: the build fails: the tests find no `ChartScope`, `Charts`, `ChartChunk`, `chart_key`, `SERIES` or `series_of` (nor the imports the code above them brings).

- [ ] **Step 3: Implement**

`crates/data-avalanche-state/src/charts.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/charts.rs b/crates/data-avalanche-state/src/charts.rs
index 7f4856d..dc55d51 100644
--- a/crates/data-avalanche-state/src/charts.rs
+++ b/crates/data-avalanche-state/src/charts.rs
@@ -5,6 +5,358 @@
 //! writers share an object and a failover's two terms are both kept. A day lists its scope's hours
 //! in one listing, and a whole day expires by deleting its folder.
 
+use std::collections::BTreeMap;
+use std::sync::{Arc, Mutex};
+use std::time::Duration;
+
+use chrono::{DateTime, NaiveDate, TimeDelta, Timelike, Utc};
+use data_avalanche_storage::escape_segment;
+use futures_util::{StreamExt, TryStreamExt, stream};
+use serde::{Deserialize, Serialize};
+
+use crate::{DocStore, Document, StateError};
+
+/// The folder of the chart history, under `globals.state`.
+pub const CHARTS_PREFIX: &str = "history";
+
+/// How many chunks a read loads at once.
+const READ_CONCURRENCY: usize = 16;
+
+/// The most ended hours a [`Charts`] keeps in memory; past it, the oldest go first.
+pub const CLOSED_KEPT: usize = 4096;
+
+/// How long after its hour ends a chunk is taken as final and kept in memory: its writer writes
+/// it once more within a minute of the hour's end.
+pub const CLOSED_AFTER: Duration = Duration::from_secs(300);
+
+/// What a series belongs to.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum SeriesScope {
+    Pipeline,
+    /// A pipeline's table: kept in the pipeline's chunks.
+    Table,
+    Node,
+}
+
+/// How samples that fall in one step of a coarser answer fold into one.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum Fold {
+    Max,
+    Mean,
+}
+
+/// One series the history keeps.
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub struct Series {
+    pub name: &'static str,
+    pub scope: SeriesScope,
+    pub unit: &'static str,
+    pub fold: Fold,
+    /// Between two samples.
+    pub every: Duration,
+}
+
+/// Between two samples of a pipeline's or a node's series.
+pub const SAMPLE_EVERY: Duration = Duration::from_secs(15);
+
+/// Between two samples of a table's series.
+pub const TABLE_SAMPLE_EVERY: Duration = Duration::from_secs(60);
+
+const fn series(name: &'static str, scope: SeriesScope, unit: &'static str, fold: Fold) -> Series {
+    Series {
+        name,
+        scope,
+        unit,
+        fold,
+        every: match scope {
+            SeriesScope::Table => TABLE_SAMPLE_EVERY,
+            SeriesScope::Pipeline | SeriesScope::Node => SAMPLE_EVERY,
+        },
+    }
+}
+
+/// Every series the history keeps (D-M13e.2).
+pub const SERIES: &[Series] = &[
+    series("lag_seconds", SeriesScope::Pipeline, "seconds", Fold::Max),
+    series(
+        "changes_per_minute",
+        SeriesScope::Pipeline,
+        "changes/min",
+        Fold::Mean,
+    ),
+    series("wal_held_bytes", SeriesScope::Pipeline, "bytes", Fold::Max),
+    series(
+        "binlog_lag_seconds",
+        SeriesScope::Pipeline,
+        "seconds",
+        Fold::Max,
+    ),
+    series(
+        "snapshot_rows_per_second",
+        SeriesScope::Pipeline,
+        "rows/s",
+        Fold::Mean,
+    ),
+    series(
+        "changes_per_minute",
+        SeriesScope::Table,
+        "changes/min",
+        Fold::Mean,
+    ),
+    series("files_rewritten", SeriesScope::Table, "files", Fold::Max),
+    series("commit_seconds", SeriesScope::Table, "seconds", Fold::Max),
+    series("memory_bytes", SeriesScope::Node, "bytes", Fold::Max),
+    series("cpu_cores", SeriesScope::Node, "cores", Fold::Mean),
+    series(
+        "lease_renew_seconds",
+        SeriesScope::Node,
+        "seconds",
+        Fold::Max,
+    ),
+];
+
+/// The series `name` of `scope`.
+pub fn series_of(scope: SeriesScope, name: &str) -> Option<&'static Series> {
+    SERIES.iter().find(|s| s.scope == scope && s.name == name)
+}
+
+/// Whose history a chunk is.
+#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
+#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
+pub enum ChartScope {
+    /// A pipeline, by id: its series and its tables'.
+    Pipeline(String),
+    /// A node, by its name in the cluster.
+    Node(String),
+}
+
+impl ChartScope {
+    fn path(&self) -> String {
+        match self {
+            Self::Pipeline(id) => format!("pipelines/{}", escape_segment(id)),
+            Self::Node(name) => format!("nodes/{}", escape_segment(name)),
+        }
+    }
+}
+
+/// The start of the hour `at` falls in.
+pub fn hour_of(at: DateTime<Utc>) -> DateTime<Utc> {
+    at.with_nanosecond(0)
+        .and_then(|t| t.with_second(0))
+        .and_then(|t| t.with_minute(0))
+        .expect("an hour's start exists")
+}
+
+/// The object of `scope`'s hour `hour` as `writer` writes it.
+pub fn chart_key(scope: &ChartScope, hour: DateTime<Utc>, writer: &str) -> String {
+    format!(
+        "{CHARTS_PREFIX}/{}/{}/{}-{}.json",
+        hour.format("%Y-%m-%d"),
+        scope.path(),
+        hour.format("%H"),
+        escape_segment(writer)
+    )
+}
+
+/// A series' samples in one hour: each at `t` seconds into the hour, of value `v`.
+#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct Points {
+    pub t: Vec<u16>,
+    pub v: Vec<f64>,
+}
+
+impl Points {
+    pub fn push(&mut self, t: u16, v: f64) {
+        self.t.push(t);
+        self.v.push(v);
+    }
+
+    pub fn is_empty(&self) -> bool {
+        self.t.is_empty()
+    }
+
+    /// Each sample, `(seconds into the hour, value)`; a document whose two lists differ in length
+    /// stops at the shorter.
+    pub fn iter(&self) -> impl Iterator<Item = (u16, f64)> + '_ {
+        self.t.iter().copied().zip(self.v.iter().copied())
+    }
+}
+
+/// One scope's hour as one writer sampled it.
+#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
+#[serde(deny_unknown_fields)]
+pub struct ChartChunk {
+    pub scope: ChartScope,
+    /// The hour's start.
+    pub hour: DateTime<Utc>,
+    /// The lease term (`e<epoch>`) or the process (`i<incarnation>`) that sampled it.
+    pub writer: String,
+    /// The scope's own series, by name.
+    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
+    pub series: BTreeMap<String, Points>,
+    /// A pipeline's tables' series: by table (its config key), then by name.
+    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
+    pub tables: BTreeMap<String, BTreeMap<String, Points>>,
+}
+
+impl Document for ChartChunk {
+    const KIND: &'static str = "chart history";
+    const SCHEMA_VERSION: u64 = 1;
+}
+
+impl ChartChunk {
+    pub fn new(scope: ChartScope, hour: DateTime<Utc>, writer: &str) -> Self {
+        Self {
+            scope,
+            hour,
+            writer: writer.to_owned(),
+            series: BTreeMap::new(),
+            tables: BTreeMap::new(),
+        }
+    }
+
+    pub fn key(&self) -> String {
+        chart_key(&self.scope, self.hour, &self.writer)
+    }
+
+    pub fn is_empty(&self) -> bool {
+        self.series.values().all(Points::is_empty)
+            && self
+                .tables
+                .values()
+                .all(|s| s.values().all(Points::is_empty))
+    }
+}
+
+/// The chart history in `globals.state`. Keeps the ended hours it read in memory, as they no
+/// longer change.
+#[derive(Debug)]
+pub struct Charts {
+    docs: DocStore,
+    closed: Mutex<BTreeMap<String, Arc<ChartChunk>>>,
+}
+
+impl Charts {
+    pub fn new(docs: DocStore) -> Self {
+        Self {
+            docs,
+            closed: Mutex::new(BTreeMap::new()),
+        }
+    }
+
+    /// Writes `chunk` over what its writer wrote of the hour before.
+    ///
+    /// # Errors
+    /// A storage failure.
+    pub async fn write(&self, chunk: &ChartChunk) -> Result<(), StateError> {
+        self.docs.put(&chunk.key(), chunk).await.map(|_| ())
+    }
+
+    /// `scope`'s chunks of the hours that meet `[since, until)`, each writer's apart, by hour. A
+    /// chunk that can't be read (another schema version, corrupt) is left out with a warning.
+    ///
+    /// # Errors
+    /// A storage failure.
+    pub async fn read(
+        &self,
+        scope: &ChartScope,
+        since: DateTime<Utc>,
+        until: DateTime<Utc>,
+        now: DateTime<Utc>,
+    ) -> Result<Vec<Arc<ChartChunk>>, StateError> {
+        if since >= until {
+            return Ok(Vec::new());
+        }
+        let store = self.docs.store();
+        let path = scope.path();
+        let mut keys = Vec::new();
+        let mut day = hour_of(since).date_naive();
+        let last = (until - TimeDelta::microseconds(1)).date_naive();
+        while day <= last {
+            let folder = format!("{CHARTS_PREFIX}/{}/{path}", day.format("%Y-%m-%d"));
+            for key in store.list(&folder).await? {
+                let hour = key
+                    .strip_prefix(&format!("{folder}/"))
+                    .and_then(|name| name.get(..2))
+                    .and_then(|h| h.parse::<u32>().ok())
+                    .and_then(|h| day.and_hms_opt(h, 0, 0))
+                    .map(|t| t.and_utc());
+                if let Some(hour) = hour
+                    && key.ends_with(".json")
+                    && hour < until
+                    && hour + TimeDelta::hours(1) > since
+                {
+                    keys.push((hour, key));
+                }
+            }
+            day = day.succ_opt().expect("a next day");
+        }
+        let closed_before = now - TimeDelta::from_std(CLOSED_AFTER).expect("minutes");
+        let read: Vec<Option<Arc<ChartChunk>>> = stream::iter(keys)
+            .map(|(hour, key)| async move {
+                let kept = hour + TimeDelta::hours(1) <= closed_before;
+                if kept && let Some(c) = self.closed.lock().expect("unpoisoned").get(&key) {
+                    return Ok(Some(Arc::clone(c)));
+                }
+                match self.docs.load::<ChartChunk>(&key).await {
+                    Ok(Some(v)) => {
+                        let chunk = Arc::new(v.doc);
+                        if kept {
+                            let mut closed = self.closed.lock().expect("unpoisoned");
+                            closed.insert(key, Arc::clone(&chunk));
+                            while closed.len() > CLOSED_KEPT {
+                                closed.pop_first();
+                            }
+                        }
+                        Ok(Some(chunk))
+                    }
+                    Ok(None) => Ok(None),
+                    Err(StateError::Store(e)) => Err(StateError::Store(e)),
+                    Err(e) => {
+                        tracing::warn!(key, "a chart history chunk left out: {e}");
+                        Ok(None)
+                    }
+                }
+            })
+            .buffered(READ_CONCURRENCY)
+            .try_collect()
+            .await?;
+        Ok(read.into_iter().flatten().collect())
+    }
+
+    /// Deletes every chunk of the days that ended `retention` or more before `now`, and returns
+    /// how many, as [`crate::Events::prune`] does: whole days, each in one batched delete; safe
+    /// on every node at once.
+    ///
+    /// # Errors
+    /// A storage failure; what was deleted before it stays deleted.
+    pub async fn prune(
+        &self,
+        retention: Duration,
+        now: DateTime<Utc>,
+    ) -> Result<usize, StateError> {
+        let Some(cutoff) = TimeDelta::from_std(retention)
+            .ok()
+            .and_then(|r| now.checked_sub_signed(r))
+        else {
+            return Ok(0);
+        };
+        let keep_from = cutoff.format("%Y-%m-%d").to_string();
+        let store = self.docs.store();
+        let mut deleted = 0;
+        for day in store.folders(CHARTS_PREFIX).await? {
+            if day.parse::<NaiveDate>().is_err() || day >= keep_from {
+                continue;
+            }
+            let keys = store.list(&format!("{CHARTS_PREFIX}/{day}")).await?;
+            store.delete_many(&keys).await?;
+            deleted += keys.len();
+        }
+        Ok(deleted)
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use chrono::TimeZone;
````

`crates/data-avalanche-state/src/doc.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/doc.rs b/crates/data-avalanche-state/src/doc.rs
index 09e7914..924db40 100644
--- a/crates/data-avalanche-state/src/doc.rs
+++ b/crates/data-avalanche-state/src/doc.rs
@@ -170,6 +170,12 @@ impl DocStore {
     ///
     /// # Errors
     /// `Store(Conflict)` when it moved or is gone; `Store(Contention)` when the outcome is unknown.
+    /// Writes `doc` at `key` unconditionally: for a document only one writer ever writes (a
+    /// chart history chunk, its own term's or process's).
+    pub async fn put<T: Document>(&self, key: &str, doc: &T) -> Result<ObjectVersion, StateError> {
+        Ok(self.store.put(key, encode(doc)).await?)
+    }
+
     pub async fn update<T: Document>(
         &self,
         key: &str,
````

`crates/data-avalanche-state/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 6474445..341c634 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -49,6 +49,10 @@ mod tables;
 mod test_support;
 
 pub use anchor::{ANCHOR_KEY, Anchor, AnchorDoc, HashAlgo, establish_anchor};
+pub use charts::{
+    CHARTS_PREFIX, CLOSED_AFTER, CLOSED_KEPT, ChartChunk, ChartScope, Charts, Fold, Points,
+    SAMPLE_EVERY, SERIES, Series, SeriesScope, TABLE_SAMPLE_EVERY, chart_key, hour_of, series_of,
+};
 pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
 pub use events::{
     Actor, EVENTS_PREFIX, EventDoc, EventPage, EventQuery, EventSeverity, Events, MAX_EVENTS,
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-state --lib charts::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state/src/charts.rs crates/data-avalanche-state/src/doc.rs crates/data-avalanche-state/src/lib.rs
git commit -m "feat(state): the chart history's store — a scope's hour per writer under history/, read over a range, whole days expired"
```

---

### Task 2: A table's last minute; the node's slowest renewal (`data-avalanche-pipeline`, `-telemetry`)

**Files:**
- Modify: `crates/data-avalanche-api/src/live.rs`
- Modify: `crates/data-avalanche-api/src/tables.rs`
- Modify: `crates/data-avalanche-pipeline/src/status.rs`
- Modify: `crates/data-avalanche-pipeline/src/writer.rs`
- Modify: `crates/data-avalanche-telemetry/src/lib.rs`
- Modify: `crates/data-avalanche-telemetry/tests/metrics.rs`

**Interfaces:**
- Consumes: `TableCommit`, `TableFigures`, `CHANGES_WINDOW` (M13d); `PipelineMetrics::renewed` (M8).
- Produces:
  - `TableCommit::took: Duration` (the commit and its retries; the writer sets it);
  - `TableFigures::slowest_commit(&self, now) -> Option<Duration>`, `TableFigures::most_rewritten(&self, now) -> Option<u64>`:
    over the [`CHANGES_WINDOW`] before `now`; a snapshot chunk's commit counts for the first, not the second;
  - `Metrics::take_slowest_renewal(&self) -> Option<Duration>`: the slowest renewal of any pipeline on the node since the
    last call, which forgets it.

- [ ] **Step 1: Write the failing tests**

The tests: `a_table_s_last_minute_has_its_slowest_commit_and_costliest_merge` (status), `the_slowest_renewal_since_the_last_sample_is_taken_once` (telemetry). The API's and the pipeline's tests that build a `TableCommit` gain `took`.

`crates/data-avalanche-api/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/live.rs b/crates/data-avalanche-api/src/live.rs
index 3ee3b66..d7b636f 100644
--- a/crates/data-avalanche-api/src/live.rs
+++ b/crates/data-avalanche-api/src/live.rs
@@ -470,6 +470,7 @@ mod tests {
                 files: Some(820),
             },
             at: now,
+            took: std::time::Duration::ZERO,
         });
         // A copy outside the config that ended: its job tells, the page doesn't list it.
         w.committed(&TableCommit {
@@ -479,6 +480,7 @@ mod tests {
             version: 2,
             cost: CommitCost::default(),
             at: now,
+            took: std::time::Duration::ZERO,
         });
         w
     }
````

`crates/data-avalanche-api/src/tables.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/tables.rs b/crates/data-avalanche-api/src/tables.rs
index a534ba3..b5d98ae 100644
--- a/crates/data-avalanche-api/src/tables.rs
+++ b/crates/data-avalanche-api/src/tables.rs
@@ -1121,6 +1121,7 @@ mod tests {
                 files: Some(files),
             },
             at: Utc::now(),
+            took: std::time::Duration::ZERO,
         }
     }
 
````

`crates/data-avalanche-pipeline/src/status.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
index ac1adde..bbbf0ca 100644
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -421,6 +421,7 @@ mod tests {
                 files: Some(10),
             },
             at: DateTime::from_timestamp(secs, 0).unwrap(),
+            took: Duration::from_millis(rows * 10),
         }
     }
 
@@ -453,6 +454,29 @@ mod tests {
         assert_eq!(f.changes_per_minute(at(1_200)), 0);
     }
 
+    /// The history samples a table's slowest commit and its costliest MERGE of the last minute
+    /// (D-M13e.2): a snapshot chunk's commit is slow but rewrites nothing of the stream's.
+    #[test]
+    fn a_table_s_last_minute_has_its_slowest_commit_and_costliest_merge() {
+        let mut st = WorkerStatus::default();
+        st.committed(&commit(7, false, 1_000));
+        st.committed(&commit(500, true, 1_030));
+        st.committed(&commit(3, false, 1_050));
+        let f = &st.tables[&TableRef::new("public", "t")];
+        let at = |secs| DateTime::from_timestamp(secs, 0).unwrap();
+        assert_eq!(f.slowest_commit(at(1_055)), Some(Duration::from_secs(5)));
+        assert_eq!(f.most_rewritten(at(1_055)), Some(7));
+        assert_eq!(
+            (f.slowest_commit(at(1_091)), f.most_rewritten(at(1_091))),
+            (Some(Duration::from_millis(30)), Some(3)),
+            "the first two are over a minute old"
+        );
+        assert_eq!(
+            (f.slowest_commit(at(1_200)), f.most_rewritten(at(1_200))),
+            (None, None)
+        );
+    }
+
     /// A snapshot commit moves the version and the last commit, not the cost: a re-snapshot's
     /// chunks rewrite what they copy, and the table's cost is its stream's last MERGE.
     #[test]
````

`crates/data-avalanche-telemetry/tests/metrics.rs`:

````diff
diff --git a/crates/data-avalanche-telemetry/tests/metrics.rs b/crates/data-avalanche-telemetry/tests/metrics.rs
index 08ffdd9..f5fb03f 100644
--- a/crates/data-avalanche-telemetry/tests/metrics.rs
+++ b/crates/data-avalanche-telemetry/tests/metrics.rs
@@ -77,6 +77,29 @@ fn renewals_count_their_failures_and_time_their_successes() {
     );
 }
 
+/// The node's chart of lease renewals samples the slowest of any pipeline since its last sample
+/// (D-M13e.2).
+#[test]
+fn the_slowest_renewal_since_the_last_sample_is_taken_once() {
+    let m = Metrics::new();
+    assert_eq!(m.take_slowest_renewal(), None);
+    let p = m.pipeline(labels(), false);
+    let q = m.pipeline(
+        PipelineLabels {
+            pipeline: "billing".into(),
+            ..labels()
+        },
+        false,
+    );
+    p.renewed(Duration::from_millis(20));
+    q.renewed(Duration::from_millis(610));
+    p.renewed(Duration::from_millis(41));
+    assert_eq!(m.take_slowest_renewal(), Some(Duration::from_millis(610)));
+    assert_eq!(m.take_slowest_renewal(), None);
+    q.renewed(Duration::from_millis(9));
+    assert_eq!(m.take_slowest_renewal(), Some(Duration::from_millis(9)));
+}
+
 #[test]
 fn counters_keep_counting_across_workers() {
     let m = Metrics::new();
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-pipeline --lib status:: && cargo test -p data-avalanche-telemetry --test metrics`
Expected: the build fails: `TableCommit` has no field `took`; `TableFigures` has no `slowest_commit` or `most_rewritten` (and, past it, `Metrics` has no `take_slowest_renewal`).

- [ ] **Step 3: Implement**

`crates/data-avalanche-pipeline/src/status.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/status.rs b/crates/data-avalanche-pipeline/src/status.rs
index bbbf0ca..1f08c72 100644
--- a/crates/data-avalanche-pipeline/src/status.rs
+++ b/crates/data-avalanche-pipeline/src/status.rs
@@ -192,9 +192,12 @@ pub struct TableCommit {
     pub version: u64,
     pub cost: CommitCost,
     pub at: DateTime<Utc>,
+    /// How long it took, its retries included.
+    pub took: Duration,
 }
 
-/// The window [`TableFigures::changes_per_minute`] counts over.
+/// The window [`TableFigures::changes_per_minute`], [`TableFigures::slowest_commit`] and
+/// [`TableFigures::most_rewritten`] look over.
 pub const CHANGES_WINDOW: Duration = Duration::from_secs(60);
 
 /// A table's figures since the worker started, from its commits.
@@ -202,8 +205,8 @@ pub const CHANGES_WINDOW: Duration = Duration::from_secs(60);
 pub struct TableFigures {
     /// The stream's changes committed to it; a snapshot's rows don't count.
     pub changes: u64,
-    /// The commits of the last [`CHANGES_WINDOW`] that carried changes: when, and how many.
-    recent: VecDeque<(DateTime<Utc>, u64)>,
+    /// The commits of the last [`CHANGES_WINDOW`].
+    recent: VecDeque<Recent>,
     pub last_commit: Option<DateTime<Utc>>,
     /// The table's version after the last commit.
     pub version: Option<u64>,
@@ -212,6 +215,17 @@ pub struct TableFigures {
     pub cost: Option<CommitCost>,
 }
 
+/// One commit of a table's last [`CHANGES_WINDOW`].
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+struct Recent {
+    at: DateTime<Utc>,
+    /// The stream's changes it wrote; none for a snapshot chunk.
+    changes: u64,
+    took: Duration,
+    /// The files it rewrote: a stream commit that wrote rows only.
+    rewritten: Option<u64>,
+}
+
 impl TableFigures {
     fn add(&mut self, c: &TableCommit) {
         self.last_commit = Some(c.at);
@@ -220,16 +234,18 @@ impl TableFigures {
         if c.rows > 0 && !c.snapshot {
             self.cost = Some(c.cost);
         }
-        if !c.snapshot && c.rows > 0 {
+        let stream = !c.snapshot && c.rows > 0;
+        if stream {
             self.changes += c.rows;
-            self.recent.push_back((c.at, c.rows));
         }
+        self.recent.push_back(Recent {
+            at: c.at,
+            changes: if stream { c.rows } else { 0 },
+            took: c.took,
+            rewritten: stream.then_some(c.cost.files_rewritten),
+        });
         let window = chrono::Duration::from_std(CHANGES_WINDOW).expect("a minute");
-        while self
-            .recent
-            .front()
-            .is_some_and(|(at, _)| *at <= c.at - window)
-        {
+        while self.recent.front().is_some_and(|r| r.at <= c.at - window) {
             self.recent.pop_front();
         }
     }
@@ -237,12 +253,25 @@ impl TableFigures {
     /// The changes committed in the [`CHANGES_WINDOW`] before `now`.
     #[must_use]
     pub fn changes_per_minute(&self, now: DateTime<Utc>) -> u64 {
+        self.window(now).map(|r| r.changes).sum()
+    }
+
+    /// The longest commit of the [`CHANGES_WINDOW`] before `now`; `None` without one.
+    #[must_use]
+    pub fn slowest_commit(&self, now: DateTime<Utc>) -> Option<Duration> {
+        self.window(now).map(|r| r.took).max()
+    }
+
+    /// The most files a stream commit of the [`CHANGES_WINDOW`] before `now` rewrote; `None`
+    /// when none wrote rows.
+    #[must_use]
+    pub fn most_rewritten(&self, now: DateTime<Utc>) -> Option<u64> {
+        self.window(now).filter_map(|r| r.rewritten).max()
+    }
+
+    fn window(&self, now: DateTime<Utc>) -> impl Iterator<Item = &Recent> {
         let window = chrono::Duration::from_std(CHANGES_WINDOW).expect("a minute");
-        self.recent
-            .iter()
-            .filter(|(at, _)| *at > now - window)
-            .map(|(_, rows)| rows)
-            .sum()
+        self.recent.iter().filter(move |r| r.at > now - window)
     }
 }
 
````

`crates/data-avalanche-pipeline/src/writer.rs`:

````diff
diff --git a/crates/data-avalanche-pipeline/src/writer.rs b/crates/data-avalanche-pipeline/src/writer.rs
index cc3d190..010a6b3 100644
--- a/crates/data-avalanche-pipeline/src/writer.rs
+++ b/crates/data-avalanche-pipeline/src/writer.rs
@@ -258,6 +258,7 @@ impl Writer {
                         version,
                         cost,
                         at: chrono::Utc::now(),
+                        took: at.elapsed(),
                     }));
                     observer.committed(rows, at.elapsed());
                     span.record("outcome", "committed");
````

`crates/data-avalanche-telemetry/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-telemetry/src/lib.rs b/crates/data-avalanche-telemetry/src/lib.rs
index acc3f6e..cc0a0bb 100644
--- a/crates/data-avalanche-telemetry/src/lib.rs
+++ b/crates/data-avalanche-telemetry/src/lib.rs
@@ -229,6 +229,8 @@ struct Families {
     lease_state: Family<Labels, Gauge>,
     lease_renew_failures: Family<Labels, Counter>,
     lease_renew_latency: HistogramFamily,
+    /// The node's slowest renewal since the history last took it (D-M13e.2).
+    slowest_renewal: Mutex<Option<Duration>>,
     owned_pipelines: Gauge,
     cluster_members: Gauge,
     config_generation: Gauge,
@@ -305,6 +307,7 @@ impl Metrics {
             lease_state: Family::default(),
             lease_renew_failures: Family::default(),
             lease_renew_latency: Family::new_with_constructor(renew_latency),
+            slowest_renewal: Mutex::new(None),
             owned_pipelines: Gauge::default(),
             cluster_members: Gauge::default(),
             config_generation: Gauge::default(),
@@ -610,6 +613,16 @@ impl Metrics {
         self.families.ring_generation.set(gauge(generation));
     }
 
+    /// The slowest lease renewal of any pipeline on this node since the last call, which forgets
+    /// it; `None` when none renewed.
+    pub fn take_slowest_renewal(&self) -> Option<Duration> {
+        self.families
+            .slowest_renewal
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner)
+            .take()
+    }
+
     /// The handle that sets one pipeline's series. `per_table` is `sink.metrics.per_table`.
     pub fn pipeline(&self, labels: PipelineLabels, per_table: bool) -> PipelineMetrics {
         PipelineMetrics {
@@ -718,6 +731,12 @@ impl PipelineMetrics {
             .lease_renew_latency
             .get_or_create(&self.base())
             .observe(took.as_secs_f64());
+        let mut slowest = self
+            .families
+            .slowest_renewal
+            .lock()
+            .unwrap_or_else(PoisonError::into_inner);
+        *slowest = (*slowest).max(Some(took));
     }
 
     /// `None` drops the series: the source doesn't report it, or its read failed.
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-pipeline --lib status:: && cargo test -p data-avalanche-telemetry --test metrics`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/live.rs crates/data-avalanche-api/src/tables.rs crates/data-avalanche-pipeline/src/status.rs crates/data-avalanche-pipeline/src/writer.rs crates/data-avalanche-telemetry/src/lib.rs crates/data-avalanche-telemetry/tests/metrics.rs
git commit -m "feat(pipeline): a table's slowest commit and costliest MERGE of the last minute; the node's slowest lease renewal"
```

---

### Task 3: The history sampler (`data-avalanche-cluster`, `data-avalanche`)

**Files:**
- Regenerate: `Cargo.lock`
- Modify: `crates/data-avalanche-api/src/live.rs`
- Modify: `crates/data-avalanche-cluster/Cargo.toml`
- Modify: `crates/data-avalanche-cluster/src/controller.rs`
- Create: `crates/data-avalanche-cluster/src/history.rs`
- Modify: `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: Task 1's `Charts`, `ChartChunk`, `ChartScope`, `hour_of`, `SAMPLE_EVERY`, `TABLE_SAMPLE_EVERY`; Task 2's
  `slowest_commit`, `most_rewritten`, `take_slowest_renewal`; `ControllerStatus` (M6b, M13d); `ProcessFigures` (M13b);
  `ClusterReconciler::watch_statuses` (M13d).
- Produces (cluster crate root):
  - `Sampler::new(Arc<Charts>, node: &str, incarnation: u64)`;
    `Sampler::sample(&mut self, now, &BTreeMap<String, ControllerStatus>, &ProcessFigures, slowest_renewal: Option<Duration>, tables: bool)`;
    `Sampler::write(&mut self, now) -> usize` (the writes that failed); `Sampler::held(&self) -> usize`;
  - `sample_history(Sampler, statuses, process, Metrics)` and `expire_history(Arc<Charts>, watch::Receiver<Duration>)`,
    each a task until aborted;
  - `WRITE_EVERY` (60 s), `UNWRITTEN_KEPT` (1 h), `HISTORY_PRUNE_EVERY` (1 h);
  - `ControllerStatus::fresh_as_of(&self, now) -> Option<DateTime<Utc>>`, moved from the API's `live.rs` (`None`
    without a worker).
- The binary starts both tasks on every node; a reload's `history.retention` reaches the expiry.

- [ ] **Step 1: Write the failing tests**

The tests: `a_held_pipeline_and_the_node_are_sampled_and_written_at_once`, `a_chunk_is_written_each_minute_while_it_changes`, `a_table_new_to_the_term_is_sampled_and_written_at_once`, `a_new_term_or_hour_ends_the_chunk_before_it`, `samples_wait_through_a_storage_outage`, `a_snapshot_s_rate_is_its_rows_between_samples`. The new module holds only its tests for now.

`crates/data-avalanche-cluster/src/history.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/history.rs b/crates/data-avalanche-cluster/src/history.rs
new file mode 100644
index 0000000..b4d753d
--- /dev/null
+++ b/crates/data-avalanche-cluster/src/history.rs
@@ -0,0 +1,388 @@
+//! The history sampler (Admin_Console §7, D-M13e.3): every [`SAMPLE_EVERY`] each node samples the
+//! series it owns, those of the pipelines whose lease it holds (their tables' every
+//! [`TABLE_SAMPLE_EVERY`], and a table new to the term's chunk at once) and its own, into the
+//! current hour's chunks. A chunk is written when it gets its first sample or a new series, then
+//! every [`WRITE_EVERY`] while it changes, and once more when its hour or its lease term ends. A write that fails is tried again at the next sample; the samples wait
+//! in memory, never holding up a pipeline. Another task expires the days past
+//! `globals.console.history.retention`.
+
+#[cfg(test)]
+mod tests {
+    use chrono::TimeZone;
+    use data_avalanche_core::{CommitCost, TableRef};
+    use data_avalanche_pipeline::{Phase, SnapshotProgress, TableCommit, WorkerStatus};
+    use data_avalanche_state::DocStore;
+    use data_avalanche_storage::VersionedStore;
+    use data_avalanche_telemetry::Reading;
+
+    use super::*;
+    use crate::test_support::{Fault, Kind, ScriptedStore};
+
+    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
+        Utc.with_ymd_and_hms(2026, 11, 18, h, m, s).unwrap()
+    }
+
+    fn sampler() -> (Sampler, Arc<Charts>, Arc<ScriptedStore>) {
+        let store = ScriptedStore::new();
+        let dyn_store: Arc<dyn VersionedStore> = store.clone();
+        let charts = Arc::new(Charts::new(DocStore::new(dyn_store)));
+        (
+            Sampler::new(Arc::clone(&charts), "node-a", 7),
+            charts,
+            store,
+        )
+    }
+
+    /// A running pipeline's status at `now`: 30 s behind, one table with two commits, the slot
+    /// holding 4 KiB.
+    fn running(epoch: u64, now: DateTime<Utc>) -> ControllerStatus {
+        let mut w = WorkerStatus {
+            phase: Phase::Streaming,
+            retained_bytes: Some(4096),
+            ..WorkerStatus::default()
+        };
+        for (rows, secs, millis) in [(40, 20, 120), (20, 10, 900)] {
+            w.committed(&TableCommit {
+                table: TableRef::new("public", "orders"),
+                rows,
+                snapshot: false,
+                version: 3,
+                cost: CommitCost {
+                    files_rewritten: rows / 10,
+                    files: Some(12),
+                },
+                at: now - TimeDelta::seconds(secs),
+                took: Duration::from_millis(millis),
+            });
+        }
+        ControllerStatus {
+            actual: Some(Actual::Running),
+            owner_epoch: Some(epoch),
+            worker: Some(w),
+            pending_since_micros: Some((now - TimeDelta::seconds(30)).timestamp_micros()),
+            ..ControllerStatus::default()
+        }
+    }
+
+    fn statuses(list: &[(&str, ControllerStatus)]) -> BTreeMap<String, ControllerStatus> {
+        list.iter()
+            .map(|(id, s)| ((*id).to_owned(), s.clone()))
+            .collect()
+    }
+
+    fn figures(memory: u64, cores: f64) -> ProcessFigures {
+        ProcessFigures {
+            reading: Reading {
+                memory_bytes: Some(memory),
+                ..Reading::default()
+            },
+            cpu_cores: Some(cores),
+        }
+    }
+
+    async fn stored(charts: &Charts, scope: &ChartScope, now: DateTime<Utc>) -> Vec<ChartChunk> {
+        charts
+            .read(
+                scope,
+                now - TimeDelta::hours(2),
+                now + TimeDelta::hours(1),
+                now,
+            )
+            .await
+            .unwrap()
+            .iter()
+            .map(|c| (**c).clone())
+            .collect()
+    }
+
+    #[tokio::test]
+    async fn a_held_pipeline_and_the_node_are_sampled_and_written_at_once() {
+        let (mut s, charts, _) = sampler();
+        let now = at(9, 0, 15);
+        let st = statuses(&[
+            ("orders", running(4, now)),
+            // A standby controller is another node's pipeline.
+            (
+                "other",
+                ControllerStatus {
+                    actual: Some(Actual::Standby { holder: None }),
+                    ..ControllerStatus::default()
+                },
+            ),
+        ]);
+        s.sample(
+            now,
+            &st,
+            &figures(1 << 30, 0.5),
+            Some(Duration::from_millis(41)),
+            true,
+        );
+        assert_eq!(s.write(now).await, 0);
+        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), now).await;
+        assert_eq!(p.len(), 1);
+        assert_eq!(p[0].writer, "e4");
+        let series = |name: &str| (p[0].series[name].t.clone(), p[0].series[name].v.clone());
+        assert_eq!(series("lag_seconds"), (vec![15], vec![30.0]));
+        assert_eq!(series("changes_per_minute"), (vec![15], vec![60.0]));
+        assert_eq!(series("wal_held_bytes"), (vec![15], vec![4096.0]));
+        assert!(!p[0].series.contains_key("binlog_lag_seconds"));
+        let t = &p[0].tables["public.orders"];
+        assert_eq!(t["changes_per_minute"].v, [60.0]);
+        assert_eq!(t["files_rewritten"].v, [4.0]);
+        assert_eq!(t["commit_seconds"].v, [0.9]);
+        assert!(
+            stored(&charts, &ChartScope::Pipeline("other".into()), now)
+                .await
+                .is_empty()
+        );
+        let n = stored(&charts, &ChartScope::Node("node-a".into()), now).await;
+        assert_eq!(n[0].writer, "i7");
+        assert_eq!(n[0].series["memory_bytes"].v, [f64::from(1u32 << 30)]);
+        assert_eq!(n[0].series["cpu_cores"].v, [0.5]);
+        assert_eq!(n[0].series["lease_renew_seconds"].v, [0.041]);
+    }
+
+    #[tokio::test]
+    async fn a_chunk_is_written_each_minute_while_it_changes() {
+        let (mut s, charts, _) = sampler();
+        let none = ProcessFigures::default();
+        let mut now = at(9, 0, 0);
+        let mut writes = Vec::new();
+        for _ in 0..8 {
+            let st = statuses(&[("orders", running(4, now))]);
+            s.sample(now, &st, &none, None, false);
+            s.write(now).await;
+            let p = stored(&charts, &ChartScope::Pipeline("orders".into()), now).await;
+            writes.push(p[0].series["lag_seconds"].t.len());
+            now += TimeDelta::seconds(15);
+        }
+        // Written with the first sample, then a minute later, and a minute after that.
+        assert_eq!(writes, [1, 1, 1, 1, 5, 5, 5, 5]);
+        // A table is sampled when it is new to the chunk, then only when asked.
+        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), now).await;
+        assert_eq!(p[0].tables["public.orders"]["changes_per_minute"].t, [0]);
+    }
+
+    /// A table the worker starts tracking is sampled at once, and its chunk written at the next
+    /// write rather than a minute after the last: a page sees a new table within a sample.
+    #[tokio::test]
+    async fn a_table_new_to_the_term_is_sampled_and_written_at_once() {
+        let (mut s, charts, _) = sampler();
+        let none = ProcessFigures::default();
+        let t1 = at(9, 0, 0);
+        s.sample(
+            t1,
+            &statuses(&[("orders", running(4, t1))]),
+            &none,
+            None,
+            false,
+        );
+        s.write(t1).await;
+        let t2 = at(9, 0, 15);
+        let mut st = running(4, t2);
+        st.worker
+            .as_mut()
+            .unwrap()
+            .tracked
+            .insert(TableRef::new("public", "items"));
+        s.sample(t2, &statuses(&[("orders", st)]), &none, None, false);
+        s.write(t2).await;
+        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), t2).await;
+        let items = &p[0].tables["public.items"];
+        assert_eq!(items["changes_per_minute"].t, [15]);
+        assert_eq!(items["changes_per_minute"].v, [0.0], "no commit yet");
+        assert!(!items.contains_key("commit_seconds"));
+        assert_eq!(p[0].tables["public.orders"]["changes_per_minute"].t, [0]);
+    }
+
+    /// A failover in the hour: the next term writes its own chunk, and the last one of the
+    /// earlier term is written once more and let go.
+    #[tokio::test]
+    async fn a_new_term_or_hour_ends_the_chunk_before_it() {
+        let (mut s, charts, _) = sampler();
+        let none = ProcessFigures::default();
+        let t1 = at(9, 59, 30);
+        s.sample(
+            t1,
+            &statuses(&[("orders", running(4, t1))]),
+            &none,
+            None,
+            false,
+        );
+        s.write(t1).await;
+        let t2 = at(9, 59, 45);
+        s.sample(
+            t2,
+            &statuses(&[("orders", running(4, t2))]),
+            &none,
+            None,
+            false,
+        );
+        s.write(t2).await;
+        // The hour ends: its chunk is written with its last sample.
+        let t3 = at(10, 0, 0);
+        s.sample(
+            t3,
+            &statuses(&[("orders", running(4, t3))]),
+            &none,
+            None,
+            false,
+        );
+        s.write(t3).await;
+        // The lease moves to epoch 5 on this node: the epoch-4 chunk ends.
+        let t4 = at(10, 0, 15);
+        s.sample(
+            t4,
+            &statuses(&[("orders", running(5, t4))]),
+            &none,
+            None,
+            false,
+        );
+        s.write(t4).await;
+        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), t4).await;
+        let got: Vec<(DateTime<Utc>, &str, Vec<u16>)> = p
+            .iter()
+            .map(|c| (c.hour, c.writer.as_str(), c.series["lag_seconds"].t.clone()))
+            .collect();
+        assert_eq!(
+            got,
+            [
+                (at(9, 0, 0), "e4", vec![3570, 3585]),
+                (at(10, 0, 0), "e4", vec![0]),
+                (at(10, 0, 0), "e5", vec![15]),
+            ]
+        );
+        // The lease lost: the pipeline's chunks are let go once written.
+        let t5 = at(10, 0, 30);
+        s.sample(t5, &statuses(&[]), &none, None, false);
+        s.write(t5).await;
+        assert_eq!(s.held(), 0);
+    }
+
+    /// A storage outage: the samples wait in memory and land with the next write that goes
+    /// through; an hour that never could is dropped an hour after it ended.
+    #[tokio::test]
+    async fn samples_wait_through_a_storage_outage() {
+        let (mut s, charts, store) = sampler();
+        let none = ProcessFigures::default();
+        store.fail_always(Some(Fault {
+            kind: Kind::Transient,
+            lands: false,
+        }));
+        let t1 = at(9, 0, 0);
+        s.sample(
+            t1,
+            &statuses(&[("orders", running(4, t1))]),
+            &none,
+            None,
+            false,
+        );
+        assert_eq!(s.write(t1).await, 1);
+        let t2 = at(9, 0, 15);
+        s.sample(
+            t2,
+            &statuses(&[("orders", running(4, t2))]),
+            &none,
+            None,
+            false,
+        );
+        assert_eq!(s.write(t2).await, 1, "tried again at the next sample");
+        store.fail_always(None);
+        let t3 = at(9, 0, 30);
+        s.sample(
+            t3,
+            &statuses(&[("orders", running(4, t3))]),
+            &none,
+            None,
+            false,
+        );
+        assert_eq!(s.write(t3).await, 0);
+        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), t3).await;
+        assert_eq!(p[0].series["lag_seconds"].t, [0, 15, 30]);
+        // Down again to the hour's end: the 9 o'clock chunk's last samples can't be written.
+        store.fail_always(Some(Fault {
+            kind: Kind::Transient,
+            lands: false,
+        }));
+        let late = at(9, 59, 45);
+        s.sample(
+            late,
+            &statuses(&[("orders", running(4, late))]),
+            &none,
+            None,
+            false,
+        );
+        s.write(late).await;
+        let t4 = at(10, 0, 0);
+        s.sample(
+            t4,
+            &statuses(&[("orders", running(4, t4))]),
+            &none,
+            None,
+            false,
+        );
+        s.write(t4).await;
+        assert_eq!(s.held(), 2);
+        let t5 = at(11, 0, 1);
+        s.sample(t5, &statuses(&[]), &none, None, false);
+        s.write(t5).await;
+        assert_eq!(
+            s.held(),
+            1,
+            "the 9 o'clock chunk dropped, the 10 o'clock one kept"
+        );
+    }
+
+    /// A snapshot's rate is its rows done between two samples of one term: none at the first
+    /// sample, nor across terms.
+    #[tokio::test]
+    async fn a_snapshot_s_rate_is_its_rows_between_samples() {
+        let (mut s, charts, _) = sampler();
+        let none = ProcessFigures::default();
+        let copying = |epoch, now, rows| {
+            let mut st = running(epoch, now);
+            st.worker.as_mut().unwrap().snapshots.insert(
+                TableRef::new("public", "orders"),
+                SnapshotProgress {
+                    rows_done: rows,
+                    waiting: false,
+                },
+            );
+            st
+        };
+        let t1 = at(9, 0, 0);
+        s.sample(
+            t1,
+            &statuses(&[("orders", copying(4, t1, 1_000))]),
+            &none,
+            None,
+            false,
+        );
+        let t2 = at(9, 0, 15);
+        s.sample(
+            t2,
+            &statuses(&[("orders", copying(4, t2, 31_000))]),
+            &none,
+            None,
+            false,
+        );
+        let t3 = at(9, 0, 30);
+        s.sample(
+            t3,
+            &statuses(&[("orders", copying(5, t3, 40_000))]),
+            &none,
+            None,
+            false,
+        );
+        s.write(t3).await;
+        let p = stored(&charts, &ChartScope::Pipeline("orders".into()), t3).await;
+        let rate = |writer: &str| {
+            p.iter()
+                .find(|c| c.writer == writer)
+                .and_then(|c| c.series.get("snapshot_rows_per_second"))
+                .map(|r| r.v.clone())
+        };
+        assert_eq!(rate("e4"), Some(vec![2_000.0]));
+        assert_eq!(rate("e5"), None);
+    }
+}
````

`crates/data-avalanche-cluster/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 737bfe2..35cdf1d 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -13,6 +13,7 @@ pub mod discovery;
 mod error;
 pub mod events;
 pub mod gossip;
+pub mod history;
 mod jobs;
 pub mod lease;
 pub mod node;
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-cluster --lib history::`
Expected: the build fails: the tests find no `Sampler`, `ChartScope`, `Charts`, `ControllerStatus` or `ProcessFigures` (nor the imports the code above them brings).

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/live.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/live.rs b/crates/data-avalanche-api/src/live.rs
index d7b636f..39140b9 100644
--- a/crates/data-avalanche-api/src/live.rs
+++ b/crates/data-avalanche-api/src/live.rs
@@ -69,28 +69,6 @@ fn replicating(s: &ControllerStatus, w: &WorkerStatus) -> bool {
     }
 }
 
-/// What lake readers see of the pipeline's tables as of now, over the controller's lease term
-/// (its workers' freshness carried across their runs): every change read before the oldest one
-/// not yet durable (`replication_lag_seconds`, System-Overview); while the worker streams, all of
-/// them when none waits. Otherwise (a source outage, a retry, an error, a pause, a stop) as of
-/// when the term was last caught up; `None` when that isn't known either.
-fn fresh_as_of(
-    s: &ControllerStatus,
-    w: &WorkerStatus,
-    now: DateTime<Utc>,
-) -> Option<DateTime<Utc>> {
-    let pending = s
-        .pending_since_micros
-        .and_then(DateTime::from_timestamp_micros)
-        .map(|at| at.min(now));
-    let streams = matches!(s.actual, Some(Actual::Running)) && w.phase == Phase::Streaming;
-    if streams {
-        Some(pending.unwrap_or(now))
-    } else {
-        pending.or_else(|| s.caught_up_at.map(|at| at.min(now)))
-    }
-}
-
 /// One table's row, as the pipeline's live figures list it (and the tables, and a table's page
 /// as its `live`): the same fields whether its worker shows it or only its halt is known.
 struct Row<'a> {
@@ -175,7 +153,7 @@ fn tables(
     now: DateTime<Utc>,
 ) -> Vec<Value> {
     let replicating = replicating(s, w);
-    let fresh = fresh_as_of(s, w, now);
+    let fresh = s.fresh_as_of(now);
     let window = chrono::Duration::from_std(FRESH_WITHIN).expect("a minute");
     // A copy outside the config is listed while it copies: once it ends, or halts, its job
     // tells (its figures stay in `tables` until the worker ends).
@@ -233,7 +211,7 @@ fn answer(
     let (actual, error) = s.actual.as_ref().map_or(("starting", None), actual_of);
     let w = s.worker.as_ref();
     let worker = w.map(|w| {
-        let fresh = fresh_as_of(s, w, now);
+        let fresh = s.fresh_as_of(now);
         json!({
             "phase": phase_name(&w.phase),
             "reconnecting": match &w.phase {
````

`crates/data-avalanche-cluster/Cargo.toml`:

````diff
diff --git a/crates/data-avalanche-cluster/Cargo.toml b/crates/data-avalanche-cluster/Cargo.toml
index d2ef726..ba15f12 100644
--- a/crates/data-avalanche-cluster/Cargo.toml
+++ b/crates/data-avalanche-cluster/Cargo.toml
@@ -20,6 +20,7 @@ data-avalanche-state.workspace = true
 data-avalanche-storage.workspace = true
 data-avalanche-telemetry.workspace = true
 foca.workspace = true
+futures-util.workspace = true
 gethostname.workspace = true
 getrandom.workspace = true
 if-addrs.workspace = true
````

`crates/data-avalanche-cluster/src/controller.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 37007ff..44afad6 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -135,6 +135,28 @@ pub struct ControllerStatus {
     pub caught_up_at: Option<DateTime<Utc>>,
 }
 
+impl ControllerStatus {
+    /// What lake readers see of the pipeline's tables as of `now`, over the lease term (its
+    /// workers' freshness carried across their runs): every change read before the oldest one
+    /// not yet durable (`replication_lag_seconds`, System-Overview); while the worker streams,
+    /// all of them when none waits. Otherwise (a source outage, a retry, an error, a pause, a
+    /// stop) as of when the term was last caught up; `None` when that isn't known either, or no
+    /// worker runs. The pipeline's live figures and its history's `lag_seconds` read it.
+    pub fn fresh_as_of(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
+        let w = self.worker.as_ref()?;
+        let pending = self
+            .pending_since_micros
+            .and_then(DateTime::from_timestamp_micros)
+            .map(|at| at.min(now));
+        let streams = matches!(self.actual, Some(Actual::Running)) && w.phase == Phase::Streaming;
+        if streams {
+            Some(pending.unwrap_or(now))
+        } else {
+            pending.or_else(|| self.caught_up_at.map(|at| at.min(now)))
+        }
+    }
+}
+
 /// A controller's timings.
 #[derive(Debug, Clone)]
 pub struct ControllerSettings {
````

`crates/data-avalanche-cluster/src/history.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/history.rs b/crates/data-avalanche-cluster/src/history.rs
index b4d753d..0e0247a 100644
--- a/crates/data-avalanche-cluster/src/history.rs
+++ b/crates/data-avalanche-cluster/src/history.rs
@@ -6,6 +6,354 @@
 //! in memory, never holding up a pipeline. Another task expires the days past
 //! `globals.console.history.retention`.
 
+use std::collections::{BTreeMap, BTreeSet};
+use std::sync::Arc;
+use std::time::Duration;
+
+use chrono::{DateTime, TimeDelta, Utc};
+use data_avalanche_core::TableRef;
+use data_avalanche_state::{
+    ChartChunk, ChartScope, Charts, SAMPLE_EVERY, TABLE_SAMPLE_EVERY, chart_key, hour_of,
+};
+use data_avalanche_telemetry::{Metrics, ProcessFigures};
+use futures_util::{StreamExt, stream};
+use tokio::sync::watch;
+use tokio::time::MissedTickBehavior;
+
+use crate::controller::{Actual, ControllerStatus};
+
+/// Between two writes of a chunk that keeps changing.
+pub const WRITE_EVERY: Duration = Duration::from_secs(60);
+
+/// How long after its hour ends a chunk that can't be written is still kept for another try;
+/// past it, its samples are dropped with a warning.
+pub const UNWRITTEN_KEPT: Duration = Duration::from_secs(3600);
+
+/// How often the expired days are deleted.
+pub const HISTORY_PRUNE_EVERY: Duration = Duration::from_secs(3600);
+
+/// How many chunks are written at once.
+const WRITE_CONCURRENCY: usize = 8;
+
+/// A chunk being sampled, or ended and waiting for its last write.
+#[derive(Debug)]
+struct Open {
+    chunk: ChartChunk,
+    /// Samples since the last write.
+    dirty: bool,
+    /// A series it didn't have at the last write: written at the next call.
+    fresh: bool,
+    /// The last write; `None` before the first.
+    written_at: Option<DateTime<Utc>>,
+    /// Its hour or its lease term ended: written once more, then dropped.
+    ended: bool,
+}
+
+/// The node's sampler: what it samples and holds until written.
+#[derive(Debug)]
+pub struct Sampler {
+    charts: Arc<Charts>,
+    node: String,
+    incarnation: u64,
+    /// By chunk key.
+    open: BTreeMap<String, Open>,
+    /// Per pipeline, its snapshots' rows done at the last sample, and its term's writer.
+    copied: BTreeMap<String, (String, DateTime<Utc>, u64)>,
+}
+
+/// The values one sample adds to a scope: the scope's own series, then each table's.
+type Values = (
+    Vec<(&'static str, f64)>,
+    Vec<(String, Vec<(&'static str, f64)>)>,
+);
+
+impl Sampler {
+    /// The sampler of node `node`, whose process is `incarnation`.
+    pub fn new(charts: Arc<Charts>, node: &str, incarnation: u64) -> Self {
+        Self {
+            charts,
+            node: node.to_owned(),
+            incarnation,
+            open: BTreeMap::new(),
+            copied: BTreeMap::new(),
+        }
+    }
+
+    /// Samples at `now`: each pipeline in `statuses` this node runs (its tables too when
+    /// `tables`), and the node, from `process` and the slowest lease renewal since the last
+    /// sample. A chunk of a scope this sample doesn't reach (a lease gone) or of another hour or
+    /// term ends.
+    pub fn sample(
+        &mut self,
+        now: DateTime<Utc>,
+        statuses: &BTreeMap<String, ControllerStatus>,
+        process: &ProcessFigures,
+        slowest_renewal: Option<Duration>,
+        tables: bool,
+    ) {
+        let hour = hour_of(now);
+        let t = u16::try_from((now - hour).num_seconds()).unwrap_or(u16::MAX);
+        let mut current = Vec::new();
+        for (id, s) in statuses {
+            let (Some(epoch), Some(actual)) = (s.owner_epoch, s.actual.as_ref()) else {
+                continue;
+            };
+            if matches!(actual, Actual::Standby { .. }) {
+                continue;
+            }
+            let writer = format!("e{epoch}");
+            let scope = ChartScope::Pipeline(id.clone());
+            let known: BTreeSet<String> = self
+                .open
+                .get(&chart_key(&scope, hour, &writer))
+                .map(|o| o.chunk.tables.keys().cloned().collect())
+                .unwrap_or_default();
+            let values = self.pipeline(id, &writer, s, now, tables, &known);
+            current.push(self.add(scope, hour, &writer, t, values));
+        }
+        let r = &process.reading;
+        let node = [
+            ("memory_bytes", r.memory_bytes.map(|b| b as f64)),
+            ("cpu_cores", process.cpu_cores),
+            (
+                "lease_renew_seconds",
+                slowest_renewal.map(|d| d.as_secs_f64()),
+            ),
+        ];
+        let values = (
+            node.into_iter()
+                .filter_map(|(n, v)| v.map(|v| (n, v)))
+                .collect(),
+            Vec::new(),
+        );
+        let writer = format!("i{}", self.incarnation);
+        let scope = ChartScope::Node(self.node.clone());
+        current.push(self.add(scope, hour, &writer, t, values));
+        for (key, open) in &mut self.open {
+            open.ended = !current.contains(key);
+        }
+        self.copied
+            .retain(|id, _| statuses.get(id).is_some_and(|s| s.worker.is_some()));
+    }
+
+    /// A pipeline's values at `now`, from its controller's status.
+    fn pipeline(
+        &mut self,
+        id: &str,
+        writer: &str,
+        s: &ControllerStatus,
+        now: DateTime<Utc>,
+        tables: bool,
+        known: &BTreeSet<String>,
+    ) -> Values {
+        let Some(w) = s.worker.as_ref() else {
+            return (Vec::new(), Vec::new());
+        };
+        let mut own = Vec::new();
+        if let Some(fresh) = s.fresh_as_of(now) {
+            own.push(("lag_seconds", (now - fresh).as_seconds_f64().max(0.0)));
+        }
+        let changes: u64 = w.tables.values().map(|f| f.changes_per_minute(now)).sum();
+        own.push(("changes_per_minute", changes as f64));
+        if let Some(b) = w.retained_bytes {
+            own.push(("wal_held_bytes", b as f64));
+        }
+        if let Some(d) = w.source_behind {
+            own.push(("binlog_lag_seconds", d.as_secs_f64()));
+        }
+        if w.snapshots.is_empty() {
+            self.copied.remove(id);
+        } else {
+            let rows: u64 = w.snapshots.values().map(|p| p.rows_done).sum();
+            if let Some((was_writer, at, was)) = self.copied.get(id)
+                && was_writer == writer
+                && rows >= *was
+                && now > *at
+            {
+                let secs = (now - *at).as_seconds_f64();
+                own.push(("snapshot_rows_per_second", (rows - was) as f64 / secs));
+            }
+            self.copied
+                .insert(id.to_owned(), (writer.to_owned(), now, rows));
+        }
+        let mut each = Vec::new();
+        // Every table the worker tracks or committed to; one the term's chunk hasn't sampled yet
+        // at once, the others every [`TABLE_SAMPLE_EVERY`].
+        let names: BTreeSet<&TableRef> = w.tracked.iter().chain(w.tables.keys()).collect();
+        for table in names {
+            let name = table.to_string();
+            if !tables && known.contains(&name) {
+                continue;
+            }
+            let f = w.tables.get(table);
+            let changes = f.map_or(0, |f| f.changes_per_minute(now));
+            let mut v = vec![("changes_per_minute", changes as f64)];
+            if let Some(n) = f.and_then(|f| f.most_rewritten(now)) {
+                v.push(("files_rewritten", n as f64));
+            }
+            if let Some(d) = f.and_then(|f| f.slowest_commit(now)) {
+                v.push(("commit_seconds", d.as_secs_f64()));
+            }
+            each.push((name, v));
+        }
+        (own, each)
+    }
+
+    /// Adds `values` at `t` to `scope`'s chunk of `hour` by `writer`, and returns its key. No
+    /// values open no chunk.
+    fn add(
+        &mut self,
+        scope: ChartScope,
+        hour: DateTime<Utc>,
+        writer: &str,
+        t: u16,
+        (own, tables): Values,
+    ) -> String {
+        let chunk = ChartChunk::new(scope, hour, writer);
+        let key = chunk.key();
+        if own.is_empty() && tables.iter().all(|(_, v)| v.is_empty()) {
+            return key;
+        }
+        let open = self.open.entry(key.clone()).or_insert(Open {
+            chunk,
+            dirty: false,
+            fresh: false,
+            written_at: None,
+            ended: false,
+        });
+        for (name, v) in own {
+            open.fresh |= !open.chunk.series.contains_key(name);
+            open.chunk.series.entry(name.into()).or_default().push(t, v);
+            open.dirty = true;
+        }
+        for (table, values) in tables {
+            let series = open.chunk.tables.entry(table).or_default();
+            for (name, v) in values {
+                open.fresh |= !series.contains_key(name);
+                series.entry(name.into()).or_default().push(t, v);
+                open.dirty = true;
+            }
+        }
+        key
+    }
+
+    /// Writes the chunks due at `now`: one never written, one with a new series, one that changed
+    /// [`WRITE_EVERY`] or more after its last write, and one that ended. A written chunk that ended is dropped; one
+    /// that can't be written stays for the next call, until [`UNWRITTEN_KEPT`] after its hour.
+    /// Returns how many writes failed.
+    pub async fn write(&mut self, now: DateTime<Utc>) -> usize {
+        let every = TimeDelta::from_std(WRITE_EVERY).expect("a minute");
+        let due: Vec<(String, ChartChunk)> = self
+            .open
+            .iter()
+            .filter(|(_, o)| {
+                o.dirty && (o.ended || o.fresh || o.written_at.is_none_or(|w| now - w >= every))
+            })
+            .map(|(k, o)| (k.clone(), o.chunk.clone()))
+            .collect();
+        let charts = Arc::clone(&self.charts);
+        let results: Vec<(String, bool)> = stream::iter(due)
+            .map(|(key, chunk)| {
+                let charts = Arc::clone(&charts);
+                async move {
+                    match charts.write(&chunk).await {
+                        Ok(()) => (key, true),
+                        Err(e) => {
+                            tracing::warn!(key, "chart history unwritten, tried again later: {e}");
+                            (key, false)
+                        }
+                    }
+                }
+            })
+            .buffer_unordered(WRITE_CONCURRENCY)
+            .collect()
+            .await;
+        let mut failed = 0;
+        for (key, ok) in results {
+            if let Some(o) = self.open.get_mut(&key) {
+                if ok {
+                    o.dirty = false;
+                    o.fresh = false;
+                    o.written_at = Some(now);
+                } else {
+                    failed += 1;
+                }
+            }
+        }
+        let kept = TimeDelta::hours(1) + TimeDelta::from_std(UNWRITTEN_KEPT).expect("an hour");
+        self.open.retain(|key, o| {
+            if !o.ended {
+                true
+            } else if !o.dirty {
+                false
+            } else if now - o.chunk.hour > kept {
+                tracing::warn!(
+                    key,
+                    "chart history dropped unwritten: the store kept failing"
+                );
+                false
+            } else {
+                true
+            }
+        });
+        failed
+    }
+
+    /// The chunks held in memory, written or not.
+    pub fn held(&self) -> usize {
+        self.open.len()
+    }
+}
+
+/// Samples every [`SAMPLE_EVERY`] (the tables every [`TABLE_SAMPLE_EVERY`]) the controllers
+/// `statuses` lists and the process's `figures`, and writes what is due, until the task is
+/// aborted.
+pub async fn sample_history(
+    mut sampler: Sampler,
+    statuses: watch::Receiver<BTreeMap<String, watch::Receiver<ControllerStatus>>>,
+    process: watch::Receiver<ProcessFigures>,
+    metrics: Metrics,
+) {
+    let per_table = (TABLE_SAMPLE_EVERY.as_secs() / SAMPLE_EVERY.as_secs()).max(1);
+    let mut every = tokio::time::interval(SAMPLE_EVERY);
+    every.set_missed_tick_behavior(MissedTickBehavior::Delay);
+    let mut ticks: u64 = 0;
+    loop {
+        every.tick().await;
+        let now = Utc::now();
+        let current: BTreeMap<String, ControllerStatus> = statuses
+            .borrow()
+            .iter()
+            .map(|(id, s)| (id.clone(), s.borrow().clone()))
+            .collect();
+        let figures = *process.borrow();
+        sampler.sample(
+            now,
+            &current,
+            &figures,
+            metrics.take_slowest_renewal(),
+            ticks.is_multiple_of(per_table),
+        );
+        sampler.write(now).await;
+        ticks += 1;
+    }
+}
+
+/// Deletes the days past the retention `retention` holds now, every [`HISTORY_PRUNE_EVERY`] (the
+/// first a minute after the start), until the task is aborted. Every node runs it.
+pub async fn expire_history(charts: Arc<Charts>, retention: watch::Receiver<Duration>) {
+    tokio::time::sleep(Duration::from_secs(60)).await;
+    loop {
+        let keep = *retention.borrow();
+        match charts.prune(keep, Utc::now()).await {
+            Ok(0) => {}
+            Ok(n) => tracing::info!(deleted = n, "expired chart history deleted"),
+            Err(e) => tracing::warn!("expired chart history undeleted, tried again later: {e}"),
+        }
+        tokio::time::sleep(HISTORY_PRUNE_EVERY).await;
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use chrono::TimeZone;
````

`crates/data-avalanche-cluster/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 35cdf1d..4e6109b 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -45,6 +45,9 @@ pub use gossip::{
     Advertiser, Announcer, Gossip, GossipFigures, GossipSettings, Member, Members, NodeIdentity,
     NodeMeta, Phase, SUSPICIONS_KEPT, SUSPICIONS_LISTED,
 };
+pub use history::{
+    HISTORY_PRUNE_EVERY, Sampler, UNWRITTEN_KEPT, WRITE_EVERY, expire_history, sample_history,
+};
 pub use lease::{LeaseGuard, LeaseRun, LeaseTiming, RenewEnd, RenewState};
 pub use node::{JOIN_WAIT, join, leave};
 pub use placement::Placement;
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 72cead5..a5fd1ef 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -13,15 +13,15 @@ use data_avalanche_api::{
 use data_avalanche_cluster::{
     ClusterReconciler, ConfigCoordinator, ControllerDeps, ControllerSettings, CoordinatorDeps,
     DocsFn, Gossip, GossipKey, GossipSettings, LeaseTiming, NodeIdentity, NodeMeta, Phase,
-    ReconcilerSettings, Recorder, Seeds, Udp, advertise, api_url, discover, expire_events,
-    file_hash, interfaces, join as join_gossip, leave, node_identity, record_members,
-    removed_pipelines, watch_file,
+    ReconcilerSettings, Recorder, Sampler, Seeds, Udp, advertise, api_url, discover, expire_events,
+    expire_history, file_hash, interfaces, join as join_gossip, leave, node_identity,
+    record_members, removed_pipelines, sample_history, watch_file,
 };
 use data_avalanche_config::{
     Diagnostic, FingerprintKey, ProcessEnv, Report, ResolvedConfig, Severity, load_str,
 };
 use data_avalanche_pipeline::{Backoff, Tuning};
-use data_avalanche_state::{ConfigGenerations, Events, Leases, Removals};
+use data_avalanche_state::{Charts, ConfigGenerations, Events, Leases, Removals};
 use data_avalanche_telemetry::{Metrics, ProcessFigures, sample_process};
 use thiserror::Error;
 use tokio::sync::{Semaphore, oneshot, watch};
@@ -181,6 +181,12 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         Recorder::start(Arc::clone(&events), &holder.node_id, metrics.clone());
     let (retention, kept) = watch::channel(config.globals.console.events.retention);
     let _expiry = tokio::spawn(expire_events(Arc::clone(&events), kept));
+    // The chart history (Admin_Console §7): sampled once the controllers run, expired on every
+    // node.
+    let charts = Arc::new(Charts::new(joined.globals.clone()));
+    let (history_retention, history_kept) =
+        watch::channel(config.globals.console.history.retention);
+    let _history_expiry = tokio::spawn(expire_history(Arc::clone(&charts), history_kept));
     let c = &config.globals.cluster;
     let docs = Arc::clone(&buckets);
     let docs: Arc<DocsFn> = Arc::new(move |p| docs.pipeline_docs(p).map_err(|e| e.to_string()));
@@ -262,6 +268,16 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         gossip.members(),
         ReconcilerSettings::new(c),
     );
+    let _history = tokio::spawn(sample_history(
+        Sampler::new(
+            Arc::clone(&charts),
+            &deps.holder.node_id,
+            deps.holder.incarnation,
+        ),
+        reconciler.watch_statuses(),
+        process.clone(),
+        metrics.clone(),
+    ));
     // Each newer config generation, this node's reloads included (Clustering §9).
     let (log, reloaded_auth) = (Arc::clone(&logger), Arc::clone(&auth));
     let coordinator = Arc::new(ConfigCoordinator::start(
@@ -283,6 +299,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
                 log.trace(&g.tracing);
                 reloaded_auth.set(&g.api, &g.console.username_claim);
                 retention.send_replace(g.console.events.retention);
+                history_retention.send_replace(g.console.history.retention);
             }),
             recorder: recorder.clone(),
         },
````

Then regenerate `Cargo.lock`: `cargo build` updates `Cargo.lock` (the cluster crate's `futures-util`).

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-cluster --lib history::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock crates/data-avalanche-api/src/live.rs crates/data-avalanche-cluster/Cargo.toml crates/data-avalanche-cluster/src/controller.rs crates/data-avalanche-cluster/src/history.rs crates/data-avalanche-cluster/src/lib.rs crates/data-avalanche/src/run.rs
git commit -m "feat(cluster): the history sampler — each node samples what it owns into globals.state every 15 s; whole days expired"
```

---

### Task 4: `GET /v1/history` (`data-avalanche-api`, `data-avalanche`)

**Files:**
- Regenerate: `crates/data-avalanche-api/openapi.json`
- Modify: `crates/data-avalanche-api/src/events.rs`
- Create: `crates/data-avalanche-api/src/history.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Modify: `crates/data-avalanche-api/src/openapi.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche/src/run.rs`
- Regenerate: `docs/public/openapi.json`
- Regenerate: `docs/src/content/docs/reference/api.md`
- Modify: `xtask/src/api.rs`

**Interfaces:**
- Consumes: Task 1's `Charts::read`, `SERIES`, `series_of`, `Fold`; `named_table`, `READ_AT_ONCE` (M13c), `events::time`
  (now `pub(crate)`).
- Produces: `Node::charts: Arc<Charts>`; the route `GET /v1/history` (viewer) with `history::PARAMETERS`;
  `MAX_POINTS` (1440) and `MAX_SPAN` (31 days) at the crate root. The answer: `{since, until, step, kept_since,
  series: [{pipeline | node, table?, name, unit, points: [[unix seconds, value]]}]}`.
- The site's API reference gains a `history` section (`xtask/src/api.rs`), generated with the route so `cargo xtask docs --check` holds at every commit.

- [ ] **Step 1: Write the failing tests**

The tests: `a_query_names_what_it_asks_and_its_step`, `a_query_that_isn_t_one_is_refused`, `samples_fold_into_points_at_the_step`, `the_history_answers_from_storage_through_any_node`, `nothing_older_than_the_retention_is_shown` (history); the route table.

`crates/data-avalanche-api/src/history.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/history.rs b/crates/data-avalanche-api/src/history.rs
new file mode 100644
index 0000000..2bc1182
--- /dev/null
+++ b/crates/data-avalanche-api/src/history.rs
@@ -0,0 +1,248 @@
+//! `GET /v1/history` (Admin_Console §7, D-M13e.5): the series the nodes sampled, read by any node
+//! from the chart history in `globals.state`, over a range and at a step. A pipeline's series
+//! follow it across its lease terms, a node's across its processes.
+
+#[cfg(test)]
+mod tests {
+    use chrono::{DurationRound, TimeZone};
+    use data_avalanche_state::{Charts, DocStore, Points};
+    use data_avalanche_storage::ObjectStoreVersioned;
+    use object_store::memory::InMemory;
+
+    use super::*;
+    use crate::test_support::{config, json_of, open, served, served_with};
+
+    fn params(q: &str) -> Vec<(String, String)> {
+        url::form_urlencoded::parse(q.as_bytes())
+            .into_owned()
+            .collect()
+    }
+
+    fn at(h: u32, m: u32, s: u32) -> DateTime<Utc> {
+        Utc.with_ymd_and_hms(2026, 11, 18, h, m, s).unwrap()
+    }
+
+    #[test]
+    fn a_query_names_what_it_asks_and_its_step() {
+        let c = config();
+        let now = at(12, 0, 0);
+        let q = query(&params(""), &c, now).unwrap();
+        assert!(matches!(&q.asked, Asked::Pipelines(p) if p.len() == 1));
+        assert_eq!(q.series.len(), 5);
+        assert_eq!((q.since, q.until, q.step), (at(11, 0, 0), now, 15));
+        let q = query(
+            &params("pipeline=orders&table=public.orders&since=2026-11-17T12:00:00Z"),
+            &c,
+            now,
+        )
+        .unwrap();
+        assert!(
+            matches!(&q.asked, Asked::Table(_, key, kept) if key == "public.orders" && kept == "public.orders")
+        );
+        // A day at a minute a sample: 1440 points.
+        assert_eq!(q.step, 60);
+        let q = query(
+            &params("node=node-b&series=cpu_cores,memory_bytes&since=2026-11-11T12:00:00Z"),
+            &c,
+            now,
+        )
+        .unwrap();
+        // A week: 420 s, a multiple of 15 s, keeps 1440 points.
+        assert_eq!((q.series.len(), q.step), (2, 420));
+        let q = query(&params("node=n&step=1"), &c, now).unwrap();
+        assert_eq!(q.step, 15, "never below the samples' own");
+        let q = query(&params("until=2026-11-19T00:00:00Z"), &c, now).unwrap();
+        assert_eq!(q.until, now, "never past now");
+    }
+
+    #[test]
+    fn a_query_that_isn_t_one_is_refused() {
+        let c = config();
+        let now = at(12, 0, 0);
+        for (q, code, says) in [
+            ("node=a&pipeline=orders", 400, "node is asked alone"),
+            ("table=public.orders", 400, "one pipeline"),
+            (
+                "pipeline=orders,orders&table=public.orders",
+                400,
+                "one pipeline",
+            ),
+            ("pipeline=orders&table=orders", 400, "schema.table"),
+            ("pipeline=nope", 404, "no pipeline \"nope\""),
+            (
+                "series=memory_bytes",
+                400,
+                "no series \"memory_bytes\" of a pipeline",
+            ),
+            ("since=2026-11-18T12:00:00Z", 400, "since is before until"),
+            ("since=2026-10-01T00:00:00Z", 400, "at most 31 days"),
+            ("since=yesterday", 400, "RFC 3339"),
+            ("step=0", 400, "step"),
+            ("limit=3", 400, "unknown parameter"),
+            ("node=a&node=b", 400, "given twice"),
+        ] {
+            let f = query(&params(q), &c, now).unwrap_err();
+            assert_eq!(f.code.as_u16(), code, "{q}");
+            assert!(f.error.contains(says), "{q}: {}", f.error);
+        }
+    }
+
+    fn chunk(scope: ChartScope, hour: DateTime<Utc>, writer: &str) -> ChartChunk {
+        ChartChunk::new(scope, hour, writer)
+    }
+
+    fn pts(list: &[(u16, f64)]) -> Points {
+        let mut p = Points::default();
+        for (t, v) in list {
+            p.push(*t, *v);
+        }
+        p
+    }
+
+    /// Each term's samples meet in one series; a coarser step folds a gauge to its peak and a
+    /// rate to its mean.
+    #[test]
+    fn samples_fold_into_points_at_the_step() {
+        let p = ChartScope::Pipeline("orders".into());
+        let mut a = chunk(p.clone(), at(9, 0, 0), "e4");
+        a.series
+            .insert("lag_seconds".into(), pts(&[(0, 2.0), (15, 9.0), (30, 3.0)]));
+        a.series
+            .insert("changes_per_minute".into(), pts(&[(0, 10.0), (15, 20.0)]));
+        let mut b = chunk(p, at(9, 0, 0), "e5");
+        b.series
+            .insert("lag_seconds".into(), pts(&[(45, 1.0), (60, 7.0)]));
+        let chunks = [Arc::new(a), Arc::new(b)];
+        let lag = series_of(SeriesScope::Pipeline, "lag_seconds").unwrap();
+        let base = at(9, 0, 0).timestamp();
+        assert_eq!(
+            points(&chunks, None, lag, at(9, 0, 0), at(10, 0, 0), 15),
+            [
+                (base, 2.0),
+                (base + 15, 9.0),
+                (base + 30, 3.0),
+                (base + 45, 1.0),
+                (base + 60, 7.0)
+            ]
+        );
+        assert_eq!(
+            points(&chunks, None, lag, at(9, 0, 0), at(10, 0, 0), 60),
+            [(base, 9.0), (base + 60, 7.0)]
+        );
+        // Within [from, until): the first sample before it, the last at its end.
+        assert_eq!(
+            points(&chunks, None, lag, at(9, 0, 15), at(9, 1, 0), 15),
+            [(base + 15, 9.0), (base + 30, 3.0), (base + 45, 1.0)]
+        );
+        let rate = series_of(SeriesScope::Pipeline, "changes_per_minute").unwrap();
+        assert_eq!(
+            points(&chunks, None, rate, at(9, 0, 0), at(10, 0, 0), 60),
+            [(base, 15.0)]
+        );
+    }
+
+    async fn written(charts: &Charts, chunks: Vec<ChartChunk>) {
+        for c in chunks {
+            charts.write(&c).await.unwrap();
+        }
+    }
+
+    #[tokio::test]
+    async fn the_history_answers_from_storage_through_any_node() {
+        let charts = Arc::new(Charts::new(DocStore::new(Arc::new(
+            ObjectStoreVersioned::new(Arc::new(InMemory::new()), "g").unwrap(),
+        ))));
+        let now = Utc::now();
+        let hour = now.duration_trunc(TimeDelta::hours(1)).unwrap();
+        let offset = |t: DateTime<Utc>| u16::try_from((t - hour).num_seconds()).unwrap();
+        // Two samples within the last minute of the current hour, or of the hour before.
+        let (t1, t2) = (now - TimeDelta::seconds(40), now - TimeDelta::seconds(25));
+        let hour = if t1 < hour {
+            hour - TimeDelta::hours(1)
+        } else {
+            hour
+        };
+        let mut p = chunk(ChartScope::Pipeline("orders".into()), hour, "e2");
+        p.series.insert(
+            "lag_seconds".into(),
+            pts(&[(offset(t1), 4.0), (offset(t2), 6.0)]),
+        );
+        p.tables.insert(
+            "public.orders".into(),
+            [("commit_seconds".to_owned(), pts(&[(offset(t2), 0.5)]))].into(),
+        );
+        let mut n = chunk(ChartScope::Node("node-b".into()), hour, "i3");
+        n.series
+            .insert("cpu_cores".into(), pts(&[(offset(t1), 0.25)]));
+        written(&charts, vec![p, n]).await;
+        let (addr, _lone, _dir) =
+            served_with("http://127.0.0.1:1", open(), |node| node.charts = charts).await;
+        let (code, v) = json_of(
+            addr,
+            "GET",
+            "/v1/history?pipeline=orders&series=lag_seconds",
+        )
+        .await;
+        assert_eq!(code, 200, "{v}");
+        let s = &v["series"][0];
+        assert_eq!(
+            (&s["pipeline"], &s["name"], &s["unit"]),
+            (&json!("orders"), &json!("lag_seconds"), &json!("seconds"))
+        );
+        let values: Vec<f64> = s["points"]
+            .as_array()
+            .unwrap()
+            .iter()
+            .map(|p| p[1].as_f64().unwrap())
+            .collect();
+        assert_eq!(values, [4.0, 6.0]);
+        assert_eq!(v["step"], 15);
+        let (_, v) = json_of(
+            addr,
+            "GET",
+            "/v1/history?pipeline=orders&table=public.orders&series=commit_seconds",
+        )
+        .await;
+        assert_eq!(v["series"][0]["table"], "public.orders");
+        assert_eq!(v["series"][0]["points"][0][1], 0.5);
+        // Another node's history: its process wrote it, any node reads it.
+        let (_, v) = json_of(addr, "GET", "/v1/history?node=node-b").await;
+        let names: Vec<&str> = v["series"]
+            .as_array()
+            .unwrap()
+            .iter()
+            .map(|s| s["name"].as_str().unwrap())
+            .collect();
+        assert_eq!(names, ["memory_bytes", "cpu_cores", "lease_renew_seconds"]);
+        assert_eq!(v["series"][1]["points"][0][1], 0.25);
+        assert_eq!(v["series"][0]["points"], json!([]));
+    }
+
+    /// Past the retention nothing is shown, though storage still holds the day: the history's
+    /// start is `kept_since`, and the range before it is Prometheus's.
+    #[tokio::test]
+    async fn nothing_older_than_the_retention_is_shown() {
+        let (addr, lone, _dir) = served("http://127.0.0.1:1", open()).await;
+        let now = Utc::now();
+        let old = now - TimeDelta::hours(30);
+        let hour = old.duration_trunc(TimeDelta::hours(1)).unwrap();
+        let mut p = chunk(ChartScope::Pipeline("orders".into()), hour, "e1");
+        p.series.insert("lag_seconds".into(), pts(&[(0, 1.0)]));
+        written(&lone.node.charts, vec![p]).await;
+        let since = (now - TimeDelta::hours(31)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
+        let (code, v) = json_of(
+            addr,
+            "GET",
+            &format!("/v1/history?pipeline=orders&series=lag_seconds&since={since}"),
+        )
+        .await;
+        assert_eq!(code, 200, "{v}");
+        assert_eq!(v["series"][0]["points"], json!([]));
+        let kept = DateTime::parse_from_rfc3339(v["kept_since"].as_str().unwrap()).unwrap();
+        let expected = now - TimeDelta::hours(24);
+        assert!(
+            (kept.with_timezone(&Utc) - expected).num_seconds().abs() < 5,
+            "{v}"
+        );
+    }
+}
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 26277e7..cf2b509 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -14,6 +14,7 @@ mod cluster;
 mod config;
 mod events;
 mod generations;
+mod history;
 mod limit;
 mod live;
 mod nodes;
@@ -983,6 +984,7 @@ mod tests {
                 ("POST", "/v1/nodes/{node}/config/reload", Operator),
                 ("GET", "/v1/nodes/{node}/config/status", Viewer),
                 ("GET", "/v1/events", Viewer),
+                ("GET", "/v1/history", Viewer),
                 ("GET", "/v1/openapi.json", Viewer),
             ]
         );
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib -- history:: tests::the_routes`
Expected: the build fails: no `query`, `points` or `Asked` in `history`, `events::time` is private, and `Node` has no field `charts`.

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/events.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/events.rs b/crates/data-avalanche-api/src/events.rs
index a74df70..6cb5db1 100644
--- a/crates/data-avalanche-api/src/events.rs
+++ b/crates/data-avalanche-api/src/events.rs
@@ -32,7 +32,7 @@ pub const PARAMETERS: &[(&str, &str)] = &[
 /// The default page.
 pub(crate) const DEFAULT_LIMIT: usize = 100;
 
-fn time(name: &str, value: &str) -> Result<DateTime<Utc>, Failure> {
+pub(crate) fn time(name: &str, value: &str) -> Result<DateTime<Utc>, Failure> {
     DateTime::parse_from_rfc3339(value)
         .map(|t| t.with_timezone(&Utc))
         .map_err(|e| {
````

`crates/data-avalanche-api/src/history.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/history.rs b/crates/data-avalanche-api/src/history.rs
index 2bc1182..d0d2716 100644
--- a/crates/data-avalanche-api/src/history.rs
+++ b/crates/data-avalanche-api/src/history.rs
@@ -2,6 +2,390 @@
 //! from the chart history in `globals.state`, over a range and at a step. A pipeline's series
 //! follow it across its lease terms, a node's across its processes.
 
+use axum::extract::{RawQuery, State};
+use axum::http::StatusCode;
+use axum::response::Response;
+use chrono::{DateTime, TimeDelta, Utc};
+use data_avalanche_config::{Pipeline, ResolvedConfig};
+use data_avalanche_state::{
+    ChartChunk, ChartScope, Fold, SERIES, Series, SeriesScope, StateError, series_of,
+};
+use futures_util::{StreamExt, TryStreamExt, stream};
+use serde_json::{Value, json};
+use std::sync::Arc;
+
+use crate::events::time;
+use crate::snapshots::{READ_AT_ONCE, named_table};
+use crate::{Api, Failure, Node, body, starting};
+
+/// The query's parameters, as a page and `curl` write them.
+pub const PARAMETERS: &[(&str, &str)] = &[
+    (
+        "pipeline",
+        "a pipeline's id, or several comma-separated; without it or `node`, every pipeline",
+    ),
+    (
+        "table",
+        "a table of the one pipeline asked, as its config key: the table's series",
+    ),
+    ("node", "a node's name: the node's series"),
+    (
+        "series",
+        "the series, comma-separated; by default every series of what is asked",
+    ),
+    (
+        "since",
+        "from this time (RFC 3339); an hour before `until` by default",
+    ),
+    ("until", "to this time (RFC 3339); now by default"),
+    (
+        "step",
+        "seconds between points; by default the samples' own, coarsened to at most 1440 points",
+    ),
+];
+
+/// The most points a series has by default.
+pub const MAX_POINTS: i64 = 1440;
+
+/// The longest range a query asks.
+pub const MAX_SPAN: TimeDelta = TimeDelta::days(31);
+
+/// What a query asks the series of.
+#[derive(Debug)]
+enum Asked {
+    Pipelines(Vec<Pipeline>),
+    /// A pipeline's table: as asked (its config key), and as the history keeps it.
+    Table(Box<Pipeline>, String, String),
+    Node(String),
+}
+
+#[derive(Debug)]
+struct Query {
+    asked: Asked,
+    series: Vec<&'static Series>,
+    since: DateTime<Utc>,
+    until: DateTime<Utc>,
+    /// Seconds between points.
+    step: i64,
+}
+
+fn bad(error: impl Into<String>) -> Failure {
+    Failure::new(StatusCode::BAD_REQUEST, error)
+}
+
+/// The query of `params`, each once, against `config` at `now`.
+fn query(
+    params: &[(String, String)],
+    config: &ResolvedConfig,
+    now: DateTime<Utc>,
+) -> Result<Query, Failure> {
+    let mut given = std::collections::BTreeMap::new();
+    for (name, value) in params {
+        if !PARAMETERS.iter().any(|(n, _)| n == name) {
+            return Err(bad(format!(
+                "unknown parameter {name:?}: the history takes {}",
+                PARAMETERS
+                    .iter()
+                    .map(|(n, _)| *n)
+                    .collect::<Vec<_>>()
+                    .join(", ")
+            )));
+        }
+        if given.insert(name.as_str(), value.trim()).is_some() {
+            return Err(bad(format!("{name} is given twice")));
+        }
+    }
+    let list = |v: &str| -> Vec<String> {
+        v.split(',')
+            .map(str::trim)
+            .filter(|s| !s.is_empty())
+            .map(str::to_owned)
+            .collect()
+    };
+    let pipeline = |id: &str| {
+        config
+            .pipelines
+            .values()
+            .find(|p| p.id == id)
+            .cloned()
+            .ok_or_else(|| {
+                Failure::new(
+                    StatusCode::NOT_FOUND,
+                    format!("no pipeline {id:?} in the config generation this node runs"),
+                )
+            })
+    };
+    let asked = match (
+        given.get("pipeline").copied(),
+        given.get("table").copied(),
+        given.get("node").copied(),
+    ) {
+        (_, _, Some(_)) if given.contains_key("pipeline") || given.contains_key("table") => {
+            return Err(bad("node is asked alone, without pipeline or table"));
+        }
+        (_, _, Some("")) => return Err(bad("node is a node's name")),
+        (_, _, Some(node)) => Asked::Node(node.to_owned()),
+        (Some(ids), Some(table), None) => {
+            let ids = list(ids);
+            let [id] = ids.as_slice() else {
+                return Err(bad("table is asked of one pipeline"));
+            };
+            let p = pipeline(id)?;
+            let t = named_table(&p, table).map_err(bad)?;
+            Asked::Table(Box::new(p), table.to_owned(), t.to_string())
+        }
+        (None, Some(_), None) => return Err(bad("table is asked of one pipeline")),
+        (Some(ids), None, None) => {
+            let ids = list(ids);
+            if ids.is_empty() {
+                return Err(bad("pipeline names a pipeline's id"));
+            }
+            Asked::Pipelines(
+                ids.iter()
+                    .map(|id| pipeline(id))
+                    .collect::<Result<_, _>>()?,
+            )
+        }
+        (None, None, None) => {
+            let mut all: Vec<Pipeline> = config.pipelines.values().cloned().collect();
+            all.sort_by(|a, b| a.id.cmp(&b.id));
+            Asked::Pipelines(all)
+        }
+    };
+    let scope = match asked {
+        Asked::Pipelines(_) => SeriesScope::Pipeline,
+        Asked::Table(..) => SeriesScope::Table,
+        Asked::Node(_) => SeriesScope::Node,
+    };
+    let series: Vec<&'static Series> = match given.get("series") {
+        None => SERIES.iter().filter(|s| s.scope == scope).collect(),
+        Some(names) => {
+            let names = list(names);
+            if names.is_empty() {
+                return Err(bad("series names a series"));
+            }
+            let mut out = Vec::new();
+            for name in names {
+                let Some(s) = series_of(scope, &name) else {
+                    return Err(bad(format!(
+                        "no series {name:?} of a {}: {}",
+                        match scope {
+                            SeriesScope::Pipeline => "pipeline",
+                            SeriesScope::Table => "table",
+                            SeriesScope::Node => "node",
+                        },
+                        SERIES
+                            .iter()
+                            .filter(|s| s.scope == scope)
+                            .map(|s| s.name)
+                            .collect::<Vec<_>>()
+                            .join(", ")
+                    )));
+                };
+                if !out.contains(&s) {
+                    out.push(s);
+                }
+            }
+            out
+        }
+    };
+    let until = match given.get("until") {
+        Some(v) => time("until", v)?.min(now),
+        None => now,
+    };
+    let since = match given.get("since") {
+        Some(v) => time("since", v)?,
+        None => until - TimeDelta::hours(1),
+    };
+    if since >= until {
+        return Err(bad("since is before until, and until isn't past now"));
+    }
+    if until - since > MAX_SPAN {
+        return Err(bad(format!(
+            "the range is at most {} days",
+            MAX_SPAN.num_days()
+        )));
+    }
+    let span = (until - since).num_seconds().max(1);
+    let native = series
+        .iter()
+        .map(|s| i64::try_from(s.every.as_secs()).unwrap_or(i64::MAX))
+        .max()
+        .unwrap_or(1);
+    let coarse = (span + MAX_POINTS - 1) / MAX_POINTS;
+    let step = match given.get("step") {
+        None => native.max((coarse + native - 1) / native * native),
+        Some(v) => {
+            let asked: i64 = v
+                .parse()
+                .ok()
+                .filter(|n| (1..=span).contains(n))
+                .ok_or_else(|| bad("step is a number of seconds within the range"))?;
+            asked.max(native)
+        }
+    };
+    Ok(Query {
+        asked,
+        series,
+        since,
+        until,
+        step,
+    })
+}
+
+/// The samples of series `name` in `chunks` (of a table when `table` names it) within
+/// `[from, until)`, folded into points at multiples of `step`, oldest first.
+fn points(
+    chunks: &[Arc<ChartChunk>],
+    table: Option<&str>,
+    s: &Series,
+    from: DateTime<Utc>,
+    until: DateTime<Utc>,
+    step: i64,
+) -> Vec<(i64, f64)> {
+    let (from, until) = (from.timestamp(), until.timestamp());
+    let mut samples: Vec<(i64, f64)> = chunks
+        .iter()
+        .filter_map(|c| {
+            let series = match table {
+                None => c.series.get(s.name),
+                Some(t) => c.tables.get(t).and_then(|t| t.get(s.name)),
+            };
+            series.map(|p| (c.hour.timestamp(), p))
+        })
+        .flat_map(|(hour, p)| p.iter().map(move |(t, v)| (hour + i64::from(t), v)))
+        .filter(|(t, v)| (from..until).contains(t) && v.is_finite())
+        .collect();
+    samples.sort_by_key(|(t, _)| *t);
+    let mut out: Vec<(i64, f64, usize)> = Vec::new();
+    for (t, v) in samples {
+        let at = t.div_euclid(step) * step;
+        match out.last_mut() {
+            Some((last, folded, n)) if *last == at => {
+                *folded = match s.fold {
+                    Fold::Max => folded.max(v),
+                    Fold::Mean => *folded + v,
+                };
+                *n += 1;
+            }
+            _ => out.push((at, v, 1)),
+        }
+    }
+    out.into_iter()
+        .map(|(at, v, n)| match s.fold {
+            Fold::Max => (at, v),
+            Fold::Mean => (at, v / n as f64),
+        })
+        .collect()
+}
+
+/// One series of the answer.
+fn series_json(who: Value, s: &Series, points: &[(i64, f64)]) -> Value {
+    let mut v = who;
+    v["name"] = json!(s.name);
+    v["unit"] = json!(s.unit);
+    v["points"] = json!(
+        points
+            .iter()
+            .map(|(t, v)| json!([t, v]))
+            .collect::<Vec<_>>()
+    );
+    v
+}
+
+/// Reads `scope`'s chunks over `[from, until)`; none when the range is empty.
+async fn chunks(
+    node: &Node,
+    scope: ChartScope,
+    from: DateTime<Utc>,
+    until: DateTime<Utc>,
+    now: DateTime<Utc>,
+) -> Result<Vec<Arc<ChartChunk>>, StateError> {
+    if from >= until {
+        return Ok(Vec::new());
+    }
+    node.charts.read(&scope, from, until, now).await
+}
+
+/// The answer to `q` at `now`: each asked thing's series, kept since `kept_since`.
+async fn answer(
+    node: &Node,
+    q: &Query,
+    kept_since: DateTime<Utc>,
+    now: DateTime<Utc>,
+) -> Result<Vec<Value>, StateError> {
+    let from = q.since.max(kept_since);
+    let fold = |who: Value, chunks: &[Arc<ChartChunk>], table: Option<&str>| {
+        q.series
+            .iter()
+            .map(|s| {
+                let p = points(chunks, table, s, from, q.until, q.step);
+                series_json(who.clone(), s, &p)
+            })
+            .collect::<Vec<_>>()
+    };
+    Ok(match &q.asked {
+        Asked::Node(name) => {
+            let c = chunks(node, ChartScope::Node(name.clone()), from, q.until, now).await?;
+            fold(json!({ "node": name }), &c, None)
+        }
+        Asked::Table(p, key, kept) => {
+            let c = chunks(node, ChartScope::Pipeline(p.id.clone()), from, q.until, now).await?;
+            fold(json!({ "pipeline": p.id, "table": key }), &c, Some(kept))
+        }
+        Asked::Pipelines(ps) => {
+            let scopes: Vec<ChartScope> = ps
+                .iter()
+                .map(|p| ChartScope::Pipeline(p.id.clone()))
+                .collect();
+            let until = q.until;
+            let read: Vec<Vec<Arc<ChartChunk>>> = stream::iter(scopes)
+                .map(|scope| chunks(node, scope, from, until, now))
+                .buffered(READ_AT_ONCE)
+                .try_collect()
+                .await?;
+            ps.iter()
+                .zip(read)
+                .flat_map(|(p, c)| fold(json!({ "pipeline": p.id }), &c, None))
+                .collect()
+        }
+    })
+}
+
+pub(crate) async fn read(State(api): State<Api>, RawQuery(raw): RawQuery) -> Response {
+    let Some(node) = api.node.get() else {
+        return starting();
+    };
+    let params: Vec<(String, String)> =
+        url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes())
+            .into_owned()
+            .collect();
+    let config = node.coordinator.config().await;
+    let now = Utc::now();
+    let q = match query(&params, &config, now) {
+        Ok(q) => q,
+        Err(f) => return f.into(),
+    };
+    let retention = config.globals.console.history.retention;
+    let kept_since = now - TimeDelta::from_std(retention).unwrap_or(MAX_SPAN);
+    match answer(node, &q, kept_since, now).await {
+        Ok(series) => body(
+            StatusCode::OK,
+            &json!({
+                "since": q.since,
+                "until": q.until,
+                "step": q.step,
+                "kept_since": kept_since,
+                "series": series,
+            }),
+        ),
+        Err(e) => body(
+            StatusCode::SERVICE_UNAVAILABLE,
+            &json!({ "error": format!("the chart history: {e}") }),
+        ),
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use chrono::{DurationRound, TimeZone};
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index cf2b509..2615726 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -42,7 +42,7 @@ use data_avalanche_cluster::{
     ConfigCoordinator, ControllerStatus, DocsFn, GossipFigures, Members, Recorder, WorkerFactory,
 };
 use data_avalanche_config::Role;
-use data_avalanche_state::{Events, Leases};
+use data_avalanche_state::{Charts, Events, Leases};
 use data_avalanche_telemetry::{CONTENT_TYPE, Metrics, ProcessFigures};
 use serde_json::{Value, json};
 use tokio::sync::watch;
@@ -50,6 +50,7 @@ use tokio::sync::watch;
 pub use actions::{DENIALS_BURST, MAX_ERROR, MAX_NOTE, MAX_TARGET, Recorded, note_of};
 pub use auth::{Auth, FETCH_TIMEOUT, KEYS_MAX_AGE, MAX_KEY_DOCUMENT, Principal, REFETCH_AFTER};
 pub use cluster::{Control, Stop};
+pub use history::{MAX_POINTS, MAX_SPAN};
 pub use limit::{RateLimit, VALIDATE_BURST, VALIDATE_PER_SECOND};
 pub use live::FRESH_WITHIN;
 pub use openapi::openapi;
@@ -110,6 +111,8 @@ pub struct Node {
     pub factory: Arc<dyn WorkerFactory>,
     /// The tables' facts and columns read, each kept [`FACTS_FOR`].
     pub lake: Arc<Lake>,
+    /// The chart history every node samples into `globals.state` (Admin_Console §7).
+    pub charts: Arc<Charts>,
 }
 
 impl std::fmt::Debug for Node {
@@ -787,6 +790,24 @@ fn v1() -> Vec<(Route, MethodRouter<Api>)> {
             ),
             get(events::list),
         ),
+        (
+            r(
+                "GET",
+                "/v1/history",
+                Viewer,
+                "The series the nodes sampled, over a range: a pipeline's, a table's or a node's, from any node",
+                &[
+                    (
+                        200,
+                        "each series' points, `[unix seconds, value]`, oldest first; `kept_since`: where the history kept starts",
+                    ),
+                    (400, "a parameter that isn't one, or isn't in its form"),
+                    (404, "no such pipeline in the generation this node runs"),
+                    (503, "the chart history can't be read"),
+                ],
+            ),
+            get(history::read),
+        ),
         (
             r(
                 "GET",
````

`crates/data-avalanche-api/src/openapi.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/openapi.rs b/crates/data-avalanche-api/src/openapi.rs
index b326c48..a8c67e7 100644
--- a/crates/data-avalanche-api/src/openapi.rs
+++ b/crates/data-avalanche-api/src/openapi.rs
@@ -117,6 +117,25 @@ fn operation(r: &Route) -> utoipa::openapi::path::Operation {
             );
         }
     }
+    if r.path == "/v1/history" {
+        for (name, description) in crate::history::PARAMETERS {
+            let schema = if *name == "step" {
+                ObjectBuilder::new()
+                    .schema_type(Type::Integer)
+                    .minimum(Some(1))
+            } else {
+                ObjectBuilder::new().schema_type(Type::String)
+            };
+            op = op.parameter(
+                ParameterBuilder::new()
+                    .name(*name)
+                    .parameter_in(ParameterIn::Query)
+                    .required(Required::False)
+                    .description(Some(*description))
+                    .schema(Some(schema)),
+            );
+        }
+    }
     if r.path == "/v1/events" {
         for (name, description) in crate::events::PARAMETERS {
             let schema = if *name == "limit" {
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index 982fe0d..aa587a3 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -315,6 +315,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             statuses: reconciler.watch_statuses(),
             factory: Arc::new(Lakes(sink.clone())),
             lake: Arc::default(),
+            charts: Arc::new(data_avalanche_state::Charts::new(docs.clone())),
         },
         events,
         leases,
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index a5fd1ef..555d7fe 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -328,6 +328,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         // A table's Delta facts, read by any node on request.
         factory: Arc::clone(&deps.factory),
         lake: Arc::default(),
+        charts,
     });
     ready.store(true, Ordering::SeqCst);
     let mut cluster = Some((gossip, reconciler, discovery));
````

`xtask/src/api.rs`:

````diff
diff --git a/xtask/src/api.rs b/xtask/src/api.rs
index 8a5857c..0e5be66 100644
--- a/xtask/src/api.rs
+++ b/xtask/src/api.rs
@@ -32,6 +32,10 @@ const TAGS: &[(&str, &str)] = &[
         "events",
         "The cluster's timeline and audit trail, kept for `globals.console.events.retention`.",
     ),
+    (
+        "history",
+        "The series the nodes sampled for charts, kept for `globals.console.history.retention`; a pipeline's older range from `globals.console.prometheus` when it is set.",
+    ),
     ("openapi.json", "The document this page is rendered from."),
 ];
 
````

Then regenerate `crates/data-avalanche-api/openapi.json`, `docs/public/openapi.json`, `docs/src/content/docs/reference/api.md`: `DATA_AVALANCHE_BLESS=1 cargo test -p data-avalanche-api --lib openapi::` writes the document (check its diff is the new path and its parameters), then `cargo xtask docs` writes the site's reference and its copy of the document.

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib -- history:: tests::the_routes`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/openapi.json crates/data-avalanche-api/src/events.rs crates/data-avalanche-api/src/history.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/openapi.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche/src/run.rs docs/public/openapi.json docs/src/content/docs/reference/api.md xtask/src/api.rs
git commit -m "feat(api): GET /v1/history — the series the nodes sampled, from any node, over a range and at a step"
```

---

### Task 5: A pipeline's older range from Prometheus (`data-avalanche-api`, `data-avalanche`)

**Files:**
- Modify: `crates/data-avalanche-api/src/history.rs`
- Modify: `crates/data-avalanche-api/src/lib.rs`
- Create: `crates/data-avalanche-api/src/prometheus.rs`
- Modify: `crates/data-avalanche-api/src/test_support.rs`
- Modify: `crates/data-avalanche/src/run.rs`

**Interfaces:**
- Consumes: `Prometheus`, `PrometheusAuth` (config, M13a); Task 4's handler.
- Produces (API crate root): `PrometheusClient::new(&Prometheus) -> Result<Self, PrometheusError>`,
  `PrometheusClient::range(&self, query, start, end, step) -> Result<BTreeMap<String, Vec<(i64, f64)>>, PrometheusError>`
  (by the `pipeline` label); `PrometheusReader` (`client`, `record`, `last`), `Outcome { at, error }`;
  `promql(&Series, names, extra_labels, step) -> Option<String>`; `PrometheusError`; `MAX_PROMETHEUS_ANSWER` (8 MiB),
  `MIN_RATE_WINDOW` (300 s). `Node::prometheus: Arc<PrometheusReader>`.
- The answer gains, with `prometheus` set: each series' `older` (`prometheus`, `failing`, `not_in_prometheus`) when the
  range starts before `kept_since`, and `prometheus: {state: unasked | ok | failing, at, error}`.

- [ ] **Step 1: Write the failing tests**

The tests: `each_pipeline_series_has_its_query`, `a_reader_keeps_its_client_while_the_config_stays` (prometheus); `a_pipeline_s_range_past_the_retention_comes_from_prometheus`, `a_failing_prometheus_leaves_the_history_kept` (history, against a fake Prometheus on a loopback port).

`crates/data-avalanche-api/src/history.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/history.rs b/crates/data-avalanche-api/src/history.rs
index d0d2716..2f977b3 100644
--- a/crates/data-avalanche-api/src/history.rs
+++ b/crates/data-avalanche-api/src/history.rs
@@ -394,7 +394,14 @@ mod tests {
     use object_store::memory::InMemory;
 
     use super::*;
-    use crate::test_support::{config, json_of, open, served, served_with};
+    use std::collections::BTreeMap;
+    use std::net::SocketAddr;
+
+    use data_avalanche_config::ProcessEnv;
+
+    use crate::test_support::{
+        Lone, api, config, json_of, lone_with, open, serve, served, served_with,
+    };
 
     fn params(q: &str) -> Vec<(String, String)> {
         url::form_urlencoded::parse(q.as_bytes())
@@ -629,4 +636,166 @@ mod tests {
             "{v}"
         );
     }
+
+    /// Every range query a fake Prometheus was asked: its form, and its `Authorization`.
+    type Queries = Arc<std::sync::Mutex<Vec<(BTreeMap<String, String>, Option<String>)>>>;
+
+    /// A fake Prometheus on a loopback port: it answers each range query `code` and, on 200,
+    /// `orders` at the query's first two steps (7, then 8).
+    async fn fake_prometheus(code: u16) -> (String, Queries) {
+        use axum::body::Bytes;
+        use axum::http::HeaderMap;
+        let asked: Queries = Arc::default();
+        let seen = Arc::clone(&asked);
+        let app = axum::Router::new().route(
+            "/api/v1/query_range",
+            axum::routing::post(move |headers: HeaderMap, body: Bytes| {
+                let seen = Arc::clone(&seen);
+                async move {
+                    let form: BTreeMap<String, String> =
+                        url::form_urlencoded::parse(&body).into_owned().collect();
+                    let auth = headers
+                        .get("authorization")
+                        .map(|v| v.to_str().unwrap().to_owned());
+                    let start: i64 = form["start"].parse().unwrap();
+                    let step: i64 = form["step"].trim_end_matches('s').parse().unwrap();
+                    seen.lock().unwrap().push((form, auth));
+                    let answer = json!({
+                        "status": "success",
+                        "data": { "resultType": "matrix", "result": [{
+                            "metric": { "pipeline": "orders" },
+                            "values": [[start, "7"], [start + step, "8"]],
+                        }]},
+                    });
+                    match code {
+                        200 => (StatusCode::OK, answer.to_string()),
+                        c => (
+                            StatusCode::from_u16(c).unwrap(),
+                            "the TSDB is starting".to_owned(),
+                        ),
+                    }
+                }
+            }),
+        );
+        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
+        let addr = listener.local_addr().unwrap();
+        tokio::spawn(async move { axum::serve(listener, app).await });
+        (format!("http://{addr}"), asked)
+    }
+
+    /// node-a's API running [`crate::test_support::CONFIG`] with `globals.console.prometheus`
+    /// at `url`.
+    async fn served_prometheus(url: &str) -> (SocketAddr, Lone, tempfile::TempDir) {
+        let text = crate::test_support::CONFIG.replace(
+            "pipelines:\n",
+            &format!(
+                "  console:\n    prometheus: {{ url: \"{url}\", bearer_token: tok, extra_labels: {{ cluster: prod-eu }} }}\npipelines:\n"
+            ),
+        );
+        let dir = tempfile::tempdir().unwrap();
+        let file = dir.path().join("config.yaml");
+        std::fs::write(&file, &text).unwrap();
+        let lone = lone_with(file, &text, Arc::new(ProcessEnv)).await;
+        let a = api(open());
+        a.node.set(lone.node.clone()).unwrap();
+        (serve(a).await, lone, dir)
+    }
+
+    /// A lag sample 30 s ago, in the history kept.
+    async fn recent(charts: &Charts) -> i64 {
+        let t = Utc::now() - TimeDelta::seconds(30);
+        let hour = t.duration_trunc(TimeDelta::hours(1)).unwrap();
+        let mut p = chunk(ChartScope::Pipeline("orders".into()), hour, "e1");
+        let offset = u16::try_from((t - hour).num_seconds()).unwrap();
+        p.series.insert("lag_seconds".into(), pts(&[(offset, 2.0)]));
+        written(charts, vec![p]).await;
+        t.timestamp()
+    }
+
+    fn since(hours: i64) -> String {
+        (Utc::now() - TimeDelta::hours(hours)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
+    }
+
+    #[tokio::test]
+    async fn a_pipeline_s_range_past_the_retention_comes_from_prometheus() {
+        let (url, asked) = fake_prometheus(200).await;
+        let (addr, lone, _dir) = served_prometheus(&url).await;
+        let sampled = recent(&lone.node.charts).await;
+        let (code, v) = json_of(
+            addr,
+            "GET",
+            &format!("/v1/history?series=lag_seconds&since={}", since(30)),
+        )
+        .await;
+        assert_eq!(code, 200, "{v}");
+        let step = v["step"].as_i64().unwrap();
+        assert_eq!(step, 75, "30 h at 1440 points, a multiple of 15 s");
+        let s = &v["series"][0];
+        assert_eq!(s["older"], "prometheus");
+        let points = s["points"].as_array().unwrap();
+        let (first, second) = (points[0][0].as_i64().unwrap(), &points[1]);
+        assert_eq!(
+            (&points[0][1], second),
+            (&json!(7.0), &json!([first + step, 8.0]))
+        );
+        // The history kept follows: its sample in its step.
+        let last = points.last().unwrap();
+        assert_eq!(last, &json!([sampled.div_euclid(step) * step, 2.0]));
+        assert_eq!(v["prometheus"]["state"], "ok");
+        let asked = asked.lock().unwrap();
+        let (form, auth) = &asked[0];
+        assert_eq!(auth.as_deref(), Some("Bearer tok"));
+        assert_eq!(
+            form["query"],
+            r#"max by (pipeline) (data_avalanche_replication_lag_seconds{pipeline=~"orders",cluster="prod-eu"})"#
+        );
+        assert_eq!(form["step"], "75s");
+        let kept = DateTime::parse_from_rfc3339(v["kept_since"].as_str().unwrap())
+            .unwrap()
+            .timestamp();
+        assert!(form["end"].parse::<i64>().unwrap() + step <= kept);
+    }
+
+    /// Prometheus failing never fails the answer: the history kept is shown, and the older range
+    /// is marked. A table's or a node's series aren't asked of it.
+    #[tokio::test]
+    async fn a_failing_prometheus_leaves_the_history_kept() {
+        let (url, asked) = fake_prometheus(503).await;
+        let (addr, lone, _dir) = served_prometheus(&url).await;
+        recent(&lone.node.charts).await;
+        let (_, v) = json_of(addr, "GET", "/v1/history?series=lag_seconds").await;
+        assert_eq!(
+            v["prometheus"]["state"], "unasked",
+            "an hour is within the history"
+        );
+        assert!(v["series"][0].get("older").is_none());
+        let (code, v) = json_of(
+            addr,
+            "GET",
+            &format!("/v1/history?series=lag_seconds&since={}", since(30)),
+        )
+        .await;
+        assert_eq!(code, 200, "{v}");
+        assert_eq!(v["series"][0]["older"], "failing");
+        assert_eq!(v["series"][0]["points"].as_array().unwrap().len(), 1);
+        assert_eq!(v["prometheus"]["state"], "failing");
+        assert!(
+            v["prometheus"]["error"].as_str().unwrap().contains("503"),
+            "{v}"
+        );
+        let before = asked.lock().unwrap().len();
+        for q in ["node=node-a", "pipeline=orders&table=public.orders"] {
+            let (_, v) =
+                json_of(addr, "GET", &format!("/v1/history?{q}&since={}", since(30))).await;
+            assert!(
+                v["series"]
+                    .as_array()
+                    .unwrap()
+                    .iter()
+                    .all(|s| s["older"] == "not_in_prometheus"),
+                "{v}"
+            );
+        }
+        assert_eq!(asked.lock().unwrap().len(), before);
+    }
 }
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index 2615726..e16a25b 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -21,6 +21,7 @@ mod nodes;
 mod openapi;
 mod peers;
 mod pipelines;
+mod prometheus;
 mod server;
 mod snapshots;
 mod tables;
````

`crates/data-avalanche-api/src/prometheus.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/prometheus.rs b/crates/data-avalanche-api/src/prometheus.rs
new file mode 100644
index 0000000..f8004ae
--- /dev/null
+++ b/crates/data-avalanche-api/src/prometheus.rs
@@ -0,0 +1,64 @@
+//! The Prometheus reader (Admin_Console §7, D-M13e.6): a pipeline's series older than the history
+//! kept come from `globals.console.prometheus` when it is set. Each series is one range query
+//! over the node's own metrics, grouped by pipeline, with `extra_labels` in every selector. A
+//! table's and a node's series aren't in Prometheus (their metrics carry no such label).
+
+#[cfg(test)]
+mod tests {
+    use data_avalanche_state::{SeriesScope, series_of};
+
+    use super::*;
+
+    fn s(name: &str) -> &'static Series {
+        series_of(SeriesScope::Pipeline, name).unwrap()
+    }
+
+    #[test]
+    fn each_pipeline_series_has_its_query() {
+        let names = vec!["orders".to_owned(), "bill.ing".to_owned()];
+        let extra = BTreeMap::from([("cluster".to_owned(), "prod \"eu\"".to_owned())]);
+        assert_eq!(
+            promql(s("lag_seconds"), &names, &extra, 60).unwrap(),
+            r#"max by (pipeline) (data_avalanche_replication_lag_seconds{pipeline=~"orders|bill\\.ing",cluster="prod \"eu\""})"#
+        );
+        assert_eq!(
+            promql(s("changes_per_minute"), &names[..1], &BTreeMap::new(), 600).unwrap(),
+            r#"60 * sum by (pipeline) (rate(data_avalanche_rows_read_total{pipeline=~"orders",op!="snapshot"}[600s]))"#
+        );
+        assert_eq!(
+            promql(
+                s("snapshot_rows_per_second"),
+                &names[..1],
+                &BTreeMap::new(),
+                15
+            )
+            .unwrap(),
+            r#"sum by (pipeline) (rate(data_avalanche_rows_read_total{pipeline=~"orders",op="snapshot"}[300s]))"#
+        );
+        for name in ["wal_held_bytes", "binlog_lag_seconds"] {
+            assert!(promql(s(name), &names, &extra, 60).is_some(), "{name}");
+        }
+        let table = series_of(SeriesScope::Table, "commit_seconds").unwrap();
+        assert_eq!(promql(table, &names, &extra, 60), None);
+    }
+
+    #[test]
+    fn a_reader_keeps_its_client_while_the_config_stays() {
+        let config = Prometheus {
+            url: "http://127.0.0.1:9".into(),
+            auth: PrometheusAuth::None,
+            headers: BTreeMap::new(),
+            ca: None,
+            extra_labels: BTreeMap::new(),
+            timeout: std::time::Duration::from_secs(5),
+        };
+        let r = PrometheusReader::default();
+        let a = r.client(&config).unwrap();
+        assert!(Arc::ptr_eq(&a, &r.client(&config).unwrap()));
+        let other = Prometheus {
+            url: "http://127.0.0.1:10".into(),
+            ..config
+        };
+        assert!(!Arc::ptr_eq(&a, &r.client(&other).unwrap()));
+    }
+}
````

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-api --lib -- prometheus:: history::`
Expected: the build fails: the new module's tests find no `promql`, `PrometheusReader`, `Prometheus`, `PrometheusAuth` or `Series` (nor the imports the code above them brings).

- [ ] **Step 3: Implement**

`crates/data-avalanche-api/src/history.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/history.rs b/crates/data-avalanche-api/src/history.rs
index 2f977b3..45b50da 100644
--- a/crates/data-avalanche-api/src/history.rs
+++ b/crates/data-avalanche-api/src/history.rs
@@ -1,12 +1,13 @@
 //! `GET /v1/history` (Admin_Console §7, D-M13e.5): the series the nodes sampled, read by any node
 //! from the chart history in `globals.state`, over a range and at a step. A pipeline's series
-//! follow it across its lease terms, a node's across its processes.
+//! follow it across its lease terms, a node's across its processes. A pipeline's range older than
+//! the history kept comes from `globals.console.prometheus` when it is set (D-M13e.6).
 
 use axum::extract::{RawQuery, State};
 use axum::http::StatusCode;
 use axum::response::Response;
 use chrono::{DateTime, TimeDelta, Utc};
-use data_avalanche_config::{Pipeline, ResolvedConfig};
+use data_avalanche_config::{Pipeline, Prometheus, ResolvedConfig};
 use data_avalanche_state::{
     ChartChunk, ChartScope, Fold, SERIES, Series, SeriesScope, StateError, series_of,
 };
@@ -15,6 +16,7 @@ use serde_json::{Value, json};
 use std::sync::Arc;
 
 use crate::events::time;
+use crate::prometheus::{Outcome, promql};
 use crate::snapshots::{READ_AT_ONCE, named_table};
 use crate::{Api, Failure, Node, body, starting};
 
@@ -368,21 +370,123 @@ pub(crate) async fn read(State(api): State<Api>, RawQuery(raw): RawQuery) -> Res
     };
     let retention = config.globals.console.history.retention;
     let kept_since = now - TimeDelta::from_std(retention).unwrap_or(MAX_SPAN);
-    match answer(node, &q, kept_since, now).await {
-        Ok(series) => body(
-            StatusCode::OK,
-            &json!({
-                "since": q.since,
-                "until": q.until,
-                "step": q.step,
-                "kept_since": kept_since,
-                "series": series,
+    let mut series = match answer(node, &q, kept_since, now).await {
+        Ok(series) => series,
+        Err(e) => {
+            return body(
+                StatusCode::SERVICE_UNAVAILABLE,
+                &json!({ "error": format!("the chart history: {e}") }),
+            );
+        }
+    };
+    let mut out = json!({
+        "since": q.since,
+        "until": q.until,
+        "step": q.step,
+        "kept_since": kept_since,
+    });
+    // Without a Prometheus, the history kept is the whole answer: nothing is marked missing.
+    if let Some(prometheus) = &config.globals.console.prometheus {
+        if q.since < kept_since {
+            older(node, prometheus, &q, kept_since, &mut series).await;
+        }
+        out["prometheus"] = match node.prometheus.last() {
+            None => json!({ "state": "unasked" }),
+            Some(o) => json!({
+                "state": if o.error.is_some() { "failing" } else { "ok" },
+                "at": o.at,
+                "error": o.error,
             }),
-        ),
-        Err(e) => body(
-            StatusCode::SERVICE_UNAVAILABLE,
-            &json!({ "error": format!("the chart history: {e}") }),
-        ),
+        };
+    }
+    out["series"] = json!(series);
+    body(StatusCode::OK, &out)
+}
+
+/// Adds to `series` the points of their range before `kept_since` from `prometheus`, at the
+/// query's step, and says where each one's older range comes from (`older`): `prometheus`,
+/// `failing` (Prometheus didn't answer; the range shows the history kept only), or
+/// `not_in_prometheus` (a table's or a node's series). Records how the reads went.
+async fn older(
+    node: &Node,
+    prometheus: &Prometheus,
+    q: &Query,
+    kept_since: DateTime<Utc>,
+    series: &mut [Value],
+) {
+    let Asked::Pipelines(ps) = &q.asked else {
+        for s in series.iter_mut() {
+            s["older"] = json!("not_in_prometheus");
+        }
+        return;
+    };
+    let names: Vec<String> = ps.iter().map(|p| p.name.clone()).collect();
+    let start = q.since.timestamp().div_euclid(q.step) * q.step;
+    // A point's step must end by `kept_since`: the step it begins is the history's.
+    let last = kept_since.timestamp().div_euclid(q.step) * q.step - q.step;
+    let (Some(start_at), Some(end_at)) = (
+        DateTime::from_timestamp(start, 0),
+        DateTime::from_timestamp(last.min(q.until.timestamp()), 0),
+    ) else {
+        return;
+    };
+    if start_at > end_at {
+        // The range older than the history kept is within the step the history begins.
+        return;
+    }
+    let read = match node.prometheus.client(prometheus) {
+        Err(e) => Err(e),
+        Ok(client) => {
+            let queries = q.series.iter().map(|s| {
+                let client = Arc::clone(&client);
+                let query = promql(s, &names, &prometheus.extra_labels, q.step);
+                async move {
+                    match query {
+                        None => Ok((s.name, Default::default())),
+                        Some(query) => client
+                            .range(&query, start_at, end_at, q.step)
+                            .await
+                            .map(|r| (s.name, r)),
+                    }
+                }
+            });
+            futures_util::future::join_all(queries)
+                .await
+                .into_iter()
+                .collect::<Result<Vec<_>, _>>()
+        }
+    };
+    node.prometheus.record(Outcome {
+        at: Utc::now(),
+        error: read.as_ref().err().map(ToString::to_string),
+    });
+    let Ok(read) = read else {
+        for s in series.iter_mut() {
+            s["older"] = json!("failing");
+        }
+        return;
+    };
+    let by_id: std::collections::BTreeMap<&str, &str> = ps
+        .iter()
+        .map(|p| (p.id.as_str(), p.name.as_str()))
+        .collect();
+    for s in series.iter_mut() {
+        let name = s["pipeline"].as_str().and_then(|id| by_id.get(id)).copied();
+        let found = read
+            .iter()
+            .find(|(n, _)| Some(*n) == s["name"].as_str())
+            .and_then(|(_, r)| name.and_then(|n| r.get(n)));
+        let mut points: Vec<Value> = found
+            .into_iter()
+            .flatten()
+            .filter(|(t, _)| *t >= start && *t <= last)
+            .map(|(t, v)| json!([t, v]))
+            .collect();
+        if let Some(kept) = s["points"].as_array() {
+            points.extend(kept.iter().cloned());
+        }
+        s["points"] = json!(points);
+        s["older"] = json!("prometheus");
     }
 }
 
````

`crates/data-avalanche-api/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/lib.rs b/crates/data-avalanche-api/src/lib.rs
index e16a25b..6dbb935 100644
--- a/crates/data-avalanche-api/src/lib.rs
+++ b/crates/data-avalanche-api/src/lib.rs
@@ -59,6 +59,10 @@ pub use peers::{
     ACTION_LIMIT, ANSWERED_BY, DETAIL_LIMIT, FORWARDED_BY, MAX_ANSWER, Peers, PeersError,
     STATUS_LIMIT, TARGET,
 };
+pub use prometheus::{
+    MAX_PROMETHEUS_ANSWER, MIN_RATE_WINDOW, Outcome, PrometheusClient, PrometheusError,
+    PrometheusReader, promql,
+};
 pub use server::{Bound, HANDSHAKE_TIMEOUT, IDLE_TIMEOUT, MAX_CONNECTIONS, ServeError, bind};
 pub use snapshots::{EndedJobs, READ_AT_ONCE};
 pub use tables::{FACTS_FOR, FACTS_WITHIN, Lake};
@@ -114,6 +118,8 @@ pub struct Node {
     pub lake: Arc<Lake>,
     /// The chart history every node samples into `globals.state` (Admin_Console §7).
     pub charts: Arc<Charts>,
+    /// Reads a pipeline's series older than the history kept from `globals.console.prometheus`.
+    pub prometheus: Arc<PrometheusReader>,
 }
 
 impl std::fmt::Debug for Node {
````

`crates/data-avalanche-api/src/prometheus.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/prometheus.rs b/crates/data-avalanche-api/src/prometheus.rs
index f8004ae..1d63d8b 100644
--- a/crates/data-avalanche-api/src/prometheus.rs
+++ b/crates/data-avalanche-api/src/prometheus.rs
@@ -3,6 +3,302 @@
 //! over the node's own metrics, grouped by pipeline, with `extra_labels` in every selector. A
 //! table's and a node's series aren't in Prometheus (their metrics carry no such label).
 
+use std::collections::BTreeMap;
+use std::sync::{Arc, Mutex};
+
+use chrono::{DateTime, Utc};
+use data_avalanche_config::{Prometheus, PrometheusAuth};
+use data_avalanche_state::Series;
+use serde::Deserialize;
+
+/// The largest answer read from Prometheus.
+pub const MAX_PROMETHEUS_ANSWER: usize = 8 << 20;
+
+/// The shortest window a rate looks over: a few scrapes at the usual intervals.
+pub const MIN_RATE_WINDOW: i64 = 300;
+
+#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
+pub enum PrometheusError {
+    #[error("the Prometheus client: {0}")]
+    Client(String),
+    #[error("Prometheus can't be reached: {0}")]
+    Unreachable(String),
+    #[error("Prometheus answered {code}: {text}")]
+    Answered { code: u16, text: String },
+    #[error("Prometheus's answer isn't one: {0}")]
+    Invalid(String),
+}
+
+/// A PromQL string literal of `v`.
+fn quoted(v: &str) -> String {
+    let mut out = String::with_capacity(v.len() + 2);
+    out.push('"');
+    for c in v.chars() {
+        match c {
+            '\\' => out.push_str("\\\\"),
+            '"' => out.push_str("\\\""),
+            '\n' => out.push_str("\\n"),
+            c => out.push(c),
+        }
+    }
+    out.push('"');
+    out
+}
+
+/// `v` matched literally inside a regular expression.
+fn literal(v: &str) -> String {
+    let mut out = String::with_capacity(v.len());
+    for c in v.chars() {
+        if "\\.+*?()|[]{}^$".contains(c) {
+            out.push('\\');
+        }
+        out.push(c);
+    }
+    out
+}
+
+/// The selector of the pipelines named `names`, `extra` and `more`.
+fn selector(names: &[String], extra: &BTreeMap<String, String>, more: &str) -> String {
+    let pipelines = names
+        .iter()
+        .map(|n| literal(n))
+        .collect::<Vec<_>>()
+        .join("|");
+    let mut m = vec![format!("pipeline=~{}", quoted(&pipelines))];
+    m.extend(extra.iter().map(|(k, v)| format!("{k}={}", quoted(v))));
+    if !more.is_empty() {
+        m.push(more.to_owned());
+    }
+    format!("{{{}}}", m.join(","))
+}
+
+/// The query of series `s` for the pipelines named `names`, one result per pipeline, at `step`
+/// seconds; `None` for a series Prometheus doesn't have.
+pub fn promql(
+    s: &Series,
+    names: &[String],
+    extra: &BTreeMap<String, String>,
+    step: i64,
+) -> Option<String> {
+    let window = step.max(MIN_RATE_WINDOW);
+    let gauge =
+        |metric: &str| format!("max by (pipeline) ({metric}{})", selector(names, extra, ""));
+    let rate = |op: &str| {
+        format!(
+            "sum by (pipeline) (rate(data_avalanche_rows_read_total{}[{window}s]))",
+            selector(names, extra, op)
+        )
+    };
+    match s.name {
+        "lag_seconds" => Some(gauge("data_avalanche_replication_lag_seconds")),
+        "wal_held_bytes" => Some(gauge("data_avalanche_wal_lag_bytes")),
+        "binlog_lag_seconds" => Some(gauge("data_avalanche_binlog_lag_seconds")),
+        "changes_per_minute" => Some(format!("60 * {}", rate(r#"op!="snapshot""#))),
+        "snapshot_rows_per_second" => Some(rate(r#"op="snapshot""#)),
+        _ => None,
+    }
+}
+
+#[derive(Deserialize)]
+struct Answer {
+    status: String,
+    #[serde(default)]
+    data: Option<Data>,
+    #[serde(default)]
+    error: Option<String>,
+}
+
+#[derive(Deserialize)]
+struct Data {
+    #[serde(rename = "resultType")]
+    result_type: String,
+    result: Vec<Range>,
+}
+
+#[derive(Deserialize)]
+struct Range {
+    metric: BTreeMap<String, String>,
+    values: Vec<(f64, String)>,
+}
+
+/// A client of one `globals.console.prometheus`.
+#[derive(Debug)]
+pub struct PrometheusClient {
+    config: Prometheus,
+    http: reqwest::Client,
+}
+
+impl PrometheusClient {
+    /// The client of `config`: its CA besides the platform's roots, its time limit per query; no
+    /// redirects or proxies.
+    ///
+    /// # Errors
+    /// [`PrometheusError::Client`]: the CA isn't PEM, or the client can't be built.
+    pub fn new(config: &Prometheus) -> Result<Self, PrometheusError> {
+        let mut b = reqwest::Client::builder()
+            .redirect(reqwest::redirect::Policy::none())
+            .no_proxy()
+            .timeout(config.timeout);
+        if let Some(ca) = &config.ca {
+            let certs = reqwest::Certificate::from_pem_bundle(ca.text.expose().as_bytes())
+                .map_err(|e| PrometheusError::Client(format!("tls.ca: {e}")))?;
+            b = b.tls_certs_merge(certs);
+        }
+        Ok(Self {
+            config: config.clone(),
+            http: b
+                .build()
+                .map_err(|e| PrometheusError::Client(e.to_string()))?,
+        })
+    }
+
+    /// `query`'s values from `start` to `end` every `step` seconds, by the `pipeline` label;
+    /// values that aren't finite are left out.
+    ///
+    /// # Errors
+    /// [`PrometheusError`]: unreachable, past the time limit, an error answer, or one that
+    /// isn't a range.
+    pub async fn range(
+        &self,
+        query: &str,
+        start: DateTime<Utc>,
+        end: DateTime<Utc>,
+        step: i64,
+    ) -> Result<BTreeMap<String, Vec<(i64, f64)>>, PrometheusError> {
+        let url = format!(
+            "{}/api/v1/query_range",
+            self.config.url.trim_end_matches('/')
+        );
+        let form = url::form_urlencoded::Serializer::new(String::new())
+            .append_pair("query", query)
+            .append_pair("start", &start.timestamp().to_string())
+            .append_pair("end", &end.timestamp().to_string())
+            .append_pair("step", &format!("{step}s"))
+            .finish();
+        let mut req = self
+            .http
+            .post(url)
+            .header("Content-Type", "application/x-www-form-urlencoded")
+            .body(form);
+        req = match &self.config.auth {
+            PrometheusAuth::None => req,
+            PrometheusAuth::Basic { user, password } => {
+                req.basic_auth(user, Some(password.expose()))
+            }
+            PrometheusAuth::Bearer { token } => req.bearer_auth(token.expose()),
+        };
+        for (name, value) in &self.config.headers {
+            req = req.header(name.as_str(), value.expose());
+        }
+        let answer = req
+            .send()
+            .await
+            .map_err(|e| PrometheusError::Unreachable(e.without_url().to_string()))?;
+        let code = answer.status().as_u16();
+        if answer
+            .content_length()
+            .is_some_and(|n| n > MAX_PROMETHEUS_ANSWER as u64)
+        {
+            return Err(PrometheusError::Invalid(format!(
+                "over {MAX_PROMETHEUS_ANSWER} bytes"
+            )));
+        }
+        let bytes = answer
+            .bytes()
+            .await
+            .map_err(|e| PrometheusError::Unreachable(e.without_url().to_string()))?;
+        if bytes.len() > MAX_PROMETHEUS_ANSWER {
+            return Err(PrometheusError::Invalid(format!(
+                "over {MAX_PROMETHEUS_ANSWER} bytes"
+            )));
+        }
+        let parsed: Result<Answer, _> = serde_json::from_slice(&bytes);
+        let a = match parsed {
+            Ok(a) if a.status == "success" && code == 200 => a,
+            Ok(a) => {
+                return Err(PrometheusError::Answered {
+                    code,
+                    text: a.error.unwrap_or_else(|| a.status.clone()),
+                });
+            }
+            Err(_) if code != 200 => {
+                let text = String::from_utf8_lossy(&bytes);
+                return Err(PrometheusError::Answered {
+                    code,
+                    text: text.chars().take(200).collect(),
+                });
+            }
+            Err(e) => return Err(PrometheusError::Invalid(e.to_string())),
+        };
+        let data = a
+            .data
+            .filter(|d| d.result_type == "matrix")
+            .ok_or_else(|| PrometheusError::Invalid("not a range (matrix)".into()))?;
+        let mut out = BTreeMap::new();
+        for r in data.result {
+            let Some(pipeline) = r.metric.get("pipeline") else {
+                continue;
+            };
+            let points: Vec<(i64, f64)> = r
+                .values
+                .iter()
+                .filter_map(|(t, v)| {
+                    let v: f64 = v.parse().ok()?;
+                    v.is_finite().then_some((t.round() as i64, v))
+                })
+                .collect();
+            out.insert(pipeline.clone(), points);
+        }
+        Ok(out)
+    }
+
+    pub fn config(&self) -> &Prometheus {
+        &self.config
+    }
+}
+
+/// How the node's last read of Prometheus went.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct Outcome {
+    pub at: DateTime<Utc>,
+    /// The error; `None` when it answered.
+    pub error: Option<String>,
+}
+
+/// The node's Prometheus client, made again when the config changes, and how its last read
+/// went.
+#[derive(Debug, Default)]
+pub struct PrometheusReader {
+    client: Mutex<Option<Arc<PrometheusClient>>>,
+    last: Mutex<Option<Outcome>>,
+}
+
+impl PrometheusReader {
+    /// The client of `config`, the one made before while it is the same.
+    ///
+    /// # Errors
+    /// [`PrometheusError::Client`]: it can't be made.
+    pub fn client(&self, config: &Prometheus) -> Result<Arc<PrometheusClient>, PrometheusError> {
+        let mut held = self.client.lock().expect("unpoisoned");
+        if let Some(c) = held.as_ref()
+            && c.config() == config
+        {
+            return Ok(Arc::clone(c));
+        }
+        let c = Arc::new(PrometheusClient::new(config)?);
+        *held = Some(Arc::clone(&c));
+        Ok(c)
+    }
+
+    pub fn record(&self, outcome: Outcome) {
+        *self.last.lock().expect("unpoisoned") = Some(outcome);
+    }
+
+    pub fn last(&self) -> Option<Outcome> {
+        self.last.lock().expect("unpoisoned").clone()
+    }
+}
+
 #[cfg(test)]
 mod tests {
     use data_avalanche_state::{SeriesScope, series_of};
````

`crates/data-avalanche-api/src/test_support.rs`:

````diff
diff --git a/crates/data-avalanche-api/src/test_support.rs b/crates/data-avalanche-api/src/test_support.rs
index aa587a3..2d177f0 100644
--- a/crates/data-avalanche-api/src/test_support.rs
+++ b/crates/data-avalanche-api/src/test_support.rs
@@ -316,6 +316,7 @@ pub(crate) async fn lone_with(file: PathBuf, text: &str, env: Arc<dyn Env + Send
             factory: Arc::new(Lakes(sink.clone())),
             lake: Arc::default(),
             charts: Arc::new(data_avalanche_state::Charts::new(docs.clone())),
+            prometheus: Arc::default(),
         },
         events,
         leases,
````

`crates/data-avalanche/src/run.rs`:

````diff
diff --git a/crates/data-avalanche/src/run.rs b/crates/data-avalanche/src/run.rs
index 555d7fe..049c9a5 100644
--- a/crates/data-avalanche/src/run.rs
+++ b/crates/data-avalanche/src/run.rs
@@ -329,6 +329,7 @@ pub async fn run(args: RunArgs) -> Result<(), Fatal> {
         factory: Arc::clone(&deps.factory),
         lake: Arc::default(),
         charts,
+        prometheus: Arc::default(),
     });
     ready.store(true, Ordering::SeqCst);
     let mut cluster = Some((gossip, reconciler, discovery));
````

- [ ] **Step 4: Run the tests and the gates**

Run: `cargo test -p data-avalanche-api --lib -- prometheus:: history::`, then

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-api/src/history.rs crates/data-avalanche-api/src/lib.rs crates/data-avalanche-api/src/prometheus.rs crates/data-avalanche-api/src/test_support.rs crates/data-avalanche/src/run.rs
git commit -m "feat(api): a pipeline's history older than the retention from globals.console.prometheus"
```

---

### Task 6: The site and the research doc

**Files:**
- Modify: `doc/research/Observability_Metrics_and_Tracing.md`
- Modify: `docs/src/content/docs/deployment/observability.md`

**Interfaces:**
- Consumes: Tasks 1–5.
- Produces: the observability page's "Chart history" and "Older ranges from Prometheus";
  `Observability_Metrics_and_Tracing.md` §5a.

- [ ] **Step 1: Write the pages**

The observability page says what each node samples, where it keeps it, for how long and what it costs, how `GET /v1/history` reads it, and how `globals.console.prometheus` fills the older range; the research doc records the layout and the cadence.

`doc/research/Observability_Metrics_and_Tracing.md`:

````diff
diff --git a/doc/research/Observability_Metrics_and_Tracing.md b/doc/research/Observability_Metrics_and_Tracing.md
index 69cc064..3a829c0 100644
--- a/doc/research/Observability_Metrics_and_Tracing.md
+++ b/doc/research/Observability_Metrics_and_Tracing.md
@@ -1,7 +1,7 @@
 ---
-updated: 2026-10-29
+updated: 2026-11-18
 domain: Architecture
-tags: [observability, metrics, prometheus, tracing, otlp, logs, correlation, cardinality, cdc]
+tags: [observability, metrics, prometheus, tracing, otlp, logs, correlation, cardinality, cdc, chart-history, console]
 resolves: [G-33, G-32]
 ---
 
@@ -188,6 +188,39 @@ the cardinality policy; the §16 `otlp_endpoint` reservation promoted.
 
 ---
 
+## §5a — Chart history (1.1.0, M13e)
+
+The console's charts (`Admin_Console.md` §4.2, §7) read a built-in history, independent of any
+Prometheus. Settled in `doc/v1.1/M13e-console-history.md` (D-M13e.1–7):
+
+- **Who samples what.** Each node samples the series it owns every 15 s: the pipelines whose lease
+  it holds (`lag_seconds` as the live figures compute it, `changes_per_minute`, `wal_held_bytes`,
+  `binlog_lag_seconds`, `snapshot_rows_per_second`), their tables every minute (every table the
+  worker tracks; one new to the term's chunk at once)
+  (`changes_per_minute`, `files_rewritten`: the minute's costliest MERGE, `commit_seconds`: its
+  slowest commit), and itself (`memory_bytes`, `cpu_cores`, `lease_renew_seconds`: its slowest
+  renewal since the last sample).
+- **Layout.** `globals.state/history/<YYYY-MM-DD>/<pipelines|nodes>/<id>/<HH>-<writer>.json`: one
+  object per scope, hour and writer, the writer a lease term (`e<epoch>`) or a process
+  (`i<incarnation>`). Only its writer writes it, unconditionally, so a failover's two terms both
+  stay and no write races another. Samples are `{t: seconds into the hour, v}` lists.
+- **Cadence.** A chunk is written with its first sample or a new series, then every minute while it changes, and
+  once more when its hour or its term ends: a crash loses at most a minute; about 60 writes an
+  hour per pipeline. A failing store keeps the samples in memory (an hour past their hour at
+  most) and never slows a pipeline.
+- **Retention.** `globals.console.history.retention` (24 h): whole days deleted on every node, as
+  the events are; reads stop at the retention whatever storage still holds.
+- **Reading.** `GET /v1/history` lists a day's folder per scope and loads the hours that meet the
+  range, 16 at a time; an hour ended over 5 minutes ago is final and kept in memory (4096 at
+  most). A coarser step folds a gauge to its peak and a rate to its mean.
+- **Prometheus** (`globals.console.prometheus`): a pipeline's range older than the retention is one
+  `query_range` per series over the catalogue's metrics, `by (pipeline)`, with `extra_labels` in
+  every selector; a rate over at least 5 minutes. Tables and nodes have no such series (their
+  metrics carry no `table` or node label) and say so (`not_in_prometheus`). Prometheus failing
+  never fails an answer.
+
+---
+
 ## §6 — Resolved decision summary
 
 | Decision | Resolution |
````

`docs/src/content/docs/deployment/observability.md`:

````diff
diff --git a/docs/src/content/docs/deployment/observability.md b/docs/src/content/docs/deployment/observability.md
index 47e4205..bfabec3 100644
--- a/docs/src/content/docs/deployment/observability.md
+++ b/docs/src/content/docs/deployment/observability.md
@@ -58,6 +58,59 @@ is a starting set of Prometheus rules, with `promtool` tests beside it:
 | `DataAvalancheWalLag` | a slot holds more than 10 GiB of WAL for 10 minutes | [A lost position](/data-avalanche/runbooks/position-lost/) |
 | `DataAvalancheEventsDropped` | a node dropped events from the cluster's timeline in the last 15 minutes | [Events](/data-avalanche/deployment/events/#when-the-timeline-has-gaps) |
 
+## Chart history
+
+Every node keeps a short history of the figures its charts need, without Prometheus, in the
+cluster's state location (`globals.state`, under `history/`). Each node samples what it owns:
+
+| Of | Series | Every |
+|---|---|---|
+| a pipeline, by the node that holds its lease | `lag_seconds`, `changes_per_minute`, `wal_held_bytes` (PostgreSQL), `binlog_lag_seconds` (MySQL, MariaDB), `snapshot_rows_per_second` (while a snapshot copies) | 15 s |
+| each of its tables | `changes_per_minute`, `files_rewritten` (the costliest MERGE of the minute), `commit_seconds` (the slowest commit of the minute) | 1 min; a table the pipeline starts tracking at once |
+| the node | `memory_bytes` and `cpu_cores` (Linux), `lease_renew_seconds` (its slowest lease renewal since the sample before) | 15 s |
+
+`lag_seconds` is what the pipeline's live figures show: how far behind lake readers are. The
+samples of the current hour are written when the hour begins or a new series starts (a table the
+pipeline starts tracking shows within a sample), then every minute, so a node that
+crashes loses at most a minute of them, and a pipeline's history follows it to the node that
+takes it over. A storage outage holds the samples in memory and writes them when it ends; it
+never slows a pipeline. History older than `globals.console.history.retention` (24 h by default)
+is deleted a day at a time. A pipeline's history costs about 60 writes an hour, to one object
+an hour.
+
+`GET /v1/history` reads it, from any node:
+
+```sh
+curl -u oleksii:… 'https://avalanche.example.com:8080/v1/history?pipeline=orders-pg&series=lag_seconds&since=2026-11-18T06:00:00Z'
+```
+
+It answers each series' points, `[unix seconds, value]`, at a step that keeps a series to 1440
+points (a longer step keeps each step's peak of a lag or a size, and the mean of a rate), and
+`kept_since`, where the history kept begins.
+
+### Older ranges from Prometheus
+
+With `globals.console.prometheus` set, a pipeline's range older than the retention comes from
+that Prometheus, which scrapes the nodes' `/metrics`: each series is one range query over
+`data_avalanche_replication_lag_seconds`, `data_avalanche_rows_read_total`,
+`data_avalanche_wal_lag_bytes` or `data_avalanche_binlog_lag_seconds`, grouped by `pipeline`,
+with `extra_labels` in every selector so another cluster's series never mix in:
+
+```yaml
+globals:
+  console:
+    prometheus:
+      url: https://prometheus.observability:9090
+      bearer_token: ${PROM_TOKEN}
+      extra_labels: { cluster: prod-eu }
+```
+
+Each series then says where its older range came from (`older`): `prometheus`; `failing`, when
+Prometheus didn't answer within `timeout` (the history kept is shown, and the answer's
+`prometheus` says what failed); or `not_in_prometheus` for a table's or a node's series, whose
+metrics carry no such label. Without the setting, the history kept is the whole answer, and
+nothing is marked.
+
 ## The log
 
 To stderr, as `globals.log.format`: `json` (one object a line), `compact` or `pretty`; at
````

- [ ] **Step 2: Build the site**

Run: `cd docs && npm ci && npm run build`
Expected: the build ends with "All internal links are valid" and 31 pages.

- [ ] **Step 3: Run the gates**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 4: Commit**

```bash
git add doc/research/Observability_Metrics_and_Tracing.md docs/src/content/docs/deployment/observability.md
git commit -m "docs: the chart history and its Prometheus reader; GET /v1/history"
```

---

### Task 7: The scenario on three nodes (`data-avalanche` E2E)

**Files:**
- Create: `crates/data-avalanche/tests/e2e/console_history.rs`
- Modify: `crates/data-avalanche/tests/e2e/main.rs`

**Interfaces:**
- Consumes: the E2E harness's `Cluster` (M8), `eventually`; Tasks 3–4 through the binary.
- Produces: `console_history::a_pipeline_s_history_follows_it_across_its_nodes`.

- [ ] **Step 1: Write the scenario**

Through a node that doesn't hold the test's pipeline: its lag, its table's series and the holder's renewals, each written with its first sample; then the holder killed, the next term's lag after the kill, and every point the killed holder wrote still there.

`crates/data-avalanche/tests/e2e/console_history.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/console_history.rs b/crates/data-avalanche/tests/e2e/console_history.rs
new file mode 100644
index 0000000..9459586
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/console_history.rs
@@ -0,0 +1,99 @@
+//! The chart history through any node (Admin_Console §7; M13e), on three nodes: a node that
+//! doesn't run the test's pipeline reads its lag, its table's series and the holder's own series
+//! from `globals.state`; the holder killed, the pipeline's history goes on under the next lease
+//! term and keeps what the killed holder wrote.
+
+use std::time::Duration;
+
+use chrono::Utc;
+use data_avalanche_testkit::require_it;
+use serde_json::Value;
+
+use crate::harness::{Doc, eventually};
+use crate::nodes::Cluster;
+
+/// The times of the points of series `name` in a history answer.
+fn times(v: &Value, name: &str) -> Vec<i64> {
+    v["series"]
+        .as_array()
+        .into_iter()
+        .flatten()
+        .filter(|s| s["name"] == name)
+        .flat_map(|s| s["points"].as_array().cloned().unwrap_or_default())
+        .filter_map(|p| p[0].as_i64())
+        .collect()
+}
+
+#[tokio::test]
+async fn a_pipeline_s_history_follows_it_across_its_nodes() {
+    require_it!();
+    let mut c = Cluster::new("console_history").await;
+    c.start_all().await;
+    let test = c.e.test.clone();
+    c.e.insert_series("items", 5, Doc::Row).await;
+    c.landed().await;
+    let holders = c.holders().await;
+    let holder = holders[&test].0.clone();
+    let other = ["n1", "n2", "n3"]
+        .into_iter()
+        .find(|n| *n != holder)
+        .expect("another node")
+        .to_owned();
+    let items = if c.e.source.is_postgres() {
+        format!("{}.items", c.e.schema)
+    } else {
+        "items".to_owned()
+    };
+
+    // The pipeline's lag and its table's series, written with their first samples, read through
+    // a node that doesn't run it; the holder's own series too.
+    let lag = format!("/v1/history?pipeline={test}&series=lag_seconds");
+    let table = format!("/v1/history?pipeline={test}&table={items}");
+    let node = format!("/v1/history?node={holder}&series=lease_renew_seconds");
+    for (what, path, name) in [
+        ("the lag", &lag, "lag_seconds"),
+        ("the table's changes", &table, "changes_per_minute"),
+        ("the holder's renewals", &node, "lease_renew_seconds"),
+    ] {
+        eventually(what, Duration::from_secs(60), async || {
+            let (code, v) = c.proc(&other).call("GET", path).await;
+            code == 200 && !times(&v, name).is_empty()
+        })
+        .await;
+    }
+    let (_, v) = c.proc(&other).call("GET", &table).await;
+    let names: Vec<&str> = v["series"]
+        .as_array()
+        .unwrap()
+        .iter()
+        .map(|s| s["name"].as_str().unwrap())
+        .collect();
+    assert_eq!(
+        names,
+        ["changes_per_minute", "files_rewritten", "commit_seconds"],
+        "{v}"
+    );
+    assert_eq!(v["series"][0]["table"], items.as_str(), "{v}");
+
+    // The holder killed: the next term's samples join what the killed holder wrote.
+    let (_, before) = c.proc(&other).call("GET", &lag).await;
+    let written = times(&before, "lag_seconds");
+    let killed = Utc::now().timestamp();
+    let i = c.index(&holder);
+    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
+    c.settled().await;
+    let next = c.holders().await[&test].clone();
+    assert_ne!(next.0, holder);
+    eventually("the next term's lag", Duration::from_secs(60), async || {
+        let (code, v) = c.proc(&other).call("GET", &lag).await;
+        code == 200 && times(&v, "lag_seconds").iter().any(|t| *t > killed)
+    })
+    .await;
+    let (_, after) = c.proc(&other).call("GET", &lag).await;
+    let all = times(&after, "lag_seconds");
+    assert!(
+        written.iter().all(|t| all.contains(t)),
+        "the killed holder's samples are kept: {written:?} in {all:?}"
+    );
+    c.stop().await;
+}
````

`crates/data-avalanche/tests/e2e/main.rs`:

````diff
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index e04a52e..34dd668 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -8,6 +8,7 @@
 
 mod api;
 mod cluster;
+mod console_history;
 mod console_nodes;
 mod console_pipelines;
 mod crashes;
````

- [ ] **Step 2: Run it**

Run, with the emulators up (`test-infra/up.sh`):

```bash
DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e console_history -- --test-threads=1
for m in mysql-84 mysql-84-nogtid mariadb-118; do
  DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_E2E_SOURCE=mysql DATA_AVALANCHE_MYSQL=$m \
    cargo test -p data-avalanche --features e2e-hooks --test e2e console_history -- --test-threads=1
done
```

Expected: `1 passed` on each, in about 42 s. The scenario passes at once: Tasks 3 and 4 built what it checks.

- [ ] **Step 3: Run the gates**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings
cargo test --workspace
```

Expected: no output from the first three; every test passes.

- [ ] **Step 4: Commit**

```bash
git add crates/data-avalanche/tests/e2e/console_history.rs crates/data-avalanche/tests/e2e/main.rs
git commit -m "test(e2e): a pipeline's history through any node, across a failover"
```

---

## Exit

- Every task committed, one commit each, after the plan's and the spec patch's commits; the gates
  pass on the last one (`cargo fmt --all --check`, both clippy runs, `cargo test --workspace`).
- With the emulators up: the whole E2E suite on PostgreSQL 18 (`DATA_AVALANCHE_IT=1
  DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e --
  --test-threads=1`), and Task 7's scenario on `mysql-84`, `mysql-84-nogtid` and `mariadb-118`.
- `cargo xtask docs --check` and `cd docs && npm run build` (its link check) pass.
- At the plan's verification (the code above, on `cf78904`, each task's red and green commits apart): every Step 2 failure as stated; each task's gates and its tests pass at its commit; `cargo xtask docs --check` holds from Task 4 on; `cargo test --workspace` 1741 passed at Task 7; Task 7's scenario passed on PostgreSQL 18, `mysql-84`, `mysql-84-nogtid` and `mariadb-118` (42 s each); the site built (31 pages, link check clean).
- Then, on Oleksii's request: the whole-branch review and its fixes, the push, the PR.
