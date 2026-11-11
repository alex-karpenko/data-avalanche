---
updated: 2026-11-11
domain: Architecture
tags: [v1, implementation-plan, m12, m12c, acceptance, load, soak, chaos, cloud, release, v1.0.0]
resolves: [DA-SRC-39, DA-SRC-38, DA-SRC-37, DA-SNAP-23, DA-OPS-14]
---

# M12c — v1 Acceptance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** the last of M12's four parts (Q-M12-8): v1's acceptance, and `v1.0.0`. That covers:
- the four gaps left open after M12d, fixed (Q-M12c-1):
  - a session that ends on an error closes its PostgreSQL stream (DA-SRC-39);
  - server changes far apart start the alternation count again, and every alternation halts with
    a code (DA-SRC-38);
  - a refused reposition's halt stays while the lake holds the table (DA-SRC-37);
  - on MySQL, a sink column that differs from the source's by case alone is the same column at
    a first-met baseline (DA-SNAP-23);
- DA-OPS-14 closed by a measurement (D-M12c.1): no code;
- the E2E suite on any object store, and a cloud subset of it on real S3, GCS and Azure, GCS on
  every PR (Q-M12-2, Q-M12c-4);
- the acceptance runs, on Oleksii's machine first (Q-M12-3, Q-M12c-2, Q-M12c-3):
  - D-V1.3's lag targets measured at the product's defaults, in steady state and across a
    holder's loss;
  - MERGE's cost under random-key churn (R-5);
  - chaos: a storage outage shorter and longer than the lease, a source restart, a partition;
  - a 24 h soak with churn;
- the roadmap §8 checklist, the acceptance report and the version `1.0.0`. Oleksii pushes the
  `v1.0.0` tag after the merge; `release.yml` publishes it.

**Architecture:** the gap fixes are local: the stream session (Task 1), the controller's count
and one PostgreSQL message (Task 2), the start's halt clearing (Task 3), and the first-met
baseline (Task 4). The acceptance work is test tooling around the unchanged binary:

- **The E2E harness takes a store** (Task 5). `Store` (`tests/e2e/store.rs`) is RustFS by default,
  or what `DATA_AVALANCHE_E2E_STORE` names: `azurite` (the Azure path on the emulator), `s3`,
  `gcs` or `azure`. It gives the config's `globals.cloud` and bucket, the binary's credentials
  (passed through from the test's environment), and the test's own readers. The cloud subset is
  four existing scenarios listed in `test-infra/cloud-e2e.txt`; CI runs them by exact name.
- **The acceptance runs are a second test target** (Task 6). `tests/load` includes the E2E
  harness's modules by path, and the three-node `Cluster` moves from `cluster.rs` to a shared,
  test-free `nodes.rs`. A writer drives a steady mixed load; the lag gauge is sampled every second;
  each run ends with the lake holding the source's rows, each key once, and the owner epochs
  never going back. `cargo xtask bench|chaos|soak` builds the binary in release mode and writes the
  measurements to `target/acceptance/results.jsonl`.

**Tech Stack:** Rust 1.98.1 (pinned), tokio, the E2E harness (a binary per node, gossip
forwarders, a TCP proxy), `object_store` (S3, GCS, Azure; Azurite), `docker compose`, GitHub
Actions (`ci.yml`, `cloud.yml`).

**Spec:** `doc/specs/Data-Flow.md` (Phase 2b's dump checks and the Source Disconnect alternation
halt; Phase 4's reposition), `doc/specs/Schema_Change_Detection_and_Evolution.md` §7.2. D-V1.3
(`doc/research/V1_Scope_Decisions.md`) for the lag targets. The roadmap: §4 M12 (M12c's scope and
exit), §5.1 (`xtask bench`, `xtask soak`), §8 (the acceptance checklist). The tracker rows:
DA-SRC-37, 38 and DA-SNAP-23 (§60.1), DA-SRC-39 (§61.1), DA-OPS-14 (§54.1) of
`analysis/CDC_Design_Gap_Analysis_v1.md`.

**Spec patches:** three places where this plan goes past what the specs say, drafted in §Spec
patches below and applied in Task 7. **Oleksii approved the plan and the patches P-1…P-3 on
2026-11-11.**

## Global Constraints

- Toolchain 1.98.1 (`rust-toolchain.toml`); `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets -- -D warnings` (and with `-p data-avalanche --features
  e2e-hooks`) and `cargo test --workspace` pass before every commit. `unsafe_code = "forbid"`;
  libraries return `thiserror` errors; every new public item is re-exported at its crate root.
- Plain `cargo test` stays hermetic. A test that needs a server or an emulator starts with
  `require_it!()`; an acceptance run with `require_load!()` (`DATA_AVALANCHE_LOAD` beside
  `DATA_AVALANCHE_IT`). A cloud store needs its bucket variable and credentials, and runs only when
  `DATA_AVALANCHE_E2E_STORE` names it.
- No state document changes shape: v0.1.0 readers must read what M12c writes. `HaltReason` and
  `TableHalt` keep their fields; a refused reposition's halt is told by its detail's prefix.
- The halt codes are the existing ones. No new code.
- `SOURCE_CHANGES_IN_A_ROW = 3` (DA-SRC-34); `SOURCE_CHANGE_WINDOW = 10 min` (D-M12c.2).
- D-V1.3's targets at the product's defaults (`flush` 30 s, `lease_duration` 15 s renewed every
  5 s): steady-state p99 of `data_avalanche_replication_lag_seconds` at most 60 s; after a
  holder's loss, a change written after it lands and the lag is back under 60 s within 2 minutes.
- Credentials are never printed, searched for or recorded. A cloud run's credentials pass from the
  environment to the binary; a command that needs Oleksii's is run by him with `!`.
- The acceptance runs restart a test-infra database container only in the opt-in chaos run
  (Q-M12c-3), with `docker compose restart`; they never stop, remove or prune containers.
- Commit trailer, as every commit on the branch: the `Claude-Session:` line from the session.

## Review Focus

1. **A lag figure that measures nothing.** The gauge reads 0 while no change waits, and is absent
   while no worker runs. A run whose writer stalled (a failed statement loop) or whose pipeline
   isn't the holder would report a fine p99. `steady_lag_samples` and `steady_rate` are recorded
   beside the p99, and the run fails unless its lake matches the source afterwards; the failover
   run counts recovery only once a row written after the kill is in the lake.
2. **A cloud run that runs nothing.** `--exact` runs zero tests for a name that matches none, and
   a cloud job with missing credentials could skip silently. `the_cloud_e2e_list_names_scenarios_of_the_suite`
   pins the names; a missing bucket variable panics in `Store::bucket` (`required_env`), and
   `ci.yml` fails a same-repository PR whose GCS secret is missing, as the integration job does.
3. **A halt that never goes.** DA-SRC-37 keeps a halt the start used to clear; the halt must still
   go when the table leaves the lake or a job copies it, or a dropped Delta table would leave a
   halt forever. `a_refused_repositions_halt_stays_while_the_lake_holds_the_table` drops the table
   and checks the halt goes.
4. **A fold where names don't fold.** DA-SNAP-23's respelling must apply only where
   `column_names_fold` holds: on PostgreSQL `doc` and `DOC` are two columns and the pair halts.
   `where_names_fold_a_sink_column_matching_by_case_alone_is_the_column` checks both.
5. **The chaos runs' own connections.** A source restart drops the harness's admin connection too,
   and the restart returns before the server takes connections; the run must reconnect, not panic
   in its final comparison. Found in planning (§Measured in planning); `a_source_restart_loses_nothing`
   waits for the server and reconnects.

---

## Decisions and rulings

**Rulings asked of Oleksii (2026-11-11):**

- **Q-M12c-1, the four open gaps: fix all four.** DA-SRC-39, 38, 37 and DA-SNAP-23, one task each
  before the acceptance work.
- **Q-M12c-2, the load harness: a test target, with `xtask` wrappers.** A gated `--test load` that
  reuses the E2E harness, built in release mode by `cargo xtask bench|chaos|soak`, which writes a
  report.
- **Q-M12c-3, the source restart: `docker compose restart`.** The chaos run restarts the source's
  container, in that opt-in run only.
- **Q-M12c-4, the cloud E2E: a subset.** Four scenarios (a snapshot then the stream, a crash
  replay, a node takeover, a halted table copied again) × PostgreSQL and MySQL on each cloud:
  weekly and on dispatch in `cloud.yml`, and on every PR for GCS in `ci.yml`.

**Rulings made in planning (D-M12c):**

- **D-M12c.1, DA-OPS-14: closed by measurement, no cache.** `BINLOG_GTID_POS(file, pos)` costs
  0–1 ms on MariaDB 11.8 (`binlog_gtid_index` on, the default since 11.4) at any depth, and about
  1.4 ms per MB of small transactions on 10.11, which has no index: about 1.4 s at a full 1 GiB
  file, once per stream open (§Measured in planning).
- **D-M12c.2, the alternation window: 10 minutes** (`SOURCE_CHANGE_WINDOW`). An alternation's
  runs follow each other within the restart backoff (a minute at most). Failovers further apart
  start the count again, though no checkpoint moved between them (an idle source). A constant,
  as `SOURCE_CHANGES_IN_A_ROW` is.
- **D-M12c.3, the code of a promoted-reconnect alternation: from the message.** The PostgreSQL
  reconnect to a promoted server of the same system asks its rerun with a message prefixed
  `WAL_SOURCE_CHANGED: ` (`reconnect_check`), as its other source-change messages are; the
  controller's `alternating` already takes the prefix. The controller doesn't guess a code from the
  source's kind.
- **D-M12c.4, DA-SRC-37: the halt is told by its detail, and kept while the sink holds the table.**
  `REPOSITION_REFUSED` ("the source changed, and its reposition fails") begins the detail of the
  halt Task 6 of M12d records. The start keeps such a halt of a table nobody tracks while
  `sink.table_state(table).exists`, one call per such halt; a job's copy, or the table gone from
  the lake, ends it. No new halt field: `HaltReason` keeps its shape (Global Constraints).
- **D-M12c.5, DA-SNAP-23: the cached path's rule.** At a first-met baseline where names fold, the
  session writes the sink's spelling, the key's too, as a cached schema keeps it (D-M12d.3); a
  column the sink lacks is added under the source's spelling.
- **D-M12c.6, the cloud subset: existing scenarios, by name.** `smoke::a_pipeline_runs_stops_on_sigterm_and_resumes`,
  `crashes::a_crash_between_a_commit_and_its_checkpoint_skips_the_replay`,
  `cluster::a_killed_nodes_pipelines_are_taken_over`, `jobs::a_job_clears_a_dropped_table_s_halt`,
  in `test-infra/cloud-e2e.txt`, which both workflows read; an xtask test fails when a name
  matches no scenario. No scenario of its own: the four already cover Q-M12c-4's list.
- **D-M12c.7, the load target shares the harness by path.** `tests/load/main.rs` includes
  `harness.rs`, `source.rs`, `store.rs`, `proxy.rs`, `forwarder.rs` and `nodes.rs` with
  `#[path]`. `Cluster` moves to `nodes.rs`, which holds no test, so the load binary doesn't run
  the cluster scenarios.
- **D-M12c.8, how the lag is measured.** D-V1.3 names the metric: the runs sample
  `data_avalanche_replication_lag_seconds` every second (the nearest-rank quantile). The failover
  run's recovery is the first second at which a row inserted just after the kill is in the lake
  and the new holder (a later epoch) reads a lag of at most 60 s.
- **D-M12c.9, the soak's leak check: `n1`, at most twice.** `n1` is never restarted by the churn
  (the leaves and kills take `n2` and `n3` in turn), so its resident memory across the hourly
  checks is the leak signal: the last reading at most twice the first.
- **D-M12c.10, the acceptance defaults.** `Options::defaults` drops the suite's 6 s lease and 1 s
  flush for the product's (15 s / 5 s, 30 s): D-V1.3's targets assume them.
- **D-M12c.11, Azurite stands in for Azure locally.** `DATA_AVALANCHE_E2E_STORE=azurite` runs the
  binary's Azure path (`AZURE_STORAGE_USE_EMULATOR`) against test-infra's Azurite, so the store
  switch is checked without a cloud account.
- **D-M12c.12, `1.0.0`.** The workspace version and the chart's `version` and `appVersion` become
  `1.0.0` (`release.yml` refuses a tag that disagrees with them). The tag `v1.0.0` is pushed by
  Oleksii on `main` after the merge (CLAUDE.md: `main` is his).
- **D-M12c.13, DA-SRC-39's residual.** A worker task aborted outright (its lease lost) drops its
  session without a close. Its node no longer holds the lease; the new holder's bounded retry, or
  its end of the stale walsender (DA-SRC-28), covers the slot. Recorded in the research, not a gap.

## Spec patches (approved 2026-11-11; applied in Task 7)

**P-1 — `Data-Flow.md`, Phase 2b's dump checks and the Source Disconnect block** (DA-SRC-38). Two
places say "three Phase 0 reruns in a row for a server change, none with a durable checkpoint past
its start". Both gain the window; the second gains the code:

```diff
-│         recovery. Three Phase 0 reruns in a row for a server change, none with a durable
-│         checkpoint past its start, halt the pipeline with the change's code, naming the
-│         alternation: the address reaches more than one server (DA-SRC-34)
+│         recovery. Three Phase 0 reruns in a row for a server change, each within 10 minutes
+│         of the one before and none with a durable checkpoint past its start, halt the pipeline
+│         with the change's code, naming the alternation: the address reaches more than one
+│         server (DA-SRC-34, DA-SRC-38)
```

```diff
-    ─ Any source: three Phase 0 reruns in a row for a server change, none with a durable
-      checkpoint past its start, halt the pipeline, naming the alternation: the address reaches
-      more than one server (DA-SRC-34)
+    ─ Any source: three Phase 0 reruns in a row for a server change, each within 10 minutes of
+      the one before and none with a durable checkpoint past its start, halt the pipeline with
+      the change's code, naming the alternation: the address reaches more than one server
+      (DA-SRC-34, DA-SRC-38)
```

**P-2 — `Data-Flow.md`, Phase 4's reposition** (DA-SRC-37):

```diff
 A recovery from a changed source commits one per table the sink holds, and per table the pipeline
 resolves or has a cached schema for, under the key the table was made with; a table whose
-reposition the sink refuses for good halts `SINK_REJECTED` (DA-SRC-32). Each is fenced like any commit, …
+reposition the sink refuses for good halts `SINK_REJECTED` (DA-SRC-32), and stays halted while the
+sink holds it, tracked or not: no later start repositions it, so only a snapshot job's copy of it,
+or the table gone from the sink, ends the halt (DA-SRC-37). Each is fenced like any commit, …
```

**P-3 — `Schema_Change_Detection_and_Evolution.md` §7.2, the case-only row** (DA-SNAP-23):

```diff
-| MySQL RENAME/CHANGE COLUMN that changes only the name's case (MySQL compares column names without case) | **Ignorable** | none | none | the column keeps its cached spelling; a note (DA-SRC-25) |
+| MySQL RENAME/CHANGE COLUMN that changes only the name's case (MySQL compares column names without case) | **Ignorable** | none | none | the column keeps its cached spelling; a note (DA-SRC-25). A baseline against a target table the pipeline meets with no cached schema (a lost state, a table first tracked at a resume) keeps the target's spelling (DA-SNAP-23) |
```

## Measured in planning (2026-11-11)

**DA-OPS-14** (D-M12c.1): `SELECT BINLOG_GTID_POS(<file>, <pos>)` timed in the server
(`NOW(6)` before and after), at the end of a binlog file filled by a procedure committing
single-row inserts (the most events per byte):

| Server | Index | File at | Transactions | Lookup |
|---|---|---|---|---|
| MariaDB 11.8.9 (`mariadb-118`) | `binlog_gtid_index` on | 24 / 49 / 74 MB | 100k / 200k / 300k | 0–1 ms |
| MariaDB 11.8.9 | on | 272 MB of 8 MB row events | 34 | 0–1 ms |
| MariaDB 10.11.19 (`mariadb-1011`) | none | 24 / 49 / 73 MB | 100k / 200k / 300k | 33–50 / 79–101 / 98–102 ms |

On 10.11 the cost grows with the file, about 1.4 ms per MB: a full 1 GiB file (`max_binlog_size`)
of tiny transactions is about 1.4 s, once per stream open. No cache. The probe left a database
`m12c_probe` (table `s`, procedure `fill`) on both servers, dropped on 2026-11-11 at Oleksii's
word, and binlog growth of about 350 MB on `mariadb-118`.

**The acceptance runs, short** (debug build, PostgreSQL 18, RustFS; each checks the run's logic,
not D-V1.3, which the release-mode runs of Task 8 measure):

| Run | Knobs | Result |
|---|---|---|
| `bench::steady_state_lag_at_the_defaults` | 10 000 keys × 4 tables, 1000 changes/s, 2 min | p50 16.4 s, p99 30.7 s, max 30.8 s; 121 samples; 996 changes/s |
| `bench::merge_cost_under_random_key_churn` | 20 000 keys, 2 min | 4 merges, 0.46 s each; each wrote about the whole table (276 kB of 256 kB) |
| `bench::a_failover_recovers_within_two_minutes` | 10 000 keys × 2 tables | landed and recovered after 53.5 s |
| `chaos::a_storage_outage_shorter_than_the_lease_loses_nothing` | 5 s outage | the same epoch |
| `chaos::a_storage_outage_longer_than_the_lease_loses_nothing` | 45 s outage | one epoch more; running again 1.6 s after the store answered |
| `chaos::a_source_restart_loses_nothing` | `postgres-18` restarted | 2.8 s to take connections; 1 statement lost to the writer, none to the lake |
| `chaos::a_partition_under_load_loses_nothing` | the holder cut off 60 s | the same holder and epoch |
| `soak::a_day_of_churn_loses_nothing` | 12 min, churn every 2 min, checks every 6 | 5 churn steps (each kind once), 136 337 changes, both checks matched; `n1` 105 → 86 MB |

**The cloud subset on Azurite** (D-M12c.11, PostgreSQL 18): the four scenarios passed in 101 s.
`smoke::` alone passed on RustFS and Azurite after the store switch; the cluster scenarios (7)
passed after `Cluster` moved to `nodes.rs`.

**Found while writing it, and fixed in the code below:**
- The source restart: `docker compose restart` returns before PostgreSQL takes connections, so the
  run's reconnect failed, and the two runs after it started against a recovering server. The run
  now waits for a connection, up to 2 minutes.
- PostgreSQL's `ON CONFLICT` refuses a key twice in one statement: the writer's batches are
  deduplicated.
- MariaDB has no `VALUES … AS new` row alias, which MySQL 8.4 asks for instead of `VALUES()`: the
  writer's upsert takes each server's form.
- The load target's `pub` helpers made the shared harness's types reachable from its root, which
  `missing_debug_implementations` flags: they are `pub(crate)`.
- A session's test first sent a drain before the session read the failing transaction, so the
  session ended clean: the test now runs the session to its own end.
- The prefix of D-M12c.3 put `WAL_SOURCE_CHANGED` in the log of the slot-sync failover scenario
  (M12d's DA-SRC-33), which checked the code's absence to mean "no halt": it now checks for no
  halt with that code (Task 2).
- The fake source didn't read a chunk under folded names (a limit M12d's review recorded): Task 4's
  worker test needs it, so `read_chunk` folds where `fold_column_names` is on.

## File Structure

| File | Task | What changes |
|---|---|---|
| `crates/data-avalanche-pipeline/src/session.rs` | 1 | `Session::run` and `finish` close the stream on their error exits |
| `crates/data-avalanche-testkit/src/fake_source.rs` | 1, 2, 4 | `FakeSource::closes`; the promoted reconnect's prefix; `read_chunk` folds names (`respell`) |
| `crates/data-avalanche-cluster/src/controller.rs`, `lib.rs` | 2 | `SOURCE_CHANGE_WINDOW`; the count starts again after it |
| `crates/data-avalanche-source-pg/src/slot.rs` | 2 | `reconnect_check`'s promoted message prefixed `WAL_SOURCE_CHANGED: ` |
| `crates/data-avalanche/tests/e2e/failover.rs` | 2 | the slot-sync scenario checks for no halt, not for the code's absence |
| `crates/data-avalanche-pipeline/src/worker.rs` | 3, 4 | `REPOSITION_REFUSED`, `refused_reposition`, the kept halt; the first-met baseline's schema |
| `crates/data-avalanche-pipeline/src/schema_change.rs` | 4 | `Baseline`; `missing_columns(sink, source, fold)` |
| `crates/data-avalanche/tests/e2e/harness.rs`, `mysql.rs` | 4 | `E2e::wipe_state`; `a_lost_state_meets_a_recased_column_as_the_same_column` |
| `crates/data-avalanche/tests/e2e/store.rs` (new), `harness.rs`, `main.rs` | 5 | `Store`; the harness reads and writes through it |
| `test-infra/cloud-e2e.txt` (new), `xtask/tests/cloud_e2e.rs` (new) | 5 | the cloud subset, and its guard |
| `.github/workflows/ci.yml`, `cloud.yml` | 5 | `e2e-gcs` per PR; `e2e` weekly on S3, GCS and Azure |
| `crates/data-avalanche/tests/e2e/nodes.rs` (new), `cluster.rs` | 6 | `Cluster` moved, with `Cluster::with` |
| `crates/data-avalanche/tests/e2e/harness.rs`, `source.rs` | 6 | `Options::defaults`, `Proc::pid`; `Db::try_connect`, `Source::service` |
| `crates/data-avalanche/tests/load/{main,workload,report,bench,chaos,soak}.rs` (new) | 6 | the acceptance runs |
| `crates/data-avalanche/Cargo.toml` | 6 | the `load` test target |
| `xtask/src/acceptance.rs` (new), `lib.rs`, `main.rs`, `Cargo.toml` | 6 | `cargo xtask bench\|chaos\|soak` |
| `doc/specs/Data-Flow.md`, `Schema_Change_Detection_and_Evolution.md` | 7 | P-1, P-2, P-3 |
| `docs/src/content/docs/runbooks/halted-table.md`, `position-lost.mdx` | 7 | the kept halt, the case-only column, the window |
| `doc/research/{Delivery_Guarantee…,MySQL_DDL_Classifier,Postgres_Slot…}.md` | 7 | the fixes' notes |
| `test-infra/README.md`, `doc/v1.0/README.md` | 7 | the store switch, the acceptance runs, the gates |
| `doc/analysis/CDC_Design_Gap_Analysis_v1.md` | 7, 8 | §63 the gaps closed; §64 the acceptance |
| `doc/v1.0/v1-acceptance.md` (new) | 8 | the acceptance report |
| `docs/src/content/docs/deployment/observability.md`, `configuration/sink.md` | 8 | the measured lag and MERGE cost |
| `doc/v1.0/00-roadmap.md` | 8, 9 | §8 checked; M12's status |
| `Cargo.toml`, `Cargo.lock`, `deploy/helm/data-avalanche/Chart.yaml`, `README.md` | 9 | `1.0.0` |

## Tasks

### Task 1: A session closes its stream on every exit (`pipeline`, `testkit`) — DA-SRC-39

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/session.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`

**Interfaces:**
- Produces: `FakeSource::closes(&self) -> usize`, the streams closed so far.

- [ ] **Step 1: Write the failing test.** `session.rs` tests:
  `a_session_that_fails_closes_its_stream`. The sink refuses the first commit of `a` with
  `SinkError::NoTable` (the pipeline stops); the session runs to its own end, within 10 s, ends
  `Err(WorkerError::Sink(SinkError::NoTable { .. }))`, and `w.source.closes() == 1`. The fake's
  `closes` counter comes with it (the diff below).

- [ ] **Step 2: Run it to see it fail**

Run: `cargo test -p data-avalanche-pipeline --lib a_session_that_fails_closes_its_stream`
Expected: FAIL: `the stream is closed, not dropped`, left `0`, right `1`.

- [ ] **Step 3: Close the stream on both error exits.** `run`'s `Err` arm and `finish`'s
  `WriterEvent::Failed` arm call `self.stream.close()` (its result ignored: the session already
  fails) before they return. The PostgreSQL close is bounded by the command timeout.

```diff
diff --git a/crates/data-avalanche-pipeline/src/session.rs b/crates/data-avalanche-pipeline/src/session.rs
index cd8d62f..0527866 100644
--- a/crates/data-avalanche-pipeline/src/session.rs
+++ b/crates/data-avalanche-pipeline/src/session.rs
@@ -394,7 +394,9 @@ impl<'c> Session<'c> {
     }
 
     /// Runs the session to its end. Flushes already handed to the writer are made durable before
-    /// it returns, unless it fails.
+    /// it returns, unless it fails. The stream is closed either way (DA-SRC-39): a dropped
+    /// PostgreSQL client's worker takes the slot again moments later, and the next session's
+    /// stream would find it active.
     pub async fn run(mut self) -> Result<End, WorkerError> {
         let ended = self.event_loop().await;
         // The chunk reads still out are aborted: the next session reads again from the cursors.
@@ -406,6 +408,7 @@ impl<'c> Session<'c> {
             }
             Err(e) => {
                 self.writer.0.abort();
+                let _ = self.stream.close().await;
                 Err(e)
             }
         }
@@ -419,7 +422,10 @@ impl<'c> Session<'c> {
         drop(self.jobs);
         while let Some(ev) = self.events.recv().await {
             match ev {
-                WriterEvent::Failed(e) => return Err(e),
+                WriterEvent::Failed(e) => {
+                    let _ = self.stream.close().await;
+                    return Err(e);
+                }
                 WriterEvent::Halted(h) => self.core.update(|st| {
                     st.snapshots.remove(&h.table);
                     if !h.copy {
@@ -1954,6 +1960,30 @@ mod tests {
         assert_eq!(w.ids("a"), [1]);
     }
 
+    /// DA-SRC-39: a session that ends on a worker error closes its stream too. A dropped
+    /// PostgreSQL client's worker takes the slot again moments later, so the next session's
+    /// stream would find it active.
+    #[tokio::test]
+    async fn a_session_that_fails_closes_its_stream() {
+        let w = World::new(&["a"]);
+        w.started(&["a"]).await;
+        w.sink
+            .fail(SinkOp::Commit, Some(&t("a")), 1, || SinkError::NoTable {
+                table: t("a"),
+            });
+        let (mut core, _control, _status) = w.core(&flush_each(), &["a"]);
+        let s = Session::open(&mut core).await.unwrap().unwrap();
+        w.source.commit(FakeTxn::new().insert(&t("a"), row(1, "x")));
+        let end = tokio::time::timeout(Duration::from_secs(10), s.run())
+            .await
+            .expect("the session ends");
+        assert!(
+            matches!(end, Err(WorkerError::Sink(SinkError::NoTable { .. }))),
+            "a missing sink table stops the pipeline: {end:?}"
+        );
+        assert_eq!(w.source.closes(), 1, "the stream is closed, not dropped");
+    }
+
     #[tokio::test]
     async fn re_query_reads_the_values_the_stream_left_unchanged() {
         for (strategy, want) in [("re_query", Some("big")), ("carry_forward", None)] {
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index af933d5..5a1fd77 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -271,6 +271,8 @@ struct Db {
     readers_ended: usize,
     reconfigures: usize,
     teardowns: usize,
+    /// Streams closed (`close`) so far.
+    closes: usize,
     fetches: BTreeMap<TableRef, usize>,
     prepares: Vec<Option<Checkpoint>>,
     /// What each `prepare_marked` was told the lake's highest marker is.
@@ -336,6 +338,7 @@ impl FakeSource {
             readers_ended: 0,
             reconfigures: 0,
             teardowns: 0,
+            closes: 0,
             fetches: BTreeMap::new(),
             prepares: vec![],
             marks: vec![],
@@ -695,6 +698,11 @@ impl FakeSource {
         self.db().teardowns
     }
 
+    /// The streams closed so far (`ChangeStream::close`): a stream dropped unclosed isn't one.
+    pub fn closes(&self) -> usize {
+        self.db().closes
+    }
+
     /// `fetch_rows` calls for `table` so far, failed ones included.
     pub fn fetches(&self, table: &TableRef) -> usize {
         self.db().fetches.get(table).copied().unwrap_or(0)
@@ -1514,6 +1522,7 @@ impl ChangeStream for FakeStream {
     }
 
     async fn close(self: Box<Self>) -> Result<(), SourceError> {
+        self.source.db().closes += 1;
         Ok(())
     }
 }
```

- [ ] **Step 4: Run it to see it pass, and the crate's tests**

Run: `cargo test -p data-avalanche-pipeline --lib`
Expected: PASS (270 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline/src/session.rs crates/data-avalanche-testkit/src/fake_source.rs
git commit -m "fix(pipeline): a session closes its stream on every exit, an error's too (DA-SRC-39)"
```

### Task 2: Server changes far apart start the count again (`cluster`, `source-pg`, `testkit`) — DA-SRC-38

**Files:**
- Modify: `crates/data-avalanche-cluster/src/controller.rs`, `crates/data-avalanche-cluster/src/lib.rs`
- Modify: `crates/data-avalanche-source-pg/src/slot.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`
- Modify: `crates/data-avalanche/tests/e2e/failover.rs`

**Interfaces:**
- Produces: `data_avalanche_cluster::SOURCE_CHANGE_WINDOW: Duration` (600 s), re-exported.

- [ ] **Step 1: Write the failing tests.**
  - `controller.rs`: `server_changes_far_apart_dont_halt`. Two runs end `SourceChanged` at their
    stream's open; the third streams; `SOURCE_CHANGE_WINDOW` and a minute later its stream ends
    `SourceChanged` with no checkpoint moved. A fourth worker runs, the pipeline stays `Running`,
    and a minute later no fifth was built. And `an_alternation_halts_with_the_code_its_change_names`:
    `alternating` of a `WAL_SOURCE_CHANGED: …` message carries the code and the message's rest.
  - `slot.rs`: `a_reconnect_to_another_server_halts_or_asks_a_rerun`'s promoted case expects the
    message to start `WAL_SOURCE_CHANGED: the checkpoint 0/20 was read on PostgreSQL system 7 on
    timeline 1, …`.
  - `failover.rs`: `a_promotion_the_failover_slot_followed_continues_the_stream` checks that no
    halt carries `WAL_SOURCE_CHANGED` (`halted_with`), not that the log never names it: the
    reconnect's rerun now does.
  Add the constant with no logic first, so the controller test fails on behaviour.

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-cluster --lib -- server_changes_far_apart_dont_halt an_alternation_halts_with_the_code` and `cargo test -p data-avalanche-source-pg --lib slot::`
Expected: `server_changes_far_apart_dont_halt` FAILs (the third change halts the pipeline);
`a_reconnect_to_another_server_halts_or_asks_a_rerun` FAILs (no prefix).
`an_alternation_halts_with_the_code_its_change_names` passes already: it pins `alternating`, which
needs no change once the message carries the code (D-M12c.3).

- [ ] **Step 3: The window, and the prefix.** The controller keeps `changed_at`, when the last
  counted change ended; a `SourceChanged` end more than `SOURCE_CHANGE_WINDOW` after it sets the
  count to 1, as one after progress does. `reconnect_check` prefixes the promoted reconnect's
  message; the fake source's message, likewise.

```diff
diff --git a/crates/data-avalanche-cluster/src/controller.rs b/crates/data-avalanche-cluster/src/controller.rs
index 7949604..8078281 100644
--- a/crates/data-avalanche-cluster/src/controller.rs
+++ b/crates/data-avalanche-cluster/src/controller.rs
@@ -140,6 +140,11 @@ pub struct ControllerSettings {
 /// (DA-SRC-34) resets every table's marker at each run and streams nothing.
 pub const SOURCE_CHANGES_IN_A_ROW: u32 = 3;
 
+/// How long after the server change counted before another one still counts toward
+/// [`SOURCE_CHANGES_IN_A_ROW`]. An alternation reruns within the restart backoff (a minute at
+/// most); genuine failovers come further apart and start the count again (DA-SRC-38).
+pub const SOURCE_CHANGE_WINDOW: Duration = Duration::from_secs(600);
+
 /// The halt of a pipeline whose runs keep finding another server: the code of the last change
 /// (`WAL_SOURCE_CHANGED`, `BINLOG_SOURCE_CHANGED`), and the alternation named.
 fn alternating(why: &str, runs: u32) -> Failure {
@@ -814,8 +819,10 @@ impl PipelineController {
         let mut retry: Option<(Instant, String)> = None;
         let mut failures: u32 = 0;
         // The runs in a row that ended finding another server, none with a durable checkpoint
-        // past the one it started from (DA-SRC-34).
+        // past the one it started from (DA-SRC-34), each within SOURCE_CHANGE_WINDOW of the one
+        // before (DA-SRC-38), and when the last ended.
         let mut changed: u32 = 0;
+        let mut changed_at: Option<Instant> = None;
         // The lag's start across this term's workers: what one read and didn't make durable still
         // counts while none runs and while the next one reads it again (I2).
         let mut pending = Lag::default();
@@ -967,11 +974,19 @@ impl PipelineController {
                             "the worker task ended abnormally: {e}"
                         )))
                     });
+                    let now = Instant::now();
+                    let apart =
+                        changed_at.is_some_and(|at| now.duration_since(at) > SOURCE_CHANGE_WINDOW);
                     changed = match &ended {
-                        Err(WorkerError::Source(SourceError::SourceChanged(_))) if progressed => 1,
+                        Err(WorkerError::Source(SourceError::SourceChanged(_)))
+                            if progressed || apart =>
+                        {
+                            1
+                        }
                         Err(WorkerError::Source(SourceError::SourceChanged(_))) => changed + 1,
                         _ => 0,
                     };
+                    changed_at = (changed > 0).then_some(now);
                     match ended {
                         Err(WorkerError::Source(SourceError::SourceChanged(why)))
                             if changed >= SOURCE_CHANGES_IN_A_ROW =>
@@ -2146,6 +2161,48 @@ mod tests {
         assert_eq!(f.builds(), 5);
     }
 
+    /// DA-SRC-38: server changes far apart are failovers, not an alternation. A change more than
+    /// `SOURCE_CHANGE_WINDOW` after the one counted before starts the count again, though no run
+    /// in between made a checkpoint durable (an idle source).
+    #[tokio::test(start_paused = true)]
+    async fn server_changes_far_apart_dont_halt() {
+        let store = ScriptedStore::new();
+        let docs = pipeline_docs();
+        let f = TestFactory::new(&["a"]);
+        f.source.fail(Method::OpenStream, changed());
+        f.source.fail(Method::OpenStream, changed());
+        let mut n = node("n1", &store, &docs, &f);
+        eventually("a third worker", || f.builds() == 3).await;
+        until(&mut n.status, "running", is(Actual::Running)).await;
+        tokio::time::sleep(SOURCE_CHANGE_WINDOW + Duration::from_secs(60)).await;
+        f.source.fail(Method::Next, changed());
+        eventually("a fourth worker", || f.builds() == 4).await;
+        until(&mut n.status, "running again", is(Actual::Running)).await;
+        tokio::time::sleep(Duration::from_secs(60)).await;
+        assert_eq!(n.status.borrow().actual, Some(Actual::Running));
+        assert_eq!(f.builds(), 4);
+    }
+
+    /// DA-SRC-38: an alternation whose changes name no code (a message without one) halts with
+    /// none, and says so; the PostgreSQL source prefixes every change it reports, a promoted
+    /// server's reconnect included (`reconnect_check`), so its alternation halts with the code.
+    #[test]
+    fn an_alternation_halts_with_the_code_its_change_names() {
+        let f = alternating(
+            "WAL_SOURCE_CHANGED: the checkpoint 0/20 was read on PostgreSQL system 7 on timeline 1",
+            3,
+        );
+        assert_eq!(f.code.as_deref(), Some("WAL_SOURCE_CHANGED"));
+        assert!(
+            f.error.starts_with("WAL_SOURCE_CHANGED: 3 runs in a row")
+                && f.error.ends_with(
+                    "The last: the checkpoint 0/20 was read on PostgreSQL system 7 on timeline 1"
+                ),
+            "{}",
+            f.error
+        );
+    }
+
     #[tokio::test(start_paused = true)]
     async fn a_restart_asked_runs_a_new_worker_under_the_same_lease() {
         let store = ScriptedStore::new();
diff --git a/crates/data-avalanche-cluster/src/lib.rs b/crates/data-avalanche-cluster/src/lib.rs
index 490b849..c0d9bc8 100644
--- a/crates/data-avalanche-cluster/src/lib.rs
+++ b/crates/data-avalanche-cluster/src/lib.rs
@@ -24,8 +24,8 @@ pub mod wire;
 pub use claim::{Claim, Claimed, release};
 pub use controller::{
     Actual, Configured, ControllerDeps, ControllerSettings, ControllerStatus, CurrentPipeline,
-    Parts, PipelineController, RemovedPipelines, ResolveError, Resolved, SOURCE_CHANGES_IN_A_ROW,
-    Target, Update, WorkerFactory,
+    Parts, PipelineController, RemovedPipelines, ResolveError, Resolved, SOURCE_CHANGE_WINDOW,
+    SOURCE_CHANGES_IN_A_ROW, Target, Update, WorkerFactory,
 };
 pub use coordinator::{
     Checked, ConfigCoordinator, CoordinatorDeps, DocsFn, PUBLISH_ATTEMPTS, ReloadError, Reloaded,
diff --git a/crates/data-avalanche-source-pg/src/slot.rs b/crates/data-avalanche-source-pg/src/slot.rs
index 1af1a00..a70453d 100644
--- a/crates/data-avalanche-source-pg/src/slot.rs
+++ b/crates/data-avalanche-source-pg/src/slot.rs
@@ -388,8 +388,8 @@ pub fn reconnect_check(
         && timeline > was_on
     {
         return Err(SourceError::SourceChanged(format!(
-            "the checkpoint {} was read on {was}, and the stream's reconnect reaches {now}, a \
-             promoted server of the same system: the pipeline starts again, and its start decides \
+            "WAL_SOURCE_CHANGED: the checkpoint {} was read on {was}, and the stream's reconnect \
+             reaches {now}, a promoted server of the same system: the pipeline starts again, and its start decides \
              whether the stream continues there (a failover slot synchronized up to the \
              checkpoint) or halts as source_change_action says",
             lost.position.untagged()
@@ -1247,9 +1247,9 @@ mod tests {
             match reconnect_check(&promoted, &now, action) {
                 Err(SourceError::SourceChanged(m)) => assert!(
                     m.starts_with(
-                        "the checkpoint 0/20 was read on PostgreSQL system 7 on timeline 1, and \
-                         the stream's reconnect reaches PostgreSQL system 7 on timeline 2, a \
-                         promoted server"
+                        "WAL_SOURCE_CHANGED: the checkpoint 0/20 was read on PostgreSQL system 7 \
+                         on timeline 1, and the stream's reconnect reaches PostgreSQL system 7 on \
+                         timeline 2, a promoted server"
                     ) && m.contains("its start decides"),
                     "{m}"
                 ),
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index 5a1fd77..0888f7b 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -761,7 +761,7 @@ impl Db {
             && u > t
         {
             return Err(SourceError::SourceChanged(format!(
-                "the checkpoint {} was read on {was}, and the stream's reconnect reaches {now}, a promoted server of the same system",
+                "WAL_SOURCE_CHANGED: the checkpoint {} was read on {was}, and the stream's reconnect reaches {now}, a promoted server of the same system",
                 at.position
             )));
         }
diff --git a/crates/data-avalanche/tests/e2e/failover.rs b/crates/data-avalanche/tests/e2e/failover.rs
index 2defe72..cce4668 100644
--- a/crates/data-avalanche/tests/e2e/failover.rs
+++ b/crates/data-avalanche/tests/e2e/failover.rs
@@ -390,7 +390,8 @@ async fn a_promotion_the_failover_slot_followed_continues_the_stream() {
         log.contains("so the stream continues on the new timeline"),
         "{log}"
     );
-    assert!(!log.contains("WAL_SOURCE_CHANGED"), "{log}");
+    // The reconnect's rerun names the change (D-M12c.3), but nothing halts on it.
+    assert!(!halted_with(&log, "WAL_SOURCE_CHANGED"), "{log}");
     assert!(p.terminate().await.success());
     standby
         .exec(&format!(
```

- [ ] **Step 4: Run them to see them pass**

Run: `cargo test -p data-avalanche-cluster --lib` and `cargo test -p data-avalanche-source-pg --lib` and `cargo test -p data-avalanche-pipeline --lib`
Expected: PASS (cluster 174, source-pg's `slot::` 14, pipeline 270).
Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 DATA_AVALANCHE_PG_REPLICA=55420:55421 DATA_AVALANCHE_PG_SYNC_REPLICA=55422:55423 cargo test -p data-avalanche --features e2e-hooks --test e2e failover:: -- --test-threads=1`,
the replica pairs freshly recreated (`docker compose -f test-infra/compose.yaml --profile replica up -d --wait --force-recreate postgres-primary postgres-replica postgres-sync-primary postgres-sync-replica`).
Expected: PASS (2).

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-cluster crates/data-avalanche-source-pg/src/slot.rs crates/data-avalanche-testkit/src/fake_source.rs crates/data-avalanche/tests/e2e/failover.rs
git commit -m "fix(cluster): server changes far apart start the alternation count again, and every alternation halts with a code (DA-SRC-38)"
```

### Task 3: A refused reposition's halt stays while the lake holds the table (`pipeline`) — DA-SRC-37

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/worker.rs`

**Interfaces:**
- Produces: `REPOSITION_REFUSED: &str` and `refused_reposition(&HaltReason) -> bool`, private to
  `worker.rs`.

- [ ] **Step 1: Write the failing test.** `worker.rs` tests, beside
  `a_refused_reposition_halts_that_table_and_the_start_goes_on`:
  `a_refused_repositions_halt_stays_while_the_lake_holds_the_table`. A failover whose reposition
  of the untracked `x` the sink refuses; a second run (a plain resume) still shows `x` halted
  `SINK_REJECTED`, and `halts.json` still holds it; once `w.sink.drop_table(&t("x"))`, a third run
  shows no halt of `x` and `halts.json` holds none.

- [ ] **Step 2: Run it to see it fail**

Run: `cargo test -p data-avalanche-pipeline --lib a_refused_repositions_halt_stays`
Expected: FAIL at the second run's assertion: `x` isn't halted (the start cleared it).

- [ ] **Step 3: Keep the halt.** The reposition's halt detail starts with `REPOSITION_REFUSED`.
  The start's loop over the recorded halts of tables nobody tracks keeps one that
  `refused_reposition` matches while `sink.table_state(table)` (retried as the start's other sink
  reads are) says the table exists, and puts it in the status; any other such halt is cleared, as
  before.

```diff
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 105b133..2ab8fcc 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -355,6 +355,15 @@ enum Empty {
     Truncate,
 }
 
+/// How the halt of a table whose reposition the sink refused begins (DA-SRC-32): the start
+/// that clears the halts of tables nobody tracks keeps it while the lake holds the table
+/// (DA-SRC-37).
+const REPOSITION_REFUSED: &str = "the source changed, and its reposition fails";
+
+fn refused_reposition(reason: &HaltReason) -> bool {
+    matches!(reason, HaltReason::SinkRejected { detail } if detail.starts_with(REPOSITION_REFUSED))
+}
+
 /// Whether a cached schema's columns are the catalog's now: the same, in order, with the names
 /// compared without case when the source compares them so (`fold`, MySQL): a change of case alone
 /// is the same column, cached under its first spelling (DA-SRC-25).
@@ -1121,13 +1130,28 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
     // A copy's halt goes once its job carries it: the pipeline doesn't replicate the table, and
     // no job can clear the halt of a table gone. It is the halt of a table neither configured
     // nor cached (a table taken out of the config keeps its schema) nor read now.
-    for table in recorded_halts.keys() {
+    //
+    // A refused reposition's halt stays while the lake holds the table (DA-SRC-37): the
+    // checkpoint names the new server, so no later start repositions it, and tracked later the
+    // table would skip the new server's changes up to its old marker. A job's copy of it, or the
+    // table gone from the lake, ends it.
+    for (table, reason) in &recorded_halts {
         if !configured.contains(table)
             && !cached.contains_key(table)
             && !requests.contains(table)
             && !copies.contains_key(table)
             && !cleared.contains(table)
         {
+            if refused_reposition(reason)
+                && sink_retry(&tuning, || Ok(()), || sink.table_state(table))
+                    .await?
+                    .exists
+            {
+                core.update(|st| {
+                    st.halted.insert(table.clone(), reason.clone());
+                });
+                continue;
+            }
             core.may_write()?;
             core.docs.clear_halt(table).await?;
         }
@@ -1159,7 +1183,7 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
                 )),
                 Err(WorkerError::Sink(SinkError::Permanent { reason })) => {
                     let reason = HaltReason::SinkRejected {
-                        detail: format!("the source changed, and its reposition fails: {reason}"),
+                        detail: format!("{REPOSITION_REFUSED}: {reason}"),
                     };
                     core.may_write()?;
                     core.docs
@@ -2860,6 +2884,72 @@ mod tests {
         assert_eq!(x, old, "x's marker stays: its reposition was refused");
     }
 
+    /// DA-SRC-37: the halt of a refused reposition outlives the next start while the lake holds
+    /// the table, though the pipeline tracks it neither then nor before: the checkpoint names the
+    /// new server, so no start repositions the table again, and tracked later it would skip the
+    /// new server's changes up to its old marker. Once the table leaves the lake, the halt goes.
+    #[tokio::test]
+    async fn a_refused_repositions_halt_stays_while_the_lake_holds_the_table() {
+        let w = World::new(&["a", "x"]);
+        lake_with_an_untracked_table(&w).await;
+        w.sink
+            .fail(SinkOp::Commit, Some(&t("x")), 1, || SinkError::Permanent {
+                reason: "refused".into(),
+            });
+        w.source.fail_over(
+            ServerIdentity::Postgres {
+                system_identifier: 7,
+                timeline: 2,
+            },
+            Lsn(0x100),
+        );
+        let (end, _) = run(&w, &mode("mode: never"), async |status| {
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        let (end, st) = run(&w, &mode("mode: never"), async |status| {
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert!(
+            matches!(
+                st.halted.get(&t("x")),
+                Some(HaltReason::SinkRejected { .. })
+            ),
+            "{:?}",
+            st.halted
+        );
+        assert!(
+            w.docs
+                .halts()
+                .await
+                .unwrap()
+                .tables
+                .iter()
+                .any(|h| h.table == t("x")),
+            "the halt stays recorded"
+        );
+        w.sink.drop_table(&t("x"));
+        let (end, st) = run(&w, &mode("mode: never"), async |status| {
+            streamed(&status, &w).await;
+        })
+        .await;
+        end.unwrap();
+        assert!(!st.halted.contains_key(&t("x")), "{:?}", st.halted);
+        assert!(
+            !w.docs
+                .halts()
+                .await
+                .unwrap()
+                .tables
+                .iter()
+                .any(|h| h.table == t("x")),
+            "the halt of a table the lake no longer holds goes"
+        );
+    }
+
     /// DA-SRC-32: a held table that records no key, and that the pipeline doesn't track either,
     /// has no key to reposition under: its marker stays, and the status says so.
     #[tokio::test]
```

- [ ] **Step 4: Run it to see it pass**

Run: `cargo test -p data-avalanche-pipeline --lib -- reposition`
Expected: PASS (5 tests; the crate's 271).

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline/src/worker.rs
git commit -m "fix(pipeline): a refused reposition's halt stays while the lake holds the table (DA-SRC-37)"
```

### Task 4: A first-met MySQL column that changed case alone (`pipeline`, `testkit`, E2E) — DA-SNAP-23

**Files:**
- Modify: `crates/data-avalanche-pipeline/src/schema_change.rs`, `crates/data-avalanche-pipeline/src/worker.rs`
- Modify: `crates/data-avalanche-testkit/src/fake_source.rs`
- Modify: `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/mysql.rs`

**Interfaces:**
- Produces: `pub(crate) struct Baseline { adds: Vec<SchemaChange>, schema: TableSchema }` and
  `missing_columns(sink: &Schema, source: &TableSchema, fold: bool) -> Result<Baseline, HaltReason>`
  in `schema_change.rs`; `E2e::wipe_state(&self)`.

- [ ] **Step 1: Write the failing tests.**
  - `worker.rs`: `where_names_fold_a_sink_column_matching_by_case_alone_is_the_column`: against a
    sink table of `(id, doc)`, the source's `(ID, DOC, note)` keyed on `ID` baselines with
    `fold` to the schema `(id, doc, note)` keyed on `id`, adding `note` alone; without `fold` it
    halts. And `a_first_met_table_whose_names_changed_case_alone_is_baselined_where_names_fold`: a
    table first tracked at a resume (`b`, `(ID, Doc)`) whose sink table a copy made as `(id, doc)`,
    with `fold_column_names`, runs with no halt, lands its row, and caches `(id, doc)`.
  - The existing `missing_columns` tests pass `false` and read `.adds`.
  - `mysql.rs`: `a_lost_state_meets_a_recased_column_as_the_same_column`: a snapshot, then
    `RENAME COLUMN doc TO DOC`, a row, the pipeline's state wiped (`E2e::wipe_state`); the next
    start copies the table again with no halt, a streamed row lands, and the lake holds every
    row under `doc`.
  The signature changes first, with `fold` unused, so the tests fail on behaviour.

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-pipeline --lib -- missing first_met where_names_fold case_alone non_null baselined`
Expected: the two new tests FAIL (a `SchemaMismatch` halt).
Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_E2E_SOURCE=mysql cargo test -p data-avalanche --features e2e-hooks --test e2e a_lost_state_meets_a_recased_column -- --test-threads=1`
Expected: FAIL: `timed out waiting for items's snapshot` (the table halted).

- [ ] **Step 3: Fold where the source folds.** In `missing_columns`, where `fold` holds, a sink
  column that differs from a source column by case alone is noted (`respelled`) instead of
  halting; the returned schema names those columns, and the identifier, as the sink spells them,
  and keeps the source id. The worker's first-met arm passes `source.column_names_fold()` and
  takes the returned schema as the session's. The fake source's `read_chunk` folds the read's
  names where `fold_column_names` is on (`respell`), as MySQL's read does.

```diff
diff --git a/crates/data-avalanche-pipeline/src/schema_change.rs b/crates/data-avalanche-pipeline/src/schema_change.rs
index 8f61551..2110324 100644
--- a/crates/data-avalanche-pipeline/src/schema_change.rs
+++ b/crates/data-avalanche-pipeline/src/schema_change.rs
@@ -2,11 +2,13 @@
 //! the sink's evolve. A stream session runs them at a signal's position, with a flush in between.
 //! Phase 0 runs them to baseline a table whose stream position was lost.
 
+use std::collections::BTreeMap;
+
 use arrow_schema::Schema;
 use data_avalanche_core::{
-    DeltaClass, EvolveOutcome, HaltReason, INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, SchemaChange,
-    SchemaDelta, SchemaHint, Sink, SinkError, SourceConnector, SourceError, TableRef, TableSchema,
-    classify, differ_by_case_alone,
+    ColumnSchema, DeltaClass, EvolveOutcome, HaltReason, INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS,
+    SchemaChange, SchemaDelta, SchemaHint, Sink, SinkError, SourceConnector, SourceError, TableRef,
+    TableSchema, classify, differ_by_case_alone,
 };
 use tokio::sync::watch;
 use tracing::Instrument;
@@ -28,11 +30,23 @@ pub(crate) fn soft_dropped(sink: &Schema, source: &TableSchema) -> usize {
         .count()
 }
 
+/// What baselines a sink table the pipeline meets for the first time ([`missing_columns`]).
+#[derive(Debug)]
+pub(crate) struct Baseline {
+    /// The source's columns the sink table lacks, to add before the copy.
+    pub adds: Vec<SchemaChange>,
+    /// The schema the session writes the table with.
+    pub schema: TableSchema,
+}
+
 /// A table the sink holds while the pipeline has no schema cached for it (a wiped state,
 /// DA-SNAP-13, or a table first tracked at a resume, DA-SNAP-22): the source's columns the sink
 /// table lacks, by name, as the `AddColumn`s that baseline it before the copy. Names match
-/// without case, Unicode's, as Delta's do ([`differ_by_case_alone`]): a sink column that matches
-/// by case alone can't sit beside the source's, so it halts, naming both spellings. A column it
+/// without case, Unicode's, as Delta's do ([`differ_by_case_alone`]). Where the source compares
+/// names so too (`fold`, MySQL), a sink column that matches by case alone is the source's: the
+/// session writes it under the sink's spelling, in the key too, as a cached schema keeps it
+/// (DA-SNAP-23, DA-SRC-25). Elsewhere it can't sit beside the source's, so it halts, naming both
+/// spellings. A column it
 /// holds as another type still surfaces at the first commit (`SINK_REJECTED`); a nullable one the
 /// source lacks stays, soft-dropped. A key column it lacks can't be added: the table is keyed
 /// otherwise, and halts. So does a non-null column the source lacks: no row of the source's could
@@ -40,8 +54,11 @@ pub(crate) fn soft_dropped(sink: &Schema, source: &TableSchema) -> usize {
 pub(crate) fn missing_columns(
     sink: &Schema,
     source: &TableSchema,
-) -> Result<Vec<SchemaChange>, HaltReason> {
+    fold: bool,
+) -> Result<Baseline, HaltReason> {
     let mut adds = vec![];
+    // The source's names the sink spells otherwise, by case alone, where names fold.
+    let mut respelled: BTreeMap<&str, &str> = BTreeMap::new();
     for c in source.columns() {
         if sink.field_with_name(&c.name).is_ok() {
             continue;
@@ -51,6 +68,10 @@ pub(crate) fn missing_columns(
             .iter()
             .find(|f| differ_by_case_alone(f.name(), &c.name))
         {
+            if fold {
+                respelled.insert(&c.name, f.name());
+                continue;
+            }
             return Err(HaltReason::SchemaMismatch {
                 column: c.name.clone(),
                 detail: format!(
@@ -92,7 +113,30 @@ pub(crate) fn missing_columns(
                 .into(),
         });
     }
-    Ok(adds)
+    if respelled.is_empty() {
+        return Ok(Baseline {
+            adds,
+            schema: source.clone(),
+        });
+    }
+    let name = |n: &str| respelled.get(n).map_or(n, |s| *s).to_owned();
+    let columns = source
+        .columns()
+        .iter()
+        .map(|c| ColumnSchema {
+            name: name(&c.name),
+            ..c.clone()
+        })
+        .collect();
+    let identifier = source.identifier().iter().map(|k| name(k)).collect();
+    let schema = TableSchema::new(source.table().clone(), columns, identifier)?;
+    Ok(Baseline {
+        adds,
+        schema: match source.source_id() {
+            Some(id) => schema.with_source_id(id),
+            None => schema,
+        },
+    })
 }
 
 /// A change to apply: the table's new schema, and the changes the sink must evolve.
diff --git a/crates/data-avalanche-pipeline/src/worker.rs b/crates/data-avalanche-pipeline/src/worker.rs
index 2ab8fcc..64fab78 100644
--- a/crates/data-avalanche-pipeline/src/worker.rs
+++ b/crates/data-avalanche-pipeline/src/worker.rs
@@ -798,8 +798,15 @@ pub(crate) async fn start(core: &mut Core, lost: Option<Checkpoint>) -> Result<(
             // tracked at a resume whose sink table a job's copy made. Baselined against the
             // sink table's columns (DA-SNAP-13, DA-SNAP-22).
             None if state.exists => {
-                let adds = match state.schema.as_deref().map(|s| missing_columns(s, now)) {
-                    Some(Ok(adds)) => adds,
+                let adds = match state
+                    .schema
+                    .as_deref()
+                    .map(|s| missing_columns(s, now, fold))
+                {
+                    Some(Ok(base)) => {
+                        schema = base.schema;
+                        base.adds
+                    }
                     Some(Err(reason)) => {
                         halts.push((table.clone(), reason));
                         tracked.insert(table.clone());
@@ -2477,6 +2484,83 @@ mod tests {
         assert_eq!(w.text("b", "note"), [Some("n".to_owned())]);
     }
 
+    /// DA-SNAP-23: where the source compares names without case (MySQL), a sink column that
+    /// differs from the source's by case alone is that column: no add, no halt, and the session
+    /// writes it under the sink's spelling, in the key too, as the cached path does (DA-SRC-25).
+    #[test]
+    fn where_names_fold_a_sink_column_matching_by_case_alone_is_the_column() {
+        let sink = upsert_schema(&schema("a"));
+        let cased = TableSchema::new(
+            t("a"),
+            vec![
+                ColumnSchema::new("ID", V::Int64, "bigint").not_null(),
+                ColumnSchema::new("DOC", V::Text, "text"),
+                ColumnSchema::new("note", V::Text, "text"),
+            ],
+            vec!["ID".into()],
+        )
+        .unwrap();
+        let base = missing_columns(&sink, &cased, true).unwrap();
+        let names: Vec<_> = base
+            .schema
+            .columns()
+            .iter()
+            .map(|c| c.name.as_str())
+            .collect();
+        assert_eq!(names, ["id", "doc", "note"], "the sink's spelling");
+        assert_eq!(base.schema.identifier(), ["id"]);
+        assert!(
+            matches!(&base.adds[..], [SchemaChange::AddColumn(c)] if c.name == "note"),
+            "{:?}",
+            base.adds
+        );
+        assert!(
+            missing_columns(&sink, &cased, false).is_err(),
+            "PostgreSQL's names are case-sensitive: the pair halts"
+        );
+    }
+
+    /// DA-SNAP-23: a table first tracked at a resume whose MySQL columns changed case alone since
+    /// a job's copy made its sink table is baselined, not halted, and its rows land.
+    #[tokio::test]
+    async fn a_first_met_table_whose_names_changed_case_alone_is_baselined_where_names_fold() {
+        let w = World::new(&["a"]);
+        w.source.fold_column_names();
+        let p = mode("mode: initial");
+        run(&w, &p, async |status| streamed(&status, &w).await)
+            .await
+            .0
+            .unwrap();
+        w.sink
+            .ensure_table(&TableSpec::new(&schema("b"), vec![]).unwrap())
+            .await
+            .unwrap();
+        let cased = TableSchema::new(
+            t("b"),
+            vec![
+                ColumnSchema::new("ID", V::Int64, "bigint").not_null(),
+                ColumnSchema::new("Doc", V::Text, "text"),
+            ],
+            vec!["ID".into()],
+        )
+        .unwrap();
+        w.source.create_table(cased);
+        w.source.load(
+            &t("b"),
+            vec![Row(vec![
+                ("ID".into(), ColumnValue::Int64(1)),
+                ("Doc".into(), ColumnValue::Text("x".into())),
+            ])],
+        );
+        let (end, st) = run(&w, &p, async |status| streamed(&status, &w).await).await;
+        assert_eq!(end.unwrap(), Exit::Drained);
+        assert!(st.halted.is_empty(), "{:?}", st.halted);
+        assert_eq!(w.ids("b"), [1]);
+        let cached = w.docs.table_schema(&t("b")).await.unwrap().unwrap().doc;
+        let names: Vec<_> = cached.columns().iter().map(|c| c.name.as_str()).collect();
+        assert_eq!(names, ["id", "doc"], "the sink's spelling");
+    }
+
     /// DA-SNAP-22: a sink table holding a non-null column the source no longer has (a key it
     /// was made with) can't take the source's rows: it halts at the baseline, naming the column,
     /// rather than at the first MERGE.
@@ -2494,7 +2578,7 @@ mod tests {
         .unwrap();
         let sink = upsert_schema(&keyed);
         let Err(HaltReason::SchemaMismatch { column, detail }) =
-            missing_columns(&sink, &schema("a"))
+            missing_columns(&sink, &schema("a"), false)
         else {
             panic!("a non-null column the source lacks halts");
         };
@@ -2513,7 +2597,12 @@ mod tests {
             )
             .unwrap(),
         );
-        assert!(missing_columns(&sink, &schema("a")).unwrap().is_empty());
+        assert!(
+            missing_columns(&sink, &schema("a"), false)
+                .unwrap()
+                .adds
+                .is_empty()
+        );
     }
 
     /// A sink table without the source's key column is keyed otherwise: it halts.
@@ -2530,10 +2619,15 @@ mod tests {
         )
         .unwrap();
         assert!(matches!(
-            missing_columns(&sink, &rekeyed),
+            missing_columns(&sink, &rekeyed, false),
             Err(HaltReason::SchemaMismatch { column, .. }) if column == "uid"
         ));
-        assert!(missing_columns(&sink, &schema("a")).unwrap().is_empty());
+        assert!(
+            missing_columns(&sink, &schema("a"), false)
+                .unwrap()
+                .adds
+                .is_empty()
+        );
     }
 
     /// Delta's names are case-insensitive: a sink column that matches by case alone halts, on a
@@ -2553,25 +2647,26 @@ mod tests {
             .unwrap()
         };
         let Err(HaltReason::SchemaMismatch { column, detail }) =
-            missing_columns(&sink, &cased("id", "DOC"))
+            missing_columns(&sink, &cased("id", "DOC"), false)
         else {
             panic!("a case-only match halts");
         };
         assert_eq!(column, "DOC");
         assert!(detail.contains("doc") && detail.contains("DOC"), "{detail}");
         assert!(matches!(
-            missing_columns(&sink, &cased("ID", "doc")),
+            missing_columns(&sink, &cased("ID", "doc"), false),
             Err(HaltReason::SchemaMismatch { column, .. }) if column == "ID"
         ));
         assert!(
-            missing_columns(&sink, &cased("id", "doc"))
+            missing_columns(&sink, &cased("id", "doc"), false)
                 .unwrap()
+                .adds
                 .is_empty()
         );
         // Unicode case too, as delta-rs folds it (DA-SINK-12).
         let sink = upsert_schema(&cased("id", "ärger"));
         assert!(matches!(
-            missing_columns(&sink, &cased("id", "ÄRGER")),
+            missing_columns(&sink, &cased("id", "ÄRGER"), false),
             Err(HaltReason::SchemaMismatch { column, .. }) if column == "ÄRGER"
         ));
     }
diff --git a/crates/data-avalanche-testkit/src/fake_source.rs b/crates/data-avalanche-testkit/src/fake_source.rs
index 0888f7b..c8806b3 100644
--- a/crates/data-avalanche-testkit/src/fake_source.rs
+++ b/crates/data-avalanche-testkit/src/fake_source.rs
@@ -955,6 +955,22 @@ impl Db {
 }
 
 /// What `augment` and `rescan` compare: columns by name, with their types.
+/// `row` with each column named as `schema` spells it, matched without case.
+fn respell(row: Row, schema: &TableSchema) -> Row {
+    Row(row
+        .0
+        .into_iter()
+        .map(|(n, v)| {
+            let named = schema
+                .columns()
+                .iter()
+                .find(|c| c.name.to_lowercase() == n.to_lowercase())
+                .map_or(n, |c| c.name.clone());
+            (named, v)
+        })
+        .collect())
+}
+
 fn shape(
     columns: &[ColumnSchema],
     fold: bool,
@@ -1243,7 +1259,9 @@ impl SourceConnector for FakeSource {
                     reason: HaltReason::TableDropped,
                 });
             };
-            if shape(now.columns(), false) != shape(table.columns(), false) {
+            // Where names fold, the read asks the cached spelling, as MySQL's reads it.
+            let fold = db.fold;
+            if shape(now.columns(), fold) != shape(table.columns(), fold) {
                 return Err(SourceError::Transient(format!(
                     "{} changed under the snapshot",
                     table.table()
@@ -1255,7 +1273,10 @@ impl SourceConnector for FakeSource {
                 .into_iter()
                 .filter(|(k, _)| from.as_ref().is_none_or(|f| k > f))
                 .take(limit)
-                .map(|(_, r)| r)
+                .map(|(_, r)| match fold {
+                    true => respell(r, table),
+                    false => r,
+                })
                 .collect();
             (rows, db.snapshot())
         };
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index 49b6491..f26afca 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -822,6 +822,13 @@ pipelines:
         delete_prefix(stores::rustfs().as_ref(), &self.table_root(table)).await;
     }
 
+    /// Deletes the pipeline's own documents (`<sink.prefix>/.pipeline-state`), as a lost state
+    /// would: the next start meets the lake's tables with no schema cached (DA-SNAP-13).
+    pub async fn wipe_state(&self) {
+        let at = colocated_state(TEST_BUCKET, &self.lake_prefix());
+        delete_prefix(stores::rustfs().as_ref(), &at.root).await;
+    }
+
     /// The metrics' `source_db` label: PostgreSQL's database, MySQL's (the test's).
     pub fn source_db(&self) -> &str {
         match self.source {
diff --git a/crates/data-avalanche/tests/e2e/mysql.rs b/crates/data-avalanche/tests/e2e/mysql.rs
index c6c77f2..ddfc8de 100644
--- a/crates/data-avalanche/tests/e2e/mysql.rs
+++ b/crates/data-avalanche/tests/e2e/mysql.rs
@@ -219,6 +219,49 @@ async fn a_deleted_table_of_a_recased_key_is_made_again_under_the_cached_spellin
     e.cleanup().await;
 }
 
+/// DA-SNAP-23: a column whose name changed case alone while the pipeline's state was lost. The
+/// next start meets the lake's table with no schema cached, and MySQL's `DOC` is the sink's `doc`:
+/// the table is baselined under the sink's spelling and copied again, not halted.
+#[tokio::test]
+async fn a_lost_state_meets_a_recased_column_as_the_same_column() {
+    require_it!();
+    crate::mysql_only!();
+    let e = E2e::new(
+        "my_recased_lost",
+        ITEMS,
+        Options {
+            tables: ", snapshots: { mode: initial }",
+            ..Options::default()
+        },
+    )
+    .await;
+    e.exec("INSERT INTO items VALUES (1, 'a')").await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.completed("items", None).await;
+    assert!(p.terminate().await.success());
+    e.exec("ALTER TABLE items RENAME COLUMN doc TO DOC").await;
+    e.exec("INSERT INTO items VALUES (2, 'b')").await;
+    e.wipe_state().await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    e.completed("items", None).await;
+    e.streaming().await;
+    e.exec("INSERT INTO items VALUES (3, 'c')").await;
+    eventually("the rows land", Duration::from_secs(60), async || {
+        e.ids("items").await == [1, 2, 3]
+    })
+    .await;
+    assert!(e.halts().await.tables.is_empty(), "{:?}", e.halts().await);
+    assert_eq!(
+        e.rows("items", &["id", "doc"]).await,
+        [["1", "a"], ["2", "b"], ["3", "c"]],
+        "the sink's spelling holds every row"
+    );
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
 /// A failover behind the pipeline's address in file/position mode: the reconnect reaches another
 /// server, whose binlog files the checkpoint means nothing in. The dump's open checks the server
 /// on its own connection and halts the pipeline with `BINLOG_SOURCE_CHANGED`. Back on its server,
```

- [ ] **Step 4: Run them to see them pass**

Run: the commands of Step 2, the E2E on `DATA_AVALANCHE_MYSQL=mysql-84` and `mariadb-118`.
Expected: PASS (pipeline 273). The E2E fails again with the worker's call given `false`: it
discriminates.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-pipeline crates/data-avalanche-testkit/src/fake_source.rs crates/data-avalanche/tests/e2e/harness.rs crates/data-avalanche/tests/e2e/mysql.rs
git commit -m "fix(pipeline): on MySQL a first-met column that differs by case alone is the same column (DA-SNAP-23)"
```

### Task 5: The E2E suite on any store, and the cloud runs (E2E, CI) — Q-M12-2, Q-M12c-4

**Files:**
- Create: `crates/data-avalanche/tests/e2e/store.rs`, `test-infra/cloud-e2e.txt`, `xtask/tests/cloud_e2e.rs`
- Modify: `crates/data-avalanche/tests/e2e/harness.rs`, `crates/data-avalanche/tests/e2e/main.rs`
- Modify: `.github/workflows/ci.yml`, `.github/workflows/cloud.yml`

**Interfaces:**
- Produces: `Store` (`from_env`, `cloud`, `backend`, `bucket`, `objects`, `lake_options`, `env`)
  and `E2e::store: Store`, used by Task 6.

- [ ] **Step 1: Write the failing test.** `xtask/tests/cloud_e2e.rs`:
  `the_cloud_e2e_list_names_scenarios_of_the_suite`: `test-infra/cloud-e2e.txt` holds four
  `module::test` lines, each an `async fn <test>()` under `#[tokio::test]` in
  `crates/data-avalanche/tests/e2e/<module>.rs`.

- [ ] **Step 2: Run it to see it fail**

Run: `cargo test -p xtask --test cloud_e2e`
Expected: FAIL: the list file doesn't exist.

- [ ] **Step 3: The store switch, the list and the jobs.** `Store::from_env` reads
  `DATA_AVALANCHE_E2E_STORE`; the harness's config takes its `cloud` and `bucket`, the binary's
  environment its `env()` (RustFS's fixed keys, Azurite's emulator flag, or a cloud's credential
  variables passed through when set and not empty), and the readers its `objects()` and
  `lake_options()`. The list names D-M12c.6's four scenarios. `ci.yml` gains `e2e-gcs` (PostgreSQL
  and MySQL, the GCS credentials as the integration job writes them, a fork PR skipped with a
  notice), and `cloud.yml` gains `e2e` (S3, GCS and Azure × PostgreSQL and MySQL), both running
  `--exact $(cat test-infra/cloud-e2e.txt)`.

```diff
diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml
index 390a4dd..64ae6b4 100644
--- a/.github/workflows/ci.yml
+++ b/.github/workflows/ci.yml
@@ -179,6 +179,63 @@ jobs:
         if: always()
         run: docker compose -f test-infra/compose.yaml down -v
 
+  # The cloud E2E subset (Q-M12-2, Q-M12c-4) on the real GCS bucket, per change: the four scenarios
+  # test-infra/cloud-e2e.txt names (a snapshot then the stream, a crash replay, a node takeover, a
+  # halted table copied again), with globals.state and the lake in the bucket
+  # (DATA_AVALANCHE_E2E_STORE=gcs), on PostgreSQL 18 and on MySQL 8.4. S3 and Azure run it weekly
+  # (cloud.yml). The GCS credentials as the integration job's; a fork PR skips it with a notice.
+  e2e-gcs:
+    runs-on: ubuntu-latest
+    timeout-minutes: 60
+    needs: check
+    strategy:
+      fail-fast: false
+      matrix:
+        source: [postgres, mysql]
+    env:
+      DATA_AVALANCHE_IT: "1"
+      DATA_AVALANCHE_E2E_STORE: gcs
+      DATA_AVALANCHE_E2E_SOURCE: ${{ matrix.source }}
+      DATA_AVALANCHE_GCS_BUCKET: ${{ vars.DATA_AVALANCHE_GCS_BUCKET }}
+    steps:
+      - name: Free runner disk
+        run: sudo rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc /opt/hostedtoolcache/CodeQL
+      - uses: actions/checkout@v7
+      - uses: actions-rust-lang/setup-rust-toolchain@v2
+      - name: GCS credentials
+        env:
+          GCP_SA_KEY: ${{ secrets.GCP_SA_KEY }}
+          IS_FORK_PR: ${{ github.event.pull_request.head.repo.fork }}
+        run: |
+          if [ -z "$GCP_SA_KEY" ] || [ -z "$DATA_AVALANCHE_GCS_BUCKET" ]; then
+            if [ "$IS_FORK_PR" = "true" ]; then
+              echo "::notice::GCP_SA_KEY or DATA_AVALANCHE_GCS_BUCKET unavailable (fork PR) - the GCS E2E is skipped"
+              echo "SKIP_CLOUD_E2E=1" >> "$GITHUB_ENV"
+            else
+              echo "::error::GCP_SA_KEY secret or DATA_AVALANCHE_GCS_BUCKET variable is not configured (see test-infra/README.md \"CI requirements\")"
+              exit 1
+            fi
+          else
+            umask 077
+            printf '%s' "$GCP_SA_KEY" > "$RUNNER_TEMP/gcs-key.json"
+            echo "GOOGLE_SERVICE_ACCOUNT_PATH=$RUNNER_TEMP/gcs-key.json" >> "$GITHUB_ENV"
+          fi
+      - name: Start the source
+        if: env.SKIP_CLOUD_E2E != '1'
+        run: docker compose -f test-infra/compose.yaml up -d --wait ${{ matrix.source == 'postgres' && 'postgres-18' || 'mysql-84' }}
+      - name: Cloud E2E on GCS
+        if: env.SKIP_CLOUD_E2E != '1'
+        run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 --exact $(cat test-infra/cloud-e2e.txt)
+      - name: Container logs on failure
+        if: failure()
+        run: docker compose -f test-infra/compose.yaml logs --no-color
+      - name: Stop the containers
+        if: always()
+        run: docker compose -f test-infra/compose.yaml down -v
+      - name: Remove GCS key
+        if: always()
+        run: rm -f "$RUNNER_TEMP/gcs-key.json"
+
   # The smoke deploy (M11a): the image built from deploy/docker/Dockerfile, imported into
   # test-infra's k3s, and the Helm chart's three pods running a pipeline from PostgreSQL 18 into
   # RustFS; a deleted pod hands the pipeline to the others. No secrets.
diff --git a/.github/workflows/cloud.yml b/.github/workflows/cloud.yml
index fc8b717..e0869a7 100644
--- a/.github/workflows/cloud.yml
+++ b/.github/workflows/cloud.yml
@@ -1,9 +1,11 @@
 name: cloud
 
 # Real-cloud conformance (roadmap §5.4, P-5): the T1–T10 lease suite on real S3 and real Azure Blob,
-# through the production `open_bucket` path. GCS already runs in ci.yml. Requirements are in
-# test-infra/README.md, "Real S3 and Azure (cloud.yml)". Credentials are injected as env vars and
-# never printed.
+# through the production `open_bucket` path. GCS already runs in ci.yml. And the cloud E2E subset
+# (Q-M12-2, Q-M12c-4): the scenarios test-infra/cloud-e2e.txt names, with globals.state and the
+# lake on S3, GCS and Azure (DATA_AVALANCHE_E2E_STORE), on PostgreSQL 18 and on MySQL 8.4; ci.yml
+# runs the GCS half per change. Requirements are in test-infra/README.md, "Real S3 and Azure
+# (cloud.yml)". Credentials are injected as env vars and never printed.
 
 on:
   schedule:
@@ -49,3 +51,53 @@ jobs:
           AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
           AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
         run: cargo test --locked -p data-avalanche-state --test lease_conformance real_azure -- --nocapture
+
+  e2e:
+    runs-on: ubuntu-latest
+    timeout-minutes: 60
+    strategy:
+      fail-fast: false
+      matrix:
+        store: [s3, gcs, azure]
+        source: [postgres, mysql]
+    env:
+      DATA_AVALANCHE_IT: "1"
+      DATA_AVALANCHE_E2E_STORE: ${{ matrix.store }}
+      DATA_AVALANCHE_E2E_SOURCE: ${{ matrix.source }}
+      DATA_AVALANCHE_S3_BUCKET: ${{ vars.DATA_AVALANCHE_S3_BUCKET }}
+      AWS_REGION: ${{ vars.DATA_AVALANCHE_S3_REGION }}
+      DATA_AVALANCHE_GCS_BUCKET: ${{ vars.DATA_AVALANCHE_GCS_BUCKET }}
+      DATA_AVALANCHE_AZURE_CONTAINER: ${{ vars.DATA_AVALANCHE_AZURE_CONTAINER }}
+      AZURE_STORAGE_ACCOUNT_NAME: ${{ vars.DATA_AVALANCHE_AZURE_ACCOUNT }}
+    steps:
+      - name: Free runner disk
+        run: sudo rm -rf /usr/share/dotnet /usr/local/lib/android /opt/ghc /opt/hostedtoolcache/CodeQL
+      - uses: actions/checkout@v7
+      - uses: actions-rust-lang/setup-rust-toolchain@v2
+      - name: GCS credentials
+        if: matrix.store == 'gcs'
+        env:
+          GCP_SA_KEY: ${{ secrets.GCP_SA_KEY }}
+        run: |
+          umask 077
+          printf '%s' "$GCP_SA_KEY" > "$RUNNER_TEMP/gcs-key.json"
+          echo "GOOGLE_SERVICE_ACCOUNT_PATH=$RUNNER_TEMP/gcs-key.json" >> "$GITHUB_ENV"
+      - name: Start the source
+        run: docker compose -f test-infra/compose.yaml up -d --wait ${{ matrix.source == 'postgres' && 'postgres-18' || 'mysql-84' }}
+      - name: Cloud E2E
+        env:
+          AWS_ACCESS_KEY_ID: ${{ matrix.store == 's3' && secrets.AWS_ACCESS_KEY_ID || '' }}
+          AWS_SECRET_ACCESS_KEY: ${{ matrix.store == 's3' && secrets.AWS_SECRET_ACCESS_KEY || '' }}
+          AZURE_TENANT_ID: ${{ matrix.store == 'azure' && secrets.AZURE_TENANT_ID || '' }}
+          AZURE_CLIENT_ID: ${{ matrix.store == 'azure' && secrets.AZURE_CLIENT_ID || '' }}
+          AZURE_CLIENT_SECRET: ${{ matrix.store == 'azure' && secrets.AZURE_CLIENT_SECRET || '' }}
+        run: cargo test --locked -p data-avalanche --features e2e-hooks --test e2e -- --test-threads=1 --exact $(cat test-infra/cloud-e2e.txt)
+      - name: Container logs on failure
+        if: failure()
+        run: docker compose -f test-infra/compose.yaml logs --no-color
+      - name: Stop the containers
+        if: always()
+        run: docker compose -f test-infra/compose.yaml down -v
+      - name: Remove GCS key
+        if: always()
+        run: rm -f "$RUNNER_TEMP/gcs-key.json"
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index f26afca..d1deb58 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -1,7 +1,7 @@
 //! One E2E test's world on the source under test ([`Source::from_env`]): PostgreSQL's schema
 //! `e2e_<test>` with its slot and publication, or MySQL's database `e2e_<test>` with a user of
-//! that name; a RustFS prefix for `globals.state` and the lake, a config file, and the binary as
-//! a child process.
+//! that name; a prefix for `globals.state` and the lake on the run's [`Store`] (RustFS unless
+//! `DATA_AVALANCHE_E2E_STORE` names another), a config file, and the binary as a child process.
 
 use std::path::PathBuf;
 use std::process::{Child, Command, ExitStatus, Stdio};
@@ -17,17 +17,15 @@ use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, DeltaSinkConfig};
 use data_avalanche_state::{
     DocStore, HaltsDoc, LeaseDoc, Leases, PipelineDocs, TableSnapshotDoc, Versioned,
 };
-use data_avalanche_storage::{Backend, ObjectStoreVersioned, colocated_state, table_root};
-use data_avalanche_testkit::endpoints::{
-    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
-};
-use data_avalanche_testkit::{delete_prefix, stores, unique_prefix};
+use data_avalanche_storage::{ObjectStoreVersioned, colocated_state, table_root};
+use data_avalanche_testkit::{delete_prefix, unique_prefix};
 use futures_util::TryStreamExt;
 use rustls::pki_types::{CertificateDer, ServerName};
 use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
 use tokio_rustls::TlsConnector;
 
 pub use crate::source::{Db, Source};
+pub use crate::store::Store;
 
 /// Polls `cond` every 100 ms until it holds, for at most `within`.
 pub async fn eventually<F, Fut>(what: &str, within: Duration, mut cond: F)
@@ -107,6 +105,8 @@ pub struct E2e {
     /// PostgreSQL's schema, MySQL's database: `e2e_<test>`.
     pub schema: String,
     pub source: Source,
+    /// Where `globals.state` and the lake are.
+    pub store: Store,
     prefix: String,
     /// The first node's (`n1`) API port.
     port: u16,
@@ -184,6 +184,7 @@ impl E2e {
             test: test.to_owned(),
             schema,
             source,
+            store: Store::from_env(),
             prefix: unique_prefix(&format!("e2e-{test}")),
             port: free_port(),
             dir: tempfile::tempdir().expect("a temp dir"),
@@ -274,11 +275,11 @@ impl E2e {
             .replace("@INCLUDE@", &include)
             .replace("@SCHEMA@", &self.schema)
             .replace("@PREFIX@", &self.prefix)
-            .replace("@BUCKET@", TEST_BUCKET);
+            .replace("@BUCKET@", &self.store.bucket());
         format!(
             r#"config_version: 1
 globals:
-  cloud: aws
+  cloud: {cloud}
   state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
   api: {{ address: "127.0.0.1:${{E2E_API_PORT}}"{api} }}
   log: {log}
@@ -297,7 +298,8 @@ pipelines:
     {pipeline}
 {more}
 "#,
-            bucket = TEST_BUCKET,
+            bucket = self.store.bucket(),
+            cloud = self.store.cloud(),
             prefix = self.prefix,
             test = self.test,
             source = self.source_block("", o.port, o.source, o.postgres, o.mysql),
@@ -349,11 +351,7 @@ pipelines:
             .arg(self.config_path())
             .env_clear()
             .env("PATH", std::env::var("PATH").unwrap_or_default())
-            .env("AWS_ACCESS_KEY_ID", RUSTFS_ACCESS_KEY)
-            .env("AWS_SECRET_ACCESS_KEY", RUSTFS_SECRET_KEY)
-            .env("AWS_REGION", "us-east-1")
-            .env("AWS_ENDPOINT_URL", RUSTFS_URL)
-            .env("AWS_ALLOW_HTTP", "true")
+            .envs(self.store.env())
             .env("RUST_BACKTRACE", "1")
             .env("E2E_NODE", node)
             .env("E2E_API_PORT", port.to_string())
@@ -393,27 +391,20 @@ pipelines:
         TableRef::new(&self.schema, name)
     }
 
-    /// A reader of the pipeline's Delta tables on RustFS.
+    /// A reader of the pipeline's Delta tables.
     pub fn lake(&self) -> DeltaSink {
         self.lake_in("lake")
     }
 
     /// [`Self::lake`] of the pipeline whose sink prefix ends in `lake` (`lake`, `lake_b`).
     pub fn lake_in(&self, lake: &str) -> DeltaSink {
-        let opt = |k: &str, v: &str| (k.to_owned(), v.to_owned());
         DeltaSink::new(DeltaSinkConfig {
             pipeline_id: self.test.clone(),
             location: DeltaLocation::Bucket {
-                backend: Backend::S3,
-                bucket: TEST_BUCKET.into(),
+                backend: self.store.backend(),
+                bucket: self.store.bucket(),
                 prefix: format!("{}/{lake}", self.prefix),
-                options: vec![
-                    opt("endpoint", RUSTFS_URL),
-                    opt("allow_http", "true"),
-                    opt("region", "us-east-1"),
-                    opt("access_key_id", RUSTFS_ACCESS_KEY),
-                    opt("secret_access_key", RUSTFS_SECRET_KEY),
-                ],
+                options: self.store.lake_options(),
             },
             table: Default::default(),
             merge_memory_limit: None,
@@ -659,7 +650,7 @@ pipelines:
     }
 
     fn docs_at(&self, root: &str) -> DocStore {
-        let store = ObjectStoreVersioned::new(stores::rustfs(), root).expect("a root");
+        let store = ObjectStoreVersioned::new(self.store.objects(), root).expect("a root");
         DocStore::new(std::sync::Arc::new(store))
     }
 
@@ -712,7 +703,7 @@ pipelines:
 
     /// The pipeline's own documents, co-located with the lake (`<sink.prefix>/.pipeline-state`).
     pub fn docs(&self) -> PipelineDocs {
-        let at = colocated_state(TEST_BUCKET, &self.lake_prefix());
+        let at = colocated_state(&self.store.bucket(), &self.lake_prefix());
         PipelineDocs::new(self.docs_at(&at.root))
     }
 
@@ -761,9 +752,9 @@ pipelines:
 
     /// [`Self::log`] in the lake `lake`.
     pub async fn log_in(&self, lake: &str, table: &str) -> Vec<Vec<serde_json::Value>> {
-        let store = stores::rustfs();
+        let store = self.store.objects();
         let root = table_root(
-            TEST_BUCKET,
+            &self.store.bucket(),
             &format!("{}/{lake}", self.prefix),
             &self.schema,
             table,
@@ -804,12 +795,18 @@ pipelines:
     }
 
     fn table_root(&self, table: &str) -> String {
-        table_root(TEST_BUCKET, &self.lake_prefix(), &self.schema, table).root
+        table_root(
+            &self.store.bucket(),
+            &self.lake_prefix(),
+            &self.schema,
+            table,
+        )
+        .root
     }
 
-    /// The Delta table's root as an `s3://` URL.
+    /// The Delta table's root as an `s3://` URL (RustFS's).
     pub fn table_url(&self, table: &str) -> String {
-        format!("s3://{TEST_BUCKET}/{}", self.table_root(table))
+        format!("s3://{}/{}", self.store.bucket(), self.table_root(table))
     }
 
     /// The Delta table's commits: its `_delta_log/*.json` files.
@@ -819,14 +816,14 @@ pipelines:
 
     /// Deletes the Delta table from the lake, as an operator would.
     pub async fn drop_lake_table(&self, table: &str) {
-        delete_prefix(stores::rustfs().as_ref(), &self.table_root(table)).await;
+        delete_prefix(self.store.objects().as_ref(), &self.table_root(table)).await;
     }
 
     /// Deletes the pipeline's own documents (`<sink.prefix>/.pipeline-state`), as a lost state
     /// would: the next start meets the lake's tables with no schema cached (DA-SNAP-13).
     pub async fn wipe_state(&self) {
-        let at = colocated_state(TEST_BUCKET, &self.lake_prefix());
-        delete_prefix(stores::rustfs().as_ref(), &at.root).await;
+        let at = colocated_state(&self.store.bucket(), &self.lake_prefix());
+        delete_prefix(self.store.objects().as_ref(), &at.root).await;
     }
 
     /// The metrics' `source_db` label: PostgreSQL's database, MySQL's (the test's).
@@ -880,7 +877,7 @@ pipelines:
                 .await
                 .expect("cleanup"),
         }
-        delete_prefix(stores::rustfs().as_ref(), &self.prefix).await;
+        delete_prefix(self.store.objects().as_ref(), &self.prefix).await;
     }
 }
 
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index fbd890c..491fabd 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -25,6 +25,7 @@ mod schema;
 mod smoke;
 mod snapshots;
 mod source;
+mod store;
 mod streaming;
 mod toast;
 mod types;
diff --git a/crates/data-avalanche/tests/e2e/store.rs b/crates/data-avalanche/tests/e2e/store.rs
new file mode 100644
index 0000000..210b433
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/store.rs
@@ -0,0 +1,139 @@
+//! Where one E2E run keeps `globals.state` and the lake: RustFS (the default), or the store
+//! `DATA_AVALANCHE_E2E_STORE` names. `azurite` runs the Azure path on test-infra's emulator; `s3`,
+//! `gcs` and `azure` are the real clouds of the cloud runs (Q-M12-2), each through the bucket and
+//! the credentials its conformance suite already uses (`test-infra/README.md`). The binary gets
+//! those credentials from the test's environment, never from the config.
+
+use std::sync::Arc;
+
+use data_avalanche_storage::{Backend, BucketSpec, Timeouts, open_bucket};
+use data_avalanche_testkit::endpoints::{
+    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
+};
+use data_avalanche_testkit::{required_env, stores};
+use object_store::ObjectStore;
+
+/// The credential variables a cloud run passes on to the binary, when set and not empty (a
+/// workflow sets the other clouds' to ''): the binary's environment is cleared, and these are the
+/// ones `object_store` reads.
+const PASSED: &[&str] = &[
+    "AWS_ACCESS_KEY_ID",
+    "AWS_SECRET_ACCESS_KEY",
+    "AWS_SESSION_TOKEN",
+    "AWS_REGION",
+    "GOOGLE_SERVICE_ACCOUNT_PATH",
+    "GOOGLE_SERVICE_ACCOUNT_KEY",
+    "GOOGLE_APPLICATION_CREDENTIALS",
+    "AZURE_STORAGE_ACCOUNT_NAME",
+    "AZURE_TENANT_ID",
+    "AZURE_CLIENT_ID",
+    "AZURE_CLIENT_SECRET",
+];
+
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum Store {
+    RustFs,
+    Azurite,
+    S3,
+    Gcs,
+    Azure,
+}
+
+impl Store {
+    /// `DATA_AVALANCHE_E2E_STORE`: `rustfs` (or unset), `azurite`, `s3`, `gcs` or `azure`.
+    pub fn from_env() -> Self {
+        match std::env::var("DATA_AVALANCHE_E2E_STORE").as_deref() {
+            Err(_) | Ok("" | "rustfs") => Self::RustFs,
+            Ok("azurite") => Self::Azurite,
+            Ok("s3") => Self::S3,
+            Ok("gcs") => Self::Gcs,
+            Ok("azure") => Self::Azure,
+            Ok(other) => panic!(
+                "DATA_AVALANCHE_E2E_STORE={other}: expected rustfs, azurite, s3, gcs or azure"
+            ),
+        }
+    }
+
+    /// `globals.cloud`.
+    pub fn cloud(self) -> &'static str {
+        match self {
+            Self::RustFs | Self::S3 => "aws",
+            Self::Gcs => "google",
+            Self::Azurite | Self::Azure => "azure",
+        }
+    }
+
+    pub fn backend(self) -> Backend {
+        match self {
+            Self::RustFs | Self::S3 => Backend::S3,
+            Self::Gcs => Backend::Gcs,
+            Self::Azurite | Self::Azure => Backend::Azure { account: None },
+        }
+    }
+
+    /// The bucket (Azure's container) of `globals.state` and the lake.
+    pub fn bucket(self) -> String {
+        match self {
+            Self::RustFs | Self::Azurite => TEST_BUCKET.to_owned(),
+            Self::S3 => required_env("DATA_AVALANCHE_S3_BUCKET"),
+            Self::Gcs => required_env("DATA_AVALANCHE_GCS_BUCKET"),
+            Self::Azure => required_env("DATA_AVALANCHE_AZURE_CONTAINER"),
+        }
+    }
+
+    /// The store the tests read the lake's and the state's objects through.
+    pub fn objects(self) -> Arc<dyn ObjectStore> {
+        match self {
+            Self::RustFs => stores::rustfs(),
+            Self::Azurite => stores::azurite(),
+            Self::S3 | Self::Gcs | Self::Azure => open_bucket(&BucketSpec {
+                backend: self.backend(),
+                bucket: self.bucket(),
+                options: vec![],
+                timeouts: Timeouts::COORDINATION,
+            })
+            .unwrap_or_else(|e| panic!("{e}")),
+        }
+    }
+
+    /// The Delta sink's storage options for a reader in the test: the emulators' endpoints and
+    /// keys; none for a cloud, whose credentials come from the environment.
+    pub fn lake_options(self) -> Vec<(String, String)> {
+        let opt = |k: &str, v: &str| (k.to_owned(), v.to_owned());
+        match self {
+            Self::RustFs => vec![
+                opt("endpoint", RUSTFS_URL),
+                opt("allow_http", "true"),
+                opt("region", "us-east-1"),
+                opt("access_key_id", RUSTFS_ACCESS_KEY),
+                opt("secret_access_key", RUSTFS_SECRET_KEY),
+            ],
+            Self::Azurite => vec![opt("use_emulator", "true")],
+            Self::S3 | Self::Gcs | Self::Azure => vec![],
+        }
+    }
+
+    /// What the binary's environment gets for this store.
+    pub fn env(self) -> Vec<(String, String)> {
+        let var = |k: &str, v: &str| (k.to_owned(), v.to_owned());
+        match self {
+            Self::RustFs => vec![
+                var("AWS_ACCESS_KEY_ID", RUSTFS_ACCESS_KEY),
+                var("AWS_SECRET_ACCESS_KEY", RUSTFS_SECRET_KEY),
+                var("AWS_REGION", "us-east-1"),
+                var("AWS_ENDPOINT_URL", RUSTFS_URL),
+                var("AWS_ALLOW_HTTP", "true"),
+            ],
+            Self::Azurite => vec![var("AZURE_STORAGE_USE_EMULATOR", "true")],
+            Self::S3 | Self::Gcs | Self::Azure => PASSED
+                .iter()
+                .filter_map(|k| {
+                    std::env::var(k)
+                        .ok()
+                        .filter(|v| !v.is_empty())
+                        .map(|v| ((*k).to_owned(), v))
+                })
+                .collect(),
+        }
+    }
+}
diff --git a/test-infra/cloud-e2e.txt b/test-infra/cloud-e2e.txt
new file mode 100644
index 0000000..0a4cba0
--- /dev/null
+++ b/test-infra/cloud-e2e.txt
@@ -0,0 +1,4 @@
+smoke::a_pipeline_runs_stops_on_sigterm_and_resumes
+crashes::a_crash_between_a_commit_and_its_checkpoint_skips_the_replay
+cluster::a_killed_nodes_pipelines_are_taken_over
+jobs::a_job_clears_a_dropped_table_s_halt
diff --git a/xtask/tests/cloud_e2e.rs b/xtask/tests/cloud_e2e.rs
new file mode 100644
index 0000000..9a0217e
--- /dev/null
+++ b/xtask/tests/cloud_e2e.rs
@@ -0,0 +1,23 @@
+//! The cloud E2E runs (`ci.yml`'s `e2e-gcs`, `cloud.yml`'s `e2e`) pass `test-infra/cloud-e2e.txt`'s
+//! names to `--exact`, which runs nothing for a name that matches no test: each must name a
+//! scenario of the E2E suite.
+
+#[test]
+fn the_cloud_e2e_list_names_scenarios_of_the_suite() {
+    let root = xtask::repo_root();
+    let list = std::fs::read_to_string(root.join("test-infra/cloud-e2e.txt")).expect("the list");
+    let names: Vec<&str> = list.lines().filter(|l| !l.trim().is_empty()).collect();
+    assert_eq!(names.len(), 4, "Q-M12c-4: four scenarios");
+    for name in names {
+        let (module, test) = name.split_once("::").expect("module::test");
+        let file = root.join(format!("crates/data-avalanche/tests/e2e/{module}.rs"));
+        let text = std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("{name}: {e}"));
+        let at = text
+            .find(&format!("async fn {test}()"))
+            .unwrap_or_else(|| panic!("{name}: no such scenario in {}", file.display()));
+        assert!(
+            text[..at].trim_end().ends_with("#[tokio::test]"),
+            "{name}: not a test"
+        );
+    }
+}
```

- [ ] **Step 4: Run it to see it pass, and the store switch**

Run: `cargo test -p xtask --test cloud_e2e`. Expected: PASS; and FAIL with one name misspelt.
Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche --features e2e-hooks --test e2e smoke:: -- --test-threads=1`, then with `DATA_AVALANCHE_E2E_STORE=azurite`, then the four on Azurite: `… -- --test-threads=1 --exact $(cat test-infra/cloud-e2e.txt)`.
Expected: PASS each (the four in about 100 s). `ruby -ryaml -e 'ARGV.each { |f| YAML.load_file(f) }' .github/workflows/ci.yml .github/workflows/cloud.yml` parses both.
On GCS, Oleksii runs the four with his credentials (`test-infra/README.md`, "The E2E suite on
another store"); the PR's `e2e-gcs` runs them too.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests/e2e test-infra/cloud-e2e.txt xtask/tests/cloud_e2e.rs .github/workflows/ci.yml .github/workflows/cloud.yml
git commit -m "test(e2e): the suite on any store, and the cloud subset on S3, GCS and Azure (Q-M12-2)"
```

### Task 6: The acceptance runs (E2E harness, `load`, `xtask`) — Q-M12-3, Q-M12c-2, Q-M12c-3

**Files:**
- Create: `crates/data-avalanche/tests/e2e/nodes.rs`; `crates/data-avalanche/tests/load/{main,workload,report,bench,chaos,soak}.rs`; `xtask/src/acceptance.rs`
- Modify: `crates/data-avalanche/tests/e2e/{cluster,harness,main,source}.rs`, `crates/data-avalanche/Cargo.toml`
- Modify: `xtask/src/lib.rs`, `xtask/src/main.rs`, `xtask/Cargo.toml`

**Interfaces:**
- Consumes: Task 5's `Store`.
- Produces: `nodes::Cluster` with `Cluster::with(test, ddl, more, Options) -> Cluster` (and its
  methods made `pub`); `Options::defaults: bool`; `Proc::pid`; `Db::try_connect`;
  `Source::service`; `xtask::{invocation, Invocation, RUNS}`; the `load` test target with
  `bench::{steady_state_lag_at_the_defaults, merge_cost_under_random_key_churn,
  a_failover_recovers_within_two_minutes}`, `chaos::{a_storage_outage_shorter_than_the_lease_loses_nothing,
  a_storage_outage_longer_than_the_lease_loses_nothing, a_source_restart_loses_nothing,
  a_partition_under_load_loses_nothing}`, `soak::a_day_of_churn_loses_nothing`.

- [ ] **Step 1: Write the failing tests.** `xtask/src/acceptance.rs`:
  `a_run_is_its_module_of_the_load_target_in_release_mode` (the invocation of `chaos` is
  `test --release --locked -p data-avalanche --features e2e-hooks --test load -- --test-threads=1
  --nocapture chaos::`, with `DATA_AVALANCHE_LOAD=1` and `DATA_AVALANCHE_LOAD_OUT=<root>/target/acceptance`;
  `docs` isn't a run). `tests/load/report.rs`: `the_quantile_is_the_nearest_rank`.

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p xtask --lib`. Expected: FAIL to compile (`invocation` undefined).

- [ ] **Step 3: The harness's pieces, the target and the commands.** `Cluster` moves out of
  `cluster.rs` unchanged but for `pub` and `Cluster::with`; `Cluster::new` calls it with the
  suite's options. The runs are described in their files' docs; their measurements go through
  `report::record`.

```diff
diff --git a/crates/data-avalanche/Cargo.toml b/crates/data-avalanche/Cargo.toml
index d255d5c..353022a 100644
--- a/crates/data-avalanche/Cargo.toml
+++ b/crates/data-avalanche/Cargo.toml
@@ -53,3 +53,9 @@ tokio-rustls.workspace = true
 name = "e2e"
 path = "tests/e2e/main.rs"
 required-features = ["e2e-hooks"]
+
+# The v1 acceptance runs (M12c): `cargo xtask bench|chaos|soak` builds it in release mode.
+[[test]]
+name = "load"
+path = "tests/load/main.rs"
+required-features = ["e2e-hooks"]
diff --git a/crates/data-avalanche/tests/e2e/cluster.rs b/crates/data-avalanche/tests/e2e/cluster.rs
index 8ebafdb..93d7bad 100644
--- a/crates/data-avalanche/tests/e2e/cluster.rs
+++ b/crates/data-avalanche/tests/e2e/cluster.rs
@@ -9,250 +9,14 @@
 use std::collections::BTreeMap;
 use std::time::{Duration, Instant};
 
-use crate::forwarder::Forwarder;
-use data_avalanche_cluster::Ring;
 use data_avalanche_testkit::require_it;
 use serde_json::json;
 
-use crate::harness::{E2e, Options, Proc, eventually, free_udp_port};
+use crate::harness::{Proc, eventually};
+use crate::nodes::{Cluster, lake};
 
 /// The E2E config's `lease_duration` (harness.rs: `lease_duration: 6s`).
 const LEASE: Duration = Duration::from_secs(6);
-/// The `commitInfo.operation` of a table's creation commit.
-const CREATE_OP: &str = "CREATE TABLE";
-
-const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";
-
-/// The test's pipeline and two more over the same table, each into a lake of its own.
-const MORE: &str = r#"  b:
-    id: b
-@SOURCE(_b)@
-    tables: { include: ['@INCLUDE@'], snapshots: { mode: never } }
-    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_b, delta: {}, flush: { seconds: 1 } }
-  c:
-    id: c
-@SOURCE(_c)@
-    tables: { include: ['@INCLUDE@'], snapshots: { mode: never } }
-    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_c, delta: {}, flush: { seconds: 1 } }
-"#;
-
-struct Node {
-    name: String,
-    /// The port the node gossips from (its bind port).
-    gossip: u16,
-    fwd: Forwarder,
-    proc: Option<Proc>,
-}
-
-/// The nodes are dropped (and print their logs on a failure) before the test's directory.
-struct Cluster {
-    nodes: Vec<Node>,
-    /// The pipelines the config runs: the test's, `b`, `c`; a reload changes them.
-    ids: Vec<String>,
-    e: E2e,
-}
-
-/// Each pipeline but the test's own has a source suffix and a lake of its name.
-const SPARES: [&str; 3] = ["b", "c", "d"];
-
-/// A pipeline's lake.
-fn lake(test: &str, id: &str) -> String {
-    if id == test {
-        "lake".to_owned()
-    } else {
-        format!("lake_{id}")
-    }
-}
-
-impl Cluster {
-    /// `n1`, `n2` and `n3`, not started.
-    async fn new(test: &str) -> Self {
-        let mut nodes = Vec::new();
-        for name in ["n1", "n2", "n3"] {
-            let gossip = free_udp_port();
-            let fwd = Forwarder::start(gossip).await;
-            nodes.push(Node {
-                name: name.to_owned(),
-                gossip,
-                fwd,
-                proc: None,
-            });
-        }
-        let seeds: Vec<String> = nodes
-            .iter()
-            .map(|n| format!("\"127.0.0.1:{}\"", n.fwd.port()))
-            .collect();
-        let memberlist = format!(
-            ", push_pull_interval: 5s, join_members: [{}]",
-            seeds.join(", ")
-        );
-        let e = E2e::new(
-            test,
-            ITEMS,
-            Options {
-                tables: ", snapshots: { mode: never }",
-                cluster: ", rebalance_stabilization: 3s",
-                memberlist: &memberlist,
-                pipelines: MORE,
-                ..Options::default()
-            },
-        )
-        .await;
-        for id in SPARES {
-            e.drop_source(&format!("_{id}")).await;
-        }
-        let ids = vec![e.test.clone(), "b".into(), "c".into()];
-        Self { nodes, ids, e }
-    }
-
-    async fn start(&mut self, i: usize) {
-        let n = &mut self.nodes[i];
-        let (gossip, advertise) = (n.gossip.to_string(), n.fwd.port().to_string());
-        let mut p = self.e.start_node_with(
-            &n.name,
-            &[
-                ("E2E_GOSSIP_PORT", &gossip),
-                ("E2E_ADVERTISE_PORT", &advertise),
-            ],
-        );
-        p.ready().await;
-        n.proc = Some(p);
-    }
-
-    async fn start_all(&mut self) {
-        for i in 0..self.nodes.len() {
-            self.start(i).await;
-        }
-        self.settled().await;
-    }
-
-    fn ids(&self) -> Vec<String> {
-        self.ids.clone()
-    }
-
-    /// Each pipeline's lease: its holder and epoch, unless released.
-    async fn holders(&self) -> BTreeMap<String, (String, u64)> {
-        let mut by = BTreeMap::new();
-        for id in self.ids() {
-            if let Some(l) = self.e.lease_of(&id).await
-                && !l.doc.released
-            {
-                by.insert(id, (l.doc.holder_node_id, l.doc.owner_epoch));
-            }
-        }
-        by
-    }
-
-    fn live(&self) -> Vec<&Node> {
-        self.nodes.iter().filter(|n| n.proc.is_some()).collect()
-    }
-
-    fn proc(&self, name: &str) -> &Proc {
-        self.nodes[self.index(name)]
-            .proc
-            .as_ref()
-            .expect("a live node")
-    }
-
-    fn index(&self, name: &str) -> usize {
-        self.nodes
-            .iter()
-            .position(|n| n.name == name)
-            .expect("a node")
-    }
-
-    /// Waits until every live node sees the others and holds what its metric says, and each
-    /// pipeline is held by the node the ring over the live nodes places it on.
-    async fn settled(&self) {
-        let live = self.live();
-        let ring = Ring::new(live.iter().map(|n| n.name.as_str()), 128);
-        eventually("the cluster settles", Duration::from_secs(90), async || {
-            let holders = self.holders().await;
-            let placed = self
-                .ids()
-                .iter()
-                .all(|id| holders.get(id).map(|(h, _)| h.as_str()) == ring.owner(id));
-            if !placed {
-                return false;
-            }
-            for n in &live {
-                let text = n.proc.as_ref().unwrap().metrics().await;
-                let held = holders.values().filter(|(h, _)| *h == n.name).count();
-                let lines = [
-                    format!("data_avalanche_cluster_members {}", live.len()),
-                    format!("data_avalanche_owned_pipelines {held}"),
-                ];
-                if !lines.iter().all(|l| text.lines().any(|t| t == l)) {
-                    return false;
-                }
-            }
-            true
-        })
-        .await;
-    }
-
-    /// Cuts `side` off from every other node, both ways.
-    fn cut(&self, side: &[&str]) {
-        let (a, b): (Vec<&Node>, Vec<&Node>) = self
-            .nodes
-            .iter()
-            .partition(|n| side.contains(&n.name.as_str()));
-        for x in &a {
-            x.fwd.block(&b.iter().map(|n| n.gossip).collect::<Vec<_>>());
-        }
-        for y in &b {
-            y.fwd.block(&a.iter().map(|n| n.gossip).collect::<Vec<_>>());
-        }
-    }
-
-    fn heal(&self) {
-        for n in &self.nodes {
-            n.fwd.heal();
-        }
-    }
-
-    /// Every lake's `items` holds the source's rows, and its owner epochs never went back.
-    async fn landed(&self) {
-        for id in self.ids() {
-            let lake = lake(&self.e.test, &id);
-            let lake = lake.as_str();
-            self.e
-                .converged_in(lake, "items", Duration::from_secs(60))
-                .await;
-            let log = self.e.log_in(lake, "items").await;
-            assert!(!log.is_empty(), "{lake}: no commits");
-            let mut epochs = Vec::new();
-            for actions in &log {
-                let info = actions
-                    .iter()
-                    .find(|a| a.get("commitInfo").is_some())
-                    .expect("a commit has its commitInfo");
-                match info["commitInfo"]["data-avalanche.owner_epoch"].as_u64() {
-                    Some(e) => epochs.push(e),
-                    // CREATE_OP is the table's creation: it carries no epoch.
-                    None => assert_eq!(
-                        info["commitInfo"]["operation"], CREATE_OP,
-                        "{lake}: a commit without an epoch: {info}"
-                    ),
-                }
-            }
-            assert!(!epochs.is_empty(), "{lake}: the commits carry their epoch");
-            assert!(epochs.is_sorted(), "{lake}: an epoch went back: {epochs:?}");
-        }
-    }
-
-    async fn stop(mut self) {
-        for n in &mut self.nodes {
-            if let Some(p) = n.proc.take() {
-                assert!(p.terminate().await.success(), "{} stops", n.name);
-            }
-        }
-        for id in SPARES {
-            self.e.drop_source(&format!("_{id}")).await;
-        }
-        self.e.cleanup().await;
-    }
-}
 
 /// The holder of the test's own pipeline is killed: its pipelines go to the others once their
 /// leases ran out (suspicion 4 s, lease 6 s), at the next epoch, and every row lands.
diff --git a/crates/data-avalanche/tests/e2e/harness.rs b/crates/data-avalanche/tests/e2e/harness.rs
index d1deb58..2379887 100644
--- a/crates/data-avalanche/tests/e2e/harness.rs
+++ b/crates/data-avalanche/tests/e2e/harness.rs
@@ -89,6 +89,10 @@ pub struct Options<'a> {
     pub globals: &'a str,
     /// `globals.log`, a YAML flow map; `{ level: info, format: compact }` when empty.
     pub log: &'a str,
+    /// The product's defaults for the lease (15 s, renewed every 5 s) and the test's pipeline's
+    /// flush (30 s), which D-V1.3's lag targets assume: the acceptance runs. Off, the suite's
+    /// shorter ones (a 6 s lease, a 1 s flush).
+    pub defaults: bool,
 }
 
 /// The rows [`E2e::insert_series`] gives a key `g`.
@@ -283,9 +287,7 @@ globals:
   state: {{ bucket: {bucket}, prefix: {prefix}/globals }}
   api: {{ address: "127.0.0.1:${{E2E_API_PORT}}"{api} }}
   log: {log}
-  cluster:
-    lease_duration: 6s
-    lease_renewal_interval: 2s
+  cluster:{lease}
     drain_timeout: 30s{cluster}
     memberlist: {{ node_name: "${{E2E_NODE}}", bind_addr: [127.0.0.1], bind_port: "${{E2E_GOSSIP_PORT}}", advertise_addr: 127.0.0.1, advertise_port: "${{E2E_ADVERTISE_PORT}}"{memberlist} }}
   {globals}
@@ -294,7 +296,7 @@ pipelines:
     id: {test}
 {source}
     tables: {{ include: ['{include}']{tables} }}
-    sink: {{ type: delta, bucket: {bucket}, prefix: {prefix}/lake, delta: {{}}, flush: {{ seconds: 1 }}{sink} }}
+    sink: {{ type: delta, bucket: {bucket}, prefix: {prefix}/lake, delta: {{}}{flush}{sink} }}
     {pipeline}
 {more}
 "#,
@@ -307,6 +309,16 @@ pipelines:
             sink = o.sink,
             pipeline = o.pipeline,
             cluster = o.cluster.replace(", ", "\n    "),
+            lease = if o.defaults {
+                ""
+            } else {
+                "\n    lease_duration: 6s\n    lease_renewal_interval: 2s"
+            },
+            flush = if o.defaults {
+                ""
+            } else {
+                ", flush: { seconds: 1 }"
+            },
             memberlist = o.memberlist,
             api = o.api,
             globals = o.globals,
@@ -1025,6 +1037,12 @@ impl Proc {
         .await;
     }
 
+    /// The process's id: its memory, for the soak (`tests/load`).
+    #[allow(dead_code)]
+    pub fn pid(&self) -> u32 {
+        self.child.id()
+    }
+
     /// Waits for `/health/ready`.
     pub async fn ready(&mut self) {
         let until = Instant::now() + Duration::from_secs(60);
diff --git a/crates/data-avalanche/tests/e2e/main.rs b/crates/data-avalanche/tests/e2e/main.rs
index 491fabd..db20925 100644
--- a/crates/data-avalanche/tests/e2e/main.rs
+++ b/crates/data-avalanche/tests/e2e/main.rs
@@ -18,6 +18,7 @@ mod jobs;
 mod k3s;
 mod lifecycle;
 mod mysql;
+mod nodes;
 mod observability;
 mod proxy;
 mod reload;
diff --git a/crates/data-avalanche/tests/e2e/nodes.rs b/crates/data-avalanche/tests/e2e/nodes.rs
new file mode 100644
index 0000000..a6cac43
--- /dev/null
+++ b/crates/data-avalanche/tests/e2e/nodes.rs
@@ -0,0 +1,271 @@
+//! Three nodes as three processes on the host (Q-M8-4): each gossips on its own loopback port
+//! behind a [`Forwarder`], seeded with every node's forwarder, and runs one config's pipelines.
+//! The cluster scenarios (`cluster.rs`) and the acceptance runs (`tests/load`) share it.
+
+use std::collections::BTreeMap;
+use std::time::Duration;
+
+use data_avalanche_cluster::Ring;
+
+use crate::forwarder::Forwarder;
+use crate::harness::{E2e, Options, Proc, eventually, free_udp_port};
+
+/// The `commitInfo.operation` of a table's creation commit.
+pub const CREATE_OP: &str = "CREATE TABLE";
+
+pub const ITEMS: &str = "CREATE TABLE items (id bigint PRIMARY KEY, doc text);";
+
+/// The test's pipeline and two more over the same table, each into a lake of its own.
+pub const MORE: &str = r#"  b:
+    id: b
+@SOURCE(_b)@
+    tables: { include: ['@INCLUDE@'], snapshots: { mode: never } }
+    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_b, delta: {}, flush: { seconds: 1 } }
+  c:
+    id: c
+@SOURCE(_c)@
+    tables: { include: ['@INCLUDE@'], snapshots: { mode: never } }
+    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_c, delta: {}, flush: { seconds: 1 } }
+"#;
+
+pub struct Node {
+    pub name: String,
+    /// The port the node gossips from (its bind port).
+    pub gossip: u16,
+    pub fwd: Forwarder,
+    pub proc: Option<Proc>,
+}
+
+/// The nodes are dropped (and print their logs on a failure) before the test's directory.
+pub struct Cluster {
+    pub nodes: Vec<Node>,
+    /// The pipelines the config runs: the test's, and the [`SPARES`] its `more` names (`b`, `c`
+    /// for [`Cluster::new`]); a reload changes them.
+    pub ids: Vec<String>,
+    pub e: E2e,
+}
+
+/// Each pipeline but the test's own has a source suffix and a lake of its name.
+pub const SPARES: [&str; 3] = ["b", "c", "d"];
+
+/// A pipeline's lake.
+pub fn lake(test: &str, id: &str) -> String {
+    if id == test {
+        "lake".to_owned()
+    } else {
+        format!("lake_{id}")
+    }
+}
+
+impl Cluster {
+    /// `n1`, `n2` and `n3`, not started: the test's pipeline over [`ITEMS`] and [`MORE`]'s two,
+    /// each flushing every second, none snapshotting.
+    pub async fn new(test: &str) -> Self {
+        Self::with(
+            test,
+            ITEMS,
+            MORE,
+            Options {
+                tables: ", snapshots: { mode: never }",
+                cluster: ", rebalance_stabilization: 3s",
+                ..Options::default()
+            },
+        )
+        .await
+    }
+
+    /// `n1`, `n2` and `n3`, not started, gossiping through their forwarders: `ddl` in the test's
+    /// schema, the test's pipeline as `o` says, and `more` pipelines, each named for one of
+    /// [`SPARES`] and reading through `@SOURCE(_<id>)@`.
+    pub async fn with(test: &str, ddl: &str, more: &str, o: Options<'_>) -> Self {
+        let mut nodes = Vec::new();
+        for name in ["n1", "n2", "n3"] {
+            let gossip = free_udp_port();
+            let fwd = Forwarder::start(gossip).await;
+            nodes.push(Node {
+                name: name.to_owned(),
+                gossip,
+                fwd,
+                proc: None,
+            });
+        }
+        let seeds: Vec<String> = nodes
+            .iter()
+            .map(|n| format!("\"127.0.0.1:{}\"", n.fwd.port()))
+            .collect();
+        let memberlist = format!(
+            ", push_pull_interval: 5s, join_members: [{}]",
+            seeds.join(", ")
+        );
+        let e = E2e::new(
+            test,
+            ddl,
+            Options {
+                memberlist: &memberlist,
+                pipelines: more,
+                ..o
+            },
+        )
+        .await;
+        for id in SPARES {
+            e.drop_source(&format!("_{id}")).await;
+        }
+        let ids = std::iter::once(e.test.clone())
+            .chain(
+                SPARES
+                    .iter()
+                    .filter(|id| more.contains(&format!("  {id}:\n")))
+                    .map(|id| (*id).to_owned()),
+            )
+            .collect();
+        Self { nodes, ids, e }
+    }
+
+    pub async fn start(&mut self, i: usize) {
+        let n = &mut self.nodes[i];
+        let (gossip, advertise) = (n.gossip.to_string(), n.fwd.port().to_string());
+        let mut p = self.e.start_node_with(
+            &n.name,
+            &[
+                ("E2E_GOSSIP_PORT", &gossip),
+                ("E2E_ADVERTISE_PORT", &advertise),
+            ],
+        );
+        p.ready().await;
+        n.proc = Some(p);
+    }
+
+    pub async fn start_all(&mut self) {
+        for i in 0..self.nodes.len() {
+            self.start(i).await;
+        }
+        self.settled().await;
+    }
+
+    pub fn ids(&self) -> Vec<String> {
+        self.ids.clone()
+    }
+
+    /// Each pipeline's lease: its holder and epoch, unless released.
+    pub async fn holders(&self) -> BTreeMap<String, (String, u64)> {
+        let mut by = BTreeMap::new();
+        for id in self.ids() {
+            if let Some(l) = self.e.lease_of(&id).await
+                && !l.doc.released
+            {
+                by.insert(id, (l.doc.holder_node_id, l.doc.owner_epoch));
+            }
+        }
+        by
+    }
+
+    pub fn live(&self) -> Vec<&Node> {
+        self.nodes.iter().filter(|n| n.proc.is_some()).collect()
+    }
+
+    pub fn proc(&self, name: &str) -> &Proc {
+        self.nodes[self.index(name)]
+            .proc
+            .as_ref()
+            .expect("a live node")
+    }
+
+    pub fn index(&self, name: &str) -> usize {
+        self.nodes
+            .iter()
+            .position(|n| n.name == name)
+            .expect("a node")
+    }
+
+    /// Waits until every live node sees the others and holds what its metric says, and each
+    /// pipeline is held by the node the ring over the live nodes places it on.
+    pub async fn settled(&self) {
+        let live = self.live();
+        let ring = Ring::new(live.iter().map(|n| n.name.as_str()), 128);
+        eventually("the cluster settles", Duration::from_secs(90), async || {
+            let holders = self.holders().await;
+            let placed = self
+                .ids()
+                .iter()
+                .all(|id| holders.get(id).map(|(h, _)| h.as_str()) == ring.owner(id));
+            if !placed {
+                return false;
+            }
+            for n in &live {
+                let text = n.proc.as_ref().unwrap().metrics().await;
+                let held = holders.values().filter(|(h, _)| *h == n.name).count();
+                let lines = [
+                    format!("data_avalanche_cluster_members {}", live.len()),
+                    format!("data_avalanche_owned_pipelines {held}"),
+                ];
+                if !lines.iter().all(|l| text.lines().any(|t| t == l)) {
+                    return false;
+                }
+            }
+            true
+        })
+        .await;
+    }
+
+    /// Cuts `side` off from every other node, both ways.
+    pub fn cut(&self, side: &[&str]) {
+        let (a, b): (Vec<&Node>, Vec<&Node>) = self
+            .nodes
+            .iter()
+            .partition(|n| side.contains(&n.name.as_str()));
+        for x in &a {
+            x.fwd.block(&b.iter().map(|n| n.gossip).collect::<Vec<_>>());
+        }
+        for y in &b {
+            y.fwd.block(&a.iter().map(|n| n.gossip).collect::<Vec<_>>());
+        }
+    }
+
+    pub fn heal(&self) {
+        for n in &self.nodes {
+            n.fwd.heal();
+        }
+    }
+
+    /// Every lake's `items` holds the source's rows, and its owner epochs never went back.
+    pub async fn landed(&self) {
+        for id in self.ids() {
+            let lake = lake(&self.e.test, &id);
+            let lake = lake.as_str();
+            self.e
+                .converged_in(lake, "items", Duration::from_secs(60))
+                .await;
+            let log = self.e.log_in(lake, "items").await;
+            assert!(!log.is_empty(), "{lake}: no commits");
+            let mut epochs = Vec::new();
+            for actions in &log {
+                let info = actions
+                    .iter()
+                    .find(|a| a.get("commitInfo").is_some())
+                    .expect("a commit has its commitInfo");
+                match info["commitInfo"]["data-avalanche.owner_epoch"].as_u64() {
+                    Some(e) => epochs.push(e),
+                    // CREATE_OP is the table's creation: it carries no epoch.
+                    None => assert_eq!(
+                        info["commitInfo"]["operation"], CREATE_OP,
+                        "{lake}: a commit without an epoch: {info}"
+                    ),
+                }
+            }
+            assert!(!epochs.is_empty(), "{lake}: the commits carry their epoch");
+            assert!(epochs.is_sorted(), "{lake}: an epoch went back: {epochs:?}");
+        }
+    }
+
+    pub async fn stop(mut self) {
+        for n in &mut self.nodes {
+            if let Some(p) = n.proc.take() {
+                assert!(p.terminate().await.success(), "{} stops", n.name);
+            }
+        }
+        for id in SPARES {
+            self.e.drop_source(&format!("_{id}")).await;
+        }
+        self.e.cleanup().await;
+    }
+}
diff --git a/crates/data-avalanche/tests/e2e/source.rs b/crates/data-avalanche/tests/e2e/source.rs
index 183740a..94313ab 100644
--- a/crates/data-avalanche/tests/e2e/source.rs
+++ b/crates/data-avalanche/tests/e2e/source.rs
@@ -109,6 +109,16 @@ impl Source {
         matches!(self, Self::Postgres { .. })
     }
 
+    /// The server's compose service: `postgres-<major>` on `554<major>`, a MySQL target's own; the
+    /// chaos run restarts it (`tests/load`).
+    #[allow(dead_code)]
+    pub fn service(self) -> String {
+        match self {
+            Self::Postgres { port } => format!("postgres-{}", port - 55400),
+            Self::Mysql(t) => t.service.to_owned(),
+        }
+    }
+
     /// The server's container: `postgres-<major>` on `554<major>`, a MySQL target's service.
     pub fn container(self) -> String {
         match self {
@@ -153,6 +163,13 @@ pub enum Db {
 
 impl Db {
     pub async fn connect(source: Source) -> Self {
+        Self::try_connect(source)
+            .await
+            .unwrap_or_else(|e| panic!("connect to the test source: {e}"))
+    }
+
+    /// [`Self::connect`], its failure returned: a source that is restarting.
+    pub async fn try_connect(source: Source) -> Result<Self, String> {
         match source {
             Source::Postgres { port } => {
                 let dsn = format!(
@@ -160,11 +177,22 @@ impl Db {
                 );
                 let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
                     .await
-                    .expect("connect to test PostgreSQL");
+                    .map_err(|e| e.to_string())?;
                 tokio::spawn(conn);
-                Self::Pg(client)
+                Ok(Self::Pg(client))
+            }
+            Source::Mysql(t) => {
+                let opts = mysql_async::OptsBuilder::default()
+                    .ip_or_hostname("127.0.0.1")
+                    .tcp_port(t.port)
+                    .user(Some("root"))
+                    .pass(Some("avalanche"))
+                    .prefer_socket(false);
+                let conn = mysql_async::Conn::new(opts)
+                    .await
+                    .map_err(|e| e.to_string())?;
+                Ok(Self::Mysql(Mutex::new(conn)))
             }
-            Source::Mysql(t) => Self::Mysql(Mutex::new(mysql(t.port).await)),
         }
     }
 
@@ -235,16 +263,3 @@ impl Db {
         }
     }
 }
-
-/// A `root` connection to the MySQL server on `port`, in no database.
-pub async fn mysql(port: u16) -> mysql_async::Conn {
-    let opts = mysql_async::OptsBuilder::default()
-        .ip_or_hostname("127.0.0.1")
-        .tcp_port(port)
-        .user(Some("root"))
-        .pass(Some("avalanche"))
-        .prefer_socket(false);
-    mysql_async::Conn::new(opts)
-        .await
-        .expect("connect to the test MySQL server")
-}
diff --git a/crates/data-avalanche/tests/load/bench.rs b/crates/data-avalanche/tests/load/bench.rs
new file mode 100644
index 0000000..1db81d5
--- /dev/null
+++ b/crates/data-avalanche/tests/load/bench.rs
@@ -0,0 +1,204 @@
+//! D-V1.3's lag targets at the product's defaults (a 30 s flush, a 15 s lease renewed every 5 s):
+//! p99 of `data_avalanche_replication_lag_seconds` at most 60 s in steady state, and the lag back
+//! under 60 s within 2 minutes of the holder's loss. And MERGE's cost under random-key churn on a
+//! large table (R-5, D-MS.15), measured, with no target: D-V1.3 excludes it.
+
+use std::time::{Duration, Instant};
+
+use crate::harness::{E2e, Options, eventually};
+use crate::nodes::Cluster;
+use crate::report::{Samples, histogram, knob, lag, record};
+use crate::workload::Workload;
+use crate::{lands, require_load, snapshotted};
+
+const RUN: &str = "bench";
+/// Before the lag is sampled: the writer's first flushes.
+const WARM_UP: Duration = Duration::from_secs(60);
+/// D-V1.3.
+const STEADY_P99: f64 = 60.0;
+const FAILOVER: Duration = Duration::from_secs(120);
+
+fn rate() -> u32 {
+    u32::try_from(knob("DATA_AVALANCHE_LOAD_RATE", 1000)).expect("a rate")
+}
+
+/// Four tables of `DATA_AVALANCHE_LOAD_KEYS` keys (100 000) each, snapshotted, then changed at
+/// `DATA_AVALANCHE_LOAD_RATE` changes a second (1000) for `DATA_AVALANCHE_LOAD_MINUTES` (30)
+/// after a minute's warm-up, the lag sampled every second.
+#[tokio::test]
+async fn steady_state_lag_at_the_defaults() {
+    require_load!();
+    let w = Workload::new(4, knob("DATA_AVALANCHE_LOAD_KEYS", 100_000), rate());
+    let window = Duration::from_secs(60 * knob("DATA_AVALANCHE_LOAD_MINUTES", 30));
+    let e = E2e::new(
+        "load_steady",
+        &w.ddl(),
+        Options {
+            tables: ", snapshots: { mode: initial }",
+            defaults: true,
+            ..Options::default()
+        },
+    )
+    .await;
+    w.prefill(&e).await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    snapshotted(&e, &w.tables, Duration::from_secs(1800)).await;
+    let writer = w.start(&e);
+    tokio::time::sleep(WARM_UP).await;
+    let lag = Samples::take(&p, &e.test, window).await;
+    let counts = writer.stop().await;
+    let p99 = lag.quantile(0.99);
+    let secs = (WARM_UP + window).as_secs_f64();
+    record(RUN, "steady_lag_p50", lag.quantile(0.5), "s", None);
+    record(
+        RUN,
+        "steady_lag_p99",
+        p99,
+        "s",
+        Some(("<= 60", p99 <= STEADY_P99)),
+    );
+    record(RUN, "steady_lag_max", lag.quantile(1.0), "s", None);
+    record(RUN, "steady_lag_samples", lag.0.len() as f64, "", None);
+    record(
+        RUN,
+        "steady_rate",
+        counts.changes as f64 / secs,
+        "changes/s",
+        None,
+    );
+    for t in &w.tables {
+        lands(&e, "lake", t, Duration::from_secs(300)).await;
+    }
+    assert!(p99 <= STEADY_P99, "p99 lag {p99} s over {STEADY_P99} s");
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// One table of `DATA_AVALANCHE_CHURN_KEYS` keys (1 000 000), its rows changed at random keys over
+/// the whole key space for `DATA_AVALANCHE_CHURN_MINUTES` (10): each MERGE touches most of the
+/// table's files. Its commit time and what each commit rewrites, measured.
+#[tokio::test]
+async fn merge_cost_under_random_key_churn() {
+    require_load!();
+    let w = Workload::new(1, knob("DATA_AVALANCHE_CHURN_KEYS", 1_000_000), rate());
+    let window = Duration::from_secs(60 * knob("DATA_AVALANCHE_CHURN_MINUTES", 10));
+    let e = E2e::new(
+        "load_churn",
+        &w.ddl(),
+        Options {
+            tables: ", snapshots: { mode: initial }",
+            defaults: true,
+            ..Options::default()
+        },
+    )
+    .await;
+    w.prefill(&e).await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    snapshotted(&e, &w.tables, Duration::from_secs(3600)).await;
+    let table = &w.tables[0];
+    let copied = e.log(table).await.len();
+    let commit = "data_avalanche_sink_commit_duration_seconds";
+    let (s0, c0) = histogram(&p, commit, &e.test).await;
+    let writer = w.start(&e);
+    tokio::time::sleep(window).await;
+    writer.stop().await;
+    lands(&e, "lake", table, Duration::from_secs(600)).await;
+    let (s1, c1) = histogram(&p, commit, &e.test).await;
+    let log = e.log(table).await;
+    let size = |actions: &[serde_json::Value], kind: &str| -> (f64, f64) {
+        actions
+            .iter()
+            .filter_map(|a| a.get(kind))
+            .fold((0.0, 0.0), |(n, b), f| {
+                (n + 1.0, b + f["size"].as_f64().unwrap_or(0.0))
+            })
+    };
+    let table_bytes: f64 = log[..copied].iter().map(|c| size(c, "add").1).sum();
+    let merges: Vec<_> = log[copied..].iter().collect();
+    let n = merges.len().max(1) as f64;
+    let written: f64 = merges.iter().map(|c| size(c, "add").1).sum();
+    let removed: f64 = merges.iter().map(|c| size(c, "remove").0).sum();
+    record(RUN, "churn_table_bytes", table_bytes, "B", None);
+    record(RUN, "churn_merges", merges.len() as f64, "", None);
+    record(
+        RUN,
+        "churn_commit_seconds_mean",
+        (s1 - s0) / (c1 - c0).max(1.0),
+        "s",
+        None,
+    );
+    record(RUN, "churn_bytes_written_per_merge", written / n, "B", None);
+    record(RUN, "churn_files_removed_per_merge", removed / n, "", None);
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// Three nodes at the product's defaults, two tables under load: the holder is killed, and a row
+/// written just after must land, and the new holder's lag be under 60 s, within 2 minutes.
+#[tokio::test]
+async fn a_failover_recovers_within_two_minutes() {
+    require_load!();
+    let w = Workload::new(2, knob("DATA_AVALANCHE_LOAD_KEYS", 100_000), rate());
+    let ddl = format!(
+        "{} CREATE TABLE sentinel (id bigint PRIMARY KEY, doc text);",
+        w.ddl()
+    );
+    let mut c = Cluster::with(
+        "load_failover",
+        &ddl,
+        "",
+        Options {
+            tables: ", snapshots: { mode: initial }",
+            defaults: true,
+            ..Options::default()
+        },
+    )
+    .await;
+    w.prefill(&c.e).await;
+    c.start_all().await;
+    snapshotted(&c.e, &w.tables, Duration::from_secs(1800)).await;
+    let writer = w.start(&c.e);
+    tokio::time::sleep(WARM_UP).await;
+    let test = c.e.test.clone();
+    let (victim, epoch) = c.holders().await[&test].clone();
+    let i = c.index(&victim);
+    let t0 = Instant::now();
+    assert!(!c.nodes[i].proc.take().unwrap().kill().await.success());
+    c.e.exec("INSERT INTO sentinel VALUES (1, 'after the kill')")
+        .await;
+    eventually("the sentinel lands", Duration::from_secs(600), async || {
+        c.e.ids("sentinel").await == [1]
+    })
+    .await;
+    let landed = t0.elapsed();
+    let recovered = loop {
+        if let Some((holder, e)) = c.holders().await.get(&test).cloned()
+            && holder != victim
+            && e > epoch
+            && lag(c.proc(&holder), &test)
+                .await
+                .is_some_and(|l| l <= STEADY_P99)
+        {
+            break t0.elapsed();
+        }
+        assert!(t0.elapsed() < Duration::from_secs(600), "no recovery");
+        tokio::time::sleep(Duration::from_secs(1)).await;
+    };
+    writer.stop().await;
+    record(RUN, "failover_landed", landed.as_secs_f64(), "s", None);
+    record(
+        RUN,
+        "failover_recovered",
+        recovered.as_secs_f64(),
+        "s",
+        Some(("<= 120", recovered <= FAILOVER)),
+    );
+    c.settled().await;
+    for t in &w.tables {
+        lands(&c.e, "lake", t, Duration::from_secs(300)).await;
+    }
+    assert!(recovered <= FAILOVER, "recovered after {recovered:?}");
+    c.stop().await;
+}
diff --git a/crates/data-avalanche/tests/load/chaos.rs b/crates/data-avalanche/tests/load/chaos.rs
new file mode 100644
index 0000000..eda1046
--- /dev/null
+++ b/crates/data-avalanche/tests/load/chaos.rs
@@ -0,0 +1,210 @@
+//! Chaos under load, at the product's defaults (roadmap §4 M12c): a storage outage shorter than
+//! the lease and one longer, a restart of the source's server (Q-M12c-3: `docker compose
+//! restart`, in this opt-in run only), and a partition of the holder from the other nodes. Each
+//! ends with the lake holding the source's rows, each key once, its owner epochs never going
+//! back.
+
+use std::time::{Duration, Instant};
+
+use data_avalanche_testkit::endpoints::RUSTFS_URL;
+
+use crate::harness::{Db, E2e, Options, Store};
+use crate::nodes::Cluster;
+use crate::proxy::Proxy;
+use crate::report::{knob, record};
+use crate::workload::Workload;
+use crate::{lands, require_load, snapshotted};
+
+const RUN: &str = "chaos";
+/// The product's `lease_duration`.
+const LEASE: Duration = Duration::from_secs(15);
+/// Under load before the fault, and after it before the writer stops.
+const AROUND: Duration = Duration::from_secs(30);
+
+/// A quarter of the bench's rate, over two tables of 20 000 keys.
+fn workload() -> Workload {
+    let rate = u32::try_from(knob("DATA_AVALANCHE_LOAD_RATE", 1000) / 4).expect("a rate");
+    Workload::new(2, 20_000, rate)
+}
+
+fn options() -> Options<'static> {
+    Options {
+        tables: ", snapshots: { mode: initial }",
+        defaults: true,
+        ..Options::default()
+    }
+}
+
+/// A port nothing listens on.
+fn dead_port() -> u16 {
+    std::net::TcpListener::bind("127.0.0.1:0")
+        .expect("a port")
+        .local_addr()
+        .expect("its address")
+        .port()
+}
+
+/// The node reaches RustFS through a proxy, which refuses every connection for `outage` under
+/// load; what the lease's epoch was before and after.
+async fn storage_outage(test: &str, outage: Duration) -> Option<(u64, u64)> {
+    let w = workload();
+    let e = E2e::new(test, &w.ddl(), options()).await;
+    if e.store != Store::RustFs {
+        eprintln!("skipped: the storage outage runs on RustFS");
+        return None;
+    }
+    let rustfs: u16 = RUSTFS_URL
+        .rsplit(':')
+        .next()
+        .and_then(|p| p.parse().ok())
+        .expect("RustFS's port");
+    let proxy = Proxy::tcp(rustfs).await;
+    let endpoint = format!("http://127.0.0.1:{}", proxy.port());
+    w.prefill(&e).await;
+    let mut p = e.start(&[("AWS_ENDPOINT_URL", &endpoint)]);
+    p.ready().await;
+    snapshotted(&e, &w.tables, Duration::from_secs(600)).await;
+    let writer = w.start(&e);
+    tokio::time::sleep(AROUND).await;
+    let before = e.lease().await.expect("a lease").doc.owner_epoch;
+    proxy.switch(dead_port());
+    tokio::time::sleep(outage).await;
+    proxy.switch(rustfs);
+    let back = Instant::now();
+    p.metric(&e.status("running")).await;
+    record(
+        RUN,
+        &format!("{test}_running_again"),
+        back.elapsed().as_secs_f64(),
+        "s",
+        None,
+    );
+    tokio::time::sleep(AROUND).await;
+    writer.stop().await;
+    for t in &w.tables {
+        lands(&e, "lake", t, Duration::from_secs(300)).await;
+    }
+    let after = e.lease().await.expect("a lease").doc.owner_epoch;
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+    Some((before, after))
+}
+
+/// Shorter than the lease: the node keeps its lease (the same epoch) and catches up.
+#[tokio::test]
+async fn a_storage_outage_shorter_than_the_lease_loses_nothing() {
+    require_load!();
+    let outage = LEASE / 3;
+    let Some((before, after)) = storage_outage("chaos_store_short", outage).await else {
+        return;
+    };
+    record(
+        RUN,
+        "short_outage_epochs",
+        (after - before) as f64,
+        "",
+        None,
+    );
+    assert_eq!(after, before, "an outage of {outage:?} keeps the lease");
+}
+
+/// Longer than the lease: the node stops writing when its lease runs out, and claims it again,
+/// at the next epoch, once the store answers.
+#[tokio::test]
+async fn a_storage_outage_longer_than_the_lease_loses_nothing() {
+    require_load!();
+    let outage = LEASE * 3;
+    let Some((before, after)) = storage_outage("chaos_store_long", outage).await else {
+        return;
+    };
+    record(RUN, "long_outage_epochs", (after - before) as f64, "", None);
+    assert!(after > before, "an outage of {outage:?} loses the lease");
+}
+
+/// The source's server restarts under load (`docker compose restart`): the stream reconnects and
+/// the writer's committed changes all land.
+#[tokio::test]
+async fn a_source_restart_loses_nothing() {
+    require_load!();
+    let w = workload();
+    let mut e = E2e::new("chaos_source", &w.ddl(), options()).await;
+    w.prefill(&e).await;
+    let mut p = e.start(&[]);
+    p.ready().await;
+    snapshotted(&e, &w.tables, Duration::from_secs(600)).await;
+    let writer = w.start(&e);
+    tokio::time::sleep(AROUND).await;
+    let compose = concat!(env!("CARGO_MANIFEST_DIR"), "/../../test-infra/compose.yaml");
+    let t = Instant::now();
+    let status = std::process::Command::new("docker")
+        .args(["compose", "-f", compose, "restart", &e.source.service()])
+        .status()
+        .expect("docker runs");
+    assert!(status.success(), "the restart: {status}");
+    // The harness's own connection went with the server; the restart returns before the server
+    // takes connections again.
+    e.db = loop {
+        match Db::try_connect(e.source).await {
+            Ok(db) => break db,
+            Err(why) => {
+                assert!(
+                    t.elapsed() < Duration::from_secs(120),
+                    "the server is back: {why}"
+                );
+                tokio::time::sleep(Duration::from_secs(1)).await;
+            }
+        }
+    };
+    record(RUN, "source_restart", t.elapsed().as_secs_f64(), "s", None);
+    p.metric(&e.status("running")).await;
+    e.streaming().await;
+    tokio::time::sleep(AROUND).await;
+    let counts = writer.stop().await;
+    record(
+        RUN,
+        "source_restart_failed_statements",
+        counts.failed as f64,
+        "",
+        None,
+    );
+    for t in &w.tables {
+        lands(&e, "lake", t, Duration::from_secs(300)).await;
+    }
+    assert!(p.terminate().await.success());
+    e.cleanup().await;
+}
+
+/// Three nodes under load; the holder is cut off from the other two for four leases: the store
+/// still answers it, so it keeps the pipeline (the same holder and epoch), and nothing is lost.
+#[tokio::test]
+async fn a_partition_under_load_loses_nothing() {
+    require_load!();
+    let w = workload();
+    let mut c = Cluster::with("chaos_split", &w.ddl(), "", options()).await;
+    w.prefill(&c.e).await;
+    c.start_all().await;
+    snapshotted(&c.e, &w.tables, Duration::from_secs(600)).await;
+    let writer = w.start(&c.e);
+    tokio::time::sleep(AROUND).await;
+    let test = c.e.test.clone();
+    let before = c.holders().await[&test].clone();
+    c.cut(&[before.0.as_str()]);
+    tokio::time::sleep(LEASE * 4).await;
+    c.heal();
+    c.settled().await;
+    tokio::time::sleep(AROUND).await;
+    writer.stop().await;
+    let after = c.holders().await[&test].clone();
+    record(
+        RUN,
+        "partition_epochs",
+        (after.1 - before.1) as f64,
+        "",
+        None,
+    );
+    for t in &w.tables {
+        lands(&c.e, "lake", t, Duration::from_secs(300)).await;
+    }
+    assert_eq!(after, before, "the partition moved the pipeline");
+    c.stop().await;
+}
diff --git a/crates/data-avalanche/tests/load/main.rs b/crates/data-avalanche/tests/load/main.rs
new file mode 100644
index 0000000..1dfff18
--- /dev/null
+++ b/crates/data-avalanche/tests/load/main.rs
@@ -0,0 +1,104 @@
+//! The v1 acceptance runs (roadmap §4 M12c and §8; Q-M12-3, Q-M12c-2, Q-M12c-3), on test-infra:
+//! D-V1.3's lag targets measured at the product's defaults, in steady state and across a node's
+//! loss, and MERGE's cost under random-key churn (`bench`); a storage outage shorter and longer
+//! than the lease, a source restart and a network partition, under load (`chaos`); a long soak
+//! with churn (`soak`). Each ends with the lake holding the source's rows, each key once, and its
+//! owner epochs never going back. The binary is the E2E suite's, built in release mode by
+//! `cargo xtask bench|chaos|soak`, which writes the measurements to `target/acceptance/`.
+//!
+//! Needs `DATA_AVALANCHE_LOAD=1` beside `DATA_AVALANCHE_IT=1`, and the `e2e-hooks` feature:
+//!
+//! `cargo test --release -p data-avalanche --features e2e-hooks --test load -- --test-threads=1`
+
+#[allow(dead_code)]
+#[path = "../e2e/forwarder.rs"]
+mod forwarder;
+#[allow(dead_code)]
+#[path = "../e2e/harness.rs"]
+mod harness;
+#[allow(dead_code)]
+#[path = "../e2e/nodes.rs"]
+mod nodes;
+#[allow(dead_code)]
+#[path = "../e2e/proxy.rs"]
+mod proxy;
+#[allow(dead_code, unused_macros)]
+#[path = "../e2e/source.rs"]
+mod source;
+#[allow(dead_code)]
+#[path = "../e2e/store.rs"]
+mod store;
+
+mod bench;
+mod chaos;
+mod report;
+mod soak;
+mod workload;
+
+use std::time::{Duration, Instant};
+
+use harness::E2e;
+
+/// Returns early, with a line saying so, unless `DATA_AVALANCHE_LOAD` is set and the ITs are on:
+/// the runs take minutes to hours, and need test-infra.
+#[macro_export]
+macro_rules! require_load {
+    () => {
+        data_avalanche_testkit::require_it!();
+        if std::env::var("DATA_AVALANCHE_LOAD").map_or(true, |v| v.is_empty()) {
+            eprintln!("skipped: set DATA_AVALANCHE_LOAD=1 (cargo xtask bench|chaos|soak)");
+            return;
+        }
+    };
+}
+
+/// Waits until every one of `tables` finished its snapshot in the test's pipeline, up to
+/// `within`: a prefilled table's copy outlasts the suite's minute.
+pub(crate) async fn snapshotted(e: &E2e, tables: &[String], within: Duration) {
+    let until = Instant::now() + within;
+    for t in tables {
+        loop {
+            if e.snapshot(t)
+                .await
+                .is_some_and(|s| s.completed_at.is_some())
+            {
+                break;
+            }
+            assert!(Instant::now() < until, "{t}'s snapshot within {within:?}");
+            tokio::time::sleep(Duration::from_secs(1)).await;
+        }
+    }
+}
+
+/// The owner epochs of `lake`'s `table` in commit order never go back (a zombie's write after a
+/// takeover would), and some commit carries one.
+pub(crate) async fn epochs_never_go_back(e: &E2e, lake: &str, table: &str) {
+    let mut epochs = vec![];
+    for actions in e.log_in(lake, table).await {
+        let info = actions
+            .iter()
+            .find(|a| a.get("commitInfo").is_some())
+            .expect("a commit has its commitInfo");
+        if let Some(epoch) = info["commitInfo"]["data-avalanche.owner_epoch"].as_u64() {
+            epochs.push(epoch);
+        }
+    }
+    assert!(
+        !epochs.is_empty(),
+        "{lake}/{table}: no epoch in its commits"
+    );
+    assert!(
+        epochs.is_sorted(),
+        "{lake}/{table}: an epoch went back: {epochs:?}"
+    );
+}
+
+/// The lake `lake`'s `table` holds the source's rows on [`workload::COLS`], each key once, within
+/// `within`: first the ids (cheap to compare while they move), then the rows.
+pub(crate) async fn lands(e: &E2e, lake: &str, table: &str, within: Duration) {
+    e.converged_in(lake, table, within).await;
+    if lake == "lake" {
+        e.mirrored(table, workload::COLS).await;
+    }
+    epochs_never_go_back(e, lake, table).await;
+}
diff --git a/crates/data-avalanche/tests/load/report.rs b/crates/data-avalanche/tests/load/report.rs
new file mode 100644
index 0000000..b1ee74e
--- /dev/null
+++ b/crates/data-avalanche/tests/load/report.rs
@@ -0,0 +1,126 @@
+//! What the runs measure, and where it goes: one JSON line per measurement in
+//! `<DATA_AVALANCHE_LOAD_OUT>/results.jsonl` (`cargo xtask` sets it to `target/acceptance`), and
+//! a line on stdout. The acceptance report (`doc/v1.0/v1-acceptance.md`) quotes them.
+
+use std::io::Write;
+use std::path::PathBuf;
+use std::time::Duration;
+
+use crate::harness::Proc;
+
+/// A run's knob: `name`'s value, else `default`.
+pub fn knob(name: &str, default: u64) -> u64 {
+    std::env::var(name)
+        .ok()
+        .filter(|v| !v.is_empty())
+        .map_or(default, |v| {
+            v.parse()
+                .unwrap_or_else(|_| panic!("{name}={v}: a whole number"))
+        })
+}
+
+/// Records `value` (in `unit`) of `run`'s `metric`; `target`, when there is one, and whether it
+/// was met.
+pub fn record(run: &str, metric: &str, value: f64, unit: &str, target: Option<(&str, bool)>) {
+    let line = serde_json::json!({
+        "run": run,
+        "metric": metric,
+        "value": value,
+        "unit": unit,
+        "target": target.map(|(t, _)| t),
+        "met": target.map(|(_, m)| m),
+        "at": chrono::Utc::now().to_rfc3339(),
+        "source": std::env::var("DATA_AVALANCHE_E2E_SOURCE").unwrap_or_else(|_| "postgres".into()),
+    });
+    println!("acceptance: {line}");
+    let dir = std::env::var("DATA_AVALANCHE_LOAD_OUT").map_or_else(
+        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/acceptance"),
+        PathBuf::from,
+    );
+    std::fs::create_dir_all(&dir).expect("the results directory");
+    let mut f = std::fs::OpenOptions::new()
+        .create(true)
+        .append(true)
+        .open(dir.join("results.jsonl"))
+        .expect("the results file");
+    writeln!(f, "{line}").expect("a result written");
+}
+
+/// `pipeline`'s `data_avalanche_replication_lag_seconds` on `p`, if `p` runs it.
+pub async fn lag(p: &Proc, pipeline: &str) -> Option<f64> {
+    let label = format!("pipeline=\"{pipeline}\"");
+    p.metrics().await.lines().find_map(|l| {
+        let rest = l.strip_prefix("data_avalanche_replication_lag_seconds{")?;
+        let (labels, value) = rest.split_once("} ")?;
+        labels
+            .split(',')
+            .any(|kv| kv == label)
+            .then(|| value.trim().parse().ok())?
+    })
+}
+
+/// The sum and the count of a histogram of `pipeline` on `p` (`<name>_sum`, `<name>_count`).
+pub async fn histogram(p: &Proc, name: &str, pipeline: &str) -> (f64, f64) {
+    let label = format!("pipeline=\"{pipeline}\"");
+    let text = p.metrics().await;
+    let pick = |suffix: &str| {
+        text.lines()
+            .filter_map(|l| {
+                let rest = l.strip_prefix(&format!("{name}_{suffix}{{"))?;
+                let (labels, value) = rest.split_once("} ")?;
+                labels
+                    .split(',')
+                    .any(|kv| kv == label)
+                    .then(|| value.trim().parse::<f64>().ok())?
+            })
+            .sum::<f64>()
+    };
+    (pick("sum"), pick("count"))
+}
+
+/// Lag samples, one a second.
+#[derive(Debug, Default)]
+pub struct Samples(pub Vec<f64>);
+
+impl Samples {
+    /// The `q` quantile (0..=1), nearest rank.
+    pub fn quantile(&self, q: f64) -> f64 {
+        let mut v = self.0.clone();
+        v.sort_by(f64::total_cmp);
+        if v.is_empty() {
+            return f64::NAN;
+        }
+        let rank = (q * v.len() as f64).ceil() as usize;
+        v[rank.clamp(1, v.len()) - 1]
+    }
+
+    /// Samples `pipeline`'s lag on `p` every second for `window`; a second the metric is missing
+    /// (no worker yet) isn't a sample.
+    pub async fn take(p: &Proc, pipeline: &str, window: Duration) -> Self {
+        let mut s = Self::default();
+        let mut tick = tokio::time::interval(Duration::from_secs(1));
+        let until = tokio::time::Instant::now() + window;
+        while tokio::time::Instant::now() < until {
+            tick.tick().await;
+            if let Some(v) = lag(p, pipeline).await {
+                s.0.push(v);
+            }
+        }
+        s
+    }
+}
+
+#[cfg(test)]
+mod tests {
+    use super::Samples;
+
+    #[test]
+    fn the_quantile_is_the_nearest_rank() {
+        let s = Samples((1..=100).map(f64::from).collect());
+        assert_eq!(s.quantile(0.99), 99.0);
+        assert_eq!(s.quantile(0.5), 50.0);
+        assert_eq!(s.quantile(1.0), 100.0);
+        assert_eq!(Samples(vec![7.0]).quantile(0.99), 7.0);
+        assert!(Samples::default().quantile(0.99).is_nan());
+    }
+}
diff --git a/crates/data-avalanche/tests/load/soak.rs b/crates/data-avalanche/tests/load/soak.rs
new file mode 100644
index 0000000..3622531
--- /dev/null
+++ b/crates/data-avalanche/tests/load/soak.rs
@@ -0,0 +1,171 @@
+//! The soak (roadmap §4 M12c: 24 h with churn): three nodes, three pipelines over three tables,
+//! the writer at a fifth of the bench's rate, and every `DATA_AVALANCHE_SOAK_CHURN_MINUTES` (10)
+//! one change in turn: a column added, the test's pipeline paused and resumed, a node's leave and
+//! restart, a snapshot job, a node's `kill -9` and restart. Every
+//! `DATA_AVALANCHE_SOAK_CHECK_MINUTES` (60) the writer stops, every lake must hold the source's
+//! rows, and `n1`'s resident memory is read: `n1` is never restarted, so its memory over the run is
+//! the leak check (at most twice the first reading).
+
+use std::time::{Duration, Instant};
+
+use crate::harness::{Options, eventually};
+use crate::nodes::{Cluster, lake};
+use crate::report::{knob, record};
+use crate::workload::Workload;
+use crate::{lands, require_load, snapshotted};
+
+const RUN: &str = "soak";
+
+/// The test's pipeline's two peers over the same tables, at the product's defaults.
+const PEERS: &str = r#"  b:
+    id: b
+@SOURCE(_b)@
+    tables: { include: ['@INCLUDE@'], snapshots: { mode: initial } }
+    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_b, delta: {} }
+  c:
+    id: c
+@SOURCE(_c)@
+    tables: { include: ['@INCLUDE@'], snapshots: { mode: initial } }
+    sink: { type: delta, bucket: @BUCKET@, prefix: @PREFIX@/lake_c, delta: {} }
+"#;
+
+/// `pid`'s resident memory, in KiB (`ps`).
+fn rss(pid: u32) -> u64 {
+    let out = std::process::Command::new("ps")
+        .args(["-o", "rss=", "-p", &pid.to_string()])
+        .output()
+        .expect("ps runs");
+    String::from_utf8_lossy(&out.stdout)
+        .trim()
+        .parse()
+        .expect("a resident size")
+}
+
+/// The table a request or a key names: the source's per-table config key.
+fn key(c: &Cluster, table: &str) -> String {
+    if c.e.source.is_postgres() {
+        format!("{}.{table}", c.e.schema)
+    } else {
+        table.to_owned()
+    }
+}
+
+/// Churn `step`, in turn of five.
+async fn churn(c: &mut Cluster, w: &Workload, step: usize) {
+    let test = c.e.test.clone();
+    match step % 5 {
+        0 => {
+            c.e.exec(&format!(
+                "ALTER TABLE {} ADD COLUMN extra_{step} text",
+                w.tables[step % w.tables.len()]
+            ))
+            .await;
+        }
+        1 => {
+            let n1 = c.proc("n1");
+            assert_eq!(n1.ask(&test, "pause").await.0, 202);
+            n1.shows(&test, "paused").await;
+            assert_eq!(n1.ask(&test, "resume").await.0, 202);
+            n1.shows(&test, "running").await;
+        }
+        2 | 4 => {
+            // n2 and n3 in turn; n1 stays up throughout (the memory check's node).
+            let i = 1 + (step / 5) % 2;
+            let p = c.nodes[i].proc.take().expect("a live node");
+            if step % 5 == 2 {
+                assert!(p.terminate().await.success(), "a leave");
+            } else {
+                assert!(!p.kill().await.success(), "a kill");
+            }
+            c.start(i).await;
+            c.settled().await;
+        }
+        _ => {
+            let body = serde_json::json!({ "tables": [key(c, &w.tables[0])] }).to_string();
+            let path = format!("/v1/pipelines/{test}/snapshots");
+            let n1 = c.proc("n1");
+            let (code, _, text) = n1
+                .send(None, "POST", &path, &body)
+                .await
+                .expect("the API answers");
+            assert_eq!(code, 202, "{text}");
+            let job: serde_json::Value = serde_json::from_str(&text).expect("a job");
+            let id = job["job_id"].as_str().expect("its id").to_owned();
+            eventually("the job ends", Duration::from_secs(1800), async || {
+                n1.call("GET", &format!("/v1/snapshots/{id}")).await.1["status"] == "completed"
+            })
+            .await;
+        }
+    }
+}
+
+/// The writer stopped: every pipeline's lake holds the source's rows.
+async fn check(c: &Cluster, w: &Workload) {
+    c.settled().await;
+    for id in c.ids.clone() {
+        for t in &w.tables {
+            lands(&c.e, &lake(&c.e.test, &id), t, Duration::from_secs(600)).await;
+        }
+    }
+}
+
+#[tokio::test]
+async fn a_day_of_churn_loses_nothing() {
+    require_load!();
+    let total = Duration::from_secs(60 * knob("DATA_AVALANCHE_SOAK_MINUTES", 24 * 60));
+    let churn_every = Duration::from_secs(60 * knob("DATA_AVALANCHE_SOAK_CHURN_MINUTES", 10));
+    let check_every = Duration::from_secs(60 * knob("DATA_AVALANCHE_SOAK_CHECK_MINUTES", 60));
+    let rate = u32::try_from(knob("DATA_AVALANCHE_LOAD_RATE", 1000) / 5).expect("a rate");
+    let w = Workload::new(3, 50_000, rate);
+    let mut c = Cluster::with(
+        "load_soak",
+        &w.ddl(),
+        PEERS,
+        Options {
+            tables: ", snapshots: { mode: initial }",
+            defaults: true,
+            ..Options::default()
+        },
+    )
+    .await;
+    w.prefill(&c.e).await;
+    c.start_all().await;
+    snapshotted(&c.e, &w.tables, Duration::from_secs(1800)).await;
+    let n1 = c.proc("n1").pid();
+    let mut memory = vec![];
+    let mut writer = w.start(&c.e);
+    let start = Instant::now();
+    let (mut next_churn, mut next_check) = (start + churn_every, start + check_every);
+    let mut step = 0;
+    let mut changes = 0;
+    loop {
+        let end = start + total;
+        let at = next_churn.min(next_check).min(end);
+        tokio::time::sleep_until(at.into()).await;
+        let now = Instant::now();
+        if now >= next_churn && now < end {
+            churn(&mut c, &w, step).await;
+            step += 1;
+            next_churn += churn_every;
+        }
+        if now >= next_check || now >= end {
+            changes += writer.stop().await.changes;
+            check(&c, &w).await;
+            let kib = rss(n1);
+            memory.push(kib);
+            record(RUN, "n1_rss", kib as f64 * 1024.0, "B", None);
+            if now >= end {
+                break;
+            }
+            writer = w.start(&c.e);
+            next_check += check_every;
+        }
+    }
+    record(RUN, "churn_steps", step as f64, "", None);
+    record(RUN, "changes", changes as f64, "", None);
+    let (first, last) = (memory[0], *memory.last().expect("a reading"));
+    let grew = last as f64 / first as f64;
+    record(RUN, "n1_rss_growth", grew, "x", Some(("<= 2", grew <= 2.0)));
+    assert!(grew <= 2.0, "n1's memory {first} KiB → {last} KiB");
+    c.stop().await;
+}
diff --git a/crates/data-avalanche/tests/load/workload.rs b/crates/data-avalanche/tests/load/workload.rs
new file mode 100644
index 0000000..501313e
--- /dev/null
+++ b/crates/data-avalanche/tests/load/workload.rs
@@ -0,0 +1,190 @@
+//! The load: `load_<i>` tables of `(id, doc, n)`, filled with keys `1..=keys`, then changed at a
+//! steady rate by one writer on a connection of its own. Every tenth of a second it commits one
+//! transaction on one table, chosen in turn: an upsert, an update or a delete of a batch of
+//! random keys (6 : 3 : 1), `rate / 10` changes in all. A failed statement (the source restarting)
+//! is counted, and the writer reconnects and goes on.
+
+use std::sync::Arc;
+use std::sync::atomic::{AtomicBool, Ordering};
+use std::time::{Duration, Instant};
+
+use crate::harness::{Db, E2e, Source};
+
+/// The columns the runs compare, source against lake.
+pub const COLS: &[&str] = &["id", "doc", "n"];
+
+/// What one run writes.
+#[derive(Debug, Clone)]
+pub struct Workload {
+    pub tables: Vec<String>,
+    /// Keys `1..=keys` per table: the prefill, and the space the changes pick from.
+    pub keys: u64,
+    /// Changes a second, over all tables.
+    pub rate: u32,
+}
+
+/// What a writer did.
+#[derive(Debug, Clone, Copy, Default)]
+pub struct Counts {
+    pub transactions: u64,
+    pub changes: u64,
+    /// Statements that failed (a restarting source): each lost, the writer reconnected.
+    pub failed: u64,
+}
+
+impl Workload {
+    /// `tables` tables `load_0…`, each `keys` keys, changed at `rate` a second.
+    pub fn new(tables: usize, keys: u64, rate: u32) -> Self {
+        Self {
+            tables: (0..tables).map(|i| format!("load_{i}")).collect(),
+            keys,
+            rate,
+        }
+    }
+
+    /// The tables' DDL.
+    pub fn ddl(&self) -> String {
+        self.tables
+            .iter()
+            .map(|t| format!("CREATE TABLE {t} (id bigint PRIMARY KEY, doc text, n bigint);"))
+            .collect()
+    }
+
+    /// Fills every table with its keys, `n` 0, in batches of 100 000.
+    pub async fn prefill(&self, e: &E2e) {
+        for t in &self.tables {
+            let mut from = 1;
+            while from <= self.keys {
+                let to = (from + 99_999).min(self.keys);
+                e.exec(&fill(e.source, t, from, to)).await;
+                from = to + 1;
+            }
+        }
+    }
+
+    /// Starts the writer.
+    pub fn start(&self, e: &E2e) -> Writer {
+        let stop = Arc::new(AtomicBool::new(false));
+        let task = tokio::spawn(write(
+            e.source,
+            e.schema.clone(),
+            self.clone(),
+            Arc::clone(&stop),
+        ));
+        Writer { stop, task }
+    }
+}
+
+/// Keys `from..=to` of `table`, in one statement.
+fn fill(source: Source, table: &str, from: u64, to: u64) -> String {
+    match source {
+        Source::Postgres { .. } => format!(
+            "INSERT INTO {table} SELECT g, 'row ' || g, 0 FROM generate_series({from}, {to}) g"
+        ),
+        Source::Mysql(t) => {
+            let var = if t.mariadb {
+                "max_recursive_iterations"
+            } else {
+                "cte_max_recursion_depth"
+            };
+            format!(
+                "SET SESSION {var} = {n}; INSERT INTO {table} (id, doc, n) WITH RECURSIVE s (g) AS
+                 (SELECT {from} UNION ALL SELECT g + 1 FROM s WHERE g < {to})
+                 SELECT g, CONCAT('row ', g), 0 FROM s",
+                n = to - from + 1
+            )
+        }
+    }
+}
+
+/// The writer, running until [`Writer::stop`].
+pub struct Writer {
+    stop: Arc<AtomicBool>,
+    task: tokio::task::JoinHandle<Counts>,
+}
+
+impl Writer {
+    /// Stops it after its transaction in flight; what it did.
+    pub async fn stop(self) -> Counts {
+        self.stop.store(true, Ordering::SeqCst);
+        self.task.await.expect("the writer")
+    }
+}
+
+async fn write(source: Source, schema: String, w: Workload, stop: Arc<AtomicBool>) -> Counts {
+    let mut db = Db::connect(source).await;
+    let mut counts = Counts::default();
+    let batch = (w.rate / 10).max(1) as u64;
+    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
+    let mut next = |bound: u64| {
+        x ^= x << 13;
+        x ^= x >> 7;
+        x ^= x << 17;
+        x % bound
+    };
+    let mut tick = tokio::time::interval(Duration::from_millis(100));
+    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
+    let mut turn = 0usize;
+    while !stop.load(Ordering::SeqCst) {
+        tick.tick().await;
+        let table = format!("{schema}.{}", w.tables[turn % w.tables.len()]);
+        turn += 1;
+        // Each key once: PostgreSQL's upsert refuses a key twice in one statement.
+        let keys: Vec<u64> = (0..batch)
+            .map(|_| next(w.keys) + 1)
+            .collect::<std::collections::BTreeSet<_>>()
+            .into_iter()
+            .collect();
+        let list = keys
+            .iter()
+            .map(u64::to_string)
+            .collect::<Vec<_>>()
+            .join(", ");
+        let op = turn as u64;
+        let sql = match next(10) {
+            0..=5 => upsert(source, &table, &keys, op),
+            6..=8 => format!("UPDATE {table} SET doc = 'u{op}', n = n + 1 WHERE id IN ({list})"),
+            _ => format!("DELETE FROM {table} WHERE id IN ({list})"),
+        };
+        let at = Instant::now();
+        match db.exec(&sql).await {
+            Ok(()) => {
+                counts.transactions += 1;
+                counts.changes += keys.len() as u64;
+            }
+            Err(e) => {
+                counts.failed += 1;
+                eprintln!("load: a statement failed after {:?}: {e}", at.elapsed());
+                tokio::time::sleep(Duration::from_secs(1)).await;
+                if let Ok(again) = Db::try_connect(source).await {
+                    db = again;
+                }
+            }
+        }
+    }
+    counts
+}
+
+/// One transaction: `keys` inserted, or their `doc` and `n` changed where they exist.
+fn upsert(source: Source, table: &str, keys: &[u64], op: u64) -> String {
+    let rows = keys
+        .iter()
+        .map(|k| format!("({k}, 'i{op}', 0)"))
+        .collect::<Vec<_>>()
+        .join(", ");
+    match source {
+        Source::Postgres { .. } => format!(
+            "INSERT INTO {table} (id, doc, n) VALUES {rows}
+             ON CONFLICT (id) DO UPDATE SET doc = excluded.doc, n = {table}.n + 1"
+        ),
+        // MariaDB has no row alias; MySQL 8.4 deprecates VALUES() for it.
+        Source::Mysql(t) if t.mariadb => format!(
+            "INSERT INTO {table} (id, doc, n) VALUES {rows}
+             ON DUPLICATE KEY UPDATE doc = VALUES(doc), n = {table}.n + 1"
+        ),
+        Source::Mysql(_) => format!(
+            "INSERT INTO {table} (id, doc, n) VALUES {rows} AS new
+             ON DUPLICATE KEY UPDATE doc = new.doc, n = {table}.n + 1"
+        ),
+    }
+}
diff --git a/xtask/Cargo.toml b/xtask/Cargo.toml
index f9cd7ce..0e6efe2 100644
--- a/xtask/Cargo.toml
+++ b/xtask/Cargo.toml
@@ -4,7 +4,7 @@ version.workspace = true
 edition.workspace = true
 rust-version.workspace = true
 publish.workspace = true
-description = "Repository automation: the documentation site's generated pages (cargo xtask docs)."
+description = "Repository automation: the documentation site's generated pages (cargo xtask docs) and the acceptance runs (cargo xtask bench|chaos|soak)."
 
 [lints]
 workspace = true
diff --git a/xtask/src/acceptance.rs b/xtask/src/acceptance.rs
new file mode 100644
index 0000000..453d9f9
--- /dev/null
+++ b/xtask/src/acceptance.rs
@@ -0,0 +1,75 @@
+//! `cargo xtask bench|chaos|soak` (roadmap §5.1, Q-M12c-2): the acceptance runs of
+//! `crates/data-avalanche/tests/load`, one module each, with the binary built in release mode, the
+//! gates set, and the measurements written to `target/acceptance/results.jsonl`. The runs' knobs
+//! (`DATA_AVALANCHE_LOAD_RATE`, `…_MINUTES`, `DATA_AVALANCHE_SOAK_MINUTES`, …) pass through from
+//! the caller's environment.
+
+use std::path::Path;
+
+/// The acceptance runs, as `cargo xtask` names them and as the load target's modules are named.
+pub const RUNS: [&str; 3] = ["bench", "chaos", "soak"];
+
+/// One run's `cargo` invocation.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct Invocation {
+    pub args: Vec<String>,
+    /// Set unless the caller's environment already sets it.
+    pub env: Vec<(String, String)>,
+}
+
+/// `run`'s invocation from the repository at `root`; `None` for a name that isn't a run.
+pub fn invocation(run: &str, root: &Path) -> Option<Invocation> {
+    if !RUNS.contains(&run) {
+        return None;
+    }
+    let args = [
+        "test",
+        "--release",
+        "--locked",
+        "-p",
+        "data-avalanche",
+        "--features",
+        "e2e-hooks",
+        "--test",
+        "load",
+        "--",
+        "--test-threads=1",
+        "--nocapture",
+    ]
+    .into_iter()
+    .map(str::to_owned)
+    .chain([format!("{run}::")])
+    .collect();
+    let out = root.join("target/acceptance");
+    let env = [
+        ("DATA_AVALANCHE_IT", "1".to_owned()),
+        ("DATA_AVALANCHE_LOAD", "1".to_owned()),
+        ("DATA_AVALANCHE_SKIP_GCS", "1".to_owned()),
+        ("DATA_AVALANCHE_LOAD_OUT", out.display().to_string()),
+    ]
+    .into_iter()
+    .map(|(k, v)| (k.to_owned(), v))
+    .collect();
+    Some(Invocation { args, env })
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+
+    #[test]
+    fn a_run_is_its_module_of_the_load_target_in_release_mode() {
+        let i = invocation("chaos", Path::new("/repo")).unwrap();
+        assert_eq!(
+            i.args.join(" "),
+            "test --release --locked -p data-avalanche --features e2e-hooks --test load -- \
+             --test-threads=1 --nocapture chaos::"
+        );
+        assert!(i.env.contains(&("DATA_AVALANCHE_LOAD".into(), "1".into())));
+        assert!(i.env.contains(&(
+            "DATA_AVALANCHE_LOAD_OUT".into(),
+            "/repo/target/acceptance".into()
+        )));
+        assert_eq!(invocation("docs", Path::new("/repo")), None);
+    }
+}
diff --git a/xtask/src/lib.rs b/xtask/src/lib.rs
index 8413ce4..321e712 100644
--- a/xtask/src/lib.rs
+++ b/xtask/src/lib.rs
@@ -1,8 +1,10 @@
-//! Repository automation (roadmap §2): the documentation site's generated pages (M11b). The
+//! Repository automation (roadmap §2): the documentation site's generated pages (M11b), and the
+//! acceptance runs' invocations (M12c, [`invocation`]). The
 //! configuration reference comes from the config model (`config_keys`, `FIELD_DOCS`, `RULES`,
 //! `key_mutability`), the API reference from the OpenAPI document, so neither can drift from the
 //! code: `cargo xtask docs` writes them, and a workspace test fails while a committed copy is stale.
 
+mod acceptance;
 mod api;
 mod config;
 mod examples;
@@ -10,6 +12,7 @@ mod rules;
 
 use std::path::{Path, PathBuf};
 
+pub use acceptance::{Invocation, RUNS, invocation};
 pub use examples::{ConfigExample, config_examples};
 
 /// The site's base path on GitHub Pages (`astro.config.mjs`'s `base`): every link between pages
diff --git a/xtask/src/main.rs b/xtask/src/main.rs
index 132f2a6..87a9d2b 100644
--- a/xtask/src/main.rs
+++ b/xtask/src/main.rs
@@ -1,15 +1,17 @@
 //! `cargo xtask docs` writes the documentation site's generated pages; `cargo xtask docs --check`
-//! exits 1, naming them, when a committed copy differs.
+//! exits 1, naming them, when a committed copy differs. `cargo xtask bench|chaos|soak` runs an
+//! acceptance run (M12c) and says where its measurements are.
 
-use std::process::ExitCode;
+use std::process::{Command, ExitCode};
 
 fn main() -> ExitCode {
     let args: Vec<String> = std::env::args().skip(1).collect();
     let check = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
         ["docs"] => false,
         ["docs", "--check"] => true,
+        [run] if xtask::RUNS.contains(&run) => return acceptance(run),
         _ => {
-            eprintln!("usage: cargo xtask docs [--check]");
+            eprintln!("usage: cargo xtask docs [--check] | bench | chaos | soak");
             return ExitCode::from(2);
         }
     };
@@ -37,3 +39,26 @@ fn main() -> ExitCode {
     }
     ExitCode::FAILURE
 }
+
+/// Runs `run` (`cargo xtask bench|chaos|soak`), its exit code the test run's.
+fn acceptance(run: &str) -> ExitCode {
+    let root = xtask::repo_root();
+    let i = xtask::invocation(run, &root).expect("a run");
+    let mut cmd = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()));
+    cmd.args(&i.args).current_dir(&root);
+    for (k, v) in &i.env {
+        if std::env::var_os(k).is_none() {
+            cmd.env(k, v);
+        }
+    }
+    let status = cmd.status().expect("cargo runs");
+    println!(
+        "the measurements: {}",
+        root.join("target/acceptance/results.jsonl").display()
+    );
+    if status.success() {
+        ExitCode::SUCCESS
+    } else {
+        ExitCode::FAILURE
+    }
+}
```

- [ ] **Step 4: Run them to see them pass, and each run short**

Run: `cargo test -p xtask` and `cargo clippy -p data-avalanche --features e2e-hooks --test load --test e2e -- -D warnings`.
Expected: PASS; clean.
Run the cluster scenarios again (`--test e2e cluster::`): PASS (7).
Each run once, short, in debug (`DATA_AVALANCHE_LOAD=1 DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1`,
`cargo test -p data-avalanche --features e2e-hooks --test load -- --test-threads=1 --nocapture <run>::`):
`bench::` with `DATA_AVALANCHE_LOAD_KEYS=10000 DATA_AVALANCHE_LOAD_MINUTES=2
DATA_AVALANCHE_CHURN_KEYS=20000 DATA_AVALANCHE_CHURN_MINUTES=2`; `chaos::`; `soak::` with
`DATA_AVALANCHE_SOAK_MINUTES=12 DATA_AVALANCHE_SOAK_CHURN_MINUTES=2 DATA_AVALANCHE_SOAK_CHECK_MINUTES=6`.
Expected: PASS each, in about 9, 9 and 13 minutes (§Measured in planning).

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche/tests crates/data-avalanche/Cargo.toml xtask
git commit -m "test(load): the v1 acceptance runs and cargo xtask bench|chaos|soak (Q-M12-3)"
```

### Task 7: The docs, the specs, the research and the tracker

**Files:**
- Modify: `doc/specs/Data-Flow.md`, `doc/specs/Schema_Change_Detection_and_Evolution.md` (P-1…P-3, approved)
- Modify: `docs/src/content/docs/runbooks/halted-table.md`, `docs/src/content/docs/runbooks/position-lost.mdx`
- Modify: `doc/research/Delivery_Guarantee_and_Position_Guard.md`, `doc/research/MySQL_DDL_Classifier.md`, `doc/research/Postgres_Slot_and_Publication_Lifecycle.md`
- Modify: `test-infra/README.md`, `doc/v1.0/README.md`
- Modify: `doc/analysis/CDC_Design_Gap_Analysis_v1.md`

- [ ] **Step 1: The specs (approved 2026-11-11).** Apply P-1, P-2 and P-3 as drafted in §Spec patches, and bump each
  spec's `updated:`.

- [ ] **Step 2: The pages, the research, the READMEs.**

````diff
diff --git a/doc/research/Delivery_Guarantee_and_Position_Guard.md b/doc/research/Delivery_Guarantee_and_Position_Guard.md
index 33bc80e..31a1878 100644
--- a/doc/research/Delivery_Guarantee_and_Position_Guard.md
+++ b/doc/research/Delivery_Guarantee_and_Position_Guard.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-11-10
+updated: 2026-11-11
 domain: Data Flow
 tags: [delivery-guarantee, exactly-once, gtid, lsn, idempotency, checkpoint, multi-table, reposition, source-change, cdc]
 resolves: [G-14, DA-X-12, DA-SRC-32, DA-SRC-34]
@@ -214,13 +214,15 @@ tracked, skipped the new server's changes up to it. The set is now
   foreign-format table, a key it can't take) records a `TableHalt` `SINK_REJECTED` ("the source
   changed, and its reposition fails: …") and the start goes on. Before, it ended every start. A
   transient error is retried; any other error ends the start.
-- **Known limitation (M-1).** The halt of a refused reposition of a table the pipeline doesn't
-  track is cleared at the next start (a halt of a table neither configured, cached, requested nor
-  copied goes), and the reposition is never retried: the checkpoint already names the new server.
-  Such a table, later tracked with `snapshots: never`, keeps the old server's marker. Holding the
-  checkpoint back until every reposition succeeds would make an always-refusing table fail every
-  start, the regression the halt closes. The operator docs say to drop or move such a Delta table
-  before tracking it.
+- **The halt stays while the lake holds the table (DA-SRC-37, M12c).** The reposition is never
+  retried: the checkpoint already names the new server, and holding it back until every reposition
+  succeeds would make an always-refusing table fail every start, the regression the halt closes.
+  So a later start keeps a refused reposition's halt (its detail begins `REPOSITION_REFUSED`) of a
+  table the pipeline doesn't track while the sink still holds it (`table_state(..).exists`), where
+  it used to clear the halt of every table neither configured, cached, requested nor copied. Tracked
+  later, the table stays halted rather than skipping the new server's changes up to its old marker;
+  a snapshot job's copy ends the halt, and so does the table leaving the lake (a dropped or moved
+  Delta table). Test: `a_refused_repositions_halt_stays_while_the_lake_holds_the_table`.
 - The same listing feeds `highest_marker` for DA-SRC-33's continuation
   (`Postgres_Slot_and_Publication_Lifecycle.md` §8).
 - Tests: `a_source_change_repositions_a_copy_the_pipeline_doesnt_track`,
@@ -237,7 +239,10 @@ again: a loop of recoveries. The controller counts them:
 - `WorkerStatus::started_from` is the checkpoint the worker's first stream session started from. A
   run **progressed** when its checkpoint at the end is present and differs from it.
 - A run that ends `SourceError::SourceChanged` after progress sets the count to 1; one without
-  progress adds 1. Any other end sets it to 0, and so does a stop (D-M12d.8).
+  progress adds 1, unless it ends more than `SOURCE_CHANGE_WINDOW` (10 minutes) after the change
+  counted before, which sets it to 1 too: an alternation reruns within the restart backoff, a
+  minute at most, and failovers further apart are failovers (DA-SRC-38, M12c). Any other end sets
+  it to 0, and so does a stop (D-M12d.8).
 - At `SOURCE_CHANGES_IN_A_ROW = 3` the pipeline goes to `ERROR` with the change's code (the prefix
   of the last `SourceChanged` message, `WAL_SOURCE_CHANGED` or `BINLOG_SOURCE_CHANGED`) and the
   message "<code>: 3 runs in a row found another source server than the run before, and none made
@@ -245,10 +250,9 @@ again: a loop of recoveries. The controller counts them:
   DNS name over several). Point the pipeline at one server, then stop and resume it. The last: …".
 - A single failover can't reach it: its reconnect makes the count at most 1, and the rerun's start
   either streams (the next end resets it), or refuses with a `Permanent` (0, a halt).
-- **Known limitations.** The count never decays with time: three genuine failovers on a source
-  that writes nothing in between (no durable checkpoint past each start) halt the pipeline, however
-  far apart. And a PostgreSQL reconnect to a promoted server of the same system asks the rerun with
-  a message that carries no code (DA-SRC-33, P-6), so an alternation of such reconnects halts
-  without one in the status.
+- A PostgreSQL reconnect to a promoted server of the same system asks the rerun with a message
+  prefixed `WAL_SOURCE_CHANGED` too (M12c; until then it carried no code, DA-SRC-38), so an
+  alternation of such reconnects halts with the code.
 - Tests: `runs_that_keep_finding_another_server_halt`,
-  `a_durable_checkpoint_starts_the_count_of_server_changes_again`.
+  `a_durable_checkpoint_starts_the_count_of_server_changes_again`,
+  `server_changes_far_apart_dont_halt`, `an_alternation_halts_with_the_code_its_change_names`.
diff --git a/doc/research/MySQL_DDL_Classifier.md b/doc/research/MySQL_DDL_Classifier.md
index 8b70075..983633a 100644
--- a/doc/research/MySQL_DDL_Classifier.md
+++ b/doc/research/MySQL_DDL_Classifier.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-11-10
+updated: 2026-11-11
 domain: Schema Evolution
 tags: [mysql, mariadb, ddl, sqlparser, classifier, channel-c, schema-evolution, rename, online-migration, case-folding, privileges, cdc]
 resolves: [G-19a, DA-SRC-25, DA-SRC-35, DA-SRC-36]
@@ -180,11 +180,14 @@ hold `a` beside `A` (DA-SINK-12). So the column keeps the spelling the pipeline
   otherwise than the cached schema (a `SCHEMA_MISMATCH`, or a panic on a recased key). The plan's
   column is now the cached name over the catalog's column; `scope` spells the identifier as the
   result's column. A Phase 0 baseline of a fold-only difference caches the sink's spelling.
-- **A limit, conservative (D-M12d.1).** The Phase 0 baseline of a sink table first met (DA-SNAP-22,
-  `missing_columns`) folds as Delta does and halts `SCHEMA_MISMATCH` on a sink column that matches a
-  source column by case alone; it doesn't consult `column_names_fold`. On MySQL that halts a table
-  whose column only changed case, which the cached path accepts. Safe (a halt, no loss); recorded
-  as a known limitation.
+- **The first-met baseline (DA-SNAP-23, M12c).** The Phase 0 baseline of a sink table first met
+  (DA-SNAP-22, `missing_columns`) folds as Delta does. Where `column_names_fold` holds (MySQL), a
+  sink column that matches a source column by case alone is that column: no add, and the session
+  writes it under the sink's spelling, in the key too, as the cached path keeps it. On PostgreSQL
+  the pair still halts `SCHEMA_MISMATCH`, naming both spellings. Until M12c it halted on MySQL too
+  (D-M12d.1, conservative). Tests: `where_names_fold_a_sink_column_matching_by_case_alone_is_the_column`,
+  `a_first_met_table_whose_names_changed_case_alone_is_baselined_where_names_fold`, E2E
+  `a_lost_state_meets_a_recased_column_as_the_same_column` (MySQL).
 - Tests: `a_change_of_case_alone_keeps_the_cached_name` (IT, MySQL 8.4 with and without GTIDs and MariaDB 11.8: a key
   column recased too, chunk size 1 with a resumed cursor), `a_rename_onto_a_dropped_name_keeps_its_new_spelling`,
   `a_change_of_case_alone_is_no_drift_where_names_fold`,
diff --git a/doc/research/Postgres_Slot_and_Publication_Lifecycle.md b/doc/research/Postgres_Slot_and_Publication_Lifecycle.md
index c573233..fcaeaa2 100644
--- a/doc/research/Postgres_Slot_and_Publication_Lifecycle.md
+++ b/doc/research/Postgres_Slot_and_Publication_Lifecycle.md
@@ -1,5 +1,5 @@
 ---
-updated: 2026-11-10
+updated: 2026-11-11
 domain: Data Flow
 tags: [postgres, replication-slot, publication, lifecycle, wal, re-snapshot, failover-slot, slot-sync, cdc]
 resolves: [G-15, DA-SRC-33, DA-SNAP-22]
@@ -163,7 +163,13 @@ shape. `slot.rs` `publication_shape` reads them from `pg_publication_rel` (`prqu
   below. Amended 2026-10-22 (M8a's whole-branch review): a holder ends such a reader at most once a
   lease term. Two holders that each believe they hold the lease (two lease stores on one slot) would
   otherwise end each other in turn, splitting the slot's changes between two lakes; with the limit,
-  the later busy spell gives up loudly with the bounded retry's error.
+  the later busy spell gives up loudly with the bounded retry's error. Amended 2026-11-11 (M12c,
+  DA-SRC-39): a session closes its stream on every exit, an error's too. A dropped
+  `pgwire-replication` client detaches its worker, which takes the slot again about 80 ms later
+  (measured in M12d's PostgreSQL 14 reconnect test), so the next session's open met the slot still
+  active (`55006`) and spent a step of the bounded retry. A worker task aborted outright (a lost
+  lease) still drops its session unclosed: its node no longer holds the lease, and the new holder's
+  retry, or its end of the stale reader, covers it.
 - **External (non-app) collisions cannot be detected.** PG replication slots carry **no ownership
   tag/comment**, so a slot created by an unrelated system under the same name is indistinguishable.
   Mitigation is V-058 naming uniqueness + operator discipline; documented as a known limitation, not
diff --git a/doc/v1.0/README.md b/doc/v1.0/README.md
index 946a763..dc9dcdf 100644
--- a/doc/v1.0/README.md
+++ b/doc/v1.0/README.md
@@ -70,6 +70,8 @@ This folder holds the plan for **building** v1. The design it implements is in `
 | `DATA_AVALANCHE_MYSQL=<target>` (from M7a) | The MySQL or MariaDB server and position mode of the MySQL source's integration tests, and of the E2E suite on MySQL (`test-infra/README.md`; default `mysql-84`) |
 | `DATA_AVALANCHE_E2E_SOURCE=postgres\|mysql` (from M7b) | The source the slice-1 E2E suite runs on (default `postgres`; `mysql` reads `DATA_AVALANCHE_MYSQL`) |
 | `DATA_AVALANCHE_PG_REPLICA=<primary port>:<replica port>` (from M12a) | The PostgreSQL failover E2E scenario on `test-infra`'s `replica` profile (`55420:55421`); unset, it is skipped |
+| `DATA_AVALANCHE_E2E_STORE=rustfs\|azurite\|s3\|gcs\|azure` (from M12c) | The store of an E2E run's state and lake (default `rustfs`); the cloud subset (`test-infra/cloud-e2e.txt`) on a real cloud |
+| `DATA_AVALANCHE_LOAD` (non-empty, from M12c) | Enables the acceptance runs (`--test load`), with `DATA_AVALANCHE_IT`; `cargo xtask bench\|chaos\|soak` sets both |
 
 CI secrets and variables are listed in `test-infra/README.md` ("CI requirements") and in the comments
 of `.github/workflows/ci.yml`.
diff --git a/docs/src/content/docs/runbooks/halted-table.md b/docs/src/content/docs/runbooks/halted-table.md
index 07a006a..9b3d5d5 100644
--- a/docs/src/content/docs/runbooks/halted-table.md
+++ b/docs/src/content/docs/runbooks/halted-table.md
@@ -37,8 +37,8 @@ Its `halted_tables` lists each halted table with its code and a detail.
 | `TABLE_NOT_REPLICABLE` | an `UNLOGGED` PostgreSQL table, or a table the source can't stream; a PostgreSQL table its publication publishes with a row filter or a column list (some of its changes never reach the stream); a table whose snapshot read fails for a reason of its own, such as a lost `SELECT` or a type or collation error (the detail names it) | make it logged, or drop it from the config; publish the table whole in one transaction (`BEGIN; ALTER PUBLICATION … DROP TABLE …; ALTER PUBLICATION … ADD TABLE …; COMMIT;`), then copy it again; for a failed read, fix the cause the detail names (grant `SELECT` again) |
 | `NUMERIC_OVERFLOW` | a value that doesn't fit its decimal type | widen the type with `tables.column_type_overrides`, or `numeric_overflow_action: string` |
 | `INVALID_DATE_VALUE` | MySQL: a zero or invalid date such as `2024-00-15` | override the column to `string`, or fix the rows |
-| `SCHEMA_MISMATCH` | a column whose type the stream and the table disagree on, or an override that can't apply; a Delta column that matches a source column by case alone (Delta names are case-insensitive), the detail naming both spellings; or a Delta table the pipeline meets for the first time (left by a state that's gone, or made by a job's copy) holding a non-null column the source lacks | correct the `column_type_overrides` entry; for a case-only match or a non-null column the source lacks, drop the Delta table and copy the table again |
-| `SINK_REJECTED` | Delta refused the table's changes for good, for example a MERGE over `merge_memory_limit`; a table whose two source columns differ by case alone, which a Delta table can't hold; a table whose reposition after a source change Delta refused (the detail says `its reposition fails`); or a table a truncating job asked for whose Delta table is keyed otherwise than the pipeline's, by other columns or the same spelled otherwise (the detail says `a truncating job`), which isn't emptied | the detail names it: raise the limit, or lower the `flush` bounds; rename one of the two source columns; for a truncating job's table, drop or move the Delta table, then ask the job again; for a refused reposition, fix what the detail names, or drop or move the Delta table, then copy the table again. A table the pipeline doesn't replicate is repositioned only at that start: its halt can go at the next start, but its Delta table keeps the old server's marker, so drop or move it before you add the table to the config |
+| `SCHEMA_MISMATCH` | a column whose type the stream and the table disagree on, or an override that can't apply; on PostgreSQL, a Delta column that matches a source column by case alone (Delta names are case-insensitive), the detail naming both spellings (on MySQL and MariaDB, whose column names are case-insensitive too, it is the same column, and keeps the Delta table's spelling); or a Delta table the pipeline meets for the first time (left by a state that's gone, or made by a job's copy) holding a non-null column the source lacks | correct the `column_type_overrides` entry; for a case-only match or a non-null column the source lacks, drop the Delta table and copy the table again |
+| `SINK_REJECTED` | Delta refused the table's changes for good, for example a MERGE over `merge_memory_limit`; a table whose two source columns differ by case alone, which a Delta table can't hold; a table whose reposition after a source change Delta refused (the detail says `its reposition fails`); or a table a truncating job asked for whose Delta table is keyed otherwise than the pipeline's, by other columns or the same spelled otherwise (the detail says `a truncating job`), which isn't emptied | the detail names it: raise the limit, or lower the `flush` bounds; rename one of the two source columns; for a truncating job's table, drop or move the Delta table, then ask the job again; for a refused reposition, fix what the detail names, or drop or move the Delta table, then copy the table again. A table the pipeline doesn't replicate is repositioned only at that start, so its halt stays while the lake holds its Delta table: drop or move that table (the next start clears the halt), or add the table to the config and copy it again |
 | `SNAPSHOT_TIMEOUT` | the table's snapshot ran past `source.timeouts.snapshot` | raise the timeout |
 
 A config fix goes through `POST /v1/config/reload`; most of these keys restart the pipeline.
diff --git a/docs/src/content/docs/runbooks/position-lost.mdx b/docs/src/content/docs/runbooks/position-lost.mdx
index c0c4cc3..46ad74d 100644
--- a/docs/src/content/docs/runbooks/position-lost.mdx
+++ b/docs/src/content/docs/runbooks/position-lost.mdx
@@ -123,10 +123,11 @@ Leave `source_change_action` at `re_snapshot` only where a failover should recov
 
 If the pipeline's address reaches more than one server, the runs keep finding another one. Under
 `re_snapshot` each such run starts the pipeline again, and three runs in a row that each find
-another server and make no checkpoint durable stop the pipeline in `error`, as for a changing
-binlog server (see the MySQL and MariaDB tab), usually with `WAL_SOURCE_CHANGED`. Reconnects that
-keep reaching a promoted server of the same system do the same under either action, and that halt
-carries no code: look for "the source's address reaches more than one server" in its reason.
+another server, each within 10 minutes of the one before, and make no checkpoint durable stop the
+pipeline in `error` with `WAL_SOURCE_CHANGED`, as for a changing binlog server (see the MySQL and
+MariaDB tab). Reconnects that keep reaching a promoted server of the same system do the same under
+either action. Failovers further apart than that are failovers, not an alternation: each starts
+the count again.
 
 ## A renamed or dropped publication
 
@@ -186,15 +187,14 @@ reconnects, not only at a start.
   without a restart) and `POST /v1/pipelines/<id>/restart`.
 
 **A server that keeps changing.** Under `re_snapshot` a run that finds another server ends and the
-pipeline starts again. Three runs in a row that each find another server than the run before, none
-making a checkpoint durable, stop the pipeline in `error` with
-`BINLOG_SOURCE_CHANGED` (on PostgreSQL, `WAL_SOURCE_CHANGED`), saying that the source's address
-reaches more than one server (a load balancer, or a DNS name over several). Point the pipeline
-at one server (no load balancer, no round-robin DNS name), then stop and resume it. The count
-starts again when a run makes a checkpoint durable or ends for another reason, and when the
-pipeline is stopped. It doesn't decay with time: on a source that writes nothing in between, three
-genuine failovers in a row, however far apart, can stop the pipeline the same way; stop and resume
-it.
+pipeline starts again. Three runs in a row that each find another server than the run before,
+each within 10 minutes of the one before and none making a checkpoint durable, stop the pipeline
+in `error` with `BINLOG_SOURCE_CHANGED` (on PostgreSQL, `WAL_SOURCE_CHANGED`), saying that the
+source's address reaches more than one server (a load balancer, or a DNS name over several).
+Point the pipeline at one server (no load balancer, no round-robin DNS name), then stop and resume
+it. The count starts again when a run makes a checkpoint durable or ends for another reason, when
+a server change comes more than 10 minutes after the one before (genuine failovers, even on a
+source that writes nothing in between), and when the pipeline is stopped.
 
 Two cases aren't caught. On MariaDB: another server with the same `server_id` and the same binlog
 file names, whose file happens to have an event boundary at the checkpoint's offset. On MySQL: a
diff --git a/test-infra/README.md b/test-infra/README.md
index 4c44605..631f6f4 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -216,6 +216,44 @@ By hand: `DATA_AVALANCHE_CLOUD=s3 DATA_AVALANCHE_S3_BUCKET=<bucket> AWS_REGION=<
 data-avalanche-state --test lease_conformance real_s3 -- --nocapture` with AWS credentials in the
 environment or profile (`azure` / `real_azure` likewise, with `AZURE_*`).
 
+## The E2E suite on another store (M12c)
+
+`DATA_AVALANCHE_E2E_STORE` puts an E2E run's `globals.state` and lake on another store than RustFS:
+`azurite` (the binary's Azure path, on the emulator above), or a real cloud, `s3`, `gcs` or `azure`,
+through the bucket variable and the credentials of the tables above (`DATA_AVALANCHE_GCS_BUCKET`
+and `GOOGLE_SERVICE_ACCOUNT_PATH` for GCS). The binary gets the credentials from the test's
+environment. A run writes under `data-avalanche-it/e2e-<test>/<uuid>/` and deletes it at the end.
+
+The cloud subset is the four scenarios `cloud-e2e.txt` names (Q-M12c-4). `ci.yml`'s `e2e-gcs` runs
+it on GCS on every PR, on PostgreSQL 18 and MySQL 8.4; `cloud.yml`'s `e2e` runs it on S3, GCS and
+Azure weekly and on manual dispatch. By hand, with the source's container up:
+
+```sh
+DATA_AVALANCHE_IT=1 DATA_AVALANCHE_E2E_STORE=gcs DATA_AVALANCHE_GCS_BUCKET=<bucket> \
+  GOOGLE_SERVICE_ACCOUNT_PATH=<key.json> cargo test -p data-avalanche --features e2e-hooks \
+  --test e2e -- --test-threads=1 --exact $(cat test-infra/cloud-e2e.txt)
+```
+
+## The acceptance runs (M12c)
+
+`cargo xtask bench`, `cargo xtask chaos` and `cargo xtask soak` run the v1 acceptance runs
+(`crates/data-avalanche/tests/load`) against this infrastructure, the binary built in release mode,
+and append each measurement to `target/acceptance/results.jsonl`. They run on a developer's machine
+(Q-M12-3), with `test-infra/up.sh` done; the E2E source variables pick the source as for the E2E
+suite.
+
+| Run | What | Default length |
+|---|---|---|
+| `bench` | D-V1.3's lag in steady state (p99 at most 60 s) and across a holder's loss (back within 2 minutes), at the product's defaults; MERGE's cost under random-key churn | about 1 h 30 |
+| `chaos` | under load: a storage outage shorter and longer than the lease (RustFS through a proxy), a restart of the source's container (`docker compose restart`), a partition of the holder | about 25 min |
+| `soak` | three nodes, three pipelines, churn every 10 minutes (a column added, a pause, a leave, a job, a `kill -9`), the lakes checked every hour, `n1`'s memory at most twice its first reading | 24 h |
+
+The knobs, each optional: `DATA_AVALANCHE_LOAD_RATE` (changes a second, 1000; chaos takes a
+quarter, the soak a fifth), `DATA_AVALANCHE_LOAD_KEYS` (keys a table, 100 000),
+`DATA_AVALANCHE_LOAD_MINUTES` (the steady window, 30), `DATA_AVALANCHE_CHURN_KEYS` (1 000 000),
+`DATA_AVALANCHE_CHURN_MINUTES` (10), `DATA_AVALANCHE_SOAK_MINUTES` (1440),
+`DATA_AVALANCHE_SOAK_CHURN_MINUTES` (10), `DATA_AVALANCHE_SOAK_CHECK_MINUTES` (60).
+
 ## Lease conformance T1–T10 (M2)
 
 `crates/data-avalanche-state/tests/lease_conformance.rs` runs T1–T10 (Clustering §17.4) through
````

- [ ] **Step 3: The tracker.** A dated §63 "M12c — the gaps closed": DA-SRC-39, DA-SRC-38,
  DA-SRC-37 and DA-SNAP-23 🟥 → ✅, each with its task, its tests and its spec patch;
  DA-OPS-14 🟥 → ✅ closed by the measurement of §Measured in planning (D-M12c.1). Each domain row
  gets its `**✅ 2026-11-…**` rationale, the open count goes from 5 to 0 (v1), and the counts
  paragraph gains the line, as §60 did.

- [ ] **Step 4: Check the site and the generated pages**

Run: `cargo xtask docs --check`; `cd docs && npm run build` (the links validated).
Expected: PASS; the site builds.

- [ ] **Step 5: Commit**, one commit per kind, as M12d's Task 9:

```bash
git add doc/specs && git commit -m "spec: Data-Flow, Schema Change — the alternation window, a refused reposition's halt, a first-met case-only column (P-1…P-3)"
git add docs doc/research test-infra/README.md doc/v1.0/README.md && git commit -m "docs: M12c — the gap fixes' pages and research, the store switch and the acceptance runs"
git add doc/analysis && git commit -m "gap: v1 §63 — M12c's gaps closed (DA-SRC-37, 38, 39, DA-SNAP-23; DA-OPS-14 measured)"
```

### Task 8: The acceptance runs and the report — roadmap §8

On Oleksii's machine (Q-M12-3), against test-infra (`test-infra/up.sh`; the `replica` profile isn't
needed). The runs are long: the controller starts each in the background and waits for it.

**Files:**
- Create: `doc/v1.0/v1-acceptance.md`
- Modify: `docs/src/content/docs/deployment/observability.md`, `docs/src/content/docs/configuration/sink.md`
- Modify: `doc/v1.0/00-roadmap.md` (§8), `doc/analysis/CDC_Design_Gap_Analysis_v1.md`

- [ ] **Step 1: Run the bench and chaos on PostgreSQL 18, then on MySQL 8.4**

```bash
cargo xtask bench && cargo xtask chaos
DATA_AVALANCHE_E2E_SOURCE=mysql cargo xtask bench && DATA_AVALANCHE_E2E_SOURCE=mysql cargo xtask chaos
```

Expected: every run PASSes; `target/acceptance/results.jsonl` holds `steady_lag_p99` at most 60 and
`failover_recovered` at most 120 for each source. A failed run is diagnosed, not rerun until it
passes: a miss of a D-V1.3 target is a finding for Oleksii, with its numbers.

- [ ] **Step 2: Run the soak on PostgreSQL 18**

```bash
cargo xtask soak
```

Expected: PASS after about 24 h; `n1_rss_growth` at most 2.

- [ ] **Step 3: The report.** `doc/v1.0/v1-acceptance.md` (frontmatter as every doc; `domain:
  Architecture`): the machine and the commit; per source, each run's measurements from
  `results.jsonl` against its target (D-V1.3's two, the soak's memory), MERGE's cost (R-5), each
  chaos run's outcome; the cloud subset's runs (PR CI's `e2e-gcs`; `cloud.yml`'s `e2e`, filled at
  the Exit); and the §8 checklist, item by item, with where each is met. The site:
  `observability.md`'s targets paragraph gains the measured p99 and recovery; `sink.md`'s
  flush-bounds paragraph the churn numbers (R-5).

- [ ] **Step 4: The roadmap and the tracker.** §8: check the items the report shows met (D-V1.3
  measured; no loss in the crash, replay and chaos suites; the compile time, by M12b's Task 15; the
  gap tracker updated; the docs site builds); the cloud E2E, the image and chart, and the runbooks'
  review stay for the Exit. R-5's row points at the report. Tracker §64 "M12c — the acceptance"
  records the runs and R-5's measurement.

- [ ] **Step 5: Commit**

```bash
git add doc/v1.0/v1-acceptance.md doc/v1.0/00-roadmap.md docs && git commit -m "infra: v1 acceptance — the report, the measured lag and MERGE cost"
git add doc/analysis && git commit -m "gap: v1 §64 — M12c's acceptance runs"
```

### Task 9: `1.0.0`

**Files:**
- Modify: `Cargo.toml` (`[workspace.package] version`), `Cargo.lock`
- Modify: `deploy/helm/data-avalanche/Chart.yaml` (`version`, `appVersion`), `deploy/helm/data-avalanche/README.md` (the install example's `--version`)
- Modify: `doc/v1.0/README.md` (M12c's row), `doc/v1.0/00-roadmap.md` (M12's status)

- [ ] **Step 1: The version.** `1.0.0` in the three places `release.yml`'s `version` job compares;
  `cargo update -w` writes the lock; the chart README's example installs `--version 1.0.0`.

- [ ] **Step 2: Check it as the release will**

Run: the `version` job's three `sed` lines (`.github/workflows/release.yml`) by hand; `cargo build`;
`data-avalanche --version`; `helm lint deploy/helm/data-avalanche`.
Expected: `workspace 1.0.0, chart 1.0.0, appVersion 1.0.0`; the binary prints `1.0.0`; the chart
lints.

- [ ] **Step 3: The gates**: fmt, clippy (also with `e2e-hooks`), `cargo xtask docs --check`,
  `cargo test --workspace`.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock deploy/helm/data-avalanche doc/v1.0 && git commit -m "build: 1.0.0"
```

## Exit

- [ ] Every task's tests, the gates, and the ITs and E2E their tasks name, green (the whole-branch
  run as M12d's Exit: the workspace ITs, the MySQL targets, PostgreSQL 17 and 14, the E2E on
  PostgreSQL 18 with both replica pairs and on `mysql-84`).
- [ ] The acceptance runs green on Oleksii's machine (Task 8), D-V1.3 met on both sources.
- [ ] After the whole-branch review and its fixes: the branch pushed, the PR open; its CI green,
  `e2e-gcs` included. With Oleksii's OK, `cloud.yml` dispatched on the branch (`gh workflow run
  cloud.yml --ref feat/claude/m12c-acceptance`): `e2e` green on S3, GCS and Azure, and the report's
  cloud rows filled from it.
- [ ] Oleksii merges, then tags `v1.0.0` on `main`; `release.yml` publishes the image and the
  chart. A short follow-up PR records the release in the README and §8 (as v0.1.0 was recorded in
  M11b's row).

## Verification in planning (2026-11-11)

Every task's code above was written and run in the working tree of
`feat/claude/m12c-acceptance` (base 4de218d), then replayed task by task onto that base (each
task's diff applied after the previous, `rustfmt` on its Rust files): the replay's final tree
equals the tree the runs below used, file for file. The tree was then reset; the code lives in
this plan until the tasks run.

| Check | Result |
|---|---|
| `cargo fmt --all --check` | clean |
| `cargo clippy --workspace --all-targets -- -D warnings`; and `-p data-avalanche --features e2e-hooks --all-targets` | clean |
| `cargo xtask docs --check` | OK |
| `cargo test --workspace` | 1507 passed, 0 failed |
| Task 1–4's crates | pipeline 273, cluster 174, source-pg `slot::` 14 |
| DA-SNAP-23's E2E | passes on `mysql-84` and `mariadb-118`; fails with the worker's `fold` given `false` |
| The store switch | `smoke::` on RustFS and Azurite; the cloud subset (4) on Azurite in 101 s |
| The cluster scenarios after the move | 7 passed |
| The acceptance runs, short | §Measured in planning: every run passes |
| The E2E suite, PostgreSQL 18, both replica pairs | 51 scenarios: 49 passed (607 s); the two `failover::` scenarios found their standbys promoted by an earlier run, and passed (60 s) once the pairs were recreated. With Task 2's prefix and the slot-sync scenario's old check, that scenario fails at the check; with the new one it passes |

Not run in planning: the cloud subset on real S3, GCS and Azure (Task 5's Step 4 and the Exit:
Oleksii's credentials, or the repository's secrets in CI), and the full-length acceptance runs
(Task 8).
