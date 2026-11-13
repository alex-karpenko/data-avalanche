---
updated: 2026-11-13
domain: Architecture
tags: [v1, acceptance, load, soak, chaos, lag, failover, merge-cost, cloud, release]
---

# data-avalanche v1 — Acceptance Report

The measurements behind roadmap §8, the v1.0.0 acceptance checklist: D-V1.3's two lag targets,
the soak's memory, MERGE's cost under random-key churn (R-5, D-MS.15), the chaos runs and the
cloud subset. Every measurement below is read from `target/acceptance/results.jsonl`, which the
runs append to (one JSON line per measurement, with its target and whether it was met); the
cloud subset's times are from Task 5's run. The runs are
the `load` test target (`crates/data-avalanche/tests/load/`), started by `cargo xtask
bench|chaos|soak` in release mode (M12c Task 6, Q-M12c-2).

## The machine and the commit

| | |
|---|---|
| Machine | MacBook Pro 16,1: Intel Core i7-9750H 2.60 GHz, 6 cores / 12 threads, 16 GiB |
| OS | macOS 26.7.3 |
| Containers | Docker 29.9.3 (6 CPUs, about 7.7 GiB): test-infra's PostgreSQL 18, MySQL 8.4 and RustFS (`test-infra/up.sh matrix`) |
| Commit | `89d7e1c` (branch `feat/claude/m12c-acceptance`) |
| Dates | bench and chaos 2026-11-11 22:25 … 2026-11-12 00:39 EET; soak 2026-11-12 00:39 … 2026-11-13 00:42 EET |

Every run is at the product's defaults: a 30 s flush, a 15 s lease renewed every 5 s. The storage
is RustFS on the same machine, so object-store latency is lower than a cloud bucket's. The steady,
churn and storage-outage runs are one node; the failover, the partition and the soak three; the
source, the lake and the nodes share the machine's cores.

## D-V1.3: the lag targets

`steady_state_lag_at_the_defaults`: one node, four tables of 100 000 keys each, snapshotted, then changed at
1000 changes a second for 30 minutes after a minute's warm-up, the lag
(`data_avalanche_replication_lag_seconds`) sampled every second.
`a_failover_recovers_within_two_minutes`: two tables, three nodes; the lease holder is killed
under load, a sentinel row written just after must land, and the new holder's lag be under 60 s.

| Measurement | Target | PostgreSQL 18 | MySQL 8.4 |
|---|---|---|---|
| Steady lag p99 | ≤ 60 s | **30.9 s** ✅ | **30.8 s** ✅ |
| Steady lag p50 / max | — | 16.2 s / 31.4 s | 16.1 s / 31.3 s |
| Samples / rate achieved | ≥ 1620 / ≥ 800 changes/s | 1801 / 999.5 changes/s | 1801 / 998.3 changes/s |
| Writer statements failed | 0 | 0 | 0 |
| Failover: sentinel landed | — | 52.5 s | 53.7 s |
| Failover: recovered (lag back under 60 s) | ≤ 120 s | **52.5 s** ✅ | **53.7 s** ✅ |

The steady lag follows the flush: a change waits up to the 30 s flush interval plus the MERGE,
so p50 sits at about half the interval and the maximum just above it. The failover's time is
measured as a whole; what it should be made of (the 15 s lease's expiry, the takeover, the
stream's reopen, the replay from the last checkpoint) is expected, not measured apart.

## R-5: MERGE's cost under random-key churn

`merge_cost_under_random_key_churn` (no target: D-V1.3 excludes heavy random-key MERGEs): one
node, one table of 1 000 000 keys, snapshotted, then updated at 1000 changes a second for 10
minutes over its whole key range, so every flush's keys fall in every data file. The table size
is the snapshot's files; the MERGE count, bytes written and files removed are summed from the
table's Delta log over the commits after the snapshot; the mean commit time is the delta of
`data_avalanche_sink_commit_duration_seconds` over the window.

| Measurement | PostgreSQL 18 | MySQL 8.4 |
|---|---|---|
| Table size | 13.1 MB | 13.1 MB |
| MERGEs (flushes) | 20 | 20 |
| Mean commit time | 1.18 s | 1.38 s |
| Bytes written per MERGE | 13.0 MB | 11.1 MB |
| Data files removed per MERGE | 5.95 | 5.95 |

Each MERGE wrote 84 to 99 % of the table's size, so it rewrote close to the whole table, as D-MS.15
predicts: the cost is proportional to the files touched, not to the rows changed. On this table
it is about a second a flush; it grows with the table's size, and is what
`configuration/sink.md`'s flush guidance (fewer, larger flushes) and
`data_avalanche_sink_merge_files_rewritten_total` are for. R-5 is measured; the deletion-vector
writer that would bound it stays post-v1.

## The soak

`a_day_of_churn_loses_nothing` on PostgreSQL 18: three nodes and three pipelines over three
tables for 24 hours, the writer at a fifth of the bench's rate (200 changes a second). Every 10
minutes one change, in turn: a column added; a pipeline paused and resumed; a node's leave and
restart; a snapshot job; a node's `kill -9` and restart. Every 60 minutes the writer stops and
every lake must hold the source's rows, and node 1's resident memory is read: node 1 is never
restarted, so its memory over the run is the leak check.

The soak ran 24 h 3 min. Node 1's memory moved with the flushes and snapshot jobs (between 27
and 56 MB) and ended below where it started: no growth.

| Measurement | Target | PostgreSQL 18 |
|---|---|---|
| Checks (lake = source) | every hour passes | 24 of 24 ✅ |
| Churn steps | — | 143 |
| Changes written | — | 17 005 517 |
| Writer statements failed | 0 | 0 |
| Node 1 RSS, first check → last | — | 40.4 MB → 36.5 MB (lowest 27.3 MB, highest 55.8 MB) |
| Node 1 RSS growth (last ÷ first) | ≤ 2× | **0.90×** ✅ |

## The chaos runs

Each under load at the defaults, each ending with the lake holding the source's rows, every key
once, and the owner epochs never going back (`lands`). All passed on both sources. The storage
outages cut the node off from RustFS for 5 s (a third of the lease) and 45 s (three leases); the
partition cuts the holder off from the other two nodes for four leases. A storage outage's time
is from the store's return to the pipeline's status `running` (after the short outage it was
already `running`); a restart's is from `docker compose restart` to the server accepting connections again,
the pipeline's recovery shown by the lake converging afterwards.

| Run | PostgreSQL 18 | MySQL 8.4 |
|---|---|---|
| Storage outage shorter than the lease | ✅ `running` 0.0007 s after the store's return, 0 epochs | ✅ 0.0004 s, 0 epochs |
| Storage outage longer than the lease | ✅ `running` 2.66 s after, 1 epoch (the lease lost and retaken) | ✅ 2.69 s, 1 epoch |
| Source server restart (`docker compose restart`) | ✅ accepting connections 3.0 s after the restart began; 1 writer statement failed while it was down | ✅ 8.0 s; 4 statements failed while it was down |
| Partition of the holder from the other nodes | ✅ 0 epochs (the holder kept its lease through the store) | ✅ 0 epochs |

The failed statements are the workload's own writes refused by the restarting server; nothing the
source committed was lost.

## The cloud subset

The four scenarios in `test-infra/cloud-e2e.txt` (Q-M12c-4), run with `--exact` against a real
bucket through `DATA_AVALANCHE_E2E_STORE`. The CI rows are the test binary's own time; every
run passed all four.

| Where | Store | PostgreSQL 18 | MySQL 8.4 |
|---|---|---|---|
| Oleksii's machine, 2026-11-11 (Task 5, `00f7eaf`) | GCS | 4/4, 462 s | 4/4, 412 s |
| PR CI `e2e-gcs`, PR #22 run 43372201106, 2026-11-13 (`3364b20`) | GCS | 4/4, 168 s | 4/4, 131 s |
| `cloud.yml` `e2e`, run 43375990336, 2026-11-13 (`3364b20`) | S3 | 4/4, 159 s | 4/4, 133 s |
| `cloud.yml` `e2e`, same run | GCS | 4/4, 193 s | 4/4, 173 s |
| `cloud.yml` `e2e`, same run | Azure | 4/4, 132 s | 4/4, 145 s |

## The §8 checklist

| Item | Status | Where it is met |
|---|---|---|
| All milestone exit criteria met; CI, weekly and cloud workflows green | at the merge | M0–M12d merged with their exits (`doc/v1.0/README.md`); weekly 4 in a row treated as met (Q-M12-5); M12c's PR CI and `cloud.yml` green at `3364b20` (above); M12c's exit is its merge |
| D-V1.3 measured: steady p99 ≤ 60 s, failover recovery ≤ 2 min | ✅ | §D-V1.3 above: 30.9 s / 52.5 s (PostgreSQL 18), 30.8 s / 53.7 s (MySQL 8.4) |
| No data loss or duplicate keys in the crash/replay/chaos suites | ✅ | the crash and replay scenarios in `crates/data-avalanche/tests/e2e/` (`crashes.rs`, `failover.rs`; every PR); the chaos runs above; the soak's hourly checks |
| Real-cloud E2E on S3, GCS and Azure | ✅ | the cloud subset 4/4 on each store and source: `cloud.yml` run 43375990336, and PR CI's `e2e-gcs` (above) |
| Container image and Helm chart published; k3s smoke deploy green | at the Exit | `release.yml` on the `v1.0.0` tag (M11a) |
| Documentation site builds strict; runbooks reviewed | builds ✅; review at the Exit | `docs/` builds strict, every internal link valid (2026-11-13, after this report's page edits); the runbooks' review is Oleksii's |
| Compile time and dependency tree reviewed against `cargo build --timings` | ✅ | M12b Task 15 (`doc/v1.0/M12b-control-hardening.md`): measured, line tables taken |
| Gap tracker updated | ✅ | `CDC_Design_Gap_Analysis_v1.md` §63 (open 5 → 0) and §64 |
