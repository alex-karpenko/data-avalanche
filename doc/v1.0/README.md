---
updated: 2026-10-01
domain: Architecture
tags: [v1, implementation-plan, index, conventions]
---

# v1.0 planning artifacts

This folder holds the plan for **building** v1. The design it implements is in `doc/specs/`,
`doc/research/` (v1 decisions: `V1_Scope_Decisions.md`) and the probe evidence in
`prototypes/*/README.md`. When a plan and a spec disagree, the spec wins: stop and raise it.

## Index

| File | What it is | Status |
|---|---|---|
| [`00-roadmap.md`](00-roadmap.md) | Master plan: planning decisions P-1…P-10, workspace layout, cross-crate interfaces, milestones M0–M12 with exit criteria, test/CI strategy, risks, spec edits S-A…S-Q, implementation decisions D-IMPL-1…12 | Draft for review |
| [`M0-foundations.md`](M0-foundations.md) | Detailed task plan for M0: workspace, CI, core domain types, all four position dialects, test infrastructure | Draft for review; code blocks compiled, and the test/clippy/integration results verified in a scratch crate on 2026-09-26 |
| [`M1-configuration.md`](M1-configuration.md) | Detailed task plan for M1: the `data-avalanche-config` crate. Strict YAML parse, `${VAR}` and file resolution, §8 merge, all V-rules, the resolved config, the reload diff | **Landed 2026-09-28** on `feat/claude/impl-v1` (15 tasks, two review fix waves, owner follow-up batch FU-A…FU-E). The plan's "Deviations during execution" section lists where the code differs from its task blocks; the landed interface is roadmap §3.8 |
| [`M2-storage-and-state.md`](M2-storage-and-state.md) | Detailed task plan for M2: `data-avalanche-storage` (backends with bounded timeouts, config §10 paths, `VersionedStore` CAS) and `data-avalanche-state` (versioned documents, leases, cluster anchor, config generations, per-pipeline and snapshot state), T1–T10 on every backend, `cloud.yml` | **Executed 2026-09-29** on `feat/claude/impl-v1`: 11 tasks, fix rounds on Tasks 3, 5 and 8, and one final-review fix wave. T1–T10 pass in memory and on RustFS, Azurite, real GCS, real S3 and real Azure (`cloud.yml` run 36604762684); every exit item is met. The plan's "Deviations during execution" section lists where the code differs from its task blocks; the landed interface is roadmap §3.5 |
| [`M3-core-data-path.md`](M3-core-data-path.md) | Detailed task plan for M3: the schema model and §7.2 classifier, Hop 2 Arrow schemas, `TableChangeSet` and the `Sink` trait (core); the in-memory reference sink (testkit); the accumulator in the new `data-avalanche-pipeline` (keep-last collapse, TOAST carry-forward, transactions and positions, flush policy, TRUNCATE and schema barriers, split commits); the collapse property tests | **Landed 2026-09-30** on `feat/claude/impl-v1`: 13 tasks, three test follow-ups during the tasks, and one final-review fix wave (C1, I1, I2, I3 and four smaller items; spec edit S-Q). Spec edits S-N, S-O, S-P, S-Q are applied. 440 workspace tests pass, including the two collapse properties at 512 cases each. The plan's "Deviations during execution" section lists where the code differs from its task blocks. The landed interface is roadmap §3.1, §3.3, §3.4 and §3.9 |
| [`M4-delta-sink.md`](M4-delta-sink.md) | Detailed task plan for M4: `data-avalanche-sink-delta`, the Delta Lake `Sink`. Hop 3b types and values (the Delta date clamp), the MERGE with unchanged masks, the `txn` fence, the `commitInfo` position with the walk-back, truncate, evolution, retries; the probes P1–P7 and P10 through the trait on a local directory, RustFS, Azurite and real GCS; conformance with `MemorySink`; a DuckDB cross-read. Also `table_root`'s `~XX` escape in the storage crate (Q-M4-5) | **Executed 2026-10-01** on `feat/claude/m4-delta-sink`: 13 tasks, fix rounds on Tasks 4 and 7, and one final-review fix wave. The fix wave covered a re-created-table skip, error classes, the retry set narrowed to real races, a truncate that a concurrent OPTIMIZE outran, and crate-private Hop 3b modules. 519 workspace tests pass. The scenarios pass on local, RustFS, Azurite and real GCS, and the DuckDB cross-read passes. The plan's "Deviations during execution" section lists where the code differs from its task blocks; the landed interface is roadmap §3.10 |
| [`M5-pg-source.md`](M5-pg-source.md) | Detailed task plan for M5: the source contract in core (`SourceConnector`, `ChangeStream`, `StreamItem`), and `data-avalanche-source-pg`. The pgoutput v1 decoder, Hop 1 for every mapped type, the slot state machine and publication reconciliation, preflight, channels A, C and D, keyset snapshot chunks and the keyed re-read; S-PG0…8 as regression tests on PostgreSQL 18, and 14–17 nightly; the `postgres-tls` service | **Approved 2026-10-01**; executed on `feat/claude/m5-pg-source` (13 tasks and 5b, then the whole-branch review and its three fix rounds); PR pending. Final tree: 666 workspace tests; 53 integration tests on PostgreSQL 18 with TLS, 49 on each of 14–17 |
| `M6-…` … `M12-…` | One detailed plan per milestone | Written just before each milestone starts, from the interfaces that actually landed |

## How the plans are written and run

- **Staged.** The roadmap fixes the milestone order and the interfaces; each `M<n>-*.md` is written
  when M<n−1> is done, so it builds on the real code. After each milestone, update the roadmap's
  §3 interfaces to match what landed.
- **Test-first, one commit per task.** Every task lists its files, the interfaces it consumes and
  produces, the failing test, the command and expected failure, the implementation, the passing run,
  and the commit. Each plan has a *Review Focus* section: the input classes most likely to bite,
  each pinned by a test.
- **Executor: Claude agents, subagent-driven** (P-9, confirmed 2026-09-26). A fresh agent implements
  each task and a fresh reviewer checks it before the next task starts; a whole-branch review closes
  each milestone. Tasks are sized so an agent that sees only its task can finish it.
- **Verification before claims.** A result that depends on an external system (a crate's behaviour,
  an emulator, a cloud) is measured, not assumed. Plans say where each number came from.

## Test gates (all milestones)

| Variable | Effect |
|---|---|
| `DATA_AVALANCHE_IT` (non-empty) | Enables integration tests against `test-infra/compose.yaml` (start with `test-infra/up.sh`) |
| `DATA_AVALANCHE_GCS_BUCKET` + `GOOGLE_SERVICE_ACCOUNT_PATH` | Real-GCS integration tests (P-10); required whenever `DATA_AVALANCHE_IT` is set |
| `DATA_AVALANCHE_SKIP_GCS` (non-empty) | Explicit opt-out of the GCS tests (CI sets it for fork PRs) |
| `DATA_AVALANCHE_CLOUD=s3\|azure` (from M2) | Real S3 / Azure conformance runs (`cloud.yml`) |

CI secrets and variables are listed in `test-infra/README.md` ("CI requirements") and in the comments
of `.github/workflows/ci.yml`.

## Commit conventions for implementation work

The doc prefixes from `CLAUDE.md` (`spec:`, `research:`, `gap:`, `infra:`) cover design
documents. Code commits use `build:`, `ci:`, `feat(<crate>):`, `fix(<crate>):`, `test(<area>):`,
`docs:`, as each task's commit step specifies.
