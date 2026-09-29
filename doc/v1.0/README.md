---
updated: 2026-09-29
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
| [`00-roadmap.md`](00-roadmap.md) | Master plan: planning decisions P-1…P-10, workspace layout, cross-crate interfaces, milestones M0–M12 with exit criteria, test/CI strategy, risks, spec edits S-A…S-M, implementation decisions D-IMPL-1…9 | Draft for review |
| [`M0-foundations.md`](M0-foundations.md) | Detailed task plan for M0: workspace, CI, core domain types, all four position dialects, test infrastructure | Draft for review; code blocks compiled, and the test/clippy/integration results verified in a scratch crate on 2026-09-26 |
| [`M1-configuration.md`](M1-configuration.md) | Detailed task plan for M1: the `data-avalanche-config` crate. Strict YAML parse, `${VAR}` and file resolution, §8 merge, all V-rules, the resolved config, the reload diff | **Landed 2026-09-28** on `feat/claude/impl-v1` (15 tasks, two review fix waves, owner follow-up batch FU-A…FU-E). The plan's "Deviations during execution" section lists where the code differs from its task blocks; the landed interface is roadmap §3.8 |
| [`M2-storage-and-state.md`](M2-storage-and-state.md) | Detailed task plan for M2: `data-avalanche-storage` (backends with bounded timeouts, config §10 paths, `VersionedStore` CAS) and `data-avalanche-state` (versioned documents, leases, cluster anchor, config generations, per-pipeline and snapshot state), T1–T10 on every backend, `cloud.yml` | **Executed 2026-09-29** on `feat/claude/impl-v1`: 11 tasks, fix rounds on Tasks 3, 5 and 8, and one final-review fix wave. T1–T10 pass in memory and on RustFS, Azurite, real GCS, real S3 and real Azure (`cloud.yml` run 36604762684); every exit item is met. The plan's "Deviations during execution" section lists where the code differs from its task blocks; the landed interface is roadmap §3.5 |
| `M3-…` … `M12-…` | One detailed plan per milestone | Written just before each milestone starts, from the interfaces that actually landed |

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
