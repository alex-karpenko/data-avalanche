# data-avalanche — Project Instructions

---

## What this project is

A **Rust-based Change Data Capture (CDC) system** that replicates row-level changes from
PostgreSQL and MySQL into **lakehouse tables on cloud object storage** (S3, GCS, Azure Blob). The
target table format is **pluggable per pipeline** (`sink.type`, required, no default):

- **Delta Lake** via `delta-rs` — the v1 mirror sink (MERGE commits deletes + upserts atomically).
- **Apache Iceberg** via `iceberg-rust` — designed and partly probed, but **deferred to v2**
  (2026-09-26): mirror mode needs a delete-capable commit iceberg-rust doesn't ship (DA-SINK-1i). In
  v1 an Iceberg pipeline is refused at start; the Iceberg gaps are the ⏭ v2 backlog (tracker §26.2).

Key subsystems: distributed worker cluster (consistent hashing + SWIM/memberlist + storage CAS
lease), Apache Arrow as the in-memory columnar format, a sink-neutral `Sink` trait with per-format
realizations, and Iceberg catalog backends (AWS Glue, HMS, REST).

**Current phase: v1 implementation.** The design corpus (specs, research, gap tracker) and the
capability probes are done enough to build on; v1 is now being built milestone by milestone
(M0–M12) from the plans in `doc/v1.0/`. Which milestones have landed, and which plan is next, is
in the index table of `doc/v1.0/README.md` (the source of truth for status; this file doesn't
repeat it).

Work style: **plan-driven, test-first implementation**, with the design kept alive beside it.
- For each milestone, Claude writes a detailed plan, verifies its code in a scratch workspace, and
  gets it approved. Then the plan is executed **subagent-driven** (P-9): one implementer and one
  reviewer per task, and a whole-branch review at the end.
- Each milestone, plan and implementation both, is **its own branch and its own pull request**.
  Oleksii reviews and merges every PR. The flow is under "Implementation workflow" below.
- Design questions that come up while building go back into the specs (with approval), the
  research docs and the gap tracker, as in-place edits.

---

## Repository layout

```
data-avalanche/
├── CLAUDE.md                                     ← this file
├── Cargo.toml, Cargo.lock, rust-toolchain.toml   Cargo workspace (members crates/*), toolchain 1.98.1
├── crates/                                       the application (roadmap §2 has the full target layout)
│   ├── data-avalanche-core/                      domain types: values, events, positions, tables (no I/O)
│   ├── data-avalanche-config/                    YAML config: parse, resolve, merge, V-rules, reload diff
│   ├── data-avalanche-testkit/                   integration-test helpers (gates, emulator stores)
│   └── …                                         one crate per milestone (storage and state come in M2)
├── test-infra/                                   compose.yaml (PG, MySQL, MariaDB, RustFS, Azurite), up.sh/down.sh, README
├── .github/workflows/                            ci.yml (pull requests only); cloud.yml (real S3/Azure, from M2)
├── doc/
│   ├── specs/                                     source of truth (Oleksii owns)
│   │   ├── System-Overview.md                     top-level requirements (incl. Sinks block)
│   │   ├── Data-Flow.md                           pipeline phases, TableChangeSet, sink contract + realizations
│   │   ├── Configuration_Specification.md         YAML config model, validation rules V-001…V-118
│   │   └── Schema_Change_Detection_and_Evolution.md   DDL detection & per-sink evolution
│   ├── research/                                  Claude authors, Oleksii reviews
│   │   ├── Multi_Sink_Architecture_and_Delta_Lake.md   sink abstraction, Delta sink, D-MS.1–16
│   │   ├── Type_Mapping.md                        DB → ColumnValue → Arrow → sink (Hop 3a Iceberg / 3b Delta)
│   │   ├── Clustering_Architecture_Research.md    two-plane cluster, lease, fencing (D-1…D-11)
│   │   ├── Catalog_Auth_Research.md               Iceberg catalog authentication
│   │   ├── Snapshot_Resumability_and_Consistency.md   incremental chunked snapshot
│   │   ├── Ghost_Row_Dedup_and_Primary_Keys.md    keying, collapse, PK policy
│   │   ├── Delivery_Guarantee_and_Position_Guard.md   SourcePosition, committed_position
│   │   ├── Pipeline_Lifecycle_State_Machine.md    pause/stop/resume/restart/flush
│   │   ├── Postgres_Slot_and_Publication_Lifecycle.md
│   │   ├── MySQL_DDL_Classifier.md
│   │   ├── Iceberg_Partitioning_and_Layout_Evolution.md
│   │   ├── Iceberg_Maintenance.md
│   │   ├── Large_Transaction_Handling.md
│   │   ├── Observability_Metrics_and_Tracing.md
│   │   ├── API_RBAC_and_Versioning.md
│   │   ├── Operational_and_Scope_Gaps_Batch.md
│   │   └── V1_Scope_Decisions.md               v1 decision register D-V1.1–6 + confirmed deferrals
│   ├── v1.0/                                      the v1 build plan (Claude authors, Oleksii approves)
│   │   ├── README.md                              index + status of every plan, conventions, test gates
│   │   ├── 00-roadmap.md                          milestones M0–M12, cross-crate interfaces, decisions
│   │   └── M<n>-<topic>.md                        one detailed, test-first task plan per milestone
│   ├── analysis/
│   │   ├── CDC_Design_Gap_Analysis_v1.md          **current** living gap tracker (DA-* IDs)
│   │   └── CDC_Gap_Analysis_Report.md             legacy tracker (v13.13, G-* IDs) — frozen audit trail
│   └── sources/
│       └── Sources_Index_v3.md                    50-source external index (read-only)
└── prototypes/                                    capability probes (standalone crates, not the app)
    ├── lease-conformance/                         storage CAS lease T1–T10 on real S3/GCS/Azure
    ├── iceberg-capability/                        iceberg-rust gates (append-only finding, G2/G2b/G2c probes)
    ├── delta-capability/                          delta-rs 1.0 gates P1–P10 (MERGE, fence, cost, types, P8 clouds)
    └── source-capability/                         PG pgoutput + MySQL/MariaDB binlog client gates S-PG*/S-MY* (local containers)
```

The prototypes are frozen evidence (excluded from the workspace, their own lockfiles); the
application lives in `crates/`.

---

## Document authority and edit rules

| Directory | Authority | Claude may edit? |
|---|---|---|
| `doc/specs/` | Source of truth | Propose patches; edit only on explicit approval |
| `doc/research/` | Claude authors; Oleksii reviews | Yes — in-place edits/extensions on request |
| `doc/analysis/` | Claude maintains | Yes — in-place updates every gap-analysis cycle |
| `doc/sources/` | Static reference | Never edit |
| `prototypes/` | Claude maintains | Yes — probes are evidence, keep READMEs current with results |
| `doc/v1.0/` | Claude authors; Oleksii approves | Yes — a new or changed plan needs approval before execution; record deviations found during execution in the plan |
| `crates/`, `test-infra/`, `.github/` | Claude implements the approved plan | Yes — through the plan's tasks; changes outside a plan need a request |

When Oleksii says **"update the gap report"** or **"re-run gap analysis"**, edit
`doc/analysis/CDC_Design_Gap_Analysis_v1.md` in place: update the affected rows, record the change
in a dated section (e.g. §25 "Multi-sink re-scope"), and bump `updated:`. Leave
`CDC_Gap_Analysis_Report.md` untouched (legacy audit trail).

When Oleksii says **"write a research doc on X"**, create `doc/research/X.md` following the
frontmatter convention below.

When Oleksii says **"apply this fix to the spec"**, make the surgical `str_replace`-style edit to
the relevant `doc/specs/*.md` file and summarise what changed. For larger spec changes, draft the
patch on scratch copies, get approval, then apply.

---

## Frontmatter convention

Every document must carry a YAML frontmatter block:

```yaml
---
updated: YYYY-MM-DD
domain: <one of: Architecture | Data Types | Configuration | Data Flow | Auth | Clustering | Schema Evolution | Sink>
tags: [comma, separated, tags]
resolves: [DA-xx-n, G-yy]       # gap IDs closed or advanced by this doc (omit if none)
sources: [source name, ...]     # from Sources_Index_v3.md (omit if none)
---
```

---

## Gap analysis conventions

### Severity codes
- 🔴 **Blocker** — implementation cannot start without resolving this
- 🟡 **High** — wrong answer causes correctness bugs or data loss
- 🟢 **Medium** — wrong answer degrades performance or operability

### Status markers
- ✅ **Resolved** — spec or research doc fully closes this gap
- 🔄 **Partial** — progress made; sub-questions remain open
- 🟥 **Open** — no settled answer yet
- 🧪 **Verify** — settled on paper, rests on an unproven external capability (crate / cloud) —
  closed by a prototype, not by a document
- ⏭ **v2** — out of v1 scope (deferred with the Iceberg sink); kept as the v2 backlog, not closed
- 🆕 **New gap** — surfaced by the latest revision cycle

### Gap ID system
- **Current tracker:** `DA-<DOMAIN>-<n>` (`SYS`, `CFG`, `FLOW`, `TYPE`, `SCHEMA`, `CLUS`, `SNAP`,
  `DEDUP`, `CAT`, `SINK`, `SRC`, `OBS`, `API`, `OPS`, `DEP`), cross-cutting `DA-X-<n>`, multi-sink
  `DA-MS-<n>`.
- **Legacy (in parentheses for reconciliation):** `G-1…G-40`, `G-NEW-1…G-NEW-22`, `OQ-*`.
- **Design decisions** live in research docs: `D-<n>` / `D-<gap>.<n>` (e.g. `D-13.1`), multi-sink
  `D-MS.<n>`, v1 scope `D-V1.<n>`, revisions `R-<n>`.
- Probe IDs: lease `T1–T10`, iceberg `G1–G5` (+ `G2b`, `G2c`), delta `P1–P10`, source `S-PG0–8` / `S-MY0–9`.

When closing a gap, mark it ✅ and add a one-line rationale pointing at the doc/section (or probe)
that closes it. **Do not delete resolved gaps** — they are the audit trail. An upstream claim (epic
checkbox, release note) is never proof: verify against the compiled crate or a real service.

---

## Core domain concepts (quick reference)

### Sources
- **PostgreSQL:** logical replication via `pgoutput`, WAL/LSN, replication slots, publications
- **MySQL:** binlog / GTID-set, row-based replication (`binlog_row_image = FULL`); MariaDB via the
  MySQL path — **in v1** (D-V1.5; GTID dialect proven, S-MY9)
- **Client crates (proven 2026-09-26, `prototypes/source-capability/`):** PG `pgwire-replication =0.4.1`
  is transport-only — **the pgoutput decoder is ours**; MySQL/MariaDB `mysql_async =0.37.1` binlog stream
  (+ own MariaDB `GTID_EVENT` decode, + payload guard DA-SRC-12). Install one rustls `CryptoProvider`
  at process start (two providers are linked — DA-DEP-5).

### In-memory format and the sink boundary
- **Apache Arrow** `RecordBatch` — columnar; a **single arrow major** workspace-wide (arrow 59:
  deltalake 1.0 + iceberg-rust `main`/0.11)
- **`ColumnValue` enum** — source-decoded value (incl. `UInt64`, tz-flagged `Timestamp`, `Time`,
  typed `Array`)
- **Collapse** (keep-last by per-change `_seq`, ≤1 delete + ≤1 upsert per key) happens in the core;
  the sink receives a sink-neutral **`TableChangeSet`**
- **Idempotency is correctness** (PK-upsert); position guard and epoch fence are optimizations /
  defense-in-depth

### Sinks (`sink.type`, one per pipeline)
- **`Sink` trait:** `table_state`, `ensure_table`, `evolve` (Applied / Degraded / Unsupported→halt),
  `commit` (atomic per table, idempotent, fenced by `owner_epoch`); declared **capabilities** checked
  at pipeline start
- **Delta Lake (delta-rs 1.0, DataFusion):** one MERGE per table per flush; fence = `txn`
  app-transaction; position = `commitInfo` + own-commit walk-back; truncate = metadata-only delete;
  S3 commits via conditional PUT (no DynamoDB); column mapping **off** (it blocks ADD COLUMN); no
  drop/rename/widen → soft-drop / halt; cost ∝ files touched (random-key churn ≈ full rewrite —
  D-MS.15, DV writer post-v1)
- **Apache Iceberg (iceberg-rust):** MOR equality deletes / COW designed; public `Transaction` API is
  **append-only** on every release so far → mirror gated; fence = `cdc.owner_epoch` table property
  (+ snapshot summary), position = own-snapshot walk-back (DA-X-10); commits are **pinned to the
  fenced base** — `Transaction::commit` otherwise rebases a stale zombie (DA-X-11, probe G2b)
- **Iceberg catalogs:** AWS Glue, Hive Metastore (Thrift: none / LDAP / Kerberos), REST (bearer /
  oauth2 / sigv4); REST's built-in `/v1/oauth/tokens` is deprecated (Java 1.6.0), removed in 2.0 —
  `oauth2` must point to an external IdP. Delta tables are path-based in v1 (no catalog registration)
- **Maintenance:** external for data rewrite/expiry/vacuum; Delta checkpoints + log cleanup built-in

### Type mapping chain
```
DB wire format → ColumnValue (decode) → Arrow + logical tag (accumulate) → sink schema (Hop 3a Iceberg / 3b Delta)
```
Non-trivial cases: `TINYINT(1)` boolean detection via `information_schema`; `BIGINT UNSIGNED` →
`decimal(20,0)`; Delta has no `uuid`/`time` (→ string); Iceberg `timestamp` = no tz but Delta
`timestamp` = with tz — config type overrides use a **sink-neutral logical grammar**.

### Cluster coordination
- **AP plane:** `memberlist`/SWIM (+ Lifeguard) for membership; hash ring is *derived* from
  membership (not gossiped); suspects stay in the ring (flap immunity)
- **CP plane:** per-pipeline lease object in `globals.state`, **compare-and-swap** acquire / renew /
  takeover with a renewal safety deadline; `ClusterReconciler` + per-pipeline `PipelineController`
- **Storage client: `object_store` everywhere** (P-5): S3, GCS (generation preconditions) and
  Azure through one crate. The native per-backend stores in `prototypes/lease-conformance` are the
  proven fallback; T1–T10 are re-proven on `object_store` in M2
- **Single-node** is a one-member cluster with the lease active — same code path

### State storage
- `globals.state`: cluster anchor (`tokens`, `lease_duration`, `lock_backend`, `fingerprint_key`),
  config generation (raw config text snapshots), leases — cross-pipeline only; every file has a
  `version`, updates are conditional writes
- Per-pipeline state (offsets, snapshot cursors, recorded `sink_type`) under the pipeline's
  `state.prefix` / `sink.prefix`

---

## Implementation workflow

- **`main` is off limits.** Never commit to, push to or merge into `main`. This is strictly
  prohibited. Oleksii merges every PR himself.
- **One milestone, one branch, one PR.** Every milestone's planning and implementation happen on
  one branch cut from the current `origin/main` (`git fetch` first), named
  `feat/claude/m<n>-<topic>`. The branch ends as one PR against `main`. The flow, with its pauses:
  1. Create the milestone branch from `main`.
  2. Write the plan, verify its code in a scratch workspace, and present it. **Pause.** Approving
     the plan is not a request to execute it: wait for an explicit request to start.
  3. Execute task by task (one implementer and one reviewer per task). **Pause after every task**
     and wait for Oleksii's "go" before starting the next.
  4. After the last task, **pause**. Start the whole-branch review and its fixes only when Oleksii
     asks.
  5. After the review and its fixes, push the milestone branch and open its PR against `main`.
     Don't merge it.
- **Commits.** Inside this flow, commit as the plan says: one commit per task, plus plan, doc and
  spec commits. Outside the flow, commit, push or open a PR only when asked. Any other change,
  such as a `CLAUDE.md` edit, also goes on its own branch and PR.
- **PR descriptions** carry no link to the Claude session. This overrides any attribution the
  harness suggests for PRs.
- **The spec wins.** When a plan and a spec disagree, stop and raise it. Spec edits need explicit
  approval, even when a plan that proposes them is approved.
- **Plans.** Each `doc/v1.0/M<n>-*.md` follows `doc/v1.0/README.md`: frontmatter, Goal /
  Architecture / Tech Stack / Spec, Global Constraints, Review Focus, Decisions and rulings, a file
  map, and test-first tasks (failing test → expected failure → implementation → passing run →
  commit). The code blocks are replayed in a scratch workspace before the plan is presented.
- **Quality gates** before every commit:
  `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`. Workspace lints: `unsafe_code = "forbid"` (so tests can't call
  `std::env::set_var`); libraries return `thiserror` errors; every public type is re-exported at
  its crate root.
- **Test gates** (`doc/v1.0/README.md`): `DATA_AVALANCHE_IT` (emulators; start with
  `test-infra/up.sh`), `DATA_AVALANCHE_GCS_BUCKET` + `GOOGLE_SERVICE_ACCOUNT_PATH` (real GCS),
  `DATA_AVALANCHE_SKIP_GCS`, `DATA_AVALANCHE_CLOUD=s3|azure` (real clouds). Plain `cargo test` stays
  hermetic.
- **Credentials and keys.** Never print, search for or record credentials, or the path to a key
  file, in docs, code or output. When a command needs Oleksii's credentials, ask him to run it with
  `!`. Never commit key material; tests generate throwaway keys at run time.
- **Verification before claims.** A result that depends on a crate, an emulator or a cloud is
  measured, not assumed, and a plan or README says where each result came from.
- Temporary files go in the session scratchpad, not the repository.

---

## Session startup checklist

For **implementation work**:

1. Read `doc/v1.0/README.md` (status), the roadmap sections the task touches (§2 layout, §3
   interfaces, the milestone's scope and exit), and the current milestone plan.
2. Read the specs and research docs the plan's **Spec** list names.
3. Check `git log` and `git status` before changing anything. Check which branch you're on:
   never work on `main`. After a context reset, trust the SDD ledger and `git log` over memory.

Before responding to any gap-analysis or spec-review request:

1. Read `doc/analysis/CDC_Design_Gap_Analysis_v1.md` (dashboard §3, the latest dated section, and
   the domain tables relevant to the request).
2. Read the specific `doc/specs/` or `doc/research/` files relevant to the request.
3. For a capability question, check the matching `prototypes/*/README.md` first — results there
   outrank documentation claims.
4. Cross-reference with `doc/sources/Sources_Index_v3.md` if the topic maps to a known source.
5. Only then produce analysis or edits.

For a **full re-evaluation cycle**, read all specs, all research docs, the current tracker and the
prototype READMEs before writing.

---

## Commit message conventions

```
spec: <file or topic> — <what changed>        # spec doc edit
research: <topic> — <what this covers>        # new or updated research doc (+ its prototype)
gap: v1 §<N> — <what resolved/surfaced>       # gap tracker update
infra: <topic>                                # CLAUDE.md, repo structure, doc/v1.0 plans
```

Code commits (as each plan task specifies): `build:`, `ci:`, `feat(<crate>):`, `fix(<crate>):`,
`test(<area>):`, `docs(<crate>):`.
