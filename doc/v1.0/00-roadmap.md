---
updated: 2026-09-26
domain: Architecture
tags: [v1, implementation-plan, roadmap, milestones, workspace, crates, interfaces, test-strategy, ci, risks]
sources: []
---

# data-avalanche v1 — Implementation Roadmap

This is the master plan for building v1. It fixes the **workspace layout**, the **cross-crate
interfaces**, the **milestone sequence** with exit criteria, the **test and CI strategy**, the **risk
register**, and the list of **spec edits** the plan depends on. Each milestone gets its own detailed,
test-first task plan (`M<n>-*.md` in this folder), written just before the milestone starts so it can
build on the interfaces that actually landed. That staging was chosen on 2026-09-26.

**Inputs (the design this plan implements):**
- Specs: `doc/specs/System-Overview.md`, `Data-Flow.md`, `Configuration_Specification.md`,
  `Schema_Change_Detection_and_Evolution.md`.
- Research: all of `doc/research/`. The v1-specific decisions are in `V1_Scope_Decisions.md` (D-V1.1–6).
- Evidence: `prototypes/*/README.md`. Probe results outrank documentation claims.
- Gap tracker: `doc/analysis/CDC_Design_Gap_Analysis_v1.md` §26. v1 scope has no 🔴, 🧪 or 🟥 items.

**Planning decisions taken with Oleksii (2026-09-26):**

| # | Question | Decision |
|---|---|---|
| P-1 | Plan shape | This roadmap, plus detailed plans per milestone, written in stages |
| P-2 | First shippable slice | **PostgreSQL → Delta on a single node** (M0–M6) |
| P-3 | Flush default | **30s** (config §9.4.1 and Data-Flow Phase 3 change from 60s); D-V1.3 lag target stands |
| P-4 | MySQL positioning | **Both modes**: `gtid` (new **default**) and `binlog` file/position. File/position mode needs its own probe (M7) |
| P-5 | Storage client for lease/state/config | **`object_store` everywhere** (0.14.2 since 2026-09-26; deltalake 1.0 still pulls its own 0.13, see R-13). T1–T10 conformance is re-proven on it against real S3/GCS/Azure before the lease is trusted (M2) |
| P-6 | Deliverables | Binary, **container image**, **Kubernetes manifests + Helm chart**, **CI** (GitHub Actions), **documentation site** (MkDocs): architecture and internal flows, REST API reference, configuration reference with detailed examples, operator runbooks |
| P-7 | S3 emulator | **RustFS** (not MinIO) |
| P-8 | Source versions | PostgreSQL 14–18, MySQL 8.4 LTS, MariaDB 10.11 / 11.4 / 11.8 LTS. CI runs the newest of each on every change and the full matrix nightly |
| P-9 | Plan executor | Claude agents, **subagent-driven** (confirmed 2026-09-26): a fresh agent implements each task and a fresh reviewer checks it before the next starts; a whole-branch review closes each milestone. Tasks give exact files, full test code, commands, expected output, and one commit per task |
| P-10 | GCS test backend | **A real GCS bucket**, locally and in PR CI (fake-gcs-server can't serve `object_store`, measured in M0 planning). CI needs secret `GCP_SA_KEY` and variable `DATA_AVALANCHE_GCS_BUCKET`; fork PRs skip GCS with a notice. S3 and Azure use RustFS and Azurite |

---

## 1. Scope recap (what v1 is)

- **Sources:** PostgreSQL (`pgoutput`, proto v1, our own decoder over `pgwire-replication =0.4.1`),
  MySQL 8.4 and MariaDB (`mysql_async =0.37.1` binlog stream; GTID and file/position modes; MariaDB GTID
  dialect; compressed-transaction check).
- **Sink:** Delta Lake mirror (`deltalake =1.0.0`, one MERGE per table per flush, `txn` fence,
  `commitInfo` position). `sink.type: iceberg` is accepted by validation and refused at pipeline start
  (D-V1.4).
- **Runtime:** incremental chunked snapshot (stream-first, watermarks), keep-last collapse, idempotent
  PK-upsert, position guard, split commits for oversized transactions, the four-channel schema
  detection with sink-mediated evolution, per-table halts.
- **Cluster:** SWIM membership, a derived consistent-hash ring, a per-pipeline storage CAS lease with a
  renewer state machine, a `ClusterReconciler` plus one `PipelineController` per pipeline, planned
  drain and unplanned takeover, config generations in cloud storage. A single node is a one-member
  cluster.
- **Operations:** management REST API (`/v1`, four auth modes, RBAC), Prometheus metrics, optional
  OTLP tracing, structured logs.
- **Lag targets (D-V1.3):** steady-state p99 ≤ 60s; ≤ 2 min to recover after an owner failure, at
  default config.
- **Out of v1:** Iceberg sink, `changelog` write mode, Delta catalog registration, secret-manager
  integration, `kubernetes` lock backend, per-endpoint takeover bulkheads, spill-to-disk, cross-table
  atomicity, the Delta deletion-vector writer.

---

## 2. Workspace and crate layout

A Cargo workspace at the repository root. `prototypes/` stays out of it: the probes are frozen evidence
with their own lockfiles.

```
data-avalanche/
├── Cargo.toml                    workspace: members = ["crates/*", "xtask"], exclude = ["prototypes"]
├── rust-toolchain.toml           pinned stable toolchain (1.98.1)
├── .github/workflows/            ci.yml (per change) · nightly.yml (matrix) · cloud.yml (real clouds, scheduled/manual)
├── test-infra/compose.yaml       PG 14–18, MySQL 8.4, MariaDB 10.11/11.4/11.8, RustFS, Azurite (GCS = real bucket)
├── crates/
│   ├── data-avalanche-core/           domain types: values, events, positions, schema model, change set, Sink trait, errors
│   ├── data-avalanche-config/         YAML model, strict parse, env substitution, merge, V-rules, resolution, diff
│   ├── data-avalanche-storage/        object_store backends from globals.cloud, path rules (config §10), VersionedStore (CAS)
│   ├── data-avalanche-state/          per-pipeline state files + globals.state (anchor, config generations)
│   ├── data-avalanche-pipeline/       accumulator, collapse, flush policy, schema apply protocol, snapshot orchestration,
│   │                             PipelineWorker task graph, checkpointer, lifecycle state machine
│   ├── data-avalanche-source-pg/      pgoutput decoder, type decode, replication client, slot/publication lifecycle,
│   │                             catalog channels A/C/D, keyset chunk reader
│   ├── data-avalanche-source-mysql/   binlog stream, GTID / MariaDB GTID / file-pos positions, payload check, TABLE_MAP
│   │                             decode, DDL classifier, catalog channels, chunk reader
│   ├── data-avalanche-sink-delta/     Delta realization of the Sink trait
│   ├── data-avalanche-cluster/        membership, ring, lease + renewer, ClusterReconciler, PipelineController,
│   │                             config-generation coordinator
│   ├── data-avalanche-api/            axum REST API, auth (none/basic/jwt/oauth2), RBAC, OpenAPI (utoipa)
│   ├── data-avalanche-telemetry/      metrics registry + catalogue, tracing/OTLP setup, log setup
│   ├── data-avalanche-testkit/        test helpers: compose service handles, fixture builders, in-memory sink, fakes
│   └── data-avalanche/           the binary: CLI, wiring, crypto-provider install, signals
├── xtask/                        repo automation (docs build, config-reference generation, release helpers)
├── deploy/
│   ├── docker/Dockerfile
│   └── helm/data-avalanche/      StatefulSet, headless Service, ConfigMap, probes
└── docs/                         MkDocs site (mkdocs.yml at repo root)
```

**Dependency direction** (arrows point at dependencies; nothing points up):

```
data-avalanche (bin) ─► api ─► cluster ─► pipeline ─► core
      │             │        │          │  ▲
      │             │        │          ├──┼── source-pg ─► core
      │             │        │          ├──┼── source-mysql ─► core
      │             │        │          └──┼── sink-delta ─► core, storage
      │             │        └─► state ─► storage ─► core
      └────────────►┴────────┴─► config ─► core            telemetry ◄─ (all runtime crates)
```

Rules:
- `data-avalanche-core` has **no I/O** and no async runtime dependency beyond trait definitions
  (`async-trait`).
- Source crates never depend on sink crates, or the reverse. Only `data-avalanche-pipeline` joins them,
  through `core` traits.
- One `arrow` major workspace-wide (**59**, pinned by `deltalake =1.0.0`), per D-MS.14.
- Every binary calls `rustls::crypto::CryptoProvider::install_default()` (aws-lc-rs) before any TLS
  (DA-DEP-5).
- Libraries return `thiserror` errors; `anyhow` only in the binary, `xtask` and tests.

### 2.1 Pinned external crates (exact pins come from the probes)

| Purpose | Crate | Pin | Evidence |
|---|---|---|---|
| Delta sink | `deltalake` (features `datafusion`, `rustls`, `s3`, `gcs`, `azure`) | `=1.0.0` | delta P1–P10, P8 on S3/GCS/Azure |
| Object storage / CAS | `object_store` | `0.14.2` (bumped from 0.13.2 on 2026-09-26; deltalake 1.0 requires `^0.13.2`, so M4 adds a second copy, R-13) | API checked; RustFS/Azurite/real-GCS CAS re-run on 0.14.2; T1–T10 re-proof in M2 |
| Arrow | `arrow` / `parquet` | `59` (as resolved by deltalake) | P9 |
| PG replication transport | `pgwire-replication` | `=0.4.1` | S-PG0–8 |
| PG SQL (catalog, chunks) | `tokio-postgres` + `tokio-postgres-rustls` | `0.7` | S-PG probes |
| MySQL binlog + SQL | `mysql_async` / `mysql_common` | `=0.37.1` / `=0.37.3` | S-MY0–9 |
| DDL classifier | `sqlparser` | latest 0.x, pinned in M7 | MySQL_DDL_Classifier §5 |
| YAML 1.2 (serde) | `serde-saphyr` | `1.3` | verified at the start of M1 (fallback `serde_yaml_ng`) |
| Membership | `memberlist` `0.8.5` (fallback `foca` `2.0`) | pinned in M8 | **unprobed** — M8 opens with a probe gate |
| HTTP API | `axum` `0.8` + `utoipa` `6` | | |
| Metrics | `prometheus-client` | | |
| Tracing | `tracing`, `tracing-subscriber`, `opentelemetry-otlp` | | |
| CLI | `clap` `4` | | |
| Misc | `thiserror` 2, `async-trait`, `bytes`, `chrono`, `uuid`, `serde_json`, `humantime`, `xxhash-rust` (ring hash, anchor `hash_algo: xxh3`), `proptest` (dev) | | |

---

## 3. Cross-crate interfaces (the contracts milestone plans implement)

Shapes are normative; exact signatures are fixed by the milestone that owns them and recorded back
here. `data-avalanche-core` owns every type in this section unless noted.

### 3.1 Values and events (Data-Flow §CdcEvent, Type_Mapping)

```rust
pub struct TableRef { pub schema: String, pub name: String }        // MySQL: schema = source.db

pub enum ColumnValue {
    Null, Bool(bool), Int16(i16), Int32(i32), Int64(i64), UInt64(u64),
    Float32(f32), Float64(f64),
    Numeric(Decimal),                        // see D-IMPL-1: i128 unscaled + scale (38 digits)
    Text(String), Bytes(Vec<u8>),
    Timestamp { micros: i64, with_tz: bool },   // µs since Unix epoch (= Arrow range), UTC instant or wall clock
    Date(i32),                               // days since Unix epoch = Arrow Date32; PG ±infinity clamps (Q-M5-1)
    Time(chrono::NaiveTime),
    Uuid(uuid::Uuid), Json(serde_json::Value),
    Array { element_type: ColumnValueType, values: Vec<ColumnValue> },
}
pub enum ColumnValueType { Bool, Int16, Int32, Int64, UInt64, Float32, Float64, Numeric { precision: u8, scale: i8 },
                           Text, Bytes, Timestamp { with_tz: bool }, Date, Time, Uuid, Json, Array(Box<ColumnValueType>) }
pub enum LogicalType { Bool, Int16, Int32, Int64, Float32, Float64, Decimal { precision: u8, scale: i8 }, String,
                       Binary, Date, Time, TimestampTz, TimestampNtz, Uuid, Json, List(Box<LogicalType>) }
    // grammar shared with config `column_type_overrides` (V-078/V-087); parse + Display round-trip

pub enum Op { Insert, Update, Delete, Snapshot }
pub struct CdcEvent {
    pub op: Op, pub table: TableRef,
    pub before: Option<Row>, pub after: Option<Row>,
    pub source_pos: SourcePosition,          // commit position of the enclosing transaction
    pub change_seq: SourceSeq,               // per-change order (keep-last key, `_seq`)
    pub xid: u64, pub commit_ts_micros: i64,
}
impl CdcEvent { pub fn validate(&self) -> Result<(), EventShapeError>; }   // image shape per op
pub enum EventShapeError { MissingAfter{..}, MissingBefore{..}, UnexpectedBefore{..}, UnexpectedAfter{..} }
pub struct TransactionBoundary { pub xid: u64, pub commit_pos: SourcePosition, pub commit_ts_micros: i64 }
pub struct Row(pub Vec<(String, ColumnValue)>);  // the PG unchanged-TOAST mask (D-IMPL-3) needs a shape: Q-M3-1
// Landed in M0 (2026-09-26): all of the above, plus Decimal { unscaled: i128, scale: i8 } with parse / precision /
// rescale / fits (Eq/Hash compare the representation: rescale before keying by value).
```

### 3.2 Positions (Delivery §2; P-4 adds file/position)

```rust
pub enum SourcePosition {
    Pg(Lsn),                                 // u64; "X/Y" text form
    MySqlGtid(GtidSet),                      // canonical: UUIDs ascending, merged intervals
    MariaDbGtid(MariaGtidState),             // per-domain (server_id, seq); "d-s-n,d-s-n" ascending by domain
    BinlogFile(BinlogFilePos),               // (file base, file index, pos); ordered by (index, pos)
}
impl SourcePosition {
    pub fn covers(&self, other: &Self) -> Result<bool, PositionError>;     // dialect mismatch = error
    pub fn union(&self, other: &Self) -> Result<Self, PositionError>;
    pub fn intersect(&self, other: &Self) -> Result<Self, PositionError>;
    pub fn to_tagged(&self) -> String;                                     // "pg:0/16B3748", "mysql-gtid:…",
    pub fn parse_tagged(s: &str) -> Result<Self, PositionError>;           // "mariadb-gtid:…", "binlog:…"
    pub fn dialect(&self) -> &'static str;                                 // "pg" | "mysql-gtid" | "mariadb-gtid" | "binlog"
}
pub enum PositionError { DialectMismatch { left, right }, DifferentBinlogSeries { left, right }, Parse { input, reason } }
// Component types live in `data_avalanche_core::position` (Lsn, GtidSet + GtidSource, MariaGtidState + MariaGtid,
// BinlogFilePos, parse_binlog_index); Display/FromStr of each is the untagged form. Serde of SourcePosition = tagged string.
pub enum SourceSeq { Pg(Lsn), Binlog { file_index: u64, end_log_pos: u64 } }   // total order within one stream
```

### 3.3 Schema model and change set (Schema_Change §7, Multi_Sink §6.2)

```rust
pub struct ColumnSchema { pub name: String, pub value_type: ColumnValueType, pub logical: LogicalType,
                          pub nullable: bool, pub source_type: String, pub source_ordinal: Option<i32> /* PG attnum */ }
pub struct TableSchema { pub table: TableRef, pub columns: Vec<ColumnSchema>, pub identifier: Vec<String> }

pub enum SchemaChange { AddColumn(ColumnSchema), DropColumn(String), RenameColumn { from: String, to: String, proven: bool },
                        ChangeType { column: String, from: ColumnValueType, to: ColumnValueType },
                        SetNotNull(String), DropNotNull(String), IdentifierChanged, TableDropped, Other(String) }
pub struct SchemaDelta { pub table: TableRef, pub changes: Vec<SchemaChange> }
pub enum DeltaClass { Safe, Unsafe { reason: HaltReason }, Ignorable }
pub fn classify(delta: &SchemaDelta) -> Vec<(SchemaChange, DeltaClass)>;   // the §7.2 table as code

pub struct TableChangeSet {
    pub table: TableRef, pub schema: arrow::datatypes::SchemaRef, pub identifier: Vec<String>,
    pub truncate: bool, pub upserts: Vec<RecordBatch>, pub deletes: Vec<RecordBatch>,
    pub position: PositionRange, pub split: Option<SplitInfo>,
}
pub enum HaltReason { ReservedColumnCollision { column: String }, TableDropped, UnsafeDdl { detail: String },
                      NoIdentifier, IdentifierNotInReplicaIdentity { missing: Vec<String> }, EvolutionUnsupported { detail: String },
                      EvolutionDisabled, NumericOverflow { column: String },
                      InvalidDateValue { column: String, raw: String },   // MySQL/MariaDB zero-in-part / invalid dates (Q-M7-1)
                      /* … extended by milestones */ }
// Pipeline-scope halts (not per table): BINLOG_PAYLOAD_INVALID (D-V1.6), BINLOG_SOURCE_CHANGED (Q-M7-2)
```

### 3.4 Sink trait (Multi_Sink §6.3–6.4)

```rust
#[async_trait]
pub trait Sink: Send + Sync {
    fn kind(&self) -> SinkKind;
    fn capabilities(&self) -> &SinkCapabilities;
    async fn table_state(&self, t: &TableRef) -> Result<SinkTableState, SinkError>;
    async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError>;
    async fn evolve(&self, t: &TableRef, d: &SchemaDelta) -> Result<EvolveOutcome, SinkError>; // Applied|Degraded|Unsupported
    async fn commit(&self, cs: TableChangeSet, m: CommitMarker) -> Result<CommitOutcome, SinkError>; // Committed|SkippedCovered|Fenced
}
pub struct CommitMarker { pub owner_epoch: u64, pub committed_position: SourcePosition, pub pipeline_id: String }
```

### 3.5 Storage CAS (`data-avalanche-storage`, P-5)

```rust
pub struct ObjectVersion { pub e_tag: Option<String>, pub version: Option<String> }   // = object_store::UpdateVersion
#[async_trait]
pub trait VersionedStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError>;
    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError>;                 // Conflict if present
    async fn update(&self, key: &str, expected: &ObjectVersion, body: Bytes) -> Result<ObjectVersion, StoreError>; // Conflict if moved
    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError>;                    // unconditional (immutable snapshots)
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}
pub enum StoreError { Conflict, NotFound, Transient(String), Permanent(String) }   // renewer taxonomy (Clustering §19.2)
```

Lease release is a **tombstone write** (object_store has no conditional delete), as in
`prototypes/lease-conformance`.

### 3.6 Source connector (`data-avalanche-pipeline` owns the trait; each source crate implements it)

```rust
#[async_trait]
pub trait SourceConnector: Send + Sync {
    async fn preflight(&self) -> Result<Preflight, SourceError>;                               // config §11.2 source rows
    async fn resolve_tables(&self, f: &TableFilter) -> Result<Vec<ResolvedTable>, SourceError>; // channel A (+ skips/halts)
    async fn current_position(&self) -> Result<SourcePosition, SourceError>;                  // watermarks lw/hw
    async fn read_chunk(&self, t: &TableSchema, after: Option<&KeysetCursor>, limit: usize) -> Result<Chunk, SourceError>;
    async fn open_stream(&self, from: &SourcePosition, tables: &[TableRef]) -> Result<Box<dyn ChangeStream>, SourceError>;
    async fn describe(&self, t: &TableRef) -> Result<Option<TableSchema>, SourceError>;         // channel C; None = gone
    async fn rescan(&self, tables: &[TableRef]) -> Result<RescanReport, SourceError>;          // channel D
}
#[async_trait]
pub trait ChangeStream: Send {
    async fn next(&mut self) -> Result<StreamItem, SourceError>;
    async fn acknowledge(&mut self, durable: &SourcePosition) -> Result<(), SourceError>;       // PG status update; MySQL no-op
}
pub enum StreamItem { Begin { xid: u64, commit_pos: SourcePosition, commit_ts_micros: i64 },
                      Change(CdcEvent), Truncate { tables: Vec<TableRef>, seq: SourceSeq },
                      SchemaSignal { table: TableRef, seq: SourceSeq, hint: SchemaHint },       // PG Relation diff / MySQL DDL
                      Commit(TransactionBoundary), KeepAlive(SourcePosition) }
```

### 3.7 Cluster (Clustering §19–§20)

`LeaseStore` over `VersionedStore` (acquire / renew / takeover-evaluate / release). A `Renewer` state
machine (`Held | Renewing | Degraded | Lost`) exposes `may_commit()` = state ∈ {Held, Degraded} ∧
now < deadline. `PipelineController` is the sole lease writer and owns the `PipelineWorker`.
`ClusterReconciler` pushes `Own/DontOwn` targets. The same types run in single-node mode; M6 ships them
with a self-only ring.

---

## 4. Milestones

Legend: **Deps** = milestones that must be merged first. **Exit** = the acceptance gate; nothing merges
to `main` in a milestone until its exit criteria hold on CI.

```
M0 ──► M1 ──┐
  ├──► M2 ──┼──► M4 ──┐
  └──► M3 ──┴──► M5 ──┴──► M6 (slice 1: PG→Delta single node) ──► M7 ──┐
                                                          ├──► M8 ──────┼──► M9 ──► M11 ──► M12 (v1.0.0)
                                                          └──► M10 (continuous from M6) ────┘
```

Parallel lanes after M0: **{M1}**, **{M2}** and **{M3}** are independent. M4 needs M2 and M3. M5 needs
M1 and M3.

### M0 — Foundations

Detailed plan: `M0-foundations.md`.
- **Scope:**
  - Workspace scaffold, pinned toolchain, lints, CI skeleton.
  - `data-avalanche-core` domain types: TableRef and reserved names, values, `Decimal`, `LogicalType`
    grammar, all four position dialects, `SourceSeq`, `CdcEvent`.
  - `test-infra/compose.yaml` with every database and storage emulator.
  - `data-avalanche-testkit` service handles.
  - A RustFS conditional-write check that decides whether RustFS can carry the CAS tests.
- **Deps:** none.
- **Exit:**
  - `cargo fmt --check`, `clippy -D warnings` and `cargo test --workspace` are green on CI.
  - Position algebra has property tests (covers/union/intersect laws per dialect).
  - `compose up --wait` brings up the default profile, and the testkit smoke test passes against every
    service.
  - The RustFS conditional-write result is recorded in `test-infra/README.md`.

### M1 — Configuration (`data-avalanche-config`)

- **Scope:**
  - The full config §3–§10 model: every field, default, union and tri-state, including the Iceberg
    blocks, because they must validate even though v1 refuses them at start.
  - Strict YAML 1.2 parse (`deny_unknown_fields`).
  - `${VAR}` / `${VAR:-d}` / `$${` substitution on string leaves, then type coercion.
  - Phase 1 structural validation on unresolved values, via a `Resolvable<T>` leaf that is literal or
    template.
  - Phase 2 resolution: env vars, `credentials_file`, `token_file`, PEM material, other files.
  - Merge semantics §8, including union subtrees and list replacement.
  - The **whole V-rule table** (V-001…V-116, with the P-3/P-4 changes) as data-driven validators with
    locus and severity.
  - The resolved-config type (§11.1), the diff with R/PR/PROC classification (§12.2–12.3), and the
    validation report (JSON, used later by the API).
- **Deps:** M0.
- **Exit:**
  - Every V-rule has at least one passing and one failing fixture.
  - The §14 annotated example (updated per P-3/P-4) parses and resolves.
  - Diff classification is tested for each mutability class and for conditional PR (`tables.include`
    only when the resolved set changes, which needs a resolver callback).
  - Error messages carry the YAML path and, when available, line/column.

### M2 — Storage and state (`data-avalanche-storage`, `data-avalanche-state`)

- **Scope:**
  - `object_store` backends from `globals.cloud`: S3 via the default chain, GCS via ADC, Azure via
    managed/workload identity plus `globals.azure.account_name`, and emulator endpoints for tests.
  - Path construction (config §10).
  - `VersionedStore` and its error mapping, including GCS generation and S3 409.
  - Port of **T1–T10** to `VersionedStore`, run in PR CI against RustFS, Azurite and a **real GCS
    bucket** (P-10), and against **real S3 and Azure** in `cloud.yml` (the P-5 gate).
  - Lease tombstone semantics.
  - Per-pipeline state documents (`pipeline-state.json` with `sink_type` and `desired_state`,
    `replication/slot.json`, `replication/position.json` (tagged position for all MySQL dialects + source
    server identity), `snapshots/<schema>.<table>.json`),
    with a `version` field and CAS updates.
  - `globals.state` documents: cluster anchor (first-writer-wins; mismatch → refuse), config generation
    pointer and immutable snapshots.
- **Deps:** M0.
- **Exit:**
  - T1–T10 pass on all three emulators, or are documented as skipped where an emulator is loose; the
    recorded run shows them passing on real S3, GCS and Azure.
  - State round-trips and CAS races (N writers → 1 winner) pass.
  - Anchor mismatch detection is tested.

### M3 — Core data path (`data-avalanche-core` schema model + `data-avalanche-pipeline` accumulator)

- **Scope:**
  - Schema model and the §7.2 classifier (every row as a test case, including the reserved-name and
    `DROP TABLE` rows).
  - Arrow schema derivation with the `data-avalanche.logical` field metadata (Hop 2).
  - Per-table accumulator: builders, the lineage columns `_op _ts _lsn _seq _source_db _pipeline`,
    byte estimate.
  - Transaction-boundary tracking.
  - Flush policy: OR semantics, tri-state, 30s default, boundary-only flushes.
  - **Collapse** (keep-last by `_seq`, ≤ 1 delete + ≤ 1 upsert per key, PK change = two keys).
  - TRUNCATE barrier.
  - Split commits with the checkpoint pinned at transaction start.
  - `max_transaction_buffer_bytes` halt.
  - TOAST carry-forward inside the batch (D-IMPL-3).
  - `TableChangeSet` assembly.
  - The `Sink` trait and an **in-memory reference sink** that implements the contract exactly
    (idempotent upsert map, fence, guard), used by M5/M6 tests.
- **Deps:** M0.
- **Exit:**
  - Property tests: collapse is idempotent and order-insensitive to batch boundaries (replaying a
    stream split at any boundary yields the same final table in the reference sink).
  - Flush policy is tested under simulated time.

### M4 — Delta sink (`data-avalanche-sink-delta`)

- **Scope:**
  - Capabilities.
  - `table_state` (`_delta_log` exists; foreign-format detection).
  - `ensure_table`: column mapping off, deletion vectors off, checkpoint interval, log retention,
    allow-listed properties, `timestampNtz` per `timestamp_ntz_as`.
  - Hop 3b conversions: uuid → string, time → string, fixed → binary, ntz options.
  - MERGE with the `__data_avalanche_del` flag and optional partition pruning; TOAST-aware update expressions
    (D-IMPL-3).
  - Fence pre-check plus the `txn` carrier; `commitInfo` position; own-commit walk-back guard.
  - Truncate as a metadata-only delete.
  - `evolve`: add column (Applied), soft-drop (Degraded plus metric), rename → Degraded, widen →
    Unsupported.
  - Retry and conflict handling (`ConcurrentTransaction` on the owner `app_id` → `Fenced`).
  - Checkpoint and log-cleanup hooks.
  - Merge metrics.
- **Deps:** M2, M3.
- **Exit:**
  - The delta-capability probes P1–P7 and P10 are re-run as integration tests against the crate on a
    local filesystem and RustFS.
  - Conformance with the M3 reference sink: the same change-set sequence gives the same table content.
  - A foreign OPTIMIZE doesn't disarm the fence or the guard.

### M5 — PostgreSQL source (`data-avalanche-source-pg`)

- **Scope:**
  - pgoutput v1 decoder, production grade: all messages, text and binary tuples, unchanged-TOAST,
    origin, type messages.
  - OID → `ColumnValue` decode for the whole Type_Mapping PG table, plus the unsupported-type fallback
    chain, `numeric` precision cap and array element resolution.
  - Replication client wrapper: session pinning, status/wakeup interval = heartbeat, acknowledgment,
    reconnect with backoff.
  - Slot state machine (D-15.1–15.3: managed / require_existing, orphan policy, WAL-gone → re-snapshot).
  - Publication reconciliation (D-15.4).
  - Preflight checks (config §11.2 PG rows).
  - Catalog channels: A (baseline, identifier, replica identity, reserved names), C (`attnum` diff,
    rename proof), D (batched rescan, **dropped OID → `TableDropped`**).
  - Keyset chunk reader with a `COLLATE "C"` cursor and `pg_current_wal_lsn()` watermarks.
  - `max_wal_lag_bytes` guard.
- **Deps:** M1, M3.
- **Exit:**
  - Against PG 18 (every change) and PG 14–17 (nightly), every probe gate S-PG0…8 has a regression test.
  - A type round-trip suite covers every mapped type.
  - The slot state-machine table is exercised cell by cell.

### M6 — Pipeline runtime and single-node control (**slice 1: PG → Delta, single node**)

- **Scope:**
  - `PipelineWorker` task graph (Data-Flow concurrency model), with bounded channels
    (`buffer_capacity_rows`).
  - Phase 0: state load, `snapshot_mode` evaluation per table, table resolution, sink preflight,
    D-V1.4 Iceberg refusal, recorded `sink_type` check.
  - Snapshot orchestration: stream-first chunk loop, lw/hw reconciliation against the live stream,
    cursor checkpoint per chunk, `parallelism`, per-table timeout, dump modes.
  - Streaming, the sink writer and Phase 5 acknowledgment.
  - **Checkpoint = flush frontier, persisted after every table in the flush reports Committed or
    SkippedCovered** (D-IMPL-2).
  - Schema apply protocol §8 (seal → flush → classify → evolve → rebuild → resume), including DDL
    idempotency on replay.
  - Per-table halt registry with reasons.
  - Error paths: source reconnect, sink retry → DEGRADED, Fenced → stop.
  - Lifecycle state machine: desired state persisted; pause, stop and resume flush to a boundary.
  - Lease + renewer + `PipelineController` in single-node form: self-only ring, one `ClusterReconciler`
    tick.
  - Binary: `data-avalanche run --config <path>` with CLI log overrides, and a minimal HTTP listener serving
    `/health/live`, `/health/ready` and `/metrics`.
  - Metrics for everything built so far.
- **Deps:** M1, M2, M3, M4, M5.
- **Exit:** the **slice-1 E2E suite** passes on PG 18 + RustFS and nightly on PG 14–17. See §5.3 for
  the scenario list.

### M7 — MySQL and MariaDB source (`data-avalanche-source-mysql`)

- **Scope:**
  - **Probe S-MY10 first:** file/position resume, rotation, watermark via `SHOW BINARY LOG STATUS`
    (MySQL 8.4) / `SHOW BINLOG STATUS` (MariaDB), position behaviour across a server restart, and the
    server-identity check (`@@server_uuid` / `@@server_id`).
  - File/position mode halts the pipeline with `BINLOG_SOURCE_CHANGED` when the connected server's
    identity differs from the recorded one (Q-M7-2); a purged file follows `binlog_purge_action`.
  - Date/time edge values (Q-M7-1): all-zero `DATE`/`DATETIME`/`TIMESTAMP` → NULL; zero-in-part and
    impossible dates (and all-zero in a key column) → table halt `INVALID_DATE_VALUE`; a `string`
    override carries the raw server text. Tests cover MariaDB's default `sql_mode` (which accepts
    zero-in-part dates) and MySQL with `sql_mode=''` / `ALLOW_INVALID_DATES`.
  - Binlog stream (`mysql_async`): GTID mode (MySQL), MariaDB GTID (connect-state variables, our own
    `GTID_EVENT` decode) and file/position mode.
  - Heartbeat period.
  - **`TRANSACTION_PAYLOAD` check** → pipeline halt `BINLOG_PAYLOAD_INVALID` (D-V1.6).
  - TABLE_MAP decode with FULL metadata: names, signedness (read correctly), ENUM/SET strings.
  - Type decode for the Type_Mapping MySQL table, including `TINYINT(1)` detection, `UInt64` and
    ENUM/SET.
  - Two-tier DDL classifier (sqlparser MySqlDialect → tokenizer fallback) routing to channel C;
    TRUNCATE/DROP/RENAME table handling.
  - Preflight (config §11.2 MySQL rows).
  - Catalog channels over `information_schema`.
  - Chunk reader with `utf8mb4_bin` ordering.
  - `binlog_purge_action`.
  - Spec edits for P-4 land before this milestone.
- **Deps:** M6.
- **Exit:**
  - Slice-1 E2E parity suite on MySQL 8.4 and MariaDB 11.8 (every change) and MariaDB 10.11 / 11.4
    (nightly), across all three position modes.
  - Payload-check fault injection (corrupted payload) halts with no silent loss.

### M8 — Cluster (`data-avalanche-cluster`)

- **Scope:**
  - **Probe gate first:** `memberlist` 0.8.5 over TCP/UDP with TLS; node metadata; join/leave/dead
    detection; Lifeguard. Fallback: `foca`.
  - NodeMeta, phases and incarnation.
  - Ring derived from membership (xxh3, `tokens`), with flap rules (D-11).
  - `ClusterReconciler`; `PipelineController` takeover-evaluation and planned drain with lease release.
  - `max_concurrent_handoffs` / `max_concurrent_takeovers` with jitter.
  - Config-generation coordinator: publish via CAS, gossip nudge plus poll, cold-start adoption (D-3).
  - Anchor validation (V-062 / V-076 / V-094).
  - Discovery: `static` and `dns` with periodic re-resolve.
- **Deps:** M2, M6.
- **Exit:** a multi-node E2E suite with 3 in-process nodes plus a docker-compose 3-node run. It covers:
  - kill → takeover within the D-6 window, with no double commit (verified from Delta `txn` versions);
  - graceful leave → immediate acquire;
  - a GC-pause simulation (a zombie is fenced);
  - partition (gossip split with storage reachable);
  - rolling restart with bounded handoffs;
  - a reload that propagates to every node.

### M9 — Management API (`data-avalanche-api`)

- **Scope:**
  - axum server; TLS (`globals.api.tls`).
  - Auth: none / basic / jwt (JWKS) / oauth2 (OIDC discovery), with `role_mapping`.
  - RBAC matrix (API_RBAC §2).
  - Every `/v1` endpoint in System-Overview's table, including pipelines list/status with halted-table
    reasons, snapshot jobs (on-demand chunk loop; jobs persisted under `snapshots/jobs/`, listed via
    `GET /v1/pipelines/{id}/snapshots` after a restart or failover, running jobs resumed by the owner —
    Q-M9-1), lifecycle actions with `409`, config
    validate/reload/status (per-node applied generation), cluster status, leave and shutdown.
  - OpenAPI generated with `utoipa`, served at `/v1/openapi.json`.
- **Deps:** M6 (M8 for reload propagation and cluster endpoints).
- **Exit:** an endpoint × role matrix test (401 vs 403); lifecycle transitions and `409`s; the OpenAPI
  document is validated and snapshot-tested.

### M10 — Observability completion (`data-avalanche-telemetry`)

- **Scope:**
  - The full metric catalogue with the cardinality policy.
  - OTLP tracing: spans per flush/commit, snapshot chunk and DDL apply; sampling; `trace_id` / `span_id`
    in JSON logs.
  - `globals.log` level/format, overridden by CLI.
  - Example Prometheus alert rules (lag > 60s for 5 min, lease degraded, halted tables, WAL lag).
- **Deps:** M6; runs continuously alongside M7–M9.
- **Exit:** a metric catalogue test asserts every mandatory series exists with the right labels; a
  tracing smoke test runs against an OTLP collector container.

### M11 — Packaging and documentation

- **Scope:**
  - Multi-stage Dockerfile (distroless or `debian-slim` runtime, non-root).
  - Helm chart: StatefulSet (D-1), headless Service, `node_name` from the Downward API, ConfigMap,
    probes, PDB, resource defaults.
  - Release workflow: image and chart publish, SBOM.
  - **MkDocs site** (`mkdocs-material`):
    - architecture and internal flows (from the specs, with diagrams);
    - REST API reference rendered from the OpenAPI document;
    - configuration reference generated by `xtask` from the config model, plus detailed examples for
      each source type (PG, MySQL, MariaDB), each position mode, each cloud and single-node vs cluster;
    - operator runbooks: IAM/privilege prerequisites (G-37), halted table, slot or binlog loss and
      re-snapshot, lease/failover, config reload and the "restart ≠ reload" rule, upgrade.
- **Deps:** M9 (API reference), M10.
- **Exit:**
  - `helm lint` and `helm template` pass.
  - A kind-cluster smoke deploy of 3 replicas runs one pipeline end to end.
  - `mkdocs build --strict` passes.
  - Every config example in the docs parses and validates (tested by `xtask`).

### M12 — Hardening and v1 acceptance

- **Scope:**
  - Load tests measuring the D-V1.3 targets, in steady state and across a failover.
  - Long soak (24h) with churn.
  - Chaos: storage outage shorter and longer than the lease, source restart, network partition.
  - The real-cloud matrix: S3, GCS, Azure × PG and MySQL.
  - The nightly DB version matrix green for 7 consecutive runs.
  - A dependency audit: `cargo audit` clean, or every remaining advisory triaged and recorded (the CI
    step is non-blocking, so this is where the audit is enforced).
  - Tag `v1.0.0`.
- **Deps:** all of the above.
- **Exit:** the acceptance checklist in §8.

---

## 5. Test strategy

### 5.1 Layers

| Layer | Where | Runs |
|---|---|---|
| Unit + property (`proptest`) | in-crate `#[cfg(test)]` | every change |
| Contract tests | `data-avalanche-testkit` reference sink / fake source vs real implementations | every change |
| Integration (containers) | `crates/*/tests/*.rs`, gated on `DATA_AVALANCHE_IT=1`, using `test-infra/compose.yaml` | every change (newest versions), nightly (matrix) |
| E2E scenarios | `crates/data-avalanche/tests/e2e_*.rs` — real binary against containers | every change (slice suites), nightly (full) |
| Real-cloud | `cloud.yml`, gated on `DATA_AVALANCHE_CLOUD=s3\|azure` + secrets (GCS already runs in `ci.yml`, P-10) | scheduled weekly + manual dispatch + before release |
| Load / soak / chaos | `xtask bench`, `xtask soak` | M12, then before each release |

Conventions:
- Integration tests are skipped unless `DATA_AVALANCHE_IT` is set and non-empty. This is the `SRC_PROBE`
  lesson: an empty value must not enable them.
- Each test creates uniquely named resources (schema, slot, bucket prefix) so they can run in parallel.

### 5.2 Test infrastructure (`test-infra/compose.yaml`)

Default profile (M0): `postgres-18`, `mysql-84`, `mariadb-118`, `rustfs`, `azurite`. GCS has no
emulator: `fake-gcs-server` 1.56.1 rejects `object_store`'s PUT (`400 invalid uploadType`) and ignores
preconditions (re-checked 2026-09-26 with `object_store` 0.14.2: still `400 invalid uploadType`; upstream
fake-gcs issue #994 still open), so GCS tests use a real bucket (P-10; `DATA_AVALANCHE_GCS_BUCKET` + `GOOGLE_SERVICE_ACCOUNT_PATH`,
opt-out `DATA_AVALANCHE_SKIP_GCS=1`).
Profile `matrix` adds `postgres-14…17`, `mariadb-1011` and `mariadb-114`; profile `init` holds the
one-shot bucket/container creators. Later milestones add `postgres-tls` (M5, a CA-signed
certificate, as S-PG8 needed) and `mysql-84-nogtid` (M7, GTID **off** for file/position tests).
Every service uses a fixed, non-default host port listed in `test-infra/README.md`; Azurite keeps
10000 because `object_store`'s emulator mode targets `127.0.0.1:10000`. Source servers are configured
as the preconditions require: PG `wal_level=logical`; MySQL/MariaDB ROW / FULL image / FULL metadata /
GTID on (MariaDB also `gtid_strict_mode=1`).

Pinned images (tags checked against their registries on 2026-09-26): `postgres:14…18`, `mysql:8.4`,
`mariadb:10.11/11.4/11.8`, `rustfs/rustfs:1.0.0`, `mcr.microsoft.com/azure-storage/azurite:3.37.0`;
init tools `amazon/aws-cli:2.37.4`, `mcr.microsoft.com/azure-cli:2.90.0-azurelinux4.0`. Conditional
create/update measured 2026-09-26: **RustFS PASS, Azurite PASS, real GCS PASS** (via `object_store`),
fake-gcs FAIL (risk R-3).

### 5.3 Slice-1 E2E scenarios (M6 exit, then reused as the parity suite in M7)

1. Snapshot modes: `initial` on an empty sink; `initial` with state present; `always`; `never`;
   `dump_always`; `dump_initial` (all three table-state cases).
2. Streaming: I/U/D, PK-changing UPDATE, multi-table transaction, empty transaction, a 50k-row
   transaction (atomic), and a split-commit transaction with a crash mid-split, then replay.
3. Crash/replay: kill -9 at each phase boundary (mid-chunk, after the sink commit but before the
   checkpoint, mid-multi-table-flush); final table equals source; no duplicate keys.
4. Schema: ADD COLUMN (Applied), DROP COLUMN (soft-drop), RENAME (Degraded), widen (halt),
   reserved-name ADD (halt `RESERVED_COLUMN_COLLISION`), `DROP TABLE` (halt `TABLE_DROPPED` within
   `rescan_interval`), TRUNCATE (metadata-only clear), a DDL replayed after a crash (idempotent).
5. TOAST: an UPDATE that doesn't touch a TOASTed column keeps the value, both within one batch and
   across batches (D-IMPL-3).
6. Types: the full type round-trip, source → Delta → DuckDB `delta_scan`.
7. Lifecycle: pause/resume/stop across a restart; `desired_state` survives.
8. Single-node lease: a second process against the same `globals.state` doesn't run the pipeline and
   takes over after the first dies.
9. Iceberg refusal: an `iceberg` pipeline is refused while a `delta` pipeline in the same config runs.
10. Guards: `max_wal_lag_bytes` halt; `max_transaction_buffer_bytes` halt; slot WAL-gone → re-snapshot.

### 5.4 CI (`.github/workflows/`)

- `ci.yml` (push/PR): fmt; clippy `-D warnings`; unit + property tests; containers (default profile) +
  integration, including real GCS; slice E2E; docs build (from M11); at the end of the check job, a
  **non-blocking** `cargo audit` (RustSec) step: an advisory fails the step and adds a warning
  annotation, but never fails the job. Requires secret
  `GCP_SA_KEY` (service-account JSON, `roles/storage.objectAdmin` on the bucket) and variable
  `DATA_AVALANCHE_GCS_BUCKET`, documented in `test-infra/README.md` and in the workflow comments.
- `nightly.yml`: full DB matrix, full E2E, longer property-test case counts.
- `cloud.yml` (weekly + manual): T1–T10 and the Delta P8 re-run on real S3/GCS/Azure, plus one E2E per
  cloud. Credentials are repository secrets injected as env vars; nothing is printed.

---

## 6. Risk register

| # | Risk | Likelihood / impact | Mitigation (where) |
|---|---|---|---|
| R-1 | `memberlist` 0.8.5 has never been probed (TLS, metadata size, Lifeguard, runtime-agnostic API) | medium / high | M8 opens with a probe gate; `foca` 2.0 as fallback; everything above membership consumes an owner function, so the swap is contained |
| R-2 | `object_store` CAS behaves differently from the proven native clients (GCS generation, S3 409, Azure MI) | low / high | T1–T10 re-proof on real clouds in M2 before any lease code depends on it; the proven per-backend stores in `prototypes/lease-conformance` are the fallback |
| R-3 | An emulator's conditional writes are incomplete | ~~medium~~ low / low | Measured 2026-09-26: RustFS and Azurite honour Create and Update preconditions; fake-gcs doesn't serve `object_store` at all, so it's replaced by a real bucket (P-10). Real GCS via `object_store` passed the same checks (2026-09-26). Re-checked with `object_store` 0.14.2 the same day: fake-gcs 1.56.1 (= `latest`) still fails the plain PUT. Closed |
| R-12 | Clamped PG edge dates (Arrow min/max, Q-M5-1) are valid Parquet but some engines can't render years ±5.8M / ±294k | medium / low | M4 type round-trip reads them through DuckDB `delta_scan`; the operator docs list the clamping and the engines checked |
| R-11 | The real GCS bucket makes PR CI depend on credentials, network and cost | low / low | Per-test unique prefixes plus cleanup, a 1-day lifecycle rule on `data-avalanche-it/`, a dedicated CI bucket and service account; fork PRs skip GCS with a notice (P-10) |
| R-4 | File/position MySQL mode (new per P-4) has unknown resume edge cases (rotation, restart, failover invalidates positions) | medium / medium | Probe S-MY10 opens M7; a changed source server halts the pipeline (`BINLOG_SOURCE_CHANGED`, Q-M7-2) and the operator docs give the recovery (switch to `gtid` or re-snapshot) |
| R-5 | Delta MERGE cost under random-key churn (D-MS.15) | known / medium | Merge metrics (M4), flush guidance (M11 docs), measured in M12 |
| R-6 | DataFusion memory during large MERGEs | medium / medium | Bound the batch bytes per commit (split at `flush.bytes`); a memory test in M4; the DataFusion memory pool configured |
| R-7 | `mysql_async` silent payload drop (DA-SRC-12) | low / high | Our own check (M7) plus a fault-injection test; upstream report |
| R-8 | `pgwire-replication` =0.4.1 is a young crate (proto v1 only) | medium / medium | Thin wrapper; the decoder is ours; a fork path is documented |
| R-9 | `serde-saphyr` lacks a strictness or error-location feature the spec needs | low / medium | First M1 task verifies it; `serde_yaml_ng` fallback |
| R-10 | TOAST carry-forward across batches depends on the MERGE update expression (D-IMPL-3) | medium / high | M4 test with an unchanged TOAST column across two flushes; `re_query` strategy as the alternative |
| R-13 | Two `object_store` versions after M4: ours at 0.14, deltalake 1.0 at `^0.13.2`. Store handles can't be shared across them (e.g. passing our store to delta-rs), and `quick-xml` 0.39.4 (RUSTSEC-2026-0194/0195, fixed in 0.41) comes back through deltalake's copy | known / low | Keep the two apart: the storage crate owns 0.14 for lease/state/config; the Delta sink builds its stores through deltalake's own API. The non-blocking `cargo audit` step reports the advisories. Move to one version when a deltalake release accepts 0.14 (check at M4) |

---

## 7. Spec edits this plan depends on

**Status:** S-A…S-G approved and applied on 2026-09-26, together with the Q-M7-2 / Q-M9-1 answers
(§9.1) and the D-IMPL-7 rename. The `Type_Mapping.md` fix below is applied as well.

| # | Spec | Change | From | Before |
|---|---|---|---|---|
| S-A | `Configuration_Specification.md` §9.4.1, §8.5, §14; `Data-Flow.md` Phase 3 | `flush.seconds` default 60 → **30** | P-3 | M1 |
| S-B | `Configuration_Specification.md` §9.2.2 (+ §14 example) | `replication_type` default `binlog` → **`gtid`**; `binlog` = file/position mode, supported | P-4 | M1 |
| S-C | `Data-Flow.md` Phase 2b, Phase 5, State Layout | Describe file/position mode: `COM_BINLOG_DUMP` at (file, pos), `SHOW BINARY LOG STATUS` watermark, `gtid.json` → a position document covering all three MySQL dialects | P-4 | M7 |
| S-D | `Data-Flow.md` §On-Demand Snapshot | Endpoint path `POST /pipelines/{name}/snapshot` → `POST /v1/pipelines/{id}/snapshots` (System-Overview / API_RBAC are authoritative) | consistency | M9 |
| S-E | `Configuration_Specification.md` §12.1, §12.4 | Replace the stale "design item" notes (cross-worker propagation, pipeline state machine) with pointers to Clustering §9 and Pipeline_Lifecycle | consistency | M1 |
| S-F | `Data-Flow.md` Phase 3 metadata list, Phase 5 | Lineage list gains `_seq`/`_pipeline`; Phase 5 states the checkpoint rule D-IMPL-2 | D-IMPL-2 | M6 |
| S-G | `Data-Flow.md` Phase 2a TOAST paragraph | State the carry-forward mechanism D-IMPL-3 | D-IMPL-3 | M4 |

`Type_Mapping.md` (research) still names `mysql-binlog-connector-rust` as the MySQL decoder, but
`mysql_async` replaced it. That research-doc fix is applied (2026-09-26).

---

## 8. v1.0.0 acceptance checklist

- [ ] All milestone exit criteria met; CI, nightly and cloud workflows green (nightly 7 in a row).
- [ ] D-V1.3 measured: steady-state p99 ≤ 60s and failover recovery ≤ 2 min at default config (M12
      report).
- [ ] No data loss or duplicate keys in the crash/replay/chaos suites (source vs Delta comparison).
- [ ] Real-cloud E2E on S3, GCS and Azure.
- [ ] Container image and Helm chart published; kind smoke deploy green.
- [ ] Documentation site builds strict; runbooks reviewed.
- [ ] Gap tracker updated: implementation-surfaced gaps recorded, v1 rows closed with pointers to code
      and tests.

---

## 9. Implementation decisions taken in this plan

These fill details the specs leave open. **D-IMPL-1…6 approved as written; D-IMPL-7 approved with
the naming below (2026-09-26).**

| ID | Decision | Why | Milestone |
|---|---|---|---|
| D-IMPL-1 | `ColumnValue::Numeric` carries an own `Decimal { unscaled: i128, scale: i8 }`, not `rust_decimal` | `rust_decimal` holds only 28–29 digits; the design needs `decimal(38,S)`. An i128 mantissa maps 1:1 onto Arrow `Decimal128` | M0 |
| D-IMPL-2 | The pipeline resume checkpoint is the **flush frontier**, persisted only after every table in that flush reports `Committed`/`SkippedCovered`; each table's `committed_position` = the stream frontier at the flush (not just the transactions that touched it) | With per-table "touched" positions, an idle table would pin the laggard forever and block PG WAL release. This rule gives the same result as Delivery §3's laggard, without the idle-table stall | M6 |
| D-IMPL-3 | PG unchanged-TOAST under `carry_forward`: the collapse fills the value from an earlier image in the same batch (or the superseded snapshot row); otherwise the row carries a per-column "unchanged" mask and the Delta MERGE updates that column with `CASE WHEN <mask> THEN t.col ELSE s.col END`. An insert with an unresolved unchanged column (target row absent, e.g. `snapshot_mode: never`) writes NULL and increments a metric | Keeps the last known value without a source round-trip; `re_query` stays the explicit alternative | M3/M4/M5 |
| D-IMPL-4 | Position tagged-string forms: `pg:<X/Y>`, `mysql-gtid:<canonical set>`, `mariadb-gtid:<d-s-n,…>`, `binlog:<file>:<pos>` | `committed_position` must be self-describing across dialects (Delivery §2) | M0 |
| D-IMPL-5 | `SourceSeq` for binlog = (file index from the `.NNNNNN` suffix, `end_log_pos`), in every MySQL mode | Totally ordered within a stream across rotations | M0 |
| D-IMPL-6 | Integration tests gate on `DATA_AVALANCHE_IT` (non-empty), cloud tests on `DATA_AVALANCHE_CLOUD` | Keeps `cargo test` hermetic by default | M0 |
| D-IMPL-7 | Product name **`data-avalanche`** everywhere: binary and crate family `data-avalanche` / `data-avalanche-*` (imports `data_avalanche_*`); Helm chart and image `data-avalanche`; metric prefix **`data_avalanche_`** (replaces the specified `cdc_`); Delta `app_id` **`data-avalanche/<pipeline_id>/owner`**; `commitInfo` keys and the Arrow field tag `data-avalanche.*` (`data-avalanche.committed_position`, `.owner_epoch`, `.pipeline`, `data-avalanche.logical`); internal column prefix **`__data_avalanche_`** (MERGE flag `__data_avalanche_del`); env vars `DATA_AVALANCHE_*`; test bucket `data-avalanche-test`, test object prefix `data-avalanche-it/` | Owner decision (2026-09-26); specs, research docs and this plan updated. Untouched: `prototypes/` (probe evidence as run) and the legacy gap report; Iceberg's `cdc.owner_epoch` table property is v2 and keeps its name until the Iceberg sink is planned | M0 |

### 9.1 Questions answered at plan review (2026-09-26)

| # | Question | Answer | Recorded in |
|---|---|---|---|
| Q-M5-1 | PG `infinity` / `-infinity` for `date` / `timestamp[tz]` | **Clamp to the Arrow min/max** (`Date32` `i32::MIN/MAX`; `Timestamp(µs)` `i64::MIN/MAX`); out-of-range PG timestamps clamp too | `Type_Mapping.md` §Date/time edge values; `ColumnValue::Date(i32)` (§3.1) |
| Q-M7-1 | MySQL/MariaDB zero dates under a permissive `sql_mode` | **All-zero date → NULL** (halt if it is in a key column); **zero-in-part / impossible dates → table halt `INVALID_DATE_VALUE`**, remedy `column_type_overrides: string` (raw text). Revised at review: MariaDB 11.8 accepts zero-in-part dates by default (measured) | `Type_Mapping.md` §Date/time edge values; config §9.3 `column_type_overrides` |
| Q-M7-2 | File/position mode under a source failover | **Halt with a clear error**: pipeline halt `BINLOG_SOURCE_CHANGED` when `@@server_uuid` (MySQL) / `@@server_id` (MariaDB) differs from the recorded one; remedy = `gtid` or re-snapshot | `Data-Flow.md` Phase 2b; config §9.2.2 |
| Q-M9-1 | Snapshot-job persistence | **Persisted in the pipeline state location** (`snapshots/jobs/<job_id>.json`); listed after a restart or failover via `GET /v1/pipelines/{id}/snapshots`; a running job continues on the new owner from its chunk cursors | `Data-Flow.md` §On-Demand Snapshot + State Layout; System-Overview and API_RBAC endpoint tables |

### 9.2 Open questions (answer before the named milestone)

- **Q-M3-1:** Shape of the PG unchanged-TOAST mask on `Row` (D-IMPL-3): a second field (breaks the
  `Row(vec![..])` tuple constructor), a named-field struct, or a `ColumnValue::Unchanged` marker.
- **Q-M3-2 (M0 final review I-1):** `_seq` must sort in source order as text, but `SourceSeq`'s
  `Display` (PG `X/Y` unpadded, binlog `NNNNNN:pos`) doesn't (`"0/9" > "0/10"`). Proposal: a fixed-width
  column form (`{:08X}/{:08X}`, `{:010}:{:020}`) for the `_seq` column only, keeping the canonical
  `Display`; Data-Flow §Data Lineage states it (spec edit).
- **Q-M3-3 (M0 final review I-2):** `ColumnValue::Time(NaiveTime)` can't hold PG `time` `24:00:00` or
  MySQL/MariaDB `TIME` (an elapsed time, −838:59:59…838:59:59). Options: `Time(i64 µs)` plus edge rules
  in Type_Mapping, or a halt/string rule for out-of-day values. (Delta has no `time` type; it maps to
  string there.)
- Recorded for later plans: M1 — depth limit on `list<T>` in the `LogicalType` parser (config input);
  M7 — the decoder validates `gno >= 1` before `GtidSet::insert` (anonymous GTID events carry 0, and
  `insert_range` panics on 0); M1 — settle one public-API convention (root re-exports, `FromStr` for
  `Decimal`, error-type naming) before more types land.

- **Q-M9-2:** `GET /v1/snapshots/{job_id}` is served by any node, but the job lives in one pipeline's
  state location. Either the job ID embeds the pipeline ID so any node can read it from storage, or
  the status endpoint moves under `/v1/pipelines/{id}/snapshots/{job_id}`. Decide in M9 planning.
