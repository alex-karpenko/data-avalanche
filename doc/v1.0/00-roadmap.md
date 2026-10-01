---
updated: 2026-10-01
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
│   ├── data-avalanche-core/           domain types: values, events, positions, schema model, change set, Sink and source traits, errors
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
      │             │        │          ├──┼── source-pg ─► core, config
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
| Delta sink | `deltalake` (features `datafusion`, `rustls`, `s3`, `gcs`, `azure`) + `deltalake-core` | `=1.0.0` both (Q-M4-1: the `deltalake` facade asks for `deltalake-core = "1.0"`, which resolves to 1.1; the direct pin keeps the engine the probes ran on) | delta P1–P10, P8 on S3/GCS/Azure |
| Object storage / CAS | `object_store` | `0.14.2` (bumped from 0.13.2 on 2026-09-26; deltalake 1.0 requires `^0.13.2`, so M4 adds a second copy, R-13) | API checked; RustFS/Azurite/real-GCS CAS re-run on 0.14.2; T1–T10 re-proof in M2 |
| Arrow | `arrow` / `parquet` | `59` (as resolved by deltalake) | P9 |
| PG replication transport | `pgwire-replication` (`default-features = false`, features `tls-rustls`, `scram`, `md5`) | `=0.4.1` | S-PG0–8, re-run as M5's regression tests |
| PG SQL (catalog, chunks) | `tokio-postgres` + `tokio-postgres-rustls` (trust roots `webpki-roots` 1) | `0.7` / `0.14` | S-PG probes; M5's integration suite |
| MySQL binlog + SQL | `mysql_async` / `mysql_common` | `=0.37.1` / `=0.37.3` | S-MY0–9 |
| DDL classifier | `sqlparser` | latest 0.x, pinned in M7 | MySQL_DDL_Classifier §5 |
| YAML 1.2 (serde) | `serde-saphyr` (`deserialize` only) | `=1.3.0` | Verified in M1 planning (2026-09-27): strict booleans, duplicate-key errors, `Spanned` locations, unknown-key errors with location. One trap: `${…}` inside a flow collection must be quoted (M1 Review Focus 1). R-9 closed |
| Membership | `memberlist` `0.8.5` (fallback `foca` `2.0`) | pinned in M8 | **unprobed** — M8 opens with a probe gate |
| HTTP API | `axum` `0.8` + `utoipa` `6` | | |
| Metrics | `prometheus-client` | | |
| Tracing | `tracing`, `tracing-subscriber`, `opentelemetry-otlp` | | |
| CLI | `clap` `4` | | |
| Misc | `thiserror` 2, `async-trait`, `bytes`, `chrono`, `uuid`, `serde_json` (feature `arbitrary_precision`: JSON numbers keep every digit, Q-M5-11), `humantime`, `xxhash-rust` (ring hash, anchor `hash_algo: xxh3`), `proptest` (dev) | | |

---

## 3. Cross-crate interfaces (the contracts milestone plans implement)

Shapes are normative; exact signatures are fixed by the milestone that owns them and recorded back
here. `data-avalanche-core` owns every type in this section unless noted.

### 3.1 Values and events (Data-Flow §CdcEvent, Type_Mapping; landed in M0, extended in M3)

```rust
pub struct TableRef { pub schema: String, pub name: String }        // MySQL: schema = source.db

pub enum ColumnValue {
    Null, Bool(bool), Int16(i16), Int32(i32), Int64(i64), UInt64(u64),
    Float32(f32), Float64(f64),
    Numeric(Decimal),                        // D-IMPL-1: i128 unscaled + scale (38 digits)
    Text(String), Bytes(Vec<u8>),
    Timestamp { micros: i64, with_tz: bool },   // µs since Unix epoch (= Arrow range), UTC instant or wall clock
    Date(i32),                               // days since Unix epoch = Arrow Date32; PG ±infinity clamps (Q-M5-1)
    Time(i64),                               // signed µs (Q-M3-3): PG 0..=24:00:00, MySQL ±838:59:59; `time_text` renders it
    Uuid(uuid::Uuid), Json(serde_json::Value),
    Array { element_type: ColumnValueType, values: Vec<ColumnValue> },
    Unchanged,                               // PG unchanged TOAST (pgoutput `u`, D-IMPL-3, Q-M3-1): not NULL; never inside an array
}
impl ColumnValue { pub fn is_null(&self) -> bool; pub fn fits(&self, t: &ColumnValueType) -> bool; }
pub enum ColumnValueType { Bool, Int16, Int32, Int64, UInt64, Float32, Float64, Numeric { precision: u8, scale: i8 },
                           Text, Bytes, Timestamp { with_tz: bool }, Date, Time, Uuid, Json, Array(Box<ColumnValueType>) }
pub enum LogicalType { Bool, Int16, Int32, Int64, Float32, Float64, Decimal { precision: u8, scale: i8 }, String,
                       Binary, Date, Time, TimestampTz, TimestampNtz, Uuid, Json, List(Box<LogicalType>) }
    // grammar shared with config `column_type_overrides` (V-078/V-087); parse + Display round-trip

pub enum Op { Insert, Update, Delete, Snapshot }
pub struct CdcEvent {
    pub op: Op, pub table: TableRef,
    pub before: Option<Row>,                 // only the columns the source sent (a PG `K` old tuple: the key columns)
    pub after: Option<Row>,
    pub source_pos: SourcePosition,          // commit position of the enclosing transaction
    pub change_seq: SourceSeq,               // per-change order (keep-last key, `_seq`)
    pub xid: u64, pub commit_ts_micros: i64,
}
impl CdcEvent { pub fn validate(&self) -> Result<(), EventShapeError>; }   // image shape per op; no Unchanged in arrays
pub enum EventShapeError { MissingAfter{..}, MissingBefore{..}, UnexpectedBefore{..}, UnexpectedAfter{..},
                           UnchangedInArray { table, column } }
pub struct TransactionBoundary { pub xid: u64, pub commit_pos: SourcePosition, pub commit_ts_micros: i64 }
pub struct Row(pub Vec<(String, ColumnValue)>);   // Row::get(column)
impl SourceSeq { pub fn lineage_text(&self) -> String; }   // `_seq`: fixed width, sorts as text (S-N)
impl SourcePosition { pub fn untagged(&self) -> String; }  // `_lsn` (S-N)
// Also: Decimal { unscaled: i128, scale: i8 } with parse / precision / rescale / fits (Eq/Hash compare the
// representation: rescale before keying by value); table.rs: LINEAGE_COLUMNS
// (`_op _ts _lsn _seq _source_db _pipeline`), INTERNAL_COLUMN_PREFIX `__data_avalanche_`,
// is_reserved_column / first_reserved_collision.
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

### 3.3 Schema model and change set (Schema_Change §7, Multi_Sink §6.2; landed in M3)

`M3-core-data-path.md` D-M3.1…18 and its "Deviations during execution" are the rulings behind this.

```rust
pub struct ColumnSchema { pub name: String, pub value_type: ColumnValueType, pub logical: LogicalType,
                          pub nullable: bool /* source's; targets ignore it (S-O) */, pub source_type: String,
                          pub source_ordinal: Option<i32> /* PG attnum */ }
impl ColumnSchema { pub fn new(name, value_type, source_type) -> Self /* canonical logical, nullable */; pub fn not_null(self) -> Self; }
pub fn canonical_logical(t: &ColumnValueType) -> LogicalType;             // UInt64 → decimal(20,0), …
pub fn hop2_compatible(value_type: &ColumnValueType, logical: &LogicalType) -> bool;   // an override Hop 2 can store

pub struct TableSchema { /* private: table, columns, identifier */ }
impl TableSchema {
    // Checks, in order: no reserved name; a non-empty identifier; identifier columns listed once, present,
    // NOT NULL, non-floating, primitive; unique names; every logical type hop2_compatible.
    pub fn new(table: TableRef, columns: Vec<ColumnSchema>, identifier: Vec<String>) -> Result<Self, HaltReason>;
    pub fn table(&self) -> &TableRef; pub fn columns(&self) -> &[ColumnSchema]; pub fn identifier(&self) -> &[String];
    pub fn column(&self, name: &str) -> Option<&ColumnSchema>; pub fn index_of(&self, name: &str) -> Option<usize>;
    pub fn is_identifier(&self, name: &str) -> bool;
}
pub enum HaltReason {                        // code(): the SCREAMING_SNAKE name, e.g. NULL_IDENTIFIER
    ReservedColumnCollision { column }, TableDropped, UnsafeDdl { detail }, NoIdentifier,
    IdentifierNotUsable { column, reason }, IdentifierNotInReplicaIdentity { missing: Vec<String> },
    EvolutionUnsupported { detail }, EvolutionDisabled, NumericOverflow { column },
    InvalidDateValue { column, raw },        // MySQL/MariaDB zero-in-part / invalid dates (Q-M7-1)
    SchemaMismatch { column, detail },       // a value or buffered row that doesn't fit the schema
    NullIdentifier { column },               // NULL or unsent key value
}
// Pipeline-scope halts (not per table): BINLOG_PAYLOAD_INVALID (D-V1.6), BINLOG_SOURCE_CHANGED (Q-M7-2)

pub enum SchemaChange { AddColumn(ColumnSchema), DropColumn(String), RenameColumn { from, to, proven: bool },
                        ChangeType { column, from: ColumnSchema, to: ColumnSchema, narrowed: bool },
                        SetNotNull(String), DropNotNull(String), IdentifierChanged, TableDropped,
                        EnumValuesAppended(String), EnumValuesChanged(String), NoTargetEffect(String),
                        Unclassifiable(String) }
pub struct SchemaDelta { pub table: TableRef, pub changes: Vec<SchemaChange> }
pub enum DeltaClass { Safe, Unsafe(HaltReason), Ignorable }
pub fn classify(schema: &TableSchema, delta: &SchemaDelta) -> Vec<(SchemaChange, DeltaClass)>;  // the §7.2 table

// Hop 2 Arrow (columnar.rs): field metadata LOGICAL_TAG = "data-avalanche.logical" where the Arrow type
// is ambiguous; LIST_ELEMENT = "element"; unchanged masks `__data_avalanche_unchanged:<col>` (non-null Boolean).
pub fn arrow_type(logical: &LogicalType) -> DataType;
pub fn column_field(schema: &TableSchema, c: &ColumnSchema) -> Field;   // identifier NOT NULL, the rest nullable
pub fn added_column_field(c: &ColumnSchema) -> Field;                    // always nullable
pub fn upsert_schema(schema: &TableSchema) -> SchemaRef;                 // source columns, then the lineage columns
pub fn delete_schema(schema: &TableSchema, extra: &[String]) -> Result<SchemaRef, HaltReason>;   // identifier first
pub fn unchanged_mask_field(column: &str) -> Field;   // also: lineage_fields(), logical_tag(&ColumnSchema)

pub struct PositionRange { pub start: SourcePosition, pub end: SourcePosition }   // (start, end]
pub struct SplitInfo { pub txn_start: SourcePosition, pub slice: u32 }              // slice ranges end at txn_start
pub struct TableChangeSet {
    pub table: TableRef, pub schema: SchemaRef /* upsert_schema */, pub identifier: Vec<String>,
    pub truncate: bool, pub upserts: Vec<RecordBatch>, pub deletes: Vec<RecordBatch>,
    pub position: PositionRange, pub split: Option<SplitInfo>,
}
impl TableChangeSet {
    // Upsert batches: schema's fields, then the same unchanged masks (non-identifier columns) in every batch;
    // a non-empty identifier; delete batches start with it; a key at most once across both; no NULL key;
    // the range doesn't run backwards; a slice's range ends at txn_start.
    pub fn validate(&self) -> Result<(), ChangeSetError>;
    pub fn upsert_rows(&self) -> usize; pub fn delete_rows(&self) -> usize; pub fn is_empty(&self) -> bool;
}
pub struct ChangeSetError { pub table: TableRef, pub detail: String }
```

### 3.4 Sink trait (Multi_Sink §6.3–6.4; landed in M3)

```rust
#[async_trait]
pub trait Sink: Send + Sync {             // object safe: Arc<dyn Sink>
    fn kind(&self) -> SinkKind;
    fn capabilities(&self) -> &SinkCapabilities;
    async fn table_state(&self, table: &TableRef) -> Result<SinkTableState, SinkError>;
    async fn ensure_table(&self, spec: &TableSpec) -> Result<(), SinkError>;         // a present table is left alone
    async fn evolve(&self, table: &TableRef, delta: &SchemaDelta) -> Result<EvolveOutcome, SinkError>;
    async fn commit(&self, cs: TableChangeSet, marker: CommitMarker) -> Result<CommitOutcome, SinkError>;
}
// commit, in order (documented on the trait; testkit `MemorySink` is the reference):
//  1. cs.validate(); 2. refuse a marker whose committed_position != cs.position.end; 3. NoTable;
//  4. fence, then guard, against the stored marker (Fenced / SkippedCovered change nothing);
//  5. every column of the set in the table with the same type; a table column missing from the set is written NULL;
//  6. one table version: truncate clear → deletes → upserts; a masked cell keeps the stored value (NULL on a new row);
//  7. store the marker with the UNION of the prior and new positions.
pub enum SinkKind { Delta, Iceberg }        // as_str(): "delta" | "iceberg" (pipeline state `sink_type`)
pub enum WriteMode { Mirror, Changelog }
pub enum SchemaOp { AddColumn, DropColumn, RenameColumn, WidenType, DropNotNull }
pub enum PartitionTransform { Identity, Bucket { n: u32 }, Truncate { width: u32 }, Year, Month, Day, Hour }  // kind() → TransformKind
pub struct PartitionField { pub column: String, pub transform: PartitionTransform }
pub struct SinkCapabilities { pub write_modes: BTreeSet<WriteMode>, pub schema_ops: BTreeSet<SchemaOp>,
                              pub partition_transforms: BTreeSet<TransformKind> }
impl SinkCapabilities { pub fn refusal(&self, mode: WriteMode, partition_by: &[PartitionField]) -> Option<String>; }
pub struct TableSpec { pub table: TableRef, pub schema: SchemaRef /* upsert_schema */, pub identifier: Vec<String>,
                       pub partition_by: Vec<PartitionField> }
impl TableSpec { pub fn new(schema: &TableSchema, partition_by: Vec<PartitionField>) -> Result<Self, HaltReason>; }
pub struct CommitMarker { pub owner_epoch: u64, pub committed_position: SourcePosition, pub pipeline_id: String }
impl CommitMarker {
    pub fn fences(&self, ours: &CommitMarker) -> bool;   // stored.owner_epoch > ours.owner_epoch
    // Guard (D-M3.14, S-P): split None → stored covers cs.position.end;
    // split Some → stored strictly past txn_start (covers it and isn't equal).
    pub fn covers(&self, cs: &TableChangeSet) -> Result<bool, PositionError>;
}
pub struct SinkTableState { pub exists: bool, pub schema: Option<SchemaRef> /* Hop 2 terms, soft-drops stay */,
                            pub marker: Option<CommitMarker> }
pub enum EvolveOutcome { Applied, Degraded { warning: String }, Unsupported { reason: String } }
pub enum CommitOutcome { Committed { version: u64 }, SkippedCovered, Fenced { current_epoch: u64 } }
pub enum SinkError { InvalidChangeSet(ChangeSetError), Position(PositionError), NoTable { table },
                     Transient { reason }, Permanent { reason } }   // is_transient(): Transient only
```

`data-avalanche-testkit::MemorySink` (`Clone` shares the tables) implements this contract with the Delta
evolution rules: add column `Applied`; a drop is a soft-drop (kept, written NULL) and a rename adds the new
name and keeps the old, both `Degraded`; NOT NULL changes, appended enum values and no-effect changes need
nothing; everything else `Unsupported`. It also has
`content(&TableRef) -> Option<RecordBatch>` (rows in key order) and `version(&TableRef) -> Option<u64>`.

### 3.5 Storage and state (`data-avalanche-storage`, `data-avalanche-state`, landed in M2)

The crate roots are the whole public surface. D-M2.1…15 in `M2-storage-and-state.md` are the rulings behind it.

**Storage** (`data-avalanche-storage`, P-5: `object_store` 0.14.2 everywhere):

```rust
pub fn open_bucket(spec: &BucketSpec) -> Result<Arc<dyn ObjectStore>, OpenError>;
pub struct BucketSpec { pub backend: Backend, pub bucket: String, pub options: Vec<(String, String)>, pub timeouts: Timeouts }
pub enum Backend { S3, Gcs, Azure { account: Option<String> } }       // credentials from each cloud's standard chain
impl Timeouts { pub const COORDINATION: Self; }                         // a hung store surfaces in about 8 s
pub struct ObjectVersion { pub e_tag: Option<String>, pub version: Option<String> }   // = object_store::UpdateVersion
#[async_trait]
pub trait VersionedStore: Send + Sync + Debug {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError>;
    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError>;                 // Conflict if present
    async fn update(&self, key: &str, expected: &ObjectVersion, body: Bytes) -> Result<ObjectVersion, StoreError>; // Conflict if moved or missing
    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError>;                    // unconditional
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}
pub enum StoreError { Conflict { key }, Contention { key }, Transient { key, reason }, Permanent { key, reason } }
```

- `ObjectStoreVersioned::new(store, root)` is the implementation.
- There is no `NotFound`: `get` of a missing object is `Ok(None)`, and an `update` of a missing object is `Conflict` (D-M2.2).
- `Contention` (S3's 409) and `Transient` on a conditional write are **unknown outcomes**: the write may have landed.
- The path rules follow config §10: `table_root`, `colocated_state`, `explicit_state`, `globals_state` and `escape_segment`, each returning a `Location { bucket, root }`.
- `table_root` escapes names with `escape_table_segment` (`~XX`; M4 Q-M4-5): a table root never holds a `%`, because delta-kernel readers decode a table path twice. `escape_segment` (`%XX`) stays for state keys and snapshot file names.
- `OpenError` and `BucketSpec`'s `Debug` never show an option value.

**State** (`data-avalanche-state`): every document is pretty JSON with `version` first and `deny_unknown_fields`. A document with another schema version is `StateError::UnsupportedVersion`; a malformed one is `Corrupt`. Neither is ever treated as absent.

- `DocStore` provides `load`, `create`, `update`, and `modify` (read-modify-write with `MODIFY_ATTEMPTS` retries). `StateError::{is_race, is_conflict}` classify its errors.
- `globals.state` holds three document types:
  - **Cluster anchor:** `establish_anchor(&DocStore, &AnchorDoc) -> Anchor { doc, created }` writes `cluster/anchor.json`. The first writer wins. The anchor holds `tokens`, `lease_duration_ms`, `lock_backend`, `hash_algo` and `fingerprint_key`; `Debug` shows `***` for the key.
  - **Config generations:** `ConfigGenerations::{current, bootstrap_or_adopt, publish}` manage the `config/generation.json` CAS pointer and the raw-text snapshots `config/snapshots/<NNNNNN>-<blake3>.yaml`.
  - **Leases:** `Leases::{acquire, renew, observe, take_over, release}` operate on `leases/<pipeline_id>.json`.
    - The epoch is +1 on every acquisition. Release writes a tombstone that keeps the epoch.
    - `Acquired::{Mine, HeldBy}` is what `acquire` returns.
    - After an unknown outcome, the caller `observe`s before retrying: a lease still `is_held_by` it at the same epoch is its own.
    - Timing (renew interval, safety deadline) is M8's.
- Per pipeline, `PipelineDocs` is rooted at the pipeline's state location:
  - `open(pipeline_id, sink_type)` creates or checks `pipeline-state.json`, returning `PipelineIdMismatch` or `SinkTypeMismatch` on a mismatch;
  - `set_desired_state`;
  - `slot` / `save_slot` for PostgreSQL;
  - `position` / `save_position` for MySQL and MariaDB, recording `ServerIdentity`;
  - `table_snapshot` / `save_table_snapshot` (Data-Flow §State Layout as amended by S-M);
  - `create_snapshot_job` / `save_snapshot_job` / `snapshot_job` / `snapshot_jobs`.

Lease release is a **tombstone write** (object_store has no conditional delete). T1–T10 prove the lease CAS on every backend (`tests/lease_conformance.rs`, `test-infra/README.md`).

### 3.6 Source connector (`data-avalanche-core` owns the trait, Q-M5-2; each source crate implements it; defined in M5)

`data-avalanche-core::source` (M5 Task 1; D-M5.1 lists where it differs from the first sketch):

```rust
#[async_trait]
pub trait SourceConnector: Send + Sync {                    // built from the resolved Pipeline
    async fn preflight(&self) -> Result<Preflight, SourceError>;                    // config §11.2 pipeline rows
    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError>;         // channel A: tables, halted, skipped, warnings
    async fn prepare(&self, checkpoint: Option<&SourcePosition>, tables: &[TableRef])
        -> Result<Prepared, SourceError>;                                           // publication + slot state machine → StartPlan
    async fn reset_position(&self) -> Result<SourcePosition, SourceError>;         // after PositionUnreachable (D-15.3)
    async fn retained_bytes(&self) -> Result<Option<u64>, SourceError>;            // the max_wal_lag_bytes measure
    async fn current_position(&self) -> Result<SourcePosition, SourceError>;       // watermarks lw/hw
    async fn read_chunk(&self, t: &TableSchema, after: Option<&KeysetCursor>, limit: usize) -> Result<Chunk, SourceError>;
    async fn fetch_rows(&self, t: &TableSchema, keys: &[Row]) -> Result<Vec<Row>, SourceError>;  // toast_strategy: re_query
    async fn open_stream(&self, from: &SourcePosition, tables: &[TableSchema]) -> Result<Box<dyn ChangeStream>, SourceError>;
    async fn augment(&self, cached: &TableSchema, hint: &SchemaHint)
        -> Result<(SchemaDelta, Option<TableSchema>), SourceError>;                // channel C; None = dropped
    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError>;  // channel D
    async fn teardown(&self) -> Result<(), SourceError>;                           // auto_cleanup
}
#[async_trait]
pub trait ChangeStream: Send {
    async fn next(&mut self) -> Result<StreamItem, SourceError>;                   // an error ends the stream
    async fn acknowledge(&mut self, durable: &SourcePosition) -> Result<(), SourceError>;  // PG status update; MySQL no-op
    async fn close(self: Box<Self>) -> Result<(), SourceError>;
}
pub enum StreamItem { Begin { xid: u64, commit_ts_micros: i64 },                  // no commit position: known at Commit
                      Change(CdcEvent), Truncate { tables: Vec<TableRef>, seq: SourceSeq },
                      SchemaSignal { table: TableRef, seq: SourceSeq, hint: SchemaHint },  // PG Relation diff / MySQL DDL
                      TableHalt { table: TableRef, seq: SourceSeq, reason: HaltReason },  // an undecodable change
                      Commit(TransactionBoundary), KeepAlive(SourcePosition) }
pub enum SchemaHint { Columns(Vec<ColumnSchema>), Ddl(String) }
pub enum StartPlan { Resume, Snapshot { from: SourcePosition, reason: SnapshotReason } }  // FirstStart | PositionLost | AdoptedOrphan
pub struct Prepared { pub plan: StartPlan, pub warnings: Vec<String> }
pub enum SourceError { Transient(String), Permanent(String),
                       PositionUnreachable { position: String, detail: String },
                       TableHalt { table: TableRef, reason: HaltReason } }
```

Also: `Preflight { server_version, warnings }`, `ResolvedTables { tables, halted, skipped, warnings }`,
`KeysetCursor { key: Row, collation }`, `Chunk { rows, next }`, `RescanReport { dropped, changed,
halted }`, `identifier_values(&TableSchema, &Row)`. PostgreSQL positions (D-M5.2): a change's
`source_pos` is its transaction's commit LSN, its `seq` the XLogData `wal_start`; `Commit` carries
the `end_lsn` that is acknowledged and resumed from.

### 3.7 Cluster (Clustering §19–§20)

`LeaseStore` over `VersionedStore` (acquire / renew / takeover-evaluate / release). A `Renewer` state
machine (`Held | Renewing | Degraded | Lost`) exposes `may_commit()` = state ∈ {Held, Degraded} ∧
now < deadline. `PipelineController` is the sole lease writer and owns the `PipelineWorker`.
`ClusterReconciler` pushes `Own/DontOwn` targets. The same types run in single-node mode; M6 ships them
with a self-only ring.

### 3.8 Configuration (`data-avalanche-config`, landed in M1)

The crate root is the whole public surface (internals are `pub(crate)`):

- `load_file(path: &Path, env: &dyn Env, key: &FingerprintKey) -> Result<Loaded, Report>`, and
  `load_str(text, env, key)` for tests. `Loaded { config: ResolvedConfig, warnings: Vec<Diagnostic> }`.
  Any error rejects the whole config.
- `validate_str(text, env) -> Report` for the validation endpoint; `Report::to_json` is its body. It
  never returns the resolved config.
- `FingerprintKey` (32 bytes, from the `globals.state` cluster anchor, config §6.2). Startup (M2):
  1. Load the local file with `FingerprintKey::random()` to learn `globals.state`.
  2. Read or create the anchor, and run `check_cluster_anchor`.
  3. Adopt the cloud config generation (`bootstrap_or_adopt`, Clustering §9.3), and load its text
     with the anchor's key.

  `tests/startup.rs` runs this sequence. A reload loads the new text with the same key and
  publishes it as the next generation.
- `diff(old, new, table_set_changed: &dyn Fn(&Pipeline, &Pipeline) -> Option<bool>) -> ConfigDiff`:
  field changes classified by `Mutability` (R / PR / PROC); a PROC change rejects the reload (V-061).
  Both configs must be loaded with the same key.
- Checks that need runtime input: `check_cluster_anchor(&Globals, &ClusterAnchor)` (M2/M8 startup),
  `check_table_set(&Pipeline, &[TableRef])` (M5/M7, when the table set resolves),
  `check_unbounded_numeric(&Pipeline, &[(TableRef, String)])` (M5/M7, schema resolution). Each
  returns a `Report`.
- The resolved model (`ResolvedConfig`, `Globals`, `Pipeline`, …), the config enums, `Secret`
  (Debug `***`, Serialize = keyed fingerprint), `Pem`, `FileRef`, `ByteSize`, `HostPort`;
  diagnostics (`Report`, `Diagnostic`, `Severity`, `Locus`, `PARSE`, `TYPE`); the registry
  (`RULES`, `rule_meta`).

No diagnostic contains an env-resolved value, a file-reference value, file content or a secret
(config §11.1).


### 3.9 The accumulator (`data-avalanche-pipeline`, landed in M3)

No I/O and no clock: the orchestrator passes events and `now` in, reads `Flush`es out and commits them
through a `Sink`. The crate root is the whole public surface.

```rust
pub struct Lineage { pub source_db: String, pub pipeline: String }   // MAX_BATCH_BYTES = 512 MiB per batch
pub struct FlushPolicy { pub seconds: Option<u64>, pub rows: Option<u64>, pub bytes: Option<u64> }   // Default 30 s / 100 000 / 512 MiB
pub struct TxnLimits { pub batch_rows: Option<u64>, pub buffer_bytes: u64 }                          // Default no split / 1 GiB
pub enum FlushReason { Split, Boundary, Age, Rows, Bytes }
pub struct Flush { pub change_sets: Vec<TableChangeSet>, pub checkpoint: SourcePosition }   // checkpoint: durable once all are committed

impl Accumulator {
    pub fn new(lineage: Lineage, policy: FlushPolicy, resume_from: SourcePosition) -> Self;
    pub fn set_policy(&mut self, p: FlushPolicy); pub fn set_txn_limits(&mut self, l: TxnLimits);
    pub fn add_table(&mut self, schema: TableSchema, delete_extra: &[String]) -> Result<(), AccumulatorError>;
    pub fn halt_table(&mut self, table: &TableRef);                       // later events of the table are dropped
    pub fn begin(&mut self, xid: u64) -> Result<(), AccumulatorError>;
    pub fn apply(&mut self, e: &CdcEvent, now: Instant) -> Result<(), AccumulatorError>;
    pub fn truncate(&mut self, tables: &[TableRef], now: Instant) -> Result<(), AccumulatorError>;   // barrier
    pub fn set_schema(&mut self, schema: TableSchema, delete_extra: &[String]) -> Result<(), AccumulatorError>;  // barrier
    pub fn commit(&mut self, b: &TransactionBoundary, now: Instant) -> Result<(), AccumulatorError>;
    pub fn keepalive(&mut self, p: &SourcePosition, now: Instant) -> Result<(), AccumulatorError>;  // between transactions
    pub fn due(&self, now: Instant) -> Option<FlushReason>;              // after each boundary and on a timer
    pub fn flush(&mut self) -> Flush;                                     // committed layer: checkpoint..=frontier
    pub fn split(&mut self) -> Result<Flush, AccumulatorError>;          // a slice of the open transaction
    pub fn checkpoint(&self) -> &SourcePosition; pub fn frontier(&self) -> &SourcePosition; pub fn in_transaction(&self) -> bool;
}
pub enum AccumulatorError {
    Halt { table, reason: HaltReason },     // the table is halted in the accumulator too
    Event(EventShapeError), UnknownTable { table }, Protocol { detail }, Position(PositionError),  // connector bugs
    FlushRequired { table },                // barrier: nothing changed; flush + commit, then call again
    SplitRequired,                          // splitting on, open txn at buffer_bytes: nothing changed; split + commit, apply again
    TransactionTooLarge { bytes, limit },   // splitting off: halt the pipeline
}
```

`TableBuffer` (per table: keep-last collapse by `_seq`, PK change = delete + upsert, TOAST fill from the
buffered image then the sent before-image) and `CollapsedRow`/`RowKind` are public for tests; the
orchestrator uses the `Accumulator`. Deferred to later milestones (M3 plan, Deviations): `_lsn` for a
binlog-file position is taken from the event, not the commit boundary (M6); a rename inside an open
transaction NULLs the renamed column's open values; `add_table` doesn't mark a table halted when its
buffer can't be created.

### 3.10 The Delta sink (`data-avalanche-sink-delta`, landed in M4)

Path-based Delta tables on delta-rs 1.0 (no catalog in v1). The crate root is the whole public
surface. Everything else (`convert`, `marker`, `types`, the table handle, the MERGE) is private.

```rust
pub struct DeltaSinkConfig { pub pipeline_id: String, pub location: DeltaLocation, pub table: TableDefaults,
                             pub merge_memory_limit: Option<usize> }        // None: no DataFusion memory ceiling
pub enum DeltaLocation {                                                     // Debug hides option values
    Local { root: PathBuf },
    Bucket { backend: Backend, bucket: String, prefix: String, options: Vec<(String, String)> },
}
impl DeltaLocation {
    pub fn table_url(&self, table: &TableRef) -> Result<Url, SinkError>;    // config §10 table_root (~XX names); Permanent if invalid
    pub fn storage_options(&self, env: impl IntoIterator<Item = (String, String)>) -> HashMap<String, String>;  // Azure: AZURE_* merged
}
pub struct TableDefaults { pub checkpoint_interval: u64, pub log_retention: Duration,
                           pub timestamp_ntz_as: NtzAs, pub table_properties: BTreeMap<String, String> }
pub enum NtzAs { TimestampNtz, Timestamp, String }

pub struct DeltaSink;                                   // impl Sink: kind Delta; Mirror, AddColumn, Identity transforms
impl DeltaSink {
    pub fn new(config: DeltaSinkConfig) -> Self;        // registers the S3/GCS/Azure handlers once
    pub fn stats(&self) -> DeltaStats;
    pub async fn content(&self, table: &TableRef) -> Result<Option<RecordBatch>, SinkError>;  // Hop 2 rows, key order
}
pub struct DeltaStats { pub commits, pub skipped, pub fenced, pub retries, pub rows_inserted, pub rows_updated,
                        pub rows_deleted, pub rows_copied, pub files_added, pub files_removed: u64 }   // M10 exports them
pub const COMMIT_ATTEMPTS: usize = 3;
pub fn app_id(pipeline_id: &str) -> String;            // the fence's txn app id: data-avalanche/<pipeline>/owner
pub const POSITION_KEY, EPOCH_KEY, PIPELINE_KEY: &str;  // commitInfo entries of the marker
pub const TYPE_KEY, IDENTIFIER_KEY: &str;              // Delta column / table metadata: logical type, identifier
pub const DELETE_FLAG: &str;                           // the MERGE source's delete flag column
pub const DELTA_MIN_DATE, DELTA_MAX_DATE: i32; pub const DELTA_MIN_MICROS, DELTA_MAX_MICROS: i64;   // Hop 3b clamp (Q-M4-2)
```

`commit` (per table, one at a time) runs these steps:
1. **Fence pre-check.** The snapshot's `txn` version for `app_id` is compared with the marker's
   epoch; a greater one gives `Fenced`.
2. **Guard.** The walk-back over own `commitInfo` entries stops at the cached own version, after
   1000 commits, or at a cleaned-up file. If the position covers the set, the result is
   `SkippedCovered`.
3. **Truncate**, when the set has one: a DELETE commit with the `txn` but no position. It is
   retried when files remain: delta-rs 1.0 doesn't refuse a predicate-less DELETE that a
   concurrent OPTIMIZE outran (it declares no read; probe P11a).
4. **Write.** One MERGE on the identifier with unchanged masks, carrying the `txn` and the
   position. A set that changes no row gets a marker-only commit instead.

Errors:
- A real race (the six `CommitConflict` race variants, `MaxCommitAttempts`, `VersionAlreadyExists`)
  is retried up to `COMMIT_ATTEMPTS` times, then becomes `Transient`. A zombie that loses a race
  ends `Fenced`.
- `Permanent`: data or table errors, Delta validation failures, the other conflict variants,
  unsupported features, invalid locations.
- `Transient`: storage and network errors, and a MERGE over `merge_memory_limit`.

`table_state` reads the Hop 2 schema back from the column metadata. Its `marker` is `None` when no
positioned own commit is in reach. The rulings are D-M4.1…15 (D-IMPL-11), with the plan's
deviations.

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

- **Plan:** [`M1-configuration.md`](M1-configuration.md), 15 tasks. **Landed 2026-09-28** on
  `feat/claude/impl-v1`, with the owner follow-up batch; interface in §3.8, deviations in the plan.
- **Scope:**
  - The full config §3–§10 model: every field, default, union and tri-state, including the Iceberg
    blocks, because they must validate even though v1 refuses them at start.
  - Strict YAML 1.2 parse (`deny_unknown_fields`).
  - `${VAR}` / `${VAR:-d}` / `$${` substitution on string leaves, then type coercion.
  - Phase 1 structural validation on unresolved values, via a `Resolvable<T>` leaf that is literal or
    template.
  - Phase 2 resolution: env vars, `credentials_file`, `token_file`, PEM material, other files.
  - Merge semantics §8, including union subtrees and list replacement.
  - The **whole V-rule table** (V-001…V-117, with the P-3/P-4 changes and S-H…S-L) as data-driven validators with
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

- **Plan:** [`M2-storage-and-state.md`](M2-storage-and-state.md), 11 tasks. **Approved 2026-09-28**; **executed 2026-09-29** on `feat/claude/impl-v1`; every exit item is met (T1–T10 on real S3, GCS and Azure included).
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
  - pgoutput v1 decoder, production grade: all messages, text tuples (Q-M5-3: the stream never
    asks for binary, and a binary datum is a protocol error), unchanged-TOAST, origin, type messages.
  - OID → `ColumnValue` decode for the whole Type_Mapping PG table, plus the unsupported-type fallback
    chain, `numeric` precision cap and array element resolution.
  - Replication client wrapper: session pinning, status/wakeup interval = heartbeat, acknowledgment.
    The stream doesn't reconnect by itself: a `Transient` error ends it, and M6 reopens from the
    checkpoint with its backoff (M5 D-M5.4).
  - Slot state machine (D-15.1–15.3: managed / require_existing, orphan policy, WAL-gone → re-snapshot).
  - Publication reconciliation (D-15.4).
  - Preflight checks (config §11.2 PG rows).
  - Catalog channels: A (baseline, identifier, replica identity, reserved names), C (`attnum` diff,
    rename proof), D (batched rescan, **dropped OID → `TableDropped`**).
  - Keyset chunk reader with a `COLLATE "C"` cursor and `pg_current_wal_lsn()` watermarks.
  - The `max_wal_lag_bytes` measure (`retained_bytes`); the halt is M6's.
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
- **Carried from M4** (`M4-delta-sink.md`, Deviations):
  - Route a MERGE that fails over `merge_memory_limit` (`Transient`, but the same set fails the
    same way every time) to a smaller flush or a table halt, and add the config key.
  - `marker: None` from `table_state` doesn't mean "no own commit": a truncate's DELETE carries
    none.
  - Re-open a Delta table handle after repeated `Transient` errors. This covers a table
    re-created behind a live handle, `evolve` on such a handle included. A table dropped between
    `refresh` and the re-open comes back `Permanent` (`NotATable`) and should be `NoTable`.
  - Refuse a commit whose identifier differs from the table's recorded `data-avalanche.identifier`,
    together with the identifier-change path.
  - `Sink::evolve` carries no marker, so a zombie's ADD COLUMN isn't fenced. It is idempotent, but
    the trait question is open.
- **Carried from M5** (`M5-pg-source.md`):
  - Persist the start position right after `prepare` creates the slot. A crash before the first
    checkpoint otherwise leaves an orphan slot, and the next start meets `slot_on_orphan`.
  - The reconnect backoff: a stream error ends the stream, and the pipeline reopens it from the
    checkpoint (D-M5.4).
  - The `max_wal_lag_bytes` halt, comparing against `SourceConnector::retained_bytes` (V-104).
  - Keep-alives can arrive often (each heartbeat, and on every server keep-alive); don't do per-item
    work that assumes they are rare.
  - `toast_strategy: re_query` calls `fetch_rows` (Q-M5-6).
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
one-shot bucket/container creators. M5 adds `postgres-tls` to the default profile (port 55419: TLS
required on every TCP connection; a throwaway CA, server certificate and client certificate made at
each start, the CA and client pair written to the git-ignored `test-infra/.tls/`). M7 adds
`mysql-84-nogtid` (GTID **off** for file/position tests).
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

- `ci.yml` (pull requests only; each push to a PR's head branch re-runs it): fmt; clippy `-D warnings`; unit + property tests; containers (default profile) +
  integration, including real GCS; slice E2E; docs build (from M11); at the end of the check job, a
  **non-blocking** `cargo audit` (RustSec) step: an advisory fails the step and adds a warning
  annotation, but never fails the job. Requires secret
  `GCP_SA_KEY` (service-account JSON, `roles/storage.objectAdmin` on the bucket) and variable
  `DATA_AVALANCHE_GCS_BUCKET`, documented in `test-infra/README.md` and in the workflow comments.
- `nightly.yml` (scheduled + manual; from M5): full DB matrix (M5: the PostgreSQL source suite on
  PostgreSQL 14–17, one job each, through `DATA_AVALANCHE_PG_PORT`), full E2E, longer property-test
  case counts.
- `cloud.yml` (weekly + manual): T1–T10 and the Delta P8 re-run on real S3/GCS/Azure, plus one E2E per
  cloud. Credentials are repository secrets injected as env vars; nothing is printed.

---

## 6. Risk register

| # | Risk | Likelihood / impact | Mitigation (where) |
|---|---|---|---|
| R-1 | `memberlist` 0.8.5 has never been probed (TLS, metadata size, Lifeguard, runtime-agnostic API) | medium / high | M8 opens with a probe gate; `foca` 2.0 as fallback; everything above membership consumes an owner function, so the swap is contained |
| R-2 | `object_store` CAS behaves differently from the proven native clients (GCS generation, S3 409, Azure MI) | low / high | T1–T10 re-proof on real clouds in M2 before any lease code depends on it; the proven per-backend stores in `prototypes/lease-conformance` are the fallback. **Re-proven 2026-09-29** on `object_store` 0.14.2 through `Leases` (`tests/lease_conformance.rs`): in memory, RustFS, Azurite, real GCS, real S3 and real Azure all PASS (S3 and Azure: `cloud.yml` run 36604762684) |
| R-3 | An emulator's conditional writes are incomplete | ~~medium~~ low / low | Measured 2026-09-26: RustFS and Azurite honour Create and Update preconditions; fake-gcs doesn't serve `object_store` at all, so it's replaced by a real bucket (P-10). Real GCS via `object_store` passed the same checks (2026-09-26). Re-checked with `object_store` 0.14.2 the same day: fake-gcs 1.56.1 (= `latest`) still fails the plain PUT. Closed |
| R-12 | Clamped PG edge dates (Arrow min/max, Q-M5-1) are valid Parquet but some engines can't render years ±5.8M / ±294k | medium / low | M4 clamps `date` / `timestamp` / `timestamp_ntz` cells to the Delta range 0001-01-01 … 9999-12-31 23:59:59.999999 in Hop 3b (Q-M4-2), and its type round trip reads the bounds through DuckDB `delta_scan`. DuckDB 1.5.6 can't open a table path with non-ASCII text (Q-M4-6). The operator docs (M11) list the clamping, that limit and the engines checked |
| R-11 | The real GCS bucket makes PR CI depend on credentials, network and cost | low / low | Per-test unique prefixes plus cleanup, a 1-day lifecycle rule on `data-avalanche-it/`, a dedicated CI bucket and service account; fork PRs skip GCS with a notice (P-10) |
| R-4 | File/position MySQL mode (new per P-4) has unknown resume edge cases (rotation, restart, failover invalidates positions) | medium / medium | Probe S-MY10 opens M7; a changed source server halts the pipeline (`BINLOG_SOURCE_CHANGED`, Q-M7-2) and the operator docs give the recovery (switch to `gtid` or re-snapshot) |
| R-5 | Delta MERGE cost under random-key churn (D-MS.15) | known / medium | Merge metrics (M4), flush guidance (M11 docs), measured in M12 |
| R-6 | DataFusion memory during large MERGEs | medium / medium | Bound the batch bytes per commit (split at `flush.bytes`); a memory test in M4; the DataFusion memory pool configured |
| R-7 | `mysql_async` silent payload drop (DA-SRC-12) | low / high | Our own check (M7) plus a fault-injection test; upstream report |
| R-8 | `pgwire-replication` =0.4.1 is a young crate (proto v1 only) | medium / medium | Thin wrapper; the decoder is ours; a fork path is documented |
| R-9 | `serde-saphyr` lacks a strictness or error-location feature the spec needs | ~~low / medium~~ closed | Verified in M1 planning (2026-09-27) against the full M1 implementation in a scratch workspace: every needed feature is there (§2.1). Closed; `serde_yaml_ng` not needed |
| R-10 | TOAST carry-forward across batches depends on the MERGE update expression (D-IMPL-3) | medium / high | M4 test with an unchanged TOAST column across two flushes; `re_query` strategy as the alternative |
| R-13 | Two `object_store` versions after M4: ours at 0.14, deltalake 1.0 at `^0.13.2`. Store handles can't be shared across them (e.g. passing our store to delta-rs), and `quick-xml` 0.39.4 (RUSTSEC-2026-0194/0195, fixed in 0.41) comes back through deltalake's copy | known / low | Keep the two apart: the storage crate owns 0.14 for lease/state/config; the Delta sink builds its stores through deltalake's own API. The non-blocking `cargo audit` step reports the advisories. Move to one version when a deltalake release accepts 0.14. Re-checked at M4 planning (2026-09-30): `deltalake` 1.1.0 still requires `^0.13.2` |

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
| S-H | `Configuration_Specification.md` §13 intro | Add: "A diagnostic that no V-rule covers carries `PARSE` (YAML syntax or structure, an unknown key, a required field with no rule of its own) or `TYPE` (a scalar that does not coerce to its field's type where no V-rule covers the field)." | Q-M1-2 | M1 |
| S-I | `Configuration_Specification.md` §9.4.3 `table_properties` note, §13.4 V-093 | Name the allow-list: `delta.dataSkippingNumIndexedCols`, `delta.dataSkippingStatsColumns`, `delta.deletedFileRetentionDuration`, `delta.targetFileSize`, `delta.tuneFileSizesForRewrites`, `delta.checkpoint.writeStatsAsJson`, `delta.checkpoint.writeStatsAsStruct`. Any other `delta.*` key is an error; keys without the `delta.` prefix pass through unchanged | Q-M1-1 | M1 |
| S-J | `Configuration_Specification.md` §9.6 `prefix` note, §10 | `defaults.state.prefix` is a **parent** prefix, applied only when state has an explicit location (`state.bucket` on the pipeline or in `defaults`). It has no effect, and no warning, when state co-locates with the sink. A pipeline does not inherit it as its own `state.prefix` | Q-M1-3 | M1 |
| S-K | `Configuration_Specification.md` V-001, V-084, §9.2 intro, §9.4 `iceberg`/`delta` row | An absent `mysql` / `delta` block means all defaults (every field in them has one); `postgres` / `iceberg` blocks stay required (they have required fields) | Q-M1-4 | M1 |
| S-L | `Configuration_Specification.md` §13.4 (new row), §9.3 `include` note | New rule **V-117**: PostgreSQL `tables.include`/`exclude` patterns are `schema.table` (one `.`, both parts non-empty, globs within parts) | Q-M1-5 | M1 |

**S-H…S-L: approved and applied on 2026-09-27** (spec §15 item 22 records them). The M1 plan
implements them.

| # | Spec | Change | From | Before |
|---|---|---|---|---|
| S-M | `Data-Flow.md` §State Layout, per-table snapshot example | `"start_lsn": "0/1A2B3C4"` → `"start_position": "pg:0/1A2B3C4"` (MySQL has no LSN); add `"version"` and `"table": {"schema", "name"}`; show the cursor as `{key: [{col, val, type}], chunk_size, rows_done, collation}`; note the file-name escaping | M2 D-M2.12 | M2 exit |

**S-M: approved and applied on 2026-09-28.** The M2 plan implements it (D-M2.12).

| # | Spec | Change | From | Before |
|---|---|---|---|---|
| S-N | `Data-Flow.md` §Data Lineage Per Row, Phase 3 metadata list | `_seq` is fixed-width text that sorts like the source order (PG `XXXXXXXX/XXXXXXXX`, binlog `<10-digit file index>:<20-digit end_log_pos>`); `_lsn` is the commit position, untagged | Q-M3-2 | M3 |
| S-O | `Schema_Change_Detection_and_Evolution.md` §7.2, §7.3 | Every non-identifier column is created nullable, identifier columns NOT NULL; `nullable → NOT NULL` and `NOT NULL → nullable` are Ignorable | Q-M3-4 | M3 |
| S-P | `Data-Flow.md` Phase 4 §Position guard, Phase 3 oversized transactions | A split slice is skipped only when the stored position is strictly past its transaction's start; the rest of a split transaction goes out with the next regular flush | M3 D-M3.14 | M3 |

**S-N, S-O, S-P: approved and applied on 2026-09-30.** The M3 plan implements them.

| # | Spec | Change | From | Before |
|---|---|---|---|---|
| S-Q | `Data-Flow.md` §TOAST `carry_forward` steps 2 and 4, §CdcEvent Structure `before` | A masked column is filled under the old key after a key change, then from the before-image when the source sent the column (REPLICA IDENTITY FULL). An unresolved key change writes NULL (step 4); the remedy is REPLICA IDENTITY FULL or `toast_strategy: re_query`. A before-image carries only the columns the source sent (a PG `K` tuple: the key columns) | M3 final review, I1 | M3 exit |

**S-Q: approved and applied on 2026-09-30.** The M3 final-review fix wave implements it; M5's decoder follows its before-image rule.

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
| D-IMPL-8 | M1 config rulings **D-M1.1…D-M1.15** (listed in `M1-configuration.md` §Decisions and rulings, each pinned by a test). Among them: `Leaf<T>` replaces §4 M1's `Resolvable<T>`; templates allowed in union discriminators; a defaults catalog `type` dropped when the pipeline picks a block; required fields without a V-rule reported as `PARSE`; V-069/V-087 have no failing input in v1; the validate endpoint never returns the resolved config. D-M1.5, D-M1.12 and D-M1.14 were changed at review (Q-M1-4…6) | The spec is silent on these; each choice is the one that keeps a hand-written config valid or safe | M1 — **approved 2026-09-27** |
| D-IMPL-9 | M2 storage/state rulings **D-M2.1…D-M2.15** (listed in `M2-storage-and-state.md` §Decisions and rulings, each pinned by a test). Among them: `StoreError` is `Conflict` / `Contention` / `Transient` / `Permanent` with no `NotFound` (§3.5 now shows the landed interface); `Timeouts::COORDINATION` bounds a storage call to about 8 s; the lease tombstone keeps `owner_epoch`; config snapshots are the raw text under a content-addressed name with a `blake3:` hash | The specs and research docs are silent on these; each keeps the lease and state safe under races, hangs and hand-edited objects | M2 — **approved 2026-09-28** |
| D-IMPL-10 | M3 core data-path rulings **D-M3.1…D-M3.18** (listed in `M3-core-data-path.md` §Decisions and rulings, each pinned by a test). Among them: unchanged TOAST fills from the row's previous image, under its old key after a key change; Hop 2 uses `Utf8`/`Binary` with 512 MiB batch rollover; a flush takes only complete transactions and covers `checkpoint..=frontier`; barriers return `FlushRequired` (seal → flush → apply); a split slice ends at its transaction's start and is guard-skipped only when the stored position is strictly past it | The specs are silent on these, or (D-M3.14) were wrong; the property tests prove replay convergence | M3 — **approved 2026-09-30** |
| D-IMPL-11 | M4 Delta-sink rulings **D-M4.1…D-M4.15** (listed in `M4-delta-sink.md` §Decisions and rulings, each pinned by a test). Among them: every column carries its logical type in the Delta column metadata, so `table_state` reads Hop 2 back; the MERGE matches on the identifier only (no partition pruning); a zombie that loses a race is retried and ends `Fenced`; the walk-back stops at the cached own version, after 1000 commits or at a cleaned-up commit file; a truncate's DELETE carries no position; a set that changes no row gets a marker-only commit | The specs are silent on these; each keeps the fence, the guard or the round trip exact | M4 — **approved 2026-09-30** |
| D-IMPL-12 | M5 PostgreSQL-source rulings **D-M5.1…D-M5.19** (listed in `M5-pg-source.md` §Decisions and rulings, each pinned by a test). Among them: the source contract lives in core with `prepare`, `augment`, `fetch_rows` and `TableHalt` (§3.6); a change's position is its commit LSN, `Commit` carries the `end_lsn` that is acknowledged; a Relation signal is emitted with the next change's sequence; the stream doesn't reconnect by itself; the slot state table as one function; a publication the pipeline can't change that publishes via the partition root while tracking a partition is refused; snapshot rows are read as output-function text, keys in `COLLATE "C"` | The specs are silent on these; each keeps the stream's positions, the slot or the round trip exact | M5 — **approved 2026-10-01** |

### 9.1 Questions answered at plan review (2026-09-26)

| # | Question | Answer | Recorded in |
|---|---|---|---|
| Q-M5-1 | PG `infinity` / `-infinity` for `date` / `timestamp[tz]` | **Clamp to the Arrow min/max** (`Date32` `i32::MIN/MAX`; `Timestamp(µs)` `i64::MIN/MAX`); out-of-range PG timestamps clamp too | `Type_Mapping.md` §Date/time edge values; `ColumnValue::Date(i32)` (§3.1) |
| Q-M7-1 | MySQL/MariaDB zero dates under a permissive `sql_mode` | **All-zero date → NULL** (halt if it is in a key column); **zero-in-part / impossible dates → table halt `INVALID_DATE_VALUE`**, remedy `column_type_overrides: string` (raw text). Revised at review: MariaDB 11.8 accepts zero-in-part dates by default (measured) | `Type_Mapping.md` §Date/time edge values; config §9.3 `column_type_overrides` |
| Q-M7-2 | File/position mode under a source failover | **Halt with a clear error**: pipeline halt `BINLOG_SOURCE_CHANGED` when `@@server_uuid` (MySQL) / `@@server_id` (MariaDB) differs from the recorded one; remedy = `gtid` or re-snapshot | `Data-Flow.md` Phase 2b; config §9.2.2 |
| Q-M1-1 (2026-09-27) | What does V-093 allow in `sink.delta.table_properties`? | **A 7-key allow-list** (S-I); any other `delta.*` key is an error, non-`delta.` keys pass through | `M1-configuration.md`; spec edit S-I |
| Q-M1-2 (2026-09-27) | Which rule does a diagnostic with no V-rule carry? | **`PARSE` / `TYPE`** (S-H) | `M1-configuration.md`; spec edit S-H |
| Q-M1-3 (2026-09-27) | What does `defaults.state.prefix` mean? | **A parent prefix for explicit state locations**; co-located state ignores it silently (S-J) | `M1-configuration.md`; spec edit S-J |
| Q-M1-4 (2026-09-27) | Must an operator write an empty `mysql: {}` / `delta: {}`? | **No**: every field in those blocks has a default, so absent = all defaults; `postgres` / `iceberg` stay required (S-K) | `M1-configuration.md`; spec edit S-K |
| Q-M1-5 (2026-09-27) | PostgreSQL `include` pattern without a schema? | **A validation error, new rule V-117** (S-L) | `M1-configuration.md`; spec edit S-L |
| Q-M1-6 (2026-09-27) | Commit a test TLS key pair? | **Never commit key material**; tests generate a throwaway pair at run time (`rcgen` dev-dependency) | `M1-configuration.md` D-M1.14 |
| Q-M2-1 (2026-09-28) | What does a config snapshot in `globals.state` hold? | **The raw config file text**; each node resolves it itself. Clustering §9/§12 corrected | `M2-storage-and-state.md`; `Clustering_Architecture_Research.md` §9.1, §12 |
| Q-M2-2 (2026-09-28) | How much of the lease is M2? | **The lease document and its CAS operations**, proven by T1–T10; the renewer, safety deadline and controller are M6/M8 | `M2-storage-and-state.md` |
| Q-M2-3 (2026-09-28) | Where is M2 verified during planning? | **RustFS, Azurite and real GCS** (all passed 2026-09-28); real S3 and Azure at execution through `cloud.yml` | `M2-storage-and-state.md`, `test-infra/README.md` |
| Q-M2-4 (2026-09-28) | Snapshot cursor shape? | **Defined in M2; each cursor key value is a JSON value**; M5/M7 convert it | `M2-storage-and-state.md` D-M2.12 |
| Q-M3-1 (2026-09-29) | Shape of the unchanged-TOAST mask on `Row` | **A `ColumnValue::Unchanged` marker**; `Row` keeps its shape | `M3-core-data-path.md` D-M3.1 |
| Q-M3-2 (2026-09-29) | `_seq` must sort in source order as text | **A fixed-width form for the `_seq` column only** (`SourceSeq::lineage_text`); `Display` stays canonical | `M3-core-data-path.md`; spec edit S-N |
| Q-M3-3 (2026-09-29) | `Time(NaiveTime)` can't hold PG `24:00:00` or MySQL `TIME` intervals | **`ColumnValue::Time(i64)`, signed µs** | `M3-core-data-path.md`; `Type_Mapping.md` §Date/time edge values |
| Q-M3-4 (2026-09-29) | Does the target keep the source's `NOT NULL`? | **No: identifier columns NOT NULL, every other column nullable** | `M3-core-data-path.md`; spec edit S-O |
| Q-M4-1 (2026-09-30) | `deltalake` 1.0.0 pulls `deltalake-core` 1.1.0 | **Pin `deltalake-core =1.0.0` directly** (§2.1) | `M4-delta-sink.md` |
| Q-M4-2 (2026-09-30) | delta-rs can't write `Date32` `i32::MIN` (PG `-infinity` after Q-M5-1), and readers can't show years beyond 9999 | **The Delta sink clamps `date` / `timestamp` / `timestamp_ntz` cells to the Delta range** (0001-01-01 … 9999-12-31 23:59:59.999999) in Hop 3b; PG `±infinity` lands on the bounds. Revises Q-M5-1 for Delta | `Type_Mapping.md` §Date/time edge values; `M4-delta-sink.md` |
| Q-M4-3 (2026-09-30) | Truncate as one commit or two? | **As the spec says: a DELETE commit, then the MERGE** | `M4-delta-sink.md` D-M4.8 |
| Q-M4-4 (2026-09-30) | Where do the M4 integration tests run? | **Local and RustFS (the exit), plus Azurite, the DuckDB cross-read in CI, and real GCS in PR CI** | `M4-delta-sink.md` Tasks 10–13 |
| Q-M4-5 (2026-09-30) | delta-kernel decodes a table path twice, so `%XX` escapes break tables named with `%`, `/`, a control character, `.`/`..` | **Table roots escape with `~XX`** (`escape_table_segment`); state keys keep `%XX` | §3.5; `M4-delta-sink.md` Task 1; `M2-storage-and-state.md` D-M2.4 |
| Q-M4-6 (2026-09-30) | DuckDB can't open a Delta table under a non-ASCII path | **Keep non-ASCII names verbatim**; the operator docs list the limit (R-12) | `M4-delta-sink.md` |
| Q-M5-2 (2026-10-01) | Where does the source trait live? | **In `data-avalanche-core`**, next to `Sink` (§3.6) | `M5-pg-source.md` |
| Q-M5-3 (2026-10-01) | Binary tuples? | **Text only**; a binary datum is a protocol error | `M5-pg-source.md`; §4 M5 |
| Q-M5-4 (2026-10-01) | `pgwire-replication` takes TLS files; config allows inline PEM | **An inline PEM becomes a private temporary file** (0600 in a 0700 directory, removed on drop) | `M5-pg-source.md` |
| Q-M5-5 (2026-10-01) | Partitioned tables | **The leaves replicate as tables**; a partitioned parent is skipped with a warning; `publish_via_partition_root = false`, and a publication the pipeline can't change that publishes via the root while a partition is tracked is refused | `M5-pg-source.md` D-M5.14 |
| Q-M5-6 (2026-10-01) | Who builds the `re_query` keyed re-read? | **M5 adds `fetch_rows`**; M6 calls it | §3.6 |
| Q-M5-7 (2026-10-01) | Where do PostgreSQL 14–17 run? | **`nightly.yml`**, one job per version | §5.4 |
| Q-M5-8 (2026-10-01) | A bare `numeric` under `numeric_overflow_action: error` | **`decimal(38,18)`, halting per value** (`NUMERIC_OVERFLOW`) | `Type_Mapping.md` §PostgreSQL-specific notes |
| Q-M5-9 (2026-10-01) | The `varbit` value layout | **PostgreSQL's binary form**: a 4-byte big-endian bit count, then the bits packed MSB-first | `Type_Mapping.md` |
| Q-M5-10 (2026-10-01) | `sslmode=require` with `ssl_ca` set: check the chain, as libpq does? | **No**: `require` encrypts without checking the chain on both connections; only `verify-ca` / `verify-full` check it | `M5-pg-source.md` D-M5.16 |
| Q-M5-11 (2026-10-01) | JSON numbers went through `f64` (rounded beyond 15–17 digits; beyond the `f64` range the table halted) | **serde_json `arbitrary_precision`, workspace-wide**: `jsonb` numbers are `numeric` and keep every digit | §2.1; `Type_Mapping.md`; `M5-pg-source.md` Deviations, Task 4 |
| Q-M9-1 | Snapshot-job persistence | **Persisted in the pipeline state location** (`snapshots/jobs/<job_id>.json`); listed after a restart or failover via `GET /v1/pipelines/{id}/snapshots`; a running job continues on the new owner from its chunk cursors | `Data-Flow.md` §On-Demand Snapshot + State Layout; System-Overview and API_RBAC endpoint tables |

### 9.2 Open questions (answer before the named milestone)

- Recorded for later plans: M7 — the decoder validates `gno >= 1` before `GtidSet::insert` (anonymous
  GTID events carry 0, and `insert_range` panics on 0). The two M1 items are settled in M1 Task 1
  (2026-09-27): `MAX_LIST_DEPTH = 8` for `list<T>`, and the public-API convention (root re-exports,
  `Display`+`FromStr` round-trip incl. `FromStr for Decimal`, errors named after what failed) is in
  `data-avalanche-core`'s crate docs.

- **Q-M9-2:** `GET /v1/snapshots/{job_id}` is served by any node, but the job lives in one pipeline's
  state location. Either the job ID embeds the pipeline ID so any node can read it from storage, or
  the status endpoint moves under `/v1/pipelines/{id}/snapshots/{job_id}`. Decide in M9 planning.
