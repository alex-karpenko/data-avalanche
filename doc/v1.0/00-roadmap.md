---
updated: 2026-10-29
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
| P-6 | Deliverables | Binary, **container image**, **Kubernetes manifests + Helm chart**, **CI** (GitHub Actions), **documentation site** (an Astro Starlight site, Q-M11b-1): architecture and internal flows, REST API reference, configuration reference with detailed examples, operator runbooks |
| P-7 | S3 emulator | **RustFS** (not MinIO) |
| P-8 | Source versions | PostgreSQL 14–18, MySQL 8.4 LTS, MariaDB 10.11 / 11.4 / 11.8 LTS. CI runs the newest of each on every change and the full matrix weekly |
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
├── .github/workflows/            ci.yml (per change) · weekly.yml (matrix) · cloud.yml (real clouds, scheduled/manual)
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
│   ├── docker/Dockerfile         the image: distroless cc (Debian 13), non-root (M11a)
│   ├── helm/data-avalanche/      StatefulSet, headless Service, ConfigMap, probes, PDB (M11a)
│   └── prometheus/               example alert rules and their promtool test (M10)
└── docs/                         Starlight site: astro.config.mjs, package.json, pages under src/content/docs/ (M11b)
```

**Dependency direction** (arrows point at dependencies; nothing points up):

```
data-avalanche (bin) ─► api ─► cluster ─► pipeline ─► core, config, state, storage
      │             │        │
      │             │        └─► state ─► storage ─► core
      ├─► source-pg ─► core, config
      ├─► source-mysql ─► core
      ├─► sink-delta ─► core, storage
      └────────────►┴─► config ─► core                         telemetry ◄─ (all runtime crates)
```

Rules:
- `data-avalanche-core` has **no I/O** and no async runtime dependency beyond trait definitions
  (`async-trait`).
- Source crates never depend on sink crates, or the reverse. `data-avalanche-pipeline` joins them at
  run time through `core`'s traits and depends on neither; only the binary links them (Q-M5-24). No
  other crate takes `sink-delta`, even as a dev-dependency; the pipeline's live tests take
  `source-pg` as a dev-dependency.
- `data-avalanche-api` (M9a) reads `state`, `config`, `core` and `telemetry` directly beside
  `cluster`, and links no source or sink crate: a status is read from the state documents.
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
| MySQL binlog + SQL | `mysql_async` (`default-features = false`, features `default-rustls`, `binlog`) / `mysql_common` (dev, building events in tests) | `=0.37.1` / `=0.37.3` | S-MY0–11b; M7a's integration suite |
| MariaDB compressed events | `flate2` (already in the lockfile through `mysql_common`) | `1` | Q-M7a-5; M7a Task 6 |
| DDL classifier | `sqlparser` (`default-features = false`, features `std`: no `recursive-protection`, which builds C) | `=0.62.0` | MySQL_DDL_Classifier §5; M7a Task 11 |
| YAML 1.2 (serde) | `serde-saphyr` (`deserialize` only) | `=1.3.0` | Verified in M1 planning (2026-09-27): strict booleans, duplicate-key errors, `Spanned` locations, unknown-key errors with location. One trap: `${…}` inside a flow collection must be quoted (M1 Review Focus 1). R-9 closed |
| Membership (SWIM) | `foca` (`default-features = false`, features `std`, `serde`, `postcard-codec`) + `rand` (features `std`, `std_rng`, `sys_rng`; foca's random source) | `=2.0.0` / `0.10` | The M8 probe gate (2026-10-20, `prototypes/cluster-capability`): `memberlist` 0.8.5 failed (reaping corrupts its member table, C13), foca passed F1–F8 (Q-M8-1); M8a's gossip and cluster suites |
| Host addresses | `if-addrs` (an unset `advertise_addr`'s private IPv4) | `0.15` | M8a Task 9 |
| HTTP API | `axum` `0.8` + `utoipa` `6` | | |
| Metrics | `prometheus-client` | | |
| Tracing | `tracing`, `tracing-subscriber`, `opentelemetry-otlp` | | |
| CLI | `clap` `4` | | |
| Misc | `thiserror` 2, `async-trait`, `bytes`, `chrono`, `uuid`, `serde_json` (feature `arbitrary_precision`: JSON numbers keep every digit, Q-M5-11), `humantime`, `xxhash-rust` `0.8` (feature `xxh3`: the ring's hash, anchor `hash_algo: xxh3`; M8a), `proptest` (dev) | | |

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

### 3.3 Schema model and change set (Schema_Change §7, Multi_Sink §6.2; landed in M3, extended in M6a)

`M3-core-data-path.md` D-M3.1…18 and its "Deviations during execution" are the rulings behind this.
M6a adds the `snapshot` flag, two halt codes, and serde for `TableSchema` and `HaltReason` (D-IMPL-13).

```rust
pub struct ColumnSchema { pub name: String, pub value_type: ColumnValueType, pub logical: LogicalType,
                          pub nullable: bool /* source's; targets ignore it (S-O) */, pub source_type: String,
                          pub source_ordinal: Option<i32> /* PG attnum */ }
impl ColumnSchema { pub fn new(name, value_type, source_type) -> Self /* canonical logical, nullable */; pub fn not_null(self) -> Self; }
pub fn canonical_logical(t: &ColumnValueType) -> LogicalType;             // UInt64 → decimal(20,0), …
pub fn hop2_compatible(value_type: &ColumnValueType, logical: &LogicalType) -> bool;   // an override Hop 2 can store

pub struct TableSchema { /* private: table, columns, identifier, source_id */ }   // serde: checked like new() on load
impl TableSchema {
    // Checks, in order: no reserved name; a non-empty identifier; identifier columns listed once, present,
    // NOT NULL, non-floating, primitive; unique names; every logical type hop2_compatible.
    pub fn new(table: TableRef, columns: Vec<ColumnSchema>, identifier: Vec<String>) -> Result<Self, HaltReason>;
    pub fn table(&self) -> &TableRef; pub fn columns(&self) -> &[ColumnSchema]; pub fn identifier(&self) -> &[String];
    pub fn column(&self, name: &str) -> Option<&ColumnSchema>; pub fn index_of(&self, name: &str) -> Option<usize>;
    pub fn is_identifier(&self, name: &str) -> bool;
    // The source's identity of the table (PostgreSQL: the relation OID); another one under the name
    // is a dropped table (Q-M5-20). Kept in the cached schema.
    pub fn with_source_id(self, id: u64) -> Self; pub fn source_id(&self) -> Option<u64>;
}
pub enum HaltReason {                        // code(): the SCREAMING_SNAKE name, e.g. NULL_IDENTIFIER
    ReservedColumnCollision { column }, TableDropped, UnsafeDdl { detail }, NoIdentifier,
    IdentifierNotUsable { column, reason }, IdentifierNotInReplicaIdentity { missing: Vec<String> },
    EvolutionUnsupported { detail }, EvolutionDisabled, NumericOverflow { column },
    InvalidDateValue { column, raw },        // MySQL/MariaDB zero-in-part / invalid dates (Q-M7-1)
    SchemaMismatch { column, detail },       // a value or buffered row that doesn't fit the schema
    NullIdentifier { column },               // NULL or unsent key value
    NotReplicable { reason },                // TABLE_NOT_REPLICABLE: a named UNLOGGED table (Q-M5-13)
    SinkRejected { detail },                 // a Permanent commit or evolve error (M6a, D-M6a.3)
    SnapshotTimeout { limit_secs: u64 },     // timeouts.snapshot exceeded in one run (Q-M6a-3)
}   // serde: tagged by code, {"code": "UNSAFE_DDL", "detail": …} (the halt registry, §3.5)
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

pub struct PositionRange { pub start: SourcePosition, pub end: SourcePosition,     // (start, end]; end = the flush frontier, the marker
                          pub last_change: SourcePosition }   // M6b: the table's last transaction in the range, which the guard compares
pub struct SplitInfo { pub txn_start: SourcePosition, pub slice: u32 }              // slice ranges end at txn_start
pub struct TableChangeSet {
    pub table: TableRef, pub schema: SchemaRef /* upsert_schema */, pub identifier: Vec<String>,
    pub truncate: bool, pub upserts: Vec<RecordBatch>, pub deletes: Vec<RecordBatch>,
    pub position: PositionRange, pub split: Option<SplitInfo>,
    pub snapshot: bool,          // chunk rows read now: the guard never skips them (Q-M6a-6)
}
impl TableChangeSet {
    // Upsert batches: schema's fields, then the same unchanged masks (non-identifier columns) in every batch;
    // a non-empty identifier; delete batches start with it; a key at most once across both; no NULL key;
    // the range doesn't run backwards and holds last_change; a slice's range ends at txn_start; a snapshot set only upserts
    // (no deletes, truncate or slice).
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
    fn stats(&self) -> SinkStats { SinkStats::default() }                              // M6b: the MERGE cost, for the metrics
    async fn evolve(&self, table: &TableRef, delta: &SchemaDelta, owner_epoch: u64)  // M6b: fenced (Q-M6b-7)
        -> Result<EvolveOutcome, SinkError>;
    async fn commit(&self, cs: TableChangeSet, marker: CommitMarker) -> Result<CommitOutcome, SinkError>;
}
// commit, in order (documented on the trait; testkit `MemorySink` is the reference):
//  1. cs.validate(); 2. refuse a marker whose committed_position != cs.position.end; 3. NoTable;
//  4. fence, then guard, against the stored marker (Fenced / SkippedCovered change nothing);
//  5. the set's identifier is the table's (M6b); every column of the set in the table with the same type;
//     a table column missing from the set is written NULL;
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
// A partition column is a source column or a lineage column (`day(_ts)`, config §9.3; M6a).
pub struct CommitMarker { pub owner_epoch: u64, pub committed_position: SourcePosition, pub pipeline_id: String }
impl CommitMarker {
    pub fn fences(&self, ours: &CommitMarker) -> bool;   // stored.owner_epoch > ours.owner_epoch
    // Guard (D-M3.14, S-P): a snapshot set → never covered (Q-M6a-6, S-T; the fence still applies);
    // split None → stored covers cs.position.last_change (M6b, DA-X-12; was end); split Some → stored strictly past txn_start.
    pub fn covers(&self, cs: &TableChangeSet) -> Result<bool, PositionError>;
}
pub struct SinkTableState { pub exists: bool, pub schema: Option<SchemaRef> /* Hop 2 terms, soft-drops stay */,
                            pub marker: Option<CommitMarker> }   // None: no positioned own commit in reach, never "never written"
pub enum EvolveOutcome { Applied, Degraded { warning: String }, Unsupported { reason: String },
                         Fenced { current_epoch: u64 } }        // M6b: a newer owner wrote the table; nothing changed
pub struct SinkStats { pub files_rewritten: u64, pub rows_copied: u64 }   // M6b; a sink that doesn't measure reports zeros
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
  - `create_snapshot_job` / `save_snapshot_job` / `snapshot_job` / `snapshot_jobs`;
  - from M6a (S-R): the schema cache, `table_schema` / `table_schemas` / `save_table_schema`
    (`schemas/<schema>.<table>.json`, `table_schema_key`; a stored schema that names another table
    is `Corrupt`); the halt registry, `halts` / `record_halt` / `clear_halt` (`halts.json`,
    `HaltsDoc { tables: Vec<TableHalt> }`, `TableHalt { table, reason: HaltReason, at:
    SourcePosition }`); and the slot-creation intent, `SlotDoc::intent(slot_name)` /
    `is_intent()` (`confirmed_flush_lsn` `0/0`, written before a managed first start's `prepare`).
  - from M9a (D-M9a.2…5): the owner's view, `status` / `save_status` (`status.json`, `StatusDoc {
    holder_node_id, owner_epoch, actual: ActualState, error, code, degraded, written_at }`,
    trusted only under the live lease's epoch and holder); the requests, `requests` / `request`
    (`requests.json`, `RequestsDoc { restart, flush: Option<Request { id, at }> }`, the last of
    each `Action`); and `transition(to) -> Transition { Changed { from }, Unchanged, Refused {
    current } }`, the desired state's checked compare-and-swap (Pipeline_Lifecycle §3).
  - from M9b (D-M9b.2): the job document takes a status per table, `JobTable { table, status:
    JobTableStatus { Queued, Running, Completed, Failed }, rows_done, error }`;
    `SnapshotJobDoc::new(job_id, tables, requested_by, requested_at)` (all queued),
    `is_finished`, and `settle(now)`, which derives the job's status from its tables' and, once
    it ends, sets `finished_at` and `error`; `update_snapshot_job(job_id, f) ->
    Option<SnapshotJobDoc>` (read-modify-write, `None` when absent) and
    `unfinished_snapshot_jobs()`. The job id is `<pipeline_id>~<UUID v7 hex>` (Q-M9-2).

Lease release is a **tombstone write** (object_store has no conditional delete). T1–T10 prove the lease CAS on every backend (`tests/lease_conformance.rs`, `test-infra/README.md`).

### 3.6 Source connector (`data-avalanche-core` owns the trait, Q-M5-2; each source crate implements it; defined in M5, extended in M6a, M6b, M7a and M7b)

`data-avalanche-core::source` (M5 Task 1; D-M5.1 lists where it differs from the first sketch):

```rust
#[async_trait]
pub trait SourceConnector: Send + Sync {                    // built from the resolved Pipeline
    async fn preflight(&self) -> Result<Preflight, SourceError>;                    // config §11.2 pipeline rows
    async fn resolve_tables(&self) -> Result<ResolvedTables, SourceError>;         // channel A: tables, halted, skipped, warnings
    async fn prepare(&self, checkpoint: Option<&Checkpoint>, tables: &[TableRef])
        -> Result<Prepared, SourceError>;                                           // PG: publication + slot state machine; MySQL: dialect, server, purge → StartPlan
    async fn reset_position(&self) -> Result<Checkpoint, SourceError>;             // after PositionUnreachable (D-15.3); M7b: with its server
    async fn retained(&self) -> Result<Option<RetainedLog>, SourceError>;          // M6b (was retained_bytes): the heartbeat's read
    async fn current_position(&self) -> Result<SourcePosition, SourceError>;       // a rescan's catch-up mark (Q-M6a-4)
    async fn in_flight(&self) -> Result<Option<Vec<InFlightTxn>>, SourceError>;   // M6a; default None (DA-SNAP-9)
    async fn read_chunk(&self, t: &TableSchema, after: Option<&KeysetCursor>, limit: usize) -> Result<Chunk, SourceError>;
    async fn fetch_rows(&self, t: &TableSchema, keys: &[Row]) -> Result<Vec<Row>, SourceError>;  // toast_strategy: re_query
    async fn open_stream(&self, from: &Checkpoint, tables: &[TableSchema]) -> Result<Box<dyn ChangeStream>, SourceError>;  // M7b: a Checkpoint
    async fn end_other_reader(&self) -> Result<bool, SourceError>;                // M8a; default false: the lease holder ends a stale reader (DA-SRC-28)
    async fn augment(&self, cached: &TableSchema, hint: &SchemaHint)
        -> Result<(SchemaDelta, Option<TableSchema>), SourceError>;                // channel C; None = dropped
    async fn rescan(&self, known: &[TableSchema]) -> Result<RescanReport, SourceError>;  // channel D
    async fn teardown(&self) -> Result<(), SourceError>;                           // auto_cleanup
}
#[async_trait]
pub trait ChangeStream: Send {
    async fn next(&mut self) -> Result<StreamItem, SourceError>;                   // an error ends the stream; cancel-safe
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
pub struct Prepared { pub plan: StartPlan, pub added: Vec<TableRef>, pub warnings: Vec<String>,
                      pub server: Option<ServerIdentity> }                  // M7a: the server the stream reads (MySQL)
pub struct Checkpoint { pub position: SourcePosition, pub server: Option<ServerIdentity> }  // M7a; From<SourcePosition>
pub enum ServerIdentity { Mysql { server_uuid: String }, Mariadb { server_id: u32 } }      // M7a: moved from the state crate
pub enum SourceError { Transient(String), Permanent(String), Contended(String) /* M6a: PG 55006 */,
                       PositionUnreachable { position: String, detail: String },
                       TableHalt { table: TableRef, reason: HaltReason } }
impl SourceError { pub fn halt_code(&self) -> Option<&str>; }   // M7b: a Permanent's `CODE: ` prefix (D-M7b.2)
```

Also: `Preflight { server_version, warnings }`, `ResolvedTables { tables, halted, skipped, warnings }`,
`RetainedLog { bytes, streamed }` (M6b: the `max_wal_lag_bytes` measure, and whether a stream reads
the slot, the watchdog's evidence, DA-SRC-16; M10, D-M10.4: `bytes: Option<u64>`, `None` on MySQL,
and `behind: Option<Duration>`, MySQL's binlog lag, the age of the last transaction the stream
read, `0` at the server's head), `KeysetCursor { key: Row, collation }`,
`Chunk { rows, next, visibility: Option<ChunkVisibility> }`,
`RescanReport { dropped, changed, halted }` (the pipeline applies a `changed` schema only once the
stream has passed `current_position` read after the rescan, Q-M6a-4), `identifier_values(&TableSchema,
&Row)`. From M6a (DA-SNAP-9): `ChunkVisibility::Xids(XidSnapshot)`, `XidSnapshot { xmin, xmax, xip }`
with `sees(xid)` (a full `xid8` or the stream's 32-bit xid), `InFlightTxn { xid, tables: Vec<u64> }`
(tables by `source_id`). PostgreSQL reads each chunk in a REPEATABLE READ transaction that records
`pg_current_snapshot()`, and `in_flight` lists writers from `pg_locks`; a source with `None`
(MySQL) falls back to the window rule. PostgreSQL positions (D-M5.2): a change's
`source_pos` is its transaction's commit LSN, its `seq` the XLogData `wal_start`; `Commit` carries
the `end_lsn` that is acknowledged and resumed from. M6b bounds the catalog connect by
`timeouts.connect` as a whole (TCP, TLS, startup, authentication).

From M7a (D-M7a.1, D-M7a.18…22): a checkpoint carries the server it was read from, and `prepare`
reports the server the stream reads (`Prepared.server`; PostgreSQL records none). The state
crate re-exports core's `ServerIdentity`, so `replication/position.json` is unchanged; M7b records
it with the worker's MySQL checkpoint. MySQL positions: a change's `source_pos` (`_lsn`) is its
transaction's own GTID, or its start `file:pos` in file mode; its `seq` is (file index, computed end
offset, Q-M7-4); `Commit` carries the cumulative GTID set, the MariaDB state, or the XID's end.
MySQL `prepare` follows `binlog_purge_action` for a purged or other-dialect checkpoint and halts
`BINLOG_SOURCE_CHANGED: …` for a file position from another server; `retained` is `bytes` 0 and
`streamed` from the dump's connection id in `PROCESSLIST`; `fetch_rows` is `Permanent`; `teardown`
does nothing; chunks carry no visibility (the window rule, with locking reads, Q-M7-3).

From M7b (D-M7b.1, D-M7b.2, D-M7b.7, D-M7b.8): a reconnect opens the stream without `prepare`, and a
lost position is replaced without it, so `open_stream` takes the whole checkpoint and
`reset_position` returns one with the server it read (PostgreSQL: none). In file/position mode
the MySQL dump reads the identity on its own connection before the dump command and halts
`BINLOG_SOURCE_CHANGED` on another server or none recorded; GTID modes don't check. A server that
reports no identity (no row, NULL, empty) is `Permanent`. `halt_code` reads the code D-M7a.8 puts
at the front of a pipeline-scope halt's message: `[A-Z][A-Z0-9_]*` holding a `_`, then `": "`.

From M8b (D-M8b.1): `fn reconfigure(&self, pipeline: &dyn Any)`, a no-op by default, hands the
running connector a reloaded `data_avalanche_config::Pipeline` (core names no config type).
PostgreSQL takes `heartbeat`, `slot_on_orphan`, `auto_cleanup` and the table patterns; MySQL takes
`binlog_purge_action`, `heartbeat` and the table patterns. Each is read at the next connection,
prepare, resolve or teardown; an open stream keeps what it opened with.

From M9b (D-M9b.10): `async fn describe_tables(&self, tables: &[TableRef]) ->
Result<ResolvedTables, SourceError>` describes tables by exact name, in the order given, whatever
the config's patterns include and without its table-set checks (an on-demand snapshot's tables
outside the config): a missing table is skipped `no such table`; a PostgreSQL partitioned parent
skipped, an unlogged table halted; a MySQL table of another database skipped, a system-versioned
one halted; a table without an identifier halted on both. MySQL's channel C takes a
`SchemaHint::Columns` as a baseline to the catalog: the whole table, no rename proven
(DA-SCHEMA-10; it was refused, which halted a MySQL baseline with a column change).

### 3.7 Cluster (Clustering §19–§20)

`LeaseStore` over `VersionedStore` (acquire / renew / takeover-evaluate / release). A `Renewer` state
machine (`Held | Renewing | Degraded | Lost`) exposes `may_commit()` = state ∈ {Held, Degraded} ∧
now < deadline. `PipelineController` is the sole lease writer and owns the `PipelineWorker`.
`ClusterReconciler` pushes `Own/DontOwn` targets. The same types run in single-node mode; M6b ships them
with a self-only ring, behind M6a's `CommitGuard` (§3.11).

What M6b ships (`data-avalanche-cluster`, `M6b-single-node-control.md` D-M6b.4…8):

```rust
pub struct LeaseTiming { pub duration: Duration, pub renewal_interval: Duration, pub safety_margin: f64 }
impl LeaseTiming { pub fn new(cluster: &Cluster) -> Self; pub fn deadline(&self, renewed: Instant) -> Instant; }
pub enum RenewState { Held, Degraded, Lost }   // "Renewing" is no state: a renewal in flight leaves it as it was
pub struct LeaseGuard;                          // impl CommitGuard: state ∈ {Held, Degraded} ∧ now < deadline, on its own clock
impl LeaseGuard { pub fn state(&self) -> RenewState; pub async fn lost(&self); }
pub struct LeaseRun { pub guard: Arc<LeaseGuard>, pub owner_epoch: u64 }   // the renewer task
impl LeaseRun { pub fn start(leases, lease: Versioned<LeaseDoc>, holder, timing, written: Instant,
                             metrics: Arc<PipelineMetrics>) -> Self; pub async fn stop(self) -> RenewEnd; }
pub enum RenewEnd { Stopped(Versioned<LeaseDoc>), Lost { reason: String } }
pub struct Claim;                               // acquire, and takeover after a full lease duration unrenewed
impl Claim { pub fn new(leases, pipeline_id: &str, holder, timing) -> Self;
             pub async fn step(&mut self) -> Result<Claimed, StateError>; }
pub enum Claimed { Mine { lease: Versioned<LeaseDoc>, written: Instant }, HeldBy { node_id: String, takeover_at: Instant } }
pub async fn release(leases: &Leases, lease: &Versioned<LeaseDoc>) -> Result<(), StateError>;
pub trait WorkerFactory: Send + Sync + Debug { fn build(&self, pipeline: &Pipeline) -> Result<Parts, String>; }
pub struct Parts { pub source: Arc<dyn SourceConnector>, pub sink: Arc<dyn Sink>, pub hooks: Arc<dyn Hooks> }
pub enum Target { Own, DontOwn }
pub enum Actual { Standby { holder }, Starting, Running, Paused, Stopped, Dumped, Retrying { error }, Error { error, code: Option<String> /* M7b */ } }
pub struct ControllerStatus { pub actual: Option<Actual>, pub owner_epoch: Option<u64>, pub lease: Option<RenewState>,
                              pub worker: Option<WorkerStatus> }
pub struct ControllerSettings { pub timing, pub drain_timeout, pub restart: Backoff, pub start_jitter, pub tuning }
pub struct ControllerDeps { pub leases, pub holder, pub factory: Arc<dyn WorkerFactory>, pub metrics: Metrics,
                            pub settings: ControllerSettings, pub starts: Arc<Semaphore> /* Phase 0 permits */ }
impl PipelineController { pub fn new(..) -> Self; pub async fn run(self, target: watch::Receiver<Target>); }
impl ClusterReconciler { pub fn start(pipelines: Vec<(Pipeline, PipelineDocs)>, deps: &ControllerDeps) -> Self;
                         pub fn statuses(&self) -> BTreeMap<String, watch::Receiver<ControllerStatus>>;
                         pub async fn shutdown(self); }
pub fn node_identity(cluster: &Cluster, started: SystemTime) -> Result<Holder, String>;
```

The controller polls the desired state each `lease_renewal_interval` (Q-M6b-2) and maps a worker
error by `WorkerError::recovery()`: `Retry` reruns after a backoff, `Lease` stands by, `Error` holds
ERROR until a stop (Q-M6b-3, Q-M6b-12). It logs each worker warning once and each reconnect with
its reason. From M7b (Q-M7b-4), `Error` carries the halt's code (`WorkerError::code`, from a source error
only), and the `ERROR` log line logs it as the field `code`; no new metric.

What M8a adds (`M8a-membership.md` D-M8a.1…21): the gossip (foca 2.0 in our actor, Q-M8-1), the
ring over its members, and the reconciler on the ring.

```rust
pub struct GossipKey;                            // keyed BLAKE3 MAC from the anchor's fingerprint_key (Q-M8-2)
impl GossipKey { pub fn derive(fingerprint_key: &[u8; 32]) -> Self; pub fn seal(&self, payload: &[u8]) -> Bytes;
                 pub fn open<'a>(&self, packet: &'a [u8]) -> Option<&'a [u8]>; }
pub trait Wire: Send + Sync + Debug + 'static { fn send(&self, to: SocketAddr, packet: Bytes); }
impl Udp { pub async fn bind(addrs: &[SocketAddr]) -> Result<(Self, mpsc::Receiver<Bytes>), ClusterError>; }
pub enum Phase { Joining, Active, Draining, Leaving }
pub struct NodeMeta { pub phase: Phase, pub config_generation: u64, pub build: String }
pub struct NodeIdentity { pub node_id, pub incarnation: u64, pub rev: u32, pub addr: SocketAddr, pub meta: NodeMeta } // foca's Identity
pub struct Members { pub me: String, pub nodes: BTreeMap<String, Member> }   // per name, the latest live identity
pub struct Member { pub addr: SocketAddr, pub meta: NodeMeta, pub suspect: bool }
impl Gossip { pub fn start(me, key, settings: GossipSettings, wire: Arc<dyn Wire>, inbox) -> Self;
              pub fn members(&self) -> watch::Receiver<Members>; pub fn announcer(&self) -> Announcer;
              pub fn set_phase(&self, Phase); pub async fn leave(self); }
impl Ring { pub fn new<'a>(nodes: impl IntoIterator<Item = &'a str>, tokens: u32) -> Self;
            pub fn owner(&self, pipeline_id: &str) -> Option<&str>; }  // xxh3 points; first point ≥ xxh3(id)
impl Placement { pub fn new(tokens, stabilization) -> Self; pub fn update(&mut self, &Members, Instant) -> bool;
                 pub fn next_due(&self) -> Option<Instant>; pub fn ring(&self) -> &Ring; pub fn generation(&self) -> u64; }
impl ClusterReconciler { pub fn start(pipelines, deps: &ControllerDeps, members: watch::Receiver<Members>,
                                      settings: ReconcilerSettings) -> Self;
                         pub async fn released(&self); pub async fn shutdown(self); }   // a drop is a crash
pub struct ControllerDeps { /* M6b's, and */ pub handoffs: Arc<Semaphore> /* max_concurrent_handoffs */ }
impl Claim { pub fn at_least(&mut self, epoch: u64); }      // DA-CLUS-14: the fence's epoch as a floor
impl Leases { pub async fn take_over_at(&self, observed, holder, lease_duration, now, at_least: u64) -> Result<..>; }
pub enum Seeds { Static(Vec<HostPort>), Dns { name: String, port: u16 } }
pub fn discover(seeds, me: Vec<SocketAddr>, announcer: Announcer, members) -> JoinHandle<()>;  // every 5 s while alone
pub async fn advertise(m: &Memberlist, interfaces: &[IpAddr]) -> Result<SocketAddr, ClusterError>;
pub async fn join(gossip: &Gossip, seeded: bool);                 // Joining → Active (≤ 5 s for a first peer)
pub async fn leave(gossip: Gossip, reconciler: ClusterReconciler); // Draining → released → Leaving → leave
pub enum ClusterError { NodeName(String), AdvertiseAddr(String), Bind { addr, source } }
```

A node is placed on the ring when it is an `Active` member; it leaves the ring at once when it
stops being one (gone, `Draining`, `Leaving`), and joins after `rebalance_stabilization` of continuous
eligibility (D-11). A pipeline's target is `Own` exactly when the ring's owner is this node; a
hand-off waits for a `max_concurrent_handoffs` permit; a controller that ended unasked is started
again (DA-OPS-7).

What M8b adds (`M8b-config-generations.md` D-M8b.3…7, D-M8b.11): a controller's update, the
reconciler's reload, and the config coordinator.

```rust
pub enum Update { Reconfigure(Box<Pipeline>) /* R: in place */, Restart(Box<Pipeline>, PipelineDocs) /* PR: same lease */,
                  Remove /* drain, teardown, release */, DrainTimeout(Duration) }
impl PipelineController { pub fn updater(&self) -> mpsc::UnboundedSender<Update>; }
pub enum PipelineReload { Add(Pipeline, PipelineDocs), Remove(String), Reconfigure(Pipeline), Restart(Pipeline, PipelineDocs) }
pub struct Reload { pub pipelines: Vec<PipelineReload>, pub cluster: Cluster }   // the cluster's R fields too
pub struct Reloader;  impl Reloader { pub fn apply(&self, reload: Reload) -> bool; }
impl ClusterReconciler { pub fn reloader(&self) -> Reloader; }   // statuses() follow the reloads
impl Placement { pub fn set_stabilization(&mut self, stabilization: Duration); }
pub struct Advertiser; impl Advertiser { pub fn set_config_generation(&self, generation: u64); }
impl Gossip { pub fn advertiser(&self) -> Advertiser; }
impl Members { pub fn newest_generation(&self) -> u64; }
pub const PUBLISH_ATTEMPTS: u32 = 5;
pub struct CoordinatorDeps { pub generations, pub env: Arc<dyn Env + Send + Sync>, pub key: FingerprintKey, pub node_id,
                             pub docs: Arc<DocsFn>, pub factory, pub reloader: Reloader, pub advertiser: Advertiser,
                             pub metrics: Metrics, pub globals: Arc<dyn Fn(&Globals) + Send + Sync> }
pub struct Reloaded { pub generation: u64, pub published: bool }
pub enum ReloadError { Invalid(Report), Rejected(Report) /* V-061 */, Contended, State(StateError), Stopped }
impl ConfigCoordinator { pub fn start(config: ResolvedConfig, generation: Versioned<GenerationDoc>, deps: CoordinatorDeps,
                                      members: watch::Receiver<Members>, poll: Duration) -> Self;
                         pub fn applied(&self) -> u64; pub async fn config(&self) -> ResolvedConfig;
                         pub async fn latest(&self) -> Result<Option<u64>, StateError>;
                         pub async fn reload(&self, text: &str) -> Result<Reloaded, ReloadError>; pub async fn stop(&self); }
```

A node adopts each newer generation, found by a poll every `lease_renewal_interval` or announced by
a member's gossiped `config_generation`. It resolves the generation in its own environment, diffs
it (config §12), and hands the changes to the reconciler. A generation it can't apply is logged
once and tried again at each poll. A reload publishes the node's file with a CAS on the
generation pointer, retried up to `PUBLISH_ATTEMPTS` times when another node published first. The
same text is published again when it resolves differently here. A pipeline added back while its
removal runs starts once that removal ends. A pipeline added in the same reload as a removal, and
sharing the removed one's PostgreSQL slot or publication, its sink location or its state location
(an `id` change: delete, then recreate, config §9.1), claims only once the removed pipeline's lease
is released, gone, or unrenewed for a lease duration (M8b's deviations; DA-OPS-10 for what it
doesn't cover).

From M9a (D-M9a.5, D-M9a.7): the lease holder's controller writes `status.json` at a poll when its
status changed, and reads `requests.json` when its term starts (the baseline). At each poll a new
restart id discards the worker and runs a new one under the same lease (`ERROR` cleared, the
desired state kept); a new flush id sends `Command::Flush`. `ConfigCoordinator::check(&self, text)
-> Result<Checked { generation, warnings, changes }, ReloadError>` runs a reload's checks without
the publish.

From M9b (D-M9b.3, D-M9b.9): the controller lists the unfinished snapshot jobs when its term
starts, when it takes a restart (before the discard: a job comes with a restart under its id) and
at a reload's restart. At each poll it follows each running job table from `halts.json` and the
table's snapshot document (a halt fails it with its code; in progress gives `rows_done`;
completed completes it with `row_count`; anything else fails it), settles the job, and stops
following one that ended.

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
  unsupported features, invalid locations, and (M6b, Q-M6b-6) a MERGE over `merge_memory_limit`:
  the table halts `SINK_REJECTED`.
- `Transient`: storage and network errors. Any `Transient` drops the cached table handle (M6b).

M6b also:
- loads a table only when its `_delta_log/` lists a log file (no kernel error for an absent table);
- detects a table re-created behind a live handle at refresh, by the e-tag of the commit file at
  the handle's version, and re-opens it (Q-M6b-8); a table dropped since the refresh is `NoTable`;
- fences `evolve` by the `txn` version (Q-M6b-7), and refuses a set keyed otherwise than the table.

`table_state` reads the Hop 2 schema back from the column metadata. Its `marker` is `None` when no
positioned own commit is in reach. The rulings are D-M4.1…15 (D-IMPL-11), with the plan's
deviations.

### 3.11 The pipeline worker (`data-avalanche-pipeline`, M6a)

`M6a-pipeline-worker.md` D-M6a.1…11 (D-IMPL-13) are the rulings behind this. One worker runs one
pipeline's whole data path, from a `SourceConnector` to a `Sink`:

```rust
pub struct WorkerDeps { pub source: Arc<dyn SourceConnector>, pub sink: Arc<dyn Sink>, pub docs: PipelineDocs,
                        pub owner_epoch: u64 /* every commit's marker */, pub guard: Arc<dyn CommitGuard>,
                        pub hooks: Arc<dyn Hooks> }
impl WorkerDeps { pub fn new(source, sink, docs) -> Self; }          // epoch 1, Unguarded, NoHooks
pub struct PipelineWorker { /* private */ }
impl PipelineWorker {
    pub fn new(pipeline: &Pipeline /* resolved, §3.8 */, deps: WorkerDeps, tuning: Tuning) -> (Self, WorkerHandle);
    pub async fn run(self) -> Result<Exit, WorkerError>;              // until Drain (or the handle drops), Dumped, or an error
}
pub struct WorkerHandle { pub control: mpsc::Sender<Command>, pub status: watch::Receiver<WorkerStatus> }
pub enum Command { Drain /* flush, make durable, close: Exit::Drained */, Flush,
                   Pause /* M6b: finish the open txn, flush, durable, then read nothing; the connection stays */, Resume,
                   Reconfigure(Box<Pipeline>) /* M8b: a reload's R fields, in place; Command is Clone, not Copy */ }
pub enum Exit { Drained, Dumped /* every table a dump_* table, done */ }
pub enum Phase { Starting, Streaming, Reconnecting { error: String }, Draining, Paused /* M6b */ }
pub struct WorkerStatus { pub phase: Phase, pub read: Option<SourcePosition>, pub checkpoint: Option<SourcePosition>,
                          pub retained_bytes: Option<u64>, pub degraded: Option<String> /* the last retried error */,
                          pub halted: BTreeMap<TableRef, HaltReason>, pub snapshots: BTreeMap<TableRef, SnapshotProgress>,
                          pub warnings: Vec<String> /* each once, capped (M6b) */,
                          pub reconcile_log: ReconcileLog { entries, keys }, pub warnings_dropped: u64,
                          pub reconnects: u64 }   // the last three: M6b
pub struct SnapshotProgress { pub rows_done: u64 }
pub struct Tuning { pub source_backoff: Backoff, pub sink_backoff: Backoff, pub in_flight_backoff: Backoff /* 0.5 s → 10 s */,
                    pub contended_window: Duration /* 120 s */, pub tick: Duration /* 1 s */, pub augment_attempts: u32 /* 3 */ }
pub struct Backoff { pub first: Duration, pub max: Duration }       // default 1 s → 60 s; delay(attempt): jittered upper half
pub trait CommitGuard: Send + Sync + Debug { fn may_commit(&self) -> bool; }   // M6b: the lease renewer; Unguarded
pub trait Hooks: Send + Sync + Debug { fn at(&self, point: &HookPoint) -> HookAction; }  // crash tests; NoHooks
pub enum HookPoint { SinkCommitted { table }, CheckpointPersisted { position }, ChunkEmitted { table },
                     SchemasCached /* M6b */ }
pub enum HookAction { Continue, Crash /* → WorkerError::Crashed, nothing more written */ }
pub enum WorkerError { Refused(String) /* Phase 0: sink type, Iceberg (D-V1.4), capabilities */, Source(SourceError),
                       Contended { waited, detail }, Sink(SinkError), State(StateError), Fenced { table, current_epoch },
                       LeaseLost, TransactionTooLarge { bytes, limit }, WalLag { lag, limit }, Accumulator(AccumulatorError),
                       Crashed(HookPoint) }
impl WorkerError { pub fn recovery(&self) -> Recovery;              // M6b (Q-M6b-3)
                   pub fn code(&self) -> Option<&str>; }            // M7b: a source halt's code (Q-M7b-4)
pub enum Recovery { Retry, Lease, Error }
```

- **Phase 0** loads state and the recorded `sink_type`, resolves the tables (channel A), runs the
  source and sink preflight, writes the slot intent before a managed first start's `prepare`, and
  decides each table's snapshot. A table whose cached schema drifted while the worker was down is
  warned about on a resume, and a snapshot of it waits for a rescan (D-M6a.11).
- **A stream session** accumulates and flushes (§3.9), and runs snapshots inside the session.
  A chunk is reconciled against the stream by what its read saw, then committed as its own
  `snapshot` set after the stream's buffered changes (DA-SNAP-9, S-T). The session also applies
  schema changes (§8, channels C and D) and halts tables. Its WAL-lag guard ends the worker
  with `WalLag` past `max_wal_lag_bytes`.
- **The sink writer** works in a fixed order. It commits every table of a flush, then persists
  the halts, the checkpoint, snapshot progress and schema caches. Only then is the source
  acknowledged. A sink `Transient` retries forever (DEGRADED, backpressure through
  `buffer_capacity_rows`); `Permanent` halts the table `SINK_REJECTED`; `Fenced` ends the worker
  (S-S).
- **Between sessions**, a transient source error reconnects after `source_backoff`. A busy slot
  is retried within `contended_window`. A lost position is reset and re-snapshotted.
- **M6b adds:** every Phase 0 state write gated by the `CommitGuard`; pause and resume; the
  heartbeat's read run beside the loop, and the stream watchdog (`SILENT_HEARTBEATS` = 3: a stream
  silent for 3 heartbeats while 3 reads in a row fail or find no reader ends the session, and the
  worker reconnects; DA-SRC-16); each table's last change in its change set (DA-X-12).
- **M7b adds** (D-M7b.3…6): the checkpoint goes through one store, read from the source's block:
  PostgreSQL's slot (`replication/slot.json`, with a managed first start's intent) or MySQL's
  position document (`replication/position.json`, the position with its server). It replaces
  `Settings.slot`. The server recorded is the one the last start read (`Prepared.server`, or the
  reset checkpoint's); a reconnect keeps it. A checkpoint the store can't hold is a protocol
  error. The heartbeat is `source.postgres.heartbeat` or `source.mysql.heartbeat`, and the
  watchdog names an unread stream in the source's own terms. The PostgreSQL-only refusal at start
  is gone.
- **M9b adds** (D-M9b.5, D-M9b.6, D-M9b.8): Phase 0 reads the unfinished snapshot jobs. A queued
  job table is snapshotted from its first row whatever its mode; a running one in progress
  resumes from its cursor. A queued table with a recorded halt is rebased to the catalog's
  source id, baselined (planned and evolved as at a fresh start, its schema cached), and its halt
  cleared after the schemas. A job table neither resolved nor halted by the config is a copy
  (`Core::copies`): described by name, its sink table ensured, read in the session as a dump,
  never streamed, its schema cached nowhere. Phase 0 records each queued table as running or
  failed before the checkpoint.
- **M10 adds** (D-M10.1, D-M10.3, D-M10.5…7, D-M10.11): `pipeline::observe::{Observer,
  NoObserver}` and `WorkerDeps.observer: Arc<dyn Observer>`, which hears each flush, table commit,
  failed commit attempt and chunk read. `WorkerStatus` gains `unsynced_since_micros` (the commit
  time of the oldest change read but not durable), `rows_read: RowsRead`, `buffer_bytes`,
  `soft_dropped` (per table) and `source_behind`; `SnapshotProgress.waiting`. The worker opens
  the spans `pipeline.flush` (with a `sink.commit` per table), `snapshot.chunk` and
  `schema.apply`; it depends on `tracing` only, never on the telemetry crate.

### 3.12 Telemetry and the binary (`data-avalanche-telemetry`, `data-avalanche`; M6b)

```rust
// data-avalanche-telemetry: no workspace dependency; OpenMetrics text via prometheus-client
pub const CONTENT_TYPE: &str;                        // application/openmetrics-text; version=1.0.0
pub struct Metrics;                                  // the registry: Clone shares it
impl Metrics { pub fn new() -> Self; pub fn encode(&self) -> String; pub fn owned_by(&self, delta: i64);
               pub fn set_cluster_members(&self, n: usize);
               pub fn pipeline(&self, labels: PipelineLabels, per_table: bool) -> PipelineMetrics; }
pub struct PipelineMetrics;                          // status, lease, renewals, WAL lag, snapshot rows, halted tables,
                                                     // the reconciler log, totals (reconnects, MERGE cost); remove()
pub enum PipelineStatus; pub enum LeaseState; pub enum RenewFailure; pub struct PipelineLabels; pub struct Totals;

// data-avalanche (the binary; the only crate that links source-pg and sink-delta)
pub async fn run(args: RunArgs) -> Result<(), Fatal>;   // `data-avalanche run --config <path>` [--log-level] [--log-format]
pub enum Fatal { Config(String) /* exit 2 */, Startup(String) /* exit 1 */ }
pub struct Factory;                                      // impl WorkerFactory: PgConnector + DeltaSink per pipeline
```

- HTTP: `/health/live`; `/health/ready` once startup is done and every pipeline's controller
  runs (not: every pipeline is healthy); `/metrics`. Unauthenticated (config §6.3).
- Logs: `globals.log`, overridden by the command line; a bare `info` (or quieter) caps the Delta
  libraries (config §6.4).
- `DATA_AVALANCHE_E2E_CRASH=<point>[:<n>]` aborts the process at a hook point, only in a build
  with the `e2e-hooks` feature (Q-M6b-5).

From M8b (D-M8b.3, D-M8b.5, D-M8b.8…10):
- Telemetry: `PipelineMetrics::relabel(labels, per_table)` (a renamed pipeline) and `forget()` (a
  removed one); `Metrics::set_config_generation`, the gauge `data_avalanche_config_generation`.
- HTTP: `POST /v1/config/reload` (202 `{generation}`, 200 unchanged, 422 with the report, 409
  contended, 503 not running) and `GET /v1/config/status` (`{generation, applied, nodes,
  converged}`). Unauthenticated until M9 (Q-M8b-1).
- Logs: a generation's `globals.log` changes the filter and the format at run time, except where
  the command line set them.
- Startup warns when the node's file and the adopted generation differ in a per-node key
  (DA-OPS-8).

From M9a (D-M9a.1, D-M9a.6, D-M9a.8): the listener and every route move to
`data-avalanche-api` (`bind`, `Bound::serve`, `ServeError`, `Api`, `Node`, `router`, `routes`,
`Auth`, `Principal`, `Control`, `Stop`, `openapi`). TLS on the listener; the `/v1` routes behind
the auth and the least role of API_RBAC §2; the auth follows each generation (`Auth::set`). The
binary's run loop takes the API's leave (stays up, not ready) and shutdown (exits) as it takes
SIGTERM.

From M9b (D-M9b.1, D-M9b.4, D-M9b.11): the snapshot routes. `POST /v1/pipelines/{id}/snapshots`
(operator) writes the job, then a restart request under its id, and answers `202`; it is `409`
unless the desired state is running and a node holds the lease, or while an unfinished job holds
one of the tables. `GET /v1/pipelines/{id}/snapshots` and `GET /v1/snapshots/{job_id}` (viewer)
read the jobs from the pipeline's state, on any node.

From M10 (D-M10.1…16):
- Telemetry: the rest of the catalogue. `RowOp`, `SnapshotState`, `RowsRead`,
  `PipelineLabels.sink_type` (on the two commit series only); on `PipelineMetrics`:
  `replication_lag`, `transaction_buffer_bytes`, `binlog_lag`, `snapshot_state`,
  `soft_dropped_columns`, `flushed`, `committed`, `commit_failed`, `chunk_read`.
- Traces: `Traces::{new(instance), set(endpoint, ratio) -> Result<bool, TracesError>, layer(),
  flush(), shutdown()}`, `TracesError`, `trace_ids(span, dispatch)`. One tracer provider per
  process; a reload swaps its OTLP/HTTP exporter (`<endpoint>/v1/traces`) and sets its ratio.
- The controller implements the worker's `Observer` over the pipeline's metrics and computes the
  lag at each status change and each renewal poll.
- Logs: the format layer is filtered by the log level, the traces' layer by its own filter
  (`data_avalanche` at INFO while an endpoint is set). JSON lines carry `trace_id`, `span_id` and
  `fields.pipeline`. Text logs are in colour only on a terminal, without `NO_COLOR` (DA-OBS-6).
  `Logger::trace` at start and at each generation; `Logger::finish` at exit.
- `deploy/prometheus/alerts.yaml`: example alert rules, tested by promtool.

---

## 4. Milestones

Legend: **Deps** = milestones that must be merged first. **Exit** = the acceptance gate; nothing merges
to `main` in a milestone until its exit criteria hold on CI.

```
M0 ──► M1 ──┐
  ├──► M2 ──┼──► M4 ──┐
  └──► M3 ──┴──► M5 ──┴──► M6a ─► M6b (slice 1, single node)  ──► M7 ──┐
                                                          ├──► M8 ──────┼──► M9 ──► M11 ──► M12 (v1.0.0)
                                                          └──► M10 (continuous from M6b) ───┘
```

Parallel lanes after M0: **{M1}**, **{M2}** and **{M3}** are independent. M4 needs M2 and M3. M5 needs
M1 and M3. M6 is split in two (owner decision, 2026-10-02): **M6a**, the pipeline worker as a
library, and **M6b**, the single-node control around it and slice 1's exit.

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
  - Against PG 18 (every change) and PG 14–17 (weekly), every probe gate S-PG0…8 has a regression test.
  - A type round-trip suite covers every mapped type.
  - The slot state-machine table is exercised cell by cell.

### M6a — Pipeline worker (`data-avalanche-pipeline`)

Plan: `M6a-pipeline-worker.md` (approved 2026-10-03). The worker as a library (§3.11); everything
that needs the control plane is M6b's.

- **Scope:**
  - `PipelineWorker` task graph (Data-Flow concurrency model), with bounded backpressure
    (`buffer_capacity_rows`).
  - Phase 0: state load, recorded `sink_type` check, table resolution, source and sink preflight,
    D-V1.4 Iceberg refusal, the per-table snapshot decision (`snapshots.mode`).
  - Snapshot orchestration inside the stream session: stream-first chunk loop; each chunk
    reconciled against the stream by what its read saw (DA-SNAP-9, S-T); the cursor persisted
    with the chunk's flush; `parallelism`; per-table timeout (`SNAPSHOT_TIMEOUT`, V-118 ≥ 10 s,
    S-U); dump modes; a rescan before reading a table that drifted while down (D-M6a.11).
  - Streaming, the ordered sink writer and Phase 5 acknowledgment. **The checkpoint is the flush
    frontier, persisted after every table in the flush reports Committed or SkippedCovered**
    (D-IMPL-2), then acknowledged.
  - Schema apply protocol §8 (seal → flush → classify → evolve → rebuild → resume), including DDL
    idempotency on replay; channels C and D.
  - Per-table halt registry with reasons, and the schema cache (S-R).
  - Error paths: source reconnect, sink `Transient` retried with DEGRADED status (S-S), a
    `Permanent` commit halting its table, `Fenced` → stop.
  - `Command::Drain` / `Command::Flush`, and the `CommitGuard` seam the lease plugs into.
- **Carried from M5** (`M5-pg-source.md`); M6a closes each, as marked:
  - ✅ Persist the start position right after `prepare` creates the slot, and record the intent
    to create it first (`SlotDoc::intent`; M6a Tasks 3, 11).
  - ✅ Persist the checkpoint before acknowledging it to PostgreSQL (Tasks 6, 7).
  - ✅ Snapshot every table in `Prepared::added` under `StartPlan::Resume`, and persist those
    requests before the checkpoint (Task 11, D-M6a.8).
  - ✅ A quick restart's `55006`: `SourceError::Contended`, retried within `contended_window`
    (120 s) (Tasks 2, 11).
  - ✅ Open a stream only from a persisted checkpoint or the slot's own start (Tasks 7, 11).
  - ✅ A chunk's "changed under the snapshot" on an idle table runs a rescan (Task 14).
  - ✅ The reconnect backoff (Task 11).
  - ✅ The `max_wal_lag_bytes` guard: the worker ends with `WalLag` (Tasks 7, 11, D-M6a.10).
  - ✅ Keep-alives cost one position update, nothing more (Task 7).
  - ✅ `toast_strategy: re_query` calls `fetch_rows` (Task 8, D-M6a.6).
  - ✅ Each cached `TableSchema` persisted with its `source_id` (Tasks 3, 11).
  - ✅ Record the tables a `prepare` will add before calling it: a table added by config stays
    untracked until its schema is cached (Task 11, D-M6a.8).
  - ✅ The snapshot's merge window (DA-SNAP-9): each chunk's `pg_current_snapshot()`, the wait
    for writers in flight at session start, reconciliation at emission (Tasks 1, 2, 9, 10, 13;
    Snapshot_Resumability §1.1).
  - ✅ A failing stream `close` is tolerated (Task 7). `teardown` is M9's (pipeline deletion).
  - ✗ The stream watchdog: ruled out (D-M6a.9). The spec has none, and pgwire's idle feedback
    surfaces a dead connection as an error.
  - ✅ REPLICA IDENTITY FULL signals: a signal whose classification finds no Safe change neither
    flushes nor evolves (Task 7).
  - ✅ DataFusion stays out (Q-M5-24): the pipeline depends on neither `sink-delta` nor a source
    crate (§2).
- **Deps:** M1, M2, M3, M5. M4 is merged, but the worker doesn't link `sink-delta`: it is tested
  against the testkit's `MemorySink`.
- **Exit:** the worker's unit tests (a scripted `FakeSource`, the `MemorySink`, crash hooks at
  every durable step) pass hermetically; `tests/pg` and `tests/sync_commit` pass against PG 18 on
  every change, and `tests/pg` weekly on PG 14–17.

### M6b — Single-node control (**slice 1: PG → Delta, single node**)

- **Scope:**
  - Lifecycle state machine: desired state persisted; pause, stop and resume flush to a boundary
    (through M6a's `Command::Drain`).
  - Lease + renewer + `PipelineController` in single-node form, behind M6a's `CommitGuard`:
    self-only ring, one `ClusterReconciler` tick.
  - Binary: `data-avalanche run --config <path>` with CLI log overrides, and a minimal HTTP listener serving
    `/health/live`, `/health/ready` and `/metrics`. The only crate that links `sink-delta` and
    `source-pg` (§2).
  - Metrics for everything built so far (the worker's `WorkerStatus` included).
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
- **Owner's answers to the planning findings (2026-10-08):** the position guard compares a
  table's last change (DA-X-12); a stream watchdog and a bounded catalog connect (DA-SRC-16);
  the Delta libraries quiet at `info`; a stop and a resume lead out of ERROR.
- **Deps:** M4, M6a.
- **Exit:** the **slice-1 E2E suite** passes on PG 18 + RustFS and weekly on PG 14–17. See §5.3 for
  the scenario list. Measured while planning (`M6b-single-node-control.md`, "Verified"): 26
  scenarios, green twice on PostgreSQL 18 and once on 14.

### M7 — MySQL and MariaDB source (`data-avalanche-source-mysql`)

Split (Q-M7a-1, 2026-10-14): **M7a** builds the crate and its integration tests
(`M7a-mysql-source.md`), **M7b** wires it into the worker and runs the E2E parity suite. The scope
below is M7's; the exit's first two bullets are M7b's, and M7a's exit is the crate's suite on
MySQL 8.4 (GTIDs on and off) and MariaDB 11.8 (GTID and file/position) per change, MariaDB 11.4
and 10.11 weekly.


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
  - Two-tier DDL classifier routing to channel C: the tokenizer names every statement's tables
    and verbs, `sqlparser` (MySqlDialect) scopes an ALTER's columns and proves renames (D-M7a.14);
    TRUNCATE/DROP/RENAME table handling, the online-migration cut-over swap followed (Q-M7a-8/9),
    row-changing partition operations halt (D-M7a.15).
  - Preflight (config §11.2 MySQL rows).
  - Catalog channels over `information_schema`.
  - Chunk reader in the key's own collation (not `utf8mb4_bin`, which has no index, D-M7a.12),
    with locking reads (Q-M7-3, DA-SNAP-10).
  - `binlog_purge_action`.
  - Spec edits for P-4 land before this milestone.
- **M7b scope** (`M7b-mysql-pipeline.md`, Q-M7b-1…4): the source contract's checkpoint on open and
  reset (§3.6); the worker's MySQL checkpoint (`PositionDoc` with its server, §3.11); a
  pipeline halt's code in `Actual::Error` and its log line (§3.7); the file/position dump's server
  check and an error for no identity; the binary's wiring by `source.type`; the slice-1 E2E suite
  on either source, with MySQL scenarios (a corrupted payload and a failover through a test-only
  wire proxy, a purged checkpoint under both actions, the types in DuckDB) and its CI jobs (§5.3,
  §5.4); M7a's carry items (the identity error, the file/position resume IT, a halted table's
  rows, `re_snapshot` at start through the worker).
- **Deps:** M6b (M7a); M7a (M7b).
- **Exit:**
  - Slice-1 E2E parity suite on MySQL 8.4 and MariaDB 11.8 (every change) and MariaDB 10.11 / 11.4
    (weekly), across all three position modes.
  - Payload-check fault injection (corrupted payload) halts with no silent loss.

### M8 — Cluster (`data-avalanche-cluster`)

M8 is split in two (Q-M8-3, 2026-10-20). **M8a** (`M8a-membership.md`): the gossip (foca 2.0 after
the probe gate, Q-M8-1; packets MAC'd with a key from the anchor, Q-M8-2), NodeMeta and phases, the
ring and its flap rules, the hand-off and takeover caps, the leave, discovery, M6b's carried
DA-CLUS-14 and DA-OPS-7, and the multi-node E2E suite (three host processes, Q-M8-4). **M8b**
(`M8b-config-generations.md`): the config-generation coordinator at run time, R fields applied in
place (Q-M8b-2), a removed pipeline cleaned up by its owner (Q-M8b-3), the reload and status
endpoints (Q-M8b-1), and the reload E2E. The anchor validation already runs at M6b's startup.

- **Scope:**
  - **Probe gate first** (done 2026-10-20, `prototypes/cluster-capability`): `memberlist` 0.8.5 failed
    (C13); `foca` 2.0 passed F1–F8.
  - NodeMeta, phases and incarnation.
  - Ring derived from membership (xxh3, `tokens`), with flap rules (D-11).
  - `ClusterReconciler`; `PipelineController` takeover-evaluation and planned drain with lease release.
  - `max_concurrent_handoffs` / `max_concurrent_takeovers` with jitter.
  - Config-generation coordinator: publish via CAS, gossip nudge plus poll, cold-start adoption (D-3).
  - Anchor validation (V-062 / V-076 / V-094).
  - Discovery: `static` and `dns` with periodic re-resolve.
- **Deps:** M2, M6b.
- **Exit:** a multi-node E2E suite with 3 in-process nodes plus 3 node processes on one host (Q-M8-4;
  the docker-compose 3-node run moved to M11a's k3s smoke deploy). It covers:
  - kill → takeover within the D-6 window, with no double commit (verified from Delta `txn` versions);
  - graceful leave → immediate acquire;
  - a GC-pause simulation (a zombie is fenced);
  - partition (gossip split with storage reachable);
  - rolling restart with bounded handoffs;
  - a reload that propagates to every node.

### M9 — Management API (`data-avalanche-api`)

Split in two (Q-M9-A): **M9a** (`M9a-management-api.md`) is the API below without the snapshot
jobs; **M9b** (`M9b-snapshot-jobs.md`) is the snapshot jobs (the worker's chunk loop for requested
tables, tables outside the config, jobs persisted and resumed, the re-snapshot that clears a
persisted halt). As built in M9b: a job starts as a restart under its id (Q-M9b-1); it upserts,
with no truncate (Q-M9b-2); a table outside the config is copied, not streamed (Q-M9b-3); a job is
asked only of a running pipeline whose lease a node holds (Q-M9b-4).

- **Scope:**
  - axum server; TLS (`globals.api.tls`).
  - Auth: none / basic / jwt (JWKS) / oauth2 (OIDC discovery), with `role_mapping`.
  - RBAC matrix (API_RBAC §2).
  - Every `/v1` endpoint in System-Overview's table, including pipelines list/status with halted-table
    reasons, snapshot jobs (on-demand chunk loop; jobs persisted under `snapshots/jobs/`, listed via
    `GET /v1/pipelines/{id}/snapshots` after a restart or failover, running jobs resumed by the owner —
    Q-M9-1), lifecycle actions with `409`, config validate, config reload and status (these two
    exist from M8b, unauthenticated: M9 puts them behind auth and RBAC), cluster status, leave and
    shutdown.
  - OpenAPI generated with `utoipa`, served at `/v1/openapi.json`.
- **Deps:** M6b (M8 for reload propagation and cluster endpoints).
- **Exit:** an endpoint × role matrix test (401 vs 403); lifecycle transitions and `409`s; the OpenAPI
  document is snapshot-tested, read back by utoipa, and checked for every route's role, security
  scheme and common answers (as built in M9a; validation against the OAS 3.1 schema is DA-API-18).

### M10 — Observability completion (`data-avalanche-telemetry`)

- **Scope:**
  - The full metric catalogue with the cardinality policy.
  - OTLP tracing: spans per flush/commit, snapshot chunk and DDL apply; sampling; `trace_id` / `span_id`
    in JSON logs.
  - `globals.log` level/format, overridden by CLI.
  - Example Prometheus alert rules (lag > 60s for 5 min, lease degraded, halted tables, WAL lag).
- **Deps:** M6b; runs continuously alongside M7–M9.
- **Exit:** a metric catalogue test asserts every mandatory series exists with the right labels; a
  tracing smoke test runs against an OTLP collector container.

Planned in `M10-observability.md` (9 tasks, Q-M10-4). As planned: OTLP over HTTP with protobuf
(Q-M10-1); `table` on the snapshot series only (Q-M10-2); the lag is the age of the oldest change
read but not durable (Q-M10-3); MySQL's binlog lag through `RetainedLog::behind`, and no
`wal_lag_bytes` on MySQL (D-M10.4); the traces' exporter swapped on reload, behind one provider
(D-M10.9); the alert rules in `deploy/prometheus/` with promtool in CI (D-M10.15); a Jaeger
container for the E2E smoke test (D-M10.16). `globals.log` and its CLI overrides landed in M6b and
M8b; M10 adds colour only on a terminal (DA-OBS-6).

### M11 — Packaging and documentation

Split in two (Q-M11-A), each its own plan, branch and PR: M11a packaging, then M11b documentation.

#### M11a — Packaging

- **Scope:**
  - Multi-stage Dockerfile: the build on `rust:1.98.1-trixie`, the runtime
    `gcr.io/distroless/cc-debian13:nonroot` (Q-M11-3), linux/amd64 and linux/arm64.
  - Helm chart: StatefulSet (D-1), headless Service, `node_name` from the Downward API, ConfigMap,
    probes, PDB, resource defaults.
  - Release workflow: on a `v*` tag, the image and the chart to GHCR with an SBOM and provenance,
    a GitHub release (Q-M11-1).
- **Deps:** M9 (the probes, the API), M10 (`/metrics`).
- **Exit:**
  - `helm lint` and `helm template` pass; kubeconform finds the rendered objects valid.
  - A k3s smoke deploy (test-infra's `k3s` service, Q-M11-4) of 3 replicas runs one pipeline end
    to end, and a pod deleted hands its pipelines to the others (the 3-node container run moved
    from M8, Q-M8-4).
  - The image builds on both architectures; the release workflow passes actionlint.

Planned in `M11a-packaging.md` (4 tasks). As planned: debug info stripped, symbols kept
(D-M11a.1); the config a map or the file's text, the ports and the probes' scheme following it
(D-M11a.3); no restart on a changed config, the reload named instead (D-M11a.4); the pods
read-only as 65532, a grace above the drain (D-M11a.5); k3s in the compose file, the pods reaching
the source and the store through `hostAliases` (D-M11a.6); the release refusing a tag the
workspace's and the chart's versions don't match (D-M11a.8).

#### M11b — Documentation

- **Scope:**
  - **An Astro Starlight site** (Q-M11b-1, for the MkDocs this named first), published to GitHub
    Pages from `main` by `docs.yml` (Q-M11-2, Q-M11b-4):
    - architecture and internal flows (from the specs, with diagrams);
    - REST API reference rendered from the OpenAPI document;
    - configuration reference generated by `xtask` from the config model, plus detailed examples for
      each source type (PG, MySQL, MariaDB), each position mode, each cloud and single-node vs cluster;
    - the deployment: the image and the chart as M11a built them;
    - operator runbooks: IAM/privilege prerequisites (G-37), halted table, slot or binlog loss and
      re-snapshot, lease/failover, config reload and the "restart ≠ reload" rule, upgrade.
- **Deps:** M11a.
- **Exit:**
  - The site builds with its link validation (a missing page or anchor fails it).
  - Every config example in the docs parses and validates (tested by `xtask`).

### M12 — Hardening and v1 acceptance

- **Scope:**
  - Load tests measuring the D-V1.3 targets, in steady state and across a failover.
  - Long soak (24h) with churn.
  - Chaos: storage outage shorter and longer than the lease, source restart, network partition.
  - The real-cloud matrix: S3, GCS, Azure × PG and MySQL.
  - The weekly DB version matrix green for 4 consecutive runs (Q-M5-23).
  - A dependency audit: `cargo audit` clean, or every remaining advisory triaged and recorded (the CI
    step is non-blocking, so this is where the audit is enforced).
  - Compile time and the dependency tree (Q-M5-24), checked against a measurement:
    - `cargo build --timings`, to see what dominates: DataFusion, `aws-lc-sys` (a C build, through
      `deltalake-aws`'s `aws-config`) or linking;
    - the dev profile: dependencies already build without debug info; try `debug =
      "line-tables-only"` for workspace crates, and weigh `opt-level = 1` for dependencies (faster
      DataFusion tests, slower full builds);
    - CI: one build shared between the `check` and `integration` jobs (one cache key, or `cargo
      nextest archive`). This can land earlier, once PR CI time hurts;
    - upstream: what the delta-rs release current then offers, or a request for it.
      `deltalake-core` 1.0 takes DataFusion's default features, so we can't turn off its optional
      parts (the bzip2/xz/gzip codecs). The AWS SDK also comes in when S3 commits need only a
      conditional PUT;
    - the M6a dependency rule (§2) still holds.
  - A rate limit on `POST /v1/config/validate` (DA-API-20, Oleksii's choice 2026-10-24): validate
    of a posted body answers yes or no about the node's `${VAR}`s one request at a time, so its
    rate bounds an online guess. M9a logs every validate with its caller.
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
| Integration (containers) | `crates/*/tests/*.rs`, gated on `DATA_AVALANCHE_IT=1`, using `test-infra/compose.yaml` | every change (newest versions), weekly (matrix) |
| E2E scenarios | `crates/data-avalanche/tests/e2e_*.rs` — real binary against containers | every change (slice suites), weekly (full) |
| Real-cloud | `cloud.yml`, gated on `DATA_AVALANCHE_CLOUD=s3\|azure` + secrets (GCS already runs in `ci.yml`, P-10) | scheduled weekly + manual dispatch + before release |
| Load / soak / chaos | `xtask bench`, `xtask soak` | M12, then before each release |

Conventions:
- Integration tests are skipped unless `DATA_AVALANCHE_IT` is set and non-empty. This is the `SRC_PROBE`
  lesson: an empty value must not enable them.
- Each test creates uniquely named resources (schema, slot, bucket prefix) so they can run in parallel.

### 5.2 Test infrastructure (`test-infra/compose.yaml`)

Default profile (M0): `postgres-18`, `mysql-84`, `mariadb-118`, `rustfs`, `azurite`; M10 adds
`jaeger` (`jaegertracing/jaeger:2.21.0`, OTLP/HTTP on 54318, the query API on 56686), the E2E
tracing scenario's collector. M11a adds `k3s` (`rancher/k3s:v1.37.1-k3s1`, profile `k3s`,
privileged; its API on 56443, the chart's NodePort 30080 on 58080), which `test-infra/k3s.sh`
starts with the image imported. GCS has no
emulator: `fake-gcs-server` 1.56.1 rejects `object_store`'s PUT (`400 invalid uploadType`) and ignores
preconditions (re-checked 2026-09-26 with `object_store` 0.14.2: still `400 invalid uploadType`; upstream
fake-gcs issue #994 still open), so GCS tests use a real bucket (P-10; `DATA_AVALANCHE_GCS_BUCKET` + `GOOGLE_SERVICE_ACCOUNT_PATH`,
opt-out `DATA_AVALANCHE_SKIP_GCS=1`).
Profile `matrix` adds `postgres-14…17`, `mariadb-1011` and `mariadb-114`; profile `init` holds the
one-shot bucket/container creators. M5 adds `postgres-tls` to the default profile (port 55419: TLS
required on every TCP connection; a throwaway CA, server certificate and client certificate made at
each start, the CA and client pair written to the git-ignored `test-infra/.tls/`). M7a adds
`mysql-84-nogtid` (port 53385: GTID **off** for file/position tests, and no TLS for `ssl_mode:
prefer`'s fallback) and `mysql-tls` (port 53386: TLS required, throwaway certificates in the
git-ignored `test-infra/.tls/mysql/`). The MySQL source's tests pick a target with
`DATA_AVALANCHE_MYSQL`: `mysql-84` (default), `mysql-84-nogtid`, `mariadb-118`,
`mariadb-118-binlog` (`mariadb-118` read with `replication_type: binlog`), and `mariadb-114`,
`mariadb-1011` weekly.
Every service uses a fixed, non-default host port listed in `test-infra/README.md`; Azurite keeps
10000 because `object_store`'s emulator mode targets `127.0.0.1:10000`. Source servers are configured
as the preconditions require: PG `wal_level=logical`; MySQL/MariaDB ROW / FULL image / FULL metadata /
GTID on (MariaDB also `gtid_strict_mode=1`).

Pinned images (tags checked against their registries on 2026-09-26): `postgres:14…18`, `mysql:8.4`,
`mariadb:10.11/11.4/11.8`, `rustfs/rustfs:1.0.0`, `mcr.microsoft.com/azure-storage/azurite:3.37.0`;
init tools `amazon/aws-cli:2.37.4`, `mcr.microsoft.com/azure-cli:2.90.0-azurelinux4.0`. Conditional
create/update measured 2026-09-26: **RustFS PASS, Azurite PASS, real GCS PASS** (via `object_store`),
fake-gcs FAIL (risk R-3).

### 5.3 Slice-1 E2E scenarios (M6b exit, then reused as the parity suite in M7)

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
10. Guards: `max_wal_lag_bytes` halt; `max_transaction_buffer_bytes` halt; slot WAL-gone → re-snapshot;
    a frozen source reconnected to (M6b, DA-SRC-16).
11. MySQL (M7b): a corrupted compressed transaction halts `BINLOG_PAYLOAD_INVALID` and loses
    nothing; a purged checkpoint re-snapshots under `re_snapshot` and halts under `error`; a
    failover in file/position mode halts `BINLOG_SOURCE_CHANGED`; every MySQL type family reads
    back in DuckDB.

From M7b (Q-M7b-1) one suite runs on either source: `DATA_AVALANCHE_E2E_SOURCE=postgres|mysql`, and
`DATA_AVALANCHE_MYSQL` for the target. The shared scenarios (1–4, 7–9, and from 10 the buffer
guard and the frozen source) use dialect helpers. A scenario of one source skips itself on the
other (PostgreSQL: 5, 6, the WAL guard and a lost slot; MySQL: 11).

### 5.4 CI (`.github/workflows/`)

- `ci.yml` (pull requests only; each push to a PR's head branch re-runs it): fmt; clippy `-D warnings`; unit + property tests; containers (default profile) +
  integration, including real GCS; slice E2E; docs build (from M11); at the end of the check job, a
  **non-blocking** `cargo audit` (RustSec) step: an advisory fails the step and adds a warning
  annotation, but never fails the job. Requires secret
  `GCP_SA_KEY` (service-account JSON, `roles/storage.objectAdmin` on the bucket) and variable
  `DATA_AVALANCHE_GCS_BUCKET`, documented in `test-infra/README.md` and in the workflow comments.
  From M7a, a step runs the MySQL source's tests on `mysql-84-nogtid`, `mariadb-118` and
  `mariadb-118-binlog` (the workspace run covers `mysql-84`). From M7b (Q-M7b-2), an `e2e-mysql`
  job beside the integration job runs the E2E suite on `mysql-84`, `mysql-84-nogtid` and
  `mariadb-118`, one matrix entry each. From M10, the check job runs the alert rules' promtool
  test (`prom/prometheus:v3.15.0`), and every job that runs the E2E suite starts `jaeger`. From
  M11a, the check job lints and renders the Helm chart, validates it with kubeconform and runs
  actionlint; a `k3s` job builds the amd64 image and runs the smoke deploy.
- `weekly.yml` (weekly + manual; from M5, Q-M5-23): full DB matrix (M5: the PostgreSQL source suite on
  PostgreSQL 14–17, one job each, through `DATA_AVALANCHE_PG_PORT`; M7a: the MySQL source suite on
  MariaDB 11.4 and 10.11, through `DATA_AVALANCHE_MYSQL`; M7b: the E2E suite on
  `mariadb-118-binlog`, `mariadb-114` and `mariadb-1011`), full E2E, longer property-test case
  counts; from M11a, the arm64 image.
- `release.yml` (a `v*` tag, or by hand without pushing; M11a): the image for amd64 and arm64 on
  runners of their own architecture with an SBOM and provenance, the chart, a GitHub release.
- `cloud.yml` (weekly + manual): T1–T10 and the Delta P8 re-run on real S3/GCS/Azure, plus one E2E per
  cloud. Credentials are repository secrets injected as env vars; nothing is printed.

---

## 6. Risk register

| # | Risk | Likelihood / impact | Mitigation (where) |
|---|---|---|---|
| R-1 | `memberlist` 0.8.5 has never been probed (TLS, metadata size, Lifeguard, runtime-agnostic API) | ~~medium / high~~ realized / contained | M8 opens with a probe gate; `foca` 2.0 as fallback; everything above membership consumes an owner function, so the swap is contained. **Realized 2026-10-20**: memberlist failed the gate (reaping corrupts its member table, C13; UDP left unauthenticated by its TLS); M8a builds on foca 2.0 (Q-M8-1). Lost: Lifeguard (SWIM accuracy is a latency concern, Clustering §8.4) |
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

| # | Spec | Change | From | Before |
|---|---|---|---|---|
| S-R | `Data-Flow.md` §State Layout | Add `schemas/<schema>.<table>.json` (the cached `TableSchema` as of the checkpoint, with its source id) and `halts.json` (the table halt registry: table, code, detail, `at`); `replication/slot.json`'s `confirmed_flush_lsn` `0/0` is the intent to create the slot, written before a managed first start | M5 carried items, D-M6a.1 | M6a |
| S-S | `Data-Flow.md` §Sink Commit Failure | `Transient` retried with backoff 1 s → 60 s and jitter, without limit, DEGRADED while retrying, the full buffer stops reading the source; `Permanent` halts the table (`SINK_REJECTED`); `NoTable` and any other error stop the pipeline | Q-M6a-5 | M6a |
| S-T | `Data-Flow.md` Phase 1 step 2 and its consistency paragraph; Phase 4 §Position guard | The lw/hw watermark reconciliation becomes the one M6a builds: wait for the writers in flight at session start; read each chunk in a short repeatable-read transaction that records what it sees; at emission drop rows (or the chunk, on a TRUNCATE) that an unseen stream transaction wrote; flush the stream's buffered changes, then commit the chunk as a `snapshot` set; persist the cursor with that flush. The guard never skips a snapshot set | DA-SNAP-9, Q-M6a-2, Q-M6a-6 | M6a |
| S-U | `Configuration_Specification.md` §9.1 `timeouts.snapshot`, §13.3, §15 | New rule **V-118**: `timeouts.snapshot` is at least `10s` (error, pipeline) | Q-M6a-7 | M6a |

**S-R…S-U: approved and applied on 2026-10-03** (spec §15 item 24 records S-U). The M6a plan
implements them.

`Type_Mapping.md` (research) still names `mysql-binlog-connector-rust` as the MySQL decoder, but
`mysql_async` replaced it. That research-doc fix is applied (2026-09-26).

---

## 8. v1.0.0 acceptance checklist

- [ ] All milestone exit criteria met; CI, weekly and cloud workflows green (weekly 4 in a row).
- [ ] D-V1.3 measured: steady-state p99 ≤ 60s and failover recovery ≤ 2 min at default config (M12
      report).
- [ ] No data loss or duplicate keys in the crash/replay/chaos suites (source vs Delta comparison).
- [ ] Real-cloud E2E on S3, GCS and Azure.
- [ ] Container image and Helm chart published; k3s smoke deploy green.
- [ ] Documentation site builds strict; runbooks reviewed.
- [ ] Compile time and dependency tree reviewed against a `cargo build --timings` measurement (M12,
      Q-M5-24).
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
| D-IMPL-13 | M6a pipeline-worker rulings **D-M6a.1…D-M6a.11** (listed in `M6a-pipeline-worker.md` §Decisions and rulings, each pinned by a test). Among them: a halt's `at` is the accumulator checkpoint, persisted before the next checkpoint, and a persisted halt keeps the table halted at every start until M9's re-snapshot; a failed channel-C catalog read halts the table `UNSAFE_DDL`; `Permanent` from `evolve` halts `SINK_REJECTED`; state-store `Transient`/`Contention` retry like a sink `Transient`; channel D's dropped and halted tables also wait for the stream to catch up; a chunk read under a schema the table has since left is read again, and its cursor rides the chunk's own flush; after a gap a snapshot in progress starts over; a mid-run `PositionUnreachable` resets the position and reruns Phase 0; no stream watchdog; the WAL-lag guard ends the worker; a table that drifted while down is rescanned before its snapshot reads | The specs are silent on these; each keeps the checkpoint, the halt or the snapshot exact across a crash | M6a — **approved 2026-10-03** |
| D-IMPL-14 | M6b single-node-control rulings **Q-M6b-1…13 and D-M6b.1…13** (listed in `M6b-single-node-control.md` §Decisions and rulings, each pinned by a test). Among them: the source stays warm on pause; the controller polls `pipeline-state.json` and classifies worker errors (`Retry`, `Lease`, `Error`); `evolve` is fenced; a stale Delta handle is detected by its commit file's e-tag; a MERGE over `merge_memory_limit` halts its table (V-119); the guard compares a table's last change; the stream watchdog (reverses D-M6a.9) | The specs are silent on most; where a ruling touched a spec row, the row was patched with approval (V-119, the metric catalogue, Data-Flow Phase 4, config §6.4, §9.2, §9.2.1) | M6b — **approved 2026-10-08** |
| D-IMPL-15 | M7a MySQL-source rulings **D-M7a.1…23** (listed in `M7a-mysql-source.md` §Decisions and rulings, each pinned by a test). Among them: a checkpoint carries its source server (§3.6); the replica id derived from BLAKE3(`id`); `verify-ca` fails closed as `verify-full` (DA-SRC-22); one decoder for binlog and chunk values, correcting `mysql_common`'s signed `MEDIUMINT` and `YEAR`; chunk keys in their own collation with the expanded keyset predicate; the tokenizer names every statement's tables, `sqlparser` scopes an ALTER; the stream decodes by the schema as of its position and halts a table it can't read; fail-closed event types, two-phase XA with tracked rows halts; the purge action is one rule at `prepare` and on the dump's refusal; the dump watched by its connection id | The specs are silent on these; where a ruling touched a spec row, the row was patched with approval (`Schema_Change…` §7.2, config §9.2.2) | M7a — **approved 2026-10-14** |
| D-IMPL-16 | M7b MySQL-pipeline rulings **D-M7b.1…16** (listed in `M7b-mysql-pipeline.md` §Decisions and rulings, each pinned by a test). Among them: `open_stream` takes a `Checkpoint` and `reset_position` returns one (§3.6); `SourceError::halt_code` reads a halt's code from its message; the worker's checkpoint goes through one store, slot or position document, and records the server the last start read; a file/position dump checks its server on its own connection; no identity is an error; the binary picks the source by `source.type`; the E2E harness's per-test MySQL database and user, and its dialect helpers; the wire proxy's fault (one compressed byte flipped, the CRC32 recomputed) | The specs are silent on these; each keeps a MySQL checkpoint tied to its server across reconnects and lost positions | M7b — **approved 2026-10-17** |
| D-IMPL-17 | M8a membership rulings **D-M8a.1…22** (listed in `M8a-membership.md` §Decisions and rulings, each pinned by a test). Among them: foca's identity keyed by address, members grouped by name at the highest `(incarnation, rev)`; the packet MAC's key derived from the anchor; foca's settings from config §6.5.1; the ring's xxh3 points; placement over the Active members, a `Draining` node off the ring at once, additions after `rebalance_stabilization`; the hand-off permit; a controller that ended unasked started again (DA-OPS-7); a drop is a crash; the leave; the fence's epoch as the claim's floor, once (DA-CLUS-14); discovery while alone, the advertised address; per-node values through `${VAR}` (D-3); a busy slot's stale reader ended by the lease holder (DA-SRC-28) | The specs are silent on these, or (memberlist's keys, gossip TLS) changed by the M8a config patch | M8a — **approved 2026-10-21** |
| D-IMPL-18 | M8b config-generation rulings **D-M8b.1…12** (listed in `M8b-config-generations.md` §Decisions and rulings, each pinned by a test). Among them: `SourceConnector::reconfigure` takes the reloaded pipeline as `&dyn Any`; the worker's `Command::Reconfigure`; a controller's `Update` (restart under the same lease, `ERROR` cleared, the desired state kept; a removal's teardown only after a full drain under the lease); the reconciler's `Reload`, the caps resized without revoking a permit, a pipeline added back after its removal; the poll and the gossip nudge; a generation a node can't apply logged once and retried; the reload's check, CAS and retry, the same text published again when it resolves differently; the endpoints' answers; the log changed at run time; DA-OPS-8's warning; the coordinator stopped before the leave | The specs are silent on these | M8b — **approved 2026-10-22** |
| D-IMPL-19 | M9a management-API rulings **D-M9a.1…10** (listed in `M9a-management-api.md` §Decisions and rulings, each pinned by a test). Among them: the API crate owns the listener, TLS behind axum's `Listener` with a bounded handshake per connection; routing through storage, a status trusted only under the live lease's epoch and holder; the same state answers 200 unchanged, only a pause of a stopped pipeline is 409; restart and flush are requests by id, baselined when the owner's term starts; a restart runs a pipeline in `ERROR` again (desired `RUNNING`, confirmed 2026-10-23); JWKS keys cached and fetched again for an unknown `kid` at most every 30 s, HS* refused, a 503 when the keys can't be fetched; validate never returns the resolved config; a leave stays up; one route table for the router, the role gate and the OpenAPI document | The specs leave these open, or (restart) say `RUNNING` without naming the desired state | M9a — **approved 2026-10-23** |
| D-IMPL-20 | M9b snapshot-job rulings **D-M9b.1…11** (listed in `M9b-snapshot-jobs.md` §Decisions and rulings, each pinned by a test). Among them: the job id `<pipeline_id>~<UUID v7>`; a status per job table, the job settled from them; a job asked as a restart under its id, the unfinished jobs listed by the owner at its term's start and at a restart; `409` unless running and held, or while an unfinished job holds a table; a queued job table snapshotted from its first row whatever its mode, a halted one rebased, baselined and its halt cleared; a job upserts; a table outside the config described by name and copied, never streamed, cached nowhere; the owner follows each table from its snapshot document and the halts; `describe_tables`; MySQL's channel C takes a column hint as a catalog baseline (DA-SCHEMA-10) | The specs say what a job does, not how it reaches the worker, which pipelines take one, or what a table outside the config is | M9b — **approved 2026-10-27** |
| D-IMPL-21 | M10 observability rulings **D-M10.1…16** (listed in `M10-observability.md` §Decisions and rulings, each pinned by a test). Among them: the worker's events through an `Observer`, its gauges through `WorkerStatus`; every failed commit attempt counted, a covered commit not; the lag's start in the status and its age computed by the controller, clamped at 0; MySQL's binlog lag the age of the last transaction read, `0` at the head; `snapshot_state` running or waiting; soft drops counted at the start and after each evolve; `sink_type` on the commit series only; one tracer provider with its exporter swapped on reload; a bad endpoint a run-time warning; the four spans and their fields; the traces' own filter; the JSON correlation through a weakly held dispatch; colour only on a terminal; the example alert rules; Jaeger as the E2E collector | The specs name the series, the spans and the correlation, not where each value comes from, how a reload reaches the exporter, or which collector proves it | M10 — **approved 2026-10-28** |
| D-IMPL-22 | M11a packaging rulings **D-M11a.1…9** (listed in `M11a-packaging.md` §Decisions and rulings, each pinned by a test or a measurement). Among them: the build on the toolchain file's Rust and Debian 13, the runtime distroless cc as 65532, debug info stripped; the pods named for the ring, their environment for the config's `${VAR}`s; the config a map or text, the ports and the probes' scheme from it; no restart on a changed config; read-only pods, a grace above the drain; k3s in the compose file, `hostAliases` for the compose services; the release's version check, per-architecture builds, SBOM and provenance | The roadmap names the image, the chart and the release, not their base images, how a pod's identity reaches the config, or how the smoke deploy reaches the test infrastructure | M11a — **approved 2026-10-29** |
| D-IMPL-23 | M11b documentation rulings **D-M11b.1…11** (listed in `M11b-docs.md` §Decisions and rulings, each pinned by a test or a measurement). Among them: the site's exact pins and its base `/data-avalanche`; every link absolute, the validator's defaults kept; the config's keys generated from the raw model and their docs tied to it by tests, a default a value only where the resolved config proves it; a rule placed by its "Path / Scope" text; the generated pages plain Markdown, the API page sorted against the workspace's `serde_json` features; `docs.yml` deploying from `main`; the chart's `minReadySeconds`; the hand-written pages one task | The roadmap names the site, its references and its runbooks, not the tool's version, how the reference is tied to the model, or how the pages are checked | M11b — **approved 2026-10-29** |

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
| Q-M4-2 (2026-09-30) | delta-rs can't write `Date32` `i32::MIN` (PG `-infinity` after Q-M5-1), and readers can't show years beyond 9999 | **The Delta sink clamps `date` / `timestamp` / `timestamp_ntz` cells to the Delta range** (0001-01-01 … 9999-12-31 23:59:59.999999) in Hop 3b; PG `±infinity` lands on the bounds. Revises Q-M5-1 for Delta; a key cell halts instead (Q-M5-19) | `Type_Mapping.md` §Date/time edge values; `M4-delta-sink.md` |
| Q-M4-3 (2026-09-30) | Truncate as one commit or two? | **As the spec says: a DELETE commit, then the MERGE** | `M4-delta-sink.md` D-M4.8 |
| Q-M4-4 (2026-09-30) | Where do the M4 integration tests run? | **Local and RustFS (the exit), plus Azurite, the DuckDB cross-read in CI, and real GCS in PR CI** | `M4-delta-sink.md` Tasks 10–13 |
| Q-M4-5 (2026-09-30) | delta-kernel decodes a table path twice, so `%XX` escapes break tables named with `%`, `/`, a control character, `.`/`..` | **Table roots escape with `~XX`** (`escape_table_segment`); state keys keep `%XX` | §3.5; `M4-delta-sink.md` Task 1; `M2-storage-and-state.md` D-M2.4 |
| Q-M4-6 (2026-09-30) | DuckDB can't open a Delta table under a non-ASCII path | **Keep non-ASCII names verbatim**; the operator docs list the limit (R-12) | `M4-delta-sink.md` |
| Q-M5-2 (2026-10-01) | Where does the source trait live? | **In `data-avalanche-core`**, next to `Sink` (§3.6) | `M5-pg-source.md` |
| Q-M5-3 (2026-10-01) | Binary tuples? | **Text only**; a binary datum is a protocol error | `M5-pg-source.md`; §4 M5 |
| Q-M5-4 (2026-10-01) | `pgwire-replication` takes TLS files; config allows inline PEM | **An inline PEM becomes a private temporary file** (0600 in a 0700 directory, removed on drop) | `M5-pg-source.md` |
| Q-M5-5 (2026-10-01) | Partitioned tables | **The leaves replicate as tables**; a partitioned parent is skipped with a warning; `publish_via_partition_root = false`, and a publication the pipeline can't change that publishes via the root while a partition is tracked is refused | `M5-pg-source.md` D-M5.14 |
| Q-M5-6 (2026-10-01) | Who builds the `re_query` keyed re-read? | **M5 adds `fetch_rows`**; M6 calls it | §3.6 |
| Q-M5-7 (2026-10-01) | Where do PostgreSQL 14–17 run? | **`nightly.yml`**, one job per version (weekly as `weekly.yml` since Q-M5-23) | §5.4 |
| Q-M5-8 (2026-10-01) | A bare `numeric` under `numeric_overflow_action: error` | **`decimal(38,18)`, halting per value** (`NUMERIC_OVERFLOW`) | `Type_Mapping.md` §PostgreSQL-specific notes |
| Q-M5-9 (2026-10-01) | The `varbit` value layout | **PostgreSQL's binary form**: a 4-byte big-endian bit count, then the bits packed MSB-first | `Type_Mapping.md` |
| Q-M5-10 (2026-10-01) | `sslmode=require` with `ssl_ca` set: check the chain, as libpq does? | **No**: `require` encrypts without checking the chain on both connections; only `verify-ca` / `verify-full` check it | `M5-pg-source.md` D-M5.16 |
| Q-M5-11 (2026-10-01) | JSON numbers went through `f64` (rounded beyond 15–17 digits; beyond the `f64` range the table halted) | **serde_json `arbitrary_precision`, workspace-wide**: `jsonb` numbers are `numeric` and keep every digit | §2.1; `Type_Mapping.md`; `M5-pg-source.md` Deviations, Task 4 |
| Q-M5-12 (2026-10-01) | A `column_type_overrides` entry had to equal the column's mapped type (except `string` and decimals): `int → int64` halted, and `timestamp_ntz_as: timestamp` had no per-column form | **Lossless conversions at Hop 1**, one table in core (`conversion`, `ColumnValue::convert`) for every source: integer widening, an integer to a decimal with room, `float32 → float64`, `timestamp_ntz ↔ timestamp_tz`, `date → timestamp_*`, lists element by element; anything else halts (`SCHEMA_MISMATCH`) | config §9.3; `Type_Mapping.md`; `M5-pg-source.md` Task 5b |
| Q-M5-13 (2026-10-01) | PostgreSQL doesn't publish `UNLOGGED` tables; channel A selected them | **Skipped with a warning when a glob matched, halted (`TABLE_NOT_REPLICABLE`) when named** | config §11.2; `M5-pg-source.md` Deviations, Task 7 |
| Q-M5-14 (2026-10-01) | Task 8's review: a publication repaired at start doesn't cover the WAL a resume replays; a mixed-case publication streamed nothing; WAL retention measured from `confirmed_flush`; managed pipelines sharing a publication; Phase 5 acknowledged before persisting | **Re-snapshot after a rewriting repair, `Prepared::added` for added tables, a quoted publication name, `restart_lsn`, V-059 an error under `managed`, persist before acknowledging** | §3.6; M6 carried items; Data-Flow Phase 5; config §13; `M5-pg-source.md` Deviations, Task 8 |
| Q-M5-15 (2026-10-02) | Task 9's review: `next` not cancel-safe; channel B blind to key and replica identity changes; tables matched by name, not OID; PostgreSQL 14's raw `"char"` bytes; `close` before the slot's release | **A cancel-safe `next`; the replica identity compared; a parted name and OID halt (`TABLE_DROPPED`); the raw byte decodes; `close` waits for the release** | §3.6; `Schema_Change…` §7; `M5-pg-source.md` Deviations, Task 9 |
| Q-M5-16 (2026-10-02) | Task 10's review: a `"C"`-pinned text key made every chunk a full scan; PostgreSQL 14's raw `"char"` bytes panicked the read; a domain over `"char"` key looped; a timed-out chunk kept the catalog connection busy | **A text key in its own deterministic collation (`"C"` and a warning otherwise); `"char"` read as its code; server-side `statement_timeout`** | Snapshot_Resumability §2; `M5-pg-source.md` D-M5.17, Deviations, Task 10 |
| Q-M5-17 (2026-10-02) | Task 11's review: channel C placed columns by the catalog's current names (a rename after the signal gave a silent wrong delta); a table set `UNLOGGED` went unnoticed; §7.2 vs the code on replica identity | **A placement that is a guess halts; a later drop keeps its `attnum`; unlogged tables halt in C and D; §7.2 amended (only the identifier halts)** | `Schema_Change…` §7.2; `M5-pg-source.md` D-M5.18, D-M5.19, Deviations, Task 11 |
| Q-M5-18 (2026-10-02) | Task 12's review: a timestamp key past the i64 microseconds clamps to the value of `'infinity'`, so distinct keys merge and a snapshot ends early | **An identifier value that clamps halts the table (`SCHEMA_MISMATCH`); `±infinity` and other columns unchanged** | `Type_Mapping.md` §Date/time edge values; `M5-pg-source.md` Q-M5-1, Deviations, Tasks 4, 9, 10, 12 |
| Q-M5-19 (2026-10-02) | Task 12's re-review: the Delta sink clamps key cells to the Delta range (Q-M4-2), so distinct keys merge (BC, 10000 AD on, `±infinity` against the bounds) | **The Delta sink halts the table on an identifier cell outside the Delta range; other columns still clamp** | `Type_Mapping.md` §Date/time edge values; `data-avalanche-sink-delta` `merge_source`; `M5-pg-source.md` Deviations |
| Q-M5-20 (2026-10-02) | The whole-branch review: a re-created table stopped replicating silently; a timed-out catalog statement kept the shared connection; one table's failure stopped a snapshot; a key changed under REPLICA IDENTITY FULL never signalled | **`TableSchema.source_id` (the OID) tells a re-created table; timeouts drop and cancel the connection, the publication change runs under server-side timeouts; those failures halt the table; every FULL Relation message signals; the watermark goes to M6 (DA-SNAP-9)** | `M5-pg-source.md` Q-M5-20, Deviations; §3 `TableSchema`; M6 "Carried from M5" |
| Q-M5-21 (2026-10-02) | The whole-branch review's second round: a table renamed away and back lost the writes in between; NULL-key rows past the first chunk were skipped; the snapshot's OID check missed a table with none of the cached columns and a dropped schema | **A tracked OID under an untracked name halts the table; a nullable identifier column halts it; the snapshot resolves the OID first (`to_regclass`); smaller fixes to timeouts and rescan** | `M5-pg-source.md` Q-M5-21, Deviations |
| Q-M5-22 (2026-10-02) | The whole-branch review's third round: PostgreSQL 18 sets `attnotnull` under `NOT NULL … NOT VALID`, so an override key on such a column skipped its NULL rows | **An unvalidated not-null constraint counts as nullable at resolve and in the snapshot; a dropped-schema test; the cache-lock wait goes to DA-SRC-17** | `M5-pg-source.md` Q-M5-22, Deviations |
| Q-M5-23 (2026-10-02) | How often does the PostgreSQL 14–17 matrix run? | **Weekly (Mondays 03:41 UTC) and on manual dispatch, as `weekly.yml`; the v1 release needs 4 weekly runs green in a row** | §2, §5.4, §8, M12 exit |
| Q-M5-24 (2026-10-02) | DataFusion (34 sub-crates) and the AWS SDK come in through `deltalake`: can the dependency tree and compile time shrink? | **Nothing to remove from our manifests: the sub-crates are transitive, and MERGE needs `deltalake`'s `datafusion` feature. From M6, only the binary links `sink-delta`. The rest is measured and tuned just before release (M12): timings, profiles, CI build sharing (earlier if CI time hurts), upstream** | M6 scope; M12 scope; §8 |
| Q-M9-1 | Snapshot-job persistence | **Persisted in the pipeline state location** (`snapshots/jobs/<job_id>.json`); listed after a restart or failover via `GET /v1/pipelines/{id}/snapshots`; a running job continues on the new owner from its chunk cursors | `Data-Flow.md` §On-Demand Snapshot + State Layout; System-Overview and API_RBAC endpoint tables |
| Q-M6a-1 (2026-10-02) | `snapshots.mode: never` and a recovery case (the position lost, an orphan adopted, a table added) | **No snapshot.** A warning names the gap: the table may miss changes from before the new position | `M6a-pipeline-worker.md` Task 5 |
| Q-M6a-2 (2026-10-02) | Writers in flight when a session starts: their commits may predate the stream's start and be invisible to a chunk (DA-SNAP-9) | **Wait per table**: `in_flight()` at session start; a table's chunks are read only once none of its writers is still in flight (re-read with backoff 0.5 s → 10 s) | §3.6; spec edit S-T; M6a Task 10 |
| Q-M6a-3 (2026-10-02) | `timeouts.snapshot` exceeded | **Halt the table `SNAPSHOT_TIMEOUT`** and mark its snapshot document `broken`; the clock is per table within one run | §3.3; M6a Task 10 |
| Q-M6a-4 (2026-10-02) | When does a channel-D rescan's `changed` schema apply? | **After the stream catches up** to the position read right after the rescan; a change the stream applied meanwhile is skipped | §3.6; M6a Task 8 |
| Q-M6a-5 (2026-10-02) | A sink commit that keeps failing `Transient` | **Retry forever**, backoff 1 s → 60 s with jitter, DEGRADED while retrying; backpressure through `buffer_capacity_rows` | spec edit S-S; M6a Task 6 |
| Q-M6a-6 (2026-10-02) | Chunk rows emitted at a frontier the table's marker covers are skipped by the guard: silent loss while the cursor advances | **A snapshot flag on the change set**: `covers` never skips it, the fence still applies, `validate` refuses deletes, truncate or a split in it; the stream's changes flush before a chunk | §3.3, §3.4; spec edit S-T; M6a Tasks 1, 10 |
| Q-M6a-7 (2026-10-03) | `timeouts.snapshot: 0` halts every snapshot at once | **A V-rule: at least 10 s** (V-118) | spec edit S-U; M6a Task 15 |
| Q-M6a-8 (2026-10-03) | A table whose schema changed while the worker was down, under a snapshot: read under the cached columns, an added column stays NULL | **Rescan before snapshotting** | D-M6a.11; M6a Task 16 |
| Q-M7-3 (2026-10-14) | DA-SNAP-10: a chunk can miss a transaction the stream already delivered (semi-sync `AFTER_SYNC`, S-MY11) | **Locking chunk reads** (`LOCK IN SHARE MODE` in a short READ COMMITTED transaction); the window rule stays | `Data-Flow.md` Phase 1; `Snapshot_Resumability…` §1.1; M7a Task 10 |
| Q-M7-4 (2026-10-14) | `_seq` when the header's `log_pos` is 0 (S-MY10f), and `_lsn` | **`_seq` = (file index, computed end offset); `_lsn` = the transaction's GTID, or its start `file:pos`** | `Data-Flow.md` Phase 2b, lineage; D-IMPL-5; M7a Tasks 6, 13 |
| Q-M7-5 (2026-10-14) | Two dumps under one replica id end each other | **`source.mysql.server_id`**, derived from `id` by default; V-120 | config §9.2.2; M7a Task 2 |
| Q-M7-6 (2026-10-14) | The MySQL heartbeat period | **`source.mysql.heartbeat`** (`60s`), V-121 | config §9.2.2; M7a Tasks 2, 13 |
| Q-M7a-1 (2026-10-14) | One milestone or two? | **Split M7a / M7b**: the crate and its ITs; then the worker, the wiring and the E2E | §4 M7 |
| Q-M7a-2 (2026-10-14) | Which charsets? | **UTF-8 family and `latin1` (cp1252)**; `binary` → bytes; any other halts the table | `Type_Mapping.md`; M7a Task 7 |
| Q-M7a-3 (2026-10-14) | `PARTIAL_JSON` updates | **Apply the diff** to the before image | `Type_Mapping.md`; M7a Task 8 |
| Q-M7a-4 (2026-10-14) | MariaDB's own types | **`UUID` → uuid, `INET4`/`INET6` → string, `VECTOR` → binary** | `Type_Mapping.md` §MariaDB types; M7a Task 7 |
| Q-M7a-5 (2026-10-14) | MariaDB `log_bin_compress` | **Decompress the events ourselves** | DA-SRC-23; M7a Task 6 |
| Q-M7a-6 (2026-10-14) | A checkpoint in another dialect | **Follow `binlog_purge_action`** | M7a Task 14 |
| Q-M7a-7 (2026-10-14) | `GEOMETRY` | **SRID (4 bytes LE) + WKB, as stored** | `Type_Mapping.md`; M7a Task 7 |
| Q-M7a-8 (2026-10-14) | An online-migration cut-over swap | **Follow it** | `Schema_Change…` §7.2; M7a Tasks 11, 13 |
| Q-M7a-9 (2026-10-14) | Which swaps are cut-overs? | **gh-ost `_<t>_gho` and pt-osc `_<t>_new` shadows only**, same database, in one statement | `Schema_Change…` §7.2; M7a Task 11 |
| Q-M7b-1 (2026-10-17) | How does the E2E suite run on MySQL? | **One suite, two sources**: `DATA_AVALANCHE_E2E_SOURCE` picks the source; shared scenarios use dialect helpers; source-only scenarios skip themselves | §5.3; M7b Task 6 |
| Q-M7b-2 (2026-10-17) | Which targets, where in CI? | **The three position modes per change, in their own job** (`mysql-84`, `mysql-84-nogtid`, `mariadb-118`); `mariadb-118-binlog`, `mariadb-114`, `mariadb-1011` weekly | §5.4; M7b Task 8 |
| Q-M7b-3 (2026-10-17) | How is a corrupted payload injected? | **A test-only TCP wire proxy** in the E2E suite (one compressed byte flipped, the CRC32 recomputed) | §5.3; M7b Task 7 |
| Q-M7b-4 (2026-10-17) | How does the status show a pipeline-scope halt? | **A structured code** in `Actual::Error`, logged as a field; no new metric | §3.7; M7b Task 3 |
| Q-M8-1 (2026-10-20) | Which gossip crate, after the probe gate? | **`foca` 2.0 with our own tokio actor** (memberlist 0.8.5 failed, C13) | §2.1, §3.7; M8a Tasks 3–4 |
| Q-M8-2 (2026-10-20) | How is the gossip secured? | **A keyed BLAKE3 MAC on every packet**, keyed from the anchor's `fingerprint_key`; no encryption; `memberlist.tls` dropped (V-054 retired) | config §6.5.1; M8a Tasks 1, 3 |
| Q-M8-3 (2026-10-20) | One plan or two? | **M8a + M8b**: membership, placement and the multi-node E2E first; config generations at run time and the reload E2E second | §4 M8 |
| Q-M8-4 (2026-10-20) | How does the E2E run several nodes? | **Three host processes**, UDP forwarders, SIGSTOP/SIGCONT, `kill -9`, SIGTERM; the compose 3-node run moves to M11 | §4 M8, M11; M8a Task 12 |
| Q-M8-5 (2026-10-21) | DA-SRC-28: a PostgreSQL takeover from a paused holder waits for `wal_sender_timeout` | **End the stale walsender** (the pipeline's own role, under the lease), in M8a | §3.6; M8a Task 13 |
| Q-M8b-1 (2026-10-22) | How is a reload triggered before M9's API? | **The endpoints now, auth in M9**: `POST /v1/config/reload` and `GET /v1/config/status`, unauthenticated until M9 | §3.12, §4 M9; M8b Task 7 |
| Q-M8b-2 (2026-10-22) | Which R-class pipeline fields are applied in the running worker? | **All of them in place** | §3.6, §3.11; M8b Tasks 1–3 |
| Q-M8b-3 (2026-10-22) | Who cleans up a removed pipeline? | **Its owner**: drain, teardown, release; an offline owner leaves an orphan (DA-OPS-9) | §3.7; M8b Task 3 |
| Q-M9-A (2026-10-23) | One M9 or two? | **M9a + M9b**: the API first; the on-demand snapshot jobs second | §4 M9 |
| Q-M9-B (2026-10-23) | How does a request on one node reach the pipeline's owner? | **Through storage**: the desired state by CAS, restart and flush as requests the owner takes at its poll, a status document the owner writes; any node answers from storage | §3.5, §3.7; M9a Tasks 1, 2, 5 |
| Q-M9-C (2026-10-23) | What do leave and shutdown do? | **Leave = the M8a leave, the process stays up** (not ready, the API answers); **shutdown = SIGTERM's path** (leave, exit 0) | §3.12; M9a Task 6 |
| Q-M9-2 (2026-10-23) | `GET /v1/snapshots/{job_id}` on any node? | **The job id embeds the pipeline id** (`<pipeline_id>~<ulid>`-like), read from storage by any node; no spec edit | M9b |
| Q-M9b-1 (2026-10-27) | How does a job reach the running worker? | **Restart the worker**: a restart request under the job's id; the new worker's Phase 0 takes the job's tables | §3.7, §3.11, §3.12; M9b Tasks 3, 5, 6 |
| Q-M9b-2 (2026-10-27) | Does a job truncate the sink table first? | **Upsert only, documented**: deleted rows stay; drop or move the Delta table first (DA-SNAP-15) | M9b D-M9b.7 |
| Q-M9b-3 (2026-10-27) | What is a table outside the config? | **A copy, not streamed**: described by name, read once like a dump, its schema cached nowhere | §3.6, §3.11; M9b Tasks 2, 4 |
| Q-M9b-4 (2026-10-27) | Which pipelines take a job? | **`409` unless running**: desired state running and the lease held, `ERROR` accepted (the job's restart runs it again) | §3.12; M9b Task 6 |
| Q-M10-1 (2026-10-28) | Which OTLP transport? | **OTLP/HTTP with protobuf only**, port 4318, `/v1/traces` appended to the endpoint; no gRPC | config §6.6; M10 Task 6 |
| Q-M10-2 (2026-10-28) | Which series take `table` under `per_table`? | **Snapshot progress and snapshot state only**; the lag is the pipeline's | System-Overview; M10 Task 1 |
| Q-M10-3 (2026-10-28) | What is `replication_lag_seconds`? | **The age of the oldest undelivered change**: now minus the commit time of the oldest transaction read but not durable; `0` when none; snapshot rows excluded | System-Overview; M10 Tasks 3, 5 |
| Q-M10-4 (2026-10-28) | One milestone or two? | **One, about nine tasks** | §4 M10 |
| Q-M11-A (2026-10-29) | One plan for M11, or two? | **Two: M11a packaging, M11b documentation** | §4 M11 |
| Q-M11-1 (2026-10-29) | Where and when are the image and the chart published? | **GHCR on a `v*` tag**, with an SBOM and provenance; PR CI only builds | §4 M11a, §5.4; M11a Task 4 |
| Q-M11-2 (2026-10-29) | Where does the documentation site live? | **GitHub Pages, from `main`** | §4 M11b |
| Q-M11-3 (2026-10-29) | Which runtime image and architectures? | **distroless `cc`, linux/amd64 and linux/arm64**, each on a runner of its own architecture | §4 M11a; M11a Tasks 1, 4 |
| Q-M11-4 (2026-10-29) | kind or k3d for the smoke deploy? | **k3s in test-infra's compose file, no cluster CLI** (Oleksii's choice while planning) | §5.2; M11a Task 3 |
| Q-M11b-1 (2026-10-29) | Which site generator? | **Starlight**, pinned, with `starlight-links-validator` (a research spike over six tools; mkdocs-material reaches end of life 2027-05-05) | P-6, §2, §4 M11b; `research/Documentation_Site_Generator.md` |
| Q-M11b-2 (2026-10-29) | Where does the configuration reference come from? | **A field table in the config crate**, tied to the raw model by tests, rendered by `xtask` with the types, V-rules and reload classes | M11b Tasks 1, 2 |
| Q-M11b-3 (2026-10-29) | Where does the API reference come from? | **Markdown rendered by `xtask`** from the OpenAPI document, the document beside it, a staleness test | M11b Task 2 |
| Q-M11b-4 (2026-10-29) | How is the site published? | **`actions/deploy-pages` from `docs.yml`**; the Pages source "GitHub Actions" | M11b Task 3 |
| Q-M11b-5 (2026-10-29) | How much of the pages does the plan carry? | **Their full text**, each page built and its examples validated in a scratch tree | M11b |
| Q-M11b-6 (2026-10-29) | How deep is the architecture section? | **An operator's depth**: six pages with diagrams, linking to the specs | M11b Task 3 |

### 9.2 Open questions (answer before the named milestone)

- Recorded for later plans: M7 — the decoder validates `gno >= 1` before `GtidSet::insert` (anonymous
  GTID events carry 0, and `insert_range` panics on 0). **Done in M7a Task 13**: an anonymous GTID event
  starts a transaction without a GTID (its xid from a session counter), and a GTID event with `gno`
  0 halts the stream (`Permanent`). The two M1 items are settled in M1 Task 1
  (2026-09-27): `MAX_LIST_DEPTH = 8` for `list<T>`, and the public-API convention (root re-exports,
  `Display`+`FromStr` round-trip incl. `FromStr for Decimal`, errors named after what failed) is in
  `data-avalanche-core`'s crate docs.

- **Q-M9-2:** `GET /v1/snapshots/{job_id}` is served by any node, but the job lives in one pipeline's
  state location. Either the job ID embeds the pipeline ID so any node can read it from storage, or
  the status endpoint moves under `/v1/pipelines/{id}/snapshots/{job_id}`. **Answered 2026-10-23**
  (§9.1): the job id embeds the pipeline id; built in M9b.
