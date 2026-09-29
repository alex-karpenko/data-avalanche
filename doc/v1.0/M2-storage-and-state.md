---
updated: 2026-09-29
domain: Architecture
tags: [v1, implementation-plan, m2, storage, state, object-store, cas, lease, cluster-anchor, config-generation]
sources: []
---

# M2 — Storage and State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `data-avalanche-storage` (bucket backends, the config §10 path rules and a
compare-and-swap object interface) and `data-avalanche-state` (every durable document: leases,
the cluster anchor, config generations, per-pipeline state), and re-prove the lease's CAS
guarantees (T1–T10) through `object_store` on every backend.

**Architecture:** Two crates on top of `object_store` 0.14.2 (P-5).

- **`data-avalanche-storage`** knows buckets and keys, not documents.
  - `open_bucket(&BucketSpec)` builds an S3 / GCS / Azure store from the cloud's credential
    chain, with bounded timeouts.
  - Pure path functions give a `Location` (bucket + key prefix).
  - `ObjectStoreVersioned` roots a `VersionedStore` at a prefix: `get` / `create` / `update` / `put`
    / `list`. It maps `object_store` errors onto four answers: `Conflict`, `Contention`,
    `Transient`, `Permanent`.
- **`data-avalanche-state`** knows documents.
  - `DocStore` reads and writes JSON documents with a schema `version`, and runs the CAS
    read-modify-write loop.
  - On top of it: `Leases`, `establish_anchor`, `ConfigGenerations` (all in `globals.state`) and
    `PipelineDocs` (one pipeline's state location).
  - The state crate never depends on the config crate. The binary joins them at startup, and
    Task 11's test shows how.

Timing — when to renew a lease, how long to observe before a takeover, the safety deadline — is
not here: the renewer is M8 (Clustering §5.3, §19). M2 provides the CAS operations it calls.

**Tech Stack:** Rust 1.98.1 (edition 2024).

| Crate | Version / features | Used for |
|---|---|---|
| `object_store` | `0.14.2`, features `aws`, `gcp`, `azure` (workspace, from M0) | Backends, conditional puts, `InMemory` for hermetic tests |
| `async-trait` | `0.1` (locks 0.1.92) | `VersionedStore` is an object-safe async trait |
| `bytes` | `1` (locks 1.12.1) | Object bodies |
| `futures-util` | `0.3`, no default features, `std` (locks 0.3.34) | `TryStreamExt` for listings |
| `blake3` | `1` (workspace, from M1) | Config snapshot hashes |
| `chrono` | `0.4` with `serde` (workspace) | Document timestamps |
| `serde` / `serde_json` / `thiserror` 2 | workspace | |
| `tokio` | workspace; dev: `net`, `io-util` for the fake endpoint | Tests |

**Spec:**
- `doc/v1.0/00-roadmap.md`: §2 (crate layout, dependency direction), §3.5 (storage CAS), §3.8
  (the landed config interface), §4 M2 scope and exit, §5 (test strategy, `cloud.yml`), R-2, R-3,
  D-IMPL-6.
- `doc/specs/Configuration_Specification.md`: §6.2 (cluster anchor, `fingerprint_key`), §10
  (storage paths), §11 (`prefix: ""` is the bucket root), §13.4 (sink-type check at start).
- `doc/specs/Data-Flow.md` §State Layout, §On-Demand Snapshot.
- `doc/research/Clustering_Architecture_Research.md`: §5.2–5.3 (lease object and protocol), §8.3
  (release), §9 (config generations), §12 (`globals.state` layout), §17.4 (T1–T10), §19.2
  (renew outcome taxonomy).
- `doc/research/Pipeline_Lifecycle_State_Machine.md` §4 (`desired_state`).
- `doc/research/Snapshot_Resumability_and_Consistency.md` §2 (cursor, collation pin).
- `prototypes/lease-conformance/README.md` (T1–T10, the tombstone, the GCS generation trap).

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`, so tests can't call
  `std::env::set_var`. `cargo clippy --workspace --all-targets -- -D warnings` and
  `cargo fmt --all --check` must pass before every commit.
- **Dependency direction** (roadmap §2): `state → storage`, and `state → core`. The storage crate
  depends on neither `core` nor `config`. The state crate depends on `config` only as a
  **dev-dependency** (Task 11's startup test).
- **Public-API conventions** (core crate docs): every public type is re-exported at the crate
  root; errors are `thiserror` types named after what failed; modules are private.
- `object_store` `0.14.2` everywhere (P-5). No native cloud SDKs, no OpenDAL.
- **No credentials in code, docs or output.** Real-cloud tests read credentials from the standard
  environment variables; diagnostics name a config key, never its value.
- **No key material in the repository.** The anchor's `fingerprint_key` is generated at run time
  and never printed (`AnchorDoc`'s `Debug` shows `***`).
- **Integration gates** (D-IMPL-6, `test-infra/README.md`): emulator tests run only when
  `DATA_AVALANCHE_IT` is non-empty; real GCS unless `DATA_AVALANCHE_SKIP_GCS`; real S3 / Azure
  only when `DATA_AVALANCHE_CLOUD` names them. `cargo test` without them stays hermetic. Every test
  that touches a shared bucket writes under `unique_prefix(..)` and deletes what it wrote.
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the conditions most likely to hurt a running cluster. Each is pinned by a test in the
task named after the arrow.

1. **A hung or unreachable storage endpoint.** A lease renew that waits minutes (object_store's
   default budget is 30 s per request and 3 minutes of retries) outlives the lease. Every call must
   fail within its bound and read as `Transient` → Task 3
   (`a_hung_endpoint_fails_within_the_timeout_bound`, `a_refused_connection_is_transient`).
2. **S3 `409 ConditionalRequestConflict`** under concurrent conditional writes. It is contention,
   not a loss and not a hard error. It must be retried, then surface as `Contention` → Task 3
   (`update_retries_s3_409_before_reporting_contention`, `http_answers_map_to_store_errors`),
   Task 6 (T9).
3. **A corrupt, hand-edited or newer-release state document.** It must be refused, never read as
   "absent" and never overwritten. Overwriting a lease or anchor is how two owners happen →
   Task 4 (`refuses_other_schema_versions`, `refuses_corrupt_documents`,
   `a_corrupt_stored_document_is_an_error_not_absent`), Task 7
   (`a_bad_stored_anchor_is_refused_without_echoing_the_key`).
4. **Database identifiers with `.`, `/`, `%`, control characters or non-ASCII text**, and names
   like `.` or `..`. Keys must stay valid and unambiguous (`a.b` + `c` vs `a` + `b.c`) → Task 2
   (`table_root_escapes_only_what_breaks_a_segment`, `escaped_roots_are_valid_store_roots`),
   Task 10 (`table_snapshot_keys`, `dotted_names_do_not_collide`).
5. **`prefix: ""` and stray slashes** (config §11: `""` is the bucket root). A doubled or leading
   `/` must not create an empty key segment → Task 2
   (`join_skips_empty_segments_and_extra_slashes`, `table_root_under_sink_prefix`,
   `colocated_state_is_marked_under_sink_prefix`, `globals_state_prefix`).

## Decisions and rulings

**Asked and answered (2026-09-28):**

| # | Question | Answer |
|---|---|---|
| Q-M2-1 | What does a config snapshot hold? | **The raw config file text.** Each node resolves it itself (its own env and files); the pointer records the text's hash. Clustering §9 and §12 said "resolved" and are corrected with this plan |
| Q-M2-2 | How much of the lease is M2? | **The lease document and its CAS operations** (acquire, renew, observe, take over, tombstone release), proven by T1–T10. The renewer state machine, the safety deadline and the controller are M6/M8 |
| Q-M2-3 | Which backends does planning verify on? | **RustFS, Azurite and real GCS now; real S3 and Azure at execution** (the `cloud.yml` exit gate) |
| Q-M2-4 | The snapshot cursor? | **Define the document now; each cursor key value is a JSON value.** M5/M7 convert it to and from typed values |

**Rulings where the specs are silent** (each is pinned by a test). **Plan approved 2026-09-28**, with these rulings (roadmap D-IMPL-9):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M2.1 | Storage has no config dependency. `BucketSpec { backend, bucket, options, timeouts }` is plain data; M6 maps `globals.cloud` / `globals.azure.account_name` onto it | Keeps the dependency direction; tests build specs without a config | Task 3 |
| D-M2.2 | `StoreError` is `Conflict` / `Contention` / `Transient` / `Permanent`, each with the key. There is no `NotFound`: `get` of a missing object is `Ok(None)`, and an `update` of a missing object is `Conflict` on every backend. S3, GCS and `InMemory` already answer that way; Azure's 404 is mapped to it. This changes roadmap §3.5's sketch; update §3.5 at exit | Clustering §19.2 treats 412 and 404 on renew the same way (lost), so callers need one answer, not two | Task 1 `update_of_a_missing_object_conflicts`, `tests/versioned_store.rs` |
| D-M2.3 | Error mapping: `AlreadyExists` → `Conflict` on create, `Contention` otherwise; `Precondition` → `Conflict`; `NotFound` on update → `Conflict`; `PermissionDenied` / `Unauthenticated` / `NotSupported` / `NotImplemented` / `InvalidPath` / other `NotFound` / `NotModified` → `Permanent`; everything else (`Generic`: timeouts, 5xx, connection errors) → `Transient` | `object_store` keeps the HTTP status private, so a 400 also lands in `Generic`. Calling that `Transient` only delays the failure until the lease deadline, which is safe | Task 1 `error_mapping`, Task 3 `http_answers_map_to_store_errors` |
| D-M2.4 | Keys are stored verbatim (`Path::parse`, no percent-encoding), so `list` returns exactly what was written. An invalid key (empty segment, `.`, `..`, control character) is `Permanent`. Database identifiers go through `escape_segment` (`%`, `/`, control characters → `%XX`; a name of `.` or `..` → `%2E`…). Snapshot file names also escape `.` inside each name | Round-trip safety for listings; ordinary names stay readable (`public.orders.json`) | Task 1 `keys_are_stored_verbatim`, Task 2, Task 10 |
| D-M2.5 | `Timeouts::COORDINATION`: 3 s per request, 1 s connect, 2 retries, 5 s retry budget, so worst case is about `retry_timeout + request` = 8 s. The default lease is 15 s | A renew must fail fast enough for the renewer to act before its deadline. The M8 renewer adds its own per-attempt deadline on top | Task 3 `a_hung_endpoint_fails_within_the_timeout_bound` (with a faster preset) |
| D-M2.6 | Documents are pretty JSON with `"version"` first and `deny_unknown_fields`. Any other schema version is `UnsupportedVersion`, any malformed body `Corrupt`; there is no migration in v1 | A newer release's document must stop an older binary, not be half-read | Task 4 |
| D-M2.7 | `DocStore::modify` retries a lost race up to `MODIFY_ATTEMPTS` = 16 times. Its closure must compute the target state from what it reads (set, not blind increment), because after `Contention` the earlier write may have landed | Only the lease holder writes per-pipeline documents, so races are rare; 16 covers 8 concurrent writers with margin | Task 4 `concurrent_modifies_all_apply`, `tests/races.rs` |
| D-M2.8 | Release writes a **tombstone**: the `LeaseDoc` with `released: true`, keeping `owner_epoch`. Acquiring over a tombstone sets `epoch + 1`. **Known limitation:** deleting a lease object by hand restarts its epoch at 1, so never delete lease objects (crate docs) | `object_store` has no conditional delete. Keeping the epoch in the tombstone keeps it monotonic across release and re-acquire | Task 5 `release_leaves_a_tombstone_and_the_epoch_continues`, `an_externally_deleted_lease_restarts_at_epoch_1` |
| D-M2.9 | `acquire` never touches a live holder: it returns `Acquired::HeldBy`, and the caller runs the takeover evaluation (§5.3) and then `take_over(observed, …)`. A lost race inside `acquire` re-reads, up to 3 attempts | The "observed unchanged for a lease duration" timing is the renewer's (M8) | Task 5 |
| D-M2.10 | The cluster anchor holds `tokens`, `lease_duration_ms`, `lock_backend` (the config string), `hash_algo` (`xxh3`, the only accepted value) and `fingerprint_key` (64 lowercase hex), plus `established_by` / `established_at`. The config crate's `check_cluster_anchor` compares a node's settings with it | Config §6.2 and Clustering §12; `hash_algo` as an enum makes a future algorithm a decode error, not a silent ring split | Task 7, Task 11 |
| D-M2.11 | Config snapshots: `config/snapshots/<NNNNNN>-<blake3 hex>.yaml`, holding the raw text (Q-M2-1). The pointer's `hash` is `blake3:<hex>` (the research doc said `sha256:`; BLAKE3 is already a dependency). The snapshot is checked against the hash on every read | Content addressing means two nodes racing to publish the same generation can't overwrite each other's snapshot; the loser's file stays unreferenced | Task 8 `racing_publishers_keep_their_own_snapshots`, `a_tampered_or_missing_snapshot_is_refused` |
| D-M2.12 | The table snapshot document records `table` verbatim and uses `start_position` (a tagged `SourcePosition`) where Data-Flow's example says `start_lsn`. MySQL has no LSN. `cursor` follows Snapshot_Resumability §2 plus `collation` (the pinned binary collation, `None` when no key column is text) | One shape for both engines. Spec edit **S-M** below (approved) | Task 10 `table_snapshot_json_shape` |
| D-M2.13 | `position.json` always records the source server identity (`{"engine":"mysql","server_uuid":…}` or `{"engine":"mariadb","server_id":…}`). Only file/position mode acts on it (M7, Q-M7-2) | Cheap to record, and it lets M7 detect a changed server in every mode if needed | Task 9 `position_json_shape` |
| D-M2.14 | `PipelineDocs::open` refuses a state location whose `pipeline_id` differs (`PipelineIdMismatch`), as well as a changed `sink_type` (`SinkTypeMismatch`, config §13.4) | Two pipelines sharing a state location would corrupt each other's positions | Task 9 |
| D-M2.15 | Real S3 and Azure tests open their bucket through `open_bucket` (the production path) and run only in `cloud.yml` (weekly and manual). `DATA_AVALANCHE_CLOUD` takes a comma-separated list | Proves the credential chains, not just the CAS | Task 6 |

**Spec edit S-M (approved and applied 2026-09-28):**

- **S-M** — `Data-Flow.md` §State Layout, the per-table snapshot example: `"start_lsn": "0/1A2B3C4"`
  → `"start_position": "pg:0/1A2B3C4"`. Add `"table": {"schema": …, "name": …}`. Show the cursor
  as `{key: [{col, val, type}], chunk_size, rows_done, collation}`, or point at
  Snapshot_Resumability §2 for it.

**Research-doc corrections made with this plan** (Claude-maintained):
`Clustering_Architecture_Research.md` §9.1/§9.2/§12 now say:
- a snapshot holds the raw config text under a content-addressed name, with a `blake3:` hash;
- the anchor carries `fingerprint_key`.

## Deviations during execution (2026-09-29)

The task code blocks below are the plan as it stood when each task ran. Where a review changed
the code afterwards, the landed code differs; `git log 7806811..` on `feat/claude/impl-v1` is the
record. In order:

- **Per-task review fixes.**
  - T3: `OpenError` never shows an option value. A known option with a bad value is reported by
    key, with the value replaced by `***`.
  - T5: after a race error or an unknown outcome on its own write, `acquire` recognizes that write
    and returns `Mine`: it compares the reloaded document with the one it tried to write. A lease
    document whose `pipeline_id` doesn't match its key is `Corrupt`. `take_over` and `release`
    document `Contention`.
  - T8: `write_snapshot` read the snapshot back after `Contention`. In the plan's code, several
    nodes creating the same snapshot at once on S3 could fail cold start instead of adopting.
    `bootstrap_or_adopt` reports `created` correctly for its own landed pointer write. Tests now
    cover same-text races. The `WriteThenLoseResponse` fake moved to `src/test_support.rs`.
  - Test counts end above the plan's: 330 in the workspace (after the owner decisions below), against the plan's 314.
- **Final review, one fix wave.**
  - **The startup recipe runs the adopted generation** (Clustering §9.3, D-3).
    - Task 11's `tests/startup.rs` loaded the node's local file as the running config, which was
      a plan error. It now runs `bootstrap_or_adopt` first and loads the generation's text with
      the anchor's key.
    - The config crate's docs and roadmap §3.8 say the same, and a reload loads the new text and
      publishes it as the next generation.
  - **`Transient` on a conditional write is an unknown outcome**, like `Contention`, because
    `object_store` doesn't retry conditional PUTs. The doc comments on `renew`, `take_over`,
    `release`, `Acquired::HeldBy` and `publish` say so.
    - **M8 input:** after `Transient` or `Contention`, `observe` before retrying. A stored lease
      that `is_held_by(holder)` at the same `owner_epoch` is still this holder's, because a
      takeover always changes both. A `Conflict` on a retry is therefore not proof of loss.
    - Pass `renew` and `release` only a lease this holder got from `Mine`, `take_over` or
      `renew`. A `HeldLease` newtype is an M8 option.
  - **`write_snapshot` reads back on `Conflict` too.** On S3, a 409 on create can map to
    `AlreadyExists` without the object existing. A different stored hash is `Corrupt`, and that
    branch is now tested.
  - **Smaller fixes.**
    - An epoch at `u64::MAX` is `Corrupt`; it no longer wraps to 0.
    - One crate-private `CREATE_ATTEMPTS = 3` bounds every create-race retry.
    - `establish_anchor` reports `created` for its own landed write.
    - `BucketSpec`'s `Debug` hides option values.
    - `tests/versioned_store.rs` round-trips an escaped key on every backend.
    - `cloud.yml` passes the cloud secrets to the test step only, with `timeout-minutes: 30`.
    - `DocStore::store()` is documented as bypassing the envelope.
- **Owner decisions after the final review (2026-09-29).**
  - `TableRef` (core) now has `deny_unknown_fields`, so an unknown field inside `"table"` in a
    snapshot or job document is `Corrupt`, like any other unknown field (D-M2.6).
  - A schema named exactly `.pipeline-state` is escaped to `%2Epipeline-state` in `table_root`.
    This extends D-M2.4. Without it, with co-located state, such a table's root would be inside
    `<sink.prefix>/.pipeline-state/`, next to `slot.json` and `position.json`, and a Delta `VACUUM`
    of that table could delete them. Only the schema segment can collide; ordinary names are
    unchanged.
- **Left open.** Each is recorded here for a later milestone.
  - The escaped-key round trip covers `/`, `%` and space, not non-ASCII text or `%2E`. Both
    round-trip on RustFS and Azurite (the reviewer's probe).
  - The final review ruled the other per-task minors "leave":
    - cold paths (`escape_segment`'s `format!`);
    - plan-mandated structure (`open_bucket`'s three builder arms);
    - the testkit convention that a skipped IT test reports ok;
    - behaviour that is safe as documented: content-equality `Mine`, and single-writer overwrites
      of the slot and position.

---

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` | Workspace: the two new crates; `async-trait`, `bytes`, `futures-util` |
| `crates/data-avalanche-storage/Cargo.toml` | Crate manifest |
| `…-storage/src/lib.rs` | Modules, root re-exports, crate docs |
| `…-storage/src/store.rs` | `ObjectVersion`, `StoreError`, `VersionedStore`, `ObjectStoreVersioned`, the error mapping |
| `…-storage/src/paths.rs` | `Location`, `join`, `escape_segment`, `table_root`, `colocated_state`, `explicit_state`, `globals_state` |
| `…-storage/src/backend.rs` | `Backend`, `BucketSpec`, `Timeouts`, `OpenError`, `open_bucket` |
| `…-storage/tests/versioned_store.rs` | The same CAS checks on RustFS, Azurite and real GCS, directly and through `open_bucket` |
| `…-storage/tests/backend.rs` | Hermetic: a fake HTTP endpoint for each status, a hung server and a refused connection |
| `crates/data-avalanche-state/Cargo.toml` | Crate manifest |
| `…-state/src/lib.rs` | Modules, root re-exports, crate docs |
| `…-state/src/doc.rs` | `Document`, `Versioned`, `StateError`, `DocStore` (load / create / update / modify), encode / decode |
| `…-state/src/lease.rs` | `LeaseDoc`, `Holder`, `Acquired`, `Leases`, `lease_key` |
| `…-state/src/anchor.rs` | `AnchorDoc`, `HashAlgo`, `Anchor`, `establish_anchor` |
| `…-state/src/generation.rs` | `GenerationDoc`, `ConfigGeneration`, `ConfigGenerations`, `config_hash`, `config_snapshot_key` |
| `…-state/src/pipeline.rs` | `PipelineDocs`, `PipelineStateDoc`, `DesiredState`, `SlotDoc`, `PositionDoc`, `ServerIdentity` |
| `…-state/src/snapshot.rs` | `TableSnapshotDoc`, `SnapshotCursor`, `CursorKey`, `SnapshotStatus`, `SnapshotJobDoc`, `JobTable`, `JobStatus`, keys; the snapshot methods of `PipelineDocs` |
| `…-state/tests/races.rs` | N concurrent creators → 1 winner; N modifiers → all applied (emulators, GCS) |
| `…-state/tests/lease_conformance.rs` | T1–T10 on in-memory, RustFS, Azurite, real GCS, real S3, real Azure |
| `…-state/tests/startup.rs` | The two-load startup sequence with the config crate |
| `crates/data-avalanche-testkit/src/lib.rs` | `delete_prefix`, `cloud_enabled`, `require_cloud!`, `required_env` |
| `.github/workflows/cloud.yml` | Weekly/manual real S3 and Azure conformance |
| `test-infra/README.md` | Real-cloud variables and secrets; the T1–T10 results table |

---

### Task 1: Storage crate — `VersionedStore` and the `object_store` CAS mapping

**Files:**
- Create: `crates/data-avalanche-storage/Cargo.toml`, `crates/data-avalanche-storage/src/lib.rs`, `crates/data-avalanche-storage/src/store.rs`
- Modify: `Cargo.toml`, `crates/data-avalanche-testkit/Cargo.toml`, `crates/data-avalanche-testkit/src/lib.rs`
- Test: `crates/data-avalanche-storage/tests/versioned_store.rs`

**Interfaces:**
- Consumes: `object_store` 0.14.2 (`ObjectStore::put_opts` with `PutMode::{Create, Update, Overwrite}`,
  `get_opts`, `list`); testkit `require_it!`, `require_gcs!`, `stores::{rustfs, azurite, gcs}`,
  `unique_prefix`.
- Produces (crate `data_avalanche_storage`):
  - `ObjectVersion { e_tag: Option<String>, version: Option<String> }` (opaque token; `From<PutResult>`,
    `From<&ObjectMeta>`, `From<&ObjectVersion> for UpdateVersion`).
  - `StoreError::{Conflict { key }, Contention { key }, Transient { key, reason }, Permanent { key, reason }}`.
  - `#[async_trait] trait VersionedStore: Send + Sync + Debug` with `get(&str) -> Result<Option<(Bytes,
    ObjectVersion)>>`, `create(&str, Bytes) -> Result<ObjectVersion>`, `update(&str, &ObjectVersion,
    Bytes) -> Result<ObjectVersion>`, `put(&str, Bytes) -> Result<ObjectVersion>`,
    `list(&str) -> Result<Vec<String>>` (sorted; `""` lists the whole root).
  - `ObjectStoreVersioned::new(Arc<dyn ObjectStore>, root: &str) -> Result<Self, StoreError>`.
  - Testkit: `delete_prefix(&dyn ObjectStore, prefix: &str)` (async; panics on failure).

The error mapping is D-M2.3. `update` of a missing object is `Conflict` on every backend (D-M2.2).
`tests/versioned_store.rs` runs one scenario per backend; with no IT variables it skips.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 4384d46..6c7627a 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -20,6 +20,7 @@ all = { level = "warn", priority = -1 }
 data-avalanche-core = { path = "crates/data-avalanche-core" }
 data-avalanche-config = { path = "crates/data-avalanche-config" }
 data-avalanche-testkit = { path = "crates/data-avalanche-testkit" }
+data-avalanche-storage = { path = "crates/data-avalanche-storage" }
 thiserror = "2"
 serde = { version = "1", features = ["derive"] }
 serde_json = "1"
@@ -38,4 +39,7 @@ rustls-pki-types = { version = "1", features = ["std"] }
 blake3 = { version = "1", default-features = false, features = ["std"] }
 getrandom = { version = "0.3", default-features = false, features = ["std"] }
 tempfile = "3"
+async-trait = "0.1"
+bytes = "1"
+futures-util = { version = "0.3", default-features = false, features = ["std"] }
 rcgen = { version = "0.14", default-features = false, features = ["aws_lc_rs", "pem"] }
```

Create `crates/data-avalanche-storage/Cargo.toml`:

```toml
[package]
name = "data-avalanche-storage"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "Object storage for data-avalanche: backends, path rules and compare-and-swap writes."

[lints]
workspace = true

[dependencies]
async-trait.workspace = true
bytes.workspace = true
futures-util.workspace = true
object_store.workspace = true
thiserror.workspace = true

[dev-dependencies]
data-avalanche-testkit.workspace = true
tokio.workspace = true
```

Create `crates/data-avalanche-storage/src/lib.rs`:

```rust
//! Object storage for data-avalanche: `object_store` backends, the path rules of config §10, and
//! [`VersionedStore`], the compare-and-swap interface every coordination and state document uses.

mod store;

pub use store::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
```

Create `crates/data-avalanche-storage/src/store.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn store(root: &str) -> ObjectStoreVersioned {
        ObjectStoreVersioned::new(Arc::new(InMemory::new()), root).unwrap()
    }

    fn b(s: &'static str) -> Bytes {
        Bytes::from_static(s.as_bytes())
    }

    #[tokio::test]
    async fn create_then_get_returns_body_and_version() {
        let s = store("root");
        let v = s.create("a/doc.json", b("one")).await.unwrap();
        let (body, got) = s.get("a/doc.json").await.unwrap().unwrap();
        assert_eq!(body, b("one"));
        assert_eq!(got, v);
        assert!(s.get("a/missing.json").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_on_existing_key_conflicts() {
        let s = store("root");
        s.create("k", b("one")).await.unwrap();
        let e = s.create("k", b("two")).await.unwrap_err();
        assert!(
            matches!(&e, StoreError::Conflict { key } if key == "k"),
            "{e:?}"
        );
        assert_eq!(s.get("k").await.unwrap().unwrap().0, b("one"));
    }

    #[tokio::test]
    async fn update_is_compare_and_swap() {
        let s = store("");
        let v1 = s.create("k", b("one")).await.unwrap();
        let v2 = s.update("k", &v1, b("two")).await.unwrap();
        assert_ne!(v1, v2, "an update must advance the version");
        let stale = s.update("k", &v1, b("three")).await.unwrap_err();
        assert!(matches!(stale, StoreError::Conflict { .. }), "{stale:?}");
        assert_eq!(s.get("k").await.unwrap().unwrap(), (b("two"), v2));
    }

    #[tokio::test]
    async fn update_of_a_missing_object_conflicts() {
        let s = store("root");
        let v = s.create("k", b("one")).await.unwrap();
        let e = s.update("other", &v, b("two")).await.unwrap_err();
        assert!(matches!(e, StoreError::Conflict { .. }), "{e:?}");
        assert!(s.get("other").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_overwrites_unconditionally() {
        let s = store("root");
        s.put("k", b("one")).await.unwrap();
        let v = s.put("k", b("two")).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), (b("two"), v));
    }

    #[tokio::test]
    async fn list_returns_sorted_keys_under_whole_segment_prefix() {
        let s = store("root/sub");
        for k in [
            "snap/b.json",
            "snap/a.json",
            "snap/jobs/j1.json",
            "snapx/c.json",
            "other.json",
        ] {
            s.put(k, b("x")).await.unwrap();
        }
        assert_eq!(
            s.list("snap").await.unwrap(),
            vec!["snap/a.json", "snap/b.json", "snap/jobs/j1.json"]
        );
        assert!(s.list("none").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn roots_are_isolated() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = ObjectStoreVersioned::new(mem.clone(), "pipelines/a").unwrap();
        let b2 = ObjectStoreVersioned::new(mem.clone(), "pipelines/ab").unwrap();
        let root = ObjectStoreVersioned::new(mem, "").unwrap();
        a.create("state.json", b("a")).await.unwrap();
        b2.create("state.json", b("b")).await.unwrap();
        assert_eq!(a.get("state.json").await.unwrap().unwrap().0, b("a"));
        assert_eq!(a.list("").await.unwrap(), vec!["state.json"]);
        assert_eq!(
            root.list("pipelines").await.unwrap(),
            vec!["pipelines/a/state.json", "pipelines/ab/state.json"]
        );
    }

    #[tokio::test]
    async fn keys_are_stored_verbatim() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = ObjectStoreVersioned::new(mem.clone(), ".pipeline-state").unwrap();
        let key = "snapshots/sales.Заказы-2026 #1.json";
        s.create(key, b("x")).await.unwrap();
        let names: Vec<String> = mem
            .list(None)
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(names, vec![format!(".pipeline-state/{key}")]);
        assert_eq!(s.list("snapshots").await.unwrap(), vec![key]);
    }

    #[tokio::test]
    async fn invalid_keys_are_permanent_errors() {
        let s = store("root");
        for key in ["", "/a", "a/", "a//b", "a/../b", "./a", "a/\u{7}"] {
            let e = s.get(key).await.unwrap_err();
            assert!(
                matches!(&e, StoreError::Permanent { reason, .. } if reason.starts_with("invalid key")),
                "{key:?}: {e:?}"
            );
        }
        assert!(ObjectStoreVersioned::new(Arc::new(InMemory::new()), "a//b").is_err());
    }

    #[test]
    fn error_mapping() {
        use object_store::Error as E;
        let src = || -> Box<dyn std::error::Error + Send + Sync> { "boom".into() };
        let p = || "p".to_owned();
        let cases: Vec<(Op, E, &str)> = vec![
            (
                Op::Create,
                E::AlreadyExists {
                    path: p(),
                    source: src(),
                },
                "Conflict",
            ),
            (
                Op::Update,
                E::AlreadyExists {
                    path: p(),
                    source: src(),
                },
                "Contention",
            ),
            (
                Op::Put,
                E::AlreadyExists {
                    path: p(),
                    source: src(),
                },
                "Contention",
            ),
            (
                Op::Update,
                E::Precondition {
                    path: p(),
                    source: src(),
                },
                "Conflict",
            ),
            (
                Op::Update,
                E::NotFound {
                    path: p(),
                    source: src(),
                },
                "Conflict",
            ),
            (
                Op::List,
                E::NotFound {
                    path: p(),
                    source: src(),
                },
                "Permanent",
            ),
            (
                Op::Get,
                E::PermissionDenied {
                    path: p(),
                    source: src(),
                },
                "Permanent",
            ),
            (
                Op::Get,
                E::Unauthenticated {
                    path: p(),
                    source: src(),
                },
                "Permanent",
            ),
            (
                Op::Create,
                E::NotImplemented {
                    operation: "x".into(),
                    implementer: "y".into(),
                },
                "Permanent",
            ),
            (Op::Put, E::NotSupported { source: src() }, "Permanent"),
            (
                Op::Get,
                E::Generic {
                    store: "S3",
                    source: src(),
                },
                "Transient",
            ),
        ];
        for (op, e, want) in cases {
            let what = format!("{op:?} {e}");
            let got = match map_error(op, "k", e) {
                StoreError::Conflict { .. } => "Conflict",
                StoreError::Contention { .. } => "Contention",
                StoreError::Transient { .. } => "Transient",
                StoreError::Permanent { .. } => "Permanent",
            };
            assert_eq!(got, want, "{what}");
        }
    }
}
```

Create `crates/data-avalanche-storage/tests/versioned_store.rs`:

```rust
//! `ObjectStoreVersioned` against the emulators and the real GCS bucket: the same answers as on
//! `InMemory`, on every backend.

use std::sync::Arc;

use bytes::Bytes;
use data_avalanche_storage::{ObjectStoreVersioned, StoreError, VersionedStore};
use data_avalanche_testkit::{delete_prefix, require_gcs, require_it, stores, unique_prefix};
use object_store::ObjectStore;

async fn check(store: Arc<dyn ObjectStore>, test: &str) {
    let prefix = unique_prefix(test);
    let s = ObjectStoreVersioned::new(store.clone(), &prefix).unwrap();
    let b = |t: &'static str| Bytes::from_static(t.as_bytes());

    let v1 = s
        .create("doc.json", b("one"))
        .await
        .expect("create on an empty key");
    let again = s.create("doc.json", b("two")).await;
    assert!(
        matches!(again, Err(StoreError::Conflict { .. })),
        "create on an existing key: {again:?}"
    );
    assert_eq!(
        s.get("doc.json").await.unwrap(),
        Some((b("one"), v1.clone()))
    );

    let v2 = s
        .update("doc.json", &v1, b("two"))
        .await
        .expect("update with the current version");
    assert_ne!(v1, v2);
    let stale = s.update("doc.json", &v1, b("three")).await;
    assert!(
        matches!(stale, Err(StoreError::Conflict { .. })),
        "update with a stale version: {stale:?}"
    );
    let gone = s.update("missing.json", &v2, b("x")).await;
    assert!(
        matches!(gone, Err(StoreError::Conflict { .. })),
        "update of a missing object: {gone:?}"
    );
    assert_eq!(s.get("missing.json").await.unwrap(), None);

    s.put("snapshots/public.orders.json", b("s")).await.unwrap();
    s.put("snapshots/jobs/j1.json", b("j")).await.unwrap();
    assert_eq!(
        s.list("snapshots").await.unwrap(),
        vec!["snapshots/jobs/j1.json", "snapshots/public.orders.json"]
    );
    assert_eq!(s.get("doc.json").await.unwrap().unwrap().0, b("two"));

    delete_prefix(store.as_ref(), &prefix).await;
}

#[tokio::test]
async fn rustfs() {
    require_it!();
    check(stores::rustfs(), "storage_versioned_rustfs").await;
}

#[tokio::test]
async fn azurite() {
    require_it!();
    check(stores::azurite(), "storage_versioned_azurite").await;
}

#[tokio::test]
async fn real_gcs() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    check(stores::gcs(), "storage_versioned_gcs").await;
}
```

Change `crates/data-avalanche-testkit/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-testkit/Cargo.toml b/crates/data-avalanche-testkit/Cargo.toml
index b98bd8d..93c71fd 100644
--- a/crates/data-avalanche-testkit/Cargo.toml
+++ b/crates/data-avalanche-testkit/Cargo.toml
@@ -10,6 +10,7 @@ description = "Integration-test helpers. Never linked into production binaries."
 workspace = true
 
 [dependencies]
+futures-util.workspace = true
 object_store.workspace = true
 rustls.workspace = true
 uuid.workspace = true
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-storage -p data-avalanche-testkit`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `store::ObjectStoreVersioned`, `store::ObjectVersion`, `store::StoreError`, `store::VersionedStore`
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-storage/src/store.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Compare-and-swap object access: [`VersionedStore`] and its `object_store` implementation.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, ObjectMeta, ObjectStore, PutMode, PutOptions, PutPayload, PutResult, UpdateVersion,
};

/// The version token of a stored object: whatever the backend compares on a conditional update.
/// S3 and Azure compare the ETag, GCS the generation (`version`). Opaque to callers; only ever
/// passed back to [`VersionedStore::update`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

impl From<PutResult> for ObjectVersion {
    fn from(r: PutResult) -> Self {
        Self {
            e_tag: r.e_tag,
            version: r.version,
        }
    }
}

impl From<&ObjectMeta> for ObjectVersion {
    fn from(m: &ObjectMeta) -> Self {
        Self {
            e_tag: m.e_tag.clone(),
            version: m.version.clone(),
        }
    }
}

impl From<&ObjectVersion> for UpdateVersion {
    fn from(v: &ObjectVersion) -> Self {
        Self {
            e_tag: v.e_tag.clone(),
            version: v.version.clone(),
        }
    }
}

/// Why a storage operation failed. The split follows the lease renewer's taxonomy (Clustering
/// §19.2): `Conflict` and `Contention` are answers from the store, `Transient` may succeed on
/// retry, `Permanent` won't.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A conditional write lost: `create` found the object present, or `update` found it changed
    /// or gone.
    #[error("conflict at {key}: the object exists or changed since it was read")]
    Conflict { key: String },
    /// Concurrent conditional writes on one object (S3 409 `ConditionalRequestConflict`, after
    /// `object_store`'s own retries). The outcome is unknown: read the object again.
    #[error("contention at {key}: concurrent conditional writes; read it again")]
    Contention { key: String },
    /// A network, timeout or server failure.
    #[error("transient storage failure at {key}: {reason}")]
    Transient { key: String, reason: String },
    /// Access denied, bad credentials, an unsupported operation or an invalid key.
    #[error("storage failure at {key}: {reason}")]
    Permanent { key: String, reason: String },
}

/// Conditional object access under one root. Keys are `/`-separated and relative to the root;
/// each key is stored exactly as given (no escaping), so a key must have no empty segment, no `.`
/// or `..` segment and no control character.
#[async_trait]
pub trait VersionedStore: Send + Sync + std::fmt::Debug {
    /// The object's body and version, or `None` when it doesn't exist.
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError>;
    /// Writes only if no object exists at `key`; otherwise `Conflict`.
    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError>;
    /// Writes only if the object still has version `expected`; otherwise `Conflict` (also when it
    /// no longer exists).
    async fn update(
        &self,
        key: &str,
        expected: &ObjectVersion,
        body: Bytes,
    ) -> Result<ObjectVersion, StoreError>;
    /// Writes unconditionally.
    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError>;
    /// Every key under `prefix/`, sorted (`a/b` lists `a/b/c`, not `a/bc`); `""` lists the whole
    /// root.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}

/// [`VersionedStore`] over an `object_store` bucket, rooted at a key prefix (`""` = bucket root).
#[derive(Debug, Clone)]
pub struct ObjectStoreVersioned {
    store: Arc<dyn ObjectStore>,
    root: Path,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Get,
    Create,
    Update,
    Put,
    List,
}

impl ObjectStoreVersioned {
    /// # Errors
    /// `Permanent` when `root` is not a valid key.
    pub fn new(store: Arc<dyn ObjectStore>, root: &str) -> Result<Self, StoreError> {
        let root = if root.is_empty() {
            Path::default()
        } else {
            parse_key(root)?
        };
        Ok(Self { store, root })
    }

    fn path(&self, key: &str) -> Result<Path, StoreError> {
        let key = parse_key(key)?;
        Ok(self.root.parts().chain(key.parts()).collect())
    }

    fn relative(&self, location: &Path) -> String {
        let n = self.root.parts().count();
        location
            .parts()
            .skip(n)
            .map(|p| p.as_ref().to_owned())
            .collect::<Vec<_>>()
            .join("/")
    }

    async fn write(
        &self,
        op: Op,
        key: &str,
        body: Bytes,
        mode: PutMode,
    ) -> Result<ObjectVersion, StoreError> {
        let path = self.path(key)?;
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        self.store
            .put_opts(&path, PutPayload::from_bytes(body), opts)
            .await
            .map(ObjectVersion::from)
            .map_err(|e| map_error(op, key, e))
    }
}

fn parse_key(key: &str) -> Result<Path, StoreError> {
    let invalid = |reason: String| StoreError::Permanent {
        key: key.to_owned(),
        reason,
    };
    if key.is_empty() || key.starts_with('/') || key.ends_with('/') {
        return Err(invalid(
            "invalid key: empty, or a leading or trailing '/'".into(),
        ));
    }
    Path::parse(key).map_err(|e| invalid(format!("invalid key: {e}")))
}

fn map_error(op: Op, key: &str, e: object_store::Error) -> StoreError {
    use object_store::Error as E;
    let key = key.to_owned();
    match e {
        E::AlreadyExists { .. } if op == Op::Create => StoreError::Conflict { key },
        E::AlreadyExists { .. } => StoreError::Contention { key },
        E::Precondition { .. } => StoreError::Conflict { key },
        E::NotFound { .. } if op == Op::Update => StoreError::Conflict { key },
        e @ (E::PermissionDenied { .. }
        | E::Unauthenticated { .. }
        | E::NotSupported { .. }
        | E::NotImplemented { .. }
        | E::InvalidPath { .. }
        | E::NotFound { .. }
        | E::NotModified { .. }) => StoreError::Permanent {
            key,
            reason: e.to_string(),
        },
        e => StoreError::Transient {
            key,
            reason: e.to_string(),
        },
    }
}

#[async_trait]
impl VersionedStore for ObjectStoreVersioned {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
        let path = self.path(key)?;
        let result = match self.store.get_opts(&path, GetOptions::default()).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(map_error(Op::Get, key, e)),
        };
        let version = ObjectVersion::from(&result.meta);
        let body = result
            .bytes()
            .await
            .map_err(|e| map_error(Op::Get, key, e))?;
        Ok(Some((body, version)))
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.write(Op::Create, key, body, PutMode::Create).await
    }

    async fn update(
        &self,
        key: &str,
        expected: &ObjectVersion,
        body: Bytes,
    ) -> Result<ObjectVersion, StoreError> {
        self.write(Op::Update, key, body, PutMode::Update(expected.into()))
            .await
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.write(Op::Put, key, body, PutMode::Overwrite).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let path = if prefix.is_empty() {
            self.root.clone()
        } else {
            self.path(prefix)?
        };
        let metas: Vec<ObjectMeta> = self
            .store
            .list(Some(&path))
            .try_collect()
            .await
            .map_err(|e| map_error(Op::List, prefix, e))?;
        let mut keys: Vec<String> = metas.iter().map(|m| self.relative(&m.location)).collect();
        keys.sort();
        Ok(keys)
    }
}

```

Change `crates/data-avalanche-testkit/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/lib.rs b/crates/data-avalanche-testkit/src/lib.rs
index ff352e1..8486855 100644
--- a/crates/data-avalanche-testkit/src/lib.rs
+++ b/crates/data-avalanche-testkit/src/lib.rs
@@ -4,6 +4,7 @@
 
 use std::sync::Arc;
 
+use futures_util::TryStreamExt;
 use object_store::ObjectStore;
 use object_store::aws::AmazonS3Builder;
 use object_store::azure::MicrosoftAzureBuilder;
@@ -59,6 +60,23 @@ pub fn unique_prefix(test: &str) -> String {
     format!("data-avalanche-it/{test}/{}", uuid::Uuid::new_v4())
 }
 
+/// Deletes every object under `prefix` (test cleanup). Panics on a storage error.
+pub async fn delete_prefix(store: &dyn ObjectStore, prefix: &str) {
+    let prefix = object_store::path::Path::from(prefix);
+    let keys: Vec<_> = store
+        .list(Some(&prefix))
+        .map_ok(|m| Ok(m.location))
+        .try_collect()
+        .await
+        .expect("list for cleanup");
+    let deleted: Vec<_> = store
+        .delete_stream(Box::pin(futures_util::stream::iter(keys)))
+        .try_collect()
+        .await
+        .expect("delete for cleanup");
+    drop(deleted);
+}
+
 /// Fixed endpoints from `test-infra/compose.yaml`.
 pub mod endpoints {
     pub const POSTGRES_18: &str =
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-storage -p data-avalanche-testkit`
Expected: PASS. `data-avalanche-storage` unit tests 10; `tests/versioned_store.rs` 3; `data-avalanche-testkit` unit tests 0; `tests/smoke.rs` 3; `tests/storage_cas.rs` 3.

Then the emulators (`test-infra/up.sh` first):

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-storage --test versioned_store -- --nocapture`
Expected: PASS, with RustFS and Azurite really exercised: the only `skipped:` lines are for GCS
(and, in Task 6, for real S3 / Azure). With GCS credentials, drop `DATA_AVALANCHE_SKIP_GCS` and set
`DATA_AVALANCHE_GCS_BUCKET` + `GOOGLE_SERVICE_ACCOUNT_PATH`; the `real_gcs*` tests must PASS too.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml \
  crates/data-avalanche-storage/Cargo.toml \
  crates/data-avalanche-storage/src/lib.rs \
  crates/data-avalanche-storage/src/store.rs \
  crates/data-avalanche-storage/tests/versioned_store.rs \
  crates/data-avalanche-testkit/Cargo.toml \
  crates/data-avalanche-testkit/src/lib.rs \
  Cargo.lock
git commit -m "feat(storage): crate scaffold, VersionedStore and the object_store CAS mapping"
```


---

### Task 2: Storage path rules (config §10)

**Files:**
- Create: `crates/data-avalanche-storage/src/paths.rs`
- Modify: `crates/data-avalanche-storage/src/lib.rs`

**Interfaces:**
- Consumes: `ObjectStoreVersioned::new` (Task 1), in one test.
- Produces:
  - `Location { bucket: String, root: String }` (`root` has no leading or trailing `/`; `""` is
    the bucket root).
  - `join(&[&str]) -> String`.
  - `escape_segment(&str) -> String`.
  - `table_root(sink_bucket, sink_prefix, schema, table) -> Location`.
  - `colocated_state(sink_bucket, sink_prefix) -> Location` (adds `COLOCATED_STATE_DIR` =
    `.pipeline-state`).
  - `explicit_state(bucket, parent_prefix: Option<&str>, prefix) -> Location`.
  - `globals_state(bucket, prefix) -> Location`.

Pure functions over plain strings (D-M2.1). M4 uses `table_root`. M6 turns the config's
`StateLocation` into `colocated_state` / `explicit_state`, as Task 11's test does.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-storage/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/lib.rs b/crates/data-avalanche-storage/src/lib.rs
index 7e88414..441e08b 100644
--- a/crates/data-avalanche-storage/src/lib.rs
+++ b/crates/data-avalanche-storage/src/lib.rs
@@ -1,6 +1,11 @@
 //! Object storage for data-avalanche: `object_store` backends, the path rules of config §10, and
 //! [`VersionedStore`], the compare-and-swap interface every coordination and state document uses.
 
+mod paths;
 mod store;
 
+pub use paths::{
+    COLOCATED_STATE_DIR, Location, colocated_state, escape_segment, explicit_state, globals_state,
+    join, table_root,
+};
 pub use store::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
```

Create `crates/data-avalanche-storage/src/paths.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn loc(bucket: &str, root: &str) -> Location {
        Location {
            bucket: bucket.into(),
            root: root.into(),
        }
    }

    #[test]
    fn join_skips_empty_segments_and_extra_slashes() {
        assert_eq!(join(&["/a/", "", "b//c", "/"]), "a/b/c");
        assert_eq!(join(&["", ""]), "");
        assert_eq!(join(&[]), "");
    }

    #[test]
    fn table_root_under_sink_prefix() {
        assert_eq!(
            table_root("lake", "orders-pg", "public", "orders"),
            loc("lake", "orders-pg/public/orders")
        );
        assert_eq!(
            table_root("lake", "/team/cdc/", "shop", "items"),
            loc("lake", "team/cdc/shop/items")
        );
        assert_eq!(
            table_root("lake", "", "public", "orders"),
            loc("lake", "public/orders"),
            "prefix \"\" is the bucket root"
        );
    }

    #[test]
    fn table_root_escapes_only_what_breaks_a_segment() {
        assert_eq!(
            table_root("b", "p", "sales", "order.items").root,
            "p/sales/order.items"
        );
        assert_eq!(
            table_root("b", "p", "продажи", "Заказы 2026").root,
            "p/продажи/Заказы 2026"
        );
        assert_eq!(table_root("b", "p", "a/b", "50%").root, "p/a%2Fb/50%25");
        assert_eq!(table_root("b", "p", "..", ".").root, "p/%2E%2E/%2E");
        assert_eq!(table_root("b", "p", "s", "t\n").root, "p/s/t%0A");
    }

    #[test]
    fn escaped_roots_are_valid_store_roots() {
        let mem = std::sync::Arc::new(object_store::memory::InMemory::new());
        for (schema, table) in [("..", "."), ("a/b", "c\u{0}d"), ("s", "%2F"), ("", "")] {
            let root = table_root("b", "p", schema, table).root;
            crate::ObjectStoreVersioned::new(mem.clone(), &root)
                .unwrap_or_else(|e| panic!("{root:?}: {e}"));
        }
    }

    #[test]
    fn colocated_state_is_marked_under_sink_prefix() {
        assert_eq!(
            colocated_state("lake", "orders-pg"),
            loc("lake", "orders-pg/.pipeline-state")
        );
        assert_eq!(colocated_state("lake", ""), loc("lake", ".pipeline-state"));
    }

    #[test]
    fn explicit_state_composes_parent_prefix() {
        assert_eq!(
            explicit_state("ops", Some("cdc-state"), "orders-pg"),
            loc("ops", "cdc-state/orders-pg")
        );
        assert_eq!(
            explicit_state("ops", None, "orders-pg"),
            loc("ops", "orders-pg")
        );
        assert_eq!(
            explicit_state("ops", Some(""), "/orders-pg/"),
            loc("ops", "orders-pg")
        );
        assert_eq!(
            explicit_state("ops", Some("cdc-state"), ""),
            loc("ops", "cdc-state")
        );
    }

    #[test]
    fn globals_state_prefix() {
        assert_eq!(globals_state("ops", "cluster-a"), loc("ops", "cluster-a"));
        assert_eq!(globals_state("ops", ""), loc("ops", ""));
        assert_eq!(globals_state("ops", "/a//b/"), loc("ops", "a/b"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-storage`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `paths::COLOCATED_STATE_DIR`, `paths::Location`, `paths::colocated_state`, `paths::escape_segment`, `paths::explicit_state`, `paths::globals_state`, `paths::join`, `paths::table_root`
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-storage/src/paths.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Storage locations (config §10). Segments are joined with `/`; empty segments are skipped and
//! leading or trailing slashes in a segment don't add separators, so a `prefix` of `""` is the
//! bucket root.

/// A bucket (Azure: container) and the key prefix under it; `root` has no leading or trailing
/// `/`, and `""` is the bucket root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub bucket: String,
    pub root: String,
}

/// The marker segment that separates co-located pipeline state from table data (config §10).
pub const COLOCATED_STATE_DIR: &str = ".pipeline-state";

/// Joins segments with one `/`, skipping empty ones: `["/a/", "", "b//c"]` → `"a/b/c"`.
pub fn join(segments: &[&str]) -> String {
    segments
        .iter()
        .flat_map(|s| s.split('/'))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

/// Makes a database identifier safe as one key segment: `%`, `/` and control characters become
/// `%XX`, and a name of `.` or `..` has its dots escaped. Everything else (including `.` inside a
/// name and non-ASCII text) is kept, so ordinary names map to themselves.
pub fn escape_segment(ident: &str) -> String {
    if ident == "." || ident == ".." {
        return ident.replace('.', "%2E");
    }
    let mut out = String::with_capacity(ident.len());
    for c in ident.chars() {
        if c == '%' || c == '/' || c.is_control() {
            let mut buf = [0u8; 4];
            for byte in c.encode_utf8(&mut buf).bytes() {
                out.push_str(&format!("%{byte:02X}"));
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// `<sink.bucket>/<sink.prefix>/<schema>/<table>/`: one table's root, for every sink format. For
/// MySQL, `schema` is the database (`source.db`).
pub fn table_root(sink_bucket: &str, sink_prefix: &str, schema: &str, table: &str) -> Location {
    let (schema, table) = (escape_segment(schema), escape_segment(table));
    Location {
        bucket: sink_bucket.to_owned(),
        root: join(&[sink_prefix, &schema, &table]),
    }
}

/// `<sink.bucket>/<sink.prefix>/.pipeline-state/`: pipeline state stored with the sink data.
/// `defaults.state.prefix` doesn't apply here.
pub fn colocated_state(sink_bucket: &str, sink_prefix: &str) -> Location {
    Location {
        bucket: sink_bucket.to_owned(),
        root: join(&[sink_prefix, COLOCATED_STATE_DIR]),
    }
}

/// `<state.bucket>/[<defaults.state.prefix>/]<state.prefix>/`: an explicit pipeline state
/// location. `parent_prefix` is `defaults.state.prefix`.
pub fn explicit_state(bucket: &str, parent_prefix: Option<&str>, prefix: &str) -> Location {
    Location {
        bucket: bucket.to_owned(),
        root: join(&[parent_prefix.unwrap_or(""), prefix]),
    }
}

/// `<globals.state.bucket>/<globals.state.prefix>/`: cluster-wide state.
pub fn globals_state(bucket: &str, prefix: &str) -> Location {
    Location {
        bucket: bucket.to_owned(),
        root: join(&[prefix]),
    }
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-storage`
Expected: PASS. `data-avalanche-storage` unit tests 17; `tests/versioned_store.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-storage/src/lib.rs \
  crates/data-avalanche-storage/src/paths.rs
git commit -m "feat(storage): storage path rules (config §10)"
```


---

### Task 3: Opening buckets with bounded timeouts

**Files:**
- Create: `crates/data-avalanche-storage/src/backend.rs`
- Modify: `crates/data-avalanche-storage/Cargo.toml`, `crates/data-avalanche-storage/src/lib.rs`, `crates/data-avalanche-storage/tests/versioned_store.rs`
- Test: `crates/data-avalanche-storage/tests/backend.rs`

**Interfaces:**
- Consumes: `ObjectStoreVersioned`, `StoreError` (Task 1); testkit `endpoints::*`.
- Produces:
  - `Backend::{S3, Gcs, Azure { account: Option<String> }}`.
  - `Timeouts { request, connect, max_retries, retry_timeout }` with `Timeouts::COORDINATION`
    (3 s / 1 s / 2 / 5 s, D-M2.5).
  - `BucketSpec { backend, bucket, options: Vec<(String, String)>, timeouts }`.
  - `OpenError { bucket, reason }`: its message names an option key, never its value.
  - `open_bucket(&BucketSpec) -> Result<Arc<dyn ObjectStore>, OpenError>`: builder `from_env()`,
    then the options, then the timeouts and retry policy.

`tests/backend.rs` is hermetic. A tiny TCP server answers every request with one status, or never
answers, so each HTTP outcome and the time bound are tested without an emulator. The
`*_via_open_bucket` tests in `tests/versioned_store.rs` run the Task 1 scenario through
`open_bucket` on RustFS, Azurite and real GCS (credentials from `GOOGLE_*`).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-storage/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-storage/Cargo.toml b/crates/data-avalanche-storage/Cargo.toml
index 77a2259..581a7d9 100644
--- a/crates/data-avalanche-storage/Cargo.toml
+++ b/crates/data-avalanche-storage/Cargo.toml
@@ -18,4 +18,4 @@ thiserror.workspace = true
 
 [dev-dependencies]
 data-avalanche-testkit.workspace = true
-tokio.workspace = true
+tokio = { workspace = true, features = ["net", "io-util"] }
```

Change `crates/data-avalanche-storage/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/lib.rs b/crates/data-avalanche-storage/src/lib.rs
index 441e08b..42aa4ca 100644
--- a/crates/data-avalanche-storage/src/lib.rs
+++ b/crates/data-avalanche-storage/src/lib.rs
@@ -1,9 +1,11 @@
 //! Object storage for data-avalanche: `object_store` backends, the path rules of config §10, and
 //! [`VersionedStore`], the compare-and-swap interface every coordination and state document uses.
 
+mod backend;
 mod paths;
 mod store;
 
+pub use backend::{Backend, BucketSpec, OpenError, Timeouts, open_bucket};
 pub use paths::{
     COLOCATED_STATE_DIR, Location, colocated_state, escape_segment, explicit_state, globals_state,
     join, table_root,
```

Create `crates/data-avalanche-storage/tests/backend.rs`:

```rust
//! `open_bucket` and the error mapping against a local fake S3 endpoint: each HTTP answer (and a
//! server that never answers) must reach the caller as the right `StoreError`, within the
//! configured time bound. Hermetic: no emulator needed.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use data_avalanche_storage::{
    Backend, BucketSpec, ObjectStoreVersioned, ObjectVersion, StoreError, Timeouts, VersionedStore,
    open_bucket,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Copy)]
enum Reply {
    Status(u16),
    Hang,
}

/// A fake endpoint answering every request with `reply`. Returns its URL and a request counter.
async fn fake_endpoint(reply: Reply) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            let c = c.clone();
            tokio::spawn(answer(sock, reply, c));
        }
    });
    (url, count)
}

async fn answer(mut sock: TcpStream, reply: Reply, count: Arc<AtomicUsize>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let Ok(n) = sock.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
        let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
        let body_len: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse().unwrap());
        if buf.len() < end + 4 + body_len {
            continue;
        }
        count.fetch_add(1, Ordering::SeqCst);
        match reply {
            Reply::Hang => tokio::time::sleep(Duration::from_secs(3600)).await,
            Reply::Status(code) => {
                let resp =
                    format!("HTTP/1.1 {code} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                let _ = sock.write_all(resp.as_bytes()).await;
                return;
            }
        }
    }
}

const FAST: Timeouts = Timeouts {
    request: Duration::from_millis(300),
    connect: Duration::from_millis(300),
    max_retries: 2,
    retry_timeout: Duration::from_secs(1),
};

fn s3_spec(endpoint: &str) -> BucketSpec {
    let opt = |k: &str, v: &str| (k.to_owned(), v.to_owned());
    BucketSpec {
        backend: Backend::S3,
        bucket: "b".into(),
        options: vec![
            opt("endpoint", endpoint),
            opt("allow_http", "true"),
            opt("region", "us-east-1"),
            opt("access_key_id", "test"),
            opt("secret_access_key", "test"),
        ],
        timeouts: FAST,
    }
}

async fn versioned(reply: Reply) -> (ObjectStoreVersioned, Arc<AtomicUsize>) {
    let (url, count) = fake_endpoint(reply).await;
    let store = open_bucket(&s3_spec(&url)).unwrap();
    (ObjectStoreVersioned::new(store, "root").unwrap(), count)
}

fn etag() -> ObjectVersion {
    ObjectVersion {
        e_tag: Some("\"v1\"".into()),
        version: None,
    }
}

fn kind(e: &StoreError) -> &'static str {
    match e {
        StoreError::Conflict { .. } => "Conflict",
        StoreError::Contention { .. } => "Contention",
        StoreError::Transient { .. } => "Transient",
        StoreError::Permanent { .. } => "Permanent",
    }
}

#[tokio::test]
async fn http_answers_map_to_store_errors() {
    let body = || Bytes::from_static(b"{}");
    // (status, create, update); a get sends no precondition, so 409/412 can't answer it.
    let cases = [
        (412, "Conflict", "Conflict"),
        (409, "Conflict", "Contention"),
        (403, "Permanent", "Permanent"),
        (401, "Permanent", "Permanent"),
        (500, "Transient", "Transient"),
        (503, "Transient", "Transient"),
    ];
    for (status, create, update) in cases {
        let (s, _) = versioned(Reply::Status(status)).await;
        let got = kind(&s.create("k", body()).await.unwrap_err());
        assert_eq!(got, create, "create, HTTP {status}");
        let got = kind(&s.update("k", &etag(), body()).await.unwrap_err());
        assert_eq!(got, update, "update, HTTP {status}");
        if status != 409 && status != 412 {
            let got = kind(&s.get("k").await.unwrap_err());
            assert_eq!(got, create, "get, HTTP {status}");
        }
    }
}

#[tokio::test]
async fn get_of_a_missing_object_is_none() {
    let (s, _) = versioned(Reply::Status(404)).await;
    assert!(s.get("k").await.unwrap().is_none());
}

#[tokio::test]
async fn update_retries_s3_409_before_reporting_contention() {
    let (s, count) = versioned(Reply::Status(409)).await;
    let e = s
        .update("k", &etag(), Bytes::from_static(b"{}"))
        .await
        .unwrap_err();
    assert!(matches!(e, StoreError::Contention { .. }), "{e:?}");
    assert_eq!(
        count.load(Ordering::SeqCst),
        1 + FAST.max_retries,
        "object_store retries a 409 on an S3 If-Match put"
    );
}

#[tokio::test]
async fn a_hung_endpoint_fails_within_the_timeout_bound() {
    let (s, _) = versioned(Reply::Hang).await;
    for op in ["get", "create", "update"] {
        let start = Instant::now();
        let r = match op {
            "get" => s.get("k").await.map(|_| ()),
            "create" => s.create("k", Bytes::from_static(b"{}")).await.map(|_| ()),
            _ => s
                .update("k", &etag(), Bytes::from_static(b"{}"))
                .await
                .map(|_| ()),
        };
        let took = start.elapsed();
        let e = r.unwrap_err();
        assert!(matches!(e, StoreError::Transient { .. }), "{op}: {e:?}");
        let bound = FAST.retry_timeout + FAST.request * 2 + Duration::from_secs(2);
        assert!(took < bound, "{op} took {took:?}, bound {bound:?}");
    }
}

#[tokio::test]
async fn a_refused_connection_is_transient() {
    let port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let store = open_bucket(&s3_spec(&format!("http://127.0.0.1:{port}"))).unwrap();
    let s = ObjectStoreVersioned::new(store, "").unwrap();
    let e = s.get("k").await.unwrap_err();
    assert!(matches!(e, StoreError::Transient { .. }), "{e:?}");
}

#[test]
fn unknown_option_is_rejected_without_its_value() {
    let mut spec = s3_spec("http://127.0.0.1:1");
    spec.options
        .push(("secret_acces_key".into(), "s3cr3t-value".into()));
    let e = open_bucket(&spec).unwrap_err().to_string();
    assert_eq!(
        e,
        "cannot open bucket b: unknown option \"secret_acces_key\""
    );
    for backend in [
        Backend::Gcs,
        Backend::Azure {
            account: Some("acct".into()),
        },
    ] {
        let spec = BucketSpec {
            backend,
            bucket: "b".into(),
            options: vec![("no_such_option".into(), "x".into())],
            timeouts: Timeouts::COORDINATION,
        };
        assert!(
            open_bucket(&spec)
                .unwrap_err()
                .to_string()
                .contains("unknown option \"no_such_option\"")
        );
    }
}

#[test]
fn azure_emulator_opens_offline() {
    let azure = BucketSpec {
        backend: Backend::Azure { account: None },
        bucket: "c".into(),
        options: vec![("use_emulator".into(), "true".into())],
        timeouts: Timeouts::COORDINATION,
    };
    open_bucket(&azure).unwrap();
}
```

Change `crates/data-avalanche-storage/tests/versioned_store.rs`:

```diff
diff --git a/crates/data-avalanche-storage/tests/versioned_store.rs b/crates/data-avalanche-storage/tests/versioned_store.rs
index e5149a5..c971439 100644
--- a/crates/data-avalanche-storage/tests/versioned_store.rs
+++ b/crates/data-avalanche-storage/tests/versioned_store.rs
@@ -4,7 +4,12 @@
 use std::sync::Arc;
 
 use bytes::Bytes;
-use data_avalanche_storage::{ObjectStoreVersioned, StoreError, VersionedStore};
+use data_avalanche_storage::{
+    Backend, BucketSpec, ObjectStoreVersioned, StoreError, Timeouts, VersionedStore, open_bucket,
+};
+use data_avalanche_testkit::endpoints::{
+    RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY, RUSTFS_URL, TEST_BUCKET,
+};
 use data_avalanche_testkit::{delete_prefix, require_gcs, require_it, stores, unique_prefix};
 use object_store::ObjectStore;
 
@@ -73,3 +78,56 @@ async fn real_gcs() {
     data_avalanche_testkit::install_crypto_provider();
     check(stores::gcs(), "storage_versioned_gcs").await;
 }
+
+fn spec(backend: Backend, bucket: &str, options: &[(&str, &str)]) -> BucketSpec {
+    BucketSpec {
+        backend,
+        bucket: bucket.to_owned(),
+        options: options
+            .iter()
+            .map(|(k, v)| (k.to_string(), v.to_string()))
+            .collect(),
+        timeouts: Timeouts::COORDINATION,
+    }
+}
+
+#[tokio::test]
+async fn rustfs_via_open_bucket() {
+    require_it!();
+    let s = spec(
+        Backend::S3,
+        TEST_BUCKET,
+        &[
+            ("endpoint", RUSTFS_URL),
+            ("allow_http", "true"),
+            ("region", "us-east-1"),
+            ("access_key_id", RUSTFS_ACCESS_KEY),
+            ("secret_access_key", RUSTFS_SECRET_KEY),
+        ],
+    );
+    check(open_bucket(&s).unwrap(), "storage_open_rustfs").await;
+}
+
+#[tokio::test]
+async fn azurite_via_open_bucket() {
+    require_it!();
+    let s = spec(
+        Backend::Azure { account: None },
+        TEST_BUCKET,
+        &[("use_emulator", "true")],
+    );
+    check(open_bucket(&s).unwrap(), "storage_open_azurite").await;
+}
+
+/// GCS through the production path: credentials from `GOOGLE_*` (here the service-account file).
+#[tokio::test]
+async fn real_gcs_via_open_bucket() {
+    require_gcs!();
+    data_avalanche_testkit::install_crypto_provider();
+    let bucket = std::env::var("DATA_AVALANCHE_GCS_BUCKET").expect("DATA_AVALANCHE_GCS_BUCKET");
+    check(
+        open_bucket(&spec(Backend::Gcs, &bucket, &[])).unwrap(),
+        "storage_open_gcs",
+    )
+    .await;
+}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-storage`
Expected: compilation FAILS. The first errors:

```text
error[E0583]: file not found for module `backend`
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-storage/src/backend.rs`:

```rust
//! Opening a bucket: `object_store` builders fed from the environment's credential chain, with
//! explicit options and bounded timeouts on top.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::{BackoffConfig, ClientConfigKey, ObjectStore, RetryConfig};

/// Which object store a bucket lives in (config `globals.cloud`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    /// Credentials from the AWS default chain (`AWS_*` variables, web identity, instance profile).
    S3,
    /// Credentials from `GOOGLE_*` variables or Application Default Credentials.
    Gcs,
    /// Credentials from `AZURE_*` variables, workload identity or managed identity. `account` is
    /// `globals.azure.account_name`; `None` takes `AZURE_STORAGE_ACCOUNT_NAME`.
    Azure { account: Option<String> },
}

/// How long one storage call may take. Each request is bounded by `request` (`connect` for the
/// TCP/TLS connect), and retries stop once `retry_timeout` has passed since the first attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    pub request: Duration,
    pub connect: Duration,
    pub max_retries: usize,
    pub retry_timeout: Duration,
}

impl Timeouts {
    /// For leases and state documents: a hung or failing store surfaces within about
    /// `retry_timeout + request` = 8 s, about half the default 15 s lease, so the caller decides
    /// instead of waiting. (`object_store`'s own defaults allow 30 s per request and 3 minutes of
    /// retries.)
    pub const COORDINATION: Self = Self {
        request: Duration::from_secs(3),
        connect: Duration::from_secs(1),
        max_retries: 2,
        retry_timeout: Duration::from_secs(5),
    };

    fn retry(&self) -> RetryConfig {
        RetryConfig {
            backoff: BackoffConfig {
                init_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(2),
                base: 2.0,
            },
            max_retries: self.max_retries,
            retry_timeout: self.retry_timeout,
        }
    }
}

/// One bucket (Azure: container) to open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketSpec {
    pub backend: Backend,
    pub bucket: String,
    /// `object_store` config keys for the backend's builder (e.g. `endpoint`, `allow_http`,
    /// `region`, `use_emulator`), applied over the environment.
    pub options: Vec<(String, String)>,
    pub timeouts: Timeouts,
}

/// A bucket that can't be opened. The message names an option key, never its value.
#[derive(Debug, thiserror::Error)]
#[error("cannot open bucket {bucket}: {reason}")]
pub struct OpenError {
    pub bucket: String,
    pub reason: String,
}

fn ms(d: Duration) -> String {
    format!("{}ms", d.as_millis())
}

/// Builds the store for `spec`. Nothing is contacted until the first request.
///
/// # Errors
/// An unknown option key, or a configuration the builder rejects (e.g. no Azure account).
pub fn open_bucket(spec: &BucketSpec) -> Result<Arc<dyn ObjectStore>, OpenError> {
    let err = |reason: String| OpenError {
        bucket: spec.bucket.clone(),
        reason,
    };
    let key_err = |k: &str| err(format!("unknown option {k:?}"));
    let t = &spec.timeouts;
    let store: Arc<dyn ObjectStore> = match &spec.backend {
        Backend::S3 => {
            let mut b = AmazonS3Builder::from_env().with_bucket_name(&spec.bucket);
            for (k, v) in &spec.options {
                b = b.with_config(AmazonS3ConfigKey::from_str(k).map_err(|_| key_err(k))?, v);
            }
            let b = b
                .with_config(
                    AmazonS3ConfigKey::Client(ClientConfigKey::Timeout),
                    ms(t.request),
                )
                .with_config(
                    AmazonS3ConfigKey::Client(ClientConfigKey::ConnectTimeout),
                    ms(t.connect),
                )
                .with_retry(t.retry());
            Arc::new(b.build().map_err(|e| err(e.to_string()))?)
        }
        Backend::Gcs => {
            let mut b = GoogleCloudStorageBuilder::from_env().with_bucket_name(&spec.bucket);
            for (k, v) in &spec.options {
                b = b.with_config(GoogleConfigKey::from_str(k).map_err(|_| key_err(k))?, v);
            }
            let b = b
                .with_config(
                    GoogleConfigKey::Client(ClientConfigKey::Timeout),
                    ms(t.request),
                )
                .with_config(
                    GoogleConfigKey::Client(ClientConfigKey::ConnectTimeout),
                    ms(t.connect),
                )
                .with_retry(t.retry());
            Arc::new(b.build().map_err(|e| err(e.to_string()))?)
        }
        Backend::Azure { account } => {
            let mut b = MicrosoftAzureBuilder::from_env().with_container_name(&spec.bucket);
            if let Some(account) = account {
                b = b.with_account(account);
            }
            for (k, v) in &spec.options {
                b = b.with_config(AzureConfigKey::from_str(k).map_err(|_| key_err(k))?, v);
            }
            let b = b
                .with_config(
                    AzureConfigKey::Client(ClientConfigKey::Timeout),
                    ms(t.request),
                )
                .with_config(
                    AzureConfigKey::Client(ClientConfigKey::ConnectTimeout),
                    ms(t.connect),
                )
                .with_retry(t.retry());
            Arc::new(b.build().map_err(|e| err(e.to_string()))?)
        }
    };
    Ok(store)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-storage`
Expected: PASS. `data-avalanche-storage` unit tests 17; `tests/backend.rs` 7; `tests/versioned_store.rs` 6.

Then the emulators (`test-infra/up.sh` first):

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-storage --test versioned_store -- --nocapture`
Expected: PASS, with RustFS and Azurite really exercised: the only `skipped:` lines are for GCS
(and, in Task 6, for real S3 / Azure). With GCS credentials, drop `DATA_AVALANCHE_SKIP_GCS` and set
`DATA_AVALANCHE_GCS_BUCKET` + `GOOGLE_SERVICE_ACCOUNT_PATH`; the `real_gcs*` tests must PASS too.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-storage/Cargo.toml \
  crates/data-avalanche-storage/src/lib.rs \
  crates/data-avalanche-storage/tests/backend.rs \
  crates/data-avalanche-storage/tests/versioned_store.rs \
  crates/data-avalanche-storage/src/backend.rs \
  Cargo.lock
git commit -m "feat(storage): open buckets with bounded coordination timeouts"
```


---

### Task 4: State crate — versioned JSON documents and CAS read-modify-write

**Files:**
- Create: `crates/data-avalanche-state/Cargo.toml`, `crates/data-avalanche-state/src/doc.rs`, `crates/data-avalanche-state/src/lib.rs`
- Modify: `Cargo.toml`
- Test: `crates/data-avalanche-state/tests/races.rs`

**Interfaces:**
- Consumes: `VersionedStore`, `ObjectVersion`, `StoreError`, `ObjectStoreVersioned` (Task 1).
- Produces (crate `data_avalanche_state`):
  - `trait Document: Serialize + DeserializeOwned + Send + Sync { const KIND: &'static str; const SCHEMA_VERSION: u64; }`.
  - `Versioned<T> { doc: T, version: ObjectVersion }`.
  - `StateError::{Store(StoreError), Corrupt { key, kind, reason }, UnsupportedVersion { key, kind,
    found, supported }}` with `is_race()` (`Conflict` or `Contention`) and `is_conflict()`.
  - `DocStore::new(Arc<dyn VersionedStore>)`, `store()`, `load::<T>(key) -> Result<Option<Versioned<T>>>`,
    `create(key, &T)`, `update(key, &ObjectVersion, &T)`, `modify(key, FnMut(Option<T>) -> Result<T,
    StateError>) -> Result<Versioned<T>>`.
  - `MODIFY_ATTEMPTS = 16`.
  - `pub(crate) encode` / `decode`, used by later tasks' JSON-shape tests.

D-M2.6 and D-M2.7. `tests/races.rs` is the roadmap's "N writers → 1 winner" exit check on real
backends.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 6c7627a..d075362 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -21,6 +21,7 @@ data-avalanche-core = { path = "crates/data-avalanche-core" }
 data-avalanche-config = { path = "crates/data-avalanche-config" }
 data-avalanche-testkit = { path = "crates/data-avalanche-testkit" }
 data-avalanche-storage = { path = "crates/data-avalanche-storage" }
+data-avalanche-state = { path = "crates/data-avalanche-state" }
 thiserror = "2"
 serde = { version = "1", features = ["derive"] }
 serde_json = "1"
```

Create `crates/data-avalanche-state/Cargo.toml`:

```toml
[package]
name = "data-avalanche-state"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "Durable state for data-avalanche: per-pipeline documents, leases and globals.state."

[lints]
workspace = true

[dependencies]
bytes.workspace = true
data-avalanche-storage.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true

[dev-dependencies]
data-avalanche-testkit.workspace = true
object_store.workspace = true
tokio.workspace = true
```

Create `crates/data-avalanche-state/src/doc.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Counter {
        name: String,
        n: u64,
    }

    impl Document for Counter {
        const KIND: &'static str = "counter";
        const SCHEMA_VERSION: u64 = 3;
    }

    fn docs() -> DocStore {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state").unwrap();
        DocStore::new(Arc::new(store))
    }

    fn counter(n: u64) -> Counter {
        Counter {
            name: "c".into(),
            n,
        }
    }

    #[test]
    fn encodes_version_first_as_pretty_json() {
        let body = encode(&counter(1));
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            "{\n  \"version\": 3,\n  \"name\": \"c\",\n  \"n\": 1\n}"
        );
        assert_eq!(decode::<Counter>("k", &body).unwrap(), counter(1));
    }

    #[test]
    fn refuses_other_schema_versions() {
        for (body, found) in [
            (r#"{"version":4,"name":"c","n":1}"#, 4),
            (r#"{"version":2,"name":"c","n":1}"#, 2),
        ] {
            let e = decode::<Counter>("k.json", body.as_bytes()).unwrap_err();
            assert!(
                matches!(e, StateError::UnsupportedVersion { found: f, supported: 3, .. } if f == found),
                "{e:?}"
            );
        }
        let e = decode::<Counter>("k.json", br#"{"version":4,"name":"c","n":1}"#).unwrap_err();
        assert_eq!(
            e.to_string(),
            "k.json: counter document has schema version 4; this build reads version 3 (written by another data-avalanche release?)"
        );
    }

    #[test]
    fn refuses_corrupt_documents() {
        let cases = [
            (&b"{not json"[..], "key must be a string"),
            (b"[1,2]", "not a JSON object"),
            (br#"{"name":"c","n":1}"#, "no \"version\" field"),
            (
                br#"{"version":"3","name":"c","n":1}"#,
                "\"version\" is not a non-negative integer",
            ),
            (
                br#"{"version":-3,"name":"c","n":1}"#,
                "\"version\" is not a non-negative integer",
            ),
            (br#"{"version":3,"name":"c"}"#, "missing field `n`"),
            (
                br#"{"version":3,"name":"c","n":1,"extra":true}"#,
                "unknown field `extra`",
            ),
            (b"", "EOF while parsing"),
        ];
        for (body, want) in cases {
            let e = decode::<Counter>("k.json", body).unwrap_err();
            match &e {
                StateError::Corrupt {
                    key,
                    kind: "counter",
                    reason,
                } if key == "k.json" => {
                    assert!(reason.contains(want), "{reason:?} should contain {want:?}")
                }
                other => panic!("{:?}: {other:?}", String::from_utf8_lossy(body)),
            }
        }
    }

    #[tokio::test]
    async fn load_create_update_round_trip() {
        let d = docs();
        assert!(d.load::<Counter>("c.json").await.unwrap().is_none());
        let v1 = d.create("c.json", &counter(1)).await.unwrap();
        let got = d.load::<Counter>("c.json").await.unwrap().unwrap();
        assert_eq!(
            got,
            Versioned {
                doc: counter(1),
                version: v1.clone()
            }
        );
        let e = d.create("c.json", &counter(9)).await.unwrap_err();
        assert!(e.is_race(), "{e:?}");
        let v2 = d.update("c.json", &v1, &counter(2)).await.unwrap();
        let e = d.update("c.json", &v1, &counter(9)).await.unwrap_err();
        assert!(e.is_race(), "{e:?}");
        assert_eq!(
            d.load::<Counter>("c.json").await.unwrap().unwrap(),
            Versioned {
                doc: counter(2),
                version: v2
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_creates_have_one_winner() {
        let d = docs();
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let d = d.clone();
                tokio::spawn(async move { d.create("c.json", &counter(i)).await })
            })
            .collect();
        let mut wins = 0;
        for t in tasks {
            match t.await.unwrap() {
                Ok(_) => wins += 1,
                Err(e) => assert!(e.is_race(), "{e:?}"),
            }
        }
        assert_eq!(wins, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_modifies_all_apply() {
        let d = docs();
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let d = d.clone();
                tokio::spawn(async move {
                    d.modify("c.json", |cur: Option<Counter>| {
                        Ok(counter(cur.map_or(0, |c| c.n) + 1))
                    })
                    .await
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(
            d.load::<Counter>("c.json").await.unwrap().unwrap().doc,
            counter(8)
        );
    }

    #[tokio::test]
    async fn modify_writes_nothing_when_f_fails() {
        let d = docs();
        d.create("c.json", &counter(1)).await.unwrap();
        let e = d
            .modify("c.json", |_: Option<Counter>| {
                Err(StateError::Corrupt {
                    key: "c.json".into(),
                    kind: "counter",
                    reason: "refused".into(),
                })
            })
            .await
            .unwrap_err();
        assert!(e.to_string().contains("refused"));
        assert_eq!(
            d.load::<Counter>("c.json").await.unwrap().unwrap().doc,
            counter(1)
        );
    }

    #[tokio::test]
    async fn a_corrupt_stored_document_is_an_error_not_absent() {
        let d = docs();
        d.store()
            .put("c.json", Bytes::from_static(b"{}"))
            .await
            .unwrap();
        let e = d.load::<Counter>("c.json").await.unwrap_err();
        assert!(matches!(e, StateError::Corrupt { .. }), "{e:?}");
        let e = d
            .modify("c.json", |_: Option<Counter>| Ok(counter(1)))
            .await
            .unwrap_err();
        assert!(
            matches!(e, StateError::Corrupt { .. }),
            "modify must not overwrite a corrupt document: {e:?}"
        );
    }
}
```

Create `crates/data-avalanche-state/src/lib.rs`:

```rust
//! Durable state for data-avalanche: the per-pipeline documents (Data-Flow §State Layout), leases
//! and the `globals.state` documents (Clustering §12), each a JSON [`Document`] with a schema
//! `version`, written with compare-and-swap through `data-avalanche-storage`.

mod doc;

pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
```

Create `crates/data-avalanche-state/tests/races.rs`:

```rust
//! CAS races on real backends: N concurrent creators → exactly one winner; N concurrent
//! read-modify-writers → every change applied once.

use std::sync::Arc;

use data_avalanche_state::{DocStore, Document};
use data_avalanche_storage::ObjectStoreVersioned;
use data_avalanche_testkit::{delete_prefix, require_gcs, require_it, stores, unique_prefix};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Tally {
    writers: Vec<u32>,
}

impl Document for Tally {
    const KIND: &'static str = "tally";
    const SCHEMA_VERSION: u64 = 1;
}

const N: u32 = 8;

async fn races(store: Arc<dyn ObjectStore>, test: &str) {
    let prefix = unique_prefix(test);
    let docs = DocStore::new(Arc::new(
        ObjectStoreVersioned::new(store.clone(), &prefix).unwrap(),
    ));

    let creators: Vec<_> = (0..N)
        .map(|i| {
            let docs = docs.clone();
            tokio::spawn(async move {
                docs.create("created.json", &Tally { writers: vec![i] })
                    .await
            })
        })
        .collect();
    let mut wins = 0;
    for t in creators {
        match t.await.unwrap() {
            Ok(_) => wins += 1,
            Err(e) => assert!(e.is_race(), "a losing create must be a race error: {e:?}"),
        }
    }
    assert_eq!(wins, 1, "exactly one concurrent create may win");

    let writers: Vec<_> = (0..N)
        .map(|i| {
            let docs = docs.clone();
            tokio::spawn(async move {
                docs.modify("modified.json", move |cur: Option<Tally>| {
                    let mut t = cur.unwrap_or(Tally { writers: vec![] });
                    if !t.writers.contains(&i) {
                        t.writers.push(i);
                    }
                    Ok(t)
                })
                .await
            })
        })
        .collect();
    for t in writers {
        t.await
            .unwrap()
            .expect("every modify lands within MODIFY_ATTEMPTS");
    }
    let mut got = docs
        .load::<Tally>("modified.json")
        .await
        .unwrap()
        .unwrap()
        .doc
        .writers;
    got.sort();
    assert_eq!(got, (0..N).collect::<Vec<_>>());

    delete_prefix(store.as_ref(), &prefix).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs() {
    require_it!();
    races(stores::rustfs(), "state_races_rustfs").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn azurite() {
    require_it!();
    races(stores::azurite(), "state_races_azurite").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_gcs() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    races(stores::gcs(), "state_races_gcs").await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `doc::DocStore`, `doc::Document`, `doc::MODIFY_ATTEMPTS`, `doc::StateError`, `doc::Versioned`
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-state/src/doc.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! JSON documents with a schema `version`, read and written with compare-and-swap.

use std::sync::Arc;

use bytes::Bytes;
use data_avalanche_storage::{ObjectVersion, StoreError, VersionedStore};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A state document type. Stored as pretty JSON with a top-level `"version"` (the schema version)
/// beside the type's own fields; a document with another `version` is refused, never guessed at.
pub trait Document: Serialize + DeserializeOwned + Send + Sync {
    /// Names the document in errors (`"lease"`, `"pipeline state"`).
    const KIND: &'static str;
    const SCHEMA_VERSION: u64;
}

/// A document and the object version it was read or written at.
#[derive(Debug, Clone, PartialEq)]
pub struct Versioned<T> {
    pub doc: T,
    pub version: ObjectVersion,
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{key}: not a valid {kind} document: {reason}")]
    Corrupt {
        key: String,
        kind: &'static str,
        reason: String,
    },
    #[error(
        "{key}: {kind} document has schema version {found}; this build reads version {supported} (written by another data-avalanche release?)"
    )]
    UnsupportedVersion {
        key: String,
        kind: &'static str,
        found: u64,
        supported: u64,
    },
}

impl StateError {
    /// True when a conditional write lost (`Conflict`) or its outcome is unknown (`Contention`):
    /// read again and decide.
    pub fn is_race(&self) -> bool {
        matches!(
            self,
            Self::Store(StoreError::Conflict { .. } | StoreError::Contention { .. })
        )
    }
}

#[derive(Serialize)]
struct Envelope<'a, T> {
    version: u64,
    #[serde(flatten)]
    doc: &'a T,
}

pub(crate) fn encode<T: Document>(doc: &T) -> Bytes {
    let body = Envelope {
        version: T::SCHEMA_VERSION,
        doc,
    };
    Bytes::from(serde_json::to_vec_pretty(&body).expect("state documents serialize to JSON"))
}

pub(crate) fn decode<T: Document>(key: &str, body: &[u8]) -> Result<T, StateError> {
    let corrupt = |reason: String| StateError::Corrupt {
        key: key.to_owned(),
        kind: T::KIND,
        reason,
    };
    let mut value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| corrupt(e.to_string()))?;
    let map = value
        .as_object_mut()
        .ok_or_else(|| corrupt("not a JSON object".into()))?;
    let found = match map.remove("version") {
        Some(v) if v.is_u64() => v.as_u64().unwrap_or_default(),
        Some(_) => return Err(corrupt("\"version\" is not a non-negative integer".into())),
        None => return Err(corrupt("no \"version\" field".into())),
    };
    if found != T::SCHEMA_VERSION {
        return Err(StateError::UnsupportedVersion {
            key: key.to_owned(),
            kind: T::KIND,
            found,
            supported: T::SCHEMA_VERSION,
        });
    }
    serde_json::from_value(value).map_err(|e| corrupt(e.to_string()))
}

/// Typed documents over a [`VersionedStore`].
#[derive(Debug, Clone)]
pub struct DocStore {
    store: Arc<dyn VersionedStore>,
}

/// How many times [`DocStore::modify`] re-reads after losing a race before it gives up.
pub const MODIFY_ATTEMPTS: usize = 16;

impl DocStore {
    pub fn new(store: Arc<dyn VersionedStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Arc<dyn VersionedStore> {
        &self.store
    }

    /// # Errors
    /// A storage failure, or a body that isn't a valid document of this type and version.
    pub async fn load<T: Document>(&self, key: &str) -> Result<Option<Versioned<T>>, StateError> {
        match self.store.get(key).await? {
            None => Ok(None),
            Some((body, version)) => Ok(Some(Versioned {
                doc: decode(key, &body)?,
                version,
            })),
        }
    }

    /// Writes `doc` only if nothing exists at `key`.
    ///
    /// # Errors
    /// `Store(Conflict)` when the key exists.
    pub async fn create<T: Document>(
        &self,
        key: &str,
        doc: &T,
    ) -> Result<ObjectVersion, StateError> {
        Ok(self.store.create(key, encode(doc)).await?)
    }

    /// Writes `doc` only if the object is still at `expected`.
    ///
    /// # Errors
    /// `Store(Conflict)` when it moved or is gone; `Store(Contention)` when the outcome is unknown.
    pub async fn update<T: Document>(
        &self,
        key: &str,
        expected: &ObjectVersion,
        doc: &T,
    ) -> Result<ObjectVersion, StateError> {
        Ok(self.store.update(key, expected, encode(doc)).await?)
    }

    /// Read-modify-write: `f` gets the current document (`None` if absent) and returns the one to
    /// write, which is created or CAS-updated. On a lost race it reads again and calls `f` again,
    /// up to [`MODIFY_ATTEMPTS`] times. After `Contention` the earlier write may have landed, so
    /// `f` must compute the target state from what it reads (set, don't increment blindly).
    ///
    /// # Errors
    /// An error from `f` (nothing is written), a storage or decode failure, or the last race error
    /// once the attempts run out.
    pub async fn modify<T, F>(&self, key: &str, mut f: F) -> Result<Versioned<T>, StateError>
    where
        T: Document,
        F: FnMut(Option<T>) -> Result<T, StateError> + Send,
    {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let current = self.load::<T>(key).await?;
            let (doc, written) = match current {
                None => {
                    let doc = f(None)?;
                    let r = self.create(key, &doc).await;
                    (doc, r)
                }
                Some(Versioned { doc, version }) => {
                    let doc = f(Some(doc))?;
                    let r = self.update(key, &version, &doc).await;
                    (doc, r)
                }
            };
            match written {
                Ok(version) => return Ok(Versioned { doc, version }),
                Err(e) if e.is_race() && attempt < MODIFY_ATTEMPTS => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state`
Expected: PASS. `data-avalanche-state` unit tests 8; `tests/races.rs` 3.

Then the emulators (`test-infra/up.sh` first):

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-state --test races -- --nocapture`
Expected: PASS, with RustFS and Azurite really exercised: the only `skipped:` lines are for GCS
(and, in Task 6, for real S3 / Azure). With GCS credentials, drop `DATA_AVALANCHE_SKIP_GCS` and set
`DATA_AVALANCHE_GCS_BUCKET` + `GOOGLE_SERVICE_ACCOUNT_PATH`; the `real_gcs*` tests must PASS too.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml \
  crates/data-avalanche-state/Cargo.toml \
  crates/data-avalanche-state/src/doc.rs \
  crates/data-avalanche-state/src/lib.rs \
  crates/data-avalanche-state/tests/races.rs \
  Cargo.lock
git commit -m "feat(state): crate scaffold, versioned JSON documents and CAS read-modify-write"
```


---

### Task 5: The lease document and its CAS operations

**Files:**
- Create: `crates/data-avalanche-state/src/lease.rs`
- Modify: `crates/data-avalanche-state/Cargo.toml`, `crates/data-avalanche-state/src/lib.rs`, `crates/data-avalanche-state/src/doc.rs`

**Interfaces:**
- Consumes: `DocStore`, `Document`, `StateError::{is_race, is_conflict}`, `Versioned` (Task 4);
  `escape_segment` (Task 2).
- Produces:
  - `LeaseDoc { pipeline_id, holder_node_id, holder_incarnation: u64, owner_epoch: u64,
    acquired_at, renewed_at: DateTime<Utc>, lease_duration_ms: u64, released: bool }` (Clustering
    §5.2 plus `released`), with `is_held_by(&Holder) -> bool`.
  - `Holder { node_id: String, incarnation: u64 }`.
  - `Acquired::{Mine(Versioned<LeaseDoc>), HeldBy(Versioned<LeaseDoc>)}`.
  - `lease_key(pipeline_id) -> String` (`leases/<id>.json`).
  - `Leases::new(DocStore)` with:
    - `acquire(pipeline_id, &Holder, lease_duration: Duration, now) -> Result<Acquired>`;
    - `renew(&Versioned<LeaseDoc>, now)`;
    - `observe(pipeline_id) -> Result<Option<Versioned<LeaseDoc>>>`;
    - `take_over(&observed, &Holder, lease_duration, now)`;
    - `release(&Versioned<LeaseDoc>, now)`.
    
    The writing calls return `Result<Versioned<LeaseDoc>>`.

How the renewer (M8) reads the results (Clustering §19.2):
- `is_conflict()` → **Lost**;
- `Contention` → observe, then check `is_held_by`;
- `Transient` → **Degraded**;
- `Permanent` → **Lost**, and alert.

D-M2.8, D-M2.9.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-state/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-state/Cargo.toml b/crates/data-avalanche-state/Cargo.toml
index 70ce324..255625a 100644
--- a/crates/data-avalanche-state/Cargo.toml
+++ b/crates/data-avalanche-state/Cargo.toml
@@ -11,6 +11,7 @@ workspace = true
 
 [dependencies]
 bytes.workspace = true
+chrono.workspace = true
 data-avalanche-storage.workspace = true
 serde.workspace = true
 serde_json.workspace = true
```

Create `crates/data-avalanche-state/src/lease.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;

    const DUR: Duration = Duration::from_secs(15);

    fn leases() -> Leases {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        Leases::new(DocStore::new(Arc::new(store)))
    }

    fn node(id: &str) -> Holder {
        Holder {
            node_id: id.into(),
            incarnation: 7,
        }
    }

    fn t(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, s).unwrap()
    }

    async fn mine(l: &Leases, id: &str, who: &str) -> Versioned<LeaseDoc> {
        match l.acquire(id, &node(who), DUR, t(0)).await.unwrap() {
            Acquired::Mine(lease) => lease,
            other => panic!("expected to acquire, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn acquire_on_absent_creates_epoch_1() {
        let l = leases();
        let lease = mine(&l, "orders", "a").await;
        assert_eq!(
            lease.doc,
            LeaseDoc {
                pipeline_id: "orders".into(),
                holder_node_id: "a".into(),
                holder_incarnation: 7,
                owner_epoch: 1,
                acquired_at: t(0),
                renewed_at: t(0),
                lease_duration_ms: 15_000,
                released: false,
            }
        );
        assert!(lease.doc.is_held_by(&node("a")));
        assert!(!lease.doc.is_held_by(&Holder {
            node_id: "a".into(),
            incarnation: 8
        }));
        assert_eq!(l.observe("orders").await.unwrap(), Some(lease));
    }

    #[test]
    fn lease_json_shape() {
        let doc = Leases::new_doc("orders", &node("worker-7f3a9c"), 87, DUR, t(10));
        assert_eq!(
            std::str::from_utf8(&crate::doc::encode(&doc)).unwrap(),
            r#"{
  "version": 1,
  "pipeline_id": "orders",
  "holder_node_id": "worker-7f3a9c",
  "holder_incarnation": 7,
  "owner_epoch": 87,
  "acquired_at": "2026-09-28T10:00:10Z",
  "renewed_at": "2026-09-28T10:00:10Z",
  "lease_duration_ms": 15000,
  "released": false
}"#
        );
    }

    #[tokio::test]
    async fn acquire_leaves_a_live_holder_alone() {
        let l = leases();
        let a = mine(&l, "orders", "a").await;
        assert_eq!(
            l.acquire("orders", &node("b"), DUR, t(1)).await.unwrap(),
            Acquired::HeldBy(a)
        );
    }

    #[tokio::test]
    async fn renew_advances_the_version_and_keeps_the_epoch() {
        let l = leases();
        let a0 = mine(&l, "orders", "a").await;
        let a1 = l.renew(&a0, t(5)).await.unwrap();
        assert_ne!(a0.version, a1.version);
        assert_eq!(
            (a1.doc.owner_epoch, a1.doc.renewed_at, a1.doc.acquired_at),
            (1, t(5), t(0))
        );
        let stale = l.renew(&a0, t(6)).await.unwrap_err();
        assert!(stale.is_conflict(), "{stale:?}");
    }

    #[tokio::test]
    async fn take_over_bumps_the_epoch_and_fences_the_old_holder() {
        let l = leases();
        let a = mine(&l, "orders", "a").await;
        let observed = l.observe("orders").await.unwrap().unwrap();
        let b = l
            .take_over(&observed, &node("b"), DUR, t(20))
            .await
            .unwrap();
        assert_eq!(
            (
                b.doc.holder_node_id.as_str(),
                b.doc.owner_epoch,
                b.doc.acquired_at
            ),
            ("b", 2, t(20))
        );
        assert!(
            l.renew(&a, t(21)).await.unwrap_err().is_conflict(),
            "the old holder has lost it"
        );
    }

    #[tokio::test]
    async fn take_over_fails_when_the_holder_renewed_since_observed() {
        let l = leases();
        let a = mine(&l, "orders", "a").await;
        let observed = l.observe("orders").await.unwrap().unwrap();
        l.renew(&a, t(5)).await.unwrap();
        let e = l
            .take_over(&observed, &node("b"), DUR, t(20))
            .await
            .unwrap_err();
        assert!(e.is_conflict(), "{e:?}");
    }

    #[tokio::test]
    async fn release_leaves_a_tombstone_and_the_epoch_continues() {
        let l = leases();
        let a = mine(&l, "orders", "a").await;
        let a = l.renew(&a, t(3)).await.unwrap();
        let tomb = l.release(&a, t(4)).await.unwrap();
        assert!(tomb.doc.released && !tomb.doc.is_held_by(&node("a")));
        assert_eq!(
            (tomb.doc.owner_epoch, tomb.doc.holder_node_id.as_str()),
            (1, "a")
        );
        let b = mine(&l, "orders", "b").await;
        assert_eq!(
            (
                b.doc.owner_epoch,
                b.doc.holder_node_id.as_str(),
                b.doc.released
            ),
            (2, "b", false)
        );
        let b = l.release(&b, t(5)).await.unwrap();
        assert_eq!(
            mine(&l, "orders", "a").await.doc.owner_epoch,
            3,
            "epochs never repeat across releases"
        );
        assert!(
            l.release(&b, t(6)).await.unwrap_err().is_conflict(),
            "a stale release conflicts"
        );
    }

    /// Known limitation: the epoch lives in the object, so deleting it restarts the count.
    #[tokio::test]
    async fn an_externally_deleted_lease_restarts_at_epoch_1() {
        let mem = Arc::new(InMemory::new());
        let store = ObjectStoreVersioned::new(mem.clone(), "globals").unwrap();
        let l = Leases::new(DocStore::new(Arc::new(store)));
        let a = mine(&l, "orders", "a").await;
        let a = l.take_over(&a, &node("b"), DUR, t(1)).await.unwrap();
        assert_eq!(a.doc.owner_epoch, 2);
        object_store::ObjectStoreExt::delete(mem.as_ref(), &"globals/leases/orders.json".into())
            .await
            .unwrap();
        assert!(
            l.renew(&a, t(2)).await.unwrap_err().is_conflict(),
            "the holder sees the delete as a loss"
        );
        assert_eq!(mine(&l, "orders", "c").await.doc.owner_epoch, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_acquirers_have_one_winner() {
        for released_first in [false, true] {
            let l = leases();
            if released_first {
                let a = mine(&l, "orders", "a").await;
                l.release(&a, t(1)).await.unwrap();
            }
            let tasks: Vec<_> = (0..8)
                .map(|i| {
                    let l = l.clone();
                    tokio::spawn(async move {
                        l.acquire("orders", &node(&format!("n{i}")), DUR, t(2))
                            .await
                    })
                })
                .collect();
            let mut winners = vec![];
            for task in tasks {
                if let Acquired::Mine(lease) = task.await.unwrap().unwrap() {
                    winners.push(lease);
                }
            }
            assert_eq!(winners.len(), 1, "released_first={released_first}");
            let stored = l.observe("orders").await.unwrap().unwrap();
            assert_eq!(stored, winners[0]);
            assert_eq!(stored.doc.owner_epoch, if released_first { 2 } else { 1 });
        }
    }

    #[test]
    fn lease_keys_escape_the_pipeline_id() {
        assert_eq!(lease_key("orders-pg"), "leases/orders-pg.json");
        assert_eq!(lease_key("a/b"), "leases/a%2Fb.json");
    }
}
```

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index fa6cbae..c381676 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -3,5 +3,7 @@
 //! `version`, written with compare-and-swap through `data-avalanche-storage`.
 
 mod doc;
+mod lease;
 
 pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
+pub use lease::{Acquired, Holder, LeaseDoc, Leases, lease_key};
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `lease::Acquired`, `lease::Holder`, `lease::LeaseDoc`, `lease::Leases`, `lease::lease_key`
error[E0425]: cannot find type `Duration` in this scope
error[E0433]: cannot find type `Duration` in this scope
error[E0425]: cannot find type `Leases` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-state/src/doc.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/doc.rs b/crates/data-avalanche-state/src/doc.rs
index ec26d57..26a72d3 100644
--- a/crates/data-avalanche-state/src/doc.rs
+++ b/crates/data-avalanche-state/src/doc.rs
@@ -52,6 +52,12 @@ impl StateError {
             Self::Store(StoreError::Conflict { .. } | StoreError::Contention { .. })
         )
     }
+
+    /// True when a conditional write definitely lost (`Conflict`), as opposed to an unknown
+    /// outcome (`Contention`).
+    pub fn is_conflict(&self) -> bool {
+        matches!(self, Self::Store(StoreError::Conflict { .. }))
+    }
 }
 
 #[derive(Serialize)]
```

In `crates/data-avalanche-state/src/lease.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! The per-pipeline lease object `leases/<pipeline_id>.json` in `globals.state` (Clustering §5.2)
//! and its compare-and-swap operations. The object version (ETag / GCS generation) is the real
//! lease handle; the timestamps are informational. Timing — when to renew, how long to observe
//! before a takeover, the safety deadline — belongs to the renewer (Clustering §5.3, §19).

use std::time::Duration;

use chrono::{DateTime, Utc};
use data_avalanche_storage::escape_segment;
use serde::{Deserialize, Serialize};

use crate::doc::{DocStore, Document, StateError, Versioned};

/// A lease as stored. `released: true` is the tombstone a planned release leaves behind: the next
/// acquirer takes it without waiting, and `owner_epoch` carries on from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseDoc {
    pub pipeline_id: String,
    pub holder_node_id: String,
    pub holder_incarnation: u64,
    /// +1 on every acquisition, including over a released tombstone (the app-layer fence, §5.5).
    pub owner_epoch: u64,
    pub acquired_at: DateTime<Utc>,
    pub renewed_at: DateTime<Utc>,
    pub lease_duration_ms: u64,
    pub released: bool,
}

impl Document for LeaseDoc {
    const KIND: &'static str = "lease";
    const SCHEMA_VERSION: u64 = 1;
}

/// A node as a lease holder: its `node_id` and current incarnation (Clustering §3.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    pub node_id: String,
    pub incarnation: u64,
}

impl LeaseDoc {
    /// True when `holder` holds this lease (and it isn't released).
    pub fn is_held_by(&self, holder: &Holder) -> bool {
        !self.released
            && self.holder_node_id == holder.node_id
            && self.holder_incarnation == holder.incarnation
    }
}

/// The result of [`Leases::acquire`].
#[derive(Debug, Clone, PartialEq)]
pub enum Acquired {
    /// This node now holds the lease.
    Mine(Versioned<LeaseDoc>),
    /// Another holder has it; the caller may observe it and consider a takeover (§5.3).
    HeldBy(Versioned<LeaseDoc>),
}

/// The lease operations over the `globals.state` documents.
#[derive(Debug, Clone)]
pub struct Leases {
    docs: DocStore,
}

/// `leases/<pipeline_id>.json`.
pub fn lease_key(pipeline_id: &str) -> String {
    format!("leases/{}.json", escape_segment(pipeline_id))
}

impl Leases {
    /// `docs` is rooted at the `globals.state` location.
    pub fn new(docs: DocStore) -> Self {
        Self { docs }
    }

    fn new_doc(
        pipeline_id: &str,
        holder: &Holder,
        epoch: u64,
        lease_duration: Duration,
        now: DateTime<Utc>,
    ) -> LeaseDoc {
        LeaseDoc {
            pipeline_id: pipeline_id.to_owned(),
            holder_node_id: holder.node_id.clone(),
            holder_incarnation: holder.incarnation,
            owner_epoch: epoch,
            acquired_at: now,
            renewed_at: now,
            lease_duration_ms: u64::try_from(lease_duration.as_millis()).unwrap_or(u64::MAX),
            released: false,
        }
    }

    /// Acquires a free lease: creates it (epoch 1) when absent, or takes a released tombstone
    /// (epoch + 1). A live holder is left alone and returned as [`Acquired::HeldBy`].
    ///
    /// # Errors
    /// A storage or decode failure, or a race error when another acquirer won the tombstone and
    /// the lease changed again before it could be read back.
    pub async fn acquire(
        &self,
        pipeline_id: &str,
        holder: &Holder,
        lease_duration: Duration,
        now: DateTime<Utc>,
    ) -> Result<Acquired, StateError> {
        let key = lease_key(pipeline_id);
        let mut attempts = 0;
        loop {
            attempts += 1;
            let current = self.docs.load::<LeaseDoc>(&key).await?;
            let written = match &current {
                None => {
                    let doc = Self::new_doc(pipeline_id, holder, 1, lease_duration, now);
                    self.docs
                        .create(&key, &doc)
                        .await
                        .map(|v| Versioned { doc, version: v })
                }
                Some(seen) if seen.doc.released => {
                    let epoch = seen.doc.owner_epoch + 1;
                    let doc = Self::new_doc(pipeline_id, holder, epoch, lease_duration, now);
                    self.docs
                        .update(&key, &seen.version, &doc)
                        .await
                        .map(|v| Versioned { doc, version: v })
                }
                Some(seen) => return Ok(Acquired::HeldBy(seen.clone())),
            };
            match written {
                Ok(lease) => return Ok(Acquired::Mine(lease)),
                // Someone else created or took it first: read again; a live winner is reported
                // as the holder on the next pass.
                Err(e) if e.is_race() && attempts < 3 => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Renews a held lease: a CAS write from `lease.version` with a new `renewed_at`.
    ///
    /// # Errors
    /// `Store(Conflict)` means the lease was taken or deleted (lost); `Store(Contention)` means the
    /// outcome is unknown — observe it and check [`LeaseDoc::is_held_by`] (§19.2).
    pub async fn renew(
        &self,
        lease: &Versioned<LeaseDoc>,
        now: DateTime<Utc>,
    ) -> Result<Versioned<LeaseDoc>, StateError> {
        let doc = LeaseDoc {
            renewed_at: now,
            ..lease.doc.clone()
        };
        self.write(lease, doc).await
    }

    /// The lease as stored now, or `None` when it doesn't exist.
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn observe(
        &self,
        pipeline_id: &str,
    ) -> Result<Option<Versioned<LeaseDoc>>, StateError> {
        self.docs.load(&lease_key(pipeline_id)).await
    }

    /// Takes over a lease the caller has observed unchanged for a full lease duration (§5.3): a
    /// CAS write from `observed.version` naming `holder`, with `owner_epoch + 1`.
    ///
    /// # Errors
    /// `Store(Conflict)` when the lease moved since it was observed (the holder renewed, or
    /// another challenger won).
    pub async fn take_over(
        &self,
        observed: &Versioned<LeaseDoc>,
        holder: &Holder,
        lease_duration: Duration,
        now: DateTime<Utc>,
    ) -> Result<Versioned<LeaseDoc>, StateError> {
        let epoch = observed.doc.owner_epoch + 1;
        let doc = Self::new_doc(
            &observed.doc.pipeline_id,
            holder,
            epoch,
            lease_duration,
            now,
        );
        self.write(observed, doc).await
    }

    /// Releases a held lease by CAS-writing its tombstone (object_store has no conditional
    /// delete). The epoch is kept, so the next acquisition continues from it.
    ///
    /// # Errors
    /// `Store(Conflict)` when the lease is no longer the one held.
    pub async fn release(
        &self,
        lease: &Versioned<LeaseDoc>,
        now: DateTime<Utc>,
    ) -> Result<Versioned<LeaseDoc>, StateError> {
        let doc = LeaseDoc {
            renewed_at: now,
            released: true,
            ..lease.doc.clone()
        };
        self.write(lease, doc).await
    }

    async fn write(
        &self,
        from: &Versioned<LeaseDoc>,
        doc: LeaseDoc,
    ) -> Result<Versioned<LeaseDoc>, StateError> {
        let key = lease_key(&doc.pipeline_id);
        let version = self.docs.update(&key, &from.version, &doc).await?;
        Ok(Versioned { doc, version })
    }
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state`
Expected: PASS. `data-avalanche-state` unit tests 18; `tests/races.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state/Cargo.toml \
  crates/data-avalanche-state/src/lease.rs \
  crates/data-avalanche-state/src/lib.rs \
  crates/data-avalanche-state/src/doc.rs \
  Cargo.lock
git commit -m "feat(state): lease document and CAS operations"
```


---

### Task 6: T1–T10 lease conformance on every backend; `cloud.yml`

**Files:**
- Create: `.github/workflows/cloud.yml`
- Modify: `crates/data-avalanche-state/Cargo.toml`, `crates/data-avalanche-testkit/src/lib.rs`, `test-infra/README.md`
- Test: `crates/data-avalanche-state/tests/lease_conformance.rs`

**Interfaces:**
- Consumes: `Leases`, `Acquired`, `Holder` (Task 5); `open_bucket`, `BucketSpec`, `Backend`,
  `Timeouts::COORDINATION` (Task 3); testkit `stores`, `delete_prefix`.
- Produces:
  - testkit: `cloud_enabled(&str) -> bool` (reads `DATA_AVALANCHE_CLOUD` as a comma list),
    `require_cloud!("s3" | "azure")` and `required_env(&str) -> String` (panics naming a missing
    variable);
  - `.github/workflows/cloud.yml`;
  - the README tables.

T1–T10 follow Clustering §17.4 and the prototype, with these changes:
- T5 and T9 use 8 concurrent `take_over`s from one version. T9 runs 5 rounds and allows only
  `Conflict` / `Contention` for the losers.
- T10 also checks that the token the backend compares is present (`e_tag`, or `version` on GCS).

The test runs in memory on every `cargo test`, which makes it the hermetic regression suite for
the lease.

Secrets the repository needs for `cloud.yml` (set by the owner before the exit run):
- **S3:** variables `DATA_AVALANCHE_S3_BUCKET`, `DATA_AVALANCHE_S3_REGION`; secrets
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.
- **Azure:** variables `DATA_AVALANCHE_AZURE_CONTAINER`, `DATA_AVALANCHE_AZURE_ACCOUNT`; secrets
  `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-state/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-state/Cargo.toml b/crates/data-avalanche-state/Cargo.toml
index 255625a..e25b0c6 100644
--- a/crates/data-avalanche-state/Cargo.toml
+++ b/crates/data-avalanche-state/Cargo.toml
@@ -18,6 +18,7 @@ serde_json.workspace = true
 thiserror.workspace = true
 
 [dev-dependencies]
+futures-util.workspace = true
 data-avalanche-testkit.workspace = true
 object_store.workspace = true
 tokio.workspace = true
```

Create `crates/data-avalanche-state/tests/lease_conformance.rs`:

```rust
//! T1–T10: the lease's compare-and-swap guarantees (Clustering §17.4), re-proven through
//! `object_store` and `Leases` on every backend: in memory (always), RustFS and Azurite
//! (`DATA_AVALANCHE_IT`), the real GCS bucket, and real S3 / Azure (`DATA_AVALANCHE_CLOUD`,
//! `cloud.yml`). Evidence replaces `prototypes/lease-conformance` (roadmap R-2, P-5).

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use data_avalanche_state::{Acquired, DocStore, Holder, LeaseDoc, Leases, StateError, Versioned};
use data_avalanche_storage::{
    Backend, BucketSpec, ObjectStoreVersioned, Timeouts, VersionedStore, open_bucket,
};
use data_avalanche_testkit::{
    delete_prefix, require_cloud, require_gcs, require_it, required_env, stores, unique_prefix,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;

const DUR: Duration = Duration::from_secs(15);
const WRITERS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Token {
    /// The backend compares the ETag (S3, Azure, in memory).
    ETag,
    /// The backend compares the generation (GCS).
    Generation,
}

fn node(id: impl Into<String>) -> Holder {
    Holder {
        node_id: id.into(),
        incarnation: 1,
    }
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

async fn mine(l: &Leases, id: &str, who: &str) -> Versioned<LeaseDoc> {
    match l.acquire(id, &node(who), DUR, now()).await.unwrap() {
        Acquired::Mine(lease) => lease,
        other => panic!("{id}: expected to acquire, got {other:?}"),
    }
}

/// T1 — acquire on an empty key succeeds.
async fn t1(l: &Leases) {
    let lease = mine(l, "t1", "a").await;
    assert_eq!(lease.doc.owner_epoch, 1, "T1");
}

/// T2 — acquire on a held key leaves the holder alone.
async fn t2(l: &Leases) {
    let a = mine(l, "t2", "a").await;
    let got = l.acquire("t2", &node("b"), DUR, now()).await.unwrap();
    assert_eq!(got, Acquired::HeldBy(a), "T2");
}

/// T3 — a renew with the current version advances the version.
async fn t3(l: &Leases) {
    let a0 = mine(l, "t3", "a").await;
    let a1 = l
        .renew(&a0, now())
        .await
        .expect("T3: renew with the current version");
    assert_ne!(a0.version, a1.version, "T3: the version must advance");
    assert_eq!(l.observe("t3").await.unwrap(), Some(a1), "T3");
}

/// T4 — a write with a stale version is rejected with Conflict (the core safety property).
async fn t4(l: &Leases) {
    let a0 = mine(l, "t4", "a").await;
    let a1 = l.renew(&a0, now()).await.unwrap();
    let stale = l.renew(&a0, now()).await.unwrap_err();
    assert!(stale.is_conflict(), "T4: stale renew: {stale:?}");
    let stale = l.take_over(&a0, &node("b"), DUR, now()).await.unwrap_err();
    assert!(stale.is_conflict(), "T4: stale takeover: {stale:?}");
    assert_eq!(
        l.observe("t4").await.unwrap(),
        Some(a1),
        "T4: nothing stale landed"
    );
}

/// Every writer CAS-writes from the same version at once; returns (winners, losers' errors).
async fn contend(
    l: &Leases,
    from: &Versioned<LeaseDoc>,
) -> (Vec<Versioned<LeaseDoc>>, Vec<StateError>) {
    let tasks: Vec<_> = (0..WRITERS)
        .map(|i| {
            let (l, from) = (l.clone(), from.clone());
            tokio::spawn(
                async move { l.take_over(&from, &node(format!("n{i}")), DUR, now()).await },
            )
        })
        .collect();
    let (mut wins, mut losses) = (vec![], vec![]);
    for r in futures_util::future::join_all(tasks).await {
        match r.unwrap() {
            Ok(v) => wins.push(v),
            Err(e) => losses.push(e),
        }
    }
    (wins, losses)
}

/// T5 — N concurrent writes from one version: exactly one wins (single writer).
async fn t5(l: &Leases) {
    let a = mine(l, "t5", "a").await;
    let (wins, losses) = contend(l, &a).await;
    assert_eq!(
        wins.len(),
        1,
        "T5: exactly one concurrent swap may win; losers: {losses:?}"
    );
    assert_eq!(
        l.observe("t5").await.unwrap(),
        Some(wins[0].clone()),
        "T5: the winner is stored"
    );
}

/// T6 — release with the current version leaves a tombstone; acquire then succeeds.
async fn t6(l: &Leases) {
    let a = mine(l, "t6", "a").await;
    let tomb = l.release(&a, now()).await.expect("T6: release");
    assert!(tomb.doc.released, "T6");
    let b = mine(l, "t6", "b").await;
    assert_eq!(
        b.doc.owner_epoch, 2,
        "T6: the epoch continues past the tombstone"
    );
}

/// T7 — release with a stale version is rejected.
async fn t7(l: &Leases) {
    let a0 = mine(l, "t7", "a").await;
    let a1 = l.renew(&a0, now()).await.unwrap();
    let e = l.release(&a0, now()).await.unwrap_err();
    assert!(e.is_conflict(), "T7: {e:?}");
    assert_eq!(l.observe("t7").await.unwrap(), Some(a1), "T7: still held");
}

/// T8 — re-acquire right after a release, with no wait (planned handoff).
async fn t8(l: &Leases) {
    let a = mine(l, "t8", "a").await;
    l.release(&a, now()).await.unwrap();
    let b = l.acquire("t8", &node("b"), DUR, now()).await.unwrap();
    assert!(
        matches!(&b, Acquired::Mine(v) if v.doc.holder_node_id == "b"),
        "T8: {b:?}"
    );
}

/// T9 — contention never surfaces as a hard error: over several rounds of concurrent writes,
/// every loser gets Conflict or Contention (S3 409), never Transient or Permanent.
async fn t9(l: &Leases) {
    let mut current = mine(l, "t9", "a").await;
    for round in 0..5 {
        let (wins, losses) = contend(l, &current).await;
        for e in &losses {
            assert!(
                e.is_race(),
                "T9 round {round}: a loser got a hard error: {e:?}"
            );
        }
        assert!(wins.len() <= 1, "T9 round {round}: {} winners", wins.len());
        current = l.observe("t9").await.unwrap().unwrap();
    }
}

/// T10 — the round-tripped token is the one the backend compares (GCS: the generation): the
/// pre-write token fails after the object advanced, the post-write token succeeds.
async fn t10(l: &Leases, token: Token) {
    let v0 = mine(l, "t10", "a").await;
    let present = match token {
        Token::ETag => v0.version.e_tag.is_some(),
        Token::Generation => v0.version.version.is_some(),
    };
    assert!(present, "T10: {token:?} missing from {:?}", v0.version);
    let v1 = l.renew(&v0, now()).await.expect("T10: current token");
    assert!(
        l.renew(&v0, now()).await.unwrap_err().is_conflict(),
        "T10: pre-write token"
    );
    let v2 = l.renew(&v1, now()).await.expect("T10: post-write token");
    assert_ne!(v1.version, v2.version, "T10");
}

async fn suite(store: Arc<dyn ObjectStore>, root: &str, token: Token) {
    let versioned: Arc<dyn VersionedStore> =
        Arc::new(ObjectStoreVersioned::new(store, root).unwrap());
    let l = Leases::new(DocStore::new(versioned));
    t1(&l).await;
    t2(&l).await;
    t3(&l).await;
    t4(&l).await;
    t5(&l).await;
    t6(&l).await;
    t7(&l).await;
    t8(&l).await;
    t9(&l).await;
    t10(&l, token).await;
}

async fn suite_and_cleanup(store: Arc<dyn ObjectStore>, test: &str, token: Token) {
    let prefix = unique_prefix(test);
    suite(store.clone(), &prefix, token).await;
    delete_prefix(store.as_ref(), &prefix).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_memory() {
    suite(Arc::new(InMemory::new()), "globals", Token::ETag).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs() {
    require_it!();
    suite_and_cleanup(stores::rustfs(), "lease_rustfs", Token::ETag).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn azurite() {
    require_it!();
    suite_and_cleanup(stores::azurite(), "lease_azurite", Token::ETag).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_gcs() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    suite_and_cleanup(stores::gcs(), "lease_gcs", Token::Generation).await;
}

fn cloud_bucket(backend: Backend, bucket_var: &str) -> Arc<dyn ObjectStore> {
    let spec = BucketSpec {
        backend,
        bucket: required_env(bucket_var),
        options: vec![],
        timeouts: Timeouts::COORDINATION,
    };
    open_bucket(&spec).unwrap_or_else(|e| panic!("{e}"))
}

/// Real S3 through the production path (`open_bucket`, AWS default credential chain).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_s3() {
    require_cloud!("s3");
    data_avalanche_testkit::install_crypto_provider();
    let store = cloud_bucket(Backend::S3, "DATA_AVALANCHE_S3_BUCKET");
    suite_and_cleanup(store, "lease_s3", Token::ETag).await;
}

/// Real Azure Blob through the production path (`open_bucket`, `AZURE_*` credentials).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_azure() {
    require_cloud!("azure");
    data_avalanche_testkit::install_crypto_provider();
    let store = cloud_bucket(
        Backend::Azure { account: None },
        "DATA_AVALANCHE_AZURE_CONTAINER",
    );
    suite_and_cleanup(store, "lease_azure", Token::ETag).await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state -p data-avalanche-testkit`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `data_avalanche_testkit::require_cloud`, `data_avalanche_testkit::required_env`
```

- [ ] **Step 3: Implement**

Create `.github/workflows/cloud.yml`:

```yaml
name: cloud

# Real-cloud conformance (roadmap §5.4, P-5): the T1–T10 lease suite on real S3 and real Azure Blob,
# through the production `open_bucket` path. GCS already runs in ci.yml. Requirements are in
# test-infra/README.md, "Real S3 and Azure (cloud.yml)". Credentials are injected as env vars and
# never printed.

on:
  schedule:
    - cron: "17 4 * * 1"
  workflow_dispatch:

permissions:
  contents: read

env:
  CARGO_TERM_COLOR: always

jobs:
  s3:
    runs-on: ubuntu-latest
    env:
      DATA_AVALANCHE_CLOUD: s3
      DATA_AVALANCHE_S3_BUCKET: ${{ vars.DATA_AVALANCHE_S3_BUCKET }}
      AWS_REGION: ${{ vars.DATA_AVALANCHE_S3_REGION }}
      AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    steps:
      - uses: actions/checkout@v7
      - uses: actions-rust-lang/setup-rust-toolchain@v2
      - name: Lease conformance on real S3
        run: cargo test --locked -p data-avalanche-state --test lease_conformance real_s3 -- --nocapture

  azure:
    runs-on: ubuntu-latest
    env:
      DATA_AVALANCHE_CLOUD: azure
      DATA_AVALANCHE_AZURE_CONTAINER: ${{ vars.DATA_AVALANCHE_AZURE_CONTAINER }}
      AZURE_STORAGE_ACCOUNT_NAME: ${{ vars.DATA_AVALANCHE_AZURE_ACCOUNT }}
      AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
      AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
      AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
    steps:
      - uses: actions/checkout@v7
      - uses: actions-rust-lang/setup-rust-toolchain@v2
      - name: Lease conformance on real Azure Blob
        run: cargo test --locked -p data-avalanche-state --test lease_conformance real_azure -- --nocapture
```

Change `crates/data-avalanche-testkit/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-testkit/src/lib.rs b/crates/data-avalanche-testkit/src/lib.rs
index 8486855..95bee26 100644
--- a/crates/data-avalanche-testkit/src/lib.rs
+++ b/crates/data-avalanche-testkit/src/lib.rs
@@ -25,6 +25,31 @@ pub fn gcs_enabled() -> bool {
     it_enabled() && !env_set("DATA_AVALANCHE_SKIP_GCS")
 }
 
+/// True when `DATA_AVALANCHE_CLOUD` (a comma-separated list, e.g. `s3` or `s3,azure`) names
+/// `cloud`. Real-cloud tests run only in `cloud.yml` or by hand, never in `ci.yml`.
+pub fn cloud_enabled(cloud: &str) -> bool {
+    std::env::var("DATA_AVALANCHE_CLOUD").is_ok_and(|list| names(&list, cloud))
+}
+
+fn names(list: &str, cloud: &str) -> bool {
+    list.split(',').any(|c| c.trim() == cloud)
+}
+
+/// Returns early unless `DATA_AVALANCHE_CLOUD` names this cloud (`"s3"` or `"azure"`).
+#[macro_export]
+macro_rules! require_cloud {
+    ($cloud:literal) => {
+        if !$crate::cloud_enabled($cloud) {
+            eprintln!(concat!(
+                "skipped: set DATA_AVALANCHE_CLOUD=",
+                $cloud,
+                " with its credentials"
+            ));
+            return;
+        }
+    };
+}
+
 /// Returns early from a test when integration tests are disabled.
 #[macro_export]
 macro_rules! require_it {
@@ -77,6 +102,17 @@ pub async fn delete_prefix(store: &dyn ObjectStore, prefix: &str) {
     drop(deleted);
 }
 
+/// The value of a required environment variable for a real-cloud test.
+///
+/// # Panics
+/// When it is unset or empty, naming it.
+pub fn required_env(name: &str) -> String {
+    std::env::var(name)
+        .ok()
+        .filter(|v| !v.is_empty())
+        .unwrap_or_else(|| panic!("{name} is not set (see test-infra/README.md)"))
+}
+
 /// Fixed endpoints from `test-infra/compose.yaml`.
 pub mod endpoints {
     pub const POSTGRES_18: &str =
@@ -137,3 +173,17 @@ pub mod stores {
         Arc::new(store)
     }
 }
+
+#[cfg(test)]
+mod tests {
+    use super::names;
+
+    #[test]
+    fn cloud_list_parsing() {
+        assert!(names("s3", "s3"));
+        assert!(names("s3, azure", "azure"));
+        assert!(!names("", "s3"));
+        assert!(!names("s3x", "s3"));
+        assert!(!names("azure", "s3"));
+    }
+}
```

Change `test-infra/README.md`:

```diff
diff --git a/test-infra/README.md b/test-infra/README.md
index 1dc3cbf..7be9bc5 100644
--- a/test-infra/README.md
+++ b/test-infra/README.md
@@ -56,6 +56,41 @@ GCS on merge. On pushes and same-repository pull requests, a missing `GCP_SA_KEY
 prints an `::error::` and fails the integration job. S3 and Azure need no CI secrets because they
 run on the emulators above.
 
+## Real S3 and Azure (`cloud.yml`)
+
+Real S3 and Azure Blob run only in `.github/workflows/cloud.yml` (weekly and on manual dispatch) or by
+hand, never in `ci.yml`. A test runs when `DATA_AVALANCHE_CLOUD` names its cloud (`s3`, `azure`, or
+`s3,azure`); it opens the bucket through `open_bucket`, so credentials come from the standard
+chains. Tests write only under `data-avalanche-it/<test>/<uuid>/` and delete what they wrote.
+
+| Name | Kind | Value |
+|---|---|---|
+| `DATA_AVALANCHE_S3_BUCKET` | repository **variable** | S3 bucket for the tests (versioning off; lifecycle rule on `data-avalanche-it/`, 1 day). |
+| `DATA_AVALANCHE_S3_REGION` | repository **variable** | Its region, exported as `AWS_REGION`. |
+| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | repository **secrets** | An IAM user limited to `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on the bucket and `s3:ListBucket` on it. |
+| `DATA_AVALANCHE_AZURE_CONTAINER` | repository **variable** | Blob container for the tests (lifecycle rule on `data-avalanche-it/`, 1 day). |
+| `DATA_AVALANCHE_AZURE_ACCOUNT` | repository **variable** | The storage account name, exported as `AZURE_STORAGE_ACCOUNT_NAME`. |
+| `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` | repository **secrets** | A service principal with `Storage Blob Data Contributor` on the container. |
+
+By hand: `DATA_AVALANCHE_CLOUD=s3 DATA_AVALANCHE_S3_BUCKET=<bucket> AWS_REGION=<region> cargo test -p
+data-avalanche-state --test lease_conformance real_s3 -- --nocapture` with AWS credentials in the
+environment or profile (`azure` / `real_azure` likewise, with `AZURE_*`).
+
+## Lease conformance T1–T10 (M2)
+
+`crates/data-avalanche-state/tests/lease_conformance.rs` runs T1–T10 (Clustering §17.4) through
+`object_store` and `Leases` on each backend. It replaces `prototypes/lease-conformance` as the
+evidence the lease rests on (roadmap R-2).
+
+| Backend | Result | Measured | Command |
+|---|---|---|---|
+| In memory | recorded at M2 exit | | `cargo test -p data-avalanche-state --test lease_conformance in_memory` |
+| RustFS 1.0.0 | recorded at M2 exit | | `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-state --test lease_conformance rustfs` |
+| Azurite 3.37.0 | recorded at M2 exit | | same, `azurite` |
+| Real GCS | recorded at M2 exit | | `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_GCS_BUCKET=<bucket> GOOGLE_SERVICE_ACCOUNT_PATH=<key.json> cargo test -p data-avalanche-state --test lease_conformance real_gcs` |
+| Real S3 | recorded at M2 exit | | `cloud.yml`, job `s3` |
+| Real Azure Blob | recorded at M2 exit | | `cloud.yml`, job `azure` |
+
 ## Storage conditional-write results (M0 Task 12)
 
 | Backend | Create-if-absent | Update-if-version | Measured | Consequence for M2 |
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state -p data-avalanche-testkit`
Expected: PASS. `data-avalanche-state` unit tests 18; `tests/lease_conformance.rs` 6; `tests/races.rs` 3; `data-avalanche-testkit` unit tests 1; `tests/smoke.rs` 3; `tests/storage_cas.rs` 3.

Then the emulators (`test-infra/up.sh` first):

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-state --test lease_conformance -- --nocapture`
Expected: PASS, with RustFS and Azurite really exercised: the only `skipped:` lines are for GCS
(and, in Task 6, for real S3 / Azure). With GCS credentials, drop `DATA_AVALANCHE_SKIP_GCS` and set
`DATA_AVALANCHE_GCS_BUCKET` + `GOOGLE_SERVICE_ACCOUNT_PATH`; the `real_gcs*` tests must PASS too.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state/Cargo.toml \
  crates/data-avalanche-state/tests/lease_conformance.rs \
  .github/workflows/cloud.yml \
  crates/data-avalanche-testkit/src/lib.rs \
  test-infra/README.md \
  Cargo.lock
git commit -m "test(state): T1–T10 lease conformance on every backend, cloud.yml"
```


---

### Task 7: Cluster anchor — first writer wins

**Files:**
- Create: `crates/data-avalanche-state/src/anchor.rs`
- Modify: `crates/data-avalanche-state/src/lib.rs`

**Interfaces:**
- Consumes: `DocStore`, `Document` (Task 4).
- Produces:
  - `ANCHOR_KEY = "cluster/anchor.json"`.
  - `HashAlgo::Xxh3`.
  - `AnchorDoc { tokens: u32, lease_duration_ms: u64, lock_backend: String, hash_algo: HashAlgo,
    fingerprint_key: [u8; 32], established_by: String, established_at: DateTime<Utc> }`. `Debug`
    hides the key.
  - `Anchor { doc: AnchorDoc, created: bool }`.
  - `establish_anchor(&DocStore, &AnchorDoc) -> Result<Anchor, StateError>`.

The proposal's `fingerprint_key` comes from `data_avalanche_config::FingerprintKey::random()`, and
the running config is loaded with `FingerprintKey::from_bytes(anchor.fingerprint_key)` (Task 11).
D-M2.10.

- [ ] **Step 1: Write the failing tests**

Create `crates/data-avalanche-state/src/anchor.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;

    fn docs() -> DocStore {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        DocStore::new(Arc::new(store))
    }

    fn proposal(node: &str, key_byte: u8) -> AnchorDoc {
        AnchorDoc {
            tokens: 128,
            lease_duration_ms: 15_000,
            lock_backend: "storage".into(),
            hash_algo: HashAlgo::Xxh3,
            fingerprint_key: [key_byte; 32],
            established_by: node.into(),
            established_at: Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, 0).unwrap(),
        }
    }

    #[tokio::test]
    async fn first_writer_wins() {
        let d = docs();
        let a = establish_anchor(&d, &proposal("a", 1)).await.unwrap();
        assert_eq!(
            a,
            Anchor {
                doc: proposal("a", 1),
                created: true
            }
        );
        let b = establish_anchor(&d, &proposal("b", 2)).await.unwrap();
        assert_eq!(
            b,
            Anchor {
                doc: proposal("a", 1),
                created: false
            },
            "the anchor never changes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_establishers_agree_on_one_anchor() {
        let d = docs();
        let tasks: Vec<_> = (0..8u8)
            .map(|i| {
                let d = d.clone();
                tokio::spawn(
                    async move { establish_anchor(&d, &proposal(&format!("n{i}"), i)).await },
                )
            })
            .collect();
        let mut results = vec![];
        for t in tasks {
            results.push(t.await.unwrap().unwrap());
        }
        assert_eq!(results.iter().filter(|a| a.created).count(), 1);
        let stored = d.load::<AnchorDoc>(ANCHOR_KEY).await.unwrap().unwrap().doc;
        assert!(
            results.iter().all(|a| a.doc == stored),
            "every node sees the stored anchor"
        );
    }

    #[test]
    fn anchor_json_shape() {
        let mut doc = proposal("worker-7f3a9c", 0);
        doc.fingerprint_key[0] = 0xab;
        doc.fingerprint_key[31] = 0x01;
        let json = String::from_utf8(crate::doc::encode(&doc).to_vec()).unwrap();
        assert_eq!(
            json,
            format!(
                r#"{{
  "version": 1,
  "tokens": 128,
  "lease_duration_ms": 15000,
  "lock_backend": "storage",
  "hash_algo": "xxh3",
  "fingerprint_key": "ab{}01",
  "established_by": "worker-7f3a9c",
  "established_at": "2026-09-28T10:00:00Z"
}}"#,
                "0".repeat(60)
            )
        );
        assert_eq!(
            crate::doc::decode::<AnchorDoc>("k", json.as_bytes()).unwrap(),
            doc
        );
    }

    #[tokio::test]
    async fn a_bad_stored_anchor_is_refused_without_echoing_the_key() {
        let d = docs();
        let good = String::from_utf8(crate::doc::encode(&proposal("a", 0x5c)).to_vec()).unwrap();
        let key_hex = "5c".repeat(32);
        for bad in [
            good.replace(&key_hex, &format!("{}Z", &key_hex[..63])),
            good.replace(&key_hex, &key_hex[..62]),
            good.replace(&key_hex, &key_hex.to_uppercase()),
            good.replace("xxh3", "murmur3"),
        ] {
            d.store()
                .put(ANCHOR_KEY, Bytes::from(bad.clone()))
                .await
                .unwrap();
            let e = establish_anchor(&d, &proposal("b", 1)).await.unwrap_err();
            assert!(matches!(e, StateError::Corrupt { .. }), "{bad}: {e:?}");
            let text = e.to_string();
            assert!(!text.contains("5c5c") && !text.contains("5C5C"), "{text}");
        }
    }

    #[test]
    fn debug_hides_the_fingerprint_key() {
        let text = format!("{:?}", proposal("a", 0x5c));
        assert!(
            text.contains("fingerprint_key: \"***\"") && !text.contains("92"),
            "{text}"
        );
    }
}
```

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index c381676..7cb0688 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -2,8 +2,10 @@
 //! and the `globals.state` documents (Clustering §12), each a JSON [`Document`] with a schema
 //! `version`, written with compare-and-swap through `data-avalanche-storage`.
 
+mod anchor;
 mod doc;
 mod lease;
 
+pub use anchor::{ANCHOR_KEY, Anchor, AnchorDoc, HashAlgo, establish_anchor};
 pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
 pub use lease::{Acquired, Holder, LeaseDoc, Leases, lease_key};
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `anchor::ANCHOR_KEY`, `anchor::Anchor`, `anchor::AnchorDoc`, `anchor::HashAlgo`, `anchor::establish_anchor`
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-state/src/anchor.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! The cluster anchor `cluster/anchor.json` in `globals.state` (Clustering §12, config §6.2): the
//! cluster-wide settings every node must share, fixed by the first node to start (first writer
//! wins). A node whose config disagrees refuses to start (V-062, V-076, V-094 — checked by the
//! config crate's `check_cluster_anchor`).

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::doc::{DocStore, Document, StateError};

pub const ANCHOR_KEY: &str = "cluster/anchor.json";

/// The placement hash every node uses to build the ring. v1 has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgo {
    Xxh3,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnchorDoc {
    pub tokens: u32,
    pub lease_duration_ms: u64,
    /// `globals.cluster.lock_backend` as written in config (`storage`).
    pub lock_backend: String,
    pub hash_algo: HashAlgo,
    /// The key for secret and file fingerprints (config §6.2), 64 lowercase hex digits on disk.
    #[serde(serialize_with = "hex_out", deserialize_with = "hex_in")]
    pub fingerprint_key: [u8; 32],
    pub established_by: String,
    pub established_at: DateTime<Utc>,
}

impl Document for AnchorDoc {
    const KIND: &'static str = "cluster anchor";
    const SCHEMA_VERSION: u64 = 1;
}

impl fmt::Debug for AnchorDoc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnchorDoc")
            .field("tokens", &self.tokens)
            .field("lease_duration_ms", &self.lease_duration_ms)
            .field("lock_backend", &self.lock_backend)
            .field("hash_algo", &self.hash_algo)
            .field("fingerprint_key", &"***")
            .field("established_by", &self.established_by)
            .field("established_at", &self.established_at)
            .finish()
    }
}

fn hex_out<S: Serializer>(key: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&key.iter().map(|b| format!("{b:02x}")).collect::<String>())
}

fn hex_in<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
    let text = String::deserialize(d)?;
    let invalid = || serde::de::Error::custom("fingerprint_key must be 64 lowercase hex digits");
    if text.len() != 64
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    let mut key = [0u8; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).map_err(|_| invalid())?;
    }
    Ok(key)
}

/// What [`establish_anchor`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anchor {
    pub doc: AnchorDoc,
    /// True when this call created it; false when it existed (or another node won the race).
    pub created: bool,
}

/// Returns the cluster anchor, creating it from `proposal` when there is none. When several
/// nodes race, exactly one creates it and all of them return that one.
///
/// # Errors
/// A storage failure, or a stored anchor that isn't a valid document.
pub async fn establish_anchor(docs: &DocStore, proposal: &AnchorDoc) -> Result<Anchor, StateError> {
    let mut attempts = 0;
    loop {
        attempts += 1;
        if let Some(existing) = docs.load::<AnchorDoc>(ANCHOR_KEY).await? {
            return Ok(Anchor {
                doc: existing.doc,
                created: false,
            });
        }
        match docs.create(ANCHOR_KEY, proposal).await {
            Ok(_) => {
                return Ok(Anchor {
                    doc: proposal.clone(),
                    created: true,
                });
            }
            Err(e) if e.is_race() && attempts < 3 => continue,
            Err(e) => return Err(e),
        }
    }
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state`
Expected: PASS. `data-avalanche-state` unit tests 23; `tests/lease_conformance.rs` 6; `tests/races.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state/src/anchor.rs \
  crates/data-avalanche-state/src/lib.rs
git commit -m "feat(state): cluster anchor, first writer wins"
```


---

### Task 8: Config generations — CAS pointer and content-addressed snapshots

**Files:**
- Create: `crates/data-avalanche-state/src/generation.rs`
- Modify: `crates/data-avalanche-state/Cargo.toml`, `crates/data-avalanche-state/src/lib.rs`

**Interfaces:**
- Consumes: `DocStore` (Task 4); `blake3`.
- Produces:
  - `GENERATION_KEY = "config/generation.json"`.
  - `GenerationDoc { generation: u64, hash: String, written_by: String, written_at }`.
  - `ConfigGeneration { pointer: Versioned<GenerationDoc>, text: String }`.
  - `config_hash(&str) -> String` (`blake3:<hex>`).
  - `config_snapshot_key(generation, hash) -> String`.
  - `ConfigGenerations::new(DocStore)` with:
    - `current() -> Result<Option<ConfigGeneration>>`;
    - `bootstrap_or_adopt(text, node_id, now) -> Result<(ConfigGeneration, bool)>`;
    - `publish(&Versioned<GenerationDoc>, text, node_id, now) -> Result<ConfigGeneration>`.

Cold start is Clustering §9.3: adopt the cloud generation, and use the file only at first
bootstrap. Reload is §9.2: publish from the generation the diff was computed against; a
`Conflict` means re-read and re-validate. D-M2.11.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-state/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-state/Cargo.toml b/crates/data-avalanche-state/Cargo.toml
index e25b0c6..3a1d573 100644
--- a/crates/data-avalanche-state/Cargo.toml
+++ b/crates/data-avalanche-state/Cargo.toml
@@ -10,6 +10,7 @@ description = "Durable state for data-avalanche: per-pipeline documents, leases
 workspace = true
 
 [dependencies]
+blake3.workspace = true
 bytes.workspace = true
 chrono.workspace = true
 data-avalanche-storage.workspace = true
```

Create `crates/data-avalanche-state/src/generation.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;

    fn gens() -> ConfigGenerations {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "globals").unwrap();
        ConfigGenerations::new(DocStore::new(Arc::new(store)))
    }

    fn t(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, s).unwrap()
    }

    const V1: &str = "globals:\n  state:\n    bucket: ops\n";
    const V2: &str = "globals:\n  state:\n    bucket: ops\n# reload\n";

    #[test]
    fn keys_and_hashes() {
        let h = config_hash("x");
        assert_eq!(h, format!("blake3:{}", blake3::hash(b"x").to_hex()));
        assert_eq!(
            config_snapshot_key(1, &h),
            format!("config/snapshots/000001-{}.yaml", &h[7..])
        );
        assert_eq!(
            config_snapshot_key(1_234_567, "blake3:ab"),
            "config/snapshots/1234567-ab.yaml"
        );
    }

    #[tokio::test]
    async fn bootstrap_creates_generation_1_then_later_starts_adopt_it() {
        let g = gens();
        assert!(g.current().await.unwrap().is_none());
        let (first, created) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        assert!(created);
        assert_eq!(
            first.pointer.doc,
            GenerationDoc {
                generation: 1,
                hash: config_hash(V1),
                written_by: "a".into(),
                written_at: t(0)
            }
        );
        assert_eq!(first.text, V1);
        let (adopted, created) = g.bootstrap_or_adopt(V2, "b", t(1)).await.unwrap();
        assert!(
            !created,
            "a started node adopts the cloud generation, not its own file (§9.3)"
        );
        assert_eq!(adopted, first);
        assert_eq!(g.current().await.unwrap(), Some(first));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_bootstraps_adopt_one_winner() {
        let g = gens();
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let g = g.clone();
                tokio::spawn(async move {
                    let text = format!("# node {i}\n");
                    g.bootstrap_or_adopt(&text, &format!("n{i}"), t(0)).await
                })
            })
            .collect();
        let mut results = vec![];
        for task in tasks {
            results.push(task.await.unwrap().unwrap());
        }
        assert_eq!(results.iter().filter(|(_, created)| *created).count(), 1);
        let current = g.current().await.unwrap().unwrap();
        assert!(
            results
                .iter()
                .all(|(g2, _)| g2.text == current.text && g2.pointer.doc == current.pointer.doc)
        );
    }

    #[tokio::test]
    async fn publish_advances_the_generation_with_cas() {
        let g = gens();
        let (first, _) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        let second = g.publish(&first.pointer, V2, "b", t(5)).await.unwrap();
        assert_eq!(
            (second.pointer.doc.generation, second.text.as_str()),
            (2, V2)
        );
        assert_eq!(g.current().await.unwrap(), Some(second.clone()));
        let e = g
            .publish(&first.pointer, "# racing reload\n", "c", t(6))
            .await
            .unwrap_err();
        assert!(
            e.is_conflict(),
            "a reload from a stale generation loses: {e:?}"
        );
        assert_eq!(
            g.current().await.unwrap(),
            Some(second),
            "the losing reload changed nothing"
        );
    }

    #[tokio::test]
    async fn racing_publishers_keep_their_own_snapshots() {
        let g = gens();
        let (first, _) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        let winner = g.publish(&first.pointer, V2, "b", t(5)).await.unwrap();
        g.publish(&first.pointer, "# loser\n", "c", t(6))
            .await
            .unwrap_err();
        let keys = g.docs.store().list("config/snapshots").await.unwrap();
        assert_eq!(keys.len(), 3, "{keys:?}");
        assert_eq!(
            g.current().await.unwrap().unwrap().text,
            winner.text,
            "the loser didn't overwrite generation 2"
        );
    }

    #[tokio::test]
    async fn a_tampered_or_missing_snapshot_is_refused() {
        let g = gens();
        let (first, _) = g.bootstrap_or_adopt(V1, "a", t(0)).await.unwrap();
        let key = config_snapshot_key(1, &first.pointer.doc.hash);
        g.docs
            .store()
            .put(&key, Bytes::from_static(b"globals: {}\n"))
            .await
            .unwrap();
        let e = g.current().await.unwrap_err();
        assert!(e.to_string().contains("doesn't match the hash"), "{e}");
        let other = GenerationDoc {
            hash: config_hash("never written"),
            ..first.pointer.doc.clone()
        };
        g.docs
            .update(GENERATION_KEY, &first.pointer.version, &other)
            .await
            .unwrap();
        let e = g.current().await.unwrap_err();
        assert!(e.to_string().contains("missing"), "{e}");
    }
}
```

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 7cb0688..c48ef48 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -4,8 +4,13 @@
 
 mod anchor;
 mod doc;
+mod generation;
 mod lease;
 
 pub use anchor::{ANCHOR_KEY, Anchor, AnchorDoc, HashAlgo, establish_anchor};
 pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
+pub use generation::{
+    ConfigGeneration, ConfigGenerations, GENERATION_KEY, GenerationDoc, config_hash,
+    config_snapshot_key,
+};
 pub use lease::{Acquired, Holder, LeaseDoc, Leases, lease_key};
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `generation::ConfigGeneration`, `generation::ConfigGenerations`, `generation::GENERATION_KEY`, `generation::GenerationDoc`, `generation::config_hash`, `generation::config_snapshot_key`
error[E0425]: cannot find type `ConfigGenerations` in this scope
error[E0433]: cannot find type `DocStore` in this scope
error[E0425]: cannot find type `DateTime` in this scope
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-state/src/generation.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Config generations in `globals.state` (Clustering §9): the CAS pointer
//! `config/generation.json` and immutable snapshots of the config file text. Each node resolves
//! the text itself (its own environment and files), so a snapshot holds the raw text, never a
//! resolved config.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::doc::{DocStore, Document, StateError, Versioned};

pub const GENERATION_KEY: &str = "config/generation.json";

/// The generation pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationDoc {
    /// 1 at bootstrap, +1 per published reload.
    pub generation: u64,
    /// `blake3:<64 hex>` of the snapshot text.
    pub hash: String,
    pub written_by: String,
    pub written_at: DateTime<Utc>,
}

impl Document for GenerationDoc {
    const KIND: &'static str = "config generation";
    const SCHEMA_VERSION: u64 = 1;
}

/// A generation with its config text.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigGeneration {
    pub pointer: Versioned<GenerationDoc>,
    pub text: String,
}

/// `blake3:<64 hex>` of `text`.
pub fn config_hash(text: &str) -> String {
    format!("blake3:{}", blake3::hash(text.as_bytes()).to_hex())
}

/// `config/snapshots/<generation, 6+ digits>-<hash hex>.yaml`. Content-addressed, so two nodes
/// racing to publish the same generation never overwrite each other's snapshot.
pub fn config_snapshot_key(generation: u64, hash: &str) -> String {
    let hex = hash.strip_prefix("blake3:").unwrap_or(hash);
    format!("config/snapshots/{generation:06}-{hex}.yaml")
}

/// The config generation operations over the `globals.state` documents.
#[derive(Debug, Clone)]
pub struct ConfigGenerations {
    docs: DocStore,
}

impl ConfigGenerations {
    /// `docs` is rooted at the `globals.state` location.
    pub fn new(docs: DocStore) -> Self {
        Self { docs }
    }

    /// The current generation and its text, or `None` before the first bootstrap.
    ///
    /// # Errors
    /// A storage or decode failure; `Corrupt` when the snapshot is missing or its text doesn't
    /// match the pointer's hash.
    pub async fn current(&self) -> Result<Option<ConfigGeneration>, StateError> {
        let Some(pointer) = self.docs.load::<GenerationDoc>(GENERATION_KEY).await? else {
            return Ok(None);
        };
        let key = config_snapshot_key(pointer.doc.generation, &pointer.doc.hash);
        let corrupt = |reason: &str| StateError::Corrupt {
            key: key.clone(),
            kind: "config snapshot",
            reason: reason.to_owned(),
        };
        let Some((body, _)) = self.docs.store().get(&key).await? else {
            return Err(corrupt("missing; config/generation.json points at it"));
        };
        let text = String::from_utf8(body.to_vec()).map_err(|_| corrupt("not UTF-8"))?;
        if config_hash(&text) != pointer.doc.hash {
            return Err(corrupt(
                "its text doesn't match the hash in config/generation.json",
            ));
        }
        Ok(Some(ConfigGeneration { pointer, text }))
    }

    /// Cold start (§9.3): adopts the current generation if there is one; otherwise publishes
    /// `text` as generation 1. Concurrent bootstrappers race on the pointer's create; the losers
    /// adopt the winner's generation. Returns the generation and whether this call created it.
    ///
    /// # Errors
    /// As [`Self::current`], or a storage failure while writing.
    pub async fn bootstrap_or_adopt(
        &self,
        text: &str,
        node_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(ConfigGeneration, bool), StateError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            if let Some(current) = self.current().await? {
                return Ok((current, false));
            }
            let doc = self.write_snapshot(1, text, node_id, now).await?;
            match self.docs.create(GENERATION_KEY, &doc).await {
                Ok(version) => {
                    let pointer = Versioned { doc, version };
                    return Ok((
                        ConfigGeneration {
                            pointer,
                            text: text.to_owned(),
                        },
                        true,
                    ));
                }
                Err(e) if e.is_race() && attempts < 3 => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Reload (§9.2): publishes `text` as the generation after `base`, with a CAS on the pointer.
    ///
    /// # Errors
    /// `Store(Conflict)` when another node published since `base` was read: re-read, re-validate
    /// against the new generation and try again. The losing snapshot stays behind, unreferenced.
    pub async fn publish(
        &self,
        base: &Versioned<GenerationDoc>,
        text: &str,
        node_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ConfigGeneration, StateError> {
        let doc = self
            .write_snapshot(base.doc.generation + 1, text, node_id, now)
            .await?;
        let version = self
            .docs
            .update(GENERATION_KEY, &base.version, &doc)
            .await?;
        Ok(ConfigGeneration {
            pointer: Versioned { doc, version },
            text: text.to_owned(),
        })
    }

    async fn write_snapshot(
        &self,
        generation: u64,
        text: &str,
        node_id: &str,
        now: DateTime<Utc>,
    ) -> Result<GenerationDoc, StateError> {
        let hash = config_hash(text);
        let key = config_snapshot_key(generation, &hash);
        match self.docs.store().create(&key, text.to_owned().into()).await {
            // Same key = same generation and same text: someone already wrote these exact bytes.
            Ok(_) | Err(data_avalanche_storage::StoreError::Conflict { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(GenerationDoc {
            generation,
            hash,
            written_by: node_id.to_owned(),
            written_at: now,
        })
    }
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state`
Expected: PASS. `data-avalanche-state` unit tests 29; `tests/lease_conformance.rs` 6; `tests/races.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state/Cargo.toml \
  crates/data-avalanche-state/src/generation.rs \
  crates/data-avalanche-state/src/lib.rs \
  Cargo.lock
git commit -m "feat(state): config generations — CAS pointer and content-addressed snapshots"
```


---

### Task 9: Per-pipeline state, slot and position documents

**Files:**
- Create: `crates/data-avalanche-state/src/pipeline.rs`
- Modify: `crates/data-avalanche-state/Cargo.toml`, `crates/data-avalanche-state/src/lib.rs`, `crates/data-avalanche-state/src/doc.rs`

**Interfaces:**
- Consumes: `DocStore` (Task 4); `data_avalanche_core::{SourcePosition, position::Lsn}` (M0).
- Produces:
  - `StateError::{SinkTypeMismatch { recorded, configured }, PipelineIdMismatch { recorded, configured }}`.
  - `PIPELINE_STATE_KEY`, `SLOT_KEY`, `POSITION_KEY`.
  - `DesiredState::{Running, Paused, Stopped}`.
  - `PipelineStateDoc { pipeline_id, sink_type: String, desired_state }`.
  - `SlotDoc { slot_name, confirmed_flush_lsn: Lsn }`: the LSN is stored as PostgreSQL prints it.
  - `ServerIdentity::{Mysql { server_uuid }, Mariadb { server_id: u32 }}`.
  - `PositionDoc { position: SourcePosition, server: ServerIdentity }`.
  - `PipelineDocs::new(DocStore)` with:
    - `open(pipeline_id, sink_type) -> Result<Versioned<PipelineStateDoc>>`;
    - `set_desired_state(DesiredState)`;
    - `slot()` and `save_slot(&SlotDoc)`;
    - `position()` and `save_position(&PositionDoc)`.
  - `pub(crate) docs` field, for Task 10.

`open` runs at every pipeline start: it creates the document the first time and checks it on
every later start (config §13.4 sink-type row; D-M2.14). `desired_state` is Pipeline_Lifecycle §4.
D-M2.13.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-state/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-state/Cargo.toml b/crates/data-avalanche-state/Cargo.toml
index 3a1d573..e827b38 100644
--- a/crates/data-avalanche-state/Cargo.toml
+++ b/crates/data-avalanche-state/Cargo.toml
@@ -13,6 +13,7 @@ workspace = true
 blake3.workspace = true
 bytes.workspace = true
 chrono.workspace = true
+data-avalanche-core.workspace = true
 data-avalanche-storage.workspace = true
 serde.workspace = true
 serde_json.workspace = true
```

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index c48ef48..58d4c73 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -6,6 +6,7 @@ mod anchor;
 mod doc;
 mod generation;
 mod lease;
+mod pipeline;
 
 pub use anchor::{ANCHOR_KEY, Anchor, AnchorDoc, HashAlgo, establish_anchor};
 pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
@@ -14,3 +15,7 @@ pub use generation::{
     config_snapshot_key,
 };
 pub use lease::{Acquired, Holder, LeaseDoc, Leases, lease_key};
+pub use pipeline::{
+    DesiredState, PIPELINE_STATE_KEY, POSITION_KEY, PipelineDocs, PipelineStateDoc, PositionDoc,
+    SLOT_KEY, ServerIdentity, SlotDoc,
+};
```

Create `crates/data-avalanche-state/src/pipeline.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;

    use super::*;

    fn pipeline() -> PipelineDocs {
        let store =
            ObjectStoreVersioned::new(Arc::new(InMemory::new()), "lake/orders/.pipeline-state")
                .unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(store)))
    }

    fn pos(s: &str) -> SourcePosition {
        s.parse().unwrap()
    }

    fn json<T: Document>(doc: &T) -> String {
        String::from_utf8(crate::doc::encode(doc).to_vec()).unwrap()
    }

    #[tokio::test]
    async fn open_creates_running_state_then_checks_it() {
        let p = pipeline();
        let first = p.open("orders", "delta").await.unwrap();
        assert_eq!(
            first.doc,
            PipelineStateDoc {
                pipeline_id: "orders".into(),
                sink_type: "delta".into(),
                desired_state: DesiredState::Running
            }
        );
        assert_eq!(p.open("orders", "delta").await.unwrap(), first);
    }

    #[tokio::test]
    async fn a_changed_sink_type_is_refused() {
        let p = pipeline();
        p.open("orders", "delta").await.unwrap();
        let e = p.open("orders", "iceberg").await.unwrap_err();
        assert!(
            matches!(&e, StateError::SinkTypeMismatch { recorded, configured } if recorded == "delta" && configured == "iceberg"),
            "{e:?}"
        );
        assert_eq!(
            e.to_string(),
            "pipeline state records sink type delta but the config says iceberg; positions and markers are sink-specific, so a sink type change needs a new state location (re-seed)"
        );
    }

    #[tokio::test]
    async fn another_pipelines_state_is_refused() {
        let p = pipeline();
        p.open("orders", "delta").await.unwrap();
        let e = p.open("invoices", "delta").await.unwrap_err();
        assert!(matches!(e, StateError::PipelineIdMismatch { .. }), "{e:?}");
    }

    #[tokio::test]
    async fn desired_state_round_trips() {
        let p = pipeline();
        assert!(
            p.set_desired_state(DesiredState::Paused).await.is_err(),
            "not opened yet"
        );
        p.open("orders", "delta").await.unwrap();
        for s in [
            DesiredState::Paused,
            DesiredState::Stopped,
            DesiredState::Running,
        ] {
            assert_eq!(p.set_desired_state(s).await.unwrap().doc.desired_state, s);
            assert_eq!(
                p.open("orders", "delta").await.unwrap().doc.desired_state,
                s,
                "survives a restart"
            );
        }
    }

    #[test]
    fn pipeline_state_json_shape() {
        let doc = PipelineStateDoc {
            pipeline_id: "orders".into(),
            sink_type: "delta".into(),
            desired_state: DesiredState::Paused,
        };
        assert_eq!(
            json(&doc),
            "{\n  \"version\": 1,\n  \"pipeline_id\": \"orders\",\n  \"sink_type\": \"delta\",\n  \"desired_state\": \"paused\"\n}"
        );
    }

    #[tokio::test]
    async fn slot_round_trips() {
        let p = pipeline();
        assert!(p.slot().await.unwrap().is_none());
        let doc = SlotDoc {
            slot_name: "data_avalanche_orders".into(),
            confirmed_flush_lsn: "0/16B3748".parse().unwrap(),
        };
        assert_eq!(
            json(&doc),
            "{\n  \"version\": 1,\n  \"slot_name\": \"data_avalanche_orders\",\n  \"confirmed_flush_lsn\": \"0/16B3748\"\n}"
        );
        p.save_slot(&doc).await.unwrap();
        let later = SlotDoc {
            confirmed_flush_lsn: "1/0".parse().unwrap(),
            ..doc
        };
        p.save_slot(&later).await.unwrap();
        assert_eq!(p.slot().await.unwrap().unwrap().doc, later);
    }

    #[tokio::test]
    async fn position_round_trips_for_every_mysql_dialect() {
        let p = pipeline();
        let cases = [
            (
                "mysql-gtid:3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
                ServerIdentity::Mysql {
                    server_uuid: "3e11fa47-71ca-11e1-9e33-c80aa9429562".into(),
                },
            ),
            (
                "mariadb-gtid:0-1-100,1-2-7",
                ServerIdentity::Mariadb { server_id: 1 },
            ),
            (
                "binlog:mysql-bin.000042:1337",
                ServerIdentity::Mysql {
                    server_uuid: "u".into(),
                },
            ),
        ];
        for (text, server) in cases {
            let doc = PositionDoc {
                position: pos(text),
                server,
            };
            p.save_position(&doc).await.unwrap();
            assert_eq!(p.position().await.unwrap().unwrap().doc, doc, "{text}");
        }
    }

    #[test]
    fn position_json_shape() {
        let doc = PositionDoc {
            position: pos("binlog:mysql-bin.000042:1337"),
            server: ServerIdentity::Mariadb { server_id: 7 },
        };
        assert_eq!(
            json(&doc),
            r#"{
  "version": 1,
  "position": "binlog:mysql-bin.000042:1337",
  "server": {
    "engine": "mariadb",
    "server_id": 7
  }
}"#
        );
    }

    #[tokio::test]
    async fn malformed_positions_are_corrupt() {
        let p = pipeline();
        for body in [
            r#"{"version":1,"position":"nope:1","server":{"engine":"mariadb","server_id":1}}"#,
            r#"{"version":1,"position":"pg:0/1","server":{"engine":"oracle","sid":1}}"#,
        ] {
            p.docs
                .store()
                .put(POSITION_KEY, Bytes::from(body))
                .await
                .unwrap();
            let e = p.position().await.unwrap_err();
            assert!(matches!(e, StateError::Corrupt { .. }), "{body}: {e:?}");
        }
        p.docs
            .store()
            .put(
                SLOT_KEY,
                Bytes::from(r#"{"version":1,"slot_name":"s","confirmed_flush_lsn":"16B3748"}"#),
            )
            .await
            .unwrap();
        assert!(matches!(
            p.slot().await.unwrap_err(),
            StateError::Corrupt { .. }
        ));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `pipeline::DesiredState`, `pipeline::PIPELINE_STATE_KEY`, `pipeline::POSITION_KEY`, `pipeline::PipelineDocs`, `pipeline::PipelineStateDoc`, `pipeline::PositionDoc`, `pipeline::SLOT_KEY`, `pipeline::ServerIdentity`, `pipeline::SlotDoc`
error[E0425]: cannot find type `PipelineDocs` in this scope
error[E0433]: cannot find type `DocStore` in this scope
error[E0425]: cannot find type `SourcePosition` in this scope
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-state/src/doc.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/doc.rs b/crates/data-avalanche-state/src/doc.rs
index 26a72d3..6fb1771 100644
--- a/crates/data-avalanche-state/src/doc.rs
+++ b/crates/data-avalanche-state/src/doc.rs
@@ -41,6 +41,20 @@ pub enum StateError {
         found: u64,
         supported: u64,
     },
+    #[error(
+        "pipeline state records sink type {recorded} but the config says {configured}; positions and markers are sink-specific, so a sink type change needs a new state location (re-seed)"
+    )]
+    SinkTypeMismatch {
+        recorded: String,
+        configured: String,
+    },
+    #[error(
+        "the state location belongs to pipeline {recorded:?}, not {configured:?}; two pipelines must not share a state location"
+    )]
+    PipelineIdMismatch {
+        recorded: String,
+        configured: String,
+    },
 }
 
 impl StateError {
```

In `crates/data-avalanche-state/src/pipeline.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Per-pipeline documents under the pipeline's state location (Data-Flow §State Layout):
//! `pipeline-state.json`, `replication/slot.json` and `replication/position.json`. Only the lease
//! holder writes them; a CAS read-modify-write keeps a slow writer from losing another's update.

use std::fmt::Display;
use std::str::FromStr;

use data_avalanche_core::SourcePosition;
use data_avalanche_core::position::Lsn;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::doc::{DocStore, Document, StateError, Versioned};

pub const PIPELINE_STATE_KEY: &str = "pipeline-state.json";
pub const SLOT_KEY: &str = "replication/slot.json";
pub const POSITION_KEY: &str = "replication/position.json";

/// Operator intent (Pipeline_Lifecycle §4): survives restarts, failover and config reloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DesiredState {
    Running,
    Paused,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineStateDoc {
    pub pipeline_id: String,
    /// `sink.type` when the state was created (`delta`, `iceberg`).
    pub sink_type: String,
    pub desired_state: DesiredState,
}

impl Document for PipelineStateDoc {
    const KIND: &'static str = "pipeline state";
    const SCHEMA_VERSION: u64 = 1;
}

/// PostgreSQL: the replication slot and the LSN the source may release WAL up to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotDoc {
    pub slot_name: String,
    /// As PostgreSQL prints it (`0/16B3748`).
    #[serde(serialize_with = "display", deserialize_with = "from_str")]
    pub confirmed_flush_lsn: Lsn,
}

impl Document for SlotDoc {
    const KIND: &'static str = "replication slot";
    const SCHEMA_VERSION: u64 = 1;
}

/// The source server a MySQL / MariaDB position was read from. File/position mode refuses to
/// resume against a different server (`BINLOG_SOURCE_CHANGED`, Q-M7-2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "engine", rename_all = "lowercase", deny_unknown_fields)]
pub enum ServerIdentity {
    Mysql { server_uuid: String },
    Mariadb { server_id: u32 },
}

/// MySQL / MariaDB: the stream position (`mysql-gtid:…`, `mariadb-gtid:…`, `binlog:<file>:<pos>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionDoc {
    pub position: SourcePosition,
    pub server: ServerIdentity,
}

impl Document for PositionDoc {
    const KIND: &'static str = "replication position";
    const SCHEMA_VERSION: u64 = 1;
}

fn display<T: Display, S: Serializer>(v: &T, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(v)
}

fn from_str<'de, T, D>(d: D) -> Result<T, D::Error>
where
    T: FromStr,
    T::Err: Display,
    D: Deserializer<'de>,
{
    String::deserialize(d)?
        .parse()
        .map_err(serde::de::Error::custom)
}

/// The documents of one pipeline. `docs` is rooted at its state location
/// (`data_avalanche_storage::colocated_state` / `explicit_state`).
#[derive(Debug, Clone)]
pub struct PipelineDocs {
    pub(crate) docs: DocStore,
}

impl PipelineDocs {
    pub fn new(docs: DocStore) -> Self {
        Self { docs }
    }

    /// Pipeline start: creates `pipeline-state.json` (desired state `running`) on first start;
    /// afterwards checks it belongs to `pipeline_id` and records `sink_type`.
    ///
    /// # Errors
    /// `PipelineIdMismatch`, `SinkTypeMismatch` (refuse the pipeline, config §13.4), or a storage
    /// or decode failure.
    pub async fn open(
        &self,
        pipeline_id: &str,
        sink_type: &str,
    ) -> Result<Versioned<PipelineStateDoc>, StateError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            if let Some(existing) = self
                .docs
                .load::<PipelineStateDoc>(PIPELINE_STATE_KEY)
                .await?
            {
                let doc = &existing.doc;
                if doc.pipeline_id != pipeline_id {
                    return Err(StateError::PipelineIdMismatch {
                        recorded: doc.pipeline_id.clone(),
                        configured: pipeline_id.to_owned(),
                    });
                }
                if doc.sink_type != sink_type {
                    return Err(StateError::SinkTypeMismatch {
                        recorded: doc.sink_type.clone(),
                        configured: sink_type.to_owned(),
                    });
                }
                return Ok(existing);
            }
            let doc = PipelineStateDoc {
                pipeline_id: pipeline_id.to_owned(),
                sink_type: sink_type.to_owned(),
                desired_state: DesiredState::Running,
            };
            match self.docs.create(PIPELINE_STATE_KEY, &doc).await {
                Ok(version) => return Ok(Versioned { doc, version }),
                Err(e) if e.is_race() && attempts < 3 => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Records a pause / resume / stop (Pipeline_Lifecycle §4).
    ///
    /// # Errors
    /// `Corrupt` when the pipeline was never opened, or a storage or decode failure.
    pub async fn set_desired_state(
        &self,
        state: DesiredState,
    ) -> Result<Versioned<PipelineStateDoc>, StateError> {
        self.docs
            .modify(
                PIPELINE_STATE_KEY,
                |cur: Option<PipelineStateDoc>| match cur {
                    Some(doc) => Ok(PipelineStateDoc {
                        desired_state: state,
                        ..doc
                    }),
                    None => Err(StateError::Corrupt {
                        key: PIPELINE_STATE_KEY.into(),
                        kind: PipelineStateDoc::KIND,
                        reason: "missing; open the pipeline first".into(),
                    }),
                },
            )
            .await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn slot(&self) -> Result<Option<Versioned<SlotDoc>>, StateError> {
        self.docs.load(SLOT_KEY).await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn save_slot(&self, doc: &SlotDoc) -> Result<Versioned<SlotDoc>, StateError> {
        self.docs
            .modify(SLOT_KEY, |_: Option<SlotDoc>| Ok(doc.clone()))
            .await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn position(&self) -> Result<Option<Versioned<PositionDoc>>, StateError> {
        self.docs.load(POSITION_KEY).await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn save_position(
        &self,
        doc: &PositionDoc,
    ) -> Result<Versioned<PositionDoc>, StateError> {
        self.docs
            .modify(POSITION_KEY, |_: Option<PositionDoc>| Ok(doc.clone()))
            .await
    }
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state`
Expected: PASS. `data-avalanche-state` unit tests 38; `tests/lease_conformance.rs` 6; `tests/races.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state/Cargo.toml \
  crates/data-avalanche-state/src/lib.rs \
  crates/data-avalanche-state/src/pipeline.rs \
  crates/data-avalanche-state/src/doc.rs \
  Cargo.lock
git commit -m "feat(state): per-pipeline state, slot and position documents"
```


---

### Task 10: Table snapshot state and snapshot jobs

**Files:**
- Create: `crates/data-avalanche-state/src/snapshot.rs`
- Modify: `crates/data-avalanche-state/src/lib.rs`

**Interfaces:**
- Consumes: `PipelineDocs` and its `docs` field (Task 9); `data_avalanche_core::TableRef`;
  `escape_segment` (Task 2).
- Produces:
  - Snapshot types:
    - `SnapshotStatus::{None, InProgress, Completed, Broken}`;
    - `CursorKey { col, val: serde_json::Value, ty }` (JSON name `type`);
    - `SnapshotCursor { key: Vec<CursorKey>, chunk_size, rows_done, collation: Option<String> }`;
    - `TableSnapshotDoc { table: TableRef, status, start_position: Option<SourcePosition>, cursor:
      Option<SnapshotCursor>, row_count: Option<u64>, completed_at: Option<DateTime<Utc>> }`.
  - Job types:
    - `JobStatus::{Queued, Running, Completed, Failed}`;
    - `JobTable { table, status, rows_done }`;
    - `SnapshotJobDoc { job_id, status, tables, requested_at, requested_by, finished_at, error }`.
  - Keys: `table_snapshot_key(&TableRef)`, `job_key(&str)`, `JOBS_PREFIX`.
  - `PipelineDocs` methods: `table_snapshot(&TableRef)`, `save_table_snapshot(&doc)`,
    `create_snapshot_job(&doc)` (a duplicate ID is `Conflict`), `save_snapshot_job(&doc)`,
    `snapshot_job(id)`, `snapshot_jobs()` (sorted by request time, then ID).

D-M2.12. M5/M7 write the cursor once per chunk; M9's API lists the jobs.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 58d4c73..8a80753 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -7,6 +7,7 @@ mod doc;
 mod generation;
 mod lease;
 mod pipeline;
+mod snapshot;
 
 pub use anchor::{ANCHOR_KEY, Anchor, AnchorDoc, HashAlgo, establish_anchor};
 pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
@@ -19,3 +20,7 @@ pub use pipeline::{
     DesiredState, PIPELINE_STATE_KEY, POSITION_KEY, PipelineDocs, PipelineStateDoc, PositionDoc,
     SLOT_KEY, ServerIdentity, SlotDoc,
 };
+pub use snapshot::{
+    CursorKey, JOBS_PREFIX, JobStatus, JobTable, SnapshotCursor, SnapshotJobDoc, SnapshotStatus,
+    TableSnapshotDoc, job_key, table_snapshot_key,
+};
```

Create `crates/data-avalanche-state/src/snapshot.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use chrono::TimeZone;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;
    use serde_json::json;

    use super::*;
    use crate::doc::DocStore;

    fn pipeline() -> PipelineDocs {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state/orders").unwrap();
        PipelineDocs::new(DocStore::new(Arc::new(store)))
    }

    fn t(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 28, 10, 0, s).unwrap()
    }

    fn in_progress(table: TableRef) -> TableSnapshotDoc {
        TableSnapshotDoc {
            table,
            status: SnapshotStatus::InProgress,
            start_position: Some("pg:0/1A2B3C4".parse().unwrap()),
            cursor: Some(SnapshotCursor {
                key: vec![
                    CursorKey {
                        col: "tenant_id".into(),
                        val: json!(42),
                        ty: "int8".into(),
                    },
                    CursorKey {
                        col: "id".into(),
                        val: json!("9f1c"),
                        ty: "uuid".into(),
                    },
                ],
                chunk_size: 10_000,
                rows_done: 4_530_000,
                collation: None,
            }),
            row_count: None,
            completed_at: None,
        }
    }

    #[test]
    fn table_snapshot_keys() {
        let key = |s: &str, n: &str| table_snapshot_key(&TableRef::new(s, n));
        assert_eq!(key("public", "orders"), "snapshots/public.orders.json");
        assert_eq!(key("a.b", "c"), "snapshots/a%2Eb.c.json");
        assert_eq!(key("a", "b.c"), "snapshots/a.b%2Ec.json");
        assert_eq!(key("sales", "a/b 50%"), "snapshots/sales.a%2Fb 50%25.json");
        assert_eq!(key("продажи", "заказы"), "snapshots/продажи.заказы.json");
        assert_eq!(job_key("0199-abc"), "snapshots/jobs/0199-abc.json");
    }

    #[test]
    fn table_snapshot_json_shape() {
        let doc = in_progress(TableRef::new("public", "orders"));
        assert_eq!(
            String::from_utf8(crate::doc::encode(&doc).to_vec()).unwrap(),
            r#"{
  "version": 1,
  "table": {
    "schema": "public",
    "name": "orders"
  },
  "status": "in_progress",
  "start_position": "pg:0/1A2B3C4",
  "cursor": {
    "key": [
      {
        "col": "tenant_id",
        "val": 42,
        "type": "int8"
      },
      {
        "col": "id",
        "val": "9f1c",
        "type": "uuid"
      }
    ],
    "chunk_size": 10000,
    "rows_done": 4530000,
    "collation": null
  },
  "row_count": null,
  "completed_at": null
}"#
        );
    }

    #[tokio::test]
    async fn table_snapshot_checkpoints_then_completes() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        assert!(p.table_snapshot(&orders).await.unwrap().is_none());
        let mut doc = in_progress(orders.clone());
        p.save_table_snapshot(&doc).await.unwrap();
        assert_eq!(p.table_snapshot(&orders).await.unwrap().unwrap().doc, doc);
        doc.status = SnapshotStatus::Completed;
        doc.cursor = None;
        doc.row_count = Some(4_531_234);
        doc.completed_at = Some(t(9));
        p.save_table_snapshot(&doc).await.unwrap();
        assert_eq!(p.table_snapshot(&orders).await.unwrap().unwrap().doc, doc);
    }

    #[tokio::test]
    async fn dotted_names_do_not_collide() {
        let p = pipeline();
        let (x, y) = (TableRef::new("a.b", "c"), TableRef::new("a", "b.c"));
        p.save_table_snapshot(&in_progress(x.clone()))
            .await
            .unwrap();
        assert!(p.table_snapshot(&y).await.unwrap().is_none());
        p.save_table_snapshot(&in_progress(y.clone()))
            .await
            .unwrap();
        assert_eq!(p.table_snapshot(&x).await.unwrap().unwrap().doc.table, x);
        assert_eq!(p.table_snapshot(&y).await.unwrap().unwrap().doc.table, y);
    }

    #[tokio::test]
    async fn a_document_for_another_table_is_corrupt() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        let body = crate::doc::encode(&in_progress(TableRef::new("public", "invoices")));
        p.docs
            .store()
            .put(&table_snapshot_key(&orders), body)
            .await
            .unwrap();
        let e = p.table_snapshot(&orders).await.unwrap_err();
        assert!(
            e.to_string()
                .contains("it is for table public.invoices, not public.orders"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn an_unknown_status_is_corrupt() {
        let p = pipeline();
        let orders = TableRef::new("public", "orders");
        let text =
            String::from_utf8(crate::doc::encode(&in_progress(orders.clone())).to_vec()).unwrap();
        let bad = text.replace("in_progress", "paused");
        p.docs
            .store()
            .put(&table_snapshot_key(&orders), Bytes::from(bad))
            .await
            .unwrap();
        assert!(matches!(
            p.table_snapshot(&orders).await.unwrap_err(),
            StateError::Corrupt { .. }
        ));
    }

    fn job(id: &str, at: u32) -> SnapshotJobDoc {
        SnapshotJobDoc {
            job_id: id.into(),
            status: JobStatus::Queued,
            tables: vec![JobTable {
                table: TableRef::new("public", "orders"),
                status: SnapshotStatus::None,
                rows_done: 0,
            }],
            requested_at: t(at),
            requested_by: "admin".into(),
            finished_at: None,
            error: None,
        }
    }

    #[tokio::test]
    async fn jobs_persist_and_list_in_request_order() {
        let p = pipeline();
        assert!(p.snapshot_jobs().await.unwrap().is_empty());
        p.create_snapshot_job(&job("b-job", 1)).await.unwrap();
        p.create_snapshot_job(&job("a-job", 2)).await.unwrap();
        assert!(
            p.create_snapshot_job(&job("a-job", 3))
                .await
                .unwrap_err()
                .is_conflict(),
            "job IDs are unique"
        );
        let mut done = job("b-job", 1);
        done.status = JobStatus::Failed;
        done.finished_at = Some(t(5));
        done.error = Some("table public.orders not found".into());
        p.save_snapshot_job(&done).await.unwrap();
        let listed: Vec<_> = p
            .snapshot_jobs()
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.doc)
            .collect();
        assert_eq!(listed, vec![done.clone(), job("a-job", 2)]);
        assert_eq!(p.snapshot_job("b-job").await.unwrap().unwrap().doc, done);
        assert!(p.snapshot_job("nope").await.unwrap().is_none());
        p.save_table_snapshot(&in_progress(TableRef::new("public", "jobs")))
            .await
            .unwrap();
        assert_eq!(
            p.snapshot_jobs().await.unwrap().len(),
            2,
            "a table named `jobs` isn't a job"
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-state`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `snapshot::CursorKey`, `snapshot::JOBS_PREFIX`, `snapshot::JobStatus`, `snapshot::JobTable`, `snapshot::SnapshotCursor`, `snapshot::SnapshotJobDoc`, `snapshot::SnapshotStatus`, `snapshot::TableSnapshotDoc`, `snapshot::job_key`, `snapshot::table_snapshot_key`
error[E0425]: cannot find type `PipelineDocs` in this scope
error[E0433]: cannot find type `PipelineDocs` in this scope
error[E0425]: cannot find type `DateTime` in this scope
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-state/src/snapshot.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Snapshot documents under the pipeline's state location (Data-Flow §State Layout,
//! Snapshot_Resumability §2): one per table, `snapshots/<schema>.<table>.json`, and one per
//! on-demand job, `snapshots/jobs/<job_id>.json`.

use chrono::{DateTime, Utc};
use data_avalanche_core::{SourcePosition, TableRef};
use data_avalanche_storage::escape_segment;
use serde::{Deserialize, Serialize};

use crate::doc::{Document, StateError, Versioned};
use crate::pipeline::PipelineDocs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStatus {
    None,
    InProgress,
    Completed,
    Broken,
}

/// One identifier column of the last row of the last committed chunk. `val` is the value as JSON
/// (number, string, bool); the source connector converts it back to a typed value (M5/M7).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CursorKey {
    pub col: String,
    pub val: serde_json::Value,
    /// The source column type (`int8`, `uuid`, `varchar`, …).
    #[serde(rename = "type")]
    pub ty: String,
}

/// Where a chunked snapshot resumes (Snapshot_Resumability §2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotCursor {
    /// The identifier tuple, in key order.
    pub key: Vec<CursorKey>,
    pub chunk_size: u64,
    pub rows_done: u64,
    /// The binary collation the keyset ordering is pinned to (`C`, `utf8mb4_bin`); `None` when no
    /// key column is text.
    pub collation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSnapshotDoc {
    /// The table this document is for (its key escapes the names; this keeps them verbatim).
    pub table: TableRef,
    pub status: SnapshotStatus,
    /// The stream position when the snapshot started (PG: the slot's start LSN).
    pub start_position: Option<SourcePosition>,
    /// `None` before the first chunk and after completion.
    pub cursor: Option<SnapshotCursor>,
    pub row_count: Option<u64>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl Document for TableSnapshotDoc {
    const KIND: &'static str = "table snapshot";
    const SCHEMA_VERSION: u64 = 1;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobTable {
    pub table: TableRef,
    pub status: SnapshotStatus,
    pub rows_done: u64,
}

/// An on-demand snapshot job (Data-Flow §On-Demand Snapshot); survives restart and failover.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotJobDoc {
    pub job_id: String,
    pub status: JobStatus,
    pub tables: Vec<JobTable>,
    pub requested_at: DateTime<Utc>,
    pub requested_by: String,
    pub finished_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

impl Document for SnapshotJobDoc {
    const KIND: &'static str = "snapshot job";
    const SCHEMA_VERSION: u64 = 1;
}

/// `snapshots/<schema>.<table>.json`. Each name is escaped as a key segment and its dots become
/// `%2E`, so the `.` between them is unambiguous (`a.b` + `c` ≠ `a` + `b.c`).
pub fn table_snapshot_key(table: &TableRef) -> String {
    let part = |s: &str| escape_segment(s).replace('.', "%2E");
    format!(
        "snapshots/{}.{}.json",
        part(&table.schema),
        part(&table.name)
    )
}

pub const JOBS_PREFIX: &str = "snapshots/jobs";

/// `snapshots/jobs/<job_id>.json`.
pub fn job_key(job_id: &str) -> String {
    format!("{JOBS_PREFIX}/{}.json", escape_segment(job_id))
}

impl PipelineDocs {
    /// # Errors
    /// A storage or decode failure; `Corrupt` when the stored document names another table.
    pub async fn table_snapshot(
        &self,
        table: &TableRef,
    ) -> Result<Option<Versioned<TableSnapshotDoc>>, StateError> {
        let key = table_snapshot_key(table);
        let found = self.docs.load::<TableSnapshotDoc>(&key).await?;
        if let Some(v) = &found
            && &v.doc.table != table
        {
            return Err(StateError::Corrupt {
                key,
                kind: TableSnapshotDoc::KIND,
                reason: format!("it is for table {}, not {table}", v.doc.table),
            });
        }
        Ok(found)
    }

    /// Checkpoints a table's snapshot state (once per chunk, Snapshot_Resumability §5).
    ///
    /// # Errors
    /// A storage or decode failure.
    pub async fn save_table_snapshot(
        &self,
        doc: &TableSnapshotDoc,
    ) -> Result<Versioned<TableSnapshotDoc>, StateError> {
        let key = table_snapshot_key(&doc.table);
        self.docs
            .modify(&key, |_: Option<TableSnapshotDoc>| Ok(doc.clone()))
            .await
    }

    /// Persists a new job before the API responds.
    ///
    /// # Errors
    /// `Store(Conflict)` when a job with this ID exists, or a storage failure.
    pub async fn create_snapshot_job(
        &self,
        doc: &SnapshotJobDoc,
    ) -> Result<Versioned<SnapshotJobDoc>, StateError> {
        let version = self.docs.create(&job_key(&doc.job_id), doc).await?;
        Ok(Versioned {
            doc: doc.clone(),
            version,
        })
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn save_snapshot_job(
        &self,
        doc: &SnapshotJobDoc,
    ) -> Result<Versioned<SnapshotJobDoc>, StateError> {
        self.docs
            .modify(&job_key(&doc.job_id), |_: Option<SnapshotJobDoc>| {
                Ok(doc.clone())
            })
            .await
    }

    /// # Errors
    /// A storage or decode failure.
    pub async fn snapshot_job(
        &self,
        job_id: &str,
    ) -> Result<Option<Versioned<SnapshotJobDoc>>, StateError> {
        self.docs.load(&job_key(job_id)).await
    }

    /// Every job of this pipeline, oldest request first.
    ///
    /// # Errors
    /// A storage or decode failure (one bad job document fails the listing).
    pub async fn snapshot_jobs(&self) -> Result<Vec<Versioned<SnapshotJobDoc>>, StateError> {
        let mut jobs = vec![];
        for key in self.docs.store().list(JOBS_PREFIX).await? {
            if let Some(job) = self.docs.load::<SnapshotJobDoc>(&key).await? {
                jobs.push(job);
            }
        }
        jobs.sort_by(|a, b| {
            (a.doc.requested_at, &a.doc.job_id).cmp(&(b.doc.requested_at, &b.doc.job_id))
        });
        Ok(jobs)
    }
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-state`
Expected: PASS. `data-avalanche-state` unit tests 45; `tests/lease_conformance.rs` 6; `tests/races.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state/src/lib.rs \
  crates/data-avalanche-state/src/snapshot.rs
git commit -m "feat(state): table snapshot state and snapshot jobs"
```


---

### Task 11: The startup sequence end to end; crate docs; M2 exit checks

**Files:**
- Modify: `crates/data-avalanche-state/Cargo.toml`, `crates/data-avalanche-state/src/lib.rs`, `crates/data-avalanche-storage/src/lib.rs`
- Test: `crates/data-avalanche-state/tests/startup.rs`

**Interfaces:**
- Consumes: everything above.
- Also consumes (dev-dependency) `data_avalanche_config::{load_str, FingerprintKey, ClusterAnchor,
  LockBackend, check_cluster_anchor, StateLocation, Pipeline}` (roadmap §3.8).
- Produces: `tests/startup.rs` and the crate docs of both crates.

The test is the startup recipe M6 wires into the binary:
1. Load with a random key.
2. `establish_anchor` at `globals_state(..)`, proposing a fresh `fingerprint_key`.
3. `check_cluster_anchor`: a mismatch refuses the node (V-062 shown).
4. Load again with the anchor's key.
5. `bootstrap_or_adopt`.
6. `PipelineDocs::open` at each pipeline's state location.

It also shows that two nodes fingerprint the same secret identically only with the anchor's key.

- [ ] **Step 1: Write the startup test and the crate docs**

Change `crates/data-avalanche-state/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-state/Cargo.toml b/crates/data-avalanche-state/Cargo.toml
index e827b38..7c74591 100644
--- a/crates/data-avalanche-state/Cargo.toml
+++ b/crates/data-avalanche-state/Cargo.toml
@@ -20,6 +20,7 @@ serde_json.workspace = true
 thiserror.workspace = true
 
 [dev-dependencies]
+data-avalanche-config.workspace = true
 futures-util.workspace = true
 data-avalanche-testkit.workspace = true
 object_store.workspace = true
```

Change `crates/data-avalanche-state/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-state/src/lib.rs b/crates/data-avalanche-state/src/lib.rs
index 8a80753..b1779f6 100644
--- a/crates/data-avalanche-state/src/lib.rs
+++ b/crates/data-avalanche-state/src/lib.rs
@@ -1,6 +1,32 @@
 //! Durable state for data-avalanche: the per-pipeline documents (Data-Flow §State Layout), leases
 //! and the `globals.state` documents (Clustering §12), each a JSON [`Document`] with a schema
 //! `version`, written with compare-and-swap through `data-avalanche-storage`.
+//!
+//! Every document is pretty JSON with `"version"` first. A document with another schema version
+//! is refused ([`StateError::UnsupportedVersion`]) and a malformed one is
+//! [`StateError::Corrupt`]; neither is ever treated as absent or overwritten. [`DocStore`] reads
+//! and writes them; [`DocStore::modify`] is the read-modify-write loop that retries lost races.
+//!
+//! `globals.state` (one [`DocStore`] rooted at `data_avalanche_storage::globals_state`):
+//! - [`establish_anchor`]: `cluster/anchor.json`, first writer wins. It holds `tokens`,
+//!   `lease_duration_ms`, `lock_backend`, `hash_algo` and the `fingerprint_key` for the config
+//!   crate's [`FingerprintKey`](https://docs.rs/) (config §6.2). Startup: load the config with a
+//!   random key, establish the anchor, run the config crate's `check_cluster_anchor`, then load
+//!   again with the anchor's key (`tests/startup.rs` runs the whole sequence).
+//! - [`ConfigGenerations`]: `config/generation.json` (CAS pointer) and content-addressed
+//!   `config/snapshots/<NNNNNN>-<hash>.yaml` holding the raw config text. A starting node adopts
+//!   the current generation; the first one bootstraps generation 1 (Clustering §9.3).
+//! - [`Leases`]: `leases/<pipeline_id>.json`: acquire, renew, observe, take over (epoch + 1) and
+//!   release (a tombstone that keeps the epoch). Timing — renew interval, takeover wait, safety
+//!   deadline — is the renewer's job (M8). T1–T10 in `tests/lease_conformance.rs` prove the CAS
+//!   on every backend. Known limitation: deleting a lease object by hand restarts its epoch at 1;
+//!   never delete lease objects.
+//!
+//! Per pipeline (one [`PipelineDocs`] rooted at the pipeline's state location):
+//! [`PipelineDocs::open`] creates or checks `pipeline-state.json` (refusing another pipeline's
+//! state or a changed `sink_type`), the desired state, `replication/slot.json` (PostgreSQL),
+//! `replication/position.json` (MySQL / MariaDB, with the source server identity), per-table
+//! snapshot state and on-demand snapshot jobs.
 
 mod anchor;
 mod doc;
```

Create `crates/data-avalanche-state/tests/startup.rs`:

```rust
//! The M2 startup sequence end to end, on in-memory buckets (config crate docs, "Startup and
//! reload"): load the config with a random fingerprint key to learn `globals.state`; read or
//! create the cluster anchor there; check this node's cluster settings against it (V-062, V-076,
//! V-094); load again with the anchor's key; adopt or bootstrap the config generation; open each
//! pipeline's state.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use data_avalanche_config::{
    ClusterAnchor, FingerprintKey, LockBackend, Pipeline, ResolvedConfig, StateLocation,
    check_cluster_anchor, load_str,
};
use data_avalanche_state::{
    AnchorDoc, ConfigGenerations, DesiredState, DocStore, HashAlgo, PipelineDocs, StateError,
    establish_anchor,
};
use data_avalanche_storage::{
    Location, ObjectStoreVersioned, colocated_state, explicit_state, globals_state,
};
use object_store::memory::InMemory;

const CONFIG: &str = r#"
config_version: 1
globals:
  state: { bucket: app-state, prefix: cluster-a }
  cluster: { tokens: 64, lease_duration: 20s }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      delta: {}
  invoices:
    id: invoices
    source:
      host: db.example.com
      db: billing
      user: cdc
      password: ${PW}
      postgres: { publication: invoices_pub, slot: invoices_slot }
    tables:
      include: [public.invoices]
    sink:
      type: delta
      bucket: lake
      delta: {}
    state: { bucket: ops, prefix: invoices-state }
"#;

/// One in-memory object store per bucket name, shared by every simulated node.
#[derive(Default)]
struct Buckets(Mutex<HashMap<String, Arc<InMemory>>>);

impl Buckets {
    fn docs(&self, at: &Location) -> DocStore {
        let store = self
            .0
            .lock()
            .unwrap()
            .entry(at.bucket.clone())
            .or_default()
            .clone();
        DocStore::new(Arc::new(
            ObjectStoreVersioned::new(store, &at.root).unwrap(),
        ))
    }
}

fn env() -> BTreeMap<String, String> {
    BTreeMap::from([("PW".to_string(), "s3cret".to_string())])
}

fn load(text: &str, key: &FingerprintKey) -> ResolvedConfig {
    load_str(text, &env(), key)
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config
}

fn state_location(p: &Pipeline) -> Location {
    match &p.state {
        StateLocation::CoLocated => colocated_state(&p.sink.bucket, &p.sink.prefix),
        StateLocation::Explicit {
            bucket,
            parent_prefix,
            prefix,
        } => explicit_state(bucket, parent_prefix.as_deref(), prefix),
    }
}

struct Started {
    config: ResolvedConfig,
    anchor: AnchorDoc,
}

/// Steps 1–5 for one node. `Err` carries the refusal a real node would print.
async fn start(buckets: &Buckets, node: &str, text: &str) -> Result<Started, String> {
    // 1. First load: only globals.state and the cluster settings matter; the key is throwaway.
    let first = load(text, &FingerprintKey::random());
    let g = &first.globals;
    let globals = buckets.docs(&globals_state(&g.state.bucket, &g.state.prefix));

    // 2. Read or create the anchor; the creator generates the fingerprint key.
    let proposal = AnchorDoc {
        tokens: g.cluster.tokens,
        lease_duration_ms: u64::try_from(g.cluster.lease_duration.as_millis()).unwrap(),
        lock_backend: g.cluster.lock_backend.to_string(),
        hash_algo: HashAlgo::Xxh3,
        fingerprint_key: *FingerprintKey::random().as_bytes(),
        established_by: node.to_owned(),
        established_at: Utc::now(),
    };
    let anchor = establish_anchor(&globals, &proposal)
        .await
        .map_err(|e| e.to_string())?
        .doc;

    // 3. This node's settings must equal the cluster's.
    let established = ClusterAnchor {
        tokens: anchor.tokens,
        lock_backend: anchor
            .lock_backend
            .parse::<LockBackend>()
            .map_err(|e| e.to_string())?,
        lease_duration: Duration::from_millis(anchor.lease_duration_ms),
    };
    let report = check_cluster_anchor(g, &established);
    if !report.is_ok() {
        return Err(report
            .iter()
            .map(|d| format!("{} {}", d.rule, d.message))
            .collect::<Vec<_>>()
            .join("; "));
    }

    // 4. The running config: loaded with the cluster's key.
    let config = load(text, &FingerprintKey::from_bytes(anchor.fingerprint_key));

    // 5. Adopt the cloud generation, or bootstrap it from this file.
    ConfigGenerations::new(globals)
        .bootstrap_or_adopt(text, node, Utc::now())
        .await
        .map_err(|e| e.to_string())?;
    Ok(Started { config, anchor })
}

#[tokio::test]
async fn nodes_share_the_anchor_and_fingerprint_key() {
    let buckets = Buckets::default();
    let a = start(&buckets, "node-a", CONFIG).await.unwrap();
    let b = start(&buckets, "node-b", CONFIG).await.unwrap();
    assert_eq!(a.anchor, b.anchor, "node-b adopts node-a's anchor");
    assert_eq!(
        (a.anchor.established_by.as_str(), a.anchor.tokens),
        ("node-a", 64)
    );
    let creds = |s: &Started| {
        serde_json::to_value(&s.config.pipelines["orders"].source.credentials).unwrap()
    };
    assert_eq!(
        creds(&a),
        creds(&b),
        "the same secret fingerprints the same on every node"
    );
    assert!(!creds(&a).to_string().contains("s3cret"));

    let random = load(CONFIG, &FingerprintKey::random());
    let other = serde_json::to_value(&random.pipelines["orders"].source.credentials).unwrap();
    assert_ne!(
        creds(&a),
        other,
        "without the anchor's key the fingerprints would disagree"
    );
}

#[tokio::test]
async fn a_node_with_other_cluster_settings_refuses_to_start() {
    let buckets = Buckets::default();
    start(&buckets, "node-a", CONFIG).await.unwrap();
    let other = CONFIG.replace(
        "tokens: 64, lease_duration: 20s",
        "tokens: 128, lease_duration: 20s",
    );
    let e = start(&buckets, "node-b", &other)
        .await
        .err()
        .expect("node-b must refuse");
    assert_eq!(
        e,
        "V-062 tokens differs from the cluster's established value 64; every node must use the same value"
    );
    let elsewhere = other.replace("prefix: cluster-a", "prefix: cluster-b");
    start(&buckets, "node-c", &elsewhere)
        .await
        .expect("another globals.state prefix is another cluster");
}

#[tokio::test]
async fn a_restarted_node_adopts_the_cloud_generation_not_its_file() {
    let buckets = Buckets::default();
    start(&buckets, "node-a", CONFIG).await.unwrap();
    let edited = format!("{CONFIG}# edited after bootstrap\n");
    start(&buckets, "node-a", &edited).await.unwrap();
    let globals = buckets.docs(&globals_state("app-state", "cluster-a"));
    let current = ConfigGenerations::new(globals)
        .current()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (current.pointer.doc.generation, current.text.as_str()),
        (1, CONFIG)
    );
}

#[tokio::test]
async fn pipeline_state_lives_where_config_section_10_puts_it() {
    let buckets = Buckets::default();
    let node = start(&buckets, "node-a", CONFIG).await.unwrap();
    let mut locations = vec![];
    for p in node.config.pipelines.values() {
        let at = state_location(p);
        let docs = PipelineDocs::new(buckets.docs(&at));
        let opened = docs.open(&p.id, &p.sink.kind.to_string()).await.unwrap();
        assert_eq!(opened.doc.desired_state, DesiredState::Running);
        locations.push((p.id.clone(), at));
    }
    locations.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        locations,
        vec![
            (
                "invoices".into(),
                Location {
                    bucket: "ops".into(),
                    root: "invoices-state".into()
                }
            ),
            (
                "orders".into(),
                Location {
                    bucket: "lake".into(),
                    root: "orders/.pipeline-state".into()
                }
            ),
        ]
    );

    let orders = &node.config.pipelines["orders"];
    let docs = PipelineDocs::new(buckets.docs(&state_location(orders)));
    let e = docs.open("orders", "iceberg").await.unwrap_err();
    assert!(matches!(e, StateError::SinkTypeMismatch { .. }), "{e:?}");
}
```

Change `crates/data-avalanche-storage/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-storage/src/lib.rs b/crates/data-avalanche-storage/src/lib.rs
index 42aa4ca..a49af2f 100644
--- a/crates/data-avalanche-storage/src/lib.rs
+++ b/crates/data-avalanche-storage/src/lib.rs
@@ -1,5 +1,22 @@
 //! Object storage for data-avalanche: `object_store` backends, the path rules of config §10, and
 //! [`VersionedStore`], the compare-and-swap interface every coordination and state document uses.
+//!
+//! - **Opening a bucket:** [`open_bucket`] takes a [`BucketSpec`] — [`Backend`] (S3, GCS, Azure),
+//!   bucket or container, extra `object_store` options (emulator endpoints) and [`Timeouts`].
+//!   Credentials come from each cloud's standard chain. [`Timeouts::COORDINATION`] suits leases
+//!   and state: a hung store fails within about 8 s.
+//! - **Where things live:** [`table_root`], [`colocated_state`], [`explicit_state`] and
+//!   [`globals_state`] give a [`Location`] (bucket + key prefix, `""` = bucket root).
+//!   [`escape_segment`] makes a database identifier safe as a key segment.
+//! - **Compare-and-swap:** [`ObjectStoreVersioned`] roots a [`VersionedStore`] at a
+//!   [`Location`]'s prefix: `get`, `create` (if absent), `update` (if the version still matches),
+//!   `put`, `list`. Failures are a [`StoreError`]: `Conflict` (the condition failed; also an update
+//!   of a missing object, on every backend), `Contention` (S3 409 after `object_store`'s retries:
+//!   read again), `Transient`, `Permanent`. The version token is the ETag on S3 and Azure and the
+//!   generation on GCS; [`ObjectVersion`] carries both and callers treat it as opaque.
+//!
+//! There is no conditional delete in `object_store`; releasing an object is a CAS write of a
+//! tombstone (see `data-avalanche-state`'s leases).
 
 mod backend;
 mod paths;
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p data-avalanche-state`
Expected: PASS. These tests drive code from Tasks 4–10, so nothing fails first; a failure here is a
bug in an earlier task. `data-avalanche-state` unit tests 45; `tests/lease_conformance.rs` 6; `tests/races.rs` 3; `tests/startup.rs` 4.

- [ ] **Step 3: Build the docs with warnings as errors**

Run: `RUSTDOCFLAGS="-D warnings" cargo doc -p data-avalanche-storage -p data-avalanche-state --no-deps`
Expected: finishes with no warnings.

- [ ] **Step 4: Run the M2 exit checks**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: PASS (314 tests in the workspace), no clippy warnings, no fmt output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-state/Cargo.toml \
  crates/data-avalanche-state/src/lib.rs \
  crates/data-avalanche-state/tests/startup.rs \
  crates/data-avalanche-storage/src/lib.rs \
  Cargo.lock
git commit -m "test(state): startup sequence end to end; docs(storage, state): crate docs"
```


---

## M2 exit checklist

The roadmap's M2 exit criteria, and where each is met:

- [x] **T1–T10 pass on RustFS, Azurite and real GCS** (2026-09-29: RustFS and Azurite in the controller's run, real GCS in the owner's run) (`tests/lease_conformance.rs`, run in
  `ci.yml`'s integration job); nothing needed skipping on either emulator.
- [x] **The recorded run shows T1–T10 passing on real S3, real GCS and real Azure.** Real GCS: the owner's run (2026-09-29). Real S3 and Azure: `cloud.yml` run 36604762684 (2026-09-29, at 3a287dd).
  - Real S3 and Azure: `cloud.yml`, run once by manual dispatch after the owner sets its secrets.
  - Real GCS: the integration job.
  
  Record each date in `test-infra/README.md` ("Lease conformance T1–T10").
- [x] **State round-trips and CAS races pass**:
  - JSON shape and round-trip tests: Tasks 5, 7, 8, 9, 10.
  - N writers → 1 winner: `tests/races.rs`, Task 4 `concurrent_creates_have_one_winner`, Task 5
    `concurrent_acquirers_have_one_winner`, Task 7 `concurrent_establishers_agree_on_one_anchor`,
    Task 8 `concurrent_bootstraps_adopt_one_winner`.
- [x] **Anchor mismatch detection is tested**: `tests/startup.rs`
  `a_node_with_other_cluster_settings_refuses_to_start`.
- [x] `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` and `RUSTDOCFLAGS="-D warnings" cargo doc -p data-avalanche-storage
  -p data-avalanche-state --no-deps` are clean (2026-09-29: 330 tests).
- [x] Roadmap §3.5 is replaced by the landed storage/state interface (D-M2.2's error set, the
  state crate's entry points), and R-2 records the `object_store` re-proof, per the doc/v1.0
  README.
- [x] The table snapshot document matches `Data-Flow.md` §State Layout as amended by S-M (approved and applied 2026-09-28).
