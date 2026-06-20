//! iceberg-rust capability gates (G1–G5), from
//! `doc/analysis/CDC_Design_Gap_Analysis_v1.md` §24.2–§24.6.
//!
//! Default `cargo test` runs only `scaffold_status` (green, no iceberg code).
//! The real probes compile + run under `--features probe`:
//!
//! ```bash
//! cargo test -p iceberg-capability --features probe -- --nocapture
//! ```
//!
//! The probe bodies are written against the *documented* iceberg-rust 0.9.0
//! surface; reconcile every `// VERIFY:` call against the pinned crate before the
//! first `--features probe` compile. A `todo!()` marks a step whose exact API
//! must be filled in — it is an honest "implement against the pin", not a pass.

/// Always-compiled, always-green: prints the gate roster and how to run the probes.
#[test]
fn scaffold_status() {
    iceberg_capability::print_scaffold_status();
}

// ===========================================================================
// G1 — DA-SINK-1 🔴  equality-delete same-commit sequencing
// ===========================================================================
//
// The blocker. If this fails, the MOR equality-delete writer is unusable as
// designed and the sink moves to position-deletes-after-read or COW
// (Ghost_Row §8 fallback). Verify EARLY — it gates the sink-writer spec.
//
// Plan (gap-analysis §24.2):
//   1. MOR table with identifier-field-ids = [id].
//   2. Commit C1: insert (id=K, v="a")                       -> data seq s1
//   3. Commit C2 (one txn): equality-delete(id=K) + insert(id=K, v="b")
//   4. ASSERT read: exactly {id=K, v="b"}  (C1's K purged, same-commit K spared)
//   5. ASSERT metadata: C2 delete-file seq == C2 data seq
//   6. CROSS-READ with Spark/Trino: result agrees with iceberg-rust's own reader
#[cfg(feature = "probe")]
#[tokio::test]
async fn g1_equality_delete_same_commit_sequencing() -> anyhow::Result<()> {
    // let (catalog, _tmp) = iceberg_capability::probe::temp_catalog().await?;
    // ... create MOR table (cdc_schema) ...
    //
    // VERIFY: the equality-delete writer path in 0.9.0:
    //   - iceberg::writer::base_writer::equality_delete_writer::* (builder)
    //   - the data writer + equality-delete writer must land in ONE Transaction
    //     so they share the commit's data-sequence-number.
    //   let mut txn = iceberg::transaction::Transaction::new(&table);
    //   // append C2 data file (insert K=b) AND C2 equality-delete file (key id=K)
    //   txn.commit(&catalog).await?;
    //
    // let rows = scan_all(&table).await?;            // VERIFY: TableScan -> arrow batches
    // assert_eq!(rows, vec![("K".into(), "b".into())], "same-commit insert must survive; C1 must be purged");
    //
    // let dseq = c2_data_file_seq(&table);           // VERIFY: manifest entry sequence_number
    // let xseq = c2_delete_file_seq(&table);
    // assert_eq!(dseq, xseq, "equality-delete file must carry the enclosing commit's sequence number");
    //
    // CROSS-READ (manual / CI step, not in-crate): point Spark or Trino at the
    // temp warehouse and confirm the same single row. Record the result in CI.
    todo!("G1: implement against iceberg-rust 0.9.0 equality-delete writer (VERIFY)");
}

// ===========================================================================
// G2 — DA-SINK-2 🧪  custom snapshot-summary properties persist across commits
// ===========================================================================
//
// Carrier for cdc.owner_epoch (clustering fence) and cdc.committed_position
// (delivery guard). If it fails: move committed_position to per-pipeline state
// storage; drop owner_epoch to advisory (Delivery §4 / §24.3). Not a blocker.
#[cfg(feature = "probe")]
#[tokio::test]
async fn g2_snapshot_summary_roundtrip() -> anyhow::Result<()> {
    // commit #1 with summary props { "cdc.owner_epoch":"5", "cdc.committed_position":"0/1A2B3C4" }
    //   VERIFY: Transaction / SnapshotProduceAction lets you set summary "other" props.
    // let s1 = iceberg_capability::probe::current_summary()?;
    // assert_eq!(s1.get("cdc.owner_epoch").map(String::as_str), Some("5"));
    //
    // commit #2 with owner_epoch="6" (re-assert: summaries are per-snapshot, not inherited)
    // let s2 = iceberg_capability::probe::current_summary()?;
    // assert_eq!(s2.get("cdc.owner_epoch").map(String::as_str), Some("6"));
    //
    // commit #3 WITHOUT setting the prop, after explicitly carrying it forward from s2;
    //   confirm the writer CAN read the prior summary to carry values across commits.
    todo!("G2: implement summary set + read-back across 3 commits (VERIFY)");
}

// ===========================================================================
// G3 — DA-SCHEMA-8 🧪  drop / rename / type-promote as metadata-only
// ===========================================================================
//
// Rename correctness rests on field-ID stability. If an op is unsupported:
// route it into the "halt table" branch (Schema_Change §7.1) — already the
// safety fallback. Not a blocker.
#[cfg(feature = "probe")]
#[tokio::test]
async fn g3_schema_evolution_ops() -> anyhow::Result<()> {
    // VERIFY: Transaction::update_schema()/UpdateSchemaAction in 0.9.0.
    //   add_column("c", optional string)            -> ASSERT present, nullable
    //   let fid = field_id("v");
    //   rename_column("v" -> "v2")                   -> ASSERT field_id("v2") == fid  (ID PRESERVED)
    //   update_column("n": int -> long)             -> ASSERT promotion, metadata-only (no data rewrite)
    //   drop_column("c")                             -> ASSERT gone
    // Any op returning Unsupported is a Gate::Fail for THAT op only -> document the halt fallback.
    todo!("G3: exercise add/rename/promote/drop, assert field-id preserved on rename (VERIFY)");
}

// ===========================================================================
// G4 — DA-TYPE-5 🧪  FixedSizeBinary(16) -> uuid annotation
// ===========================================================================
//
// If unavailable: store uuid as fixed(16) or string (cosmetic logical-type
// loss; data intact). Not a blocker.
#[cfg(feature = "probe")]
#[tokio::test]
async fn g4_uuid_annotation() -> anyhow::Result<()> {
    // Build an Arrow field FixedSizeBinary(16) with the iceberg hint metadata,
    // OR build the Iceberg schema directly with PrimitiveType::Uuid, then create
    // the table and read the column type back.
    //   VERIFY: the hint key the 0.9.0 arrow->iceberg converter honors, e.g.
    //           field metadata {"PARQUET:field_id": ..} + {"iceberg.type":"uuid"}
    //           or schema-builder PrimitiveType::Uuid directly.
    // assert_eq!(iceberg_column_type("id"), "uuid");  // not "fixed[16]"
    todo!("G4: assert FixedSizeBinary(16) resolves to Iceberg uuid (VERIFY)");
}

// ===========================================================================
// G5 — DA-SINK-10 🧪 (deferred)  rewrite/compaction API surface
// ===========================================================================
//
// Informational only — v1 maintenance is external (Spark/Trino). This is a
// compile-time presence check, not a behavioral assertion. No v1 impact.
#[cfg(feature = "probe")]
#[test]
fn g5_compaction_api_surface() {
    // Does 0.9.0 expose rewrite_data_files / rewrite_position_delete_files /
    // expire_snapshots equivalents? If not, built-in maintenance stays deferred
    // (already the plan). Record findings in the README results table.
    eprintln!("G5: API-surface check is a manual/doc note — see Iceberg_Maintenance.md §14");
}
