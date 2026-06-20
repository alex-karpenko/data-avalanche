//! iceberg-capability — verification probe for the iceberg-rust behaviors the
//! CDC design assumes but cannot confirm on paper.
//!
//! Gates (see `doc/analysis/CDC_Design_Gap_Analysis_v1.md` §21 / §24):
//!
//! | Gate | DA-ID | Proves |
//! |------|-------|--------|
//! | G1 | **DA-SINK-1** 🔴 | An equality-delete file gets the **enclosing commit's** data-sequence-number, so a same-commit `delete(K)+insert(K')` purges prior-commit `K` but spares the same-commit `K'` (the ghost-row strictly-less-than rule, `Ghost_Row_Dedup_and_Primary_Keys.md` §0). |
//! | G2 | **DA-SINK-2** 🧪 | Custom `cdc.*` snapshot-summary properties are readable and survive across commits (`owner_epoch` fence + `committed_position` guard carrier). |
//! | G3 | **DA-SCHEMA-8** 🧪 | `add_column` / `drop_column` / `rename_column` / type-promotion commit as metadata-only ops, with **field-IDs preserved on rename**. |
//! | G4 | **DA-TYPE-5** 🧪 | A field-level hint maps Arrow `FixedSizeBinary(16)` → Iceberg `uuid` (not `fixed(16)`). |
//! | G5 | **DA-SINK-10** 🧪 (deferred) | API-surface presence of rewrite/compaction/expire entry points — informational only; v1 maintenance is external. |
//!
//! **Only G1 can force a redesign** (position-deletes / COW). G2–G4 degrade
//! gracefully (idempotency is correctness; halt-the-table; cosmetic uuid). G5 is
//! deferred. The triage lives in gap-analysis §24.0.
//!
//! The iceberg-touching code is behind the `probe` feature; the default build is
//! a green scaffold-status check (`tests/capability.rs`).

/// Outcome of one capability gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Gate {
    /// The assumed behavior holds against the pinned crate.
    Pass,
    /// The assumed behavior does NOT hold — consult the fallback in gap-analysis §24.
    Fail(String),
    /// Not run (probe feature off, or no warehouse configured).
    Skip(String),
}

impl Gate {
    pub fn is_blocking_fail(&self, da_id: &str) -> bool {
        matches!(self, Gate::Fail(_)) && da_id == "DA-SINK-1"
    }
}

/// The five gate ids in execution order, with their severity tag.
pub const GATES: &[(&str, &str, &str)] = &[
    ("G1", "DA-SINK-1", "equality-delete same-commit sequencing (BLOCKER)"),
    ("G2", "DA-SINK-2", "custom snapshot-summary properties persist"),
    ("G3", "DA-SCHEMA-8", "drop/rename/promote schema-evolution ops"),
    ("G4", "DA-TYPE-5", "FixedSizeBinary(16) -> uuid annotation"),
    ("G5", "DA-SINK-10", "rewrite/compaction API surface (deferred)"),
];

/// Print the gate roster + run guidance. Used by the default (no-feature) build
/// so `cargo test` is self-documenting and green.
pub fn print_scaffold_status() {
    println!("iceberg-capability — probe scaffold");
    println!("  default build runs no iceberg code; enable with `--features probe`.");
    println!("  gates:");
    for (g, da, what) in GATES {
        println!("    {g}  {da:<12} {what}");
    }
    println!("  fallbacks + full plan: doc/analysis/CDC_Design_Gap_Analysis_v1.md §24");
}

// ---------------------------------------------------------------------------
// Probe helpers (compiled only with `--features probe`).
// ---------------------------------------------------------------------------
#[cfg(feature = "probe")]
pub mod probe {
    //! Shared helpers for the gate tests. Every `// VERIFY:` marks a call whose
    //! exact signature must be checked against the pinned iceberg-rust 0.9.0.

    use std::collections::HashMap;

    /// Build an in-process Memory catalog over a temp local-fs warehouse so the
    /// probe is fully self-contained (no cloud needed for the functional gates).
    ///
    /// Returns the catalog handle and the temp dir guard (drop = cleanup).
    pub async fn temp_catalog() -> anyhow::Result<()> {
        // VERIFY: the 0.9.0 constructor + storage wiring. Sketch:
        //   let tmp = tempfile::tempdir()?;
        //   let warehouse = format!("file://{}", tmp.path().display());
        //   let file_io = iceberg::io::FileIOBuilder::new("fs").build()?;   // VERIFY
        //   let catalog = iceberg_catalog_memory::MemoryCatalog::new(file_io, Some(warehouse)); // VERIFY
        //   Ok((catalog, tmp))
        todo!("wire MemoryCatalog over a tempfile fs warehouse (VERIFY 0.9.0 API)")
    }

    /// A 2-column identifier'd schema: `id uuid (required, identifier)`, `v string`.
    /// Used by G1/G2/G3. The uuid column also exercises G4's annotation path.
    pub fn cdc_schema() -> anyhow::Result<()> {
        // VERIFY: iceberg::spec::{Schema, NestedField, Type, PrimitiveType}
        //   NestedField::required(1, "id", Type::Primitive(PrimitiveType::Uuid))
        //   NestedField::optional(2, "v",  Type::Primitive(PrimitiveType::String))
        //   Schema::builder().with_fields(..).with_identifier_field_ids([1]).build()
        todo!("build Schema with identifier-field-ids = [id] (VERIFY 0.9.0 API)")
    }

    /// Read the current snapshot's summary as a string map (G2).
    pub fn current_summary() -> anyhow::Result<HashMap<String, String>> {
        // VERIFY: table.metadata().current_snapshot()?.summary() -> &Summary { other: HashMap<..> , .. }
        todo!("read current_snapshot().summary() (VERIFY 0.9.0 API)")
    }
}
