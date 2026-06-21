//! iceberg-capability — iceberg-rust 0.9.1 capability assessment for the CDC sink.
//!
//! **RESULT (2026-06): iceberg-rust 0.9.1's public write API is APPEND-ONLY.**
//! `EqualityDeleteFileWriter` / `PositionDeleteFileWriter` exist (delete files can be
//! *written*), but the `Transaction` commit surface is data-file-only — `fast_append`
//! + a few metadata actions, with **no row-delta / overwrite / rewrite / delete-commit
//! action and no `update_schema`**. So the MOR-equality-delete sink (and its COW
//! fallback) is **not buildable on 0.9.1**. Full analysis + options:
//! `doc/analysis/CDC_Design_Gap_Analysis_v1.md` §24.2.1.
//!
//! | Gate | DA-ID | Verdict (0.9.1) |
//! |------|-------|-----------------|
//! | G1 | **DA-SINK-1** | 🔴 Unsupported — delete files can be written but not committed (blocks the sink). |
//! | G2 | **DA-SINK-2** | ⏸ Moot until deletes can be committed. |
//! | G3 | **DA-SCHEMA-8** | 🔴 Unsupported — no `update_schema` transaction action. |
//! | G4 | **DA-TYPE-5** | 🟡 Likely OK — schema-definition only, independent of the commit path. |
//! | G5 | **DA-SINK-10** | 🔴 (expected) — no rewrite/compaction API; confirms external-maintenance-for-v1. |
//!
//! Re-run when iceberg-rust gains a delete-aware commit action (see README). The
//! `probe` feature pulls iceberg so this can be re-checked on a crate bump.

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

/// The five gates with their 0.9.1 verdict.
pub const GATES: &[(&str, &str, &str)] = &[
    (
        "G1",
        "DA-SINK-1",
        "🔴 UNSUPPORTED — delete files writable, NOT committable (append-only Transaction)",
    ),
    ("G2", "DA-SINK-2", "⏸ moot until deletes can be committed"),
    (
        "G3",
        "DA-SCHEMA-8",
        "🔴 UNSUPPORTED — no update_schema transaction action",
    ),
    ("G4", "DA-TYPE-5", "🟡 likely OK — schema-definition only"),
    (
        "G5",
        "DA-SINK-10",
        "🔴 expected — no rewrite/compaction API",
    ),
];

/// Print the capability verdict. Used by the default (no-feature) build so
/// `cargo test` reports the finding and stays green.
pub fn print_scaffold_status() {
    println!("iceberg-capability — iceberg-rust 0.9.1 capability assessment");
    println!("  RESULT: 0.9.1 public write API is APPEND-ONLY (fast_append data files only);");
    println!(
        "          deletes/overwrites/rewrites and update_schema are NOT in the Transaction API."
    );
    println!("  => MOR-equality-delete sink (and COW fallback) NOT buildable on 0.9.1.");
    println!("  gate verdicts:");
    for (g, da, verdict) in GATES {
        println!("    {g}  {da:<12} {verdict}");
    }
    println!("  full analysis + options: doc/analysis/CDC_Design_Gap_Analysis_v1.md §24.2.1");
}

// ---------------------------------------------------------------------------
// Re-check hook (`--features probe`).
// ---------------------------------------------------------------------------
//
// There is intentionally NO probe implementation here yet: on iceberg 0.9.1 the
// blocking gate (G1) is UNSUPPORTED because the `Transaction` commit API cannot
// commit delete files (it is append-only). So the probe can only be *built* once
// iceberg-rust gains a delete-aware commit action.
//
// This module exists so `cargo build --features probe` exercises the iceberg dep
// and acts as a CANARY: if a crate bump removes/relocates these types, it breaks
// here, prompting a re-run of the §24.2.1 assessment. The single thing to look for
// on a bump is a NEW delete-aware action on `Transaction` (row-delta / overwrite /
// merge-append). When that lands, implement G1 per the README.
#[cfg(feature = "probe")]
pub mod probe {
    /// Canary against a confirmed 0.9.1 path: the transaction whose action surface
    /// gates the commit. Re-inspect `Transaction`'s methods on any iceberg bump for a
    /// new delete-aware action (row-delta / overwrite / merge-append); 0.9.1 has none.
    pub type TransactionCanary = iceberg::transaction::Transaction;
}
