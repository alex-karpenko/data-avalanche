//! iceberg-capability — iceberg-rust capability assessment for the CDC sink.
//!
//! **RESULT (2026-06, re-verified 2026-09-19 against the compiled 0.10.1 crate):
//! iceberg-rust's public write API is APPEND-ONLY.**
//! `EqualityDeleteFileWriter` / `PositionDeleteFileWriter` exist (delete files can be
//! *written*), but the `Transaction` commit surface is data-file-only — `fast_append`
//! plus a few metadata actions, with **no row-delta / overwrite / rewrite / delete-commit
//! action**. So the MOR-equality-delete sink (and its COW fallback) is **not buildable
//! on any published version**, and even the v0.11.0 release candidates (rc.1/rc.2)
//! keep the same append-only surface. Full analysis + options:
//! `doc/analysis/CDC_Design_Gap_Analysis_v1.md` §24.2.1/§24.2.4.
//!
//! | Gate | DA-ID | Verdict |
//! |------|-------|---------|
//! | G1 | **DA-SINK-1** | 🔴 Unsupported — delete files can be written but not committed (blocks the sink). |
//! | G2 | **DA-SINK-2** | 🟢 Flash-verified on the only existing commit path (append): custom `cdc.*` snapshot-summary properties round-trip atomically (see `tests/probe_append_and_summary.rs`). Full gate still re-check when a delete-commit path exists. |
//! | G3 | **DA-SCHEMA-8** | 🟡 `update_schema` exists; drop/rename/promotion extent still unverified. |
//! | G4 | **DA-TYPE-5** | 🟡 Likely OK — schema-definition only, independent of the commit path (not yet exercised here). |
//! | G5 | **DA-SINK-10** | 🟡 `expire_snapshots` landed; rewrite/compaction still absent (external maintenance for v1). |
//!
//! Re-run when iceberg-rust gains a delete-aware commit action (see README). The
//! `probe` feature pulls iceberg so this can be re-checked on a crate bump, and
//! `tests/probe_append_and_summary.rs` is a real (passing) probe of the append path.

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

/// The six gates with their verdict from the 2026-09-19 recheck (iceberg 0.10.1 compiled).
pub const GATES: &[(&str, &str, &str)] = &[
    (
        "G1",
        "DA-SINK-1",
        "🔴 UNSUPPORTED — delete files writable, NOT committable (append-only Transaction; same in v0.11.0-rc.1/rc.2)",
    ),
    (
        "G2",
        "DA-SINK-2",
        "🟢 flash-verified on append path — cdc.* summary props round-trip atomically (tests/probe_append_and_summary.rs)",
    ),
    (
        "G3",
        "DA-SCHEMA-8",
        "🟡 update_schema exists; drop/rename/promotion extent unverified",
    ),
    (
        "G4",
        "DA-TYPE-5",
        "🟡 likely OK — schema-definition only",
    ),
    (
        "G5",
        "DA-SINK-10",
        "🟡 expire_snapshots exists; rewrite/compaction absent (external maintenance for v1)",
    ),
];

/// Print the capability verdict. Used by the default (no-feature) build so
/// `cargo test` reports the finding and stays green.
pub fn print_scaffold_status() {
    println!("iceberg-capability — iceberg-rust capability assessment (recheck 2026-09-19, iceberg 0.10.1 compiled)");
    println!("  RESULT: public write API is APPEND-ONLY (fast_append data files only);");
    println!(
        "          no delete-commit action in 0.10.1 — nor in the v0.11.0-rc.2 Transaction surface."
    );
    println!("  => MOR-equality-delete sink (and COW fallback) NOT buildable on any published version.");
    println!("  gate verdicts:");
    for (g, da, verdict) in GATES {
        println!("    {g}  {da:<12} {verdict}");
    }
    println!("  full analysis + options: doc/analysis/CDC_Design_Gap_Analysis_v1.md §24.2.1/§24.2.4");
}

// ---------------------------------------------------------------------------
// Re-check hook (`--features probe`).
// ---------------------------------------------------------------------------
//
// G1's full probe (equality-delete same-commit sequencing) still cannot be
// implemented: on every published version (and on the v0.11.0-rc.1/rc.2
// surfaces read from the repo) the `Transaction` commit API has no way to commit
// delete files — it is append-only. So the G1 probe can only run once iceberg-rust
// merges + publishes a delete-aware commit action.
//
// This module exists so `cargo build --features probe` exercises the iceberg dep
// and acts as a CANARY: if a crate bump removes/relocates these types, it breaks
// here, prompting a re-run of the §24.2 assessment. The single thing to look for
// on a bump is a NEW delete-aware action on `Transaction` (row-delta / overwrite /
// merge-append). When that lands, implement G1 per the README. Meanwhile
// `tests/probe_append_and_summary.rs` is a PASSING real probe of the one commit
// path that exists (append + cdc.* summary properties → DA-SINK-2 carrier).
#[cfg(feature = "probe")]
pub mod probe {
    /// Canary against a confirmed path (transaction module re-exported unchanged
    /// through 0.10.1): the struct whose action surface gates the commit.
    /// Re-inspect `Transaction`'s methods on any iceberg bump for a new
    /// delete-aware action (row-delta / overwrite / merge-append); none exist yet.
    pub type TransactionCanary = iceberg::transaction::Transaction;
}
