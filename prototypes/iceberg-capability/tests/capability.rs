//! iceberg-rust capability assessment (see `src/lib.rs` and gap-analysis §24.2.1/§24.2.4).
//!
//! RESULT (re-verified 2026-09-19 against compiled iceberg 0.10.1 and the
//! v0.11.0-rc.1/rc.2 surfaces): the public write API is APPEND-ONLY — delete files
//! can be written (`EqualityDeleteFileWriter`) but NOT committed (the `Transaction`
//! surface is `fast_append` + metadata actions only; no row-delta/overwrite/rewrite).
//! So the MOR-equality-delete sink (G1 / DA-SINK-1) is NOT buildable on any
//! published version, and the COW fallback is equally blocked.
//!
//! G1's probe cannot run yet — the capability is absent. Re-run when iceberg-rust
//! merges + publishes a delete-aware commit action (README has the G1 plan). The
//! append-path probe (DA-SINK-2 carrier) IS live and passing:
//! `tests/probe_append_and_summary.rs`.
//!
//!   cargo test  -p iceberg-capability                  # prints the verdict (green)
//!   cargo test  -p iceberg-capability --features probe # canary + G2 append-path probe

/// Always-on: prints the capability verdict (and stays green).
#[test]
fn capability_report() {
    iceberg_capability::print_scaffold_status();
}

/// Canary (only with `--features probe`): forces the iceberg dep to compile so a
/// crate bump that changes the gating types surfaces here. When `Transaction`
/// gains a delete-aware action, that is the signal to implement G1 for real.
#[cfg(feature = "probe")]
#[test]
fn iceberg_dep_canary() {
    // Referencing the type alias is enough to require iceberg to build.
    fn _assert<T>() {}
    _assert::<iceberg_capability::probe::TransactionCanary>();
}
