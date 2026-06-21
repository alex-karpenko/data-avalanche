//! iceberg-rust 0.9.1 capability assessment (see `src/lib.rs` and gap-analysis §24.2.1).
//!
//! RESULT: 0.9.1's public write API is APPEND-ONLY — delete files can be written
//! (`EqualityDeleteFileWriter`) but NOT committed (the `Transaction` surface is
//! `fast_append` + metadata actions only; no row-delta/overwrite/rewrite, no
//! `update_schema`). So the MOR-equality-delete sink (G1 / DA-SINK-1) is NOT
//! buildable on 0.9.1, and the COW fallback is equally blocked.
//!
//! There is no passing probe to run yet — the capability is absent. Re-run when
//! iceberg-rust gains a delete-aware commit action (README has the G1 plan).
//!
//!   cargo test  -p iceberg-capability                  # prints the verdict (green)
//!   cargo build -p iceberg-capability --features probe # canary: iceberg still resolves/compiles

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
