# iceberg-capability — iceberg-rust capability probe

Proves the iceberg-rust 0.9.0 behaviors the CDC **sink** and **clustering fence** assume but
that cannot be confirmed on paper. Companion to `prototypes/lease-conformance/` (which proves the
storage-CAS lease); together they close the six verification gates in
`doc/analysis/CDC_Design_Gap_Analysis_v1.md` §21 / §24.

This is a **prototype / verification harness**, not the production sink. It exists to answer one
question per gate with a runnable test before those behaviors are baked into the application spec.

## Gates

| Test | DA-ID | Sev | Proves | If it fails (fallback, gap-analysis §24) |
|---|---|---|---|---|
| G1 | DA-SINK-1 | 🔴 | equality-delete file gets the **enclosing commit's** sequence number → same-commit `delete(K)+insert(K')` purges prior `K`, spares same-commit `K'` | **redesign**: position-deletes-after-read or COW (§24.2) |
| G2 | DA-SINK-2 | 🧪 | custom `cdc.*` snapshot-summary props are readable + persist across commits | move `committed_position` to state storage; `owner_epoch` → advisory (§24.3) |
| G3 | DA-SCHEMA-8 | 🧪 | `add`/`drop`/`rename`/promote commit metadata-only; **field-ID preserved on rename** | route unsupported op to halt-table (§24.4) — already the safety branch |
| G4 | DA-TYPE-5 | 🧪 | `FixedSizeBinary(16)` → Iceberg `uuid` via annotation | store as `fixed(16)`/`string`, cosmetic (§24.5) |
| G5 | DA-SINK-10 | 🧪 (deferred) | rewrite/compaction API surface present | external maintenance (the v1 plan) — no impact (§24.6) |

**Only G1 can force a redesign.** G2–G4 degrade gracefully; G5 is deferred. So if you run nothing
else, run **G1** (and the lease suite's T10) before specifying the sink writer and the lease.

## Running

The iceberg-dependent probes are behind the `probe` feature and **off by default**, so plain
`cargo test` is green and self-documenting:

```bash
cargo test -p iceberg-capability                       # prints the gate roster, runs no iceberg code
cargo test -p iceberg-capability --features probe -- --nocapture   # compiles + runs the real probes
```

G1–G4 are self-contained: they run against an in-process Memory catalog over a temporary local-fs
warehouse — no cloud needed for the functional assertions. **G1 additionally requires a manual
cross-read** with a mature engine (Spark or Trino Iceberg) pointed at the temp warehouse, to confirm
the on-disk result independently of iceberg-rust's own reader; record that result in CI.

## Status: SCAFFOLD — API reconciliation required

The probe bodies encode the exact test plan (steps + assertions) but are written against the
*documented* iceberg-rust 0.9.0 surface. Every `// VERIFY:` marks a call whose signature must be
checked against the pinned crate, and each gate currently ends in `todo!()` — an honest "implement
against the pin", not a passing test. Before the first `--features probe` compile:

1. Pin `iceberg`, `iceberg-catalog-memory`, and the `arrow-*` crates to the exact versions the sink
   will use (must match the `iceberg-storage-opendal` arrow pin and MSRV 1.92.0).
2. Reconcile each `// VERIFY:` against the crate docs (writer builders, `Transaction` API,
   `update_schema`, snapshot-summary access, the uuid annotation key).
3. Replace the `todo!()` bodies with the assertions already spelled out in comments.

Record the outcome (Pass / Fail + the fallback taken) in the table above and feed it back into
gap-analysis §24 and `Ghost_Row_Dedup_and_Primary_Keys.md` §8.
