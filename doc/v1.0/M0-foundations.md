---
updated: 2026-09-26
domain: Architecture
tags: [v1, implementation-plan, m0, workspace, core-types, positions, gtid, mariadb, lsn, decimal, test-infra, ci]
sources: []
---

# M0 — Foundations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Cargo workspace, CI skeleton, the I/O-free domain types every later milestone
builds on (tables, values, decimals, logical types, all four source-position dialects, events), and
the container-based test infrastructure. Also record whether each storage emulator honours
conditional writes.

**Architecture:** One workspace at the repository root with `crates/*` members, excluding the
`prototypes/` probes. `data-avalanche-core` holds pure data types with unit and property tests.
`data-avalanche-testkit` holds integration-test helpers, gated on `DATA_AVALANCHE_IT`. `test-infra/compose.yaml`
runs PostgreSQL, MySQL, MariaDB, RustFS and Azurite. GCS tests use a real bucket, because
fake-gcs-server can't serve `object_store` (Task 12).

**Tech Stack:** Rust 1.98.1 (edition 2024), `thiserror` 2, `serde`/`serde_json`, `chrono` 0.4, `uuid` 1,
`proptest` 1, `tokio` 1, `object_store` 0.13, `tokio-postgres` 0.7, `mysql_async` =0.37.1, `rustls` 0.23
(aws-lc-rs), Docker Compose, GitHub Actions.

**Spec:** `doc/v1.0/00-roadmap.md` (§2 layout, §3.1–3.2 interfaces, §5 test strategy, §9 D-IMPL-1/4/5/6),
`doc/specs/Data-Flow.md` (§CdcEvent Structure, §Data Lineage), `doc/research/Delivery_Guarantee_and_Position_Guard.md`
§2, `doc/research/Type_Mapping.md`, `doc/specs/Configuration_Specification.md` §5 and §9.3
(`column_type_overrides` grammar).

## Global Constraints

- Toolchain pinned to `1.98.1` via `rust-toolchain.toml`; edition `2024`; `rust-version = "1.98"`.
- `unsafe_code = "forbid"` in every crate; `cargo clippy --workspace --all-targets -- -D warnings` must pass.
- `data-avalanche-core` performs **no I/O** and has no tokio dependency.
- Libraries return `thiserror` errors; no `anyhow` in library code; no `unwrap()`/`expect()` outside tests except where a documented invariant makes failure impossible.
- Integration tests run only when `DATA_AVALANCHE_IT` is set **and non-empty**; they print `skipped:` and return otherwise.
- GCS integration tests use a **real bucket** (`DATA_AVALANCHE_GCS_BUCKET` + `GOOGLE_SERVICE_ACCOUNT_PATH`), write only under
  `unique_prefix()` and clean up; `DATA_AVALANCHE_SKIP_GCS=1` is the only way to skip them when `DATA_AVALANCHE_IT` is set.
- Reserved column names (config §5, D-V1.1): `_op`, `_ts`, `_lsn`, `_seq`, `_source_db`, `_pipeline`, and the prefix `__data_avalanche_` — compared case-insensitively.
- Decimal precision ceiling is **38** digits (Arrow `Decimal128`, Delta/Iceberg `decimal`).
- Position tagged-string forms (D-IMPL-4): `pg:<X/Y>`, `mysql-gtid:<canonical set>`, `mariadb-gtid:<d-s-n,…>`, `binlog:<file>:<pos>`.
- Logical type grammar (config §9.3): `bool`, `int16`, `int32`, `int64`, `float32`, `float64`, `decimal(P,S)`, `string`, `binary`, `date`, `time`, `timestamp_tz`, `timestamp_ntz`, `uuid`, `json`, `list<T>` — lower-case only.
- The `prototypes/*` crates keep building on their own; the workspace must exclude them.
- Run `cargo fmt --all` before every commit. The code blocks in this plan show content, not final
  rustfmt layout.
- Unit-test filters use the module path (`cargo test -p data-avalanche-core gtid::`) so they don't also
  match test names in other modules.
- One commit per task, message prefix `feat(core):` / `build:` / `test(infra):` / `ci:` as indicated. Every commit ends with the session's attribution trailer if one is configured.

## Review Focus

These are the input classes and failure modes the specs imply but no single task naturally
exercises. Each is pinned by a test in the task named after the arrow.

1. **MySQL `gtid_executed` output with newlines and upper-case UUIDs.** The server prints
   `…:1-5,\n<uuid>:1-3`. Parsing must ignore all whitespace and normalize UUIDs to lower case, or
   `covers` silently returns false across a restart → Task 7 (`parses_multiline_uppercase_server_output`).
2. **MySQL 8.4 GTID tags** (`uuid:tag:1-3`). An untagged-only parser rejects a legitimate server
   position and the pipeline can't resume → Task 7 (`parses_and_prints_tagged_intervals`).
3. **Binlog file rotation across a digit boundary** (`.000999` → `.001000`). Comparing file names as
   strings mis-orders them → Task 9 (`orders_across_rotation_by_index_not_text`).
4. **PostgreSQL numeric specials** (`NaN`, `Infinity`, `-Infinity`, valid since PG 14). These must be
   rejected as non-finite, not mis-parsed → Task 3 (`rejects_non_finite_values`).
5. **Tiny decimals with a scale larger than their digit count** (`0.0005`). They must display with
   leading zeros; a naive formatter prints `.5` or `5` → Task 3 (`displays_leading_zeros_for_small_values`).

---

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` | Workspace root: members, exclusions, shared package metadata, lints, dependency versions |
| `rust-toolchain.toml` | Toolchain pin |
| `.github/workflows/ci.yml` | CI: fmt, clippy, unit tests; integration job against `test-infra` |
| `crates/data-avalanche-core/Cargo.toml` | Core crate manifest |
| `crates/data-avalanche-core/src/lib.rs` | Module list and re-exports |
| `crates/data-avalanche-core/src/table.rs` | `TableRef`, reserved-column rules |
| `crates/data-avalanche-core/src/decimal.rs` | `Decimal` (i128 unscaled + scale), parse/display/rescale/fits |
| `crates/data-avalanche-core/src/value.rs` | `ColumnValue`, `ColumnValueType`, `Row` |
| `crates/data-avalanche-core/src/logical.rs` | `LogicalType` grammar |
| `crates/data-avalanche-core/src/position/mod.rs` | `SourcePosition` dispatch, tagged strings, serde, `PositionError` |
| `crates/data-avalanche-core/src/position/lsn.rs` | PostgreSQL `Lsn` |
| `crates/data-avalanche-core/src/position/gtid.rs` | MySQL `GtidSet` (+ tags) |
| `crates/data-avalanche-core/src/position/mariadb.rs` | MariaDB `MariaGtidState` |
| `crates/data-avalanche-core/src/position/binlog.rs` | File/position `BinlogFilePos` |
| `crates/data-avalanche-core/src/event.rs` | `SourceSeq`, `Op`, `CdcEvent`, `TransactionBoundary` |
| `test-infra/compose.yaml` | Databases + storage emulators + one-shot init services |
| `test-infra/up.sh`, `test-infra/down.sh` | Bring the default (or matrix) profile up/down and initialise buckets |
| `test-infra/README.md` | Ports, credentials, profiles, emulator CAS results |
| `crates/data-avalanche-testkit/Cargo.toml`, `src/lib.rs` | IT gate, endpoints, object-store builders, crypto-provider install |
| `crates/data-avalanche-testkit/tests/smoke.rs` | Databases reachable and configured as preconditions require |
| `crates/data-avalanche-testkit/tests/storage_cas.rs` | Conditional create/update semantics per backend (RustFS, Azurite, real GCS) |

---

### Task 1: Workspace scaffold and `TableRef` with reserved-column rules

**Files:**
- Create: `Cargo.toml`, `rust-toolchain.toml`
- Create: `crates/data-avalanche-core/Cargo.toml`, `crates/data-avalanche-core/src/lib.rs`, `crates/data-avalanche-core/src/table.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `data_avalanche_core::TableRef { schema: String, name: String }` with `TableRef::new(schema, name)`,
  `Display` = `"schema.name"`, `Serialize`/`Deserialize`, `Ord`, `Hash`;
  `data_avalanche_core::LINEAGE_COLUMNS: [&str; 6]`; `data_avalanche_core::INTERNAL_COLUMN_PREFIX: &str`;
  `data_avalanche_core::is_reserved_column(name: &str) -> bool`;
  `data_avalanche_core::first_reserved_collision<'a, I: IntoIterator<Item = &'a str>>(columns: I) -> Option<&'a str>`.

- [ ] **Step 1: Create the workspace manifest and toolchain pin**

`Cargo.toml`:

```toml
[workspace]
resolver = "3"
members = ["crates/*"]
exclude = ["prototypes"]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.98"
publish = false

[workspace.lints.rust]
unsafe_code = "forbid"
missing_debug_implementations = "warn"

[workspace.lints.clippy]
all = { level = "warn", priority = -1 }

[workspace.dependencies]
data-avalanche-core = { path = "crates/data-avalanche-core" }
thiserror = "2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = { version = "0.4", default-features = false, features = ["std", "serde"] }
uuid = { version = "1", features = ["serde", "v4"] }
proptest = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
object_store = { version = "0.13", features = ["aws", "gcp", "azure"] }
tokio-postgres = "0.7"
mysql_async = { version = "=0.37.1", default-features = false, features = ["default-rustls", "binlog"] }
rustls = { version = "0.23", features = ["aws_lc_rs"] }
```

`rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.98.1"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 2: Create the core crate manifest and module list**

`crates/data-avalanche-core/Cargo.toml`:

```toml
[package]
name = "data-avalanche-core"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "Domain types shared by every data-avalanche crate. No I/O."

[lints]
workspace = true

[dependencies]
serde.workspace = true
thiserror.workspace = true

[dev-dependencies]
proptest.workspace = true
serde_json.workspace = true
```

`crates/data-avalanche-core/src/lib.rs`:

```rust
//! Domain types shared by every data-avalanche crate. Nothing in this crate performs I/O.

pub mod table;

pub use table::{
    INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
};
```

- [ ] **Step 3: Write the failing tests**

`crates/data-avalanche-core/src/table.rs` (tests only for now):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_as_schema_dot_name() {
        assert_eq!(TableRef::new("public", "orders").to_string(), "public.orders");
    }

    #[test]
    fn orders_by_schema_then_name() {
        let mut v = vec![TableRef::new("b", "a"), TableRef::new("a", "z"), TableRef::new("a", "b")];
        v.sort();
        assert_eq!(v, vec![TableRef::new("a", "b"), TableRef::new("a", "z"), TableRef::new("b", "a")]);
    }

    #[test]
    fn lineage_names_are_reserved_in_any_case() {
        for n in ["_op", "_OP", "_Ts", "_lsn", "_SEQ", "_source_db", "_Pipeline"] {
            assert!(is_reserved_column(n), "{n} should be reserved");
        }
    }

    #[test]
    fn internal_prefix_is_reserved_in_any_case() {
        assert!(is_reserved_column("__data_avalanche_del"));
        assert!(is_reserved_column("__DATA_AVALANCHE_anything"));
    }

    #[test]
    fn near_misses_are_not_reserved() {
        for n in ["op", "_opx", "x_op", "__data_avalanche", "_data_avalanche_del", "source_db", ""] {
            assert!(!is_reserved_column(n), "{n:?} should not be reserved");
        }
    }

    #[test]
    fn reports_first_collision_in_column_order() {
        assert_eq!(first_reserved_collision(["id", "_TS", "_op"]), Some("_TS"));
        assert_eq!(first_reserved_collision(["id", "name"]), None);
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core`
Expected: compilation FAILS with `cannot find type `TableRef` in this scope` (and the other missing items).

- [ ] **Step 5: Implement `table.rs`** (above the test module)

```rust
use std::fmt;

use serde::{Deserialize, Serialize};

/// A source table. For MySQL the `schema` slot holds the database (`source.db`), so the layout is
/// uniform across engines (config §10, `Data-Flow.md` `TableRef`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

impl TableRef {
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self { schema: schema.into(), name: name.into() }
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

/// Lineage columns the accumulator adds to every row (`Data-Flow.md` §Data Lineage).
pub const LINEAGE_COLUMNS: [&str; 6] = ["_op", "_ts", "_lsn", "_seq", "_source_db", "_pipeline"];

/// Prefix reserved for sink-internal working columns, e.g. the Delta MERGE delete flag (config §5).
pub const INTERNAL_COLUMN_PREFIX: &str = "__data_avalanche_";

/// True when a source column name collides with a reserved name (config §5, D-V1.1).
/// The comparison is case-insensitive because Delta column names are.
pub fn is_reserved_column(name: &str) -> bool {
    let lower = name.to_lowercase();
    LINEAGE_COLUMNS.contains(&lower.as_str()) || lower.starts_with(INTERNAL_COLUMN_PREFIX)
}

/// The first colliding column in column order; the table halts with `RESERVED_COLUMN_COLLISION`
/// naming it.
pub fn first_reserved_collision<'a, I>(columns: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    columns.into_iter().find(|c| is_reserved_column(c))
}
```

- [ ] **Step 6: Run the tests to verify they pass, and check the prototypes stay outside the workspace**

Run: `cargo test -p data-avalanche-core`
Expected: PASS — `test result: ok. 6 passed`.

Run: `cargo metadata --no-deps --format-version 1 --manifest-path prototypes/lease-conformance/Cargo.toml > /dev/null && echo OK`
Expected: `OK` (no "believes it's in a workspace when it's not" error).

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`
Expected: no output from fmt; clippy finishes with no warnings.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml crates/data-avalanche-core
git commit -m "build: workspace scaffold; feat(core): TableRef and reserved-column rules (D-V1.1)"
```

---

### Task 2: CI skeleton

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: the workspace from Task 1.
- Produces: a `check` job (fmt, clippy, unit tests) that later milestones extend. Task 12 adds the
  `integration` job.

- [ ] **Step 1: Write the workflow**

```yaml
name: ci

on:
  push:
  pull_request:

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

env:
  CARGO_TERM_COLOR: always

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      # Reads rust-toolchain.toml (channel + components) and caches the build.
      - uses: actions-rust-lang/setup-rust-toolchain@v1
      - name: Format
        run: cargo fmt --all --check
      - name: Clippy
        run: cargo clippy --workspace --all-targets -- -D warnings
      - name: Unit and property tests
        run: cargo test --workspace
```

- [ ] **Step 2: Run the same steps locally**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: all three succeed; `test result: ok. 6 passed`.

Run: `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.12 -color .github/workflows/ci.yml`
Expected: no output, exit code 0.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: fmt, clippy and unit-test job"
```

---

### Task 3: `Decimal` — 38-digit fixed-point value (D-IMPL-1)

**Files:**
- Create: `crates/data-avalanche-core/src/decimal.rs`
- Modify: `crates/data-avalanche-core/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `data_avalanche_core::Decimal` (`Copy`, `Eq`, `Hash`) with `Decimal::new(unscaled: i128, scale: i8)`,
  `unscaled() -> i128`, `scale() -> i8`, `Decimal::parse(&str) -> Result<Decimal, DecimalError>`,
  `precision() -> u8`, `rescale(i8) -> Result<Decimal, DecimalError>`, `fits(precision: u8, scale: i8) -> bool`,
  `Display`; `data_avalanche_core::MAX_DECIMAL_PRECISION: u8 = 38`; `data_avalanche_core::DecimalError`
  (`Invalid(String)`, `NotFinite(String)`, `TooManyDigits(String)`, `Rescale { value: String, scale: i8 }`).

- [ ] **Step 1: Register the module**

Add to `crates/data-avalanche-core/src/lib.rs`:

```rust
pub mod decimal;

pub use decimal::{Decimal, DecimalError, MAX_DECIMAL_PRECISION};
```

- [ ] **Step 2: Write the failing tests** (`crates/data-avalanche-core/src/decimal.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_and_keeps_the_text_scale() {
        assert_eq!(Decimal::parse("-12.340").unwrap(), Decimal::new(-12_340, 3));
        assert_eq!(Decimal::parse("100").unwrap(), Decimal::new(100, 0));
        assert_eq!(Decimal::parse("+0.5").unwrap(), Decimal::new(5, 1));
        assert_eq!(Decimal::parse(" 7.0 ").unwrap(), Decimal::new(70, 1));
        assert_eq!(Decimal::parse("-0.00").unwrap(), Decimal::new(0, 2));
    }

    #[test]
    fn displays_with_exact_scale() {
        for s in ["-12.340", "100", "0.5", "-0.5", "123456.000001"] {
            assert_eq!(Decimal::parse(s).unwrap().to_string(), s);
        }
    }

    #[test]
    fn displays_leading_zeros_for_small_values() {
        assert_eq!(Decimal::new(5, 4).to_string(), "0.0005");
        assert_eq!(Decimal::new(-5, 4).to_string(), "-0.0005");
        assert_eq!(Decimal::new(0, 2).to_string(), "0.00");
    }

    #[test]
    fn rejects_malformed_text() {
        for s in ["", ".", "-", "1.2.3", "1e5", "abc", "1 2", "--1", "0x10"] {
            assert!(matches!(Decimal::parse(s), Err(DecimalError::Invalid(_))), "{s:?}");
        }
    }

    #[test]
    fn rejects_non_finite_values() {
        for s in ["NaN", "nan", "Infinity", "-Infinity", "+infinity", "inf"] {
            assert!(matches!(Decimal::parse(s), Err(DecimalError::NotFinite(_))), "{s:?}");
        }
    }

    #[test]
    fn accepts_38_significant_digits_but_not_39() {
        let d38 = "9".repeat(38);
        assert!(Decimal::parse(&d38).is_ok());
        assert!(Decimal::parse(&format!("000{d38}")).is_ok(), "leading zeros are not significant");
        let d39 = "9".repeat(39);
        assert!(matches!(Decimal::parse(&d39), Err(DecimalError::TooManyDigits(_))));
    }

    #[test]
    fn rejects_scale_above_38() {
        let s = format!("0.{}1", "0".repeat(38));
        assert!(matches!(Decimal::parse(&s), Err(DecimalError::TooManyDigits(_))));
    }

    #[test]
    fn counts_precision_in_digits() {
        assert_eq!(Decimal::new(0, 0).precision(), 1);
        assert_eq!(Decimal::new(-12_340, 3).precision(), 5);
        assert_eq!(Decimal::new(10i128.pow(37), 0).precision(), 38);
    }

    #[test]
    fn rescales_up_and_down_without_loss() {
        let d = Decimal::parse("12.30").unwrap();
        assert_eq!(d.rescale(4).unwrap(), Decimal::new(123_000, 4));
        assert_eq!(d.rescale(1).unwrap(), Decimal::new(123, 1));
        assert_eq!(d.rescale(2).unwrap(), d);
    }

    #[test]
    fn refuses_lossy_or_overflowing_rescale() {
        assert!(Decimal::parse("12.34").unwrap().rescale(1).is_err());
        assert!(Decimal::new(10i128.pow(37), 0).rescale(2).is_err(), "would exceed 38 digits");
    }

    #[test]
    fn fits_checks_integer_and_fraction_room() {
        let d = Decimal::parse("123.45").unwrap();
        assert!(d.fits(5, 2));
        assert!(d.fits(6, 3));
        assert!(!d.fits(4, 2));
        assert!(!Decimal::parse("1.005").unwrap().fits(5, 2));
        assert!(Decimal::new(0, 0).fits(1, 1));
    }

    proptest! {
        #[test]
        fn display_then_parse_round_trips(
            unscaled in -(10i128.pow(38) - 1)..=(10i128.pow(38) - 1),
            scale in 0i8..=38,
        ) {
            let d = Decimal::new(unscaled, scale);
            prop_assert_eq!(Decimal::parse(&d.to_string()).unwrap(), d);
        }
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core decimal::`
Expected: compilation FAILS with `cannot find type `Decimal` in this scope`.

- [ ] **Step 4: Implement `decimal.rs`** (above the test module)

```rust
use std::fmt;

use thiserror::Error;

/// Largest precision any v1 target stores (Arrow `Decimal128`, Delta/Iceberg `decimal`).
pub const MAX_DECIMAL_PRECISION: u8 = 38;

/// A fixed-point decimal: `unscaled × 10^-scale`, up to 38 significant digits (D-IMPL-1).
/// `rust_decimal` holds only 28–29 digits, too few for `decimal(38, S)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    unscaled: i128,
    scale: i8,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DecimalError {
    #[error("not a decimal number: {0:?}")]
    Invalid(String),
    #[error("decimal is not finite: {0:?}")]
    NotFinite(String),
    #[error("decimal {0:?} has more than 38 significant digits or fraction digits")]
    TooManyDigits(String),
    #[error("rescaling {value} to scale {scale} would lose digits or exceed 38 digits")]
    Rescale { value: String, scale: i8 },
}

impl Decimal {
    pub fn new(unscaled: i128, scale: i8) -> Self {
        Self { unscaled, scale }
    }

    pub fn unscaled(&self) -> i128 {
        self.unscaled
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }

    /// Parses the text form PostgreSQL and MySQL print (`-12.340`), keeping the text's own scale.
    /// No exponent notation; `NaN`/`Infinity` are rejected as non-finite.
    pub fn parse(text: &str) -> Result<Self, DecimalError> {
        let t = text.trim();
        let bare = t.trim_start_matches(['+', '-']).to_ascii_lowercase();
        if matches!(bare.as_str(), "nan" | "infinity" | "inf") {
            return Err(DecimalError::NotFinite(text.to_string()));
        }
        let (negative, body) = match t.as_bytes().first() {
            Some(b'-') => (true, &t[1..]),
            Some(b'+') => (false, &t[1..]),
            _ => (false, t),
        };
        let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
        let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
        if (int_part.is_empty() && frac_part.is_empty()) || !all_digits(int_part) || !all_digits(frac_part)
        {
            return Err(DecimalError::Invalid(text.to_string()));
        }
        if frac_part.len() > usize::from(MAX_DECIMAL_PRECISION) {
            return Err(DecimalError::TooManyDigits(text.to_string()));
        }
        let digits = format!("{int_part}{frac_part}");
        let significant = digits.trim_start_matches('0');
        if significant.len() > usize::from(MAX_DECIMAL_PRECISION) {
            return Err(DecimalError::TooManyDigits(text.to_string()));
        }
        // At most 38 digits: cannot overflow i128 (max ≈ 1.7 × 10^38).
        let mut unscaled: i128 = 0;
        for b in significant.bytes() {
            unscaled = unscaled * 10 + i128::from(b - b'0');
        }
        if negative {
            unscaled = -unscaled;
        }
        let scale = i8::try_from(frac_part.len()).map_err(|_| DecimalError::TooManyDigits(text.to_string()))?;
        Ok(Self { unscaled, scale })
    }

    /// Number of decimal digits in the unscaled value (zero has precision 1).
    pub fn precision(&self) -> u8 {
        let mut n = self.unscaled.unsigned_abs();
        let mut digits = 1u8;
        while n >= 10 {
            n /= 10;
            digits += 1;
        }
        digits
    }

    /// Changes the scale without losing digits: padding zeros when the scale grows, dropping only
    /// zero digits when it shrinks. The result must stay within 38 digits.
    pub fn rescale(&self, scale: i8) -> Result<Self, DecimalError> {
        let err = || DecimalError::Rescale { value: self.to_string(), scale };
        let diff = i16::from(scale) - i16::from(self.scale);
        if diff == 0 {
            return Ok(*self);
        }
        let factor = 10i128.checked_pow(u32::from(diff.unsigned_abs())).ok_or_else(err)?;
        let out = if diff > 0 {
            Self { unscaled: self.unscaled.checked_mul(factor).ok_or_else(err)?, scale }
        } else {
            if self.unscaled % factor != 0 {
                return Err(err());
            }
            Self { unscaled: self.unscaled / factor, scale }
        };
        if out.precision() > MAX_DECIMAL_PRECISION {
            return Err(err());
        }
        Ok(out)
    }

    /// True when the value can be stored as `decimal(precision, scale)` without loss.
    pub fn fits(&self, precision: u8, scale: i8) -> bool {
        self.rescale(scale).is_ok_and(|v| v.precision() <= precision)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.unscaled < 0 { "-" } else { "" };
        let digits = self.unscaled.unsigned_abs().to_string();
        if self.scale <= 0 {
            let zeros = if self.unscaled == 0 { String::new() } else { "0".repeat(usize::from(self.scale.unsigned_abs())) };
            return write!(f, "{sign}{digits}{zeros}");
        }
        let scale = usize::from(self.scale.unsigned_abs());
        let padded = if digits.len() <= scale { format!("{}{digits}", "0".repeat(scale - digits.len() + 1)) } else { digits };
        let (int, frac) = padded.split_at(padded.len() - scale);
        write!(f, "{sign}{int}.{frac}")
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core decimal::`
Expected: PASS — 12 tests including the proptest.

Run: `cargo clippy -p data-avalanche-core --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/data-avalanche-core
git commit -m "feat(core): 38-digit Decimal with lossless rescale (D-IMPL-1)"
```

---

### Task 4: `ColumnValue`, `ColumnValueType`, `Row`

**Files:**
- Create: `crates/data-avalanche-core/src/value.rs`
- Modify: `crates/data-avalanche-core/src/lib.rs`, `crates/data-avalanche-core/Cargo.toml`

**Interfaces:**
- Consumes: `Decimal` (Task 3).
- Produces: `data_avalanche_core::ColumnValueType` (`Bool, Int16, Int32, Int64, UInt64, Float32, Float64,
  Numeric { precision: u8, scale: i8 }, Text, Bytes, Timestamp { with_tz: bool }, Date, Time, Uuid, Json,
  Array(Box<ColumnValueType>)`); `data_avalanche_core::ColumnValue` (`Null, Bool(bool), Int16(i16), Int32(i32),
  Int64(i64), UInt64(u64), Float32(f32), Float64(f64), Numeric(Decimal), Text(String), Bytes(Vec<u8>),
  Timestamp { micros: i64, with_tz: bool }, Date(i32), Time(NaiveTime), Uuid(Uuid), Json(serde_json::Value),
  Array { element_type: ColumnValueType, values: Vec<ColumnValue> }`) with `is_null()` and
  `fits(&ColumnValueType) -> bool`; `data_avalanche_core::Row(pub Vec<(String, ColumnValue)>)` with
  `get(&str) -> Option<&ColumnValue>` and `columns() -> impl Iterator<Item = &str>`.
  M3 extends `Row` with the unchanged-TOAST mask (D-IMPL-3); that is out of scope here.

- [ ] **Step 1: Add dependencies and register the module**

In `crates/data-avalanche-core/Cargo.toml` `[dependencies]` add:

```toml
chrono.workspace = true
serde_json.workspace = true
uuid.workspace = true
```

Add to `crates/data-avalanche-core/src/lib.rs`:

```rust
pub mod value;

pub use value::{ColumnValue, ColumnValueType, Row};
```

- [ ] **Step 2: Write the failing tests** (`crates/data-avalanche-core/src/value.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Decimal;

    #[test]
    fn null_fits_every_type() {
        for t in [ColumnValueType::Bool, ColumnValueType::Json, ColumnValueType::Array(Box::new(ColumnValueType::Text))] {
            assert!(ColumnValue::Null.fits(&t));
        }
        assert!(ColumnValue::Null.is_null());
    }

    #[test]
    fn scalar_values_fit_only_their_own_type() {
        assert!(ColumnValue::Int32(7).fits(&ColumnValueType::Int32));
        assert!(!ColumnValue::Int32(7).fits(&ColumnValueType::Int64));
        assert!(ColumnValue::UInt64(u64::MAX).fits(&ColumnValueType::UInt64));
        assert!(!ColumnValue::Text("x".into()).fits(&ColumnValueType::Bytes));
    }

    #[test]
    fn numeric_respects_declared_precision_and_scale() {
        let v = ColumnValue::Numeric(Decimal::parse("123.45").unwrap());
        assert!(v.fits(&ColumnValueType::Numeric { precision: 5, scale: 2 }));
        assert!(!v.fits(&ColumnValueType::Numeric { precision: 4, scale: 2 }));
    }

    #[test]
    fn date_and_timestamp_hold_the_full_arrow_range() {
        // Clamped PG edge values (Q-M5-1) must be representable.
        for d in [i32::MIN, 0, i32::MAX] {
            assert!(ColumnValue::Date(d).fits(&ColumnValueType::Date));
        }
        for micros in [i64::MIN, i64::MAX] {
            assert!(ColumnValue::Timestamp { micros, with_tz: false }.fits(&ColumnValueType::Timestamp { with_tz: false }));
        }
    }

    #[test]
    fn timestamp_must_match_time_zone_flavour() {
        let tz = ColumnValue::Timestamp { micros: 0, with_tz: true };
        assert!(tz.fits(&ColumnValueType::Timestamp { with_tz: true }));
        assert!(!tz.fits(&ColumnValueType::Timestamp { with_tz: false }));
    }

    #[test]
    fn array_checks_declared_and_actual_element_types() {
        let ints = ColumnValue::Array {
            element_type: ColumnValueType::Int32,
            values: vec![ColumnValue::Int32(1), ColumnValue::Null],
        };
        assert!(ints.fits(&ColumnValueType::Array(Box::new(ColumnValueType::Int32))));
        assert!(!ints.fits(&ColumnValueType::Array(Box::new(ColumnValueType::Int64))));
        let lying = ColumnValue::Array {
            element_type: ColumnValueType::Int32,
            values: vec![ColumnValue::Text("x".into())],
        };
        assert!(!lying.fits(&ColumnValueType::Array(Box::new(ColumnValueType::Int32))));
    }

    #[test]
    fn row_looks_up_by_name_in_column_order() {
        let row = Row(vec![("id".into(), ColumnValue::Int64(1)), ("name".into(), ColumnValue::Null)]);
        assert_eq!(row.get("id"), Some(&ColumnValue::Int64(1)));
        assert_eq!(row.get("missing"), None);
        assert_eq!(row.columns().collect::<Vec<_>>(), vec!["id", "name"]);
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core value::`
Expected: compilation FAILS with `cannot find type `ColumnValue` in this scope`.

- [ ] **Step 4: Implement `value.rs`** (above the test module)

```rust
use chrono::NaiveTime;
use uuid::Uuid;

use crate::Decimal;

/// The type of a column as decoded from the source (Hop 1 output, `Type_Mapping.md`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnValueType {
    Bool,
    Int16,
    Int32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Numeric { precision: u8, scale: i8 },
    Text,
    Bytes,
    Timestamp { with_tz: bool },
    Date,
    Time,
    Uuid,
    Json,
    Array(Box<ColumnValueType>),
}

/// A source-decoded value (`Data-Flow.md` §CdcEvent Structure).
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    /// MySQL/MariaDB `BIGINT UNSIGNED`.
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Numeric(Decimal),
    Text(String),
    Bytes(Vec<u8>),
    /// Microseconds since the Unix epoch. `with_tz = true`: a UTC instant (PG `timestamptz`, MySQL
    /// `TIMESTAMP`). `false`: a wall-clock value without a zone (PG `timestamp`, MySQL `DATETIME`),
    /// stored as if it were UTC.
    Timestamp { micros: i64, with_tz: bool },
    /// Days since the Unix epoch — exactly Arrow `Date32`, so PG `±infinity` can clamp to `i32::MIN` /
    /// `i32::MAX` (Q-M5-1). chrono's `NaiveDate` is too narrow for that.
    Date(i32),
    Time(NaiveTime),
    Uuid(Uuid),
    Json(serde_json::Value),
    Array { element_type: ColumnValueType, values: Vec<ColumnValue> },
}

impl ColumnValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// True when the value can be stored in a column of type `t`. NULL fits every type
    /// (nullability is checked separately); arrays check the declared element type and every element.
    pub fn fits(&self, t: &ColumnValueType) -> bool {
        use ColumnValueType as T;
        match (self, t) {
            (Self::Null, _) => true,
            (Self::Bool(_), T::Bool)
            | (Self::Int16(_), T::Int16)
            | (Self::Int32(_), T::Int32)
            | (Self::Int64(_), T::Int64)
            | (Self::UInt64(_), T::UInt64)
            | (Self::Float32(_), T::Float32)
            | (Self::Float64(_), T::Float64)
            | (Self::Text(_), T::Text)
            | (Self::Bytes(_), T::Bytes)
            | (Self::Date(_), T::Date)
            | (Self::Time(_), T::Time)
            | (Self::Uuid(_), T::Uuid)
            | (Self::Json(_), T::Json) => true,
            (Self::Numeric(d), T::Numeric { precision, scale }) => d.fits(*precision, *scale),
            (Self::Timestamp { with_tz, .. }, T::Timestamp { with_tz: column_tz }) => with_tz == column_tz,
            (Self::Array { element_type, values }, T::Array(column_element)) => {
                element_type == column_element.as_ref() && values.iter().all(|v| v.fits(column_element))
            }
            _ => false,
        }
    }
}

/// A row image: `(column name, value)` pairs in source column order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Row(pub Vec<(String, ColumnValue)>);

impl Row {
    pub fn get(&self, column: &str) -> Option<&ColumnValue> {
        self.0.iter().find(|(name, _)| name == column).map(|(_, v)| v)
    }

    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|(name, _)| name.as_str())
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core value::tests && cargo clippy -p data-avalanche-core --all-targets -- -D warnings`
Expected: PASS — 7 tests; no clippy warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/data-avalanche-core
git commit -m "feat(core): ColumnValue, ColumnValueType and Row"
```

---

### Task 5: `LogicalType` grammar

**Files:**
- Create: `crates/data-avalanche-core/src/logical.rs`
- Modify: `crates/data-avalanche-core/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `data_avalanche_core::LogicalType` (`Bool, Int16, Int32, Int64, Float32, Float64, Decimal { precision: u8, scale: i8 },
  String, Binary, Date, Time, TimestampTz, TimestampNtz, Uuid, Json, List(Box<LogicalType>)`) implementing
  `FromStr<Err = LogicalTypeError>` and `Display` (canonical, round-trips);
  `data_avalanche_core::LogicalTypeError { input: String, reason: String }`. M1 wraps it into V-078; sinks
  map it in Hop 3.

- [ ] **Step 1: Register the module**

Add to `crates/data-avalanche-core/src/lib.rs`:

```rust
pub mod logical;

pub use logical::{LogicalType, LogicalTypeError};
```

- [ ] **Step 2: Write the failing tests** (`crates/data-avalanche-core/src/logical.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const SIMPLE: [&str; 14] = [
        "bool", "int16", "int32", "int64", "float32", "float64", "string", "binary", "date", "time",
        "timestamp_tz", "timestamp_ntz", "uuid", "json",
    ];

    #[test]
    fn simple_names_round_trip() {
        for s in SIMPLE {
            let t: LogicalType = s.parse().unwrap();
            assert_eq!(t.to_string(), s);
        }
    }

    #[test]
    fn decimal_accepts_spaces_and_prints_canonically() {
        let t: LogicalType = "decimal( 38 , 10 )".parse().unwrap();
        assert_eq!(t, LogicalType::Decimal { precision: 38, scale: 10 });
        assert_eq!(t.to_string(), "decimal(38,10)");
    }

    #[test]
    fn lists_nest() {
        let t: LogicalType = "list<list<string>>".parse().unwrap();
        assert_eq!(t, LogicalType::List(Box::new(LogicalType::List(Box::new(LogicalType::String)))));
        assert_eq!(t.to_string(), "list<list<string>>");
        assert_eq!("list< int64 >".parse::<LogicalType>().unwrap().to_string(), "list<int64>");
    }

    #[test]
    fn rejects_out_of_range_decimals() {
        for s in ["decimal(0,0)", "decimal(39,1)", "decimal(10,11)", "decimal(10,-1)", "decimal(10)", "decimal"] {
            assert!(s.parse::<LogicalType>().is_err(), "{s}");
        }
    }

    #[test]
    fn rejects_ambiguous_and_unknown_names() {
        // `timestamp` means opposite things in Iceberg and Delta, so the grammar has no bare `timestamp` (D-MS.7).
        for s in ["timestamp", "TIMESTAMP_TZ", "Int64", "", "list<>", "list<foo>", "list<int64", "varchar"] {
            assert!(s.parse::<LogicalType>().is_err(), "{s:?}");
        }
    }

    #[test]
    fn error_names_the_input() {
        let err = "decimal(99,1)".parse::<LogicalType>().unwrap_err();
        assert_eq!(err.input, "decimal(99,1)");
        assert!(err.to_string().contains("decimal(99,1)"));
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core logical::`
Expected: compilation FAILS with `cannot find type `LogicalType` in this scope`.

- [ ] **Step 4: Implement `logical.rs`** (above the test module)

```rust
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// Sink-neutral logical type — the grammar of `tables.column_type_overrides` (config §9.3) and of the
/// `data-avalanche.logical` Arrow field tag (`Type_Mapping.md`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogicalType {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal { precision: u8, scale: i8 },
    String,
    Binary,
    Date,
    Time,
    TimestampTz,
    TimestampNtz,
    Uuid,
    Json,
    List(Box<LogicalType>),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid logical type {input:?}: {reason}")]
pub struct LogicalTypeError {
    pub input: String,
    pub reason: String,
}

impl FromStr for LogicalType {
    type Err = LogicalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_type(s.trim()).map_err(|reason| LogicalTypeError { input: s.to_string(), reason })
    }
}

fn parse_type(s: &str) -> Result<LogicalType, String> {
    let simple = match s {
        "bool" => Some(LogicalType::Bool),
        "int16" => Some(LogicalType::Int16),
        "int32" => Some(LogicalType::Int32),
        "int64" => Some(LogicalType::Int64),
        "float32" => Some(LogicalType::Float32),
        "float64" => Some(LogicalType::Float64),
        "string" => Some(LogicalType::String),
        "binary" => Some(LogicalType::Binary),
        "date" => Some(LogicalType::Date),
        "time" => Some(LogicalType::Time),
        "timestamp_tz" => Some(LogicalType::TimestampTz),
        "timestamp_ntz" => Some(LogicalType::TimestampNtz),
        "uuid" => Some(LogicalType::Uuid),
        "json" => Some(LogicalType::Json),
        _ => None,
    };
    if let Some(t) = simple {
        return Ok(t);
    }
    if let Some(args) = s.strip_prefix("decimal") {
        let inner = args
            .trim()
            .strip_prefix('(')
            .and_then(|a| a.strip_suffix(')'))
            .ok_or("decimal needs (P,S)")?;
        let (p, sc) = inner.split_once(',').ok_or("decimal needs (P,S)")?;
        let precision: u8 = p.trim().parse().map_err(|_| format!("bad precision {:?}", p.trim()))?;
        let scale: i8 = sc.trim().parse().map_err(|_| format!("bad scale {:?}", sc.trim()))?;
        if !(1..=38).contains(&precision) {
            return Err(format!("precision {precision} is outside 1..=38"));
        }
        if scale < 0 || scale.unsigned_abs() > precision {
            return Err(format!("scale {scale} is outside 0..={precision}"));
        }
        return Ok(LogicalType::Decimal { precision, scale });
    }
    if let Some(args) = s.strip_prefix("list") {
        let inner = args
            .trim()
            .strip_prefix('<')
            .and_then(|a| a.strip_suffix('>'))
            .ok_or("list needs <T>")?;
        return Ok(LogicalType::List(Box::new(parse_type(inner.trim())?)));
    }
    Err("unknown type".to_string())
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool => f.write_str("bool"),
            Self::Int16 => f.write_str("int16"),
            Self::Int32 => f.write_str("int32"),
            Self::Int64 => f.write_str("int64"),
            Self::Float32 => f.write_str("float32"),
            Self::Float64 => f.write_str("float64"),
            Self::Decimal { precision, scale } => write!(f, "decimal({precision},{scale})"),
            Self::String => f.write_str("string"),
            Self::Binary => f.write_str("binary"),
            Self::Date => f.write_str("date"),
            Self::Time => f.write_str("time"),
            Self::TimestampTz => f.write_str("timestamp_tz"),
            Self::TimestampNtz => f.write_str("timestamp_ntz"),
            Self::Uuid => f.write_str("uuid"),
            Self::Json => f.write_str("json"),
            Self::List(inner) => write!(f, "list<{inner}>"),
        }
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core logical::tests && cargo clippy -p data-avalanche-core --all-targets -- -D warnings`
Expected: PASS — 6 tests; no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/data-avalanche-core
git commit -m "feat(core): sink-neutral LogicalType grammar (config §9.3)"
```

---

### Task 6: PostgreSQL `Lsn`

**Files:**
- Create: `crates/data-avalanche-core/src/position/lsn.rs`, `crates/data-avalanche-core/src/position/mod.rs`
- Modify: `crates/data-avalanche-core/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `data_avalanche_core::position::Lsn(pub u64)` (`Copy`, `Ord`, `Hash`), `FromStr<Err = LsnParseError>`
  for `X/Y` hex, `Display` in upper-case hex (PostgreSQL's own form); `data_avalanche_core::position::LsnParseError(pub String)`.

- [ ] **Step 1: Create the module skeleton**

`crates/data-avalanche-core/src/position/mod.rs`:

```rust
//! Source positions per engine dialect (`Delivery_Guarantee_and_Position_Guard.md` §2).

pub mod lsn;

pub use lsn::{Lsn, LsnParseError};
```

Add to `crates/data-avalanche-core/src/lib.rs`:

```rust
pub mod position;
```

- [ ] **Step 2: Write the failing tests** (`crates/data-avalanche-core/src/position/lsn.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_postgres_text_form() {
        assert_eq!("0/16B3748".parse::<Lsn>().unwrap(), Lsn(0x16B_3748));
        assert_eq!("16/B374D848".parse::<Lsn>().unwrap(), Lsn(0x16_B374_D848));
        assert_eq!("0/16b3748".parse::<Lsn>().unwrap(), Lsn(0x16B_3748), "lower-case hex accepted");
    }

    #[test]
    fn prints_upper_case_like_postgres() {
        assert_eq!(Lsn(0x16_B374_D848).to_string(), "16/B374D848");
        assert_eq!(Lsn(0).to_string(), "0/0");
    }

    #[test]
    fn orders_numerically() {
        assert!("0/FFFFFFFF".parse::<Lsn>().unwrap() < "1/0".parse::<Lsn>().unwrap());
    }

    #[test]
    fn rejects_malformed_text() {
        for s in ["", "0", "/1", "1/", "0/123456789", "g/1", "0/1/2", " 0/1"] {
            assert!(s.parse::<Lsn>().is_err(), "{s:?}");
        }
    }

    proptest! {
        #[test]
        fn round_trips(v in any::<u64>()) {
            prop_assert_eq!(Lsn(v).to_string().parse::<Lsn>().unwrap(), Lsn(v));
        }
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core lsn::`
Expected: compilation FAILS with `cannot find type `Lsn` in this scope`.

- [ ] **Step 4: Implement `lsn.rs`** (above the test module)

```rust
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// A PostgreSQL WAL position (`pg_lsn`): 64 bits, written `X/Y` with two hex halves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Lsn(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid LSN {0:?} (expected X/Y with 1-8 hex digits each)")]
pub struct LsnParseError(pub String);

impl FromStr for Lsn {
    type Err = LsnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || LsnParseError(s.to_string());
        let (hi, lo) = s.split_once('/').ok_or_else(err)?;
        let half = |part: &str| {
            if part.is_empty() || part.len() > 8 || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(err());
            }
            u32::from_str_radix(part, 16).map_err(|_| err())
        };
        Ok(Lsn((u64::from(half(hi)?) << 32) | u64::from(half(lo)?)))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core lsn::tests && cargo clippy -p data-avalanche-core --all-targets -- -D warnings`
Expected: PASS — 5 tests; no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/data-avalanche-core
git commit -m "feat(core): PostgreSQL Lsn"
```

---

### Task 7: MySQL `GtidSet` (with GTID tags)

**Files:**
- Create: `crates/data-avalanche-core/src/position/gtid.rs`
- Modify: `crates/data-avalanche-core/src/position/mod.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `data_avalanche_core::position::GtidSource { uuid: Uuid, tag: Option<String> }` (`Ord`: UUID, then
  untagged before tags); `data_avalanche_core::position::GtidSet` (`Default` = empty, `Eq`) with `FromStr<Err = GtidParseError>`,
  canonical `Display`, `is_empty()`, `insert(&mut self, source: &GtidSource, gno: u64)`,
  `insert_range(&mut self, source: GtidSource, start: u64, end: u64)` (panics unless `1 <= start <= end`),
  `covers(&self, other: &GtidSet) -> bool` (`other ⊆ self`), `union(&self, other) -> GtidSet`,
  `intersect(&self, other) -> GtidSet`; `data_avalanche_core::position::GtidParseError { input: String, reason: String }`.

- [ ] **Step 1: Register the module**

Append to `crates/data-avalanche-core/src/position/mod.rs`:

```rust
pub mod gtid;

pub use gtid::{GtidParseError, GtidSet, GtidSource};
```

- [ ] **Step 2: Write the failing tests** (`crates/data-avalanche-core/src/position/gtid.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const B: &str = "4e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn set(s: &str) -> GtidSet {
        s.parse().unwrap()
    }

    #[test]
    fn parses_and_prints_canonical_form() {
        assert_eq!(set(&format!("{B}:5,{A}:1-3:7")).to_string(), format!("{A}:1-3:7,{B}:5"));
        assert_eq!(set("").to_string(), "");
        assert!(set("").is_empty());
    }

    #[test]
    fn parses_multiline_uppercase_server_output() {
        let raw = format!("{}:1-5,\n  {}:1-3", A.to_uppercase(), B);
        assert_eq!(set(&raw).to_string(), format!("{A}:1-5,{B}:1-3"));
    }

    #[test]
    fn merges_overlapping_and_adjacent_intervals() {
        assert_eq!(set(&format!("{A}:1-3:4-6:5-9:11")).to_string(), format!("{A}:1-9:11"));
    }

    #[test]
    fn parses_and_prints_tagged_intervals() {
        let s = set(&format!("{A}:1-3:ETL:1-2:audit:5"));
        assert_eq!(s.to_string(), format!("{A}:1-3:audit:5:etl:1-2"));
    }

    #[test]
    fn rejects_malformed_members() {
        for s in [
            A.to_string(),
            format!("{A}:"),
            format!("{A}:0"),
            format!("{A}:5-3"),
            "not-a-uuid:1".to_string(),
            format!("{A}:tag"),
            format!("{A}:1-3:Bad-Tag:1"),
            format!("{A}:1-3,"),
        ] {
            assert!(s.parse::<GtidSet>().is_err(), "{s:?}");
        }
    }

    #[test]
    fn covers_is_subset() {
        let big = set(&format!("{A}:1-10,{B}:1-5"));
        assert!(big.covers(&set(&format!("{A}:2-4:9"))));
        assert!(big.covers(&set("")));
        assert!(!big.covers(&set(&format!("{A}:10-11"))));
        assert!(!big.covers(&set(&format!("{A}:1-10:etl:1"))), "different tag is a different source");
    }

    #[test]
    fn union_and_intersect() {
        let x = set(&format!("{A}:1-5,{B}:1-2"));
        let y = set(&format!("{A}:4-8"));
        assert_eq!(x.union(&y).to_string(), format!("{A}:1-8,{B}:1-2"));
        assert_eq!(x.intersect(&y).to_string(), format!("{A}:4-5"));
    }

    #[test]
    fn insert_extends_intervals() {
        let src = GtidSource { uuid: A.parse().unwrap(), tag: None };
        let mut s = GtidSet::default();
        for gno in [1, 2, 3, 5] {
            s.insert(&src, gno);
        }
        assert_eq!(s.to_string(), format!("{A}:1-3:5"));
    }

    fn arb_set() -> impl Strategy<Value = GtidSet> {
        let uuids = [A, B, "5e11fa47-71ca-11e1-9e33-c80aa9429562"];
        let tags = [None, Some("t1"), Some("t2")];
        prop::collection::vec((0usize..3, 0usize..3, 1u64..60, 0u64..6), 0..8).prop_map(move |items| {
            let mut s = GtidSet::default();
            for (u, t, start, len) in items {
                let src = GtidSource { uuid: uuids[u].parse().unwrap(), tag: tags[t].map(str::to_string) };
                s.insert_range(src, start, start + len);
            }
            s
        })
    }

    proptest! {
        #[test]
        fn display_parse_round_trip(s in arb_set()) {
            prop_assert_eq!(s.to_string().parse::<GtidSet>().unwrap(), s);
        }

        #[test]
        fn union_covers_both_and_commutes(a in arb_set(), b in arb_set()) {
            let u = a.union(&b);
            prop_assert!(u.covers(&a) && u.covers(&b));
            prop_assert_eq!(u, b.union(&a));
        }

        #[test]
        fn intersect_is_covered_by_both(a in arb_set(), b in arb_set()) {
            let i = a.intersect(&b);
            prop_assert!(a.covers(&i) && b.covers(&i));
        }

        #[test]
        fn covers_iff_union_is_identity(a in arb_set(), b in arb_set()) {
            prop_assert_eq!(a.covers(&b), a.union(&b) == a);
        }
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core gtid::`
Expected: compilation FAILS with `cannot find type `GtidSet` in this scope`.

- [ ] **Step 4: Implement `gtid.rs`** (above the test module)

```rust
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;
use uuid::Uuid;

/// One GTID source: the server UUID plus an optional MySQL 8.4 GTID tag.
/// Ordering puts untagged intervals before tagged ones for the same UUID, as MySQL prints them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GtidSource {
    pub uuid: Uuid,
    pub tag: Option<String>,
}

/// A MySQL GTID set. Intervals are inclusive, sorted, and merged (no overlap, no adjacency), so the
/// structure is canonical and `==` is set equality.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GtidSet {
    intervals: BTreeMap<GtidSource, Vec<(u64, u64)>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid GTID set {input:?}: {reason}")]
pub struct GtidParseError {
    pub input: String,
    pub reason: String,
}

impl GtidSet {
    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty()
    }

    /// Adds one transaction number (streaming: record each transaction's GTID).
    pub fn insert(&mut self, source: &GtidSource, gno: u64) {
        self.insert_range(source.clone(), gno, gno);
    }

    /// Adds the inclusive range `start..=end`.
    ///
    /// # Panics
    /// Unless `1 <= start <= end` (a GTID transaction number starts at 1).
    pub fn insert_range(&mut self, source: GtidSource, start: u64, end: u64) {
        assert!(start >= 1 && start <= end, "invalid GTID interval {start}-{end}");
        let ivs = self.intervals.entry(source).or_default();
        if let Some(last) = ivs.last_mut() {
            // Fast path for streaming inserts that extend the newest interval.
            if start >= last.0 && start <= last.1.saturating_add(1) {
                last.1 = last.1.max(end);
                return;
            }
        }
        ivs.push((start, end));
        normalize(ivs);
    }

    /// True when `other ⊆ self`.
    pub fn covers(&self, other: &GtidSet) -> bool {
        other.intervals.iter().all(|(src, theirs)| match self.intervals.get(src) {
            None => theirs.is_empty(),
            Some(mine) => theirs.iter().all(|&(a, b)| mine.iter().any(|&(x, y)| x <= a && b <= y)),
        })
    }

    pub fn union(&self, other: &GtidSet) -> GtidSet {
        let mut out = self.clone();
        for (src, ivs) in &other.intervals {
            let entry = out.intervals.entry(src.clone()).or_default();
            entry.extend_from_slice(ivs);
            normalize(entry);
        }
        out
    }

    pub fn intersect(&self, other: &GtidSet) -> GtidSet {
        let mut out = GtidSet::default();
        for (src, mine) in &self.intervals {
            let Some(theirs) = other.intervals.get(src) else { continue };
            let (mut i, mut j) = (0, 0);
            let mut common = Vec::new();
            while i < mine.len() && j < theirs.len() {
                let (a1, b1) = mine[i];
                let (a2, b2) = theirs[j];
                let (lo, hi) = (a1.max(a2), b1.min(b2));
                if lo <= hi {
                    common.push((lo, hi));
                }
                if b1 < b2 {
                    i += 1;
                } else {
                    j += 1;
                }
            }
            if !common.is_empty() {
                normalize(&mut common);
                out.intervals.insert(src.clone(), common);
            }
        }
        out
    }
}

fn normalize(ivs: &mut Vec<(u64, u64)>) {
    ivs.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ivs.len());
    for &(a, b) in ivs.iter() {
        match merged.last_mut() {
            Some(last) if a <= last.1.saturating_add(1) => last.1 = last.1.max(b),
            _ => merged.push((a, b)),
        }
    }
    *ivs = merged;
}

fn parse_interval(part: &str) -> Result<(u64, u64), String> {
    let (a, b) = part.split_once('-').unwrap_or((part, part));
    let parse = |n: &str| n.parse::<u64>().map_err(|_| format!("bad interval {part:?}"));
    let (a, b) = (parse(a)?, parse(b)?);
    if a == 0 || a > b {
        return Err(format!("bad interval {part:?}"));
    }
    Ok((a, b))
}

fn validate_tag(tag: &str) -> Result<(), String> {
    let mut chars = tag.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_lowercase() || c == '_');
    let rest_ok = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if first_ok && rest_ok && tag.len() <= 32 {
        Ok(())
    } else {
        Err(format!("bad GTID tag {tag:?}"))
    }
}

impl FromStr for GtidSet {
    type Err = GtidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = |reason: String| GtidParseError { input: s.to_string(), reason };
        let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        let mut set = GtidSet::default();
        if compact.is_empty() {
            return Ok(set);
        }
        for member in compact.split(',') {
            let mut parts = member.split(':');
            let uuid_text = parts.next().unwrap_or_default();
            let uuid = Uuid::parse_str(uuid_text).map_err(|_| err(format!("bad source UUID {uuid_text:?}")))?;
            let mut current = GtidSource { uuid, tag: None };
            let mut current_has_interval = false;
            let mut any_interval = false;
            for part in parts {
                if part.starts_with(|c: char| c.is_ascii_digit()) {
                    let (a, b) = parse_interval(part).map_err(err)?;
                    set.insert_range(current.clone(), a, b);
                    current_has_interval = true;
                    any_interval = true;
                } else {
                    if current.tag.is_some() && !current_has_interval {
                        return Err(err(format!("tag {:?} has no intervals", current.tag)));
                    }
                    let tag = part.to_ascii_lowercase();
                    validate_tag(&tag).map_err(err)?;
                    current = GtidSource { uuid, tag: Some(tag) };
                    current_has_interval = false;
                }
            }
            if !any_interval || !current_has_interval {
                return Err(err(format!("member {member:?} ends without intervals")));
            }
        }
        Ok(set)
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut current_uuid: Option<Uuid> = None;
        for (src, ivs) in &self.intervals {
            if current_uuid != Some(src.uuid) {
                if current_uuid.is_some() {
                    f.write_str(",")?;
                }
                write!(f, "{}", src.uuid.hyphenated())?;
                current_uuid = Some(src.uuid);
            }
            if let Some(tag) = &src.tag {
                write!(f, ":{tag}")?;
            }
            for &(a, b) in ivs {
                if a == b {
                    write!(f, ":{a}")?;
                } else {
                    write!(f, ":{a}-{b}")?;
                }
            }
        }
        Ok(())
    }
}
```

Note on the "member ends without intervals" check: `uuid:1-3:tag` fails because the trailing tag has
no intervals, which is what the `{A}:tag` test expects.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core gtid::tests && cargo clippy -p data-avalanche-core --all-targets -- -D warnings`
Expected: PASS — 12 tests (8 unit + 4 property); no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/data-avalanche-core
git commit -m "feat(core): MySQL GtidSet with tags, canonical form and set algebra"
```

---

### Task 8: MariaDB `MariaGtidState`

**Files:**
- Create: `crates/data-avalanche-core/src/position/mariadb.rs`
- Modify: `crates/data-avalanche-core/src/position/mod.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `data_avalanche_core::position::MariaGtid { domain: u32, server_id: u32, seq: u64 }` (`Copy`);
  `data_avalanche_core::position::MariaGtidState` (`Default`, `Eq`) — one GTID per replication domain, the
  shape of `gtid_binlog_pos` / `@slave_connect_state` — with `FromStr<Err = MariaGtidParseError>`,
  `Display` (`d-s-n` joined by `,`, ascending domain), `advance(&mut self, gtid: MariaGtid)` (stream
  order is the truth; requires `gtid_strict_mode`, enforced at connect in M7),
  `get(domain: u32) -> Option<MariaGtid>`, `covers`, `union`, `intersect` (all per-domain on `seq`);
  `data_avalanche_core::position::MariaGtidParseError { input: String, reason: String }`.

- [ ] **Step 1: Register the module**

Append to `crates/data-avalanche-core/src/position/mod.rs`:

```rust
pub mod mariadb;

pub use mariadb::{MariaGtid, MariaGtidParseError, MariaGtidState};
```

- [ ] **Step 2: Write the failing tests** (`crates/data-avalanche-core/src/position/mariadb.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn st(s: &str) -> MariaGtidState {
        s.parse().unwrap()
    }

    #[test]
    fn parses_and_prints_sorted_by_domain() {
        assert_eq!(st("1-2-50, 0-1-100").to_string(), "0-1-100,1-2-50");
        assert_eq!(st("").to_string(), "");
    }

    #[test]
    fn rejects_malformed_and_duplicate_domains() {
        for s in ["0-1", "0-1-x", "a-1-2", "0-1-2-3", "0-1-2,0-3-4", "0-1-2,"] {
            assert!(s.parse::<MariaGtidState>().is_err(), "{s:?}");
        }
    }

    #[test]
    fn covers_compares_sequence_per_domain() {
        let pos = st("0-1-100,1-2-50");
        assert!(pos.covers(&st("0-1-99")));
        assert!(pos.covers(&st("0-9-100")), "server id does not matter for coverage");
        assert!(!pos.covers(&st("0-1-101")));
        assert!(!pos.covers(&st("2-1-1")), "unknown domain is not covered");
        assert!(pos.covers(&st("")));
    }

    #[test]
    fn union_takes_max_and_intersect_takes_min_per_domain() {
        let a = st("0-1-100,1-2-50");
        let b = st("0-1-90,2-3-7");
        assert_eq!(a.union(&b).to_string(), "0-1-100,1-2-50,2-3-7");
        assert_eq!(a.intersect(&b).to_string(), "0-1-90");
    }

    #[test]
    fn advance_records_latest_gtid_of_its_domain() {
        let mut s = MariaGtidState::default();
        s.advance(MariaGtid { domain: 0, server_id: 1, seq: 5 });
        s.advance(MariaGtid { domain: 0, server_id: 2, seq: 6 });
        assert_eq!(s.to_string(), "0-2-6");
        assert_eq!(s.get(0), Some(MariaGtid { domain: 0, server_id: 2, seq: 6 }));
        assert_eq!(s.get(1), None);
    }

    fn arb_state() -> impl Strategy<Value = MariaGtidState> {
        prop::collection::btree_map(0u32..4, (1u32..4, 1u64..1000), 0..4).prop_map(|m| {
            let mut s = MariaGtidState::default();
            for (domain, (server_id, seq)) in m {
                s.advance(MariaGtid { domain, server_id, seq });
            }
            s
        })
    }

    proptest! {
        #[test]
        fn round_trips(s in arb_state()) {
            prop_assert_eq!(s.to_string().parse::<MariaGtidState>().unwrap(), s);
        }

        #[test]
        fn union_covers_both(a in arb_state(), b in arb_state()) {
            let u = a.union(&b);
            prop_assert!(u.covers(&a) && u.covers(&b));
        }

        #[test]
        fn intersect_is_covered_by_both(a in arb_state(), b in arb_state()) {
            let i = a.intersect(&b);
            prop_assert!(a.covers(&i) && b.covers(&i));
        }
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core mariadb::`
Expected: compilation FAILS with `cannot find type `MariaGtidState` in this scope`.

- [ ] **Step 4: Implement `mariadb.rs`** (above the test module)

```rust
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// One MariaDB GTID: `domain-server_id-seq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MariaGtid {
    pub domain: u32,
    pub server_id: u32,
    pub seq: u64,
}

/// A MariaDB replication position: the last GTID per replication domain (the shape of
/// `gtid_binlog_pos` and `@slave_connect_state`). Comparisons use `seq` within a domain, which is
/// monotonic under `gtid_strict_mode` (required by the source, D-V1.5).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MariaGtidState {
    by_domain: BTreeMap<u32, (u32, u64)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid MariaDB GTID position {input:?}: {reason}")]
pub struct MariaGtidParseError {
    pub input: String,
    pub reason: String,
}

impl MariaGtidState {
    /// Records the GTID of the transaction just read; stream order is the truth.
    pub fn advance(&mut self, gtid: MariaGtid) {
        self.by_domain.insert(gtid.domain, (gtid.server_id, gtid.seq));
    }

    pub fn get(&self, domain: u32) -> Option<MariaGtid> {
        self.by_domain.get(&domain).map(|&(server_id, seq)| MariaGtid { domain, server_id, seq })
    }

    /// True when every domain of `other` is present here at an equal or later sequence.
    pub fn covers(&self, other: &MariaGtidState) -> bool {
        other
            .by_domain
            .iter()
            .all(|(domain, &(_, seq))| self.by_domain.get(domain).is_some_and(|&(_, mine)| mine >= seq))
    }

    /// Per domain, the later of the two GTIDs.
    pub fn union(&self, other: &MariaGtidState) -> MariaGtidState {
        let mut out = self.clone();
        for (&domain, &(server_id, seq)) in &other.by_domain {
            let later = out.by_domain.get(&domain).is_none_or(|&(_, mine)| seq > mine);
            if later {
                out.by_domain.insert(domain, (server_id, seq));
            }
        }
        out
    }

    /// Per domain present in both, the earlier of the two GTIDs.
    pub fn intersect(&self, other: &MariaGtidState) -> MariaGtidState {
        let mut out = MariaGtidState::default();
        for (&domain, &mine) in &self.by_domain {
            if let Some(&theirs) = other.by_domain.get(&domain) {
                out.by_domain.insert(domain, if theirs.1 < mine.1 { theirs } else { mine });
            }
        }
        out
    }
}

impl FromStr for MariaGtidState {
    type Err = MariaGtidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = |reason: String| MariaGtidParseError { input: s.to_string(), reason };
        let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
        let mut state = MariaGtidState::default();
        if compact.is_empty() {
            return Ok(state);
        }
        for item in compact.split(',') {
            let fields: Vec<&str> = item.split('-').collect();
            let [d, s_id, n] = fields.as_slice() else {
                return Err(err(format!("expected domain-server-seq, got {item:?}")));
            };
            let domain: u32 = d.parse().map_err(|_| err(format!("bad domain in {item:?}")))?;
            let server_id: u32 = s_id.parse().map_err(|_| err(format!("bad server id in {item:?}")))?;
            let seq: u64 = n.parse().map_err(|_| err(format!("bad sequence in {item:?}")))?;
            if state.by_domain.insert(domain, (server_id, seq)).is_some() {
                return Err(err(format!("domain {domain} appears twice")));
            }
        }
        Ok(state)
    }
}

impl fmt::Display for MariaGtidState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, (domain, (server_id, seq))) in self.by_domain.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{domain}-{server_id}-{seq}")?;
        }
        Ok(())
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core mariadb::tests && cargo clippy -p data-avalanche-core --all-targets -- -D warnings`
Expected: PASS — 8 tests; no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/data-avalanche-core
git commit -m "feat(core): MariaDB GTID position (per-domain) for D-V1.5"
```

---

### Task 9: File/position `BinlogFilePos`

**Files:**
- Create: `crates/data-avalanche-core/src/position/binlog.rs`
- Modify: `crates/data-avalanche-core/src/position/mod.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `data_avalanche_core::position::BinlogFilePos` with `BinlogFilePos::new(file: &str, pos: u64) -> Result<Self, BinlogPosError>`,
  `file() -> &str`, `base() -> &str`, `file_index() -> u64`, `pos() -> u64`,
  `compare(&self, other) -> Option<Ordering>` (`None` when the two come from different log series),
  `FromStr` for `"<file>:<pos>"`, `Display`; `data_avalanche_core::position::parse_binlog_index(file: &str) -> Result<(&str, u64), BinlogPosError>`
  (also used by `SourceSeq` in Task 11); `data_avalanche_core::position::BinlogPosError(pub String)`.

- [ ] **Step 1: Register the module**

Append to `crates/data-avalanche-core/src/position/mod.rs`:

```rust
pub mod binlog;

pub use binlog::{BinlogFilePos, BinlogPosError, parse_binlog_index};
```

- [ ] **Step 2: Write the failing tests** (`crates/data-avalanche-core/src/position/binlog.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    fn p(s: &str) -> BinlogFilePos {
        s.parse().unwrap()
    }

    #[test]
    fn parses_file_and_position() {
        let pos = p("mysql-bin.000123:4567");
        assert_eq!((pos.file(), pos.base(), pos.file_index(), pos.pos()), ("mysql-bin.000123", "mysql-bin", 123, 4567));
        assert_eq!(pos.to_string(), "mysql-bin.000123:4567");
    }

    #[test]
    fn orders_across_rotation_by_index_not_text() {
        assert_eq!(p("mysql-bin.000999:900").compare(&p("mysql-bin.001000:4")), Some(Ordering::Less));
        assert_eq!(p("mysql-bin.000010:4").compare(&p("mysql-bin.000009:500")), Some(Ordering::Greater));
        assert_eq!(p("mysql-bin.000010:4").compare(&p("mysql-bin.000010:4")), Some(Ordering::Equal));
    }

    #[test]
    fn different_log_series_do_not_compare() {
        assert_eq!(p("mysql-bin.000001:4").compare(&p("other-bin.000001:4")), None);
    }

    #[test]
    fn rejects_malformed_positions() {
        for s in ["mysql-bin.000001", "mysql-bin:4", "mysql-bin.abc:4", "mysql-bin.000001:x", ":4", "mysql-bin.:4"] {
            assert!(s.parse::<BinlogFilePos>().is_err(), "{s:?}");
        }
    }

    #[test]
    fn extracts_index_from_file_name() {
        assert_eq!(parse_binlog_index("mariadb-bin.000042").unwrap(), ("mariadb-bin", 42));
        assert!(parse_binlog_index("no-suffix").is_err());
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core binlog::`
Expected: compilation FAILS with `cannot find type `BinlogFilePos` in this scope`.

- [ ] **Step 4: Implement `binlog.rs`** (above the test module)

```rust
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use thiserror::Error;

/// A binlog file/position (MySQL or MariaDB `replication_type: binlog`, P-4). Ordered by the
/// numeric file index, then the position; positions from different log series (base names) are
/// not comparable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BinlogFilePos {
    file: String,
    base_len: usize,
    index: u64,
    pos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid binlog position {0:?} (expected <base>.<NNNNNN>:<pos>)")]
pub struct BinlogPosError(pub String);

/// Splits `mysql-bin.000123` into (`mysql-bin`, 123).
pub fn parse_binlog_index(file: &str) -> Result<(&str, u64), BinlogPosError> {
    let err = || BinlogPosError(file.to_string());
    let (base, suffix) = file.rsplit_once('.').ok_or_else(err)?;
    if base.is_empty() || suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()) {
        return Err(err());
    }
    Ok((base, suffix.parse().map_err(|_| err())?))
}

impl BinlogFilePos {
    pub fn new(file: &str, pos: u64) -> Result<Self, BinlogPosError> {
        let (base, index) = parse_binlog_index(file)?;
        Ok(Self { file: file.to_string(), base_len: base.len(), index, pos })
    }

    pub fn file(&self) -> &str {
        &self.file
    }

    pub fn base(&self) -> &str {
        &self.file[..self.base_len]
    }

    pub fn file_index(&self) -> u64 {
        self.index
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// Order within one log series; `None` across series (e.g. after a source failover).
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        (self.base() == other.base()).then(|| (self.index, self.pos).cmp(&(other.index, other.pos)))
    }
}

impl FromStr for BinlogFilePos {
    type Err = BinlogPosError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (file, pos) = s.rsplit_once(':').ok_or_else(|| BinlogPosError(s.to_string()))?;
        let pos: u64 = pos.parse().map_err(|_| BinlogPosError(s.to_string()))?;
        Self::new(file, pos).map_err(|_| BinlogPosError(s.to_string()))
    }
}

impl fmt::Display for BinlogFilePos {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file, self.pos)
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core binlog::tests && cargo clippy -p data-avalanche-core --all-targets -- -D warnings`
Expected: PASS — 5 tests; no warnings.

- [ ] **Step 6: Commit**

```bash
git add crates/data-avalanche-core
git commit -m "feat(core): binlog file/position for replication_type binlog (P-4)"
```

---

### Task 10: `SourcePosition` — dialect dispatch, tagged strings, serde

**Files:**
- Modify: `crates/data-avalanche-core/src/position/mod.rs`, `crates/data-avalanche-core/src/lib.rs`

**Interfaces:**
- Consumes: `Lsn` (Task 6), `GtidSet` (Task 7), `MariaGtidState` (Task 8), `BinlogFilePos` (Task 9).
- Produces: `data_avalanche_core::SourcePosition` (`Pg(Lsn)`, `MySqlGtid(GtidSet)`, `MariaDbGtid(MariaGtidState)`,
  `BinlogFile(BinlogFilePos)`) with `dialect() -> &'static str`, `covers(&Self) -> Result<bool, PositionError>`,
  `union(&Self) -> Result<Self, PositionError>`, `intersect(&Self) -> Result<Self, PositionError>`,
  `to_tagged() -> String`, `parse_tagged(&str) -> Result<Self, PositionError>`, `Display` = tagged,
  `FromStr` = `parse_tagged`, `Serialize`/`Deserialize` as the tagged string;
  `data_avalanche_core::PositionError` (`DialectMismatch { left, right }`, `DifferentBinlogSeries { left, right }`,
  `Parse { input, reason }`).

- [ ] **Step 1: Write the failing tests** (append a test module to `crates/data-avalanche-core/src/position/mod.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn pos(s: &str) -> SourcePosition {
        s.parse().unwrap()
    }

    #[test]
    fn tagged_strings_round_trip_for_every_dialect() {
        for s in [
            "pg:16/B374D848".to_string(),
            format!("mysql-gtid:{A}:1-5"),
            "mysql-gtid:".to_string(),
            "mariadb-gtid:0-1-100,1-2-50".to_string(),
            "binlog:mysql-bin.000123:4567".to_string(),
        ] {
            assert_eq!(pos(&s).to_tagged(), s);
        }
    }

    #[test]
    fn rejects_unknown_or_malformed_tags() {
        for s in ["", "pg", "pg:zz", "oracle:1", "binlog:nofile", "mysql-gtid:bad"] {
            assert!(matches!(s.parse::<SourcePosition>(), Err(PositionError::Parse { .. })), "{s:?}");
        }
    }

    #[test]
    fn covers_dispatches_per_dialect() {
        assert!(pos("pg:0/20").covers(&pos("pg:0/10")).unwrap());
        assert!(!pos("pg:0/10").covers(&pos("pg:0/20")).unwrap());
        assert!(pos(&format!("mysql-gtid:{A}:1-9")).covers(&pos(&format!("mysql-gtid:{A}:3"))).unwrap());
        assert!(pos("mariadb-gtid:0-1-9").covers(&pos("mariadb-gtid:0-1-3")).unwrap());
        assert!(pos("binlog:b.000002:4").covers(&pos("binlog:b.000001:900")).unwrap());
    }

    #[test]
    fn union_and_intersect_for_scalar_dialects() {
        assert_eq!(pos("pg:0/10").union(&pos("pg:0/20")).unwrap(), pos("pg:0/20"));
        assert_eq!(pos("pg:0/10").intersect(&pos("pg:0/20")).unwrap(), pos("pg:0/10"));
        assert_eq!(pos("binlog:b.000001:9").union(&pos("binlog:b.000002:4")).unwrap(), pos("binlog:b.000002:4"));
        assert_eq!(pos("binlog:b.000001:9").intersect(&pos("binlog:b.000002:4")).unwrap(), pos("binlog:b.000001:9"));
    }

    #[test]
    fn mixing_dialects_is_an_error() {
        let err = pos("pg:0/10").covers(&pos("mariadb-gtid:0-1-1")).unwrap_err();
        assert_eq!(err, PositionError::DialectMismatch { left: "pg", right: "mariadb-gtid" });
    }

    #[test]
    fn different_binlog_series_is_an_error() {
        assert!(matches!(
            pos("binlog:a.000001:4").covers(&pos("binlog:b.000001:4")),
            Err(PositionError::DifferentBinlogSeries { .. })
        ));
    }

    #[test]
    fn serializes_as_a_tagged_json_string() {
        let p = pos("pg:0/16B3748");
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "\"pg:0/16B3748\"");
        assert_eq!(serde_json::from_str::<SourcePosition>(&json).unwrap(), p);
        assert!(serde_json::from_str::<SourcePosition>("\"nope:1\"").is_err());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core position::tests`
Expected: compilation FAILS with `cannot find type `SourcePosition` in this scope`.

- [ ] **Step 3: Implement the dispatch** (in `crates/data-avalanche-core/src/position/mod.rs`, after the `pub use` lines)

```rust
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// A source position in one of the four engine dialects (roadmap §3.2). `committed_position`
/// markers and state files store it as a self-describing tagged string (D-IMPL-4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourcePosition {
    Pg(Lsn),
    MySqlGtid(GtidSet),
    MariaDbGtid(MariaGtidState),
    BinlogFile(BinlogFilePos),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PositionError {
    #[error("cannot combine a {left} position with a {right} position")]
    DialectMismatch { left: &'static str, right: &'static str },
    #[error("binlog positions from different log series: {left} vs {right}")]
    DifferentBinlogSeries { left: String, right: String },
    #[error("invalid position {input:?}: {reason}")]
    Parse { input: String, reason: String },
}

impl SourcePosition {
    pub fn dialect(&self) -> &'static str {
        match self {
            Self::Pg(_) => "pg",
            Self::MySqlGtid(_) => "mysql-gtid",
            Self::MariaDbGtid(_) => "mariadb-gtid",
            Self::BinlogFile(_) => "binlog",
        }
    }

    fn mismatch(&self, other: &Self) -> PositionError {
        PositionError::DialectMismatch { left: self.dialect(), right: other.dialect() }
    }

    fn binlog_order(a: &BinlogFilePos, b: &BinlogFilePos) -> Result<Ordering, PositionError> {
        a.compare(b).ok_or_else(|| PositionError::DifferentBinlogSeries {
            left: a.to_string(),
            right: b.to_string(),
        })
    }

    /// True when `other` is already included in `self` (PG/binlog: `>=`; GTID: subset;
    /// MariaDB: every domain at an equal or later sequence).
    pub fn covers(&self, other: &Self) -> Result<bool, PositionError> {
        match (self, other) {
            (Self::Pg(a), Self::Pg(b)) => Ok(a >= b),
            (Self::MySqlGtid(a), Self::MySqlGtid(b)) => Ok(a.covers(b)),
            (Self::MariaDbGtid(a), Self::MariaDbGtid(b)) => Ok(a.covers(b)),
            (Self::BinlogFile(a), Self::BinlogFile(b)) => Ok(Self::binlog_order(a, b)? != Ordering::Less),
            _ => Err(self.mismatch(other)),
        }
    }

    pub fn union(&self, other: &Self) -> Result<Self, PositionError> {
        match (self, other) {
            (Self::Pg(a), Self::Pg(b)) => Ok(Self::Pg(*a.max(b))),
            (Self::MySqlGtid(a), Self::MySqlGtid(b)) => Ok(Self::MySqlGtid(a.union(b))),
            (Self::MariaDbGtid(a), Self::MariaDbGtid(b)) => Ok(Self::MariaDbGtid(a.union(b))),
            (Self::BinlogFile(a), Self::BinlogFile(b)) => {
                let later = if Self::binlog_order(a, b)? == Ordering::Less { b } else { a };
                Ok(Self::BinlogFile(later.clone()))
            }
            _ => Err(self.mismatch(other)),
        }
    }

    pub fn intersect(&self, other: &Self) -> Result<Self, PositionError> {
        match (self, other) {
            (Self::Pg(a), Self::Pg(b)) => Ok(Self::Pg(*a.min(b))),
            (Self::MySqlGtid(a), Self::MySqlGtid(b)) => Ok(Self::MySqlGtid(a.intersect(b))),
            (Self::MariaDbGtid(a), Self::MariaDbGtid(b)) => Ok(Self::MariaDbGtid(a.intersect(b))),
            (Self::BinlogFile(a), Self::BinlogFile(b)) => {
                let earlier = if Self::binlog_order(a, b)? == Ordering::Greater { b } else { a };
                Ok(Self::BinlogFile(earlier.clone()))
            }
            _ => Err(self.mismatch(other)),
        }
    }

    pub fn to_tagged(&self) -> String {
        match self {
            Self::Pg(v) => format!("pg:{v}"),
            Self::MySqlGtid(v) => format!("mysql-gtid:{v}"),
            Self::MariaDbGtid(v) => format!("mariadb-gtid:{v}"),
            Self::BinlogFile(v) => format!("binlog:{v}"),
        }
    }

    pub fn parse_tagged(s: &str) -> Result<Self, PositionError> {
        let err = |reason: String| PositionError::Parse { input: s.to_string(), reason };
        let (tag, rest) = s.split_once(':').ok_or_else(|| err("missing dialect tag".into()))?;
        match tag {
            "pg" => rest.parse().map(Self::Pg).map_err(|e| err(e.to_string())),
            "mysql-gtid" => rest.parse().map(Self::MySqlGtid).map_err(|e| err(e.to_string())),
            "mariadb-gtid" => rest.parse().map(Self::MariaDbGtid).map_err(|e| err(e.to_string())),
            "binlog" => rest.parse().map(Self::BinlogFile).map_err(|e| err(e.to_string())),
            other => Err(err(format!("unknown dialect {other:?}"))),
        }
    }
}

impl fmt::Display for SourcePosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_tagged())
    }
}

impl FromStr for SourcePosition {
    type Err = PositionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_tagged(s)
    }
}

impl Serialize for SourcePosition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SourcePosition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}
```

Add to `crates/data-avalanche-core/src/lib.rs`:

```rust
pub use position::{PositionError, SourcePosition};
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core position:: && cargo clippy -p data-avalanche-core --all-targets -- -D warnings`
Expected: PASS — every position test (Lsn 5, Gtid 12, MariaDB 8, binlog 5, dispatch 7); no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core
git commit -m "feat(core): SourcePosition dispatch with tagged serde form (D-IMPL-4)"
```

---

### Task 11: `SourceSeq`, `Op`, `CdcEvent`, `TransactionBoundary`

**Files:**
- Create: `crates/data-avalanche-core/src/event.rs`
- Modify: `crates/data-avalanche-core/src/lib.rs`

**Interfaces:**
- Consumes: `TableRef` (Task 1), `Row` (Task 4), `Lsn`, `parse_binlog_index`, `BinlogPosError` (Tasks 6, 9),
  `SourcePosition` (Task 10).
- Produces: `data_avalanche_core::SourceSeq` (`Pg(Lsn)`, `Binlog { file_index: u64, end_log_pos: u64 }`;
  `Copy`, `PartialOrd` — `None` across variants; `SourceSeq::binlog(file: &str, end_log_pos: u64) -> Result<SourceSeq, BinlogPosError>`;
  `Display`: PG `X/Y`, binlog `NNNNNN:pos` with a 6-digit zero-padded index);
  `data_avalanche_core::Op` (`Insert, Update, Delete, Snapshot`; `lineage_code(self) -> char` → `I/U/D/r`);
  `data_avalanche_core::CdcEvent { op, table, before: Option<Row>, after: Option<Row>, source_pos: SourcePosition, change_seq: SourceSeq, xid: u64, commit_ts_micros: i64 }`
  with `validate(&self) -> Result<(), EventShapeError>`;
  `data_avalanche_core::EventShapeError` (`MissingAfter { op, table }`, `MissingBefore { op, table }`,
  `UnexpectedBefore { op, table }`, `UnexpectedAfter { op, table }`);
  `data_avalanche_core::TransactionBoundary { xid: u64, commit_pos: SourcePosition, commit_ts_micros: i64 }`.

- [ ] **Step 1: Register the module**

Add to `crates/data-avalanche-core/src/lib.rs`:

```rust
pub mod event;

pub use event::{CdcEvent, EventShapeError, Op, SourceSeq, TransactionBoundary};
```

- [ ] **Step 2: Write the failing tests** (`crates/data-avalanche-core/src/event.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::position::Lsn;
    use crate::{ColumnValue, Row, SourcePosition, TableRef};
    use std::cmp::Ordering;

    fn row() -> Option<Row> {
        Some(Row(vec![("id".into(), ColumnValue::Int64(1))]))
    }

    fn event(op: Op, before: Option<Row>, after: Option<Row>) -> CdcEvent {
        CdcEvent {
            op,
            table: TableRef::new("public", "t"),
            before,
            after,
            source_pos: SourcePosition::Pg(Lsn(100)),
            change_seq: SourceSeq::Pg(Lsn(90)),
            xid: 7,
            commit_ts_micros: 0,
        }
    }

    #[test]
    fn seq_orders_within_a_dialect_only() {
        assert!(SourceSeq::Pg(Lsn(1)) < SourceSeq::Pg(Lsn(2)));
        let a = SourceSeq::binlog("mysql-bin.000009", 900).unwrap();
        let b = SourceSeq::binlog("mysql-bin.000010", 4).unwrap();
        assert!(a < b);
        assert_eq!(SourceSeq::Pg(Lsn(1)).partial_cmp(&a), None::<Ordering>);
    }

    #[test]
    fn seq_displays_for_the_seq_column() {
        assert_eq!(SourceSeq::Pg(Lsn(0x16B_3748)).to_string(), "0/16B3748");
        assert_eq!(SourceSeq::binlog("mysql-bin.000123", 4567).unwrap().to_string(), "000123:4567");
    }

    #[test]
    fn lineage_codes_match_the_spec() {
        assert_eq!(
            [Op::Insert, Op::Update, Op::Delete, Op::Snapshot].map(Op::lineage_code),
            ['I', 'U', 'D', 'r']
        );
    }

    #[test]
    fn well_formed_events_validate() {
        assert!(event(Op::Insert, None, row()).validate().is_ok());
        assert!(event(Op::Update, None, row()).validate().is_ok(), "PG DEFAULT identity may omit the old image");
        assert!(event(Op::Update, row(), row()).validate().is_ok());
        assert!(event(Op::Delete, row(), None).validate().is_ok());
        assert!(event(Op::Snapshot, None, row()).validate().is_ok());
    }

    #[test]
    fn malformed_events_are_rejected() {
        assert!(matches!(event(Op::Insert, None, None).validate(), Err(EventShapeError::MissingAfter { .. })));
        assert!(matches!(event(Op::Insert, row(), row()).validate(), Err(EventShapeError::UnexpectedBefore { .. })));
        assert!(matches!(event(Op::Update, row(), None).validate(), Err(EventShapeError::MissingAfter { .. })));
        assert!(matches!(event(Op::Delete, None, None).validate(), Err(EventShapeError::MissingBefore { .. })));
        assert!(matches!(event(Op::Delete, row(), row()).validate(), Err(EventShapeError::UnexpectedAfter { .. })));
        assert!(matches!(event(Op::Snapshot, row(), row()).validate(), Err(EventShapeError::UnexpectedBefore { .. })));
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core event::`
Expected: compilation FAILS with `cannot find type `SourceSeq` in this scope`.

- [ ] **Step 4: Implement `event.rs`** (above the test module)

```rust
use std::cmp::Ordering;
use std::fmt;

use thiserror::Error;

use crate::position::{BinlogPosError, Lsn, parse_binlog_index};
use crate::{Row, SourcePosition, TableRef};

/// Per-change source order — the keep-last key of the collapse (`_seq`, `Ghost_Row…` §4).
/// Comparable only within one dialect (one pipeline has one source).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceSeq {
    /// The change's own WAL LSN.
    Pg(Lsn),
    /// Binlog file index (from the `.NNNNNN` suffix) and the event's `end_log_pos` (D-IMPL-5).
    Binlog { file_index: u64, end_log_pos: u64 },
}

impl SourceSeq {
    pub fn binlog(file: &str, end_log_pos: u64) -> Result<Self, BinlogPosError> {
        let (_, file_index) = parse_binlog_index(file)?;
        Ok(Self::Binlog { file_index, end_log_pos })
    }
}

impl PartialOrd for SourceSeq {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Pg(a), Self::Pg(b)) => Some(a.cmp(b)),
            (
                Self::Binlog { file_index: fa, end_log_pos: pa },
                Self::Binlog { file_index: fb, end_log_pos: pb },
            ) => Some((fa, pa).cmp(&(fb, pb))),
            _ => None,
        }
    }
}

impl fmt::Display for SourceSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pg(lsn) => write!(f, "{lsn}"),
            Self::Binlog { file_index, end_log_pos } => write!(f, "{file_index:06}:{end_log_pos}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    Insert,
    Update,
    Delete,
    Snapshot,
}

impl Op {
    /// The `_op` lineage value. `D` never reaches a mirror table (deletes remove rows); it is kept
    /// for the reserved `changelog` mode.
    pub fn lineage_code(self) -> char {
        match self {
            Self::Insert => 'I',
            Self::Update => 'U',
            Self::Delete => 'D',
            Self::Snapshot => 'r',
        }
    }
}

/// One row change, identical for both sources (`Data-Flow.md` §CdcEvent Structure).
#[derive(Debug, Clone, PartialEq)]
pub struct CdcEvent {
    pub op: Op,
    pub table: TableRef,
    pub before: Option<Row>,
    pub after: Option<Row>,
    /// Commit position of the enclosing transaction.
    pub source_pos: SourcePosition,
    pub change_seq: SourceSeq,
    pub xid: u64,
    /// Server commit time, microseconds since the Unix epoch (UTC).
    pub commit_ts_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EventShapeError {
    #[error("{op:?} event for {table} is missing its after-image")]
    MissingAfter { op: Op, table: TableRef },
    #[error("{op:?} event for {table} is missing its before-image")]
    MissingBefore { op: Op, table: TableRef },
    #[error("{op:?} event for {table} must not carry a before-image")]
    UnexpectedBefore { op: Op, table: TableRef },
    #[error("{op:?} event for {table} must not carry an after-image")]
    UnexpectedAfter { op: Op, table: TableRef },
}

impl CdcEvent {
    /// Checks the image shape per op: Insert/Snapshot = after only; Update = after (+ optional before);
    /// Delete = before only.
    pub fn validate(&self) -> Result<(), EventShapeError> {
        let (op, table) = (self.op, self.table.clone());
        match (self.op, self.before.is_some(), self.after.is_some()) {
            (Op::Insert | Op::Snapshot, true, _) => Err(EventShapeError::UnexpectedBefore { op, table }),
            (Op::Insert | Op::Snapshot | Op::Update, _, false) => Err(EventShapeError::MissingAfter { op, table }),
            (Op::Delete, false, _) => Err(EventShapeError::MissingBefore { op, table }),
            (Op::Delete, true, true) => Err(EventShapeError::UnexpectedAfter { op, table }),
            _ => Ok(()),
        }
    }
}

/// The end of a source transaction; only complete transactions are flushed.
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionBoundary {
    pub xid: u64,
    pub commit_pos: SourcePosition,
    pub commit_ts_micros: i64,
}
```

Check of the match order in `validate`: `Insert` with `before = Some, after = None` hits the first arm
(`UnexpectedBefore`), and the tests expect `MissingAfter` only for `Insert` with both images absent,
which falls to the second arm. `Snapshot` with both images hits `UnexpectedBefore`, as the test expects.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core event::tests && cargo clippy -p data-avalanche-core --all-targets -- -D warnings`
Expected: PASS — 5 tests; no warnings.

- [ ] **Step 6: Run the whole crate once**

Run: `cargo test -p data-avalanche-core`
Expected: PASS — all core tests (73).

- [ ] **Step 7: Commit**

```bash
git add crates/data-avalanche-core
git commit -m "feat(core): CdcEvent, SourceSeq, Op and TransactionBoundary"
```

---

### Task 12: Test infrastructure, testkit, and storage conditional-write check

**Files:**
- Create: `test-infra/compose.yaml`, `test-infra/up.sh`, `test-infra/down.sh`, `test-infra/README.md`
- Create: `crates/data-avalanche-testkit/Cargo.toml`, `crates/data-avalanche-testkit/src/lib.rs`
- Create: `crates/data-avalanche-testkit/tests/smoke.rs`, `crates/data-avalanche-testkit/tests/storage_cas.rs`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: nothing from core.
- Produces: `data_avalanche_testkit::it_enabled() -> bool`; macro `data_avalanche_testkit::require_it!()` (prints
  `skipped: …` and returns when disabled); `data_avalanche_testkit::install_crypto_provider()` (idempotent,
  aws-lc-rs, DA-DEP-5); `data_avalanche_testkit::endpoints::{POSTGRES_18, MYSQL_84, MARIADB_118, RUSTFS_URL,
  TEST_BUCKET, RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY}`; `data_avalanche_testkit::stores::{rustfs(), azurite()}`
  returning `Arc<dyn object_store::ObjectStore>` for bucket/container `data-avalanche-test`;
  `data_avalanche_testkit::gcs_enabled() -> bool`, macro `data_avalanche_testkit::require_gcs!()`,
  `data_avalanche_testkit::stores::gcs() -> Arc<dyn object_store::ObjectStore>` (real bucket from
  `DATA_AVALANCHE_GCS_BUCKET`, credentials from `GOOGLE_*` env); `data_avalanche_testkit::unique_prefix(test: &str) -> String`
  (`data-avalanche-it/<test>/<uuid v4>`). M2 and later milestones build on these.

**Why real GCS (decided 2026-09-26):** `fsouza/fake-gcs-server:1.56.1` rejects `object_store`'s
path-style PUT (`400 invalid uploadType`, measured while this plan was written), and `object_store`'s
own test suite notes fake-gcs doesn't honour preconditions (fake-gcs-server issue #994). GCS tests
therefore run against a **real bucket**, both locally and in PR CI. RustFS (S3) and Azurite (Azure)
passed both conditional-write checks in the same run and stay as local emulators.

- [ ] **Step 1: Write the compose file** (`test-infra/compose.yaml`)

```yaml
# Integration-test infrastructure. Default profile = newest of each engine + S3/Azure emulators.
# GCS has no usable emulator for object_store; GCS tests use a real bucket (see README.md).
# `matrix` profile adds the older supported versions (P-8). `init` profile = one-shot bucket creation.
# Host ports are fixed and non-default; see README.md.
name: data-avalanche-test

x-pg: &pg
  environment: { POSTGRES_USER: avalanche, POSTGRES_PASSWORD: avalanche, POSTGRES_DB: avalanche }
  command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=20",
            "-c", "max_wal_senders=20", "-c", "logical_decoding_work_mem=64kB"]
  healthcheck: { test: ["CMD-SHELL", "pg_isready -U avalanche -d avalanche"], interval: 2s, retries: 30 }

x-mariadb: &mariadb
  environment: { MARIADB_ROOT_PASSWORD: avalanche, MARIADB_DATABASE: avalanche }
  command: ["--server-id=1", "--log-bin=mariadb-bin", "--binlog-format=ROW",
            "--binlog-row-image=FULL", "--binlog-row-metadata=FULL", "--gtid-strict-mode=1"]
  healthcheck: { test: ["CMD", "healthcheck.sh", "--connect", "--innodb_initialized"], interval: 2s, retries: 60 }

services:
  postgres-18: { <<: *pg, image: "postgres:18", ports: ["55418:5432"] }
  postgres-17: { <<: *pg, image: "postgres:17", ports: ["55417:5432"], profiles: [matrix] }
  postgres-16: { <<: *pg, image: "postgres:16", ports: ["55416:5432"], profiles: [matrix] }
  postgres-15: { <<: *pg, image: "postgres:15", ports: ["55415:5432"], profiles: [matrix] }
  postgres-14: { <<: *pg, image: "postgres:14", ports: ["55414:5432"], profiles: [matrix] }

  mysql-84:
    image: "mysql:8.4"
    environment: { MYSQL_ROOT_PASSWORD: avalanche, MYSQL_DATABASE: avalanche }
    command: ["--server-id=1", "--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL",
              "--binlog-row-metadata=FULL", "--gtid-mode=ON", "--enforce-gtid-consistency=ON"]
    ports: ["53384:3306"]
    healthcheck: { test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-uroot", "-pavalanche"], interval: 2s, retries: 60 }

  mariadb-118: { <<: *mariadb, image: "mariadb:11.8", ports: ["53318:3306"] }
  mariadb-114: { <<: *mariadb, image: "mariadb:11.4", ports: ["53314:3306"], profiles: [matrix] }
  mariadb-1011: { <<: *mariadb, image: "mariadb:10.11", ports: ["53311:3306"], profiles: [matrix] }

  rustfs:
    image: "rustfs/rustfs:1.0.0"
    environment:
      RUSTFS_ACCESS_KEY: avalanche
      RUSTFS_SECRET_KEY: avalanche-secret
      RUSTFS_ADDRESS: ":9000"
      RUSTFS_CONSOLE_ENABLE: "false"
    command: ["/data"]
    volumes: ["rustfs-data:/data"]
    ports: ["59000:9000"]

  azurite:
    image: "mcr.microsoft.com/azure-storage/azurite:3.37.0"
    command: ["azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000", "--skipApiVersionCheck", "--loose"]
    # object_store's emulator mode targets 127.0.0.1:10000, so this port is not remapped.
    ports: ["10000:10000"]

  rustfs-init:
    image: "amazon/aws-cli:2.37.4"
    profiles: [init]
    environment: { AWS_ACCESS_KEY_ID: avalanche, AWS_SECRET_ACCESS_KEY: avalanche-secret, AWS_DEFAULT_REGION: us-east-1 }
    entrypoint: ["/bin/sh", "-c"]
    command:
      - 'for i in $$(seq 1 60); do aws --endpoint-url http://rustfs:9000 s3api head-bucket --bucket data-avalanche-test 2>/dev/null && exit 0;
         aws --endpoint-url http://rustfs:9000 s3api create-bucket --bucket data-avalanche-test >/dev/null 2>&1 && exit 0; sleep 1; done; exit 1'

  azurite-init:
    image: "mcr.microsoft.com/azure-cli:2.90.0-azurelinux4.0"
    profiles: [init]
    entrypoint: ["/bin/sh", "-c"]
    command:
      - 'for i in $$(seq 1 60); do az storage container create --name data-avalanche-test --only-show-errors
         --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;"
         >/dev/null && exit 0; sleep 1; done; exit 1'

volumes:
  rustfs-data: {}
```

The Azurite account key above is the public, well-known development key Azurite ships with. It is not
a secret.

- [ ] **Step 2: Write the up/down scripts**

`test-infra/up.sh`:

```bash
#!/usr/bin/env bash
# Start the integration-test infrastructure and create the test bucket in every emulator.
#   test-infra/up.sh            default profile
#   test-infra/up.sh matrix     default + older engine versions
set -euo pipefail
cd "$(dirname "$0")"
profiles=()
if [[ "${1:-}" == "matrix" ]]; then profiles=(--profile matrix); fi
# ${a[@]+...} keeps `set -u` happy with an empty array on macOS bash 3.2.
docker compose ${profiles[@]+"${profiles[@]}"} up -d --wait
for init in rustfs-init azurite-init; do
  docker compose --profile init run --rm "$init"
done
echo "test-infra ready — export DATA_AVALANCHE_IT=1 to run integration tests"
```

`test-infra/down.sh`:

```bash
#!/usr/bin/env bash
# Stop everything and drop volumes (all profiles).
set -euo pipefail
cd "$(dirname "$0")"
docker compose --profile matrix --profile init down -v --remove-orphans
```

Run: `chmod +x test-infra/up.sh test-infra/down.sh`

- [ ] **Step 3: Verify every pinned image exists before first use**

Run:

```bash
for img in postgres:18 postgres:17 postgres:16 postgres:15 postgres:14 mysql:8.4 mariadb:11.8 mariadb:11.4 mariadb:10.11 \
  rustfs/rustfs:1.0.0 mcr.microsoft.com/azure-storage/azurite:3.37.0 \
  amazon/aws-cli:2.37.4 mcr.microsoft.com/azure-cli:2.90.0-azurelinux4.0; do
  docker manifest inspect "$img" > /dev/null && echo "ok  $img" || echo "MISSING $img"
done
```

Expected: every line starts with `ok`. If one is `MISSING`, replace that tag with the newest stable tag
from the registry, re-run, and note the change in `test-infra/README.md`.

- [ ] **Step 4: Create the testkit crate**

Add to the root `Cargo.toml` `[workspace.dependencies]`:

```toml
data-avalanche-testkit = { path = "crates/data-avalanche-testkit" }
```

`crates/data-avalanche-testkit/Cargo.toml`:

```toml
[package]
name = "data-avalanche-testkit"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "Integration-test helpers. Never linked into production binaries."

[lints]
workspace = true

[dependencies]
object_store.workspace = true
rustls.workspace = true
uuid.workspace = true

[dev-dependencies]
tokio.workspace = true
tokio-postgres.workspace = true
mysql_async.workspace = true
```

`crates/data-avalanche-testkit/src/lib.rs`:

```rust
//! Helpers for integration tests that run against `test-infra/compose.yaml` and, for GCS, a real
//! bucket. Tests call `require_it!()` / `require_gcs!()` first, so `cargo test` stays hermetic unless
//! `DATA_AVALANCHE_IT` is set.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;

fn env_set(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| !v.is_empty())
}

/// True when `DATA_AVALANCHE_IT` is set to a non-empty value (an empty value must not enable tests).
pub fn it_enabled() -> bool {
    env_set("DATA_AVALANCHE_IT")
}

/// True when integration tests are enabled and GCS tests were not explicitly opted out with
/// `DATA_AVALANCHE_SKIP_GCS` (used by CI for fork PRs, which receive no secrets).
pub fn gcs_enabled() -> bool {
    it_enabled() && !env_set("DATA_AVALANCHE_SKIP_GCS")
}

/// Returns early from a test when integration tests are disabled.
#[macro_export]
macro_rules! require_it {
    () => {
        if !$crate::it_enabled() {
            eprintln!("skipped: set DATA_AVALANCHE_IT=1 after running test-infra/up.sh");
            return;
        }
    };
}

/// Returns early unless GCS tests are enabled. When they are enabled, `stores::gcs()` fails loudly on
/// missing configuration instead of skipping, so a misconfigured CI can't pass silently.
#[macro_export]
macro_rules! require_gcs {
    () => {
        if !$crate::gcs_enabled() {
            eprintln!("skipped: GCS tests need DATA_AVALANCHE_IT=1 and no DATA_AVALANCHE_SKIP_GCS");
            return;
        }
    };
}

/// Installs aws-lc-rs as the process-wide rustls provider. Several dependencies link different
/// providers; without an explicit choice the first TLS handshake panics (DA-DEP-5). Idempotent.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// A key prefix no other test run uses: `data-avalanche-it/<test>/<uuid v4>`. Tests on the shared real
/// GCS bucket must write only under such a prefix and delete it when done.
pub fn unique_prefix(test: &str) -> String {
    format!("data-avalanche-it/{test}/{}", uuid::Uuid::new_v4())
}

/// Fixed endpoints from `test-infra/compose.yaml`.
pub mod endpoints {
    pub const POSTGRES_18: &str = "host=127.0.0.1 port=55418 user=avalanche password=avalanche dbname=avalanche";
    pub const MYSQL_84: &str = "mysql://root:avalanche@127.0.0.1:53384/avalanche";
    pub const MARIADB_118: &str = "mysql://root:avalanche@127.0.0.1:53318/avalanche";
    pub const RUSTFS_URL: &str = "http://127.0.0.1:59000";
    pub const RUSTFS_ACCESS_KEY: &str = "avalanche";
    pub const RUSTFS_SECRET_KEY: &str = "avalanche-secret";
    pub const TEST_BUCKET: &str = "data-avalanche-test";
}

/// Object stores for integration tests.
pub mod stores {
    use super::*;
    use endpoints::*;

    pub fn rustfs() -> Arc<dyn ObjectStore> {
        let store = AmazonS3Builder::new()
            .with_endpoint(RUSTFS_URL)
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_bucket_name(TEST_BUCKET)
            .with_access_key_id(RUSTFS_ACCESS_KEY)
            .with_secret_access_key(RUSTFS_SECRET_KEY)
            .build()
            .expect("valid static RustFS configuration");
        Arc::new(store)
    }

    pub fn azurite() -> Arc<dyn ObjectStore> {
        let store = MicrosoftAzureBuilder::new()
            .with_use_emulator(true)
            .with_container_name(TEST_BUCKET)
            .build()
            .expect("valid static Azurite configuration");
        Arc::new(store)
    }

    /// The real GCS bucket named by `DATA_AVALANCHE_GCS_BUCKET`. Credentials come from the standard
    /// `GOOGLE_*` variables read by `object_store` (`GOOGLE_SERVICE_ACCOUNT_PATH`,
    /// `GOOGLE_SERVICE_ACCOUNT_KEY` or `GOOGLE_APPLICATION_CREDENTIALS`).
    ///
    /// # Panics
    /// When `DATA_AVALANCHE_GCS_BUCKET` is unset or the builder rejects the credentials, naming what to set.
    pub fn gcs() -> Arc<dyn ObjectStore> {
        let bucket = std::env::var("DATA_AVALANCHE_GCS_BUCKET").ok().filter(|b| !b.is_empty()).expect(
            "DATA_AVALANCHE_GCS_BUCKET is not set: export it and GOOGLE_SERVICE_ACCOUNT_PATH, \
             or set DATA_AVALANCHE_SKIP_GCS=1 (see test-infra/README.md)",
        );
        let store = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .expect("GCS credentials: set GOOGLE_SERVICE_ACCOUNT_PATH (see test-infra/README.md)");
        Arc::new(store)
    }
}
```

- [ ] **Step 5: Write the smoke tests** (`crates/data-avalanche-testkit/tests/smoke.rs`)

```rust
use data_avalanche_testkit::{endpoints, install_crypto_provider, require_it};
use mysql_async::prelude::Queryable;

#[tokio::test]
async fn postgres_18_runs_with_logical_wal() {
    require_it!();
    let (client, conn) = tokio_postgres::connect(endpoints::POSTGRES_18, tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(conn);
    let row = client.query_one("SHOW wal_level", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "logical");
}

#[tokio::test]
async fn mysql_84_has_gtid_and_full_row_image() {
    require_it!();
    install_crypto_provider();
    let pool = mysql_async::Pool::new(endpoints::MYSQL_84);
    let mut conn = pool.get_conn().await.unwrap();
    let (gtid, format, image): (String, String, String) =
        conn.query_first("SELECT @@gtid_mode, @@binlog_format, @@binlog_row_image").await.unwrap().unwrap();
    assert_eq!((gtid.as_str(), format.as_str(), image.as_str()), ("ON", "ROW", "FULL"));
    drop(conn);
    pool.disconnect().await.unwrap();
}

#[tokio::test]
async fn mariadb_118_has_row_format_and_strict_gtid() {
    require_it!();
    install_crypto_provider();
    let pool = mysql_async::Pool::new(endpoints::MARIADB_118);
    let mut conn = pool.get_conn().await.unwrap();
    let (format, image, strict): (String, String, i64) =
        conn.query_first("SELECT @@binlog_format, @@binlog_row_image, @@gtid_strict_mode").await.unwrap().unwrap();
    assert_eq!((format.as_str(), image.as_str(), strict), ("ROW", "FULL", 1));
    drop(conn);
    pool.disconnect().await.unwrap();
}
```

- [ ] **Step 6: Write the conditional-write tests** (`crates/data-avalanche-testkit/tests/storage_cas.rs`)

```rust
//! Does each storage backend honour create-if-absent and update-if-version-matches? The results
//! decide where the M2 lease conformance suite runs (roadmap R-3).

use std::sync::Arc;

use data_avalanche_testkit::{require_gcs, require_it, stores, unique_prefix};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload, UpdateVersion};

async fn check_conditional_writes(store: Arc<dyn ObjectStore>, test: &str) {
    let key = Path::from(format!("{}/cas", unique_prefix(test)));

    let first = store
        .put_opts(&key, PutPayload::from_static(b"one"), PutMode::Create.into())
        .await
        .expect("create on an empty key must succeed");

    let again = store.put_opts(&key, PutPayload::from_static(b"two"), PutMode::Create.into()).await;
    assert!(matches!(again, Err(object_store::Error::AlreadyExists { .. })), "create on an existing key: {again:?}");

    let v1: UpdateVersion = first.into();
    store
        .put_opts(&key, PutPayload::from_static(b"two"), PutMode::Update(v1.clone()).into())
        .await
        .expect("update with the current version must succeed");

    let stale = store.put_opts(&key, PutPayload::from_static(b"three"), PutMode::Update(v1).into()).await;
    assert!(matches!(stale, Err(object_store::Error::Precondition { .. })), "update with a stale version: {stale:?}");

    let body = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert_eq!(&body[..], b"two", "the stale update must not have landed");

    store.delete(&key).await.expect("cleanup");
}

#[tokio::test]
async fn rustfs_honours_conditional_writes() {
    require_it!();
    check_conditional_writes(stores::rustfs(), "rustfs_cas").await;
}

#[tokio::test]
async fn azurite_honours_conditional_writes() {
    require_it!();
    check_conditional_writes(stores::azurite(), "azurite_cas").await;
}

#[tokio::test]
async fn real_gcs_honours_conditional_writes() {
    require_gcs!();
    data_avalanche_testkit::install_crypto_provider();
    check_conditional_writes(stores::gcs(), "gcs_cas").await;
}
```

- [ ] **Step 7: Verify the tests are skipped by default**

Run: `cargo test -p data-avalanche-testkit`
Expected: PASS — 6 tests pass, each printing `skipped: set DATA_AVALANCHE_IT=1 …` (visible with `-- --nocapture`).

Run: `DATA_AVALANCHE_IT= cargo test -p data-avalanche-testkit -- --nocapture 2>&1 | grep -c skipped`
Expected: `6` (an empty value must not enable them).

Run: `DATA_AVALANCHE_IT=1 DATA_AVALANCHE_SKIP_GCS=1 cargo test -p data-avalanche-testkit --test storage_cas real_gcs -- --nocapture 2>&1 | grep -c "skipped: GCS"`
Expected: `1` (the explicit opt-out skips the GCS test even with `DATA_AVALANCHE_IT` set).

- [ ] **Step 8: Run against the infrastructure and the real GCS bucket**

Run (credentials stay in your environment; nothing prints them):

```bash
test-infra/up.sh
export DATA_AVALANCHE_IT=1 DATA_AVALANCHE_GCS_BUCKET=<dev bucket, e.g. leasetest>
export GOOGLE_SERVICE_ACCOUNT_PATH=<path to the service-account key JSON>
cargo test -p data-avalanche-testkit -- --test-threads=1 --nocapture
```

Expected: the three smoke tests PASS. `rustfs_honours_conditional_writes` and
`azurite_honours_conditional_writes` and `real_gcs_honours_conditional_writes` PASS (all three
measured on 2026-09-26 while this plan was written; GCS against bucket `leasetest`). Record the outcome. **A failure is a finding, not an M0 blocker**: record it in Step 9
and raise it before M2 starts.

- [ ] **Step 9: Write `test-infra/README.md` with the recorded results**

```markdown
# test-infra

Containers for integration tests. Start with `test-infra/up.sh` (add `matrix` for older engine
versions), stop with `test-infra/down.sh`, and run tests with `DATA_AVALANCHE_IT=1 cargo test`.

| Service | Image | Host port | Credentials | Profile |
|---|---|---|---|---|
| postgres-18 | postgres:18 | 55418 | avalanche / avalanche, db `avalanche` | default |
| postgres-14…17 | postgres:14…17 | 55414…55417 | same | matrix |
| mysql-84 | mysql:8.4 (GTID on) | 53384 | root / avalanche, db `avalanche` | default |
| mariadb-118 | mariadb:11.8 (`gtid_strict_mode=1`) | 53318 | root / avalanche | default |
| mariadb-114, mariadb-1011 | mariadb:11.4, 10.11 | 53314, 53311 | root / avalanche | matrix |
| rustfs (S3) | rustfs/rustfs:1.0.0 | 59000 | avalanche / avalanche-secret | default |
| azurite (Azure Blob) | mcr.microsoft.com/azure-storage/azurite:3.37.0 | 10000 | Azurite's public dev account | default |

Bucket / container `data-avalanche-test` is created by the `init` profile (`up.sh` runs it). Azurite keeps
port 10000 because `object_store`'s emulator mode always targets `127.0.0.1:10000`.

Added by later milestones: `postgres-tls` (M5, CA-signed server certificate) and `mysql-84-nogtid`
(M7, file/position mode).

## GCS: a real bucket, not an emulator

`fake-gcs-server` 1.56.1 rejects `object_store`'s path-style PUT (`400 invalid uploadType`) and
doesn't honour preconditions (fake-gcs-server issue #994), so GCS tests use a real bucket.

| Variable | Meaning |
|---|---|
| `DATA_AVALANCHE_GCS_BUCKET` | Bucket for integration tests. Tests write only under `data-avalanche-it/<test>/<uuid>/` and delete what they wrote. |
| `GOOGLE_SERVICE_ACCOUNT_PATH` | Path to a service-account key JSON (read by `object_store`). `GOOGLE_SERVICE_ACCOUNT_KEY` (inline JSON) or `GOOGLE_APPLICATION_CREDENTIALS` also work. |
| `DATA_AVALANCHE_SKIP_GCS` | Any non-empty value skips GCS tests. With `DATA_AVALANCHE_IT` set and this unset, a missing bucket or credentials **fails** the test instead of skipping it. |

Bucket requirements:
- The service account needs `roles/storage.objectAdmin` on the bucket, and nothing project-wide.
- **Object versioning off** (conditional writes use generations; see the lease-conformance README).
- A lifecycle rule deleting objects under `data-avalanche-it/` after 1 day, to clean up after crashed runs:
  `gcloud storage buckets update gs://<bucket> --lifecycle-file=lifecycle.json` with
  `{"rule":[{"action":{"type":"Delete"},"condition":{"age":1,"matchesPrefix":["data-avalanche-it/"]}}]}`.

## CI requirements (GitHub Actions)

| Name | Kind | Value |
|---|---|---|
| `GCP_SA_KEY` | repository **secret** | The service-account key JSON, verbatim. The workflow writes it to `$RUNNER_TEMP` and exports `GOOGLE_SERVICE_ACCOUNT_PATH`; it is never echoed. |
| `DATA_AVALANCHE_GCS_BUCKET` | repository **variable** | The CI bucket name (not secret). Prefer a bucket separate from local development. |

Pull requests from forks receive no secrets. The workflow detects the empty secret, sets
`DATA_AVALANCHE_SKIP_GCS=1` and emits a notice, so fork PRs test S3/Azure only; a maintainer re-runs
GCS on merge. S3 and Azure need no CI secrets because they run on the emulators above.

## Storage conditional-write results (M0 Task 12)

| Backend | Create-if-absent | Update-if-version | Measured | Consequence for M2 |
|---|---|---|---|---|
| RustFS 1.0.0 (S3 API) | PASS | PASS | 2026-09-26 | Carries the S3 lease/CAS suite in PR CI |
| Azurite 3.37.0 | PASS | PASS | 2026-09-26 | Carries the Azure lease/CAS suite in PR CI |
| fake-gcs-server 1.56.1 | FAIL (plain PUT → 400 `invalid uploadType`) | not reachable | 2026-09-26 | Dropped |
| Real GCS (`DATA_AVALANCHE_GCS_BUCKET`) | PASS | PASS | 2026-09-26 (bucket `leasetest`) | Carries the GCS lease/CAS suite in PR CI |
```

All four rows were measured while this plan was written (2026-09-26); re-run Step 8 to confirm them
on the implementation.

- [ ] **Step 10: Add the integration job to CI** (append to `.github/workflows/ci.yml` under `jobs:`)

```yaml
  # Integration tests: DB containers + S3/Azure emulators (test-infra/compose.yaml) + a REAL GCS bucket.
  # Requirements (documented in test-infra/README.md, "CI requirements"):
  #   secrets.GCP_SA_KEY           service-account key JSON with roles/storage.objectAdmin on the bucket
  #   vars.DATA_AVALANCHE_GCS_BUCKET    CI bucket name (versioning off, lifecycle rule on data-avalanche-it/)
  # Fork PRs get no secrets: GCS tests are then skipped (DATA_AVALANCHE_SKIP_GCS=1) with a notice.
  integration:
    runs-on: ubuntu-latest
    needs: check
    env:
      DATA_AVALANCHE_IT: "1"
      DATA_AVALANCHE_GCS_BUCKET: ${{ vars.DATA_AVALANCHE_GCS_BUCKET }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions-rust-lang/setup-rust-toolchain@v1
      - name: GCS credentials
        env:
          GCP_SA_KEY: ${{ secrets.GCP_SA_KEY }}
        run: |
          if [ -z "$GCP_SA_KEY" ] || [ -z "$DATA_AVALANCHE_GCS_BUCKET" ]; then
            echo "::notice::GCP_SA_KEY or DATA_AVALANCHE_GCS_BUCKET unavailable (fork PR?) - GCS tests skipped"
            echo "DATA_AVALANCHE_SKIP_GCS=1" >> "$GITHUB_ENV"
          else
            umask 077
            printf '%s' "$GCP_SA_KEY" > "$RUNNER_TEMP/gcs-key.json"
            echo "GOOGLE_SERVICE_ACCOUNT_PATH=$RUNNER_TEMP/gcs-key.json" >> "$GITHUB_ENV"
          fi
      - name: Start test infrastructure
        run: test-infra/up.sh
      - name: Integration tests
        run: cargo test --workspace -- --test-threads=1
      - name: Container logs on failure
        if: failure()
        run: docker compose -f test-infra/compose.yaml logs --no-color
      - name: Stop test infrastructure
        if: always()
        run: test-infra/down.sh
      - name: Remove GCS key
        if: always()
        run: rm -f "$RUNNER_TEMP/gcs-key.json"
```

- [ ] **Step 11: Final local check and teardown**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: all green; integration tests skipped.

Run: `test-infra/down.sh`
Expected: containers and volumes removed.

- [ ] **Step 12: Commit**

```bash
git add test-infra crates/data-avalanche-testkit Cargo.toml Cargo.lock .github/workflows/ci.yml
git commit -m "test(infra): compose test infrastructure, testkit, storage conditional-write findings (real GCS)"
```

---

## M0 exit checklist

- [ ] `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` pass locally and in CI.
- [ ] The CI `integration` job passes with `GCP_SA_KEY` / `DATA_AVALANCHE_GCS_BUCKET` configured: smoke tests and all three CAS tests green (or a recorded finding raised before M2).
- [ ] `test-infra/README.md` records the CAS results, including real GCS; a failure goes into roadmap risk R-3 and the M2 plan.
- [ ] `prototypes/*` still resolve on their own (`cargo metadata --manifest-path prototypes/<p>/Cargo.toml`).
- [ ] Roadmap §3.1–3.2 match what landed. Any signature that changed during implementation is updated there, since the next milestone plans read it.
